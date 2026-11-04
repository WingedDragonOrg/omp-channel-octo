import { expect, test } from "bun:test";
import { access, mkdtemp, mkdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { captureReviewSnapshot } from "../src/review/snapshot";

async function git(cwd: string, args: readonly string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", cwd, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" },
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (exitCode !== 0) throw new Error(stderr || stdout || `git exited ${exitCode}`);
  return stdout.trim();
}

async function findGitExecutable(): Promise<string> {
  for (const directory of (process.env.PATH ?? "").split(":")) {
    const candidate = join(directory, "git");
    try {
      await access(candidate);
      return await realpath(candidate);
    } catch {}
  }
  throw new Error("git executable was not found on PATH");
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

/** Polling here waits for real detached Git child processes to enter both projects. */
async function waitForProjectMarkers(marker: string, projects: readonly string[]): Promise<void> {
  for (let turn = 0; turn < 400; turn += 1) {
    try {
      const lines = (await readFile(marker, "utf8")).split("\n");
      if (projects.every(project => lines.includes(project))) return;
    } catch {}
    const delay = Promise.withResolvers<void>();
    setTimeout(delay.resolve, 5);
    await delay.promise;
  }
  throw new Error("Git wrapper did not start for every project");
}

async function fixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "review-snapshot-"));
  await git(root, ["init", "--quiet"]);
  await writeFile(join(root, "delete-me.ts"), "export const removed = true;\n");
  await writeFile(join(root, "rename-me.ts"), "export const renamed = true;\n");
  await writeFile(join(root, "mixed.ts"), "export const value = 1;\n");
  await writeFile(join(root, "binary.bin"), new Uint8Array([0, 1, 2, 3]));
  await git(root, ["add", "."]);
  await git(root, [
    "-c", "user.name=Review Snapshot Fixture",
    "-c", "user.email=review-snapshot@example.invalid",
    "-c", "commit.gpgsign=false",
    "commit", "--quiet", "-m", "baseline",
  ]);
  return root;
}

test("captures staged, worktree, and untracked evidence while omitting binary contents", async () => {
  const root = await fixture();
  try {
    await rm(join(root, "delete-me.ts"));
    await git(root, ["mv", "rename-me.ts", "renamed.ts"]);
    await writeFile(join(root, "binary.bin"), new Uint8Array([0, 9, 2, 3]));
    await writeFile(join(root, "mixed.ts"), "export const value = 2;\n");
    await git(root, ["add", "delete-me.ts", "renamed.ts", "binary.bin", "mixed.ts"]);
    await writeFile(join(root, "mixed.ts"), "export const value = 3;\n");
    await mkdir(join(root, "new-dir"));
    await writeFile(join(root, "new-dir", "note.txt"), "untracked evidence\n");
    await writeFile(join(root, "new.bin"), new Uint8Array([0, 7, 8, 9]));

    const expectedHead = await git(root, ["rev-parse", "HEAD"]);
    const expectedPath = await realpath(root);
    const snapshot = await captureReviewSnapshot([{ name: "fixture", path: root }]);
    const project = snapshot.projects[0]!;

    expect(project.name).toBe("fixture");
    expect(project.path).toBe(expectedPath);
    expect(project.repositoryPath).toBe(expectedPath);
    expect(project.headBefore).toBe(expectedHead);
    expect(project.headAfter).toBe(expectedHead);
    expect(project.baselineStable).toBe(true);
    expect(project.uncovered).toContainEqual({
      scope: "staged",
      detail: 'binary content omitted from review evidence: "binary.bin"',
    });

    expect(project.staged.files).toEqual([
      "binary.bin",
      "delete-me.ts",
      "mixed.ts",
      "rename-me.ts -> renamed.ts",
    ]);
    expect(project.staged.evidence).toContain("deleted file mode");
    expect(project.staged.evidence).toContain("rename from rename-me.ts");
    expect(project.staged.evidence).toContain("rename to renamed.ts");
    expect(project.staged.evidence).not.toContain("GIT binary patch");
    expect(project.staged.evidence).toContain("-export const value = 1;");
    expect(project.staged.evidence).toContain("+export const value = 2;");

    expect(project.unstaged.files).toEqual(["mixed.ts"]);
    expect(project.unstaged.evidence).toContain("-export const value = 2;");
    expect(project.unstaged.evidence).toContain("+export const value = 3;");

    expect(project.untracked.files).toEqual(["new-dir/note.txt", "new.bin"]);
    expect(project.untracked.evidence).toContain("untracked evidence");
    expect(project.untracked.evidence).not.toContain("GIT binary patch");
    expect(project.uncovered).toContainEqual({
      scope: "untracked",
      detail: 'binary content omitted from review evidence: "new.bin"',
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("redacts credential values while preserving evidence that the credential changed", async () => {
  const root = await fixture();
  try {
    const credentials = [
      '{"password":"password-secret","api_key":"api-key-secret","access_token":"access-token-secret","refresh_token":"refresh-token-secret","private_key":"private-key-secret","authorization":"authorization-secret","client_secret":"client-secret","token":"token-secret"}',
      "{'apiKey': 'camel-secret'}",
      "-----BEGIN PRIVATE KEY-----",
      "private-key-body-secret",
      "-----END PRIVATE KEY-----",
    ].join("\n");
    await writeFile(join(root, "credentials.txt"), `${credentials}\n`);
    const snapshot = await captureReviewSnapshot([{ name: "fixture", path: root }]);
    const project = snapshot.projects[0]!;

    expect(project.untracked.files).toEqual(["credentials.txt"]);
    for (const secret of [
      "super-secret-review-token",
      "password-secret",
      "api-key-secret",
      "access-token-secret",
      "refresh-token-secret",
      "private-key-secret",
      "authorization-secret",
      "client-secret",
      "token-secret",
      "camel-secret",
      "private-key-body-secret",
    ]) {
      expect(project.untracked.evidence).not.toContain(secret);
    }
    expect(project.untracked.evidence).toContain('"password":"[redacted]"');
    expect(project.untracked.evidence).toContain('"api_key":"[redacted]"');
    expect(project.untracked.evidence).toContain("'apiKey': '[redacted]'");
    expect(project.untracked.evidence).toContain("[redacted private key]");
    expect(project.uncovered).toContainEqual({
      scope: "untracked",
      detail: "sensitive-looking values were redacted from review evidence",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("preserves global Git excludes without passing credential environment to Git", async () => {
  const root = await fixture();
  const home = await mkdtemp(join(tmpdir(), "review-snapshot-home-"));
  const wrapperDir = await mkdtemp(join(tmpdir(), "review-snapshot-git-wrapper-"));
  const ignoreFile = join(home, "global-ignore");
  const configFile = join(home, "gitconfig");
  const marker = join(wrapperDir, "credential-inherited");
  const envKeys = [
    "HOME",
    "XDG_CONFIG_HOME",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_NOSYSTEM",
    "PATH",
    "REVIEW_SNAPSHOT_CREDENTIAL",
  ] as const;
  const original = new Map(envKeys.map(key => [key, process.env[key]]));
  try {
    const realGit = await findGitExecutable();
    await writeFile(join(wrapperDir, "git"), `#!/bin/sh
if [ -n "\${REVIEW_SNAPSHOT_CREDENTIAL:-}" ]; then printf inherited > ${shellQuote(marker)}; fi
exec ${shellQuote(realGit)} "$@"\n`, { mode: 0o755 });
    await writeFile(ignoreFile, "*.pem\n");
    await writeFile(configFile, `[core]\n\texcludesFile = ${ignoreFile}\n`);
    await writeFile(join(root, "credential.pem"), "must stay globally ignored\n");
    process.env.HOME = home;
    process.env.XDG_CONFIG_HOME = join(home, ".config");
    process.env.GIT_CONFIG_GLOBAL = configFile;
    process.env.GIT_CONFIG_NOSYSTEM = "1";
    process.env.PATH = `${wrapperDir}:${process.env.PATH ?? ""}`;
    process.env.REVIEW_SNAPSHOT_CREDENTIAL = "host-credential-sentinel";
    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;
    const markerExists = await access(marker).then(() => true, () => false);
    expect(markerExists).toBe(false);
    expect(project.untracked.files).toEqual([]);
    expect(project.untracked.evidence).toBe("");
  } finally {
    for (const key of envKeys) {
      const value = original.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
    await rm(home, { recursive: true, force: true });
    await rm(wrapperDir, { recursive: true, force: true });
  }
});

test("reports oversized files and patch output rather than returning partial evidence", async () => {
  const root = await fixture();
  try {
    await writeFile(join(root, "mixed.ts"), "x".repeat(4 * 1024 * 1024 + 1));
    await writeFile(join(root, "oversized.bin"), Buffer.alloc(2 * 1024 * 1024 + 1, 0x78));

    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;

    expect(project.unstaged.evidence).toBe("");
    expect(project.untracked.files).toEqual(["oversized.bin"]);
    expect(project.untracked.evidence).toBe("");
    expect(project.uncovered.some(item =>
      item.scope === "unstaged" && item.detail.includes("4194304 byte Git output limit"))).toBe(true);
    expect(project.uncovered.some(item =>
      item.scope === "untracked" && item.detail.includes("2097152 byte capture limit"))).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounds aggregate untracked patch evidence without returning a partial file diff", async () => {
  const root = await fixture();
  try {
    for (const name of ["large-0.txt", "large-1.txt", "large-2.txt"]) {
      await writeFile(join(root, name), "a".repeat(1_500_000));
    }

    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;

    expect(Buffer.byteLength(project.untracked.evidence, "utf8")).toBeLessThanOrEqual(4 * 1024 * 1024);
    expect(project.untracked.evidence).toContain("large-0.txt");
    expect(project.untracked.evidence).toContain("large-1.txt");
    expect(project.untracked.evidence).not.toContain("large-2.txt");
    expect(project.uncovered.some(item =>
      item.scope === "untracked" && item.detail.includes("4194304 byte capture limit"))).toBe(true);
    expect(project.baselineStable).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("captures a file named dash and never opens an untracked symlink target", async () => {
  const root = await fixture();
  try {
    const fifo = join(root, "pipe");
    const fifoChild = Bun.spawn(["mkfifo", fifo], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
    expect(await fifoChild.exited).toBe(0);
    await symlink("pipe", join(root, "pipe-link"));
    await writeFile(join(root, "-"), "dash-path evidence\n");

    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;

    expect(project.untracked.files).toContain("-");
    expect(project.untracked.evidence).toContain("dash-path evidence");
    expect(project.untracked.evidence).toContain("[untracked symlink] pipe-link -> pipe");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("treats no-index exit 1 with stderr as an evidence error", async () => {
  const root = await fixture();
  const wrapperDir = await mkdtemp(join(tmpdir(), "review-snapshot-git-wrapper-"));
  const originalPath = process.env.PATH;
  try {
    const realGit = await findGitExecutable();
    const wrapper = join(wrapperDir, "git");
    await writeFile(wrapper, `#!/bin/sh
no_index=0
numstat=0
for argument in "$@"; do
  [ "$argument" = "--no-index" ] && no_index=1
  [ "$argument" = "--numstat" ] && numstat=1
done
if [ "$no_index" -eq 1 ] && [ "$numstat" -eq 1 ]; then
  printf 'injected Git failure\\n' >&2
  exit 1
fi
exec ${shellQuote(realGit)} "$@"\n`, { mode: 0o755 });
    process.env.PATH = `${wrapperDir}:${originalPath ?? ""}`;
    await writeFile(join(root, "new.txt"), "text whose stat must fail\n");

    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;

    expect(project.untracked.files).toEqual(["new.txt"]);
    expect(project.untracked.evidence).toBe("");
    expect(project.uncovered.some(item =>
      item.scope === "untracked" && item.detail.includes("could not be classified safely"))).toBe(true);
    expect(JSON.stringify(project)).not.toContain("injected Git failure");
  } finally {
    process.env.PATH = originalPath;
    await rm(root, { recursive: true, force: true });
    await rm(wrapperDir, { recursive: true, force: true });
  }
});

test("waits for every detached Git process group before propagating multi-project cancellation", async () => {
  const first = await fixture();
  const second = await fixture();
  const wrapperDir = await mkdtemp(join(tmpdir(), "review-snapshot-git-wrapper-"));
  const marker = join(wrapperDir, "started-projects");
  const originalPath = process.env.PATH;
  const controller = new AbortController();
  let operation: Promise<unknown> | undefined;
  try {
    const realGit = await findGitExecutable();
    await writeFile(join(wrapperDir, "git"), `#!/bin/sh
project_path=$(/bin/pwd)
printf '%s\\n' "\${project_path##*/}" >> ${shellQuote(marker)}
/bin/sleep 5
exec ${shellQuote(realGit)} "$@"\n`, { mode: 0o755 });
    process.env.PATH = `${wrapperDir}:${originalPath ?? ""}`;
    const startedAt = Date.now();
    operation = captureReviewSnapshot([
      { name: "first", path: first },
      { name: "second", path: second },
    ], controller.signal);
    await waitForProjectMarkers(marker, [basename(first), basename(second)]);
    const reason = new Error("review stopped");
    controller.abort(reason);

    let rejection: unknown;
    try {
      await operation;
    } catch (error) {
      rejection = error;
    }
    expect(rejection).toBe(reason);
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  } finally {
    if (!controller.signal.aborted) controller.abort(new Error("test cleanup"));
    await operation?.catch(() => {});
    process.env.PATH = originalPath;
    await rm(first, { recursive: true, force: true });
    await rm(second, { recursive: true, force: true });
    await rm(wrapperDir, { recursive: true, force: true });
  }
});

test("reports dirty submodule paths including spaces as uncovered", async () => {
  const root = await fixture();
  const source = await fixture();
  try {
    await git(root, ["-c", "protocol.file.allow=always", "submodule", "add", "-q", source, "vendor/my module"]);
    await git(root, ["add", ".gitmodules", "vendor/my module"]);
    await git(root, [
      "-c", "user.name=Review Snapshot Fixture",
      "-c", "user.email=review-snapshot@example.invalid",
      "-c", "commit.gpgsign=false",
      "commit", "--quiet", "-m", "add submodule",
    ]);
    await writeFile(join(root, "vendor/my module", "mixed.ts"), "dirty submodule content\n");

    const project = (await captureReviewSnapshot([{ name: "fixture", path: root }])).projects[0]!;

    expect(project.uncovered.some(item =>
      item.scope === "project"
        && item.detail === 'dirty submodule contents are not included in review evidence: "vendor/my module"')).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
    await rm(source, { recursive: true, force: true });
  }
});
test("reports a missing project as uncovered without hiding snapshots of valid projects", async () => {
  const root = await fixture();
  try {
    const missing = join(root, "does-not-exist");
    const snapshot = await captureReviewSnapshot([
      { name: "missing", path: missing },
      { name: "valid", path: root },
    ]);

    expect(snapshot.projects).toHaveLength(2);
    expect(snapshot.projects[0]).toMatchObject({
      name: "missing",
      requestedPath: missing,
      baselineStable: false,
      staged: { files: [], evidence: "" },
      unstaged: { files: [], evidence: "" },
      untracked: { files: [], evidence: "" },
    });
    expect(snapshot.projects[0]!.uncovered).toEqual([
      { scope: "project", detail: "project path is unavailable" },
    ]);
    expect(snapshot.projects[1]!.headBefore).toMatch(/^[0-9a-f]{40,64}$/);
    expect(snapshot.projects[1]!.uncovered).toEqual([]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
