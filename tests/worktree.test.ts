import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWorktreePort, worktreeCandidates, worktreeSlug } from "../src/workspace/worktree";

/**
 * The port shells out to the OMP executable and to Worktrunk, so these tests use executable stubs
 * that accept the same argv and perform the real git work; production runs `omp worktree add`
 * (fallback) and `wt switch --create` / `wt merge` / `wt remove` (preferred).
 */
const TRANSLATING_STUB = String.raw`set -e
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "worktree" ] && [ "$2" = "add" ] || exit 64
path="$3"
shift 3
base=""
branch=""
repo=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -b) branch="$2"; shift 2 ;;
    -C) repo="$2"; shift 2 ;;
    -q) shift ;;
    *) [ -z "$base" ] || exit 64; base="$1"; shift ;;
  esac
done
[ -n "$branch" ] && [ -n "$repo" ] || exit 65
if [ -n "$base" ]; then
  exec git -C "$repo" worktree add -q -b "$branch" "$path" "$base"
fi
exec git -C "$repo" worktree add -q -b "$branch" "$path"`;

const WT_SWITCH_STUB = String.raw`set -e
if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "switch" ] && [ "$2" = "--create" ] || exit 64
branch="$3"
base="HEAD"
config=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --base) base="$2"; shift 2 ;;
    worktree-path=*) config="$1"; shift ;;
    *) shift ;;
  esac
done
[ -n "$config" ] || exit 65
path=$(printf '%s' "$config" | sed -e 's/^worktree-path=//' -e 's/^"//' -e 's/"$//')
git worktree add -q -b "$branch" "$path" "$base"
printf '{"action":"created","branch":"%s","path":"%s"}\n' "$branch" "$path"`;

const WT_MERGE_STUB = String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "merge" ] || exit 64
printf '{"branch":"b","target":"main","squashed":true,"rebased":true,"committed":false,"removed":false}\n'`;
const WT_REAL_MERGE_STUB = String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
[ "$1" = "merge" ] || exit 64
common=$(git rev-parse --path-format=absolute --git-common-dir)
repo=$(dirname "$common")
git -C "$repo" merge --no-ff --no-edit feature >/dev/null
printf '{"branch":"feature","target":"main","squashed":false,"rebased":false,"committed":true,"removed":false}\n'`;

const WT_REMOVE_STUB = String.raw`set -e
if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "remove" ] && [ "$2" = "--foreground" ] || exit 64
force=""
path=""
for arg in "$@"; do
  case "$arg" in
    -f) force="--force" ;;
    /*) path="$arg" ;;
  esac
done
[ -n "$path" ] || exit 65
worktree_repo=$(git rev-parse --show-toplevel)
[ "$worktree_repo" = "$path" ] || {
  echo "wt must run in the target worktree" >&2
  exit 66
}
git -C "$path" worktree remove $force "$path"
printf '[{"branch":"b","path":"%s","kind":"worktree","branch_outcome":"deleted"}]\n' "$path"`;

async function writeStub(directory: string, body: string, name = "stub-omp"): Promise<string> {
  const path = join(directory, name);
  await writeFile(path, `#!/bin/sh\n${body}\n`, "utf8");
  await chmod(path, 0o755);
  return path;
}

async function runGit(directory: string, args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", directory, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
  }
  return stdout.trim();
}

async function createRepo(root: string): Promise<string> {
  const repo = join(root, "project");
  await mkdir(repo);
  await writeFile(join(repo, "file.txt"), "hello\n", "utf8");
  await runGit(repo, ["init", "-q", "-b", "main"]);
  await runGit(repo, ["add", "."]);
  await runGit(repo, ["-c", "user.email=test@example.com", "-c", "user.name=Fixture", "commit", "-q", "-m", "init"]);
  return repo;
}
async function createRepoWithRelease(root: string): Promise<string> {
  const repo = await createRepo(root);
  await runGit(repo, ["switch", "-q", "-c", "release"]);
  await writeFile(join(repo, "file.txt"), "from release\n", "utf8");
  await runGit(repo, ["add", "."]);
  await runGit(repo, ["-c", "user.email=test@example.com", "-c", "user.name=Fixture", "commit", "-q", "-m", "release"]);
  await runGit(repo, ["switch", "-q", "main"]);
  return repo;
}

/** Reads the argv a stub recorded for its last invocation. */
async function recordedArgv(stub: string): Promise<string[]> {
  return (await readFile(`${stub}.argv`, "utf8")).split("\n").filter((line) => line.length > 0);
}

/** A worktree stub set where wt is unavailable, so creation exercises the omp fallback. */
const NOWT = ["/nonexistent/wt"];

test("worktreeSlug keeps ASCII names readable and empties non-ASCII ones", () => {
  expect(worktreeSlug("Fix Login Bug")).toBe("fix-login-bug");
  expect(worktreeSlug("  Spaced   name! ")).toBe("spaced-name");
  expect(worktreeSlug("a".repeat(40))).toBe("a".repeat(32));
  expect(worktreeSlug("修复登录")).toBe("");
  expect(worktreeSlug("emoji 🎉")).toBe("emoji");
});

test("worktreeCandidates prefer the thread name and keep the thread id as fallback", () => {
  expect(worktreeCandidates("/repo", "fix-login", "123456789012345")).toEqual([
    { path: "/repo.fix-login", branch: "fix-login" },
    { path: "/repo.fix-login-123456789012345", branch: "fix-login-123456789012345" },
  ]);
});

test("the Worktrunk probe accepts any successful version and reports a missing cli", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const candidates = [
      { name: "wt-newer", output: `echo "wt v0.80.0"` },
      { name: "wt-older", output: `echo "wt v0.75.1"` },
      { name: "wt-opaque", output: `echo "Worktrunk build without a semver"` },
    ];
    for (const { name, output } of candidates) {
      const command = await writeStub(root, output, name);
      const probe = createWorktreePort({
        command: "/nonexistent/omp",
        worktrunkCandidates: [command],
      });
      await expect(probe.worktrunk()).resolves.toMatchObject({
        available: true,
        command,
      });
    }

    const missing = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: NOWT });
    await expect(missing.worktrunk()).resolves.toEqual({ available: false, reason: "missing" });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("creates the worktree through wt at the agreed directory and branch", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const omp = await writeStub(root, TRANSLATING_STUB);
    const port = createWorktreePort({ command: omp, worktrunkCandidates: [wt] });

    const workspace = await port.create({
      baseDir: repo,
      label: "Fix Login Bug",
      threadId: "123456789012345",
    });
    const expected = join(root, "project.fix-login-bug");
    expect(workspace).toEqual({ cwd: await realpath(expected), branch: "fix-login-bug" });
    expect(await readFile(join(expected, "file.txt"), "utf8")).toBe("hello\n");
    expect(await recordedArgv(wt)).toEqual([
      "switch",
      "--create",
      "fix-login-bug",
      "--no-cd",
      "--format",
      "json",
      "--config-set",
      `worktree-path="${expected}"`,
    ]);
    // The fallback executable is never touched while wt works.
    expect(existsSync(`${omp}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("creates at an explicit shared project path from the requested base and reports the plan before Git", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepoWithRelease(root);
    const mainHead = await runGit(repo, ["rev-parse", "HEAD"]);
    const entry = join(root, "thread-workspaces", "thread-123");
    await mkdir(entry, { recursive: true });
    const explicitPath = join(entry, "project alpha");
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const planned: Array<{ cwd: string; branch: string; repo: string }> = [];
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const workspace = await port.create({
      baseDir: repo,
      label: "Shared Project",
      threadId: "123456789012345",
      path: explicitPath,
      base: "release",
      onPlanned: (plan) => {
        planned.push(plan);
        expect(existsSync(plan.cwd)).toBe(false);
      },
    });

    const releaseHead = await runGit(repo, ["rev-parse", "release"]);
    expect(workspace).toMatchObject({ cwd: explicitPath, branch: "shared-project" });
    expect(await readFile(join(explicitPath, "file.txt"), "utf8")).toBe("from release\n");
    expect(await runGit(workspace.cwd, ["rev-parse", "HEAD"])).toBe(releaseHead);
    expect(planned).toEqual([
      { cwd: explicitPath, branch: "shared-project", repo: await realpath(repo) },
    ]);
    expect(await recordedArgv(wt)).toEqual([
      "switch",
      "--create",
      "shared-project",
      "--base",
      "release",
      "--no-cd",
      "--format",
      "json",
      "--config-set",
      `worktree-path="${explicitPath}"`,
    ]);
    expect(await runGit(repo, ["branch", "--show-current"])).toBe("main");
    expect(await runGit(repo, ["rev-parse", "HEAD"])).toBe(mainHead);
    expect(await readFile(join(repo, "file.txt"), "utf8")).toBe("hello\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("passes the requested base to the real-git OMP fallback", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepoWithRelease(root);
    const mainHead = await runGit(repo, ["rev-parse", "HEAD"]);
    const omp = await writeStub(root, TRANSLATING_STUB);
    const port = createWorktreePort({ command: omp, worktrunkCandidates: NOWT });

    const workspace = await port.create({
      baseDir: repo,
      label: "Fallback Base",
      threadId: "123456789012345",
      base: "release",
    });
    const expectedPath = join(root, "project.fallback-base");
    const releaseHead = await runGit(repo, ["rev-parse", "release"]);
    expect(workspace).toMatchObject({ cwd: expectedPath, branch: "fallback-base" });
    expect(await readFile(join(expectedPath, "file.txt"), "utf8")).toBe("from release\n");
    expect(await runGit(expectedPath, ["rev-parse", "HEAD"])).toBe(releaseHead);
    expect(await recordedArgv(omp)).toEqual([
      "worktree",
      "add",
      expectedPath,
      "release",
      "-b",
      "fallback-base",
      "-C",
      repo,
      "-q",
    ]);
    expect(await runGit(repo, ["branch", "--show-current"])).toBe("main");
    expect(await runGit(repo, ["rev-parse", "HEAD"])).toBe(mainHead);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("steps aside to the thread id and falls back to omp when wt is missing", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const port = createWorktreePort({
      command: await writeStub(root, TRANSLATING_STUB),
      worktrunkCandidates: NOWT,
    });

    await expect(
      port.create({ baseDir: repo, label: "Fix Login Bug", threadId: "123456789012345" }),
    ).resolves.toEqual({
      cwd: join(root, "project.fix-login-bug"),
      branch: "fix-login-bug",
    });
    expect(await readFile(join(root, "project.fix-login-bug", "file.txt"), "utf8")).toBe("hello\n");

    // A branch of the same name belongs to someone else: the thread keeps the numeric fallback.
    await runGit(repo, ["branch", "second"]);
    await expect(
      port.create({ baseDir: repo, label: "Second", threadId: "111222333444555" }),
    ).resolves.toEqual({
      cwd: join(root, "project.second-111222333444555"),
      branch: "second-111222333444555",
    });

    // So does an occupied directory.
    await mkdir(join(root, "project.occupied"));
    await expect(
      port.create({ baseDir: repo, label: "occupied", threadId: "222333444555666" }),
    ).resolves.toEqual({
      cwd: join(root, "project.occupied-222333444555666"),
      branch: "occupied-222333444555666",
    });

    // Non-ASCII names cannot name a workspace, so they are refused instead of renamed.
    await expect(
      port.create({ baseDir: repo, label: "修复登录", threadId: "987654321098765" }),
    ).rejects.toThrow(/ASCII/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("same-named worktrees stay isolated across separate source repositories", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const firstParent = join(root, "first");
    const secondParent = join(root, "second");
    await mkdir(firstParent);
    await mkdir(secondParent);
    const firstRepo = await createRepo(firstParent);
    const secondRepo = await createRepo(secondParent);
    await writeFile(join(firstRepo, "source.txt"), "first repository\n", "utf8");
    await runGit(firstRepo, ["add", "."]);
    await runGit(firstRepo, ["-c", "user.email=test@example.com", "-c", "user.name=Fixture", "commit", "-q", "-m", "first source"]);
    await writeFile(join(secondRepo, "source.txt"), "second repository\n", "utf8");
    await runGit(secondRepo, ["add", "."]);
    await runGit(secondRepo, ["-c", "user.email=test@example.com", "-c", "user.name=Fixture", "commit", "-q", "-m", "second source"]);
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const planned: Array<{ cwd: string; branch: string; repo: string }> = [];

    const [first, second] = await Promise.all([
      port.create({
        baseDir: firstRepo,
        label: "Same Project",
        threadId: "111222333444555",
        onPlanned: (plan) => planned.push(plan),
      }),
      port.create({
        baseDir: secondRepo,
        label: "Same Project",
        threadId: "222333444555666",
        onPlanned: (plan) => planned.push(plan),
      }),
    ]);

    expect(first).toMatchObject({ cwd: join(firstParent, "project.same-project"), branch: "same-project" });
    expect(second).toMatchObject({ cwd: join(secondParent, "project.same-project"), branch: "same-project" });
    expect(await readFile(join(first.cwd, "source.txt"), "utf8")).toBe("first repository\n");
    expect(await readFile(join(second.cwd, "source.txt"), "utf8")).toBe("second repository\n");
    expect(await runGit(first.cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).toBe(
      await runGit(firstRepo, ["rev-parse", "--path-format=absolute", "--git-common-dir"]),
    );
    expect(await runGit(second.cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).toBe(
      await runGit(secondRepo, ["rev-parse", "--path-format=absolute", "--git-common-dir"]),
    );
    expect(await runGit(firstRepo, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).not.toBe(
      await runGit(secondRepo, ["rev-parse", "--path-format=absolute", "--git-common-dir"]),
    );
    expect(planned).toHaveLength(2);
    expect(planned.map((plan) => plan.repo).sort()).toEqual(
      [await realpath(firstRepo), await realpath(secondRepo)].sort(),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("concurrent creation in one repository reserves distinct branches and paths", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const delayedCreate = WT_SWITCH_STUB.replace(
      'git worktree add -q -b "$branch" "$path" "$base"',
      'sleep 0.15\ngit worktree add -q -b "$branch" "$path" "$base"',
    );
    const wt = await writeStub(root, delayedCreate, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const planned: Array<{ cwd: string; branch: string; repo: string }> = [];

    const workspaces = await Promise.all([
      port.create({
        baseDir: repo,
        label: "Concurrent",
        threadId: "111222333444555",
        onPlanned: (plan) => planned.push(plan),
      }),
      port.create({
        baseDir: repo,
        label: "Concurrent",
        threadId: "222333444555666",
        onPlanned: (plan) => planned.push(plan),
      }),
    ]);
    const paths = workspaces.map((workspace) => workspace.cwd).sort();
    const branches = workspaces.map((workspace) => workspace.branch).sort();
    const primaryPath = join(root, "project.concurrent");

    expect(paths).toContain(primaryPath);
    expect(paths).toHaveLength(2);
    expect(new Set(paths).size).toBe(2);
    expect(new Set(branches).size).toBe(2);
    expect(
      paths.filter((path) => path !== primaryPath).every((path) =>
        [join(root, "project.concurrent-111222333444555"), join(root, "project.concurrent-222333444555666")].includes(path),
      ),
    ).toBe(true);
    for (const workspace of workspaces) {
      expect(existsSync(workspace.cwd)).toBe(true);
      expect(await runGit(workspace.cwd, ["rev-parse", "--show-toplevel"])).toBe(workspace.cwd);
      expect(await runGit(workspace.cwd, ["branch", "--show-current"])).toBe(workspace.branch);
    }
    expect(planned.map((plan) => plan.cwd).sort()).toEqual(paths);
    expect(planned.map((plan) => plan.branch).sort()).toEqual(branches);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a failed wt operation does not silently fall back to omp", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.75.1"; exit 0; fi
printf '%s\\n' "$@" > "$0.argv"
echo "worktrunk command failed" >&2
exit 23`,
      "wt-fails",
    );
    const omp = await writeStub(root, TRANSLATING_STUB);
    const port = createWorktreePort({ command: omp, worktrunkCandidates: [wt] });

    const failure = await port
      .create({ baseDir: repo, label: "failed wt", threadId: "123456789012345" })
      .then(() => "")
      .catch((error: Error) => error.message);
    expect(failure).toContain("退出码 23");
    expect(failure).toContain("worktrunk command failed");
    expect((await recordedArgv(wt)).slice(0, 2)).toEqual(["switch", "--create"]);
    expect(existsSync(`${omp}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("reports a failed command with a bounded message and a hanging one by its timeout", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const failing = await writeStub(root, "yes boom | head -c 500 >&2\nexit 3");
    const port = createWorktreePort({ command: failing, worktrunkCandidates: NOWT });

    await expect(
      port.create({ baseDir: root, label: "outside", threadId: "123456789012345" }),
    ).rejects.toThrow(/不是 git 仓库/);
    const failure = await port
      .create({ baseDir: repo, label: "failing", threadId: "123456789012345" })
      .then(() => "")
      .catch((error: Error) => error.message);
    expect(failure).toContain("退出码 3");
    expect(failure).toContain("boom");
    expect(failure.length).toBeLessThanOrEqual(220);

    const hanging = await writeStub(root, "exec sleep 30");
    const slow = createWorktreePort({
      command: hanging,
      worktrunkCandidates: NOWT,
      createTimeoutMs: 150,
    });
    await expect(
      slow.create({ baseDir: repo, label: "slow", threadId: "123456789012345" }),
    ).rejects.toThrow(/超时/);

    const controller = new AbortController();
    controller.abort();
    await expect(
      slow.create({ baseDir: repo, label: "aborted", threadId: "123456789012345" }, controller.signal),
    ).rejects.toThrow(/已取消/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge maps its parameters onto the wt pipeline and returns the wt result", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    await runGit(repo, ["worktree", "add", "-q", join(root, "linked"), "-b", "linked-branch"]);
    const wt = await writeStub(root, WT_MERGE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const summary = await port.merge({
      cwd: join(root, "linked"),
      target: "release-1",
      squash: false,
      remove: false,
      ff: false,
      stage: "tracked",
    });
    expect(summary).toEqual({
      branch: "b",
      target: "main",
      squashed: true,
      rebased: true,
      committed: false,
      removed: false,
      commit: await runGit(repo, ["rev-parse", "refs/heads/main"]),
    });
    expect(await recordedArgv(wt)).toEqual([
      "merge",
      "--format",
      "json",
      "release-1",
      "--no-squash",
      "--no-remove",
      "--no-ff",
      "--stage",
      "tracked",
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("non-fast-forward merge reports the real target ref commit", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const linked = join(root, "feature-worktree");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "feature"]);
    await writeFile(join(linked, "feature.txt"), "feature change\n", "utf8");
    await runGit(linked, ["add", "."]);
    await runGit(linked, [
      "-c",
      "user.email=test@example.com",
      "-c",
      "user.name=Fixture",
      "commit",
      "-q",
      "-m",
      "feature",
    ]);
    const featureHead = await runGit(linked, ["rev-parse", "HEAD"]);
    const wt = await writeStub(root, WT_REAL_MERGE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const summary = await port.merge({ cwd: linked, target: "main", ff: false, remove: false });
    const targetHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    expect(summary.commit).toBe(targetHead);
    expect(summary.commit).not.toBe(featureHead);
    expect((await runGit(repo, ["rev-list", "--parents", "-n", "1", targetHead])).split(" ")).toHaveLength(3);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge never falls back without wt and refuses the main worktree", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const withoutWt = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: NOWT,
    });
    await expect(withoutWt.merge({ cwd: repo })).rejects.toThrow(/没有可用的 Worktrunk/);
    await expect(withoutWt.merge({ cwd: join(root, "does-not-exist") })).rejects.toThrow(
      /没有可用的 Worktrunk/,
    );

    const wt = await writeStub(root, WT_MERGE_STUB, "wt");
    const withWt = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    await expect(withWt.merge({ cwd: repo })).rejects.toThrow(/不是独立的 git worktree/);
    await expect(
      withWt.merge({ cwd: join(root, "does-not-exist") }),
    ).rejects.toThrow(/已不存在/);
    expect(existsSync(`${wt}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("removes through wt when available and reports the branch outcome", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const worktree = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", worktree, "-b", "linked-branch"]);
    const wt = await writeStub(root, WT_REMOVE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(port.remove({ path: worktree, force: false })).resolves.toEqual({
      kind: "removed",
      via: "wt",
      branchOutcome: "deleted",
    });
    expect(await recordedArgv(wt)).toEqual([
      "remove",
      "--foreground",
      "--format",
      "json",
      worktree,
    ]);
    expect(existsSync(worktree)).toBe(false);

    // A workspace that is already gone reports itself instead of spawning anything.
    await expect(port.remove({ path: worktree, force: true })).resolves.toEqual({ kind: "missing" });
    expect(await recordedArgv(wt)).toHaveLength(5);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("removal falls back to git without wt and force discards uncommitted changes", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const clean = join(root, "clean");
    await runGit(repo, ["worktree", "add", "-q", clean, "-b", "clean-branch"]);
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: NOWT });

    await expect(port.remove({ path: clean, force: false })).resolves.toEqual({
      kind: "removed",
      via: "git",
      branchOutcome: null,
    });
    expect(existsSync(clean)).toBe(false);

    const dirty = join(root, "dirty");
    await runGit(repo, ["worktree", "add", "-q", dirty, "-b", "dirty-branch"]);
    await writeFile(join(dirty, "scratch.txt"), "wip\n", "utf8");
    const failure = await port
      .remove({ path: dirty, force: false })
      .then(() => "")
      .catch((error: Error) => error.message);
    expect(failure).toContain("untracked");
    expect(failure.length).toBeLessThanOrEqual(220);
    expect(existsSync(join(dirty, "scratch.txt"))).toBe(true);

    await expect(port.remove({ path: dirty, force: true })).resolves.toEqual({
      kind: "removed",
      via: "git",
      branchOutcome: null,
    });
    expect(existsSync(dirty)).toBe(false);

    // The main checkout is never removable.
    await expect(port.remove({ path: repo, force: true })).rejects.toThrow(/main working tree/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("refuses wt removal when the worktree belongs to another source repository", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const sourceParent = join(root, "source");
    const unrelatedParent = join(root, "unrelated");
    await mkdir(sourceParent);
    await mkdir(unrelatedParent);
    const repo = await createRepo(sourceParent);
    const unrelatedRepo = await createRepo(unrelatedParent);
    const worktree = join(root, "owned-worktree");
    await runGit(repo, ["worktree", "add", "-q", worktree, "-b", "owned-branch"]);
    const wt = await writeStub(root, WT_REMOVE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(
      port.remove({ path: worktree, force: true, repo: unrelatedRepo, branch: "owned-branch" }),
    ).rejects.toThrow();
    expect(existsSync(worktree)).toBe(true);
    expect(await runGit(worktree, ["branch", "--show-current"])).toBe("owned-branch");
    expect(await readFile(join(worktree, "file.txt"), "utf8")).toBe("hello\n");
    expect(existsSync(`${wt}.argv`)).toBe(false);

    await expect(
      port.remove({ path: worktree, force: true, repo, branch: "owned-branch" }),
    ).resolves.toEqual({ kind: "removed", via: "wt", branchOutcome: "deleted" });
    expect(existsSync(worktree)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("refuses Git fallback removal when the checked-out branch differs from its owner", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const worktree = join(root, "owned-worktree");
    await runGit(repo, ["worktree", "add", "-q", worktree, "-b", "owned-branch"]);
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: NOWT });

    await expect(
      port.remove({ path: worktree, force: true, repo, branch: "different-branch" }),
    ).rejects.toThrow();
    expect(existsSync(worktree)).toBe(true);
    expect(await runGit(worktree, ["branch", "--show-current"])).toBe("owned-branch");
    expect(await readFile(join(worktree, "file.txt"), "utf8")).toBe("hello\n");

    await expect(
      port.remove({ path: worktree, force: true, repo, branch: "owned-branch" }),
    ).resolves.toEqual({ kind: "removed", via: "git", branchOutcome: null });
    expect(existsSync(worktree)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("refuses forced wt removal when the primary checkout matches the claimed owner", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(root, WT_REMOVE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(
      port.remove({ path: repo, force: true, repo, branch: "main" }),
    ).rejects.toThrow();
    expect(existsSync(repo)).toBe(true);
    expect(await runGit(repo, ["branch", "--show-current"])).toBe("main");
    expect(await readFile(join(repo, "file.txt"), "utf8")).toBe("hello\n");
    expect(existsSync(`${wt}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("refuses removal through a symlink replacing the recorded worktree path", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const recordedPath = join(root, "recorded-worktree");
    const relocatedPath = join(root, "relocated-worktree");
    await runGit(repo, ["worktree", "add", "-q", recordedPath, "-b", "owned-branch"]);
    await runGit(repo, ["worktree", "move", recordedPath, relocatedPath]);
    await symlink(relocatedPath, recordedPath);
    const wt = await writeStub(root, WT_REMOVE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(
      port.remove({ path: recordedPath, force: true, repo, branch: "owned-branch" }),
    ).rejects.toThrow();
    expect(existsSync(recordedPath)).toBe(true);
    expect(await realpath(recordedPath)).toBe(relocatedPath);
    expect(existsSync(relocatedPath)).toBe(true);
    expect(await runGit(relocatedPath, ["branch", "--show-current"])).toBe("owned-branch");
    expect(existsSync(`${wt}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
