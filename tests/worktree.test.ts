import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createWorktreePort, worktreeCandidates, worktreeSlug } from "../src/workspace/worktree";

/**
 * The port shells out to the OMP executable and to Worktrunk, so these tests use executable stubs
 * that accept the same argv and perform the real git work; production runs `omp worktree add`
 * (fallback) and `wt switch --create` / `wt merge` / `wt remove` (preferred).
 */
const TRANSLATING_STUB = String.raw`set -e
[ "$1" = "worktree" ] && [ "$2" = "add" ] || exit 64
[ "$4" = "-b" ] && [ "$6" = "-C" ] || exit 64
exec git -C "$7" worktree add "$3" -b "$5" -q`;

const WT_SWITCH_STUB = String.raw`set -e
if [ "$1" = "--version" ]; then echo "wt v0.76.0"; exit 0; fi
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "switch" ] && [ "$2" = "--create" ] && [ "$4" = "--no-cd" ] || exit 64
branch="$3"
config=""
for arg in "$@"; do
  case "$arg" in worktree-path=*) config="$arg" ;; esac
done
[ -n "$config" ] || exit 65
path=$(printf '%s' "$config" | sed -e 's/^worktree-path=//' -e 's/^"//' -e 's/"$//')
git worktree add "$path" -b "$branch" -q
printf '{"action":"created","branch":"%s","path":"%s"}\n' "$branch" "$path"`;

const WT_MERGE_STUB = String.raw`if [ "$1" = "--version" ]; then echo "wt v0.76.0"; exit 0; fi
printf '%s\n' "$@" > "$0.argv"
[ "$1" = "merge" ] || exit 64
printf '{"branch":"b","target":"main","squashed":true,"rebased":true,"committed":false,"removed":false}\n'`;

const WT_REMOVE_STUB = String.raw`set -e
if [ "$1" = "--version" ]; then echo "wt v0.76.0"; exit 0; fi
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
git -C "$path" worktree remove $force "$path"
printf '[{"branch":"b","path":"%s","kind":"worktree","branch_outcome":"deleted"}]\n' "$path"`;

async function writeStub(directory: string, body: string, name = "stub-omp"): Promise<string> {
  const path = join(directory, name);
  await writeFile(path, `#!/bin/sh\n${body}\n`, "utf8");
  await chmod(path, 0o755);
  return path;
}

async function runGit(directory: string, args: string[]): Promise<void> {
  const child = Bun.spawn(["git", "-C", directory, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stderr = await new Response(child.stderr).text();
  if ((await child.exited) !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
  }
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

test("the worktrunk probe takes v0.76.x, names an unsupported version, and reports a missing cli", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const stable = await writeStub(root, `echo "wt v0.76.0"`, "wt-stable");
    const old = await writeStub(root, `echo "wt v0.75.1"`, "wt-old");
    const opaque = await writeStub(root, `echo "not a version"`, "wt-unknown");

    const available = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [old, opaque, stable],
    });
    await expect(available.worktrunk()).resolves.toEqual({
      available: true,
      command: stable,
      version: "v0.76.0",
    });

    const incompatible = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [opaque, old],
    });
    await expect(incompatible.worktrunk()).resolves.toEqual({
      available: false,
      reason: "incompatible",
      command: old,
      version: "v0.75.1",
    });

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

test("an incompatible wt still leaves creation on the omp fallback", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const old = await writeStub(root, `printf '%s\\n' "$@" > "$0.argv"\necho "wt v0.75.1"`, "wt-old");
    const port = createWorktreePort({
      command: await writeStub(root, TRANSLATING_STUB),
      worktrunkCandidates: [old],
    });

    await expect(
      port.create({ baseDir: repo, label: "old wt", threadId: "123456789012345" }),
    ).resolves.toEqual({
      cwd: join(root, "project.old-wt"),
      branch: "old-wt",
    });
    // The probe ran `--version`; the old wt was never asked to create anything.
    expect(await recordedArgv(old)).toEqual(["--version"]);
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

    await expect(
      port.merge({
        cwd: join(root, "linked"),
        target: "release-1",
        squash: false,
        remove: false,
        ff: false,
        stage: "tracked",
      }),
    ).resolves.toEqual({
      branch: "b",
      target: "main",
      squashed: true,
      rebased: true,
      committed: false,
      removed: false,
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
