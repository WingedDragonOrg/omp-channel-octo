import { expect, test } from "bun:test";
import type { WorktreeMergeLifecycle } from "../src/contracts.ts";
import { existsSync, watch } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { WorktreeMergeExecutionError } from "../src/threads/thread-merge";
import { createWorktreePort } from "../src/workspace/worktree";


import { groupAlive, terminateProcessGroup, waitForGroupGone } from "../src/omp/process.ts";
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
const WT_PLAN_STUB = String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
if [ "$1" = "config" ]; then
  printf '%s\n' "$@" > "$0.default-argv"
  [ "$2" = "state" ] && [ "$3" = "default-branch" ] || exit 64
  printf 'main\n'
  exit 0
fi
exit 64`;
const WT_PLAN_FAIL_STUB = String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
if [ "$1" = "config" ]; then
  printf '%s\n' "$@" > "$0.default-argv"
  [ "$2" = "state" ] && [ "$3" = "default-branch" ] || exit 64
  exit 19
fi
exit 64`;



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
async function waitForFileCreated(path: string, timeoutMs: number): Promise<void> {
  if (existsSync(path)) return;
  const watcher = watch(dirname(path));
  const timeout = AbortSignal.timeout(timeoutMs);
  try {
    while (!existsSync(path)) await once(watcher, "change", { signal: timeout });
  } catch (error) {
    if (timeout.aborted) {
      throw new Error(`timed out waiting for ${basename(path)} after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    watcher.close();
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

test("creates a worktree with every word in a long readable task name", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const slug = "complete-human-readable-worktree-names-preserve-every-meaningful-word";

    const workspace = await port.create({
      baseDir: repo,
      label: "Complete human readable worktree names preserve every meaningful word",
      threadId: "123456789012345",
    });

    expect(workspace).toEqual({ cwd: join(root, `project.${slug}`), branch: slug });
    expect(await runGit(workspace.cwd, ["branch", "--show-current"])).toBe(slug);
    expect(await readFile(join(workspace.cwd, "file.txt"), "utf8")).toBe("hello\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("rejects task names over 100 normalized characters before planning or allocating resources", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const omp = await writeStub(root, TRANSLATING_STUB);
    const port = createWorktreePort({ command: omp, worktrunkCandidates: NOWT });
    let planned = false;

    await expect(
      port.create({
        baseDir: repo,
        label: "feature feature feature feature feature feature feature feature feature feature feature feature feature",
        threadId: "123456789012345",
        onPlanned: () => {
          planned = true;
        },
      }),
    ).rejects.toThrow(/100/);

    expect(planned).toBe(false);
    expect(await runGit(repo, ["for-each-ref", "--format=%(refname:short)", "refs/heads/"])).toBe("main");
    expect(existsSync(join(root, "project.feature-feature-feature-feature-feature-feature-feature-feature-feature-feature-feature-feature-feature"))).toBe(false);
    expect(existsSync(`${omp}.argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("creates an exact 100-character slug and preserves its full base through collision suffixes", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const slug = "a".repeat(100);
    expect(slug).toHaveLength(100);

    const workspaces = [];
    for (let index = 0; index < 3; index += 1) {
      workspaces.push(
        await port.create({
          baseDir: repo,
          label: slug,
          threadId: `12345678901234${index}`,
        }),
      );
    }

    expect(workspaces.map(({ cwd, branch }) => ({ cwd, branch }))).toEqual([
      { cwd: join(root, `project.${slug}`), branch: slug },
      { cwd: join(root, `project.${slug}-2`), branch: `${slug}-2` },
      { cwd: join(root, `project.${slug}-3`), branch: `${slug}-3` },
    ]);
    for (const workspace of workspaces) {
      expect(await runGit(workspace.cwd, ["branch", "--show-current"])).toBe(workspace.branch);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
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
test("merge planning resolves wt's default branch to its actual checked-out worktree", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const source = join(root, "source");
    await runGit(repo, ["worktree", "add", "-q", source, "-b", "source-branch"]);
    const wt = await writeStub(root, WT_PLAN_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const plan = await port.planMerge({ cwd: source, repo, branch: "source-branch" });

    expect(plan).toEqual({ target: "main", cwds: [await realpath(source), await realpath(repo)] });
    expect((await readFile(`${wt}.default-argv`, "utf8")).trim().split("\n")).toEqual([
      "config",
      "state",
      "default-branch",
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge planning maps an explicit refs/heads target to its distinct linked checkout", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const source = join(root, "source");
    const targetCheckout = join(root, "target checkout");
    await runGit(repo, ["worktree", "add", "-q", source, "-b", "source-branch"]);
    await runGit(repo, ["worktree", "add", "-q", targetCheckout, "-b", "release-target"]);
    const wt = await writeStub(root, WT_PLAN_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const plan = await port.planMerge({
      cwd: source,
      repo,
      branch: "source-branch",
      target: "refs/heads/release-target",
    });

    expect(plan).toEqual({
      target: "refs/heads/release-target",
      cwds: [await realpath(source), await realpath(targetCheckout)],
    });
    expect(existsSync(`${wt}.default-argv`)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge planning includes no fabricated checkout for an unchecked-out target branch", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const source = join(root, "source");
    await runGit(repo, ["worktree", "add", "-q", source, "-b", "source-branch"]);
    await runGit(repo, ["branch", "unlinked-target"]);
    const wt = await writeStub(root, WT_PLAN_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const plan = await port.planMerge({
      cwd: source,
      repo,
      branch: "source-branch",
      target: "unlinked-target",
    });

    expect(plan).toEqual({ target: "unlinked-target", cwds: [await realpath(source)] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge planning rejects a failed wt default-branch lookup instead of returning source-only", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const source = join(root, "source");
    await runGit(repo, ["worktree", "add", "-q", source, "-b", "source-branch"]);
    const wt = await writeStub(root, WT_PLAN_FAIL_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(
      port.planMerge({ cwd: source, repo, branch: "source-branch" }),
    ).rejects.toThrow();
    expect((await readFile(`${wt}.default-argv`, "utf8")).trim().split("\n")).toEqual([
      "config",
      "state",
      "default-branch",
    ]);
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

test("keeps explicit shared paths across branch conflicts and suffixes actual path conflicts", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const wt = await writeStub(root, WT_SWITCH_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const entry = join(root, "thread-workspaces", "thread-123");
    const requestedPath = join(entry, "project alpha");
    await mkdir(entry, { recursive: true });
    await runGit(repo, ["branch", "shared-project"]);
    const planned: Array<{ cwd: string; branch: string; repo: string }> = [];

    const workspaces = [];
    for (let index = 0; index < 3; index += 1) {
      workspaces.push(
        await port.create({
          baseDir: repo,
          label: "Shared Project",
          threadId: `12345678901234${index}`,
          path: requestedPath,
          onPlanned: (plan) => planned.push(plan),
        }),
      );
    }

    expect(workspaces.map(({ cwd, branch }) => ({ cwd, branch }))).toEqual([
      { cwd: requestedPath, branch: "shared-project-2" },
      { cwd: `${requestedPath}-2`, branch: "shared-project-3" },
      { cwd: `${requestedPath}-3`, branch: "shared-project-4" },
    ]);
    expect(planned.map(({ cwd, branch }) => ({ cwd, branch }))).toEqual(
      workspaces.map(({ cwd, branch }) => ({ cwd, branch })),
    );
    expect(planned.every((plan) => plan.repo === repo)).toBe(true);
    for (const workspace of workspaces) {
      expect(await runGit(workspace.cwd, ["branch", "--show-current"])).toBe(workspace.branch);
    }
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

test("allocates readable numeric suffixes for real branch and path conflicts", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const port = createWorktreePort({
      command: await writeStub(root, TRANSLATING_STUB),
      worktrunkCandidates: NOWT,
    });

    await runGit(repo, ["branch", "branch-collision"]);
    const branchConflict = await port.create({
      baseDir: repo,
      label: "Branch Collision",
      threadId: "111222333444555",
    });
    expect(branchConflict).toEqual({
      cwd: join(root, "project.branch-collision"),
      branch: "branch-collision-2",
    });

    await mkdir(join(root, "project.path-collision"));
    const pathConflict = await port.create({
      baseDir: repo,
      label: "Path Collision",
      threadId: "222333444555666",
    });
    expect(pathConflict).toEqual({
      cwd: join(root, "project.path-collision-2"),
      branch: "path-collision",
    });

    await mkdir(join(root, "project.repeated-collision"));
    await mkdir(join(root, "project.repeated-collision-2"));
    const repeatedPathConflict = await port.create({
      baseDir: repo,
      label: "Repeated Collision",
      threadId: "333444555666777",
    });
    expect(repeatedPathConflict).toEqual({
      cwd: join(root, "project.repeated-collision-3"),
      branch: "repeated-collision",
    });

    await runGit(repo, ["branch", "branch-collision-chain"]);
    await runGit(repo, ["branch", "branch-collision-chain-2"]);
    const repeatedBranchConflict = await port.create({
      baseDir: repo,
      label: "Branch Collision Chain",
      threadId: "444555666777888",
    });
    expect(repeatedBranchConflict).toEqual({
      cwd: join(root, "project.branch-collision-chain"),
      branch: "branch-collision-chain-3",
    });

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

test("concurrent creation in one repository reserves readable distinct branches and paths", async () => {
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

    expect(paths).toEqual([primaryPath, `${primaryPath}-2`].sort());
    expect(branches).toEqual(["concurrent", "concurrent-2"]);
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
test("removed wt merge waits for background worktree cleanup and its process group", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  let runnerPgid = 0;
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    await writeFile(join(linked, "feature.txt"), "merged\n", "utf8");
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
    const cleanupFinished = join(root, "cleanup-finished");
    const wt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
set -e
[ "$1" = "merge" ] || exit 64
worktree=$(pwd -P)
common_dir=$(git rev-parse --path-format=absolute --git-common-dir)
repo=$(dirname "$common_dir")
branch=$(git branch --show-current)
git -C "$repo" merge --ff-only "$branch" >/dev/null
trash_root="$common_dir/wt/trash"
trash="$trash_root/linked"
mkdir -p "$trash_root"
mv "$worktree" "$trash"
mkdir "$worktree"
git -C "$repo" worktree prune --expire now
git -C "$repo" branch -d "$branch" >/dev/null
(
  sleep 0.2
  rmdir "$worktree"
  rm -rf "$trash"
  printf done > "${cleanupFinished}"
) </dev/null >/dev/null 2>&1 &
printf '{"branch":"%s","target":"main","squashed":false,"rebased":false,"committed":false,"removed":true}\\n' "$branch"`,
      "wt-async-cleanup",
    );
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => lifecycleEvents.push("reserved"),
      onSpawn: (identity) => {
        runnerPgid = identity.pgid;
        lifecycleEvents.push("spawned");
      },
      onConfirmedExit: () => {
        expect(existsSync(linked)).toBe(false);
        expect(existsSync(cleanupFinished)).toBe(true);
        expect(groupAlive(runnerPgid)).toBe(false);
        lifecycleEvents.push("exited");
      },
      complete: () => {},
      settle: () => {},
      onNotSpawned: () => lifecycleEvents.push("not-spawned"),
      onUnknown: () => lifecycleEvents.push("unknown"),
    };
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(port.merge({ cwd: linked, lifecycle })).resolves.toMatchObject({ removed: true });

    expect(existsSync(linked)).toBe(false);
    expect(await readFile(cleanupFinished, "utf8")).toBe("done");
    expect(await runGit(repo, ["branch", "--list", "linked-branch"])).toBe("");
    expect(await runGit(repo, ["worktree", "list", "--porcelain"])).not.toContain(`worktree ${linked}`);
    expect(lifecycleEvents).toEqual(["reserved", "spawned", "exited"]);
    expect(runnerPgid).toBeGreaterThan(0);
    expect(groupAlive(runnerPgid)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("timed-out background cleanup is killed and remains an unknown merge outcome", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  let runnerPgid = 0;
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const started = join(root, "cleanup-started");
    const wt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
[ "$1" = "merge" ] || exit 64
(
  trap '' TERM
  printf started > "${started}"
  while :; do sleep 10; done
) </dev/null >/dev/null 2>&1 &
printf '{"branch":"linked-branch","target":"main","squashed":true,"rebased":true,"committed":true,"removed":true}\\n'`,
      "wt-slow-cleanup",
    );
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => lifecycleEvents.push("reserved"),
      onSpawn: (identity) => {
        runnerPgid = identity.pgid;
        lifecycleEvents.push("spawned");
      },
      onConfirmedExit: () => lifecycleEvents.push("exited"),
      complete: () => {},
      settle: () => {},
      onNotSpawned: () => lifecycleEvents.push("not-spawned"),
      onUnknown: () => lifecycleEvents.push("unknown"),
    };
    const port = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [wt],
      mergeTimeoutMs: 500,
    });
    const running = port.merge({ cwd: linked, lifecycle }).catch((error: unknown) => error);
    await waitForFileCreated(started, 5_000);

    const outcome = await running;

    expect(String((outcome as Error).message)).toContain("超时");
    expect(existsSync(linked)).toBe(true);
    expect(lifecycleEvents).toEqual(["reserved", "spawned", "unknown"]);
    expect(runnerPgid).toBeGreaterThan(0);
    expect(groupAlive(runnerPgid)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("only a nonzero wt merge is repairable and its diagnostic is credential-safe", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const failingWt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
echo "CONFLICT token=merge-secret password=hidden-value" >&2
yes x | head -c 1000000 >&2
exit 17`,
      "wt-fails",
    );
    const failedPort = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [failingWt],
    });
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: (cwd) => lifecycleEvents.push(`reserve:${cwd}`),
      onSpawn: (identity) => lifecycleEvents.push(`spawn:${identity.pid}:${identity.pgid}`),
      onConfirmedExit: () => lifecycleEvents.push("exit"),
      complete: (outcome) => {
        lifecycleEvents.push(`outcome:${outcome.kind}`);
      },
      settle: () => lifecycleEvents.push("settle"),
      onNotSpawned: () => lifecycleEvents.push("not-spawned"),
      onUnknown: () => lifecycleEvents.push("unknown"),
    };
    const failure = await failedPort
      .merge({ cwd: linked, lifecycle })
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(WorktreeMergeExecutionError);
    expect((failure as WorktreeMergeExecutionError).exitCode).toBe(17);
    expect((failure as WorktreeMergeExecutionError).diagnostic).toContain("CONFLICT");
    expect((failure as WorktreeMergeExecutionError).diagnostic).toContain("[redacted]");
    expect((failure as WorktreeMergeExecutionError).diagnostic).not.toContain("merge-secret");
    expect((failure as WorktreeMergeExecutionError).diagnostic).not.toContain("hidden-value");
    expect((failure as Error).message.length).toBeLessThanOrEqual(230);
    expect(lifecycleEvents).toHaveLength(3);
    expect(lifecycleEvents[0]).toBe(`reserve:${linked}`);
    expect(lifecycleEvents[1]).toMatch(/^spawn:\d+:\d+$/u);
    expect(lifecycleEvents[2]).toBe("exit");

    const malformedWt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
echo "not-json"`,
      "wt-malformed",
    );
    const malformedPort = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [malformedWt],
    });
    const malformed = await malformedPort.merge({ cwd: linked }).catch((error: unknown) => error);
    expect(malformed).not.toBeInstanceOf(WorktreeMergeExecutionError);

    const noWt = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: NOWT,
    });
    const unavailable = await noWt.merge({ cwd: linked }).catch((error: unknown) => error);
    expect(unavailable).not.toBeInstanceOf(WorktreeMergeExecutionError);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("wt output cannot forge the parked-shell not-started marker", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const wt = await writeStub(
      root,
      `if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
echo "__omp_worktrunk_not_started__" >&2
exit 18`,
      "wt-marker",
    );
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => lifecycleEvents.push("reserved"),
      onSpawn: () => lifecycleEvents.push("spawned"),
      onConfirmedExit: () => lifecycleEvents.push("exited"),
      complete: () => {},
      settle: () => {},
      onNotSpawned: () => lifecycleEvents.push("not-spawned"),
      onUnknown: () => lifecycleEvents.push("unknown"),
    };
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const outcome = await port.merge({ cwd: linked, lifecycle }).catch((error: unknown) => error);

    expect(outcome).toBeInstanceOf(WorktreeMergeExecutionError);
    expect((outcome as WorktreeMergeExecutionError).exitCode).toBe(18);
    expect(lifecycleEvents).toEqual(["reserved", "spawned", "exited"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("merge refuses a worktree whose recorded repository, branch, or path identity changed", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "owned-branch"]);
    const otherRoot = join(root, "other");
    await mkdir(otherRoot);
    const otherRepo = await createRepo(otherRoot);
    const wt = await writeStub(root, WT_MERGE_STUB, "wt");
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    await expect(
      port.merge({ cwd: linked, repo: otherRepo, branch: "owned-branch" }),
    ).rejects.toThrow();
    await expect(
      port.merge({ cwd: linked, repo, branch: "different-branch" }),
    ).rejects.toThrow();
    expect(existsSync(`${wt}.argv`)).toBe(false);

    const relocated = join(root, "relocated");
    await runGit(repo, ["worktree", "move", linked, relocated]);
    await symlink(relocated, linked);
    await expect(
      port.merge({ cwd: linked, repo, branch: "owned-branch" }),
    ).rejects.toThrow();
    expect(existsSync(`${wt}.argv`)).toBe(false);
    expect(existsSync(relocated)).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("cancelling wt merge waits until its hook child can no longer mutate the worktree", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const started = join(root, "merge-started");
    const hookEffect = join(root, "hook-effect");
    // The hook child announces that it has started before its delayed write; cancellation must
    // wait until the whole process group and its inherited output pipes have drained.
    const wt = await writeStub(
      root,
      String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
(
  printf started > "${started}"
  sleep 0.25
  printf survived > "${hookEffect}"
) &
while :; do sleep 10; done`,
      "wt-cancel",
    );
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });
    const controller = new AbortController();
    const running = port.merge({ cwd: linked }, controller.signal).catch((error: unknown) => error);
    await waitForFileCreated(started, 5_000);
    expect(existsSync(started)).toBe(true);

    controller.abort();
    const outcome = await running;
    expect(outcome).not.toBeInstanceOf(WorktreeMergeExecutionError);
    expect(existsSync(hookEffect)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("timed-out wt merge kills its TERM-resistant process group before returning", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  let runnerPgid = 0;
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const started = join(root, "merge-started");
    const hookEffect = join(root, "hook-after-timeout");
    const wt = await writeStub(
      root,
      String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
trap '' TERM
printf '%s\n' "$$" > "${started}"
(trap '' TERM; sleep 3; printf survived > "${hookEffect}") &
while :; do sleep 10; done`,
      "wt-timeout",
    );
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => lifecycleEvents.push("reserved"),
      onSpawn: (identity) => {
        runnerPgid = identity.pgid;
        lifecycleEvents.push("spawned");
      },
      onConfirmedExit: () => lifecycleEvents.push("exited"),
      complete: () => {},
      settle: () => {},
      onNotSpawned: () => lifecycleEvents.push("not-spawned"),
      onUnknown: () => lifecycleEvents.push("unknown"),
    };
    const port = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [wt],
      mergeTimeoutMs: 100,
    });

    const outcome = await port.merge({ cwd: linked, lifecycle }).catch((error: unknown) => error);

    expect(outcome).not.toBeInstanceOf(WorktreeMergeExecutionError);
    expect(String((outcome as Error).message)).toContain("超时");
    expect(lifecycleEvents).toEqual(["reserved", "spawned", "exited"]);
    expect(existsSync(hookEffect)).toBe(false);
    expect(runnerPgid).toBeGreaterThan(0);
    expect(groupAlive(runnerPgid)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("cancellation after durable spawn but before the run handshake clears only a stopped group", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  let runnerPgid = 0;
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const execution = join(root, "merge-executed");
    const wt = await writeStub(
      root,
      String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
printf executed > "${execution}"
printf '{"branch":"linked-branch","target":"main","squashed":true,"rebased":true,"committed":true,"removed":false}\n'`,
      "wt-parked",
    );
    const controller = new AbortController();
    const lifecycleEvents: string[] = [];
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => {
        lifecycleEvents.push("reserved");
      },
      onSpawn: (identity) => {
        runnerPgid = identity.pgid;
        lifecycleEvents.push("spawned");
        controller.abort();
      },
      onConfirmedExit: () => {
        lifecycleEvents.push("exited");
      },
      complete: (outcome) => {
        lifecycleEvents.push(`outcome:${outcome.kind}`);
      },
      settle: () => {
        lifecycleEvents.push("settled");
      },
      onNotSpawned: () => {
        lifecycleEvents.push("not-spawned");
      },
      onUnknown: () => {
        lifecycleEvents.push("unknown");
      },
    };
    const port = createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [wt] });

    const outcome = await port.merge({ cwd: linked, lifecycle }, controller.signal).catch((error: unknown) => error);

    expect(outcome).not.toBeInstanceOf(WorktreeMergeExecutionError);
    expect(String((outcome as Error).message)).toContain("已取消");
    expect(lifecycleEvents).toEqual(["reserved", "spawned", "not-spawned"]);
    expect(existsSync(execution)).toBe(false);
    expect(runnerPgid).toBeGreaterThan(0);
    expect(groupAlive(runnerPgid)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("wt merge descendants stop after the host process is killed", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  let host: Bun.Subprocess | undefined;
  let worktreePid = 0;
  try {
    const repo = await createRepo(root);
    const linked = join(root, "linked");
    await runGit(repo, ["worktree", "add", "-q", linked, "-b", "linked-branch"]);
    const started = join(root, "wt-started");
    const hookEffect = join(root, "hook-after-parent-death");
    const wt = await writeStub(
      root,
      String.raw`if [ "$1" = "--version" ]; then echo "wt v0.80.0"; exit 0; fi
trap '' TERM
parent_pid=$$
(
  printf '%s\n' "$parent_pid" > "${started}.tmp"
  mv "${started}.tmp" "${started}"
  sleep 2
  printf survived > "${hookEffect}"
) &
while :; do sleep 10; done`,
      "wt-orphan",
    );
    const hostScript = join(root, "merge-host.ts");
    const lifecycleState = join(root, "merge-lifecycle.json");
    const worktreeModule = new URL("../src/workspace/worktree.ts", import.meta.url).href;
    await writeFile(
      hostScript,
      `import { writeFileSync } from "node:fs";\n` +
        `import { createWorktreePort } from ${JSON.stringify(worktreeModule)};\n` +
        `const [command, cwd, state] = process.argv.slice(2);\n` +
        `const save = (value) => writeFileSync(state, JSON.stringify(value));\n` +
        `const lifecycle = { beforeSpawn: (path) => save({ stage: "reserved", cwd: path }), ` +
        `onSpawn: (identity) => save({ stage: "spawned", identity }), ` +
        `onConfirmedExit: () => save({ stage: "exited" }), ` +
        `complete: (outcome) => save({ stage: "complete", outcome }), settle: () => save({ stage: "settled" }), ` +
        `onNotSpawned: () => save({ stage: "not-spawned" }), onUnknown: () => save({ stage: "unknown" }) };\n` +
        `await createWorktreePort({ command: "/nonexistent/omp", worktrunkCandidates: [command], mergeTimeoutMs: 60_000 })` +
        `.merge({ cwd, lifecycle });\n`,
      "utf8",
    );
    host = Bun.spawn([process.execPath, hostScript, wt, linked, lifecycleState], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      detached: true,
    });

    // Synchronize with the real command process; fake timers cannot advance its shell children.
    await waitForFileCreated(started, 5_000);
    worktreePid = Number(await readFile(started, "utf8"));
    const lifecycle = JSON.parse(await readFile(lifecycleState, "utf8")) as {
      stage: string;
      identity?: { pid: number; pgid: number };
    };
    expect(lifecycle.stage).toBe("spawned");
    expect(lifecycle.identity?.pid).toBe(worktreePid);
    expect(lifecycle.identity?.pgid).toBe(worktreePid);
    host.kill("SIGKILL");
    await host.exited;

    // The delayed hook would write after the parent has died unless the parent-death watchdog
    // terminates the detached wt process group.
    expect(await waitForGroupGone(worktreePid, 3_000)).toBe(true);
    expect(existsSync(hookEffect)).toBe(false);
  } finally {
    if (host !== undefined) {
      host.kill("SIGKILL");
      await host.exited.catch(() => -1);
    }
    if (worktreePid > 0 && groupAlive(worktreePid)) {
      await terminateProcessGroup(worktreePid, 1_000);
      expect(groupAlive(worktreePid)).toBe(false);
    }
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
