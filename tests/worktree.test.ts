import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createOmpWorktreePort, worktreeCandidates, worktreeSlug } from "../src/worktree";

/**
 * The port shells out to the OMP executable, so these tests use an executable stub that accepts
 * the same argv and performs the real `git worktree add`; production runs `omp worktree add`.
 */
const TRANSLATING_STUB = String.raw`set -e
[ "$1" = "worktree" ] && [ "$2" = "add" ] || exit 64
[ "$4" = "-b" ] && [ "$6" = "-C" ] || exit 64
exec git -C "$7" worktree add "$3" -b "$5" -q`;

async function writeStub(directory: string, body: string): Promise<string> {
  const path = join(directory, "stub-omp");
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
  expect(worktreeCandidates("/repo", "", "123456789012345")).toEqual([
    { path: "/repo.123456789012345", branch: "123456789012345" },
  ]);
});

test("creates a sibling worktree and steps aside when the name is taken", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const port = createOmpWorktreePort({ command: await writeStub(root, TRANSLATING_STUB) });

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

    // Non-ASCII names have no slug at all and use the thread id directly.
    await expect(
      port.create({ baseDir: repo, label: "修复登录", threadId: "987654321098765" }),
    ).resolves.toEqual({
      cwd: join(root, "project.987654321098765"),
      branch: "987654321098765",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("reports a missing repository and a failed command with a bounded message", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const failing = await writeStub(root, "yes boom | head -c 500 >&2\nexit 3");
    const port = createOmpWorktreePort({ command: failing });

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
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounds a hanging command by its timeout and refuses an aborted request", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-worktree-")));
  try {
    const repo = await createRepo(root);
    const hanging = await writeStub(root, "exec sleep 30");
    const port = createOmpWorktreePort({ command: hanging, timeoutMs: 150 });
    await expect(
      port.create({ baseDir: repo, label: "slow", threadId: "123456789012345" }),
    ).rejects.toThrow(/超时/);

    const controller = new AbortController();
    controller.abort();
    await expect(
      port.create({ baseDir: repo, label: "aborted", threadId: "123456789012345" }, controller.signal),
    ).rejects.toThrow(/已取消/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
