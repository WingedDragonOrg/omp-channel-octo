import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import { cleanupOwnedThreadWorkspace } from "../src/threads/thread-workspace-cleanup.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";

// Remote thread deletion releases removed worktrees without claiming ownership of retained Git refs.
test("legacy shared cleanup removes its empty entry while Git keeps the project branch", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-legacy-cleanup-")));
  const store = await openChannelStore(join(root, "state"));
  const git = async (cwd: string, ...args: string[]): Promise<string> => {
    const child = Bun.spawn(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe", stdin: "ignore" });
    const [out, err, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    if (code !== 0) throw new Error(err);
    return out.trim();
  };
  try {
    const repo = join(root, "source");
    const entry = join(root, "entry");
    const worktree = join(entry, "project");
    await mkdir(repo);
    await mkdir(entry);
    await git(repo, "init", "-qb", "main");
    await writeFile(join(repo, "base"), "base\n");
    await git(repo, "add", ".");
    await git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", "commit", "-qm", "base");
    await git(repo, "worktree", "add", "-qb", "feature", worktree);
    const record = {
      botUid: "bot", channelType: 5 as const, channelId: "group____topic", layout: "shared" as const,
      status: "ready" as const, entryPath: entry, workspaceRoot: null, error: null,
      projects: [{ name: "project", repo, path: worktree, branch: "feature", created: true }],
    };
    store.saveThreadWorkspace(record);
    const result = await cleanupOwnedThreadWorkspace({
      ownership: record, store,
      worktrees: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [] }),
      deleteBranch: false, force: false,
      lease: { ready: Promise.resolve(), complete() {} },
    });
    expect(existsSync(worktree)).toBe(false);
    expect(await git(repo, "branch", "--list", "feature")).toBe("feature");
    expect(existsSync(entry)).toBe(false);
    expect(result.complete).toBe(true);
    expect(store.getThreadWorkspace("bot", record)?.projects[0]).toMatchObject({ path: null, branch: null, created: false });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("cancelled cleanup leaves an empty shared entry in place", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-cancel-entry-")));
  const store = await openChannelStore(join(root, "state"));
  try {
    const entry = join(root, "entry");
    await mkdir(entry);
    const ownership = {
      botUid: "bot", channelType: 5 as const, channelId: "group____topic", layout: "shared" as const,
      status: "failed" as const, entryPath: entry, workspaceRoot: null, error: null,
      projects: [{ name: "removed", repo: root, path: null, branch: null, created: false }],
    };
    store.saveThreadWorkspace(ownership);
    const controller = new AbortController();
    controller.abort();
    await cleanupOwnedThreadWorkspace({
      ownership, store, worktrees: undefined, deleteBranch: true, force: false,
      signal: controller.signal, lease: { ready: Promise.resolve(), complete() {} },
    });
    expect(existsSync(entry)).toBe(true);
    expect(store.getThreadWorkspace("bot", ownership)?.entryPath).toBe(entry);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("legacy cleanup refuses incomplete branch ownership even with force", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-incomplete-cleanup-")));
  const store = await openChannelStore(join(root, "state"));
  const git = async (cwd: string, ...args: string[]) => {
    const process = Bun.spawn(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe" });
    const [out, err, code] = await Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text(), process.exited]);
    if (code !== 0) throw new Error(err);
    return out.trim();
  };
  try {
    const repo = join(root, "source");
    const path = join(root, "owned");
    await mkdir(repo);
    await git(repo, "init", "-qb", "main");
    await git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", "commit", "--allow-empty", "-qm", "base");
    await git(repo, "worktree", "add", "-qb", "other-feature", path);
    const sentinel = join(path, "uncommitted");
    await writeFile(sentinel, "must survive incomplete ownership\\n");
    const ownership = {
      botUid: "bot", channelType: 5 as const, channelId: "group____incomplete", layout: "single" as const,
      status: "ready" as const, entryPath: null, workspaceRoot: null, error: null,
      projects: [{ name: "project", repo, path, branch: null, created: true }],
    };
    store.saveThreadWorkspace(ownership);
    await cleanupOwnedThreadWorkspace({
      ownership, store,
      worktrees: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [] }),
      deleteBranch: false, force: true, lease: { ready: Promise.resolve(), complete() {} },
    });
    expect(existsSync(sentinel)).toBe(true);
    expect(await git(repo, "rev-parse", "refs/heads/other-feature")).toBe(await git(repo, "rev-parse", "HEAD"));
    expect(store.getThreadWorkspace("bot", ownership)?.projects[0]?.path).toBe(path);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("shared cleanup removes only an empty owned task root and preserves unknown resources", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-owned-task-root-")));
  const store = await openChannelStore(join(root, "state"));
  const git = async (cwd: string, ...args: string[]): Promise<string> => {
    const child = Bun.spawn(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe", stdin: "ignore" });
    const [out, err, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
    if (code !== 0) throw new Error(err);
    return out.trim();
  };
  try {
    const repo = join(root, "source");
    const workspaces = join(root, "thread-workspaces");
    const emptyRoot = join(workspaces, "empty-task");
    const emptyEntry = join(emptyRoot, "scope");
    const emptyProject = join(emptyEntry, "project");
    const occupiedRoot = join(workspaces, "occupied-task");
    const occupiedEntry = join(occupiedRoot, "scope");
    const occupiedProject = join(occupiedEntry, "project");
    const unknownFile = join(occupiedRoot, "unrecognized-resource");
    const foreignFile = join(workspaces, "other-task", "keep");
    await mkdir(repo);
    await git(repo, "init", "-qb", "main");
    await writeFile(join(repo, "base"), "base\n");
    await git(repo, "add", ".");
    await git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", "commit", "-qm", "base");
    await mkdir(emptyEntry, { recursive: true });
    await mkdir(occupiedEntry, { recursive: true });
    await mkdir(join(workspaces, "other-task"), { recursive: true });
    await writeFile(unknownFile, "must survive cleanup\n");
    await writeFile(foreignFile, "owned by another task\n");
    await git(repo, "worktree", "add", "-qb", "empty-feature", emptyProject);
    await git(repo, "worktree", "add", "-qb", "occupied-feature", occupiedProject);

    const emptyOwnership = {
      botUid: "bot", channelType: 5 as const, channelId: "group____empty", layout: "shared" as const,
      status: "ready" as const, entryPath: emptyEntry, workspaceRoot: emptyRoot, error: null,
      projects: [{ name: "project", repo, path: emptyProject, branch: "empty-feature", created: true }],
    };
    const occupiedOwnership = {
      botUid: "bot", channelType: 5 as const, channelId: "group____occupied", layout: "shared" as const,
      status: "ready" as const, entryPath: occupiedEntry, workspaceRoot: occupiedRoot, error: null,
      projects: [{ name: "project", repo, path: occupiedProject, branch: "occupied-feature", created: true }],
    };
    store.saveThreadWorkspace(emptyOwnership);
    store.saveThreadWorkspace(occupiedOwnership);
    const cleanupOptions = {
      store,
      worktrees: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [] }),
      deleteBranch: false, force: false,
      lease: { ready: Promise.resolve(), complete() {} },
    };

    const emptyResult = await cleanupOwnedThreadWorkspace({ ...cleanupOptions, ownership: emptyOwnership });
    expect(emptyResult.complete).toBe(true);
    expect(existsSync(emptyProject)).toBe(false);
    expect(existsSync(emptyEntry)).toBe(false);
    expect(existsSync(emptyRoot)).toBe(false);
    expect(store.getThreadWorkspace("bot", emptyOwnership)).toMatchObject({
      entryPath: null, workspaceRoot: null, projects: [{ path: null, branch: null, created: false }],
    });

    await cleanupOwnedThreadWorkspace({ ...cleanupOptions, ownership: occupiedOwnership });
    expect(existsSync(occupiedProject)).toBe(false);
    expect(existsSync(occupiedEntry)).toBe(false);
    expect(existsSync(occupiedRoot)).toBe(true);
    expect(await Bun.file(unknownFile).text()).toBe("must survive cleanup\n");
    expect(store.getThreadWorkspace("bot", occupiedOwnership)).toMatchObject({
      entryPath: null, workspaceRoot: occupiedRoot, projects: [{ path: null, branch: null, created: false }],
    });
    expect(await Bun.file(foreignFile).text()).toBe("owned by another task\n");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
