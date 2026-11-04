import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import type { ThreadWorkspaceRecord, ThreadWorkspaceProjectRecord } from "../src/store/store-records.ts";
import { readWorkspaceChanges } from "../src/workspace/git-status.ts";

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
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function withTempRoot(run: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-git-status-"));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function createRepo(
  root: string,
  name: string,
  files: Record<string, string | Uint8Array>,
): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo, { recursive: true });
  await runGit(repo, ["init", "-q", "-b", "main"]);
  for (const [name, contents] of Object.entries(files)) {
    const path = join(repo, name);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, contents);
  }
  await runGit(repo, ["add", "."]);
  await runGit(repo, [
    "-c", "user.email=test@example.com",
    "-c", "user.name=Fixture",
    "commit", "-q", "-m", "initial",
  ]);
  return repo;
}

async function addWorktree(repo: string, path: string, branch: string): Promise<string> {
  await mkdir(dirname(path), { recursive: true });
  await runGit(repo, ["worktree", "add", "-q", "-b", branch, path]);
  return path;
}

function project(
  name: string,
  repo: string,
  path: string | null,
  created = path !== null,
): ThreadWorkspaceProjectRecord {
  return { name, repo, path, branch: path === null ? null : `status-${name}`, created };
}

function workspace(
  layout: ThreadWorkspaceRecord["layout"],
  projects: ThreadWorkspaceProjectRecord[],
  entryPath: string | null = null,
  channelId = "group____git-status",
): ThreadWorkspaceRecord {
  return {
    botUid: "bot-git-status",
    channelType: 5,
    channelId,
    layout,
    status: "ready",
    entryPath,
    projects,
    error: null,
  };
}

function signal(): AbortSignal {
  return new AbortController().signal;
}

test("reports tracked additions, modifications, and deletions against HEAD", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "tracked-project", {
      "change.txt": "before\nkeep\n",
      "delete.txt": "remove one\nremove two\n",
    });
    await writeFile(join(repo, "added.txt"), "new one\nnew two\n");
    await runGit(repo, ["add", "added.txt"]);
    await writeFile(join(repo, "change.txt"), "after\nkeep\n");
    await rm(join(repo, "delete.txt"));

    const indexBefore = await readFile(join(repo, ".git", "index"));
    const configBefore = await readFile(join(repo, ".git", "config"));
    const cwdBefore = process.cwd();
    const changes = await readWorkspaceChanges(repo, null, signal());

    expect(changes).toHaveLength(1);
    expect(changes[0]).toMatchObject({
      name: basename(repo),
      path: repo,
      state: "git",
      baseline: "HEAD",
      added: 3,
      deleted: 3,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
    expect(process.cwd()).toBe(cwdBefore);
    expect(Buffer.compare(await readFile(join(repo, ".git", "index")), indexBefore)).toBe(0);
    expect(Buffer.compare(await readFile(join(repo, ".git", "config")), configBefore)).toBe(0);
  });
});
test("marks a conflicted repository unavailable instead of summing unmerged numstat entries", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "conflicted-project", { "conflict.txt": "base\n" });
    await runGit(repo, ["checkout", "-q", "-b", "side"]);
    await writeFile(join(repo, "conflict.txt"), "side\n");
    await runGit(repo, ["add", "conflict.txt"]);
    await runGit(repo, [
      "-c", "user.email=test@example.com",
      "-c", "user.name=Fixture",
      "commit", "-q", "-m", "side",
    ]);
    await runGit(repo, ["checkout", "-q", "main"]);
    await writeFile(join(repo, "conflict.txt"), "main\n");
    await runGit(repo, ["add", "conflict.txt"]);
    await runGit(repo, [
      "-c", "user.email=test@example.com",
      "-c", "user.name=Fixture",
      "commit", "-q", "-m", "main",
    ]);
    await expect(runGit(repo, ["merge", "side"])).rejects.toThrow("git merge side failed");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toEqual({
      name: basename(repo),
      path: repo,
      state: "unavailable",
      baseline: null,
      added: 0,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});

test("counts the final same-file diff once and omits staged changes cancelled in the worktree", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "net-diff", {
      "net.txt": "alpha\nbeta\nomega\n",
      "cancel.txt": "original\n",
    });

    await writeFile(join(repo, "net.txt"), "alpha\nstaged\nomega\n");
    await runGit(repo, ["add", "net.txt"]);
    await writeFile(join(repo, "net.txt"), "alpha\nfinal\nomega\n");

    await writeFile(join(repo, "cancel.txt"), "staged version\n");
    await runGit(repo, ["add", "cancel.txt"]);
    await writeFile(join(repo, "staged-add-then-delete.txt"), "staged only\n");
    await runGit(repo, ["add", "staged-add-then-delete.txt"]);
    await rm(join(repo, "staged-add-then-delete.txt"));
    await writeFile(join(repo, "cancel.txt"), "original\n");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      baseline: "HEAD",
      added: 1,
      deleted: 1,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});

test("reports binary and untracked files separately without treating untracked lines as a diff", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "binary-project", {
      ".gitignore": "ignored.tmp\n",
      "image.bin": new Uint8Array([0, 1, 2, 3]),
    });
    await writeFile(join(repo, "image.bin"), new Uint8Array([0, 1, 2, 4]));
    await writeFile(join(repo, "new.txt"), "one\ntwo\n");
    await mkdir(join(repo, "nested"));
    await writeFile(join(repo, "nested", "other.txt"), "three\n");
    await writeFile(join(repo, "ignored.tmp"), "ignored\n");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      baseline: "HEAD",
      state: "git",
      added: 0,
      deleted: 0,
      binaryFiles: 1,
      untrackedFiles: 2,
    });
  });
});
test("parses newline filenames and NUL-delimited rename records", async () => {
  await withTempRoot(async (root) => {
    const oldName = "old\nname.txt";
    const newName = "new\nname.txt";
    const repo = await createRepo(root, "rename-project", {
      [oldName]: "first\nsecond\nthird\n",
    });
    await runGit(repo, ["config", "diff.renames", "true"]);
    await runGit(repo, ["mv", "--", oldName, newName]);
    await writeFile(join(repo, newName), "first\nchanged\nthird\n");
    await writeFile(join(repo, "untracked\nname.txt"), "untracked\n");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      baseline: "HEAD",
      state: "git",
      added: 1,
      deleted: 1,
      binaryFiles: 0,
      untrackedFiles: 1,
    });
  });
});
test("does not count lines from an unstaged rename destination as tracked additions", async () => {
  await withTempRoot(async (root) => {
    const oldName = "old-name.txt";
    const newName = "new-name.txt";
    const repo = await createRepo(root, "unstaged-rename", {
      [oldName]: "first\nsecond\nthird\n",
    });
    await rename(join(repo, oldName), join(repo, newName));

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      baseline: "HEAD",
      state: "git",
      added: 0,
      deleted: 3,
      untrackedFiles: 1,
    });
  });
});

test("returns unavailable without partial counts when numstat capture is truncated", async () => {
  await withTempRoot(async (root) => {
    const repo = join(root, "large-diff");
    await mkdir(repo);
    await runGit(repo, ["init", "-q", "-b", "main"]);
    const directories = Array.from({ length: 4 }, (_, index) =>
      `${index}${"d".repeat(149)}`,
    );
    const directory = join(repo, ...directories);
    await mkdir(directory, { recursive: true });
    const paths = Array.from({ length: 400 }, (_, index) =>
      join(directory, `${String(index).padStart(5, "0")}-${"x".repeat(180)}.txt`),
    );
    for (const path of paths) await writeFile(path, "before\n");
    await runGit(repo, ["add", "."]);
    await runGit(repo, [
      "-c", "user.email=test@example.com",
      "-c", "user.name=Fixture",
      "commit", "-q", "-m", "initial",
    ]);
    for (const path of paths) await writeFile(path, "after\n");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toEqual({
      name: basename(repo),
      path: repo,
      state: "unavailable",
      baseline: null,
      added: 0,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});

test("marks a non-Git cwd as non-git instead of reporting a command failure", async () => {
  await withTempRoot(async (root) => {
    const directory = join(root, "not-a-repository");
    await mkdir(directory);

    const changes = await readWorkspaceChanges(directory, null, signal());
    expect(changes).toHaveLength(1);
    expect(changes[0]).toMatchObject({
      name: basename(directory),
      path: directory,
      state: "non-git",
      baseline: null,
      added: 0,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});
test("allows a session cwd below its Git root when no persistent workspace is recorded", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "session-project", { "nested/tracked.txt": "before\nkeep\n" });
    const cwd = join(repo, "nested");
    await writeFile(join(cwd, "tracked.txt"), "after\nkeep\n");

    const [change] = await readWorkspaceChanges(cwd, null, signal());
    expect(change).toMatchObject({
      name: "nested",
      path: cwd,
      state: "git",
      baseline: "HEAD",
      added: 1,
      deleted: 1,
    });
  });
});

test("counts the whole worktree for a nested cwd even when the repository enables diff.relative", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "nested-relative", {
      ".gitignore": "ignored.tmp\n",
      "root.txt": "root before\nkeep\n",
      "nested/tracked.txt": "nested before\nkeep\n",
    });
    await runGit(repo, ["config", "diff.relative", "true"]);
    const cwd = join(repo, "nested");
    await writeFile(join(repo, "root.txt"), "root after\nkeep\n");
    await writeFile(join(cwd, "tracked.txt"), "nested after\nkeep\n");
    await writeFile(join(repo, "root-untracked.txt"), "root untracked\n");
    await writeFile(join(cwd, "nested-untracked.txt"), "nested untracked\n");
    await writeFile(join(repo, "ignored.tmp"), "ignored\n");

    const [change] = await readWorkspaceChanges(cwd, null, signal());
    expect(change).toMatchObject({
      name: "nested",
      path: cwd,
      state: "git",
      baseline: "HEAD",
      added: 2,
      deleted: 2,
      binaryFiles: 0,
      untrackedFiles: 2,
    });
  });
});

test("uses the empty-tree baseline for staged content in an unborn repository", async () => {
  await withTempRoot(async (root) => {
    const repo = join(root, "unborn");
    await mkdir(repo);
    await runGit(repo, ["init", "-q", "-b", "main"]);
    await writeFile(join(repo, "first.txt"), "one\ntwo\nthree\n");
    await runGit(repo, ["add", "first.txt"]);

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      state: "git",
      baseline: "empty-tree",
      added: 3,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});

test("reads each shared project worktree, not its common entry or parent repository", async () => {
  await withTempRoot(async (root) => {
    const parent = await createRepo(root, "parent", { "parent.txt": "before\n" });
    const entryPath = join(parent, "shared-entry");
    await mkdir(entryPath);
    const firstSource = await createRepo(root, "source-one", { "tracked.txt": "one\nbase\n" });
    const secondSource = await createRepo(root, "source-two", { "tracked.txt": "two\nbase\n" });
    const firstPath = await addWorktree(firstSource, join(entryPath, "one"), "status-shared-one");
    const secondPath = await addWorktree(secondSource, join(entryPath, "two"), "status-shared-two");
    await writeFile(join(firstPath, "tracked.txt"), "one changed\nbase\n");
    await writeFile(join(secondPath, "tracked.txt"), "two changed\nbase\nextra\n");
    await writeFile(join(parent, "parent.txt"), "parent-only change\n");

    const record = workspace(
      "shared",
      [project("one", firstSource, firstPath), project("two", secondSource, secondPath)],
      entryPath,
    );
    const changes = await readWorkspaceChanges(parent, record, signal());

    expect(changes).toHaveLength(2);
    expect(changes.map((change) => change.baseline)).toEqual(["HEAD", "HEAD"]);
    expect(changes.map(({ name, path, added, deleted }) => ({ name, path, added, deleted }))).toEqual([
      { name: "one", path: firstPath, added: 1, deleted: 1 },
      { name: "two", path: secondPath, added: 2, deleted: 1 },
    ]);
  });
});

test("uses the project path for single and separate workspace records", async () => {
  await withTempRoot(async (root) => {
    const singleSource = await createRepo(root, "single-source", { "tracked.txt": "base\n" });
    const singlePath = await addWorktree(singleSource, join(root, "single-worktree"), "status-single");
    await writeFile(join(singlePath, "tracked.txt"), "single\nbase\n");

    const separateSource = await createRepo(root, "separate-source", { "tracked.txt": "base\nremove\n" });
    const separatePath = await addWorktree(separateSource, join(root, "separate-worktree"), "status-separate");
    await writeFile(join(separatePath, "tracked.txt"), "base\n");

    const single = await readWorkspaceChanges(root, workspace(
      "single",
      [project("single project", singleSource, singlePath)],
      null,
      "group____single",
    ), signal());
    const separate = await readWorkspaceChanges(root, workspace(
      "separate",
      [project("separate project", separateSource, separatePath)],
      null,
      "group____separate",
    ), signal());

    expect(single).toHaveLength(1);
    expect(single[0]).toMatchObject({
      baseline: "HEAD",
      name: "single project", path: singlePath, state: "git", added: 1, deleted: 0,
    });
    expect(separate).toHaveLength(1);
    expect(separate[0]).toMatchObject({
      baseline: "HEAD",
      name: "separate project", path: separatePath, state: "git", added: 0, deleted: 1,
    });
  });
});
test("does not mistake a stale project path inside a parent repository for its removed worktree", async () => {
  await withTempRoot(async (root) => {
    const parent = await createRepo(root, "parent", { "parent.txt": "before\n" });
    const source = await createRepo(root, "source", { "tracked.txt": "source\n" });
    const stalePath = await addWorktree(source, join(parent, "stale-worktree"), "status-stale");
    await rm(join(stalePath, ".git"));
    await writeFile(join(parent, "parent.txt"), "parent-only change\n");

    const [change] = await readWorkspaceChanges(parent, workspace(
      "single",
      [project("stale project", source, stalePath)],
      null,
      "group____stale",
    ), signal());

    expect(change).toEqual({
      name: "stale project",
      path: stalePath,
      state: "unavailable",
      baseline: null,
      added: 0,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    });
  });
});

test("reports a project with no recorded path as unavailable without falling back to repo or cwd", async () => {
  await withTempRoot(async (root) => {
    const cwd = await createRepo(root, "current-parent", { "parent.txt": "before\n" });
    const source = await createRepo(root, "project-source", { "tracked.txt": "source\n" });
    await writeFile(join(cwd, "parent.txt"), "parent changed\n");

    const changes = await readWorkspaceChanges(cwd, workspace(
      "single",
      [project("missing project", source, null, false)],
      null,
      "group____missing",
    ), signal());

    expect(changes).toEqual([{
      name: "missing project",
      path: null,
      baseline: null,
      state: "unavailable",
      added: 0,
      deleted: 0,
      binaryFiles: 0,
      untrackedFiles: 0,
    }]);
  });
});

test("does not run a repository fsmonitor hook while reading status", async () => {
  await withTempRoot(async (root) => {
    const repo = await createRepo(root, "fsmonitor-project", { "tracked.txt": "before\nkeep\n" });
    const hook = join(root, "fsmonitor-hook.sh");
    await writeFile(hook, '#!/bin/sh\n: > "$0.marker"\nexit 0\n', "utf8");
    await chmod(hook, 0o755);
    await runGit(repo, ["config", "core.fsmonitor", hook]);
    await writeFile(join(repo, "tracked.txt"), "after\nkeep\n");

    const [change] = await readWorkspaceChanges(repo, null, signal());
    expect(change).toMatchObject({
      state: "git",
      baseline: "HEAD",
      added: 1,
      deleted: 1,
    });
    expect(existsSync(`${hook}.marker`)).toBe(false);
  });
});
