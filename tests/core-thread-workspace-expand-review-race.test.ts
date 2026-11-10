import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { access, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import type {
  ChatTarget,
  DeleteThreadOptions,
  HostTool,
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorkerProcess,
} from "../src/contracts.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorktrees,
  inbound,
  type CoreHarness,
  type FakePrompt,
  type FakePromptContext,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-workspace-race";
const GROUP_TARGET = { channelType: 2 as const, channelId: GROUP_ID };
const DIRTY_CONTENT = "uncommitted legacy workspace data\n";
const PRIME_TEXT = "prime workspace move session";
const EXPAND_TEXT = "expand the legacy thread workspace";
const DELETE_REMOTE_TEXT = "delete the thread remotely";

function fixtureBot() {
  return { uid: `bot-workspace-race-${randomUUID()}`, name: "Workspace Race Fixture" };
}

type ThreadTarget = ChatTarget & { channelType: 5; shortId: string };

function targetOf(thread: { channelId: string; shortId: string }): ThreadTarget {
  return { channelType: 5, channelId: thread.channelId, shortId: thread.shortId };
}

function ownerMessage(harness: CoreHarness, target: ChatTarget, messageId: string, text: string) {
  return inbound({
    ...target,
    messageId,
    senderId: "owner-1",
    text,
    mentionedUserIds: [harness.bot.uid],
  });
}

function managementTool(context: FakePromptContext["promptContext"]): HostTool {
  const tool = context?.tools?.find(({ name }) => name === "octo_management");
  if (!tool) throw new Error("octo_management host tool missing");
  return tool;
}

async function git(cwd: string, ...args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", cwd, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr.trim()}`);
  return stdout.trim();
}

async function createRepo(root: string, name: string): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo, { recursive: true });
  await writeFile(join(repo, "origin.txt"), `${name} origin\n`, "utf8");
  await git(repo, "init", "-q", "-b", "main");
  await git(repo, "add", ".");
  await git(repo, "-c", "user.email=fixture@example.com", "-c", "user.name=Fixture", "commit", "-q", "-m", "fixture source");
  return realpath(repo);
}

async function addWorktree(repo: string, path: string, branch: string): Promise<string> {
  await mkdir(dirname(path), { recursive: true });
  await git(repo, "worktree", "add", "-q", "-b", branch, path);
  return realpath(path);
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function withStore<T>(harness: CoreHarness, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

async function waitUntil<T>(
  read: () => T | null | undefined | false | Promise<T | null | undefined | false>,
  description: string,
  timeoutMs = 10_000,
): Promise<NonNullable<T>> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    const result = await read();
    if (result !== null && result !== undefined && result !== false) return result as NonNullable<T>;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${description}`);
}


async function sendTask(harness: CoreHarness, target: ChatTarget, messageId: string, text: string): Promise<void> {
  await harness.service.submitInbound(ownerMessage(harness, target, messageId, text));
}


async function createThread(harness: CoreHarness, name: string): Promise<ThreadTarget> {
  return targetOf(await harness.transport.createThread(GROUP_ID, name));
}

async function seedOwnedWorkspace(
  harness: CoreHarness,
  target: ThreadTarget,
): Promise<{ workspace: ThreadWorkspaceRecord; repo: string; path: string }> {
  const repo = await createRepo(harness.root, `workspace-${target.shortId}`);
  const branch = `workspace-${target.shortId}`;
  const path = await addWorktree(repo, join(harness.root, `worktree-${target.shortId}`), branch);
  const workspace: ThreadWorkspaceRecord = {
    botUid: harness.bot.uid,
    channelType: 5,
    channelId: target.channelId,
    layout: "single",
    status: "ready",
    entryPath: null,
    projects: [{ name: "api", repo, path, branch, created: true }],
    error: null,
  };
  await withStore(harness, store => {
    store.ensureBinding({
      botUid: harness.bot.uid,
      channelType: 5,
      channelId: target.channelId,
      sessionId: `session-${target.shortId}`,
      cwd: path,
      sessionFile: null,
      cwdPinned: true,
    });
    store.saveThreadWorkspace(workspace);
  });
  return { workspace, repo, path };
}

function waitForNextRemoteText(harness: CoreHarness, target: ChatTarget) {
  const existing = new Set(harness.transport.listRemoteText(target).map(message => message.messageId));
  return harness.transport.waitForRemoteText(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && !existing.has(message.messageId));
}

async function settleWithin(promise: Promise<unknown>, timeoutMs = 3_000): Promise<void> {
  const timeout = Promise.withResolvers<void>();
  const timer = setTimeout(timeout.resolve, timeoutMs);
  try {
    await Promise.race([promise.then(() => undefined, () => undefined), timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

class GatedDeleteTransport extends FakeTransport {
  readonly deleteStarted = Promise.withResolvers<void>();
  private readonly releaseGate = Promise.withResolvers<void>();

  releaseDelete(): void {
    this.releaseGate.resolve();
  }

  override async deleteThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
    options?: DeleteThreadOptions,
  ): Promise<void> {
    this.deleteStarted.resolve();
    await this.releaseGate.promise;
    await super.deleteThread(groupId, shortId, signal, options);
  }
}


type ProcessLifecycle = {
  beforeSpawn(cwd: string): void;
  onSpawn(process: WorkerProcess): void;
  onConfirmedExit(): void;
  onNotSpawned(): void;
};

async function runTrackedGit(
  cwd: string,
  args: string[],
  processCwd: string,
  lifecycle: ProcessLifecycle | undefined,
  afterBeforeSpawn?: () => Promise<void>,
): Promise<void> {
  try {
    lifecycle?.beforeSpawn(processCwd);
    await afterBeforeSpawn?.();
  } catch (error) {
    lifecycle?.onNotSpawned();
    throw error;
  }
  let child: Bun.Subprocess;
  try {
    child = Bun.spawn(["git", "-C", cwd, ...args], {
      cwd: processCwd,
      detached: true,
      stdin: "ignore",
      stdout: "ignore",
      stderr: "pipe",
    });
  } catch (error) {
    lifecycle?.onNotSpawned();
    throw error;
  }
  lifecycle?.onSpawn({ pid: child.pid, pgid: child.pid, identity: `fixture-git-${child.pid}`, cwd: processCwd });
  const [stderr, exitCode] = await Promise.all([
    new Response(child.stderr as ReadableStream<Uint8Array>).text(),
    child.exited,
  ]);
  lifecycle?.onConfirmedExit();
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed (${exitCode}): ${stderr.trim()}`);
}

type Gate = {
  entered: Promise<void>;
  signalEntered(): void;
  released: Promise<void>;
  release(): void;
};

function createGate(): Gate {
  const entered = Promise.withResolvers<void>();
  const released = Promise.withResolvers<void>();
  return {
    entered: entered.promise,
    signalEntered: () => entered.resolve(),
    released: released.promise,
    release: () => released.resolve(),
  };
}

class RealGitWorktrees extends FakeWorktrees {
  createGate: Gate | undefined;
  private removeGate: { path: string; gate: Gate } | undefined;
  private readonly fixtureRoot: string;

  constructor(root: string) {
    super(root);
    this.fixtureRoot = root;
  }

  holdRemovalAfterReservation(path: string): Gate {
    const gate = createGate();
    this.removeGate = { path, gate };
    return gate;
  }

  override async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    const { onPlanned, ...recorded } = request;
    this.requests.push({ ...recorded });
    if (this.failure) throw this.failure;
    const gate = this.createGate;
    if (gate) {
      gate.signalEntered();
      await gate.released;
      if (this.createGate === gate) this.createGate = undefined;
    }
    const repo = await realpath(request.baseDir);
    const path = request.path ?? join(this.fixtureRoot, "worktrees", request.threadId);
    const branch = request.label;
    onPlanned?.({ cwd: path, branch, repo });
    await runTrackedGit(
      repo,
      ["worktree", "add", "-q", "-b", branch, path, ...(request.base === undefined ? [] : [request.base])],
      repo,
      request.lifecycle,
    );
    return { cwd: await realpath(path), branch };
  }

  override async remove(request: WorktreeRemovalRequest): Promise<WorktreeRemovalOutcome> {
    this.removals.push({ ...request });
    const gate = this.removeGate?.path === request.path ? this.removeGate : undefined;
    if (!request.repo) throw new Error("real Git cleanup requires a recorded source repository");
    const wasPresent = await pathExists(request.path);
    if (wasPresent) {
      await runTrackedGit(
        request.repo,
        ["worktree", "remove", ...(request.force === true ? ["--force"] : []), request.path],
        request.path,
        request.lifecycle?.("worktree"),
        gate
          ? async () => {
            gate.gate.signalEntered();
            await gate.gate.released;
            if (this.removeGate === gate) this.removeGate = undefined;
          }
          : undefined,
      );
    }
    let branchOutcome: "deleted" | "kept" | "missing" = "kept";
    if (request.deleteBranch === true && request.branch !== undefined) {
      if ((await git(request.repo, "branch", "--list", request.branch)).length === 0) {
        branchOutcome = "missing";
      } else {
        await runTrackedGit(
          request.repo,
          ["branch", request.force === true ? "-D" : "-d", request.branch],
          request.repo,
          request.lifecycle?.("branch"),
        );
        branchOutcome = "deleted";
      }
    }
    return { kind: wasPresent ? "removed" : "missing", via: "git", branchOutcome };
  }
}


// /move rewrites the real session header before worker shutdown is confirmed. /status must not
// reconcile that intermediate header into the binding ahead of the atomic workspace commit.
test("conversion does not publish a moved binding until its native worker exits", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-thread-workspace-move-race-")));
  const bot = fixtureBot();
  const worktrees = new RealGitWorktrees(root);
  const moveStopEntered = Promise.withResolvers<void>();
  const releaseMoveStop = Promise.withResolvers<void>();
  let stopCalls = 0;
  let harness: CoreHarness | undefined;
  let addError: string | undefined;
  const addReturned = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ text, promptContext }) => {
    if (text === PRIME_TEXT || text !== EXPAND_TEXT) return;
    try {
      await managementTool(promptContext).execute({
        action: "thread-workspace-add",
        worktree_name: "move-race-expansion",
        projects: [{ name: "web", repo: join(root, "web-repo") }],
      }, new AbortController().signal);
    } catch (error) {
      addError = error instanceof Error ? error.message : String(error);
    } finally {
      addReturned.resolve();
    }
  };

  try {
    harness = await createCoreHarness({
      root,
      bot,
      worktreePort: worktrees,
      allowedGroupIds: [GROUP_ID],
      prompt,
    });
    const target = await createThread(harness, "single workspace conversion");
    const repo = await createRepo(root, "legacy-repo");
    const webRepo = await createRepo(root, "web-repo");
    const legacyPath = await addWorktree(repo, join(root, "legacy-worktree"), "legacy-existing-branch");
    await writeFile(join(legacyPath, "dirty.txt"), DIRTY_CONTENT, "utf8");
    await withStore(harness, store => {
      store.ensureBinding({
        botUid: bot.uid,
        channelType: 2,
        channelId: GROUP_ID,
        sessionId: "group-session",
        cwd: repo,
        sessionFile: null,
        cwdPinned: false,
      });
      store.ensureBinding({
        botUid: bot.uid,
        channelType: 5,
        channelId: target.channelId,
        sessionId: "legacy-session",
        cwd: legacyPath,
        sessionFile: null,
        cwdPinned: true,
      });
      store.saveThreadWorkspace({
        botUid: bot.uid,
        channelType: 5,
        channelId: target.channelId,
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{ name: "legacy", repo, path: legacyPath, branch: "legacy-existing-branch", created: true }],
        error: null,
      });
    });

    await sendTask(harness, target, "prime-move-session", PRIME_TEXT);
    const original = await waitForStatus(harness.service, target, status => status.state === "completed");
    if (!original.sessionId) throw new Error("prime task did not persist its session");
    const oldBinding = await withStore(harness, store => store.getBinding(bot.uid, target));
    if (!oldBinding?.sessionFile) throw new Error("prime task did not persist an exact session file");

    harness.workers.stopHandler = async () => {
      stopCalls += 1;
      if (stopCalls === 2) {
        moveStopEntered.resolve();
        await releaseMoveStop.promise;
      }
    };
    await sendTask(harness, target, "expand-during-native-move", EXPAND_TEXT);
    await addReturned.promise;
    expect(addError).toBeUndefined();
    await waitForStatus(harness.service, target, status => status.state === "completed");
    await moveStopEntered.promise;

    const pending = await withStore(harness, store => ({
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
      operation: store.getLatestThreadWorkspaceOperation(bot.uid, target),
    }));
    const entry = pending.operation?.candidateWorkspace.entryPath;
    if (!entry) throw new Error("conversion operation did not persist its candidate entry");
    const status = await harness.service.status(target);
    const afterStatus = await withStore(harness, store => ({
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));

    expect(status).toMatchObject({ cwd: legacyPath, sessionId: original.sessionId });
    expect(afterStatus.binding).toMatchObject({ cwd: legacyPath, sessionId: original.sessionId });
    expect(afterStatus.workspace).toMatchObject({ layout: "single", status: "ready" });
    expect(pending.operation).toMatchObject({ phase: "move-dispatch-intent", status: "running" });
    expect(await realpath(join(entry, "legacy"))).toBe(legacyPath);
    expect(await readFile(join(legacyPath, "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);

    releaseMoveStop.resolve();
    await waitUntil(
      () => withStore(harness!, store => {
        const workspace = store.getThreadWorkspace(bot.uid, target);
        const binding = store.getBinding(bot.uid, target);
        return workspace?.status === "ready" && workspace.layout === "shared"
          && binding?.cwd === workspace.entryPath && binding.sessionId === original.sessionId
          ? { workspace, binding }
          : null;
      }),
      "workspace and binding publication",
    );
    const final = await withStore(harness, store => ({
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    expect(final.workspace).toMatchObject({ layout: "shared", status: "ready", entryPath: entry });
    expect(final.binding).toMatchObject({ cwd: entry, sessionId: original.sessionId });
    expect(await readFile(join(entry, "legacy", "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);
    expect(worktrees.requests.some(request => request.baseDir === webRepo)).toBe(true);
  } finally {
    releaseMoveStop.resolve();
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("a persisted thread-delete reservation prevents same-target /thread remove before remote deletion", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-thread-workspace-delete-remove-race-")));
  const bot = fixtureBot();
  const transport = new GatedDeleteTransport({ bot });
  const worktrees = new RealGitWorktrees(root);
  const deleteTaskDone = Promise.withResolvers<void>();
  let deleteTaskStarted = false;
  let deleteError: string | undefined;
  let deleteTarget: ThreadTarget | undefined;
  let harness: CoreHarness | undefined;
  const prompt: FakePrompt = async ({ text, promptContext }) => {
    if (text !== DELETE_REMOTE_TEXT) return;
    try {
      await managementTool(promptContext).execute({
        action: "thread-delete",
        short_id: deleteTarget!.shortId,
      }, new AbortController().signal);
    } catch (error) {
      deleteError = error instanceof Error ? error.message : String(error);
    } finally {
      deleteTaskDone.resolve();
    }
  };

  try {
    harness = await createCoreHarness({
      root,
      bot,
      transport,
      worktreePort: worktrees,
      allowedGroupIds: [GROUP_ID],
      prompt,
    });
    const target = await createThread(harness, "delete versus remove");
    deleteTarget = target;
    const owned = await seedOwnedWorkspace(harness, target);
    const before = await withStore(harness, store => ({
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));

    await sendTask(harness, GROUP_TARGET, "reserve-delete-before-remove", DELETE_REMOTE_TEXT);
    deleteTaskStarted = true;
    await transport.deleteStarted.promise;
    const reserved = await withStore(harness, store => ({
      blocked: store.isThreadWorkspaceCleanupBlocked(bot.uid, target),
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    expect(reserved.blocked).toBe(true);
    expect(reserved.workspace).toEqual(before.workspace);
    expect(reserved.binding).toEqual(before.binding);
    expect(await pathExists(owned.path)).toBe(true);
    expect(await git(owned.repo, "worktree", "list", "--porcelain")).toContain(owned.path);

    const removeReply = waitForNextRemoteText(harness, target);
    await sendTask(harness, target, "remove-during-remote-delete", "/thread remove");
    await removeReply;

    const afterRemove = await withStore(harness, store => ({
      blocked: store.isThreadWorkspaceCleanupBlocked(bot.uid, target),
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    expect(afterRemove.blocked).toBe(true);
    expect(afterRemove.workspace).toEqual(before.workspace);
    expect(afterRemove.binding).toEqual(before.binding);
    expect(await pathExists(owned.path)).toBe(true);
    expect(await git(owned.repo, "worktree", "list", "--porcelain")).toContain(owned.path);
    transport.releaseDelete();
    await deleteTaskDone.promise;
    expect(deleteError).toBeUndefined();
  } finally {
    transport.releaseDelete();
    if (deleteTaskStarted) await settleWithin(deleteTaskDone.promise);
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("a durable /thread remove reservation prevents same-target thread-delete before ownership changes", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-thread-workspace-remove-delete-race-")));
  const bot = fixtureBot();
  const transport = new GatedDeleteTransport({ bot });
  const worktrees = new RealGitWorktrees(root);
  const deleteTaskDone = Promise.withResolvers<void>();
  let deleteTaskStarted = false;
  let deleteError: string | undefined;
  let deleteTarget: ThreadTarget | undefined;
  let harness: CoreHarness | undefined;
  const prompt: FakePrompt = async ({ text, promptContext }) => {
    if (text !== DELETE_REMOTE_TEXT) return;
    try {
      await managementTool(promptContext).execute({
        action: "thread-delete",
        short_id: deleteTarget!.shortId,
      }, new AbortController().signal);
    } catch (error) {
      deleteError = error instanceof Error ? error.message : String(error);
    } finally {
      deleteTaskDone.resolve();
    }
  };
  let removalGate: Gate | undefined;
  let removalStarted = false;
  let archiveWaiter: Promise<void> | undefined;

  try {
    harness = await createCoreHarness({
      root,
      bot,
      transport,
      worktreePort: worktrees,
      allowedGroupIds: [GROUP_ID],
      prompt,
    });
    const target = await createThread(harness, "remove versus delete");
    deleteTarget = target;
    const owned = await seedOwnedWorkspace(harness, target);
    const before = await withStore(harness, store => ({
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    removalGate = worktrees.holdRemovalAfterReservation(owned.path);
    archiveWaiter = transport.waitForManagementCalls(call =>
      call.action === "thread-archive" && call.shortId === target.shortId);

    removalStarted = true;
    await sendTask(harness, target, "reserve-remove-before-delete", "/thread remove");
    await removalGate.entered;
    const reserved = await withStore(harness, store => ({
      leases: store.listHostRemovalLeases(bot.uid),
      blocked: store.isThreadWorkspaceCleanupBlocked(bot.uid, target),
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    expect(reserved.leases).toHaveLength(1);
    expect(reserved.leases[0]).toMatchObject({
      channelId: target.channelId,
      sourceId: "reserve-remove-before-delete",
      phase: "starting",
      step: "worktree",
      cwd: owned.path,
    });
    expect(reserved.blocked).toBe(false);
    expect(reserved.workspace).toMatchObject({ status: "failed" });
    expect(reserved.binding).toEqual(before.binding);
    expect(await pathExists(owned.path)).toBe(true);
    expect(await git(owned.repo, "worktree", "list", "--porcelain")).toContain(owned.path);

    await sendTask(harness, GROUP_TARGET, "delete-during-reserved-remove", DELETE_REMOTE_TEXT);
    deleteTaskStarted = true;
    const deleteOutcome = await Promise.race([
      deleteTaskDone.promise.then(() => "settled" as const),
      transport.deleteStarted.promise.then(() => "remote-delete-started" as const),
    ]);
    expect(deleteOutcome).toBe("settled");
    expect(deleteError).toContain("正在清理");
    expect(transport.managementCalls.filter(call =>
      call.action === "thread-delete" && call.shortId === target.shortId)).toEqual([]);
    const afterDelete = await withStore(harness, store => ({
      leases: store.listHostRemovalLeases(bot.uid),
      blocked: store.isThreadWorkspaceCleanupBlocked(bot.uid, target),
      workspace: store.getThreadWorkspace(bot.uid, target),
      binding: store.getBinding(bot.uid, target),
    }));
    expect(afterDelete.leases).toHaveLength(1);
    expect(afterDelete.blocked).toBe(false);
    expect(afterDelete.workspace).toEqual(reserved.workspace);
    expect(afterDelete.binding).toEqual(before.binding);
    expect(await pathExists(owned.path)).toBe(true);
    expect(await git(owned.repo, "worktree", "list", "--porcelain")).toContain(owned.path);

    removalGate.release();
    await archiveWaiter;
    expect(await pathExists(owned.path)).toBe(false);
  } finally {
    removalGate?.release();
    if (deleteTaskStarted) await settleWithin(deleteTaskDone.promise);
    if (removalStarted && archiveWaiter) await settleWithin(archiveWaiter);
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

