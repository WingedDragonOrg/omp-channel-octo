import { expect, test } from "bun:test";
import { access, lstat, mkdir, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type {
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorktreeMergePlan,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorktreeRemovalStep as RemovalStep,
  WorktrunkProbe,
} from "../src/contracts.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  type CoreHarness,
  type FakePrompt,
  FakeTransport,
  waitForStatus,
} from "./fixtures/core-fakes.ts";
import { createExecutionTracker, encodeExecutionSnapshot } from "../src/cards/execution-tracker.ts";
import { ThreadArchiveScheduler } from "../src/threads/thread-archive.ts";

type ThreadTarget = { channelType: 5; channelId: string };
type RecordedRemovalRequest = Omit<WorktreeRemovalRequest, "lifecycle">;
type ProcessResult = "confirmed" | "not-spawned" | "not-started" | "unknown";
type RunRemovalProcess = (step: RemovalStep, result?: ProcessResult) => void;
type RemovalBehavior = (
  request: RecordedRemovalRequest,
  runProcess: RunRemovalProcess,
) => Promise<WorktreeRemovalOutcome>;

const REMOVE_GROUP = "group-tools";
const REMOVE_RECOVERY_BODY = "恢复中的正文待投递。";
const REMOVE_RECOVERY_RECEIPT = "恢复的本地清理回执。";
const INCOMPLETE_WORKSPACE_NOTICE = "当前子区工作区未就绪或资源已缺失，未接收任务。";

function ownerMessage(
  harness: CoreHarness,
  target: ThreadTarget,
  messageId: string,
  text: string,
  senderId = "owner-1",
) {
  return inbound({
    ...target,
    messageId,
    senderId,
    text,
    mentionedUserIds: [harness.bot.uid],
  });
}

function removalCommand(
  harness: CoreHarness,
  target: ThreadTarget,
  messageId: string,
  argument = "",
) {
  return ownerMessage(
    harness,
    target,
    messageId,
    `/thread remove${argument.length === 0 ? "" : ` ${argument}`}`,
  );
}

function pathTag(target: ThreadTarget): string {
  return target.channelId.replaceAll(/[^a-zA-Z0-9-]/gu, "-");
}

async function seedOwnedWorkspace(
  harness: CoreHarness,
  target: ThreadTarget,
  options: {
    readonly names?: readonly string[];
    readonly layout?: ThreadWorkspaceRecord["layout"];
    readonly entryPath?: string | null;
    readonly bindingCwd?: string;
    readonly missingPaths?: readonly string[];
  } = {},
): Promise<{
  readonly record: ThreadWorkspaceRecord;
  readonly entryPath: string | null;
  readonly bindingCwd: string;
}> {
  const tag = pathTag(target);
  const names = options.names ?? ["api"];
  const layout = options.layout ?? "single";
  const entryPath = options.entryPath === undefined
    ? layout === "shared" ? join(harness.root, `${tag}-entry`) : null
    : options.entryPath;
  if (entryPath !== null) await mkdir(entryPath, { recursive: true });

  const missingPaths = new Set(options.missingPaths ?? []);
  const projects = names.map((name) => {
    const repo = join(harness.root, `${tag}-repo-${name}`);
    const path = layout === "shared"
      ? join(entryPath!, name)
      : join(harness.root, `${tag}-worktree-${name}`);
    return { name, repo, path, branch: `thread-${tag}-${name}`, created: true };
  });
  for (const project of projects) {
    await mkdir(project.repo, { recursive: true });
    if (!missingPaths.has(project.name)) await mkdir(project.path, { recursive: true });
  }

  const bindingCwd = options.bindingCwd
    ?? projects.find(({ name }) => !missingPaths.has(name))?.path
    ?? join(harness.root, `${tag}-binding`);
  await mkdir(bindingCwd, { recursive: true });
  const record: ThreadWorkspaceRecord = {
    botUid: harness.bot.uid,
    channelType: 5,
    channelId: target.channelId,
    layout,
    status: "ready",
    entryPath,
    projects,
    error: null,
  };
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...target,
      cwd: bindingCwd,
      sessionId: `session-${tag}`,
      sessionFile: null,
      cwdPinned: true,
    });
    store.saveThreadWorkspace(record);
  } finally {
    store.close();
  }
  return { record, entryPath, bindingCwd };
}

async function withStore<T>(harness: CoreHarness, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function waitForAction(
  harness: CoreHarness,
  action: string,
  timeoutMs = 1_000,
): Promise<boolean> {
  const timeout = Promise.withResolvers<boolean>();
  const timer = setTimeout(() => timeout.resolve(false), timeoutMs);
  try {
    return await Promise.race([
      harness.transport.waitForManagementCalls((call) => call.action === action).then(() => true),
      timeout.promise,
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function waitForNextTargetText(harness: CoreHarness, target: ThreadTarget) {
  const existingMessageIds = new Set(
    harness.transport.listRemoteText(target).map(message => message.messageId),
  );
  return harness.transport.waitForRemoteText(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && !existingMessageIds.has(message.messageId));
}


/** The wall-clock bound proves a busy remove command rejects instead of waiting for task settlement. */
async function settlesWithin(promise: Promise<void>, timeoutMs = 250): Promise<boolean> {
  const timeout = Promise.withResolvers<boolean>();
  const timer = setTimeout(() => timeout.resolve(false), timeoutMs);
  try {
    return await Promise.race([promise.then(() => true), timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

class RecordingWorktrees implements WorktreePort {
  readonly requests: RecordedRemovalRequest[] = [];
  readonly timeline: string[] = [];
  readonly processSteps: Array<{ step: RemovalStep; cwd: string }> = [];
  removeBehavior: RemovalBehavior = async (request, runProcess) => {
    runProcess("worktree");
    await rm(request.path, { recursive: true, force: true });
    if (request.deleteBranch === true) runProcess("branch");
    this.timeline.push("worktree-removed");
    return {
      kind: "removed",
      via: "git",
      branchOutcome: request.deleteBranch === true ? "deleted" : null,
    };
  };

  private nextPid = 48_000;

  async create(_request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    throw new Error("thread remove tests do not create worktrees");
  }

  async planMerge(_request: WorktreeMergeRequest): Promise<WorktreeMergePlan> {
    throw new Error("thread remove must not plan worktree merges");
  }

  async merge(_request: WorktreeMergeRequest): Promise<WorktreeMergeSummary> {
    throw new Error("thread remove must not merge worktrees");
  }

  async remove(request: WorktreeRemovalRequest, signal?: AbortSignal): Promise<WorktreeRemovalOutcome> {
    if (signal?.aborted === true) throw new Error("fixture removal aborted");
    const { lifecycle: _lifecycle, ...recorded } = request;
    this.requests.push({ ...recorded });
    this.timeline.push("remove-started");
    return this.removeBehavior(recorded, (step, result = "confirmed") => {
      const lifecycle = request.lifecycle?.(step);
      if (!lifecycle) return;
      if (result === "not-spawned") {
        lifecycle.onNotSpawned();
        return;
      }
      const cwd = step === "branch" ? request.repo ?? request.path : request.path;
      lifecycle.beforeSpawn(cwd);
      this.processSteps.push({ step, cwd });
      const pid = this.nextPid++;
      lifecycle.onSpawn({
        pid,
        pgid: pid,
        identity: `pid=${pid};start=fixture-${pid}`,
        cwd,
      });
      if (result === "not-started") {
        lifecycle.onNotSpawned();
        return;
      }
      if (result === "unknown") lifecycle.onUnknown();
      else lifecycle.onConfirmedExit();
    });
  }

  async worktrunk(): Promise<WorktrunkProbe> {
    return { available: true, command: "wt" };
  }
}

class RecoveryBlockedTransport extends FakeTransport {
  readonly recoveryEditStarted = Promise.withResolvers<void>();
  readonly releaseRecoveryEdit = Promise.withResolvers<void>();
  readonly removalTimeline: string[] = [];
  readonly recoveryBodyStarted = Promise.withResolvers<void>();
  readonly recoveryBodySent = Promise.withResolvers<void>();
  readonly releaseRecoveryBody = Promise.withResolvers<void>();
  readonly recoveryReceiptSent = Promise.withResolvers<void>();

  override async sendText(
    ...args: Parameters<FakeTransport["sendText"]>
  ): ReturnType<FakeTransport["sendText"]> {
    const [, text] = args;
    if (text === REMOVE_RECOVERY_BODY) {
      this.recoveryBodyStarted.resolve();
      await this.releaseRecoveryBody.promise;
    }
    const receipt = await super.sendText(...args);
    if (text === REMOVE_RECOVERY_BODY) {
      this.removalTimeline.push("body-sent");
      this.recoveryBodySent.resolve();
    } else if (text === REMOVE_RECOVERY_RECEIPT) {
      this.removalTimeline.push("receipt-sent");
      this.recoveryReceiptSent.resolve();
    }
    return receipt;
  }
  override async editProgressCard(
    ...args: Parameters<FakeTransport["editProgressCard"]>
  ): Promise<void> {
    this.recoveryEditStarted.resolve();
    await this.releaseRecoveryEdit.promise;
    await super.editProgressCard(...args);
  }

  override async archiveThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const channelId = `${groupId}____${shortId}`;
    this.removalTimeline.push(
      this.sent.some(({ target, text }) =>
        target.channelType === 5 && target.channelId === channelId && text === REMOVE_RECOVERY_RECEIPT,
      )
        ? "archive-after-receipt"
        : "archive-before-receipt",
    );
    await super.archiveThread(groupId, shortId, signal);
  }
}

function observeReceiptBeforeArchive(
  harness: CoreHarness,
  worktrees: RecordingWorktrees,
  target: ThreadTarget,
): void {
  const sendText = harness.transport.sendText.bind(harness.transport);
  harness.transport.sendText = async (messageTarget, text, clientMsgNo, signal) => {
    const result = await sendText(messageTarget, text, clientMsgNo, signal);
    if (worktrees.requests.length > 0 && messageTarget.channelType === target.channelType
      && messageTarget.channelId === target.channelId) {
      worktrees.timeline.push("receipt-sent");
    }
    return result;
  };
  const archiveThread = harness.transport.archiveThread.bind(harness.transport);
  harness.transport.archiveThread = async (groupId, shortId, signal) => {
    worktrees.timeline.push("archive");
    await archiveThread(groupId, shortId, signal);
  };
}


test("/thread remove cleans persisted ownership once and archives only after its receipt is sent", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-success` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target);
    observeReceiptBeforeArchive(harness, worktrees, target);
    const command = removalCommand(harness, target, "remove-success-command");

    await harness.service.submitInbound(command);
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(worktrees.requests).toHaveLength(1);
    await harness.service.submitInbound(command);

    const request = worktrees.requests[0]! as RecordedRemovalRequest & { deleteBranch?: boolean };
    expect(request.path).toBe(workspace.record.projects[0]!.path!);
    expect(request.repo).toBe(workspace.record.projects[0]!.repo);
    expect(request.branch).toBe(workspace.record.projects[0]!.branch!);
    expect(request.force ?? false).toBe(false);
    expect(request.deleteBranch).toBe(true);
    expect(worktrees.processSteps).toEqual([
      { step: "worktree", cwd: workspace.record.projects[0]!.path! },
      { step: "branch", cwd: workspace.record.projects[0]!.repo },
    ]);
    expect(worktrees.timeline).toEqual([
      "remove-started",
      "worktree-removed",
      "receipt-sent",
      "archive",
    ]);
    expect(harness.transport.sent.filter(({ target: sentTarget }) =>
      sentTarget.channelType === target.channelType && sentTarget.channelId === target.channelId,
    )).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-delete")).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(false);
    await withStore(harness, (store) => {
      const persisted = store.getThreadWorkspace(harness.bot.uid, target);
      expect(persisted?.projects[0]).toMatchObject({ created: false, path: null, branch: null });
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
      expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
      expect(store.listHostRemovalLeases(harness.bot.uid)).toEqual([]);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread remove --force forwards force without changing its owner-only command boundary", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-force` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target);
    const command = removalCommand(harness, target, "remove-force-command", "--force");
    await harness.service.submitInbound(command);
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(worktrees.requests).toHaveLength(1);
    const request = worktrees.requests[0]! as RecordedRemovalRequest & { deleteBranch?: boolean };
    expect(request.path).toBe(workspace.record.projects[0]!.path!);
    expect(request.repo).toBe(workspace.record.projects[0]!.repo);
    expect(request.branch).toBe(workspace.record.projects[0]!.branch!);
    expect(request.force).toBe(true);
    expect(request.deleteBranch).toBe(true);
    expect(harness.workers.prompts).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
      expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread remove is owner-only and does not create a task for a group member", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-owner` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    await seedOwnedWorkspace(harness, target);
    const command = ownerMessage(harness, target, "remove-not-owner", "/thread remove", "member-2");
    const refusal = harness.transport.waitForRemoteText(message =>
      message.target.channelType === target.channelType
      && message.target.channelId === target.channelId
      && message.text.includes("命令仅 owner 可用"));
    await harness.service.submitInbound(command);
    await refusal;

    expect(worktrees.requests).toEqual([]);
    expect(harness.transport.sent.some(({ text }) => text.includes("命令仅 owner 可用"))).toBe(true);
    expect(harness.workers.prompts).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
      expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a shared entry left nonempty after project cleanup remains owned and is not archived", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-shared` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target, { layout: "shared" });
    const marker = join(workspace.entryPath!, "unowned-note.txt");
    await writeFile(marker, "keep this file\n");
    const command = removalCommand(harness, target, "remove-shared-command");
    const reply = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(command);
    await reply;

    expect(worktrees.requests).toHaveLength(1);
    expect(await pathExists(marker)).toBe(true);
    expect(await pathExists(workspace.entryPath!)).toBe(true);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    await withStore(harness, (store) => {
      const persisted = store.getThreadWorkspace(harness.bot.uid, target);
      expect(persisted?.entryPath).toBe(workspace.entryPath);
      expect(persisted?.projects[0]).toMatchObject({ created: false, path: null, branch: null });
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a replaced shared entry symlink and its target stay untouched", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-shared-link` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target, { layout: "shared" });
    const sentinel = join(harness.root, "outside-shared-entry");
    await mkdir(sentinel);
    await rm(workspace.entryPath!, { recursive: true, force: true });
    await symlink(sentinel, workspace.entryPath!);

    const command = removalCommand(harness, target, "remove-shared-link-command");
    const refusal = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(command);
    await refusal;

    expect((await lstat(workspace.entryPath!)).isSymbolicLink()).toBe(true);
    expect(await pathExists(sentinel)).toBe(true);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.entryPath).toBe(workspace.entryPath);
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects).toEqual(workspace.record.projects);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a rebound thread removes the original owned project, not its current binding", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-rebound` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const reboundCwd = join(harness.root, "rebound-thread-cwd");
    const workspace = await seedOwnedWorkspace(harness, target, { bindingCwd: reboundCwd });
    await harness.service.submitInbound(removalCommand(harness, target, "remove-rebound-command"));
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(worktrees.requests).toHaveLength(1);
    expect(worktrees.requests[0]!.path).toBe(workspace.record.projects[0]!.path!);
    expect(worktrees.requests[0]!.path).not.toBe(workspace.bindingCwd);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(false);
    expect(await pathExists(workspace.bindingCwd)).toBe(true);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects[0]).toMatchObject({
        created: false,
        path: null,
        branch: null,
      });
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a missing owned worktree still reaches branch cleanup instead of the incomplete-workspace gate", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-missing` };
  const worktrees = new RecordingWorktrees();
  worktrees.removeBehavior = async (_request, runProcess) => {
    runProcess("branch");
    return { kind: "removed", via: "git", branchOutcome: "deleted" };
  };
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target, {
      missingPaths: ["api"],
      bindingCwd: join(harness.root, "remove-missing-binding"),
    });
    const missingPath = workspace.record.projects[0]!.path!;
    expect(await pathExists(missingPath)).toBe(false);

    await harness.service.submitInbound(removalCommand(harness, target, "remove-missing-command"));
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(worktrees.requests).toHaveLength(1);
    expect(worktrees.requests[0]!.path).toBe(missingPath);
    expect(worktrees.requests[0]!.branch).toBe(workspace.record.projects[0]!.branch!);
    expect((worktrees.requests[0] as RecordedRemovalRequest & { deleteBranch?: boolean }).deleteBranch).toBe(true);
    expect(harness.transport.sent.some(({ text }) => text === INCOMPLETE_WORKSPACE_NOTICE)).toBe(false);
    expect(harness.workers.prompts).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects[0]).toMatchObject({
        created: false,
        path: null,
        branch: null,
      });
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a missing outcome without confirmed branch state retains ownership and cannot archive", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-missing-branch` };
  const worktrees = new RecordingWorktrees();
  worktrees.removeBehavior = async (_request, runProcess) => {
    runProcess("worktree", "not-spawned");
    return { kind: "missing" };
  };
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target, { missingPaths: ["api"] });
    const command = removalCommand(harness, target, "remove-missing-branch-command");
    const refusal = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(command);
    await refusal;

    expect(worktrees.requests).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects[0]).toEqual(
        workspace.record.projects[0],
      );
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a partial branch-removal result retains the original workspace and remaining branch for the next command", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-branch-only` };
  const worktrees = new RecordingWorktrees();
  worktrees.removeBehavior = async (request, runProcess) => {
    if (worktrees.requests.length === 1) {
      runProcess("worktree");
      await rm(request.path, { recursive: true, force: true });
      runProcess("branch");
      return { kind: "removed", via: "git", branchOutcome: "kept" };
    }
    runProcess("branch");
    return { kind: "removed", via: "git", branchOutcome: "deleted" };
  };
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target);
    const firstCommand = removalCommand(harness, target, "remove-branch-partial");
    const firstReply = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(firstCommand);
    await firstReply;

    expect(worktrees.requests).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects[0]).toMatchObject({
        created: true,
        path: workspace.record.projects[0]!.path,
        branch: workspace.record.projects[0]!.branch,
      });
    });

    const retryCommand = removalCommand(harness, target, "remove-branch-retry", "--force");
    await harness.service.submitInbound(retryCommand);
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(worktrees.requests[1]!.path).toBe(workspace.record.projects[0]!.path!);
    expect(worktrees.requests[1]!.repo).toBe(workspace.record.projects[0]!.repo);
    expect(worktrees.requests[1]!.branch).toBe(workspace.record.projects[0]!.branch!);
    expect(worktrees.requests[1]!.force).toBe(true);
    expect((worktrees.requests[1] as RecordedRemovalRequest & { deleteBranch?: boolean }).deleteBranch).toBe(true);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects[0]).toMatchObject({
        created: false,
        path: null,
        branch: null,
      });
      expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an unknown removal process blocks later projects and preserves every unresolved owner row", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-unknown` };
  const worktrees = new RecordingWorktrees();
  worktrees.removeBehavior = async (_request, runProcess) => {
    runProcess("worktree", "unknown");
    throw new Error("removal process exit is unknown");
  };
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target, { names: ["api", "web"] });
    const command = removalCommand(harness, target, "remove-unknown-command");
    const refusal = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(command);
    await refusal;

    expect(worktrees.requests).toHaveLength(1);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(true);
    expect(await pathExists(workspace.record.projects[1]!.path!)).toBe(true);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    await withStore(harness, (store) => {
      expect(store.getThreadWorkspace(harness.bot.uid, target)?.projects).toEqual(workspace.record.projects);
      expect(store.getLatestTask(harness.bot.uid, target)).toBeNull();
      expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
      const leases = store.listHostRemovalLeases(harness.bot.uid);
      expect(leases).toHaveLength(1);
      expect(leases[0]).toMatchObject({
        channelId: target.channelId,
        sourceId: "remove-unknown-command",
        phase: "unknown",
        step: "worktree",
        cwd: workspace.record.projects[0]!.path,
        resourcePaths: expect.arrayContaining([
          workspace.record.projects[0]!.path!,
          workspace.record.projects[0]!.repo,
          workspace.record.projects[1]!.path!,
          workspace.record.projects[1]!.repo,
        ]),
      });
      expect(leases[0]?.processIdentity).not.toBeNull();
      expect(store.listHostMergeLeases(harness.bot.uid)).toEqual([]);
    });
    const retry = removalCommand(harness, target, "remove-unknown-retry");
    const retryRefusal = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(retry);
    await retryRefusal;
    expect(worktrees.requests).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a pending same-thread ask refuses removal immediately even after the binding moved elsewhere", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-busy-ask` };
  const worktrees = new RecordingWorktrees();
  const askStarted = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ promptContext, text }) => {
    const ask = promptContext?.tools?.find(({ name }) => name === "ask");
    if (!ask) throw new Error("ask host tool missing");
    askStarted.resolve();
    await ask.execute(
      { questions: [{ id: "remove-busy", question: text }] },
      new AbortController().signal,
    );
  };
  const harness = await createCoreHarness({ worktreePort: worktrees, prompt });
  try {
    const reboundCwd = join(harness.root, "remove-busy-rebound-cwd");
    const workspace = await seedOwnedWorkspace(harness, target, { bindingCwd: reboundCwd });
    const task = ownerMessage(harness, target, "remove-busy-ask-task", "hold on an ask");
    await harness.service.submitInbound(task);
    await askStarted.promise;
    const waiting = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );

    const removeMessage = removalCommand(harness, target, "remove-busy-ask-command");
    const refusal = waitForNextTargetText(harness, target);
    const remove = harness.service.submitInbound(removeMessage);
    expect(await settlesWithin(remove)).toBe(true);
    await refusal;
    const stillWaiting = await harness.service.status(target);
    expect(stillWaiting.pendingQuestionIds).toEqual(waiting.pendingQuestionIds);
    expect(stillWaiting.queued).toBe(0);
    expect(worktrees.requests).toEqual([]);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(true);
    expect(harness.workers.prompts).toHaveLength(1);

    await harness.service.submitInbound(ownerMessage(harness, target, "remove-busy-ask-answer", "done"));
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an active subdirectory lease refuses removal without waiting or touching the owned worktree", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-subdir` };
  const worktrees = new RecordingWorktrees();
  const started = Promise.withResolvers<void>();
  const stopped = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ onStop }) => {
    onStop(stopped.resolve);
    started.resolve();
    await stopped.promise;
  };
  const harness = await createCoreHarness({ worktreePort: worktrees, prompt });
  try {
    const workspace = await seedOwnedWorkspace(harness, target);
    const nestedCwd = join(workspace.record.projects[0]!.path!, "nested");
    await mkdir(nestedCwd, { recursive: true });
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      store.ensureBinding({
        botUid: harness.bot.uid,
        channelType: 2,
        channelId: "group-b",
        cwd: nestedCwd,
        sessionId: "subdirectory-task-session",
        sessionFile: null,
        cwdPinned: true,
      });
    } finally {
      store.close();
    }

    await harness.service.submitInbound(inbound({
      channelType: 2,
      channelId: "group-b",
      messageId: "remove-subdir-task",
      senderId: "owner-1",
      text: "write inside the nested directory",
      mentionedUserIds: [harness.bot.uid],
    }));
    await started.promise;
    const removeMessage = removalCommand(harness, target, "remove-subdir-command");
    const refusal = waitForNextTargetText(harness, target);
    const remove = harness.service.submitInbound(removeMessage);
    expect(await settlesWithin(remove)).toBe(true);
    await refusal;

    expect(worktrees.requests).toEqual([]);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(true);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a recovered removal receipt stays behind terminal-card drain and never retries cleanup", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-recovery` };
  const worktrees = new RecordingWorktrees();
  const original = await createCoreHarness({ worktreePort: worktrees });
  let originalStopped = false;
  let startup: Promise<CoreHarness> | undefined;
  let restarted: CoreHarness | undefined;
  let transport: RecoveryBlockedTransport | undefined;
  try {
    const workspace = await seedOwnedWorkspace(original, target);
    const project = workspace.record.projects[0]!;
    const command = removalCommand(original, target, "remove-recovery-command");
    const receiptOutboxId = "remove-recovery-receipt";
    await rm(project.path!, { recursive: true, force: true });
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;

    await withStore(original, (store) => {
      store.saveThreadWorkspace({
        ...workspace.record,
        status: "failed",
        error: "cleanup completed before restart",
        projects: [{ ...project, created: false, path: null, branch: null }],
      });
      expect(store.claimCommand({
        botUid: original.bot.uid,
        target: command,
        messageId: command.messageId,
        senderId: command.senderId,
        text: command.text,
        supported: command.supported,
        kind: "command",
      })).toBe(true);
      store.advanceThreadArchiveGeneration(original.bot.uid, target);
      store.enqueueOutbox({
        id: "remove-recovery-body",
        botUid: original.bot.uid,
        ...target,
        taskId: null,
        kind: "text",
        text: REMOVE_RECOVERY_BODY,
        path: null,
        fileKind: null,
        clientMsgNo: "remove-recovery-body",
      });
      store.reserveHostRemovalLease({
        id: "remove-recovery-lease",
        botUid: original.bot.uid,
        target,
        sourceId: command.messageId,
        cwd: project.path!,
        resourcePaths: [project.path!, project.repo],
        commandText: command.text,
        senderId: command.senderId,
        messageSeq: command.messageSeq,
        receiptOutboxId,
      });
      store.persistHostRemovalOutcome("remove-recovery-lease", {
        kind: "complete",
        receiptText: REMOVE_RECOVERY_RECEIPT,
      });
      const tracker = createExecutionTracker({
        runId: "remove-recovery-terminal-task",
        initialAtMs: 1_000,
      });
      tracker.apply({ type: "turn_start", turnId: 1, atMs: 1_000 });
      store.saveProgressCard({
        botUid: original.bot.uid,
        taskId: "remove-recovery-terminal-task",
        ...target,
        messageId: "remove-recovery-terminal-card",
        lastSeq: 4,
        snapshot: encodeExecutionSnapshot(tracker.snapshot(2_000)),
      });
    });

    transport = new RecoveryBlockedTransport({ bot: original.bot });
    startup = createCoreHarness({
      root: original.root,
      bot: original.bot,
      transport,
      worktreePort: worktrees,
    });
    expect(await settlesWithin(transport.recoveryEditStarted.promise, 1_000)).toBe(true);
    expect(transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    transport.releaseRecoveryEdit.resolve();
    restarted = await startup;
    expect(await settlesWithin(transport.recoveryBodyStarted.promise, 1_000)).toBe(true);
    expect(transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    transport.releaseRecoveryBody.resolve();
    expect(await settlesWithin(transport.recoveryReceiptSent.promise, 1_000)).toBe(true);
    expect(await waitForAction(restarted, "thread-archive")).toBe(true);
    expect(worktrees.requests).toEqual([]);
    expect(transport.removalTimeline).toEqual(["body-sent", "receipt-sent", "archive-after-receipt"]);
    await withStore(restarted, (store) => {
      expect(store.listHostRemovalLeases(restarted!.bot.uid)).toEqual([]);
      expect(store.getCommandClaimState(restarted!.bot.uid, target, command.messageId)).toBe("completed");
      expect(store.listThreadMergeEvents(restarted!.bot.uid)).toEqual([]);
    });
  } finally {
    transport?.releaseRecoveryBody.resolve();
    transport?.releaseRecoveryEdit.resolve();
    if (restarted) {
      await closeCoreHarness(restarted);
    } else if (startup) {
      const lateHarness = await startup.catch(() => undefined);
      if (lateHarness) await closeCoreHarness(lateHarness);
      else await rm(original.root, { recursive: true, force: true });
    } else if (originalStopped) {
      await rm(original.root, { recursive: true, force: true });
    } else {
      await closeCoreHarness(original);
    }
  }
});

test("a verified parked process stopped before execution does not block the next removal", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-not-started` };
  const worktrees = new RecordingWorktrees();
  const normal = worktrees.removeBehavior;
  worktrees.removeBehavior = async (_request, runProcess) => {
    runProcess("worktree", "not-started");
    throw new Error("guarded command did not start");
  };
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const workspace = await seedOwnedWorkspace(harness, target);
    const firstCommand = removalCommand(harness, target, "remove-not-started-first");
    const firstRefusal = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(firstCommand);
    await firstRefusal;
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(true);
    await withStore(harness, store => expect(store.listHostRemovalLeases(harness.bot.uid)).toEqual([]));
    worktrees.removeBehavior = normal;
    await harness.service.submitInbound(removalCommand(harness, target, "remove-not-started-retry"));
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(false);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("restart retains an exited removal without a business result and blocks a new destructive command", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-exited-unknown` };
  const worktrees = new RecordingWorktrees();
  const original = await createCoreHarness({ worktreePort: worktrees });
  const workspace = await seedOwnedWorkspace(original, target);
  await closeCoreHarness(original, { remove: false });
  let restarted: CoreHarness | undefined;
  try {
    await withStore(original, store => {
      const path = workspace.record.projects[0]!.path!;
      store.reserveHostRemovalLease({
        id: "exited-removal", botUid: original.bot.uid, target, sourceId: "interrupted-remove",
        cwd: path, resourcePaths: [path, workspace.record.projects[0]!.repo],
        commandText: "/thread remove --force", senderId: "owner-1", messageSeq: 0, receiptOutboxId: "exited-receipt",
      });
      store.beginHostRemovalStep("exited-removal", "worktree", path);
      store.recordHostRemovalProcess("exited-removal", { pid: 48001, pgid: 48001, identity: "fixture", cwd: path });
      store.markHostRemovalProcessExited("exited-removal");
    });
    restarted = await createCoreHarness({ root: original.root, bot: original.bot, worktreePort: worktrees });
    await withStore(restarted, store => expect(store.listHostRemovalLeases(original.bot.uid)).toMatchObject([{
      id: "exited-removal", phase: "unknown", outcome: null,
    }]));
    const blockedCommand = removalCommand(restarted, target, "new-remove-after-exit", "--force");
    const refusal = waitForNextTargetText(restarted, target);
    await restarted.service.submitInbound(blockedCommand);
    await refusal;
    expect(worktrees.requests).toEqual([]);
    expect(await pathExists(workspace.record.projects[0]!.path!)).toBe(true);
    expect(restarted.transport.managementCalls).toEqual([]);
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    else await rm(original.root, { recursive: true, force: true });
  }
});

test("multi-project removal delivers every actual project result within the transport text limit", async () => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____remove-long-receipt` };
  const worktrees = new RecordingWorktrees();
  const harness = await createCoreHarness({ worktreePort: worktrees });
  try {
    const names = Array.from({ length: 12 }, (_, index) => `project-${index}-${"x".repeat(80)}`);
    await seedOwnedWorkspace(harness, target, { names, layout: "shared" });
    await harness.service.submitInbound(removalCommand(harness, target, "long-receipt-command"));
    expect(await waitForAction(harness, "thread-archive")).toBe(true);
    const receipt = harness.transport.sent.filter(message => message.target.channelId === target.channelId);
    const full = receipt.map(message => message.text).join("\n");
    for (const name of names) expect(full.includes(name)).toBe(true);
    expect(receipt.every(message => Buffer.byteLength(message.text) <= 3_000)).toBe(true);
    expect(worktrees.requests).toHaveLength(12);
  } finally {
    await closeCoreHarness(harness);
  }
});

test.each(["sent", "dead"] as const)("archive requires every receipt segment delivered (%s first segment)", async state => {
  const target: ThreadTarget = { channelType: 5, channelId: `${REMOVE_GROUP}____receipt-segments-${state}` };
  const harness = await createCoreHarness();
  try {
    await withStore(harness, async store => {
      for (const id of ["receipt-first", "receipt-last"]) {
        store.enqueueOutbox({ id, botUid: harness.bot.uid, ...target, taskId: null,
          kind: "text", text: id, path: null, fileKind: null, clientMsgNo: id });
        store.markOutboxSent(id);
      }
      if (state === "dead") store.markOutboxDead("receipt-first", "fixture permanent refusal");
      const archives = new ThreadArchiveScheduler({
        store, transport: harness.transport, botUid: () => harness.bot.uid, stopping: () => false,
        outbox: { flush: async () => {}, enqueueText: () => {} },
      });
      archives.schedule(target, REMOVE_GROUP, `receipt-segments-${state}`, 0, ["receipt-first", "receipt-last"]);
      await archives.run(target, Promise.resolve());
      expect(harness.transport.managementCalls.filter(call => call.action === "thread-archive")).toHaveLength(state === "sent" ? 1 : 0);
    });
  } finally {
    await closeCoreHarness(harness);
  }
});
