import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { groupAlive, queryProcess } from "../src/omp/process.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";
import type { WorktreePort } from "../src/contracts.ts";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  FakeWorktrees,
  FakeTransport,
  FakeWorkerFactory,
  waitForStatus,
  type CoreHarness,
  type FakeGoalRunContext,
  type FakePrompt,
} from "./fixtures/core-fakes";
import { WorktreeMergeExecutionError } from "../src/threads/thread-merge.ts";
import type { WorktreeMergeProcessIdentity } from "../src/contracts.ts";
import { ThreadArchiveScheduler } from "../src/threads/thread-archive.ts";
import { ThreadCommands } from "../src/commands/thread-commands.ts";
import { createExecutionTracker, encodeExecutionSnapshot } from "../src/cards/execution-tracker.ts";

async function waitForHostMergeOutcome(
  store: Awaited<ReturnType<typeof openChannelStore>>,
  botUid: string,
  target: { readonly channelType: 5; readonly channelId: string },
  sourceId: string,
  kind: "succeeded" | "execution-failed",
) {
  const deadline = performance.now() + 2_000;
  while (performance.now() < deadline) {
    const lease = store.getHostMergeLeaseBySourceId(botUid, target, sourceId);
    if (lease?.outcome?.kind === kind) return lease;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  throw new Error(`host merge ${kind} outcome was not persisted`);
}
async function waitForSentText(transport: FakeTransport, prefix: string): Promise<void> {
  const deadline = performance.now() + 2_000;
  while (performance.now() < deadline) {
    if (transport.sent.some(({ text }) => text.startsWith(prefix))) return;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  throw new Error(`expected outbound text was not sent: ${prefix}`);
}


// The merge itself is host-owned; the default listen receipt is delivered in the creator chat.
test("/thread merge runs in the host and emits one default merge-listen notice", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____host-first", channelType: 5 as const };
  const parent = { channelId: "group-allowed", channelType: 2 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const message = inbound({
      ...thread,
      messageId: "host-first-success",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(message);
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, parent, status => status.state === "completed");
    await harness.service.handleInbound(message);
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.worktrees?.merges[0]).toMatchObject({ cwd: workspace });
    expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
    expect(harness.workers.prompts.map(({ text }) => text)).toHaveLength(1);
    expect(harness.workers.prompts[0]?.text.startsWith("[子区合并回传]")).toBe(true);
    expect(store.getLatestTask(harness.bot.uid, parent)?.messageId.startsWith("merge-notify:")).toBe(true);
    const events = store.listThreadMergeEvents(harness.bot.uid);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      channelId: thread.channelId,
      targetBranch: "main",
      removed: true,
      archive: "scheduled",
    });
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === thread.channelId && text.includes("main"),
    )).toBe(true);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([
      { action: "thread-archive", groupId: "group-allowed", shortId: "host-first" },
    ]);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

test("/thread merge updates an independent running progress card before the merge settles", async () => {
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const processExitedEditStarted = Promise.withResolvers<void>();
  const releaseProcessExitedEdit = Promise.withResolvers<void>();
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-progress", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  let merge: Promise<void> | undefined;
  vi.useFakeTimers();
  let mergeSettled = false;
  let mergeAcknowledged = false;
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-progress-session",
      sessionFile: null,
      cwdPinned: true,
    });
    harness.transport.reviewProgressCardSupported = true;
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      const { lifecycle, ...mergeRequest } = request;
      lifecycle?.beforeSpawn(request.cwd);
      lifecycle?.onSpawn({
        pid: 0x7fff_ffff,
        pgid: 0x7fff_ffff,
        identity: "pid=2147483647;start=fixture",
        cwd: request.cwd,
      });
      mergeStarted.resolve();
      await releaseMerge.promise;
      const summary = await mergeWorktree(mergeRequest, signal);
      lifecycle?.onConfirmedExit();
      mergeSettled = true;
      return summary;
    };

    const mergeCommand = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-progress-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    merge = mergeCommand;
    void mergeCommand.then(
      () => { mergeAcknowledged = true; },
      () => {},
    );
    await mergeStarted.promise;
    for (let step = 0; step < 8; step += 1) await Promise.resolve();
    expect(mergeAcknowledged).toBe(true);
    expect(mergeSettled).toBe(false);

    const card = harness.transport.reviewProgressCards.find(({ target }) =>
      target.channelType === thread.channelType && target.channelId === thread.channelId);
    expect(card).toBeDefined();
    if (!card) throw new Error("thread merge progress card was not created");
    expect(card.progress.state).toBe("running");
    expect(card.progress.endedAtMs).toBeNull();
    expect(card.progress.hostMerge).toBeDefined();

    vi.advanceTimersByTime(10_000);
    for (let step = 0; step < 8; step += 1) await Promise.resolve();
    const elapsedUpdate = harness.transport.reviewProgressCardEdits.find(edit =>
      edit.messageId === card.messageId
      && edit.progress.state === "running"
      && edit.progress.elapsedMs > card.progress.elapsedMs
      && edit.progress.hostMerge !== undefined);
    expect(elapsedUpdate).toBeDefined();
    if (!elapsedUpdate) throw new Error("thread merge progress card did not update elapsed time");
    expect(elapsedUpdate.target).toEqual(thread);
    expect(elapsedUpdate.progress.endedAtMs).toBeNull();
    expect(mergeSettled).toBe(false);
    expect(harness.transport.reviewProgressCardEdits.some(edit =>
      edit.messageId === card.messageId && edit.progress.state !== "running")).toBe(false);
    let archiveBeforeTerminalCard = false;
    const archiveThread = harness.transport.archiveThread.bind(harness.transport);
    harness.transport.archiveThread = async (groupId: string, shortId: string, signal?: AbortSignal) => {
      archiveBeforeTerminalCard ||= !harness.transport.reviewProgressCardEdits.some(edit =>
        edit.messageId === card.messageId
        && edit.progress.hostMerge !== undefined
        && edit.progress.state !== "running");
      await archiveThread(groupId, shortId, signal);
    };
    const editProgressCard = harness.transport.editProgressCard.bind(harness.transport);
    harness.transport.editProgressCard = async (target, messageId, progress, cardSeq, signal) => {
      if (progress.hostMerge?.phase === "process-exited") {
        processExitedEditStarted.resolve();
        await releaseProcessExitedEdit.promise;
      }
      await editProgressCard(target, messageId, progress, cardSeq, signal);
    };

    releaseMerge.resolve();
    await processExitedEditStarted.promise;
    await merge;
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    releaseProcessExitedEdit.resolve();
    vi.advanceTimersByTime(0);
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(archiveBeforeTerminalCard).toBe(false);
    const terminalCard = harness.transport.reviewProgressCardEdits.find(edit =>
      edit.messageId === card.messageId && edit.progress.hostMerge?.phase === "succeeded");
    expect(terminalCard).toBeDefined();
    if (!terminalCard) throw new Error("thread merge progress card did not record its result");
    expect(terminalCard.progress.endedAtMs).not.toBeNull();
    expect(terminalCard.progress.hostMerge?.result).toContain("fixture-branch");
  } finally {
    releaseMerge.resolve();
    releaseProcessExitedEdit.resolve();
    if (merge !== undefined) await Promise.allSettled([merge]);
    vi.useRealTimers();
    store.close();
    await closeCoreHarness(harness);
  }
});
test("/status responds while /thread merge is still running", async () => {
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-progress-status", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  let merge: Promise<void> | undefined;
  let status: Promise<void> | undefined;
  let mergeSettled = false;
  let mergeAcknowledged = false;
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-progress-status-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      const { lifecycle, ...mergeRequest } = request;
      lifecycle?.beforeSpawn(request.cwd);
      lifecycle?.onSpawn({
        pid: 0x7fff_fffe,
        pgid: 0x7fff_fffe,
        identity: "pid=2147483646;start=fixture",
        cwd: request.cwd,
      });
      mergeStarted.resolve();
      await releaseMerge.promise;
      const summary = await mergeWorktree(mergeRequest, signal);
      lifecycle?.onConfirmedExit();
      mergeSettled = true;
      return summary;
    };

    const mergeCommand = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-progress-status-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    merge = mergeCommand;
    void mergeCommand.then(
      () => { mergeAcknowledged = true; },
      () => {},
    );
    await mergeStarted.promise;
    for (let step = 0; step < 8; step += 1) await Promise.resolve();
    expect(mergeAcknowledged).toBe(true);
    expect(mergeSettled).toBe(false);

    status = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-progress-status-query",
      text: "/status",
      mentionedUserIds: [harness.bot.uid],
    }));
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const statusReturned = await Promise.race([
      status.then(() => true),
      new Promise<boolean>((resolve) => {
        timeout = setTimeout(() => resolve(false), 1_000);
      }),
    ]);
    if (timeout !== undefined) clearTimeout(timeout);
    expect(statusReturned).toBe(true);
    expect(mergeSettled).toBe(false);
  } finally {
    releaseMerge.resolve();
    await Promise.allSettled([
      ...(merge === undefined ? [] : [merge]),
      ...(status === undefined ? [] : [status]),
    ]);
    store.close();
    await closeCoreHarness(harness);
  }
});
test("host merge progress storage rejects an invalid persisted end timestamp", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "group-allowed____merge-progress-invalid-time", channelType: 5 as const };
  const sourceId = "merge-progress-invalid-time";
  const store = await openChannelStore(harness.stateDir);
  const corrupt = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    store.reserveHostMergeProgressCard({
      botUid: harness.bot.uid,
      target,
      sourceId,
      clientMsgNo: "thread-merge-invalid-end-timestamp",
      startedAt: Date.now(),
    });
    corrupt.query(`
      UPDATE host_merge_progress_cards
      SET ended_at = -1
      WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND source_id = ?
    `).run(harness.bot.uid, target.channelId, sourceId);
    expect(() => store.getHostMergeProgressCard(harness.bot.uid, target, sourceId))
      .toThrow("invalid host merge progress end timestamp");
  } finally {
    corrupt.close();
    store.close();
    await closeCoreHarness(harness);
  }
});


test("/thread merge with listen=off consumes success without creating a parent task", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____host-first-off", channelType: 5 as const };
  const parent = { channelId: "group-allowed", channelType: 2 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-off-session",
      sessionFile: null,
      cwdPinned: true,
    });
    store.ensureThreadListen({
      botUid: harness.bot.uid,
      channelId: thread.channelId,
      creatorChannelType: parent.channelType,
      creatorChannelId: parent.channelId,
      threadName: "silent merge",
      listen: "off",
    });
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "host-first-off-success",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    const events = store.listThreadMergeEvents(harness.bot.uid);
    expect(events).toHaveLength(1);
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
    expect(store.getCommandClaimState(
      harness.bot.uid,
      parent,
      `merge-notify-off:${events[0]!.id}`,
    )).toBe("completed");
    expect(store.getLatestTask(harness.bot.uid, parent)).toBeNull();
    expect(harness.workers.prompts).toEqual([]);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("actual host merge and repair failures stay suppressed from the creator chat", async () => {
  const repairStarted = Promise.withResolvers<void>();
  let repairCwd: string | undefined;
  const harness = await createCoreHarness({
    prompt: async ({ cwd }) => {
      if (cwd !== repairCwd) return;
      repairStarted.resolve();
      throw new Error("repair failed");
    },
  });
  const thread = { channelId: "group-allowed____merge-failure-listen", channelType: 5 as const };
  const parent = { channelId: "group-allowed", channelType: 2 as const };
  const workspace = join(harness.root, "workspace");
  repairCwd = workspace;
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-failure-session",
      sessionFile: null,
      cwdPinned: true,
    });
    if (!harness.worktrees) throw new Error("expected fixture worktrees");
    harness.worktrees.mergeFailure = new WorktreeMergeExecutionError(1, "fixture merge failure");
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-failure-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await repairStarted.promise;
    await waitForStatus(harness.service, thread, status => status.state === "failed");
    expect(store.getLatestTask(harness.bot.uid, thread)).toMatchObject({
      messageId: "thread-merge-repair:merge-failure-command",
      relaySuppressed: true,
      state: "failed",
    });
    expect(store.getThreadListen(harness.bot.uid, thread)).toMatchObject({ mergePending: true });
    expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    expect(store.getLatestTask(harness.bot.uid, parent)).toBeNull();
    expect(harness.workers.prompts.some(({ text }) => text.startsWith("[子区合并回传]"))).toBe(false);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("/thread merge redelivery after restart does not repeat the completed merge", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-restart", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const message = inbound({
    ...thread,
    messageId: "merge-restart-command",
    text: "/thread merge",
    mentionedUserIds: [harness.bot.uid],
  });
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  let originalStopped = false;
  try {
    await mkdir(workspace);
    const setupStore = await openChannelStore(harness.stateDir);
    try {
      setupStore.ensureBinding({
        botUid: harness.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: "merge-restart-session",
        sessionFile: null,
        cwdPinned: true,
      });
    } finally {
      setupStore.close();
    }
    await harness.service.handleInbound(message);
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    await closeCoreHarness(harness, { remove: false });
    originalStopped = true;

    restarted = await createCoreHarness({ root: harness.root, bot: harness.bot });
    const restartedStore = await openChannelStore(restarted.stateDir);
    try {
      await restarted.service.handleInbound(message);
      expect(restarted.worktrees?.merges).toEqual([]);
      expect(restartedStore.getLatestTask(restarted.bot.uid, thread)).toBeNull();
      expect(restarted.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    } finally {
      restartedStore.close();
    }
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    else if (!originalStopped) await closeCoreHarness(harness);
  }
});
test("startup recovers a known host merge outcome through one listen event", async () => {
  const original = await createCoreHarness();
  const thread = { channelId: "group-allowed____known-outcome", channelType: 5 as const };
  const parent = { channelId: "group-allowed", channelType: 2 as const };
  const workspace = join(original.root, "workspace");
  const parentWorkspace = join(original.root, "known-outcome-parent");
  const sourceId = "known-outcome-command";
  const queuedParentText = "queued parent task before merge notice";
  let first: CoreHarness | undefined;
  let second: CoreHarness | undefined;
  let originalStopped = false;
  const notices: string[] = [];
  try {
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    await mkdir(workspace);
    await mkdir(parentWorkspace);
    const setup = await openChannelStore(original.stateDir);
    try {
      setup.ensureBinding({
        botUid: original.bot.uid,
        ...parent,
        cwd: parentWorkspace,
        sessionId: "known-outcome-parent-session",
        sessionFile: null,
        cwdPinned: false,
      });
      const queuedParentMessageId = "known-outcome-parent-task-message";
      expect(setup.acceptTask(
        {
          botUid: original.bot.uid,
          target: parent,
          messageId: queuedParentMessageId,
          senderId: "owner-1",
          text: queuedParentText,
          supported: true,
          kind: "task",
        },
        {
          id: "known-outcome-parent-task",
          botUid: original.bot.uid,
          channelType: parent.channelType,
          channelId: parent.channelId,
          messageId: queuedParentMessageId,
          senderId: "owner-1",
          text: queuedParentText,
          sessionId: "known-outcome-parent-session",
          cwd: parentWorkspace,
          state: "queued",
          resultText: null,
          errorText: null,
          messageSeq: 0,
          historyEligible: true,
          relaySuppressed: false,
        },
      )).toBe(true);
      setup.ensureBinding({
        botUid: original.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: "known-outcome-session",
        sessionFile: null,
        cwdPinned: true,
      });
      setup.ensureThreadListen({
        botUid: original.bot.uid,
        channelId: thread.channelId,
        creatorChannelType: parent.channelType,
        creatorChannelId: parent.channelId,
        threadName: "known outcome",
        listen: "merge",
      });
      expect(setup.claimCommand({
        botUid: original.bot.uid,
        target: thread,
        messageId: sourceId,
        senderId: "owner-1",
        text: "/thread merge",
        supported: true,
        kind: "command",
      })).toBe(true);
      setup.reserveHostMergeLease({
        id: "known-outcome-lease",
        botUid: original.bot.uid,
        target: thread,
        sourceId,
        cwd: workspace,
        resourceCwds: [workspace],
        commandText: "/thread merge",
        operatorNote: "",
        senderId: "owner-1",
        messageSeq: 17,
        receiptOutboxId: "known-outcome-receipt",
      });
      setup.recordHostMergeProcess("known-outcome-lease", {
        pid: 43003,
        pgid: 43003,
        identity: "pid=43003;start=Wed Jun 10 12:01:00 2026",
      });
      setup.markHostMergeProcessExited("known-outcome-lease");
      setup.persistHostMergeOutcome("known-outcome-lease", {
        kind: "succeeded",
        summary: {
          branch: "known-outcome-branch",
          target: "main",
          squashed: true,
          rebased: true,
          committed: true,
          removed: false,
          commit: "known-outcome-commit",
        },
      });
    } finally {
      setup.close();
    }

    const prompt: FakePrompt = async ({ text }) => {
      if (text.startsWith("[子区合并回传]")) notices.push(text);
    };
    first = await createCoreHarness({ root: original.root, bot: original.bot, prompt });
    await first.workers.waitForPromptCount(2);
    const promptTexts = first.workers.prompts.map(({ text }) => text);
    const queuedPromptIndex = promptTexts.findIndex(text => text.includes(queuedParentText));
    const noticePromptIndex = promptTexts.findIndex(text => text.startsWith("[子区合并回传]"));
    expect(queuedPromptIndex).toBeGreaterThanOrEqual(0);
    expect(noticePromptIndex).toBeGreaterThan(queuedPromptIndex);
    await waitForStatus(first.service, parent, status => status.state === "completed");
    const recovered = await openChannelStore(first.stateDir);
    try {
      const events = recovered.listThreadMergeEvents(first.bot.uid);
      expect(events).toHaveLength(1);
      expect(events[0]?.commit).toBe("known-outcome-commit");
      expect(recovered.isThreadMergeEventPending(events[0]!.id)).toBe(false);
      expect(recovered.listHostMergeLeases(first.bot.uid)).toEqual([]);
      expect(recovered.getCommandClaimState(first.bot.uid, thread, sourceId)).toBe("completed");
      expect(recovered.getLatestTask(first.bot.uid, parent)?.messageId).toBe(
        `merge-notify:${events[0]!.id}`,
      );
    } finally {
      recovered.close();
    }
    await closeCoreHarness(first, { remove: false });
    first = undefined;

    second = await createCoreHarness({ root: original.root, bot: original.bot, prompt });
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const tick = Promise.withResolvers<void>();
      setImmediate(tick.resolve);
      await tick.promise;
    }
    expect(notices).toHaveLength(1);
    expect(second.workers.prompts).toEqual([]);
    const reopened = await openChannelStore(second.stateDir);
    try {
      expect(reopened.listThreadMergeEvents(second.bot.uid)).toHaveLength(1);
      expect(reopened.getLatestTask(second.bot.uid, parent)?.messageId).toMatch(/^merge-notify:/);
    } finally {
      reopened.close();
    }
  } finally {
    if (second) await closeCoreHarness(second);
    if (first) await closeCoreHarness(first);
    if (!originalStopped) await closeCoreHarness(original);
  }
});

test("startup recovers unresolved host merge leases and resumes queued work", async () => {
  const original = await createCoreHarness();
  const recoveryStarted = Promise.withResolvers<void>();
  const releaseRecovery = Promise.withResolvers<void>();
  const workerStarted = Promise.withResolvers<void>();
  let originalStopped = false;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  let startup: ReturnType<typeof createCoreHarness> | undefined;
  const thread = { channelId: "group-allowed____merge-recovery", channelType: 5 as const };
  const workspace = join(original.root, "workspace");
  const taskId = "merge-recovery-queued-task";
  const text = "resume queued work after unknown merge recovery";
  const processIdentity: WorktreeMergeProcessIdentity = {
    pid: 43001,
    pgid: 43001,
    identity: "pid=43001;start=Wed Jun 10 12:00:00 2026",
    cwd: workspace,
  };
  class RecoverableWorktrees extends FakeWorktrees {
    readonly recovered: WorktreeMergeProcessIdentity[] = [];

    async recoverMergeProcess(identity: WorktreeMergeProcessIdentity): Promise<boolean> {
      this.recovered.push(identity);
      recoveryStarted.resolve();
      await releaseRecovery.promise;
      return true;
    }
  }

  try {
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    await mkdir(workspace);
    const setup = await openChannelStore(original.stateDir);
    try {
      setup.ensureBinding({
        botUid: original.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: "merge-recovery-session",
        sessionFile: null,
        cwdPinned: true,
      });
      const messageId = "merge-recovery-task-message";
      expect(setup.acceptTask(
        {
          botUid: original.bot.uid,
          target: thread,
          messageId,
          senderId: "owner-1",
          text,
          supported: true,
          kind: "task",
        },
        {
          id: taskId,
          botUid: original.bot.uid,
          channelType: thread.channelType,
          channelId: thread.channelId,
          messageId,
          senderId: "owner-1",
          text,
          sessionId: "merge-recovery-session",
          cwd: workspace,
          state: "queued",
          resultText: null,
          errorText: null,
          messageSeq: 0,
          historyEligible: true,
          relaySuppressed: false,
        },
      )).toBe(true);
      setup.reserveHostMergeLease({
        id: "unresolved-host-merge",
        botUid: original.bot.uid,
        target: thread,
        sourceId: "merge-recovery-command",
        cwd: workspace,
        resourceCwds: [workspace],
      });
      setup.recordHostMergeProcess("unresolved-host-merge", processIdentity);
    } finally {
      setup.close();
    }

    const worktrees = new RecoverableWorktrees(original.root);
    const start = createCoreHarness({
      root: original.root,
      bot: original.bot,
      worktreePort: worktrees,
      prompt: async () => {
        workerStarted.resolve();
      },
    });
    startup = start;
    await recoveryStarted.promise;
    releaseRecovery.resolve();
    restarted = await start;
    await workerStarted.promise;

    expect(worktrees.recovered).toEqual([processIdentity]);
    expect(restarted.workers.prompts.map(({ text: promptText }) => promptText)).toEqual([text]);
    const recoveredStore = await openChannelStore(restarted.stateDir);
    try {
      expect(recoveredStore.getTask(taskId)?.state).not.toBe("queued");
      expect(recoveredStore.listHostMergeLeases(original.bot.uid)).toMatchObject([
        { id: "unresolved-host-merge", phase: "unknown", cwd: workspace },
      ]);
    } finally {
      recoveredStore.close();
    }
  } finally {
    releaseRecovery.resolve();
    if (restarted) {
      await closeCoreHarness(restarted);
    } else if (startup) {
      const lateHarness = await startup.catch(() => undefined);
      if (lateHarness) await closeCoreHarness(lateHarness);
      else await rm(original.root, { recursive: true, force: true });
    } else if (!originalStopped) {
      await closeCoreHarness(original);
    } else {
      await rm(original.root, { recursive: true, force: true });
    }
  }
});

test("a progress-card terminal persistence failure does not replace a completed merge outcome", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-card-persistence-failure", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  const fault = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-card-persistence-failure-session",
      sessionFile: null,
      cwdPinned: true,
    });
    harness.transport.reviewProgressCardSupported = true;
    fault.exec(`
      CREATE TRIGGER fail_terminal_host_merge_progress
      BEFORE UPDATE OF phase ON host_merge_progress_cards
      WHEN NEW.phase = 'succeeded'
      BEGIN SELECT RAISE(ABORT, 'terminal progress persistence fault'); END;
    `);
    const message = inbound({
      ...thread,
      messageId: "merge-card-persistence-failure-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });

    await harness.service.handleInbound(message);
    await waitForSentText(harness.transport, "子区合并成功：");

    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === thread.channelId && text.startsWith("子区合并成功："),
    )).toBe(true);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === thread.channelId && text.startsWith("合并未完成"),
    )).toBe(false);
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
  } finally {
    fault.exec("DROP TRIGGER IF EXISTS fail_terminal_host_merge_progress");
    fault.close();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("a progress-card terminal persistence failure still accepts the merge repair task", async () => {
  const worktrees = new FakeWorktrees("unused");
  worktrees.mergeFailure = new WorktreeMergeExecutionError(1, "terminal-card failure fixture");
  const harness = await createCoreHarness({ worktreePort: worktrees });
  const thread = { channelId: "group-allowed____merge-card-repair", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  const fault = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-card-repair-session",
      sessionFile: null,
      cwdPinned: true,
    });
    harness.transport.reviewProgressCardSupported = true;
    fault.exec(`
      CREATE TRIGGER fail_failed_host_merge_progress
      BEFORE UPDATE OF phase ON host_merge_progress_cards
      WHEN NEW.phase = 'execution-failed'
      BEGIN SELECT RAISE(ABORT, 'terminal progress persistence fault'); END;
    `);
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-card-repair-command",
      text: "/thread merge, keep the repair note",
      mentionedUserIds: [harness.bot.uid],
    }));

    await harness.workers.waitForPromptCount(1);
    expect(worktrees.merges).toHaveLength(1);
    expect(store.getLatestTask(harness.bot.uid, thread)).toMatchObject({
      messageId: "thread-merge-repair:merge-card-repair-command",
      text: expect.stringContaining("keep the repair note"),
    });
    const lease = store.getHostMergeLeaseBySourceId(
      harness.bot.uid,
      thread,
      "merge-card-repair-command",
    );
    if (lease === null) {
      expect(store.getHostMergeProgressCard(
        harness.bot.uid,
        thread,
        "merge-card-repair-command",
      )).toBeNull();
    } else {
      expect(lease).toMatchObject({ outcome: { kind: "execution-failed" } });
    }
  } finally {
    fault.exec("DROP TRIGGER IF EXISTS fail_failed_host_merge_progress");
    fault.close();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("startup with a removed workspace waits for card recovery before replaying a delivered merge archive", async () => {
  const original = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-card-recovery", channelType: 5 as const };
  const workspace = join(original.root, "workspace");
  const receiptOutboxId = "merge-card-recovery-receipt";
  let originalStopped = false;
  let startup: ReturnType<typeof createCoreHarness> | undefined;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  class RecoveryBlockedTransport extends FakeTransport {
    readonly recoveryEditStarted = Promise.withResolvers<void>();
    readonly releaseRecoveryEdit = Promise.withResolvers<void>();

    override async editProgressCard(
      ...args: Parameters<FakeTransport["editProgressCard"]>
    ): Promise<void> {
      this.recoveryEditStarted.resolve();
      await this.releaseRecoveryEdit.promise;
      await super.editProgressCard(...args);
    }
  }
  const transport = new RecoveryBlockedTransport({ bot: original.bot });

  try {
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    const store = await openChannelStore(original.stateDir);
    try {
      store.ensureBinding({
        botUid: original.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: "merge-card-recovery-session",
        sessionFile: null,
        cwdPinned: true,
      });
      expect(store.claimCommand({
        botUid: original.bot.uid,
        target: thread,
        messageId: "merge-card-recovery-command",
        senderId: "owner-1",
        text: "/thread merge",
        supported: true,
        kind: "command",
      })).toBe(true);
      store.reserveHostMergeLease({
        id: "merge-card-recovery-lease",
        botUid: original.bot.uid,
        target: thread,
        sourceId: "merge-card-recovery-command",
        cwd: workspace,
        resourceCwds: [workspace],
        commandText: "/thread merge",
        archiveGeneration: null,
        receiptOutboxId,
      });
      store.recordHostMergeProcess("merge-card-recovery-lease", {
        pid: 43_011,
        pgid: 43_011,
        identity: "pid=43011;start=Wed Jun 10 12:00:00 2026",
      });
      store.markHostMergeProcessExited("merge-card-recovery-lease");
      store.persistHostMergeOutcome("merge-card-recovery-lease", {
        kind: "succeeded",
        summary: {
          branch: "fixture-branch",
          target: "main",
          squashed: true,
          rebased: true,
          committed: true,
          removed: true,
        },
      });
      store.reserveHostMergeProgressCard({
        botUid: original.bot.uid,
        target: thread,
        sourceId: "merge-card-recovery-command",
        clientMsgNo: "thread-merge-ready-card-recovery",
        startedAt: Date.now(),
      });
      store.enqueueOutbox({
        id: receiptOutboxId,
        botUid: original.bot.uid,
        ...thread,
        taskId: null,
        kind: "text",
        text: "子区合并成功：fixture-branch → main；已清理工作区与已合并分支；回执送达后将归档子区。",
        path: null,
        fileKind: null,
        clientMsgNo: receiptOutboxId,
      });
      store.markOutboxSent(receiptOutboxId);
      const tracker = createExecutionTracker({
        runId: "merge-card-recovery-legacy-task",
        initialAtMs: 1_000,
      });
      tracker.apply({ type: "turn_start", turnId: 1, atMs: 1_000 });
      store.saveProgressCard({
        botUid: original.bot.uid,
        taskId: "merge-card-recovery-legacy-task",
        ...thread,
        messageId: "merge-card-recovery-legacy-card",
        lastSeq: 4,
        snapshot: encodeExecutionSnapshot(tracker.snapshot(2_000)),
      });
    } finally {
      store.close();
    }

    const start = createCoreHarness({
      root: original.root,
      bot: original.bot,
      transport,
    });
    startup = start;
    await transport.recoveryEditStarted.promise;
    const recoveringStore = await openChannelStore(original.stateDir);
    try {
      expect(recoveringStore.getHostMergeLeaseBySourceId(
        original.bot.uid,
        thread,
        "merge-card-recovery-command",
      )).not.toBeNull();
    } finally {
      recoveringStore.close();
    }
    expect(transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);

    transport.releaseRecoveryEdit.resolve();
    restarted = await start;
    await transport.waitForManagementCalls(({ action }) => action === "thread-archive");

    expect(transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([
      { action: "thread-archive", groupId: "group-allowed", shortId: "merge-card-recovery" },
    ]);
    expect(restarted.worktrees?.merges).toEqual([]);
  } finally {
    transport.releaseRecoveryEdit.resolve();
    if (restarted) await closeCoreHarness(restarted);
    else if (startup) {
      const lateHarness = await startup.catch(() => undefined);
      if (lateHarness) await closeCoreHarness(lateHarness);
      else await rm(original.root, { recursive: true, force: true });
    } else if (!originalStopped) {
      await closeCoreHarness(original);
    } else {
      await rm(original.root, { recursive: true, force: true });
    }
  }
});
test("startup does not restore a merge archive cancelled by a newer inbound", async () => {
  const original = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-archive-cancelled", channelType: 5 as const };
  const sourceId = "merge-archive-cancelled-command";
  const receiptOutboxId = "merge-archive-cancelled-receipt";
  let originalStopped = false;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  try {
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    const store = await openChannelStore(original.stateDir);
    try {
      const target = {
        channelType: thread.channelType,
        channelId: thread.channelId,
      };
      const command = {
        botUid: original.bot.uid,
        target,
        messageId: sourceId,
        senderId: "owner-1",
        text: "/thread merge",
        supported: true,
        kind: "command" as const,
      };
      expect(store.claimCommand(command)).toBe(true);
      store.reserveHostMergeLease({
        id: "merge-archive-cancelled-lease",
        botUid: original.bot.uid,
        target,
        sourceId,
        cwd: join(original.root, "workspace"),
        resourceCwds: [join(original.root, "workspace")],
        commandText: command.text,
        senderId: command.senderId,
        receiptOutboxId,
        archiveGeneration: 0,
      });
      store.recordHostMergeProcess("merge-archive-cancelled-lease", {
        pid: 43_012,
        pgid: 43_012,
        identity: "pid=43012;start=Wed Jun 10 12:00:00 2026",
      });
      store.markHostMergeProcessExited("merge-archive-cancelled-lease");
      store.persistHostMergeOutcome("merge-archive-cancelled-lease", {
        kind: "succeeded",
        summary: {
          branch: "fixture-branch",
          target: "main",
          squashed: true,
          rebased: true,
          committed: true,
          removed: true,
        },
      });
      store.advanceThreadArchiveGeneration(original.bot.uid, target);
    } finally {
      store.close();
    }

    const transport = new FakeTransport({ bot: original.bot });
    restarted = await createCoreHarness({
      root: original.root,
      bot: original.bot,
      transport,
    });
    await waitForSentText(transport, "子区合并成功");
    const recoveredStore = await openChannelStore(restarted.stateDir);
    try {
      expect(recoveredStore.getHostMergeLeaseBySourceId(original.bot.uid, thread, sourceId)).toBeNull();
      expect(recoveredStore.getThreadArchiveGeneration(original.bot.uid, thread)).toBe(1);
      expect(
        recoveredStore.listThreadMergeEvents(original.bot.uid)
          .find(event => event.channelId === thread.channelId),
      ).toMatchObject({ removed: true, archive: "not-scheduled" });
    } finally {
      recoveredStore.close();
    }
    expect(restarted.worktrees?.merges).toEqual([]);
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    else if (!originalStopped) await closeCoreHarness(original);
    else await rm(original.root, { recursive: true, force: true });
  }
});

test("startup kills unresolved merge processes, resumes queued work, and does not replay", async () => {
  const original = await createCoreHarness();
  const recoveryStarted = Promise.withResolvers<void>();
  const releaseRecovery = Promise.withResolvers<void>();
  const workers = new FakeWorkerFactory(async () => {});
  const thread = { channelId: "group-allowed____merge-real-recovery", channelType: 5 as const };
  const workspace = join(original.root, "workspace");
  const taskId = "merge-real-recovery-queued-task";
  const text = "queued work after exact process recovery";
  let child: ChildProcess | undefined;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  let startup: ReturnType<typeof createCoreHarness> | undefined;
  let originalStopped = false;
  let mergeCalls = 0;
  const recovered: WorktreeMergeProcessIdentity[] = [];
  try {
    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    await mkdir(workspace);
    child = spawn("/bin/sh", ["-c", "sleep 60 & wait"], {
      detached: true,
      stdio: "ignore",
    });
    const spawned = Promise.withResolvers<void>();
    child.once("spawn", spawned.resolve);
    child.once("error", spawned.reject);
    await spawned.promise;
    child.unref();
    if (child.pid === undefined) throw new Error("detached merge fixture did not start");
    const snapshot = await queryProcess(child.pid);
    if (!snapshot || snapshot.pgid !== child.pid) {
      throw new Error("detached merge fixture did not own its process group");
    }
    const processIdentity: WorktreeMergeProcessIdentity = {
      pid: child.pid,
      pgid: snapshot.pgid,
      identity: snapshot.identity,
      cwd: workspace,
    };
    const setup = await openChannelStore(original.stateDir);
    try {
      setup.ensureBinding({
        botUid: original.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: "merge-real-recovery-session",
        sessionFile: null,
        cwdPinned: true,
      });
      const messageId = "merge-real-recovery-task-message";
      expect(setup.acceptTask(
        {
          botUid: original.bot.uid,
          target: thread,
          messageId,
          senderId: "owner-1",
          text,
          supported: true,
          kind: "task",
        },
        {
          id: taskId,
          botUid: original.bot.uid,
          channelType: thread.channelType,
          channelId: thread.channelId,
          messageId,
          senderId: "owner-1",
          text,
          sessionId: "merge-real-recovery-session",
          cwd: workspace,
          state: "queued",
          resultText: null,
          errorText: null,
          messageSeq: 0,
          historyEligible: true,
          relaySuppressed: false,
        },
      )).toBe(true);
      setup.reserveHostMergeLease({
        id: "real-unresolved-host-merge",
        botUid: original.bot.uid,
        target: thread,
        sourceId: "real-merge-recovery-command",
        cwd: workspace,
        resourceCwds: [workspace],
      });
      setup.recordHostMergeProcess("real-unresolved-host-merge", processIdentity);
    } finally {
      setup.close();
    }

    const realPort = createWorktreePort({ command: "omp" });
    const recover = realPort.recoverMergeProcess;
    if (!recover) throw new Error("worktree port lacks merge process recovery");
    const worktrees: WorktreePort = {
      create: (request, signal) => realPort.create(request, signal),
      planMerge: (request, signal) => realPort.planMerge(request, signal),
      merge: (request, signal) => {
        mergeCalls += 1;
        return realPort.merge(request, signal);
      },
      remove: (request, signal) => realPort.remove(request, signal),
      worktrunk: signal => realPort.worktrunk(signal),
      recoverMergeProcess: async identity => {
        recovered.push(identity);
        recoveryStarted.resolve();
        await releaseRecovery.promise;
        return recover(identity);
      },
    };
    const start = createCoreHarness({
      root: original.root,
      bot: original.bot,
      workers,
      worktreePort: worktrees,
    });
    startup = start;
    await recoveryStarted.promise;
    expect(workers.prompts).toEqual([]);
    releaseRecovery.resolve();
    restarted = await start;
    await workers.waitForPromptCount(1);

    expect(recovered).toEqual([processIdentity]);
    expect(await queryProcess(processIdentity.pid)).toBeUndefined();
    expect(groupAlive(processIdentity.pgid)).toBe(false);
    expect(mergeCalls).toBe(0);
    expect(workers.prompts.map(({ text: promptText }) => promptText)).toEqual([text]);
    const recoveredStore = await openChannelStore(restarted.stateDir);
    try {
      expect(recoveredStore.getTask(taskId)?.state).not.toBe("queued");
      expect(recoveredStore.listHostMergeLeases(original.bot.uid)).toMatchObject([
        { id: "real-unresolved-host-merge", phase: "unknown", cwd: workspace },
      ]);
    } finally {
      recoveredStore.close();
    }
  } finally {
    releaseRecovery.resolve();
    if (child?.pid !== undefined) {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {}
      child.kill("SIGKILL");
    }
    if (restarted) {
      await closeCoreHarness(restarted);
    } else if (startup) {
      const lateHarness = await startup.catch(() => undefined);
      if (lateHarness) await closeCoreHarness(lateHarness);
      else await rm(original.root, { recursive: true, force: true });
    } else if (!originalStopped) {
      await closeCoreHarness(original);
    } else {
      await rm(original.root, { recursive: true, force: true });
    }
  }
});

test("/thread merge waits for a running workspace task before acquiring its lease", async () => {
  const started = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text !== "existing workspace task") return;
      started.resolve();
      await finish.promise;
    },
  });
  const thread = { channelId: "group-allowed____merge-wait", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-wait-session",
      sessionFile: null,
      cwdPinned: true,
    });
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-wait-task",
      text: "existing workspace task",
      mentionedUserIds: [harness.bot.uid],
    }));
    await started.promise;

    const merge = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-wait-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    expect(harness.worktrees?.merges).toEqual([]);

    finish.resolve();
    await merge;
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.worktrees?.merges[0]).toMatchObject({ cwd: workspace });
    expect(harness.workers.prompts.filter(({ text }) => text === "existing workspace task")).toHaveLength(1);
  } finally {
    finish.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("a host merge reservation runs before later same-chat workspace writes", async () => {
  const earlierStarted = Promise.withResolvers<void>();
  const laterStarted = Promise.withResolvers<void>();
  const finishEarlier = Promise.withResolvers<void>();
  const finishLater = Promise.withResolvers<void>();
  const mergeStarted = Promise.withResolvers<void>();
  const finishMerge = Promise.withResolvers<void>();
  const mergeTypingSent = Promise.withResolvers<void>();
  let typingCalls = 0;
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "earlier workspace write") {
        earlierStarted.resolve();
        await finishEarlier.promise;
      } else if (text === "later workspace write") {
        laterStarted.resolve();
        await finishLater.promise;
      }
    },
  });
  const thread = { channelId: "group-allowed____merge-reservation", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-reservation-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const sendTyping = harness.transport.sendTyping.bind(harness.transport);
    harness.transport.sendTyping = async (target, signal) => {
      await sendTyping(target, signal);
      if (target.channelId === thread.channelId && ++typingCalls === 2) {
        mergeTypingSent.resolve();
      }
    };
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      mergeStarted.resolve();
      await finishMerge.promise;
      return mergeWorktree(request, signal);
    };
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-reservation-earlier",
      text: "earlier workspace write",
      mentionedUserIds: [harness.bot.uid],
    }));
    await earlierStarted.promise;
    const merge = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-reservation-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeTypingSent.promise;
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-reservation-later",
      text: "later workspace write",
      mentionedUserIds: [harness.bot.uid],
    }));

    finishEarlier.resolve();
    const firstOperation = await Promise.race([
      mergeStarted.promise.then(() => "merge" as const),
      laterStarted.promise.then(() => "later-write" as const),
    ]);
    expect(firstOperation).toBe("merge");
    expect(harness.worktrees?.merges).toEqual([]);

    finishMerge.resolve();
    await merge;
    await laterStarted.promise;
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.worktrees?.merges[0]).toMatchObject({ cwd: workspace });
  } finally {
    finishEarlier.resolve();
    finishLater.resolve();
    finishMerge.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("/thread merge waits for the previous task's terminal progress-card edit", async () => {
  const taskStarted = Promise.withResolvers<void>();
  const finishTask = Promise.withResolvers<void>();
  const terminalEditStarted = Promise.withResolvers<void>();
  const finishTerminalEdit = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text, emit }) => {
      if (text !== "progress-tail task") return;
      emit({
        type: "tool_start",
        toolCallId: "progress-tail-tool",
        toolName: "bash",
        atMs: Date.now(),
      });
      taskStarted.resolve();
      await finishTask.promise;
    },
  });
  const thread = { channelId: "group-allowed____merge-progress-tail", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-progress-tail-session",
      sessionFile: null,
      cwdPinned: true,
    });
    harness.transport.sendProgressCard = async () => ({ messageId: "progress-tail-card" });
    const editProgressCard = harness.transport.editProgressCard.bind(harness.transport);
    harness.transport.editProgressCard = async (target, messageId, progress, cardSeq, signal) => {
      if (progress.state !== "running") {
        terminalEditStarted.resolve();
        await finishTerminalEdit.promise;
      }
      await editProgressCard(target, messageId, progress, cardSeq, signal);
    };
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-progress-tail-task",
      text: "progress-tail task",
      mentionedUserIds: [harness.bot.uid],
    }));
    await taskStarted.promise;
    const merge = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-progress-tail-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));

    finishTask.resolve();
    await terminalEditStarted.promise;
    await merge;
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);

    finishTerminalEdit.resolve();
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.worktrees?.merges[0]).toMatchObject({ cwd: workspace });
  } finally {
    finishTask.resolve();
    finishTerminalEdit.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("/thread merge runs concurrently with queued cross-chat project work", async () => {
  const firstStarted = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const releaseFirst = Promise.withResolvers<void>();
  const releaseSecond = Promise.withResolvers<void>();
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "first project task") {
        firstStarted.resolve();
        await releaseFirst.promise;
      } else if (text === "queued project task") {
        secondStarted.resolve();
        await releaseSecond.promise;
      }
    },
  });
  const thread = { channelId: "group-allowed____cross-chat-merge", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const project = join(workspace, "project");
  const nested = join(project, "nested");
  const group = { channelId: "group-allowed", channelType: 2 as const };
  await mkdir(nested, { recursive: true });
  const store = await openChannelStore(harness.stateDir);
  let merge: Promise<void> | undefined;
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: project,
      sessionId: "cross-chat-merge-session",
      sessionFile: null,
      cwdPinned: true,
    });
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...group,
      cwd: nested,
      sessionId: "cross-chat-project-session",
      sessionFile: null,
      cwdPinned: true,
    });
    store.setChatMode(harness.bot.uid, group, "queue");
    store.saveThreadWorkspace({
      botUid: harness.bot.uid,
      ...thread,
      layout: "single",
      status: "ready",
      entryPath: workspace,
      projects: [{
        name: "api",
        repo: "/repos/api",
        path: project,
        branch: "thread-api",
        created: true,
      }],
      error: null,
    });
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      mergeStarted.resolve();
      await releaseMerge.promise;
      return mergeWorktree(request, signal);
    };
    await harness.service.handleInbound(inbound({
      ...group,
      messageId: "cross-chat-first-task",
      text: "first project task",
      mentionedUserIds: [harness.bot.uid],
    }));
    await firstStarted.promise;
    await harness.service.handleInbound(inbound({
      ...group,
      messageId: "cross-chat-queued-task",
      text: "/queue queued project task",
      mentionedUserIds: [harness.bot.uid],
    }));
    expect(store.getLatestTask(harness.bot.uid, group)).toMatchObject({
      messageId: "cross-chat-queued-task",
      state: "queued",
    });

    merge = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "cross-chat-merge-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeStarted.promise;
    releaseFirst.resolve();
    await secondStarted.promise;

    releaseSecond.resolve();
    releaseMerge.resolve();
    await merge;
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(harness.worktrees?.merges[0]).toMatchObject({
      cwd: project,
      repo: "/repos/api",
      branch: "thread-api",
    });
    expect(harness.workers.prompts.map(({ text }) => text).filter(
      text => text === "first project task" || text === "queued project task",
    )).toEqual([
      "first project task",
      "queued project task",
    ]);
  } finally {
    releaseFirst.resolve();
    releaseSecond.resolve();
    releaseMerge.resolve();
    if (merge !== undefined) await Promise.allSettled([merge]);
    store.close();
    await closeCoreHarness(harness);
  }
});

test("a merge reservation drains run and queued work before it, then queues late default steering", async () => {
  const runningStarted = Promise.withResolvers<void>();
  const queuedStarted = Promise.withResolvers<void>();
  const lateStarted = Promise.withResolvers<void>();
  const mergeReservationStarted = Promise.withResolvers<void>();
  const mergeStarted = Promise.withResolvers<void>();
  const releaseRunning = Promise.withResolvers<void>();
  const releaseQueued = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const sequence: string[] = [];
  let runningAppended: () => string[] = () => [];
  const harness = await createCoreHarness({
    prompt: async ({ text, appended }) => {
      if (text === "running before host merge") {
        runningAppended = () => appended().map(({ text: value }) => value);
        sequence.push("running");
        runningStarted.resolve();
        await releaseRunning.promise;
      } else if (text === "queued before host merge") {
        sequence.push("queued");
        queuedStarted.resolve();
        await releaseQueued.promise;
      } else if (text === "late default-steer message") {
        sequence.push("late");
        lateStarted.resolve();
      }
    },
  });
  const thread = { channelId: "group-allowed____merge-ordering", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-ordering-session",
      sessionFile: null,
      cwdPinned: true,
    });
    let typingCalls = 0;
    const sendTyping = harness.transport.sendTyping.bind(harness.transport);
    harness.transport.sendTyping = async (target, signal) => {
      await sendTyping(target, signal);
      if (target.channelId === thread.channelId && ++typingCalls === 3) {
        mergeReservationStarted.resolve();
      }
    };
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      sequence.push("merge");
      mergeStarted.resolve();
      await releaseMerge.promise;
      return mergeWorktree(request, signal);
    };

    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-ordering-running",
      text: "running before host merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await runningStarted.promise;
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-ordering-queued",
      text: "/queue queued before host merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    expect(store.getLatestTask(harness.bot.uid, thread)).toMatchObject({
      messageId: "merge-ordering-queued",
      state: "queued",
    });

    const command = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-ordering-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeReservationStarted.promise;
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-ordering-late",
      text: "late default-steer message",
      mentionedUserIds: [harness.bot.uid],
    }));
    expect(store.getLatestTask(harness.bot.uid, thread)).toMatchObject({
      messageId: "merge-ordering-late",
      state: "queued",
    });
    expect(runningAppended()).toEqual([]);

    releaseRunning.resolve();
    await queuedStarted.promise;
    expect(harness.worktrees?.merges).toEqual([]);
    expect(sequence).toEqual(["running", "queued"]);
    releaseQueued.resolve();
    await mergeStarted.promise;
    expect(sequence).toEqual(["running", "queued", "merge"]);
    expect(harness.workers.prompts.map(({ text }) => text)).not.toContain("late default-steer message");

    releaseMerge.resolve();
    await command;
    await lateStarted.promise;
    expect(sequence).toEqual(["running", "queued", "merge", "late"]);
  } finally {
    releaseRunning.resolve();
    releaseQueued.resolve();
    releaseMerge.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});
test("a host merge reservation waits for an earlier native goal task", async () => {
  const goalStarted = Promise.withResolvers<void>();
  const releaseGoal = Promise.withResolvers<void>();
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const sequence: string[] = [];
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-goal-order", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-goal-order-session",
      sessionFile: null,
      cwdPinned: true,
    });
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-goal-order-prepare",
      text: "prepare workspace session",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, thread, status => status.state === "completed");

    harness.workers.goalHandler = async ({ request, emit }: FakeGoalRunContext) => {
      if (request.op !== "create") throw new Error("expected a structured goal create");
      const goal = {
        id: "merge-goal-order-native",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
        ...(request.tokenBudget === undefined ? {} : { tokenBudget: request.tokenBudget }),
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      sequence.push("goal-started");
      goalStarted.resolve();
      await releaseGoal.promise;
      sequence.push("goal-finished");
      return { goal, state: { enabled: true, mode: "active", goal } };
    };
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      const { lifecycle, ...requestWithoutLifecycle } = request;
      if (lifecycle) {
        lifecycle.beforeSpawn(request.cwd);
        lifecycle.onSpawn({
          pid: 0x7fff_ffff,
          pgid: 0x7fff_ffff,
          identity: "pid=2147483647;start=fixture",
          cwd: request.cwd,
        });
      }
      sequence.push("merge-started");
      mergeStarted.resolve();
      await releaseMerge.promise;
      if (lifecycle) lifecycle.onConfirmedExit();
      return mergeWorktree(requestWithoutLifecycle, signal);
    };

    const goalCommand = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-goal-order-goal",
      text: "/goal set hold the workspace",
      mentionedUserIds: [harness.bot.uid],
    }));
    await goalStarted.promise;
    const mergeCommand = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-goal-order-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    for (let attempt = 0; attempt < 10; attempt += 1) {
      const tick = Promise.withResolvers<void>();
      setImmediate(tick.resolve);
      await tick.promise;
    }
    expect(store.getThreadListen(harness.bot.uid, thread)?.mergePending).toBe(false);
    expect(sequence).toEqual(["goal-started"]);

    releaseGoal.resolve();
    await mergeStarted.promise;
    expect(store.getThreadListen(harness.bot.uid, thread)?.mergePending).toBe(true);
    expect(sequence).toEqual(["goal-started", "goal-finished", "merge-started"]);
    releaseMerge.resolve();
    await Promise.all([goalCommand, mergeCommand]);
  } finally {
    releaseGoal.resolve();
    releaseMerge.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});



test("a new authorized message during host merge prevents a later archive schedule", async () => {
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-generation", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  await mkdir(workspace);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-generation-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected fixture worktrees");
    const merge = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request) => {
      mergeStarted.resolve();
      await releaseMerge.promise;
      return merge(request);
    };
    const command = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-generation-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeStarted.promise;
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-generation-new-message",
      text: "work after the merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    releaseMerge.resolve();
    await command;
    await harness.workers.waitForPromptCount(1);

    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    expect(harness.worktrees.merges).toHaveLength(1);
    expect(harness.worktrees.merges[0]).toMatchObject({ cwd: workspace });
  } finally {
    releaseMerge.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});
test("a temporary success-receipt delivery failure prevents archive until delivery succeeds", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-receipt", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  let receiptWasSentBeforeArchive = false;
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-receipt-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const archive = harness.transport.archiveThread.bind(harness.transport);
    harness.transport.archiveThread = async (groupId, shortId, signal) => {
      receiptWasSentBeforeArchive = harness.transport.sent.some(({ text }) =>
        text.startsWith("子区合并成功："),
      );
      await archive(groupId, shortId, signal);
    };
    harness.transport.setOffline();
    const message = inbound({
      ...thread,
      messageId: "merge-receipt-offline",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(message);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    await harness.service.handleInbound(message);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);

    await harness.transport.reconnect();
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toHaveLength(1);
    expect(receiptWasSentBeforeArchive).toBe(true);
    expect(harness.transport.sent.some(({ text }) => text.startsWith("子区合并成功："))).toBe(true);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("a success-receipt persistence failure recovers delivery without rerunning the merge", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-receipt-recovery", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  const fault = new Database(join(harness.stateDir, "channel.sqlite"));
  let stopped = false;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-receipt-recovery-session",
      sessionFile: null,
      cwdPinned: true,
    });
    fault.exec(`
      CREATE TRIGGER fail_merge_receipt BEFORE INSERT ON outbox
      WHEN NEW.text LIKE '子区合并成功：%'
      BEGIN SELECT RAISE(ABORT, 'receipt persistence fault'); END;
    `);
    const message = inbound({
      ...thread,
      messageId: "merge-receipt-recovery-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(message);
    await waitForHostMergeOutcome(
      store,
      harness.bot.uid,
      thread,
      message.messageId,
      "succeeded",
    );
    await waitForSentText(harness.transport, "合并已受理，但后续处理未能完成：");
    fault.exec("DROP TRIGGER fail_merge_receipt");
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect(store.listHostMergeLeases(harness.bot.uid)).toMatchObject([{
      sourceId: message.messageId,
      phase: "exited",
      outcome: { kind: "succeeded" },
    }]);

    await closeCoreHarness(harness, { remove: false });
    stopped = true;
    restarted = await createCoreHarness({ root: harness.root, bot: harness.bot });
    await restarted.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    expect(restarted.worktrees?.merges).toEqual([]);
    expect(restarted.transport.sent.filter(({ text }) => text.startsWith("子区合并成功："))).toHaveLength(1);
    const recoveredStore = await openChannelStore(restarted.stateDir);
    try {
      expect(recoveredStore.listHostMergeLeases(restarted.bot.uid)).toEqual([]);
    } finally {
      recoveredStore.close();
    }
    await restarted.service.handleInbound(message);
    expect(restarted.worktrees?.merges).toEqual([]);
  } finally {
    fault.exec("DROP TRIGGER IF EXISTS fail_merge_receipt");
    fault.close();
    store.close();
    if (restarted) {
      await closeCoreHarness(restarted);
    } else if (!stopped) {
      await closeCoreHarness(harness);
    } else {
      await rm(harness.root, { recursive: true, force: true });
    }
  }
});

test("a repair-task persistence failure recovers the same repair without rerunning the merge", async () => {
  const initialWorktrees = new FakeWorktrees("unused");
  initialWorktrees.mergeFailure = new WorktreeMergeExecutionError(1, "durable repair recovery fault");
  const harness = await createCoreHarness({ worktreePort: initialWorktrees });
  const thread = { channelId: "group-allowed____merge-repair-recovery", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  const fault = new Database(join(harness.stateDir, "channel.sqlite"));
  let stopped = false;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  const terminalCardEdited = Promise.withResolvers<void>();
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-repair-recovery-session",
      sessionFile: null,
      cwdPinned: true,
    });
    harness.transport.reviewProgressCardSupported = true;
    const editProgressCard = harness.transport.editProgressCard.bind(harness.transport);
    harness.transport.editProgressCard = async (target, messageId, progress, cardSeq, signal) => {
      await editProgressCard(target, messageId, progress, cardSeq, signal);
      if (progress.hostMerge?.phase === "execution-failed") terminalCardEdited.resolve();
    };
    fault.exec(`
      CREATE TRIGGER fail_merge_repair BEFORE INSERT ON tasks
      WHEN NEW.message_id = 'thread-merge-repair:merge-repair-recovery-command'
      BEGIN SELECT RAISE(ABORT, 'repair persistence fault'); END;
    `);
    const message = inbound({
      ...thread,
      messageId: "merge-repair-recovery-command",
      text: "/thread merge, preserve repair recovery note",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(message);
    await waitForHostMergeOutcome(
      store,
      harness.bot.uid,
      thread,
      message.messageId,
      "execution-failed",
    );
    await waitForSentText(harness.transport, "合并已受理，但后续处理未能完成：");
    await terminalCardEdited.promise;
    for (let step = 0; step < 200 && store.getHostMergeProgressCard(
      harness.bot.uid,
      thread,
      message.messageId,
    ) !== null; step += 1) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(store.getHostMergeProgressCard(
      harness.bot.uid,
      thread,
      message.messageId,
    )).toBeNull();
    fault.exec("DROP TRIGGER fail_merge_repair");
    expect(initialWorktrees.merges).toHaveLength(1);
    expect(store.listHostMergeLeases(harness.bot.uid)).toMatchObject([{
      sourceId: message.messageId,
      phase: "exited",
      outcome: { kind: "execution-failed", diagnostic: "durable repair recovery fault" },
    }]);

    await closeCoreHarness(harness, { remove: false });
    stopped = true;
    restarted = await createCoreHarness({ root: harness.root, bot: harness.bot });
    await restarted.workers.waitForPromptCount(1);
    expect(restarted.worktrees?.merges).toEqual([]);
    const recoveredStore = await openChannelStore(restarted.stateDir);
    try {
      expect(recoveredStore.getLatestTask(restarted.bot.uid, thread)).toMatchObject({
        messageId: "thread-merge-repair:merge-repair-recovery-command",
        text: expect.stringContaining("preserve repair recovery note"),
      });
      expect(recoveredStore.listHostMergeLeases(restarted.bot.uid)).toEqual([]);
    } finally {
      recoveredStore.close();
    }
  } finally {
    fault.exec("DROP TRIGGER IF EXISTS fail_merge_repair");
    fault.close();
    store.close();
    if (restarted) {
      await closeCoreHarness(restarted);
    } else if (!stopped) {
      await closeCoreHarness(harness);
    } else {
      await rm(harness.root, { recursive: true, force: true });
    }
  }
});


test("a permanently dead success receipt prevents archive", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-dead-receipt", channelType: 5 as const };
  const store = await openChannelStore(harness.stateDir);
  const archiveCalls: string[] = [];
  const archives = new ThreadArchiveScheduler({
    store,
    transport: {
      archiveThread: async (groupId, shortId) => {
        archiveCalls.push(`${groupId}/${shortId}`);
      },
    },
    outbox: { flush: async () => {}, enqueueText: () => {} },
    botUid: () => harness.bot.uid,
    stopping: () => false,
  });
  const receiptOutboxId = "permanently-dead-merge-receipt";
  try {
    store.enqueueOutbox({
      id: receiptOutboxId,
      botUid: harness.bot.uid,
      channelId: thread.channelId,
      channelType: thread.channelType,
      taskId: null,
      kind: "text",
      text: "子区合并成功：fixture",
      path: null,
      fileKind: null,
      clientMsgNo: "permanently-dead-merge-receipt-client",
    });
    store.markOutboxDead(receiptOutboxId, "fixture permanent delivery failure");
    archives.schedule(thread, "group-allowed", "merge-dead-receipt", 0, [receiptOutboxId]);

    await archives.run(thread, Promise.resolve());

    expect(store.getOutbox(receiptOutboxId)).toMatchObject({ state: "dead" });
    expect(archiveCalls).toEqual([]);
  } finally {
    archives.stop();
    store.close();
    await closeCoreHarness(harness);
  }
});

for (const settlement of ["delivered", "cancelled", "stopped"] as const) {
  test(`a delivered merge receipt waits for pending assistant output until ${settlement}`, async () => {
    const harness = await createCoreHarness();
    const thread = { channelId: "group-allowed____merge-output-drain", channelType: 5 as const };
    const store = await openChannelStore(harness.stateDir);
    const calls: string[] = [];
    const receiptId = "merge-output-drain-receipt";
    const outputId = "merge-output-drain-assistant";
    const archives = new ThreadArchiveScheduler({
      store,
      botUid: () => harness.bot.uid,
      stopping: () => false,
      transport: { archiveThread: async () => { calls.push("archive"); } },
      // An outbox in retry backoff legitimately has no active flush to await.
      outbox: { flush: async () => {}, enqueueText: async () => {} },
    });
    let operation: Promise<void> | undefined;
    vi.useFakeTimers();
    try {
      for (const id of [receiptId, outputId]) {
        store.enqueueOutbox({
          id,
          botUid: harness.bot.uid,
          ...thread,
          taskId: null,
          kind: "text",
          text: id === receiptId ? "子区合并成功" : "assistant final output",
          path: null,
          fileKind: null,
          clientMsgNo: id,
        });
      }
      store.markOutboxSent(receiptId);
      archives.schedule(thread, "group-allowed", "merge-output-drain", 0, [receiptId]);
      operation = archives.run(thread, Promise.resolve());
      for (let step = 0; step < 120; step += 1) {
        vi.advanceTimersByTime(100);
        await Bun.file(join(harness.stateDir, "channel.sqlite")).stat();
      }
      expect(store.countPendingOutbox(harness.bot.uid, thread)).toBe(1);
      expect(calls).toEqual([]);

      if (settlement === "delivered") store.markOutboxSent(outputId);
      else if (settlement === "cancelled") archives.cancel(thread);
      else archives.stop();
      vi.advanceTimersByTime(100);
      await operation;
      expect(calls).toEqual(settlement === "delivered" ? ["archive"] : []);
    } finally {
      archives.stop();
      await operation;
      vi.useRealTimers();
      store.close();
      await closeCoreHarness(harness);
    }
  });
}
test("a refused merge repair remains pending when its exact stable task was not accepted", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-repair-refused", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  const message = inbound({
    ...thread,
    messageId: "merge-repair-refused-command",
    text: "/thread merge preserve this note",
    mentionedUserIds: [harness.bot.uid],
  });
  const notices: string[] = [];
  let repairMessageId = "";
  let settleCalls = 0;
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-repair-refused-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const commands = new ThreadCommands({
      store,
      handleThreadModel: async () => {},
      ensureThreadListen: async () => {},
      archiveForkThread: async () => {
        throw new Error("unexpected fork archive path in worktree merge repair test");
      },
      replayThreadMergeSuccess: async () => null,
      refuseIncompleteThreadWorkspace: async () => false,
      botUid: () => harness.bot.uid,
      worktrees: harness.worktrees!,
      archiveGeneration: () => 0,
      stopping: () => false,
      executeHostMerge: async () => {
        throw new WorktreeMergeExecutionError(1, "actual merge failure");
      },
      executeHostRemoval: async () => ({ kind: "refused", message: "unused" }),
      enqueueRemovalReceipt: () => {},
      settleHostRemoval: () => {},
      recoverRemovalArchive: () => {},
      enqueueMergeReceipt: () => {},
      settleHostMerge: () => {
        settleCalls += 1;
      },
      recoverMergeArchive: () => {},
      runThreadArchive: () => {},
      materialize: async (_message, text) => text ?? null,
      commandClaim: (target, _name) => ({
        botUid: harness.bot.uid,
        target,
        messageId: target.messageId,
        senderId: target.senderId,
        text: target.text,
        supported: target.supported,
        kind: "command",
      }),
      withMessageTyping: async (_target, work) => work(),
      appendToRunningTask: async () => false,
      acceptTask: async (repairMessage) => {
        repairMessageId = repairMessage.messageId;
        return false;
      },
      enqueueText: async (_target, text) => {
        notices.push(text);
      },
    });

    await expect(commands.handle(message, "merge preserve this note"))
      .rejects.toThrow("merge repair task was not durably accepted");

    expect(repairMessageId).toBe("thread-merge-repair:merge-repair-refused-command");
    expect(store.getTaskByMessageId(
      harness.bot.uid,
      thread,
      repairMessageId,
    )).toBeNull();
    expect(notices.some((text) => text.includes("修复任务未能持久接收"))).toBe(true);
    expect(settleCalls).toBe(0);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});


test("only an actual Worktrunk execution failure creates one stable repair task", async () => {
  let fixtureWorktrees: FakeWorktrees | undefined;
  const harness = await createCoreHarness({
    prompt: async ({ text, promptContext }) => {
      if (!text.startsWith("用户已确认合并当前子区工作区")) return;
      const worktrees = fixtureWorktrees;
      if (!worktrees) throw new Error("expected fixture worktrees");
      worktrees.mergeFailure = undefined;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      await management.execute(
        { action: "thread-merge" },
        new AbortController().signal,
        "host-first-repair-merge",
      );
      management.onResultAccepted?.("host-first-repair-merge");
    },
  });
  fixtureWorktrees = harness.worktrees;
  const thread = { channelId: "group-allowed____merge-repair", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-repair-session",
      sessionFile: null,
      cwdPinned: true,
    });
    if (!fixtureWorktrees) throw new Error("expected fixture worktrees");
    fixtureWorktrees.mergeFailure = new WorktreeMergeExecutionError(1, "fixture pre-merge failure");
    const command = inbound({
      ...thread,
      messageId: "merge-repair-command",
      text: "/thread merge, preserve the original note token=merge-secret-value",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(command);
    await harness.workers.waitForPromptCount(1);
    await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
    const repair = store.getLatestTask(harness.bot.uid, thread);
    expect(repair?.messageId).toBe("thread-merge-repair:merge-repair-command");
    expect(repair?.text).toContain("fixture pre-merge failure");
    expect(repair?.text).toContain("/thread merge, preserve the original note token=[redacted]");
    expect(repair?.text).not.toContain("merge-secret-value");
    expect(repair?.text).toContain("preserve the original note");
    expect(repair?.text).toContain(workspace);
    expect(fixtureWorktrees.merges).toHaveLength(2);

    await harness.service.handleInbound(command);
    expect(fixtureWorktrees.merges).toHaveLength(2);
    expect(store.getLatestTask(harness.bot.uid, thread)?.messageId).toBe(repair?.messageId);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

test("an uncertain merge failure is reported without dispatching a repair task", async () => {
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-uncertain", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-uncertain-session",
      sessionFile: null,
      cwdPinned: true,
    });
    if (!harness.worktrees) throw new Error("expected fixture worktrees");
    harness.worktrees.mergeFailure = new Error("execution result unknown");
    await harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-uncertain-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));

    expect(harness.worktrees.merges).toHaveLength(1);
    expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(({ text }) => text.includes("execution result unknown"))).toBe(true);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

for (const action of ["cancel", "interrupt"] as const) {
  test(`/${action} aborts an in-progress host merge`, async () => {
    const mergeStarted = Promise.withResolvers<void>();
    const harness = await createCoreHarness();
    const thread = { channelId: `group-allowed____merge-${action}`, channelType: 5 as const };
    const workspace = join(harness.root, "workspace");
    const store = await openChannelStore(harness.stateDir);
    let mergeSignal: AbortSignal | undefined;
    try {
      await mkdir(workspace);
      store.ensureBinding({
        botUid: harness.bot.uid,
        ...thread,
        cwd: workspace,
        sessionId: `merge-${action}-session`,
        sessionFile: null,
        cwdPinned: true,
      });
      const worktrees = harness.worktreePort;
      if (!worktrees) throw new Error("expected worktree port");
      worktrees.merge = async (_request, signal) => {
        mergeSignal = signal;
        mergeStarted.resolve();
        if (!signal) throw new Error("merge signal was not forwarded");
        const aborted = Promise.withResolvers<never>();
        if (signal.aborted) aborted.reject(signal.reason);
        else signal.addEventListener("abort", () => aborted.reject(signal.reason), { once: true });
        return await aborted.promise;
      };
      const command = harness.service.handleInbound(inbound({
        ...thread,
        messageId: `merge-${action}-command`,
        text: "/thread merge",
        mentionedUserIds: [harness.bot.uid],
      }));
      await mergeStarted.promise;
      await harness.service.handleInbound(inbound({
        ...thread,
        messageId: `merge-${action}-abort`,
        text: `/${action}`,
        mentionedUserIds: [harness.bot.uid],
      }));
      await command;

      expect(mergeSignal?.aborted).toBe(true);
      expect(harness.worktrees?.merges).toEqual([]);
      expect(harness.transport.managementCalls.filter(({ action: name }) => name === "thread-archive")).toEqual([]);
      expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
    } finally {
      store.close();
      await closeCoreHarness(harness);
    }
  });
}

test("service stop aborts an in-progress host merge without archive or repair task", async () => {
  const mergeStarted = Promise.withResolvers<void>();
  const harness = await createCoreHarness();
  const thread = { channelId: "group-allowed____merge-stop", channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  const store = await openChannelStore(harness.stateDir);
  let mergeSignal: AbortSignal | undefined;
  try {
    await mkdir(workspace);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-stop-session",
      sessionFile: null,
      cwdPinned: true,
    });
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("expected worktree port");
    worktrees.merge = async (_request, signal) => {
      mergeSignal = signal;
      mergeStarted.resolve();
      if (!signal) throw new Error("merge signal was not forwarded");
      const aborted = Promise.withResolvers<never>();
      if (signal.aborted) aborted.reject(signal.reason);
      else signal.addEventListener("abort", () => aborted.reject(signal.reason), { once: true });
      return await aborted.promise;
    };
    const command = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-stop-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeStarted.promise;

    const stopping = harness.service.stop();
    await command;
    await stopping;

    expect(mergeSignal?.aborted).toBe(true);
    expect(harness.worktrees?.merges).toEqual([]);
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
    expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

test("a merge does not block steering from another chat into a running workspace task", async () => {
  const writerStarted = Promise.withResolvers<void>();
  const mergeStarted = Promise.withResolvers<void>();
  const releaseWriter = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const sequence: string[] = [];
  let appendedMessages: () => string[] = () => [];
  const harness = await createCoreHarness({
    prompt: async ({ text, appended }) => {
      if (text !== "writer before root merge") return;
      sequence.push("writer");
      appendedMessages = () => appended().map(message => message.text);
      writerStarted.resolve();
      await releaseWriter.promise;
    },
  });
  const thread = { channelId: "group-allowed____merge-root-steering", channelType: 5 as const };
  const writer = { channelId: "dm-root-steering", channelType: 1 as const };
  const workspace = join(harness.root, "root-steering");
  const child = join(workspace, "src");
  const store = await openChannelStore(harness.stateDir);
  let merge: Promise<void> | undefined;
  try {
    await mkdir(child, { recursive: true });
    await Bun.$`git init -q ${workspace}`.quiet();
    const writerSessionFile = join(harness.root, "root-steering-session.jsonl");
    await writeFile(writerSessionFile, `${JSON.stringify({
      type: "session",
      id: "writer-root-steering",
      cwd: child,
      previousSessionFiles: [],
    })}\n`);
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...thread,
      cwd: workspace,
      sessionId: "merge-root-steering",
      sessionFile: null,
      cwdPinned: true,
    });
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...writer,
      cwd: child,
      sessionId: "writer-root-steering",
      sessionFile: writerSessionFile,
    });
    const worktrees = harness.worktreePort!;
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      sequence.push("merge");
      mergeStarted.resolve();
      await releaseMerge.promise;
      return mergeWorktree(request, signal);
    };
    await harness.service.handleInbound(inbound({
      ...writer,
      messageId: "writer-before-root-merge",
      text: "writer before root merge",
    }));
    await writerStarted.promise;
    merge = harness.service.handleInbound(inbound({
      ...thread,
      messageId: "merge-root-steering-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeStarted.promise;
    await harness.service.handleInbound(inbound({
      ...writer,
      messageId: "late-writer-after-root-merge",
      text: "late writer after root merge",
    }));
    expect(appendedMessages()).toEqual(["late writer after root merge"]);
    expect(store.getLatestTask(harness.bot.uid, writer)).toMatchObject({ state: "running" });
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["writer before root merge"]);

    releaseWriter.resolve();
    releaseMerge.resolve();
    await merge;
    await waitForStatus(harness.service, writer, status => status.state === "completed");
    expect(sequence).toEqual(["writer", "merge"]);
    expect(harness.worktrees!.merges).toHaveLength(1);
    expect(harness.worktrees!.merges[0]).toMatchObject({ cwd: workspace });
  } finally {
    releaseWriter.resolve();
    releaseMerge.resolve();
    if (merge !== undefined) await Promise.allSettled([merge]);
    store.close();
    await closeCoreHarness(harness);
  }
});
