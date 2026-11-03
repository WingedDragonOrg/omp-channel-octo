import { expect, test, vi } from "bun:test";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, ThreadSummary } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { ChannelStore } from "../src/store/index.ts";
import type { ThreadForkRecord, TaskRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  type CoreHarness,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

type ForkTarget = ChatTarget & { channelType: 5 };
type ForkSourceTarget = { channelType: 2 | 5; channelId: string };

const FORK_GROUP = "group-tools";

interface SeededFork {
  readonly target: ForkTarget;
  readonly sourceTarget: ForkSourceTarget;
  readonly thread: ThreadSummary;
  readonly label: string;
  readonly cwd: string;
  readonly sourceSessionId: string;
  readonly forkSessionId: string;
  readonly sourceSessionFile: string;
  readonly forkSessionFile: string;
}

function ownerMessage(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
) {
  return inbound({
    ...target,
    messageId,
    senderId: "owner-1",
    text,
    mentionedUserIds: [harness.bot.uid],
  });
}

function threadCommand(
  harness: CoreHarness,
  target: ForkTarget,
  messageId: string,
  command: string,
) {
  return ownerMessage(harness, target, messageId, `/thread ${command}`);
}

async function withStore<T>(harness: CoreHarness, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await openChannelStore(harness.stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

async function seedFork(
  harness: CoreHarness,
  label: string,
  phase: ThreadForkRecord["phase"] = "ready",
  sourceTarget: ForkSourceTarget = { channelType: 2, channelId: FORK_GROUP },
): Promise<SeededFork> {
  const sourceSessionId = `source-session-${label}`;
  const forkSessionId = `fork-session-${label}`;
  const cwd = join(harness.root, `fork-shared-cwd-${label}`);
  const sourceSessionFile = join(harness.root, `source-${label}.jsonl`);
  const forkSessionFile = join(harness.root, `fork-${label}.jsonl`);
  await mkdir(cwd, { recursive: true });
  await writeFile(join(cwd, "shared.txt"), `shared contents ${label}\n`);
  await writeFile(sourceSessionFile, `${JSON.stringify({ type: "session", version: 3, id: sourceSessionId, cwd })}\n`);
  await writeFile(forkSessionFile, `${JSON.stringify({ type: "session", version: 3, id: forkSessionId, cwd })}\n`);

  const thread = await harness.transport.createThread(FORK_GROUP, `fork ${label}`);
  const target: ForkTarget = { channelType: 5, channelId: thread.channelId };
  const source = sourceTarget;
  const forkId = `fork-operation-${label}`;
  const record: ThreadForkRecord = {
    id: forkId,
    botUid: harness.bot.uid,
    sourceChannelType: source.channelType,
    sourceChannelId: source.channelId,
    sourceSessionId,
    sourceSessionFile,
    cwd,
    groupId: FORK_GROUP,
    name: thread.name,
    preparationDir: join(harness.stateDir, "fork-preparations", forkId),
    phase,
    preparationStarted: true,
    sessionId: forkSessionId,
    sessionFile: forkSessionFile,
    thread,
    error: null,
  };

  await withStore(harness, store => {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...source,
      cwd,
      sessionId: sourceSessionId,
      sessionFile: sourceSessionFile,
      cwdPinned: true,
    });
    store.saveThreadFork(record);
    store.saveThreadWorkspaceIntent({
      id: forkId,
      botUid: harness.bot.uid,
      groupId: FORK_GROUP,
      channelIds: [target.channelId],
      status: "preparing",
    });
    if (phase === "ready") store.completeThreadFork(harness.bot.uid, forkId);
  });

  return { target, sourceTarget, thread, label, cwd, sourceSessionId, forkSessionId, sourceSessionFile, forkSessionFile };
}

async function waitForArchive(harness: CoreHarness, thread: ThreadSummary): Promise<void> {
  await harness.transport.waitForManagementCalls(call =>
    call.action === "thread-archive" && call.groupId === thread.groupId && call.shortId === thread.shortId,
  );
}

async function settleOneTurn(): Promise<void> {
  const next = Promise.withResolvers<void>();
  setImmediate(next.resolve);
  await next.promise;
}

function settlesBeforeNextTurn(promise: Promise<unknown>): Promise<boolean> {
  const next = Promise.withResolvers<boolean>();
  setImmediate(() => next.resolve(false));
  return Promise.race([
    promise.then(() => true, () => true),
    next.promise,
  ]);
}

function archiveCalls(harness: CoreHarness) {
  return harness.transport.managementCalls.filter(call => call.action === "thread-archive");
}

function isArchiveOnlyReceiptText(text: string | undefined): boolean {
  return text !== undefined
    && text.includes("归档")
    && /(?:仅|只).*归档/u.test(text)
    && !/合并成功|工作区已删除|分支已删除/u.test(text);
}

function hasArchiveOnlyReceipt(harness: CoreHarness, target: ChatTarget): boolean {
  return harness.transport.sent.some(({ target: sentTarget, text }) =>
    sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && isArchiveOnlyReceiptText(text),
  );
}

async function expectArchiveOnlyResult(harness: CoreHarness, fork: SeededFork): Promise<void> {
  expect(hasArchiveOnlyReceipt(harness, fork.target)).toBe(true);
  await waitForArchive(harness, fork.thread);
  const remote = await harness.transport.getThread(FORK_GROUP, fork.thread.shortId);
  expect(harness.transport.managementCalls.filter(call => call.action === "thread-delete")).toEqual([]);
  expect(remote.status).toBe(2);

  const targetMessages = harness.transport.sent.filter(message =>
    message.target.channelType === fork.target.channelType
      && message.target.channelId === fork.target.channelId,
  );
  expect(targetMessages.some(({ text }) => text?.includes("归档"))).toBe(true);
  expect(targetMessages.some(({ text }) => /合并成功|工作区已删除|分支已删除/u.test(text ?? ""))).toBe(false);
  expect(harness.transport.sent.filter(message =>
    message.target.channelType === fork.sourceTarget.channelType
      && message.target.channelId === fork.sourceTarget.channelId,
  )).toEqual([]);

  await withStore(harness, store => {
    expect(store.getThreadWorkspace(harness.bot.uid, fork.target)).toBeNull();
    expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    expect(store.getBinding(harness.bot.uid, fork.sourceTarget)).toMatchObject({
      cwd: fork.cwd,
      sessionId: fork.sourceSessionId,
      sessionFile: fork.sourceSessionFile,
    });
    expect(store.getBinding(harness.bot.uid, fork.target)).toMatchObject({
      cwd: fork.cwd,
      sessionId: fork.forkSessionId,
      sessionFile: fork.forkSessionFile,
    });
  });
  expect(await readFile(join(fork.cwd, "shared.txt"), "utf8")).toBe(`shared contents ${fork.label}\n`);
  expect(await readFile(fork.sourceSessionFile, "utf8")).toContain(fork.sourceSessionId);
  expect(await readFile(fork.forkSessionFile, "utf8")).toContain(fork.forkSessionId);
  expect(harness.workers.prompts).toEqual([]);
}

test.each(["merge", "remove"] as const)(
  "/thread %s on a ready fork archives only Octo and preserves shared local resources",
  async action => {
    const harness = await createCoreHarness({ worktrees: null });
    try {
      const fork = await seedFork(harness, `only-${action}`);
      expect(harness.worktreePort).toBeUndefined();

      await harness.service.handleInbound(threadCommand(harness, fork.target, `archive-only-${action}`, action));
      await expectArchiveOnlyResult(harness, fork);
      expect(archiveCalls(harness)).toHaveLength(1);
    } finally {
      await closeCoreHarness(harness);
    }
  },
);

test("archiving a fork never removes its source thread's workspace ownership", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const sourceThread = await harness.transport.createThread(FORK_GROUP, "source workspace owner");
    const sourceTarget = { channelType: 5 as const, channelId: sourceThread.channelId };
    const fork = await seedFork(harness, "source-workspace-owner", "ready", sourceTarget);
    const repository = join(harness.root, "source-owned-repository");
    await mkdir(repository);
    await withStore(harness, store => {
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        ...sourceTarget,
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{
          name: "source",
          repo: repository,
          path: fork.cwd,
          branch: "source-thread-branch",
          created: true,
        }],
        error: null,
      });
    });

    await harness.service.handleInbound(threadCommand(harness, fork.target, "source-workspace-fork-remove", "remove"));
    await expectArchiveOnlyResult(harness, fork);
    expect((await harness.transport.getThread(FORK_GROUP, sourceThread.shortId)).status).toBe(1);
    await withStore(harness, store => {
      expect(store.getThreadWorkspace(harness.bot.uid, sourceTarget)).toMatchObject({
        status: "ready",
        projects: [{ path: fork.cwd, branch: "source-thread-branch", created: true }],
      });
      expect(store.getThreadWorkspace(harness.bot.uid, fork.target)).toBeNull();
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test.each(["merge", "remove"] as const)(
  "fork /thread %s archives without waiting for a held source task in the same cwd",
  async action => {
    const sourceStarted = Promise.withResolvers<void>();
    const releaseSource = Promise.withResolvers<void>();
    const harness = await createCoreHarness({
      worktrees: null,
      prompt: async ({ text }) => {
        if (text !== "held source turn in shared cwd") return;
        sourceStarted.resolve();
        await releaseSource.promise;
      },
    });
    let command: Promise<void> | undefined;
    try {
      const fork = await seedFork(harness, "source-busy");
      const source = { channelType: 2 as const, channelId: FORK_GROUP };
      await harness.service.handleInbound(ownerMessage(harness, source, "source-busy-task", "held source turn in shared cwd"));
      await sourceStarted.promise;

      command = harness.service.handleInbound(threadCommand(harness, fork.target, `source-busy-${action}`, action));
      expect(await settlesBeforeNextTurn(command)).toBe(true);
      await command;
      expect(hasArchiveOnlyReceipt(harness, fork.target)).toBe(true);
      await waitForArchive(harness, fork.thread);
      expect(harness.workers.prompts.map(prompt => prompt.text)).toEqual(["held source turn in shared cwd"]);
      expect((await harness.service.status(source)).state).not.toBe("completed");
      expect(archiveCalls(harness)).toHaveLength(1);
    } finally {
      releaseSource.resolve();
      if (command) await command.catch(() => {});
      await closeCoreHarness(harness);
    }
  },
);

test("fork /thread merge waits for earlier active and queued work on its own chat", async () => {
  const firstStarted = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const releaseFirst = Promise.withResolvers<void>();
  const releaseSecond = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    worktrees: null,
    prompt: async ({ text }) => {
      if (text === "first fork target task") {
        firstStarted.resolve();
        await releaseFirst.promise;
      } else if (text === "queued fork target task") {
        secondStarted.resolve();
        await releaseSecond.promise;
      }
    },
  });
  let merge: Promise<void> | undefined;
  try {
    const fork = await seedFork(harness, "merge-own-queue");
    await withStore(harness, store => store.setChatMode(harness.bot.uid, fork.target, "queue"));

    await harness.service.handleInbound(ownerMessage(harness, fork.target, "fork-first-target-task", "first fork target task"));
    await firstStarted.promise;
    await harness.service.handleInbound(ownerMessage(harness, fork.target, "fork-queued-target-task", "queued fork target task"));
    await waitForStatus(harness.service, fork.target, status => status.queued === 1);

    merge = harness.service.handleInbound(threadCommand(harness, fork.target, "fork-merge-own-queue", "merge"));
    for (let turn = 0; turn < 4; turn += 1) await settleOneTurn();
    expect(archiveCalls(harness)).toEqual([]);

    releaseFirst.resolve();
    await secondStarted.promise;
    for (let turn = 0; turn < 4; turn += 1) await settleOneTurn();
    expect(archiveCalls(harness)).toEqual([]);

    releaseSecond.resolve();
    await waitForStatus(harness.service, fork.target, status => status.state === "completed" && status.queued === 0);
    await merge;
    expect(hasArchiveOnlyReceipt(harness, fork.target)).toBe(true);
    await waitForArchive(harness, fork.thread);
    expect(harness.workers.prompts.map(prompt => prompt.text)).toEqual([
      "first fork target task",
      "queued fork target task",
    ]);
    expect(archiveCalls(harness)).toHaveLength(1);
  } finally {
    releaseFirst.resolve();
    releaseSecond.resolve();
    if (merge) await merge.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("fork /thread merge archives only after the target's terminal progress-card edit drains", async () => {
  const taskStarted = Promise.withResolvers<void>();
  const finishTask = Promise.withResolvers<void>();
  const terminalEditStarted = Promise.withResolvers<void>();
  const finishTerminalEdit = Promise.withResolvers<void>();
  let terminalEditFinished = false;
  const harness = await createCoreHarness({
    worktrees: null,
    prompt: async ({ text, emit }) => {
      if (text !== "fork target progress-card task") return;
      emit({
        type: "tool_start",
        toolCallId: "fork-target-progress-tool",
        toolName: "bash",
        atMs: Date.now(),
      });
      taskStarted.resolve();
      await finishTask.promise;
    },
  });
  let merge: Promise<void> | undefined;
  try {
    const fork = await seedFork(harness, "merge-card-drain");
    harness.transport.sendProgressCard = async () => ({ messageId: "fork-target-progress-card" });
    const editProgressCard = harness.transport.editProgressCard.bind(harness.transport);
    harness.transport.editProgressCard = async (target, messageId, progress, cardSeq, signal) => {
      if (progress.state !== "running") {
        terminalEditStarted.resolve();
        await finishTerminalEdit.promise;
      }
      await editProgressCard(target, messageId, progress, cardSeq, signal);
      if (progress.state !== "running") terminalEditFinished = true;
    };
    const archiveThread = harness.transport.archiveThread.bind(harness.transport);
    let archiveBeforeTerminalEdit = false;
    harness.transport.archiveThread = async (groupId, shortId, signal) => {
      if (!terminalEditFinished) archiveBeforeTerminalEdit = true;
      await archiveThread(groupId, shortId, signal);
    };

    await harness.service.handleInbound(ownerMessage(harness, fork.target, "fork-card-task", "fork target progress-card task"));
    await taskStarted.promise;
    merge = harness.service.handleInbound(threadCommand(harness, fork.target, "fork-card-merge", "merge"));
    finishTask.resolve();
    await terminalEditStarted.promise;
    for (let turn = 0; turn < 4; turn += 1) await settleOneTurn();
    expect(archiveCalls(harness)).toEqual([]);

    finishTerminalEdit.resolve();
    await merge;
    expect(hasArchiveOnlyReceipt(harness, fork.target)).toBe(true);
    expect(archiveBeforeTerminalEdit).toBe(false);
    await waitForArchive(harness, fork.thread);
    expect(terminalEditFinished).toBe(true);
    expect(archiveCalls(harness)).toHaveLength(1);
  } finally {
    finishTask.resolve();
    finishTerminalEdit.resolve();
    if (merge) await merge.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("fork /thread remove refuses while its own thread has a running task", async () => {
  const targetStarted = Promise.withResolvers<void>();
  const releaseTarget = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text !== "held fork-thread task") return;
      targetStarted.resolve();
      await releaseTarget.promise;
    },
  });
  try {
    const fork = await seedFork(harness, "target-running");
    await harness.service.handleInbound(ownerMessage(harness, fork.target, "target-running-task", "held fork-thread task"));
    await targetStarted.promise;
    const command = harness.service.handleInbound(threadCommand(harness, fork.target, "target-running-remove", "remove"));

    expect(await settlesBeforeNextTurn(command)).toBe(true);
    await command;
    for (let attempt = 0; attempt < 4; attempt += 1) await settleOneTurn();
    expect(archiveCalls(harness)).toEqual([]);
    expect((await harness.service.status(fork.target)).state).not.toBe("completed");
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === fork.target.channelId && /任务|运行|排队/u.test(text ?? ""),
    )).toBe(true);
  } finally {
    releaseTarget.resolve();
    await closeCoreHarness(harness);
  }
});

test("fork /thread remove refuses when its own thread has queued work", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "target-queued");
    const messageId = "target-queued-task";
    const text = "persisted work must remain queued";
    await withStore(harness, store => {
      const task: TaskRecord = {
        id: "fork-target-queued-task-id",
        botUid: harness.bot.uid,
        channelType: fork.target.channelType,
        channelId: fork.target.channelId,
        messageId,
        senderId: "owner-1",
        text,
        sessionId: "fork-session-target-queued",
        cwd: fork.cwd,
        state: "queued",
        resultText: null,
        errorText: null,
        messageSeq: 0,
        historyEligible: true,
        relaySuppressed: false,
      };
      expect(store.acceptTask(
        {
          botUid: harness.bot.uid,
          target: fork.target,
          messageId,
          senderId: "owner-1",
          text,
          supported: true,
          kind: "task",
        },
        task,
      )).toBe(true);
    });

    const command = harness.service.handleInbound(threadCommand(harness, fork.target, "target-queued-remove", "remove"));
    expect(await settlesBeforeNextTurn(command)).toBe(true);
    await command;
    for (let attempt = 0; attempt < 4; attempt += 1) await settleOneTurn();
    expect(archiveCalls(harness)).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === fork.target.channelId && /任务|运行|排队/u.test(text ?? ""),
    )).toBe(true);
    await withStore(harness, store => {
      expect(store.getTaskByMessageId(harness.bot.uid, fork.target, messageId)).toMatchObject({ state: "queued" });
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("invalid /thread remove arguments retain usage handling even for a ready fork", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "invalid-remove");
    await harness.service.handleInbound(threadCommand(harness, fork.target, "invalid-remove-command", "remove --unknown"));

    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === fork.target.channelId && text?.startsWith("用法：/thread remove"),
    )).toBe(true);
    expect(archiveCalls(harness)).toEqual([]);
    expect(await readFile(join(fork.cwd, "shared.txt"), "utf8")).toContain("shared contents");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an incomplete fork record does not take the archive-only path", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "incomplete", "creating");
    await harness.service.handleInbound(threadCommand(harness, fork.target, "incomplete-fork-merge", "merge"));

    expect(archiveCalls(harness)).toEqual([]);
    expect(harness.transport.sent.some(({ target }) => target.channelId === fork.target.channelId)).toBe(true);
    expect(await readFile(join(fork.cwd, "shared.txt"), "utf8")).toContain("shared contents");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("ordinary threads without fork provenance keep missing-workspace refusals", async () => {
  const harness = await createCoreHarness();
  try {
    const thread = await harness.transport.createThread(FORK_GROUP, "ordinary thread");
    const target: ForkTarget = { channelType: 5, channelId: thread.channelId };
    await harness.service.handleInbound(threadCommand(harness, target, "ordinary-missing-merge", "merge"));
    await harness.service.handleInbound(threadCommand(harness, target, "ordinary-missing-remove", "remove"));

    expect(archiveCalls(harness)).toEqual([]);
    expect(harness.transport.sent.some(({ target: sentTarget, text }) =>
      sentTarget.channelId === target.channelId && text === "当前子区未绑定隔离工作区，/thread merge 不可用。",
    )).toBe(true);
    expect(harness.transport.sent.some(({ target: sentTarget, text }) =>
      sentTarget.channelId === target.channelId && text?.startsWith("当前子区没有持久隔离工作区归属"),
    )).toBe(true);
    expect(harness.worktrees?.merges).toEqual([]);
    expect(harness.worktrees?.removals).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("fork archive waits for queued target output and its archive-only receipt", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "receipt-drain");
    const priorOutput = "previous target output";
    await withStore(harness, store => {
      store.enqueueOutbox({
        id: "fork-archive-prior-output",
        botUid: harness.bot.uid,
        ...fork.target,
        taskId: null,
        kind: "text",
        text: priorOutput,
        path: null,
        fileKind: null,
        clientMsgNo: "fork-archive-prior-output-client",
      });
    });
    harness.transport.setOffline();
    let bothMessagesSentBeforeArchive = false;
    const archiveThread = harness.transport.archiveThread.bind(harness.transport);
    harness.transport.archiveThread = async (groupId, shortId, signal) => {
      const targetMessages = harness.transport.sent.filter(({ target }) =>
        target.channelType === fork.target.channelType && target.channelId === fork.target.channelId,
      );
      bothMessagesSentBeforeArchive = targetMessages.some(({ text }) => text === priorOutput)
        && targetMessages.some(({ text }) => text?.includes("归档"));
      await archiveThread(groupId, shortId, signal);
    };

    await harness.service.handleInbound(threadCommand(harness, fork.target, "receipt-drain-command", "merge"));
    expect(archiveCalls(harness)).toEqual([]);
    expect(harness.transport.sent.filter(({ target }) =>
      target.channelType === fork.target.channelType && target.channelId === fork.target.channelId,
    )).toEqual([]);

    await harness.transport.reconnect();
    const archiveReceipt = await harness.transport.waitForRemoteText(message =>
      message.target.channelType === fork.target.channelType
        && message.target.channelId === fork.target.channelId
        && isArchiveOnlyReceiptText(message.text),
    );
    expect(archiveReceipt.text).toContain("归档");
    await waitForArchive(harness, fork.thread);
    expect(bothMessagesSentBeforeArchive).toBe(true);
    expect(archiveCalls(harness)).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a new authorized inbound cancels a pending fork archive", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "cancel-pending");
    harness.transport.setOffline();
    await harness.service.handleInbound(threadCommand(harness, fork.target, "cancel-pending-command", "remove"));
    expect(archiveCalls(harness)).toEqual([]);

    await harness.service.handleInbound(threadCommand(harness, fork.target, "cancel-pending-new-input", "remove invalid"));
    await harness.transport.reconnect();
    const firstReply = await harness.transport.waitForRemoteText(message =>
      message.target.channelType === fork.target.channelType
        && message.target.channelId === fork.target.channelId
        && isArchiveOnlyReceiptText(message.text),
    );
    expect(firstReply.text).toContain("归档");
    await harness.transport.waitForRemoteText(message =>
      message.target.channelId === fork.target.channelId && message.text.includes("用法：/thread remove"),
    );
    await settleOneTurn();

    expect(archiveCalls(harness)).toEqual([]);
    expect((await harness.transport.getThread(FORK_GROUP, fork.thread.shortId)).status).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("fork archive retries a transient server failure through the archive scheduler", async () => {
  const harness = await createCoreHarness({ worktrees: null });
  try {
    const fork = await seedFork(harness, "transient-retry");
    const transient = Object.assign(new Error("temporary Octo failure"), { status: 503 });
    harness.transport.failArchiveTimes([transient]);
    vi.useFakeTimers();

    await harness.service.handleInbound(threadCommand(harness, fork.target, "transient-retry-command", "merge"));
    expect(hasArchiveOnlyReceipt(harness, fork.target)).toBe(true);
    await harness.transport.waitForManagementCalls(call =>
      call.action === "thread-archive" && call.shortId === fork.thread.shortId,
    );
    for (let turn = 0; turn < 5; turn += 1) await Promise.resolve();
    vi.advanceTimersByTime(5_000);
    await harness.transport.waitForManagementCalls(call =>
      call.action === "thread-archive" && call.shortId === fork.thread.shortId,
      2,
    );

    expect(archiveCalls(harness)).toHaveLength(2);
    expect((await harness.transport.getThread(FORK_GROUP, fork.thread.shortId)).status).toBe(2);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("ready fork provenance survives restart and still selects archive-only handling", async () => {
  const original = await createCoreHarness({ worktrees: null });
  let originalStopped = false;
  let restarted: CoreHarness | undefined;
  try {
    const fork = await seedFork(original, "restart-ready");
    const transport = new FakeTransport({ bot: original.bot });
    transport.setThreads(FORK_GROUP, [fork.thread]);

    await closeCoreHarness(original, { remove: false });
    originalStopped = true;
    restarted = await createCoreHarness({
      root: original.root,
      bot: original.bot,
      transport,
      worktrees: null,
    });

    await restarted.service.handleInbound(threadCommand(restarted, fork.target, "restart-ready-remove", "remove"));
    expect(hasArchiveOnlyReceipt(restarted, fork.target)).toBe(true);
    await waitForArchive(restarted, fork.thread);
    expect(transport.managementCalls.filter(call => call.action === "thread-archive")).toHaveLength(1);
    expect(transport.managementCalls.filter(call => call.action === "thread-delete")).toEqual([]);
    expect(transport.sent.some(({ target, text }) =>
      target.channelId === fork.target.channelId && text?.includes("归档"),
    )).toBe(true);
    expect(await readFile(join(fork.cwd, "shared.txt"), "utf8")).toContain("shared contents restart-ready");
    await withStore(restarted, store => {
      expect(store.getThreadForkByTarget(restarted!.bot.uid, fork.target)).toMatchObject({ phase: "ready" });
      expect(store.listThreadMergeEvents(restarted!.bot.uid)).toEqual([]);
    });
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    else if (!originalStopped) await closeCoreHarness(original);
    else await rm(original.root, { recursive: true, force: true });
  }
});
