import { expect, test } from "bun:test";
import { mkdir, realpath } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, PromptContext } from "../src/contracts.ts";
import type { ThreadForkRecord } from "../src/store/store-records.ts";
import { createCoreHarness, closeCoreHarness, readText, waitForStatus, submitAndWaitForIngressHandoff, waitForIngressJob, type CoreHarness } from "./fixtures/core-fakes.ts";
import {
  FORK_DM,
  FORK_GROUP,
  ForkTestTransport,
  ForkTestWorkerFactory,
  gate,
  ownerMessage,
  seedBinding,
  threadSummary,
  waitForForkBoundary,
  withStore,
} from "./fixtures/core-fork.ts";

const sourceThread: ChatTarget & { channelType: 5 } = {
  channelId: `${FORK_GROUP.channelId}____source-topic`,
  channelType: 5,
};

const textFor = (harness: CoreHarness, target: ChatTarget): string[] =>
  harness.transport.sent
    .filter(({ target: sent }) => sent.channelId === target.channelId && sent.channelType === target.channelType)
    .map(({ text }) => text);


async function waitForForkRecord(
  harness: CoreHarness,
  predicate: (record: ThreadForkRecord) => boolean,
): Promise<ThreadForkRecord> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const record = await withStore(harness, store =>
      store.listThreadForks(harness.bot.uid).find(predicate));
    if (record !== undefined) return record;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error("fork operation did not reach the requested durable state");
}

async function waitForSentOutbox(harness: CoreHarness, id: string) {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const receipt = await withStore(harness, store => store.getOutbox(id));
    if (receipt?.state === "sent") return receipt;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`outbox receipt ${id} was not sent`);
}

function toolsOf(context: PromptContext | undefined) {
  return context?.tools ?? [];
}

test("/fork snapshots a held source immediately, then the new thread resumes without replaying the command", async () => {
  const sourceStarted = gate();
  const releaseSource = gate();
  const workers = new ForkTestWorkerFactory(async ({ text, append }) => {
    if (text === "hold source turn") {
      sourceStarted.resolve();
      await releaseSource.promise;
    }
    await append("fork-command.log", `${text}\n`);
  });
  workers.holdForkStop = true;
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let forkError: unknown;
  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "fork-held-source", "hold source turn"));
    await waitForForkBoundary(sourceStarted.promise, "source turn start");
    const sourceWorker = workers.workers[0]!;
    const sourceSessionId = sourceWorker.sessionId;
    const sourceSessionFile = sourceWorker.sessionFile;
    const sourceBefore = await harness.service.status(FORK_GROUP);

    const forkMessage = ownerMessage(harness, FORK_GROUP, "fork-held-command", "/fork incident review");
    void harness.service.submitInbound(forkMessage).catch((error: unknown) => {
      forkError = error;
    });
    await waitForForkBoundary(workers.forkStopStarted.promise, "native fork worker stop");

    expect(transport.createAttempts).toBe(0);
    expect(sourceWorker.stopped).toBe(false);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(sourceSessionId);
    expect(sourceBefore.sessionId).toBe(sourceSessionId);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["hold source turn"]);

    workers.releaseForkStop.resolve();
    const handoff = await waitForIngressJob(harness, forkMessage, {
      predicate: job => job.state === "handed-off",
      rejectStates: [],
      description: "durable fork operation handoff",
    });
    expect(handoff).toMatchObject({ state: "handed-off", consumerKind: "operation" });
    const fork = await waitForForkRecord(
      harness,
      record => record.sourceChannelType === FORK_GROUP.channelType
        && record.sourceChannelId === FORK_GROUP.channelId
        && record.name === "incident review"
        && record.phase === "ready",
    );
    expect(forkError).toBeUndefined();
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toEqual([
      expect.objectContaining({ sourceSessionFile, cwd: sourceBefore.cwd }),
    ]);
    expect(sourceWorker.stopped).toBe(false);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["hold source turn"]);

    if (!fork.sessionId || !fork.sessionFile || !fork.thread) {
      throw new Error("fork result did not contain a usable target session");
    }
    const forkSessionId = fork.sessionId;
    const forkSessionFile = fork.sessionFile;
    expect(fork).toMatchObject({
      phase: "ready",
      sourceChannelType: 2,
      sourceChannelId: FORK_GROUP.channelId,
      sourceSessionId,
      sourceSessionFile,
      cwd: sourceBefore.cwd,
      name: "incident review",
      sessionId: expect.any(String),
      sessionFile: expect.any(String),
      thread: {
        channelId: `${FORK_GROUP.channelId}____thread-1`,
        groupId: FORK_GROUP.channelId,
        name: "incident review",
      },
    });
    const target: ChatTarget & { channelType: 5 } = {
      channelId: fork.thread.channelId,
      channelType: 5,
    };
    await withStore(harness, (store) => {
      expect(store.getBinding(harness.bot.uid, target)).toMatchObject({
        sessionId: forkSessionId,
        sessionFile: forkSessionFile,
        cwd: sourceBefore.cwd,
      });
      expect(store.getThreadWorkspace(harness.bot.uid, target)).toBeNull();
      expect(store.getThreadListen(harness.bot.uid, target)).toMatchObject({
        creatorChannelType: 2,
        creatorChannelId: FORK_GROUP.channelId,
        listen: "merge",
      });
    });
    await transport.waitForRemoteText(message =>
      message.target.channelType === FORK_GROUP.channelType
      && message.target.channelId === FORK_GROUP.channelId
      && message.text.includes("目录共享"),
    );
    const receipt = await waitForSentOutbox(harness, `ingress:${handoff.id}:receipt:0`);
    expect(receipt.text).toContain("目录共享");
    expect(textFor(harness, FORK_GROUP).join("\n")).toContain("目录共享");

    const targetMessage = ownerMessage(harness, target, "fork-first-target-turn", "read the inherited session");
    await harness.service.submitInbound(targetMessage);
    await workers.waitForPromptCount(2);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(workers.creations.at(-1)?.sessionFile).toBe(forkSessionFile);
    expect(workers.prompts.map(({ text, sessionId }) => ({ text, sessionId }))).toEqual([
      { text: "hold source turn", sessionId: sourceSessionId },
      { text: "read the inherited session", sessionId: forkSessionId },
    ]);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(sourceSessionId);

    await submitAndWaitForIngressHandoff(harness, forkMessage);
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
  } finally {
    releaseSource.resolve();
    workers.releaseForkStop.resolve();
    await closeCoreHarness(harness);
  }
});

test("the fork tool uses the current thread binding, creates a same-parent sibling, and keeps model/context policy independent", async () => {
  let toolResult = "";
  const workers = new ForkTestWorkerFactory(async ({ append, promptContext, text }) => {
    if (text === "invoke fork tool") {
      const tool = toolsOf(promptContext).find(({ name }) => name === "fork");
      if (!tool) throw new Error("fork host tool is missing in the current group thread");
      const signal = new AbortController().signal;
      const first = await tool.execute({ name: "review sibling" }, signal, "native-fork-call-7");
      const duplicate = await tool.execute({ name: "review sibling" }, signal, "native-fork-call-7");
      toolResult = `${first}\n${duplicate}`;
      await append("fork-tool-result.log", toolResult);
      return;
    }
    await append("fork-tool-result.log", `${text}\n`);
  });
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const cwd = join(harness.root, "shared thread cwd");
  await mkdir(cwd, { recursive: true });
  const sourceSummary = threadSummary(FORK_GROUP.channelId, "source-topic", "Source Topic");
  transport.setThreads(FORK_GROUP.channelId, [sourceSummary]);
  transport.setGroupContext(sourceThread, {
    content: "source-only context snapshot",
    version: 4,
    updatedAt: "2026-11-03T00:00:00.000Z",
    updatedBy: "owner-1",
  });
  const sourceSessionFile = await seedBinding(harness, sourceThread, cwd, "source-thread-session");
  await withStore(harness, (store) => {
    store.setThreadModel(harness.bot.uid, FORK_GROUP.channelId, "other/fast");
    store.setChatModel(harness.bot.uid, sourceThread, "fixture/default");
  });

  const canonicalCwd = await realpath(cwd);
  try {
    await harness.service.submitInbound(ownerMessage(harness, sourceThread, "fork-tool-source-turn", "invoke fork tool"));
    await waitForStatus(harness.service, sourceThread, (status) => status.state === "completed");
    expect(transport.createAttempts).toBe(1);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([
      { action: "create-thread", groupId: FORK_GROUP.channelId, shortId: null },
    ]);
    expect(workers.forkAttempts).toMatchObject([
      { sourceSessionFile, cwd: canonicalCwd },
    ]);
    expect(workers.forks).toHaveLength(1);
    expect(toolResult).toContain("thread-1");
    expect(await readText(join(cwd, "fork-tool-result.log"))).toContain(toolResult);

    const fork = await withStore(harness, (store) => store.listThreadForks(harness.bot.uid)[0]);
    expect(fork).toMatchObject({
      phase: "ready",
      sourceChannelType: 5,
      sourceChannelId: sourceThread.channelId,
      sourceSessionId: "source-thread-session",
      sourceSessionFile,
      cwd: canonicalCwd,
      name: "review sibling",
      thread: {
        groupId: FORK_GROUP.channelId,
        channelId: `${FORK_GROUP.channelId}____thread-1`,
      },
    });
    if (!fork?.sessionId || !fork.sessionFile || !fork.thread) {
      throw new Error("fork tool did not return a usable sibling session");
    }
    const target: ChatTarget & { channelType: 5 } = {
      channelId: fork.thread.channelId,
      channelType: 5,
    };
    expect(target.channelId).toBe(`${FORK_GROUP.channelId}____thread-1`);
    await withStore(harness, (store) => {
      expect(store.getBinding(harness.bot.uid, sourceThread)).toMatchObject({
        sessionId: "source-thread-session",
        sessionFile: sourceSessionFile,
        cwd: canonicalCwd,
      });
      expect(store.getChatModel(harness.bot.uid, target)).toBeNull();
      expect(store.resolveChatModel(harness.bot.uid, target)).toEqual({
        model: "other/fast",
        source: "thread",
      });
      expect(store.getThreadListen(harness.bot.uid, target)).toMatchObject({
        creatorChannelType: 5,
        creatorChannelId: sourceThread.channelId,
        listen: "merge",
      });
    });
    expect(workers.creations[0]?.model).toBe("fixture/default");
    const forkWorker = workers.creations.find((options) => "forkFrom" in options) as
      | (typeof workers.creations)[number] & { model?: string }
      | undefined;
    expect(forkWorker?.model).toBeUndefined();

    transport.setGroupContext(target, {
      content: "target-only context snapshot",
      version: 1,
      updatedAt: "2026-11-03T00:00:00.000Z",
      updatedBy: "owner-1",
    });
    await harness.service.submitInbound(ownerMessage(harness, target, "fork-tool-target-turn", "read own context"));
    await workers.waitForPromptCount(2);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const targetPrompt = workers.prompts.at(-1)!;
    expect(targetPrompt.sessionId).toBe(fork.sessionId);
    expect(targetPrompt.text).toBe("read own context");
    expect(targetPrompt.promptContext?.contextInjection?.snapshot.content).toBe("target-only context snapshot");
    expect(targetPrompt.promptContext?.contextInjection?.snapshot.content).not.toContain("source-only");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/fork is owner-only group/thread control and rejects private or unmentioned entry points", async () => {
  const transport = new ForkTestTransport();
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const unlisted = { channelId: "group-fork-unlisted", channelType: 2 as const };

  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_DM, "fork-dm-command", "/fork private"));
    await harness.service.submitInbound({
      ...ownerMessage(harness, FORK_GROUP, "fork-member-command", "/fork member"),
      senderId: "member-2",
    });
    await harness.service.submitInbound({
      ...ownerMessage(harness, FORK_GROUP, "fork-no-mention-command", "/fork no mention"),
      mentionedUserIds: [],
    });
    await harness.service.submitInbound(ownerMessage(harness, unlisted, "fork-unlisted-command", "/fork unlisted"));

    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);
    expect(workers.prompts).toHaveLength(0);
    await withStore(harness, (store) => {
      expect(store.getLatestTask(harness.bot.uid, FORK_DM)).toBeNull();
      expect(store.getLatestTask(harness.bot.uid, FORK_GROUP)).toBeNull();
      expect(store.getLatestTask(harness.bot.uid, unlisted)).toBeNull();
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("fork host tool exposes only optional name, requires a native call identity, and is absent from DMs", async () => {
  const privateTools: string[][] = [];
  const privateWorkers = new ForkTestWorkerFactory(async ({ promptContext }) => {
    privateTools.push(toolsOf(promptContext).map(({ name }) => name));
  });
  const privateHarness = await createCoreHarness({ workers: privateWorkers, transport: new ForkTestTransport() });
  try {
    await privateHarness.service.submitInbound(ownerMessage(privateHarness, FORK_DM, "fork-dm-tool", "inspect private tools"));
    await waitForStatus(privateHarness.service, FORK_DM, (status) => status.state === "completed");
    expect(privateTools).toHaveLength(1);
    expect(privateTools[0]).not.toContain("fork");
  } finally {
    await closeCoreHarness(privateHarness);
  }

  const checked = gate();
  const workers = new ForkTestWorkerFactory(async ({ promptContext, text }) => {
    if (text !== "validate fork args") return;
    const tool = toolsOf(promptContext).find(({ name }) => name === "fork");
    if (!tool) throw new Error("fork host tool is missing in a permitted group");
    const parameters = tool.parameters as {
      properties?: Record<string, unknown>;
      required?: string[];
      additionalProperties?: boolean;
    };
    expect(Object.keys(parameters.properties ?? {}).sort()).toEqual(["name"]);
    expect(parameters.required ?? []).toEqual([]);
    expect(parameters.additionalProperties).toBe(false);
    const rejections = await Promise.all([
      tool.execute(
        { name: "valid", cwd: "/tmp", sessionFile: "/tmp/source" },
        new AbortController().signal,
        "bad-args-call",
      ).then(() => false, () => true),
      tool.execute({ name: "valid" }, new AbortController().signal).then(() => false, () => true),
    ]);
    expect(rejections).toEqual([true, true]);
    checked.resolve();
  });
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "fork-tool-invalid-args", "validate fork args"));
    await waitForForkBoundary(checked.promise, "fork argument checks");
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);
    expect(workers.creations.some((options) => "forkFrom" in options)).toBe(false);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("/fork reports actual partial session and thread identities when target binding conflicts", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  let harness!: CoreHarness;
  const updateGroupContext = transport.updateGroupContext.bind(transport);
  transport.updateGroupContext = async (target, content, signal) => {
    if (target.channelType === 5) {
      const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
      if (!source) throw new Error("source binding was not persisted");
      await withStore(harness, (store) => store.ensureBinding({
        botUid: harness.bot.uid,
        ...target,
        sessionId: "conflicting-target-session",
        sessionFile: null,
        cwd: source.cwd,
      }));
    }
    return updateGroupContext(target, content, signal);
  };
  harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "fork-receipt-source", "persist receipt source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const conflictMessage = ownerMessage(harness, FORK_GROUP, "fork-receipt-conflict", "/fork receipt target");
    await harness.service.submitInbound(conflictMessage);
    const record = await waitForForkRecord(
      harness,
      candidate => candidate.sourceChannelId === FORK_GROUP.channelId
        && candidate.phase === "remote-created",
    );
    const receipt = await harness.transport.waitForRemoteText(message =>
      message.target.channelType === FORK_GROUP.channelType
      && message.target.channelId === FORK_GROUP.channelId
      && message.text.includes("session_id="),
    );
    if (!record.sessionId || !record.thread) throw new Error("known partial fork identity was not persisted");
    expect(record.phase).toBe("remote-created");
    expect(receipt.text).toContain(`session_id=${record.sessionId}`);
    expect(receipt.text).toContain(`short_id=${record.thread.shortId}`);
    expect(receipt.text).toContain(`name=${record.thread.name}`);
    expect(receipt.text).toContain(`cwd=${record.cwd}`);
    expect(transport.createAttempts).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a detached fork tool releases a deferred restart after its source turn has already settled", async () => {
  const forkCallStarted = gate();
  const restartRequests: number[] = [];
  const forkAbort = new AbortController();
  let forkOperation: Promise<string> | undefined;
  let restartResult = "";
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const workers = new ForkTestWorkerFactory(async ({ promptContext, text }) => {
    if (text !== "start detached fork tool") return;
    const tools = toolsOf(promptContext);
    const forkTool = tools.find(({ name }) => name === "fork");
    const channelTool = tools.find(({ name }) => name === "channel_control");
    if (!forkTool || !channelTool) throw new Error("fork or channel_control host tool is missing");
    const operation = forkTool.execute(
      { name: "detached fork" },
      forkAbort.signal,
      "detached-fork-operation",
    );
    forkOperation = operation;
    void operation.catch(() => {});
    restartResult = await channelTool.execute({ action: "restart" }, new AbortController().signal);
    forkCallStarted.resolve();
  });
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
    restart: {
      request: async () => {
        restartRequests.push(Date.now());
      },
    },
  });

  try {
    await harness.service.submitInbound(ownerMessage(
      harness,
      FORK_GROUP,
      "fork-tool-deferred-restart-source",
      "start detached fork tool",
    ));
    await forkCallStarted.promise;
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    await waitForForkBoundary(transport.createThreadStarted.promise, "detached fork remote create");
    expect(restartResult).toContain("已排定重启");
    expect(restartRequests).toHaveLength(0);

    forkAbort.abort(new Error("native fork tool call cancelled"));
    await forkOperation?.catch(() => {});
    for (let attempt = 0; attempt < 2000 && restartRequests.length === 0; attempt += 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    expect(restartRequests).toHaveLength(1);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["start detached fork tool"]);
  } finally {
    forkAbort.abort(new Error("test cleanup"));
    transport.releaseCreateThread.resolve();
    await forkOperation?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("/fork keeps a deferred restart pending until its result is sent and command claim completes", async () => {
  const restartScheduled = gate();
  const restartRequested = gate();
  const resultSendStarted = gate();
  const releaseResultSend = gate();
  const resultSendFinished = gate();
  const restartObservations: Array<{ receiptSent: boolean; claimState: string | null }> = [];
  const restartCalls: number[] = [];
  const forkCommandId = "fork-deferred-restart-command";
  let restartResult = "";
  let harness!: CoreHarness;
  const transport = new ForkTestTransport();
  const sendText = transport.sendText.bind(transport);
  transport.sendText = async (target, text, clientMsgNo, signal) => {
    if (
      target.channelType === FORK_GROUP.channelType
      && target.channelId === FORK_GROUP.channelId
      && text.startsWith("已创建子区")
    ) {
      resultSendStarted.resolve();
      await releaseResultSend.promise;
      const receipt = await sendText(target, text, clientMsgNo, signal);
      resultSendFinished.resolve();
      return receipt;
    }
    return sendText(target, text, clientMsgNo, signal);
  };
  const workers = new ForkTestWorkerFactory(async ({ promptContext, text }) => {
    if (text !== "schedule deferred restart during fork") return;
    const channelTool = toolsOf(promptContext).find(({ name }) => name === "channel_control");
    if (!channelTool) throw new Error("channel_control host tool is missing");
    restartResult = await channelTool.execute({ action: "restart" }, new AbortController().signal);
    restartScheduled.resolve();
  });
  workers.holdForkStop = true;
  harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
    restart: {
      request: async () => {
        restartCalls.push(Date.now());
        const claimState = await withStore(harness, (store) =>
          store.getCommandClaimState(harness.bot.uid, FORK_GROUP, forkCommandId));
        restartObservations.push({
          receiptSent: textFor(harness, FORK_GROUP).some((text) => text.startsWith("已创建子区")),
          claimState,
        });
        restartRequested.resolve();
      },
    },
  });
  let forkError: unknown;
  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "fork-restart-source", "persist restart source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");

    const forkCommandMessage = ownerMessage(harness, FORK_GROUP, forkCommandId, "/fork deferred restart");
    void harness.service.submitInbound(forkCommandMessage).catch((error: unknown) => {
      forkError = error;
    });
    await waitForForkBoundary(workers.forkStopStarted.promise, "held native fork preparation stop");

    await harness.service.submitInbound(ownerMessage(
      harness,
      FORK_DM,
      "fork-restart-request",
      "schedule deferred restart during fork",
    ));
    await restartScheduled.promise;
    await waitForStatus(harness.service, FORK_DM, (status) => status.state === "completed");
    expect(restartResult).toContain("已排定重启");
    expect(restartCalls).toHaveLength(0);

    workers.releaseForkStop.resolve();
    await waitForForkRecord(
      harness,
      record => record.name === "deferred restart" && record.phase === "ready",
    );
    const forkAdmission = await waitForIngressJob(harness, forkCommandMessage, {
      predicate: job => job.state === "accepted",
      rejectStates: [],
      description: "deferred fork admission",
    });
    await waitForForkBoundary(resultSendStarted.promise, "held fork result delivery");
    expect(restartCalls).toHaveLength(0);
    expect(await withStore(harness, (store) =>
      store.getCommandClaimState(harness.bot.uid, FORK_GROUP, forkCommandId)))
      .toBe("pending");

    releaseResultSend.resolve();
    await resultSendFinished.promise;
    const receipt = await waitForSentOutbox(harness, `ingress:${forkAdmission.id}:receipt:0`);
    expect(receipt.text).toMatch(/^已创建子区/u);
    await restartRequested.promise;
    expect(forkError).toBeUndefined();
    expect(restartObservations).toEqual([{ receiptSent: true, claimState: "completed" }]);
    expect(restartCalls).toHaveLength(1);
  } finally {
    workers.releaseForkStop.resolve();
    releaseResultSend.resolve();
    restartScheduled.resolve();
    restartRequested.resolve();
    resultSendStarted.resolve();
    resultSendFinished.resolve();
    await closeCoreHarness(harness);
  }
});

test("a pending source ask remains source-owned while a fork target starts a fresh user turn", async () => {
  const sourceAskStarted = gate();
  const targetTurnStarted = gate();
  const workers = new ForkTestWorkerFactory(async ({ promptContext, text }) => {
    if (text === "ask in source") {
      const ask = toolsOf(promptContext).find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask tool is missing");
      sourceAskStarted.resolve();
      await ask.execute(
        { questions: [{ question: "source-only question?" }] },
        new AbortController().signal,
        "source-ask-call",
      );
      return;
    }
    if (text === "new target message") {
      targetTurnStarted.resolve();
    }
  });
  const transport = new ForkTestTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "fork-pending-ask-source", "ask in source"));
    await waitForForkBoundary(sourceAskStarted.promise, "source ask start");
    const sourceQuestion = await harness.service.status(FORK_GROUP);
    expect(sourceQuestion.pendingQuestionIds).toHaveLength(1);

    const sourceForkMessage = ownerMessage(harness, FORK_GROUP, "fork-with-source-ask", "/fork independent ask");
    await harness.service.submitInbound(sourceForkMessage);
    const fork = await waitForForkRecord(
      harness,
      record => record.sourceChannelId === FORK_GROUP.channelId
        && record.name === "independent ask"
        && record.phase === "ready",
    );
    if (!fork.sessionId || !fork.thread) throw new Error("fork command did not return a target");
    const target: ChatTarget & { channelType: 5 } = {
      channelId: fork.thread.channelId,
      channelType: 5,
    };
    expect((await harness.service.status(target)).pendingQuestionIds).toEqual([]);

    await harness.service.submitInbound(ownerMessage(harness, target, "fork-target-not-source-answer", "new target message"));
    await waitForForkBoundary(targetTurnStarted.promise, "target user turn");
    expect(workers.prompts.at(-1)?.text).toBe("new target message");
    expect(workers.prompts.at(-1)?.sessionId).toBe(fork.sessionId);
    expect((await harness.service.status(FORK_GROUP)).pendingQuestionIds).toEqual(sourceQuestion.pendingQuestionIds);
    expect((await harness.service.status(target)).pendingQuestionIds).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});
