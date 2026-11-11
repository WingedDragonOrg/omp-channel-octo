import { expect, test } from "bun:test";
import { OctoError } from "../src/octo/error.ts";
import { mkdir, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import type { ThreadForkRecord } from "../src/store/store-records.ts";
import {
  createCoreHarness,
  closeCoreHarness,
  waitForStatus,
  waitForIngressJob,
  submitAndWaitForIngressHandoff,
  type CoreHarness,
  type RemoteTextMessage,
} from "./fixtures/core-fakes.ts";
import {
  FORK_GROUP,
  ForkTestTransport,
  ForkTestWorkerFactory,
  ownerMessage,
  threadSummary,
  waitForForkBoundary,
  withStore,
} from "./fixtures/core-fork.ts";

const incoming = (harness: CoreHarness, target: ChatTarget, id: string, text: string) =>
  ownerMessage(harness, target, id, text);


async function waitForForkRecord(
  harness: CoreHarness,
  predicate: (record: ThreadForkRecord) => boolean,
): Promise<ThreadForkRecord> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const record = await withStore(harness, store => store.listThreadForks(harness.bot.uid).find(predicate));
    if (record !== undefined) return record;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error("fork operation did not reach the requested durable state");
}

async function waitForUnknownRemoteCreate(
  harness: CoreHarness,
  sourceSessionId?: string,
): Promise<ThreadForkRecord> {
  return await waitForForkRecord(
    harness,
    record => (sourceSessionId === undefined || record.sourceSessionId === sourceSessionId)
      && record.phase === "creating"
      && record.error !== null,
  );
}

function waitForTargetText(
  harness: CoreHarness,
  target: ChatTarget,
  predicate: (text: string) => boolean,
): Promise<RemoteTextMessage> {
  return harness.transport.waitForRemoteText(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && predicate(message.text));
}

function waitForNextTargetText(harness: CoreHarness, target: ChatTarget): Promise<RemoteTextMessage> {
  const existingMessageIds = new Set(
    harness.transport.listRemoteText(target).map(message => message.messageId),
  );
  return harness.transport.waitForRemoteText(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && !existingMessageIds.has(message.messageId));
}

async function waitForTaskState(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  state: string,
): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const task = await withStore(harness, store => store.getTaskByMessageId(harness.bot.uid, target, messageId));
    if (task?.state === state) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`task ${messageId} did not reach state ${state}`);
}
test("fork refuses missing bindings, absent session files, and session headers that disagree with the binding", async () => {
  const targets = [
    { channelId: "group-fork-no-binding", channelType: 2 as const },
    { channelId: "group-fork-no-file", channelType: 2 as const },
    { channelId: "group-fork-absent-file", channelType: 2 as const },
    { channelId: "group-fork-stale-file", channelType: 2 as const },
  ];
  const transport = new ForkTestTransport();
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: targets.map(({ channelId }) => channelId),
  });
  const cwd = join(harness.root, "source cwd");
  await mkdir(cwd, { recursive: true });
  const canonicalCwd = await realpath(cwd);
  const missingSessionFile = join(harness.root, "missing-bound-session.jsonl");
  const staleSessionFile = join(harness.root, "stale-bound-session.jsonl");
  await writeFile(
    staleSessionFile,
    `${JSON.stringify({ type: "session", id: "actual-file-session", cwd: canonicalCwd })}\n`,
    "utf8",
  );
  await withStore(harness, (store) => {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...targets[1]!,
      sessionId: "session-with-no-file",
      cwd: canonicalCwd,
      sessionFile: null,
    });
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...targets[2]!,
      sessionId: "session-with-absent-file",
      cwd: canonicalCwd,
      sessionFile: missingSessionFile,
    });
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...targets[3]!,
      sessionId: "binding-session-id",
      cwd: canonicalCwd,
      sessionFile: staleSessionFile,
    });
  });

  try {
    for (const [index, target] of targets.entries()) {
      await submitAndWaitForIngressHandoff(harness, incoming(harness, target, `fork-invalid-source-${index}`, "/fork invalid"));
    }
    expect(workers.forkAttempts).toEqual([]);
    expect(workers.creations).toEqual([]);
    expect(transport.createAttempts).toBe(0);
    await withStore(harness, (store) => {
      expect(store.getBinding(harness.bot.uid, targets[0]!)).toBeNull();
      expect(store.getBinding(harness.bot.uid, targets[1]!)).toMatchObject({
        sessionId: "session-with-no-file",
        sessionFile: null,
      });
      expect(store.getBinding(harness.bot.uid, targets[2]!)).toMatchObject({
        sessionId: "session-with-absent-file",
        sessionFile: missingSessionFile,
      });
      expect(store.getBinding(harness.bot.uid, targets[3]!)).toMatchObject({
        sessionId: "binding-session-id",
        sessionFile: staleSessionFile,
      });
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("fork records the validated candidate identity but stays unavailable when native preparation exit is uncertain", async () => {
  const workers = new ForkTestWorkerFactory();
  workers.forkStopResult = false;
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-prep-source", "persist source session"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    const forkMessage = incoming(harness, FORK_GROUP, "fork-prep-unknown-exit", "/fork candidate");
    const reply = waitForNextTargetText(harness, FORK_GROUP);
    await harness.service.submitInbound(forkMessage);
    await reply;
    const record = await waitForForkRecord(
      harness,
      candidate => candidate.sourceChannelId === FORK_GROUP.channelId
        && candidate.sessionId !== null
        && candidate.phase !== "ready",
    );
    expect(record).toMatchObject({
      sourceSessionId: source?.sessionId,
      sourceSessionFile: source?.sessionFile,
      sessionId: workers.forks[0]?.sessionId,
      sessionFile: workers.forks[0]?.sessionFile,
    });
    expect(record?.sessionId).not.toBeNull();
    expect(record?.sessionFile).not.toBeNull();
    expect(record?.phase).not.toBe("ready");
    expect(workers.forkAttempts[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(transport.createAttempts).toBe(0);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["persist source session"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("startup reads the exact stopped ledger and terminalizes its candidate without remote creation", async () => {
  const workers = new ForkTestWorkerFactory();
  workers.forkStopResult = false;
  const transport = new ForkTestTransport();
  let harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-stopped-ledger-source", "persist stopped-ledger source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const forkMessage = incoming(harness, FORK_GROUP, "fork-stopped-ledger", "/fork stopped ledger");
    await harness.service.submitInbound(forkMessage);
    const preparing = await waitForForkRecord(
      harness,
      candidate => candidate.sourceChannelId === FORK_GROUP.channelId
        && candidate.sessionId !== null
        && candidate.phase === "preparing",
    );
    if (!preparing?.sessionId || !preparing.sessionFile) throw new Error("preparation candidate was not persisted");
    const ledger = await withStore(harness, (store) =>
      store.listWorkerLedger(harness.bot.uid).find(({ id }) => id === `thread-fork:${preparing.id}`));
    if (!ledger) throw new Error("preparation worker ledger was not recorded");
    await withStore(harness, (store) => store.markWorkerStopped(ledger.id));

    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds: [FORK_GROUP.channelId] });

    const recovered = await withStore(harness, (store) => store.getThreadFork(bot.uid, preparing.id));
    expect(recovered).toMatchObject({
      phase: "failed",
      sessionId: preparing.sessionId,
      sessionFile: preparing.sessionFile,
    });
    expect(await withStore(harness, (store) => store.getThreadWorkspaceIntent(bot.uid, preparing.id)))
      .toMatchObject({ status: "failed" });
    expect(workers.forkAttempts).toHaveLength(1);
    expect(transport.createAttempts).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test.each([true, false])("an undispatched fork recovers without blocking its source (intent=%s)", async (intentPersisted) => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  let harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-no-ledger-source", "persist no-ledger source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    if (!source?.sessionFile) throw new Error("source session was not persisted");
    const id = "thread-fork-crash-before-spawn";
    await withStore(harness, (store) => {
      store.saveThreadFork({
        id,
        botUid: harness.bot.uid,
        sourceChannelType: 2,
        sourceChannelId: FORK_GROUP.channelId,
        sourceSessionId: source.sessionId,
        sourceSessionFile: source.sessionFile!,
        cwd: source.cwd,
        groupId: FORK_GROUP.channelId,
        name: "crash before spawn",
        preparationDir: join(harness.stateDir, "fork-preparations", id),
        phase: "preparing",
        sessionId: null,
        sessionFile: null,
        thread: null,
        error: null,
      });
      if (intentPersisted) store.saveThreadWorkspaceIntent({
        id,
        botUid: harness.bot.uid,
        groupId: FORK_GROUP.channelId,
        status: "preparing",
        channelIds: [],
      });
    });

    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds: [FORK_GROUP.channelId] });

    const recovered = await withStore(harness, (store) => store.getThreadFork(bot.uid, id));
    expect(recovered?.phase).toBe("failed");
    expect(await withStore(harness, (store) => store.getThreadWorkspaceIntent(bot.uid, id)))
      .toEqual(intentPersisted ? expect.objectContaining({ status: "failed" }) : null);
    const nextTurn = incoming(harness, FORK_GROUP, "fork-no-ledger-next-turn", "source remains usable");
    await harness.service.submitInbound(nextTurn);
    await waitForTaskState(harness, FORK_GROUP, nextTurn.messageId, "completed");
    expect(workers.prompts.map(({ text }) => text)).toContain("source remains usable");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a dispatched preparation with a missing ledger remains unknown and prevents retry", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  let harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-dispatched-no-ledger-source", "persist dispatched source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    if (!source?.sessionFile) throw new Error("source session was not persisted");
    const id = "thread-fork-crash-after-dispatch";
    await withStore(harness, (store) => {
      const fork = {
        id,
        botUid: harness.bot.uid,
        sourceChannelType: 2 as const,
        sourceChannelId: FORK_GROUP.channelId,
        sourceSessionId: source.sessionId,
        sourceSessionFile: source.sessionFile!,
        cwd: source.cwd,
        groupId: FORK_GROUP.channelId,
        name: "crash after dispatch",
        preparationDir: join(harness.stateDir, "fork-preparations", id),
        preparationStarted: true,
        phase: "preparing" as const,
        sessionId: null,
        sessionFile: null,
        thread: null,
        error: null,
      };
      store.saveThreadFork(fork);
      store.saveThreadWorkspaceIntent({
        id,
        botUid: harness.bot.uid,
        groupId: FORK_GROUP.channelId,
        status: "preparing",
        channelIds: [],
      });
    });

    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds: [FORK_GROUP.channelId] });

    const recovered = await withStore(harness, (store) => store.getThreadFork(bot.uid, id));
    expect(recovered).toMatchObject({ phase: "preparing", preparationStarted: true });
    await submitAndWaitForIngressHandoff(
      harness,
      incoming(harness, FORK_GROUP, "fork-dispatched-no-ledger-retry", "/fork unsafe retry"),
    );
    expect(workers.forkAttempts).toHaveLength(0);
    expect(transport.createAttempts).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an HTTP 503 after remote allocation remains globally blocked and reports the known candidate after restart", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const allowedGroupIds = [FORK_GROUP.channelId];
  let harness = await createCoreHarness({ transport, workers, allowedGroupIds });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-503-source", "persist 503 source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    transport.afterCreateFailure = new OctoError("server returned 503 after allocating the child", { status: 503 });
    const forkMessage = incoming(harness, FORK_GROUP, "fork-503-operation", "/fork transient server error");
    await harness.service.submitInbound(forkMessage);
    const record = await waitForForkRecord(
      harness,
      candidate => candidate.sourceChannelId === FORK_GROUP.channelId
        && candidate.sessionId !== null
        && candidate.phase !== "ready",
    );
    if (!record.sessionId) throw new Error("native fork candidate was not retained");
    const receipt = await waitForTargetText(harness, FORK_GROUP, text => text.includes(record.sessionId!));
    expect(receipt.text).toContain(record.sessionId);
    expect(transport.createAttempts).toBe(1);
    const [remoteThread] = await transport.listThreads(FORK_GROUP.channelId);
    if (!remoteThread) throw new Error("fixture did not allocate the remote child before returning 503");

    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds });
    const sibling: ChatTarget = { channelType: 5, channelId: remoteThread.channelId };
    await submitAndWaitForIngressHandoff(harness, incoming(harness, sibling, "fork-503-unbound-inbound", "must stay blocked"));
    expect(await withStore(harness, (store) => store.getBinding(bot.uid, sibling))).toBeNull();
    expect(workers.prompts.map(({ text }) => text)).not.toContain("must stay blocked");

    await submitAndWaitForIngressHandoff(harness, forkMessage, { rejectStates: [] });
    expect(transport.createAttempts).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("startup terminalizes a prepared fork with no durable continuation so the source can start a new fork", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  let harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-prepared-recovery-source", "persist prepared source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    if (!source?.sessionFile) throw new Error("source session was not persisted");
    const id = "thread-fork-prepared-without-continuation";
    const candidateSessionId = "prepared-candidate-session";
    const candidateSessionFile = join(harness.root, "prepared-candidate-session.jsonl");
    await writeFile(
      candidateSessionFile,
      `${JSON.stringify({ type: "session", id: candidateSessionId, cwd: source.cwd, previousSessionFiles: [] })}\n`,
      "utf8",
    );
    await withStore(harness, (store) => {
      store.saveThreadFork({
        id,
        botUid: harness.bot.uid,
        sourceChannelType: 2,
        sourceChannelId: FORK_GROUP.channelId,
        sourceSessionId: source.sessionId,
        sourceSessionFile: source.sessionFile!,
        cwd: source.cwd,
        groupId: FORK_GROUP.channelId,
        name: "prepared without continuation",
        preparationDir: join(harness.stateDir, "fork-preparations", id),
        preparationStarted: true,
        phase: "prepared",
        sessionId: candidateSessionId,
        sessionFile: candidateSessionFile,
        thread: null,
        error: null,
      });
      store.saveThreadWorkspaceIntent({
        id,
        botUid: harness.bot.uid,
        groupId: FORK_GROUP.channelId,
        status: "preparing",
        channelIds: [],
      });
    });

    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds: [FORK_GROUP.channelId] });
    const recovered = await withStore(harness, (store) => store.getThreadFork(bot.uid, id));
    expect(recovered).toMatchObject({
      phase: "failed",
      sessionId: candidateSessionId,
      sessionFile: candidateSessionFile,
    });
    expect(await withStore(harness, (store) => store.getThreadWorkspaceIntent(bot.uid, id)))
      .toMatchObject({ status: "failed" });
    expect(transport.createAttempts).toBe(0);

    const replacementMessage = incoming(harness, FORK_GROUP, "fork-after-prepared-recovery", "/fork replacement");
    await harness.service.submitInbound(replacementMessage);
    const replacement = await waitForForkRecord(
      harness,
      candidate => candidate.sourceChannelId === FORK_GROUP.channelId
        && candidate.phase === "ready"
        && candidate.id !== id,
    );
    const forks = await withStore(harness, (store) => store.listThreadForks(bot.uid));
    expect(forks).toHaveLength(2);
    expect(forks[0]).toMatchObject({ phase: "failed", sessionId: candidateSessionId });
    expect(forks[1]).toMatchObject({ id: replacement.id, phase: "ready" });
    expect(workers.forkAttempts).toHaveLength(1);
    expect(transport.createAttempts).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a fork target with conflicting persisted binding stays blocked instead of switching to an empty session", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const target: ChatTarget = { channelId: `${FORK_GROUP.channelId}____thread-1`, channelType: 5 };

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-conflict-source", "persist source session"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    if (!source) throw new Error("source binding was not persisted");
    await withStore(harness, (store) => store.ensureBinding({
      botUid: harness.bot.uid,
      ...target,
      sessionId: "unrelated-existing-session",
      sessionFile: null,
      cwd: source.cwd,
    }));

    const forkMessage = incoming(harness, FORK_GROUP, "fork-conflicting-target", "/fork conflict");
    await harness.service.submitInbound(forkMessage);
    const record = await waitForForkRecord(
      harness,
      candidate => candidate.thread?.channelId === target.channelId && candidate.phase === "remote-created",
    );
    expect(record).toMatchObject({
      phase: "remote-created",
      thread: { channelId: target.channelId },
      sessionId: expect.any(String),
    });
    expect(transport.createAttempts).toBe(1);
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, target))).toMatchObject({
      sessionId: "unrelated-existing-session",
      cwd: source.cwd,
    });

    const creationsBeforeBlockedTarget = workers.creations.length;
    const blockedNew = incoming(harness, target, "fork-target-new-blocked", "/new");
    const blockedNewReply = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(blockedNew);
    await blockedNewReply;
    const blockedMessage = incoming(harness, target, "fork-target-message-blocked", "do not start empty session");
    const blockedMessageReply = waitForNextTargetText(harness, target);
    await harness.service.submitInbound(blockedMessage);
    await blockedMessageReply;
    expect(workers.creations).toHaveLength(creationsBeforeBlockedTarget);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["persist source session"]);
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, target)?.sessionId))
      .toBe("unrelated-existing-session");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an unknown remote create is never repeated after source /new, duplicate delivery, or restart", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const allowedGroupIds = [FORK_GROUP.channelId];
  let harness = await createCoreHarness({ transport, workers, allowedGroupIds });
  const forkMessage = incoming(harness, FORK_GROUP, "fork-remote-unknown-operation", "/fork remote uncertainty");
  let sourceSessionId = "";
  let sourceSessionFile: string | null = null;

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-unknown-source", "persist source before create"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const source = await withStore(harness, (store) => store.getBinding(harness.bot.uid, FORK_GROUP));
    if (!source) throw new Error("source binding was not persisted");
    sourceSessionId = source.sessionId;
    sourceSessionFile = source.sessionFile;

    transport.afterCreateFailure = new Error("transport timed out after remote thread creation");
    const firstHandoff = await submitAndWaitForIngressHandoff(harness, forkMessage, { rejectStates: [] });
    expect(firstHandoff).toMatchObject({ state: "handed-off", consumerKind: "operation" });
    const unknownBeforeReplay = await waitForUnknownRemoteCreate(harness, sourceSessionId);
    expect(transport.createAttempts).toBe(1);
    expect((await transport.listThreads(FORK_GROUP.channelId)).map(({ channelId }) => channelId))
      .toEqual([`${FORK_GROUP.channelId}____thread-1`]);

    const sourceNew = incoming(harness, FORK_GROUP, "fork-source-new-after-unknown", "/new");
    await harness.service.submitInbound(sourceNew);
    await waitForStatus(harness.service, FORK_GROUP, status => status.sessionId !== sourceSessionId);
    await harness.service.submitInbound(forkMessage).catch(() => {});
    await waitForUnknownRemoteCreate(harness, sourceSessionId);
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect(unknownBeforeReplay).toMatchObject({
      sourceSessionId,
      sourceSessionFile,
      phase: "creating",
      error: expect.stringContaining("结果不明"),
    });
    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds });
    await harness.service.submitInbound(forkMessage).catch(() => {});
    const recordAfterRestart = await waitForUnknownRemoteCreate(harness, sourceSessionId);
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect(recordAfterRestart).toMatchObject({ sourceSessionId, sourceSessionFile });

  } finally {
    await closeCoreHarness(harness);
  }
});

test("/cancel aborts the in-flight fork operation and still applies normal source-task cancellation", async () => {
  const sourceStarted = Promise.withResolvers<void>();
  const releaseSource = Promise.withResolvers<void>();
  const workers = new ForkTestWorkerFactory(async ({ text }) => {
    if (text === "hold source for cancellation") {
      sourceStarted.resolve();
      await releaseSource.promise;
    }
  });
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let forkRequest: Promise<void> | undefined;
  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-cancel-source-running", "hold source for cancellation"));
    await waitForForkBoundary(sourceStarted.promise, "source turn for cancellation");
    forkRequest = harness.service
      .submitInbound(incoming(harness, FORK_GROUP, "fork-cancel-operation", "/fork cancel me"))
      .catch(() => {});
    await waitForForkBoundary(transport.createThreadStarted.promise, "cancelled remote create");
    const cancelMessage = incoming(harness, FORK_GROUP, "fork-cancel-command", "/cancel");
    await harness.service.submitInbound(cancelMessage);
    await waitForTaskState(harness, FORK_GROUP, "fork-cancel-source-running", "cancelled");
    expect(transport.lastCreateSignal?.aborted).toBe(true);
    transport.releaseCreateThread.resolve();
    await forkRequest;
    const unknown = await waitForUnknownRemoteCreate(harness);

    expect(transport.lastCreateSignal?.aborted).toBe(true);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([]);
    expect(transport.createAttempts).toBe(1);
    expect((await harness.service.status(FORK_GROUP)).state).toBe("cancelled");
    expect(unknown).toMatchObject({
      phase: "creating",
      error: expect.stringContaining("结果不明"),
    });
  } finally {
    releaseSource.resolve();
    transport.releaseCreateThread.resolve();
    workers.releaseForkStop.resolve();
    await forkRequest;
    await closeCoreHarness(harness);
  }
});

test("/interrupt stops an in-flight fork without cancelling queued source work", async () => {
  const sourceStarted = Promise.withResolvers<void>();
  const releaseSource = Promise.withResolvers<void>();
  const queuedStarted = Promise.withResolvers<void>();
  const workers = new ForkTestWorkerFactory(async ({ text, onStop, isStopped, waitForAbort }) => {
    if (text === "hold source for interrupt") {
      onStop(releaseSource.resolve);
      sourceStarted.resolve();
      await Promise.race([releaseSource.promise, waitForAbort()]);
      if (isStopped()) throw new Error("fixture source task interrupted");
    }
    if (text === "queued after interrupt") queuedStarted.resolve();
  });
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let first: Promise<void> | undefined;
  let forkRequest: Promise<void> | undefined;

  try {
    const modeMessage = incoming(harness, FORK_GROUP, "fork-interrupt-mode", "/mode queue");
    const modeReply = waitForNextTargetText(harness, FORK_GROUP);
    await harness.service.submitInbound(modeMessage);
    await modeReply;
    first = harness.service.submitInbound(
      incoming(harness, FORK_GROUP, "fork-interrupt-source", "hold source for interrupt"),
    );
    await waitForForkBoundary(sourceStarted.promise, "source turn for interrupt");
    await harness.service.submitInbound(
      incoming(harness, FORK_GROUP, "fork-interrupt-queued", "queued after interrupt"),
    );
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.queued === 1);

    forkRequest = harness.service
      .submitInbound(incoming(harness, FORK_GROUP, "fork-interrupt-operation", "/fork interrupt me"))
      .catch(() => {});
    await waitForForkBoundary(transport.createThreadStarted.promise, "interrupted remote create");
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-interrupt-command", "/interrupt"));
    await waitForTaskState(harness, FORK_GROUP, "fork-interrupt-source", "interrupted");
    expect(transport.lastCreateSignal?.aborted).toBe(true);
    transport.releaseCreateThread.resolve();
    await Promise.all([first, forkRequest]);
    await waitForForkBoundary(queuedStarted.promise, "queued task after interrupt");
    await waitForStatus(
      harness.service,
      FORK_GROUP,
      (status) => status.state === "completed" && status.queued === 0,
    );
    const unknown = await waitForUnknownRemoteCreate(harness);

    expect(transport.lastCreateSignal?.aborted).toBe(true);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([]);
    expect(transport.createAttempts).toBe(1);
    expect(unknown).toMatchObject({
      phase: "creating",
      error: expect.stringContaining("结果不明"),
    });
    expect(workers.prompts.map(({ text }) => text)).toEqual([
      "hold source for interrupt",
      "queued after interrupt",
    ]);
    expect(await withStore(harness, (store) =>
      store.getTaskByMessageId(harness.bot.uid, FORK_GROUP, "fork-interrupt-source")))
      .toMatchObject({ state: "interrupted" });
    expect(await withStore(harness, (store) =>
      store.getTaskByMessageId(harness.bot.uid, FORK_GROUP, "fork-interrupt-queued")))
      .toMatchObject({ state: "completed" });
  } finally {
    releaseSource.resolve();
    transport.releaseCreateThread.resolve();
    workers.releaseForkStop.resolve();
    await first?.catch(() => {});
    await forkRequest;
    await closeCoreHarness(harness);
  }
});

test("fork refuses a remote result whose parent identity differs from the planned source group", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const foreignThread = threadSummary("foreign-group", "foreign-short");
  transport.threadResultOverride = foreignThread;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-foreign-source", "persist source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const forkMessage = incoming(harness, FORK_GROUP, "fork-foreign-result", "/fork foreign");
    await harness.service.submitInbound(forkMessage);
    const record = await waitForForkRecord(
      harness,
      candidate => candidate.sessionId !== null
        && candidate.thread !== null
        && candidate.phase !== "ready",
    );
    expect(transport.createAttempts).toBe(1);
    expect(await withStore(harness, (store) =>
      store.getBinding(harness.bot.uid, { channelId: foreignThread.channelId, channelType: 5 })))
      .toBeNull();
    if (!record.sessionId || !record.thread) throw new Error("foreign thread result was not retained");
    expect(transport.groupContextCalls.some(({ channelId }) => channelId === foreignThread.channelId)).toBe(false);
    const receipt = await waitForTargetText(
      harness,
      FORK_GROUP,
      text => text.includes(`session_id=${record.sessionId}`),
    );
    expect(receipt.text).toContain(`session_id=${record.sessionId}`);
    expect(receipt.text).toContain(`short_id=${foreignThread.shortId}`);
    expect(receipt.text).toContain(`name=${foreignThread.name}`);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["persist source"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("shutdown preserves an unknown in-flight create and restart does not retry it", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const allowedGroupIds = [FORK_GROUP.channelId];
  let harness = await createCoreHarness({ transport, workers, allowedGroupIds });
  const forkMessage = incoming(harness, FORK_GROUP, "fork-shutdown-unknown-operation", "/fork shutdown");
  let forkRequest: Promise<void> | undefined;

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-shutdown-source", "persist before shutdown"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    forkRequest = harness.service.submitInbound(forkMessage).catch(() => {});
    await waitForForkBoundary(transport.createThreadStarted.promise, "shutdown remote create");
    const forkAdmission = await waitForIngressJob(harness, forkMessage, {
      predicate: job => job.state === "accepted" || job.state === "dispatch-intent",
      rejectStates: [],
      description: "in-flight fork admission",
    });
    expect(forkAdmission.ackReady).toBe(true);
    const stopping = harness.service.stop();
    transport.releaseCreateThread.resolve();
    await stopping;
    await forkRequest;

    expect(transport.lastCreateSignal?.aborted).toBe(true);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([]);
    expect(transport.createAttempts).toBe(1);
    const record = await waitForUnknownRemoteCreate(harness);
    expect(record).toMatchObject({
      phase: "creating",
      error: expect.stringContaining("结果不明"),
    });
    const root = harness.root;
    const bot = harness.bot;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds });
    await harness.service.submitInbound(forkMessage).catch(() => {});
    const recoveredUnknown = await waitForUnknownRemoteCreate(harness);
    expect(recoveredUnknown).toMatchObject({
      phase: "creating",
      error: expect.stringContaining("结果不确定"),
    });
    expect(transport.createAttempts).toBe(1);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([]);
  } finally {
    transport.releaseCreateThread.resolve();
    workers.releaseForkStop.resolve();
    await forkRequest;
    await closeCoreHarness(harness);
  }
});

test("command name validation rejects overlong thread names before starting native preparation", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });

  try {
    await harness.service.submitInbound(incoming(harness, FORK_GROUP, "fork-name-source", "persist source"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
    const overlongMessage = incoming(harness, FORK_GROUP, "fork-name-overlong", `/fork ${"x".repeat(201)}`);
    const refusal = waitForNextTargetText(harness, FORK_GROUP);
    await harness.service.submitInbound(overlongMessage);
    await refusal;
    expect(workers.forkAttempts).toEqual([]);
    expect(transport.createAttempts).toBe(0);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(workers.workers[0]?.sessionId ?? null);
  } finally {
    await closeCoreHarness(harness);
  }
});
