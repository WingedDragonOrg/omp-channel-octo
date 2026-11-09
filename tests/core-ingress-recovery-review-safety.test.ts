import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId, type IngressJobRecord } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";
import { ForkTestTransport, ForkTestWorkerFactory } from "./fixtures/core-fork.ts";

const WAIT_TIMEOUT_MS = 4_000;
const GROUP: ChatTarget & { channelType: 2 } = {
  channelType: 2,
  channelId: "group-ingress-recovery-review-safety",
};

type SessionTarget = ChatTarget & { channelType: 1 | 2 };

async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + WAIT_TIMEOUT_MS;
  while (performance.now() < deadline) {
    if (await check()) return;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
  }
  throw new Error(`timed out waiting for ${label} after ${WAIT_TIMEOUT_MS} ms`);
}

async function awaitWithin<T>(promise: Promise<T>, label: string): Promise<T> {
  let result:
    | { readonly kind: "fulfilled"; readonly value: T }
    | { readonly kind: "rejected"; readonly error: unknown }
    | undefined;
  void promise.then(
    value => { result = { kind: "fulfilled", value }; },
    error => { result = { kind: "rejected", error }; },
  );
  await waitUntil(() => result !== undefined, label);
  if (result === undefined) throw new Error(`missing result after ${label}`);
  if (result.kind === "rejected") throw result.error;
  return result.value;
}

function messageFor(
  harness: Pick<CoreHarness, "bot">,
  target: SessionTarget,
  messageId: string,
  text: string,
): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [harness.bot.uid],
  });
}

function ingressJob(store: ChannelStore, harness: CoreHarness, message: InboundMessage): IngressJobRecord | null {
  return store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
  });
}

async function waitForIngressJob(
  store: ChannelStore,
  harness: CoreHarness,
  message: InboundMessage,
  predicate: (job: IngressJobRecord) => boolean,
  label: string,
): Promise<IngressJobRecord> {
  let latest: IngressJobRecord | null = null;
  await waitUntil(() => {
    latest = ingressJob(store, harness, message);
    return latest !== null && predicate(latest);
  }, label);
  if (latest === null) throw new Error(`ingress job missing after ${label}`);
  return latest;
}

async function waitForSentText(
  harness: CoreHarness,
  target: ChatTarget,
  startIndex: number,
  predicate: (text: string) => boolean,
  label: string,
): Promise<string> {
  let result: string | undefined;
  await waitUntil(() => {
    result = harness.transport.sent.slice(startIndex).find(sent =>
      sent.target.channelType === target.channelType
      && sent.target.channelId === target.channelId
      && predicate(sent.text))?.text;
    return result !== undefined;
  }, label);
  if (result === undefined) throw new Error(`missing sent text after ${label}`);
  return result;
}

async function bindInitialSession(
  harness: CoreHarness,
  store: ChannelStore,
  target: SessionTarget,
  cwd: string,
  messageId: string,
) {
  const message = messageFor(harness, target, messageId, `/new ${cwd}`);
  const outputStart = harness.transport.sent.length;
  await awaitWithin(harness.service.receive(message).ackReady, "initial /new admission");
  await waitUntil(() => {
    const binding = store.getBinding(harness.bot.uid, target);
    return binding !== null && binding.sessionId !== null;
  }, "initial session binding");
  const binding = store.getBinding(harness.bot.uid, target);
  if (binding?.sessionFile === null || binding === null) {
    throw new Error("initial /new did not publish a session binding");
  }
  await waitForSentText(
    harness,
    target,
    outputStart,
    text => text.includes("已创建会话") && text.includes(binding.sessionId),
    "initial /new receipt",
  );
  return binding;
}

function failNextPromptBehindGate(workers: FakeWorkerFactory) {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const createWorker = workers.create.bind(workers);
  let intercepted = false;
  workers.create = async options => {
    const worker = await createWorker(options);
    if (intercepted) return worker;
    intercepted = true;
    return new Proxy(worker, {
      get(target, property) {
        if (property === "prompt") {
          return async () => {
            promptStarted.resolve();
            await releasePrompt.promise;
            throw new Error("fixture native /move prompt result is unknown");
          };
        }
        return Reflect.get(target, property, target);
      },
    });
  };
  return { promptStarted: promptStarted.promise, releasePrompt };
}

async function makeUnknownMove(
  harness: CoreHarness,
  store: ChannelStore,
  target: SessionTarget,
  destination: string,
  messageId: string,
  afterPrompt: () => Promise<void> = async () => {},
) {
  const promptGate = failNextPromptBehindGate(harness.workers);
  const message = messageFor(harness, target, messageId, `/move ${destination}`);
  const outputStart = harness.transport.sent.length;
  await awaitWithin(harness.service.receive(message).ackReady, "unknown /move admission");
  await awaitWithin(promptGate.promptStarted, "unknown /move prompt dispatch");
  promptGate.releasePrompt.resolve();
  await afterPrompt();
  const job = await waitForIngressJob(
    store,
    harness,
    message,
    current => current.state === "unknown",
    "unknown /move outcome",
  );
  await waitForSentText(
    harness,
    target,
    outputStart,
    text => text.includes("无法确认 OMP 原生 /move 是否完成"),
    "unknown /move notice",
  );
  return { job, message };
}

async function waitForReplacementBinding(
  store: ChannelStore,
  harness: CoreHarness,
  target: SessionTarget,
  previousSessionId: string,
  cwd: string,
) {
  let binding = store.getBinding(harness.bot.uid, target);
  await waitUntil(() => {
    binding = store.getBinding(harness.bot.uid, target);
    return binding !== null && binding.sessionId !== previousSessionId && binding.cwd === cwd;
  }, "recovery /new binding");
  if (binding?.sessionFile === null || binding === null) {
    throw new Error("recovery /new did not publish a session file");
  }
  return binding;
}

function expectNotExecutedNotice(text: string): void {
  expect(text).toMatch(/(?:未|没有|尚未).{0,12}(?:执行|派发)/u);
  expect(text).toMatch(/重新发送|重发/u);
  expect(text).toContain("忽略");
}

test("an unknown move keeps its barrier until explicit recovery after an unconfirmed stop", async () => {
  const workers = new FakeWorkerFactory();
  const first = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-recovery-unconfirmed-stop-safety" };
  const oldCwd = join(first.root, "old-session-cwd");
  const moveCwd = join(first.root, "unconfirmed-move-cwd");
  const recoveryCwd = join(first.root, "blocked-recovery-cwd");
  await Promise.all([oldCwd, moveCwd, recoveryCwd].map(path => mkdir(path, { recursive: true })));
  const firstStore = await openChannelStore(first.stateDir);
  const stopStarted = Promise.withResolvers<void>();
  const releaseStop = Promise.withResolvers<void>();
  let firstStoreClosed = false;
  let restarted: CoreHarness | undefined;
  let restartedStore: ChannelStore | undefined;

  try {
    const original = await bindInitialSession(first, firstStore, target, oldCwd, "safety-stop-initial");
    if (original.sessionFile === null) throw new Error("initial session file is missing");
    workers.stopHandler = async () => {
      stopStarted.resolve();
      await releaseStop.promise;
      throw new Error("fixture could not confirm the old worker process-group exit");
    };
    const { job: unknownMove } = await makeUnknownMove(
      first,
      firstStore,
      target,
      moveCwd,
      "safety-stop-unknown-move",
      async () => {
        await awaitWithin(stopStarted.promise, "old worker stop attempt");
        releaseStop.resolve();
      },
    );
    const workerLedgerId = `move-ingress-${unknownMove.id}`;
    expect(firstStore.getWorkerLedger(first.bot.uid, workerLedgerId)).toMatchObject({
      id: workerLedgerId,
      state: "running",
      sessionId: original.sessionId,
      sessionFile: original.sessionFile,
      cwd: oldCwd,
      moveTargetCwd: moveCwd,
    });
    expect(workers.workers.at(-1)?.sessionId).toBe(original.sessionId);
    expect(workers.workers.at(-1)?.stopped).toBe(false);

    const recoveryMessage = messageFor(first, target, "safety-stop-rejected-new", `/new ${recoveryCwd}`);
    const refusalStart = first.transport.sent.length;
    await awaitWithin(first.service.receive(recoveryMessage).ackReady, "recovery /new admission");
    const refusal = await waitForSentText(
      first,
      target,
      refusalStart,
      text => text.includes("尚未确认停止") && text.includes("重试 /new"),
      "recovery /new refusal while unknown A's worker remains live",
    );
    expect(refusal).not.toContain("已创建会话");
    expect(workers.creations).toHaveLength(2);

    const ordinary = messageFor(
      first,
      target,
      "safety-stop-ordinary-after-refusal",
      "do not run this in the old session",
    );
    await awaitWithin(first.service.receive(ordinary).ackReady, "ordinary input after recovery refusal");
    for (let turn = 0; turn < 8; turn += 1) {
      const nextTurn = Promise.withResolvers<void>();
      setImmediate(nextTurn.resolve);
      await nextTurn.promise;
    }
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);
    expect(workers.creations).toHaveLength(2);
    expect(await first.service.status(target)).toMatchObject({
      sessionId: original.sessionId,
      cwd: oldCwd,
    });

    firstStore.close();
    firstStoreClosed = true;
    await awaitWithin(first.service.stop(), "stopping before worker recovery");
    const shutdownStore = await openChannelStore(first.stateDir);
    try {
      expect(shutdownStore.getWorkerLedger(first.bot.uid, workerLedgerId)).toMatchObject({
        state: "running",
        sessionId: original.sessionId,
        sessionFile: original.sessionFile,
        cwd: oldCwd,
      });
    } finally {
      shutdownStore.close();
    }
    workers.stopHandler = undefined;
    workers.recovered = true;
    const activeHarness = await createCoreHarness({ root: first.root, bot: first.bot, workers });
    restarted = activeHarness;
    const activeStore = await openChannelStore(activeHarness.stateDir);
    restartedStore = activeStore;
    await waitUntil(
      () => activeStore.getWorkerLedger(first.bot.uid, workerLedgerId)?.state === "stopped",
      "startup recovery confirming unknown A's exact worker ledger stopped",
    );
    expect(workers.creations).toHaveLength(2);
    for (let turn = 0; turn < 8; turn += 1) {
      const nextTurn = Promise.withResolvers<void>();
      setImmediate(nextTurn.resolve);
      await nextTurn.promise;
    }
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);
    expect(await activeHarness.service.status(target)).toMatchObject({
      sessionId: original.sessionId,
      cwd: oldCwd,
    });

    const recoveryOutputStart = activeHarness.transport.sent.length;
    const explicitRecovery = messageFor(
      activeHarness,
      target,
      "safety-stop-explicit-recovery-after-confirmation",
      `/new ${recoveryCwd}`,
    );
    await awaitWithin(activeHarness.service.receive(explicitRecovery).ackReady, "confirmed recovery /new admission");
    const recovered = await waitForReplacementBinding(
      activeStore,
      activeHarness,
      target,
      original.sessionId,
      recoveryCwd,
    );
    await waitForSentText(
      activeHarness,
      target,
      recoveryOutputStart,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId),
      "successful explicit recovery /new receipt",
    );
    const notExecuted = await waitForSentText(
      activeHarness,
      target,
      recoveryOutputStart,
      text => text.includes(ordinary.messageId) && /未执行|未派发/u.test(text),
      "not-executed notice for the pre-recovery ordinary message",
    );
    expectNotExecutedNotice(notExecuted);
    expect(activeHarness.transport.sent.filter(sent =>
      sent.target.channelType === target.channelType
      && sent.target.channelId === target.channelId
      && sent.text.includes(ordinary.messageId))).toHaveLength(1);
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);
    expect(workers.creations).toHaveLength(3);
    expect(await activeHarness.service.status(target)).toMatchObject({
      sessionId: recovered.sessionId,
      cwd: recoveryCwd,
    });
  } finally {
    releaseStop.resolve();
    if (!firstStoreClosed) firstStore.close();
    restartedStore?.close();
    if (restarted !== undefined) await closeCoreHarness(restarted);
    else await closeCoreHarness(first);
  }
}, 20_000);

test("ordinary input queued after waiting /move is cancelled, not executed, by explicit recovery /new", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-recovery-queued-input-safety" };
  const sourceCwd = join(harness.root, "source-cwd");
  const unknownMoveCwd = join(harness.root, "unknown-move-cwd");
  const waitingMoveCwd = join(harness.root, "waiting-move-cwd");
  const recoveryCwd = join(harness.root, "recovery-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, waitingMoveCwd, recoveryCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const releaseRecoveryCandidate = Promise.withResolvers<void>();

  try {
    const original = await bindInitialSession(harness, store, target, sourceCwd, "safety-queued-initial");
    await makeUnknownMove(
      harness,
      store,
      target,
      unknownMoveCwd,
      "safety-queued-unknown-a",
    );
    const waitingMove = messageFor(harness, target, "safety-queued-move-b", `/move ${waitingMoveCwd}`);
    await awaitWithin(harness.service.receive(waitingMove).ackReady, "waiting /move B admission");

    const ordinary = messageFor(harness, target, "safety-queued-ordinary-c", "ordinary C must not run before recovery");
    await awaitWithin(harness.service.receive(ordinary).ackReady, "ordinary C admission");
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);

    const recoveryStarted = Promise.withResolvers<void>();
    workers.beforeCreate = async () => {
      recoveryStarted.resolve();
      await releaseRecoveryCandidate.promise;
    };
    const recoveryMessage = messageFor(harness, target, "safety-queued-recovery-new", `/new ${recoveryCwd}`);
    const recoveryOutputStart = harness.transport.sent.length;
    await awaitWithin(harness.service.receive(recoveryMessage).ackReady, "explicit recovery /new admission");
    await awaitWithin(recoveryStarted.promise, "recovery candidate creation past waiting /move B");
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);

    releaseRecoveryCandidate.resolve();
    workers.beforeCreate = undefined;
    const recovered = await waitForReplacementBinding(store, harness, target, original.sessionId, recoveryCwd);
    await waitForSentText(
      harness,
      target,
      recoveryOutputStart,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId),
      "explicit recovery /new receipt",
    );
    const ordinaryNotice = await waitForSentText(
      harness,
      target,
      0,
      text => text.includes(ordinary.messageId) && /未执行|未派发/u.test(text),
      "identity-specific not-executed notice for ordinary C",
    );
    expectNotExecutedNotice(ordinaryNotice);
    expect(harness.transport.sent.filter(sent =>
      sent.target.channelType === target.channelType
      && sent.target.channelId === target.channelId
      && sent.text.includes(ordinary.messageId))).toHaveLength(1);
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinary.text))).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);
    expect(workers.creations).toHaveLength(3);
    expect(store.getBinding(harness.bot.uid, target)).toMatchObject({
      sessionId: recovered.sessionId,
      cwd: recoveryCwd,
    });
  } finally {
    releaseRecoveryCandidate.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("recovery /new bypasses a waiting fork behind an unknown session transition", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [GROUP.channelId],
  });
  const sourceCwd = join(harness.root, "fork-source-cwd");
  const unknownMoveCwd = join(harness.root, "fork-unknown-move-cwd");
  const recoveryCwd = join(harness.root, "fork-recovery-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, recoveryCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const releaseCandidate = Promise.withResolvers<void>();

  try {
    const original = await bindInitialSession(harness, store, GROUP, sourceCwd, "safety-fork-initial");
    await makeUnknownMove(
      harness,
      store,
      GROUP,
      unknownMoveCwd,
      "safety-fork-unknown-a",
    );

    const forkMessage = messageFor(harness, GROUP, "safety-waiting-fork", "/fork review-after-recovery");
    await awaitWithin(harness.service.receive(forkMessage).ackReady, "waiting fork admission");
    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);

    const recoveryCandidateStarted = Promise.withResolvers<void>();
    workers.beforeCreate = async () => {
      const latest = workers.creations.at(-1);
      if (latest !== undefined && Reflect.get(latest, "forkFrom") !== undefined) {
        await releaseCandidate.promise;
        return;
      }
      recoveryCandidateStarted.resolve();
      await releaseCandidate.promise;
    };
    const recoveryMessage = messageFor(harness, GROUP, "safety-fork-recovery-new", `/new ${recoveryCwd}`);
    const recoveryOutputStart = transport.sent.length;
    await awaitWithin(harness.service.receive(recoveryMessage).ackReady, "recovery /new admission after waiting fork");
    await awaitWithin(recoveryCandidateStarted.promise, "recovery candidate past the waiting fork barrier");
    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);

    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    const recovered = await waitForReplacementBinding(store, harness, GROUP, original.sessionId, recoveryCwd);
    await waitForSentText(
      harness,
      GROUP,
      recoveryOutputStart,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId),
      "recovery /new receipt despite waiting fork",
    );
    const forkNotice = await waitForSentText(
      harness,
      GROUP,
      0,
      text => text.includes(forkMessage.messageId) && /未执行|未派发/u.test(text),
      "not-executed notice for the superseded waiting fork",
    );
    expectNotExecutedNotice(forkNotice);
    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);
    expect(store.getBinding(harness.bot.uid, GROUP)).toMatchObject({
      sessionId: recovered.sessionId,
      cwd: recoveryCwd,
    });
  } finally {
    releaseCandidate.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("an unknown fork survives restart without repeating remote create or blocking source /new", async () => {
  const workers = new ForkTestWorkerFactory();
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const allowedGroupIds = [GROUP.channelId];
  let harness = await createCoreHarness({ transport, workers, allowedGroupIds });
  const sourceCwd = join(harness.root, "unknown-fork-source-cwd");
  const recoveryCwd = join(harness.root, "unknown-fork-independent-new-cwd");
  await Promise.all([sourceCwd, recoveryCwd].map(path => mkdir(path, { recursive: true })));
  const firstStore = await openChannelStore(harness.stateDir);
  const receiptStarted = Promise.withResolvers<void>();
  const releaseReceipt = Promise.withResolvers<void>();
  const originalSendText = transport.sendText.bind(transport);
  let firstStoreClosed = false;
  let restartedStore: ChannelStore | undefined;
  let unknownReceiptHeld = false;
  transport.afterCreateFailure = new Error("fixture timed out after remote thread creation");
  transport.sendText = async (
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ) => {
    if (!unknownReceiptHeld && text.includes("远端子区创建结果不明")) {
      unknownReceiptHeld = true;
      receiptStarted.resolve();
      await releaseReceipt.promise;
    }
    return originalSendText(target, text, clientMsgNo, signal);
  };

  try {
    const source = await bindInitialSession(harness, firstStore, GROUP, sourceCwd, "safety-unknown-fork-source");
    if (source.sessionFile === null) throw new Error("source session file is missing");

    const forkMessage = messageFor(
      harness,
      GROUP,
      "safety-unknown-fork-dispatch-intent",
      "/fork remote uncertainty",
    );
    await awaitWithin(harness.service.receive(forkMessage).ackReady, "unknown fork admission");
    const forkJob = await waitForIngressJob(
      firstStore,
      harness,
      forkMessage,
      job => job.state === "dispatch-intent",
      "fork ingress dispatch-intent before remote allocation",
    );
    expect(forkJob.operationCheckpoint).toMatchObject({
      kind: "command",
      action: "fork",
      phase: "dispatch-intent",
    });
    await awaitWithin(transport.createThreadStarted.promise, "remote fork allocation gate");

    transport.releaseCreateThread.resolve();
    await awaitWithin(receiptStarted.promise, "unknown fork receipt held before ingress handoff");
    const unknownForkResult: { value: ReturnType<ChannelStore["getThreadFork"]> } = { value: null };
    await waitUntil(() => {
      unknownForkResult.value = firstStore.listThreadForks(harness.bot.uid).find(record =>
        record.sourceSessionId === source.sessionId
        && record.phase === "creating"
        && record.error !== null) ?? null;
      return unknownForkResult.value !== null;
    }, "durable unknown remote fork outcome");
    const unknownFork = unknownForkResult.value;
    if (unknownFork === null) throw new Error("unknown fork ledger was not persisted");
    expect(unknownFork).toMatchObject({
      sourceSessionId: source.sessionId,
      sourceSessionFile: source.sessionFile,
      phase: "creating",
      error: expect.stringContaining("结果不明"),
    });
    expect(firstStore.getIngressJob(forkJob.id)).toMatchObject({
      state: "dispatch-intent",
      operationCheckpoint: { kind: "command", action: "fork", phase: "dispatch-intent" },
    });
    expect(firstStore.markIngressUnknown(forkJob.id)).toBe(true);
    expect(firstStore.getIngressJob(forkJob.id)).toMatchObject({
      state: "unknown",
      operationCheckpoint: { kind: "command", action: "fork", phase: "dispatch-intent" },
    });
    expect(transport.createAttempts).toBe(1);
    expect((await transport.listThreads(GROUP.channelId)).map(thread => thread.channelId))
      .toEqual([`${GROUP.channelId}____thread-1`]);

    const root = harness.root;
    const bot = harness.bot;
    const stopping = harness.service.stop();
    releaseReceipt.resolve();
    await awaitWithin(stopping, "stopping with fork outcome unknown before ingress handoff");
    expect(firstStore.getThreadFork(bot.uid, unknownFork.id)).toMatchObject({
      sourceSessionId: source.sessionId,
      phase: "creating",
    });
    firstStore.close();
    firstStoreClosed = true;
    await closeCoreHarness(harness, { remove: false });

    harness = await createCoreHarness({ root, bot, transport, workers, allowedGroupIds });
    const activeStore = await openChannelStore(harness.stateDir);
    restartedStore = activeStore;
    await waitUntil(
      () => activeStore.getIngressJob(forkJob.id)?.state === "unknown",
      "restored unknown fork ingress record",
    );
    const restoredForkJob = activeStore.getIngressJob(forkJob.id);
    expect(restoredForkJob).toMatchObject({
      state: "unknown",
      operationCheckpoint: { kind: "command", action: "fork", phase: "dispatch-intent" },
    });
    const retainedFork = activeStore.getThreadFork(bot.uid, unknownFork.id);
    expect(retainedFork).toMatchObject({
      sourceSessionId: source.sessionId,
      sourceSessionFile: source.sessionFile,
      phase: "creating",
      error: expect.stringContaining("不会自动重试创建"),
    });
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);

    await awaitWithin(
      harness.service.submitInbound(forkMessage).catch(() => {}),
      "duplicate delivery of the unknown fork",
    );
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);

    const recoveryOutputStart = harness.transport.sent.length;
    const independentNew = messageFor(
      harness,
      GROUP,
      "safety-unknown-fork-independent-new",
      `/new ${recoveryCwd}`,
    );
    await awaitWithin(harness.service.receive(independentNew).ackReady, "independent /new admission after fork restart");
    const recovered = await waitForReplacementBinding(
      activeStore,
      harness,
      GROUP,
      source.sessionId,
      recoveryCwd,
    );
    await waitForSentText(
      harness,
      GROUP,
      recoveryOutputStart,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId),
      "independent source /new receipt after unknown fork restart",
    );

    expect(activeStore.getIngressJob(forkJob.id)?.state).toBe("unknown");
    expect(activeStore.getThreadFork(bot.uid, unknownFork.id)).toMatchObject({
      sourceSessionId: source.sessionId,
      phase: "creating",
      error: expect.stringContaining("不会自动重试创建"),
    });
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect((await transport.listThreads(GROUP.channelId)).map(thread => thread.channelId))
      .toEqual([`${GROUP.channelId}____thread-1`]);
    expect(activeStore.getBinding(bot.uid, GROUP)).toMatchObject({
      sessionId: recovered.sessionId,
      cwd: recoveryCwd,
    });
  } finally {
    releaseReceipt.resolve();
    transport.releaseCreateThread.resolve();
    if (!firstStoreClosed) firstStore.close();
    restartedStore?.close();
    await closeCoreHarness(harness);
  }
}, 20_000);
