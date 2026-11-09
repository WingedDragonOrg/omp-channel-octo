import { expect, test } from "bun:test";
import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import type { InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const WAIT_TIMEOUT_MS = 5_000;

type Target = { channelType: 1; channelId: string };

async function waitUntil(
  check: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = WAIT_TIMEOUT_MS,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await check()) return;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error(`did not observe ${label} within ${timeoutMs} ms`);
}

async function awaitBounded<T>(promise: Promise<T>, label: string): Promise<T> {
  let result:
    | { readonly kind: "fulfilled"; readonly value: T }
    | { readonly kind: "rejected"; readonly error: unknown }
    | undefined;
  void promise.then(
    value => { result = { kind: "fulfilled", value }; },
    error => { result = { kind: "rejected", error }; },
  );
  await waitUntil(() => result !== undefined, `settlement of ${label}`);
  if (result === undefined) throw new Error(`no result for ${label}`);
  if (result.kind === "rejected") throw result.error;
  return result.value;
}

function ingressJob(store: ChannelStore, harness: CoreHarness, message: InboundMessage) {
  return store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
  });
}

async function waitForSentText(
  harness: CoreHarness,
  target: Target,
  predicate: (text: string) => boolean,
  label: string,
): Promise<string> {
  let text: string | undefined;
  await waitUntil(() => {
    text = harness.transport.sent.find(message =>
      message.target.channelType === target.channelType
      && message.target.channelId === target.channelId
      && predicate(message.text))?.text;
    return text !== undefined;
  }, label);
  if (text === undefined) throw new Error(`missing ${label}`);
  return text;
}

async function bindInitialSession(
  harness: CoreHarness,
  store: ChannelStore,
  target: Target,
  cwd: string,
  messageId: string,
) {
  const message = inbound({ ...target, messageId, text: `/new ${cwd}` });
  const receipt = harness.service.receive(message);
  await awaitBounded(receipt.ackReady, "initial /new admission");
  await waitUntil(() => {
    const binding = store.getBinding(harness.bot.uid, target);
    return binding !== null && binding.sessionId !== null && binding.sessionFile !== null;
  }, "initial persisted session binding");
  const binding = store.getBinding(harness.bot.uid, target);
  if (binding === null || binding.sessionId === null || binding.sessionFile === null) {
    throw new Error("initial /new did not publish a complete session binding");
  }
  await waitForSentText(
    harness,
    target,
    text => text.includes("已创建会话") && text.includes(binding.sessionId!),
    "initial /new receipt",
  );
  return binding;
}

/** Injects one post-spawn worker failure through the real CoreService worker factory path. */
function failNextWorkerMethod(workers: FakeWorkerFactory, method: "prompt" | "refreshState"): void {
  const createWorker = workers.create.bind(workers);
  let intercepted = false;
  workers.create = async options => {
    const worker = await createWorker(options);
    if (intercepted) return worker;
    intercepted = true;
    return new Proxy(worker, {
      get(target, property) {
        if (property === method) {
          return async () => {
            throw new Error(`fixture ${method} failure after worker spawn`);
          };
        }
        return Reflect.get(target, property, target);
      },
    });
  };
}

async function makeUnknownMove(
  harness: CoreHarness,
  store: ChannelStore,
  target: Target,
  initialBinding: { readonly sessionFile: string | null },
  destination: string,
  messageId: string,
) {
  failNextWorkerMethod(harness.workers, "prompt");
  const message = inbound({ ...target, messageId, text: `/move ${destination}` });
  const receipt = harness.service.receive(message);
  await awaitBounded(receipt.ackReady, "unknown /move admission");
  await waitUntil(() => ingressJob(store, harness, message)?.state === "unknown", "unknown /move outcome");
  const notice = await waitForSentText(
    harness,
    target,
    text => text.includes("无法确认 OMP 原生 /move 是否完成") && text.includes("本次不会自动重试"),
    "unknown /move notice",
  );
  expect(notice).toContain("源与目标目录保持保护");
  const job = ingressJob(store, harness, message);
  if (job === null) throw new Error("unknown /move ingress was not persisted");
  expect(job).toMatchObject({ state: "unknown", ackReady: true });
  expect(job.operationCheckpoint).toMatchObject({ kind: "command", action: "move", phase: "dispatch-intent" });
  const ledgerId = `move-ingress-${job.id}`;
  expect(store.getWorkerLedger(harness.bot.uid, ledgerId)).toMatchObject({ state: "stopped" });
  expect(initialBinding.sessionFile).not.toBeNull();
  return { message, job };
}

test("a same-process /new recovers an unknown /new after an unknown /move without replaying either operation", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-review-same-process" };
  const sourceCwd = join(harness.root, "source-cwd");
  const unknownMoveCwd = join(harness.root, "unknown-move-cwd");
  const recoveryBCwd = join(harness.root, "recovery-b-cwd");
  const recoveryCCwd = join(harness.root, "recovery-c-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, recoveryBCwd, recoveryCCwd]
    .map(path => mkdir(path, { recursive: true })));

  const store = await openChannelStore(harness.stateDir);
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  try {
    const initial = await bindInitialSession(harness, store, target, sourceCwd, "review-chain-initial");
    const unknownA = await makeUnknownMove(
      harness,
      store,
      target,
      initial,
      unknownMoveCwd,
      "review-chain-unknown-a",
    );

    failNextWorkerMethod(workers, "refreshState");
    const recoveryB = inbound({
      ...target,
      messageId: "review-chain-unknown-b",
      text: `/new ${recoveryBCwd}`,
    });
    const recoveryBReceipt = harness.service.receive(recoveryB);
    await awaitBounded(recoveryBReceipt.ackReady, "recovery B /new admission");
    await waitUntil(() => ingressJob(store, harness, recoveryB)?.state === "unknown", "unknown recovery B outcome");
    await waitForSentText(
      harness,
      target,
      text => text.includes("无法确认新会话是否完整创建") && text.includes("本次不会自动重试"),
      "unknown recovery B notice",
    );
    const unknownB = ingressJob(store, harness, recoveryB);
    if (unknownB === null) throw new Error("unknown recovery B ingress was not persisted");
    expect(unknownB).toMatchObject({ state: "unknown", ackReady: true });
    expect(store.getWorkerLedger(harness.bot.uid, `new-ingress-${unknownB.id}`))
      .toMatchObject({ state: "stopped" });
    expect(workers.creations.filter(options => options.cwd === recoveryBCwd)).toHaveLength(1);

    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
    };
    const recoveryC = inbound({
      ...target,
      messageId: "review-chain-recovery-c",
      text: `/new ${recoveryCCwd}`,
    });
    const recoveryCReceipt = harness.service.receive(recoveryC);
    await awaitBounded(recoveryCReceipt.ackReady, "recovery C /new admission");
    await awaitBounded(candidateStarted.promise, "recovery C candidate creation");
    expect(ingressJob(store, harness, recoveryC)).toMatchObject({ state: "dispatch-intent", ackReady: true });
    expect(ingressJob(store, harness, recoveryB)).toMatchObject({ state: "unknown" });
    expect(store.getWorkerLedger(harness.bot.uid, `new-ingress-${unknownB.id}`))
      .toMatchObject({ state: "stopped" });
    expect(workers.creations.filter(options => options.cwd === recoveryBCwd)).toHaveLength(1);

    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    await waitUntil(() => {
      const current = store.getBinding(harness.bot.uid, target);
      return current !== null
        && current.sessionId !== null
        && current.sessionId !== initial.sessionId
        && current.cwd === recoveryCCwd;
    }, "recovery C binding");
    const recovered = store.getBinding(harness.bot.uid, target);
    if (recovered === null || recovered.sessionId === null || recovered.sessionFile === null) {
      throw new Error("recovery C did not publish a complete session binding");
    }
    await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId!),
      "recovery C receipt",
    );
    await waitUntil(() => ingressJob(store, harness, recoveryC)?.state === "handed-off", "recovery C handoff");

    const fresh = inbound({
      ...target,
      messageId: "review-chain-fresh-input",
      text: "continue only in recovery C's session",
    });
    const freshReceipt = harness.service.receive(fresh);
    await awaitBounded(freshReceipt.ackReady, "post-recovery input admission");
    await waitUntil(
      () => workers.prompts.some(prompt => prompt.text.includes(fresh.text)),
      "post-recovery input prompt",
    );
    const freshPrompt = workers.prompts.find(prompt => prompt.text.includes(fresh.text));
    expect(freshPrompt).toMatchObject({ sessionId: recovered.sessionId, cwd: recoveryCCwd });

    expect(ingressJob(store, harness, unknownA.message)).toMatchObject({ state: "unknown" });
    expect(ingressJob(store, harness, recoveryB)).toMatchObject({ state: "unknown" });
    expect(workers.prompts.some(prompt => prompt.text === unknownA.message.text)).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text === recoveryB.text)).toBe(false);
    expect(workers.creations.filter(options => options.sessionFile === initial.sessionFile)).toHaveLength(1);
    expect(workers.creations.filter(options => options.cwd === recoveryBCwd)).toHaveLength(1);
  } finally {
    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("a /move accepted during recovery /new runs on the newly recovered session", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-review-new-move" };
  const sourceCwd = join(harness.root, "source-cwd");
  const unknownMoveCwd = join(harness.root, "unknown-move-cwd");
  const recoveryCwd = join(harness.root, "recovered-cwd");
  const moveCwd = join(harness.root, "move-after-recovery-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, recoveryCwd, moveCwd]
    .map(path => mkdir(path, { recursive: true })));

  const store = await openChannelStore(harness.stateDir);
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  try {
    const initial = await bindInitialSession(harness, store, target, sourceCwd, "review-move-initial");
    const unknownA = await makeUnknownMove(
      harness,
      store,
      target,
      initial,
      unknownMoveCwd,
      "review-move-unknown-a",
    );

    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
    };
    const recovery = inbound({
      ...target,
      messageId: "review-move-recovery-new",
      text: `/new ${recoveryCwd}`,
    });
    const recoveryReceipt = harness.service.receive(recovery);
    await awaitBounded(recoveryReceipt.ackReady, "recovery /new admission");
    await awaitBounded(candidateStarted.promise, "recovery /new candidate creation");
    const recoveryJob = ingressJob(store, harness, recovery);
    if (recoveryJob === null) throw new Error("recovery /new ingress was not persisted");
    expect(recoveryJob).toMatchObject({ state: "dispatch-intent", ackReady: true });

    const move = inbound({
      ...target,
      messageId: "review-move-during-recovery",
      text: `/move ${moveCwd}`,
    });
    const moveReceipt = harness.service.receive(move);
    await awaitBounded(moveReceipt.ackReady, "during-recovery /move admission");
    await waitUntil(() => ingressJob(store, harness, move)?.state === "accepted", "accepted during-recovery /move");
    const acceptedMove = ingressJob(store, harness, move);
    expect(acceptedMove).toMatchObject({ state: "accepted", ackReady: true });
    expect(workers.prompts.some(prompt => prompt.text === move.text)).toBe(false);

    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    await waitUntil(() => {
      const current = store.getBinding(harness.bot.uid, target);
      return current !== null
        && current.sessionId !== null
        && current.sessionId !== initial.sessionId
        && current.cwd === recoveryCwd;
    }, "recovered /new binding before /move");
    const recovered = store.getBinding(harness.bot.uid, target);
    if (recovered === null || recovered.sessionId === null || recovered.sessionFile === null) {
      throw new Error("recovery /new did not publish a complete session binding");
    }
    await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId!),
      "recovery /new receipt",
    );

    await waitUntil(() => {
      const job = ingressJob(store, harness, move);
      return job?.state === "handed-off" || job?.state === "unknown";
    }, "during-recovery /move outcome");
    expect(ingressJob(store, harness, move)).toMatchObject({ state: "handed-off" });
    await waitUntil(
      () => workers.prompts.some(prompt => prompt.text === move.text),
      "native /move prompt on the recovered session",
    );
    const movePrompt = workers.prompts.find(prompt => prompt.text === move.text);
    expect(movePrompt).toMatchObject({ sessionId: recovered.sessionId, cwd: recoveryCwd });

    const moved = store.getBinding(harness.bot.uid, target);
    expect(moved).toMatchObject({
      sessionId: recovered.sessionId,
      sessionFile: recovered.sessionFile,
      cwd: moveCwd,
    });
    expect(workers.creations.filter(options => options.sessionFile === recovered.sessionFile)).toHaveLength(1);
    expect(workers.prompts.filter(prompt => prompt.text === move.text)).toHaveLength(1);
    expect(workers.prompts.some(prompt => prompt.text === unknownA.message.text)).toBe(false);
    expect(workers.creations.filter(options => options.sessionFile === initial.sessionFile)).toHaveLength(1);
  } finally {
    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("recovery cancels a waiting /move after unknown /new and preserves later input", async () => {
  const workers = new FakeWorkerFactory();
  const first = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-review-waiting-move" };
  const sourceCwd = join(first.root, "source-cwd");
  const unknownMoveCwd = join(first.root, "unknown-move-cwd");
  const recoveryBCwd = join(first.root, "recovery-b-cwd");
  const recoveryCCwd = join(first.root, "recovery-c-cwd");
  const waitingMoveCwd = join(first.root, "waiting-move-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, recoveryBCwd, recoveryCCwd, waitingMoveCwd]
    .map(path => mkdir(path, { recursive: true })));

  let store: ChannelStore | undefined = await openChannelStore(first.stateDir);
  let restartedStore: ChannelStore | undefined;
  let restarted: CoreHarness | undefined;
  let firstStopped = false;
  const dispatchGate = Promise.withResolvers<void>();
  try {
    const activeStore = store;
    if (activeStore === undefined) throw new Error("initial store is unavailable");
    const initial = await bindInitialSession(first, activeStore, target, sourceCwd, "review-waiting-move-initial");
    const unknownA = await makeUnknownMove(
      first,
      activeStore,
      target,
      initial,
      unknownMoveCwd,
      "review-waiting-move-unknown-a",
    );

    failNextWorkerMethod(workers, "refreshState");
    if (!Reflect.set(first.service, "startupGate", dispatchGate)) {
      throw new Error("could not install deterministic pre-dispatch gate");
    }
    const recoveryB = inbound({
      ...target,
      messageId: "review-waiting-move-unknown-b",
      text: `/new ${recoveryBCwd}`,
    });
    const recoveryBReceipt = first.service.receive(recoveryB);
    await awaitBounded(recoveryBReceipt.ackReady, "recovery B /new admission");
    await waitUntil(
      () => ingressJob(activeStore, first, recoveryB)?.state === "accepted",
      "accepted recovery B before worker dispatch",
    );
    const creationsBeforeBDispatch = workers.creations.length;
    const waitingMove = inbound({
      ...target,
      messageId: "review-waiting-move-m",
      text: `/move ${waitingMoveCwd}`,
    });
    const waitingMoveReceipt = first.service.receive(waitingMove);
    await awaitBounded(waitingMoveReceipt.ackReady, "waiting /move admission");
    await waitUntil(
      () => ingressJob(activeStore, first, waitingMove)?.state === "accepted",
      "waiting /move durable acceptance",
    );
    expect(workers.creations).toHaveLength(creationsBeforeBDispatch);
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);

    dispatchGate.resolve();
    await waitUntil(
      () => ingressJob(activeStore, first, recoveryB)?.state === "unknown",
      "unknown recovery B outcome",
    );
    await waitForSentText(
      first,
      target,
      text => text.includes("无法确认新会话是否完整创建") && text.includes("本次不会自动重试"),
      "unknown recovery B notice",
    );
    const unknownB = ingressJob(activeStore, first, recoveryB);
    if (unknownB === null) throw new Error("unknown recovery B ingress was not persisted");
    expect(activeStore.getWorkerLedger(first.bot.uid, `new-ingress-${unknownB.id}`))
      .toMatchObject({ state: "stopped" });
    expect(ingressJob(activeStore, first, waitingMove)).toMatchObject({ state: "accepted" });
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);

    activeStore.close();
    store = undefined;
    await awaitBounded(first.service.stop(), "first service shutdown");
    firstStopped = true;

    restarted = await createCoreHarness({ root: first.root, bot: first.bot, workers });
    restartedStore = await openChannelStore(restarted.stateDir);
    const recoveredStore = restartedStore;
    if (recoveredStore === undefined) throw new Error("restarted store is unavailable");
    const recoveryC = inbound({
      ...target,
      messageId: "review-waiting-move-recovery-c",
      text: `/new ${recoveryCCwd}`,
    });
    const recoveryCReceipt = restarted.service.receive(recoveryC);
    await awaitBounded(recoveryCReceipt.ackReady, "recovery C /new admission");
    await waitUntil(() => {
      const current = recoveredStore.getBinding(restarted!.bot.uid, target);
      return current !== null
        && current.sessionId !== null
        && current.sessionId !== initial.sessionId
        && current.cwd === recoveryCCwd;
    }, "recovery C binding");
    const recovered = recoveredStore.getBinding(restarted.bot.uid, target);
    if (recovered === null || recovered.sessionId === null || recovered.sessionFile === null) {
      throw new Error("recovery C did not publish a complete session binding");
    }
    await waitForSentText(
      restarted,
      target,
      text => text.includes("已创建会话") && text.includes(recovered.sessionId!),
      "recovery C receipt",
    );
    await waitUntil(
      () => ingressJob(recoveredStore, restarted!, recoveryC)?.state === "handed-off",
      "recovery C handoff",
    );

    const moveNotice = await waitForSentText(
      restarted,
      target,
      text => text.includes(waitingMove.messageId) && text.includes("未执行"),
      "not-executed notice for waiting /move",
    );
    expect(moveNotice).toContain("忽略");
    expect(moveNotice).toMatch(/重新发送|重发/u);
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);

    const fresh = inbound({
      ...target,
      messageId: "review-waiting-move-fresh-input",
      text: "continue only after recovery C",
    });
    const freshReceipt = restarted.service.receive(fresh);
    await awaitBounded(freshReceipt.ackReady, "post-recovery input admission");
    await waitUntil(
      () => workers.prompts.some(prompt => prompt.text.includes(fresh.text)),
      "post-recovery prompt",
    );
    const freshPrompt = workers.prompts.find(prompt => prompt.text.includes(fresh.text));
    expect(freshPrompt).toMatchObject({ sessionId: recovered.sessionId, cwd: recoveryCCwd });

    expect(recoveredStore.getWorkerLedger(first.bot.uid, `new-ingress-${unknownB.id}`))
      .toMatchObject({ state: "stopped" });
    expect(workers.prompts.some(prompt => prompt.text === unknownA.message.text)).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text === recoveryB.text)).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text === waitingMove.text)).toBe(false);
    expect(workers.creations.filter(options => options.sessionFile === initial.sessionFile)).toHaveLength(1);
    expect(workers.creations.filter(options => options.cwd === recoveryBCwd)).toHaveLength(1);
  } finally {
    dispatchGate.resolve();
    store?.close();
    restartedStore?.close();
    if (!firstStopped) await first.service.stop().catch(() => {});
    if (restarted !== undefined) {
      await closeCoreHarness(restarted);
    } else if (firstStopped) {
      await rm(first.root, { recursive: true, force: true });
    }
  }
}, 25_000);
