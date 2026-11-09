import { expect, test } from "bun:test";
import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import type { InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
  }
  throw new Error(`did not observe ${label}`);
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

function ingressJob(
  store: ChannelStore,
  harness: CoreHarness,
  message: InboundMessage,
) {
  return store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
  });
}

function sentText(
  harness: CoreHarness,
  target: { channelType: number; channelId: string },
  predicate: (text: string) => boolean,
): string | undefined {
  return harness.transport.sent.find(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && predicate(message.text))?.text;
}

async function waitForSentText(
  harness: CoreHarness,
  target: { channelType: number; channelId: string },
  predicate: (text: string) => boolean,
  label: string,
): Promise<string> {
  let text: string | undefined;
  await waitUntil(() => {
    text = sentText(harness, target, predicate);
    return text !== undefined;
  }, label);
  return text!;
}

async function bindInitialSession(
  harness: CoreHarness,
  store: ChannelStore,
  target: { channelType: 1; channelId: string },
  cwd: string,
  messageId: string,
) {
  const message = inbound({ ...target, messageId, text: `/new ${cwd}` });
  const receipt = harness.service.receive(message);
  await awaitBounded(receipt.ackReady, "initial /new admission");
  await waitUntil(
    () => {
      const binding = store.getBinding(harness.bot.uid, target);
      return binding !== null && binding.sessionId !== null;
    },
    "initial /new binding",
  );
  const binding = store.getBinding(harness.bot.uid, target);
  if (binding?.sessionFile === null || binding === null) {
    throw new Error("initial /new did not publish a persisted session binding");
  }
  await waitForSentText(
    harness,
    target,
    text => text.includes("已创建会话") && text.includes(binding.sessionId),
    "initial /new receipt",
  );
  return binding;
}

/** Fail the next real fixture worker operation after its spawn has been recorded in SQLite. */
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
  target: { channelType: 1; channelId: string },
  initialBinding: { sessionFile: string | null },
  destination: string,
  messageId: string,
) {
  failNextWorkerMethod(harness.workers, "prompt");
  const message = inbound({ ...target, messageId, text: `/move ${destination}` });
  const receipt = harness.service.receive(message);
  await awaitBounded(receipt.ackReady, "unknown /move admission");
  const userReceipt = await waitForSentText(
    harness,
    target,
    text => text.includes("无法确认 OMP 原生 /move 是否完成") && text.includes("本次不会自动重试"),
    "unknown /move receipt",
  );
  const job = ingressJob(store, harness, message);
  if (job === null) throw new Error("unknown /move ingress job was not persisted");
  expect(job).toMatchObject({ state: "unknown", ackReady: true });
  expect(job.operationCheckpoint).toMatchObject({ kind: "command", action: "move", phase: "dispatch-intent" });
  expect(userReceipt).toContain("源与目标目录保持保护");
  const ledgerId = `move-ingress-${job.id}`;
  expect(job.operationCheckpoint?.kind === "command" ? job.operationCheckpoint.workerLedgerId : null).toBe(ledgerId);
  expect(store.getWorkerLedger(harness.bot.uid, ledgerId)).toMatchObject({ state: "stopped" });
  expect(initialBinding.sessionFile).not.toBeNull();
  return { message, job };
}

test("a blocked /move does not deadlock explicit recovery, and input arriving during recovery uses the new binding", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-recovery-chain-live" };
  const sourceCwd = join(harness.root, "source-cwd");
  const moveAPath = join(harness.root, "unknown-move-a");
  const moveBPath = join(harness.root, "blocked-move-b");
  const recoveryCwd = join(harness.root, "recovered-cwd");
  await Promise.all([sourceCwd, moveAPath, moveBPath, recoveryCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const releaseCandidate = Promise.withResolvers<void>();
  try {
    const original = await bindInitialSession(harness, store, target, sourceCwd, "recovery-chain-initial");
    const { job: unknownA } = await makeUnknownMove(
      harness,
      store,
      target,
      original,
      moveAPath,
      "recovery-chain-unknown-a",
    );

    const blockedMove = inbound({
      ...target,
      messageId: "recovery-chain-blocked-move-b",
      text: `/move ${moveBPath}`,
    });
    const blockedReceipt = harness.service.receive(blockedMove);
    await awaitBounded(blockedReceipt.ackReady, "pre-recovery /move admission");
    await waitUntil(() => ingressJob(store, harness, blockedMove)?.state === "accepted", "pre-recovery /move acceptance");
    const blockedJob = ingressJob(store, harness, blockedMove);
    expect(blockedJob?.routeSnapshot?.barrierJobId).toBe(unknownA.id);
    expect(workers.creations.filter(options => options.sessionFile === original.sessionFile)).toHaveLength(1);
    expect(workers.prompts.some(prompt => prompt.text === blockedMove.text)).toBe(false);

    const candidateStarted = Promise.withResolvers<void>();
    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
    };
    const recovery = inbound({
      ...target,
      messageId: "recovery-chain-new-c",
      text: `/new ${recoveryCwd}`,
    });
    const recoveryReceipt = harness.service.receive(recovery);
    await awaitBounded(recoveryReceipt.ackReady, "recovery /new admission");
    await awaitBounded(candidateStarted.promise, "recovery /new candidate creation past the unknown barrier");
    const recoveryJob = ingressJob(store, harness, recovery);
    expect(recoveryJob).toMatchObject({ state: "dispatch-intent", ackReady: true });

    const arrivingDuringRecovery = inbound({
      ...target,
      messageId: "recovery-chain-during-candidate",
      text: "run this only after the recovered session is bound",
    });
    const arrivingReceipt = harness.service.receive(arrivingDuringRecovery);
    await awaitBounded(arrivingReceipt.ackReady, "message admission during recovery");
    expect(ingressJob(store, harness, arrivingDuringRecovery)).toMatchObject({ state: "accepted", ackReady: true });

    releaseCandidate.resolve();
    const freshBinding = await (async () => {
      await waitUntil(() => {
        const current = store.getBinding(harness.bot.uid, target);
        return current !== null
          && current.sessionId !== null
          && current.sessionId !== original.sessionId
          && current.cwd === recoveryCwd;
      }, "recovery /new binding");
      const current = store.getBinding(harness.bot.uid, target);
      if (current?.sessionFile === null || current === null) throw new Error("recovery binding has no session file");
      return current;
    })();
    await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(freshBinding.sessionId),
      "recovery /new receipt",
    );
    await waitUntil(
      () => workers.prompts.some(prompt => prompt.text.includes(arrivingDuringRecovery.text)),
      "prompt for input received during recovery",
    );

    const freshPrompt = workers.prompts.find(prompt => prompt.text.includes(arrivingDuringRecovery.text));
    expect(freshPrompt).toMatchObject({ sessionId: freshBinding.sessionId, cwd: recoveryCwd });
    expect(workers.prompts.some(prompt => prompt.text === blockedMove.text)).toBe(false);
    expect(workers.creations.filter(options => options.sessionFile === original.sessionFile)).toHaveLength(1);
    expect(ingressJob(store, harness, recovery)).toMatchObject({ state: "handed-off" });

    const blockedNotice = await waitForSentText(
      harness,
      target,
      text => text.includes(blockedMove.messageId) && text.includes("未执行"),
      "identifiable not-executed notice for the pre-recovery /move",
    );
    expect(blockedNotice).toContain("忽略");
    expect(blockedNotice).toMatch(/重新发送|重发/u);
    expect(sentText(harness, target, text => text.includes(arrivingDuringRecovery.messageId) && text.includes("未执行")))
      .toBeUndefined();
  } finally {
    releaseCandidate.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("a later /new recovers both a prior unknown move and an unknown recovery /new without replay", async () => {
  const workers = new FakeWorkerFactory();
  const first = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-recovery-chain-restarted" };
  const sourceCwd = join(first.root, "source-cwd");
  const moveAPath = join(first.root, "unknown-move-a");
  const recoveryBCwd = join(first.root, "unknown-recovery-b-cwd");
  const recoveryCCwd = join(first.root, "recovered-c-cwd");
  await Promise.all([sourceCwd, moveAPath, recoveryBCwd, recoveryCCwd].map(path => mkdir(path, { recursive: true })));
  let firstStore: ChannelStore | undefined = await openChannelStore(first.stateDir);
  let firstStopped = false;
  let restarted: CoreHarness | undefined;
  let restartedStore: ChannelStore | undefined;
  try {
    const activeStore = firstStore;
    if (activeStore === undefined) throw new Error("initial store is unavailable");
    const original = await bindInitialSession(first, activeStore, target, sourceCwd, "chain-restart-initial");
    const { job: unknownA } = await makeUnknownMove(
      first,
      activeStore,
      target,
      original,
      moveAPath,
      "chain-restart-unknown-a",
    );
    const unknownAMessageId = unknownA.messageId;
    if (unknownAMessageId === null) throw new Error("unknown /move lost its source message id");

    failNextWorkerMethod(workers, "refreshState");
    const recoveryB = inbound({
      ...target,
      messageId: "chain-restart-unknown-b",
      text: `/new ${recoveryBCwd}`,
    });
    const recoveryBReceipt = first.service.receive(recoveryB);
    await awaitBounded(recoveryBReceipt.ackReady, "unknown recovery /new admission");
    const unknownBNotice = await waitForSentText(
      first,
      target,
      text => text.includes("无法确认新会话是否完整创建") && text.includes("本次不会自动重试"),
      "unknown recovery /new receipt",
    );
    expect(unknownBNotice).toContain("相关目录保持保护");
    await waitUntil(() => ingressJob(activeStore, first, recoveryB)?.state === "unknown", "unknown recovery /new state");
    const unknownB = ingressJob(activeStore, first, recoveryB);
    if (unknownB === null) throw new Error("unknown recovery /new ingress job was not persisted");
    expect(activeStore.getWorkerLedger(first.bot.uid, `new-ingress-${unknownB.id}`)).toMatchObject({ state: "stopped" });

    const attemptsBeforeRestart = workers.creations.length;
    expect(attemptsBeforeRestart).toBe(3);
    activeStore.close();
    firstStore = undefined;
    await first.service.stop();
    firstStopped = true;

    const transport = new FakeTransport({ bot: first.bot });
    restarted = await createCoreHarness({ root: first.root, bot: first.bot, transport, workers });
    restartedStore = await openChannelStore(restarted.stateDir);
    const creationCountBeforeC = workers.creations.length;
    expect(creationCountBeforeC).toBe(attemptsBeforeRestart);
    expect(ingressJob(restartedStore, restarted, recoveryB)).toMatchObject({ state: "unknown" });

    const recoveryC = inbound({
      ...target,
      messageId: "chain-restart-recovery-c",
      text: `/new ${recoveryCCwd}`,
    });
    const recoveryCReceipt = restarted.service.receive(recoveryC);
    await awaitBounded(recoveryCReceipt.ackReady, "later recovery /new admission");
    await waitUntil(() => {
      const current = restartedStore!.getBinding(restarted!.bot.uid, target);
      return current !== null
        && current.sessionId !== original.sessionId
        && current.cwd === recoveryCCwd;
    }, "second recovery /new binding");
    const recoveredBinding = restartedStore.getBinding(restarted.bot.uid, target);
    if (recoveredBinding?.sessionFile === null || recoveredBinding === null) {
      throw new Error("later recovery /new did not publish a persisted session");
    }
    await waitForSentText(
      restarted,
      target,
      text => text.includes("已创建会话") && text.includes(recoveredBinding.sessionId),
      "later recovery /new receipt",
    );
    expect(workers.creations).toHaveLength(creationCountBeforeC + 1);


    const afterRecoveryInput = inbound({
      ...target,
      messageId: "chain-restart-fresh-input",
      text: "continue only in recovery C's new session",
    });
    const freshReceipt = restarted.service.receive(afterRecoveryInput);
    await awaitBounded(freshReceipt.ackReady, "post-recovery message admission");
    await waitUntil(
      () => workers.prompts.some(prompt => prompt.text.includes(afterRecoveryInput.text)),
      "prompt after chained session recovery",
    );
    const freshPrompt = workers.prompts.find(prompt => prompt.text.includes(afterRecoveryInput.text));
    expect(freshPrompt).toMatchObject({ sessionId: recoveredBinding.sessionId, cwd: recoveryCCwd });
    expect(workers.creations).toHaveLength(creationCountBeforeC + 2);
    expect(workers.prompts.some(prompt => prompt.text === `/move ${moveAPath}`)).toBe(false);
    expect(workers.prompts.some(prompt => prompt.text.includes(recoveryB.text))).toBe(false);
    expect(restarted.transport.sent.some(message =>
      message.target.channelId === target.channelId
      && (message.text.includes(unknownAMessageId) || message.text.includes(recoveryB.messageId))))
      .toBe(false);
  } finally {
    firstStore?.close();
    restartedStore?.close();
    if (!firstStopped) await first.service.stop().catch(() => {});
    if (restarted !== undefined) {
      await closeCoreHarness(restarted);
    } else {
      await rm(first.root, { recursive: true, force: true });
    }
  }
}, 20_000);
