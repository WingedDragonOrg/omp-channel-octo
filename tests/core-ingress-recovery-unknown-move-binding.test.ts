import { expect, test } from "bun:test";
import { mkdir, rename, rm } from "node:fs/promises";
import { basename, join } from "node:path";
import type { InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId, type IngressJobRecord } from "../src/inbound/ingress-types.ts";
import { findMovedSession, loadValidatedSessionState } from "../src/session/project-session.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const WAIT_TIMEOUT_MS = 5_000;
type Target = { channelType: 1; channelId: string };

interface SessionRootsGate {
  readonly entered: Promise<void>;
  readonly release: () => void;
}

async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + WAIT_TIMEOUT_MS;
  while (performance.now() < deadline) {
    if (await check()) return;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error(`did not observe ${label} within ${WAIT_TIMEOUT_MS} ms`);
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

function ingressJob(store: ChannelStore, harness: CoreHarness, message: InboundMessage): IngressJobRecord | null {
  return store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
  });
}

async function waitForSentText(
  harness: CoreHarness,
  target: Target,
  startIndex: number,
  predicate: (text: string) => boolean,
  label: string,
): Promise<string> {
  let text: string | undefined;
  await waitUntil(() => {
    text = harness.transport.sent.slice(startIndex).find(message =>
      message.target.channelType === target.channelType
      && message.target.channelId === target.channelId
      && predicate(message.text))?.text;
    return text !== undefined;
  }, label);
  if (text === undefined) throw new Error(`missing ${label}`);
  return text;
}
/** Gate SessionLifecycle's real awaited root lookup; its first caller is /status, then /move proceeds. */
function gateFirstSessionRootsResolution(workers: FakeWorkerFactory): SessionRootsGate {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let firstCall = true;
  Object.assign(workers, {
    sessionRootsFor: async (_cwd: string): Promise<readonly string[]> => {
      if (firstCall) {
        firstCall = false;
        entered.resolve();
        await release.promise;
      }
      return [];
    },
  });
  return { entered: entered.promise, release: release.resolve };
}

async function bindInitialSession(
  harness: CoreHarness,
  store: ChannelStore,
  target: Target,
  cwd: string,
) {
  const message = inbound({ ...target, messageId: "unknown-move-binding-initial", text: `/new ${cwd}` });
  const outputStart = harness.transport.sent.length;
  await awaitBounded(harness.service.receive(message).ackReady, "initial /new admission");
  await waitUntil(() => {
    const binding = store.getBinding(harness.bot.uid, target);
    return binding !== null && binding.sessionId !== null && binding.sessionFile !== null;
  }, "initial /new binding");
  const binding = store.getBinding(harness.bot.uid, target);
  if (binding === null || binding.sessionId === null || binding.sessionFile === null) {
    throw new Error("initial /new did not publish a complete session binding");
  }
  await waitForSentText(
    harness,
    target,
    outputStart,
    text => text.includes("已创建会话") && text.includes(binding.sessionId!),
    "initial /new receipt",
  );
  return { sessionId: binding.sessionId, sessionFile: binding.sessionFile, cwd: binding.cwd };
}

/** Let the fixture's native /move update its session file, then make state verification uncertain. */
function failNextRefreshState(workers: FakeWorkerFactory): void {
  const createWorker = workers.create.bind(workers);
  let intercepted = false;
  workers.create = async options => {
    const worker = await createWorker(options);
    if (intercepted) return worker;
    intercepted = true;
    return new Proxy(worker, {
      get(target, property) {
        if (property === "refreshState") {
          return async () => {
            throw new Error("fixture refreshState failed after native /move");
          };
        }
        return Reflect.get(target, property, target);
      },
    });
  };
}
async function createUnknownMoveWithNativeMigration(
  harness: CoreHarness,
  store: ChannelStore,
  target: Target,
  original: { readonly sessionId: string; readonly sessionFile: string; readonly cwd: string },
  moveCwd: string,
  messageId: string,
) {
  failNextRefreshState(harness.workers);
  const message = inbound({ ...target, messageId, text: `/move ${moveCwd}` });
  const outputStart = harness.transport.sent.length;
  await awaitBounded(harness.service.receive(message).ackReady, "unknown /move admission");
  await waitUntil(() => ingressJob(store, harness, message)?.state === "unknown", "unknown /move outcome");
  const notice = await waitForSentText(
    harness,
    target,
    outputStart,
    text => text.includes("无法确认 OMP 原生 /move 是否完成") && text.includes("本次不会自动重试"),
    "unknown /move notice",
  );
  expect(notice).toContain("源与目标目录保持保护");

  const job = ingressJob(store, harness, message);
  if (job === null) throw new Error("unknown /move ingress was not persisted");
  expect(job).toMatchObject({
    state: "unknown",
    ackReady: true,
    operationCheckpoint: { kind: "command", action: "move", phase: "dispatch-intent" },
  });
  const ledgerId = `move-ingress-${job.id}`;
  expect(store.getWorkerLedger(harness.bot.uid, ledgerId)).toMatchObject({
    state: "stopped",
    sessionId: original.sessionId,
    sessionFile: original.sessionFile,
    cwd: original.cwd,
    moveTargetCwd: moveCwd,
  });
  expect(store.getBinding(harness.bot.uid, target)).toMatchObject(original);

  const moveWorkerOptions = harness.workers.creations.at(-1);
  if (moveWorkerOptions === undefined || moveWorkerOptions.sessionFile !== original.sessionFile) {
    throw new Error("fixture /move did not use the bound native session file");
  }
  const justMoved = await loadValidatedSessionState(original.sessionFile, original.sessionId);
  expect(justMoved.state).toMatchObject({ sessionId: original.sessionId, cwd: moveCwd });
  expect(justMoved.previousSessionFiles).toContain(original.sessionFile);

  // The fixture's native /move wrote a valid OMP header; place that artifact in the destination
  // project's session directory so the production external-move search can discover it.
  const migrationFile = join(moveWorkerOptions.sessionDir, basename(original.sessionFile));
  await rename(original.sessionFile, migrationFile);
  const migrated = await loadValidatedSessionState(migrationFile, original.sessionId);
  expect(migrated.state).toMatchObject({
    sessionId: original.sessionId,
    sessionFile: migrationFile,
    cwd: moveCwd,
  });
  expect(migrated.previousSessionFiles).toContain(original.sessionFile);
  expect(await findMovedSession(
    { sessionId: original.sessionId, sessionFile: original.sessionFile, cwd: original.cwd },
    { roots: [join(harness.stateDir, "sessions")] },
  )).toEqual(migrated.state);
  return { message, job, ledgerId, migrationFile };
}

test("ordinary preparation and /status preserve an unknown /move binding until explicit /new after restart", async () => {
  const workers = new FakeWorkerFactory();
  const first = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-unknown-move-native-binding" };
  const sourceCwd = join(first.root, "unknown-move-source");
  const moveCwd = join(first.root, "unknown-move-target");
  await Promise.all([sourceCwd, moveCwd].map(path => mkdir(path, { recursive: true })));

  let firstStore: ChannelStore | undefined = await openChannelStore(first.stateDir);
  let firstStopped = false;
  let restarted: CoreHarness | undefined;
  let restartedStore: ChannelStore | undefined;
  try {
    const store = firstStore;
    if (store === undefined) throw new Error("initial store is unavailable");
    const original = await bindInitialSession(first, store, target, sourceCwd);
    const creationsBeforeMove = workers.creations.length;

    const {
      message: moveMessage,
      job: unknownMove,
      ledgerId: moveLedgerId,
      migrationFile,
    } = await createUnknownMoveWithNativeMigration(
      first,
      store,
      target,
      original,
      moveCwd,
      "unknown-move-binding-move",
    );
    expect(store.getBinding(first.bot.uid, target)).toMatchObject(original);
    expect(workers.creations).toHaveLength(creationsBeforeMove + 1);

    firstStore.close();
    firstStore = undefined;
    await closeCoreHarness(first, { remove: false });
    firstStopped = true;

    const transport = new FakeTransport({ bot: first.bot });
    restarted = await createCoreHarness({ root: first.root, bot: first.bot, transport, workers });
    restartedStore = await openChannelStore(restarted.stateDir);
    const recoveredStore = restartedStore;
    if (recoveredStore === undefined) throw new Error("restarted store is unavailable");
    expect(ingressJob(recoveredStore, restarted, moveMessage)).toMatchObject({ state: "unknown" });
    expect(recoveredStore.getWorkerLedger(restarted.bot.uid, moveLedgerId)).toMatchObject({ state: "stopped" });
    expect(recoveredStore.getBinding(restarted.bot.uid, target)).toMatchObject(original);

    const ordinaryMessage = inbound({
      ...target,
      messageId: "unknown-move-binding-ordinary-before-new",
      text: "ordinary input remains blocked until explicit session recovery",
    });
    await awaitBounded(restarted.service.receive(ordinaryMessage).ackReady, "pre-recovery ordinary admission");
    await waitUntil(() => {
      const job = ingressJob(recoveredStore, restarted!, ordinaryMessage);
      return job !== null && job.ackReady && job.preparedText?.includes(ordinaryMessage.text) === true;
    }, "pre-recovery ordinary input preparation");
    const preparedOrdinary = ingressJob(recoveredStore, restarted, ordinaryMessage);
    if (preparedOrdinary === null) throw new Error("prepared ordinary ingress was not persisted");
    expect(preparedOrdinary).toMatchObject({ state: "accepted", ackReady: true });
    expect(preparedOrdinary.routeSnapshot?.barrierJobId).toBe(unknownMove.id);
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinaryMessage.text))).toBe(false);
    expect(recoveredStore.getBinding(restarted.bot.uid, target)).toMatchObject(original);

    const statusMessage = inbound({
      ...target,
      messageId: "unknown-move-binding-status-before-new",
      text: "/status",
    });
    const statusOutputStart = transport.sent.length;
    await awaitBounded(restarted.service.receive(statusMessage).ackReady, "post-restart /status admission");
    const oldStatus = await waitForSentText(
      restarted,
      target,
      statusOutputStart,
      text => text.includes("session=") && text.includes("cwd="),
      "post-restart /status response",
    );
    expect(oldStatus).toContain(`session=${original.sessionId}`);
    expect(oldStatus).toContain(`cwd=${sourceCwd}`);
    expect(oldStatus).not.toContain(`cwd=${moveCwd}`);
    expect(recoveredStore.getBinding(restarted.bot.uid, target)).toMatchObject(original);
    expect(workers.creations).toHaveLength(creationsBeforeMove + 1);

    const recoveryMessage = inbound({
      ...target,
      messageId: "unknown-move-binding-explicit-new",
      text: `/new ${moveCwd}`,
    });
    const recoveryOutputStart = transport.sent.length;
    await awaitBounded(restarted.service.receive(recoveryMessage).ackReady, "explicit recovery /new admission");
    const recoveryReceipt = await waitForSentText(
      restarted,
      target,
      recoveryOutputStart,
      text => text.includes("已创建会话"),
      "explicit recovery /new receipt",
    );
    await waitUntil(() => {
      const binding = recoveredStore.getBinding(restarted!.bot.uid, target);
      return binding !== null
        && binding.sessionId !== original.sessionId
        && binding.sessionFile !== original.sessionFile
        && binding.cwd === moveCwd;
    }, "independent recovery /new binding");
    const recoveredBinding = recoveredStore.getBinding(restarted.bot.uid, target);
    if (recoveredBinding === null || recoveredBinding.sessionId === null || recoveredBinding.sessionFile === null) {
      throw new Error("explicit recovery /new did not publish a complete binding");
    }
    expect(recoveryReceipt).toContain(recoveredBinding.sessionId);
    expect(recoveryReceipt).toContain(`cwd=${moveCwd}`);
    expect(recoveryReceipt).not.toContain("当前会话绑定已变化");
    const notExecuted = await waitForSentText(
      restarted,
      target,
      recoveryOutputStart,
      text => text.includes(ordinaryMessage.messageId) && /未执行|未派发/u.test(text),
      "not-executed notice for the pre-recovery ordinary input",
    );
    expect(notExecuted).toContain("忽略");
    expect(notExecuted).toMatch(/重新发送|重发/u);
    expect(workers.prompts.some(prompt => prompt.text.includes(ordinaryMessage.text))).toBe(false);
    expect(ingressJob(recoveredStore, restarted, recoveryMessage)).toMatchObject({ state: "handed-off" });
    expect(ingressJob(recoveredStore, restarted, moveMessage)).toMatchObject({ state: "unknown" });
    expect(workers.creations).toHaveLength(creationsBeforeMove + 2);
    expect(workers.prompts.filter(prompt => prompt.text === `/move ${moveCwd}`)).toHaveLength(1);

    const recoveredWorker = workers.workers.at(-1);
    expect(recoveredWorker).toMatchObject({
      sessionId: recoveredBinding.sessionId,
      sessionFile: recoveredBinding.sessionFile,
    });
    expect(recoveredWorker?.sessionFile).not.toBe(migrationFile);
    expect(await loadValidatedSessionState(migrationFile, original.sessionId)).toMatchObject({
      state: { sessionId: original.sessionId, cwd: moveCwd },
    });

    const finalStatusMessage = inbound({
      ...target,
      messageId: "unknown-move-binding-status-after-new",
      text: "/status",
    });
    const finalStatusStart = transport.sent.length;
    await awaitBounded(restarted.service.receive(finalStatusMessage).ackReady, "recovered /status admission");
    const newStatus = await waitForSentText(
      restarted,
      target,
      finalStatusStart,
      text => text.includes(`session=${recoveredBinding.sessionId}`),
      "recovered /status response",
    );
    expect(newStatus).toContain(`cwd=${moveCwd}`);
    expect(recoveredStore.getBinding(restarted.bot.uid, target)).toMatchObject(recoveredBinding);
  } finally {
    firstStore?.close();
    restartedStore?.close();
    if (!firstStopped) await closeCoreHarness(first).catch(() => {});
    if (restarted !== undefined) {
      await closeCoreHarness(restarted);
    } else if (firstStopped) {
      await rm(first.root, { recursive: true, force: true });
    }
  }
}, 20_000);

test("a /status external-session lookup already awaiting roots cannot rebind after /move becomes unknown", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-unknown-move-scan-race" };
  const sourceCwd = join(harness.root, "scan-race-source");
  const moveCwd = join(harness.root, "scan-race-target");
  await Promise.all([sourceCwd, moveCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  let rootsGate: SessionRootsGate | undefined;
  try {
    const original = await bindInitialSession(harness, store, target, sourceCwd);
    const creationsBeforeMove = workers.creations.length;
    rootsGate = gateFirstSessionRootsResolution(workers);

    const statusMessage = inbound({
      ...target,
      messageId: "unknown-move-scan-race-status",
      text: "/status",
    });
    const statusOutputStart = harness.transport.sent.length;
    await awaitBounded(harness.service.receive(statusMessage).ackReady, "racing /status admission");
    await awaitBounded(rootsGate.entered, "racing /status session-root lookup");

    const moveMessageBeforeDispatch = inbound({
      ...target,
      messageId: "unknown-move-scan-race-move",
      text: `/move ${moveCwd}`,
    });
    expect(ingressJob(store, harness, moveMessageBeforeDispatch)).toBeNull();
    const {
      message: moveMessage,
      job: unknownMove,
      ledgerId,
      migrationFile,
    } = await createUnknownMoveWithNativeMigration(
      harness,
      store,
      target,
      original,
      moveCwd,
      moveMessageBeforeDispatch.messageId,
    );
    expect(unknownMove).toMatchObject({ state: "unknown" });
    expect(store.getWorkerLedger(harness.bot.uid, ledgerId)).toMatchObject({ state: "stopped" });
    expect(store.getBinding(harness.bot.uid, target)).toMatchObject(original);
    expect(harness.transport.sent.slice(statusOutputStart).some(message => message.text.includes("session=")))
      .toBe(false);

    rootsGate.release();
    const statusText = await waitForSentText(
      harness,
      target,
      statusOutputStart,
      text => text.includes("session=") && text.includes("cwd="),
      "racing /status response after unknown /move",
    );
    expect(statusText).toContain(`session=${original.sessionId}`);
    expect(statusText).toContain(`cwd=${sourceCwd}`);
    expect(statusText).not.toContain(`cwd=${moveCwd}`);
    expect(store.getBinding(harness.bot.uid, target)).toMatchObject(original);

    const recoveryMessage = inbound({
      ...target,
      messageId: "unknown-move-scan-race-new",
      text: `/new ${moveCwd}`,
    });
    const recoveryOutputStart = harness.transport.sent.length;
    await awaitBounded(harness.service.receive(recoveryMessage).ackReady, "racing recovery /new admission");
    const recoveryReceipt = await waitForSentText(
      harness,
      target,
      recoveryOutputStart,
      text => text.includes("已创建会话"),
      "racing recovery /new receipt",
    );
    await waitUntil(() => {
      const binding = store.getBinding(harness.bot.uid, target);
      return binding !== null
        && binding.sessionId !== original.sessionId
        && binding.sessionFile !== original.sessionFile
        && binding.cwd === moveCwd;
    }, "racing recovery /new binding");
    const recovered = store.getBinding(harness.bot.uid, target);
    if (recovered === null || recovered.sessionId === null || recovered.sessionFile === null) {
      throw new Error("racing recovery /new did not publish a complete session binding");
    }
    expect(recoveryReceipt).toContain(recovered.sessionId);
    expect(recoveryReceipt).toContain(`cwd=${moveCwd}`);
    expect(recoveryReceipt).not.toContain("当前会话绑定已变化");
    expect(ingressJob(store, harness, recoveryMessage)).toMatchObject({ state: "handed-off" });
    expect(ingressJob(store, harness, moveMessage)).toMatchObject({ state: "unknown" });
    expect(workers.creations).toHaveLength(creationsBeforeMove + 2);
    expect(workers.prompts.filter(prompt => prompt.text === `/move ${moveCwd}`)).toHaveLength(1);
    expect(workers.workers.at(-1)).toMatchObject({
      sessionId: recovered.sessionId,
      sessionFile: recovered.sessionFile,
    });
    expect(await loadValidatedSessionState(migrationFile, original.sessionId)).toMatchObject({
      state: { sessionId: original.sessionId, cwd: moveCwd },
    });
  } finally {
    rootsGate?.release();
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);
