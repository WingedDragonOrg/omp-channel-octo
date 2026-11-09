import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
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
    return binding !== null
      && binding.sessionId !== null
      && binding.sessionFile !== null
      && binding.cwd === cwd;
  }, "initial persisted session binding");
  const binding = store.getBinding(harness.bot.uid, target);
  if (binding === null || binding.sessionId === null || binding.sessionFile === null) {
    throw new Error("initial /new did not publish a complete session binding");
  }
  await waitForSentText(
    harness,
    target,
    text => text.includes("已创建会话") && text.includes(binding.sessionId!) && text.includes(`cwd=${cwd}`),
    "initial /new receipt",
  );
  return binding;
}

/** Fail one real CoreService worker operation after its worker has been created. */
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
            throw new Error(`fixture ${method} failure after worker creation`);
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
  initial: { readonly sessionId: string; readonly cwd: string },
  cwd: string,
  messageId: string,
) {
  failNextWorkerMethod(harness.workers, "prompt");
  const message = inbound({ ...target, messageId, text: `/move ${cwd}` });
  const receipt = harness.service.receive(message);
  await awaitBounded(receipt.ackReady, "unknown /move admission");
  const notice = await waitForSentText(
    harness,
    target,
    text => text.includes("无法确认 OMP 原生 /move 是否完成") && text.includes("本次不会自动重试"),
    "unknown /move notice",
  );
  expect(notice).toContain("源与目标目录保持保护");
  expect(store.getBinding(harness.bot.uid, target)).toMatchObject(initial);
  return message;
}

/** Hold a selected prompt at the worker boundary while the real service handles later ingress. */
function holdWorkerPrompt(
  workers: FakeWorkerFactory,
  promptText: string,
  onStart: () => void,
  waitForRelease: () => Promise<void>,
): void {
  const createWorker = workers.create.bind(workers);
  let held = false;
  workers.create = async options => {
    const worker = await createWorker(options);
    return new Proxy(worker, {
      get(target, property) {
        if (property !== "prompt") return Reflect.get(target, property, target);
        return async (...args: unknown[]) => {
          if (!held && args[0] === promptText) {
            held = true;
            onStart();
            await waitForRelease();
          }
          const prompt = Reflect.get(target, property, target);
          return Reflect.apply(prompt, target, args);
        };
      },
    });
  };
}

function createdSessionId(receipt: string): string {
  const match = /已创建会话\s+([^，\s]+)/u.exec(receipt);
  if (match === null) throw new Error(`receipt does not name a created session: ${receipt}`);
  return match[1]!;
}

function expectNoStaleNewRejection(harness: CoreHarness, target: Target): void {
  expect(harness.transport.sent.some(message =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && message.text.includes("当前会话绑定已变化")
    && message.text.includes("/new 未执行"))).toBe(false);
}

test("a /move accepted during recovery /new runs after recovery, and a later /new follows it", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-transition-order-unknown" };
  const sourceCwd = join(harness.root, "source-cwd");
  const unknownMoveCwd = join(harness.root, "unknown-move-cwd");
  const recoveryCwd = join(harness.root, "recovery-b-cwd");
  const moveCwd = join(harness.root, "move-c-cwd");
  await Promise.all([sourceCwd, unknownMoveCwd, recoveryCwd, moveCwd]
    .map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const recoveryCandidateStarted = Promise.withResolvers<void>();
  const releaseRecoveryCandidate = Promise.withResolvers<void>();
  const releaseMoveWorker = Promise.withResolvers<void>();
  const movePromptStarted = Promise.withResolvers<void>();
  const releaseMovePrompt = Promise.withResolvers<void>();
  try {
    const initial = await bindInitialSession(
      harness,
      store,
      target,
      sourceCwd,
      "transition-unknown-initial",
    );
    const unknown = await makeUnknownMove(
      harness,
      store,
      target,
      initial,
      unknownMoveCwd,
      "transition-unknown-a",
    );

    holdWorkerPrompt(
      workers,
      `/move ${moveCwd}`,
      () => movePromptStarted.resolve(),
      () => releaseMovePrompt.promise,
    );
    workers.beforeCreate = async () => {
      const options = workers.creations.at(-1);
      if (options?.cwd === recoveryCwd && options.sessionFile === undefined) {
        recoveryCandidateStarted.resolve();
        await releaseRecoveryCandidate.promise;
      } else if (options?.sessionFile !== undefined) {
        await releaseMoveWorker.promise;
      }
    };

    const recovery = inbound({
      ...target,
      messageId: "transition-recovery-b",
      text: `/new ${recoveryCwd}`,
    });
    const recoveryReceipt = harness.service.receive(recovery);
    await awaitBounded(recoveryReceipt.ackReady, "recovery /new admission");
    await awaitBounded(recoveryCandidateStarted.promise, "gated recovery candidate creation");

    const move = inbound({
      ...target,
      messageId: "transition-move-c",
      text: `/move ${moveCwd}`,
    });
    const moveReceipt = harness.service.receive(move);
    await awaitBounded(moveReceipt.ackReady, "/move admission while recovery B is held");
    releaseRecoveryCandidate.resolve();
    workers.beforeCreate = undefined;
    await waitUntil(() => {
      const current = store.getBinding(harness.bot.uid, target);
      return current !== null
        && current.sessionId !== null
        && current.sessionId !== initial.sessionId
        && current.cwd === recoveryCwd;
    }, "successful recovery B binding");
    const recovered = store.getBinding(harness.bot.uid, target);
    if (recovered === null || recovered.sessionId === null || recovered.sessionFile === null) {
      throw new Error("recovery B did not publish a complete session binding");
    }
    const recoveryText = await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话")
        && text.includes(recovered.sessionId!)
        && text.includes(`cwd=${recoveryCwd}`),
      "recovery B visible receipt",
    );
    expect(createdSessionId(recoveryText)).toBe(recovered.sessionId);

    releaseMoveWorker.resolve();
    await awaitBounded(movePromptStarted.promise, "move C worker prompt start");
    const laterNew = inbound({
      ...target,
      messageId: "transition-new-d",
      text: "/new",
    });
    const laterNewReceipt = harness.service.receive(laterNew);
    await awaitBounded(laterNewReceipt.ackReady, "new D admission during move C");
    releaseMovePrompt.resolve();

    const movedText = await waitForSentText(
      harness,
      target,
      text => text.includes(`已迁移会话 ${recovered.sessionId}`) && text.includes(`cwd=${moveCwd}`),
      "move C visible receipt",
    );
    expect(movedText).toContain(recovered.sessionId);
    const movePrompt = workers.prompts.find(prompt => prompt.text === move.text);
    expect(movePrompt).toMatchObject({ sessionId: recovered.sessionId, cwd: recoveryCwd });

    const finalText = await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(`cwd=${moveCwd}`),
      "new D visible receipt after move C",
    );
    const finalSessionId = createdSessionId(finalText);
    expect(finalSessionId).not.toBe(recovered.sessionId);
    const finalBinding = store.getBinding(harness.bot.uid, target);
    expect(finalBinding).toMatchObject({ sessionId: finalSessionId, cwd: moveCwd });
    expectNoStaleNewRejection(harness, target);
    expect(harness.transport.sent.some(message =>
      message.target.channelType === target.channelType
      && message.target.channelId === target.channelId
      && message.text.includes("无法确认 OMP 原生 /move 是否完成")
      && message.text.includes("本次不会自动重试"))).toBe(true);
    expect(workers.prompts.some(prompt => prompt.text === unknown.text)).toBe(false);
  } finally {
    releaseRecoveryCandidate.resolve();
    releaseMoveWorker.resolve();
    releaseMovePrompt.resolve();
    workers.beforeCreate = undefined;
    store.close();
    await closeCoreHarness(harness);
  }
}, 25_000);

test("a new /new waits for an ordinary in-flight /new and then uses its binding", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-transition-order-new" };
  const sourceCwd = join(harness.root, "source-cwd");
  const firstCwd = join(harness.root, "first-new-cwd");
  await Promise.all([sourceCwd, firstCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const firstCandidateStarted = Promise.withResolvers<void>();
  const releaseFirstCandidate = Promise.withResolvers<void>();
  try {
    const initial = await bindInitialSession(harness, store, target, sourceCwd, "normal-new-initial");
    workers.beforeCreate = async () => {
      workers.beforeCreate = undefined;
      firstCandidateStarted.resolve();
      await releaseFirstCandidate.promise;
    };

    const first = inbound({
      ...target,
      messageId: "normal-new-first",
      text: `/new ${firstCwd}`,
    });
    const firstReceipt = harness.service.receive(first);
    await awaitBounded(firstReceipt.ackReady, "first normal /new admission");
    await awaitBounded(firstCandidateStarted.promise, "first normal /new candidate start");

    const second = inbound({
      ...target,
      messageId: "normal-new-second",
      text: "/new",
    });
    const secondReceipt = harness.service.receive(second);
    await awaitBounded(secondReceipt.ackReady, "second normal /new admission");
    releaseFirstCandidate.resolve();

    const firstText = await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(`cwd=${firstCwd}`),
      "first normal /new receipt",
    );
    const firstSessionId = createdSessionId(firstText);
    const secondText = await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话")
        && text.includes(`cwd=${firstCwd}`)
        && createdSessionId(text) !== firstSessionId,
      "second normal /new receipt",
    );
    const secondSessionId = createdSessionId(secondText);
    expect(firstSessionId).not.toBe(initial.sessionId);
    expect(secondSessionId).not.toBe(firstSessionId);
    expect(store.getBinding(harness.bot.uid, target)).toMatchObject({
      sessionId: secondSessionId,
      cwd: firstCwd,
    });
    expectNoStaleNewRejection(harness, target);
  } finally {
    releaseFirstCandidate.resolve();
    workers.beforeCreate = undefined;
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);

test("a new /new waits for an ordinary in-flight /move and then uses the moved binding", async () => {
  const workers = new FakeWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target: Target = { channelType: 1, channelId: "dm-recovery-transition-order-move" };
  const sourceCwd = join(harness.root, "source-cwd");
  const moveCwd = join(harness.root, "move-cwd");
  await Promise.all([sourceCwd, moveCwd].map(path => mkdir(path, { recursive: true })));
  const store = await openChannelStore(harness.stateDir);
  const movePromptStarted = Promise.withResolvers<void>();
  const releaseMovePrompt = Promise.withResolvers<void>();
  try {
    const initial = await bindInitialSession(harness, store, target, sourceCwd, "normal-move-initial");
    const move = inbound({
      ...target,
      messageId: "normal-move-first",
      text: `/move ${moveCwd}`,
    });
    holdWorkerPrompt(
      workers,
      move.text,
      () => movePromptStarted.resolve(),
      () => releaseMovePrompt.promise,
    );
    const moveReceipt = harness.service.receive(move);
    await awaitBounded(moveReceipt.ackReady, "normal /move admission");
    await awaitBounded(movePromptStarted.promise, "normal /move worker prompt start");

    const nextNew = inbound({
      ...target,
      messageId: "normal-move-following-new",
      text: "/new",
    });
    const newReceipt = harness.service.receive(nextNew);
    await awaitBounded(newReceipt.ackReady, "new /new admission during normal /move");
    releaseMovePrompt.resolve();

    const movedText = await waitForSentText(
      harness,
      target,
      text => text.includes(`已迁移会话 ${initial.sessionId}`) && text.includes(`cwd=${moveCwd}`),
      "normal /move visible receipt",
    );
    expect(movedText).toContain(initial.sessionId);
    const movePrompt = workers.prompts.find(prompt => prompt.text === move.text);
    expect(movePrompt).toMatchObject({ sessionId: initial.sessionId, cwd: sourceCwd });

    const newText = await waitForSentText(
      harness,
      target,
      text => text.includes("已创建会话") && text.includes(`cwd=${moveCwd}`),
      "new session receipt after normal /move",
    );
    const newSessionId = createdSessionId(newText);
    expect(newSessionId).not.toBe(initial.sessionId);
    expect(store.getBinding(harness.bot.uid, target)).toMatchObject({
      sessionId: newSessionId,
      cwd: moveCwd,
    });
    expectNoStaleNewRejection(harness, target);
  } finally {
    releaseMovePrompt.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
}, 20_000);
