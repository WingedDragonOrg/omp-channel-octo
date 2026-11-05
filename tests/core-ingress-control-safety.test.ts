import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WorkerSteerRejectedError, type ChatTarget, type InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const WATCH_CONFIG = {
  enabled: true as const,
  protocol: "openai-decisions" as const,
  baseUrl: "https://watch.example",
  model: "watch-fixture",
  apiKeyEnv: "OMP_CHANNEL_OCTO_WATCH_KEY",
  apiKey: "fixture-watch-key",
};

async function waitUntil(
  check: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`did not observe ${label}`);
}

async function settleTurns(count = 8): Promise<void> {
  for (let index = 0; index < count; index += 1) {
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
}

async function observedState(promise: Promise<unknown>): Promise<"pending" | "fulfilled" | "rejected"> {
  let state: "pending" | "fulfilled" | "rejected" = "pending";
  void promise.then(
    () => { state = "fulfilled"; },
    () => { state = "rejected"; },
  );
  await settleTurns(4);
  return state;
}

async function withStore<T>(harness: CoreHarness, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await openChannelStore(harness.stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

function ownerMessage(
  botUid: string,
  target: ChatTarget,
  messageId: string,
  text: string,
  mentioned = target.channelType !== 1,
): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    mentionedUserIds: mentioned ? [botUid] : [],
  });
}

async function readIngressJob(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
) {
  return withStore(harness, store => store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(target.channelType, target.channelId, messageId),
  }));
}

async function readTask(harness: CoreHarness, target: ChatTarget, messageId: string) {
  return withStore(harness, store => store.getTaskByMessageId(harness.bot.uid, target, messageId));
}

async function readTaskStopIntent(harness: CoreHarness, messageId: string): Promise<string | null> {
  const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    const row = database.query("SELECT stop_intent FROM tasks WHERE bot_uid = ? AND message_id = ?")
      .get(harness.bot.uid, messageId) as { stop_intent: string | null } | null;
    return row?.stop_intent ?? null;
  } finally {
    database.close();
  }
}

async function countTasksByMessageId(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
): Promise<number> {
  const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    const row = database.query(`
      SELECT COUNT(*) AS count
      FROM tasks
      WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
    `).get(harness.bot.uid, target.channelType, target.channelId, messageId) as { count: number };
    return row.count;
  } finally {
    database.close();
  }
}


function spawnChild(root: string, source: string): { child: Bun.Subprocess; stderr: Promise<string> } {
  const child = Bun.spawn([process.execPath, "-e", source], {
    env: { ...process.env, INGRESS_CONTROL_ROOT: root },
    stdout: "ignore",
    stderr: "pipe",
  });
  const stderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();
  return { child, stderr };
}

async function waitForChildFile(
  child: Bun.Subprocess,
  stderr: Promise<string>,
  path: string,
  label: string,
): Promise<void> {
  await waitUntil(async () => child.exitCode !== null || await Bun.file(path).exists(), label, 12_000);
  if (child.exitCode !== null) {
    throw new Error(`${label}: child exited early (${child.exitCode}): ${await stderr}`);
  }
}

async function killChild(child: Bun.Subprocess | undefined, stderr: Promise<string> | undefined): Promise<void> {
  if (child && child.exitCode === null) {
    child.kill("SIGKILL");
    await child.exited;
  }
  if (stderr) await stderr.catch(() => "");
}

function gatedCommandHook(messageId: string, gatePath: string): string {
  return `
    const originalHandleCommand = core.handleCommand.bind(core);
    core.handleCommand = async (message, ...args) => {
      if (message.messageId === ${JSON.stringify(messageId)}) {
        writeFileSync(${JSON.stringify(gatePath)}, "dispatch intent reached handler boundary", "utf8");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
      }
      return originalHandleCommand(message, ...args);
    };
  `;
}

test("SIGKILL after /interrupt dispatch intent retains the durable task stop intent", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-interrupt-intent-crash-"));
  const bot = { uid: "bot-ingress-interrupt-intent", name: "Interrupt Intent Bot" };
  const target = { channelType: 1 as const, channelId: "dm-ingress-interrupt-intent" };
  const activeMessageId = "interrupt-intent-active";
  const controlMessageId = "interrupt-intent-command";
  const gatePath = join(root, "interrupt-handler-gate");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    import { join } from "node:path";
    const root = process.env.INGRESS_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const activeStarted = Promise.withResolvers();
    const workers = new FakeWorkerFactory(async ({ text, waitForAbort }) => {
      if (text === "long-running interrupt target") {
        activeStarted.resolve();
        await waitForAbort();
      }
    });
    const harness = await createCoreHarness({ root, bot, workers });
    const core = harness.service;
    const active = core.receive(inbound({ ...target, messageId: ${JSON.stringify(activeMessageId)}, text: "long-running interrupt target" }));
    await active.ackReady;
    await activeStarted.promise;
    ${gatedCommandHook(controlMessageId, gatePath)}
    const interrupt = core.receive(inbound({ ...target, messageId: ${JSON.stringify(controlMessageId)}, text: "/interrupt" }));
    await interrupt.ackReady;
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, gatePath, "interrupt reaches the pre-handler crash boundary");

    const db = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
    try {
      const control = db.query(`
        SELECT state, operation_checkpoint_json FROM ingress_jobs
        WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
      `).get(bot.uid, target.channelType, target.channelId, controlMessageId) as {
        state: string;
        operation_checkpoint_json: string | null;
      } | null;
      const task = db.query(`
        SELECT state, stop_intent FROM tasks WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
      `).get(bot.uid, target.channelType, target.channelId, activeMessageId) as {
        state: string;
        stop_intent: string | null;
      } | null;
      expect(control?.state).toBe("dispatch-intent");
      expect(task).toMatchObject({ state: "started", stop_intent: "interrupt" });
    } finally {
      db.close();
    }

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await settleTurns();
    expect(await readTask(restarted, target, activeMessageId)).toMatchObject({ state: "interrupted" });
    expect(restartWorkers.prompts).toHaveLength(0);
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("SIGKILL after /goal pause dispatch intent retains the goal continuation revocation", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-goal-pause-crash-"));
  const bot = { uid: "bot-ingress-goal-pause", name: "Goal Pause Intent Bot" };
  const target = { channelType: 1 as const, channelId: "dm-ingress-goal-pause" };
  const goalMessageId = "goal-pause-active";
  const controlMessageId = "goal-pause-command";
  const goalStartedPath = join(root, "goal-started");
  const gatePath = join(root, "goal-pause-handler-gate");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    import { join } from "node:path";
    const root = process.env.INGRESS_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const goalStartedPath = ${JSON.stringify(goalStartedPath)};
    const goalRelease = Promise.withResolvers();
    const workers = new FakeWorkerFactory();
    workers.goalHandler = async ({ request, emit, sessionId }) => {
      const goal = {
        id: "fixture-crash-goal",
        objective: request.op === "create" ? request.objective : "pause this goal",
        status: "active",
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      if (request.op === "create") {
        emit({ type: "goal_accepted", goalId: goal.id });
        emit({ type: "goal_updated", goal });
        emit({ type: "prompt_accepted" });
        writeFileSync(goalStartedPath, sessionId, "utf8");
        await goalRelease.promise;
        return { goal, state: { enabled: true, mode: "active", goal } };
      }
      return { goal: { ...goal, status: request.op === "pause" ? "paused" : "dropped" }, state: { enabled: true, mode: "active", goal } };
    };
    const harness = await createCoreHarness({ root, bot, workers });
    const core = harness.service;
    const start = core.receive(inbound({ ...target, messageId: ${JSON.stringify(goalMessageId)}, text: "/goal set Keep this goal paused" }));
    await start.ackReady;
    while (!(await Bun.file(goalStartedPath).exists())) {
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    ${gatedCommandHook(controlMessageId, gatePath)}
    const pause = core.receive(inbound({ ...target, messageId: ${JSON.stringify(controlMessageId)}, text: "/goal pause" }));
    await pause.ackReady;
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, gatePath, "goal pause reaches the pre-handler crash boundary");

    const observer = await openChannelStore(join(root, "state"));
    let sessionId: string;
    try {
      sessionId = observer.getBinding(bot.uid, target)?.sessionId ?? "";
      expect(sessionId).not.toBe("");
      expect(observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, controlMessageId),
      })).toMatchObject({ state: "dispatch-intent" });
      expect(observer.getGoalSession(bot.uid, target, sessionId)).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "pause",
      });
    } finally {
      observer.close();
    }

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await settleTurns();
    expect(restartWorkers.goalRuns).toHaveLength(0);
    await withStore(restarted, store => {
      expect(store.getGoalSession(bot.uid, target, sessionId)).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "pause",
      });
    });
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("SIGKILL after /watch stop dispatch intent retains the durable watch revocation", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-watch-stop-crash-"));
  const bot = { uid: "bot-ingress-watch-stop", name: "Watch Stop Intent Bot" };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const createMessageId = "watch-stop-crash-create";
  const controlMessageId = "watch-stop-crash-command";
  const watchIdPath = join(root, "watch-id");
  const gatePath = join(root, "watch-stop-handler-gate");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    const root = process.env.INGRESS_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const watchIdPath = ${JSON.stringify(watchIdPath)};
    const workers = new FakeWorkerFactory(async ({ promptContext }) => {
      const watch = promptContext?.tools?.find(({ name }) => name === "watch");
      if (!watch) throw new Error("watch host tool missing");
      const created = JSON.parse(await watch.execute({
        action: "create",
        command: "fixture-watch-command",
        condition: "stop me before restart",
        interval: 3600,
        timeout: 7200,
      }, new AbortController().signal, "crash-watch-create"));
      writeFileSync(watchIdPath, created.id, "utf8");
    });
    const harness = await createCoreHarness({
      root,
      bot,
      workers,
      decisions: ${JSON.stringify(WATCH_CONFIG)},
      watchCollector: async () => ({ kind: "success", stdout: "still waiting" }),
      watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    });
    const core = harness.service;
    const create = core.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(createMessageId)},
      text: "create a bounded watch",
      mentionedUserIds: [bot.uid],
    }));
    await create.ackReady;
    await workers.waitForPromptCount(1);
    while (!(await Bun.file(watchIdPath).exists())) {
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    const watchId = (await Bun.file(watchIdPath).text()).trim();
    ${gatedCommandHook(controlMessageId, gatePath)}
    const stop = core.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(controlMessageId)},
      text: "/watch stop " + watchId,
      mentionedUserIds: [bot.uid],
    }));
    await stop.ackReady;
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, gatePath, "watch stop reaches the pre-handler crash boundary");
    const watchId = (await readFile(watchIdPath, "utf8")).trim();
    const observer = await openChannelStore(join(root, "state"));
    try {
      expect(observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, controlMessageId),
      })).toMatchObject({ state: "dispatch-intent" });
      expect(observer.getWatch(bot.uid, watchId)).toMatchObject({ state: "stopped" });
    } finally {
      observer.close();
    }

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    restarted = await createCoreHarness({
      root,
      bot,
      decisions: WATCH_CONFIG,
      watchCollector: async () => ({ kind: "success", stdout: "still waiting" }),
      watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    });
    await settleTurns();
    await withStore(restarted, store => {
      expect(store.getWatch(bot.uid, watchId)).toMatchObject({ state: "stopped" });
    });
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("an explicit ordinary-steer refusal after /cancel cannot enqueue a task", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ text, waitForAbort }) => {
      if (text === "active owner task") await waitForAbort();
    },
  });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const activeMessageId = "steer-refusal-active";
  const steerMessageId = "steer-refusal-ordinary-message";
  const steerStarted = Promise.withResolvers<void>();
  const releaseRefusal = Promise.withResolvers<void>();
  harness.workers.beforeSteer = async () => {
    steerStarted.resolve();
    await releaseRefusal.promise;
    throw new WorkerSteerRejectedError("fixture worker explicitly refused the steer");
  };

  try {
    const active = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      activeMessageId,
      "active owner task",
    ));
    await active.ackReady;
    await harness.workers.waitForPromptCount(1);

    const steering = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      steerMessageId,
      "ordinary steer must not become a task after cancellation",
    ));
    await steering.ackReady;
    await steerStarted.promise;

    const cancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      "steer-refusal-cancel",
      "/cancel",
    ));
    await cancel.ackReady;
    await waitUntil(async () => await readTaskStopIntent(harness, activeMessageId) === "cancel",
      "durable cancel stop intent before releasing ordinary steer refusal");
    releaseRefusal.resolve();

    const afterCancelMessageId = "steer-refusal-after-cancel";
    const afterCancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      afterCancelMessageId,
      "ordinary work after cancel",
    ));
    await afterCancel.ackReady;
    await waitUntil(async () => (await readTask(harness, target, afterCancelMessageId))?.state === "completed",
      "ordinary work after the cancelled steer");
    expect(await readTask(harness, target, steerMessageId)).toBeNull();
    expect(harness.workers.prompts.some(({ text }) =>
      text.includes("ordinary steer must not become a task after cancellation"))).toBe(false);
  } finally {
    releaseRefusal.resolve();
    await closeCoreHarness(harness);
  }
});

test("an explicit /thread-task steer refusal after /cancel cannot enqueue a fallback task", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ text, waitForAbort }) => {
      if (text === "active owner task for thread race") await waitForAbort();
    },
  });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const activeMessageId = "thread-steer-refusal-active";
  const threadMessageId = "thread-steer-refusal-command-task";
  const intent = "thread command fallback must not start after cancellation";
  const steerStarted = Promise.withResolvers<void>();
  const releaseRefusal = Promise.withResolvers<void>();
  harness.workers.beforeSteer = async () => {
    steerStarted.resolve();
    await releaseRefusal.promise;
    throw new WorkerSteerRejectedError("fixture worker explicitly refused the thread-task steer");
  };

  try {
    const active = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      activeMessageId,
      "active owner task for thread race",
    ));
    await active.ackReady;
    await harness.workers.waitForPromptCount(1);

    const steering = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      threadMessageId,
      `/thread ${intent}`,
    ));
    await steering.ackReady;
    await steerStarted.promise;

    const cancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      "thread-steer-refusal-cancel",
      "/cancel",
    ));
    await cancel.ackReady;
    await waitUntil(async () => await readTaskStopIntent(harness, activeMessageId) === "cancel",
      "durable cancel stop intent before releasing thread-task steer refusal");
    releaseRefusal.resolve();

    const afterCancelMessageId = "thread-steer-refusal-after-cancel";
    const afterCancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      afterCancelMessageId,
      "ordinary work after cancelling thread-task steer",
    ));
    await afterCancel.ackReady;
    await waitUntil(async () => (await readTask(harness, target, afterCancelMessageId))?.state === "completed",
      "ordinary work after the cancelled thread-task steer");

    expect(await countTasksByMessageId(harness, target, threadMessageId)).toBe(0);
    expect(await readTask(harness, target, threadMessageId)).toBeNull();
    expect(harness.workers.prompts.some(({ text }) => text.includes(intent))).toBe(false);
  } finally {
    releaseRefusal.resolve();
    await closeCoreHarness(harness);
  }
});

test("SIGKILL after cancel acceptance keeps pre-authorization candidate tombstones atomic", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-preauth-crash-"));
  const bot = { uid: "bot-ingress-preauth-crash", name: "Preauth Crash Bot" };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const candidateMessageId = "preauth-candidate-before-cancel";
  const cancelMessageId = "preauth-cancel-control";
  const lookupStartedPath = join(root, "preauth-lookup-started");
  const acceptedPath = join(root, "cancel-accepted-before-next-step");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { createCoreHarness, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    import { join } from "node:path";
    const root = process.env.INGRESS_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const lookupStartedPath = ${JSON.stringify(lookupStartedPath)};
    const acceptedPath = ${JSON.stringify(acceptedPath)};
    const harness = await createCoreHarness({ root, bot });
    const core = harness.service;
    harness.transport.getGroupMentionPref = async () => {
      writeFileSync(lookupStartedPath, "candidate authorization held", "utf8");
      return new Promise(() => {});
    };
    const originalAccept = core.store.acceptIngressJob.bind(core.store);
    core.store.acceptIngressJob = input => {
      const accepted = originalAccept(input);
      if (input.messageId === ${JSON.stringify(cancelMessageId)}) {
        // Call the real SQLite transaction and pause only after its committed result is available.
        writeFileSync(acceptedPath, "real control acceptance committed", "utf8");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
      }
      return accepted;
    };
    const candidate = core.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(candidateMessageId)},
      text: "delayed authorization must stay cancelled",
      mentionedUserIds: [],
    }));
    while (!(await Bun.file(lookupStartedPath).exists())) {
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    const cancel = core.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(cancelMessageId)},
      text: "/cancel",
      mentionedUserIds: [bot.uid],
    }));
    await cancel.ackReady;
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, acceptedPath, "cancel acceptance commits before in-memory tombstoning");
    const observer = await openChannelStore(join(root, "state"));
    try {
      expect(observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, cancelMessageId),
      })).toMatchObject({ state: "accepted", ackReady: true });
      expect(observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, candidateMessageId),
      })).toMatchObject({ state: "cancelled", ackReady: true });
    } finally {
      observer.close();
    }

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    restarted = await createCoreHarness({ root, bot });
    const redelivery = restarted.service.receive(ownerMessage(
      bot.uid,
      target,
      candidateMessageId,
      "delayed authorization must stay cancelled",
      false,
    ));
    await redelivery.ackReady;
    await settleTurns();
    expect(await readIngressJob(restarted, target, candidateMessageId)).toMatchObject({ state: "cancelled" });
    expect(await readTask(restarted, target, candidateMessageId)).toBeNull();
    expect(restarted.workers.prompts).toHaveLength(0);
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("a recovered shared-session /move stays behind the older task from another chat", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-ingress-move-fifo-crash-")));
  const bot = { uid: "bot-ingress-move-fifo", name: "Move FIFO Bot" };
  const firstTarget = { channelType: 1 as const, channelId: "dm-move-fifo-first" };
  const moveTarget = { channelType: 1 as const, channelId: "dm-move-fifo-second" };
  const source = join(root, "shared-session-source");
  const destination = join(root, "shared-session-destination");
  const olderMessageId = "move-fifo-older-task";
  const moveMessageId = "move-fifo-later-command";
  const boundaryPath = join(root, "move-fifo-crash-boundary");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { openChannelStore } from ${JSON.stringify(new URL("../src/store/index.ts", import.meta.url).href)};
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    import { mkdirSync, writeFileSync } from "node:fs";
    import { join } from "node:path";
    const root = process.env.INGRESS_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const firstTarget = ${JSON.stringify(firstTarget)};
    const moveTarget = ${JSON.stringify(moveTarget)};
    const source = ${JSON.stringify(source)};
    const destination = ${JSON.stringify(destination)};
    mkdirSync(source, { recursive: true });
    mkdirSync(destination, { recursive: true });
    const taskStarted = Promise.withResolvers();
    const workers = new FakeWorkerFactory(async ({ text, waitForAbort }) => {
      if (text === "older shared-session task") {
        taskStarted.resolve();
        await waitForAbort();
      }
    });
    const harness = await createCoreHarness({ root, bot, workers });
    const core = harness.service;
    const store = await openChannelStore(harness.stateDir);
    const binding = {
      botUid: bot.uid,
      channelType: 1,
      channelId: firstTarget.channelId,
      cwd: source,
      sessionId: "shared-session-for-cross-chat-fifo",
      sessionFile: null,
      cwdPinned: true,
    };
    store.ensureBinding(binding);
    store.ensureBinding({ ...binding, channelId: moveTarget.channelId });
    store.close();
    const older = core.receive(inbound({ ...firstTarget, messageId: ${JSON.stringify(olderMessageId)}, text: "older shared-session task" }));
    await older.ackReady;
    await taskStarted.promise;
    const afterTaskStore = await openChannelStore(harness.stateDir);
    const taskBinding = afterTaskStore.getBinding(bot.uid, firstTarget);
    if (!taskBinding?.sessionFile) throw new Error("older task did not persist its shared session file");
    afterTaskStore.ensureBinding({ ...taskBinding, ...moveTarget });
    afterTaskStore.close();
    const move = core.receive(inbound({ ...moveTarget, messageId: ${JSON.stringify(moveMessageId)}, text: "/move " + destination }));
    await move.ackReady;
    writeFileSync(${JSON.stringify(boundaryPath)}, "older task and later move are durable", "utf8");
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;
  const releaseRecoveredTask = Promise.withResolvers<void>();

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, boundaryPath, "older task and later /move reach recovery boundary");

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;
    // Opening a store reapplies schema in a write transaction; inspect only after the crashed writer exits.
    const observer = await openChannelStore(join(root, "state"));
    try {
      expect(observer.getTaskByMessageId(bot.uid, firstTarget, olderMessageId)).toMatchObject({ state: "running" });
      expect(observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(moveTarget.channelType, moveTarget.channelId, moveMessageId),
      })).toMatchObject({ state: "accepted" });
    } finally {
      observer.close();
    }

    const restartWorkers = new FakeWorkerFactory(async ({ text }) => {
      if (text === "/retry") {
        await releaseRecoveredTask.promise;
      }
    });
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await waitUntil(() => restartWorkers.prompts.some(({ text }) => text === "/retry"), "older task resume start");
    expect(restartWorkers.prompts[0]?.text).toBe("/retry");
    expect(await withStore(restarted, store => store.getBinding(bot.uid, moveTarget)?.cwd)).toBe(source);

    releaseRecoveredTask.resolve();
    try {
      await waitUntil(async () => {
        const job = await readIngressJob(restarted!, moveTarget, moveMessageId);
        return job?.state === "handed-off";
      }, "recovered /move durable handoff after older shared-session task", 5_000);
    } catch (error) {
      const job = await readIngressJob(restarted, moveTarget, moveMessageId);
      const bindingCwd = await withStore(restarted, store => store.getBinding(bot.uid, moveTarget)?.cwd ?? null);
      const prompts = restartWorkers.prompts.map(({ text }) => text).join(" -> ");
      throw new Error(
        `recovered /move did not reach durable handoff: job=${job?.state ?? "missing"} `
          + `checkpoint=${JSON.stringify(job?.operationCheckpoint ?? null)} `
          + `bindingCwd=${bindingCwd ?? "missing"} prompts=${prompts || "none"}`,
        { cause: error },
      );
    }
    expect(await withStore(restarted, store => store.getBinding(bot.uid, moveTarget)?.cwd)).toBe(destination);
    expect(restartWorkers.prompts.findIndex(({ text }) => text === "/retry"))
      .toBeLessThan(restartWorkers.prompts.findIndex(({ text }) => text === `/move ${destination}`));
  } finally {
    releaseRecoveredTask.resolve();
    try {
      await killChild(child, childStderr);
    } finally {
      try {
        if (restarted) await closeCoreHarness(restarted, { remove: false });
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  }
}, 45_000);

async function assertReadQuerySurvivesCancel(text: "/mode" | "/queue" | "/switch", label: string): Promise<void> {
  const harness = await createCoreHarness();
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const queryMessageId = `${label}-read-query`;
  const lookupStarted = Promise.withResolvers<void>();
  const releaseLookup = Promise.withResolvers<void>();
  const releaseQueryHandler = Promise.withResolvers<void>();
  let queryHandlerBlocked = false;
  harness.transport.getGroupMentionPref = async (_groupId, signal) => {
    lookupStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(signal?.reason ?? new Error("held authorization cancelled"));
    if (signal?.aborted) rejectAbort();
    else signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseLookup.promise, aborted.promise]);
    return { effective: true };
  };
  const core = harness.service as unknown as {
    handleCommand: (message: InboundMessage, ...args: unknown[]) => Promise<void>;
  };
  const handleCommand = core.handleCommand.bind(core);
  core.handleCommand = async (message, ...args) => {
    if (message.messageId === queryMessageId) {
      queryHandlerBlocked = true;
      await releaseQueryHandler.promise;
    }
    return handleCommand(message, ...args);
  };
  const held = harness.service.receive(ownerMessage(
    harness.bot.uid,
    target,
    `${label}-held-authorization`,
    "held no-mention permission lookup",
    false,
  ));

  try {
    await lookupStarted.promise;
    const query = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      queryMessageId,
      text,
    ));
    expect(await observedState(query.ackReady)).toBe("fulfilled");
    await waitUntil(() => queryHandlerBlocked, `${text} reaches its real handler before release`);

    const cancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      `${label}-cancel`,
      "/cancel",
    ));
    await cancel.ackReady;
    await held.ackReady;
    releaseQueryHandler.resolve();
    releaseLookup.resolve();

    await waitUntil(async () => (await readIngressJob(harness, target, queryMessageId))?.state === "handed-off",
      `${text} query durable handoff after cancel`);
    expect(await readTask(harness, target, queryMessageId)).toBeNull();
  } finally {
    releaseQueryHandler.resolve();
    releaseLookup.resolve();
    await closeCoreHarness(harness);
  }
}

for (const [text, label] of [
  ["/mode", "mode"],
  ["/queue", "queue"],
  ["/switch", "switch"],
] as const) {
  test(`no-argument ${text} read query bypasses held authorization and survives /cancel`, async () => {
    await assertReadQuerySurvivesCancel(text, label);
  });
}

test("/cancel closes the pending question left by a timed-out ask", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const questions = [{
    id: "after-timeout-cancel",
    question: "Should the timed-out work continue?",
    options: [{ label: "yes" }, { label: "no" }],
  }];
  const harness = await createCoreHarness({
    transport,
    ask: { timeoutMs: 40 },
    prompt: async ({ append, promptContext, text, waitForAbort }) => {
      if (text.startsWith("[ask 答复]")) {
        await append("unexpected-wake.log", text);
        return;
      }
      if (text === "1") {
        await append("ordinary-reply.log", text);
        return;
      }
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      try {
        await append("answer.log", await ask.execute({ questions }, new AbortController().signal));
      } catch {
        await waitForAbort();
      }
    },
  });
  const target = { channelType: 1 as const, channelId: "dm-ask-timeout-cancel" };
  const startMessageId = "ask-times-out-before-cancel";

  try {
    const start = harness.service.receive(inbound({
      ...target,
      messageId: startMessageId,
      text: "work that waits for an answer",
    }));
    await start.ackReady;
    await harness.workers.waitForPromptCount(1);
    const timedOut = await waitForStatus(
      harness.service,
      target,
      status => status.state === "interrupted" && status.pendingQuestionIds.length === 1,
    );
    const questionId = timedOut.pendingQuestionIds[0];
    if (!questionId) throw new Error("timed-out ask question identity missing");

    const cancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      "cancel-timed-out-ask",
      "/cancel",
    ));
    await cancel.ackReady;
    await waitUntil(
      async () => withStore(harness, store => store.getQuestion(questionId)?.state === "cancelled"),
      "cancelled question after /cancel",
      2_000,
    );
    await withStore(harness, store => expect(store.getQuestion(questionId)?.state).toBe("cancelled"));

    const lateReplyMessageId = "reply-after-timeout-cancel";
    const ordinaryReplyPath = join(harness.root, "tmp", "ordinary-reply.log");
    const lateReply = harness.service.receive(inbound({
      ...target,
      messageId: lateReplyMessageId,
      text: "1",
    }));
    await lateReply.ackReady;
    await waitUntil(
      () => harness.workers.prompts.length >= 2,
      "ordinary late-reply prompt after cancelled ask",
      2_000,
    );
    await waitUntil(async () => {
      const task = await readTask(harness, target, lateReplyMessageId);
      if (task?.state !== "completed") return false;
      try {
        return await readFile(ordinaryReplyPath, "utf8") === "1";
      } catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ENOENT") return false;
        throw error;
      }
    }, "late ordinary reply task completion after /cancel", 2_000);
    expect(harness.workers.prompts[1]?.text).toBe("1");
    await settleTurns();
    expect(await readTask(harness, target, `ask-answer:${questionId}`)).toBeNull();
    expect(await Bun.file(join(harness.root, "tmp", "unexpected-wake.log")).exists()).toBe(false);
    expect(await readFile(ordinaryReplyPath, "utf8")).toBe("1");
  } finally {
    await closeCoreHarness(harness);
  }
}, 15_000);
