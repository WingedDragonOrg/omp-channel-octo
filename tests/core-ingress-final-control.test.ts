import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
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
  timeoutMs = 8_000,
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

async function withStore<T>(stateDir: string, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await openChannelStore(stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

async function readTask(
  stateDir: string,
  botUid: string,
  target: ChatTarget,
  messageId: string,
) {
  return withStore(stateDir, store => store.getTaskByMessageId(botUid, target, messageId));
}

async function readIngressJob(
  stateDir: string,
  botUid: string,
  target: ChatTarget,
  messageId: string,
) {
  return withStore(stateDir, store => store.getIngressJobBySource({
    botUid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(target.channelType, target.channelId, messageId),
  }));
}

async function readTaskStopIntent(stateDir: string, botUid: string, messageId: string): Promise<string | null> {
  const database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  try {
    const row = database.query("SELECT stop_intent FROM tasks WHERE bot_uid = ? AND message_id = ?")
      .get(botUid, messageId) as { stop_intent: string | null } | null;
    return row?.stop_intent ?? null;
  } finally {
    database.close();
  }
}

function ownerMessage(botUid: string, target: ChatTarget, messageId: string, text: string): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [botUid],
  });
}

function spawnChild(root: string, source: string): { child: Bun.Subprocess; stderr: Promise<string> } {
  const child = Bun.spawn([process.execPath, "-e", source], {
    env: { ...process.env, INGRESS_FINAL_CONTROL_ROOT: root },
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
  timeoutMs = 15_000,
): Promise<void> {
  await waitUntil(async () => child.exitCode !== null || await Bun.file(path).exists(), label, timeoutMs);
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

test("an ACKed /new before candidate creation restores its barrier after SIGKILL", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-new-barrier-crash-"));
  const bot = { uid: "bot-new-barrier-crash", name: "New Barrier Crash Bot" };
  const target = { channelType: 1 as const, channelId: "dm-new-barrier-crash" };
  const cwd = join(root, "replacement");
  const switchId = "new-barrier-switch";
  const laterId = "new-barrier-later";
  const boundary = join(root, "boundary");
  await mkdir(cwd);
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const source = `
    import { createCoreHarness, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    const harness = await createCoreHarness({ root: process.env.INGRESS_FINAL_CONTROL_ROOT, bot: ${JSON.stringify(bot)} });
    Object.assign(harness.service, { startupGate: Promise.withResolvers() });
    const target = ${JSON.stringify(target)};
    await harness.service.receive(inbound({ ...target, messageId: ${JSON.stringify(switchId)}, text: ${JSON.stringify(`/new ${cwd}`)} })).ackReady;
    await harness.service.receive(inbound({ ...target, messageId: ${JSON.stringify(laterId)}, text: "use replacement session" })).ackReady;
    if (harness.workers.creations.length !== 0) throw new Error("candidate started before boundary");
    writeFileSync(${JSON.stringify(boundary)}, "ready");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  `;
  let child: Bun.Subprocess | undefined;
  let stderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;
  try {
    ({ child, stderr } = spawnChild(root, source));
    await waitForChildFile(child, stderr, boundary, "ACKed session switch before candidate");
    const stateDir = join(root, "state");
    const switchJob = await readIngressJob(stateDir, bot.uid, target, switchId);
    const laterJob = await readIngressJob(stateDir, bot.uid, target, laterId);
    expect(switchJob).toMatchObject({ state: "accepted", ackReady: true, operationCheckpoint: { kind: "command", phase: "barrier-installed" } });
    expect(laterJob).toMatchObject({ state: "accepted", ackReady: true, routeSnapshot: { barrierJobId: switchJob?.id } });
    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await stderr;
    child = undefined;
    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers });
    await waitUntil(() => workers.prompts.length === 1, "later prompt after recovered switch");
    expect(workers.prompts[0]?.cwd).toBe(await realpath(cwd));
    expect((await restarted.service.status(target)).cwd).toBe(await realpath(cwd));
    await restarted.service.receive(ownerMessage(bot.uid, target, switchId, `/new ${cwd}`)).ackReady;
    expect(workers.prompts).toHaveLength(1);
  } finally {
    await killChild(child, stderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);

test("an ACKed /interrupt before dispatch survives SIGKILL and stops the active task", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-final-interrupt-ack-crash-"));
  const bot = { uid: "bot-final-interrupt-ack", name: "Final Interrupt ACK Bot" };
  const target = { channelType: 1 as const, channelId: "dm-final-interrupt-ack" };
  const activeMessageId = "final-interrupt-ack-active";
  const interruptMessageId = "final-interrupt-ack-command";
  const gatePath = join(root, "interrupt-ack-before-dispatch");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const childSource = `
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    import { writeFileSync } from "node:fs";
    const root = process.env.INGRESS_FINAL_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_FINAL_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const activeStarted = Promise.withResolvers();
    const workers = new FakeWorkerFactory(async ({ text, waitForAbort }) => {
      if (text === "active task before acknowledged interrupt") {
        activeStarted.resolve();
        await waitForAbort();
      }
    });
    const harness = await createCoreHarness({ root, bot, workers });
    const active = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(activeMessageId)},
      text: "active task before acknowledged interrupt",
    }));
    await active.ackReady;
    await activeStarted.promise;
    const dispatchGate = Promise.withResolvers();
    Object.assign(harness.service, { startupGate: dispatchGate });
    const interrupt = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(interruptMessageId)},
      text: "/interrupt",
    }));
    await interrupt.ackReady;
    writeFileSync(${JSON.stringify(gatePath)}, "interrupt ACK ready before command dispatch", "utf8");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, gatePath, "ACKed interrupt held before dispatch");

    const database = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
    try {
      const interrupt = database.query(`
        SELECT state, ack_ready FROM ingress_jobs
        WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
      `).get(bot.uid, target.channelType, target.channelId, interruptMessageId) as {
        state: string;
        ack_ready: number;
      } | null;
      const active = database.query(`
        SELECT state, stop_intent FROM tasks
        WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
      `).get(bot.uid, target.channelType, target.channelId, activeMessageId) as {
        state: string;
        stop_intent: string | null;
      } | null;
      expect(interrupt).toEqual({ state: "accepted", ack_ready: 1 });
      expect(active).toMatchObject({ state: "started", stop_intent: "interrupt" });
    } finally {
      database.close();
    }

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers });
    await waitUntil(async () => {
      const task = await readTask(restarted!.stateDir, bot.uid, target, activeMessageId);
      return task?.state === "interrupted" || workers.prompts.length > 0;
    }, "interrupted task recovery without a resumed prompt");
    expect(await readTask(restarted.stateDir, bot.uid, target, activeMessageId)).toMatchObject({ state: "interrupted" });
    expect(await restarted.service.status(target)).toMatchObject({ state: "interrupted" });
    expect(workers.prompts).toHaveLength(0);

    const statusReply = restarted.transport.waitForRemoteText(message =>
      message.target.channelId === target.channelId && message.text.includes("state=interrupted"));
    await restarted.service.submitInbound(ownerMessage(bot.uid, target, "final-interrupt-status", "/status"));
    expect((await statusReply).text).toContain("state=interrupted");
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);

test("an ACKed /interrupt cancels an earlier waiting /thread merge across SIGKILL after dispatch intent", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-ingress-final-interrupt-merge-crash-")));
  const bot = { uid: "bot-final-interrupt-merge", name: "Final Interrupt Merge Bot" };
  const shortId = "final-interrupt-waiting-merge";
  const target = { channelType: 5 as const, channelId: `group-allowed____${shortId}` };
  const threadSummary = {
    shortId,
    groupId: "group-allowed",
    channelId: target.channelId,
    name: "Final interrupt merge thread",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-04T00:00:00.000Z",
    updatedAt: "2026-11-04T00:00:00.000Z",
  };
  const activeMessageId = "final-interrupt-merge-active";
  const mergeMessageId = "final-interrupt-merge-command";
  const interruptMessageId = "final-interrupt-merge-interrupt";
  const activeText = "active task before waiting thread merge";
  const mergeReadyPath = join(root, "merge-waiting-session");
  const interruptGatePath = join(root, "interrupt-after-dispatch-intent");
  const cwd = join(root, "merge-workspace");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const storeUrl = new URL("../src/store/index.ts", import.meta.url).href;
  const childSource = `
    import { mkdirSync, writeFileSync } from "node:fs";
    import { openChannelStore } from ${JSON.stringify(storeUrl)};
    import { createCoreHarness, FakeTransport, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    const root = process.env.INGRESS_FINAL_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_FINAL_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const threadSummary = ${JSON.stringify(threadSummary)};
    mkdirSync(${JSON.stringify(cwd)}, { recursive: true });
    const transport = new FakeTransport({ bot });
    transport.setThreads("group-allowed", [threadSummary]);
    const activeStarted = Promise.withResolvers();
    const workers = new FakeWorkerFactory(async ({ text, waitForAbort }) => {
      if (text === ${JSON.stringify(activeText)}) {
        activeStarted.resolve();
        await waitForAbort();
      }
    });
    const harness = await createCoreHarness({ root, bot, transport, workers });
    const setup = await openChannelStore(harness.stateDir);
    setup.ensureBinding({
      botUid: bot.uid,
      ...target,
      cwd: ${JSON.stringify(cwd)},
      sessionId: "final-interrupt-merge-session",
      sessionFile: null,
      cwdPinned: true,
    });
    setup.close();

    const active = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(activeMessageId)},
      text: "/queue " + ${JSON.stringify(activeText)},
      mentionedUserIds: [bot.uid],
    }));
    await active.ackReady;
    await activeStarted.promise;

    const merge = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(mergeMessageId)},
      text: "/thread merge",
      mentionedUserIds: [bot.uid],
    }));
    await merge.ackReady;
    const observer = await openChannelStore(harness.stateDir);
    let mergeJob;
    while (true) {
      mergeJob = observer.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: ${JSON.stringify(recvIngressSourceId(target.channelType, target.channelId, mergeMessageId))},
      });
      if (mergeJob?.state === "accepted" && mergeJob.operationCheckpoint?.phase === "waiting-session") break;
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    observer.close();
    writeFileSync(${JSON.stringify(mergeReadyPath)}, JSON.stringify({
      state: mergeJob.state,
      phase: mergeJob.operationCheckpoint?.phase,
    }), "utf8");

    const core = harness.service;
    const originalHandleCommand = core.handleCommand.bind(core);
    core.handleCommand = async (message, ...args) => {
      if (message.messageId === ${JSON.stringify(interruptMessageId)}) {
        writeFileSync(${JSON.stringify(interruptGatePath)}, "dispatch intent persisted before interrupt handler", "utf8");
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
      }
      return originalHandleCommand(message, ...args);
    };
    const interrupt = core.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(interruptMessageId)},
      text: "/interrupt",
      mentionedUserIds: [bot.uid],
    }));
    await interrupt.ackReady;
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, mergeReadyPath, "earlier /thread merge waiting for the active task");
    expect(JSON.parse(await readFile(mergeReadyPath, "utf8"))).toEqual({
      state: "accepted",
      phase: "waiting-session",
    });
    await waitForChildFile(child, childStderr, interruptGatePath, "interrupt dispatch intent before its handler");

    expect(await readIngressJob(join(root, "state"), bot.uid, target, mergeMessageId)).toMatchObject({ state: "cancelled" });
    expect(await readIngressJob(join(root, "state"), bot.uid, target, interruptMessageId)).toMatchObject({
      state: "dispatch-intent",
      ackReady: true,
    });
    expect(await readTaskStopIntent(join(root, "state"), bot.uid, activeMessageId)).toBe("interrupt");

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    const transport = new FakeTransport({ bot });
    transport.setThreads("group-allowed", [threadSummary]);
    restarted = await createCoreHarness({ root, bot, transport, workers });
    await waitUntil(async () => {
      const job = await readIngressJob(restarted!.stateDir, bot.uid, target, mergeMessageId);
      const task = await readTask(restarted!.stateDir, bot.uid, target, activeMessageId);
      return job?.state === "cancelled" && task?.state === "interrupted";
    }, "waiting merge cancellation and active task interruption recovery");
    expect(await restarted.service.status(target)).toMatchObject({ state: "interrupted", cwd });
    expect(restarted.worktrees?.merges).toHaveLength(0);
    expect(workers.prompts).toHaveLength(0);
  } finally {
    await killChild(child, childStderr);
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);

test("a recovered earlier ingress task runs before its accepted watch wake", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-ingress-final-watch-restart-order-"));
  const bot = { uid: "bot-final-watch-restart", name: "Final Watch Restart Bot" };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const sourceMessageId = "final-watch-restart-source";
  const olderMessageId = "final-watch-restart-older-ingress";
  const sourceText = "create a watch before the older ingress task";
  const olderText = "older ingress task remains active across restart";
  const boundaryPath = join(root, "watch-wake-queued-after-ingress");
  const promptOrderPath = join(root, "recovered-prompt-order");
  const releaseRecoveredPath = join(root, "release-recovered-task");
  const recoveredCompletePath = join(root, "recovered-tasks-complete");
  const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
  const storeUrl = new URL("../src/store/index.ts", import.meta.url).href;
  const childSource = `
    import { writeFileSync } from "node:fs";
    import { openChannelStore } from ${JSON.stringify(storeUrl)};
    import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
    const root = process.env.INGRESS_FINAL_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_FINAL_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const sourceStarted = Promise.withResolvers();
    const collectStarted = Promise.withResolvers();
    const releaseCollection = Promise.withResolvers();
    const olderStarted = Promise.withResolvers();
    let watchId = "";
    const workers = new FakeWorkerFactory(async ({ text, promptContext, waitForAbort }) => {
      if (text === ${JSON.stringify(sourceText)}) {
        const watchTool = promptContext?.tools?.find(({ name }) => name === "watch");
        if (!watchTool) throw new Error("watch host tool missing from the source task");
        const created = JSON.parse(await watchTool.execute({
          action: "create",
          command: "fixture-watch-command",
          condition: "the restart ordering condition is met",
          interval: 60,
          timeout: 60,
        }, new AbortController().signal, "final-watch-restart-order-call"));
        watchId = created.id;
        sourceStarted.resolve();
        return;
      }
      if (text === ${JSON.stringify(olderText)}) {
        olderStarted.resolve();
        await waitForAbort();
      }
    });
    const harness = await createCoreHarness({
      root,
      bot,
      workers,
      decisions: ${JSON.stringify(WATCH_CONFIG)},
      watchCollector: async () => {
        collectStarted.resolve();
        await releaseCollection.promise;
        return { kind: "success", stdout: "condition met" };
      },
      watchJudge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    });
    const source = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(sourceMessageId)},
      text: ${JSON.stringify(sourceText)},
      mentionedUserIds: [bot.uid],
    }));
    await source.ackReady;
    await sourceStarted.promise;
    await collectStarted.promise;

    const observer = await openChannelStore(harness.stateDir);
    while (observer.getTaskByMessageId(bot.uid, target, ${JSON.stringify(sourceMessageId)})?.state !== "completed") {
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    observer.close();

    const older = harness.service.receive(inbound({
      ...target,
      messageId: ${JSON.stringify(olderMessageId)},
      text: "/queue " + ${JSON.stringify(olderText)},
      mentionedUserIds: [bot.uid],
    }));
    await older.ackReady;
    await olderStarted.promise;
    releaseCollection.resolve();

    const wakeObserver = await openChannelStore(harness.stateDir);
    while (true) {
      const watch = wakeObserver.getWatch(bot.uid, watchId);
      const wake = wakeObserver.getTaskByMessageId(bot.uid, target, "watch:" + watchId);
      if (watch?.wakeStatus === "accepted" && wake?.state === "queued") break;
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    wakeObserver.close();
    writeFileSync(${JSON.stringify(boundaryPath)}, watchId, "utf8");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  `;
  const restartSource = `
    import { writeFileSync } from "node:fs";
    import { openChannelStore } from ${JSON.stringify(storeUrl)};
    import { createCoreHarness, FakeWorkerFactory } from ${JSON.stringify(fixtureUrl)};
    const root = process.env.INGRESS_FINAL_CONTROL_ROOT;
    if (!root) throw new Error("INGRESS_FINAL_CONTROL_ROOT is missing");
    const bot = ${JSON.stringify(bot)};
    const target = ${JSON.stringify(target)};
    const watchId = ${JSON.stringify("watch-id-from-parent")};
    const promptOrder = [];
    const workers = new FakeWorkerFactory(async ({ text }) => {
      promptOrder.push(text);
      writeFileSync(${JSON.stringify(promptOrderPath)}, JSON.stringify(promptOrder), "utf8");
      if (text === "/retry") {
        while (!(await Bun.file(${JSON.stringify(releaseRecoveredPath)}).exists())) {
          const turn = Promise.withResolvers();
          setImmediate(turn.resolve);
          await turn.promise;
        }
      }
    });
    workers.recovered = true;
    const harness = await createCoreHarness({
      root,
      bot,
      workers,
      decisions: ${JSON.stringify(WATCH_CONFIG)},
      watchCollector: async () => ({ kind: "success", stdout: "condition still met" }),
      watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    });
    const observer = await openChannelStore(harness.stateDir);
    while (
      observer.getTaskByMessageId(bot.uid, target, ${JSON.stringify(olderMessageId)})?.state !== "completed"
      || observer.getTaskByMessageId(bot.uid, target, "watch:" + watchId)?.state !== "completed"
    ) {
      const turn = Promise.withResolvers();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    const older = observer.getTaskByMessageId(bot.uid, target, ${JSON.stringify(olderMessageId)});
    const wake = observer.getTaskByMessageId(bot.uid, target, "watch:" + watchId);
    observer.close();
    writeFileSync(${JSON.stringify(recoveredCompletePath)}, JSON.stringify({
      olderState: older?.state,
      wakeState: wake?.state,
      prompts: promptOrder,
    }), "utf8");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  `;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restartChild: Bun.Subprocess | undefined;
  let restartStderr: Promise<string> | undefined;

  try {
    const spawned = spawnChild(root, childSource);
    child = spawned.child;
    childStderr = spawned.stderr;
    await waitForChildFile(child, childStderr, boundaryPath, "older ingress plus later accepted watch wake crash boundary", 10_000);
    const watchId = await readFile(boundaryPath, "utf8");
    const older = await readTask(join(root, "state"), bot.uid, target, olderMessageId);
    const wake = await readTask(join(root, "state"), bot.uid, target, `watch:${watchId}`);
    expect(older).toMatchObject({ state: "running" });
    expect(wake).toMatchObject({ state: "queued" });
    expect(await withStore(join(root, "state"), store => store.getWatch(bot.uid, watchId))).toMatchObject({
      state: "matched",
      wakeStatus: "accepted",
      wakeTaskId: wake?.id,
    });

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartSourceWithWatchId = restartSource.replace(
      JSON.stringify("watch-id-from-parent"),
      JSON.stringify(watchId),
    );
    const restarted = spawnChild(root, restartSourceWithWatchId);
    restartChild = restarted.child;
    restartStderr = restarted.stderr;
    await waitForChildFile(restartChild, restartStderr, promptOrderPath, "first task dispatch after restart", 10_000);
    const firstOrder = JSON.parse(await readFile(promptOrderPath, "utf8")) as string[];
    expect(firstOrder[0]).toBe("/retry");
    expect(firstOrder.some(text => text.includes(`Watch ID: watch:${watchId}`))).toBe(false);

    await writeFile(releaseRecoveredPath, "release recovered ingress task", "utf8");
    await waitForChildFile(restartChild, restartStderr, recoveredCompletePath, "both recovered tasks to settle", 10_000);
    const recovered = JSON.parse(await readFile(recoveredCompletePath, "utf8")) as {
      olderState: string;
      wakeState: string;
      prompts: string[];
    };
    expect(recovered).toMatchObject({ olderState: "completed", wakeState: "completed" });
    expect(recovered.prompts.findIndex(text => text === "/retry"))
      .toBeLessThan(recovered.prompts.findIndex(text => text.includes(`Watch ID: watch:${watchId}`)));
  } finally {
    await killChild(child, childStderr);
    await killChild(restartChild, restartStderr);
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);

test("three live watch wakes cannot invert a later ingress scheduler order", async () => {
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const sourceMessageId = "final-watch-live-source";
  const sourceText = "create three independent watches and hold the session";
  const laterMessageId = "final-watch-live-later-ingress";
  const laterText = "later ingress must follow all accepted watch wakes";
  const laterCommand = `/queue ${laterText}`;
  const watchIds: string[] = [];
  const sourceStarted = Promise.withResolvers<void>();
  const releaseSource = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    decisions: WATCH_CONFIG,
    watchCollector: async () => ({ kind: "success", stdout: "condition met" }),
    watchJudge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const watchTool = promptContext?.tools?.find(({ name }) => name === "watch");
      if (!watchTool) throw new Error("watch host tool missing from the live ingress task");
      for (let index = 0; index < 3; index += 1) {
        const created = JSON.parse(await watchTool.execute({
          action: "create",
          command: "fixture-watch-command",
          condition: `live ordering condition ${index + 1} is met`,
          interval: 60,
          timeout: 60,
        }, new AbortController().signal, `final-live-watch-call-${index + 1}`)) as { id: string };
        watchIds.push(created.id);
      }
      sourceStarted.resolve();
      await releaseSource.promise;
    },
  });

  try {
    const source = harness.service.receive(ownerMessage(harness.bot.uid, target, sourceMessageId, sourceText));
    await source.ackReady;
    await sourceStarted.promise;
    await waitUntil(async () => watchIds.length === 3 && await withStore(harness.stateDir, store =>
      watchIds.every(id => store.getWatch(harness.bot.uid, id)?.wakeStatus === "accepted"
        && store.getTaskByMessageId(harness.bot.uid, target, `watch:${id}`)?.state === "queued")),
    "three live watch wake tasks queued behind their source ingress");

    const later = harness.service.receive(ownerMessage(harness.bot.uid, target, laterMessageId, laterCommand));
    await later.ackReady;
    await waitUntil(async () =>
      (await readTask(harness.stateDir, harness.bot.uid, target, laterMessageId))?.state === "queued",
    "later ingress task queued after the watch wakes");

    releaseSource.resolve();
    await waitUntil(async () =>
      (await readTask(harness.stateDir, harness.bot.uid, target, sourceMessageId))?.state === "completed"
      && (await readTask(harness.stateDir, harness.bot.uid, target, laterMessageId))?.state === "completed"
      && await withStore(harness.stateDir, store => watchIds.every(id =>
        store.getTaskByMessageId(harness.bot.uid, target, `watch:${id}`)?.state === "completed")),
    "live ingress, all three wakes, and later ingress to complete",
    12_000);

    const prompts = harness.workers.prompts.map(({ text }) => text);
    const laterIndex = prompts.findIndex(text => text === laterText);
    const wakeIndices = prompts.flatMap((text, index) => text.includes("Watch ID: watch:") ? [index] : []);
    expect(prompts[0]).toBe(sourceText);
    expect(wakeIndices).toHaveLength(3);
    expect(Math.max(...wakeIndices)).toBeLessThan(laterIndex);
    expect(laterIndex).toBeGreaterThan(0);
  } finally {
    releaseSource.resolve();
    await closeCoreHarness(harness);
  }
}, 30_000);

test("an explicit rejected /thread task steer after /cancel is cancelled rather than unknown", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ text, waitForAbort }) => {
      if (text === "active task for rejected thread steer") await waitForAbort();
    },
  });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const activeMessageId = "final-thread-steer-cancel-active";
  const threadMessageId = "final-thread-steer-cancel-command";
  const intent = "explicitly refused thread task stays cancelled";
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
      "active task for rejected thread steer",
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
      "final-thread-steer-cancel-control",
      "/cancel",
    ));
    await cancel.ackReady;
    await waitUntil(async () => await readTaskStopIntent(harness.stateDir, harness.bot.uid, activeMessageId) === "cancel",
      "durable cancel intent before the explicit steer refusal");
    releaseRefusal.resolve();

    await waitUntil(async () => (await readIngressJob(harness.stateDir, harness.bot.uid, target, threadMessageId))?.state !== "accepted",
      "thread-task refusal to settle to a durable ingress outcome");
    const afterCancelMessageId = "final-thread-steer-after-cancel";
    const afterCancel = harness.service.receive(ownerMessage(
      harness.bot.uid,
      target,
      afterCancelMessageId,
      "ordinary work after the refused thread task was cancelled",
    ));
    await afterCancel.ackReady;
    await waitUntil(async () =>
      (await readTask(harness.stateDir, harness.bot.uid, target, afterCancelMessageId))?.state === "completed",
    "ordinary work after the cancelled thread task");

    expect(await readIngressJob(harness.stateDir, harness.bot.uid, target, threadMessageId)).toMatchObject({ state: "cancelled" });
    expect(await readTask(harness.stateDir, harness.bot.uid, target, threadMessageId)).toBeNull();
    expect(harness.workers.prompts.some(({ text }) => text.includes(intent))).toBe(false);
    expect(harness.transport.sent.some(({ target: sentTarget, text }) =>
      sentTarget.channelId === target.channelId && text.includes("当前任务已取消"))).toBe(true);
  } finally {
    releaseRefusal.resolve();
    await closeCoreHarness(harness);
  }
});

async function assertReadControlQuerySurvivesCancel(
  text: string,
  label: string,
  expectedReply: string,
): Promise<void> {
  const harness = await createCoreHarness();
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const queryMessageId = `final-control-query-${label}`;
  let lookupDidStart = false;
  const releaseLookup = Promise.withResolvers<void>();
  let queryHandlerDidReach = false;
  const releaseQueryHandler = Promise.withResolvers<void>();
  harness.transport.getGroupMentionPref = async (_groupId, signal) => {
    lookupDidStart = true;
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(signal?.reason ?? new Error("held authorization cancelled"));
    if (signal?.aborted) rejectAbort();
    else signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseLookup.promise, aborted.promise]);
    return { effective: true };
  };
  const core = harness.service as unknown as {
    handleCommand: (message: InboundMessage, ...args: unknown[]) => Promise<void>;
  };
  const originalHandleCommand = core.handleCommand.bind(harness.service);
  core.handleCommand = async (message, ...args) => {
    if (message.messageId === queryMessageId) {
      queryHandlerDidReach = true;
      await releaseQueryHandler.promise;
    }
    return originalHandleCommand(message, ...args);
  };
  const held = harness.service.receive(inbound({
    ...target,
    messageId: `final-control-query-${label}-held-auth`,
    text: "hold authorization before the read-only query",
    mentionedUserIds: [],
  }));
  const reply = harness.transport.waitForRemoteText(message =>
    message.target.channelId === target.channelId && message.text.includes(expectedReply));

  try {
    await waitUntil(() => lookupDidStart, "held group authorization lookup");
    const query = harness.service.receive(ownerMessage(harness.bot.uid, target, queryMessageId, text));
    await query.ackReady;
    await waitUntil(() => queryHandlerDidReach, `${text} reaches its command handler ahead of held authorization`);

    const cancel = harness.service.receive(ownerMessage(harness.bot.uid, target, `${queryMessageId}-cancel`, "/cancel"));
    await cancel.ackReady;
    await held.ackReady;
    releaseQueryHandler.resolve();
    releaseLookup.resolve();

    await waitUntil(async () => (await readIngressJob(harness.stateDir, harness.bot.uid, target, queryMessageId))?.state === "handed-off",
      `${text} read-only command handoff after cancellation`);
    expect(await readTask(harness.stateDir, harness.bot.uid, target, queryMessageId)).toBeNull();
    expect((await reply).text).toContain(expectedReply);
  } finally {
    releaseQueryHandler.resolve();
    releaseLookup.resolve();
    await closeCoreHarness(harness);
  }
}

for (const [text, label, expectedReply] of [
  ["/goal", "goal-empty", "当前会话没有原生目标"],
  ["/goal budget", "goal-budget", "当前会话没有原生目标"],
  ["/watch final-control-query-missing-watch", "watch-id", "当前聊天中没有这个 watch"],
] as const) {
  test(`${text} read-only query bypasses held authorization and retains its normal reply`, async () => {
    await assertReadControlQuerySurvivesCancel(text, label, expectedReply);
  }, 15_000);
}
