import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ChatTarget,
  CommitResult,
  CommitWorker,
  CommitWorkerOptions,
  InboundMessage,
} from "../src/contracts.ts";
import { recvIngressSourceId, type IngressJobRecord } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { BindingRecord, TaskRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  FakeWorktrees,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const HEAD_A = "a".repeat(40);
const HEAD_B = "b".repeat(40);
const COMMIT_RESULT: CommitResult = { exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_B };

async function waitUntil(
  check: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 4_000,
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

async function bindWorkspace(
  harness: CoreHarness,
  target: ChatTarget,
  cwd: string,
  pinned = true,
): Promise<void> {
  await withStore(harness, store => {
    const binding: BindingRecord = {
      botUid: harness.bot.uid,
      ...target,
      cwd,
      sessionId: `ingress-safety-${target.channelType}-${target.channelId}`,
      sessionFile: null,
      cwdPinned: pinned,
    };
    store.ensureBinding(binding);
  });
}

function ownerMessage(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
  overrides: Partial<InboundMessage> = {},
): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [harness.bot.uid],
    ...overrides,
  });
}

function registerFixtureThread(transport: FakeTransport, groupId: string, shortId: string): void {
  const channelId = `${groupId}____${shortId}`;
  transport.setThreads(groupId, [{
    shortId,
    groupId,
    channelId,
    name: "Ingress safety fixture thread",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-04T00:00:00.000Z",
    updatedAt: "2026-11-04T00:00:00.000Z",
  }]);
}

async function readTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
): Promise<TaskRecord | null> {
  return withStore(harness, store => store.getTaskByMessageId(harness.bot.uid, target, messageId));
}

async function readIngressJob(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
): Promise<IngressJobRecord | null> {
  return withStore(harness, store => store.getIngressJobBySource({
    botUid: harness.bot.uid,
    sourceKind: "recv",
    sourceId: recvIngressSourceId(target.channelType, target.channelId, messageId),
  }));
}

test("opening a legacy chats table preserves its row and adds nullable session provenance", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-legacy-chat-"));
  const stateDir = join(root, "state");
  const botUid = "bot-ingress-legacy-chat";
  const target = { channelType: 1 as const, channelId: "dm-ingress-legacy-chat" };
  const cwd = join(root, "legacy-workspace");

  try {
    await mkdir(stateDir, { recursive: true });
    const legacyDb = new Database(join(stateDir, "channel.sqlite"));
    try {
      legacyDb.exec(`
        CREATE TABLE chats (
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
          channel_id TEXT NOT NULL,
          session_id TEXT NOT NULL,
          cwd TEXT NOT NULL,
          session_file TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(bot_uid, channel_type, channel_id)
        )
      `);
      legacyDb.query(`
        INSERT INTO chats (
          bot_uid, channel_type, channel_id, session_id, cwd, session_file, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(botUid, target.channelType, target.channelId, "legacy-session", cwd, null, 1, 1);
    } finally {
      legacyDb.close();
    }

    const migrated = await openChannelStore(stateDir);
    try {
      const migratedBinding = migrated.getBinding(botUid, target);
      expect(migratedBinding).toMatchObject({
        botUid,
        ...target,
        sessionId: "legacy-session",
        cwd,
        sessionFile: null,
        sessionSwitchedAt: null,
      });
      expect(migratedBinding).not.toHaveProperty("cwdPinned");
      expect(migratedBinding).not.toHaveProperty("sessionInitialization");
    } finally {
      migrated.close();
    }

    const migratedDb = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    try {
      const columns = migratedDb.query("PRAGMA table_info(chats)").all() as Array<{
        name: string;
        type: string;
        notnull: number;
      }>;
      expect(columns.find(({ name }) => name === "session_initialization_json"))
        .toMatchObject({ type: "TEXT", notnull: 0 });
      const row = migratedDb.query(`
        SELECT session_initialization_json FROM chats
        WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
      `).get(botUid, target.channelType, target.channelId) as {
        session_initialization_json: string | null;
      } | null;
      expect(row).toEqual({ session_initialization_json: null });
    } finally {
      migratedDb.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

async function settleIngress(harness: CoreHarness): Promise<void> {
  const service = harness.service as unknown as { ingress: { settle(): Promise<void> } };
  await service.ingress.settle();
}

async function waitForTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  predicate: (task: TaskRecord | null) => boolean,
  label: string,
): Promise<TaskRecord | null> {
  let task: TaskRecord | null = null;
  await waitUntil(async () => {
    task = await readTask(harness, target, messageId);
    return predicate(task);
  }, label);
  return task;
}

class CommitEffectWorkerFactory extends FakeWorkerFactory {
  createCalls = 0;
  runCalls = 0;

  override async createCommit(options: CommitWorkerOptions): Promise<CommitWorker> {
    const index = this.createCalls++;
    options.onSpawn({
      pid: 920_000 + index,
      pgid: 920_000 + index,
      identity: `ingress-safety-commit-${index}`,
      cwd: options.cwd,
    });
    return {
      run: async () => {
        this.runCalls += 1;
        await writeFile(join(options.cwd, "native-commit-effect"), "commit ran", "utf8");
        return COMMIT_RESULT;
      },
      stop: async () => true,
    };
  }
}

type CommitWorkspaceResolver = (
  target: ChatTarget,
  binding: BindingRecord,
) => Promise<{ root: string } | { error: string }>;

function holdCommitWorkspaceResolution(
  harness: CoreHarness,
  root: string,
): { entered: Promise<void>; release: () => void; restore: () => void } {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const core = harness.service as unknown as { resolveCommitWorkspace: CommitWorkspaceResolver };
  const original = core.resolveCommitWorkspace.bind(harness.service);
  let held = false;
  core.resolveCommitWorkspace = async (target, binding) => {
    if (!held) {
      held = true;
      entered.resolve();
      await release.promise;
    }
    if (binding.cwd !== root) throw new Error("commit gate target changed unexpectedly");
    return original(target, binding);
  };
  return {
    entered: entered.promise,
    release: () => release.resolve(),
    restore: () => { core.resolveCommitWorkspace = original; },
  };
}

async function createGitRepository(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
  const process = Bun.spawn(["git", "init", "-q", path], { stdout: "ignore", stderr: "ignore" });
  const exitCode = await process.exited;
  if (exitCode !== 0) throw new Error(`git init failed for ${path}: ${exitCode}`);
}

test("/cancel cannot let a delayed /commit admission create a native task afterward", async () => {
  const workers = new CommitEffectWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-safety-cancel-commit" };
  const repo = join(harness.root, "cancelled-commit-repo");
  await createGitRepository(repo);
  await bindWorkspace(harness, target, repo);
  const held = holdCommitWorkspaceResolution(harness, repo);
  const commit = ownerMessage(harness, target, "safety-cancel-commit", "/commit");

  try {
    const commitReceipt = harness.service.receive(commit);
    await commitReceipt.ackReady;
    await held.entered;
    expect(await readIngressJob(harness, target, commit.messageId)).toMatchObject({ state: "accepted" });

    const cancelReceipt = harness.service.receive(ownerMessage(
      harness,
      target,
      "safety-cancel-commit-control",
      "/cancel",
    ));
    await cancelReceipt.ackReady;
    await waitUntil(
      () => harness.transport.sent.some(message =>
        message.target.channelId === target.channelId && message.text.includes("当前任务已取消")),
      "cancel completion while commit workspace resolution is held",
    );
    expect(await readIngressJob(harness, target, commit.messageId)).toMatchObject({ state: "cancelled" });

    held.release();
    await settleIngress(harness);
    expect(await readTask(harness, target, commit.messageId)).toBeNull();
    expect(workers.createCalls).toBe(0);
    expect(workers.runCalls).toBe(0);
    expect(await readFile(join(repo, "native-commit-effect")).catch(() => null)).toBeNull();
  } finally {
    held.release();
    held.restore();
    await closeCoreHarness(harness);
  }
});

test("/queue admission cancelled while task preparation is held cannot create a task later", async () => {
  const harness = await createCoreHarness();
  const target = { channelType: 1 as const, channelId: "dm-ingress-safety-cancel-queue" };
  const queue = ownerMessage(harness, target, "safety-cancel-queue", "/queue late queued work");
  const hold = holdIngressTaskPreparation(harness.service, queue.messageId);

  try {
    const queueReceipt = harness.service.receive(queue);
    await queueReceipt.ackReady;
    await hold.entered;
    expect(await readIngressJob(harness, target, queue.messageId)).toMatchObject({ state: "accepted" });

    const cancel = harness.service.receive(ownerMessage(harness, target, "safety-cancel-queue-control", "/cancel"));
    await cancel.ackReady;
    await waitUntil(
      () => harness.transport.sent.some(message =>
        message.target.channelId === target.channelId && message.text.includes("当前任务已取消")),
      "cancel completion while /queue admission is held",
    );
    expect(await readIngressJob(harness, target, queue.messageId)).toMatchObject({ state: "cancelled" });

    hold.release();
    await settleIngress(harness);
    expect(await readTask(harness, target, queue.messageId)).toBeNull();
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    hold.release();
    hold.restore();
    await closeCoreHarness(harness);
  }
});

test("/commit stop received during workspace resolution prevents the later native commit", async () => {
  const workers = new CommitEffectWorkerFactory();
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-safety-stop-commit" };
  const repo = join(harness.root, "stopped-commit-repo");
  await createGitRepository(repo);
  await bindWorkspace(harness, target, repo);
  const held = holdCommitWorkspaceResolution(harness, repo);
  const commit = ownerMessage(harness, target, "safety-stop-commit", "/commit");

  try {
    const commitReceipt = harness.service.receive(commit);
    await commitReceipt.ackReady;
    await held.entered;

    const stop = harness.service.receive(ownerMessage(harness, target, "safety-stop-commit-control", "/commit stop"));
    await stop.ackReady;
    await waitUntil(
      () => harness.transport.sent.some(message =>
        message.target.channelId === target.channelId && message.text.includes("已停止尚未开始的提交；没有 Git 副作用。")),
      "commit stop result before admission finishes",
    );

    held.release();
    await settleIngress(harness);
    expect(await readTask(harness, target, commit.messageId)).toBeNull();
    expect(workers.createCalls).toBe(0);
    expect(workers.runCalls).toBe(0);
    expect(await readFile(join(repo, "native-commit-effect")).catch(() => null)).toBeNull();
  } finally {
    held.release();
    held.restore();
    await closeCoreHarness(harness);
  }
});

test("an ACKed /commit stop killed before handler effects cancels the pending commit on restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-commit-stop-crash-"));
  const stateDir = join(root, "state");
  const bot = { uid: "bot-ingress-commit-stop-crash", name: "Ingress Commit Stop Crash Bot" };
  const target = { channelType: 1 as const, channelId: "dm-ingress-commit-stop-crash" };
  const repo = join(root, "pending-commit-repo");
  const commitMessageId = "safety-commit-stop-pending-commit";
  const stopMessageId = "safety-commit-stop-command";
  const workspaceHeldPath = join(root, "commit-workspace-held");
  const stopHandlerGatePath = join(root, "commit-stop-handler-gate");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let boundaryDb: Database | undefined;
  let restarted: CoreHarness | undefined;

  try {
    await createGitRepository(repo);
    const setup = await openChannelStore(stateDir);
    setup.ensureBinding({
      botUid: bot.uid,
      ...target,
      cwd: repo,
      sessionId: `ingress-safety-${target.channelType}-${target.channelId}`,
      sessionFile: null,
      cwdPinned: true,
    });
    setup.close();

    // The separate process permits a deterministic SIGKILL between durable admission and stop-handler dispatch.
    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
      import { writeFileSync } from "node:fs";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = ${JSON.stringify(target)};
      const workspaceHeldPath = join(root, "commit-workspace-held");
      const stopHandlerGatePath = join(root, "commit-stop-handler-gate");
      const workspaceStarted = Promise.withResolvers();
      const workspaceRelease = Promise.withResolvers();
      const service = await createCoreHarness({ root, bot, workers: new FakeWorkerFactory() });
      const core = service.service;
      const originalResolveWorkspace = core.resolveCommitWorkspace.bind(core);
      core.resolveCommitWorkspace = async (commitTarget, binding) => {
        writeFileSync(workspaceHeldPath, "commit workspace resolution is held");
        workspaceStarted.resolve();
        await workspaceRelease.promise;
        return originalResolveWorkspace(commitTarget, binding);
      };
      const commit = core.receive(inbound({
        ...target,
        messageId: "${commitMessageId}",
        text: "/commit",
      }));
      await commit.ackReady;
      await workspaceStarted.promise;

      const originalHandleCommit = core.handleCommit.bind(core);
      const blocked = new Int32Array(new SharedArrayBuffer(4));
      core.handleCommit = async (message, argument, ingressJobId) => {
        if (argument === "stop") {
          writeFileSync(stopHandlerGatePath, "stop accepted before handler effects");
          Atomics.wait(blocked, 0, 0);
        }
        return originalHandleCommit(message, argument, ingressJobId);
      };
      const stop = core.receive(inbound({
        ...target,
        messageId: "${stopMessageId}",
        text: "/commit stop",
      }));
      await stop.ackReady;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    await waitUntil(async () => (
      child!.exitCode !== null || await fileExists(stopHandlerGatePath)
    ), "ACKed commit stop held before handler effects", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`commit-stop crash fixture exited early: ${await childStderr}`);
    }
    expect(await fileExists(workspaceHeldPath)).toBe(true);

    boundaryDb = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    const ingressRow = (messageId: string): {
      id: number;
      state: string;
      ack_ready: number;
      operation_checkpoint_json: string | null;
    } | null => boundaryDb!.query(
      "SELECT id, state, ack_ready, operation_checkpoint_json FROM ingress_jobs WHERE message_id = ? AND channel_type = ? AND channel_id = ?",
    ).get(messageId, target.channelType, target.channelId) as {
      id: number;
      state: string;
      ack_ready: number;
      operation_checkpoint_json: string | null;
    } | null;
    const taskRow = (): { id: string } | null =>
      boundaryDb!.query("SELECT id FROM tasks WHERE message_id = ?")
        .get(commitMessageId) as { id: string } | null;
    const stopRow = ingressRow(stopMessageId);
    const pendingCommit = ingressRow(commitMessageId);
    expect(stopRow).toMatchObject({
      state: "accepted",
      ack_ready: 1,
    });
    expect(stopRow?.operation_checkpoint_json).not.toBeNull();
    expect(pendingCommit).not.toBeNull();
    expect(JSON.parse(stopRow!.operation_checkpoint_json!)).toMatchObject({
      phase: "commit-stop-intent",
      cancelledCommitIngressJobIds: [pendingCommit!.id],
      cancelledCommitTaskIds: [],
      stoppedCommitTaskIds: [],
    });
    expect(pendingCommit).toMatchObject({ state: "cancelled" });
    expect(taskRow()).toBeNull();
    boundaryDb.close();
    boundaryDb = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new CommitEffectWorkerFactory();
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await settleIngress(restarted);
    expect(await readIngressJob(restarted, target, commitMessageId)).toMatchObject({ state: "cancelled" });
    expect(await readTask(restarted, target, commitMessageId)).toBeNull();
    expect(restartWorkers.createCalls).toBe(0);
    expect(restartWorkers.runCalls).toBe(0);
    expect(await readFile(join(repo, "native-commit-effect")).catch(() => null)).toBeNull();
  } finally {
    boundaryDb?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

type PrepareIngressTask = (
  message: InboundMessage,
  text: string,
  ingressJobId: number,
) => Promise<unknown>;

function holdIngressTaskPreparation(
  service: CoreHarness["service"],
  messageId: string,
): { entered: Promise<void>; release: () => void; restore: () => void } {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const core = service as unknown as { prepareIngressTask: PrepareIngressTask };
  const original = core.prepareIngressTask.bind(service);
  let held = false;
  core.prepareIngressTask = async (message, text, ingressJobId) => {
    if (message.messageId === messageId && !held) {
      held = true;
      entered.resolve();
      await release.promise;
    }
    return original(message, text, ingressJobId);
  };
  return {
    entered: entered.promise,
    release: () => release.resolve(),
    restore: () => { core.prepareIngressTask = original; },
  };
}

function registerMergeRecorder(port: FakeWorktrees | undefined, effects: string[]): void {
  if (!port) throw new Error("expected fake worktree port");
  const merge = port.merge.bind(port);
  port.merge = async request => {
    effects.push("merge");
    return merge(request);
  };
}

function heldPrompt(
  onStart: (text: string) => void,
  release: Promise<void>,
): (context: { text: string; onStop(callback: () => void): void }) => Promise<void> {
  return async ({ text, onStop }) => {
    onStart(text);
    const stopped = Promise.withResolvers<void>();
    onStop(stopped.resolve);
    await Promise.race([release, stopped.promise]);
  };
}

function threadTarget(shortId: string): ChatTarget & { channelType: 5 } {
  return { channelType: 5, channelId: `group-allowed____${shortId}` };
}

test("an earlier /queue initializes an unbound pinned session before a waiting /thread merge", async () => {
  const effects: string[] = [];
  const taskStarted = Promise.withResolvers<void>();
  const releaseTask = Promise.withResolvers<void>();
  const workers = new FakeWorkerFactory(heldPrompt(text => {
    if (text === "queued work before merge") {
      effects.push("queue-task");
      taskStarted.resolve();
    }
  }, releaseTask.promise));
  const harness = await createCoreHarness({ workers });
  const target = threadTarget("ingress-safety-queue-before-merge");
  registerFixtureThread(harness.transport, "group-allowed", "ingress-safety-queue-before-merge");
  const cwd = join(harness.root, "queue-before-merge-workspace");
  await mkdir(cwd, { recursive: true });
  await bindWorkspace(harness, target, cwd);
  const initialBinding = await withStore(harness, store => store.getBinding(harness.bot.uid, target));
  if (initialBinding === null) throw new Error("expected the unbound pinned fixture binding");
  expect(initialBinding).toMatchObject({ cwd, cwdPinned: true, sessionFile: null });
  registerMergeRecorder(harness.worktrees, effects);
  const queue = ownerMessage(harness, target, "safety-queue-before-merge", "/queue queued work before merge");
  const hold = holdIngressTaskPreparation(harness.service, queue.messageId);

  try {
    const queued = harness.service.receive(queue);
    await queued.ackReady;
    await hold.entered;
    expect(await readTask(harness, target, queue.messageId)).toBeNull();

    const merge = harness.service.receive(ownerMessage(
      harness,
      target,
      "safety-merge-after-queue",
      "/thread merge",
    ));
    await merge.ackReady;
    const queueJobBeforePreparation = await readIngressJob(harness, target, queue.messageId);
    const mergeJobBeforeTask = await readIngressJob(harness, target, "safety-merge-after-queue");
    if (queueJobBeforePreparation === null || mergeJobBeforeTask === null) {
      throw new Error("queue and merge must both have durable ingress identities");
    }
    expect(queueJobBeforePreparation.id).toBeLessThan(mergeJobBeforeTask.id);
    expect(mergeJobBeforeTask).toMatchObject({
      state: "accepted",
      routeSnapshot: {
        sessionGeneration: 0,
        binding: {
          sessionId: initialBinding.sessionId,
          cwd,
          sessionFile: null,
        },
        barrierJobId: null,
      },
    });

    hold.release();
    await waitUntil(() => effects.length > 0, "the first queued-task or merge effect");
    expect(effects[0]).toBe("queue-task");
    await taskStarted.promise;
    expect(effects).not.toContain("merge");
    const queueTask = await readTask(harness, target, queue.messageId);
    const queueJobAtStart = await readIngressJob(harness, target, queue.messageId);
    const bindingAfterQueueStart = await withStore(harness, store => store.getBinding(harness.bot.uid, target));
    if (queueTask === null || queueJobAtStart === null) {
      throw new Error("the earlier queue must hand off its exact task before the merge waits");
    }
    expect(queueTask.state).toBe("running");
    expect(queueJobAtStart).toMatchObject({
      state: "handed-off",
      consumerKind: "task",
      consumerId: queueTask.id,
    });
    expect(queueJobAtStart.id).toBeLessThan(mergeJobBeforeTask.id);
    expect(bindingAfterQueueStart).toMatchObject({ cwd, cwdPinned: true });
    expect(bindingAfterQueueStart?.sessionId).not.toBe(initialBinding.sessionId);
    expect(typeof bindingAfterQueueStart?.sessionFile).toBe("string");
    expect(bindingAfterQueueStart?.sessionSwitchedAt ?? null).toBe(initialBinding.sessionSwitchedAt ?? null);
    await waitUntil(async () => {
      const job = await readIngressJob(harness, target, "safety-merge-after-queue");
      return job?.operationCheckpoint?.kind === "command"
        && job.operationCheckpoint.phase === "waiting-session";
    }, "the merge's persisted waiting-session checkpoint while the earlier task is active");
    const mergeWaitingBeforeTaskRelease = await readIngressJob(harness, target, "safety-merge-after-queue");
    if (mergeWaitingBeforeTaskRelease === null) throw new Error("waiting merge ingress disappeared");
    expect(mergeWaitingBeforeTaskRelease).toMatchObject({
      id: mergeJobBeforeTask.id,
      state: "accepted",
      operationCheckpoint: {
        kind: "command",
        action: "thread",
        phase: "waiting-session",
        expectedBinding: {
          sessionId: initialBinding.sessionId,
          cwd,
          sessionFile: null,
          cwdPinned: true,
          sessionSwitchedAt: null,
        },
      },
      routeSnapshot: {
        binding: {
          sessionId: initialBinding.sessionId,
          cwd,
          sessionFile: null,
          cwdPinned: true,
          sessionSwitchedAt: null,
        },
      },
    });

    releaseTask.resolve();
    const finishedQueueTask = await waitForTask(
      harness,
      target,
      queue.messageId,
      task => task?.state === "completed",
      "the earlier queue task to settle after worker release",
    );
    expect(finishedQueueTask?.state).toBe("completed");
    await settleIngress(harness);
    try {
      await waitUntil(() => effects.includes("merge"), "merge after the earlier queue task settles");
    } catch (error) {
      const mergeJob = await readIngressJob(harness, target, "safety-merge-after-queue");
      const replies = harness.transport.sent
        .filter(message => message.target.channelId === target.channelId)
        .map(({ text }) => text);
      const db = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
      let provenanceDiagnostic: unknown;
      try {
        const raw = (sql: string, ...values: (string | number)[]) =>
          db.query(sql).get(...values) as Record<string, unknown> | null;
        const chat = raw(
          `SELECT bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned,
                  session_switched_at, session_initialization_json
           FROM chats WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?`,
          harness.bot.uid,
          target.channelType,
          target.channelId,
        );
        const task = raw(
          `SELECT id, bot_uid, channel_type, channel_id, message_id, session_id, cwd, state FROM tasks
           WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
          harness.bot.uid,
          target.channelType,
          target.channelId,
          queue.messageId,
        );
        const queueIngress = raw(
          `SELECT id, bot_uid, channel_type, channel_id, message_id, source_id, lane, state,
                  consumer_kind, consumer_id, route_snapshot_json
           FROM ingress_jobs WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
          harness.bot.uid,
          target.channelType,
          target.channelId,
          queue.messageId,
        );
        const mergeIngress = raw(
          `SELECT id, bot_uid, channel_type, channel_id, message_id, source_id, state, consumer_kind,
                  consumer_id, route_snapshot_json, operation_checkpoint_json
           FROM ingress_jobs WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
          harness.bot.uid,
          target.channelType,
          target.channelId,
          "safety-merge-after-queue",
        );
        const json = (value: unknown): unknown =>
          typeof value === "string" ? JSON.parse(value) as unknown : null;
        const record = (value: unknown): Record<string, unknown> | null =>
          value !== null && typeof value === "object" && !Array.isArray(value)
            ? value as Record<string, unknown>
            : null;
        const sameSnapshot = (left: unknown, right: unknown): boolean => {
          const a = record(left);
          const b = record(right);
          return a !== null && b !== null
            && a.sessionId === b.sessionId
            && a.cwd === b.cwd
            && a.sessionFile === b.sessionFile
            && a.cwdPinned === b.cwdPinned
            && a.sessionSwitchedAt === b.sessionSwitchedAt;
        };
        const expected = mergeWaitingBeforeTaskRelease.routeSnapshot?.binding ?? null;
        const acceptedRoute = mergeWaitingBeforeTaskRelease.routeSnapshot;
        const sourceRoute = record(json(queueIngress?.route_snapshot_json));
        const sourceBinding = sourceRoute?.binding;
        const provenance = record(chat?.session_initialization_json === null
          ? null
          : json(chat?.session_initialization_json));
        const dbTaskId = task?.id === undefined ? null : String(task.id);
        const exactTaskInitializationChecks = {
          jobBelongsToBot: mergeWaitingBeforeTaskRelease.botUid === harness.bot.uid,
          expectedBindingExists: expected !== null,
          expectedBindingIsPinnedAndUninitialized: expected?.cwdPinned === true
            && expected.sessionFile === null
            && expected.sessionSwitchedAt !== undefined,
          acceptedRouteBindingMatchesExpected: sameSnapshot(acceptedRoute?.binding, expected),
          currentChatIdentityMatches: chat !== null
            && chat.bot_uid === harness.bot.uid
            && chat.channel_type === target.channelType
            && chat.channel_id === target.channelId,
          currentBindingMatchesInitialCwdAndPin: chat !== null
            && expected !== null
            && chat.cwd === expected.cwd
            && chat.cwd_pinned === 1,
          currentSessionInitializedWithStableGeneration: typeof chat?.session_file === "string"
            && chat.session_file.length > 0
            && chat.session_id !== expected?.sessionId
            && (chat.session_switched_at ?? null) === expected?.sessionSwitchedAt,
          provenanceExistsAndMatchesInitialBinding: provenance !== null
            && provenance.fromSessionId === expected?.sessionId
            && provenance.fromCwd === expected?.cwd
            && provenance.fromSessionFile === null
            && provenance.fromCwdPinned === true
            && provenance.fromSessionSwitchedAt === expected?.sessionSwitchedAt,
          provenancePointsToLinkedTask: task !== null
            && provenance?.taskId === dbTaskId
            && task.message_id === queue.messageId
            && task.bot_uid === harness.bot.uid
            && task.channel_type === target.channelType
            && task.channel_id === target.channelId
            && task.cwd === expected?.cwd
            && (task.session_id === expected?.sessionId || task.session_id === chat?.session_id),
          sourceIsEarlierOrderedTaskHandoff: queueIngress !== null
            && Number(queueIngress.id) < mergeWaitingBeforeTaskRelease.id
            && queueIngress.bot_uid === harness.bot.uid
            && queueIngress.channel_type === target.channelType
            && queueIngress.channel_id === target.channelId
            && queueIngress.message_id === queue.messageId
            && queueIngress.lane === "ordered"
            && queueIngress.state === "handed-off"
            && queueIngress.consumer_kind === "task"
            && queueIngress.consumer_id === dbTaskId,
          sourceRouteHasExactGenerationAndBarrier: sourceRoute !== null
            && sourceRoute.sessionGeneration === acceptedRoute?.sessionGeneration
            && sourceRoute.barrierJobId === acceptedRoute?.barrierJobId,
          sourceRouteBindingMatchesExpected: sameSnapshot(sourceBinding, expected),
        };
        const sameBindingStateChecks = {
          sessionIdEqual: chat?.session_id === initialBinding.sessionId,
          cwdEqual: chat?.cwd === initialBinding.cwd,
          sessionFileEqual: chat?.session_file === initialBinding.sessionFile,
          pinnedEqual: (chat?.cwd_pinned === 1) === (initialBinding.cwdPinned === true),
          switchedAtEqual: (chat?.session_switched_at ?? null)
            === (initialBinding.sessionSwitchedAt ?? null),
          initializationEqual: provenance === null
            && (initialBinding.sessionInitialization ?? null) === null,
        };
        const predicateResult = (checks: Record<string, boolean>) => ({
          result: Object.values(checks).every(Boolean),
          failed: Object.entries(checks)
            .filter(([, passed]) => !passed)
            .map(([name]) => name),
          checks,
        });
        provenanceDiagnostic = {
          acceptedMergeSnapshot: acceptedRoute,
          acceptedMergeCheckpoint: mergeWaitingBeforeTaskRelease.operationCheckpoint,
          currentChat: chat,
          provenance,
          linkedTask: task,
          queueIngress,
          mergeIngress,
          predicates: {
            hasExactTaskSessionInitialization: predicateResult(exactTaskInitializationChecks),
            sameBindingStateAgainstFrozenInitial: predicateResult(sameBindingStateChecks),
          },
        };
      } finally {
        db.close();
      }
      throw new Error(
        `${error instanceof Error ? error.message : String(error)}; `
        + `completedTask=${JSON.stringify(finishedQueueTask)}; `
        + `mergeIngress=${JSON.stringify(mergeJob)}; `
        + `replies=${JSON.stringify(replies)}; `
        + `provenanceDiagnostic=${JSON.stringify(provenanceDiagnostic)}`,
      );
    }
    expect(effects.indexOf("queue-task")).toBeLessThan(effects.indexOf("merge"));
  } finally {
    hold.release();
    releaseTask.resolve();
    hold.restore();
    await closeCoreHarness(harness);
  }
});

test("a pending merge refuses a same-cwd /new session generation change", async () => {
  const workers = new FakeWorkerFactory();
  const newCreateStarted = Promise.withResolvers<void>();
  const releaseNewCreate = Promise.withResolvers<void>();
  workers.beforeCreate = async () => {
    newCreateStarted.resolve();
    await releaseNewCreate.promise;
  };
  const harness = await createCoreHarness({ workers });
  const target = threadTarget("ingress-safety-new-before-merge");
  registerFixtureThread(harness.transport, "group-allowed", "ingress-safety-new-before-merge");
  const cwd = join(harness.root, "new-before-merge-workspace");
  await mkdir(cwd, { recursive: true });
  await bindWorkspace(harness, target, cwd);
  const initialBinding = await withStore(harness, store => store.getBinding(harness.bot.uid, target));
  if (initialBinding === null) throw new Error("expected the pinned binding before /new");
  const effects: string[] = [];
  registerMergeRecorder(harness.worktrees, effects);
  const switchMessage = ownerMessage(harness, target, "safety-new-before-merge", "/new");
  const mergeMessage = ownerMessage(harness, target, "safety-merge-after-new", "/thread merge");

  try {
    const switching = harness.service.receive(switchMessage);
    await switching.ackReady;
    await newCreateStarted.promise;
    const switchJob = await readIngressJob(harness, target, switchMessage.messageId);
    if (switchJob === null) throw new Error("/new must have a durable ingress job before worker creation");
    expect(switchJob).toMatchObject({
      state: "dispatch-intent",
      operationCheckpoint: {
        kind: "command",
        action: "new",
        phase: "dispatch-intent",
      },
    });

    const merging = harness.service.receive(mergeMessage);
    await merging.ackReady;
    const mergeBeforeSwitch = await readIngressJob(harness, target, mergeMessage.messageId);
    if (mergeBeforeSwitch === null) throw new Error("merge must be durably admitted while /new is held");
    expect(mergeBeforeSwitch).toMatchObject({
      state: "accepted",
      routeSnapshot: {
        sessionGeneration: switchJob.id,
        binding: {
          sessionId: initialBinding.sessionId,
          cwd,
          sessionFile: null,
        },
        barrierJobId: switchJob.id,
      },
    });
    expect(harness.worktrees?.merges).toHaveLength(0);

    releaseNewCreate.resolve();
    await waitUntil(async () => (
      (await readIngressJob(harness, target, switchMessage.messageId))?.state === "handed-off"
    ), "same-cwd /new handoff");
    const switchedBinding = await withStore(harness, store => store.getBinding(harness.bot.uid, target));
    expect(switchedBinding).toMatchObject({ cwd, cwdPinned: true });
    expect(switchedBinding?.sessionId).not.toBe(initialBinding.sessionId);
    expect(typeof switchedBinding?.sessionFile).toBe("string");
    expect(typeof switchedBinding?.sessionSwitchedAt).toBe("number");
    expect(switchedBinding?.sessionSwitchedAt).not.toBe(initialBinding.sessionSwitchedAt);

    const refused = (): boolean => harness.transport.sent.some(message =>
      message.target.channelId === target.channelId
      && /未接收任务|未执行合并/u.test(message.text),
    );
    await waitUntil(
      () => refused() || effects.includes("merge"),
      "merge refusal or an unexpected merge side effect after /new",
    );
    expect(effects).not.toContain("merge");
    expect(harness.worktrees?.merges).toHaveLength(0);
    expect(refused()).toBe(true);
    const finalMergeJob = await readIngressJob(harness, target, mergeMessage.messageId);
    if (finalMergeJob === null) throw new Error("merge refusal must have a durable ingress outcome");
    expect(finalMergeJob.state).toBe("handed-off");
    expect(finalMergeJob.consumerKind === "outbox" || finalMergeJob.consumerKind === "operation").toBe(true);
  } finally {
    releaseNewCreate.resolve();
    await closeCoreHarness(harness);
  }
});

test("a task accepted before a waiting merge resumes before that merge after restart", async () => {
  const effects: string[] = [];
  const firstTaskStarted = Promise.withResolvers<void>();
  const releaseFirstTask = Promise.withResolvers<void>();
  const firstWorkers = new FakeWorkerFactory(heldPrompt(text => {
    if (text === "task accepted before merge") firstTaskStarted.resolve();
  }, releaseFirstTask.promise));
  const first = await createCoreHarness({ workers: firstWorkers });
  const target = threadTarget("ingress-safety-restart-order");
  registerFixtureThread(first.transport, "group-allowed", "ingress-safety-restart-order");
  const cwd = join(first.root, "restart-order-workspace");
  await mkdir(cwd, { recursive: true });
  await bindWorkspace(first, target, cwd);
  const queue = ownerMessage(first, target, "safety-task-before-merge", "/queue task accepted before merge");
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const queueReceipt = first.service.receive(queue);
    await queueReceipt.ackReady;
    await firstTaskStarted.promise;
    const queuedTask = await waitForTask(
      first,
      target,
      queue.messageId,
      task => task?.state === "running",
      "the task accepted before the merge",
    );
    expect(queuedTask?.text).toBe("task accepted before merge");

    const mergeMessage = ownerMessage(first, target, "safety-waiting-merge", "/thread merge");
    const mergeReceipt = first.service.receive(mergeMessage);
    await mergeReceipt.ackReady;
    await waitUntil(async () => {
      const job = await readIngressJob(first, target, mergeMessage.messageId);
      return job?.state === "accepted"
        && job.operationCheckpoint?.kind === "command"
        && job.operationCheckpoint.phase !== "admitted";
    }, "the accepted merge reservation while the earlier task is active");
    expect(first.worktrees?.merges).toHaveLength(0);

    await first.service.stop();
    firstStopped = true;
    releaseFirstTask.resolve();

    const releaseRecoveredTask = Promise.withResolvers<void>();
    const workers = new FakeWorkerFactory(heldPrompt(text => {
      if (text === "/retry") effects.push("older-task-resumed");
    }, releaseRecoveredTask.promise));
    workers.recovered = true;
    const transport = new FakeTransport({ bot: first.bot });
    registerFixtureThread(transport, "group-allowed", "ingress-safety-restart-order");
    const recoveredWorktrees = new FakeWorktrees(first.root);
    registerMergeRecorder(recoveredWorktrees, effects);
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      workers,
      worktrees: recoveredWorktrees,
    });

    await waitUntil(() => effects.length > 0, "the first recovered task or merge effect");
    expect(effects[0]).toBe("older-task-resumed");
    expect(effects).not.toContain("merge");

    // Let the resumed task settle so the accepted destructive command can acquire its lease.
    releaseRecoveredTask.resolve();
    await waitUntil(() => effects.includes("merge"), "waiting merge after recovered task settlement");
    expect(effects.indexOf("older-task-resumed")).toBeLessThan(effects.indexOf("merge"));
  } finally {
    releaseFirstTask.resolve();
    if (!firstStopped) await first.service.stop().catch(() => {});
    if (restarted) await closeCoreHarness(restarted, { remove: true });
    else await closeCoreHarness(first, { remove: true });
  }
});

test("a task that initializes an unbound pinned thread session stays ahead of its waiting merge after SIGKILL", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-session-merge-crash-")));
  const bot = { uid: "bot-ingress-session-merge-crash", name: "Ingress Session Merge Crash Bot" };
  const shortId = "ingress-safety-session-merge-crash";
  const target = threadTarget(shortId);
  const stateDir = join(root, "state");
  const cwd = join(root, "initial-pinned-thread-workspace");
  const oldSessionId = "initial-pinned-unbound-session";
  const queueMessageId = "safety-session-merge-queue";
  const mergeMessageId = "safety-session-merge";
  const boundaryPath = join(root, "session-merge-boundary-ready");
  const debugPath = join(root, "session-merge-child-state.json");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let observer: ChannelStore | undefined;
  let restarted: CoreHarness | undefined;
  let queueJobId: number | undefined;
  let mergeJobId: number | undefined;

  try {
    await mkdir(cwd, { recursive: true });
    const setup = await openChannelStore(stateDir);
    setup.ensureBinding({
      botUid: bot.uid,
      ...target,
      cwd,
      sessionId: oldSessionId,
      sessionFile: null,
      cwdPinned: true,
      sessionSwitchedAt: null,
    });
    setup.close();

    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const ingressTypesUrl = new URL("../src/inbound/ingress-types.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeTransport, FakeWorkerFactory, FakeWorktrees, inbound } from ${JSON.stringify(fixtureUrl)};
      import { recvIngressSourceId } from ${JSON.stringify(ingressTypesUrl)};
      import { writeFile } from "node:fs/promises";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = { channelType: 5, channelId: "group-allowed____${shortId}" };
      const queueMessageId = ${JSON.stringify(queueMessageId)};
      const mergeMessageId = ${JSON.stringify(mergeMessageId)};
      const boundaryPath = join(root, "session-merge-boundary-ready");
      const debugPath = join(root, "session-merge-child-state.json");
      const prepareEntered = Promise.withResolvers();
      const releasePreparation = Promise.withResolvers();
      const taskStarted = Promise.withResolvers();
      const ingressJob = messageId => store.getIngressJobBySource({
        botUid: bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, messageId),
      });
      const debugHistory = [];
      let debugWrite = Promise.resolve();
      const writeDebug = stage => {
        debugWrite = debugWrite.then(async () => {
          const queueJob = ingressJob(queueMessageId);
          const mergeJob = ingressJob(mergeMessageId);
          const task = store.getTaskByMessageId(bot.uid, target, queueMessageId);
          const binding = store.getBinding(bot.uid, target);
          const snapshot = {
            at: Date.now(),
            stage,
            queueIngress: queueJob === null ? null : {
              id: queueJob.id,
              state: queueJob.state,
              lane: queueJob.lane,
              consumerKind: queueJob.consumerKind,
              consumerId: queueJob.consumerId,
              routeSnapshot: queueJob.routeSnapshot,
            },
            mergeIngress: mergeJob === null ? null : {
              id: mergeJob.id,
              state: mergeJob.state,
              lane: mergeJob.lane,
              consumerKind: mergeJob.consumerKind,
              consumerId: mergeJob.consumerId,
              routeSnapshot: mergeJob.routeSnapshot,
              operationCheckpoint: mergeJob.operationCheckpoint,
            },
            task: task === null ? null : {
              id: task.id,
              messageId: task.messageId,
              sessionId: task.sessionId,
              cwd: task.cwd,
              state: task.state,
            },
            binding,
          };
          debugHistory.push(snapshot);
          await writeFile(debugPath, JSON.stringify({
            latest: snapshot,
            history: debugHistory,
          }, null, 2), "utf8");
        });
        return debugWrite;
      };
      const waitSignal = async (promise, label) => {
        let outcome = "pending";
        let rejection;
        promise.then(
          () => { outcome = "fulfilled"; },
          error => { outcome = "rejected"; rejection = error; },
        );
        const deadline = Date.now() + 5_000;
        while (outcome === "pending" && Date.now() < deadline) {
          await writeDebug("waiting:" + label);
          await new Promise(resolve => setTimeout(resolve, 100));
        }
        if (outcome === "pending") {
          await writeDebug("timeout:" + label);
          throw new Error("timed out waiting for " + label);
        }
        if (outcome === "rejected") {
          await writeDebug("rejected:" + label);
          throw rejection;
        }
        await writeDebug("reached:" + label);
      };
      const waitFor = async (check, label) => {
        const deadline = Date.now() + 5_000;
        let nextSnapshot = 0;
        while (!(await check())) {
          if (Date.now() >= deadline) {
            await writeDebug("timeout:" + label);
            throw new Error("timed out waiting for " + label);
          }
          if (Date.now() >= nextSnapshot) {
            await writeDebug("waiting:" + label);
            nextSnapshot = Date.now() + 100;
          }
          await new Promise(resolve => setTimeout(resolve, 10));
        }
        await writeDebug("reached:" + label);
      };
      const transport = new FakeTransport({ bot });
      transport.setThreads("group-allowed", [{
        shortId: "${shortId}",
        groupId: "group-allowed",
        channelId: target.channelId,
        name: "Ingress safety session-merge fixture",
        creatorUid: "owner-1",
        status: 1,
        memberCount: 1,
        messageCount: 0,
        createdAt: "2026-11-04T00:00:00.000Z",
        updatedAt: "2026-11-04T00:00:00.000Z",
      }]);
      const workers = new FakeWorkerFactory(async ({ text, onStop }) => {
        if (text !== "older queued work before merge") return;
        taskStarted.resolve();
        const stopped = Promise.withResolvers();
        onStop(stopped.resolve);
        await stopped.promise;
      });
      const worktrees = new FakeWorktrees(root);
      const harness = await createCoreHarness({ root, bot, transport, workers, worktrees });
      const core = harness.service;
      const store = core.store;
      await writeDebug("service-started");
      const originalPrepare = core.prepareIngressTask.bind(core);
      core.prepareIngressTask = async (message, text, ingressJobId) => {
        if (message.messageId === queueMessageId) {
          await writeDebug("queue-prepare-entered");
          prepareEntered.resolve();
          await releasePreparation.promise;
        }
        return originalPrepare(message, text, ingressJobId);
      };
      const message = (messageId, text) => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [bot.uid],
      });
      const queueReceipt = core.receive(message(queueMessageId, "/queue older queued work before merge"));
      await waitSignal(queueReceipt.ackReady, "queue receipt acknowledgement");
      await waitSignal(prepareEntered.promise, "queue task preparation");
      const mergeReceipt = core.receive(message(mergeMessageId, "/thread merge"));
      await waitSignal(mergeReceipt.ackReady, "merge receipt acknowledgement");
      await writeDebug("merge-ack-ready-before-queue-admission");
      releasePreparation.resolve();
      await writeDebug("queue-preparation-released");
      await waitSignal(taskStarted.promise, "queued worker prompt start");

      await waitFor(() => {
        const queueJob = ingressJob(queueMessageId);
        const mergeJob = ingressJob(mergeMessageId);
        const task = store.getTaskByMessageId(bot.uid, target, queueMessageId);
        const binding = store.getBinding(bot.uid, target);
        return queueJob?.state === "handed-off"
          && queueJob.consumerKind === "task"
          && queueJob.consumerId === task?.id
          && task?.state === "running"
          && binding?.sessionInitialization?.taskId === task.id
          && mergeJob?.state === "accepted"
          && mergeJob.operationCheckpoint?.kind === "command"
          && mergeJob.operationCheckpoint.phase === "waiting-session";
      }, "durable task handoff, provenance, and waiting merge checkpoint");
      if (worktrees.merges.length !== 0) throw new Error("merge ran before the initial task settled");
      await writeDebug("SIGKILL-boundary-ready");
      await writeFile(boundaryPath, "accepted task, exact session initialization, and waiting merge", "utf8");
      const hold = Promise.withResolvers();
      process.stdin.once("data", hold.resolve);
      await hold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    const readDebug = async (): Promise<string> =>
      readFile(debugPath, "utf8").catch(() => "<no child state snapshot>");
    try {
      await waitUntil(async () => child!.exitCode !== null || await fileExists(boundaryPath),
        "the child has durably accepted the initial task and waiting merge", 30_000);
    } catch (error) {
      throw new Error(
        `${error instanceof Error ? error.message : String(error)}; childState=${await readDebug()}`,
      );
    }
    if (child.exitCode !== null) {
      throw new Error(
        `session-merge crash fixture exited early; childState=${await readDebug()}; `
        + `stderr=${await childStderr}`,
      );
    }

    observer = await openChannelStore(stateDir);
    const source = (messageId: string) => ({
      botUid: bot.uid,
      sourceKind: "recv" as const,
      sourceId: recvIngressSourceId(target.channelType, target.channelId, messageId),
    });
    const queueIngress = observer.getIngressJobBySource(source(queueMessageId));
    const mergeIngress = observer.getIngressJobBySource(source(mergeMessageId));
    const queuedTask = observer.getTaskByMessageId(bot.uid, target, queueMessageId);
    const initializedBinding = observer.getBinding(bot.uid, target);
    if (queueIngress === null || mergeIngress === null || queuedTask === null || initializedBinding === null) {
      throw new Error("SIGKILL boundary is missing a durable ingress, task, or binding row");
    }
    queueJobId = queueIngress.id;
    mergeJobId = mergeIngress.id;
    expect(queueIngress.id).toBeLessThan(mergeIngress.id);
    expect(queueIngress).toMatchObject({
      lane: "ordered",
      state: "handed-off",
      consumerKind: "task",
      consumerId: queuedTask.id,
      routeSnapshot: {
        sessionGeneration: 0,
        barrierJobId: null,
        binding: {
          sessionId: oldSessionId,
          cwd,
          sessionFile: null,
          cwdPinned: true,
          sessionSwitchedAt: null,
        },
      },
    });
    expect(queuedTask).toMatchObject({ state: "running", cwd });
    expect(initializedBinding).toMatchObject({
      cwd,
      cwdPinned: true,
      sessionSwitchedAt: null,
      sessionInitialization: {
        taskId: queuedTask.id,
        fromSessionId: oldSessionId,
        fromCwd: cwd,
        fromSessionFile: null,
        fromCwdPinned: true,
        fromSessionSwitchedAt: null,
      },
    });
    expect(initializedBinding.sessionId).not.toBe(oldSessionId);
    expect(initializedBinding.sessionFile).toBeTruthy();
    expect(mergeIngress).toMatchObject({
      lane: "ordered",
      state: "accepted",
      routeSnapshot: {
        sessionGeneration: 0,
        barrierJobId: null,
        binding: {
          sessionId: oldSessionId,
          cwd,
          sessionFile: null,
          cwdPinned: true,
          sessionSwitchedAt: null,
        },
      },
      operationCheckpoint: {
        kind: "command",
        action: "thread",
        phase: "waiting-session",
      },
    });
    expect(child.exitCode).toBeNull();
    observer.close();
    observer = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const effects: string[] = [];
    const mergeTaskStates: Array<string | null> = [];
    const releaseRecoveredTask = Promise.withResolvers<void>();
    const workers = new FakeWorkerFactory(heldPrompt(text => {
      if (text === "/retry") effects.push("older-task-resumed");
    }, releaseRecoveredTask.promise));
    workers.recovered = true;
    const transport = new FakeTransport({ bot });
    registerFixtureThread(transport, "group-allowed", shortId);
    const worktrees = new FakeWorktrees(root);
    const merge = worktrees.merge.bind(worktrees);
    worktrees.merge = async request => {
      const task = await readTask(restarted!, target, queueMessageId);
      mergeTaskStates.push(task?.state ?? null);
      effects.push("merge");
      return merge(request);
    };
    restarted = await createCoreHarness({
      root,
      bot,
      transport,
      workers,
      worktrees,
    });

    await waitUntil(() => effects.length > 0, "recovered task or premature merge effect", 10_000);
    expect(effects[0]).toBe("older-task-resumed");
    expect(effects).not.toContain("merge");
    const recoveredTask = await readTask(restarted, target, queueMessageId);
    const recoveredQueue = await readIngressJob(restarted, target, queueMessageId);
    const recoveredMerge = await readIngressJob(restarted, target, mergeMessageId);
    expect(recoveredTask).toMatchObject({ id: queuedTask.id, state: "running", cwd });
    expect(recoveredQueue).toMatchObject({
      id: queueJobId,
      state: "handed-off",
      consumerKind: "task",
      consumerId: queuedTask.id,
      routeSnapshot: {
        binding: {
          sessionId: oldSessionId,
          cwd,
          sessionFile: null,
          cwdPinned: true,
          sessionSwitchedAt: null,
        },
      },
    });
    expect(recoveredMerge).toMatchObject({
      id: mergeJobId,
      state: "accepted",
      operationCheckpoint: {
        kind: "command",
        action: "thread",
        phase: "waiting-session",
      },
    });
    expect(mergeTaskStates).toEqual([]);
    expect(worktrees.merges).toHaveLength(0);

    releaseRecoveredTask.resolve();
    const completedTask = await waitForTask(
      restarted,
      target,
      queueMessageId,
      task => task?.state === "completed",
      "the original initialized-session task to complete after restart",
    );
    expect(completedTask?.id).toBe(queuedTask.id);
    await waitUntil(() => effects.includes("merge"), "the same waiting merge after recovered task completion", 10_000);
    expect(effects).toEqual(["older-task-resumed", "merge"]);
    expect(mergeTaskStates).toEqual(["completed"]);
    expect(worktrees.merges).toHaveLength(1);
    expect(worktrees.merges[0]).toMatchObject({ cwd, target: "main" });
    expect(await readTask(restarted, target, queueMessageId)).toMatchObject({
      id: queuedTask.id,
      state: "completed",
    });
  } finally {
    observer?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);


test("an ACKed cancel killed before dispatch recovers from durable task intent", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-cancel-premarker-"));
  const bot = { uid: "bot-ingress-cancel-premarker", name: "Ingress Cancel Premarker Bot" };
  const target = { channelType: 1 as const, channelId: "dm-ingress-cancel-premarker" };
  const activeMessageId = "safety-cancel-premarker-active";
  const queuedMessageId = "safety-cancel-premarker-queued";
  const cancelMessageId = "safety-cancel-premarker-command";
  const dispatchGatePath = join(root, "dispatch-gate");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let boundaryDb: Database | undefined;
  let restarted: CoreHarness | undefined;

  try {
    // Keep the service in a separate process so this boundary really ends with SIGKILL.
    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
      import { openChannelStore } from ${JSON.stringify(new URL("../src/store/index.ts", import.meta.url).href)};
      import { writeFileSync } from "node:fs";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = { channelType: 1, channelId: "dm-ingress-cancel-premarker" };
      const dispatchGatePath = join(root, "dispatch-gate");
      const activeStarted = Promise.withResolvers();
      const workers = new FakeWorkerFactory(async ({ text, onStop }) => {
        if (text !== "active task before cancel") return;
        activeStarted.resolve();
        const stopped = Promise.withResolvers();
        onStop(stopped.resolve);
        await stopped.promise;
      });
      const service = await createCoreHarness({ root, bot, workers });
      const serviceStore = service.service.store;
      const childHold = Promise.withResolvers();
      process.stdin.once("data", childHold.resolve);
      const message = (messageId, text) => inbound({ ...target, messageId, text });
      const active = service.service.receive(message("${activeMessageId}", "active task before cancel"));
      await active.ackReady;
      await activeStarted.promise;
      const queued = service.service.receive(message("${queuedMessageId}", "/queue queued task before cancel"));
      await queued.ackReady;
      const observer = await openChannelStore(service.stateDir);
      try {
        while (observer.getTaskByMessageId(bot.uid, target, "${queuedMessageId}")?.state !== "queued") {
          const turn = Promise.withResolvers();
          setImmediate(turn.resolve);
          await turn.promise;
        }
      } finally {
        observer.close();
      }
      const originalMarkDispatchIntent = serviceStore.markIngressDispatchIntent.bind(serviceStore);
      const blocked = new Int32Array(new SharedArrayBuffer(4));
      serviceStore.markIngressDispatchIntent = id => {
        writeFileSync(dispatchGatePath, "cancel intent durable; dispatch marker not yet written");
        Atomics.wait(blocked, 0, 0);
        return originalMarkDispatchIntent(id);
      };
      const cancel = service.service.receive(message("${cancelMessageId}", "/cancel"));
      await cancel.ackReady;
      await childHold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    await waitUntil(async () => (
      child!.exitCode !== null || await fileExists(dispatchGatePath)
    ), "cancel admitted and held before its dispatch marker", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`pre-marker cancel fixture exited early: ${await childStderr}`);
    }

    boundaryDb = new Database(join(root, "state", "channel.sqlite"), { readonly: true });
    const cancelRow = (): { state: string; ack_ready: number; operation_checkpoint_json: string | null } | null =>
      boundaryDb!.query(
        "SELECT state, ack_ready, operation_checkpoint_json FROM ingress_jobs WHERE message_id = ? AND channel_type = ? AND channel_id = ?",
      ).get(cancelMessageId, target.channelType, target.channelId) as {
        state: string;
        ack_ready: number;
        operation_checkpoint_json: string | null;
      } | null;
    const activeRow = (): { id: string; state: string; stop_intent: string | null } | null =>
      boundaryDb!.query("SELECT id, state, stop_intent FROM tasks WHERE message_id = ?")
        .get(activeMessageId) as { id: string; state: string; stop_intent: string | null } | null;
    const queuedRow = (): { id: string; state: string } | null =>
      boundaryDb!.query("SELECT id, state FROM tasks WHERE message_id = ?")
        .get(queuedMessageId) as { id: string; state: string } | null;
    const cancel = cancelRow();
    const active = activeRow();
    const queued = queuedRow();
    expect(cancel).toMatchObject({ state: "accepted", ack_ready: 1 });
    expect(cancel?.operation_checkpoint_json).not.toBeNull();
    expect(JSON.parse(cancel!.operation_checkpoint_json!)).toMatchObject({
      phase: "cancel-intent",
      stoppedTaskIds: [active?.id],
      cancelledQueuedTaskIds: [queued?.id],
    });
    expect(active).toMatchObject({ stop_intent: "cancel" });
    expect(queued).toMatchObject({ state: "cancelled" });
    boundaryDb.close();
    boundaryDb = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await waitUntil(async () => (
      (await readTask(restarted!, target, activeMessageId))?.state === "cancelled"
      && (await readTask(restarted!, target, queuedMessageId))?.state === "cancelled"
    ), "durable cancel intent recovery before task scheduling", 10_000);
    await settleIngress(restarted);
    expect(restartWorkers.prompts).toHaveLength(0);
  } finally {
    boundaryDb?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("an ACKed /cancel persists intent before SIGKILL and settles tasks on restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-cancel-crash-"));
  const bot = { uid: "bot-ingress-cancel-crash", name: "Ingress Cancel Crash Bot" };
  const shortId = "ingress-safety-cancel-crash";
  const target = threadTarget(shortId);
  const stateDir = join(root, "state");
  const cwd = join(root, "cancel-crash-workspace");
  const activeMessageId = "safety-crash-active-task";
  const queuedMessageId = "safety-crash-queued-task";
  const mergeMessageId = "safety-crash-pending-merge";
  const cancelMessageId = "safety-crash-cancel";
  const mergeHeldPath = join(root, "merge-held");
  const cancelAckedPath = join(root, "cancel-acked");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;
  let boundaryDb: Database | undefined;

  try {
    await mkdir(cwd, { recursive: true });
    const setup = await openChannelStore(stateDir);
    setup.ensureBinding({
      botUid: bot.uid,
      ...target,
      cwd,
      sessionId: `ingress-safety-${target.channelType}-${target.channelId}`,
      sessionFile: null,
      cwdPinned: true,
    });
    setup.close();

    // The `-e` child must import its own CoreService fixture modules; parent imports cannot cross the SIGKILL boundary.
    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeTransport, FakeWorkerFactory, FakeWorktrees, inbound } from ${JSON.stringify(fixtureUrl)};
      import { openChannelStore } from ${JSON.stringify(new URL("../src/store/index.ts", import.meta.url).href)};
      import { mkdir, writeFile } from "node:fs/promises";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = {
        channelType: 5,
        channelId: "group-allowed____${shortId}",
      };
      const activeStarted = Promise.withResolvers();
      const mergeHeldPath = join(root, "merge-held");
      const cancelAckedPath = join(root, "cancel-acked");
      const fileExists = async path => Bun.file(path).exists();
      const waitFor = async check => {
        while (!(await check())) {
          const turn = Promise.withResolvers();
          setImmediate(turn.resolve);
          await turn.promise;
        }
      };
      await mkdir(join(root, "cancel-crash-workspace"), { recursive: true });
      const transport = new FakeTransport({ bot });
      transport.setThreads("group-allowed", [{
        shortId: "${shortId}",
        groupId: "group-allowed",
        channelId: target.channelId,
        name: "Ingress safety crash fixture thread",
        creatorUid: "owner-1",
        status: 1,
        memberCount: 1,
        messageCount: 0,
        createdAt: "2026-11-04T00:00:00.000Z",
        updatedAt: "2026-11-04T00:00:00.000Z",
      }]);
      const workers = new FakeWorkerFactory(async ({ text, onStop }) => {
        if (text !== "active task before cancel") {
          await writeFile(join(root, "unexpected-prompt"), text, "utf8");
          return;
        }
        activeStarted.resolve();
        const stopped = Promise.withResolvers();
        onStop(stopped.resolve);
        await stopped.promise;
      });
      const worktrees = new FakeWorktrees(root);
      const mergeRelease = Promise.withResolvers();
      worktrees.worktrunk = async () => {
        await writeFile(mergeHeldPath, "held", "utf8");
        await mergeRelease.promise;
        return { available: true, command: "wt", version: "fixture" };
      };
      const service = await createCoreHarness({ root, bot, transport, workers, worktrees });
      const message = (messageId, text) => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [bot.uid],
      });
      const activeReceipt = service.service.receive(message("${activeMessageId}", "active task before cancel"));
      await activeReceipt.ackReady;
      await activeStarted.promise;

      const queuedReceipt = service.service.receive(message("${queuedMessageId}", "/queue queued task to cancel"));
      await queuedReceipt.ackReady;
      const observer = await openChannelStore(service.stateDir);
      try {
        await waitFor(() =>
          observer.getTaskByMessageId(bot.uid, target, "${queuedMessageId}")?.state === "queued",
        );
      } finally {
        observer.close();
      }

      const mergeReceipt = service.service.receive(message("${mergeMessageId}", "/thread merge"));
      await mergeReceipt.ackReady;
      await waitFor(() => fileExists(mergeHeldPath));
      const cancelReceipt = service.service.receive(message("${cancelMessageId}", "/cancel"));
      await cancelReceipt.ackReady;
      await writeFile(cancelAckedPath, "ackReady", "utf8");
      const childHold = Promise.withResolvers();
      process.stdin.once("data", childHold.resolve);
      await childHold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    await waitUntil(async () => child!.exitCode !== null || await fileExists(cancelAckedPath),
      "ACKed cancel while the merge is held", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`cancel crash fixture exited early: ${await childStderr}`);
    }
    expect(await fileExists(mergeHeldPath)).toBe(true);

    boundaryDb = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    const activeStopIntent = (): { stop_intent: string | null } | null =>
      boundaryDb!.query("SELECT stop_intent FROM tasks WHERE message_id = ?")
        .get(activeMessageId) as { stop_intent: string | null } | null;
    const cancelIngress = (): { state: string; ack_ready: number } | null =>
      boundaryDb!.query(
        "SELECT state, ack_ready FROM ingress_jobs WHERE message_id = ? AND channel_type = ? AND channel_id = ?",
      ).get(cancelMessageId, target.channelType, target.channelId) as { state: string; ack_ready: number } | null;
    await waitUntil(
      () => activeStopIntent()?.stop_intent === "cancel" && cancelIngress()?.state === "dispatch-intent",
      "durable cancel stop intent and dispatch marker before SIGKILL",
      10_000,
    );
    expect(activeStopIntent()).toMatchObject({ stop_intent: "cancel" });
    expect(cancelIngress()).toMatchObject({ state: "dispatch-intent", ack_ready: 1 });
    boundaryDb.close();
    boundaryDb = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory(async ({ onStop }) => {
      const stopped = Promise.withResolvers<void>();
      onStop(stopped.resolve);
      await stopped.promise;
    });
    restartWorkers.recovered = true;
    const transport = new FakeTransport({ bot });
    registerFixtureThread(transport, "group-allowed", shortId);
    restarted = await createCoreHarness({
      root,
      bot,
      transport,
      workers: restartWorkers,
    });

    await waitUntil(async () => (
      (await readTask(restarted!, target, activeMessageId))?.state === "cancelled"
      && (await readTask(restarted!, target, queuedMessageId))?.state === "cancelled"
      && (await readIngressJob(restarted!, target, mergeMessageId))?.state === "cancelled"
    ), "cancel recovery of active, queued, and waiting-merge work", 10_000);
    await settleIngress(restarted);
    expect(restartWorkers.prompts).toHaveLength(0);
    expect(restarted.worktrees?.merges).toHaveLength(0);
  } finally {
    boundaryDb?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("a thread-task steer accepted before SIGKILL is not replayed after restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-steer-crash-"));
  const bot = { uid: "bot-ingress-steer-crash", name: "Ingress Steer Crash Bot" };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const activeMessageId = "safety-steer-crash-active";
  const threadTaskMessageId = "safety-steer-crash-task";
  const steerStartedPath = join(root, "steer-started");
  const steerReleasePath = join(root, "steer-release");
  const steerAcceptedPath = join(root, "steer-accepted");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let observer: ChannelStore | undefined;
  let restarted: CoreHarness | undefined;

  try {
    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
      import { writeFile } from "node:fs/promises";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = { channelType: 2, channelId: "group-allowed" };
      const steerStartedPath = join(root, "steer-started");
      const steerReleasePath = join(root, "steer-release");
      const steerAcceptedPath = join(root, "steer-accepted");
      const fileExists = async path => Bun.file(path).exists();
      const waitFor = async check => {
        while (!(await check())) {
          const turn = Promise.withResolvers();
          setImmediate(turn.resolve);
          await turn.promise;
        }
      };
      const activeStarted = Promise.withResolvers();
      const workers = new FakeWorkerFactory(async ({ text, onStop }) => {
        if (text !== "running owner task") return;
        activeStarted.resolve();
        const stopped = Promise.withResolvers();
        onStop(stopped.resolve);
        await stopped.promise;
      });
      workers.beforeSteer = async () => {
        await writeFile(steerStartedPath, "attempt", "utf8");
        await waitFor(() => fileExists(steerReleasePath));
        await writeFile(steerAcceptedPath, "accepted", "utf8");
      };
      const service = await createCoreHarness({ root, bot, workers });
      const childHold = Promise.withResolvers();
      process.stdin.once("data", childHold.resolve);
      const message = (messageId, text) => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [bot.uid],
      });
      const active = service.service.receive(message("${activeMessageId}", "running owner task"));
      await active.ackReady;
      await activeStarted.promise;
      const threadTask = service.service.receive(
        message("${threadTaskMessageId}", "/thread implement this once"),
      );
      await threadTask.ackReady;
      await childHold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    await waitUntil(async () => child!.exitCode !== null || await fileExists(steerStartedPath),
      "thread task reaches the gated worker-steer boundary", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`steer crash fixture exited early: ${await childStderr}`);
    }
    await writeFile(steerReleasePath, "release", "utf8");

    observer = await openChannelStore(join(root, "state"));
    const sourceKey = {
      botUid: bot.uid,
      sourceKind: "recv" as const,
      sourceId: recvIngressSourceId(target.channelType, target.channelId, threadTaskMessageId),
    };
    await waitUntil(() => {
      const job = observer!.getIngressJobBySource(sourceKey);
      return job?.state === "handed-off"
        && job.consumerKind === "steering"
        && job.consumerId === threadTaskMessageId
        && observer!.getCommandClaimState(bot.uid, target, threadTaskMessageId) === "completed";
    }, "accepted steering and command claim are durably handed off together", 10_000);
    expect(await fileExists(steerAcceptedPath)).toBe(true);
    const acceptedJob = observer.getIngressJobBySource(sourceKey);
    expect(acceptedJob).toMatchObject({
      state: "handed-off",
      consumerKind: "steering",
      consumerId: threadTaskMessageId,
    });
    observer.close();
    observer = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await settleIngress(restarted);
    await settleTurns();

    expect(await readTask(restarted, target, threadTaskMessageId)).toBeNull();
    expect(restartWorkers.workers.reduce(
      (total, worker) => total + worker.steerCalls.length,
      0,
    )).toBe(0);
    expect(restartWorkers.prompts.some(({ text }) => text.includes("implement this once"))).toBe(false);
    expect(await readIngressJob(restarted, target, threadTaskMessageId)).toMatchObject({
      state: "handed-off",
      consumerKind: "steering",
      consumerId: threadTaskMessageId,
    });
  } finally {
    observer?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
test("an accepted thread-task steer killed before handoff is unknown and never replayed", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-ingress-steer-unknown-"));
  const bot = { uid: "bot-ingress-steer-unknown", name: "Ingress Steer Unknown Bot" };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const activeMessageId = "safety-steer-unknown-active";
  const threadTaskMessageId = "safety-steer-unknown-task";
  const steerStartedPath = join(root, "steer-started");
  const steerReleasePath = join(root, "steer-release");
  const handoffStartedPath = join(root, "handoff-started");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let observer: ChannelStore | undefined;
  let restarted: CoreHarness | undefined;

  try {
    // The child owns the running service so SIGKILL cannot run graceful service cleanup.
    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { createCoreHarness, FakeWorkerFactory, inbound } from ${JSON.stringify(fixtureUrl)};
      import { writeFile } from "node:fs/promises";
      import { writeFileSync } from "node:fs";
      import { join } from "node:path";
      const root = process.env.INGRESS_SAFETY_ROOT;
      if (!root) throw new Error("INGRESS_SAFETY_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const target = { channelType: 2, channelId: "group-allowed" };
      const steerStartedPath = join(root, "steer-started");
      const steerReleasePath = join(root, "steer-release");
      const handoffStartedPath = join(root, "handoff-started");
      const fileExists = async path => Bun.file(path).exists();
      const waitFor = async check => {
        while (!(await check())) {
          const turn = Promise.withResolvers();
          setImmediate(turn.resolve);
          await turn.promise;
        }
      };
      const activeStarted = Promise.withResolvers();
      const workers = new FakeWorkerFactory(async ({ text, onStop }) => {
        if (text !== "running owner task") return;
        activeStarted.resolve();
        const stopped = Promise.withResolvers();
        onStop(stopped.resolve);
        await stopped.promise;
      });
      workers.beforeSteer = async () => {
        await writeFile(steerStartedPath, "attempt", "utf8");
        await waitFor(() => fileExists(steerReleasePath));
      };
      const service = await createCoreHarness({ root, bot, workers });
      const childHold = Promise.withResolvers();
      process.stdin.once("data", childHold.resolve);
      const serviceStore = service.service.store;
      const originalHandoff = serviceStore.handoffIngressJob.bind(serviceStore);
      const blocked = new Int32Array(new SharedArrayBuffer(4));
      serviceStore.handoffIngressJob = (id, consumer, persistConsumer) => {
        if (consumer.kind === "steering" && consumer.id === "${threadTaskMessageId}") {
          writeFileSync(handoffStartedPath, "worker steer returned; handoff not committed");
          Atomics.wait(blocked, 0, 0);
        }
        return originalHandoff(id, consumer, persistConsumer);
      };
      const message = (messageId, text) => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [bot.uid],
      });
      const active = service.service.receive(message("${activeMessageId}", "running owner task"));
      await active.ackReady;
      await activeStarted.promise;
      const threadTask = service.service.receive(
        message("${threadTaskMessageId}", "/thread implement exactly once"),
      );
      await threadTask.ackReady;
      await childHold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, INGRESS_SAFETY_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const fileExists = async (path: string): Promise<boolean> =>
      readFile(path).then(() => true, () => false);
    await waitUntil(async () => child!.exitCode !== null || await fileExists(steerStartedPath),
      "thread task reaches its gated worker-steer boundary", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`uncertain-steer fixture exited early: ${await childStderr}`);
    }
    await writeFile(steerReleasePath, "release", "utf8");
    await waitUntil(async () => child!.exitCode !== null || await fileExists(handoffStartedPath),
      "accepted steer reaches pre-handoff gate", 10_000);
    if (child.exitCode !== null) {
      throw new Error(`uncertain-steer fixture exited before handoff gate: ${await childStderr}`);
    }

    observer = await openChannelStore(join(root, "state"));
    const sourceKey = {
      botUid: bot.uid,
      sourceKind: "recv" as const,
      sourceId: recvIngressSourceId(target.channelType, target.channelId, threadTaskMessageId),
    };
    const preHandoffJob = observer.getIngressJobBySource(sourceKey);
    expect(preHandoffJob).toMatchObject({ state: "dispatch-intent", consumerKind: null });
    expect(observer.getCommandClaimState(bot.uid, target, threadTaskMessageId)).toBe("pending");
    observer.close();
    observer = undefined;

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({ root, bot, workers: restartWorkers });
    await waitUntil(async () =>
      (await readIngressJob(restarted!, target, threadTaskMessageId))?.state === "unknown",
    "uncertain accepted steer is quarantined after restart", 10_000);
    await settleIngress(restarted);
    await settleTurns();

    const finalJob = await readIngressJob(restarted, target, threadTaskMessageId);
    expect(finalJob).toMatchObject({ state: "unknown", consumerKind: null });
    expect(await readTask(restarted, target, threadTaskMessageId)).toBeNull();
    expect(restartWorkers.workers.reduce(
      (total, worker) => total + worker.steerCalls.length,
      0,
    )).toBe(0);
    expect(restartWorkers.prompts.some(({ text }) => text.includes("implement exactly once"))).toBe(false);
  } finally {
    observer?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, 45_000);


test("empty-text media received during an ask is materialized as work, not silently consumed as an answer", async () => {
  const attachmentUrl = "https://cdn.example.test/ingress-safety-pending-ask.png";
  const imageBytes = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3]);
  const harness = await createCoreHarness({
    attachments: { enabled: true, maxBytes: 1_048_576 },
    prompt: async ({ promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      await ask.execute({
        questions: [{ id: "media-answer", question: "What is shown?", options: [{ label: "A" }, { label: "B" }] }],
      }, new AbortController().signal);
    },
  });
  const target = { channelType: 1 as const, channelId: "dm-ingress-safety-empty-media" };
  harness.transport.attachmentBodies.set(attachmentUrl, imageBytes);
  let downloadStarted = false;
  const releaseDownload = Promise.withResolvers<void>();
  const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.downloadAttachment = async request => {
    downloadStarted = true;
    await releaseDownload.promise;
    return originalDownload(request);
  };

  try {
    const ask = harness.service.receive(ownerMessage(harness, target, "safety-empty-media-ask", "ask about this image"));
    await ask.ackReady;
    await waitUntil(async () => (await harness.service.status(target)).pendingQuestionIds.length === 1, "pending ask");

    const media = ownerMessage(harness, target, "safety-empty-media", "", {
      attachments: [{
        url: attachmentUrl,
        kind: "image",
        name: "pending-ask.png",
        size: imageBytes.byteLength,
        durationSeconds: 0,
      }],
    });
    const receipt = harness.service.receive(media);
    await waitUntil(() => downloadStarted, "download of empty-text media during a pending ask");
    expect(await observedState(receipt.ackReady)).toBe("pending");
    releaseDownload.resolve();
    await receipt.ackReady;
    await settleIngress(harness);

    const status = await harness.service.status(target);
    expect(status.pendingQuestionIds).toHaveLength(1);
    await waitUntil(async () => {
      const job = await readIngressJob(harness, target, media.messageId);
      return job?.state === "handed-off" && job.consumerKind === "steering";
    }, "durable steering handoff for empty-text media");
    expect(await readIngressJob(harness, target, media.messageId)).toMatchObject({
      state: "handed-off",
      consumerKind: "steering",
      consumerId: media.messageId,
    });
    expect(await readTask(harness, target, media.messageId)).toBeNull();
    expect(harness.transport.downloadRequests).toHaveLength(1);
    const transferPath = harness.transport.downloadRequests[0]?.path;
    expect(transferPath).toBeDefined();
    expect(transferPath?.endsWith(".part")).toBe(true);
    const canonicalPath = join(
      harness.stateDir,
      "attachments",
      "1-dm-ingress-safety-empty-media",
      media.messageId,
      "0-pending-ask.png",
    );
    const steeredBody = harness.workers.workers[0]?.steerCalls.find(text => text.includes(canonicalPath));
    expect(steeredBody).toBeDefined();
    expect(steeredBody).toContain("pending-ask.png");
    expect([...await readFile(canonicalPath)]).toEqual([...imageBytes]);
    expect(await readFile(transferPath!).catch(() => null)).toBeNull();
  } finally {
    releaseDownload.resolve();
    await closeCoreHarness(harness);
  }
});
