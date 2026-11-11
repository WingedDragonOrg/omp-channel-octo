import { expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { ChannelService, ChatTarget } from "../src/contracts.ts";
import { projectSessionDir } from "../src/session/session-dir.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { BindingRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

interface ServiceStatusSnapshot {
  version: 1;
  sampledAt: string;
  pid: number;
  runningSessions: number;
  queuedTasks: number;
}

interface ChatStatusSnapshot {
  version: 1;
  sampledAt: string;
  channelType: number;
  channelId: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
}

interface StatusSnapshotApi {
  statusSnapshot(): ServiceStatusSnapshot;
  chatStatusSnapshot(target: ChatTarget): ChatStatusSnapshot;
}

function statusSnapshotApi(service: ChannelService): StatusSnapshotApi {
  const snapshotApi = service as unknown as StatusSnapshotApi;
  expect(typeof snapshotApi.statusSnapshot).toBe("function");
  expect(typeof snapshotApi.chatStatusSnapshot).toBe("function");
  return snapshotApi;
}

function expectUtcTimestamp(value: string): void {
  expect(Number.isNaN(Date.parse(value))).toBe(false);
  expect(new Date(value).toISOString()).toBe(value);
}

const CHAT_A: ChatTarget = { channelType: 1, channelId: "dm-snapshot-a" };
const CHAT_B: ChatTarget = { channelType: 1, channelId: "dm-snapshot-b" };
const UNKNOWN_CHAT: ChatTarget = { channelType: 1, channelId: "dm-snapshot-unknown" };
const CHAT_GOAL_CONTROL: ChatTarget = { channelType: 1, channelId: "dm-snapshot-goal-control" };

test("status snapshots reflect live work, queued tasks, chat isolation, and unknown chats", async () => {
  const promptAStarted = Promise.withResolvers<void>();
  const promptBStarted = Promise.withResolvers<void>();
  const releaseA = Promise.withResolvers<void>();
  const releaseB = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "snapshot A running") {
        promptAStarted.resolve();
        await releaseA.promise;
      } else if (text === "snapshot B running") {
        promptBStarted.resolve();
        await releaseB.promise;
      }
    },
  });
  const store = await openChannelStore(harness.stateDir);

  try {
    store.setChatMode(harness.bot.uid, CHAT_A, "queue");
    store.setChatModel(harness.bot.uid, CHAT_A, "fixture/snapshot-a");
    store.setChatModel(harness.bot.uid, CHAT_B, "fixture/snapshot-b");

    await harness.service.submitInbound(inbound({
      ...CHAT_A,
      messageId: "snapshot-a-running",
      text: "snapshot A running",
    }));
    await promptAStarted.promise;

    await harness.service.submitInbound(inbound({
      ...CHAT_A,
      messageId: "snapshot-a-queued",
      text: "snapshot A queued",
    }));
    await waitForStatus(
      harness.service,
      CHAT_A,
      status => status.state === "running" && status.queued === 1,
    );

    await harness.service.submitInbound(inbound({
      ...CHAT_B,
      messageId: "snapshot-b-running",
      text: "snapshot B running",
    }));
    await promptBStarted.promise;

    const snapshots = statusSnapshotApi(harness.service);
    const serviceSnapshot = snapshots.statusSnapshot();
    expect(serviceSnapshot).toMatchObject({
      version: 1,
      pid: process.pid,
      botUid: harness.bot.uid,
      ownerUid: "owner-1",
      runningSessions: 2,
      queuedTasks: 1,
    });
    expectUtcTimestamp(serviceSnapshot.sampledAt);

    const chatA = snapshots.chatStatusSnapshot(CHAT_A);
    expect(chatA).toMatchObject({
      version: 1,
      channelType: CHAT_A.channelType,
      channelId: CHAT_A.channelId,
      state: "running",
      model: "fixture/snapshot-a",
      cwd: join(harness.root, "tmp"),
      queuedTasks: 1,
      pendingQuestions: 0,
    });
    expectUtcTimestamp(chatA.sampledAt);

    const chatB = snapshots.chatStatusSnapshot(CHAT_B);
    expect(chatB).toMatchObject({
      version: 1,
      channelType: CHAT_B.channelType,
      channelId: CHAT_B.channelId,
      state: "running",
      model: "fixture/snapshot-b",
      cwd: join(harness.root, "tmp"),
      queuedTasks: 0,
      pendingQuestions: 0,
    });
    expectUtcTimestamp(chatB.sampledAt);

    const unknown = snapshots.chatStatusSnapshot(UNKNOWN_CHAT);
    expect(unknown).toMatchObject({
      version: 1,
      channelType: UNKNOWN_CHAT.channelType,
      channelId: UNKNOWN_CHAT.channelId,
      state: "none",
      model: null,
      cwd: null,
      queuedTasks: 0,
      pendingQuestions: 0,
    });
    expectUtcTimestamp(unknown.sampledAt);
    expect(store.getBinding(harness.bot.uid, UNKNOWN_CHAT)).toBeNull();
    expect(store.getLatestTask(harness.bot.uid, UNKNOWN_CHAT)).toBeNull();

    releaseA.resolve();
    releaseB.resolve();
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(
      harness.service,
      CHAT_A,
      status => status.state === "completed" && status.queued === 0,
    );
    await waitForStatus(harness.service, CHAT_B, status => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text).sort()).toEqual([
      "snapshot A queued",
      "snapshot A running",
      "snapshot B running",
    ]);
  } finally {
    releaseA.resolve();
    releaseB.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("status snapshot reads persisted binding and model without reconciling a moved session", async () => {
  const harness = await createCoreHarness();
  const store = await openChannelStore(harness.stateDir);
  const target: ChatTarget = { channelType: 1, channelId: "dm-snapshot-persisted-binding" };

  try {
    const originalCwd = join(harness.root, "snapshot-original-workspace");
    const movedCwd = join(harness.root, "snapshot-moved-workspace");
    await mkdir(originalCwd, { recursive: true });
    await mkdir(movedCwd, { recursive: true });

    const previousSessionFile = join(harness.root, "old-sessions", "snapshot-session.jsonl");
    const movedSessionFile = join(
      projectSessionDir(join(harness.stateDir, "sessions"), originalCwd),
      basename(previousSessionFile),
    );
    await mkdir(join(harness.stateDir, "sessions"), { recursive: true });
    await mkdir(dirname(movedSessionFile), { recursive: true });
    await writeFile(
      movedSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "persisted-snapshot-session",
        cwd: movedCwd,
        previousSessionFiles: [previousSessionFile],
      })}\n`,
      "utf8",
    );

    const binding: BindingRecord = {
      botUid: harness.bot.uid,
      ...target,
      sessionId: "persisted-snapshot-session",
      cwd: originalCwd,
      sessionFile: previousSessionFile,
      cwdPinned: true,
      sessionSwitchedAt: null,
    };
    store.ensureBinding(binding);
    store.setChatModel(harness.bot.uid, target, "fixture/persisted-model");
    const bindingBefore = store.getBinding(harness.bot.uid, target);
    expect(bindingBefore).toEqual(binding);

    const snapshots = statusSnapshotApi(harness.service);
    const serviceSnapshot = snapshots.statusSnapshot();
    expect(serviceSnapshot).toMatchObject({ version: 1, pid: process.pid, runningSessions: 0, queuedTasks: 0 });
    expectUtcTimestamp(serviceSnapshot.sampledAt);
    expect(store.getBinding(harness.bot.uid, target)).toEqual(bindingBefore);

    const chatSnapshot = snapshots.chatStatusSnapshot(target);
    expect(chatSnapshot).toMatchObject({
      version: 1,
      channelType: target.channelType,
      channelId: target.channelId,
      state: "idle",
      model: "fixture/persisted-model",
      cwd: originalCwd,
      queuedTasks: 0,
      pendingQuestions: 0,
    });
    expectUtcTimestamp(chatSnapshot.sampledAt);
    expect(store.getBinding(harness.bot.uid, target)).toEqual(bindingBefore);
    expect(store.getChatModel(harness.bot.uid, target)).toBe("fixture/persisted-model");
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

test("status snapshot counts an idle goal-show temporary worker until its control call settles", async () => {
  const harness = await createCoreHarness();
  const store = await openChannelStore(harness.stateDir);
  const goalCallStarted = Promise.withResolvers<void>();
  const releaseGoalCall = Promise.withResolvers<void>();
  const originalCreate = harness.workers.create.bind(harness.workers);

  try {
    await harness.service.submitInbound(inbound({
      ...CHAT_GOAL_CONTROL,
      messageId: "snapshot-goal-prime",
      text: "prepare a persistent session",
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, CHAT_GOAL_CONTROL, status => status.state === "completed");

    const initialWorker = harness.workers.workers[0]!;
    const stopDeadline = performance.now() + 3_000;
    while (!initialWorker.stopped && performance.now() < stopDeadline) {
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
    expect(initialWorker.stopped).toBe(true);
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;

    const binding = store.getBinding(harness.bot.uid, CHAT_GOAL_CONTROL);
    expect(binding?.sessionFile).toBeTruthy();
    if (!binding?.sessionFile) throw new Error("priming task did not persist its session binding");

    const snapshots = statusSnapshotApi(harness.service);
    expect(snapshots.statusSnapshot().runningSessions).toBe(0);
    expect((await harness.service.status(CHAT_GOAL_CONTROL)).state).toBe("completed");

    harness.workers.create = async options => {
      const worker = await originalCreate(options);
      if (options.sessionFile === binding.sessionFile) {
        const goal = worker.goal.bind(worker);
        worker.goal = async op => {
          if (op === "get") {
            goalCallStarted.resolve();
            await releaseGoalCall.promise;
          }
          return await goal(op);
        };
      }
      return worker;
    };

    const previousSentCount = harness.transport.sent.length;
    await harness.service.submitInbound(inbound({
      ...CHAT_GOAL_CONTROL,
      messageId: "snapshot-goal-show",
      text: "/goal show",
    }));
    await goalCallStarted.promise;

    const temporaryCreation = harness.workers.creations.at(-1);
    expect(temporaryCreation).toMatchObject({
      cwd: binding.cwd,
      sessionFile: binding.sessionFile,
    });
    expect(harness.workers.workers.at(-1)?.sessionId).toBe(binding.sessionId);
    expect(snapshots.statusSnapshot().runningSessions).toBe(1);

    releaseGoalCall.resolve();
    const replyDeadline = performance.now() + 3_000;
    while (harness.transport.sent.length <= previousSentCount && performance.now() < replyDeadline) {
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
    expect(harness.transport.sent.length).toBeGreaterThan(previousSentCount);
    expect(snapshots.statusSnapshot().runningSessions).toBe(0);
  } finally {
    releaseGoalCall.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});
