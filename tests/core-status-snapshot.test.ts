import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdir, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { ChannelService, ChatTarget } from "../src/contracts.ts";
import type { LoopTask } from "../src/loop/types.ts";
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

interface ChatDetailsSnapshot extends ChatTarget {
  version: 1;
  sampledAt: string;
  context: {
    source: "live" | "persisted" | "unavailable";
    stale: boolean;
    usedTokens: number | null;
    contextWindow: number | null;
    percent: number | null;
    model: string | null;
    observedAt: string | null;
    note: string;
  };
  references: { pr: string[]; mr: string[] };
  tasks: { workspaceId: string; taskId: string; title: string | null; status: string | null; url: string | null }[];
}

interface ChatDetailsSnapshotApi {
  chatDetailsSnapshot(target: ChatTarget, signal: AbortSignal): Promise<ChatDetailsSnapshot>;
}

function chatDetailsSnapshotApi(service: ChannelService): ChatDetailsSnapshotApi {
  const snapshotApi = service as unknown as ChatDetailsSnapshotApi;
  expect(typeof snapshotApi.chatDetailsSnapshot).toBe("function");
  return snapshotApi;
}

function databaseVersion(database: Database): number {
  const row: unknown = database.query("PRAGMA data_version").get();
  if (
    typeof row !== "object"
    || row === null
    || !("data_version" in row)
    || typeof row.data_version !== "number"
  ) {
    throw new Error("SQLite did not return its data version");
  }
  return row.data_version;
}

function loopTask(taskId: string, title: string, status: string): LoopTask {
  return {
    taskId,
    title,
    description: `private-task-description:${taskId}`,
    status,
    priority: "normal",
    assigneeType: null,
    assigneeId: null,
    assigneeOctoUid: null,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-11T08:00:00.000Z",
    updatedAt: "2026-11-11T08:00:00.000Z",
  };
}

async function writeBoundSession(sessionFile: string, sessionId: string, cwd: string): Promise<void> {
  await mkdir(dirname(sessionFile), { recursive: true });
  await writeFile(
    sessionFile,
    `${JSON.stringify({
      type: "session",
      version: 3,
      id: sessionId,
      timestamp: "2026-11-11T08:00:00.000Z",
      cwd,
    })}\n`,
    "utf8",
  );
}

function expectUtcTimestamp(value: string): void {
  expect(Number.isNaN(Date.parse(value))).toBe(false);
  expect(new Date(value).toISOString()).toBe(value);
}

const CHAT_A: ChatTarget = { channelType: 1, channelId: "dm-snapshot-a" };
const CHAT_B: ChatTarget = { channelType: 1, channelId: "dm-snapshot-b" };
const UNKNOWN_CHAT: ChatTarget = { channelType: 1, channelId: "dm-snapshot-unknown" };
const CHAT_GOAL_CONTROL: ChatTarget = { channelType: 1, channelId: "dm-snapshot-goal-control" };

const THREAD_GROUP = "group-tools";
const DETAILS_THREAD: ChatTarget = { channelType: 5, channelId: `${THREAD_GROUP}____status-details` };
const SIBLING_THREAD: ChatTarget = { channelType: 5, channelId: `${THREAD_GROUP}____status-sibling` };
const UNBOUND_THREAD: ChatTarget = { channelType: 5, channelId: `${THREAD_GROUP}____status-unbound` };

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

test("details snapshots retain thread-routed Loop tasks across workspace switches", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  let promptFinished = false;
  const harness = await createCoreHarness({
    allowedGroupIds: [THREAD_GROUP],
    prompt: async () => {
      promptStarted.resolve();
      await releasePrompt.promise;
      promptFinished = true;
    },
  });
  const store = await openChannelStore(harness.stateDir);
  const observer = new Database(join(harness.stateDir, "channel.sqlite"));

  try {
    const workspace = { workspaceId: "details-workspace", name: "Details", slug: "details" };
    const mainTask = loopTask("details-main-task", "Main thread task", "in_progress");
    store.bindLoopWorkspace(harness.bot.uid, THREAD_GROUP, workspace, [mainTask]);
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: THREAD_GROUP,
      workspaceId: workspace.workspaceId,
      taskId: mainTask.taskId,
    }, DETAILS_THREAD.channelId);
    const missingSnapshotTaskId = "details-missing-snapshot";
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: THREAD_GROUP,
      workspaceId: workspace.workspaceId,
      taskId: missingSnapshotTaskId,
    }, DETAILS_THREAD.channelId);

    const mainPr = "https://github.com/example/service/pull/17";
    const mainMr = "https://gitlab.example/team/service/-/merge_requests/42";
    const siblingPr = "https://github.com/example/service/pull/99";
    store.addThreadReferences(harness.bot.uid, DETAILS_THREAD, [
      { kind: "pr", url: mainPr },
      { kind: "mr", url: mainMr },
    ]);
    store.addThreadReferences(harness.bot.uid, SIBLING_THREAD, [{ kind: "pr", url: siblingPr }]);
    store.addThreadReferences("another-bot", DETAILS_THREAD, [
      { kind: "pr", url: "https://github.com/private/other-bot/pull/1" },
    ]);

    await harness.service.submitInbound(inbound({
      ...DETAILS_THREAD,
      messageId: "details-live-worker",
      text: "private-thread-message-must-not-leak",
      mentionedUserIds: [harness.bot.uid],
    }));
    await promptStarted.promise;
    await waitForStatus(harness.service, DETAILS_THREAD, status => status.state === "running");

    const snapshotApi = chatDetailsSnapshotApi(harness.service);
    const bindingBefore = store.getBinding(harness.bot.uid, DETAILS_THREAD);
    expect(bindingBefore).not.toBeNull();
    const referencesBefore = store.getThreadReferences(harness.bot.uid, DETAILS_THREAD);
    const routesBefore = store.listLoopThreadTasks(harness.bot.uid, THREAD_GROUP, DETAILS_THREAD.channelId);
    const inputsBefore = store.listLoopInputs(harness.bot.uid);
    const versionBefore = databaseVersion(observer);
    const workersBefore = harness.workers.workers.length;

    const details = await snapshotApi.chatDetailsSnapshot(DETAILS_THREAD, new AbortController().signal);

    expect(details).toMatchObject({
      version: 1,
      channelType: DETAILS_THREAD.channelType,
      channelId: DETAILS_THREAD.channelId,
      context: {
        source: "live",
        stale: false,
        usedTokens: 4_096,
        contextWindow: 8_192,
        percent: 50,
      },
      references: { pr: [mainPr], mr: [mainMr] },
    });
    expectUtcTimestamp(details.sampledAt);
    expect(details.context.observedAt).not.toBeNull();
    expectUtcTimestamp(details.context.observedAt!);
    expect(details.tasks).toEqual([
      {
        workspaceId: workspace.workspaceId,
        taskId: mainTask.taskId,
        title: mainTask.title,
        status: mainTask.status,
        url: null,
      },
      {
        workspaceId: workspace.workspaceId,
        taskId: missingSnapshotTaskId,
        title: null,
        status: null,
        url: null,
      },
    ]);
    expect(JSON.stringify(details)).not.toContain("private-thread-message-must-not-leak");
    expect(JSON.stringify(details)).not.toContain("private-task-description");
    expect(JSON.stringify(details)).not.toContain("sibling");
    expect(store.getBinding(harness.bot.uid, DETAILS_THREAD)).toEqual(bindingBefore);
    expect(store.getThreadReferences(harness.bot.uid, DETAILS_THREAD)).toEqual(referencesBefore);
    expect(store.listLoopThreadTasks(harness.bot.uid, THREAD_GROUP, DETAILS_THREAD.channelId)).toEqual(routesBefore);
    expect(store.getSessionContextSnapshot(harness.bot.uid, DETAILS_THREAD)).toBeNull();
    expect(store.listLoopInputs(harness.bot.uid)).toEqual(inputsBefore);
    expect(databaseVersion(observer)).toBe(versionBefore);
    expect(harness.workers.workers).toHaveLength(workersBefore);
    expect(harness.workers.workers[0]?.contextUsageReads).toBe(1);
    expect(promptFinished).toBe(false);

    const nextWorkspace = { workspaceId: "details-workspace-next", name: "Details Next", slug: "details-next" };
    const nextTask = loopTask(mainTask.taskId, "Next workspace task", "done");
    store.bindLoopWorkspace(harness.bot.uid, THREAD_GROUP, nextWorkspace, [nextTask]);
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: THREAD_GROUP,
      workspaceId: nextWorkspace.workspaceId,
      taskId: nextTask.taskId,
    }, DETAILS_THREAD.channelId);
    expect(store.getLoopBinding(harness.bot.uid, THREAD_GROUP)?.workspaceId).toBe(nextWorkspace.workspaceId);
    const switchedVersion = databaseVersion(observer);

    const afterSwitch = await snapshotApi.chatDetailsSnapshot(DETAILS_THREAD, new AbortController().signal);
    expect(afterSwitch.tasks).toEqual([
      {
        workspaceId: workspace.workspaceId,
        taskId: mainTask.taskId,
        title: mainTask.title,
        status: mainTask.status,
        url: null,
      },
      {
        workspaceId: nextWorkspace.workspaceId,
        taskId: nextTask.taskId,
        title: nextTask.title,
        status: nextTask.status,
        url: null,
      },
      {
        workspaceId: workspace.workspaceId,
        taskId: missingSnapshotTaskId,
        title: null,
        status: null,
        url: null,
      },
    ]);
    expect(harness.workers.workers).toHaveLength(workersBefore);

    const sibling = await snapshotApi.chatDetailsSnapshot(SIBLING_THREAD, new AbortController().signal);
    expect(sibling).toMatchObject({
      channelType: SIBLING_THREAD.channelType,
      channelId: SIBLING_THREAD.channelId,
      references: { pr: [siblingPr], mr: [] },
      tasks: [],
    });
    expect(JSON.stringify(sibling)).not.toContain(mainPr);
    expect(JSON.stringify(sibling)).not.toContain(mainTask.title);

    const unbound = await snapshotApi.chatDetailsSnapshot(UNBOUND_THREAD, new AbortController().signal);
    expect(unbound).toMatchObject({
      channelType: UNBOUND_THREAD.channelType,
      channelId: UNBOUND_THREAD.channelId,
      context: { source: "unavailable", usedTokens: null, contextWindow: null, percent: null, model: null },
      references: { pr: [], mr: [] },
      tasks: [],
    });
    const parentGroup = await snapshotApi.chatDetailsSnapshot(
      { channelType: 2, channelId: THREAD_GROUP },
      new AbortController().signal,
    );
    expect(parentGroup.references).toEqual({ pr: [], mr: [] });
    expect(parentGroup.tasks).toEqual([]);
    expect(harness.workers.workers).toHaveLength(workersBefore);
    expect(databaseVersion(observer)).toBe(switchedVersion);
  } finally {
    releasePrompt.resolve();
    observer.close();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("details snapshots use an identity-matched persisted usage sample and return empty unavailable data", async () => {
  const harness = await createCoreHarness();
  const store = await openChannelStore(harness.stateDir);
  const observer = new Database(join(harness.stateDir, "channel.sqlite"));
  const target: ChatTarget = { channelType: 1, channelId: "dm-persisted-details" };

  try {
    const cwd = join(harness.root, "persisted-details-workspace");
    const sessionFile = join(harness.root, "sessions", "persisted-details.jsonl");
    const sessionId = "persisted-details-session";
    await mkdir(cwd, { recursive: true });
    await writeBoundSession(sessionFile, sessionId, cwd);
    const binding: BindingRecord = {
      botUid: harness.bot.uid,
      ...target,
      sessionId,
      cwd,
      sessionFile,
      cwdPinned: true,
      sessionSwitchedAt: null,
    };
    store.ensureBinding(binding);
    const file = await stat(sessionFile);
    store.saveSessionContextSnapshot({
      botUid: harness.bot.uid,
      ...target,
      sessionId,
      sessionFile,
      cwd,
      usedTokens: 2_048,
      contextWindow: 8_192,
      percent: 25,
      model: "fixture/persisted-details",
      observedAt: "2026-11-11T08:00:00.000Z",
      fileSize: file.size,
      fileMtimeMs: file.mtimeMs,
    });
    const versionBefore = databaseVersion(observer);
    const snapshotApi = chatDetailsSnapshotApi(harness.service);

    const persisted = await snapshotApi.chatDetailsSnapshot(target, new AbortController().signal);

    expect(persisted).toMatchObject({
      channelType: target.channelType,
      channelId: target.channelId,
      context: {
        source: "persisted",
        stale: false,
        usedTokens: 2_048,
        contextWindow: 8_192,
        percent: 25,
        model: "fixture/persisted-details",
        observedAt: "2026-11-11T08:00:00.000Z",
      },
      references: { pr: [], mr: [] },
      tasks: [],
    });
    expect(persisted.context.note).toMatch(/非实时|历史/u);
    expect(harness.workers.workers).toHaveLength(0);

    const unavailable = await snapshotApi.chatDetailsSnapshot(UNBOUND_THREAD, new AbortController().signal);
    expect(unavailable).toMatchObject({
      context: {
        source: "unavailable",
        usedTokens: null,
        contextWindow: null,
        percent: null,
        model: null,
        observedAt: null,
      },
      references: { pr: [], mr: [] },
      tasks: [],
    });
    expect(harness.workers.workers).toHaveLength(0);
    expect(databaseVersion(observer)).toBe(versionBefore);
  } finally {
    observer.close();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("details snapshots discard a live usage result when the session binding changes during the async read", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const usageStarted = Promise.withResolvers<void>();
  const releaseUsage = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async () => {
      promptStarted.resolve();
      await releasePrompt.promise;
    },
  });
  const store = await openChannelStore(harness.stateDir);
  const observer = new Database(join(harness.stateDir, "channel.sqlite"));

  try {
    await harness.service.submitInbound(inbound({
      ...CHAT_A,
      messageId: "details-binding-race",
      text: "binding race fixture",
    }));
    await promptStarted.promise;
    await waitForStatus(harness.service, CHAT_A, status => status.state === "running");

    const binding = store.getBinding(harness.bot.uid, CHAT_A);
    expect(binding).not.toBeNull();
    const worker = harness.workers.workers[0]!;
    const readContextUsage = worker.contextUsage.bind(worker);
    worker.contextUsage = async signal => {
      usageStarted.resolve();
      await releaseUsage.promise;
      return readContextUsage(signal);
    };
    const snapshotApi = chatDetailsSnapshotApi(harness.service);
    const detailsPromise = snapshotApi.chatDetailsSnapshot(CHAT_A, new AbortController().signal);
    await usageStarted.promise;

    const replacementSessionId = "replacement-session-during-details";
    const replacementSessionFile = join(harness.root, "replacement-session.jsonl");
    await writeBoundSession(replacementSessionFile, replacementSessionId, binding!.cwd);
    store.ensureBinding({
      ...binding!,
      sessionId: replacementSessionId,
      sessionFile: replacementSessionFile,
    });
    const versionAfterRebind = databaseVersion(observer);
    releaseUsage.resolve();

    const details = await detailsPromise;

    expect(details.context).toMatchObject({
      source: "unavailable",
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
      observedAt: null,
    });
    expect(store.getSessionContextSnapshot(harness.bot.uid, CHAT_A)).toBeNull();
    expect(databaseVersion(observer)).toBe(versionAfterRebind);
    expect(harness.workers.workers).toHaveLength(1);
  } finally {
    releaseUsage.resolve();
    releasePrompt.resolve();
    observer.close();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("cancelling a live details context read does not persist a snapshot or start a worker", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const usageStarted = Promise.withResolvers<void>();
  const releaseUsage = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async () => {
      promptStarted.resolve();
      await releasePrompt.promise;
    },
  });
  const store = await openChannelStore(harness.stateDir);
  const observer = new Database(join(harness.stateDir, "channel.sqlite"));

  try {
    await harness.service.submitInbound(inbound({
      ...CHAT_B,
      messageId: "details-cancellation",
      text: "cancellation fixture",
    }));
    await promptStarted.promise;
    await waitForStatus(harness.service, CHAT_B, status => status.state === "running");

    const worker = harness.workers.workers[0]!;
    const readContextUsage = worker.contextUsage.bind(worker);
    worker.contextUsage = async signal => {
      usageStarted.resolve();
      await releaseUsage.promise;
      return readContextUsage(signal);
    };
    const snapshotApi = chatDetailsSnapshotApi(harness.service);
    const requestController = new AbortController();
    const versionBefore = databaseVersion(observer);
    const detailsPromise = snapshotApi.chatDetailsSnapshot(CHAT_B, requestController.signal);
    await usageStarted.promise;
    requestController.abort(new DOMException("request cancelled", "AbortError"));
    releaseUsage.resolve();

    await expect(detailsPromise).rejects.toMatchObject({ name: "AbortError" });

    expect(store.getSessionContextSnapshot(harness.bot.uid, CHAT_B)).toBeNull();
    expect(databaseVersion(observer)).toBe(versionBefore);
    expect(harness.workers.workers).toHaveLength(1);
  } finally {
    releaseUsage.resolve();
    releasePrompt.resolve();
    observer.close();
    store.close();
    await closeCoreHarness(harness);
  }
});
