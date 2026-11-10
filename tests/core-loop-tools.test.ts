import { expect, test } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, HostTool, InboundMessage, ThreadSummary } from "../src/contracts.ts";
import { createHostTools, type HostToolHost } from "../src/host-tools.ts";
import { LoopThreadBindings } from "../src/loop/thread-bindings.ts";
import type { LoopReadPort, LoopTask, LoopTaskKey, LoopWorkspace } from "../src/loop/types.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { TaskRecord } from "../src/store/store-records.ts";
import { FakeTransport, FakeWorktrees } from "./fixtures/core-fakes.ts";

const BOT_UID = "bot-loop-tools";
const OWNER_UID = "owner-loop-tools";
const GROUP_ID = "group-loop-tools";
const OTHER_GROUP_ID = "group-loop-tools-other";
const LOOP_TASK_ID = "loop-task-17";
const MAIN_WORKSPACE: LoopWorkspace = { workspaceId: "workspace-main", name: "Main workspace", slug: "main-workspace" };
const OTHER_WORKSPACE: LoopWorkspace = { workspaceId: "workspace-other", name: "Other workspace", slug: "other-workspace" };

function loopTask(taskId: string): LoopTask {
  return {
    taskId,
    title: `Task ${taskId}`,
    description: null,
    status: "todo",
    priority: "normal",
    assigneeType: null,
    assigneeId: null,
    assigneeOctoUid: null,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-01T00:00:00.000Z",
    updatedAt: "2026-11-01T00:00:00.000Z",
  };
}

function threadRecord(groupId: string, shortId: string): ThreadSummary {
  return {
    shortId,
    groupId,
    channelId: `${groupId}____${shortId}`,
    name: `Topic ${shortId}`,
    creatorUid: OWNER_UID,
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-01T00:00:00.000Z",
    updatedAt: "2026-11-01T00:00:00.000Z",
  };
}

class BlockingCreateTransport extends FakeTransport {
  readonly started = Promise.withResolvers<ThreadSummary>();
  readonly release = Promise.withResolvers<void>();

  override async createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary> {
    const thread = await super.createThread(groupId, name, signal);
    this.started.resolve(thread);
    await this.release.promise;
    return thread;
  }
}

class UnknownCreateTransport extends FakeTransport {
  override async createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary> {
    await super.createThread(groupId, name, signal);
    throw new Error("connection closed after remote allocation");
  }
}

class DescriptionFailureTransport extends FakeTransport {
  override async updateGroupContext(
    target: { channelId: string; channelType: 1 | 2 | 5 },
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }> {
    if (target.channelType === 5) throw new Error("description write failed");
    return super.updateGroupContext(target, content, signal);
  }
}

interface LoopToolsFixture {
  root: string;
  store: ChannelStore;
  transport: FakeTransport;
  worktrees: FakeWorktrees | undefined;
  bindings: LoopThreadBindings;
  deliveredMessages: InboundMessage[];
  deliveryGuards: Array<() => boolean>;
  notices: string[];
  controls: {
    beforeDeliveryAccept?: () => Promise<void>;
    beforeTaskRead?: (taskId: string) => Promise<void>;
  };
  management(groupId?: string, messageId?: string): HostTool;
  automaticMessageId(taskId?: string, targetGroupId?: string): string;
  setTaskStatus(taskId: string, status: string): void;
  key(groupId?: string, workspace?: LoopWorkspace, taskId?: string): LoopTaskKey;
  close(): Promise<void>;
}

async function createFixture(options: {
  transport?: FakeTransport;
  groupId?: string;
  worktrees?: boolean;
} = {}): Promise<LoopToolsFixture> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-loop-thread-tools-")));
  const groupId = options.groupId ?? GROUP_ID;
  const worktrees = options.worktrees ? new FakeWorktrees(root) : undefined;
  const workspaces = [MAIN_WORKSPACE, OTHER_WORKSPACE];
  const tasks = [
    { workspaceId: MAIN_WORKSPACE.workspaceId, task: loopTask(LOOP_TASK_ID) },
    { workspaceId: OTHER_WORKSPACE.workspaceId, task: loopTask("loop-task-other") },
  ];
  const read: LoopReadPort = {
    async resolveWorkspace(selector) {
      const workspace = workspaces.find(
        (candidate) => candidate.workspaceId === selector || candidate.name === selector || candidate.slug === selector,
      );
      if (workspace === undefined) throw new Error(`unknown fixture workspace ${selector}`);
      return workspace;
    },
    async listTasks(workspaceId) {
      return tasks.filter((entry) => entry.workspaceId === workspaceId).map((entry) => entry.task);
    },
    async getTask(workspaceId, taskId) {
      await controls.beforeTaskRead?.(taskId);
      const entry = tasks.find((candidate) => candidate.workspaceId === workspaceId && candidate.task.taskId === taskId);
      if (entry === undefined) throw new Error(`fixture task ${taskId} is not in ${workspaceId}`);
      return entry.task;
    },
  };
  const store = await ChannelStore.open(join(root, "channel.sqlite"));
  store.setBotUid(BOT_UID);
  store.setOwnerId(BOT_UID, OWNER_UID);
  store.bindLoopWorkspace(BOT_UID, groupId, MAIN_WORKSPACE, []);
  const transport = options.transport ?? new FakeTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const bindings = new LoopThreadBindings(store, read, BOT_UID);
  const deliveryGuards: Array<() => boolean> = [];
  const deliveredMessages: InboundMessage[] = [];
  const notices: string[] = [];
  const controls: {
    beforeDeliveryAccept?: () => Promise<void>;
    beforeTaskRead?: (taskId: string) => Promise<void>;
  } = {};
  const automaticMessages = new Map<string, string>();
  await bindings.recover();

  const management = (
    targetGroupId: string = groupId,
    messageId = "inbound-tool-call",
  ): HostTool => {
    const task: TaskRecord = {
      id: "channel-execution-42",
      botUid: BOT_UID,
      channelId: targetGroupId,
      channelType: 2,
      messageId,
      senderId: OWNER_UID,
      text: "manage Loop task routing",
      sessionId: "session-loop-tools",
      cwd: root,
      state: "running",
      resultText: null,
      errorText: null,
      messageSeq: 1,
      historyEligible: true,
      relaySuppressed: false,
    };
    const host = {
      stateDir: root,
      transport,
      worktrees,
      store,
      botUid: BOT_UID,
      loop: bindings,
      threadReferences: store.getThreadReferences.bind(store, BOT_UID),
      deleteThreadReferences: store.deleteThreadReferences.bind(store, BOT_UID),
      tryAcquireDestructiveResources: async () => undefined,
      enqueueText: (_target: ChatTarget, text: string) => notices.push(text),
      flushTarget: async () => {},
      threadArchiveGeneration: () => 0,
      planThreadArchive: () => {},
      publishThreadMergeSuccess: async () => {},
      replayThreadMergeSuccess: async () => null,
      deliverToChat: async (
        message: InboundMessage,
        deliveryGuard?: () => boolean,
        beforeAccept?: () => Promise<void>,
      ) => {
        if (deliveryGuard !== undefined && !deliveryGuard()) throw new Error("route changed");
        if (beforeAccept !== undefined) {
          await controls.beforeDeliveryAccept?.();
          await beforeAccept();
        }
        if (deliveryGuard !== undefined && !deliveryGuard()) throw new Error("route changed");
        deliveredMessages.push(message);
        if (deliveryGuard !== undefined) deliveryGuards.push(deliveryGuard);
        return "queued" as const;
      },
      askQuestions: async () => "",
      forkSession: async () => "",
      channelStatus: () => ({ pid: 1, restart_pending: false, running_targets: [], queued_tasks: 0, blocked_cwds: 0 }),
      scheduleRestart: () => "",
    } as unknown as HostToolHost;
    const tool = createHostTools(host, task).find(({ name }) => name === "octo_management");
    if (tool === undefined) throw new Error("management tool missing");
    return tool;
  };
  const automaticMessageId = (
    taskId: string = LOOP_TASK_ID,
    targetGroupId: string = groupId,
  ): string => {
    const identity = `${targetGroupId}:${taskId}`;
    const existing = automaticMessages.get(identity);
    if (existing !== undefined) return existing;
    const binding = store.getLoopBinding(BOT_UID, targetGroupId);
    const entry = tasks.find(
      (candidate) => candidate.workspaceId === MAIN_WORKSPACE.workspaceId
        && candidate.task.taskId === taskId,
    );
    if (binding === null || entry === undefined) throw new Error(`unknown automatic Loop task ${taskId}`);
    entry.task.assigneeType = "member";
    entry.task.assigneeOctoUid = OWNER_UID;
    store.commitLoopScan(binding, [entry.task]);
    const input = store.listLoopInputs(BOT_UID).find(
      (candidate) => candidate.groupId === targetGroupId
        && (candidate.taskId === taskId || candidate.tasks.some((item) => item.taskId === taskId)),
    );
    if (input === undefined) throw new Error(`no durable Loop input for ${taskId}`);
    const inputTarget = { channelType: 2 as const, channelId: targetGroupId };
    if (input.state === "pending") {
      if (!store.claimLoopInputIntent(input.id, inputTarget)) throw new Error("Loop input intent was not claimed");
      store.markLoopInputDelivered(input.id);
    }
    const messageId = `loop:${input.id}`;
    automaticMessages.set(identity, messageId);
    return messageId;
  };
  const setTaskStatus = (taskId: string, status: string): void => {
    const entry = tasks.find(
      (candidate) => candidate.workspaceId === MAIN_WORKSPACE.workspaceId
        && candidate.task.taskId === taskId,
    );
    if (entry === undefined) throw new Error(`unknown fixture Loop task ${taskId}`);
    entry.task.status = status;
  };
  return {
    root,
    store,
    transport,
    worktrees,
    bindings,
    deliveredMessages,
    deliveryGuards,
    notices,
    controls,
    management,
    automaticMessageId,
    setTaskStatus,
    key: (targetGroupId = groupId, workspace = MAIN_WORKSPACE, taskId = LOOP_TASK_ID) => ({
      botUid: BOT_UID,
      groupId: targetGroupId,
      workspaceId: workspace.workspaceId,
      taskId,
    }),
    async close() {
      store.close();
      await rm(root, { recursive: true, force: true });
    },
  };
}

function execute(tool: HostTool, args: Record<string, unknown>, toolCallId = "native-call-1"): Promise<string> {
  return tool.execute(args, new AbortController().signal, toolCallId);
}

function createCalls(transport: FakeTransport, groupId = GROUP_ID): number {
  return transport.managementCalls.filter(
    ({ action, groupId: calledGroup }) => action === "create-thread" && calledGroup === groupId,
  ).length;
}

test("recovery makes unconfirmed reservations unknown and preserves allocated partials", async () => {
  const fixture = await createFixture();
  try {
    const reservedKey = fixture.key(GROUP_ID, MAIN_WORKSPACE, "reserved-task");
    const allocatedKey = fixture.key(GROUP_ID, MAIN_WORKSPACE, "allocated-task");
    fixture.store.reserveLoopThreadCreation(reservedKey, "operation-reserved");
    fixture.store.reserveLoopThreadCreation(allocatedKey, "operation-allocated");
    fixture.store.recordLoopThreadAllocated(
      allocatedKey,
      "operation-allocated",
      threadRecord(GROUP_ID, "partial-thread").channelId,
    );

    await fixture.bindings.recover();

    expect(fixture.store.getLoopThreadCreation(reservedKey)).toMatchObject({ state: "unknown" });
    expect(fixture.store.getLoopThreadCreation(allocatedKey)).toMatchObject({
      state: "allocated",
      threadChannelId: threadRecord(GROUP_ID, "partial-thread").channelId,
    });
  } finally {
    await fixture.close();
  }
});

test("reserved and unknown Loop allocations block only unowned children across workspace replacement", async () => {
  const fixture = await createFixture();
  try {
    const key = fixture.key(GROUP_ID, MAIN_WORKSPACE, "unknown-task");
    fixture.store.reserveLoopThreadCreation(key, "operation-unknown");
    const target = { channelId: `${GROUP_ID}____orphan`, channelType: 5 as const };
    expect(fixture.bindings.isTargetBlocked(target)).toBe(true);
    fixture.store.markLoopThreadCreationUnknown(key, "operation-unknown", "remote identity unavailable");
    expect(fixture.bindings.isTargetBlocked(target)).toBe(true);

    fixture.store.bindLoopWorkspace(BOT_UID, GROUP_ID, OTHER_WORKSPACE, []);
    expect(fixture.bindings.isTargetBlocked(target)).toBe(true);

    const owned = { channelId: `${GROUP_ID}____owned`, channelType: 5 as const };
    fixture.store.ensureBinding({
      botUid: BOT_UID,
      ...owned,
      sessionId: "session-owned-thread",
      cwd: fixture.root,
      sessionFile: null,
      cwdPinned: true,
    });
    expect(fixture.bindings.isTargetBlocked(owned)).toBe(false);
  } finally {
    await fixture.close();
  }
});

test("an explicit thread-bind releases a known partial Loop allocation block", async () => {
  const fixture = await createFixture();
  try {
    const thread = threadRecord(GROUP_ID, "partial-allocation");
    fixture.store.reserveLoopThreadCreation(fixture.key(), "operation-partial");
    fixture.store.recordLoopThreadAllocated(fixture.key(), "operation-partial", thread.channelId);
    fixture.transport.setThreads(GROUP_ID, [thread]);
    const target = { channelId: thread.channelId, channelType: 5 as const };
    expect(fixture.bindings.isTargetBlocked(target)).toBe(true);

    await execute(fixture.management(), {
      action: "thread-bind",
      task_id: LOOP_TASK_ID,
      short_id: thread.shortId,
    });

    expect(fixture.bindings.isTargetBlocked(target)).toBe(false);
  } finally {
    await fixture.close();
  }
});

test("concurrent task-aware create and native tool-call replay allocate once", async () => {
  const transport = new BlockingCreateTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const fixture = await createFixture({ transport });
  try {
    const tool = fixture.management();
    const args = { action: "create-thread", name: "Task 17", task_id: LOOP_TASK_ID };
    const first = execute(tool, args, "native-create-17");
    const allocated = await transport.started.promise;
    const replay = execute(tool, args, "native-create-17");
    transport.release.resolve();
    const [firstResult, replayResult] = await Promise.all([first, replay]);
    expect(JSON.parse(replayResult).channel_id).toBe(allocated.channelId);
    expect(JSON.parse(firstResult).channel_id).toBe(allocated.channelId);
    expect(createCalls(transport)).toBe(1);
    expect(fixture.store.getLoopTaskRoute(fixture.key())?.threadChannelId).toBe(allocated.channelId);
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({
      state: "ready",
      threadChannelId: allocated.channelId,
    });
    expect(JSON.parse(await execute(tool, args, "native-create-17")).channel_id).toBe(allocated.channelId);
    expect(createCalls(transport)).toBe(1);
  } finally {
    transport.release.resolve();
    await fixture.close();
  }
});

test("thread-bind rebinds explicitly and thread info/list report source IDs", async () => {
  const fixture = await createFixture();
  try {
    fixture.transport.setThreads(GROUP_ID, [threadRecord(GROUP_ID, "topic-a"), threadRecord(GROUP_ID, "topic-b")]);
    const tool = fixture.management();
    const first = await execute(tool, { action: "thread-bind", task_id: LOOP_TASK_ID, short_id: "topic-a" });
    expect(first).toContain(MAIN_WORKSPACE.workspaceId);
    expect(first).toContain(LOOP_TASK_ID);
    const firstRoute = fixture.store.getLoopTaskRoute(fixture.key());
    await execute(tool, { action: "thread-bind", task_id: LOOP_TASK_ID, short_id: "topic-a" });
    expect(fixture.store.getLoopTaskRoute(fixture.key())?.version).toBe(firstRoute?.version);
    const info = await execute(tool, { action: "thread-info", short_id: "topic-a" });
    const list = await execute(tool, { action: "list-threads" });
    expect(info).toContain(MAIN_WORKSPACE.workspaceId);
    expect(info).toContain(LOOP_TASK_ID);
    expect(list).toContain(MAIN_WORKSPACE.workspaceId);
    expect(list).toContain(LOOP_TASK_ID);
    const before = fixture.store.getLoopTaskRoute(fixture.key());
    const rebound = await execute(tool, { action: "thread-bind", task_id: LOOP_TASK_ID, short_id: "topic-b" });
    expect(rebound).toContain(threadRecord(GROUP_ID, "topic-a").channelId);
    expect(rebound).toContain(threadRecord(GROUP_ID, "topic-b").channelId);
    const after = fixture.store.getLoopTaskRoute(fixture.key());
    expect(after?.threadChannelId).toBe(threadRecord(GROUP_ID, "topic-b").channelId);
    expect(after?.version).toBe((before?.version ?? 0) + 1);
  } finally {
    await fixture.close();
  }
});

test("automatic Loop tasks can create and dispatch while the latest status is open", async () => {
  const fixture = await createFixture();
  try {
    const messageId = fixture.automaticMessageId();
    const tool = fixture.management(GROUP_ID, messageId);
    const created = JSON.parse(await execute(tool, {
      action: "create-thread",
      name: "Open automatic task",
      task_id: LOOP_TASK_ID,
    })) as { channel_id: string; short_id: string; route_bound: boolean };
    expect(created.route_bound).toBe(true);

    const dispatched = JSON.parse(await execute(tool, {
      action: "thread-dispatch",
      short_id: created.short_id,
      task_id: LOOP_TASK_ID,
      text: "open task work",
    })) as { channel_id: string; delivery: string };
    expect(dispatched).toMatchObject({ channel_id: created.channel_id, delivery: "queued" });
    expect(fixture.deliveredMessages).toHaveLength(1);
    expect(fixture.notices.some((text) => text.includes("来自群会话的派发任务") && text.includes("open task work"))).toBe(true);
  } finally {
    await fixture.close();
  }
});

test("automatic Loop creation rechecks status immediately before remote allocation", async () => {
  const fixture = await createFixture();
  try {
    const messageId = fixture.automaticMessageId();
    let reads = 0;
    fixture.controls.beforeTaskRead = async (taskId) => {
      if (taskId !== LOOP_TASK_ID) return;
      reads += 1;
      if (reads === 3) fixture.setTaskStatus(taskId, "done");
    };
    await expect(execute(fixture.management(GROUP_ID, messageId), {
      action: "create-thread",
      name: "Task closed during creation",
      task_id: LOOP_TASK_ID,
    })).rejects.toThrow(/closed Loop task/);
    expect(createCalls(fixture.transport)).toBe(0);
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toBeNull();
  } finally {
    await fixture.close();
  }
});

test("workspace-backed automatic creation rechecks status immediately before remote allocation", async () => {
  const fixture = await createFixture({ worktrees: true });
  try {
    execFileSync("git", ["init", "--quiet", fixture.root]);
    const messageId = fixture.automaticMessageId();
    let reads = 0;
    fixture.controls.beforeTaskRead = async (taskId) => {
      if (taskId !== LOOP_TASK_ID) return;
      reads += 1;
      if (reads === 4) fixture.setTaskStatus(taskId, "cancelled");
    };
    await expect(execute(fixture.management(GROUP_ID, messageId), {
      action: "create-thread",
      name: "Workspace task closed during creation",
      task_id: LOOP_TASK_ID,
      worktree_name: "task-seventeen",
      projects: [{ name: "main-repository", repo: fixture.root }],
    })).rejects.toThrow(/closed Loop task/);
    expect(createCalls(fixture.transport)).toBe(0);
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toBeNull();
  } finally {
    await fixture.close();
  }
});

test.each(["done", "cancelled", "backlog"])(
  "automatic Loop creation rejects a latest closed status: %s",
  async (status) => {
    const fixture = await createFixture();
    try {
      const messageId = fixture.automaticMessageId();
      fixture.setTaskStatus(LOOP_TASK_ID, status);
      await expect(execute(fixture.management(GROUP_ID, messageId), {
        action: "create-thread",
        name: "Closed automatic task",
        task_id: LOOP_TASK_ID,
      })).rejects.toThrow(/closed Loop task/);
      expect(createCalls(fixture.transport)).toBe(0);
      expect(fixture.store.getLoopThreadCreation(fixture.key())).toBeNull();
    } finally {
      await fixture.close();
    }
  },
);

test("automatic Loop create and dispatch require task_id, while explicit thread-bind remains available", async () => {
  const fixture = await createFixture();
  try {
    const messageId = fixture.automaticMessageId();
    fixture.setTaskStatus(LOOP_TASK_ID, "done");
    const tool = fixture.management(GROUP_ID, messageId);
    await expect(execute(tool, {
      action: "create-thread",
      name: "Unscoped automatic thread",
    })).rejects.toThrow(/task_id is required/);

    const thread = threadRecord(GROUP_ID, "manual-bind");
    fixture.transport.setThreads(GROUP_ID, [thread]);
    await expect(execute(tool, {
      action: "thread-dispatch",
      short_id: thread.shortId,
      text: "unscoped automatic dispatch",
    })).rejects.toThrow(/task_id is required/);
    expect(fixture.deliveredMessages).toHaveLength(0);
    expect(fixture.notices).toHaveLength(0);

    await execute(tool, {
      action: "thread-bind",
      task_id: LOOP_TASK_ID,
      short_id: thread.shortId,
    });
    expect(fixture.store.getLoopTaskRoute(fixture.key())?.threadChannelId).toBe(thread.channelId);
  } finally {
    await fixture.close();
  }
});

test.each(["done", "cancelled", "backlog"])(
  "automatic Loop dispatch rejects a latest closed status: %s",
  async (status) => {
    const fixture = await createFixture();
    try {
      const messageId = fixture.automaticMessageId();
      const thread = threadRecord(GROUP_ID, "closed-dispatch");
      fixture.transport.setThreads(GROUP_ID, [thread]);
      fixture.store.bindLoopTask(fixture.key(), thread.channelId);
      fixture.setTaskStatus(LOOP_TASK_ID, status);

      await expect(execute(fixture.management(GROUP_ID, messageId), {
        action: "thread-dispatch",
        short_id: thread.shortId,
        task_id: LOOP_TASK_ID,
        text: "must not be dispatched",
      })).rejects.toThrow(/closed Loop task/);
      expect(fixture.deliveredMessages).toHaveLength(0);
    } finally {
      await fixture.close();
    }
  },
);

test("automatic Loop dispatch rechecks status at the destination acceptance boundary", async () => {
  const fixture = await createFixture();
  try {
    const messageId = fixture.automaticMessageId();
    const thread = threadRecord(GROUP_ID, "dispatch-status-race");
    fixture.transport.setThreads(GROUP_ID, [thread]);
    fixture.store.bindLoopTask(fixture.key(), thread.channelId);
    fixture.controls.beforeDeliveryAccept = async () => fixture.setTaskStatus(LOOP_TASK_ID, "cancelled");

    await expect(execute(fixture.management(GROUP_ID, messageId), {
      action: "thread-dispatch",
      short_id: thread.shortId,
      task_id: LOOP_TASK_ID,
      text: "must not be accepted after cancellation",
    })).rejects.toThrow(/closed Loop task/);
    expect(fixture.notices).toHaveLength(0);
  } finally {
    await fixture.close();
  }
});

test("manual explicit create and dispatch remain available for a closed Loop task", async () => {
  const fixture = await createFixture();
  try {
    fixture.setTaskStatus(LOOP_TASK_ID, "done");
    const tool = fixture.management(GROUP_ID, "human-explicit-task");
    const created = JSON.parse(await execute(tool, {
      action: "create-thread",
      name: "Manual explicit task",
      task_id: LOOP_TASK_ID,
    })) as { channel_id: string; short_id: string };

    const dispatch = JSON.parse(await execute(tool, {
      action: "thread-dispatch",
      short_id: created.short_id,
      task_id: LOOP_TASK_ID,
      text: "human explicitly assigned this work",
    })) as { channel_id: string };
    expect(dispatch.channel_id).toBe(created.channel_id);
    expect(fixture.deliveredMessages).toHaveLength(1);
  } finally {
    await fixture.close();
  }
});

test("task-aware dispatch rejects a target other than its current Loop route", async () => {
  const fixture = await createFixture();
  try {
    fixture.transport.setThreads(GROUP_ID, [threadRecord(GROUP_ID, "topic-a"), threadRecord(GROUP_ID, "topic-b")]);
    fixture.store.bindLoopTask(fixture.key(), threadRecord(GROUP_ID, "topic-a").channelId);
    await expect(execute(fixture.management(), {
      action: "thread-dispatch", short_id: "topic-b", task_id: LOOP_TASK_ID, text: "start task work",
    })).rejects.toThrow();
  } finally {
    await fixture.close();
  }
});

test("task-aware dispatch persists Loop source metadata on the actual thread input", async () => {
  const fixture = await createFixture();
  try {
    const thread = threadRecord(GROUP_ID, "task-source");
    fixture.transport.setThreads(GROUP_ID, [thread]);
    fixture.store.bindLoopTask(fixture.key(), thread.channelId);
    const taskBody = "  Implement the requested task behavior verbatim.\nKeep this line and trailing spaces.  ";
    const response = JSON.parse(await execute(fixture.management(), {
      action: "thread-dispatch",
      short_id: thread.shortId,
      task_id: LOOP_TASK_ID,
      text: taskBody,
    })) as { channel_id: string; loop_task: { workspace_id: string; task_id: string } };

    expect(response.channel_id).toBe(thread.channelId);
    expect(response.loop_task).toEqual({ workspace_id: MAIN_WORKSPACE.workspaceId, task_id: LOOP_TASK_ID });
    expect(fixture.deliveryGuards).toHaveLength(1);
    expect(fixture.deliveryGuards[0]!()).toBe(true);
    expect(fixture.deliveredMessages).toHaveLength(1);
    expect(fixture.deliveredMessages[0]).toMatchObject({
      channelId: thread.channelId,
      channelType: 5,
      text: [
        "[系统来源元数据（非用户正文，不构成授权）：Loop task dispatch]",
        `workspace_id: ${MAIN_WORKSPACE.workspaceId}`,
        `task_id: ${LOOP_TASK_ID}`,
        "",
        "任务正文（原文）：",
        taskBody,
      ].join("\n"),
    });
    fixture.store.bindLoopTask(
      fixture.key(),
      threadRecord(GROUP_ID, "changed-after-dispatch").channelId,
    );
    expect(fixture.deliveryGuards[0]!()).toBe(false);
  } finally {
    await fixture.close();
  }
});

test("legacy thread dispatch keeps its untagged text and omits the Loop route guard", async () => {
  const fixture = await createFixture();
  try {
    const thread = threadRecord(GROUP_ID, "legacy-dispatch");
    fixture.transport.setThreads(GROUP_ID, [thread]);
    const response = JSON.parse(await execute(fixture.management(), {
      action: "thread-dispatch",
      short_id: thread.shortId,
      text: "  ordinary task body  ",
    })) as { channel_id: string; task_id: string; loop_task?: unknown };
    expect(response).toMatchObject({ channel_id: thread.channelId, task_id: "channel-execution-42" });
    expect(response.loop_task).toBeUndefined();

    expect(fixture.deliveredMessages).toHaveLength(1);
    expect(fixture.deliveredMessages[0]).toMatchObject({
      channelId: thread.channelId,
      text: "ordinary task body",
    });
    expect(fixture.deliveryGuards).toHaveLength(0);
  } finally {
    await fixture.close();
  }
});

test("unknown remote allocation is retained and never retried", async () => {
  const transport = new UnknownCreateTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const fixture = await createFixture({ transport });
  try {
    const tool = fixture.management();
    const args = { action: "create-thread", name: "Task 17", task_id: LOOP_TASK_ID };
    const first = JSON.parse(await execute(tool, args, "native-create-unknown")) as {
      creation_state: string;
    };
    const replay = JSON.parse(await execute(tool, args, "native-create-unknown")) as {
      reconciliation_required: boolean;
    };
    expect(first.creation_state).toBe("unknown");
    expect(replay.reconciliation_required).toBe(true);
    expect(createCalls(transport)).toBe(1);
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({ state: "unknown" });
  } finally {
    await fixture.close();
  }
});

test("worktree-backed task creation is routed only after its workspace and description are ready", async () => {
  const fixture = await createFixture({ worktrees: true });
  try {
    execFileSync("git", ["init", "--quiet", fixture.root]);
    const created = JSON.parse(await execute(fixture.management(), {
      action: "create-thread",
      name: "Task 17",
      task_id: LOOP_TASK_ID,
      worktree_name: "task-seventeen",
      projects: [{ name: "main-repository", repo: fixture.root }],
    }, "native-create-worktree")) as { channel_id: string; ready: boolean };
    const threadTarget = { channelId: created.channel_id, channelType: 5 as const };
    expect(created.ready).toBe(true);
    expect(fixture.store.getThreadWorkspace(BOT_UID, threadTarget)).toMatchObject({
      status: "ready",
      projects: [{ created: true }],
    });
    expect(fixture.store.getLoopTaskRoute(fixture.key())?.threadChannelId).toBe(created.channel_id);
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({
      state: "ready",
      threadChannelId: created.channel_id,
    });
  } finally {
    await fixture.close();
  }
});

test("worktree preparation failure retains the allocated thread and never re-creates it", async () => {
  const fixture = await createFixture({ worktrees: true });
  try {
    execFileSync("git", ["init", "--quiet", fixture.root]);
    fixture.worktrees!.failure = new Error("fixture worktree creation failed");
    const args = {
      action: "create-thread",
      name: "Task 17",
      task_id: LOOP_TASK_ID,
      worktree_name: "task-seventeen",
      projects: [{ name: "main-repository", repo: fixture.root }],
    };
    const first = JSON.parse(await execute(fixture.management(), args, "native-create-worktree-partial")) as {
      ready: boolean;
      creation_state: string;
      allocated_channel_id: string;
    };
    const replay = JSON.parse(await execute(fixture.management(), args, "native-create-worktree-partial")) as {
      reconciliation_required: boolean;
    };
    expect(first.ready).toBe(false);
    expect(first.creation_state).toBe("allocated");
    expect(first.allocated_channel_id).toBe(threadRecord(GROUP_ID, "thread-1").channelId);
    expect(replay.reconciliation_required).toBe(true);
    expect(createCalls(fixture.transport)).toBe(1);
    expect(fixture.store.getLoopTaskRoute(fixture.key())).toBeNull();
  } finally {
    await fixture.close();
  }
});

test("known allocation with failed description preparation stays unbound and is not recreated", async () => {
  const transport = new DescriptionFailureTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const fixture = await createFixture({ transport });
  try {
    const tool = fixture.management();
    const args = {
      action: "create-thread", name: "Task 17", task_id: LOOP_TASK_ID, background: "Prepare a task thread",
    };
    await execute(tool, args, "native-create-description-failure");
    expect(fixture.store.getLoopTaskRoute(fixture.key())).toBeNull();
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({ state: "allocated" });
    await execute(tool, args, "native-create-description-failure");
    expect(createCalls(transport)).toBe(1);
    expect(fixture.store.getLoopTaskRoute(fixture.key())).toBeNull();
  } finally {
    await fixture.close();
  }
});

test("workspace replacement during creation preserves the allocation without publishing it", async () => {
  const transport = new BlockingCreateTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const fixture = await createFixture({ transport });
  try {
    const pending = execute(fixture.management(), {
      action: "create-thread", name: "Task 17", task_id: LOOP_TASK_ID,
    }, "native-create-workspace-race");
    const allocated = await transport.started.promise;
    fixture.store.bindLoopWorkspace(BOT_UID, GROUP_ID, OTHER_WORKSPACE, []);
    transport.release.resolve();
    await pending;
    expect(fixture.store.getLoopTaskRoute(fixture.key())).toBeNull();
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({
      threadChannelId: allocated.channelId, state: "superseded",
    });
  } finally {
    transport.release.resolve();
    await fixture.close();
  }
});

test("explicit thread-bind during creation wins the route compare-and-set", async () => {
  const transport = new BlockingCreateTransport({ bot: { uid: BOT_UID, name: "Loop Fixture Bot" } });
  const fixture = await createFixture({ transport });
  try {
    transport.setThreads(GROUP_ID, [threadRecord(GROUP_ID, "existing-target")]);
    const tool = fixture.management();
    const pending = execute(tool, { action: "create-thread", name: "Task 17", task_id: LOOP_TASK_ID }, "native-create-bind-race");
    const allocated = await transport.started.promise;
    await execute(tool, { action: "thread-bind", task_id: LOOP_TASK_ID, short_id: "existing-target" });
    transport.release.resolve();
    await pending;
    expect(fixture.store.getLoopTaskRoute(fixture.key())?.threadChannelId).toBe(
      threadRecord(GROUP_ID, "existing-target").channelId,
    );
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toMatchObject({
      threadChannelId: allocated.channelId, state: "superseded",
    });
    const existing = JSON.parse(await execute(tool, {
      action: "create-thread",
      name: "Task 17",
      task_id: LOOP_TASK_ID,
    })) as { superseded_creation: { allocated_channel_id: string } };
    expect(existing.superseded_creation.allocated_channel_id).toBe(allocated.channelId);
    expect(createCalls(transport)).toBe(1);
  } finally {
    transport.release.resolve();
    await fixture.close();
  }
});

test("existing target is reused and separate multi-thread creation is rejected", async () => {
  const fixture = await createFixture();
  try {
    fixture.transport.setThreads(GROUP_ID, [threadRecord(GROUP_ID, "existing-target")]);
    fixture.store.bindLoopTask(fixture.key(), threadRecord(GROUP_ID, "existing-target").channelId);
    const tool = fixture.management();
    const existing = JSON.parse(await execute(tool, {
      action: "create-thread", name: "do not duplicate", task_id: LOOP_TASK_ID,
    }, "native-create-existing")) as { channel_id: string };
    expect(existing.channel_id).toBe(threadRecord(GROUP_ID, "existing-target").channelId);
    expect(createCalls(fixture.transport)).toBe(0);
    await expect(execute(tool, {
      action: "create-thread", name: "separate projects", task_id: "loop-task-other",
      layout: "separate", worktree_name: "separate-task",
      projects: [
        { name: "project-one", repo: fixture.root },
        { name: "project-two", repo: fixture.root },
      ],
    }, "native-create-separate")).rejects.toThrow(/separate/i);
    expect(createCalls(fixture.transport)).toBe(0);
  } finally {
    await fixture.close();
  }
});

test("Loop task routes are isolated to the current parent group", async () => {
  const fixture = await createFixture();
  try {
    fixture.store.bindLoopTask(fixture.key(), threadRecord(GROUP_ID, "topic-a").channelId);
    fixture.store.bindLoopWorkspace(BOT_UID, OTHER_GROUP_ID, MAIN_WORKSPACE, []);
    fixture.transport.setThreads(OTHER_GROUP_ID, [threadRecord(OTHER_GROUP_ID, "topic-a")]);
    await expect(execute(fixture.management(OTHER_GROUP_ID), {
      action: "thread-dispatch", short_id: "topic-a", task_id: LOOP_TASK_ID, text: "must stay group-local",
    })).rejects.toThrow();
    expect(fixture.store.getLoopTaskRoute(fixture.key(OTHER_GROUP_ID))).toBeNull();
  } finally {
    await fixture.close();
  }
});

test("task IDs outside the current workspace are rejected before thread allocation", async () => {
  const fixture = await createFixture();
  try {
    await expect(execute(fixture.management(), {
      action: "create-thread", name: "wrong workspace task", task_id: "loop-task-other",
    })).rejects.toThrow();
    expect(createCalls(fixture.transport)).toBe(0);
  } finally {
    await fixture.close();
  }
});

test("create-thread without task_id retains ordinary behavior", async () => {
  const fixture = await createFixture();
  try {
    const created = JSON.parse(await execute(fixture.management(), {
      action: "create-thread", name: "ordinary thread",
    })) as { channel_id: string };
    expect(created.channel_id).toBe(threadRecord(GROUP_ID, "thread-1").channelId);
    expect(createCalls(fixture.transport)).toBe(1);
    expect(fixture.store.getThreadListen(BOT_UID, {
      channelId: created.channel_id,
      channelType: 5,
    })).toMatchObject({ listen: "merge" });
    expect(fixture.store.getLoopThreadCreation(fixture.key())).toBeNull();
  } finally {
    await fixture.close();
  }
});
