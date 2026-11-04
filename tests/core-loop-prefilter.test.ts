import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BotIdentity, ChatTarget } from "../src/contracts.ts";
import { LoopController, type LoopControllerOptions } from "../src/loop/controller.ts";
import type { LoopTask, LoopWorkspace } from "../src/loop/types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const BOT_UID = "loop-prefilter-bot";
const OWNER_UID = "persisted-loop-owner";
const IDENTITY_OWNER_UID = "registration-owner-must-not-authorize";
const GROUP_ID = "group-allowed";
const WORKSPACE: LoopWorkspace = {
  workspaceId: "loop-prefilter-workspace",
  name: "Prefilter tasks",
  slug: "prefilter-tasks",
};
const MAIN_TARGET: ChatTarget = { channelType: 2, channelId: GROUP_ID };
const BOT: BotIdentity = { uid: BOT_UID, name: "Prefilter Bot", ownerUid: IDENTITY_OWNER_UID };

function task(taskId: string, title: string, overrides: Partial<LoopTask> = {}): LoopTask {
  return {
    taskId,
    title,
    description: `description for ${taskId}`,
    status: "open",
    priority: "normal",
    assigneeType: "member",
    assigneeId: `fleet-user-${taskId}`,
    assigneeOctoUid: OWNER_UID,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-04T00:00:00Z",
    updatedAt: "2026-11-04T00:00:00Z",
    ...overrides,
  };
}

function loopFixture(tasks: LoopTask[] = []): FakeLoopReadPort {
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(WORKSPACE.workspaceId, WORKSPACE);
  loop.tasks.set(WORKSPACE.workspaceId, tasks);
  return loop;
}

function groupMessage(messageId: string, text: string) {
  return inbound({
    channelType: 2,
    channelId: GROUP_ID,
    messageId,
    senderId: OWNER_UID,
    text,
    mentionedUserIds: [BOT_UID],
  });
}

function bindMessage(messageId: string) {
  return groupMessage(messageId, `/bind loop ${WORKSPACE.workspaceId}`);
}

async function startHarness(loop: FakeLoopReadPort, root?: string, prompt?: FakePrompt) {
  return createCoreHarness({
    ...(root === undefined ? {} : { root }),
    bot: BOT,
    ownerId: OWNER_UID,
    allowedGroupIds: [GROUP_ID],
    loop,
    ...(prompt === undefined ? {} : { prompt }),
  });
}

interface PersistedLoopInput {
  id: string;
  group_id: string;
  workspace_id: string;
  kind: string;
  task_id: string | null;
  tasks_json: string;
  state: string;
  target_channel_type: number | null;
  target_channel_id: string | null;
}

function persistedInputs(stateDir: string): PersistedLoopInput[] {
  const database = new Database(join(stateDir, "channel.sqlite"));
  try {
    return database.query(
      `SELECT id, group_id, workspace_id, kind, task_id, tasks_json, state,
              target_channel_type, target_channel_id
       FROM loop_inputs WHERE bot_uid = ? ORDER BY sequence ASC`,
    ).all(BOT_UID) as PersistedLoopInput[];
  } finally {
    database.close();
  }
}


async function inTemporaryDirectory(run: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-loop-prefilter-"));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

interface DeliveryStats {
  calls: number;
  acceptAttempts: number;
}

function durableStoreDelivery(
  store: ChannelStore,
  cwd: string,
  stats: DeliveryStats,
): LoopControllerOptions["deliver"] {
  return async (input, _binding, target, text, guard, _beforeWorkerSteer, beforeTaskAccept) => {
    stats.calls += 1;
    if (!guard() || !beforeTaskAccept()) return "not-accepted";
    stats.acceptAttempts += 1;
    const messageId = `loop:${input.id}`;
    const claim: InboundClaim = {
      botUid: BOT_UID,
      target,
      messageId,
      senderId: OWNER_UID,
      text,
      supported: true,
      kind: "task",
    };
    const taskRecord: TaskRecord = {
      id: `accepted-${input.id}`,
      botUid: BOT_UID,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      senderId: OWNER_UID,
      text,
      sessionId: `session-${input.id}`,
      cwd,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relaySuppressed: false,
    };
    return store.acceptTask(claim, taskRecord) ? "accepted" : "not-accepted";
  };
}

test("CoreService baseline retains every task but accepts only persisted-owner and Bot member assignments", async () => {
  const tasks = [
    task("persisted-owner-task", "Persisted owner task", {
      assigneeId: IDENTITY_OWNER_UID,
      assigneeOctoUid: OWNER_UID,
    }),
    task("bot-task", "Bot task", {
      assigneeId: BOT_UID,
      assigneeOctoUid: BOT_UID,
    }),
    task("registration-owner-only", "Registration owner only", {
      assigneeOctoUid: IDENTITY_OWNER_UID,
    }),
    task("fleet-id-collision", "Fleet UUID is not a Bot UID", {
      assigneeId: OWNER_UID,
      assigneeOctoUid: "different-octo-user",
    }),
    task("missing-octo-uid", "No resolved Octo UID", {
      assigneeId: BOT_UID,
      assigneeOctoUid: null,
    }),
    task("agent-collision", "Agent ID collision", {
      assigneeType: "agent",
      assigneeId: OWNER_UID,
      assigneeOctoUid: OWNER_UID,
    }),
    task("squad-collision", "Squad ID collision", {
      assigneeType: "squad",
      assigneeId: BOT_UID,
      assigneeOctoUid: BOT_UID,
    }),
  ];
  const loop = loopFixture(tasks);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.handleInbound(bindMessage("baseline-filter"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const inputs = persistedInputs(harness.stateDir);
    expect(inputs).toHaveLength(1);
    const input = store.getLoopInput(inputs[0]!.id)!;
    expect(input).toMatchObject({ kind: "baseline", state: "delivered", target: MAIN_TARGET });
    expect(input.tasks.map((current) => current.taskId)).toEqual([
      "persisted-owner-task",
      "bot-task",
    ]);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]?.text).toContain("Persisted owner task");
    expect(harness.workers.prompts[0]?.text).toContain("Bot task");
    for (const excluded of tasks.slice(2)) {
      expect(harness.workers.prompts[0]?.text).not.toContain(excluded.title);
    }
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${input.id}`)).not.toBeNull();

    const database = new Database(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(database.query(
        `SELECT COUNT(*) AS count FROM loop_task_snapshots
         WHERE bot_uid = ? AND group_id = ? AND workspace_id = ?`,
      ).get(BOT_UID, GROUP_ID, WORKSPACE.workspaceId)).toEqual({ count: tasks.length });
    } finally {
      database.close();
    }
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("unobserved creations stay silent and later owner or Bot assignment delivers their persisted changes", async () => {
  const firstOther = task("existing-other", "Existing other task", {
    assigneeId: "fleet-other-user",
    assigneeOctoUid: "other-user",
  });
  const loop = loopFixture([firstOther]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.handleInbound(bindMessage("unobserved-baseline"));
    expect(harness.workers.prompts).toEqual([]);
    expect(persistedInputs(harness.stateDir)).toEqual([]);
    expect(store.getLatestTask(BOT_UID, MAIN_TARGET)).toBeNull();

    const newOther = task("new-other", "New other task", {
      assigneeId: "fleet-another-user",
      assigneeOctoUid: "another-user",
    });
    loop.tasks.set(WORKSPACE.workspaceId, [firstOther, newOther]);
    await harness.service.handleInbound(bindMessage("unobserved-created"));
    expect(harness.workers.prompts).toEqual([]);
    expect(persistedInputs(harness.stateDir)).toEqual([]);
    expect(store.getLatestTask(BOT_UID, MAIN_TARGET)).toBeNull();

    const assignedOwner = task("existing-other", "Existing task assigned to owner", {
      assigneeId: "fleet-resolved-owner-uuid",
      assigneeOctoUid: OWNER_UID,
    });
    loop.tasks.set(WORKSPACE.workspaceId, [assignedOwner, newOther]);
    await harness.service.handleInbound(bindMessage("owner-assignment"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const assignedBot = task("new-other", "New task assigned to Bot", {
      assigneeId: "fleet-resolved-bot-uuid",
      assigneeOctoUid: BOT_UID,
    });
    loop.tasks.set(WORKSPACE.workspaceId, [assignedOwner, assignedBot]);
    await harness.service.handleInbound(bindMessage("bot-assignment"));
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const inputs = persistedInputs(harness.stateDir);
    expect(inputs.map(({ kind, task_id }) => [kind, task_id])).toEqual([
      ["updated", "existing-other"],
      ["updated", "new-other"],
    ]);
    expect(JSON.parse(inputs[0]!.tasks_json)).toMatchObject([assignedOwner]);
    expect(JSON.parse(inputs[1]!.tasks_json)).toMatchObject([assignedBot]);
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${inputs[0]!.id}`)).not.toBeNull();
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${inputs[1]!.id}`)).not.toBeNull();
    expect(harness.transport.sent.filter(({ text }) => text.includes("Loop 关联子区暂不可用"))).toEqual([]);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("an unassociated task transferred away from the owner stops producing inputs after its snapshot advances", async () => {
  const owned = task("transfer-out", "Initially owned task");
  const loop = loopFixture([owned]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.handleInbound(bindMessage("transfer-bind"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const transferred = task("transfer-out", "Transferred to another member", {
      assigneeId: "fleet-other-user",
      assigneeOctoUid: "other-user",
    });
    loop.tasks.set(WORKSPACE.workspaceId, [transferred]);
    await harness.service.handleInbound(bindMessage("transfer-away"));
    expect(harness.workers.prompts).toHaveLength(1);
    expect(persistedInputs(harness.stateDir)).toHaveLength(1);

    await harness.service.handleInbound(bindMessage("transfer-away-repeat"));
    expect(harness.workers.prompts).toHaveLength(1);
    expect(persistedInputs(harness.stateDir)).toHaveLength(1);
    const database = new Database(join(harness.stateDir, "channel.sqlite"));
    try {
      const snapshot = database.query(
        "SELECT task_json FROM loop_task_snapshots WHERE bot_uid = ? AND group_id = ? AND task_id = ?",
      ).get(BOT_UID, GROUP_ID, "transfer-out") as { task_json: string };
      expect(JSON.parse(snapshot.task_json)).toMatchObject(transferred);
    } finally {
      database.close();
    }
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${persistedInputs(harness.stateDir)[0]!.id}`)).not.toBeNull();
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a task route keeps changes direct to its child after reassignment or unassignment", async () => {
  const owned = task("routed-away", "Initially owned and unassociated");
  const loop = loopFixture([owned]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.handleInbound(bindMessage("routed-bind"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );
    const thread = await harness.transport.createThread(GROUP_ID, "Routed task");
    store.bindLoopTask({
      botUid: BOT_UID,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: owned.taskId,
    }, thread.channelId);

    const noAssignee = task("routed-away", "No assignee, existing route", {
      assigneeType: null,
      assigneeId: null,
      assigneeOctoUid: null,
    });
    loop.tasks.set(WORKSPACE.workspaceId, [noAssignee]);
    await harness.service.handleInbound(bindMessage("routed-unassigned-update"));
    await harness.workers.waitForPromptCount(2);
    const childTarget: ChatTarget = { channelType: 5, channelId: thread.channelId };
    await waitForStatus(
      harness.service,
      childTarget,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const inputs = persistedInputs(harness.stateDir);
    expect(inputs).toHaveLength(2);
    const routedInput = store.getLoopInput(inputs[1]!.id)!;
    expect(routedInput).toMatchObject({
      kind: "updated",
      tasks: [noAssignee],
      state: "delivered",
      target: childTarget,
    });
    expect(store.getTaskByMessageId(BOT_UID, childTarget, `loop:${routedInput.id}`)).not.toBeNull();
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${routedInput.id}`)).toBeNull();
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("CoreService sends a historical-route baseline task to its child, not the main group batch", async () => {
  const routedTask = task("baseline-existing-route", "Historically routed task", {
    assigneeId: "fleet-other-user",
    assigneeOctoUid: "other-user",
  });
  const mainTask = task("baseline-main-task", "Owner task for the main group");
  const loop = loopFixture([routedTask, mainTask]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    const thread = await harness.transport.createThread(GROUP_ID, "Existing task route");
    store.bindLoopWorkspace(BOT_UID, GROUP_ID, WORKSPACE, []);
    store.bindLoopTask({
      botUid: BOT_UID,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: routedTask.taskId,
    }, thread.channelId);
    store.bindLoopWorkspace(BOT_UID, GROUP_ID, {
      workspaceId: "temporary-workspace",
      name: "Temporary",
      slug: "temporary",
    }, []);
    store.bindLoopWorkspace(BOT_UID, GROUP_ID, WORKSPACE, [routedTask, mainTask]);

    await harness.service.handleInbound(bindMessage("existing-route-baseline-drain"));
    await harness.workers.waitForPromptCount(2);
    const childTarget: ChatTarget = { channelType: 5, channelId: thread.channelId };
    await Promise.all([
      waitForStatus(
        harness.service,
        MAIN_TARGET,
        (status) => status.state === "completed" && status.queued === 0,
      ),
      waitForStatus(
        harness.service,
        childTarget,
        (status) => status.state === "completed" && status.queued === 0,
      ),
    ]);

    const inputs = persistedInputs(harness.stateDir);
    expect(inputs).toHaveLength(2);
    const routedRow = inputs.find((input) => input.task_id === routedTask.taskId)!;
    const mainRow = inputs.find((input) => input.task_id === null)!;
    expect(store.getLoopInput(routedRow.id)).toMatchObject({
      kind: "baseline",
      taskId: routedTask.taskId,
      tasks: [routedTask],
      state: "delivered",
      target: childTarget,
    });
    expect(store.getLoopInput(mainRow.id)).toMatchObject({
      kind: "baseline",
      taskId: null,
      tasks: [mainTask],
      state: "delivered",
      target: MAIN_TARGET,
    });
    expect(store.getTaskByMessageId(BOT_UID, childTarget, `loop:${routedRow.id}`)).not.toBeNull();
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${routedRow.id}`)).toBeNull();
    expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${mainRow.id}`)).not.toBeNull();
    expect(store.getTaskByMessageId(BOT_UID, childTarget, `loop:${mainRow.id}`)).toBeNull();
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("unobserved Loop tasks cannot answer or queue behind a pending ask", async () => {
  const askStarted = Promise.withResolvers<void>();
  const owned = task("ask-owner", "Owned baseline");
  const loop = loopFixture([owned]);
  let promptCount = 0;
  const harness = await startHarness(loop, undefined, async (context) => {
    promptCount += 1;
    if (promptCount !== 1) return;
    const ask = context.promptContext?.tools?.find((tool) => tool.name === "ask");
    if (!ask) throw new Error("ask tool unavailable in fixture prompt");
    askStarted.resolve();
    await ask.execute({ questions: [{ id: "prefilter-pending-question", question: "Which option?" }] }, new AbortController().signal);
  });
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.handleInbound(bindMessage("ask-owner-bind"));
    await harness.workers.waitForPromptCount(1);
    await askStarted.promise;
    const waiting = await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.pendingQuestionIds.length === 1,
    );

    const unrelated = task("ask-unrelated-other", "Not an answer", {
      assigneeId: "fleet-other-user",
      assigneeOctoUid: "other-user",
    });
    loop.tasks.set(WORKSPACE.workspaceId, [owned, unrelated]);
    await harness.service.handleInbound(bindMessage("ask-unrelated-scan"));
    const after = await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.pendingQuestionIds.length === 1 && status.queued === 0,
    );

    expect(after.pendingQuestionIds).toEqual(waiting.pendingQuestionIds);
    expect(after.queued).toBe(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(persistedInputs(harness.stateDir)).toHaveLength(1);
    expect(store.listPendingQuestions(BOT_UID, MAIN_TARGET)).toHaveLength(1);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a recovered mixed pending baseline is cropped to its currently observed tasks", async () => {
  const ownerTask = task("recovered-owner", "Pending owner task");
  const otherTask = task("recovered-other", "Historical other task", {
    assigneeId: "fleet-other-user",
    assigneeOctoUid: "other-user",
  });
  const loop = loopFixture([ownerTask, otherTask]);
  const first = await startHarness(loop);
  const root = first.root;
  let inputId = "";
  let seedStore: ChannelStore | undefined;
  try {
    seedStore = await openChannelStore(first.stateDir);
    seedStore.bindLoopWorkspace(BOT_UID, GROUP_ID, WORKSPACE, [ownerTask, otherTask]);
    const mixedInput = seedStore.listLoopInputs(BOT_UID)[0]!;
    inputId = mixedInput.id;
    const database = new Database(join(first.stateDir, "channel.sqlite"));
    try {
      database.query("UPDATE loop_inputs SET tasks_json = ? WHERE id = ?")
        .run(JSON.stringify([ownerTask, otherTask]), mixedInput.id);
    } finally {
      database.close();
    }
  } finally {
    seedStore?.close();
    await closeCoreHarness(first, { remove: false });
  }

  const recovered = await startHarness(loop, root);
  let recoveredStore: ChannelStore | undefined;
  try {
    recoveredStore = await openChannelStore(recovered.stateDir);
    await recovered.workers.waitForPromptCount(1);
    const prepared = recoveredStore.getLoopInput(inputId)!;
    expect(prepared).toMatchObject({ state: "delivered", tasks: [ownerTask] });
    expect(recovered.workers.prompts[0]?.text).toContain("Pending owner task");
    expect(recovered.workers.prompts[0]?.text).not.toContain("Historical other task");
  } finally {
    recoveredStore?.close();
    await closeCoreHarness(recovered);
  }
});

test("a recovered pending baseline with no currently observed tasks is superseded without an agent", async () => {
  const ownerTask = task("recovered-all-drop", "Previously owned task");
  const loop = loopFixture([ownerTask]);
  const first = await startHarness(loop);
  const root = first.root;
  let inputId = "";
  let seedStore: ChannelStore | undefined;
  try {
    seedStore = await openChannelStore(first.stateDir);
    const binding = seedStore.bindLoopWorkspace(BOT_UID, GROUP_ID, WORKSPACE, [ownerTask]).binding;
    inputId = seedStore.listLoopInputs(BOT_UID)[0]!.id;
    const transferred = task("recovered-all-drop", "No longer observed", {
      assigneeId: "fleet-other-user",
      assigneeOctoUid: "other-user",
    });
    expect(seedStore.commitLoopScan(binding, [transferred])).toBe(true);
    loop.tasks.set(WORKSPACE.workspaceId, [transferred]);
  } finally {
    seedStore?.close();
    await closeCoreHarness(first, { remove: false });
  }

  const recovered = await startHarness(loop, root);
  let recoveredStore: ChannelStore | undefined;
  try {
    recoveredStore = await openChannelStore(recovered.stateDir);
    await recovered.service.handleInbound(bindMessage("recovered-all-drop-drain"));
    expect(recovered.workers.prompts).toEqual([]);
    expect(recoveredStore.getLoopInput(inputId)).toMatchObject({
      state: "superseded",
      tasks: [ownerTask],
    });
  } finally {
    recoveredStore?.close();
    await closeCoreHarness(recovered);
  }
});


test("an assignment change during asynchronous target validation blocks acceptance until pending tasks are re-trimmed", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    const ownerTask = task("validation-race-owner", "Owner during validation");
    const secondOwnerTask = task("validation-race-retained", "Still observed after validation");
    const loop = loopFixture([ownerTask, secondOwnerTask]);
    const validationStarted = Promise.withResolvers<void>();
    const releaseValidation = Promise.withResolvers<void>();
    let validationCount = 0;
    const deliveryStats: DeliveryStats = { calls: 0, acceptAttempts: 0 };
    const controller = new LoopController({
      store,
      read: loop,
      botUid: BOT_UID,
      validateTarget: async () => {
        validationCount += 1;
        if (validationCount === 1) {
          validationStarted.resolve();
          await releaseValidation.promise;
        }
      },
      deliver: durableStoreDelivery(store, root, deliveryStats),
      isStopping: () => false,
      notifyFault: async () => {},
    });
    store.setOwnerId(BOT_UID, OWNER_UID);
    try {
      const bindOperation = controller.bind(GROUP_ID, WORKSPACE.workspaceId);
      await validationStarted.promise;
      const binding = store.getLoopBinding(BOT_UID, GROUP_ID)!;
      const transferred = task("validation-race-owner", "Transferred during validation", {
        assigneeId: "fleet-other-user",
        assigneeOctoUid: "other-user",
      });
      loop.tasks.set(WORKSPACE.workspaceId, [transferred, secondOwnerTask]);
      expect(store.commitLoopScan(binding, [transferred, secondOwnerTask])).toBe(true);
      releaseValidation.resolve();
      await bindOperation;
      await new Promise<void>((resolve) => setImmediate(resolve));

      const pending = store.listLoopInputs(BOT_UID)[0]!;
      expect(pending.tasks).toEqual([ownerTask, secondOwnerTask]);
      expect(deliveryStats.calls).toBe(0);
      expect(deliveryStats.acceptAttempts).toBe(0);
      expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${pending.id}`)).toBeNull();

      await controller.bind(GROUP_ID, WORKSPACE.workspaceId);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(store.getLoopInput(pending.id)).toMatchObject({
        state: "delivered",
        tasks: [secondOwnerTask],
      });
      expect(deliveryStats.calls).toBe(1);
      expect(deliveryStats.acceptAttempts).toBe(1);
      expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${pending.id}`)).not.toBeNull();
    } finally {
      releaseValidation.resolve();
      await controller.stop();
      store.close();
    }
  });
});

test("a route added during main-baseline validation blocks acceptance and is durably split to its child", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    const routedTask = task("route-during-validation", "Will route during validation");
    const mainTask = task("remains-in-main", "Still belongs in main group");
    const loop = loopFixture([routedTask, mainTask]);
    const validationStarted = Promise.withResolvers<void>();
    const releaseValidation = Promise.withResolvers<void>();
    let validationCount = 0;
    const deliveryStats: DeliveryStats = { calls: 0, acceptAttempts: 0 };
    const controller = new LoopController({
      store,
      read: loop,
      botUid: BOT_UID,
      validateTarget: async () => {
        validationCount += 1;
        if (validationCount === 1) {
          validationStarted.resolve();
          await releaseValidation.promise;
        }
      },
      deliver: durableStoreDelivery(store, root, deliveryStats),
      isStopping: () => false,
      notifyFault: async () => {},
    });
    store.setOwnerId(BOT_UID, OWNER_UID);
    try {
      const bindOperation = controller.bind(GROUP_ID, WORKSPACE.workspaceId);
      await validationStarted.promise;
      const source = store.listLoopInputs(BOT_UID)[0]!;
      const childChannelId = `${GROUP_ID}____new-route`;
      store.bindLoopTask({
        botUid: BOT_UID,
        groupId: GROUP_ID,
        workspaceId: WORKSPACE.workspaceId,
        taskId: routedTask.taskId,
      }, childChannelId);
      releaseValidation.resolve();
      await bindOperation;
      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(deliveryStats.calls).toBe(0);
      expect(deliveryStats.acceptAttempts).toBe(0);
      expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${source.id}`)).toBeNull();
      expect(store.getLoopInput(source.id)).toMatchObject({
        state: "pending",
        taskId: null,
        tasks: [routedTask, mainTask],
      });

      await controller.bind(GROUP_ID, WORKSPACE.workspaceId);
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(store.getLoopInput(source.id)).toMatchObject({
        state: "delivered",
        taskId: null,
        tasks: [mainTask],
        target: MAIN_TARGET,
      });
      expect(deliveryStats.calls).toBe(1);
      expect(deliveryStats.acceptAttempts).toBe(1);
      expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${source.id}`)).not.toBeNull();

      const routeInputs = store.listLoopInputs(BOT_UID).filter((input) => input.taskId === routedTask.taskId);
      expect(routeInputs).toHaveLength(1);
      const routedInput = routeInputs[0]!;
      expect(routedInput).toMatchObject({
        kind: "baseline",
        tasks: [routedTask],
        state: "pending",
      });
      await controller.bind(GROUP_ID, WORKSPACE.workspaceId);
      await new Promise<void>((resolve) => setImmediate(resolve));
      const childTarget: ChatTarget = { channelType: 5, channelId: childChannelId };
      expect(store.getLoopInput(routedInput.id)).toMatchObject({
        state: "delivered",
        taskId: routedTask.taskId,
        tasks: [routedTask],
        target: childTarget,
      });
      expect(deliveryStats.calls).toBe(2);
      expect(deliveryStats.acceptAttempts).toBe(2);
      expect(store.getTaskByMessageId(BOT_UID, childTarget, `loop:${routedInput.id}`)).not.toBeNull();
      expect(store.getTaskByMessageId(BOT_UID, MAIN_TARGET, `loop:${routedInput.id}`)).toBeNull();
    } finally {
      releaseValidation.resolve();
      await controller.stop();
      store.close();
    }
  });
});
