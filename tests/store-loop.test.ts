import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { Database } from "bun:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { LoopTask, LoopTaskKey, LoopWorkspace } from "../src/loop/types.ts";

const ownerUid = "loop-store-owner";
const botUid = "loop-store-bot";
const groupId = "loop-store-group";
const workspaceA: LoopWorkspace = { workspaceId: "workspace-a", name: "Workspace A", slug: "workspace-a" };
const workspaceB: LoopWorkspace = { workspaceId: "workspace-b", name: "Workspace B", slug: "workspace-b" };
const mainTarget: ChatTarget = { channelType: 2, channelId: groupId };

type Store = ChannelStore;

function loopTask(overrides: Partial<LoopTask> = {}): LoopTask {
  return {
    taskId: "loop-task-1",
    title: "Prepare release",
    description: "Collect the release notes",
    status: "open",
    priority: "normal",
    assigneeType: "member",
    assigneeId: "fleet-user-uuid",
    assigneeOctoUid: ownerUid,
    projectId: "project-1",
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-01T10:00:00.000Z",
    updatedAt: "2026-11-01T10:00:00.000Z",
    ...overrides,
  };
}

function taskKey(overrides: Partial<LoopTaskKey> = {}): LoopTaskKey {
  return {
    botUid,
    groupId,
    workspaceId: workspaceA.workspaceId,
    taskId: "loop-task-1",
    ...overrides,
  };
}

async function inTemporaryDirectory(run: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-store-loop-"));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function openStore(root: string): Promise<Store> {
  const store = await openChannelStore(root);
  store.setOwnerId(botUid, ownerUid);
  return store;
}
test("loop bindings are bot/group isolated, same-workspace idempotent, and replacements supersede only old pending inputs", async () => {
  await inTemporaryDirectory(async (root) => {
    let store = await openStore(root);
    try {
      const first = store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
      expect(first).toMatchObject({ changed: true, binding: { botUid, groupId, workspaceId: "workspace-a", version: 1 } });
      const originalBaseline = store.listLoopInputs(botUid)[0]!;
      const same = store.bindLoopWorkspace(botUid, groupId, { ...workspaceA, name: "renamed" }, [loopTask({ title: "ignored" })]);
      expect(same.changed).toBe(false);
      expect(same.binding.version).toBe(1);
      expect(store.listLoopInputs(botUid)).toHaveLength(1);

      const otherGroup = store.bindLoopWorkspace(botUid, "another-group", workspaceA, [loopTask()]);
      const otherBot = store.bindLoopWorkspace("another-bot", groupId, workspaceA, []);
      expect(otherGroup.binding.groupId).toBe("another-group");
      expect(otherBot.binding.botUid).toBe("another-bot");
      expect(store.listLoopBindings(botUid).map((binding) => binding.groupId).sort()).toEqual(["another-group", groupId]);
      expect(store.listLoopBindings("another-bot")).toHaveLength(1);

      const key = taskKey();
      const route = store.bindLoopTask(key, `${groupId}____thread-a`);
      expect(route.threadChannelId).toBe(`${groupId}____thread-a`);

      const replacement = store.bindLoopWorkspace(botUid, groupId, workspaceB, [loopTask()]);
      expect(replacement).toMatchObject({ changed: true, binding: { workspaceId: "workspace-b", version: 2 } });
      expect(store.getLoopInput(originalBaseline.id)?.state).toBe("superseded");
      expect(store.getLoopTaskRoute(key)).toEqual(route);
      expect(store.getLoopTaskRoute(taskKey({ workspaceId: workspaceB.workspaceId }))).toBeNull();
      expect(store.listLoopInputs(botUid).map(({ groupId: inputGroupId, workspaceId }) => [inputGroupId, workspaceId])).toEqual([
        ["another-group", workspaceA.workspaceId],
        [groupId, workspaceB.workspaceId],
      ]);
    } finally {
      store.close();
    }

    store = await openStore(root);
    try {
      expect(store.getLoopBinding(botUid, groupId)).toMatchObject({ workspaceId: "workspace-b", version: 2 });
      expect(store.getLoopTaskRoute(taskKey())?.threadChannelId).toBe(`${groupId}____thread-a`);
    } finally {
      store.close();
    }
  });
});

test("loop bind writes complete bounded observed baselines and omits empty baselines", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const tasks = Array.from({ length: 41 }, (_, index) => loopTask({ taskId: `task-${index}` }));
      store.bindLoopWorkspace(botUid, groupId, workspaceA, tasks);
      const baseline = store.listLoopInputs(botUid).filter((input) => input.kind === "baseline");
      expect(baseline.map((input) => input.tasks.length)).toEqual([20, 20, 1]);
      expect(baseline.flatMap((input) => input.tasks.map((task) => task.taskId))).toEqual(tasks.map((task) => task.taskId));

      store.bindLoopWorkspace(botUid, "empty-group", workspaceA, []);
      expect(store.listLoopInputs(botUid).filter((input) => input.groupId === "empty-group")).toEqual([]);
    } finally {
      store.close();
    }
  });
});

test("loop snapshots ignore timestamp-only updates, retain missing tasks, and record observed A-to-B-to-A business changes", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const original = loopTask();
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [original]).binding;
      const baseline = store.listLoopInputs(botUid)[0]!;
      const timestampOnly = loopTask({ createdAt: "2026-11-01T11:00:00.000Z", updatedAt: "2026-11-02T10:00:00.000Z" });
      expect(store.commitLoopScan(binding, [timestampOnly])).toBe(true);
      expect(store.listLoopInputs(botUid)).toHaveLength(1);

      const changed = loopTask({ title: "Ship release", updatedAt: "2026-11-02T11:00:00.000Z" });
      const created = loopTask({ taskId: "new-loop-task", title: "New task" });
      expect(store.commitLoopScan(binding, [changed, created])).toBe(true);
      const changes = store.listLoopInputs(botUid).filter((input) => input.id !== baseline.id);
      const toB = changes.find((input) => input.kind === "updated")!;
      const createdInput = changes.find((input) => input.kind === "created")!;
      expect(toB).toMatchObject({ kind: "updated", taskId: original.taskId, previous: timestampOnly, tasks: [changed] });
      expect(createdInput).toMatchObject({ kind: "created", taskId: created.taskId, previous: null, tasks: [created] });

      expect(store.commitLoopScan(binding, [original])).toBe(true);
      const inputsAfterReturn = store.listLoopInputs(botUid);
      expect(inputsAfterReturn).toHaveLength(4);
      expect(inputsAfterReturn[3]).toMatchObject({ kind: "updated", previous: changed, tasks: [original] });
      expect(inputsAfterReturn[1]!.id).not.toBe(inputsAfterReturn[3]!.id);

      const resolvedUid = loopTask({
        assigneeId: original.assigneeId,
        assigneeOctoUid: botUid,
      });
      expect(store.commitLoopScan(binding, [resolvedUid])).toBe(true);
      expect(store.listLoopInputs(botUid)).toHaveLength(5);
      expect(store.listLoopInputs(botUid)[4]).toMatchObject({
        kind: "updated",
        previous: original,
        tasks: [resolvedUid],
      });

      expect(store.commitLoopScan(binding, [])).toBe(true);
      expect(store.commitLoopScan(binding, [resolvedUid])).toBe(true);
      expect(store.listLoopInputs(botUid)).toHaveLength(5);

    } finally {
      store.close();
    }
  });
});

test("loop snapshots retain every task while baseline and change inputs include only owner-or-bot members", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const ownerTask = loopTask({ taskId: "owner-task" });
      const botTask = loopTask({
        taskId: "bot-task",
        assigneeId: "fleet-bot-user-uuid",
        assigneeOctoUid: botUid,
      });
      const otherMember = loopTask({
        taskId: "other-member",
        assigneeId: ownerUid,
        assigneeOctoUid: "someone-else",
      });
      const noUidMember = loopTask({
        taskId: "no-uid-member",
        assigneeId: botUid,
        assigneeOctoUid: null,
      });
      const agentCollision = loopTask({
        taskId: "agent-collision",
        assigneeType: "agent",
        assigneeId: ownerUid,
        assigneeOctoUid: ownerUid,
      });
      const squadCollision = loopTask({
        taskId: "squad-collision",
        assigneeType: "squad",
        assigneeId: botUid,
        assigneeOctoUid: botUid,
      });
      const initiallyUnassigned = loopTask({
        taskId: "initially-unassigned",
        assigneeId: null,
        assigneeOctoUid: null,
      });
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [
        ownerTask,
        botTask,
        otherMember,
        noUidMember,
        agentCollision,
        squadCollision,
        initiallyUnassigned,
      ]).binding;
      const baseline = store.listLoopInputs(botUid).find((input) => input.kind === "baseline")!;
      expect(baseline.tasks.map((task) => task.taskId)).toEqual(["owner-task", "bot-task"]);

      const changedUnobserved = loopTask({
        ...initiallyUnassigned,
        title: "latest ignored title",
      });
      const newUnobserved = loopTask({
        taskId: "new-unobserved",
        assigneeId: null,
        assigneeOctoUid: null,
      });
      expect(store.commitLoopScan(binding, [
        ownerTask,
        botTask,
        otherMember,
        noUidMember,
        agentCollision,
        squadCollision,
        changedUnobserved,
        newUnobserved,
      ])).toBe(true);
      expect(store.listLoopInputs(botUid)).toHaveLength(1);

      const assignedOwner = loopTask({
        ...changedUnobserved,
        assigneeId: "fleet-resolved-owner-uuid",
        assigneeOctoUid: ownerUid,
      });
      const assignedBot = loopTask({
        ...newUnobserved,
        assigneeId: "fleet-resolved-bot-uuid",
        assigneeOctoUid: botUid,
      });
      expect(store.commitLoopScan(binding, [
        ownerTask,
        botTask,
        otherMember,
        noUidMember,
        agentCollision,
        squadCollision,
        assignedOwner,
        assignedBot,
      ])).toBe(true);
      const changes = store.listLoopInputs(botUid).filter((input) => input.kind !== "baseline");
      expect(changes).toHaveLength(2);
      expect(changes[0]).toMatchObject({
        kind: "updated",
        taskId: "initially-unassigned",
        previous: changedUnobserved,
        tasks: [assignedOwner],
      });
      expect(changes[1]).toMatchObject({
        kind: "updated",
        taskId: "new-unobserved",
        previous: newUnobserved,
        tasks: [assignedBot],
      });
    } finally {
      store.close();
    }
  });
});

test("a current task route observes tasks assigned to someone else and to nobody", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const unowned = loopTask({
        taskId: "routed-task",
        assigneeId: "fleet-other-uuid",
        assigneeOctoUid: "other-user",
      });
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [unowned]).binding;
      expect(store.listLoopInputs(botUid)).toEqual([]);
      store.bindLoopTask(taskKey({ taskId: unowned.taskId }), `${groupId}____routed`);

      const otherMember = loopTask({
        ...unowned,
        title: "Routed despite another member",
      });
      expect(store.commitLoopScan(binding, [otherMember])).toBe(true);
      expect(store.listLoopInputs(botUid)).toMatchObject([
        { kind: "updated", taskId: "routed-task", tasks: [otherMember] },
      ]);

      const nobody = loopTask({
        ...otherMember,
        title: "Routed despite no assignee",
        assigneeType: null,
        assigneeId: null,
        assigneeOctoUid: null,
      });
      expect(store.commitLoopScan(binding, [nobody])).toBe(true);
      expect(store.listLoopInputs(botUid)).toMatchObject([
        { kind: "updated", taskId: "routed-task", tasks: [otherMember] },
        { kind: "updated", taskId: "routed-task", tasks: [nobody] },
      ]);
    } finally {
      store.close();
    }
  });
});

test("task routes do not make same-ID tasks observed across bot, group, or workspace scope", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const unmatched = loopTask({
        taskId: "same-id",
        assigneeId: "fleet-other-uuid",
        assigneeOctoUid: "someone-else",
      });
      const current = store.bindLoopWorkspace(botUid, groupId, workspaceA, [unmatched]).binding;
      const otherBot = "other-bot";
      store.bindLoopWorkspace(otherBot, groupId, workspaceA, []);
      store.bindLoopTask({ botUid: otherBot, groupId, workspaceId: workspaceA.workspaceId, taskId: unmatched.taskId }, "other-bot-thread");
      store.bindLoopWorkspace(botUid, "other-group", workspaceA, []);
      store.bindLoopTask({ botUid, groupId: "other-group", workspaceId: workspaceA.workspaceId, taskId: unmatched.taskId }, "other-group-thread");

      expect(store.commitLoopScan(current, [loopTask({ ...unmatched, title: "still not observed" })])).toBe(true);
      expect(store.listLoopInputs(botUid)).toEqual([]);
      store.bindLoopTask(taskKey({ taskId: unmatched.taskId }), "workspace-a-thread");

      const workspaceBBinding = store.bindLoopWorkspace(botUid, groupId, workspaceB, []).binding;
      expect(store.commitLoopScan(workspaceBBinding, [unmatched])).toBe(true);
      expect(store.listLoopInputs(botUid)).toEqual([]);
    } finally {
      store.close();
    }
  });
});

test("a newly created baseline sends existing routed tasks in task-specific inputs", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const routed = loopTask({
        taskId: "historically-routed",
        assigneeId: "fleet-other-uuid",
        assigneeOctoUid: "other-user",
      });
      const ownerTask = loopTask({ taskId: "main-group-task" });
      store.bindLoopWorkspace(botUid, groupId, workspaceA, []);
      store.bindLoopTask(taskKey({ taskId: routed.taskId }), `${groupId}____existing-route`);
      store.bindLoopWorkspace(botUid, groupId, workspaceB, []);
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [routed, ownerTask]);

      const inputs = store.listLoopInputs(botUid);
      expect(inputs).toHaveLength(2);
      expect(inputs.find((input) => input.taskId === routed.taskId)).toMatchObject({
        kind: "baseline",
        taskId: routed.taskId,
        tasks: [routed],
      });
      expect(inputs.find((input) => input.taskId === null)).toMatchObject({
        kind: "baseline",
        taskId: null,
        tasks: [ownerTask],
      });
    } finally {
      store.close();
    }
  });
});

test("pending baseline preparation persists routed-task splits, trims unobserved tasks, and supersedes empty inputs", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const ownerTask = loopTask({ taskId: "owner-task" });
      const otherTask = loopTask({
        taskId: "other-task",
        assigneeId: "fleet-other-uuid",
        assigneeOctoUid: "other-user",
      });
      const routedTask = loopTask({
        taskId: "routed-task",
        assigneeId: "fleet-routed-user",
        assigneeOctoUid: "routed-user",
      });
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [ownerTask, otherTask, routedTask]);
      const mixedBaseline = store.listLoopInputs(botUid)[0]!;
      const database = new Database(join(root, "channel.sqlite"));
      try {
        database.query("UPDATE loop_inputs SET tasks_json = ? WHERE id = ?")
          .run(JSON.stringify([ownerTask, otherTask, routedTask]), mixedBaseline.id);
      } finally {
        database.close();
      }
      store.bindLoopTask(taskKey({ taskId: routedTask.taskId }), `${groupId}____historical`);

      const preparedMixed = store.prepareLoopInput(mixedBaseline.id);
      expect(preparedMixed?.tasks.map((task) => task.taskId)).toEqual(["owner-task"]);
      expect(store.getLoopInput(mixedBaseline.id)).toMatchObject({
        state: "pending",
        tasks: [ownerTask],
      });
      const routedInputs = store.listLoopInputs(botUid).filter((input) => input.taskId === routedTask.taskId);
      expect(routedInputs).toHaveLength(1);
      const routedInput = routedInputs[0]!;
      expect(routedInput).toMatchObject({
        kind: "baseline",
        taskId: routedTask.taskId,
        tasks: [routedTask],
        state: "pending",
      });
      expect(store.prepareLoopInput(routedInput.id)?.id).toBe(routedInput.id);
      expect(store.listLoopInputs(botUid).filter((input) => input.taskId === routedTask.taskId)).toHaveLength(1);

      const allDrop = loopTask({ taskId: "all-drop", assigneeId: "fleet-owner", assigneeOctoUid: ownerUid });
      const allDropBinding = store.bindLoopWorkspace(botUid, "all-drop-group", workspaceA, [allDrop]).binding;
      const allDropInput = store.listLoopInputs(botUid).find((input) => input.groupId === "all-drop-group")!;
      const noLongerObserved = loopTask({
        ...allDrop,
        assigneeId: ownerUid,
        assigneeOctoUid: "someone-else",
      });
      expect(store.commitLoopScan(allDropBinding, [noLongerObserved])).toBe(true);
      expect(store.prepareLoopInput(allDropInput.id)).toBeNull();
      expect(store.getLoopInput(allDropInput.id)?.state).toBe("superseded");
    } finally {
      store.close();
    }
  });
});

test("legacy snapshots without assigneeOctoUid compare null as unchanged", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const task = loopTask({ assigneeOctoUid: null });
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [task]).binding;
      store.bindLoopTask(taskKey({ taskId: task.taskId }), `${groupId}____legacy`);
      const database = new Database(join(root, "channel.sqlite"));
      try {
        const row = database.query(
          "SELECT task_json, business_json FROM loop_task_snapshots WHERE task_id = ?",
        ).get(task.taskId) as { task_json: string; business_json: string };
        const taskJson = JSON.parse(row.task_json) as Record<string, unknown>;
        delete taskJson.assigneeOctoUid;
        const businessJson = JSON.parse(row.business_json) as Record<string, unknown>;
        delete businessJson.assigneeOctoUid;
        database.query(
          "UPDATE loop_task_snapshots SET task_json = ?, business_json = ? WHERE task_id = ?",
        ).run(JSON.stringify(taskJson), JSON.stringify(businessJson), task.taskId);
      } finally {
        database.close();
      }
      expect(store.commitLoopScan(binding, [task])).toBe(true);
      expect(store.listLoopInputs(botUid)).toEqual([]);
    } finally {
      store.close();
    }
  });
});

test("pending preparation preserves intent inputs and accepted or unknown delivery fences", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const current = loopTask();
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [current]).binding;
      const pending = store.listLoopInputs(botUid)[0]!;
      expect(store.claimLoopInputIntent(pending.id, mainTarget)).toBe(true);
      store.commitLoopScan(binding, [loopTask({
        ...current,
        assigneeOctoUid: "someone-else",
      })]);
      expect(store.prepareLoopInput(pending.id)).toBeNull();
      expect(store.getLoopInput(pending.id)).toMatchObject({
        state: "intent",
        tasks: [current],
      });
      store.markLoopInputUnknown(pending.id);
      expect(store.prepareLoopInput(pending.id)).toBeNull();
      expect(store.getLoopInput(pending.id)).toMatchObject({
        state: "unknown",
        tasks: [current],
      });

      const acceptedTask = loopTask({ taskId: "accepted-input" });
      const acceptedBinding = store.bindLoopWorkspace(botUid, "accepted-group", workspaceA, [acceptedTask]).binding;
      const accepted = store.listLoopInputs(botUid).find((input) => input.groupId === "accepted-group")!;
      expect(store.claimLoopInputIntent(accepted.id, { channelType: 2, channelId: "accepted-group" })).toBe(true);
      store.markLoopInputDelivered(accepted.id);
      store.commitLoopScan(acceptedBinding, [loopTask({
        ...acceptedTask,
        assigneeOctoUid: "someone-else",
      })]);
      expect(store.prepareLoopInput(accepted.id)).toBeNull();
      expect(store.getLoopInput(accepted.id)).toMatchObject({
        state: "delivered",
        tasks: [acceptedTask],
      });
    } finally {
      store.close();
    }
  });
});

test("a scan captured under a replaced binding cannot commit snapshots or changes", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      const oldBinding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]).binding;
      const replacement = store.bindLoopWorkspace(botUid, groupId, workspaceB, [loopTask({ title: "Workspace B task" })]);
      expect(store.commitLoopScan(oldBinding, [loopTask({ title: "stale result" })])).toBe(false);
      expect(store.getLoopBinding(botUid, groupId)).toEqual(replacement.binding);
      const inputs = store.listLoopInputs(botUid);
      expect(inputs.map((input) => input.workspaceId)).toEqual([workspaceB.workspaceId]);
      expect(inputs[0]!.tasks[0]!.title).toBe("Workspace B task");
    } finally {
      store.close();
    }
  });
});

test("loop input intent, known-not-sent retry, unknown state, fault notice, and target survive reopen", async () => {
  await inTemporaryDirectory(async (root) => {
    let store = await openStore(root);
    let inputId = "";
    try {
      const binding = store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]).binding;
      const input = store.listLoopInputs(botUid)[0]!;
      inputId = input.id;
      expect(store.claimLoopInputIntent(input.id, mainTarget)).toBe(true);
      expect(store.getLoopInput(input.id)).toMatchObject({ state: "intent", target: mainTarget });
      store.markLoopInputPending(input.id);
      expect(store.claimLoopInputIntent(input.id, { channelType: 2, channelId: "replacement-target" })).toBe(true);
      store.markLoopInputUnknown(input.id);
      expect(store.getLoopInput(input.id)).toMatchObject({ state: "unknown", target: { channelType: 2, channelId: "replacement-target" } });
      expect(store.listLoopInputs(botUid)).toHaveLength(0);
      expect(store.claimLoopInputIntent(input.id, mainTarget)).toBe(false);

      const delivered = loopTask({ title: "Follow-up change" });
      expect(store.commitLoopScan(binding, [delivered])).toBe(true);
      const deliveredInput = store.listLoopInputs(botUid)[0]!;
      expect(store.claimLoopInputIntent(deliveredInput.id, mainTarget)).toBe(true);
      store.markLoopInputDelivered(deliveredInput.id);
      expect(store.getLoopInput(deliveredInput.id)?.state).toBe("delivered");
      expect(store.listLoopInputs(botUid)).toEqual([]);

      expect(store.markLoopInputFault(input.id, true)).toBe(true);
      expect(store.markLoopInputFault(input.id, true)).toBe(false);
      expect(store.getLoopInput(input.id)?.faultNotified).toBe(true);
    } finally {
      store.close();
    }

    store = await openStore(root);
    try {
      expect(store.getLoopInput(inputId)).toMatchObject({
        id: inputId,
        state: "unknown",
        target: { channelType: 2, channelId: "replacement-target" },
        faultNotified: true,
      });
      expect(store.listLoopInputs(botUid)).toEqual([]);
    } finally {
      store.close();
    }
  });
});

test("loop task routes are isolated and increment their durable version only when the target changes", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
      const otherGroupId = "another-loop-group";
      store.bindLoopWorkspace(botUid, otherGroupId, workspaceA, [loopTask()]);
      store.bindLoopWorkspace("another-bot", groupId, workspaceA, [loopTask()]);
      const otherGroupRoute = store.bindLoopTask(taskKey({ groupId: otherGroupId }), `${otherGroupId}____thread`);
      const otherBotRoute = store.bindLoopTask(taskKey({ botUid: "another-bot" }), `${groupId}____other-bot-thread`);
      expect(store.getLoopTaskRoute(taskKey({ groupId: otherGroupId }))).toEqual(otherGroupRoute);
      expect(store.getLoopTaskRoute(taskKey({ botUid: "another-bot" }))).toEqual(otherBotRoute);
      const key = taskKey();
      const first = store.bindLoopTask(key, `${groupId}____thread-a`);
      expect(first.version).toBe(1);
      expect(store.bindLoopTask(key, `${groupId}____thread-a`)).toEqual(first);
      const second = store.bindLoopTask(key, `${groupId}____thread-b`);
      expect(second.version).toBe(2);
      expect(store.listLoopThreadTasks(botUid, groupId, `${groupId}____thread-a`)).toEqual([]);
      expect(store.listLoopThreadTasks(botUid, groupId, `${groupId}____thread-b`)).toEqual([second]);
      expect(store.listLoopThreadTasks(botUid, groupId, `${groupId}____other-bot-thread`)).toEqual([]);
      expect(() => store.bindLoopTask(taskKey({ workspaceId: "not-current" }), `${groupId}____thread-c`)).toThrow();
    } finally {
      store.close();
    }
  });
});

test("loop creation reservation is unique, unknown is never reassigned, and stale allocations cannot publish", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    let contender: Store | null = null;
    try {
      contender = await openStore(root);
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
      const key = taskKey();
      const [first, concurrentRetry] = await Promise.all([
        Promise.resolve(store.reserveLoopThreadCreation(key, "tool-call-1")),
        Promise.resolve(contender!.reserveLoopThreadCreation(key, "tool-call-2")),
      ]);
      expect(first).toEqual(concurrentRetry);
      expect(first).toMatchObject({ operationId: "tool-call-1", state: "reserved", bindingVersion: 1, routeVersion: 0 });

      store.recordLoopThreadAllocated(key, first.operationId, `${groupId}____allocated`);
      const initialRoute = store.bindLoopTask(key, `${groupId}____existing`);
      const movedRoute = store.bindLoopTask(key, `${groupId}____replacement`);
      expect(initialRoute.version).toBe(1);
      expect(movedRoute.version).toBe(initialRoute.version + 1);

      expect(store.completeLoopThreadCreation(key, first.operationId)).toBe(false);
      expect(store.getLoopTaskRoute(key)).toEqual(movedRoute);
      expect(store.getLoopThreadCreation(key)).toMatchObject({
        state: "superseded",
        threadChannelId: `${groupId}____allocated`,
      });

      const unknownKey = taskKey({ taskId: "loop-task-unknown" });
      const unknown = store.reserveLoopThreadCreation(unknownKey, "tool-call-unknown");
      store.recordLoopThreadAllocated(unknownKey, unknown.operationId, `${groupId}____unknown-partial`);
      store.markLoopThreadCreationUnknown(unknownKey, unknown.operationId, "remote response timed out");
      expect(store.reserveLoopThreadCreation(unknownKey, "must-not-reallocate")).toMatchObject({
        operationId: "tool-call-unknown",
        state: "unknown",
        threadChannelId: `${groupId}____unknown-partial`,
        error: "remote response timed out",
      });
      expect(store.getLoopThreadCreation(unknownKey)?.operationId).toBe("tool-call-unknown");
    } finally {
      store.close();
      contender?.close();
    }
  });
});

test("loop thread creation atomically publishes its route and remains complete after reopen", async () => {
  await inTemporaryDirectory(async (root) => {
    let store = await openStore(root);
    const key = taskKey();
    try {
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
      const creation = store.reserveLoopThreadCreation(key, "tool-call-success");
      store.recordLoopThreadAllocated(key, creation.operationId, `${groupId}____created`);
      expect(store.completeLoopThreadCreation(key, creation.operationId)).toBe(true);
      expect(store.getLoopTaskRoute(key)).toMatchObject({
        threadChannelId: `${groupId}____created`,
        version: 1,
      });
      expect(store.getLoopThreadCreation(key)).toMatchObject({ state: "ready", threadChannelId: `${groupId}____created` });
      expect(store.completeLoopThreadCreation(key, creation.operationId)).toBe(true);
    } finally {
      store.close();
    }

    store = await openStore(root);
    try {
      expect(store.getLoopTaskRoute(key)).toMatchObject({ threadChannelId: `${groupId}____created`, version: 1 });
      expect(store.getLoopThreadCreation(key)).toMatchObject({ state: "ready", operationId: "tool-call-success" });
    } finally {
      store.close();
    }
  });
});

test("workspace replacement invalidates an old thread creation while preserving its remote resource identity", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openStore(root);
    try {
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
      const key = taskKey();
      const creation = store.reserveLoopThreadCreation(key, "tool-call-before-rebind");
      store.bindLoopWorkspace(botUid, groupId, workspaceB, [loopTask()]);
      store.recordLoopThreadAllocated(key, creation.operationId, `${groupId}____remote-created`);

      expect(store.completeLoopThreadCreation(key, creation.operationId)).toBe(false);
      expect(store.getLoopTaskRoute(key)).toBeNull();
      expect(store.getLoopThreadCreation(key)).toMatchObject({
        state: "superseded",
        threadChannelId: `${groupId}____remote-created`,
      });
    } finally {
      store.close();
    }
  });
});

test("loop storage migrates additively without changing legacy owner or metadata rows", async () => {
  await inTemporaryDirectory(async (root) => {
    const databasePath = join(root, "channel.sqlite");
    const legacy = new Database(databasePath);
    try {
      legacy.exec(`
        CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        INSERT INTO meta (key, value) VALUES ('legacy-marker', 'preserved');
        CREATE TABLE owners (
          bot_uid TEXT PRIMARY KEY NOT NULL,
          owner_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO owners (bot_uid, owner_id, created_at, updated_at)
        VALUES ('legacy-bot', 'legacy-owner', 1, 2);
        CREATE TABLE legacy_notes (note TEXT NOT NULL);
        INSERT INTO legacy_notes (note) VALUES ('kept');
      `);
    } finally {
      legacy.close();
    }

    let store = await openStore(root);
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      store.bindLoopWorkspace(botUid, groupId, workspaceA, [loopTask()]);
    } finally {
      store.close();
    }

    store = await openStore(root);
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      expect(store.getLoopBinding(botUid, groupId)).toMatchObject({ workspaceId: workspaceA.workspaceId, version: 1 });
      expect(store.listLoopInputs(botUid)).toHaveLength(1);
    } finally {
      store.close();
    }

    const migrated = new Database(databasePath);
    try {
      expect(migrated.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get()).toEqual({ value: "preserved" });
      expect(migrated.query("SELECT note FROM legacy_notes").get()).toEqual({ note: "kept" });
      expect(migrated.query("SELECT owner_id FROM owners WHERE bot_uid = 'legacy-bot'").get()).toEqual({ owner_id: "legacy-owner" });
    } finally {
      migrated.close();
    }
  });
});
