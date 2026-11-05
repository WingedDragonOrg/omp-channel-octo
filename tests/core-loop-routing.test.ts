import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LoopController } from "../src/loop/controller.ts";
import type { InboundMessage } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { ChannelStore } from "../src/store/index.ts";
import type { LoopInput, LoopTask, LoopWorkspace } from "../src/loop/types.ts";

const GROUP_ID = "group-allowed";
const WORKSPACE: LoopWorkspace = {
  workspaceId: "loop-workspace-1",
  name: "Product Tasks",
  slug: "product-tasks",
};

function task(taskId: string, title: string, updatedAt = "2026-11-04T00:00:00Z"): LoopTask {
  return {
    taskId,
    title,
    description: "fixture task description",
    status: "open",
    priority: "normal",
    assigneeType: "member",
    assigneeId: "fleet-owner-1",
    assigneeOctoUid: "owner-1",
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-03T00:00:00Z",
    updatedAt,
  };
}

function groupMessage(botUid: string, messageId: string, text: string): InboundMessage {
  return inbound({
    channelType: 2,
    channelId: GROUP_ID,
    messageId,
    text,
    mentionedUserIds: [botUid],
  });
}

function bindMessage(botUid: string, messageId: string, selector = WORKSPACE.workspaceId): InboundMessage {
  return groupMessage(botUid, messageId, `/bind loop ${selector}`);
}

function loopFixture(tasks: LoopTask[] = []): FakeLoopReadPort {
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(WORKSPACE.workspaceId, WORKSPACE);
  loop.tasks.set(WORKSPACE.workspaceId, tasks);
  return loop;
}
async function waitUntil(predicate: () => boolean, description: string, timeoutMs = 3_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForSignal<T>(
  signal: Promise<T>,
  description: string,
  timeoutMs = 3_000,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timedOut = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out waiting for ${description}`)), timeoutMs);
    });
    return await Promise.race([signal, timedOut]);
  } finally {
    clearTimeout(timer);
  }
}

test("an old periodic poll cannot overwrite a newer same-workspace bind snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "loop-poll-bind-race-"));
  const loop = loopFixture([task("loop-race-task", "Snapshot A")]);
  const pollEntered = Promise.withResolvers<void>();
  const releasePoll = Promise.withResolvers<void>();
  let store: ChannelStore | undefined;
  let controller: LoopController | undefined;
  let raw: Database | undefined;
  try {
    store = await openChannelStore(root);
    store.setOwnerId("loop-race-bot", "owner-1");
    controller = new LoopController({
      store,
      read: loop,
      botUid: "loop-race-bot",
      validateTarget: async () => {},
      deliver: async (_input, _binding, _target, _text, guard, _beforeSteer, beforeAccept) =>
        guard() && beforeAccept() ? "accepted" : "not-accepted",
      isStopping: () => false,
      notifyFault: async () => {},
    });
    await controller.recover();
    await controller.bind(GROUP_ID, WORKSPACE.workspaceId);

    let holdPoll = true;
    loop.afterListTasksSnapshot = async (workspaceId, snapshot) => {
      if (!holdPoll || workspaceId !== WORKSPACE.workspaceId) return;
      holdPoll = false;
      expect(snapshot[0]?.title).toBe("Snapshot A");
      pollEntered.resolve();
      await releasePoll.promise;
    };
    controller.start();
    await waitForSignal(pollEntered.promise, "periodic Loop snapshot poll");

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-race-task", "Snapshot B")]);
    const sameWorkspaceBind = controller.bind(GROUP_ID, WORKSPACE.workspaceId);
    await new Promise<void>((resolve) => setImmediate(resolve));
    releasePoll.resolve();
    await sameWorkspaceBind;
    await new Promise<void>((resolve) => setImmediate(resolve));
    await controller.stop();

    raw = new Database(store!.path, { readonly: true });
    const updates = raw.query(
      "SELECT tasks_json, previous_json FROM loop_inputs WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND kind = 'updated'",
    ).all("loop-race-bot", GROUP_ID, WORKSPACE.workspaceId) as Array<{
      tasks_json: string;
      previous_json: string;
    }>;
    expect(updates).toHaveLength(1);
    expect(JSON.parse(updates[0]!.tasks_json)[0].title).toBe("Snapshot B");
    expect(JSON.parse(updates[0]!.previous_json).title).toBe("Snapshot A");

    const snapshot = raw.query(
      "SELECT task_json FROM loop_task_snapshots WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?",
    ).get("loop-race-bot", GROUP_ID, WORKSPACE.workspaceId, "loop-race-task") as { task_json: string } | null;
    expect(JSON.parse(snapshot!.task_json).title).toBe("Snapshot B");
  } finally {
    releasePoll.resolve();
    await controller?.stop();
    raw?.close();
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
});


test("Loop summaries bound long descriptions without losing task IDs or full stored snapshots", async () => {
  const longDescription = "Full Loop task detail. ".repeat(2_000);
  const detailed = {
    ...task("loop-task-long", "Long-description task"),
    description: longDescription,
    status: "in-progress",
    priority: "high",
    assigneeType: "member",
    assigneeId: "owner-42",
    projectId: "project-7",
    parentTaskId: "parent-3",
    startDate: "2026-11-01",
    dueDate: "2026-11-30",
  };
  const inputTasks = [detailed, task("loop-task-two", "Second task"), task("loop-task-three", "Third task")];
  const loop = loopFixture(inputTasks);
  const harness = await createCoreHarness({ loop });
  let raw: Database | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "summary-boundary-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "Loop summary baseline prompt");
    await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.state === "completed" && status.queued === 0,
    );

    const text = harness.workers.prompts[0]?.text ?? "";
    const jsonStart = text.indexOf("```json\n") + "```json\n".length;
    const jsonEnd = text.lastIndexOf("\n```");
    const rendered = JSON.parse(text.slice(jsonStart, jsonEnd)) as {
      tasks: Array<{
        task_id: string;
        current: {
          title_preview: string;
          description_preview: string;
          title_truncated: boolean;
          description_truncated: boolean;
          detail_lookup: string | null;
          status: string;
          priority: string;
          assignee_type: string | null;
          assignee_id: string | null;
          project_id: string | null;
          parent_task_id: string | null;
          start_date: string | null;
          due_date: string | null;
          created_at: string;
          updated_at: string;
        };
      }>;
      baseline_batch: { task_ids: string[]; task_count: number; coverage: string };
    };
    const summary = rendered.tasks.find((entry) => entry.task_id === detailed.taskId)?.current;
    expect(rendered.tasks.map((entry) => entry.task_id)).toEqual(inputTasks.map((entry) => entry.taskId));
    expect(rendered.baseline_batch.task_ids).toEqual(inputTasks.map((entry) => entry.taskId));
    expect(rendered.baseline_batch.task_count).toBe(inputTasks.length);
    expect(summary?.description_truncated).toBe(true);
    expect(summary).toMatchObject({
      status: detailed.status,
      priority: detailed.priority,
      assignee_type: detailed.assigneeType,
      assignee_id: detailed.assigneeId,
      project_id: detailed.projectId,
      parent_task_id: detailed.parentTaskId,
      start_date: detailed.startDate,
      due_date: detailed.dueDate,
      created_at: detailed.createdAt,
      updated_at: detailed.updatedAt,
    });
    expect(text).not.toContain(longDescription);

    raw = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
    const row = raw.query(
      "SELECT task_json FROM loop_task_snapshots WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?",
    ).get(harness.bot.uid, GROUP_ID, WORKSPACE.workspaceId, detailed.taskId) as { task_json: string } | null;
    expect(row).not.toBeNull();
    expect(JSON.parse(row!.task_json)).toEqual(detailed);
    raw?.close();
    raw = undefined;
    const updated = { ...detailed, description: "Compact changed detail", updatedAt: "2026-11-05T00:00:00Z" };
    loop.tasks.set(WORKSPACE.workspaceId, [updated, ...inputTasks.slice(1)]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "summary-boundary-update"));
    await waitUntil(() => harness.workers.prompts.length >= 2, "Loop summary update prompt");

    const updateText = harness.workers.prompts[1]?.text ?? "";
    const updateStart = updateText.indexOf("```json\n") + "```json\n".length;
    const updateEnd = updateText.lastIndexOf("\n```");
    const update = JSON.parse(updateText.slice(updateStart, updateEnd)) as {
      tasks: Array<{
        current: { description_preview: string; description_truncated: boolean };
        previous: { description_preview: string; description_truncated: boolean } | null;
      }>;
    };
    expect(update.tasks).toHaveLength(1);
    expect(update.tasks[0]?.current).toMatchObject({
      description_preview: "Compact changed detail",
      description_truncated: false,
    });
    expect(update.tasks[0]?.previous?.description_preview.length).toBeLessThanOrEqual(601);
    expect(update.tasks[0]?.previous?.description_truncated).toBe(true);
    expect(updateText).not.toContain(longDescription);

    raw = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
    const previousRow = raw.query(
      "SELECT previous_json FROM loop_inputs WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND kind = 'updated' AND task_id = ?",
    ).get(harness.bot.uid, GROUP_ID, WORKSPACE.workspaceId, detailed.taskId) as { previous_json: string } | null;
    expect(previousRow).not.toBeNull();
    expect(JSON.parse(previousRow!.previous_json).description).toBe(longDescription);
  } finally {
    raw?.close();
    await closeCoreHarness(harness);
  }
});

test("new unassociated tasks reach the main group while linked updates go only to their child", async () => {
  const loop = loopFixture([task("loop-task-1", "Initial task")]);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "route-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "initial child-route prompt");
    const thread = await harness.transport.createThread(GROUP_ID, "Task one");
    store = await openChannelStore(harness.stateDir);
    const key = {
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: "loop-task-1",
    };
    store.bindLoopTask(key, thread.channelId);
    await harness.transport.archiveThread(GROUP_ID, thread.shortId);

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-1", "Updated task")]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "route-update"));
    await waitUntil(() => harness.workers.prompts.length >= 2, "child-route update prompt");
    const child = await waitForStatus(
      harness.service,
      { channelType: 5, channelId: thread.channelId },
      (status) => status.state === "completed",
    );

    expect(child.sessionId).not.toBeNull();
    expect(harness.workers.prompts[1]?.text).toContain("Updated task");
    expect(harness.workers.prompts).toHaveLength(2);

    loop.tasks.set(WORKSPACE.workspaceId, [
      task("loop-task-1", "Updated task"),
      task("loop-task-2", "New unassociated task"),
    ]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "route-new-task"));
    await waitUntil(() => harness.workers.prompts.length >= 3, "new unassociated task prompt");
    expect(harness.workers.prompts[2]?.text).toContain("New unassociated task");
    expect((await harness.service.status({ channelType: 2, channelId: GROUP_ID })).sessionId)
      .not.toBe(child.sessionId);
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a busy Loop destination steers the stable system input instead of creating a second task", async () => {
  const loop = loopFixture([task("loop-task-busy-baseline", "Initial busy work")]);
  const promptStarted = Promise.withResolvers<void>();
  const steered = Promise.withResolvers<string>();
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      promptStarted.resolve();
      const appended = await context.waitForAppend();
      steered.resolve(appended.text);
    },
  });
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "busy-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "busy Loop worker prompt");
    await waitForSignal(promptStarted.promise, "busy Loop worker started");

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-busy", "Queued behind active work")]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "busy-new-task"));
    const append = await waitForSignal(steered.promise, "busy Loop steer append");
    const status = await harness.service.status({ channelType: 2, channelId: GROUP_ID });

    expect(append).toContain("Queued behind active work");
    expect(status.queued).toBe(0);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("cancelled Loop steer waiting behind steerSerial stays pending without an accepted claim", async () => {
  vi.useFakeTimers();
  const loop = loopFixture([task("loop-task-cancel-baseline", "Initial cancel work")]);
  const promptStarted = Promise.withResolvers<void>();
  const workerAborted = Promise.withResolvers<void>();
  const steerEntered = Promise.withResolvers<void>();
  const releaseSteer = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      promptStarted.resolve();
      await context.waitForAppend();
      await context.waitForAbort();
      workerAborted.resolve();
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "cancel-loop-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "cancelled Loop worker prompt");
    await waitForSignal(promptStarted.promise, "cancelled Loop worker started");
    harness.workers.beforeSteer = async (text) => {
      if (text !== "Ordinary steer before Loop update") return;
      steerEntered.resolve();
      await releaseSteer.promise;
      throw new Error("simulated unknown steer response");
    };
    const ordinarySteer = harness.service.submitInbound(
      groupMessage(harness.bot.uid, "cancel-blocking-steer", "Ordinary steer before Loop update"),
    );
    await waitForSignal(steerEntered.promise, "ordinary Loop steer gate entered");

    store = await openChannelStore(harness.stateDir);
    const taskListCallsBeforePoll = loop.taskListCalls.length;
    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-cancelled", "Cancelled Loop update")]);
    vi.advanceTimersByTime(60_000);

    let loopInput: LoopInput | undefined;
    await waitUntil(() => {
      loopInput = store!.listLoopInputs(harness.bot.uid)
        .find((input) => input.kind === "created" && input.taskId === "loop-task-cancelled");
      return loop.taskListCalls.length > taskListCallsBeforePoll
        && loopInput !== undefined && store!.getCommandClaimState(
          harness.bot.uid,
          { channelType: 2, channelId: GROUP_ID },
          `loop:${loopInput.id}`,
        ) === "pending";
    }, "periodic Loop update to remain pending behind the active steering delivery");
    expect(loopInput).toBeDefined();
    expect(store.getCommandClaimState(
      harness.bot.uid,
      { channelType: 2, channelId: GROUP_ID },
      `loop:${loopInput!.id}`,
    )).toBe("pending");

    const cancel = harness.service.submitInbound(
      groupMessage(harness.bot.uid, "cancel-loop-while-steering", "/cancel"),
    );
    await waitForSignal(workerAborted.promise, "cancelled Loop worker abort");
    releaseSteer.resolve();
    await Promise.all([ordinarySteer, cancel]);
    await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.state === "cancelled" && status.queued === 0,
    );

    await waitUntil(() => store!.getCommandClaimState(
      harness.bot.uid,
      { channelType: 2, channelId: GROUP_ID },
      `loop:${loopInput!.id}`,
    ) !== "pending", "cancel to release the Loop command claim");
    expect(store.getLoopInput(loopInput!.id)?.state).toBe("pending");
    expect(store.getCommandClaimState(
      harness.bot.uid,
      { channelType: 2, channelId: GROUP_ID },
      `loop:${loopInput!.id}`,
    )).toBeNull();
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    releaseSteer.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
    store?.close();
  }
});

test("task-aware child dispatch never falls back to a second task after an uncertain steer", async () => {
  const loop = loopFixture([task("loop-task-dispatch", "Dispatch source")]);
  const childStarted = Promise.withResolvers<void>();
  const childEffect = Promise.withResolvers<string>();
  const steerEntered = Promise.withResolvers<void>();
  const releaseSteer = Promise.withResolvers<void>();
  const dispatchResult = Promise.withResolvers<{ ok: boolean; response?: string }>();
  let dispatchShortId = "";
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      if (context.text === "child hold") {
        childStarted.resolve();
        const appended = await context.waitForAppend();
        childEffect.resolve(appended.text);
        return;
      }
      if (context.text !== "dispatch-control") return;
      try {
        const management = context.promptContext?.tools?.find((tool) => tool.name === "octo_management");
        if (!management) throw new Error("management tool missing");
        const response = await management.execute({
          action: "thread-dispatch",
          short_id: dispatchShortId,
          task_id: "loop-task-dispatch",
          text: "dispatch body with an uncertain acknowledgement",
        }, new AbortController().signal);
        dispatchResult.resolve({ ok: true, response });
      } catch {
        dispatchResult.resolve({ ok: false });
      }
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "dispatch-source-bind"));
    await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.state === "completed" && status.queued === 0,
    );
    const thread = await harness.transport.createThread(GROUP_ID, "Task dispatch child");
    dispatchShortId = thread.shortId;
    harness.transport.setThreads(GROUP_ID, [thread]);
    store = await openChannelStore(harness.stateDir);
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: "loop-task-dispatch",
    }, thread.channelId);

    await harness.service.submitInbound(inbound({
      channelType: 5,
      channelId: thread.channelId,
      messageId: "dispatch-child-session",
      text: `/new ${harness.root}`,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.service.submitInbound(inbound({
      channelType: 5,
      channelId: thread.channelId,
      messageId: "dispatch-child-hold",
      text: "child hold",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitUntil(() => harness.workers.prompts.length >= 2, "child-session prompts");
    await waitForSignal(childStarted.promise, "child task start");
    harness.workers.beforeSteer = async (text) => {
      if (!text.includes("dispatch body with an uncertain acknowledgement")) return;
      steerEntered.resolve();
      await releaseSteer.promise;
      throw new Error("simulated lost steer acknowledgement");
    };

    await harness.service.submitInbound(groupMessage(harness.bot.uid, "dispatch-control-task", "dispatch-control"));
    await waitUntil(() => harness.workers.prompts.length >= 3, "dispatch-control worker prompt");
    await waitForSignal(steerEntered.promise, "uncertain Loop steer gate entered");
    expect(await waitForSignal(childEffect.promise, "child dispatch append")).toContain(
      "dispatch body with an uncertain acknowledgement",
    );
    releaseSteer.resolve();
    const result = await waitForSignal(dispatchResult.promise, "dispatch-control result");
    await waitForStatus(
      harness.service,
      { channelType: 5, channelId: thread.channelId },
      (status) => status.state === "completed" && status.queued === 0,
    );

    expect(result.ok).toBe(false);
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    releaseSteer.resolve();
    await closeCoreHarness(harness);
    store?.close();
  }
});

test("Loop input queues behind a pending ask without answering or steering it", async () => {
  const loop = loopFixture([task("loop-task-ask", "Baseline task")]);
  const askStarted = Promise.withResolvers<void>();
  let promptCount = 0;
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      promptCount += 1;
      if (promptCount !== 1) return;
      const ask = context.promptContext?.tools?.find((tool) => tool.name === "ask");
      if (!ask) throw new Error("ask tool unavailable in fixture prompt");
      askStarted.resolve();
      await ask.execute({ questions: [{ id: "pending-loop-question", question: "Which option?" }] }, new AbortController().signal);
    },
  });
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "ask-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "Loop ask worker prompt");
    await waitForSignal(askStarted.promise, "Loop ask tool start");
    const waiting = await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.pendingQuestionIds.length === 1,
    );

    loop.tasks.set(WORKSPACE.workspaceId, [
      task("loop-task-ask", "Baseline task"),
      task("loop-task-new-during-ask", "Independent queued Loop input"),
    ]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "ask-new-loop-task"));
    const after = await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.queued === 1 && status.pendingQuestionIds.length === 1,
    );

    expect(waiting.pendingQuestionIds).toHaveLength(1);
    expect(after.pendingQuestionIds).toEqual(waiting.pendingQuestionIds);
    expect(after.queued).toBe(1);
    expect(harness.workers.prompts).toHaveLength(1);

    await harness.service.submitInbound(groupMessage(harness.bot.uid, "ask-answer", "Owner's answer"));
    await waitUntil(() => harness.workers.prompts.length >= 2, "Loop task after answer");
    await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.state === "completed" && status.queued === 0,
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a binding replaced during selector reads rejects the stale read result", async () => {
  const loop = loopFixture([task("loop-task-replace-baseline", "Original workspace work")]);
  const second: LoopWorkspace = {
    workspaceId: "loop-workspace-2",
    name: "Replacement Tasks",
    slug: "replacement-tasks",
  };
  loop.workspaces.set(second.workspaceId, second);
  loop.tasks.set(second.workspaceId, []);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  const releaseRead = Promise.withResolvers<void>();
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "replace-bind-first"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "initial replacement-binding prompt");

    const readEntered = Promise.withResolvers<void>();
    loop.beforeListTasks = async (workspaceId) => {
      if (workspaceId === WORKSPACE.workspaceId) {
        readEntered.resolve();
        await releaseRead.promise;
      }
    };
    const staleBind = harness.service.submitInbound(bindMessage(harness.bot.uid, "replace-stale", WORKSPACE.workspaceId));
    await waitForSignal(readEntered.promise, "stale selector read");
    loop.beforeListTasks = undefined;
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "replace-current", second.workspaceId));
    releaseRead.resolve();
    await staleBind;
    store = await openChannelStore(harness.stateDir);

    expect(store.getLoopBinding(harness.bot.uid, GROUP_ID)?.workspaceId).toBe(second.workspaceId);
  } finally {
    releaseRead.resolve();
    await closeCoreHarness(harness);
    store?.close();
  }
});

test("restart does not replay delivered inputs and later business changes remain observable", async () => {
  const loop = loopFixture([task("loop-task-restart", "Before restart")]);
  const first = await createCoreHarness({ loop });
  await first.service.submitInbound(bindMessage(first.bot.uid, "restart-bind"));
  await waitForStatus(
    first.service,
    { channelType: 2, channelId: GROUP_ID },
    (status) => status.state === "completed" && status.queued === 0,
  );
  const root = first.root;
  const botUid = first.bot.uid;
  await closeCoreHarness(first, { remove: false });

  const second = await createCoreHarness({ root, loop });
  let store: ChannelStore | undefined;
  try {
    expect(second.workers.prompts).toEqual([]);
    loop.tasks.set(WORKSPACE.workspaceId, [
      task("loop-task-restart", "Before restart", "2026-11-05T00:00:00Z"),
    ]);
    await second.service.submitInbound(bindMessage(botUid, "restart-timestamp-only"));
    store = await openChannelStore(second.stateDir);
    expect(second.workers.prompts).toEqual([]);
    expect(store.listLoopInputs(botUid).filter((input) => input.kind !== "baseline")).toHaveLength(0);

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-restart", "After restart")]);
    await second.service.submitInbound(bindMessage(botUid, "restart-business-change"));
    await waitUntil(() => second.workers.prompts.length >= 1, "post-restart business-change prompt");
    expect(second.workers.prompts[0]?.text).toContain("After restart");
  } finally {
    store?.close();
    await closeCoreHarness(second);
  }
});

test("stop aborts a Loop selector read before the shared store is closed", async () => {
  const loop = loopFixture();
  const harness = await createCoreHarness({ loop });
  try {
    const entered = Promise.withResolvers<void>();
    loop.beforeListTasks = async (_workspaceId, signal) => {
      entered.resolve();
      const aborted = Promise.withResolvers<void>();
      if (signal?.aborted) aborted.resolve();
      else signal?.addEventListener("abort", () => aborted.resolve(), { once: true });
      await aborted.promise;
    };
    const inboundOperation = harness.service.submitInbound(bindMessage(harness.bot.uid, "stop-loop-read"));
    await entered.promise;
    await harness.service.stop();
    await inboundOperation.catch(() => {});
    expect(loop.taskListCalls).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("service shutdown stops the active worker before awaiting a queued Loop steer", async () => {
  vi.useFakeTimers();
  const loop = loopFixture([task("loop-task-stop-baseline", "Baseline before shutdown")]);
  const promptStarted = Promise.withResolvers<void>();
  const steerEntered = Promise.withResolvers<void>();
  const releaseSteer = Promise.withResolvers<void>();
  const workerStopStarted = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      promptStarted.resolve();
      await context.waitForAppend();
      await context.waitForAbort();
    },
  });
  let store: ChannelStore | undefined;
  let ordinarySteer: Promise<void> | undefined;
  let shutdown: Promise<void> | undefined;
  try {
    harness.workers.stopHandler = () => workerStopStarted.resolve();
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "stop-steer-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "shutdown-test worker prompt");
    await waitForSignal(promptStarted.promise, "shutdown-test worker start");

    harness.workers.beforeSteer = async (text) => {
      if (text !== "Ordinary steer holds the serial lane") return;
      steerEntered.resolve();
      await releaseSteer.promise;
    };
    ordinarySteer = harness.service.submitInbound(
      groupMessage(harness.bot.uid, "stop-steer-blocker", "Ordinary steer holds the serial lane"),
    );
    await waitForSignal(steerEntered.promise, "shutdown-test steer gate entered");

    store = await openChannelStore(harness.stateDir);
    const taskListCallsBeforePoll = loop.taskListCalls.length;
    loop.tasks.set(WORKSPACE.workspaceId, [
      task("loop-task-stop-baseline", "Baseline before shutdown"),
      task("loop-task-stop-update", "Loop update queued behind steer"),
    ]);
    vi.advanceTimersByTime(60_000);

    let input: LoopInput | undefined;
    await waitUntil(() => {
      input = store!.listLoopInputs(harness.bot.uid)
        .find((item) => item.taskId === "loop-task-stop-update");
      return loop.taskListCalls.length > taskListCallsBeforePoll
        && input !== undefined && store!.getCommandClaimState(
          harness.bot.uid,
          { channelType: 2, channelId: GROUP_ID },
          `loop:${input.id}`,
        ) === "pending";
    }, "periodic Loop claim to remain pending before shutdown");
    expect(input).toBeDefined();
    expect(store.getCommandClaimState(
      harness.bot.uid,
      { channelType: 2, channelId: GROUP_ID },
      `loop:${input!.id}`,
    )).toBe("pending");

    shutdown = harness.service.stop();
    const workerCloseResult = Promise.withResolvers<boolean>();
    const timeout = setTimeout(() => workerCloseResult.resolve(false), 500);
    void workerStopStarted.promise.then(() => workerCloseResult.resolve(true));
    const workerCloseReached = await workerCloseResult.promise;
    clearTimeout(timeout);
    expect(workerCloseReached).toBe(true);
    releaseSteer.resolve();
    await waitForSignal(shutdown, "service shutdown completion");
    await waitForSignal(ordinarySteer, "ordinary steer completion after shutdown");
  } finally {
    releaseSteer.resolve();
    vi.useRealTimers();
    await Promise.allSettled([
      ...(ordinarySteer === undefined ? [] : [ordinarySteer]),
      ...(shutdown === undefined ? [] : [shutdown]),
    ]);
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("one unavailable child fault notifies the main group once across pending task changes", async () => {
  const loop = loopFixture([task("loop-task-fault", "Before fault")]);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "fault-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "initial child-fault prompt");
    const thread = await harness.transport.createThread(GROUP_ID, "Faulted child");
    store = await openChannelStore(harness.stateDir);
    await waitUntil(
      () => store!.listLoopInputs(harness.bot.uid)
        .filter(input => input.workspaceId === WORKSPACE.workspaceId).length === 0,
      "initial Loop baseline durable handoff",
    );
    const key = {
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: "loop-task-fault",
    };
    store.bindLoopTask(key, thread.channelId);
    harness.transport.failManagementWith(new Error("fixture access denied"));

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-fault", "First unavailable update")]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "fault-update-1"));
    await waitUntil(() => {
      const notices = harness.transport.sent.filter(({ target, text }) =>
        target.channelType === 2
        && target.channelId === GROUP_ID
        && text.includes("Loop 关联子区暂不可用"));
      const firstFault = store!.listLoopInputs(harness.bot.uid).some((input) =>
        input.taskId === "loop-task-fault"
        && input.faultNotified
        && input.tasks.some(({ title }) => title === "First unavailable update"));
      return notices.length === 1 && firstFault;
    }, "first Loop update fault and its single notice to persist");
    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-fault", "Second unavailable update")]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "fault-update-2"));

    await waitUntil(() => {
      const notices = harness.transport.sent.filter(({ target, text }) =>
        target.channelType === 2
        && target.channelId === GROUP_ID
        && text.includes("Loop 关联子区暂不可用"));
      const inputs = store!.listLoopInputs(harness.bot.uid).filter((input) =>
        input.workspaceId === WORKSPACE.workspaceId
        && input.taskId === "loop-task-fault");
      return notices.length === 1 && inputs.length === 2 && inputs.every((input) => input.faultNotified);
    }, "both Loop faults and their single notice to persist");

    const faultNotices = harness.transport.sent.filter(({ target, text }) =>
      target.channelType === 2
      && target.channelId === GROUP_ID
      && text.includes("Loop 关联子区暂不可用"));
    const pending = store.listLoopInputs(harness.bot.uid).filter((input) =>
      input.workspaceId === WORKSPACE.workspaceId
      && input.taskId === "loop-task-fault");
    expect(faultNotices).toHaveLength(1);
    expect(pending).toHaveLength(2);
    expect(pending.every((input) => input.faultNotified)).toBe(true);
    expect(harness.workers.prompts).toHaveLength(1);

    harness.transport.failManagementWith(undefined);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "fault-retry"));
    await waitUntil(() => harness.workers.prompts.length >= 3, "child-fault retry prompt");
    await waitForStatus(
      harness.service,
      { channelType: 5, channelId: thread.channelId },
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    await closeCoreHarness(harness);
    store?.close();
  }
});

test("a route replacement during child validation prevents acceptance by the old child", async () => {
  const loop = loopFixture([task("loop-task-route-race", "Before route change")]);
  const harness = await createCoreHarness({ loop });
  const validationStarted = Promise.withResolvers<void>();
  const releaseValidation = Promise.withResolvers<void>();
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "route-race-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "initial route-race prompt");
    const oldThread = await harness.transport.createThread(GROUP_ID, "Old route");
    const currentThread = await harness.transport.createThread(GROUP_ID, "Current route");
    store = await openChannelStore(harness.stateDir);
    const key = {
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: "loop-task-route-race",
    };
    store.bindLoopTask(key, oldThread.channelId);
    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-route-race", "Changed during route read")]);

    harness.transport.beforeGetThread = async (_groupId, shortId) => {
      if (shortId === oldThread.shortId) {
        validationStarted.resolve();
        await releaseValidation.promise;
      }
    };
    const oldRouteDelivery = harness.service.submitInbound(bindMessage(harness.bot.uid, "route-race-update"));
    await waitForSignal(validationStarted.promise, "old child route validation");
    store.bindLoopTask(key, currentThread.channelId);
    releaseValidation.resolve();
    await oldRouteDelivery;
    harness.transport.beforeGetThread = undefined;

    await harness.service.submitInbound(bindMessage(harness.bot.uid, "route-race-current"));
    await waitUntil(() => harness.workers.prompts.length >= 2, "current-route worker prompt");
    const staleStatus = await harness.service.status({ channelType: 5, channelId: oldThread.channelId });
    const currentStatus = await waitForStatus(
      harness.service,
      { channelType: 5, channelId: currentThread.channelId },
      (status) => status.state === "completed",
    );

    expect(staleStatus.sessionId).toBeNull();
    expect(currentStatus.sessionId).not.toBeNull();
    expect(harness.workers.prompts[1]?.text).toContain("Changed during route read");
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    releaseValidation.resolve();
    await closeCoreHarness(harness);
    store?.close();
  }
});

test("direct Loop thread tasks use the thread's task-level completion policy", async () => {
  const loop = loopFixture([task("loop-task-listen", "Before child work")]);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "listen-bind"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "initial thread-listen prompt");
    const thread = await harness.transport.createThread(GROUP_ID, "Listen child");
    store = await openChannelStore(harness.stateDir);
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId: "loop-task-listen",
    }, thread.channelId);
    store.ensureThreadListen({
      botUid: harness.bot.uid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: GROUP_ID,
      threadName: thread.name,
      listen: "task",
    });
    store.setThreadListen(harness.bot.uid, { channelType: 5, channelId: thread.channelId }, "task");

    loop.tasks.set(WORKSPACE.workspaceId, [task("loop-task-listen", "After child work")]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "listen-change"));
    await waitUntil(() => harness.workers.prompts.length >= 3, "updated thread-listen prompt");
    await waitForStatus(
      harness.service,
      { channelType: 5, channelId: thread.channelId },
      (status) => status.state === "completed",
    );
    await waitForStatus(
      harness.service,
      { channelType: 2, channelId: GROUP_ID },
      (status) => status.state === "completed" && status.queued === 0,
    );

    expect(harness.workers.prompts[1]?.text).toContain("After child work");
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("steer intent with unknown acceptance becomes non-replayable during restart", async () => {
  const loop = loopFixture([task("loop-task-unknown", "Unconfirmed steer")]);
  const first = await createCoreHarness({ loop });
  const store = await openChannelStore(first.stateDir);
  const { binding } = store.bindLoopWorkspace(first.bot.uid, GROUP_ID, WORKSPACE, [
    task("loop-task-unknown", "Unconfirmed steer"),
  ]);
  const input = store.listLoopInputs(first.bot.uid)[0]!;
  expect(store.claimLoopInputIntent(input.id, { channelType: 2, channelId: GROUP_ID })).toBe(true);
  store.close();
  const root = first.root;
  const botUid = first.bot.uid;
  await closeCoreHarness(first, { remove: false });

  const second = await createCoreHarness({ root, loop });
  const recovered = await openChannelStore(second.stateDir);
  try {
    expect(recovered.getLoopBinding(botUid, GROUP_ID)?.version).toBe(binding.version);
    expect(recovered.getLoopInput(input.id)?.state).toBe("unknown");
    expect(second.workers.prompts).toEqual([]);
  } finally {
    recovered.close();
    await closeCoreHarness(second);
  }
});
