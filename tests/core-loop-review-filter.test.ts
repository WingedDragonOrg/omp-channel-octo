import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import type { BotIdentity, ChatTarget } from "../src/contracts.ts";
import type { LoopTask, LoopTaskKey, LoopWorkspace } from "../src/loop/types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const BOT_UID = "loop-review-filter-bot";
const OWNER_UID = "loop-review-filter-owner";
const GROUP_ID = "group-allowed";
const WORKSPACE: LoopWorkspace = {
  workspaceId: "loop-review-filter-workspace",
  name: "Review filter tasks",
  slug: "review-filter-tasks",
};
const MAIN_TARGET: ChatTarget = { channelType: 2, channelId: GROUP_ID };
const BOT: BotIdentity = { uid: BOT_UID, name: "Review Filter Bot", ownerUid: "identity-owner" };

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
    startDate: "2026-11-04T00:00:00Z",
    dueDate: null,
    createdAt: "2026-11-04T00:00:00Z",
    updatedAt: "2026-11-04T00:00:00Z",
    ...overrides,
  };
}

function taskKey(taskId: string): LoopTaskKey {
  return { botUid: BOT_UID, groupId: GROUP_ID, workspaceId: WORKSPACE.workspaceId, taskId };
}

function loopFixture(tasks: LoopTask[] = []): FakeLoopReadPort {
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(WORKSPACE.workspaceId, WORKSPACE);
  loop.tasks.set(WORKSPACE.workspaceId, tasks);
  return loop;
}

function bindMessage(messageId: string) {
  return inbound({
    channelType: 2,
    channelId: GROUP_ID,
    messageId,
    senderId: OWNER_UID,
    text: `/bind loop ${WORKSPACE.workspaceId}`,
    mentionedUserIds: [BOT_UID],
  });
}

async function startHarness(loop: FakeLoopReadPort) {
  return createCoreHarness({
    bot: BOT,
    ownerId: OWNER_UID,
    allowedGroupIds: [GROUP_ID],
    loop,
  });
}

interface PersistedInput {
  id: string;
  kind: string;
  task_id: string | null;
  state: string;
}

function persistedInputs(stateDir: string): PersistedInput[] {
  const database = new Database(join(stateDir, "channel.sqlite"));
  try {
    return database.query(
      `SELECT id, kind, task_id, state FROM loop_inputs
       WHERE bot_uid = ? ORDER BY sequence ASC`,
    ).all(BOT_UID) as PersistedInput[];
  } finally {
    database.close();
  }
}

interface RenderedTask {
  current: { title_preview: string | null; description_preview: string | null; status: string };
  previous: { title_preview: string | null; description_preview: string | null; status: string } | null;
}

function renderedTasks(prompt: string): RenderedTask[] {
  const match = /```json\n([\s\S]*?)\n```/u.exec(prompt);
  if (!match) throw new Error("Loop prompt did not contain its JSON payload");
  return (JSON.parse(match[1]!) as { tasks: RenderedTask[] }).tasks;
}

async function waitUntil(predicate: () => boolean, description: string): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (predicate()) return;
    const { promise, resolve } = Promise.withResolvers<void>();
    setImmediate(resolve);
    await promise;
  }
  throw new Error(`timed out waiting for ${description}`);
}

test("an in_review-only scan advances the snapshot without waking CoreService, then leaving review delivers", async () => {
  const initial = task("review-only", "Review only task");
  const loop = loopFixture([initial]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.submitInbound(bindMessage("review-only-bind"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const underReview = { ...initial, status: "in_review" };
    loop.tasks.set(WORKSPACE.workspaceId, [underReview]);
    await harness.service.submitInbound(bindMessage("review-only-change"));
    await waitUntil(
      () => store?.getLoopTaskSummary(taskKey(initial.taskId))?.status === "in_review",
      "the in_review snapshot",
    );
    expect(harness.workers.prompts).toHaveLength(1);
    expect(persistedInputs(harness.stateDir)).toHaveLength(1);
    expect(store.getLoopTaskSummary(taskKey(initial.taskId))).toMatchObject({ status: "in_review" });

    const done = { ...underReview, status: "done" };
    loop.tasks.set(WORKSPACE.workspaceId, [done]);
    await harness.service.submitInbound(bindMessage("review-leaves"));
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    expect(persistedInputs(harness.stateDir).map(({ kind }) => kind)).toEqual(["baseline", "updated"]);
    const [delivered] = renderedTasks(harness.workers.prompts[1]!.text);
    expect(delivered?.current.status).toBe("done");
    expect(delivered?.previous?.status).toBe("in_review");
    expect(store.getLoopTaskSummary(taskKey(initial.taskId))).toMatchObject({ status: "done" });
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a mixed title and description change into in_review delivers without presenting a status change", async () => {
  const initial = task("review-mixed", "Review draft", { description: "First draft" });
  const loop = loopFixture([initial]);
  const harness = await startHarness(loop);
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.submitInbound(bindMessage("mixed-review-bind"));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const mixed = {
      ...initial,
      title: "Review draft revised",
      description: "Revised draft",
      status: "in_review",
      updatedAt: "2026-11-04T01:00:00Z",
    };
    loop.tasks.set(WORKSPACE.workspaceId, [mixed]);
    await harness.service.submitInbound(bindMessage("mixed-review-change"));
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(
      harness.service,
      MAIN_TARGET,
      (status) => status.state === "completed" && status.queued === 0,
    );

    expect(persistedInputs(harness.stateDir).map(({ kind }) => kind)).toEqual(["baseline", "updated"]);
    const [delivered] = renderedTasks(harness.workers.prompts[1]!.text);
    expect(delivered?.current).toMatchObject({
      title_preview: mixed.title,
      description_preview: mixed.description,
    });
    expect(delivered?.previous).toMatchObject({
      title_preview: initial.title,
      description_preview: initial.description,
    });
    expect(delivered?.current.status).toBe(delivered?.previous?.status);
    expect(store.getLoopTaskSummary(taskKey(initial.taskId))).toMatchObject({
      title: mixed.title,
      status: "in_review",
    });
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});
