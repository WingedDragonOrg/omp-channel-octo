import { Database } from "bun:sqlite";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
} from "./fixtures/core-fakes.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { ChannelStore } from "../src/store/index.ts";
import type { InboundMessage } from "../src/contracts.ts";
import type { LoopTask } from "../src/loop/types.ts";

const WORKSPACE = {
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

function bindMessage(
  botUid: string,
  messageId: string,
  selector = WORKSPACE.workspaceId,
): InboundMessage {
  return inbound({
    channelType: 2,
    channelId: "group-allowed",
    messageId,
    text: `/bind loop ${selector}`,
    mentionedUserIds: [botUid],
  });
}

function loopFixture(tasks: LoopTask[] = []): FakeLoopReadPort {
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(WORKSPACE.workspaceId, WORKSPACE);
  loop.tasks.set(WORKSPACE.workspaceId, tasks);
  return loop;
}
async function waitUntil(
  predicate: () => boolean,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (predicate()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`timed out waiting for ${description}`);
}

// A control command outside a main group must never fall through to a model prompt.
test("Loop binding in a private chat is refused without starting agent work", async () => {
  const harness = await createCoreHarness();
  try {
    const message = inbound({ messageId: "loop-private-bind", text: "/bind loop 8077bebd-3577-486f-b62f-0820f34a855f" });
    await harness.service.submitInbound(message);
    await waitUntil(
      () => harness.transport.sent.some(
        ({ target, text }) => target.channelId === message.channelId && text.includes("主群"),
      ),
      "private-chat Loop refusal",
    );
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(({ target, text }) => target.channelId === message.channelId && text.includes("主群"))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("Loop binding without a configured read port is explicitly refused", async () => {
  const harness = await createCoreHarness();
  try {
    const message = bindMessage(harness.bot.uid, "loop-unconfigured");
    await harness.service.submitInbound(message);
    await waitUntil(
      () => harness.transport.sent.some(
        ({ target, text }) => target.channelId === message.channelId && text.includes("未配置"),
      ),
      "unconfigured Loop refusal",
    );
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(({ text }) => text.includes("未配置"))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});


test("Loop bind reads the complete selector and delivers the baseline as a main-group task", async () => {
  const loop = loopFixture([task("loop-task-a", "First baseline task"), task("loop-task-b", "Second baseline task")]);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "loop-bind-baseline", "Product Tasks"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "Loop baseline worker prompt");
    store = await openChannelStore(harness.stateDir);

    expect(loop.resolutionCalls).toEqual(["Product Tasks"]);
    expect(harness.workers.prompts[0]?.text).toContain("First baseline task");
    expect(harness.workers.prompts[0]?.text).toContain("Second baseline task");
    expect(store.getLoopBinding(harness.bot.uid, "group-allowed")?.workspaceId).toBe(WORKSPACE.workspaceId);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelId === "group-allowed"
      && text.includes("主群 group-allowed")
      && text.includes(WORKSPACE.name)
      && text.includes(WORKSPACE.workspaceId))).toBe(true);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("Repeated bind to the same workspace does not publish a second baseline", async () => {
  const harness = await createCoreHarness({ loop: loopFixture([task("loop-bind-once-task", "Initial task")]) });
  let database: Database | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "loop-bind-once"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "first Loop bind worker prompt");
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "loop-bind-twice"));
    database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });

    const row = database.query(
      "SELECT COUNT(*) AS count FROM loop_inputs WHERE bot_uid = ? AND group_id = ? AND kind = 'baseline'",
    ).get(harness.bot.uid, "group-allowed") as { count: number };
    expect(row.count).toBe(1);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    database?.close();
    await closeCoreHarness(harness);
  }
});

test("Failed replacement read leaves the current Loop workspace and baseline untouched", async () => {
  const loop = loopFixture([task("loop-task-old", "Current workspace task")]);
  const replacement = {
    workspaceId: "loop-workspace-replacement",
    name: "Replacement",
    slug: "replacement",
  };
  loop.workspaces.set(replacement.workspaceId, replacement);
  loop.tasks.set(replacement.workspaceId, [task("loop-task-new", "Replacement task")]);
  const harness = await createCoreHarness({ loop });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "loop-bind-current"));
    await waitUntil(() => harness.workers.prompts.length >= 1, "current Loop bind worker prompt");
    loop.listFailure = new Error("fixture read failed");
    await harness.service.submitInbound(
      bindMessage(harness.bot.uid, "loop-bind-read-failure", replacement.workspaceId),
    );
    store = await openChannelStore(harness.stateDir);

    expect(store.getLoopBinding(harness.bot.uid, "group-allowed")?.workspaceId).toBe(WORKSPACE.workspaceId);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.transport.sent.some(({ text }) => text.includes("绑定失败"))).toBe(true);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});
