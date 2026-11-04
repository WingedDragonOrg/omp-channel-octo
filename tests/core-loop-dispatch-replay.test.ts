import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

// A repeated native host call must not create another durable execution; a distinct call must.
test("task-aware native dispatch replay executes once while a new call still runs", async () => {
  const loop = new FakeLoopReadPort();
  const workspaceId = "dispatch-workspace";
  const taskId = "dispatch-task";
  const groupId = "group-allowed";
  loop.workspaces.set(workspaceId, { workspaceId, name: "Dispatch workspace", slug: "dispatch-workspace" });
  loop.tasks.set(workspaceId, [{
    taskId, title: "Dispatch replay acceptance", description: null, status: "todo", priority: "medium",
    assigneeType: "member", assigneeId: "fleet-owner-1", assigneeOctoUid: "owner-1",
    projectId: null, parentTaskId: null,
    startDate: null, dueDate: null, createdAt: "2026-11-04T00:00:00Z", updatedAt: "2026-11-04T00:00:00Z",
  }]);
  let store: ChannelStore | undefined;
  let threadChannelId = "";
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      if (context.text.includes("LOOP_NATIVE_DISPATCH_WORK")) {
        await context.append("native-dispatch-effects.log", "effect\n");
        return;
      }
      const management = context.promptContext?.tools?.find(tool => tool.name === "octo_management");
      if (!management) throw new Error("scoped management tool missing");
      const signal = new AbortController().signal;
      const created = JSON.parse(await management.execute({ action: "create-thread", name: "Replay child", task_id: taskId }, signal, "replay-create")) as { short_id: string; channel_id: string };
      threadChannelId = created.channel_id;
      store!.setChatMode(harness.bot.uid, { channelType: 5, channelId: threadChannelId }, "queue");
      const args = { action: "thread-dispatch", short_id: created.short_id, task_id: taskId, text: "LOOP_NATIVE_DISPATCH_WORK" };
      await management.execute(args, signal, "replayed-native-call");
      await management.execute(args, signal, "replayed-native-call");
      await management.execute(args, signal, "distinct-native-call");
    },
  });
  try {
    store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    const parent = { channelType: 2 as const, channelId: groupId };
    await harness.service.handleInbound(inbound({ ...parent, messageId: "native-dispatch-bind", text: `/bind loop ${workspaceId}`, mentionedUserIds: [harness.bot.uid] }));
    await harness.workers.waitForPromptCount(1);
    const parentStatus = await waitForStatus(harness.service, parent, status => status.state === "completed" || status.state === "failed");
    expect(parentStatus.state).toBe("completed");
    const childStatus = await waitForStatus(harness.service, { channelType: 5, channelId: threadChannelId }, status => status.state === "completed" && status.queued === 0);
    const effects = await readFile(join(childStatus.cwd!, "native-dispatch-effects.log"), "utf8");
    expect(effects).toBe("effect\neffect\n");
    expect(harness.workers.prompts.filter(prompt => prompt.text.includes("LOOP_NATIVE_DISPATCH_WORK"))).toHaveLength(2);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});
