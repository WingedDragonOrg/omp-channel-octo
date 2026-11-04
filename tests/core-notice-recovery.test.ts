import { expect, test } from "bun:test";
import type { ChatTarget, WorkerNoticeEvent } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { CoreHarness } from "./fixtures/core-fakes.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

function notice(messageId: string, text: string, level: WorkerNoticeEvent["level"] = "info"): WorkerNoticeEvent {
  return { type: "notice", messageId, text, level };
}

function sameTarget(left: ChatTarget, right: ChatTarget): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}

test("failed notice sends recover into their original chats without rerunning completed tasks", async () => {
  const firstTarget: ChatTarget = { channelType: 1, channelId: "dm-notice-recovery-a" };
  const secondTarget: ChatTarget = { channelType: 1, channelId: "dm-notice-recovery-b" };
  const transport = new FakeTransport();
  const first = await createCoreHarness({
    transport,
    prompt: async ({ text, emit }) => {
      const firstChat = text === "first recovery task";
      emit(notice(
        firstChat ? "recovery-a" : "recovery-b",
        firstChat ? "Recovery notice for chat A" : "Recovery notice for chat B",
        "warning",
      ));
      emit(assistantText(firstChat ? "result-a" : "result-b", "Task finished", true));
    },
  });
  let restarted: CoreHarness | undefined;
  try {
    transport.online = false;
    await first.service.handleInbound(inbound({
      channelId: firstTarget.channelId,
      messageId: "compaction-recovery-task-a",
      text: "first recovery task",
    }));
    await first.service.handleInbound(inbound({
      channelId: secondTarget.channelId,
      messageId: "compaction-recovery-task-b",
      text: "second recovery task",
    }));
    await waitForStatus(first.service, firstTarget, status => status.state === "completed");
    await waitForStatus(first.service, secondTarget, status => status.state === "completed");
    await Promise.all([
      transport.waitForAttempt(attempt => sameTarget(attempt.target, firstTarget)
        && attempt.text.includes("Recovery notice for chat A") && attempt.error !== undefined),
      transport.waitForAttempt(attempt => sameTarget(attempt.target, secondTarget)
        && attempt.text.includes("Recovery notice for chat B") && attempt.error !== undefined),
    ]);

    const store = await openChannelStore(first.stateDir);
    try {
      const taskA = store.getTaskByMessageId(first.bot.uid, firstTarget, "compaction-recovery-task-a");
      const taskB = store.getTaskByMessageId(first.bot.uid, secondTarget, "compaction-recovery-task-b");
      expect(taskA?.state).toBe("completed");
      expect(taskB?.state).toBe("completed");
      expect(store.listPendingOutbox(first.bot.uid, firstTarget)).toContainEqual(expect.objectContaining({
        taskId: taskA?.id,
        channelId: firstTarget.channelId,
        text: expect.stringContaining("Recovery notice for chat A"),
      }));
      expect(store.listPendingOutbox(first.bot.uid, secondTarget)).toContainEqual(expect.objectContaining({
        taskId: taskB?.id,
        channelId: secondTarget.channelId,
        text: expect.stringContaining("Recovery notice for chat B"),
      }));
    } finally {
      store.close();
    }

    await closeCoreHarness(first, { remove: false });
    transport.online = true;
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      prompt: async () => {
        throw new Error("a completed task must not be replayed during outbox recovery");
      },
    });
    await waitForStatus(restarted.service, firstTarget, status => status.state === "completed" && status.outbox === 0);
    await waitForStatus(restarted.service, secondTarget, status => status.state === "completed" && status.outbox === 0);

    const visibleA = transport.visibleText(firstTarget);
    const visibleB = transport.visibleText(secondTarget);
    expect(visibleA).toContain("Recovery notice for chat A");
    expect(visibleA).not.toContain("Recovery notice for chat B");
    expect(visibleB).toContain("Recovery notice for chat B");
    expect(visibleB).not.toContain("Recovery notice for chat A");
    expect(restarted.workers.prompts).toHaveLength(0);
  } finally {
    if (restarted) {
      await closeCoreHarness(restarted);
    } else {
      await closeCoreHarness(first);
    }
  }
});
