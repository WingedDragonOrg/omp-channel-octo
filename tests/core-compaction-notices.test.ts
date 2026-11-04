import { expect, test } from "bun:test";
import type { ChatTarget, WorkerNoticeEvent } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
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

test("manual and automatic compaction notices reach their task chat without becoming task output", async () => {
  const target: ChatTarget = { channelType: 1, channelId: "dm-compaction-lifecycle" };
  const harness = await createCoreHarness({
    prompt: async ({ emit }) => {
      emit(notice("manual-start", "Manual compaction started"));
      emit(notice("manual-start", "Manual compaction started"));
      emit(notice("manual-end", "Manual compaction completed"));
      emit(notice("automatic-start", "Automatic compaction started"));
      emit(notice("automatic-end", "Automatic compaction completed"));
      emit(assistantText("assistant-result", "The task result", true));
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.handleInbound(inbound({
      channelId: target.channelId,
      messageId: "compaction-lifecycle-task",
      text: "run both compaction paths",
    }));
    await waitForStatus(harness.service, target, status => status.state === "completed" && status.outbox === 0);

    const messages = harness.transport.listRemoteText(target);
    const visible = messages.map(message => message.text).join("\n");
    expect(visible).toContain("Manual compaction started");
    expect(visible).toContain("Manual compaction completed");
    expect(visible).toContain("Automatic compaction started");
    expect(visible).toContain("Automatic compaction completed");
    expect(messages.filter(message => message.text.includes("Manual compaction started"))).toHaveLength(1);

    store = await openChannelStore(harness.stateDir);
    expect(store.getTaskByMessageId(harness.bot.uid, target, "compaction-lifecycle-task")).toMatchObject({
      state: "completed",
      resultText: "The task result",
    });
    expect(harness.transport.visibleText({ channelType: 1, channelId: "unrelated-compaction-chat" })).toBe("");
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a slow compaction notice send stays durable while the task completes", async () => {
  const target: ChatTarget = { channelType: 1, channelId: "dm-compaction-slow-send" };
  const sendEntered = Promise.withResolvers<void>();
  const releaseSend = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit }) => {
      emit(notice("slow-manual-start", "Slow manual compaction started"));
      emit(assistantText("slow-task-result", "Work continued", true));
    },
  });
  let store: Awaited<ReturnType<typeof openChannelStore>> | undefined;
  const sendText = harness.transport.sendText.bind(harness.transport);
  harness.transport.sendText = async (sendTarget, text, clientMsgNo, signal) => {
    if (text.includes("Slow manual compaction started")) {
      sendEntered.resolve();
      await releaseSend.promise;
    }
    return sendText(sendTarget, text, clientMsgNo, signal);
  };
  try {
    await harness.service.handleInbound(inbound({
      channelId: target.channelId,
      messageId: "compaction-slow-task",
      text: "continue work despite a slow notice delivery",
    }));
    await sendEntered.promise;
    await waitForStatus(harness.service, target, status => status.state === "completed");

    store = await openChannelStore(harness.stateDir);
    const task = store.getTaskByMessageId(harness.bot.uid, target, "compaction-slow-task");
    expect(task?.state).toBe("completed");
    const pendingNotice = store.listPendingOutbox(harness.bot.uid, target).find(item =>
      item.streamMessageId?.endsWith(":notice:slow-manual-start"),
    );
    expect(pendingNotice).toMatchObject({
      taskId: task?.id,
      channelType: target.channelType,
      channelId: target.channelId,
      text: expect.stringContaining("Slow manual compaction started"),
    });
    if (!pendingNotice) throw new Error("compaction notice was not persisted while delivery was blocked");
    expect(["pending", "unknown"]).toContain(pendingNotice.state);

    releaseSend.resolve();
    await waitForStatus(harness.service, target, status => status.state === "completed" && status.outbox === 0);
    expect(harness.transport.visibleText(target)).toContain("Slow manual compaction started");
  } finally {
    releaseSend.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a cancelled compaction cannot surface a late native success as completed", async () => {
  const target: ChatTarget = { channelType: 1, channelId: "dm-compaction-cancel" };
  const compactionStarted = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit, waitForAbort }) => {
      emit(notice("cancelled-manual-start", "Manual compaction started"));
      compactionStarted.resolve();
      await waitForAbort();
      // Models a late end frame racing the accepted cancel; TaskEvents must ignore it.
      emit(notice("cancelled-manual-end", "Manual compaction completed"));
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.handleInbound(inbound({
      channelId: target.channelId,
      messageId: "compaction-cancel-task",
      text: "cancel manual compaction",
    }));
    await compactionStarted.promise;
    await harness.transport.waitForRemoteText(message =>
      sameTarget(message.target, target) && message.text.includes("Manual compaction started"),
    );

    await harness.service.handleInbound(inbound({
      channelId: target.channelId,
      messageId: "compaction-cancel-command",
      text: "/cancel",
    }));
    await waitForStatus(harness.service, target, status => status.state === "cancelled" && status.outbox === 0);

    store = await openChannelStore(harness.stateDir);
    expect(store.getTaskByMessageId(harness.bot.uid, target, "compaction-cancel-task")).toMatchObject({
      state: "cancelled",
      resultText: null,
    });
    const visible = harness.transport.visibleText(target);
    expect(visible).toContain("Manual compaction started");
    expect(visible).not.toContain("Manual compaction completed");
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("failed notice sends recover into their original chats without rerunning completed tasks", async () => {
  const firstTarget: ChatTarget = { channelType: 1, channelId: "dm-compaction-recovery-a" };
  const secondTarget: ChatTarget = { channelType: 1, channelId: "dm-compaction-recovery-b" };
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
