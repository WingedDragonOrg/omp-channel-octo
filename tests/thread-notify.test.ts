import { expect, test } from "bun:test";
import type { TaskRecord, ThreadNotifyRecord } from "../src/store-records";
import { threadNotifyMessageId, threadOutcomeNotice } from "../src/thread-notify";

function task(overrides: Partial<TaskRecord> = {}): TaskRecord {
  return {
    id: "task-1",
    botUid: "bot-1",
    channelId: "group-a____short-9",
    channelType: 5,
    messageId: "message-1",
    senderId: "owner-1",
    text: "检查部署状态",
    sessionId: "session-1",
    cwd: "/tmp/work",
    state: "completed",
    resultText: "部署正常",
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    ...overrides,
  };
}

const relay: ThreadNotifyRecord = {
  botUid: "bot-1",
  channelType: 5,
  channelId: "group-a____short-9",
  creatorChannelType: 2,
  creatorChannelId: "group-a",
  threadName: "回传线程",
  createdAt: 1,
};

test("the notice names the thread, the task, the state, and the outcome", () => {
  const notice = threadOutcomeNotice(task(), relay);
  expect(notice).toContain("[子区任务回传] 「回传线程」的一条任务已结束。");
  expect(notice).toContain("short_id short-9");
  expect(notice).toContain("channel_id group-a____short-9");
  expect(notice).toContain("任务 ID：task-1");
  expect(notice).toContain("状态：已完成");
  expect(notice).toContain("任务：检查部署状态");
  expect(notice).toContain("结果：部署正常");
  expect(threadNotifyMessageId("task-1")).toBe("notify:task-1");
  expect(threadNotifyMessageId("task-1")).toContain("task-1");
});

test("failed, cancelled and interrupted outcomes carry their own lines", () => {
  expect(
    threadOutcomeNotice(task({ state: "failed", resultText: null, errorText: "worker failed" }), relay),
  ).toContain("失败原因：worker failed");
  expect(
    threadOutcomeNotice(task({ state: "cancelled", resultText: null }), relay),
  ).toContain("说明：任务已取消（不回滚已发生的副作用）。");
  expect(
    threadOutcomeNotice(
      task({ state: "interrupted", resultText: null, errorText: "用户中断当前回合" }),
      relay,
    ),
  ).toContain("说明：用户中断当前回合");
  expect(threadOutcomeNotice(task({ resultText: null }), relay)).toContain("结果：（无正文）");
  expect(threadOutcomeNotice(task({ text: "  " }), relay)).not.toContain("任务：");
});

test("long task text and outcome text are cut to their caps with an explicit marker", () => {
  const notice = threadOutcomeNotice(
    task({ text: "任".repeat(300), resultText: "果".repeat(2_000) }),
    relay,
  );
  const lines = notice.split("\n");
  const taskLine = lines.find((line) => line.startsWith("任务：")) ?? "";
  const resultLine = lines.find((line) => line.startsWith("结果：")) ?? "";
  expect(taskLine).toBe(`任务：${"任".repeat(200)}…`);
  expect(resultLine).toBe(`结果：${"果".repeat(1_500)}…`);
});
