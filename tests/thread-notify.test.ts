import { expect, test } from "bun:test";
import type { TaskRecord, ThreadMergeEventRecord } from "../src/store/store-records";
import {
  threadMergeMessageId,
  threadMergeNotice,
  threadNotifyMessageId,
  threadOutcomeNotice,
} from "../src/threads/thread-notify";

function task(overrides: Partial<TaskRecord> = {}): TaskRecord {
  return {
    id: "task-1",
    botUid: "bot-1",
    channelId: "group-a____short-9",
    channelType: 5,
    messageId: "dispatch:1",
    senderId: "owner-1",
    text: "PRIVATE_TASK_SECRET",
    sessionId: "session-1",
    cwd: "/tmp/work",
    state: "completed",
    resultText: "PRIVATE_RESULT_SECRET",
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relaySuppressed: false,
    ...overrides,
  };
}

test("task notices carry stable identity and terminal state but no task or result content", () => {
  const notice = threadOutcomeNotice(task(), "回传线程");
  expect(notice).toContain("short_id short-9");
  expect(notice).toContain("channel_id group-a____short-9");
  expect(notice).toContain("任务 ID：task-1");
  expect(notice).toContain("状态：已完成");
  expect(notice).toContain("说明：派发任务已完成。");
  expect(notice).not.toContain("PRIVATE_TASK_SECRET");
  expect(notice).not.toContain("PRIVATE_RESULT_SECRET");
  expect(threadNotifyMessageId("task-1")).toBe("notify:task-1");
});

test("Loop task outcomes identify their source without calling them dispatched tasks", () => {
  const notice = threadOutcomeNotice(
    task({ messageId: "loop:change-1", state: "failed", errorText: "连接超时" }),
    "回传线程",
  );
  expect(notice).toContain("Loop");
  expect(notice).toContain("连接超时");
  expect(notice).not.toContain("派发任务");
});

test("failed outcomes include the persisted reason; other terminal outcomes keep their state", () => {
  const failed = threadOutcomeNotice(
    task({ state: "failed", resultText: null, errorText: "数据库事务冲突 token=PRIVATE_TOKEN" }),
    "回传线程",
  );
  expect(failed).toContain("状态：失败");
  expect(failed).toContain("数据库事务冲突");
  expect(failed).not.toContain("PRIVATE_TOKEN");
  expect(threadOutcomeNotice(task({ state: "failed", errorText: null }), "回传线程")).toContain("原因不可用");
  expect(
    threadOutcomeNotice(task({ state: "cancelled", resultText: null }), "回传线程"),
  ).toContain("说明：派发任务已取消。");
  expect(
    threadOutcomeNotice(
      task({ state: "interrupted", resultText: null, errorText: "PRIVATE_STOP_REASON" }),
      "回传线程",
    ),
  ).toContain("说明：派发任务已中断。");
});

test("merge summaries expose a stable event identity and actual target disposition", () => {
  const event: ThreadMergeEventRecord = {
    id: "merge-event-1",
    botUid: "bot-1",
    channelId: "group-a____short-9",
    targetBranch: "release/next",
    commit: "0123456789abcdef",
    removed: true,
    archive: "scheduled",
  };
  const notice = threadMergeNotice(event, "回传线程");
  expect(threadMergeMessageId(event.id)).toBe("merge-notify:merge-event-1");
  expect(notice).toContain("目标分支：release/next");
  expect(notice).toContain("目标提交：0123456789abcdef");
  expect(notice).toContain("工作区清理：已排定");
  expect(notice).toContain("归档处置：已排定");
  expect(threadMergeNotice({ ...event, removed: false }, "回传线程")).toContain("工作区清理：保留");
});

test("untrusted task fields do not appear in bounded task summaries", () => {
  const notice = threadOutcomeNotice(
    task({
      text: "LONG_TASK_SECRET".repeat(200),
      resultText: "LONG_RESULT_SECRET".repeat(200),
      errorText: "LONG_ERROR_SECRET".repeat(200),
    }),
    "子区".repeat(200),
  );
  expect(notice).not.toContain("LONG_TASK_SECRET");
  expect(notice).not.toContain("LONG_RESULT_SECRET");
  expect(notice).not.toContain("LONG_ERROR_SECRET");
  expect(notice.length).toBeLessThan(2_000);
});
