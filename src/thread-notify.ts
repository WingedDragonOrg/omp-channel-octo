/**
 * 子区任务完成回传的纯函数与边界：通知消息 ID、文本模板和两段可变内容的截断上限。
 * 交付路径（合成入站消息、追加模式、去重、失败处理）留在 `CoreService`。
 */

import type { TaskRecord, ThreadNotifyRecord } from "./store-records.ts";
import { threadShortId } from "./thread-target.ts";

/** Synthetic inbound id prefix: one notice per thread task, deduplicated by the inbound claim. */
export const THREAD_NOTIFY_MESSAGE_PREFIX = "notify:";

export function threadNotifyMessageId(threadTaskId: string): string {
  return `${THREAD_NOTIFY_MESSAGE_PREFIX}${threadTaskId}`;
}

/** Caps of the two variable blocks; the whole notice stays well under three kilobytes. */
const MAX_TASK_CHARS = 200;
const MAX_OUTCOME_CHARS = 1_500;

/** Cuts one block to its cap, marking the cut explicitly instead of dropping it silently. */
function bounded(value: string, maxChars: number): string {
  return value.length > maxChars ? `${value.slice(0, maxChars)}…` : value;
}

function stateLabel(state: TaskRecord["state"]): string {
  switch (state) {
    case "completed":
      return "已完成";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
    case "interrupted":
      return "已中断";
    default:
      return state;
  }
}

/** The outcome line of a terminal task; the caller only relays terminal states. */
function outcomeLine(task: TaskRecord): string {
  switch (task.state) {
    case "completed": {
      const result = (task.resultText ?? "").trim();
      return `结果：${result.length === 0 ? "（无正文）" : bounded(result, MAX_OUTCOME_CHARS)}`;
    }
    case "failed": {
      const reason = (task.errorText ?? "").trim();
      return `失败原因：${reason.length === 0 ? "（未记录原因）" : bounded(reason, MAX_OUTCOME_CHARS)}`;
    }
    case "cancelled":
      return "说明：任务已取消（不回滚已发生的副作用）。";
    case "interrupted": {
      const reason = (task.errorText ?? "").trim();
      return `说明：${reason.length === 0 ? "任务已中断。" : bounded(reason, MAX_OUTCOME_CHARS)}`;
    }
    default:
      return "说明：任务已结束。";
  }
}

/**
 * The notice one finished thread task delivers to the chat that created the thread: bounded,
 * free of tokens and raw tool payloads, and readable as the creator assistant's next input.
 */
export function threadOutcomeNotice(task: TaskRecord, registration: ThreadNotifyRecord): string {
  const excerpt = task.text.trim();
  return [
    `[子区任务回传] 「${registration.threadName}」的一条任务已结束。`,
    `子区：short_id ${threadShortId(task.channelId) ?? "-"}；channel_id ${task.channelId}`,
    `任务 ID：${task.id}`,
    `状态：${stateLabel(task.state)}`,
    ...(excerpt.length === 0 ? [] : [`任务：${bounded(excerpt, MAX_TASK_CHARS)}`]),
    outcomeLine(task),
    "这是系统自动回传的子区任务通知；如需向本群反馈或继续处理，直接回复即可。",
  ].join("\n");
}
