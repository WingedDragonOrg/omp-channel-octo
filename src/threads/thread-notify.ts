/**
 * 子区任务回传的纯函数与边界：通知消息 ID、文本模板和两段可变内容的截断上限。
 * 回传登记随任务行持久化（`TaskRecord.relay*` 字段），交付路径留在 `CoreService`。
 */

import type { TaskRecord } from "../store/store-records.ts";
import { threadShortId } from "./thread-target.ts";

/** Synthetic inbound id prefix: one notice per relayed task, deduplicated by the inbound claim. */
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
 * The notice one relayed task delivers to the chat that dispatched it (the task carries its
 * relay target and the thread name captured at dispatch time): bounded, free of tokens and raw
 * tool payloads, and readable as the dispatching assistant's next input.
 */
export function threadOutcomeNotice(task: TaskRecord): string {
  const excerpt = task.text.trim();
  const name = task.relayThreadName?.trim() ?? "";
  return [
    name.length > 0
      ? `[子区任务回传] 「${name}」的一条任务已结束。`
      : "[子区任务回传] 一条子区任务已结束。",
    `子区：short_id ${threadShortId(task.channelId) ?? "-"}；channel_id ${task.channelId}`,
    `任务 ID：${task.id}`,
    `状态：${stateLabel(task.state)}`,
    ...(excerpt.length === 0 ? [] : [`任务：${bounded(excerpt, MAX_TASK_CHARS)}`]),
    outcomeLine(task),
    "这是派发这条任务时要求的回传通知；如需向本群反馈或继续处理，直接回复即可。",
  ].join("\n");
}
