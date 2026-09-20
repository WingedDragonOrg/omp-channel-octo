import type { ChatTarget } from "./contracts.ts";
import type { TaskRecord } from "./store/store-records.ts";

/** 聊天目标的稳定键：真实 channelType + channelId，不用昵称。 */
export function targetKey(target: ChatTarget): string {
  return `${target.channelType}:${target.channelId}`;
}

/** 文本流的分段投递键：同一条 assistant 消息的所有分段共用一个键。 */
export function streamTargetKey(target: ChatTarget, streamMessageId: string): string {
  return `${targetKey(target)}:${streamMessageId}`;
}

/** 任务所属的聊天目标。 */
export function taskTarget(task: TaskRecord): ChatTarget {
  return { channelId: task.channelId, channelType: task.channelType };
}

/** 群与子区都算群侧目标（私聊 channelType=1）。 */
export function isGroupTarget(target: ChatTarget): boolean {
  return target.channelType !== 1;
}
