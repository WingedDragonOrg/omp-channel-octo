/**
 * 持久层的记录类型：每张表在 TypeScript 侧的形状，被 store 门面与各领域模块共用。
 */

import type { ChatAppendMode, ChatExecutionState, ChatTarget, WorkerProcess } from "../contracts.ts";

export interface BindingRecord extends ChatTarget {
  botUid: string;
  sessionId: string;
  cwd: string;
  sessionFile: string | null;
  /**
   * The cwd was assigned explicitly (an isolated thread workspace) rather than derived, so the
   * default work directory order must keep it even when the binding has no verified session yet.
   */
  cwdPinned?: boolean;
}

export interface InboundClaim {
  botUid: string;
  target: ChatTarget;
  messageId: string;
  senderId: string;
  text: string;
  supported: boolean;
  kind: "task" | "command";
}

export interface TaskRecord extends ChatTarget {
  id: string;
  botUid: string;
  messageId: string;
  senderId: string;
  text: string;
  sessionId: string;
  cwd: string;
  state: ChatExecutionState;
  resultText: string | null;
  errorText: string | null;
  /** Per-channel WuKongIM sequence of the triggering message; `0` means unknown. */
  messageSeq: number;
  /** Whether group history accounting applies to this task and its triggering message. */
  historyEligible: boolean;
  /**
   * The completion relay a dispatch asked for (`thread-dispatch --notify`): when this task reaches
   * a terminal state, one bounded notice goes to the chat that dispatched it. Null means no relay.
   */
  relayChannelType: 2 | 5 | null;
  relayChannelId: string | null;
  /** Thread display name captured at dispatch time for the notice; only set with a relay target. */
  relayThreadName: string | null;
}
export interface RestartNoticeRecord extends ChatTarget {
  id: string;
  botUid: string;
  requestedAt: number;
}

/**
 * 启动恢复的一次规范化结果。`queued` 是按接受顺序待执行的任务：没有派发过的行重放原文，已有
 * OMP 接受回执的行续接原会话。`uncertain` 是本次被 fail-closed 的已派发行——prompt 已落账但收不到
 * 接受回执，无法判定它是否真的进过会话，既不重放也不错续，由服务按实际异常说明结算并关闭其问题。
 */
export interface TaskRestartRecovery {
  queued: TaskRecord[];
  uncertain: TaskRecord[];
}

/**
 * The last progress-card frame the server accepted for a task. Card identity only exists in the
 * process that observed the run, so this row lets the next process submit one terminal edit
 * instead of leaving the client on the last running state.
 */
export interface ProgressCardRecord extends ChatTarget {
  botUid: string;
  taskId: string;
  messageId: string;
  lastSeq: number;
  /** Encoded `ExecutionSnapshot` of the last accepted frame. */
  snapshot: string;
  /** Recovery attempts already spent on this row. */
  attempts: number;
}

export interface OutboxRecord extends ChatTarget {
  id: string;
  botUid: string;
  taskId: string | null;
  kind: "text" | "file";
  text: string | null;
  path: string | null;
  fileKind: "image" | "file" | null;
  clientMsgNo: string;
  state: "pending" | "unknown" | "sent" | "dead";
  attempts: number;
  lastError: string | null;
  remoteMessageId: string | null;
  streamMessageId: string | null;
  segmentIndex: number;
  desiredVersion: number;
  ackedVersion: number;
}

export type OutboxInsert = Omit<
  OutboxRecord,
  "state" | "attempts" | "lastError" | "remoteMessageId" | "streamMessageId" | "segmentIndex" | "desiredVersion" | "ackedVersion"
> & {
  remoteMessageId?: string | null;
  streamMessageId?: string | null;
  segmentIndex?: number;
  desiredVersion?: number;
  ackedVersion?: number;
};

export interface QuestionRecord extends ChatTarget {
  id: string;
  botUid: string;
  taskId: string;
  payload: string;
  state: "pending" | "answered" | "cancelled" | "expired";
  answer: string | null;
  /** Uid of whoever answered (owner or an allowed group member); `null` for old rows and open questions. */
  answeredBy: string | null;
  /** Remote id of the interactive card carrying this question; `null` for text-only questions. */
  cardMessageId: string | null;
  /**
   * 本轮 ask 的收场时间（结果已交付给模型，或等待超时/本地取消），`null` 表示既没交付也没收场——
   * 只可能是被重启中断的那一轮，启动恢复据此只认领这些行。
   */
  settledAt: number | null;
}

export interface WorkerLedgerRecord extends WorkerProcess {
  id: string;
  botUid: string;
  sessionId: string;
  sessionFile: string;
  moveTargetCwd: string | null;
  state: "running" | "stopping" | "stopped" | "unknown";
}

export interface ContextLedgerRecord extends ChatTarget {
  botUid: string;
  sessionId: string;
  version: number;
  phase: "pending" | "committed";
  attemptId: string;
  previousVersion: number | null;
}

export interface HistoryLedgerRecord {
  botUid: string;
  channelType: 2 | 5;
  channelId: string;
  sessionId: string;
  lastSeq: number;
  phase: "pending" | "committed";
  attemptId: string;
  previousSeq: number | null;
}

/**
 * One chat the owner enabled at runtime instead of in the config's `allowedGroupIds`. The two
 * sets are consulted as one allowlist, so a granted chat needs no restart and no config reload.
 */
export interface AllowedChatRecord extends ChatTarget {
  botUid: string;
  channelType: 2 | 5;
  channelId: string;
  grantedBy: string;
  grantedAt: number;
}

