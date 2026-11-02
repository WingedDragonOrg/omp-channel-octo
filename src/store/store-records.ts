/**
 * 持久层的记录类型：每张表在 TypeScript 侧的形状，被 store 门面与各领域模块共用。
 */

import type { ChatAppendMode, ChatExecutionState, ChatTarget, TextMention, WorkerGoal, WorkerGoalRun, WorkerProcess } from "../contracts.ts";

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
  /**
   * 本聊天最近一次主动换会话（`/new`/`/reset` 成功）的时间；`null`/缺省表示从未主动切换。
   * 问答卡据此判断关闭问题的迟到提交是否属于切换前的旧会话。`/move` 保持会话身份，不写这里。
   */
  sessionSwitchedAt?: number | null;
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

export type CommandClaimState = "pending" | "completed" | "failed";

/**
 * 用户/服务已下达的停止意图：`interrupt` 停止当前回合，`question_timeout` 是等待回答超时导致的停轮。
 * 意图在动手之前先落库，进程被 SIGKILL 时它是「这一回合是被叫停的」唯一证据，重启恢复据此落终态而
 * 不是自动续接。`/cancel` 不需要这一列——在线路径在 ACK 之前就已经把任务写成 cancelled。
 */
export type TaskStopIntent = "interrupt" | "question_timeout";

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

/** Persisted form of an accepted structured goal task; task.text is never its execution payload. */
export type GoalTaskRequest = Extract<WorkerGoalRun, { op: "create" | "resume" }>;

export interface GoalTaskRecord extends ChatTarget {
  botUid: string;
  taskId: string;
  sessionId: string;
  dispatchOp: WorkerGoalRun["op"] | null;
  request: GoalTaskRequest;
  dispatched: boolean;
  accepted: boolean;
  agentAccepted: boolean;
  acceptedGoalId: string | null;
}

export type GoalStopIntent = "pause" | "drop" | "interrupt" | "question_timeout" | "cancel";

export interface GoalSessionRecord extends ChatTarget {
  botUid: string;
  sessionId: string;
  goalId: string | null;
  objective: string;
  status: WorkerGoal["status"] | null;
  tokensUsed: number;
  timeUsedSeconds: number;
  tokenBudget: number | null;
  continuationAuthorized: boolean;
  stopIntent: GoalStopIntent | null;
}

/**
 * 启动恢复结算的停止行：任务记录本身（已是 `interrupted` 终态）再加上结算前的持久意图。结算会清列，
 * 所以原因必须随记录一起交回，供服务按原因分流（interrupt 与 question_timeout 对遗留提问的处理不同，
 * 回传通知也用它说明原因）。只在这条恢复记录上带 `stopIntent`，`TaskRecord` 保持原样。
 */
export interface StoppedTaskRecovery extends TaskRecord {
  stopIntent: TaskStopIntent;
}

export interface GoalStoppedRecovery extends TaskRecord {
  goalStopIntent: GoalStopIntent;
  goalStatus: WorkerGoal["status"] | null;
}

/**
 * 启动恢复的一次规范化结果。`queued` 是按接受顺序待执行的任务：没有派发过的行重放原文，已有
 * OMP 接受回执的行续接原会话。`uncertain` 是本次被 fail-closed 的已派发行——prompt 已落账但收不到
 * 接受回执，无法判定它是否真的进过会话，既不重放也不错续，由服务按实际异常说明结算并关闭其问题。
 * `stopped` 是本次按持久停止意图结算的行。三个数组各自按 accepted_at, rowid 升序；`uncertain` 与
 * `stopped` 只含本次结算的行，后续启动不会重复上报。
 */
export interface TaskRestartRecovery {
  queued: TaskRecord[];
  uncertain: TaskRecord[];
  stopped: StoppedTaskRecovery[];
  goalStopped: GoalStoppedRecovery[];
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

export interface OutboxTextPayload {
  text: string;
  mention: TextMention | null;
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
  mention: TextMention | null;
  streamFinal: boolean;
  createPayload: OutboxTextPayload | null;
}

export interface TextStreamOutboxSegment {
  text: string;
  mention: TextMention | null;
}

export interface TextStreamOutboxCommit {
  botUid: string;
  target: ChatTarget;
  taskId: string;
  streamMessageId: string;
  segments: readonly TextStreamOutboxSegment[];
  final: boolean;
  forceFinalSentinel: boolean;
}

export type OutboxInsert = Omit<
  OutboxRecord,
  | "state"
  | "attempts"
  | "lastError"
  | "remoteMessageId"
  | "streamMessageId"
  | "segmentIndex"
  | "desiredVersion"
  | "ackedVersion"
  | "mention"
  | "streamFinal"
  | "createPayload"
> & {
  remoteMessageId?: string | null;
  streamMessageId?: string | null;
  segmentIndex?: number;
  desiredVersion?: number;
  ackedVersion?: number;
  mention?: TextMention | null;
  streamFinal?: boolean;
  createPayload?: OutboxTextPayload | null;
};

/** 关闭原因：`cancelled`＝用户 `/cancel`，`interrupted`＝用户 `/interrupt`，`ended`＝生命周期收尾。 */
export type QuestionCloseReason = "cancelled" | "interrupted" | "ended";

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
  /** 建立时间：按时序判断提问是否属于某次主动会话切换之前的旧会话。 */
  createdAt: number;
  /**
   * 行被关闭时记下的原因（`cancelled`/`interrupted`/`ended`）；`null` 只出现在开启迟到提交分流
   * 之前落库的旧行，读取方按 `ended` 处理。
   */
  closedReason: QuestionCloseReason | null;
  /**
   * 本轮 ask 的收场时间（结果已交付给模型，或等待超时/本地取消），`null` 表示既没交付也没收场——
   * 只可能是被重启中断的那一轮，启动恢复据此只认领这些行。
   */
  settledAt: number | null;
}

/** 新建提问行的输入：状态、答案、卡片绑定与各时间戳都由存储层补。 */
export type QuestionInsert = Omit<
  QuestionRecord,
  "state" | "answer" | "answeredBy" | "cardMessageId" | "createdAt" | "closedReason" | "settledAt"
>;

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

/** A durable owner click; action and Bot event identities are both unique per Bot. */
export interface ChatGrantOperationInput extends ChatTarget {
  actionId: string;
  eventId: number;
  botUid: string;
  ownerId: string;
  actorId: string;
  channelType: 2 | 5;
  channelId: string;
  cardMessageId: string;
  /** Stable client message ID used if the card edit cannot deliver the receipt. */
  resultMessageId: string;
}

export interface ChatGrantOperationRecord extends ChatGrantOperationInput {
  acceptedAt: number;
  /** Last sequence used on the confirmation card; a new grant card starts at 1. */
  cardEditSeq: number;
  configMirrorState: "pending" | "succeeded" | "failed";
  configMirrorError: string | null;
  resultState: "pending" | "delivered";
}

export type ThreadWorkspaceLayout = "single" | "shared" | "separate";
export type ThreadWorkspaceStatus = "preparing" | "ready" | "failed";

export interface ThreadWorkspaceProjectRecord {
  name: string;
  repo: string;
  base?: string;
  path: string | null;
  branch: string | null;
  created: boolean;
}

export interface ThreadWorkspaceRecord extends ChatTarget {
  botUid: string;
  channelType: 5;
  channelId: string;
  layout: ThreadWorkspaceLayout;
  /** Present only for the legacy `worktree_name` API, which retains parent-cwd fallback. */
  legacyWorktreeName?: string;
  status: ThreadWorkspaceStatus;
  entryPath: string | null;
  projects: ThreadWorkspaceProjectRecord[];
  error: string | null;
}

export type ThreadWorkspaceIntentStatus = "preparing" | "failed" | "complete";

export interface ThreadWorkspaceIntentRecord {
  id: string;
  botUid: string;
  groupId: string;
  status: ThreadWorkspaceIntentStatus;
  channelIds: string[];
  error: string | null;
}

export type ThreadWorkspaceIntentInput =
  Omit<ThreadWorkspaceIntentRecord, "error" | "status"> & { status: "preparing"; error?: string | null };
