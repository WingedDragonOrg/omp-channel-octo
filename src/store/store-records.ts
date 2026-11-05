/**
 * 持久层的记录类型：每张表在 TypeScript 侧的形状，被 store 门面与各领域模块共用。
 */

import type { ChatAppendMode, ChatExecutionState, ChatTarget, HostMergeOutcome, HostMergeProgressPhase, TextMention, ThreadListenLevel, ThreadSummary, WorkerGoal, WorkerGoalRun, WorkerProcess, WorktreeRemovalStep } from "../contracts.ts";

export interface BindingSessionInitialization {
  taskId: string;
  fromSessionId: string;
  fromCwd: string;
  fromSessionFile: null;
  fromCwdPinned: true;
  fromSessionSwitchedAt: number | null;
}

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
  /** Exact provenance for an initial pinned-thread session update; absent on legacy rows. */
  sessionInitialization?: BindingSessionInitialization | null;
}

/** Latest OMP-native context estimate for one bot/chat, tied to the exact persisted session file. */
export interface SessionContextSnapshotRecord extends ChatTarget {
  botUid: string;
  sessionId: string;
  sessionFile: string;
  cwd: string;
  usedTokens: number | null;
  contextWindow: number | null;
  percent: number | null;
  model: string | null;
  /** Response receipt time, not the session JSONL timestamp. */
  observedAt: string;
  /** JSONL fingerprint taken before the live get_state request. */
  fileSize: number;
  fileMtimeMs: number;
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

export type TanOperationPhase = "pending" | "done" | "failed" | "cancelled";

/**
 * A durably accepted `/tan` request. The source identity is an immutable snapshot: recovery must
 * never substitute a newer binding or materialized body for the operation that won deduplication.
 */
export interface TanOperation {
  /** Full source-scoped operation ID produced by the shared thread-fork identity function. */
  id: string;
  botUid: string;
  sourceChannelType: 2 | 5;
  sourceChannelId: string;
  messageId: string;
  senderId: string;
  text: string;
  sourceSessionId: string;
  sourceSessionFile: string;
  cwd: string;
  phase: TanOperationPhase;
  error: string | null;
}

/**
 * 持久停止意图：运行任务在执行停止前先落库，进程被 SIGKILL 后也不会重启续跑；`cancel` 同时终止排队任务。
 * 恢复结果携带原意图，以便区分取消、用户中断和等待回答超时。
 */
export type TaskStopIntent = "cancel" | "interrupt" | "question_timeout";

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
  /** Completion notices are suppressed after a merge intent begins. */
  relaySuppressed: boolean;
}

/** Persisted, user-visible chat history used to restore PR/MR reference context. */
export interface ReferenceHistoryEntry {
  role: "user" | "assistant";
  text: string;
  taskId: string | null;
}

/** High-water marks captured before asynchronous reference judging begins. */
export interface ReferenceHistorySnapshot extends ChatTarget {
  botUid: string;
  inboundRowId: number;
  outboxRowId: number;
  ingressJobId: number;
  unfinishedTaskIds: readonly string[];
  assistantHistoryOverflow: boolean;
  capturedAt: number;
}

export type ReviewJobState = "accepted" | "running" | "completed" | "failed" | "stopped";

/** A `/review` run is deliberately not a chat task: it owns a fresh session and an independent lane. */
export interface ReviewJobRecord extends ChatTarget {
  id: string;
  botUid: string;
  messageId: string;
  senderId: string;
  focus: string;
  projects: readonly { name: string; path: string }[];
  cwd: string;
  sessionId: string;
  sessionFile: string | null;
  state: ReviewJobState;
  dispatchStarted: boolean;
  promptAccepted: boolean;
  stopRequested: boolean;
  resultText: string | null;
  errorText: string | null;
}

export interface ThreadListenRecord {
  botUid: string;
  channelId: string;
  creatorChannelType: 2 | 5;
  creatorChannelId: string;
  threadName: string;
  listen: ThreadListenLevel;
  mergePending: boolean;
}

export interface ThreadMergeEventRecord {
  id: string;
  botUid: string;
  channelId: string;
  targetBranch: string;
  commit: string | null;
  removed: boolean;
  archive: "scheduled" | "not-scheduled";
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
 * 启动恢复结算的停止行：已按持久停止意图落为 `cancelled` 或 `interrupted` 终态，并携带结算前的意图。
 * 原因随记录交回，供服务按原因分流；只在这条恢复记录上带 `stopIntent`，`TaskRecord` 保持原样。
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

/** Progress-card delivery for a host merge, independent of any task or task-owned progress row. */

export type HostMergeProgressDeliveryState = "ready" | "attempted" | "unknown" | "unavailable" | "bound";
export interface HostMergeProgressCardRecord extends ChatTarget {
  botUid: string;
  sourceId: string;
  clientMsgNo: string;
  messageId: string | null;
  deliveryState: HostMergeProgressDeliveryState;
  lastSeq: number;
  phase: HostMergeProgressPhase;
  startedAt: number;
  endedAt: number | null;
  result: string | null;
  updatedAt: number;
}

export interface HostMergeProgressCardInput {
  botUid: string;
  target: ChatTarget;
  sourceId: string;
  clientMsgNo: string;
  startedAt: number;
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
  sessionFile: string | null;
  moveTargetCwd: string | null;
  state: "running" | "stopping" | "stopped" | "unknown";
}
export interface HostMergeLeaseRecord extends ChatTarget {
  id: string;
  botUid: string;
  channelType: 5;
  sourceId: string;
  cwd: string;
  /** Canonical source and checked-out target paths held by this destructive merge. */
  resourceCwds: readonly string[];
  phase: "starting" | "running" | "exited" | "unknown";
  pid: number | null;
  pgid: number | null;
  processIdentity: string | null;
  commandText: string | null;
  operatorNote: string | null;
  senderId: string | null;
  receiptOutboxId: string | null;
  messageSeq: number | null;
  archiveGeneration: number | null;
  outcome: HostMergeOutcome | null;
  createdAt: number;
  updatedAt: number;
}

export interface HostMergeLeaseInput {
  id: string;
  botUid: string;
  target: ChatTarget;
  sourceId: string;
  cwd: string;
  resourceCwds: readonly string[];
  commandText?: string;
  operatorNote?: string;
  senderId?: string;
  messageSeq?: number;
  receiptOutboxId?: string;
  archiveGeneration?: number | null;
}

export type HostThreadRemovalOutcome =
  | { kind: "complete"; receiptText: string }
  | { kind: "partial"; receiptText: string };

export interface HostRemovalLeaseRecord extends ChatTarget {
  id: string;
  botUid: string;
  channelType: 5;
  sourceId: string;
  cwd: string;
  phase: "starting" | "running" | "exited" | "unknown";
  pid: number | null;
  pgid: number | null;
  processIdentity: string | null;
  step: WorktreeRemovalStep | null;
  resourcePaths: string[];
  commandText: string | null;
  senderId: string | null;
  messageSeq: number | null;
  receiptOutboxId: string;
  outcome: HostThreadRemovalOutcome | null;
  createdAt: number;
  updatedAt: number;
}

export interface HostRemovalLeaseInput {
  id: string;
  botUid: string;
  target: ChatTarget;
  sourceId: string;
  cwd: string;
  resourcePaths: readonly string[];
  commandText: string;
  senderId: string;
  messageSeq: number;
  receiptOutboxId: string;
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

export type ThreadForkPhase =
  | "preparing"
  | "prepared"
  | "creating"
  | "remote-created"
  | "ready"
  | "failed";

export interface ThreadForkRecord {
  id: string;
  botUid: string;
  sourceChannelType: 2 | 5;
  sourceChannelId: string;
  sourceSessionId: string;
  sourceSessionFile: string;
  cwd: string;
  groupId: string;
  name: string;
  preparationDir: string;
  phase: ThreadForkPhase;
  preparationStarted: boolean;
  sessionId: string | null;
  sessionFile: string | null;
  thread: ThreadSummary | null;
  error: string | null;
}

export interface ThreadForkInput extends Omit<
  ThreadForkRecord,
  "phase" | "sessionId" | "sessionFile" | "thread" | "error" | "preparationStarted"
> {
  phase?: ThreadForkPhase;
  sessionId?: string | null;
  sessionFile?: string | null;
  thread?: ThreadSummary | null;
  preparationStarted?: boolean;
  error?: string | null;
}
