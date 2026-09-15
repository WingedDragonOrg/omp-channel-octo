import type { ExecutionEvent, ExecutionSnapshot } from "./execution-card.ts";
import type { ProjectSessionState } from "./project-session.ts";

export interface ChatTarget {
  channelId: string;
  channelType: 1 | 2 | 5;
}

export interface InboundMessage extends ChatTarget {
  messageId: string;
  senderId: string;
  text: string;
  mentionedUserIds: string[];
  supported: boolean;
  /**
   * Per-channel monotonic WuKongIM sequence of this message. `0` means the
   * transport could not decode one, and such messages never participate in
   * group history accounting.
   */
  messageSeq: number;
  /** Server send time in epoch milliseconds; `0` means unknown. */
  messageTimestamp: number;
}

/**
 * Delivery choice for a message that arrives while its chat already runs a task:
 * `steer` injects it into the running OMP turn, `queue` runs it after that turn yields.
 */
export type ChatAppendMode = "steer" | "queue";

export interface BotIdentity {
  uid: string;
  name: string;
  /** Owner private-chat UID returned by Bot registration, when available. */
  ownerUid?: string;
}

export interface SendReceipt {
  messageId?: string;
}

/** One text message of a group/thread channel history window. */
export interface HistoryMessage {
  seq: number;
  senderId: string;
  text: string;
  timestamp: number;
}

/**
 * Server-side channel history page request. `untilSeq === 0` means "the newest
 * `limit` messages"; otherwise the page covers `[sinceSeq, untilSeq)` starting
 * at `sinceSeq` and is truncated by `limit` (WuKongIM `pull_mode=1`).
 */
export interface HistoryFetch {
  sinceSeq: number;
  untilSeq: number;
  limit: number;
  signal?: AbortSignal;
}

export interface ChannelTransport {
  initialize(): Promise<BotIdentity>;
  start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void>;
  /** Group/thread history page; rejects when the server call fails. */
  fetchGroupMessages(target: ChatTarget, fetch: HistoryFetch): Promise<HistoryMessage[]>;
  getGroupContext(target: ChatTarget): Promise<GroupContext | null>;
  updateGroupContext(
    target: ChatTarget,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }>;
  getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo>;
  getGroupMembers(groupId: string, signal?: AbortSignal): Promise<GroupMember[]>;
  updateGroupInfo(
    groupId: string,
    patch: GroupInfoPatch,
    signal?: AbortSignal,
  ): Promise<void>;
  listThreads(groupId: string, signal?: AbortSignal): Promise<ThreadSummary[]>;
  createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary>;
  getThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<ThreadSummary>;
  getThreadMembers(groupId: string, shortId: string, signal?: AbortSignal): Promise<GroupMember[]>;
  joinThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void>;
  leaveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void>;
  deleteThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void>;
  sendFile(
    target: ChatTarget,
    path: string,
    kind: "image" | "file",
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt>;
  sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt>;
  editText(
    target: ChatTarget,
    messageId: string,
    text: string,
    signal?: AbortSignal,
  ): Promise<void>;
  sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void>;
  sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null>;
  editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void>;
  stop(): Promise<void>;
}
export interface GroupContext {
  present: boolean;
  content: string;
  version: number;
  updatedAt: string | null;
  updatedBy: string;
}

export interface GroupInfo {
  groupId: string;
  name: string;
  notice: string;
  creator: string;
  status: number;
  createdAt: string;
}

export interface GroupInfoPatch {
  name?: string;
  notice?: string;
}

export interface GroupMember {
  uid: string;
  name: string;
  role: number;
  robot: boolean;
  joinedAt: string;
}

export interface ThreadSummary {
  shortId: string;
  groupId: string;
  channelId: string;
  name: string;
  creatorUid: string;
  status: number;
  memberCount: number;
  messageCount: number;
  createdAt: string;
  updatedAt: string;
}
 
export type ProgressCard = ExecutionSnapshot;

export interface TextEvent {
  type: "text";
  messageId: string;
  text: string;
  final: boolean;
}

export type WorkerEvent = TextEvent | ExecutionEvent;

export interface WorkerProcess {
  pid: number;
  pgid: number;
  identity: string;
  cwd: string;
}

export interface WorkerContextScope {
  sidecarRoot: string;
  botUid: string;
  target: ChatTarget;
}

export interface WorkerOptions {
  cwd: string;
  sessionDir: string;
  sessionFile?: string;
  contextScope?: WorkerContextScope;
  onSpawn: (process: WorkerProcess) => void;
}

export interface HostTool {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  execute(args: Record<string, unknown>, signal: AbortSignal): Promise<string>;
}

export interface ContextInjection {
  snapshot: GroupContext;
  injectionDecision: "initial" | "version-change" | "unchanged";
  attemptId: string;
  explanation?: string;
}

export interface PromptContext {
  tools?: HostTool[];
  contextInjection?: ContextInjection;
}

export interface OmpWorker {
  readonly sessionFile: string;
  readonly sessionId: string;
  prompt(
    text: string,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<void>;
  /** Injects a message into the running OMP turn; fails once no turn is streaming. */
  steer(text: string): Promise<void>;
  /** Aborts the current OMP turn without clearing its queued messages or killing the worker. */
  abortTurn(): Promise<void>;
  refreshState(): Promise<ProjectSessionState>;
  stop(): Promise<boolean>;
}

export interface WorkerFactory {
  sessionRootsFor?(cwd: string): Promise<readonly string[]>;
  create(options: WorkerOptions): Promise<OmpWorker>;
  recover(process: WorkerProcess): Promise<boolean>;
}

export interface GroupHistoryConfig {
  enabled: boolean;
  /** Upper bound of history entries injected per task turn (1..200). */
  maxMessages: number;
}

export interface CoreServiceConfig {
  ownerId?: string;
  allowedGroupIds: string[];
  stateDir: string;
  /** Injectable home for deterministic tests; production defaults to os.homedir(). */
  homeDir?: string;
  /** `parseConfig` always supplies this (defaults to enabled with `maxMessages = 50`); omitting it disables history. */
  groupHistory?: GroupHistoryConfig;
}

export type ChatExecutionState =
  | "none"
  | "idle"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "interrupted";

export interface ChannelStatus {
  sessionId: string | null;
  cwd: string | null;
  state: ChatExecutionState;
  mode: ChatAppendMode;
  queued: number;
  outbox: number;
  unknownOutbox: number;
  pendingQuestionIds: string[];
}

export interface ChannelCommandContext {
  message: InboundMessage;
  name: string;
  argument: string;
}

export interface ChannelCommandHandler {
  name: string;
  intercept: boolean;
  handle(context: ChannelCommandContext): Promise<void> | void;
}

export interface ChannelServiceOptions {
  config: CoreServiceConfig;
  transport: ChannelTransport;
  workerFactory: WorkerFactory;
  commandHandlers?: readonly ChannelCommandHandler[];
  /**
   * Deployment restart port, present only when a restart command is configured.
   * `request` runs that command and the host then stops this process.
   */
  restart?: RestartPort;
}

export interface RestartPort {
  request(): Promise<void>;
}

export interface ChannelService {
  /** Accepts an inbound message after durable dedupe/command handling; task work is asynchronous. */
  handleInbound(message: InboundMessage): Promise<void>;
  status(target: ChatTarget): Promise<ChannelStatus>;
  start(): Promise<void>;
  stop(): Promise<void>;
}
