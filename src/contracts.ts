import type { ExecutionEvent, ExecutionSnapshot } from "./execution-card.ts";
import type { ProjectSessionState } from "./project-session.ts";

export interface ChatTarget {
  channelId: string;
  channelType: 1 | 2 | 5;
}

export type InboundAttachmentKind = "image" | "file" | "audio" | "video";

/**
 * One downloadable object carried by an inbound message. `url` is always an absolute
 * http/https URL; relative or otherwise unusable references never produce an entry.
 */
export interface InboundAttachment {
  url: string;
  kind: InboundAttachmentKind;
  /** Client-declared name; empty when the client sent none. */
  name: string;
  /** Client-declared byte size; `0` when unknown. Only used for the pre-download size check. */
  size: number;
  /** Client-declared duration in seconds for audio/video; `0` when unknown. */
  durationSeconds: number;
}

/**
 * Text of the message a reply quotes, taken from the payload the client embedded in
 * `reply`. Quoted media never lives here: it is merged into `InboundMessage.attachments`
 * so the normal download and prompt blocks apply to it.
 */
export interface InboundQuote {
  /** Display name of the quoted message's sender, as the quoting client rendered it. */
  senderName: string;
  text: string;
}

/** One attachment download performed by the transport. */
export interface AttachmentDownload {
  url: string;
  path: string;
  maxBytes: number;
  signal?: AbortSignal;
}

export interface InboundMessage extends ChatTarget {
  messageId: string;
  senderId: string;
  /** Text extractable from this message; empty for media without caption. */
  text: string;
  mentionedUserIds: string[];
  /** Attachments carried by this message, already filtered to absolute http/https URLs. */
  attachments: InboundAttachment[];
  /**
   * Message this one replies to, as the client quoted it; `null` when it is not a reply.
   * The quoted media is merged into `attachments`, so only its text needs a separate field.
   */
  quoted: InboundQuote | null;
  /** True when the message yields usable input: non-empty text or at least one attachment. */
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

/**
 * Capability tier an Octo card frame needs: `octo/v1` carries display elements and local
 * actions, `octo/v2` adds `Action.Submit` and `Input.*`.
 */
export type CardProfile = "octo/v1" | "octo/v2";

/** One Octo interactive card message (raw type 17) with its authoritative plain text. */
export interface CardFrame {
  card: Record<string, unknown>;
  plain: string;
  profile: CardProfile;
}

/** A `card_action` event: one answered click on a card this Bot sent. */
export interface BotCardAction {
  messageId: string;
  channelType: 1 | 2 | 5;
  /** Consumer-side channel id: the peer uid in a DM, the channel itself in groups/threads. */
  channelId: string;
  actionId: string;
  operatorUid: string;
  inputs: Record<string, string>;
}

/**
 * One entry of this Bot's event queue. Event types the channel does not consume stay
 * addressable so they can be acknowledged instead of redelivered forever.
 */
export type BotEvent =
  | { eventId: number; kind: "card_action"; action: BotCardAction }
  | { eventId: number; kind: "other" };

export interface BotEventFetch {
  /** Exclusive cursor: the read returns events with a strictly greater id. */
  afterEventId: number;
  limit: number;
  /** Long-poll hold in seconds; `0` reads once and answers immediately. */
  waitSeconds: number;
  signal?: AbortSignal;
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
  /** Downloads one inbound attachment to `path`; returns the stored byte count. */
  downloadAttachment(request: AttachmentDownload): Promise<number>;
  /** Best-effort Octo server-side transcription of a saved audio file; `null` when unavailable. */
  transcribeAudio(path: string, signal?: AbortSignal): Promise<string | null>;
  getGroupContext(target: ChatTarget): Promise<GroupContext | null>;
  updateGroupContext(
    target: ChatTarget,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }>;
  getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo>;
  /** Group-level 免@ decision as the server computes it; rejects when the call fails. */
  getGroupMentionPref(groupId: string, signal?: AbortSignal): Promise<GroupMentionPref>;
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
  /** Sends one card frame; `null` when the deployment profile cannot carry it. */
  sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null>;
  /** Edits a card this Bot sent; rejects when the frame cannot be carried. */
  editCard(
    target: ChatTarget,
    messageId: string,
    frame: CardFrame,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void>;
  /** Reads this Bot's event queue; `null` when the deployment has no Bot event endpoint. */
  fetchBotEvents(fetch: BotEventFetch): Promise<BotEvent[] | null>;
  /** Acknowledges one processed event so the server stops redelivering it. */
  ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void>;
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

/**
 * Per-group 免@ decision. The server AND-combines the bot owner's intent
 * (`bot_mention_pref.no_mention`) with the group's own `allow_no_mention` switch,
 * and reports the result in `effective`; only `effective` decides whether the bot
 * may answer a group message without an @mention.
 */
export interface GroupMentionPref {
  effective: boolean;
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
  /** Inbound attachment pipeline; `parseConfig` always supplies it (defaults to enabled, 25 MiB). */
  attachments?: AttachmentConfig;
}

export interface AttachmentConfig {
  enabled: boolean;
  /** Per-attachment byte ceiling; also the pre-download check against the client-declared size. */
  maxBytes: number;
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
   * Supervisor restart port, present only when the host detected the process supervisor that
   * runs this channel. `request` asks that supervisor to restart the program, which stops this
   * process.
   */
  restart?: RestartPort;
  /**
   * Isolated thread workspaces, present when the host can materialize git worktrees.
   * `octo_management` reaches it through `create-thread`'s `isolate` argument.
   */
  worktrees?: WorktreePort;
}

export interface RestartPort {
  request(): Promise<void>;
}

export interface ThreadWorkspaceRequest {
  /** Chat directory the workspace is derived from: the calling chat's cwd. */
  baseDir: string;
  /** Thread name, used for the workspace directory and branch. */
  label: string;
  /** Server-assigned thread id, used to keep the workspace name unique. */
  threadId: string;
}

export interface ThreadWorkspace {
  /** Absolute directory the new thread is bound to. */
  cwd: string;
  /** Branch the workspace checks out. */
  branch: string;
}

export interface WorktreePort {
  /** Creates a new git worktree beside the repository that contains `baseDir`. */
  create(request: ThreadWorkspaceRequest, signal?: AbortSignal): Promise<ThreadWorkspace>;
}

export interface ChannelService {
  /** Accepts an inbound message after durable dedupe/command handling; task work is asynchronous. */
  handleInbound(message: InboundMessage): Promise<void>;
  status(target: ChatTarget): Promise<ChannelStatus>;
  start(): Promise<void>;
  stop(): Promise<void>;
}
