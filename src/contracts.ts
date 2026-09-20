import type { ExecutionEvent, ExecutionSnapshot } from "./execution-tracker.ts";
import type { ProjectSessionState } from "./project-session.ts";

export interface ChatTarget {
  channelId: string;
  channelType: 1 | 2 | 5;
}

/**
 * The completion relay one dispatched task carries (`thread-dispatch --notify`): when the task
 * reaches a terminal state, one bounded notice is delivered to this chat — the chat that
 * dispatched it — following that chat's append mode.
 */
export interface TaskRelayTarget extends ChatTarget {
  channelType: 2 | 5;
  /** Thread display name captured at dispatch time; rendered in the notice. */
  threadName: string;
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

/** Attachment descriptor of one history message; `url` is always an absolute http(s) link. */
export interface HistoryMessageAttachment {
  kind: InboundAttachmentKind;
  name: string;
  size: number;
  url: string;
}

/**
 * One message of a group/thread channel history window. `attachments` carries the absolute
 * download references of media-bearing messages (added 2026-09-20); absent means none/unknown.
 * An attachment-bearing message with empty text is a full history entry in its own right.
 */
export interface HistoryMessage {
  seq: number;
  senderId: string;
  text: string;
  timestamp: number;
  attachments?: HistoryMessageAttachment[];
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

/** Retry knob for `deleteThread`; see the method's doc comment. */
export interface DeleteThreadOptions {
  freshConnection?: boolean;
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
  /** Display identity of one user by uid; rejects when the server cannot resolve it. */
  getUserInfo(uid: string, signal?: AbortSignal): Promise<UserInfo>;
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
  /**
   * Deletes one thread. `freshConnection` is the retry knob: it makes the call spend a
   * connection of its own instead of a pooled one, so a retry after a transient server
   * failure cannot run on the connection the failed attempt used.
   */
  deleteThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
    options?: DeleteThreadOptions,
  ): Promise<void>;
  /**
   * Archives one thread. The server call is idempotent — archiving an already-archived thread
   * answers success — and any message sent into the thread afterwards reactivates it, so callers
   * archive only after the chat's own output has been delivered.
   */
  archiveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void>;
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

/** One user as `GET /v1/bot/user/info` reports it; `name` is empty when the account has none. */
export interface UserInfo {
  uid: string;
  name: string;
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

/**
 * A user-attributed steering message left OMP's steering queue and joined the running turn's
 * context, so the next provider call carries it. The queue is FIFO, so consumption follows the
 * order the messages were delivered in.
 */
export interface SteerConsumedEvent {
  type: "steer_consumed";
  text: string;
  atMs: number;
}

/**
 * The run completed without generation: OMP answered a prompt locally (the prompt ack or a
 * `prompt_result` carries `agentInvoked: false`), so no assistant turn ever existed. Emitted
 * once, immediately before the run settles; a completion replay after settlement is ignored.
 */
export interface LocalCommandEvent {
  type: "local_command";
}

/**
 * One upstream `notify` UI notification surfaced during a run (e.g. the /autoresearch
 * extension's local-only updates). `messageId` preserves the upstream frame id as validated
 * (nonempty, control-free, at most 200 UTF-16 code units); `text` is the sanitized,
 * bounded message. `level` follows the upstream severity, unknown or missing levels
 * falling back to `info`.
 */
export interface WorkerNoticeEvent {
  type: "notice";
  messageId: string;
  text: string;
  level: "info" | "warning" | "error";
}

export type WorkerEvent = TextEvent | ExecutionEvent | SteerConsumedEvent | WorkerNoticeEvent | LocalCommandEvent;

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
  /**
   * Canonical `provider/id` the worker starts with (`--model`). It wins over the model recorded in
   * a resumed session, so a chat's switch applies from the next started turn on.
   */
  model?: string;
  contextScope?: WorkerContextScope;
  onSpawn: (process: WorkerProcess) => void;
}

/** One model OMP can use right now; `provider`/`id` are the canonical selector parts. */
export interface WorkerModelInfo {
  provider: string;
  id: string;
  name?: string;
}

/** What a single probe of the worker reports for the `/switch` command. */
export interface WorkerModelCatalog {
  /** The live session's model as `provider/id`; null when OMP reports none. */
  current: string | null;
  /** The session's thinking level (`off`…`max`); null when OMP reports none. */
  level: string | null;
  available: WorkerModelInfo[];
}

/**
 * Outcome of handing a selector to OMP's own `/switch`: either the resolved model and thinking
 * level the session now runs, or a selector OMP rejected (session unchanged).
 */
export type WorkerModelSwitch =
  | { ok: true; model: string; level: string | null }
  | { ok: false };

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
  /** Reads the live session's model and the models OMP currently offers, without prompting. */
  models(): Promise<WorkerModelCatalog>;
  /**
   * Hands a model selector to OMP's own `/switch`, which resolves it against the live catalog
   * (`provider/id`, model id or substring, `@role`, an optional `:level` suffix) and switches
   * this session. Works mid-turn: the dispatch is not an agent prompt.
   */
  switchModel(selector: string): Promise<WorkerModelSwitch>;
  stop(): Promise<boolean>;
}

export interface WorkerFactory {
  sessionRootsFor?(cwd: string): Promise<readonly string[]>;
  create(options: WorkerOptions): Promise<OmpWorker>;
  recover(process: WorkerProcess): Promise<boolean>;
}

/**
 * Why a task could not start. A bounded vocabulary: values reach service logs, the owner-only task
 * row and the chat notice, so they never carry paths, payloads or credentials.
 */
export type TaskStartupCode =
  /** The bound cwd (including an isolated workspace) is gone. */
  | "cwd-missing"
  /** The state directory's sessions folder is missing or unusable. */
  | "session-dir-missing"
  /** The bound session file is gone, or the resumed session state failed validation. */
  | "session-invalid"
  /** The child process could not be spawned. */
  | "spawn-failed"
  /** The spawned child's process identity could not be verified. */
  | "identity-unverified"
  /** The parent-death watchdog did not become ready. */
  | "watchdog-unavailable"
  /** No `ready` frame arrived within the worker startup budget. */
  | "not-ready"
  /** The child exited, or its output ended, before the worker was usable. */
  | "exited"
  /** Protocol negotiation, session creation or state handshake failed. */
  | "handshake-failed";

/**
 * Typed startup failure thrown by the worker factory and by the host's binding checks. The message
 * keeps the human wording; `code` is what the channel classifies, logs and records.
 */
export class TaskStartupError extends Error {
  readonly code: TaskStartupCode;

  constructor(code: TaskStartupCode, message: string) {
    super(message);
    this.name = "TaskStartupError";
    this.code = code;
  }
}

/** Tags `error` with `code`, keeping an existing classification untouched. */
export function taskStartupError(code: TaskStartupCode, error: unknown): TaskStartupError {
  if (error instanceof TaskStartupError) return error;
  const message = error instanceof Error && error.message.trim().length > 0
    ? error.message
    : "task startup failed";
  return new TaskStartupError(code, message);
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
  /**
   * Root of the per-project OMP session directories. `parseConfig` always supplies it
   * (defaults to `~/.oco/sessions`); a config that omits it falls back to
   * `<stateDir>/sessions` for the pre-isolation layout.
   */
  sessionsDir?: string;
  /** Injectable home for deterministic tests; production defaults to os.homedir(). */
  homeDir?: string;
  /** `parseConfig` always supplies this (defaults to enabled with `maxMessages = 50`); omitting it disables history. */
  groupHistory?: GroupHistoryConfig;
  /** Inbound attachment pipeline; `parseConfig` always supplies it (defaults to enabled, 25 MiB). */
  attachments?: AttachmentConfig;
  /** `ask` wait behavior; omitting it uses `DEFAULT_ASK_TIMEOUT_MS`. */
  ask?: AskConfig;
}

export interface AttachmentConfig {
  enabled: boolean;
  /** Per-attachment byte ceiling; also the pre-download check against the client-declared size. */
  maxBytes: number;
}

/**
 * `ask` wait behavior. A question that waits past `timeoutMs` does not expire: the rows stay
 * pending, the cards stay submittable, and the turn waiting on them is stopped like a cancelled
 * tool call until the owner's answer arrives as a new turn input.
 */
export interface AskConfig {
  /** How long an `ask` waits for its answer. */
  timeoutMs: number;
}

/** Default `ask` wait (6 hours, the confirmed deployment default). */
export const DEFAULT_ASK_TIMEOUT_MS = 6 * 60 * 60 * 1_000;

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
  /** The chat's `provider/id` model, or null when it follows OMP's default. */
  model: string | null;
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
  /**
   * Per-bot octo-cli credential. `provision` runs once the bot identity is known, so OMP tasks
   * can call `octo-cli` as this bot; failures leave the agent without a CLI identity.
   */
  cliCredentials?: CliCredentialPort;
  /**
   * Operator-visible allowlist baseline. A chat the owner enables at runtime is durable in the
   * state store on its own; this port is what keeps the config file's `allowedGroupIds` in step,
   * and a failure there only costs that mirror.
   */
  allowlistWriteback?: AllowlistWriteback;
}

export interface AllowlistWriteback {
  /** Appends one chat id to the config file's allowlist; idempotent, atomic, mode-preserving. */
  append(groupId: string): Promise<void>;
}

export interface CliCredentialPort {
  provision(botUid: string): Promise<void>;
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

/** Result of probing the local Worktrunk CLI; only stable v0.76.x counts as available. */
export type WorktrunkProbe =
  | { available: true; command: string; version: string }
  | { available: false; reason: "missing" }
  | { available: false; reason: "incompatible"; command: string; version: string };

/** One `wt merge` invocation; every optional field maps to a wt flag, wt's defaults apply otherwise. */
export interface WorktreeMergeRequest {
  /** The worktree to merge; `wt merge` merges the branch checked out here into the target. */
  cwd: string;
  /** Target branch; wt defaults to the repository's default branch. */
  target?: string;
  squash?: boolean;
  commit?: boolean;
  rebase?: boolean;
  /** Whether wt removes the worktree (and a merged branch) after a successful merge. */
  remove?: boolean;
  ff?: boolean;
  stage?: "all" | "tracked" | "none";
}

export interface WorktreeMergeSummary {
  branch: string;
  target: string;
  squashed: boolean;
  rebased: boolean;
  committed: boolean;
  removed: boolean;
}

export interface WorktreeRemovalRequest {
  /** Worktree directory to remove. */
  path: string;
  /** Discard uncommitted changes instead of refusing to remove a dirty worktree. */
  force?: boolean;
}

export type WorktreeRemovalOutcome =
  | { kind: "missing" }
  | { kind: "removed"; via: "wt" | "git"; branchOutcome: "deleted" | "kept" | null };

export interface WorktreePort {
  /** Creates a new git worktree beside the repository that contains `baseDir`. */
  create(request: ThreadWorkspaceRequest, signal?: AbortSignal): Promise<ThreadWorkspace>;
  /**
   * Merges the workspace's branch into the target branch through the Worktrunk pipeline.
   * There is no fallback: without a compatible wt this rejects with a bounded reason.
   */
  merge(request: WorktreeMergeRequest, signal?: AbortSignal): Promise<WorktreeMergeSummary>;
  /**
   * Removes one workspace (the `thread-delete` linkage): through wt when available, otherwise
   * through plain git. A workspace that is already gone reports `missing`.
   */
  remove(request: WorktreeRemovalRequest, signal?: AbortSignal): Promise<WorktreeRemovalOutcome>;
  /** Probes the local Worktrunk; the result drives creation, merging and the command prompts. */
  worktrunk(signal?: AbortSignal): Promise<WorktrunkProbe>;
}

export interface ChannelService {
  /** Accepts an inbound message after durable dedupe/command handling; task work is asynchronous. */
  handleInbound(message: InboundMessage): Promise<void>;
  status(target: ChatTarget): Promise<ChannelStatus>;
  start(): Promise<void>;
  stop(): Promise<void>;
}
