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
}

export interface BotIdentity {
  uid: string;
  name: string;
  /** Owner private-chat UID returned by Bot registration, when available. */
  ownerUid?: string;
}

export interface SendReceipt {
  messageId?: string;
}

export interface ChannelTransport {
  initialize(): Promise<BotIdentity>;
  start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void>;
  getGroupContext(target: ChatTarget): Promise<GroupContext | null>;
  updateGroupContext(
    target: ChatTarget,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }>;
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
  refreshState(): Promise<ProjectSessionState>;
  stop(): Promise<boolean>;
}

export interface WorkerFactory {
  sessionRootsFor?(cwd: string): Promise<readonly string[]>;
  create(options: WorkerOptions): Promise<OmpWorker>;
  recover(process: WorkerProcess): Promise<boolean>;
}

export interface CoreServiceConfig {
  ownerId?: string;
  allowedGroupIds: string[];
  stateDir: string;
  /** Injectable home for deterministic tests; production defaults to os.homedir(). */
  homeDir?: string;
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
