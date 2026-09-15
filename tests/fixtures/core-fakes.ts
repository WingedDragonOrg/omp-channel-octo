import { readFileSync } from "node:fs";
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import type {
  BotIdentity,
  ChannelService,
  ChannelStatus,
  ChannelTransport,
  ChatTarget,
  CoreServiceConfig,
  GroupContext,
  InboundMessage,
  OmpWorker,
  ProgressCard,
  PromptContext,
  RestartPort,
  SendReceipt,
  WorkerEvent,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../../src/contracts";
import {
  contextSidecarPath,
  normalizeGroupContext,
  writePreparedReceipt,
} from "../../src/context-sidecar";
import type { ProjectSessionState } from "../../src/project-session";
import { createChannelService } from "../../src/service";

type GroupContextInput = Omit<GroupContext, "present"> & Partial<Pick<GroupContext, "present">>;

export interface SentMessage {
  target: ChatTarget;
  text: string;
  clientMsgNo: string;
}

export interface SentFile {
  target: ChatTarget;
  path: string;
  kind: "image" | "file";
  clientMsgNo: string;
}

export interface TransportAttempt extends SentMessage {
  error?: string;
}

export interface RemoteTextMessage extends SentMessage {
  messageId: string;
}

export interface TextRequest {
  operation: "send" | "edit";
  target: ChatTarget;
  messageId: string | null;
  clientMsgNo: string | null;
  text: string;
  startedAt: number;
  finishedAt?: number;
  error?: string;
}

export function assistantText(
  messageId: string,
  text: string,
  final: boolean,
): WorkerEvent {
  return { type: "text", messageId, text, final } as unknown as WorkerEvent;
}
interface RemoteTextWaiter {
  predicate: (message: RemoteTextMessage) => boolean;
  resolve: (message: RemoteTextMessage) => void;
}

interface TextRequestWaiter {
  predicate: (request: TextRequest) => boolean;
  resolve: (request: TextRequest) => void;
}
export interface FakeTransportOptions {
  bot?: BotIdentity;
  online?: boolean;
  startOnline?: boolean;
}

export class FakeTransport implements ChannelTransport {
  readonly attempts: TransportAttempt[] = [];
  readonly sent: SentMessage[] = [];
  readonly fileAttempts: Array<SentFile & { error?: string }> = [];
  readonly sentFiles: SentFile[] = [];
  readonly textRequests: TextRequest[] = [];
  readonly remoteTextMessages = new Map<string, RemoteTextMessage>();
  readonly groupContextCalls: ChatTarget[] = [];
  readonly bot: BotIdentity;
  online: boolean;
  maxConcurrentTextRequests = 0;

  private readonly startOnline: boolean;
  private readonly contexts = new Map<string, GroupContext | null>();
  private readonly attemptWaiters: Array<{
    predicate: (attempt: TransportAttempt) => boolean;
    resolve: (attempt: TransportAttempt) => void;
  }> = [];
  private readonly textRequestWaiters: TextRequestWaiter[] = [];
  private readonly remoteTextWaiters: RemoteTextWaiter[] = [];
  private readonly editGates: Array<Promise<void>> = [];
  private readonly remoteTextByClientMsgNo = new Map<string, string>();
  private activeTextRequests = 0;
  private nextRemoteTextId = 1;
  private onInbound: ((message: InboundMessage) => Promise<void>) | undefined;
  private onOnline: (() => void) | undefined;
  private failOncePredicate: ((message: SentMessage) => boolean) | undefined;
  private failEditOncePredicate: ((request: TextRequest) => boolean) | undefined;
  private groupContextFailure: Error | undefined;
  constructor(options: FakeTransportOptions = {}) {
    this.bot = options.bot ?? { uid: "bot-fixture", name: "Fixture Bot" };
    this.online = options.online ?? true;
    this.startOnline = options.startOnline ?? true;
  }

  initialize(): Promise<BotIdentity> {
    return Promise.resolve(this.bot);
  }

  async start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void> {
    this.onInbound = onMessage;
    this.onOnline = onOnline;
    if (this.startOnline && this.online) {
      onOnline();
    }
  }

  async getGroupContext(target: ChatTarget): Promise<GroupContext | null> {
    this.groupContextCalls.push(target);
    if (this.groupContextFailure) {
      throw this.groupContextFailure;
    }
    if (target.channelType === 1) return null;
    return this.contexts.get(`${target.channelType}:${target.channelId}`) ?? normalizeGroupContext(null);
  }

  async updateGroupContext(
    target: ChatTarget,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }> {
    if (signal?.aborted) {
      throw new Error("fixture update aborted");
    }
    const key = `${target.channelType}:${target.channelId}`;
    const previous = this.contexts.get(key);
    const version = (previous?.version ?? 0) + 1;
    this.contexts.set(key, {
      present: content.length > 0,
      content,
      version,
      updatedAt: new Date().toISOString(),
      updatedBy: this.bot.uid,
    });
    return { version };
  }

  setGroupContext(target: ChatTarget, context: GroupContextInput | null): void {
    this.contexts.set(
      `${target.channelType}:${target.channelId}`,
      context === null ? null : normalizeGroupContext(context),
    );
  }

  failGroupContextWith(error: Error): void {
    this.groupContextFailure = error;
  }

  async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const request: TextRequest = {
      operation: "send",
      target: { ...target },
      messageId: null,
      clientMsgNo,
      text,
      startedAt: Date.now(),
    };
    this.beginTextRequest(request);
    const message: SentMessage = { target: { ...target }, text, clientMsgNo };
    try {
      if (signal?.aborted) throw new Error("fixture text aborted");
      if (Buffer.byteLength(text, "utf8") > 3_000) {
        throw new Error("fixture text exceeds 3000 UTF-8 bytes");
      }
      if (!this.online || this.failOncePredicate?.(message)) {
        throw new Error("fixture transport unavailable");
      }
      const existingId = this.remoteTextByClientMsgNo.get(clientMsgNo);
      const messageId = existingId ?? `text-${this.nextRemoteTextId++}`;
      const existingRemote = existingId ? this.remoteTextMessages.get(existingId) : undefined;
      const remote = existingRemote ?? { ...message, messageId };
      if (!existingRemote) {
        this.remoteTextMessages.set(messageId, remote);
        this.remoteTextByClientMsgNo.set(clientMsgNo, messageId);
      }
      request.messageId = messageId;
      this.recordAttempt(message);
      this.sent.push(message);
      if (!existingRemote) this.notifyRemoteText(remote);
      return { messageId };
    } catch (error) {
      const errorText = error instanceof Error ? error.message : "fixture transport unavailable";
      request.error = errorText;
      this.recordAttempt({ ...message, error: errorText });
      throw error instanceof Error ? error : new Error(errorText);
    } finally {
      this.finishTextRequest(request);
    }
  }

  async editText(
    target: ChatTarget,
    messageId: string,
    text: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const request: TextRequest = {
      operation: "edit",
      target: { ...target },
      messageId,
      clientMsgNo: null,
      text,
      startedAt: Date.now(),
    };
    this.beginTextRequest(request);
    try {
      if (signal?.aborted) throw new Error("fixture text edit aborted");
      await this.waitForEditGate(signal);
      if (signal?.aborted) throw new Error("fixture text edit aborted");
      if (Buffer.byteLength(text, "utf8") > 3_000) {
        throw new Error("fixture text exceeds 3000 UTF-8 bytes");
      }
      if (!this.online) throw new Error("fixture transport unavailable");
      if (this.failEditOncePredicate?.(request)) {
        throw new Error("fixture text edit unavailable");
      }
      const remote = this.remoteTextMessages.get(messageId);
      if (!remote) throw new Error("fixture remote text message not found");
      remote.text = text;
      this.notifyRemoteText(remote);
    } catch (error) {
      const errorText = error instanceof Error ? error.message : "fixture text edit unavailable";
      request.error = errorText;
      throw error instanceof Error ? error : new Error(errorText);
    } finally {
      this.finishTextRequest(request);
    }
  }
  async sendTyping(_target: ChatTarget, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new Error("fixture typing aborted");
  }

  async sendProgressCard(
    _target: ChatTarget,
    _progress: ProgressCard,
    _clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    if (signal?.aborted) throw new Error("fixture progress card aborted");
    return null;
  }

  async editProgressCard(
    _target: ChatTarget,
    _messageId: string,
    _progress: ProgressCard,
    _cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw new Error("fixture progress card edit aborted");
  }

  async sendFile(
    target: ChatTarget,
    path: string,
    kind: "image" | "file",
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const file: SentFile = { target, path, kind, clientMsgNo };
    if (signal?.aborted || !this.online) {
      this.fileAttempts.push({ ...file, error: "fixture transport unavailable" });
      throw new Error("fixture transport unavailable");
    }
    this.fileAttempts.push(file);
    this.sentFiles.push(file);
    return { messageId: clientMsgNo };
  }

  async stop(): Promise<void> {
    this.onInbound = undefined;
    this.onOnline = undefined;
  }

  private recordAttempt(attempt: TransportAttempt): void {
    this.attempts.push(attempt);
    for (const waiter of this.attemptWaiters) {
      if (waiter.predicate(attempt)) waiter.resolve(attempt);
    }
  }

  waitForAttempt(predicate: (attempt: TransportAttempt) => boolean): Promise<TransportAttempt> {
    const existing = this.attempts.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise<TransportAttempt>((resolve) => {
      this.attemptWaiters.push({ predicate, resolve });
    });
  }

  failOnceWhen(predicate: (message: SentMessage) => boolean): void {
    let pending = true;
    this.failOncePredicate = (message) => {
      if (pending && predicate(message)) {
        pending = false;
        return true;
      }
      return false;
    };
  }

  failEditOnceWhen(predicate: (request: TextRequest) => boolean): void {
    let pending = true;
    this.failEditOncePredicate = (request) => {
      if (pending && predicate(request)) {
        pending = false;
        return true;
      }
      return false;
    };
  }

  holdNextEdit(): () => void {
    let release!: () => void;
    this.editGates.push(
      new Promise<void>((resolve) => {
        release = resolve;
      }),
    );
    return release;
  }

  waitForTextRequest(predicate: (request: TextRequest) => boolean): Promise<TextRequest> {
    const existing = this.textRequests.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise<TextRequest>((resolve) => {
      this.textRequestWaiters.push({ predicate, resolve });
    });
  }

  listRemoteText(target: ChatTarget): RemoteTextMessage[] {
    return [...this.remoteTextMessages.values()].filter(
      (message) =>
        message.target.channelType === target.channelType && message.target.channelId === target.channelId,
    );
  }

  waitForRemoteText(predicate: (message: RemoteTextMessage) => boolean): Promise<RemoteTextMessage> {
    const existing = [...this.remoteTextMessages.values()].find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise<RemoteTextMessage>((resolve) => {
      this.remoteTextWaiters.push({ predicate, resolve });
    });
  }

  visibleText(target: ChatTarget): string {
    return this.listRemoteText(target)
      .map(({ text }) => text)
      .join("");
  }

  private beginTextRequest(request: TextRequest): void {
    this.textRequests.push(request);
    this.activeTextRequests += 1;
    this.maxConcurrentTextRequests = Math.max(this.maxConcurrentTextRequests, this.activeTextRequests);
    this.notifyTextRequest(request);
  }

  private finishTextRequest(request: TextRequest): void {
    request.finishedAt = Date.now();
    this.activeTextRequests -= 1;
    this.notifyTextRequest(request);
  }

  private notifyTextRequest(request: TextRequest): void {
    for (const waiter of this.textRequestWaiters) {
      if (waiter.predicate(request)) waiter.resolve(request);
    }
  }

  private notifyRemoteText(message: RemoteTextMessage): void {
    for (const waiter of this.remoteTextWaiters) {
      if (waiter.predicate(message)) waiter.resolve(message);
    }
  }

  private async waitForEditGate(signal?: AbortSignal): Promise<void> {
    const gate = this.editGates.shift();
    if (!gate) return;
    const aborted = new Promise<void>((_resolve, reject) => {
      if (!signal) return;
      const rejectAborted = () => reject(new Error("fixture text edit aborted"));
      if (signal.aborted) rejectAborted();
      else signal.addEventListener("abort", rejectAborted, { once: true });
    });
    await Promise.race([gate, aborted]);
  }

  async reconnect(): Promise<void> {
    this.online = true;
    this.onOnline?.();
  }

  setOffline(): void {
    this.online = false;
  }

  async deliver(message: InboundMessage): Promise<void> {
    if (!this.onInbound) {
      throw new Error("fixture transport is not started");
    }
    await this.onInbound(message);
  }
}

export interface FakePromptContext {
  readonly text: string;
  readonly cwd: string;
  readonly sessionId: string;
  readonly promptContext: PromptContext | undefined;
  readonly emit: (event: WorkerEvent) => void;
  append(relativeOrAbsolutePath: string, text: string): Promise<void>;
  isStopped(): boolean;
  onStop(callback: () => void): void;
}

export type FakePrompt = (context: FakePromptContext) => Promise<void> | void;

export interface PromptRecord {
  text: string;
  cwd: string;
  sessionId: string;
  promptContext: PromptContext | undefined;
}

class FixtureWorker implements OmpWorker {
  readonly sessionFile: string;
  readonly sessionId: string;
  stopped = false;

  private currentCwd: string;
  private readonly stopCallbacks = new Set<() => void>();

  constructor(
    private readonly options: WorkerOptions,
    private readonly promptHandler: FakePrompt,
    private readonly recordPrompt: (record: PromptRecord) => void,
    sessionId: string,
  ) {
    this.sessionFile = options.sessionFile ?? join(options.sessionDir, `${sessionId}.session`);
    let persistedSessionId: string | undefined;
    if (options.sessionFile) {
      try {
        const firstLine = readFileSync(options.sessionFile, "utf8").split("\n", 1)[0] ?? "";
        const parsed = JSON.parse(firstLine) as { id?: unknown };
        if (typeof parsed.id === "string" && parsed.id.length > 0) persistedSessionId = parsed.id;
      } catch {
        // The fixture creates its session header on refreshState.
      }
    }
    this.sessionId = persistedSessionId ?? sessionId;
    this.currentCwd = options.cwd;
    options.onSpawn({
      pid: 30000 + Number(this.sessionId.replace(/\D/g, "")),
      pgid: 30000 + Number(this.sessionId.replace(/\D/g, "")),
      identity: `fixture-worker-${this.sessionId}`,
      cwd: options.cwd,
    });
  }

  async refreshState(): Promise<ProjectSessionState> {
    let header: { cwd?: unknown; previousSessionFiles?: unknown } | undefined;
    try {
      const firstLine = (await readFile(this.sessionFile, "utf8")).split("\n", 1)[0] ?? "";
      const parsed = JSON.parse(firstLine) as {
        id?: unknown;
        cwd?: unknown;
        previousSessionFiles?: unknown;
      };
      if (parsed.id === this.sessionId && typeof parsed.cwd === "string") {
        header = parsed;
      }
    } catch {
      // Fresh fixture workers materialize their session on this call.
    }
    if (!header) {
      await this.writeSessionHeader();
      header = { cwd: this.currentCwd };
    }
    const cwd = await realpath(header.cwd as string);
    const sessionFile = await realpath(this.sessionFile);
    return { sessionId: this.sessionId, sessionFile, cwd };
  }

  private async writeSessionHeader(previousSessionFiles: readonly string[] = []): Promise<void> {
    await mkdir(dirname(this.sessionFile), { recursive: true });
    await writeFile(
      this.sessionFile,
      `${JSON.stringify({
        type: "session",
        version: 3,
        id: this.sessionId,
        cwd: this.currentCwd,
        ...(previousSessionFiles.length > 0 ? { previousSessionFiles } : {}),
      })}\n`,
      "utf8",
    );
  }

  async prompt(
    text: string,
    onEvent: (event: WorkerEvent) => void,
    promptContext?: PromptContext,
  ): Promise<void> {
    const injection = promptContext?.contextInjection;
    const scope = this.options.contextScope;
    if (scope && injection && injection.injectionDecision !== "unchanged") {
      await writePreparedReceipt(
        contextSidecarPath(scope.sidecarRoot, this.sessionId),
        {
          botUid: scope.botUid,
          target: { ...scope.target },
          sessionId: this.sessionId,
          version: injection.snapshot.version,
          attemptId: injection.attemptId,
        },
      );
    }
    const cwd = this.currentCwd;
    this.recordPrompt({
      text,
      cwd,
      sessionId: this.sessionId,
      promptContext,
    });
    if (text.startsWith("/move ")) {
      const targetCwd = await realpath(text.slice("/move ".length).trim());
      let previousSessionFiles: string[] = [];
      try {
        const firstLine = (await readFile(this.sessionFile, "utf8")).split("\n", 1)[0] ?? "";
        const parsed = JSON.parse(firstLine) as { previousSessionFiles?: unknown };
        if (Array.isArray(parsed.previousSessionFiles)) {
          previousSessionFiles = parsed.previousSessionFiles.filter(
            (value): value is string => typeof value === "string",
          );
        }
      } catch {
        // refreshState has already materialized this fixture session.
      }
      if (!previousSessionFiles.includes(this.sessionFile)) previousSessionFiles.push(this.sessionFile);
      this.currentCwd = targetCwd;
      await this.writeSessionHeader(previousSessionFiles);
      return;
    }
    await this.promptHandler({
      text,
      cwd,
      sessionId: this.sessionId,
      promptContext,
      emit: onEvent,
      append: async (relativeOrAbsolutePath, content) => {
        const path = isAbsolute(relativeOrAbsolutePath)
          ? relativeOrAbsolutePath
          : join(cwd, relativeOrAbsolutePath);
        await mkdir(dirname(path), { recursive: true });
        await appendFile(path, content, "utf8");
      },
      isStopped: () => this.stopped,
      onStop: (callback) => {
        this.stopCallbacks.add(callback);
      },
    });
  }

  async stop(): Promise<boolean> {
    this.stopped = true;
    for (const callback of this.stopCallbacks) {
      callback();
    }
    return true;
  }
}

export class FakeWorkerFactory implements WorkerFactory {
  readonly workers: FixtureWorker[] = [];
  readonly prompts: PromptRecord[] = [];

  private nextSessionId = 1;
  private readonly promptWaiters: Array<{
    count: number;
    resolve: () => void;
  }> = [];

  constructor(
    private readonly promptHandler: FakePrompt = async ({ append, text }) => {
      await append("effects.log", `${text}\n`);
    },
  ) {}

  async create(options: WorkerOptions): Promise<OmpWorker> {
    const worker = new FixtureWorker(
      options,
      this.promptHandler,
      (record) => {
        this.prompts.push(record);
        for (const waiter of this.promptWaiters) {
          if (this.prompts.length >= waiter.count) {
            waiter.resolve();
          }
        }
      },
      String(this.nextSessionId++),
    );
    this.workers.push(worker);
    return worker;
  }

  async recover(_process: WorkerProcess): Promise<boolean> {
    return false;
  }

  waitForPromptCount(count: number): Promise<void> {
    if (this.prompts.length >= count) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.promptWaiters.push({ count, resolve });
    });
  }
}

export interface CoreHarness {
  readonly root: string;
  readonly stateDir: string;
  readonly config: CoreServiceConfig;
  readonly bot: BotIdentity;
  readonly transport: FakeTransport;
  readonly workers: FakeWorkerFactory;
  readonly service: ChannelService;
}

export interface CoreHarnessOptions {
  root?: string;
  bot?: BotIdentity;
  transport?: FakeTransport;
  workers?: FakeWorkerFactory;
  prompt?: FakePrompt;
  startOnline?: boolean;
  /**
   * `null` omits ownerId from the test config so enrollment starts unconfigured.
   */
  ownerId?: string | null;
  restart?: RestartPort;
}

export async function createCoreHarness(options: CoreHarnessOptions = {}): Promise<CoreHarness> {
  const rootPath = options.root ?? (await mkdtemp(join(tmpdir(), "omp-channel-octo-core-")));
  const root = await realpath(rootPath);
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const requestedBot =
    options.bot ?? { uid: `bot-${root.split("/").at(-1) ?? "fixture"}`, name: "Fixture Bot" };
  const transport =
    options.transport ??
    new FakeTransport({
      bot: requestedBot,
      startOnline: options.startOnline ?? true,
    });
  const workers = options.workers ?? new FakeWorkerFactory(options.prompt);
  const config = {
    ...(options.ownerId === null ? {} : { ownerId: options.ownerId ?? "owner-1" }),
    allowedGroupIds: ["group-allowed", "group-a", "group-b", "group-fresh", "group-unavailable", "group-tools"],
    stateDir,
    homeDir: root,
  } as CoreServiceConfig;
  const service = await createChannelService({
    config,
    transport,
    workerFactory: workers,
    ...(options.restart === undefined ? {} : { restart: options.restart }),
  });
  await service.start();
  return { root, stateDir, config, bot: transport.bot, transport, workers, service };
}

export async function closeCoreHarness(
  harness: CoreHarness,
  options: { remove?: boolean } = {},
): Promise<void> {
  await harness.service.stop();
  if (options.remove ?? true) {
    await rm(harness.root, { recursive: true, force: true });
  }
}

export function inbound(
  overrides: Partial<InboundMessage> = {},
): InboundMessage {
  return {
    channelId: "dm-owner",
    channelType: 1,
    messageId: `message-${Math.random().toString(36).slice(2)}`,
    senderId: "owner-1",
    text: "perform fixture work",
    mentionedUserIds: [],
    supported: true,
    ...overrides,
  };
}

export async function waitForStatus(
  service: ChannelService,
  target: ChatTarget,
  predicate: (status: ChannelStatus) => boolean,
): Promise<ChannelStatus> {
  for (let attempt = 0; attempt < 10000; attempt += 1) {
    const status = await service.status(target);
    if (predicate(status)) {
      return status;
    }
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error(`status did not reach expectation for ${target.channelType}:${target.channelId}`);
}

export async function readText(path: string): Promise<string> {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return "";
    }
    throw error;
  }
}
