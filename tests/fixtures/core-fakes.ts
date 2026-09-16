import { readFileSync } from "node:fs";
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import type {
  AllowlistWriteback,
  AskConfig,
  AttachmentDownload,
  BotCardAction,
  BotEvent,
  BotEventFetch,
  BotIdentity,
  CardFrame,
  ChannelCommandHandler,
  ChannelService,
  ChannelStatus,
  ChannelTransport,
  ChatTarget,
  CliCredentialPort,
  CoreServiceConfig,
  DeleteThreadOptions,
  GroupContext,
  GroupHistoryConfig,
  GroupInfo,
  GroupInfoPatch,
  GroupMember,
  GroupMentionPref,
  HistoryFetch,
  HistoryMessage,
  InboundMessage,
  OmpWorker,
  ProgressCard,
  PromptContext,
  RestartPort,
  SendReceipt,
  ThreadSummary,
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorkerEvent,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorktrunkProbe,
} from "../../src/contracts";
import { AttachmentError } from "../../src/attachments";
import {
  contextSidecarPath,
  normalizeGroupContext,
  readBoundSession,
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

export interface SentCard {
  target: ChatTarget;
  frame: CardFrame;
  clientMsgNo: string;
  messageId: string;
}

export interface CardEdit {
  target: ChatTarget;
  messageId: string;
  frame: CardFrame;
  cardSeq: number;
}

/** One `card_action` event exactly as the Bot event queue delivers it. */
export function cardActionEvent(eventId: number, action: BotCardAction): BotEvent {
  return { eventId, kind: "card_action", action };
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
  /** Cards stay opt-in so the text-only path is what every other test exercises. */
  cardSupported = false;
  readonly sentCards: SentCard[] = [];
  readonly cardEdits: CardEdit[] = [];
  readonly botEvents: BotEvent[] = [];
  readonly eventFetches: number[] = [];
  readonly ackedEvents: number[] = [];
  cardSendFailure: Error | undefined;
  /** Test hook: a frame the deployment cannot carry, refused the way the real limits refuse it. */
  cardSendRefusal: ((frame: CardFrame) => boolean) | undefined;
  cardEditFailure: Error | undefined;
  /** When true, `fetchBotEvents` reports a deployment without a Bot event endpoint. */
  botEventsUnsupported = false;
  eventFetchFailure: Error | undefined;
  ackFailure: Error | undefined;
  readonly fileAttempts: Array<SentFile & { error?: string }> = [];
  readonly sentFiles: SentFile[] = [];
  readonly textRequests: TextRequest[] = [];
  readonly remoteTextMessages = new Map<string, RemoteTextMessage>();
  readonly groupContextCalls: ChatTarget[] = [];
  readonly managementCalls: Array<{
    action: string;
    groupId: string;
    shortId: string | null;
    /** Present when the caller asked for a connection of its own (a retry). */
    freshConnection?: boolean;
  }> = [];
  readonly historyCalls: Array<{ target: ChatTarget } & HistoryFetch> = [];
  readonly attachmentBodies = new Map<string, Uint8Array>();
  readonly downloadRequests: Array<{ url: string; path: string; maxBytes: number }> = [];
  readonly transcriptionRequests: string[] = [];
  readonly transcriptionByPath = new Map<string, string>();
  /** Groups whose 免@ decision was asked for, in call order. */
  readonly mentionPrefCalls: string[] = [];
  /** When set, `downloadAttachment` throws this instead of consulting `attachmentBodies`. */
  attachmentFailure: AttachmentError | undefined;
  /** When set, `transcribeAudio` throws this instead of consulting `transcriptionByPath`. */
  transcriptionError: Error | undefined;
  /** When true, `downloadAttachment` waits for its signal to abort instead of finishing. */
  downloadHangs = false;
  /** When true, `transcribeAudio` waits for its signal to abort instead of answering. */
  transcriptionHangs = false;
  readonly bot: BotIdentity;
  online: boolean;
  maxConcurrentTextRequests = 0;
  /** How often the service asked for its Bot identity, i.e. how often it registered. */
  initializeCalls = 0;

  private readonly startOnline: boolean;
  private readonly contexts = new Map<string, GroupContext | null>();
  private readonly histories = new Map<string, HistoryMessage[]>();
  private readonly mentionPrefs = new Map<string, GroupMentionPref>();
  private mentionPrefFailure: Error | undefined;
  private historyFailure: Error | undefined;
  private readonly attemptWaiters: Array<{
    predicate: (attempt: TransportAttempt) => boolean;
    resolve: (attempt: TransportAttempt) => void;
  }> = [];
  private readonly textRequestWaiters: TextRequestWaiter[] = [];
  private readonly remoteTextWaiters: RemoteTextWaiter[] = [];
  private readonly cardEditWaiters: Array<{
    predicate: (edit: CardEdit) => boolean;
    resolve: (edit: CardEdit) => void;
  }> = [];
  private readonly cardSendWaiters: Array<{
    predicate: (card: SentCard) => boolean;
    resolve: (card: SentCard) => void;
  }> = [];
  private readonly ackWaiters: Array<{ eventId: number; resolve: (eventId: number) => void }> = [];
  private readonly eventWaiters: Array<() => void> = [];
  private readonly editGates: Array<Promise<void>> = [];
  private readonly remoteTextByClientMsgNo = new Map<string, string>();
  private activeTextRequests = 0;
  private nextRemoteTextId = 1;
  private onInbound: ((message: InboundMessage) => Promise<void>) | undefined;
  private onOnline: (() => void) | undefined;
  private failOncePredicate: ((message: SentMessage) => boolean) | undefined;
  private failEditOncePredicate: ((request: TextRequest) => boolean) | undefined;
  private groupContextFailure: Error | undefined;
  private readonly groupInfo = new Map<string, GroupInfo>();
  private readonly groupMembers = new Map<string, GroupMember[]>();
  private readonly threads = new Map<string, ThreadSummary[]>();
  private readonly threadMembers = new Map<string, GroupMember[]>();
  private managementFailure: Error | undefined;
  private managementFailures: Error[] = [];
  private nextThreadId = 1;
  constructor(options: FakeTransportOptions = {}) {
    this.bot = options.bot ?? { uid: "bot-fixture", name: "Fixture Bot" };
    this.online = options.online ?? true;
    this.startOnline = options.startOnline ?? true;
  }

  initialize(): Promise<BotIdentity> {
    this.initializeCalls += 1;
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

  async fetchGroupMessages(target: ChatTarget, fetch: HistoryFetch): Promise<HistoryMessage[]> {
    this.historyCalls.push({ target: { ...target }, ...fetch });
    if (fetch.signal?.aborted) throw new Error("fixture history aborted");
    if (this.historyFailure) throw this.historyFailure;
    const stored = this.histories.get(`${target.channelType}:${target.channelId}`) ?? [];
    const ascending = [...stored].sort((left, right) => left.seq - right.seq);
    if (fetch.sinceSeq === 0 && fetch.untilSeq === 0) {
      return ascending.slice(Math.max(0, ascending.length - fetch.limit));
    }
    return ascending
      .filter((message) => message.seq >= fetch.sinceSeq
        && (fetch.untilSeq === 0 || message.seq < fetch.untilSeq))
      .slice(0, fetch.limit);
  }

  setGroupHistory(target: ChatTarget, messages: readonly HistoryMessage[]): void {
    this.histories.set(
      `${target.channelType}:${target.channelId}`,
      messages.map((message) => ({ ...message })),
    );
  }

  /** Passing `undefined` clears the failure so later reads succeed again. */
  failGroupHistoryWith(error?: Error): void {
    this.historyFailure = error;
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

  /** Passing `undefined` clears the failure so later reads succeed again. */
  failGroupContextWith(error?: Error): void {
    this.groupContextFailure = error;
  }

  private recordManagement(
    action: string,
    groupId: string,
    shortId: string | null,
    signal?: AbortSignal,
    options?: DeleteThreadOptions,
  ): void {
    if (signal?.aborted) throw new Error("fixture management aborted");
    this.managementCalls.push({
      action,
      groupId,
      shortId,
      ...(options?.freshConnection === true ? { freshConnection: true } : {}),
    });
    const queued = this.managementFailures.shift();
    if (queued) throw queued;
    if (this.managementFailure) throw this.managementFailure;
  }

  private groupInfoFor(groupId: string): GroupInfo {
    return (
      this.groupInfo.get(groupId) ?? {
        groupId,
        name: "Fixture Group",
        notice: "",
        creator: "owner-1",
        status: 1,
        createdAt: "2026-09-12T00:00:00.000Z",
      }
    );
  }

  async getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo> {
    this.recordManagement("group-info", groupId, null, signal);
    return this.groupInfoFor(groupId);
  }

  /**
   * 免@ decision for one group. Groups with nothing set answer `effective: false`, so a
   * test only opts into the exemption it is about; `signal` is honoured like the real one.
   */
  async getGroupMentionPref(groupId: string, signal?: AbortSignal): Promise<GroupMentionPref> {
    if (signal?.aborted) throw new Error("fixture mention preference aborted");
    this.mentionPrefCalls.push(groupId);
    if (this.mentionPrefFailure) throw this.mentionPrefFailure;
    return this.mentionPrefs.get(groupId) ?? { effective: false };
  }

  setGroupMentionPref(groupId: string, pref: GroupMentionPref): void {
    this.mentionPrefs.set(groupId, pref);
  }

  /** Passing `undefined` clears the failure so later lookups succeed again. */
  failGroupMentionPrefWith(error?: Error): void {
    this.mentionPrefFailure = error;
  }

  async getGroupMembers(groupId: string, signal?: AbortSignal): Promise<GroupMember[]> {
    this.recordManagement("group-members", groupId, null, signal);
    return this.groupMembers.get(groupId) ?? [];
  }

  async updateGroupInfo(
    groupId: string,
    patch: GroupInfoPatch,
    signal?: AbortSignal,
  ): Promise<void> {
    this.recordManagement("group-update", groupId, null, signal);
    const previous = this.groupInfoFor(groupId);
    this.groupInfo.set(groupId, {
      ...previous,
      ...(patch.name === undefined ? {} : { name: patch.name }),
      ...(patch.notice === undefined ? {} : { notice: patch.notice }),
    });
  }

  async listThreads(groupId: string, signal?: AbortSignal): Promise<ThreadSummary[]> {
    this.recordManagement("list-threads", groupId, null, signal);
    return (this.threads.get(groupId) ?? []).map((thread) => ({ ...thread }));
  }

  async createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary> {
    this.recordManagement("create-thread", groupId, null, signal);
    const shortId = `thread-${this.nextThreadId}`;
    this.nextThreadId += 1;
    const thread: ThreadSummary = {
      shortId,
      groupId,
      channelId: `${groupId}____${shortId}`,
      name,
      creatorUid: this.bot.uid,
      status: 1,
      memberCount: 1,
      messageCount: 0,
      createdAt: "2026-09-12T00:00:00.000Z",
      updatedAt: "2026-09-12T00:00:00.000Z",
    };
    this.threads.set(groupId, [...(this.threads.get(groupId) ?? []), thread]);
    return { ...thread };
  }

  async getThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<ThreadSummary> {
    this.recordManagement("thread-info", groupId, shortId, signal);
    const thread = (this.threads.get(groupId) ?? []).find((entry) => entry.shortId === shortId);
    if (!thread) throw new Error(`fixture thread ${shortId} is not known`);
    return { ...thread };
  }

  async getThreadMembers(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
  ): Promise<GroupMember[]> {
    this.recordManagement("thread-members", groupId, shortId, signal);
    return this.threadMembers.get(`${groupId}:${shortId}`) ?? [];
  }

  async joinThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    this.recordManagement("thread-join", groupId, shortId, signal);
  }

  async leaveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    this.recordManagement("thread-leave", groupId, shortId, signal);
  }

  async deleteThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
    options?: DeleteThreadOptions,
  ): Promise<void> {
    this.recordManagement("thread-delete", groupId, shortId, signal, options);
    this.threads.set(
      groupId,
      (this.threads.get(groupId) ?? []).filter((entry) => entry.shortId !== shortId),
    );
  }

  setGroupInfo(groupId: string, info: GroupInfo): void {
    this.groupInfo.set(groupId, { ...info });
  }

  setGroupMembers(groupId: string, members: readonly GroupMember[]): void {
    this.groupMembers.set(
      groupId,
      members.map((member) => ({ ...member })),
    );
  }

  setThreadMembers(groupId: string, shortId: string, members: readonly GroupMember[]): void {
    this.threadMembers.set(
      `${groupId}:${shortId}`,
      members.map((member) => ({ ...member })),
    );
  }

  setThreads(groupId: string, threads: readonly ThreadSummary[]): void {
    this.threads.set(
      groupId,
      threads.map((thread) => ({ ...thread })),
    );
  }

  /** Passing `undefined` clears the failure so later calls succeed again. */
  failManagementWith(error?: Error): void {
    this.managementFailure = error;
  }

  /** Fails the next management calls, one queued error per call, then lets them succeed again. */
  failManagementTimes(errors: readonly Error[]): void {
    this.managementFailures = [...errors];
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

  async sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    if (signal?.aborted) throw new Error("fixture card send aborted");
    if (!this.cardSupported) return null;
    if (this.cardSendRefusal?.(frame) === true) return null;
    if (this.cardSendFailure) throw this.cardSendFailure;
    const messageId = `card-${this.sentCards.length + 1}`;
    this.sentCards.push({ target, frame, clientMsgNo, messageId });
    this.notifyCardEdit();
    return { messageId };
  }

  async editCard(
    target: ChatTarget,
    messageId: string,
    frame: CardFrame,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw new Error("fixture card edit aborted");
    if (this.cardEditFailure) throw this.cardEditFailure;
    this.cardEdits.push({ target, messageId, frame, cardSeq });
    this.notifyCardEdit();
  }

  async fetchBotEvents(fetch: BotEventFetch): Promise<BotEvent[] | null> {
    this.eventFetches.push(fetch.afterEventId);
    if (this.botEventsUnsupported) return null;
    for (;;) {
      if (fetch.signal?.aborted) throw new Error("fixture bot events aborted");
      if (this.eventFetchFailure) throw this.eventFetchFailure;
      const ready = this.botEvents
        .filter((event) => event.eventId > fetch.afterEventId)
        .sort((left, right) => left.eventId - right.eventId)
        .slice(0, fetch.limit);
      if (ready.length > 0) {
        this.botEvents.splice(0, ready.length);
        return ready;
      }
      // Hold like the server does: the next pushed event wakes every waiting fetch.
      await this.holdForBotEvent(fetch.signal);
    }
  }

  async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new Error("fixture bot event ack aborted");
    if (this.ackFailure) throw this.ackFailure;
    this.ackedEvents.push(eventId);
    this.notifyCardEdit();
  }

  /** Queues one event as if the server delivered it. */
  pushBotEvent(event: BotEvent): void {
    this.botEvents.push(event);
    for (const wake of this.eventWaiters.splice(0)) wake();
  }

  waitForCardEdit(predicate: (edit: CardEdit) => boolean): Promise<CardEdit> {
    const existing = this.cardEdits.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise<CardEdit>((resolve) => {
      this.cardEditWaiters.push({ predicate, resolve });
    });
  }

  waitForCardSend(predicate: (card: SentCard) => boolean): Promise<SentCard> {
    const existing = this.sentCards.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise<SentCard>((resolve) => {
      this.cardSendWaiters.push({ predicate, resolve });
    });
  }

  waitForAck(eventId: number): Promise<number> {
    if (this.ackedEvents.includes(eventId)) return Promise.resolve(eventId);
    return new Promise<number>((resolve) => {
      this.ackWaiters.push({ eventId, resolve });
    });
  }

  private holdForBotEvent(signal: AbortSignal | undefined): Promise<void> {
    const { promise, resolve } = Promise.withResolvers<void>();
    const wake = (): void => {
      signal?.removeEventListener("abort", wake);
      resolve();
    };
    this.eventWaiters.push(wake);
    signal?.addEventListener("abort", wake, { once: true });
    return promise;
  }

  private notifyCardEdit(): void {
    for (const waiter of this.cardEditWaiters) {
      const hit = this.cardEdits.find(waiter.predicate);
      if (hit) waiter.resolve(hit);
    }
    for (const waiter of this.cardSendWaiters) {
      const hit = this.sentCards.find(waiter.predicate);
      if (hit) waiter.resolve(hit);
    }
    for (const waiter of this.ackWaiters) {
      if (this.ackedEvents.includes(waiter.eventId)) waiter.resolve(waiter.eventId);
    }
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

  /**
   * Resolves one attachment from `attachmentBodies` and writes it locally; no network request
   * is made. `attachmentFailure` wins over the unknown-URL and oversize checks.
   */
  async downloadAttachment(request: AttachmentDownload): Promise<number> {
    this.downloadRequests.push({ url: request.url, path: request.path, maxBytes: request.maxBytes });
    if (request.signal?.aborted) {
      throw new AttachmentError("failed", "fixture attachment download aborted");
    }
    if (this.downloadHangs) {
      await new Promise<never>((_resolve, reject) => {
        request.signal?.addEventListener(
          "abort",
          () => reject(new AttachmentError("timeout", "fixture attachment download timed out")),
          { once: true },
        );
      });
    }
    if (this.attachmentFailure) throw this.attachmentFailure;
    const body = this.attachmentBodies.get(request.url);
    if (!body) throw new AttachmentError("failed", `fixture has no body for ${request.url}`);
    if (body.byteLength > request.maxBytes) {
      throw new AttachmentError("oversize", `fixture attachment exceeds ${request.maxBytes} bytes`);
    }
    await writeFile(request.path, body, { mode: 0o600 });
    return body.byteLength;
  }

  /** Returns the transcription registered for `path`, `null` when none, or `transcriptionError`. */
  async transcribeAudio(path: string, signal?: AbortSignal): Promise<string | null> {
    this.transcriptionRequests.push(path);
    if (signal?.aborted) throw new Error("fixture transcription aborted");
    if (this.transcriptionHangs) {
      await new Promise<never>((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(new Error("fixture transcription aborted")), { once: true });
      });
    }
    if (this.transcriptionError) throw this.transcriptionError;
    return this.transcriptionByPath.get(path) ?? null;
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

export interface FakeAppendRecord {
  text: string;
  sessionId: string;
  cwd: string;
}

export interface FakePromptContext {
  readonly text: string;
  readonly cwd: string;
  readonly sessionId: string;
  readonly promptContext: PromptContext | undefined;
  readonly emit: (event: WorkerEvent) => void;
  /** Appended messages this prompt already received, oldest first. */
  appended(): readonly FakeAppendRecord[];
  /** Resolves with the next appended message delivered while this prompt runs. */
  waitForAppend(): Promise<FakeAppendRecord>;
  /** Resolves when `/interrupt` aborts this prompt's turn. */
  waitForAbort(): Promise<void>;
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
  /** Scope root this prompt ran under, for group and thread turns. */
  sidecarRoot?: string;
  /** Session the host published as bound in that scope when this prompt started. */
  boundSessionId?: string;
}

class FixtureWorker implements OmpWorker {
  readonly sessionFile: string;
  readonly sessionId: string;
  stopped = false;
  /** How often the host asked this turn to abort (`/interrupt`, `/cancel`, a question stop). */
  abortCalls = 0;

  private currentCwd: string;
  private readonly stopCallbacks = new Set<() => void>();
  private promptInFlight = false;
  private abortSignaled = false;
  private appendCursor = 0;
  private readonly appendedMessages: FakeAppendRecord[] = [];
  private readonly appendWaiters: Array<(record: FakeAppendRecord) => void> = [];
  private readonly abortWaiters: Array<() => void> = [];

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
    // Mirrors the OMP extension: a published pointer naming another session
    // belongs to a sub-session, and only the driven session records the receipt.
    const bound = scope === undefined ? undefined : await readBoundSession(scope.sidecarRoot);
    if (
      scope !== undefined
      && (bound === undefined || bound.sessionId === this.sessionId)
      && injection
      && injection.injectionDecision !== "unchanged"
    ) {
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
      ...(scope === undefined ? {} : { sidecarRoot: scope.sidecarRoot }),
      ...(bound === undefined ? {} : { boundSessionId: bound.sessionId }),
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
    this.promptInFlight = true;
    try {
      await this.promptHandler({
        text,
        cwd,
        sessionId: this.sessionId,
        promptContext,
        emit: onEvent,
        appended: () => [...this.appendedMessages],
        waitForAppend: () => this.takeAppend(),
        waitForAbort: () => this.takeAbort(),
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
    } finally {
      this.promptInFlight = false;
    }
  }

  async steer(text: string): Promise<void> {
    if (!this.promptInFlight) throw new Error("fixture worker is not streaming a turn");
    this.appendedMessages.push({
      text,
      sessionId: this.sessionId,
      cwd: this.currentCwd,
    });
    while (this.appendWaiters.length > 0 && this.appendCursor < this.appendedMessages.length) {
      const waiter = this.appendWaiters.shift()!;
      const record = this.appendedMessages[this.appendCursor++]!;
      waiter(record);
    }
  }

  async abortTurn(): Promise<void> {
    if (!this.promptInFlight) throw new Error("fixture worker has no prompt in progress");
    this.abortCalls += 1;
    this.abortSignaled = true;
    for (const callback of this.abortWaiters.splice(0)) callback();
  }

  private takeAppend(): Promise<FakeAppendRecord> {
    const next = this.appendedMessages[this.appendCursor];
    if (next) {
      this.appendCursor += 1;
      return Promise.resolve(next);
    }
    return new Promise<FakeAppendRecord>((resolve) => {
      this.appendWaiters.push(resolve);
    });
  }

  private takeAbort(): Promise<void> {
    if (this.abortSignaled) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.abortWaiters.push(resolve);
    });
  }

  async stop(): Promise<boolean> {
    this.stopped = true;
    for (const callback of this.stopCallbacks) {
      callback();
    }
    // A stopped worker can never continue the turn, so prompts awaiting an abort
    // must not keep the fixture (and any awaiting test) alive.
    this.abortSignaled = true;
    for (const callback of this.abortWaiters.splice(0)) callback();
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
  readonly worktrees: FakeWorktrees | undefined;
  readonly service: ChannelService;
}

/** Records isolate requests and materializes a directory per thread under the harness root. */
export class FakeWorktrees implements WorktreePort {
  readonly requests: ThreadWorkspaceRequest[] = [];
  /** When set, `create` rejects with this error instead of returning a workspace. */
  failure: Error | undefined;
  /** Merge requests recorded by `merge`; the fake never runs wt. */
  readonly merges: WorktreeMergeRequest[] = [];
  /** Removal requests recorded by `remove`. */
  readonly removals: WorktreeRemovalRequest[] = [];
  /** The probe answer `worktrunk()` reports; tests flip it to model a missing or old wt. */
  probe: WorktrunkProbe = { available: true, command: "wt", version: "v0.76.0" };
  /** When set, `merge` rejects with this error instead of returning a summary. */
  mergeFailure: Error | undefined;
  /** When set, `remove` rejects with this error instead of reporting an outcome. */
  removeFailure: Error | undefined;
  /** What `remove` reports; `missing` models a workspace that is already gone. */
  removal: WorktreeRemovalOutcome = { kind: "removed", via: "wt", branchOutcome: "deleted" };

  constructor(private readonly root: string) {}

  async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    this.requests.push({ ...request });
    if (this.failure) throw this.failure;
    const cwd = join(this.root, "worktrees", request.threadId);
    await mkdir(cwd, { recursive: true });
    return { cwd: await realpath(cwd), branch: request.threadId };
  }

  async merge(request: WorktreeMergeRequest): Promise<WorktreeMergeSummary> {
    this.merges.push({ ...request });
    if (this.mergeFailure) throw this.mergeFailure;
    return {
      branch: "fixture-branch",
      target: request.target ?? "main",
      squashed: true,
      rebased: true,
      committed: true,
      removed: request.remove !== false,
    };
  }

  async remove(request: WorktreeRemovalRequest): Promise<WorktreeRemovalOutcome> {
    this.removals.push({ ...request });
    if (this.removeFailure) throw this.removeFailure;
    return this.removal;
  }

  async worktrunk(): Promise<WorktrunkProbe> {
    return this.probe;
  }
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
  /** Overrides the allowlist when a test drives chat targets outside the defaults. */
  allowedGroupIds?: string[];
  /** Group history configuration; absent means the service default (disabled). */
  groupHistory?: GroupHistoryConfig;
  /** `ask` wait configuration; absent means the service default (6-hour timeout). */
  ask?: AskConfig;
  /** Extra command handlers, e.g. an observing handler that still runs a prompt. */
  commandHandlers?: readonly ChannelCommandHandler[];
  restart?: RestartPort;
  /** Isolated thread workspaces; `null` omits the port so isolate reports an unconfigured deployment. */
  worktrees?: FakeWorktrees | null;
  /** Per-bot octo-cli credential port; absent means the deployment has no such capability. */
  cliCredentials?: CliCredentialPort;
  /** Allowlist mirror into the config file; absent means a hand-built config with no path. */
  allowlistWriteback?: AllowlistWriteback;
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
  const worktrees =
    options.worktrees === null ? undefined : (options.worktrees ?? new FakeWorktrees(root));
  const config = {
    ...(options.ownerId === null ? {} : { ownerId: options.ownerId ?? "owner-1" }),
    allowedGroupIds: options.allowedGroupIds ?? [
      "group-allowed",
      "group-a",
      "group-b",
      "group-fresh",
      "group-unavailable",
      "group-tools",
    ],
    stateDir,
    homeDir: root,
    ...(options.groupHistory === undefined ? {} : { groupHistory: options.groupHistory }),
    ...(options.ask === undefined ? {} : { ask: options.ask }),
  } as CoreServiceConfig;
  const service = await createChannelService({
    config,
    transport,
    workerFactory: workers,
    ...(worktrees === undefined ? {} : { worktrees }),
    ...(options.commandHandlers === undefined ? {} : { commandHandlers: options.commandHandlers }),
    ...(options.restart === undefined ? {} : { restart: options.restart }),
    ...(options.cliCredentials === undefined ? {} : { cliCredentials: options.cliCredentials }),
    ...(options.allowlistWriteback === undefined ? {} : { allowlistWriteback: options.allowlistWriteback }),
  });
  await service.start();
  return {
    root,
    stateDir,
    config,
    bot: transport.bot,
    transport,
    workers,
    worktrees,
    service,
  };
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
    attachments: [],
    quoted: null,
    supported: true,
    messageSeq: 0,
    messageTimestamp: 0,
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
