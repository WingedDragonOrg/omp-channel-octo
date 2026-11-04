import { readFileSync } from "node:fs";
import { appendFile, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import type {
  AllowlistWriteback,
  AskConfig,
  AttachmentConfig,
  AttachmentDownload,
  BotCardAction,
  BotEvent,
  BotEventFetch,
  BotIdentity,
  CardFrame,
  ChannelCommandHandler,
  ChannelService,
  ChannelStatus,
  CommitWorker,
  CommitWorkerOptions,
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
  UserInfo,
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorkerEvent,
  WorkerContextUsage,
  WorkerGoal,
  WorkerGoalResult,
  WorkerGoalRun,
  WorkerFactory,
  WorkerModelCatalog,
  WorkerModelSwitch,
  WorkerOptions,
  WorkerProcess,
  WorktreeMergePlan,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
  WorktreePort,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  WorktrunkProbe,
} from "../../src/contracts";
import type { LoopReadPort, LoopTask, LoopWorkspace } from "../../src/loop/types.ts";
import { RESUME_CONTINUATION_PROMPT } from "../../src/contracts";
import { AttachmentError } from "../../src/inbound/attachments";
import {
  contextSidecarPath,
  normalizeGroupContext,
  readBoundSession,
  writePreparedReceipt,
} from "../../src/session/context-sidecar";
import type { ProjectSessionState } from "../../src/session/project-session";
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

/** One management call the transport recorded; `freshConnection` marks a retry on its own connection. */
export interface ManagementCall {
  action: string;
  groupId: string;
  shortId: string | null;
  /** Present when the caller asked for a connection of its own (a retry). */
  freshConnection?: boolean;
}

/** 服务端的子区状态取值：1 活跃、2 已归档。 */
const THREAD_STATUS_ARCHIVED = 2;

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
  readonly managementCalls: ManagementCall[] = [];
  readonly historyCalls: Array<{ target: ChatTarget } & HistoryFetch> = [];
  readonly attachmentBodies = new Map<string, Uint8Array>();
  readonly downloadRequests: Array<{ url: string; path: string; maxBytes: number }> = [];
  readonly transcriptionRequests: string[] = [];
  readonly transcriptionByPath = new Map<string, string>();
  /** Groups whose 免@ decision was asked for, in call order. */
  readonly mentionPrefCalls: string[] = [];
  /** uid → 显示名（署名解析的假实现）；未登记的 uid 与 `userInfoFailure` 走失败路径。 */
  readonly userNames = new Map<string, string>();
  readonly userInfoCalls: string[] = [];
  userInfoFailure: Error | undefined;
  /** When true, `getUserInfo` waits for its signal to abort instead of answering. */
  userInfoHangs = false;
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
  private readonly managementWaiters: Array<{
    predicate: (call: ManagementCall) => boolean;
    count: number;
    resolve: () => void;
  }> = [];
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
  beforeGetThread: ((groupId: string, shortId: string, signal?: AbortSignal) => Promise<void>) | undefined;
  private managementFailure: Error | undefined;
  private managementFailures: Error[] = [];
  /** 排定归档的失败注入：按调用顺序消费一个错误，随后恢复成功。 */
  private archiveFailures: Error[] = [];
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
    this.settleManagementWaiters();
    const queued = this.managementFailures.shift();
    if (queued) throw queued;
    if (this.managementFailure) throw this.managementFailure;
  }

  /** 每次记录后结算到达条件的等待者：失败的调用同样算数（重试测试要看到失败尝试）。 */
  private settleManagementWaiters(): void {
    for (let index = this.managementWaiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.managementWaiters[index]!;
      if (this.managementCalls.filter(waiter.predicate).length < waiter.count) continue;
      this.managementWaiters.splice(index, 1);
      waiter.resolve();
    }
  }

  /** Resolves once `count` recorded management calls match `predicate` (past calls included). */
  waitForManagementCalls(
    predicate: (call: ManagementCall) => boolean,
    count = 1,
  ): Promise<void> {
    if (this.managementCalls.filter(predicate).length >= count) return Promise.resolve();
    const wait = Promise.withResolvers<void>();
    this.managementWaiters.push({ predicate, count, resolve: () => wait.resolve() });
    return wait.promise;
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

  async getUserInfo(uid: string, signal?: AbortSignal): Promise<UserInfo> {
    this.userInfoCalls.push(uid);
    if (this.userInfoHangs) {
      await new Promise<void>((resolve) => {
        if (signal?.aborted) resolve();
        else signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    }
    if (signal?.aborted) throw new Error("fixture user info aborted");
    if (this.userInfoFailure !== undefined) throw this.userInfoFailure;
    const name = this.userNames.get(uid);
    if (name === undefined) throw new Error(`fixture has no user ${uid}`);
    return { uid, name };
  }

  /** Registers one uid's display name for `getUserInfo`; unregistered uids fail the lookup. */
  setUserName(uid: string, name: string): void {
    this.userNames.set(uid, name);
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
    await this.beforeGetThread?.(groupId, shortId, signal);
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

  async archiveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    this.recordManagement("thread-archive", groupId, shortId, signal);
    const queued = this.archiveFailures.shift();
    if (queued) throw queued;
    const thread = (this.threads.get(groupId) ?? []).find((entry) => entry.shortId === shortId);
    if (thread) thread.status = THREAD_STATUS_ARCHIVED;
  }

  /** Fails the next archive calls, one queued error per call, then lets them succeed again. */
  failArchiveTimes(errors: readonly Error[]): void {
    this.archiveFailures = [...errors];
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
  /** Every `sendTyping` call in order, for early-typing and renewal cadence assertions. */
  readonly typingCalls: { target: ChatTarget }[] = [];

  async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new Error("fixture typing aborted");
    this.typingCalls.push({ target: { ...target } });
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
export interface FakeGoalRunContext {
  readonly request: WorkerGoalRun;
  readonly sessionId: string;
  readonly promptContext: PromptContext | undefined;
  readonly current: WorkerGoalResult;
  readonly emit: (event: WorkerEvent) => void;
  readonly appended: () => readonly FakeAppendRecord[];
  readonly waitForAppend: () => Promise<FakeAppendRecord>;
  readonly waitForAbort: () => Promise<void>;
}

export type FakeGoalHandler = (
  context: FakeGoalRunContext,
) => Promise<WorkerGoalResult> | WorkerGoalResult;

export interface FakeGoalRunRecord {
  request: WorkerGoalRun;
  sessionId: string;
  promptContext: PromptContext | undefined;
}

export interface FakeBtwRunContext {
  readonly question: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
  readonly signal: AbortSignal | undefined;
}

export type FakeBtwHandler = (context: FakeBtwRunContext) => Promise<string> | string;

interface FixtureGoalState {
  value: WorkerGoalResult;
}

function fakeGoalResult(goal: WorkerGoal | null): WorkerGoalResult {
  return goal === null
    ? { goal: null, state: null }
    : {
        goal,
        state: {
          enabled: true,
          mode: goal.status === "active" ? "active" : "exiting",
          goal,
        },
      };
}

/**
 * Stand-in for OMP's `/switch` resolution: `@role` through the fixture's role map, then an exact
 * `provider/id`, then a unique model id, then a unique substring, with an optional `:level`
 * suffix. Anything unresolvable (including an ambiguous match) is `null` — OMP's answer for a
 * selector it cannot pin down.
 */
function resolveFixtureModel(
  selector: string,
  roles: Readonly<Record<string, { model: string; level: string | null }>>,
  catalog: WorkerModelCatalog,
): { model: string; level: string | null } | null {
  const levels = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];
  const trimmed = selector.trim();
  if (trimmed.startsWith("@")) {
    const role = roles[trimmed.slice(1).toLowerCase()];
    return role === undefined ? null : { ...role };
  }
  const level = levels.find(candidate => trimmed.endsWith(`:${candidate}`));
  const body = level === undefined ? trimmed : trimmed.slice(0, -level.length - 1);
  if (body.length === 0) return null;
  const matches = [
    catalog.available.filter(model => `${model.provider}/${model.id}` === body),
    catalog.available.filter(model => model.id === body),
    catalog.available.filter(model => model.id.includes(body) || `${model.provider}/${model.id}`.includes(body)),
  ].find(candidates => candidates.length > 0);
  if (matches === undefined || matches.length !== 1) return null;
  const match = matches[0]!;
  return { model: `${match.provider}/${match.id}`, level: level ?? catalog.level };
}

class FixtureWorker implements OmpWorker {
  readonly sessionFile: string;
  readonly sessionId: string;
  stopped = false;
  contextUsageReads = 0;
  /** How often the host asked this turn to abort (`/interrupt`, `/cancel`, a question stop). */
  abortCalls = 0;
  /** Selectors this session was asked to switch to, in order — the native `/switch` dispatches. */
  readonly switchDispatches: string[] = [];
  /** Steering attempts visible at the worker boundary. */
  readonly steerCalls: string[] = [];
  /** Native goal RPC controls requested in this session. */
  readonly goalOperations: Array<"get" | "pause" | "drop"> = [];

  private readonly goalState: FixtureGoalState;
  private currentCwd: string;
  private switchedModel: { model: string; level: string | null } | undefined;
  private readonly stopCallbacks = new Set<() => void>();
  private readonly steerStopWaiters = new Set<(error: Error) => void>();
  private promptInFlight = false;
  private goalRunInFlight = false;
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
    private readonly catalog: () => WorkerModelCatalog,
    private readonly resolveModel: (selector: string) => { model: string; level: string | null } | null,
    private readonly modelsFailure: () => Error | undefined,
    private readonly goalStateFor: (sessionId: string) => FixtureGoalState,
    private readonly goalHandler: FakeGoalHandler,
    private readonly recordGoalRun: (record: FakeGoalRunRecord) => void,
    private readonly waitBeforeStop: (sessionId: string) => Promise<void>,
    private readonly steerFailure: () => Error | undefined,
    private readonly beforeSteer: (text: string) => Promise<void>,
    private readonly btwHandler: FakeBtwHandler,
    private readonly recordBtw: (record: FakeBtwRunContext) => void,
    private readonly contextUsageFor: () => Omit<WorkerContextUsage, "sessionId" | "sessionFile">,
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
    this.goalState = this.goalStateFor(this.sessionId);
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

  /** Mirrors OMP: a worker started with `--model` reports that model, and `/switch` moves it. */
  async models(): Promise<WorkerModelCatalog> {
    const failure = this.modelsFailure();
    if (failure !== undefined) throw failure;
    const catalog = this.catalog();
    return {
      current: this.switchedModel?.model ?? this.options.model ?? catalog.current,
      level: this.switchedModel?.level ?? catalog.level,
      available: catalog.available.map(model => ({ ...model })),
    };
  }

  async contextUsage(signal?: AbortSignal): Promise<WorkerContextUsage> {
    signal?.throwIfAborted();
    this.contextUsageReads += 1;
    const sample = this.contextUsageFor();
    const catalog = this.catalog();
    const currentModel = this.switchedModel?.model ?? this.options.model ?? catalog.current;
    const level = this.switchedModel?.level ?? catalog.level;
    return {
      ...sample,
      model: sample.model ?? `${currentModel}${level === null ? "" : `:${level}`}`,
      sessionId: this.sessionId,
      sessionFile: this.sessionFile,
    };
  }

  /** Mirrors OMP's builtin `/switch`: the selector resolves against the catalog or is rejected. */
  async switchModel(selector: string): Promise<WorkerModelSwitch> {
    this.switchDispatches.push(selector);
    const resolved = this.resolveModel(selector);
    if (resolved === null) return { ok: false };
    this.switchedModel = resolved;
    return { ok: true, model: resolved.model, level: resolved.level };
  }
  async goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> {
    this.goalOperations.push(op);
    const current = this.goalState.value;
    if (op === "get" || current.goal === null) return current;
    const goal: WorkerGoal = {
      ...current.goal,
      status: op === "pause" ? "paused" : "dropped",
    };
    this.goalState.value = fakeGoalResult(goal);
    return this.goalState.value;
  }
  async btw(question: string, signal?: AbortSignal): Promise<string> {
    const record: FakeBtwRunContext = {
      question,
      sessionId: this.sessionId,
      sessionFile: this.sessionFile,
      cwd: this.currentCwd,
      signal,
    };
    this.recordBtw(record);
    return this.btwHandler(record);
  }

  async runGoal(
    request: WorkerGoalRun,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<WorkerGoalResult> {
    this.recordGoalRun({
      request: { ...request },
      sessionId: this.sessionId,
      promptContext: context,
    });
    this.goalRunInFlight = true;
    try {
      const result = await this.goalHandler({
        request,
        sessionId: this.sessionId,
        promptContext: context,
        current: this.goalState.value,
        emit: event => {
          if (event.type === "goal_updated") {
            this.goalState.value = fakeGoalResult(event.goal);
          }
          onEvent(event);
        },
        appended: () => [...this.appendedMessages],
        waitForAppend: () => this.takeAppend(),
        waitForAbort: () => this.takeAbort(),
      });
      const latestGoal = this.goalState.value.goal;
      if (
        result.goal !== null
        && latestGoal?.id === result.goal.id
        && (latestGoal.status === "paused" || latestGoal.status === "dropped")
        && result.goal.status === "active"
      ) {
        return result;
      }
      this.goalState.value = result;
      return result;
    } finally {
      this.goalRunInFlight = false;
    }
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
      // Mirrors the live turn boundary the real worker reports at `agent_start`: the fixture's
      // prompt callback stands for the agent turn, so the acceptance is reported before it runs.
      onEvent({ type: "prompt_accepted" });
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

  /**
   * Mirrors `RpcWorker.resume`: dispatch OMP's own `/retry` in this session and, when the retry
   * completes locally (nothing to retry), send one same-session continuation instruction. The
   * retry dispatch's local completion is internal and never reaches the caller's event stream.
   */
  async resume(onEvent: (event: WorkerEvent) => void, promptContext?: PromptContext): Promise<void> {
    let localOnly = false;
    await this.prompt(
      "/retry",
      event => {
        if (event.type === "local_command") {
          localOnly = true;
          return;
        }
        onEvent(event);
      },
      promptContext,
    );
    if (localOnly) await this.prompt(RESUME_CONTINUATION_PROMPT, onEvent, promptContext);
  }
  async steer(text: string): Promise<void> {
    const failure = this.steerFailure();
    if (failure) throw failure;
    if (!this.promptInFlight && !this.goalRunInFlight) {
      throw new Error("fixture worker is not streaming a turn");
    }
    if (this.stopped) throw new Error("fixture worker is stopped");
    this.steerCalls.push(text);
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
    let rejectStopped!: (error: Error) => void;
    const stopped = new Promise<never>((_resolve, reject) => {
      rejectStopped = reject;
    });
    const onStopped = (error: Error): void => rejectStopped(error);
    this.steerStopWaiters.add(onStopped);
    try {
      await Promise.race([this.beforeSteer(text), stopped]);
    } finally {
      this.steerStopWaiters.delete(onStopped);
    }
  }

  async abortTurn(): Promise<void> {
    if (!this.promptInFlight && !this.goalRunInFlight) {
      throw new Error("fixture worker has no prompt or goal run in progress");
    }
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
    await this.waitBeforeStop(this.sessionId);
    this.stopped = true;
    for (const reject of this.steerStopWaiters) reject(new Error("fixture worker stopped during steer"));
    this.steerStopWaiters.clear();
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
  /** Options every creation was asked for, in call order, for model and target assertions. */
  readonly creations: WorkerOptions[] = [];
  readonly goalRuns: FakeGoalRunRecord[] = [];
  readonly btwCalls: FakeBtwRunContext[] = [];
  /** A narrow shutdown seam for tests that need to hold the lease cleanup window open. */
  stopHandler: ((sessionId: string) => void | Promise<void>) | undefined = undefined;
  /** Holds worker creation to exercise commands received before a task's first prompt. */
  beforeCreate: (() => Promise<void>) | undefined;
  beforeSteer: ((text: string) => Promise<void>) | undefined;
  /** The native structured-goal behavior a test asks this fixture to perform. */
  goalHandler: FakeGoalHandler = async ({ request, current, emit, sessionId }) => {
    if (request.op === "create") {
      if (current.goal !== null) return current;
      const goal: WorkerGoal = {
        id: `fixture-goal-${sessionId}`,
        objective: request.objective,
        status: "active",
        tokensUsed: 0,
        timeUsedSeconds: 0,
        ...(request.tokenBudget === undefined ? {} : { tokenBudget: request.tokenBudget }),
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      return fakeGoalResult(goal);
    }
    if (
      current.goal === null
      || (request.op === "recover" && current.goal.id !== request.goalId)
    ) {
      return current;
    }
    const goal = request.op === "resume"
      ? { ...current.goal, status: "active" as const }
      : current.goal;
    emit({ type: "goal_accepted", goalId: goal.id });
    emit({ type: "goal_updated", goal });
    emit({ type: "prompt_accepted" });
    return fakeGoalResult(goal);
  };
  btwHandler: FakeBtwHandler = async ({ question }) => `Fixture BTW answer: ${question}`;
  private readonly goalRunWaiters: Array<{
    count: number;
    resolve: () => void;
  }> = [];
  private readonly goalStates = new Map<string, FixtureGoalState>();

  /**
   * Catalog fixture workers report for `/switch`. A worker created with `WorkerOptions.model`
   * reports that model as `current`, mirroring OMP's `--model` startup; a successful `/switch`
   * dispatch moves the session instead.
   */
  catalog: WorkerModelCatalog = {
    current: "fixture/default",
    level: null,
    available: [
      { provider: "fixture", id: "default", name: "Fixture Default" },
      { provider: "fixture", id: "fast", name: "Fixture Fast" },
      { provider: "other", id: "fast", name: "Other Fast" },
    ],
  };

  /** Role aliases the fixture `/switch` resolves, standing in for OMP's `settings.modelRoles`. */
  roles: Record<string, { model: string; level: string | null }> = {
    fast: { model: "other/fast", level: "max" },
    default: { model: "fixture/default", level: null },
  };
  /** When set, `create` rejects with this error instead of materializing a worker. */
  createFailure: Error | undefined;
  /** When set, every worker's `models()` rejects with this error, mirroring a dead RPC worker. */
  modelsFailure: Error | undefined;
  contextUsage: Omit<WorkerContextUsage, "sessionId" | "sessionFile"> = {
    tokens: 4_096,
    contextWindow: 8_192,
    percent: 50,
    model: null,
  };
  /** When set, the next attempted worker steer rejects before appending its message. */
  steerFailure: Error | undefined;
  /**
   * What `recover` reports for a persisted worker process: `true` means the old process is
   * confirmed stopped, so its directory is free again. The default `false` keeps the directory
   * blocked as unverified, which is what fail-closed recovery tests need.
   */
  recovered = false;

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

  /** Passing `undefined` clears the failure so later creates succeed again. */
  failCreateWith(error?: Error): void {
    this.createFailure = error;
  }

  /** Passing `undefined` clears the failure so `models()` reports the catalog again. */
  failModelsWith(error?: Error): void {
    this.modelsFailure = error;
  }
  /** Passing `undefined` clears the failure so worker steering appends normally. */
  failSteerWith(error?: Error): void {
    this.steerFailure = error;
  }

  async create(options: WorkerOptions): Promise<OmpWorker> {
    this.creations.push(options);
    if (this.createFailure) throw this.createFailure;
    await this.beforeCreate?.();
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
      () => this.catalog,
      selector => resolveFixtureModel(selector, this.roles, this.catalog),
      () => this.modelsFailure,
      (sessionId) => this.goalStateFor(sessionId),
      (context) => this.goalHandler(context),
      (record) => this.recordGoalRun(record),
      async sessionId => {
        await this.stopHandler?.(sessionId);
      },
      () => {
        const failure = this.steerFailure;
        this.steerFailure = undefined;
        return failure;
      },
      async (text) => {
        await this.beforeSteer?.(text);
      },
      (context) => this.btwHandler(context),
      (record) => {
        this.btwCalls.push(record);
      },
      () => this.contextUsage,
    );
    this.workers.push(worker);
    return worker;
  }
  async createCommit(_options: CommitWorkerOptions): Promise<CommitWorker> {
    throw new Error("FakeWorkerFactory does not implement native commit workers");
  }
  async recover(_process: WorkerProcess): Promise<boolean> {
    return this.recovered;
  }

  /** Simulates native goal state that has not yet reached the channel's progress projection. */
  setNativeGoal(sessionId: string, goal: WorkerGoal | null): void {
    this.goalStateFor(sessionId).value = fakeGoalResult(goal);
  }

  private goalStateFor(sessionId: string): FixtureGoalState {
    let state = this.goalStates.get(sessionId);
    if (state === undefined) {
      state = { value: { goal: null, state: null } };
      this.goalStates.set(sessionId, state);
    }
    return state;
  }

  private recordGoalRun(record: FakeGoalRunRecord): void {
    this.goalRuns.push(record);
    for (const waiter of this.goalRunWaiters) {
      if (this.goalRuns.length >= waiter.count) waiter.resolve();
    }
  }

  waitForGoalRunCount(count: number): Promise<void> {
    if (this.goalRuns.length >= count) return Promise.resolve();
    const { promise, resolve } = Promise.withResolvers<void>();
    this.goalRunWaiters.push({ count, resolve });
    return promise;
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
  readonly worktreePort: WorktreePort | undefined;
  readonly loop: LoopReadPort | undefined;
  readonly service: ChannelService;
}

/** Records isolate requests and materializes a directory per thread under the harness root. */
export class FakeWorktrees implements WorktreePort {
  readonly requests: ThreadWorkspaceRequest[] = [];
  /** When set, `create` rejects with this error instead of returning a workspace. */
  failure: Error | undefined;
  /** Merge requests recorded by `merge`; lifecycle callbacks model a process that exits before its result. */
  readonly merges: Omit<WorktreeMergeRequest, "lifecycle">[] = [];
  /** Removal requests recorded by `remove`. */
  readonly removals: WorktreeRemovalRequest[] = [];
  /** The probe answer `worktrunk()` reports; tests can model an unavailable wt. */
  probe: WorktrunkProbe = { available: true, command: "wt" };
  /** When set, `merge` rejects with this error instead of returning a summary. */
  mergeFailure: Error | undefined;
  /** When set, `remove` rejects with this error instead of reporting an outcome. */
  removeFailure: Error | undefined;
  /** What `remove` reports; `missing` models a workspace that is already gone. */
  removal: WorktreeRemovalOutcome = { kind: "removed", via: "wt", branchOutcome: "deleted" };
  /** Explicit plan fixture: omitted targets resolve to this wt-default branch. */
  mergeDefaultTarget = "main";
  /** Explicit checked-out target path; absent means the target has no other checkout. */
  mergeTargetPath: string | undefined;
  /** When set, `planMerge` rejects instead of producing a target path plan. */
  mergePlanFailure: Error | undefined;

  constructor(private readonly root: string) {}

  async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    const { onPlanned, ...recordedRequest } = request;
    this.requests.push({ ...recordedRequest });
    if (this.failure) throw this.failure;
    const cwd = request.path ?? join(this.root, "worktrees", request.threadId);
    const branch = request.threadId;
    const repo = await realpath(request.baseDir);
    onPlanned?.({ cwd, branch, repo });
    await mkdir(cwd, { recursive: true });
    return { cwd: await realpath(cwd), branch };
  }

  async planMerge(request: WorktreeMergeRequest): Promise<WorktreeMergePlan> {
    if (this.mergePlanFailure) throw this.mergePlanFailure;
    const sourcePath = await realpath(request.cwd);
    const cwds = [sourcePath];
    if (this.mergeTargetPath !== undefined) {
      const targetPath = await realpath(this.mergeTargetPath);
      if (!cwds.includes(targetPath)) cwds.push(targetPath);
    }
    return { target: request.target ?? this.mergeDefaultTarget, cwds };
  }

  async merge(request: WorktreeMergeRequest): Promise<WorktreeMergeSummary> {
    const { lifecycle, ...recordedRequest } = request;
    this.merges.push({ ...recordedRequest });
    if (lifecycle) {
      lifecycle.beforeSpawn(request.cwd);
      lifecycle.onSpawn({
        pid: 0x7fff_ffff,
        pgid: 0x7fff_ffff,
        identity: "pid=2147483647;start=fixture",
        cwd: request.cwd,
      });
      lifecycle.onConfirmedExit();
    }
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
export class FakeLoopReadPort implements LoopReadPort {
  readonly workspaces = new Map<string, LoopWorkspace>();
  readonly tasks = new Map<string, LoopTask[]>();
  readonly resolutionCalls: string[] = [];
  readonly taskListCalls: string[] = [];
  readonly taskDetailCalls: Array<{ workspaceId: string; taskId: string }> = [];
  resolveFailure: Error | undefined;
  listFailure: Error | undefined;
  beforeListTasks: ((workspaceId: string, signal?: AbortSignal) => Promise<void>) | undefined;
  afterListTasksSnapshot: ((workspaceId: string, tasks: readonly LoopTask[]) => Promise<void>) | undefined;

  async resolveWorkspace(selector: string, signal?: AbortSignal): Promise<LoopWorkspace> {
    if (signal?.aborted) throw signal.reason ?? new Error("fixture Loop read aborted");
    this.resolutionCalls.push(selector);
    if (this.resolveFailure) throw this.resolveFailure;
    const matches = [...this.workspaces.values()].filter((workspace) =>
      workspace.workspaceId === selector || workspace.name === selector || workspace.slug === selector);
    if (matches.length !== 1) {
      throw new Error(matches.length === 0 ? "fixture workspace not found" : "fixture workspace ambiguous");
    }
    return { ...matches[0]! };
  }

  async listTasks(workspaceId: string, signal?: AbortSignal): Promise<LoopTask[]> {
    if (signal?.aborted) throw signal.reason ?? new Error("fixture Loop read aborted");
    this.taskListCalls.push(workspaceId);
    await this.beforeListTasks?.(workspaceId, signal);
    if (signal?.aborted) throw signal.reason ?? new Error("fixture Loop read aborted");
    if (this.listFailure) throw this.listFailure;
    const snapshot = (this.tasks.get(workspaceId) ?? []).map((task) => ({ ...task }));
    await this.afterListTasksSnapshot?.(workspaceId, snapshot);
    return snapshot;
  }

  async getTask(workspaceId: string, taskId: string, signal?: AbortSignal): Promise<LoopTask> {
    if (signal?.aborted) throw signal.reason ?? new Error("fixture Loop read aborted");
    this.taskDetailCalls.push({ workspaceId, taskId });
    const task = this.tasks.get(workspaceId)?.find((candidate) => candidate.taskId === taskId);
    if (!task) throw new Error("fixture task not found");
    return { ...task };
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
  /** Inbound attachment configuration; absent means the deployment has no attachment pipeline. */
  attachments?: AttachmentConfig;
  /** Group history configuration; absent means the service default (disabled). */
  groupHistory?: GroupHistoryConfig;
  /** `ask` wait configuration; absent means the service default (6-hour timeout). */
  ask?: AskConfig;
  /** Extra command handlers, e.g. an observing handler that still runs a prompt. */
  commandHandlers?: readonly ChannelCommandHandler[];
  restart?: RestartPort;
  /** Isolated thread workspaces; `null` omits the port so isolate reports an unconfigured deployment. */
  worktrees?: FakeWorktrees | null;
  /** Injects a real or instrumented host WorktreePort in integration tests. */
  worktreePort?: WorktreePort;
  /** Per-bot octo-cli credential port; absent means the deployment has no such capability. */
  cliCredentials?: CliCredentialPort;
  /** Allowlist mirror into the config file; absent means a hand-built config with no path. */
  allowlistWriteback?: AllowlistWriteback;
  loop?: LoopReadPort;
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
  const worktreePort =
    options.worktrees === null
      ? undefined
      : (options.worktreePort ?? options.worktrees ?? new FakeWorktrees(root));
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
    ...(options.attachments === undefined ? {} : { attachments: options.attachments }),
    ...(options.groupHistory === undefined ? {} : { groupHistory: options.groupHistory }),
    ...(options.ask === undefined ? {} : { ask: options.ask }),
  } as CoreServiceConfig;
  const service = await createChannelService({
    config,
    transport,
    ...(options.loop === undefined ? {} : { loop: options.loop }),
    workerFactory: workers,
    ...(worktreePort === undefined ? {} : { worktrees: worktreePort }),
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
    worktrees: worktreePort instanceof FakeWorktrees ? worktreePort : undefined,
    worktreePort,
    loop: options.loop,
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
