import { randomUUID } from "node:crypto";
import { stat } from "node:fs/promises";
import type { ChannelTransport, ChatTarget, TextEvent } from "./contracts.ts";
import { asAppError, failureText, type AppError } from "./errors.ts";
import type { ChannelStore } from "./store.ts";
import type { OutboxRecord } from "./store-records.ts";
import { streamTargetKey, targetKey } from "./target.ts";

const MAX_TEXT_BYTES = 3_000;
/** 同一条文本流两次编辑之间的最小间隔，合并高频快照。 */
const TEXT_MERGE_MS = 1_000;
/**
 * 单条投递在放弃（`dead`）前的最大尝试次数：到点就把队首让给后面的消息，一条投不出去的
 * 消息不能让整个聊天永久静音。预算按当前内容的连续失败次数计——成功投递或内容更新都会
 * 清零 `attempts`，因此流式消息不会把历史失败攒成一次误判。
 */
const MAX_OUTBOX_ATTEMPTS = 6;
/** 投递重试退避：1s 起、翻倍、上限 30s，向下抖动（与传输层重连同口径）。 */
const OUTBOX_RETRY_BASE_MS = 1_000;
const OUTBOX_RETRY_MAX_MS = 30_000;
const OUTBOX_RETRY_JITTER_MIN = 0.75;

/**
 * 分段断点候选，层级从高到低：换行 > 句末标点 > 逗号/顿号 > 空白，都没有才在窗口末尾硬切。
 * ASCII 标点只有后面是空白或文本结尾才算断点，免得小数、URL 和缩写被当成断点。
 */
const SENTENCE_CUTS: Record<string, true> = { "。": true, "！": true, "？": true, "；": true, "…": true };
const CLAUSE_CUTS: Record<string, true> = { "，": true, "、": true, ",": true, ";": true };
const WORD_CUTS: Record<string, true> = { " ": true, "\t": true };
const ASCII_CUTS: Record<string, true> = { ".": true, "!": true, "?": true, ",": true, ";": true };
const SPACE_CHARS: Record<string, true> = { " ": true, "\t": true, "\n": true, "\r": true };

/** 每个 code point 的 UTF-8 字节数；不落地编码结果，避免逐字符分配。 */
function utf8ByteLength(codePoint: number): number {
  if (codePoint < 0x80) return 1;
  if (codePoint < 0x800) return 2;
  if (codePoint < 0x10000) return 3;
  return 4;
}

/** 在 `cut` 处断开的层级：0 不是断点，1 换行，2 句末标点，3 逗号/顿号，4 空白。 */
function cutTier(characters: readonly string[], cut: number): number {
  const character = characters[cut - 1] as string;
  if (character === "\n") return 1;
  const following = characters[cut];
  if (
    ASCII_CUTS[character] === true &&
    following !== undefined &&
    SPACE_CHARS[following] !== true
  ) {
    return 0;
  }
  if (SENTENCE_CUTS[character] === true) return 2;
  if (CLAUSE_CUTS[character] === true) return 3;
  if (WORD_CUTS[character] === true) return 4;
  return 0;
}

/** 在 `(start, end]` 里取层级最高、同层最靠后的断点；没有断点时返回窗口末尾。 */
function findCut(characters: readonly string[], start: number, end: number): number {
  let sentence = -1;
  let clause = -1;
  let word = -1;
  for (let cut = end; cut > start; cut -= 1) {
    const tier = cutTier(characters, cut);
    if (tier === 0) continue;
    if (tier === 1) return cut;
    if (tier === 2 && sentence < 0) sentence = cut;
    else if (tier === 3 && clause < 0) clause = cut;
    else if (tier === 4 && word < 0) word = cut;
  }
  if (sentence > 0) return sentence;
  if (clause > 0) return clause;
  if (word > 0) return word;
  return end;
}

/**
 * 按 UTF-8 字节上限贪心分段：剩余内容放得下就是当前最后一段（流式增量继续编辑它），
 * 放不下就在恰好放得下的窗口内取最后一个自然断点——换行、句末标点、逗号/顿号、
 * 空白——都没有才在窗口末尾硬切。断点字符归上一段，其后的空白留在下一段开头，
 * 不修剪也不吞并。每段的边界只由它自己的窗口决定，所以已切出的段不随流式增量重排。
 */
export function splitUtf8(text: string, maxBytes = MAX_TEXT_BYTES): string[] {
  if (text.length === 0) return [""];
  const characters = Array.from(text);
  const chunks: string[] = [];
  let start = 0;
  while (start < characters.length) {
    let bytes = 0;
    let end = start;
    while (end < characters.length) {
      const size = utf8ByteLength(characters[end]?.codePointAt(0) ?? 0);
      if (bytes + size > maxBytes) break;
      bytes += size;
      end += 1;
    }
    if (end === characters.length) {
      chunks.push(characters.slice(start).join(""));
      break;
    }
    const cut = end > start ? findCut(characters, start, end) : start + 1;
    chunks.push(characters.slice(start, cut).join(""));
    start = cut;
  }
  return chunks;
}

function isPermanentOutboxError(error: unknown, item: OutboxRecord): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as {
    code?: unknown;
    permanent?: unknown;
    status?: unknown;
  };
  // bot API 把内部失败（含瞬时 500）包在线缆 400 上：机器码比状态码更可信。
  if (candidate.code === "err.server.bot_api.store_failed") return false;
  if (candidate.permanent === true) return true;
  if (
    typeof candidate.status === "number" &&
    candidate.status >= 400 &&
    candidate.status < 500 &&
    ![408, 409, 425, 429].includes(candidate.status)
  ) {
    return true;
  }
  return item.kind === "file" && candidate.code === "ENOENT";
}

/**
 * 出站投递需要的那部分服务状态。队列状态、节流计时器与在途尝试都留在本模块内；服务只经
 * `Outbox` 入队与排空，文字回复、流式编辑与文件投递因此共用一条通道。
 */
export interface OutboxDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  botUid(): string | undefined;
  /** 只有已启动的服务才投递；未启动时入队内容留给下一个进程恢复。 */
  started(): boolean;
}

export interface Outbox {
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  enqueueStreamText(target: ChatTarget, taskId: string, event: TextEvent): void;
  flush(target: ChatTarget, announceUnknown?: boolean): Promise<void>;
  flushAll(): void;
  /** 注册一个任务的文本投递取消句柄：任务被中断时中止其所有在途尝试。 */
  registerTask(taskId: string): { stop(): void };
  /** 丢弃任务的取消句柄（任务结束时调用，无需中止）。 */
  forgetTask(taskId: string): void;
  /** 任务不再有排队内容时释放其取消句柄。 */
  releaseIfIdle(taskId: string): void;
  /** 停机第一步：中止所有在途文本尝试。 */
  abortAll(): void;
  /** 停机第二步：清掉重试与流式计时器。 */
  stopTimers(): void;
  /** 停机第三步：等待在途排空结束。 */
  settle(): Promise<void>;
}

export function createOutboxDelivery(deps: OutboxDeps): Outbox {
  const outboxFlushes = new Map<string, Promise<void>>();
  const outboxActive = new Set<string>();
  const retryTimers = new Map<string, NodeJS.Timeout>();
  const streamTimers = new Map<string, NodeJS.Timeout>();
  const streamNotBefore = new Map<string, number>();
  const textAbortControllers = new Map<string, AbortController>();
  const textAttempts = new Set<AbortController>();

  function currentBotUid(): string {
    const uid = deps.botUid();
    if (uid === undefined) throw new Error("Bot 身份未注册");
    return uid;
  }

  function releaseTextAbortIfIdle(taskId: string): void {
    if (!deps.botUid()) return;
    const task = deps.store.getTask(taskId);
    if (
      !task ||
      task.state === "queued" ||
      task.state === "running" ||
      deps.store.listPendingOutbox(currentBotUid()).some((item) => item.taskId === taskId)
    ) {
      return;
    }
    textAbortControllers.delete(taskId);
    const streamMarker = `:${taskId}:`;
    for (const [key, timer] of streamTimers) {
      if (!key.includes(streamMarker)) continue;
      clearTimeout(timer);
      streamTimers.delete(key);
    }
    for (const key of streamNotBefore.keys()) {
      if (key.includes(streamMarker)) streamNotBefore.delete(key);
    }
  }

  function enqueueStreamText(target: ChatTarget, taskId: string, event: TextEvent): void {
    if (!deps.botUid()) return;
    const streamMessageId = `${taskId}:${event.messageId}`;
    const previous = deps.store.listTextStream(currentBotUid(), taskId, streamMessageId);
    const version = previous.reduce(
      (highest, item) => Math.max(highest, item.desiredVersion),
      0,
    ) + 1;
    const chunks = event.text.length === 0 ? [] : splitUtf8(event.text);
    const segmentCount = Math.max(previous.length, chunks.length);
    let changed = false;
    for (let segmentIndex = 0; segmentIndex < segmentCount; segmentIndex += 1) {
      const chunk = chunks[segmentIndex] ?? "";
      const existing = previous[segmentIndex];
      if (existing) {
        if (existing.text === chunk) continue;
        if (deps.store.updateTextOutbox(existing.id, chunk, version)) changed = true;
        continue;
      }
      if (chunk.length === 0) continue;
      deps.store.enqueueOutbox({
        id: randomUUID(),
        botUid: currentBotUid(),
        channelId: target.channelId,
        channelType: target.channelType,
        taskId,
        kind: "text",
        text: chunk,
        path: null,
        fileKind: null,
        clientMsgNo: randomUUID(),
        streamMessageId,
        segmentIndex,
        desiredVersion: version,
        ackedVersion: 0,
      });
      changed = true;
    }
    if (!changed) return;
    if (previous.length > 0) {
      scheduleStreamFlush(target, streamMessageId);
    } else {
      void flushTarget(target).catch(() => {});
    }
  }

  function scheduleStreamFlush(target: ChatTarget, streamMessageId: string): void {
    const key = streamTargetKey(target, streamMessageId);
    if (streamTimers.has(key) || !deps.started()) return;
    const notBefore = streamNotBefore.get(key) ?? Date.now();
    const timer = setTimeout(() => {
      streamTimers.delete(key);
      void flushTarget(target).catch(() => {});
    }, Math.max(0, notBefore - Date.now()));
    streamTimers.set(key, timer);
  }

  function canSendStreamEdit(item: OutboxRecord, target: ChatTarget): boolean {
    if (!item.streamMessageId || !item.remoteMessageId) return true;
    const key = streamTargetKey(target, item.streamMessageId);
    const notBefore = streamNotBefore.get(key) ?? 0;
    if (Date.now() >= notBefore) return true;
    scheduleStreamFlush(target, item.streamMessageId);
    return false;
  }

  function markStreamEditStarted(item: OutboxRecord, target: ChatTarget): void {
    if (!item.streamMessageId) return;
    const key = streamTargetKey(target, item.streamMessageId);
    streamNotBefore.set(
      key,
      Math.max(streamNotBefore.get(key) ?? 0, Date.now() + TEXT_MERGE_MS),
    );
  }

  function enqueueText(target: ChatTarget, text: string, taskId: string | null): void {
    for (const chunk of splitUtf8(text)) {
      deps.store.enqueueOutbox({
        id: randomUUID(),
        botUid: currentBotUid(),
        channelId: target.channelId,
        channelType: target.channelType,
        taskId,
        kind: "text",
        text: chunk,
        path: null,
        fileKind: null,
        clientMsgNo: randomUUID(),
      });
    }
    flushTarget(target);
  }

  function flushAllOutbox(): void {
    if (!deps.botUid()) return;
    for (const target of deps.store.listPendingOutboxTargets(currentBotUid())) {
      const key = targetKey(target);
      clearTimeout(retryTimers.get(key));
      retryTimers.delete(key);
      const flush = flushTarget(target, true);
      void flush.then(
        () => {
          void flushTarget(target).catch(() => {});
        },
        () => {
          void flushTarget(target).catch(() => {});
        },
      );
    }
  }
  function flushTarget(target: ChatTarget, announceUnknown = false): Promise<void> {
    if (!deps.botUid() || !deps.started()) return Promise.resolve();
    const key = targetKey(target);
    if (outboxActive.has(key)) return outboxFlushes.get(key) ?? Promise.resolve();
    outboxActive.add(key);
    const drain = flushTargetInternal(target, announceUnknown, key);
    outboxFlushes.set(key, drain);
    void drain.catch(() => {}).then(() => {
      if (outboxFlushes.get(key) === drain) outboxFlushes.delete(key);
    });
    return drain;
  }

  async function flushTargetInternal(
    target: ChatTarget,
    announceUnknown: boolean,
    key: string,
  ): Promise<void> {
    let unknownNoticeSent = false;
    try {
      while (deps.started() && deps.botUid()) {
        const item = deps.store.listPendingOutbox(currentBotUid(), target)[0];
        if (!item) return;
        if (
          item.streamMessageId &&
          !item.remoteMessageId &&
          item.state === "pending" &&
          item.attempts === 0 &&
          (item.text ?? "") === ""
        ) {
          deps.store.acknowledgeTextOutbox(item.id, item.desiredVersion, null);
          if (item.taskId) releaseTextAbortIfIdle(item.taskId);
          continue;
        }
        if (
          item.streamMessageId &&
          item.remoteMessageId &&
          !canSendStreamEdit(item, target)
        ) {
          return;
        }
        if (
          announceUnknown &&
          item.state === "unknown" &&
          !item.streamMessageId &&
          !unknownNoticeSent
        ) {
          unknownNoticeSent = true;
          await sendUnknownNotice(target, item);
        }
        let attempted = item;
        try {
          const delivery = await sendOutbox(item, target);
          if (item.streamMessageId) {
            if (delivery.reconcile && delivery.remoteMessageId) {
              deps.store.bindTextOutbox(item.id, delivery.remoteMessageId);
              const bound = deps.store.getOutbox(item.id);
              if (bound?.remoteMessageId) {
                attempted = bound;
                const correction = await sendOutbox(bound, target);
                deps.store.acknowledgeTextOutbox(
                  bound.id,
                  correction.attemptedVersion,
                  correction.remoteMessageId,
                );
              }
              if (item.taskId) releaseTextAbortIfIdle(item.taskId);
              if (
                deps.store.listPendingOutbox(currentBotUid(), target).some(
                  (pending) => pending.streamMessageId === item.streamMessageId,
                )
              ) {
                scheduleStreamFlush(target, item.streamMessageId);
              }
              continue;
            }
            deps.store.acknowledgeTextOutbox(item.id, delivery.attemptedVersion, delivery.remoteMessageId);
            if (item.taskId) releaseTextAbortIfIdle(item.taskId);
          } else {
            deps.store.markOutboxSent(item.id);
            if (item.taskId) releaseTextAbortIfIdle(item.taskId);
          }
        } catch (error) {
          // Content updates reset the budget; an older in-flight failure must not spend it.
          if (attempted.streamMessageId) {
            const latest = deps.store.getOutbox(attempted.id);
            if (latest && latest.desiredVersion > attempted.desiredVersion) continue;
          }
          const reason = failureText(error, "transport unavailable; delivery is unknown");
          if (item.streamMessageId) {
            deps.store.failTextOutbox(item.id, attempted.desiredVersion, reason);
          } else {
            deps.store.markOutboxFailure(item.id, reason);
          }
          const attempts = attempted.attempts + 1;
          // 永久失败或重试预算耗尽：标记 dead 后继续排空队尾，不把后面的消息一起拖住。
          if (isPermanentOutboxError(error, item) || attempts >= MAX_OUTBOX_ATTEMPTS) {
            console.error(
              `[outbox] 放弃投递 ${item.kind} ${item.id} target=${key} attempts=${attempts}：${reason}`,
            );
            deps.store.markOutboxDead(item.id, reason);
            if (item.kind === "file") {
              enqueueText(target, "文件发送失败，未阻塞后续文字回复。", item.taskId);
            }
            continue;
          }
          scheduleOutboxRetry(target, attempts);
          return;
        }
      }
    } finally {
      // Release the drain slot synchronously, before this promise settles: an item enqueued
      // after the last queue read must start a fresh drain instead of waiting for an unrelated
      // flush trigger.
      outboxActive.delete(key);
    }
  }

  function scheduleOutboxRetry(target: ChatTarget, attempts: number): void {
    const key = targetKey(target);
    if (retryTimers.has(key) || !deps.started()) return;
    const exponential = Math.min(
      OUTBOX_RETRY_MAX_MS,
      OUTBOX_RETRY_BASE_MS * 2 ** Math.max(0, attempts - 1),
    );
    const delay = Math.round(
      exponential * (OUTBOX_RETRY_JITTER_MIN + Math.random() * (1 - OUTBOX_RETRY_JITTER_MIN)),
    );
    const timer = setTimeout(() => {
      retryTimers.delete(key);
      flushTarget(target);
    }, delay);
    retryTimers.set(key, timer);
  }

  async function sendUnknownNotice(target: ChatTarget, item: OutboxRecord): Promise<void> {
    try {
      await deps.transport.sendText(
        target,
        "上一条消息送达状态未知，正在重试。",
        `delivery-unknown-${item.id}`,
      );
    } catch {
      // The notice is intentionally best effort and never creates another outbox item.
    }
  }

  async function withTextAttempt<T>(
    taskId: string | null,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const attempt = new AbortController();
    const parent = taskId ? textAbortControllers.get(taskId) : undefined;
    const abort = () => attempt.abort();
    if (parent?.signal.aborted) attempt.abort();
    else parent?.signal.addEventListener("abort", abort, { once: true });
    textAttempts.add(attempt);
    try {
      return await work(attempt.signal);
    } finally {
      textAttempts.delete(attempt);
      parent?.signal.removeEventListener("abort", abort);
    }
  }

  async function sendOutbox(
    item: OutboxRecord,
    target: ChatTarget,
  ): Promise<{ remoteMessageId: string | null; reconcile: boolean; attemptedVersion: number }> {
    if (item.kind === "file") {
      if (!item.path || !item.fileKind) throw new Error("invalid file outbox item");
      try {
        await stat(item.path);
      } catch (error) {
        if (asAppError(error).code === "ENOENT") {
          const missing = new Error("outbox file spool is missing") as AppError & { permanent?: boolean };
          missing.code = "ENOENT";
          missing.permanent = true;
          throw missing;
        }
        throw error;
      }
      await deps.transport.sendFile(target, item.path, item.fileKind, item.clientMsgNo);
      return { remoteMessageId: null, reconcile: false, attemptedVersion: 0 };
    }
    if (item.streamMessageId) {
      if (item.remoteMessageId) {
        markStreamEditStarted(item, target);
        await withTextAttempt(item.taskId, (signal) =>
          deps.transport.editText(target, item.remoteMessageId!, item.text ?? "", signal),
        );
        return {
          remoteMessageId: item.remoteMessageId,
          reconcile: false,
          attemptedVersion: item.desiredVersion,
        };
      }
      const reconcile = item.state === "unknown";
      if (!reconcile) deps.store.beginTextCreate(item.id);
      markStreamEditStarted(item, target);
      const receipt = await withTextAttempt(item.taskId, (signal) =>
        deps.transport.sendText(target, item.text ?? "", item.clientMsgNo, signal),
      );
      if (!receipt?.messageId) throw new Error("stream text send did not return a remote message id");
      return { remoteMessageId: receipt.messageId, reconcile, attemptedVersion: item.desiredVersion };
    }
    await deps.transport.sendText(target, item.text ?? "", item.clientMsgNo);
    return { remoteMessageId: null, reconcile: false, attemptedVersion: 0 };
  }


  /** 注册一个任务的文本投递取消句柄：任务被中断时中止其所有在途文本尝试。 */
  function registerTask(taskId: string): { stop(): void } {
    const controller = new AbortController();
    textAbortControllers.set(taskId, controller);
    return { stop: () => controller.abort() };
  }

  /** 丢弃任务的取消句柄（任务结束时调用，无需中止）。 */
  function forgetTask(taskId: string): void {
    textAbortControllers.delete(taskId);
  }

  function abortAll(): void {
    for (const controller of textAbortControllers.values()) controller.abort();
    for (const controller of textAttempts) controller.abort();
  }

  function stopTimers(): void {
    for (const timer of retryTimers.values()) clearTimeout(timer);
    retryTimers.clear();
    for (const timer of streamTimers.values()) clearTimeout(timer);
    streamTimers.clear();
    streamNotBefore.clear();
  }

  async function settle(): Promise<void> {
    await Promise.allSettled([...outboxFlushes.values()]);
  }

  return {
    enqueueText,
    enqueueStreamText,
    flush: flushTarget,
    flushAll: flushAllOutbox,
    registerTask,
    forgetTask,
    releaseIfIdle: releaseTextAbortIfIdle,
    abortAll,
    stopTimers,
    settle,
  };
}
