/**
 * 群/线程历史的写侧所有者：每个聊天的 live 覆盖 buffer、回填取页（含超时 deadline）与
 * ledger 的 prepare/commit 都在这里。窗口选择、渲染与取页规划仍由 `./group-history.ts`
 * 的纯函数完成，本模块只负责把它们落到 store 与 transport。
 *
 * 模块不依赖 `service.ts`：持久化的历史 ledger（`store`）、群历史取页（`transport`）、
 * 群历史配置（`config`）与当前 bot 身份（`botUid`）都由 `CoreService` 以端口注入，并由它
 * 在构造函数里创建一次本协调器。buffer map 属本对象独占，外部只能经 `record`、
 * `resetCoverage`、`attachmentCandidates` 观察，拿不到可变集合。
 */

import type {
  ChannelTransport,
  ChatTarget,
  CoreServiceConfig,
  HistoryMessage,
  InboundMessage,
} from "./contracts.ts";
import {
  GROUP_HISTORY_DEFAULT_MAX_MESSAGES,
  createHistoryBuffer,
  planHistoryFetch,
  renderHistoryBlock,
  selectHistoryWindow,
  type HistoryBuffer,
  type HistoryEntry,
  type PlannedHistoryFetch,
} from "./group-history.ts";
import type { ChannelStore } from "./store.ts";
import type { TaskRecord } from "./store-records.ts";
import { targetKey } from "./target.ts";

/** One sync page answers within this deadline, or the turn degrades instead of blocking. */
const GROUP_HISTORY_FETCH_TIMEOUT_MS = 10_000;

/**
 * Group history injected into one task turn together with the ledger attempt it
 * claims; the attempt is committed once the prompt returns, cancellation included.
 */
export type HistoryTurn = {
  text: string;
  lastSeq: number;
  attemptId: string;
};

/**
 * What the coordinator may touch outside its own buffers: the history ledger rows, one bounded
 * history page, the config that decides whether history applies at all, and the identity those
 * rows are keyed by — a getter because the bot is learned after start.
 */
export interface GroupHistoryDeps {
  readonly store: ChannelStore;
  readonly transport: Pick<ChannelTransport, "fetchGroupMessages">;
  readonly config: CoreServiceConfig["groupHistory"];
  readonly botUid: () => string | undefined;
}

export class GroupHistoryCoordinator {
  private readonly buffers = new Map<string, HistoryBuffer>();

  constructor(private readonly deps: GroupHistoryDeps) {}

  /**
   * Every group/thread message proves coverage before any permission gate; only text
   * from other senders becomes a renderable entry.
   */
  record(message: InboundMessage): void {
    if (this.deps.config?.enabled !== true) return;
    if (message.channelType !== 2 && message.channelType !== 5) return;
    const senderId = message.senderId.trim();
    this.bufferFor(message).record({
      seq: message.messageSeq,
      supported: message.supported,
      senderId,
      text: message.text,
      timestamp: message.messageTimestamp,
      fromBot: senderId.length > 0 && senderId === this.deps.botUid(),
      // Live 归一化已滤掉不可用的相对 URL，这里保留的都是绝对直链。
      attachments: message.attachments.map(({ kind, name, size, url }) => ({ kind, name, size, url })),
    });
  }

  /** A websocket connect/reconnect makes the covered seq range unknown again. */
  resetCoverage(): void {
    for (const buffer of this.buffers.values()) {
      buffer.resetCoverage();
    }
  }

  /**
   * 「这个」的关联候选：只查已有 buffer，没有 live 覆盖的聊天因此不参与推断——服务在调用前
   * 已判定群历史与附件接收开启、消息自身不带附件且是群/线程消息。
   */
  attachmentCandidates(message: InboundMessage): HistoryEntry[] {
    const buffer = this.buffers.get(targetKey(message));
    if (buffer === undefined) return [];
    return buffer.attachmentCandidates(
      message.senderId.trim(),
      message.messageSeq,
      message.messageTimestamp,
    );
  }

  /**
   * Builds the history block for one group/thread turn and claims the ledger attempt that
   * the post-prompt commit closes.  A range the live buffer cannot prove covered is taken
   * from sync; when that sync fails the turn keeps its plain text and the watermark stays
   * put, so the next turn replays the same range instead of silently losing it.
   */
  async prepareTurn(
    task: TaskRecord,
    target: ChatTarget,
    sessionId: string,
  ): Promise<HistoryTurn | null> {
    const config = this.deps.config;
    if (config?.enabled !== true) return null;
    if (task.channelType !== 2 && task.channelType !== 5) return null;
    if (!task.historyEligible || !Number.isInteger(task.messageSeq) || task.messageSeq <= 0) return null;
    const botUid = this.deps.botUid();
    if (botUid === undefined) return null;
    const currentSeq = task.messageSeq;
    // Session identity may have moved (`/new`, external rebinding); drop orphan rows first.
    this.deps.store.pruneHistoryLedger(botUid, target, sessionId);
    const ledger = this.deps.store.getHistoryLedger(botUid, target, sessionId);
    // A committed row's lastSeq is the accounted-through watermark; a pending row still
    // owes the range its previous attempt started from.
    const watermark = ledger === null
      ? null
      : ledger.phase === "pending"
        ? ledger.previousSeq
        : ledger.lastSeq;
    if (watermark !== null && watermark >= currentSeq) return null;
    const buffer = this.bufferFor(target);
    const candidates = buffer.entriesInRange(watermark ?? 0, currentSeq);
    let truncatedEarlier = false;
    if (!buffer.covers(watermark ?? 0, currentSeq)) {
      const plan = planHistoryFetch({
        previousSeq: watermark,
        currentSeq,
        maxMessages: config.maxMessages,
      });
      const page = await this.fetchPage(target, plan);
      if (page === null) return null;
      truncatedEarlier = plan.truncatedEarlier;
      for (const message of page) {
        if (!Number.isInteger(message.seq) || message.seq <= 0 || message.seq >= currentSeq) continue;
        const entry: HistoryEntry = {
          seq: message.seq,
          senderId: message.senderId,
          text: message.text,
          timestamp: message.timestamp,
        };
        if (message.attachments !== undefined) entry.attachments = message.attachments;
        candidates.push(entry);
      }
    }
    const { kept, skipped } = selectHistoryWindow({
      entries: candidates,
      maxMessages: config.maxMessages,
    });
    const block = renderHistoryBlock(kept, { skipped, truncatedEarlier });
    const prepared = this.deps.store.prepareHistoryInjection(botUid, target, sessionId, currentSeq);
    console.error(
      `[group-history] target=${targetKey(target)} seq=${currentSeq}`
      + ` injected=${kept.length} skipped=${skipped}${truncatedEarlier ? " truncated" : ""}`,
    );
    return {
      text: block === null ? task.text : `${block}\n${task.text}`,
      lastSeq: prepared.lastSeq,
      attemptId: prepared.attemptId,
    };
  }

  /**
   * Closes the claimed attempt once the prompt returned.  A steer that landed mid-turn
   * raised the row's lastSeq under the same attempt, so the commit follows it instead of
   * reporting a CAS failure that would replay the steered message on the next turn.
   */
  commitTurn(target: ChatTarget, sessionId: string, turn: HistoryTurn): void {
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    try {
      const current = this.deps.store.getHistoryLedger(botUid, target, sessionId);
      const lastSeq = current === null ? turn.lastSeq : Math.max(turn.lastSeq, current.lastSeq);
      if (this.deps.store.commitHistoryInjection(botUid, target, sessionId, lastSeq, turn.attemptId)) return;
      console.error(`[group-history] commit rejected target=${targetKey(target)} seq=${lastSeq}`);
    } catch (error) {
      console.error(
        `[group-history] commit failed target=${targetKey(target)}:`
        + ` ${error instanceof Error ? error.message : "unknown error"}`,
      );
    }
  }

  /**
   * A steered message is already inside the running turn, so it is accounted for directly.
   * The session identity comes from the caller: the coordinator never sees a runtime task.
   */
  recordSteered(message: InboundMessage, sessionId: string): void {
    if (this.deps.config?.enabled !== true) return;
    if (sessionId.length === 0) return;
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    this.deps.store.advanceHistoryLedger(botUid, message, sessionId, message.messageSeq);
  }

  /** Live per-chat coverage buffer; chats without one simply have no coverage history. */
  private bufferFor(target: ChatTarget): HistoryBuffer {
    const key = targetKey(target);
    const existing = this.buffers.get(key);
    if (existing) return existing;
    const created = createHistoryBuffer(
      this.deps.config?.maxMessages ?? GROUP_HISTORY_DEFAULT_MAX_MESSAGES,
    );
    this.buffers.set(key, created);
    return created;
  }

  /** One bounded sync page; `null` marks a degraded turn that must not advance the watermark. */
  private async fetchPage(
    target: ChatTarget,
    plan: PlannedHistoryFetch,
  ): Promise<HistoryMessage[] | null> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), GROUP_HISTORY_FETCH_TIMEOUT_MS);
    try {
      return await this.deps.transport.fetchGroupMessages(target, {
        sinceSeq: plan.sinceSeq,
        untilSeq: plan.untilSeq,
        limit: plan.limit,
        signal: controller.signal,
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : "fetch failed";
      console.error(
        `[group-history] sync failed target=${targetKey(target)} since=${plan.sinceSeq}`
        + ` until=${plan.untilSeq}: ${reason}`,
      );
      return null;
    } finally {
      clearTimeout(timer);
    }
  }
}
