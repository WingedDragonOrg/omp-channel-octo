/**
 * 进度卡的完整生命周期：一轮 user round 一张卡（懒建卡、按版本合并的运行中编辑、终态编辑、
 * 被 steer 顶掉后的滚动），以及进程重启后替上一进程补发终态卡片的持久恢复。两者只共享卡片
 * 行与卡片投递两条边界，身份分别是运行中的 round 与 `progress_cards` 行。
 *
 * 模块不依赖 `service.ts`：卡片行（`saveProgressCard` / `listProgressCards` /
 * `recordProgressCardAttempt` / `clearProgressCard` / `getTask`）、卡片投递
 * （`sendProgressCard` / `editProgressCard`）、bot 身份、关停标志，以及「卡片出站操作要在关库
 * 前被排空」的登记口都由 `CoreService` 以窄端口注入。每个 task 一个 `TaskProgress`：
 * `runTask` 开始时 `startTask`，事件交 `apply`，终态交 `finish`，steer 成功交 `roll`，
 * `finally` 里 `stop`，归档前 `await tail`。
 */

import {
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  stripCardSummaries,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "./execution-tracker.ts";
import type { ExecutionEvent } from "./execution-tracker.ts";
import type { ChannelTransport, ChatTarget, ProgressCard, WorkerEvent } from "./contracts.ts";
import type { ChannelStore } from "./store.ts";
import type { ProgressCardRecord } from "./store-records.ts";
import { targetKey } from "./target.ts";

const PROGRESS_MERGE_MS = 1_000;
const ACTIVITY_REQUEST_TIMEOUT_MS = 2_000;
/** Recovery attempts for a card left by a previous process before the row is dropped. */
const PROGRESS_CARD_RECOVERY_ATTEMPTS = 3;

/**
 * One user round's progress card inside a run. A run serves every message of its chat, and each
 * appended message is answered in the card that appears right below it, so a round owns its own
 * card, idempotency key, and sequence.
 */
type ProgressRound = {
  /** Server idempotency key: stable across creation retries, distinct between rounds. */
  readonly clientMsgNo: string;
  readonly tracker: ExecutionTracker;
  /** True once creation of this round's card has been queued. */
  started: boolean;
  /** True once a terminal edit for this card has been queued. */
  finished: boolean;
  /** True once a later round took over; only this card's terminal edit may still run. */
  retired: boolean;
  disabled: boolean;
  /** Set after this card's first running-edit failure so a repeating error logs once. */
  failureLogged: boolean;
  /**
   * True once the deployment's edge refused a card body and the retry dropped the free-text step
   * summaries. The rest of the round keeps that shape — including the terminal edit — so the card
   * keeps updating instead of freezing on the refusal.
   */
  degraded: boolean;
  messageId: string | undefined;
  seq: number;
  latest: ProgressCard | undefined;
  version: number;
  sentVersion: number;
  nextEditAt: number;
};

/** A fresh round: `roundIndex` is the run's own counter, so the idempotency key never repeats. */
function makeRound(taskId: string, roundIndex: number): ProgressRound {
  return {
    clientMsgNo: `progress-${taskId}-${roundIndex}`,
    tracker: createExecutionTracker({ runId: taskId }),
    started: false,
    finished: false,
    retired: false,
    disabled: false,
    failureLogged: false,
    degraded: false,
    messageId: undefined,
    seq: 0,
    latest: undefined,
    version: 0,
    sentVersion: 0,
    nextEditAt: 0,
  };
}

/**
 * The edge in front of the bot API refuses a request body it dislikes with its own 4xx and page,
 * while every bot API error — transient or not — arrives as a JSON envelope carrying `error.code`.
 * So a 4xx with no code is a refusal of the body rather than a verdict on the request's meaning,
 * and the same call can go out again in a different shape. Read structurally, like
 * `isPermanentOutboxError`, so a fake transport can reproduce the shape without the real module.
 */
function isEdgeRefusal(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { code?: unknown; status?: unknown };
  return typeof candidate.status === "number"
    && candidate.status >= 400
    && candidate.status < 500
    && candidate.code === undefined;
}

/**
 * Persists the card frame the server accepted. A process that did not observe the run cannot
 * address the card, so this row is what lets a later process close it. Storage failures are
 * logged instead of being treated as card failures.
 */
function rememberCard(
  deps: ProgressCardsDeps,
  taskId: string,
  target: ChatTarget,
  messageId: string,
  lastSeq: number,
  snapshot: ExecutionSnapshot,
): void {
  const botUid = deps.botUid();
  if (botUid === undefined) return;
  try {
    deps.store.saveProgressCard({
      botUid,
      taskId,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      lastSeq,
      snapshot: encodeExecutionSnapshot(snapshot),
    });
  } catch (error) {
    console.error(
      `[progress-card] 无法记录卡片状态 task=${taskId}：`
      + `${error instanceof Error ? error.message : "unknown error"}`,
    );
  }
}

/** Drops the persisted card row. Bookkeeping failures are logged, never fatal. */
function forgetCard(deps: ProgressCardsDeps, taskId: string, messageId: string): void {
  const botUid = deps.botUid();
  if (botUid === undefined) return;
  try {
    deps.store.clearProgressCard(botUid, taskId, messageId);
  } catch (error) {
    console.error(
      `[progress-card] 无法清除卡片记录 task=${taskId}：`
      + `${error instanceof Error ? error.message : "unknown error"}`,
    );
  }
}

/**
 * Counts a recovery attempt, whose row also advances the stored sequence: an attempt whose
 * response was lost may already be accepted, so the next one has to stay strictly above it. A
 * failing state disk must not keep the channel from starting, so that failure is logged and
 * reported as the last attempt.
 */
function recordCardAttempt(deps: ProgressCardsDeps, taskId: string, messageId: string): number {
  const botUid = deps.botUid();
  if (botUid === undefined) return PROGRESS_CARD_RECOVERY_ATTEMPTS;
  try {
    return deps.store.recordProgressCardAttempt(botUid, taskId, messageId);
  } catch (error) {
    console.error(
      `[progress-card] 无法记录补发尝试 task=${taskId}：`
      + `${error instanceof Error ? error.message : "unknown error"}`,
    );
    return PROGRESS_CARD_RECOVERY_ATTEMPTS;
  }
}

/**
 * 进度卡域对外的两条边界：`progress_cards` 行（含恢复时要读的 task 终态）与卡片投递。
 * `botUid` / `stopping` 是 getter，因为身份在 `start()` 之后才学到、关停标志随服务生命周期
 * 变化；`trackOperation` 把一条卡片出站操作登记进服务的 activity 集合，关库前会被排空。
 */
export interface ProgressCardsDeps {
  readonly store: Pick<
    ChannelStore,
    "saveProgressCard" | "listProgressCards" | "recordProgressCardAttempt" | "clearProgressCard" | "getTask"
  >;
  readonly transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard">;
  /** 卡片行以 bot 身份为键；未就绪（未 start）即整个域 no-op。 */
  readonly botUid: () => string | undefined;
  /** 关停中不再排新的合并编辑；在途请求由 `stop` 的 abort 收束。 */
  readonly stopping: () => boolean;
  readonly trackOperation: (operation: Promise<void>) => void;
}

/** 全服务共用的持久恢复：卡片身份只存在于观察过该回合的进程里。 */
export class ProgressCards {
  constructor(private readonly deps: ProgressCardsDeps) {}

  /** 一轮 task 一个 owner；`runTask` 建，`finally` 里 `stop`。 */
  startTask(taskId: string, target: ChatTarget): TaskProgress {
    return new TaskProgress(this.deps, taskId, target);
  }

  /**
   * Closes cards left open by an earlier process — a restart that interrupted the turn, or a
   * terminal edit that never landed. Without this the client keeps showing the last running state
   * forever, because the card's message id and sequence only existed in that process.
   */
  async recover(): Promise<void> {
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    for (const card of this.deps.store.listProgressCards(botUid)) {
      const target: ChatTarget = { channelType: card.channelType, channelId: card.channelId };
      const task = this.deps.store.getTask(card.taskId);
      const state: Exclude<ExecutionState, "running"> = task?.state === "completed"
        ? "completed"
        : task?.state === "failed"
          ? "failed"
          : "cancelled";
      const snapshot = restoreExecutionSnapshot(card.snapshot, { state, atMs: Date.now() });
      if (snapshot === null) {
        forgetCard(this.deps, card.taskId, card.messageId);
        console.error(`[progress-card] 丢弃无法解析的卡片记录 task=${card.taskId}`);
        continue;
      }
      try {
        await this.submitRecoveredCard(target, card, snapshot);
        forgetCard(this.deps, card.taskId, card.messageId);
        console.error(
          `[progress-card] 已补发终态卡片 task=${card.taskId} target=${targetKey(target)} state=${state}`,
        );
      } catch (error) {
        const attempts = recordCardAttempt(this.deps, card.taskId, card.messageId);
        console.error(
          `[progress-card] 补发终态失败 task=${card.taskId} attempt=${attempts}：`
          + `${error instanceof Error ? error.message : "unknown error"}`,
        );
        if (attempts >= PROGRESS_CARD_RECOVERY_ATTEMPTS) {
          forgetCard(this.deps, card.taskId, card.messageId);
        }
      }
    }
  }

  /** One recovery edit, under the same bounded deadline as a live card edit. */
  private async submitRecoveredCard(
    target: ChatTarget,
    card: ProgressCardRecord,
    snapshot: ExecutionSnapshot,
  ): Promise<void> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ACTIVITY_REQUEST_TIMEOUT_MS);
    try {
      await this.deps.transport.editProgressCard(
        target,
        card.messageId,
        snapshot,
        card.lastSeq + 1,
        controller.signal,
      );
    } finally {
      clearTimeout(timer);
    }
  }
}

/**
 * 一个 task 的进度卡：当前轮（含 tracker）、合并编辑定时器、卡片出站链尾与运行中编辑的
 * abort 源。卡片是懒建的，没有工具/待办进度的一轮不显示卡片。
 */
export class TaskProgress {
  private timer: NodeJS.Timeout | undefined;
  private progressTail: Promise<void> = Promise.resolve();
  private readonly progressAbort = new AbortController();
  private roundIndex = 0;
  private round: ProgressRound;

  constructor(
    private readonly deps: ProgressCardsDeps,
    private readonly taskId: string,
    private readonly target: ChatTarget,
  ) {
    this.round = makeRound(taskId, 0);
  }

  /** 当前轮已进入终态（终态编辑已入队）：`stop` 据此决定是否 abort 在途编辑。 */
  get finished(): boolean {
    return this.round.finished;
  }

  /**
   * 本 task 全部卡片出站操作的链尾。读取的是当前链尾而非快照：入队后才取的值必须把新操作
   * 也包含进来（归档与关停都等它）。
   */
  get tail(): Promise<void> {
    return this.progressTail;
  }

  /**
   * Feeds one real execution event into the current round's card. The card is created lazily
   * on the round's first real tool/todo progress, so a round without tool work shows no card.
   */
  apply(event: ExecutionEvent): void {
    this.track(event, this.round.tracker.apply(event));
  }

  /** 终态帧：入队后本轮不再编辑，`stop` 也不再 abort 它。 */
  finish(state: Exclude<ProgressCard["state"], "running">): void {
    const current = this.round;
    if (!current.started || current.finished) return;
    current.finished = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    current.latest = current.tracker.apply({
      type: "run_end",
      atMs: Date.now(),
      state,
    });
    current.version += 1;
    this.queueProgress(() => this.editProgress(current));
  }

  /**
   * A steered message opens the next user round. The card the reader is looking at must sit
   * below that message instead of staying where the first tool call of the run happened, so the
   * round in progress is closed and the next real tool progress opens a card of its own.
   */
  roll(): void {
    const previous = this.round;
    if (previous.retired) return;
    previous.retired = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.roundIndex += 1;
    this.round = makeRound(this.taskId, this.roundIndex);
    // The todo list belongs to the task rather than to one round, so the new card keeps it.
    const todos = previous.latest?.todos ?? [];
    if (todos.length > 0) {
      this.round.tracker.apply({ type: "todo_update", atMs: Date.now(), phases: todos });
    }
    if (!previous.started || previous.finished || previous.disabled) return;
    previous.finished = true;
    previous.latest = previous.tracker.apply({ type: "run_end", atMs: Date.now(), state: "completed" });
    previous.version += 1;
    this.queueProgress(() => this.editProgress(previous));
  }

  /**
   * 关停/取消/回合结束：已完成轮只停排新编辑（终态编辑不可中断，由 `tail` 等待），未完成轮
   * 连在途的运行中编辑一起 abort。
   */
  stop(abortProgress = true): void {
    if (!this.round.finished || abortProgress) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (abortProgress) this.progressAbort.abort();
  }

  private track(event: WorkerEvent, progress: ProgressCard): void {
    const current = this.round;
    current.latest = progress;
    if (current.retired) return;
    if (current.finished && progress.state !== "running") return;
    current.version += 1;
    const startsCard = event.type === "tool_start" ||
      event.type === "tool_end" ||
      event.type === "todo_update";
    if (!current.started && !startsCard) return;
    if (!current.started) {
      current.started = true;
      const initial = progress;
      const initialVersion = current.version;
      if (progress.state !== "running") {
        current.finished = true;
      }
      this.queueProgress(async () => {
        try {
          const receipt = await this.runActivityRequest(
            (signal) => this.deps.transport.sendProgressCard(
              this.target,
              initial,
              current.clientMsgNo,
              signal,
            ),
            this.progressAbort.signal,
          );
          if (!receipt?.messageId) {
            current.disabled = true;
            console.error(
              `[progress-card] 未创建进度卡 target=${targetKey(this.target)}：卡片能力不可用或内容超出限制`,
            );
            return;
          }
          current.messageId = receipt.messageId;
          current.seq = 1;
          current.sentVersion = initialVersion;
          rememberCard(this.deps, this.taskId, this.target, current.messageId, 1, initial);
          if (current.version > current.sentVersion) {
            if (current.finished) this.queueProgress(() => this.editProgress(current));
            else this.scheduleProgressEdit();
          } else if (!current.finished) {
            this.scheduleProgressEdit();
          }
        } catch (error) {
          current.started = false;
          console.error(
            `[progress-card] 创建失败 target=${targetKey(this.target)}：`
            + `${error instanceof Error ? error.message : "unknown error"}`,
          );
        }
      });
      return;
    }
    if (progress.state !== "running") {
      current.finished = true;
      clearTimeout(this.timer);
      this.timer = undefined;
      this.queueProgress(() => this.editProgress(current));
    } else {
      this.scheduleProgressEdit();
    }
  }

  private queueProgress(work: () => Promise<void>): void {
    this.progressTail = this.progressTail.then(work, work).catch(() => {});
    this.deps.trackOperation(this.progressTail);
  }

  private async runActivityRequest<T>(
    work: (signal: AbortSignal) => Promise<T>,
    parentSignal?: AbortSignal,
  ): Promise<T> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    if (parentSignal?.aborted) {
      controller.abort();
    } else {
      parentSignal?.addEventListener("abort", abort, { once: true });
    }
    const timer = setTimeout(() => controller.abort(), ACTIVITY_REQUEST_TIMEOUT_MS);
    try {
      return await work(controller.signal);
    } finally {
      clearTimeout(timer);
      parentSignal?.removeEventListener("abort", abort);
    }
  }

  private scheduleProgressEdit(): void {
    const current = this.round;
    if (this.deps.stopping() || current.retired || !current.started || current.disabled || this.timer) return;
    if (current.version <= current.sentVersion) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      const pending = this.round;
      if (!pending.retired && pending.version > pending.sentVersion) {
        this.queueProgress(() => this.editProgress(pending));
      }
    }, Math.max(0, current.nextEditAt - Date.now()));
  }

  private async editProgress(round: ProgressRound): Promise<void> {
    const messageId = round.messageId;
    const progress = round.latest;
    if (round.disabled || !messageId || !progress) return;
    if (round.version <= round.sentVersion) {
      if (!round.retired) this.scheduleProgressEdit();
      return;
    }
    const running = progress.state === "running";
    // A superseded card only still receives the terminal edit that closes it.
    if (running && round.retired) return;
    if (running && Date.now() < round.nextEditAt) {
      this.scheduleProgressEdit();
      return;
    }
    const version = round.version;
    const cardSeq = ++round.seq;
    const snapshot = round.degraded ? stripCardSummaries(progress) : progress;
    if (running) round.nextEditAt = Date.now() + PROGRESS_MERGE_MS;
    const parentSignal = running ? this.progressAbort.signal : undefined;
    try {
      await this.runActivityRequest(
        (signal) => this.deps.transport.editProgressCard(this.target, messageId, snapshot, cardSeq, signal),
        parentSignal,
      );
      if (round.version === version) round.sentVersion = version;
      if (running) {
        rememberCard(this.deps, this.taskId, this.target, messageId, cardSeq, snapshot);
      } else {
        forgetCard(this.deps, this.taskId, messageId);
      }
    } catch (error) {
      const reason = error instanceof Error ? error.message : "unknown error";
      if (!running) {
        round.disabled = true;
        if (!round.retired) {
          clearTimeout(this.timer);
          this.timer = undefined;
        }
        console.error(
          `[progress-card] 终态编辑失败 target=${targetKey(this.target)} state=${progress.state}`
          + ` seq=${cardSeq}：${reason}`,
        );
      } else {
        if (!round.failureLogged) {
          round.failureLogged = true;
          console.error(`[progress-card] 运行中编辑失败 target=${targetKey(this.target)} seq=${cardSeq}：${reason}`);
        }
        // The edge refused the body rather than the request: the same card goes out once without
        // the free-text step summaries, and the round keeps that shape from here on.
        if (!round.degraded && isEdgeRefusal(error)) {
          round.degraded = true;
          console.error(
            `[progress-card] 卡片正文被网关拒绝，改用不带步骤摘要的形态重试`
            + ` target=${targetKey(this.target)} seq=${cardSeq}`,
          );
        }
      }
    } finally {
      if (!round.retired) this.scheduleProgressEdit();
    }
  }
}
