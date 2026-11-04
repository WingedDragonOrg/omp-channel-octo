/**
 * 进度卡的完整生命周期：一轮 user round 一张卡（懒建卡、按版本合并的运行中编辑、终态编辑、
 * 被 steer 顶掉后的滚动），以及进程重启后对上一进程遗留卡片的两种处置：任务已经结束的按存
 * 储的终态补发一次终态编辑，任务即将续接的直接接管——不改卡片身份、不改序号，接着编辑读者
 * 眼前那张卡。两者只共享卡片行与卡片投递两条边界，身份分别是运行中的 round 与
 * `progress_cards` 行。
 *
 * 模块不依赖 `service.ts`：卡片行（`saveProgressCard` / `listProgressCards` /
 * `recordProgressCardAttempt` / `clearProgressCard` / `getTask`）、卡片投递
 * （`sendProgressCard` / `editProgressCard`）、bot 身份、关停标志，以及「卡片出站操作要在关库
 * 前被排空」的登记口都由 `CoreService` 以窄端口注入。每个 task 一个 `TaskProgress`：
 * `runTask` 开始时 `startTask`，事件交 `apply`，终态交 `finish`，steer 成功交 `roll`，
 * `finally` 里 `stop`，归档前 `await tail`。
 */

import { randomUUID } from "node:crypto";
import {
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  restoreExecutionTracker,
  stripCardSummaries,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "./execution-tracker.ts";
import type { ExecutionEvent } from "./execution-tracker.ts";
import type { ChannelTransport, ChatTarget, ProgressCard, WorkerEvent } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ProgressCardRecord } from "../store/store-records.ts";
import { targetKey } from "../target.ts";

const PROGRESS_MERGE_MS = 1_000;
const ACTIVITY_REQUEST_TIMEOUT_MS = 2_000;
/** A silent native commit refreshes its elapsed time without inventing a new phase. */
const COMMIT_ELAPSED_REFRESH_MS = 10_000;
/** Recovery attempts for a card left by a previous process before the row is dropped. */
const PROGRESS_CARD_RECOVERY_ATTEMPTS = 3;

/**
 * 本任务最近一次 `research_update` 携带的研究快照。研究带与 todo 一样属于任务而非单轮：
 * steer 滚出的新卡从它起步，普通新任务有自己的 TaskProgress，天然不带它。
 */
type LatestResearch = Extract<ExecutionEvent, { type: "research_update" }>["research"];

/**
 * One user round's progress card inside a run. A run serves every message of its chat, and each
 * appended message is answered in the card that appears right below it, so a round owns its own
 * card, idempotency key, and sequence.
 */
type ProgressRound = {
  /**
   * Server idempotency key: stable across creation retries, distinct between rounds. A round of a
   * task that came through recovery carries a recovery-only segment instead of the plain counter,
   * because the counter the previous process reached is not durable and its keys must never repeat.
   */
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

/**
 * A fresh round: `roundIndex` is the run's own counter, so the idempotency key never repeats.
 * A resumed round arrives with the tracker replayed from the stored payload instead of a new one.
 */
function makeRound(
  taskId: string,
  roundIndex: number,
  keyPrefix: string,
  tracker?: ExecutionTracker,
): ProgressRound {
  return {
    clientMsgNo: `progress-${taskId}-${keyPrefix}${roundIndex}`,
    tracker: tracker ?? createExecutionTracker({ runId: taskId }),
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
 * 上一进程遗留、即将由本进程续接的一轮：卡片已经存在服务端，所以身份与序号都取自那行，而不是
 * 再发一次创建。`snapshot` 是那行里的最后一帧重放出来的样子——续接的一轮从读者眼前的状态继续，
 * todo、工具轨迹、子代理与研究都还在。
 */
type ResumedRound = {
  readonly messageId: string;
  /** The last sequence the server accepted for this card; the next edit stays strictly above it. */
  readonly lastSeq: number;
  readonly tracker: ExecutionTracker;
  readonly snapshot: ExecutionSnapshot;
};

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
    | "saveProgressCard"
    | "listProgressCards"
    | "recordProgressCardAttempt"
    | "clearProgressCard"
    | "getTask"
    /** 该 task 是否会被本进程续接：决定遗留卡片是接管还是补发终态。 */
    | "isTaskResuming"
    | "getReviewJob"
  >;
  /** Review jobs are independent from chat tasks but use the same card transport and persistence. */
  readonly isReviewResuming?: (taskId: string) => boolean;
  readonly transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard">;
  /** 卡片行以 bot 身份为键；未就绪（未 start）即整个域 no-op。 */
  readonly botUid: () => string | undefined;
  /** 关停中不再排新的合并编辑；在途请求由 `stop` 的 abort 收束。 */
  readonly stopping: () => boolean;
  readonly trackOperation: (operation: Promise<void>) => void;
}

/** 恢复留给本轮的两件事：接管的那张卡（可能没有），以及新开卡必须换用的 idempotency key 段。 */
type RecoveryHandoff = {
  readonly round: ResumedRound | undefined;
  readonly keyPrefix: string;
};

/** 全服务共用的持久恢复：卡片身份只存在于观察过该回合的进程里。 */
export class ProgressCards {
  /**
   * 恢复期对某个任务的决定，按 task id 存放；`startTask` 取走即消费，所以同一任务只会取一次。
   * 值 `null` 表示这次恢复没有卡片可接管——最后一行不可用，或它已经按终态补发过——对新的一轮
   * 而言只剩「开卡必须换 key 段」这一件事。
   */
  private readonly recovered = new Map<string, ResumedRound | null>();
  /** A fresh recovery segment keeps new rounds distinct from every previously issued card key. */
  private readonly recoveryKeyPrefix = `r${randomUUID()}-`;

  constructor(private readonly deps: ProgressCardsDeps) {}

  /** 一轮 task 一个 owner；`runTask` 建，`finally` 里 `stop`。 */
  startTask(taskId: string, target: ChatTarget): TaskProgress {
    const recovered = this.recovered.get(taskId);
    if (recovered === undefined && !this.deps.store.isTaskResuming(taskId) && !this.deps.isReviewResuming?.(taskId)) {
      return new TaskProgress(this.deps, taskId, target);
    }
    this.recovered.delete(taskId);
    return new TaskProgress(this.deps, taskId, target, {
      round: recovered ?? undefined,
      keyPrefix: this.recoveryKeyPrefix,
    });
  }

  /**
   * Settles the cards left open by an earlier process — a restart that interrupted the turn, or a
   * terminal edit that never landed. Without this the client keeps showing the last running state
   * forever, because the card's message id and sequence only existed in that process.
   *
   * A task this process is about to resume is the exception: its newest card is taken over instead
   * of closed, so the resumed turn keeps editing the card the reader is looking at rather than
   * flashing a cancelled one and opening a second below the same message. Rounds the run had
   * already rolled away are still closed, in the state the roll gave them.
   */
  async recover(): Promise<void> {
    const botUid = this.deps.botUid();
    if (botUid === undefined) return;
    // 同一任务的行按创建顺序读（rowid）：最后一行是上一进程死前正在编辑的那一轮，前面的是它已经
    // 滚掉的轮。带上时间戳的那些行不能当作新旧的依据——旧轮的迟到重写与重试都会落在新轮之后。
    const toResume = new Map<string, ProgressCardRecord[]>();
    const withRows = new Set<string>();
    for (const card of this.deps.store.listProgressCards(botUid)) {
      withRows.add(card.taskId);
      if (this.deps.store.isTaskResuming(card.taskId) || this.deps.isReviewResuming?.(card.taskId)) {
        const rounds = toResume.get(card.taskId);
        if (rounds === undefined) toResume.set(card.taskId, [card]);
        else rounds.push(card);
        continue;
      }
      const snapshot = this.restoreStoredCard(card);
      if (snapshot !== null) {
        await this.closeStoredCard(
          { channelType: card.channelType, channelId: card.channelId },
          card,
          snapshot,
        );
      }
    }
    for (const [taskId, rounds] of toResume) {
      const current = rounds[rounds.length - 1]!;
      const atMs = Date.now();
      const tracker = restoreExecutionTracker(current.snapshot, { state: "running", atMs });
      if (tracker === null) {
        forgetCard(this.deps, taskId, current.messageId);
        console.error(`[progress-card] 丢弃无法解析的卡片记录 task=${taskId}`);
        this.recovered.set(taskId, null);
      } else {
        this.recovered.set(taskId, {
          messageId: current.messageId,
          lastSeq: current.lastSeq,
          tracker,
          snapshot: tracker.snapshot(atMs),
        });
      }
      // 被 steer 滚掉的轮不再有读者，但卡片还开着：按 `roll` 当时给出的结局收尾（新消息已经由
      // 后一轮回答），而不是让它们停在运行中的样子。
      for (const retired of rounds.slice(0, -1)) {
        const snapshot = this.restoreStoredCard(retired, "completed");
        if (snapshot === null) continue;
        await this.closeStoredCard(
          { channelType: retired.channelType, channelId: retired.channelId },
          retired,
          snapshot,
        );
      }
    }
    // 其余恢复时还有卡片行、又可能再跑一次的任务（典型是刚补发完终态、即将重放原文的排队行）同样
    // 不能再从 `progress-<taskId>-0` 数 key：上一进程已经用掉过那批 key。终态任务不会再跑，不留意图。
    for (const taskId of withRows) {
      if (this.recovered.has(taskId)) continue;
      const task = this.deps.store.getTask(taskId);
      const review = this.deps.store.getReviewJob(taskId);
      const active = task !== null
        ? task.state !== "completed" && task.state !== "failed"
          && task.state !== "cancelled" && task.state !== "interrupted"
        : review?.state === "accepted" || review?.state === "running";
      if (active) this.recovered.set(taskId, null);
    }
  }

  /**
   * The stored frame replayed into a terminal card body, or null when the row cannot be used — and
   * then dropped, because no later process could address that card either. A card whose own task
   * row is missing gets the cancelled verdict the old recovery always gave it: nothing can revive
   * a run no task record describes.
   */
  private restoreStoredCard(
    card: ProgressCardRecord,
    state?: Exclude<ExecutionState, "running">,
  ): ExecutionSnapshot | null {
    const task = this.deps.store.getTask(card.taskId);
    const review = this.deps.store.getReviewJob(card.taskId);
    const verdict = state ?? (task?.state === "completed" || review?.state === "completed"
      ? "completed"
      : task?.state === "failed" || review?.state === "failed"
        ? "failed"
        : "cancelled");
    const snapshot = restoreExecutionSnapshot(card.snapshot, { state: verdict, atMs: Date.now() });
    if (snapshot === null) {
      forgetCard(this.deps, card.taskId, card.messageId);
      console.error(`[progress-card] 丢弃无法解析的卡片记录 task=${card.taskId}`);
    }
    return snapshot;
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

  /**
   * Submits one terminal edit for a card nobody will edit again and drops the row. A failed edit
   * spends a bounded attempt and keeps the row — a lost response may already have been accepted, so
   * the next process retries above it — until the attempts run out.
   */
  private async closeStoredCard(
    target: ChatTarget,
    card: ProgressCardRecord,
    snapshot: ExecutionSnapshot,
  ): Promise<void> {
    try {
      await this.submitRecoveredCard(target, card, snapshot);
      forgetCard(this.deps, card.taskId, card.messageId);
      console.error(
        `[progress-card] 已补发终态卡片 task=${card.taskId} target=${targetKey(target)} state=${snapshot.state}`,
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

/**
 * 一个 task 的进度卡：当前轮（含 tracker）、合并编辑定时器、卡片出站链尾与运行中编辑的
 * abort 源。卡片是懒建的，没有工具/待办/研究进度的一轮不显示卡片。
 */
export class TaskProgress {
  private timer: NodeJS.Timeout | undefined;
  private commitHeartbeat: NodeJS.Timeout | undefined;
  private commitSummary: string | undefined;
  private progressTail: Promise<void> = Promise.resolve();
  private readonly progressAbort = new AbortController();
  private roundIndex = 0;
  private round: ProgressRound;
  /** 本任务最近一次研究快照：随 `apply` 更新，`roll` 时结转进新轮。 */
  private latestResearch: LatestResearch | undefined;
  /**
   * 本任务后续轮的 idempotency key 前缀。新任务为空（`progress-<taskId>-<n>` 从未用过）；
   * 恢复过的任务换成恢复期的新段，保证同一任务里新卡永远不会复用上一进程用过的 key。
   */
  private keyPrefix = "";

  constructor(
    private readonly deps: ProgressCardsDeps,
    private readonly taskId: string,
    private readonly target: ChatTarget,
    handoff?: RecoveryHandoff,
  ) {
    this.keyPrefix = handoff?.keyPrefix ?? "";
    const resumed = handoff?.round;
    if (resumed === undefined) {
      this.round = makeRound(taskId, 0, this.keyPrefix);
      return;
    }
    // 上一进程已经建过这轮卡片：恢复不改身份、不改序号，只是接着编辑读者眼前那张卡。
    // 研究带和 todo 一样属于任务而不是单轮，接管时先从快照里恢复任务级快照，否则恢复后
    // 第一次 steer 滚出的新卡会静默丢掉上一进程已经报出来的研究带。
    this.latestResearch = resumed.snapshot.research;
    this.commitSummary = resumed.snapshot.commit?.summary;
    this.round = {
      ...makeRound(taskId, 0, this.keyPrefix, resumed.tracker),
      started: true,
      messageId: resumed.messageId,
      seq: resumed.lastSeq,
      latest: resumed.snapshot,
      // 存储里的那一帧就是客户端正在看的一版；第一次真实变化是下一版，由它发出增量编辑。
      version: 1,
      sentVersion: 0,
    };
  }

  /** 当前轮已进入终态（终态编辑已入队）：`stop` 据此决定是否 abort 在途编辑。 */
  get finished(): boolean {
    return this.round.finished;
  }

  /** True once any native commit stage reached this task, so a fallback may close its card. */
  get commitStarted(): boolean {
    return this.commitSummary !== undefined;
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
   * on the round's first real tool/research/todo progress, so a round without any of them
   * shows no card. The research snapshot is remembered before `track`'s early returns: it
   * belongs to the task, so a frame arriving after the round was retired still seeds the next
   * card when the reader steers again.
   */
  apply(event: ExecutionEvent): void {
    if (event.type === "research_update") this.latestResearch = event.research;
    if (event.type === "commit_progress") {
      this.commitSummary = event.summary;
      this.startCommitHeartbeat();
    }
    if (event.type === "commit_result") this.stopCommitHeartbeat();
    this.track(event, this.round.tracker.apply(event));
  }

  /** Opens a card from a real host-owned lifecycle start, without inventing a tool or percentage. */
  beginLifecycle(): void {
    const lifecycle = { type: "turn_start", turnId: this.taskId, atMs: Date.now() } as const;
    this.track(lifecycle, this.round.tracker.snapshot(lifecycle.atMs), true);
  }

  /** 终态帧：入队后本轮不再编辑，`stop` 也不再 abort 它。 */
  finish(state: Exclude<ProgressCard["state"], "running">): void {
    this.stopCommitHeartbeat();
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
    this.stopCommitHeartbeat();
    this.commitSummary = undefined;
    this.roundIndex += 1;
    this.round = makeRound(this.taskId, this.roundIndex, this.keyPrefix);
    // The todo list belongs to the task rather than to one round, so the new card keeps it.
    const todos = previous.latest?.todos ?? [];
    if (todos.length > 0) {
      this.round.tracker.apply({ type: "todo_update", atMs: Date.now(), phases: todos });
    }
    // 研究带同样属于任务：滚出的新卡从最近一次研究快照起步。时间戳与 todo 结转一致取当前
    // 时刻，不把上一轮的耗时倒算进新轮。普通新任务有自己的 TaskProgress，不会继承旧快照。
    if (this.latestResearch !== undefined) {
      this.round.tracker.apply({ type: "research_update", atMs: Date.now(), research: this.latestResearch });
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
    this.stopCommitHeartbeat();
    if (!this.round.finished || abortProgress) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (abortProgress) this.progressAbort.abort();
  }

  private track(event: WorkerEvent, progress: ProgressCard, forceStart = false): void {
    const current = this.round;
    current.latest = progress;
    if (current.retired) return;
    if (current.finished && progress.state !== "running") return;
    current.version += 1;
    const startsCard = forceStart ||
      event.type === "tool_start" ||
      event.type === "tool_end" ||
      event.type === "todo_update" ||
      event.type === "research_update" ||
      event.type === "commit_progress" ||
      event.type === "commit_result";
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
    const deadline = Promise.withResolvers<never>();
    // A transport that ignores the abort signal must not keep this card operation alive forever;
    // the caller's task and text receipt never wait for card I/O, and shutdown must still see the
    // progress tail settle.
    const timer = setTimeout(() => {
      controller.abort();
      deadline.reject(new Error("progress card request timed out"));
    }, ACTIVITY_REQUEST_TIMEOUT_MS);
    const timeout = deadline.promise;
    try {
      return await Promise.race([work(controller.signal), timeout]);
    } finally {
      clearTimeout(timer);
      parentSignal?.removeEventListener("abort", abort);
    }
  }

  /**
   * A native commit can stay silent for minutes while Git or hooks work. Every ten seconds the
   * same safe summary is re-applied so the snapshot's measured elapsed time advances; the
   * tracker's bounded history drops the duplicate line, so no stage or percentage is invented.
   */
  private startCommitHeartbeat(): void {
    if (this.commitHeartbeat !== undefined || this.deps.stopping() || this.round.finished) return;
    this.commitHeartbeat = setInterval(() => {
      const summary = this.commitSummary;
      if (summary === undefined || this.round.finished || this.round.retired) return;
      this.apply({ type: "commit_progress", atMs: Date.now(), summary });
    }, COMMIT_ELAPSED_REFRESH_MS);
    this.commitHeartbeat.unref?.();
  }

  private stopCommitHeartbeat(): void {
    if (this.commitHeartbeat === undefined) return;
    clearInterval(this.commitHeartbeat);
    this.commitHeartbeat = undefined;
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
        if (!round.degraded && isEdgeRefusal(error)) {
          round.degraded = true;
          console.error(
            `[progress-card] 终态卡片正文被网关拒绝，改用不带步骤摘要的形态重试`
            + ` target=${targetKey(this.target)} seq=${cardSeq}`,
          );
          await this.editProgress(round);
          return;
        }
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
