import { createHash } from "node:crypto";
import type {
  ChannelTransport,
  ChatTarget,
  HostMergeOutcome,
  HostMergeProgressPhase,
} from "../contracts.ts";
import { safeErrorMessage } from "../errors.ts";
import { safeDisplayText } from "./execution-text.ts";
import {
  createExecutionTracker,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "./execution-tracker.ts";
import type { ChannelStore } from "../store/index.ts";
import type { HostMergeLeaseRecord, HostMergeProgressCardRecord, HostMergeProgressDeliveryState } from "../store/store-records.ts";
import { targetKey } from "../target.ts";

const PROGRESS_REFRESH_MS = 10_000;
const ACTIVITY_REQUEST_TIMEOUT_MS = 2_000;
type HostMergeProgressStore = Pick<ChannelStore,
  | "reserveHostMergeProgressCard"
  | "getHostMergeProgressCard"
  | "listHostMergeProgressCards"
  | "updateHostMergeProgressCard"
  | "bindHostMergeProgressCardMessage"
  | "markHostMergeProgressCardAttempted"
  | "markHostMergeProgressCardUnknown"
  | "markHostMergeProgressCardUnavailable"
  | "recordHostMergeProgressCardAttempt"
  | "clearHostMergeProgressCard"
>;

interface HostMergeProgressDependencies {
  readonly store: HostMergeProgressStore;
  readonly transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard">;
  readonly botUid: () => string | undefined;
  readonly stopping: () => boolean;
  readonly trackOperation: (operation: Promise<void>) => void;
  readonly trackTail: (target: ChatTarget, operationId: string, getTail: () => Promise<void>) => void;
  readonly releaseTail: (target: ChatTarget, operationId: string, tail: Promise<void>) => void;
}

function isActivePhase(phase: HostMergeProgressPhase): boolean {
  return phase === "preparing" || phase === "process-started" || phase === "process-exited";
}

function executionState(phase: HostMergeProgressPhase): Exclude<ExecutionState, "running"> {
  if (phase === "succeeded") return "completed";
  if (phase === "cancelled") return "cancelled";
  return "failed";
}

function operationId(sourceId: string): string {
  return `host-merge:${sourceId}`;
}

function sessionKey(target: ChatTarget, sourceId: string): string {
  return `${targetKey(target)}\u0000${sourceId}`;
}

function clientMessageNumber(botUid: string, target: ChatTarget, sourceId: string): string {
  const digest = createHash("sha256")
    .update(JSON.stringify([botUid, target.channelType, target.channelId, sourceId]))
    .digest("hex")
    .slice(0, 40);
  return `thread-merge-${digest}`;
}

function resultText(outcome: HostMergeOutcome): string {
  if (outcome.kind === "not-started") {
    const diagnostic = safeDisplayText(outcome.diagnostic, 260);
    const detail = diagnostic === undefined ? "" : `：${diagnostic}`;
    return safeDisplayText(`Worktrunk merge 未启动，未执行合并${detail}`, 400)
      ?? "Worktrunk merge 未启动，未执行合并。";
  }
  if (outcome.kind === "succeeded") {
    const { branch, target, removed } = outcome.summary;
    return safeDisplayText(
      `合并成功：${branch} → ${target}；${removed ? "工作区已清理" : "工作区保留"}。`,
      400,
    ) ?? "合并成功。";
  }
  const diagnostic = safeDisplayText(outcome.diagnostic, 260);
  const detail = diagnostic === undefined ? "" : `：${diagnostic}`;
  return safeDisplayText(`Worktrunk merge 执行失败（退出码 ${outcome.exitCode}）${detail}`, 400)
    ?? "Worktrunk merge 执行失败。";
}

function terminalResult(text: unknown, fallback: string): string {
  return safeDisplayText(text, 400) ?? fallback;
}

export class HostMergeProgressCards {
  private readonly sessions = new Map<string, HostMergeProgressSession>();

  constructor(private readonly deps: HostMergeProgressDependencies) {}

  start(target: ChatTarget, sourceId: string): HostMergeProgressSession {
    const key = sessionKey(target, sourceId);
    const active = this.sessions.get(key);
    if (active) return active;
    const botUid = this.deps.botUid();
    if (!botUid || target.channelType !== 5 || sourceId.length === 0) {
      return HostMergeProgressSession.unavailable(this.deps, target, sourceId, botUid ?? "");
    }
    let existing: HostMergeProgressCardRecord | null;
    try {
      existing = this.deps.store.getHostMergeProgressCard(botUid, target, sourceId);
    } catch (error) {
      console.error(
        `[thread-merge-progress] 无法读取进度卡 target=${targetKey(target)}：`
        + safeErrorMessage(error, "progress card lookup failed"),
      );
      return HostMergeProgressSession.unavailable(this.deps, target, sourceId, botUid);
    }
    if (existing !== null) {
      const session = HostMergeProgressSession.create(this.deps, existing, released => this.release(key, released));
      this.sessions.set(key, session);
      this.deps.trackTail(target, operationId(sourceId), () => session.archiveTail);
      if (existing.messageId !== null) session.start(false);
      else if (existing.deliveryState === "ready" || existing.deliveryState === "unavailable") session.discardUnsent();
      else session.holdUnknownDelivery();
      return session;
    }
    let record: HostMergeProgressCardRecord;
    try {
      record = this.deps.store.reserveHostMergeProgressCard({
        botUid,
        target,
        sourceId,
        clientMsgNo: clientMessageNumber(botUid, target, sourceId),
        startedAt: Date.now(),
      });
    } catch (error) {
      console.error(
        `[thread-merge-progress] 无法登记进度卡 target=${targetKey(target)}：`
        + safeErrorMessage(error, "progress card persistence failed"),
      );
      return HostMergeProgressSession.unavailable(this.deps, target, sourceId, botUid);
    }
    const session = HostMergeProgressSession.create(this.deps, record, released => this.release(key, released));
    this.sessions.set(key, session);
    this.deps.trackTail(target, operationId(sourceId), () => session.archiveTail);
    session.start();
    return session;
  }

  async recover(botUid: string, leases: readonly HostMergeLeaseRecord[]): Promise<void> {
    let records: HostMergeProgressCardRecord[];
    try {
      records = this.deps.store.listHostMergeProgressCards(botUid);
    } catch (error) {
      console.error(
        `[thread-merge-progress] 无法读取遗留进度卡：${safeErrorMessage(error, "progress card recovery failed")}`,
      );
      return;
    }
    const leasesBySource = new Map<string, HostMergeLeaseRecord>();
    for (const lease of leases) {
      leasesBySource.set(sessionKey(lease, lease.sourceId), lease);
    }
    for (const record of records) {
      const target: ChatTarget = { channelType: record.channelType, channelId: record.channelId };
      const key = sessionKey(target, record.sourceId);
      const session = HostMergeProgressSession.create(this.deps, record, released => this.release(key, released));
      this.sessions.set(key, session);
      this.deps.trackTail(target, operationId(record.sourceId), () => session.archiveTail);
      const lease = leasesBySource.get(key);
      try {
        if (record.messageId === null) {
          if (record.deliveryState === "ready" || record.deliveryState === "unavailable") {
            session.discardUnsent();
          } else {
            if (lease?.outcome !== null && lease?.outcome !== undefined) {
              session.finishFromOutcome(lease.outcome, lease.updatedAt);
            }
            session.holdUnknownDelivery();
          }
          continue;
        }
        if (lease?.outcome !== null && lease?.outcome !== undefined) {
          session.finishFromOutcome(lease.outcome, lease.updatedAt);
        } else if (isActivePhase(record.phase)) {
          if (lease === undefined && record.phase === "preparing") {
            session.notStarted("服务重启前 Worktrunk merge 尚未启动；不会自动重放。");
          } else {
            session.unknown("服务重启后无法确认 Worktrunk merge 的最终结果；不会自动重放，请先核对目标分支与工作区。");
          }
        } else {
          session.start(false);
        }
      } catch (error) {
        console.error(
          `[thread-merge-progress] 无法恢复进度 target=${targetKey(target)}：`
          + safeErrorMessage(error, "progress card recovery failed"),
        );
        session.holdUnknownDelivery();
      }
    }
  }

  stop(): void {
    for (const session of this.sessions.values()) session.stopTimer();
  }

  private release(key: string, session: HostMergeProgressSession): void {
    if (this.sessions.get(key) === session) this.sessions.delete(key);
  }
}

export class HostMergeProgressSession {
  private readonly tracker: ExecutionTracker;
  private readonly target: ChatTarget;
  private readonly botUid: string;
  private readonly sourceId: string;
  private readonly clientMsgNo: string;
  private readonly startedAt: number;
  private readonly operationId: string;
  private readonly onRelease: (session: HostMergeProgressSession) => void;
  private phaseValue: HostMergeProgressPhase;
  private resultValue: string | undefined;
  private messageId: string | undefined;
  private endedAt: number | null;
  private timer: ReturnType<typeof setInterval> | undefined;
  private tailValue: Promise<void> = Promise.resolve();
  private queued = false;
  private dirty = false;
  private disabled: boolean;
  private released = false;
  private failureLogged = false;
  private version = 0;
  private readonly archiveBarrier = Promise.withResolvers<void>();
  private archiveBarrierReleased = false;
  private deliveryState: HostMergeProgressDeliveryState | undefined;

  private constructor(
    private readonly deps: HostMergeProgressDependencies,
    record: HostMergeProgressCardRecord | null,
    onRelease: (session: HostMergeProgressSession) => void,
    fallback?: { readonly target: ChatTarget; readonly botUid: string; readonly sourceId: string },
  ) {
    const target = record ?? fallback!.target;
    this.target = { channelType: target.channelType, channelId: target.channelId };
    this.botUid = record?.botUid ?? fallback!.botUid;
    this.sourceId = record?.sourceId ?? fallback!.sourceId;
    this.clientMsgNo = record?.clientMsgNo ?? "";
    this.startedAt = record?.startedAt ?? Date.now();
    this.operationId = operationId(this.sourceId);
    this.phaseValue = record?.phase ?? "preparing";
    this.resultValue = record?.result ?? undefined;
    this.messageId = record?.messageId ?? undefined;
    this.deliveryState = record?.deliveryState;
    this.endedAt = record?.endedAt ?? null;
    this.disabled = record === null;
    if (record === null) this.resolveArchiveBarrier();
    this.onRelease = onRelease;
    this.tracker = createExecutionTracker({ runId: this.clientMsgNo, initialAtMs: this.startedAt });
    if (!isActivePhase(this.phaseValue)) {
      this.tracker.apply({
        type: "run_end",
        atMs: this.endedAt ?? record?.updatedAt ?? this.startedAt,
        state: executionState(this.phaseValue),
      });
    }
  }

  static create(
    deps: HostMergeProgressDependencies,
    record: HostMergeProgressCardRecord,
    onRelease: (session: HostMergeProgressSession) => void,
  ): HostMergeProgressSession {
    return new HostMergeProgressSession(deps, record, onRelease);
  }
  static unavailable(
    deps: HostMergeProgressDependencies,
    target: ChatTarget,
    sourceId: string,
    botUid: string,
  ): HostMergeProgressSession {
    return new HostMergeProgressSession(deps, null, () => {}, { target, sourceId, botUid });
  }

  get phase(): HostMergeProgressPhase {
    return this.phaseValue;
  }

  get tail(): Promise<void> {
    return this.tailValue;
  }

  get archiveTail(): Promise<void> {
    return this.archiveBarrier.promise;
  }

  start(startTimer = true): void {
    if (this.disabled) return;
    if (startTimer && isActivePhase(this.phaseValue) && !this.deps.stopping() && this.timer === undefined) {
      this.timer = setInterval(() => this.refreshElapsed(), PROGRESS_REFRESH_MS);
      this.timer.unref();
    }
    this.queueLatest();
  }

  processStarted(): void {
    this.setActivePhase("process-started");
  }

  processExited(): void {
    this.setActivePhase("process-exited");
  }

  finishFromOutcome(outcome: HostMergeOutcome, atMs = Date.now()): void {
    const phase = outcome.kind === "succeeded"
      ? "succeeded"
      : outcome.kind === "not-started"
        ? "not-started"
        : "execution-failed";
    this.finish(phase, resultText(outcome), atMs, true);
  }

  notStarted(result = "Worktrunk merge 尚未启动，因此没有执行合并。"): void {
    this.finish("not-started", result);
  }

  cancelled(result = "取消在 Worktrunk merge 启动前得到确认；合并未执行。"): void {
    this.finish("cancelled", result);
  }

  unknown(result = "Worktrunk merge 的最终结果无法确认；不会自动重放，请先核对目标分支与工作区。"): void {
    this.finish("unknown", result);
  }

  discardUnsent(): void {
    this.disabled = true;
    this.stopTimer();
    if (this.clearRecord()) this.deliveryState = undefined;
    this.resolveArchiveBarrier();
    this.release();
  }

  holdUnknownDelivery(): void {
    this.disabled = true;
    this.stopTimer();
    this.release();
  }

  stopTimer(): void {
    if (this.timer !== undefined) clearInterval(this.timer);
    this.timer = undefined;
  }

  private setActivePhase(phase: HostMergeProgressPhase): void {
    if (this.disabled || !isActivePhase(this.phaseValue) || this.phaseValue === phase) return;
    if (!this.persistPhase(phase, null, this.resultValue ?? null)) return;
    this.phaseValue = phase;
    this.version += 1;
    this.queueLatest();
  }

  private finish(phase: HostMergeProgressPhase, result: string, atMs = Date.now(), force = false): void {
    const wasActive = isActivePhase(this.phaseValue);
    if (!wasActive && !force) return;
    const terminal = terminalResult(result, "合并结果已记录。");
    const endedAt = Math.max(this.startedAt, Number.isSafeInteger(atMs) ? atMs : Date.now());
    const changed = this.phaseValue !== phase || this.resultValue !== terminal || this.endedAt !== endedAt;
    if (changed) this.version += 1;
    this.phaseValue = phase;
    this.resultValue = terminal;
    this.endedAt = endedAt;
    if (wasActive) this.tracker.apply({ type: "run_end", atMs: endedAt, state: executionState(phase) });
    this.queueLatest();
  }

  private persistPhase(phase: HostMergeProgressPhase, endedAt: number | null, result: string | null): boolean {
    if (this.deliveryState === undefined) return true;
    try {
      this.deps.store.updateHostMergeProgressCard(
        this.botUid,
        this.target,
        this.sourceId,
        phase,
        endedAt,
        result,
      );
      return true;
    } catch (error) {
      console.error(
        `[thread-merge-progress] 无法保存进度 target=${targetKey(this.target)}：`
        + safeErrorMessage(error, "progress card persistence failed"),
      );
      return false;
    }
  }

  private refreshElapsed(): void {
    if (this.disabled || !isActivePhase(this.phaseValue) || this.deps.stopping()) return;
    this.version += 1;
    this.queueLatest();
  }

  private snapshot(): ExecutionSnapshot {
    const current = this.tracker.snapshot(Date.now());
    return {
      ...current,
      hostMerge: {
        phase: this.phaseValue,
        ...(this.resultValue === undefined ? {} : { result: this.resultValue }),
      },
    };
  }

  private queueLatest(): void {
    if (this.disabled) return;
    this.dirty = true;
    if (this.queued) return;
    this.queued = true;
    const next = this.tailValue.then(async () => {
      while (this.dirty && !this.disabled) {
        this.dirty = false;
        const version = this.version;
        try {
          await this.deliver(version);
        } catch (error) {
          if (!this.failureLogged) {
            this.failureLogged = true;
            console.error(
              `[thread-merge-progress] 卡片投递失败 target=${targetKey(this.target)}：`
              + safeErrorMessage(error, "progress card request failed"),
            );
          }
        }
      }
    });
    this.tailValue = next.catch(() => {});
    this.deps.trackOperation(this.tailValue);
    void this.tailValue.then(() => {
      this.queued = false;
      if (this.dirty && !this.disabled) this.queueLatest();
      else if (!isActivePhase(this.phaseValue)) this.release();
    });
  }

  private async deliver(version: number): Promise<void> {
    const progress = this.snapshot();
    if (this.messageId === undefined) {
      if (this.deliveryState !== "ready") {
        this.holdUnknownDelivery();
        return;
      }
      try {
        this.deps.store.markHostMergeProgressCardAttempted(this.botUid, this.target, this.sourceId);
        this.deliveryState = "attempted";
      } catch (error) {
        this.discardUnsent();
        throw error;
      }
      let receipt: Awaited<ReturnType<ChannelTransport["sendProgressCard"]>>;
      try {
        receipt = await this.runActivityRequest(signal => this.deps.transport.sendProgressCard(
          this.target,
          progress,
          this.clientMsgNo,
          signal,
        ));
      } catch (error) {
        this.markUnknownDelivery();
        throw error;
      }
      if (receipt === null) {
        try {
          this.deps.store.markHostMergeProgressCardUnavailable(this.botUid, this.target, this.sourceId);
          this.deliveryState = "unavailable";
          this.discardUnsent();
          return;
        } catch (error) {
          this.markUnknownDelivery();
          throw error;
        }
      }
      if (typeof receipt?.messageId !== "string" || receipt.messageId.length === 0) {
        this.markUnknownDelivery();
        throw new Error("progress card create returned no verifiable message identity");
      }
      try {
        this.messageId = this.deps.store.bindHostMergeProgressCardMessage(
          this.botUid,
          this.target,
          this.sourceId,
          receipt.messageId,
        );
        this.deliveryState = "bound";
      } catch (error) {
        this.markUnknownDelivery();
        throw error;
      }
      if (this.version !== version) this.dirty = true;
      else if (!isActivePhase(this.phaseValue)) this.clearAfterDelivery();
      return;
    }
    const sequence = this.deps.store.recordHostMergeProgressCardAttempt(
      this.botUid,
      this.target,
      this.sourceId,
    );
    await this.runActivityRequest(signal => this.deps.transport.editProgressCard(
      this.target,
      this.messageId!,
      progress,
      sequence,
      signal,
    ));
    if (this.version !== version) this.dirty = true;
    else if (!isActivePhase(this.phaseValue)) this.clearAfterDelivery();
  }

  private async runActivityRequest<T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    const deadline = Promise.withResolvers<never>();
    const timer = setTimeout(() => {
      controller.abort();
      deadline.reject(new Error("progress card request timed out"));
    }, ACTIVITY_REQUEST_TIMEOUT_MS);
    try {
      return await Promise.race([work(controller.signal), deadline.promise]);
    } finally {
      clearTimeout(timer);
    }
  }

  private clearAfterDelivery(): void {
    if (this.clearRecord()) this.deliveryState = undefined;
    this.resolveArchiveBarrier();
  }

  private clearRecord(): boolean {
    try {
      this.deps.store.clearHostMergeProgressCard(this.botUid, this.target, this.sourceId);
      return true;
    } catch (error) {
      console.error(
        `[thread-merge-progress] 无法清除进度卡记录 target=${targetKey(this.target)}：`
        + safeErrorMessage(error, "progress card cleanup failed"),
      );
      return false;
    }
  }

  private markUnknownDelivery(): void {
    if (this.deliveryState === "attempted") {
      try {
        this.deps.store.markHostMergeProgressCardUnknown(this.botUid, this.target, this.sourceId);
      } catch (error) {
        console.error(
          `[thread-merge-progress] 无法持久化未知卡片投递状态 target=${targetKey(this.target)}：`
          + safeErrorMessage(error, "progress card delivery state update failed"),
        );
      }
      this.deliveryState = "unknown";
    }
    this.holdUnknownDelivery();
  }

  private resolveArchiveBarrier(): void {
    if (this.archiveBarrierReleased) return;
    this.archiveBarrierReleased = true;
    this.archiveBarrier.resolve();
  }

  private release(): void {
    if (this.released) return;
    this.released = true;
    this.stopTimer();
    this.onRelease(this);
    this.deps.releaseTail(this.target, this.operationId, this.archiveBarrier.promise);
  }
}
