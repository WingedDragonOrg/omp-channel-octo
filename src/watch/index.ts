import { randomUUID } from "node:crypto";
import { realpath, stat } from "node:fs/promises";
import type { ChatTarget, WorkerProcess } from "../contracts.ts";
import { groupAlive, isSafeGroupId, queryProcess, terminateProcessGroup } from "../omp/process.ts";
import type { TaskRecord } from "../store/store-records.ts";
import { collectWatch } from "./collector.ts";
import { createWatchJudge } from "./decision.ts";
import type {
  WatchCheckCompletion,
  WatchCollection,
  WatchCoordinatorOptions,
  WatchCreateArgs,
  WatchCreateResult,
  WatchJudgeResult,
  WatchListItem,
  WatchRecord,
} from "./types.ts";

export { collectWatch, createWatchJudge };
export type * from "./types.ts";

const DEFAULT_INTERVAL_SECONDS = 300;
const DEFAULT_TIMEOUT_SECONDS = 21_600;
const MIN_INTERVAL_SECONDS = 60;
const MAX_TIMEOUT_SECONDS = 86_400;
const MAX_CHECKS = 300;
const MAX_SCRIPT_BYTES = 16 * 1024;
const MAX_ARGV_BYTES = 16 * 1024;
const MAX_CONDITION_BYTES = 4_096;
const MAX_STDOUT_BYTES = 16 * 1024;
const PROCESS_STOP_GRACE_MS = 2_000;

interface CheckJob {
  record: WatchRecord;
  controller: AbortController;
  promise: Promise<void>;
}

export class WatchCoordinator {
  private readonly now: () => number;
  private readonly jobs = new Map<string, CheckJob>();
  private readonly heldUses = new Map<string, () => void>();
  private readonly dispatching = new Set<string>();
  private started = false;
  private stopping = false;
  private closed = false;
  private readyPromise: Promise<void> | null = null;
  private stopPromise: Promise<void> | null = null;
  private pumpPromise: Promise<void> | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(private readonly options: WatchCoordinatorOptions) {
    this.now = options.now ?? Date.now;
  }

  /** Synchronously rejects new starts/creates before the host begins asynchronous shutdown. */
  seal(): void {
    this.closed = true;
    this.stopping = true;
    this.clearTimer();
  }

  /** Recovers every persisted process identity before enabling any new collection. */
  start(): Promise<void> {
    if (this.closed) return Promise.reject(new Error("watch coordinator is sealed"));
    if (this.started) return Promise.resolve();
    if (this.readyPromise) return this.readyPromise;
    this.stopping = false;
    const ready = (async () => {
      await this.recoverPersistedProcesses();
      this.options.store.expireWatchRecords(this.now());
      if (this.stopping || this.closed) return;
      this.started = true;
      if (this.options.enabled()) this.schedule(0);
    })();
    const wrapped = ready.finally(() => {
      if (this.readyPromise === wrapped) this.readyPromise = null;
    });
    this.readyPromise = wrapped;
    return wrapped;
  }

  /** Stops scheduling and waits for every in-flight check and local wake acceptance to settle. */
  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.seal();
    const stopped = (async () => {
      if (this.readyPromise) await this.readyPromise.catch(() => undefined);
      for (const job of this.jobs.values()) job.controller.abort();
      if (this.pumpPromise) await this.pumpPromise.catch(() => undefined);
      await Promise.allSettled([...this.jobs.values()].map((job) => job.promise));
      this.started = false;
    })();
    const wrapped = stopped.finally(() => {
      if (this.stopPromise === wrapped) this.stopPromise = null;
      this.stopping = false;
    });
    this.stopPromise = wrapped;
    return wrapped;
  }

  async create(task: TaskRecord, args: WatchCreateArgs, toolCallId?: string): Promise<WatchCreateResult> {
    if (this.closed) throw new Error("watch coordinator is sealed");
    if (!toolCallId?.trim()) throw new Error("watch create requires the native tool call id");
    await this.start();
    if (!this.options.enabled()) throw new Error("watch is disabled");
    const botUid = this.options.botUid();
    if (!botUid || task.botUid !== botUid || !this.options.permitted(task)) throw new Error("watch is not permitted for this task");

    const source = this.options.store.getTask(task.id);
    if (!source || source.botUid !== botUid || source.id !== task.id || !sameTarget(source, task)) {
      throw new Error("watch source task is not persisted in this chat");
    }
    if (source.messageId.startsWith("watch:") || task.messageId.startsWith("watch:")) {
      throw new Error("a watch wake task cannot create another watch");
    }

    const spec = await this.buildSpec(source, args);
    if (this.closed || !this.options.enabled() || this.options.botUid() !== botUid ||
        !this.options.permitted(source) || !this.options.store.getTask(source.id)) {
      throw new Error("watch coordinator stopped or source task is no longer eligible");
    }
    const createdAt = this.now();
    const timeoutMs = spec.timeoutMs;
    const record: WatchRecord = {
      id: randomUUID(),
      botUid,
      target: { channelType: source.channelType, channelId: source.channelId },
      sourceTaskId: source.id,
      creationKey: toolCallId,
      spec,
      createdAt,
      deadlineAt: createdAt + timeoutMs,
      nextAt: createdAt,
      checks: 0,
      maxChecks: Math.ceil(timeoutMs / spec.intervalMs),
      generation: 0,
      failures: 0,
      state: "waiting",
      latest: null,
      matchedEvidence: null,
      process: null,
      wakeTaskId: null,
      wakeStatus: "none",
      stopReason: null,
      matchedAt: null,
    };
    const result = this.options.store.createWatch(record);
    if (result.kind === "created" && this.started && this.options.enabled()) this.schedule(0);
    return result;
  }

  list(target: ChatTarget, limit = 25, offset = 0): WatchListItem[] {
    const botUid = this.options.botUid();
    if (!botUid) return [];
    return this.options.store.listWatches(botUid, target, limit, offset);
  }

  get(target: ChatTarget, id: string): WatchRecord | null {
    const botUid = this.options.botUid();
    if (!botUid) return null;
    const record = this.options.store.getWatch(botUid, id);
    return record && sameTarget(record, target) ? record : null;
  }

  /** The store transaction revokes first; only then are matching check signals aborted. */
  stopWatch(target: ChatTarget, id: string | "all"): WatchRecord[] {
    const botUid = this.options.botUid();
    if (!botUid) return [];
    const stopped = this.options.store.stopWatches(botUid, target, id, this.now(), "operator-stop");
    const ids = new Set(stopped.map((record) => record.id));
    for (const [watchId, job] of this.jobs) {
      if (ids.has(watchId) && sameTarget(job.record, target)) job.controller.abort();
    }
    return stopped;
  }

  /** Tombstone and revoke the full chat before aborting collectors or releasing shared cwd use. */
  deleteChat(target: ChatTarget): WatchRecord[] {
    const botUid = this.options.botUid();
    if (!botUid) return [];
    this.options.store.deleteWatchChat(botUid, target, this.now());
    const deleted = this.options.store.listWatchRecords(botUid).filter((record) => sameTarget(record, target));
    for (const job of this.jobs.values()) {
      if (sameTarget(job.record, target)) job.controller.abort();
    }
    return deleted;
  }

  wakeAllowed(id: string): boolean {
    const botUid = this.options.botUid();
    if (this.closed || !botUid || !this.options.enabled()) return false;
    const record = this.options.store.getWatch(botUid, id);
    return !!record && this.options.permitted(record.target) && this.options.store.wakeAllowed(id, this.now());
  }

  markWakeDispatched(id: string, taskId: string): boolean {
    if (!this.wakeAllowed(id)) return false;
    return this.options.store.markWatchWakeDispatched(id, taskId, this.now());
  }

  private async buildSpec(source: TaskRecord, args: WatchCreateArgs): Promise<WatchRecord["spec"]> {
    if (typeof args.condition !== "string" || Buffer.byteLength(args.condition, "utf8") > MAX_CONDITION_BYTES || !args.condition.trim()) {
      throw new Error("watch condition must be non-empty and at most 4096 UTF-8 bytes");
    }

    let argv: string[];
    if (typeof args.script === "string") {
      if (args.command !== undefined || args.args !== undefined) throw new Error("watch accepts script or command/args, not both");
      if (!args.script.trim() || Buffer.byteLength(args.script, "utf8") > MAX_SCRIPT_BYTES) {
        throw new Error("watch script must be non-empty and at most 16384 UTF-8 bytes");
      }
      argv = ["/bin/bash", "-c", args.script];
    } else {
      if (typeof args.command !== "string" || !args.command.trim()) throw new Error("watch requires script or command");
      if (args.args !== undefined && (!Array.isArray(args.args) || args.args.some((arg) => typeof arg !== "string"))) {
        throw new Error("watch args must be an array of strings");
      }
      argv = [args.command, ...(args.args ?? [])];
    }
    if (argv.some((part) => part.includes("\0")) || Buffer.byteLength(JSON.stringify(argv), "utf8") > MAX_ARGV_BYTES) {
      throw new Error("watch argv is invalid or exceeds 16384 UTF-8 bytes");
    }

    const intervalSeconds = args.interval ?? DEFAULT_INTERVAL_SECONDS;
    const timeoutSeconds = args.timeout ?? DEFAULT_TIMEOUT_SECONDS;
    if (!Number.isSafeInteger(intervalSeconds) || intervalSeconds < MIN_INTERVAL_SECONDS) {
      throw new Error("watch interval must be an integer of at least 60 seconds");
    }
    if (!Number.isSafeInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > MAX_TIMEOUT_SECONDS) {
      throw new Error("watch timeout must be an integer from 1 through 86400 seconds");
    }
    const intervalMs = intervalSeconds * 1_000;
    const timeoutMs = timeoutSeconds * 1_000;
    if (Math.ceil(timeoutMs / intervalMs) > MAX_CHECKS) throw new Error("watch schedule exceeds 300 checks");

    const requestedCwd = args.cwd ?? source.cwd;
    const cwd = await realpath(requestedCwd);
    if (!(await stat(cwd)).isDirectory()) throw new Error("watch cwd is not a directory");
    return { argv, cwd, condition: args.condition, intervalMs, timeoutMs };
  }

  private async recoverPersistedProcesses(): Promise<void> {
    for (const record of this.options.store.listWatchProcessRecords()) {
      const processIdentity = record.process;
      if (!processIdentity) {
        let release = this.heldUses.get(record.id);
        if (!release) {
          try {
            release = await this.options.acquireUse(record);
          } catch {
            release = undefined;
          }
          if (release) this.heldUses.set(record.id, release);
        }
        continue;
      }
      let release = this.heldUses.get(record.id);
      if (!release) {
        try {
          release = await this.options.acquireUse(record);
        } catch {
          release = undefined;
        }
        if (release) this.heldUses.set(record.id, release);
      }

      let groupIsAlive = false;
      try {
        groupIsAlive = groupAlive(processIdentity.pgid);
      } catch {
        groupIsAlive = true;
      }
      if (!groupIsAlive) {
        this.confirmRecoveredExit(record);
        continue;
      }

      if (!release || !isSafeGroupId(processIdentity.pgid)) {
        this.options.store.markWatchUnknown(record.id, record.generation, "process-identity-unconfirmed", this.now());
        continue;
      }
      let snapshot;
      try {
        snapshot = await queryProcess(processIdentity.pid);
      } catch {
        snapshot = undefined;
      }
      if (!snapshot || snapshot.identity !== processIdentity.identity || snapshot.pgid !== processIdentity.pgid) {
        this.options.store.markWatchUnknown(record.id, record.generation, "process-identity-unconfirmed", this.now());
        continue;
      }

      let stopped = false;
      try {
        stopped = await terminateProcessGroup(processIdentity.pgid, PROCESS_STOP_GRACE_MS);
      } catch {
        stopped = false;
      }
      if (stopped && !groupAlive(processIdentity.pgid)) this.confirmRecoveredExit(record);
      else this.options.store.markWatchUnknown(record.id, record.generation, "process-exit-unconfirmed", this.now());
    }
  }

  private confirmRecoveredExit(record: WatchRecord): void {
    if (!record.process) return;
    this.options.store.setWatchProcess(record.id, record.generation, null);
    const release = this.heldUses.get(record.id);
    if (release) this.releaseUse(record.id, release);
    this.options.store.recoverWatchProcess(record.id, record.generation, this.now());
  }

  private schedule(delayMs: number): void {
    if (!this.started || this.closed || this.stopping || !this.options.enabled()) return;
    this.clearTimer();
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.pump();
    }, Math.max(0, Math.min(delayMs, 2_147_000_000)));
    this.timer.unref?.();
  }

  private clearTimer(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
  }

  private pump(): Promise<void> {
    if (!this.started || this.closed || this.stopping) return Promise.resolve();
    if (this.pumpPromise) return this.pumpPromise;
    const current = this.runPump().finally(() => {
      if (this.pumpPromise === current) this.pumpPromise = null;
      this.scheduleNext();
    });
    this.pumpPromise = current;
    return current;
  }

  private async runPump(): Promise<void> {
    const now = this.now();
    this.options.store.expireWatchRecords(now);
    for (const job of this.jobs.values()) {
      if (job.record.deadlineAt <= now) job.controller.abort();
    }
    const botUid = this.options.botUid();
    if (botUid) this.revokeUnpermitted(botUid);
    if (this.options.enabled() && !this.closed) await this.dispatchQueuedWakes();
    if (!this.options.enabled() || this.closed || this.stopping || !botUid) return;
    const due = this.options.store.claimDueWatches(now, 2, botUid);
    for (const record of due) this.launchCheck(record);
  }

  private revokeUnpermitted(botUid: string): void {
    for (const record of this.options.store.listActiveWatchRecords(botUid)) {
      let permitted = false;
      try {
        permitted = this.options.permitted(record.target);
      } catch {
        permitted = false;
      }
      if (permitted) continue;
      const revoked = this.options.store.stopWatches(botUid, record.target, record.id, this.now(), "permission-revoked");
      if (revoked.length > 0) this.jobs.get(record.id)?.controller.abort();
    }
  }

  private async dispatchQueuedWakes(): Promise<void> {
    const botUid = this.options.botUid();
    if (!botUid || this.closed) return;
    const queued = this.options.store.listActiveWatchRecords(botUid)
      .filter((record) => record.state === "matched" && record.wakeStatus === "queued");
    for (const record of queued) {
      if (this.stopping || this.closed || this.dispatching.has(record.id)) continue;
      const dispatch = this.dispatchWake(record);
      this.dispatching.add(record.id);
      try {
        await dispatch;
      } finally {
        this.dispatching.delete(record.id);
      }
    }
  }

  private async dispatchWake(record: WatchRecord): Promise<void> {
    const now = this.now();
    if (this.closed || this.stopping || !this.options.enabled() || this.options.botUid() !== record.botUid) return;
    if (!this.options.store.wakeAllowed(record.id, now)) return;
    if (!this.options.permitted(record.target)) {
      this.options.store.markWatchWakeFailed(record.id, "chat-permission-revoked");
      return;
    }
    const text = formatWake(record);
    try {
      const taskId = await this.options.deliver(record, text);
      if (!taskId) {
        if (!this.closed && !this.stopping) this.options.store.markWatchWakeFailed(record.id, "wake-task-not-accepted");
        return;
      }
      this.options.store.markWatchWakeTask(record.id, taskId, this.now());
    } catch {
      if (!this.closed && !this.stopping) this.options.store.markWatchWakeFailed(record.id, "wake-delivery-failed");
    }
  }

  private launchCheck(record: WatchRecord): void {
    const controller = new AbortController();
    const job: CheckJob = { record, controller, promise: Promise.resolve() };
    this.jobs.set(record.id, job);
    job.promise = this.executeCheck(record, controller).catch(() => undefined).finally(() => {
      if (this.jobs.get(record.id) === job) this.jobs.delete(record.id);
      this.schedule(0);
    });
  }

  private async executeCheck(record: WatchRecord, controller: AbortController): Promise<void> {
    const { signal } = controller;
    let release: (() => void) | undefined;
    let processPersisted = false;
    let exitConfirmed = false;
    let unknownCode: string | undefined;
    let unknownKeepsUse = false;
    let completion: WatchCheckCompletion | undefined;
    let checkSettled = false;
    try {
      release = await this.options.acquireUse(record);
      if (!release) {
        completion = { kind: "failure", collectedAt: this.now(), code: "cwd-use-unavailable" };
      } else {
        this.heldUses.set(record.id, release);
        const onSpawn = (identity: WorkerProcess): void => {
          if (!this.options.store.setWatchProcess(record.id, record.generation, identity)) {
            throw new Error("watch process identity could not be persisted");
          }
          processPersisted = true;
        };
        const onExit = (): void => {
          if (!processPersisted || exitConfirmed) return;
          exitConfirmed = true;
          this.options.store.setWatchProcess(record.id, record.generation, null);
          this.releaseUse(record.id, release!);
          release = undefined;
          if (checkSettled) {
            this.options.store.recoverWatchProcess(record.id, record.generation, this.now());
            this.schedule(0);
          }
        };
        if (signal.aborted) throw new Error("watch check was aborted");
        const collection = await this.options.collect(record.spec, signal, onSpawn, onExit);
        if (collection.kind === "unknown" && !exitConfirmed) {
          if (!processPersisted && collection.process) {
            processPersisted = this.options.store.setWatchProcess(record.id, record.generation, collection.process);
          }
          unknownCode = collection.code;
          unknownKeepsUse = !processPersisted;
        } else if (processPersisted && !exitConfirmed) {
          unknownCode = "process-exit-unconfirmed";
        } else {
          completion = await this.toCompletion(record, collection, signal);
        }
      }
    } catch {
      if (processPersisted && !exitConfirmed) unknownCode = "process-exit-unconfirmed";
      else if (!signal.aborted) completion = { kind: "failure", collectedAt: this.now(), code: "collector-failed" };
    }

    if ((processPersisted && !exitConfirmed) || unknownKeepsUse) {
      this.options.store.markWatchUnknown(record.id, record.generation, unknownCode ?? "process-exit-unconfirmed", this.now());
      checkSettled = true;
      return;
    }
    if (release) {
      this.releaseUse(record.id, release);
      release = undefined;
    }
    if (signal.aborted) {
      this.options.store.recoverWatchProcess(record.id, record.generation, this.now());
      checkSettled = true;
      return;
    }
    if (unknownCode) {
      this.options.store.markWatchUnknown(record.id, record.generation, unknownCode, this.now());
      checkSettled = true;
      return;
    }
    if (completion) this.options.store.finishWatchCheck(record.id, record.generation, completion, this.now());
    checkSettled = true;
  }

  private async toCompletion(
    record: WatchRecord,
    collection: WatchCollection,
    signal: AbortSignal,
  ): Promise<WatchCheckCompletion> {
    const collectedAt = this.now();
    if (collection.kind !== "success") return { kind: "failure", collectedAt, code: collection.code };
    if (Buffer.byteLength(collection.stdout, "utf8") > MAX_STDOUT_BYTES) {
      return { kind: "failure", collectedAt, code: "stdout-limit-exceeded" };
    }
    if (collectedAt >= record.deadlineAt) return { kind: "failure", collectedAt, code: "deadline" };
    if (signal.aborted) return { kind: "failure", collectedAt, code: "cancelled" };
    if (!this.options.judge) return { kind: "failure", collectedAt, code: "judge-unavailable" };

    let judged: WatchJudgeResult;
    try {
      judged = await this.options.judge({ condition: record.spec.condition, stdout: collection.stdout, collectedAt }, signal);
    } catch {
      return { kind: "failure", collectedAt: this.now(), code: "judge-failed" };
    }
    if (judged.kind === "failure") return { kind: "failure", collectedAt: this.now(), code: judged.code };
    if (!Number.isFinite(judged.probability) || judged.probability < 0 || judged.probability > 1) {
      return { kind: "failure", collectedAt: this.now(), code: "invalid-judgment" };
    }
    return {
      kind: "judgment",
      collectedAt,
      stdout: collection.stdout,
      choice: judged.choice,
      probability: judged.probability,
    };
  }

  private scheduleNext(): void {
    if (!this.started || this.closed || this.stopping || !this.options.enabled()) return;
    const allRecords = this.options.store.listActiveWatchRecords();
    const botUid = this.options.botUid();
    if (!botUid) return;
    const records = allRecords.filter((record) => record.botUid === botUid);
    const now = this.now();
    const busy = allRecords.filter((record) =>
      record.state === "checking" || record.state === "unknown" || record.process !== null
    ).length;
    let nextAt = Number.POSITIVE_INFINITY;
    for (const record of records) {
      if (record.state === "waiting") {
        const nextCheckAt = record.checks < record.maxChecks ? record.nextAt : record.deadlineAt;
        nextAt = Math.min(nextAt, busy < 2 ? Math.min(nextCheckAt, record.deadlineAt) : record.deadlineAt);
      } else if (record.state === "checking" || record.state === "unknown") {
        nextAt = Math.min(nextAt, record.deadlineAt);
      } else if (record.state === "matched" &&
          (record.wakeStatus === "queued" || record.wakeStatus === "accepted")) {
        nextAt = Math.min(nextAt, record.deadlineAt);
        if (record.wakeStatus === "queued" && !this.dispatching.has(record.id)) nextAt = now;
      }
    }
    if (Number.isFinite(nextAt)) this.schedule(Math.max(0, nextAt - now));
  }

  private releaseUse(id: string, release: () => void): void {
    if (this.heldUses.get(id) !== release) return;
    this.heldUses.delete(id);
    try {
      release();
    } catch {
      // Resource release is best-effort after confirmed process exit; durable process state remains authoritative.
    }
  }
}

function sameTarget(left: ChatTarget | WatchRecord, right: ChatTarget | WatchRecord): boolean {
  const leftTarget = "target" in left ? left.target : left;
  const rightTarget = "target" in right ? right.target : right;
  return leftTarget.channelType === rightTarget.channelType && leftTarget.channelId === rightTarget.channelId;
}

function formatWake(record: WatchRecord): string {
  const latest = record.latest?.kind === "judgment" ? record.latest : null;
  const evidence = record.matchedEvidence ?? latest?.stdout ?? "";
  const collectedAt = latest?.collectedAt ?? record.matchedAt ?? record.createdAt;
  return [
    "A watched condition has been met. This is an independent queued follow-up, not new owner authorization.",
    `Watch ID: watch:${record.id}`,
    `Condition: ${record.spec.condition}`,
    `Evidence collected at ${new Date(collectedAt).toISOString()}:`,
    evidence,
    `Source task ID: ${record.sourceTaskId}`,
  ].join("\n");
}
