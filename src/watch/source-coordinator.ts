import { randomUUID } from "node:crypto";
import { parseWatchResource, createWatchSource } from "./sources.ts";
import type { WatchSourceHooks } from "./sources.ts";
import type { ChatTarget } from "../contracts.ts";
import type { TaskRecord } from "../store/store-records.ts";
import type { WatchJudge } from "./types.ts";
import type {
  SourceWatchCreateArgs,
  SourceWatchCreateResult,
  SourceWatchDecision,
  SourceWatchEventRecord,
  SourceWatchEndWhen,
  WatchSourceFactory,
  SourceWatchListItem,
  SourceWatchRecord,
  SourceWatchSpec,
  WatchSource,
  WatchSourceObservation,
} from "./source-types.ts";
import { throwIfAborted } from "../abort.ts";

const DEFAULT_TIMEOUT_MS = 6 * 60 * 60 * 1_000;
const MAX_TIMEOUT_MS = 24 * 60 * 60 * 1_000;
const MAX_SOURCE_BUDGET = 300;
const MAX_DECISION_BUDGET = 300;
const MAX_WAKE_BUDGET = 20;
const MAX_CONDITION_BYTES = 4_096;
const MAX_SCOPE_BYTES = 4_096;
const MAX_RESOURCE_BYTES = 2_048;
const MAX_BODY_BYTES = 16 * 1_024;
const MAX_EVENT_TYPES = 20;
const DECISION_RETRY_MS = 30_000;
const GITHUB_MIN_POLL_MS = 60_000;
const GITLAB_MIN_POLL_MS = 30_000;
const BASELINE_IDENTITY = "__watch_baseline_snapshot__";

interface SourceWatchJob {
  controller: AbortController;
  promise: Promise<void>;
}

export interface SourceWatchCoordinatorOptions {
  store: import("./source-types.ts").SourceWatchStore;
  botUid: () => string | undefined;
  enabled: () => boolean;
  permitted: (target: ChatTarget) => boolean;
  judge: WatchJudge | undefined;
  decisionProfile: import("./source-types.ts").SourceWatchDecisionProfile | null;
  deliver: (record: SourceWatchRecord, event: SourceWatchEventRecord, messageId: string, text: string) => Promise<string | null>;
  sourceFactory?: WatchSourceFactory;
  now?: () => number;
}

export class SourceWatchCoordinator {
  private readonly now: () => number;
  private readonly jobs = new Map<string, SourceWatchJob>();
  private readonly baselineControllers = new Map<string, AbortController>();
  private readonly dispatching = new Set<string>();
  private readonly ready = new Map<string, Promise<void>>();
  private started = false;
  private stopping = false;
  private closed = false;
  private stopPromise: Promise<void> | null = null;

  constructor(private readonly options: SourceWatchCoordinatorOptions) {
    this.now = options.now ?? Date.now;
  }

  seal(): void {
    this.closed = true;
    this.stopping = true;
  }

  async start(): Promise<void> {
    if (this.closed) throw new Error("source watch coordinator is sealed");
    if (this.started) return;
    this.stopping = false;
    this.started = true;
    const botUid = this.options.botUid();
    if (!botUid) return;
    const now = this.now();
    this.options.store.expireSourceWatches(now);
    for (const record of this.options.store.listActiveSourceWatches(botUid)) {
      const recovered = this.options.store.recoverSourceWatch(record.id, now);
      if (!recovered || recovered.state === "failed" || recovered.state === "expired" || recovered.state === "stopped" || recovered.state === "deleted") {
        continue;
      }
      this.launch(recovered);
    }
    await this.dispatchQueuedWakes();
  }

  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.seal();
    const stopped = (async () => {
      for (const controller of this.baselineControllers.values()) controller.abort();
      for (const job of this.jobs.values()) job.controller.abort();
      await Promise.allSettled([...this.jobs.values()].map(job => job.promise));
      await Promise.allSettled([...this.ready.values()]);
      this.started = false;
    })();
    const wrapped = stopped.finally(() => {
      if (this.stopPromise === wrapped) this.stopPromise = null;
      this.stopping = false;
    });
    this.stopPromise = wrapped;
    return wrapped;
  }

  async create(task: TaskRecord, args: SourceWatchCreateArgs, toolCallId?: string): Promise<SourceWatchCreateResult> {
    if (this.closed) throw new Error("source watch coordinator is sealed");
    if (!toolCallId?.trim()) throw new Error("source watch create requires the native tool call id");
    await this.start();
    if (!this.options.enabled()) throw new Error("source watch is disabled");
    const botUid = this.options.botUid();
    if (!botUid || task.botUid !== botUid || !this.options.permitted(task)) throw new Error("source watch is not permitted for this task");
    const sourceTask = this.options.store.getTask(task.id);
    if (!sourceTask || sourceTask.botUid !== botUid || sourceTask.id !== task.id || !sameTarget(sourceTask, task)) {
      throw new Error("source watch source task is not persisted in this chat");
    }
    if (sourceTask.messageId.startsWith("watch:")) throw new Error("watch wake tasks cannot create another watch");

    const spec = buildSpec(args, this.options.decisionProfile);
    if ((spec.consumeCondition !== null || spec.endCondition !== null) && !this.options.judge) {
      throw new Error("semantic source watch requires an available Decisions service");
    }
    parseWatchResource(spec.resource);
    const createdAt = this.now();
    const record: SourceWatchRecord = {
      id: `source-${randomUUID()}`,
      botUid,
      channelType: sourceTask.channelType,
      channelId: sourceTask.channelId,
      sourceTaskId: sourceTask.id,
      creationKey: toolCallId,
      spec,
      createdAt,
      deadlineAt: createdAt + spec.timeoutMs,
      state: "initializing",
      generation: 0,
      cursor: null,
      snapshot: null,
      snapshotSequence: 0,
      sourceUsed: 0,
      decisionUsed: 0,
      wakeUsed: 0,
      failures: 0,
      gap: null,
      latestError: null,
      stopReason: null,
      observedAt: null,
    };
    const created = this.options.store.createSourceWatch(record);
    if (!created.record) return created;
    if (created.kind === "duplicate") {
      const existing = created.record;
      if (existing.state === "initializing") await this.ensureBaseline(existing);
      const current = this.options.store.getSourceWatch(botUid, existing.id) ?? existing;
      if (current.state === "watching" || current.state === "budget-exhausted") this.launch(current);
      else if (current.state === "ended") await this.dispatchWatchWakes(current);
      return { kind: "duplicate", record: current };
    }

    await this.ensureBaseline(created.record);
    const readyRecord = this.options.store.getSourceWatch(botUid, created.record.id) ?? created.record;
    if (readyRecord.state === "watching" || readyRecord.state === "budget-exhausted") this.launch(readyRecord);
    else if (readyRecord.state === "ended") await this.dispatchWatchWakes(readyRecord);
    return { kind: "created", record: readyRecord };
  }

  discover(): Record<string, unknown> {
    const available = this.options.enabled();
    const decisionsConfigured = this.options.decisionProfile !== null && this.options.judge !== undefined;
    return {
      sources: [
        {
          provider: "github",
          resources: ["github://<owner>/<repo>/pull/<number>", "https://github.com/<owner>/<repo>/pull/<number>"],
          events: ["snapshot", "comment", "review"],
          method: "read-only GitHub REST requests; per-source polling and rate-limit aware scheduling",
          fields: ["state", "merged", "title", "head SHA", "comment/review identity, version, and bounded body"],
          limits: ["no inline review comments", "no CI events", "pagination is best-effort: changes deleted, overwritten, or delayed between scans may be missed"],
          available,
        },
        {
          provider: "gitlab",
          resources: ["gitlab://<host>/<namespace>/<project>/-/merge_requests/<iid>", "https://<host>/<namespace>/<project>/-/merge_requests/<iid>"],
          events: ["snapshot", "note"],
          method: "read-only MR and notes API; explicit instance host; per-source polling and rate-limit aware scheduling",
          fields: ["state", "merged", "title", "head SHA", "note identity, version, and bounded body"],
          limits: ["no guarantee of all inline discussions", "no CI events", "pagination is best-effort: changes deleted, overwritten, or delayed between scans may be missed"],
          available,
        },
      ],
      decisions: {
        configured: decisionsConfigured,
        service: decisionsConfigured ? this.options.decisionProfile : null,
        judgments: ["consume", "end"],
        authorization: "Each subscription must explicitly authorize the selected Decisions service and bounded source fields; operator configuration alone is not consent.",
      },
      budgets: { defaultTimeoutSeconds: 21_600, maxTimeoutSeconds: 86_400, sourceRequests: 300, decisions: 300, followUps: 20 },
    };
  }

  list(target: ChatTarget, limit = 25, offset = 0): SourceWatchListItem[] {
    const botUid = this.options.botUid();
    return botUid ? this.options.store.listSourceWatchesForChat(botUid, target, limit, offset) : [];
  }

  get(target: ChatTarget, id: string): SourceWatchRecord | null {
    const botUid = this.options.botUid();
    if (!botUid) return null;
    const record = this.options.store.getSourceWatch(botUid, id);
    return record && sameTarget(record, target) ? record : null;
  }

  stopWatch(target: ChatTarget, id: string | "all"): SourceWatchRecord[] {
    const botUid = this.options.botUid();
    if (!botUid) return [];
    const stopped = this.options.store.stopSourceWatches(botUid, target, id, this.now(), "operator-stop");
    const ids = new Set(stopped.map(record => record.id));
    for (const watchId of ids) {
      this.baselineControllers.get(watchId)?.abort();
      this.jobs.get(watchId)?.controller.abort();
    }
    return stopped;
  }

  deleteChat(target: ChatTarget): SourceWatchRecord[] {
    const botUid = this.options.botUid();
    if (!botUid) return [];
    const deleted = this.options.store.deleteSourceWatchChat(botUid, target, this.now());
    const ids = new Set(deleted.map(record => record.id));
    for (const watchId of ids) {
      this.baselineControllers.get(watchId)?.abort();
      this.jobs.get(watchId)?.controller.abort();
    }
    return deleted;
  }

  wakeAllowed(target: ChatTarget, messageId: string): boolean {
    const botUid = this.options.botUid();
    if (this.closed || !botUid || !this.options.enabled()) return false;
    const pair = this.options.store.getSourceWakeByMessageId(botUid, target, messageId);
    if (!pair) return false;
    const watch = this.options.store.getSourceWatch(botUid, pair.watchId);
    return !!watch && this.options.permitted(watch) && this.options.store.sourceWakeAllowed(pair.id, this.now());
  }

  markWakeDispatched(target: ChatTarget, messageId: string, taskId: string, sessionId: string): boolean {
    const botUid = this.options.botUid();
    if (this.closed || !botUid || !this.options.enabled()) return false;
    const pair = this.options.store.getSourceWakeByMessageId(botUid, target, messageId);
    if (!pair) return false;
    const watch = this.options.store.getSourceWatch(botUid, pair.watchId);
    if (!watch || !this.options.permitted(watch)) return false;
    if (pair.wakeStatus === "queued" && !this.options.store.markSourceWakeTask(pair.id, taskId, this.now())) return false;
    return this.options.store.markSourceWakeDispatched(pair.id, taskId, sessionId, this.now());
  }

  private async ensureBaseline(record: SourceWatchRecord): Promise<void> {
    const existing = this.ready.get(record.id);
    if (existing) return existing;
    const operation = this.initialize(record);
    const wrapped = operation.finally(() => {
      if (this.ready.get(record.id) === wrapped) this.ready.delete(record.id);
    });
    this.ready.set(record.id, wrapped);
    return wrapped;
  }

  private async initialize(record: SourceWatchRecord): Promise<void> {
    const controller = new AbortController();
    this.baselineControllers.set(record.id, controller);
    const source = this.source(record);
    const timeout = this.deadlineController(record, controller);
    try {
      if (!this.options.permitted(record)) {
        this.revoke(record);
        return;
      }
      let observation: WatchSourceObservation = { kind: "failure", failure: { code: "source-initialize-failed" } };
      for (let attempt = 0; attempt < 3; attempt += 1) {
        if (controller.signal.aborted) return;
        try {
          observation = await this.observe(record, source, "initialize", null, controller.signal);
        } catch (error) {
          if (controller.signal.aborted) return;
          if (error instanceof SourceBudgetError) {
            this.options.store.failSourceWatch(
              record.id,
              record.generation,
              "source-budget-exhausted-before-baseline",
              this.now(),
              "baseline-incomplete: source budget exhausted",
            );
            return;
          }
          observation = { kind: "failure", failure: { code: "source-initialize-failed" } };
        }
        if (observation.kind !== "failure") break;
        this.recordFailure(record, observation.failure.code);
        const current = this.options.store.getSourceWatch(record.botUid, record.id);
        if (current?.state !== "initializing" || attempt === 2) return;
        await this.sleepUntil(this.now() + Math.min(1_000, Math.max(0, current.deadlineAt - this.now())), controller.signal);
      }
      if (controller.signal.aborted) return;
      if (observation.kind === "gap") {
        this.options.store.failSourceWatch(
          record.id,
          record.generation,
          observation.gap.code,
          this.now(),
          observation.gap.code,
        );
        return;
      }
      if (observation.kind === "failure") return;
      if (!this.options.permitted(record)) {
        this.revoke(record);
        return;
      }
      const now = this.now();
      const valid = this.options.store.initializeSourceWatch(
        record.id,
        record.generation,
        observation.cursor,
        observation.snapshot,
        observation.events,
        now,
      );
      if (!valid) return;
      const current = this.options.store.getSourceWatch(record.botUid, record.id);
      if (current) await this.processPendingEvents(current, controller.signal);
    } finally {
      clearTimeout(timeout);
      if (this.baselineControllers.get(record.id) === controller) this.baselineControllers.delete(record.id);
      const current = this.options.store.getSourceWatch(record.botUid, record.id);
      if (current?.state === "watching" || current?.state === "budget-exhausted") {
        await this.dispatchWatchWakes(current);
        this.launch(current);
      } else if (current?.state === "ended") {
        await this.dispatchWatchWakes(current);
        this.launch(current);
      }
    }

  }
  private launch(record: SourceWatchRecord): void {
    if (this.closed || this.stopping || this.jobs.has(record.id) ||
        (record.state !== "watching" && record.state !== "budget-exhausted" && record.state !== "ended")) return;
    const controller = new AbortController();
    const job: SourceWatchJob = { controller, promise: Promise.resolve() };
    this.jobs.set(record.id, job);
    job.promise = this.runWatch(record, controller).catch(() => undefined).finally(() => {
      if (this.jobs.get(record.id) === job) this.jobs.delete(record.id);
    });
  }

  private async runWatch(initial: SourceWatchRecord, controller: AbortController): Promise<void> {
    const source = this.source(initial);
    const timeout = this.deadlineController(initial, controller);
    try {
      while (!controller.signal.aborted && !this.closed && !this.stopping) {
        const botUid = this.options.botUid();
        if (!botUid || botUid !== initial.botUid) return;
        let record = this.options.store.getSourceWatch(botUid, initial.id);
        if (!record) return;
        if (record.deadlineAt <= this.now()) {
          this.options.store.expireSourceWatches(this.now());
          return;
        }
        if (!this.options.enabled()) return;
        if (!this.options.permitted(record)) {
          this.revoke(record);
          return;
        }
        if (record.state === "ended") {
          await this.processPendingEvents(record, controller.signal);
          record = this.options.store.getSourceWatch(botUid, initial.id);
          if (!record) return;
          await this.dispatchWatchWakes(record);
          const pending = this.options.store.listSourceWatchEvents(record.id, Number.POSITIVE_INFINITY)
            .filter(event => event.state === "pending" || event.state === "judging");
          if (pending.length === 0) return;
          const nextAttemptAt = Math.min(...pending.map(event => event.nextAttemptAt));
          await this.sleepUntil(Math.max(this.now() + 1, nextAttemptAt), controller.signal);
          continue;
        }
        if (record.state === "failed" || record.state === "stopped" || record.state === "expired" || record.state === "deleted") return;
        if (record.state === "budget-exhausted") {
          await this.processPendingEvents(record, controller.signal);
          record = this.options.store.getSourceWatch(botUid, initial.id);
          if (record) await this.dispatchWatchWakes(record);
          if (!record || record.state !== "budget-exhausted") continue;
          const pending = this.options.store.listSourceWatchEvents(record.id, Number.POSITIVE_INFINITY)
            .filter(event => event.state === "pending" || event.state === "judging");
          if (pending.length > 0) {
            const nextAttemptAt = Math.min(...pending.map(event => event.nextAttemptAt));
            await this.sleepUntil(Math.max(this.now() + 1, nextAttemptAt), controller.signal);
            continue;
          }
          return;
        }
        await this.processPendingEvents(record, controller.signal);
        record = this.options.store.getSourceWatch(botUid, initial.id);
        if (record) await this.dispatchWatchWakes(record);
        if (!record || record.state !== "watching") continue;
        if (record.sourceUsed >= record.spec.sourceBudget ||
            decisionBudgetExhausted(record) ||
            record.wakeUsed >= record.spec.wakeBudget) {
          this.options.store.markSourceBudgetExhausted(record.id, record.generation, this.now());
          continue;
        }
        await this.sleepUntil(this.nextPollAt(record, source), controller.signal);
        if (controller.signal.aborted) return;
        if (!this.options.permitted(record)) {
          this.revoke(record);
          return;
        }
        let observation: WatchSourceObservation;
        try {
          observation = await this.observe(record, source, "poll", record.cursor, controller.signal);
        } catch (error) {
          if (controller.signal.aborted) return;
          if (error instanceof SourceBudgetError) {
            this.options.store.markSourceBudgetExhausted(record.id, record.generation, this.now());
          } else {
            this.recordFailure(record, "source-poll-failed");
          }
          continue;
        }
        if (controller.signal.aborted) return;
        if (observation.kind === "gap") {
          this.options.store.failSourceWatch(record.id, record.generation, observation.gap.code, this.now(), observation.gap.code);
          return;
        }
        if (observation.kind === "failure") {
          this.recordFailure(record, observation.failure.code);
          const afterFailure = this.options.store.getSourceWatch(record.botUid, record.id);
          if (afterFailure?.state !== "watching") return;
          if (afterFailure.sourceUsed >= afterFailure.spec.sourceBudget) {
            this.options.store.markSourceBudgetExhausted(afterFailure.id, afterFailure.generation, this.now());
          }
          await this.sleepUntil(this.nextPollAt(afterFailure, source), controller.signal);
          continue;
        }
        if (!this.options.permitted(record)) {
          this.revoke(record);
          return;
        }
        const committed = this.options.store.commitSourceObservation(
          record.id,
          record.generation,
          observation.cursor,
          observation.snapshot,
          observation.events,
          this.now(),
        );
        if (!committed) return;
        const current = this.options.store.getSourceWatch(record.botUid, record.id);
        if (current) await this.processPendingEvents(current, controller.signal);
        const latest = this.options.store.getSourceWatch(record.botUid, record.id);
        if (latest) await this.dispatchWatchWakes(latest);
        if (latest?.state === "watching" && (
          latest.sourceUsed >= latest.spec.sourceBudget ||
          decisionBudgetExhausted(latest) ||
          latest.wakeUsed >= latest.spec.wakeBudget
        )) this.options.store.markSourceBudgetExhausted(latest.id, latest.generation, this.now());
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  private async processPendingEvents(record: SourceWatchRecord, signal: AbortSignal): Promise<void> {
    const events = this.options.store.listSourceWatchEvents(
      record.id,
      record.state === "ended" ? Number.POSITIVE_INFINITY : this.now(),
    ).sort((left, right) => Number(isBaselineSnapshot(right)) - Number(isBaselineSnapshot(left)));
    let hasSatisfiedEnd = this.options.store.listSourceWatchEvents(record.id, Number.POSITIVE_INFINITY)
      .some(event => event.endState === "satisfied");
    for (let event of events) {
      if (this.closed || this.stopping) return;
      if (event.state === "ready" || event.state === "failed" || event.state === "cancelled") continue;
      const watch = this.options.store.getSourceWatch(record.botUid, record.id);
      if (!watch || watch.deadlineAt <= this.now()) return;
      if (watch.state === "ended") {
        if (!hasSatisfiedEnd) return;
        if (event.nextAttemptAt > this.now() && retryable(event.consumeState)) continue;
      } else if (watch.state !== "watching" && watch.state !== "budget-exhausted") {
        return;
      }
      if (!this.options.permitted(watch)) {
        this.revoke(watch);
        return;
      }
      const baseline = isBaselineSnapshot(event);
      const selected = !baseline && watch.spec.eventTypes.includes(event.type);
      let consumeState: SourceWatchEventRecord["consumeState"] = baseline || !selected
        ? "not_satisfied"
        : event.consumeState;
      let endState: SourceWatchEventRecord["endState"] = event.endState;
      if (selected && watch.spec.consumeCondition && retryable(consumeState)) {
        const result = await this.decide(watch, event, "consume", watch.spec.consumeCondition, signal);
        if (result === null) return;
        event = result;
        consumeState = event.consumeState;
      } else if (selected && !watch.spec.consumeCondition) {
        consumeState = "deterministic";
      }
      if (watch.spec.endCondition && watch.state !== "ended" && retryable(endState)) {
        const result = await this.decide(watch, event, "end", watch.spec.endCondition, signal);
        if (result === null) return;
        event = result;
        endState = event.endState;
      } else if (watch.spec.endWhen && retryable(endState)) {
        endState = endWhenMatches(watch.spec.endWhen, event, baseline) ? "satisfied" : "not_satisfied";
      } else if (!watch.spec.endCondition && !watch.spec.endWhen) {
        endState = "not_satisfied";
      }
      const endSatisfied = endState === "satisfied";
      hasSatisfiedEnd ||= endSatisfied;
      const completed = this.options.store.completeSourceEvent(
        watch.id,
        watch.generation,
        event.id,
        consumeState,
        endState,
        endSatisfied,
        this.now(),
      );
      if (!completed) continue;
      const latest = this.options.store.getSourceWatch(watch.botUid, watch.id);
      if (latest?.state === "watching" && (
        decisionBudgetExhausted(latest) || latest.wakeUsed >= latest.spec.wakeBudget
      )) this.options.store.markSourceBudgetExhausted(latest.id, latest.generation, this.now());
    }
  }


  private async decide(
    record: SourceWatchRecord,
    event: SourceWatchEventRecord,
    kind: "consume" | "end",
    condition: string,
    signal: AbortSignal,
  ): Promise<SourceWatchEventRecord | null> {
    const authorizedProfile = record.spec.decisionProfile;
    const currentProfile = this.options.decisionProfile;
    const profileMatches = authorizedProfile !== null && currentProfile !== null &&
      authorizedProfile.protocol === currentProfile.protocol &&
      authorizedProfile.endpoint === currentProfile.endpoint &&
      authorizedProfile.model === currentProfile.model;
    if (!this.options.judge || !record.spec.decisionConsent || !profileMatches) {
      this.options.store.markSourceEventFailure(
        record.id,
        event.id,
        this.options.judge && record.spec.decisionConsent && authorizedProfile && currentProfile
          ? "decision-profile-mismatch"
          : "decision-unavailable",
        this.now() + DECISION_RETRY_MS,
      );
      return event;
    }
    if (!this.options.permitted(record)) {
      this.revoke(record);
      return null;
    }
    const intent = this.options.store.beginSourceDecision(record.id, event.id, kind, record.generation, this.now());
    if (!intent) {
      const current = this.options.store.getSourceWatch(record.botUid, record.id);
      if (!current || current.state === "stopped" || current.state === "deleted" || current.state === "expired" ||
          current.state === "failed" || current.deadlineAt <= this.now()) return null;
      if (current.decisionUsed >= current.spec.decisionBudget) {
        this.options.store.markSourceEventFailure(
          record.id,
          event.id,
          "decision-budget-exhausted",
          this.now() + DECISION_RETRY_MS,
        );
        this.options.store.markSourceBudgetExhausted(record.id, record.generation, this.now());
        return event;
      }
      return null;
    }
    let decision: SourceWatchDecision | null = null;
    let error: string | null = null;
    try {
      const body = JSON.stringify({
        type: event.type,
        version: event.version,
        observedAt: new Date(event.observedAt).toISOString(),
        body: event.body,
      });
      if (Buffer.byteLength(body, "utf8") > MAX_BODY_BYTES) throw new Error("event body exceeds authorized limit");
      if (signal.aborted || !this.options.permitted(record)) {
        if (!signal.aborted) this.revoke(record);
        return null;
      }
      const result = await this.options.judge({ condition, stdout: body, collectedAt: event.observedAt }, signal);
      if (result.kind === "failure") error = result.code;
      else if (!Number.isFinite(result.probability) || result.probability < 0 || result.probability > 1) error = "invalid-judgment";
      else decision = { choice: result.choice, probability: result.probability, decidedAt: this.now() };
    } catch (caught) {
      error = caught instanceof Error ? caught.message : "decision-failed";
    }
    return this.options.store.finishSourceDecision(intent, decision, error, this.now());
  }

  private async dispatchQueuedWakes(): Promise<void> {
    const botUid = this.options.botUid();
    if (!botUid || this.closed || !this.options.enabled()) return;
    for (const record of this.options.store.listActiveSourceWatches(botUid)) await this.dispatchWatchWakes(record);
  }

  private async dispatchWatchWakes(record: SourceWatchRecord): Promise<void> {
    if (this.closed || this.stopping || !this.options.enabled()) return;
    for (const event of this.options.store.listSourceWatchEvents(record.id, Number.POSITIVE_INFINITY)) {
      const consumable = event.consumeState === "satisfied" || event.consumeState === "deterministic";
      const shouldDispatch = event.state === "ready" || (event.state === "pending" && consumable);
      if (event.wakeStatus !== "queued" || !shouldDispatch || this.dispatching.has(event.id)) continue;
      try {
        await this.dispatchWake(record, event);
      } finally {
        this.dispatching.delete(event.id);
      }
    }
  }

  private async dispatchWake(record: SourceWatchRecord, event: SourceWatchEventRecord): Promise<void> {
    const messageId = sourceWakeMessageId(record.id, event.id);
    if (!this.options.store.sourceWakeAllowed(event.id, this.now())) return;
    if (!this.options.permitted(record)) {
      this.options.store.markSourceWakeFailed(event.id, "chat-permission-revoked", this.now());
      return;
    }
    const text = formatSourceWake(record, event);
    try {
      const taskId = await this.options.deliver(record, event, messageId, text);
      if (!taskId) {
        if (!this.closed && !this.stopping) this.options.store.markSourceWakeFailed(event.id, "wake-task-not-accepted", this.now());
        return;
      }
      this.options.store.markSourceWakeTask(event.id, taskId, this.now());
    } catch {
      if (!this.closed && !this.stopping) this.options.store.markSourceWakeFailed(event.id, "wake-delivery-failed", this.now());
    }
  }

  private source(record: SourceWatchRecord): WatchSource {
    const resource = parseWatchResource(record.spec.resource);
    const factory = this.options.sourceFactory;
    if (factory) return factory(resource);
    const hooks: WatchSourceHooks = {
      beforeRequest: () => {
        const botUid = this.options.botUid();
        const current = botUid ? this.options.store.getSourceWatch(botUid, record.id) : null;
        if (!current || !["initializing", "watching"].includes(current.state) ||
            current.generation !== record.generation || current.deadlineAt <= this.now()) {
          throw new SourceBudgetError();
        }
        if (!this.options.permitted(current)) {
          this.revoke(current);
          throw new Error("source watch permission was revoked");
        }
        if (!this.options.store.reserveSourceRequest(record.id, record.generation, this.now())) {
          if (current.state === "watching") {
            this.options.store.markSourceBudgetExhausted(record.id, record.generation, this.now());
          }
          throw new SourceBudgetError();
        }
      },
      afterResponse: response => this.noteRateLimit(record, response),
    };
    return createWatchSource({ resource, ...hooks });
  }

  private readonly pollAt = new Map<string, number>();

  private noteRateLimit(record: SourceWatchRecord, response: Response): void {
    const provider = parseWatchResource(record.spec.resource).provider;
    const minimum = provider === "github" ? GITHUB_MIN_POLL_MS : GITLAB_MIN_POLL_MS;
    const now = this.now();
    let nextAt = now + minimum;
    const retryAfter = response.headers.get("retry-after");
    if (retryAfter) {
      const seconds = Number(retryAfter);
      const date = Number.isFinite(seconds) ? now + Math.max(0, seconds) * 1_000 : Date.parse(retryAfter);
      if (Number.isFinite(date)) nextAt = Math.max(nextAt, date);
    }
    const reset = response.headers.get("x-ratelimit-reset");
    if (reset && Number.isFinite(Number(reset))) nextAt = Math.max(nextAt, Number(reset) * 1_000);
    this.pollAt.set(record.id, nextAt);
  }

  private nextPollAt(record: SourceWatchRecord, _source: WatchSource): number {
    const provider = parseWatchResource(record.spec.resource).provider;
    const minimum = provider === "github" ? GITHUB_MIN_POLL_MS : GITLAB_MIN_POLL_MS;
    return Math.max(this.pollAt.get(record.id) ?? 0, this.now() + (this.options.sourceFactory ? 0 : minimum));
  }

  private async observe(
    record: SourceWatchRecord,
    source: WatchSource,
    method: "initialize" | "poll",
    cursor: SourceWatchRecord["cursor"],
    signal: AbortSignal,
  ): Promise<WatchSourceObservation> {
    throwIfAborted(signal);
    if (this.options.sourceFactory) {
      if (!this.options.store.reserveSourceRequest(record.id, record.generation, this.now())) throw new SourceBudgetError();
    }
    return method === "initialize"
      ? source.initialize(signal)
      : source.poll(cursor!, signal);
  }

  private recordFailure(record: SourceWatchRecord, reason: string): void {
    this.options.store.failSourceWatch(record.id, record.generation, reason, this.now());
  }

  private revoke(record: SourceWatchRecord): void {
    const botUid = this.options.botUid();
    if (!botUid) return;
    const stopped = this.options.store.stopSourceWatches(botUid, record, record.id, this.now(), "permission-revoked");
    if (stopped.length > 0) this.jobs.get(record.id)?.controller.abort();
  }

  private deadlineController(record: SourceWatchRecord, controller: AbortController): ReturnType<typeof setTimeout> {
    const timer = setTimeout(() => {
      this.options.store.expireSourceWatches(this.now());
      controller.abort();
    }, Math.max(0, record.deadlineAt - this.now()));
    timer.unref?.();
    return timer;
  }

  private async sleepUntil(nextAt: number, signal: AbortSignal): Promise<void> {
    const delay = Math.max(0, nextAt - this.now());
    if (delay === 0 || signal.aborted) return;
    await new Promise<void>(resolve => {
      const timer = setTimeout(done, delay);
      timer.unref?.();
      function done() {
        clearTimeout(timer);
        signal.removeEventListener("abort", done);
        resolve();
      }
      signal.addEventListener("abort", done, { once: true });
      if (signal.aborted) done();
    });
  }

}

class SourceBudgetError extends Error {}

function buildSpec(
  args: SourceWatchCreateArgs,
  decisionProfile: import("./source-types.ts").SourceWatchDecisionProfile | null,
): SourceWatchSpec {
  if (typeof args.resource !== "string" || !args.resource.trim() || Buffer.byteLength(args.resource, "utf8") > MAX_RESOURCE_BYTES) {
    throw new Error("resource must be a non-empty supported source resource");
  }
  const resource = parseWatchResource(args.resource);
  const supportedTypes = resource.provider === "github"
    ? { snapshot: true, comment: true, review: true }
    : { snapshot: true, note: true };
  if (!Array.isArray(args.eventTypes) || args.eventTypes.length === 0 || args.eventTypes.length > MAX_EVENT_TYPES ||
      args.eventTypes.some(type => typeof type !== "string" || !type.trim() || !Object.hasOwn(supportedTypes, type.trim()))) {
    throw new Error(`eventTypes must contain 1 through ${MAX_EVENT_TYPES} supported ${resource.provider} event types`);
  }
  if (args.consumeCondition !== undefined && (typeof args.consumeCondition !== "string" || !args.consumeCondition.trim() || Buffer.byteLength(args.consumeCondition, "utf8") > MAX_CONDITION_BYTES)) {
    throw new Error("consumeCondition must be non-empty and at most 4096 UTF-8 bytes");
  }
  if (args.endCondition !== undefined && (typeof args.endCondition !== "string" || !args.endCondition.trim() || Buffer.byteLength(args.endCondition, "utf8") > MAX_CONDITION_BYTES)) {
    throw new Error("endCondition must be non-empty and at most 4096 UTF-8 bytes");
  }
  if (args.endWhen !== undefined && args.endCondition !== undefined) {
    throw new Error("endWhen cannot be combined with endCondition");
  }
  const endWhen = parseEndWhen(args.endWhen);
  const semantic = args.consumeCondition !== undefined || args.endCondition !== undefined;
  if (typeof args.taskScope !== "string" || !args.taskScope.trim() || Buffer.byteLength(args.taskScope, "utf8") > MAX_SCOPE_BYTES) {
    throw new Error("taskScope must be non-empty and at most 4096 UTF-8 bytes");
  }
  if (typeof args.decisionConsent !== "boolean") throw new Error("decisionConsent must be explicitly true or false");
  if (args.decisionConsent === false && semantic) {
    throw new Error("semantic Decisions require explicit per-subscription source-data consent");
  }
  if (semantic && decisionProfile === null) {
    throw new Error("semantic source watch requires a configured Decisions service");
  }
  const timeoutSeconds = args.timeout ?? DEFAULT_TIMEOUT_MS / 1_000;
  if (!Number.isSafeInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > MAX_TIMEOUT_MS / 1_000) {
    throw new Error("timeout must be an integer from 1 through 86400 seconds");
  }
  const sourceBudget = args.sourceBudget ?? MAX_SOURCE_BUDGET;
  const decisionBudget = args.decisionBudget ?? MAX_DECISION_BUDGET;
  const wakeBudget = args.wakeBudget ?? MAX_WAKE_BUDGET;
  if (!Number.isSafeInteger(sourceBudget) || sourceBudget < 1 || sourceBudget > MAX_SOURCE_BUDGET) {
    throw new Error("sourceBudget must be an integer from 1 through 300");
  }
  if (!Number.isSafeInteger(decisionBudget) || decisionBudget < 0 || decisionBudget > MAX_DECISION_BUDGET) {
    throw new Error("decisionBudget must be an integer from 0 through 300");
  }
  if (!Number.isSafeInteger(wakeBudget) || wakeBudget < 0 || wakeBudget > MAX_WAKE_BUDGET) {
    throw new Error("wakeBudget must be an integer from 0 through 20");
  }
  return {
    resource: args.resource.trim(),
    eventTypes: [...new Set(args.eventTypes.map(type => type.trim()))],
    consumeCondition: args.consumeCondition?.trim() ?? null,
    endCondition: args.endCondition?.trim() ?? null,
    endWhen,
    taskScope: args.taskScope.trim(),
    decisionProfile: semantic ? decisionProfile : null,
    decisionConsent: args.decisionConsent,
    timeoutMs: timeoutSeconds * 1_000,
    sourceBudget,
    decisionBudget,
    wakeBudget,
  };
}

function parseEndWhen(value: unknown): SourceWatchEndWhen | null {
  if (value === undefined) return null;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("endWhen must be an object with field and equals");
  }
  const object = value as Record<string, unknown>;
  if (Object.keys(object).length !== 2 || !Object.hasOwn(object, "field") || !Object.hasOwn(object, "equals")) {
    throw new Error("endWhen must contain only field and equals");
  }
  if (object.field === "state" && typeof object.equals === "string") {
    return { field: "state", equals: object.equals };
  }
  if (object.field === "merged" && typeof object.equals === "boolean") {
    return { field: "merged", equals: object.equals };
  }
  if (object.field === "title" && typeof object.equals === "string") {
    return { field: "title", equals: object.equals };
  }
  if (object.field === "headSha" && typeof object.equals === "string") {
    return { field: "headSha", equals: object.equals };
  }
  throw new Error("endWhen field and equals must use a supported matching type");
}

function endWhenMatches(rule: SourceWatchEndWhen, event: SourceWatchEventRecord, baseline: boolean): boolean {
  if (!baseline && event.type !== "snapshot") return false;
  const value = baseline ? event.body : event.body.current;
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  return (value as Record<string, unknown>)[rule.field] === rule.equals;
}

function isBaselineSnapshot(event: SourceWatchEventRecord): boolean {
  return event.identity === BASELINE_IDENTITY && event.version === "baseline";
}


function retryable(state: SourceWatchEventRecord["consumeState"]): boolean {
  return state === "pending" || state === "failure" || state === "insufficient_evidence";
}

function decisionBudgetExhausted(record: SourceWatchRecord): boolean {
  const usesDecisions = record.spec.consumeCondition !== null || record.spec.endCondition !== null;
  return usesDecisions && record.decisionUsed >= record.spec.decisionBudget;
}

function sameTarget(left: ChatTarget | TaskRecord | SourceWatchRecord, right: ChatTarget | TaskRecord | SourceWatchRecord): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}

function sourceWakeMessageId(watchId: string, eventId: string): string {
  return `watch:event:${watchId}:${eventId}`;
}


function formatSourceWake(record: SourceWatchRecord, event: SourceWatchEventRecord): string {
  const content = JSON.stringify({ type: event.type, version: event.version, body: event.body });
  return [
    "A source watch observed an authorized event. This is an independent queued follow-up, not a new human instruction or business authorization.",
    `Watch ID: ${record.id}`,
    `Original task scope: ${record.spec.taskScope}`,
    `Consumption condition: ${record.spec.consumeCondition ?? "consume selected event types deterministically"}`,
    `Source: ${record.spec.resource}`,
    `Observed at ${new Date(event.observedAt).toISOString()}:`,
    content,
  ].join("\n");
}
