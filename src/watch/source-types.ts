import type { ChatTarget } from "../contracts.ts";
import type { TaskRecord } from "../store/store-records.ts";
import type {
  WatchSource,
  WatchSourceEvent,
  WatchSourceObservation,
  WatchSourceResource,
} from "./sources.ts";
export type { WatchSource, WatchSourceEvent, WatchSourceObservation, WatchSourceResource };
export type WatchSourceCursor = Extract<WatchSourceObservation, { kind: "success" }>["cursor"];
export type WatchSourceSnapshot = Extract<WatchSourceObservation, { kind: "success" }>["snapshot"];

export type WatchSourceFactory = (resource: WatchSourceResource) => WatchSource;

export interface SourceWatchDecisionProfile {
  protocol: string;
  endpoint: string;
  model: string;
}

export type SourceWatchEndWhen =
  | { field: "state"; equals: string }
  | { field: "merged"; equals: boolean }
  | { field: "title"; equals: string }
  | { field: "headSha"; equals: string };

export interface SourceWatchSpec {
  resource: string;
  eventTypes: string[];
  consumeCondition: string | null;
  endCondition: string | null;
  endWhen: SourceWatchEndWhen | null;
  taskScope: string;
  decisionProfile: SourceWatchDecisionProfile | null;
  decisionConsent: boolean;
  timeoutMs: number;
  sourceBudget: number;
  decisionBudget: number;
  wakeBudget: number;
}

export type SourceWatchState =
  | "initializing"
  | "watching"
  | "budget-exhausted"
  | "ended"
  | "failed"
  | "expired"
  | "stopped"
  | "deleted";

export type SourceWatchJudgmentState =
  | "pending"
  | "satisfied"
  | "not_satisfied"
  | "insufficient_evidence"
  | "failure"
  | "deterministic";

export type SourceWatchEventState = "pending" | "judging" | "ready" | "failed" | "cancelled";
export type SourceWatchWakeStatus = "none" | "queued" | "accepted" | "dispatched" | "cancelled" | "failed";

export interface SourceWatchDecision {
  choice: "satisfied" | "not_satisfied" | "insufficient_evidence";
  probability: number;
  decidedAt: number;
}

export interface SourceWatchEventRecord {
  id: string;
  watchId: string;
  identity: string;
  type: string;
  version: string;
  body: Record<string, unknown>;
  observedAt: number;
  snapshotSequence: number;
  state: SourceWatchEventState;
  consumeState: SourceWatchJudgmentState;
  consumeDecision: SourceWatchDecision | null;
  endState: SourceWatchJudgmentState;
  endDecision: SourceWatchDecision | null;
  wakeTaskId: string | null;
  wakeStatus: SourceWatchWakeStatus;
  nextAttemptAt: number;
  error: string | null;
}

export interface SourceWatchRecord extends ChatTarget {
  id: string;
  botUid: string;
  sourceTaskId: string;
  creationKey: string;
  spec: SourceWatchSpec;
  createdAt: number;
  deadlineAt: number;
  state: SourceWatchState;
  generation: number;
  cursor: WatchSourceCursor | null;
  snapshot: WatchSourceSnapshot | null;
  snapshotSequence: number;
  sourceUsed: number;
  decisionUsed: number;
  wakeUsed: number;
  failures: number;
  gap: string | null;
  latestError: string | null;
  stopReason: string | null;
  observedAt: number | null;
}

export type SourceWatchListItem = Omit<
  SourceWatchRecord,
  "cursor" | "snapshot" | "spec" | "sourceTaskId" | "creationKey"
> & {
  resource: string;
  eventTypes: string[];
  consumeCondition: string | null;
  endCondition: string | null;
  endWhen: SourceWatchEndWhen | null;
  taskScope: string;
  decisionProfile: SourceWatchDecisionProfile | null;
  decisionConsent: boolean;
  timeoutMs: number;
  sourceBudget: number;
  decisionBudget: number;
  wakeBudget: number;
  pendingEvents: number;
  unresolvedEvents: number;
};

export interface SourceWatchCreateArgs {
  resource: string;
  eventTypes: string[];
  consumeCondition?: string;
  endCondition?: string;
  endWhen?: SourceWatchEndWhen;
  taskScope: string;
  decisionConsent: boolean;
  timeout?: number;
  sourceBudget?: number;
  decisionBudget?: number;
  wakeBudget?: number;
}

export type SourceWatchCreateResult =
  | { kind: "created" | "duplicate"; record: SourceWatchRecord }
  | { kind: "quota-chat" | "quota-bot" | "deleted"; record: null };

export interface SourceWatchDecisionIntent {
  id: string;
  event: SourceWatchEventRecord;
  condition: string;
  generation: number;
  kind: "consume" | "end";
}

export interface SourceWatchStore {
  createSourceWatch(record: SourceWatchRecord): SourceWatchCreateResult;
  getSourceWatch(botUid: string, id: string): SourceWatchRecord | null;
  listSourceWatches(botUid?: string): SourceWatchRecord[];
  listActiveSourceWatches(botUid?: string): SourceWatchRecord[];
  listSourceWatchesForChat(botUid: string, target: ChatTarget, limit: number, offset: number): SourceWatchListItem[];
  stopSourceWatches(botUid: string, target: ChatTarget, id: string | "all", now: number, reason: string): SourceWatchRecord[];
  deleteSourceWatchChat(botUid: string, target: ChatTarget, now: number): SourceWatchRecord[];
  expireSourceWatches(now: number): void;
  initializeSourceWatch(id: string, generation: number, cursor: WatchSourceCursor, snapshot: WatchSourceSnapshot, events: readonly WatchSourceEvent[], observedAt: number): boolean;
  commitSourceObservation(id: string, generation: number, cursor: WatchSourceCursor, snapshot: WatchSourceSnapshot, events: readonly WatchSourceEvent[], observedAt: number): boolean;
  failSourceWatch(id: string, generation: number, reason: string, now: number, gap?: string): boolean;
  reserveSourceRequest(id: string, generation: number, now: number): boolean;
  beginSourceDecision(id: string, eventId: string, kind: "consume" | "end", generation: number, now: number): SourceWatchDecisionIntent | null;
  finishSourceDecision(intent: SourceWatchDecisionIntent, decision: SourceWatchDecision | null, error: string | null, now: number): SourceWatchEventRecord | null;
  completeSourceEvent(id: string, generation: number, eventId: string, consumeState: SourceWatchJudgmentState, endState: SourceWatchJudgmentState, endSatisfied: boolean, now: number): boolean;
  listSourceWatchEvents(id: string, dueAt: number): SourceWatchEventRecord[];
  markSourceEventFailure(id: string, eventId: string, error: string, nextAttemptAt: number): boolean;
  markSourceBudgetExhausted(id: string, generation: number, now: number): boolean;
  markSourceWakeTask(eventId: string, taskId: string, now: number): boolean;
  sourceWakeAllowed(eventId: string, now: number): boolean;
  markSourceWakeDispatched(eventId: string, taskId: string, sessionId: string, now: number): boolean;
  markSourceWakeFailed(eventId: string, reason: string, now: number): boolean;
  getSourceWakeByMessageId(botUid: string, target: ChatTarget, messageId: string): SourceWatchEventRecord | null;
  recoverSourceWatch(id: string, now: number): SourceWatchRecord | null;
  getTask(taskId: string): TaskRecord | null;
}
