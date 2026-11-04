import type { ChatTarget, WorkerProcess } from "../contracts.ts";
import type { TaskRecord } from "../store/store-records.ts";

export interface WatchSpec {
  argv: string[];
  cwd: string;
  condition: string;
  intervalMs: number;
  timeoutMs: number;
}

export type WatchChoice = "satisfied" | "not_satisfied" | "insufficient_evidence";
export type WatchState = "waiting" | "checking" | "matched" | "failed" | "expired" | "stopped" | "deleted" | "unknown";
export type WatchWakeStatus = "none" | "queued" | "accepted" | "dispatched" | "cancelled" | "failed";

export type WatchCollection =
  | { kind: "success"; stdout: string }
  | { kind: "failure"; code: string }
  | { kind: "unknown"; code: string; process?: WorkerProcess };

export type WatchJudgeResult =
  | { kind: "judgment"; choice: WatchChoice; probability: number }
  | { kind: "failure"; code: string };

export type WatchJudge = (
  input: { condition: string; stdout: string; collectedAt: number },
  signal: AbortSignal,
) => Promise<WatchJudgeResult>;

export type WatchCheckCompletion =
  | { kind: "judgment"; collectedAt: number; stdout: string; choice: WatchChoice; probability: number }
  | { kind: "failure"; collectedAt: number; code: string };

export interface WatchRecord {
  id: string;
  botUid: string;
  target: ChatTarget;
  sourceTaskId: string;
  creationKey: string;
  spec: WatchSpec;
  createdAt: number;
  deadlineAt: number;
  nextAt: number;
  checks: number;
  maxChecks: number;
  generation: number;
  failures: number;
  state: WatchState;
  latest: WatchCheckCompletion | null;
  matchedEvidence: string | null;
  process: WorkerProcess | null;
  wakeTaskId: string | null;
  wakeStatus: WatchWakeStatus;
  stopReason: string | null;
  matchedAt: number | null;
}

/** Bounded list projection: private command and evidence fields are intentionally omitted. */
export type WatchListItem = Omit<WatchRecord, "spec" | "latest" | "matchedEvidence" | "process">;

export type WatchCreateResult =
  | { kind: "created" | "duplicate"; record: WatchRecord }
  | { kind: "quota-chat" | "quota-bot" | "deleted"; record: null };

export interface WatchStore {
  createWatch(record: WatchRecord): WatchCreateResult;
  claimDueWatches(now: number, limit?: number, botUid?: string): WatchRecord[];
  setWatchProcess(id: string, generation: number, process: WorkerProcess | null): boolean;
  finishWatchCheck(id: string, generation: number, completion: WatchCheckCompletion, finishedAt: number): WatchRecord | null;
  stopWatches(botUid: string, target: ChatTarget, id: string | "all", now: number, reason: string): WatchRecord[];
  wakeAllowed(id: string, now: number): boolean;
  markWatchWakeTask(id: string, taskId: string, now?: number): boolean;
  markWatchWakeDispatched(id: string, taskId: string, now: number): boolean;
  markWatchWakeFailed(id: string, reason: string): boolean;
  deleteWatchChat(botUid: string, target: ChatTarget, now: number): void;
  isWatchChatDeleted(botUid: string, target: ChatTarget): boolean;
  getWatch(botUid: string, id: string): WatchRecord | null;
  getTask(taskId: string): TaskRecord | null;
  listWatches(botUid: string, target: ChatTarget, limit: number, offset: number): WatchListItem[];
  listWatchRecords(botUid?: string): WatchRecord[];
  listActiveWatchRecords(botUid?: string): WatchRecord[];
  listWatchProcessRecords(botUid?: string): WatchRecord[];
  markWatchUnknown(id: string, generation: number, reason: string, now: number): boolean;
  recoverWatchProcess(id: string, generation: number, now: number): WatchRecord | null;
  expireWatchRecords(now: number): void;
}

export interface WatchCreateArgs {
  script?: string;
  command?: string;
  args?: string[];
  cwd?: string;
  condition: string;
  interval?: number;
  timeout?: number;
}

export type WatchCollector = (
  spec: WatchSpec,
  signal: AbortSignal,
  onSpawn: (process: WorkerProcess) => void,
  onExit: () => void,
  env?: Record<string, string>,
) => Promise<WatchCollection>;

export interface WatchCoordinatorOptions {
  store: WatchStore;
  botUid: () => string | undefined;
  enabled: () => boolean;
  collect: WatchCollector;
  judge: WatchJudge | undefined;
  permitted: (target: ChatTarget) => boolean;
  deliver: (record: WatchRecord, text: string) => Promise<string | null>;
  acquireUse: (record: WatchRecord) => Promise<(() => void) | undefined>;
  now?: () => number;
}
