import type { ChatTarget, WorkerFactory, WorkerProcess } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { TaskRecord } from "../store/store-records.ts";

export type ScheduleStatus = "active" | "paused" | "stopped";
export type ScheduleRunStatus =
  | "claimed"
  | "dispatching"
  | "running"
  | "skipped"
  | "cancelled"
  | "failed"
  | "success"
  | "unknown";
export type ScheduleRunStage = "claimed" | "dispatch-intent" | "accepted" | "process-exited" | "settled";
export type ScheduleSourceKind = "direct" | "synthetic";

export type ScheduleSpec =
  | { kind: "agent"; prompt: string }
  | { kind: "script"; runner: { kind: "shell"; script: string } | { kind: "exec"; command: string; args: string[] } };

export interface ScheduleRecord {
  id: string;
  botUid: string;
  name: string;
  cron: string;
  timezone: string;
  cwd: string;
  spec: ScheduleSpec;
  source: ChatTarget;
  sourceMessageId: string;
  sourceRequesterUid: string | null;
  sourceKind: ScheduleSourceKind;
  sourceTaskId: string;
  toolCallId: string;
  target: ChatTarget;
  timeoutMs: number;
  createdAt: number;
  status: ScheduleStatus;
}

export interface ScheduleArtifact {
  name: string;
  type: string;
  path?: string;
  remoteRef?: string;
  target: ChatTarget;
  receipt?: string;
}

export interface ScheduleRunRecord {
  id: string;
  scheduleId: string;
  botUid: string;
  scheduledAt: number;
  target: ChatTarget;
  createdAt: number;
  startedAt: number | null;
  endedAt: number | null;
  status: ScheduleRunStatus;
  stage: ScheduleRunStage;
  stopRequested: boolean;
  promptAccepted: boolean;
  process: WorkerProcess | null;
  processExited: boolean;
  sessionId: string | null;
  sessionFile: string | null;
  resultText: string | null;
  errorText: string | null;
  stdout: string | null;
  stderr: string | null;
  skippedReason: string | null;
  artifacts: ScheduleArtifact[];
}

export type ScheduleCreateResult =
  | { kind: "created" | "duplicate"; record: ScheduleRecord };

export type ScheduleOccurrenceResult =
  | { kind: "claimed" | "skipped" | "duplicate"; run: ScheduleRunRecord }
  | { kind: "inactive"; run: null };

export interface ScheduleRunSettlement {
  status: "cancelled" | "failed" | "skipped" | "success" | "unknown";
  resultText?: string | null;
  errorText?: string | null;
  stdout?: string | null;
  stderr?: string | null;
}
export type ScheduleAccessRole = "owner" | "source" | "denied";

export interface ScheduleCreateRequest {
  name: string;
  cron: string;
  timezone: string;
  cwd: string;
  spec: ScheduleSpec;
  target: ChatTarget;
  timeoutMs: number;
}

export interface ScheduleScriptRequest {
  schedule: ScheduleRecord;
  run: ScheduleRunRecord;
  signal: AbortSignal;
  manifestPath: string;
  cliEnv: Record<string, string> | undefined;
  beforeSpawn?(): void;
  onSpawn(process: WorkerProcess): void;
  onExit(): void;
}

export type ScheduleScriptResult =
  | { kind: "completed"; exitCode: number; stdout: string; stderr: string }
  | { kind: "not-started"; reason: "cancelled" | "spawn-failed" }
  | { kind: "cancelled"; processExited: boolean; stdout: string; stderr: string }
  | { kind: "timed-out"; processExited: boolean; stdout: string; stderr: string }
  | { kind: "output-exceeded"; processExited: boolean; stdout: string; stderr: string }
  | { kind: "unknown"; reason: "process-identity" | "process-exit"; stdout: string; stderr: string };

export type ScheduleScriptRunner = (request: ScheduleScriptRequest) => Promise<ScheduleScriptResult>;

export interface ScheduleClock {
  now(): number;
  setTimeout(callback: () => void, delayMs: number): NodeJS.Timeout;
  clearTimeout(timer: NodeJS.Timeout): void;
}

export interface ScheduleCoordinatorOptions {
  store: ChannelStore;
  workerFactory: WorkerFactory;
  botUid: () => string | undefined;
  stateDir: string;
  sessionRoot?: string;
  clock?: ScheduleClock;
  runScript?: ScheduleScriptRunner;
  createRecord: (
    task: TaskRecord,
    request: ScheduleCreateRequest,
    toolCallId: string,
    signal: AbortSignal,
  ) => Promise<ScheduleRecord>;
  classifyAccess: (
    task: TaskRecord,
    record: ScheduleRecord,
    signal: AbortSignal,
  ) => Promise<ScheduleAccessRole>;
  validateExecution: (record: ScheduleRecord, signal: AbortSignal) => Promise<boolean>;
  cliEnv?: () => Record<string, string> | undefined;
  cliReady?: () => boolean;
  blockDirectory?: (cwd: string) => void;
}

export type ScheduleStopResult =
  | { kind: "stopped" | "unknown"; record: ScheduleRecord; runs: ScheduleRunRecord[] }
  | { kind: "not-found" };

export const SYSTEM_SCHEDULE_CLOCK: ScheduleClock = {
  now: () => Date.now(),
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (timer) => clearTimeout(timer),
};
