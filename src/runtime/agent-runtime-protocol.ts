import type {
  PromptContext,
  WorkerGoalResult,
  WorkerModelCatalog,
  WorkerModelSwitch,
  WorkerOptions,
  WorkerProcess,
  WorkerContextUsage,
  OmpAvailableCommand,
  OmpSubagentSnapshot,
  OmpSubagentMessages,
  OmpWorker,
} from "../contracts.ts";
import type { ProjectSessionState } from "../session/project-session.ts";

export const AGENT_RUNTIME_PROTOCOL_VERSION = 1;

export type AgentRuntimeStage =
  | "spawn-intent"
  | "identity-verified"
  | "omp-ready"
  | "active"
  | "stopping"
  | "exited-confirmed"
  | "unknown";

export type AgentRuntimeDispatchState = "active" | "completed" | "failed" | "unknown";
export type AgentRuntimeDispatchKind = "prompt" | "resume" | "goal";

export interface AgentRuntimeDispatchSnapshot {
  readonly dispatchId: string;
  readonly kind: AgentRuntimeDispatchKind;
  readonly state: AgentRuntimeDispatchState;
  readonly nextSeq: number;
  readonly result?: unknown;
  readonly error?: string;
}

export interface AgentRuntimeWorkerSnapshot {
  readonly botUid: string;
  /** Channel task ledger ID; it is deliberately not a WorkerOptions property. */
  readonly taskId: string;
  readonly runId: string;
  readonly runtimeId: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly process: WorkerProcess | null;
  readonly stage: AgentRuntimeStage;
  readonly activeDispatchId: string | null;
  readonly dispatches: readonly AgentRuntimeDispatchSnapshot[];
}

export type AgentRuntimeWorkerOptions = Omit<WorkerOptions, "onSpawn" | "signal">;
export interface AgentRuntimeCreateWorkerInput {
  readonly taskId: string;
  readonly runId: string;
  readonly workerOptions: AgentRuntimeWorkerOptions;
}

export interface AgentRuntimeAttachWorkerInput {
  readonly taskId: string;
  readonly runId: string;
  readonly sessionId: string;
}

export interface AgentRuntimeDispatchInput {
  readonly dispatchId: string;
  readonly kind: AgentRuntimeDispatchKind;
  readonly text?: string;
  readonly goal?: Parameters<OmpWorker["runGoal"]>[0];
  readonly context?: PromptContext;
}

export type AgentRuntimePromptContext = Omit<PromptContext, "tools"> & {
  readonly tools?: readonly {
    readonly name: string;
    readonly description: string;
    readonly parameters: Record<string, unknown>;
  }[];
};

export type AgentRuntimeWireRequest = {
  readonly kind: "request";
  readonly id: string;
  readonly epoch: number;
  readonly operation: string;
  readonly args: unknown;
};

export type AgentRuntimeWireResponse =
  | { readonly kind: "response"; readonly id: string; readonly ok: true; readonly value: unknown }
  | { readonly kind: "response"; readonly id: string; readonly ok: false; readonly error: string };

export type AgentRuntimeWireEvent = {
  readonly kind: "event";
  readonly epoch: number;
  readonly event: string;
  readonly taskId: string;
  readonly runId: string;
  readonly dispatchId?: string;
  readonly seq?: number;
  readonly value?: unknown;
};

export interface AgentRuntimeStartDispatch {
  readonly dispatchId: string;
  readonly kind: AgentRuntimeDispatchKind;
  readonly text?: string;
  readonly goal?: { readonly op: "create" | "resume" | "recover"; readonly objective?: string; readonly tokenBudget?: number; readonly goalId?: string };
  readonly context?: AgentRuntimePromptContext;
}

export type AgentRuntimeOperationResult =
  | { readonly kind: "void" }
  | { readonly kind: "goal"; readonly value: WorkerGoalResult }
  | { readonly kind: "session-state"; readonly value: ProjectSessionState }
  | { readonly kind: "context-usage"; readonly value: WorkerContextUsage }
  | { readonly kind: "models"; readonly value: WorkerModelCatalog }
  | { readonly kind: "model-switch"; readonly value: WorkerModelSwitch }
  | { readonly kind: "commands"; readonly value: readonly OmpAvailableCommand[] }
  | { readonly kind: "subagents"; readonly value: readonly OmpSubagentSnapshot[] }
  | { readonly kind: "subagent-messages"; readonly value: OmpSubagentMessages };
