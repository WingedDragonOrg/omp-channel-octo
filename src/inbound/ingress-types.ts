import type { BindingSessionInitialization, BindingRecord } from "../store/store-records.ts";
import type { BotCardAction, ChatTarget, InboundMessage } from "../contracts.ts";

export type IngressSourceKind = "recv" | "bot-event" | "synthetic";

export interface IngressSourceKey {
  botUid: string;
  sourceKind: IngressSourceKind;
  sourceId: string;
}
export interface IngressCancellationCandidate extends IngressSourceKey {
  messageId: string;
}

/** Stable, collision-free WebSocket receive identity, including the real chat target. */
export function recvIngressSourceId(
  channelType: 1 | 2 | 5,
  channelId: string,
  messageId: string,
): string {
  return JSON.stringify([channelType, channelId, messageId]);
}

export interface IngressBindingSnapshot {
  sessionId: string;
  cwd: string;
  sessionFile: string | null;
  /** Optional only for legacy persisted snapshots; absent means initialization recovery is unsafe. */
  cwdPinned?: boolean;
  /** Optional only for legacy persisted snapshots; absent means initialization recovery is unsafe. */
  sessionSwitchedAt?: number | null;
  sessionInitialization?: BindingSessionInitialization | null;
}

export type IngressRoutePurpose =
  | { kind: "command"; action: string }
  | { kind: "ordinary" }
  | { kind: "questions"; questionIds: readonly string[] }
  | { kind: "none" };

export interface IngressRouteSnapshot {
  purpose: IngressRoutePurpose;
  sessionGeneration: number;
  binding: IngressBindingSnapshot | null;
  /** Exact id of the earlier session-switch ingress job that gates this route, if any. */
  barrierJobId: number | null;
}

export interface IngressValidatedConfigAction {
  kind: "switch" | "mode";
  actionId: string;
  messageId: string;
  target: ChatTarget;
  ownerUid: string;
  value: string;
  cardSeq: number;
}

/** Already-authorized, serializable source snapshot retained until durable consumer handoff. */
export type IngressPayload =
  | { kind: "recv"; message: InboundMessage }
  | {
      kind: "bot-event";
      eventId: number;
      action: BotCardAction;
      acceptedAction: IngressValidatedConfigAction | null;
    }
  | { kind: "synthetic"; message: InboundMessage };

export type IngressOperationCheckpoint =
  | {
      kind: "command";
      action: string;
      argument: string;
      phase:
        | "admitted"
        | "cancel-intent"
        | "commit-stop-intent"
        | "interrupt-intent"
        | "barrier-installed"
        | "planning"
        | "waiting-session"
        | "waiting-resource"
        | "dispatch-intent";
      expectedBinding: IngressBindingSnapshot | null;
      /** Earlier ordered ingress jobs cancelled before this command was ACKed. */
      cancelledIngressJobIds?: readonly number[];
      /** Earlier dispatch-intent jobs explicitly requested to cancel before this command was ACKed. */
      cancelRequestedIngressJobIds?: readonly number[];
      /** Task identities whose cancellation was committed before this command was ACKed. */
      cancelledQueuedTaskIds?: readonly string[];
      stoppedTaskIds?: readonly string[];
      /** Workspace-add identities whose cancellation was committed before this command was ACKed. */
      cancelRequestedWorkspaceOperationIds?: readonly string[];
      /** Exact open question identities closed by this cancellation transaction. */
      cancelledQuestionIds?: readonly string[];
      /** Earlier commit waiters/tasks stopped before this command was ACKed. */
      cancelledCommitIngressJobIds?: readonly number[];
      cancelledCommitTaskIds?: readonly string[];
      stoppedCommitTaskIds?: readonly string[];
      /** Exact paths established before a session or host operation waits on resources. */
      resourceCwds?: readonly string[];
      /** Stable existing worker-ledger identity linked before a session RPC can start. */
      workerLedgerId?: string;
      /** Stable existing host-operation ledger identity linked before native process creation. */
      hostLedgerId?: string;
      /** Cancellation requests that target this dispatch-intent without replaying its action. */
      cancelRequestedByIngressJobIds?: readonly number[];
      /** Running task turns durably stopped by this interrupt before its ACK. */
      interruptedTaskIds?: readonly string[];
      /** Accepted host operations safely cancelled before dispatch intent. */
      cancelledHostIngressJobIds?: readonly number[];
      /** Host operations already in dispatch-intent; their result remains unknown/fail-closed. */
      interruptRequestedHostIngressJobIds?: readonly number[];
      /** Earlier unknown session transitions this explicit recovery is allowed to supersede. */
      recoveryBarrierJobIds?: readonly number[];
      /** Set only by the successful binding/handoff transaction; state remains `unknown`. */
      recoveredByIngressJobId?: number;
      /** Exact binding committed by the successful recovery linked above. */
      recoveredBindingSnapshot?: IngressBindingSnapshot;
    }
  | {
      kind: "session-handoff-outcome";
      action: "new" | "move";
      binding: IngressBindingSnapshot;
    }
  | {
      kind: "cancellation-request";
      cancelRequestedByIngressJobIds: readonly number[];
    }
  | {
      kind: "session-recovery-cancellation";
      barrierJobId: number;
      recoveryJobId: number;
    }
  | { kind: "config-action"; phase: "accepted" | "dispatch-intent" }
  | { kind: "config-action"; phase: "applied"; appliedResult: string };


export type IngressLane = "ordered" | "control";
export type IngressState = "accepted" | "preparing" | "dispatch-intent" | "handed-off" | "unknown" | "cancelled";
export type IngressConsumerKind = "task" | "steering" | "ask" | "operation" | "outbox";

export interface IngressConsumer {
  kind: IngressConsumerKind;
  id: string;
}

export interface IngressJobInput extends IngressSourceKey, ChatTarget {
  messageId: string | null;
  payload: IngressPayload;
  routeSnapshot: IngressRouteSnapshot;
  lane: IngressLane;
  state: "accepted" | "preparing";
  ackReady: boolean;
  operationCheckpoint: IngressOperationCheckpoint | null;
  /** Earlier accepted-but-not-yet-persisted candidates atomically tombstoned by a control command. */
  cancellationCandidates?: readonly IngressCancellationCandidate[];
}

export interface IngressSessionRecoveryResult {
  kind: "ready" | "blocked" | "stale";
  cancelledJobIds: readonly number[];
}

export interface IngressSessionHandoff {
  readonly barrierJobIds: readonly number[];
  readonly expectedBinding: IngressBindingSnapshot | null;
  readonly binding: BindingRecord;
}

export interface IngressJobRecord extends IngressSourceKey, ChatTarget {
  id: number;
  scheduleOrder: number;
  messageId: string | null;
  payload: IngressPayload | null;
  routeSnapshot: IngressRouteSnapshot | null;
  preparedText: string | null;
  spoolPaths: string[];
  ackReady: boolean;
  lane: IngressLane;
  state: IngressState;
  operationCheckpoint: IngressOperationCheckpoint | null;
  consumerKind: IngressConsumerKind | null;
  consumerId: string | null;
  createdAt: number;
  updatedAt: number;
}
