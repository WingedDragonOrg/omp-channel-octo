import type { BotCardAction, InboundMessage, InboundReceipt, ChatTarget } from "../contracts.ts";
import { InboundShutdownError } from "../errors.ts";
import {
  recvIngressSourceId,
  type IngressBindingSnapshot,
  type IngressCancellationCandidate,
  type IngressConsumer,
  type IngressJobInput,
  type IngressValidatedConfigAction,
  type IngressJobRecord,
  type IngressLane,
  type IngressOperationCheckpoint,
  type IngressRoutePurpose,
  type IngressRouteSnapshot,
  type IngressSourceKey,
} from "./ingress-types.ts";
import { targetKey } from "../target.ts";
export class IngressSuspendedError extends Error {
  constructor() {
    super("ingress operation suspended before dispatch intent");
    this.name = "IngressSuspendedError";
  }
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

interface IngressStorePort {
  acceptIngressJob(input: IngressJobInput): { job: IngressJobRecord; created: boolean };
  getIngressJob(id: number): IngressJobRecord | null;
  getIngressJobBySource(key: IngressSourceKey): IngressJobRecord | null;
  listRecoverableIngressJobs(botUid: string): IngressJobRecord[];
  finishIngressRecoveryPreparation(
    id: number,
    consumer: IngressConsumer,
    persistConsumer?: () => boolean,
  ): boolean;
  markIngressPrepared(id: number, preparedText: string, spoolPaths: readonly string[]): boolean;
  markIngressDispatchIntent(id: number): boolean;
  markIngressConfigActionDispatchIntent(id: number): boolean;
  markIngressConfigActionApplied(id: number, appliedResult: string): boolean;
  markIngressUnknown(id: number, persistUnknownOutcome?: () => boolean): boolean;
  cancelIngressJobAfterCancellationRequest(id: number): boolean;
  cancelIngressJobBySource(
    key: IngressSourceKey,
    attribution: ChatTarget & { messageId: string },
    cancellationJobId?: number,
  ): boolean;
  cancelIngressJobAfterSessionRecovery(
    key: IngressSourceKey,
    target: ChatTarget,
    messageId: string,
    barrierJobId: number,
  ): IngressJobRecord | null;
  handoffIngressJob(
    id: number,
    consumer: IngressConsumer,
    persistConsumer: () => boolean,
  ): boolean;
}
export interface IngressClassification {
  readonly lane: IngressLane;
  readonly action: string | null;
  readonly argument: string;
  readonly preparesBody: boolean;
  readonly sessionBarrier: boolean;
  readonly cancelsEarlier: boolean;
}

export type IngressAdmission =
  | { readonly kind: "consumed" }
  | {
      readonly kind: "accepted";
      readonly purpose: IngressRoutePurpose;
      readonly preparesBody: boolean;
      readonly checkpoint: IngressOperationCheckpoint | null;
    };

export type IngressPreparation =
  | { readonly kind: "prepared"; readonly text: string; readonly spoolPaths: readonly string[] }
  | {
      readonly kind: "consumed";
      readonly consumer: IngressConsumer;
      readonly persistConsumer?: () => boolean;
      readonly publishAfterCommit?: () => void | Promise<void>;
    };
export interface IngressDispatch {
  readonly consumer: IngressConsumer;
  /** Resolves the exact durable consumer after an operation that may queue steering or tasks. */
  readonly resolveConsumer?: () => IngressConsumer;
  /** Synchronous domain persistence run in the same SQLite transaction as ingress handoff. */
  readonly persistConsumer?: () => boolean;
  /** A refused steer may fall back only if no accepted cancellation request blocks task creation. */
  readonly cancelOnPersistFailureWhenRequested?: () => boolean;
  /** Waiter/UI publication invoked only after the joint transaction commits. */
  readonly publishAfterCommit?: () => void | Promise<void>;
  /** Keep the ordered business lane until a task/steering consumer is durably handed off. */
  readonly holdBusinessUntilHandoff?: boolean;
  readonly completion?: Promise<void>;

}
export interface IngressCoordinatorDeps {
  readonly store: IngressStorePort;
  botUid(): string | undefined;
  classify(message: InboundMessage): IngressClassification;
  authorize(message: InboundMessage, signal: AbortSignal): Promise<IngressAdmission>;
  bindingSnapshot(target: ChatTarget): IngressBindingSnapshot | null;
  prepare(
    message: InboundMessage,
    purpose: IngressRoutePurpose,
    signal: AbortSignal,
    job: IngressJobRecord,
  ): Promise<IngressPreparation>;
  dispatch(job: IngressJobRecord): Promise<IngressDispatch>;
  awaitDispatchReady(): Promise<void>;
  markUnknown?(job: IngressJobRecord): boolean;
  onAccepted?(job: IngressJobRecord): void;
  onReliableAccepted?(job: IngressJobRecord): void;
  onCancelled?(jobId: number): void;
  onIdle?(): void;
}

interface Candidate {
  readonly key: IngressSourceKey;
  readonly target: ChatTarget;
  readonly message: InboundMessage | null;
  readonly classification: IngressClassification;
  readonly sequence: number;
  controller: AbortController;
  ack: Deferred<void>;
  readonly admissionSettled: Deferred<void>;
  readonly businessSettled: Deferred<void>;
  readonly barrierSettled: Deferred<void>;
  readonly admissionBefore: Promise<void>;
  readonly businessBefore: Promise<void>;
  readonly sessionBarrierBefore: Candidate | null;
  restoredJob: IngressJobRecord | null;
  jobId: number | null;
  ackSettled: boolean;
  cancelled: boolean;
  barrierUnknown: boolean;
  active: boolean;
  retryable: boolean;
  redeliveryPending: boolean;
}

const RESOLVED = Promise.resolve();

function sameTarget(left: ChatTarget, right: ChatTarget): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}

function errorFromAbort(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new InboundShutdownError();
}

function isTerminal(job: IngressJobRecord): boolean {
  return job.state === "handed-off" || job.state === "cancelled" || job.state === "unknown";
}

/**
 * Owns the short receive/admission lifetime. Every asynchronous continuation is registered in
 * `operations`; business work is ordered per real chat but its completion is never part of the
 * transport callback. Control candidates have their own authorization and dispatch path.
 */
export class IngressCoordinator {
  private readonly candidates = new Map<string, Candidate>();
  private readonly admissionTails = new Map<string, Promise<void>>();
  private readonly businessTails = new Map<string, Promise<void>>();
  private readonly sessionBarriers = new Map<string, Candidate>();
  private readonly operations = new Set<Promise<unknown>>();
  private sequence = 0;
  private sealed = false;

  constructor(private readonly deps: IngressCoordinatorDeps) {}

  receive(message: InboundMessage): InboundReceipt {
    const botUid = this.deps.botUid();
    if (this.sealed || botUid === undefined) {
      const ackReady = Promise.reject(new InboundShutdownError());
      ackReady.then(() => {}, () => {});
      return { ackReady };
    }
    const key: IngressSourceKey = {
      botUid,
      sourceKind: "recv",
      sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
    };
    const mapKey = this.sourceMapKey(key);
    const existing = this.candidates.get(mapKey);
    if (existing !== undefined) {
      if (existing.retryable && !this.sealed) {
        if (existing.active) {
          if (!existing.redeliveryPending) {
            const ack = Promise.withResolvers<void>();
            ack.promise.then(() => {}, () => {});
            existing.controller = new AbortController();
            existing.ack = ack;
            existing.ackSettled = false;
            existing.redeliveryPending = true;
          }
        } else {
          const ack = Promise.withResolvers<void>();
          ack.promise.then(() => {}, () => {});
          existing.controller = new AbortController();
          existing.ack = ack;
          existing.ackSettled = false;
          existing.retryable = false;
          this.launch(existing);
        }
      }
      return { ackReady: existing.ack.promise };
    }
    const classification = this.deps.classify(message);
    const candidate = this.createCandidate({
      key,
      target: { channelType: message.channelType, channelId: message.channelId },
      message,
      classification,
      restoredJob: null,
    });
    this.candidates.set(mapKey, candidate);
    this.launch(candidate);
    return { ackReady: candidate.ack.promise };
  }

  /** Run an internal durable consumer in chat order with already accepted ingress work. */
  runInBusinessOrder<T>(
    target: ChatTarget,
    signal: AbortSignal,
    operation: () => Promise<T>,
  ): Promise<T> {
    if (this.sealed) return Promise.reject(new InboundShutdownError());
    const key = targetKey(target);
    const prior = this.businessTails.get(key) ?? RESOLVED;
    const settled = Promise.withResolvers<void>();
    this.businessTails.set(key, settled.promise);
    const ordered = (async (): Promise<T> => {
      try {
        await this.abortable(prior, signal);
        if (this.sealed) throw new InboundShutdownError();
        if (signal.aborted) throw errorFromAbort(signal);
        return await operation();
      } finally {
        await prior;
        settled.resolve();
        if (this.businessTails.get(key) === settled.promise) this.businessTails.delete(key);
      }
    })();
    this.track(ordered);
    return ordered;
  }

  /**
   * The menu has already validated owner, chat, action generation and value. Persist that exact
   * snapshot synchronously so the Bot-event cursor never waits for policy probes or card HTTP.
   */
  acceptBotEvent(
    action: BotCardAction,
    eventId: number,
    acceptedAction: IngressValidatedConfigAction,
  ): "accepted" | "duplicate" | "retry" {
    const botUid = this.deps.botUid();
    if (this.sealed || botUid === undefined) return "retry";
    const key: IngressSourceKey = { botUid, sourceKind: "bot-event", sourceId: String(eventId) };
    try {
      const binding = this.deps.bindingSnapshot(acceptedAction.target);
      const accepted = this.deps.store.acceptIngressJob({
        ...key,
        ...acceptedAction.target,
        messageId: action.messageId,
        payload: { kind: "bot-event", eventId, action, acceptedAction },
        routeSnapshot: {
          purpose: { kind: "command", action: `config:${acceptedAction.kind}` },
          sessionGeneration: 0,
          binding,
          barrierJobId: null,
        },
        lane: "ordered",
        state: "accepted",
        ackReady: true,
        operationCheckpoint: { kind: "config-action", phase: "accepted" },
      });
      this.resumePersisted(accepted.job);
      return accepted.created ? "accepted" : "duplicate";
    } catch {
      return "retry";
    }
  }

  /** Register recoverable jobs and fail-closed session barriers before recovered tasks are scheduled. */
  restore(botUid: string, unknownJobs: readonly IngressJobRecord[] = []): void {
    const jobs = [
      ...this.deps.store.listRecoverableIngressJobs(botUid),
      ...unknownJobs.filter(job => job.state === "unknown"),
    ].sort((left, right) => left.id - right.id);
    for (const job of jobs) {
      if (job.state === "unknown") this.restoreUnknownBarrier(job);
      else this.resumePersisted(job);
    }
  }

  /** Stops new admission immediately; durable jobs remain recoverable and are not turned into cancellation. */
  seal(reason: Error = new InboundShutdownError()): void {
    if (this.sealed) return;
    this.sealed = true;
    for (const candidate of this.candidates.values()) {
      candidate.controller.abort(reason);
      if (!candidate.ackSettled && candidate.restoredJob?.ackReady !== true) this.rejectAck(candidate, reason);
      if (candidate.retryable && !candidate.active) {
        candidate.retryable = false;
        this.releaseCandidateFences(candidate);
        this.finishCandidate(candidate);
      }
    }
  }

  /** Called only after worker/session/process leases have been released, avoiding shutdown lock inversion. */
  async settle(): Promise<void> {
    while (this.operations.size > 0) {
      await Promise.allSettled([...this.operations]);
    }
  }

  pendingCount(): number {
    return this.candidates.size;
  }

  releaseSessionBarrier(jobId: number): boolean {
    for (const candidate of this.candidates.values()) {
      if (candidate.jobId !== jobId || !candidate.classification.sessionBarrier) continue;
      candidate.barrierSettled.resolve();
      return true;
    }
    return false;
  }
  /** Promote a validated recovery only after its dispatch intent is durable. */
  promoteRecoverySessionBarrier(jobId: number): boolean {
    const job = this.deps.store.getIngressJob(jobId);
    const checkpoint = job?.operationCheckpoint;
    if (
      job?.state !== "dispatch-intent"
      || checkpoint?.kind !== "command"
      || checkpoint.action !== "new"
      || (checkpoint.recoveryBarrierJobIds?.length ?? 0) === 0
    ) return false;
    for (const candidate of this.candidates.values()) {
      if (candidate.jobId !== jobId || !candidate.classification.sessionBarrier) continue;
      const key = targetKey(candidate.target);
      const current = this.sessionBarriers.get(key);
      if (current === undefined || current.sequence < candidate.sequence) {
        this.sessionBarriers.set(key, candidate);
      }
      return this.sessionBarriers.get(key) === candidate;
    }
    return false;
  }

  /** Reinstall a queued transition once its prior barrier is resolved and its durable intent can run. */
  promoteSessionBarrier(jobId: number): boolean {
    const job = this.deps.store.getIngressJob(jobId);
    const checkpoint = job?.operationCheckpoint;
    if (
      (job?.state !== "accepted" && job?.state !== "dispatch-intent")
      || checkpoint?.kind !== "command"
      || (checkpoint.action !== "new" && checkpoint.action !== "move" && checkpoint.action !== "reset")
    ) return false;
    for (const candidate of this.candidates.values()) {
      if (candidate.jobId !== jobId || !candidate.classification.sessionBarrier) continue;
      const key = targetKey(candidate.target);
      const current = this.sessionBarriers.get(key);
      if (current === undefined || current.sequence < candidate.sequence) {
        this.sessionBarriers.set(key, candidate);
      }
      return this.sessionBarriers.get(key) === candidate;
    }
    return false;
  }
  /** Apply durable session-recovery cancellations to their in-memory ordered candidates. */
  cancelRecoveredIngressJobs(jobIds: readonly number[]): void {
    const cancelled = new Set(jobIds);
    for (const candidate of this.candidates.values()) {
      if (candidate.jobId === null || !cancelled.has(candidate.jobId)) continue;
      const job = this.deps.store.getIngressJob(candidate.jobId);
      if (job?.state === "preparing" && !job.ackReady) continue;
      candidate.cancelled = true;
      candidate.retryable = false;
      candidate.controller.abort(new Error("ingress cancelled by confirmed session recovery"));
      this.releaseCandidateFences(candidate);
      this.resolveAck(candidate);
      this.deps.onCancelled?.(candidate.jobId);
      if (!candidate.active) this.finishCandidate(candidate);
    }
  }

  /** Release only the exact unknown barriers committed by the session-handoff transaction. */
  releaseRecoveredSessionBarriers(jobIds: readonly number[]): void {
    const resolved = new Set(jobIds);
    for (const candidate of this.candidates.values()) {
      if (candidate.jobId === null || !resolved.has(candidate.jobId)) continue;
      candidate.barrierUnknown = false;
      candidate.barrierSettled.resolve();
      const key = targetKey(candidate.target);
      if (this.sessionBarriers.get(key) === candidate) this.sessionBarriers.delete(key);
      if (!candidate.active) this.finishCandidate(candidate);
    }
  }

  private resumePersisted(job: IngressJobRecord): void {
    if (isTerminal(job)) return;
    const mapKey = this.sourceMapKey(job);
    if (this.candidates.has(mapKey)) return;
    const message = job.payload?.kind === "recv" || job.payload?.kind === "synthetic"
      ? job.payload.message
      : null;
    const candidate = this.createCandidate({
      key: { botUid: job.botUid, sourceKind: job.sourceKind, sourceId: job.sourceId },
      target: { channelType: job.channelType, channelId: job.channelId },
      message,
      classification: this.persistedClassification(job),
      restoredJob: job,
    });
    candidate.jobId = job.id;
    const cancellationMustReplay = job.operationCheckpoint?.kind === "command"
      && (
        (job.operationCheckpoint.action === "cancel" && job.operationCheckpoint.phase === "cancel-intent")
        || (
          job.operationCheckpoint.action === "commit"
          && job.operationCheckpoint.phase === "commit-stop-intent"
        )
      );
    if (job.ackReady && !cancellationMustReplay) this.resolveAck(candidate);
    this.candidates.set(mapKey, candidate);
    this.launch(candidate);
  }

  private restoreUnknownBarrier(job: IngressJobRecord): void {
    const checkpoint = job.operationCheckpoint;
    if (checkpoint?.kind === "command" && checkpoint.recoveredByIngressJobId !== undefined) return;
    const classification = this.persistedClassification(job);
    if (!classification.sessionBarrier || classification.action === "fork") return;
    const mapKey = this.sourceMapKey(job);
    if (this.candidates.has(mapKey)) return;
    const candidate = this.createCandidate({
      key: { botUid: job.botUid, sourceKind: job.sourceKind, sourceId: job.sourceId },
      target: { channelType: job.channelType, channelId: job.channelId },
      message: null,
      classification,
      restoredJob: job,
    });
    candidate.jobId = job.id;
    candidate.barrierUnknown = true;
    this.resolveAck(candidate);
    void candidate.admissionBefore.then(() => candidate.admissionSettled.resolve());
    void candidate.businessBefore.then(() => candidate.businessSettled.resolve());
    this.candidates.set(mapKey, candidate);
    this.sessionBarriers.set(targetKey(candidate.target), candidate);
  }

  private persistedClassification(job: IngressJobRecord): IngressClassification {
    const commandCheckpoint = job.operationCheckpoint?.kind === "command"
      ? job.operationCheckpoint
      : null;
    const action = commandCheckpoint?.action
      ?? (job.routeSnapshot?.purpose.kind === "command" ? job.routeSnapshot.purpose.action : null);
    return {
      lane: job.lane,
      action,
      argument: commandCheckpoint?.argument ?? "",
      preparesBody: job.state === "preparing",
      sessionBarrier: action === "new" || action === "move" || action === "reset" || action === "fork",
      cancelsEarlier: action === "cancel" || action === "stop",
    };
  }

  private canProbeSessionBarrier(action: string | null, barrier: Candidate | null): boolean {
    if ((action !== "new" && action !== "reset") || barrier === null) return false;
    if (barrier.jobId === null) return barrier.barrierUnknown;
    const visited = new Set<number>();
    const visit = (jobId: number): boolean => {
      if (visited.has(jobId)) return false;
      visited.add(jobId);
      const job = this.deps.store.getIngressJob(jobId);
      if (job === null) return false;
      const checkpoint = job.operationCheckpoint;
      if (checkpoint?.kind !== "command" || checkpoint.recoveredByIngressJobId !== undefined) {
        return false;
      }
      const sessionTransition = checkpoint.action === "new"
        || checkpoint.action === "move"
        || checkpoint.action === "reset";
      const undispatchedFork = checkpoint.action === "fork"
        && job.state === "accepted"
        && checkpoint.phase === "barrier-installed";
      if (!sessionTransition && !undispatchedFork) return false;
      if (job.state === "unknown") return sessionTransition;
      if (job.state === "dispatch-intent") {
        return sessionTransition && (checkpoint.cancelRequestedByIngressJobIds?.length ?? 0) > 0;
      }
      if (job.state !== "accepted") return false;
      const ancestors = [
        ...(checkpoint.recoveryBarrierJobIds ?? []),
        ...(job.routeSnapshot?.barrierJobId == null ? [] : [job.routeSnapshot.barrierJobId]),
      ];
      return ancestors.some(visit);
    };
    return visit(barrier.jobId);
  }

  private createCandidate(input: {
    key: IngressSourceKey;
    target: ChatTarget;
    message: InboundMessage | null;
    classification: IngressClassification;
    restoredJob: IngressJobRecord | null;
  }): Candidate {
    const admissionSettled = Promise.withResolvers<void>();
    const businessSettled = Promise.withResolvers<void>();
    const barrierSettled = Promise.withResolvers<void>();
    const ack = Promise.withResolvers<void>();
    ack.promise.then(() => {}, () => {});
    const key = targetKey(input.target);
    const ordered = input.classification.lane === "ordered";
    const admissionBefore = ordered ? (this.admissionTails.get(key) ?? RESOLVED) : RESOLVED;
    const businessBefore = ordered ? (this.businessTails.get(key) ?? RESOLVED) : RESOLVED;
    const sessionBarrierBefore = ordered ? (this.sessionBarriers.get(key) ?? null) : null;
    const probeUnknownBarrier = this.canProbeSessionBarrier(input.classification.action, sessionBarrierBefore);
    const candidate: Candidate = {
      ...input,
      sequence: ++this.sequence,
      controller: new AbortController(),
      ack,
      admissionSettled,
      businessSettled,
      barrierSettled,
      admissionBefore,
      businessBefore,
      sessionBarrierBefore,
      jobId: input.restoredJob?.id ?? null,
      ackSettled: false,
      cancelled: false,
      barrierUnknown: false,
      active: false,
      retryable: false,
      redeliveryPending: false,
    };
    if (ordered) {
      this.admissionTails.set(key, admissionSettled.promise);
      this.businessTails.set(key, businessSettled.promise);
      // A waiting transition cannot shadow an already-unknown barrier restored later in the chain.
      if (
        input.classification.sessionBarrier
        && !probeUnknownBarrier
        && sessionBarrierBefore?.barrierUnknown !== true
      ) this.sessionBarriers.set(key, candidate);
    }
    return candidate;
  }

  private launch(candidate: Candidate): void {
    candidate.active = true;
    const operation = this.runCandidate(candidate);
    this.track(operation);
    operation.then(
      () => this.finishCandidate(candidate),
      () => this.finishCandidate(candidate),
    );
  }

  private async runCandidate(candidate: Candidate): Promise<void> {
    let admissionReleased = false;
    let businessReleased = false;
    const releaseAdmission = (): void => {
      if (admissionReleased) return;
      admissionReleased = true;
      candidate.admissionSettled.resolve();
    };
    const releaseBusiness = (): void => {
      if (businessReleased) return;
      businessReleased = true;
      candidate.businessSettled.resolve();
    };
    const cancelBehindRecoveredBarrier = (
      job: IngressJobRecord | null,
      message: InboundMessage,
    ): boolean => {
      if (candidate.classification.lane !== "ordered" || job === null) return false;
      const barrierJobId = job.routeSnapshot?.barrierJobId ?? null;
      if (barrierJobId === null) return false;
      const barrier = this.deps.store.getIngressJob(barrierJobId);
      const checkpoint = barrier?.operationCheckpoint;
      if (
        checkpoint?.kind !== "command"
        || checkpoint.recoveredByIngressJobId === undefined
        || job?.routeSnapshot?.purpose.kind === "questions"
        || job?.operationCheckpoint?.kind === "session-recovery-cancellation"
      ) return false;
      const recovery = this.deps.store.getIngressJob(checkpoint.recoveredByIngressJobId);
      if (
        recovery === null
        || recovery.state !== "handed-off"
        || (job !== null && job.scheduleOrder >= recovery.scheduleOrder)
      ) return false;
      const cancelled = this.deps.store.cancelIngressJobAfterSessionRecovery(
        candidate.key,
        candidate.target,
        message.messageId,
        barrierJobId,
      );
      if (cancelled === null || cancelled.state !== "cancelled") {
        throw new Error("ingress after a recovered session barrier could not be durably cancelled");
      }
      candidate.restoredJob = cancelled;
      candidate.jobId = cancelled.id;
      candidate.cancelled = true;
      this.resolveAck(candidate);
      return true;
    };
    try {
      await candidate.admissionBefore;
      if (this.sealed) throw new InboundShutdownError();
      const commitStop = candidate.classification.action === "commit"
        && candidate.classification.argument.trim() === "stop";
      const cancellationCandidates = candidate.classification.cancelsEarlier || commitStop
        ? [...this.candidates.values()].flatMap<IngressCancellationCandidate>(earlier => {
            const message = earlier.message;
            if (
              message === null
              || earlier === candidate
              || earlier.sequence >= candidate.sequence
              || earlier.classification.lane !== "ordered"
              || !sameTarget(earlier.target, candidate.target)
              || (commitStop && (
                earlier.classification.action !== "commit"
                || earlier.classification.argument.trim().length !== 0
              ))
            ) return [];
            return [{ ...earlier.key, messageId: message.messageId }];
          })
        : undefined;
      let job = candidate.restoredJob ?? this.deps.store.getIngressJobBySource(candidate.key);
      const persistedMessage = job?.payload?.kind === "recv"
        ? job.payload.message
        : job?.payload?.kind === "synthetic"
          ? job.payload.message
          : null;
      const existingMessage = candidate.message ?? persistedMessage;
      if (job !== null && existingMessage !== null
        && cancelBehindRecoveredBarrier(job, existingMessage)) return;
      if (job === null) {
        const message = candidate.message;
        if (message === null) throw new Error("recoverable ingress payload is missing its message");
        const admissionOperation = this.deps.authorize(message, candidate.controller.signal);
        this.track(admissionOperation);
        const admission = await this.abortable(admissionOperation, candidate.controller.signal);
        if (admission.kind === "consumed") {
          this.resolveAck(candidate);
          return;
        }
        if (cancelBehindRecoveredBarrier(null, message)) return;
        const binding = this.deps.bindingSnapshot(candidate.target);
        const barrierJobId = candidate.sessionBarrierBefore?.jobId ?? null;
        const routeSnapshot: IngressRouteSnapshot = {
          purpose: admission.purpose,
          sessionGeneration: barrierJobId ?? 0,
          binding,
          barrierJobId,
        };
        const checkpoint = admission.checkpoint === null ? null : {
          ...admission.checkpoint,
          expectedBinding: binding,
        };
        const accepted = this.deps.store.acceptIngressJob({
          ...candidate.key,
          ...candidate.target,
          messageId: message.messageId,
          payload: { kind: "recv", message },
          routeSnapshot,
          lane: candidate.classification.lane,
          state: admission.preparesBody ? "preparing" : "accepted",
          ackReady: !admission.preparesBody,
          operationCheckpoint: checkpoint,
          ...(cancellationCandidates === undefined ? {} : { cancellationCandidates }),
        });
        job = accepted.job;
      }
      candidate.restoredJob = job;
      candidate.jobId = job.id;
      releaseAdmission();

      if (!isTerminal(job)) {
        if (candidate.classification.action === "interrupt") this.applyInterruptEffects(job);
        else if (candidate.classification.cancelsEarlier || commitStop) this.cancelEarlier(candidate);
      }
      if (job.ackReady) {
        if (!isTerminal(job)) this.deps.onReliableAccepted?.(job);
        this.resolveAck(candidate);
      }
      if (isTerminal(job)) {
        if (job.state === "cancelled") this.resolveAck(candidate);
        return;
      }
      this.deps.onAccepted?.(job);

      if (!job.ackReady) {
        const message = job.payload?.kind === "recv" || job.payload?.kind === "synthetic"
          ? job.payload.message
          : null;
        const purpose = job.routeSnapshot?.purpose;
        if (message === null || purpose === undefined) throw new Error("preparing ingress job lost its durable payload");
        const prepared = await this.deps.prepare(message, purpose, candidate.controller.signal, job);
        const current = this.deps.store.getIngressJob(job.id);
        if (
          prepared.kind === "consumed"
          && current?.state === "preparing"
          && current.operationCheckpoint?.kind === "session-recovery-cancellation"
        ) {
          if (!this.deps.store.finishIngressRecoveryPreparation(
            job.id,
            prepared.consumer,
            prepared.persistConsumer ?? (() => true),
          )) throw new Error("cancelled ingress preparation outcome could not be persisted");
          this.publishAfterCommit(prepared.publishAfterCommit);
          const settled = this.deps.store.getIngressJob(job.id);
          if (settled === null) throw new Error("cancelled ingress preparation disappeared");
          this.deps.onReliableAccepted?.(settled);
          this.resolveAck(candidate);
          return;
        }
        if (prepared.kind === "prepared") {
          if (!this.deps.store.markIngressPrepared(job.id, prepared.text, prepared.spoolPaths)) {
            const changed = this.deps.store.getIngressJob(job.id);
            if (changed !== null && isTerminal(changed)) {
              if (changed.ackReady) this.resolveAck(candidate);
              return;
            }
            throw new Error("ingress preparation could not be persisted");
          }
          job = this.deps.store.getIngressJob(job.id);
          if (job === null) throw new Error("prepared ingress job disappeared");
          this.deps.onReliableAccepted?.(job);
          this.resolveAck(candidate);
          if (isTerminal(job)) return;
        } else {
          const handedOff = this.deps.store.handoffIngressJob(
            job.id,
            prepared.consumer,
            prepared.persistConsumer ?? (() => true),
          );
          if (!handedOff) throw new Error("consumed ingress preparation could not be handed off");
          this.publishAfterCommit(prepared.publishAfterCommit);
          this.deps.onReliableAccepted?.(job);
          this.resolveAck(candidate);
          return;
        }
      }

      const probeUnknownBarrier = this.canProbeSessionBarrier(
        candidate.classification.action,
        candidate.sessionBarrierBefore,
      );
      if (!probeUnknownBarrier) await candidate.businessBefore;
      if (candidate.sessionBarrierBefore !== null && !probeUnknownBarrier) {
        await this.abortable(
          candidate.sessionBarrierBefore.barrierSettled.promise,
          candidate.controller.signal,
        );
      }
      if (
        candidate.sessionBarrierBefore !== null
        && !probeUnknownBarrier
        && candidate.classification.sessionBarrier
        && candidate.jobId !== null
      ) this.promoteSessionBarrier(candidate.jobId);
      if (candidate.controller.signal.aborted) throw errorFromAbort(candidate.controller.signal);
      await this.deps.awaitDispatchReady();
      if (candidate.controller.signal.aborted) throw errorFromAbort(candidate.controller.signal);

      let current = this.deps.store.getIngressJob(job.id);
      if (current === null) return;
      if (isTerminal(current)) {
        if (current.state === "unknown" && candidate.classification.sessionBarrier) {
          this.retainUnknownSessionBarrier(candidate);
        }
        return;
      }
      if (current.state === "dispatch-intent") {
        this.markUnknown(current);
        const updated = this.deps.store.getIngressJob(current.id);
        if (
          candidate.classification.sessionBarrier
          && (updated?.state === "unknown" || updated?.state === "dispatch-intent")
        ) this.retainUnknownSessionBarrier(candidate);
        return;
      }
      if (
        current.sourceKind === "bot-event"
        && current.operationCheckpoint?.kind !== "config-action"
      ) {
        this.markUnknown(current);
        return;
      }
      const dispatched = await this.deps.dispatch(current);
      if (dispatched.completion !== undefined) {
        if (!dispatched.holdBusinessUntilHandoff) releaseBusiness();
        await dispatched.completion;
      }
      const consumer = dispatched.resolveConsumer?.() ?? dispatched.consumer;
      const handedOff = this.deps.store.handoffIngressJob(
        current.id,
        consumer,
        dispatched.persistConsumer ?? (() => true),
      );
      if (!handedOff) {
        const changed = this.deps.store.getIngressJob(current.id);
        if (changed?.state === "cancelled") {
          this.resolveAck(candidate);
          releaseBusiness();
          return;
        }
        if (
          dispatched.cancelOnPersistFailureWhenRequested?.()
          && this.deps.store.cancelIngressJobAfterCancellationRequest(current.id)
        ) {
          this.deps.onCancelled?.(current.id);
          this.resolveAck(candidate);
          releaseBusiness();
          return;
        }
        if (changed === null || changed.state !== "handed-off") {
          throw new Error("ingress consumer handoff was not persisted");
        }
      } else {
        this.publishAfterCommit(dispatched.publishAfterCommit);
      }
      releaseBusiness();
    } catch (error) {
      if (error instanceof IngressSuspendedError) return;
      const persisted = this.deps.store.getIngressJobBySource(candidate.key);
      if (candidate.cancelled || persisted?.state === "cancelled") {
        this.resolveAck(candidate);
        return;
      }
      if (persisted?.state === "dispatch-intent") this.markUnknown(persisted);
      const afterUnknown = persisted === null
        ? null
        : this.deps.store.getIngressJob(persisted.id);
      if (
        candidate.classification.sessionBarrier
        && (afterUnknown?.state === "unknown" || afterUnknown?.state === "dispatch-intent")
      ) this.retainUnknownSessionBarrier(candidate);
      if (persisted?.state === "unknown" || afterUnknown?.state === "unknown") return;
      if (
        !candidate.ackSettled
        && !candidate.cancelled
        && !this.sealed
        && !candidate.controller.signal.aborted
        && persisted?.state === "preparing"
        && !persisted.ackReady
      ) {
        candidate.retryable = true;
        this.rejectAck(candidate, error instanceof Error ? error : new Error(String(error)));
        return;
      }
      if (!candidate.ackSettled) {
        this.rejectAck(candidate, error instanceof Error ? error : new Error(String(error)));
      }
      throw error;
    } finally {
      releaseAdmission();
      // A retryable preparation failure keeps its ordered position without leaving a pending operation.
      if (!candidate.retryable) {
        releaseBusiness();
        if (!candidate.barrierUnknown) candidate.barrierSettled.resolve();
      }
    }
  }

  private retainUnknownSessionBarrier(candidate: Candidate): void {
    if (candidate.classification.action === "fork") return;
    candidate.barrierUnknown = true;
    const key = targetKey(candidate.target);
    const current = this.sessionBarriers.get(key);
    if (current === undefined || !current.barrierUnknown || current.sequence < candidate.sequence) {
      this.sessionBarriers.set(key, candidate);
    }
  }

  private markUnknown(job: IngressJobRecord): void {
    if (this.deps.markUnknown !== undefined) {
      this.deps.markUnknown(job);
      return;
    }
    this.deps.store.markIngressUnknown(job.id);
  }

  private publishAfterCommit(publish?: () => void | Promise<void>): void {
    if (publish === undefined) return;
    try {
      const pending = publish();
      if (pending !== undefined) {
        const observed = Promise.resolve(pending).catch(error => {
          console.error(`[ingress publish] ${error instanceof Error ? error.message : String(error)}`);
        });
        this.track(observed);
      }
    } catch (error) {
      console.error(`[ingress publish] ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private applyInterruptEffects(job: IngressJobRecord): void {
    const checkpoint = job.operationCheckpoint;
    if (checkpoint?.kind !== "command" || checkpoint.action !== "interrupt") return;
    for (const jobId of checkpoint.cancelledHostIngressJobIds ?? []) {
      this.deps.onCancelled?.(jobId);
      for (const candidate of this.candidates.values()) {
        if (candidate.jobId !== jobId) continue;
        candidate.cancelled = true;
        candidate.retryable = false;
        candidate.controller.abort(new Error("host operation interrupted before dispatch"));
        this.releaseCandidateFences(candidate);
        this.resolveAck(candidate);
        if (!candidate.active) this.finishCandidate(candidate);
      }
    }
  }

  private releaseCandidateFences(candidate: Candidate): void {
    candidate.admissionSettled.resolve();
    candidate.businessSettled.resolve();
    if (!candidate.barrierUnknown) candidate.barrierSettled.resolve();
  }

  private cancelEarlier(canceller: Candidate): void {
    const commitStop = canceller.classification.action === "commit"
      && canceller.classification.argument.trim() === "stop";
    const checkpoint = canceller.restoredJob?.operationCheckpoint;
    const durableCancelledIds = checkpoint?.kind !== "command"
      ? new Set<number>()
      : new Set(
          commitStop
            ? checkpoint.cancelledCommitIngressJobIds ?? []
            : canceller.classification.cancelsEarlier
              ? checkpoint.cancelledIngressJobIds ?? []
              : [],
        );
    for (const candidate of this.candidates.values()) {
      if (
        candidate === canceller
        || candidate.sequence >= canceller.sequence
        || candidate.classification.lane !== "ordered"
        || !sameTarget(candidate.target, canceller.target)
        || (commitStop && (
          candidate.classification.action !== "commit"
          || candidate.classification.argument.trim().length !== 0
        ))
      ) continue;
      const persisted = this.deps.store.getIngressJobBySource(candidate.key);
      if (persisted?.operationCheckpoint?.kind === "config-action") continue;
      if (persisted?.state === "dispatch-intent") continue;
      let cancelled = false;
      let cancelledJobId: number | null = null;
      if (persisted === null) {
        const message = candidate.message;
        if (message === null) continue;
        cancelled = this.deps.store.cancelIngressJobBySource(
          candidate.key,
          {
            channelType: message.channelType,
            channelId: message.channelId,
            messageId: message.messageId,
          },
          canceller.classification.cancelsEarlier || commitStop
            ? canceller.jobId ?? undefined
            : undefined,
        );
        if (cancelled) {
          const tombstone = this.deps.store.getIngressJobBySource(candidate.key);
          if (tombstone?.state === "cancelled") cancelledJobId = tombstone.id;
        }
      } else if (persisted.state === "cancelled" && durableCancelledIds.has(persisted.id)) {
        cancelled = true;
        cancelledJobId = persisted.id;
      } else if (!isTerminal(persisted)) {
        const message = candidate.message;
        cancelled = this.deps.store.cancelIngressJobBySource(
          candidate.key,
          message === null
            ? {
                channelType: persisted.channelType,
                channelId: persisted.channelId,
                messageId: persisted.messageId ?? "",
              }
            : {
                channelType: message.channelType,
                channelId: message.channelId,
                messageId: message.messageId,
              },
          canceller.jobId ?? undefined,
        );
        if (cancelled) cancelledJobId = persisted.id;
      }
      if (!cancelled) continue;
      candidate.cancelled = true;
      candidate.retryable = false;
      candidate.controller.abort(new Error("ingress cancelled by later control command"));
      if (cancelledJobId !== null) this.deps.onCancelled?.(cancelledJobId);
      this.releaseCandidateFences(candidate);
      this.resolveAck(candidate);
      if (!candidate.active) this.finishCandidate(candidate);
  }
  }
  private async abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) throw errorFromAbort(signal);
    const aborted = Promise.withResolvers<never>();
    const onAbort = (): void => aborted.reject(errorFromAbort(signal));
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      return await Promise.race([operation, aborted.promise]);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  private resolveAck(candidate: Candidate): void {
    if (candidate.ackSettled) return;
    candidate.ackSettled = true;
    candidate.ack.resolve();
  }

  private rejectAck(candidate: Candidate, error: Error): void {
    if (candidate.ackSettled) return;
    candidate.ackSettled = true;
    candidate.ack.reject(error);
  }

  private finishCandidate(candidate: Candidate): void {
    candidate.active = false;
    if (candidate.retryable && !candidate.cancelled && !this.sealed) {
      if (candidate.redeliveryPending) {
        candidate.redeliveryPending = false;
        candidate.retryable = false;
        this.launch(candidate);
      }
      return;
    }
    candidate.retryable = false;
    candidate.redeliveryPending = false;
    if (!candidate.barrierUnknown) this.releaseCandidateFences(candidate);
    const mapKey = this.sourceMapKey(candidate.key);
    let removedCandidate = false;
    if (!candidate.barrierUnknown && this.candidates.get(mapKey) === candidate) {
      this.candidates.delete(mapKey);
      removedCandidate = true;
    }
    const key = targetKey(candidate.target);
    if (!candidate.barrierUnknown && this.sessionBarriers.get(key) === candidate) {
      this.sessionBarriers.delete(key);
    }
    if (this.admissionTails.get(key) === candidate.admissionSettled.promise) this.admissionTails.delete(key);
    if (this.businessTails.get(key) === candidate.businessSettled.promise) this.businessTails.delete(key);
    if (removedCandidate && this.candidates.size === 0) this.deps.onIdle?.();
  }

  private track<T>(operation: Promise<T>): void {
    this.operations.add(operation);
    operation.then(
      () => this.operations.delete(operation),
      () => this.operations.delete(operation),
    );
  }

  private sourceMapKey(key: IngressSourceKey): string {
    return JSON.stringify([key.botUid, key.sourceKind, key.sourceId]);
  }
}
