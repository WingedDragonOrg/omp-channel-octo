import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { BindingRecord, OutboxInsert, WorkerLedgerRecord } from "./store-records.ts";
import {
  type IngressBindingSnapshot,
  type IngressCancellationCandidate,
  type IngressConsumer,
  type IngressJobInput,
  type IngressJobRecord,
  type IngressOperationCheckpoint,
  type IngressPayload,
  type IngressRouteSnapshot,
  type IngressSessionHandoff,
  type IngressSessionRecoveryResult,
  type IngressSourceKey,
  type IngressSourceKind,
  type IngressState,
} from "../inbound/ingress-types.ts";
import { allocateScheduleOrder, asNullableString, asNumber, asString, query, run, targetValues, targetWhere, transaction, type SqlRow, type SqlValue } from "./store-schema.ts";
import { ensureBindingIfMatches, getBinding } from "./store-chats.ts";
import { enqueueOutbox } from "./store-outbox.ts";
import { cancelQueued, isCommitTask, markCancelled, markTaskStopIntent } from "./store-tasks.ts";
import { cancelQuestions, pendingQuestionIds } from "./store-questions.ts";
import { recordWorker } from "./store-workers.ts";

function sourceValues(key: IngressSourceKey): SqlValue[] {
  return [key.botUid, key.sourceKind, key.sourceId];
}

function sourceRow(db: Database, key: IngressSourceKey): SqlRow | undefined {
  return query(
    db,
    "SELECT * FROM ingress_jobs WHERE bot_uid = ? AND source_kind = ? AND source_id = ?",
    sourceValues(key),
  )[0];
}

function jobRow(db: Database, id: number): SqlRow | undefined {
  return query(db, "SELECT * FROM ingress_jobs WHERE id = ?", [id])[0];
}

function serialize(value: object): string {
  return JSON.stringify(value)!;
}

function parseJson<T>(value: string | null): T | null {
  return value === null ? null : JSON.parse(value) as T;
}

function ingressJobFromRow(row: SqlRow): IngressJobRecord {
  const checkpointJson = asNullableString(row.operation_checkpoint_json);
  const consumerKind = asNullableString(row.consumer_kind);
  return {
    id: asNumber(row.id),
    scheduleOrder: asNumber(row.schedule_order),
    botUid: asString(row.bot_uid),
    sourceKind: asString(row.source_kind) as IngressSourceKind,
    sourceId: asString(row.source_id),
    channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
    channelId: asString(row.channel_id),
    messageId: asNullableString(row.message_id),
    payload: parseJson<IngressPayload>(asNullableString(row.payload_json)),
    routeSnapshot: parseJson<IngressRouteSnapshot>(asNullableString(row.route_snapshot_json)),
    preparedText: asNullableString(row.prepared_text),
    spoolPaths: JSON.parse(asString(row.spool_paths_json)) as string[],
    ackReady: asNumber(row.ack_ready) !== 0,
    lane: asString(row.lane) as IngressJobRecord["lane"],
    state: asString(row.state) as IngressState,
    operationCheckpoint: checkpointJson === null ? null : JSON.parse(checkpointJson) as IngressOperationCheckpoint,
    consumerKind: consumerKind as IngressConsumer["kind"] | null,
    consumerId: asNullableString(row.consumer_id),
    createdAt: asNumber(row.created_at),
    updatedAt: asNumber(row.updated_at),
  };
}

function addCancellationRequest(db: Database, id: number, cancellationJobId: number): boolean {
  const row = jobRow(db, id);
  if (row === undefined) return false;
  const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
  if (checkpoint?.kind === "config-action") return false;
  const requestedBy = checkpoint?.kind === "command" || checkpoint?.kind === "cancellation-request"
    ? checkpoint.cancelRequestedByIngressJobIds ?? []
    : checkpoint === null
      ? []
      : null;
  if (requestedBy === null) return false;
  const nextCheckpoint: IngressOperationCheckpoint = checkpoint?.kind === "command"
    ? {
        ...checkpoint,
        cancelRequestedByIngressJobIds: [...new Set([...requestedBy, cancellationJobId])],
      }
    : {
        kind: "cancellation-request",
        cancelRequestedByIngressJobIds: [...new Set([...requestedBy, cancellationJobId])],
      };
  return run(
    db,
    "UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ? WHERE id = ? AND state = 'dispatch-intent'",
    [serialize(nextCheckpoint), Date.now(), id],
  ).changes === 1;
}

function insertCancellationTombstone(
  db: Database,
  key: IngressSourceKey,
  target: ChatTarget,
  messageId: string,
): number {
  const now = Date.now();
  const scheduleOrder = allocateScheduleOrder(db);
  run(
    db,
    `INSERT INTO ingress_jobs(
       bot_uid, source_kind, source_id, channel_type, channel_id, message_id,
       payload_json, route_snapshot_json, prepared_text, spool_paths_json,
       ack_ready, lane, state, operation_checkpoint_json, schedule_order, created_at, updated_at
     ) VALUES(?, ?, ?, ?, ?, ?, NULL, NULL, NULL, '[]', 1, 'control', 'cancelled', NULL, ?, ?, ?)
     ON CONFLICT(bot_uid, source_kind, source_id) DO NOTHING`,
    [
      key.botUid,
      key.sourceKind,
      key.sourceId,
      target.channelType,
      target.channelId,
      messageId,
      scheduleOrder,
      now,
      now,
    ],
  );
  const tombstone = sourceRow(db, key);
  if (tombstone === undefined) throw new Error("cancelled ingress tombstone disappeared");
  return asNumber(tombstone.id);
}

function cancelCandidateInTransaction(
  db: Database,
  input: IngressJobInput,
  candidate: IngressCancellationCandidate,
  cancellationJobId: number,
  commitOnly: boolean,
): { cancelledId?: number; requestedId?: number } {
  if (candidate.botUid !== input.botUid) return {};
  const target: ChatTarget = { channelType: input.channelType, channelId: input.channelId };
  const row = sourceRow(db, candidate);
  if (row === undefined) return { cancelledId: insertCancellationTombstone(db, candidate, target, candidate.messageId) };
  const id = asNumber(row.id);
  if (
    asString(row.bot_uid) !== input.botUid
    || asNumber(row.channel_type) !== target.channelType
    || asString(row.channel_id) !== target.channelId
    || asString(row.lane) !== "ordered"
    || (asNullableString(row.message_id) !== null
      && asNullableString(row.message_id) !== candidate.messageId)
  ) return {};
  const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
  if (checkpoint?.kind === "config-action") return {};
  if (commitOnly && (
    checkpoint?.kind !== "command"
    || checkpoint.action !== "commit"
    || checkpoint.argument.trim().length !== 0
  )) return {};
  const state = asString(row.state);
  if (state === "cancelled") return { cancelledId: id };
  if (state === "accepted" || state === "preparing") {
    if (!cancelUndispatchedRow(db, id)) return {};
    const messageId = asNullableString(row.message_id);
    if (messageId !== null && checkpoint?.kind === "command" && asString(row.source_kind) !== "bot-event") {
      run(
        db,
        `DELETE FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
        [input.botUid, target.channelType, target.channelId, messageId],
      );
    }
    return { cancelledId: id };
  }
  if (!commitOnly && state === "dispatch-intent" && addCancellationRequest(db, id, cancellationJobId)) {
    return { requestedId: id };
  }
  return {};
}

/** Persist cancellation effects in the same transaction that makes the ingress job ACKable. */
function persistCancelIntent(
  db: Database,
  input: IngressJobInput,
  currentJobId: number,
): {
  cancelledIngressJobIds: number[];
  cancelRequestedIngressJobIds: number[];
  cancelledQuestionIds: string[];
  cancelledQueuedTaskIds: string[];
  stoppedTaskIds: string[];
  cancelRequestedWorkspaceOperationIds: string[];
} {
  const target: ChatTarget = { channelType: input.channelType, channelId: input.channelId };
  const cancelledIngressJobIds = new Set<number>();
  const cancelRequestedIngressJobIds = new Set<number>();
  const earlierJobs = query(
    db,
    `SELECT id, message_id, source_kind, state, operation_checkpoint_json FROM ingress_jobs
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND lane = 'ordered'
       AND id < ? AND state IN ('accepted', 'preparing', 'dispatch-intent')
     ORDER BY id ASC`,
    [input.botUid, target.channelType, target.channelId, currentJobId],
  );
  for (const row of earlierJobs) {
    const id = asNumber(row.id);
    const checkpoint = parseJson<IngressOperationCheckpoint>(
      asNullableString(row.operation_checkpoint_json),
    );
    if (checkpoint?.kind === "config-action") continue;
    if (asString(row.state) === "dispatch-intent") {
      if (addCancellationRequest(db, id, currentJobId)) cancelRequestedIngressJobIds.add(id);
      continue;
    }
    if (!cancelUndispatchedRow(db, id)) continue;
    const messageId = asNullableString(row.message_id);
    const sourceKind = asString(row.source_kind);
    if (
      checkpoint?.kind === "command"
      && messageId !== null
      && sourceKind !== "bot-event"
    ) {
      run(
        db,
        `DELETE FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
        [input.botUid, target.channelType, target.channelId, messageId],
      );
    }
    cancelledIngressJobIds.add(id);
  }
  for (const candidate of input.cancellationCandidates ?? []) {
    const result = cancelCandidateInTransaction(db, input, candidate, currentJobId, false);
    if (result.cancelledId !== undefined) cancelledIngressJobIds.add(result.cancelledId);
    if (result.requestedId !== undefined) cancelRequestedIngressJobIds.add(result.requestedId);
  }
  run(db,
    `UPDATE host_merge_leases
     SET stop_requested = 1, updated_at = ?
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
       AND operation_kind = 'merge' AND auto_commit_task_id IS NOT NULL
       AND outcome_json IS NULL AND not_started_text IS NULL AND stop_requested = 0`,
    [Date.now(), input.botUid, target.channelType, target.channelId],
  );


  const cancelledQuestionIds = pendingQuestionIds(db, input.botUid, target);
  cancelQuestions(db, input.botUid, target, "cancelled");
  const cancelledQueuedTaskIds = cancelQueued(db, input.botUid, target);
  const activeTaskIds = query(
    db,
    `SELECT id FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'started'`,
    [input.botUid, ...targetValues(target)],
  );
  const stoppedTaskIds: string[] = [];
  for (const row of activeTaskIds) {
    const taskId = asString(row.id);
    if (markTaskStopIntent(db, taskId, "cancel")) stoppedTaskIds.push(taskId);
  }
  const cancelRequestedWorkspaceOperationIds: string[] = [];
  if (target.channelType === 5) {
    for (const row of query(
      db,
      `SELECT id FROM thread_workspace_operations
       WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND status IN ('queued', 'running')
       ORDER BY schedule_order ASC, created_at ASC, id ASC`,
      [input.botUid, target.channelId],
    )) {
      const operationId = asString(row.id);
      if (run(
        db,
        `UPDATE thread_workspace_operations SET cancel_requested = 1, updated_at = ?
         WHERE bot_uid = ? AND id = ? AND status IN ('queued', 'running')`,
        [Date.now(), input.botUid, operationId],
      ).changes === 1) cancelRequestedWorkspaceOperationIds.push(operationId);
    }
  }
  return {
    cancelledIngressJobIds: [...cancelledIngressJobIds],
    cancelRequestedIngressJobIds: [...cancelRequestedIngressJobIds],
    cancelledQuestionIds,
    cancelledQueuedTaskIds,
    stoppedTaskIds,
    cancelRequestedWorkspaceOperationIds,
  };
}

/** Persist exact earlier commit waiters and task stops before ACKing `/commit stop`. */
function persistCommitStopIntent(
  db: Database,
  input: IngressJobInput,
  currentJobId: number,
): {
  cancelledCommitIngressJobIds: number[];
  cancelledCommitTaskIds: string[];
  stoppedCommitTaskIds: string[];
} {
  const target: ChatTarget = { channelType: input.channelType, channelId: input.channelId };
  const cancelledCommitIngressJobIds = new Set<number>();
  const pendingJobs = query(
    db,
    `SELECT id, message_id, operation_checkpoint_json FROM ingress_jobs
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
       AND id < ? AND state IN ('accepted', 'preparing')
     ORDER BY id ASC`,
    [input.botUid, target.channelType, target.channelId, currentJobId],
  );
  for (const row of pendingJobs) {
    const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
    if (
      checkpoint?.kind !== "command"
      || checkpoint.action !== "commit"
      || checkpoint.argument.trim().length !== 0
    ) continue;
    const id = asNumber(row.id);
    if (!cancelUndispatchedRow(db, id)) continue;
    const messageId = asNullableString(row.message_id);
    if (messageId !== null) {
      run(
        db,
        `DELETE FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
        [input.botUid, target.channelType, target.channelId, messageId],
      );
    }
    cancelledCommitIngressJobIds.add(id);
  }
  for (const candidate of input.cancellationCandidates ?? []) {
    const result = cancelCandidateInTransaction(db, input, candidate, currentJobId, true);
    if (result.cancelledId !== undefined) cancelledCommitIngressJobIds.add(result.cancelledId);
  }

  const taskRows = query(
    db,
    `SELECT id, state FROM tasks
     WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('queued', 'started')
     ORDER BY accepted_at ASC, rowid ASC`,
    [input.botUid, ...targetValues(target)],
  );
  const cancelledCommitTaskIds: string[] = [];
  const stoppedCommitTaskIds: string[] = [];
  for (const row of taskRows) {
    const taskId = asString(row.id);
    if (!isCommitTask(db, taskId)) continue;
    if (asString(row.state) === "queued") {
      markCancelled(db, taskId, "commit cancelled before start");
      if (asString(query(db, "SELECT state FROM tasks WHERE id = ?", [taskId])[0]?.state) === "cancelled") {
        cancelledCommitTaskIds.push(taskId);
      }
    } else if (markTaskStopIntent(db, taskId, "cancel")) {
      stoppedCommitTaskIds.push(taskId);
    }
  }
  return {
    cancelledCommitIngressJobIds: [...cancelledCommitIngressJobIds],
    cancelledCommitTaskIds,
    stoppedCommitTaskIds,
  };
}


/** Persist `/interrupt` task and host-operation effects before the command becomes ACKable. */
function persistInterruptIntent(
  db: Database,
  input: IngressJobInput,
  currentJobId: number,
): {
  interruptedTaskIds: string[];
  cancelledHostIngressJobIds: number[];
  interruptRequestedHostIngressJobIds: number[];
} {
  const target: ChatTarget = { channelType: input.channelType, channelId: input.channelId };
  const activeTasks = query(
    db,
    `SELECT id FROM tasks
     WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'started'
     ORDER BY accepted_at ASC, rowid ASC`,
    [input.botUid, ...targetValues(target)],
  );
  const activeTaskIds = activeTasks.map(row => asString(row.id));
  const commitActive = activeTaskIds.some(taskId => isCommitTask(db, taskId));
  const interruptedTaskIds = commitActive
    ? []
    : activeTaskIds.filter(taskId => markTaskStopIntent(db, taskId, "interrupt"));

  const cancelledHostIngressJobIds: number[] = [];
  const interruptRequestedHostIngressJobIds: number[] = [];
  const earlierJobs = query(
    db,
    `SELECT id, message_id, source_kind, state, operation_checkpoint_json
     FROM ingress_jobs
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND id < ?
       AND state IN ('accepted', 'preparing', 'dispatch-intent')
     ORDER BY id ASC`,
    [input.botUid, target.channelType, target.channelId, currentJobId],
  );
  for (const row of earlierJobs) {
    const checkpoint = parseJson<IngressOperationCheckpoint>(
      asNullableString(row.operation_checkpoint_json),
    );
    if (
      checkpoint?.kind !== "command"
      || !(
        checkpoint.action === "new"
        || checkpoint.action === "move"
        || checkpoint.action === "fork"
        || (
          checkpoint.action === "thread"
          && /^(?:merge(?:$|[^\p{L}\p{N}_-])|remove(?:$|\s))/iu.test(checkpoint.argument.trim())
        )
      )
    ) continue;
    const id = asNumber(row.id);
    if (asString(row.state) === "dispatch-intent") {
      interruptRequestedHostIngressJobIds.push(id);
      continue;
    }
    if (!cancelUndispatchedRow(db, id)) continue;
    const messageId = asNullableString(row.message_id);
    if (messageId !== null && asString(row.source_kind) !== "bot-event") {
      run(
        db,
        `DELETE FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
        [input.botUid, target.channelType, target.channelId, messageId],
      );
    }
    cancelledHostIngressJobIds.push(id);
  }
  return {
    interruptedTaskIds,
    cancelledHostIngressJobIds,
    interruptRequestedHostIngressJobIds,
  };
}

export function acceptIngressJob(
  db: Database,
  input: IngressJobInput,
): { job: IngressJobRecord; created: boolean } {
  return transaction(db, () => {
    const existing = sourceRow(db, input);
    if (existing) return { job: ingressJobFromRow(existing), created: false };
    const checkpoint = input.operationCheckpoint;
    const cancelCheckpoint = checkpoint?.kind === "command" && checkpoint.action === "cancel"
      ? { ...checkpoint, phase: "cancel-intent" as const }
      : null;
    const commitStopCheckpoint = checkpoint?.kind === "command"
      && checkpoint.action === "commit"
      && checkpoint.argument.trim() === "stop"
      ? { ...checkpoint, phase: "commit-stop-intent" as const }
      : null;
    const interruptCheckpoint = checkpoint?.kind === "command" && checkpoint.action === "interrupt"
      ? { ...checkpoint, phase: "interrupt-intent" as const }
      : null;
    const operationCheckpoint = cancelCheckpoint
      ?? commitStopCheckpoint
      ?? interruptCheckpoint
      ?? checkpoint;
    const scheduleOrder = allocateScheduleOrder(db);
    const now = Date.now();
    const inserted = run(db,
      `INSERT INTO ingress_jobs(
         bot_uid, source_kind, source_id, channel_type, channel_id, message_id,
         payload_json, route_snapshot_json, prepared_text, spool_paths_json,
         ack_ready, lane, state, operation_checkpoint_json, schedule_order, created_at, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, '[]', ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(bot_uid, source_kind, source_id) DO NOTHING`,
      [
        input.botUid,
        input.sourceKind,
        input.sourceId,
        input.channelType,
        input.channelId,
        input.messageId,
        serialize(input.payload),
        serialize(input.routeSnapshot),
        input.ackReady ? 1 : 0,
        input.lane,
        input.state,
        operationCheckpoint === null ? null : serialize(operationCheckpoint),
        scheduleOrder,
        now,
        now,
      ],
    ).changes === 1;
    if (
      inserted
      && cancelCheckpoint?.kind === "command"
      && cancelCheckpoint.action === "cancel"
    ) {
      const currentRow = sourceRow(db, input);
      if (currentRow === undefined) throw new Error("cancel ingress row disappeared during acceptance");
      const cancellation = persistCancelIntent(db, input, asNumber(currentRow.id));
      const persistedCheckpoint: IngressOperationCheckpoint = {
        ...cancelCheckpoint,
        ...cancellation,
      };
      const saved = run(
        db,
        "UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ? WHERE id = ? AND state = ?",
        [serialize(persistedCheckpoint), Date.now(), asNumber(currentRow.id), input.state],
      ).changes === 1;
      if (!saved) throw new Error("cancel intent checkpoint could not be persisted");
    }
    if (inserted && commitStopCheckpoint !== null) {
      const currentRow = sourceRow(db, input)!;
      const cancellation = persistCommitStopIntent(db, input, asNumber(currentRow.id));
      const persistedCheckpoint: IngressOperationCheckpoint = {
        ...commitStopCheckpoint,
        ...cancellation,
      };
      const saved = run(
        db,
        "UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ? WHERE id = ? AND state = ?",
        [serialize(persistedCheckpoint), Date.now(), asNumber(currentRow.id), input.state],
      ).changes === 1;
      if (!saved) throw new Error("commit stop intent could not be persisted");
    }
    if (inserted && interruptCheckpoint !== null) {
      const currentRow = sourceRow(db, input)!;
      const effects = persistInterruptIntent(db, input, asNumber(currentRow.id));
      const persistedCheckpoint: IngressOperationCheckpoint = {
        ...interruptCheckpoint,
        ...effects,
      };
      const saved = run(
        db,
        "UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ? WHERE id = ? AND state = ?",
        [serialize(persistedCheckpoint), Date.now(), asNumber(currentRow.id), input.state],
      ).changes === 1;
      if (!saved) throw new Error("interrupt intent could not be persisted");
    }
    const row = sourceRow(db, input)!;
    return { job: ingressJobFromRow(row), created: inserted };
  });
}

const acceptAndHandoffRollback = Symbol("ingress-accept-and-handoff-not-committed");

export function acceptAndHandoffIngressJob(
  db: Database,
  input: IngressJobInput,
  consumer: IngressConsumer,
  persistConsumer: () => boolean,
): boolean {
  try {
    return transaction(db, () => {
      const accepted = acceptIngressJob(db, input);
      if (accepted.job.state === "handed-off") return true;
      if (accepted.job.state !== "accepted" && accepted.job.state !== "preparing") return false;
      const handedOff = handoffIngressJob(db, accepted.job.id, consumer, persistConsumer);
      if (!handedOff && accepted.created) throw acceptAndHandoffRollback;
      return handedOff;
    });
  } catch (error) {
    if (error === acceptAndHandoffRollback) return false;
    throw error;
  }
}

export function getIngressJob(db: Database, id: number): IngressJobRecord | null {
  const row = jobRow(db, id);
  return row ? ingressJobFromRow(row) : null;
}


export function getIngressJobBySource(db: Database, key: IngressSourceKey): IngressJobRecord | null {
  const row = sourceRow(db, key);
  return row ? ingressJobFromRow(row) : null;
}
export function ensureBindingIfMatchesIfNoUnresolvedSessionTransition(
  db: Database,
  binding: BindingRecord,
  expected: BindingRecord | null,
  options: { clearSessionInitialization?: boolean } = {},
): boolean {
  return transaction(db, () => {
    const candidateRows = query(
      db,
      `SELECT operation_checkpoint_json FROM ingress_jobs
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
         AND state IN ('accepted', 'preparing', 'dispatch-intent', 'unknown')`,
      [binding.botUid, binding.channelType, binding.channelId],
    );
    for (const row of candidateRows) {
      const checkpoint = parseJson<IngressOperationCheckpoint>(
        asNullableString(row.operation_checkpoint_json),
      );
      if (
        checkpoint?.kind === "command"
        && checkpoint.recoveredByIngressJobId === undefined
        && (checkpoint.action === "new" || checkpoint.action === "move" || checkpoint.action === "reset")
      ) return false;
    }
    return ensureBindingIfMatches(db, binding, expected, options);
  });
}

export function listRecoverableIngressJobs(db: Database, botUid: string): IngressJobRecord[] {
  return query(
    db,
    `SELECT * FROM ingress_jobs
     WHERE bot_uid = ? AND state IN ('accepted', 'preparing', 'dispatch-intent')
     ORDER BY id ASC`,
    [botUid],
  ).map(ingressJobFromRow);
}

export function listUnknownIngressJobs(db: Database, botUid: string): IngressJobRecord[] {
  return query(
    db,
    "SELECT * FROM ingress_jobs WHERE bot_uid = ? AND state = 'unknown' ORDER BY id ASC",
    [botUid],
  ).map(ingressJobFromRow);
}

export function countPendingIngressJobs(db: Database, botUid: string): number {
  const row = query(
    db,
    `SELECT COUNT(*) AS count FROM ingress_jobs
     WHERE bot_uid = ? AND state IN ('accepted', 'preparing', 'dispatch-intent')`,
    [botUid],
  )[0];
  return asNumber(row?.count);
}

export function markIngressPrepared(
  db: Database,
  id: number,
  preparedText: string,
  spoolPaths: readonly string[],
): boolean {
  return transaction(db, () => {
    const row = jobRow(db, id);
    if (row === undefined || asString(row.state) !== "preparing" || asNumber(row.ack_ready) !== 0) return false;
    const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
    const recoveryCancelled = checkpoint?.kind === "session-recovery-cancellation";
    const consumerId = recoveryCancelled ? recoveryNoticeId(id) : null;
    return run(db,
      `UPDATE ingress_jobs
       SET prepared_text = ?, spool_paths_json = ?, ack_ready = 1, state = ?,
           consumer_kind = ?, consumer_id = ?, updated_at = ?
       WHERE id = ? AND state = 'preparing' AND ack_ready = 0`,
      [
        preparedText,
        serialize(spoolPaths),
        recoveryCancelled ? "handed-off" : "accepted",
        recoveryCancelled ? "outbox" : null,
        consumerId,
        Date.now(),
        id,
      ],
    ).changes === 1;
  });
}

export function finishIngressRecoveryPreparation(
  db: Database,
  id: number,
  consumer: IngressConsumer,
  persistConsumer?: () => boolean,
): boolean {
  return transaction(db, () => {
    const row = jobRow(db, id);
    const checkpoint = row === undefined
      ? null
      : parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
    if (
      row === undefined
      || asString(row.state) !== "preparing"
      || asNumber(row.ack_ready) !== 0
      || checkpoint?.kind !== "session-recovery-cancellation"
      || (persistConsumer !== undefined && !persistConsumer())
    ) return false;
    return run(db,
      `UPDATE ingress_jobs
       SET ack_ready = 1, state = 'handed-off', consumer_kind = ?, consumer_id = ?, updated_at = ?
       WHERE id = ? AND state = 'preparing' AND ack_ready = 0`,
      [consumer.kind, consumer.id, Date.now(), id],
    ).changes === 1;
  });
}

export function saveIngressCheckpoint(
  db: Database,
  id: number,
  checkpoint: IngressOperationCheckpoint,
): boolean {
  return run(db,
    `UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ?
     WHERE id = ? AND state IN ('accepted', 'preparing') AND ack_ready = 1`,
    [serialize(checkpoint), Date.now(), id],
  ).changes === 1;
}

export function markIngressDispatchIntent(db: Database, id: number): boolean {
  return run(db,
    `UPDATE ingress_jobs SET state = 'dispatch-intent', updated_at = ?
     WHERE id = ? AND state = 'accepted' AND ack_ready = 1 AND payload_json IS NOT NULL`,
    [Date.now(), id],
  ).changes === 1;
}
export function markIngressCommandDispatchIntent(
  db: Database,
  id: number,
  checkpoint: IngressOperationCheckpoint,
): boolean {
  if (checkpoint.kind !== "command" || checkpoint.phase !== "dispatch-intent") return false;
  return run(db,
    `UPDATE ingress_jobs
     SET state = 'dispatch-intent', operation_checkpoint_json = ?, updated_at = ?
     WHERE id = ? AND state = 'accepted' AND ack_ready = 1 AND payload_json IS NOT NULL`,
    [serialize(checkpoint), Date.now(), id],
  ).changes === 1;
}

/** Atomically binds a spawned session worker to the dispatch-intent that authorized its exact paths. */
export function recordIngressWorker(
  db: Database,
  id: number,
  worker: WorkerLedgerRecord,
): boolean {
  return transaction(db, () => {
    const row = jobRow(db, id);
    if (row === undefined) return false;
    const job = ingressJobFromRow(row);
    const checkpoint = job.operationCheckpoint;
    if (
      !job.ackReady
      || job.state !== "dispatch-intent"
      || checkpoint?.kind !== "command"
      || (checkpoint.action !== "new" && checkpoint.action !== "move")
      || checkpoint.workerLedgerId !== worker.id
      || worker.id !== `${checkpoint.action}-ingress-${job.id}`
      || job.botUid !== worker.botUid
    ) return false;
    const resources = checkpoint.resourceCwds ?? [];
    if (
      !resources.includes(worker.cwd)
      || (worker.moveTargetCwd !== null && !resources.includes(worker.moveTargetCwd))
    ) return false;
    if (checkpoint.action === "new" && worker.moveTargetCwd !== null) return false;
    if (checkpoint.action === "move") {
      const expected = checkpoint.expectedBinding;
      if (
        expected === null
        || expected.cwd !== worker.cwd
        || expected.sessionId !== worker.sessionId
        || expected.sessionFile !== worker.sessionFile
        || worker.moveTargetCwd === null
      ) return false;
    }
    recordWorker(db, worker);
    return true;
  });
}

export function markIngressConfigActionDispatchIntent(db: Database, id: number): boolean {
  return run(db,
    `UPDATE ingress_jobs
     SET state = 'dispatch-intent', operation_checkpoint_json = ?, updated_at = ?
     WHERE id = ? AND state = 'accepted' AND ack_ready = 1 AND operation_checkpoint_json = ?`,
    [
      serialize({ kind: "config-action", phase: "dispatch-intent" }),
      Date.now(),
      id,
      serialize({ kind: "config-action", phase: "accepted" }),
    ],
  ).changes === 1;
}

export function markIngressConfigActionApplied(db: Database, id: number, appliedResult: string): boolean {
  return run(db,
    `UPDATE ingress_jobs
     SET state = 'accepted', operation_checkpoint_json = ?, updated_at = ?
     WHERE id = ? AND state = 'dispatch-intent' AND operation_checkpoint_json = ?`,
    [
      serialize({ kind: "config-action", phase: "applied", appliedResult }),
      Date.now(),
      id,
      serialize({ kind: "config-action", phase: "dispatch-intent" }),
    ],
  ).changes === 1;
}

export function markIngressUnknown(
  db: Database,
  id: number,
  persistUnknownOutcome?: () => boolean,
): boolean {
  return transaction(db, () => {
    const changed = run(db,
      `UPDATE ingress_jobs SET state = 'unknown', updated_at = ?
       WHERE id = ? AND ack_ready = 1 AND state IN ('accepted', 'dispatch-intent')`,
      [Date.now(), id],
    ).changes === 1;
    if (!changed) return false;
    if (persistUnknownOutcome !== undefined && !persistUnknownOutcome()) {
      throw new Error("unknown ingress outcome could not be persisted");
    }
    return true;
  });
}
export function isIngressCancellationRequested(db: Database, id: number): boolean {
  const row = jobRow(db, id);
  if (row === undefined || asString(row.state) !== "dispatch-intent") return false;
  const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
  return checkpoint?.kind === "cancellation-request"
    ? checkpoint.cancelRequestedByIngressJobIds.length > 0
    : checkpoint?.kind === "command"
      && (checkpoint.cancelRequestedByIngressJobIds?.length ?? 0) > 0;
}

export function cancelIngressJobAfterCancellationRequest(db: Database, id: number): boolean {
  return transaction(db, () => {
    const row = jobRow(db, id);
    if (row === undefined || asString(row.state) !== "dispatch-intent") return false;
    const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
    const hasRequest = checkpoint?.kind === "cancellation-request"
      ? checkpoint.cancelRequestedByIngressJobIds.length > 0
      : checkpoint?.kind === "command"
        && (checkpoint.cancelRequestedByIngressJobIds?.length ?? 0) > 0;
    if (!hasRequest) return false;
    const changed = run(
      db,
      `UPDATE ingress_jobs
       SET payload_json = NULL, route_snapshot_json = NULL, prepared_text = NULL,
           spool_paths_json = '[]', ack_ready = 1, state = 'cancelled',
           operation_checkpoint_json = NULL, updated_at = ?
       WHERE id = ? AND state = 'dispatch-intent'`,
      [Date.now(), id],
    ).changes === 1;
    if (!changed) return false;
    const messageId = asNullableString(row.message_id);
    if (
      messageId !== null
      && checkpoint?.kind === "command"
      && asString(row.source_kind) !== "bot-event"
    ) {
      run(
        db,
        `DELETE FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
        [asString(row.bot_uid), asNumber(row.channel_type), asString(row.channel_id), messageId],
      );
    }
    return true;
  });
}

function cancelUndispatchedRow(db: Database, id: number): boolean {
  const row = jobRow(db, id);
  if (row === undefined) return false;
  const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
  if (checkpoint?.kind === "config-action") return false;
  return run(db,
    `UPDATE ingress_jobs
     SET payload_json = NULL, route_snapshot_json = NULL, prepared_text = NULL,
         spool_paths_json = '[]', ack_ready = 1, state = 'cancelled',
         operation_checkpoint_json = NULL, updated_at = ?
     WHERE id = ? AND state IN ('accepted', 'preparing')`,
    [Date.now(), id],
  ).changes === 1;
}

export function cancelIngressJob(db: Database, id: number): boolean {
  return transaction(db, () => cancelUndispatchedRow(db, id));
}

export function cancelIngressJobBySource(
  db: Database,
  key: IngressSourceKey,
  attribution: ChatTarget & { messageId: string },
  cancellationJobId?: number,
): boolean {
  return transaction(db, () => {
    const existing = sourceRow(db, key);
    let cancelledJobId: number;
    if (existing !== undefined) {
      cancelledJobId = asNumber(existing.id);
      if (
        asNumber(existing.channel_type) !== attribution.channelType
        || asString(existing.channel_id) !== attribution.channelId
        || asNullableString(existing.message_id) !== attribution.messageId
        || asString(existing.lane) !== "ordered"
        || !cancelUndispatchedRow(db, cancelledJobId)
      ) return false;
    } else {
      const now = Date.now();
      const scheduleOrder = allocateScheduleOrder(db);
      const inserted = run(db,
        `INSERT INTO ingress_jobs(
           bot_uid, source_kind, source_id, channel_type, channel_id, message_id,
           payload_json, route_snapshot_json, prepared_text, spool_paths_json,
           ack_ready, lane, state, operation_checkpoint_json, schedule_order, created_at, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, NULL, NULL, NULL, '[]', 1, 'control', 'cancelled', NULL, ?, ?, ?)
         ON CONFLICT(bot_uid, source_kind, source_id) DO NOTHING`,
        [
          key.botUid,
          key.sourceKind,
          key.sourceId,
          attribution.channelType,
          attribution.channelId,
          attribution.messageId,
          scheduleOrder,
          now,
          now,
        ],
      ).changes === 1;
      if (!inserted) return false;
      const tombstone = sourceRow(db, key);
      if (tombstone === undefined) throw new Error("cancelled ingress tombstone disappeared");
      cancelledJobId = asNumber(tombstone.id);
    }

    if (cancellationJobId !== undefined) {
      const cancellerRow = jobRow(db, cancellationJobId);
      if (cancellerRow === undefined) throw new Error("cancellation ingress job disappeared");
      const checkpoint = parseJson<IngressOperationCheckpoint>(
        asNullableString(cancellerRow.operation_checkpoint_json),
      );
      const state = asString(cancellerRow.state);
      if (checkpoint?.kind !== "command" || (state !== "accepted" && state !== "dispatch-intent")) {
        throw new Error("cancellation ingress intent is not recoverable");
      }
      let persistedCheckpoint: IngressOperationCheckpoint;
      if (checkpoint.action === "cancel" && checkpoint.phase === "cancel-intent") {
        persistedCheckpoint = {
          ...checkpoint,
          cancelledIngressJobIds: [...new Set([
            ...(checkpoint.cancelledIngressJobIds ?? []),
            cancelledJobId,
          ])],
        };
      } else if (
        checkpoint.action === "commit"
        && checkpoint.argument.trim() === "stop"
        && checkpoint.phase === "commit-stop-intent"
      ) {
        persistedCheckpoint = {
          ...checkpoint,
          cancelledCommitIngressJobIds: [...new Set([
            ...(checkpoint.cancelledCommitIngressJobIds ?? []),
            cancelledJobId,
          ])],
        };
      } else {
        throw new Error("ingress job does not own a cancellation intent");
      }
      const saved = run(
        db,
        "UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ? WHERE id = ? AND state = ?",
        [serialize(persistedCheckpoint), Date.now(), cancellationJobId, state],
      ).changes === 1;
      if (!saved) throw new Error("cancelled ingress identity could not be checkpointed");
    }
    return true;
  });
}

function bindingMatchesSnapshot(
  binding: BindingRecord | null,
  botUid: string,
  target: ChatTarget,
  snapshot: IngressBindingSnapshot | null,
): boolean {
  if (binding === null || snapshot === null) return binding === null && snapshot === null;
  return binding.botUid === botUid
    && binding.channelType === target.channelType
    && binding.channelId === target.channelId
    && binding.sessionId === snapshot.sessionId
    && binding.cwd === snapshot.cwd
    && binding.sessionFile === snapshot.sessionFile
    && (binding.cwdPinned === true) === (snapshot.cwdPinned === true)
    && (binding.sessionSwitchedAt ?? null) === (snapshot.sessionSwitchedAt ?? null)
    && JSON.stringify(binding.sessionInitialization ?? null)
      === JSON.stringify(snapshot.sessionInitialization ?? null);
}

function unknownTransitionIsStopped(db: Database, botUid: string, id: number): boolean {
  const row = jobRow(db, id);
  if (row === undefined || asString(row.state) !== "unknown" || asString(row.bot_uid) !== botUid) return false;
  const checkpoint = parseJson<IngressOperationCheckpoint>(asNullableString(row.operation_checkpoint_json));
  if (
    checkpoint?.kind !== "command"
    || checkpoint.recoveredByIngressJobId !== undefined
    || (checkpoint.action !== "new" && checkpoint.action !== "move" && checkpoint.action !== "reset")
  ) return false;
  const ledgerId = `${checkpoint.action}-ingress-${id}`;
  if (checkpoint.workerLedgerId !== ledgerId) return false;
  const ledger = query(db, "SELECT state FROM worker_ledger WHERE bot_uid = ? AND id = ?", [botUid, ledgerId])[0];
  return ledger !== undefined && asString(ledger.state) === "stopped";
}

function validateRecoveryBarrierChain(
  db: Database,
  job: IngressJobRecord,
  barrierJobIds: readonly number[],
): boolean {
  const checkpoint = job.operationCheckpoint;
  if (checkpoint?.kind !== "command" || (checkpoint.action !== "new" && checkpoint.action !== "reset")) {
    return false;
  }
  const unique = [...new Set(barrierJobIds)];
  if (unique.length !== barrierJobIds.length) return false;
  const expected = checkpoint.recoveryBarrierJobIds;
  if (
    expected !== undefined
    && (expected.length !== unique.length || expected.some((id, index) => id !== unique[index]))
  ) return false;
  const recoveredIds = new Set<number>();
  const visited = new Set<number>();
  let valid = true;
  const visit = (id: number): void => {
    if (!valid || visited.has(id)) return;
    visited.add(id);
    const row = jobRow(db, id);
    if (row === undefined) {
      valid = false;
      return;
    }
    const barrier = ingressJobFromRow(row);
    const barrierCheckpoint = barrier.operationCheckpoint;
    if (
      barrier.botUid !== job.botUid
      || barrier.channelType !== job.channelType
      || barrier.channelId !== job.channelId
    ) {
      valid = false;
      return;
    }
    if (barrier.state === "handed-off" || barrier.state === "cancelled") return;
    if (
      barrierCheckpoint?.kind !== "command"
      || barrierCheckpoint.recoveredByIngressJobId !== undefined
    ) {
      valid = false;
      return;
    }
    const sessionTransition = barrierCheckpoint.action === "new"
      || barrierCheckpoint.action === "move"
      || barrierCheckpoint.action === "reset";
    const undispatchedFork = barrierCheckpoint.action === "fork"
      && barrier.state === "accepted"
      && barrierCheckpoint.phase === "barrier-installed";
    if (!sessionTransition && !undispatchedFork) {
      valid = false;
      return;
    }
    if (barrier.state === "unknown") {
      if (!sessionTransition) {
        valid = false;
        return;
      }
      recoveredIds.add(id);
    } else if (barrier.state !== "accepted") {
      valid = false;
      return;
    }
    for (const ancestorId of barrierCheckpoint.recoveryBarrierJobIds ?? []) visit(ancestorId);
    const parentId = barrier.routeSnapshot?.barrierJobId;
    if (parentId !== null && parentId !== undefined) visit(parentId);
  };
  for (const barrierId of checkpoint.recoveryBarrierJobIds ?? []) visit(barrierId);
  const routeBarrierId = job.routeSnapshot?.barrierJobId;
  if (routeBarrierId !== null && routeBarrierId !== undefined) visit(routeBarrierId);
  const actual = [...recoveredIds].sort((left, right) => left - right);
  if (
    !valid
    || actual.length !== unique.length
    || actual.some((id, index) => id !== unique[index])
  ) return false;
  for (const barrierJobId of unique) {
    if (!unknownTransitionIsStopped(db, job.botUid, barrierJobId)) return false;
  }
  return true;
}

function recoveryNoticeId(jobId: number): string {
  return `ingress-session-recovery:${jobId}:not-executed`;
}

function recoveryNotice(db: Database, row: SqlRow): void {
  const messageId = asNullableString(row.message_id);
  const jobId = asNumber(row.id);
  const id = recoveryNoticeId(jobId);
  if (query(db, "SELECT 1 FROM outbox WHERE id = ?", [id])[0] !== undefined) return;
  const notice: OutboxInsert = {
    id,
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
    channelId: asString(row.channel_id),
    taskId: null,
    kind: "text",
    text: messageId === null
      ? `待处理消息（入站项 #${jobId}）未执行：此前的会话切换结果未知；如仍需处理，请重新发送，否则可忽略。`
      : `消息 ${messageId} 未执行：此前的会话切换结果未知；如仍需处理，请重新发送，否则可忽略。`,
    path: null,
    fileKind: null,
    clientMsgNo: id,
  };
  enqueueOutbox(db, notice);
}
function recoveryLineageRoot(
  db: Database,
  id: number,
  botUid: string,
  target: ChatTarget,
  recoveryBarrierIds: ReadonlySet<number>,
  rowsById: ReadonlyMap<number, SqlRow>,
  visited: Set<number> = new Set(),
): number | null {
  if (recoveryBarrierIds.has(id)) return id;
  if (visited.has(id)) return null;
  visited.add(id);
  const row = rowsById.get(id) ?? jobRow(db, id);
  if (
    row === undefined
    || asString(row.bot_uid) !== botUid
    || asNumber(row.channel_type) !== target.channelType
    || asString(row.channel_id) !== target.channelId
  ) return null;
  const job = ingressJobFromRow(row);
  const parents = [
    ...(job.operationCheckpoint?.kind === "command"
      ? job.operationCheckpoint.recoveryBarrierJobIds ?? []
      : []),
    ...(job.routeSnapshot?.barrierJobId == null ? [] : [job.routeSnapshot.barrierJobId]),
  ];
  for (const parent of parents) {
    const root = recoveryLineageRoot(db, parent, botUid, target, recoveryBarrierIds, rowsById, visited);
    if (root !== null) return root;
  }
  return null;
}

function recoveryAffectedIngressRows(
  db: Database,
  rows: readonly SqlRow[],
  botUid: string,
  target: ChatTarget,
  barrierJobIds: readonly number[],
): Array<{ row: SqlRow; barrierJobId: number }> {
  const roots = new Set(barrierJobIds);
  const rowsById = new Map(rows.map(row => [asNumber(row.id), row]));
  const affected: Array<{ row: SqlRow; barrierJobId: number }> = [];
  for (const row of rows) {
    if (roots.has(asNumber(row.id))) continue;
    const route = parseJson<IngressRouteSnapshot>(asNullableString(row.route_snapshot_json));
    if (route?.purpose.kind === "questions") continue;
    const barrierJobId = route?.barrierJobId;
    if (
      barrierJobId === null
      || barrierJobId === undefined
      || recoveryLineageRoot(db, barrierJobId, botUid, target, roots, rowsById) === null
    ) continue;
    affected.push({ row, barrierJobId });
  }
  return affected;
}


function cancelIngressForSessionRecovery(
  db: Database,
  row: SqlRow,
  recoveryJobId: number,
  barrierJobId: number,
): boolean {
  const state = asString(row.state);
  const preparing = state === "preparing";
  const ackReady = asNumber(row.ack_ready) === 1;
  if ((state !== "accepted" && !preparing) || preparing === ackReady) return false;
  const id = asNumber(row.id);
  const checkpoint: IngressOperationCheckpoint = {
    kind: "session-recovery-cancellation",
    barrierJobId,
    recoveryJobId,
  };
  const changed = run(
    db,
    `UPDATE ingress_jobs
     SET ack_ready = ?, state = ?, operation_checkpoint_json = ?, consumer_kind = ?, consumer_id = ?, updated_at = ?
     WHERE id = ? AND state = ? AND ack_ready = ?`,
    [
      ackReady ? 1 : 0,
      preparing ? "preparing" : "handed-off",
      serialize(checkpoint),
      preparing ? null : "outbox",
      preparing ? null : recoveryNoticeId(id),
      Date.now(),
      id,
      state,
      ackReady ? 1 : 0,
    ],
  ).changes === 1;
  if (changed) recoveryNotice(db, row);
  return changed;
}
export function cancelIngressJobAfterSessionRecovery(
  db: Database,
  key: IngressSourceKey,
  target: ChatTarget,
  messageId: string,
  barrierJobId: number,
): IngressJobRecord | null {
  return transaction(db, () => {
    const barrierRow = jobRow(db, barrierJobId);
    if (
      barrierRow === undefined
      || asString(barrierRow.bot_uid) !== key.botUid
      || asNumber(barrierRow.channel_type) !== target.channelType
      || asString(barrierRow.channel_id) !== target.channelId
      || asString(barrierRow.state) !== "unknown"
    ) return null;
    const barrierCheckpoint = parseJson<IngressOperationCheckpoint>(
      asNullableString(barrierRow.operation_checkpoint_json),
    );
    if (
      barrierCheckpoint?.kind !== "command"
      || (barrierCheckpoint.action !== "new"
        && barrierCheckpoint.action !== "move"
        && barrierCheckpoint.action !== "reset")
      || barrierCheckpoint.recoveredByIngressJobId === undefined
    ) return null;
    const barrierLedgerId = `${barrierCheckpoint.action}-ingress-${barrierJobId}`;
    const worker = query(
      db,
      "SELECT state FROM worker_ledger WHERE bot_uid = ? AND id = ?",
      [key.botUid, barrierLedgerId],
    )[0];
    if (
      barrierCheckpoint.workerLedgerId !== barrierLedgerId
      || worker === undefined
      || asString(worker.state) !== "stopped"
    ) return null;
    const recoveryRow = jobRow(db, barrierCheckpoint.recoveredByIngressJobId);
    if (
      recoveryRow === undefined
      || asString(recoveryRow.bot_uid) !== key.botUid
      || asNumber(recoveryRow.channel_type) !== target.channelType
      || asString(recoveryRow.channel_id) !== target.channelId
      || asString(recoveryRow.state) !== "handed-off"
    ) return null;

    const existing = sourceRow(db, key);
    if (
      existing === undefined
      || asNumber(existing.channel_type) !== target.channelType
      || asString(existing.channel_id) !== target.channelId
      || asNullableString(existing.message_id) !== messageId
      || asString(existing.lane) !== "ordered"
      || asNumber(existing.schedule_order) >= asNumber(recoveryRow.schedule_order)
    ) return null;
    const route = parseJson<IngressRouteSnapshot>(asNullableString(existing.route_snapshot_json));
    if (route?.purpose.kind === "questions") return null;
    const state = asString(existing.state);
    if (state === "cancelled") return ingressJobFromRow(existing);
    if (
      (state !== "accepted" && state !== "preparing")
      || route?.barrierJobId !== barrierJobId
      || !cancelIngressForSessionRecovery(
        db,
        existing,
        barrierCheckpoint.recoveredByIngressJobId,
        barrierJobId,
      )
    ) return null;
    const cancelled = jobRow(db, asNumber(existing.id));
    return cancelled === undefined ? null : ingressJobFromRow(cancelled);
  });
}

function prepareIngressSessionRecoveryInTransaction(
  db: Database,
  recoveryJobId: number,
  barrierJobIds: readonly number[],
): IngressSessionRecoveryResult {
  const row = jobRow(db, recoveryJobId);
  if (row === undefined) return { kind: "stale", cancelledJobIds: [] };
  const job = ingressJobFromRow(row);
  const checkpoint = job.operationCheckpoint;
  if (
    job.state !== "accepted"
    || checkpoint?.kind !== "command"
    || checkpoint.phase !== "waiting-session"
    || !validateRecoveryBarrierChain(db, job, barrierJobIds)
  ) return { kind: "stale", cancelledJobIds: [] };
  const target: ChatTarget = { channelType: job.channelType, channelId: job.channelId };
  if (!bindingMatchesSnapshot(
    getBinding(db, job.botUid, target),
    job.botUid,
    target,
    checkpoint.expectedBinding,
  )) return { kind: "stale", cancelledJobIds: [] };

  const lowerBound = Math.min(
    job.scheduleOrder,
    ...barrierJobIds.map(barrierId => asNumber(jobRow(db, barrierId)?.schedule_order)),
  );
  const rows = query(
    db,
    `SELECT * FROM ingress_jobs
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
       AND schedule_order > ? AND schedule_order < ?
       AND state IN ('accepted', 'preparing', 'dispatch-intent', 'unknown', 'cancelled')
     ORDER BY schedule_order ASC, id ASC`,
    [job.botUid, target.channelType, target.channelId, lowerBound, job.scheduleOrder],
  );
  for (const { row: pending } of recoveryAffectedIngressRows(
    db,
    rows,
    job.botUid,
    target,
    barrierJobIds,
  )) {
    const state = asString(pending.state);
    if (state === "dispatch-intent" || state === "unknown") {
      return { kind: "blocked", cancelledJobIds: [] };
    }
  }

  const recoveryCheckpoint: IngressOperationCheckpoint = {
    ...checkpoint,
    recoveryBarrierJobIds: [...barrierJobIds],
  };
  if (run(
    db,
    `UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ?
     WHERE id = ? AND state = 'accepted'`,
    [serialize(recoveryCheckpoint), Date.now(), recoveryJobId],
  ).changes !== 1) return { kind: "stale", cancelledJobIds: [] };
  return { kind: "ready", cancelledJobIds: [] };
}

export function prepareIngressSessionRecovery(
  db: Database,
  recoveryJobId: number,
  barrierJobIds: readonly number[],
): IngressSessionRecoveryResult {
  return transaction(db, () => prepareIngressSessionRecoveryInTransaction(db, recoveryJobId, barrierJobIds));
}

function commitIngressSessionHandoff(
  db: Database,
  recoveryJobId: number,
  handoff: IngressSessionHandoff,
  cancelledJobIds: Set<number>,
): boolean {
  const row = jobRow(db, recoveryJobId);
  if (row === undefined) return false;
  const job = ingressJobFromRow(row);
  if (job.state !== "dispatch-intent") return false;
  const checkpoint = job.operationCheckpoint;
  if (
    checkpoint?.kind !== "command"
    || (checkpoint.action !== "new" && checkpoint.action !== "reset")
    || !validateRecoveryBarrierChain(db, job, handoff.barrierJobIds)
  ) return false;
  const currentLedgerId = `${checkpoint.action}-ingress-${recoveryJobId}`;
  if (checkpoint.workerLedgerId !== currentLedgerId) return false;
  const currentLedger = query(
    db,
    "SELECT state FROM worker_ledger WHERE bot_uid = ? AND id = ?",
    [job.botUid, currentLedgerId],
  )[0];
  if (currentLedger === undefined || asString(currentLedger.state) !== "stopped") return false;
  const target: ChatTarget = { channelType: job.channelType, channelId: job.channelId };
  if (!bindingMatchesSnapshot(
    getBinding(db, job.botUid, target),
    job.botUid,
    target,
    handoff.expectedBinding,
  )) return false;
  const expectedRecord: BindingRecord | null = handoff.expectedBinding === null
    ? null
    : {
        botUid: job.botUid,
        ...target,
        sessionId: handoff.expectedBinding.sessionId,
        cwd: handoff.expectedBinding.cwd,
        sessionFile: handoff.expectedBinding.sessionFile,
        cwdPinned: handoff.expectedBinding.cwdPinned === true,
        sessionSwitchedAt: handoff.expectedBinding.sessionSwitchedAt ?? null,
        ...(handoff.expectedBinding.sessionInitialization === undefined
          ? {}
          : { sessionInitialization: handoff.expectedBinding.sessionInitialization }),
      };
  if (
    handoff.binding.botUid !== job.botUid
    || handoff.binding.channelType !== target.channelType
    || handoff.binding.channelId !== target.channelId
    || !ensureBindingIfMatches(db, handoff.binding, expectedRecord, { clearSessionInitialization: true })
  ) return false;

  if (handoff.barrierJobIds.length > 0) {
    const lowerBound = Math.min(
      job.scheduleOrder,
      ...handoff.barrierJobIds.map(barrierId => asNumber(jobRow(db, barrierId)?.schedule_order)),
    );
    const pendingRows = query(
      db,
      `SELECT * FROM ingress_jobs
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
         AND schedule_order > ? AND schedule_order < ?
         AND state IN ('accepted', 'preparing', 'dispatch-intent', 'unknown', 'cancelled')
       ORDER BY schedule_order ASC, id ASC`,
      [job.botUid, target.channelType, target.channelId, lowerBound, job.scheduleOrder],
    );
    for (const { row: pending, barrierJobId } of recoveryAffectedIngressRows(
      db,
      pendingRows,
      job.botUid,
      target,
      handoff.barrierJobIds,
    )) {
      const id = asNumber(pending.id);
      const state = asString(pending.state);
      if (state === "dispatch-intent" || state === "unknown") return false;
      if (state === "accepted" || state === "preparing") {
        if (cancelIngressForSessionRecovery(db, pending, recoveryJobId, barrierJobId)) {
          cancelledJobIds.add(id);
        } else {
          return false;
        }
      }
    }
    const recoveredBindingSnapshot: IngressBindingSnapshot = {
      sessionId: handoff.binding.sessionId,
      cwd: handoff.binding.cwd,
      sessionFile: handoff.binding.sessionFile,
      cwdPinned: handoff.binding.cwdPinned === true,
      sessionSwitchedAt: handoff.binding.sessionSwitchedAt ?? null,
      sessionInitialization: handoff.binding.sessionInitialization ?? null,
    };
    for (const barrierJobId of handoff.barrierJobIds) {
      const barrierRow = jobRow(db, barrierJobId);
      if (barrierRow === undefined) return false;
      const barrier = ingressJobFromRow(barrierRow);
      if (barrier.operationCheckpoint?.kind !== "command") return false;
      const nextCheckpoint: IngressOperationCheckpoint = {
        ...barrier.operationCheckpoint,
        recoveredByIngressJobId: recoveryJobId,
        recoveredBindingSnapshot,
      };
      if (run(
        db,
        `UPDATE ingress_jobs SET operation_checkpoint_json = ?, updated_at = ?
         WHERE id = ? AND state = 'unknown'`,
        [serialize(nextCheckpoint), Date.now(), barrierJobId],
      ).changes !== 1) return false;
    }
  }
  return true;
}

export function handoffSessionIngressJob(
  db: Database,
  id: number,
  consumer: IngressConsumer,
  persistConsumer: () => boolean,
  handoff: IngressSessionHandoff,
): { handedOff: boolean; cancelledJobIds: readonly number[] } {
  const cancelledJobIds = new Set<number>();
  const binding = handoff.binding;
  const outcomeCheckpoint: IngressOperationCheckpoint = {
    kind: "session-handoff-outcome",
    action: "new",
    binding: {
      sessionId: binding.sessionId,
      cwd: binding.cwd,
      sessionFile: binding.sessionFile,
      cwdPinned: binding.cwdPinned === true,
      sessionSwitchedAt: binding.sessionSwitchedAt ?? null,
      sessionInitialization: binding.sessionInitialization ?? null,
    },
  };
  const handedOff = handoffIngressJob(
    db,
    id,
    consumer,
    () => commitIngressSessionHandoff(db, id, handoff, cancelledJobIds) && persistConsumer(),
    outcomeCheckpoint,
  );
  return { handedOff, cancelledJobIds: [...cancelledJobIds] };
}
const consumerNotPersisted = Symbol("consumer-not-persisted");

export function handoffIngressJob(
  db: Database,
  id: number,
  consumer: IngressConsumer,
  persistConsumer: () => boolean,
  preservedCheckpoint?: IngressOperationCheckpoint,
): boolean {
  try {
    return transaction(db, () => {
      if (!persistConsumer()) throw consumerNotPersisted;
      const committed = run(db,
        `UPDATE ingress_jobs
         SET state = 'handed-off', consumer_kind = ?, consumer_id = ?, ack_ready = 1,
             payload_json = NULL,
             route_snapshot_json = CASE WHEN ? = 'task' THEN route_snapshot_json ELSE NULL END,
             prepared_text = NULL, spool_paths_json = '[]', operation_checkpoint_json = ?,
             updated_at = ?
         WHERE id = ? AND state IN ('accepted', 'preparing', 'dispatch-intent')`,
        [
          consumer.kind,
          consumer.id,
          consumer.kind,
          preservedCheckpoint === undefined ? null : serialize(preservedCheckpoint),
          Date.now(),
          id,
        ],
      ).changes === 1;
      if (!committed) throw consumerNotPersisted;
      return true;
    });
  } catch (error) {
    if (error === consumerNotPersisted) return false;
    throw error;
  }
}
