import { Database } from "bun:sqlite";
import type { InboundClaim, OutboxInsert, OutboxRecord, TanOperation, TanOperationPhase, TaskRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, transaction, type SqlRow } from "./store-schema.ts";
import { enqueueOutbox, getOutbox } from "./store-outbox.ts";
import { acceptTaskInTransaction } from "./store-tasks.ts";

function tanOperationFromRow(row: SqlRow): TanOperation {
  const phase = asString(row.phase);
  if (phase !== "pending" && phase !== "done" && phase !== "failed" && phase !== "cancelled") {
    throw new Error(`invalid TAN operation phase: ${phase}`);
  }
  const channelType = asNumber(row.source_channel_type);
  if (channelType !== 2 && channelType !== 5) {
    throw new Error(`invalid TAN source channel type: ${channelType}`);
  }
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    sourceChannelType: channelType,
    sourceChannelId: asString(row.source_channel_id),
    messageId: asString(row.message_id),
    senderId: asString(row.sender_id),
    text: asString(row.text),
    sourceSessionId: asString(row.source_session_id),
    sourceSessionFile: asString(row.source_session_file),
    cwd: asString(row.cwd),
    phase,
    error: asNullableString(row.error),
  };
}

function assertAcceptedOperationInput(claim: InboundClaim, operation: TanOperation): void {
  if (claim.kind !== "command") throw new Error("TAN acceptance requires a command claim");
  if (claim.target.channelType !== 2 && claim.target.channelType !== 5) {
    throw new Error("TAN source must be a group or thread");
  }
  if (
    operation.botUid !== claim.botUid
    || operation.sourceChannelType !== claim.target.channelType
    || operation.sourceChannelId !== claim.target.channelId
    || operation.messageId !== claim.messageId
    || operation.senderId !== claim.senderId
  ) {
    throw new Error("TAN operation does not match its command claim");
  }
  if (!/^fork-[0-9a-f]{64}$/.test(operation.id)) {
    throw new Error("TAN operation requires its full stable fork operation ID");
  }
  if (operation.phase !== "pending" || operation.error !== null) {
    throw new Error("a newly accepted TAN operation must be pending without an error");
  }
  if (
    operation.sourceSessionId.length === 0
    || operation.sourceSessionFile.length === 0
    || operation.cwd.length === 0
  ) {
    throw new Error("TAN operation requires an exact source session and cwd");
  }
}

/**
 * A process restart may leave the temporary command claim pending while attachment materialization
 * never reached durable TAN acceptance. Reset only that exact pending row, in the same write
 * transaction that proves no accepted operation exists; completed acceptance can never be deleted.
 */
export function reclaimTanCommand(db: Database, claim: InboundClaim): boolean {
  if (claim.kind !== "command" || (claim.target.channelType !== 2 && claim.target.channelType !== 5)) {
    return false;
  }
  return transaction(db, () => {
    const accepted = query(db,
      `SELECT 1 FROM tan_operations
       WHERE bot_uid = ? AND source_channel_type = ? AND source_channel_id = ? AND message_id = ?`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    )[0];
    if (accepted) return false;
    const pending = query(db,
      `SELECT rowid FROM inbound
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
         AND sender_id = ? AND text = ? AND supported = ?
         AND kind = 'command' AND command_state = 'pending'`,
      [
        claim.botUid,
        claim.target.channelType,
        claim.target.channelId,
        claim.messageId,
        claim.senderId,
        claim.text,
        claim.supported ? 1 : 0,
      ],
    );
    if (pending.length !== 1) return false;
    const deleted = run(db,
      "DELETE FROM inbound WHERE rowid = ? AND kind = 'command' AND command_state = 'pending'",
      [asNumber(pending[0]?.rowid)],
    ).changes;
    if (deleted !== 1) throw new Error("TAN command claim changed during retry reset");
    return true;
  });
}

/** Atomically persists the immutable source snapshot and completes its temporary command claim. */
export function acceptTanOperation(db: Database, claim: InboundClaim, operation: TanOperation): boolean {
  assertAcceptedOperationInput(claim, operation);
  return transaction(db, () => {
    const duplicate = query(db,
      `SELECT 1 FROM tan_operations
       WHERE (bot_uid = ? AND id = ?)
          OR (bot_uid = ? AND source_channel_type = ? AND source_channel_id = ? AND message_id = ?)`,
      [
        operation.botUid,
        operation.id,
        operation.botUid,
        operation.sourceChannelType,
        operation.sourceChannelId,
        operation.messageId,
      ],
    )[0];
    if (duplicate) return false;

    const pendingClaims = query(db,
      `SELECT 1 FROM inbound
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
         AND sender_id = ? AND text = ? AND supported = ?
         AND kind = 'command' AND command_state = 'pending'`,
      [
        claim.botUid,
        claim.target.channelType,
        claim.target.channelId,
        claim.messageId,
        claim.senderId,
        claim.text,
        claim.supported ? 1 : 0,
      ],
    );
    if (pendingClaims.length !== 1) return false;
    const completed = run(db,
      `UPDATE inbound SET command_state = 'completed'
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
         AND sender_id = ? AND text = ? AND supported = ?
         AND kind = 'command' AND command_state = 'pending'`,
      [
        claim.botUid,
        claim.target.channelType,
        claim.target.channelId,
        claim.messageId,
        claim.senderId,
        claim.text,
        claim.supported ? 1 : 0,
      ],
    ).changes;
    if (completed !== 1) throw new Error("TAN command claim changed during durable acceptance");

    const now = Date.now();
    run(db,
      `INSERT INTO tan_operations(
         bot_uid, id, source_channel_type, source_channel_id, message_id, sender_id, text,
         source_session_id, source_session_file, cwd, phase, error, accepted_at, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, ?)`,
      [
        operation.botUid,
        operation.id,
        operation.sourceChannelType,
        operation.sourceChannelId,
        operation.messageId,
        operation.senderId,
        operation.text,
        operation.sourceSessionId,
        operation.sourceSessionFile,
        operation.cwd,
        now,
        now,
      ],
    );
    return true;
  });
}

export function getTanOperation(db: Database, botUid: string, id: string): TanOperation | null {
  const row = query(db,
    "SELECT * FROM tan_operations WHERE bot_uid = ? AND id = ?",
    [botUid, id],
  )[0];
  return row ? tanOperationFromRow(row) : null;
}

export function listPendingTanOperations(db: Database, botUid: string): TanOperation[] {
  return query(db,
    `SELECT * FROM tan_operations
     WHERE bot_uid = ? AND phase = 'pending'
     ORDER BY accepted_at ASC, rowid ASC`,
    [botUid],
  ).map(tanOperationFromRow);
}

/** Cancellation is persisted first; these rows still need a result receipt after recovery. */
export function listCancelledTanOperations(db: Database, botUid: string): TanOperation[] {
  return query(db,
    `SELECT * FROM tan_operations
     WHERE bot_uid = ? AND phase = 'cancelled' AND error IS NULL
     ORDER BY accepted_at ASC, rowid ASC`,
    [botUid],
  ).map(tanOperationFromRow);
}

/** Successful task handoff is already durable; only pending TAN rows can transition to done. */
export function finishTanOperation(db: Database, botUid: string, id: string): void {
  run(db,
    `UPDATE tan_operations SET phase = 'done', error = NULL, updated_at = ?
     WHERE bot_uid = ? AND id = ? AND phase = 'pending'`,
    [Date.now(), botUid, id],
  );
}

function assertTanReceiptRecords(
  operation: TanOperation,
  phase: Extract<TanOperationPhase, "failed" | "cancelled">,
  records: readonly OutboxInsert[],
): void {
  if (records.length === 0) throw new Error("TAN terminal receipt requires at least one outbox record");
  const suffix = phase === "failed" ? "failure" : "cancel-result";
  const expectedPrefix = `tan:${operation.id}:${suffix}:`;
  for (const [index, record] of records.entries()) {
    const expectedId = `${expectedPrefix}${index}`;
    if (
      record.id !== expectedId
      || record.clientMsgNo !== expectedId
      || record.botUid !== operation.botUid
      || record.channelType !== operation.sourceChannelType
      || record.channelId !== operation.sourceChannelId
      || record.taskId !== null
      || record.kind !== "text"
      || record.text === null
      || record.text.length === 0
      || record.path !== null
      || record.fileKind !== null
      || record.segmentIndex !== undefined && record.segmentIndex !== 0
      || record.desiredVersion !== undefined && record.desiredVersion !== 0
      || record.ackedVersion !== undefined && record.ackedVersion !== 0
      || record.mention != null
      || record.streamFinal === true
      || record.createPayload != null
        && (record.createPayload.text !== record.text || record.createPayload.mention !== null)
    ) {
      throw new Error("TAN terminal receipt outbox identity is invalid");
    }
  }
}

function existingTanReceiptMatches(existing: OutboxRecord, record: OutboxInsert): boolean {
  const mention = record.mention ?? null;
  const createPayload = record.createPayload ?? { text: record.text ?? "", mention };
  return existing.botUid === record.botUid
    && existing.channelType === record.channelType
    && existing.channelId === record.channelId
    && existing.taskId === null
    && existing.kind === "text"
    && existing.text === record.text
    && existing.path === null
    && existing.fileKind === null
    && existing.clientMsgNo === record.clientMsgNo
    && JSON.stringify(existing.mention) === JSON.stringify(mention)
    && JSON.stringify(existing.createPayload) === JSON.stringify(createPayload);
}

/**
 * Finalizes a failure, or completes a previously persisted cancel request, atomically with its
 * stable source-chat receipt chunks. Replays never rewrite an existing outbox identity or payload.
 */
export function finishTanOperationWithOutbox(
  db: Database,
  botUid: string,
  id: string,
  phase: Extract<TanOperationPhase, "failed" | "cancelled">,
  error: string,
  records: readonly OutboxInsert[],
): boolean {
  if (error.length === 0) throw new Error("TAN terminal result must not be empty");
  return transaction(db, () => {
    const operation = getTanOperation(db, botUid, id);
    if (!operation) return false;
    assertTanReceiptRecords(operation, phase, records);

    const wherePhase = phase === "failed" ? "pending" : "cancelled";
    const requiresUnsetError = phase === "cancelled" ? " AND error IS NULL" : "";
    const written = run(db,
      `UPDATE tan_operations SET phase = ?, error = ?, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND phase = ?${requiresUnsetError}`,
      [phase, error, Date.now(), botUid, id, wherePhase],
    ).changes;
    if (written !== 1) return false;

    for (const record of records) {
      const existing = getOutbox(db, record.id);
      if (existing) {
        if (!existingTanReceiptMatches(existing, record)) {
          throw new Error(`TAN receipt ${record.id} already exists with different content`);
        }
        continue;
      }
      enqueueOutbox(db, record);
    }
    return true;
  });
}

/** Wins only while preparation has not handed the request to the target task. */
export function cancelTanOperation(db: Database, botUid: string, id: string): boolean {
  return transaction(db, () => run(db,
    `UPDATE tan_operations SET phase = 'cancelled', error = NULL, updated_at = ?
     WHERE bot_uid = ? AND id = ? AND phase = 'pending'`,
    [Date.now(), botUid, id],
  ).changes === 1);
}

function taskMatchesOperation(claim: InboundClaim, task: TaskRecord, operation: TanOperation): boolean {
  return claim.kind === "task"
    && claim.target.channelType === 5
    && claim.botUid === operation.botUid
    && claim.messageId === `dispatch:tan:${operation.id}`
    && claim.senderId === operation.senderId
    && claim.text === operation.text
    && task.botUid === claim.botUid
    && task.channelType === claim.target.channelType
    && task.channelId === claim.target.channelId
    && task.messageId === claim.messageId
    && task.senderId === claim.senderId
    && task.text === claim.text
    && task.sessionId.length > 0
    && task.cwd === operation.cwd;
}

/**
 * Serializes source-chat cancellation against normal target task acceptance. The shared
 * in-transaction insertion primitive keeps the inbound row, task row, and TAN terminal phase
 * inside this one BEGIN IMMEDIATE, so they either all commit or all roll back.
 */
export function acceptTanTask(
  db: Database,
  claim: InboundClaim,
  operationId: string,
  task: TaskRecord,
): boolean {
  return transaction(db, () => {
    const operation = getTanOperation(db, claim.botUid, operationId);
    if (!operation || operation.phase !== "pending" || !taskMatchesOperation(claim, task, operation)) {
      return false;
    }
    if (!acceptTaskInTransaction(db, claim, task)) return false;
    const finished = run(db,
      `UPDATE tan_operations SET phase = 'done', error = NULL, updated_at = ?
       WHERE bot_uid = ? AND id = ? AND phase = 'pending'`,
      [Date.now(), claim.botUid, operationId],
    ).changes === 1;
    if (!finished) throw new Error("TAN operation changed while accepting its target task");
    return true;
  });
}
