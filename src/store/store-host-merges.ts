import { Database } from "bun:sqlite";
import type { ChatTarget, WorktreeMergeOutcome, WorktreeRemovalStep } from "../contracts.ts";
import type {
  HostMergeLeaseInput,
  HostMergeLeaseRecord,
  HostRemovalLeaseInput,
  HostRemovalLeaseRecord,
  HostThreadRemovalOutcome,
} from "./store-records.ts";
import { asNullableString, asString, query, run } from "./store-schema.ts";

export type HostMergeLeaseCompletion = "not-spawned";

function nullablePositiveInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : null;
}
function nullableSafeInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function outcome(row: Record<string, unknown>): WorktreeMergeOutcome | null {
  const kind = asString(row.outcome_kind);
  const encoded = asString(row.outcome_json);
  if (kind.length === 0 || encoded.length === 0) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(encoded);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
  const value = parsed as Record<string, unknown>;
  if (kind === "execution-failed" && value.kind === kind) {
    return Number.isSafeInteger(value.exitCode) && typeof value.diagnostic === "string"
      ? { kind, exitCode: value.exitCode as number, diagnostic: value.diagnostic }
      : null;
  }
  if (kind !== "succeeded" || value.kind !== kind) return null;
  const summary = value.summary;
  if (typeof summary !== "object" || summary === null || Array.isArray(summary)) return null;
  const result = summary as Record<string, unknown>;
  if (
    typeof result.branch !== "string"
    || typeof result.target !== "string"
    || typeof result.squashed !== "boolean"
    || typeof result.rebased !== "boolean"
    || typeof result.committed !== "boolean"
    || typeof result.removed !== "boolean"
  ) {
    return null;
  }
  return {
    kind,
    summary: {
      branch: result.branch,
      target: result.target,
      squashed: result.squashed,
      rebased: result.rebased,
      committed: result.committed,
      removed: result.removed,
      ...(typeof result.commit === "string" ? { commit: result.commit } : {}),
    },
  };
}

function phase(value: unknown): HostMergeLeaseRecord["phase"] {
  return value === "starting" || value === "running" || value === "exited" ? value : "unknown";
}

function decodeHostMergeLease(row: Record<string, unknown>): HostMergeLeaseRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: 5,
    channelId: asString(row.channel_id),
    sourceId: asString(row.source_id),
    cwd: asString(row.cwd),
    phase: phase(row.phase),
    pid: nullablePositiveInteger(row.pid),
    pgid: nullablePositiveInteger(row.pgid),
    processIdentity: asNullableString(row.process_identity),
    commandText: asNullableString(row.source_text),
    operatorNote: asNullableString(row.operator_note),
    senderId: asNullableString(row.sender_id),
    messageSeq: nullableSafeInteger(row.message_seq),
    receiptOutboxId: asNullableString(row.receipt_outbox_id),
    outcome: outcome(row),
    createdAt: Number(row.created_at ?? 0),
    updatedAt: Number(row.updated_at ?? 0),
  };
}

function removalOutcome(row: Record<string, unknown>): HostThreadRemovalOutcome | null {
  const encoded = asNullableString(row.removal_outcome_json);
  if (encoded === null) return null;
  try {
    const parsed: unknown = JSON.parse(encoded);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
    const value = parsed as Record<string, unknown>;
    if (
      (value.kind === "complete" || value.kind === "partial")
      && typeof value.receiptText === "string"
    ) {
      return { kind: value.kind, receiptText: value.receiptText };
    }
  } catch {
    return null;
  }
  return null;
}

function removalStep(value: unknown): WorktreeRemovalStep | null {
  return value === "worktree" || value === "branch" ? value : null;
}

function removalResourcePaths(value: unknown): string[] {
  if (typeof value !== "string") return [];
  try {
    const parsed: unknown = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed.filter((path): path is string => typeof path === "string" && path.length > 0)
      : [];
  } catch {
    return [];
  }
}

function decodeHostRemovalLease(row: Record<string, unknown>): HostRemovalLeaseRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: 5,
    channelId: asString(row.channel_id),
    sourceId: asString(row.source_id),
    cwd: asString(row.cwd),
    phase: phase(row.phase),
    pid: nullablePositiveInteger(row.pid),
    pgid: nullablePositiveInteger(row.pgid),
    processIdentity: asNullableString(row.process_identity),
    step: removalStep(row.removal_step),
    resourcePaths: removalResourcePaths(row.removal_resources_json),
    commandText: asNullableString(row.source_text),
    senderId: asNullableString(row.sender_id),
    messageSeq: nullableSafeInteger(row.message_seq),
    receiptOutboxId: asString(row.receipt_outbox_id),
    outcome: removalOutcome(row),
    createdAt: Number(row.created_at ?? 0),
    updatedAt: Number(row.updated_at ?? 0),
  };
}

/** Insert the fail-closed reservation before the worktrunk merge process is spawned. */
export function reserveHostMergeLease(db: Database, input: HostMergeLeaseInput): void {
  if (
    input.target.channelType !== 5
    || input.id.length === 0
    || input.botUid.length === 0
    || input.target.channelId.length === 0
    || input.sourceId.length === 0
    || input.cwd.length === 0
    || (input.messageSeq !== undefined && (!Number.isSafeInteger(input.messageSeq) || input.messageSeq < 0))
  ) {
    throw new Error("invalid host merge lease identity");
  }
  const existing = query(db,
    `SELECT id FROM host_merge_leases
     WHERE operation_kind = 'merge' AND bot_uid = ? AND channel_type = 5 AND channel_id = ? AND source_id = ?
     ORDER BY created_at DESC, rowid DESC LIMIT 1`,
    [input.botUid, input.target.channelId, input.sourceId],
  )[0];
  if (existing) throw new Error("host merge source already has a durable lease");
  const now = Date.now();
  run(db,
    `INSERT INTO host_merge_leases(
       id, bot_uid, channel_type, channel_id, source_id, cwd, phase, pid, pgid, process_identity,
       source_text, operator_note, sender_id, message_seq, receipt_outbox_id, outcome_kind, outcome_json,
       created_at, updated_at
     ) VALUES(?, ?, 5, ?, ?, ?, 'starting', NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
    [
      input.id,
      input.botUid,
      input.target.channelId,
      input.sourceId,
      input.cwd,
      input.commandText ?? null,
      input.operatorNote ?? null,
      input.senderId ?? null,
      input.messageSeq ?? null,
      input.receiptOutboxId ?? null,
      now,
      now,
    ],
  );
}

/** Persist the exact worktrunk process-group identity as soon as spawn succeeds. */
export function recordHostMergeProcess(
  db: Database,
  id: string,
  process: { pid: number; pgid: number; identity: string },
): void {
  if (!Number.isSafeInteger(process.pid) || process.pid <= 0 || !Number.isSafeInteger(process.pgid) || process.pgid <= 0 || process.identity.length === 0) {
    throw new Error("invalid host merge process identity");
  }
  const result = run(db,
    `UPDATE host_merge_leases SET phase = 'running', pid = ?, pgid = ?, process_identity = ?, updated_at = ?
     WHERE id = ? AND operation_kind = 'merge' AND phase = 'starting'`,
    [process.pid, process.pgid, process.identity, Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge lease disappeared before process identity was recorded");
}

/** Keep the row after a confirmed process exit until its known outcome is durably settled. */
export function markHostMergeProcessExited(db: Database, id: string): void {
  const result = run(db,
    "UPDATE host_merge_leases SET phase = 'exited', updated_at = ? WHERE id = ? AND operation_kind = 'merge' AND phase = 'running' AND outcome_json IS NULL",
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge process exit did not match a running lease");
}

/** Mark any ambiguous outcome as permanently blocking until operator intervention. */
export function markHostMergeLeaseUnknown(db: Database, id: string): void {
  const result = run(db,
    "UPDATE host_merge_leases SET phase = 'unknown', updated_at = ? WHERE id = ? AND operation_kind = 'merge' AND outcome_json IS NULL",
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge lease disappeared or already has a known outcome");
}

/** Record the known business result without releasing ownership before delivery is accepted. */
export function persistHostMergeOutcome(db: Database, id: string, value: WorktreeMergeOutcome): void {
  const result = run(db,
    `UPDATE host_merge_leases
     SET outcome_kind = ?, outcome_json = ?, updated_at = ?
     WHERE id = ? AND operation_kind = 'merge' AND phase = 'exited' AND outcome_json IS NULL`,
    [value.kind, JSON.stringify(value), Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge outcome is not attached to a confirmed exit");
}

export function completeHostMergeLease(db: Database, id: string, completion: HostMergeLeaseCompletion): void {
  if (completion !== "not-spawned") throw new Error("unknown host merge lease completion");
  const result = run(db,
    "DELETE FROM host_merge_leases WHERE id = ? AND operation_kind = 'merge' AND phase IN ('starting', 'running') AND outcome_json IS NULL",
    [id],
  );
  if (result.changes !== 1) throw new Error("host merge lease is not a proven non-execution");
}

export function settleHostMergeLease(db: Database, id: string): void {
  const row = query(db, "SELECT * FROM host_merge_leases WHERE id = ? AND operation_kind = 'merge'", [id])[0];
  if (!row) return;
  if (phase(row.phase) !== "exited" || outcome(row) === null) {
    throw new Error("host merge result is not durably settleable");
  }
  const result = run(db,
    `DELETE FROM host_merge_leases
     WHERE id = ? AND operation_kind = 'merge' AND phase = 'exited' AND outcome_kind = ? AND outcome_json = ?`,
    [id, asString(row.outcome_kind), asString(row.outcome_json)],
  );
  if (result.changes !== 1 && query(db, "SELECT 1 FROM host_merge_leases WHERE id = ? AND operation_kind = 'merge'", [id]).length > 0) {
    throw new Error("host merge result changed before settlement");
  }
}

export function getHostMergeLeaseBySourceId(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sourceId: string,
): HostMergeLeaseRecord | null {
  const row = query(db,
    `SELECT * FROM host_merge_leases
     WHERE operation_kind = 'merge' AND bot_uid = ? AND channel_type = ? AND channel_id = ? AND source_id = ?
     ORDER BY created_at DESC, rowid DESC LIMIT 1`,
    [botUid, target.channelType, target.channelId, sourceId],
  )[0];
  return row ? decodeHostMergeLease(row) : null;
}

/** All durable host merge reservations for one Bot. */
export function listHostMergeLeases(db: Database, botUid: string): HostMergeLeaseRecord[] {
  return query(db, "SELECT * FROM host_merge_leases WHERE bot_uid = ? AND operation_kind = 'merge' ORDER BY created_at, id", [botUid])
    .map(decodeHostMergeLease);
}

/** Reserve the same managed-process ledger for one owner-triggered local workspace removal. */
export function reserveHostRemovalLease(db: Database, input: HostRemovalLeaseInput): void {
  if (
    input.target.channelType !== 5
    || input.id.length === 0
    || input.botUid.length === 0
    || input.target.channelId.length === 0
    || input.sourceId.length === 0
    || input.cwd.length === 0
    || input.commandText.length === 0
    || input.senderId.length === 0
    || input.receiptOutboxId.length === 0
    || !Number.isSafeInteger(input.messageSeq)
    || input.messageSeq < 0
    || input.resourcePaths.some(path => path.length === 0)
  ) {
    throw new Error("invalid host removal lease identity");
  }
  const existing = query(db,
    `SELECT id FROM host_merge_leases
     WHERE operation_kind = 'thread-remove' AND bot_uid = ? AND channel_type = 5 AND channel_id = ? AND source_id = ?
     ORDER BY created_at DESC, rowid DESC LIMIT 1`,
    [input.botUid, input.target.channelId, input.sourceId],
  )[0];
  if (existing) throw new Error("thread removal source already has a durable lease");
  const now = Date.now();
  run(db,
    `INSERT INTO host_merge_leases(
       id, bot_uid, channel_type, channel_id, operation_kind, source_id, cwd, phase,
       pid, pgid, process_identity, source_text, operator_note, sender_id, message_seq,
       receipt_outbox_id, outcome_kind, outcome_json, removal_step, removal_resources_json,
       removal_outcome_json, created_at, updated_at
     ) VALUES(?, ?, 5, ?, 'thread-remove', ?, ?, 'starting', NULL, NULL, NULL, ?, NULL, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?)`,
    [
      input.id,
      input.botUid,
      input.target.channelId,
      input.sourceId,
      input.cwd,
      input.commandText,
      input.senderId,
      input.messageSeq,
      input.receiptOutboxId,
      JSON.stringify([...new Set(input.resourcePaths)]),
      now,
      now,
    ],
  );
}

/** Persist the next destructive command only after the prior process is confirmed or absent. */
export function beginHostRemovalStep(
  db: Database,
  id: string,
  step: WorktreeRemovalStep,
  cwd: string,
): void {
  if ((step !== "worktree" && step !== "branch") || cwd.length === 0) {
    throw new Error("invalid host removal step");
  }
  const result = run(db,
    `UPDATE host_merge_leases
     SET phase = 'starting', pid = NULL, pgid = NULL, process_identity = NULL,
         removal_step = ?, cwd = ?, updated_at = ?
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase IN ('starting', 'exited')
       AND removal_outcome_json IS NULL`,
    [step, cwd, Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host removal lease cannot advance from an unconfirmed process");
}

/** Persist the exact process-group identity before a destructive command is allowed to run. */
export function recordHostRemovalProcess(
  db: Database,
  id: string,
  process: { pid: number; pgid: number; identity: string; cwd: string },
): void {
  if (
    !Number.isSafeInteger(process.pid) || process.pid <= 0
    || !Number.isSafeInteger(process.pgid) || process.pgid <= 0
    || process.identity.length === 0 || process.cwd.length === 0
  ) {
    throw new Error("invalid host removal process identity");
  }
  const result = run(db,
    `UPDATE host_merge_leases SET phase = 'running', pid = ?, pgid = ?, process_identity = ?, updated_at = ?
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase = 'starting' AND cwd = ?`,
    [process.pid, process.pgid, process.identity, Date.now(), id, process.cwd],
  );
  if (result.changes !== 1) throw new Error("host removal lease disappeared before process identity was recorded");
}

export function markHostRemovalProcessExited(db: Database, id: string): void {
  const result = run(db,
    `UPDATE host_merge_leases SET phase = 'exited', updated_at = ?
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase = 'running' AND removal_outcome_json IS NULL`,
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host removal process exit did not match a running lease");
}

/** Ambiguous removal processes keep their exact resources blocked until explicit operator action. */
export function markHostRemovalLeaseUnknown(db: Database, id: string): void {
  const result = run(db,
    `UPDATE host_merge_leases SET phase = 'unknown', updated_at = ?
     WHERE id = ? AND operation_kind = 'thread-remove' AND removal_outcome_json IS NULL`,
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host removal lease disappeared or already has a known outcome");
}

/** Persist command-level receipt intent only after all known per-resource outcomes are saved. */
export function persistHostRemovalOutcome(
  db: Database,
  id: string,
  value: HostThreadRemovalOutcome,
): void {
  if (
    (value.kind !== "complete" && value.kind !== "partial")
    || value.receiptText.length === 0
  ) {
    throw new Error("invalid host removal outcome");
  }
  const result = run(db,
    `UPDATE host_merge_leases
     SET phase = 'exited', removal_outcome_json = ?, updated_at = ?
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase IN ('starting', 'exited')
       AND removal_outcome_json IS NULL`,
    [JSON.stringify(value), Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host removal outcome is not attached to a confirmed process state");
}

/** Drop only a process-free removal with no durable receipt result. */
export function releaseHostRemovalLease(db: Database, id: string): void {
  const result = run(db,
    `DELETE FROM host_merge_leases
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase IN ('starting', 'exited')
       AND removal_outcome_json IS NULL`,
    [id],
  );
  if (result.changes !== 1 && query(db, "SELECT 1 FROM host_merge_leases WHERE id = ? AND operation_kind = 'thread-remove'", [id]).length > 0) {
    throw new Error("host removal lease is still running or has an unresolved outcome");
  }
}

/** Release only after a durable receipt has been enqueued and the archive scheduler is staged. */
export function settleHostRemovalLease(db: Database, id: string): void {
  const row = query(db,
    "SELECT * FROM host_merge_leases WHERE id = ? AND operation_kind = 'thread-remove'",
    [id],
  )[0];
  if (!row) return;
  if (phase(row.phase) !== "exited" || removalOutcome(row) === null) {
    throw new Error("host removal result is not durably settleable");
  }
  const result = run(db,
    `DELETE FROM host_merge_leases
     WHERE id = ? AND operation_kind = 'thread-remove' AND phase = 'exited' AND removal_outcome_json = ?`,
    [id, asString(row.removal_outcome_json)],
  );
  if (
    result.changes !== 1
    && query(db, "SELECT 1 FROM host_merge_leases WHERE id = ? AND operation_kind = 'thread-remove'", [id]).length > 0
  ) {
    throw new Error("host removal result changed before settlement");
  }
}

export function getHostRemovalLeaseBySourceId(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sourceId: string,
): HostRemovalLeaseRecord | null {
  const row = query(db,
    `SELECT * FROM host_merge_leases
     WHERE operation_kind = 'thread-remove' AND bot_uid = ? AND channel_type = ? AND channel_id = ? AND source_id = ?
     ORDER BY created_at DESC, rowid DESC LIMIT 1`,
    [botUid, target.channelType, target.channelId, sourceId],
  )[0];
  return row ? decodeHostRemovalLease(row) : null;
}

export function listHostRemovalLeases(db: Database, botUid: string): HostRemovalLeaseRecord[] {
  return query(db,
    "SELECT * FROM host_merge_leases WHERE bot_uid = ? AND operation_kind = 'thread-remove' ORDER BY created_at, id",
    [botUid],
  ).map(decodeHostRemovalLease);
}
