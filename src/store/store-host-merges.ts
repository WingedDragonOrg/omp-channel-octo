import { Database } from "bun:sqlite";
import type { ChatTarget, WorktreeMergeOutcome } from "../contracts.ts";
import type { HostMergeLeaseInput, HostMergeLeaseRecord } from "./store-records.ts";
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
     WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND source_id = ?
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
     WHERE id = ? AND phase = 'starting'`,
    [process.pid, process.pgid, process.identity, Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge lease disappeared before process identity was recorded");
}

/** Keep the row after a confirmed process exit until its known outcome is durably settled. */
export function markHostMergeProcessExited(db: Database, id: string): void {
  const result = run(db,
    "UPDATE host_merge_leases SET phase = 'exited', updated_at = ? WHERE id = ? AND phase = 'running' AND outcome_json IS NULL",
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge process exit did not match a running lease");
}

/** Mark any ambiguous outcome as permanently blocking until operator intervention. */
export function markHostMergeLeaseUnknown(db: Database, id: string): void {
  const result = run(db,
    "UPDATE host_merge_leases SET phase = 'unknown', updated_at = ? WHERE id = ? AND outcome_json IS NULL",
    [Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge lease disappeared or already has a known outcome");
}

/** Record the known business result without releasing ownership before delivery is accepted. */
export function persistHostMergeOutcome(db: Database, id: string, value: WorktreeMergeOutcome): void {
  const result = run(db,
    `UPDATE host_merge_leases
     SET outcome_kind = ?, outcome_json = ?, updated_at = ?
     WHERE id = ? AND phase = 'exited' AND outcome_json IS NULL`,
    [value.kind, JSON.stringify(value), Date.now(), id],
  );
  if (result.changes !== 1) throw new Error("host merge outcome is not attached to a confirmed exit");
}

/** Release only a reservation whose operation is proven not to have started. */
export function completeHostMergeLease(db: Database, id: string, completion: HostMergeLeaseCompletion): void {
  if (completion !== "not-spawned") throw new Error("unknown host merge lease completion");
  const result = run(db,
    "DELETE FROM host_merge_leases WHERE id = ? AND phase IN ('starting', 'running') AND outcome_json IS NULL",
    [id],
  );
  if (result.changes !== 1) throw new Error("host merge lease is not a proven non-execution");
}

/** Delete only after a known result has been durably accepted by its consumer. */
export function settleHostMergeLease(db: Database, id: string): void {
  const row = query(db, "SELECT * FROM host_merge_leases WHERE id = ?", [id])[0];
  if (!row) return;
  if (phase(row.phase) !== "exited" || outcome(row) === null) {
    throw new Error("host merge result is not durably settleable");
  }
  const result = run(db,
    `DELETE FROM host_merge_leases
     WHERE id = ? AND phase = 'exited' AND outcome_kind = ? AND outcome_json = ?`,
    [id, asString(row.outcome_kind), asString(row.outcome_json)],
  );
  if (result.changes !== 1 && query(db, "SELECT 1 FROM host_merge_leases WHERE id = ?", [id]).length > 0) {
    throw new Error("host merge result changed before settlement");
  }
}

/** Find the exact operation source used by a retrying host tool; callers never guess by latest row. */
export function getHostMergeLeaseBySourceId(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sourceId: string,
): HostMergeLeaseRecord | null {
  const row = query(db,
    `SELECT * FROM host_merge_leases
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND source_id = ?
     ORDER BY created_at DESC, rowid DESC LIMIT 1`,
    [botUid, target.channelType, target.channelId, sourceId],
  )[0];
  return row ? decodeHostMergeLease(row) : null;
}

/** All durable host merge reservations for one Bot. */
export function listHostMergeLeases(db: Database, botUid: string): HostMergeLeaseRecord[] {
  return query(db, "SELECT * FROM host_merge_leases WHERE bot_uid = ? ORDER BY created_at, id", [botUid])
    .map(decodeHostMergeLease);
}
