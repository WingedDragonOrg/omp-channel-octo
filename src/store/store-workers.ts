/**
 * Worker 台账持久化；进度卡记录由 `store-cards.ts` 管理。
 */

import { Database } from "bun:sqlite";
import type { WorkerLedgerInput, WorkerLedgerRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, transaction } from "./store-schema.ts";

/** Legacy writes omit runtimeId; only a confirmed-stopped task in the same bot/workspace may be reused. */
export function recordWorker(db: Database, worker: WorkerLedgerInput): void {
  transaction(db, () => {
    const existing = query(db,
      `SELECT bot_uid, session_id, session_file, identity, runtime_id, state, cwd
       FROM worker_ledger WHERE id = ?`,
      [worker.id],
    )[0];
    if (existing) {
      const currentRuntimeId = asNullableString(existing.runtime_id);
      const nextRuntimeId = worker.runtimeId ?? null;
      const sameTaskWorkspace =
        asString(existing.bot_uid) === worker.botUid
        && asString(existing.cwd) === worker.cwd;
      const sameSessionBinding =
        asString(existing.bot_uid) === worker.botUid
        && asString(existing.session_id) === worker.sessionId
        && asNullableString(existing.session_file) === worker.sessionFile;
      const sameProcessIdentity = sameSessionBinding && asString(existing.identity) === worker.identity;
      const isLegacyUpgrade = currentRuntimeId === null && nextRuntimeId !== null && sameProcessIdentity;
      const isStoppedReplacement =
        asString(existing.state) === "stopped"
        && worker.state === "running"
        && sameTaskWorkspace;
      if (
        (currentRuntimeId !== nextRuntimeId && !isLegacyUpgrade && !isStoppedReplacement)
        || (!sameProcessIdentity && !isStoppedReplacement)
      ) {
        throw new Error(`worker ledger owner or identity conflicts for ${worker.id}`);
      }
    }

    run(db,
      `INSERT INTO worker_ledger(
         id, bot_uid, session_id, session_file, pid, pgid, identity, cwd, move_target_cwd, runtime_id, state, updated_at
       )
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, session_file = excluded.session_file,
         state = excluded.state, updated_at = excluded.updated_at, pid = excluded.pid, pgid = excluded.pgid,
         identity = excluded.identity, cwd = excluded.cwd, move_target_cwd = excluded.move_target_cwd,
         runtime_id = excluded.runtime_id`,
      [
        worker.id,
        worker.botUid,
        worker.sessionId,
        worker.sessionFile,
        worker.pid,
        worker.pgid,
        worker.identity,
        worker.cwd,
        worker.moveTargetCwd,
        worker.runtimeId ?? null,
        worker.state,
        Date.now(),
      ],
    );
  });
}

export function getWorkerLedger(db: Database, botUid: string, id: string): WorkerLedgerRecord | null {
  const row = query(
    db,
    "SELECT * FROM worker_ledger WHERE bot_uid = ? AND id = ?",
    [botUid, id],
  )[0];
  return row ? workerLedgerFromRow(row) : null;
}

export function listWorkerLedger(db: Database, botUid: string): WorkerLedgerRecord[] {
  return query(db, "SELECT * FROM worker_ledger WHERE bot_uid = ? AND state != 'stopped'", [botUid])
    .map(workerLedgerFromRow);
}

function workerLedgerFromRow(row: Record<string, unknown>): WorkerLedgerRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    sessionId: asString(row.session_id),
    sessionFile: asNullableString(row.session_file),
    pid: asNumber(row.pid),
    pgid: asNumber(row.pgid),
    identity: asString(row.identity),
    cwd: asString(row.cwd),
    moveTargetCwd: asNullableString(row.move_target_cwd),
    runtimeId: asNullableString(row.runtime_id),
    state: row.state === "running" || row.state === "stopping" || row.state === "unknown" ? row.state : "stopped",
  };
}

export function markWorkerStopped(db: Database, id: string): void {
  run(db, "UPDATE worker_ledger SET state = 'stopped', updated_at = ? WHERE id = ?", [Date.now(), id]);
}
