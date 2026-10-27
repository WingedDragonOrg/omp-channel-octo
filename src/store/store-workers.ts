/**
 * Worker 台账持久化；进度卡记录由 `store-cards.ts` 管理。
 */

import { Database } from "bun:sqlite";
import type { WorkerLedgerRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run } from "./store-schema.ts";

export function recordWorker(db: Database, worker: WorkerLedgerRecord): void {
  run(db,
    `INSERT INTO worker_ledger(
       id, bot_uid, session_id, session_file, pid, pgid, identity, cwd, move_target_cwd, state, updated_at
     )
     VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at,
       pid = excluded.pid, pgid = excluded.pgid, identity = excluded.identity, cwd = excluded.cwd,
       move_target_cwd = excluded.move_target_cwd`,
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
      worker.state,
      Date.now(),
    ],
  );
}

export function markWorkerMoveTarget(db: Database, id: string, cwd: string): void {
  run(db,
    "UPDATE worker_ledger SET move_target_cwd = ?, updated_at = ? WHERE id = ?",
    [cwd, Date.now(), id],
  );
}

export function listWorkerLedger(db: Database, botUid: string): WorkerLedgerRecord[] {
  return query(db, "SELECT * FROM worker_ledger WHERE bot_uid = ? AND state != 'stopped'", [botUid]).map(
    (row) => ({
      id: asString(row.id),
      botUid: asString(row.bot_uid),
      sessionId: asString(row.session_id),
      sessionFile: asString(row.session_file),
      pid: asNumber(row.pid),
      pgid: asNumber(row.pgid),
      identity: asString(row.identity),
      cwd: asString(row.cwd),
      moveTargetCwd: asNullableString(row.move_target_cwd),
      state: row.state === "running" || row.state === "stopping" || row.state === "unknown" ? row.state : "stopped",
    }),
  );
}

export function markWorkerStopped(db: Database, id: string): void {
  run(db, "UPDATE worker_ledger SET state = 'stopped', updated_at = ? WHERE id = ?", [Date.now(), id]);
}
