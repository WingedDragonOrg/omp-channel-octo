/**
 * 进度卡与 worker 台账：每张卡一行（重试次数、最后快照）、worker 进程记录与移动目标。
 */

import { Database } from "bun:sqlite";
import type { ProgressCardRecord, WorkerLedgerRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run } from "./store-schema.ts";

export function saveProgressCard(db: Database, record: Omit<ProgressCardRecord, "attempts">): void {
  run(db,
    `INSERT INTO progress_cards(
       bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
     )
     VALUES(?, ?, ?, ?, ?, ?, ?, 0, ?)
     ON CONFLICT(bot_uid, task_id, message_id) DO UPDATE SET
       channel_type = excluded.channel_type,
       channel_id = excluded.channel_id,
       last_seq = excluded.last_seq,
       snapshot = excluded.snapshot,
       attempts = 0,
       updated_at = excluded.updated_at`,
    [
      record.botUid,
      record.taskId,
      record.channelType,
      record.channelId,
      record.messageId,
      record.lastSeq,
      record.snapshot,
      Date.now(),
    ],
  );
}

export function listProgressCards(db: Database, botUid: string): ProgressCardRecord[] {
  return query(db,
    "SELECT * FROM progress_cards WHERE bot_uid = ? ORDER BY updated_at ASC, task_id ASC",
    [botUid],
  ).map((row) => ({
    botUid: asString(row.bot_uid),
    taskId: asString(row.task_id),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    messageId: asString(row.message_id),
    lastSeq: asNumber(row.last_seq),
    snapshot: asString(row.snapshot),
    attempts: asNumber(row.attempts),
  }));
}

export function recordProgressCardAttempt(db: Database, botUid: string, taskId: string, messageId: string): number {
  run(db,
    `UPDATE progress_cards
     SET attempts = attempts + 1, last_seq = last_seq + 1, updated_at = ?
     WHERE bot_uid = ? AND task_id = ? AND message_id = ?`,
    [Date.now(), botUid, taskId, messageId],
  );
  const row = query(db,
    "SELECT attempts FROM progress_cards WHERE bot_uid = ? AND task_id = ? AND message_id = ?",
    [botUid, taskId, messageId],
  )[0];
  return asNumber(row?.attempts);
}

export function clearProgressCard(db: Database, botUid: string, taskId: string, messageId: string): void {
  run(db,
    "DELETE FROM progress_cards WHERE bot_uid = ? AND task_id = ? AND message_id = ?",
    [botUid, taskId, messageId],
  );
}

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
