/**
 * 任务与 inbound 认领：去重声明、命令声明与状态、任务状态迁移、重启通知。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "./contracts.ts";
import type { InboundClaim, RestartNoticeRecord, TaskRecord } from "./store-records.ts";
import { asNumber, asString, query, run, targetValues, targetWhere, taskFromRow, transaction } from "./store-schema.ts";

export function acceptTask(db: Database, claim: InboundClaim, task: TaskRecord): boolean {
  return transaction(db, () => {
    const existing = query(db,
      `SELECT 1 FROM inbound WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    )[0];
    if (existing) return false;
    const now = Date.now();
    run(db,
      `INSERT INTO inbound(bot_uid, channel_type, channel_id, message_id, sender_id, text, supported, kind, task_id, accepted_at)
       VALUES(?, ?, ?, ?, ?, ?, ?, 'task', ?, ?)`,
      [
        claim.botUid,
        claim.target.channelType,
        claim.target.channelId,
        claim.messageId,
        claim.senderId,
        claim.text,
        claim.supported ? 1 : 0,
        task.id,
        now,
      ],
    );
    run(db,
      `INSERT INTO tasks(
         id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd,
         state, accepted_at, message_seq, history_eligible,
         relay_channel_type, relay_channel_id, relay_thread_name
       )
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)`,
      [
        task.id,
        task.botUid,
        task.channelType,
        task.channelId,
        task.messageId,
        task.senderId,
        task.text,
        task.sessionId,
        task.cwd,
        now,
        task.messageSeq,
        task.historyEligible ? 1 : 0,
        task.relayChannelType,
        task.relayChannelId,
        task.relayThreadName,
      ],
    );
    return true;
  });
}

export function claimCommand(db: Database, claim: InboundClaim): boolean {
  return transaction(db, () => {
    const existing = query(db,
      `SELECT kind, command_state FROM inbound
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    )[0];
    if (existing) {
      if (existing.kind !== "command" || existing.command_state === "completed" || existing.command_state === "pending") {
        return false;
      }
      run(db,
        `UPDATE inbound SET command_state = 'pending'
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
        [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
      );
      return true;
    }
    run(db,
      `INSERT INTO inbound(bot_uid, channel_type, channel_id, message_id, sender_id, text, supported, kind, command_state, accepted_at)
       VALUES(?, ?, ?, ?, ?, ?, ?, 'command', 'pending', ?)`,
      [
        claim.botUid,
        claim.target.channelType,
        claim.target.channelId,
        claim.messageId,
        claim.senderId,
        claim.text,
        claim.supported ? 1 : 0,
        Date.now(),
      ],
    );
    return true;
  });
}

export function completeCommand(db: Database, claim: InboundClaim): void {
  run(db,
    `UPDATE inbound SET command_state = 'completed'
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function failCommand(db: Database, claim: InboundClaim): void {
  run(db,
    `UPDATE inbound SET command_state = 'failed'
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function releaseClaim(db: Database, claim: InboundClaim): void {
  run(db,
    `DELETE FROM inbound
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function getTask(db: Database, taskId: string): TaskRecord | null {
  const row = query(db, "SELECT * FROM tasks WHERE id = ?", [taskId])[0];
  return row ? taskFromRow(row) : null;
}

export function getLatestTask(db: Database, botUid: string, target: ChatTarget): TaskRecord | null {
  const row = query(db,
    `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} ORDER BY accepted_at DESC, rowid DESC LIMIT 1`,
    [botUid, ...targetValues(target)],
  )[0];
  return row ? taskFromRow(row) : null;
}

export function getActiveTask(db: Database, botUid: string, target: ChatTarget): TaskRecord | null {
  const row = query(db,
    `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('queued', 'started')
     ORDER BY accepted_at ASC, rowid ASC LIMIT 1`,
    [botUid, ...targetValues(target)],
  )[0];
  return row ? taskFromRow(row) : null;
}

export function countQueuedTasks(db: Database, botUid: string, target: ChatTarget): number {
  const row = query(db,
    `SELECT COUNT(*) AS count FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
    [botUid, ...targetValues(target)],
  )[0];
  return asNumber(row?.count);
}

export function listQueuedTasks(db: Database): TaskRecord[] {
  return query(db,
    "SELECT * FROM tasks WHERE state = 'queued' ORDER BY accepted_at ASC, rowid ASC",
  ).map((row) => taskFromRow(row));
}

export function markStarted(db: Database, taskId: string): boolean {
  return run(db,
    "UPDATE tasks SET state = 'started', started_at = ? WHERE id = ? AND state = 'queued'",
    [Date.now(), taskId],
  ).changes === 1;
}

export function markCompleted(db: Database, taskId: string, resultText: string | null): void {
  run(db,
    "UPDATE tasks SET state = 'completed', finished_at = ?, result_text = ?, error_text = NULL WHERE id = ?",
    [Date.now(), resultText, taskId],
  );
}

export function markFailed(db: Database, taskId: string, errorText: string): void {
  run(db,
    "UPDATE tasks SET state = 'failed', finished_at = ?, error_text = ?, result_text = NULL WHERE id = ?",
    [Date.now(), errorText, taskId],
  );
}

export function markCancelled(db: Database, taskId: string, errorText = "cancelled"): void {
  run(db,
    "UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ? WHERE id = ? AND state IN ('queued', 'started')",
    [Date.now(), errorText, taskId],
  );
}

export function markInterrupted(db: Database, taskId: string, errorText: string): void {
  run(db,
    "UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = ? WHERE id = ? AND state IN ('queued', 'started')",
    [Date.now(), errorText, taskId],
  );
}

export function markInterruptedOnStart(db: Database, botUid: string): TaskRecord[] {
  return transaction(db, () => {
    const rows = query(db,
      "SELECT * FROM tasks WHERE bot_uid = ? AND state IN ('queued', 'started')",
      [botUid],
    );
    run(db,
      "UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = 'service restarted before task completion' WHERE bot_uid = ? AND state IN ('queued', 'started')",
      [Date.now(), botUid],
    );
    run(db,
      "UPDATE questions SET state = 'cancelled', answered_at = ? WHERE bot_uid = ? AND state = 'pending'",
      [Date.now(), botUid],
    );
    return rows.map((row) => taskFromRow(row));
  });
}

export function addRestartNotice(db: Database, record: RestartNoticeRecord): void {
  run(db,
    `INSERT INTO restart_notices(id, bot_uid, channel_type, channel_id, requested_at)
     VALUES(?, ?, ?, ?, ?)`,
    [record.id, record.botUid, record.channelType, record.channelId, record.requestedAt],
  );
}

export function listRestartNotices(db: Database, botUid: string): RestartNoticeRecord[] {
  return query(db,
    "SELECT * FROM restart_notices WHERE bot_uid = ? ORDER BY requested_at ASC, rowid ASC",
    [botUid],
  ).map((row) => ({
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    requestedAt: asNumber(row.requested_at),
  }));
}

export function clearRestartNotice(db: Database, id: string): void {
  run(db, "DELETE FROM restart_notices WHERE id = ?", [id]);
}

export function cancelQueued(db: Database, botUid: string, target: ChatTarget): string[] {
  return transaction(db, () => {
    const rows = query(db,
      `SELECT id FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [botUid, ...targetValues(target)],
    );
    run(db,
      `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = 'cancelled before start'
       WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [Date.now(), botUid, ...targetValues(target)],
    );
    return rows.map((row) => asString(row.id));
  });
}
