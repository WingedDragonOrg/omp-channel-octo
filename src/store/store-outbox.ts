/**
 * 出站投递：outbox 的创建、版本推进、确认与失败收敛，以及 Bot 事件队列的游标。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { OutboxInsert, OutboxRecord } from "./store-records.ts";
import { SqlValue, asNullableString, asNumber, asString, outboxFromRow, query, run, targetValues, targetWhere, transaction } from "./store-schema.ts";

export function enqueueOutbox(db: Database, record: OutboxInsert): void {
  run(db,
    `INSERT INTO outbox(
       id, bot_uid, channel_type, channel_id, task_id, kind, text, path, file_kind, client_msg_no,
       remote_message_id, stream_message_id, segment_index, desired_version, acked_version, state, created_at
     )
     VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
    [
      record.id,
      record.botUid,
      record.channelType,
      record.channelId,
      record.taskId,
      record.kind,
      record.text,
      record.path,
      record.fileKind,
      record.clientMsgNo,
      record.remoteMessageId ?? null,
      record.streamMessageId ?? null,
      record.segmentIndex ?? 0,
      record.desiredVersion ?? 0,
      record.ackedVersion ?? 0,
      Date.now(),
    ],
  );
}

export function getOutbox(db: Database, id: string): OutboxRecord | null {
  const row = query(db, "SELECT * FROM outbox WHERE id = ?", [id])[0];
  return row ? outboxFromRow(row) : null;
}

export function beginTextCreate(db: Database, id: string): boolean {
  return transaction(db, () => {
    const row = query(db,
      "SELECT kind, remote_message_id, state FROM outbox WHERE id = ?",
      [id],
    )[0];
    if (!row || row.kind !== "text" || asNullableString(row.remote_message_id)) return false;
    if (row.state === "unknown") return true;
    if (row.state !== "pending") return false;
    return run(db,
      "UPDATE outbox SET state = 'unknown', last_error = NULL WHERE id = ? AND state = 'pending' AND remote_message_id IS NULL",
      [id],
    ).changes === 1;
  });
}

export function listTextStream(db: Database, botUid: string, taskId: string, streamMessageId: string): OutboxRecord[] {
  return query(db,
    `SELECT * FROM outbox
     WHERE bot_uid = ? AND task_id = ? AND stream_message_id = ?
     ORDER BY segment_index ASC, created_at ASC, rowid ASC`,
    [botUid, taskId, streamMessageId],
  ).map((row) => outboxFromRow(row));
}

export function updateTextOutbox(db: Database, id: string, text: string, desiredVersion: number): boolean {
  return transaction(db, () => {
    const row = query(db,
      "SELECT kind, remote_message_id, state, desired_version, acked_version FROM outbox WHERE id = ?",
      [id],
    )[0];
    if (!row || row.kind !== "text") return false;
    const currentDesired = asNumber(row.desired_version);
    if (desiredVersion <= currentDesired) return false;
    const remoteMessageId = asNullableString(row.remote_message_id);
    const currentAcked = asNumber(row.acked_version);
    const preservesUnknownCreate = row.state === "unknown" && !remoteMessageId;
    const nextState = preservesUnknownCreate
      ? "unknown"
      : remoteMessageId && currentAcked >= desiredVersion
        ? "sent"
        : "pending";
    run(db,
      `UPDATE outbox
       SET text = ?, desired_version = ?, acked_version = ?, state = ?, attempts = 0, last_error = NULL
       WHERE id = ? AND desired_version < ?`,
      [text, desiredVersion, currentAcked, nextState, id, desiredVersion],
    );
    return true;
  });
}

export function bindTextOutbox(db: Database, id: string, remoteMessageId: string): boolean {
  const normalizedRemoteMessageId = remoteMessageId.trim();
  if (normalizedRemoteMessageId.length === 0) return false;
  return transaction(db, () => {
    const row = query(db,
      "SELECT kind, remote_message_id, desired_version, acked_version FROM outbox WHERE id = ?",
      [id],
    )[0];
    if (!row || row.kind !== "text") return false;
    const currentRemoteMessageId = asNullableString(row.remote_message_id);
    if (currentRemoteMessageId && currentRemoteMessageId !== normalizedRemoteMessageId) return false;
    const desiredVersion = asNumber(row.desired_version);
    const ackedVersion = asNumber(row.acked_version);
    const state = ackedVersion >= desiredVersion ? "sent" : "pending";
    run(db,
      `UPDATE outbox
       SET remote_message_id = ?, state = ?, attempts = 0, last_error = NULL,
           sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END
       WHERE id = ?`,
      [normalizedRemoteMessageId, state, state, Date.now(), id],
    );
    return true;
  });
}

export function acknowledgeTextOutbox(db: Database, id: string,
  attemptedVersion: number,
  remoteMessageId: string | null,): boolean {
  return transaction(db, () => {
    const row = query(db,
      "SELECT kind, remote_message_id, desired_version, acked_version FROM outbox WHERE id = ?",
      [id],
    )[0];
    if (!row || row.kind !== "text") return false;
    const currentRemote = asNullableString(row.remote_message_id);
    const nextRemote = remoteMessageId ?? currentRemote;
    const desiredVersion = asNumber(row.desired_version);
    const nextAcked = Math.max(
      asNumber(row.acked_version),
      Math.min(attemptedVersion, desiredVersion),
    );
    const knownRemote = typeof nextRemote === "string" && nextRemote.length > 0;
    const currentText = asNullableString(
      query(db, "SELECT text FROM outbox WHERE id = ?", [id])[0]?.text,
    );
    const emptyWithoutRemote = currentText === "" && !knownRemote;
    const nextState = emptyWithoutRemote || (knownRemote && nextAcked >= desiredVersion)
      ? "sent"
      : knownRemote
        ? "pending"
        : "unknown";
    run(db,
      `UPDATE outbox
       SET remote_message_id = ?, acked_version = ?, state = ?, attempts = 0,
           last_error = NULL,
           sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END
       WHERE id = ?`,
      [nextRemote, nextAcked, nextState, nextState, Date.now(), id],
    );
    return true;
  });
}

export function failTextOutbox(db: Database, id: string, attemptedVersion: number, errorText: string): boolean {
  return transaction(db, () => {
    const row = query(db,
      "SELECT desired_version, remote_message_id FROM outbox WHERE id = ?",
      [id],
    )[0];
    if (!row) return false;
    const desiredVersion = asNumber(row.desired_version);
    const remoteMessageId = asNullableString(row.remote_message_id);
    const state = !remoteMessageId
      ? "unknown"
      : attemptedVersion < desiredVersion
        ? "pending"
        : "unknown";
    run(db,
      "UPDATE outbox SET state = ?, attempts = attempts + 1, last_error = ? WHERE id = ?",
      [state, errorText, id],
    );
    return true;
  });
}

export function listPendingOutbox(db: Database, botUid: string, target?: ChatTarget): OutboxRecord[] {
  const clauses = ["bot_uid = ?", "state IN ('pending', 'unknown')"];
  const values: SqlValue[] = [botUid];
  if (target) {
    clauses.push(targetWhere(target));
    values.push(...targetValues(target));
  }
  return query(db,
    `SELECT * FROM outbox WHERE ${clauses.join(" AND ")} ORDER BY created_at ASC, rowid ASC`,
    values,
  ).map((row) => outboxFromRow(row));
}

export function listPendingOutboxTargets(db: Database, botUid: string): ChatTarget[] {
  return query(db,
    `SELECT DISTINCT channel_type, channel_id FROM outbox WHERE bot_uid = ? AND state IN ('pending', 'unknown')`,
    [botUid],
  ).map((row) => ({
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
  }));
}

export function countPendingOutbox(db: Database, botUid: string, target: ChatTarget): number {
  const row = query(db,
    `SELECT COUNT(*) AS count FROM outbox WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('pending', 'unknown')`,
    [botUid, ...targetValues(target)],
  )[0];
  return asNumber(row?.count);
}

export function countUnknownOutbox(db: Database, botUid: string, target: ChatTarget): number {
  const row = query(db,
    `SELECT COUNT(*) AS count FROM outbox WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'unknown'`,
    [botUid, ...targetValues(target)],
  )[0];
  return asNumber(row?.count);
}

export function markOutboxSent(db: Database, id: string): void {
  run(db, "UPDATE outbox SET state = 'sent', sent_at = ?, last_error = NULL WHERE id = ?", [Date.now(), id]);
}

export function markOutboxFailure(db: Database, id: string, errorText: string): void {
  run(db,
    "UPDATE outbox SET state = 'unknown', attempts = attempts + 1, last_error = ? WHERE id = ?",
    [errorText, id],
  );
}

export function markOutboxDead(db: Database, id: string, errorText: string): void {
  run(db,
    "UPDATE outbox SET state = 'dead', last_error = ?, sent_at = ? WHERE id = ?",
    [errorText, Date.now(), id],
  );
}

export function getBotEventCursor(db: Database, botUid: string): number {
  const row = query(db, "SELECT event_id FROM bot_event_cursor WHERE bot_uid = ?", [botUid])[0];
  return row === undefined ? 0 : asNumber(row.event_id);
}

export function setBotEventCursor(db: Database, botUid: string, eventId: number): void {
  run(db,
    `INSERT INTO bot_event_cursor(bot_uid, event_id, updated_at) VALUES(?, ?, ?)
     ON CONFLICT(bot_uid) DO UPDATE SET event_id = excluded.event_id, updated_at = excluded.updated_at
     WHERE excluded.event_id > bot_event_cursor.event_id`,
    [botUid, eventId, Date.now()],
  );
}
