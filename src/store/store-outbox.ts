/**
 * 出站投递：outbox 的创建、版本推进、确认与失败收敛，以及 Bot 事件队列的游标。
 */

import { randomUUID } from "node:crypto";
import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { OutboxInsert, OutboxRecord, OutboxTextPayload, TextStreamOutboxCommit } from "./store-records.ts";
import { SqlValue, asNullableString, asNumber, asString, outboxFromRow, query, run, targetValues, targetWhere, transaction } from "./store-schema.ts";

export function enqueueOutbox(db: Database, record: OutboxInsert): void {
  const mention = record.mention ?? null;
  const createPayload = record.createPayload ?? (
    record.kind === "text" && record.text !== null
      ? { text: record.text, mention }
      : null
  );
  run(db,
    `INSERT INTO outbox(
       id, bot_uid, channel_type, channel_id, task_id, kind, text, path, file_kind, client_msg_no,
       remote_message_id, stream_message_id, segment_index, desired_version, acked_version,
       mention_json, stream_final, create_payload_json, state, created_at
     )
     VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
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
      mention === null ? null : JSON.stringify(mention),
      record.streamFinal === true ? 1 : 0,
      createPayload === null ? null : JSON.stringify(createPayload),
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
    const row = query(db, "SELECT * FROM outbox WHERE id = ?", [id])[0];
    if (!row) return false;
    const item = outboxFromRow(row);
    if (item.kind !== "text" || item.remoteMessageId) return false;
    const createPayload = item.createPayload ?? (
      item.text === null ? null : { text: item.text, mention: item.mention }
    );
    const serializedPayload = createPayload === null ? null : JSON.stringify(createPayload);
    if (item.state === "unknown") {
      if (item.createPayload === null && serializedPayload !== null) {
        run(db,
          "UPDATE outbox SET create_payload_json = ? WHERE id = ? AND state = 'unknown' AND remote_message_id IS NULL",
          [serializedPayload, id],
        );
      }
      return true;
    }
    if (item.state !== "pending") return false;
    return run(db,
      `UPDATE outbox
       SET state = 'unknown', create_payload_json = COALESCE(create_payload_json, ?), last_error = NULL
       WHERE id = ? AND state = 'pending' AND remote_message_id IS NULL`,
      [serializedPayload, id],
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
function mentionJson(mention: OutboxTextPayload["mention"]): string | null {
  return mention === null ? null : JSON.stringify(mention);
}

function createPayloadJson(text: string, mention: OutboxTextPayload["mention"]): string {
  return JSON.stringify({ text, mention });
}

/** Persists one assistant-message snapshot and its segment updates in a single transaction. */
export function commitTextStream(db: Database, stream: TextStreamOutboxCommit): boolean {
  return transaction(db, () => {
    const previous = query(db,
      `SELECT * FROM outbox
       WHERE bot_uid = ? AND task_id = ? AND stream_message_id = ?
       ORDER BY segment_index ASC, created_at ASC, rowid ASC`,
      [stream.botUid, stream.taskId, stream.streamMessageId],
    ).map((row) => outboxFromRow(row));
    if (!stream.final && previous.some((item) => item.streamFinal)) return false;

    const byIndex = new Map(previous.map((item) => [item.segmentIndex, item]));
    const previousCount = previous.reduce((count, item) => Math.max(count, item.segmentIndex + 1), 0);
    const needsFinalSentinel =
      stream.final &&
      stream.forceFinalSentinel &&
      previousCount === 0 &&
      stream.segments.length === 0;
    const segmentCount = Math.max(
      previousCount,
      stream.segments.length,
      needsFinalSentinel ? 1 : 0,
    );
    const version = previous.reduce(
      (highest, item) => Math.max(highest, item.desiredVersion),
      0,
    ) + 1;
    let changed = false;

    for (let segmentIndex = 0; segmentIndex < segmentCount; segmentIndex += 1) {
      const segment = stream.segments[segmentIndex] ?? { text: "", mention: null };
      const existing = byIndex.get(segmentIndex);
      if (!existing) {
        if (segmentIndex >= stream.segments.length && !needsFinalSentinel) continue;
        if (segmentIndex < stream.segments.length && segment.text.length === 0) continue;
        const id = randomUUID();
        const clientMsgNo = randomUUID();
        run(db,
          `INSERT INTO outbox(
             id, bot_uid, channel_type, channel_id, task_id, kind, text, path, file_kind, client_msg_no,
             remote_message_id, stream_message_id, segment_index, desired_version, acked_version,
             mention_json, stream_final, create_payload_json, state, created_at
           )
           VALUES(?, ?, ?, ?, ?, 'text', ?, NULL, NULL, ?, NULL, ?, ?, ?, 0, ?, ?, ?, 'pending', ?)`,
          [
            id,
            stream.botUid,
            stream.target.channelType,
            stream.target.channelId,
            stream.taskId,
            segment.text,
            clientMsgNo,
            stream.streamMessageId,
            segmentIndex,
            version,
            mentionJson(segment.mention),
            stream.final ? 1 : 0,
            createPayloadJson(segment.text, segment.mention),
            Date.now(),
          ],
        );
        changed = true;
        continue;
      }

      const textChanged = existing.text !== segment.text;
      const mentionChanged = mentionJson(existing.mention) !== mentionJson(segment.mention);
      const payloadChanged = textChanged || mentionChanged;
      const finalChanged = stream.final && !existing.streamFinal;
      if (!payloadChanged && !finalChanged) continue;

      const desiredVersion = payloadChanged ? version : existing.desiredVersion;
      const preservesUnknownCreate = existing.state === "unknown" && !existing.remoteMessageId;
      const nextState = payloadChanged
        ? preservesUnknownCreate
          ? "unknown"
          : existing.remoteMessageId && existing.ackedVersion >= desiredVersion
            ? "sent"
            : "pending"
        : existing.state;
      const previouslyAcknowledgedEmptySentinel =
        existing.state === "sent" &&
        existing.text === "" &&
        !existing.remoteMessageId;
      const nextCreatePayload = payloadChanged &&
        !existing.remoteMessageId &&
        existing.attempts === 0 &&
        (existing.state === "pending" || previouslyAcknowledgedEmptySentinel)
        ? createPayloadJson(segment.text, segment.mention)
        : existing.createPayload === null
          ? null
          : createPayloadJson(existing.createPayload.text, existing.createPayload.mention);
      run(db,
        `UPDATE outbox
         SET text = ?, mention_json = ?, desired_version = ?, state = ?, stream_final = ?,
             create_payload_json = ?, attempts = CASE WHEN ? THEN 0 ELSE attempts END,
             last_error = CASE WHEN ? THEN NULL ELSE last_error END
         WHERE id = ?`,
        [
          segment.text,
          mentionJson(segment.mention),
          desiredVersion,
          nextState,
          existing.streamFinal || stream.final ? 1 : 0,
          nextCreatePayload,
          payloadChanged ? 1 : 0,
          payloadChanged ? 1 : 0,
          existing.id,
        ],
      );
      changed = true;
    }
    return changed;
  });
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

/**
 * Latest finalized assistant body persisted for this task. The stream id namespaces are
 * `taskId:<messageId>` and `taskId:resume:<uuid>:<messageId>`; info notices use
 * `taskId:notice:<id>` and are excluded. Only the newest finalized stream is eligible: an
 * empty latest final frame is reported as empty rather than substituted with older output.
 */
export function getLatestFinalAssistantText(
  db: Database,
  botUid: string,
  taskId: string,
): string | null {
  const streamPrefix = `${taskId}:`;
  const rows = query(db,
    `SELECT rowid AS row_id, stream_message_id, segment_index, stream_final, text
     FROM outbox
     WHERE bot_uid = ? AND task_id = ? AND kind = 'text' AND stream_message_id IS NOT NULL
     ORDER BY rowid ASC`,
    [botUid, taskId],
  );
  const streams = new Map<string, {
    firstRowId: number;
    final: boolean;
    segments: Map<number, string>;
  }>();
  for (const row of rows) {
    const streamId = asString(row.stream_message_id);
    if (!streamId.startsWith(streamPrefix)) continue;
    const messageId = streamId.slice(streamPrefix.length);
    if (
      messageId.length === 0
      || messageId.startsWith("notice:")
      || /^resume:[^:]+:notice:/u.test(messageId)
    ) continue;
    const segmentIndex = asNumber(row.segment_index);
    const text = asNullableString(row.text) ?? "";
    const final = asNumber(row.stream_final) !== 0;
    const existing = streams.get(streamId);
    if (existing) {
      existing.segments.set(segmentIndex, text);
      existing.final = existing.final || final;
    } else {
      streams.set(streamId, {
        firstRowId: asNumber(row.row_id),
        final,
        segments: new Map([[segmentIndex, text]]),
      });
    }
  }
  let latest: { firstRowId: number; final: boolean; segments: Map<number, string> } | null = null;
  for (const stream of streams.values()) {
    if (latest === null || stream.firstRowId > latest.firstRowId) latest = stream;
  }
  if (latest === null || !latest.final) return null;
  return [...latest.segments.entries()]
    .sort(([left], [right]) => left - right)
    .map(([, text]) => text)
    .join("");
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
