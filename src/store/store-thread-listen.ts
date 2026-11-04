import { Database } from "bun:sqlite";
import type { ChatTarget, ThreadListenLevel } from "../contracts.ts";
import type { ThreadListenRecord, ThreadMergeEventRecord } from "./store-records.ts";
import { asNumber, asString, query, run, transaction } from "./store-schema.ts";

export function ensureThreadListen(
  db: Database,
  record: Omit<ThreadListenRecord, "mergePending">,
): ThreadListenRecord {
  run(db,
    `INSERT OR IGNORE INTO thread_listens(
       bot_uid, channel_id, creator_channel_type, creator_channel_id, thread_name, listen, merge_pending
     ) VALUES(?, ?, ?, ?, ?, ?, 0)`,
    [
      record.botUid,
      record.channelId,
      record.creatorChannelType,
      record.creatorChannelId,
      record.threadName,
      record.listen,
    ],
  );
  const stored = threadListen(db, record.botUid, record.channelId);
  if (!stored) throw new Error("thread listen policy was not stored");
  return stored;
}

export function getThreadListen(
  db: Database,
  botUid: string,
  target: ChatTarget,
): ThreadListenRecord | null {
  if (target.channelType !== 5) return null;
  const row = query(db,
    "SELECT * FROM thread_listens WHERE bot_uid = ? AND channel_id = ?",
    [botUid, target.channelId],
  )[0];
  return row ? threadListenFromRow(row) : null;
}

export function setThreadListen(
  db: Database,
  botUid: string,
  target: ChatTarget,
  listen: ThreadListenLevel,
): ThreadListenRecord {
  if (target.channelType !== 5) throw new Error("thread listen policies require a thread target");
  run(db,
    "UPDATE thread_listens SET listen = ? WHERE bot_uid = ? AND channel_id = ?",
    [listen, botUid, target.channelId],
  );
  const stored = threadListen(db, botUid, target.channelId);
  if (!stored) throw new Error("thread listen policy must be ensured before it is changed");
  return stored;
}

export function getThreadArchiveGeneration(
  db: Database,
  botUid: string,
  target: ChatTarget,
): number {
  if (target.channelType !== 5) throw new Error("thread archive generations require a thread target");
  const row = query(db,
    "SELECT generation FROM thread_archive_generations WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ?",
    [botUid, target.channelId],
  )[0];
  const generation = row?.generation ?? 0;
  if (typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 0) {
    throw new Error("invalid persisted thread archive generation");
  }
  return generation;
}

export function advanceThreadArchiveGeneration(
  db: Database,
  botUid: string,
  target: ChatTarget,
): number {
  if (target.channelType !== 5 || botUid.length === 0 || target.channelId.length === 0) {
    throw new Error("invalid thread archive generation target");
  }
  run(db,
    `INSERT INTO thread_archive_generations(bot_uid, channel_type, channel_id, generation)
     VALUES(?, 5, ?, 1)
     ON CONFLICT(bot_uid, channel_type, channel_id)
     DO UPDATE SET generation = generation + 1`,
    [botUid, target.channelId],
  );
  return getThreadArchiveGeneration(db, botUid, target);
}

export function beginThreadMerge(db: Database, botUid: string, target: ChatTarget): void {
  if (target.channelType !== 5) throw new Error("thread merge intent requires a thread target");
  transaction(db, () => {
    const policy = threadListen(db, botUid, target.channelId);
    if (!policy) throw new Error("thread listen policy must be ensured before merge intent");
    run(db,
      "UPDATE thread_listens SET merge_pending = 1 WHERE bot_uid = ? AND channel_id = ?",
      [botUid, target.channelId],
    );
    run(db,
      `UPDATE tasks SET relay_suppressed = 1
       WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND state IN ('queued', 'started')`,
      [botUid, target.channelId],
    );
  });
}

export function saveThreadMergeEvent(db: Database, event: ThreadMergeEventRecord): void {
  transaction(db, () => {
    if (!threadListen(db, event.botUid, event.channelId)) {
      throw new Error("thread listen policy must be ensured before merge completion");
    }
    const inserted = run(db,
      `INSERT OR IGNORE INTO thread_merge_events(
         id, bot_uid, channel_id, target_branch, commit_hash, removed, archive
       ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
      [
        event.id,
        event.botUid,
        event.channelId,
        event.targetBranch,
        event.commit,
        event.removed ? 1 : 0,
        event.archive,
      ],
    );
    if (inserted.changes === 0) return;
    run(db,
      `UPDATE tasks SET relay_suppressed = 1
       WHERE bot_uid = ? AND channel_type = 5 AND channel_id = ? AND state IN ('queued', 'started')`,
      [event.botUid, event.channelId],
    );
    run(db,
      "UPDATE thread_listens SET merge_pending = 0 WHERE bot_uid = ? AND channel_id = ?",
      [event.botUid, event.channelId],
    );
  });
}

export function getThreadMergeEvent(db: Database, eventId: string): ThreadMergeEventRecord | null {
  const row = query(db, "SELECT * FROM thread_merge_events WHERE id = ?", [eventId])[0];
  return row ? threadMergeEventFromRow(row) : null;
}

export function listThreadMergeEvents(db: Database, botUid: string): ThreadMergeEventRecord[] {
  return query(db,
    "SELECT * FROM thread_merge_events WHERE bot_uid = ? ORDER BY rowid",
    [botUid],
  ).map(threadMergeEventFromRow);
}

export function listPendingThreadMergeEvents(db: Database, botUid: string): ThreadMergeEventRecord[] {
  return query(db,
    "SELECT * FROM thread_merge_events WHERE bot_uid = ? AND delivery_state = 'pending' ORDER BY rowid",
    [botUid],
  ).map(threadMergeEventFromRow);
}

export function isThreadMergeEventPending(db: Database, eventId: string): boolean {
  return query(db,
    "SELECT 1 FROM thread_merge_events WHERE id = ? AND delivery_state = 'pending'",
    [eventId],
  ).length > 0;
}

export function claimThreadMergeDispatchIntent(db: Database, eventId: string): boolean {
  return run(db,
    "UPDATE thread_merge_events SET delivery_state = 'intent' WHERE id = ? AND delivery_state = 'pending'",
    [eventId],
  ).changes === 1;
}

export function markThreadMergeEventDelivered(db: Database, eventId: string): void {
  run(db,
    `UPDATE thread_merge_events SET delivery_state = 'delivered'
     WHERE id = ? AND delivery_state IN ('pending', 'intent')`,
    [eventId],
  );
}

function threadListen(db: Database, botUid: string, channelId: string): ThreadListenRecord | null {
  const row = query(db,
    "SELECT * FROM thread_listens WHERE bot_uid = ? AND channel_id = ?",
    [botUid, channelId],
  )[0];
  return row ? threadListenFromRow(row) : null;
}

function threadListenFromRow(row: Record<string, unknown>): ThreadListenRecord {
  return {
    botUid: asString(row.bot_uid),
    channelId: asString(row.channel_id),
    creatorChannelType: asNumber(row.creator_channel_type) as 2 | 5,
    creatorChannelId: asString(row.creator_channel_id),
    threadName: asString(row.thread_name),
    listen: asString(row.listen) as ThreadListenLevel,
    mergePending: asNumber(row.merge_pending) !== 0,
  };
}

function threadMergeEventFromRow(row: Record<string, unknown>): ThreadMergeEventRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelId: asString(row.channel_id),
    targetBranch: asString(row.target_branch),
    commit: typeof row.commit_hash === "string" ? row.commit_hash : null,
    removed: asNumber(row.removed) !== 0,
    archive: asString(row.archive) as ThreadMergeEventRecord["archive"],
  };
}
