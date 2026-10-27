/**
 * 进度卡持久化：保存卡片重试次数与最后快照。
 */

import { Database } from "bun:sqlite";
import type { ProgressCardRecord } from "./store-records.ts";
import { asNumber, asString, query, run } from "./store-schema.ts";

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

/**
 * 该 Bot 的全部进度卡，按 rowid 升序——rowid 是首次插入顺序（UPSERT 只改行内容、不改 rowid），
 * 也就是轮次创建顺序：同一任务最后一行必定是最新一轮。不能用 updated_at 排序——旧轮的重试/迟到
 * 重写会把它的行推到新轮之后，让「最后一条」指向过期的轮次。
 */
export function listProgressCards(db: Database, botUid: string): ProgressCardRecord[] {
  return query(db,
    "SELECT * FROM progress_cards WHERE bot_uid = ? ORDER BY rowid ASC",
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

