/**
 * 聊天身份与授权：bot uid、owner、owner 认领、运行期允许列表、会话绑定与追加模式。
 */

import { Database } from "bun:sqlite";
import type { ChatAppendMode, ChatTarget } from "./contracts.ts";
import type { AllowedChatRecord, BindingRecord, OutboxInsert } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, targetValues, targetWhere, transaction } from "./store-schema.ts";
import { enqueueOutbox } from "./store-outbox.ts";

export function setBotUid(db: Database, botUid: string): void {
  run(db,
    "INSERT INTO meta(key, value) VALUES('bot_uid', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    [botUid],
  );
}

export function getBotUid(db: Database): string | null {
  const row = query(db, "SELECT value FROM meta WHERE key = 'bot_uid'")[0];
  return row ? asNullableString(row.value) : null;
}

export function getOwnerId(db: Database, botUid: string): string | null {
  const row = query(db, "SELECT owner_id FROM owners WHERE bot_uid = ?", [botUid])[0];
  return row ? asNullableString(row.owner_id) : null;
}

export function setOwnerId(db: Database, botUid: string, ownerId: string): void {
  const now = Date.now();
  run(db,
    `INSERT INTO owners(bot_uid, owner_id, created_at, updated_at)
     VALUES(?, ?, ?, ?)
     ON CONFLICT(bot_uid) DO UPDATE SET owner_id = excluded.owner_id, updated_at = excluded.updated_at`,
    [botUid, ownerId, now, now],
  );
}

export function hasOwnerEnrollment(db: Database, botUid: string, target: ChatTarget, messageId: string): boolean {
  const row = query(db,
    `SELECT 1 FROM owner_enrollments
     WHERE bot_uid = ? AND ${targetWhere(target)} AND message_id = ?`,
    [botUid, ...targetValues(target), messageId],
  )[0];
  return Boolean(row);
}

export function claimOwnerEnrollment(db: Database, botUid: string,
  target: ChatTarget,
  messageId: string,
  senderId: string,
  text: string,
  outbox: OutboxInsert,): boolean {
  return transaction(db, () => {
    const existingEnrollment = query(db,
      `SELECT 1 FROM owner_enrollments
       WHERE bot_uid = ? AND ${targetWhere(target)} AND message_id = ?`,
      [botUid, ...targetValues(target), messageId],
    )[0];
    if (existingEnrollment) return false;
    const existingOwner = query(db, "SELECT 1 FROM owners WHERE bot_uid = ?", [botUid])[0];
    if (existingOwner) return false;
    const now = Date.now();
    run(db,
      "INSERT INTO owners(bot_uid, owner_id, created_at, updated_at) VALUES(?, ?, ?, ?)",
      [botUid, senderId, now, now],
    );
    run(db,
      `INSERT INTO owner_enrollments(
         bot_uid, channel_type, channel_id, message_id, sender_id, text, accepted_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
      [botUid, target.channelType, target.channelId, messageId, senderId, text, now],
    );
    enqueueOutbox(db, outbox);
    return true;
  });
}

export function grantAllowedChat(db: Database, record: {
  botUid: string;
  channelType: 2 | 5;
  channelId: string;
  grantedBy: string;
  }): boolean {
  return run(db,
    `INSERT INTO allowed_chats(bot_uid, channel_type, channel_id, granted_by, granted_at)
     VALUES(?, ?, ?, ?, ?)
     ON CONFLICT(bot_uid, channel_type, channel_id) DO NOTHING`,
    [record.botUid, record.channelType, record.channelId, record.grantedBy, Date.now()],
  ).changes === 1;
}

export function listAllowedChats(db: Database, botUid: string): AllowedChatRecord[] {
  return query(db,
    "SELECT * FROM allowed_chats WHERE bot_uid = ? ORDER BY granted_at ASC, channel_id ASC",
    [botUid],
  ).map((row) => ({
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) === 5 ? 5 : 2,
    channelId: asString(row.channel_id),
    grantedBy: asString(row.granted_by),
    grantedAt: asNumber(row.granted_at),
  }));
}

export function ensureBinding(db: Database, binding: BindingRecord): void {
  const now = Date.now();
  transaction(db, () => {
    run(db,
      `INSERT INTO chats(bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned, created_at, updated_at)
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET
         session_id = excluded.session_id,
         cwd = excluded.cwd,
         session_file = excluded.session_file,
         cwd_pinned = excluded.cwd_pinned,
         updated_at = excluded.updated_at`,
      [
        binding.botUid,
        binding.channelType,
        binding.channelId,
        binding.sessionId,
        binding.cwd,
        binding.sessionFile,
        binding.cwdPinned === true ? 1 : 0,
        now,
        now,
      ],
    );
  });
}

export function getBinding(db: Database, botUid: string, target: ChatTarget): BindingRecord | null {
  const row = query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned
     FROM chats WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  )[0];
  if (!row) return null;
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    cwd: asString(row.cwd),
    sessionFile: asNullableString(row.session_file),
    ...(asNumber(row.cwd_pinned) === 1 ? { cwdPinned: true } : {}),
  };
}

export function getChatMode(db: Database, botUid: string, target: ChatTarget): ChatAppendMode {
  const row = query(db,
    `SELECT mode FROM chat_modes WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  )[0];
  return row && row.mode === "queue" ? "queue" : "steer";
}

export function setChatMode(db: Database, botUid: string, target: ChatTarget, mode: ChatAppendMode): void {
  run(db,
    `INSERT INTO chat_modes(bot_uid, channel_type, channel_id, mode, updated_at)
     VALUES(?, ?, ?, ?, ?)
     ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET mode = excluded.mode, updated_at = excluded.updated_at`,
    [botUid, target.channelType, target.channelId, mode, Date.now()],
  );
}

export function updateBindingSession(db: Database, botUid: string, target: ChatTarget, sessionId: string, sessionFile: string | null): void {
  run(db,
    `UPDATE chats SET session_id = ?, session_file = ?, updated_at = ? WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [sessionId, sessionFile, Date.now(), botUid, ...targetValues(target)],
  );
}
