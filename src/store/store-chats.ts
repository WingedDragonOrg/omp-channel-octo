/**
 * 聊天身份与授权：bot uid、owner、owner 认领、运行期允许列表、会话绑定与追加模式。
 */

import { Database } from "bun:sqlite";
import type { ChatAppendMode, ChatTarget } from "../contracts.ts";
import type { AllowedChatRecord, BindingRecord, BindingSessionInitialization, OutboxInsert } from "./store-records.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";
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

export function ensureBinding(
  db: Database,
  binding: BindingRecord,
  options: { clearSessionInitialization?: boolean } = {},
): void {
  const now = Date.now();
  transaction(db, () => {
    run(db,
      `INSERT INTO chats(bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned,
                          session_switched_at, session_initialization_json, created_at, updated_at)
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
       ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET
         session_initialization_json = CASE
           WHEN ? = 1
             OR chats.session_id <> excluded.session_id
             OR chats.cwd <> excluded.cwd
             OR chats.session_file IS NOT excluded.session_file
             OR chats.cwd_pinned <> excluded.cwd_pinned
             OR excluded.session_switched_at IS NOT NULL
           THEN NULL
           ELSE chats.session_initialization_json
         END,
         session_id = excluded.session_id,
         cwd = excluded.cwd,
         session_file = excluded.session_file,
         cwd_pinned = excluded.cwd_pinned,
         session_switched_at = COALESCE(excluded.session_switched_at, chats.session_switched_at),
         updated_at = excluded.updated_at`,
      [
        binding.botUid,
        binding.channelType,
        binding.channelId,
        binding.sessionId,
        binding.cwd,
        binding.sessionFile,
        binding.cwdPinned === true ? 1 : 0,
        binding.sessionSwitchedAt ?? null,
        now,
        now,
        options.clearSessionInitialization === true ? 1 : 0,
      ],
    );
  });
}

/** Atomically records which started task initialized an unbound pinned thread session. */
export function initializeBindingSessionFromTask(
  db: Database,
  botUid: string,
  target: ChatTarget,
  taskId: string,
  previous: BindingRecord,
  sessionId: string,
  sessionFile: string,
): boolean {
  if (
    target.channelType !== 5
    || previous.botUid !== botUid
    || previous.channelType !== target.channelType
    || previous.channelId !== target.channelId
    || previous.sessionFile !== null
    || previous.cwdPinned !== true
    || sessionId === previous.sessionId
    || sessionFile.length === 0
  ) return false;

  const provenance: BindingSessionInitialization = {
    taskId,
    fromSessionId: previous.sessionId,
    fromCwd: previous.cwd,
    fromSessionFile: null,
    fromCwdPinned: true,
    fromSessionSwitchedAt: previous.sessionSwitchedAt ?? null,
  };
  return transaction(db, () => {
    const startedTask = query(
      db,
      `SELECT 1 FROM tasks
       WHERE id = ? AND bot_uid = ? AND channel_type = ? AND channel_id = ?
         AND session_id = ? AND cwd = ? AND state = 'started'`,
      [taskId, botUid, target.channelType, target.channelId, previous.sessionId, previous.cwd],
    )[0];
    if (startedTask === undefined) return false;
    return run(
      db,
      `UPDATE chats
       SET session_id = ?, session_file = ?, session_initialization_json = ?, updated_at = ?
       WHERE bot_uid = ? AND ${targetWhere(target)}
         AND session_id = ? AND cwd = ? AND session_file IS NULL AND cwd_pinned = 1
         AND session_switched_at IS ? AND session_initialization_json IS NULL`,
      [
        sessionId,
        sessionFile,
        JSON.stringify(provenance),
        Date.now(),
        botUid,
        ...targetValues(target),
        previous.sessionId,
        previous.cwd,
        previous.sessionSwitchedAt ?? null,
      ],
    ).changes === 1;
  });
}

function sessionInitializationFromValue(value: unknown): BindingSessionInitialization | null {
  const serialized = asNullableString(value);
  if (serialized === null) return null;
  try {
    const parsed: unknown = JSON.parse(serialized);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    const provenance = parsed as Partial<BindingSessionInitialization>;
    const switchedAt = provenance.fromSessionSwitchedAt;
    if (
      typeof provenance.taskId !== "string"
      || typeof provenance.fromSessionId !== "string"
      || typeof provenance.fromCwd !== "string"
      || provenance.fromSessionFile !== null
      || provenance.fromCwdPinned !== true
      || switchedAt === undefined
      || (switchedAt !== null && typeof switchedAt !== "number")
    ) return null;
    return {
      taskId: provenance.taskId,
      fromSessionId: provenance.fromSessionId,
      fromCwd: provenance.fromCwd,
      fromSessionFile: null,
      fromCwdPinned: true,
      fromSessionSwitchedAt: switchedAt,
    };
  } catch {
    return null;
  }
}

export function getBinding(db: Database, botUid: string, target: ChatTarget): BindingRecord | null {
  const row = query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned,
            session_switched_at, session_initialization_json
     FROM chats WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  )[0];
  if (!row) return null;
  const sessionInitialization = sessionInitializationFromValue(row.session_initialization_json);
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    cwd: asString(row.cwd),
    sessionFile: asNullableString(row.session_file),
    ...(asNumber(row.cwd_pinned) === 1 ? { cwdPinned: true } : {}),
    sessionSwitchedAt: row.session_switched_at === null || row.session_switched_at === undefined
      ? null
      : asNumber(row.session_switched_at),
    ...(sessionInitialization === null ? {} : { sessionInitialization }),
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

/**
 * The chat's explicit `provider/id[:level]` preference, or null when none is set. Like
 * `chat_modes`, it is independent of the session binding. Use `resolveChatModel` for worker
 * startup policy, including a parent group's child-thread default.
 */
export function getChatModel(db: Database, botUid: string, target: ChatTarget): string | null {
  const row = query(db,
    `SELECT model FROM chat_models WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  )[0];
  if (!row) return null;
  const model = asString(row.model).trim();
  return model.length > 0 ? model : null;
}

export function setChatModel(db: Database, botUid: string, target: ChatTarget, model: string): void {
  run(db,
    `INSERT INTO chat_models(bot_uid, channel_type, channel_id, model, updated_at)
     VALUES(?, ?, ?, ?, ?)
     ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET model = excluded.model, updated_at = excluded.updated_at`,
    [botUid, target.channelType, target.channelId, model, Date.now()],
  );
}

export function clearChatModel(db: Database, botUid: string, target: ChatTarget): void {
  run(db,
    `DELETE FROM chat_models WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  );
}

export function getThreadModel(db: Database, botUid: string, groupId: string): string | null {
  const row = query(db,
    "SELECT model FROM thread_models WHERE bot_uid = ? AND group_id = ?",
    [botUid, groupId],
  )[0];
  if (!row) return null;
  const model = asString(row.model).trim();
  return model.length > 0 ? model : null;
}

export function setThreadModel(db: Database, botUid: string, groupId: string, model: string): void {
  run(db,
    `INSERT INTO thread_models(bot_uid, group_id, model, updated_at)
     VALUES(?, ?, ?, ?)
     ON CONFLICT(bot_uid, group_id) DO UPDATE SET model = excluded.model, updated_at = excluded.updated_at`,
    [botUid, groupId, model, Date.now()],
  );
}

export function clearThreadModel(db: Database, botUid: string, groupId: string): void {
  run(db,
    "DELETE FROM thread_models WHERE bot_uid = ? AND group_id = ?",
    [botUid, groupId],
  );
}

export function resolveChatModel(
  db: Database,
  botUid: string,
  target: ChatTarget,
): { model: string | null; source: "chat" | "thread" | "omp" } {
  const chatModel = getChatModel(db, botUid, target);
  if (chatModel !== null) return { model: chatModel, source: "chat" };

  if (target.channelType === 5) {
    const groupId = threadParentGroupId(target.channelId);
    if (groupId !== null) {
      const threadModel = getThreadModel(db, botUid, groupId);
      if (threadModel !== null) return { model: threadModel, source: "thread" };
    }
  }

  return { model: null, source: "omp" };
}

export function updateBindingSession(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string,
  sessionFile: string | null,
): void {
  run(db,
    `UPDATE chats
     SET session_id = ?, session_file = ?,
         session_initialization_json = CASE
           WHEN session_id = ? AND session_file IS ? THEN session_initialization_json
           ELSE NULL
         END,
         updated_at = ?
     WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [sessionId, sessionFile, sessionId, sessionFile, Date.now(), botUid, ...targetValues(target)],
  );
}
