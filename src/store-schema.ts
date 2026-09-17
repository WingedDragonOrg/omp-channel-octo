/**
 * SQLite 的物理形状：建表 DDL、列迁移、行解码、带 db 句柄的查询辅助，以及 `openChannelStore`。
 * 领域读写分布在 `store-chats/tasks/cards/delivery/ledgers`，门面在 `store.ts`。
 */

import { Database } from "bun:sqlite";
import type { ChatExecutionState, ChatTarget } from "./contracts.ts";
import type { ContextLedgerRecord, HistoryLedgerRecord, OutboxRecord, QuestionRecord, TaskRecord } from "./store-records.ts";

export type SqlValue = string | number | null | Uint8Array;
export type SqlRow = Record<string, unknown>;

export function targetWhere(target: ChatTarget): string {
  return "channel_type = ? AND channel_id = ?";
}

export function targetValues(target: ChatTarget): SqlValue[] {
  return [target.channelType, target.channelId];
}

/**
 * One row per progress card. A run opens a new card for every user round it serves, so the row is
 * keyed by the card's server message id: each card is then closed, retried, and dropped on its own.
 */
export const PROGRESS_CARD_COLUMNS = `
      bot_uid TEXT NOT NULL,
      task_id TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      last_seq INTEGER NOT NULL CHECK(last_seq >= 1),
      snapshot TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, task_id, message_id)`;

/**
 * Group history accounting is keyed on full group (2) / CommunityTopic thread (5)
 * identities; anything else can never own a ledger row.
 */
export function validHistoryIdentity(botUid: string, target: ChatTarget, sessionId: string): boolean {
  return (
  botUid.length > 0 &&
  sessionId.length > 0 &&
  target.channelId.length > 0 &&
  (target.channelType === 2 || target.channelType === 5)
  );
}

export function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

export function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

export function asNumber(value: unknown): number {
  return typeof value === "number" ? value : Number(value ?? 0);
}

export function asState(value: unknown): ChatExecutionState {
  if (
  value === "none" ||
  value === "idle" ||
  value === "queued" ||
  value === "started" ||
  value === "running" ||
  value === "completed" ||
  value === "failed" ||
  value === "cancelled" ||
  value === "interrupted"
  ) {
  return value === "started" ? "running" : value;
  }
  return "failed";
}


export function applySchema(db: Database): void {
  db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY NOT NULL,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS owners (
      bot_uid TEXT PRIMARY KEY NOT NULL,
      owner_id TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS owner_enrollments (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      accepted_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, message_id)
    );
    CREATE TABLE IF NOT EXISTS allowed_chats (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      granted_by TEXT NOT NULL,
      granted_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS chats (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      cwd TEXT NOT NULL,
      session_file TEXT,
      cwd_pinned INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS inbound (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      supported INTEGER NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('task', 'command')),
      command_state TEXT NOT NULL DEFAULT 'pending' CHECK(command_state IN ('pending', 'completed', 'failed')),
      task_id TEXT,
      accepted_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      session_id TEXT NOT NULL,
      cwd TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'interrupted')),
      accepted_at INTEGER NOT NULL,
      started_at INTEGER,
      finished_at INTEGER,
      result_text TEXT,
      error_text TEXT,
      message_seq INTEGER NOT NULL DEFAULT 0,
      history_eligible INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS tasks_chat_order
      ON tasks(bot_uid, channel_type, channel_id, accepted_at, id);
    CREATE INDEX IF NOT EXISTS tasks_active
      ON tasks(bot_uid, channel_type, channel_id, state);
    CREATE TABLE IF NOT EXISTS outbox (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      task_id TEXT,
      kind TEXT NOT NULL CHECK(kind IN ('text', 'file')),
      text TEXT,
      path TEXT,
      file_kind TEXT CHECK(file_kind IN ('image', 'file')),
      client_msg_no TEXT NOT NULL UNIQUE,
      state TEXT NOT NULL CHECK(state IN ('pending', 'unknown', 'sent', 'dead')),
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      remote_message_id TEXT,
      stream_message_id TEXT,
      segment_index INTEGER NOT NULL DEFAULT 0,
      desired_version INTEGER NOT NULL DEFAULT 0,
      acked_version INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      sent_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS outbox_order
      ON outbox(bot_uid, channel_type, channel_id, created_at, id);
    CREATE TABLE IF NOT EXISTS questions (
      id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      payload TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('pending', 'answered', 'cancelled', 'expired')),
      answer TEXT,
      answered_by TEXT,
      created_at INTEGER NOT NULL,
      answered_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS questions_chat_pending
      ON questions(bot_uid, channel_type, channel_id, state);
    CREATE TABLE IF NOT EXISTS worker_ledger (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      session_id TEXT NOT NULL,
      session_file TEXT NOT NULL,
      pid INTEGER NOT NULL,
      pgid INTEGER NOT NULL,
      identity TEXT NOT NULL,
      cwd TEXT NOT NULL,
      move_target_cwd TEXT,
      state TEXT NOT NULL CHECK(state IN ('running', 'stopping', 'stopped', 'unknown')),
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS session_context_ledger (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      version INTEGER NOT NULL CHECK(version >= 0),
      phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
      attempt_id TEXT NOT NULL,
      previous_version INTEGER CHECK(previous_version IS NULL OR previous_version >= 0),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
    );
    CREATE INDEX IF NOT EXISTS session_context_target
      ON session_context_ledger(bot_uid, channel_type, channel_id);
    CREATE TABLE IF NOT EXISTS group_history_ledger (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      last_seq INTEGER NOT NULL CHECK(last_seq >= 0),
      phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
      attempt_id TEXT NOT NULL,
      previous_seq INTEGER CHECK(previous_seq IS NULL OR previous_seq >= 0),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
    );
    CREATE INDEX IF NOT EXISTS group_history_target
      ON group_history_ledger(bot_uid, channel_type, channel_id);
    CREATE TABLE IF NOT EXISTS restart_notices (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      requested_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS chat_modes (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      mode TEXT NOT NULL CHECK(mode IN ('steer', 'queue')),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS chat_models (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      model TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_notify (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      creator_channel_type INTEGER NOT NULL CHECK(creator_channel_type IN (2, 5)),
      creator_channel_id TEXT NOT NULL,
      thread_name TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS progress_cards (${PROGRESS_CARD_COLUMNS}
    );
    CREATE TABLE IF NOT EXISTS bot_event_cursor (
      bot_uid TEXT PRIMARY KEY NOT NULL,
      event_id INTEGER NOT NULL CHECK(event_id >= 0),
      updated_at INTEGER NOT NULL
    );
  `);
  for (const statement of [
    "ALTER TABLE outbox ADD COLUMN remote_message_id TEXT",
    "ALTER TABLE outbox ADD COLUMN stream_message_id TEXT",
    "ALTER TABLE outbox ADD COLUMN segment_index INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN desired_version INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN acked_version INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE worker_ledger ADD COLUMN move_target_cwd TEXT",
    "ALTER TABLE tasks ADD COLUMN message_seq INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN history_eligible INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE chats ADD COLUMN cwd_pinned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE questions ADD COLUMN card_message_id TEXT",
    "ALTER TABLE questions ADD COLUMN answered_by TEXT",
  ]) {
    try {
      db.exec(statement);
    } catch (error) {
      if (!String(error).includes("duplicate column")) throw error;
    }
  }
  db.exec(
    "CREATE INDEX IF NOT EXISTS outbox_stream ON outbox(bot_uid, task_id, stream_message_id, segment_index)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS questions_card ON questions(bot_uid, channel_type, channel_id, card_message_id)",
  );
  migrateProgressCards(db);
}

export function migrateProgressCards(db: Database): void {
  const sql = asString(
    query(db, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'progress_cards'")[0]?.sql,
  );
  if (sql.includes("PRIMARY KEY(bot_uid, task_id, message_id)")) return;
  transaction(db, () => {
    db.exec(`
      ALTER TABLE progress_cards RENAME TO progress_cards_legacy;
      CREATE TABLE progress_cards (${PROGRESS_CARD_COLUMNS}
      );
      INSERT INTO progress_cards(
        bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
      )
      SELECT bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
      FROM progress_cards_legacy;
      DROP TABLE progress_cards_legacy;
    `);
  });
}

export function query(db: Database, sql: string, values: readonly SqlValue[] = []): SqlRow[] {
  return db.query(sql).all(...values) as SqlRow[];
}

export function run(db: Database, sql: string, values: readonly SqlValue[] = []): { changes: number } {
  const result = db.query(sql).run(...values) as { changes?: number };
  return { changes: result.changes ?? 0 };
}

export function transaction<T>(db: Database, work: () => T): T {
  db.exec("BEGIN IMMEDIATE");
  try {
    const result = work();
    db.exec("COMMIT");
    return result;
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // Preserve the original database failure.
    }
    throw error;
  }
}

export function historyLedgerRow(db: Database, botUid: string, target: ChatTarget, sessionId: string): SqlRow | undefined {
  return query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, last_seq, phase, attempt_id, previous_seq
     FROM group_history_ledger
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
    [botUid, ...targetValues(target), sessionId],
  )[0];
}

export function historyLedgerFromRow(row: SqlRow): HistoryLedgerRecord {
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    lastSeq: asNumber(row.last_seq),
    phase: row.phase === "committed" ? "committed" : "pending",
    attemptId: asString(row.attempt_id),
    previousSeq: row.previous_seq === null || row.previous_seq === undefined
      ? null
      : asNumber(row.previous_seq),
  };
}

export function contextLedgerFromRow(row: SqlRow): ContextLedgerRecord {
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    version: asNumber(row.version),
    phase: row.phase === "committed" ? "committed" : "pending",
    attemptId: asString(row.attempt_id),
    previousVersion: row.previous_version === null || row.previous_version === undefined
      ? null
      : asNumber(row.previous_version),
  };
}

export function taskFromRow(row: SqlRow): TaskRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    messageId: asString(row.message_id),
    senderId: asString(row.sender_id),
    text: asString(row.text),
    sessionId: asString(row.session_id),
    cwd: asString(row.cwd),
    state: asState(row.state),
    resultText: asNullableString(row.result_text),
    errorText: asNullableString(row.error_text),
    messageSeq: asNumber(row.message_seq),
    historyEligible: asNumber(row.history_eligible) !== 0,
  };
}

export function outboxFromRow(row: SqlRow): OutboxRecord {
  const kind = row.kind === "file" ? "file" : "text";
  const fileKind = row.file_kind === "image" || row.file_kind === "file" ? row.file_kind : null;
  const state = row.state === "sent" || row.state === "unknown" || row.state === "dead" ? row.state : "pending";
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    taskId: asNullableString(row.task_id),
    kind,
    text: asNullableString(row.text),
    path: asNullableString(row.path),
    fileKind,
    clientMsgNo: asString(row.client_msg_no),
    state,
    attempts: asNumber(row.attempts),
    lastError: asNullableString(row.last_error),
    remoteMessageId: asNullableString(row.remote_message_id),
    streamMessageId: asNullableString(row.stream_message_id),
    segmentIndex: asNumber(row.segment_index),
    desiredVersion: asNumber(row.desired_version),
    ackedVersion: asNumber(row.acked_version),
  };
}

export function questionFromRow(row: SqlRow): QuestionRecord {
  const state = row.state === "answered" || row.state === "cancelled" || row.state === "expired" ? row.state : "pending";
  return {
    id: asString(row.id),
    taskId: asString(row.task_id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    payload: asString(row.payload),
    state,
    answer: asNullableString(row.answer),
    answeredBy: asNullableString(row.answered_by),
    cardMessageId: asNullableString(row.card_message_id),
  };
}
