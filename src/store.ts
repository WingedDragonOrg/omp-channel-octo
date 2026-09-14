import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ChatTarget, ChatExecutionState, WorkerProcess } from "./contracts.ts";

export interface BindingRecord extends ChatTarget {
  botUid: string;
  sessionId: string;
  cwd: string;
  sessionFile: string | null;
}

export interface InboundClaim {
  botUid: string;
  target: ChatTarget;
  messageId: string;
  senderId: string;
  text: string;
  supported: boolean;
  kind: "task" | "command";
}

export interface TaskRecord extends ChatTarget {
  id: string;
  botUid: string;
  messageId: string;
  senderId: string;
  text: string;
  sessionId: string;
  cwd: string;
  state: ChatExecutionState;
  resultText: string | null;
  errorText: string | null;
}
export interface OutboxRecord extends ChatTarget {
  id: string;
  botUid: string;
  taskId: string | null;
  kind: "text" | "file";
  text: string | null;
  path: string | null;
  fileKind: "image" | "file" | null;
  clientMsgNo: string;
  state: "pending" | "unknown" | "sent" | "dead";
  attempts: number;
  lastError: string | null;
  remoteMessageId: string | null;
  streamMessageId: string | null;
  segmentIndex: number;
  desiredVersion: number;
  ackedVersion: number;
}

export type OutboxInsert = Omit<
  OutboxRecord,
  "state" | "attempts" | "lastError" | "remoteMessageId" | "streamMessageId" | "segmentIndex" | "desiredVersion" | "ackedVersion"
> & {
  remoteMessageId?: string | null;
  streamMessageId?: string | null;
  segmentIndex?: number;
  desiredVersion?: number;
  ackedVersion?: number;
};

export interface QuestionRecord extends ChatTarget {
  id: string;
  botUid: string;
  taskId: string;
  payload: string;
  state: "pending" | "answered" | "cancelled" | "expired";
  answer: string | null;
}

export interface WorkerLedgerRecord extends WorkerProcess {
  id: string;
  botUid: string;
  sessionId: string;
  sessionFile: string;
  moveTargetCwd: string | null;
  state: "running" | "stopping" | "stopped" | "unknown";
}

type SqlValue = string | number | null | Uint8Array;
type SqlRow = Record<string, unknown>;

function targetWhere(target: ChatTarget): string {
  return "channel_type = ? AND channel_id = ?";
}

function targetValues(target: ChatTarget): SqlValue[] {
  return [target.channelType, target.channelId];
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number {
  return typeof value === "number" ? value : Number(value ?? 0);
}

function asState(value: unknown): ChatExecutionState {
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

export class ChannelStore {
  private readonly db: Database;

  private constructor(readonly path: string) {
    this.db = new Database(path);
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    this.db.exec(`
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
      CREATE TABLE IF NOT EXISTS chats (
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cwd TEXT NOT NULL,
        session_file TEXT,
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
        error_text TEXT
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
    `);
    for (const statement of [
      "ALTER TABLE outbox ADD COLUMN remote_message_id TEXT",
      "ALTER TABLE outbox ADD COLUMN stream_message_id TEXT",
      "ALTER TABLE outbox ADD COLUMN segment_index INTEGER NOT NULL DEFAULT 0",
      "ALTER TABLE outbox ADD COLUMN desired_version INTEGER NOT NULL DEFAULT 0",
      "ALTER TABLE outbox ADD COLUMN acked_version INTEGER NOT NULL DEFAULT 0",
      "ALTER TABLE worker_ledger ADD COLUMN move_target_cwd TEXT",
    ]) {
      try {
        this.db.exec(statement);
      } catch (error) {
        if (!String(error).includes("duplicate column")) throw error;
      }
    }
    this.db.exec(
      "CREATE INDEX IF NOT EXISTS outbox_stream ON outbox(bot_uid, task_id, stream_message_id, segment_index)",
    );
  }

  static async open(path: string): Promise<ChannelStore> {
    await mkdir(dirname(path), { recursive: true });
    return new ChannelStore(path);
  }

  close(): void {
    this.db.close(false);
  }

  private query(sql: string, values: readonly SqlValue[] = []): SqlRow[] {
    return this.db.query(sql).all(...values) as SqlRow[];
  }
  private run(sql: string, values: readonly SqlValue[] = []): { changes: number } {
    const result = this.db.query(sql).run(...values) as { changes?: number };
    return { changes: result.changes ?? 0 };
  }

  private transaction<T>(work: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        // Preserve the original database failure.
      }
      throw error;
    }
  }

  setBotUid(botUid: string): void {
    this.run(
      "INSERT INTO meta(key, value) VALUES('bot_uid', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      [botUid],
    );
  }

  getBotUid(): string | null {
    const row = this.query("SELECT value FROM meta WHERE key = 'bot_uid'")[0];
    return row ? asNullableString(row.value) : null;
  }

  getOwnerId(botUid: string): string | null {
    const row = this.query("SELECT owner_id FROM owners WHERE bot_uid = ?", [botUid])[0];
    return row ? asNullableString(row.owner_id) : null;
  }

  setOwnerId(botUid: string, ownerId: string): void {
    const now = Date.now();
    this.run(
      `INSERT INTO owners(bot_uid, owner_id, created_at, updated_at)
       VALUES(?, ?, ?, ?)
       ON CONFLICT(bot_uid) DO UPDATE SET owner_id = excluded.owner_id, updated_at = excluded.updated_at`,
      [botUid, ownerId, now, now],
    );
  }

  hasOwnerEnrollment(botUid: string, target: ChatTarget, messageId: string): boolean {
    const row = this.query(
      `SELECT 1 FROM owner_enrollments
       WHERE bot_uid = ? AND ${targetWhere(target)} AND message_id = ?`,
      [botUid, ...targetValues(target), messageId],
    )[0];
    return Boolean(row);
  }

  claimOwnerEnrollment(
    botUid: string,
    target: ChatTarget,
    messageId: string,
    senderId: string,
    text: string,
    outbox: OutboxInsert,
  ): boolean {
    return this.transaction(() => {
      const existingEnrollment = this.query(
        `SELECT 1 FROM owner_enrollments
         WHERE bot_uid = ? AND ${targetWhere(target)} AND message_id = ?`,
        [botUid, ...targetValues(target), messageId],
      )[0];
      if (existingEnrollment) return false;
      const existingOwner = this.query("SELECT 1 FROM owners WHERE bot_uid = ?", [botUid])[0];
      if (existingOwner) return false;
      const now = Date.now();
      this.run(
        "INSERT INTO owners(bot_uid, owner_id, created_at, updated_at) VALUES(?, ?, ?, ?)",
        [botUid, senderId, now, now],
      );
      this.run(
        `INSERT INTO owner_enrollments(
           bot_uid, channel_type, channel_id, message_id, sender_id, text, accepted_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
        [botUid, target.channelType, target.channelId, messageId, senderId, text, now],
      );
      this.enqueueOutbox(outbox);
      return true;
    });
  }

  ensureBinding(binding: BindingRecord): void {
    const now = Date.now();
    this.transaction(() => {
      this.run(
        `INSERT INTO chats(bot_uid, channel_type, channel_id, session_id, cwd, session_file, created_at, updated_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET
           session_id = excluded.session_id,
           cwd = excluded.cwd,
           session_file = excluded.session_file,
           updated_at = excluded.updated_at`,
        [
          binding.botUid,
          binding.channelType,
          binding.channelId,
          binding.sessionId,
          binding.cwd,
          binding.sessionFile,
          now,
          now,
        ],
      );
    });
  }

  getBinding(botUid: string, target: ChatTarget): BindingRecord | null {
    const row = this.query(
      `SELECT bot_uid, channel_type, channel_id, session_id, cwd, session_file
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
    };
  }

  acceptTask(claim: InboundClaim, task: TaskRecord): boolean {
    return this.transaction(() => {
      const existing = this.query(
        `SELECT 1 FROM inbound WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
        [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
      )[0];
      if (existing) return false;
      const now = Date.now();
      this.run(
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
      this.run(
        `INSERT INTO tasks(id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd, state, accepted_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)`,
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
        ],
      );
      return true;
    });
  }

  claimCommand(claim: InboundClaim): boolean {
    return this.transaction(() => {
      const existing = this.query(
        `SELECT kind, command_state FROM inbound
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
        [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
      )[0];
      if (existing) {
        if (existing.kind !== "command" || existing.command_state === "completed" || existing.command_state === "pending") {
          return false;
        }
        this.run(
          `UPDATE inbound SET command_state = 'pending'
           WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
          [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
        );
        return true;
      }
      this.run(
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

  completeCommand(claim: InboundClaim): void {
    this.run(
      `UPDATE inbound SET command_state = 'completed'
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    );
  }

  failCommand(claim: InboundClaim): void {
    this.run(
      `UPDATE inbound SET command_state = 'failed'
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    );
  }

  getTask(taskId: string): TaskRecord | null {
    const row = this.query("SELECT * FROM tasks WHERE id = ?", [taskId])[0];
    return row ? this.taskFromRow(row) : null;
  }

  getLatestTask(botUid: string, target: ChatTarget): TaskRecord | null {
    const row = this.query(
      `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} ORDER BY accepted_at DESC, rowid DESC LIMIT 1`,
      [botUid, ...targetValues(target)],
    )[0];
    return row ? this.taskFromRow(row) : null;
  }

  getActiveTask(botUid: string, target: ChatTarget): TaskRecord | null {
    const row = this.query(
      `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('queued', 'started')
       ORDER BY accepted_at ASC, rowid ASC LIMIT 1`,
      [botUid, ...targetValues(target)],
    )[0];
    return row ? this.taskFromRow(row) : null;
  }

  countQueuedTasks(botUid: string, target: ChatTarget): number {
    const row = this.query(
      `SELECT COUNT(*) AS count FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [botUid, ...targetValues(target)],
    )[0];
    return asNumber(row?.count);
  }

  listQueuedTasks(): TaskRecord[] {
    return this.query(
      "SELECT * FROM tasks WHERE state = 'queued' ORDER BY accepted_at ASC, rowid ASC",
    ).map((row) => this.taskFromRow(row));
  }

  markStarted(taskId: string): boolean {
    return this.run(
      "UPDATE tasks SET state = 'started', started_at = ? WHERE id = ? AND state = 'queued'",
      [Date.now(), taskId],
    ).changes === 1;
  }

  markCompleted(taskId: string, resultText: string | null): void {
    this.run(
      "UPDATE tasks SET state = 'completed', finished_at = ?, result_text = ?, error_text = NULL WHERE id = ?",
      [Date.now(), resultText, taskId],
    );
  }


  markFailed(taskId: string, errorText: string): void {
    this.run(
      "UPDATE tasks SET state = 'failed', finished_at = ?, error_text = ?, result_text = NULL WHERE id = ?",
      [Date.now(), errorText, taskId],
    );
  }

  markCancelled(taskId: string, errorText = "cancelled"): void {
    this.run(
      "UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ? WHERE id = ? AND state IN ('queued', 'started')",
      [Date.now(), errorText, taskId],
    );
  }

  markInterruptedOnStart(botUid: string): TaskRecord[] {
    return this.transaction(() => {
      const rows = this.query(
        "SELECT * FROM tasks WHERE bot_uid = ? AND state IN ('queued', 'started')",
        [botUid],
      );
      this.run(
        "UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = 'service restarted before task completion' WHERE bot_uid = ? AND state IN ('queued', 'started')",
        [Date.now(), botUid],
      );
      this.run(
        "UPDATE questions SET state = 'cancelled', answered_at = ? WHERE bot_uid = ? AND state = 'pending'",
        [Date.now(), botUid],
      );
      return rows.map((row) => this.taskFromRow(row));
    });
  }

  cancelQueued(botUid: string, target: ChatTarget): string[] {
    return this.transaction(() => {
      const rows = this.query(
        `SELECT id FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
        [botUid, ...targetValues(target)],
      );
      this.run(
        `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = 'cancelled before start'
         WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
        [Date.now(), botUid, ...targetValues(target)],
      );
      return rows.map((row) => asString(row.id));
    });
  }

  recordWorker(worker: WorkerLedgerRecord): void {
    this.run(
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

  markWorkerMoveTarget(id: string, cwd: string): void {
    this.run(
      "UPDATE worker_ledger SET move_target_cwd = ?, updated_at = ? WHERE id = ?",
      [cwd, Date.now(), id],
    );
  }

  listWorkerLedger(botUid: string): WorkerLedgerRecord[] {
    return this.query("SELECT * FROM worker_ledger WHERE bot_uid = ? AND state != 'stopped'", [botUid]).map(
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
  updateBindingSession(botUid: string, target: ChatTarget, sessionId: string, sessionFile: string | null): void {
    this.run(
      `UPDATE chats SET session_id = ?, session_file = ?, updated_at = ? WHERE bot_uid = ? AND ${targetWhere(target)}`,
      [sessionId, sessionFile, Date.now(), botUid, ...targetValues(target)],
    );
  }

  markWorkerStopped(id: string): void {
    this.run("UPDATE worker_ledger SET state = 'stopped', updated_at = ? WHERE id = ?", [Date.now(), id]);
  }


  enqueueOutbox(record: OutboxInsert): void {
    this.run(
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
  getOutbox(id: string): OutboxRecord | null {
    const row = this.query("SELECT * FROM outbox WHERE id = ?", [id])[0];
    return row ? this.outboxFromRow(row) : null;
  }

  beginTextCreate(id: string): boolean {
    return this.transaction(() => {
      const row = this.query(
        "SELECT kind, remote_message_id, state FROM outbox WHERE id = ?",
        [id],
      )[0];
      if (!row || row.kind !== "text" || asNullableString(row.remote_message_id)) return false;
      if (row.state === "unknown") return true;
      if (row.state !== "pending") return false;
      return this.run(
        "UPDATE outbox SET state = 'unknown', last_error = NULL WHERE id = ? AND state = 'pending' AND remote_message_id IS NULL",
        [id],
      ).changes === 1;
    });
  }

  listTextStream(botUid: string, taskId: string, streamMessageId: string): OutboxRecord[] {
    return this.query(
      `SELECT * FROM outbox
       WHERE bot_uid = ? AND task_id = ? AND stream_message_id = ?
       ORDER BY segment_index ASC, created_at ASC, rowid ASC`,
      [botUid, taskId, streamMessageId],
    ).map((row) => this.outboxFromRow(row));
  }

  updateTextOutbox(id: string, text: string, desiredVersion: number): boolean {
    return this.transaction(() => {
      const row = this.query(
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
      this.run(
        `UPDATE outbox
         SET text = ?, desired_version = ?, acked_version = ?, state = ?, last_error = NULL
         WHERE id = ? AND desired_version < ?`,
        [text, desiredVersion, currentAcked, nextState, id, desiredVersion],
      );
      return true;
    });
  }

  bindTextOutbox(id: string, remoteMessageId: string): boolean {
    const normalizedRemoteMessageId = remoteMessageId.trim();
    if (normalizedRemoteMessageId.length === 0) return false;
    return this.transaction(() => {
      const row = this.query(
        "SELECT kind, remote_message_id, desired_version, acked_version FROM outbox WHERE id = ?",
        [id],
      )[0];
      if (!row || row.kind !== "text") return false;
      const currentRemoteMessageId = asNullableString(row.remote_message_id);
      if (currentRemoteMessageId && currentRemoteMessageId !== normalizedRemoteMessageId) return false;
      const desiredVersion = asNumber(row.desired_version);
      const ackedVersion = asNumber(row.acked_version);
      const state = ackedVersion >= desiredVersion ? "sent" : "pending";
      this.run(
        `UPDATE outbox
         SET remote_message_id = ?, state = ?, last_error = NULL,
             sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END
         WHERE id = ?`,
        [normalizedRemoteMessageId, state, state, Date.now(), id],
      );
      return true;
    });
  }

  acknowledgeTextOutbox(
    id: string,
    attemptedVersion: number,
    remoteMessageId: string | null,
  ): boolean {
    return this.transaction(() => {
      const row = this.query(
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
        this.query("SELECT text FROM outbox WHERE id = ?", [id])[0]?.text,
      );
      const emptyWithoutRemote = currentText === "" && !knownRemote;
      const nextState = emptyWithoutRemote || (knownRemote && nextAcked >= desiredVersion)
        ? "sent"
        : knownRemote
          ? "pending"
          : "unknown";
      this.run(
        `UPDATE outbox
         SET remote_message_id = ?, acked_version = ?, state = ?,
             last_error = NULL,
             sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END
         WHERE id = ?`,
        [nextRemote, nextAcked, nextState, nextState, Date.now(), id],
      );
      return true;
    });
  }

  failTextOutbox(id: string, attemptedVersion: number, errorText: string): boolean {
    return this.transaction(() => {
      const row = this.query(
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
      this.run(
        "UPDATE outbox SET state = ?, attempts = attempts + 1, last_error = ? WHERE id = ?",
        [state, errorText, id],
      );
      return true;
    });
  }

  listPendingOutbox(botUid: string, target?: ChatTarget): OutboxRecord[] {
    const clauses = ["bot_uid = ?", "state IN ('pending', 'unknown')"];
    const values: SqlValue[] = [botUid];
    if (target) {
      clauses.push(targetWhere(target));
      values.push(...targetValues(target));
    }
    return this.query(
      `SELECT * FROM outbox WHERE ${clauses.join(" AND ")} ORDER BY created_at ASC, rowid ASC`,
      values,
    ).map((row) => this.outboxFromRow(row));
  }

  listPendingOutboxTargets(botUid: string): ChatTarget[] {
    return this.query(
      `SELECT DISTINCT channel_type, channel_id FROM outbox WHERE bot_uid = ? AND state IN ('pending', 'unknown')`,
      [botUid],
    ).map((row) => ({
      channelType: asNumber(row.channel_type) as 1 | 2 | 5,
      channelId: asString(row.channel_id),
    }));
  }

  countPendingOutbox(botUid: string, target: ChatTarget): number {
    const row = this.query(
      `SELECT COUNT(*) AS count FROM outbox WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('pending', 'unknown')`,
      [botUid, ...targetValues(target)],
    )[0];
    return asNumber(row?.count);
  }
  countUnknownOutbox(botUid: string, target: ChatTarget): number {
    const row = this.query(
      `SELECT COUNT(*) AS count FROM outbox WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'unknown'`,
      [botUid, ...targetValues(target)],
    )[0];
    return asNumber(row?.count);
  }


  markOutboxSent(id: string): void {
    this.run("UPDATE outbox SET state = 'sent', sent_at = ?, last_error = NULL WHERE id = ?", [Date.now(), id]);
  }

  markOutboxFailure(id: string, errorText: string): void {
    this.run(
      "UPDATE outbox SET state = 'unknown', attempts = attempts + 1, last_error = ? WHERE id = ?",
      [errorText, id],
    );
  }
  markOutboxDead(id: string, errorText: string): void {
    this.run(
      "UPDATE outbox SET state = 'dead', last_error = ?, sent_at = ? WHERE id = ?",
      [errorText, Date.now(), id],
    );
  }

  createQuestion(record: Omit<QuestionRecord, "state" | "answer">): void {
    this.run(
      `INSERT INTO questions(id, task_id, bot_uid, channel_type, channel_id, payload, state, created_at)
       VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)`,
      [
        record.id,
        record.taskId,
        record.botUid,
        record.channelType,
        record.channelId,
        record.payload,
        Date.now(),
      ],
    );
  }

  getQuestion(id: string): QuestionRecord | null {
    const row = this.query("SELECT * FROM questions WHERE id = ?", [id])[0];
    return row ? this.questionFromRow(row) : null;
  }

  listPendingQuestions(botUid: string, target: ChatTarget): QuestionRecord[] {
    return this.query(
      `SELECT * FROM questions WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'pending' ORDER BY created_at ASC, rowid ASC`,
      [botUid, ...targetValues(target)],
    ).map((row) => this.questionFromRow(row));
  }

  answerQuestion(id: string, botUid: string, target: ChatTarget, answer: string): boolean {
    return this.run(
      `UPDATE questions SET state = 'answered', answer = ?, answered_at = ?
       WHERE id = ? AND bot_uid = ? AND ${targetWhere(target)} AND state = 'pending'`,
      [answer, Date.now(), id, botUid, ...targetValues(target)],
    ).changes === 1;
  }

  cancelQuestions(botUid: string, target: ChatTarget): void {
    this.run(
      `UPDATE questions SET state = 'cancelled', answered_at = ?
       WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'pending'`,
      [Date.now(), botUid, ...targetValues(target)],
    );
  }
  cancelQuestion(id: string): boolean {
    return this.run(
      "UPDATE questions SET state = 'cancelled', answered_at = ? WHERE id = ? AND state = 'pending'",
      [Date.now(), id],
    ).changes === 1;
  }

  expireQuestion(id: string): boolean {
    return this.run(
      "UPDATE questions SET state = 'expired', answered_at = ? WHERE id = ? AND state = 'pending'",
      [Date.now(), id],
    ).changes === 1;
  }

  pendingQuestionIds(botUid: string, target: ChatTarget): string[] {
    return this.listPendingQuestions(botUid, target).map(({ id }) => id);
  }

  private taskFromRow(row: SqlRow): TaskRecord {
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
    };
  }

  private outboxFromRow(row: SqlRow): OutboxRecord {
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

  private questionFromRow(row: SqlRow): QuestionRecord {
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
    };
  }
}

export async function openChannelStore(stateDir: string): Promise<ChannelStore> {
  return ChannelStore.open(`${stateDir}/channel.sqlite`);
}
