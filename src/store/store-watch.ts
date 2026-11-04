import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type {
  WatchCheckCompletion,
  WatchCreateResult,
  WatchListItem,
  WatchRecord,
  WatchSpec,
} from "../watch/types.ts";
import { asNumber, asString, query, run, targetValues, targetWhere, transaction, type SqlValue } from "./store-schema.ts";

const ACTIVE_WATCH_SQL = "(state IN ('waiting', 'checking', 'unknown') OR process_json IS NOT NULL OR process_unknown = 1 OR (state = 'matched' AND wake_status IN ('queued', 'accepted')))";

function cancelUnstartedWakeTask(
  db: Database,
  botUid: string,
  target: ChatTarget,
  watchId: string,
  now: number,
  reason: string,
  taskId?: string,
): void {
  const taskClause = taskId === undefined ? "" : " AND id = ?";
  const values: SqlValue[] = [
    now,
    reason,
    botUid,
    target.channelType,
    target.channelId,
    `watch:${watchId}`,
  ];
  if (taskId !== undefined) values.push(taskId);
  run(db,
    `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ?, stop_intent = NULL
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?
       AND (state = 'queued' OR (state = 'started' AND prompt_started = 0))${taskClause}`,
    values,
  );
}

export interface WatchMatchEvent {
  messageId: string;
  watchId: string;
  botUid: string;
  target: ChatTarget;
  createdAt: number;
}

export function createWatch(db: Database, record: WatchRecord): WatchCreateResult {
  return transaction(db, () => {
    if (isWatchChatDeleted(db, record.botUid, record.target)) return { kind: "deleted", record: null };

    const duplicate = query(db,
      "SELECT * FROM watches WHERE bot_uid = ? AND source_task_id = ? AND creation_key = ?",
      [record.botUid, record.sourceTaskId, record.creationKey],
    )[0];
    if (duplicate) return { kind: "duplicate", record: watchFromRow(duplicate) };

    if (query(db,
      `SELECT 1 FROM watches WHERE bot_uid = ? AND ${targetWhere(record.target)} AND ${ACTIVE_WATCH_SQL} LIMIT 1 OFFSET 4`,
      [record.botUid, ...targetValues(record.target)],
    ).length > 0) return { kind: "quota-chat", record: null };

    if (query(db,
      `SELECT 1 FROM watches WHERE bot_uid = ? AND ${ACTIVE_WATCH_SQL} LIMIT 1 OFFSET 19`,
      [record.botUid],
    ).length > 0) return { kind: "quota-bot", record: null };

    const idCollision = query(db, "SELECT 1 FROM watches WHERE id = ?", [record.id])[0];
    if (idCollision) throw new Error("watch id already exists");

    run(db,
      `INSERT INTO watches(
        id, bot_uid, channel_type, channel_id, source_task_id, creation_key, spec_json,
        created_at, deadline_at, next_at, checks, max_checks, generation, failures, state,
        latest_json, matched_evidence, process_json, wake_task_id, wake_status, stop_reason, matched_at
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.id,
        record.botUid,
        record.target.channelType,
        record.target.channelId,
        record.sourceTaskId,
        record.creationKey,
        JSON.stringify(record.spec),
        record.createdAt,
        record.deadlineAt,
        record.nextAt,
        record.checks,
        record.maxChecks,
        record.generation,
        record.failures,
        record.state,
        jsonOrNull(record.latest),
        record.matchedEvidence,
        jsonOrNull(record.process),
        record.wakeTaskId,
        record.wakeStatus,
        record.stopReason,
        record.matchedAt,
      ],
    );
    const inserted = getWatchById(db, record.id);
    if (!inserted) throw new Error("watch insert did not persist");
    return { kind: "created", record: inserted };
  });
}

export function claimDueWatches(db: Database, now: number, limit = 2, botUid?: string): WatchRecord[] {
  return transaction(db, () => {
    run(db,
      `UPDATE watches SET state = 'expired', stop_reason = 'deadline'
       WHERE state = 'waiting' AND deadline_at <= ?`,
      [now],
    );

    const busy = asNumber(query(db,
      `SELECT COUNT(*) AS count FROM watches WHERE state IN ('checking', 'unknown') OR process_json IS NOT NULL OR process_unknown = 1`,
    )[0]?.count);
    const available = Math.max(0, Math.min(2, Math.trunc(limit)) - busy);
    if (available === 0) return [];

    const due = botUid === undefined
      ? query(db,
        `SELECT id FROM watches
         WHERE state = 'waiting' AND next_at <= ? AND deadline_at > ? AND checks < max_checks
         ORDER BY next_at ASC, created_at ASC, id ASC LIMIT ?`,
        [now, now, available],
      )
      : query(db,
        `SELECT id FROM watches
         WHERE bot_uid = ? AND state = 'waiting' AND next_at <= ? AND deadline_at > ? AND checks < max_checks
         ORDER BY next_at ASC, created_at ASC, id ASC LIMIT ?`,
        [botUid, now, now, available],
      );
    const claimed: WatchRecord[] = [];
    for (const row of due) {
      const id = asString(row.id);
      run(db,
        `UPDATE watches SET state = 'checking', checks = checks + 1,
         generation = generation + 1, next_at = ?, latest_json = NULL
         WHERE id = ? AND state = 'waiting'`,
        [now, id],
      );
      const record = getWatchById(db, id);
      if (record?.state === "checking") claimed.push(record);
    }
    return claimed;
  });
}

export function setWatchProcess(
  db: Database,
  id: string,
  generation: number,
  process: WatchRecord["process"],
): boolean {
  if (process !== null) {
    return run(db,
      `UPDATE watches SET process_json = ?, process_unknown = 0
       WHERE id = ? AND generation = ? AND state = 'checking' AND process_json IS NULL`,
      [JSON.stringify(process), id, generation],
    ).changes === 1;
  }
  return run(db,
    `UPDATE watches SET process_json = NULL, process_unknown = 0 WHERE id = ? AND generation = ? AND process_json IS NOT NULL`,
    [id, generation],
  ).changes === 1;
}

export function markWatchUnknown(db: Database, id: string, generation: number, reason: string, now: number): boolean {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record || record.generation !== generation) return false;
    if (record.state === "checking") {
      return run(db,
        `UPDATE watches SET state = 'unknown', stop_reason = ?, latest_json = ?,
         process_unknown = CASE WHEN process_json IS NULL THEN 1 ELSE process_unknown END
         WHERE id = ? AND generation = ? AND state = 'checking'`,
        [reason, JSON.stringify({ kind: "failure", collectedAt: now, code: "process-exit-unconfirmed" }), id, generation],
      ).changes === 1;
    }
    if (record.process === null && (record.state === "unknown" || record.state === "stopped" ||
        record.state === "expired" || record.state === "deleted")) {
      return run(db,
        `UPDATE watches SET process_unknown = 1
         WHERE id = ? AND generation = ? AND process_json IS NULL AND state IN ('unknown', 'stopped', 'expired', 'deleted')`,
        [id, generation],
      ).changes === 1;
    }
    return false;
  });
}
export function recoverWatchProcess(db: Database, id: string, generation: number, now: number): WatchRecord | null {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record || record.generation !== generation || record.process !== null) return null;
    if (record.state !== "checking" && record.state !== "unknown") return record;

    const failures = record.failures + 1;
    const nextAt = Math.max(record.nextAt + record.spec.intervalMs, now);
    const failed = failures >= 3;
    const expired = now >= record.deadlineAt;
    const state = expired ? "expired" : failed ? "failed" : "waiting";
    run(db,
      `UPDATE watches SET state = ?, failures = ?, latest_json = ?, next_at = ?, stop_reason = ?
       WHERE id = ? AND generation = ? AND state IN ('checking', 'unknown') AND process_json IS NULL AND process_unknown = 0`,
      [
        state,
        failures,
        JSON.stringify({ kind: "failure", collectedAt: now, code: "interrupted-by-restart" }),
        nextAt,
        expired ? "deadline" : failed ? "three-consecutive-failures" : null,
        id,
        generation,
      ],
    );
    return getWatchById(db, id);
  });
}

export function expireWatchRecords(db: Database, now: number): void {
  transaction(db, () => {
    const matchedWakeRows = query(db,
      `SELECT id, bot_uid, channel_type, channel_id FROM watches
       WHERE state = 'matched' AND wake_status IN ('queued', 'accepted') AND deadline_at <= ?`,
      [now],
    );
    run(db,
      `UPDATE watches SET state = 'expired', wake_status = CASE
         WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END,
       stop_reason = 'deadline'
       WHERE (state = 'waiting' AND deadline_at <= ?)
         OR (state = 'matched' AND wake_status IN ('queued', 'accepted') AND deadline_at <= ?)
         OR (state IN ('checking', 'unknown') AND deadline_at <= ?)`,
      [now, now, now],
    );
    for (const row of matchedWakeRows) {
      cancelUnstartedWakeTask(
        db,
        asString(row.bot_uid),
        { channelType: asNumber(row.channel_type) as ChatTarget["channelType"], channelId: asString(row.channel_id) },
        asString(row.id),
        now,
        "watch deadline",
      );
    }
  });
}

export function listActiveWatchRecords(db: Database, botUid?: string): WatchRecord[] {
  const rows = botUid === undefined
    ? query(db, `SELECT * FROM watches WHERE ${ACTIVE_WATCH_SQL} ORDER BY next_at ASC, created_at ASC, id ASC`)
    : query(db, `SELECT * FROM watches WHERE bot_uid = ? AND ${ACTIVE_WATCH_SQL} ORDER BY next_at ASC, created_at ASC, id ASC`, [botUid]);
  return rows.map(watchFromRow);
}


export function finishWatchCheck(
  db: Database,
  id: string,
  generation: number,
  completion: WatchCheckCompletion,
  finishedAt: number,
): WatchRecord | null {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record || record.state !== "checking" || record.generation !== generation || record.process !== null) return null;

    const latestJson = JSON.stringify(completion);
    if (finishedAt >= record.deadlineAt) {
      run(db,
        `UPDATE watches SET state = 'expired', latest_json = ?, stop_reason = 'deadline'
         WHERE id = ? AND generation = ? AND state = 'checking' AND process_json IS NULL`,
        [latestJson, id, generation],
      );
      return null;
    }

    if (completion.kind === "failure") {
      const failures = record.failures + 1;
      const exhausted = failures >= 3;
      const nextAt = Math.max(record.nextAt + record.spec.intervalMs, finishedAt);
      run(db,
        `UPDATE watches SET state = ?, failures = ?, latest_json = ?, next_at = ?,
         stop_reason = ?, process_json = NULL, process_unknown = 0
         WHERE id = ? AND generation = ? AND state = 'checking' AND process_json IS NULL`,
        [
          exhausted ? "failed" : "waiting",
          failures,
          latestJson,
          nextAt,
          exhausted ? "three-consecutive-failures" : null,
          id,
          generation,
        ],
      );
    } else {
      const matched = completion.choice === "satisfied" && completion.probability >= 0.95;
      const nextAt = Math.max(record.nextAt + record.spec.intervalMs, finishedAt);
      run(db,
        `UPDATE watches SET state = ?, failures = 0, latest_json = ?, matched_evidence = ?,
         matched_at = ?, next_at = ?, stop_reason = ?, process_json = NULL, process_unknown = 0,
         wake_status = CASE WHEN ? THEN 'queued' ELSE wake_status END
         WHERE id = ? AND generation = ? AND state = 'checking' AND process_json IS NULL`,
        [
          matched ? "matched" : "waiting",
          latestJson,
          matched ? completion.stdout : null,
          matched ? finishedAt : null,
          nextAt,
          null,
          matched ? 1 : 0,
          id,
          generation,
        ],
      );
      if (matched) {
        const current = getWatchById(db, id);
        if (current?.state === "matched") {
          run(db,
            `INSERT OR IGNORE INTO watch_match_events(message_id, watch_id, bot_uid, channel_type, channel_id, created_at)
             VALUES(?, ?, ?, ?, ?, ?)`,
            [`watch:${id}`, id, current.botUid, current.target.channelType, current.target.channelId, finishedAt],
          );
        }
      }
    }
    return getWatchById(db, id);
  });
}

export function stopWatches(
  db: Database,
  botUid: string,
  target: ChatTarget,
  id: string | "all",
  now: number,
  reason: string,
): WatchRecord[] {
  return transaction(db, () => {
    const idClause = id === "all" ? "" : " AND id = ?";
    const values: SqlValue[] = [botUid, ...targetValues(target)];
    if (id !== "all") values.push(id);
    const rows = query(db,
      `SELECT id FROM watches WHERE bot_uid = ? AND ${targetWhere(target)}${idClause}
       AND (${ACTIVE_WATCH_SQL} OR (state = 'matched' AND wake_status = 'dispatched'))
       ORDER BY created_at ASC, id ASC`,
      values,
    );
    const stopped: WatchRecord[] = [];
    for (const row of rows) {
      const watchId = asString(row.id);
      run(db,
        `UPDATE watches SET state = 'stopped', wake_status = CASE
           WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END,
         stop_reason = ?, next_at = ? WHERE id = ? AND ${ACTIVE_WATCH_SQL}`,
        [reason, now, watchId],
      );
      cancelUnstartedWakeTask(db, botUid, target, watchId, now, reason);
      const record = getWatchById(db, watchId);
      if (record) stopped.push(record);
    }
    return stopped;
  });
}

export function deleteWatchChat(db: Database, botUid: string, target: ChatTarget, now: number): void {
  transaction(db, () => {
    run(db,
      `INSERT INTO watch_chat_tombstones(bot_uid, channel_type, channel_id, deleted_at)
       VALUES(?, ?, ?, ?) ON CONFLICT(bot_uid, channel_type, channel_id)
       DO UPDATE SET deleted_at = MIN(deleted_at, excluded.deleted_at)`,
      [botUid, target.channelType, target.channelId, now],
    );
    const watchIds = query(db,
      `SELECT id FROM watches WHERE bot_uid = ? AND ${targetWhere(target)}`,
      [botUid, ...targetValues(target)],
    ).map((row) => asString(row.id));
    run(db,
      `UPDATE watches SET state = 'deleted', wake_status = CASE
         WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END,
       stop_reason = 'chat-deleted', next_at = ? WHERE bot_uid = ? AND ${targetWhere(target)}`,
      [now, botUid, ...targetValues(target)],
    );
    for (const watchId of watchIds) {
      cancelUnstartedWakeTask(db, botUid, target, watchId, now, "chat-deleted");
    }
  });
}

export function isWatchChatDeleted(db: Database, botUid: string, target: ChatTarget): boolean {
  return query(db,
    `SELECT 1 FROM watch_chat_tombstones WHERE bot_uid = ? AND ${targetWhere(target)} LIMIT 1`,
    [botUid, ...targetValues(target)],
  ).length > 0;
}

export function getWatch(db: Database, botUid: string, id: string): WatchRecord | null {
  const row = query(db, "SELECT * FROM watches WHERE bot_uid = ? AND id = ?", [botUid, id])[0];
  return row ? watchFromRow(row) : null;
}

export function listWatches(db: Database, botUid: string, target: ChatTarget, limit: number, offset: number): WatchListItem[] {
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  const safeOffset = Math.max(0, Math.trunc(offset));
  return query(db,
    `SELECT * FROM watches WHERE bot_uid = ? AND ${targetWhere(target)} ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?`,
    [botUid, ...targetValues(target), safeLimit, safeOffset],
  ).map((row) => {
    const { spec: _spec, latest: _latest, matchedEvidence: _evidence, process: _process, ...summary } = watchFromRow(row);
    return summary;
  });
}

export function listWatchRecords(db: Database, botUid?: string): WatchRecord[] {
  const rows = botUid === undefined
    ? query(db, "SELECT * FROM watches ORDER BY created_at ASC, id ASC")
    : query(db, "SELECT * FROM watches WHERE bot_uid = ? ORDER BY created_at ASC, id ASC", [botUid]);
  return rows.map(watchFromRow);
}

export function listWatchProcessRecords(db: Database, botUid?: string): WatchRecord[] {
  const rows = botUid === undefined
    ? query(db, "SELECT * FROM watches WHERE process_json IS NOT NULL OR process_unknown = 1 ORDER BY created_at ASC, id ASC")
    : query(db, "SELECT * FROM watches WHERE bot_uid = ? AND (process_json IS NOT NULL OR process_unknown = 1) ORDER BY created_at ASC, id ASC", [botUid]);
  return rows.map(watchFromRow);
}

export function wakeAllowed(db: Database, id: string, now: number): boolean {
  const record = getWatchById(db, id);
  return !!record && record.state === "matched" && record.deadlineAt > now &&
    (record.wakeStatus === "queued" || record.wakeStatus === "accepted") &&
    !isWatchChatDeleted(db, record.botUid, record.target);
}

export function markWatchWakeTask(db: Database, id: string, taskId: string, now = Date.now()): boolean {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record) return false;
    if (record.state !== "matched") {
      cancelUnstartedWakeTask(db, record.botUid, record.target, id, now, "watch is no longer active", taskId);
      return false;
    }
    const chatDeleted = isWatchChatDeleted(db, record.botUid, record.target);
    if (record.wakeStatus === "accepted") {
      if (record.deadlineAt <= now || chatDeleted) {
        const state = chatDeleted ? "deleted" : "expired";
        const reason = chatDeleted ? "chat-deleted" : "deadline";
        run(db,
          `UPDATE watches SET state = ?, wake_status = 'cancelled', stop_reason = ?
           WHERE id = ? AND state = 'matched' AND wake_status = 'accepted'`,
          [state, reason, id],
        );
        cancelUnstartedWakeTask(db, record.botUid, record.target, id, now, reason);
        return false;
      }
      if (record.wakeTaskId === taskId) return true;
      cancelUnstartedWakeTask(db, record.botUid, record.target, id, now, "watch task mismatch", taskId);
      return false;
    }
    if (record.wakeStatus !== "queued" || record.wakeTaskId !== null) return false;
    if (record.deadlineAt <= now || chatDeleted) {
      const state = chatDeleted ? "deleted" : "expired";
      const reason = chatDeleted ? "chat-deleted" : "deadline";
      run(db,
        `UPDATE watches SET state = ?, wake_status = 'cancelled', stop_reason = ?
         WHERE id = ? AND state = 'matched' AND wake_status = 'queued'`,
        [state, reason, id],
      );
      cancelUnstartedWakeTask(db, record.botUid, record.target, id, now, reason);
      return false;
    }
    return run(db,
      `UPDATE watches SET wake_task_id = ?, wake_status = 'accepted'
       WHERE id = ? AND state = 'matched' AND wake_status = 'queued' AND wake_task_id IS NULL`,
      [taskId, id],
    ).changes === 1;
  });
}

export function markWatchWakeDispatched(db: Database, id: string, taskId: string, now: number): boolean {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record || record.state !== "matched" || record.wakeStatus !== "accepted" || record.wakeTaskId !== taskId) return false;
    const chatDeleted = isWatchChatDeleted(db, record.botUid, record.target);
    if (record.deadlineAt <= now || chatDeleted) {
      const state = chatDeleted ? "deleted" : "expired";
      const reason = chatDeleted ? "chat-deleted" : "deadline";
      run(db,
        `UPDATE watches SET state = ?, wake_status = 'cancelled', stop_reason = ?
         WHERE id = ? AND state = 'matched' AND wake_status = 'accepted' AND wake_task_id = ?`,
        [state, reason, id, taskId],
      );
      cancelUnstartedWakeTask(db, record.botUid, record.target, id, now, reason);
      return false;
    }
    return run(db,
      `UPDATE watches SET wake_status = 'dispatched' WHERE id = ? AND state = 'matched'
       AND wake_status = 'accepted' AND wake_task_id = ?`,
      [id, taskId],
    ).changes === 1;
  });
}

export function markWatchWakeFailed(db: Database, id: string, reason: string): boolean {
  return transaction(db, () => {
    const record = getWatchById(db, id);
    if (!record || record.state !== "matched") return false;
    const changed = run(db,
      `UPDATE watches SET wake_status = 'failed', stop_reason = ?
       WHERE id = ? AND state = 'matched' AND wake_status IN ('queued', 'accepted')`,
      [reason, id],
    ).changes === 1;
    if (changed) cancelUnstartedWakeTask(db, record.botUid, record.target, id, Date.now(), reason);
    return changed;
  });
}

export function getWatchMatchEvent(db: Database, id: string): WatchMatchEvent | null {
  const row = query(db, "SELECT * FROM watch_match_events WHERE watch_id = ?", [id])[0];
  if (!row) return null;
  return {
    messageId: asString(row.message_id),
    watchId: asString(row.watch_id),
    botUid: asString(row.bot_uid),
    target: {
      channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
      channelId: asString(row.channel_id),
    },
    createdAt: asNumber(row.created_at),
  };
}

function getWatchById(db: Database, id: string): WatchRecord | null {
  const row = query(db, "SELECT * FROM watches WHERE id = ?", [id])[0];
  return row ? watchFromRow(row) : null;
}

function watchFromRow(row: Record<string, unknown>): WatchRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    target: {
      channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
      channelId: asString(row.channel_id),
    },
    sourceTaskId: asString(row.source_task_id),
    creationKey: asString(row.creation_key),
    spec: parseJson<WatchSpec>(row.spec_json),
    createdAt: asNumber(row.created_at),
    deadlineAt: asNumber(row.deadline_at),
    nextAt: asNumber(row.next_at),
    checks: asNumber(row.checks),
    maxChecks: asNumber(row.max_checks),
    generation: asNumber(row.generation),
    failures: asNumber(row.failures),
    state: isWatchState(row.state) ? row.state : "failed",
    latest: parseNullableJson<WatchCheckCompletion>(row.latest_json),
    matchedEvidence: typeof row.matched_evidence === "string" ? row.matched_evidence : null,
    process: parseNullableJson<WatchRecord["process"]>(row.process_json),
    wakeTaskId: typeof row.wake_task_id === "string" ? row.wake_task_id : null,
    wakeStatus: isWakeStatus(row.wake_status) ? row.wake_status : "failed",
    stopReason: typeof row.stop_reason === "string" ? row.stop_reason : null,
    matchedAt: row.matched_at === null || row.matched_at === undefined ? null : asNumber(row.matched_at),
  };
}

function parseJson<T>(value: unknown): T {
  if (typeof value !== "string") throw new Error("persisted watch JSON is invalid");
  return JSON.parse(value) as T;
}

function parseNullableJson<T>(value: unknown): T | null {
  if (typeof value !== "string") return null;
  return JSON.parse(value) as T;
}

function jsonOrNull(value: unknown): string | null {
  return value === null ? null : JSON.stringify(value);
}

function isWatchState(value: unknown): value is WatchRecord["state"] {
  return value === "waiting" || value === "checking" || value === "matched" || value === "failed" ||
    value === "expired" || value === "stopped" || value === "deleted" || value === "unknown";
}

function isWakeStatus(value: unknown): value is WatchRecord["wakeStatus"] {
  return value === "none" || value === "queued" || value === "accepted" || value === "dispatched" ||
    value === "cancelled" || value === "failed";
}
