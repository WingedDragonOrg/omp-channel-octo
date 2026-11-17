import { randomUUID } from "node:crypto";
import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type {
  SourceWatchCreateResult,
  SourceWatchDecision,
  SourceWatchDecisionIntent,
  SourceWatchEventRecord,
  SourceWatchJudgmentState,
  SourceWatchListItem,
  SourceWatchRecord,
  SourceWatchSpec,
  SourceWatchState,
  SourceWatchWakeStatus,
  WatchSourceCursor,
  WatchSourceEvent,
  WatchSourceSnapshot,
} from "../watch/source-types.ts";
import { markTaskPromptStarted } from "./store-tasks.ts";
import {
  asNumber,
  asString,
  query,
  run,
  targetValues,
  targetWhere,
  transaction,
  type SqlValue,
} from "./store-schema.ts";

const ACTIVE_LEGACY_SQL = "(state IN ('waiting', 'checking', 'unknown') OR process_json IS NOT NULL OR process_unknown = 1 OR (state = 'matched' AND wake_status IN ('queued', 'accepted')))";
const ACTIVE_SOURCE_SQL = "(state IN ('initializing', 'watching') OR (state = 'budget-exhausted' AND EXISTS (SELECT 1 FROM source_watch_events e WHERE e.watch_id = source_watches.id AND (e.state IN ('pending', 'judging') OR e.wake_status IN ('queued', 'accepted')))) OR (state = 'ended' AND EXISTS (SELECT 1 FROM source_watch_events e WHERE e.watch_id = source_watches.id AND (e.state IN ('pending', 'judging') OR e.wake_status IN ('queued', 'accepted')))))";
const BASELINE_SNAPSHOT_IDENTITY = "__watch_baseline_snapshot__";

export function createSourceWatch(db: Database, record: SourceWatchRecord): SourceWatchCreateResult {
  return transaction(db, () => {
    if (isWatchChatDeleted(db, record.botUid, record)) return { kind: "deleted", record: null };

    const duplicate = query(db,
      "SELECT * FROM source_watches WHERE bot_uid = ? AND source_task_id = ? AND creation_key = ?",
      [record.botUid, record.sourceTaskId, record.creationKey],
    )[0];
    if (duplicate) return { kind: "duplicate", record: sourceWatchFromRow(db, duplicate) };

    const chatCount = activeWatchCount(db, record.botUid, record);
    if (chatCount >= 5) return { kind: "quota-chat", record: null };
    if (activeWatchCount(db, record.botUid) >= 20) return { kind: "quota-bot", record: null };
    if (query(db, "SELECT 1 FROM watches WHERE id = ? UNION ALL SELECT 1 FROM source_watches WHERE id = ? LIMIT 1", [record.id, record.id]).length > 0) {
      throw new Error("watch id already exists");
    }

    run(db,
      `INSERT INTO source_watches(
        id, bot_uid, channel_type, channel_id, source_task_id, creation_key, spec_json,
        created_at, deadline_at, state, generation, cursor_json, snapshot_json, snapshot_sequence,
        source_budget, source_used, decision_budget, decision_used, wake_budget, wake_used,
        failures, gap, latest_error, stop_reason, observed_at
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.id,
        record.botUid,
        record.channelType,
        record.channelId,
        record.sourceTaskId,
        record.creationKey,
        JSON.stringify(record.spec),
        record.createdAt,
        record.deadlineAt,
        record.state,
        record.generation,
        jsonOrNull(record.cursor),
        jsonOrNull(record.snapshot),
        record.snapshotSequence,
        record.spec.sourceBudget,
        record.sourceUsed,
        record.spec.decisionBudget,
        record.decisionUsed,
        record.spec.wakeBudget,
        record.wakeUsed,
        record.failures,
        record.gap,
        record.latestError,
        record.stopReason,
        record.observedAt,
      ],
    );
    const inserted = getSourceWatchById(db, record.id);
    if (!inserted) throw new Error("source watch insert did not persist");
    return { kind: "created", record: inserted };
  });
}

export function getSourceWatch(db: Database, botUid: string, id: string): SourceWatchRecord | null {
  const row = query(db, "SELECT * FROM source_watches WHERE bot_uid = ? AND id = ?", [botUid, id])[0];
  return row ? sourceWatchFromRow(db, row) : null;
}

export function listSourceWatches(db: Database, botUid?: string): SourceWatchRecord[] {
  const rows = botUid === undefined
    ? query(db, "SELECT * FROM source_watches ORDER BY created_at ASC, id ASC")
    : query(db, "SELECT * FROM source_watches WHERE bot_uid = ? ORDER BY created_at ASC, id ASC", [botUid]);
  return rows.map(row => sourceWatchFromRow(db, row));
}

export function listActiveSourceWatches(db: Database, botUid?: string): SourceWatchRecord[] {
  const rows = botUid === undefined
    ? query(db, `SELECT * FROM source_watches WHERE ${ACTIVE_SOURCE_SQL} ORDER BY deadline_at ASC, created_at ASC, id ASC`)
    : query(db, `SELECT * FROM source_watches WHERE bot_uid = ? AND ${ACTIVE_SOURCE_SQL} ORDER BY deadline_at ASC, created_at ASC, id ASC`, [botUid]);
  return rows.map(row => sourceWatchFromRow(db, row));
}

export function listSourceWatchesForChat(
  db: Database,
  botUid: string,
  target: ChatTarget,
  limit: number,
  offset: number,
): SourceWatchListItem[] {
  const safeLimit = Math.max(1, Math.min(100, Math.trunc(limit)));
  const safeOffset = Math.max(0, Math.trunc(offset));
  return query(db,
    `SELECT * FROM source_watches WHERE bot_uid = ? AND ${targetWhere(target)}
     ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?`,
    [botUid, ...targetValues(target), safeLimit, safeOffset],
  ).map(row => {
    const record = sourceWatchFromRow(db, row);
    const {
      cursor: _cursor,
      snapshot: _snapshot,
      spec: _spec,
      sourceTaskId: _sourceTaskId,
      creationKey: _creationKey,
      ...summary
    } = record;
    const counts = query(db,
      `SELECT
         SUM(CASE WHEN state IN ('pending', 'judging') THEN 1 ELSE 0 END) AS pending,
         SUM(CASE WHEN state IN ('pending', 'judging', 'failed') THEN 1 ELSE 0 END) AS unresolved
       FROM source_watch_events WHERE watch_id = ?`,
      [record.id],
    )[0];
    return {
      ...summary,
      resource: record.spec.resource,
      eventTypes: [...record.spec.eventTypes],
      consumeCondition: record.spec.consumeCondition,
      endCondition: record.spec.endCondition,
      endWhen: record.spec.endWhen,
      taskScope: record.spec.taskScope,
      decisionProfile: record.spec.decisionProfile,
      decisionConsent: record.spec.decisionConsent,
      timeoutMs: record.spec.timeoutMs,
      sourceBudget: record.spec.sourceBudget,
      decisionBudget: record.spec.decisionBudget,
      wakeBudget: record.spec.wakeBudget,
      pendingEvents: asNumber(counts?.pending),
      unresolvedEvents: asNumber(counts?.unresolved),
    };
  });
}

export function stopSourceWatches(
  db: Database,
  botUid: string,
  target: ChatTarget,
  id: string | "all",
  now: number,
  reason: string,
): SourceWatchRecord[] {
  return transaction(db, () => {
    const idClause = id === "all" ? "" : " AND id = ?";
    const values: SqlValue[] = [botUid, ...targetValues(target)];
    if (id !== "all") values.push(id);
    const rows = query(db,
      `SELECT id FROM source_watches WHERE bot_uid = ? AND ${targetWhere(target)}${idClause}
       AND (${ACTIVE_SOURCE_SQL} OR (state = 'ended' AND EXISTS (
         SELECT 1 FROM source_watch_events e WHERE e.watch_id = source_watches.id AND e.wake_status IN ('queued', 'accepted')
       ))) ORDER BY created_at ASC, id ASC`,
      values,
    );
    const stopped: SourceWatchRecord[] = [];
    for (const row of rows) {
      const watchId = asString(row.id);
      const events = query(db,
        "SELECT id, wake_task_id FROM source_watch_events WHERE watch_id = ? AND wake_status IN ('queued', 'accepted')",
        [watchId],
      );
      run(db,
        `UPDATE source_watches SET state = 'stopped', generation = generation + 1,
         stop_reason = ?, latest_error = ? WHERE id = ? AND state NOT IN ('stopped', 'deleted', 'expired', 'failed')`,
        [reason, reason, watchId],
      );
      run(db,
        "UPDATE source_watch_events SET state = 'cancelled', wake_status = CASE WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END, error = ? WHERE watch_id = ? AND state NOT IN ('failed', 'cancelled')",
        [reason, watchId],
      );
      for (const event of events) {
        const taskId = typeof event.wake_task_id === "string" ? event.wake_task_id : null;
        if (taskId) cancelUnstartedSourceWakeTask(db, botUid, target, taskId, now, reason);
      }
      const current = getSourceWatchById(db, watchId);
      if (current) stopped.push(current);
    }
    return stopped;
  });
}

export function deleteSourceWatchChat(db: Database, botUid: string, target: ChatTarget, now: number): SourceWatchRecord[] {
  return transaction(db, () => {
    run(db,
      `INSERT INTO watch_chat_tombstones(bot_uid, channel_type, channel_id, deleted_at)
       VALUES(?, ?, ?, ?) ON CONFLICT(bot_uid, channel_type, channel_id)
       DO UPDATE SET deleted_at = MIN(deleted_at, excluded.deleted_at)`,
      [botUid, target.channelType, target.channelId, now],
    );
    const rows = query(db,
      `SELECT id FROM source_watches WHERE bot_uid = ? AND ${targetWhere(target)}`,
      [botUid, ...targetValues(target)],
    );
    const deleted: SourceWatchRecord[] = [];
    for (const row of rows) {
      const watchId = asString(row.id);
      const events = query(db,
        "SELECT wake_task_id FROM source_watch_events WHERE watch_id = ? AND wake_status IN ('queued', 'accepted')",
        [watchId],
      );
      run(db,
        `UPDATE source_watches SET state = 'deleted', generation = generation + 1,
         stop_reason = 'chat-deleted', latest_error = 'chat-deleted' WHERE id = ?`,
        [watchId],
      );
      run(db,
        "UPDATE source_watch_events SET state = 'cancelled', wake_status = CASE WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END, error = 'chat-deleted' WHERE watch_id = ? AND state NOT IN ('failed', 'cancelled')",
        [watchId],
      );
      for (const event of events) {
        const taskId = typeof event.wake_task_id === "string" ? event.wake_task_id : null;
        if (taskId) cancelUnstartedSourceWakeTask(db, botUid, target, taskId, now, "chat-deleted");
      }
      const current = getSourceWatchById(db, watchId);
      if (current) deleted.push(current);
    }
    return deleted;
  });
}

export function expireSourceWatches(db: Database, now: number): void {
  transaction(db, () => {
    const expired = query(db,
      `SELECT id, bot_uid, channel_type, channel_id FROM source_watches
       WHERE (state IN ('initializing', 'watching', 'budget-exhausted') OR (state = 'ended' AND EXISTS (
         SELECT 1 FROM source_watch_events e WHERE e.watch_id = source_watches.id
         AND (e.state IN ('pending', 'judging') OR e.wake_status IN ('queued', 'accepted'))
       ))) AND deadline_at <= ?`,
      [now],
    );
    for (const row of expired) {
      const watchId = asString(row.id);
      const botUid = asString(row.bot_uid);
      const target = {
        channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
        channelId: asString(row.channel_id),
      };
      const events = query(db,
        "SELECT wake_task_id FROM source_watch_events WHERE watch_id = ? AND wake_status IN ('queued', 'accepted')",
        [watchId],
      );
      run(db,
        `UPDATE source_watches SET state = 'expired', generation = generation + 1,
         stop_reason = 'deadline', latest_error = 'deadline' WHERE id = ? AND deadline_at <= ?`,
        [watchId, now],
      );
      run(db,
        "UPDATE source_watch_events SET state = CASE WHEN state IN ('pending', 'judging') OR wake_status IN ('queued', 'accepted') THEN 'failed' ELSE state END, wake_status = CASE WHEN wake_status IN ('queued', 'accepted') THEN 'cancelled' ELSE wake_status END, error = CASE WHEN state IN ('pending', 'judging') OR wake_status IN ('queued', 'accepted') THEN COALESCE(error, 'deadline') ELSE error END WHERE watch_id = ? AND state NOT IN ('failed', 'cancelled')",
        [watchId],
      );
      for (const event of events) {
        const taskId = typeof event.wake_task_id === "string" ? event.wake_task_id : null;
        if (taskId) cancelUnstartedSourceWakeTask(db, botUid, target, taskId, now, "watch deadline");
      }
    }
  });
}

export function initializeSourceWatch(
  db: Database,
  id: string,
  generation: number,
  cursor: WatchSourceCursor,
  snapshot: WatchSourceSnapshot,
  events: readonly WatchSourceEvent[],
  observedAt: number,
): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch || watch.state !== "initializing" || watch.generation !== generation || watch.deadlineAt <= observedAt ||
        isWatchChatDeleted(db, watch.botUid, watch)) return false;
    const cursorJson = JSON.stringify(cursor);
    const snapshotJson = JSON.stringify(snapshot);
    const initialEvents: WatchSourceEvent[] = [...events];
    if (watch.spec.endCondition !== null || watch.spec.endWhen !== null) {
      initialEvents.push({
        identity: BASELINE_SNAPSHOT_IDENTITY,
        type: "snapshot",
        version: "baseline",
        body: { ...snapshot },
      });
    }
    if (!insertSourceEvents(db, watch, initialEvents, observedAt, 0)) return false;
    return run(db,
      `UPDATE source_watches SET state = 'watching', cursor_json = ?, snapshot_json = ?,
       snapshot_sequence = 0, observed_at = ?, failures = 0, latest_error = NULL
       WHERE id = ? AND state = 'initializing' AND generation = ? AND deadline_at > ?`,
      [cursorJson, snapshotJson, observedAt, id, generation, observedAt],
    ).changes === 1;
  });
}

export function commitSourceObservation(
  db: Database,
  id: string,
  generation: number,
  cursor: WatchSourceCursor,
  snapshot: WatchSourceSnapshot,
  events: readonly WatchSourceEvent[],
  observedAt: number,
): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch || watch.state !== "watching" || watch.generation !== generation || watch.deadlineAt <= observedAt ||
        isWatchChatDeleted(db, watch.botUid, watch)) return false;
    const cursorJson = JSON.stringify(cursor);
    const snapshotJson = JSON.stringify(snapshot);
    const changed = watch.snapshot !== null && JSON.stringify(watch.snapshot) !== snapshotJson;
    const sequence = changed ? watch.snapshotSequence + 1 : watch.snapshotSequence;
    const observedEvents: WatchSourceEvent[] = [...events];
    if (changed && watch.snapshot !== null &&
        (watch.spec.endCondition !== null || watch.spec.endWhen !== null) &&
        !events.some(event => event.type === "snapshot")) {
      observedEvents.push({
        identity: BASELINE_SNAPSHOT_IDENTITY,
        type: "snapshot",
        version: `transition:${sequence}`,
        body: { previous: { ...watch.snapshot }, current: { ...snapshot } },
      });
    }
    if (!insertSourceEvents(db, watch, observedEvents, observedAt, sequence)) return false;
    return run(db,
      `UPDATE source_watches SET cursor_json = ?, snapshot_json = ?, snapshot_sequence = ?,
       observed_at = ?, failures = 0, latest_error = NULL
       WHERE id = ? AND state = 'watching' AND generation = ? AND deadline_at > ?`,
      [cursorJson, snapshotJson, sequence, observedAt, id, generation, observedAt],
    ).changes === 1;
  });
}

export function failSourceWatch(db: Database, id: string, generation: number, reason: string, now: number, gap?: string): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch || watch.generation !== generation || !["initializing", "watching", "budget-exhausted"].includes(watch.state) ||
        watch.deadlineAt <= now || isWatchChatDeleted(db, watch.botUid, watch)) return false;
    const terminal = gap !== undefined || watch.failures + 1 >= 3;
    const nextState: SourceWatchState = terminal ? "failed" : watch.state;
    return run(db,
      `UPDATE source_watches SET state = ?, failures = failures + 1, latest_error = ?, gap = COALESCE(?, gap),
       stop_reason = CASE WHEN ? IS NOT NULL THEN 'coverage-gap' WHEN ? THEN 'three-consecutive-failures' ELSE stop_reason END
       WHERE id = ? AND generation = ? AND state IN ('initializing', 'watching', 'budget-exhausted')`,
      [nextState, reason, gap ?? null, gap ?? null, terminal ? 1 : 0, id, generation],
    ).changes === 1;
  });
}

export function reserveSourceRequest(db: Database, id: string, generation: number, now: number): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch || !["initializing", "watching"].includes(watch.state) || watch.generation !== generation || watch.deadlineAt <= now ||
        watch.sourceUsed >= watch.spec.sourceBudget || isWatchChatDeleted(db, watch.botUid, watch)) return false;
    return run(db,
      `UPDATE source_watches SET source_used = source_used + 1
       WHERE id = ? AND generation = ? AND state IN ('initializing', 'watching') AND source_used < source_budget AND deadline_at > ?`,
      [id, generation, now],
    ).changes === 1;
  });
}

export function beginSourceDecision(
  db: Database,
  id: string,
  eventId: string,
  kind: "consume" | "end",
  generation: number,
  now: number,
): SourceWatchDecisionIntent | null {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    const eventRow = query(db, "SELECT * FROM source_watch_events WHERE id = ? AND watch_id = ?", [eventId, id])[0];
    if (!watch || !eventRow) return null;
    const endedSettlement = watch.state === "ended" && query(db,
      "SELECT 1 FROM source_watch_events WHERE watch_id = ? AND end_state = 'satisfied' LIMIT 1",
      [id],
    ).length > 0;
    if ((!endedSettlement && watch.state !== "watching" && watch.state !== "budget-exhausted") ||
        watch.generation !== generation || watch.deadlineAt <= now || !watch.spec.decisionConsent ||
        watch.decisionUsed >= watch.spec.decisionBudget || eventRow.state === "cancelled" ||
        isWatchChatDeleted(db, watch.botUid, watch)) return null;
    const state = kind === "consume" ? asString(eventRow.consume_state) : asString(eventRow.end_state);
    if (!isRetryableJudgment(state)) return null;
    const open = query(db,
      "SELECT 1 FROM source_watch_decision_intents WHERE event_id = ? AND kind = ? AND state = 'dispatched' LIMIT 1",
      [eventId, kind],
    )[0];
    if (open) return null;
    const condition = kind === "consume" ? watch.spec.consumeCondition : watch.spec.endCondition;
    if (!condition) return null;
    const intentId = randomUUID();
    const reservation = run(db,
      `UPDATE source_watches SET decision_used = decision_used + 1
       WHERE id = ? AND generation = ? AND state IN ('watching', 'budget-exhausted', 'ended')
       AND decision_used < decision_budget AND deadline_at > ?`,
      [id, generation, now],
    );
    if (reservation.changes !== 1) return null;
    run(db,
      `INSERT INTO source_watch_decision_intents(id, watch_id, event_id, kind, generation, state, created_at, updated_at)
       VALUES(?, ?, ?, ?, ?, 'dispatched', ?, ?)`,
      [intentId, id, eventId, kind, generation, now, now],
    );
    const judging = run(db,
      "UPDATE source_watch_events SET state = 'judging' WHERE id = ? AND state IN ('pending', 'judging')",
      [eventId],
    );
    if (judging.changes !== 1) throw new Error("source decision event could not enter judging state");
    return {
      id: intentId,
      event: sourceWatchEventFromRow(eventRow),
      condition,
      generation,
      kind,
    };
  });
}

export function finishSourceDecision(
  db: Database,
  intent: SourceWatchDecisionIntent,
  decision: SourceWatchDecision | null,
  error: string | null,
  now: number,
): SourceWatchEventRecord | null {
  return transaction(db, () => {
    const intentRow = query(db, "SELECT * FROM source_watch_decision_intents WHERE id = ?", [intent.id])[0];
    if (!intentRow || asString(intentRow.state) !== "dispatched") return null;
    run(db,
      `UPDATE source_watch_decision_intents SET state = 'completed', decision_json = ?, error = ?, updated_at = ?
       WHERE id = ? AND state = 'dispatched'`,
      [jsonOrNull(decision), error, now, intent.id],
    );
    const watch = getSourceWatchById(db, intent.event.watchId);
    const eventRow = query(db, "SELECT * FROM source_watch_events WHERE id = ?", [intent.event.id])[0];
    if (!watch || !eventRow) return null;
    const endedSettlement = watch.state === "ended" && query(db,
      "SELECT 1 FROM source_watch_events WHERE watch_id = ? AND end_state = 'satisfied' LIMIT 1",
      [watch.id],
    ).length > 0;
    if ((!endedSettlement && watch.state !== "watching" && watch.state !== "budget-exhausted") ||
        watch.generation !== intent.generation || watch.deadlineAt <= now || isWatchChatDeleted(db, watch.botUid, watch)) return null;

    const state: SourceWatchJudgmentState = decision === null
      ? "failure"
      : decision.choice === "satisfied" && decision.probability >= 0.95
        ? "satisfied"
        : decision.choice === "not_satisfied"
          ? "not_satisfied"
          : "insufficient_evidence";
    const column = intent.kind === "consume" ? "consume" : "end";
    run(db,
      `UPDATE source_watch_events SET ${column}_state = ?, ${column}_decision_json = ?, error = ?,
       next_attempt_at = CASE WHEN ? = 'failure' OR ? = 'insufficient_evidence' THEN ? ELSE 0 END
       WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging')`,
      [state, jsonOrNull(decision), error, state, state, now + 30_000, intent.event.id, intent.event.watchId],
    );
    const updated = query(db, "SELECT * FROM source_watch_events WHERE id = ?", [intent.event.id])[0];
    return updated ? sourceWatchEventFromRow(updated) : null;
  });
}

export function completeSourceEvent(
  db: Database,
  id: string,
  generation: number,
  eventId: string,
  consumeState: SourceWatchJudgmentState,
  endState: SourceWatchJudgmentState,
  endSatisfied: boolean,
  now: number,
): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    const eventRow = query(db, "SELECT * FROM source_watch_events WHERE id = ? AND watch_id = ?", [eventId, id])[0];
    if (!watch || !eventRow) return false;
    const endedSettlement = watch.state === "ended" && query(db,
      "SELECT 1 FROM source_watch_events WHERE watch_id = ? AND end_state = 'satisfied' LIMIT 1",
      [id],
    ).length > 0;
    if ((!endedSettlement && watch.state !== "watching" && watch.state !== "budget-exhausted") ||
        watch.generation !== generation || watch.deadlineAt <= now || isWatchChatDeleted(db, watch.botUid, watch)) return false;

    const consumeUnresolved = isRetryableJudgment(consumeState);
    const endUnresolved = watch.state !== "ended" && isRetryableJudgment(endState);
    const consumeSatisfied = consumeState === "satisfied" || consumeState === "deterministic";
    const decisionBudgetExhausted = watch.decisionUsed >= watch.spec.decisionBudget;
    const retryAt = decisionBudgetExhausted ? watch.deadlineAt : now + 30_000;
    const retryError = decisionBudgetExhausted
      ? "decision-budget-exhausted"
      : consumeState === "failure" || endState === "failure"
        ? "decision-failed"
        : "insufficient-evidence";
    let eventChanges = 0;
    if ((consumeUnresolved || endUnresolved) && !consumeSatisfied) {
      eventChanges = run(db,
        `UPDATE source_watch_events SET state = 'pending', consume_state = ?, end_state = ?,
         next_attempt_at = ?, error = COALESCE(error, ?)
         WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging')`,
        [consumeState, endState, retryAt, retryError, eventId, id],
      ).changes;
    } else {
      const shouldQueueWake = consumeSatisfied && asString(eventRow.wake_status) === "none";
      const wakeBudgetExhausted = shouldQueueWake && watch.wakeUsed >= watch.spec.wakeBudget;
      if (wakeBudgetExhausted) {
        eventChanges = run(db,
          `UPDATE source_watch_events SET state = ?, consume_state = ?, end_state = ?, wake_status = 'failed',
           next_attempt_at = ?, error = 'wake-budget-exhausted'
           WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging') AND wake_status = 'none'`,
          [endUnresolved ? "pending" : "failed", consumeState, endState, endUnresolved ? retryAt : 0, eventId, id],
        ).changes;
      } else if (shouldQueueWake) {
        eventChanges = run(db,
          `UPDATE source_watch_events SET state = ?, consume_state = ?, end_state = ?, wake_status = 'queued',
           next_attempt_at = ?, error = ?
           WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging') AND wake_status = 'none'`,
          [
            endUnresolved ? "pending" : "ready",
            consumeState,
            endState,
            endUnresolved ? retryAt : 0,
            endUnresolved ? retryError : null,
            eventId,
            id,
          ],
        ).changes;
        if (eventChanges === 1) {
          run(db, "UPDATE source_watches SET wake_used = wake_used + 1 WHERE id = ? AND generation = ?", [id, generation]);
        }
      } else {
        const keepPending = consumeUnresolved || endUnresolved;
        eventChanges = run(db,
          `UPDATE source_watch_events SET state = ?, consume_state = ?, end_state = ?,
           next_attempt_at = ?, error = ?
           WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging')`,
          [
            keepPending ? "pending" : "ready",
            consumeState,
            endState,
            keepPending ? retryAt : 0,
            keepPending ? retryError : null,
            eventId,
            id,
          ],
        ).changes;
      }
    }
    if (eventChanges !== 1) return false;

    if (endSatisfied) {
      run(db,
        `UPDATE source_watches SET state = 'ended', stop_reason = 'end-condition', latest_error = NULL
         WHERE id = ? AND generation = ? AND state IN ('watching', 'budget-exhausted')`,
        [id, generation],
      );
    } else if (
      watch.state !== "ended" &&
      (watch.wakeUsed >= watch.spec.wakeBudget ||
        (asString(eventRow.wake_status) === "none" && consumeSatisfied && watch.wakeUsed + 1 >= watch.spec.wakeBudget))
    ) {
      run(db,
        `UPDATE source_watches SET state = 'budget-exhausted', stop_reason = 'wake-budget-exhausted', latest_error = 'wake-budget-exhausted'
         WHERE id = ? AND generation = ? AND state = 'watching'`,
        [id, generation],
      );
    }
    return true;
  });
}
 
export function listSourceWatchEvents(db: Database, id: string, dueAt: number): SourceWatchEventRecord[] {
  return query(db,
    `SELECT * FROM source_watch_events WHERE watch_id = ? AND state <> 'cancelled' AND next_attempt_at <= ?
     ORDER BY observed_at ASC, id ASC`,
    [id, dueAt],
  ).map(sourceWatchEventFromRow);
}
 
export function markSourceEventFailure(db: Database, id: string, eventId: string, error: string, nextAttemptAt: number): boolean {
  return run(db,
    `UPDATE source_watch_events SET state = 'pending', error = ?, next_attempt_at = ?
     WHERE id = ? AND watch_id = ? AND state IN ('pending', 'judging')`,
    [error, nextAttemptAt, eventId, id],
  ).changes === 1;
}
 
export function markSourceBudgetExhausted(db: Database, id: string, generation: number, now: number): boolean {
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch || watch.generation !== generation || watch.state !== "watching") return false;
    const reason = watch.sourceUsed >= watch.spec.sourceBudget
      ? "source-budget-exhausted"
      : (watch.spec.consumeCondition !== null || watch.spec.endCondition !== null) &&
          watch.decisionUsed >= watch.spec.decisionBudget
        ? "decision-budget-exhausted"
        : watch.wakeUsed >= watch.spec.wakeBudget
          ? "wake-budget-exhausted"
          : null;
    if (!reason) return false;
    return run(db,
      `UPDATE source_watches SET state = 'budget-exhausted', stop_reason = ?, latest_error = ?
       WHERE id = ? AND generation = ? AND state = 'watching'`,
      [reason, reason, id, generation],
    ).changes === 1;
  });
}

export function markSourceWakeTask(db: Database, eventId: string, taskId: string, now: number): boolean {
  return transaction(db, () => {
    const event = getSourceEventById(db, eventId);
    if (!event) return false;
    const watch = getSourceWatchById(db, event.watchId);
    if (!watch) return false;
    if (!sourceWatchPermitted(db, watch, event, now)) {
      cancelUnstartedSourceWakeTask(db, watch.botUid, watch, taskId, now, "source watch wake was revoked");
      return false;
    }
    if (event.wakeStatus === "accepted") return event.wakeTaskId === taskId;
    if (event.wakeStatus !== "queued" || event.wakeTaskId !== null) return false;
    return run(db,
      `UPDATE source_watch_events SET wake_task_id = ?, wake_status = 'accepted'
       WHERE id = ? AND wake_status = 'queued' AND wake_task_id IS NULL`,
      [taskId, eventId],
    ).changes === 1;
  });
}
 
export function sourceWakeAllowed(db: Database, eventId: string, now: number): boolean {
  const event = getSourceEventById(db, eventId);
  const watch = event ? getSourceWatchById(db, event.watchId) : null;
  return !!event && !!watch && sourceWatchPermitted(db, watch, event, now);
}
 
export function markSourceWakeDispatched(
  db: Database,
  eventId: string,
  taskId: string,
  sessionId: string,
  now: number,
): boolean {
  return transaction(db, () => {
    let event = getSourceEventById(db, eventId);
    const watch = event ? getSourceWatchById(db, event.watchId) : null;
    if (!event || !watch || !sourceWatchPermitted(db, watch, event, now)) {
      if (watch) cancelUnstartedSourceWakeTask(db, watch.botUid, watch, taskId, now, "source watch wake was revoked before prompt dispatch");
      return false;
    }
    if (event.wakeStatus === "queued" && event.wakeTaskId === null) {
      const associated = run(db,
        `UPDATE source_watch_events SET wake_task_id = ?, wake_status = 'accepted'
         WHERE id = ? AND wake_status = 'queued' AND wake_task_id IS NULL`,
        [taskId, eventId],
      );
      if (associated.changes !== 1) return false;
      event = getSourceEventById(db, eventId);
    }
    if (!event || event.wakeTaskId !== taskId || event.wakeStatus !== "accepted" ||
        !sourceWatchPermitted(db, watch, event, now)) {
      cancelUnstartedSourceWakeTask(db, watch.botUid, watch, taskId, now, "source watch wake was revoked before prompt dispatch");
      return false;
    }
    const task = query(db,
      "SELECT bot_uid, channel_type, channel_id, message_id FROM tasks WHERE id = ?",
      [taskId],
    )[0];
    if (!task || asString(task.bot_uid) !== watch.botUid ||
        asNumber(task.channel_type) !== watch.channelType || asString(task.channel_id) !== watch.channelId ||
        asString(task.message_id) !== `watch:event:${watch.id}:${event.id}`) {
      cancelUnstartedSourceWakeTask(db, watch.botUid, watch, taskId, now, "source watch wake task identity mismatch");
      return false;
    }
    const dispatched = run(db,
      `UPDATE source_watch_events SET wake_status = 'dispatched'
       WHERE id = ? AND wake_task_id = ? AND wake_status = 'accepted'`,
      [eventId, taskId],
    );
    if (dispatched.changes !== 1) return false;
    markTaskPromptStarted(db, taskId, sessionId);
    return true;
  });
}


export function markSourceWakeFailed(db: Database, eventId: string, reason: string, now: number): boolean {
  return transaction(db, () => {
    const event = getSourceEventById(db, eventId);
    if (!event || (event.wakeStatus !== "queued" && event.wakeStatus !== "accepted")) return false;
    const watch = getSourceWatchById(db, event.watchId);
    run(db,
      "UPDATE source_watch_events SET wake_status = 'failed', state = 'failed', error = ? WHERE id = ?",
      [reason, eventId],
    );
    if (watch && event.wakeTaskId) cancelUnstartedSourceWakeTask(db, watch.botUid, watch, event.wakeTaskId, now, reason);
    return true;
  });
}

export function getSourceWakeByMessageId(
  db: Database,
  botUid: string,
  target: ChatTarget,
  messageId: string,
): SourceWatchEventRecord | null {
  const prefix = "watch:event:";
  if (!messageId.startsWith(prefix)) return null;
  const parts = messageId.slice(prefix.length).split(":");
  if (parts.length !== 2) return null;
  const [watchId, eventId] = parts as [string, string];
  const row = query(db,
    `SELECT e.* FROM source_watch_events e JOIN source_watches w ON w.id = e.watch_id
     WHERE w.bot_uid = ? AND w.id = ? AND e.id = ? AND w.channel_type = ? AND w.channel_id = ?`,
    [botUid, watchId, eventId, target.channelType, target.channelId],
  )[0];
  return row ? sourceWatchEventFromRow(row) : null;
}

export function recoverSourceWatch(db: Database, id: string, now: number): SourceWatchRecord | null {
  const beforeRecovery = getSourceWatchById(db, id);
  if (!beforeRecovery) return null;
  if (
    ["initializing", "watching", "budget-exhausted", "ended"].includes(beforeRecovery.state) &&
    beforeRecovery.deadlineAt <= now
  ) {
    expireSourceWatches(db, now);
    return getSourceWatchById(db, id);
  }
  return transaction(db, () => {
    const watch = getSourceWatchById(db, id);
    if (!watch) return null;
    run(db,
      `UPDATE source_watch_decision_intents SET state = 'abandoned', error = 'interrupted-by-restart', updated_at = ?
       WHERE watch_id = ? AND state = 'dispatched'`,
      [now, id],
    );
    run(db,
      `UPDATE source_watch_events SET state = 'pending', next_attempt_at = ?, error = 'interrupted-by-restart'
       WHERE watch_id = ? AND state = 'judging'`,
      [now, id],
    );
    if (watch.state === "initializing") {
      run(db,
        `UPDATE source_watches SET state = 'failed', generation = generation + 1,
         gap = 'baseline-interrupted: source continuity cannot be proven after restart',
         latest_error = 'baseline-interrupted', stop_reason = 'coverage-gap'
         WHERE id = ? AND state = 'initializing'`,
        [id],
      );
    }
    return getSourceWatchById(db, id);
  });
}

export function getSourceWatchByMessageId(
  db: Database,
  botUid: string,
  target: ChatTarget,
  messageId: string,
): { watch: SourceWatchRecord; event: SourceWatchEventRecord } | null {
  const event = getSourceWakeByMessageId(db, botUid, target, messageId);
  if (!event) return null;
  const watch = getSourceWatchById(db, event.watchId);
  return watch ? { watch, event } : null;
}

export function isWatchChatDeleted(db: Database, botUid: string, target: ChatTarget): boolean {
  return query(db,
    `SELECT 1 FROM watch_chat_tombstones WHERE bot_uid = ? AND ${targetWhere(target)} LIMIT 1`,
    [botUid, ...targetValues(target)],
  ).length > 0;
}

function insertSourceEvents(
  db: Database,
  watch: SourceWatchRecord,
  events: readonly WatchSourceEvent[],
  observedAt: number,
  snapshotSequence: number,
): boolean {
  const normalized: Array<{ identity: string; type: string; version: string; body: string }> = [];
  for (const event of events) {
    const identity = event.identity;
    const type = event.type;
    const version = event.version;
    const body = JSON.stringify(event.body);
    if (typeof identity !== "string" || !identity || Buffer.byteLength(identity, "utf8") > 1_024 ||
        typeof type !== "string" || !type || Buffer.byteLength(type, "utf8") > 128 ||
        typeof version !== "string" || !version || Buffer.byteLength(version, "utf8") > 1_024 ||
        Buffer.byteLength(body, "utf8") > 16 * 1024) return false;
    normalized.push({ identity, type, version, body });
  }
  for (const event of normalized) {
    run(db,
      `INSERT OR IGNORE INTO source_watch_events(
        id, watch_id, identity, event_type, version, body_json, observed_at, snapshot_sequence,
        state, consume_state, consume_decision_json, end_state, end_decision_json,
        wake_task_id, wake_status, next_attempt_at, error
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending', NULL, 'pending', NULL, NULL, 'none', 0, NULL)`,
      [randomUUID(), watch.id, event.identity, event.type, event.version, event.body, observedAt, snapshotSequence],
    );
  }
  return true;
}

function activeWatchCount(db: Database, botUid: string, target?: ChatTarget): number {
  const chat = target ? ` AND ${targetWhere(target)}` : "";
  const legacyValues: SqlValue[] = [botUid];
  const sourceValues: SqlValue[] = [botUid];
  if (target) {
    legacyValues.push(...targetValues(target));
    sourceValues.push(...targetValues(target));
  }
  const legacy = asNumber(query(db,
    `SELECT COUNT(*) AS count FROM watches WHERE bot_uid = ?${chat} AND ${ACTIVE_LEGACY_SQL}`,
    legacyValues,
  )[0]?.count);
  const source = asNumber(query(db,
    `SELECT COUNT(*) AS count FROM source_watches WHERE bot_uid = ?${chat} AND ${ACTIVE_SOURCE_SQL}`,
    sourceValues,
  )[0]?.count);
  return legacy + source;
}

function sourceWatchPermitted(db: Database, watch: SourceWatchRecord, event: SourceWatchEventRecord, now: number): boolean {
  const consumable = event.consumeState === "satisfied" || event.consumeState === "deterministic";
  const stateAllowsWake = event.state === "ready" || (event.state === "pending" && consumable);
  return (watch.state === "watching" || watch.state === "budget-exhausted" || watch.state === "ended") &&
    watch.deadlineAt > now && stateAllowsWake &&
    (event.wakeStatus === "queued" || event.wakeStatus === "accepted") &&
    !isWatchChatDeleted(db, watch.botUid, watch);
}

function cancelUnstartedSourceWakeTask(
  db: Database,
  botUid: string,
  target: ChatTarget,
  taskId: string,
  now: number,
  reason: string,
): void {
  run(db,
    `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ?, stop_intent = NULL
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND id = ?
       AND (state = 'queued' OR (state = 'started' AND prompt_started = 0))`,
    [now, reason, botUid, target.channelType, target.channelId, taskId],
  );
}

function getSourceWatchById(db: Database, id: string): SourceWatchRecord | null {
  const row = query(db, "SELECT * FROM source_watches WHERE id = ?", [id])[0];
  return row ? sourceWatchFromRow(db, row) : null;
}

function getSourceEventById(db: Database, id: string): SourceWatchEventRecord | null {
  const row = query(db, "SELECT * FROM source_watch_events WHERE id = ?", [id])[0];
  return row ? sourceWatchEventFromRow(row) : null;
}

function sourceWatchFromRow(db: Database, row: Record<string, unknown>): SourceWatchRecord {
  const id = asString(row.id);
  return {
    id,
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
    channelId: asString(row.channel_id),
    sourceTaskId: asString(row.source_task_id),
    creationKey: asString(row.creation_key),
    spec: parseSourceWatchSpec(row.spec_json),
    createdAt: asNumber(row.created_at),
    deadlineAt: asNumber(row.deadline_at),
    state: isSourceWatchState(row.state) ? row.state : "failed",
    generation: asNumber(row.generation),
    cursor: parseNullableJson<WatchSourceCursor>(row.cursor_json),
    snapshot: parseNullableJson<WatchSourceSnapshot>(row.snapshot_json),
    snapshotSequence: asNumber(row.snapshot_sequence),
    sourceUsed: asNumber(row.source_used),
    decisionUsed: asNumber(row.decision_used),
    wakeUsed: asNumber(row.wake_used),
    failures: asNumber(row.failures),
    gap: typeof row.gap === "string" ? row.gap : null,
    latestError: typeof row.latest_error === "string" ? row.latest_error : null,
    stopReason: typeof row.stop_reason === "string" ? row.stop_reason : null,
    observedAt: row.observed_at === null || row.observed_at === undefined ? null : asNumber(row.observed_at),
  };
}

function sourceWatchEventFromRow(row: Record<string, unknown>): SourceWatchEventRecord {
  return {
    id: asString(row.id),
    watchId: asString(row.watch_id),
    identity: asString(row.identity),
    type: asString(row.event_type),
    version: asString(row.version),
    body: parseJson<Record<string, unknown>>(row.body_json),
    observedAt: asNumber(row.observed_at),
    snapshotSequence: asNumber(row.snapshot_sequence),
    state: isSourceWatchEventState(row.state) ? row.state : "failed",
    consumeState: isJudgmentState(row.consume_state) ? row.consume_state : "failure",
    consumeDecision: parseNullableJson<SourceWatchDecision>(row.consume_decision_json),
    endState: isJudgmentState(row.end_state) ? row.end_state : "failure",
    endDecision: parseNullableJson<SourceWatchDecision>(row.end_decision_json),
    wakeTaskId: typeof row.wake_task_id === "string" ? row.wake_task_id : null,
    wakeStatus: isSourceWakeStatus(row.wake_status) ? row.wake_status : "failed",
    nextAttemptAt: asNumber(row.next_attempt_at),
    error: typeof row.error === "string" ? row.error : null,
  };
}

function parseJson<T>(value: unknown): T {
  if (typeof value !== "string") throw new Error("persisted source watch JSON is invalid");
  return JSON.parse(value) as T;
}
function parseSourceWatchSpec(value: unknown): SourceWatchSpec {
  const spec = parseJson<SourceWatchSpec>(value);
  return { ...spec, endWhen: spec.endWhen ?? null };
}

function parseNullableJson<T>(value: unknown): T | null {
  if (typeof value !== "string") return null;
  return JSON.parse(value) as T;
}

function jsonOrNull(value: unknown): string | null {
  return value === null ? null : JSON.stringify(value);
}

function isRetryableJudgment(state: string): boolean {
  return state === "pending" || state === "failure" || state === "insufficient_evidence";
}

function isSourceWatchState(value: unknown): value is SourceWatchState {
  return value === "initializing" || value === "watching" || value === "budget-exhausted" || value === "ended" ||
    value === "failed" || value === "expired" || value === "stopped" || value === "deleted";
}

function isSourceWatchEventState(value: unknown): value is SourceWatchEventRecord["state"] {
  return value === "pending" || value === "judging" || value === "ready" || value === "failed" || value === "cancelled";
}

function isJudgmentState(value: unknown): value is SourceWatchJudgmentState {
  return value === "pending" || value === "satisfied" || value === "not_satisfied" || value === "insufficient_evidence" ||
    value === "failure" || value === "deterministic";
}

function isSourceWakeStatus(value: unknown): value is SourceWatchWakeStatus {
  return value === "none" || value === "queued" || value === "accepted" || value === "dispatched" ||
    value === "cancelled" || value === "failed";
}
