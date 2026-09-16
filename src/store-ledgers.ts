/**
 * 上下文与群历史的注入台账：pending/committed 的比较并交换、水位推进与清理。
 */

import { randomUUID } from "node:crypto";
import { Database } from "bun:sqlite";
import type { ChatTarget } from "./contracts.ts";
import type { ContextLedgerRecord, HistoryLedgerRecord } from "./store-records.ts";
import { SqlValue, asNumber, contextLedgerFromRow, historyLedgerFromRow, historyLedgerRow, query, run, targetValues, targetWhere, transaction, validHistoryIdentity } from "./store-schema.ts";

export function getContextLedger(db: Database, botUid: string,
  target: ChatTarget,
  sessionId: string,): ContextLedgerRecord | null {
  if (target.channelType !== 2 && target.channelType !== 5) return null;
  if (botUid.length === 0 || sessionId.length === 0) return null;
  const row = query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version
     FROM session_context_ledger
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
    [botUid, ...targetValues(target), sessionId],
  )[0];
  return row ? contextLedgerFromRow(row) : null;
}

export function prepareContextInjection(db: Database, botUid: string,
  target: ChatTarget,
  sessionId: string,
  version: number,
  requestedAttemptId?: string,): ContextLedgerRecord {
  if (
    botUid.length === 0
    || sessionId.length === 0
    || (target.channelType !== 2 && target.channelType !== 5)
    || !Number.isSafeInteger(version)
    || version < 0
  ) {
    throw new Error("context ledger identity or version is invalid");
  }
  const attemptId = requestedAttemptId ?? randomUUID();
  if (attemptId.length === 0) throw new Error("context ledger attempt ID is invalid");
  return transaction(db, () => {
    const existingRow = query(db,
      `SELECT bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version
       FROM session_context_ledger
       WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
      [botUid, ...targetValues(target), sessionId],
    )[0];
    if (existingRow) {
      const existing = contextLedgerFromRow(existingRow);
      if (existing.version >= version) return existing;
      const now = Date.now();
      run(db,
        `UPDATE session_context_ledger
         SET version = ?, phase = 'pending', attempt_id = ?, previous_version = ?, updated_at = ?
         WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
        [
          version,
          attemptId,
          existing.version,
          now,
          botUid,
          ...targetValues(target),
          sessionId,
        ],
      );
    } else {
      run(db,
        `INSERT INTO session_context_ledger(
           bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version, updated_at
         ) VALUES(?, ?, ?, ?, ?, 'pending', ?, NULL, ?)`,
        [
          botUid,
          target.channelType,
          target.channelId,
          sessionId,
          version,
          attemptId,
          Date.now(),
        ],
      );
    }
    const row = query(db,
      `SELECT bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version
       FROM session_context_ledger
       WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
      [botUid, ...targetValues(target), sessionId],
    )[0];
    if (!row) throw new Error("context ledger row disappeared after prepare");
    return contextLedgerFromRow(row);
  });
}

export function commitContextInjection(db: Database, botUid: string,
  target: ChatTarget,
  sessionId: string,
  version: number,
  attemptId: string,): boolean {
  if (
    botUid.length === 0
    || sessionId.length === 0
    || attemptId.length === 0
    || (target.channelType !== 2 && target.channelType !== 5)
    || !Number.isSafeInteger(version)
    || version < 0
  ) {
    return false;
  }
  return run(db,
    `UPDATE session_context_ledger
     SET phase = 'committed', updated_at = ?
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?
       AND version = ? AND attempt_id = ? AND phase = 'pending'`,
    [
      Date.now(),
      botUid,
      ...targetValues(target),
      sessionId,
      version,
      attemptId,
    ],
  ).changes === 1;
}

export function clearContextLedger(db: Database, botUid: string, target: ChatTarget, sessionId?: string): void {
  if (botUid.length === 0 || (target.channelType !== 2 && target.channelType !== 5)) return;
  const sessionClause = sessionId === undefined ? "" : " AND session_id = ?";
  const values: SqlValue[] = [botUid, ...targetValues(target)];
  if (sessionId !== undefined) values.push(sessionId);
  run(db,
    `DELETE FROM session_context_ledger
     WHERE bot_uid = ? AND ${targetWhere(target)}${sessionClause}`,
    values,
  );
}

export function getHistoryLedger(db: Database, botUid: string, target: ChatTarget, sessionId: string): HistoryLedgerRecord | null {
  if (!validHistoryIdentity(botUid, target, sessionId)) return null;
  const row = historyLedgerRow(db, botUid, target, sessionId);
  return row ? historyLedgerFromRow(row) : null;
}

export function prepareHistoryInjection(db: Database, botUid: string,
  target: ChatTarget,
  sessionId: string,
  lastSeq: number,): HistoryLedgerRecord {
  if (
    !validHistoryIdentity(botUid, target, sessionId)
    || !Number.isSafeInteger(lastSeq)
    || lastSeq < 0
  ) {
    throw new Error("group history ledger identity or seq is invalid");
  }
  return transaction(db, () => {
    const existingRow = historyLedgerRow(db, botUid, target, sessionId);
    if (existingRow) {
      const existing = historyLedgerFromRow(existingRow);
      const pending = existing.phase === "pending";
      run(db,
        `UPDATE group_history_ledger
         SET last_seq = ?, phase = 'pending', attempt_id = ?, previous_seq = ?, updated_at = ?
         WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
        [
          Math.max(existing.lastSeq, lastSeq),
          pending ? existing.attemptId : randomUUID(),
          pending ? existing.previousSeq : existing.lastSeq,
          Date.now(),
          botUid,
          ...targetValues(target),
          sessionId,
        ],
      );
    } else {
      run(db,
        `INSERT INTO group_history_ledger(
           bot_uid, channel_type, channel_id, session_id, last_seq, phase, attempt_id, previous_seq, updated_at
         ) VALUES(?, ?, ?, ?, ?, 'pending', ?, NULL, ?)`,
        [botUid, target.channelType, target.channelId, sessionId, lastSeq, randomUUID(), Date.now()],
      );
    }
    const row = historyLedgerRow(db, botUid, target, sessionId);
    if (!row) throw new Error("group history ledger row disappeared after prepare");
    return historyLedgerFromRow(row);
  });
}

export function commitHistoryInjection(db: Database, botUid: string,
  target: ChatTarget,
  sessionId: string,
  lastSeq: number,
  attemptId: string,): boolean {
  if (
    !validHistoryIdentity(botUid, target, sessionId)
    || attemptId.length === 0
    || !Number.isSafeInteger(lastSeq)
    || lastSeq < 0
  ) {
    return false;
  }
  return run(db,
    `UPDATE group_history_ledger
     SET phase = 'committed', updated_at = ?
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?
       AND last_seq = ? AND attempt_id = ? AND phase = 'pending'`,
    [Date.now(), botUid, ...targetValues(target), sessionId, lastSeq, attemptId],
  ).changes === 1;
}

export function pruneHistoryLedger(db: Database, botUid: string, target: ChatTarget, keepSessionId: string): number {
  if (!validHistoryIdentity(botUid, target, keepSessionId)) return 0;
  return run(db,
    `DELETE FROM group_history_ledger
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id != ?`,
    [botUid, ...targetValues(target), keepSessionId],
  ).changes;
}

export function advanceHistoryLedger(db: Database, botUid: string, target: ChatTarget, sessionId: string, seq: number): void {
  if (!validHistoryIdentity(botUid, target, sessionId) || !Number.isSafeInteger(seq) || seq < 0) return;
  transaction(db, () => {
    const row = historyLedgerRow(db, botUid, target, sessionId);
    if (!row) {
      run(db,
        `INSERT INTO group_history_ledger(
           bot_uid, channel_type, channel_id, session_id, last_seq, phase, attempt_id, previous_seq, updated_at
         ) VALUES(?, ?, ?, ?, ?, 'committed', ?, NULL, ?)`,
        [botUid, target.channelType, target.channelId, sessionId, seq, randomUUID(), Date.now()],
      );
      return;
    }
    if (asNumber(row.last_seq) >= seq) return;
    run(db,
      `UPDATE group_history_ledger
       SET last_seq = ?, updated_at = ?
       WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
      [seq, Date.now(), botUid, ...targetValues(target), sessionId],
    );
  });
}
