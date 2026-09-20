/**
 * 提问记录：创建、答复的比较并交换、取消，以及卡片消息 id 的回填。等待超时不落状态——
 * 超时只是停止当前回合，问题行保持 `pending` 直到被答复或取消（`expired` 仅为旧库遗留的读值）。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { QuestionRecord } from "./store-records.ts";
import { asNumber, query, questionFromRow, run, type SqlValue, targetValues, targetWhere, transaction } from "./store-schema.ts";

export function createQuestion(db: Database, record: Omit<QuestionRecord, "state" | "answer" | "answeredBy" | "cardMessageId" | "settledAt">): void {
  run(db,
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

export function getQuestion(db: Database, id: string): QuestionRecord | null {
  const row = query(db, "SELECT * FROM questions WHERE id = ?", [id])[0];
  return row ? questionFromRow(row) : null;
}

export function listPendingQuestions(db: Database, botUid: string, target: ChatTarget): QuestionRecord[] {
  return query(db,
    `SELECT * FROM questions WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'pending' ORDER BY created_at ASC, rowid ASC`,
    [botUid, ...targetValues(target)],
  ).map((row) => questionFromRow(row));
}

/**
 * 一个任务的全部提问行，按写入顺序（created_at, rowid）返回，状态不过滤：重启恢复要能看到同一
 * 回合里已经存在的等待（含已答复/已取消的行），才能判断该继续等还是重开问题。
 */
export function listTaskQuestions(db: Database, taskId: string): QuestionRecord[] {
  return query(db,
    "SELECT * FROM questions WHERE task_id = ? ORDER BY created_at ASC, rowid ASC",
    [taskId],
  ).map((row) => questionFromRow(row));
}

export function answerQuestion(db: Database, id: string, botUid: string, target: ChatTarget, answer: string, answeredBy: string | null): boolean {
  return run(db,
    `UPDATE questions SET state = 'answered', answer = ?, answered_by = ?, answered_at = ?
     WHERE id = ? AND bot_uid = ? AND ${targetWhere(target)} AND state = 'pending'`,
    [answer, answeredBy, Date.now(), id, botUid, ...targetValues(target)],
  ).changes === 1;
}

export function answerQuestions(db: Database, botUid: string,
  target: ChatTarget,
  answers: ReadonlyArray<{ id: string; answer: string; answeredBy: string | null }>,): string[] {
  return transaction(db, () => {
    const landed: string[] = [];
    for (const { id, answer, answeredBy } of answers) {
      if (answerQuestion(db, id, botUid, target, answer, answeredBy)) landed.push(id);
    }
    return landed;
  });
}

export function cancelQuestions(db: Database, botUid: string, target: ChatTarget): void {
  run(db,
    `UPDATE questions SET state = 'cancelled', answered_at = ?
     WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'pending'`,
    [Date.now(), botUid, ...targetValues(target)],
  );
}

export function cancelQuestion(db: Database, id: string): boolean {
  return run(db,
    "UPDATE questions SET state = 'cancelled', answered_at = ? WHERE id = ? AND state = 'pending'",
    [Date.now(), id],
  ).changes === 1;
}

export function pendingQuestionIds(db: Database, botUid: string, target: ChatTarget): string[] {
  return listPendingQuestions(db, botUid, target).map(({ id }) => id);
}

export function setQuestionCard(db: Database, id: string, messageId: string): void {
  run(db,
    "UPDATE questions SET card_message_id = ?, card_seq = 1 WHERE id = ?",
    [messageId, id],
  );
}

/**
 * 该卡下一帧要用的编辑序号，单事务内预留并持久化到该卡的所有行。
 * - 已知序号（行上都有值）：取同卡同聊天最大值 + 1，严格递增。
 * - 首次预留但行上没有序号（补列之前的旧行，`NULL` = 未知）或卡上一行都还没有：一次性 bootstrap 到
 *   高水位——取「当前时间」与「该卡自身持久时间戳」的最大值，保证高于旧线上内存序号加时间戳起点用过
 *   的任何值；空卡没有旧进程可超越，直接给 2（首帧用掉 1，绝不与首帧撞号）。
 * 之后每次预留都落在行上，跨重启严格单调，不再依赖时钟。
 */
export function reserveQuestionCardSeq(db: Database, botUid: string, target: ChatTarget, cardMessageId: string): number {
  return transaction(db, () => {
    const scope = `bot_uid = ? AND ${targetWhere(target)} AND card_message_id = ?`;
    const values: SqlValue[] = [botUid, ...targetValues(target), cardMessageId];
    const row = query(db,
      `SELECT COUNT(*) AS rows, SUM(card_seq IS NULL) AS unknown_rows, MAX(card_seq) AS max_seq,
              MAX(created_at) AS max_created, MAX(answered_at) AS max_answered
       FROM questions WHERE ${scope}`,
      values,
    )[0];
    const rows = asNumber(row?.rows);
    const known = rows > 0 && asNumber(row?.unknown_rows) === 0 && typeof row?.max_seq === "number";
    const watermark = Math.max(
      Date.now(),
      asNumber(row?.max_seq),
      asNumber(row?.max_created),
      asNumber(row?.max_answered),
    );
    const seq = known ? asNumber(row?.max_seq) + 1 : rows === 0 ? 2 : watermark + 1;
    run(db, `UPDATE questions SET card_seq = ? WHERE ${scope}`, [seq, ...values]);
    return seq;
  });
}

/**
 * 一轮 ask 调用的收场：给尚未落定的行盖上时间（结果交付成功、等待超时停住、本地取消/失败都算收场）。
 * 只写 `settled_at IS NULL` 的行——时间戳是第一次收场的证据，重复调用不会改写它；空数组 no-op。
 * 停机挂起时调用方不落定，正是重开进程 beginRecovery 要认领的那批行。
 */
export function markQuestionsSettled(db: Database, ids: readonly string[]): void {
  if (ids.length === 0) return;
  transaction(db, () => {
    const settledAt = Date.now();
    for (const id of ids) {
      run(db, "UPDATE questions SET settled_at = ? WHERE id = ? AND settled_at IS NULL", [settledAt, id]);
    }
  });
}

export function setQuestionsCard(db: Database, ids: readonly string[], messageId: string): void {
  transaction(db, () => {
    for (const id of ids) setQuestionCard(db, id, messageId);
  });
}

export function listQuestionsByCard(db: Database, botUid: string, target: ChatTarget, messageId: string): QuestionRecord[] {
  return query(db,
    `SELECT * FROM questions WHERE bot_uid = ? AND ${targetWhere(target)} AND card_message_id = ?
     ORDER BY rowid ASC`,
    [botUid, ...targetValues(target), messageId],
  ).map((row) => questionFromRow(row));
}
