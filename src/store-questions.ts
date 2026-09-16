/**
 * 提问记录：创建、答复的比较并交换、取消与过期，以及卡片消息 id 的回填。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "./contracts.ts";
import type { QuestionRecord } from "./store-records.ts";
import { query, questionFromRow, run, targetValues, targetWhere, transaction } from "./store-schema.ts";

export function createQuestion(db: Database, record: Omit<QuestionRecord, "state" | "answer" | "cardMessageId">): void {
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

export function answerQuestion(db: Database, id: string, botUid: string, target: ChatTarget, answer: string): boolean {
  return run(db,
    `UPDATE questions SET state = 'answered', answer = ?, answered_at = ?
     WHERE id = ? AND bot_uid = ? AND ${targetWhere(target)} AND state = 'pending'`,
    [answer, Date.now(), id, botUid, ...targetValues(target)],
  ).changes === 1;
}

export function answerQuestions(db: Database, botUid: string,
  target: ChatTarget,
  answers: ReadonlyArray<{ id: string; answer: string }>,): string[] {
  return transaction(db, () => {
    const landed: string[] = [];
    for (const { id, answer } of answers) {
      if (answerQuestion(db, id, botUid, target, answer)) landed.push(id);
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

export function expireQuestion(db: Database, id: string): boolean {
  return run(db,
    "UPDATE questions SET state = 'expired', answered_at = ? WHERE id = ? AND state = 'pending'",
    [Date.now(), id],
  ).changes === 1;
}

export function pendingQuestionIds(db: Database, botUid: string, target: ChatTarget): string[] {
  return listPendingQuestions(db, botUid, target).map(({ id }) => id);
}

export function setQuestionCard(db: Database, id: string, messageId: string): void {
  run(db, "UPDATE questions SET card_message_id = ? WHERE id = ?", [messageId, id]);
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
