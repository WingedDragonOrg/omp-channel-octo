/** 开通确认卡的持久授权、配置镜像与回执状态。 */

import { Database } from "bun:sqlite";
import type { ChatGrantOperationInput, ChatGrantOperationRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, transaction } from "./store-schema.ts";

export function acceptChatGrantOperation(db: Database, input: ChatGrantOperationInput): boolean {
  return transaction(db, () => {
    const owner = query(db, "SELECT owner_id FROM owners WHERE bot_uid = ?", [input.botUid])[0];
    if (input.actorId !== input.ownerId || asNullableString(owner?.owner_id) !== input.ownerId) return false;

    const acceptedAt = Date.now();
    const inserted = run(db,
      `INSERT INTO chat_grant_operations(
         bot_uid, action_id, event_id, owner_id, actor_id, channel_type, channel_id,
         card_message_id, result_message_id, accepted_at, config_mirror_state,
         config_mirror_error, result_state
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 'pending')
       ON CONFLICT DO NOTHING`,
      [
        input.botUid,
        input.actionId,
        input.eventId,
        input.ownerId,
        input.actorId,
        input.channelType,
        input.channelId,
        input.cardMessageId,
        input.resultMessageId,
        acceptedAt,
      ],
    ).changes === 1;
    if (!inserted) return false;

    run(db,
      `INSERT INTO allowed_chats(bot_uid, channel_type, channel_id, granted_by, granted_at)
       VALUES(?, ?, ?, ?, ?)
       ON CONFLICT(bot_uid, channel_type, channel_id) DO NOTHING`,
      [input.botUid, input.channelType, input.channelId, input.actorId, acceptedAt],
    );
    return true;
  });
}

export function getChatGrantOperation(
  db: Database,
  botUid: string,
  actionId: string,
): ChatGrantOperationRecord | null {
  const row = query(db,
    "SELECT * FROM chat_grant_operations WHERE bot_uid = ? AND action_id = ?",
    [botUid, actionId],
  )[0];
  return row ? chatGrantOperationFromRow(row) : null;
}

export function getChatGrantOperationByEvent(
  db: Database,
  botUid: string,
  eventId: number,
): ChatGrantOperationRecord | null {
  const row = query(db,
    "SELECT * FROM chat_grant_operations WHERE bot_uid = ? AND event_id = ?",
    [botUid, eventId],
  )[0];
  return row ? chatGrantOperationFromRow(row) : null;
}

export function listPendingChatGrantOperations(db: Database, botUid: string): ChatGrantOperationRecord[] {
  return query(db,
    `SELECT * FROM chat_grant_operations
     WHERE bot_uid = ? AND (config_mirror_state = 'pending' OR result_state = 'pending')
     ORDER BY accepted_at ASC, event_id ASC, action_id ASC`,
    [botUid],
  ).map(chatGrantOperationFromRow);
}

export function markChatGrantConfigMirror(
  db: Database,
  botUid: string,
  actionId: string,
  state: "succeeded" | "failed",
  error?: string,
): boolean {
  return run(db,
    `UPDATE chat_grant_operations
     SET config_mirror_state = ?, config_mirror_error = ?
     WHERE bot_uid = ? AND action_id = ? AND config_mirror_state = 'pending'`,
    [state, state === "failed" ? error ?? null : null, botUid, actionId],
  ).changes === 1;
}

export function markChatGrantResultDelivered(db: Database, botUid: string, actionId: string): boolean {
  return run(db,
    `UPDATE chat_grant_operations
     SET result_state = 'delivered'
     WHERE bot_uid = ? AND action_id = ? AND config_mirror_state <> 'pending' AND result_state = 'pending'`,
    [botUid, actionId],
  ).changes === 1;
}

export function nextChatGrantCardEditSeq(db: Database, botUid: string, actionId: string): number {
  return transaction(db, () => {
    const row = query(db,
      `SELECT card_edit_seq, config_mirror_state, result_state
       FROM chat_grant_operations
       WHERE bot_uid = ? AND action_id = ?`,
      [botUid, actionId],
    )[0];
    if (
      !row ||
      asString(row.config_mirror_state) === "pending" ||
      asString(row.result_state) !== "pending"
    ) {
      throw new Error("Chat grant operation is not pending a card result");
    }

    const currentSequence = asNumber(row.card_edit_seq);
    if (
      !Number.isSafeInteger(currentSequence) ||
      currentSequence < 1 ||
      currentSequence >= Number.MAX_SAFE_INTEGER
    ) {
      throw new Error("Chat grant operation has an invalid card edit sequence");
    }
    const nextSequence = currentSequence + 1;
    const updated = run(db,
      `UPDATE chat_grant_operations SET card_edit_seq = ?
       WHERE bot_uid = ? AND action_id = ? AND card_edit_seq = ?
         AND config_mirror_state <> 'pending' AND result_state = 'pending'`,
      [nextSequence, botUid, actionId, currentSequence],
    ).changes === 1;
    if (!updated) throw new Error("Chat grant card edit sequence could not be advanced");
    return nextSequence;
  });
}

function chatGrantOperationFromRow(row: Record<string, unknown>): ChatGrantOperationRecord {
  const mirrorState = asString(row.config_mirror_state);
  return {
    actionId: asString(row.action_id),
    eventId: asNumber(row.event_id),
    botUid: asString(row.bot_uid),
    ownerId: asString(row.owner_id),
    actorId: asString(row.actor_id),
    channelType: asNumber(row.channel_type) === 5 ? 5 : 2,
    channelId: asString(row.channel_id),
    cardMessageId: asString(row.card_message_id),
    resultMessageId: asString(row.result_message_id),
    acceptedAt: asNumber(row.accepted_at),
    cardEditSeq: asNumber(row.card_edit_seq),
    configMirrorState: mirrorState === "succeeded" || mirrorState === "failed" ? mirrorState : "pending",
    configMirrorError: asNullableString(row.config_mirror_error),
    resultState: asString(row.result_state) === "delivered" ? "delivered" : "pending",
  };
}
