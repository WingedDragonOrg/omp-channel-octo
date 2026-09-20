/**
 * 任务与 inbound 认领：去重声明、命令声明与状态、任务状态迁移、重启通知。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { InboundClaim, RestartNoticeRecord, TaskRecord, TaskRestartRecovery } from "./store-records.ts";
import { asNumber, asString, query, run, targetValues, targetWhere, taskFromRow, transaction } from "./store-schema.ts";

export function acceptTask(db: Database, claim: InboundClaim, task: TaskRecord): boolean {
  return transaction(db, () => {
    const existing = query(db,
      `SELECT 1 FROM inbound WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    )[0];
    if (existing) return false;
    const now = Date.now();
    run(db,
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
    run(db,
      `INSERT INTO tasks(
         id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd,
         state, accepted_at, message_seq, history_eligible,
         relay_channel_type, relay_channel_id, relay_thread_name
       )
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)`,
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
        task.messageSeq,
        task.historyEligible ? 1 : 0,
        task.relayChannelType,
        task.relayChannelId,
        task.relayThreadName,
      ],
    );
    return true;
  });
}

export function claimCommand(db: Database, claim: InboundClaim): boolean {
  return transaction(db, () => {
    const existing = query(db,
      `SELECT kind, command_state FROM inbound
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
      [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
    )[0];
    if (existing) {
      if (existing.kind !== "command" || existing.command_state === "completed" || existing.command_state === "pending") {
        return false;
      }
      run(db,
        `UPDATE inbound SET command_state = 'pending'
         WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
        [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
      );
      return true;
    }
    run(db,
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

export function completeCommand(db: Database, claim: InboundClaim): void {
  run(db,
    `UPDATE inbound SET command_state = 'completed'
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function failCommand(db: Database, claim: InboundClaim): void {
  run(db,
    `UPDATE inbound SET command_state = 'failed'
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function releaseClaim(db: Database, claim: InboundClaim): void {
  run(db,
    `DELETE FROM inbound
     WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ? AND kind = 'command'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId],
  );
}

export function getTask(db: Database, taskId: string): TaskRecord | null {
  const row = query(db, "SELECT * FROM tasks WHERE id = ?", [taskId])[0];
  return row ? taskFromRow(row) : null;
}

export function getLatestTask(db: Database, botUid: string, target: ChatTarget): TaskRecord | null {
  const row = query(db,
    `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} ORDER BY accepted_at DESC, rowid DESC LIMIT 1`,
    [botUid, ...targetValues(target)],
  )[0];
  return row ? taskFromRow(row) : null;
}

export function getActiveTask(db: Database, botUid: string, target: ChatTarget): TaskRecord | null {
  const row = query(db,
    `SELECT * FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state IN ('queued', 'started')
     ORDER BY accepted_at ASC, rowid ASC LIMIT 1`,
    [botUid, ...targetValues(target)],
  )[0];
  return row ? taskFromRow(row) : null;
}

export function countQueuedTasks(db: Database, botUid: string, target: ChatTarget): number {
  const row = query(db,
    `SELECT COUNT(*) AS count FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
    [botUid, ...targetValues(target)],
  )[0];
  return asNumber(row?.count);
}

export function listQueuedTasks(db: Database): TaskRecord[] {
  return query(db,
    "SELECT * FROM tasks WHERE state = 'queued' ORDER BY accepted_at ASC, rowid ASC",
  ).map((row) => taskFromRow(row));
}

export function markStarted(db: Database, taskId: string): boolean {
  return run(db,
    "UPDATE tasks SET state = 'started', started_at = ? WHERE id = ? AND state = 'queued'",
    [Date.now(), taskId],
  ).changes === 1;
}

export function markCompleted(db: Database, taskId: string, resultText: string | null): void {
  run(db,
    "UPDATE tasks SET state = 'completed', finished_at = ?, result_text = ?, error_text = NULL WHERE id = ?",
    [Date.now(), resultText, taskId],
  );
}

export function markFailed(db: Database, taskId: string, errorText: string): void {
  run(db,
    "UPDATE tasks SET state = 'failed', finished_at = ?, error_text = ?, result_text = NULL WHERE id = ?",
    [Date.now(), errorText, taskId],
  );
}

export function markCancelled(db: Database, taskId: string, errorText = "cancelled"): void {
  run(db,
    "UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ? WHERE id = ? AND state IN ('queued', 'started')",
    [Date.now(), errorText, taskId],
  );
}

export function markInterrupted(db: Database, taskId: string, errorText: string): void {
  run(db,
    "UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = ? WHERE id = ? AND state IN ('queued', 'started')",
    [Date.now(), errorText, taskId],
  );
}

/**
 * 启动恢复入口：一次事务内规范化该 Bot 遗留的在途任务，并把结果分成两组。
 * - `queued`：重排回队列、按接受顺序（accepted_at, rowid 升序）待执行的任务。没有派发过的行
 *   （`prompt_started = 0`）重放原文；已有 OMP 接受回执的行（`prompt_started = 1 AND prompt_accepted = 1`）
 *   必须续接原会话，调用方用 {@link isTaskResuming} 分流。
 * - `uncertain`：本次被 fail-closed 成 `interrupted` 的已派发行（`prompt_started = 1` 且
 *   `prompt_accepted = 0`，含升级时按「接受状态未知」记账的旧库 started 行）。prompt 已经落账却没有
 *   接受回执，无法判定它是否真的进过会话，因此既不重放原文也不错续到别的会话；这些行只报告本次
 *   结算的，后续启动不会重复上报。
 *
 * `queued` 行原样保留，终态行不参与，等待中的提问不动，调度也不在这里发生。`session_id` 与 relay
 * 等元数据随行保留。
 */
export function prepareTasksForRestart(db: Database, botUid: string): TaskRestartRecovery {
  return transaction(db, () => {
    const uncertainIds = query(db,
      `SELECT id FROM tasks
       WHERE bot_uid = ? AND state = 'started' AND prompt_started = 1 AND prompt_accepted = 0`,
      [botUid],
    ).map((row) => asString(row.id));
    run(db,
      `UPDATE tasks SET state = 'interrupted', finished_at = ?,
         error_text = 'prompt acceptance unknown at restart (dispatch recorded, worker receipt missing)'
       WHERE bot_uid = ? AND state = 'started' AND prompt_started = 1 AND prompt_accepted = 0`,
      [Date.now(), botUid],
    );
    run(db, "UPDATE tasks SET state = 'queued' WHERE bot_uid = ? AND state = 'started'", [botUid]);
    // 两组行一次性取回并按同一顺序排序，再按状态拆开：只有本次结算的 uncertain 行进入 IN 列表。
    const uncertainRows = uncertainIds.length === 0 ? "" : ` OR id IN (${uncertainIds.map(() => "?").join(", ")})`;
    const rows = query(db,
      `SELECT * FROM tasks WHERE bot_uid = ? AND (state = 'queued'${uncertainRows})
       ORDER BY accepted_at ASC, rowid ASC`,
      [botUid, ...uncertainIds],
    ).map((row) => taskFromRow(row));
    return {
      queued: rows.filter((task) => task.state === "queued"),
      uncertain: rows.filter((task) => task.state === "interrupted"),
    };
  });
}

/**
 * 发出 prompt 之前的一次性落账：置 `prompt_started` 并记录真实会话身份，同时清掉接受回执——
 * 新一次派发默认还没有被 OMP 接受。只有仍在 `started`、且尚未标记过的任务写得进去：其他状态
 * 说明调用方拿错了行，重复标记则是在重放原输入并覆盖恢复所需的会话身份，两者都直接抛错而不是
 * 静默改写。
 */
export function markTaskPromptStarted(db: Database, taskId: string, sessionId: string): void {
  const marked = run(db,
    "UPDATE tasks SET prompt_started = 1, prompt_accepted = 0, session_id = ? WHERE id = ? AND state = 'started' AND prompt_started = 0",
    [sessionId, taskId],
  ).changes === 1;
  if (marked) return;
  const row = query(db, "SELECT state, prompt_started FROM tasks WHERE id = ?", [taskId])[0];
  if (!row) throw new Error(`task ${taskId} does not exist`);
  if (asString(row.state) !== "started") {
    throw new Error(`task ${taskId} is ${asString(row.state)}, not started; the prompt marker follows markStarted`);
  }
  throw new Error(`task ${taskId} already recorded a prompt; resume its session instead of replaying the input`);
}

/**
 * OMP 接受 prompt 的回执（worker 的 `prompt_accepted` 事件）：确认这次派发真的进了会话，重启时才
 * 能安全续接。只要求派发标记已经落账，不校验状态——回执与后续状态迁移在同一事件流里，晚到或重复
 * 到达都是幂等的，写终态行也无害；缺派发标记说明调用方没按「先标记再处理后续事件」的顺序接线。
 */
export function markTaskPromptAccepted(db: Database, taskId: string): void {
  const accepted = run(db,
    "UPDATE tasks SET prompt_accepted = 1 WHERE id = ? AND prompt_started = 1",
    [taskId],
  ).changes === 1;
  if (!accepted) {
    throw new Error(`task ${taskId} has no dispatched prompt; markTaskPromptStarted comes first`);
  }
}

/**
 * 在途任务是否必须续接既有会话：非终态，且 prompt 已落账并收到 OMP 接受回执。缺回执的行在恢复时
 * 会被 fail-closed 成 interrupted，因此恢复流程看到的可续接任务一定满足这里。
 */
export function isTaskResuming(db: Database, taskId: string): boolean {
  const row = query(db,
    `SELECT 1 FROM tasks
     WHERE id = ? AND state IN ('queued', 'started') AND prompt_started = 1 AND prompt_accepted = 1`,
    [taskId],
  )[0];
  return row !== undefined;
}

export function addRestartNotice(db: Database, record: RestartNoticeRecord): void {
  run(db,
    `INSERT INTO restart_notices(id, bot_uid, channel_type, channel_id, requested_at)
     VALUES(?, ?, ?, ?, ?)`,
    [record.id, record.botUid, record.channelType, record.channelId, record.requestedAt],
  );
}

export function listRestartNotices(db: Database, botUid: string): RestartNoticeRecord[] {
  return query(db,
    "SELECT * FROM restart_notices WHERE bot_uid = ? ORDER BY requested_at ASC, rowid ASC",
    [botUid],
  ).map((row) => ({
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    requestedAt: asNumber(row.requested_at),
  }));
}

export function clearRestartNotice(db: Database, id: string): void {
  run(db, "DELETE FROM restart_notices WHERE id = ?", [id]);
}

export function cancelQueued(db: Database, botUid: string, target: ChatTarget): string[] {
  return transaction(db, () => {
    const rows = query(db,
      `SELECT id FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [botUid, ...targetValues(target)],
    );
    run(db,
      `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = 'cancelled before start'
       WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [Date.now(), botUid, ...targetValues(target)],
    );
    return rows.map((row) => asString(row.id));
  });
}
