/**
 * 任务与 inbound 认领：去重声明、命令声明与状态、任务状态迁移、重启通知。
 */

import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { InboundClaim, StoppedTaskRecovery, TaskRecord, TaskRestartRecovery, TaskStopIntent } from "./store-records.ts";
import { asNumber, asStopIntent, asString, query, run, targetValues, targetWhere, taskFromRow, transaction } from "./store-schema.ts";
import { cancelTaskQuestions } from "./store-questions.ts";

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
  transaction(db, () => {
    const completed = run(db,
      "UPDATE tasks SET state = 'completed', finished_at = ?, result_text = ?, error_text = NULL, stop_intent = NULL WHERE id = ?",
      [Date.now(), resultText, taskId],
    ).changes === 1;
    // 终态与关闭本任务待答提问同事务提交：commit 与 finally 清题之间被杀，旧卡不能再唤醒新回合。
    // 任务自行结束记 `ended`——它没有用户明确停止，迟到提交按新输入接受。
    if (completed) cancelTaskQuestions(db, taskId, "ended");
  });
}

export function markFailed(db: Database, taskId: string, errorText: string): void {
  transaction(db, () => {
    const failed = run(db,
      "UPDATE tasks SET state = 'failed', finished_at = ?, error_text = ?, result_text = NULL, stop_intent = NULL WHERE id = ?",
      [Date.now(), errorText, taskId],
    ).changes === 1;
    if (failed) cancelTaskQuestions(db, taskId, "ended");
  });
}

export function markCancelled(db: Database, taskId: string, errorText = "cancelled"): void {
  transaction(db, () => {
    const cancelled = run(db,
      "UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = ?, stop_intent = NULL WHERE id = ? AND state IN ('queued', 'started')",
      [Date.now(), errorText, taskId],
    ).changes === 1;
    // 用户取消是终态：待答提问与任务终态同事务关闭，迟到的回答不再唤醒新回合。
    if (cancelled) cancelTaskQuestions(db, taskId, "cancelled");
  });
}

/**
 * 停下这一回合。持久停止意图是 `interrupt` 时，任务终态与关闭待答提问在同一事务里完成——用户在
 * 线中断后，遗留提问不能再被点开、迟到的回答不能再唤醒新回合；`question_timeout` 与无意图的
 * 中断路径保持提问可答（迟到答复照在线路径唤醒新回合）。
 */
export function markInterrupted(db: Database, taskId: string, errorText: string): void {
  transaction(db, () => {
    const intent = asStopIntent(query(db, "SELECT stop_intent FROM tasks WHERE id = ?", [taskId])[0]?.stop_intent);
    const interrupted = run(db,
      "UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = ?, stop_intent = NULL WHERE id = ? AND state IN ('queued', 'started')",
      [Date.now(), errorText, taskId],
    ).changes === 1;
    if (interrupted && intent === "interrupt") cancelTaskQuestions(db, taskId, "interrupted");
  });
}

/**
 * 用户已确认的停止意图，先落库再动手：进程被 SIGKILL 时，这一列是「这一回合是被叫停的」唯一证据，
 * 重启恢复据此落终态而不是自动执行或续接。只写仍在途（queued/started）的行——行已终态说明无需再
 * 叫停，静默 no-op 返回 false（命令处理器与 settle 之间存在竞态）；重复写是允许的（再次 /interrupt
 * 重新武装）。
 *
 * `interrupt` 的意图与「关闭本任务待答提问（记 `interrupted`）」在同一事务里提交：这些行对迟到提交
 * 不可接受，而在线中断与重启结算必须给出同一个分流答案。
 *
 * `cancelQuestionIds` 只给 `question_timeout` 用（提问超时的恢复快照遗留题）：意图写入与这批行的取消
 * （记 `ended`）在同一事务里提交，本回合正在等的那批题仍保持可答。
 */
export function markTaskStopIntent(
  db: Database,
  taskId: string,
  intent: TaskStopIntent,
  cancelQuestionIds: readonly string[] = [],
): boolean {
  return transaction(db, () => {
    const written = run(db,
      "UPDATE tasks SET stop_intent = ? WHERE id = ? AND state IN ('queued', 'started')",
      [intent, taskId],
    ).changes === 1;
    // 守卫失败（行已终态）时连题也不动：这次叫停本来就没生效。
    if (!written) return false;
    if (intent === "interrupt") cancelTaskQuestions(db, taskId, "interrupted");
    // 只有 `question_timeout` 用选择集：它按设计保留本回合正在等的提问（B），却必须原子关闭恢复快照里
    // 没人接手的那批遗留题（A），否则被叫停的回合结束后 A 仍可点开、唤醒新回合。逐行校验归属与状态，
    // 借用调用方传来的 id 不能越界取消别的任务或已收场的行。
    if (intent === "question_timeout") {
      for (const id of cancelQuestionIds) cancelOwnedPendingQuestion(db, taskId, id);
    }
    return true;
  });
}

/** 只取消「id 指向、且确实属于该任务、且仍 pending」的那一行（记 `ended`）；其余情况静默 no-op。 */
function cancelOwnedPendingQuestion(db: Database, taskId: string, id: string): boolean {
  return run(db,
    "UPDATE questions SET state = 'cancelled', answered_at = ?, closed_reason = 'ended' WHERE id = ? AND task_id = ? AND state = 'pending'",
    [Date.now(), id, taskId],
  ).changes === 1;
}

/** 停止意图被真实工作结果覆盖（interrupt 之后这一回合自己跑完了）时清除；无条件清 NULL。 */
export function clearTaskStopIntent(db: Database, taskId: string): void {
  run(db, "UPDATE tasks SET stop_intent = NULL WHERE id = ?", [taskId]);
}

/**
 * 启动恢复入口：一次事务内规范化该 Bot 遗留的在途任务，并把结果分成三组。
 * - `stopped`：先结算持久停止意图（`stop_intent`）——进程被 SIGKILL 时它是「这一回合是被叫停的」唯一
 *   证据。`interrupt` 与 `question_timeout` 都落 `interrupted`，说明文案由调用方传入（必须与在线路径
 *   同源，因为它会被回传通知当「原因」原样使用）；结算清列并把原意图随记录交回。intent 为 `interrupt`
 *   的行连同其待答提问一起在本次事务里关闭（迟到答案不能再唤醒新回合）；`question_timeout` 的提问
 *   保持可答，由服务按原因分流。
 * - `uncertain`：本次被 fail-closed 成 `interrupted` 的已派发行（`prompt_started = 1` 且
 *   `prompt_accepted = 0`，含升级时按「接受状态未知」记账的旧库 started 行）。prompt 已经落账却没有
 *   接受回执，无法判定它是否真的进过会话，因此既不重放原文也不错续到别的会话；其待答提问与终态在
 *   同一事务里关闭。
 * - `queued`：重排回队列、按接受顺序待执行的任务。没有派发过的行（`prompt_started = 0`）重放原文；
 *   已有 OMP 接受回执的行（`prompt_started = 1 AND prompt_accepted = 1`）必须续接原会话，调用方用
 *   {@link isTaskResuming} 分流。
 *
 * `stopped` 与 `uncertain` 只含本次结算的行，后续启动不会重复上报；三组各自按 accepted_at, rowid
 * 升序。`queued` 行原样保留，终态行不参与，等待中的提问不动，调度也不在这里发生；`session_id` 与
 * relay 等元数据随行保留。
 */
export function prepareTasksForRestart(
  db: Database,
  botUid: string,
  reasonTexts: Record<TaskStopIntent, string>,
): TaskRestartRecovery {
  return transaction(db, () => {
    // 1) 停止意图优先：用户确认过的叫停不该因为一次重启变成自动执行或自动续接。
    const stopped: StoppedTaskRecovery[] = [];
    for (const row of query(db,
      `SELECT id, stop_intent FROM tasks
       WHERE bot_uid = ? AND state IN ('queued', 'started') AND stop_intent IS NOT NULL
       ORDER BY accepted_at ASC, rowid ASC`,
      [botUid],
    )) {
      const stopIntent = asStopIntent(row.stop_intent);
      if (stopIntent === null) continue;
      const taskId = asString(row.id);
      run(db,
        `UPDATE tasks SET state = 'interrupted', finished_at = ?, error_text = ?, stop_intent = NULL
         WHERE id = ?`,
        [Date.now(), reasonTexts[stopIntent], taskId],
      );
      const settled = query(db, "SELECT * FROM tasks WHERE id = ?", [taskId])[0];
      if (settled) stopped.push({ ...taskFromRow(settled), stopIntent });
      // 中断的回合连同它的待答提问一起在本次事务里关闭（记 `interrupted`）：恢复过程本身再被 SIGKILL
      // 也不会留下可被点开、可唤醒新回合的旧卡；提问超时停下的行保持可答（迟到答复照在线路径唤醒）。
      if (stopIntent === "interrupt") cancelTaskQuestions(db, taskId, "interrupted");
    }
    // 2) 已派发但没有接受回执的行：接受状态未知，fail-closed（不重放、不错续）。
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
    // 这些回合已经失败收场：它们的待答提问同样在本次事务里关闭（记 `ended`——不是用户叫停，迟到提交
    // 按新输入接受）。
    for (const taskId of uncertainIds) cancelTaskQuestions(db, taskId, "ended");
    // 3) 其余在途行重排回队列，返回待执行集合。
    run(db, "UPDATE tasks SET state = 'queued' WHERE bot_uid = ? AND state = 'started'", [botUid]);
    return {
      queued: query(db,
        "SELECT * FROM tasks WHERE bot_uid = ? AND state = 'queued' ORDER BY accepted_at ASC, rowid ASC",
        [botUid],
      ).map((row) => taskFromRow(row)),
      uncertain: uncertainIds.length === 0 ? [] : query(db,
        `SELECT * FROM tasks WHERE id IN (${uncertainIds.map(() => "?").join(", ")})
         ORDER BY accepted_at ASC, rowid ASC`,
        uncertainIds,
      ).map((row) => taskFromRow(row)),
      stopped,
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

export function clearRestartNotices(db: Database, botUid: string): void {
  run(db, "DELETE FROM restart_notices WHERE bot_uid = ?", [botUid]);
}

export function cancelQueued(db: Database, botUid: string, target: ChatTarget): string[] {
  return transaction(db, () => {
    const rows = query(db,
      `SELECT id FROM tasks WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [botUid, ...targetValues(target)],
    );
    const taskIds = rows.map((row) => asString(row.id));
    // 与 markCancelled 同一收口：取消是终态，停止意图清掉、选中任务的待答提问在同一事务里关闭，
    // 否则「cancelQueued → 通知/清理之间被杀」会留下可点开、可唤醒新回合的旧卡（恢复回来的 1/1
    // 任务排队等前序时正是这个窗口）。
    run(db,
      `UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = 'cancelled before start', stop_intent = NULL
       WHERE bot_uid = ? AND ${targetWhere(target)} AND state = 'queued'`,
      [Date.now(), botUid, ...targetValues(target)],
    );
    for (const taskId of taskIds) cancelTaskQuestions(db, taskId, "cancelled");
    return taskIds;
  });
}
