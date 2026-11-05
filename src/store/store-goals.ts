import { Database } from "bun:sqlite";
import type { ChatTarget, WorkerGoal } from "../contracts.ts";
import type { GoalSessionRecord, GoalStopIntent, GoalTaskRecord, GoalTaskRequest, TaskRecord } from "./store-records.ts";
import { getBinding, initializeBindingSessionFromTask, updateBindingSession } from "./store-chats.ts";
import { asNumber, asNullableString, asString, query, run, targetValues, targetWhere, transaction } from "./store-schema.ts";
import { cancelTaskQuestions } from "./store-questions.ts";

function goalTaskFromRow(row: Record<string, unknown>): GoalTaskRecord {
  const op = asString(row.request_op);
  const request: GoalTaskRequest = op === "create"
    ? {
      op: "create",
      objective: asString(row.objective),
      ...(row.token_budget === null || row.token_budget === undefined
        ? {}
        : { tokenBudget: asNumber(row.token_budget) }),
    }
    : { op: "resume" };
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    taskId: asString(row.task_id),
    sessionId: asString(row.session_id),
    dispatchOp: row.dispatch_op === "create" || row.dispatch_op === "resume" || row.dispatch_op === "recover"
      ? row.dispatch_op
      : null,
    request,
    dispatched: asNumber(row.dispatched) !== 0,
    accepted: asNumber(row.accepted) !== 0,
    agentAccepted: asNumber(row.agent_accepted) !== 0,
    acceptedGoalId: asNullableString(row.accepted_goal_id),
  };
}

function goalSessionFromRow(row: Record<string, unknown>): GoalSessionRecord {
  const status = row.status;
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    goalId: asNullableString(row.goal_id),
    objective: asString(row.objective),
    status: status === "active" || status === "paused" || status === "budget-limited"
      || status === "complete" || status === "dropped"
      ? status
      : null,
    tokensUsed: asNumber(row.tokens_used),
    timeUsedSeconds: asNumber(row.time_used_seconds),
    tokenBudget: row.token_budget === null || row.token_budget === undefined
      ? null
      : asNumber(row.token_budget),
    continuationAuthorized: asNumber(row.continuation_authorized) !== 0,
    stopIntent: row.stop_intent === "pause" || row.stop_intent === "drop"
      || row.stop_intent === "interrupt" || row.stop_intent === "question_timeout" || row.stop_intent === "cancel"
      ? row.stop_intent
      : null,
  };
}

export function getGoalTask(db: Database, taskId: string): GoalTaskRecord | null {
  const row = query(db, "SELECT * FROM goal_tasks WHERE task_id = ?", [taskId])[0];
  return row ? goalTaskFromRow(row) : null;
}

export function getGoalSession(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string,
): GoalSessionRecord | null {
  const row = query(db,
    `SELECT * FROM goal_sessions WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
    [botUid, ...targetValues(target), sessionId],
  )[0];
  return row ? goalSessionFromRow(row) : null;
}

export function hasActiveGoalTask(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string,
): boolean {
  return query(db,
    `SELECT 1 FROM goal_tasks AS goal JOIN tasks AS task ON task.id = goal.task_id
     WHERE goal.bot_uid = ? AND goal.channel_type = ? AND goal.channel_id = ? AND goal.session_id = ?
       AND task.state IN ('queued', 'started') LIMIT 1`,
    [botUid, target.channelType, target.channelId, sessionId],
  )[0] !== undefined;
}

/** Inserted inside acceptTask's transaction, so the typed request and task become durable atomically. */
export function insertGoalTask(db: Database, task: TaskRecord, request: GoalTaskRequest): void {
  const now = Date.now();
  if (request.op === "create") {
    const existing = query(db,
      `SELECT 1 FROM goal_sessions WHERE bot_uid = ? AND ${targetWhere(task)} AND session_id = ?`,
      [task.botUid, ...targetValues(task), task.sessionId],
    )[0];
    if (existing) throw new Error("a native goal already exists for this exact session");
    run(db,
      `INSERT INTO goal_sessions(
         bot_uid, channel_type, channel_id, session_id, goal_id, objective, status,
         tokens_used, time_used_seconds, token_budget, continuation_authorized, stop_intent, updated_at
       ) VALUES(?, ?, ?, ?, NULL, ?, NULL, 0, 0, ?, 1, NULL, ?)`,
      [task.botUid, task.channelType, task.channelId, task.sessionId, request.objective, request.tokenBudget ?? null, now],
    );
  } else {
    const authorized = run(db,
      `UPDATE goal_sessions SET continuation_authorized = 1, stop_intent = NULL, updated_at = ?
       WHERE bot_uid = ? AND ${targetWhere(task)} AND session_id = ? AND goal_id IS NOT NULL AND status = 'paused'`,
      [now, task.botUid, ...targetValues(task), task.sessionId],
    ).changes === 1;
    if (!authorized) throw new Error("no paused native goal exists for this exact session");
  }
  run(db,
    `INSERT INTO goal_tasks(
       task_id, bot_uid, channel_type, channel_id, session_id, request_op, objective, token_budget,
       dispatched, accepted, accepted_goal_id, created_at, updated_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?)`,
    [
      task.id,
      task.botUid,
      task.channelType,
      task.channelId,
      task.sessionId,
      request.op,
      request.op === "create" ? request.objective : "",
      request.op === "create" ? request.tokenBudget ?? null : null,
      now,
      now,
    ],
  );
}

/** Commit OMP's fresh-session identity before any structured goal RPC is sent. */
export function bindGoalTaskSession(
  db: Database,
  taskId: string,
  expectedSessionId: string,
  sessionId: string,
  sessionFile: string,
): void {
  transaction(db, () => {
    const request = getGoalTask(db, taskId);
    if (
      !request
      || request.dispatched
      || request.sessionId !== expectedSessionId
      || request.request.op !== "create"
    ) {
      throw new Error(`goal task ${taskId} cannot change its session after dispatch or outside fresh create`);
    }
    const binding = getBinding(db, request.botUid, request);
    if (!binding || binding.sessionId !== expectedSessionId || binding.sessionFile !== null) {
      throw new Error(`goal task ${taskId} does not own a fresh unpersisted binding`);
    }
    const task = query(db, "SELECT state, session_id FROM tasks WHERE id = ?", [taskId])[0];
    if (task?.state !== "started" || asString(task.session_id) !== expectedSessionId) {
      throw new Error(`goal task ${taskId} changed task session before dispatch`);
    }
    if (run(db,
      `UPDATE goal_sessions SET session_id = ?
       WHERE bot_uid = ? AND ${targetWhere(request)} AND session_id = ?`,
      [sessionId, request.botUid, ...targetValues(request), expectedSessionId],
    ).changes !== 1) {
      throw new Error(`goal task ${taskId} has no placeholder goal session`);
    }
    if (run(db,
      "UPDATE goal_tasks SET session_id = ?, updated_at = ? WHERE task_id = ? AND session_id = ? AND dispatched = 0",
      [sessionId, Date.now(), taskId, expectedSessionId],
    ).changes !== 1) {
      throw new Error(`goal task ${taskId} session reservation changed before dispatch`);
    }
    if (binding.cwdPinned === true) {
      if (!initializeBindingSessionFromTask(db, request.botUid, request, taskId, binding, sessionId, sessionFile)) {
        throw new Error(`goal task ${taskId} could not atomically bind its initial thread session`);
      }
    } else {
      updateBindingSession(db, request.botUid, request, sessionId, sessionFile);
    }
    if (run(db,
      "UPDATE tasks SET session_id = ? WHERE id = ? AND session_id = ? AND state = 'started'",
      [sessionId, taskId, expectedSessionId],
    ).changes !== 1) {
      throw new Error(`goal task ${taskId} task session changed before dispatch`);
    }
  });
}

export function markGoalTaskDispatched(
  db: Database,
  taskId: string,
  sessionId: string,
  dispatchOp?: "create" | "resume" | "recover",
): void {
  transaction(db, () => {
    const row = query(db, "SELECT * FROM goal_tasks WHERE task_id = ?", [taskId])[0];
    if (!row) throw new Error(`goal task ${taskId} does not exist`);
    const request = goalTaskFromRow(row);
    if (request.sessionId !== sessionId) {
      throw new Error(`goal task ${taskId} session identity mismatch before dispatch`);
    }
    const task = query(db, "SELECT state FROM tasks WHERE id = ?", [taskId])[0];
    if (task?.state !== "started") throw new Error(`goal task ${taskId} must be started before dispatch`);
    const nextOp = dispatchOp ?? request.request.op;
    if (nextOp === "recover") {
      if (!request.dispatched || !request.accepted || request.acceptedGoalId === null) {
        throw new Error(`goal task ${taskId} has no previously accepted goal to recover`);
      }
    } else {
      if (request.dispatched) throw new Error(`goal task ${taskId} already recorded dispatch`);
      if (nextOp !== request.request.op) throw new Error(`goal task ${taskId} dispatch operation mismatch`);
    }
    const session = getGoalSession(db, request.botUid, request, sessionId);
    if (
      !session
      || !session.continuationAuthorized
      || session.stopIntent !== null
      || (nextOp === "recover" && session.goalId !== request.acceptedGoalId)
    ) {
      throw new Error(`goal task ${taskId} continuation authorization or goal identity has been revoked`);
    }
    const marked = run(db,
      `UPDATE goal_tasks SET dispatch_op = ?, dispatched = 1,
         accepted = CASE WHEN ? = 'recover' THEN accepted ELSE 0 END,
         accepted_goal_id = CASE WHEN ? = 'recover' THEN accepted_goal_id ELSE NULL END,
         agent_accepted = 0, updated_at = ?
       WHERE task_id = ? AND session_id = ? AND (dispatched = 0 OR accepted = 1)`,
      [nextOp, nextOp, nextOp, Date.now(), taskId, sessionId],
    ).changes === 1;
    if (!marked) throw new Error(`goal task ${taskId} dispatch marker was not recorded`);
  });
}

/** The native acceptance receipt persists its exact id without ever re-authorizing continuation. */
export function markGoalTaskAccepted(db: Database, taskId: string, goalId: string): void {
  transaction(db, () => {
    if (goalId.trim().length === 0) throw new Error("native goal acceptance had an empty goal id");
    const row = query(db, "SELECT * FROM goal_tasks WHERE task_id = ?", [taskId])[0];
    if (!row) throw new Error(`goal task ${taskId} does not exist`);
    if (asNumber(row.dispatched) === 0) throw new Error(`goal task ${taskId} has no dispatch receipt`);
    const acceptedGoalId = asNullableString(row.accepted_goal_id);
    if (acceptedGoalId !== null && acceptedGoalId !== goalId) {
      throw new Error(`goal task ${taskId} acceptance identity mismatch`);
    }
    const request = goalTaskFromRow(row);
    const session = getGoalSession(db, request.botUid, request, request.sessionId);
    if (!session) throw new Error(`goal task ${taskId} has no exact goal session`);
    if (session.goalId !== null && session.goalId !== goalId) {
      throw new Error(`goal task ${taskId} native goal id mismatch`);
    }
    run(db,
      "UPDATE goal_tasks SET accepted = 1, accepted_goal_id = ?, updated_at = ? WHERE task_id = ? AND dispatched = 1",
      [goalId, Date.now(), taskId],
    );
    run(db,
      `UPDATE goal_sessions SET goal_id = ?, updated_at = ?
       WHERE bot_uid = ? AND ${targetWhere(request)} AND session_id = ?`,
      [goalId, Date.now(), request.botUid, ...targetValues(request), request.sessionId],
    );
  });
}

/** Generation acceptance is independent of the native control acceptance and its exact goal id. */
export function markGoalTaskAgentAccepted(db: Database, taskId: string): void {
  const row = query(db, "SELECT dispatched FROM goal_tasks WHERE task_id = ?", [taskId])[0];
  if (!row || asNumber(row.dispatched) === 0) {
    throw new Error(`goal task ${taskId} has no dispatched generation`);
  }
  run(db,
    "UPDATE goal_tasks SET agent_accepted = 1, updated_at = ? WHERE task_id = ? AND dispatched = 1",
    [Date.now(), taskId],
  );
}

export function updateGoalTaskProgress(db: Database, taskId: string, goal: WorkerGoal | null): void {
  if (goal === null) return;
  transaction(db, () => {
    const request = getGoalTask(db, taskId);
    if (!request?.accepted || request.acceptedGoalId === null) {
      throw new Error(`goal task ${taskId} progress arrived before native acceptance`);
    }
    if (request.acceptedGoalId !== goal.id) throw new Error(`goal task ${taskId} progress identity mismatch`);
    persistGoalSessionProgress(db, request.botUid, request, request.sessionId, goal, false);
  });
}

/** Apply a settled run result; unlike intermediate snapshots, terminal pause revokes auto-recovery. */
export function finishGoalTaskProgress(db: Database, taskId: string, goal: WorkerGoal): void {
  transaction(db, () => {
    const request = getGoalTask(db, taskId);
    if (!request?.accepted || request.acceptedGoalId === null) {
      throw new Error(`goal task ${taskId} settled without native acceptance`);
    }
    if (request.acceptedGoalId !== goal.id) throw new Error(`goal task ${taskId} settled goal identity mismatch`);
    persistGoalSessionProgress(db, request.botUid, request, request.sessionId, goal, true);
  });
}

export function saveGoalSessionProgress(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string,
  goal: WorkerGoal,
): void {
  transaction(db, () => persistGoalSessionProgress(db, botUid, target, sessionId, goal, false));
}

export function clearGoalSession(db: Database, botUid: string, target: ChatTarget, sessionId: string): void {
  transaction(db, () => {
    run(db,
      `DELETE FROM goal_sessions WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
      [botUid, ...targetValues(target), sessionId],
    );
  });
}

function persistGoalSessionProgress(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string,
  goal: WorkerGoal,
  revokeAuthorizationOnTerminal: boolean,
): void {
  const previous = getGoalSession(db, botUid, target, sessionId);
  if (previous?.goalId !== null && previous?.goalId !== undefined && previous.goalId !== goal.id) {
    throw new Error("native goal identity mismatch for this exact session");
  }
  const status = goal.status;
  const statusUpdatesAuthorization = revokeAuthorizationOnTerminal && goal.status !== "active" ? 1 : 0;
  if (!previous) {
    run(db,
      `INSERT INTO goal_sessions(
         bot_uid, channel_type, channel_id, session_id, goal_id, objective, status,
         tokens_used, time_used_seconds, token_budget, continuation_authorized, stop_intent, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)`,
      [
        botUid,
        target.channelType,
        target.channelId,
        sessionId,
        goal.id,
        goal.objective,
        goal.status,
        goal.tokensUsed,
        goal.timeUsedSeconds,
        goal.tokenBudget ?? null,
        Date.now(),
      ],
    );
    return;
  }
  run(db,
    `UPDATE goal_sessions SET goal_id = ?, objective = ?, status = ?, tokens_used = ?, time_used_seconds = ?,
       token_budget = ?, continuation_authorized = CASE WHEN ? = 1 THEN 0 ELSE continuation_authorized END,
       updated_at = ?
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
    [
      goal.id,
      goal.objective,
      status,
      goal.tokensUsed,
      goal.timeUsedSeconds,
      goal.tokenBudget ?? null,
      statusUpdatesAuthorization,
      Date.now(),
      botUid,
      ...targetValues(target),
      sessionId,
    ],
  );
}

export function isGoalTaskResuming(db: Database, taskId: string): boolean {
  return query(db,
    `SELECT 1 FROM goal_tasks AS task_goal
     JOIN tasks AS task ON task.id = task_goal.task_id
     JOIN goal_sessions AS session ON session.bot_uid = task_goal.bot_uid
       AND session.channel_type = task_goal.channel_type
       AND session.channel_id = task_goal.channel_id
       AND session.session_id = task_goal.session_id
     WHERE task_goal.task_id = ? AND task.state IN ('queued', 'started')
       AND task_goal.dispatched = 1 AND task_goal.accepted = 1
       AND task_goal.accepted_goal_id IS NOT NULL
       AND session.continuation_authorized = 1 AND session.stop_intent IS NULL`,
    [taskId],
  )[0] !== undefined;
}

/** Revoke first, then cancel only queued goal requests for this chat/session. */
export function stopGoalContinuation(
  db: Database,
  botUid: string,
  target: ChatTarget,
  sessionId: string | null,
  intent: GoalStopIntent,
  useTransaction = true,
): string[] {
  const work = (): string[] => {
    const sessionFilter = sessionId === null ? "" : " AND session_id = ?";
    run(db,
      `UPDATE goal_sessions SET continuation_authorized = 0,
         stop_intent = CASE
           WHEN stop_intent IN ('interrupt', 'question_timeout', 'cancel') THEN stop_intent
           WHEN stop_intent = 'drop' AND ? = 'pause' THEN stop_intent
           ELSE ? END,
         updated_at = ?
       WHERE bot_uid = ? AND ${targetWhere(target)}${sessionFilter}`,
      [intent, intent, Date.now(), botUid, ...targetValues(target), ...(sessionId === null ? [] : [sessionId])],
    );
    const sessionTaskFilter = sessionId === null ? "" : " AND goal.session_id = ?";
    const queued = query(db,
      `SELECT goal.task_id, goal.request_op, goal.session_id, goal.dispatched
       FROM goal_tasks AS goal JOIN tasks AS task ON task.id = goal.task_id
       WHERE goal.bot_uid = ? AND goal.channel_type = ? AND goal.channel_id = ?${sessionTaskFilter} AND task.state = 'queued'
       ORDER BY task.accepted_at, task.rowid`,
      [botUid, target.channelType, target.channelId, ...(sessionId === null ? [] : [sessionId])],
    );
    const taskIds = queued.map((row) => asString(row.task_id));
    for (const taskId of taskIds) {
      run(db,
        "UPDATE tasks SET state = 'cancelled', finished_at = ?, error_text = 'goal continuation revoked before start', stop_intent = NULL WHERE id = ? AND state = 'queued'",
        [Date.now(), taskId],
      );
      cancelTaskQuestions(db, taskId, "cancelled");
    }
    for (const row of queued) {
      if (row.request_op === "create" && asNumber(row.dispatched) === 0) {
        run(db,
          `DELETE FROM goal_sessions WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ? AND goal_id IS NULL`,
          [botUid, ...targetValues(target), asString(row.session_id)],
        );
      }
    }
    return taskIds;
  };
  return useTransaction ? transaction(db, work) : work();
}

export function stopGoalContinuationForTask(db: Database, taskId: string, intent: GoalStopIntent): string[] {
  const row = query(db,
    "SELECT bot_uid, channel_type, channel_id, session_id FROM tasks WHERE id = ?",
    [taskId],
  )[0];
  if (!row) return [];
  return stopGoalContinuation(db, asString(row.bot_uid), {
    channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
    channelId: asString(row.channel_id),
  }, asString(row.session_id), intent, false);
}

/** Startup recovery helper: tasks with a dispatched request but no receipt must never be replayed. */
export function undispatchedGoalAcceptanceIds(db: Database, botUid: string): string[] {
  return query(db,
    `SELECT goal.task_id FROM goal_tasks AS goal JOIN tasks AS task ON task.id = goal.task_id
     WHERE goal.bot_uid = ? AND task.state = 'started' AND goal.dispatched = 1 AND goal.accepted = 0
     ORDER BY task.accepted_at, task.rowid`,
    [botUid],
  ).map((row) => asString(row.task_id));
}

/** Started tasks whose accepted native run was explicitly revoked are terminal, never goal-recovered. */
export function revokedGoalTaskIds(db: Database, botUid: string): string[] {
  return query(db,
    `SELECT goal.task_id FROM goal_tasks AS goal
     JOIN tasks AS task ON task.id = goal.task_id
     JOIN goal_sessions AS session ON session.bot_uid = goal.bot_uid
       AND session.channel_type = goal.channel_type
       AND session.channel_id = goal.channel_id
       AND session.session_id = goal.session_id
     WHERE goal.bot_uid = ? AND task.state = 'started' AND goal.dispatched = 1 AND goal.accepted = 1
       AND (session.continuation_authorized = 0 OR session.stop_intent IS NOT NULL)
     ORDER BY task.accepted_at, task.rowid`,
    [botUid],
  ).map((row) => asString(row.task_id));
}
