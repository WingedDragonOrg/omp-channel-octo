/**
 * Loop 子区关联失效事实：归档、`/thread merge`、`/thread remove` 或观察到的服务端归档状态都会
 * 在这里留下一条持久记录。记录绑定当时的路由版本与子区 ID；显式重新绑定/创建新子区会清除它，
 * 因此人工重新打开子区不会恢复自动路由，而目标改绑可以恢复。
 */

import type { Database } from "bun:sqlite";
import type {
  LoopTaskKey,
  LoopTaskRouteEnding,
  LoopTaskRouteEndingReason,
  LoopTaskRouteEndingState,
} from "../loop/types.ts";
import { asNumber, asString, query, run, transaction, type SqlRow } from "./store-schema.ts";

function loopTaskRouteEndingFromRow(row: SqlRow): LoopTaskRouteEnding {
  return {
    botUid: asString(row.bot_uid),
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    taskId: asString(row.task_id),
    threadChannelId: asString(row.thread_channel_id),
    routeVersion: asNumber(row.route_version),
    state: asString(row.state) as LoopTaskRouteEndingState,
    reason: asString(row.reason) as LoopTaskRouteEndingReason,
    updatedAt: asNumber(row.updated_at),
  };
}

export function getLoopTaskRouteEnding(db: Database, key: LoopTaskKey): LoopTaskRouteEnding | null {
  const row = query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, thread_channel_id, route_version,
            state, reason, updated_at
     FROM loop_route_endings
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?`,
    [key.botUid, key.groupId, key.workspaceId, key.taskId],
  )[0];
  return row ? loopTaskRouteEndingFromRow(row) : null;
}

export function listLoopTaskRouteEndings(db: Database, botUid: string): LoopTaskRouteEnding[] {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, thread_channel_id, route_version,
            state, reason, updated_at
     FROM loop_route_endings WHERE bot_uid = ? ORDER BY updated_at ASC, task_id ASC`,
    [botUid],
  ).map((row) => loopTaskRouteEndingFromRow(row));
}

/**
 * Marks every current route to this child. A confirmed `ended` fact is never downgraded to
 * `pending` by a later in-flight operation on the same thread.
 */
export function markLoopThreadRoutesEnding(
  db: Database,
  botUid: string,
  groupId: string,
  threadChannelId: string,
  reason: LoopTaskRouteEndingReason,
  state: LoopTaskRouteEndingState,
): number {
  return transaction(db, () => {
    const now = Date.now();
    let changed = 0;
    for (const row of query(db,
      `SELECT bot_uid, group_id, workspace_id, task_id, thread_channel_id, version
       FROM loop_task_routes
       WHERE bot_uid = ? AND group_id = ? AND thread_channel_id = ?`,
      [botUid, groupId, threadChannelId],
    )) {
      const key: LoopTaskKey = {
        botUid: asString(row.bot_uid),
        groupId: asString(row.group_id),
        workspaceId: asString(row.workspace_id),
        taskId: asString(row.task_id),
      };
      run(db,
        `INSERT INTO loop_route_endings(
           bot_uid, group_id, workspace_id, task_id, thread_channel_id, route_version,
           state, reason, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(bot_uid, group_id, workspace_id, task_id) DO UPDATE SET
           thread_channel_id = excluded.thread_channel_id,
           route_version = excluded.route_version,
           state = CASE
             WHEN loop_route_endings.state = 'ended' AND excluded.state = 'pending'
             THEN 'ended'
             ELSE excluded.state
           END,
           reason = CASE
             WHEN loop_route_endings.state = 'ended' AND excluded.state = 'pending'
             THEN loop_route_endings.reason
             ELSE excluded.reason
           END,
           updated_at = excluded.updated_at`,
        [
          key.botUid,
          key.groupId,
          key.workspaceId,
          key.taskId,
          threadChannelId,
          asNumber(row.version),
          state,
          reason,
          now,
        ],
      );
      changed += 1;
    }
    return changed;
  });
}

/** A failed in-flight end operation withdraws only its provisional fact. */
export function clearPendingLoopThreadRoutesEnding(
  db: Database,
  botUid: string,
  groupId: string,
  threadChannelId: string,
): number {
  return run(db,
    `DELETE FROM loop_route_endings
     WHERE bot_uid = ? AND group_id = ? AND thread_channel_id = ? AND state = 'pending'`,
    [botUid, groupId, threadChannelId],
  ).changes;
}

/** An explicit binding or creation of a new route is the supported way back to child routing. */
export function clearLoopTaskRouteEnding(db: Database, key: LoopTaskKey): void {
  run(db,
    `DELETE FROM loop_route_endings
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?`,
    [key.botUid, key.groupId, key.workspaceId, key.taskId],
  );
}
