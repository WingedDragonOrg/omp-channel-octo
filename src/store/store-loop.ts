import { randomUUID } from "node:crypto";
import type { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { LoopBinding, LoopInput, LoopTask, LoopTaskKey, LoopTaskRoute, LoopThreadCreation, LoopWorkspace } from "../loop/types.ts";
import { getOwnerId } from "./store-chats.ts";
import { asNullableString, asNumber, asString, query, run, transaction, type SqlRow, type SqlValue } from "./store-schema.ts";

const BASELINE_BATCH_SIZE = 20;

type StoredLoopInput = {
  botUid: string;
  groupId: string;
  workspaceId: string;
  bindingVersion: number;
  kind: LoopInput["kind"];
  taskId: string | null;
  tasks: readonly LoopTask[];
  previous: LoopTask | null;
};

function loopBindingFromRow(row: SqlRow): LoopBinding {
  return {
    botUid: asString(row.bot_uid),
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    name: asString(row.workspace_name),
    slug: asString(row.workspace_slug),
    version: asNumber(row.version),
  };
}

function loopInputFromRow(row: SqlRow): LoopInput {
  const targetChannelType = row.target_channel_type === null
    ? null
    : asNumber(row.target_channel_type) as ChatTarget["channelType"];
  const targetChannelId = asNullableString(row.target_channel_id);
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    bindingVersion: asNumber(row.binding_version),
    kind: asString(row.kind) as LoopInput["kind"],
    taskId: asNullableString(row.task_id),
    tasks: JSON.parse(asString(row.tasks_json)) as LoopTask[],
    previous: row.previous_json === null ? null : JSON.parse(asString(row.previous_json)) as LoopTask,
    state: asString(row.state) as LoopInput["state"],
    target: targetChannelType === null || targetChannelId === null
      ? null
      : { channelType: targetChannelType, channelId: targetChannelId },
    faultNotified: asNumber(row.fault_notified) === 1,
  };
}

function loopTaskRouteFromRow(row: SqlRow): LoopTaskRoute {
  return {
    botUid: asString(row.bot_uid),
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    taskId: asString(row.task_id),
    threadChannelId: asString(row.thread_channel_id),
    version: asNumber(row.version),
  };
}

function loopThreadCreationFromRow(row: SqlRow): LoopThreadCreation {
  return {
    botUid: asString(row.bot_uid),
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    taskId: asString(row.task_id),
    operationId: asString(row.operation_id),
    bindingVersion: asNumber(row.binding_version),
    routeVersion: asNumber(row.route_version),
    state: asString(row.state) as LoopThreadCreation["state"],
    threadChannelId: asNullableString(row.thread_channel_id),
    error: asNullableString(row.error),
  };
}

function loopBindingRow(db: Database, botUid: string, groupId: string): SqlRow | undefined {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, workspace_name, workspace_slug, version
     FROM loop_bindings WHERE bot_uid = ? AND group_id = ?`,
    [botUid, groupId],
  )[0];
}

function loopTaskSnapshotRow(db: Database, key: LoopTaskKey): SqlRow | undefined {
  return query(db,
    `SELECT task_json, business_json FROM loop_task_snapshots
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?`,
    loopTaskKeyValues(key),
  )[0];
}

function loopTaskKeyValues(key: LoopTaskKey): SqlValue[] {
  return [key.botUid, key.groupId, key.workspaceId, key.taskId];
}

function loopTaskRouteRow(db: Database, key: LoopTaskKey): SqlRow | undefined {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, thread_channel_id, version
     FROM loop_task_routes
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?`,
    loopTaskKeyValues(key),
  )[0];
}

function loopThreadCreationRow(db: Database, key: LoopTaskKey): SqlRow | undefined {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, operation_id, binding_version, route_version,
            state, thread_channel_id, error
     FROM loop_thread_creations
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?`,
    loopTaskKeyValues(key),
  )[0];
}

function assertLoopTaskKey(key: LoopTaskKey): void {
  if (!key.botUid || !key.groupId || !key.workspaceId || !key.taskId) {
    throw new Error("Loop task identity is invalid");
  }
}

function deduplicateTasks(tasks: readonly LoopTask[]): LoopTask[] {
  const unique = new Map<string, LoopTask>();
  for (const task of tasks) {
    if (!task.taskId) throw new Error("Loop task ID is required");
    unique.set(task.taskId, task);
  }
  return [...unique.values()];
}

function isLoopTaskAssignedToOwnerOrBot(task: LoopTask, botUid: string, ownerId: string | null): boolean {
  const assigneeOctoUid = task.assigneeOctoUid ?? null;
  return task.assigneeType === "member"
    && assigneeOctoUid !== null
    && (assigneeOctoUid === ownerId || assigneeOctoUid === botUid);
}

function taskBusinessJson(task: LoopTask): string {
  return JSON.stringify({
    taskId: task.taskId,
    title: task.title,
    description: task.description,
    status: task.status,
    priority: task.priority,
    assigneeType: task.assigneeType,
    assigneeId: task.assigneeId,
    assigneeOctoUid: task.assigneeOctoUid ?? null,
    projectId: task.projectId,
    parentTaskId: task.parentTaskId,
    startDate: task.startDate,
    dueDate: task.dueDate,
  });
}

function taskBusinessChanged(previousJson: string, task: LoopTask): boolean {
  const current = taskBusinessJson(task);
  return previousJson !== current
    && taskBusinessJson(JSON.parse(previousJson) as LoopTask) !== current;
}

function saveLoopTaskSnapshot(db: Database, key: LoopTaskKey, task: LoopTask, now: number): void {
  run(db,
    `INSERT INTO loop_task_snapshots(
       bot_uid, group_id, workspace_id, task_id, task_json, business_json, updated_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(bot_uid, group_id, workspace_id, task_id) DO UPDATE SET
       task_json = excluded.task_json,
       business_json = excluded.business_json,
       updated_at = excluded.updated_at`,
    [key.botUid, key.groupId, key.workspaceId, task.taskId, JSON.stringify(task), taskBusinessJson(task), now],
  );
}

function insertLoopInput(db: Database, input: StoredLoopInput, now: number): string {
  const id = randomUUID();
  run(db,
    `INSERT INTO loop_inputs(
       id, bot_uid, group_id, workspace_id, binding_version, kind, task_id, tasks_json,
       previous_json, state, target_channel_type, target_channel_id, fault_notified, created_at
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, 0, ?)`,
    [
      id,
      input.botUid,
      input.groupId,
      input.workspaceId,
      input.bindingVersion,
      input.kind,
      input.taskId,
      JSON.stringify(input.tasks),
      input.previous === null ? null : JSON.stringify(input.previous),
      now,
    ],
  );
  return id;
}

function currentBindingMatches(db: Database, binding: LoopBinding): boolean {
  const row = loopBindingRow(db, binding.botUid, binding.groupId);
  return row !== undefined
    && asString(row.workspace_id) === binding.workspaceId
    && asNumber(row.version) === binding.version;
}

export function getLoopBinding(db: Database, botUid: string, groupId: string): LoopBinding | null {
  const row = loopBindingRow(db, botUid, groupId);
  return row ? loopBindingFromRow(row) : null;
}

export function listLoopBindings(db: Database, botUid: string): LoopBinding[] {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, workspace_name, workspace_slug, version
     FROM loop_bindings WHERE bot_uid = ? ORDER BY group_id ASC`,
    [botUid],
  ).map((row) => loopBindingFromRow(row));
}

export function bindLoopWorkspace(
  db: Database,
  botUid: string,
  groupId: string,
  workspace: LoopWorkspace,
  tasks: readonly LoopTask[],
): { binding: LoopBinding; changed: boolean } {
  if (!botUid || !groupId || !workspace.workspaceId) throw new Error("Loop workspace binding identity is invalid");
  const uniqueTasks = deduplicateTasks(tasks);
  return transaction(db, () => {
    const existingRow = loopBindingRow(db, botUid, groupId);
    const existing = existingRow ? loopBindingFromRow(existingRow) : null;
    if (existing?.workspaceId === workspace.workspaceId) {
      run(db,
        `UPDATE loop_bindings SET workspace_name = ?, workspace_slug = ?, updated_at = ?
         WHERE bot_uid = ? AND group_id = ? AND workspace_id = ?`,
        [workspace.name, workspace.slug, Date.now(), botUid, groupId, workspace.workspaceId],
      );
      return {
        binding: { ...existing, name: workspace.name, slug: workspace.slug },
        changed: false,
      };
    }

    const version = existing === null ? 1 : existing.version + 1;
    const now = Date.now();
    run(db,
      `INSERT INTO loop_bindings(bot_uid, group_id, workspace_id, workspace_name, workspace_slug, version, updated_at)
       VALUES(?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(bot_uid, group_id) DO UPDATE SET
         workspace_id = excluded.workspace_id,
         workspace_name = excluded.workspace_name,
         workspace_slug = excluded.workspace_slug,
         version = excluded.version,
         updated_at = excluded.updated_at`,
      [botUid, groupId, workspace.workspaceId, workspace.name, workspace.slug, version, now],
    );
    if (existing !== null) {
      run(db,
        `UPDATE loop_inputs SET state = 'superseded'
         WHERE bot_uid = ? AND group_id = ? AND state IN ('pending', 'intent')`,
        [botUid, groupId],
      );
    }

    const keyBase = { botUid, groupId, workspaceId: workspace.workspaceId, taskId: "" };
    const ownerId = getOwnerId(db, botUid);
    const routedTasks: LoopTask[] = [];
    const unthreadedObservedTasks: LoopTask[] = [];
    for (const task of uniqueTasks) {
      const key = { ...keyBase, taskId: task.taskId };
      saveLoopTaskSnapshot(db, key, task, now);
      if (loopTaskRouteRow(db, key)) {
        routedTasks.push(task);
      } else if (isLoopTaskAssignedToOwnerOrBot(task, botUid, ownerId)) {
        unthreadedObservedTasks.push(task);
      }
    }
    for (const task of routedTasks) {
      insertLoopInput(db, {
        botUid,
        groupId,
        workspaceId: workspace.workspaceId,
        bindingVersion: version,
        kind: "baseline",
        taskId: task.taskId,
        tasks: [task],
        previous: null,
      }, now);
    }
    const batches = Array.from(
      { length: Math.ceil(unthreadedObservedTasks.length / BASELINE_BATCH_SIZE) },
      (_, index) => unthreadedObservedTasks.slice(
        index * BASELINE_BATCH_SIZE,
        (index + 1) * BASELINE_BATCH_SIZE,
      ),
    );
    for (const batch of batches) {
      insertLoopInput(db, {
        botUid,
        groupId,
        workspaceId: workspace.workspaceId,
        bindingVersion: version,
        kind: "baseline",
        taskId: null,
        tasks: batch,
        previous: null,
      }, now);
    }

    const bindingRow = loopBindingRow(db, botUid, groupId);
    if (!bindingRow) throw new Error("Loop workspace binding disappeared after save");
    return { binding: loopBindingFromRow(bindingRow), changed: true };
  });
}

export function commitLoopScan(db: Database, binding: LoopBinding, tasks: readonly LoopTask[]): boolean {
  const uniqueTasks = deduplicateTasks(tasks);
  return transaction(db, () => {
    if (!currentBindingMatches(db, binding)) return false;
    const now = Date.now();
    const ownerId = getOwnerId(db, binding.botUid);
    const keyBase = {
      botUid: binding.botUid,
      groupId: binding.groupId,
      workspaceId: binding.workspaceId,
      taskId: "",
    };
    for (const task of uniqueTasks) {
      const key = { ...keyBase, taskId: task.taskId };
      const previousRow = loopTaskSnapshotRow(db, key);
      const observed = Boolean(loopTaskRouteRow(db, key))
        || isLoopTaskAssignedToOwnerOrBot(task, binding.botUid, ownerId);
      if (!previousRow) {
        if (observed) {
          insertLoopInput(db, {
            botUid: binding.botUid,
            groupId: binding.groupId,
            workspaceId: binding.workspaceId,
            bindingVersion: binding.version,
            kind: "created",
            taskId: task.taskId,
            tasks: [task],
            previous: null,
          }, now);
        }
      } else {
        const previous = JSON.parse(asString(previousRow.task_json)) as LoopTask;
        if (observed && taskBusinessChanged(asString(previousRow.business_json), task)) {
          insertLoopInput(db, {
            botUid: binding.botUid,
            groupId: binding.groupId,
            workspaceId: binding.workspaceId,
            bindingVersion: binding.version,
            kind: "updated",
            taskId: task.taskId,
            tasks: [task],
            previous,
          }, now);
        }
      }
      saveLoopTaskSnapshot(db, key, task, now);
    }
    return true;
  });
}

export function listLoopInputs(db: Database, botUid: string): LoopInput[] {
  return query(db,
    `SELECT id, bot_uid, group_id, workspace_id, binding_version, kind, task_id, tasks_json,
            previous_json, state, target_channel_type, target_channel_id, fault_notified
     FROM loop_inputs WHERE bot_uid = ? AND state IN ('pending', 'intent') ORDER BY sequence ASC`,
    [botUid],
  ).map((row) => loopInputFromRow(row));
}

export function getLoopInput(db: Database, id: string): LoopInput | null {
  const row = query(db,
    `SELECT id, bot_uid, group_id, workspace_id, binding_version, kind, task_id, tasks_json,
            previous_json, state, target_channel_type, target_channel_id, fault_notified
     FROM loop_inputs WHERE id = ?`,
    [id],
  )[0];
  return row ? loopInputFromRow(row) : null;
}

function latestLoopTaskSnapshot(db: Database, key: LoopTaskKey): LoopTask | null {
  const row = loopTaskSnapshotRow(db, key);
  return row ? JSON.parse(asString(row.task_json)) as LoopTask : null;
}

export function isLoopInputObserved(db: Database, input: LoopInput): boolean {
  if (input.tasks.length === 0) return false;
  const ownerId = getOwnerId(db, input.botUid);
  for (const task of input.tasks) {
    const key = {
      botUid: input.botUid,
      groupId: input.groupId,
      workspaceId: input.workspaceId,
      taskId: task.taskId,
    };
    const routed = loopTaskRouteRow(db, key) !== undefined;
    if (input.taskId === null) {
      if (routed) return false;
    } else if (routed) {
      continue;
    }
    const latest = latestLoopTaskSnapshot(db, key);
    if (!latest || !isLoopTaskAssignedToOwnerOrBot(latest, input.botUid, ownerId)) return false;
  }
  return true;
}

export function prepareLoopInput(db: Database, id: string): LoopInput | null {
  return transaction(db, () => {
    const input = getLoopInput(db, id);
    if (!input || input.state !== "pending") return null;
    const binding = loopBindingRow(db, input.botUid, input.groupId);
    if (!binding
      || asString(binding.workspace_id) !== input.workspaceId
      || asNumber(binding.version) !== input.bindingVersion) return null;

    if (input.kind === "baseline" && input.taskId === null) {
      const ownerId = getOwnerId(db, input.botUid);
      const mainTasks: LoopTask[] = [];
      const routedGroups: LoopTask[][] = [];
      for (const task of deduplicateTasks(input.tasks)) {
        const key = {
          botUid: input.botUid,
          groupId: input.groupId,
          workspaceId: input.workspaceId,
          taskId: task.taskId,
        };
        if (loopTaskRouteRow(db, key)) {
          routedGroups.push([task]);
          continue;
        }
        const latest = latestLoopTaskSnapshot(db, key);
        if (latest && isLoopTaskAssignedToOwnerOrBot(latest, input.botUid, ownerId)) {
          mainTasks.push(task);
        }
      }
      const mainGroups: LoopTask[][] = Array.from(
        { length: Math.ceil(mainTasks.length / BASELINE_BATCH_SIZE) },
        (_, index) => mainTasks.slice(
          index * BASELINE_BATCH_SIZE,
          (index + 1) * BASELINE_BATCH_SIZE,
        ),
      );
      const groups = [...mainGroups.map((tasks) => ({ taskId: null, tasks })), ...routedGroups.map((tasks) => ({
        taskId: tasks[0]!.taskId,
        tasks,
      }))];
      if (groups.length === 0) {
        run(db, "UPDATE loop_inputs SET state = 'superseded' WHERE id = ? AND state = 'pending'", [input.id]);
        return null;
      }
      const first = groups[0]!;
      const remaining = groups.slice(1);
      run(db,
        `UPDATE loop_inputs SET task_id = ?, tasks_json = ?, previous_json = NULL
         WHERE id = ? AND state = 'pending'`,
        [first.taskId, JSON.stringify(first.tasks), input.id],
      );
      const now = Date.now();
      for (const group of remaining) {
        insertLoopInput(db, {
          botUid: input.botUid,
          groupId: input.groupId,
          workspaceId: input.workspaceId,
          bindingVersion: input.bindingVersion,
          kind: "baseline",
          taskId: group.taskId,
          tasks: group.tasks,
          previous: null,
        }, now);
      }
      return getLoopInput(db, input.id);
    }

    if (!isLoopInputObserved(db, input)) {
      run(db, "UPDATE loop_inputs SET state = 'superseded' WHERE id = ? AND state = 'pending'", [input.id]);
      return null;
    }
    return input;
  });
}


export function claimLoopInputIntent(db: Database, id: string, target: ChatTarget): boolean {
  return run(db,
    `UPDATE loop_inputs SET state = 'intent', target_channel_type = ?, target_channel_id = ?
     WHERE id = ? AND state = 'pending'`,
    [target.channelType, target.channelId, id],
  ).changes === 1;
}

export function markLoopInputDelivered(db: Database, id: string): void {
  run(db, "UPDATE loop_inputs SET state = 'delivered' WHERE id = ? AND state = 'intent'", [id]);
}

export function markLoopInputPending(db: Database, id: string): void {
  run(db, "UPDATE loop_inputs SET state = 'pending' WHERE id = ? AND state = 'intent'", [id]);
}

export function markLoopInputUnknown(db: Database, id: string): void {
  run(db, "UPDATE loop_inputs SET state = 'unknown' WHERE id = ? AND state = 'intent'", [id]);
}

export function markLoopInputFault(db: Database, id: string, notified: boolean): boolean {
  if (!notified) return false;
  return run(db,
    "UPDATE loop_inputs SET fault_notified = 1 WHERE id = ? AND fault_notified = 0",
    [id],
  ).changes === 1;
}

export function getLoopTaskRoute(db: Database, key: LoopTaskKey): LoopTaskRoute | null {
  assertLoopTaskKey(key);
  const row = loopTaskRouteRow(db, key);
  return row ? loopTaskRouteFromRow(row) : null;
}

export function bindLoopTask(db: Database, key: LoopTaskKey, threadChannelId: string): LoopTaskRoute {
  assertLoopTaskKey(key);
  if (!threadChannelId) throw new Error("Loop thread target is required");
  return transaction(db, () => {
    const binding = loopBindingRow(db, key.botUid, key.groupId);
    if (!binding || asString(binding.workspace_id) !== key.workspaceId) {
      throw new Error("Loop task workspace is not the current group binding");
    }
    const existingRow = loopTaskRouteRow(db, key);
    const existing = existingRow ? loopTaskRouteFromRow(existingRow) : null;
    if (existing?.threadChannelId === threadChannelId) return existing;
    const version = existing === null ? 1 : existing.version + 1;
    const now = Date.now();
    run(db,
      `INSERT INTO loop_task_routes(
         bot_uid, group_id, workspace_id, task_id, thread_channel_id, version, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(bot_uid, group_id, workspace_id, task_id) DO UPDATE SET
         thread_channel_id = excluded.thread_channel_id,
         version = excluded.version,
         updated_at = excluded.updated_at`,
      [...loopTaskKeyValues(key), threadChannelId, version, now],
    );
    const routeRow = loopTaskRouteRow(db, key);
    if (!routeRow) throw new Error("Loop task route disappeared after save");
    return loopTaskRouteFromRow(routeRow);
  });
}

export function listLoopThreadTasks(db: Database, botUid: string, groupId: string, threadChannelId: string): LoopTaskRoute[] {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, thread_channel_id, version
     FROM loop_task_routes
     WHERE bot_uid = ? AND group_id = ? AND thread_channel_id = ?
     ORDER BY task_id ASC, workspace_id ASC`,
    [botUid, groupId, threadChannelId],
  ).map((row) => loopTaskRouteFromRow(row));
}

export function getLoopThreadCreation(db: Database, key: LoopTaskKey): LoopThreadCreation | null {
  assertLoopTaskKey(key);
  const row = loopThreadCreationRow(db, key);
  return row ? loopThreadCreationFromRow(row) : null;
}

export function reserveLoopThreadCreation(db: Database, key: LoopTaskKey, operationId: string): LoopThreadCreation {
  assertLoopTaskKey(key);
  if (!operationId) throw new Error("Loop thread creation operation ID is required");
  return transaction(db, () => {
    const existingRow = loopThreadCreationRow(db, key);
    if (existingRow) return loopThreadCreationFromRow(existingRow);

    const binding = loopBindingRow(db, key.botUid, key.groupId);
    if (!binding || asString(binding.workspace_id) !== key.workspaceId) {
      throw new Error("Loop task workspace is not the current group binding");
    }
    const routeRow = loopTaskRouteRow(db, key);
    const route = routeRow ? loopTaskRouteFromRow(routeRow) : null;
    const state: LoopThreadCreation["state"] = route ? "ready" : "reserved";
    run(db,
      `INSERT INTO loop_thread_creations(
         bot_uid, group_id, workspace_id, task_id, operation_id, binding_version, route_version,
         state, thread_channel_id, error, updated_at
       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
      [
        ...loopTaskKeyValues(key),
        operationId,
        asNumber(binding.version),
        route?.version ?? 0,
        state,
        route?.threadChannelId ?? null,
        Date.now(),
      ],
    );
    const creationRow = loopThreadCreationRow(db, key);
    if (!creationRow) throw new Error("Loop thread creation disappeared after reservation");
    return loopThreadCreationFromRow(creationRow);
  });
}

export function recordLoopThreadAllocated(db: Database, key: LoopTaskKey, operationId: string, threadChannelId: string): void {
  assertLoopTaskKey(key);
  if (!operationId || !threadChannelId) throw new Error("Loop thread allocation identity is invalid");
  transaction(db, () => {
    const row = loopThreadCreationRow(db, key);
    if (!row || asString(row.operation_id) !== operationId) {
      throw new Error("Loop thread creation reservation does not match allocation");
    }
    const creation = loopThreadCreationFromRow(row);
    if (creation.threadChannelId !== null && creation.threadChannelId !== threadChannelId) {
      throw new Error("Loop thread creation already records a different remote resource");
    }
    const state = creation.state === "ready" || creation.state === "superseded" ? creation.state : "allocated";
    run(db,
      `UPDATE loop_thread_creations SET thread_channel_id = ?, state = ?, error = NULL, updated_at = ?
       WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ? AND operation_id = ?`,
      [threadChannelId, state, Date.now(), ...loopTaskKeyValues(key), operationId],
    );
  });
}

export function completeLoopThreadCreation(db: Database, key: LoopTaskKey, operationId: string): boolean {
  assertLoopTaskKey(key);
  if (!operationId) return false;
  return transaction(db, () => {
    const row = loopThreadCreationRow(db, key);
    if (!row || asString(row.operation_id) !== operationId) return false;
    const creation = loopThreadCreationFromRow(row);
    const bindingRow = loopBindingRow(db, key.botUid, key.groupId);
    const bindingIsCurrent = bindingRow !== undefined
      && asString(bindingRow.workspace_id) === creation.workspaceId
      && asNumber(bindingRow.version) === creation.bindingVersion;
    const routeRow = loopTaskRouteRow(db, key);
    const route = routeRow ? loopTaskRouteFromRow(routeRow) : null;

    if (creation.state === "ready") {
      const publishedOrReused = route?.version === creation.routeVersion
        || route?.version === creation.routeVersion + 1;
      return bindingIsCurrent
        && route !== null
        && publishedOrReused
        && route.threadChannelId === creation.threadChannelId;
    }
    if (creation.state === "superseded") return false;
    if (!bindingIsCurrent || (route?.version ?? 0) !== creation.routeVersion) {
      run(db,
        `UPDATE loop_thread_creations SET state = 'superseded', updated_at = ?
         WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ? AND operation_id = ?
           AND state IN ('reserved', 'allocated', 'unknown')`,
        [Date.now(), ...loopTaskKeyValues(key), operationId],
      );
      return false;
    }
    if (creation.state !== "allocated" || creation.threadChannelId === null) return false;

    const nextRouteVersion = creation.routeVersion + 1;
    const now = Date.now();
    if (route === null) {
      run(db,
        `INSERT INTO loop_task_routes(
           bot_uid, group_id, workspace_id, task_id, thread_channel_id, version, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?)`,
        [...loopTaskKeyValues(key), creation.threadChannelId, nextRouteVersion, now],
      );
    } else {
      const updated = run(db,
        `UPDATE loop_task_routes SET thread_channel_id = ?, version = ?, updated_at = ?
         WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ? AND version = ?`,
        [creation.threadChannelId, nextRouteVersion, now, ...loopTaskKeyValues(key), creation.routeVersion],
      );
      if (updated.changes !== 1) throw new Error("Loop task route changed during creation completion");
    }
    const completed = run(db,
      `UPDATE loop_thread_creations SET state = 'ready', updated_at = ?
       WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ? AND operation_id = ?
         AND binding_version = ? AND route_version = ? AND state = 'allocated'`,
      [now, ...loopTaskKeyValues(key), operationId, creation.bindingVersion, creation.routeVersion],
    );
    if (completed.changes !== 1) throw new Error("Loop thread creation reservation changed during completion");
    return true;
  });
}

export function markLoopThreadCreationUnknown(db: Database, key: LoopTaskKey, operationId: string, error: string): void {
  assertLoopTaskKey(key);
  if (!operationId) throw new Error("Loop thread creation operation ID is required");
  transaction(db, () => {
    const row = loopThreadCreationRow(db, key);
    if (!row || asString(row.operation_id) !== operationId) {
      throw new Error("Loop thread creation reservation does not match failure");
    }
    run(db,
      `UPDATE loop_thread_creations SET state = 'unknown', error = ?, updated_at = ?
       WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ? AND operation_id = ?
         AND state NOT IN ('ready', 'superseded')`,
      [error, Date.now(), ...loopTaskKeyValues(key), operationId],
    );
  });
}
export function releaseLoopThreadCreation(
  db: Database,
  key: LoopTaskKey,
  operationId: string,
): boolean {
  assertLoopTaskKey(key);
  if (!operationId) return false;
  return transaction(db, () => run(
    db,
    `DELETE FROM loop_thread_creations
     WHERE bot_uid = ? AND group_id = ? AND workspace_id = ? AND task_id = ?
       AND operation_id = ? AND state = 'reserved' AND thread_channel_id IS NULL`,
    [...loopTaskKeyValues(key), operationId],
  ).changes === 1);
}

export function listLoopThreadCreations(db: Database, botUid: string): LoopThreadCreation[] {
  return query(db,
    `SELECT bot_uid, group_id, workspace_id, task_id, operation_id, binding_version, route_version,
            state, thread_channel_id, error
     FROM loop_thread_creations WHERE bot_uid = ?
     ORDER BY group_id ASC, workspace_id ASC, task_id ASC`,
    [botUid],
  ).map((row) => loopThreadCreationFromRow(row));
}
