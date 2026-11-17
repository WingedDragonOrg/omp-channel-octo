/**
 * 只读发现查询：给定结构化线索，按 bot 返回本实例已记录的 Loop 路由、PR/MR 引用与 workspace
 * 归属。只做精确匹配，不按名称、URL 前缀或路径前缀推断关系；空线索直接返回空数组，绝不枚举
 * 全库记录。调用方负责在使用结果前核验目标聊天与 Loop workspace 的实时读权限。
 */

import type { Database } from "bun:sqlite";
import type { LoopTaskRouteEndingReason } from "../loop/types.ts";
import { canonicalReferenceUrl } from "../references/links.ts";
import type { ReferenceKind } from "../references/types.ts";
import type { ThreadWorkspaceProjectRecord, ThreadWorkspaceRecord } from "./store-records.ts";
import { asNumber, asString, query, type SqlRow } from "./store-schema.ts";
import { listThreadWorkspaces } from "./store-workspaces.ts";

/** Structured clues accepted by the discovery read surface; at least one must be non-empty. */
export interface DiscoveryQuery {
  workspaceId?: string;
  taskId?: string;
  referenceUrl?: string;
  repo?: string;
  path?: string;
}

/** A Loop task route, either the live route or a durable ending that preserves its own thread/version. */
export interface LoopRouteAssociation {
  kind: "loop_route";
  channelType: 5;
  channelId: string;
  groupId: string;
  workspaceId: string;
  taskId: string;
  routeVersion: number;
  state: "active" | "pending" | "ended";
  reason: LoopTaskRouteEndingReason | null;
  updatedAt: number | null;
}

/** A PR/MR URL the channel recorded from model-judged message evidence, not a human binding. */
export interface RecordedReferenceAssociation {
  kind: "recorded_reference";
  channelType: 5;
  channelId: string;
  referenceKind: ReferenceKind;
  url: string;
}

/** A persisted project/workspace record for the channel; ownership evidence, not business identity. */
export interface WorkspaceAssociation {
  kind: "workspace";
  channelType: 5;
  channelId: string;
  layout: ThreadWorkspaceRecord["layout"];
  status: ThreadWorkspaceRecord["status"];
  entryPath: string | null;
  workspaceRoot: string | null;
  projects: readonly ThreadWorkspaceProjectRecord[];
  error: string | null;
}

export type DiscoveryAssociation =
  | LoopRouteAssociation
  | RecordedReferenceAssociation
  | WorkspaceAssociation;

interface LoopRouteFact {
  groupId: string;
  workspaceId: string;
  taskId: string;
  threadChannelId: string;
  version: number;
  updatedAt: number;
}

interface LoopEndingFact {
  groupId: string;
  workspaceId: string;
  taskId: string;
  threadChannelId: string;
  routeVersion: number;
  state: "pending" | "ended";
  reason: LoopTaskRouteEndingReason;
  updatedAt: number;
}

function nonEmpty(value: string | undefined): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function loopTaskKey(groupId: string, workspaceId: string, taskId: string): string {
  return JSON.stringify([groupId, workspaceId, taskId]);
}

function routeFactFromRow(row: SqlRow): LoopRouteFact {
  return {
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    taskId: asString(row.task_id),
    threadChannelId: asString(row.thread_channel_id),
    version: asNumber(row.version),
    updatedAt: asNumber(row.updated_at),
  };
}

function endingFactFromRow(row: SqlRow): LoopEndingFact {
  return {
    groupId: asString(row.group_id),
    workspaceId: asString(row.workspace_id),
    taskId: asString(row.task_id),
    threadChannelId: asString(row.thread_channel_id),
    routeVersion: asNumber(row.route_version),
    state: asString(row.state) as LoopEndingFact["state"],
    reason: asString(row.reason) as LoopTaskRouteEndingReason,
    updatedAt: asNumber(row.updated_at),
  };
}

/**
 * Current routes and durable endings are joined on the exact task key. The ending merges into the
 * route only when it names the same thread and route version; otherwise both facts stay visible,
 * because an ending is history and the current route may already point at a different child.
 */
function findLoopRouteAssociations(
  db: Database,
  botUid: string,
  workspaceId: string,
  taskId: string | null,
): LoopRouteAssociation[] {
  const where = taskId === null
    ? "bot_uid = ? AND workspace_id = ?"
    : "bot_uid = ? AND workspace_id = ? AND task_id = ?";
  const values = taskId === null ? [botUid, workspaceId] : [botUid, workspaceId, taskId];

  const routes = query(
    db,
    `SELECT group_id, workspace_id, task_id, thread_channel_id, version, updated_at
     FROM loop_task_routes
     WHERE ${where}
     ORDER BY group_id ASC, task_id ASC, thread_channel_id ASC, version ASC`,
    values,
  ).map(routeFactFromRow);
  const endings = query(
    db,
    `SELECT group_id, workspace_id, task_id, thread_channel_id, route_version,
            state, reason, updated_at
     FROM loop_route_endings
     WHERE ${where}
     ORDER BY group_id ASC, task_id ASC, thread_channel_id ASC, route_version ASC`,
    values,
  ).map(endingFactFromRow);

  const endingsByTask = new Map<string, LoopEndingFact>();
  for (const ending of endings) {
    endingsByTask.set(loopTaskKey(ending.groupId, ending.workspaceId, ending.taskId), ending);
  }
  const mergedTasks = new Set<string>();
  const associations: LoopRouteAssociation[] = [];

  for (const route of routes) {
    const key = loopTaskKey(route.groupId, route.workspaceId, route.taskId);
    const ending = endingsByTask.get(key);
    if (
      ending !== undefined
      && ending.threadChannelId === route.threadChannelId
      && ending.routeVersion === route.version
    ) {
      mergedTasks.add(key);
      associations.push({
        kind: "loop_route",
        channelType: 5,
        channelId: route.threadChannelId,
        groupId: route.groupId,
        workspaceId: route.workspaceId,
        taskId: route.taskId,
        routeVersion: route.version,
        state: ending.state,
        reason: ending.reason,
        updatedAt: ending.updatedAt,
      });
      continue;
    }
    associations.push({
      kind: "loop_route",
      channelType: 5,
      channelId: route.threadChannelId,
      groupId: route.groupId,
      workspaceId: route.workspaceId,
      taskId: route.taskId,
      routeVersion: route.version,
      state: "active",
      reason: null,
      updatedAt: route.updatedAt,
    });
  }

  for (const ending of endings) {
    const key = loopTaskKey(ending.groupId, ending.workspaceId, ending.taskId);
    if (mergedTasks.has(key)) continue;
    associations.push({
      kind: "loop_route",
      channelType: 5,
      channelId: ending.threadChannelId,
      groupId: ending.groupId,
      workspaceId: ending.workspaceId,
      taskId: ending.taskId,
      routeVersion: ending.routeVersion,
      state: ending.state,
      reason: ending.reason,
      updatedAt: ending.updatedAt,
    });
  }

  return associations.sort((left, right) =>
    compareText(left.groupId, right.groupId)
    || compareText(left.workspaceId, right.workspaceId)
    || compareText(left.taskId, right.taskId)
    || compareText(left.channelId, right.channelId)
    || left.routeVersion - right.routeVersion);
}

/**
 * The stored `canonical_url` was written in canonical form, so the query URL is canonicalized with
 * the same rules first. Raw variants such as `/files`, redundant zeroes, or host casing therefore
 * identify the same recorded reference, while neighbouring repositories or PR numbers do not.
 */
function findRecordedReferences(
  db: Database,
  botUid: string,
  referenceUrl: string,
): RecordedReferenceAssociation[] {
  const canonical = canonicalReferenceUrl(referenceUrl);
  if (canonical === null) return [];
  return query(
    db,
    `SELECT channel_id, kind, canonical_url
     FROM thread_references
     WHERE bot_uid = ? AND canonical_url = ?
     ORDER BY channel_id ASC, kind ASC`,
    [botUid, canonical],
  ).map((row): RecordedReferenceAssociation => ({
    kind: "recorded_reference",
    channelType: 5,
    channelId: asString(row.channel_id),
    referenceKind: asString(row.kind) as ReferenceKind,
    url: asString(row.canonical_url),
  }));
}

/**
 * A workspace clue only looks at this bot's own persisted records. Repo and path are compared as
 * whole values against the fields the record actually owns; a shared prefix never matches. When
 * both clues resolve to one record the result contains that channel once.
 */
function findWorkspaceAssociations(
  db: Database,
  botUid: string,
  repo: string | null,
  path: string | null,
): WorkspaceAssociation[] {
  if (repo === null && path === null) return [];
  const associations: WorkspaceAssociation[] = [];
  for (const record of listThreadWorkspaces(db, botUid)) {
    const repoMatch = repo !== null
      && record.projects.some((project) => project.repo === repo);
    const pathMatch = path !== null
      && (record.workspaceRoot === path
        || record.entryPath === path
        || record.projects.some((project) =>
          project.path === path
          || project.accessPath === path
          || project.accessTarget === path));
    if (!repoMatch && !pathMatch) continue;
    associations.push({
      kind: "workspace",
      channelType: 5,
      channelId: record.channelId,
      layout: record.layout,
      status: record.status,
      entryPath: record.entryPath,
      workspaceRoot: record.workspaceRoot ?? null,
      projects: record.projects,
      error: record.error,
    });
  }
  return associations;
}

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function findDiscoveryAssociations(
  db: Database,
  botUid: string,
  input: DiscoveryQuery,
): DiscoveryAssociation[] {
  if (botUid.length === 0) return [];
  const workspaceId = nonEmpty(input.workspaceId);
  const taskId = nonEmpty(input.taskId);
  const referenceUrl = nonEmpty(input.referenceUrl);
  const repo = nonEmpty(input.repo);
  const path = nonEmpty(input.path);

  // A task without its workspace is ambiguous, and no clue is not a licence to enumerate.
  if (workspaceId === null && referenceUrl === null && repo === null && path === null) return [];

  const associations: DiscoveryAssociation[] = [];
  if (workspaceId !== null) {
    associations.push(...findLoopRouteAssociations(db, botUid, workspaceId, taskId));
  }
  if (referenceUrl !== null) {
    associations.push(...findRecordedReferences(db, botUid, referenceUrl));
  }
  associations.push(...findWorkspaceAssociations(db, botUid, repo, path));
  return associations;
}
