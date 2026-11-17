import { randomUUID } from "node:crypto";
import { throwIfAborted } from "../abort.ts";
import type {
  DiscoveryAssociation,
  DiscoveryQuery,
  LoopRouteAssociation,
  RecordedReferenceAssociation,
  WorkspaceAssociation,
} from "../store/store-discovery.ts";
import type {
  DiscoveryGroup as DiscoveryRemoteGroup,
  DiscoveryGroupBucket as DiscoveryRemoteGroupBucket,
  DiscoveryGroupSearchPage as DiscoveryRemoteGroupSearchPage,
  DiscoveryMessageHit as DiscoveryRemoteMessageHit,
  DiscoveryMessagePage as DiscoveryRemoteMessagePage,
  DiscoveryRemote as DiscoveryRemotePort,
  DiscoveryTask as DiscoveryRemoteTask,
  DiscoveryTaskPage as DiscoveryRemoteTaskPage,
  DiscoveryThread as DiscoveryRemoteThread,
  DiscoveryThreadPage as DiscoveryRemoteThreadPage,
  DiscoveryTargetVerification as DiscoveryRemoteTargetVerification,
  DiscoveryWorkspaceDetail as DiscoveryRemoteWorkspaceDetail,
  DiscoveryWorkspacePage as DiscoveryRemoteWorkspacePage,
  DiscoveryWorkspaceRow as DiscoveryRemoteWorkspaceRow,
} from "./remote.ts";
export type {
  DiscoveryRemoteGroup,
  DiscoveryRemoteGroupBucket,
  DiscoveryRemoteGroupSearchPage,
  DiscoveryRemoteMessageHit,
  DiscoveryRemoteMessagePage,
  DiscoveryRemotePort,
  DiscoveryRemoteTask,
  DiscoveryRemoteTaskPage,
  DiscoveryRemoteThread,
  DiscoveryRemoteThreadPage,
  DiscoveryRemoteTargetVerification,
  DiscoveryRemoteWorkspaceDetail,
  DiscoveryRemoteWorkspacePage,
  DiscoveryRemoteWorkspaceRow,
};
import { threadParentGroupId, threadShortId } from "../threads/thread-target.ts";

/** Fixed, published query budgets; none of these are operator-configurable. */
export const DISCOVERY_LIMITS = {
  pageSize: 20,
  remoteRequestsPerSource: 10,
  collectedCandidates: 200,
  modelCandidates: 100,
  modelCharsPerCandidate: 1_000,
  modelDeadlineMs: 30_000,
  snapshotMaxBytes: 2 * 1024 * 1024,
  snapshotTtlMs: 5 * 60_000,
  queriesPerTask: 4,
  queriesPerBot: 32,
  remoteKeywordChars: 64,
  queryChars: 1_000,
} as const;


/** Only the bot-scoped, read-only local association query participates in discovery. */
export interface DiscoveryStorePort {
  findDiscoveryAssociations(botUid: string, query: DiscoveryQuery): readonly DiscoveryAssociation[];
}

export type DiscoveryJudge = (
  query: string,
  candidates: readonly { id: string; text: string }[],
  signal: AbortSignal,
) => Promise<readonly { id: string; relevant: boolean; score: number }[]>;

export interface DiscoverySearchInput {
  botUid: string;
  /** Durable task that owns this query; the cache rejects any other task. */
  taskId: string;
  query: string;
  workspaceId?: string;
  loopTaskId?: string;
  referenceUrl?: string;
  repo?: string;
  path?: string;
}

export interface DiscoveryPageInput {
  botUid: string;
  taskId: string;
  queryId: string;
  collection?: "candidates" | "filtered";
  position?: number;
}

export interface DiscoveryCoordinator {
  search(input: DiscoverySearchInput, signal: AbortSignal): Promise<string>;
  page(input: DiscoveryPageInput, signal: AbortSignal): Promise<string>;
  releaseTask(taskId: string): void;
  stop(): void;
}

export interface DiscoveryLimits {
  pageSize: number;
  remoteRequestsPerSource: number;
  collectedCandidates: number;
  modelCandidates: number;
  modelCharsPerCandidate: number;
  modelDeadlineMs: number;
  snapshotMaxBytes: number;
  snapshotTtlMs: number;
  queriesPerTask: number;
  queriesPerBot: number;
  remoteKeywordChars: number;
  queryChars: number;
}

type SourceState =
  | "complete"
  | "partial"
  | "truncated"
  | "restricted"
  | "unknown"
  | "failed"
  | "unavailable"
  | "skipped"
  | "budget_exhausted";

interface SourceStatus {
  state: SourceState;
  requests: number;
  candidates: number;
  complete: boolean;
  sampledAt: number | null;
  keywordTruncated?: boolean;
  includesArchived?: boolean;
  detail?: string;
}

interface DiscoveryEvidence {
  kind: "loop_route" | "recorded_reference" | "workspace" | "message_match" | "name_match";
  [key: string]: unknown;
}

interface CandidateSnapshot {
  channelType: 2 | 5;
  channelId: string;
  groupId: string | null;
  shortId: string | null;
  name: string | null;
  groupName: string | null;
  evidence: DiscoveryEvidence[];
  explicit: boolean;
  filtered: boolean;
  score: number | null;
  sampledAt: number;
}

interface Snapshot {
  queryId: string;
  botUid: string;
  taskId: string;
  query: string;
  createdAt: number;
  expiresAt: number;
  candidates: CandidateSnapshot[];
  sources: Record<string, SourceStatus>;
  model: {
    status: "complete" | "partial" | "failed" | "skipped";
    evaluated: number;
    unprocessed: number;
    detail?: string;
  };
}

interface MessageQueueEntry {
  channelType: 2 | 5;
  channelId: string;
  cursor: string | undefined;
}

function bytesOf(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function iso(epochMs: number): string {
  return new Date(epochMs).toISOString();
}

/** Unicode code-point truncation; never splits surrogate pairs. */
function truncateCodePoints(value: string, max: number): { text: string; truncated: boolean } {
  const points = Array.from(value);
  if (points.length <= max) return { text: value, truncated: false };
  return { text: points.slice(0, max).join(""), truncated: true };
}

function candidateKey(channelType: 2 | 5, channelId: string): string {
  return `${channelType}:${channelId}`;
}

function evidenceKey(evidence: DiscoveryEvidence): string {
  switch (evidence.kind) {
    case "loop_route":
      return ["loop", evidence.workspace_id, evidence.task_id, evidence.route_version, evidence.channel_id, evidence.state].join(":");
    case "recorded_reference":
      return ["reference", evidence.reference_kind, evidence.url].join(":");
    case "workspace":
      return ["workspace", evidence.channel_id, evidence.layout, evidence.entry_path, evidence.workspace_root].join(":");
    case "message_match":
      return ["message", evidence.channel_id, evidence.message_id].join(":");
    case "name_match":
      return ["name", evidence.channel_id, evidence.matched].join(":");
  }
}

function evidenceRank(evidence: DiscoveryEvidence): number {
  switch (evidence.kind) {
    case "loop_route":
      return 0;
    case "recorded_reference":
      return 1;
    case "workspace":
      return 2;
    case "message_match":
      return 3;
    case "name_match":
      return 4;
  }
}

function candidateRank(candidate: CandidateSnapshot): number {
  return Math.min(...candidate.evidence.map(evidenceRank));
}

function isExplicit(evidence: readonly DiscoveryEvidence[]): boolean {
  return evidence.some((entry) => entry.kind === "loop_route" || entry.kind === "recorded_reference");
}

function loopRouteEvidence(
  association: LoopRouteAssociation,
  task?: DiscoveryRemoteTask,
): DiscoveryEvidence {
  return {
    kind: "loop_route",
    source: "local_records",
    channel_type: 5,
    channel_id: association.channelId,
    group_id: association.groupId,
    workspace_id: association.workspaceId,
    task_id: association.taskId,
    route_version: association.routeVersion,
    state: association.state,
    reason: association.reason,
    updated_at: association.updatedAt,
    ...(task === undefined ? {} : { task_source: "loop_tasks" }),
    task: task === undefined ? null : {
      task_id: task.task_id,
      title: task.title,
      status: task.status,
      ...(task.identifier === undefined ? {} : { identifier: task.identifier }),
    },
  };
}

function mergeEvidence(candidate: CandidateSnapshot, evidence: readonly DiscoveryEvidence[]): void {
  for (const entry of evidence) {
    if (candidate.evidence.some((existing) => evidenceKey(existing) === evidenceKey(entry))) continue;
    candidate.evidence.push(entry);
  }
}

function addCandidate(
  candidates: Map<string, CandidateSnapshot>,
  target: { channelType: 2 | 5; channelId: string },
  evidence: DiscoveryEvidence,
  explicit: boolean,
  limit: number,
  sampledAt: number,
): boolean {
  const key = candidateKey(target.channelType, target.channelId);
  const existing = candidates.get(key);
  if (existing !== undefined) {
    mergeEvidence(existing, [evidence]);
    existing.explicit ||= explicit;
    return true;
  }
  if (candidates.size >= limit) return false;
  candidates.set(key, {
    channelType: target.channelType,
    channelId: target.channelId,
    groupId: target.channelType === 5 ? threadParentGroupId(target.channelId) : target.channelId,
    shortId: target.channelType === 5 ? threadShortId(target.channelId) : null,
    name: null,
    groupName: null,
    evidence: [evidence],
    explicit,
    filtered: false,
    score: null,
    sampledAt,
  });
  return true;
}

function sourceStatus(): SourceStatus {
  return { state: "unavailable", requests: 0, candidates: 0, complete: false, sampledAt: null };
}

function safeRemoteError(error: unknown): string {
  if (error !== null && typeof error === "object") {
    const rawCode = "code" in error ? error.code : undefined;
    const code = (typeof rawCode === "string" && /^[A-Za-z0-9_.-]{1,80}$/u.test(rawCode))
      || typeof rawCode === "number" && Number.isSafeInteger(rawCode)
      ? String(rawCode)
      : null;
    const rawStatus = "status" in error ? error.status : undefined;
    const status = typeof rawStatus === "number" && Number.isSafeInteger(rawStatus) && rawStatus >= 100 && rawStatus <= 599
      ? `HTTP ${rawStatus}`
      : null;
    const rawName = "name" in error ? error.name : undefined;
    const name = typeof rawName === "string" && /^[A-Za-z][A-Za-z0-9_.-]{0,63}$/u.test(rawName)
      ? rawName
      : "remote error";
    const detail = [status, code].filter((part): part is string => part !== null).join(", ");
    return detail.length === 0 ? name : `${name} (${detail})`;
  }
  return "remote error";
}

function tokensOf(query: string): string[] {
  const tokens = query
    .toLowerCase()
    .split(/[^\p{L}\p{N}_-]+/u)
    .filter((token) => token.length >= 2);
  return [...new Set(tokens)].slice(0, 8);
}

function nameMatches(name: string | null | undefined, tokens: readonly string[]): boolean {
  if (name === null || name === undefined || name.length === 0) return false;
  const folded = name.toLowerCase();
  return tokens.some((token) => folded.includes(token));
}

/** The bot thread list is active-only; reject a malformed row instead of filling missing fields. */
function normalizeThread(entry: DiscoveryRemoteThread): DiscoveryRemoteThread | null {
  if (typeof entry.channel_id !== "string" || entry.channel_id.length === 0) return null;
  if (typeof entry.group_no !== "string" || typeof entry.short_id !== "string") return null;
  if (entry.channel_id !== `${entry.group_no}____${entry.short_id}`) return null;
  if (typeof entry.name !== "string" || typeof entry.status !== "number") return null;
  return entry;
}

function publicEvidence(evidence: DiscoveryEvidence): DiscoveryEvidence {
  return { ...evidence };
}

function readEntry(
  candidate: Pick<CandidateSnapshot, "channelType" | "channelId" | "groupId" | "shortId">,
  evidence: readonly DiscoveryEvidence[],
  query: string,
): Record<string, unknown> {
  const octoCli: string[] = [];
  if (candidate.channelType === 5 && candidate.groupId !== null && candidate.shortId !== null) {
    octoCli.push(`octo-cli thread get ${candidate.groupId} ${candidate.shortId}`);
  } else if (candidate.channelType === 2) {
    octoCli.push(`octo-cli group get ${candidate.channelId}`);
  }
  octoCli.push(
    `octo-cli message search --chat-id ${candidate.channelId} --channel-type ${candidate.channelType} --keyword ${JSON.stringify(query)}`,
  );
  const referenceUrls = evidence.flatMap((entry) =>
    entry.kind === "recorded_reference" && typeof entry.url === "string" ? [entry.url] : []);
  const loopCalls = evidence.flatMap((entry) => {
    if (entry.kind !== "loop_route") return [];
    const workspaceId = typeof entry.workspace_id === "string" ? entry.workspace_id : null;
    const taskId = typeof entry.task_id === "string" ? entry.task_id : null;
    return workspaceId !== null && taskId !== null
      ? [`octo-cli loop task get ${taskId} --workspace-id ${workspaceId}`]
      : [];
  });
  const workspaceEvidence = evidence.find((entry) => entry.kind === "workspace");
  return {
    channel: { channel_type: candidate.channelType, channel_id: candidate.channelId },
    octo_cli: octoCli,
    loop: loopCalls,
    references: referenceUrls,
    workspace: workspaceEvidence === undefined ? null : publicEvidence(workspaceEvidence),
  };
}

function outputCandidate(candidate: CandidateSnapshot, query: string): Record<string, unknown> {
  return {
    channel_type: candidate.channelType,
    channel_id: candidate.channelId,
    channelType: candidate.channelType,
    channelId: candidate.channelId,
    group_id: candidate.channelType === 2 ? candidate.channelId : candidate.groupId,
    parent_channel_id: candidate.channelType === 5 ? candidate.groupId : null,
    short_id: candidate.shortId,
    name: candidate.name,
    group_name: candidate.groupName,
    evidence: candidate.evidence.map(publicEvidence),
    sampled_at: iso(candidate.sampledAt),
    explicit: candidate.explicit,
    read: readEntry(candidate, candidate.evidence, query),
    ...(candidate.filtered ? { filtered: true } : {}),
    ...(candidate.score === null ? {} : { score: candidate.score }),
  };
}

function compareCandidates(left: CandidateSnapshot, right: CandidateSnapshot): number {
  const explicitLeft = left.explicit ? 0 : 1;
  const explicitRight = right.explicit ? 0 : 1;
  if (explicitLeft !== explicitRight) return explicitLeft - explicitRight;
  if (left.explicit) {
    const rank = candidateRank(left) - candidateRank(right);
    if (rank !== 0) return rank;
  } else {
    const score = (right.score ?? -1) - (left.score ?? -1);
    if (score !== 0) return score;
    const rank = candidateRank(left) - candidateRank(right);
    if (rank !== 0) return rank;
  }
  return left.channelId < right.channelId ? -1 : left.channelId > right.channelId ? 1 : 0;
}

function pageSlice(
  candidates: readonly CandidateSnapshot[],
  position: number,
  pageSize: number,
): { items: CandidateSnapshot[]; hasMore: boolean } {
  const start = (position - 1) * pageSize;
  return {
    items: candidates.slice(start, start + pageSize),
    hasMore: start + pageSize < candidates.length,
  };
}

function publicSources(sources: Record<string, SourceStatus>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(sources).map(([name, status]) => [name, {
    state: status.state,
    requests: status.requests,
    candidates: status.candidates,
    complete: status.complete,
    sampled_at: status.sampledAt === null ? null : iso(status.sampledAt),
    ...(status.keywordTruncated === undefined ? {} : { keyword_truncated: status.keywordTruncated }),
    ...(status.includesArchived === undefined ? {} : { includes_archived: status.includesArchived }),
    ...(status.detail === undefined ? {} : { detail: status.detail }),
  }]));
}

function publicModel(model: Snapshot["model"]): Record<string, unknown> {
  return {
    status: model.status,
    evaluated: model.evaluated,
    unprocessed: model.unprocessed,
    ...(model.detail === undefined ? {} : { detail: model.detail }),
  };
}

function publicBudgets(limits: DiscoveryLimits): Record<string, number> {
  return {
    candidates_per_page: limits.pageSize,
    remote_requests_per_source: limits.remoteRequestsPerSource,
    collected_candidates: limits.collectedCandidates,
    model_candidates: limits.modelCandidates,
    model_chars_per_candidate: limits.modelCharsPerCandidate,
    model_deadline_ms: limits.modelDeadlineMs,
    snapshot_bytes: limits.snapshotMaxBytes,
    snapshot_ttl_ms: limits.snapshotTtlMs,
    queries_per_task: limits.queriesPerTask,
    queries_per_bot: limits.queriesPerBot,
    remote_keyword_chars: limits.remoteKeywordChars,
    query_chars: limits.queryChars,
  };
}

function sourceCandidateCounts(
  candidates: readonly CandidateSnapshot[],
  sources: Record<string, SourceStatus>,
): void {
  const localMatches = candidates.filter((candidate) => candidate.evidence.some((entry) =>
    entry.source === "local_records")).length;
  sources.local_records!.candidates = localMatches;
  sources.group_search!.candidates = candidates.filter((candidate) =>
    candidate.evidence.some((entry) => entry.source === "group_search")).length;
  sources.message_search!.candidates = candidates.filter((candidate) =>
    candidate.evidence.some((entry) => entry.source === "message_search")).length;
  sources.group_threads!.candidates = candidates.filter((candidate) =>
    candidate.evidence.some((entry) => entry.source === "group_threads")).length;
  sources.loop_workspaces!.candidates = candidates.filter((candidate) =>
    candidate.evidence.some((entry) => entry.kind === "loop_route")).length;
  sources.loop_tasks!.candidates = candidates.filter((candidate) =>
    candidate.evidence.some((entry) => entry.kind === "loop_route" && entry.task_source === "loop_tasks")).length;
  sources.permissions!.candidates = candidates.length;
}

function downgradeCompleteSources(sources: Record<string, SourceStatus>): void {
  for (const status of Object.values(sources)) {
    if (status.state === "complete") status.state = "partial";
    status.complete = false;
  }
}

function pageOutput(
  snapshot: Snapshot,
  collection: "candidates" | "filtered",
  position: number,
  limits: DiscoveryLimits,
  servedAt: number,
): Record<string, unknown> {
  const kept = snapshot.candidates.filter((candidate) => !candidate.filtered);
  const filtered = snapshot.candidates.filter((candidate) => candidate.filtered);
  const selected = collection === "filtered" ? filtered : kept;
  const slice = pageSlice(selected, position, limits.pageSize);
  return {
    query_id: snapshot.queryId,
    action: "page",
    query: snapshot.query,
    generated_at: iso(snapshot.createdAt),
    served_at: iso(servedAt),
    page: {
      collection,
      position,
      page_size: limits.pageSize,
      has_more: slice.hasMore,
      next_position: slice.hasMore ? position + 1 : null,
    },
    candidates: slice.items.map((candidate) => outputCandidate(candidate, snapshot.query)),
    filtered: {
      count: filtered.length,
      page_size: limits.pageSize,
      has_more: filtered.length > limits.pageSize,
      next_position: filtered.length > limits.pageSize ? 1 : null,
    },
    sources: publicSources(snapshot.sources),
    model: publicModel(snapshot.model),
    permissions: {
      state: snapshot.sources.permissions!.state,
      candidates: snapshot.candidates.length,
    },
    budgets: publicBudgets(limits),
  };
}

export interface DiscoveryCoordinatorOptions {
  store: DiscoveryStorePort;
  remote?: DiscoveryRemotePort;
  judge?: DiscoveryJudge;
  now?: () => number;
  limits?: Partial<DiscoveryLimits>;
}

export function createDiscoveryCoordinator(options: DiscoveryCoordinatorOptions): DiscoveryCoordinator {
  const limits: DiscoveryLimits = { ...DISCOVERY_LIMITS, ...options.limits };
  const now = options.now ?? Date.now;
  const remote = options.remote;
  const judge = options.judge;
  const snapshots = new Map<string, Snapshot>();
  const inFlightByTask = new Map<string, number>();
  const inFlightByBot = new Map<string, number>();
  const activeByTask = new Map<string, Set<AbortController>>();
  const taskScopeKey = (botUid: string, taskId: string): string => `${botUid}\u0000${taskId}`;
  const beginOperation = (
    taskId: string,
    parentSignal: AbortSignal,
  ): { signal: AbortSignal; finish: () => void } => {
    const controller = new AbortController();
    let active = activeByTask.get(taskId);
    if (active === undefined) {
      active = new Set();
      activeByTask.set(taskId, active);
    }
    active.add(controller);
    const abortFromParent = (): void => controller.abort(parentSignal.reason);
    parentSignal.addEventListener("abort", abortFromParent, { once: true });
    if (parentSignal.aborted) abortFromParent();
    return {
      signal: controller.signal,
      finish: () => {
        parentSignal.removeEventListener("abort", abortFromParent);
        active.delete(controller);
        if (active.size === 0 && activeByTask.get(taskId) === active) activeByTask.delete(taskId);
      },
    };
  };
  let stopped = false;

  const purgeExpired = (): void => {
    const current = now();
    for (const [queryId, snapshot] of snapshots) {
      if (snapshot.expiresAt <= current) snapshots.delete(queryId);
    }
  };
  const assertSignal = (signal: AbortSignal): void => throwIfAborted(signal);
  const countOf = (source: Map<string, number>, key: string): number => source.get(key) ?? 0;
  const increment = (source: Map<string, number>, key: string): void => {
    source.set(key, countOf(source, key) + 1);
  };
  const decrement = (source: Map<string, number>, key: string): void => {
    const remaining = countOf(source, key) - 1;
    if (remaining <= 0) source.delete(key);
    else source.set(key, remaining);
  };

  async function collectLocal(
    input: DiscoverySearchInput,
    candidates: Map<string, CandidateSnapshot>,
    source: SourceStatus,
    current: number,
  ): Promise<void> {
    const query: DiscoveryQuery = {
      ...(input.workspaceId === undefined ? {} : { workspaceId: input.workspaceId }),
      ...(input.loopTaskId === undefined ? {} : { taskId: input.loopTaskId }),
      ...(input.referenceUrl === undefined ? {} : { referenceUrl: input.referenceUrl }),
      ...(input.repo === undefined ? {} : { repo: input.repo }),
      ...(input.path === undefined ? {} : { path: input.path }),
    };
    if (input.workspaceId === undefined
      && input.loopTaskId === undefined
      && input.referenceUrl === undefined
      && input.repo === undefined
      && input.path === undefined) {
      source.state = "skipped";
      source.complete = false;
      source.sampledAt = current;
      source.detail = "没有结构化线索；未查询本地关联记录";
      return;
    }
    try {
      const associations = options.store.findDiscoveryAssociations(input.botUid, query);
      source.state = "complete";
      source.complete = true;
      source.sampledAt = current;
      for (const association of associations) {
        if (association.kind === "loop_route") {
          const added = addCandidate(
            candidates,
            { channelType: 5, channelId: association.channelId },
            loopRouteEvidence(association),
            true,
            limits.collectedCandidates,
            current,
          );
          if (!added) {
            source.state = "truncated";
            source.complete = false;
            source.detail = "本地关联候选达到单次查询上限";
            break;
          }
        } else if (association.kind === "recorded_reference") {
          const added = addCandidate(candidates, { channelType: 5, channelId: association.channelId }, {
            kind: "recorded_reference",
            source: "local_records",
            channel_type: 5,
            channel_id: association.channelId,
            reference_kind: association.referenceKind,
            url: association.url,
            model_identified: true,
          }, true, limits.collectedCandidates, current);
          if (!added) {
            source.state = "truncated";
            source.complete = false;
            source.detail = "本地关联候选达到单次查询上限";
            break;
          }
        } else {
          const added = addCandidate(candidates, { channelType: 5, channelId: association.channelId }, {
            kind: "workspace",
            source: "local_records",
            channel_type: 5,
            channel_id: association.channelId,
            layout: association.layout,
            status: association.status,
            entry_path: association.entryPath,
            workspace_root: association.workspaceRoot,
            projects: association.projects.map((project) => ({
              name: project.name,
              repo: project.repo,
              path: project.path,
              branch: project.branch,
              created: project.created,
              ...(project.accessPath === undefined ? {} : { access_path: project.accessPath }),
              ...(project.accessTarget === undefined ? {} : { access_target: project.accessTarget }),
            })),
            error: association.error,
          }, false, limits.collectedCandidates, current);
          if (!added) {
            source.state = "truncated";
            source.complete = false;
            source.detail = "本地关联候选达到单次查询上限";
            break;
          }
        }
      }
    } catch {
      source.state = "failed";
      source.complete = false;
      source.detail = "本地记录读取失败";
    }
  }

  async function collectGroupSearch(
    query: string,
    candidates: Map<string, CandidateSnapshot>,
    source: SourceStatus,
    messageQueue: MessageQueueEntry[],
    groupNames: Map<string, string>,
    current: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (remote === undefined) {
      source.state = "unavailable";
      return;
    }
    const keyword = truncateCodePoints(query, limits.remoteKeywordChars);
    if (keyword.truncated) source.keywordTruncated = true;
    try {
      source.requests = 1;
      const result = await remote.searchGroups(keyword.text, signal);
      assertSignal(signal);
      source.sampledAt = current;
      const truncated = result.pagination?.has_more === true;
      source.state = truncated ? "truncated" : "partial";
      source.complete = false;
      source.detail = truncated
        ? "关键词聚合已截断；聚合无游标，计数与命中集合为近似"
        : "聚合没有游标且计数为近似；仅覆盖本次返回的可见聊天";
      for (const bucket of result.data.groups) {
        if (bucket.channel_type !== 2 && bucket.channel_type !== 5) continue;
        if (typeof bucket.channel_id !== "string" || bucket.channel_id.length === 0) continue;
        const groupId = bucket.parent_group_no ?? bucket.channel_id.split("____")[0];
        if (bucket.channel_type === 5 && (groupId === undefined || groupId.length === 0)) continue;
        if (bucket.channel_type === 2 && bucket.group_name.length > 0) {
          groupNames.set(bucket.channel_id, bucket.group_name);
        }
        const preview = Array.isArray(bucket.preview) ? bucket.preview : [];
        if (preview.length === 0) {
          messageQueue.push({ channelType: bucket.channel_type, channelId: bucket.channel_id, cursor: undefined });
          continue;
        }
        for (const hit of preview) {
          if (hit.channel_id !== bucket.channel_id) continue;
          if (typeof hit.message_id !== "string" || hit.message_id.length === 0) continue;
          const added = addCandidate(candidates, {
            channelType: bucket.channel_type,
            channelId: bucket.channel_id,
          }, {
            kind: "message_match",
            source: "group_search",
            channel_type: bucket.channel_type,
            channel_id: bucket.channel_id,
            message_id: hit.message_id,
            sent_at: typeof hit.sent_at === "string" ? hit.sent_at : null,
            approximate: bucket.match_count_approx ?? true,
          }, false, limits.collectedCandidates, current);
          if (!added) {
            source.state = "truncated";
            source.detail = "候选数量达到单次查询上限";
            break;
          }
        }
        if (bucket.channel_type === 5 && nameMatches(bucket.thread_name, tokensOf(query))) {
          const added = addCandidate(candidates, { channelType: 5, channelId: bucket.channel_id }, {
            source: "group_search",
            kind: "name_match",
            channel_type: 5,
            channel_id: bucket.channel_id,
            matched: "thread",
          }, false, limits.collectedCandidates, current);
          if (!added) {
            source.state = "truncated";
            source.complete = false;
            source.detail = "候选数量达到单次查询上限";
          }
        }
      }
    } catch (error) {
      if (signal.aborted) throw error;
      source.state = "failed";
      source.complete = false;
      source.detail = safeRemoteError(error);
    }
  }

  async function collectMessageSearch(
    query: string,
    candidates: Map<string, CandidateSnapshot>,
    source: SourceStatus,
    queue: MessageQueueEntry[],
    current: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (remote === undefined) {
      source.state = "unavailable";
      return;
    }
    if (queue.length === 0) {
      source.state = "skipped";
      source.complete = false;
      source.sampledAt = current;
      source.detail = "聚合命中的聊天均已包含消息预览，未发起单频道检索";
      return;
    }
    const keyword = truncateCodePoints(query, limits.remoteKeywordChars);
    if (keyword.truncated) source.keywordTruncated = true;
    const budget: { max: number; requests: number } = { max: limits.remoteRequestsPerSource, requests: 0 };
    source.sampledAt = current;
    while (queue.length > 0 && budget.requests < budget.max) {
      const pending = queue.shift()!;
      try {
        budget.requests += 1;
        source.requests += 1;
        const result = await remote.searchMessages(keyword.text, pending.channelType, pending.channelId, pending.cursor, signal);
        assertSignal(signal);
        for (const hit of result.data) {
          if (hit.channel_id !== pending.channelId) continue;
          if (hit.channel_type !== undefined && hit.channel_type !== pending.channelType) continue;
          if (typeof hit.message_id !== "string" || hit.message_id.length === 0) continue;
          const added = addCandidate(candidates, {
            channelType: pending.channelType,
            channelId: pending.channelId,
          }, {
            source: "message_search",
            kind: "message_match",
            channel_type: pending.channelType,
            channel_id: pending.channelId,
            message_id: hit.message_id,
            sent_at: typeof hit.sent_at === "string" ? hit.sent_at : null,
            approximate: false,
          }, false, limits.collectedCandidates, current);
          if (!added) {
            source.state = "truncated";
            source.detail = "候选数量达到单次查询上限";
            queue.length = 0;
            break;
          }
        }
        if (result.pagination.has_more) {
          if (result.pagination.next_cursor.length > 0) {
            queue.push({ ...pending, cursor: result.pagination.next_cursor });
          } else {
            source.state = "failed";
            source.complete = false;
            source.detail = "远端分页契约缺少续页游标";
          }
        }
      } catch (error) {
        if (signal.aborted) throw error;
        source.state = "failed";
        source.complete = false;
        source.detail = safeRemoteError(error);
        break;
      }
    }
    if (source.state !== "failed" && source.state !== "truncated") {
      if (queue.length > 0) {
        source.state = "budget_exhausted";
        source.complete = false;
        source.detail = "单来源远端请求预算已用完；仍有聊天或分页未检索";
      } else {
        source.state = "partial";
        source.complete = false;
        source.detail = "仅检索聚合命中的聊天；单频道结果已到末页";
      }
    }
  }

  async function collectEnumeration(
    query: string,
    candidates: Map<string, CandidateSnapshot>,
    source: SourceStatus,
    groupNames: Map<string, string>,
    current: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (remote === undefined) {
      source.state = "unavailable";
      return;
    }
    const tokens = tokensOf(query);
    const budget: { max: number; requests: number } = { max: limits.remoteRequestsPerSource, requests: 0 };
    let truncated = false;
    let failure: string | null = null;
    source.sampledAt = current;
    source.includesArchived = false;
    try {
      budget.requests += 1;
      source.requests += 1;
      const groups = await remote.listGroups(signal);
      assertSignal(signal);
      for (const group of groups) {
        if (typeof group.group_no !== "string" || group.group_no.length === 0) continue;
        const groupName = typeof group.name === "string" ? group.name : "";
        if (groupName.length > 0) groupNames.set(group.group_no, groupName);
        if (nameMatches(groupName, tokens)) {
          const added = addCandidate(candidates, { channelType: 2, channelId: group.group_no }, {
            source: "group_threads",
            kind: "name_match",
            channel_type: 2,
            channel_id: group.group_no,
            matched: "group",
          }, false, limits.collectedCandidates, current);
          if (!added) truncated = true;
        }
        let page = 1;
        while (budget.requests < budget.max && !truncated) {
          try {
            budget.requests += 1;
            source.requests += 1;
            const threadPage = await remote.listThreads(group.group_no, page, signal);
            assertSignal(signal);
            for (const raw of threadPage.list) {
              const entry = normalizeThread(raw);
              if (entry === null || !nameMatches(entry.name, tokens)) continue;
              const added = addCandidate(candidates, { channelType: 5, channelId: entry.channel_id }, {
                kind: "name_match",
                source: "group_threads",
                channel_type: 5,
                channel_id: entry.channel_id,
                matched: "thread",
              }, false, limits.collectedCandidates, current);
              if (!added) {
                truncated = true;
                break;
              }
            }
            if (truncated || page * 100 >= threadPage.count) break;
            page += 1;
          } catch (error) {
            if (signal.aborted) throw error;
            failure = safeRemoteError(error);
            break;
          }
        }
        if (failure !== null || truncated || budget.requests >= budget.max) break;
      }
      if (truncated || budget.requests >= budget.max) {
        source.state = "truncated";
        source.complete = false;
        source.detail = "群/子区枚举在候选或远端请求上限处截断；Bot 子区列表只含 active";
      } else if (failure !== null) {
        source.state = "failed";
        source.complete = false;
        source.detail = failure;
      } else {
        source.state = "partial";
        source.complete = false;
        source.detail = "群列表没有分页且只证明非删除成员；子区列表只含 active，不含归档";
      }
    } catch (error) {
      if (signal.aborted) throw error;
      source.state = "failed";
      source.complete = false;
      source.detail = safeRemoteError(error);
    }
  }

  async function collectLoopSources(
    input: DiscoverySearchInput,
    candidates: Map<string, CandidateSnapshot>,
    workspaceSource: SourceStatus,
    taskSource: SourceStatus,
    permissions: SourceStatus,
    current: number,
    signal: AbortSignal,
  ): Promise<{ restricted: boolean }> {
    workspaceSource.sampledAt = current;
    taskSource.sampledAt = current;
    if (remote === undefined) {
      workspaceSource.state = "unavailable";
      taskSource.state = "unavailable";
      return { restricted: false };
    }

    const routeEvidence = [...candidates.values()].flatMap((candidate) =>
      candidate.evidence.filter((entry): entry is DiscoveryEvidence & {
        kind: "loop_route";
        workspace_id: string;
        task_id: string;
      } => entry.kind === "loop_route"
        && typeof entry.workspace_id === "string"
        && typeof entry.task_id === "string"));
    const workspaceRows: DiscoveryRemoteWorkspaceRow[] = [];
    let workspaceListComplete = false;
    let workspaceListFailed = false;
    let restricted = false;

    if (input.workspaceId !== undefined) {
      workspaceSource.state = "partial";
      workspaceSource.detail = "仅核验指定 workspace 与其本地路由，不枚举其他 workspace";
    } else {
      let page = 1;
      while (workspaceSource.requests < limits.remoteRequestsPerSource) {
        try {
          workspaceSource.requests += 1;
          const result = await remote.listWorkspaces(page, signal);
          assertSignal(signal);
          workspaceRows.push(...result.data);
          if (result.data.length === 0
            || result.pagination.total <= result.pagination.page * result.pagination.page_size) {
            workspaceListComplete = true;
            break;
          }
          if (workspaceSource.requests >= limits.remoteRequestsPerSource) {
            workspaceSource.state = "budget_exhausted";
            workspaceSource.detail = "workspace 列表仍有后续页；远端请求预算已用完";
            break;
          }
          page += 1;
        } catch (error) {
          if (signal.aborted) throw error;
          workspaceListFailed = true;
          workspaceSource.state = "failed";
          workspaceSource.complete = false;
          workspaceSource.detail = safeRemoteError(error);
          break;
        }
      }
      if (!workspaceListComplete && !workspaceListFailed && workspaceSource.state === "unavailable") {
        workspaceSource.state = "budget_exhausted";
        workspaceSource.detail = "workspace 列表未完成";
      }
      if (workspaceListComplete) {
        workspaceSource.state = "complete";
        workspaceSource.complete = true;
      }
    }

    const workspaceIds = new Set<string>();
    if (input.workspaceId !== undefined) workspaceIds.add(input.workspaceId);
    for (const evidence of routeEvidence) workspaceIds.add(evidence.workspace_id);
    for (const row of workspaceRows) workspaceIds.add(row.workspace_id);

    const authorizedWorkspaces = new Set<string>();
    let verificationLimited = false;
    let workspaceDenied = false;
    let workspaceVerificationRequests = 0;
    for (const workspaceId of workspaceIds) {
      if (workspaceVerificationRequests >= limits.remoteRequestsPerSource) {
        verificationLimited = true;
        restricted = true;
        removeLoopEvidence(candidates, workspaceId);
        continue;
      }
      permissions.requests += 1;
      workspaceVerificationRequests += 1;
      try {
        const detail = await remote.verifyWorkspace(workspaceId, signal);
        assertSignal(signal);
        const id = typeof detail.workspace_id === "string" ? detail.workspace_id : detail.id;
        if (typeof id !== "string" || id.toLowerCase() !== workspaceId.toLowerCase()) {
          throw new Error("workspace verification identity mismatch");
        }
        authorizedWorkspaces.add(workspaceId);
      } catch (error) {
        if (signal.aborted) throw error;
        restricted = true;
        workspaceDenied = true;
        removeLoopEvidence(candidates, workspaceId);
      }
    }

    for (const [key, candidate] of candidates) {
      if (candidate.evidence.length === 0) candidates.delete(key);
    }
    if (workspaceDenied) {
      workspaceSource.state = workspaceListFailed ? "failed" : "restricted";
      workspaceSource.complete = false;
      workspaceSource.detail = "部分 Loop workspace 无法核验；受限关联已移除";
      permissions.state = "restricted";
    } else if (verificationLimited) {
      workspaceSource.state = "budget_exhausted";
      workspaceSource.complete = false;
      workspaceSource.detail = "单次查询最多核验 10 个 workspace；其余 Loop 证据已移除";
      permissions.state = "restricted";
    } else if (input.workspaceId !== undefined) {
      workspaceSource.state = "partial";
      workspaceSource.complete = false;
      workspaceSource.detail = "仅核验指定 workspace 与其本地路由，不枚举其他 workspace";
    }

    const taskWorkspaceIds = [...workspaceIds].filter((workspaceId) =>
      authorizedWorkspaces.has(workspaceId));
    if (input.workspaceId !== undefined) {
      taskWorkspaceIds.splice(0, taskWorkspaceIds.length, input.workspaceId);
    }
    const taskKeyword = input.loopTaskId === undefined
      ? truncateCodePoints(input.query, limits.remoteKeywordChars)
      : { text: input.loopTaskId, truncated: false };
    if (taskKeyword.truncated) taskSource.keywordTruncated = true;
    let taskFailure: string | undefined;
    let taskBudgetExhausted = false;
    let taskResponses = 0;
    let candidateLimitReached = false;
    const searchedWorkspaces = new Set<string>();

    outer: for (const workspaceId of taskWorkspaceIds) {
      if (!authorizedWorkspaces.has(workspaceId)) continue;
      let page = 1;
      while (true) {
        if (taskSource.requests >= limits.remoteRequestsPerSource) {
          taskBudgetExhausted = true;
          break outer;
        }
        taskSource.requests += 1;
        searchedWorkspaces.add(workspaceId);
        try {
          const result = await remote.searchTasks(workspaceId, taskKeyword.text, page, signal);
          taskResponses += 1;
          assertSignal(signal);
          for (const task of result.data) {
            if (task.workspace_id !== undefined && task.workspace_id !== workspaceId) continue;
            if (input.loopTaskId !== undefined && task.task_id !== input.loopTaskId) continue;
            const associations = options.store.findDiscoveryAssociations(input.botUid, {
              workspaceId,
              taskId: task.task_id,
            });
            for (const association of associations) {
              if (association.kind !== "loop_route"
                || association.workspaceId !== workspaceId
                || association.taskId !== task.task_id) continue;
              const evidence = loopRouteEvidence(association, task);
              const key = candidateKey(association.channelType, association.channelId);
              const candidate = candidates.get(key);
              if (candidate !== undefined) {
                const existing = candidate.evidence.find((entry) =>
                  entry.kind === "loop_route" && evidenceKey(entry) === evidenceKey(evidence));
                if (existing !== undefined) existing.task = evidence.task;
                else mergeEvidence(candidate, [evidence]);
                candidate.explicit = true;
                continue;
              }
              if (!addCandidate(candidates, {
                channelType: association.channelType,
                channelId: association.channelId,
              }, evidence, true, limits.collectedCandidates, current)) {
                candidateLimitReached = true;
                break outer;
              }
            }
          }
          if (result.data.length === 0
            || result.pagination.total <= result.pagination.page * result.pagination.page_size) break;
          page += 1;
        } catch (error) {
          if (signal.aborted) throw error;
          taskFailure = safeRemoteError(error);
          break outer;
        }
      }
    }

    if (taskFailure !== undefined) {
      taskSource.state = taskResponses > 0 ? "partial" : "failed";
      taskSource.complete = false;
      taskSource.detail = taskFailure;
    } else if (candidateLimitReached) {
      taskSource.state = "truncated";
      taskSource.complete = false;
      taskSource.detail = "候选数量达到单次查询上限";
    } else if (taskBudgetExhausted) {
      taskSource.state = "budget_exhausted";
      taskSource.complete = false;
      taskSource.detail = "Loop task 分页或 workspace 搜索仍有未处理范围";
    } else if (searchedWorkspaces.size === 0) {
      taskSource.state = "skipped";
      taskSource.complete = false;
      taskSource.detail = "没有完成 workspace 权限核验，未搜索 Loop task";
    } else {
      taskSource.state = "partial";
      taskSource.complete = false;
      taskSource.detail = input.workspaceId === undefined
        ? "只覆盖已列出并核验的 workspace 和 Loop task 搜索索引"
        : "只覆盖指定 workspace 的 Loop task 搜索索引";
    }

    if (workspaceListFailed && workspaceSource.state !== "failed") {
      workspaceSource.state = "failed";
      workspaceSource.complete = false;
    }
    return { restricted };
  }

  function removeLoopEvidence(
    candidates: Map<string, CandidateSnapshot>,
    workspaceId: string,
    taskId?: string,
  ): void {
    for (const candidate of candidates.values()) {
      candidate.evidence = candidate.evidence.filter((entry) =>
        entry.kind !== "loop_route"
        || entry.workspace_id !== workspaceId
        || (taskId !== undefined && entry.task_id !== taskId));
      candidate.explicit = isExplicit(candidate.evidence);
    }
  }


  async function verifyTargets(
    candidates: Map<string, CandidateSnapshot>,
    groupNames: Map<string, string>,
    permissions: SourceStatus,
    current: number,
    signal: AbortSignal,
  ): Promise<{ restricted: boolean }> {
    if (remote === undefined) {
      permissions.state = "unavailable";
      candidates.clear();
      return { restricted: false };
    }
    let restricted = false;
    const verified = new Map<string, DiscoveryRemoteTargetVerification>();
    let permissionRequests = 0;
    for (const [key, candidate] of candidates) {
      let verification = verified.get(key);
      if (verification === undefined) {
        permissionRequests += 1;
        try {
          verification = await remote.verifyTarget(
            { channelType: candidate.channelType, channelId: candidate.channelId },
            signal,
          );
          assertSignal(signal);
          if (verification.channelType !== candidate.channelType || verification.channelId !== candidate.channelId) {
            throw new Error("target verification identity mismatch");
          }
          verified.set(key, verification);
        } catch (error) {
          if (signal.aborted) throw error;
          restricted = true;
          candidates.delete(key);
          continue;
        }
      }
      if (verification.channelType === 5) {
        const thread = normalizeThread(verification.thread);
        if (thread === null || thread.channel_id !== candidate.channelId) {
          restricted = true;
          candidates.delete(key);
          continue;
        }
        candidate.groupId = thread.group_no;
        candidate.shortId = thread.short_id;
        candidate.name = thread.name;
        candidate.groupName = thread.group_name ?? groupNames.get(thread.group_no) ?? candidate.groupName;
      } else {
        candidate.groupId = candidate.channelId;
        candidate.shortId = null;
        candidate.name = groupNames.get(candidate.channelId) ?? null;
        candidate.groupName = candidate.name;
      }
    }
    permissions.state = restricted || permissions.state === "restricted" ? "restricted" : "complete";
    permissions.requests += permissionRequests;
    permissions.sampledAt = current;
    return { restricted };
  }

  async function revalidateSnapshot(
    snapshot: Snapshot,
    current: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (remote === undefined) {
      snapshot.candidates = [];
      snapshot.sources.permissions!.state = "unavailable";
      return;
    }
    let changed = false;
    let permissionRequests = 0;
    const verified = new Map<string, DiscoveryRemoteTargetVerification>();
    const workspaceAccess = new Map<string, boolean>();
    const surviving: CandidateSnapshot[] = [];
    for (const original of snapshot.candidates) {
      const candidate: CandidateSnapshot = {
        ...original,
        evidence: original.evidence.map((entry) => ({ ...entry })),
      };
      const key = candidateKey(candidate.channelType, candidate.channelId);
      let verification = verified.get(key);
      if (verification === undefined) {
        permissionRequests += 1;
        try {
          verification = await remote.verifyTarget(
            { channelType: candidate.channelType, channelId: candidate.channelId },
            signal,
          );
          assertSignal(signal);
          if (verification.channelType !== candidate.channelType || verification.channelId !== candidate.channelId) {
            changed = true;
            continue;
          }
          verified.set(key, verification);
        } catch (error) {
          if (signal.aborted) throw error;
          changed = true;
          continue;
        }
      }
      if (verification.channelType === 5) {
        const thread = normalizeThread(verification.thread);
        if (thread === null || thread.channel_id !== candidate.channelId) {
          changed = true;
          continue;
        }
        candidate.groupId = thread.group_no;
        candidate.shortId = thread.short_id;
        candidate.name = thread.name;
        candidate.groupName = thread.group_name ?? candidate.groupName;
      }
      const loopEntries = candidate.evidence.filter((entry) =>
        entry.kind === "loop_route" && typeof entry.workspace_id === "string");
      for (const entry of loopEntries) {
        const workspaceId = entry.workspace_id as string;
        let allowed = workspaceAccess.get(workspaceId);
        if (allowed === undefined) {
          permissionRequests += 1;
          try {
            const detail = await remote.verifyWorkspace(workspaceId, signal);
            assertSignal(signal);
            const id = typeof detail.workspace_id === "string" ? detail.workspace_id : detail.id;
            allowed = typeof id === "string" && id.toLowerCase() === workspaceId.toLowerCase();
            if (!allowed) changed = true;
          } catch (error) {
            if (signal.aborted) throw error;
            allowed = false;
            changed = true;
          }
          workspaceAccess.set(workspaceId, allowed);
        }
        if (!allowed) {
          candidate.evidence = candidate.evidence.filter((existing) => existing !== entry);
          changed = true;
        }
      }
      candidate.explicit = isExplicit(candidate.evidence);
      if (candidate.evidence.length === 0) {
        changed = true;
        continue;
      }
      surviving.push(candidate);
    }
    assertSignal(signal);
    if (changed) {
      snapshot.sources.permissions!.state = "restricted";
      downgradeCompleteSources(snapshot.sources);
      snapshot.sources.permissions!.state = "restricted";
    }
    snapshot.candidates = surviving;
    snapshot.sources.permissions!.candidates = surviving.length;
    snapshot.sources.permissions!.requests += permissionRequests;
    snapshot.sources.permissions!.sampledAt = current;
    sourceCandidateCounts(snapshot.candidates, snapshot.sources);
  }

  async function classify(
    query: string,
    candidates: Map<string, CandidateSnapshot>,
    model: Snapshot["model"],
    signal: AbortSignal,
  ): Promise<void> {
    if (judge === undefined) {
      model.status = "skipped";
      return;
    }
    const eligible = [...candidates.values()].filter((candidate) => !candidate.explicit);
    if (eligible.length === 0) {
      model.status = "skipped";
      return;
    }
    const batch = eligible.slice(0, limits.modelCandidates);
    const unprocessed = eligible.length - batch.length;
    model.unprocessed = unprocessed;
    const controller = new AbortController();
    const abortFromOuter = (): void => controller.abort(signal.reason);
    signal.addEventListener("abort", abortFromOuter, { once: true });
    const timer = setTimeout(() => {
      controller.abort(new Error("decision deadline exceeded"));
    }, limits.modelDeadlineMs);
    try {
      const response = await judge(query, batch.map((candidate) => {
        const text = truncateCodePoints(
          [
            candidate.name ?? "",
            candidate.groupName ?? "",
            ...candidate.evidence.map((entry) => {
              if (entry.kind === "loop_route") {
                const task = entry.task;
                const taskTitle = task !== null && typeof task === "object" && "title" in task
                  ? String(task.title)
                  : "";
                return `loop_route ${taskTitle} ${String(entry.state)}`;
              }
              if (entry.kind === "recorded_reference") return `reference ${String(entry.url)}`;
              if (entry.kind === "workspace") {
                const projects = Array.isArray(entry.projects) ? entry.projects : [];
                return `workspace ${projects
                  .map((project: { repo?: unknown }) => typeof project.repo === "string" ? project.repo : "")
                  .filter((repo) => repo.length > 0)
                  .join(" ")}`;
              }
              return entry.kind === "message_match" ? "message_match" : "name_match";
            }),
          ].join(" | "),
          limits.modelCharsPerCandidate,
        ).text;
        return { id: candidateKey(candidate.channelType, candidate.channelId), text };
      }), controller.signal);
      assertSignal(signal);
      if (!Array.isArray(response)) throw new Error("decision response is not an array");
      const ids = new Set(batch.map((candidate) => candidateKey(candidate.channelType, candidate.channelId)));
      const seen = new Set<string>();
      for (const entry of response) {
        if (typeof entry.id !== "string" || !ids.has(entry.id) || seen.has(entry.id)) {
          throw new Error("decision response contains an unknown or duplicate candidate id");
        }
        if (typeof entry.relevant !== "boolean") throw new Error("decision response is missing relevance");
        if (typeof entry.score !== "number" || !Number.isFinite(entry.score) || entry.score < 0 || entry.score > 1) {
          throw new Error("decision response has an invalid score");
        }
        seen.add(entry.id);
      }
      if (seen.size !== batch.length) throw new Error("decision response is incomplete");
      for (const candidate of candidates.values()) {
        if (candidate.explicit) continue;
        const judgment = response.find((entry) =>
          entry.id === candidateKey(candidate.channelType, candidate.channelId));
        if (judgment === undefined) continue;
        candidate.score = judgment.score;
        candidate.filtered = !judgment.relevant;
      }
      model.evaluated = batch.length;
      model.status = unprocessed > 0 ? "partial" : "complete";
      if (unprocessed > 0) model.detail = "部分推断候选超出模型预算，已保留且未分类";
    } catch (error) {
      if (signal.aborted) throw error;
      for (const candidate of candidates.values()) {
        if (candidate.explicit) continue;
        candidate.filtered = false;
        candidate.score = null;
      }
      model.status = "failed";
      model.evaluated = 0;
      model.unprocessed = eligible.length;
      model.detail = controller.signal.aborted
        ? "模型请求超时或取消；候选已保留并标记未完成"
        : "模型请求未完成；候选已保留并标记未完成";
    } finally {
      clearTimeout(timer);
      signal.removeEventListener("abort", abortFromOuter);
    }
  }

  async function search(input: DiscoverySearchInput, signal: AbortSignal): Promise<string> {
    if (stopped) throw new Error("发现查询已失效");
    assertSignal(signal);
    const query = input.query.trim();
    if (query.length === 0) throw new Error("query 不能为空");
    if (Array.from(query).length > limits.queryChars) throw new Error("query 超出长度上限");
    if (input.loopTaskId !== undefined && input.workspaceId === undefined) {
      throw new Error("task_id 必须与 workspace_id 成对提供");
    }
    purgeExpired();
    const taskKey = taskScopeKey(input.botUid, input.taskId);
    const taskQueries = [...snapshots.values()].filter((snapshot) =>
      snapshot.botUid === input.botUid && snapshot.taskId === input.taskId).length
      + countOf(inFlightByTask, taskKey);
    if (taskQueries >= limits.queriesPerTask) throw new Error("本任务活动查询数量已达上限");
    const botQueries = [...snapshots.values()].filter((snapshot) => snapshot.botUid === input.botUid).length
      + countOf(inFlightByBot, input.botUid);
    if (botQueries >= limits.queriesPerBot) throw new Error("本 bot 活动查询数量已达上限");

    const current = now();
    increment(inFlightByTask, taskKey);
    increment(inFlightByBot, input.botUid);
    const operation = beginOperation(input.taskId, signal);
    signal = operation.signal;
    const sources: Record<string, SourceStatus> = {
      local_records: sourceStatus(),
      group_search: sourceStatus(),
      message_search: sourceStatus(),
      group_threads: sourceStatus(),
      loop_workspaces: sourceStatus(),
      loop_tasks: sourceStatus(),
      permissions: sourceStatus(),
    };
    try {
      assertSignal(signal);
      const candidates = new Map<string, CandidateSnapshot>();
      const messageQueue: MessageQueueEntry[] = [];
      const groupNames = new Map<string, string>();
      await collectLocal(input, candidates, sources.local_records!, current);
      await collectGroupSearch(query, candidates, sources.group_search!, messageQueue, groupNames, current, signal);
      await collectMessageSearch(query, candidates, sources.message_search!, messageQueue, current, signal);
      await collectEnumeration(query, candidates, sources.group_threads!, groupNames, current, signal);
      const loop = await collectLoopSources(
        input,
        candidates,
        sources.loop_workspaces!,
        sources.loop_tasks!,
        sources.permissions!,
        current,
        signal,
      );
      const target = await verifyTargets(candidates, groupNames, sources.permissions!, current, signal);
      assertSignal(signal);
      const restricted = loop.restricted || target.restricted;
      if (restricted) {
        downgradeCompleteSources(sources);
        sources.permissions!.state = "restricted";
      }
      const model: Snapshot["model"] = { status: "skipped", evaluated: 0, unprocessed: 0 };
      await classify(query, candidates, model, signal);
      assertSignal(signal);
      sourceCandidateCounts([...candidates.values()], sources);
      const snapshot: Snapshot = {
        queryId: randomUUID(),
        botUid: input.botUid,
        taskId: input.taskId,
        query,
        createdAt: current,
        expiresAt: current + limits.snapshotTtlMs,
        candidates: [...candidates.values()].sort(compareCandidates),
        sources,
        model,
      };
      if (bytesOf(snapshot) > limits.snapshotMaxBytes) {
        throw new Error("查询快照超出容量上限，未发布");
      }
      if (signal.aborted || stopped) throw new Error("发现查询已取消");
      snapshots.set(snapshot.queryId, snapshot);
      const kept = snapshot.candidates.filter((candidate) => !candidate.filtered);
      const filtered = snapshot.candidates.filter((candidate) => candidate.filtered);
      const first = pageSlice(kept, 1, limits.pageSize);
      return JSON.stringify({
        query_id: snapshot.queryId,
        action: "search",
        query,
        generated_at: iso(current),
        page: {
          collection: "candidates",
          position: 1,
          page_size: limits.pageSize,
          has_more: first.hasMore,
          next_position: first.hasMore ? 2 : null,
        },
        candidates: first.items.map((candidate) => outputCandidate(candidate, query)),
        filtered: {
          count: filtered.length,
          page_size: limits.pageSize,
          has_more: filtered.length > limits.pageSize,
          next_position: filtered.length > limits.pageSize ? 1 : null,
        },
        sources: publicSources(sources),
        model: publicModel(model),
        permissions: {
          state: sources.permissions!.state,
          candidates: snapshot.candidates.length,
        },
        budgets: publicBudgets(limits),
      });
    } finally {
      decrement(inFlightByTask, taskKey);
      decrement(inFlightByBot, input.botUid);
      operation.finish();
    }
  }

  async function page(input: DiscoveryPageInput, signal: AbortSignal): Promise<string> {
    if (stopped) throw new Error("发现查询已失效");
    assertSignal(signal);
    purgeExpired();
    const snapshot = snapshots.get(input.queryId);
    if (snapshot === undefined || snapshot.botUid !== input.botUid || snapshot.taskId !== input.taskId) {
      throw new Error("查询不存在或已失效");
    }
    const collection = input.collection ?? "candidates";
    if (collection !== "candidates" && collection !== "filtered") throw new Error("collection 无效");
    const position = input.position ?? 1;
    if (!Number.isSafeInteger(position) || position < 1) throw new Error("position 必须是正整数");
    const operation = beginOperation(input.taskId, signal);
    signal = operation.signal;
    try {
      await revalidateSnapshot(snapshot, now(), signal);
      assertSignal(signal);
      return JSON.stringify(pageOutput(snapshot, collection, position, limits, now()));
    } finally {
      operation.finish();
    }
  }

  return {
    search,
    page,
    releaseTask: (taskId: string): void => {
      for (const [queryId, snapshot] of snapshots) {
        if (snapshot.taskId === taskId) snapshots.delete(queryId);
      }
      for (const controller of activeByTask.get(taskId) ?? []) {
        controller.abort(new Error("task finished"));
      }
    },
    stop: (): void => {
      stopped = true;
      for (const controllers of activeByTask.values()) {
        for (const controller of controllers) controller.abort(new Error("channel stopping"));
      }
      snapshots.clear();
    },
  };
}

export type {
  DiscoveryAssociation,
  DiscoveryQuery,
  LoopRouteAssociation,
  RecordedReferenceAssociation,
  WorkspaceAssociation,
};
