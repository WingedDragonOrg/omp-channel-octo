import { expect, test } from "bun:test";
import {
  createDiscoveryCoordinator,
  type DiscoveryAssociation,
  type DiscoveryRemoteGroup,
  type DiscoveryRemoteGroupBucket,
  type DiscoveryRemoteGroupSearchPage,
  type DiscoveryJudge,
  type LoopRouteAssociation,
  type DiscoveryRemoteMessageHit,
  type DiscoveryRemoteMessagePage,
  type DiscoveryRemotePort,
  type DiscoveryRemoteTask,
  type DiscoveryRemoteTaskPage,
  type DiscoveryRemoteThread,
  type DiscoveryRemoteThreadPage,
  type DiscoveryRemoteWorkspaceDetail,
  type DiscoveryRemoteWorkspacePage,
  type DiscoveryRemoteWorkspaceRow,
  type DiscoveryQuery,
  type DiscoveryStorePort,
} from "../src/discovery/coordinator.ts";

const BOT = "discovery-bot";
const OTHER_BOT = "other-bot";

function thread(groupId: string, shortId: string, name: string, status = 1): DiscoveryRemoteThread {
  return { short_id: shortId, group_no: groupId, channel_id: `${groupId}____${shortId}`, name, status };
}

function group(groupId: string, name: string): DiscoveryRemoteGroup {
  return { group_no: groupId, name };
}

function bucket(input: {
  groupId: string;
  shortId?: string;
  name: string;
  channelId?: string;
  preview?: DiscoveryRemoteMessageHit[];
}): DiscoveryRemoteGroupBucket {
  const channelId = input.channelId ?? (input.shortId ? `${input.groupId}____${input.shortId}` : input.groupId);
  return {
    channel_id: channelId,
    channel_type: input.shortId ? 5 : 2,
    group_name: input.name,
    match_count: 1,
    match_count_approx: true,
    latest_at: "2026-11-17T00:00:00.000Z",
    ...(input.shortId === undefined ? {} : {
      parent_group_no: input.groupId,
      thread_id: input.shortId,
      thread_name: input.name,
    }),
    preview: input.preview ?? [],
  };
}

function messageHit(input: {
  channelId: string;
  channelType?: 2 | 5;
  messageId: string;
  snippet?: string;
}): DiscoveryRemoteMessageHit {
  return {
    message_id: input.messageId,
    channel_id: input.channelId,
    channel_type: input.channelType ?? 5,
    sender_id: "owner-1",
    sent_at: "2026-11-17T00:00:00.000Z",
    ...(input.snippet === undefined ? {} : { snippet: input.snippet }),
  };
}

function loopRoute(input: {
  channelId: string;
  groupId: string;
  workspaceId: string;
  taskId: string;
}): LoopRouteAssociation {
  return {
    kind: "loop_route",
    channelType: 5,
    channelId: input.channelId,
    groupId: input.groupId,
    workspaceId: input.workspaceId,
    taskId: input.taskId,
    routeVersion: 2,
    state: "active",
    reason: null,
    updatedAt: 1_760_000_000_000,
  };
}

function recordedReference(channelId: string, url: string): DiscoveryAssociation {
  return { kind: "recorded_reference", channelType: 5, channelId, referenceKind: "pr", url };
}

interface CallCounts {
  listGroups: number;
  listThreads: number;
  searchGroups: number;
  searchMessages: number;
  listWorkspaces: number;
  searchTasks: number;
  verifyTarget: number;
  verifyWorkspace: number;
}

class FakeRemote implements DiscoveryRemotePort {
  readonly groups: DiscoveryRemoteGroup[] = [];
  readonly threads = new Map<string, DiscoveryRemoteThread[]>();
  readonly buckets: DiscoveryRemoteGroupBucket[] = [];
  readonly messageHits = new Map<string, DiscoveryRemoteMessageHit[]>();
  readonly workspaces: DiscoveryRemoteWorkspaceRow[] = [];
  readonly tasks = new Map<string, DiscoveryRemoteTask[]>();
  readonly workspaceDetails = new Map<string, DiscoveryRemoteWorkspaceDetail>();
  readonly deniedTargets = new Set<string>();
  readonly deniedWorkspaces = new Set<string>();
  readonly calls: CallCounts = {
    listGroups: 0,
    listThreads: 0,
    searchGroups: 0,
    searchMessages: 0,
    listWorkspaces: 0,
    searchTasks: 0,
    verifyTarget: 0,
    verifyWorkspace: 0,
  };
  lastSearchQuery = "";

  async listGroups(signal?: AbortSignal): Promise<DiscoveryRemoteGroup[]> {
    signal?.throwIfAborted();
    this.calls.listGroups += 1;
    return this.groups;
  }

  async listThreads(groupId: string, page: number, signal?: AbortSignal): Promise<DiscoveryRemoteThreadPage> {
    signal?.throwIfAborted();
    this.calls.listThreads += 1;
    const all = this.threads.get(groupId) ?? [];
    const start = (page - 1) * 100;
    return { count: all.length, list: all.slice(start, start + 100) };
  }

  async searchGroups(query: string, signal?: AbortSignal): Promise<DiscoveryRemoteGroupSearchPage> {
    signal?.throwIfAborted();
    this.calls.searchGroups += 1;
    this.lastSearchQuery = query;
    const data = {
      sequence: 1,
      query_id: "search-1",
      total_groups: this.buckets.length,
      total_groups_approx: true,
      groups: this.buckets,
    };
    return { data, pagination: { has_more: false, next_cursor: "" } };
  }

  async searchMessages(
    query: string,
    channelType: 1 | 2 | 5,
    channelId: string,
    cursor: string | undefined,
    signal?: AbortSignal,
  ): Promise<DiscoveryRemoteMessagePage> {
    signal?.throwIfAborted();
    this.calls.searchMessages += 1;
    this.lastSearchQuery = query;
    const hits = (this.messageHits.get(channelId) ?? []).filter((hit) =>
      hit.channel_type === undefined || hit.channel_type === channelType);
    const start = cursor === undefined ? 0 : Number(cursor);
    const page = hits.slice(start, start + 2);
    const next = start + page.length;
    return {
      data: page,
      pagination: {
        has_more: next < hits.length,
        next_cursor: next < hits.length ? String(next) : "",
      },
    };
  }

  async listWorkspaces(page: number, signal?: AbortSignal): Promise<DiscoveryRemoteWorkspacePage> {
    signal?.throwIfAborted();
    this.calls.listWorkspaces += 1;
    return {
      data: this.workspaces.slice((page - 1) * 100, page * 100),
      pagination: { total: this.workspaces.length, page, page_size: 100 },
    };
  }

  async searchTasks(workspaceId: string, query: string, page: number, signal?: AbortSignal): Promise<DiscoveryRemoteTaskPage> {
    signal?.throwIfAborted();
    this.calls.searchTasks += 1;
    const matches = (this.tasks.get(workspaceId) ?? []).filter((task) =>
      task.task_id === query || task.title.toLowerCase().includes(query.toLowerCase()));
    const start = (page - 1) * 100;
    return {
      data: matches.slice(start, start + 100),
      pagination: { total: matches.length, page, page_size: 100 },
    };
  }

  async verifyTarget(
    target: { channelType: 2 | 5; channelId: string },
    signal?: AbortSignal,
  ): Promise<{ channelType: 2; channelId: string } | { channelType: 5; channelId: string; thread: DiscoveryRemoteThread }> {
    signal?.throwIfAborted();
    this.calls.verifyTarget += 1;
    if (this.deniedTargets.has(`${target.channelType}:${target.channelId}`)) {
      throw new Error("target read denied");
    }
    if (target.channelType === 2) {
      return { channelType: 2, channelId: target.channelId };
    }
    const match = [...this.threads.values()].flat().find((entry) => entry.channel_id === target.channelId);
    if (!match) throw new Error("target thread is not readable");
    return { channelType: 5, channelId: match.channel_id, thread: match };
  }

  async verifyWorkspace(workspaceId: string, signal?: AbortSignal): Promise<DiscoveryRemoteWorkspaceDetail> {
    signal?.throwIfAborted();
    this.calls.verifyWorkspace += 1;
    if (this.deniedWorkspaces.has(workspaceId)) throw new Error("workspace read denied");
    const detail = this.workspaceDetails.get(workspaceId);
    if (detail) return detail;
    const row = this.workspaces.find((entry) => entry.workspace_id === workspaceId);
    if (row !== undefined) {
      return { id: row.workspace_id, name: row.name, slug: row.slug };
    }
    return { id: workspaceId, name: workspaceId, slug: workspaceId };
  }
}

function storeReturning(...associations: DiscoveryAssociation[]): DiscoveryStorePort & { calls: number } {
  const store = {
    calls: 0,
    findDiscoveryAssociations: (_botUid: string, query: DiscoveryQuery): readonly DiscoveryAssociation[] => {
      store.calls += 1;
      return associations.filter((association) => {
        if (association.kind === "loop_route") {
          return query.workspaceId !== undefined
            && association.workspaceId === query.workspaceId
            && (query.taskId === undefined || association.taskId === query.taskId);
        }
        if (association.kind === "recorded_reference") {
          return query.referenceUrl !== undefined && association.url === query.referenceUrl;
        }
        if (query.repo !== undefined && association.projects.some((project) => project.repo === query.repo)) return true;
        return query.path !== undefined && (
          association.entryPath === query.path
          || association.workspaceRoot === query.path
          || association.projects.some((project) =>
            project.path === query.path || project.accessPath === query.path || project.accessTarget === query.path)
        );
      });
    },
  };
  return store;
}

function parse(output: string): Record<string, unknown> {
  return JSON.parse(output) as Record<string, unknown>;
}

function candidates(result: Record<string, unknown>): Record<string, unknown>[] {
  if (!Array.isArray(result.candidates)) throw new Error("missing candidates");
  return result.candidates as Record<string, unknown>[];
}

function candidateIds(result: Record<string, unknown>): string[] {
  return candidates(result).map((candidate) => String(candidate.channel_id));
}

function evidenceKinds(candidate: Record<string, unknown>): string[] {
  if (!Array.isArray(candidate.evidence)) return [];
  return (candidate.evidence as Record<string, unknown>[]).map((entry) => String(entry.kind));
}

function filteredCount(result: Record<string, unknown>): number {
  const filtered = result.filtered as { count?: unknown } | undefined;
  return typeof filtered?.count === "number" ? filtered.count : -1;
}

function source(result: Record<string, unknown>, name: string): Record<string, unknown> {
  const sources = result.sources as Record<string, Record<string, unknown>>;
  const entry = sources[name];
  if (!entry) throw new Error(`missing source ${name}`);
  return entry;
}

test("first search merges local and remote evidence on real channel identities", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Cross-group design")]);
  remote.threads.set("group-b", [thread("group-b", "topic-b", "Delivery")]);
  remote.threads.set("group-c", [thread("group-c", "topic-c", "Message match")]);
  remote.buckets.push(bucket({
    groupId: "group-c",
    shortId: "topic-c",
    name: "Message match",
    preview: [messageHit({ channelId: "group-c____topic-c", messageId: "message-1" })],
  }));
  const store = storeReturning(
    loopRoute({ channelId: "group-a____topic-a", groupId: "group-a", workspaceId: "workspace-a", taskId: "task-a" }),
    recordedReference("group-a____topic-a", "https://github.com/example/service/pull/42"),
    recordedReference("group-b____topic-b", "https://github.com/example/service/pull/42"),
  );
  const coordinator = createDiscoveryCoordinator({
    store,
    remote,
    now: () => 1_760_000_000_000,
  });

  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "service association",
    workspaceId: "workspace-a",
    loopTaskId: "task-a",
    referenceUrl: "https://github.com/example/service/pull/42",
  }, new AbortController().signal));

  expect(candidateIds(result).sort()).toEqual([
    "group-a____topic-a",
    "group-b____topic-b",
    "group-c____topic-c",
  ]);
  const design = candidates(result).find((candidate) => candidate.channel_id === "group-a____topic-a")!;
  expect(evidenceKinds(design).sort()).toEqual(["loop_route", "recorded_reference"]);
  expect(design).toMatchObject({
    channel_type: 5,
    channel_id: "group-a____topic-a",
    channelId: "group-a____topic-a",
    group_id: "group-a",
    parent_channel_id: "group-a",
    short_id: "topic-a",
    name: "Cross-group design",
  });
  expect(typeof design.sampled_at).toBe("string");
  const read = design.read as Record<string, unknown>;
  expect(read.channel).toMatchObject({ channel_type: 5, channel_id: "group-a____topic-a" });
  expect((read.octo_cli as string[]).some((entry) => entry.includes("octo-cli thread get group-a topic-a"))).toBe(true);
  expect((read.loop as string[]).some((entry) => entry.includes("task-a") && entry.includes("workspace-a"))).toBe(true);
  expect((read.references as string[]).some((entry) => entry.includes("https://github.com/example/service/pull/42"))).toBe(true);

  const delivery = candidates(result).find((candidate) => candidate.channel_id === "group-b____topic-b")!;
  expect(evidenceKinds(delivery)).toEqual(["recorded_reference"]);
  const remoteMatch = candidates(result).find((candidate) => candidate.channel_id === "group-c____topic-c")!;
  expect(evidenceKinds(remoteMatch)).toEqual(["message_match"]);
  expect(remote.calls.searchGroups).toBe(1);
  expect(remote.calls.verifyTarget).toBe(3);
  expect(store.calls).toBe(1);
  expect(source(result, "local_records").state).toBe("complete");
  expect(source(result, "group_search").state).not.toBe("complete");
  expect(source(result, "message_search").state).not.toBe("complete");
});

test("plain text searches report local structured lookup as skipped", async () => {
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote: new FakeRemote(),
    now: () => 1_760_000_000_000,
  });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-text-only",
    query: "payment rollout",
  }, new AbortController().signal));
  expect(source(result, "local_records")).toMatchObject({ state: "skipped", complete: false, candidates: 0 });
});

test("Loop task search maps only exact local routes after workspace authorization", async () => {
  const remote = new FakeRemote();
  remote.workspaces.push({ workspace_id: "workspace-a", name: "Design", slug: "design" });
  remote.workspaceDetails.set("workspace-a", { id: "workspace-a", name: "Design", slug: "design" });
  remote.tasks.set("workspace-a", [
    { task_id: "task-match", workspace_id: "workspace-a", title: "Payment rollout", status: "in_progress" },
    { task_id: "task-unrouted", workspace_id: "workspace-a", title: "Payment rollout", status: "todo" },
  ]);
  remote.threads.set("group-task", [thread("group-task", "payment", "Payment rollout")]);
  const store = storeReturning(loopRoute({
    channelId: "group-task____payment",
    groupId: "group-task",
    workspaceId: "workspace-a",
    taskId: "task-match",
  }));
  const coordinator = createDiscoveryCoordinator({
    store,
    remote,
    now: () => 1_760_000_000_000,
  });

  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-loop-search",
    query: "payment rollout",
  }, new AbortController().signal));
  expect(candidateIds(result)).toEqual(["group-task____payment"]);
  const candidate = candidates(result)[0]!;
  expect(evidenceKinds(candidate)).toEqual(["loop_route"]);
  expect(candidate.evidence).toContainEqual(expect.objectContaining({
    task: expect.objectContaining({ task_id: "task-match", title: "Payment rollout" }),
  }));
  expect(remote.calls.listWorkspaces).toBe(1);
  expect(remote.calls.verifyWorkspace).toBe(1);
  expect(remote.calls.searchTasks).toBe(1);
});

test("Loop task discovery follows returned pagination through an exact local route", async () => {
  const remote = new FakeRemote();
  remote.workspaces.push({ workspace_id: "workspace-a", name: "Design", slug: "design" });
  remote.workspaceDetails.set("workspace-a", { id: "workspace-a", name: "Design", slug: "design" });
  remote.tasks.set("workspace-a", Array.from({ length: 101 }, (_, index) => ({
    task_id: index === 100 ? "task-final" : `task-${String(index).padStart(3, "0")}`,
    workspace_id: "workspace-a",
    title: "Payment rollout",
    status: "todo",
  })));
  remote.threads.set("group-task", [thread("group-task", "payment", "Payment rollout")]);
  const store = storeReturning(loopRoute({
    channelId: "group-task____payment",
    groupId: "group-task",
    workspaceId: "workspace-a",
    taskId: "task-final",
  }));
  const coordinator = createDiscoveryCoordinator({ store, remote, now: () => 1_760_000_000_000 });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-loop-pagination",
    query: "payment rollout",
  }, new AbortController().signal));
  expect(candidateIds(result)).toEqual(["group-task____payment"]);
  expect(candidates(result)[0]!.evidence).toContainEqual(expect.objectContaining({
    task: expect.objectContaining({ task_id: "task-final" }),
  }));
  expect(remote.calls.searchTasks).toBe(2);
  expect(source(result, "loop_tasks")).toMatchObject({ requests: 2, state: "partial" });
});

test("first Loop task request failure is reported as failed without losing authorized routes", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-task", [thread("group-task", "payment", "Payment rollout")]);
  remote.workspaceDetails.set("workspace-a", { id: "workspace-a", name: "Design", slug: "design" });
  remote.searchTasks = async () => {
    throw Object.assign(new Error("private Loop response body"), { status: 429, code: "rate_limit" });
  };
  const store = storeReturning(loopRoute({
    channelId: "group-task____payment",
    groupId: "group-task",
    workspaceId: "workspace-a",
    taskId: "task-a",
  }));
  const coordinator = createDiscoveryCoordinator({ store, remote, now: () => 1_760_000_000_000 });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-loop-failure",
    query: "payment rollout",
    workspaceId: "workspace-a",
    loopTaskId: "task-a",
  }, new AbortController().signal));
  expect(candidateIds(result)).toEqual(["group-task____payment"]);
  expect(source(result, "loop_tasks")).toMatchObject({ state: "failed", requests: 1 });
  expect(source(result, "loop_tasks").detail).toBe("Error (HTTP 429, rate_limit)");
  expect(JSON.stringify(result)).not.toContain("private Loop response body");
});

test("denied targets and workspaces never reach output, evidence or the model", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-secret", [thread("group-secret", "secret-topic", "Secret task")]);
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Visible task")]);
  remote.deniedTargets.add("5:group-secret____secret-topic");
  remote.deniedWorkspaces.add("workspace-secret");
  remote.workspaceDetails.set("workspace-a", { id: "workspace-a", name: "Visible workspace", slug: "visible" });
  const store = storeReturning(
    loopRoute({ channelId: "group-secret____secret-topic", groupId: "group-secret", workspaceId: "workspace-a", taskId: "task-secret" }),
    recordedReference("group-secret____secret-topic", "https://github.com/private/secret/pull/7"),
    loopRoute({ channelId: "group-a____topic-a", groupId: "group-a", workspaceId: "workspace-secret", taskId: "task-hidden" }),
    recordedReference("group-a____topic-a", "https://github.com/example/service/pull/42"),
  );
  const judgeCalls: Array<readonly { id: string; text: string }[]> = [];
  const judge: DiscoveryJudge = async (_query, batch) => {
    judgeCalls.push(batch);
    return batch.map((entry) => ({ id: entry.id, relevant: true, score: 0.5 }));
  };
  const coordinator = createDiscoveryCoordinator({ store, remote, judge, now: () => 1_760_000_000_000 });

  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "secret task",
    workspaceId: "workspace-a",
    loopTaskId: "task-secret",
    referenceUrl: "https://github.com/private/secret/pull/7",
  }, new AbortController().signal));

  const text = JSON.stringify(result);
  expect(text).not.toContain("group-secret");
  expect(text).not.toContain("secret-topic");
  expect(text).not.toContain("Secret task");
  expect(text).not.toContain("task-secret");
  expect(text).not.toContain("workspace-secret");
  expect(candidateIds(result)).not.toContain("group-secret____secret-topic");

  const workspaceResult = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-workspace",
    query: "service task",
    workspaceId: "workspace-secret",
    referenceUrl: "https://github.com/example/service/pull/42",
  }, new AbortController().signal));
  const workspaceText = JSON.stringify(workspaceResult);
  expect(workspaceText).not.toContain("workspace-secret");
  expect(workspaceText).not.toContain("task-hidden");
  const visible = candidates(workspaceResult).find((candidate) => candidate.channel_id === "group-a____topic-a")!;
  expect(evidenceKinds(visible)).toEqual(["recorded_reference"]);
  expect(judgeCalls).toHaveLength(0);
  expect(source(result, "permissions").state).toBe("restricted");
  expect(source(workspaceResult, "permissions").state).toBe("restricted");

});

test("the model filters only inference candidates and keeps a separate filtered page", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Explicit task")]);
  remote.threads.set("group-b", [thread("group-b", "topic-b", "Relevant inference")]);
  remote.threads.set("group-c", [thread("group-c", "topic-c", "Filtered inference")]);
  remote.buckets.push(
    bucket({ groupId: "group-b", shortId: "topic-b", name: "Relevant inference", preview: [messageHit({ channelId: "group-b____topic-b", messageId: "m-b" })] }),
    bucket({ groupId: "group-c", shortId: "topic-c", name: "Filtered inference", preview: [messageHit({ channelId: "group-c____topic-c", messageId: "m-c" })] }),
  );
  const store = storeReturning(recordedReference("group-a____topic-a", "https://github.com/example/service/pull/42"));
  const batches: string[][] = [];
  const judge: DiscoveryJudge = async (_query, batch) => {
    batches.push(batch.map((entry) => entry.id));
    return [
      { id: "5:group-b____topic-b", relevant: true, score: 0.92 },
      { id: "5:group-c____topic-c", relevant: false, score: 0.11 },
    ];
  };
  const coordinator = createDiscoveryCoordinator({ store, remote, judge, now: () => 1_760_000_000_000 });
  const searched = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "service task",
    referenceUrl: "https://github.com/example/service/pull/42",
  }, new AbortController().signal));

  expect(candidateIds(searched).sort()).toEqual(["group-a____topic-a", "group-b____topic-b"]);
  expect(filteredCount(searched)).toBe(1);
  expect(source(searched, "permissions").state).toBe("complete");
  const model = searched.model as Record<string, unknown>;
  expect(model.status).toBe("complete");
  expect(model.evaluated).toBe(2);
  expect(batches).toHaveLength(1);
  expect(batches[0]!.sort()).toEqual(["5:group-b____topic-b", "5:group-c____topic-c"]);
  // Explicit evidence is never sent to the model and never filtered.
  expect(batches[0]).not.toContain("5:group-a____topic-a");
  expect(searched.query_id).toBeString();

  const filteredPage = parse(await coordinator.page({
    botUid: BOT,
    taskId: "runtime-task",
    queryId: String(searched.query_id),
    collection: "filtered",
    position: 1,
  }, new AbortController().signal));
  expect(candidateIds(filteredPage)).toEqual(["group-c____topic-c"]);
  expect(source(filteredPage, "permissions").state).toMatch(/complete|restricted/u);
});

test("model failure and an unconfigured model keep every authorized candidate", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Task A")]);
  remote.buckets.push(bucket({
    groupId: "group-a",
    shortId: "topic-a",
    name: "Task A",
    preview: [messageHit({ channelId: "group-a____topic-a", messageId: "m-a" })],
  }));

  const failing: DiscoveryJudge = async () => {
    throw new Error("model unavailable");
  };
  const failingCoordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote,
    judge: failing,
    now: () => 1_760_000_000_000,
  });
  const failed = parse(await failingCoordinator.search({
    botUid: BOT,
    taskId: "runtime-failing",
    query: "service task",
  }, new AbortController().signal));
  expect(candidateIds(failed)).toEqual(["group-a____topic-a"]);
  expect(filteredCount(failed)).toBe(0);
  expect((failed.model as Record<string, unknown>).status).toBe("failed");
  expect(JSON.stringify(failed)).not.toContain("model unavailable");

  const fallback = createDiscoveryCoordinator({
    store: storeReturning(),
    remote,
    now: () => 1_760_000_000_000,
  });
  const unfiltered = parse(await fallback.search({
    botUid: BOT,
    taskId: "runtime-fallback",
    query: "service task",
  }, new AbortController().signal));
  expect(candidateIds(unfiltered)).toEqual(["group-a____topic-a"]);
  expect((unfiltered.model as Record<string, unknown>).status).toBe("skipped");
});

test("pages revalidate permission and Loop workspace access without querying sources again", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Loop candidate")]);
  remote.threads.set("group-b", [thread("group-b", "topic-b", "Revoked candidate")]);
  remote.workspaceDetails.set("workspace-a", { id: "workspace-a", name: "Workspace A", slug: "workspace-a" });
  const store = storeReturning(
    loopRoute({ channelId: "group-a____topic-a", groupId: "group-a", workspaceId: "workspace-a", taskId: "task-a" }),
    recordedReference("group-a____topic-a", "https://github.com/example/service/pull/42"),
    recordedReference("group-b____topic-b", "https://github.com/example/service/pull/42"),
  );
  const coordinator = createDiscoveryCoordinator({ store, remote, now: () => 1_760_000_000_000 });
  const searched = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "service",
    workspaceId: "workspace-a",
    loopTaskId: "task-a",
    referenceUrl: "https://github.com/example/service/pull/42",
  }, new AbortController().signal));
  expect(candidateIds(searched).sort()).toEqual(["group-a____topic-a", "group-b____topic-b"]);

  const sourceCallsBeforePage = {
    groups: remote.calls.searchGroups,
    messages: remote.calls.searchMessages,
    threads: remote.calls.listThreads,
  };
  remote.deniedTargets.add("5:group-b____topic-b");
  remote.deniedWorkspaces.add("workspace-a");

  const page = parse(await coordinator.page({
    botUid: BOT,
    taskId: "runtime-task",
    queryId: String(searched.query_id),
    collection: "candidates",
    position: 1,
  }, new AbortController().signal));
  expect(candidateIds(page)).toEqual(["group-a____topic-a"]);
  expect(remote.calls.searchGroups).toBe(sourceCallsBeforePage.groups);
  expect(remote.calls.searchMessages).toBe(sourceCallsBeforePage.messages);
  expect(remote.calls.listThreads).toBe(sourceCallsBeforePage.threads);
  expect(remote.calls.verifyTarget).toBeGreaterThan(3);
  expect(remote.calls.verifyWorkspace).toBeGreaterThan(1);

  // Target survives its recorded reference, but the revoked Loop evidence is removed.
  const visible = candidates(page)[0]!;
  expect(evidenceKinds(visible)).toEqual(["recorded_reference"]);
  expect(JSON.stringify(page)).not.toContain("task-a");
  expect(source(page, "permissions").state).toBe("restricted");
  expect(source(page, "local_records").state).not.toBe("complete");
});

test("candidate and filtered collections page independently and deterministically", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-x", []);
  const buckets: DiscoveryRemoteGroupBucket[] = [];
  for (let index = 0; index < 30; index += 1) {
    const shortId = `topic-${String(index).padStart(2, "0")}`;
    remote.threads.set("group-x", [...(remote.threads.get("group-x") ?? []), thread("group-x", shortId, `Task ${index}`)]);
    buckets.push(bucket({
      groupId: "group-x",
      shortId,
      name: `Task ${index}`,
      preview: [messageHit({ channelId: `group-x____${shortId}`, messageId: `m-${index}` })],
    }));
  }
  remote.buckets.push(...buckets);
  const judge: DiscoveryJudge = async (_query, batch) =>
    batch.map((entry, index) => ({
      id: entry.id,
      relevant: index % 6 !== 0,
      score: 1 - index / 100,
    }));
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote,
    judge,
    now: () => 1_760_000_000_000,
  });
  const searched = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "task",
  }, new AbortController().signal));
  expect(candidates(searched)).toHaveLength(20);
  expect((searched.page as Record<string, unknown>).has_more).toBe(true);
  expect((searched.page as Record<string, unknown>).next_position).toBe(2);
  expect(filteredCount(searched)).toBe(5);
  expect((searched.filtered as Record<string, unknown>).has_more).toBe(false);

  const second = parse(await coordinator.page({
    botUid: BOT,
    taskId: "runtime-task",
    queryId: String(searched.query_id),
    collection: "candidates",
    position: 2,
  }, new AbortController().signal));
  expect(candidates(second)).toHaveLength(5);
  expect((second.page as Record<string, unknown>).has_more).toBe(false);
  expect(second.page as Record<string, unknown>).toMatchObject({ position: 2, collection: "candidates" });

  const filteredFirst = parse(await coordinator.page({
    botUid: BOT,
    taskId: "runtime-task",
    queryId: String(searched.query_id),
    collection: "filtered",
    position: 1,
  }, new AbortController().signal));
  expect(candidates(filteredFirst)).toHaveLength(5);
  expect((filteredFirst.page as Record<string, unknown>).collection).toBe("filtered");
});

test("remote source budgets, keyword bounds and candidate caps are reported", async () => {
  const remote = new FakeRemote();
  const noPreviewBuckets: DiscoveryRemoteGroupBucket[] = [];
  for (let index = 0; index < 12; index += 1) {
    const shortId = `budget-${index}`;
    remote.threads.set("group-budget", [...(remote.threads.get("group-budget") ?? []), thread("group-budget", shortId, `Budget ${index}`)]);
    remote.messageHits.set(`group-budget____${shortId}`, [
      messageHit({ channelId: `group-budget____${shortId}`, messageId: `hit-${index}-1` }),
      messageHit({ channelId: `group-budget____${shortId}`, messageId: `hit-${index}-2` }),
      messageHit({ channelId: `group-budget____${shortId}`, messageId: `hit-${index}-3` }),
    ]);
    noPreviewBuckets.push(bucket({ groupId: "group-budget", shortId, name: `Budget ${index}` }));
  }
  remote.buckets.push(...noPreviewBuckets);
  const longQuery = `${"a".repeat(70)} service`;
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote,
    now: () => 1_760_000_000_000,
  });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-budget",
    query: longQuery,
  }, new AbortController().signal));
  expect(remote.calls.searchMessages).toBe(10);
  expect(candidates(result)).toHaveLength(10);
  expect(Array.from(remote.lastSearchQuery)).toHaveLength(64);
  expect(source(result, "message_search").state).toBe("budget_exhausted");
  expect(source(result, "message_search").requests).toBe(10);
  expect(source(result, "group_search").keyword_truncated).toBe(true);
  expect((result.budgets as Record<string, unknown>).remote_requests_per_source).toBe(10);
  expect((result.budgets as Record<string, unknown>).candidates_per_page).toBe(20);

  const cappedRemote = new FakeRemote();
  for (let groupIndex = 0; groupIndex < 4; groupIndex += 1) {
    const groupId = `group-${groupIndex}`;
    const threads: DiscoveryRemoteThread[] = [];
    for (let index = 0; index < 60; index += 1) {
      threads.push(thread(groupId, `service-${index}`, `Service design ${index}`));
    }
    cappedRemote.threads.set(groupId, threads);
    cappedRemote.groups.push(group(groupId, `Group ${groupIndex}`));
  }
  const capped = createDiscoveryCoordinator({
    store: storeReturning(),
    remote: cappedRemote,
    now: () => 1_760_000_000_000,
  });
  const cappedResult = parse(await capped.search({
    botUid: BOT,
    taskId: "runtime-capped",
    query: "service",
  }, new AbortController().signal));
  expect((cappedResult.budgets as Record<string, unknown>).collected_candidates).toBe(200);
  expect(source(cappedResult, "group_threads").state).toBe("truncated");
  expect(cappedRemote.calls.listGroups).toBe(1);
  expect(cappedRemote.calls.listThreads).toBe(4);
  expect(source(cappedResult, "group_threads").includes_archived).toBe(false);
});

test("group-thread discovery follows active-thread pagination and reports its bound", async () => {
  const remote = new FakeRemote();
  remote.groups.push(group("group-pages", "Unrelated"));
  remote.threads.set("group-pages", Array.from({ length: 110 }, (_, index) =>
    thread("group-pages", `service-${index}`, `Service task ${index}`)));
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote,
    now: () => 1_760_000_000_000,
  });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-thread-pagination",
    query: "service",
  }, new AbortController().signal));
  expect(candidates(result)).toHaveLength(20);
  expect((result.page as Record<string, unknown>).has_more).toBe(true);
  expect(remote.calls.listThreads).toBe(2);
  expect(source(result, "group_threads")).toMatchObject({
    requests: 3,
    candidates: 110,
    state: "partial",
    includes_archived: false,
  });
});

test("query cache is bound to bot and task, expires, and releases at task end", async () => {
  let now = 1_760_000_000_000;
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote: new FakeRemote(),
    now: () => now,
    limits: { queriesPerTask: 1, queriesPerBot: 2, snapshotTtlMs: 60_000 },
  });
  const search = async (botUid: string, taskId: string): Promise<Record<string, unknown>> =>
    parse(await coordinator.search({ botUid, taskId, query: "service" }, new AbortController().signal));
  const first = await search(BOT, "task-1");
  const second = await search(OTHER_BOT, "task-2");
  await expect(search(BOT, "task-1")).rejects.toThrow("上限");
  const third = await search(OTHER_BOT, "task-3");
  await expect(search(OTHER_BOT, "task-4")).rejects.toThrow("上限");

  const pageOf = async (result: Record<string, unknown>, botUid: string, taskId: string) =>
    parse(await coordinator.page({
      botUid,
      taskId,
      queryId: String(result.query_id),
      collection: "candidates",
      position: 1,
    }, new AbortController().signal));

  await expect(pageOf(first, OTHER_BOT, "task-1")).rejects.toThrow("失效");
  await expect(pageOf(first, BOT, "task-2")).rejects.toThrow("失效");
  expect((await pageOf(first, BOT, "task-1")).query_id).toBe(first.query_id);
  expect((await pageOf(second, OTHER_BOT, "task-2")).query_id).toBe(second.query_id);
  coordinator.releaseTask("task-2");
  await expect(pageOf(second, OTHER_BOT, "task-2")).rejects.toThrow("失效");
  expect((await pageOf(third, OTHER_BOT, "task-3")).query_id).toBe(third.query_id);
  now += 120_000;
  await expect(pageOf(first, BOT, "task-1")).rejects.toThrow("失效");
  await expect(pageOf(third, OTHER_BOT, "task-3")).rejects.toThrow("失效");
  coordinator.stop();
  await expect(pageOf(third, OTHER_BOT, "task-3")).rejects.toThrow("失效");
});

test("first pages abort without publishing and page cancellation keeps the query", async () => {
  const coordinator = createDiscoveryCoordinator({
    store: storeReturning(),
    remote: new FakeRemote(),
    now: () => 1_760_000_000_000,
    limits: { queriesPerTask: 1 },
  });
  const aborted = new AbortController();
  aborted.abort(new Error("cancelled"));
  await expect(coordinator.search({
    botUid: BOT,
    taskId: "task-abort",
    query: "service",
  }, aborted.signal)).rejects.toThrow();
  // A cancelled first page must release its in-flight slot, so the next search may publish.
  const published = parse(await coordinator.search({
    botUid: BOT,
    taskId: "task-abort",
    query: "service",
  }, new AbortController().signal));
  const pageAbort = new AbortController();
  pageAbort.abort(new Error("page cancelled"));
  await expect(coordinator.page({
    botUid: BOT,
    taskId: "task-abort",
    queryId: String(published.query_id),
    collection: "candidates",
    position: 1,
  }, pageAbort.signal)).rejects.toThrow();
  const afterCancel = parse(await coordinator.page({
    botUid: BOT,
    taskId: "task-abort",
    queryId: String(published.query_id),
    collection: "candidates",
    position: 1,
  }, new AbortController().signal));
  expect(afterCancel.query_id).toBe(published.query_id);
});

test("stopping the coordinator aborts in-flight discovery and prevents snapshot publication", async () => {
  const remote = new FakeRemote();
  const started = Promise.withResolvers<void>();
  const response = Promise.withResolvers<DiscoveryRemoteGroupSearchPage>();
  remote.searchGroups = (_query, signal) => {
    started.resolve();
    signal?.addEventListener("abort", () => response.reject(signal.reason), { once: true });
    if (signal?.aborted) response.reject(signal.reason);
    return response.promise;
  };
  const coordinator = createDiscoveryCoordinator({ store: storeReturning(), remote });
  const pending = coordinator.search({
    botUid: BOT,
    taskId: "task-stop",
    query: "service",
  }, new AbortController().signal);
  await started.promise;
  coordinator.stop();
  await expect(pending).rejects.toThrow();
  await expect(coordinator.search({
    botUid: BOT,
    taskId: "task-after-stop",
    query: "service",
  }, new AbortController().signal)).rejects.toThrow("失效");
});

test("task release aborts its in-flight query before it can publish", async () => {
  const remote = new FakeRemote();
  const started = Promise.withResolvers<void>();
  const response = Promise.withResolvers<DiscoveryRemoteGroupSearchPage>();
  remote.searchGroups = (_query, signal) => {
    started.resolve();
    signal?.addEventListener("abort", () => response.reject(signal.reason), { once: true });
    if (signal?.aborted) response.reject(signal.reason);
    return response.promise;
  };
  const coordinator = createDiscoveryCoordinator({ store: storeReturning(), remote });
  const pending = coordinator.search({
    botUid: BOT,
    taskId: "task-release-active",
    query: "service",
  }, new AbortController().signal);
  await started.promise;
  coordinator.releaseTask("task-release-active");
  await expect(pending).rejects.toThrow();
  await expect(coordinator.page({
    botUid: BOT,
    taskId: "task-release-active",
    queryId: "unpublished",
  }, new AbortController().signal)).rejects.toThrow("失效");
});

test("snapshot capacity errors never publish a partial snapshot", async () => {
  const remote = new FakeRemote();
  remote.threads.set("group-a", [thread("group-a", "topic-a", "Task A")]);
  const store = storeReturning(loopRoute({
    channelId: "group-a____topic-a",
    groupId: "group-a",
    workspaceId: "workspace-a",
    taskId: "task-a",
  }));
  const coordinator = createDiscoveryCoordinator({
    store,
    remote,
    now: () => 1_760_000_000_000,
    limits: { snapshotMaxBytes: 64 },
  });
  await expect(coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "service",
    workspaceId: "workspace-a",
    loopTaskId: "task-a",
  }, new AbortController().signal)).rejects.toThrow("容量");
});

test("without a remote port, local records are never returned unverified", async () => {
  const store = storeReturning(loopRoute({
    channelId: "group-a____topic-a",
    groupId: "group-a",
    workspaceId: "workspace-a",
    taskId: "task-a",
  }));
  const coordinator = createDiscoveryCoordinator({ store, now: () => 1_760_000_000_000 });
  const result = parse(await coordinator.search({
    botUid: BOT,
    taskId: "runtime-task",
    query: "service",
    workspaceId: "workspace-a",
    loopTaskId: "task-a",
  }, new AbortController().signal));
  expect(candidateIds(result)).toEqual([]);
  expect(source(result, "permissions").state).toBe("unavailable");
  expect(JSON.stringify(result)).not.toContain("group-a____topic-a");
});
