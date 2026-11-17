import { expect, test } from "bun:test";
import type { GroupInfo, HostTool, ThreadSummary } from "../src/contracts.ts";
import type { LoopTask, LoopWorkspace } from "../src/loop/types.ts";
import type {
  DiscoveryRemote,
  DiscoveryTaskPage,
  DiscoveryThread,
  DiscoveryThreadPage,
  DiscoveryWorkspacePage,
} from "../src/discovery/remote.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const GROUP_A = "group-a";
const GROUP_B = "group-b";
const GROUP_DENIED = "group-denied";
const THREAD_A = `${GROUP_A}____topic-a`;
const THREAD_B = `${GROUP_B}____topic-b`;
const THREAD_DENIED = `${GROUP_DENIED}____secret-topic`;
const THREAD_OTHER_BOT = `${GROUP_A}____topic-other-bot`;
const PR_URL = "https://github.com/example/service/pull/42";
const SECRET_PR_URL = "https://github.com/private/secret/pull/7";
const WORKSPACE_A: LoopWorkspace = { workspaceId: "workspace-a", name: "Design workspace", slug: "design" };
const WORKSPACE_DENIED: LoopWorkspace = { workspaceId: "workspace-secret", name: "Secret workspace", slug: "secret" };
const WORKSPACE_OTHER: LoopWorkspace = { workspaceId: "workspace-other", name: "Other bot workspace", slug: "other-bot" };

function loopTask(taskId: string, title: string): LoopTask {
  return {
    taskId,
    title,
    description: null,
    status: "todo",
    priority: "normal",
    assigneeType: null,
    assigneeId: null,
    assigneeOctoUid: null,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-17T00:00:00.000Z",
    updatedAt: "2026-11-17T00:00:00.000Z",
  };
}

function threadRecord(groupId: string, shortId: string, name: string): ThreadSummary {
  return {
    shortId,
    groupId,
    channelId: `${groupId}____${shortId}`,
    name,
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-17T00:00:00.000Z",
    updatedAt: "2026-11-17T00:00:00.000Z",
  };
}

function groupInfo(groupId: string, name: string): GroupInfo {
  return {
    groupId,
    name,
    notice: "",
    creator: "owner-1",
    status: 1,
    createdAt: "2026-11-17T00:00:00.000Z",
  };
}

/** Remote reads reject for targets the bot cannot currently access; no local record bypasses it. */
class ReadDenialTransport extends FakeTransport {
  readonly deniedGroups = new Set<string>();
  readonly deniedReads: string[] = [];

  private deny(groupId: string): void {
    if (!this.deniedGroups.has(groupId)) return;
    this.deniedReads.push(groupId);
    throw new Error("remote target read denied");
  }

  override async getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo> {
    this.deny(groupId);
    return super.getGroupInfo(groupId, signal);
  }

  override async getThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<ThreadSummary> {
    this.deny(groupId);
    return super.getThread(groupId, shortId, signal);
  }

  override async listThreads(groupId: string, signal?: AbortSignal): Promise<ThreadSummary[]> {
    this.deny(groupId);
    return super.listThreads(groupId, signal);
  }
}

function discoveryFixture(transport: ReadDenialTransport): DiscoveryRemote {
  const threadFromTransport = async (groupId: string, shortId: string): Promise<DiscoveryThread> => {
    const thread = await transport.getThread(groupId, shortId);
    const group = await transport.getGroupInfo(groupId);
    return {
      short_id: thread.shortId,
      group_no: thread.groupId,
      channel_id: thread.channelId,
      name: thread.name,
      status: thread.status,
      group_name: group.name,
    };
  };
  return {
    async listGroups() {
      return [
        { group_no: GROUP_A, name: "Design group" },
        { group_no: GROUP_B, name: "Delivery group" },
        { group_no: GROUP_DENIED, name: "Secret group" },
      ];
    },
    async listThreads(groupId, page): Promise<DiscoveryThreadPage> {
      const rows = (await transport.listThreads(groupId)).filter((thread) => thread.status === 1);
      const start = (page - 1) * 100;
      return {
        count: rows.length,
        list: await Promise.all(rows.slice(start, start + 100).map((thread) =>
          threadFromTransport(thread.groupId, thread.shortId))),
      };
    },
    async searchGroups() {
      return {
        data: { sequence: 1, query_id: "fixture-search", total_groups: 0, total_groups_approx: true, groups: [] },
        pagination: { has_more: false, next_cursor: "" },
      };
    },
    async searchMessages() {
      return { data: [], pagination: { has_more: false, next_cursor: "" } };
    },
    async listWorkspaces(page): Promise<DiscoveryWorkspacePage> {
      return {
        data: page === 1
          ? [{ workspace_id: WORKSPACE_A.workspaceId, name: WORKSPACE_A.name, slug: WORKSPACE_A.slug }]
          : [],
        pagination: { total: 1, page, page_size: 100 },
      };
    },
    async searchTasks(_workspaceId, _query, page): Promise<DiscoveryTaskPage> {
      return { data: [], pagination: { total: 0, page, page_size: 100 } };
    },
    async verifyTarget(target) {
      if (target.channelType === 2) {
        await transport.listThreads(target.channelId);
        return { channelType: 2, channelId: target.channelId };
      }
      const separator = target.channelId.indexOf("____");
      if (separator < 1) throw new Error("invalid thread identity");
      const groupId = target.channelId.slice(0, separator);
      const shortId = target.channelId.slice(separator + 4);
      return {
        channelType: 5,
        channelId: target.channelId,
        thread: await threadFromTransport(groupId, shortId),
      };
    },
    async verifyWorkspace(workspaceId) {
      if (workspaceId === WORKSPACE_OTHER.workspaceId) throw new Error("workspace access denied");
      const workspace = workspaceId === WORKSPACE_A.workspaceId ? WORKSPACE_A : WORKSPACE_DENIED;
      return { id: workspaceId, name: workspace.name, slug: workspace.slug };
    },
  };
}

async function waitForPrompts(harness: CoreHarness, count: number): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (harness.workers.prompts.length >= count) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`timed out waiting for ${count} worker prompts`);
}

function discoveryTool(promptContext: { tools?: readonly HostTool[] } | undefined): HostTool {
  const tool = promptContext?.tools?.find(({ name }) => name === "octo_discover");
  if (!tool) throw new Error("octo_discover host tool is missing");
  return tool;
}

function objectFrom(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} is not an object`);
  }
  return Object.fromEntries(Object.entries(value));
}

function candidatesFrom(value: unknown): Record<string, unknown>[] {
  const result = objectFrom(value, "octo_discover result");
  if (!Array.isArray(result.candidates)) {
    throw new Error("octo_discover result is missing the candidates page");
  }
  return result.candidates.map((candidate) => objectFrom(candidate, "discovery candidate"));
}

function channelIdOf(candidate: Record<string, unknown>): string {
  const channelId = candidate.channel_id ?? candidate.channelId;
  if (typeof channelId !== "string" || channelId.length === 0) {
    throw new Error("discovery candidate is missing channel_id");
  }
  return channelId;
}

function evidenceKinds(candidate: Record<string, unknown>): string[] {
  if (!Array.isArray(candidate.evidence)) return [];
  return candidate.evidence.flatMap((entry) => {
    if (typeof entry === "string") return [entry];
    const evidence = objectFrom(entry, "discovery evidence");
    const kind = evidence.kind ?? evidence.source ?? evidence.type;
    return typeof kind === "string" ? [kind] : [];
  });
}

/** A candidate must carry a usable further-read entry, not only an opaque identifier. */
function readEntry(candidate: Record<string, unknown>): unknown {
  for (const key of ["read", "read_entry", "read_entries", "read_surface", "read_hint", "octo_cli"]) {
    const value = candidate[key];
    if (value !== undefined && value !== null && JSON.stringify(value).length > 2) return value;
  }
  throw new Error(`discovery candidate has no read entry: ${JSON.stringify(candidate)}`);
}

async function search(discovery: HostTool, args: Record<string, unknown>): Promise<unknown> {
  const output = await discovery.execute(
    { action: "search", query: "service association", ...args },
    new AbortController().signal,
  );
  if (typeof output !== "string") throw new Error("octo_discover search did not return JSON text");
  return JSON.parse(output) as unknown;
}

test("every chat, including private chats, advertises octo_discover", async () => {
  const advertised: Array<{ chat: string; tools: readonly HostTool[] }> = [];
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A, `group-fresh`],
    prompt: async ({ promptContext }) => {
      advertised.push({
        chat: String(advertised.length),
        tools: promptContext?.tools ?? [],
      });
    },
  });
  const message = () => inbound({
    senderId: "owner-1",
    mentionedUserIds: [harness.bot.uid],
    text: "discover target work",
  });
  try {
    await harness.service.submitInbound(message());
    await waitForPrompts(harness, 1);
    await harness.service.submitInbound({
      ...message(),
      channelId: GROUP_A,
      channelType: 2 as const,
      messageId: "group-discovery-tools",
    });
    await waitForPrompts(harness, 2);
    await harness.service.submitInbound({
      ...message(),
      channelId: `group-fresh____tools`,
      channelType: 5 as const,
      messageId: "thread-discovery-tools",
    });
    await waitForPrompts(harness, 3);
    expect(advertised).toHaveLength(3);
    for (const { chat, tools } of advertised) {
      expect({ chat, hasDiscover: tools.some(({ name }) => name === "octo_discover") })
        .toEqual({ chat, hasDiscover: true });
      const parameters = objectFrom(discoveryTool({ tools }).parameters, "octo_discover parameters");
      expect(parameters.type).toBe("object");
      // Codex rejects these keywords at the function-parameters root.
      for (const keyword of ["oneOf", "anyOf", "allOf", "enum", "const", "not"]) {
        expect(parameters).not.toHaveProperty(keyword);
      }
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("private discovery finds cross-group local associations and withholds unreadable targets", async () => {
  const transport = new ReadDenialTransport({ bot: { uid: "bot-discovery", name: "Discovery Bot" } });
  transport.setGroupInfo(GROUP_A, groupInfo(GROUP_A, "Design group"));
  transport.setGroupInfo(GROUP_B, groupInfo(GROUP_B, "Delivery group"));
  transport.setGroupInfo(GROUP_DENIED, groupInfo(GROUP_DENIED, "Secret group"));
  transport.setThreads(GROUP_A, [
    threadRecord(GROUP_A, "topic-a", "Cross-group design"),
    threadRecord(GROUP_A, "topic-other-bot", "Other bot task"),
  ]);
  transport.setThreads(GROUP_B, [threadRecord(GROUP_B, "topic-b", "Cross-group delivery")]);
  transport.setThreads(GROUP_DENIED, [threadRecord(GROUP_DENIED, "secret-topic", "Secret task")]);
  transport.deniedGroups.add(GROUP_DENIED);

  const harness = await createCoreHarness({
    transport,
    discovery: discoveryFixture(transport),
    decisions: { enabled: false },
    prompt: async ({ promptContext }) => {
      const discovery = discoveryTool(promptContext);
      const parameters = objectFrom(discovery.parameters, "octo_discover parameters");
      expect(parameters.required).toContain("action");
      expect(parameters.required).not.toContain("query");
      const signal = new AbortController().signal;
      await expect(discovery.execute({ action: "search" }, signal))
        .rejects.toThrow("query must be a non-empty string");
      await expect(discovery.execute({ action: "page" }, signal))
        .rejects.toThrow("query_id must be a non-empty string");
      const properties = objectFrom(parameters.properties, "octo_discover properties");
      expect(objectFrom(properties.action, "action schema").enum).toContain("search");
      expect(objectFrom(properties.query, "query schema").type).toBe("string");

      const found = await search(discovery, {
        task_id: "loop-task-a",
        workspace_id: WORKSPACE_A.workspaceId,
        reference_url: PR_URL,
      });
      const candidates = candidatesFrom(found);
      const ids = candidates.map(channelIdOf);
      const design = candidates.find((candidate) => channelIdOf(candidate) === THREAD_A);
      const delivery = candidates.find((candidate) => channelIdOf(candidate) === THREAD_B);
      expect(ids).toContain(THREAD_A);
      expect(ids).toContain(THREAD_B);
      expect(ids).not.toContain(THREAD_OTHER_BOT);
      expect(evidenceKinds(design ?? {})).toContain("loop_route");
      expect(evidenceKinds(delivery ?? {})).toContain("recorded_reference");
      readEntry(design ?? {});
      readEntry(delivery ?? {});

      const firstSearch = objectFrom(found, "first discovery response");
      const nextPage = await discovery.execute({
        action: "page",
        query_id: firstSearch.query_id,
        collection: "filtered",
        position: 1,
      }, new AbortController().signal);
      expect(typeof nextPage).toBe("string");

      const denied = await search(discovery, {
        workspace_id: WORKSPACE_DENIED.workspaceId,
        reference_url: SECRET_PR_URL,
      });
      const deniedIds = candidatesFrom(denied).map(channelIdOf);
      const deniedText = JSON.stringify(denied);
      expect(deniedIds).not.toContain(THREAD_DENIED);
      expect(deniedText).not.toContain(GROUP_DENIED);
      expect(deniedText).not.toContain("secret-topic");
      expect(deniedText).not.toContain("Secret task");

      const foreign = await search(discovery, {
        task_id: "loop-task-other",
        workspace_id: WORKSPACE_OTHER.workspaceId,
      });
      expect(candidatesFrom(foreign).map(channelIdOf)).not.toContain(THREAD_OTHER_BOT);
    },
  });
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    const botUid = harness.bot.uid;
    store.bindLoopWorkspace(botUid, GROUP_A, WORKSPACE_A, [loopTask("loop-task-a", "Cross-group design")]);
    store.bindLoopTask(
      { botUid, groupId: GROUP_A, workspaceId: WORKSPACE_A.workspaceId, taskId: "loop-task-a" },
      THREAD_A,
    );
    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_B }, [{ kind: "pr", url: PR_URL }]);

    store.bindLoopWorkspace(botUid, GROUP_DENIED, WORKSPACE_DENIED, [loopTask("loop-task-secret", "Secret task")]);
    store.bindLoopTask(
      { botUid, groupId: GROUP_DENIED, workspaceId: WORKSPACE_DENIED.workspaceId, taskId: "loop-task-secret" },
      THREAD_DENIED,
    );
    store.addThreadReferences(botUid, { channelType: 5, channelId: THREAD_DENIED }, [{ kind: "pr", url: SECRET_PR_URL }]);

    store.bindLoopWorkspace("other-bot", GROUP_A, WORKSPACE_OTHER, [loopTask("loop-task-other", "Other bot task")]);
    store.bindLoopTask(
      { botUid: "other-bot", groupId: GROUP_A, workspaceId: WORKSPACE_OTHER.workspaceId, taskId: "loop-task-other" },
      THREAD_OTHER_BOT,
    );
    store.close();
    store = undefined;

    await harness.service.submitInbound(inbound({
      channelId: "dm-discovery",
      channelType: 1,
      messageId: "dm-discovery-query",
      senderId: "owner-1",
      text: "find the service association",
    }));
    await waitForPrompts(harness, 1);
    await waitForStatus(
      harness.service,
      { channelType: 1, channelId: "dm-discovery" },
      (status) => status.state === "completed",
    );
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});
