import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { LoopTask, LoopTaskKey, LoopTaskRouteEndingReason, LoopWorkspace } from "../src/loop/types.ts";
import type { ThreadWorkspaceProjectRecord, ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import type { ThreadReference } from "../src/references/types.ts";

/**
 * Read-only discovery surface contract under test:
 *
 *   ChannelStore.findDiscoveryAssociations(botUid, query) -> DiscoveryAssociation[]
 *
 * The store method is intentionally absent until the discovery implementation lands; the cast
 * below keeps this file runnable so the regression fails on the missing method, not on test syntax.
 */
interface DiscoveryQuery {
  workspaceId?: string;
  taskId?: string;
  referenceUrl?: string;
  repo?: string;
  path?: string;
}

interface LoopRouteAssociation {
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

interface RecordedReferenceAssociation {
  kind: "recorded_reference";
  channelType: 5;
  channelId: string;
  referenceKind: "pr" | "mr";
  url: string;
}

interface WorkspaceAssociation {
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

type DiscoveryAssociation = LoopRouteAssociation | RecordedReferenceAssociation | WorkspaceAssociation;

function findAssociations(
  store: ChannelStore,
  botUid: string,
  query: DiscoveryQuery,
): DiscoveryAssociation[] {
  const discovery = store as unknown as {
    findDiscoveryAssociations(botUid: string, query: DiscoveryQuery): DiscoveryAssociation[];
  };
  return discovery.findDiscoveryAssociations(botUid, query);
}

function loopRouteRows(rows: DiscoveryAssociation[]): LoopRouteAssociation[] {
  return rows.filter((row): row is LoopRouteAssociation => row.kind === "loop_route");
}

function referenceRows(rows: DiscoveryAssociation[]): RecordedReferenceAssociation[] {
  return rows.filter((row): row is RecordedReferenceAssociation => row.kind === "recorded_reference");
}

function workspaceRows(rows: DiscoveryAssociation[]): WorkspaceAssociation[] {
  return rows.filter((row): row is WorkspaceAssociation => row.kind === "workspace");
}

function loopTask(taskId: string, title: string): LoopTask {
  return {
    taskId,
    title,
    description: null,
    status: "open",
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

function threadChannel(groupId: string, shortId: string): string {
  return `${groupId}____${shortId}`;
}

function workspaceProject(
  repo: string,
  path: string | null,
): ThreadWorkspaceProjectRecord {
  return { name: "fixture-project", repo, path, branch: "main", created: true };
}

function workspaceRecord(input: {
  botUid: string;
  channelId: string;
  layout: ThreadWorkspaceRecord["layout"];
  status: ThreadWorkspaceRecord["status"];
  entryPath: string | null;
  workspaceRoot: string | null;
  projects: ThreadWorkspaceProjectRecord[];
}): ThreadWorkspaceRecord {
  return {
    botUid: input.botUid,
    channelType: 5,
    channelId: input.channelId,
    layout: input.layout,
    status: input.status,
    entryPath: input.entryPath,
    workspaceRoot: input.workspaceRoot,
    projects: input.projects,
    error: null,
  };
}

async function inTemporaryStore(
  run: (store: ChannelStore, root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "oco-discovery-associations-"));
  const store = await openChannelStore(root);
  try {
    await run(store, root);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

test("loop discovery returns current routes, ending facts, and pair-scoped matches per bot", async () => {
  await inTemporaryStore(async (store) => {
    const botUid = "discovery-loop-bot";
    const otherBotUid = "discovery-loop-other-bot";
    const groupId = "discovery-loop-group";
    const workspace: LoopWorkspace = {
      workspaceId: "discovery-loop-workspace",
      name: "Discovery Loop Workspace",
      slug: "discovery-loop-workspace",
    };
    const otherWorkspace: LoopWorkspace = {
      workspaceId: "discovery-loop-workspace-two",
      name: "Discovery Loop Workspace Two",
      slug: "discovery-loop-workspace-two",
    };
    const activeTask = loopTask("task-active", "Active route");
    const endedTask = loopTask("task-ended", "Archived route");
    const pendingTask = loopTask("task-pending", "Pending merge route");

    store.bindLoopWorkspace(botUid, groupId, workspace, [activeTask, endedTask, pendingTask]);
    store.bindLoopTask(
      { botUid, groupId, workspaceId: workspace.workspaceId, taskId: activeTask.taskId },
      threadChannel(groupId, "active-child"),
    );
    const endedRoute = store.bindLoopTask(
      { botUid, groupId, workspaceId: workspace.workspaceId, taskId: endedTask.taskId },
      threadChannel(groupId, "ended-child"),
    );
    const pendingRoute = store.bindLoopTask(
      { botUid, groupId, workspaceId: workspace.workspaceId, taskId: pendingTask.taskId },
      threadChannel(groupId, "pending-child"),
    );

    store.markLoopThreadRoutesEnding(
      botUid,
      groupId,
      threadChannel(groupId, "ended-child"),
      "archived",
      "ended",
    );
    const endedEnding = store.getLoopTaskRouteEnding({
      botUid,
      groupId,
      workspaceId: workspace.workspaceId,
      taskId: endedTask.taskId,
    })!;
    store.markLoopThreadRoutesEnding(
      botUid,
      groupId,
      threadChannel(groupId, "pending-child"),
      "merged",
      "pending",
    );
    const pendingEnding = store.getLoopTaskRouteEnding({
      botUid,
      groupId,
      workspaceId: workspace.workspaceId,
      taskId: pendingTask.taskId,
    })!;

    const activeKey: LoopTaskKey = {
      botUid,
      groupId,
      workspaceId: workspace.workspaceId,
      taskId: activeTask.taskId,
    };
    const activeRoute = store.getLoopTaskRoute(activeKey)!;

    const activeRows = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: activeTask.taskId,
    });
    expect(loopRouteRows(activeRows)).toHaveLength(1);
    const activeAssociation = loopRouteRows(activeRows)[0]!;
    expect(activeAssociation).toMatchObject({
      kind: "loop_route",
      channelType: 5,
      channelId: activeRoute.threadChannelId,
      groupId,
      workspaceId: workspace.workspaceId,
      taskId: activeTask.taskId,
      routeVersion: activeRoute.version,
      state: "active",
      reason: null,
    });

    const endedRows = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: endedTask.taskId,
    });
    expect(loopRouteRows(endedRows)).toHaveLength(1);
    const endedAssociation = loopRouteRows(endedRows)[0]!;
    expect(endedAssociation).toMatchObject({
      kind: "loop_route",
      channelType: 5,
      channelId: endedEnding.threadChannelId,
      groupId: endedEnding.groupId,
      workspaceId: endedEnding.workspaceId,
      taskId: endedEnding.taskId,
      routeVersion: endedEnding.routeVersion,
      state: "ended",
      reason: "archived",
    });
    expect(endedAssociation.routeVersion).toBe(endedRoute.version);
    expect(endedAssociation.updatedAt).toBe(endedEnding.updatedAt);

    const pendingRows = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: pendingTask.taskId,
    });
    expect(loopRouteRows(pendingRows)).toHaveLength(1);
    expect(loopRouteRows(pendingRows)[0]).toMatchObject({
      kind: "loop_route",
      channelType: 5,
      channelId: pendingEnding.threadChannelId,
      groupId: pendingEnding.groupId,
      workspaceId: pendingEnding.workspaceId,
      taskId: pendingEnding.taskId,
      routeVersion: pendingEnding.routeVersion,
      state: "pending",
      reason: "merged",
      updatedAt: pendingEnding.updatedAt,
    });
    expect(loopRouteRows(pendingRows)[0]!.routeVersion).toBe(pendingRoute.version);

    // The same task ID in another workspace must not answer a paired workspace query.
    const otherGroupId = "discovery-loop-group-two";
    store.bindLoopWorkspace(botUid, otherGroupId, otherWorkspace, [activeTask]);
    store.bindLoopTask(
      {
        botUid,
        groupId: otherGroupId,
        workspaceId: otherWorkspace.workspaceId,
        taskId: activeTask.taskId,
      },
      threadChannel(otherGroupId, "active-child-two"),
    );

    const firstWorkspaceRows = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: activeTask.taskId,
    });
    expect(loopRouteRows(firstWorkspaceRows)).toHaveLength(1);
    expect(loopRouteRows(firstWorkspaceRows)[0]!.channelId).toBe(activeRoute.threadChannelId);
    expect(loopRouteRows(firstWorkspaceRows)[0]!.groupId).toBe(groupId);

    const secondWorkspaceRows = findAssociations(store, botUid, {
      workspaceId: otherWorkspace.workspaceId,
      taskId: activeTask.taskId,
    });
    expect(loopRouteRows(secondWorkspaceRows)).toHaveLength(1);
    expect(loopRouteRows(secondWorkspaceRows)[0]!.channelId).toBe(
      threadChannel(otherGroupId, "active-child-two"),
    );
    expect(loopRouteRows(secondWorkspaceRows)[0]!.groupId).toBe(otherGroupId);
    expect(loopRouteRows(secondWorkspaceRows)[0]!.workspaceId).toBe(otherWorkspace.workspaceId);

    // A task ID without its workspace is ambiguous, not a cross-workspace shortcut.
    expect(loopRouteRows(findAssociations(store, botUid, { taskId: activeTask.taskId }))).toEqual([]);
    // No clue is not a licence to enumerate every local record for the bot.
    expect(findAssociations(store, botUid, {})).toEqual([]);

    // The other bot has the same task key but its own child route; neither bot sees the other's.
    store.bindLoopWorkspace(otherBotUid, groupId, workspace, [activeTask]);
    store.bindLoopTask(
      { botUid: otherBotUid, groupId, workspaceId: workspace.workspaceId, taskId: activeTask.taskId },
      threadChannel(groupId, "other-bot-child"),
    );
    const otherBotRows = findAssociations(store, otherBotUid, {
      workspaceId: workspace.workspaceId,
      taskId: activeTask.taskId,
    });
    expect(loopRouteRows(otherBotRows)).toHaveLength(1);
    expect(loopRouteRows(otherBotRows)[0]!.channelId).toBe(
      threadChannel(groupId, "other-bot-child"),
    );
    expect(
      loopRouteRows(findAssociations(store, botUid, {
        workspaceId: workspace.workspaceId,
        taskId: activeTask.taskId,
      })).map((row) => row.channelId),
    ).toEqual([activeRoute.threadChannelId]);
  });
});

test("recorded PR/MR discovery matches canonical URLs by bot and channel, never by similarity", async () => {
  await inTemporaryStore(async (store) => {
    const botUid = "discovery-reference-bot";
    const otherBotUid = "discovery-reference-other-bot";
    const groupId = "discovery-reference-group";
    const first: ChatTarget = { channelType: 5, channelId: threadChannel(groupId, "first") };
    const second: ChatTarget = { channelType: 5, channelId: threadChannel(groupId, "second") };
    const otherBotTarget: ChatTarget = {
      channelType: 5,
      channelId: threadChannel(groupId, "other-bot"),
    };
    const pr: ThreadReference = {
      kind: "pr",
      url: "https://github.com/example/service/pull/17",
    };
    const otherPr: ThreadReference = {
      kind: "pr",
      url: "https://github.com/example/service/pull/18",
    };
    const mr: ThreadReference = {
      kind: "mr",
      url: "https://gitlab.example/team/service/-/merge_requests/42",
    };

    store.addThreadReferences(botUid, first, [pr, mr]);
    store.addThreadReferences(botUid, second, [pr]);
    store.addThreadReferences(otherBotUid, otherBotTarget, [pr]);

    const prRows = referenceRows(findAssociations(store, botUid, { referenceUrl: pr.url }));
    expect(prRows).toHaveLength(2);
    expect(prRows.map((row) => row.channelId).sort()).toEqual(
      [first.channelId, second.channelId].sort(),
    );
    for (const row of prRows) {
      expect(row).toMatchObject({
        kind: "recorded_reference",
        channelType: 5,
        referenceKind: "pr",
        url: pr.url,
      });
    }
    expect(prRows.every((row) => row.channelId !== otherBotTarget.channelId)).toBe(true);

    const mrRows = referenceRows(findAssociations(store, botUid, { referenceUrl: mr.url }));
    expect(mrRows).toHaveLength(1);
    expect(mrRows[0]).toMatchObject({
      kind: "recorded_reference",
      channelType: 5,
      channelId: first.channelId,
      referenceKind: "mr",
      url: mr.url,
    });

    const otherBotRows = referenceRows(
      findAssociations(store, otherBotUid, { referenceUrl: pr.url }),
    );
    expect(otherBotRows).toHaveLength(1);
    expect(otherBotRows[0]).toMatchObject({
      kind: "recorded_reference",
      channelType: 5,
      channelId: otherBotTarget.channelId,
      referenceKind: "pr",
      url: pr.url,
    });

    // A neighbouring PR number or an unrecorded URL is a different reference.
    expect(referenceRows(findAssociations(store, botUid, { referenceUrl: otherPr.url }))).toEqual([]);
  });
});

test("workspace discovery matches repo and path exactly and deduplicates the same record", async () => {
  await inTemporaryStore(async (store, root) => {
    const botUid = "discovery-workspace-bot";
    const otherBotUid = "discovery-workspace-other-bot";
    const groupId = "discovery-workspace-group";
    const alphaRoot = join(root, "workspaces", "alpha");
    const siblingRoot = `${alphaRoot}-sibling`;
    const betaEntry = join(root, "workspaces", "beta-entry");
    const serviceRepo = "https://github.com/example/service";
    const serviceToolsRepo = "https://github.com/example/service-tools";

    const alpha = workspaceRecord({
      botUid,
      channelId: threadChannel(groupId, "alpha"),
      layout: "single",
      status: "ready",
      entryPath: null,
      workspaceRoot: alphaRoot,
      projects: [workspaceProject(serviceRepo, join(alphaRoot, "service"))],
    });
    const sibling = workspaceRecord({
      botUid,
      channelId: threadChannel(groupId, "sibling"),
      layout: "shared",
      status: "ready",
      entryPath: null,
      workspaceRoot: siblingRoot,
      projects: [workspaceProject(serviceToolsRepo, join(siblingRoot, "tools"))],
    });
    const beta = workspaceRecord({
      botUid,
      channelId: threadChannel(groupId, "beta"),
      layout: "separate",
      status: "ready",
      entryPath: betaEntry,
      workspaceRoot: null,
      projects: [workspaceProject(serviceToolsRepo, join(betaEntry, "tools"))],
    });
    const otherBotAlpha = workspaceRecord({
      botUid: otherBotUid,
      channelId: threadChannel(groupId, "other-bot-alpha"),
      layout: "single",
      status: "ready",
      entryPath: null,
      workspaceRoot: alphaRoot,
      projects: [workspaceProject(serviceRepo, join(alphaRoot, "service"))],
    });
    store.saveThreadWorkspaces([alpha, sibling, beta, otherBotAlpha]);

    const byRepo = workspaceRows(findAssociations(store, botUid, { repo: serviceRepo }));
    expect(byRepo).toHaveLength(1);
    expect(byRepo[0]).toMatchObject({
      kind: "workspace",
      channelType: 5,
      channelId: alpha.channelId,
      layout: alpha.layout,
      status: alpha.status,
      entryPath: alpha.entryPath,
      workspaceRoot: alphaRoot,
      error: null,
    });
    expect(byRepo[0]!.projects).toEqual(alpha.projects);

    // A longer repo name shares a prefix but is not the same project record.
    const byToolsRepo = workspaceRows(
      findAssociations(store, botUid, { repo: serviceToolsRepo }),
    );
    expect(byToolsRepo.map((row) => row.channelId).sort()).toEqual(
      [sibling.channelId, beta.channelId].sort(),
    );
    expect(byToolsRepo.every((row) => row.channelId !== alpha.channelId)).toBe(true);
    expect(workspaceRows(findAssociations(store, botUid, { repo: serviceRepo.slice(0, -1) }))).toEqual([]);

    const byWorkspaceRoot = workspaceRows(findAssociations(store, botUid, { path: alphaRoot }));
    expect(byWorkspaceRoot).toHaveLength(1);
    expect(byWorkspaceRoot[0]!.channelId).toBe(alpha.channelId);

    const byProjectPath = workspaceRows(
      findAssociations(store, botUid, { path: join(alphaRoot, "service") }),
    );
    expect(byProjectPath).toHaveLength(1);
    expect(byProjectPath[0]!.channelId).toBe(alpha.channelId);

    const byEntryPath = workspaceRows(findAssociations(store, botUid, { path: betaEntry }));
    expect(byEntryPath).toHaveLength(1);
    expect(byEntryPath[0]!.channelId).toBe(beta.channelId);

    // A sibling root sharing the prefix is a distinct workspace, and prefix matching must not leak it.
    const bySiblingRoot = workspaceRows(findAssociations(store, botUid, { path: siblingRoot }));
    expect(bySiblingRoot).toHaveLength(1);
    expect(bySiblingRoot[0]!.channelId).toBe(sibling.channelId);
    expect(byWorkspaceRoot[0]!.channelId).toBe(alpha.channelId);
    expect(workspaceRows(findAssociations(store, botUid, { path: `${alphaRoot}-other` }))).toEqual([]);

    // Both hints resolve to the same record; discovery reports each channel once.
    const deduplicated = workspaceRows(
      findAssociations(store, botUid, { repo: serviceRepo, path: alphaRoot }),
    );
    expect(deduplicated).toHaveLength(1);
    expect(deduplicated[0]!.channelId).toBe(alpha.channelId);

    const otherBotRows = workspaceRows(
      findAssociations(store, otherBotUid, { repo: serviceRepo }),
    );
    expect(otherBotRows).toHaveLength(1);
    expect(otherBotRows[0]!.channelId).toBe(otherBotAlpha.channelId);
    expect(byRepo.map((row) => row.channelId)).toEqual([alpha.channelId]);
  });
});

test("a Loop workspace clue alone scopes routes to that bot and workspace", async () => {
  await inTemporaryStore(async (store) => {
    const botUid = "discovery-loop-workspace-bot";
    const otherBotUid = "discovery-loop-workspace-other-bot";
    const groupOne = "discovery-loop-workspace-group-one";
    const groupTwo = "discovery-loop-workspace-group-two";
    const firstWorkspace: LoopWorkspace = {
      workspaceId: "discovery-loop-workspace-one",
      name: "Discovery Loop Workspace One",
      slug: "discovery-loop-workspace-one",
    };
    const secondWorkspace: LoopWorkspace = {
      workspaceId: "discovery-loop-workspace-two",
      name: "Discovery Loop Workspace Two",
      slug: "discovery-loop-workspace-two",
    };
    const firstTask = loopTask("workspace-task-one", "First route");
    const secondTask = loopTask("workspace-task-two", "Second route");

    store.bindLoopWorkspace(botUid, groupOne, firstWorkspace, [firstTask, secondTask]);
    store.bindLoopTask(
      { botUid, groupId: groupOne, workspaceId: firstWorkspace.workspaceId, taskId: firstTask.taskId },
      threadChannel(groupOne, "one"),
    );
    store.bindLoopTask(
      { botUid, groupId: groupOne, workspaceId: firstWorkspace.workspaceId, taskId: secondTask.taskId },
      threadChannel(groupOne, "two"),
    );

    store.bindLoopWorkspace(botUid, groupTwo, secondWorkspace, [secondTask]);
    store.bindLoopTask(
      { botUid, groupId: groupTwo, workspaceId: secondWorkspace.workspaceId, taskId: secondTask.taskId },
      threadChannel(groupTwo, "two"),
    );

    store.bindLoopWorkspace(otherBotUid, groupOne, firstWorkspace, [firstTask]);
    store.bindLoopTask(
      { botUid: otherBotUid, groupId: groupOne, workspaceId: firstWorkspace.workspaceId, taskId: firstTask.taskId },
      threadChannel(groupOne, "other-bot"),
    );

    const firstRows = loopRouteRows(
      findAssociations(store, botUid, { workspaceId: firstWorkspace.workspaceId }),
    );
    expect(firstRows.map((row) => row.channelId).sort()).toEqual(
      [threadChannel(groupOne, "one"), threadChannel(groupOne, "two")].sort(),
    );
    expect(firstRows.every((row) => row.workspaceId === firstWorkspace.workspaceId)).toBe(true);

    const secondRows = loopRouteRows(
      findAssociations(store, botUid, { workspaceId: secondWorkspace.workspaceId }),
    );
    expect(secondRows.map((row) => row.channelId)).toEqual([threadChannel(groupTwo, "two")]);

    const otherBotRows = loopRouteRows(
      findAssociations(store, otherBotUid, { workspaceId: firstWorkspace.workspaceId }),
    );
    expect(otherBotRows.map((row) => row.channelId)).toEqual([threadChannel(groupOne, "other-bot")]);

    // A workspace clue belongs to the requested bot only.
    expect(
      loopRouteRows(findAssociations(store, botUid, { workspaceId: "unknown-workspace" })),
    ).toEqual([]);
  });
});

test("recorded reference discovery canonicalizes the query URL before exact matching", async () => {
  await inTemporaryStore(async (store) => {
    const botUid = "discovery-reference-canonical-bot";
    const groupId = "discovery-reference-canonical-group";
    const target: ChatTarget = {
      channelType: 5,
      channelId: threadChannel(groupId, "canonical"),
    };
    const storedUrl = "https://github.com/example/service/pull/17";
    store.addThreadReferences(botUid, target, [{ kind: "pr", url: storedUrl }]);

    for (const url of [
      storedUrl,
      "https://github.com/Example/Service/pull/17/files",
      "https://github.com/example/service/pull/017",
    ]) {
      const rows = referenceRows(findAssociations(store, botUid, { referenceUrl: url }));
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        kind: "recorded_reference",
        channelId: target.channelId,
        referenceKind: "pr",
        url: storedUrl,
      });
    }

    // Non-reference URLs and a different canonical reference are not matches.
    expect(
      referenceRows(
        findAssociations(store, botUid, { referenceUrl: "https://github.com/example/service" }),
      ),
    ).toEqual([]);
    expect(
      referenceRows(
        findAssociations(store, botUid, {
          referenceUrl: "https://github.com/other/service/pull/17",
        }),
      ),
    ).toEqual([]);
  });
});

test("one query unions Loop, recorded reference, and workspace evidence", async () => {
  await inTemporaryStore(async (store, root) => {
    const botUid = "discovery-union-bot";
    const groupId = "discovery-union-group";
    const workspace: LoopWorkspace = {
      workspaceId: "discovery-union-workspace",
      name: "Discovery Union Workspace",
      slug: "discovery-union-workspace",
    };
    const task = loopTask("discovery-union-task", "Union task");
    const loopThread = threadChannel(groupId, "loop");
    const referenceThread = threadChannel(groupId, "reference");
    const workspaceThread = threadChannel(groupId, "workspace");
    const serviceRepo = "https://github.com/example/service";
    const prUrl = "https://github.com/example/service/pull/23";
    const workspaceRoot = join(root, "union-workspace");

    store.bindLoopWorkspace(botUid, groupId, workspace, [task]);
    store.bindLoopTask(
      { botUid, groupId, workspaceId: workspace.workspaceId, taskId: task.taskId },
      loopThread,
    );
    store.addThreadReferences(
      botUid,
      { channelType: 5, channelId: referenceThread },
      [{ kind: "pr", url: prUrl }],
    );
    store.saveThreadWorkspaces([
      workspaceRecord({
        botUid,
        channelId: workspaceThread,
        layout: "single",
        status: "ready",
        entryPath: null,
        workspaceRoot,
        projects: [workspaceProject(serviceRepo, join(workspaceRoot, "service"))],
      }),
    ]);

    const rows = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: task.taskId,
      referenceUrl: prUrl,
      repo: serviceRepo,
    });
    expect(loopRouteRows(rows).map((row) => row.channelId)).toEqual([loopThread]);
    expect(referenceRows(rows).map((row) => row.channelId)).toEqual([referenceThread]);
    expect(workspaceRows(rows).map((row) => row.channelId)).toEqual([workspaceThread]);

    // A source without its own clue contributes nothing to the union.
    const loopOnly = findAssociations(store, botUid, {
      workspaceId: workspace.workspaceId,
      taskId: task.taskId,
    });
    expect(referenceRows(loopOnly)).toEqual([]);
    expect(workspaceRows(loopOnly)).toEqual([]);
  });
});

test("ending history stays independent when its thread or route version differs from the current route", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-discovery-route-history-"));
  try {
    const botUid = "discovery-route-history-bot";
    const groupId = "discovery-route-history-group";
    const workspace: LoopWorkspace = {
      workspaceId: "discovery-route-history-workspace",
      name: "Discovery Route History Workspace",
      slug: "discovery-route-history-workspace",
    };
    const reboundTask = loopTask("route-history-rebound", "Rebound route");
    const versionTask = loopTask("route-history-version", "Versioned route");
    const oldThread = threadChannel(groupId, "old-child");
    const newThread = threadChannel(groupId, "new-child");
    const sameThread = threadChannel(groupId, "same-child");

    const seed = await openChannelStore(root);
    let newRouteVersion = 0;
    let sameThreadRouteVersion = 0;
    try {
      seed.bindLoopWorkspace(botUid, groupId, workspace, [reboundTask, versionTask]);
      seed.bindLoopTask(
        { botUid, groupId, workspaceId: workspace.workspaceId, taskId: reboundTask.taskId },
        oldThread,
      );
      newRouteVersion = seed.bindLoopTask(
        { botUid, groupId, workspaceId: workspace.workspaceId, taskId: reboundTask.taskId },
        newThread,
      ).version;
      sameThreadRouteVersion = seed.bindLoopTask(
        { botUid, groupId, workspaceId: workspace.workspaceId, taskId: versionTask.taskId },
        sameThread,
      ).version;
    } finally {
      seed.close();
    }

    // Simulate the crash window between a route re-bind and ending cleanup: the durable ending
    // still names the previous thread/version and must never be merged into the current route.
    const raw = new Database(join(root, "channel.sqlite"));
    try {
      raw.run(
        `INSERT INTO loop_route_endings(
           bot_uid, group_id, workspace_id, task_id, thread_channel_id, route_version,
           state, reason, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [botUid, groupId, workspace.workspaceId, reboundTask.taskId, oldThread, 1, "ended", "merged", 111],
      );
      raw.run(
        `INSERT INTO loop_route_endings(
           bot_uid, group_id, workspace_id, task_id, thread_channel_id, route_version,
           state, reason, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          botUid,
          groupId,
          workspace.workspaceId,
          versionTask.taskId,
          sameThread,
          sameThreadRouteVersion + 1,
          "pending",
          "archived",
          222,
        ],
      );
    } finally {
      raw.close();
    }

    const store = await openChannelStore(root);
    try {
      const reboundRows = loopRouteRows(
        findAssociations(store, botUid, {
          workspaceId: workspace.workspaceId,
          taskId: reboundTask.taskId,
        }),
      );
      expect(reboundRows).toHaveLength(2);
      expect(reboundRows.find((row) => row.state === "active")).toMatchObject({
        channelId: newThread,
        routeVersion: newRouteVersion,
        reason: null,
      });
      expect(reboundRows.find((row) => row.state === "ended")).toMatchObject({
        channelId: oldThread,
        routeVersion: 1,
        reason: "merged",
        updatedAt: 111,
      });

      const versionRows = loopRouteRows(
        findAssociations(store, botUid, {
          workspaceId: workspace.workspaceId,
          taskId: versionTask.taskId,
        }),
      );
      expect(versionRows).toHaveLength(2);
      expect(versionRows.find((row) => row.state === "active")).toMatchObject({
        channelId: sameThread,
        routeVersion: sameThreadRouteVersion,
        reason: null,
      });
      expect(versionRows.find((row) => row.state === "pending")).toMatchObject({
        channelId: sameThread,
        routeVersion: sameThreadRouteVersion + 1,
        reason: "archived",
        updatedAt: 222,
      });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
