import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type {
  InboundClaim,
  TaskRecord,
  ThreadWorkspaceIntentInput,
  ThreadWorkspaceProjectRecord,
  ThreadWorkspaceRecord,
} from "../src/store/store-records.ts";
import { TaskScheduler, type TaskSchedulerDeps } from "../src/tasks/task-scheduler.ts";

const botUid = "bot-workspace-ownership";

type WorkspaceStatus = ThreadWorkspaceRecord["status"];
type WorkspaceProject = ThreadWorkspaceProjectRecord;
async function withStore<T>(work: (store: ChannelStore, stateDir: string, root: string) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-workspace-ownership-"));
  const stateDir = join(root, "state");
  const store = await openChannelStore(stateDir);
  try {
    return await work(store, stateDir, root);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

function sharedWorkspace(
  channelId: string,
  entryPath: string | null,
  status: WorkspaceStatus,
  projects: WorkspaceProject[],
  error: string | null = null,
  ownerBotUid = botUid,
): ThreadWorkspaceRecord {
  return {
    botUid: ownerBotUid,
    channelId,
    channelType: 5,
    layout: "shared",
    status,
    entryPath,
    workspaceRoot: null,
    projects,
    error,
  };
}

function schedulerFor(store: ChannelStore, overrides: Partial<TaskSchedulerDeps> = {}): TaskScheduler {
  return new TaskScheduler({
    store,
    botUid: () => botUid,
    stopping: () => false,
    isDirectoryBlocked: () => false,
    reconcileBinding: async (_taskId, _target, binding) => binding,
    repositoryRoot: async () => null,
    repositoryBlocked: async () => false,
    shouldRunTaskBeforeRepositoryProbe: async () => false,
    runTask: async () => {},
    awaitStartReady: async () => {},
    enqueueText: async () => {},
    notifyThreadOutcome: async () => {},
    ...overrides,
  });
}

function taskFor(target: ChatTarget, messageId: string, taskId: string, cwd: string): {
  claim: InboundClaim;
  task: TaskRecord;
} {
  const text = `task ${taskId}`;
  return {
    claim: {
      botUid,
      target,
      messageId,
      senderId: "owner-1",
      text,
      supported: true,
      kind: "task",
    },
    task: {
      id: taskId,
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      senderId: "owner-1",
      text,
      sessionId: `session-${taskId}`,
      cwd,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: true,
      relaySuppressed: false,
    },
  };
}

test("preparing and failed ownership plus planned/created outcomes survive database reopens", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-workspace-ownership-reopen-"));
  const stateDir = join(root, "state");
  const entryPath = join(root, "thread-workspaces", "batch-17");
  const workspaceRoot = join(root, "thread-workspaces");
  const plannedPath = join(entryPath, "project-b");
  const createdPath = join(entryPath, "project-a");
  const preparing = {
    ...sharedWorkspace("group-a____preparing", entryPath, "preparing", [
      { name: "project-b", repo: "/repos/project-b", base: "main", path: plannedPath, branch: "thread-project-b", created: false },
    ]),
    workspaceRoot,
  };
  const failed = sharedWorkspace("group-a____failed", entryPath, "failed", [
    { name: "project-a", repo: "/repos/project-a", base: "release", path: createdPath, branch: "thread-project-a", created: true },
    { name: "project-b", repo: "/repos/project-b", base: "main", path: plannedPath, branch: "thread-project-b", created: false },
  ], "project-b worktree creation was not confirmed");

  try {
    const first = await openChannelStore(stateDir);
    try {
      first.saveThreadWorkspaces([preparing, failed]);
      expect(first.getThreadWorkspace(botUid, { channelType: 5, channelId: preparing.channelId })).toEqual(preparing);
      expect(first.getThreadWorkspace(botUid, { channelType: 5, channelId: failed.channelId })).toEqual(failed);
    } finally {
      first.close();
    }

    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.getThreadWorkspace(botUid, { channelType: 5, channelId: preparing.channelId })).toEqual(preparing);
      expect(reopened.getThreadWorkspace(botUid, { channelType: 5, channelId: failed.channelId })).toEqual(failed);
      const listed = reopened.listThreadWorkspaces(botUid);
      expect(listed).toHaveLength(2);
      expect(listed).toContainEqual(preparing);
      expect(listed).toContainEqual(failed);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("opening a legacy database adds nullable workspace ownership without changing existing records", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-workspace-ownership-legacy-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const legacyBinding = {
    botUid,
    channelType: 5 as const,
    channelId: "group-legacy____thread-1",
    sessionId: "legacy-session",
    cwd: join(root, "legacy-cwd"),
    sessionFile: null,
    cwdPinned: true,
  };
  const legacyWorkspace = sharedWorkspace(legacyBinding.channelId, join(root, "legacy-entry"), "preparing", [
    { name: "legacy-project", repo: "/repos/legacy-project", path: join(root, "legacy-entry", "legacy-project"), branch: "thread-legacy-project", created: false },
  ]);

  const legacyDb = new Database(join(stateDir, "channel.sqlite"));
  try {
    legacyDb.exec(`
      CREATE TABLE chats (
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cwd TEXT NOT NULL,
        session_file TEXT,
        cwd_pinned INTEGER NOT NULL DEFAULT 0,
        session_switched_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, channel_type, channel_id)
      );
      CREATE TABLE thread_workspaces (
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type = 5),
        channel_id TEXT NOT NULL,
        layout TEXT NOT NULL CHECK(layout IN ('single', 'shared', 'separate')),
        status TEXT NOT NULL CHECK(status IN ('preparing', 'ready', 'failed')),
        entry_path TEXT,
        projects TEXT NOT NULL,
        error TEXT,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, channel_id)
      );
    `);
    legacyDb.query(
      `INSERT INTO chats(bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned, session_switched_at, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      legacyBinding.botUid,
      legacyBinding.channelType,
      legacyBinding.channelId,
      legacyBinding.sessionId,
      legacyBinding.cwd,
      legacyBinding.sessionFile,
      1,
      null,
      10,
      10,
    );
    legacyDb.query(
      `INSERT INTO thread_workspaces(bot_uid, channel_type, channel_id, layout, status, entry_path, projects, error, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      legacyWorkspace.botUid,
      legacyWorkspace.channelType,
      legacyWorkspace.channelId,
      legacyWorkspace.layout,
      legacyWorkspace.status,
      legacyWorkspace.entryPath,
      JSON.stringify(legacyWorkspace.projects),
      legacyWorkspace.error,
      10,
    );
    const columns = legacyDb.query("PRAGMA table_info(thread_workspaces)").all() as Array<{ name: string }>;
    expect(columns.map(({ name }) => name)).not.toContain("workspace_root");
  } finally {
    legacyDb.close();
  }

  try {
    const upgraded = await openChannelStore(stateDir);
    try {
      expect(upgraded.getBinding(botUid, legacyBinding)).toMatchObject({
        sessionId: "legacy-session",
        cwd: legacyBinding.cwd,
        cwdPinned: true,
      });
      expect(upgraded.getThreadWorkspace(botUid, legacyBinding)).toEqual(legacyWorkspace);
    } finally {
      upgraded.close();
    }

    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.getBinding(botUid, legacyBinding)).toMatchObject({
        sessionId: "legacy-session",
        cwd: legacyBinding.cwd,
        cwdPinned: true,
      });
      expect(reopened.getThreadWorkspace(botUid, legacyBinding)).toEqual(legacyWorkspace);
    } finally {
      reopened.close();
    }

    const migratedDb = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    try {
      const columns = migratedDb.query("PRAGMA table_info(thread_workspaces)").all() as Array<{
        name: string;
        type: string;
        notnull: number;
      }>;
      expect(columns.find(({ name }) => name === "workspace_root"))
        .toMatchObject({ type: "TEXT", notnull: 0 });
      const storedWorkspace = migratedDb.query(`
        SELECT layout, status, entry_path, workspace_root, projects, error, updated_at
        FROM thread_workspaces
        WHERE bot_uid = ? AND channel_id = ?
      `).get(botUid, legacyBinding.channelId);
      expect(storedWorkspace).toEqual({
        layout: "shared",
        status: "preparing",
        entry_path: legacyWorkspace.entryPath,
        workspace_root: null,
        projects: JSON.stringify(legacyWorkspace.projects),
        error: null,
        updated_at: 10,
      });
    } finally {
      migratedDb.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("workspace directory expansion covers entries, owned projects, subdirectories, and explicit parent bindings by path segments", async () => {
  await withStore(async (store, _stateDir, root) => {
    const pool = join(root, "thread-workspaces");
    const rawEntry = join(pool, "thread-1");
    const rawProjectA = join(rawEntry, "project-a");
    const rawProjectB = join(rawEntry, "project-b");
    const rawSubdirectory = join(rawProjectA, "src");
    const rawPrefixSibling = join(pool, `${basename(rawEntry)}-sibling`);
    await Promise.all([
      mkdir(rawProjectA, { recursive: true }),
      mkdir(rawProjectB, { recursive: true }),
      mkdir(rawSubdirectory, { recursive: true }),
      mkdir(rawPrefixSibling, { recursive: true }),
    ]);
    const entry = await realpath(rawEntry);
    const projectA = await realpath(rawProjectA);
    const projectB = await realpath(rawProjectB);
    const subdirectory = await realpath(rawSubdirectory);
    const prefixSibling = await realpath(rawPrefixSibling);
    const parent = await realpath(pool);
    const repoA = await realpath(root);
    const repoB = await realpath(root);

    store.saveThreadWorkspace(sharedWorkspace("group-a____thread-1", entry, "ready", [
      { name: "project-a", repo: repoA, path: projectA, branch: "thread-project-a", created: true },
      { name: "project-b", repo: repoB, path: projectB, branch: "thread-project-b", created: true },
    ]));

    const fromProjectChild = new Set(store.workspaceDirectories(subdirectory));
    expect(fromProjectChild.has(subdirectory)).toBe(true);
    expect(fromProjectChild.has(projectA)).toBe(true);
    expect(fromProjectChild.has(projectB)).toBe(true);
    expect(fromProjectChild.has(entry)).toBe(true);

    const fromEntry = new Set(store.workspaceDirectories(entry));
    expect(fromEntry.has(entry)).toBe(true);
    expect(fromEntry.has(projectA)).toBe(true);
    expect(fromEntry.has(projectB)).toBe(true);

    const fromExplicitParentBinding = new Set(store.workspaceDirectories(parent));
    expect(fromExplicitParentBinding.has(parent)).toBe(true);
    expect(fromExplicitParentBinding.has(entry)).toBe(true);
    expect(fromExplicitParentBinding.has(projectA)).toBe(true);
    expect(fromExplicitParentBinding.has(projectB)).toBe(true);

    expect(store.workspaceDirectories(prefixSibling)).toEqual([prefixSibling]);
  });
});

test("destructive workspace leases protect actual paths and exempt only their owning task", async () => {
  await withStore(async (store, _stateDir, root) => {
    const entry = join(root, "thread-workspaces", "thread-lease");
    const projectA = join(entry, "project-a");
    const projectB = join(entry, "project-b");
    const projectAChild = join(projectA, "src");
    await Promise.all([
      mkdir(projectAChild, { recursive: true }),
      mkdir(projectB, { recursive: true }),
    ]);
    const canonicalEntry = await realpath(entry);
    const canonicalProjectA = await realpath(projectA);
    const canonicalProjectB = await realpath(projectB);
    const canonicalChild = await realpath(projectAChild);
    const repo = await realpath(root);
    store.saveThreadWorkspace(sharedWorkspace("group-a____thread-lease", canonicalEntry, "ready", [
      { name: "project-a", repo, path: canonicalProjectA, branch: "thread-project-a", created: true },
      { name: "project-b", repo, path: canonicalProjectB, branch: "thread-project-b", created: true },
    ]));

    const firstTarget = { channelType: 5 as const, channelId: "group-a____thread-owner" };
    const secondTarget = { channelType: 5 as const, channelId: "group-b____thread-other" };
    const first = taskFor(firstTarget, "message-first", "task-first", canonicalChild);
    const second = taskFor(secondTarget, "message-second", "task-second", canonicalChild);
    expect(store.acceptTask(first.claim, first.task)).toBe(true);

    const firstStarted = Promise.withResolvers<void>();
    const secondStarted = Promise.withResolvers<void>();
    const releaseFirst = Promise.withResolvers<void>();
    const releaseSecond = Promise.withResolvers<void>();
    const scheduler = schedulerFor(store, {
      runTask: async (taskId) => {
        store.markStarted(taskId);
        if (taskId === first.task.id) {
          firstStarted.resolve();
          await releaseFirst.promise;
        } else {
          secondStarted.resolve();
          await releaseSecond.promise;
        }
      },
    });
    scheduler.scheduleTask(first.task.id);
    await firstStarted.promise;

    const ownerLease = await scheduler.tryAcquireDestructiveResources([canonicalProjectA], first.task);
    expect(ownerLease).toBeDefined();
    if (!ownerLease) throw new Error("expected the owning task to upgrade its shared resource");
    await ownerLease.ready;
    ownerLease.complete();
    const siblingLease = await scheduler.tryAcquireDestructiveResources([canonicalProjectB], first.task);
    expect(siblingLease).toBeDefined();
    siblingLease?.complete();

    expect(store.acceptTask(second.claim, second.task)).toBe(true);
    scheduler.scheduleTask(second.task.id);
    await secondStarted.promise;
    expect(
      await scheduler.tryAcquireDestructiveResources([canonicalProjectA], first.task),
    ).toBeUndefined();
    expect(
      await scheduler.tryAcquireDestructiveResources([canonicalEntry], first.task),
    ).toBeUndefined();

    releaseFirst.resolve();
    releaseSecond.resolve();
    await scheduler.settle();
  });
});

test("queued tasks from separate project directories do not serialize on shared workspace ownership", async () => {
  await withStore(async (store, _stateDir, root) => {
    const entry = join(root, "thread-workspaces", "thread-order");
    const projectA = join(entry, "project-a");
    const projectB = join(entry, "project-b");
    await Promise.all([mkdir(projectA, { recursive: true }), mkdir(projectB, { recursive: true })]);
    const canonicalEntry = await realpath(entry);
    const canonicalProjectA = await realpath(projectA);
    const canonicalProjectB = await realpath(projectB);
    const repo = await realpath(root);
    store.saveThreadWorkspace(sharedWorkspace("group-a____thread-order", canonicalEntry, "ready", [
      { name: "project-a", repo, path: canonicalProjectA, branch: "thread-project-a", created: true },
      { name: "project-b", repo, path: canonicalProjectB, branch: "thread-project-b", created: true },
    ]));

    const firstTarget = { channelType: 5 as const, channelId: "group-a____thread-a" };
    const secondTarget = { channelType: 5 as const, channelId: "group-b____thread-b" };
    const first = taskFor(firstTarget, "message-first", "task-first", join(canonicalProjectA, "src"));
    const second = taskFor(secondTarget, "message-second", "task-second", join(canonicalProjectB, "src"));
    expect(store.acceptTask(first.claim, first.task)).toBe(true);
    expect(store.acceptTask(second.claim, second.task)).toBe(true);

    const firstStarted = Promise.withResolvers<void>();
    const releaseFirst = Promise.withResolvers<void>();
    const secondStarted = Promise.withResolvers<void>();
    let secondStartCount = 0;
    const scheduler = schedulerFor(store, {
      runTask: async (taskId) => {
        if (taskId === first.task.id) {
          firstStarted.resolve();
          await releaseFirst.promise;
          return;
        }
        if (taskId === second.task.id) {
          secondStartCount += 1;
          secondStarted.resolve();
        }
      },
    });

    scheduler.scheduleTask(first.task.id);
    await firstStarted.promise;
    scheduler.scheduleTask(second.task.id);
    await secondStarted.promise;
    expect(secondStartCount).toBe(1);

    releaseFirst.resolve();
    await scheduler.settle();
  });
});


test("a batch ownership commit rolls back every row when one target is not a CommunityTopic", async () => {
  await withStore(async (store, _stateDir, root) => {
    const entry = join(root, "thread-workspaces", "batch-commit");
    const pendingA = sharedWorkspace("group-a____thread-a", entry, "preparing", [
      { name: "project-a", repo: "/repos/project-a", path: join(entry, "project-a"), branch: "thread-project-a", created: false },
    ]);
    const pendingB = sharedWorkspace("group-b____thread-b", entry, "preparing", [
      { name: "project-b", repo: "/repos/project-b", path: join(entry, "project-b"), branch: "thread-project-b", created: true },
    ]);
    store.saveThreadWorkspaces([pendingA, pendingB]);

    const readyA = {
      ...pendingA,
      status: "ready" as const,
      projects: pendingA.projects.map((project) => ({ ...project, created: true })),
    };
    const invalid = {
      ...pendingB,
      channelType: 2,
      projects: pendingB.projects.map((project) => ({ ...project, created: false })),
    } as unknown as ThreadWorkspaceRecord;
    expect(() => store.saveThreadWorkspaces([readyA, invalid])).toThrow();
    expect(store.getThreadWorkspace(botUid, { channelType: 5, channelId: pendingA.channelId })).toEqual(pendingA);
    expect(store.getThreadWorkspace(botUid, { channelType: 5, channelId: pendingB.channelId })).toEqual(pendingB);
  });
});

test("thread workspace preparation intents survive reopen and fail closed by exact group and channel", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-workspace-intent-"));
  const stateDir = join(root, "state");
  const preparing: ThreadWorkspaceIntentInput = {
    id: "intent-preparing",
    botUid,
    groupId: "group-a",
    status: "preparing",
    channelIds: [],
    error: null,
  };
  try {
    const first = await openChannelStore(stateDir);
    try {
      first.saveThreadWorkspaceIntent(preparing);
      first.addThreadWorkspaceIntentChannel(botUid, preparing.id, "group-a____created-thread");
      first.addThreadWorkspaceIntentChannel(botUid, preparing.id, "group-a____created-thread");
      first.saveThreadWorkspaceIntent(preparing);
    } finally {
      first.close();
    }

    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.getThreadWorkspaceIntent(botUid, preparing.id)).toEqual({
        id: preparing.id,
        botUid,
        groupId: "group-a",
        kind: "workspace",
        creatorChannelType: null,
        creatorChannelId: null,
        status: "preparing",
        channelIds: ["group-a____created-thread"],
        error: null,
      });
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____created-thread" },
      )).toBe(true);
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____not-yet-mapped" },
      )).toBe(true);
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-b____unrelated" },
      )).toBe(false);

      reopened.saveThreadWorkspace(sharedWorkspace(
        "group-a____created-thread",
        null,
        "failed",
        [],
        "one project was not confirmed",
      ));
      reopened.failThreadWorkspaceIntent(botUid, preparing.id, "one project was not confirmed");
      expect(reopened.getThreadWorkspaceIntent(botUid, preparing.id)).toMatchObject({
        status: "failed",
        error: "one project was not confirmed",
      });
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____created-thread" },
      )).toBe(false);
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____not-yet-mapped" },
      )).toBe(false);

      const completeTarget = { channelType: 5 as const, channelId: "group-a____complete" };
      reopened.saveThreadWorkspaceIntent({
        ...preparing,
        id: "intent-complete",
        channelIds: [completeTarget.channelId],
      });
      reopened.saveThreadWorkspace(sharedWorkspace(completeTarget.channelId, null, "ready", []));
      reopened.completeThreadWorkspaceIntent(botUid, "intent-complete");
      expect(reopened.isThreadWorkspaceIntentBlocked(botUid, completeTarget)).toBe(false);
      expect(reopened.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____complete-unknown" },
      )).toBe(false);
    } finally {
      reopened.close();
    }

    const failedReopen = await openChannelStore(stateDir);
    try {
      expect(failedReopen.getThreadWorkspaceIntent(botUid, preparing.id)).toMatchObject({
        status: "failed",
        channelIds: ["group-a____created-thread"],
      });
      expect(failedReopen.isThreadWorkspaceIntentBlocked(
        botUid,
        { channelType: 5, channelId: "group-a____created-thread" },
      )).toBe(false);
    } finally {
      failedReopen.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("a definitively failed thread-create intent does not block unrelated children in its group", async () => {
  await withStore(async (store) => {
    store.saveThreadWorkspaceIntent({
      id: "known-create-rejection",
      botUid,
      groupId: "group-a",
      kind: "thread-create",
      creatorChannelType: 2,
      creatorChannelId: "group-a",
      status: "preparing",
      channelIds: [],
    });
    store.failThreadWorkspaceIntent(botUid, "known-create-rejection", "remote rejected the request");

    expect(store.getThreadWorkspaceIntent(botUid, "known-create-rejection")).toMatchObject({
      status: "failed",
      channelIds: [],
    });
    expect(store.isThreadWorkspaceIntentBlocked(
      botUid,
      { channelType: 5, channelId: "group-a____existing-child" },
    )).toBe(false);
  });
});

test("unidentified thread creation does not block an established child after reopen, but its own mapped creation does", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-thread-intent-scope-"));
  const target = { channelType: 5 as const, channelId: "group-a____ready" };
  try {
    const first = await openChannelStore(root);
    first.ensureBinding({
      botUid,
      ...target,
      sessionId: "ready-session",
      cwd: root,
      sessionFile: null,
    });
    first.saveThreadWorkspaceIntent({
      id: "unknown-create",
      botUid,
      groupId: "group-a",
      kind: "thread-create",
      creatorChannelType: 2,
      creatorChannelId: "group-a",
      status: "preparing",
      channelIds: [],
    });
    first.close();

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.isThreadWorkspaceIntentBlocked(botUid, target)).toBe(false);
      expect(reopened.isThreadWorkspaceIntentBlocked(botUid, {
        channelType: 5, channelId: "group-a____unidentified",
      })).toBe(true);
      expect(reopened.getThreadWorkspaceIntent(botUid, "unknown-create")?.status).toBe("preparing");
      reopened.addThreadWorkspaceIntentChannel(botUid, "unknown-create", target.channelId);
      expect(reopened.isThreadWorkspaceIntentBlocked(botUid, target)).toBe(true);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("thread workspace intent finalizes after a crash only when every mapped ownership row is terminal", async () => {
  await withStore(async (store) => {
    const readyTarget = { channelType: 5 as const, channelId: "group-a____ready-after-crash" };
    const failedTarget = { channelType: 5 as const, channelId: "group-a____failed-after-crash" };
    store.saveThreadWorkspaceIntent({
      id: "intent-ready-after-crash",
      botUid,
      groupId: "group-a",
      status: "preparing",
      channelIds: [readyTarget.channelId],
    });
    store.saveThreadWorkspace(sharedWorkspace(readyTarget.channelId, null, "ready", []));
    expect(store.isThreadWorkspaceIntentBlocked(botUid, readyTarget)).toBe(false);
    expect(store.getThreadWorkspaceIntent(botUid, "intent-ready-after-crash")?.status).toBe("complete");
    const unconfirmedTarget = { channelType: 5 as const, channelId: "group-a____unconfirmed-after-crash" };
    store.saveThreadWorkspaceIntent({
      id: "intent-unconfirmed-after-crash",
      botUid,
      groupId: "group-a",
      status: "preparing",
      channelIds: [unconfirmedTarget.channelId],
    });
    store.saveThreadWorkspace(sharedWorkspace(
      unconfirmedTarget.channelId,
      null,
      "ready",
      [{
        name: "planned",
        repo: "/repos/planned",
        path: "/thread-entry/planned",
        branch: "thread-planned",
        created: false,
      }],
    ));
    expect(store.isThreadWorkspaceIntentBlocked(botUid, unconfirmedTarget)).toBe(false);
    expect(store.getThreadWorkspaceIntent(botUid, "intent-unconfirmed-after-crash")?.status).toBe("preparing");


    store.saveThreadWorkspaceIntent({
      id: "intent-failed-after-crash",
      botUid,
      groupId: "group-a",
      status: "preparing",
      channelIds: [failedTarget.channelId],
    });
    store.saveThreadWorkspace(sharedWorkspace(failedTarget.channelId, null, "failed", [], "unconfirmed project"));
    expect(store.isThreadWorkspaceIntentBlocked(botUid, failedTarget)).toBe(false);
    expect(store.getThreadWorkspaceIntent(botUid, "intent-failed-after-crash")).toMatchObject({
      status: "failed",
      error: "unconfirmed project",
    });
  });
});
