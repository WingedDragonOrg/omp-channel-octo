import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, ThreadSummary } from "../src/contracts.ts";
import type { ThreadForkRecord, WorkerLedgerRecord } from "../src/store/store-records.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";

const botUid = "fork-store-bot";
const groupId = "fork-store-group";
const sourceTarget = { channelType: 5, channelId: `${groupId}____source` } as const satisfies ChatTarget;
const forkThread: ThreadSummary = {
  shortId: "fork-short",
  groupId,
  channelId: `${groupId}____fork-short`,
  name: "Forked conversation",
  creatorUid: "owner-1",
  status: 1,
  memberCount: 1,
  messageCount: 0,
  createdAt: "2026-11-03T10:00:00Z",
  updatedAt: "2026-11-03T10:00:00Z",
};
const forkTarget: ChatTarget = { channelType: 5, channelId: forkThread.channelId };

type Store = ChannelStore;

function forkRecord(overrides: Partial<ThreadForkRecord> = {}): ThreadForkRecord {
  return {
    id: "fork-op-1",
    botUid,
    sourceChannelType: sourceTarget.channelType,
    sourceChannelId: sourceTarget.channelId,
    sourceSessionId: "source-session-1",
    sourceSessionFile: "/sessions/source-session-1.jsonl",
    cwd: "/work/shared-project",
    groupId,
    name: "Forked conversation",
    preparationDir: "/state/forks/fork-op-1",
    phase: "preparing",
    preparationStarted: false,
    sessionId: null,
    sessionFile: null,
    thread: null,
    error: null,
    ...overrides,
  };
}

function forkIntent(store: Store, record: ThreadForkRecord): void {
  if (record.thread === null) throw new Error("fork intent fixture requires the known target thread");
  store.saveThreadWorkspaceIntent({
    id: record.id,
    botUid: record.botUid,
    groupId: record.groupId,
    status: "preparing",
    channelIds: [record.thread.channelId],
  });
}

async function inTemporaryDirectory(run: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-store-fork-"));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("fork operation id and source session identity cannot be rewritten by an upsert", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const initial = forkRecord();
      store.saveThreadFork(initial);
      store.saveThreadFork({
        ...initial,
        phase: "prepared",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
      });

      const persisted = store.getThreadFork(botUid, initial.id);
      expect(persisted).toMatchObject({
        id: initial.id,
        botUid,
        sourceChannelType: sourceTarget.channelType,
        sourceChannelId: sourceTarget.channelId,
        sourceSessionId: "source-session-1",
        sourceSessionFile: "/sessions/source-session-1.jsonl",
        cwd: "/work/shared-project",
        phase: "prepared",
      });
      expect(() => store.saveThreadFork({
        ...persisted!,
        sourceChannelId: "some-other-source____topic",
        sourceSessionId: "other-source-session",
        sourceSessionFile: "/sessions/other-source-session.jsonl",
      })).toThrow();
      expect(store.getThreadFork(botUid, initial.id)).toEqual(persisted);
    } finally {
      store.close();
    }
  });
});

test("fork phase and native session resources survive store reopen", async () => {
  await inTemporaryDirectory(async (root) => {
    let store = await openChannelStore(root);
    try {
      const initial = forkRecord({ preparationStarted: true });
      store.saveThreadFork(initial);
      store.saveThreadFork({
        ...initial,
        phase: "prepared",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
      });
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    try {
      expect(store.getThreadFork(botUid, "fork-op-1")).toMatchObject({
        id: "fork-op-1",
        preparationStarted: true,
        phase: "prepared",
        sourceSessionId: "source-session-1",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: null,
        error: null,
      });
      store.saveThreadFork({
        ...store.getThreadFork(botUid, "fork-op-1")!,
        phase: "remote-created",
        thread: forkThread,
      });
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    try {
      expect(store.getThreadFork(botUid, "fork-op-1")).toMatchObject({
        phase: "remote-created",
        preparationStarted: true,
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      expect(store.listThreadForks(botUid)).toHaveLength(1);
    } finally {
      store.close();
    }
  });
});

test("an incomplete fork target stays blocked despite a persisted binding", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      forkIntent(store, remoteCreated);
      store.saveThreadFork(remoteCreated);
      store.ensureBinding({
        botUid,
        ...forkTarget,
        sessionId: "fork-session-1",
        cwd: "/work/shared-project",
        sessionFile: "/sessions/fork-session-1.jsonl",
      });

      expect(store.getThreadForkByTarget(botUid, forkTarget)).toMatchObject({
        id: "fork-op-1",
        phase: "remote-created",
        thread: forkThread,
      });
      expect(store.isThreadWorkspaceIntentBlocked(botUid, forkTarget)).toBe(true);
      expect(store.getThreadListen(botUid, forkTarget)).toBeNull();
    } finally {
      store.close();
    }
  });
});
test("fork completion refuses to overwrite a conflicting target binding", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      forkIntent(store, remoteCreated);
      store.saveThreadFork(remoteCreated);
      store.ensureBinding({
        botUid,
        ...forkTarget,
        sessionId: "unrelated-session",
        cwd: "/work/unrelated",
        sessionFile: "/sessions/unrelated.jsonl",
      });
      const originalBinding = store.getBinding(botUid, forkTarget);

      expect(() => store.completeThreadFork(botUid, remoteCreated.id)).toThrow();
      expect(store.getBinding(botUid, forkTarget)).toEqual(originalBinding);
      expect(store.getThreadListen(botUid, forkTarget)).toBeNull();
      expect(store.getThreadFork(botUid, remoteCreated.id)?.phase).toBe("remote-created");
      expect(store.getThreadWorkspaceIntent(botUid, remoteCreated.id)?.status).toBe("preparing");
    } finally {
      store.close();
    }
  });
});


test("fork completion fails closed without its matching persisted intent", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      store.saveThreadFork(remoteCreated);

      expect(() => store.completeThreadFork(botUid, remoteCreated.id)).toThrow();
      expect(store.getBinding(botUid, forkTarget)).toBeNull();
      expect(store.getThreadListen(botUid, forkTarget)).toBeNull();
      expect(store.getThreadFork(botUid, remoteCreated.id)?.phase).toBe("remote-created");
    } finally {
      store.close();
    }
  });
});

test("fork completion recovers legacy null creator metadata and atomically publishes binding, listen, and intent", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      forkIntent(store, remoteCreated);
      store.saveThreadFork(remoteCreated);

      store.completeThreadFork(botUid, remoteCreated.id);

      expect(store.getThreadFork(botUid, remoteCreated.id)).toMatchObject({
        phase: "ready",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
        error: null,
      });
      expect(store.getThreadForkByTarget(botUid, forkTarget)).toMatchObject({
        id: remoteCreated.id,
        phase: "ready",
      });
      expect(store.getBinding(botUid, forkTarget)).toMatchObject({
        botUid,
        ...forkTarget,
        sessionId: "fork-session-1",
        cwd: "/work/shared-project",
        sessionFile: "/sessions/fork-session-1.jsonl",
      });
      expect(store.getThreadListen(botUid, forkTarget)).toMatchObject({
        botUid,
        channelId: forkThread.channelId,
        creatorChannelType: sourceTarget.channelType,
        creatorChannelId: sourceTarget.channelId,
        threadName: forkThread.name,
        listen: "merge",
        mergePending: false,
      });
      expect(store.getThreadWorkspaceIntent(botUid, remoteCreated.id)).toMatchObject({
        id: remoteCreated.id,
        groupId,
        creatorChannelType: sourceTarget.channelType,
        creatorChannelId: sourceTarget.channelId,
        status: "complete",
        channelIds: [forkThread.channelId],
        error: null,
      });
      expect(store.getThreadWorkspace(botUid, forkTarget)).toBeNull();
      expect(store.isThreadWorkspaceIntentBlocked(botUid, forkTarget)).toBe(false);

      store.completeThreadFork(botUid, remoteCreated.id);
      expect(store.getBinding(botUid, forkTarget)?.sessionId).toBe("fork-session-1");
      expect(store.getThreadListen(botUid, forkTarget)?.listen).toBe("merge");
    } finally {
      store.close();
    }
  });
});

test("a failed fork intent completion rolls back binding, listen, and ready publication", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      forkIntent(store, remoteCreated);
      store.saveThreadFork(remoteCreated);

      const triggerDb = new Database(join(root, "channel.sqlite"));
      try {
        triggerDb.exec(`
          CREATE TRIGGER reject_fork_intent_completion
          BEFORE UPDATE OF status ON thread_workspace_intents
          WHEN NEW.id = 'fork-op-1' AND NEW.status = 'complete'
          BEGIN
            SELECT RAISE(ABORT, 'injected fork intent completion failure');
          END;
        `);
      } finally {
        triggerDb.close();
      }

      expect(() => store.completeThreadFork(botUid, remoteCreated.id)).toThrow();
      expect(store.getBinding(botUid, forkTarget)).toBeNull();
      expect(store.getThreadListen(botUid, forkTarget)).toBeNull();
      expect(store.getThreadFork(botUid, remoteCreated.id)?.phase).toBe("remote-created");
      expect(store.getThreadWorkspaceIntent(botUid, remoteCreated.id)?.status).toBe("preparing");
    } finally {
      store.close();
    }
  });
});

test("fork completion does not copy source workspace ownership, model, context, questions, or goal", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const remoteCreated = forkRecord({
        phase: "remote-created",
        sessionId: "fork-session-1",
        sessionFile: "/sessions/fork-session-1.jsonl",
        thread: forkThread,
      });
      forkIntent(store, remoteCreated);
      store.saveThreadFork(remoteCreated);

      store.ensureBinding({
        botUid,
        ...sourceTarget,
        sessionId: "source-session-1",
        cwd: "/work/shared-project",
        sessionFile: "/sessions/source-session-1.jsonl",
      });
      store.saveThreadWorkspace({
        botUid,
        channelType: 5,
        channelId: sourceTarget.channelId,
        layout: "shared",
        status: "ready",
        entryPath: "/work/shared-project",
        projects: [],
        error: null,
      });
      store.setChatModel(botUid, sourceTarget, "provider/source-model");
      store.prepareContextInjection(botUid, sourceTarget, "source-session-1", 7, "source-context-attempt");
      store.createQuestion({
        id: "source-question",
        taskId: "source-task",
        botUid,
        ...sourceTarget,
        payload: "Source-only question",
      });
      store.saveGoalSessionProgress(botUid, sourceTarget, "source-session-1", {
        id: "source-goal",
        objective: "Source-only goal",
        status: "active",
        tokensUsed: 17,
        timeUsedSeconds: 9,
        tokenBudget: 100,
      });

      store.completeThreadFork(botUid, remoteCreated.id);

      expect(store.getThreadWorkspace(botUid, sourceTarget)).toMatchObject({ status: "ready" });
      expect(store.getThreadWorkspace(botUid, forkTarget)).toBeNull();
      expect(store.getChatModel(botUid, sourceTarget)).toBe("provider/source-model");
      expect(store.getChatModel(botUid, forkTarget)).toBeNull();
      expect(store.getContextLedger(botUid, sourceTarget, "source-session-1")).toMatchObject({
        sessionId: "source-session-1",
        version: 7,
      });
      expect(store.getContextLedger(botUid, forkTarget, "fork-session-1")).toBeNull();
      expect(store.listPendingQuestions(botUid, sourceTarget).map((question) => question.id)).toEqual(["source-question"]);
      expect(store.listPendingQuestions(botUid, forkTarget)).toEqual([]);
      expect(store.getGoalSession(botUid, sourceTarget, "source-session-1")).toMatchObject({
        sessionId: "source-session-1",
        objective: "Source-only goal",
      });
      expect(store.getGoalSession(botUid, forkTarget, "fork-session-1")).toBeNull();
    } finally {
      store.close();
    }
  });
});

test("ordinary workspace intents still block unknown allocations but allow an established binding", async () => {
  await inTemporaryDirectory(async (root) => {
    const store = await openChannelStore(root);
    try {
      const sibling: ChatTarget = { channelType: 5, channelId: `${groupId}____unknown-sibling` };
      store.saveThreadWorkspaceIntent({
        id: "ordinary-workspace-op",
        botUid,
        groupId,
        status: "preparing",
        channelIds: [],
      });
      expect(store.isThreadWorkspaceIntentBlocked(botUid, sibling)).toBe(true);

      store.ensureBinding({
        botUid,
        ...sibling,
        sessionId: "pre-existing-session",
        cwd: "/work/pre-existing",
        sessionFile: "/sessions/pre-existing.jsonl",
      });
      expect(store.isThreadWorkspaceIntentBlocked(botUid, sibling)).toBe(false);
    } finally {
      store.close();
    }
  });
});

test("legacy schema migration adds fork storage and preparation markers idempotently", async () => {
  await inTemporaryDirectory(async (root) => {
    const legacy = new Database(join(root, "channel.sqlite"));
    try {
      legacy.exec(`
        CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        INSERT INTO meta (key, value) VALUES ('legacy-marker', 'preserved');
        CREATE TABLE owners (
          bot_uid TEXT PRIMARY KEY NOT NULL,
          owner_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO owners (bot_uid, owner_id, created_at, updated_at)
        VALUES ('legacy-bot', 'legacy-owner', 1, 1);
        CREATE TABLE chats (
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL,
          channel_id TEXT NOT NULL,
          session_id TEXT NOT NULL,
          cwd TEXT NOT NULL,
          session_file TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(bot_uid, channel_type, channel_id)
        );
        INSERT INTO chats(
          bot_uid, channel_type, channel_id, session_id, cwd, session_file, created_at, updated_at
        ) VALUES('legacy-bot', 2, 'legacy-group', 'legacy-session', '/legacy/cwd', '/legacy/session.jsonl', 2, 3);
        CREATE TABLE thread_workspace_intents (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          group_id TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('preparing', 'failed', 'complete')),
          channel_ids TEXT NOT NULL,
          error TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO thread_workspace_intents(
          id, bot_uid, group_id, status, channel_ids, error, created_at, updated_at
        ) VALUES('legacy-intent', 'legacy-bot', 'legacy-group', 'failed', '["legacy-group____old-thread"]', 'old failure', 4, 5);
        CREATE TABLE thread_forks (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          source_channel_type INTEGER NOT NULL CHECK(source_channel_type IN (2, 5)),
          source_channel_id TEXT NOT NULL,
          source_session_id TEXT NOT NULL,
          source_session_file TEXT NOT NULL,
          cwd TEXT NOT NULL,
          group_id TEXT NOT NULL,
          name TEXT NOT NULL,
          preparation_dir TEXT NOT NULL,
          phase TEXT NOT NULL CHECK(phase IN ('preparing', 'prepared', 'creating', 'remote-created', 'ready', 'failed')),
          session_id TEXT,
          session_file TEXT,
          thread_channel_id TEXT,
          thread_json TEXT,
          error TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          CHECK((thread_channel_id IS NULL) = (thread_json IS NULL))
        );
        INSERT INTO thread_forks(
          id, bot_uid, source_channel_type, source_channel_id, source_session_id, source_session_file,
          cwd, group_id, name, preparation_dir, phase, session_id, session_file, thread_channel_id,
          thread_json, error, created_at, updated_at
        ) VALUES(
          'legacy-pre-marker', 'legacy-bot', 2, 'legacy-group', 'legacy-session',
          '/legacy/session.jsonl', '/legacy/cwd', 'legacy-group', 'Legacy fork',
          '/legacy/state/forks/legacy-pre-marker', 'preparing', NULL, NULL, NULL, NULL, NULL, 6, 7
        );
      `);
    } finally {
      legacy.close();
    }

    let store = await openChannelStore(root);
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      expect(store.getBinding("legacy-bot", { channelType: 2, channelId: "legacy-group" })).toMatchObject({
        sessionId: "legacy-session",
        cwd: "/legacy/cwd",
        sessionFile: "/legacy/session.jsonl",
      });
      expect(store.getThreadWorkspaceIntent("legacy-bot", "legacy-intent")).toMatchObject({
        id: "legacy-intent",
        status: "failed",
        channelIds: ["legacy-group____old-thread"],
        error: "old failure",
      });
      expect(store.getThreadFork("legacy-bot", "legacy-pre-marker")).toMatchObject({
        id: "legacy-pre-marker",
        phase: "preparing",
        preparationStarted: false,
        sourceSessionFile: "/legacy/session.jsonl",
      });
      store.saveThreadFork(forkRecord({
        id: "legacy-fork-op",
        botUid: "legacy-bot",
        sourceChannelType: 2,
        sourceChannelId: "legacy-group",
        sourceSessionId: "legacy-session",
        sourceSessionFile: "/legacy/session.jsonl",
        cwd: "/legacy/cwd",
        groupId: "legacy-group",
        preparationDir: "/legacy/state/forks/legacy-fork-op",
      }));
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      expect(store.getBinding("legacy-bot", { channelType: 2, channelId: "legacy-group" })?.sessionId)
        .toBe("legacy-session");
      expect(store.getThreadWorkspaceIntent("legacy-bot", "legacy-intent")?.error).toBe("old failure");
      expect(store.getThreadFork("legacy-bot", "legacy-fork-op")).toMatchObject({
        id: "legacy-fork-op",
        phase: "preparing",
        sourceSessionId: "legacy-session",
      });
      expect(store.getThreadFork("legacy-bot", "legacy-pre-marker")).toMatchObject({
        id: "legacy-pre-marker",
        preparationStarted: false,
        sourceSessionId: "legacy-session",
      });
    } finally {
      if (store) store.close();
    }

    const migrated = new Database(join(root, "channel.sqlite"));
    try {
      expect(migrated.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get()).toEqual({
        value: "preserved",
      });
      expect(migrated.query("SELECT owner_id FROM owners WHERE bot_uid = 'legacy-bot'").get()).toEqual({
        owner_id: "legacy-owner",
      });
      expect(migrated.query("SELECT session_id FROM chats WHERE bot_uid = 'legacy-bot' AND channel_id = 'legacy-group'").get())
        .toEqual({ session_id: "legacy-session" });
      expect(migrated.query("PRAGMA table_info(thread_forks)").all()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: "preparation_started", notnull: 1, dflt_value: "0" }),
        ]),
      );
      expect(migrated.query("SELECT created_at, updated_at FROM thread_forks WHERE id = 'legacy-pre-marker'").get())
        .toEqual({ created_at: 6, updated_at: 7 });
    } finally {
      migrated.close();
    }
  });
});
test("legacy worker ledger rows survive nullable session file migration and stopped prep rows reopen", async () => {
  await inTemporaryDirectory(async (root) => {
    const legacy = new Database(join(root, "channel.sqlite"));
    try {
      legacy.exec(`
        CREATE TABLE worker_ledger (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          session_id TEXT NOT NULL,
          session_file TEXT NOT NULL,
          pid INTEGER NOT NULL,
          pgid INTEGER NOT NULL,
          identity TEXT NOT NULL,
          cwd TEXT NOT NULL,
          move_target_cwd TEXT,
          state TEXT NOT NULL CHECK(state IN ('running', 'stopping', 'stopped', 'unknown')),
          updated_at INTEGER NOT NULL
        );
        INSERT INTO worker_ledger(
          id, bot_uid, session_id, session_file, pid, pgid, identity, cwd, move_target_cwd, state, updated_at
        ) VALUES
          ('legacy-running', 'legacy-worker-bot', 'legacy-session', '/legacy/session.jsonl', 61001, 61001,
           'pid=61001;start=legacy-running', '/legacy/cwd', '/legacy/move-target', 'running', 101),
          ('legacy-stopped', 'legacy-worker-bot', 'old-session', '/legacy/stopped.jsonl', 61002, 61002,
           'pid=61002;start=legacy-stopped', '/legacy/old-cwd', NULL, 'stopped', 102),
          ('other-bot-worker', 'other-worker-bot', 'other-session', '/other/session.jsonl', 61003, 61003,
           'pid=61003;start=other-worker', '/other/cwd', NULL, 'unknown', 103);
      `);
    } finally {
      legacy.close();
    }

    let store = await openChannelStore(root);
    const preparationWorker: WorkerLedgerRecord = {
      id: "fork-preparation-worker",
      botUid: "fork-preparation-bot",
      sessionId: "fork-op-1",
      sessionFile: null,
      pid: 62001,
      pgid: 62001,
      identity: "pid=62001;start=fork-preparation",
      cwd: "/work/shared-project",
      moveTargetCwd: null,
      state: "running",
    };
    try {
      expect(store.getWorkerLedger("legacy-worker-bot", "legacy-running")).toEqual({
        id: "legacy-running",
        botUid: "legacy-worker-bot",
        sessionId: "legacy-session",
        sessionFile: "/legacy/session.jsonl",
        pid: 61001,
        pgid: 61001,
        identity: "pid=61001;start=legacy-running",
        cwd: "/legacy/cwd",
        moveTargetCwd: "/legacy/move-target",
        state: "running",
      });
      expect(store.getWorkerLedger("legacy-worker-bot", "legacy-stopped")).toMatchObject({
        id: "legacy-stopped",
        identity: "pid=61002;start=legacy-stopped",
        moveTargetCwd: null,
        state: "stopped",
      });
      expect(store.getWorkerLedger("legacy-worker-bot", "other-bot-worker")).toBeNull();
      expect(store.listWorkerLedger("legacy-worker-bot").map((worker) => worker.id)).toEqual(["legacy-running"]);

      store.recordWorker(preparationWorker);
      store.markWorkerStopped(preparationWorker.id);
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    try {
      expect(store.getWorkerLedger("fork-preparation-bot", "fork-preparation-worker")).toEqual({
        ...preparationWorker,
        state: "stopped",
      });
      expect(store.getWorkerLedger("other-worker-bot", "fork-preparation-worker")).toBeNull();
      expect(store.getWorkerLedger("legacy-worker-bot", "legacy-running")).toMatchObject({
        sessionFile: "/legacy/session.jsonl",
        identity: "pid=61001;start=legacy-running",
        moveTargetCwd: "/legacy/move-target",
        state: "running",
      });
      expect(store.getWorkerLedger("legacy-worker-bot", "legacy-stopped")?.state).toBe("stopped");
    } finally {
      store.close();
    }
    const migrated = new Database(join(root, "channel.sqlite"));
    try {
      expect(migrated.query("PRAGMA table_info(worker_ledger)").all()).toEqual(
        expect.arrayContaining([expect.objectContaining({ name: "session_file", notnull: 0 })]),
      );
      expect(migrated.query("SELECT updated_at FROM worker_ledger WHERE id = 'legacy-running'").get())
        .toEqual({ updated_at: 101 });
      expect(migrated.query("SELECT updated_at FROM worker_ledger WHERE id = 'legacy-stopped'").get())
        .toEqual({ updated_at: 102 });
    } finally {
      migrated.close();
    }
  });
});

