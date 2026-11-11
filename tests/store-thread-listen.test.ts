import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { openChannelStore } from "../src/store/index.ts";

const botUid = "thread-listen-bot";
const thread: ChatTarget = { channelType: 5, channelId: "listen-parent____listen-short" };

function task(id: string, target: ChatTarget, state: TaskRecord["state"] = "queued"): TaskRecord {
  return {
    ...target,
    id: `task-${id}`,
    botUid,
    messageId: `message-${id}`,
    senderId: "owner",
    text: `task ${id}`,
    sessionId: `session-${id}`,
    cwd: "/tmp/thread-listen",
    state,
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
}

function claim(id: string, target: ChatTarget): InboundClaim {
  return {
    botUid,
    target,
    messageId: `message-${id}`,
    senderId: "owner",
    text: `task ${id}`,
    supported: true,
    kind: "task",
  };
}

test("thread listen policy persists without changing its creator target", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-listen-"));
  let store = await openChannelStore(root);
  try {
    const initial = store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
    });
    expect(initial).toEqual({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
      mergePending: false,
    });

    expect(store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 5,
      creatorChannelId: "different-creator",
      threadName: "Replacement name",
      listen: "off",
    })).toEqual(initial);
    expect(store.setThreadListen(botUid, thread, "off")).toEqual({ ...initial, listen: "off" });
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getThreadListen(botUid, thread)).toEqual({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "off",
      mergePending: false,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("creation publishes its requested policy over an inbound fallback without overwriting later changes", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-listen-race-"));
  const store = await openChannelStore(root);
  try {
    const fallback = {
      botUid, channelId: thread.channelId, creatorChannelType: 2 as const,
      creatorChannelId: "listen-parent", threadName: "", listen: "merge" as const,
    };
    store.ensureThreadListen(fallback);
    const created = { ...fallback, threadName: "Created", listen: "task" as const };
    expect(store.recordCreatedThreadListen(created)).toMatchObject({ threadName: "Created", listen: "task" });
    store.setThreadListen(botUid, thread, "off");
    expect(store.recordCreatedThreadListen(created)).toMatchObject({ threadName: "Created", listen: "off" });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("merge intent suppresses current and newly accepted tasks; successful events are stable and durable", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-merge-event-"));
  let store = await openChannelStore(root);
  try {
    store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
    });
    expect(store.acceptTask(claim("queued", thread), task("queued", thread))).toBe(true);
    expect(store.acceptTask(claim("running", thread), task("running", thread))).toBe(true);
    expect(store.markStarted("task-running")).toBe(true);

    store.beginThreadMerge(botUid, thread);
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(true);
    expect(store.getTask("task-queued")?.relaySuppressed).toBe(true);
    expect(store.getTask("task-running")?.relaySuppressed).toBe(true);

    expect(store.acceptTask(claim("repair", thread), task("repair", thread))).toBe(true);
    expect(store.getTask("task-repair")?.relaySuppressed).toBe(true);

    const event = {
      id: "merge:listen-parent____listen-short:abc123",
      botUid,
      channelId: thread.channelId,
      targetBranch: "main",
      commit: "abc123",
      removed: true,
      archive: "scheduled" as const,
    };
    store.saveThreadMergeEvent(event);
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(false);
    expect(store.getTask("task-repair")?.relaySuppressed).toBe(true);
    expect(store.listThreadMergeEvents(botUid)).toEqual([event]);

    store.saveThreadMergeEvent({ ...event, targetBranch: "changed", removed: false, archive: "not-scheduled" });
    expect(store.listThreadMergeEvents(botUid)).toEqual([event]);
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(false);
    expect(store.getTask("task-queued")?.relaySuppressed).toBe(true);
    expect(store.getTask("task-running")?.relaySuppressed).toBe(true);
    expect(store.getTask("task-repair")?.relaySuppressed).toBe(true);
    expect(store.listThreadMergeEvents(botUid)).toEqual([{
      id: "merge:listen-parent____listen-short:abc123",
      botUid,
      channelId: thread.channelId,
      targetBranch: "main",
      commit: "abc123",
      removed: true,
      archive: "scheduled",
    }]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("replayed merge success cannot suppress new tasks or clear a later merge intent", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-event-replay-"));
  const store = await openChannelStore(root);
  try {
    store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
    });
    const event = {
      id: "merge:first",
      botUid,
      channelId: thread.channelId,
      targetBranch: "main",
      commit: "first-commit",
      removed: false,
      archive: "not-scheduled" as const,
    };
    store.saveThreadMergeEvent(event);
    store.acceptTask(claim("after-merge", thread), task("after-merge", thread));
    store.saveThreadMergeEvent(event);
    expect(store.getTask("task-after-merge")?.relaySuppressed).toBe(false);

    store.beginThreadMerge(botUid, thread);
    store.saveThreadMergeEvent(event);
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(true);
    store.acceptTask(claim("later-repair", thread), task("later-repair", thread));
    expect(store.getTask("task-later-repair")?.relaySuppressed).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("task relay suppression persists for only the selected task and leaves merge pending unchanged", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-task-relay-suppression-"));
  let store = await openChannelStore(root);
  try {
    store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
    });
    store.acceptTask(claim("first", thread), task("first", thread));
    store.acceptTask(claim("second", thread), task("second", thread));
    const policyBefore = store.getThreadListen(botUid, thread);

    store.suppressTaskRelay("task-first");

    expect(store.getTask("task-first")?.relaySuppressed).toBe(true);
    expect(store.getTask("task-second")?.relaySuppressed).toBe(false);
    expect(store.getThreadListen(botUid, thread)).toEqual(policyBefore);
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getTask("task-first")?.relaySuppressed).toBe(true);
    expect(store.getTask("task-second")?.relaySuppressed).toBe(false);
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("merge event receipts distinguish pending, dispatch intent, and delivered events by event id", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-event-receipt-"));
  let store = await openChannelStore(root);
  const events = [
    {
      id: "merge-receipt-intent",
      botUid,
      channelId: thread.channelId,
      targetBranch: "main",
      commit: "intent-commit",
      removed: false,
      archive: "not-scheduled" as const,
    },
    {
      id: "merge-receipt-pending",
      botUid,
      channelId: thread.channelId,
      targetBranch: "main",
      commit: "pending-commit",
      removed: true,
      archive: "scheduled" as const,
    },
  ];
  try {
    store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "merge",
    });
    for (const event of events) store.saveThreadMergeEvent(event);
    expect(store.listPendingThreadMergeEvents(botUid)).toEqual(events);
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(true);
    expect(store.claimThreadMergeDispatchIntent(events[0]!.id)).toBe(true);
    expect(store.claimThreadMergeDispatchIntent(events[0]!.id)).toBe(false);
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
    expect(store.listPendingThreadMergeEvents(botUid)).toEqual([events[1]!]);

    store.markThreadMergeEventDelivered(events[1]!.id);
    expect(store.isThreadMergeEventPending(events[1]!.id)).toBe(false);
    expect(store.listPendingThreadMergeEvents(botUid)).toEqual([]);
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.listPendingThreadMergeEvents(botUid)).toEqual([]);
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
    expect(store.listThreadMergeEvents(botUid)).toEqual(events);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("unfinished merge intent survives restart and suppresses repair work accepted afterward", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-pending-restart-"));
  let store = await openChannelStore(root);
  try {
    store.ensureThreadListen({
      botUid,
      channelId: thread.channelId,
      creatorChannelType: 2,
      creatorChannelId: "listen-parent",
      threadName: "Listen thread",
      listen: "task",
    });
    store.beginThreadMerge(botUid, thread);
    expect(store.setThreadListen(botUid, thread, "off")).toMatchObject({ listen: "off", mergePending: true });
    expect(store.getThreadListen(botUid, thread)?.mergePending).toBe(true);
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getThreadListen(botUid, thread)).toMatchObject({ listen: "off", mergePending: true });
    expect(store.listThreadMergeEvents(botUid)).toEqual([]);
    expect(store.acceptTask(
      claim("repair-after-restart", thread),
      task("repair-after-restart", thread),
    )).toBe(true);
    expect(store.getTask("task-repair-after-restart")?.relaySuppressed).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("legacy task relays migrate to merge policies and relay columns are removed", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-listen-migration-"));
  const databasePath = join(root, "channel.sqlite");
  try {
    const legacyDb = new Database(databasePath);
    try {
      legacyDb.exec(`
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
        CREATE TABLE thread_workspaces (
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL,
          channel_id TEXT NOT NULL,
          layout TEXT NOT NULL,
          status TEXT NOT NULL,
          entry_path TEXT,
          projects TEXT NOT NULL,
          error TEXT,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(bot_uid, channel_id)
        );
        CREATE TABLE thread_merge_events (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          channel_id TEXT NOT NULL,
          target_branch TEXT NOT NULL,
          commit_hash TEXT,
          removed INTEGER NOT NULL,
          archive TEXT NOT NULL
        );
        INSERT INTO thread_merge_events (
          id, bot_uid, channel_id, target_branch, commit_hash, removed, archive
        ) VALUES (
          'legacy-merge-event', 'legacy-listen-bot', 'legacy-parent____legacy-short',
          'main', 'legacy-commit', 1, 'scheduled'
        );
        CREATE TABLE tasks (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL,
          channel_id TEXT NOT NULL,
          message_id TEXT NOT NULL,
          sender_id TEXT NOT NULL,
          text TEXT NOT NULL,
          session_id TEXT NOT NULL,
          cwd TEXT NOT NULL,
          state TEXT NOT NULL,
          accepted_at INTEGER NOT NULL,
          started_at INTEGER,
          finished_at INTEGER,
          result_text TEXT,
          error_text TEXT,
          message_seq INTEGER NOT NULL DEFAULT 0,
          history_eligible INTEGER NOT NULL DEFAULT 1,
          prompt_started INTEGER NOT NULL DEFAULT 0,
          prompt_accepted INTEGER NOT NULL DEFAULT 0,
          stop_intent TEXT,
          relay_channel_type INTEGER,
          relay_channel_id TEXT,
          relay_thread_name TEXT
        );
        INSERT INTO tasks (
          id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd,
          state, accepted_at, relay_channel_type, relay_channel_id, relay_thread_name
        ) VALUES (
          'legacy-notify-task', 'legacy-listen-bot', 5, 'legacy-parent____legacy-short',
          'legacy-message', 'legacy-owner', 'preserve this task', 'legacy-session', '/tmp/legacy',
          'queued', 123, 2, 'legacy-parent', 'Legacy thread'
        );
        INSERT INTO chats (
          bot_uid, channel_type, channel_id, session_id, cwd, session_file, created_at, updated_at
        ) VALUES (
          'legacy-listen-bot', 5, 'chat-parent____chat-short', 'chat-session', '/tmp/chat', NULL, 123, 123
        );
        INSERT INTO thread_workspaces (
          bot_uid, channel_type, channel_id, layout, status, entry_path, projects, error, updated_at
        ) VALUES (
          'legacy-listen-bot', 5, 'workspace-parent____workspace-short', 'single', 'ready',
          '/tmp/workspace', '[]', NULL, 123
        );
      `);
    } finally {
      legacyDb.close();
    }

    let store = await openChannelStore(root);
    try {
      expect(store.getThreadListen("legacy-listen-bot", {
        channelType: 5,
        channelId: "legacy-parent____legacy-short",
      })).toEqual({
        botUid: "legacy-listen-bot",
        channelId: "legacy-parent____legacy-short",
        creatorChannelType: 2,
        creatorChannelId: "legacy-parent",
        threadName: "Legacy thread",
        listen: "merge",
        mergePending: false,
      });
      expect(store.getThreadListen("legacy-listen-bot", {
        channelType: 5,
        channelId: "chat-parent____chat-short",
      })).toEqual({
        botUid: "legacy-listen-bot",
        channelId: "chat-parent____chat-short",
        creatorChannelType: 2,
        creatorChannelId: "chat-parent",
        threadName: "",
        listen: "merge",
        mergePending: false,
      });
      expect(store.getThreadListen("legacy-listen-bot", {
        channelType: 5,
        channelId: "workspace-parent____workspace-short",
      })).toEqual({
        botUid: "legacy-listen-bot",
        channelId: "workspace-parent____workspace-short",
        creatorChannelType: 2,
        creatorChannelId: "workspace-parent",
        threadName: "",
        listen: "merge",
        mergePending: false,
      });
      expect(store.listPendingThreadMergeEvents("legacy-listen-bot")).toEqual([{
        id: "legacy-merge-event",
        botUid: "legacy-listen-bot",
        channelId: "legacy-parent____legacy-short",
        targetBranch: "main",
        commit: "legacy-commit",
        removed: true,
        archive: "scheduled",
      }]);
      expect(store.getTask("legacy-notify-task")).toMatchObject({
        id: "legacy-notify-task",
        text: "preserve this task",
        state: "queued",
        relaySuppressed: false,
      });
      expect(Object.keys(store.getTask("legacy-notify-task") ?? {}).some((key) => key.startsWith("relayChannel"))).toBe(false);
    } finally {
      store.close();
    }

    const migratedDb = new Database(databasePath);
    try {
      const taskColumns = migratedDb.query("PRAGMA table_info(tasks)").all() as Array<{ name: string }>;
      expect(taskColumns.map(({ name }) => name)).not.toContain("relay_channel_type");
      expect(taskColumns.map(({ name }) => name)).not.toContain("relay_channel_id");
      expect(taskColumns.map(({ name }) => name)).not.toContain("relay_thread_name");
    } finally {
      migratedDb.close();
    }

    store = await openChannelStore(root);
    try {
      expect(store.getThreadListen("legacy-listen-bot", {
        channelType: 5,
        channelId: "legacy-parent____legacy-short",
      })).toMatchObject({ listen: "merge", creatorChannelId: "legacy-parent" });
      expect(store.listPendingThreadMergeEvents("legacy-listen-bot")).toEqual([{
        id: "legacy-merge-event",
        botUid: "legacy-listen-bot",
        channelId: "legacy-parent____legacy-short",
        targetBranch: "main",
        commit: "legacy-commit",
        removed: true,
        archive: "scheduled",
      }]);
      expect(store.getTask("legacy-notify-task")).toMatchObject({
        id: "legacy-notify-task",
        text: "preserve this task",
        state: "queued",
        relaySuppressed: false,
      });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
