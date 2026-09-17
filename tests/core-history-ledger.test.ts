import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts";
import { type ChannelStore, openChannelStore } from "../src/store";
import type { TaskRecord, InboundClaim } from "../src/store-records";

const botUid = "bot-group-history";
const group: ChatTarget = { channelId: "group-history", channelType: 2 };
const thread: ChatTarget = { channelId: "thread-history", channelType: 5 };

async function withStore<T>(
  work: (store: ChannelStore, stateDir: string) => T | Promise<T>,
): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-history-ledger-"));
  const stateDir = join(root, "state");
  const store = await openChannelStore(stateDir);
  try {
    return await work(store, stateDir);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

/** Row count of the durable ledger table, read through a second connection. */
function countLedgerRows(stateDir: string): number {
  const db = new Database(join(stateDir, "channel.sqlite"));
  try {
    const row = db.query("SELECT COUNT(*) AS count FROM group_history_ledger").get() as
      | { count: number }
      | null;
    return row?.count ?? 0;
  } finally {
    db.close();
  }
}

function claimFor(target: ChatTarget, messageId: string): InboundClaim {
  return {
    botUid,
    target,
    messageId,
    senderId: "owner-1",
    text: "history task",
    supported: true,
    kind: "task",
  };
}

function taskFor(
  target: ChatTarget,
  messageId: string,
  taskId: string,
  meta: { messageSeq: number; historyEligible: boolean },
): TaskRecord {
  return {
    id: taskId,
    botUid,
    channelType: target.channelType,
    channelId: target.channelId,
    messageId,
    senderId: "owner-1",
    text: "history task",
    sessionId: "session-task-meta",
    cwd: "/tmp/history-task-cwd",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: meta.messageSeq,
    historyEligible: meta.historyEligible,
    relayChannelType: null,
    relayChannelId: null,
    relayThreadName: null,
  };
}

test("reopens a fresh state database and exposes the ledger and task columns", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-history-ledger-"));
  const stateDir = join(root, "state");
  try {
    const first = await openChannelStore(stateDir);
    first.close();

    const second = await openChannelStore(stateDir);
    try {
      // group_history_ledger exists, so an empty lookup answers with null instead of throwing.
      expect(second.getHistoryLedger(botUid, group, "session-a")).toBeNull();
      expect(second.getHistoryLedger(botUid, thread, "session-a")).toBeNull();

      // tasks.message_seq and tasks.history_eligible accept writes and read back.
      const target: ChatTarget = { channelId: "group-task-columns", channelType: 2 };
      expect(
        second.acceptTask(
          claimFor(target, "columns-message"),
          taskFor(target, "columns-message", "columns-task", { messageSeq: 12, historyEligible: true }),
        ),
      ).toBe(true);
      expect(second.getTask("columns-task")).toMatchObject({
        messageSeq: 12,
        historyEligible: true,
      });
    } finally {
      second.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("migrates a database that predates the history ledger and the task columns", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-history-ledger-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const legacy = new Database(join(stateDir, "channel.sqlite"));
  try {
    legacy.exec(`
      CREATE TABLE tasks (
        id TEXT PRIMARY KEY NOT NULL,
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        text TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cwd TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'interrupted')),
        accepted_at INTEGER NOT NULL,
        started_at INTEGER,
        finished_at INTEGER,
        result_text TEXT,
        error_text TEXT
      );
    `);
    legacy
      .query(
        `INSERT INTO tasks(id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd, state, accepted_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)`,
      )
      .run(
        "legacy-task",
        botUid,
        group.channelType,
        group.channelId,
        "legacy-message",
        "owner-1",
        "legacy work",
        "session-legacy",
        "/tmp/legacy-cwd",
        Date.now(),
      );
  } finally {
    legacy.close();
  }

  try {
    const upgraded = await openChannelStore(stateDir);
    upgraded.close();
    // Reopening an already upgraded database hits the duplicate-column migration path.
    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.getTask("legacy-task")).toMatchObject({
        messageSeq: 0,
        historyEligible: true,
      });
      expect(reopened.getHistoryLedger(botUid, group, "session-legacy")).toBeNull();
      expect(reopened.prepareHistoryInjection(botUid, group, "session-legacy", 3)).toMatchObject({
        phase: "pending",
        lastSeq: 3,
        previousSeq: null,
      });
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("prepares a first pending injection and extends it without replacing the attempt", () =>
  withStore((store) => {
    const first = store.prepareHistoryInjection(botUid, group, "session-a", 10);
    expect(first).toMatchObject({
      botUid,
      channelType: 2,
      channelId: group.channelId,
      sessionId: "session-a",
      lastSeq: 10,
      phase: "pending",
      previousSeq: null,
    });
    expect(first.attemptId).toEqual(expect.any(String));

    const extended = store.prepareHistoryInjection(botUid, group, "session-a", 14);
    expect(extended.attemptId).toBe(first.attemptId);
    expect(extended.lastSeq).toBe(14);
    expect(extended.previousSeq).toBeNull();
    expect(extended.phase).toBe("pending");

    const regressed = store.prepareHistoryInjection(botUid, group, "session-a", 12);
    expect(regressed.attemptId).toBe(first.attemptId);
    expect(regressed.lastSeq).toBe(14);
  }));

test("commits only the matching pending attempt", () =>
  withStore((store) => {
    const pending = store.prepareHistoryInjection(botUid, group, "session-a", 10);
    expect(store.commitHistoryInjection(botUid, group, "session-a", 10, "other-attempt")).toBe(false);
    expect(store.commitHistoryInjection(botUid, group, "session-a", 11, pending.attemptId)).toBe(false);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({
      phase: "pending",
      lastSeq: 10,
      attemptId: pending.attemptId,
    });

    expect(store.commitHistoryInjection(botUid, group, "session-a", 10, pending.attemptId)).toBe(true);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({
      phase: "committed",
      lastSeq: 10,
    });
    expect(store.commitHistoryInjection(botUid, group, "session-a", 10, pending.attemptId)).toBe(false);
  }));

test("prepares the next injection from the committed watermark with a fresh attempt", () =>
  withStore((store) => {
    const first = store.prepareHistoryInjection(botUid, group, "session-a", 10);
    expect(store.commitHistoryInjection(botUid, group, "session-a", 10, first.attemptId)).toBe(true);

    const second = store.prepareHistoryInjection(botUid, group, "session-a", 18);
    expect(second.previousSeq).toBe(10);
    expect(second.lastSeq).toBe(18);
    expect(second.phase).toBe("pending");
    expect(second.attemptId).not.toBe(first.attemptId);

    // Committing the older attempt must not disturb the newer pending one.
    expect(store.commitHistoryInjection(botUid, group, "session-a", 10, first.attemptId)).toBe(false);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({
      phase: "pending",
      lastSeq: 18,
      previousSeq: 10,
      attemptId: second.attemptId,
    });
  }));

test("prunes other sessions of the same chat and keeps the current session", () =>
  withStore((store) => {
    store.prepareHistoryInjection(botUid, group, "session-keep", 5);
    const replaced = store.prepareHistoryInjection(botUid, group, "session-replaced", 7);
    store.commitHistoryInjection(botUid, group, "session-replaced", 7, replaced.attemptId);
    store.prepareHistoryInjection(botUid, group, "session-orphan", 9);
    store.prepareHistoryInjection(botUid, thread, "session-replaced", 3);

    expect(store.pruneHistoryLedger(botUid, group, "session-keep")).toBe(2);
    expect(store.getHistoryLedger(botUid, group, "session-keep")).toMatchObject({
      lastSeq: 5,
      phase: "pending",
    });
    expect(store.getHistoryLedger(botUid, group, "session-replaced")).toBeNull();
    expect(store.getHistoryLedger(botUid, group, "session-orphan")).toBeNull();
    // The same session id on a different chat is untouched.
    expect(store.getHistoryLedger(botUid, thread, "session-replaced")).toMatchObject({
      lastSeq: 3,
      phase: "pending",
    });
    expect(store.pruneHistoryLedger(botUid, group, "session-keep")).toBe(0);
  }));

test("advances the watermark for steered messages without replacing the phase", () =>
  withStore((store) => {
    store.advanceHistoryLedger(botUid, group, "session-a", 4);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({
      lastSeq: 4,
      phase: "committed",
      previousSeq: null,
    });

    store.advanceHistoryLedger(botUid, group, "session-a", 6);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({ lastSeq: 6 });
    store.advanceHistoryLedger(botUid, group, "session-a", 5);
    expect(store.getHistoryLedger(botUid, group, "session-a")).toMatchObject({ lastSeq: 6 });

    const pending = store.prepareHistoryInjection(botUid, group, "session-b", 3);
    store.advanceHistoryLedger(botUid, group, "session-b", 8);
    expect(store.getHistoryLedger(botUid, group, "session-b")).toMatchObject({
      lastSeq: 8,
      phase: "pending",
      previousSeq: null,
      attemptId: pending.attemptId,
    });
  }));

test("rejects invalid ledger identities without writing rows", () =>
  withStore((store, stateDir) => {
    expect(() =>
      store.prepareHistoryInjection(botUid, { channelId: "dm-history", channelType: 1 }, "session-a", 5),
    ).toThrow();
    expect(() => store.prepareHistoryInjection(botUid, group, "", 5)).toThrow();
    expect(() => store.prepareHistoryInjection(botUid, group, "session-a", -1)).toThrow();
    expect(() => store.prepareHistoryInjection(botUid, group, "session-a", 1.5)).toThrow();
    expect(() =>
      store.prepareHistoryInjection(botUid, { channelId: "", channelType: 2 }, "session-a", 5),
    ).toThrow();
    expect(() => store.prepareHistoryInjection("", group, "session-a", 5)).toThrow();

    // Commit and accounting helpers treat invalid identities as misses instead of writing.
    expect(
      store.commitHistoryInjection(botUid, { channelId: "dm-history", channelType: 1 }, "session-a", 5, "attempt"),
    ).toBe(false);
    store.advanceHistoryLedger(botUid, { channelId: "dm-history", channelType: 1 }, "session-a", 5);
    store.advanceHistoryLedger(botUid, group, "session-a", -3);
    expect(store.pruneHistoryLedger(botUid, { channelId: "dm-history", channelType: 1 }, "session-a")).toBe(0);
    expect(countLedgerRows(stateDir)).toBe(0);
  }));

test("round-trips message_seq and history_eligible through acceptTask and getTask", () =>
  withStore((store) => {
    const target: ChatTarget = { channelId: "group-task-meta", channelType: 2 };
    expect(
      store.acceptTask(
        claimFor(target, "meta-message-zero"),
        taskFor(target, "meta-message-zero", "meta-task-zero", { messageSeq: 0, historyEligible: false }),
      ),
    ).toBe(true);
    expect(store.getTask("meta-task-zero")).toMatchObject({
      messageSeq: 0,
      historyEligible: false,
    });

    expect(
      store.acceptTask(
        claimFor(target, "meta-message-numbered"),
        taskFor(target, "meta-message-numbered", "meta-task-numbered", {
          messageSeq: 42,
          historyEligible: true,
        }),
      ),
    ).toBe(true);
    expect(store.getTask("meta-task-numbered")).toMatchObject({
      messageSeq: 42,
      historyEligible: true,
    });
    expect(store.getLatestTask(botUid, target)).toMatchObject({
      id: "meta-task-numbered",
      messageSeq: 42,
      historyEligible: true,
    });
    expect(store.listQueuedTasks().map((task) => [task.id, task.messageSeq, task.historyEligible])).toEqual([
      ["meta-task-zero", 0, false],
      ["meta-task-numbered", 42, true],
    ]);
  }));
