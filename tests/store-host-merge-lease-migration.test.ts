import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

test("legacy channel databases create durable host merge lease storage", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-host-merge-migration-"));
  try {
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
        VALUES ('bot-existing', 'owner-existing', 1, 1);
        CREATE TABLE host_merge_leases (
          id TEXT PRIMARY KEY NOT NULL,
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL CHECK(channel_type = 5),
          channel_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          cwd TEXT NOT NULL,
          phase TEXT NOT NULL CHECK(phase IN ('starting', 'running', 'exited', 'unknown')),
          pid INTEGER,
          pgid INTEGER,
          process_identity TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO host_merge_leases(
          id, bot_uid, channel_type, channel_id, source_id, cwd, phase,
          pid, pgid, process_identity, created_at, updated_at
        ) VALUES(
          'legacy-unknown-host-merge', 'bot-existing', 5, 'group-existing____thread',
          'legacy-source', '/work/existing-thread', 'unknown', NULL, NULL, NULL, 2, 3
        );
      `);
    } finally {
      legacy.close();
    }

    const store = await openChannelStore(root);
    try {
      store.reserveHostMergeLease({
        id: "legacy-host-merge-lease",
        botUid: "bot-legacy",
        target: { channelType: 5, channelId: "group-legacy____thread-legacy" },
        sourceId: "legacy-command-message",
        cwd: "/work/legacy-thread",
        resourceCwds: ["/work/legacy-thread"],
      });
      expect(store.getOwnerId("bot-existing")).toBe("owner-existing");
      expect(store.listHostMergeLeases("bot-legacy")).toMatchObject([{
        id: "legacy-host-merge-lease",
        phase: "starting",
        pid: null,
        pgid: null,
        processIdentity: null,
      }]);
      expect(store.listHostMergeLeases("bot-existing")).toMatchObject([{
        id: "legacy-unknown-host-merge",
        sourceId: "legacy-source",
        phase: "unknown",
        cwd: "/work/existing-thread",
        resourceCwds: ["/work/existing-thread"],
      }]);
    } finally {
      store.close();
    }

    const reopened = await openChannelStore(root);
    try {
      const lease = reopened.listHostMergeLeases("bot-legacy")[0];
      expect(lease).toMatchObject({
        id: "legacy-host-merge-lease",
        sourceId: "legacy-command-message",
        cwd: "/work/legacy-thread",
        phase: "starting",
      });
      expect(reopened.getOwnerId("bot-existing")).toBe("owner-existing");
    } finally {
      reopened.close();
    }

    const migrated = new Database(join(root, "channel.sqlite"));
    try {
      expect(migrated.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get()).toEqual({
        value: "preserved",
      });
      expect(migrated.query("SELECT owner_id FROM owners WHERE bot_uid = 'bot-existing'").get()).toEqual({
        owner_id: "owner-existing",
      });
    } finally {
      migrated.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("host merge outcome survives restart until delivery settles the lease", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-host-merge-lease-"));
  try {
    const store = await openChannelStore(root);
    try {
      store.reserveHostMergeLease({
        id: "host-merge-operation",
        botUid: "bot-merge",
        target: { channelType: 5, channelId: "group-merge____thread" },
        sourceId: "merge-command",
        cwd: "/work/merge-thread",
        resourceCwds: ["/work/merge-thread", "/work/target-checkout"],
        commandText: "/thread merge preserve note token=[redacted]",
        operatorNote: "preserve note",
        senderId: "owner-1",
        messageSeq: 23,
        receiptOutboxId: "stable-merge-receipt",
        archiveGeneration: 9,
      });
      store.recordHostMergeProcess("host-merge-operation", {
        pid: 43001,
        pgid: 43001,
        identity: "pid=43001;start=Wed Jun 10 12:00:00 2026",
      });
      expect(store.listHostMergeLeases("bot-merge")).toMatchObject([{
        phase: "running",
        sourceId: "merge-command",
      }]);
      store.markHostMergeProcessExited("host-merge-operation");
      store.persistHostMergeOutcome("host-merge-operation", {
        kind: "succeeded",
        summary: {
          branch: "thread-branch",
          target: "main",
          squashed: true,
          rebased: true,
          committed: true,
          removed: true,
          commit: "actual-target-commit",
        },
      });
      expect(store.listHostMergeLeases("bot-merge")).toMatchObject([{
        phase: "exited",
        pid: 43001,
        pgid: 43001,
        processIdentity: "pid=43001;start=Wed Jun 10 12:00:00 2026",
        resourceCwds: ["/work/merge-thread", "/work/target-checkout"],
        archiveGeneration: 9,
      }]);
    } finally {
      store.close();
    }

    const restarted = await openChannelStore(root);
    try {
      expect(restarted.getHostMergeLeaseBySourceId(
        "bot-merge",
        { channelType: 5, channelId: "group-merge____thread" },
        "merge-command",
      )).toMatchObject({
        phase: "exited",
        commandText: "/thread merge preserve note token=[redacted]",
        operatorNote: "preserve note",
        senderId: "owner-1",
        messageSeq: 23,
        receiptOutboxId: "stable-merge-receipt",
        outcome: { kind: "succeeded", summary: { target: "main", commit: "actual-target-commit" } },
        archiveGeneration: 9,
      });
      restarted.settleHostMergeLease("host-merge-operation");
      expect(restarted.listHostMergeLeases("bot-merge")).toEqual([]);
      restarted.reserveHostMergeLease({
        id: "unknown-host-merge-operation",
        botUid: "bot-merge",
        target: { channelType: 5, channelId: "group-merge____thread" },
        sourceId: "unknown-command",
        cwd: "/work/merge-thread",
        resourceCwds: ["/work/merge-thread"],
      });
      restarted.markHostMergeLeaseUnknown("unknown-host-merge-operation");
      expect(restarted.listHostMergeLeases("bot-merge")).toMatchObject([{ phase: "unknown" }]);
      expect(() => restarted.settleHostMergeLease("unknown-host-merge-operation")).toThrow();
      expect(restarted.listHostMergeLeases("bot-merge")).toHaveLength(1);
    } finally {
      restarted.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("a confirmed pre-execution process exit persists its result, but unknown cannot", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-host-merge-not-spawned-"));
  try {
    const store = await openChannelStore(root);
    try {
      store.reserveHostMergeLease({
        id: "parked-host-merge",
        botUid: "bot-parked",
        target: { channelType: 5, channelId: "group-parked____thread" },
        sourceId: "parked-merge-call",
        cwd: "/work/parked-thread",
        resourceCwds: ["/work/parked-thread"],
      });
      store.recordHostMergeProcess("parked-host-merge", {
        pid: 43002,
        pgid: 43002,
        identity: "pid=43002;start=Wed Jun 10 12:00:00 2026",
      });
      store.completeHostMergeLease("parked-host-merge", {
        kind: "not-spawned",
        diagnostic: "confirmed not started",
      });
      expect(store.listHostMergeLeases("bot-parked")).toMatchObject([{
        phase: "exited",
        outcome: { kind: "not-started", diagnostic: "confirmed not started" },
      }]);
      store.settleHostMergeLease("parked-host-merge");
      expect(store.listHostMergeLeases("bot-parked")).toEqual([]);

      store.reserveHostMergeLease({
        id: "unknown-host-merge",
        botUid: "bot-parked",
        target: { channelType: 5, channelId: "group-parked____thread" },
        sourceId: "unknown-merge-call",
        cwd: "/work/parked-thread",
        resourceCwds: ["/work/parked-thread"],
      });
      store.recordHostMergeProcess("unknown-host-merge", {
        pid: 43003,
        pgid: 43003,
        identity: "pid=43003;start=Wed Jun 10 12:01:00 2026",
      });
      store.markHostMergeLeaseUnknown("unknown-host-merge");
      expect(() => store.completeHostMergeLease("unknown-host-merge", {
        kind: "not-spawned",
        diagnostic: "confirmed not started",
      })).toThrow();
      expect(store.listHostMergeLeases("bot-parked")).toMatchObject([{ phase: "unknown" }]);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("thread archive generations persist inbound invalidation across restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-archive-generation-"));
  const target = { channelType: 5 as const, channelId: "group-generation____thread" };
  try {
    const store = await openChannelStore(root);
    try {
      expect(store.getThreadArchiveGeneration("bot-generation", target)).toBe(0);
      expect(store.advanceThreadArchiveGeneration("bot-generation", target)).toBe(1);
    } finally {
      store.close();
    }

    const restarted = await openChannelStore(root);
    try {
      expect(restarted.getThreadArchiveGeneration("bot-generation", target)).toBe(1);
      expect(restarted.advanceThreadArchiveGeneration("bot-generation", target)).toBe(2);
    } finally {
      restarted.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
