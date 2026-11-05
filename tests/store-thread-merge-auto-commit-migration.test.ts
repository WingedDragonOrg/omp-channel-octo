import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

const legacyLease = {
  id: "legacy-unknown-host-merge",
  bot_uid: "bot-existing",
  channel_type: 5,
  channel_id: "group-existing____thread",
  source_id: "legacy-source",
  cwd: "/work/existing-thread",
  phase: "unknown",
  pid: 43003,
  pgid: 43003,
  process_identity: "pid=43003;start=legacy",
  created_at: 2,
  updated_at: 3,
};

function readLegacyLease(db: Database): Record<string, unknown> {
  return db.query(`
    SELECT id, bot_uid, channel_type, channel_id, source_id, cwd, phase,
      pid, pgid, process_identity, created_at, updated_at
    FROM host_merge_leases WHERE id = ?
  `).get(legacyLease.id) as Record<string, unknown>;
}

test("thread auto-commit migration preserves legacy leases and supports new dispatch states", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-merge-auto-commit-migration-"));
  try {
    const legacy = new Database(join(root, "channel.sqlite"));
    try {
      legacy.query("CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)").run();
      legacy.query("INSERT INTO meta (key, value) VALUES ('legacy-marker', 'preserved')").run();
      legacy.query(`
        CREATE TABLE owners (
          bot_uid TEXT PRIMARY KEY NOT NULL,
          owner_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `).run();
      legacy.query(`
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
        )
      `).run();
      legacy.query(`
        INSERT INTO host_merge_leases(
          id, bot_uid, channel_type, channel_id, source_id, cwd, phase,
          pid, pgid, process_identity, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        legacyLease.id,
        legacyLease.bot_uid,
        legacyLease.channel_type,
        legacyLease.channel_id,
        legacyLease.source_id,
        legacyLease.cwd,
        legacyLease.phase,
        legacyLease.pid,
        legacyLease.pgid,
        legacyLease.process_identity,
        legacyLease.created_at,
        legacyLease.updated_at,
      );
    } finally {
      legacy.close();
    }

    const firstOpen = await openChannelStore(root);
    try {
      expect(firstOpen.listHostMergeLeases(legacyLease.bot_uid)).toMatchObject([{
        id: legacyLease.id,
        sourceId: legacyLease.source_id,
        cwd: legacyLease.cwd,
        phase: "unknown",
        pid: legacyLease.pid,
        pgid: legacyLease.pgid,
        processIdentity: legacyLease.process_identity,
        autoCommitState: null,
        autoCommitDispatched: true,
        autoCommitResult: null,
      }]);
    } finally {
      firstOpen.close();
    }

    const migratedOnce = new Database(join(root, "channel.sqlite"), { readonly: true });
    try {
      expect(readLegacyLease(migratedOnce)).toEqual(legacyLease);
      expect(migratedOnce.query(
        "SELECT auto_commit_dispatched FROM host_merge_leases WHERE id = ?",
      ).get(legacyLease.id)).toEqual({ auto_commit_dispatched: 1 });
      expect(migratedOnce.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get())
        .toEqual({ value: "preserved" });
    } finally {
      migratedOnce.close();
    }

    const secondOpen = await openChannelStore(root);
    try {
      expect(secondOpen.getHostMergeLeaseBySourceId(
        legacyLease.bot_uid,
        { channelType: 5, channelId: legacyLease.channel_id },
        legacyLease.source_id,
      )).toMatchObject({ autoCommitState: null, autoCommitDispatched: true });

      const target = { channelType: 5 as const, channelId: "group-new____auto-commit" };
      secondOpen.reserveHostMergeLease({
        id: "new-auto-commit-lease",
        botUid: "bot-new",
        target,
        sourceId: "new-auto-commit-command",
        cwd: "/work/new-thread",
        resourceCwds: ["/work/new-thread"],
        autoCommitTaskId: "new-auto-commit-task",
      });
      expect(secondOpen.getHostMergeLeaseBySourceId(
        "bot-new",
        target,
        "new-auto-commit-command",
      )).toMatchObject({ autoCommitState: "committing", autoCommitDispatched: false });

      expect(secondOpen.markHostMergeAutoCommitDispatched("new-auto-commit-lease")).toBe(true);
      secondOpen.recordHostMergeAutoCommitResult("new-auto-commit-lease", {
        exitCode: 0,
        headBefore: "a".repeat(40),
        headAfter: "b".repeat(40),
      });
      expect(secondOpen.getHostMergeLeaseBySourceId(
        "bot-new",
        target,
        "new-auto-commit-command",
      )).toMatchObject({
        autoCommitState: "result-known",
        autoCommitDispatched: true,
        autoCommitResult: {
          exitCode: 0,
          headBefore: "a".repeat(40),
          headAfter: "b".repeat(40),
        },
      });

      expect(secondOpen.markHostMergeAutoCommitMergeReady("new-auto-commit-lease")).toBe(true);
      expect(secondOpen.beginHostMergeAutoCommitMerge("new-auto-commit-lease")).toBe(true);
      expect(secondOpen.getHostMergeLeaseBySourceId(
        "bot-new",
        target,
        "new-auto-commit-command",
      )).toMatchObject({ autoCommitState: "merging", autoCommitDispatched: true });
    } finally {
      secondOpen.close();
    }

    const migratedTwice = new Database(join(root, "channel.sqlite"), { readonly: true });
    try {
      expect(readLegacyLease(migratedTwice)).toEqual(legacyLease);
      expect(migratedTwice.query(
        "SELECT auto_commit_dispatched FROM host_merge_leases WHERE id = ?",
      ).get(legacyLease.id)).toEqual({ auto_commit_dispatched: 1 });
    } finally {
      migratedTwice.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
