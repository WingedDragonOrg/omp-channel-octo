import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

test("legacy host process ledger preserves merge rows while removal state survives reopen independently", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-removal-migration-"));
  try {
    const db = new Database(join(root, "channel.sqlite"));
    db.exec(`CREATE TABLE host_merge_leases (
      id TEXT PRIMARY KEY NOT NULL, bot_uid TEXT NOT NULL, channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL, source_id TEXT NOT NULL, cwd TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('starting','running','exited','unknown')),
      pid INTEGER, pgid INTEGER, process_identity TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    INSERT INTO host_merge_leases VALUES ('legacy-merge','bot',5,'group____topic','source','/legacy','unknown',47001,47001,'old-identity',11,22);`);
    db.close();
    const target = { channelType: 5 as const, channelId: "group____topic" };
    let store = await openChannelStore(root);
    expect(store.listHostMergeLeases("bot")).toMatchObject([{
      id: "legacy-merge", sourceId: "source", phase: "unknown", cwd: "/legacy",
      pid: 47001, pgid: 47001, processIdentity: "old-identity", createdAt: 11, updatedAt: 22,
    }]);
    store.reserveHostRemovalLease({ id: "removal", botUid: "bot", target, sourceId: "source",
      cwd: "/owned", resourcePaths: ["/owned", "/source"], commandText: "/thread remove",
      senderId: "owner", messageSeq: 19, receiptOutboxId: "receipt" });
    store.beginHostRemovalStep("removal", "branch", "/source");
    store.recordHostRemovalProcess("removal", { pid: 48001, pgid: 48001, identity: "removal-identity", cwd: "/source" });
    store.markHostRemovalProcessExited("removal");
    store.persistHostRemovalOutcome("removal", { kind: "partial", receiptText: "Residual branch retained." });
    store.close();
    store = await openChannelStore(root);
    expect(store.getHostRemovalLeaseBySourceId("bot", target, "source")).toMatchObject({
      id: "removal", phase: "exited", step: "branch", messageSeq: 19,
      resourcePaths: ["/owned", "/source"], outcome: { kind: "partial", receiptText: "Residual branch retained." },
    });
    expect(store.listHostMergeLeases("bot").map(row => row.id)).toEqual(["legacy-merge"]);
    store.settleHostRemovalLease("legacy-merge");
    expect(store.listHostMergeLeases("bot").map(row => row.id)).toEqual(["legacy-merge"]);
    store.settleHostRemovalLease("removal");
    store.close();
    store = await openChannelStore(root);
    expect(store.listHostRemovalLeases("bot")).toEqual([]);
    expect(store.listHostMergeLeases("bot")).toMatchObject([{ id: "legacy-merge", processIdentity: "old-identity" }]);
    store.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
