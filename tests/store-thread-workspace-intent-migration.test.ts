import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

test("legacy thread workspace intents migrate as workspace intents without creator metadata", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-intent-migration-"));
  try {
    const legacy = new Database(join(root, "channel.sqlite"));
    try {
      legacy.exec(`
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
        ) VALUES(
          'legacy-intent', 'legacy-bot', 'legacy-group', 'failed',
          '["legacy-group____legacy-thread"]', 'legacy failure', 4, 5
        );
      `);
    } finally {
      legacy.close();
    }

    const store = await openChannelStore(root);
    try {
      expect(store.listThreadWorkspaceIntents("legacy-bot", "legacy-group")).toEqual([{
        id: "legacy-intent",
        botUid: "legacy-bot",
        groupId: "legacy-group",
        kind: "workspace",
        creatorChannelType: null,
        creatorChannelId: null,
        status: "failed",
        channelIds: ["legacy-group____legacy-thread"],
        error: "legacy failure",
      }]);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
