import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

test("outbox mention columns migrate without modifying legacy stream rows", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-migration-"));
  try {
    const database = new Database(join(root, "channel.sqlite"));
    try {
      database.exec(`
      CREATE TABLE outbox (
        id TEXT PRIMARY KEY NOT NULL,
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        task_id TEXT,
        kind TEXT NOT NULL CHECK(kind IN ('text', 'file')),
        text TEXT,
        path TEXT,
        file_kind TEXT CHECK(file_kind IN ('image', 'file')),
        client_msg_no TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL CHECK(state IN ('pending', 'unknown', 'sent', 'dead')),
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        remote_message_id TEXT,
        stream_message_id TEXT,
        segment_index INTEGER NOT NULL DEFAULT 0,
        desired_version INTEGER NOT NULL DEFAULT 0,
        acked_version INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        sent_at INTEGER
      );
    `);
      database.query(`
        INSERT INTO outbox (
          id, bot_uid, channel_type, channel_id, task_id, kind, text, client_msg_no, state, created_at,
          stream_message_id, segment_index, desired_version, acked_version
        ) VALUES (?, ?, ?, ?, ?, 'text', ?, ?, 'pending', ?, ?, ?, ?, ?)
      `).run(
        "legacy-outbox-row",
        "bot-mention-migration",
        2,
        "group-mention-migration",
        "legacy-task",
        "legacy text",
        "legacy-client-message-number",
        Date.now(),
        "legacy-task:assistant:legacy:mention-tail",
        0,
        1,
        0,
      );
    } finally {
      database.close();
    }

    const legacy = await openChannelStore(root);
    const target = { channelType: 2 as const, channelId: "group-mention-migration" };
    try {
      const oldRow = legacy.listPendingOutbox("bot-mention-migration", target).find(
        ({ id }) => id === "legacy-outbox-row",
      );
      expect(oldRow).toMatchObject({
        text: "legacy text",
        streamMessageId: "legacy-task:assistant:legacy:mention-tail",
        segmentIndex: 0,
        desiredVersion: 1,
        mention: null,
        streamFinal: false,
        createPayload: null,
        clientMsgNo: "legacy-client-message-number",
      });
      legacy.commitTextStream({
        botUid: "bot-mention-migration",
        target,
        taskId: "new-task",
        streamMessageId: "new-stream",
        segments: [{
          text: "@林",
          mention: {
            uids: ["member-migration"],
            entities: [{ uid: "member-migration", offset: 0, length: 2 }],
          },
        }],
        final: true,
        forceFinalSentinel: false,
      });
    } finally {
      legacy.close();
    }

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.listPendingOutbox("bot-mention-migration", target)).toContainEqual(
        expect.objectContaining({
          id: "legacy-outbox-row",
          text: "legacy text",
          streamMessageId: "legacy-task:assistant:legacy:mention-tail",
          segmentIndex: 0,
          desiredVersion: 1,
          mention: null,
          streamFinal: false,
          createPayload: null,
          clientMsgNo: "legacy-client-message-number",
        }),
      );
      expect(reopened.listPendingOutbox("bot-mention-migration", target)).toContainEqual(
        expect.objectContaining({
          taskId: "new-task",
          text: "@林",
          mention: {
            uids: ["member-migration"],
            entities: [{ uid: "member-migration", offset: 0, length: 2 }],
          },
          streamFinal: true,
          createPayload: {
            text: "@林",
            mention: {
              uids: ["member-migration"],
              entities: [{ uid: "member-migration", offset: 0, length: 2 }],
            },
          },
        }),
      );
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
