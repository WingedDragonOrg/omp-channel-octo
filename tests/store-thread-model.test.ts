import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";

test("thread model resolution prefers its chat, then its parent default, never the parent chat model", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-model-priority-"));
  try {
    const store = await openChannelStore(root);
    const botUid = "bot-thread-model-priority";
    const groupId = "group-thread-model-priority";
    const group = { channelType: 2 as const, channelId: groupId };
    const thread = { channelType: 5 as const, channelId: `${groupId}____topic-1` };
    try {
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: null, source: "omp" });

      store.setChatModel(botUid, group, "fixture/parent-chat");
      expect(store.getChatModel(botUid, group)).toBe("fixture/parent-chat");
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: null, source: "omp" });

      store.setThreadModel(botUid, groupId, "fixture/thread-default");
      expect(store.getThreadModel(botUid, groupId)).toBe("fixture/thread-default");
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/thread-default", source: "thread" });

      store.setChatModel(botUid, thread, "fixture/thread-explicit");
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/thread-explicit", source: "chat" });

      store.clearChatModel(botUid, thread);
      expect(store.getChatModel(botUid, thread)).toBeNull();
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/thread-default", source: "thread" });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("thread defaults are isolated by bot and parent group", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-model-isolation-"));
  try {
    const store = await openChannelStore(root);
    try {
      store.setThreadModel("bot-thread-model-a", "group-shared-id", "fixture/bot-a");
      store.setThreadModel("bot-thread-model-b", "group-shared-id", "fixture/bot-b");
      store.setThreadModel("bot-thread-model-a", "group-other", "fixture/other-group");

      expect(store.getThreadModel("bot-thread-model-a", "group-shared-id")).toBe("fixture/bot-a");
      expect(store.getThreadModel("bot-thread-model-b", "group-shared-id")).toBe("fixture/bot-b");
      expect(store.getThreadModel("bot-thread-model-a", "group-other")).toBe("fixture/other-group");
      expect(store.getThreadModel("bot-thread-model-b", "group-other")).toBeNull();
      expect(store.getThreadModel("bot-thread-model-a", "missing-group")).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("clearing a parent thread default leaves the thread's explicit model intact", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-model-clear-"));
  try {
    const store = await openChannelStore(root);
    const botUid = "bot-thread-model-clear";
    const groupId = "group-thread-model-clear";
    const thread = { channelType: 5 as const, channelId: `${groupId}____topic-1` };
    try {
      store.setThreadModel(botUid, groupId, "fixture/parent-default");
      store.setChatModel(botUid, thread, "fixture/thread-explicit");

      store.clearThreadModel(botUid, groupId);

      expect(store.getThreadModel(botUid, groupId)).toBeNull();
      expect(store.getChatModel(botUid, thread)).toBe("fixture/thread-explicit");
      expect(store.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/thread-explicit", source: "chat" });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("thread default and explicit models survive closing and reopening the store", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-model-reopen-"));
  const botUid = "bot-thread-model-reopen";
  const groupId = "group-thread-model-reopen";
  const thread = { channelType: 5 as const, channelId: `${groupId}____topic-1` };
  try {
    const store = await openChannelStore(root);
    store.setThreadModel(botUid, groupId, "fixture/persisted-default");
    store.setChatModel(botUid, thread, "fixture/persisted-explicit");
    store.close();

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.getThreadModel(botUid, groupId)).toBe("fixture/persisted-default");
      expect(reopened.getChatModel(botUid, thread)).toBe("fixture/persisted-explicit");
      expect(reopened.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/persisted-explicit", source: "chat" });

      reopened.clearChatModel(botUid, thread);
      expect(reopened.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/persisted-default", source: "thread" });
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("legacy chat model rows survive thread model migration and repeated reopen", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-model-migration-"));
  const databasePath = join(root, "channel.sqlite");
  const botUid = "bot-thread-model-migration";
  const groupId = "group-thread-model-migration";
  const group = { channelType: 2 as const, channelId: groupId };
  const thread = { channelType: 5 as const, channelId: `${groupId}____topic-1` };
  try {
    const legacy = new Database(databasePath);
    try {
      legacy.exec(`
        CREATE TABLE chat_models (
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
          channel_id TEXT NOT NULL,
          model TEXT NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY(bot_uid, channel_type, channel_id)
        );
      `);
      legacy.query(`
        INSERT INTO chat_models (bot_uid, channel_type, channel_id, model, updated_at)
        VALUES (?, 2, ?, ?, ?)
      `).run(botUid, groupId, "fixture/legacy-parent-chat", Date.now());

      expect(legacy.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'thread_models'").get()).toBeNull();
      expect(legacy.query("SELECT model FROM chat_models WHERE bot_uid = ? AND channel_type = 2 AND channel_id = ?")
        .get(botUid, groupId)).toEqual({ model: "fixture/legacy-parent-chat" });
    } finally {
      legacy.close();
    }

    const migrated = await openChannelStore(root);
    try {
      expect(migrated.getChatModel(botUid, group)).toBe("fixture/legacy-parent-chat");
      expect(migrated.getChatModel(botUid, thread)).toBeNull();
      expect(migrated.resolveChatModel(botUid, thread)).toEqual({ model: null, source: "omp" });

      migrated.setThreadModel(botUid, groupId, "fixture/new-thread-default");
      expect(migrated.getThreadModel(botUid, groupId)).toBe("fixture/new-thread-default");
      expect(migrated.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/new-thread-default", source: "thread" });
    } finally {
      migrated.close();
    }

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.getChatModel(botUid, group)).toBe("fixture/legacy-parent-chat");
      expect(reopened.getThreadModel(botUid, groupId)).toBe("fixture/new-thread-default");
      expect(reopened.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/new-thread-default", source: "thread" });
    } finally {
      reopened.close();
    }

    const reopenedAgain = await openChannelStore(root);
    try {
      expect(reopenedAgain.getChatModel(botUid, group)).toBe("fixture/legacy-parent-chat");
      expect(reopenedAgain.getThreadModel(botUid, groupId)).toBe("fixture/new-thread-default");
      expect(reopenedAgain.resolveChatModel(botUid, thread)).toEqual({ model: "fixture/new-thread-default", source: "thread" });
    } finally {
      reopenedAgain.close();
    }

    const upgraded = new Database(databasePath);
    try {
      expect(upgraded.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'thread_models'").get())
        .toEqual({ name: "thread_models" });
      expect(upgraded.query("SELECT bot_uid, group_id, model FROM thread_models WHERE bot_uid = ? AND group_id = ?")
        .all(botUid, groupId)).toEqual([{ bot_uid: botUid, group_id: groupId, model: "fixture/new-thread-default" }]);
    } finally {
      upgraded.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
