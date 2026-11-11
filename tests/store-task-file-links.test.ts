import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";

const botUid = "bot-file-link-store";
const target: ChatTarget = { channelType: 1, channelId: "dm-file-link-store" };
const taskId = "task-file-link-store";

test("empty resolver snapshots stay disabled and corrupted persisted snapshots fail explicitly", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-task-file-links-"));
  try {
    const store = await openChannelStore(join(root, "state"));
    const task: TaskRecord = {
      id: taskId,
      botUid,
      ...target,
      messageId: "message-file-link-store",
      senderId: "owner-file-link-store",
      text: "preserve a disabled resolver snapshot",
      sessionId: "session-file-link-store",
      cwd: "/workspace/file-link-store",
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relaySuppressed: false,
    };
    const claim: InboundClaim = {
      botUid,
      target,
      messageId: task.messageId,
      senderId: task.senderId,
      text: task.text,
      supported: true,
      kind: "task",
    };
    const databasePath = store.path;
    try {
      expect(store.acceptTask(claim, task)).toBe(true);
      expect(store.ensureTaskFileLinkContext({
        taskId,
        cwd: task.cwd,
        resolvers: [],
      })).toEqual({ taskId, cwd: task.cwd, resolvers: [] });
      expect(store.ensureTaskFileLinkContext({
        taskId,
        cwd: "/workspace/changed-after-start",
        resolvers: [{ type: "zed", mode: "local" }],
      })).toEqual({ taskId, cwd: task.cwd, resolvers: [] });
    } finally {
      store.close();
    }

    for (const invalidSnapshot of [
      "not-json",
      "{}",
      '[{"type":"zed","mode":"ssh"}]',
    ]) {
      const database = new Database(databasePath);
      try {
        database.query(
          "UPDATE task_file_link_context SET resolvers_json = ? WHERE task_id = ?",
        ).run(invalidSnapshot, taskId);
      } finally {
        database.close();
      }

      const recovered = await openChannelStore(join(root, "state"));
      try {
        expect(() => recovered.getTaskFileLinkContext(taskId))
          .toThrow("Invalid persisted task file-link resolver configuration");
      } finally {
        recovered.close();
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
