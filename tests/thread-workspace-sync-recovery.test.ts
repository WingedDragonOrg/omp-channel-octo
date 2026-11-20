import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import { recoverPendingWorkspaceSync } from "../src/workspace/thread-workspaces.ts";
import { createCoreHarness, closeCoreHarness } from "./fixtures/core-fakes.ts";

test("restart makes an interrupted sync unready and durably notifies its creator once", async () => {
  const root = await mkdtemp(join(tmpdir(), "thread-sync-recovery-"));
  try {
    const store = await openChannelStore(root);
    const botUid = "fixture-bot";
    const channelId = "fixture-group____fixture-thread";
    try {
      store.recordCreatedThreadListen({ botUid, channelId, creatorChannelType: 2, creatorChannelId: "fixture-group", threadName: "fixture", listen: "merge" });
      store.saveThreadWorkspace({ botUid, channelType: 5, channelId, layout: "single", status: "preparing", entryPath: null, projects: [{ name: "project", repo: "/repo", path: "/repo.task", branch: "task", created: false, syncState: "pending" }], error: null });
      recoverPendingWorkspaceSync(store, botUid);
      recoverPendingWorkspaceSync(store, botUid);
      const workspace = store.getThreadWorkspace(botUid, { channelType: 5, channelId });
      expect(workspace?.status).toBe("failed");
      expect(workspace?.projects[0]?.syncState).toBe("unknown");
      const db = new Database(join(root, "channel.sqlite"));
      try {
        const rows = db.query("SELECT channel_id, text, state FROM outbox WHERE bot_uid = ?").all(botUid) as Array<{ channel_id: string; text: string; state: string }>;
        expect(rows).toHaveLength(1);
        expect(rows[0]).toMatchObject({ channel_id: "fixture-group", state: "pending" });
        expect(rows[0]?.text).toContain("同步结果未确认");
        expect(rows[0]?.text).toContain("/repo.task");
      } finally { db.close(); }
    } finally { store.close(); }
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("service startup delivers a recovered interrupted-sync notice", async () => {
  const root = await mkdtemp(join(tmpdir(), "thread-sync-service-recovery-"));
  const stateDir = join(root, "state");
  const botUid = "fixture-sync-bot";
  const channelId = "fixture-group____fixture-thread";
  try {
    const store = await openChannelStore(stateDir);
    store.recordCreatedThreadListen({ botUid, channelId, creatorChannelType: 2, creatorChannelId: "fixture-group", threadName: "fixture", listen: "merge" });
    store.saveThreadWorkspace({ botUid, channelType: 5, channelId, layout: "single", status: "preparing", entryPath: null, projects: [{ name: "project", repo: "/repo", path: "/repo.task", branch: "task", created: false, syncState: "pending" }], error: null });
    store.close();
    const harness = await createCoreHarness({ root, bot: { uid: botUid, name: "Fixture" }, allowedGroupIds: ["fixture-group"] });
    try {
      const deadline = Date.now() + 2_000;
      while (!harness.transport.sent.some((message) => message.text.includes("同步结果未确认")) && Date.now() < deadline) {
        const { promise, resolve } = Promise.withResolvers<void>();
        setImmediate(resolve);
        await promise;
      }
      expect(harness.transport.sent.some((message) =>
        message.target.channelId === "fixture-group" && message.text.includes("/repo.task")
      )).toBe(true);
    } finally { await closeCoreHarness(harness, { remove: false }); }
  } finally { await rm(root, { recursive: true, force: true }); }
});
