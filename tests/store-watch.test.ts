import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import type { WatchCheckCompletion, WatchRecord } from "../src/watch/types.ts";
import { openChannelStore } from "../src/store/index.ts";

const botUid = "watch-store-bot";
const target: ChatTarget = { channelType: 5, channelId: "store-parent____topic" };

function tempRoot(): Promise<string> {
  return mkdtemp(join(tmpdir(), "oco-watch-store-"));
}

function watch(overrides: Partial<WatchRecord> = {}): WatchRecord {
  const id = overrides.id ?? "watch-1";
  return {
    id,
    botUid,
    target,
    sourceTaskId: `source-${id}`,
    creationKey: `creation-${id}`,
    spec: { argv: ["/usr/bin/true"], cwd: "/tmp", condition: "the report is ready", intervalMs: 60_000, timeoutMs: 600_000 },
    createdAt: 100,
    deadlineAt: 600_100,
    nextAt: 100,
    checks: 0,
    maxChecks: 10,
    generation: 0,
    failures: 0,
    state: "waiting",
    latest: null,
    matchedEvidence: null,
    process: null,
    wakeTaskId: null,
    wakeStatus: "none",
    stopReason: null,
    matchedAt: null,
    ...overrides,
  };
}

function judgment(stdout = "ready", choice: "satisfied" | "not_satisfied" | "insufficient_evidence" = "satisfied", probability = 0.99): WatchCheckCompletion {
  return { kind: "judgment", collectedAt: 120, stdout, choice, probability };
}

test("watch creation is idempotent, enforces per-chat and per-bot quotas, and keeps records isolated", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    const first = store.createWatch(watch());
    expect(first.kind).toBe("created");
    expect(first.record?.id).toBe("watch-1");
    const replay = store.createWatch(watch({ id: "replayed-id", sourceTaskId: "source-watch-1", creationKey: "creation-watch-1", spec: { argv: ["/bin/false"], cwd: "/var/tmp", condition: "different", intervalMs: 60_000, timeoutMs: 600_000 } }));
    expect(replay.kind).toBe("duplicate");
    expect(replay.record?.id).toBe("watch-1");

    for (let index = 2; index <= 5; index += 1) {
      expect(store.createWatch(watch({ id: `watch-${index}`, creationKey: `creation-${index}` })).kind).toBe("created");
    }
    expect(store.createWatch(watch({ id: "sixth-chat-watch", creationKey: "sixth-chat-watch" })).kind).toBe("quota-chat");

    for (let index = 6; index <= 20; index += 1) {
      const otherTarget: ChatTarget = { channelType: 1, channelId: `private-${index}` };
      expect(store.createWatch(watch({ id: `watch-${index}`, creationKey: `creation-${index}`, target: otherTarget })).kind).toBe("created");
    }
    const botQuota = store.createWatch(watch({ id: "twenty-first", creationKey: "twenty-first", target: { channelType: 1, channelId: "private-over-limit" } }));
    expect(botQuota.kind).toBe("quota-bot");
    expect(store.getWatch(botUid, "watch-1")?.target).toEqual(target);
    expect(store.getWatch(botUid, "watch-6")?.target).toEqual({ channelType: 1, channelId: "private-6" });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("claiming due checks is atomic, globally bounded, and persists the process identity", async () => {
  const root = await tempRoot();
  let store = await openChannelStore(root);
  try {
    for (let index = 1; index <= 4; index += 1) {
      expect(store.createWatch(watch({ id: `watch-${index}`, creationKey: `creation-${index}` })).kind).toBe("created");
    }
    const claimed = store.claimDueWatches(100);
    expect(claimed.map(({ id }) => id)).toEqual(["watch-1", "watch-2"]);
    expect(claimed.map(({ checks, generation, state }) => ({ checks, generation, state }))).toEqual([
      { checks: 1, generation: 1, state: "checking" },
      { checks: 1, generation: 1, state: "checking" },
    ]);
    expect(store.claimDueWatches(100)).toEqual([]);

    const processIdentity = { pid: 41_231, pgid: 41_231, identity: "pid=41231;start=Tue Nov  4 10:00:00 2026", cwd: "/tmp" };
    expect(store.setWatchProcess("watch-1", 1, processIdentity)).toBe(true);
    store.close();
    store = await openChannelStore(root);
    expect(store.getWatch(botUid, "watch-1")?.process).toEqual(processIdentity);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("checking");
    expect(store.claimDueWatches(100)).toEqual([]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("bot-scoped claims share the global two-check budget without claiming another bot's watch", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    for (const id of ["current-1", "current-2"]) {
      expect(store.createWatch(watch({ id, creationKey: id })).kind).toBe("created");
    }
    for (const id of ["other-1", "other-2"]) {
      expect(store.createWatch(watch({ id, creationKey: id, botUid: "other-watch-bot" })).kind).toBe("created");
    }
    expect(store.claimDueWatches(100, 2, botUid).map(({ botUid: owner }) => owner)).toEqual([botUid, botUid]);
    expect(store.claimDueWatches(100, 2, "other-watch-bot")).toEqual([]);
    expect(store.getWatch("other-watch-bot", "other-1")).toMatchObject({ state: "waiting", checks: 0 });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("late check completion cannot overwrite a stopped generation", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    const [claimed] = store.claimDueWatches(100);
    expect(claimed?.generation).toBe(1);
    expect(store.stopWatches(botUid, target, "watch-1", 110, "operator-stop").map(({ state }) => state)).toEqual(["stopped"]);
    expect(store.finishWatchCheck("watch-1", 1, judgment("must not match"), 120)).toBeNull();
    const saved = store.getWatch(botUid, "watch-1");
    expect(saved?.state).toBe("stopped");
    expect(saved?.matchedEvidence).toBeNull();
    expect(saved?.wakeStatus).toBe("none");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("late result past the deadline expires rather than matching and never becomes dispatchable", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch({ deadlineAt: 150 }));
    const [claimed] = store.claimDueWatches(100);
    expect(claimed?.generation).toBe(1);
    expect(store.finishWatchCheck("watch-1", 1, judgment("arrived too late"), 151)).toBeNull();
    const saved = store.getWatch(botUid, "watch-1");
    expect(saved?.state).toBe("expired");
    expect(saved?.matchedEvidence).toBeNull();
    expect(store.wakeAllowed("watch-1", 151)).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("match publication and one-time prompt dispatch survive reopen without duplicate wake", async () => {
  const root = await tempRoot();
  let store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    const [claimed] = store.claimDueWatches(100);
    expect(claimed?.generation).toBe(1);
    const matched = store.finishWatchCheck("watch-1", 1, judgment("verified report"), 130);
    expect(matched?.state).toBe("matched");
    expect(matched?.matchedEvidence).toBe("verified report");
    expect(matched?.wakeStatus).toBe("queued");
    expect(store.getWatchMatchEvent("watch-1")).toMatchObject({
      messageId: "watch:watch-1",
      watchId: "watch-1",
      botUid,
      target,
    });
    expect(store.wakeAllowed("watch-1", 130)).toBe(true);

    store.close();
    store = await openChannelStore(root);
    expect(store.getWatch(botUid, "watch-1")?.wakeStatus).toBe("queued");
    expect(store.markWatchWakeTask("watch-1", "continuation-1", 130)).toBe(true);
    expect(store.markWatchWakeTask("watch-1", "continuation-1", 130)).toBe(true);
    expect(store.markWatchWakeTask("watch-1", "continuation-2", 130)).toBe(false);
    expect(store.markWatchWakeDispatched("watch-1", "continuation-1", 131)).toBe(true);
    expect(store.markWatchWakeDispatched("watch-1", "continuation-1", 132)).toBe(false);
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ wakeTaskId: "continuation-1", wakeStatus: "dispatched" });
    expect(store.stopWatches(botUid, target, "watch-1", 140, "operator-stop")[0]?.wakeStatus).toBe("dispatched");
    expect(store.markWatchWakeDispatched("watch-1", "continuation-1", 141)).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("thread deletion tombstones before late work and blocks all future creates after reopen", async () => {
  const root = await tempRoot();
  let store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    const [claimed] = store.claimDueWatches(100);
    expect(claimed?.state).toBe("checking");
    store.deleteWatchChat(botUid, target, 110);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("deleted");
    expect(store.finishWatchCheck("watch-1", 1, judgment("late"), 120)).toBeNull();
    store.close();
    store = await openChannelStore(root);
    expect(store.isWatchChatDeleted(botUid, target)).toBe(true);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("deleted");
    expect(store.createWatch(watch({ id: "after-delete", creationKey: "after-delete" })).kind).toBe("deleted");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("listing is paged and does not include command or evidence payloads", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch({ spec: { argv: ["secret-command-argument"], cwd: "/tmp", condition: "private completion condition", intervalMs: 60_000, timeoutMs: 600_000 } }));
    store.createWatch(watch({ id: "watch-2", creationKey: "creation-2" }));
    const page = store.listWatches(botUid, target, 1, 0);
    expect(page).toHaveLength(1);
    expect(page[0]?.id).toBe("watch-1");
    expect(page[0]).not.toHaveProperty("spec");
    expect(page[0]).not.toHaveProperty("latest");
    expect(page[0]).not.toHaveProperty("matchedEvidence");
    expect(page[0]).not.toHaveProperty("process");
    expect(store.listWatches(botUid, target, 1, 1).map(({ id }) => id)).toEqual(["watch-2"]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("three consecutive collection or judgment failures terminate without another due check", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    for (const startedAt of [100, 60_100, 120_100]) {
      const [claimed] = store.claimDueWatches(startedAt);
      expect(claimed?.state).toBe("checking");
      const finished = store.finishWatchCheck(
        "watch-1",
        claimed!.generation,
        { kind: "failure", collectedAt: startedAt + 10, code: "collector-failed" },
        startedAt + 10,
      );
      if (startedAt === 120_100) expect(finished?.state).toBe("failed");
      else expect(finished?.state).toBe("waiting");
    }
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "failed", checks: 3, failures: 3 });
    expect(store.claimDueWatches(180_100)).toEqual([]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("unconfirmed processes retain quota, global check capacity, and cwd-use state after stop", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    store.createWatch(watch({
      id: "watch-2",
      creationKey: "creation-2",
      target: { channelType: 1, channelId: "process-chat-2" },
    }));
    const claimed = store.claimDueWatches(100);
    expect(claimed).toHaveLength(2);
    for (const record of claimed) {
      expect(store.setWatchProcess(record.id, record.generation, {
        pid: record.id === "watch-1" ? 41_231 : 41_232,
        pgid: record.id === "watch-1" ? 41_231 : 41_232,
        identity: `pid=${record.id === "watch-1" ? 41_231 : 41_232};start=unknown-test`,
        cwd: "/tmp",
      })).toBe(true);
      expect(store.markWatchUnknown(record.id, record.generation, "cannot-confirm-group-exit", 110)).toBe(true);
    }
    for (let index = 3; index <= 6; index += 1) {
      expect(store.createWatch(watch({ id: `watch-${index}`, creationKey: `creation-${index}` })).kind).toBe("created");
    }
    expect(store.createWatch(watch({ id: "chat-overflow", creationKey: "chat-overflow" })).kind).toBe("quota-chat");
    expect(store.claimDueWatches(200)).toEqual([]);

    const [stopped] = store.stopWatches(botUid, target, "watch-1", 210, "operator-stop");
    expect(stopped?.state).toBe("stopped");
    expect(stopped?.process?.identity).toContain("unknown-test");
    expect(store.claimDueWatches(220)).toEqual([]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("a next interval beyond the deadline waits until the deadline rather than expiring an early result", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch({ deadlineAt: 30_100, maxChecks: 1 }));
    const [claimed] = store.claimDueWatches(100);
    const finished = store.finishWatchCheck(
      "watch-1",
      claimed!.generation,
      { kind: "judgment", collectedAt: 20_100, stdout: "not ready", choice: "not_satisfied", probability: 0.99 },
      20_100,
    );
    expect(finished).toMatchObject({ state: "waiting", nextAt: 60_100, checks: 1 });
    store.expireWatchRecords(30_099);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("waiting");
    store.expireWatchRecords(30_100);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("expired");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("unknown checks without a confirmed identity remain resource blockers across expiry", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    store.createWatch(watch({
      id: "watch-2",
      creationKey: "creation-2",
      target: { channelType: 1, channelId: "identity-unknown-chat" },
    }));
    const claimed = store.claimDueWatches(100);
    expect(claimed).toHaveLength(2);
    for (const record of claimed) {
      expect(store.markWatchUnknown(record.id, record.generation, "identity-unavailable", 110)).toBe(true);
    }
    expect(store.listWatchProcessRecords().filter((record) => record.process === null)).toHaveLength(2);
    expect(store.claimDueWatches(200)).toEqual([]);
    store.expireWatchRecords(600_100);
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("expired");
    expect(store.listWatchProcessRecords().filter((record) => record.process === null)).toHaveLength(2);
    expect(store.claimDueWatches(600_101)).toEqual([]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("an accepted but not yet dispatched match expires and fails the prompt gate", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    store.createWatch(watch());
    const [claimed] = store.claimDueWatches(100);
    expect(store.finishWatchCheck("watch-1", claimed!.generation, judgment("verified"), 130)?.state).toBe("matched");
    expect(store.markWatchWakeTask("watch-1", "wake-task-1", 140)).toBe(true);
    expect(store.wakeAllowed("watch-1", 600_099)).toBe(true);
    store.expireWatchRecords(600_100);
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "expired", wakeTaskId: "wake-task-1", wakeStatus: "cancelled" });
    expect(store.markWatchWakeDispatched("watch-1", "wake-task-1", 600_101)).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("a late unknown result after stop persists a blocker without reviving the tombstone", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  try {
    const records = [
      watch(),
      watch({ id: "watch-2", creationKey: "creation-2", target: { channelType: 1, channelId: "stopped-unknown-chat" } }),
    ];
    for (const record of records) store.createWatch(record);
    const claimed = store.claimDueWatches(100);
    for (const record of claimed) {
      expect(store.stopWatches(botUid, record.target, record.id, 110, "operator-stop")).toHaveLength(1);
      expect(store.markWatchUnknown(record.id, record.generation, "exit-unconfirmed", 120)).toBe(true);
      expect(store.getWatch(botUid, record.id)?.state).toBe("stopped");
    }
    expect(store.listWatchProcessRecords().filter((record) => record.process === null)).toHaveLength(2);
    expect(store.claimDueWatches(200)).toEqual([]);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
