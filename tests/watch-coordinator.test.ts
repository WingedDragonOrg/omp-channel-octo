import { expect, test, vi } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, WorkerProcess } from "../src/contracts.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { WatchRecord } from "../src/watch/types.ts";
import { WatchCoordinator } from "../src/watch/index.ts";

const botUid = "watch-coordinator-bot";
const target: ChatTarget = { channelType: 5, channelId: "coordinator-parent____topic" };
const spawnedProcess: WorkerProcess = {
  pid: 1_234_567,
  pgid: 1_234_567,
  identity: "watch-test-process-identity",
  cwd: "/tmp",
};

function tempRoot(): Promise<string> {
  return mkdtemp(join(tmpdir(), "oco-watch-coordinator-"));
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
    createdAt: 1_000,
    deadlineAt: 601_000,
    nextAt: 1_000,
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

function taskRecord(id: string, messageId: string): TaskRecord {
  return {
    ...target,
    id,
    botUid,
    messageId,
    senderId: "watch-owner",
    text: "Please monitor the report.",
    sessionId: `session-${id}`,
    cwd: "/tmp",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relaySuppressed: false,
  };
}

test("immediate successful check publishes one stable match and queues only an independent wake", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const deliveredSignal = Promise.withResolvers<void>();
  const delivered: string[] = [];
  let acquisitions = 0;
  let releases = 0;
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async (_spec, _signal, onSpawn, onExit) => {
      onSpawn(spawnedProcess);
      onExit();
      return { kind: "success", stdout: "verified report" };
    },
    judge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    permitted: () => true,
    deliver: async (_record, text) => {
      delivered.push(text);
      deliveredSignal.resolve();
      return "wake-task-1";
    },
    acquireUse: async () => {
      acquisitions += 1;
      return () => { releases += 1; };
    },
    now: () => 1_000,
  });
  try {
    store.createWatch(watch());
    await coordinator.start();
    await deliveredSignal.promise;
    await Promise.resolve();
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "matched", wakeTaskId: "wake-task-1", wakeStatus: "accepted" });
    expect(store.getWatchMatchEvent("watch-1")?.messageId).toBe("watch:watch-1");
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toContain("watch:watch-1");
    expect(acquisitions).toBe(1);
    expect(releases).toBe(1);
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("stop revokes before abort; a late successful check cannot match or release an unconfirmed process use", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const collected = Promise.withResolvers<{ signal: AbortSignal; onExit: () => void }>();
  const result = Promise.withResolvers<{ kind: "success"; stdout: string }>();
  let releases = 0;
  let deliveries = 0;
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async (_spec, signal, onSpawn, onExit) => {
      onSpawn(spawnedProcess);
      collected.resolve({ signal, onExit });
      return result.promise;
    },
    judge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    permitted: () => true,
    deliver: async () => { deliveries += 1; return "wake-task-1"; },
    acquireUse: async () => () => { releases += 1; },
    now: () => 1_000,
  });
  try {
    store.createWatch(watch());
    await coordinator.start();
    const running = await collected.promise;
    const stopped = await coordinator.stopWatch(target, "watch-1");
    expect(stopped[0]?.state).toBe("stopped");
    expect(running.signal.aborted).toBe(true);
    expect(store.getWatch(botUid, "watch-1")?.process?.identity).toBe(spawnedProcess.identity);
    expect(releases).toBe(0);

    running.onExit();
    result.resolve({ kind: "success", stdout: "verified report" });
    await coordinator.stop();
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "stopped", process: null, matchedEvidence: null });
    expect(releases).toBe(1);
    expect(deliveries).toBe(0);
  } finally {
    result.resolve({ kind: "success", stdout: "late report" });
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("disabled startup recovers persisted checks before returning and launches no collector", async () => {
  const root = await tempRoot();
  const store = await openChannelStore(root);
  let collections = 0;
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => false,
    collect: async () => { collections += 1; return { kind: "success", stdout: "unexpected" }; },
    judge: undefined,
    permitted: () => true,
    deliver: async () => "unexpected-task",
    acquireUse: async () => () => undefined,
    now: () => 2_000,
  });
  try {
    store.createWatch(watch({
      checks: 1,
      generation: 1,
      state: "checking",
      process: { pid: 2_147_483_000, pgid: 2_147_483_000, identity: "pid=2147483000;start=stale", cwd: "/tmp" },
    }));
    await coordinator.start();
    expect(collections).toBe(0);
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "waiting", process: null, checks: 1, failures: 1 });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("deadline expiry is durable before an in-flight collector is aborted", async () => {
  vi.useFakeTimers();
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const running = Promise.withResolvers<{ signal: AbortSignal; onExit: () => void }>();
  const aborted = Promise.withResolvers<void>();
  const result = Promise.withResolvers<{ kind: "failure"; code: string }>();
  let now = 1_000;
  let releases = 0;
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async (_spec, signal, onSpawn, onExit) => {
      onSpawn(spawnedProcess);
      signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      running.resolve({ signal, onExit });
      return result.promise;
    },
    judge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    permitted: () => true,
    deliver: async () => "unexpected-task",
    acquireUse: async () => () => { releases += 1; },
    now: () => now,
  });
  try {
    store.createWatch(watch({ deadlineAt: 2_000 }));
    await coordinator.start();
    vi.advanceTimersByTime(0);
    const job = await running.promise;
    now = 2_000;
    vi.advanceTimersByTime(1_000);
    await aborted.promise;
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({
      state: "expired",
      process: { identity: spawnedProcess.identity },
    });
    expect(releases).toBe(0);

    job.onExit();
    result.resolve({ kind: "failure", code: "cancelled" });
    await coordinator.stop();
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "expired", process: null });
    expect(releases).toBe(1);
  } finally {
    result.resolve({ kind: "failure", code: "cancelled" });
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
    vi.useRealTimers();
  }
});
test("unidentified unknown result keeps cwd use blocked and is reacquired after restart", async () => {
  vi.useFakeTimers();
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const collected = Promise.withResolvers<void>();
  let collections = 0;
  let acquisitions = 0;
  let releases = 0;
  const common = {
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async () => {
      collections += 1;
      collected.resolve();
      return { kind: "unknown" as const, code: "process-identity-unavailable" };
    },
    judge: undefined,
    permitted: () => true,
    deliver: async () => "unexpected-task",
    acquireUse: async () => {
      acquisitions += 1;
      return () => { releases += 1; };
    },
    now: () => 1_000,
  };
  const coordinator = new WatchCoordinator(common);
  const recovered = new WatchCoordinator({ ...common, enabled: () => false });
  try {
    store.createWatch(watch());
    await coordinator.start();
    vi.advanceTimersByTime(0);
    await collected.promise;
    await coordinator.stop();
    expect(store.getWatch(botUid, "watch-1")?.state).toBe("unknown");
    expect(store.listWatchProcessRecords()).toMatchObject([{ id: "watch-1", process: null }]);
    expect(collections).toBe(1);
    expect(releases).toBe(0);

    await recovered.start();
    expect(collections).toBe(1);
    expect(acquisitions).toBe(2);
  } finally {
    await coordinator.stop();
    await recovered.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
    vi.useRealTimers();
  }
});
test("create uses native tool call identity idempotently and rejects watch wake self-chaining", async () => {
  vi.useFakeTimers();
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async () => ({ kind: "success", stdout: "unused" }),
    judge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    permitted: () => true,
    deliver: async () => null,
    acquireUse: async () => () => undefined,
    now: () => 1_000,
  });
  try {
    const source = taskRecord("source-task", "source-message");
    const sourceClaim: InboundClaim = {
      botUid,
      target,
      messageId: source.messageId,
      senderId: source.senderId,
      text: source.text,
      supported: true,
      kind: "task",
    };
    expect(store.acceptTask(sourceClaim, source)).toBe(true);
    const args = { script: "printf 'ready\\n'", cwd: "/tmp", condition: "the report is ready", interval: 60, timeout: 120 };
    const created = await coordinator.create(source, args, "native-tool-call-1");
    expect(created).toMatchObject({
      kind: "created",
      record: { sourceTaskId: source.id, creationKey: "native-tool-call-1", maxChecks: 2 },
    });
    const replay = await coordinator.create(source, { ...args, script: "printf 'different\\n'" }, "native-tool-call-1");
    expect(replay).toMatchObject({ kind: "duplicate", record: { id: created.record?.id, spec: created.record?.spec } });

    const wake = taskRecord("wake-task", "watch:existing-watch");
    const wakeClaim: InboundClaim = {
      ...sourceClaim,
      messageId: wake.messageId,
      text: wake.text,
    };
    expect(store.acceptTask(wakeClaim, wake)).toBe(true);
    await expect(coordinator.create(wake, args, "native-tool-call-2")).rejects.toThrow("cannot create another watch");
    expect(store.listWatchRecords(botUid)).toHaveLength(1);
  } finally {
    coordinator.seal();
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
    vi.useRealTimers();
  }
});
test("shutdown preserves an unaccepted match instead of marking its wake failed", async () => {
  vi.useFakeTimers();
  const root = await tempRoot();
  const store = await openChannelStore(root);
  const dispatching = Promise.withResolvers<void>();
  const delivered = Promise.withResolvers<string | null>();
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async () => ({ kind: "success", stdout: "unused" }),
    judge: undefined,
    permitted: () => true,
    deliver: async () => {
      dispatching.resolve();
      return delivered.promise;
    },
    acquireUse: async () => () => undefined,
    now: () => 1_000,
  });
  try {
    store.createWatch(watch({
      state: "matched",
      latest: { kind: "judgment", collectedAt: 1_000, stdout: "ready", choice: "satisfied", probability: 0.99 },
      matchedEvidence: "ready",
      matchedAt: 1_000,
      wakeStatus: "queued",
    }));
    await coordinator.start();
    vi.advanceTimersByTime(0);
    await dispatching.promise;
    coordinator.seal();
    const stopping = coordinator.stop();
    delivered.resolve(null);
    await stopping;
    expect(store.getWatch(botUid, "watch-1")?.wakeStatus).toBe("queued");
  } finally {
    delivered.resolve(null);
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
    vi.useRealTimers();
  }
});
test("permission revocation durably stops a persisted watch before it can collect again", async () => {
  vi.useFakeTimers();
  const root = await tempRoot();
  const store = await openChannelStore(root);
  let collections = 0;
  const coordinator = new WatchCoordinator({
    store,
    botUid: () => botUid,
    enabled: () => true,
    collect: async () => {
      collections += 1;
      return { kind: "success", stdout: "unexpected" };
    },
    judge: undefined,
    permitted: () => false,
    deliver: async () => "unexpected-task",
    acquireUse: async () => () => undefined,
    now: () => 1_000,
  });
  try {
    store.createWatch(watch());
    await coordinator.start();
    vi.advanceTimersByTime(0);
    await coordinator.stop();
    expect(collections).toBe(0);
    expect(store.getWatch(botUid, "watch-1")).toMatchObject({ state: "stopped", stopReason: "permission-revoked" });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
    vi.useRealTimers();
  }
});
