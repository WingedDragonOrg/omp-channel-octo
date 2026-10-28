import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import type { OmpWorker, WorkerFactory, WorkerOptions, WorkerProcess } from "../src/contracts.ts";
import { openTrackedWorker, withStoppedWorker } from "../src/runtime/worker-lease.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { WorkerLedgerRecord } from "../src/store/store-records.ts";

interface WorkerLeaseHarness {
  root: string;
  databasePath: string;
  store: ChannelStore;
  workerFactory: WorkerFactory;
  workerOptions: Omit<WorkerOptions, "onSpawn">;
  ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
  process: WorkerProcess;
  moveTargetCwd: string;
  worker: OmpWorker;
  runningProcesses: Set<string>;
  blockedDirectories: Set<string>;
}

function deferred<T>() {
  return Promise.withResolvers<T>();
}

function trackedWorkerOptions(harness: WorkerLeaseHarness, moveTargetCwd?: string) {
  return {
    workerFactory: harness.workerFactory,
    store: harness.store,
    worker: harness.workerOptions,
    ledger: harness.ledger,
    ...(moveTargetCwd === undefined ? {} : { moveTargetCwd }),
    blockDirectory: (cwd: string) => harness.blockedDirectories.add(cwd),
  };
}

interface HarnessOptions {
  stopResult?: boolean;
  recoverResult?: boolean;
}

async function withHarness<T>(
  run: (harness: WorkerLeaseHarness) => Promise<T>,
  options: HarnessOptions = {},
): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-worker-lease-"));
  const databasePath = join(root, "state", "channel.sqlite");
  const store = await ChannelStore.open(databasePath);
  const id = basename(root);
  const botUid = `bot-${id}`;
  const sessionId = `session-${id}`;
  const sessionFile = join(root, "sessions", `${sessionId}.jsonl`);
  const cwd = join(root, "workspace");
  const actualProcessCwd = join(root, "actual-process-cwd");
  const moveTargetCwd = join(root, "move-target");
  const process: WorkerProcess = {
    pid: 41001,
    pgid: 41001,
    identity: `worker-process-${id}`,
    cwd: actualProcessCwd,
  };
  const workerOptions: Omit<WorkerOptions, "onSpawn"> = {
    cwd,
    sessionDir: join(root, "sessions"),
    sessionFile,
  };
  const ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd"> = {
    id: `worker-${id}`,
    botUid,
    sessionId,
    sessionFile,
  };
  const runningProcesses = new Set<string>();
  const blockedDirectories = new Set<string>();
  const worker: OmpWorker = {
    sessionFile,
    sessionId,
    prompt: async () => {},
    resume: async () => {},
    steer: async () => {},
    abortTurn: async () => {},
    refreshState: async () => ({ sessionId, sessionFile, cwd }),
    models: async () => ({ current: null, level: null, available: [] }),
    switchModel: async () => ({ ok: false }),
    stop: async () => {
      if (options.stopResult === false) return false;
      runningProcesses.delete(process.identity);
      return true;
    },
  };
  const workerFactory: WorkerFactory = {
    create: async ({ onSpawn }) => {
      runningProcesses.add(process.identity);
      onSpawn(process);
      return worker;
    },
    recover: async (spawned) => {
      if (options.recoverResult === false) return false;
      return runningProcesses.delete(spawned.identity);
    },
  };

  try {
    return await run({
      root,
      databasePath,
      store,
      workerFactory,
      workerOptions,
      ledger,
      process,
      moveTargetCwd,
      worker,
      runningProcesses,
      blockedDirectories,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

async function persistedWorkers(databasePath: string, botUid: string): Promise<WorkerLedgerRecord[]> {
  const store = await ChannelStore.open(databasePath);
  try {
    return store.listWorkerLedger(botUid);
  } finally {
    store.close();
  }
}

test("成功关闭 worker 后，持久台账不再活跃且目录未被 block", () =>
  withHarness(async (harness) => {
    const lease = await openTrackedWorker(trackedWorkerOptions(harness));

    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
      { id: harness.ledger.id, state: "running", moveTargetCwd: null },
    ]);
    expect(await lease.close()).toBe(true);

    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
    expect(harness.blockedDirectories).toEqual(new Set());
  }));

test("spawn 后创建失败但成功恢复时，结束台账并原样传播创建错误", () =>
  withHarness(async (harness) => {
    const createError = new Error("factory failed after spawn");
    harness.workerFactory.create = async ({ onSpawn }) => {
      harness.runningProcesses.add(harness.process.identity);
      onSpawn(harness.process);
      throw createError;
    };

    const caught = await openTrackedWorker(trackedWorkerOptions(harness)).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(caught).toBe(createError);

    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
    expect(harness.blockedDirectories).toEqual(new Set());
  }));

test("spawn 后创建失败且恢复未确认时，block 实际 cwd 与计划目标并持久化目标", () =>
  withHarness(
    async (harness) => {
      const createError = new Error("factory failed after spawn");
      harness.workerFactory.create = async ({ onSpawn }) => {
        harness.runningProcesses.add(harness.process.identity);
        onSpawn(harness.process);
        throw createError;
      };

      const caught = await openTrackedWorker(
        trackedWorkerOptions(harness, harness.moveTargetCwd),
      ).then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(caught).toBe(createError);

      expect(harness.runningProcesses.has(harness.process.identity)).toBe(true);
      expect([...harness.blockedDirectories].sort()).toEqual(
        [harness.process.cwd, harness.moveTargetCwd].sort(),
      );
      expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
        {
          id: harness.ledger.id,
          state: "running",
          cwd: harness.process.cwd,
          moveTargetCwd: harness.moveTargetCwd,
        },
      ]);
    },
    { recoverResult: false },
  ));

test("close 无法确认停止时，block 实际进程 cwd 与计划目标且台账保留两者", () =>
  withHarness(
    async (harness) => {
      const lease = await openTrackedWorker(
        trackedWorkerOptions(harness, harness.moveTargetCwd),
      );

      expect(await lease.close()).toBe(false);

      expect(harness.runningProcesses.has(harness.process.identity)).toBe(true);
      expect([...harness.blockedDirectories].sort()).toEqual(
        [harness.process.cwd, harness.moveTargetCwd].sort(),
      );
      expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
        {
          id: harness.ledger.id,
          state: "running",
          cwd: harness.process.cwd,
          moveTargetCwd: harness.moveTargetCwd,
        },
      ]);
    },
    { stopResult: false },
  ));

test("并发 close 共用一次进程停止与台账 finalization", () =>
  withHarness(async (harness) => {
    const lease = await openTrackedWorker(trackedWorkerOptions(harness));
    const stopGate = deferred<boolean>();
    const markWorkerStopped = harness.store.markWorkerStopped.bind(harness.store);
    let stopCalls = 0;
    let finalizationCalls = 0;
    harness.worker.stop = async () => {
      stopCalls += 1;
      const stopped = await stopGate.promise;
      if (stopped) harness.runningProcesses.delete(harness.process.identity);
      return stopped;
    };
    harness.store.markWorkerStopped = (id) => {
      finalizationCalls += 1;
      markWorkerStopped(id);
    };

    const firstClose = lease.close();
    const secondClose = lease.close();
    stopGate.resolve(true);
    const results = await Promise.all([firstClose, secondClose]);

    expect(results).toEqual([true, true]);
    expect(await lease.close()).toBe(true);
    expect(stopCalls).toBe(1);
    expect(finalizationCalls).toBe(1);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
  }));

test("并发 close 共用 rejected 台账 finalization 且不把错误降为 false", () =>
  withHarness(async (harness) => {
    const lease = await openTrackedWorker(trackedWorkerOptions(harness));
    const stopGate = deferred<boolean>();
    const finalizationError = new Error("worker ledger finalization failed");
    let stopCalls = 0;
    let finalizationCalls = 0;
    harness.worker.stop = async () => {
      stopCalls += 1;
      const stopped = await stopGate.promise;
      if (stopped) harness.runningProcesses.delete(harness.process.identity);
      return stopped;
    };
    harness.store.markWorkerStopped = () => {
      finalizationCalls += 1;
      throw finalizationError;
    };

    const firstClose = lease.close();
    const secondClose = lease.close();
    const settlements = Promise.allSettled([firstClose, secondClose]);
    stopGate.resolve(true);
    const results = await settlements;

    expect(results).toEqual([
      { status: "rejected", reason: finalizationError },
      { status: "rejected", reason: finalizationError },
    ]);
    const repeatedError = await lease.close().catch((error: unknown) => error);
    expect(repeatedError).toBe(finalizationError);
    expect(stopCalls).toBe(1);
    expect(finalizationCalls).toBe(1);
    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
  }));

test("spawn cleanup ledger failure does not replace the create error", () =>
  withHarness(async (harness) => {
    const createError = new Error("factory failed after spawn");
    harness.workerFactory.create = async ({ onSpawn }) => {
      harness.runningProcesses.add(harness.process.identity);
      onSpawn(harness.process);
      throw createError;
    };
    harness.store.markWorkerStopped = () => {
      throw new Error("worker ledger finalization failed");
    };

    const caught = await openTrackedWorker(trackedWorkerOptions(harness)).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(caught).toBe(createError);
    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
      { id: harness.ledger.id, state: "running" },
    ]);
  }));

test("scoped candidate result waits for stop and durable ledger finalization", () =>
  withHarness(async (harness) => {
    const stopGate = deferred<boolean>();
    const callbackFinished = deferred<void>();
    const stopStarted = deferred<void>();
    const events: string[] = [];
    harness.worker.stop = async () => {
      events.push("stop-started");
      stopStarted.resolve(undefined);
      const stopped = await stopGate.promise;
      if (stopped) harness.runningProcesses.delete(harness.process.identity);
      events.push("stop-confirmed");
      return stopped;
    };
    const markWorkerStopped = harness.store.markWorkerStopped.bind(harness.store);
    harness.store.markWorkerStopped = (id) => {
      markWorkerStopped(id);
      events.push("ledger-finalized");
    };

    const resultPromise = withStoppedWorker(trackedWorkerOptions(harness), async () => {
      events.push("candidate-ready");
      callbackFinished.resolve(undefined);
      return "candidate-session";
    });
    await Promise.all([callbackFinished.promise, stopStarted.promise]);

    expect(events).toEqual(["candidate-ready", "stop-started"]);
    const resultSettled = resultPromise.then(
      () => true,
      () => true,
    );
    expect(await Promise.race([resultSettled, Promise.resolve(false)])).toBe(false);

    stopGate.resolve(true);
    expect(await resultPromise).toBe("candidate-session");
    expect(events).toEqual([
      "candidate-ready",
      "stop-started",
      "stop-confirmed",
      "ledger-finalized",
    ]);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
  }));

test("scoped candidate result is rejected when stop is unconfirmed", () =>
  withHarness(
    async (harness) => {
      await expect(
        withStoppedWorker(
          trackedWorkerOptions(harness, harness.moveTargetCwd),
          async () => "candidate-session",
        ),
      ).rejects.toThrow("worker did not stop");

      expect(harness.runningProcesses.has(harness.process.identity)).toBe(true);
      expect([...harness.blockedDirectories].sort()).toEqual(
        [harness.process.cwd, harness.moveTargetCwd].sort(),
      );
      expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
        {
          id: harness.ledger.id,
          state: "running",
          cwd: harness.process.cwd,
          moveTargetCwd: harness.moveTargetCwd,
        },
      ]);
    },
    { stopResult: false },
  ));

test("scoped candidate does not return a successful result when ledger finalization rejects", () =>
  withHarness(async (harness) => {
    const finalizationError = new Error("worker ledger finalization failed");
    let finalizationCalls = 0;
    harness.store.markWorkerStopped = () => {
      finalizationCalls += 1;
      throw finalizationError;
    };
    const caught = await withStoppedWorker(trackedWorkerOptions(harness), async () => "candidate")
      .then(
        () => undefined,
        (error: unknown) => error,
      );

    expect(caught).toBe(finalizationError);
    expect(finalizationCalls).toBe(1);
    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
      { id: harness.ledger.id, state: "running" },
    ]);
  }));

test("scoped callback rejection still closes the worker and preserves the callback error", () =>
  withHarness(async (harness) => {
    const callbackError = new Error("candidate operation failed");
    let stopCalls = 0;
    let finalizationCalls = 0;
    const markWorkerStopped = harness.store.markWorkerStopped.bind(harness.store);
    harness.worker.stop = async () => {
      stopCalls += 1;
      harness.runningProcesses.delete(harness.process.identity);
      return true;
    };
    harness.store.markWorkerStopped = (id) => {
      finalizationCalls += 1;
      markWorkerStopped(id);
    };

    const caught = await withStoppedWorker(trackedWorkerOptions(harness), async () => {
      throw callbackError;
    }).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(caught).toBe(callbackError);
    expect(stopCalls).toBe(1);
    expect(finalizationCalls).toBe(1);
    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
  }));

test("scoped cleanup ledger rejection takes precedence over a callback error without double closing", () =>
  withHarness(async (harness) => {
    const callbackError = new Error("candidate operation failed");
    const finalizationError = new Error("worker ledger finalization failed");
    let stopCalls = 0;
    let finalizationCalls = 0;
    harness.worker.stop = async () => {
      stopCalls += 1;
      harness.runningProcesses.delete(harness.process.identity);
      return true;
    };
    harness.store.markWorkerStopped = () => {
      finalizationCalls += 1;
      throw finalizationError;
    };

    const caught = await withStoppedWorker(trackedWorkerOptions(harness), async () => {
      throw callbackError;
    }).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(caught).toBe(finalizationError);
    expect(stopCalls).toBe(1);
    expect(finalizationCalls).toBe(1);
  }));
