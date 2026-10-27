import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import type { OmpWorker, WorkerFactory, WorkerOptions, WorkerProcess } from "../src/contracts.ts";
import { openTrackedWorker } from "../src/runtime/worker-lease.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { WorkerLedgerRecord } from "../src/store/store-records.ts";

interface WorkerLeaseHarness {
  root: string;
  databasePath: string;
  store: ChannelStore;
  workerFactory: WorkerFactory;
  workerOptions: Omit<WorkerOptions, "onSpawn">;
  ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state">;
  process: WorkerProcess;
  worker: OmpWorker;
  runningProcesses: Set<string>;
  blockDirectories: readonly string[];
  blockedDirectories: Set<string>;
}

async function withHarness<T>(
  run: (harness: WorkerLeaseHarness) => Promise<T>,
  options: { createError?: Error; stopResult?: boolean } = {},
): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-worker-lease-"));
  const databasePath = join(root, "state", "channel.sqlite");
  const store = await ChannelStore.open(databasePath);
  const id = basename(root);
  const botUid = `bot-${id}`;
  const sessionId = `session-${id}`;
  const sessionFile = join(root, "sessions", `${sessionId}.jsonl`);
  const cwd = join(root, "workspace");
  const moveTargetCwd = join(root, "move-target");
  const process: WorkerProcess = {
    pid: 41001,
    pgid: 41001,
    identity: `worker-process-${id}`,
    cwd,
  };
  const workerOptions: Omit<WorkerOptions, "onSpawn"> = {
    cwd,
    sessionDir: join(root, "sessions"),
    sessionFile,
  };
  const ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state"> = {
    id: `worker-${id}`,
    botUid,
    sessionId,
    sessionFile,
    moveTargetCwd,
  };
  const runningProcesses = new Set<string>();
  const blockedDirectories = new Set<string>();
  const blockDirectories = [cwd, moveTargetCwd];
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
      if (options.createError) throw options.createError;
      return worker;
    },
    recover: async (spawned) => runningProcesses.delete(spawned.identity),
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
      worker,
      runningProcesses,
      blockDirectories,
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
    const lease = await openTrackedWorker({
      workerFactory: harness.workerFactory,
      store: harness.store,
      worker: harness.workerOptions,
      ledger: harness.ledger,
      blockDirectories: harness.blockDirectories,
      blockDirectory: (cwd: string) => harness.blockedDirectories.add(cwd),
    });

    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
      { id: harness.ledger.id, state: "running", moveTargetCwd: harness.ledger.moveTargetCwd },
    ]);
    expect(await lease.close()).toBe(true);

    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
    expect(harness.blockedDirectories).toEqual(new Set());
  }));

test("spawn 后创建失败时恢复 worker、结束持久台账并原样传播创建错误", () =>
  withHarness(async (harness) => {
    const createError = new Error("factory failed after spawn");
    // The factory's external-process substitute starts the process, publishes its complete identity,
    // then fails; recover() can confirm termination by removing that identity from the live set.
    harness.workerFactory.create = async ({ onSpawn }) => {
      harness.runningProcesses.add(harness.process.identity);
      onSpawn(harness.process);
      throw createError;
    };

    const caught = await openTrackedWorker({
      workerFactory: harness.workerFactory,
      store: harness.store,
      worker: harness.workerOptions,
      ledger: harness.ledger,
      blockDirectories: harness.blockDirectories,
      blockDirectory: (cwd: string) => harness.blockedDirectories.add(cwd),
    }).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(caught).toBe(createError);

    expect(harness.runningProcesses.has(harness.process.identity)).toBe(false);
    expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toEqual([]);
    expect(harness.blockedDirectories).toEqual(new Set());
  }));

test("close 无法确认停止时，所有目录均被 block 且持久台账仍活跃", () =>
  withHarness(
    async (harness) => {
      const lease = await openTrackedWorker({
        workerFactory: harness.workerFactory,
        store: harness.store,
        worker: harness.workerOptions,
        ledger: harness.ledger,
        blockDirectories: harness.blockDirectories,
        blockDirectory: (cwd: string) => harness.blockedDirectories.add(cwd),
      });

      expect(await lease.close()).toBe(false);

      expect(harness.runningProcesses.has(harness.process.identity)).toBe(true);
      expect([...harness.blockedDirectories].sort()).toEqual([...harness.blockDirectories].sort());
      expect(await persistedWorkers(harness.databasePath, harness.ledger.botUid)).toMatchObject([
        {
          id: harness.ledger.id,
          state: "running",
          cwd: harness.workerOptions.cwd,
          moveTargetCwd: harness.ledger.moveTargetCwd,
        },
      ]);
    },
    { stopResult: false },
  ));
