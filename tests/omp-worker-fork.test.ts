import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { TaskStartupError } from "../src/contracts.ts";
import type { OmpWorker, WorkerFactory, WorkerOptions, WorkerProcess } from "../src/contracts.ts";
import { createWorkerFactory } from "../src/omp/index.ts";
import { openTrackedWorker, type TrackedWorker } from "../src/runtime/worker-lease.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { WorkerLedgerRecord } from "../src/store/store-records.ts";

const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
const sourceSessionId = "source-session";

type NativeForkOptions = Omit<WorkerOptions, "onSpawn">;

type LogEntry = Record<string, unknown>;

interface ForkHarness {
  root: string;
  cwd: string;
  sessionDir: string;
  sourceFile: string;
  sourceContents: string;
  logPath: string;
  factory: WorkerFactory;
  spawned?: WorkerProcess;
}

async function createForkHarness(
  scenario: string,
  fixtureArgs: readonly string[] = [],
): Promise<ForkHarness> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-rpc-fork-")));
  const cwd = join(root, "workspace");
  const sessionDir = join(root, "sessions");
  const stateDir = join(root, "state");
  await Promise.all([mkdir(cwd), mkdir(sessionDir), mkdir(stateDir)]);
  const canonicalCwd = await realpath(cwd);
  const sourceFile = join(root, "source-session.jsonl");
  const sourceContents = `${JSON.stringify({
    type: "session",
    version: 3,
    id: sourceSessionId,
    timestamp: "2026-11-03T00:00:00.000Z",
    cwd: canonicalCwd,
  })}\n`;
  await writeFile(sourceFile, sourceContents, "utf8");
  const logPath = join(root, "fixture.log");
  const factory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [
      fixturePath,
      "--fixture-scenario",
      scenario,
      "--fixture-log",
      logPath,
      "--fixture-source-session-id",
      sourceSessionId,
      ...fixtureArgs,
    ],
    startupTimeoutMs: 2_000,
    cancelTimeoutMs: 800,
  });
  return { root, cwd, sessionDir, sourceFile, sourceContents, logPath, factory };
}

function nativeForkOptions(harness: ForkHarness, signal?: AbortSignal): NativeForkOptions {
  return {
    cwd: harness.cwd,
    sessionDir: harness.sessionDir,
    forkFrom: harness.sourceFile,
    ...(signal === undefined ? {} : { signal }),
  };
}

async function readEntries(path: string): Promise<LogEntry[]> {
  if (!existsSync(path)) return [];
  const content = await readFile(path, "utf8");
  return content.split("\n").filter(Boolean).map(line => JSON.parse(line) as LogEntry);
}

function startupEntry(entries: readonly LogEntry[]): LogEntry {
  const entry = entries.find(value => value.kind === "startup");
  if (!entry) throw new Error("RPC fixture did not record startup");
  return entry;
}

function argumentValue(argv: unknown, name: string): string | undefined {
  if (!Array.isArray(argv)) return undefined;
  for (let index = argv.length - 1; index >= 0; index--) {
    const value: unknown = argv[index];
    if (value === name && typeof argv[index + 1] === "string") return argv[index + 1] as string;
    if (typeof value === "string" && value.startsWith(`${name}=`)) return value.slice(name.length + 1);
  }
  return undefined;
}

function commandTypes(entries: readonly LogEntry[]): string[] {
  return entries.flatMap(entry => {
    const command = entry.command;
    return entry.kind === "command" && command && typeof command === "object" && !Array.isArray(command)
      && typeof (command as LogEntry).type === "string"
      ? [(command as LogEntry).type as string]
      : [];
  });
}

// The real child and SQLite ledger expose no change event; poll those actual boundaries.
async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 1_500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
    throw error;
  }
}

function processGroupIsAlive(pgid: number): boolean {
  try {
    process.kill(-pgid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
    throw error;
  }
}

async function cleanupHarness(
  harness: ForkHarness,
  options: {
    worker?: OmpWorker;
    lease?: TrackedWorker;
    store?: ChannelStore;
    botUid?: string;
  } = {},
): Promise<void> {
  if (options.lease) await options.lease.close().catch(() => false);
  else if (options.worker) await options.worker.stop().catch(() => false);
  const owned = new Map<string, WorkerProcess>();
  if (harness.spawned) owned.set(harness.spawned.identity, harness.spawned);
  if (options.store && options.botUid) {
    for (const ledger of options.store.listWorkerLedger(options.botUid)) owned.set(ledger.identity, ledger);
  }
  for (const processInfo of owned.values()) {
    if (processIsAlive(processInfo.pid)) await harness.factory.recover(processInfo).catch(() => false);
  }
  for (const processInfo of owned.values()) {
    await waitUntil(() => !processIsAlive(processInfo.pid), 1_500).catch(() => {});
  }
  options.store?.close();
  await rm(harness.root, { recursive: true, force: true });
}

function ledgerOptions(harness: ForkHarness, botUid: string) {
  return {
    id: `fork-preparation-${botUid}`,
    botUid,
    sessionId: sourceSessionId,
    sessionFile: harness.sourceFile,
  } satisfies Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
}

test("native fork uses only the requested source and preserves tracked close and ledger boundaries", async () => {
  const conflictingFork = join(tmpdir(), "must-not-be-used-as-fork-source.jsonl");
  const harness = await createForkHarness("fork", ["--fork", conflictingFork]);
  const botUid = `bot-${process.pid}-${Date.now()}`;
  const store = await ChannelStore.open(join(harness.root, "state", "channel.sqlite"));
  let lease: TrackedWorker | undefined;
  try {
    lease = await openTrackedWorker({
      workerFactory: harness.factory,
      store,
      worker: nativeForkOptions(harness),
      ledger: ledgerOptions(harness, botUid),
      blockDirectory: () => {},
    });

    expect(lease.worker.sessionId).not.toBe(sourceSessionId);
    expect(lease.worker.sessionFile).not.toBe(harness.sourceFile);
    expect(existsSync(lease.worker.sessionFile)).toBe(true);
    const state = await lease.worker.refreshState();
    expect(state).toEqual({
      sessionId: lease.worker.sessionId,
      sessionFile: await realpath(lease.worker.sessionFile),
      cwd: await realpath(harness.cwd),
    });
    const header = JSON.parse((await readFile(lease.worker.sessionFile, "utf8")).split("\n")[0]!) as LogEntry;
    expect(header).toMatchObject({ id: lease.worker.sessionId, cwd: await realpath(harness.cwd) });

    const entries = await readEntries(harness.logPath);
    const argv = startupEntry(entries).argv;
    expect(Array.isArray(argv)).toBe(true);
    expect(argumentValue(argv, "--fork")).toBe(harness.sourceFile);
    expect((argv as unknown[]).filter(value => value === "--fork")).toHaveLength(1);
    expect(argv).not.toContain("--resume");
    expect(argv).not.toContain("--prompt");
    expect(commandTypes(entries)).not.toContain("prompt");
    expect(commandTypes(entries)).not.toContain("new_session");
    expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);

    expect(store.listWorkerLedger(botUid)).toMatchObject([
      { id: `fork-preparation-${botUid}`, state: "running" },
    ]);
    const activeProcess = store.listWorkerLedger(botUid)[0]!;
    expect(await lease.close()).toBe(true);
    lease = undefined;
    await waitUntil(() => !processGroupIsAlive(activeProcess.pgid));
    expect(store.listWorkerLedger(botUid)).toEqual([]);
    expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);
  } finally {
    await cleanupHarness(harness, { ...(lease === undefined ? {} : { lease }), store, botUid });
  }
});

test("normal worker creation strips a configured --fork instead of forking an arbitrary file", async () => {
  const conflictingFork = join(tmpdir(), "untrusted-configured-fork.jsonl");
  const harness = await createForkHarness("normal", ["--fork", conflictingFork]);
  let worker: OmpWorker | undefined;
  try {
    worker = await harness.factory.create({
      cwd: harness.cwd,
      sessionDir: harness.sessionDir,
      onSpawn: processInfo => { harness.spawned = processInfo; },
    });
    expect(worker.sessionId).toBe("fixture-session-normal");
    const entries = await readEntries(harness.logPath);
    const argv = startupEntry(entries).argv;
    expect(Array.isArray(argv)).toBe(true);
    expect(argv).not.toContain("--fork");
    expect(commandTypes(entries)).toContain("new_session");
    expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);
  } finally {
    await cleanupHarness(harness, worker === undefined ? {} : { worker });
  }
});

test("rejects simultaneous resume and fork sources before spawning", async () => {
  const harness = await createForkHarness("fork");
  try {
    const failure = await harness.factory.create({
      ...nativeForkOptions(harness),
      sessionFile: harness.sourceFile,
      onSpawn: processInfo => { harness.spawned = processInfo; },
    }).then(
      () => undefined,
      error => error,
    );
    expect(failure).toBeInstanceOf(TaskStartupError);
    expect((failure as TaskStartupError).code).toBe("session-invalid");
    expect(harness.spawned).toBeUndefined();
    expect(existsSync(harness.logPath)).toBe(false);
  } finally {
    await cleanupHarness(harness);
  }
});

test("native fork rejects invalid or source-identical session state", async () => {
  for (const variant of ["missing-file", "mismatched-id", "wrong-cwd", "same-session"] as const) {
    const harness = await createForkHarness("fork", ["--fixture-fork-state", variant]);
    try {
      const failure = await harness.factory.create({
        ...nativeForkOptions(harness),
        onSpawn: processInfo => { harness.spawned = processInfo; },
      }).then(
        () => undefined,
        error => error,
      );
      expect(failure).toBeInstanceOf(TaskStartupError);
      expect((failure as TaskStartupError).code).toBe("session-invalid");
      expect(harness.spawned).toBeDefined();
      expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);
    } finally {
      await cleanupHarness(harness);
    }
  }
});

test("an already-aborted fork startup does not spawn or write the source session", async () => {
  const harness = await createForkHarness("fork");
  const botUid = `bot-aborted-${process.pid}-${Date.now()}`;
  const store = await ChannelStore.open(join(harness.root, "state", "channel.sqlite"));
  const controller = new AbortController();
  controller.abort();
  try {
    const failure = await openTrackedWorker({
      workerFactory: harness.factory,
      store,
      worker: nativeForkOptions(harness, controller.signal),
      ledger: ledgerOptions(harness, botUid),
      blockDirectory: () => {},
    }).then(
      () => undefined,
      error => error,
    );
    expect(failure).toBeInstanceOf(Error);
    expect(harness.spawned).toBeUndefined();
    expect(store.listWorkerLedger(botUid)).toEqual([]);
    expect(existsSync(harness.logPath)).toBe(false);
    expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);
  } finally {
    await cleanupHarness(harness, { store, botUid });
  }
});

test("aborting a fork held before RPC ready stops the child and finalizes its ledger", async () => {
  const harness = await createForkHarness("fork-not-ready");
  const botUid = `bot-cancel-${process.pid}-${Date.now()}`;
  const store = await ChannelStore.open(join(harness.root, "state", "channel.sqlite"));
  const controller = new AbortController();
  const blockedDirectories = new Set<string>();
  const opening = openTrackedWorker({
    workerFactory: harness.factory,
    store,
    worker: nativeForkOptions(harness, controller.signal),
    ledger: ledgerOptions(harness, botUid),
    blockDirectory: cwd => blockedDirectories.add(cwd),
  }).then(
    lease => ({ ok: true as const, lease }),
    error => ({ ok: false as const, error }),
  );
  try {
    await waitUntil(() => store.listWorkerLedger(botUid).some(ledger => ledger.state === "running"));
    await waitUntil(async () => (await readEntries(harness.logPath)).some(entry => entry.kind === "startup"));
    const activeLedger = store.listWorkerLedger(botUid)[0]!;
    controller.abort();
    // Measure the real child: fake timers cannot stop it, and this bound is below startup timeout.
    const abortedAt = Date.now();
    const outcome = await opening;
    expect(Date.now() - abortedAt).toBeLessThan(1_500);
    if (outcome.ok) {
      await outcome.lease.close();
      throw new Error("aborted fork startup unexpectedly returned a worker");
    }
    expect(outcome.error).toBeInstanceOf(Error);
    await waitUntil(() => !processGroupIsAlive(activeLedger.pgid));
    expect(store.listWorkerLedger(botUid)).toEqual([]);
    expect(blockedDirectories).toEqual(new Set());
    const entries = await readEntries(harness.logPath);
    expect(argumentValue(startupEntry(entries).argv, "--fork")).toBe(harness.sourceFile);
    expect(commandTypes(entries)).not.toContain("prompt");
    expect(await readFile(harness.sourceFile, "utf8")).toBe(harness.sourceContents);
  } finally {
    await cleanupHarness(harness, { store, botUid });
  }
});
