import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, realpath, rm, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type {
  BotIdentity,
  ChannelService,
  ChatTarget,
  CoreServiceConfig,
  OmpWorker,
  PromptContext,
  WorkerEvent,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../src/contracts";
import type { ProjectSessionState } from "../src/project-session";
import { createChannelService } from "../src/service";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

interface Gate {
  readonly promise: Promise<void>;
  readonly release: () => void;
}

function gate(): Gate {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

class WorkerProxy implements OmpWorker {
  constructor(
    private readonly inner: OmpWorker,
    private readonly stopOverride: (() => Promise<boolean>) | undefined = undefined,
  ) {}

  get sessionFile(): string {
    return this.inner.sessionFile;
  }

  get sessionId(): string {
    return this.inner.sessionId;
  }

  prompt(text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    return context === undefined
      ? this.inner.prompt(text, onEvent)
      : this.inner.prompt(text, onEvent, context);
  }

  steer(text: string): Promise<void> {
    return this.inner.steer(text);
  }

  abortTurn(): Promise<void> {
    return this.inner.abortTurn();
  }

  refreshState(): Promise<ProjectSessionState> {
    return this.inner.refreshState();
  }

  stop(): Promise<boolean> {
    return this.stopOverride ? this.stopOverride() : this.inner.stop();
  }
}

class CancelDuringCreateFactory extends FakeWorkerFactory {
  readonly createStarted = gate();
  readonly releaseCreate = gate();
  readonly stopCalled = gate();
  readonly releaseStop = gate();

  private createCount = 0;

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const createNumber = ++this.createCount;
    if (createNumber === 1) {
      this.createStarted.release();
      await this.releaseCreate.promise;
    }
    const worker = await super.create(options);
    if (createNumber !== 1) return worker;

    let stopPromise: Promise<boolean> | undefined;
    return new WorkerProxy(worker, async () => {
      if (!stopPromise) {
        this.stopCalled.release();
        stopPromise = this.releaseStop.promise.then(() => worker.stop());
      }
      return stopPromise!;
    });
  }
}

class UnstoppableCreateFactory extends FakeWorkerFactory {
  private createCount = 0;

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const createNumber = ++this.createCount;
    const worker = await super.create(options);
    if (createNumber !== 1) return worker;
    return new WorkerProxy(worker, async () => false);
  }
}

class FailingRecoveryFactory extends FakeWorkerFactory {
  readonly recoverStarted = gate();
  readonly releaseRecover = gate();

  override async recover(_process: WorkerProcess): Promise<boolean> {
    this.recoverStarted.release();
    await this.releaseRecover.promise;
    throw new Error("fixture recovery failure");
  }
}

class SuccessfulRecoveryFactory extends FakeWorkerFactory {
  override recover(_process: WorkerProcess): Promise<boolean> {
    return Promise.resolve(true);
  }
}

class ConcurrentStopFactory extends FakeWorkerFactory {
  readonly promptStarted: Gate;
  readonly releasePrompt: Gate;
  readonly stopCalled: Gate;
  readonly releaseStop: Gate;

  private createCount = 0;

  constructor() {
    const promptStarted = gate();
    const releasePrompt = gate();
    super(async ({ onStop }) => {
      onStop(() => releasePrompt.release());
      promptStarted.release();
      await releasePrompt.promise;
    });
    this.promptStarted = promptStarted;
    this.releasePrompt = releasePrompt;
    this.stopCalled = gate();
    this.releaseStop = gate();
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const createNumber = ++this.createCount;
    const worker = await super.create(options);
    if (createNumber !== 1) return worker;

    let stopPromise: Promise<boolean> | undefined;
    return new WorkerProxy(worker, async () => {
      if (!stopPromise) {
        this.stopCalled.release();
        const stopRequest = worker.stop();
        stopPromise = this.releaseStop.promise.then(() => stopRequest);
      }
      return stopPromise!;
    });
  }
}

class StopObservingTransport extends FakeTransport {
  stopStarted = false;

  override async stop(): Promise<void> {
    this.stopStarted = true;
    await super.stop();
  }
}

class CountingTransport extends FakeTransport {
  private readonly sentWaiters: Array<{ count: number; resolve: () => void }> = [];

  waitForSentCount(count: number): Promise<void> {
    if (this.sent.length >= count) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.sentWaiters.push({ count, resolve });
    });
  }

  override async sendText(target: ChatTarget, text: string, clientMsgNo: string) {
    const receipt = await super.sendText(target, text, clientMsgNo);
    for (const waiter of this.sentWaiters) {
      if (this.sent.length >= waiter.count) waiter.resolve();
    }
    return receipt;
  }
}

class PersistentSessionFactory extends FakeWorkerFactory {
  constructor(private readonly persistentSessionFile: string) {
    super();
  }
  override async create(options: WorkerOptions): Promise<OmpWorker> {
    await mkdir(dirname(this.persistentSessionFile), { recursive: true });
    await writeFile(
      this.persistentSessionFile,
      `${JSON.stringify({ type: "session", version: 3, id: "1", cwd: options.cwd })}\n`,
      "utf8",
    );
    return super.create({ ...options, sessionFile: this.persistentSessionFile });
  }
}

interface BoundaryHarness {
  readonly root: string;
  readonly stateDir: string;
  readonly config: CoreServiceConfig;
  readonly bot: BotIdentity;
  readonly transport: FakeTransport;
  readonly workers: WorkerFactory;
  readonly service: ChannelService;
}

interface BoundaryHarnessOptions {
  root?: string;
  bot?: BotIdentity;
  transport?: FakeTransport;
  workerFactory?: WorkerFactory;
  start?: boolean;
}

async function createBoundaryHarness(options: BoundaryHarnessOptions = {}): Promise<BoundaryHarness> {
  const rootPath = options.root ?? (await mkdtemp(join(tmpdir(), "omp-channel-octo-boundary-")));
  const root = await realpath(rootPath);
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const bot =
    options.bot ??
    options.transport?.bot ?? {
      uid: `bot-boundary-${randomUUID()}`,
      name: "Boundary Bot",
    };
  const transport = options.transport ?? new FakeTransport({ bot });
  const workers = options.workerFactory ?? new FakeWorkerFactory();
  const config: CoreServiceConfig = {
    ownerId: "owner-1",
    allowedGroupIds: ["group-allowed"],
    stateDir,
    homeDir: root,
  };
  const service = await createChannelService({ config, transport, workerFactory: workers });
  if (options.start ?? true) await service.start();
  return { root, stateDir, config, bot, transport, workers, service };
}

async function closeBoundaryHarness(harness: BoundaryHarness, remove = true): Promise<void> {
  await harness.service.stop().catch(() => {});
  if (remove) await rm(harness.root, { recursive: true, force: true });
}

test("same-chat tasks remain FIFO while distinct directories run concurrently", async () => {
  const firstStarted = gate();
  const firstRelease = gate();
  const parallelStarted = gate();
  const parallelRelease = gate();
  const secondStarted = gate();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "directory-a first") {
        firstStarted.release();
        await firstRelease.promise;
      } else if (text === "directory-b parallel") {
        parallelStarted.release();
        await parallelRelease.promise;
      } else if (text === "directory-a second") {
        secondStarted.release();
      }
    },
  });
  try {
    const directoryA = join(harness.root, "directory-a");
    const directoryB = join(harness.root, "directory-b");
    await mkdir(directoryA, { recursive: true });
    await mkdir(directoryB, { recursive: true });
    const firstTarget = { channelId: "dm-directory-a", channelType: 1 as const };
    const secondTarget = { channelId: "dm-directory-b", channelType: 1 as const };

    await harness.service.handleInbound(
      inbound({ messageId: "bind-directory-a", ...firstTarget, text: `/new ${directoryA}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "bind-directory-b", ...secondTarget, text: `/new ${directoryB}` }),
    );

    const firstAccepted = harness.service.handleInbound(
      inbound({ messageId: "directory-a-first", ...firstTarget, text: "directory-a first" }),
    );
    await firstStarted.promise;
    const secondAccepted = harness.service.handleInbound(
      inbound({ messageId: "directory-a-second", ...firstTarget, text: "/queue directory-a second" }),
    );
    const parallelAccepted = harness.service.handleInbound(
      inbound({ messageId: "directory-b-parallel", ...secondTarget, text: "directory-b parallel" }),
    );
    await Promise.all([secondAccepted, parallelAccepted]);
    await parallelStarted.promise;

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "directory-a first",
      "directory-b parallel",
    ]);

    firstRelease.release();
    parallelRelease.release();
    await secondStarted.promise;
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "directory-a first",
      "directory-b parallel",
      "directory-a second",
    ]);
    await firstAccepted;
  } finally {
    firstRelease.release();
    parallelRelease.release();
    await closeCoreHarness(harness);
  }
});

test("cancelling during delayed creation never prompts and keeps the directory queued until stop confirmation", async () => {
  const workers = new CancelDuringCreateFactory();
  const harness = await createCoreHarness({ workers });
  try {
    const firstTarget = { channelId: "dm-create-cancel-first", channelType: 1 as const };
    const secondTarget = { channelId: "dm-create-cancel-second", channelType: 1 as const };
    const firstAccepted = harness.service.handleInbound(
      inbound({ messageId: "create-cancel-first", ...firstTarget, text: "first create" }),
    );
    await workers.createStarted.promise;

    const secondAccepted = harness.service.handleInbound(
      inbound({ messageId: "create-cancel-second", ...secondTarget, text: "second queued" }),
    );
    await secondAccepted;
    await harness.service.handleInbound(
      inbound({ messageId: "create-cancel-command", ...firstTarget, text: "/cancel" }),
    );
    expect(workers.prompts).toHaveLength(0);
    expect(workers.workers).toHaveLength(0);

    workers.releaseCreate.release();
    await workers.stopCalled.promise;
    expect(workers.prompts).toHaveLength(0);
    expect(workers.workers).toHaveLength(1);
    expect((await harness.service.status(secondTarget)).state).toBe("queued");

    workers.releaseStop.release();
    await workers.waitForPromptCount(1);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["second queued"]);
    await firstAccepted;
  } finally {
    workers.releaseCreate.release();
    workers.releaseStop.release();
    await closeCoreHarness(harness);
  }
});

test("a recovery failure after acquiring the Bot lock releases it for a later instance", async () => {
  const bot: BotIdentity = { uid: `bot-recovery-${randomUUID()}`, name: "Recovery Bot" };
  const seedWorkers = new UnstoppableCreateFactory();
  let seed: BoundaryHarness | undefined;
  let failing: BoundaryHarness | undefined;
  let competitor: BoundaryHarness | undefined;
  let fixed: BoundaryHarness | undefined;
  let failedStart: Promise<void> | undefined;
  let root: string | undefined;
  const failingWorkers = new FailingRecoveryFactory();
  try {
    seed = await createBoundaryHarness({ bot, workerFactory: seedWorkers });
    const stateRoot = seed.root;
    root = stateRoot;
    await seed.service.handleInbound(
      inbound({ messageId: "seed-unconfirmed-worker", channelId: "dm-seed", text: "/new" }),
    );
    await closeBoundaryHarness(seed, false);
    seed = undefined;

    failing = await createBoundaryHarness({ root: stateRoot, bot, workerFactory: failingWorkers, start: false });
    failedStart = failing.service.start();
    await failingWorkers.recoverStarted.promise;

    competitor = await createBoundaryHarness({ bot, workerFactory: new FakeWorkerFactory(), start: false });
    await expect(competitor.service.start()).rejects.toThrow();

    failingWorkers.releaseRecover.release();
    await expect(failedStart).rejects.toThrow();

    fixed = await createBoundaryHarness({
      root: stateRoot,
      bot,
      workerFactory: new SuccessfulRecoveryFactory(),
      start: false,
    });
    await fixed.service.start();
  } finally {
    failingWorkers.releaseRecover.release();
    if (failedStart) await failedStart.catch(() => {});
    if (seed) await closeBoundaryHarness(seed, false);
    if (failing) await closeBoundaryHarness(failing, false);
    if (competitor) await closeBoundaryHarness(competitor);
    if (fixed) await closeBoundaryHarness(fixed, false);
    if (root) await rm(root, { recursive: true, force: true });
  }
});

test("concurrent stop calls wait for the worker and reject new work while stopping", async () => {
  const bot: BotIdentity = { uid: `bot-stop-${randomUUID()}`, name: "Stop Bot" };
  const workers = new ConcurrentStopFactory();
  const transport = new StopObservingTransport({ bot });
  const harness = await createBoundaryHarness({ bot, transport, workerFactory: workers });
  let competitor: BoundaryHarness | undefined;
  let firstStop: Promise<void> | undefined;
  let secondStop: Promise<void> | undefined;
  let firstFinished = false;
  let secondFinished = false;
  try {
    const target = { channelId: "dm-stop-running", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "stop-running", ...target, text: "running task" }),
    );
    await workers.promptStarted.promise;

    firstStop = harness.service.stop();
    secondStop = harness.service.stop();
    void firstStop.then(() => {
      firstFinished = true;
    });
    void secondStop.then(() => {
      secondFinished = true;
    });
    await workers.stopCalled.promise;

    await harness.service.handleInbound(
      inbound({ messageId: "stop-during-shutdown", ...target, text: "must not spawn" }),
    );
    expect(workers.workers).toHaveLength(1);
    expect(workers.prompts).toHaveLength(1);
    expect(transport.stopStarted).toBe(false);
    await Promise.resolve();
    expect(firstFinished).toBe(false);
    expect(secondFinished).toBe(false);

    competitor = await createBoundaryHarness({ bot, workerFactory: new FakeWorkerFactory(), start: false });
    await expect(competitor.service.start()).rejects.toThrow();

    workers.releaseStop.release();
    await Promise.all([firstStop, secondStop]);
    expect(firstFinished).toBe(true);
    expect(secondFinished).toBe(true);

    await competitor.service.start();
  } finally {
    workers.releasePrompt.release();
    workers.releaseStop.release();
    if (firstStop || secondStop) {
      await Promise.all([firstStop ?? Promise.resolve(), secondStop ?? Promise.resolve()]);
    }
    if (competitor) await closeBoundaryHarness(competitor);
    await closeBoundaryHarness(harness);
  }
});

test("a missing persisted session file fails the task without replacing its binding", async () => {
  const bot: BotIdentity = { uid: `bot-session-file-${randomUUID()}`, name: "Session Bot" };
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-session-boundary-")));
  const sessionFile = join(root, "state", "sessions", "persisted.jsonl");
  const target = { channelId: "dm-missing-session", channelType: 1 as const };
  const firstWorkers = new PersistentSessionFactory(sessionFile);
  let first: BoundaryHarness | undefined;
  let restarted: BoundaryHarness | undefined;
  try {
    first = await createBoundaryHarness({ root, bot, workerFactory: firstWorkers });
    await first.service.handleInbound(
      inbound({ messageId: "persist-session-binding", ...target, text: "/new" }),
    );
    const original = await first.service.status(target);
    expect(original.sessionId).toEqual(expect.any(String));
    expect(original.cwd).toEqual(join(root, "tmp"));
    await closeBoundaryHarness(first, false);
    first = undefined;
    await unlink(sessionFile);

    const transport = new CountingTransport({ bot });
    const restartedWorkers = new FakeWorkerFactory();
    restarted = await createBoundaryHarness({
      root,
      bot,
      transport,
      workerFactory: restartedWorkers,
    });
    await restarted.service.handleInbound(
      inbound({ messageId: "missing-session-task", ...target, text: "must fail" }),
    );
    await waitForStatus(
      restarted.service,
      target,
      (status) => status.state === "failed" && status.outbox === 0,
    );

    const afterFailure = await restarted.service.status(target);
    expect(afterFailure.state).toBe("failed");
    expect(afterFailure.sessionId).toBe(original.sessionId);
    expect(afterFailure.cwd).toBe(original.cwd);
    expect(restartedWorkers.prompts).toHaveLength(0);
  } finally {
    if (first) await closeBoundaryHarness(first, false);
    if (restarted) await closeBoundaryHarness(restarted, false);
    await rm(root, { recursive: true, force: true });
  }
});
