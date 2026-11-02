import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createWorkerFactory } from "../src/omp/index";
import type {
  OmpWorker,
  PromptContext,
  WorkerEvent,
  WorkerGoalResult,
  WorkerGoalRun,
  WorkerModelCatalog,
  WorkerModelSwitch,
  WorkerOptions,
} from "../src/contracts";
import {
  findMovedSession,
  loadValidatedSessionState,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "../src/session/project-session";
import { openChannelStore, type ChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes";
type RefreshableWorker = OmpWorker & {
  refreshState(): Promise<ProjectSessionState>;
};

function hasRefreshState(worker: OmpWorker): worker is RefreshableWorker {
  return "refreshState" in worker && typeof worker.refreshState === "function";
}
type Gate = {
  promise: Promise<void>;
  release: () => void;
  reject: (error?: Error) => void;
  wait: () => Promise<void>;
};

function createGate(label = "gate", timeoutMs?: number): Gate {
  let resolvePromise!: () => void;
  let rejectPromise!: (reason?: unknown) => void;
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const clearTimer = (): void => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  const release = (): void => {
    if (settled) return;
    settled = true;
    clearTimer();
    resolvePromise();
  };
  const fail = (error = new Error(`${label} timed out`)): void => {
    if (settled) return;
    settled = true;
    clearTimer();
    rejectPromise(error);
  };
  const wait = (): Promise<void> => {
    if (timeoutMs !== undefined && timer === undefined && !settled) {
      timer = setTimeout(() => fail(), timeoutMs);
    }
    return promise;
  };
  return { promise, release, reject: fail, wait };
}


class DelayedStopWorker implements OmpWorker {
  constructor(
    private readonly inner: OmpWorker,
    private readonly stopStarted: Gate,
    private readonly releaseStop: Gate,
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

  resume(onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    return context === undefined
      ? this.inner.resume(onEvent)
      : this.inner.resume(onEvent, context);
  }
  goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> {
    return this.inner.goal(op);
  }

  runGoal(
    request: WorkerGoalRun,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<WorkerGoalResult> {
    return context === undefined
      ? this.inner.runGoal(request, onEvent)
      : this.inner.runGoal(request, onEvent, context);
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

  models(): Promise<WorkerModelCatalog> {
    return this.inner.models();
  }

  switchModel(selector: string): Promise<WorkerModelSwitch> {
    return this.inner.switchModel(selector);
  }

  stop(): Promise<boolean> {
    this.stopStarted.release();
    return this.releaseStop.promise.then(() => this.inner.stop());
  }
}

class DelayedStopFactory extends FakeWorkerFactory {
  private createCount = 0;

  constructor(
    private readonly promptStarted: Gate,
    private readonly releasePrompt: Gate,
    private readonly stopStarted: Gate,
    private readonly releaseStop: Gate,
  ) {
    super(async ({ onStop, text }) => {
      if (text !== "long") return;
      this.promptStarted.release();
      onStop(() => this.releasePrompt.release());
      await this.releasePrompt.promise;
    });
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const createNumber = ++this.createCount;
    const worker = await super.create(options);
    return createNumber === 2
      ? new DelayedStopWorker(worker, this.stopStarted, this.releaseStop)
      : worker;
  }
}

class StopFalseWorker implements OmpWorker {
  constructor(private readonly inner: OmpWorker) {}

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

  resume(onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    return context === undefined
      ? this.inner.resume(onEvent)
      : this.inner.resume(onEvent, context);
  }
  goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> {
    return this.inner.goal(op);
  }

  runGoal(
    request: WorkerGoalRun,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<WorkerGoalResult> {
    return context === undefined
      ? this.inner.runGoal(request, onEvent)
      : this.inner.runGoal(request, onEvent, context);
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

  models(): Promise<WorkerModelCatalog> {
    return this.inner.models();
  }

  switchModel(selector: string): Promise<WorkerModelSwitch> {
    return this.inner.switchModel(selector);
  }

  stop(): Promise<boolean> {
    return Promise.resolve(false);
  }
}

class MoveStopFailureFactory extends FakeWorkerFactory {
  private createCount = 0;

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const createNumber = ++this.createCount;
    const worker = await super.create(options);
    return createNumber === 2 ? new StopFalseWorker(worker) : worker;
  }
}
class CommandSessionWorker implements OmpWorker {
  constructor(
    private readonly inner: OmpWorker,
    private readonly refreshStarted: Gate,
    private readonly releaseRefresh: Gate,
    private readonly moveTarget: string | undefined,
    private readonly moveStarted: Gate,
    private readonly releaseMove: Gate,
    private readonly onStopped: () => void,
  ) {}

  get sessionFile(): string {
    return this.inner.sessionFile;
  }

  get sessionId(): string {
    return this.inner.sessionId;
  }

  async prompt(
    text: string,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<void> {
    if (this.moveTarget !== undefined && text === `/move ${this.moveTarget}`) {
      this.moveStarted.release();
      await this.releaseMove.promise;
    }
    if (context === undefined) {
      await this.inner.prompt(text, onEvent);
    } else {
      await this.inner.prompt(text, onEvent, context);
    }
  }

  async resume(onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    if (context === undefined) {
      await this.inner.resume(onEvent);
    } else {
      await this.inner.resume(onEvent, context);
    }
  }
  goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> {
    return this.inner.goal(op);
  }

  runGoal(
    request: WorkerGoalRun,
    onEvent: (event: WorkerEvent) => void,
    context?: PromptContext,
  ): Promise<WorkerGoalResult> {
    return context === undefined
      ? this.inner.runGoal(request, onEvent)
      : this.inner.runGoal(request, onEvent, context);
  }

  async steer(text: string): Promise<void> {
    await this.inner.steer(text);
  }

  async abortTurn(): Promise<void> {
    await this.inner.abortTurn();
  }

  async refreshState(): Promise<ProjectSessionState> {
    this.refreshStarted.release();
    await this.releaseRefresh.promise;
    return this.inner.refreshState();
  }

  async models(): Promise<WorkerModelCatalog> {
    return this.inner.models();
  }

  async switchModel(selector: string): Promise<WorkerModelSwitch> {
    return this.inner.switchModel(selector);
  }

  async stop(): Promise<boolean> {
    try {
      return await this.inner.stop();
    } finally {
      this.onStopped();
    }
  }
}

class CommandSessionGateFactory extends FakeWorkerFactory {
  readonly independentTaskStarted = createGate("independent task during command", 1_000);
  readonly commandStarted = createGate("command operation", 1_000);
  readonly releaseCommandCreate = createGate();
  readonly releaseCommandMove = createGate();
  readonly commandRefreshStarted = createGate("command refresh", 1_000);
  readonly releaseCommandRefresh = createGate();
  private commandCwd = "";
  private commandMoveTarget: string | undefined;
  private commandMode: "new" | "move" = "new";
  private commandClaimed = false;
  private commandInFlight = false;
  private independentTaskStartedDuringCommand = false;
  constructor(promptHandler: FakePrompt) {
    super(promptHandler);
  }

  configureNew(cwd: string): void {
    this.commandCwd = cwd;
    this.commandMoveTarget = undefined;
    this.commandMode = "new";
  }

  configureMove(cwd: string, target: string): void {
    this.commandCwd = cwd;
    this.commandMoveTarget = target;
    this.commandMode = "move";
  }

  get didStartIndependentTaskDuringCommand(): boolean {
    return this.independentTaskStartedDuringCommand;
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const isCommand = !this.commandClaimed && options.cwd === this.commandCwd;
    if (!isCommand) {
      if (this.commandInFlight) {
        this.independentTaskStartedDuringCommand = true;
        this.independentTaskStarted.release();
      }
      return super.create(options);
    }
    this.commandClaimed = true;
    this.commandInFlight = true;
    const workerPromise = super.create(options);
    if (this.commandMode === "new") {
      this.commandStarted.release();
      await this.releaseCommandCreate.promise;
    }
    const worker = await workerPromise;
    return new CommandSessionWorker(
      worker,
      this.commandRefreshStarted,
      this.releaseCommandRefresh,
      this.commandMoveTarget,
      this.commandStarted,
      this.releaseCommandMove,
      () => {
        this.commandInFlight = false;
      },
    );
  }
}
class ReconcileGateFactory extends FakeWorkerFactory {
  readonly reconcileStarted = createGate("task reconcile", 1_000);
  readonly releaseReconcile = createGate();
  private gatePredicate: () => boolean = () => false;
  private gateUsed = false;

  constructor(onPrompt: FakePrompt) {
    super(onPrompt);
  }

  setGatePredicate(predicate: () => boolean): void {
    this.gatePredicate = predicate;
  }

  async sessionRootsFor(_cwd: string): Promise<readonly string[]> {
    if (!this.gateUsed && this.gatePredicate()) {
      this.gateUsed = true;
      this.reconcileStarted.release();
      await this.releaseReconcile.promise;
    }
    return [];
  }
}



/**
 * Binding regressions for the channel-level project/session commands.
 * These are intentionally written before the command wiring so the current
 * implementation demonstrates the missing behavior rather than passing by
 * construction.
 */
test("/new without a path reuses the currently bound directory and rotates the session", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-bindings", channelType: 1 as const };
  const project = await realpath(await mkdtemp(join(harness.root, "project-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-new-explicit", ...target, text: `/new "${project}"` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-new-reuse", ...target, text: "/new" }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(project);
    expect(after.sessionId).not.toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/reset without a path creates a fresh session in the existing directory", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-reset", channelType: 1 as const };
  const project = await realpath(await mkdtemp(join(harness.root, "project-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-reset-explicit", ...target, text: `/new ${project}` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-reset-reuse", ...target, text: "/reset" }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(project);
    expect(after.sessionId).not.toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/move keeps the session identity while changing the bound directory", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-move", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "destination-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-move-source", ...target, text: `/new ${source}` }),
    );
    const before = await harness.service.status(target);

    const bindings = await openChannelStore(harness.stateDir);
    try {
      // `/new` 记下这次主动换会话：迟到提交据此判断问题是否属于上一段对话。
      const switched = bindings.getBinding(harness.bot.uid, target)?.sessionSwitchedAt ?? null;
      expect(switched).not.toBeNull();

      await harness.service.handleInbound(
        inbound({ messageId: "bindings-move-destination", ...target, text: `/move ${destination}` }),
      );
      const after = await harness.service.status(target);

      expect(after.cwd).toBe(destination);
      expect(after.sessionId).toBe(before.sessionId);
      // `/move` 沿用会话身份，不覆盖这次主动换会话的时间。
      expect(bindings.getBinding(harness.bot.uid, target)?.sessionSwitchedAt).toBe(switched);
    } finally {
      bindings.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});
for (const occupiedDirectory of ["source", "destination"] as const) {
  test(`/move succeeds while another chat runs in the ${occupiedDirectory} directory`, async () => {
    const taskStarted = createGate(`other chat task (${occupiedDirectory})`, 1_000);
    const releaseTask = createGate();
    const heldTaskText = `move-other-chat-hold-${occupiedDirectory}`;
    const harness = await createCoreHarness({
      prompt: async ({ onStop, text }) => {
        if (text !== heldTaskText) return;
        taskStarted.release();
        onStop(() => releaseTask.release());
        await releaseTask.promise;
      },
    });
    const target = { channelId: `dm-project-move-occupied-${occupiedDirectory}`, channelType: 1 as const };
    const otherTarget = {
      channelId: `dm-project-move-occupied-other-${occupiedDirectory}`,
      channelType: 1 as const,
    };
    const source = await realpath(await mkdtemp(join(harness.root, `move-occupied-source-${occupiedDirectory}-`)));
    const destination = await realpath(
      await mkdtemp(join(harness.root, `move-occupied-destination-${occupiedDirectory}-`)),
    );
    const otherCwd = occupiedDirectory === "source" ? source : destination;
    let store: ChannelStore | undefined;
    let otherTask: Promise<void> | undefined;
    try {
      await harness.service.handleInbound(
        inbound({ messageId: `move-occupied-bind-${occupiedDirectory}`, ...target, text: `/new ${source}` }),
      );
      await harness.service.handleInbound(
        inbound({
          messageId: `move-occupied-other-bind-${occupiedDirectory}`,
          ...otherTarget,
          text: `/new ${otherCwd}`,
        }),
      );
      store = await openChannelStore(harness.stateDir);
      const before = store.getBinding(harness.bot.uid, target);
      const otherBefore = store.getBinding(harness.bot.uid, otherTarget);
      expect(before).not.toBeNull();
      expect(otherBefore).not.toBeNull();
      if (!before?.sessionId || !otherBefore?.sessionId) throw new Error("initial bindings lack session identities");
      expect(before.sessionId).not.toBe(otherBefore.sessionId);

      otherTask = harness.service.handleInbound(
        inbound({
          messageId: `move-occupied-hold-${occupiedDirectory}`,
          ...otherTarget,
          text: heldTaskText,
        }),
      );
      await taskStarted.wait();
      await waitForStatus(harness.service, otherTarget, status => status.state === "running");

      await harness.service.handleInbound(
        inbound({
          messageId: `move-occupied-command-${occupiedDirectory}`,
          ...target,
          text: `/move ${destination}`,
        }),
      );

      const after = store.getBinding(harness.bot.uid, target);
      expect(after?.cwd).toBe(destination);
      expect(after?.sessionId).toBe(before.sessionId);
      if (!after?.sessionFile || !after.sessionId) throw new Error("moved binding lacks persisted session state");
      const movedState = await loadValidatedSessionState(after.sessionFile, after.sessionId);
      expect(movedState.state.cwd).toBe(destination);
      expect(movedState.state.sessionId).toBe(before.sessionId);

      expect(store.getBinding(harness.bot.uid, otherTarget)?.cwd).toBe(otherCwd);
      expect(store.getLatestTask(harness.bot.uid, otherTarget)?.state).toBe("running");
    } finally {
      releaseTask.release();
      await otherTask?.catch(() => {});
      store?.close();
      await closeCoreHarness(harness);
    }
  });
}

test("/new creates an independent session in a directory used by another chat's running task", async () => {
  const taskStarted = createGate("other chat task for /new", 1_000);
  const releaseTask = createGate();
  const heldTaskText = "new-other-chat-hold";
  const harness = await createCoreHarness({
    prompt: async ({ onStop, text }) => {
      if (text !== heldTaskText) return;
      taskStarted.release();
      onStop(() => releaseTask.release());
      await releaseTask.promise;
    },
  });
  const target = { channelId: "dm-project-new-occupied", channelType: 1 as const };
  const otherTarget = { channelId: "dm-project-new-occupied-other", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "new-occupied-source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "new-occupied-destination-")));
  let store: ChannelStore | undefined;
  let otherTask: Promise<void> | undefined;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "new-occupied-bind", ...target, text: `/new ${source}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "new-occupied-other-bind", ...otherTarget, text: `/new ${destination}` }),
    );
    store = await openChannelStore(harness.stateDir);
    const before = store.getBinding(harness.bot.uid, target);
    const otherBefore = store.getBinding(harness.bot.uid, otherTarget);
    expect(before).not.toBeNull();
    expect(otherBefore).not.toBeNull();
    if (!before?.sessionId || !otherBefore?.sessionId) throw new Error("initial bindings lack session identities");
    expect(before.sessionId).not.toBe(otherBefore.sessionId);

    otherTask = harness.service.handleInbound(
      inbound({ messageId: "new-occupied-hold", ...otherTarget, text: heldTaskText }),
    );
    await taskStarted.wait();
    await waitForStatus(harness.service, otherTarget, status => status.state === "running");

    await harness.service.handleInbound(
      inbound({ messageId: "new-occupied-command", ...target, text: `/new ${destination}` }),
    );

    const after = store.getBinding(harness.bot.uid, target);
    expect(after?.cwd).toBe(destination);
    expect(after?.sessionId).not.toBe(before?.sessionId);
    expect(after?.sessionId).not.toBe(otherBefore?.sessionId);
    if (!after?.sessionFile || !after.sessionId) throw new Error("new binding lacks persisted session state");
    const newState = await loadValidatedSessionState(after.sessionFile, after.sessionId);
    expect(newState.state.cwd).toBe(destination);
    expect(newState.state.sessionId).toBe(after.sessionId);

    expect(store.getBinding(harness.bot.uid, otherTarget)?.cwd).toBe(destination);
    expect(store.getLatestTask(harness.bot.uid, otherTarget)?.state).toBe("running");
  } finally {
    releaseTask.release();
    await otherTask?.catch(() => {});
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a running task in the current chat still blocks /move", async () => {
  const taskStarted = createGate("current chat busy task", 1_000);
  const releaseTask = createGate();
  const heldTaskText = "current-chat-busy-hold";
  const harness = await createCoreHarness({
    prompt: async ({ onStop, text }) => {
      if (text !== heldTaskText) return;
      taskStarted.release();
      onStop(() => releaseTask.release());
      await releaseTask.promise;
    },
  });
  const target = { channelId: "dm-project-move-current-busy", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "move-current-busy-source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "move-current-busy-destination-")));
  let store: ChannelStore | undefined;
  let runningTask: Promise<void> | undefined;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "move-current-busy-bind", ...target, text: `/new ${source}` }),
    );
    store = await openChannelStore(harness.stateDir);
    const before = store.getBinding(harness.bot.uid, target);
    expect(before).not.toBeNull();

    runningTask = harness.service.handleInbound(
      inbound({ messageId: "move-current-busy-hold", ...target, text: heldTaskText }),
    );
    await taskStarted.wait();
    await waitForStatus(harness.service, target, status => status.state === "running");

    await harness.service.handleInbound(
      inbound({ messageId: "move-current-busy-command", ...target, text: `/move ${destination}` }),
    );

    expect(store.getBinding(harness.bot.uid, target)).toEqual(before);
    expect(store.getLatestTask(harness.bot.uid, target)?.state).toBe("running");
    expect(harness.workers.prompts.some(({ text }) => text === `/move ${destination}`)).toBe(false);
  } finally {
    releaseTask.release();
    await runningTask?.catch(() => {});
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("different chats retain independent project/session bindings", async () => {
  const harness = await createCoreHarness();
  const first = { channelId: "dm-project-first", channelType: 1 as const };
  const second = { channelId: "dm-project-second", channelType: 1 as const };
  const firstDirectory = await realpath(await mkdtemp(join(harness.root, "first-")));
  const secondDirectory = await realpath(await mkdtemp(join(harness.root, "second-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-isolated-first", ...first, text: `/new ${firstDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-isolated-second", ...second, text: `/new ${secondDirectory}` }),
    );

    const firstStatus = await harness.service.status(first);
    const secondStatus = await harness.service.status(second);
    expect(firstStatus.cwd).toBe(firstDirectory);
    expect(secondStatus.cwd).toBe(secondDirectory);
    expect(firstStatus.sessionId).not.toBe(secondStatus.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});


test("a rejected move leaves the prior binding untouched", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-move-failure", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const missing = join(harness.root, "does-not-exist");
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-failure-source", ...target, text: `/new ${source}` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-failure-missing", ...target, text: `/move ${missing}` }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(before.cwd);
    expect(after.sessionId).toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/move does not replace the binding when worker-ledger finalization fails", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-move-ledger-finalization", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "destination-")));
  let store: ChannelStore | undefined;
  let failureDb: Database | undefined;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "move-ledger-source", ...target, text: `/new ${source}` }),
    );
    store = await openChannelStore(harness.stateDir);
    const before = store.getBinding(harness.bot.uid, target);
    expect(before).not.toBeNull();

    failureDb = new Database(join(harness.stateDir, "channel.sqlite"));
    failureDb.query(`
      CREATE TRIGGER fail_worker_stop
      BEFORE UPDATE OF state ON worker_ledger
      WHEN NEW.state = 'stopped'
      BEGIN
        SELECT RAISE(FAIL, 'fixture worker-ledger finalization failed');
      END
    `).run();
    await harness.service.handleInbound(
      inbound({ messageId: "move-ledger-failure", ...target, text: `/move ${destination}` }),
    );

    expect(store.getBinding(harness.bot.uid, target)).toEqual(before);
    expect(harness.transport.sent.at(-1)?.text).toMatch(/无法|未能|失败/);
    expect(harness.transport.sent.at(-1)?.text).not.toContain("已迁移会话");
  } finally {
    if (failureDb !== undefined) {
      failureDb.query("DROP TRIGGER fail_worker_stop").run();
      failureDb.close();
    }
    store?.close();
    await closeCoreHarness(harness);
  }
});
for (const commandKind of ["new", "move"] as const) {
  test(`another chat can finish a same-cwd task while /${commandKind} is gated`, async () => {
    const holdStarted = createGate(`command hold (${commandKind})`, 1_000);
    const releaseHold = createGate();
    let holdFinished = false;
    let queuedTaskStartedAfterHold = false;
    const workers = new CommandSessionGateFactory(async ({ onStop, text }) => {
      if (text === "command-lease-hold") {
        holdStarted.release();
        onStop(() => releaseHold.release());
        await releaseHold.promise;
        holdFinished = true;
        return;
      }
      if (text === "command-lease-queued") queuedTaskStartedAfterHold = holdFinished;
    });
    const harness = await createCoreHarness({ workers });
    const firstTarget = {
      channelId: `dm-project-command-session-first-${commandKind}`,
      channelType: 1 as const,
    };
    const secondTarget = {
      channelId: `dm-project-command-session-second-${commandKind}`,
      channelType: 1 as const,
    };
    const source = await realpath(await mkdtemp(join(harness.root, `command-session-source-${commandKind}-`)));
    const destination = await realpath(
      await mkdtemp(join(harness.root, `command-session-destination-${commandKind}-`)),
    );
    const commandSource = await realpath(
      await mkdtemp(join(harness.root, `command-session-command-source-${commandKind}-`)),
    );
    let holdTask: Promise<void> | undefined;
    let queuedTask: Promise<void> | undefined;
    let commandTask: Promise<void> | undefined;
    let bindingStore: ChannelStore | undefined;
    let commandSessionId: string | undefined;
    try {
      await harness.service.handleInbound(
        inbound({ messageId: `command-session-bind-${commandKind}`, ...firstTarget, text: `/new ${source}` }),
      );
      const firstBinding = await harness.service.status(firstTarget);
      if (!firstBinding.sessionId) throw new Error("fresh binding did not return a session id");
      bindingStore = await openChannelStore(harness.stateDir);
      const sessionFile = bindingStore.getBinding(harness.bot.uid, firstTarget)?.sessionFile;
      if (!sessionFile) throw new Error("fresh binding did not persist a session file");
      holdTask = harness.service.handleInbound(
        inbound({ messageId: `command-session-hold-${commandKind}`, ...firstTarget, text: "command-lease-hold" }),
      );
      await holdStarted.wait();
      queuedTask = harness.service.handleInbound(
        inbound({
          messageId: `command-session-queued-${commandKind}`,
          ...firstTarget,
          text: "/queue command-lease-queued",
        }),
      );
      await queuedTask;
      await writeFile(
        sessionFile,
        `${JSON.stringify({
          type: "session",
          version: 3,
          id: firstBinding.sessionId,
          cwd: destination,
          previousSessionFiles: [sessionFile],
        })}\n`,
        "utf8",
      );

      if (commandKind === "move") {
        await harness.service.handleInbound(
          inbound({
            messageId: `command-session-command-bind-${commandKind}`,
            ...secondTarget,
            text: `/new ${commandSource}`,
          }),
        );
        const priorCommandBinding = bindingStore.getBinding(harness.bot.uid, secondTarget);
        if (!priorCommandBinding?.sessionId) throw new Error("move target did not persist a session id");
        commandSessionId = priorCommandBinding.sessionId;
        workers.configureMove(commandSource, destination);
      } else {
        workers.configureNew(destination);
      }
      commandTask = harness.service.handleInbound(
        inbound({
          messageId: `command-session-command-${commandKind}`,
          ...secondTarget,
          text: commandKind === "new" ? `/new ${destination}` : `/move ${destination}`,
        }),
      );
      await workers.commandStarted.wait();

      releaseHold.release();
      await workers.independentTaskStarted.wait();
      await harness.workers.waitForPromptCount(2);
      await waitForStatus(harness.service, firstTarget, status => status.state === "completed");

      const firstTask = bindingStore.getLatestTask(harness.bot.uid, firstTarget);
      expect(firstTask?.state).toBe("completed");
      expect(queuedTaskStartedAfterHold).toBe(true);
      const heldPrompt = workers.prompts.find(({ text }) => text === "command-lease-hold");
      const queuedPrompt = workers.prompts.find(({ text }) => text === "command-lease-queued");
      if (!heldPrompt || !queuedPrompt) throw new Error("same-chat task prompts were not both recorded");
      expect(queuedPrompt.cwd).toBe(destination);
      expect(queuedPrompt.sessionId).toBe(heldPrompt.sessionId);
      expect(queuedPrompt.sessionId).toBe(firstBinding.sessionId);
      expect(workers.didStartIndependentTaskDuringCommand).toBe(true);
      if (commandKind === "move") {
        expect(bindingStore.getBinding(harness.bot.uid, secondTarget)).toMatchObject({
          cwd: commandSource,
          sessionId: commandSessionId,
        });
      } else {
        expect(bindingStore.getBinding(harness.bot.uid, secondTarget)).toBeNull();
      }

      if (commandKind === "move") workers.releaseCommandMove.release();
      else workers.releaseCommandCreate.release();
      await workers.commandRefreshStarted.wait();
      workers.releaseCommandRefresh.release();
      await commandTask;

      const commandBinding = bindingStore.getBinding(harness.bot.uid, secondTarget);
      expect(commandBinding?.cwd).toBe(destination);
      if (!commandBinding?.sessionId) throw new Error("completed command did not persist its session id");
      expect(queuedPrompt.sessionId).not.toBe(commandBinding.sessionId);
      if (commandKind === "move") {
        if (!commandSessionId) throw new Error("move command had no original session id");
        expect(commandBinding.sessionId).toBe(commandSessionId);
      } else {
        expect(commandBinding.sessionId).not.toBe(firstBinding.sessionId);
      }
    } finally {
      releaseHold.release();
      workers.releaseCommandCreate.release();
      workers.releaseCommandMove.release();
      workers.releaseCommandRefresh.release();
      await Promise.all(
        [holdTask, queuedTask, commandTask]
          .filter((task): task is Promise<void> => task !== undefined)
          .map(task => task.catch(() => {})),
      );
      bindingStore?.close();
      await closeCoreHarness(harness);
    }
  });
}
test("cancelled queued tasks do not wait on a successor after reconcile resumes", async () => {
  const holdStarted = createGate("reconcile hold", 1_000);
  const releaseHold = createGate();
  const successorStarted = createGate("reconcile successor", 1_000);
  const workers = new ReconcileGateFactory(async ({ onStop, text }) => {
    if (text === "reconcile-hold") {
      holdStarted.release();
      onStop(() => releaseHold.release());
      await releaseHold.promise;
      return;
    }
    if (text === "reconcile-successor") successorStarted.release();
  });
  const harness = await createCoreHarness({ workers });
  const durableStore = await openChannelStore(harness.stateDir);
  workers.setGatePredicate(() =>
    durableStore.listQueuedTasks().some(task => task.messageId === "reconcile-cancelled-task"),
  );
  const earlierTarget = { channelId: "dm-project-reconcile-earlier", channelType: 1 as const };
  const cancelledTarget = { channelId: "dm-project-reconcile-cancelled", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "reconcile-source-")));
  let holdTask: Promise<void> | undefined;
  let earlierTask: Promise<void> | undefined;
  let cancelledTask: Promise<void> | undefined;
  let successorTask: Promise<void> | undefined;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "reconcile-bind-earlier", ...earlierTarget, text: `/new ${source}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "reconcile-bind-cancelled", ...cancelledTarget, text: `/new ${source}` }),
    );
    holdTask = harness.service.handleInbound(
      inbound({ messageId: "reconcile-hold", ...earlierTarget, text: "reconcile-hold" }),
    );
    await holdStarted.wait();
    earlierTask = harness.service.handleInbound(
      inbound({ messageId: "reconcile-earlier-task", ...earlierTarget, text: "reconcile-earlier-task" }),
    );
    await earlierTask;
    cancelledTask = harness.service.handleInbound(
      inbound({ messageId: "reconcile-cancelled-task", ...cancelledTarget, text: "reconcile-cancelled-task" }),
    );
    await cancelledTask;
    await workers.reconcileStarted.wait();
    await harness.service.handleInbound(
      inbound({ messageId: "reconcile-cancel", ...cancelledTarget, text: "/cancel" }),
    );
    successorTask = harness.service.handleInbound(
      inbound({ messageId: "reconcile-successor", ...cancelledTarget, text: "reconcile-successor" }),
    );
    await successorTask;
    workers.releaseReconcile.release();
    releaseHold.release();
    await successorStarted.wait();
  } finally {
    workers.releaseReconcile.release();
    releaseHold.release();
    await Promise.all(
      [holdTask, earlierTask, cancelledTask, successorTask]
        .filter((task): task is Promise<void> => task !== undefined)
        .map(task => task.catch(() => {})),
    );
    durableStore.close();
    await closeCoreHarness(harness);
  }
});
test("a queued task follows its session's moved cwd after its chat predecessor", async () => {
  const holdStarted = createGate();
  const releaseHold = createGate();
  const harness = await createCoreHarness({
    prompt: async ({ onStop, text }) => {
      if (text !== "hold-x") return;
      holdStarted.release();
      onStop(() => releaseHold.release());
      await releaseHold.promise;
    },
  });
  const target = { channelId: "dm-project-queued-move", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "destination-")));
  let firstTask: Promise<void> | undefined;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "queued-move-bind", ...target, text: `/new ${source}` }),
    );
    const before = await harness.service.status(target);
    if (!before.sessionId) throw new Error("fresh binding did not return a session id");
    const beforeStore = await openChannelStore(harness.stateDir);
    const sessionFile = beforeStore.getBinding(harness.bot.uid, target)?.sessionFile;
    beforeStore.close();
    if (!sessionFile) throw new Error("fresh binding did not persist a session file");
    firstTask = harness.service.handleInbound(
      inbound({ messageId: "queued-move-hold", ...target, text: "hold-x" }),
    );
    await holdStarted.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "queued-move-a", ...target, text: "/queue queued-a" }),
    );
    await writeFile(
      sessionFile,
      `${JSON.stringify({
        type: "session",
        version: 3,
        id: before.sessionId,
        cwd: destination,
        previousSessionFiles: [sessionFile],
      })}\n`,
      "utf8",
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queued-move-b", ...target, text: "/queue queued-b" }),
    );
    releaseHold.release();
    await harness.workers.waitForPromptCount(3);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "hold-x",
      "queued-a",
      "queued-b",
    ]);
  } finally {
    releaseHold.release();
    await firstTask?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("move stays busy until a cancelled worker confirms stop", async () => {
  const promptStarted = createGate();
  const releasePrompt = createGate();
  const stopStarted = createGate();
  const releaseStop = createGate();
  const workers = new DelayedStopFactory(promptStarted, releasePrompt, stopStarted, releaseStop);
  const harness = await createCoreHarness({ workers });
  const target = { channelId: "dm-project-stop-pending", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "destination-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "stop-pending-bind", ...target, text: `/new ${source}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "stop-pending-task", ...target, text: "long" }),
    );
    await promptStarted.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "stop-pending-cancel", ...target, text: "/cancel" }),
    );
    await stopStarted.promise;
    await harness.service.handleInbound(
      inbound({
        messageId: "stop-pending-move",
        ...target,
        text: `/move ${destination}`,
      }),
    );
    const status = await harness.service.status(target);
    expect(status.cwd).toBe(source);
    expect(harness.workers.prompts.some(({ text }) => text.startsWith("/move "))).toBe(false);
    expect(
      harness.transport.sent.some(
        ({ target: sentTarget, text }) =>
          sentTarget.channelId === target.channelId && /运行中|排队|停止/.test(text),
      ),
    ).toBe(true);
    releaseStop.release();
    await waitForStatus(harness.service, target, next => next.state === "cancelled");
  } finally {
    releasePrompt.release();
    releaseStop.release();
    await closeCoreHarness(harness);
  }
});

test("move stop uncertainty blocks both source and destination across restart", async () => {
  const bot = { uid: "bot-project-move-stop", name: "Move Stop Bot" };
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-project-move-stop-")));
  const workers = new MoveStopFailureFactory();
  const target = { channelId: "dm-project-move-stop", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  let first: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  let firstClosed = false;
  let bindingStore: ChannelStore | undefined;
  try {
    first = await createCoreHarness({ root, bot, workers });
    await first.service.handleInbound(
      inbound({ messageId: "move-stop-bind", ...target, text: `/new ${source}` }),
    );
    bindingStore = await openChannelStore(first.stateDir);
    const before = bindingStore.getBinding(bot.uid, target);
    expect(before).not.toBeNull();
    await first.service.handleInbound(
      inbound({ messageId: "move-stop-command", ...target, text: `/move ${destination}` }),
    );
    expect(bindingStore.getBinding(bot.uid, target)).toEqual(before);
    expect(first.transport.sent.at(-1)?.text).toMatch(/无法|未能|失败/);
    expect(first.transport.sent.at(-1)?.text).not.toContain("已迁移会话");
    bindingStore.close();
    bindingStore = undefined;
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;
    restarted = await createCoreHarness({ root, bot });
    await restarted.service.handleInbound(
      inbound({ messageId: "move-stop-after-restart", ...target, text: "must not run" }),
    );
    // An unconfirmed worker keeps both directories blocked: the queued task stays queued and
    // never runs — it is neither failed nor silently dropped while the process is unverified.
    await restarted.service.stop();
    expect(restarted.workers.prompts).toEqual([]);
    const store = await openChannelStore(restarted.stateDir);
    try {
      expect(store.getLatestTask(restarted.bot.uid, target)?.state).toBe("queued");
    } finally {
      store.close();
    }
  } finally {
    bindingStore?.close();
    if (first && !firstClosed) await closeCoreHarness(first, { remove: false });
    if (restarted) {
      await closeCoreHarness(restarted);
    } else {
      await rm(root, { recursive: true, force: true });
    }
  }
});

test("OMP refreshState returns the authoritative session file, identity, and header cwd", async () => {
  const tempDir = await realpath(await mkdtemp(join(tmpdir(), "omp-project-state-")));
  const logPath = join(tempDir, "fixture.log");
  const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
  const factory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [
      fixturePath,
      "--fixture-scenario",
      "normal",
      "--fixture-log",
      logPath,
    ],
    startupTimeoutMs: 2_000,
    cancelTimeoutMs: 800,
  });
  let worker: OmpWorker | undefined;
  try {
    worker = await factory.create({
      cwd: tempDir,
      sessionDir: tempDir,
      onSpawn: () => {},
    });
    if (!hasRefreshState(worker)) throw new Error("worker does not expose refreshState");
    const state = await worker.refreshState();
    expect(state.sessionId).toBe(worker.sessionId);
    expect(state.sessionFile).toBe(worker.sessionFile);
    expect(state.cwd).toBe(tempDir);
  } finally {
    await worker?.stop().catch(() => false);
    await rm(tempDir, { recursive: true, force: true });
  }
});
test("external move discovery follows only the exact session identity and breadcrumb", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-scan-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const movedDirectory = join(managedRoot, "moved");
  const decoyDirectory = join(managedRoot, "decoy");
  const oldSessionFile = join(source, "session.jsonl");
  const movedSessionFile = join(movedDirectory, "session.jsonl");
  const decoySessionFile = join(decoyDirectory, "session.jsonl");
  try {
    await mkdir(movedDirectory, { recursive: true });
    await mkdir(decoyDirectory, { recursive: true });
    await writeFile(
      movedSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-1",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await writeFile(
      decoySessionFile,
      `${JSON.stringify({ type: "session", id: "session-1", cwd: destination })}\n`,
      "utf8",
    );
    const binding: ProjectSessionBinding = {
      sessionId: "session-1",
      cwd: source,
      sessionFile: oldSessionFile,
    };
    const found = await findMovedSession(binding, { roots: [managedRoot] });
    expect(found).toEqual({
      sessionId: "session-1",
      cwd: destination,
      sessionFile: movedSessionFile,
    });
    expect(await findMovedSession(binding, { roots: [managedRoot, movedDirectory] })).toEqual({
      sessionId: "session-1",
      cwd: destination,
      sessionFile: movedSessionFile,
    });

    const secondMovedDirectory = join(root, "managed", "moved-again");
    await mkdir(secondMovedDirectory, { recursive: true });
    await writeFile(
      join(secondMovedDirectory, "session.jsonl"),
      `${JSON.stringify({
        type: "session",
        id: "session-1",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    expect(await findMovedSession(binding, { roots: [managedRoot] })).toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("native move and fresh-session verification require matching persisted headers", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-verify-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const sessionFile = join(root, "session.jsonl");
  const previous: ProjectSessionBinding = {
    sessionId: "session-old",
    cwd: source,
    sessionFile: join(source, "old.jsonl"),
  };
  try {
    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-old", cwd: destination })}\n`,
      "utf8",
    );
    const movedState: ProjectSessionState = {
      sessionId: previous.sessionId,
      sessionFile,
      cwd: destination,
    };
    await expect(verifyMovedSession(previous, movedState, destination)).resolves.toEqual({
      sessionId: "session-old",
      cwd: destination,
      sessionFile,
    });

    const freshState: ProjectSessionState = {
      sessionId: "session-new",
      sessionFile,
      cwd: destination,
    };
    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-new", cwd: destination })}\n`,
      "utf8",
    );
    await expect(verifyFreshSession(previous, freshState, destination)).resolves.toEqual({
      sessionId: "session-new",
      cwd: destination,
      sessionFile,
    });

    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-other", cwd: destination })}\n`,
      "utf8",
    );
    await expect(verifyFreshSession(previous, freshState, destination)).rejects.toThrow("session header");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("validated session loading accepts a title slot and stops at the first session record", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-loader-")));
  const cwd = await realpath(await mkdtemp(join(root, "cwd-")));
  const sessionFile = join(root, "session.jsonl");
  try {
    await writeFile(
      sessionFile,
      [
        JSON.stringify({ type: "title", v: 1, title: "session title", updatedAt: new Date().toISOString(), pad: "" }),
        JSON.stringify({ type: "session", id: "session-loader", cwd, previousSessionFiles: ["/tmp/old.jsonl"] }),
        `${"{not-json-after-header}\\n".repeat(1000)}`,
      ].join("\n"),
      "utf8",
    );
    await expect(loadValidatedSessionState(sessionFile, "session-loader")).resolves.toEqual({
      state: {
        sessionId: "session-loader",
        sessionFile,
        cwd,
      },
      previousSessionFiles: ["/tmp/old.jsonl"],
    });
    await expect(loadValidatedSessionState(sessionFile, "wrong-id")).rejects.toThrow("identity");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounded discovery filters basenames before applying the candidate file limit", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-basename-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const oldSessionFile = join(source, "session.jsonl");
  const candidateSessionFile = join(root, "session.jsonl");
  try {
    await writeFile(join(root, "unrelated.jsonl"), `${"{not-json}\\n".repeat(2000)}`, "utf8");
    await writeFile(
      candidateSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-basename",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-basename", cwd: source, sessionFile: oldSessionFile },
        { roots: [root], maxFiles: 1 },
      ),
    ).resolves.toEqual({
      sessionId: "session-basename",
      cwd: destination,
      sessionFile: candidateSessionFile,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("bounded discovery does not traverse artifact subdirectories", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-artifact-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const artifactDirectory = join(managedRoot, "session");
  const artifactSessionFile = join(artifactDirectory, "session.jsonl");
  const directDecoySessionFile = join(managedRoot, "session.jsonl");
  try {
    await mkdir(artifactDirectory, { recursive: true });
    await writeFile(
      directDecoySessionFile,
      `${JSON.stringify({ type: "session", id: "different-session", cwd: destination })}\n`,
      "utf8",
    );
    await writeFile(
      artifactSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-artifact",
        cwd: destination,
        previousSessionFiles: [join(source, "session.jsonl")],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-artifact", cwd: source, sessionFile: join(source, "session.jsonl") },
        { roots: [managedRoot] },
      ),
    ).resolves.toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounded discovery fails closed when the directory candidate limit is exceeded", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-directory-limit-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const sessionDirectory = join(managedRoot, "project");
  const extraDirectory = join(managedRoot, "other-project");
  const oldSessionFile = join(source, "session.jsonl");
  try {
    await mkdir(sessionDirectory, { recursive: true });
    await mkdir(extraDirectory, { recursive: true });
    await writeFile(
      join(sessionDirectory, "session.jsonl"),
      `${JSON.stringify({
        type: "session",
        id: "session-directory-limit",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-directory-limit", cwd: source, sessionFile: oldSessionFile },
        { roots: [managedRoot], maxDirectories: 1 },
      ),
    ).resolves.toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("OMP factory exposes profile-aware session roots without importing OMP runtime helpers", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-")));
  const xdgData = join(root, "xdg-data");
  const customAgent = join(root, "custom-agent");
  const environmentKeys = ["OMP_PROFILE", "PI_PROFILE", "PI_CONFIG_DIR", "PI_CODING_AGENT_DIR", "XDG_DATA_HOME"] as const;
  const previousEnvironment = new Map<string, string | undefined>(
    environmentKeys.map(key => [key, process.env[key]]),
  );
  try {
    await mkdir(join(xdgData, "omp", "profiles", "work"), { recursive: true });
    await mkdir(join(xdgData, "omp", "profiles", "cli"), { recursive: true });
    process.env.OMP_PROFILE = "work";
    process.env.PI_PROFILE = "ignored";
    process.env.PI_CONFIG_DIR = ".omp-test";
    process.env.PI_CODING_AGENT_DIR = customAgent;
    process.env.XDG_DATA_HOME = xdgData;
    const cliFactory = createWorkerFactory({
      ompCommand: process.execPath,
      extraArgs: ["--profile", "cli"],
    });
    if (!cliFactory.sessionRootsFor) throw new Error("OMP factory does not expose sessionRootsFor");
    expect(await cliFactory.sessionRootsFor(root)).toEqual([
      join(xdgData, "omp", "profiles", "cli", "sessions"),
    ]);

    process.env.OMP_PROFILE = "";
    const defaultFactory = createWorkerFactory({ ompCommand: process.execPath });
    if (!defaultFactory.sessionRootsFor) throw new Error("OMP factory does not expose sessionRootsFor");
    expect(await defaultFactory.sessionRootsFor(root)).toEqual([join(customAgent, "sessions")]);
  } finally {
    for (const key of environmentKeys) {
      const value = previousEnvironment.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
});
