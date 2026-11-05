import { execFileSync } from "node:child_process";
import { expect, test } from "bun:test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { CommitResult, CommitWorker, CommitWorkerOptions, InboundMessage } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const gitEnvironment = {
  ...process.env,
  GIT_AUTHOR_NAME: "Thread Merge Recovery Fixture",
  GIT_AUTHOR_EMAIL: "thread-merge-recovery@example.invalid",
  GIT_COMMITTER_NAME: "Thread Merge Recovery Fixture",
  GIT_COMMITTER_EMAIL: "thread-merge-recovery@example.invalid",
};

function git(cwd: string, ...args: string[]): string {
  return execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    env: gitEnvironment,
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

async function createRepository(cwd: string): Promise<void> {
  await mkdir(cwd, { recursive: true });
  git(cwd, "init", "--initial-branch=main");
  git(cwd, "config", "user.name", "Thread Merge Recovery Fixture");
  git(cwd, "config", "user.email", "thread-merge-recovery@example.invalid");
  await writeFile(join(cwd, "tracked.txt"), "initial\n", "utf8");
  git(cwd, "add", "tracked.txt");
  git(cwd, "commit", "--quiet", "-m", "fixture base commit");
}

async function commitNativeChanges(cwd: string): Promise<CommitResult> {
  const headBefore = git(cwd, "rev-parse", "HEAD");
  const alreadyStaged = git(cwd, "diff", "--cached", "--name-only").length > 0;
  if (!alreadyStaged) git(cwd, "add", "--all");
  git(cwd, "commit", "--quiet", "-m", "fixture native commit");
  return { exitCode: 0, headBefore, headAfter: git(cwd, "rev-parse", "HEAD") };
}

interface NativeCommitCall {
  readonly index: number;
  readonly options: CommitWorkerOptions;
  runCalls: number;
  stopCalls: number;
  readonly runStarted: Promise<void>;
  readonly markRunStarted: () => void;
}

class ThreadMergeCommitWorker implements CommitWorker {
  constructor(
    private readonly call: NativeCommitCall,
    private readonly factory: ThreadMergeCommitWorkers,
  ) {}

  run(): Promise<CommitResult> {
    this.call.runCalls += 1;
    this.factory.events.push("native-commit");
    this.call.markRunStarted();
    return this.factory.runHandler(this.call);
  }

  stop(): Promise<boolean> {
    this.call.stopCalls += 1;
    return this.factory.nativeStopHandler(this.call);
  }
}

/** Keeps CoreService and SQLite real while controlling only the native OMP commit worker. */
class ThreadMergeCommitWorkers extends FakeWorkerFactory {
  readonly calls: NativeCommitCall[] = [];
  readonly events: string[] = [];
  runHandler: (call: NativeCommitCall) => Promise<CommitResult> = async call =>
    commitNativeChanges(call.options.cwd);
  nativeStopHandler: (call: NativeCommitCall) => Promise<boolean> = async () => true;

  constructor() {
    super(async () => {});
    this.recovered = true;
  }

  override async createCommit(options: CommitWorkerOptions): Promise<CommitWorker> {
    const runStarted = Promise.withResolvers<void>();
    const call: NativeCommitCall = {
      index: this.calls.length,
      options,
      runCalls: 0,
      stopCalls: 0,
      runStarted: runStarted.promise,
      markRunStarted: () => runStarted.resolve(),
    };
    this.calls.push(call);
    options.onSpawn({
      pid: 950_000 + call.index,
      pgid: 950_000 + call.index,
      identity: `thread-merge-recovery-commit-${call.index}`,
      cwd: options.cwd,
    });
    return new ThreadMergeCommitWorker(call, this);
  }
}

interface MergeHarness {
  readonly harness: CoreHarness;
  readonly workers: ThreadMergeCommitWorkers;
  readonly workspace: string;
  readonly target: { readonly channelId: string; readonly channelType: 5 };
  readonly message: (messageId: string, text?: string) => InboundMessage;
}

async function createMergeHarness(
  suffix: string,
  existing?: Pick<CoreHarness, "root" | "bot">,
): Promise<MergeHarness> {
  const workers = new ThreadMergeCommitWorkers();
  const harness = await createCoreHarness({
    ...(existing === undefined ? {} : { root: existing.root, bot: existing.bot }),
    workers,
  });
  const target = { channelId: `group-allowed____auto-commit-recovery-${suffix}`, channelType: 5 as const };
  const workspace = join(harness.root, "workspace");
  try {
    if (existing === undefined) {
      await createRepository(workspace);
      const store = await openChannelStore(harness.stateDir);
      try {
        store.ensureBinding({
          botUid: harness.bot.uid,
          ...target,
          cwd: workspace,
          sessionId: `auto-commit-recovery-${suffix}-session`,
          sessionFile: null,
          cwdPinned: true,
        });
      } finally {
        store.close();
      }
    }
    if (!harness.worktrees) throw new Error("expected fixture worktrees");
    const worktrees = harness.worktrees;
    const merge = worktrees.merge.bind(worktrees);
    worktrees.merge = async request => {
      workers.events.push("worktree-merge");
      return merge(request);
    };
    return {
      harness,
      workers,
      workspace,
      target,
      message: (messageId, text = "/thread merge") => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [harness.bot.uid],
      }),
    };
  } catch (error) {
    await closeCoreHarness(harness);
    throw error;
  }
}

async function waitForNativeCommitRun(workers: ThreadMergeCommitWorkers): Promise<NativeCommitCall> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    const call = workers.calls[0];
    if (call?.runCalls === 1) return call;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  throw new Error("native commit worker did not start");
}

async function closeAfterRestart(
  first: MergeHarness,
  restarted: MergeHarness | undefined,
  firstStopped: boolean,
): Promise<void> {
  if (!firstStopped) await closeCoreHarness(first.harness, { remove: false });
  if (restarted) {
    await closeCoreHarness(restarted.harness);
  } else {
    await rm(first.harness.root, { recursive: true, force: true });
  }
}

test("an unknown native commit is not replayed or merged after restart and duplicate delivery", async () => {
  const first = await createMergeHarness("unknown-restart");
  const commitFinished = Promise.withResolvers<CommitResult>();
  const loseResult = Promise.withResolvers<void>();
  const stopRequested = Promise.withResolvers<void>();
  let restarted: MergeHarness | undefined;
  let firstStopped = false;
  try {
    await writeFile(join(first.workspace, "untracked.txt"), "commit happened before result was lost\n", "utf8");
    const headBefore = git(first.workspace, "rev-parse", "HEAD");
    first.workers.runHandler = async call => {
      const result = await commitNativeChanges(call.options.cwd);
      commitFinished.resolve(result);
      await loseResult.promise;
      throw new Error("fixture lost the native commit result during shutdown");
    };
    first.workers.nativeStopHandler = async () => {
      stopRequested.resolve();
      return true;
    };
    const message = first.message("auto-commit-unknown-restart-command");
    const receipt = first.harness.service.receive(message);
    await receipt.ackReady;
    const call = await waitForNativeCommitRun(first.workers);
    const commitResult = await commitFinished.promise;
    if (commitResult.headAfter === null) throw new Error("successful fixture commit has no resulting HEAD");
    const stopping = closeCoreHarness(first.harness, { remove: false });
    await stopRequested.promise;
    loseResult.resolve();
    await stopping;
    firstStopped = true;

    expect(call.stopCalls).toBe(1);
    expect(first.workers.calls).toHaveLength(1);
    expect(call.runCalls).toBe(1);
    expect(git(first.workspace, "rev-parse", "HEAD")).not.toBe(headBefore);
    const committedHead = git(first.workspace, "rev-parse", "HEAD");
    expect(git(first.workspace, "status", "--porcelain")).toBe("");
    expect(first.harness.worktrees?.merges).toEqual([]);
    expect(first.workers.events).toEqual(["native-commit"]);

    restarted = await createMergeHarness("unknown-restart", first.harness);
    const redelivery = restarted.harness.service.receive(message);
    await redelivery.ackReady;
    await restarted.harness.service.submitInbound(message);

    expect(restarted.workers.calls).toHaveLength(0);
    expect(git(restarted.workspace, "rev-parse", "HEAD")).toBe(committedHead);
    expect(restarted.harness.worktrees?.merges).toEqual([]);
    expect(restarted.workers.events).toEqual([]);
  } finally {
    loseResult.resolve();
    await closeAfterRestart(first, restarted, firstStopped);
  }
});

test("cancellation before native commit completion leaves the workspace uncommitted and unmerged", async () => {
  const fixture = await createMergeHarness("cancel-before-commit");
  const runGate = Promise.withResolvers<CommitResult>();
  const stopped = Promise.withResolvers<void>();
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "cancel before commit\n", "utf8");
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
    fixture.workers.runHandler = async () => runGate.promise;
    fixture.workers.nativeStopHandler = async () => {
      stopped.resolve();
      runGate.resolve({ exitCode: 130, headBefore, headAfter: headBefore });
      return true;
    };
    const mergeOperation = fixture.harness.service.submitInbound(
      fixture.message("auto-commit-cancel-before-commit-command"),
    );
    const call = await waitForNativeCommitRun(fixture.workers);
    const cancellation = fixture.harness.service.submitInbound(
      fixture.message("auto-commit-cancel-before-commit-stop", "/cancel"),
    );
    await Promise.all([mergeOperation, cancellation, stopped.promise]);

    expect(call.stopCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(headBefore);
    expect(git(fixture.workspace, "status", "--porcelain")).not.toBe("");
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    runGate.resolve({
      exitCode: 130,
      headBefore: git(fixture.workspace, "rev-parse", "HEAD"),
      headAfter: git(fixture.workspace, "rev-parse", "HEAD"),
    });
    await closeCoreHarness(fixture.harness);
  }
});

test("cancellation during the commit-to-merge handoff prevents merge after a successful commit", async () => {
  const first = await createMergeHarness("cancel-handoff-restart");
  const nativeCommitFinished = Promise.withResolvers<CommitResult>();
  const releaseWorkerResult = Promise.withResolvers<void>();
  const stopRequested = Promise.withResolvers<void>();
  let restarted: MergeHarness | undefined;
  let firstStopped = false;
  try {
    await writeFile(join(first.workspace, "untracked.txt"), "commit before handoff cancellation\n", "utf8");
    first.workers.runHandler = async call => {
      const result = await commitNativeChanges(call.options.cwd);
      nativeCommitFinished.resolve(result);
      await releaseWorkerResult.promise;
      return result;
    };
    first.workers.nativeStopHandler = async () => {
      stopRequested.resolve();
      return true;
    };
    const message = first.message("auto-commit-cancel-handoff-command");
    const mergeOperation = first.harness.service.submitInbound(message);
    const call = await waitForNativeCommitRun(first.workers);
    const commitResult = await nativeCommitFinished.promise;
    if (commitResult.headAfter === null) throw new Error("successful fixture commit has no resulting HEAD");
    const cancellation = first.harness.service.submitInbound(
      first.message("auto-commit-cancel-handoff-stop", "/cancel"),
    );
    await stopRequested.promise;
    releaseWorkerResult.resolve();
    await Promise.all([mergeOperation, cancellation]);

    expect(call.stopCalls).toBe(1);
    expect(git(first.workspace, "rev-parse", "HEAD")).toBe(commitResult.headAfter);
    expect(first.harness.worktrees?.merges).toEqual([]);
    expect(first.workers.events).toEqual(["native-commit"]);

    await closeCoreHarness(first.harness, { remove: false });
    firstStopped = true;
    restarted = await createMergeHarness("cancel-handoff-restart", first.harness);
    await restarted.harness.service.submitInbound(message);

    expect(restarted.workers.calls).toHaveLength(0);
    expect(git(restarted.workspace, "rev-parse", "HEAD")).toBe(commitResult.headAfter);
    expect(restarted.harness.worktrees?.merges).toEqual([]);
    expect(restarted.workers.events).toEqual([]);
  } finally {
    releaseWorkerResult.resolve();
    await closeAfterRestart(first, restarted, firstStopped);
  }
});
