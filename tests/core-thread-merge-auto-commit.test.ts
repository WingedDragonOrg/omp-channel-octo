import { execFileSync } from "node:child_process";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CommitResult, CommitWorker, CommitWorkerOptions, InboundMessage } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  waitForIngressJob,
  type CoreHarness,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";

const gitEnvironment = {
  ...process.env,
  GIT_AUTHOR_NAME: "Thread Merge Fixture",
  GIT_AUTHOR_EMAIL: "thread-merge-fixture@example.invalid",
  GIT_COMMITTER_NAME: "Thread Merge Fixture",
  GIT_COMMITTER_EMAIL: "thread-merge-fixture@example.invalid",
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
  git(cwd, "config", "user.name", "Thread Merge Fixture");
  git(cwd, "config", "user.email", "thread-merge-fixture@example.invalid");
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

  constructor(prompt: FakePrompt = async () => {}) {
    super(prompt);
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
      pid: 940_000 + call.index,
      pgid: 940_000 + call.index,
      identity: `thread-merge-native-commit-${call.index}`,
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
  readonly unknownWorkerLedgerId: string | undefined;
  readonly message: (messageId: string, text?: string) => InboundMessage;
}

interface MergeHarnessOptions {
  readonly prompt?: FakePrompt;
  readonly seedUnknownWorkerLedger?: boolean;
}

async function createMergeHarness(
  suffix: string,
  options: MergeHarnessOptions = {},
): Promise<MergeHarness> {
  const workers = new ThreadMergeCommitWorkers(options.prompt);
  const target = { channelId: `group-allowed____auto-commit-${suffix}`, channelType: 5 as const };
  const unknownWorkerLedgerId = options.seedUnknownWorkerLedger
    ? `unknown-process-ledger-${suffix}`
    : undefined;
  const root = options.seedUnknownWorkerLedger
    ? await mkdtemp(join(tmpdir(), "omp-channel-octo-merge-auto-commit-"))
    : undefined;
  let harness: CoreHarness | undefined;
  let workspace: string | undefined;
  try {
    if (root === undefined) {
      harness = await createCoreHarness({ workers });
      workspace = join(harness.root, "workspace");
      await createRepository(workspace);
    } else {
      const bot = { uid: `thread-merge-${suffix}-bot`, name: "Fixture Bot" };
      workspace = join(root, "workspace");
      await mkdir(join(root, "state"), { recursive: true });
      await createRepository(workspace);
      const setupStore = await openChannelStore(join(root, "state"));
      try {
        setupStore.recordWorker({
          id: unknownWorkerLedgerId!,
          botUid: bot.uid,
          sessionId: `unrelated-session-${suffix}`,
          sessionFile: null,
          pid: 940_777,
          pgid: 940_777,
          identity: `unconfirmed-other-chat-process-${suffix}`,
          cwd: workspace,
          moveTargetCwd: null,
          state: "unknown",
        });
      } finally {
        setupStore.close();
      }
      harness = await createCoreHarness({ workers, root, bot });
    }
    if (harness === undefined || workspace === undefined) {
      throw new Error("merge fixture initialization did not produce a harness and workspace");
    }
    const activeHarness = harness;
    const activeWorkspace = workspace;
    const store = await openChannelStore(activeHarness.stateDir);
    try {
      store.ensureBinding({
        botUid: activeHarness.bot.uid,
        ...target,
        cwd: activeWorkspace,
        sessionId: `auto-commit-${suffix}-session`,
        sessionFile: null,
        cwdPinned: true,
      });
    } finally {
      store.close();
    }
    if (!activeHarness.worktrees) throw new Error("expected fixture worktrees");
    const worktrees = activeHarness.worktrees;
    const merge = worktrees.merge.bind(worktrees);
    worktrees.merge = async request => {
      workers.events.push("worktree-merge");
      return merge(request);
    };
    return {
      harness: activeHarness,
      workers,
      workspace: activeWorkspace,
      target,
      unknownWorkerLedgerId,
      message: (messageId, text = "/thread merge") => inbound({
        ...target,
        messageId,
        text,
        mentionedUserIds: [activeHarness.bot.uid],
      }),
    };
  } catch (error) {
    if (harness !== undefined) await closeCoreHarness(harness);
    else if (root !== undefined) await rm(root, { recursive: true, force: true });
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

async function waitForThreadArchive(harness: CoreHarness): Promise<void> {
  await harness.transport.waitForManagementCalls(({ action }) => action === "thread-archive");
}

/** Wait for the command's visible result; `submitInbound` only confirms durable acceptance. */
async function waitForThreadMergeResult(
  harness: CoreHarness,
  target: MergeHarness["target"],
): Promise<void> {
  await harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
    sentTarget.channelType === target.channelType
    && sentTarget.channelId === target.channelId
    && text.length > 0);
}

async function startNativeCommitOrThrow(
  fixture: MergeHarness,
  message: InboundMessage,
): Promise<{ readonly call: NativeCommitCall; readonly visibleResult: Promise<void> }> {
  const visibleResult = waitForThreadMergeResult(fixture.harness, fixture.target);
  await fixture.harness.service.submitInbound(message);
  const first = await Promise.race([
    waitForNativeCommitRun(fixture.workers).then(call => ({ kind: "started" as const, call })),
    visibleResult.then(() => ({ kind: "result" as const })),
  ]);
  if (first.kind !== "started") {
    throw new Error(
      `dirty /thread merge produced a result before native auto-commit started: ${
        fixture.harness.transport.sent.map(({ text }) => text).join(" | ")
      }`,
    );
  }
  return { call: first.call, visibleResult };
}




test("dirty /thread merge commits through one native worker before merging and deduplicates redelivery", async () => {
  const fixture = await createMergeHarness("dirty-deduplicated");
  const releaseCommit = Promise.withResolvers<void>();
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "native commit input\n", "utf8");
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
    fixture.workers.runHandler = async call => {
      await releaseCommit.promise;
      return commitNativeChanges(call.options.cwd);
    };
    const message = fixture.message("auto-commit-dirty-command");
    const first = fixture.harness.service.submitInbound(message);
    const call = await waitForNativeCommitRun(fixture.workers);
    const duplicate = fixture.harness.service.submitInbound(message);
    releaseCommit.resolve();
    await Promise.all([first, duplicate]);
    await fixture.harness.service.submitInbound(message);
    await waitForThreadArchive(fixture.harness);

    expect(fixture.workers.calls).toHaveLength(1);
    expect(call.runCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).not.toBe(headBefore);
    expect(git(fixture.workspace, "status", "--porcelain")).toBe("");
    expect(fixture.workers.events).toEqual(["native-commit", "worktree-merge"]);
    expect(fixture.harness.worktrees?.merges).toHaveLength(1);
  } finally {
    releaseCommit.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("dirty /thread merge does not wait for another chat using the same repository", async () => {
  const otherTaskStarted = Promise.withResolvers<void>();
  const releaseOtherTask = Promise.withResolvers<void>();
  let otherTaskFinished = false;
  const fixture = await createMergeHarness("other-chat-same-repo", {
    prompt: async ({ text }) => {
      if (text !== "other chat same-repository activity") return;
      otherTaskStarted.resolve();
      await releaseOtherTask.promise;
      otherTaskFinished = true;
    },
  });
  const releaseCommit = Promise.withResolvers<void>();
  const otherChat = { channelId: "dm-owner", channelType: 1 as const };
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "native commit input\n", "utf8");
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
    const store = await openChannelStore(fixture.harness.stateDir);
    try {
      store.ensureBinding({
        botUid: fixture.harness.bot.uid,
        ...otherChat,
        cwd: fixture.workspace,
        sessionId: "other-chat-same-repo-session",
        sessionFile: null,
        cwdPinned: true,
      });
    } finally {
      store.close();
    }
    fixture.workers.runHandler = async call => {
      await releaseCommit.promise;
      return commitNativeChanges(call.options.cwd);
    };
    await fixture.harness.service.submitInbound(inbound({
      ...otherChat,
      messageId: "other-chat-same-repo-task",
      text: "other chat same-repository activity",
    }));
    await otherTaskStarted.promise;

    const merge = await startNativeCommitOrThrow(
      fixture,
      fixture.message("other-chat-same-repo-merge"),
    );
    releaseCommit.resolve();
    await Promise.all([merge.visibleResult, waitForThreadArchive(fixture.harness)]);

    expect(otherTaskFinished).toBe(false);
    expect(merge.call.runCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).not.toBe(headBefore);
    expect(git(fixture.workspace, "status", "--porcelain")).toBe("");
    expect(fixture.workers.events).toEqual(["native-commit", "worktree-merge"]);
    expect(fixture.harness.worktrees?.merges).toHaveLength(1);
  } finally {
    releaseCommit.resolve();
    releaseOtherTask.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("dirty /thread merge does not wait for an unknown worker process ledger", async () => {
  const fixture = await createMergeHarness("unknown-process-ledger", {
    seedUnknownWorkerLedger: true,
  });
  const releaseCommit = Promise.withResolvers<void>();
  try {
    const workerId = fixture.unknownWorkerLedgerId;
    if (workerId === undefined) throw new Error("expected an unknown worker ledger fixture");
    const store = await openChannelStore(fixture.harness.stateDir);
    try {
      expect(store.getWorkerLedger(fixture.harness.bot.uid, workerId)).toMatchObject({
        state: "unknown",
        sessionId: "unrelated-session-unknown-process-ledger",
        cwd: fixture.workspace,
      });
    } finally {
      store.close();
    }

    await writeFile(join(fixture.workspace, "untracked.txt"), "native commit input\n", "utf8");
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
    fixture.workers.runHandler = async call => {
      await releaseCommit.promise;
      return commitNativeChanges(call.options.cwd);
    };
    const merge = await startNativeCommitOrThrow(
      fixture,
      fixture.message("unknown-process-ledger-merge"),
    );
    releaseCommit.resolve();
    await Promise.all([merge.visibleResult, waitForThreadArchive(fixture.harness)]);

    expect(merge.call.runCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).not.toBe(headBefore);
    expect(git(fixture.workspace, "status", "--porcelain")).toBe("");
    expect(fixture.workers.events).toEqual(["native-commit", "worktree-merge"]);
    expect(fixture.harness.worktrees?.merges).toHaveLength(1);
  } finally {
    releaseCommit.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("dirty /thread merge waits for earlier work in the exact chat session", async () => {
  const earlierTaskStarted = Promise.withResolvers<void>();
  const releaseEarlierTask = Promise.withResolvers<void>();
  let earlierTaskFinished = false;
  const fixture = await createMergeHarness("same-session-order", {
    prompt: async ({ text }) => {
      if (text !== "earlier same-session work") return;
      earlierTaskStarted.resolve();
      await releaseEarlierTask.promise;
      earlierTaskFinished = true;
    },
  });
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "native commit input\n", "utf8");
    const earlierTask = fixture.message("same-session-earlier-task", "earlier same-session work");
    await fixture.harness.service.submitInbound(earlierTask);
    await earlierTaskStarted.promise;

    const mergeMessage = fixture.message("same-session-merge");
    await fixture.harness.service.submitInbound(mergeMessage);
    await waitForIngressJob(fixture.harness, mergeMessage, {
      predicate: job =>
        job.operationCheckpoint?.kind === "command"
        && job.operationCheckpoint.action === "thread"
        && job.operationCheckpoint.phase === "waiting-session",
      description: "dirty thread merge waiting for earlier same-session work",
    });
    expect(fixture.workers.calls).toHaveLength(0);
    expect(fixture.harness.worktrees?.merges).toEqual([]);

    releaseEarlierTask.resolve();
    const call = await waitForNativeCommitRun(fixture.workers);
    await waitForThreadArchive(fixture.harness);

    expect(earlierTaskFinished).toBe(true);
    expect(call.runCalls).toBe(1);
    expect(fixture.workers.events).toEqual(["native-commit", "worktree-merge"]);
    expect(fixture.harness.worktrees?.merges).toHaveLength(1);
  } finally {
    releaseEarlierTask.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("a clean /thread merge preserves the existing path without starting native commit", async () => {
  const fixture = await createMergeHarness("clean");
  try {
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
    await fixture.harness.service.submitInbound(fixture.message("auto-commit-clean-command"));
    await waitForThreadArchive(fixture.harness);

    expect(fixture.workers.calls).toHaveLength(0);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(headBefore);
    expect(fixture.workers.events).toEqual(["worktree-merge"]);
    expect(fixture.harness.worktrees?.merges).toHaveLength(1);
  } finally {
    await closeCoreHarness(fixture.harness);
  }
});

test("a failed native commit prevents /thread merge", async () => {
  const fixture = await createMergeHarness("commit-failure");
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "uncommitted\n", "utf8");
    const head = git(fixture.workspace, "rev-parse", "HEAD");
    fixture.workers.runHandler = async call => ({
      exitCode: 1,
      headBefore: git(call.options.cwd, "rev-parse", "HEAD"),
      headAfter: head,
    });

    const message = fixture.message("auto-commit-failure-command");
    const result = waitForThreadMergeResult(fixture.harness, fixture.target);
    await fixture.harness.service.submitInbound(message);
    await result;

    expect(fixture.workers.calls).toHaveLength(1);
    expect(fixture.workers.calls[0]?.runCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(head);
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    await closeCoreHarness(fixture.harness);
  }
});

test("a concurrent write before native commit confirmation prevents worktree merge from staging it", async () => {
  const fixture = await createMergeHarness("concurrent-write");
  const nativeCommitFinished = Promise.withResolvers<CommitResult>();
  const releaseWorkerResult = Promise.withResolvers<void>();
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "commit snapshot\n", "utf8");
    fixture.workers.runHandler = async call => {
      const result = await commitNativeChanges(call.options.cwd);
      nativeCommitFinished.resolve(result);
      await releaseWorkerResult.promise;
      return result;
    };
    const operation = fixture.harness.service.submitInbound(
      fixture.message("auto-commit-concurrent-write-command"),
    );
    await waitForNativeCommitRun(fixture.workers);
    const commitResult = await nativeCommitFinished.promise;
    if (commitResult.headAfter === null) throw new Error("successful fixture commit has no resulting HEAD");
    await writeFile(join(fixture.workspace, "concurrent.txt"), "must not be staged by wt\n", "utf8");
    releaseWorkerResult.resolve();
    await operation;

    expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(commitResult.headAfter);
    expect(git(fixture.workspace, "status", "--porcelain")).not.toBe("");
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    releaseWorkerResult.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("cancel between native commit completion and result handoff prevents worktree merge", async () => {
  const fixture = await createMergeHarness("cancel-commit-handoff");
  const nativeCommitFinished = Promise.withResolvers<CommitResult>();
  const releaseWorkerResult = Promise.withResolvers<void>();
  const stopRequested = Promise.withResolvers<void>();
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "commit input\n", "utf8");
    fixture.workers.runHandler = async call => {
      const result = await commitNativeChanges(call.options.cwd);
      nativeCommitFinished.resolve(result);
      await releaseWorkerResult.promise;
      return result;
    };
    fixture.workers.nativeStopHandler = async () => {
      stopRequested.resolve();
      return true;
    };
    const mergeOperation = fixture.harness.service.submitInbound(
      fixture.message("auto-commit-handoff-command"),
    );
    const call = await waitForNativeCommitRun(fixture.workers);
    const commitResult = await nativeCommitFinished.promise;
    if (commitResult.headAfter === null) throw new Error("successful fixture commit has no resulting HEAD");
    const cancellation = fixture.harness.service.submitInbound(
      fixture.message("auto-commit-handoff-cancel", "/cancel"),
    );
    await stopRequested.promise;
    releaseWorkerResult.resolve();
    await Promise.all([mergeOperation, cancellation]);

    expect(call.stopCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(commitResult.headAfter);
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    releaseWorkerResult.resolve();
    await closeCoreHarness(fixture.harness);
  }
});

test("an unknown native commit outcome never enters worktree merge", async () => {
  const fixture = await createMergeHarness("unknown-commit");
  try {
    await writeFile(join(fixture.workspace, "untracked.txt"), "unknown commit outcome\n", "utf8");
    fixture.workers.runHandler = async () => {
      throw new Error("native commit process result is unknown");
    };

    const message = fixture.message("auto-commit-unknown-command");
    const result = waitForThreadMergeResult(fixture.harness, fixture.target);
    await fixture.harness.service.submitInbound(message);
    await result;

    expect(fixture.workers.calls).toHaveLength(1);
    expect(fixture.workers.calls[0]?.runCalls).toBe(1);
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    await closeCoreHarness(fixture.harness);
  }
});

for (const stopText of ["/cancel", "/commit stop"] as const) {
  test(`${stopText} stops native auto-commit before /thread merge`, async () => {
    const fixture = await createMergeHarness(`cancel-${stopText.slice(1).replaceAll(" ", "-")}`);
    const stopped = Promise.withResolvers<void>();
    const runGate = Promise.withResolvers<CommitResult>();
    try {
      await writeFile(join(fixture.workspace, "untracked.txt"), "cancelled commit input\n", "utf8");
      const headBefore = git(fixture.workspace, "rev-parse", "HEAD");
      fixture.workers.runHandler = async () => runGate.promise;
      fixture.workers.nativeStopHandler = async () => {
        stopped.resolve();
        runGate.resolve({ exitCode: 130, headBefore, headAfter: headBefore });
        return true;
      };
      const mergeOperation = fixture.harness.service.submitInbound(
        fixture.message(`auto-commit-${stopText.slice(1).replaceAll(" ", "-")}-command`),
      );
      const call = await waitForNativeCommitRun(fixture.workers);
      const cancellation = fixture.harness.service.submitInbound(
        fixture.message(`auto-commit-${stopText.slice(1).replaceAll(" ", "-")}-stop`, stopText),
      );
      await Promise.all([mergeOperation, cancellation, stopped.promise]);

      expect(call.stopCalls).toBe(1);
      expect(git(fixture.workspace, "rev-parse", "HEAD")).toBe(headBefore);
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
}

test("remaining unstaged changes after native commit prevent /thread merge", async () => {
  const fixture = await createMergeHarness("remaining-changes");
  try {
    await writeFile(join(fixture.workspace, "tracked.txt"), "staged snapshot\n", "utf8");
    git(fixture.workspace, "add", "tracked.txt");
    await writeFile(join(fixture.workspace, "tracked.txt"), "unstaged remainder\n", "utf8");
    const headBefore = git(fixture.workspace, "rev-parse", "HEAD");

    const message = fixture.message("auto-commit-remaining-command");
    const result = waitForThreadMergeResult(fixture.harness, fixture.target);
    await fixture.harness.service.submitInbound(message);
    await result;

    expect(fixture.workers.calls).toHaveLength(1);
    expect(fixture.workers.calls[0]?.runCalls).toBe(1);
    expect(git(fixture.workspace, "rev-parse", "HEAD")).not.toBe(headBefore);
    expect(git(fixture.workspace, "show", "HEAD:tracked.txt")).toBe("staged snapshot");
    expect(git(fixture.workspace, "status", "--porcelain")).not.toBe("");
    expect(fixture.harness.worktrees?.merges).toEqual([]);
    expect(fixture.workers.events).toEqual(["native-commit"]);
  } finally {
    await closeCoreHarness(fixture.harness);
  }
});
