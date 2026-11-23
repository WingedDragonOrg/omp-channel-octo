import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { resourcePathsRelated, TaskScheduler, type TaskSchedulerDeps } from "../src/tasks/task-scheduler.ts";

const botUid = "bot-core-fork-scheduler";
const source: ChatTarget = { channelId: "group-fork____source", channelType: 5 };

function schedulerFor(store: ChannelStore, overrides: Partial<TaskSchedulerDeps> = {}) {
  return new TaskScheduler({
    store,
    botUid: () => botUid,
    stopping: () => false,
    isDirectoryBlocked: () => false,
    reconcileBinding: async (_taskId, _target, binding) => binding,
    repositoryRoot: async () => null,
    repositoryBlocked: async () => false,
    shouldRunTaskBeforeRepositoryProbe: async () => false,
    runTask: async () => {},
    awaitStartReady: async () => {},
    enqueueText: async () => {},
    notifyThreadOutcome: async () => {},
    ...overrides,
  });
}

function schedulerTask(target: ChatTarget, id: string, cwd: string): TaskRecord {
  return {
    id,
    botUid,
    ...target,
    messageId: `message-${id}`,
    senderId: "owner-1",
    text: `work ${id}`,
    sessionId: `session-${id}`,
    cwd,
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
}

function taskClaim(task: TaskRecord): InboundClaim {
  return {
    botUid,
    target: { channelType: task.channelType, channelId: task.channelId },
    messageId: task.messageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };
}
function schedulerCommitTask(target: ChatTarget, id: string, cwd: string): TaskRecord {
  return { ...schedulerTask(target, id, cwd), text: "/commit", historyEligible: false };
}

function commitClaim(task: TaskRecord): InboundClaim {
  return { ...taskClaim(task), kind: "command" };
}


test("fork preparation shares cwd use without blocking source workers and excludes destructive work through confirmed stop", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-fork-scheduler-"));
  const stateDir = join(root, "state");
  const cwdPath = join(root, "shared cwd");
  await mkdir(cwdPath, { recursive: true });
  const cwd = await realpath(cwdPath);
  const store = await openChannelStore(stateDir);
  const task: TaskRecord = {
    id: "fork-scheduler-source-task",
    botUid,
    ...source,
    messageId: "fork-scheduler-source-message",
    senderId: "owner-1",
    text: "ordinary source task",
    sessionId: "fork-scheduler-source-session",
    cwd,
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
  const claim: InboundClaim = {
    botUid,
    target: source,
    messageId: task.messageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };

  const sourceStarted = Promise.withResolvers<void>();
  const releaseSource = Promise.withResolvers<void>();
  let scheduler: TaskScheduler | undefined;
  try {
    store.ensureBinding({
      botUid,
      ...source,
      sessionId: task.sessionId,
      cwd,
      sessionFile: null,
    });
    expect(store.acceptTask(claim, task)).toBe(true);

    const activeScheduler = schedulerFor(store, {
      runTask: async (taskId) => {
        expect(store.markStarted(taskId)).toBe(true);
        sourceStarted.resolve();
        await releaseSource.promise;
        store.markCompleted(taskId, "done");
      },
    });
    scheduler = activeScheduler;

    const shared = await activeScheduler.tryAcquireSharedResources("fork-operation-1", [cwd]);
    expect(shared).toBeDefined();
    if (!shared) throw new Error("expected fork to hold a shared cwd use lease");
    await shared.ready;

    activeScheduler.scheduleTask(task.id);
    await sourceStarted.promise;
    expect(store.getTask(task.id)?.state).toBe("running");
    expect(await activeScheduler.tryAcquireDestructiveResources([cwd], task)).toBeUndefined();

    releaseSource.resolve();
    await activeScheduler.settle();
    shared.complete();

    const reservation = activeScheduler.reserveDestructiveCommandSession(source, [cwd]);
    expect(await activeScheduler.tryAcquireSharedResources("fork-operation-during-reservation", [cwd]))
      .toBeUndefined();
    reservation.release();

    const destructive = await activeScheduler.tryAcquireDestructiveResources([cwd], task);
    expect(destructive).toBeDefined();
    if (!destructive) throw new Error("expected destructive lease after fork stop was confirmed");
    expect(await activeScheduler.tryAcquireSharedResources("fork-operation-during-merge", [cwd]))
      .toBeUndefined();
    destructive.complete();
  } finally {
    releaseSource.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("a released directory block reschedules queued same-chat work in FIFO order", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-blocked-queue-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelId: "dm-blocked-fifo", channelType: 1 };
  const parent = join(root, "repo");
  const earlier = schedulerTask(target, "blocked-earlier", parent);
  const later = schedulerTask(target, "blocked-later", join(parent, "project"));
  expect(store.acceptTask(taskClaim(earlier), earlier)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  let blocked = true;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      isDirectoryBlocked: cwd => {
        if (blocked && resourcePathsRelated(parent, cwd)) {
          blocked = false;
          return true;
        }
        return false;
      },
      runTask: async taskId => {
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(earlier.id);
    scheduler.scheduleTask(later.id);
    await scheduler.settle();
    expect(started).toEqual([]);
    expect(store.getTask(earlier.id)?.state).toBe("queued");
    expect(store.getTask(later.id)?.state).toBe("queued");

    scheduler.retryQueuedTasks();
    await scheduler.settle();
    expect(started).toEqual([earlier.id, later.id]);
    expect(store.getTask(earlier.id)?.state).toBe("completed");
    expect(store.getTask(later.id)?.state).toBe("completed");
  } finally {
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("an in-flight retry keeps its original FIFO position ahead of later chat work", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-retry-fifo-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelId: "dm-retry-fifo", channelType: 1 };
  const parent = join(root, "repo");
  const earlier = schedulerTask(target, "retry-fifo-earlier", parent);
  const later = schedulerTask(target, "retry-fifo-later", join(parent, "project"));
  expect(store.acceptTask(taskClaim(earlier), earlier)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  let blockChecks = 0;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      isDirectoryBlocked: cwd => {
        if (!resourcePathsRelated(parent, cwd)) return false;
        blockChecks += 1;
        if (blockChecks === 2) {
          // This check observes the old block just as its release requests queued retries.
          scheduler?.retryQueuedTasks();
          return true;
        }
        return false;
      },
      runTask: async taskId => {
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(earlier.id);
    scheduler.scheduleTask(later.id);
    await scheduler.settle();

    expect(started).toEqual([earlier.id, later.id]);
    expect(store.getTask(earlier.id)?.state).toBe("completed");
    expect(store.getTask(later.id)?.state).toBe("completed");
  } finally {
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("released directory blocks retry queued commits before later same-repository tasks", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-retry-commit-"));
  const store = await openChannelStore(join(root, "state"));
  const repo = join(root, "repo");
  const commitTarget: ChatTarget = { channelId: "dm-retry-commit", channelType: 1 };
  const laterTarget: ChatTarget = { channelId: "dm-retry-commit-later", channelType: 1 };
  const commit = schedulerCommitTask(commitTarget, "retry-commit", repo);
  const later = schedulerTask(laterTarget, "retry-commit-later", join(repo, "project"));
  const claim = commitClaim(commit);
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptCommitTask(claim, commit)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  let blocked = true;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      isDirectoryBlocked: cwd => blocked && resourcePathsRelated(repo, cwd),
      repositoryRoot: async () => repo,
      runTask: async taskId => {
        if (store.isCommitTask(taskId)) {
          if (blocked) return;
          const activeScheduler = scheduler;
          if (!activeScheduler) throw new Error("scheduler was not initialized");
          const lease = await activeScheduler.tryAcquireCommitRepository(repo, taskId);
          expect(lease).toBeDefined();
          if (!lease) return;
          try {
            started.push(taskId);
            expect(store.markStarted(taskId)).toBe(true);
            store.markCompleted(taskId, "done");
          } finally {
            lease.complete();
          }
          return;
        }
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(commit.id);
    scheduler.scheduleTask(later.id);
    await scheduler.settle();
    expect(store.getTask(commit.id)?.state).toBe("queued");
    expect(store.getTask(later.id)?.state).toBe("queued");

    blocked = false;
    scheduler.retryQueuedTasks();
    await scheduler.settle();

    expect(started).toEqual([commit.id, later.id]);
    expect(store.getTask(commit.id)?.state).toBe("completed");
    expect(store.getTask(later.id)?.state).toBe("completed");
  } finally {
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a later same-repository task waits for a retried earlier commit after its root probe", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-root-probe-order-"));
  const store = await openChannelStore(join(root, "state"));
  const repo = join(root, "repo");
  const commitTarget: ChatTarget = { channelId: "dm-root-probe-commit", channelType: 1 };
  const laterTarget: ChatTarget = { channelId: "dm-root-probe-later", channelType: 1 };
  const commit = schedulerCommitTask(commitTarget, "root-probe-commit", repo);
  const later = schedulerTask(laterTarget, "root-probe-later", join(repo, "project"));
  const claim = commitClaim(commit);
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptCommitTask(claim, commit)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  const rootProbeEntered = Promise.withResolvers<void>();
  const releaseRootProbe = Promise.withResolvers<void>();
  const commitRetryEntered = Promise.withResolvers<void>();
  const releaseCommitRetry = Promise.withResolvers<void>();
  let blocked = false;
  let laterRootProbes = 0;
  let commitRuns = 0;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      isDirectoryBlocked: cwd => blocked && resourcePathsRelated(repo, cwd),
      repositoryRoot: async cwd => {
        // The two earlierOperations probes finish first; hold execution's later root probe.
        if (cwd === later.cwd && ++laterRootProbes === 3) {
          rootProbeEntered.resolve();
          await releaseRootProbe.promise;
        }
        return repo;
      },
      runTask: async taskId => {
        if (store.isCommitTask(taskId)) {
          if (blocked) return;
          if (++commitRuns === 1) {
            commitRetryEntered.resolve();
            await releaseCommitRetry.promise;
          }
          const activeScheduler = scheduler;
          if (!activeScheduler) throw new Error("scheduler was not initialized");
          const lease = await activeScheduler.tryAcquireCommitRepository(repo, taskId);
          expect(lease).toBeDefined();
          if (!lease) return;
          try {
            started.push(taskId);
            expect(store.markStarted(taskId)).toBe(true);
            store.markCompleted(taskId, "done");
          } finally {
            lease.complete();
          }
          return;
        }
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(later.id);
    await rootProbeEntered.promise;

    blocked = true;
    scheduler.scheduleTask(commit.id);
    await scheduler.waitForTaskSettlement(commit.id);
    expect(store.getTask(commit.id)?.state).toBe("queued");

    blocked = false;
    scheduler.retryQueuedTasks();
    await commitRetryEntered.promise;
    releaseRootProbe.resolve();
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
    releaseCommitRetry.resolve();
    await scheduler.settle();

    expect(started).toEqual([commit.id, later.id]);
    expect(store.getTask(commit.id)?.state).toBe("completed");
    expect(store.getTask(later.id)?.state).toBe("completed");
  } finally {
    releaseRootProbe.resolve();
    releaseCommitRetry.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("an unscheduled earlier commit wakes queued later work after settlement without retry", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-late-commit-schedule-"));
  const store = await openChannelStore(join(root, "state"));
  const repo = join(root, "repo");
  const commitTarget: ChatTarget = { channelId: "dm-late-commit", channelType: 1 };
  const laterTarget: ChatTarget = { channelId: "dm-late-commit-later", channelType: 1 };
  const commit = schedulerCommitTask(commitTarget, "late-commit", repo);
  const later = schedulerTask(laterTarget, "late-commit-later", join(repo, "project"));
  const claim = commitClaim(commit);
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptCommitTask(claim, commit)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  const rootProbeEntered = Promise.withResolvers<void>();
  const releaseRootProbe = Promise.withResolvers<void>();
  let laterRootProbes = 0;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      repositoryRoot: async cwd => {
        // The two earlierOperations probes finish first; hold execution's later root probe.
        if (cwd === later.cwd && ++laterRootProbes === 3) {
          rootProbeEntered.resolve();
          await releaseRootProbe.promise;
        }
        return repo;
      },
      runTask: async taskId => {
        if (store.isCommitTask(taskId)) {
          const activeScheduler = scheduler;
          if (!activeScheduler) throw new Error("scheduler was not initialized");
          const lease = await activeScheduler.tryAcquireCommitRepository(repo, taskId);
          expect(lease).toBeDefined();
          if (!lease) return;
          try {
            started.push(taskId);
            expect(store.markStarted(taskId)).toBe(true);
            store.markCompleted(taskId, "done");
          } finally {
            lease.complete();
          }
          return;
        }
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(later.id);
    await rootProbeEntered.promise;
    releaseRootProbe.resolve();
    await scheduler.waitForTaskSettlement(later.id);
    expect(store.getTask(later.id)?.state).toBe("queued");
    expect(started).toEqual([]);

    scheduler.scheduleTask(commit.id);
    await scheduler.waitForTaskSettlement(commit.id);
    await scheduler.waitForTaskSettlement(later.id);
    await scheduler.settle();

    expect(started).toEqual([commit.id, later.id]);
    expect(store.getTask(commit.id)?.state).toBe("completed");
    expect(store.getTask(later.id)?.state).toBe("completed");
  } finally {
    releaseRootProbe.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a newly blocked same-repository sibling directory is rechecked after async commit probes", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-recheck-block-"));
  const store = await openChannelStore(join(root, "state"));
  const repo = join(root, "repo");
  const commitTarget: ChatTarget = { channelId: "dm-recheck-block-commit", channelType: 1 };
  const laterTarget: ChatTarget = { channelId: "dm-recheck-block-later", channelType: 1 };
  const commit = schedulerCommitTask(commitTarget, "recheck-block-commit", repo);
  const later = schedulerTask(laterTarget, "recheck-block-later", join(repo, "project"));
  const claim = commitClaim(commit);
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptCommitTask(claim, commit)).toBe(true);
  expect(store.acceptTask(taskClaim(later), later)).toBe(true);

  const rootProbeEntered = Promise.withResolvers<void>();
  const releaseRootProbe = Promise.withResolvers<void>();
  const commitRootProbeEntered = Promise.withResolvers<void>();
  const releaseCommitRootProbe = Promise.withResolvers<void>();
  let laterRootProbes = 0;
  let commitRootProbes = 0;
  let repositoryChecks = 0;
  let blocked = false;
  const started: string[] = [];
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      isDirectoryBlocked: cwd => blocked && resourcePathsRelated(repo, cwd),
      repositoryRoot: async cwd => {
        if (cwd === later.cwd && ++laterRootProbes === 3) {
          rootProbeEntered.resolve();
          await releaseRootProbe.promise;
        }
        if (cwd === commit.cwd && ++commitRootProbes === 3) {
          commitRootProbeEntered.resolve();
          await releaseCommitRootProbe.promise;
        }
        return repo;
      },
      repositoryBlocked: async () => {
        repositoryChecks += 1;
        return blocked;
      },
      runTask: async taskId => {
        started.push(taskId);
        expect(store.markStarted(taskId)).toBe(true);
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(later.id);
    await rootProbeEntered.promise;
    releaseRootProbe.resolve();
    await commitRootProbeEntered.promise;

    // A same-root sibling becomes unknown while the queued-commit root probe is awaiting Git.
    blocked = true;
    expect(store.markStarted(commit.id)).toBe(true);
    store.markCompleted(commit.id, "done");
    releaseCommitRootProbe.resolve();
    await scheduler.settle();

    expect(repositoryChecks).toBe(2);
    expect(blocked).toBe(true);
    expect(started).toEqual([]);
    expect(store.getTask(later.id)?.state).toBe("queued");
  } finally {
    releaseRootProbe.resolve();
    releaseCommitRootProbe.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a queued commit retry does not resurrect cancellation", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-retry-cancel-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelId: "dm-retry-cancel", channelType: 1 };
  const commit = schedulerCommitTask(target, "retry-cancel-commit", join(root, "repo"));
  const claim = commitClaim(commit);
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptCommitTask(claim, commit)).toBe(true);

  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let runCount = 0;
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      runTask: async () => {
        runCount += 1;
        entered.resolve();
        await release.promise;
      },
    });
    scheduler.scheduleTask(commit.id);
    await entered.promise;
    scheduler.retryQueuedTasks();
    store.markCancelled(commit.id, "fixture cancellation");
    release.resolve();
    await scheduler.settle();

    expect(runCount).toBe(1);
    expect(store.getTask(commit.id)?.state).toBe("cancelled");
  } finally {
    release.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a completed task releases its resource use while another worker keeps the repo blocked", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-blocked-resource-"));
  const workspace = join(root, "workspace");
  await mkdir(workspace, { recursive: true });
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelId: "dm-resource-owner", channelType: 1 };
  const task = schedulerTask(target, "resource-owner-task", workspace);
  expect(store.acceptTask(taskClaim(task), task)).toBe(true);

  const started = Promise.withResolvers<void>();
  const releaseTask = Promise.withResolvers<void>();
  let repositoryBlocked = false;
  let scheduler: TaskScheduler | undefined;
  try {
    scheduler = schedulerFor(store, {
      repositoryRoot: async () => root,
      repositoryBlocked: async () => repositoryBlocked,
      runTask: async taskId => {
        expect(store.markStarted(taskId)).toBe(true);
        started.resolve();
        await releaseTask.promise;
        store.markCompleted(taskId, "done");
      },
    });
    scheduler.scheduleTask(task.id);
    await started.promise;
    repositoryBlocked = true;
    releaseTask.resolve();
    await scheduler.waitForTaskSettlement(task.id);

    repositoryBlocked = false;
    const contender = schedulerTask(
      { channelId: "dm-resource-contender", channelType: 1 },
      "resource-contender-task",
      workspace,
    );
    const lease = await scheduler.tryAcquireDestructiveResources([workspace], contender);
    expect(lease).toBeDefined();
    lease?.complete();
  } finally {
    releaseTask.resolve();
    await scheduler?.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

for (const terminal of ["cancelled", "failed"] as const) {
  test(`a directly settled ${terminal} commit wakes queued dependents`, async () => {
    const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-commit-terminal-"));
    const store = await openChannelStore(join(root, "state"));
    const repo = join(root, "repo");
    const commitTarget: ChatTarget = { channelId: `dm-terminal-commit-${terminal}`, channelType: 1 };
    const laterTarget: ChatTarget = { channelId: `dm-terminal-later-${terminal}`, channelType: 1 };
    const commit = schedulerCommitTask(commitTarget, `terminal-commit-${terminal}`, repo);
    const later = schedulerTask(laterTarget, `terminal-later-${terminal}`, join(repo, "project"));
    const claim = commitClaim(commit);
    expect(store.claimCommand(claim)).toBe(true);
    expect(store.acceptCommitTask(claim, commit)).toBe(true);
    expect(store.acceptTask(taskClaim(later), later)).toBe(true);

    const started: string[] = [];
    let scheduler: TaskScheduler | undefined;
    try {
      scheduler = schedulerFor(store, {
        repositoryRoot: async () => repo,
        runTask: async taskId => {
          if (store.isCommitTask(taskId)) throw new Error("terminal commit must not be rescheduled");
          started.push(taskId);
          expect(store.markStarted(taskId)).toBe(true);
          store.markCompleted(taskId, "done");
        },
      });
      scheduler.scheduleTask(later.id);
      await scheduler.waitForTaskSettlement(later.id);
      expect(store.getTask(later.id)?.state).toBe("queued");
      expect(started).toEqual([]);

      if (terminal === "cancelled") store.markCancelled(commit.id, "fixture cancellation");
      else store.markFailed(commit.id, "fixture failure");
      scheduler.notifyTaskTerminal(commit.id);
      await scheduler.waitForTaskSettlement(later.id);
      await scheduler.settle();

      expect(started).toEqual([later.id]);
      expect(store.getTask(commit.id)?.state).toBe(terminal);
      expect(store.getTask(later.id)?.state).toBe("completed");
    } finally {
      await scheduler?.settle();
      store.close();
      await rm(root, { recursive: true, force: true });
    }
  });
}
