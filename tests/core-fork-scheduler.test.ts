import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { TaskScheduler, type TaskSchedulerDeps } from "../src/tasks/task-scheduler.ts";

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
