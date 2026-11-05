import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type {
  ChatTarget,
  WorktreeMergePlan,
  WorktreeMergeRequest,
  WorktreeMergeSummary,
} from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorktrees,
  inbound,
  waitForIngressJob,
  waitForStatus,
  type CoreHarness,
  type CoreHarnessOptions,
} from "./fixtures/core-fakes";


type MergePlanFactory = (
  request: WorktreeMergeRequest,
  planNumber: number,
) => Promise<WorktreeMergePlan> | WorktreeMergePlan;

interface PlannedCoreHarness {
  readonly root: string;
  readonly harness: CoreHarness;
  readonly worktrees: PlannedFakeWorktrees;
}

class PlannedFakeWorktrees extends FakeWorktrees {
  readonly planRequests: WorktreeMergeRequest[] = [];
  failAfterSpawn = false;
  mergeCalls = 0;
  mergeGate: (() => Promise<void>) | undefined;

  constructor(root: string, private readonly planFor: MergePlanFactory) {
    super(root);
  }

  async planMerge(request: WorktreeMergeRequest): Promise<WorktreeMergePlan> {
    this.planRequests.push(request);
    return this.planFor(request, this.planRequests.length);
  }

  override async merge(request: WorktreeMergeRequest): Promise<WorktreeMergeSummary> {
    this.mergeCalls += 1;
    await this.mergeGate?.();
    if (this.failAfterSpawn) {
      const { lifecycle, ...recordedRequest } = request;
      this.merges.push(recordedRequest);
      if (!lifecycle) throw new Error("uncertain merge fixture requires host lifecycle");
      lifecycle.beforeSpawn(request.cwd);
      lifecycle.onSpawn({
        pid: 43_017,
        pgid: 43_017,
        identity: "pid=43017;start=Wed Jun 10 12:00:00 2026",
        cwd: request.cwd,
      });
      throw new Error("fixture merge result is uncertain");
    }
    return super.merge(request);
  }
}

interface HeldPrompt {
  readonly started: Promise<void>;
  readonly release: Promise<void>;
  readonly markStarted: () => void;
  readonly finish: () => void;
}

function createHeldPrompt(): HeldPrompt {
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  return {
    started: started.promise,
    release: release.promise,
    markStarted: started.resolve,
    finish: release.resolve,
  };
}

async function createPlannedHarness(
  planForRoot: (root: string) => MergePlanFactory,
  prompt: CoreHarnessOptions["prompt"],
): Promise<PlannedCoreHarness> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-merge-target-")));
  const worktrees = new PlannedFakeWorktrees(root, planForRoot(root));
  try {
    const harness = await createCoreHarness({
      root,
      worktrees,
      ...(prompt === undefined ? {} : { prompt }),
    });
    return { root, harness, worktrees };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

function targetPlan(targetCheckout: string) {
  return async (request: WorktreeMergeRequest): Promise<WorktreeMergePlan> => ({
    target: request.target ?? "main",
    cwds: [resolve(request.cwd), await realpath(targetCheckout)],
  });
}

async function ensureBinding(
  store: ChannelStore,
  botUid: string,
  target: ChatTarget,
  cwd: string,
  sessionId: string,
  cwdPinned = false,
): Promise<void> {
  await mkdir(cwd, { recursive: true });
  store.ensureBinding({
    botUid,
    ...target,
    cwd,
    sessionId,
    sessionFile: null,
    cwdPinned,
  });
}
async function waitForSuccessfulHostMerge(
  store: ChannelStore,
  botUid: string,
  target: ChatTarget,
  sourceId: string,
): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    const lease = store.getHostMergeLeaseBySourceId(botUid, target, sourceId);
    if (lease?.outcome?.kind === "succeeded") return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`host merge ${sourceId} did not persist a successful outcome`);
}

async function waitForRemovalCount(
  worktrees: PlannedFakeWorktrees,
  count: number,
): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (worktrees.removals.length < count && performance.now() < deadline) {
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  expect(worktrees.removals).toHaveLength(count);
}


test("host /thread merge overlaps ordinary tasks on its source and target checkouts", async () => {
  const taskTexts = [
    "source task before slash merge",
    "target task before slash merge",
    "source task during slash merge",
    "target task during slash merge",
    "independent task during slash merge",
  ];
  const heldTasks = new Map<string, HeldPrompt>();
  for (const text of taskTexts) heldTasks.set(text, createHeldPrompt());
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const targetBefore = { channelId: "group-allowed", channelType: 2 as const };
  const targetDuring = { channelId: "group-a", channelType: 2 as const };
  const independent = { channelId: "group-b", channelType: 2 as const };
  const source = { channelId: "group-allowed____slash-merge-source", channelType: 5 as const };
  const sourceBefore = { channelId: "group-allowed____source-task-before", channelType: 5 as const };
  const sourceDuring = { channelId: "group-allowed____source-task-during", channelType: 5 as const };
  const fixture = await createPlannedHarness(
    (root) => targetPlan(join(root, "target-checkout")),
    async ({ text }) => {
      const held = heldTasks.get(text);
      if (!held) return;
      held.markStarted();
      await held.release;
    },
  );
  const { harness, root, worktrees } = fixture;
  const store = await openChannelStore(harness.stateDir);
  const targetCheckout = join(root, "target-checkout");
  const sourceWorktree = join(root, "source-worktree");
  const independentWorktree = join(root, "independent-worktree");
  const taskDeliveries: Promise<void>[] = [];
  let merge: Promise<void> | undefined;

  const startTask = (target: ChatTarget, messageId: string, text: string): void => {
    taskDeliveries.push(harness.service.submitInbound(inbound({
      ...target,
      messageId,
      text,
      mentionedUserIds: [harness.bot.uid],
    })));
  };

  try {
    worktrees.mergeGate = async () => {
      mergeStarted.resolve();
      await releaseMerge.promise;
    };
    await ensureBinding(store, harness.bot.uid, targetBefore, targetCheckout, "slash-target-before", true);
    await ensureBinding(store, harness.bot.uid, targetDuring, targetCheckout, "slash-target-during", true);
    await ensureBinding(store, harness.bot.uid, independent, independentWorktree, "slash-independent", true);
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "slash-merge-source", true);
    await ensureBinding(store, harness.bot.uid, sourceBefore, sourceWorktree, "slash-source-before", true);
    await ensureBinding(store, harness.bot.uid, sourceDuring, sourceWorktree, "slash-source-during", true);

    startTask(sourceBefore, "slash-source-task-before", taskTexts[0]!);
    startTask(targetBefore, "slash-target-task-before", taskTexts[1]!);
    await Promise.all([
      heldTasks.get(taskTexts[0]!)!.started,
      heldTasks.get(taskTexts[1]!)!.started,
    ]);

    const mergeMessage = inbound({
      ...source,
      messageId: "slash-merge-with-active-tasks",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });
    merge = harness.service.submitInbound(mergeMessage);
    await merge;
    await mergeStarted.promise;
    expect(worktrees.mergeCalls).toBe(1);
    heldTasks.get(taskTexts[0]!)!.finish();
    heldTasks.get(taskTexts[1]!)!.finish();
    await Promise.all([
      waitForStatus(harness.service, sourceBefore, status => status.state === "completed"),
      waitForStatus(harness.service, targetBefore, status => status.state === "completed"),
    ]);

    startTask(sourceDuring, "slash-source-task-during", taskTexts[2]!);
    startTask(targetDuring, "slash-target-task-during", taskTexts[3]!);
    startTask(independent, "slash-independent-task-during", taskTexts[4]!);
    await Promise.all([
      heldTasks.get(taskTexts[2]!)!.started,
      heldTasks.get(taskTexts[3]!)!.started,
      heldTasks.get(taskTexts[4]!)!.started,
    ]);
    for (const held of heldTasks.values()) held.finish();
    releaseMerge.resolve();

    await Promise.all([...taskDeliveries, merge]);
    await Promise.all([
      waitForSuccessfulHostMerge(store, harness.bot.uid, source, mergeMessage.messageId),
      waitForStatus(harness.service, sourceBefore, status => status.state === "completed"),
      waitForStatus(harness.service, targetBefore, status => status.state === "completed"),
      waitForStatus(harness.service, sourceDuring, status => status.state === "completed"),
      waitForStatus(harness.service, targetDuring, status => status.state === "completed"),
      waitForStatus(harness.service, independent, status => status.state === "completed"),
    ]);
    expect(worktrees.merges).toHaveLength(1);
    expect(worktrees.merges[0]).toMatchObject({ cwd: sourceWorktree, target: "main" });
  } finally {
    for (const held of heldTasks.values()) held.finish();
    releaseMerge.resolve();
    await Promise.allSettled([...taskDeliveries, ...(merge === undefined ? [] : [merge])]);
    store.close();
    await closeCoreHarness(harness);
  }
});


test("host-tool thread merge overlaps ordinary tasks on its source and target checkouts", async () => {
  const taskTexts = [
    "source task before host-tool merge",
    "target task before host-tool merge",
    "source task during host-tool merge",
    "target task during host-tool merge",
  ];
  const heldTasks = new Map<string, HeldPrompt>();
  for (const text of taskTexts) heldTasks.set(text, createHeldPrompt());
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const toolFinished = Promise.withResolvers<string>();
  const targetBefore = { channelId: "group-allowed", channelType: 2 as const };
  const targetDuring = { channelId: "group-a", channelType: 2 as const };
  const source = { channelId: "group-allowed____tool-merge-source", channelType: 5 as const };
  const sourceBefore = { channelId: "group-allowed____tool-source-before", channelType: 5 as const };
  const sourceDuring = { channelId: "group-allowed____tool-source-during", channelType: 5 as const };
  const fixture = await createPlannedHarness(
    (root) => async (request) => ({
      target: request.target ?? "main",
      cwds: [
        resolve(request.cwd),
        await realpath(join(
          root,
          request.target === "refs/heads/release-1"
            ? "release-target-checkout"
            : "default-target-checkout",
        )),
      ],
    }),
    async ({ text, promptContext }) => {
      const held = heldTasks.get(text);
      if (held) {
        held.markStarted();
        await held.release;
        return;
      }
      if (text !== "invoke host-tool merge") return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing in child thread");
      const result = await management.execute(
        { action: "thread-merge", target: "refs/heads/release-1", remove: false },
        new AbortController().signal,
        "host-tool-overlap",
      ).then(
        (value) => `resolved:${value}`,
        (error: Error) => `rejected:${error.message}`,
      );
      toolFinished.resolve(result);
    },
  );
  const { harness, root, worktrees } = fixture;
  const store = await openChannelStore(harness.stateDir);
  const targetCheckout = join(root, "release-target-checkout");
  const sourceWorktree = join(root, "source-worktree");
  const taskDeliveries: Promise<void>[] = [];
  let mergeTask: Promise<void> | undefined;
  const startTask = (target: ChatTarget, messageId: string, text: string): void => {
    taskDeliveries.push(harness.service.submitInbound(inbound({
      ...target,
      messageId,
      text,
      mentionedUserIds: [harness.bot.uid],
    })));
  };

  try {
    worktrees.mergeGate = async () => {
      mergeStarted.resolve();
      await releaseMerge.promise;
    };
    await ensureBinding(store, harness.bot.uid, targetBefore, targetCheckout, "tool-target-before", true);
    await ensureBinding(store, harness.bot.uid, targetDuring, targetCheckout, "tool-target-during", true);
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "tool-merge-source", true);
    await ensureBinding(store, harness.bot.uid, sourceBefore, sourceWorktree, "tool-source-before", true);
    await ensureBinding(store, harness.bot.uid, sourceDuring, sourceWorktree, "tool-source-during", true);

    startTask(sourceBefore, "tool-source-task-before", taskTexts[0]!);
    startTask(targetBefore, "tool-target-task-before", taskTexts[1]!);
    await Promise.all([
      heldTasks.get(taskTexts[0]!)!.started,
      heldTasks.get(taskTexts[1]!)!.started,
    ]);

    mergeTask = harness.service.submitInbound(inbound({
      ...source,
      messageId: "tool-merge-with-active-tasks",
      text: "invoke host-tool merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await mergeTask;
    await mergeStarted.promise;
    expect(worktrees.mergeCalls).toBe(1);
    heldTasks.get(taskTexts[0]!)!.finish();
    heldTasks.get(taskTexts[1]!)!.finish();
    await Promise.all([
      waitForStatus(harness.service, sourceBefore, status => status.state === "completed"),
      waitForStatus(harness.service, targetBefore, status => status.state === "completed"),
    ]);

    startTask(sourceDuring, "tool-source-task-during", taskTexts[2]!);
    startTask(targetDuring, "tool-target-task-during", taskTexts[3]!);
    await Promise.all([
      heldTasks.get(taskTexts[2]!)!.started,
      heldTasks.get(taskTexts[3]!)!.started,
    ]);
    for (const held of heldTasks.values()) held.finish();
    releaseMerge.resolve();

    await Promise.all(taskDeliveries);
    await mergeTask;
    expect(await toolFinished.promise).toContain("resolved:");
    await Promise.all([
      waitForStatus(harness.service, sourceBefore, status => status.state === "completed"),
      waitForStatus(harness.service, targetBefore, status => status.state === "completed"),
      waitForStatus(harness.service, source, status => status.state === "completed"),
      waitForStatus(harness.service, sourceDuring, status => status.state === "completed"),
      waitForStatus(harness.service, targetDuring, status => status.state === "completed"),
    ]);
    expect(worktrees.merges).toHaveLength(1);
    expect(worktrees.merges[0]).toMatchObject({
      cwd: sourceWorktree,
      target: "refs/heads/release-1",
    });
  } finally {
    for (const held of heldTasks.values()) held.finish();
    releaseMerge.resolve();
    await Promise.allSettled([
      ...taskDeliveries,
      ...(mergeTask === undefined ? [] : [mergeTask]),
    ]);
    store.close();
    await closeCoreHarness(harness);
  }
});
test("/thread remove can run on the source and target while /thread merge is executing", async () => {
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  const source = { channelId: "group-allowed____merge-during-remove", channelType: 5 as const };
  const removeSource = { channelId: "group-allowed____remove-source-during-merge", channelType: 5 as const };
  const removeTarget = { channelId: "group-allowed____remove-target-during-merge", channelType: 5 as const };
  const fixture = await createPlannedHarness(
    (root) => targetPlan(join(root, "target-checkout")),
    async () => {},
  );
  const { harness, root, worktrees } = fixture;
  const store = await openChannelStore(harness.stateDir);
  const sourceWorktree = join(root, "source-worktree");
  const targetCheckout = join(root, "target-checkout");
  const sourceRemovalRepo = join(root, "source-removal-repo");
  const targetRemovalRepo = join(root, "target-removal-repo");
  let merge: Promise<void> | undefined;

  try {
    worktrees.mergeGate = async () => {
      mergeStarted.resolve();
      await releaseMerge.promise;
    };
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "merge-source", true);
    await ensureBinding(store, harness.bot.uid, removeSource, sourceWorktree, "remove-source", true);
    await ensureBinding(store, harness.bot.uid, removeTarget, targetCheckout, "remove-target", true);
    await Promise.all([
      mkdir(sourceRemovalRepo, { recursive: true }),
      mkdir(targetRemovalRepo, { recursive: true }),
    ]);
    store.saveThreadWorkspace({
      botUid: harness.bot.uid,
      channelType: 5,
      channelId: removeSource.channelId,
      layout: "single",
      status: "ready",
      entryPath: null,
      projects: [{
        name: "merge source checkout",
        repo: sourceRemovalRepo,
        path: sourceWorktree,
        branch: "merge-source",
        created: true,
      }],
      error: null,
    });
    store.saveThreadWorkspace({
      botUid: harness.bot.uid,
      channelType: 5,
      channelId: removeTarget.channelId,
      layout: "single",
      status: "ready",
      entryPath: null,
      projects: [{
        name: "merge target checkout",
        repo: targetRemovalRepo,
        path: targetCheckout,
        branch: "merge-target",
        created: true,
      }],
      error: null,
    });

    const mergeMessage = inbound({
      ...source,
      messageId: "merge-while-removing-resources",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    });
    merge = harness.service.submitInbound(mergeMessage);
    await merge;
    await waitForIngressJob(harness, mergeMessage, {
      predicate: job =>
        job.operationCheckpoint?.kind === "command"
        && job.operationCheckpoint.action === "thread"
        && job.operationCheckpoint.phase === "waiting-session",
      description: "merge waiting-session checkpoint",
    });
    await mergeStarted.promise;
    expect(worktrees.mergeCalls).toBe(1);

    const removeSourceMessage = inbound({
      ...removeSource,
      messageId: "remove-source-during-merge",
      text: "/thread remove",
      mentionedUserIds: [harness.bot.uid],
    });
    const removeTargetMessage = inbound({
      ...removeTarget,
      messageId: "remove-target-during-merge",
      text: "/thread remove",
      mentionedUserIds: [harness.bot.uid],
    });
    await Promise.all([
      harness.service.submitInbound(removeSourceMessage),
      harness.service.submitInbound(removeTargetMessage),
    ]);
    await Promise.all([
      waitForIngressJob(harness, removeSourceMessage),
      waitForIngressJob(harness, removeTargetMessage),
    ]);
    await waitForRemovalCount(worktrees, 2);
    expect(worktrees.removals.map(({ path }) => path).sort()).toEqual(
      [sourceWorktree, targetCheckout].sort(),
    );

    releaseMerge.resolve();
    await waitForSuccessfulHostMerge(
      store,
      harness.bot.uid,
      source,
      mergeMessage.messageId,
    );
    expect(worktrees.merges).toHaveLength(1);
  } finally {
    releaseMerge.resolve();
    if (merge !== undefined) await Promise.allSettled([merge]);
    store.close();
    await closeCoreHarness(harness);
  }
});


test("host-tool merge refuses a changed target checkout after acquiring its planned resources", async () => {
  const toolFinished = Promise.withResolvers<string>();
  const source = { channelId: "group-allowed____target-plan-change", channelType: 5 as const };
  const fixture = await createPlannedHarness(
    (root) => async (request, planNumber) => ({
      target: request.target ?? "main",
      cwds: [
        resolve(request.cwd),
        await realpath(join(root, planNumber === 1 ? "target-checkout-first" : "target-checkout-changed")),
      ],
    }),
    async ({ text, promptContext }) => {
      if (text !== "merge across a changing plan") return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing in child thread");
      const result = await management.execute(
        { action: "thread-merge", remove: false },
        new AbortController().signal,
        "changing-target-plan",
      ).then(
        (value) => `resolved:${value}`,
        (error: Error) => `rejected:${error.message}`,
      );
      toolFinished.resolve(result);
    },
  );
  const { harness, root, worktrees } = fixture;
  const sourceWorktree = join(root, "source-worktree");
  const firstTargetCheckout = join(root, "target-checkout-first");
  const changedTargetCheckout = join(root, "target-checkout-changed");
  await Promise.all([
    mkdir(firstTargetCheckout, { recursive: true }),
    mkdir(changedTargetCheckout, { recursive: true }),
  ]);
  const store = await openChannelStore(harness.stateDir);

  try {
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "changing-plan-session", true);
    await harness.service.submitInbound(inbound({
      ...source,
      messageId: "changing-target-plan-task",
      text: "merge across a changing plan",
      mentionedUserIds: [harness.bot.uid],
    }));
    const result = await toolFinished.promise;
    expect(result).toContain("rejected:");
    expect(worktrees.planRequests).toHaveLength(2);
    expect(worktrees.mergeCalls).toBe(0);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("unknown host-tool merge state persists across restart without blocking tasks or replaying", async () => {
  const mergeFinished = Promise.withResolvers<string>();
  const targetStarted = Promise.withResolvers<void>();
  const thirdStarted = Promise.withResolvers<void>();
  const target = { channelId: "group-allowed", channelType: 2 as const };
  const source = { channelId: "group-allowed____unknown-target-lease", channelType: 5 as const };
  const third = { channelId: "group-a", channelType: 2 as const };
  const fixture = await createPlannedHarness(
    (root) => targetPlan(join(root, "target-checkout")),
    async ({ text, promptContext }) => {
      if (text !== "create uncertain merge") return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing in child thread");
      const result = await management.execute(
        { action: "thread-merge", remove: false },
        new AbortController().signal,
        "uncertain-target-merge",
      ).then(
        (value) => `resolved:${value}`,
        (error: Error) => `rejected:${error.message}`,
      );
      mergeFinished.resolve(result);
    },
  );
  const { harness: initial, root, worktrees } = fixture;
  const sourceWorktree = join(root, "source-worktree");
  const targetCheckout = join(root, "target-checkout");
  const thirdWorktree = join(root, "third-worktree");
  worktrees.failAfterSpawn = true;
  let activeHarness: CoreHarness | undefined = initial;
  let store: ChannelStore | undefined;
  let restarted: CoreHarness | undefined;

  try {
    store = await openChannelStore(initial.stateDir);
    await ensureBinding(store, initial.bot.uid, target, targetCheckout, "unknown-target-session", true);
    await ensureBinding(store, initial.bot.uid, source, sourceWorktree, "unknown-source-session", true);
    await ensureBinding(store, initial.bot.uid, third, thirdWorktree, "unknown-third-session", true);
    await initial.service.submitInbound(inbound({
      ...source,
      messageId: "unknown-target-merge-task",
      text: "create uncertain merge",
      mentionedUserIds: [initial.bot.uid],
    }));
    expect(await mergeFinished.promise).toContain("rejected:");
    expect(worktrees.mergeCalls).toBe(1);
    const lease = store.listHostMergeLeases(initial.bot.uid)[0];
    expect(lease?.phase).toBe("unknown");
    expect(lease?.resourceCwds).toEqual([sourceWorktree, targetCheckout]);

    store.close();
    store = undefined;
    await closeCoreHarness(initial, { remove: false });
    activeHarness = undefined;
    restarted = await createCoreHarness({
      root,
      bot: initial.bot,
      worktreePort: worktrees,
      prompt: async ({ text }) => {
        if (text === "target task after recovery") targetStarted.resolve();
        if (text === "unrelated third-worktree task after recovery") thirdStarted.resolve();
      },
    });
    activeHarness = restarted;

    store = await openChannelStore(restarted.stateDir);
    const recoveredLease = store.listHostMergeLeases(restarted.bot.uid)[0];
    expect(recoveredLease?.phase).toBe("unknown");
    expect(recoveredLease?.resourceCwds).toEqual([sourceWorktree, targetCheckout]);

    const blockedTargetMessage = inbound({
      ...target,
      messageId: "task-on-unknown-target",
      text: "target task after recovery",
      mentionedUserIds: [restarted.bot.uid],
    });
    await restarted.service.submitInbound(blockedTargetMessage);
    await waitForIngressJob(restarted, blockedTargetMessage);
    const unrelatedMessage = inbound({
      ...third,
      messageId: "task-on-unrelated-third-worktree",
      text: "unrelated third-worktree task after recovery",
      mentionedUserIds: [restarted.bot.uid],
    });
    await restarted.service.submitInbound(unrelatedMessage);
    await waitForIngressJob(restarted, unrelatedMessage);
    await thirdStarted.promise;
    expect(store.getLatestTask(restarted.bot.uid, target)?.state).toBe("queued");
    const finalLease = store.listHostMergeLeases(restarted.bot.uid)[0];
    expect(finalLease?.phase).toBe("unknown");
    expect(finalLease?.resourceCwds).toEqual([sourceWorktree, targetCheckout]);
    expect(worktrees.mergeCalls).toBe(1);
  } finally {
    store?.close();
    if (activeHarness) await closeCoreHarness(activeHarness);
    else if (!restarted) await rm(root, { recursive: true, force: true });
  }
});
