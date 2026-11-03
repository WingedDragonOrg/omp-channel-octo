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

  constructor(root: string, private readonly planFor: MergePlanFactory) {
    super(root);
  }

  async planMerge(request: WorktreeMergeRequest): Promise<WorktreeMergePlan> {
    this.planRequests.push(request);
    return this.planFor(request, this.planRequests.length);
  }

  override async merge(request: WorktreeMergeRequest): Promise<WorktreeMergeSummary> {
    this.mergeCalls += 1;
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

async function letScheduledWorkAdvance(): Promise<void> {
  for (let turn = 0; turn < 8; turn += 1) {
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
}

test("host /thread merge waits for its source and checked-out target while unrelated work continues", async () => {
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const thirdStarted = Promise.withResolvers<void>();
  const target = { channelId: "group-allowed", channelType: 2 as const };
  const source = { channelId: "group-allowed____target-lock", channelType: 5 as const };
  const third = { channelId: "group-a", channelType: 2 as const };
  const fixture = await createPlannedHarness(
    (root) => targetPlan(join(root, "target-checkout")),
    async ({ text }) => {
      if (text === "hold target checkout") {
        parentStarted.resolve();
        await releaseParent.promise;
      } else if (text === "unrelated third-worktree task") {
        thirdStarted.resolve();
      }
    },
  );
  const { harness, root, worktrees } = fixture;
  const store = await openChannelStore(harness.stateDir);
  const targetCheckout = join(root, "target-checkout");
  const sourceWorktree = join(root, "source-worktree");
  const thirdWorktree = join(root, "third-worktree");

  try {
    await ensureBinding(store, harness.bot.uid, target, targetCheckout, "target-session", true);
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "source-session", true);
    await ensureBinding(store, harness.bot.uid, third, thirdWorktree, "third-session", true);

    await harness.service.handleInbound(inbound({
      ...target,
      messageId: "target-checkout-holder",
      text: "hold target checkout",
      mentionedUserIds: [harness.bot.uid],
    }));
    await parentStarted.promise;

    const merge = harness.service.handleInbound(inbound({
      ...source,
      messageId: "target-lock-command",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await letScheduledWorkAdvance();
    expect(worktrees.mergeCalls).toBe(0);

    await harness.service.handleInbound(inbound({
      ...third,
      messageId: "third-worktree-task",
      text: "unrelated third-worktree task",
      mentionedUserIds: [harness.bot.uid],
    }));
    await thirdStarted.promise;
    expect(worktrees.mergeCalls).toBe(0);

    releaseParent.resolve();
    await merge;
    expect(worktrees.mergeCalls).toBe(1);
    expect(worktrees.merges).toHaveLength(1);
    expect(worktrees.merges[0]).toMatchObject({ cwd: sourceWorktree, target: "main" });
  } finally {
    releaseParent.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("host-tool merge reports target contention without merging and succeeds once after release", async () => {
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const firstToolFinished = Promise.withResolvers<string>();
  const retryToolFinished = Promise.withResolvers<string>();
  const target = { channelId: "group-allowed", channelType: 2 as const };
  const source = { channelId: "group-allowed____tool-target-lock", channelType: 5 as const };
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
      if (text === "hold target checkout") {
        parentStarted.resolve();
        await releaseParent.promise;
        return;
      }
      if (text !== "attempt merge while target is held" && text !== "retry merge after release") return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing in child thread");
      const result = await management.execute(
        { action: "thread-merge", target: "refs/heads/release-1", remove: false },
        new AbortController().signal,
        text === "attempt merge while target is held" ? "held-target-merge" : "released-target-merge",
      ).then(
        (value) => `resolved:${value}`,
        (error: Error) => `rejected:${error.message}`,
      );
      if (text === "attempt merge while target is held") firstToolFinished.resolve(result);
      else retryToolFinished.resolve(result);
    },
  );
  const { harness, root, worktrees } = fixture;
  const store = await openChannelStore(harness.stateDir);
  const targetCheckout = join(root, "release-target-checkout");
  const sourceWorktree = join(root, "source-worktree");

  try {
    await ensureBinding(store, harness.bot.uid, target, targetCheckout, "tool-target-session", true);
    await ensureBinding(store, harness.bot.uid, source, sourceWorktree, "tool-source-session", true);
    await harness.service.handleInbound(inbound({
      ...target,
      messageId: "tool-target-holder",
      text: "hold target checkout",
      mentionedUserIds: [harness.bot.uid],
    }));
    await parentStarted.promise;

    await harness.service.handleInbound(inbound({
      ...source,
      messageId: "tool-merge-while-held",
      text: "attempt merge while target is held",
      mentionedUserIds: [harness.bot.uid],
    }));
    const busy = await firstToolFinished.promise;
    expect(busy).toContain("rejected:");
    expect(busy).toContain("正被其他任务使用");
    expect(worktrees.mergeCalls).toBe(0);

    releaseParent.resolve();
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(inbound({
      ...source,
      messageId: "tool-merge-after-release",
      text: "retry merge after release",
      mentionedUserIds: [harness.bot.uid],
    }));
    const retried = await retryToolFinished.promise;
    expect(retried).toContain("resolved:");
    expect(worktrees.mergeCalls).toBe(1);
    expect(worktrees.merges[0]).toMatchObject({
      cwd: sourceWorktree,
      target: "refs/heads/release-1",
    });
  } finally {
    releaseParent.resolve();
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
    await harness.service.handleInbound(inbound({
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
test("unknown host-tool merge blocks and preserves both checkout paths across restart", async () => {
  const mergeFinished = Promise.withResolvers<string>();
  const targetStarted = Promise.withResolvers<void>();
  const thirdStarted = Promise.withResolvers<void>();
  let targetDidStart = false;
  void targetStarted.promise.then(() => {
    targetDidStart = true;
  });
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
    await initial.service.handleInbound(inbound({
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

    await restarted.service.handleInbound(inbound({
      ...target,
      messageId: "task-on-unknown-target",
      text: "target task after recovery",
      mentionedUserIds: [restarted.bot.uid],
    }));
    await restarted.service.handleInbound(inbound({
      ...third,
      messageId: "task-on-unrelated-third-worktree",
      text: "unrelated third-worktree task after recovery",
      mentionedUserIds: [restarted.bot.uid],
    }));
    await thirdStarted.promise;
    await letScheduledWorkAdvance();
    expect(targetDidStart).toBe(false);
    expect(store.getLatestTask(restarted.bot.uid, target)?.state).toBe("queued");
    expect(worktrees.mergeCalls).toBe(1);
  } finally {
    store?.close();
    if (activeHarness) await closeCoreHarness(activeHarness);
    else if (!restarted) await rm(root, { recursive: true, force: true });
  }
});
