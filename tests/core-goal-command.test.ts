
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { expect, test, vi } from "bun:test";
import { WorkerGoalError } from "../src/contracts";
import { openChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness, FakeGoalRunContext } from "./fixtures/core-fakes";

const DM = { channelId: "dm-goal", channelType: 1 as const };

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, resolve };
}

async function waitForGoalOrPrompt(
  harness: CoreHarness,
  promptCount: number,
  goalRunCount = 0,
): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (
      harness.workers.goalRuns.length > goalRunCount
      || harness.workers.prompts.length > promptCount
    ) {
      return;
    }
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("goal command did not reach a worker");
}

async function prepareBoundSession(harness: CoreHarness): Promise<string> {
  await harness.service.handleInbound(inbound({
    ...DM,
    messageId: "goal-prime-session",
    text: "prepare a persistent session",
  }));
  await harness.workers.waitForPromptCount(1);
  await waitForStatus(harness.service, DM, status => status.state === "completed");
  const store = await openChannelStore(harness.stateDir);
  try {
    const binding = store.getBinding(harness.bot.uid, DM);
    expect(binding?.sessionFile).not.toBeNull();
    return binding!.sessionId;
  } finally {
    store.close();
  }
}

async function latestTask(harness: CoreHarness) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getLatestTask(harness.bot.uid, DM);
  } finally {
    store.close();
  }
}

async function waitForTaskState(
  harness: CoreHarness,
  taskId: string,
  state: "completed" | "cancelled" | "failed",
): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    const deadline = performance.now() + 3_000;
    while (performance.now() < deadline) {
      if (store.getTask(taskId)?.state === state) return;
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
    throw new Error(`task ${taskId} did not reach ${state}`);
  } finally {
    store.close();
  }
}
async function waitForSentText(harness: CoreHarness, previousCount: number): Promise<string> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    const sent = harness.transport.sent.slice(previousCount);
    if (sent.length > 0) return sent.at(-1)!.text;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("goal command did not send a response");
}

async function commandReply(
  harness: CoreHarness,
  messageId: string,
  text: string,
): Promise<string> {
  const previousCount = harness.transport.sent.length;
  await harness.service.handleInbound(inbound({ ...DM, messageId, text }));
  return waitForSentText(harness, previousCount);
}

test("/goal set runs a structured create, and live controls show and preserve the native state", async () => {
  const harness = await createCoreHarness();
  const goalStarted = deferred();
  const settleGoal = deferred();
  const steered = Promise.withResolvers<{ text: string; sessionId: string; cwd: string }>();
  let goalId = "";
  try {
    const sessionId = await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    const objective = "Prepare the launch checklist\n\nwith exact wording";
    harness.workers.goalHandler = async ({ request, emit, waitForAppend }: FakeGoalRunContext) => {
      if (request.op !== "create") throw new Error("expected a structured create request");
      const goal = {
        id: `fixture-goal-${sessionId}`,
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 42,
        timeUsedSeconds: 9,
        ...(request.tokenBudget === undefined ? {} : { tokenBudget: request.tokenBudget }),
      };
      goalId = goal.id;
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      goalStarted.resolve();
      const appended = await waitForAppend();
      steered.resolve(appended);
      emit({ type: "steer_consumed", text: appended.text, atMs: Date.now() });
      await settleGoal.promise;
      return { goal, state: { enabled: true, mode: "active", goal } };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-set-structured",
      text: `/goal set --budget 1800 ${objective}`,
    }));
    await waitForGoalOrPrompt(harness, promptCount);

    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.goalRuns[0]).toMatchObject({
      request: {
        op: "create",
        objective,
        tokenBudget: 1800,
      },
      sessionId,
    });
    expect(harness.workers.prompts).toHaveLength(promptCount);
    expect(harness.workers.creations.at(-1)?.goalContinuation).toBe(true);
    await goalStarted.promise;

    const activeReport = await commandReply(harness, "goal-show-live", "/goal show");
    expect(activeReport).toContain("Prepare the launch checklist");
    expect(activeReport).toContain(goalId);
    expect(activeReport).toContain("active");
    expect(activeReport).toContain("tokensUsed=42");
    expect(activeReport).toContain("tokenBudget=1800");
    expect(activeReport).toContain("timeUsedSeconds=9");
    const replacement = await commandReply(harness, "goal-replace-live", "/goal set replace the active objective");
    expect(replacement).toContain("不能替换");
    const budgetChange = await commandReply(harness, "goal-budget-change", "/goal budget 2400");
    expect(budgetChange).toContain("不能事后修改");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(await commandReply(harness, "goal-status-live", "/status")).toContain("goal=active");
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-live-steer",
      text: "Also add launch dates",
    }));
    expect(await steered.promise).toMatchObject({
      text: "Also add launch dates",
      sessionId,
    });

    const liveWorker = harness.workers.workers.at(-1)!;
    const pauseReply = await commandReply(harness, "goal-pause-live", "/goal pause");
    expect(pauseReply).toContain("paused");
    expect(liveWorker.goalOperations).toContain("get");
    expect(liveWorker.goalOperations).toContain("pause");
    expect((await harness.service.status(DM)).state).toBe("running");

    settleGoal.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    const pausedReport = await commandReply(harness, "goal-show-paused", "/goal show");
    expect(pausedReport).toContain("paused");

    const dropReply = await commandReply(harness, "goal-drop", "/goal drop");
    expect(dropReply).toContain("dropped");
    expect(harness.workers.workers.at(-1)?.goalOperations).toContain("drop");
    const droppedReport = await commandReply(harness, "goal-show-dropped", "/goal show");
    expect(droppedReport).toContain("dropped");

    const task = await latestTask(harness);
    expect(task).toMatchObject({
      messageId: "goal-set-structured",
      sessionId,
      state: "completed",
    });
  } finally {
    settleGoal.resolve();
    await closeCoreHarness(harness);
  }
});

test("native goal rounds remain on one durable task until the structured run settles", async () => {
  const harness = await createCoreHarness();
  const firstRound = deferred();
  const settleSecondRound = deferred();
  try {
    const sessionId = await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async ({ request, emit }: FakeGoalRunContext) => {
      if (request.op !== "create") throw new Error("expected a structured create request");
      const goal = {
        id: "fixture-goal-multiround",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 12,
        timeUsedSeconds: 3,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      emit({ type: "text", messageId: "goal-round-one", text: "native round one", final: true });
      firstRound.resolve();
      await settleSecondRound.promise;
      const completeGoal = {
        ...goal,
        status: "complete" as const,
        tokensUsed: 29,
        timeUsedSeconds: 7,
      };
      emit({ type: "text", messageId: "goal-round-two", text: "native round two", final: true });
      emit({ type: "goal_updated", goal: completeGoal });
      return {
        goal: completeGoal,
        state: { enabled: true, mode: "exiting", goal: completeGoal },
      };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-set-multiround",
      text: "/goal set Finish the migration",
    }));
    await waitForGoalOrPrompt(harness, promptCount);
    expect(harness.workers.goalRuns).toHaveLength(1);
    await firstRound.promise;
    await waitForStatus(harness.service, DM, status => status.state === "running");

    const inFlight = await latestTask(harness);
    expect(inFlight).toMatchObject({
      messageId: "goal-set-multiround",
      sessionId,
      state: "running",
    });
    const taskId = inFlight!.id;

    settleSecondRound.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");

    const settled = await latestTask(harness);
    expect(settled?.id).toBe(taskId);
    expect(settled?.resultText).toContain("native round one");
    expect(settled?.resultText).toContain("native round two");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    settleSecondRound.resolve();
    await closeCoreHarness(harness);
  }
});
test("goal acceptance arriving after pause never restores continuation authorization", async () => {
  const harness = await createCoreHarness();
  const dispatched = deferred();
  const acceptGoal = deferred();
  const accepted = deferred();
  const settleGoal = deferred();
  try {
    await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async ({ request, emit }: FakeGoalRunContext) => {
      if (request.op !== "create") throw new Error("expected a create request");
      dispatched.resolve();
      await acceptGoal.promise;
      const goal = {
        id: "fixture-goal-late-acceptance",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      accepted.resolve();
      await settleGoal.promise;
      return { goal, state: { enabled: true, mode: "active", goal } };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-late-acceptance",
      text: "/goal set Keep the task paused",
    }));
    await waitForGoalOrPrompt(harness, promptCount);
    expect(harness.workers.goalRuns).toHaveLength(1);
    await dispatched.promise;
    const liveWorker = harness.workers.workers.at(-1)!;
    await commandReply(harness, "goal-pause-before-accept", "/goal pause");
    expect(liveWorker.goalOperations).toContain("pause");
    expect((await harness.service.status(DM)).state).toBe("running");

    acceptGoal.resolve();
    await accepted.promise;
    settleGoal.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");

    const paused = await commandReply(harness, "goal-show-after-late-accept", "/goal show");
    expect(paused).toContain("paused");
    expect(harness.workers.goalRuns).toHaveLength(1);

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "ordinary-after-goal-pause",
      text: "ordinary work after explicit goal stop",
    }));
    await harness.workers.waitForPromptCount(promptCount + 1);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);
  } finally {
    acceptGoal.resolve();
    settleGoal.resolve();
    await closeCoreHarness(harness);
  }
});

test("duplicate resume while queued or live reuses one goal task behind ordinary work", async () => {
  const ordinaryStarted = deferred();
  const releaseOrdinary = deferred();
  const resumeStarted = deferred();
  const releaseResume = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "hold ordinary work") {
        ordinaryStarted.resolve();
        await releaseOrdinary.promise;
      }
      await append("effects.log", `${text}\\n`);
    },
  });
  try {
    await prepareBoundSession(harness);
    harness.workers.goalHandler = async ({ request, current, emit }: FakeGoalRunContext) => {
      if (request.op === "create") {
        const goal = {
          id: "fixture-goal-resume-queue",
          objective: request.objective,
          status: "paused" as const,
          tokensUsed: 5,
          timeUsedSeconds: 2,
        };
        emit({ type: "goal_accepted", goalId: goal.id });
        emit({ type: "goal_updated", goal });
        return {
          goal,
          state: { enabled: true, mode: "exiting", goal },
        };
      }
      if (request.op !== "resume" || current.goal === null) {
        throw new Error("expected a paused goal resume");
      }
      const active = { ...current.goal, status: "active" as const };
      emit({ type: "goal_accepted", goalId: active.id });
      emit({ type: "goal_updated", goal: active });
      emit({ type: "prompt_accepted" });
      resumeStarted.resolve();
      await releaseResume.promise;
      const complete = { ...active, status: "complete" as const };
      emit({ type: "goal_updated", goal: complete });
      return {
        goal: complete,
        state: { enabled: true, mode: "exiting", goal: complete },
      };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-create-paused",
      text: "/goal set Prepare to resume",
    }));
    await waitForGoalOrPrompt(harness, 1);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "ordinary-held-for-goal-resume",
      text: "hold ordinary work",
    }));
    await harness.workers.waitForPromptCount(2);
    await ordinaryStarted.promise;

    await harness.service.handleInbound(inbound({ ...DM, messageId: "goal-resume-once", text: "/goal resume" }));
    expect((await harness.service.status(DM)).queued).toBe(1);
    await commandReply(harness, "goal-resume-queued-duplicate", "/goal resume");
    expect((await harness.service.status(DM)).queued).toBe(1);
    expect((await latestTask(harness))?.messageId).toBe("goal-resume-once");
    expect(harness.workers.goalRuns).toHaveLength(1);

    releaseOrdinary.resolve();
    await waitForGoalOrPrompt(harness, 2, 1);
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect(harness.workers.goalRuns[1]?.request).toEqual({ op: "resume" });
    await resumeStarted.promise;

    await commandReply(harness, "goal-resume-live-duplicate", "/goal resume");
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect((await latestTask(harness))?.messageId).toBe("goal-resume-once");

    releaseResume.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    releaseOrdinary.resolve();
    releaseResume.resolve();
    await closeCoreHarness(harness);
  }
});

test("pausing cancels queued goal work but leaves ordinary queued tasks intact", async () => {
  const ordinaryStarted = deferred();
  const releaseOrdinary = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "hold ordinary work") {
        ordinaryStarted.resolve();
        await releaseOrdinary.promise;
      }
      await append("effects.log", `${text}\\n`);
    },
  });
  try {
    await prepareBoundSession(harness);
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-prequeue-create",
      text: "/goal set Preserve a paused goal",
    }));
    await waitForGoalOrPrompt(harness, 0);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "ordinary-held-before-goal-queue",
      text: "hold ordinary work",
    }));
    await harness.workers.waitForPromptCount(2);
    await ordinaryStarted.promise;

    await harness.service.handleInbound(inbound({ ...DM, messageId: "ordinary-preserved-by-goal-pause", text: "/queue keep this ordinary task" }));
    const ordinaryTask = await latestTask(harness);
    await harness.service.handleInbound(inbound({ ...DM, messageId: "goal-resume-queued-before-pause", text: "/goal resume" }));
    const goalTask = await latestTask(harness);
    expect(goalTask?.messageId).toBe("goal-resume-queued-before-pause");
    expect(goalTask?.state).toBe("queued");

    await commandReply(harness, "goal-pause-cancel-queued", "/goal pause");
    expect((await latestTask(harness))?.state).toBe("cancelled");
    expect((await harness.service.status(DM)).queued).toBe(1);

    releaseOrdinary.resolve();
    await harness.workers.waitForPromptCount(3);
    await waitForTaskState(harness, ordinaryTask!.id, "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    releaseOrdinary.resolve();
    await closeCoreHarness(harness);
  }
});
test("goal controls isolate sessions sharing an ordinary working directory", async () => {
  const targetB = { channelId: "dm-goal-shared-directory", channelType: 1 as const };
  const holdStarted = deferred();
  const releaseHold = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "hold shared goal directory") {
        holdStarted.resolve();
        await releaseHold.promise;
      }
      await append("effects.log", `${text}\n`);
    },
  });
  try {
    const sessionId = await prepareBoundSession(harness);
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-busy-create",
      text: "/goal set Keep a goal for busy control",
    }));
    await waitForStatus(harness.service, DM, status => status.state === "completed");

    await harness.service.handleInbound(inbound({
      ...targetB,
      messageId: "goal-busy-prime-b",
      text: "create the second session",
    }));
    await waitForStatus(harness.service, targetB, status => status.state === "completed");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getBinding(harness.bot.uid, DM)?.cwd)
        .toBe(store.getBinding(harness.bot.uid, targetB)?.cwd);
      expect(store.getBinding(harness.bot.uid, targetB)?.sessionId).not.toBe(sessionId);
    } finally {
      store.close();
    }

    const promptCount = harness.workers.prompts.length;
    await harness.service.handleInbound(inbound({
      ...targetB,
      messageId: "goal-busy-hold",
      text: "hold shared goal directory",
    }));
    await harness.workers.waitForPromptCount(promptCount + 1);
    await holdStarted.promise;
    expect((await harness.service.status(targetB)).state).toBe("running");

    const show = await commandReply(harness, "goal-busy-show", "/goal show");
    expect(show).toContain("objective=Keep a goal for busy control");
    expect(show).toContain("status=paused");

    const currentStore = await openChannelStore(harness.stateDir);
    try {
      await commandReply(harness, "goal-busy-pause", "/goal pause");
      expect(currentStore.getGoalSession(harness.bot.uid, DM, sessionId)).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "pause",
        status: "paused",
      });
      await commandReply(harness, "goal-busy-drop", "/goal drop");
      expect(currentStore.getGoalSession(harness.bot.uid, DM, sessionId)).toBeNull();
      expect((await harness.service.status(targetB)).state).toBe("running");
    } finally {
      currentStore.close();
    }

    releaseHold.resolve();
    await waitForStatus(harness.service, targetB, status => status.state === "completed");
  } finally {
    releaseHold.resolve();
    await closeCoreHarness(harness);
  }
});
test("goal control and execution proceed while a sibling session holds the same cwd", async () => {
  const sibling = { channelId: "dm-goal-shared-worktree", channelType: 1 as const };
  const heldStarted = deferred();
  const releaseHeld = deferred();
  const resumeStarted = deferred();
  const releaseResume = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "hold shared goal directory") {
        heldStarted.resolve();
        await releaseHeld.promise;
      }
    },
  });
  try {
    const sessionId = await prepareBoundSession(harness);
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-shared-create",
      text: "/goal set Keep a goal in the shared directory",
    }));
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);

    await harness.service.handleInbound(inbound({
      ...sibling,
      messageId: "goal-shared-prime-sibling",
      text: "create the sibling session",
    }));
    await waitForStatus(harness.service, sibling, status => status.state === "completed");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getBinding(harness.bot.uid, DM)?.cwd)
        .toBe(store.getBinding(harness.bot.uid, sibling)?.cwd);
      expect(store.getBinding(harness.bot.uid, sibling)?.sessionId).not.toBe(sessionId);
    } finally {
      store.close();
    }

    const heldTask = harness.service.handleInbound(inbound({
      ...sibling,
      messageId: "goal-shared-held-task",
      text: "hold shared goal directory",
    }));
    await heldStarted.promise;
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-shared-pause",
      text: "/goal pause",
    }));
    const paused = await openChannelStore(harness.stateDir);
    try {
      expect(paused.getGoalSession(harness.bot.uid, DM, sessionId)).toMatchObject({ status: "paused" });
    } finally {
      paused.close();
    }

    const defaultHandler = harness.workers.goalHandler;
    harness.workers.goalHandler = async context => {
      if (context.request.op !== "resume") return defaultHandler(context);
      resumeStarted.resolve();
      const goal = { ...context.current.goal!, status: "active" as const };
      context.emit({ type: "goal_accepted", goalId: goal.id });
      context.emit({ type: "goal_updated", goal });
      context.emit({ type: "prompt_accepted" });
      await releaseResume.promise;
      return {
        goal,
        state: { enabled: true, mode: "active", goal },
      };
    };
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-shared-resume",
      text: "/goal resume",
    }));
    await waitForGoalOrPrompt(harness, harness.workers.prompts.length, 1);
    await resumeStarted.promise;
    expect((await harness.service.status(DM)).state).toBe("running");
    expect((await harness.service.status(sibling)).state).toBe("running");

    releaseResume.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    releaseHeld.resolve();
    await heldTask;
    await waitForStatus(harness.service, sibling, status => status.state === "completed");
  } finally {
    releaseResume.resolve();
    releaseHeld.resolve();
    await closeCoreHarness(harness);
  }
});

test("goal control and execution proceed across owned sibling projects while one is held", async () => {
  const heldStarted = deferred();
  const releaseHeld = deferred();
  const resumeStarted = deferred();
  const releaseResume = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "hold owned project") {
        heldStarted.resolve();
        await releaseHeld.promise;
      }
    },
  });
  const api = { channelId: "group-tools____owned", channelType: 5 as const };
  const web = { channelId: "group-tools____other", channelType: 5 as const };
  const parent = join(harness.root, "repos");
  const entry = join(parent, "shared");
  const apiPath = join(entry, "api");
  const webPath = join(entry, "web");
  let sessionId: string;
  try {
    await Promise.all([
      mkdir(join(apiPath, "src"), { recursive: true }),
      mkdir(webPath, { recursive: true }),
    ]);
    for (const [target, path, messageId] of [
      [api, apiPath, "goal-owned-api"],
      [web, webPath, "goal-owned-web"],
    ] as const) {
      await harness.service.handleInbound(inbound({
        messageId,
        ...target,
        text: `/new ${path}`,
        mentionedUserIds: [harness.bot.uid],
      }));
      await harness.service.handleInbound(inbound({
        messageId: `${messageId}-prime`,
        ...target,
        text: "prepare owned goal session",
        mentionedUserIds: [harness.bot.uid],
      }));
      await waitForStatus(harness.service, target, status => status.state === "completed");
    }

    const store = await openChannelStore(harness.stateDir);
    try {
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        ...api,
        layout: "shared",
        status: "ready",
        entryPath: entry,
        projects: [
          { name: "api", repo: parent, path: apiPath, branch: "api", created: true },
          { name: "web", repo: parent, path: webPath, branch: "web", created: true },
        ],
        error: null,
      });
      sessionId = store.getBinding(harness.bot.uid, api)!.sessionId;
    } finally {
      store.close();
    }
    const goal = {
      id: `fixture-goal-${sessionId}`,
      objective: "Keep the API goal",
      status: "active" as const,
      tokensUsed: 0,
      timeUsedSeconds: 0,
    };
    const goalStore = await openChannelStore(harness.stateDir);
    try {
      goalStore.saveGoalSessionProgress(harness.bot.uid, api, sessionId, goal);
      harness.workers.setNativeGoal(sessionId, goal);
    } finally {
      goalStore.close();
    }

    const heldTask = harness.service.handleInbound(inbound({
      messageId: "goal-owned-held-task",
      ...web,
      text: "hold owned project",
      mentionedUserIds: [harness.bot.uid],
    }));
    await heldStarted.promise;
    await harness.service.handleInbound(inbound({
      messageId: "goal-owned-pause",
      ...api,
      text: "/goal pause",
      mentionedUserIds: [harness.bot.uid],
    }));
    const paused = await openChannelStore(harness.stateDir);
    try {
      expect(paused.getGoalSession(harness.bot.uid, api, sessionId)).toMatchObject({ status: "paused" });
    } finally {
      paused.close();
    }

    const defaultHandler = harness.workers.goalHandler;
    harness.workers.goalHandler = async context => {
      if (context.request.op !== "resume") return defaultHandler(context);
      resumeStarted.resolve();
      const activeGoal = { ...context.current.goal!, status: "active" as const };
      context.emit({ type: "goal_accepted", goalId: activeGoal.id });
      context.emit({ type: "goal_updated", goal: activeGoal });
      context.emit({ type: "prompt_accepted" });
      await releaseResume.promise;
      const completeGoal = { ...activeGoal, status: "complete" as const };
      context.emit({ type: "goal_updated", goal: completeGoal });
      return {
        goal: completeGoal,
        state: { enabled: true, mode: "exiting", goal: completeGoal },
      };
    };
    await harness.service.handleInbound(inbound({
      messageId: "goal-owned-resume",
      ...api,
      text: "/goal resume",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForGoalOrPrompt(harness, harness.workers.prompts.length);
    await resumeStarted.promise;
    expect((await harness.service.status(api)).state).toBe("running");
    expect((await harness.service.status(web)).state).toBe("running");

    releaseResume.resolve();
    releaseHeld.resolve();
    await heldTask;
    await waitForStatus(harness.service, web, status => status.state === "completed");
  } finally {
    releaseResume.resolve();
    releaseHeld.resolve();
    await closeCoreHarness(harness);
  }
});

test("disabled native goals return the specific refusal without falling through to a model prompt", async () => {
  const harness = await createCoreHarness();
  try {
    await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async () => {
      throw new WorkerGoalError("disabled", "native goal feature disabled");
    };

    const reply = await commandReply(harness, "goal-disabled", "/goal set Do not call a model");
    expect(reply).toContain("未启用");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/goal is owner-only and a duplicate inbound message cannot start twice", async () => {
  const harness = await createCoreHarness();
  try {
    const sentBeforeDenied = harness.transport.sent.length;
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-denied-non-owner",
      senderId: "not-the-owner",
      text: "/goal set must be denied",
    }));
    expect(harness.transport.sent).toHaveLength(sentBeforeDenied);
    expect(await latestTask(harness)).toBeNull();
    expect(harness.workers.goalRuns).toHaveLength(0);

    const accepted = inbound({
      ...DM,
      messageId: "goal-dedup-owner",
      text: "/goal set execute once",
    });
    await harness.service.handleInbound(accepted);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    await harness.service.handleInbound(accepted);
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect((await latestTask(harness))?.messageId).toBe("goal-dedup-owner");
    const store = await openChannelStore(harness.stateDir);
    try {
      const binding = store.getBinding(harness.bot.uid, DM);
      const task = store.getLatestTask(harness.bot.uid, DM);
      const goalTask = task ? store.getGoalTask(task.id) : null;
      expect(binding?.sessionFile).not.toBeNull();
      expect(goalTask?.sessionId).toBe(binding?.sessionId);
      expect(goalTask?.acceptedGoalId).toBe(`fixture-goal-${harness.workers.goalRuns[0]?.sessionId}`);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("goal results must match the exact native acceptance ID", async () => {
  const harness = await createCoreHarness();
  try {
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async ({ request, emit }) => {
      if (request.op !== "create") throw new Error("expected create");
      emit({ type: "goal_accepted", goalId: "native-goal-accepted" });
      emit({ type: "prompt_accepted" });
      const mismatched = {
        id: "different-native-goal",
        objective: request.objective,
        status: "complete" as const,
        tokensUsed: 1,
        timeUsedSeconds: 1,
      };
      return { goal: mismatched, state: { enabled: true, mode: "exiting", goal: mismatched } };
    };

    const reply = await commandReply(harness, "goal-id-mismatch", "/goal set Match the accepted ID");
    expect(reply).toContain("ID 与已记录目标不一致");
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a queued goal never switches to a replacement binding session", async () => {
  const ordinaryStarted = deferred();
  const releaseOrdinary = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "hold before goal start") {
        ordinaryStarted.resolve();
        await releaseOrdinary.promise;
      }
    },
  });
  try {
    const originalSessionId = await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-session-switch-holder",
      text: "hold before goal start",
    }));
    await harness.workers.waitForPromptCount(promptCount + 1);
    await ordinaryStarted.promise;

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-session-switch-queued",
      text: "/goal set Stay in the accepted session",
    }));
    const queued = await latestTask(harness);
    expect(queued?.state).toBe("queued");
    const store = await openChannelStore(harness.stateDir);
    try {
      store.updateBindingSession(harness.bot.uid, DM, "replacement-session", null);
    } finally {
      store.close();
    }

    releaseOrdinary.resolve();
    await waitForTaskState(harness, queued!.id, "failed");
    expect(queued?.sessionId).toBe(originalSessionId);
    expect(harness.workers.goalRuns).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(promptCount + 1);
  } finally {
    releaseOrdinary.resolve();
    await closeCoreHarness(harness);
  }
});
test("native progress may precede acceptance and a null final goal settles from the terminal update", async () => {
  const harness = await createCoreHarness();
  let createCount = 0;
  try {
    await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async ({ request, emit }) => {
      if (request.op !== "create") throw new Error("expected create");
      createCount += 1;
      const completed = {
        id: `native-complete-${createCount}`,
        objective: request.objective,
        status: "complete" as const,
        tokensUsed: 17,
        timeUsedSeconds: 8,
      };
      if (createCount === 1) {
        emit({ type: "goal_updated", goal: completed });
        emit({ type: "goal_accepted", goalId: completed.id });
        emit({ type: "prompt_accepted" });
        return { goal: null, state: null };
      }
      emit({ type: "goal_accepted", goalId: completed.id });
      emit({ type: "goal_updated", goal: completed });
      emit({ type: "prompt_accepted" });
      return { goal: completed, state: { enabled: true, mode: "exiting", goal: completed } };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-terminal-null",
      text: "/goal set Complete before returning a goal object",
    }));
    await waitForGoalOrPrompt(harness, promptCount);
    await waitForStatus(harness.service, DM, status => status.state === "completed");

    const absent = await commandReply(harness, "goal-show-after-native-absence", "/goal show");
    expect(absent).toContain("没有原生目标");
    expect(absent).not.toContain("Complete before returning a goal object");

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-set-after-native-absence",
      text: "/goal set A second goal in the same session",
    }));
    await waitForGoalOrPrompt(harness, promptCount, 1);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect(harness.workers.goalRuns[1]?.request).toMatchObject({
      op: "create",
      objective: "A second goal in the same session",
    });
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("dropping a native goal clears its current projection for a new goal in the same session", async () => {
  const harness = await createCoreHarness();
  let createCount = 0;
  try {
    await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    harness.workers.goalHandler = async ({ request, emit }) => {
      if (request.op !== "create") throw new Error("expected create");
      createCount += 1;
      const paused = {
        id: `native-drop-${createCount}`,
        objective: request.objective,
        status: "paused" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: paused.id });
      emit({ type: "goal_updated", goal: paused });
      emit({ type: "prompt_accepted" });
      return { goal: paused, state: { enabled: true, mode: "exiting", goal: paused } };
    };
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-create-before-drop",
      text: "/goal set First goal before drop",
    }));
    await waitForGoalOrPrompt(harness, promptCount);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);

    const drop = await commandReply(harness, "goal-drop-before-recreate", "/goal drop");
    expect(drop).toContain("dropped");
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-create-after-drop",
      text: "/goal set New goal after drop",
    }));
    await waitForGoalOrPrompt(harness, promptCount, 1);
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect(harness.workers.goalRuns[1]?.request).toMatchObject({
      op: "create",
      objective: "New goal after drop",
    });
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("restart recovers the exact native goal and accepts settled absence without a new receipt", async () => {
  const first = await createCoreHarness();
  let recovered: CoreHarness | undefined;
  const entered = deferred();
  const release = deferred();
  let recoveryRequests = 0;
  first.workers.goalHandler = async ({ request, emit }) => {
    if (request.op === "create") {
      const activeGoal = {
        id: "native-goal-before-restart",
        objective: "finish after restart",
        status: "active" as const,
        tokensUsed: 2,
        timeUsedSeconds: 3,
      };
      emit({ type: "goal_accepted", goalId: activeGoal.id });
      emit({ type: "goal_updated", goal: activeGoal });
      entered.resolve();
      await release.promise;
      return { goal: activeGoal, state: null };
    }
    expect(request).toEqual({ op: "recover", goalId: "native-goal-before-restart" });
    recoveryRequests += 1;
    return { goal: null, state: null };
  };
  try {
    const sessionId = await prepareBoundSession(first);
    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-recover-exact",
      text: "/goal set finish after restart",
    }));
    await entered.promise;
    const runningTask = await latestTask(first);
    expect(runningTask).not.toBeNull();

    const stopping = first.service.stop();
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
    release.resolve();
    await stopping;

    recovered = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers: first.workers,
    });
    await waitForTaskState(recovered, runningTask!.id, "completed");
    expect(recoveryRequests).toBe(1);
    expect(first.workers.goalRuns.at(-1)).toMatchObject({
      request: { op: "recover", goalId: "native-goal-before-restart" },
      sessionId,
    });
    const store = await openChannelStore(recovered.stateDir);
    try {
      expect(store.getGoalTask(runningTask!.id)).toMatchObject({
        sessionId,
        accepted: true,
        acceptedGoalId: "native-goal-before-restart",
        dispatchOp: "recover",
      });
      expect(store.getGoalSession(recovered.bot.uid, DM, sessionId)).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    release.resolve();
    if (recovered) await closeCoreHarness(recovered);
    else await closeCoreHarness(first);
  }
});

test("goal show and pause leave the native ask pending until a plain reply answers it", async () => {
  const harness = await createCoreHarness();
  const askStarted = deferred();
  const answerResult = Promise.withResolvers<string>();
  try {
    harness.workers.goalHandler = async ({ request, promptContext, emit }) => {
      if (request.op !== "create") throw new Error("expected one native goal create");
      const goal = {
        id: "goal-with-host-question",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      const ask = promptContext?.tools?.find(tool => tool.name === "ask");
      if (!ask) throw new Error("ask host tool missing from native goal context");
      askStarted.resolve();
      const answer = await ask.execute(
        {
          questions: [
            {
              id: "release-window",
              question: "When should the release happen?",
              options: [{ label: "staging" }, { label: "production" }],
            },
          ],
        },
        new AbortController().signal,
      );
      answerResult.resolve(answer);
      const pausedGoal = { ...goal, status: "paused" as const };
      emit({ type: "goal_updated", goal: pausedGoal });
      return {
        goal: pausedGoal,
        state: { enabled: true, mode: "exiting", goal: pausedGoal },
      };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-ask-start",
      text: "/goal set coordinate the release",
    }));
    await askStarted.promise;
    const pending = await waitForStatus(
      harness.service,
      DM,
      status => status.state === "running" && status.pendingQuestionIds.length === 1,
    );
    const originalTask = await latestTask(harness);
    expect(originalTask).not.toBeNull();
    expect(harness.workers.goalRuns).toHaveLength(1);

    await commandReply(harness, "goal-ask-show", "/goal show");
    const afterShow = await harness.service.status(DM);
    expect(afterShow.pendingQuestionIds).toEqual(pending.pendingQuestionIds);
    expect(afterShow.state).toBe("running");

    await commandReply(harness, "goal-ask-pause", "/goal pause");
    const afterPause = await harness.service.status(DM);
    expect(afterPause.pendingQuestionIds).toEqual(pending.pendingQuestionIds);
    expect(afterPause.state).toBe("running");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-ask-answer",
      text: "staging",
    }));
    const completed = await waitForStatus(
      harness.service,
      DM,
      status => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(await latestTask(harness)).toMatchObject({
      id: originalTask!.id,
      sessionId: originalTask!.sessionId,
      state: "completed",
    });
    expect(completed.sessionId).toBe(originalTask!.sessionId);
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    const questionId = pending.pendingQuestionIds[0]!;
    const answer = JSON.parse(await answerResult.promise) as {
      answers: { id: string; questionId: string; answeredBy: string; answer: string }[];
    };
    expect(answer.answers).toEqual([
      {
        id: "release-window",
        questionId,
        answeredBy: "owner-1",
        answer: "staging",
      },
    ]);

    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getGoalSession(
        harness.bot.uid,
        DM,
        originalTask!.sessionId,
      )).toMatchObject({ continuationAuthorized: false, status: "paused" });
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("goal ask timeout revokes recovery while its late reply stays an ordinary same-session task", async () => {
  const first = await createCoreHarness({ ask: { timeoutMs: 40 } });
  let recovered: CoreHarness | undefined;
  const askStarted = deferred();
  try {
    vi.useFakeTimers();
    first.workers.goalHandler = async ({ request, promptContext, emit, waitForAbort }) => {
      if (request.op !== "create") throw new Error("a timed-out goal must not recover");
      const goal = {
        id: "goal-with-timed-out-ask",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      const ask = promptContext?.tools?.find(tool => tool.name === "ask");
      if (!ask) throw new Error("ask host tool missing from native goal context");
      askStarted.resolve();
      try {
        await ask.execute(
          {
            questions: [
              {
                id: "timed-out-target",
                question: "Which target?",
                options: [{ label: "staging" }, { label: "production" }],
              },
            ],
          },
          new AbortController().signal,
        );
      } catch {
        await waitForAbort();
      }
      return { goal, state: { enabled: true, mode: "active", goal } };
    };

    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-timeout-start",
      text: "/goal set determine the deployment target",
    }));
    await askStarted.promise;
    const pending = await waitForStatus(
      first.service,
      DM,
      status => status.pendingQuestionIds.length === 1,
    );
    const goalTask = await latestTask(first);
    expect(goalTask).not.toBeNull();
    vi.advanceTimersByTime(40);
    const interrupted = await waitForStatus(
      first.service,
      DM,
      status => status.state === "interrupted",
    );
    expect(interrupted.pendingQuestionIds).toEqual(pending.pendingQuestionIds);

    const store = await openChannelStore(first.stateDir);
    try {
      expect(store.getGoalSession(
        first.bot.uid,
        DM,
        goalTask!.sessionId,
      )).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "question_timeout",
      });
    } finally {
      store.close();
    }

    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-timeout-late-answer",
      text: "staging",
    }));
    await waitForStatus(first.service, DM, status => status.state === "completed");
    expect(await latestTask(first)).toMatchObject({
      sessionId: goalTask!.sessionId,
      state: "completed",
    });
    expect(first.workers.goalRuns).toHaveLength(1);
    expect(first.workers.prompts).toHaveLength(1);
    expect(first.workers.prompts[0]).toMatchObject({
      sessionId: goalTask!.sessionId,
    });
    expect(first.workers.prompts[0]?.text).toContain("staging");

    vi.useRealTimers();
    await closeCoreHarness(first, { remove: false });
    recovered = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers: first.workers,
      ask: { timeoutMs: 40 },
    });
    const storeAfterRestart = await openChannelStore(recovered.stateDir);
    try {
      expect(storeAfterRestart.getTask(goalTask!.id)?.state).toBe("interrupted");
      expect(storeAfterRestart.getGoalSession(
        recovered.bot.uid,
        DM,
        goalTask!.sessionId,
      )).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "question_timeout",
      });
    } finally {
      storeAfterRestart.close();
    }
    expect(first.workers.goalRuns).toHaveLength(1);
  } finally {
    vi.useRealTimers();
    if (recovered) await closeCoreHarness(recovered);
    else await closeCoreHarness(first);
  }
});

test("interrupting a live goal revokes restart recovery and preserves ordinary queued work", async () => {
  const harness = await createCoreHarness();
  let recovered: CoreHarness | undefined;
  const goalStarted = deferred();
  try {
    harness.workers.goalHandler = async ({ request, emit, waitForAbort }) => {
      if (request.op !== "create") throw new Error("an interrupted goal must not recover");
      const goal = {
        id: "goal-interrupted-before-restart",
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      goalStarted.resolve();
      await waitForAbort();
      return { goal, state: { enabled: true, mode: "active", goal } };
    };
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-interrupt-start",
      text: "/goal set preserve deployment state",
    }));
    await goalStarted.promise;
    const goalTask = await latestTask(harness);
    expect(goalTask).not.toBeNull();

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-interrupt-queue",
      text: "/queue ordinary work after the goal",
    }));
    const queuedTask = await latestTask(harness);
    expect(queuedTask?.state).toBe("queued");

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-interrupt-command",
      text: "/interrupt",
    }));
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getTask(goalTask!.id)?.state).toBe("interrupted");
      expect(store.getGoalSession(
        harness.bot.uid,
        DM,
        goalTask!.sessionId,
      )).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "interrupt",
      });
      expect(store.getTask(queuedTask!.id)?.state).toBe("completed");
    } finally {
      store.close();
    }
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);

    await closeCoreHarness(harness, { remove: false });
    recovered = await createCoreHarness({
      root: harness.root,
      bot: harness.bot,
      workers: harness.workers,
    });
    const restartedStore = await openChannelStore(recovered.stateDir);
    try {
      expect(restartedStore.getTask(goalTask!.id)?.state).toBe("interrupted");
      expect(restartedStore.getGoalSession(
        recovered.bot.uid,
        DM,
        goalTask!.sessionId,
      )).toMatchObject({
        continuationAuthorized: false,
        stopIntent: "interrupt",
      });
    } finally {
      restartedStore.close();
    }
    expect(harness.workers.goalRuns).toHaveLength(1);
  } finally {
    if (recovered) await closeCoreHarness(recovered);
    else await closeCoreHarness(harness);
  }
});

test("terminal goal cleanup is not a live control target and cannot block the next durable goal", async () => {
  const harness = await createCoreHarness();
  const stopEntered = deferred();
  const releaseStop = deferred();
  let createCount = 0;
  try {
    harness.workers.stopHandler = async () => {
      stopEntered.resolve();
      await releaseStop.promise;
    };
    harness.workers.goalHandler = async ({ request, emit }) => {
      if (request.op !== "create") throw new Error("only the replacement goal may start");
      createCount += 1;
      const goal = {
        id: `terminal-goal-${createCount}`,
        objective: request.objective,
        status: "complete" as const,
        tokensUsed: 3,
        timeUsedSeconds: 2,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      emit({ type: "prompt_accepted" });
      return { goal, state: { enabled: true, mode: "exiting", goal } };
    };

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-terminal-first",
      text: "/goal set finish the first objective",
    }));
    await stopEntered.promise;
    const firstTask = await latestTask(harness);
    expect(firstTask?.state).toBe("completed");
    const firstWorker = harness.workers.workers.at(-1);
    expect(firstWorker).toBeDefined();

    const dropReply = await commandReply(harness, "goal-terminal-drop", "/goal drop");
    expect(dropReply).toMatch(/busy|忙|占用|使用中/iu);
    expect(firstWorker?.goalOperations).not.toContain("drop");
    expect(harness.workers.workers).toHaveLength(1);

    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-terminal-second",
      text: "/goal set start the replacement objective",
    }));
    const queuedReplacement = await latestTask(harness);
    expect(queuedReplacement).toMatchObject({
      sessionId: firstTask!.sessionId,
      state: "queued",
    });
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getGoalTask(queuedReplacement!.id)).toMatchObject({
        sessionId: firstTask!.sessionId,
        request: { op: "create", objective: "start the replacement objective" },
      });
    } finally {
      store.close();
    }
    expect(harness.workers.goalRuns).toHaveLength(1);

    releaseStop.resolve();
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(2);
    expect(harness.workers.goalRuns[1]).toMatchObject({
      request: { op: "create", objective: "start the replacement objective" },
      sessionId: firstTask!.sessionId,
    });
  } finally {
    releaseStop.resolve();
    await closeCoreHarness(harness);
  }
});

test("resume refreshes an interrupted active cache from the exact native paused goal after restart", async () => {
  const first = await createCoreHarness();
  let reopened: CoreHarness | undefined;
  const goalStarted = deferred();
  const goal = {
    id: "native-paused-after-interrupt",
    objective: "resume the interrupted native goal",
    status: "active" as const,
    tokensUsed: 11,
    timeUsedSeconds: 7,
  };
  try {
    first.workers.goalHandler = async ({ request, current, emit, waitForAbort }) => {
      if (request.op === "create") {
        emit({ type: "goal_accepted", goalId: goal.id });
        emit({ type: "goal_updated", goal });
        emit({ type: "prompt_accepted" });
        goalStarted.resolve();
        await waitForAbort();
        return { goal, state: { enabled: true, mode: "active", goal } };
      }
      if (
        request.op !== "resume"
        || current.goal === null
        || current.goal.id !== goal.id
        || current.goal.status !== "paused"
      ) {
        throw new Error("resume must target the exact paused native goal");
      }
      const active = { ...current.goal, status: "active" as const };
      emit({ type: "goal_accepted", goalId: active.id });
      emit({ type: "goal_updated", goal: active });
      emit({ type: "prompt_accepted" });
      const complete = { ...active, status: "complete" as const };
      emit({ type: "goal_updated", goal: complete });
      return { goal: complete, state: { enabled: true, mode: "exiting", goal: complete } };
    };

    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-stale-active-start",
      text: "/goal set resume the interrupted native goal",
    }));
    await goalStarted.promise;
    const interruptedTask = await latestTask(first);
    expect(interruptedTask).not.toBeNull();

    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-stale-active-interrupt",
      text: "/interrupt",
    }));
    await waitForStatus(first.service, DM, status => status.state === "interrupted");
    const sessionId = interruptedTask!.sessionId;
    const store = await openChannelStore(first.stateDir);
    try {
      expect(store.getGoalSession(first.bot.uid, DM, sessionId)).toMatchObject({
        goalId: goal.id,
        status: "active",
        continuationAuthorized: false,
        stopIntent: "interrupt",
      });
    } finally {
      store.close();
    }

    // The native session paused, but its goal_updated frame was lost before restart.
    first.workers.setNativeGoal(sessionId, { ...goal, status: "paused" });
    await closeCoreHarness(first, { remove: false });
    reopened = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers: first.workers,
    });
    const staleStore = await openChannelStore(reopened.stateDir);
    try {
      expect(staleStore.getGoalSession(reopened.bot.uid, DM, sessionId)).toMatchObject({
        goalId: goal.id,
        status: "active",
        continuationAuthorized: false,
        stopIntent: "interrupt",
      });
    } finally {
      staleStore.close();
    }

    await commandReply(reopened, "goal-stale-active-resume", "/goal resume");
    const resumedTask = await latestTask(reopened);
    expect(resumedTask?.id).not.toBe(interruptedTask!.id);
    expect(resumedTask?.sessionId).toBe(sessionId);
    await waitForStatus(reopened.service, DM, status => status.state === "completed");
    expect(first.workers.goalRuns).toHaveLength(2);
    expect(first.workers.goalRuns[1]).toMatchObject({
      request: { op: "resume" },
      sessionId,
    });
    expect(first.workers.workers.some(worker =>
      worker.sessionId === sessionId && worker.goalOperations.includes("get")
    )).toBe(true);

    const resumedStore = await openChannelStore(reopened.stateDir);
    try {
      expect(resumedStore.getGoalTask(resumedTask!.id)).toMatchObject({
        sessionId,
        request: { op: "resume" },
        accepted: true,
        acceptedGoalId: goal.id,
      });
    } finally {
      resumedStore.close();
    }
  } finally {
    if (reopened) await closeCoreHarness(reopened);
    else await closeCoreHarness(first);
  }
});

test("resume rejects a stale paused projection when the exact native session has no goal", async () => {
  const first = await createCoreHarness();
  let reopened: CoreHarness | undefined;
  const pausedGoal = {
    id: "native-goal-missing-after-pause",
    objective: "goal removed outside the channel",
    status: "paused" as const,
    tokensUsed: 4,
    timeUsedSeconds: 3,
  };
  try {
    first.workers.goalHandler = async ({ request, emit }) => {
      if (request.op !== "create") throw new Error("only a paused goal may be created");
      emit({ type: "goal_accepted", goalId: pausedGoal.id });
      emit({ type: "goal_updated", goal: pausedGoal });
      emit({ type: "prompt_accepted" });
      return {
        goal: pausedGoal,
        state: { enabled: true, mode: "exiting", goal: pausedGoal },
      };
    };
    await first.service.handleInbound(inbound({
      ...DM,
      messageId: "goal-stale-paused-start",
      text: "/goal set goal removed outside the channel",
    }));
    await waitForStatus(first.service, DM, status => status.state === "completed");
    const originalTask = await latestTask(first);
    expect(originalTask).not.toBeNull();
    const sessionId = originalTask!.sessionId;
    const cachedStore = await openChannelStore(first.stateDir);
    try {
      expect(cachedStore.getGoalSession(first.bot.uid, DM, sessionId)).toMatchObject({
        goalId: pausedGoal.id,
        status: "paused",
        continuationAuthorized: false,
      });
    } finally {
      cachedStore.close();
    }

    first.workers.setNativeGoal(sessionId, null);
    await closeCoreHarness(first, { remove: false });
    reopened = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers: first.workers,
    });

    const reply = await commandReply(reopened, "goal-stale-paused-resume", "/goal resume");
    expect(reply).toMatch(/没有|不存在|未找到|not found|no (?:native )?goal/iu);
    expect(await latestTask(reopened)).toMatchObject({
      id: originalTask!.id,
      state: "completed",
      sessionId,
    });
    expect(first.workers.goalRuns).toHaveLength(1);
    expect(first.workers.workers.some(worker =>
      worker.sessionId === sessionId && worker.goalOperations.includes("get")
    )).toBe(true);
    const store = await openChannelStore(reopened.stateDir);
    try {
      expect(store.getGoalSession(reopened.bot.uid, DM, sessionId)).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    if (reopened) await closeCoreHarness(reopened);
    else await closeCoreHarness(first);
  }
});

test("ordinary tasks still complete through the ordinary prompt path", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "ordinary-after-goal-wiring",
      text: "ordinary task remains an ordinary prompt",
    }));
    await waitForStatus(harness.service, DM, status => status.state === "completed");
    const task = await latestTask(harness);
    expect(task?.state).toBe("completed");
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.goalRuns).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
