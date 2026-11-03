import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, InboundAttachment } from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness } from "./fixtures/core-fakes";

const GROUP: ChatTarget & { channelType: 2 } = { channelId: "group-allowed", channelType: 2 };
const DM: ChatTarget & { channelType: 1 } = { channelId: "dm-thread-model", channelType: 1 };

function thread(shortId: string): ChatTarget & { channelType: 5 } {
  return { channelId: `${GROUP.channelId}____${shortId}`, channelType: 5 };
}

type ThreadModelTarget = ChatTarget;

function ownerMessage(
  harness: CoreHarness,
  target: ThreadModelTarget,
  messageId: string,
  text: string,
  options: { senderId?: string; attachments?: InboundAttachment[] } = {},
) {
  return inbound({
    ...target,
    messageId,
    senderId: options.senderId ?? "owner-1",
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [harness.bot.uid],
    ...(options.attachments === undefined ? {} : { attachments: options.attachments }),
  });
}

async function send(
  harness: CoreHarness,
  target: ThreadModelTarget,
  messageId: string,
  text: string,
  options?: { senderId?: string; attachments?: InboundAttachment[] },
): Promise<void> {
  await harness.service.handleInbound(ownerMessage(harness, target, messageId, text, options));
}

test("a group thread default is probed fresh without switching the parent session", async () => {
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "hold the parent session") {
        parentStarted.resolve();
        await releaseParent.promise;
      }
      await append("thread-model.log", `${text}\n`);
    },
  });
  const child = thread("fresh-default");
  try {
    await send(harness, GROUP, "thread-model-parent-running", "hold the parent session");
    await parentStarted.promise;
    const parentWorker = harness.workers.workers[0]!;

    await send(harness, GROUP, "thread-model-default-role", "/thread model @fast");

    expect(harness.workers.creations).toHaveLength(2);
    expect(harness.workers.creations[1]?.sessionFile).toBeUndefined();
    expect(harness.workers.workers[1]?.stopped).toBe(true);
    expect(parentWorker.switchDispatches).toEqual([]);
    expect(parentWorker.stopped).toBe(false);

    releaseParent.resolve();
    await waitForStatus(harness.service, GROUP, status => status.state === "completed");
    await send(harness, GROUP, "thread-model-parent-next", "parent keeps its own model");
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, GROUP, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();

    await send(harness, child, "thread-model-child-inherited", "child inherits the group policy");
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("other/fast:max");
  } finally {
    releaseParent.resolve();
    await closeCoreHarness(harness);
  }
});

test("thread model query and clear expose policy without creating probe workers", async () => {
  const harness = await createCoreHarness();
  const child = thread("query-default");
  try {
    await send(harness, GROUP, "thread-model-query-empty", "/thread model");
    expect(harness.transport.sent.at(-1)?.text).toContain("不指定（由 OMP 自行决定）");
    expect(harness.workers.creations).toHaveLength(0);

    await send(harness, GROUP, "thread-model-query-set", "/thread model @fast");
    expect(harness.workers.creations).toHaveLength(1);
    await send(harness, GROUP, "thread-model-query-canonical", "/thread model");
    expect(harness.transport.sent.at(-1)?.text).toContain("other/fast:max");
    expect(harness.workers.creations).toHaveLength(1);

    await send(harness, child, "thread-model-query-switch", "/switch");
    const switchReply = harness.transport.sent.at(-1)?.text ?? "";
    expect(switchReply).toContain("临时探测（不代表恢复会话）");
    expect(switchReply).toContain("下次启动策略：other/fast:max（主群子区默认）");
    await send(harness, child, "thread-model-query-status", "/status");
    const statusReply = harness.transport.sent.at(-1)?.text ?? "";
    expect(statusReply).toContain("model=other/fast:max（主群子区默认）");
    expect(statusReply).toContain("liveModel=无运行中会话");

    const creationsBeforeClear = harness.workers.creations.length;
    await send(harness, GROUP, "thread-model-query-clear", "/thread model clear");
    expect(harness.workers.creations).toHaveLength(creationsBeforeClear);
    expect(harness.workers.workers.flatMap(worker => worker.switchDispatches)).not.toContain("clear");
    await send(harness, GROUP, "thread-model-query-cleared", "/thread model");
    expect(harness.transport.sent.at(-1)?.text).toContain("不指定（由 OMP 自行决定）");
    expect(harness.workers.creations).toHaveLength(creationsBeforeClear);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("child overrides win, parent changes apply after /switch default, and clear preserves explicit models", async () => {
  const harness = await createCoreHarness();
  const child = thread("precedence");
  try {
    await send(harness, GROUP, "thread-model-set-level", "/thread model fixture/fast:high");
    await send(harness, GROUP, "thread-model-parent-switch", "/switch other/fast");

    await send(harness, GROUP, "thread-model-parent-task", "the parent uses its own switch");
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("other/fast");

    await send(harness, child, "thread-model-first-inherit", "first inherited turn");
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");

    await send(harness, child, "thread-model-child-override", "/switch fixture/default");
    await send(harness, GROUP, "thread-model-parent-change", "/thread model other/fast");
    await send(harness, child, "thread-model-child-override-task", "explicit child model remains");
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/default");

    await send(harness, child, "thread-model-child-default", "/switch default");
    await send(harness, child, "thread-model-child-updated-inherit", "child follows the changed policy");
    await harness.workers.waitForPromptCount(4);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("other/fast");

    await send(harness, child, "thread-model-child-reoverride", "/switch fixture/default");
    await send(harness, GROUP, "thread-model-clear-parent-policy", "/thread model clear");
    await send(harness, child, "thread-model-clear-keeps-override", "explicit child value survives clear");
    await harness.workers.waitForPromptCount(5);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/default");

    await send(harness, child, "thread-model-clear-child-override", "/switch default");
    await send(harness, child, "thread-model-omp-default", "no parent policy remains");
    await harness.workers.waitForPromptCount(6);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();

    await send(harness, GROUP, "thread-model-parent-switch-retained", "parent switch remains independent");
    await harness.workers.waitForPromptCount(7);
    await waitForStatus(harness.service, GROUP, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("other/fast");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a parent policy update leaves a running child on its live model and reports the next policy", async () => {
  const childStarted = Promise.withResolvers<void>();
  const releaseChild = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "hold the child session") {
        childStarted.resolve();
        await releaseChild.promise;
      }
      await append("thread-model-live.log", `${text}\n`);
    },
  });
  const child = thread("live-policy");
  try {
    await send(harness, GROUP, "thread-model-live-initial-policy", "/thread model fixture/fast");
    await send(harness, child, "thread-model-live-task", "hold the child session");
    await childStarted.promise;
    const liveWorker = harness.workers.workers.at(-1)!;

    await send(harness, GROUP, "thread-model-live-next-policy", "/thread model other/fast");
    expect(liveWorker.switchDispatches).toEqual([]);
    expect((await harness.service.status(child)).state).toBe("running");
    await send(harness, child, "thread-model-before-switch-status", "/status");
    const beforeSwitchStatus = harness.transport.sent.at(-1)?.text ?? "";
    expect(beforeSwitchStatus).toContain("fixture/fast");
    expect(beforeSwitchStatus).toContain("other/fast");
    expect(beforeSwitchStatus).toMatch(/下次|下回合|下一回合|新会话/u);

    await send(harness, child, "thread-model-explicit-live-switch", "/switch fixture/default");
    expect(liveWorker.switchDispatches).toEqual(["fixture/default"]);

    await send(harness, child, "thread-model-live-status", "/status");
    const statusReply = harness.transport.sent.at(-1)?.text ?? "";
    expect(statusReply).toContain("fixture/default");
    await send(harness, child, "thread-model-live-query", "/switch");
    const modelReport = harness.transport.sent.at(-1)?.text ?? "";
    expect(modelReport).toContain("fixture/default");
    const nextPolicy = modelReport.split("\n").find(line => /下次|下回合|下一回合|新会话/u.test(line));
    expect(nextPolicy).toContain("fixture/default");
    expect(nextPolicy).toContain("本聊天");

    releaseChild.resolve();
    await waitForStatus(harness.service, child, status => status.state === "completed");
    await send(harness, child, "thread-model-live-next-turn-explicit", "the explicit child setting remains");
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/default");

    await send(harness, child, "thread-model-live-clear-override", "/switch default");
    await send(harness, child, "thread-model-live-updated-next-turn", "start with the new default");
    await harness.workers.waitForPromptCount(3);
    expect(harness.workers.creations.at(-1)?.model).toBe("other/fast");
  } finally {
    releaseChild.resolve();
    await closeCoreHarness(harness);
  }
});

test("thread model commands keep owner and target boundaries; model-fix remains a thread intent", async () => {
  const harness = await createCoreHarness();
  const child = thread("guarded");
  try {
    await send(harness, GROUP, "thread-model-guarded-parent-policy", "/thread model fixture/fast");

    const beforeRejectedCommands = harness.workers.creations.length;
    await send(harness, DM, "thread-model-dm-refused", "/thread model other/fast");
    await send(harness, child, "thread-model-nested-refused", "/thread model other/fast");
    await send(harness, { channelId: "group-denied", channelType: 2 }, "thread-model-unlisted-refused", "/thread model other/fast");
    await harness.service.handleInbound(inbound({
      ...GROUP,
      messageId: "thread-model-unmentioned-refused",
      text: "/thread model other/fast",
      mentionedUserIds: [],
    }));
    await send(harness, GROUP, "thread-model-member-refused", "/thread model other/fast", {
      senderId: "group-member",
    });
    expect(harness.workers.creations).toHaveLength(beforeRejectedCommands);

    const promptCount = harness.workers.prompts.length;
    await send(harness, GROUP, "thread-model-empty-selector", "/thread model");
    await send(harness, GROUP, "thread-model-invalid-selector", "/thread model missing-model-selector-7");
    expect(harness.workers.prompts).toHaveLength(promptCount);
    expect(harness.transport.sent.at(-1)?.text ?? "").toContain("missing-model-selector-7");

    await send(harness, GROUP, "thread-model-fix-intent", "/thread model-fix repair the model selection");
    await harness.workers.waitForPromptCount(promptCount + 1);
    expect(harness.workers.prompts.at(-1)?.text).toContain("model-fix");

    await send(harness, child, "thread-model-guarded-task", "retain the valid parent default");
    await harness.workers.waitForPromptCount(promptCount + 2);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("duplicate model setup with a pending question stays a control command and skips attachments", async () => {
  const harness = await createCoreHarness({
    attachments: { enabled: true, maxBytes: 1_048_576 },
    prompt: async ({ promptContext, text }) => {
      if (text !== "ask the owner") return;
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      await ask.execute(
        { questions: [{ id: "thread-model-choice", question: "Which model should run in the child?" }] },
        new AbortController().signal,
      );
    },
  });
  const child = thread("pending-question");
  try {
    await send(harness, GROUP, "thread-model-ask-start", "ask the owner");
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      GROUP,
      status => status.pendingQuestionIds.length === 1,
    );
    expect(pending.pendingQuestionIds).toHaveLength(1);

    const command = ownerMessage(harness, GROUP, "thread-model-duplicate-while-asking", "/thread model fixture/fast", {
      attachments: [{
        kind: "image",
        url: "https://files.example.test/model-control.png",
        name: "model-control.png",
        size: 8,
        durationSeconds: 0,
      }],
    });
    await Promise.all([
      harness.service.handleInbound(command),
      harness.service.handleInbound(command),
    ]);

    expect(harness.workers.creations).toHaveLength(2);
    expect(harness.workers.workers[1]?.stopped).toBe(true);
    expect(harness.transport.downloadRequests).toEqual([]);
    expect((await harness.service.status(GROUP)).pendingQuestionIds).toEqual(pending.pendingQuestionIds);
    expect(harness.workers.prompts).toHaveLength(1);

    await send(harness, GROUP, "thread-model-ask-answer", "fixture/fast");
    await waitForStatus(
      harness.service,
      GROUP,
      status => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );

    await send(harness, child, "thread-model-after-ask", "use the configured child model");
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread model defaults survive restart and stay scoped to bot and parent group", async () => {
  const botOne = { uid: "bot-thread-model-one", name: "Model One" };
  const botTwo = { uid: "bot-thread-model-two", name: "Model Two" };
  const first = await createCoreHarness({ bot: botOne });
  const root = first.root;
  try {
    await send(first, GROUP, "thread-model-persist-first", "/thread model @fast");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const secondBot = await createCoreHarness({ bot: botTwo });
  const secondBotRoot = secondBot.root;
  try {
    await send(secondBot, GROUP, "thread-model-other-bot-policy", "/thread model other/fast");
  } finally {
    await closeCoreHarness(secondBot, { remove: false });
  }

  const restarted = await createCoreHarness({ root, bot: botOne });
  try {
    await send(restarted, thread("persisted"), "thread-model-persisted-child", "the policy survived restart");
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, thread("persisted"), status => status.state === "completed");
    expect(restarted.workers.creations.at(-1)?.model).toBe("other/fast:max");

    const otherGroup = { channelId: "group-b____isolated", channelType: 5 as const };
    await send(restarted, otherGroup, "thread-model-other-group-child", "another group has no inherited policy");
    await restarted.workers.waitForPromptCount(2);
    await waitForStatus(restarted.service, otherGroup, status => status.state === "completed");
    expect(restarted.workers.creations.at(-1)?.model).toBeUndefined();
  } finally {
    await closeCoreHarness(restarted);
  }

  const secondBotRestarted = await createCoreHarness({ root: secondBotRoot, bot: botTwo });
  try {
    const otherBotThread = thread("same-group-other-bot");
    await send(secondBotRestarted, otherBotThread, "thread-model-other-bot-child", "another bot has its own policy");
    await secondBotRestarted.workers.waitForPromptCount(1);
    await waitForStatus(secondBotRestarted.service, otherBotThread, status => status.state === "completed");
    expect(secondBotRestarted.workers.creations.at(-1)?.model).toBe("other/fast");
  } finally {
    await closeCoreHarness(secondBotRestarted);
  }
});

test("new, reset, move, and native goal workers use the inherited thread model", async () => {
  const harness = await createCoreHarness();
  const child = thread("session-paths");
  const source = join(harness.root, "thread-source");
  const destination = join(harness.root, "thread-destination");
  try {
    await mkdir(source, { recursive: true });
    await mkdir(destination, { recursive: true });
    await send(harness, GROUP, "thread-model-session-policy", "/thread model fixture/fast:high");

    await send(harness, child, "thread-model-new-session", `/new ${source}`);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");

    await send(harness, child, "thread-model-reset-session", "/reset");
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");

    await send(harness, child, "thread-model-move-session", `/move ${destination}`);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");

    const promptsBeforePrime = harness.workers.prompts.length;
    await send(harness, child, "thread-model-goal-prime", "prepare a goal session");
    await harness.workers.waitForPromptCount(promptsBeforePrime + 1);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    const beforeGoal = harness.workers.creations.length;

    await send(harness, child, "thread-model-goal-start", "/goal set finish the child task");
    await harness.workers.waitForGoalRunCount(1);
    await waitForStatus(harness.service, child, status => status.state === "completed");
    expect(harness.workers.creations.length).toBeGreaterThan(beforeGoal);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");
    expect(harness.workers.goalRuns).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

