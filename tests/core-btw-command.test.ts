import { expect, test } from "bun:test";
import { join } from "node:path";
import { openChannelStore } from "../src/store";
import {
  FakeWorkerFactory,
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForIngressJob,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness, FakeBtwRunContext } from "./fixtures/core-fakes";

const DM = { channelId: "dm-btw", channelType: 1 as const };
const GROUP = { channelId: "group-allowed", channelType: 2 as const };

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, resolve: () => resolve() };
}

function sentTo(harness: CoreHarness, target: { channelId: string; channelType: number }) {
  return harness.transport.sent.filter(({ target: sentTarget }) =>
    sentTarget.channelId === target.channelId && sentTarget.channelType === target.channelType,
  );
}

async function waitForBtwCall(
  harness: CoreHarness,
  count = 1,
): Promise<FakeBtwRunContext> {
  const deadline = performance.now() + 1_500;
  while (performance.now() < deadline) {
    const call = harness.workers.btwCalls[count - 1];
    if (call) return call;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("BTW did not reach the worker");
}

async function waitForSentText(
  harness: CoreHarness,
  target: { channelId: string; channelType: number },
  predicate: (text: string) => boolean,
  after = 0,
): Promise<string> {
  const deadline = performance.now() + 1_500;
  while (performance.now() < deadline) {
    const sent = sentTo(harness, target).slice(after);
    const match = sent.find(({ text }) => predicate(text));
    if (match) return match.text;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("expected user-visible reply was not sent");
}

async function waitForStopped(worker: { stopped: boolean }): Promise<void> {
  const deadline = performance.now() + 1_500;
  while (performance.now() < deadline) {
    if (worker.stopped) return;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("BTW worker did not stop");
}

async function waitForSignalAbort(signal: AbortSignal | undefined): Promise<void> {
  const deadline = performance.now() + 1_500;
  while (performance.now() < deadline) {
    if (signal?.aborted) return;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error("BTW abort signal was not raised");
}

async function boundSession(
  harness: CoreHarness,
  target: { channelId: string; channelType: 1 | 2 | 5 },
): Promise<{ sessionId: string; sessionFile: string; cwd: string }> {
  const store = await openChannelStore(harness.stateDir);
  try {
    const binding = store.getBinding(harness.bot.uid, target);
    if (!binding?.sessionFile) throw new Error("fixture task did not bind a session file");
    return { sessionId: binding.sessionId, sessionFile: binding.sessionFile, cwd: binding.cwd };
  } finally {
    store.close();
  }
}

async function boundBinding(
  harness: CoreHarness,
  target: { channelId: string; channelType: 1 | 2 | 5 },
) {
  const store = await openChannelStore(harness.stateDir);
  try {
    const binding = store.getBinding(harness.bot.uid, target);
    if (!binding) throw new Error("fixture chat has no bound session");
    return binding;
  } finally {
    store.close();
  }
}

async function waitForCommandClaim(
  harness: CoreHarness,
  target: { channelId: string; channelType: 1 | 2 | 5 },
  messageId: string,
): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    const deadline = performance.now() + 1_500;
    while (performance.now() < deadline) {
      if (store.getCommandClaimState(harness.bot.uid, target, messageId) !== null) return;
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
  } finally {
    store.close();
  }
  throw new Error("BTW command claim was not persisted");
}

async function prepareBoundSession(
  harness: CoreHarness,
  target: { channelId: string; channelType: 1 | 2 | 5 } = DM,
): Promise<{ sessionId: string; sessionFile: string; cwd: string }> {
  const promptCount = harness.workers.prompts.length;
  const message = inbound({
    ...target,
    ...(target.channelType === 1 ? {} : { mentionedUserIds: [harness.bot.uid] }),
    messageId: `btw-prime-${target.channelId}`,
    text: "create a persistent session for BTW",
  });
  await harness.service.submitInbound(message);
  await harness.workers.waitForPromptCount(promptCount + 1);
  await waitForStatus(harness.service, target, (status) => status.state === "completed");
  return boundSession(harness, target);
}

async function latestTask(harness: CoreHarness, target: { channelId: string; channelType: 1 | 2 | 5 }) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getLatestTask(harness.bot.uid, target);
  } finally {
    store.close();
  }
}

test("BTW answers beside a held task without steering it, consuming ask, or changing its result", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, emit, promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      const answer = await ask.execute(
        { questions: [{ id: "btw-approval", question: "May the task continue?" }] },
        new AbortController().signal,
      );
      await append("btw-ask-answer.json", answer);
      emit(assistantText("btw-main-output", "Main task result only.", true));
    },
  });
  try {
    const taskMessage = inbound({
      ...DM,
      messageId: "btw-held-task",
      text: "hold the main task on an ask",
    });
    await harness.service.submitInbound(taskMessage);
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      DM,
      (status) => status.state === "running" && status.pendingQuestionIds.length === 1,
    );
    const pendingQuestionIds = [...pending.pendingQuestionIds];
    const question = "BTw_ONLY_question: explain the retry boundary";
    const replyStart = sentTo(harness, DM).length;

    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-held-question",
      text: `/btw ${question}`,
    }));
    await waitForBtwCall(harness);
    const btwReply = await waitForSentText(
      harness,
      DM,
      (text) => text.includes(question) && text.includes(`Fixture BTW answer: ${question}`),
      replyStart,
    );

    expect(btwReply).toContain(question);
    expect(btwReply).toContain(`Fixture BTW answer: ${question}`);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.workers[0]!.steerCalls).toEqual([]);
    expect(await harness.service.status(DM)).toMatchObject({
      state: "running",
      queued: 0,
      pendingQuestionIds,
    });

    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-answer-real-ask",
      text: "continue",
    }));
    const completed = await waitForStatus(
      harness.service,
      DM,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    expect(completed.queued).toBe(0);
    expect(JSON.parse(await readText(join(harness.root, "tmp", "btw-ask-answer.json")))).toMatchObject({
      answers: [{ id: "btw-approval", answer: "continue" }],
    });
    expect((await latestTask(harness, DM))?.resultText).toBe("Main task result only.");
    expect((await latestTask(harness, DM))?.resultText).not.toContain(question);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a task can finish its prompt while BTW retains the worker and keeps later task work queued", async () => {
  const finishPrompt = deferred();
  const finishBtw = Promise.withResolvers<string>();
  const harness = await createCoreHarness({
    prompt: async ({ emit }) => {
      await finishPrompt.promise;
      emit(assistantText("btw-finished-task-output", "Primary task completed.", true));
    },
  });
  let btwCommand: Promise<void> | undefined;
  let newCommand: Promise<void> | undefined;
  try {
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-retain-primary",
      text: "primary task waits for BTW lease",
    }));
    await harness.workers.waitForPromptCount(1);
    const worker = harness.workers.workers[0]!;
    harness.workers.btwHandler = async () => finishBtw.promise;
    btwCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-retain-question",
      text: "/btw finish after the prompt",
    }));
    const question = await waitForBtwCall(harness);
    expect(question.sessionId).toBe(worker.sessionId);

    finishPrompt.resolve();
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
    expect(worker.stopped).toBe(false);

    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-followup-queued",
      text: "follow-up waits for the BTW request",
    }));
    await waitForStatus(harness.service, DM, (status) => status.queued === 1);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(worker.stopped).toBe(false);
    const bindingBeforeNew = await boundSession(harness, DM);
    const creationsBeforeNew = harness.workers.creations.length;
    let newCommandSettled = false;
    newCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-followup-new",
      text: "/new",
    }));
    void newCommand.then(
      () => { newCommandSettled = true; },
      () => { newCommandSettled = true; },
    );
    for (let turn = 0; turn < 20 && !newCommandSettled; turn += 1) {
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
    expect((await boundSession(harness, DM)).sessionId).toBe(bindingBeforeNew.sessionId);
    expect(harness.workers.creations).toHaveLength(creationsBeforeNew);
    expect(harness.workers.prompts).toHaveLength(1);

    finishBtw.resolve("BTW finished after the task prompt.");
    await btwCommand;
    await waitForSentText(
      harness,
      DM,
      (text) => text.includes("finish after the prompt") && text.includes("BTW finished after the task prompt."),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
  } finally {
    finishPrompt.resolve();
    finishBtw.resolve("fixture BTW cleanup answer");
    await btwCommand?.catch(() => {});
    await newCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("a failed BTW is visible but leaves its main task able to complete", async () => {
  const finishPrompt = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ emit }) => {
      await finishPrompt.promise;
      emit(assistantText("btw-after-failure-output", "Main answer survived.", true));
    },
  });
  try {
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-failure-primary",
      text: "main task while native BTW fails",
    }));
    await harness.workers.waitForPromptCount(1);
    harness.workers.btwHandler = async () => {
      throw new Error("private native RPC detail");
    };
    const replyStart = sentTo(harness, DM).length;
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-failure-question",
      text: "/btw ask while the task is held",
    }));
    await waitForBtwCall(harness);
    const failure = await waitForSentText(
      harness,
      DM,
      (text) => /失败|未能|拒绝|异常|出错|无法|error/i.test(text),
      replyStart,
    );
    expect(failure).not.toContain("private native RPC detail");
    expect(await harness.service.status(DM)).toMatchObject({ state: "running", queued: 0 });
    expect(harness.workers.workers[0]!.abortCalls).toBe(0);
    expect(harness.workers.workers[0]!.stopped).toBe(false);

    finishPrompt.resolve();
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
    expect((await latestTask(harness, DM))?.resultText).toBe("Main answer survived.");
  } finally {
    finishPrompt.resolve();
    await closeCoreHarness(harness);
  }
});

test("idle BTW opens only the currently bound session and does not create a task prompt", async () => {
  const harness = await createCoreHarness();
  try {
    const binding = await prepareBoundSession(harness);
    const taskBefore = await latestTask(harness, DM);
    const promptCount = harness.workers.prompts.length;
    const creationCount = harness.workers.creations.length;
    const question = "use exactly the existing session";
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-idle-exact-session",
      text: `/btw ${question}`,
    }));
    const call = await waitForBtwCall(harness);
    await waitForSentText(
      harness,
      DM,
      (text) => text.includes(question) && text.includes(`Fixture BTW answer: ${question}`),
    );

    expect(call).toMatchObject({
      question,
      sessionId: binding.sessionId,
      sessionFile: binding.sessionFile,
      cwd: binding.cwd,
    });
    expect(harness.workers.creations).toHaveLength(creationCount + 1);
    expect(harness.workers.creations.at(-1)?.sessionFile).toBe(binding.sessionFile);
    expect(harness.workers.prompts).toHaveLength(promptCount);
    expect((await latestTask(harness, DM))?.id).toBe(taskBefore?.id);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("BTW refuses a task that has not reached its first prompt instead of waiting behind it", async () => {
  const creationStarted = Promise.withResolvers<void>();
  const releaseCreation = deferred();
  const workers = new FakeWorkerFactory();
  workers.beforeCreate = async () => {
    creationStarted.resolve();
    await releaseCreation.promise;
  };
  const harness = await createCoreHarness({ workers });
  const promptCount = workers.prompts.length;
  const question = "do not wait for prompt startup";
  const messageId = "btw-during-task-startup";
  const replyStart = sentTo(harness, DM).length;
  let taskCommand: Promise<void> | undefined;
  let btwCommand: Promise<void> | undefined;
  try {
    taskCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-startup-primary",
      text: "task waits while creating its worker",
    }));
    await creationStarted.promise;
    btwCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId,
      text: `/btw ${question}`,
    }));
    await waitForCommandClaim(harness, DM, messageId);

    releaseCreation.resolve();
    await taskCommand;
    await workers.waitForPromptCount(promptCount + 1);
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
    await btwCommand;

    const reply = await waitForSentText(
      harness,
      DM,
      (text) => text.includes(question) || /忙|进行|运行|处理|busy/i.test(text),
      replyStart,
    );
    expect(reply).toMatch(/忙|进行|运行|处理|busy/i);
    expect(workers.btwCalls).toHaveLength(0);
    expect(workers.prompts).toHaveLength(promptCount + 1);
  } finally {
    releaseCreation.resolve();
    await taskCommand?.catch(() => {});
    await btwCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("BTW refuses a replacement session if the binding changes during task cleanup", async () => {
  const harness = await createCoreHarness();
  const stopStarted = Promise.withResolvers<void>();
  const releaseStop = deferred();
  let taskCommand: Promise<void> | undefined;
  let btwCommand: Promise<void> | undefined;
  try {
    await prepareBoundSession(harness);
    const originalBinding = await boundBinding(harness, DM);
    const replacementMessage = inbound({
      ...DM,
      messageId: "btw-race-create-replacement",
      text: "/new",
    });
    await harness.service.submitInbound(replacementMessage);
    await waitForIngressJob(harness, replacementMessage, {
      description: "replacement /new outcome before the cleanup race",
    });
    const replacementBinding = await boundBinding(harness, DM);
    const restoreBinding = await openChannelStore(harness.stateDir);
    try {
      restoreBinding.ensureBinding(originalBinding);
    } finally {
      restoreBinding.close();
    }

    harness.workers.stopHandler = async (sessionId) => {
      if (sessionId !== originalBinding.sessionId) return;
      stopStarted.resolve();
      await releaseStop.promise;
    };
    const promptCount = harness.workers.prompts.length;
    taskCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-race-primary",
      text: "finish while cleanup is held",
    }));
    await harness.workers.waitForPromptCount(promptCount + 1);
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
    await stopStarted.promise;

    const question = "stay on the originally requested session";
    const messageId = "btw-race-question";
    const replyStart = sentTo(harness, DM).length;
    btwCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId,
      text: `/btw ${question}`,
    }));
    await waitForCommandClaim(harness, DM, messageId);
    const switchBinding = await openChannelStore(harness.stateDir);
    try {
      switchBinding.ensureBinding(replacementBinding);
    } finally {
      switchBinding.close();
    }

    releaseStop.resolve();
    await taskCommand;
    await btwCommand;
    const reply = await waitForSentText(
      harness,
      DM,
      (text) => text.includes(question) || /失败|未能|拒绝|异常|出错|无法|error/i.test(text),
      replyStart,
    );
    expect(reply).toMatch(/失败|未能|拒绝|异常|出错|无法|error/i);
    expect(harness.workers.btwCalls).toHaveLength(0);
  } finally {
    releaseStop.resolve();
    await taskCommand?.catch(() => {});
    await btwCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("BTW without a binding gives a visible refusal instead of creating a session", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-no-binding",
      text: "/btw answer from the current conversation",
    }));
    const refusal = await waitForSentText(harness, DM, (text) => /绑定|会话/.test(text));
    expect(refusal).toMatch(/绑定|会话/);
    expect(harness.workers.btwCalls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(await harness.service.status(DM)).toMatchObject({ state: "none", queued: 0 });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("empty BTW argument shows usage without dispatching a worker request", async () => {
  const harness = await createCoreHarness();
  try {
    await prepareBoundSession(harness);
    const promptCount = harness.workers.prompts.length;
    const replyStart = sentTo(harness, DM).length;
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-empty-argument",
      text: "/btw",
    }));
    const usage = await waitForSentText(
      harness,
      DM,
      (text) => text.includes("/btw") && /问题|用法/.test(text),
      replyStart,
    );
    expect(usage).toContain("/btw");
    expect(harness.workers.btwCalls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(promptCount);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("duplicate BTW delivery is ignored and a concurrent BTW is visibly refused", async () => {
  const harness = await createCoreHarness();
  const finishBtw = Promise.withResolvers<string>();
  try {
    await prepareBoundSession(harness);
    harness.workers.btwHandler = async () => finishBtw.promise;
    const first = inbound({
      ...DM,
      messageId: "btw-duplicate-first",
      text: "/btw first unique question",
    });
    const firstReplyStart = sentTo(harness, DM).length;
    const firstCommand = harness.service.submitInbound(first);
    await waitForBtwCall(harness);

    await harness.service.submitInbound(first);
    expect(harness.workers.btwCalls).toHaveLength(1);

    const secondReplyStart = sentTo(harness, DM).length;
    await harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-concurrent-second",
      text: "/btw second unique question",
    }));
    const busy = await waitForSentText(
      harness,
      DM,
      (text) => /忙|进行|处理|并发|busy/i.test(text),
      secondReplyStart,
    );
    expect(busy).toMatch(/忙|进行|处理|并发|busy/i);
    expect(harness.workers.btwCalls).toHaveLength(1);

    finishBtw.resolve("first answer only");
    await firstCommand;
    const success = await waitForSentText(
      harness,
      DM,
      (text) => text.includes("first unique question") && text.includes("first answer only"),
      firstReplyStart,
    );
    expect(success).toContain("first unique question");
    expect(sentTo(harness, DM).filter(({ text }) => text.includes("first answer only"))).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    finishBtw.resolve("fixture BTW cleanup answer");
    await closeCoreHarness(harness);
  }
});

test("only an owner in an allowed, mentioned group can invoke BTW", async () => {
  const harness = await createCoreHarness();
  try {
    await prepareBoundSession(harness, GROUP);
    const promptCount = harness.workers.prompts.length;
    const question = "allowed owner group question";
    await harness.service.submitInbound(inbound({
      ...GROUP,
      messageId: "btw-owner-mentioned-group",
      mentionedUserIds: [harness.bot.uid],
      text: `/btw ${question}`,
    }));
    const ownerReply = await waitForSentText(
      harness,
      GROUP,
      (text) => text.includes(question) && text.includes(`Fixture BTW answer: ${question}`),
    );
    expect(ownerReply).toContain(question);
    expect(harness.workers.prompts).toHaveLength(promptCount);

    const member = inbound({
      ...GROUP,
      messageId: "btw-member-command",
      senderId: "group-member",
      mentionedUserIds: [harness.bot.uid],
      text: "/btw member question",
    });
    const memberReplyStart = sentTo(harness, GROUP).length;
    await harness.service.submitInbound(member);
    const ownerNotice = await waitForSentText(
      harness,
      GROUP,
      (text) => /owner|所有者/.test(text),
      memberReplyStart,
    );
    expect(ownerNotice).toMatch(/owner|所有者/);

    await harness.service.submitInbound(inbound({
      ...GROUP,
      messageId: "btw-owner-no-mention",
      text: "/btw not mentioned",
    }));
    const deniedGroup = inbound({
      channelId: "group-denied",
      channelType: 2,
      messageId: "btw-owner-unlisted-group",
      mentionedUserIds: [harness.bot.uid],
      text: "/btw unlisted group",
    });
    await harness.service.submitInbound(deniedGroup);

    expect(harness.workers.btwCalls).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(promptCount);
    expect(await harness.service.status(GROUP)).toMatchObject({ state: "completed", queued: 0 });
    expect(await harness.service.status(deniedGroup)).toMatchObject({ state: "none", queued: 0 });
  } finally {
    await closeCoreHarness(harness);
  }
});

for (const command of ["/cancel", "/stop", "/interrupt"] as const) {
  test(`${command} aborts an idle BTW request and releases its worker lease`, async () => {
    const harness = await createCoreHarness();
    const btwStarted = Promise.withResolvers<void>();
    const releaseForCleanup = deferred();
    let btwCommand: Promise<void> | undefined;
    let cancelCommand: Promise<void> | undefined;
    try {
      await prepareBoundSession(harness);
      harness.workers.btwHandler = async ({ signal }) => {
        btwStarted.resolve();
        if (!signal) {
          await releaseForCleanup.promise;
          return "must not be delivered after cancellation";
        }
        const aborted = Promise.withResolvers<void>();
        const onAbort = (): void => aborted.resolve();
        if (signal.aborted) onAbort();
        else signal.addEventListener("abort", onAbort, { once: true });
        await Promise.race([aborted.promise, releaseForCleanup.promise]);
        signal.removeEventListener("abort", onAbort);
        if (!signal.aborted) return "must not be delivered after cancellation";
        throw new Error("native BTW request cancelled");
      };
      const replyStart = sentTo(harness, DM).length;
      btwCommand = harness.service.submitInbound(inbound({
        ...DM,
        messageId: `btw-cancel-${command}`,
        text: "/btw hold until cancelled",
      }));
      const call = await waitForBtwCall(harness);
      await btwStarted.promise;
      const worker = harness.workers.workers.at(-1)!;

      cancelCommand = harness.service.submitInbound(inbound({
        ...DM,
        messageId: `btw-cancel-command-${command}`,
        text: command,
      }));
      await waitForSignalAbort(call.signal);
      await cancelCommand;
      expect(call.signal?.aborted).toBe(true);
      await btwCommand;
      await waitForStopped(worker);
      expect(sentTo(harness, DM).slice(replyStart).some(({ text }) => /取消|中断|cancel|interrupt/i.test(text))).toBe(true);
      expect(sentTo(harness, DM).some(({ text }) => text.includes("must not be delivered after cancellation"))).toBe(false);
    } finally {
      releaseForCleanup.resolve();
      await cancelCommand?.catch(() => {});
      await btwCommand?.catch(() => {});
      await closeCoreHarness(harness);
    }
  });
}

test("service shutdown aborts an idle BTW so inbound work can drain", async () => {
  const harness = await createCoreHarness();
  const btwStarted = Promise.withResolvers<void>();
  const releaseForCleanup = deferred();
  let btwCommand: Promise<void> | undefined;
  try {
    await prepareBoundSession(harness);
    harness.workers.btwHandler = async ({ signal }) => {
      btwStarted.resolve();
      if (!signal) {
        await releaseForCleanup.promise;
        return "must not be delivered after shutdown";
      }
      const aborted = Promise.withResolvers<void>();
      const onAbort = (): void => aborted.resolve();
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
      await Promise.race([aborted.promise, releaseForCleanup.promise]);
      signal.removeEventListener("abort", onAbort);
      if (!signal.aborted) return "must not be delivered after shutdown";
      throw new Error("native BTW request stopped with the service");
    };
    btwCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "btw-service-shutdown",
      text: "/btw hold until shutdown",
    }));
    const call = await waitForBtwCall(harness);
    await btwStarted.promise;
    const worker = harness.workers.workers.at(-1)!;

    const stopping = harness.service.stop();
    await waitForSignalAbort(call.signal);
    await stopping;
    expect(call.signal?.aborted).toBe(true);
    await btwCommand;
    expect(worker.stopped).toBe(true);
    expect(sentTo(harness, DM).some(({ text }) => text.includes("must not be delivered after shutdown"))).toBe(false);
  } finally {
    releaseForCleanup.resolve();
    await btwCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});
