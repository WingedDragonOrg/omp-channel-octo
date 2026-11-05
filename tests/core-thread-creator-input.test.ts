import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, HostTool, PromptContext, ThreadSummary } from "../src/contracts";
import { ChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
  type FakePromptContext,
} from "./fixtures/core-fakes";

const GROUP: ChatTarget = { channelId: "group-tools", channelType: 2 };
const THREAD_SHORT_ID = "topic-creator-input";
const THREAD: ChatTarget = {
  channelId: `${GROUP.channelId}____${THREAD_SHORT_ID}`,
  channelType: 5,
};
const THREAD_RECORD: ThreadSummary = {
  shortId: THREAD_SHORT_ID,
  groupId: GROUP.channelId,
  channelId: THREAD.channelId,
  name: "creator input fixture",
  creatorUid: "owner-1",
  status: 1,
  memberCount: 1,
  messageCount: 0,
  createdAt: "2026-11-05T00:00:00.000Z",
  updatedAt: "2026-11-05T00:00:00.000Z",
};
const CREATOR_ACTION = "thread-send-parent";
const CREATOR_BODY = "核对蓝绿部署的回滚点";
const CHILD_REQUEST = "send creator input";
const REQUESTER_CALL_ID = "creator-input-native-call";
const WAIT_TIMEOUT_MS = 3_000;

type ToolInvocation =
  | { actions: string[]; result: string }
  | { actions: string[]; error: string };

async function withStore<T>(harness: CoreHarness, inspect: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    return await inspect(store);
  } finally {
    store.close();
  }
}

async function prepareChats(
  harness: CoreHarness,
  listen: "off" | "merge" | "task" = "merge",
): Promise<{ parentCwd: string; threadCwd: string }> {
  harness.transport.setThreads(GROUP.channelId, [THREAD_RECORD]);
  const parentCwd = join(harness.root, "creator-parent-cwd");
  const threadCwd = join(harness.root, "creator-thread-cwd");
  await mkdir(parentCwd, { recursive: true });
  await mkdir(threadCwd, { recursive: true });

  await harness.service.submitInbound(
    inbound({
      messageId: "creator-input-parent-bind",
      ...GROUP,
      text: `/new ${parentCwd}`,
      mentionedUserIds: [harness.bot.uid],
    }),
  );
  await harness.service.submitInbound(
    inbound({
      messageId: "creator-input-thread-bind",
      ...THREAD,
      text: `/new ${threadCwd}`,
      mentionedUserIds: [harness.bot.uid],
    }),
  );
  await waitForStatus(
    harness.service,
    GROUP,
    (status) => status.state === "idle" && status.cwd === parentCwd,
  );
  await waitForStatus(
    harness.service,
    THREAD,
    (status) => status.state === "idle" && status.cwd === threadCwd,
  );
  await withStore(harness, (store) => {
    store.ensureThreadListen({
      botUid: harness.bot.uid,
      channelId: THREAD.channelId,
      creatorChannelType: 2,
      creatorChannelId: GROUP.channelId,
      threadName: THREAD_RECORD.name,
      listen,
    });
  });
  return { parentCwd, threadCwd };
}

function managementOf(promptContext: PromptContext | undefined): HostTool {
  const tool = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
  if (!tool) throw new Error("octo_management host tool missing from the prompt");
  return tool;
}

function managementActions(tool: HostTool): string[] {
  const properties = tool.parameters["properties"];
  if (properties === null || typeof properties !== "object") return [];
  const action = (properties as Record<string, unknown>)["action"];
  if (action === null || typeof action !== "object") return [];
  const values = (action as Record<string, unknown>)["enum"];
  return Array.isArray(values) ? values.filter((value): value is string => typeof value === "string") : [];
}

async function invokeCreatorInput(
  promptContext: PromptContext | undefined,
  text: string,
  toolCallId = REQUESTER_CALL_ID,
): Promise<ToolInvocation> {
  const management = managementOf(promptContext);
  const actions = managementActions(management);
  try {
    const result = await management.execute(
      { action: CREATOR_ACTION, text },
      new AbortController().signal,
      toolCallId,
    );
    // Model the OMP host-tool result acknowledgement, after which replay must recover the same
    // result rather than accepting another parent task.
    management.onResultAccepted?.(toolCallId);
    return { actions, result };
  } catch (error) {
    return { actions, error: error instanceof Error ? error.message : String(error) };
  }
}

function expectQueued(invocation: ToolInvocation): string {
  expect(invocation.actions).toContain(CREATOR_ACTION);
  expect("error" in invocation).toBe(false);
  if (!("result" in invocation)) throw new Error("creator input was not accepted");
  expect(invocation.result).toContain("queued");
  return invocation.result;
}


async function waitForPromptCount(harness: CoreHarness, count: number): Promise<void> {
  const deadline = performance.now() + WAIT_TIMEOUT_MS;
  while (performance.now() < deadline) {
    if (harness.workers.prompts.length >= count) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error(`timed out waiting for worker prompt count ${count}`);
}

function expectCreatorInputPrompt(harness: CoreHarness, parentCwd: string, body: string): void {
  const matches = harness.workers.prompts.filter(
    (prompt) => prompt.cwd === parentCwd && prompt.text.includes(body),
  );
  expect(matches).toHaveLength(1);
  const prompt = matches[0];
  if (!prompt) throw new Error("creator input prompt missing");
  expect(prompt.text).toContain(THREAD.channelId);
  expect(prompt.text).toMatch(/system|系统/iu);
  expect(prompt.text).toMatch(/source|来源/iu);
  expect(prompt.text).toMatch(/(?:not|non)[\s\S]{0,40}user|(?:非|不是|并非)[^\n]{0,32}用户/iu);
  expect(prompt.text).toMatch(/(?:not|non)[\s\S]{0,40}answer|(?:非|不作为|不是)[^\n]{0,32}答案/iu);
  expect(prompt.text).toMatch(
    /(?:not|non)[\s\S]{0,48}authori[sz]|(?:不构成|不代表|不得视为|不可视为|不是)[^\n]{0,36}授权/iu,
  );
}

async function submitThreadRequest(harness: CoreHarness, messageId: string): Promise<void> {
  await harness.service.submitInbound(
    inbound({
      messageId,
      ...THREAD,
      text: CHILD_REQUEST,
      mentionedUserIds: [harness.bot.uid],
    }),
  );
}

test("a child queues creator input when idle, including with listen off, and carries source safety metadata", async () => {
  const invocation = Promise.withResolvers<ToolInvocation>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === CHILD_REQUEST) {
        invocation.resolve(await invokeCreatorInput(promptContext, CREATOR_BODY));
      }
    },
  });
  try {
    const { parentCwd } = await prepareChats(harness, "off");
    const before = await withStore(harness, (store) => ({
      owner: store.getOwnerId(harness.bot.uid),
      allowedChats: store.listAllowedChats(harness.bot.uid),
    }));
    await submitThreadRequest(harness, "creator-input-idle-request");

    const toolCall = await invocation.promise;
    const receipt = expectQueued(toolCall);
    await waitForPromptCount(harness, 2);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expectCreatorInputPrompt(harness, parentCwd, CREATOR_BODY);
    expect(receipt).toContain(GROUP.channelId);
    expect(
      harness.transport.sent.some(({ target, text }) => target.channelId === GROUP.channelId && text.includes(CREATOR_BODY)),
    ).toBe(false);

    const after = await withStore(harness, (store) => ({
      owner: store.getOwnerId(harness.bot.uid),
      allowedChats: store.listAllowedChats(harness.bot.uid),
      listen: store.getThreadListen(harness.bot.uid, THREAD),
    }));
    expect(after.owner).toBe(before.owner);
    expect(after.allowedChats).toEqual(before.allowedChats);
    expect(after.listen).toMatchObject({
      creatorChannelType: 2,
      creatorChannelId: GROUP.channelId,
      listen: "off",
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("creator input queues behind a running creator turn even in steer mode", async () => {
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const invocation = Promise.withResolvers<ToolInvocation>();
  let appended: FakePromptContext["appended"] | undefined;
  const harness = await createCoreHarness({
    prompt: async (context) => {
      if (context.text === "hold creator turn") {
        appended = context.appended;
        parentStarted.resolve();
        await releaseParent.promise;
        return;
      }
      if (context.text === CHILD_REQUEST) {
        invocation.resolve(await invokeCreatorInput(context.promptContext, "运行中输入也必须排队"));
      }
    },
  });
  try {
    await prepareChats(harness);
    await harness.service.submitInbound(
      inbound({
        messageId: "creator-input-steer-mode",
        ...GROUP,
        text: "/mode steer",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.submitInbound(
      inbound({
        messageId: "creator-input-hold-parent",
        ...GROUP,
        text: "hold creator turn",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await parentStarted.promise;
    await waitForStatus(harness.service, GROUP, (status) => status.state === "running");
    await submitThreadRequest(harness, "creator-input-running-request");

    expectQueued(await invocation.promise);
    const queued = await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "running" && status.queued === 1,
    );
    expect(queued.queued).toBe(1);
    expect(appended?.()).toEqual([]);

    releaseParent.resolve();
    await waitForPromptCount(harness, 3);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expectCreatorInputPrompt(harness, join(harness.root, "creator-parent-cwd"), "运行中输入也必须排队");
  } finally {
    releaseParent.resolve();
    await closeCoreHarness(harness);
  }
});

test("creator input does not answer a pending question in the creator chat", async () => {
  const invocation = Promise.withResolvers<ToolInvocation>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === CHILD_REQUEST) {
        invocation.resolve(await invokeCreatorInput(promptContext, "子区提供待处理问题的背景"));
      }
    },
  });
  try {
    const { parentCwd } = await prepareChats(harness);
    const questionId = "creator-input-pending-question";
    await withStore(harness, (store) => {
      store.createQuestion({
        id: questionId,
        taskId: "task-open",
        botUid: harness.bot.uid,
        channelType: GROUP.channelType,
        channelId: GROUP.channelId,
        payload: JSON.stringify({ questions: [{ question: "是否继续？" }] }),
      });
    });
    await submitThreadRequest(harness, "creator-input-pending-ask-request");

    expectQueued(await invocation.promise);
    await waitForPromptCount(harness, 2);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expectCreatorInputPrompt(harness, parentCwd, "子区提供待处理问题的背景");
    expect((await harness.service.status(GROUP)).pendingQuestionIds).toContain(questionId);
    await withStore(harness, (store) => {
      expect(store.getQuestion(questionId)).toMatchObject({ state: "pending", answer: null });
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("replaying the same creator-input toolCallId accepts only one parent task", async () => {
  const releaseCreatorInput = Promise.withResolvers<void>();
  const creatorTaskStarted = Promise.withResolvers<void>();
  const invocations = Promise.withResolvers<[ToolInvocation, ToolInvocation]>();
  const body = "同一原生工具调用只能接收一次";
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === CHILD_REQUEST) {
        const first = await invokeCreatorInput(promptContext, body, "same-native-creator-call");
        const replay = await invokeCreatorInput(promptContext, body, "same-native-creator-call");
        invocations.resolve([first, replay]);
        return;
      }
      if (text.includes(body)) {
        creatorTaskStarted.resolve();
        await releaseCreatorInput.promise;
      }
    },
  });
  try {
    const { parentCwd } = await prepareChats(harness);
    await submitThreadRequest(harness, "creator-input-idempotency-request");

    const [first, replay] = await invocations.promise;
    const firstReceipt = expectQueued(first);
    const replayReceipt = expectQueued(replay);
    expect(replayReceipt).toBe(firstReceipt);
    await creatorTaskStarted.promise;
    const status = await harness.service.status(GROUP);
    expect(status.state).toBe("running");
    expect(status.queued).toBe(0);
    expect(
      harness.workers.prompts.filter((prompt) => prompt.cwd === parentCwd && prompt.text.includes(body)),
    ).toHaveLength(1);

    releaseCreatorInput.resolve();
    await waitForStatus(harness.service, GROUP, (value) => value.state === "completed");
    expect(
      harness.workers.prompts.filter((prompt) => prompt.cwd === parentCwd && prompt.text.includes(body)),
    ).toHaveLength(1);
  } finally {
    releaseCreatorInput.resolve();
    await closeCoreHarness(harness);
  }
});


test("creator input waits for earlier parent attachment preparation and preserves FIFO", async () => {
  const creatorBody = "子区输入必须排在先到达的父区附件之后";
  const parentBody = "先处理这一条父区附件消息";
  const creatorCallStarted = Promise.withResolvers<void>();
  const invocation = Promise.withResolvers<ToolInvocation>();
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  let invocationSettled = false;
  void invocation.promise.then(() => {
    invocationSettled = true;
  });
  const harness = await createCoreHarness({
    attachments: { enabled: true, maxBytes: 1024 * 1024 },
    prompt: async ({ promptContext, text }) => {
      if (text !== CHILD_REQUEST) return;
      creatorCallStarted.resolve();
      invocation.resolve(await invokeCreatorInput(promptContext, creatorBody));
    },
  });
  try {
    const { parentCwd } = await prepareChats(harness);
    const url = "https://cdn.example.test/creator-input-parent-order.txt";
    const bytes = new TextEncoder().encode("parent attachment body");
    harness.transport.attachmentBodies.set(url, bytes);
    const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
    harness.transport.downloadAttachment = async (request) => {
      downloadStarted.resolve();
      await releaseDownload.promise;
      return originalDownload(request);
    };

    const parentIngress = harness.service.receive(inbound({
      ...GROUP,
      messageId: "creator-input-prior-parent-attachment",
      text: parentBody,
      mentionedUserIds: [harness.bot.uid],
      attachments: [{
        kind: "file",
        name: "parent.txt",
        size: bytes.byteLength,
        durationSeconds: 0,
        url,
      }],
    }));
    await downloadStarted.promise;
    await submitThreadRequest(harness, "creator-input-fifo-request");
    await creatorCallStarted.promise;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;

    expect(invocationSettled).toBe(false);
    expect(harness.workers.prompts.filter((prompt) => prompt.cwd === parentCwd)).toHaveLength(0);

    releaseDownload.resolve();
    await parentIngress.ackReady;
    expectQueued(await invocation.promise);
    await waitForPromptCount(harness, 3);
    await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const parentPrompts = harness.workers.prompts.filter((prompt) => prompt.cwd === parentCwd);
    expect(parentPrompts).toHaveLength(2);
    const earlierIndex = harness.workers.prompts.findIndex(
      (prompt) => prompt.cwd === parentCwd && prompt.text.includes(parentBody),
    );
    const creatorIndex = harness.workers.prompts.findIndex(
      (prompt) => prompt.cwd === parentCwd && prompt.text.includes(creatorBody),
    );
    expect(earlierIndex).toBeGreaterThanOrEqual(0);
    expect(creatorIndex).toBeGreaterThan(earlierIndex);
    expectCreatorInputPrompt(harness, parentCwd, creatorBody);
  } finally {
    releaseDownload.resolve();
    await closeCoreHarness(harness);
  }
});

test("creator input is rejected when the source task has a durable stop intent", async () => {
  const invocation = Promise.withResolvers<ToolInvocation>();
  const sourceMessageId = "creator-input-stop-intent-request";
  let harness: CoreHarness;
  harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text !== CHILD_REQUEST) return;
      await withStore(harness, (store) => {
        const sourceTask = store.getTaskByMessageId(harness.bot.uid, THREAD, sourceMessageId);
        if (sourceTask === null) throw new Error("source task was not durably accepted");
        expect(store.markTaskStopIntent(sourceTask.id, "cancel")).toBe(true);
      });
      invocation.resolve(await invokeCreatorInput(promptContext, "不得从已取消任务派送输入"));
    },
  });
  try {
    const { parentCwd } = await prepareChats(harness);
    await submitThreadRequest(harness, sourceMessageId);

    const toolCall = await invocation.promise;
    expect(toolCall.actions).toContain(CREATOR_ACTION);
    expect("error" in toolCall).toBe(true);
    if (!("error" in toolCall)) throw new Error("stopped source unexpectedly sent creator input");
    expect(toolCall.error).toMatch(/stop intent/iu);
    await waitForStatus(harness.service, THREAD, (status) => status.state === "completed");
    expect((await harness.service.status(GROUP)).state).toBe("idle");
    expect(harness.workers.prompts.filter((prompt) => prompt.cwd === parentCwd)).toHaveLength(0);
    await withStore(harness, (store) => {
      expect(store.getLatestTask(harness.bot.uid, GROUP)).toBeNull();
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("creator input initializes a session for an otherwise unbound creator chat", async () => {
  const invocation = Promise.withResolvers<ToolInvocation>();
  const creatorBody = "父区尚无绑定也能接收子区输入";
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === CHILD_REQUEST) {
        invocation.resolve(await invokeCreatorInput(promptContext, creatorBody));
      }
    },
  });
  try {
    harness.transport.setThreads(GROUP.channelId, [THREAD_RECORD]);
    const threadCwd = join(harness.root, "creator-unbound-thread-cwd");
    await mkdir(threadCwd, { recursive: true });
    await harness.service.submitInbound(
      inbound({
        messageId: "creator-input-unbound-thread-bind",
        ...THREAD,
        text: `/new ${threadCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(
      harness.service,
      THREAD,
      (status) => status.state === "idle" && status.cwd === threadCwd,
    );
    await withStore(harness, (store) => {
      store.ensureThreadListen({
        botUid: harness.bot.uid,
        channelId: THREAD.channelId,
        creatorChannelType: 2,
        creatorChannelId: GROUP.channelId,
        threadName: THREAD_RECORD.name,
        listen: "merge",
      });
    });

    await submitThreadRequest(harness, "creator-input-unbound-parent-request");
    expectQueued(await invocation.promise);
    await waitForPromptCount(harness, 2);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    const parentBinding = await withStore(harness, (store) => store.getBinding(harness.bot.uid, GROUP));
    expect(parentBinding).not.toBeNull();
    if (parentBinding === null) throw new Error("creator binding was not created");
    expectCreatorInputPrompt(harness, parentBinding.cwd, creatorBody);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a group cannot invoke thread-send-parent", async () => {
  const invocation = Promise.withResolvers<ToolInvocation>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "try creator input from group") {
        invocation.resolve(await invokeCreatorInput(promptContext, "群消息不能冒充子区输入"));
      }
    },
  });
  try {
    await prepareChats(harness);
    await harness.service.submitInbound(
      inbound({
        messageId: "creator-input-group-rejection",
        ...GROUP,
        text: "try creator input from group",
        mentionedUserIds: [harness.bot.uid],
      }),
    );

    const toolCall = await invocation.promise;
    expect(toolCall.actions).toContain(CREATOR_ACTION);
    expect("error" in toolCall).toBe(true);
    if (!("error" in toolCall)) throw new Error("group unexpectedly sent creator input");
    expect(toolCall.error).not.toContain("unknown action");
    expect(toolCall.error).toMatch(/thread|子区/iu);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});
