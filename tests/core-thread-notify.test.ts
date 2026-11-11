import { expect, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, PromptContext } from "../src/contracts";
import type { LoopTask, LoopWorkspace } from "../src/loop/types";
import { ChannelStore } from "../src/store";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

const GROUP: ChatTarget = { channelId: "group-tools", channelType: 2 };

/** The management tool the prompt actually received. */
function managementOf(promptContext: PromptContext | undefined) {
  const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
  if (!management) throw new Error("octo_management host tool missing");
  return management;
}

/** Runs `create-thread` through the management tool the prompt actually received. */
async function createThreadViaTool(
  promptContext: PromptContext | undefined,
  args: Record<string, unknown>,
): Promise<Record<string, unknown> & { short_id: string; channel_id: string }> {
  return JSON.parse(
    await managementOf(promptContext).execute(
      { action: "create-thread", ...args },
      new AbortController().signal,
    ),
  ) as Record<string, unknown> & { short_id: string; channel_id: string };
}

/** Runs `thread-dispatch` through the management tool the prompt actually received. */
async function dispatchViaTool(
  promptContext: PromptContext | undefined,
  args: Record<string, unknown>,
): Promise<Record<string, unknown> & { delivery: string }> {
  return JSON.parse(
    await managementOf(promptContext).execute(
      { action: "thread-dispatch", ...args },
      new AbortController().signal,
    ),
  ) as Record<string, unknown> & { delivery: string };
}

function observeDispatchedOutcome(
  service: unknown,
  store: ChannelStore,
  completed: { resolve(): void },
): void {
  const seam = service as {
    notifyThreadOutcome: (taskId: string) => Promise<void>;
  };
  const notify = seam.notifyThreadOutcome.bind(service);
  seam.notifyThreadOutcome = async (taskId) => {
    await notify(taskId);
    if (store.getTask(taskId)?.messageId.startsWith("dispatch:")) completed.resolve();
  };
}

test("a task-listening thread relays the dispatched task's actual result to its creator", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const result = "实际修复结果：解析器测试通过。";
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "回传线程", listen: "task" });
        const relay = await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "PRIVATE_TASK_SECRET",
        });
        if (relay.delivery !== "queued") {
          throw new Error(`unexpected dispatch result: ${JSON.stringify(relay)}`);
        }
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "PRIVATE_TASK_SECRET") {
        emit(assistantText("earlier-progress", "正在检查旧进度。", true));
        emit(assistantText("relay-result", result, true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "task-listen-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    const notice = notices[0] ?? "";
    expect(notice).toContain(`short_id ${created.shortId}`);
    expect(notice).toContain(`channel_id ${created.channelId}`);
    expect(notice).toContain("状态：已完成");
    expect(notice).toContain(result);
    expect(notice).not.toContain("正在检查旧进度。");
    expect(notice).toContain("结果");
    expect(notice).not.toContain("PRIVATE_TASK_SECRET");

    // The notice is one ordinary task in the dispatching chat, keyed to the thread task it reports.
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const threadTask = store.getLatestTask(harness.bot.uid, {
        channelId: created.channelId,
        channelType: 5,
      });
      const groupTask = store.getLatestTask(harness.bot.uid, GROUP);
      expect(threadTask?.state).toBe("completed");
      expect(threadTask?.resultText).toContain(result);
      expect(threadTask?.relaySuppressed).toBe(false);
      expect(groupTask?.messageId).toBe(`notify:${threadTask?.id}`);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a completed task without assistant text reports the result is unavailable", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建区并派发无结果任务") {
        const thread = await createThreadViaTool(promptContext, { name: "无结果线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "PRIVATE_EMPTY_TASK" });
        return;
      }
      if (text === "PRIVATE_EMPTY_TASK") return;
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "empty-result-create",
        ...GROUP,
        text: "建区并派发无结果任务",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    const notice = notices[0] ?? "";
    expect(notice).toContain("状态：已完成");
    expect(notice).toContain("任务 ID：");
    // Missing output is stated explicitly, never rendered as a null/undefined payload.
    expect(notice).toContain("结果不可用");
    expect(notice).not.toContain("null");
    expect(notice).not.toContain("undefined");
    expect(notice).not.toContain("PRIVATE_EMPTY_TASK");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a result relay redacts recognizable credentials and bounds its length", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const token = "PRIVATE_TOKEN_9f8e7d";
  const bearer = "PRIVATE_BEARER_9f8e7d";
  const githubToken = "ghp_PRIVATEabc123456789";
  const keep = "RESULT_KEEP_ME";
  const tail = "RESULT_TAIL_MARKER";
  const longResult = `${keep} token=${token} Authorization: Bearer ${bearer} ${githubToken} ${"长结果填充。".repeat(300)} ${tail}`;
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发敏感结果") {
        const thread = await createThreadViaTool(promptContext, { name: "敏感结果线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "PRIVATE_TASK_SECRET" });
        return;
      }
      if (text === "PRIVATE_TASK_SECRET") {
        emit(assistantText("sensitive-result", longResult, true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "sensitive-result-create",
        ...GROUP,
        text: "建区并派发敏感结果",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    const notice = notices[0] ?? "";
    expect(notice).toContain(keep);
    expect(notice).not.toContain(token);
    expect(notice).not.toContain(bearer);
    expect(notice).not.toContain(githubToken);
    expect(notice).toContain("[redacted]");
    // The full body is longer than any single user-visible notice; the tail proves truncation.
    expect(notice).not.toContain(tail);
    expect(notice).toContain("…");
    expect(notice.length).toBeLessThan(1_600);
    expect(notice).not.toContain("PRIVATE_TASK_SECRET");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("default merge listening keeps a completed dispatch quiet", async () => {
  const unexpected: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "普通线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务" });
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "子区任务") {
        emit(assistantText("plain-result", "子区完成", true));
        return;
      }
      unexpected.push(text);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "plain-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    await waitForStatus(
      harness.service,
      { channelId: created.channelId, channelType: 5 },
      (status) => status.state === "completed",
    );
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    expect(unexpected).toHaveLength(0);

    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(store.getThreadListen(harness.bot.uid, { channelId: created.channelId, channelType: 5 })?.listen).toBe("merge");
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.text).toBe("建区并派发");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a successful dispatched merge emits only the merge summary, not a task terminal notice", async () => {
  const created = { shortId: "", channelId: "" };
  const childStarted = Promise.withResolvers<void>();
  const allowMerge = Promise.withResolvers<void>();
  const mergeFinished = Promise.withResolvers<void>();
  const mergeNoticeSeen = Promise.withResolvers<void>();
  const mergeNotices: string[] = [];
  const taskNotices: string[] = [];
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text.startsWith("[子区合并回传]")) {
        mergeNotices.push(text);
        mergeNoticeSeen.resolve();
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        taskNotices.push(text);
        return;
      }
      if (text === "create and dispatch merge task") {
        const thread = await createThreadViaTool(promptContext, {
          name: "只回传合并摘要",
          worktree_name: "dispatched-merge-summary",
          listen: "task",
        });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "merge child workspace",
        });
        return;
      }
      if (text === "merge child workspace") {
        childStarted.resolve();
        await allowMerge.promise;
        await managementOf(promptContext).execute(
          { action: "thread-merge", remove: false },
          new AbortController().signal,
          "dispatched-merge-operation",
        );
        mergeFinished.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "dispatch-merge-create",
        ...GROUP,
        text: "create and dispatch merge task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    await childStarted.promise;
    const thread: ChatTarget = { channelId: created.channelId, channelType: 5 };
    allowMerge.resolve();
    await mergeFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    await mergeNoticeSeen.promise;
    for (let attempt = 0; attempt < 50; attempt += 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    expect(mergeNotices).toHaveLength(1);
    expect(mergeNotices[0]).not.toContain("merge child workspace");
    expect(taskNotices).toEqual([]);
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const task = store.getLatestTask(harness.bot.uid, thread);
      const events = store.listThreadMergeEvents(harness.bot.uid);
      expect(task?.state).toBe("completed");
      expect(task?.relaySuppressed).toBe(true);
      expect(events).toHaveLength(1);
      expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId).toBe(
        `merge-notify:${events[0]!.id}`,
      );
    } finally {
      store.close();
    }
  } finally {
    allowMerge.resolve();
    await closeCoreHarness(harness);
  }
});

test("task notification rechecks policy before steering after an async member lookup", async () => {
  const created = { channelId: "", shortId: "" };
  const parentAppended: string[] = [];
  const childStarted = Promise.withResolvers<void>();
  const finishChild = Promise.withResolvers<void>();
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const memberLookupStarted = Promise.withResolvers<void>();
  const releaseMemberLookup = Promise.withResolvers<void>();
  const noticeAttemptFinished = Promise.withResolvers<void>();
  let blockMemberLookup = false;
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text, waitForAppend }) => {
      if (text === "start guarded parent") {
        const thread = await createThreadViaTool(promptContext, { name: "并发监听线程", listen: "task" });
        created.channelId = thread.channel_id;
        created.shortId = thread.short_id;
        await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "wait before terminal notice",
        });
        parentStarted.resolve();
        const arrival = waitForAppend().then(({ text: appended }) => {
          parentAppended.push(appended);
        });
        await Promise.race([arrival, releaseParent.promise]);
        await releaseParent.promise;
        return;
      }
      if (text === "wait before terminal notice") {
        childStarted.resolve();
        await finishChild.promise;
        emit(assistantText("guarded-child-result", "完成", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) return;
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  store.setChatMode(harness.bot.uid, GROUP, "steer");
  observeDispatchedOutcome(harness.service, store, noticeAttemptFinished);
  const getGroupMembers = harness.transport.getGroupMembers.bind(harness.transport);
  harness.transport.getGroupMembers = async (groupId, signal) => {
    if (blockMemberLookup && groupId === GROUP.channelId) {
      blockMemberLookup = false;
      memberLookupStarted.resolve();
      await releaseMemberLookup.promise;
    }
    return getGroupMembers(groupId, signal);
  };
  const thread: ChatTarget = { channelId: "", channelType: 5 };
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "steer-policy-race",
        ...GROUP,
        text: "start guarded parent",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await parentStarted.promise;
    await childStarted.promise;
    thread.channelId = created.channelId;
    blockMemberLookup = true;
    finishChild.resolve();
    await memberLookupStarted.promise;
    store.setThreadListen(harness.bot.uid, thread, "off");
    releaseMemberLookup.resolve();
    await noticeAttemptFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    releaseParent.resolve();
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    expect(parentAppended.filter((text) => text.startsWith("[子区任务回传]"))).toEqual([]);
    expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId).toBe("steer-policy-race");
    expect(store.getThreadListen(harness.bot.uid, thread)?.listen).toBe("off");
  } finally {
    finishChild.resolve();
    releaseMemberLookup.resolve();
    releaseParent.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("task notification rechecks policy before accepting after an async cwd probe", async () => {
  const created = { channelId: "" };
  const childStarted = Promise.withResolvers<void>();
  const finishChild = Promise.withResolvers<void>();
  const cwdProbeStarted = Promise.withResolvers<void>();
  const releaseCwdProbe = Promise.withResolvers<void>();
  const noticeAttemptFinished = Promise.withResolvers<void>();
  const notices: string[] = [];
  let blockCwdProbe = false;
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "create and dispatch") {
        const thread = await createThreadViaTool(promptContext, { name: "接收监听线程", listen: "task" });
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "wait before acceptance",
        });
        return;
      }
      if (text === "wait before acceptance") {
        childStarted.resolve();
        await finishChild.promise;
        emit(assistantText("accept-guard-result", "完成", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) notices.push(text);
    },
  });
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  observeDispatchedOutcome(harness.service, store, noticeAttemptFinished);
  // Private seam used to hold the real acceptTask cwd probe at its asynchronous boundary.
  const serviceSeam = harness.service as unknown as {
    sessions: { defaultWorkDirectory: (target: ChatTarget) => Promise<string> };
  };
  const serviceSessions = serviceSeam.sessions;
  const defaultWorkDirectory = serviceSessions.defaultWorkDirectory.bind(serviceSessions);
  serviceSessions.defaultWorkDirectory = async (target) => {
    if (blockCwdProbe && target.channelId === GROUP.channelId) {
      blockCwdProbe = false;
      cwdProbeStarted.resolve();
      await releaseCwdProbe.promise;
    }
    return defaultWorkDirectory(target);
  };
  const thread: ChatTarget = { channelId: "", channelType: 5 };
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "accept-policy-race",
        ...GROUP,
        text: "create and dispatch",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    const parentBinding = store.getBinding(harness.bot.uid, GROUP);
    if (!parentBinding) throw new Error("parent binding missing before cwd-probe test");
    store.updateBindingSession(harness.bot.uid, GROUP, parentBinding.sessionId, null);
    await childStarted.promise;
    thread.channelId = created.channelId;
    blockCwdProbe = true;
    finishChild.resolve();
    await cwdProbeStarted.promise;
    store.beginThreadMerge(harness.bot.uid, thread);
    releaseCwdProbe.resolve();
    await noticeAttemptFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");

    expect(notices).toEqual([]);
    expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId).toBe("accept-policy-race");
    expect(store.getThreadListen(harness.bot.uid, thread)).toMatchObject({
      listen: "task",
      mergePending: true,
    });
  } finally {
    finishChild.resolve();
    releaseCwdProbe.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});
test("a merge consumed while off does not replay after the policy changes to task and restarts", async () => {
  const observed: string[] = [];
  const created = { shortId: "", channelId: "" };
  const first = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "创建静默合并区") {
        const thread = await createThreadViaTool(promptContext, {
          name: "静默合并线程",
          worktree_name: "merge-off",
          listen: "off",
        });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "执行静默合并") {
        await managementOf(promptContext).execute(
          { action: "thread-merge", target: "main", remove: false },
          new AbortController().signal,
        );
        return;
      }
      if (text.startsWith("[子区合并回传]") || text.startsWith("[子区任务回传]")) {
        observed.push(text);
      }
    },
  });
  const root = first.root;
  let firstClosed = false;
  try {
    await first.service.submitInbound(
      inbound({
        messageId: "off-merge-create",
        ...GROUP,
        text: "创建静默合并区",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, GROUP, (status) => status.state === "completed");

    const thread: ChatTarget = { channelId: created.channelId, channelType: 5 };
    await waitForStatus(
      first.service,
      thread,
      (status) => status.cwd === join(root, "worktrees", created.shortId),
    );
    await first.service.submitInbound(
      inbound({
        messageId: "off-merge-run",
        ...thread,
        text: "执行静默合并",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await first.workers.waitForPromptCount(2);
    await waitForStatus(first.service, thread, (status) => status.state === "completed");
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    expect(observed).toEqual([]);
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;

    const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
    const events = store.listThreadMergeEvents(first.bot.uid);
    try {
      expect(events).toHaveLength(1);
      expect(
        store.getCommandClaimState(first.bot.uid, GROUP, `merge-notify-off:${events[0]!.id}`),
      ).toBe("completed");
      expect(store.getThreadListen(first.bot.uid, thread)?.listen).toBe("off");
      store.setThreadListen(first.bot.uid, thread, "task");
    } finally {
      store.close();
    }

    const second = await createCoreHarness({
      root,
      transport: first.transport,
      prompt: async ({ text }) => {
        observed.push(text);
      },
    });
    try {
      for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
      expect(observed).toEqual([]);
      const reopened = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
      try {
        expect(reopened.getThreadListen(second.bot.uid, thread)?.listen).toBe("task");
        expect(reopened.listThreadMergeEvents(second.bot.uid)).toEqual(events);
        expect(reopened.getLatestTask(second.bot.uid, GROUP)?.messageId).toBe("off-merge-create");
      } finally {
        reopened.close();
      }
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    if (!firstClosed) await closeCoreHarness(first);
  }
});
test("a resumed dispatched task retains its last finalized result across restart", async () => {
  const created = { channelId: "" };
  const childOutput = Promise.withResolvers<void>();
  const first = await createCoreHarness({
    prompt: async ({ emit, promptContext, text, waitForAbort }) => {
      if (text === "create resumable relay") {
        const thread = await createThreadViaTool(promptContext, { name: "续跑结果线程", listen: "task" });
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "resumable child" });
      } else if (text === "resumable child") {
        emit(assistantText("before-restart", "已完成的子区结果", true));
        childOutput.resolve();
        await waitForAbort();
      }
    },
  });
  const root = first.root;
  try {
    await first.service.submitInbound(inbound({
      messageId: "resume-relay-create", ...GROUP, text: "create resumable relay",
      mentionedUserIds: [first.bot.uid],
    }));
    await childOutput.promise;
    await closeCoreHarness(first, { remove: false });
    const notices: string[] = [];
    const seen = Promise.withResolvers<void>();
    const second = await createCoreHarness({
      root, transport: first.transport,
      prompt: async ({ text }) => {
        if (text.startsWith("[子区任务回传]")) {
          notices.push(text);
          seen.resolve();
        }
      },
    });
    try {
      await seen.promise;
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain("已完成的子区结果");
      expect(notices[0]).toContain(created.channelId);
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    await closeCoreHarness(first);
  }
});

test("restart retries an undispatched task notice but not an ambiguous steer", async () => {
  const created = { channelId: "" };
  const first = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "create relay recovery thread") {
        const thread = await createThreadViaTool(promptContext, { name: "任务恢复线程", listen: "task" });
        created.channelId = thread.channel_id;
      }
    },
  });
  const root = first.root;
  try {
    await first.service.submitInbound(inbound({
      messageId: "task-recovery-create", ...GROUP, text: "create relay recovery thread",
      mentionedUserIds: [first.bot.uid],
    }));
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, GROUP, (status) => status.state === "completed");
    const target: ChatTarget = { channelId: created.channelId, channelType: 5 };
    const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
    try {
      for (const id of ["pending-relay", "intent-relay"]) {
        const messageId = `dispatch:${id}`;
        expect(store.acceptTask(
          { botUid: first.bot.uid, target, messageId, senderId: "owner-1", text: id, supported: true, kind: "task" },
          {
            id, botUid: first.bot.uid, ...target, messageId, senderId: "owner-1", text: id,
            sessionId: `session-${id}`, cwd: root, state: "queued", resultText: null,
            errorText: null, messageSeq: 0, historyEligible: false, relaySuppressed: false,
          },
        )).toBe(true);
        store.setTaskFinalResultText(id, `实际结果 ${id}`);
        store.markCompleted(id, `实际结果 ${id}`);
        expect(store.claimCommand({
          botUid: first.bot.uid, target: GROUP, messageId: `notify:${id}`, senderId: "owner-1",
          text: "relay", supported: true, kind: "command",
        })).toBe(true);
      }
      expect(store.claimThreadTaskDispatchIntent("intent-relay")).toBe(true);
    } finally {
      store.close();
    }
    await closeCoreHarness(first, { remove: false });
    const notices: string[] = [];
    const seen = Promise.withResolvers<void>();
    const second = await createCoreHarness({
      root, transport: first.transport,
      prompt: async ({ text }) => {
        if (text.startsWith("[子区任务回传]")) {
          notices.push(text);
          seen.resolve();
        }
      },
    });
    try {
      await seen.promise;
      await waitForStatus(second.service, GROUP, (status) => status.state === "completed");
      expect(notices).toHaveLength(1);
      expect(notices[0]).toContain("实际结果 pending-relay");
      expect(notices[0]).not.toContain("intent-relay");
      const recovered = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
      try {
        expect(recovered.getTaskByMessageId(second.bot.uid, GROUP, "notify:intent-relay")).toBeNull();
      } finally {
        recovered.close();
      }
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    await closeCoreHarness(first);
  }
});

test("restart retries only merge events whose steer claim never crossed dispatch intent", async () => {
  const created = { shortId: "", channelId: "" };
  const first = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text !== "create recovery thread") return;
      const thread = await createThreadViaTool(promptContext, { name: "合并恢复线程" });
      created.shortId = thread.short_id;
      created.channelId = thread.channel_id;
    },
  });
  const root = first.root;
  let firstClosed = false;
  try {
    await first.service.submitInbound(
      inbound({
        messageId: "merge-recovery-create",
        ...GROUP,
        text: "create recovery thread",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, GROUP, (status) => status.state === "completed");
    const thread: ChatTarget = { channelId: created.channelId, channelType: 5 };
    const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
    try {
      const pending = {
        id: "merge-pending-before-steer",
        botUid: first.bot.uid,
        channelId: thread.channelId,
        targetBranch: "main",
        commit: "commit-before-steer",
        removed: false,
        archive: "not-scheduled" as const,
      };
      const intent = {
        ...pending,
        id: "merge-intent-before-restart",
        commit: "commit-intent",
      };
      store.saveThreadMergeEvent(pending);
      store.saveThreadMergeEvent(intent);
      const messageId = `merge-notify:${pending.id}`;
      const intentMessageId = `merge-notify:${intent.id}`;
      expect(store.claimCommand({
        botUid: first.bot.uid,
        target: GROUP,
        messageId,
        senderId: first.bot.uid,
        text: "interrupted merge notice",
        supported: true,
        kind: "command",
      })).toBe(true);
      expect(store.claimThreadMergeDispatchIntent(intent.id)).toBe(true);
      expect(store.claimCommand({
        botUid: first.bot.uid,
        target: GROUP,
        messageId: intentMessageId,
        senderId: first.bot.uid,
        text: "uncertain merge notice",
        supported: true,
        kind: "command",
      })).toBe(true);
    } finally {
      store.close();
    }
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;

    const observed: string[] = [];
    const noticeSeen = Promise.withResolvers<void>();
    const second = await createCoreHarness({
      root,
      transport: first.transport,
      prompt: async ({ text }) => {
        observed.push(text);
        if (text.startsWith("[子区合并回传]")) noticeSeen.resolve();
      },
    });
    try {
      await noticeSeen.promise;
      await waitForStatus(second.service, GROUP, (status) => status.state === "completed");
      const recovered = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
      try {
        expect(observed.filter((text) => text.startsWith("[子区合并回传]"))).toHaveLength(1);
        expect(recovered.getLatestTask(second.bot.uid, GROUP)?.messageId).toBe(
          `merge-notify:merge-pending-before-steer`,
        );
        expect(recovered.getCommandClaimState(
          second.bot.uid,
          GROUP,
          `merge-notify:merge-pending-before-steer`,
        )).toBeNull();
        expect(recovered.getCommandClaimState(
          second.bot.uid,
          GROUP,
          `merge-notify:merge-intent-before-restart`,
        )).toBe("pending");
        expect(recovered.listPendingThreadMergeEvents(second.bot.uid)).toEqual([]);
      } finally {
        recovered.close();
      }
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    if (!firstClosed) await closeCoreHarness(first);
  }
});
test("merge event steering rechecks off policy after the asynchronous member lookup", async () => {
  const created = { shortId: "", channelId: "" };
  const parentStarted = Promise.withResolvers<void>();
  const releaseParent = Promise.withResolvers<void>();
  const memberLookupStarted = Promise.withResolvers<void>();
  const releaseMemberLookup = Promise.withResolvers<void>();
  const mergeFinished = Promise.withResolvers<void>();
  const parentAppended: string[] = [];
  let blockMemberLookup = false;
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAppend }) => {
      if (text === "create merge race thread") {
        const thread = await createThreadViaTool(promptContext, {
          name: "合并回执竞态",
          worktree_name: "merge-notice-race",
        });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "hold merge event recipient") {
        parentStarted.resolve();
        const arrival = waitForAppend().then(({ text: appended }) => {
          parentAppended.push(appended);
        });
        await Promise.race([arrival, releaseParent.promise]);
        await releaseParent.promise;
        return;
      }
      if (text === "perform successful merge") {
        await managementOf(promptContext).execute(
          { action: "thread-merge", remove: false },
          new AbortController().signal,
          "merge-notice-race-tool",
        );
        mergeFinished.resolve();
        return;
      }
      if (text.startsWith("[子区合并回传]")) {
        parentAppended.push(text);
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  store.setChatMode(harness.bot.uid, GROUP, "steer");
  const getGroupMembers = harness.transport.getGroupMembers.bind(harness.transport);
  harness.transport.getGroupMembers = async (groupId, signal) => {
    if (blockMemberLookup && groupId === GROUP.channelId) {
      blockMemberLookup = false;
      memberLookupStarted.resolve();
      await releaseMemberLookup.promise;
    }
    return getGroupMembers(groupId, signal);
  };
  const thread: ChatTarget = { channelId: "", channelType: 5 };
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "merge-race-create",
        ...GROUP,
        text: "create merge race thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    thread.channelId = created.channelId;
    await waitForStatus(
      harness.service,
      thread,
      (status) => status.cwd === join(harness.root, "worktrees", created.shortId),
    );
    await harness.service.submitInbound(
      inbound({
        messageId: "merge-race-parent-hold",
        ...GROUP,
        text: "hold merge event recipient",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await parentStarted.promise;
    blockMemberLookup = true;
    await harness.service.submitInbound(
      inbound({
        messageId: "merge-race-trigger",
        ...thread,
        text: "perform successful merge",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await memberLookupStarted.promise;
    store.setThreadListen(harness.bot.uid, thread, "off");
    releaseMemberLookup.resolve();
    await mergeFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    expect(parentAppended).toEqual([]);
    const events = store.listThreadMergeEvents(harness.bot.uid);
    expect(events).toHaveLength(1);
    expect(store.isThreadMergeEventPending(events[0]!.id)).toBe(false);
    expect(
      store.getCommandClaimState(
        harness.bot.uid,
        GROUP,
        `merge-notify-off:${events[0]!.id}`,
      ),
    ).toBe("completed");
    expect(store.getThreadListen(harness.bot.uid, thread)).toMatchObject({
      listen: "off",
      mergePending: false,
    });
    releaseParent.resolve();
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
  } finally {
    releaseMemberLookup.resolve();
    releaseParent.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});


test("a direct thread task is not reported at task listening level by default", async () => {
  const unexpected: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建一个普通线程") {
        const thread = await createThreadViaTool(promptContext, { name: "普通线程" });
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "子区直接任务") {
        emit(assistantText("direct-result", "直接完成", true));
        return;
      }
      unexpected.push(text);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "direct-create",
        ...GROUP,
        text: "建一个普通线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.submitInbound(
      inbound({
        messageId: "direct-task",
        channelId: created.channelId,
        channelType: 5,
        text: "子区直接任务",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(
      harness.service,
      { channelId: created.channelId, channelType: 5 },
      (status) => status.state === "completed",
    );
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    expect(unexpected).toHaveLength(0);

    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        store.getLatestTask(harness.bot.uid, { channelId: created.channelId, channelType: 5 })
          ?.relaySuppressed,
      ).toBe(false);
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.text).toBe("建一个普通线程");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test.each([
  ["model", "OMP model reported an error: Codex websocket transport error: websocket closed (1006) token=PRIVATE_TOKEN", "Codex websocket transport error: websocket closed (1006)"],
  ["other", "数据库事务冲突，请重试 token=PRIVATE_TOKEN", "数据库事务冲突，请重试"],
])("a failed %s dispatch relays a safe reason", async (_name, failure, reason) => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "失败线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "PRIVATE_FAILED_TASK" });
        return;
      }
      if (text === "PRIVATE_FAILED_TASK") throw new Error(failure);
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "fail-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await noticeSeen.promise;
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("状态：失败");
    expect(notices[0]).toContain(reason);
    expect(notices[0]).not.toContain("PRIVATE_TOKEN");
    expect(notices[0]).not.toContain("PRIVATE_FAILED_TASK");
    expect(notices[0]).toContain("任务 ID：");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a context read failure reports the failing dispatched task to its creator", async () => {
  const noticeSeen = Promise.withResolvers<string>();
  let threadChannelId = "";
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "启动失败", listen: "task" });
        threadChannelId = thread.channel_id;
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务" });
      } else if (text.startsWith("[子区任务回传]")) {
        noticeSeen.resolve(text);
      }
    },
  });
  const getContext = harness.transport.getGroupContext.bind(harness.transport);
  harness.transport.getGroupContext = async target => {
    if (target.channelId === threadChannelId) throw new Error("Octo group context request failed with HTTP 503 token=PRIVATE_TOKEN");
    return getContext(target);
  };
  try {
    await harness.service.submitInbound(inbound({
      messageId: "context-fail-create",
      ...GROUP,
      text: "建区并派发",
      mentionedUserIds: [harness.bot.uid],
    }));
    const notice = await noticeSeen.promise;
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const task = store.getLatestTask(harness.bot.uid, { channelId: threadChannelId, channelType: 5 });
      expect(task?.state).toBe("failed");
      expect(task?.errorText).toContain("context-read");
      expect(notice).toContain(`任务 ID：${task?.id}`);
      expect(notice).toContain("群说明读取失败");
      expect(notice).toContain("HTTP 503");
      expect(notice).not.toContain("PRIVATE_TOKEN");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("task-level listening reports both cancelled dispatches, queued ones included", async () => {
  const notices: string[] = [];
  const bothSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建区") {
        const thread = await createThreadViaTool(promptContext, { name: "取消线程", listen: "task" });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "派发两条") {
        await dispatchViaTool(promptContext, { short_id: created.shortId, text: "慢任务" });
        await dispatchViaTool(promptContext, { short_id: created.shortId, text: "排队任务" });
        return;
      }
      if (text === "慢任务" || text === "排队任务") {
        await waitForAbort();
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        if (notices.length === 2) bothSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "cancel-create",
        ...GROUP,
        text: "建区",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    const thread = { channelId: created.channelId, channelType: 5 as const };

    // Queue mode keeps the second dispatch behind the running one instead of steering into it.
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-mode", ...thread, text: "/mode queue", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-dispatch", ...GROUP, text: "派发两条", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(3);
    await harness.service.submitInbound(
      inbound({ messageId: "cancel-command", ...thread, text: "/cancel", mentionedUserIds: [harness.bot.uid] }),
    );
    await bothSeen.promise;

    expect(notices).toHaveLength(2);
    for (const notice of notices) {
      expect(notice).toContain("状态：已取消");
      expect(notice).toContain("说明：派发任务已取消。");
    }
    expect(new Set(notices.map((notice) => notice.match(/任务 ID：(.+)/)?.[1])).size).toBe(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an interrupted task-level dispatch reports a fixed terminal summary", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "中断线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "中断任务" });
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "中断任务") {
        await waitForAbort();
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "interrupt-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);

    const thread = { channelId: created.channelId, channelType: 5 as const };
    await harness.service.submitInbound(
      inbound({ messageId: "interrupt-command", ...thread, text: "/interrupt", mentionedUserIds: [harness.bot.uid] }),
    );
    await noticeSeen.promise;
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("状态：已中断");
    expect(notices[0]).toContain("说明：派发任务已中断。");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a task notice joins the creator's running turn in steer mode", async () => {
  const appended: string[] = [];
  const appendedSeen = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text, waitForAppend }) => {
      if (text === "建区并派发并等待") {
        // The workspace keeps the thread off this turn's directory, so both can run at once.
        const thread = await createThreadViaTool(promptContext, {
          name: "并入线程",
          worktree_name: "relay-workspace",
          listen: "task",
        });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务" });
        const message = await waitForAppend();
        appended.push(message.text);
        appendedSeen.resolve();
        return;
      }
      if (text === "子区任务") {
        emit(assistantText("steer-result", "并入结果", true));
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "steer-create",
        ...GROUP,
        text: "建区并派发并等待",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await appendedSeen.promise;
    expect(appended).toHaveLength(1);
    expect(appended[0]).toContain("[子区任务回传]");
    expect(appended[0]).toContain("任务 ID：");

    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expect(harness.workers.prompts.some(({ text }) => text.startsWith("[子区任务回传]"))).toBe(false);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a task notice is never taken as the answer to an open question", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "问答线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务" });
        return;
      }
      if (text === "子区任务") {
        await release.promise;
        emit(assistantText("question-result", "问答结果", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "question-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);

    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      store.createQuestion({
        id: "question-open",
        taskId: "task-open",
        botUid: harness.bot.uid,
        channelType: 2,
        channelId: GROUP.channelId,
        payload: JSON.stringify({ questions: [{ question: "继续吗" }] }),
      });
    } finally {
      store.close();
    }

    release.resolve();
    await noticeSeen.promise;

    const reopened = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const question = reopened.getQuestion("question-open");
      expect(question?.state).toBe("pending");
      expect(question?.answer).toBeNull();
    } finally {
      reopened.close();
    }
  } finally {
    release.resolve();
    await closeCoreHarness(harness);
  }
});

test("a graceful stop never relays its in-flight dispatched tasks", async () => {
  const notices: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "停机线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "长任务" });
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "长任务") {
        await waitForAbort();
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "stop-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);

    // A graceful stop suspends the task without publishing a completion notice.
    await harness.service.stop();
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        store.getLatestTask(harness.bot.uid, { channelId: created.channelId, channelType: 5 })?.state,
      ).toBe("running");
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId.startsWith("notify:")).toBe(false);
    } finally {
      store.close();
    }
    expect(notices).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("queued dispatches resume after restart and emit each task notice once", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-listen-restart-"));
  const firstNotices: string[] = [];
  const firstSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const first = await createCoreHarness({
    root,
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "回传线程", listen: "task" });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "第一件事" });
        return;
      }
      if (text === "第一件事") {
        emit(assistantText("first-result", "第一件结果", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        firstNotices.push(text);
        firstSeen.resolve();
        return;
      }
      throw new Error(`unexpected first-session prompt: ${text}`);
    },
  });
  try {
    await first.service.submitInbound(
      inbound({
        messageId: "restart-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await firstSeen.promise;
    await waitForStatus(first.service, GROUP, (status) => status.state === "completed");
    expect(firstNotices).toHaveLength(1);
    expect(firstNotices[0]).toContain("任务 ID：");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  // A dispatch accepted before restart retains its durable task and the thread's policy.
  const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
  try {
    const persisted = store.getLatestTask(first.bot.uid, {
      channelId: created.channelId,
      channelType: 5,
    });
    expect(store.getThreadListen(first.bot.uid, { channelId: created.channelId, channelType: 5 })?.listen).toBe("task");
    expect(persisted?.relaySuppressed).toBe(false);
    expect(
      store.acceptTask(
        {
          botUid: first.bot.uid,
          target: { channelId: created.channelId, channelType: 5 },
          messageId: "dispatch:queued-before-restart",
          senderId: "owner-1",
          text: "重启前的排队任务",
          supported: true,
          kind: "task",
        },
        {
          id: "queued-relay-task",
          botUid: first.bot.uid,
          channelType: 5,
          channelId: created.channelId,
          messageId: "dispatch:queued-before-restart",
          senderId: "owner-1",
          text: "重启前的排队任务",
          sessionId: "session-queued",
          cwd: first.root,
          state: "queued",
          resultText: null,
          errorText: null,
          messageSeq: 0,
          historyEligible: false,
          relaySuppressed: false,
        },
      ),
    ).toBe(true);
  } finally {
    store.close();
  }

  const secondNotices: string[] = [];
  const secondSeen = Promise.withResolvers<void>();
  const second = await createCoreHarness({
    root,
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "再派发一次") {
        const thread = await createThreadViaTool(promptContext, { name: "回传线程", listen: "task" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "第二件事" });
        return;
      }
      if (text === "重启前的排队任务") {
        emit(assistantText("recovered-result", "恢复排队任务的结果", true));
        return;
      }
      if (text === "第二件事") {
        emit(assistantText("second-result", "第二件结果", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        secondNotices.push(text);
        if (secondNotices.length === 2) secondSeen.resolve();
        return;
      }
      throw new Error(`unexpected second-session prompt: ${text}`);
    },
  });
  try {
    await second.service.submitInbound(
      inbound({
        messageId: "restart-second",
        ...GROUP,
        text: "再派发一次",
        mentionedUserIds: [second.bot.uid],
      }),
    );
    await secondSeen.promise;

    expect(secondNotices).toHaveLength(2);
    expect(secondNotices.every((notice) => notice.includes("任务 ID："))).toBe(true);
    expect(secondNotices.some((notice) => notice.includes("第一件结果"))).toBe(false);

    const reopened = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
    try {
      expect(reopened.getTask("queued-relay-task")?.state).toBe("completed");
    } finally {
      reopened.close();
    }
  } finally {
    await closeCoreHarness(second);
  }
});
test("a new thread defaults to merge-only listening, so dispatch completion stays quiet", async () => {
  const unexpected: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "默认静默线程" });
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "DEFAULT_TASK_SECRET",
        });
        return;
      }
      if (text === "DEFAULT_TASK_SECRET") {
        emit(assistantText("default-task-result", "DEFAULT_RESULT_SECRET", true));
        return;
      }
      if (text.startsWith("[子区")) unexpected.push(text);
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "default-listen-dispatch",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    const thread = { channelId: created.channelId, channelType: 5 as const };
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));

    expect(unexpected).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an independent Loop thread task relays its actual result at task listening level", async () => {
  const workspace: LoopWorkspace = {
    workspaceId: "loop-notify-workspace",
    name: "Notify tasks",
    slug: "notify-tasks",
  };
  const baseTask: LoopTask = {
    taskId: "loop-notify-result",
    title: "Before child work",
    description: "fixture task description",
    status: "open",
    priority: "normal",
    assigneeType: "member",
    assigneeId: "owner-1",
    assigneeOctoUid: "owner-1",
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-03T00:00:00Z",
    updatedAt: "2026-11-04T00:00:00Z",
  };
  const updatedTask: LoopTask = {
    ...baseTask,
    title: "After child work",
    updatedAt: "2026-11-05T00:00:00Z",
  };
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(workspace.workspaceId, workspace);
  loop.tasks.set(workspace.workspaceId, [baseTask]);
  const threadResult = "LOOP_ACTUAL_RESULT：子区工作已完成。";
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  let threadChannelId = "";
  const harness = await createCoreHarness({
    loop,
    prompt: async ({ emit, text }) => {
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      if (text.includes("After child work")) {
        emit(assistantText("loop-result", threadResult, true));
        return;
      }
      // The initial bind snapshot and bind receipts are ordinary chat input in this fixture.
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "loop-bind-initial",
        ...GROUP,
        text: `/bind loop ${workspace.workspaceId}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "completed" && status.queued === 0,
    );

    const thread = await harness.transport.createThread(GROUP.channelId, "Loop 结果线程");
    threadChannelId = thread.channelId;
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      store.bindLoopTask({
        botUid: harness.bot.uid,
        groupId: GROUP.channelId,
        workspaceId: workspace.workspaceId,
        taskId: baseTask.taskId,
      }, thread.channelId);
      store.ensureThreadListen({
        botUid: harness.bot.uid,
        channelId: thread.channelId,
        creatorChannelType: 2,
        creatorChannelId: GROUP.channelId,
        threadName: thread.name,
        listen: "task",
      });
    } finally {
      store.close();
    }

    loop.tasks.set(workspace.workspaceId, [updatedTask]);
    await harness.service.submitInbound(
      inbound({
        messageId: "loop-bind-update",
        ...GROUP,
        text: `/bind loop ${workspace.workspaceId}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(
      harness.service,
      { channelId: threadChannelId, channelType: 5 },
      (status) => status.state === "completed",
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("Loop 任务");
    expect(notices[0]).toContain(threadResult);
    expect(notices[0]).toContain("任务 ID：");
    expect(notices[0]).not.toContain("After child work");

    const reopened = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const threadTask = reopened.getLatestTask(harness.bot.uid, {
        channelId: threadChannelId,
        channelType: 5,
      });
      expect(threadTask?.messageId.startsWith("loop:")).toBe(true);
      expect(threadTask?.resultText).toBe(threadResult);
    } finally {
      reopened.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("restart recovers a completed task whose notice was never accepted, then later restarts do not duplicate it", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-notify-crash-"));
  const result = "重启前已经完成的实际结果。";
  const created = { channelId: "", taskId: "" };
  const relayAttempted = Promise.withResolvers<void>();
  const releaseRelay = Promise.withResolvers<void>();
  const first = await createCoreHarness({
    root,
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "崩溃窗口线程", listen: "task" });
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "崩溃窗口任务" });
        return;
      }
      if (text === "崩溃窗口任务") {
        emit(assistantText("crash-window-result", result, true));
        return;
      }
      throw new Error(`unexpected first-session prompt: ${text}`);
    },
  });
  const botUid = first.bot.uid;
  const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
  const seam = first.service as unknown as {
    notifyThreadOutcome: (taskId: string) => Promise<void>;
  };
  const notify = seam.notifyThreadOutcome.bind(first.service);
  // Crash point: the terminal row and its result are committed, but no notice was accepted.
  seam.notifyThreadOutcome = async (taskId) => {
    const task = store.getTask(taskId);
    if (
      task?.channelType === 5
      && task.messageId.startsWith("dispatch:")
      && task.state === "completed"
    ) {
      created.taskId = task.id;
      relayAttempted.resolve();
      await releaseRelay.promise;
    }
    await notify(taskId);
  };
  let firstClosed = false;
  let storeClosed = false;
  try {
    await first.service.submitInbound(
      inbound({
        messageId: "crash-window-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await relayAttempted.promise;
    const threadTask = store.getTask(created.taskId);
    expect(threadTask?.state).toBe("completed");
    expect(threadTask?.resultText).toBe(result);
    expect(store.getTaskByMessageId(botUid, GROUP, `notify:${created.taskId}`)).toBeNull();
    store.close();
    storeClosed = true;

    const stopping = first.service.stop();
    releaseRelay.resolve();
    await stopping;
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;
  } finally {
    releaseRelay.resolve();
    if (!storeClosed) store.close();
    if (!firstClosed) await closeCoreHarness(first);
  }

  const recoveredNotices: string[] = [];
  const recoveredSeen = Promise.withResolvers<void>();
  const second = await createCoreHarness({
    root,
    transport: first.transport,
    prompt: async ({ text }) => {
      if (text.startsWith("[子区任务回传]")) {
        recoveredNotices.push(text);
        recoveredSeen.resolve();
      }
    },
  });
  try {
    await recoveredSeen.promise;
    await waitForStatus(second.service, GROUP, (status) => status.state === "completed" && status.queued === 0);
    expect(recoveredNotices).toHaveLength(1);
    expect(recoveredNotices[0]).toContain(result);
    expect(recoveredNotices[0]).toContain(`任务 ID：${created.taskId}`);
    const reopened = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
    try {
      const noticeTask = reopened.getTaskByMessageId(botUid, GROUP, `notify:${created.taskId}`);
      expect(noticeTask?.state).toBe("completed");
      expect(reopened.getLatestTask(botUid, GROUP)?.messageId).toBe(`notify:${created.taskId}`);
    } finally {
      reopened.close();
    }
  } finally {
    await closeCoreHarness(second, { remove: false });
  }

  const duplicateNotices: string[] = [];
  const third = await createCoreHarness({
    root,
    transport: second.transport,
    prompt: async ({ text }) => {
      if (text.startsWith("[子区任务回传]")) duplicateNotices.push(text);
    },
  });
  try {
    // The already delivered notice is durable; another restart must neither accept nor run it again.
    await waitForStatus(third.service, GROUP, (status) => status.state === "completed" && status.queued === 0);
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    expect(duplicateNotices).toEqual([]);
    const reopened = await ChannelStore.open(join(third.stateDir, "channel.sqlite"));
    try {
      const noticeTask = reopened.getTaskByMessageId(botUid, GROUP, `notify:${created.taskId}`);
      expect(noticeTask?.state).toBe("completed");
      expect(reopened.getLatestTask(botUid, GROUP)?.messageId).toBe(`notify:${created.taskId}`);
    } finally {
      reopened.close();
    }
  } finally {
    await closeCoreHarness(third);
  }
});

test("create-thread listen=task relays terminal identity and the actual result", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { channelId: "", shortId: "" };
  const result = "主动监听线程的实际结果。";
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "创建并派发") {
        const thread = await createThreadViaTool(promptContext, {
          name: "主动监听线程",
          listen: "task",
        });
        created.channelId = thread.channel_id;
        created.shortId = thread.short_id;
        await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "PRIVATE_TASK_SECRET",
        });
        return;
      }
      if (text === "PRIVATE_TASK_SECRET") {
        emit(assistantText("private-task-result", result, true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
      }
    },
  });
  try {
    await harness.service.submitInbound(
      inbound({
        messageId: "task-listen-create",
        ...GROUP,
        text: "创建并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    await waitForStatus(
      harness.service,
      { channelId: created.channelId, channelType: 5 },
      (status) => status.state === "completed",
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain(`short_id ${created.shortId}`);
    expect(notices[0]).toContain(`channel_id ${created.channelId}`);
    expect(notices[0]).toContain("任务 ID：");
    expect(notices[0]).toContain("状态：已完成");
    expect(notices[0]).toContain(result);
    expect(notices[0]).toContain("结果");
    expect(notices[0]).not.toContain("PRIVATE_TASK_SECRET");
  } finally {
    await closeCoreHarness(harness);
  }
});
