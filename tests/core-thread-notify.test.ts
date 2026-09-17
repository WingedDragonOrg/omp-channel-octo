import { expect, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, PromptContext } from "../src/contracts";
import { ChannelStore } from "../src/store";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
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

test("a notify dispatch relays its result to the dispatching chat", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "回传线程" });
        const relay = await dispatchViaTool(promptContext, {
          short_id: thread.short_id,
          text: "子区任务",
          notify: true,
        });
        if (relay.notify !== "registered" || relay.delivery !== "queued") {
          throw new Error(`unexpected dispatch result: ${JSON.stringify(relay)}`);
        }
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "子区任务") {
        emit(assistantText("relay-result", "部署检查通过", true));
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
    await harness.service.handleInbound(
      inbound({
        messageId: "relay-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await noticeSeen.promise;

    expect(notices).toHaveLength(1);
    const notice = notices[0] ?? "";
    expect(notice).toContain("[子区任务回传] 「回传线程」");
    expect(notice).toContain(`short_id ${created.shortId}`);
    expect(notice).toContain(`channel_id ${created.channelId}`);
    expect(notice).toContain("状态：已完成");
    expect(notice).toContain("任务：子区任务");
    expect(notice).toContain("结果：部署检查通过");

    // The notice is one ordinary task in the dispatching chat, keyed to the thread task it reports.
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const threadTask = store.getLatestTask(harness.bot.uid, {
        channelId: created.channelId,
        channelType: 5,
      });
      const groupTask = store.getLatestTask(harness.bot.uid, GROUP);
      expect(threadTask?.state).toBe("completed");
      expect(threadTask?.relayChannelType).toBe(2);
      expect(threadTask?.relayChannelId).toBe(GROUP.channelId);
      expect(threadTask?.relayThreadName).toBe("回传线程");
      expect(groupTask?.messageId).toBe(`notify:${threadTask?.id}`);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a dispatch without notify produces no relay and no extra turn", async () => {
  const unexpected: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "普通线程" });
        const plain = await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务" });
        if ("notify" in plain) throw new Error("relay reported for a plain dispatch");
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
    await harness.service.handleInbound(
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
      expect(
        store.getLatestTask(harness.bot.uid, { channelId: created.channelId, channelType: 5 })
          ?.relayChannelId,
      ).toBeNull();
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.text).toBe("建区并派发");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a thread task that was never dispatched produces no relay", async () => {
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
    await harness.service.handleInbound(
      inbound({
        messageId: "direct-create",
        ...GROUP,
        text: "建一个普通线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.handleInbound(
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
          ?.relayChannelId,
      ).toBeNull();
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.text).toBe("建一个普通线程");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed dispatch relays a bounded failure line", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "失败线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "会失败的任务", notify: true });
        return;
      }
      if (text === "会失败的任务") throw new Error("fixture worker failed");
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.handleInbound(
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
    expect(notices[0]).toContain("失败原因：worker failed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("cancelled dispatches relay their cancellation, queued ones included", async () => {
  const notices: string[] = [];
  const bothSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建区") {
        const thread = await createThreadViaTool(promptContext, { name: "取消线程" });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        return;
      }
      if (text === "派发两条") {
        await dispatchViaTool(promptContext, { short_id: created.shortId, text: "慢任务", notify: true });
        await dispatchViaTool(promptContext, { short_id: created.shortId, text: "排队任务", notify: true });
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
    await harness.service.handleInbound(
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
    await harness.service.handleInbound(
      inbound({ messageId: "cancel-mode", ...thread, text: "/mode queue", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "cancel-dispatch", ...GROUP, text: "派发两条", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(3);
    await harness.service.handleInbound(
      inbound({ messageId: "cancel-command", ...thread, text: "/cancel", mentionedUserIds: [harness.bot.uid] }),
    );
    await bothSeen.promise;

    expect(notices).toHaveLength(2);
    for (const notice of notices) {
      expect(notice).toContain("状态：已取消");
      expect(notice).toContain("说明：任务已取消（不回滚已发生的副作用）。");
    }
    expect(notices.some((notice) => notice.includes("任务：慢任务"))).toBe(true);
    expect(notices.some((notice) => notice.includes("任务：排队任务"))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an interrupted dispatch relays the interruption reason", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "中断线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "中断任务", notify: true });
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
    await harness.service.handleInbound(
      inbound({
        messageId: "interrupt-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);

    const thread = { channelId: created.channelId, channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "interrupt-command", ...thread, text: "/interrupt", mentionedUserIds: [harness.bot.uid] }),
    );
    await noticeSeen.promise;
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("状态：已中断");
    expect(notices[0]).toContain("说明：用户中断当前回合");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a relay joins the dispatching chat's running turn in steer mode instead of starting a second task", async () => {
  const appended: string[] = [];
  const appendedSeen = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text, waitForAppend }) => {
      if (text === "建区并派发并等待") {
        // The workspace keeps the thread off this turn's directory, so both can run at once.
        const thread = await createThreadViaTool(promptContext, {
          name: "并入线程",
          worktree_name: "relay-workspace",
        });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务", notify: true });
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
    await harness.service.handleInbound(
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
    expect(appended[0]).toContain("结果：并入结果");

    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expect(harness.workers.prompts.some(({ text }) => text.startsWith("[子区任务回传]"))).toBe(false);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a relay is never taken as the answer to an open question", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "问答线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "子区任务", notify: true });
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
    await harness.service.handleInbound(
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
        const thread = await createThreadViaTool(promptContext, { name: "停机线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "长任务", notify: true });
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
    await harness.service.handleInbound(
      inbound({
        messageId: "stop-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);

    // A graceful stop cancels the in-flight task; that lifecycle outcome is never relayed.
    await harness.service.stop();
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        store.getLatestTask(harness.bot.uid, { channelId: created.channelId, channelType: 5 })?.state,
      ).toBe("cancelled");
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId.startsWith("notify:")).toBe(false);
    } finally {
      store.close();
    }
    expect(notices).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("relays survive a restart, never repeat, and restart interruptions never relay", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-relay-restart-"));
  const firstNotices: string[] = [];
  const firstSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const first = await createCoreHarness({
    root,
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建区并派发") {
        const thread = await createThreadViaTool(promptContext, { name: "回传线程" });
        created.shortId = thread.short_id;
        created.channelId = thread.channel_id;
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "第一件事", notify: true });
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
    await first.service.handleInbound(
      inbound({
        messageId: "restart-create",
        ...GROUP,
        text: "建区并派发",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await firstSeen.promise;
    expect(firstNotices).toHaveLength(1);
    expect(firstNotices[0]).toContain("第一件结果");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  // A dispatch accepted before the restart still carries its relay, but the restart interrupts it.
  const store = await ChannelStore.open(join(first.stateDir, "channel.sqlite"));
  try {
    const persisted = store.getLatestTask(first.bot.uid, {
      channelId: created.channelId,
      channelType: 5,
    });
    expect(persisted?.relayChannelId).toBe(GROUP.channelId);
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
          relayChannelType: 2,
          relayChannelId: GROUP.channelId,
          relayThreadName: "回传线程",
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
        const thread = await createThreadViaTool(promptContext, { name: "回传线程" });
        await dispatchViaTool(promptContext, { short_id: thread.short_id, text: "第二件事", notify: true });
        return;
      }
      if (text === "第二件事") {
        emit(assistantText("second-result", "第二件结果", true));
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        secondNotices.push(text);
        secondSeen.resolve();
        return;
      }
      throw new Error(`unexpected second-session prompt: ${text}`);
    },
  });
  try {
    await second.service.handleInbound(
      inbound({
        messageId: "restart-second",
        ...GROUP,
        text: "再派发一次",
        mentionedUserIds: [second.bot.uid],
      }),
    );
    await secondSeen.promise;

    expect(secondNotices).toHaveLength(1);
    expect(secondNotices[0]).toContain("第二件结果");
    expect(secondNotices[0]).not.toContain("第一件结果");
    expect(secondNotices[0]).not.toContain("重启前的排队任务");

    const reopened = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
    try {
      expect(reopened.getTask("queued-relay-task")?.state).toBe("interrupted");
    } finally {
      reopened.close();
    }
  } finally {
    await closeCoreHarness(second);
  }
});
