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

/** Runs `create-thread` through the management tool the prompt actually received. */
async function createThreadViaTool(
  promptContext: PromptContext | undefined,
  args: Record<string, unknown>,
): Promise<Record<string, unknown> & { short_id: string; channel_id: string }> {
  const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
  if (!management) throw new Error("octo_management host tool missing");
  return JSON.parse(
    await management.execute(
      { action: "create-thread", ...args },
      new AbortController().signal,
    ),
  ) as Record<string, unknown> & { short_id: string; channel_id: string };
}

test("a notify thread relays a finished task's result to its creator chat", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { shortId: "", channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建一个回传线程") {
        const result = await createThreadViaTool(promptContext, { name: "回传线程", notify: true });
        if (result.notify !== true) throw new Error(`notify flag missing: ${JSON.stringify(result)}`);
        created.shortId = result.short_id;
        created.channelId = result.channel_id;
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
        text: "建一个回传线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    expect(created.shortId).not.toBe("");

    await harness.service.handleInbound(
      inbound({
        messageId: "relay-task",
        channelId: created.channelId,
        channelType: 5,
        text: "子区任务",
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

    // The notice is one ordinary task in the creator chat, keyed to the thread task it reports.
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const threadTask = store.getLatestTask(harness.bot.uid, {
        channelId: created.channelId,
        channelType: 5,
      });
      const groupTask = store.getLatestTask(harness.bot.uid, GROUP);
      expect(threadTask?.state).toBe("completed");
      expect(groupTask?.messageId).toBe(`notify:${threadTask?.id}`);
      expect(
        harness.transport.sent.some(
          (message) =>
            message.target.channelId === GROUP.channelId
            && message.text.includes("开始执行排队消息"),
        ),
      ).toBe(false);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("without notify a finished thread task produces no relay and no extra turn", async () => {
  const unexpected: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建一个普通线程") {
        const result = await createThreadViaTool(promptContext, { name: "普通线程" });
        if ("notify" in result) throw new Error("relay reported for a plain create");
        created.channelId = result.channel_id;
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
        text: "建一个普通线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({
        messageId: "plain-task",
        channelId: created.channelId,
        channelType: 5,
        text: "子区任务",
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
        store.getThreadNotify(harness.bot.uid, { channelId: created.channelId, channelType: 5 }),
      ).toBeNull();
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.text).toBe("建一个普通线程");
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed thread task relays a bounded failure line", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建一个失败线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "失败线程", notify: true })).channel_id;
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
        text: "建一个失败线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({
        messageId: "fail-task",
        channelId: created.channelId,
        channelType: 5,
        text: "会失败的任务",
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

test("cancelled thread tasks relay their cancellation, queued ones included", async () => {
  const notices: string[] = [];
  const bothSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建一个取消线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "取消线程", notify: true })).channel_id;
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
        text: "建一个取消线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    const thread = { channelId: created.channelId, channelType: 5 as const };

    await harness.service.handleInbound(
      inbound({ messageId: "cancel-running", ...thread, text: "慢任务", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);
    await harness.service.handleInbound(
      inbound({
        messageId: "cancel-queued",
        ...thread,
        text: "/queue 排队任务",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
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

test("an interrupted thread task relays the interruption reason", async () => {
  const notices: string[] = [];
  const noticeSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建一个中断线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "中断线程", notify: true })).channel_id;
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
        text: "建一个中断线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    const thread = { channelId: created.channelId, channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "interrupt-task", ...thread, text: "中断任务", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);
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

test("a relay joins the creator's running turn in steer mode instead of starting a second task", async () => {
  const appended: string[] = [];
  const appendedSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ emit, promptContext, text, waitForAppend }) => {
      if (text === "建区并等待") {
        const result = await createThreadViaTool(promptContext, {
          name: "并入线程",
          notify: true,
          worktree_name: "relay-workspace",
        });
        created.channelId = result.channel_id;
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
        text: "建区并等待",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    for (let attempt = 0; attempt < 10000 && created.channelId === ""; attempt += 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    expect(created.channelId).not.toBe("");

    await harness.service.handleInbound(
      inbound({
        messageId: "steer-task",
        channelId: created.channelId,
        channelType: 5,
        text: "子区任务",
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
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text === "建一个问答线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "问答线程", notify: true })).channel_id;
        return;
      }
      if (text === "子区任务") {
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
        text: "建一个问答线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

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

    await harness.service.handleInbound(
      inbound({
        messageId: "question-task",
        channelId: created.channelId,
        channelType: 5,
        text: "子区任务",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
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
    await closeCoreHarness(harness);
  }
});

test("a graceful stop never relays its in-flight thread tasks", async () => {
  const notices: string[] = [];
  const created = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text, waitForAbort }) => {
      if (text === "建一个停机线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "停机线程", notify: true })).channel_id;
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
        text: "建一个停机线程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");
    const thread = { channelId: created.channelId, channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "stop-task", ...thread, text: "长任务", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);

    // A graceful stop cancels the in-flight task; that lifecycle outcome is never relayed.
    await harness.service.stop();
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(store.getLatestTask(harness.bot.uid, thread)?.state).toBe("cancelled");
      expect(store.getLatestTask(harness.bot.uid, GROUP)?.messageId.startsWith("notify:")).toBe(false);
    } finally {
      store.close();
    }
    expect(notices).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the relay survives a restart and never repeats an already-notified task", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-relay-restart-"));
  const firstNotices: string[] = [];
  const firstSeen = Promise.withResolvers<void>();
  const created = { channelId: "" };
  const first = await createCoreHarness({
    root,
    prompt: async ({ emit, promptContext, text }) => {
      if (text === "建一个回传线程") {
        created.channelId = (await createThreadViaTool(promptContext, { name: "回传线程", notify: true })).channel_id;
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
        text: "建一个回传线程",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await waitForStatus(first.service, GROUP, (status) => status.state === "completed");
    await first.service.handleInbound(
      inbound({
        messageId: "restart-first-task",
        channelId: created.channelId,
        channelType: 5,
        text: "第一件事",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await firstSeen.promise;
    expect(firstNotices).toHaveLength(1);
    expect(firstNotices[0]).toContain("第一件结果");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const secondNotices: string[] = [];
  const secondSeen = Promise.withResolvers<void>();
  const second = await createCoreHarness({
    root,
    prompt: async ({ emit, text }) => {
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
    const store = await ChannelStore.open(join(second.stateDir, "channel.sqlite"));
    try {
      const relay = store.getThreadNotify(second.bot.uid, {
        channelId: created.channelId,
        channelType: 5,
      });
      expect(relay?.creatorChannelId).toBe(GROUP.channelId);
      expect(relay?.creatorChannelType).toBe(2);
    } finally {
      store.close();
    }

    await second.service.handleInbound(
      inbound({
        messageId: "restart-second-task",
        channelId: created.channelId,
        channelType: 5,
        text: "第二件事",
        mentionedUserIds: [second.bot.uid],
      }),
    );
    await secondSeen.promise;
    expect(secondNotices).toHaveLength(1);
    expect(secondNotices[0]).toContain("第二件结果");
    expect(secondNotices[0]).not.toContain("第一件结果");
    expect(second.workers.prompts.some(({ text }) => text.includes("第一件结果"))).toBe(false);
  } finally {
    await closeCoreHarness(second);
  }
});
