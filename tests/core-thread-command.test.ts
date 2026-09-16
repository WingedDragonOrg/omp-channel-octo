import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes";

/**
 * `/thread <任务>` hands the intent to the assistant: the command itself creates nothing, it turns
 * the message into a task carrying the hand-over instruction, so the agent - not a fixed naming
 * rule - decides the sub-thread name and its `worktree_name` and drives octo_management.
 */

test("/thread turns the message into an instructed task without creating anything itself", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-allowed", channelType: 2 as const };
    const parentCwd = join(harness.root, "project");
    await mkdir(parentCwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-bind",
        ...group,
        text: `/new ${parentCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const intent = "把进度卡片的样式调整得更紧凑，先看一遍现有渲染再改";
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-intent",
        ...group,
        text: `/thread ${intent}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);

    // Nothing is created up front: no thread, no workspace, no receipt.
    expect(harness.transport.managementCalls).toEqual([]);
    expect(harness.worktrees?.requests).toEqual([]);
    expect(harness.transport.sent.some(({ text }) => text.includes("子区"))).toBe(false);

    // The task carries the raw intent plus how to hand it over, and runs in the group's directory.
    const prompt = harness.workers.prompts[0];
    expect(prompt?.cwd).toBe(parentCwd);
    expect(prompt?.text.endsWith(intent)).toBe(true);
    for (const step of ["create-thread", "worktree_name", "thread-dispatch", "ask", "需求明确", "需求不明确"]) {
      expect(prompt?.text).toContain(step);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the instructed task sets the sub-thread up with the agent's own names", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (!text.includes("原始意图")) return; // only the hand-over task drives octo_management
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      const created = JSON.parse(
        await management.execute(
          { action: "create-thread", name: "进度卡样式", worktree_name: "progress-card-style" },
          signal,
        ),
      ) as { short_id: string };
      await management.execute(
        { action: "thread-dispatch", short_id: created.short_id, text: "调整进度卡片的样式" },
        signal,
      );
    },
  });
  try {
    const group = { channelId: "group-allowed", channelType: 2 as const };
    const parentCwd = join(harness.root, "project");
    await mkdir(parentCwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-agent-bind",
        ...group,
        text: `/new ${parentCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-agent-run",
        ...group,
        text: "/thread 调整进度卡片的样式",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const thread = { channelId: `${group.channelId}____thread-1`, channelType: 5 as const };
    const workspace = join(harness.root, "worktrees", "thread-1");
    await harness.workers.waitForPromptCount(2);

    // The workspace request carries the agent-chosen name against the group's own directory.
    expect(harness.worktrees?.requests).toEqual([
      { baseDir: parentCwd, label: "progress-card-style", threadId: "thread-1" },
    ]);
    // The dispatched work runs in that workspace, the thread shows the handoff, and the group
    // hears where the work went.
    expect(harness.workers.prompts[1]?.cwd).toBe(workspace);
    expect(harness.workers.prompts[1]?.text).toBe("调整进度卡片的样式");
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelId === thread.channelId && text.startsWith("来自群会话的派发任务"),
      ),
    ).toBe(true);
    expect((await harness.service.status(thread)).cwd).toBe(workspace);
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelId === group.channelId && text.includes("已派发任务到线程 thread-1"),
      ),
    ).toBe(true);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread without a task replies with usage, and DMs and threads are refused", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-allowed", channelType: 2 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-usage",
        ...group,
        text: "/thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    expect(harness.transport.sent.some(({ text }) => text.includes("用法：/thread <任务>"))).toBe(true);

    await harness.service.handleInbound(inbound({ messageId: "thread-dm", text: "/thread do it" }));
    expect(harness.transport.sent.some(({ text }) => text === "/thread 需要在群聊中使用。")).toBe(true);

    await harness.service.handleInbound(
      inbound({
        messageId: "thread-nested",
        channelId: `${group.channelId}____topic-9`,
        channelType: 5,
        text: "/thread nested work",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    expect(harness.transport.sent.some(({ text }) => text === "当前会话已是子区，不能再建子区。")).toBe(true);

    // Refusals and the usage hint never accept a task or touch the management surface.
    expect(harness.transport.managementCalls).toEqual([]);
    expect(harness.worktrees?.requests).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread queues the hand-over behind a running turn instead of steering it", async () => {
  let release: (() => void) | undefined;
  let promptCount = 0;
  const harness = await createCoreHarness({
    prompt: async ({ onStop }) => {
      promptCount += 1;
      if (promptCount > 1) return;
      await new Promise<void>((resolve) => {
        release = resolve;
        onStop(resolve);
      });
    },
  });
  try {
    const group = { channelId: "group-allowed", channelType: 2 as const };
    const parentCwd = join(harness.root, "project");
    await mkdir(parentCwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-busy-bind",
        ...group,
        text: `/new ${parentCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-busy-hold",
        ...group,
        text: "hold the group",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-busy-run",
        ...group,
        text: "/thread 把这件事放到子区里慢慢做",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    release?.();
    await harness.workers.waitForPromptCount(2);
    // The hand-over waits its turn as its own task in the group's directory.
    expect(harness.workers.prompts[1]?.cwd).toBe(parentCwd);
    expect(harness.workers.prompts[1]?.text).toContain("把这件事放到子区里慢慢做");
  } finally {
    release?.();
    await closeCoreHarness(harness);
  }
});
