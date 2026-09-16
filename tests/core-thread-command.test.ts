import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes";

/**
 * `/thread <任务>` composes three pieces the suite already covers separately - a CommunityTopic
 * thread, its isolated workspace, and the durable dispatch path - so these tests follow the task
 * into the new thread instead of asserting transport calls alone.
 */

test("/thread creates an isolated thread and dispatches the query into its workspace", async () => {
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
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-create",
        ...group,
        text: "/thread fix the login bug",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);

    const thread = { channelId: `${group.channelId}____thread-1`, channelType: 5 as const };
    const workspace = join(harness.root, "worktrees", "thread-1");
    expect(harness.transport.managementCalls).toContainEqual({
      action: "create-thread",
      groupId: group.channelId,
      shortId: null,
    });
    expect(harness.worktrees?.requests).toEqual([
      { baseDir: parentCwd, label: "fix-the-login-bug", threadId: "thread-1" },
    ]);
    // The dispatched task runs in the workspace, not in the parent group's directory.
    expect(harness.workers.prompts[0]).toMatchObject({ text: "fix the login bug", cwd: workspace });
    expect((await harness.service.status(thread)).cwd).toBe(workspace);

    // The thread name comes from the query; the group gets the receipt and the thread the note.
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelId === group.channelId &&
          text.includes("已创建子区「fix the login bug」（thread-1）并派发任务；隔离工作区"),
      ),
    ).toBe(true);
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelId === thread.channelId &&
          text.startsWith("来自群会话的派发任务") &&
          text.includes("fix the login bug"),
      ),
    ).toBe(true);

    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread names the workspace from the query and falls back to a timestamp without ASCII text", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-allowed", channelType: 2 as const };
    const parentCwd = join(harness.root, "project");
    await mkdir(parentCwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-unicode-bind",
        ...group,
        text: `/new ${parentCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-unicode-create",
        ...group,
        text: "/thread 修复登录流程",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);

    const request = harness.worktrees?.requests[0];
    if (!request) throw new Error("expected a recorded workspace request");
    expect(request.label).toMatch(/^thread-\d{8}-\d{6}$/);
    // The thread name keeps the query itself even when the workspace needs a generated name.
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelId === group.channelId &&
          text.includes("已创建子区「修复登录流程」（thread-1）并派发任务；隔离工作区"),
      ),
    ).toBe(true);
    await waitForStatus(
      harness.service,
      { channelId: `${group.channelId}____thread-1`, channelType: 5 },
      (status) => status.state === "completed",
    );
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

    // Refusals and the usage hint must not create a thread or start any work.
    expect(harness.transport.managementCalls.filter(({ action }) => action === "create-thread")).toEqual([]);
    expect(harness.worktrees?.requests).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread keeps the thread and the task when the workspace cannot be created", async () => {
  const harness = await createCoreHarness();
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    worktrees.failure = new Error("workspace boom");
    const group = { channelId: "group-allowed", channelType: 2 as const };
    const parentCwd = join(harness.root, "project");
    await mkdir(parentCwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-fail-bind",
        ...group,
        text: `/new ${parentCwd}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-fail-create",
        ...group,
        text: "/thread still deliver the work",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);

    const thread = { channelId: `${group.channelId}____thread-1`, channelType: 5 as const };
    // The thread falls back to the parent directory and still runs the dispatched query.
    expect(harness.workers.prompts[0]).toMatchObject({ text: "still deliver the work", cwd: parentCwd });
    expect(
      harness.transport.sent.some(
        ({ text }) =>
          text.includes(
            "已创建子区「still deliver the work」（thread-1）并派发任务；隔离工作区创建失败：workspace boom",
          ),
      ),
    ).toBe(true);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/thread works while the current chat has a running task", async () => {
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

    // Pulling work aside is most useful exactly when the chat is busy.
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-busy-run",
        ...group,
        text: "/thread move this aside",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.prompts[1]).toMatchObject({
      text: "move this aside",
      cwd: join(harness.root, "worktrees", "thread-1"),
    });
  } finally {
    release?.();
    await closeCoreHarness(harness);
  }
});
