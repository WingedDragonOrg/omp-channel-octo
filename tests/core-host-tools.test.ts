import { expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

const GROUP_TOOLS = { channelId: "group-tools", channelType: 2 as const };
const THREAD_CHANNEL_ID = "group-tools____topic-7";
const THREAD_TOOLS = { channelId: THREAD_CHANNEL_ID, channelType: 5 as const };

test("group dispatch freezes scoped management/file tools to the current target and cwd", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, cwd, promptContext }) => {
      const tools = promptContext?.tools ?? [];
      const names = tools.map(({ name }) => name).sort();
      if (names.join(",") !== "ask,octo_management,octo_send_file") {
        throw new Error(`unexpected host tools: ${names.join(",")}`);
      }
      const management = tools.find(({ name }) => name === "octo_management");
      const fileTool = tools.find(({ name }) => name === "octo_send_file");
      if (!management || !fileTool) {
        throw new Error("required host tool missing");
      }
      const signal = new AbortController().signal;
      const before = await management.execute({ action: "description-read" }, signal);
      const written = await management.execute(
        { action: "description-write", content: "updated by worker" },
        signal,
      );
      const info = JSON.parse(await management.execute({ action: "group-info" }, signal)) as {
        group_id: string;
        name: string;
      };
      const created = JSON.parse(
        await management.execute({ action: "create-thread", name: "thread from worker" }, signal),
      ) as { short_id: string; channel_id: string };
      const fileResult = await fileTool.execute({ path: "picture.png", kind: "image" }, signal);
      await append(
        "tool-effects.log",
        `${before}|${written}|${info.group_id}|${info.name}|${created.short_id}|${created.channel_id}|${fileResult}|${cwd}\n`,
      );
    },
  });
  try {
    const cwd = join(harness.root, "tool cwd");
    await mkdir(cwd, { recursive: true });
    await writeFile(join(cwd, "picture.png"), "fixture image bytes", "utf8");
    harness.transport.setGroupContext(GROUP_TOOLS, {
      content: "initial group context",
      version: 4,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "group-admin",
    });
    harness.transport.setGroupInfo("group-tools", {
      groupId: "group-tools",
      name: "Fixture Group Tools",
      notice: "",
      creator: "owner-1",
      status: 1,
      createdAt: "2026-09-12T00:00:00.000Z",
    });
    await harness.service.handleInbound(
      inbound({ messageId: "tools-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "tools-work",
        ...GROUP_TOOLS,
        text: "use the group tools",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const updated = await harness.transport.getGroupContext(GROUP_TOOLS);
    expect(updated?.content).toBe("updated by worker");
    expect(updated?.version).toBe(5);
    expect(harness.transport.sentFiles).toHaveLength(1);
    expect(harness.transport.sentFiles[0]).toMatchObject({
      target: GROUP_TOOLS,
      kind: "image",
      clientMsgNo: expect.any(String),
    });
    expect(harness.transport.sentFiles[0]?.path).toContain(join(harness.stateDir, "spool"));
    expect(harness.transport.sentFiles[0]?.path).toMatch(/picture\.png$/u);
    expect(harness.transport.managementCalls).toEqual([
      { action: "group-info", groupId: "group-tools", shortId: null },
      { action: "create-thread", groupId: "group-tools", shortId: null },
    ]);
    const effects = await readText(join(cwd, "tool-effects.log"));
    expect(effects).toContain("initial group context");
    expect(effects).toContain("|group-tools|Fixture Group Tools|thread-1|group-tools____thread-1|");
    expect(effects).toContain(cwd);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("private prompts receive ask and file tools but no group tools", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const names = (promptContext?.tools ?? []).map(({ name }) => name).sort();
      if (names.join(",") !== "ask,octo_send_file") {
        throw new Error(`unexpected private host tools: ${names.join(",")}`);
      }
      await append("private-tools.log", text);
    },
  });
  try {
    const target = { channelId: "dm-no-tools", channelType: 1 as const };
    harness.transport.setGroupContext(target, {
      content: "private must not read this",
      version: 1,
      updatedAt: null,
      updatedBy: "group-admin",
    });
    await harness.service.handleInbound(
      inbound({ messageId: "private-no-tools", ...target, text: "private prompt" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(harness.transport.groupContextCalls).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "private-tools.log"))).toBe(
      "private prompt",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread dispatch resolves management actions to the parent group and current thread", async () => {
  const harness = await createCoreHarness({
    allowedGroupIds: [THREAD_CHANNEL_ID],
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing in a thread chat");
      const signal = new AbortController().signal;
      const self = JSON.parse(await management.execute({ action: "thread-info" }, signal)) as {
        short_id: string;
        channel_id: string;
        name: string;
      };
      const listed = JSON.parse(await management.execute({ action: "list-threads" }, signal)) as {
        count: number;
      };
      const deleted = await management.execute({ action: "thread-delete", short_id: "topic-7" }, signal);
      await append(
        "thread-tools.log",
        `${self.short_id}|${self.channel_id}|${self.name}|${listed.count}|${deleted}\n`,
      );
    },
  });
  try {
    const cwd = join(harness.root, "thread cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setThreads("group-tools", [
      {
        shortId: "topic-7",
        groupId: "group-tools",
        channelId: THREAD_CHANNEL_ID,
        name: "Topic seven",
        creatorUid: "owner-1",
        status: 1,
        memberCount: 1,
        messageCount: 0,
        createdAt: "2026-09-12T00:00:00.000Z",
        updatedAt: "2026-09-12T00:00:00.000Z",
      },
    ]);
    await harness.service.handleInbound(
      inbound({ messageId: "thread-bind", ...THREAD_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "thread-work",
        ...THREAD_TOOLS,
        text: "inspect this thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, THREAD_TOOLS, (status) => status.state === "completed");

    expect(harness.transport.managementCalls).toEqual([
      { action: "thread-info", groupId: "group-tools", shortId: "topic-7" },
      { action: "list-threads", groupId: "group-tools", shortId: null },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-7" },
    ]);
    const effects = await readText(join(cwd, "thread-tools.log"));
    expect(effects).toContain(`topic-7|${THREAD_CHANNEL_ID}|Topic seven|1|`);
    expect(effects).toContain("已删除线程 topic-7");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("management actions reject bad arguments, bound listings, and surface transport failures", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      const rejections: string[] = [];
      for (const args of [
        { action: "no-such-action" },
        { action: "group-update" },
        { action: "create-thread" },
        { action: "thread-info" },
        { action: "description-write" },
      ]) {
        try {
          await management.execute(args, signal);
          rejections.push("resolved");
        } catch (error) {
          rejections.push((error as Error).message);
        }
      }
      const members = JSON.parse(await management.execute({ action: "group-members" }, signal)) as {
        members: unknown[];
        count: number;
        truncated: boolean;
      };
      const renamed = await management.execute(
        { action: "group-update", name: "renamed by worker" },
        signal,
      );
      harness.transport.failManagementWith(new Error("fixture management boom"));
      const failure = await management
        .execute({ action: "group-info" }, signal)
        .then(() => "resolved")
        .catch((error: Error) => error.message);
      await append(
        "management.log",
        `${rejections.join("|")}\n${members.count}/${members.members.length}/${members.truncated}\n${renamed}\n${failure}\n`,
      );
    },
  });
  try {
    const cwd = join(harness.root, "management cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setGroupMembers(
      "group-tools",
      Array.from({ length: 201 }, (_value, index) => ({
        uid: `member-${index}`,
        name: `Member ${index}`,
        role: 0,
        robot: false,
        joinedAt: "2026-09-12T00:00:00.000Z",
      })),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "management-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "management-work",
        ...GROUP_TOOLS,
        text: "exercise management errors",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const log = await readText(join(cwd, "management.log"));
    expect(log).toContain("unknown action: no-such-action");
    expect(log).toContain("name or notice is required");
    expect(log).toContain("name is required");
    expect(log).toContain("short_id is required when the chat itself is not a thread");
    expect(log).toContain("content is required");
    expect(log).toContain("201/200/true");
    expect(log).toContain("群信息已更新：name");
    expect(log).toContain("fixture management boom");
    harness.transport.failManagementWith(undefined);
    await expect(harness.transport.getGroupInfo("group-tools")).resolves.toMatchObject({
      name: "renamed by worker",
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread dispatch queues the task in the target thread and reports it in both chats", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, cwd, promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      const actions = (
        (management.parameters["properties"] as { action: { enum: string[] } }).action.enum
      );
      if (text === "handoff work") {
        const created = JSON.parse(
          await management.execute({ action: "create-thread", name: "handoff" }, signal),
        ) as { short_id: string };
        const dispatched = JSON.parse(
          await management.execute(
            { action: "thread-dispatch", short_id: created.short_id, text: "检查部署状态" },
            signal,
          ),
        ) as { dispatched: boolean; short_id: string; channel_id: string };
        await append(
          "dispatch.log",
          `group|${actions.includes("thread-dispatch")}|${dispatched.dispatched}|${dispatched.short_id}|${dispatched.channel_id}|${cwd}\n`,
        );
        return;
      }
      await append(
        "dispatch.log",
        `thread|${actions.includes("thread-dispatch")}|${text}|${cwd}\n`,
      );
    },
  });
  try {
    const cwd = join(harness.root, "dispatch cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "dispatch-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "dispatch-work",
        ...GROUP_TOOLS,
        text: "handoff work",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    await harness.workers.waitForPromptCount(2);
    // The fixture mints `thread-1` for the first created thread; the thread chat id is composite.
    const dispatchedChannelId = "group-tools____thread-1";
    await waitForStatus(
      harness.service,
      { channelId: dispatchedChannelId, channelType: 5 },
      (status) => status.state === "completed",
    );

    const lines = (await readText(join(cwd, "dispatch.log"))).trim().split("\n");
    expect(lines[0]).toBe(`group|true|true|thread-1|${dispatchedChannelId}|${cwd}`);
    expect(lines[1]).toBe(`thread|false|检查部署状态|${cwd}`);
    expect(harness.workers.prompts[1]?.cwd).toBe(cwd);
    const handoff = harness.transport.sent.filter(
      ({ text }) => text.startsWith("来自群会话的派发任务") || text.startsWith("已派发任务到线程"),
    );
    expect(handoff.map(({ target, text }) => `${target.channelType}:${target.channelId}|${text}`))
      .toEqual([
        `5:${dispatchedChannelId}|来自群会话的派发任务：\n\n检查部署状态`,
        `2:group-tools|已派发任务到线程 thread-1（${dispatchedChannelId}）。`,
      ]);
    expect(harness.transport.managementCalls).toEqual([
      { action: "create-thread", groupId: "group-tools", shortId: null },
      { action: "thread-info", groupId: "group-tools", shortId: "thread-1" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("threads inherit the parent group allowlist without their own entry", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ["group-allowed"] });
  try {
    const inherited = inbound({
      messageId: "thread-inherited",
      channelId: "group-allowed____topic-9",
      channelType: 5,
      mentionedUserIds: [harness.bot.uid],
      text: "inherited thread work",
    });
    const denied = inbound({
      messageId: "thread-denied",
      channelId: "group-other____topic-9",
      channelType: 5,
      mentionedUserIds: [harness.bot.uid],
      text: "denied thread work",
    });
    await harness.service.handleInbound(inherited);
    await harness.service.handleInbound(denied);
    await waitForStatus(harness.service, inherited, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["inherited thread work"]);
    expect((await harness.service.status(denied)).state).toBe("none");
  } finally {
    await closeCoreHarness(harness);
  }
});
