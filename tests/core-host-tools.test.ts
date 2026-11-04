import { expect, test, vi } from "bun:test";
import { mkdir, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ThreadSummary } from "../src/contracts";
import { WorktreeMergeExecutionError } from "../src/threads/thread-merge";
import { ChannelStore } from "../src/store";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

const GROUP_TOOLS = { channelId: "group-tools", channelType: 2 as const };
const THREAD_CHANNEL_ID = "group-tools____topic-7";
const THREAD_TOOLS = { channelId: THREAD_CHANNEL_ID, channelType: 5 as const };

/** One live thread of the fixture group, as the management listing reports it. */
function threadRecord(shortId: string): ThreadSummary {
  return {
    shortId,
    groupId: "group-tools",
    channelId: `group-tools____${shortId}`,
    name: `Topic ${shortId}`,
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-09-12T00:00:00.000Z",
    updatedAt: "2026-09-12T00:00:00.000Z",
  };
}

/** 记录归档时刻该聊天已收到的正文：回复必须先于归档送达，否则迟到的回复会把归档解档。 */
class ArchiveTracingTransport extends FakeTransport {
  readonly archiveOrder: Array<{ shortId: string; sentBefore: string[] }> = [];

  override async archiveThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    this.archiveOrder.push({ shortId, sentBefore: this.sent.map(({ text }) => text) });
    return super.archiveThread(groupId, shortId, signal);
  }
}

/** 服务端瞬时失败的两种线缆形状：纯 5xx，与内部 500 被包成 400 的 `store_failed`。 */
const TRANSIENT_ARCHIVE_FAILURES: Error[] = [
  Object.assign(new Error("Octo thread archive failed with HTTP 500: 服务器内部错误。"), { status: 500 }),
  Object.assign(new Error("Octo thread archive failed with HTTP 400: 服务器内部错误。"), {
    status: 400,
    code: "err.server.bot_api.store_failed",
  }),
  Object.assign(new Error("Octo thread archive failed with HTTP 503"), { status: 503 }),
  Object.assign(new Error("Octo thread archive failed with HTTP 500: 服务器内部错误。"), { status: 500 }),
  Object.assign(new Error("Octo thread archive failed with HTTP 500: 服务器内部错误。"), { status: 500 }),
];

/** 假定时器只接管计时：用真实 I/O 让出事件循环，fs 步骤与微任务照常推进。 */
async function yieldRealIo(root: string): Promise<void> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    await stat(root);
  }
}

test("group dispatch freezes scoped management/file tools to the current target and cwd", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, cwd, promptContext }) => {
      const tools = promptContext?.tools ?? [];
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
      { action: "group-members", groupId: "group-tools", shortId: null },
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

test("private prompts receive ask, file, and channel tools but no group tools", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const names = (promptContext?.tools ?? []).map(({ name }) => name).sort();
      if (names.join(",") !== "ask,channel_control,octo_send_file,watch") {
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

    expect(harness.transport.managementCalls).toEqual(
      expect.arrayContaining([
        { action: "thread-members", groupId: "group-tools", shortId: "topic-7" },
        { action: "thread-info", groupId: "group-tools", shortId: "topic-7" },
        { action: "list-threads", groupId: "group-tools", shortId: null },
        { action: "thread-delete", groupId: "group-tools", shortId: "topic-7" },
      ]),
    );
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
        { action: "create-thread", name: "worktree type check", worktree_name: 7 },
        { action: "create-thread", name: "worktree slug check", worktree_name: "修复登录" },
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
    expect(log).toContain("worktree_name must be a string");
    expect(log).toContain("worktree_name must contain ASCII letters or digits");
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

test("thread-delete retries a transient store failure on a fresh connection", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      harness.transport.failManagementTimes([
        Object.assign(new Error("Octo thread delete failed with HTTP 400: 服务器内部错误。"), {
          status: 400,
          code: "err.server.bot_api.store_failed",
        }),
      ]);
      const deleted = await management.execute(
        { action: "thread-delete", short_id: "topic-9" },
        new AbortController().signal,
      );
      await append("thread-delete.log", `${deleted}\n`);
    },
  });
  try {
    const cwd = join(harness.root, "delete retry cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-9")]);
    // The shape the real transport reports for the server's transient storage failure: the wire
    // status is a 400, while the body's `store_failed` code says the operation failed internally.
    await harness.service.handleInbound(
      inbound({ messageId: "delete-retry-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "delete-retry-work",
        ...GROUP_TOOLS,
        text: "delete the probe thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    expect(await readText(join(cwd, "thread-delete.log"))).toBe("已删除线程 topic-9\n");
    expect(harness.transport.managementCalls).toEqual([
      { action: "group-members", groupId: "group-tools", shortId: null },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-9" },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-9", freshConnection: true },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-delete reports one bounded line when the retry fails too", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      harness.transport.failManagementTimes([
        Object.assign(new Error("Octo thread delete failed with HTTP 503"), { status: 503 }),
        Object.assign(
          new Error(
            `Octo thread delete failed with HTTP 500:\n服务器内部错误。${"重试之后仍然失败。".repeat(60)}`,
          ),
          { status: 500 },
        ),
      ]);
      const outcome = await management
        .execute({ action: "thread-delete", short_id: "topic-9" }, new AbortController().signal)
        .then((value) => `resolved:${value}`)
        .catch((error: Error) => `rejected:${error.message}`);
      await append("thread-delete.log", `${outcome}\n`);
    },
  });
  try {
    const cwd = join(harness.root, "delete exhausted cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-9")]);
    await harness.service.handleInbound(
      inbound({ messageId: "delete-exhausted-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "delete-exhausted-work",
        ...GROUP_TOOLS,
        text: "delete the probe thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const lines = (await readText(join(cwd, "thread-delete.log"))).trimEnd().split("\n");
    expect(lines).toHaveLength(1);
    const message = lines[0]!.replace(/^rejected:/u, "");
    expect(message).toHaveLength(200);
    expect(message.startsWith("Octo thread delete failed with HTTP 500: 服务器内部错误。")).toBe(true);
    expect(harness.transport.managementCalls).toEqual([
      { action: "group-members", groupId: "group-tools", shortId: null },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-9" },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-9", freshConnection: true },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toMatchObject([
      { shortId: "topic-9" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-delete does not retry a permanent server rejection", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      harness.transport.failManagementTimes([
        Object.assign(new Error("Octo thread delete failed with HTTP 403: 无删除权限。"), { status: 403 }),
      ]);
      const outcome = await management
        .execute({ action: "thread-delete", short_id: "topic-9" }, new AbortController().signal)
        .then((value) => `resolved:${value}`)
        .catch((error: Error) => `rejected:${error.message}`);
      await append("thread-delete.log", `${outcome}\n`);
    },
  });
  try {
    const cwd = join(harness.root, "delete permanent cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-9")]);
    await harness.service.handleInbound(
      inbound({ messageId: "delete-permanent-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "delete-permanent-work",
        ...GROUP_TOOLS,
        text: "delete the probe thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    expect(await readText(join(cwd, "thread-delete.log"))).toBe(
      "rejected:Octo thread delete failed with HTTP 403: 无删除权限。\n",
    );
    expect(harness.transport.managementCalls).toEqual([
      { action: "group-members", groupId: "group-tools", shortId: null },
      { action: "thread-delete", groupId: "group-tools", shortId: "topic-9" },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toMatchObject([
      { shortId: "topic-9" },
    ]);
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
        ) as { dispatched: boolean; delivery: string; short_id: string; channel_id: string };
        await append(
          "dispatch.log",
          `group|${actions.includes("thread-dispatch")}|${dispatched.dispatched}|${dispatched.delivery}|${dispatched.short_id}|${dispatched.channel_id}|${cwd}\n`,
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
    expect(lines[0]).toBe(`group|true|true|queued|thread-1|${dispatchedChannelId}|${cwd}`);
    expect(lines[1]).toBe(`thread|false|检查部署状态|${cwd}`);
    expect(harness.workers.prompts[1]?.cwd).toBe(cwd);
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelType === 5
          && target.channelId === dispatchedChannelId
          && text.includes("检查部署状态"),
      ),
    ).toBe(true);
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelType === 2
          && target.channelId === GROUP_TOOLS.channelId
          && text.includes(dispatchedChannelId),
      ),
    ).toBe(true);
    expect(harness.transport.managementCalls).toEqual(
      expect.arrayContaining([
        { action: "group-members", groupId: "group-tools", shortId: null },
        { action: "create-thread", groupId: "group-tools", shortId: null },
        { action: "thread-info", groupId: "group-tools", shortId: "thread-1" },
        { action: "thread-members", groupId: "group-tools", shortId: "thread-1" },
      ]),
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a dispatch into a queue-mode thread lands as its own task, never an answer", async () => {
  const threadStarted = Promise.withResolvers<void>();
  const threadRelease = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ append, onStop, promptContext, text }) => {
      if (text === "hold the thread") {
        threadStarted.resolve();
        onStop(() => threadRelease.resolve());
        const ask = (promptContext?.tools ?? []).find(({ name }) => name === "ask");
        if (!ask) throw new Error("ask tool missing");
        const asked = ask.execute(
          { questions: [{ question: "先确认哪一批能力？" }] },
          new AbortController().signal,
        );
        const settled = await Promise.race([
          threadRelease.promise.then(() => "closed"),
          asked.then(() => "answered"),
        ]);
        await append("queue-mode.log", `hold:${settled}\n`);
        await threadRelease.promise;
        return;
      }
      await append("queue-mode.log", `task:${text}\n`);
    },
  });
  try {
    const groupCwd = join(harness.root, "group cwd");
    const threadCwd = join(harness.root, "thread cwd");
    await mkdir(groupCwd, { recursive: true });
    await mkdir(threadCwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
    await harness.service.handleInbound(
      inbound({ messageId: "queue-group-bind", ...GROUP_TOOLS, text: `/new ${groupCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queue-thread-bind", ...THREAD_TOOLS, text: `/new ${threadCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queue-thread-mode", ...THREAD_TOOLS, text: "/mode queue", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queue-thread-hold", ...THREAD_TOOLS, text: "hold the thread", mentionedUserIds: [harness.bot.uid] }),
    );
    await threadStarted.promise;
    await harness.workers.waitForPromptCount(1);
    expect((await harness.service.status(THREAD_TOOLS)).pendingQuestionIds).toHaveLength(1);

    await harness.service.handleInbound(
      inbound({ messageId: "queue-group-work", ...GROUP_TOOLS, text: "handoff into the busy topic", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);
    const management = (harness.workers.prompts[1]?.promptContext?.tools ?? []).find(
      ({ name }) => name === "octo_management",
    );
    if (!management) throw new Error("management tool missing");
    const dispatched = JSON.parse(
      await management.execute(
        { action: "thread-dispatch", short_id: "topic-7", text: "第二件事" },
        new AbortController().signal,
      ),
    ) as { dispatched: boolean; delivery: string; message_id: string };

    // queue mode: its own task behind the run, and the open question stays the owner's to answer.
    expect(dispatched.dispatched).toBe(true);
    expect(dispatched.delivery).toBe("queued");
    expect(dispatched.message_id.startsWith("dispatch:")).toBe(true);
    const threadStatus = await harness.service.status(THREAD_TOOLS);
    expect(threadStatus.pendingQuestionIds).toHaveLength(1);
    expect(threadStatus.queued).toBe(1);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "hold the thread",
      "handoff into the busy topic",
    ]);
    expect(await readText(join(threadCwd, "queue-mode.log"))).toBe("");
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelType === 2
          && target.channelId === GROUP_TOOLS.channelId
          && text.includes("topic-7"),
      ),
    ).toBe(true);
    expect(
      harness.transport.sent.some(
        ({ target, text }) =>
          target.channelType === 5
          && target.channelId === THREAD_TOOLS.channelId
          && text.includes("第二件事"),
      ),
    ).toBe(true);
  } finally {
    threadRelease.resolve();
    await closeCoreHarness(harness);
  }
});

test("a dispatch into a running thread joins that turn in steer mode", async () => {
  const threadStarted = Promise.withResolvers<void>();
  const threadRelease = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ append, onStop, promptContext, text, waitForAppend }) => {
      if (text === "hold the thread") {
        threadStarted.resolve();
        onStop(() => threadRelease.resolve());
        const joined = await Promise.race([
          waitForAppend().then((appended) => `steered:${appended.text}`),
          threadRelease.promise.then(() => "closed"),
        ]);
        await append("steer.log", `${joined}\n`);
        await threadRelease.promise;
        return;
      }
      await append("steer.log", `task:${text}\n`);
    },
  });
  try {
    const groupCwd = join(harness.root, "group cwd");
    const threadCwd = join(harness.root, "thread cwd");
    await mkdir(groupCwd, { recursive: true });
    await mkdir(threadCwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
    await harness.service.handleInbound(
      inbound({ messageId: "steer-group-bind", ...GROUP_TOOLS, text: `/new ${groupCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "steer-thread-bind", ...THREAD_TOOLS, text: `/new ${threadCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    const held = harness.service.handleInbound(
      inbound({ messageId: "steer-thread-hold", ...THREAD_TOOLS, text: "hold the thread", mentionedUserIds: [harness.bot.uid] }),
    );
    await threadStarted.promise;
    await harness.workers.waitForPromptCount(1);

    await harness.service.handleInbound(
      inbound({ messageId: "steer-group-work", ...GROUP_TOOLS, text: "handoff into the running topic", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);
    const management = (harness.workers.prompts[1]?.promptContext?.tools ?? []).find(
      ({ name }) => name === "octo_management",
    );
    if (!management) throw new Error("management tool missing");
    const dispatched = JSON.parse(
      await management.execute(
        { action: "thread-dispatch", short_id: "topic-7", text: "第二件事" },
        new AbortController().signal,
      ),
    ) as { delivery: string };

    // steer mode: the running turn takes it as steering, so no second task appears.
    expect(dispatched.delivery).toBe("steered");
    expect((await harness.service.status(THREAD_TOOLS)).queued).toBe(0);
    expect(
      harness.transport.sent.some(({ text }) => text === "已把任务并入线程 topic-7（group-tools____topic-7）的运行中回合。"),
    ).toBe(true);
    threadRelease.resolve();
    await held;
    await waitForStatus(harness.service, THREAD_TOOLS, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "hold the thread",
      "handoff into the running topic",
    ]);
    expect(await readText(join(threadCwd, "steer.log"))).toBe("steered:第二件事\n");
  } finally {
    threadRelease.resolve();
    await closeCoreHarness(harness);
  }
});

test("create-thread worktree_name binds the new thread to a workspace of its own", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const created = JSON.parse(
        await management.execute(
          { action: "create-thread", name: "独立线程", worktree_name: "owl-workspace" },
          new AbortController().signal,
        ),
      ) as { short_id: string; worktree?: { path: string; branch: string }; worktree_error?: string };
      await append(
        "worktree.log",
        `${created.short_id}|${created.worktree?.path ?? "none"}|${created.worktree?.branch ?? "none"}|${created.worktree_error ?? "none"}\n`,
      );
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "isolate cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "isolate-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "isolate-create",
        ...GROUP_TOOLS,
        text: "create an isolated thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const workspace = join(harness.root, "worktrees", "thread-1");
    expect(worktrees.requests).toEqual([
      { baseDir: cwd, label: "owl-workspace", threadId: "thread-1" },
    ]);
    expect(await readText(join(cwd, "worktree.log"))).toBe(
      `thread-1|${workspace}|thread-1|none\n`,
    );

    const thread = { channelId: "group-tools____thread-1", channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "isolate-thread-task",
        ...thread,
        text: "work in isolation",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    expect(harness.workers.prompts[1]?.cwd).toBe(workspace);
    await expect(harness.service.status(thread)).resolves.toMatchObject({ cwd: workspace });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("create-thread worktree_name leaves failed workspace non-dispatchable", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const created = JSON.parse(
        await management.execute(
          { action: "create-thread", name: "no workspace", worktree_name: "broken-workspace" },
          new AbortController().signal,
        ),
      ) as { short_id: string; worktree?: { path: string }; worktree_error?: string };
      await append(
        "worktree.log",
        `${created.short_id}|${created.worktree ? "worktree" : "none"}|${created.worktree_error ?? "none"}\n`,
      );
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    worktrees.failure = new Error("目录不是 git 仓库，无法创建隔离工作区：/fixture/non-repo");
    const cwd = join(harness.root, "reused cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "isolate-fail-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "isolate-fail-create",
        ...GROUP_TOOLS,
        text: "create without a workspace",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    expect(await readText(join(cwd, "worktree.log"))).toBe(
      "thread-1|none|目录不是 git 仓库，无法创建隔离工作区：/fixture/non-repo\n",
    );

    const thread = { channelId: "group-tools____thread-1", channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "isolate-fail-thread",
        ...thread,
        text: "still deliver work",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.transport.sent.some(({ text }) =>
      text === "当前子区工作区未就绪或资源已缺失，未接收任务。"
    )).toBe(true);
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        store.getThreadListen(harness.bot.uid, { channelId: thread.channelId, channelType: 5 }),
      ).toMatchObject({
        listen: "merge",
        creatorChannelType: 2,
        creatorChannelId: GROUP_TOOLS.channelId,
        threadName: "no workspace",
      });
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-listen query and update persist the policy used by dispatched tasks", async () => {
  const relaySeen = Promise.withResolvers<void>();
  const createdChannel = { channelId: "" };
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "handoff with listen") {
        const created = JSON.parse(
          await management.execute({ action: "create-thread", name: "监听线程" }, signal),
        ) as { short_id: string; channel_id: string };
        createdChannel.channelId = created.channel_id;
        const before = JSON.parse(
          await management.execute(
            { action: "thread-listen", short_id: created.short_id },
            signal,
          ),
        ) as { listen: string };
        const updated = JSON.parse(
          await management.execute(
            { action: "thread-listen", short_id: created.short_id, listen: "task" },
            signal,
          ),
        ) as { listen: string };
        const info = JSON.parse(
          await management.execute(
            { action: "thread-info", short_id: created.short_id },
            signal,
          ),
        ) as { listen: string };
        const dispatched = JSON.parse(
          await management.execute(
            { action: "thread-dispatch", short_id: created.short_id, text: "检查部署状态" },
            signal,
          ),
        ) as { delivery: string };
        await append(
          "listen.log",
          `${before.listen}|${updated.listen}|${info.listen}|${dispatched.delivery}\n`,
        );
        return;
      }
      if (text.startsWith("[子区任务回传]")) {
        await append("listen.log", "task-notice\n");
        relaySeen.resolve();
        return;
      }
      await append("listen.log", `thread:${text}\n`);
    },
  });
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "listen-handoff",
        ...GROUP_TOOLS,
        text: "handoff with listen",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await relaySeen.promise;

    expect(await readText(join(harness.root, "tmp", "listen.log"))).toBe(
      "merge|task|task|queued\nthread:检查部署状态\ntask-notice\n",
    );
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const task = store.getLatestTask(harness.bot.uid, {
        channelId: createdChannel.channelId,
        channelType: 5,
      });
      expect(task?.relaySuppressed).toBe(false);
      expect(
        store.getThreadListen(harness.bot.uid, { channelId: createdChannel.channelId, channelType: 5 })?.listen,
      ).toBe("task");
      expect(store.getLatestTask(harness.bot.uid, GROUP_TOOLS)?.messageId).toBe(`notify:${task?.id}`);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a steered dispatch keeps using the running thread without creating a completion task", async () => {
  const threadStarted = Promise.withResolvers<void>();
  const threadRelease = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ append, onStop, promptContext, text, waitForAppend }) => {
      if (text === "hold the thread") {
        threadStarted.resolve();
        onStop(() => threadRelease.resolve());
        const joined = await Promise.race([
          waitForAppend().then((appended) => `steered:${appended.text}`),
          threadRelease.promise.then(() => "closed"),
        ]);
        await append("steer-listen.log", `${joined}\n`);
        await threadRelease.promise;
        return;
      }
      await append("steer-listen.log", `task:${text}\n`);
    },
  });
  try {
    const groupCwd = join(harness.root, "group cwd");
    const threadCwd = join(harness.root, "thread cwd");
    await mkdir(groupCwd, { recursive: true });
    await mkdir(threadCwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
    await harness.service.handleInbound(
      inbound({ messageId: "steer-listen-group-bind", ...GROUP_TOOLS, text: `/new ${groupCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "steer-listen-thread-bind", ...THREAD_TOOLS, text: `/new ${threadCwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    const held = harness.service.handleInbound(
      inbound({ messageId: "steer-listen-hold", ...THREAD_TOOLS, text: "hold the thread", mentionedUserIds: [harness.bot.uid] }),
    );
    await threadStarted.promise;
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({ messageId: "steer-listen-work", ...GROUP_TOOLS, text: "handoff into the running topic", mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.workers.waitForPromptCount(2);
    const management = (harness.workers.prompts[1]?.promptContext?.tools ?? []).find(
      ({ name }) => name === "octo_management",
    );
    if (!management) throw new Error("management tool missing");
    await management.execute(
      { action: "thread-listen", short_id: "topic-7", listen: "task" },
      new AbortController().signal,
    );
    const dispatched = JSON.parse(
      await management.execute(
        { action: "thread-dispatch", short_id: "topic-7", text: "第二件事" },
        new AbortController().signal,
      ),
    ) as { delivery: string };

    expect(dispatched.delivery).toBe("steered");
    expect((await harness.service.status(THREAD_TOOLS)).queued).toBe(0);
    expect(
      harness.transport.sent.some(
        ({ text }) =>
          text === "已把任务并入线程 topic-7（group-tools____topic-7）的运行中回合。",
      ),
    ).toBe(true);

    threadRelease.resolve();
    await held;
    await waitForStatus(harness.service, THREAD_TOOLS, (status) => status.state === "completed");
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(store.getLatestTask(harness.bot.uid, GROUP_TOOLS)?.messageId.startsWith("notify:")).toBe(false);
      expect(store.getLatestTask(harness.bot.uid, THREAD_TOOLS)?.relaySuppressed).toBe(false);
    } finally {
      store.close();
    }
    expect(await readText(join(threadCwd, "steer-listen.log"))).toBe("steered:第二件事\n");
  } finally {
    threadRelease.resolve();
    await closeCoreHarness(harness);
  }
});

test("thread-merge merges the bound workspace through the port and reports wt's result", async () => {
  const log: string[] = [];
  const mergeNoticeSeen = Promise.withResolvers<void>();
  let mergeCallCount = 0;
  const secondMergeFinished = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (text.startsWith("[子区合并回传]")) {
        log.push(`merge-notice:${text}\n`);
        mergeNoticeSeen.resolve();
        return;
      }
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "create the merge thread") {
        const created = JSON.parse(
          await management.execute(
            { action: "create-thread", name: "合并线程", worktree_name: "merge-workspace" },
            signal,
          ),
        ) as { short_id: string };
        log.push(`created:${created.short_id}\n`);
        return;
      }
      if (text === "merge with parameters") {
        const mergeOperationId = `merge-call-${++mergeCallCount}`;
        const result = await management.execute(
          { action: "thread-merge", target: "release-1", remove: false },
          signal,
          mergeOperationId,
        );
        const retried = JSON.parse(
          await management.execute(
            { action: "thread-merge", target: "release-1", remove: false },
            signal,
            mergeOperationId,
          ),
        ) as { merged: boolean; replayed?: boolean; target?: string };
        expect(retried).toMatchObject({ merged: true, replayed: true, target: "release-1" });
        management.onResultAccepted?.(mergeOperationId);
        log.push(`merged:${result}\n`);
        if (mergeCallCount === 2) {
          const replay = await management.execute(
            { action: "thread-merge", target: "release-1", remove: false },
            signal,
            mergeOperationId,
          );
          log.push(`replayed:${replay}\n`);
          secondMergeFinished.resolve();
        }
        return;
      }
      if (text === "merge with a bad stage") {
        const outcome = await management
          .execute({ action: "thread-merge", stage: "sideways" }, signal)
          .then((value) => `resolved:${value}`)
          .catch((error: Error) => `rejected:${error.message}`);
        log.push(`${outcome}\n`);
        return;
      }
      if (text === "merge in the group") {
        const outcome = await management
          .execute({ action: "thread-merge" }, signal)
          .then((value) => `resolved:${value}`)
          .catch((error: Error) => `rejected:${error.message}`);
        log.push(`${outcome}\n`);
      }
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "merge cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "merge-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-create",
        ...GROUP_TOOLS,
        text: "create the merge thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const thread = { channelId: "group-tools____thread-1", channelType: 5 as const };
    const workspace = join(harness.root, "worktrees", "thread-1");
    await waitForStatus(harness.service, thread, (status) => status.cwd === workspace);
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-run",
        ...thread,
        text: "merge with parameters",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    await mergeNoticeSeen.promise;
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-run-again",
        ...thread,
        text: "merge with parameters",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await secondMergeFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    const replayReceipt = log
      .find((line) => line.startsWith("replayed:"))
      ?.slice("replayed:".length);
    expect(JSON.parse(replayReceipt ?? "{}")).toMatchObject({
      merged: true,
      replayed: true,
      target: "release-1",
      commit: null,
      removed: false,
      archive: "not-scheduled",
    });
    expect(log.join("\n")).toContain("目标分支：release-1");
    expect(log.join("\n")).toContain("目标提交：（未记录）");

    // The workspace's own directory goes to the port; wt's defaults are left alone except for the
    // caller's parameters, and the tool result carries wt's result object back to the agent.
    expect(worktrees.merges).toEqual([
      {
        cwd: workspace,
        repo: cwd,
        branch: "thread-1",
        target: "release-1",
        remove: false,
      },
      {
        cwd: workspace,
        repo: cwd,
        branch: "thread-1",
        target: "release-1",
        remove: false,
      },
    ]);
    const mergeStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const events = mergeStore.listThreadMergeEvents(harness.bot.uid);
      expect(events).toHaveLength(2);
      expect(new Set(events.map(({ id }) => id)).size).toBe(2);
      expect(events.map(({ commit }) => commit)).toEqual([null, null]);
      expect(mergeStore.getThreadListen(harness.bot.uid, thread)?.mergePending).toBe(false);
    } finally {
      mergeStore.close();
    }
    expect(log.join("\n")).toContain(
      'merged:{"merged":true,"branch":"fixture-branch","target":"release-1","squashed":true,"rebased":true,"committed":true,"removed":false}',
    );

    // A bad parameter, and a group that tries to merge, are both rejected with one bounded line.
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-bad-stage",
        ...thread,
        text: "merge with a bad stage",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    const qualificationStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(qualificationStore.getThreadListen(harness.bot.uid, thread)?.mergePending).toBe(false);
      expect(qualificationStore.getLatestTask(harness.bot.uid, thread)?.relaySuppressed).toBe(true);
      expect(qualificationStore.listThreadMergeEvents(harness.bot.uid)).toHaveLength(2);
    } finally {
      qualificationStore.close();
    }
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-in-group",
        ...GROUP_TOOLS,
        text: "merge in the group",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(4);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    expect(log.join("\n")).toContain("rejected:stage must be all, tracked or none");
    expect(log.join("\n")).toContain("rejected:thread-merge 只能在子区中调用");
    expect(worktrees.merges).toHaveLength(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-merge reports the port's failure and refuses a thread without a workspace", async () => {
  const log: string[] = [];
  const notices: string[] = [];
  const createdShortId = { value: "" };
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text.startsWith("[子区")) {
        notices.push(text);
        return;
      }
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "create the failing merge thread") {
        const created = JSON.parse(
          await management.execute(
            { action: "create-thread", name: "失败合并", worktree_name: "merge-failure", listen: "task" },
            signal,
          ),
        ) as { short_id: string };
        createdShortId.value = created.short_id;
        log.push(`created:${created.short_id}\n`);
        return;
      }
      if (text === "dispatch conflict") {
        await management.execute(
          {
            action: "thread-dispatch",
            short_id: createdShortId.value,
            text: "merge that conflicts",
          },
          signal,
        );
        return;
      }
      if (text === "merge without a workspace") {
        const callId = "merge-tool-unbound";
        const outcome = await management
          .execute({ action: "thread-merge" }, signal, callId)
          .then((value) => `resolved:${value}`)
          .catch((error: Error) => `rejected:${error.message}`);
        management.onResultAccepted?.(callId);
        log.push(`${outcome}\n`);
        return;
      }
      if (text === "merge that conflicts") {
        const callId = "merge-tool-failure";
        const outcome = await management
          .execute({ action: "thread-merge" }, signal, callId)
          .then((value) => `resolved:${value}`)
          .catch((error: Error) => `rejected:${error.message}`);
        management.onResultAccepted?.(callId);
        log.push(`${outcome}\n`);
      }
    },
  });
  const root = harness.root;
  let firstClosed = false;
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "merge failure cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "merge-fail-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-fail-create",
        ...GROUP_TOOLS,
        text: "create the failing merge thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    // A thread that never got a workspace cannot merge.
    const unbound = { channelId: "group-tools____topic-8", channelType: 5 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-fail-unbound",
        ...unbound,
        text: "merge without a workspace",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, unbound, (status) => status.state === "completed");

    // A failing wt merge surfaces as one bounded line.
    const thread = { channelId: "group-tools____thread-1", channelType: 5 as const };
    const workspace = join(harness.root, "worktrees", "thread-1");
    await waitForStatus(harness.service, thread, (status) => status.cwd === workspace);
    worktrees.mergeFailure = new WorktreeMergeExecutionError(
      1,
      "Rebase onto main incomplete CONFLICT (content): Merge conflict in shared.txt",
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-fail-dispatch",
        ...GROUP_TOOLS,
        text: "dispatch conflict",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(4);
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");

    expect(log.join("\n")).toContain("rejected:当前子区未绑定隔离工作区，无法合并");
    const failing = log.join("\n").split("\n").find((line) => line.includes("Rebase onto main"));
    expect(failing).toBeDefined();
    expect(failing?.startsWith("rejected:合并失败")).toBe(true);
    expect((failing ?? "").length).toBeLessThanOrEqual(210);
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        store.getThreadListen(harness.bot.uid, unbound),
      ).toMatchObject({ listen: "merge", mergePending: false });
      expect(store.getLatestTask(harness.bot.uid, unbound)?.relaySuppressed).toBe(true);
      expect(notices).toEqual([]);
    } finally {
      store.close();
    }
    const eventStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(
        eventStore.getThreadListen(harness.bot.uid, thread),
      ).toMatchObject({ listen: "task", mergePending: true });
      expect(eventStore.getLatestTask(harness.bot.uid, thread)?.relaySuppressed).toBe(true);
      expect(eventStore.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    } finally {
      eventStore.close();
    }
    await closeCoreHarness(harness, { remove: false });
    firstClosed = true;
    const restarted = await createCoreHarness({
      root,
      transport: harness.transport,
      prompt: async ({ text }) => {
        if (text.startsWith("[子区")) notices.push(text);
      },
    });
    try {
      for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
      expect(notices).toEqual([]);
      const recovered = await ChannelStore.open(join(restarted.stateDir, "channel.sqlite"));
      try {
        expect(
          recovered.getThreadListen(restarted.bot.uid, thread),
        ).toMatchObject({ listen: "task", mergePending: true });
        expect(recovered.getLatestTask(restarted.bot.uid, thread)?.relaySuppressed).toBe(true);
        expect(recovered.listThreadMergeEvents(restarted.bot.uid)).toEqual([]);
      } finally {
        recovered.close();
      }
    } finally {
      await closeCoreHarness(restarted);
    }
  } finally {
    if (firstClosed) await rm(root, { recursive: true, force: true });
    else await closeCoreHarness(harness);
  }
});

test("shared direct merge refusal suppresses only its task and preserves later task notices", async () => {
  const created = { channelId: "", shortId: "" };
  const mergeAttemptStarted = Promise.withResolvers<void>();
  const allowMergeAttempt = Promise.withResolvers<void>();
  const mergeRefusalFinished = Promise.withResolvers<void>();
  const noticeSeen = Promise.withResolvers<void>();
  const notices: string[] = [];
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text.startsWith("[子区任务回传]")) {
        notices.push(text);
        noticeSeen.resolve();
        return;
      }
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "prepare shared task") {
        const thread = JSON.parse(
          await management.execute(
            { action: "create-thread", name: "共享工作区线程", listen: "task" },
            signal,
          ),
        ) as { channel_id: string; short_id: string };
        created.channelId = thread.channel_id;
        created.shortId = thread.short_id;
        const entryPath = join(harness.root, "shared-entry");
        const projectPath = join(entryPath, "project-one");
        await mkdir(projectPath, { recursive: true });
        const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
        try {
          store.saveThreadWorkspace({
            botUid: harness.bot.uid,
            channelType: 5,
            channelId: thread.channel_id,
            layout: "shared",
            status: "ready",
            entryPath,
            projects: [{
              name: "项目一",
              repo: harness.root,
              path: projectPath,
              branch: "main",
              created: true,
            }],
            error: null,
          });
          store.ensureBinding({
            botUid: harness.bot.uid,
            channelType: 5,
            channelId: thread.channel_id,
            sessionId: `shared-${thread.short_id}`,
            cwd: entryPath,
            sessionFile: null,
            cwdPinned: true,
          });
        } finally {
          store.close();
        }
        await management.execute(
          {
            action: "thread-dispatch",
            short_id: thread.short_id,
            text: "attempt unsupported shared merge",
          },
          signal,
        );
        return;
      }
      if (text === "attempt unsupported shared merge") {
        mergeAttemptStarted.resolve();
        await allowMergeAttempt.promise;
        await management
          .execute({ action: "thread-merge" }, signal)
          .then(
            () => "resolved",
            (error: Error) => `rejected:${error.message}`,
          );
        mergeRefusalFinished.resolve();
        return;
      }
      if (text === "dispatch after shared refusal") {
        await management.execute(
          {
            action: "thread-dispatch",
            short_id: created.shortId,
            text: "regular after shared refusal",
          },
          signal,
        );
        return;
      }
      if (text === "regular after shared refusal") return;
      throw new Error(`unexpected prompt: ${text}`);
    },
  });
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "shared-task-prepare",
        ...GROUP_TOOLS,
        text: "prepare shared task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    await mergeAttemptStarted.promise;
    const thread = { channelId: created.channelId, channelType: 5 as const };
    allowMergeAttempt.resolve();
    await mergeRefusalFinished.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    for (let attempt = 0; attempt < 50; attempt += 1) await new Promise<void>((resolve) => setImmediate(resolve));
    expect(notices).toEqual([]);

    const afterRefusal = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(afterRefusal.getThreadListen(harness.bot.uid, thread)).toMatchObject({
        listen: "task",
        mergePending: false,
      });
      expect(afterRefusal.getLatestTask(harness.bot.uid, thread)?.relaySuppressed).toBe(true);
    } finally {
      afterRefusal.close();
    }

    await harness.service.handleInbound(
      inbound({
        messageId: "shared-task-followup",
        ...GROUP_TOOLS,
        text: "dispatch after shared refusal",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    await noticeSeen.promise;
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain(thread.channelId);
    const finalStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      expect(finalStore.getLatestTask(harness.bot.uid, GROUP_TOOLS)?.messageId).toBe(
        `notify:${finalStore.getLatestTask(harness.bot.uid, thread)?.id}`,
      );
    } finally {
      finalStore.close();
    }
  } finally {
    allowMergeAttempt.resolve();
    await closeCoreHarness(harness);
  }
});

test("thread-delete removes each deleted thread's bound workspace", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "create two workspace threads") {
        for (const name of ["清理一", "清理二"]) {
          await management.execute(
            { action: "create-thread", name, worktree_name: `cleanup-${name === "清理一" ? "one" : "two"}` },
            signal,
          );
        }
        return;
      }
      if (text === "delete the first thread") {
        await management.execute({ action: "thread-delete", short_id: "thread-1" }, signal);
        return;
      }
      if (text === "delete the missing-workspace thread") {
        await management.execute({ action: "thread-delete", short_id: "thread-2" }, signal);
      }
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "cleanup cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "cleanup-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "cleanup-create",
        ...GROUP_TOOLS,
        text: "create two workspace threads",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    // Persisted repository and branch ownership is passed to the worktree removal port.
    await harness.service.handleInbound(
      inbound({
        messageId: "cleanup-delete-one",
        ...GROUP_TOOLS,
        text: "delete the first thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    expect(worktrees.removals).toEqual([
      { path: join(harness.root, "worktrees", "thread-1"), force: false, repo: cwd, branch: "thread-1" },
    ]);

    // Deleting a thread whose workspace is already gone remains successful.
    worktrees.removal = { kind: "missing" };
    await harness.service.handleInbound(
      inbound({
        messageId: "cleanup-delete-two",
        ...GROUP_TOOLS,
        text: "delete the missing-workspace thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    expect(worktrees.removals).toEqual([
      { path: join(harness.root, "worktrees", "thread-1"), force: false, repo: cwd, branch: "thread-1" },
      { path: join(harness.root, "worktrees", "thread-2"), force: false, repo: cwd, branch: "thread-2" },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toEqual([]);

  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-delete keeps a dirty workspace unless force discards it", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "create two dirty threads") {
        for (const name of ["脏一", "脏二"]) {
          await management.execute(
            { action: "create-thread", name, worktree_name: `dirty-${name === "脏一" ? "one" : "two"}` },
            signal,
          );
        }
        return;
      }
      if (text === "delete without force") {
        await management.execute({ action: "thread-delete", short_id: "thread-1" }, signal);
        return;
      }
      if (text === "delete with force") {
        await management.execute({ action: "thread-delete", short_id: "thread-2", force: true }, signal);
      }
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "dirty cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "dirty-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "dirty-create",
        ...GROUP_TOOLS,
        text: "create two dirty threads",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    worktrees.removeFailure = new Error(
      "删除隔离工作区失败（退出码 1）：Cannot remove worktree: dirty-one has uncommitted changes",
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "dirty-delete-one",
        ...GROUP_TOOLS,
        text: "delete without force",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    expect(worktrees.removals).toEqual([
      { path: join(harness.root, "worktrees", "thread-1"), force: false, repo: cwd, branch: "thread-1" },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toMatchObject([
      { shortId: "thread-2" },
    ]);

    worktrees.removeFailure = undefined;
    await harness.service.handleInbound(
      inbound({
        messageId: "dirty-delete-two",
        ...GROUP_TOOLS,
        text: "delete with force",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");
    expect(worktrees.removals).toEqual([
      { path: join(harness.root, "worktrees", "thread-1"), force: false, repo: cwd, branch: "thread-1" },
      { path: join(harness.root, "worktrees", "thread-2"), force: true, repo: cwd, branch: "thread-2" },
    ]);
    await expect(harness.transport.listThreads("group-tools")).resolves.toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-archive schedules one archive that runs after the turn's reply is delivered", async () => {
  const transport = new ArchiveTracingTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append, emit, promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      if (text !== "archive the current thread twice") return;
      const signal = new AbortController().signal;
      const first = await management.execute({ action: "thread-archive" }, signal);
      const second = await management.execute({ action: "thread-archive" }, signal);
      emit(assistantText("archive-reply", "归档前的回复", true));
      await append("thread-archive.log", `${first}\n${second}\n`);
    },
  });
  try {
    const cwd = join(harness.root, "archive cwd");
    await mkdir(cwd, { recursive: true });
    harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
    await harness.service.handleInbound(
      inbound({ messageId: "archive-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "archive-run",
        ...THREAD_TOOLS,
        text: "archive the current thread twice",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await harness.transport.waitForManagementCalls((call) => call.action === "thread-archive");
    await waitForStatus(harness.service, THREAD_TOOLS, (status) => status.state === "completed");

    // 子区里省略 short_id 即当前线程；两次调用只排定一次，归档在本回合回复送达之后执行。
    expect(harness.transport.managementCalls.filter((call) => call.action === "thread-archive")).toEqual([
      { action: "thread-archive", groupId: "group-tools", shortId: "topic-7" },
    ]);
    expect(transport.archiveOrder).toHaveLength(1);
    expect(transport.archiveOrder[0]?.sentBefore).toContain("归档前的回复");
    expect(await readText(join(cwd, "thread-archive.log"))).toBe(
      "已排定归档子区 topic-7：本回合最后一条回复送达后执行\n"
      + "已排定归档子区 topic-7：本回合最后一条回复送达后执行\n",
    );
    await expect(harness.transport.listThreads("group-tools")).resolves.toMatchObject([
      { shortId: "topic-7", status: 2 },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a default-pipeline merge schedules the thread's archive beside the merge result", async () => {
  const transport = new ArchiveTracingTransport();
  const log: string[] = [];
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, promptContext, text }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      const signal = new AbortController().signal;
      if (text === "create the merge thread") {
        const created = JSON.parse(
          await management.execute(
            { action: "create-thread", name: "合并线程", worktree_name: "archive-workspace" },
            signal,
          ),
        ) as { short_id: string };
        log.push(`created:${created.short_id}\n`);
        return;
      }
      if (text === "merge and archive") {
        const callId = "merge-archive-tool-call";
        const result = await management.execute({ action: "thread-merge" }, signal, callId);
        management.onResultAccepted?.(callId);
        log.push(`${result}\n`);
        emit(assistantText("merge-archive-reply", "合并完成，本子区随后归档", true));
      }
    },
  });
  try {
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected a worktree port in the harness");
    const cwd = join(harness.root, "merge archive cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "merge-archive-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-archive-create",
        ...GROUP_TOOLS,
        text: "create the merge thread",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP_TOOLS, (status) => status.state === "completed");

    const thread = { channelId: "group-tools____thread-1", channelType: 5 as const };
    const workspace = join(harness.root, "worktrees", "thread-1");
    await waitForStatus(harness.service, thread, (status) => status.cwd === workspace);
    await harness.service.handleInbound(
      inbound({
        messageId: "merge-archive-run",
        ...thread,
        text: "merge and archive",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await harness.transport.waitForManagementCalls((call) => call.action === "thread-archive");
    await waitForStatus(harness.service, thread, (status) => status.state === "completed");

    // 合并结果与归档排定分别可见；归档在回复之后、以发起排定的子区为目标执行。
    expect(log.join("\n")).toContain('"archive":"scheduled"');
    expect(worktrees.merges).toHaveLength(1);
    expect(worktrees.merges[0]).toMatchObject({
      cwd: workspace,
      repo: cwd,
      branch: "thread-1",
    });
    expect(harness.transport.managementCalls.filter((call) => call.action === "thread-archive")).toEqual([
      { action: "thread-archive", groupId: "group-tools", shortId: "thread-1" },
    ]);
    expect(transport.archiveOrder[0]?.sentBefore).toContain("合并完成，本子区随后归档");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a transient archive failure retries five times five seconds apart and ends as one bounded notice", async () => {
  vi.useFakeTimers();
  try {
    const transport = new ArchiveTracingTransport();
    const harness = await createCoreHarness({
      transport,
      prompt: async ({ emit, promptContext, text }) => {
        const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
        if (!management) throw new Error("management tool missing");
        if (text !== "archive the current thread") return;
        await management.execute({ action: "thread-archive" }, new AbortController().signal);
        emit(assistantText("archive-retry-reply", "归档前的回复", true));
      },
    });
    try {
      const archiveCalls = (): number =>
        harness.transport.managementCalls.filter((call) => call.action === "thread-archive").length;
      const cwd = join(harness.root, "archive retry cwd");
      await mkdir(cwd, { recursive: true });
      harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
      harness.transport.failArchiveTimes(TRANSIENT_ARCHIVE_FAILURES);
      await harness.service.handleInbound(
        inbound({ messageId: "archive-retry-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
      );
      await harness.service.handleInbound(
        inbound({
          messageId: "archive-retry-run",
          ...THREAD_TOOLS,
          text: "archive the current thread",
          mentionedUserIds: [harness.bot.uid],
        }),
      );
      await harness.workers.waitForPromptCount(1);
      // 第一次尝试不等待计时器：排空与调用走微任务跑完。
      for (let step = 0; step < 200 && archiveCalls() < 1; step += 1) {
        await yieldRealIo(harness.stateDir);
        if (archiveCalls() >= 1) break;
        vi.advanceTimersByTime(150);
      }
      expect(archiveCalls()).toBe(1);
      // 间隔 5 秒：4 秒未到不重试，过 5 秒重试一次。
      vi.advanceTimersByTime(4_000);
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(1);
      vi.advanceTimersByTime(1_500);
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(2);
      // 其余尝试按同一间隔推进：总计至多 5 次，之后不再尝试。
      for (let step = 0; step < 100 && archiveCalls() < 5; step += 1) {
        vi.advanceTimersByTime(1_000);
        await yieldRealIo(harness.stateDir);
      }
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(5);
      vi.advanceTimersByTime(30_000);
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(5);

      const notices = harness.transport.sent.filter(({ text: body }) => body.startsWith("归档失败："));
      expect(notices).toHaveLength(1);
      expect(notices[0]!.text).not.toContain("\n");
      expect(notices[0]!.text.length).toBeLessThanOrEqual(210);
      expect(notices[0]!.text).toContain("HTTP 500");
    } finally {
      await closeCoreHarness(harness);
    }
  } finally {
    vi.useRealTimers();
  }
});

test("thread-archive does not retry a permanent failure and reports one bounded line", async () => {
  vi.useFakeTimers();
  try {
    const harness = await createCoreHarness({
      transport: new ArchiveTracingTransport(),
      prompt: async ({ emit, promptContext, text }) => {
        const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
        if (!management) throw new Error("management tool missing");
        if (text !== "archive topic nine") return;
        await management.execute(
          { action: "thread-archive", short_id: "topic-9" },
          new AbortController().signal,
        );
        emit(assistantText("archive-permanent-reply", "归档前的回复", true));
      },
    });
    try {
      const archiveCalls = (): number =>
        harness.transport.managementCalls.filter((call) => call.action === "thread-archive").length;
      const cwd = join(harness.root, "archive permanent cwd");
      await mkdir(cwd, { recursive: true });
      harness.transport.setThreads("group-tools", [threadRecord("topic-9")]);
      harness.transport.failArchiveTimes([
        Object.assign(new Error("Octo thread archive failed with HTTP 403: 无归档权限。"), { status: 403 }),
      ]);
      await harness.service.handleInbound(
        inbound({ messageId: "archive-permanent-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
      );
      await harness.service.handleInbound(
        inbound({
          messageId: "archive-permanent-run",
          ...GROUP_TOOLS,
          text: "archive topic nine",
          mentionedUserIds: [harness.bot.uid],
        }),
      );
      await harness.workers.waitForPromptCount(1);
      for (let step = 0; step < 200 && archiveCalls() < 1; step += 1) {
        await yieldRealIo(harness.stateDir);
        if (archiveCalls() >= 1) break;
        vi.advanceTimersByTime(150);
      }
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(1);
      // 永久失败不重试：推进一分钟也不会有第二次尝试。
      vi.advanceTimersByTime(60_000);
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(1);
      expect(harness.transport.managementCalls).toEqual([
        { action: "group-members", groupId: "group-tools", shortId: null },
        { action: "thread-archive", groupId: "group-tools", shortId: "topic-9" },
      ]);
      const notices = harness.transport.sent.filter(({ text: body }) => body.startsWith("归档失败："));
      expect(notices).toHaveLength(1);
      expect(notices[0]!.text).toContain("HTTP 403");
      expect(notices[0]!.text.length).toBeLessThanOrEqual(210);
    } finally {
      await closeCoreHarness(harness);
    }
  } finally {
    vi.useRealTimers();
  }
});

test("a new message in the same chat cancels the scheduled archive", async () => {
  const transport = new ArchiveTracingTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ promptContext, text, waitForAppend }) => {
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("management tool missing");
      if (text !== "archive then wait") return;
      await management.execute({ action: "thread-archive" }, new AbortController().signal);
      await waitForAppend();
    },
  });
  try {
    const cwd = join(harness.root, "archive cancel cwd");
    await mkdir(cwd, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "archive-cancel-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "archive-cancel-run",
        ...THREAD_TOOLS,
        text: "archive then wait",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    // 回合仍在运行：owner 的新消息（过权限门禁后）清除排定。
    await harness.service.handleInbound(
      inbound({
        messageId: "archive-cancel-msg",
        ...THREAD_TOOLS,
        text: "还有一件事",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    // 第二个任务在 chatTails 上串行执行：它开始即证明第一个回合的收尾（含归档检查）已经跑完。
    await harness.service.handleInbound(
      inbound({
        messageId: "archive-cancel-after",
        ...THREAD_TOOLS,
        text: "second task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    expect(transport.archiveOrder).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a pending archive is dropped when the channel stops before it runs", async () => {
  vi.useFakeTimers();
  try {
    const harness = await createCoreHarness({
      transport: new ArchiveTracingTransport(),
      prompt: async ({ emit, promptContext, text }) => {
        const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
        if (!management) throw new Error("management tool missing");
        if (text !== "archive the current thread") return;
        await management.execute({ action: "thread-archive" }, new AbortController().signal);
        emit(assistantText("archive-stop-reply", "归档前的回复", true));
      },
    });
    const archiveCalls = (): number =>
      harness.transport.managementCalls.filter((call) => call.action === "thread-archive").length;
    try {
      const cwd = join(harness.root, "archive stop cwd");
      await mkdir(cwd, { recursive: true });
      harness.transport.setThreads("group-tools", [threadRecord("topic-7")]);
      harness.transport.failArchiveTimes([
        Object.assign(new Error("Octo thread archive failed with HTTP 500"), { status: 500 }),
        Object.assign(new Error("Octo thread archive failed with HTTP 500"), { status: 500 }),
      ]);
      await harness.service.handleInbound(
        inbound({ messageId: "archive-stop-bind", ...GROUP_TOOLS, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
      );
      await harness.service.handleInbound(
        inbound({
          messageId: "archive-stop-run",
          ...THREAD_TOOLS,
          text: "archive the current thread",
          mentionedUserIds: [harness.bot.uid],
        }),
      );
      await harness.workers.waitForPromptCount(1);
      for (let step = 0; step < 200 && archiveCalls() < 1; step += 1) {
        await yieldRealIo(harness.stateDir);
        if (archiveCalls() >= 1) break;
        vi.advanceTimersByTime(150);
      }
      expect(archiveCalls()).toBe(1);
      // 停在重试等待中：关停立即结束等待并放弃归档，也不再发失败说明。
      await harness.service.stop();
      vi.advanceTimersByTime(60_000);
      await yieldRealIo(harness.stateDir);
      expect(archiveCalls()).toBe(1);
      expect(
        harness.transport.sent.filter(({ text: body }) => body.startsWith("归档失败：")),
      ).toEqual([]);
    } finally {
      await closeCoreHarness(harness);
    }
  } finally {
    vi.useRealTimers();
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

test("channel_control schedules a restart that runs once every chat is idle", async () => {
  const requests: number[] = [];
  const toolCalled = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const releaseFirst = Promise.withResolvers<void>();
  const releaseSecond = Promise.withResolvers<void>();
  let restartResult = "";
  const harness = await createCoreHarness({
    restart: {
      request: async () => {
        requests.push(Date.now());
      },
    },
    prompt: async ({ promptContext, text }) => {
      const tool = (promptContext?.tools ?? []).find(({ name }) => name === "channel_control");
      if (!tool) throw new Error("channel_control host tool missing");
      if (text.includes("first")) {
        const status = JSON.parse(
          await tool.execute({ action: "status" }, new AbortController().signal),
        ) as { pid: number; restart_pending: boolean; queued_tasks: number };
        expect(status.pid).toBe(process.pid);
        expect(status.restart_pending).toBe(false);
        restartResult = await tool.execute({ action: "restart" }, new AbortController().signal);
        toolCalled.resolve();
        await releaseFirst.promise;
        return;
      }
      secondStarted.resolve();
      await releaseSecond.promise;
    },
  });
  try {
    const first = inbound({ messageId: "channel-restart-first", text: "first task" });
    await harness.service.handleInbound(first);
    await toolCalled.promise;
    expect(restartResult).toContain("已排定重启");

    // Work that is still queued or running keeps the restart pending; it must never fire
    // inside the turn that asked for it.
    await harness.service.handleInbound(
      inbound({ messageId: "channel-restart-queued", text: "/queue second task" }),
    );
    expect(requests).toHaveLength(0);

    releaseFirst.resolve();
    // The chat stays busy while the queued task runs, so the restart must wait for it.
    await secondStarted.promise;
    await harness.workers.waitForPromptCount(2);
    expect(requests).toHaveLength(0);

    releaseSecond.resolve();
    await harness.workers.waitForPromptCount(2);
    for (let attempt = 0; attempt < 2000 && requests.length === 0; attempt += 1) {
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    expect(requests).toHaveLength(1);
  } finally {
    releaseFirst.resolve();
    releaseSecond.resolve();
    await closeCoreHarness(harness);
  }
});

test("channel_control without a detected supervisor reports it and schedules nothing", async () => {
  let restartResult = "";
  const harness = await createCoreHarness({
    prompt: async ({ promptContext }) => {
      const tool = (promptContext?.tools ?? []).find(({ name }) => name === "channel_control");
      if (!tool) throw new Error("channel_control host tool missing");
      restartResult = await tool.execute({ action: "restart" }, new AbortController().signal);
    },
  });
  try {
    const task = inbound({ messageId: "channel-restart-unconfigured", text: "restart the channel" });
    await harness.service.handleInbound(task);
    await waitForStatus(harness.service, task, (status) => status.state === "completed");
    expect(restartResult).toContain("未检测到守护进程");
  } finally {
    await closeCoreHarness(harness);
  }
});
