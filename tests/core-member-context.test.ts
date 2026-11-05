import { Database } from "bun:sqlite";
import { expect, test, vi } from "bun:test";
import { join } from "node:path";
import type { GroupMember } from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

const GROUP = { channelId: "group-allowed", channelType: 2 as const };
const THREAD = { channelId: "group-allowed____topic-7", channelType: 5 as const };

function roster(count: number): GroupMember[] {
  return Array.from({ length: count }, (_, index) => ({
    uid: `member-${index + 1}`,
    name: `Member ${index + 1}`,
    role: 0,
    robot: false,
    joinedAt: "2026-11-02T00:00:00.000Z",
  }));
}

function groupInput(botUid: string, messageId: string, text: string) {
  return inbound({
    messageId,
    ...GROUP,
    text,
    mentionedUserIds: [botUid],
  });
}

function threadInput(botUid: string, messageId: string, text: string) {
  return inbound({
    messageId,
    ...THREAD,
    text,
    mentionedUserIds: [botUid],
  });
}

test("reads the group member API once for every new task, even when a roster is empty", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setGroupMembers(GROUP.channelId, []);
    for (let index = 0; index < 2; index += 1) {
      const text = `group task ${index + 1}`;
      await harness.service.submitInbound(groupInput(harness.bot.uid, `member-task-${index + 1}`, text));
      await harness.workers.waitForPromptCount(index + 1);
      await waitForStatus(
        harness.service,
        GROUP,
        (status) => status.state === "completed",
      );
      expect(harness.workers.prompts[index]?.text).toBe(text);
    }

    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toEqual([
      { action: "group-members", groupId: GROUP.channelId, shortId: null },
      { action: "group-members", groupId: GROUP.channelId, shortId: null },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("reads each thread task's roster from that thread API rather than the parent group", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setThreadMembers("group-allowed", "topic-7", roster(2));
    for (let index = 0; index < 2; index += 1) {
      const text = `thread task ${index + 1}`;
      await harness.service.submitInbound(
        threadInput(harness.bot.uid, `thread-member-task-${index + 1}`, text),
      );
      await harness.workers.waitForPromptCount(index + 1);
      await waitForStatus(
        harness.service,
        THREAD,
        (status) => status.state === "completed",
      );
      expect(harness.workers.prompts[index]?.text).toBe(text);
    }

    expect(
      harness.transport.managementCalls.filter(({ action }) => action.endsWith("-members")),
    ).toEqual([
      { action: "thread-members", groupId: "group-allowed", shortId: "topic-7" },
      { action: "thread-members", groupId: "group-allowed", shortId: "topic-7" },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("refreshes members for each steering in accepted order without dispatching new tasks", async () => {
  const started = Promise.withResolvers<void>();
  const appended: string[] = [];
  const harness = await createCoreHarness({
    prompt: async ({ text, waitForAppend }) => {
      if (text !== "long-running group task") return;
      started.resolve();
      appended.push((await waitForAppend()).text);
      appended.push((await waitForAppend()).text);
    },
  });
  try {
    const running = harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-steer-first", "long-running group task"),
    );
    await started.promise;
    const firstSteering = harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-steer-second", "first steering facts"),
    );
    await harness.transport.waitForManagementCalls(
      ({ action }) => action === "group-members",
      2,
    );
    harness.transport.setGroupMembers(GROUP.channelId, [roster(1)[0]!]);
    const secondSteering = harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-steer-third", "second steering facts"),
    );
    await Promise.all([firstSteering, secondSteering, running]);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    expect(appended).toEqual(["first steering facts", "second steering facts"]);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["long-running group task"]);
    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toHaveLength(3);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed member refresh consumes the steering input without appending or queuing it", async () => {
  const transport = new (class extends FakeTransport {
    failMemberReads = false;
    override async getGroupMembers(groupId: string, signal?: AbortSignal) {
      const members = await super.getGroupMembers(groupId, signal);
      if (this.failMemberReads) throw new Error("private member-service response");
      return members;
    }
  })();
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const promptFinished = Promise.withResolvers<void>();
  let appended: string[] = [];
  const workers = new FakeWorkerFactory(async ({ text, appended: getAppended }) => {
    if (text !== "long-running task") return;
    started.resolve();
    await release.promise;
    appended = getAppended().map(({ text: value }) => value);
    promptFinished.resolve();
  });
  const harness = await createCoreHarness({ transport, workers });
  try {
    const running = harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-steer-failure-first", "long-running task"),
    );
    await started.promise;
    transport.failMemberReads = true;
    const failureText = "成员信息读取失败，本条消息未并入当前回合。";
    const failureReceipt = transport.waitForRemoteText(
      ({ target, text }) =>
        target.channelId === GROUP.channelId &&
        target.channelType === GROUP.channelType &&
        text === failureText,
    );
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-steer-failure-second", "must not join or queue"),
    );
    await failureReceipt;
    release.resolve();
    await running;
    await promptFinished.promise;
    await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "completed" && status.queued === 0,
    );

    expect(workers.prompts.map(({ text }) => text)).toEqual(["long-running task"]);
    expect(appended).toEqual([]);
    expect(transport.sent.map(({ text }) => text)).toContain(failureText);
    expect(transport.sent.map(({ text }) => text).join("\n"))
      .not.toContain("private member-service response");
    expect(transport.managementCalls.filter(({ action }) => action === "group-members"))
      .toHaveLength(2);
  } finally {
    release.resolve();
    await closeCoreHarness(harness);
  }
});

test("does not fetch members again when the owner sends native /retry", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-before-native-retry", "first task"),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-native-retry", "/retry"),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["first task", "/retry"]);
    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a queued group task fetches its roster only when it starts executing", async () => {
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const workers = new FakeWorkerFactory(async ({ text }) => {
    if (text === "blocking group task") {
      started.resolve();
      await release.promise;
    }
  });
  const harness = await createCoreHarness({ workers });
  try {
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-queue-mode", "/mode queue"),
    );
    const first = harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-queue-first", "blocking group task"),
    );
    await started.promise;

    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-queue-second", "queued group task"),
    );
    await waitForStatus(harness.service, GROUP, (status) => status.queued === 1);
    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toHaveLength(1);

    harness.transport.setGroupMembers(GROUP.channelId, roster(1));
    release.resolve();
    await first;
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toHaveLength(2);
  } finally {
    release.resolve();
    await closeCoreHarness(harness);
  }
});

test("a member API failure fails the new task before prompting and hides the raw error", async () => {
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  const harness = await createCoreHarness();
  const privateServerError = "private member endpoint response: token-like-detail";
  try {
    harness.transport.failManagementWith(new Error(privateServerError));
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-api-failed", "must not execute"),
    );

    const status = await waitForStatus(
      harness.service,
      GROUP,
      (next) => next.state === "failed",
    );
    expect(status.state).toBe("failed");
    expect(harness.transport.managementCalls.filter(({ action }) => action === "group-members"))
      .toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.map(({ text }) => text)).toContain(
      "成员信息读取失败，未启动任务。",
    );
    expect(harness.transport.sent.map(({ text }) => text).join("\n")).not.toContain(
      privateServerError,
    );

    const database = new Database(join(harness.stateDir, "channel.sqlite"));
    let task: { id: string; error_text: string | null } | null;
    try {
      task = database
        .query("SELECT id, error_text FROM tasks WHERE message_id = ?")
        .get("member-api-failed") as { id: string; error_text: string | null } | null;
    } finally {
      database.close();
    }
    expect(task?.error_text).toStartWith("成员信息读取失败（member-read）：");
    expect(task?.error_text).toContain("成员 API 请求失败");
    expect(task?.error_text).not.toContain("token-like-detail");

    const taskLogs = logged.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes(`task=${task?.id} `));
    expect(taskLogs).toHaveLength(1);
    expect(taskLogs[0]).toContain("kind=members");
    expect(taskLogs[0]).toContain("code=member-read");
    expect(taskLogs[0]).toContain("detail=成员 API 请求失败");
    expect(taskLogs.join("\n")).not.toContain("token-like-detail");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
});

// The real service timeout is a five-second AbortSignal boundary; this integration path must observe it.
test("a hung member API request times out before the task reaches the model", async () => {
  const transport = new FakeTransport();
  let requestSignal: AbortSignal | undefined;
  const requestAborted = Promise.withResolvers<void>();
  const memberRequest = Promise.withResolvers<GroupMember[]>();
  transport.getGroupMembers = async (_groupId, signal) => {
    requestSignal = signal;
    if (signal?.aborted) requestAborted.resolve();
    else signal?.addEventListener("abort", () => requestAborted.resolve(), { once: true });
    return await memberRequest.promise;
  };
  const harness = await createCoreHarness({ transport });
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-timeout", "must not start after timeout"),
    );
    await requestAborted.promise;
    const status = await waitForStatus(
      harness.service,
      GROUP,
      (next) => next.state === "failed",
    );

    expect(status.state).toBe("failed");
    expect(requestSignal?.aborted).toBe(true);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.map(({ text }) => text)).toContain(
      "成员信息读取失败，未启动任务。",
    );
    const database = new Database(join(harness.stateDir, "channel.sqlite"));
    let taskId: string | null;
    try {
      const task = database
        .query("SELECT id FROM tasks WHERE message_id = ?")
        .get("member-timeout") as { id: string } | null;
      taskId = task?.id ?? null;
    } finally {
      database.close();
    }
    if (taskId === null) throw new Error("timed-out member task row is missing");
    const taskLog = logged.mock.calls
      .map(([line]) => String(line))
      .find((line) => line.includes(`task=${taskId} `));
    expect(taskLog).toContain("detail=成员 API 查询超时");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
}, 10_000);

test("cancelling during member lookup aborts the request without starting a worker", async () => {
  const transport = new FakeTransport();
  const requestStarted = Promise.withResolvers<AbortSignal>();
  const memberRequest = Promise.withResolvers<GroupMember[]>();
  transport.getGroupMembers = async (_groupId, signal) => {
    if (!signal) throw new Error("member request signal was not provided");
    requestStarted.resolve(signal);
    return await memberRequest.promise;
  };
  const harness = await createCoreHarness({ transport });
  try {
    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-cancel", "must not start after cancellation"),
    );
    const requestSignal = await requestStarted.promise;

    await harness.service.submitInbound(
      groupInput(harness.bot.uid, "member-cancel-command", "/cancel"),
    );
    const status = await waitForStatus(
      harness.service,
      GROUP,
      (next) => next.state === "cancelled",
    );

    expect(status.state).toBe("cancelled");
    expect(requestSignal.aborted).toBe(true);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.map(({ text }) => text)).not.toContain(
      "成员信息读取失败，未启动任务。",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});
