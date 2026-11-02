import { expect, test } from "bun:test";
import type { GroupMember } from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
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
    const sizes = [0, 10, 11];
    for (let index = 0; index < sizes.length; index += 1) {
      harness.transport.setGroupMembers(GROUP.channelId, roster(sizes[index]!));
      const text = `group task ${index + 1}`;
      await harness.service.handleInbound(groupInput(harness.bot.uid, `member-task-${index + 1}`, text));
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
      await harness.service.handleInbound(
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

test("refreshes group members for steering without dispatching a second task", async () => {
  const started = Promise.withResolvers<void>();
  const appended: string[] = [];
  const workers = new FakeWorkerFactory(async ({ text, waitForAppend }) => {
    if (text !== "long-running group task") return;
    started.resolve();
    appended.push((await waitForAppend()).text);
  });
  const harness = await createCoreHarness({ workers });
  try {
    const running = harness.service.handleInbound(
      groupInput(harness.bot.uid, "member-steer-first", "long-running group task"),
    );
    await started.promise;
    await harness.service.handleInbound(
      groupInput(harness.bot.uid, "member-steer-second", "fresh steering details"),
    );
    await running;
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    expect(appended).toEqual(["fresh steering details"]);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["long-running group task"]);
    expect(
      harness.transport.managementCalls.filter(({ action }) => action === "group-members"),
    ).toHaveLength(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("does not fetch members again when the owner sends native /retry", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(
      groupInput(harness.bot.uid, "member-before-native-retry", "first task"),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, GROUP, (status) => status.state === "completed");

    await harness.service.handleInbound(
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
    await harness.service.handleInbound(
      groupInput(harness.bot.uid, "member-queue-mode", "/mode queue"),
    );
    const first = harness.service.handleInbound(
      groupInput(harness.bot.uid, "member-queue-first", "blocking group task"),
    );
    await started.promise;

    await harness.service.handleInbound(
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
  const harness = await createCoreHarness();
  const privateServerError = "private member endpoint response: token-like-detail";
  try {
    harness.transport.failManagementWith(new Error(privateServerError));
    await harness.service.handleInbound(
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
  } finally {
    await closeCoreHarness(harness);
  }
});
