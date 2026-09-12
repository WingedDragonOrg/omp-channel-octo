import { expect, test } from "bun:test";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

test("each group task dispatches the target's current snapshot", async () => {
  const harness = await createCoreHarness();
  try {
    const groupA = { channelId: "group-a", channelType: 2 as const };
    const groupB = { channelId: "group-b", channelType: 2 as const };
    harness.transport.setGroupContext(groupA, {
      content: "Group A instructions",
      version: 3,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin-a",
    });
    harness.transport.setGroupContext(groupB, {
      content: "Group B instructions",
      version: 8,
      updatedAt: "2026-09-12T00:01:00.000Z",
      updatedBy: "admin-b",
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "group-a-message",
        ...groupA,
        text: "work for A",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({
        messageId: "group-b-message",
        ...groupB,
        text: "work for B",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, groupA, (status) => status.state === "completed");
    await waitForStatus(harness.service, groupB, (status) => status.state === "completed");

    const promptA = harness.workers.prompts.find(({ text }) => text.includes("work for A"));
    const promptB = harness.workers.prompts.find(({ text }) => text.includes("work for B"));
    expect(promptA).toBeDefined();
    expect(promptB).toBeDefined();
    const envelopeA = JSON.parse(promptA?.text ?? "{}");
    const envelopeB = JSON.parse(promptB?.text ?? "{}");
    expect(envelopeA.userMessage).toBe("work for A");
    expect(envelopeA.groupContext.content).toBe("Group A instructions");
    expect(envelopeA.groupContext.version).toBe(3);
    expect(envelopeB.userMessage).toBe("work for B");
    expect(envelopeB.groupContext.content).toBe("Group B instructions");
    expect(envelopeB.groupContext.version).toBe(8);
    expect(promptA?.text).toContain("群说明是群共享上下文");
    expect(promptB?.text).toContain("群说明是群共享上下文");
    expect(harness.transport.groupContextCalls).toEqual([groupA, groupB]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("updates and deletion are visible to the next group task, without private injection", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-fresh", channelType: 2 as const };
    const privateChat = { channelId: "dm-private", channelType: 1 as const };
    harness.transport.setGroupContext(group, {
      content: "initial shared note",
      version: 1,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin",
    });
    harness.transport.setGroupContext(privateChat, {
      content: "must never be injected",
      version: 1,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin",
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "fresh-first",
        ...group,
        text: "first group task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    harness.transport.setGroupContext(group, {
      content: "updated shared note",
      version: 2,
      updatedAt: "2026-09-12T00:02:00.000Z",
      updatedBy: "admin-2",
    });
    await harness.service.handleInbound(
      inbound({
        messageId: "fresh-second",
        ...group,
        text: "second group task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    harness.transport.setGroupContext(group, null);
    await harness.service.handleInbound(
      inbound({
        messageId: "fresh-third",
        ...group,
        text: "third group task",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({ messageId: "fresh-private", ...privateChat, text: "private task" }),
    );
    await harness.workers.waitForPromptCount(4);
    await waitForStatus(
      harness.service,
      privateChat,
      (status) => status.state === "completed",
    );

    const groupPrompts = harness.workers.prompts.filter(({ text }) => text.includes("group task"));
    expect(JSON.parse(groupPrompts[0]?.text ?? "{}").groupContext.content).toBe(
      "initial shared note",
    );
    expect(JSON.parse(groupPrompts[1]?.text ?? "{}").groupContext.content).toBe(
      "updated shared note",
    );
    expect(JSON.parse(groupPrompts[2]?.text ?? "{}").groupContext).toBeNull();
    expect(groupPrompts[2]?.text).toContain("当前无说明");

    const privatePrompt = harness.workers.prompts.find(({ text }) => text.includes("private task"));
    expect(privatePrompt?.text).not.toContain("must never be injected");
    expect(privatePrompt?.text).not.toContain("群说明是群共享上下文");
    expect(harness.transport.groupContextCalls).toEqual([group, group, group]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a group-context read failure fails the task before any worker prompt", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-unavailable", channelType: 2 as const };
    harness.transport.failGroupContextWith(new Error("fixture context unavailable"));
    await harness.service.handleInbound(
      inbound({
        messageId: "group-read-failed",
        ...group,
        text: "must not execute",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const status = await waitForStatus(
      harness.service,
      group,
      (next) => next.state === "failed",
    );
    expect(status.state).toBe("failed");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.some(({ text }) => /失败|说明|读取/.test(text))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
