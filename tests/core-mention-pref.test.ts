import { expect, test } from "bun:test";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";


test("accepts an unmentioned group message once the server reports 免@", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setGroupMentionPref("group-allowed", { effective: true });
    const message = inbound({
      messageId: "mention-pref-open",
      channelId: "group-allowed",
      channelType: 2,
      text: "run this without mentioning the bot",
    });

    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, message, (status) => status.state === "completed");

    expect(harness.workers.prompts.map((prompt) => prompt.text)).toEqual([
      "run this without mentioning the bot",
    ]);
    expect(harness.transport.mentionPrefCalls).toEqual(["group-allowed"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("keeps requiring an @mention while the group decision is off or unreadable", async () => {
  const harness = await createCoreHarness();
  try {
    const closed = inbound({
      messageId: "mention-pref-closed",
      channelId: "group-allowed",
      channelType: 2,
      text: "no mention here",
    });
    await harness.service.handleInbound(closed);
    expect(harness.transport.mentionPrefCalls).toEqual(["group-allowed"]);

    harness.transport.failGroupMentionPrefWith(new Error("mention preference unavailable"));
    const failed = inbound({
      messageId: "mention-pref-failed",
      channelId: "group-a",
      channelType: 2,
      text: "lookup fails",
    });
    await harness.service.handleInbound(failed);

    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("reuses one 免@ decision per group and skips it for a mentioned message", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setGroupMentionPref("group-allowed", { effective: true });
    const first = inbound({
      messageId: "mention-pref-cache-1",
      channelId: "group-allowed",
      channelType: 2,
      text: "first unmentioned",
    });
    const mentioned = inbound({
      messageId: "mention-pref-mentioned",
      channelId: "group-allowed",
      channelType: 2,
      text: "mentioned message",
      mentionedUserIds: [harness.bot.uid],
    });
    const second = inbound({
      messageId: "mention-pref-cache-2",
      channelId: "group-allowed",
      channelType: 2,
      text: "second unmentioned",
    });

    await harness.service.handleInbound(first);
    await harness.service.handleInbound(mentioned);
    await harness.service.handleInbound(second);
    await waitForStatus(harness.service, first, (status) => status.state === "completed");
    await waitForStatus(harness.service, mentioned, (status) => status.state === "completed");
    await waitForStatus(harness.service, second, (status) => status.state === "completed");

    expect(harness.transport.mentionPrefCalls).toEqual(["group-allowed"]);
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("never asks for a 免@ decision on a message that is rejected first", async () => {
  const harness = await createCoreHarness();
  try {
    const otherMember = inbound({
      messageId: "mention-pref-other-member",
      channelId: "group-allowed",
      channelType: 2,
      senderId: "other-user",
      text: "other member work",
    });
    const unlistedGroup = inbound({
      messageId: "mention-pref-unlisted",
      channelId: "group-denied",
      channelType: 2,
      text: "unlisted group work",
    });
    const ownerDm = inbound({
      messageId: "mention-pref-dm",
      channelId: "dm-owner",
      channelType: 1,
      text: "direct work",
    });

    await harness.service.handleInbound(otherMember);
    await harness.service.handleInbound(unlistedGroup);
    await harness.service.handleInbound(ownerDm);
    await waitForStatus(harness.service, ownerDm, (status) => status.state === "completed");

    expect(harness.transport.mentionPrefCalls).toHaveLength(0);
    expect(harness.workers.prompts.map((prompt) => prompt.text)).toEqual(["direct work"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the 免@ exemption stays the owner's: an unmentioned member's message never runs", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setGroupMentionPref("group-allowed", { effective: true });
    const owner = inbound({
      messageId: "member-free-owner",
      channelId: "group-allowed",
      channelType: 2,
      text: "owner work",
    });
    const member = inbound({
      messageId: "member-free-member",
      channelId: "group-allowed",
      channelType: 2,
      senderId: "other-user",
      text: "member work",
    });

    await harness.service.handleInbound(owner);
    await harness.service.handleInbound(member);
    await waitForStatus(harness.service, owner, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["owner work"]);
    // Only the owner's message needed the upstream decision; the member's never asks for it.
    expect(harness.transport.mentionPrefCalls).toEqual(["group-allowed"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("uses the parent group's 免@ decision inside a thread", async () => {
  const harness = await createCoreHarness();
  try {
    harness.transport.setGroupMentionPref("group-allowed", { effective: true });
    const message = inbound({
      messageId: "mention-pref-thread",
      channelId: "group-allowed____thread-1",
      channelType: 5,
      text: "thread work without a mention",
    });

    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, message, (status) => status.state === "completed");

    expect(harness.transport.mentionPrefCalls).toEqual(["group-allowed"]);
    expect(harness.workers.prompts.map((prompt) => prompt.text)).toEqual([
      "thread work without a mention",
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});
