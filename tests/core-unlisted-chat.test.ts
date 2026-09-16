import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import type { AllowlistWriteback, InboundMessage } from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";

const ALLOWED = ["group-allowed"];
const DENIED_GROUP = "group-denied";
const DENIED_THREAD = "group-denied____topic-1";

/** Records the mirror calls; `failure` makes every append reject like an unwritable config file. */
class FakeWriteback implements AllowlistWriteback {
  readonly appended: string[] = [];
  failure: Error | undefined;

  async append(groupId: string): Promise<void> {
    if (this.failure) throw this.failure;
    this.appended.push(groupId);
  }
}

async function settle(): Promise<void> {
  for (let tick = 0; tick < 50; tick += 1) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

async function waitForSent(harness: CoreHarness, count: number): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    if (harness.transport.sent.length >= count) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error(`transport recorded ${harness.transport.sent.length} of ${count} messages`);
}

function queryRows(harness: CoreHarness, sql: string): Array<Record<string, unknown>> {
  const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    return database.query(sql).all() as Array<Record<string, unknown>>;
  } finally {
    database.close();
  }
}

function ownerGroupMessage(overrides: Partial<InboundMessage>): InboundMessage {
  return inbound({
    channelId: DENIED_GROUP,
    channelType: 2,
    mentionedUserIds: [],
    ...overrides,
  });
}

test("an owner mention in an unlisted group gets one throttled notice and never a task", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const botUid = harness.bot.uid;
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-first",
      text: "@Bot 看下这个群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, 1);

    expect(harness.transport.sent).toHaveLength(1);
    const notice = harness.transport.sent[0]!;
    expect(notice.target).toMatchObject({ channelId: DENIED_GROUP, channelType: 2 });
    expect(notice.text).toContain("本群未开通");
    expect(notice.text).toContain("开通本群");

    // Still inside the notice window: the same owner @ gets no second reply.
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-second",
      text: "@Bot 再试一次",
      mentionedUserIds: [botUid],
    }));
    // Every other rejection path stays exactly as silent as before.
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-other-member",
      senderId: "other-user",
      text: "other member tries",
      mentionedUserIds: [botUid],
    }));
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-unmentioned",
      text: "owner talks without mentioning",
    }));
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-bot-echo",
      senderId: botUid,
      text: "bot echo",
      mentionedUserIds: [botUid],
    }));
    await settle();

    expect(harness.transport.sent).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect((await harness.service.status({ channelId: DENIED_GROUP, channelType: 2 })).state).toBe("none");
    // The rejection path persists nothing: no dedup row, no task, no queued reply, and the grant
    // set is untouched.
    expect(queryRows(harness, "SELECT 1 FROM inbound")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM outbox WHERE state <> 'sent'")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the owner's passphrase enables the group durably and it survives a restart", async () => {
  const writeback = new FakeWriteback();
  const first = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  const root = first.root;
  try {
    await first.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-mention-before-grant",
      text: "@Bot 看下这个群",
      mentionedUserIds: [first.bot.uid],
    }));
    await first.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant",
      text: " 开通本群 ",
      mentionedUserIds: [first.bot.uid],
    }));
    await waitForSent(first, 2);

    expect(first.transport.sent[0]!.text).toContain("本群未开通");
    expect(first.transport.sent[1]!.text).toContain("已开通本群");
    expect(writeback.appended).toEqual([DENIED_GROUP]);
    expect(queryRows(first, "SELECT channel_type, channel_id, granted_by FROM allowed_chats")).toEqual([
      { channel_type: 2, channel_id: DENIED_GROUP, granted_by: "owner-1" },
    ]);
    expect(first.workers.prompts).toHaveLength(0);

    // No restart, no config reload: the very next message in that group is a task, and the grant
    // did not open a second notice path for it.
    const work = ownerGroupMessage({
      messageId: "unlisted-work",
      text: "现在开始干活",
      mentionedUserIds: [first.bot.uid],
    });
    await first.service.handleInbound(work);
    await waitForStatus(first.service, work, (status) => status.state === "completed");
    expect(first.workers.prompts.map(({ text }) => text)).toEqual(["现在开始干活"]);
    expect(first.transport.sent.filter(({ text }) => text.includes("未开通"))).toHaveLength(1);
    expect(first.transport.sent.filter(({ text }) => text.includes("已开通"))).toHaveLength(1);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const second = await createCoreHarness({ root, allowedGroupIds: ALLOWED });
  try {
    const resumed = ownerGroupMessage({
      messageId: "unlisted-after-restart",
      text: "重启后继续",
      mentionedUserIds: [second.bot.uid],
    });
    await second.service.handleInbound(resumed);
    await waitForStatus(second.service, resumed, (status) => status.state === "completed");
    expect(second.workers.prompts.map(({ text }) => text)).toEqual(["重启后继续"]);
    expect(queryRows(second, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
  } finally {
    await closeCoreHarness(second);
  }
});

test("the passphrase only works for the owner, only verbatim, and only with a mention", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant-other",
      senderId: "other-user",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant-unmentioned",
      text: "开通本群",
    }));
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant-not-exact",
      text: "请帮我开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await settle();

    // The near-miss got the ordinary notice; nothing granted and no task started.
    expect(harness.transport.sent).toHaveLength(1);
    expect(harness.transport.sent[0]!.text).toContain("本群未开通");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM tasks")).toHaveLength(0);

    // The owner's exact reply still enables the group.
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant-owner",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, 2);
    expect(harness.transport.sent[1]!.text).toContain("已开通本群");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a thread carries its own notice and grant while its parent group stays unlisted", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const botUid = harness.bot.uid;
    await harness.service.handleInbound(inbound({
      messageId: "unlisted-thread-mention",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "@Bot 子区里干活",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, 1);
    expect(harness.transport.sent[0]!.text).toContain("本子区未开通");
    expect(harness.transport.sent[0]!.text).toContain("开通本子区");

    await harness.service.handleInbound(inbound({
      messageId: "unlisted-thread-grant",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "开通本群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, 2);
    expect(harness.transport.sent[1]!.text).toContain("已开通本子区");
    expect(queryRows(harness, "SELECT channel_type, channel_id FROM allowed_chats")).toEqual([
      { channel_type: 5, channel_id: DENIED_THREAD },
    ]);

    // The granted thread runs tasks; the parent group keeps its own unlisted state.
    const threadWork = inbound({
      messageId: "unlisted-thread-work",
      channelId: DENIED_THREAD,
      channelType: 5,
      text: "子区任务",
      mentionedUserIds: [botUid],
    });
    await harness.service.handleInbound(threadWork);
    await waitForStatus(harness.service, threadWork, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["子区任务"]);
    expect(harness.transport.sent.map(({ text }) => text).filter((text) => text.includes("未开通"))).toHaveLength(1);

    // Granting the parent group covers its threads, including one that never had its own entry.
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-parent-grant",
      text: "开通本群",
      mentionedUserIds: [botUid],
    }));
    await waitForSent(harness, 3);
    expect(harness.transport.sent[2]!.text).toContain("已开通本群");
    const sibling = inbound({
      messageId: "unlisted-sibling-thread",
      channelId: "group-denied____topic-2",
      channelType: 5,
      text: "另一个子区任务",
      mentionedUserIds: [botUid],
    });
    await harness.service.handleInbound(sibling);
    await waitForStatus(harness.service, sibling, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["子区任务", "另一个子区任务"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an allowlisted chat runs its message and never sees the notice", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED });
  try {
    const allowed = inbound({
      messageId: "unlisted-control-allowed",
      channelId: "group-allowed",
      channelType: 2,
      text: "允许群任务",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(allowed);
    await waitForStatus(harness.service, allowed, (status) => status.state === "completed");
    await settle();

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["允许群任务"]);
    expect(harness.transport.sent.filter(({ text }) => text.includes("未开通"))).toHaveLength(0);
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed config write-back keeps the durable grant and says so", async () => {
  const writeback = new FakeWriteback();
  writeback.failure = new Error("fixture config is read-only");
  const harness = await createCoreHarness({ allowedGroupIds: ALLOWED, allowlistWriteback: writeback });
  try {
    await harness.service.handleInbound(ownerGroupMessage({
      messageId: "unlisted-grant-mirror-failure",
      text: "开通本群",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForSent(harness, 1);
    expect(harness.transport.sent[0]!.text).toContain("写回失败");
    expect(queryRows(harness, "SELECT 1 FROM allowed_chats")).toHaveLength(1);

    const work = ownerGroupMessage({
      messageId: "unlisted-work-after-mirror-failure",
      text: "照常干活",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.handleInbound(work);
    await waitForStatus(harness.service, work, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["照常干活"]);
  } finally {
    await closeCoreHarness(harness);
  }
});
