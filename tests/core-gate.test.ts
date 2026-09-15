import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";


test("only the owner and an explicitly mentioned bot can execute", async () => {
  const harness = await createCoreHarness();
  try {
    const ownerDm = inbound({
      messageId: "gate-owner-dm",
      channelId: "dm-owner",
      channelType: 1,
      text: "owner dm work",
    });
    const allowedGroup = inbound({
      messageId: "gate-allowed-group",
      channelId: "group-allowed",
      channelType: 2,
      text: "@Fixture Bot group work",
      mentionedUserIds: [harness.bot.uid],
    });

    await harness.service.handleInbound(ownerDm);
    await harness.service.handleInbound(allowedGroup);

    await waitForStatus(
      harness.service,
      ownerDm,
      (status) => status.state === "completed",
    );
    await waitForStatus(
      harness.service,
      allowedGroup,
      (status) => status.state === "completed",
    );

    const effectsPath = join(harness.root, "tmp", "effects.log");
    const effects = (await readText(effectsPath)).trim().split("\n").filter(Boolean);
    expect(effects[0]).toBe("owner dm work");
    expect(effects[1]).toBe("@Fixture Bot group work");

  } finally {
    await closeCoreHarness(harness);
  }
});

test("rejects non-owner, unallowed, unmentioned, textual-only, bot, and unsupported messages", async () => {
  const harness = await createCoreHarness();
  try {
    const rejected = [
      inbound({
        messageId: "gate-other-dm",
        channelId: "dm-other",
        channelType: 1,
        senderId: "other-user",
        text: "other dm work",
      }),
      inbound({
        messageId: "gate-other-group",
        channelId: "group-allowed",
        channelType: 2,
        senderId: "other-user",
        mentionedUserIds: [harness.bot.uid],
        text: "other member work",
      }),
      inbound({
        messageId: "gate-unallowed-group",
        channelId: "group-denied",
        channelType: 2,
        mentionedUserIds: [harness.bot.uid],
        text: "unallowed group work",
      }),
      inbound({
        messageId: "gate-unmentioned-group",
        channelId: "group-allowed",
        channelType: 2,
        text: "Fixture Bot fake textual mention",
      }),
      inbound({
        messageId: "gate-bot-echo",
        channelId: "dm-owner",
        channelType: 1,
        senderId: harness.bot.uid,
        text: "bot echo",
      }),
      inbound({
        messageId: "gate-unsupported",
        channelId: "dm-owner",
        channelType: 1,
        supported: false,
        text: "unsupported attachment",
      }),
    ];

    for (const message of rejected) {
      await harness.service.handleInbound(message);
    }

    expect(harness.workers.prompts).toHaveLength(0);
    const status = await harness.service.status({ channelId: "dm-owner", channelType: 1 });
    expect(status.state).toBe("none");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a bare mention runs a normal turn instead of being refused as unsupported", async () => {
  const harness = await createCoreHarness();
  try {
    const bare = inbound({
      messageId: "gate-bare-mention",
      channelId: "group-allowed",
      channelType: 2,
      text: "",
      mentionedUserIds: [harness.bot.uid],
    });

    await harness.service.handleInbound(bare);
    await waitForStatus(harness.service, bare, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]!.text).toBe("");
    expect(harness.transport.sent.map((message) => message.text))
      .not.toContain("不支持的消息类型，不会启动任务。");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an unconfigured bot enrolls on the first supported non-empty DM without running it", async () => {
  const harness = await createCoreHarness({ ownerId: null });
  const first = inbound({
    messageId: "enrollment-first",
    channelId: "dm-enrollment",
    channelType: 1,
    senderId: "alice",
    text: "first enrollment",
  });
  try {
    await harness.service.handleInbound(first);

    expect(harness.workers.prompts).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "effects.log"))).toBe("");
    expect(harness.transport.sent).toHaveLength(1);
    const confirmation = harness.transport.sent[0]!;
    expect(confirmation.target).toMatchObject({ channelId: "dm-enrollment", channelType: 1 });
    expect(confirmation.text).toContain("alice");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the enrollment message is durably deduplicated across repost and restart", async () => {
  const first = await createCoreHarness({ ownerId: null });
  const message = inbound({
    messageId: "enrollment-durable",
    channelId: "dm-enrollment-durable",
    channelType: 1,
    senderId: "alice",
    text: "durable enrollment",
  });
  try {
    await first.service.handleInbound(message);
    expect(first.transport.sent).toHaveLength(1);
    expect(first.transport.sent[0]!.text).toContain("alice");
    const firstReplyCount = first.transport.sent.length;
    await first.service.handleInbound(message);

    expect(first.workers.prompts).toHaveLength(0);
    expect(first.transport.sent).toHaveLength(firstReplyCount);
    expect(await readText(join(first.root, "tmp", "effects.log"))).toBe("");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({
    root: first.root,
    bot: first.bot,
    ownerId: null,
  });
  try {
    await restarted.service.handleInbound(message);

    expect(restarted.workers.prompts).toHaveLength(0);
    expect(restarted.transport.sent).toHaveLength(0);
    expect(await readText(join(first.root, "tmp", "effects.log"))).toBe("");
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("owner enrollment is isolated by bot UID in the durable store", async () => {
  const first = await createCoreHarness({
    ownerId: null,
    bot: { uid: "bot-enrollment-a", name: "Enrollment Bot A" },
  });
  const firstMessage = inbound({
    messageId: "bot-a-enrollment",
    channelId: "dm-bot-isolation",
    channelType: 1,
    senderId: "alice",
    text: "bot A enrollment",
  });
  try {
    await first.service.handleInbound(firstMessage);
    expect(first.transport.sent).toHaveLength(1);
    expect(first.transport.sent[0]!.text).toContain("alice");
    expect(first.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const second = await createCoreHarness({
    root: first.root,
    ownerId: null,
    bot: { uid: "bot-enrollment-b", name: "Enrollment Bot B" },
  });
  const secondMessage = inbound({
    messageId: "bot-b-enrollment",
    channelId: "dm-bot-isolation",
    channelType: 1,
    senderId: "bob",
    text: "bot B enrollment",
  });
  try {
    await second.service.handleInbound(secondMessage);
    expect(second.transport.sent).toHaveLength(1);
    expect(second.transport.sent[0]!.text).toContain("bob");
    expect(second.workers.prompts).toHaveLength(0);

    const task = inbound({
      messageId: "bot-b-task",
      channelId: "dm-bot-isolation",
      channelType: 1,
      senderId: "bob",
      text: "bot B task",
    });
    await second.service.handleInbound(task);
    await waitForStatus(second.service, task, (status) => status.state === "completed");
    expect(second.workers.prompts.map(({ text }) => text)).toEqual(["bot B task"]);
  } finally {
    await closeCoreHarness(second);
  }
});

test("the enrolled owner can run later tasks while another DM sender is rejected", async () => {
  const harness = await createCoreHarness({ ownerId: null });
  const target = { channelId: "dm-enrolled-owner", channelType: 1 as const };
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "enrollment-owner",
        ...target,
        senderId: "alice",
        text: "enroll me",
      }),
    );
    const ownerTask = inbound({
      messageId: "enrollment-owner-task",
      ...target,
      senderId: "alice",
      text: "alice task",
    });
    await harness.service.handleInbound(ownerTask);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({
        messageId: "enrollment-other-task",
        ...target,
        senderId: "bob",
        text: "bob task",
      }),
    );

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["alice task"]);
    expect((await readText(join(harness.root, "tmp", "effects.log"))).trim()).toBe("alice task");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("group, bot, blank, and unsupported messages cannot claim an unconfigured owner", async () => {
  const harness = await createCoreHarness({ ownerId: null });
  const target = { channelId: "dm-invalid-enrollment", channelType: 1 as const };
  try {
    const invalid = [
      inbound({
        messageId: "invalid-group",
        channelId: "group-allowed",
        channelType: 2,
        senderId: "attacker",
        text: "group claim",
        mentionedUserIds: [harness.bot.uid],
      }),
      inbound({
        messageId: "invalid-thread",
        channelId: "thread-allowed",
        channelType: 5,
        senderId: "attacker",
        text: "thread claim",
        mentionedUserIds: [harness.bot.uid],
      }),
      inbound({
        messageId: "invalid-bot",
        ...target,
        senderId: harness.bot.uid,
        text: "bot claim",
      }),
      inbound({
        messageId: "invalid-blank-text",
        ...target,
        senderId: "attacker",
        text: "   ",
      }),
      inbound({
        messageId: "invalid-unsupported",
        ...target,
        senderId: "attacker",
        supported: false,
        text: "unsupported claim",
      }),
      inbound({
        messageId: "invalid-blank-sender",
        ...target,
        senderId: " ",
        text: "blank sender",
      }),
      inbound({
        messageId: "",
        ...target,
        senderId: "attacker",
        text: "blank message id",
      }),
      inbound({
        messageId: " ",
        ...target,
        senderId: "attacker",
        text: "blank message id",
      }),
    ];
    for (const message of invalid) {
      await harness.service.handleInbound(message);
    }

    expect(harness.workers.prompts).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "effects.log"))).toBe("");

    await harness.service.handleInbound(
      inbound({
        messageId: "valid-enrollment-after-invalid",
        ...target,
        senderId: "alice",
        text: "valid enrollment",
      }),
    );
    const ownerTask = inbound({
      messageId: "valid-owner-task-after-invalid",
      ...target,
      senderId: "alice",
      text: "valid owner task",
    });
    await harness.service.handleInbound(ownerTask);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({
        messageId: "invalid-other-after-bind",
        ...target,
        senderId: "bob",
        text: "invalid other task",
      }),
    );
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["valid owner task"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("two concurrent first DMs produce exactly one durable owner", async () => {
  const harness = await createCoreHarness({ ownerId: null });
  const aliceTarget = { channelId: "dm-race-alice", channelType: 1 as const };
  const bobTarget = { channelId: "dm-race-bob", channelType: 1 as const };
  try {
    await Promise.all([
      harness.service.handleInbound(
        inbound({
          messageId: "race-alice-enrollment",
          ...aliceTarget,
          senderId: "alice",
          text: "alice first",
        }),
      ),
      harness.service.handleInbound(
        inbound({
          messageId: "race-bob-enrollment",
          ...bobTarget,
          senderId: "bob",
          text: "bob first",
        }),
      ),
    ]);

    const confirmations = harness.transport.sent.filter(({ target }) =>
      target.channelId === aliceTarget.channelId || target.channelId === bobTarget.channelId,
    );
    expect(confirmations).toHaveLength(1);
    const winner =
      confirmations[0]!.target.channelId === aliceTarget.channelId ? "alice" : "bob";
    expect(confirmations[0]!.text).toContain(winner);

    await Promise.all([
      harness.service.handleInbound(
        inbound({
          messageId: "race-alice-followup",
          ...aliceTarget,
          senderId: "alice",
          text: "alice followup",
        }),
      ),
      harness.service.handleInbound(
        inbound({
          messageId: "race-bob-followup",
          ...bobTarget,
          senderId: "bob",
          text: "bob followup",
        }),
      ),
    ]);
    await harness.workers.waitForPromptCount(1);

    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]?.text).toBe(`${winner} followup`);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an explicit owner takes priority over first-message enrollment", async () => {
  const harness = await createCoreHarness({ ownerId: "configured-owner" });
  const target = { channelId: "dm-explicit-owner", channelType: 1 as const };
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "explicit-attacker-first",
        ...target,
        senderId: "attacker",
        text: "attacker first",
      }),
    );
    expect(harness.workers.prompts).toHaveLength(0);

    const ownerTask = inbound({
      messageId: "explicit-owner-task",
      ...target,
      senderId: "configured-owner",
      text: "configured owner task",
    });
    await harness.service.handleInbound(ownerTask);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["configured owner task"]);
  } finally {
    await closeCoreHarness(harness, { remove: false });
  }

  const restarted = await createCoreHarness({
    root: harness.root,
    bot: harness.bot,
    ownerId: null,
  });
  try {
    await restarted.service.handleInbound(
      inbound({
        messageId: "explicit-attacker-after-config-removal",
        ...target,
        senderId: "attacker",
        text: "attacker after removal",
      }),
    );
    expect(restarted.workers.prompts).toHaveLength(0);

    const ownerTask = inbound({
      messageId: "explicit-owner-after-restart",
      ...target,
      senderId: "configured-owner",
      text: "configured owner after restart",
    });
    await restarted.service.handleInbound(ownerTask);
    await waitForStatus(restarted.service, target, (status) => status.state === "completed");
    expect(restarted.workers.prompts.map(({ text }) => text)).toEqual([
      "configured owner after restart",
    ]);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a failed enrollment confirmation write fails closed and does not consume the message", async () => {
  const harness = await createCoreHarness({ ownerId: null });
  const message = inbound({
    messageId: "enrollment-storage-failure",
    channelId: "dm-storage-failure",
    channelType: 1,
    senderId: "alice",
    text: "storage failure enrollment",
  });
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  database.exec(`
    CREATE TRIGGER reject_enrollment_ack
    BEFORE INSERT ON outbox
    WHEN NEW.channel_id = 'dm-storage-failure' AND NEW.task_id IS NULL
    BEGIN
      SELECT RAISE(ABORT, 'fixture enrollment confirmation rejected');
    END;
  `);
  try {
    await harness.service.handleInbound(message).catch(() => {});

    expect(harness.workers.prompts).toHaveLength(0);
    expect(
      harness.transport.sent.filter(({ target }) => target.channelId === message.channelId),
    ).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "effects.log"))).toBe("");
  } finally {
    database.exec("DROP TRIGGER reject_enrollment_ack");
    database.close();
  }

  await harness.service.handleInbound(message);
  const confirmations = harness.transport.sent.filter(
    ({ target }) => target.channelId === message.channelId,
  );
  expect(confirmations).toHaveLength(1);
  expect(confirmations[0]!.text).toContain("alice");
  expect(harness.workers.prompts).toHaveLength(0);
  await closeCoreHarness(harness);
});
