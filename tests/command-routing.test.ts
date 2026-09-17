import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import type {
  ChannelCommandContext,
  ChannelCommandHandler,
  ChannelService,
  CoreServiceConfig,
} from "../src/contracts";
import { createChannelService } from "../src/service";
import {
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

interface RoutingFixture {
  root: string;
  config: CoreServiceConfig;
  transport: FakeTransport;
  workers: FakeWorkerFactory;
}

interface RoutingHarness extends RoutingFixture {
  service: ChannelService;
}

async function createRoutingFixture(): Promise<RoutingFixture> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-command-routing-"));
  await mkdir(join(root, "state"), { recursive: true });
  const bot = {
    uid: `bot-command-routing-${basename(root)}`,
    name: "Fixture Bot",
  };
  const transport = new FakeTransport({ bot });
  const workers = new FakeWorkerFactory();
  const config: CoreServiceConfig = {
    ownerId: "owner-1",
    allowedGroupIds: ["group-allowed"],
    stateDir: join(root, "state"),
    homeDir: root,
  };
  return { root, config, transport, workers };
}

async function openRoutingHarness(
  commandHandlers: readonly ChannelCommandHandler[] = [],
): Promise<RoutingHarness> {
  const fixture = await createRoutingFixture();
  const service = await createChannelService({
    config: fixture.config,
    transport: fixture.transport,
    workerFactory: fixture.workers,
    commandHandlers,
  });
  await service.start();
  return { ...fixture, service };
}

async function closeRoutingHarness(harness: RoutingHarness): Promise<void> {
  await harness.service.stop();
  await rm(harness.root, { recursive: true, force: true });
}

test("forwards an unknown slash command to the worker without rewriting its text", async () => {
  const harness = await openRoutingHarness();
  try {
    const message = inbound({
      messageId: "routing-unknown-slash",
      text: "  /compact   --from-user  ",
    });

    await harness.service.handleInbound(message);
    await harness.workers.waitForPromptCount(1);

    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]?.text).toBe(message.text);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("an intercepting handler consumes the command without starting an OMP prompt", async () => {
  const contexts: ChannelCommandContext[] = [];
  const handler: ChannelCommandHandler = {
    name: "  CoMpAcT  ",
    intercept: true,
    async handle(context) {
      contexts.push(context);
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const message = inbound({
      messageId: "routing-intercept",
      text: "/COMPACT --dry-run",
    });

    await harness.service.handleInbound(message);

    expect(contexts).toHaveLength(1);
    expect(contexts[0]).toMatchObject({
      message,
      name: "compact",
      argument: "--dry-run",
    });
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("a non-intercepting handler observes the command while the worker receives the original text", async () => {
  const contexts: ChannelCommandContext[] = [];
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: false,
    async handle(context) {
      contexts.push(context);
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const message = inbound({
      messageId: "routing-observe",
      text: "\t/CoMpAcT   --scope local  ",
    });

    await harness.service.handleInbound(message);
    await harness.workers.waitForPromptCount(1);

    expect(contexts).toHaveLength(1);
    expect(contexts[0]).toMatchObject({
      message,
      name: "compact",
      argument: "--scope local",
    });
    expect(harness.workers.prompts[0]?.text).toBe(message.text);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("a failing non-intercepting handler does not block the original prompt", async () => {
  let calls = 0;
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: false,
    async handle() {
      calls += 1;
      throw new Error("observer failure");
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const message = inbound({
      messageId: "routing-observe-failure",
      text: " /compact   --keep-spaces ",
    });

    await harness.service.handleInbound(message);
    await harness.workers.waitForPromptCount(1);

    expect(calls).toBe(1);
    expect(harness.workers.prompts[0]?.text).toBe(message.text);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("duplicate delivery invokes a non-intercepting handler and task only once", async () => {
  const contexts: ChannelCommandContext[] = [];
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: false,
    async handle(context) {
      contexts.push(context);
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const message = inbound({
      messageId: "routing-duplicate",
      text: "/compact --once",
    });

    await harness.service.handleInbound(message);
    await harness.service.handleInbound({ ...message });
    await harness.workers.waitForPromptCount(1);

    expect(contexts).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]?.text).toBe(message.text);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("an intercepting handler failure stays retryable without starting OMP", async () => {
  let calls = 0;
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: true,
    async handle() {
      calls += 1;
      if (calls === 1) throw new Error("temporary handler failure");
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const message = inbound({
      messageId: "routing-intercept-retry",
      text: "/compact --retry",
    });

    await harness.service.handleInbound(message).catch(() => undefined);
    expect(harness.workers.prompts).toHaveLength(0);

    await harness.service.handleInbound({ ...message });

    expect(calls).toBe(2);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("built-in commands remain hard-intercepted and never reach the worker", async () => {
  const harness = await openRoutingHarness();
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "routing-built-in-status", text: "/STATUS" }),
    );

    expect(harness.workers.prompts).toHaveLength(0);
    await waitForStatus(
      harness.service,
      { channelId: "dm-owner", channelType: 1 },
      (status) => status.state === "none",
    );
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("custom handlers cannot bypass owner, group, mention, or message-id validation", async () => {
  const contexts: ChannelCommandContext[] = [];
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: false,
    async handle(context) {
      contexts.push(context);
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const rejected = [
      inbound({
        messageId: "routing-denied-dm",
        senderId: "other-user",
        text: "/compact denied",
      }),
      inbound({
        messageId: "routing-denied-group-member",
        channelId: "group-allowed",
        channelType: 2,
        senderId: "other-user",
        mentionedUserIds: [harness.transport.bot.uid],
        text: "/compact denied",
      }),
      inbound({
        messageId: "routing-denied-group-id",
        channelId: "group-denied",
        channelType: 2,
        mentionedUserIds: [harness.transport.bot.uid],
        text: "/compact denied",
      }),
      inbound({
        messageId: "routing-denied-group-mention",
        channelId: "group-allowed",
        channelType: 2,
        text: "/compact denied",
      }),
      inbound({
        messageId: " ",
        text: "/compact denied",
      }),
    ];
    for (const message of rejected) {
      await harness.service.handleInbound(message);
    }

    expect(contexts).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("a member's command earns one throttled notice and never reaches a handler, the worker or a task", async () => {
  const contexts: ChannelCommandContext[] = [];
  const handler: ChannelCommandHandler = {
    name: "compact",
    intercept: false,
    async handle(context) {
      contexts.push(context);
    },
  };
  const harness = await openRoutingHarness([handler]);
  try {
    const first = inbound({
      messageId: "routing-member-command-1",
      channelId: "group-allowed",
      channelType: 2,
      senderId: "other-user",
      mentionedUserIds: [harness.transport.bot.uid],
      text: "/compact denied",
    });
    await harness.service.handleInbound(first);
    const notice = await harness.transport.waitForRemoteText(({ text }) => text.includes("命令仅 owner 可用"));
    expect(notice.target).toMatchObject({ channelId: "group-allowed", channelType: 2 });

    // The window is open for this chat: a second command and a replay of the first stay silent.
    await harness.service.handleInbound(inbound({
      messageId: "routing-member-command-2",
      channelId: "group-allowed",
      channelType: 2,
      senderId: "other-user",
      mentionedUserIds: [harness.transport.bot.uid],
      text: "/status",
    }));
    await harness.service.handleInbound({ ...first });

    expect(contexts).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(
      harness.transport.sent.filter(({ text }) => text.includes("命令仅 owner 可用")),
    ).toHaveLength(1);
    expect((await harness.service.status({ channelId: "group-allowed", channelType: 2 })).state).toBe("none");
  } finally {
    await closeRoutingHarness(harness);
  }
});

test("handler registration rejects blank names and normalized duplicates", async () => {
  const fixture = await createRoutingFixture();
  try {
    const baseOptions = {
      config: fixture.config,
      transport: fixture.transport,
      workerFactory: fixture.workers,
    };
    await expect(
      createChannelService({
        ...baseOptions,
        commandHandlers: [{ name: "  ", intercept: true, handle: async () => {} }],
      }),
    ).rejects.toThrow();
    await expect(
      createChannelService({
        ...baseOptions,
        commandHandlers: [
          { name: "Compact", intercept: true, handle: async () => {} },
          { name: " compact ", intercept: false, handle: async () => {} },
        ],
      }),
    ).rejects.toThrow();
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});
