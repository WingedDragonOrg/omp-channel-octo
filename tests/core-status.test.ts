import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes.ts";
import type { SentCard } from "./fixtures/core-fakes.ts";

const OWNER_DM: ChatTarget = { channelType: 1, channelId: "dm-owner" };

function onlyStatusCard(transport: FakeTransport): SentCard {
  const statusCards = transport.sentCards.filter(({ frame }) => frame.profile === "octo/v1");
  expect(statusCards).toHaveLength(1);
  return statusCards[0]!;
}

function visibleText(card: SentCard): string {
  const texts: string[] = [card.frame.plain];
  const visit = (value: unknown): void => {
    if (Array.isArray(value)) {
      for (const child of value) visit(child);
      return;
    }
    if (!value || typeof value !== "object") return;
    const node = value as Record<string, unknown>;
    if (typeof node.text === "string") texts.push(node.text);
    for (const child of Object.values(node)) visit(child);
  };
  visit(card.frame.card);
  return texts.join("\n");
}

async function expectStatusTextFallback(transport: FakeTransport, messageId: string): Promise<void> {
  const harness = await createCoreHarness({ transport });
  try {
    await harness.service.submitInbound(inbound({ ...OWNER_DM, messageId, text: "/status" }));
    const reply = await transport.waitForRemoteText(({ target, text }) =>
      target.channelType === OWNER_DM.channelType
      && target.channelId === OWNER_DM.channelId
      && text.includes("session=")
      && text.includes("unknown="),
    );
    expect(reply.text).toContain("state=none");
    expect(transport.sentCards).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
}

test("/status answers during a live turn with the current fields and without a probe worker", async () => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append }) => {
      entered.resolve();
      await release.promise;
      await append("status-turn.txt", "done\n");
    },
  });
  const store = await openChannelStore(harness.stateDir);
  let safetyRelease: ReturnType<typeof setTimeout> | undefined;
  try {
    store.setChatMode(harness.bot.uid, OWNER_DM, "queue");
    store.setChatModel(harness.bot.uid, OWNER_DM, "fixture/fast");
    harness.workers.contextUsage = {
      tokens: 4_096,
      contextWindow: 8_192,
      percent: 50,
      model: "fixture/context:think",
    };
    safetyRelease = setTimeout(() => release.resolve(), 2_000);
    await harness.service.submitInbound(inbound({
      ...OWNER_DM,
      messageId: "status-live-task",
      text: "hold this turn while status is read",
    }));
    await entered.promise;
    const before = await harness.service.status(OWNER_DM);

    await harness.service.submitInbound(inbound({
      ...OWNER_DM,
      messageId: "status-live-command",
      text: "/status",
    }));

    await transport.waitForCardSend(({ target, frame }) =>
      target.channelType === OWNER_DM.channelType
      && target.channelId === OWNER_DM.channelId
      && frame.profile === "octo/v1",
    );
    const card = onlyStatusCard(transport);
    expect(card.frame.profile).toBe("octo/v1");
    const reply = visibleText(card);
    expect(reply).toContain(`session=${before.sessionId}`);
    expect(reply).toContain(`cwd=${before.cwd}`);
    expect(reply).toContain("state=running");
    expect(reply).toContain("goal=none");
    expect(reply).toContain("mode=queue");
    expect(reply).toContain("model=fixture/fast");
    expect(reply).toContain("liveModel=");
    expect(reply).toContain("liveModel=fixture/context:think");
    expect(reply).toContain("context=4.1k / 8.2k · 50%");
    expect(reply).toContain("contextSource=live");
    expect(reply).toContain(`queued=${before.queued}`);
    expect(reply).toContain(`outbox=${before.outbox}`);
    expect(reply).toContain(`unknown=${before.unknownOutbox}`);
    expect(harness.workers.creations).toHaveLength(1);
    expect(harness.workers.workers[0]?.contextUsageReads).toBe(1);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(await waitForStatus(harness.service, OWNER_DM, status => status.state === "running")).toMatchObject({
      sessionId: before.sessionId,
      pendingQuestionIds: [],
    });
    release.resolve();
    const completed = await waitForStatus(
      harness.service,
      OWNER_DM,
      status => status.state === "completed",
    );
    expect(completed.sessionId).toBe(before.sessionId);
    expect(await readText(join(harness.root, "tmp", "status-turn.txt"))).toBe("done\n");
  } finally {
    if (safetyRelease !== undefined) clearTimeout(safetyRelease);
    release.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("/status falls back to the existing outbox text when cards are unsupported", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = false;
  await expectStatusTextFallback(transport, "status-unsupported-card");
});

test("/status falls back to the existing outbox text when card delivery fails", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  transport.cardSendFailure = new Error("fixture status-card failure");
  await expectStatusTextFallback(transport, "status-failed-card");
});

test("/status preserves pending question ids and does not consume the pending ask", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      await ask.execute({ questions: [{ id: "status-question", question: "Which environment?" }] }, new AbortController().signal);
    },
  });
  try {
    await harness.service.submitInbound(inbound({ messageId: "status-ask-task", text: "ask me" }));
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      OWNER_DM,
      status => status.pendingQuestionIds.length === 1,
    );
    const [questionId] = pending.pendingQuestionIds;
    if (!questionId) throw new Error("pending question id was not recorded");

    await harness.service.submitInbound(inbound({ messageId: "status-during-ask", text: "/status" }));
    await transport.waitForCardSend(({ frame }) => frame.profile === "octo/v1");

    const reply = visibleText(onlyStatusCard(transport));
    expect(reply).toContain(questionId);
    expect(await harness.service.status(OWNER_DM)).toMatchObject({ pendingQuestionIds: [questionId] });
    expect(harness.workers.creations).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);

    await harness.service.submitInbound(inbound({ messageId: "status-answer-ask", text: "production" }));
    await waitForStatus(harness.service, OWNER_DM, status => status.pendingQuestionIds.length === 0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/status remains owner-only in an allowed group", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  try {
    await harness.service.submitInbound(inbound({
      ...target,
      messageId: "status-from-member",
      senderId: "member-1",
      mentionedUserIds: [harness.bot.uid],
      text: "/status",
    }));
    await transport.waitForRemoteText(({ text }) => text.includes("命令仅 owner 可用"));

    expect(transport.sentCards).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
    expect((await harness.service.status(target)).state).toBe("none");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("duplicate /status delivery sends one status card", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const message = inbound({ messageId: "status-duplicate", text: "/status" });
  try {
    await harness.service.submitInbound(message);
    await harness.service.submitInbound({ ...message });
    await transport.waitForCardSend(({ frame }) => frame.profile === "octo/v1");

    expect(onlyStatusCard(transport).frame.profile).toBe("octo/v1");
    expect(harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
