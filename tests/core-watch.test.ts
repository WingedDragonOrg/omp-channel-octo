import { expect, test } from "bun:test";
import { join } from "node:path";
import { ChannelStore } from "../src/store/index.ts";
import type { WatchCollection, WatchCollector } from "../src/watch/types.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
  type CoreHarness,
  type FakePromptContext,
} from "./fixtures/core-fakes.ts";

const WATCH_CONFIG = {
  enabled: true as const,
  protocol: "openai-decisions" as const,
  baseUrl: "https://watch.example",
  model: "watch-fixture",
  apiKeyEnv: "OMP_CHANNEL_OCTO_WATCH_KEY",
  apiKey: "fixture-watch-key",
};

async function waitForWatchRevocation(store: ChannelStore, botUid: string, id: string) {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    const record = store.getWatch(botUid, id);
    if (record?.state === "stopped") return record;
    const { promise, resolve } = Promise.withResolvers<void>();
    setImmediate(resolve);
    await promise;
  }
  throw new Error("owner DM watch was not revoked");
}

test("watch host tool creates, lists, reads, and stops only in its task chat", async () => {
  const target = { channelId: "group-allowed", channelType: 2 as const };
  let watchId = "";
  let createAfterStop: (() => Promise<string>) | undefined;
  const harness = await createCoreHarness({
    decisions: WATCH_CONFIG,
    watchCollector: async () => ({ kind: "success", stdout: "still waiting" }),
    watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    prompt: async ({ append, promptContext }) => {
      const tool = promptContext?.tools?.find(({ name }) => name === "watch");
      if (!tool) throw new Error("watch host tool missing");
      const signal = new AbortController().signal;
      createAfterStop = () => tool.execute({
        action: "create",
        command: "fixture-watch-command",
        condition: "a post-shutdown watch must be rejected",
        interval: 60,
        timeout: 60,
      }, signal, "native-watch-after-stop");
      const created = JSON.parse(await tool.execute({
        action: "create",
        command: "fixture-watch-command",
        args: ["literal arg"],
        condition: "the fixture condition is satisfied",
        interval: 60,
        timeout: 60,
      }, signal, "native-watch-call")) as { id: string; state: string };
      watchId = created.id;
      const listed = JSON.parse(await tool.execute({ action: "list" }, signal)) as {
        watches: Array<{ id: string }>;
      };
      const detail = JSON.parse(await tool.execute({ action: "get", id: watchId }, signal)) as {
        id: string;
        spec: { condition: string };
      };
      const stopped = JSON.parse(await tool.execute({ action: "stop", id: watchId }, signal)) as {
        watches: Array<{ id: string; state: string }>;
      };
      await append("watch-tool.log", JSON.stringify({ created, listed, detail, stopped }));
    },
  });

  try {
    await harness.service.handleInbound(inbound({
      messageId: "watch-tool-request",
      ...target,
      text: "请持续关注直到 fixture condition 成立",
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");

    expect(watchId).not.toBe("");
    const result = JSON.parse(await readText(`${harness.root}/tmp/watch-tool.log`)) as {
      created: { id: string; state: string };
      listed: { watches: Array<{ id: string }> };
      detail: { id: string; spec: { condition: string } };
      stopped: { watches: Array<{ id: string; state: string }> };
    };
    expect(result.created).toMatchObject({ id: watchId, state: "waiting" });
    expect(result.listed.watches.map(({ id }) => id)).toContain(watchId);
    expect(result.detail).toMatchObject({
      id: watchId,
      spec: { condition: "the fixture condition is satisfied" },
    });
    expect(result.stopped.watches.find(record => record.id === watchId)).toMatchObject({ id: watchId, state: "stopped" });
    expect(harness.workers.prompts).toHaveLength(1);
    await harness.service.stop();
    expect(createAfterStop).toBeDefined();
    let rejectedAfterStop = false;
    try {
      await createAfterStop!();
    } catch {
      rejectedAfterStop = true;
    }
    expect(rejectedAfterStop).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});


test("a matched watch queues an independent wake and cannot create another watch", async () => {
  const target = { channelId: "group-allowed", channelType: 2 as const };
  const sourceText = "explicit watch request for the fixture condition";
  const secondPrompt = Promise.withResolvers<{ text: string; createRejected: boolean }>();
  let promptCount = 0;
  let collectCount = 0;
  const harness = await createCoreHarness({
    decisions: WATCH_CONFIG,
    watchCollector: async () => {
      collectCount += 1;
      return { kind: "success", stdout: "condition met" };
    },
    watchJudge: async () => ({ kind: "judgment", choice: "satisfied", probability: 0.99 }),
    prompt: async ({ text, promptContext }) => {
      promptCount += 1;
      if (promptCount === 1) {
        const tool = promptContext?.tools?.find(({ name }) => name === "watch");
        if (!tool) throw new Error("watch host tool missing");
        await tool.execute({
          action: "create",
          command: "fixture-watch-command",
          condition: "the fixture condition is satisfied",
          interval: 60,
          timeout: 60,
        }, new AbortController().signal, "native-watch-call");
        return;
      }

      const tool = promptContext?.tools?.find(({ name }) => name === "watch");
      const createRejected = tool === undefined
        ? true
        : await tool.execute({
          action: "create",
          command: "fixture-watch-command",
          condition: "a forbidden self-chain",
          interval: 60,
          timeout: 60,
        }, new AbortController().signal, "wake-native-watch-call").then(() => false, () => true);
      secondPrompt.resolve({ text, createRejected });
    },
  });

  try {
    await harness.service.handleInbound(inbound({
      messageId: "watch-chain-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(2);
    const wake = await secondPrompt.promise;
    expect(wake.text).not.toContain(sourceText);
    expect(wake.text).toContain("the fixture condition is satisfied");
    expect(wake.createRejected).toBe(true);
    expect(collectCount).toBe(1);
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a persisted owner DM watch is revoked before collection after the owner changes", async () => {
  const target = { channelId: "owner-1", channelType: 1 as const };
  let watchId = "";
  const original = await createCoreHarness({
    ownerId: "owner-1",
    decisions: WATCH_CONFIG,
    watchCollector: async () => ({ kind: "success", stdout: "still waiting" }),
    watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    prompt: async ({ promptContext }) => {
      const tool = promptContext?.tools?.find(({ name }) => name === "watch");
      if (!tool) throw new Error("watch host tool missing");
      const created = JSON.parse(await tool.execute({
        action: "create",
        command: "fixture-watch-command",
        condition: "the owner DM condition is satisfied",
        interval: 60,
        timeout: 60,
      }, new AbortController().signal, "owner-dm-watch-call")) as { id: string };
      watchId = created.id;
    },
  });
  let restarted: CoreHarness | undefined;
  try {
    await original.service.handleInbound(inbound({
      ...target,
      messageId: "owner-dm-watch-source",
      senderId: "owner-1",
      text: "please keep watching this condition",
    }));
    await original.workers.waitForPromptCount(1);
    await waitForStatus(original.service, target, status => status.state === "completed");
    expect(watchId).not.toBe("");

    await closeCoreHarness(original, { remove: false });
    let revokedOwnerCollectorCalls = 0;
    restarted = await createCoreHarness({
      root: original.root,
      ownerId: "owner-2",
      decisions: WATCH_CONFIG,
      watchCollector: async () => {
        revokedOwnerCollectorCalls += 1;
        return { kind: "success", stdout: "stale owner evidence" };
      },
      watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    });
    const store = await ChannelStore.open(join(restarted.stateDir, "channel.sqlite"));
    try {
      const record = await waitForWatchRevocation(store, restarted.bot.uid, watchId);
      expect(record.stopReason).toBe("permission-revoked");
      expect(revokedOwnerCollectorCalls).toBe(0);
    } finally {
      store.close();
    }
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    else await closeCoreHarness(original);
  }
});
