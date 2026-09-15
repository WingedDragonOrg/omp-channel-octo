import { expect, test } from "bun:test";
import { join } from "node:path";
import type { ChatTarget, RestartPort } from "../src/contracts";
import { openChannelStore } from "../src/store";
import {
  closeCoreHarness,
  type CoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

function recordingPort(): { port: RestartPort; calls: () => number } {
  let calls = 0;
  return {
    port: {
      request: async (): Promise<void> => {
        calls += 1;
      },
    },
    calls: () => calls,
  };
}

const textsFor = (harness: CoreHarness, target: ChatTarget): string[] =>
  harness.transport.sent
    .filter(({ target: sent }) => sent.channelId === target.channelId && sent.channelType === target.channelType)
    .map(({ text }) => text);

test("an unconfigured restart command is refused instead of restarting", async () => {
  const harness = await createCoreHarness();
  const target: ChatTarget = { channelId: "dm-restart-unconfigured", channelType: 1 };
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "restart-unconfigured", ...target, text: "/restart" }),
    );
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    expect(textsFor(harness, target).some((text) => text.includes("未配置 restart.command"))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a restart request is refused while another chat still has a running task", async () => {
  const gate = Promise.withResolvers<void>();
  const busy: ChatTarget = { channelId: "dm-restart-busy", channelType: 1 };
  const requester: ChatTarget = { channelId: "dm-restart-requester", channelType: 1 };
  const { port, calls } = recordingPort();
  const harness = await createCoreHarness({
    restart: port,
    prompt: async () => {
      await gate.promise;
    },
  });
  try {
    await harness.service.handleInbound(inbound({ messageId: "restart-busy-task", ...busy, text: "hold" }));
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({ messageId: "restart-while-busy", ...requester, text: "/restart" }),
    );
    await waitForStatus(harness.service, requester, (status) => status.outbox === 0);
    const refusal = textsFor(harness, requester).join("\n");
    expect(refusal).toContain("1 个会话");
    expect(refusal).toContain("/restart now");
    expect(calls()).toBe(0);
    expect(harness.transport.sent.some(({ text }) => text.includes("正在重启"))).toBe(false);
  } finally {
    gate.resolve();
    await closeCoreHarness(harness);
  }
});

test("a forced restart runs the deployment command and the next start reports it back", async () => {
  const gate = Promise.withResolvers<void>();
  const busy: ChatTarget = { channelId: "dm-restart-force-busy", channelType: 1 };
  const requester: ChatTarget = { channelId: "dm-restart-force-requester", channelType: 1 };
  const { port, calls } = recordingPort();
  const first = await createCoreHarness({
    restart: port,
    prompt: async () => {
      await gate.promise;
    },
  });
  try {
    await first.service.handleInbound(inbound({ messageId: "restart-force-task", ...busy, text: "hold" }));
    await first.workers.waitForPromptCount(1);
    await first.service.handleInbound(
      inbound({ messageId: "restart-force-command", ...requester, text: "/restart now" }),
    );
    await waitForStatus(first.service, requester, (status) => status.outbox === 0);
    expect(calls()).toBe(1);
    expect(textsFor(first, requester).join("\n")).toContain("正在重启 channel");
  } finally {
    gate.resolve();
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await waitForStatus(restarted.service, requester, (status) => status.outbox === 0);
    await waitForStatus(restarted.service, busy, (status) => status.outbox === 0);
    expect(textsFor(restarted, requester).join("\n")).toContain("channel 已重启完成");
    expect(textsFor(restarted, busy).join("\n")).toContain("channel 正在重启，本会话任务已中断");
    expect(textsFor(restarted, busy).join("\n")).toContain("channel 已重启完成");
  } finally {
    await closeCoreHarness(restarted, { remove: false });
  }

  const again = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    expect(textsFor(again, requester)).toEqual([]);
  } finally {
    await closeCoreHarness(again);
  }
});

test("a restart notice older than the delivery window is dropped", async () => {
  const target: ChatTarget = { channelId: "dm-restart-stale", channelType: 1 };
  const first = await createCoreHarness({ restart: recordingPort().port });
  const botUid = first.bot.uid;
  await closeCoreHarness(first, { remove: false });

  const store = await openChannelStore(join(first.root, "state"));
  try {
    store.addRestartNotice({
      id: "stale-notice",
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      requestedAt: Date.now() - 11 * 60 * 1_000,
    });
  } finally {
    store.close();
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    expect(textsFor(restarted, target)).toEqual([]);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a task that never started reports the restart on the next start", async () => {
  const target: ChatTarget = { channelId: "dm-restart-queued", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });

  const store = await openChannelStore(join(first.root, "state"));
  try {
    store.acceptTask(
      {
        botUid: first.bot.uid,
        target,
        messageId: "queued-message",
        senderId: "owner-1",
        text: "queued work",
        supported: true,
        kind: "task",
      },
      {
        id: "queued-task",
        botUid: first.bot.uid,
        channelType: target.channelType,
        channelId: target.channelId,
        messageId: "queued-message",
        senderId: "owner-1",
        text: "queued work",
        sessionId: "session-queued",
        cwd: first.root,
        state: "queued",
        resultText: null,
        errorText: null,
        messageSeq: 0,
        historyEligible: true,
      },
    );
  } finally {
    store.close();
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await waitForStatus(restarted.service, target, (status) => status.outbox === 0);
    const texts = textsFor(restarted, target).join("\n");
    expect(texts).toContain("服务已重启完成");
    expect(texts).toContain("queued-task 因重启中断");
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("restart arguments other than now are rejected with usage", async () => {
  const target: ChatTarget = { channelId: "dm-restart-usage", channelType: 1 };
  const { port, calls } = recordingPort();
  const harness = await createCoreHarness({ restart: port });
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "restart-usage", ...target, text: "/restart maybe" }),
    );
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    expect(textsFor(harness, target).join("\n")).toContain("用法：/restart [now]");
    expect(calls()).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
