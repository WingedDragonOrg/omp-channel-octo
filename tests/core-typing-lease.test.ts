import { expect, test, vi } from "bun:test";
import type {
  AttachmentDownload,
  BotIdentity,
  ChatTarget,
  InboundMessage,
  OmpWorker,
  WorkerOptions,
} from "../src/contracts.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";

/**
 * typing 租约行为测试：门后即亮、组装/排队期间续期、终态与拒绝路径熄灭。
 * 两类证据：跨组件相对顺序（typing 在 worker 创建/附件下载之前）与续期间的精确计数
 * （每个聊天任一时刻只有一条 3 秒续期环）。
 */

class OrderTransport extends FakeTransport {
  /** When set, `downloadAttachment` blocks on this promise before writing the fixture body. */
  downloadGate: Promise<void> | undefined;

  constructor(
    readonly events: string[],
    options: { bot: BotIdentity },
  ) {
    super(options);
  }

  override async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    await super.sendTyping(target, signal);
    this.events.push("typing");
  }

  override async downloadAttachment(request: AttachmentDownload): Promise<number> {
    this.events.push("download");
    if (this.downloadGate) await this.downloadGate;
    return super.downloadAttachment(request);
  }
}

class OrderWorkerFactory extends FakeWorkerFactory {
  constructor(
    private readonly events: string[],
    prompt?: FakePrompt,
  ) {
    super(prompt);
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    this.events.push("worker");
    return super.create(options);
  }
}

function makeGate(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

/** Every prompt parks on its own gate; `release(i)` finishes prompt i+1 in creation order. */
function makeGatedPrompt(): {
  prompt: FakePrompt;
  release: (index: number) => void;
} {
  const gates: Array<{ promise: Promise<void>; resolve: () => void }> = [];
  return {
    prompt: async () => {
      const gate = makeGate();
      gates.push(gate);
      await gate.promise;
    },
    release: (index) => gates[index]?.resolve(),
  };
}

function imageAttachment(url: string): InboundMessage["attachments"][number] {
  return { url, kind: "image", name: "img.png", size: 0, durationSeconds: 0 };
}

const IMAGE_BYTES = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3]);

async function waitForEvent(events: string[], name: string): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    if (events.includes(name)) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  throw new Error(`event ${name} not observed`);
}

test("typing fires at task acceptance, before worker creation", async () => {
  const events: string[] = [];
  const gated = makeGatedPrompt();
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-order", name: "Fixture Bot" } }),
    workers: new OrderWorkerFactory(events, gated.prompt),
  });
  const chat = inbound({ channelId: "dm-typing-order", text: "do some work" });
  try {
    await harness.service.handleInbound(chat);
    await harness.workers.waitForPromptCount(1);
    const typingIndex = events.indexOf("typing");
    const workerIndex = events.indexOf("worker");
    expect(typingIndex).toBeGreaterThanOrEqual(0);
    expect(workerIndex).toBeGreaterThan(typingIndex);
    expect(harness.transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    gated.release(0);
    await waitForStatus(harness.service, chat, (status) => status.state === "completed");
  } finally {
    gated.release(0);
    await closeCoreHarness(harness);
  }
});

test("typing renews while an attachment download is still in flight", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const transport = new OrderTransport(events, { bot: { uid: "bot-att", name: "Fixture Bot" } });
  const harness = await createCoreHarness({
    transport,
    attachments: { enabled: true, maxBytes: 1_048_576 },
  });
  const url = "https://files.example/img.png";
  transport.attachmentBodies.set(url, IMAGE_BYTES);
  const gate = makeGate();
  transport.downloadGate = gate.promise;
  const chat = inbound({
    channelId: "dm-typing-attachment",
    text: "看看这张图",
    attachments: [imageAttachment(url)],
  });
  try {
    const handled = harness.service.handleInbound(chat);
    await waitForEvent(events, "download");
    expect(events.indexOf("typing")).toBeGreaterThan(0 - 1);
    expect(events.indexOf("typing")).toBeLessThan(events.indexOf("download"));
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    const before = transport.typingCalls.length;
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    expect(transport.typingCalls.length).toBe(before + 1);
    transport.downloadGate = undefined;
    gate.resolve();
    await handled;
    await waitForStatus(harness.service, chat, (status) => status.state === "completed");
  } finally {
    gate.resolve();
    transport.downloadGate = undefined;
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("refused attachments release the message lease with no renewal leak", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-refusal", name: "Fixture Bot" } }),
    attachments: { enabled: true, maxBytes: 1_048_576 },
  });
  const chat = inbound({
    channelId: "dm-typing-refusal",
    text: "太多附件",
    attachments: Array.from({ length: 9 }, (_, index) => imageAttachment(`https://files.example/${index}.png`)),
  });
  try {
    await harness.service.handleInbound(chat);
    expect(harness.transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    expect(harness.workers.creations).toHaveLength(0);
    const count = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(9_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(count);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("commands, unsupported payloads, and member command notices never type", async () => {
  const events: string[] = [];
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-quiet", name: "Fixture Bot" } }),
  });
  try {
    await harness.service.handleInbound(inbound({ channelId: "dm-quiet-a", text: "/status" }));
    await harness.service.handleInbound({ ...inbound({ channelId: "dm-quiet-b" }), supported: false });
    await harness.service.handleInbound(
      inbound({
        channelType: 2,
        channelId: "group-allowed",
        senderId: "member-1",
        text: "/status",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.service.handleInbound(inbound({ channelId: "dm-quiet-c", text: "/queue" }));
    expect(harness.transport.typingCalls).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a steered message keeps one renewal loop running and it stops at completion", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const gated = makeGatedPrompt();
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-steer", name: "Fixture Bot" } }),
    workers: new OrderWorkerFactory(events, gated.prompt),
  });
  const chat = inbound({ channelId: "dm-typing-steer", text: "first message" });
  try {
    await harness.service.handleInbound(chat);
    await harness.workers.waitForPromptCount(1);
    const beforeSteer = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(beforeSteer + 1);
    await harness.service.handleInbound({ ...chat, messageId: "steer-2", text: "second message" });
    const afterSteer = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    // 合并进运行中的回合后：消息租约释放，仍只有运行任务的一条环在续期。
    expect(harness.transport.typingCalls).toHaveLength(afterSteer + 1);
    gated.release(0);
    await waitForStatus(harness.service, chat, (status) => status.state === "completed");
    const done = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(9_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(done);
  } finally {
    gated.release(0);
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a queued task stays typed through the wait on exactly one loop until its terminal state", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const gated = makeGatedPrompt();
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-queue", name: "Fixture Bot" } }),
    workers: new OrderWorkerFactory(events, gated.prompt),
  });
  const chat = inbound({ channelId: "dm-typing-queue", text: "/mode queue" });
  try {
    await harness.service.handleInbound(chat);
    await harness.service.handleInbound({ ...chat, messageId: "queue-1", text: "first task" });
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound({ ...chat, messageId: "queue-2", text: "second task" });
    const waiting = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    // 运行中的任务加上排队中的任务同属一条环：3 秒恰好一次续期。
    expect(harness.transport.typingCalls).toHaveLength(waiting + 1);
    gated.release(0);
    await harness.workers.waitForPromptCount(2);
    gated.release(1);
    await waitForStatus(harness.service, chat, (status) => status.state === "completed" && status.queued === 0);
    const done = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(9_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(done);
  } finally {
    gated.release(0);
    gated.release(1);
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("cancelling a queued task releases its lease through the task-state guard", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const gated = makeGatedPrompt();
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-cancel", name: "Fixture Bot" } }),
    workers: new OrderWorkerFactory(events, gated.prompt),
  });
  const chat = inbound({ channelId: "dm-typing-cancel", text: "/mode queue" });
  try {
    await harness.service.handleInbound(chat);
    await harness.service.handleInbound({ ...chat, messageId: "cancel-1", text: "first task" });
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound({ ...chat, messageId: "cancel-2", text: "second task" });
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    await harness.service.handleInbound({ ...chat, messageId: "cancel-3", text: "/cancel" });
    await waitForStatus(harness.service, chat, (status) => status.state === "cancelled" && status.queued === 0);
    const settled = harness.transport.typingCalls.length;
    // 运行任务的租约在 finally 释放；被取消的排队任务由 tick 兜底回收——不超两个间隔。
    vi.advanceTimersByTime(6_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(settled);
  } finally {
    gated.release(0);
    gated.release(1);
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a duplicate intercepting command delivery leaves no lease behind", async () => {
  vi.useFakeTimers();
  const events: string[] = [];
  const harness = await createCoreHarness({
    transport: new OrderTransport(events, { bot: { uid: "bot-intercept", name: "Fixture Bot" } }),
    commandHandlers: [{ name: "probe", intercept: true, handle: () => {} }],
  });
  const chat = inbound({ channelId: "dm-typing-intercept", text: "/probe run" });
  try {
    await harness.service.handleInbound(chat);
    await harness.service.handleInbound(chat);
    expect(harness.transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    expect(harness.workers.creations).toHaveLength(0);
    const count = harness.transport.typingCalls.length;
    vi.advanceTimersByTime(9_000);
    await Promise.resolve();
    expect(harness.transport.typingCalls).toHaveLength(count);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
