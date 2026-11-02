import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, SendReceipt } from "../src/contracts.ts";
import { createOutboxDelivery } from "../src/outbox.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

interface RecordedTextDelivery {
  operation: "send" | "edit";
  target: ChatTarget;
  text: string;
  clientMsgNo: string | null;
  messageId: string | null;
  mention: unknown;
}


/** Observes the structured data passed at the ChannelTransport boundary, then uses real fake delivery. */
class MentionRecordingTransport extends FakeTransport {
  readonly deliveries: RecordedTextDelivery[] = [];

  override async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
    mention?: unknown,
  ): Promise<SendReceipt> {
    this.deliveries.push({
      operation: "send",
      target: { ...target },
      text,
      clientMsgNo,
      messageId: null,
      mention,
    });
    return super.sendText(target, text, clientMsgNo, signal);
  }

  override async editText(
    target: ChatTarget,
    messageId: string,
    text: string,
    signal?: AbortSignal,
    mention?: unknown,
  ): Promise<void> {
    this.deliveries.push({
      operation: "edit",
      target: { ...target },
      text,
      clientMsgNo: null,
      messageId,
      mention,
    });
    return super.editText(target, messageId, text, signal);
  }
}


async function flushPromises(): Promise<void> {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    await Promise.resolve();
  }
}

function sameTarget(actual: ChatTarget, expected: ChatTarget): boolean {
  return actual.channelType === expected.channelType && actual.channelId === expected.channelId;
}

function deliveriesFor(
  transport: MentionRecordingTransport,
  target: ChatTarget,
): RecordedTextDelivery[] {
  return transport.deliveries.filter((delivery) => sameTarget(delivery.target, target));
}

test("ordinary replies convert split member markers as they arrive and keep streaming one message", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-ordinary-mentions", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const literalPrefix = [
    "前文🙂：普通 @姓名，",
    "`@[member-inline:行内]` ",
    "\n```ts\n@[member-fence:围栏]\n```\n",
    "\\@[member-escaped:转义] ",
    "非法 @[not-a-marker] 与 @[member-empty:] ",
  ].join("");
  const draftFirstMarker = "@[member-old:小明]";
  const finalFirstMarker = "@[member-a:小明]";
  const escapedMarker = "@[member-b:张" + String.raw`\\\]` + "三]";
  const escapedMarkerPrefix = "@[member-b:张" + String.raw`\\\]`;
  const draftTail = `${draftFirstMarker}，又 ${escapedMarker}🙂，还有 @[member-c:Nova 🚀] 与 ${draftFirstMarker}`;
  const finalTail = `${finalFirstMarker}，又 ${escapedMarker}🙂，还有 @[member-c:Nova 🚀] 与 ${finalFirstMarker}`;
  const finalBody = `${literalPrefix}${finalTail}`;
  const visibleTail = "@小明，又 @张\\]三🙂，还有 @Nova 🚀 与 @小明";
  const task = inbound({
    messageId: "ordinary-member-mention-stream",
    channelId: "group-allowed",
    channelType: 2,
    text: "reply with the selected members",
    mentionedUserIds: [bot.uid],
  });
  const continueChunks = Promise.withResolvers<void>();
  const continueAfterFirstMarker = Promise.withResolvers<void>();
  const allowFinal = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-member-mention", `${literalPrefix}@[`, false));
      await continueChunks.promise;
      emit(assistantText("assistant-member-mention", `${literalPrefix}@[member-old:`, false));
      emit(assistantText("assistant-member-mention", `${literalPrefix}${draftFirstMarker}，又 @[member-b:`, false));
      await continueAfterFirstMarker.promise;
      emit(
        assistantText(
          "assistant-member-mention",
          `${literalPrefix}${draftFirstMarker}，又 ${escapedMarkerPrefix}`,
          false,
        ),
      );
      emit(
        assistantText(
          "assistant-member-mention",
          `${literalPrefix}${draftFirstMarker}，又 ${escapedMarker}🙂，还有 @[member-c:Nova `,
          false,
        ),
      );
      emit(assistantText("assistant-member-mention", `${literalPrefix}${draftTail}`, false));
      await allowFinal.promise;
      emit(assistantText("assistant-member-mention", finalBody, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await flushPromises();
    const firstMessage = transport.listRemoteText(task)[0];
    expect(firstMessage?.text).toBe(literalPrefix);
    expect(transport.textRequests.filter((request) => sameTarget(request.target, task))).toHaveLength(1);


    continueChunks.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([`${literalPrefix}@小明，又 `]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      text: `${literalPrefix}@小明，又 `,
      messageId: firstMessage?.messageId,
      mention: {
        uids: ["member-old"],
        entities: [{ uid: "member-old", offset: literalPrefix.length, length: 3 }],
      },
    });

    continueAfterFirstMarker.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([
      `${literalPrefix}${visibleTail}`,
    ]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      text: `${literalPrefix}${visibleTail}`,
      messageId: firstMessage?.messageId,
      mention: {
        uids: ["member-old", "member-b", "member-c"],
        entities: [
          { uid: "member-old", offset: literalPrefix.length, length: 3 },
          { uid: "member-b", offset: literalPrefix.length + 6, length: 5 },
          { uid: "member-c", offset: literalPrefix.length + 17, length: 8 },
          { uid: "member-old", offset: literalPrefix.length + 28, length: 3 },
        ],
      },
    });

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.visibleText(task)).toBe(`${literalPrefix}${visibleTail}`);
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([
      `${literalPrefix}${visibleTail}`,
    ]);
    const requests = transport.textRequests.filter((request) => sameTarget(request.target, task));
    expect(requests.filter(({ operation }) => operation === "send").map(({ text }) => text)).toEqual([
      literalPrefix,
    ]);
    const edits = requests.filter(({ operation }) => operation === "edit");
    expect(edits.map(({ text }) => text)).toEqual([
      `${literalPrefix}@小明，又 `,
      `${literalPrefix}${visibleTail}`,
      `${literalPrefix}${visibleTail}`,
    ]);
    expect(edits.every(({ messageId }) => messageId === firstMessage?.messageId)).toBe(true);

    const deliveries = deliveriesFor(transport, task);
    expect(deliveries.map(({ operation, text, mention }) => ({ operation, text, mention }))).toEqual([
      { operation: "send", text: literalPrefix, mention: undefined },
      {
        operation: "edit",
        text: `${literalPrefix}@小明，又 `,
        mention: {
          uids: ["member-old"],
          entities: [{ uid: "member-old", offset: literalPrefix.length, length: 3 }],
        },
      },
      {
        operation: "edit",
        text: `${literalPrefix}${visibleTail}`,
        mention: {
          uids: ["member-old", "member-b", "member-c"],
          entities: [
            { uid: "member-old", offset: literalPrefix.length, length: 3 },
            { uid: "member-b", offset: literalPrefix.length + 6, length: 5 },
            { uid: "member-c", offset: literalPrefix.length + 17, length: 8 },
            { uid: "member-old", offset: literalPrefix.length + 28, length: 3 },
          ],
        },
      },
      {
        operation: "edit",
        text: `${literalPrefix}${visibleTail}`,
        mention: {
          uids: ["member-a", "member-b", "member-c"],
          entities: [
            { uid: "member-a", offset: literalPrefix.length, length: 3 },
            { uid: "member-b", offset: literalPrefix.length + 6, length: 5 },
            { uid: "member-c", offset: literalPrefix.length + 17, length: 8 },
            { uid: "member-a", offset: literalPrefix.length + 28, length: 3 },
          ],
        },
      },
    ]);
  } finally {
    continueChunks.resolve();
    allowFinal.resolve();
    continueAfterFirstMarker.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("markers inside an unfinished inline span or fence stay literal while the stream advances", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-code-stream-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-code-stream", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-code-stream" };
  const taskId = "mention-code-stream-task";
  const messageId = "assistant-inline-code-stream";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  const prefix = "确定前文 ";
  const inlineSnapshot = `${prefix}\`@[member-inline-code:隐藏] secret`;
  const inlineFinal = `${inlineSnapshot}\``;
  const fencePrefix = "围栏前文";
  const fenceSnapshot = `${fencePrefix}\n\`\`\`ts\n@[member-fence-code:隐藏]\nconst value = 1;`;
  const fenceFinal = `${fenceSnapshot}\n\`\`\``;
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: prefix,
      final: false,
    }, { parseMentions: true });
    const first = await transport.waitForRemoteText((message) => sameTarget(message.target, target));
    expect(first.text).toBe(prefix);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: inlineSnapshot,
      final: false,
    }, { parseMentions: true });
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();
    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual([inlineSnapshot]);
    expect(deliveriesFor(transport, target).some(({ mention }) => mention !== undefined)).toBe(false);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: inlineFinal,
      final: true,
    }, { parseMentions: true });
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();
    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual([inlineFinal]);
    expect(deliveriesFor(transport, target).some(({ mention }) => mention !== undefined)).toBe(false);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-fenced-code-stream",
      text: fencePrefix,
      final: false,
    }, { parseMentions: true });
    await transport.waitForRemoteText(
      (message) => sameTarget(message.target, target) && message.text === fencePrefix,
    );
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-fenced-code-stream",
      text: fenceSnapshot,
      final: false,
    }, { parseMentions: true });
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();
    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual([inlineFinal, fenceSnapshot]);
    expect(deliveriesFor(transport, target).some(({ mention }) => mention !== undefined)).toBe(false);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-fenced-code-stream",
      text: fenceFinal,
      final: true,
    }, { parseMentions: true });
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();
    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual([inlineFinal, fenceFinal]);
    expect(deliveriesFor(transport, target).some(({ mention }) => mention !== undefined)).toBe(false);
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("ordinary text without a valid member marker keeps the existing create-then-edit stream", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-unmarked-stream", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "ordinary-unmarked-stream",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "continue the ordinary response",
  });
  const allowFinal = Promise.withResolvers<void>();
  const draft = "普通回复🙂 @普通名字";
  const final = "普通回复🙂 @普通名字 已完成";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-unmarked", draft, false));
      await allowFinal.promise;
      emit(assistantText("assistant-unmarked", final, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    const initial = await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task) && message.text === draft,
    );
    const initialDeliveries = deliveriesFor(transport, task);
    expect(initialDeliveries.map(({ operation, text, mention }) => ({ operation, text, mention }))).toEqual([
      { operation: "send", text: draft, mention: undefined },
    ]);

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.visibleText(task)).toBe(final);
    expect(transport.listRemoteText(task)).toHaveLength(1);
    const edits = transport.textRequests.filter(
      (request) => sameTarget(request.target, task) && request.operation === "edit",
    );
    expect(edits).toHaveLength(1);
    expect(edits[0]?.messageId).toBe(initial.messageId);
    expect(edits[0]?.text).toBe(final);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      text: final,
      mention: undefined,
    });
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a restarted outbox resumes a structured mention with the same client message number and no prompt", async () => {
  const bot = { uid: "bot-mention-outbox-restart", name: "Mention Bot" };
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-outbox-restart-")));
  const task = inbound({
    messageId: "mention-outbox-restart",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "send the final member reply",
  });
  const rawBody = "@[member-restart:恢复者] 已完成";
  const visibleBody = "@恢复者 已完成";
  const expectedMention = {
    uids: ["member-restart"],
    entities: [{ uid: "member-restart", offset: 0, length: 4 }],
  };
  try {
    const offline = new MentionRecordingTransport({ bot, online: false, startOnline: false });
    const first = await createCoreHarness({
      root,
      bot,
      transport: offline,
      prompt: async ({ emit }) => {
        emit(assistantText("assistant-mention-restart", rawBody, true));
      },
    });
    try {
      await first.service.handleInbound(task);
      await first.workers.waitForPromptCount(1);
      await waitForStatus(
        first.service,
        task,
        (status) => status.state === "completed" && status.outbox > 0,
      );
    } finally {
      await closeCoreHarness(first, { remove: false });
    }

    const online = new MentionRecordingTransport({ bot });
    const restartedWorkers = new FakeWorkerFactory();
    const restarted = await createCoreHarness({ root, bot, transport: online, workers: restartedWorkers });
    try {
      await waitForStatus(
        restarted.service,
        task,
        (status) => status.state === "completed" && status.outbox === 0,
      );
      expect(restartedWorkers.prompts).toHaveLength(0);

      const failedCreate = offline.deliveries.find(
        ({ operation, text }) => operation === "send" && text === visibleBody,
      );
      const recoveredCreate = deliveriesFor(online, task).find(
        ({ operation, text }) => operation === "send" && text === visibleBody,
      );
      expect(failedCreate?.clientMsgNo).toBeDefined();
      expect(recoveredCreate).toMatchObject({
        text: visibleBody,
        mention: expectedMention,
        clientMsgNo: failedCreate?.clientMsgNo,
      });
      expect(
        online.sent.filter(
          ({ target, text }) => sameTarget(target, task) && text === visibleBody,
        ),
      ).toHaveLength(1);
    } finally {
      await closeCoreHarness(restarted);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("private replies keep structured member markers as literal text", async () => {
  const transport = new MentionRecordingTransport();
  const task = inbound({
    messageId: "private-mention-literal",
    channelId: "dm-mention-literal",
    text: "keep the member marker literal",
  });
  const body = "Text @[member-a:小明] and @[member-b:林] stays literal.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-private-literal", body, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.visibleText(task)).toBe(body);
    expect(deliveriesFor(transport, task)).toContainEqual(
      expect.objectContaining({ operation: "send", text: body, mention: undefined }),
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a leading complete member marker is delivered immediately and duplicate finals do not resend it", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-leading-mention", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "leading-member-mention",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "answer with a leading mention",
  });
  const allowFinal = Promise.withResolvers<void>();
  const rawBody = "@[member-leading:林] 你好";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-leading-mention", rawBody, false));
      await allowFinal.promise;
      emit(assistantText("assistant-leading-mention", rawBody, true));
      emit(assistantText("assistant-leading-mention", rawBody, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    const initial = await transport.waitForRemoteText((message) => sameTarget(message.target, task));
    expect(initial.text).toBe("@林 你好");
    expect(deliveriesFor(transport, task).map(({ operation, text, mention }) => ({
      operation,
      text,
      mention,
    }))).toEqual([{
      operation: "send",
      text: "@林 你好",
      mention: {
        uids: ["member-leading"],
        entities: [{ uid: "member-leading", offset: 0, length: 2 }],
      },
    }]);

    allowFinal.resolve();
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual(["@林 你好"]);
    expect(deliveriesFor(transport, task)).toHaveLength(1);
    expect(transport.textRequests.filter(
      ({ operation, target }) => operation === "send" && sameTarget(target, task),
    )).toHaveLength(1);
    expect(transport.textRequests.filter(
      ({ operation, target }) => operation === "edit" && sameTarget(target, task),
    )).toHaveLength(0);
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a final snapshot that removes a member marker replaces the draft without stale entities", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-mention-removal", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "mention-final-removal",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "correct a streamed member reply",
  });
  const allowFinal = Promise.withResolvers<void>();
  const draft = "前文 @[member-old:小明] 尚未确定的尾段";
  const final = "修正后的普通回复";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-final-removal", draft, false));
      await allowFinal.promise;
      emit(assistantText("assistant-final-removal", final, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    const firstMessage = await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task),
    );
    expect(firstMessage.text).toBe("前文 @小明 尚未确定的尾段");
    expect(deliveriesFor(transport, task)[0]).toMatchObject({
      operation: "send",
      text: "前文 @小明 尚未确定的尾段",
      mention: {
        uids: ["member-old"],
        entities: [{ uid: "member-old", offset: 3, length: 3 }],
      },
    });
    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe(final);
    expect(transport.listRemoteText(task)[0]?.messageId).toBe(firstMessage.messageId);
    const requests = transport.textRequests.filter((request) => sameTarget(request.target, task));
    expect(requests.filter(({ operation }) => operation === "send")).toHaveLength(1);
    expect(requests.filter(({ operation }) => operation === "edit").map(({ text }) => text)).toEqual([final]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      text: final,
      mention: undefined,
    });
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("an empty final clears an already visible draft without creating a blank message", async () => {
  vi.useFakeTimers();
  const transport = new MentionRecordingTransport();
  const task = inbound({
    messageId: "mention-empty-final",
    channelId: "dm-empty-final",
    text: "finish with no text",
  });
  const allowFinal = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-empty-final", "draft", false));
      await allowFinal.promise;
      emit(assistantText("assistant-empty-final", "", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    const initial = await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task),
    );
    expect(initial.text).toBe("draft");

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.listRemoteText(task)[0]?.text).toBe("");
    expect(transport.textRequests.filter(
      (request) => sameTarget(request.target, task) && request.operation === "send",
    )).toHaveLength(1);
    expect(transport.textRequests.filter(
      (request) => sameTarget(request.target, task) && request.operation === "edit",
    ).map(({ text }) => text)).toEqual([""]);
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("an incomplete member marker does not block another assistant message or leak on failure", async () => {
  const bot = { uid: "bot-independent-mentions", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "unfinalized-mention-failure",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "produce two assistant messages",
  });
  const prefix = "已确认的前文 ";
  const unfinished = `${prefix}@[member-late:`;
  const independent = "另一个独立 assistant message";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-unfinished", unfinished, false));
      await transport.waitForRemoteText((message) => sameTarget(message.target, task));
      emit(assistantText("assistant-independent", independent, true));
      throw new Error("fixture task failed before the first message finalized");
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "failed" && status.outbox === 0,
    );

    const texts = transport.listRemoteText(task).map(({ text }) => text);
    expect(texts).toContain(prefix);
    expect(texts).toContain(independent);
    expect(texts.some((text) => text.includes("@[member-late:"))).toBe(false);
    expect(deliveriesFor(transport, task).some(({ mention }) => mention !== undefined)).toBe(false);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("mention atoms stay intact across the byte limit and oversized markers remain literal", async () => {
  const bot = { uid: "bot-mention-segments", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const longMentionBody = `@[member-a:A]${"🙂".repeat(750)}`;
  const longName = "名".repeat(1_000);
  const oversizedMarker = `@[member-long:${longName}]`;
  const firstTask = inbound({
    messageId: "mention-utf8-segments",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "split around an atomic mention",
  });
  const secondTask = inbound({
    messageId: "mention-oversized-literal",
    channelId: "group-oversized",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "preserve the oversized marker",
  });
  const harness = await createCoreHarness({
    bot,
    transport,
    allowedGroupIds: ["group-allowed", "group-oversized"],
    prompt: async ({ text, emit }) => {
      if (text === firstTask.text) {
        emit(assistantText("assistant-utf8-mention", longMentionBody, true));
      } else {
        emit(assistantText("assistant-oversized-mention", oversizedMarker, true));
      }
    },
  });
  try {
    await harness.service.handleInbound(firstTask);
    await waitForStatus(
      harness.service,
      firstTask,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const utf8Segments = transport.listRemoteText(firstTask).map(({ text }) => text);
    expect(utf8Segments).toEqual([`@A${"🙂".repeat(749)}`, "🙂"]);
    expect(utf8Segments.every((text) => Buffer.byteLength(text, "utf8") <= 3_000)).toBe(true);
    expect(deliveriesFor(transport, firstTask).map(({ text, mention }) => ({ text, mention }))).toEqual([
      {
        text: `@A${"🙂".repeat(749)}`,
        mention: {
          uids: ["member-a"],
          entities: [{ uid: "member-a", offset: 0, length: 2 }],
        },
      },
      { text: "🙂", mention: undefined },
    ]);

    await harness.service.handleInbound(secondTask);
    await waitForStatus(
      harness.service,
      secondTask,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const literalSegments = transport.listRemoteText(secondTask).map(({ text }) => text);
    expect(literalSegments.join("")).toBe(oversizedMarker);
    expect(literalSegments.length).toBeGreaterThan(1);
    expect(literalSegments.every((text) => Buffer.byteLength(text, "utf8") <= 3_000)).toBe(true);
    expect(deliveriesFor(transport, secondTask).every(({ mention }) => mention === undefined)).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the final mentioned snapshot is durable before its same-message edit", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-final-commit-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-commit", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-commit" };
  const taskId = "mention-final-commit-task";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  let observedCommittedFinal = false;
  const edit = transport.editText.bind(transport);
  transport.editText = async (peer, messageId, text, signal, mention) => {
    if (text === "final prefix @林 tail") {
      const stream = store.listTextStream(bot.uid, taskId, `${taskId}:assistant-commit-order`);
      expect(stream).toHaveLength(1);
      expect(stream[0]).toMatchObject({
        text: "final prefix @林 tail",
        mention: {
          uids: ["member-a"],
          entities: [{ uid: "member-a", offset: 13, length: 2 }],
        },
        streamFinal: true,
      });
      expect(mention).toEqual({
        uids: ["member-a"],
        entities: [{ uid: "member-a", offset: 13, length: 2 }],
      });
      observedCommittedFinal = true;
    }
    return edit(peer, messageId, text, signal, mention);
  };
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-commit-order",
      text: "draft",
      final: false,
    }, { parseMentions: true });
    const initial = await transport.waitForRemoteText((message) => sameTarget(message.target, target));
    expect(initial.text).toBe("draft");
    await outbox.flush(target);
    await outbox.settle();

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-commit-order",
      text: "final prefix @[member-a:林] tail",
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();

    expect(observedCommittedFinal).toBe(true);
    expect(transport.visibleText(target)).toBe("final prefix @林 tail");
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("non-final stream edits remain separated by the full merge interval", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-edit-gap-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-edit-gap", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-edit-gap" };
  const taskId = "mention-edit-gap-task";
  const messageId = "assistant-edit-gap";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  const edits = () => deliveriesFor(transport, target)
    .filter(({ operation }) => operation === "edit")
    .map(({ text }) => text);
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "draft",
      final: false,
    });
    await transport.waitForRemoteText((message) => sameTarget(message.target, target));
    await outbox.settle();

    vi.advanceTimersByTime(1_000);
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "first update",
      final: false,
    });
    vi.advanceTimersByTime(1);
    await flushPromises();
    await outbox.settle();
    expect(edits()).toEqual(["first update"]);
    const firstEditRequest = transport.textRequests.find(({ operation }) => operation === "edit");
    expect(firstEditRequest?.text).toBe("first update");
    const firstEditDeadline = (firstEditRequest?.startedAt ?? Date.now()) + 1_000;

    vi.advanceTimersByTime(100);
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "second update",
      final: false,
    });
    vi.advanceTimersByTime(0);
    await flushPromises();
    await outbox.settle();
    expect(edits()).toEqual(["first update"]);

    const remaining = firstEditDeadline - Date.now();
    expect(remaining).toBeGreaterThan(0);
    vi.advanceTimersByTime(remaining - 1);
    await flushPromises();
    expect(edits()).toEqual(["first update"]);
    vi.advanceTimersByTime(1);
    await flushPromises();
    await outbox.settle();
    expect(edits()).toEqual(["first update", "second update"]);
    const editRequests = transport.textRequests.filter(({ operation }) => operation === "edit");
    expect((editRequests[1]?.startedAt ?? 0) - (editRequests[0]?.startedAt ?? 0)).toBeGreaterThanOrEqual(1_000);
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a corrected final mention reuses its existing message id", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-empty-prefix-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-empty-prefix", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-empty-prefix" };
  const taskId = "mention-empty-prefix-task";
  const messageId = "assistant-empty-prefix";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "@[member-empty-prefix:林]",
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    const originalMessage = transport.listRemoteText(target)[0];
    expect(transport.deliveries.filter(({ operation }) => operation === "send").map(({ text }) => text))
      .toEqual(["@林"]);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "新增前文 @[member-empty-prefix:林]",
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();

    const correctedMessages = transport.listRemoteText(target);
    expect(correctedMessages.map(({ text }) => text)).toEqual(["新增前文 @林"]);
    expect(correctedMessages[0]?.messageId).toBe(originalMessage?.messageId);
    expect(transport.deliveries.filter(({ operation }) => operation === "send").map(({ text }) => text))
      .toEqual(["@林"]);
    expect(transport.deliveries.filter(({ operation }) => operation === "edit").map(({ text }) => text))
      .toEqual(["新增前文 @林"]);
    expect(transport.deliveries.filter(({ operation }) => operation === "edit")[0]?.mention).toEqual({
      uids: ["member-empty-prefix"],
      entities: [{ uid: "member-empty-prefix", offset: 5, length: 2 }],
    });
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a sent empty final sentinel refreshes create payload before a later body message", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-empty-final-sentinel-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-empty-final-sentinel", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-empty-final-sentinel" };
  const taskId = "empty-final-sentinel-task";
  const messageId = "assistant-empty-final-sentinel";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "",
      final: true,
    });
    await outbox.flush(target);
    await outbox.settle();
    expect(transport.listRemoteText(target)).toHaveLength(0);

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "新增前文",
      final: true,
    });
    await outbox.flush(target);
    await outbox.settle();

    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual(["新增前文"]);
    expect(transport.deliveries.filter(({ operation }) => operation === "send").map(({ text }) => text))
      .toEqual(["新增前文"]);
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("corrected mention snapshots keep the 3000-byte segments atomic in one message stream", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-final-order-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-final-order", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-final-order" };
  const taskId = "mention-final-order-task";
  const messageId = "assistant-final-order";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  const expandedPrefix = "b".repeat(3_001);
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "前文 @[member-final-order:林]",
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    const originalMessages = transport.listRemoteText(target);
    expect(originalMessages.map(({ text }) => text)).toEqual(["前文 @林"]);
    const originalMessageId = originalMessages[0]?.messageId;

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: `${expandedPrefix}@[member-final-order:林]`,
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();

    const correctedMessages = transport.listRemoteText(target);
    expect(correctedMessages.map(({ text }) => text)).toEqual([
      "b".repeat(3_000),
      "b@林",
    ]);
    expect(correctedMessages[0]?.messageId).toBe(originalMessageId);
    expect(transport.deliveries.filter(({ operation }) => operation === "send").map(({ text }) => text))
      .toEqual(["前文 @林", "b@林"]);
    const finalSegmentDelivery = transport.deliveries.find(
      ({ operation, text }) => operation === "send" && text === "b@林",
    );
    expect(finalSegmentDelivery?.mention).toEqual({
      uids: ["member-final-order"],
      entities: [{ uid: "member-final-order", offset: 1, length: 2 }],
    });
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a recovered final snapshot reuses existing segments and does not duplicate creates", async () => {
  vi.useFakeTimers();
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-final-capacity-"));
  let store = await openChannelStore(root);
  const bot = { uid: "bot-mention-final-capacity", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-final-capacity" };
  const taskId = "mention-final-capacity-task";
  const messageId = "assistant-final-capacity";
  const expandedPrefix = "b".repeat(6_001);
  const correctedText = `${expandedPrefix}@[member-final-capacity:林]`;
  let started = true;
  let outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: "前文 @[member-final-capacity:林]",
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    const originalFirstMessageId = transport.listRemoteText(target)[0]?.messageId;

    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: correctedText,
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await outbox.settle();

    const correctedMessages = transport.listRemoteText(target);
    expect(correctedMessages.map(({ text }) => text)).toEqual([
      "b".repeat(3_000),
      "b".repeat(3_000),
      "b@林",
    ]);
    expect(correctedMessages.map(({ text }) => text).join("")).toBe(`${expandedPrefix}@林`);
    expect(correctedMessages[0]?.messageId).toBe(originalFirstMessageId);
    const sendsAfterCorrection = transport.deliveries.filter(({ operation }) => operation === "send");
    expect(sendsAfterCorrection.map(({ text }) => text)).toEqual([
      "前文 @林",
      "b".repeat(3_000),
      "b@林",
    ]);
    expect(sendsAfterCorrection[2]?.mention).toEqual({
      uids: ["member-final-capacity"],
      entities: [{ uid: "member-final-capacity", offset: 1, length: 2 }],
    });

    outbox.stopTimers();
    await outbox.settle();
    store.close();
    store = await openChannelStore(root);
    store.setBotUid(bot.uid);
    outbox = createOutboxDelivery({
      store,
      transport,
      botUid: () => bot.uid,
      started: () => started,
    });
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId,
      text: correctedText,
      final: true,
    }, { parseMentions: true });
    await outbox.flush(target);
    await outbox.settle();

    expect(transport.listRemoteText(target).map(({ text }) => text)).toEqual([
      "b".repeat(3_000),
      "b".repeat(3_000),
      "b@林",
    ]);
    expect(transport.deliveries.filter(({ operation }) => operation === "send").map(({ text }) => text))
      .toEqual(["前文 @林", "b".repeat(3_000), "b@林"]);
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("inline-code member markers remain literal before and after a later code-span close", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-mention-buffered-code-span", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "mention-buffered-code-span",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "hold mention suffix until final",
  });
  const partial = "`@[member-buffered:林] secret";
  const closedCodeSpan = `${partial}\``;
  const allowSecondSnapshot = Promise.withResolvers<void>();
  const allowFinal = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-buffered-code-span", partial, false));
      await allowSecondSnapshot.promise;
      emit(assistantText("assistant-buffered-code-span", closedCodeSpan, false));
      await allowFinal.promise;
      emit(assistantText("assistant-buffered-code-span", closedCodeSpan, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => sameTarget(message.target, task));
    expect(transport.visibleText(task)).toBe(partial);

    allowSecondSnapshot.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    expect(transport.visibleText(task)).toBe(closedCodeSpan);
    expect(deliveriesFor(transport, task).every(({ mention }) => mention === undefined)).toBe(true);

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    expect(transport.visibleText(task)).toBe(closedCodeSpan);
  } finally {
    allowSecondSnapshot.resolve();
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a failed final snapshot update rolls back text and mention metadata before delivery", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-mention-rollback-"));
  const store = await openChannelStore(root);
  const bot = { uid: "bot-mention-rollback", name: "Mention Bot" };
  store.setBotUid(bot.uid);
  const transport = new MentionRecordingTransport({ bot });
  const target = { channelType: 2 as const, channelId: "group-mention-rollback" };
  const taskId = "mention-rollback-task";
  let started = true;
  const outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => started,
  });
  const database = new Database(store.path);
  try {
    outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-rollback",
      text: "old prefix",
      final: false,
    }, { parseMentions: true });
    const firstMessage = await transport.waitForRemoteText((message) => sameTarget(message.target, target));
    expect(firstMessage.text).toBe("old prefix");
    await outbox.flush(target);
    await outbox.settle();
    const callsBeforeFinal = transport.deliveries.length;

    database.exec(`
      CREATE TRIGGER reject_mention_snapshot
      BEFORE UPDATE ON outbox
      WHEN NEW.task_id = '${taskId}' AND NEW.stream_final = 1 AND NEW.mention_json IS NOT NULL
      BEGIN
        SELECT RAISE(ABORT, 'injected mention snapshot write failure');
      END;
    `);
    expect(() => outbox.enqueueStreamText(target, taskId, {
      type: "text",
      messageId: "assistant-rollback",
      text: "corrected prefix @[member-a:林] tail",
      final: true,
    }, { parseMentions: true })).toThrow("injected mention snapshot write failure");

    expect(transport.deliveries).toHaveLength(callsBeforeFinal);
    expect(transport.visibleText(target)).toBe("old prefix");
    const rows = database.query(
      "SELECT text, mention_json, stream_final FROM outbox WHERE task_id = ? ORDER BY rowid",
    ).all(taskId) as Array<{ text: string; mention_json: string | null; stream_final: number }>;
    expect(rows).toEqual([{ text: "old prefix", mention_json: null, stream_final: 0 }]);
  } finally {
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    database.close();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("worker notices remain literal while ordinary assistant text carries structured mentions", async () => {
  const bot = { uid: "bot-notice-mention", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "notice-versus-assistant-mention",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "compare notice and ordinary assistant output",
  });
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit({
        type: "notice",
        messageId: "notice-member-marker",
        level: "info",
        text: "literal @[member-notice:小明]",
      });
      emit(assistantText("assistant-member-marker", "@[member-assistant:小明]", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([
      "OMP：literal @[member-notice:小明]",
      "@小明",
    ]);
    expect(deliveriesFor(transport, task).map(({ text, mention }) => ({ text, mention }))).toEqual([
      { text: "OMP：literal @[member-notice:小明]", mention: undefined },
      {
        text: "@小明",
        mention: {
          uids: ["member-assistant"],
          entities: [{ uid: "member-assistant", offset: 0, length: 3 }],
        },
      },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread mention metadata follows each body edit on one message id", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-thread-mention", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "thread-member-mention",
    channelId: "group-allowed____thread-mention",
    channelType: 5,
    mentionedUserIds: [bot.uid],
    text: "reply in an allowed thread",
  });
  const allowSecondSnapshot = Promise.withResolvers<void>();
  const allowFinal = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    bot,
    transport,
    allowedGroupIds: ["group-allowed"],
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-thread-member", "请看 @[member-thread:线程同学]", false));
      await allowSecondSnapshot.promise;
      emit(assistantText("assistant-thread-member", "请看 @[member-thread:线程同学]，继续", false));
      await allowFinal.promise;
      emit(assistantText("assistant-thread-member", "请看 @[member-thread:线程同学]，继续完成", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    const initial = await transport.waitForRemoteText((message) => sameTarget(message.target, task));
    expect(initial.text).toBe("请看 @线程同学");
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "send",
      text: "请看 @线程同学",
      mention: {
        uids: ["member-thread"],
        entities: [{ uid: "member-thread", offset: 3, length: 5 }],
      },
    });

    allowSecondSnapshot.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    expect(transport.visibleText(task)).toBe("请看 @线程同学，继续");
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual(["请看 @线程同学，继续"]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      messageId: initial.messageId,
      text: "请看 @线程同学，继续",
      mention: {
        uids: ["member-thread"],
        entities: [{ uid: "member-thread", offset: 3, length: 5 }],
      },
    });

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    expect(transport.listRemoteText(task).map(({ text, messageId }) => ({ text, messageId }))).toEqual([{
      text: "请看 @线程同学，继续完成",
      messageId: initial.messageId,
    }]);
    expect(deliveriesFor(transport, task).map(({ operation, text, mention, messageId }) => ({
      operation,
      text,
      mention,
      messageId,
    }))).toEqual([
      {
        operation: "send",
        text: "请看 @线程同学",
        mention: {
          uids: ["member-thread"],
          entities: [{ uid: "member-thread", offset: 3, length: 5 }],
        },
        messageId: null,
      },
      {
        operation: "edit",
        text: "请看 @线程同学，继续",
        mention: {
          uids: ["member-thread"],
          entities: [{ uid: "member-thread", offset: 3, length: 5 }],
        },
        messageId: initial.messageId,
      },
      {
        operation: "edit",
        text: "请看 @线程同学，继续完成",
        mention: {
          uids: ["member-thread"],
          entities: [{ uid: "member-thread", offset: 3, length: 5 }],
        },
        messageId: initial.messageId,
      },
    ]);
  } finally {
    allowSecondSnapshot.resolve();
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("cancelling with an incomplete member marker never releases its suffix", async () => {
  const bot = { uid: "bot-cancel-mention-tail", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "cancel-mention-tail",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "cancel before mention final",
  });
  const stopped = Promise.withResolvers<void>();
  const draft = "ready prefix @[member-late:";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit, onStop, isStopped }) => {
      emit(assistantText("assistant-cancel-tail", draft, false));
      await transport.waitForRemoteText((message) => sameTarget(message.target, task));
      onStop(() => stopped.resolve());
      await stopped.promise;
      if (isStopped()) throw new Error("fixture task cancelled");
    },
  });
  try {
    await harness.service.handleInbound(task);
    const prefixMessage = await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task),
    );
    expect(prefixMessage.text).toBe("ready prefix ");

    await harness.service.handleInbound(inbound({
      messageId: "cancel-mention-tail-command",
      channelId: task.channelId,
      channelType: task.channelType,
      mentionedUserIds: [bot.uid],
      text: "/cancel",
    }));
    await stopped.promise;
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "cancelled" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task).some(({ text }) => text.includes("@[member-late:"))).toBe(false);
    expect(deliveriesFor(transport, task).some(({ mention }) => mention !== undefined)).toBe(false);
  } finally {
    stopped.resolve();
    await closeCoreHarness(harness);
  }
});
test("an empty final with no previously visible body creates no blank message", async () => {
  const transport = new MentionRecordingTransport();
  const task = inbound({
    messageId: "mention-empty-without-prefix",
    channelId: "dm-empty-without-prefix",
    text: "no assistant text",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-empty-without-prefix", "", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(0);
    expect(deliveriesFor(transport, task)).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a lost create receipt reconciles the same final mention message without a duplicate", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-mention-receipt-loss", name: "Mention Bot" };
  const target = { channelId: "group-allowed", channelType: 2 as const };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "mention-create-receipt-loss",
    ...target,
    mentionedUserIds: [bot.uid],
    text: "reconcile the final mention message",
  });
  const visibleBody = "@林 已完成";
  const expectedMention = {
    uids: ["member-receipt-loss"],
    entities: [{ uid: "member-receipt-loss", offset: 0, length: 2 }],
  };
  const receiptLossReached = Promise.withResolvers<void>();
  const releaseLostReceipt = Promise.withResolvers<void>();
  let loseReceipt = true;
  const send = transport.sendText.bind(transport);
  transport.sendText = async (peer, text, clientMsgNo, signal, mention) => {
    const receipt = await send(peer, text, clientMsgNo, signal, mention);
    if (loseReceipt && sameTarget(peer, target) && text === visibleBody) {
      loseReceipt = false;
      receiptLossReached.resolve();
      await releaseLostReceipt.promise;
      return {};
    }
    return receipt;
  };
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-receipt-loss", "@[member-receipt-loss:林] 已完成", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task) && message.text === visibleBody,
    );
    await receiptLossReached.promise;
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.unknownOutbox > 0,
    );
    releaseLostReceipt.resolve();
    await flushPromises();

    const firstAttempt = deliveriesFor(transport, task).find(
      ({ operation, text }) => operation === "send" && text === visibleBody,
    );
    expect(firstAttempt).toMatchObject({
      text: visibleBody,
      mention: expectedMention,
    });
    expect(firstAttempt?.clientMsgNo).toBeDefined();

    vi.advanceTimersByTime(2_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    const reconciliation = deliveriesFor(transport, task).filter(
      ({ operation, text }) => operation === "send" && text === visibleBody,
    );
    expect(reconciliation).toHaveLength(2);
    expect(reconciliation.map(({ mention }) => mention)).toEqual([expectedMention, expectedMention]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "edit",
      text: visibleBody,
      mention: expectedMention,
    });
  } finally {
    releaseLostReceipt.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("a final UID-only correction edits the existing mention message", async () => {
  const bot = { uid: "bot-mention-uid-correction", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "mention-uid-only-correction",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "correct a member identity without changing the text",
  });
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-uid-correction", "@[member-old:小明]", true));
      emit(assistantText("assistant-uid-correction", "@[member-new:小明]", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual(["@小明"]);
    expect(deliveriesFor(transport, task).map(({ operation, text, mention }) => ({ operation, text, mention }))).toEqual([
      {
        operation: "send",
        text: "@小明",
        mention: {
          uids: ["member-old"],
          entities: [{ uid: "member-old", offset: 0, length: 3 }],
        },
      },
      {
        operation: "edit",
        text: "@小明",
        mention: {
          uids: ["member-new"],
          entities: [{ uid: "member-new", offset: 0, length: 3 }],
        },
      },
    ]);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("an unregistered slash reply parses ordinary assistant mention output", async () => {
  const bot = { uid: "bot-command-marker-literal", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "unregistered-command-marker-output",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "/mention-test-unregistered",
  });
  const output = "@[member-command:小明]";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-command-marker", output, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual(["@小明"]);
    expect(deliveriesFor(transport, task).at(-1)).toMatchObject({
      operation: "send",
      text: "@小明",
      mention: {
        uids: ["member-command"],
        entities: [{ uid: "member-command", offset: 0, length: 3 }],
      },
    });
  } finally {
    await closeCoreHarness(harness);
  }
});
test("stopping with an incomplete member marker does not release its suffix", async () => {
  const bot = { uid: "bot-stop-mention-tail", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const task = inbound({
    messageId: "stop-unfinalized-mention",
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [bot.uid],
    text: "stop before the member reply finalizes",
  });
  const stopped = Promise.withResolvers<void>();
  const draft = "stable prefix @[member-stop:";
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit, onStop, isStopped }) => {
      emit(assistantText("assistant-stop-tail", draft, false));
      await transport.waitForRemoteText((message) => sameTarget(message.target, task));
      onStop(() => stopped.resolve());
      await stopped.promise;
      if (isStopped()) throw new Error("fixture worker stopped");
    },
  });
  try {
    await harness.service.handleInbound(task);
    const prefix = await transport.waitForRemoteText(
      (message) => sameTarget(message.target, task),
    );
    expect(prefix.text).toBe("stable prefix ");
    await flushPromises();
    const stopping = harness.service.stop();
    await stopped.promise;
    await stopping;

    const texts = transport.listRemoteText(task).map(({ text }) => text);
    expect(texts).toContain("stable prefix ");
    expect(texts.some((text) => text.includes("@[member-stop:"))).toBe(false);
  } finally {
    stopped.resolve();
    await closeCoreHarness(harness);
  }
});
