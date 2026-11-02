import { expect, test, vi } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, SendReceipt } from "../src/contracts.ts";
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

test("ordinary replies hold split member markers until final and send the structured tail as a new message", async () => {
  vi.useFakeTimers();
  const bot = { uid: "bot-ordinary-mentions", name: "Mention Bot" };
  const transport = new MentionRecordingTransport({ bot });
  const literalPrefix = [
    "前文🙂：普通 @姓名，",
    "`@[member-inline:行内]` ",
    "```ts\n@[member-fence:围栏]\n```\n",
    String.raw`\@[member-escaped:转义] `,
    "非法 @[not-a-marker] 与 @[member-empty:] ",
  ].join("");
  const draftFirstMarker = "@[member-old:小明]";
  const finalFirstMarker = "@[member-a:小明]";
  const escapedMarker = String.raw`@[member-b:张\\\]三]`;
  const escapedMarkerPrefix = String.raw`@[member-b:张\\\]`;
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
  const allowFinal = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    bot,
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-member-mention", `${literalPrefix}@[`, false));
      await continueChunks.promise;
      emit(assistantText("assistant-member-mention", `${literalPrefix}@[member-old:`, false));
      emit(assistantText("assistant-member-mention", `${literalPrefix}${draftFirstMarker}，又 @[member-b:`, false));
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
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([literalPrefix]);
    expect(
      transport.textRequests.filter(
        (request) => sameTarget(request.target, task) && request.operation === "edit",
      ),
    ).toHaveLength(0);

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
    expect(transport.listRemoteText(task).map(({ text }) => text)).toEqual([literalPrefix, visibleTail]);
    const requests = transport.textRequests.filter((request) => sameTarget(request.target, task));
    expect(requests.filter(({ operation }) => operation === "send").map(({ text }) => text)).toEqual([
      literalPrefix,
      visibleTail,
    ]);
    expect(requests.filter(({ operation }) => operation === "edit")).toHaveLength(0);
    expect(firstMessage?.messageId).not.toBe(transport.listRemoteText(task)[1]?.messageId);

    const deliveries = deliveriesFor(transport, task);
    expect(deliveries.map(({ operation, text, mention }) => ({ operation, text, mention }))).toEqual([
      { operation: "send", text: literalPrefix, mention: undefined },
      {
        operation: "send",
        text: visibleTail,
        mention: {
          uids: ["member-a", "member-b", "member-c"],
          entities: [
            { uid: "member-a", offset: 0, length: 3 },
            { uid: "member-b", offset: 6, length: 5 },
            { uid: "member-c", offset: 17, length: 8 },
            { uid: "member-a", offset: 28, length: 3 },
          ],
        },
      },
    ]);
  } finally {
    continueChunks.resolve();
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("ordinary text without a valid member marker keeps the existing create-then-edit stream", async () => {
  vi.useFakeTimers();
  const transport = new MentionRecordingTransport();
  const task = inbound({
    messageId: "ordinary-unmarked-stream",
    channelId: "dm-unmarked-stream",
    text: "continue the ordinary response",
  });
  const allowFinal = Promise.withResolvers<void>();
  const draft = "普通回复🙂 @普通名字";
  const final = "普通回复🙂 @普通名字 已完成";
  const harness = await createCoreHarness({
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
    channelId: "dm-mention-outbox-restart",
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
