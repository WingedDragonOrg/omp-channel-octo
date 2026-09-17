import { expect, test, vi } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store";
import type { RemoteTextMessage, TextRequest } from "./fixtures/core-fakes";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

async function flushPromises(): Promise<void> {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    await Promise.resolve();
  }
}

function textSends(transport: FakeTransport): TextRequest[] {
  return transport.textRequests.filter(({ operation }) => operation === "send");
}

function textEdits(transport: FakeTransport): TextRequest[] {
  return transport.textRequests.filter(({ operation }) => operation === "edit");
}

async function flushUntilRemoteCount(
  transport: FakeTransport,
  target: { channelId: string; channelType: 1 | 2 | 5 },
  count: number,
): Promise<boolean> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (transport.listRemoteText(target).length >= count) return true;
    await flushPromises();
  }
  return transport.listRemoteText(target).length >= count;
}

class SlowInitialSendTransport extends FakeTransport {
  readonly initialSendStarted = Promise.withResolvers<void>();
  private readonly initialSendGate = Promise.withResolvers<void>();
  private initialSendPending = true;

  override async sendText(
    target: { channelId: string; channelType: 1 | 2 | 5 },
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ) {
    if (this.initialSendPending) {
      this.initialSendPending = false;
      this.initialSendStarted.resolve();
      await this.initialSendGate.promise;
    }
    return super.sendText(target, text, clientMsgNo, signal);
  }

  releaseInitialSend(): void {
    this.initialSendGate.resolve();
  }
}

class ReceiptLostTransport extends FakeTransport {
  readonly firstSendAccepted = Promise.withResolvers<void>();
  private readonly firstReceiptGate = Promise.withResolvers<void>();
  private loseNextReceipt = true;

  override async sendText(
    target: { channelId: string; channelType: 1 | 2 | 5 },
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ) {
    const receipt = await super.sendText(target, text, clientMsgNo, signal);
    if (!this.loseNextReceipt) return receipt;
    this.loseNextReceipt = false;
    this.firstSendAccepted.resolve();
    await this.firstReceiptGate.promise;
    return {};
  }

  releaseFirstReceipt(): void {
    this.firstReceiptGate.resolve();
  }
}

test("assistant snapshots are visible before prompt end and final snapshots are throttled, serialized, and authoritative", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const allowBurst = Promise.withResolvers<void>();
  const allowFinal = Promise.withResolvers<void>();
  const finishPrompt = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-visible-before-end",
    channelId: "dm-stream-visible-before-end",
    text: "stream a response",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-stream", "draft", false));
      await allowBurst.promise;
      emit(assistantText("assistant-stream", "draft with more", false));
      emit(assistantText("assistant-stream", "draft with latest", false));
      await allowFinal.promise;
      emit(assistantText("assistant-stream", "final authoritative", true));
      await finishPrompt.promise;
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === "draft");

    expect(transport.visibleText(task)).toBe("draft");
    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport)).toHaveLength(0);

    allowBurst.resolve();
    await flushPromises();
    vi.advanceTimersByTime(999);
    await flushPromises();
    expect(transport.visibleText(task)).toBe("draft");
    expect(textEdits(transport)).toHaveLength(0);

    vi.advanceTimersByTime(1);
    await flushPromises();
    expect(transport.visibleText(task)).toBe("draft with latest");
    expect(textEdits(transport)).toHaveLength(1);

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(999);
    await flushPromises();
    expect(transport.visibleText(task)).toBe("draft with latest");
    expect(textEdits(transport)).toHaveLength(1);

    vi.advanceTimersByTime(1);
    await flushPromises();
    expect(transport.visibleText(task)).toBe("final authoritative");
    expect(textEdits(transport)).toHaveLength(2);
    expect((await harness.service.status(task)).state).toBe("running");

    finishPrompt.resolve();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    const edits = textEdits(transport);
    expect(textSends(transport)).toHaveLength(1);
    const firstMessageId = edits[0]?.messageId;
    expect(firstMessageId).toBeDefined();
    if (typeof firstMessageId !== "string") return;
    expect(edits.map(({ messageId }) => messageId)).toEqual([firstMessageId, firstMessageId]);
    expect(edits.at(-1)?.text).toBe("final authoritative");
    expect(edits[1]!.startedAt - edits[0]!.startedAt).toBeGreaterThanOrEqual(1_000);
  } finally {
    allowBurst.resolve();
    allowFinal.resolve();
    finishPrompt.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("continuous 100ms assistant deltas still produce a visible edit within one second", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const task = inbound({
    messageId: "stream-continuous-deltas",
    channelId: "dm-stream-continuous-deltas",
    text: "keep editing while the answer grows",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-continuous", "draft 0", false));
      await transport.waitForRemoteText((message) => message.text === "draft 0");
      for (let index = 1; index <= 20; index += 1) {
        await new Promise<void>((resolve) => setTimeout(resolve, 100));
        emit(assistantText("assistant-continuous", `draft ${index}`, false));
      }
      emit(assistantText("assistant-continuous", "continuous final", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === "draft 0");
    for (let index = 0; index < 10; index += 1) {
      vi.advanceTimersByTime(100);
      await flushPromises();
    }
    expect(textEdits(transport).length).toBeGreaterThan(0);
    for (let index = 0; index < 12; index += 1) {
      vi.advanceTimersByTime(100);
      await flushPromises();
    }
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    expect(transport.visibleText(task)).toBe("continuous final");
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a final correction arriving while the first create is slow is edited onto that remote message", async () => {
  vi.useFakeTimers();
  const transport = new SlowInitialSendTransport();
  const task = inbound({
    messageId: "stream-slow-create-correction",
    channelId: "dm-stream-slow-create-correction",
    text: "correct a slow create",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-slow-create", "stale draft", false));
      await transport.initialSendStarted.promise;
      emit(assistantText("assistant-slow-create", "authoritative final", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.initialSendStarted.promise;
    transport.releaseInitialSend();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("authoritative final");
    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport).at(-1)?.text).toBe("authoritative final");
  } finally {
    transport.releaseInitialSend();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("an empty final arriving while the first create is slow clears the accepted remote draft", async () => {
  vi.useFakeTimers();
  const transport = new SlowInitialSendTransport();
  const task = inbound({
    messageId: "stream-slow-create-empty",
    channelId: "dm-stream-slow-create-empty",
    text: "clear a slow create",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-slow-empty", "draft to clear", false));
      await transport.initialSendStarted.promise;
      emit(assistantText("assistant-slow-empty", "", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.initialSendStarted.promise;
    transport.releaseInitialSend();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("");
    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport).at(-1)?.text).toBe("");
  } finally {
    transport.releaseInitialSend();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("an accepted create with a lost receipt recovers by stable client message number", async () => {
  vi.useFakeTimers();
  const transport = new ReceiptLostTransport();
  const task = inbound({
    messageId: "stream-lost-receipt",
    channelId: "dm-stream-lost-receipt",
    text: "recover the lost receipt",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-lost-receipt", "stale accepted body", false));
      await transport.firstSendAccepted.promise;
      emit(assistantText("assistant-lost-receipt", "authoritative after receipt loss", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.firstSendAccepted.promise;
    await flushPromises();
    transport.releaseFirstReceipt();
    await flushPromises();
    await transport.reconnect();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const sends = textSends(transport);
    expect(sends).toHaveLength(2);
    expect(sends[0]?.clientMsgNo).toBe(sends[1]?.clientMsgNo);
    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("authoritative after receipt loss");
    expect(textEdits(transport).at(-1)?.text).toBe("authoritative after receipt loss");
  } finally {
    transport.releaseFirstReceipt();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("a final arriving after an unknown create preserves reconciliation and edits the latest body", async () => {
  vi.useFakeTimers();
  const transport = new ReceiptLostTransport();
  const allowFinal = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-lost-receipt-final-later",
    channelId: "dm-stream-lost-receipt-final-later",
    text: "recover a final after receipt loss",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-lost-later", "stale accepted body", false));
      await transport.firstSendAccepted.promise;
      await allowFinal.promise;
      emit(assistantText("assistant-lost-later", "latest final body", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.firstSendAccepted.promise;
    const probe = await openChannelStore(harness.stateDir);
    try {
      const pending = probe.listPendingOutbox(harness.bot.uid, task).find(
        (item) => item.streamMessageId?.endsWith(":assistant-lost-later"),
      );
      expect(pending?.state).toBe("unknown");
      expect(pending?.attempts).toBe(0);
    } finally {
      probe.close();
    }
    transport.releaseFirstReceipt();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "running" && status.unknownOutbox === 1,
    );
    allowFinal.resolve();
    await flushPromises();
    await transport.reconnect();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const sends = textSends(transport);
    expect(sends).toHaveLength(2);
    expect(sends[0]?.clientMsgNo).toBe(sends[1]?.clientMsgNo);
    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("latest final body");
    expect(textEdits(transport).at(-1)?.text).toBe("latest final body");
  } finally {
    allowFinal.resolve();
    transport.releaseFirstReceipt();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("an in-flight create recovery keeps the accepted remote ID and latest persisted body", async () => {
  const bot = { uid: "bot-stream-create-crash", name: "Fixture Bot" };
  const target = { channelId: "dm-stream-create-crash", channelType: 1 as const };
  const taskId = "stream-create-crash-task";
  const streamMessageId = `${taskId}:assistant-crash`;
  const clientMsgNo = "stream-create-crash-client";
  const rowId = "stream-create-crash-row";
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-stream-crash-")));
  const transport = new FakeTransport({ bot });
  const accepted = await transport.sendText(target, "stale accepted body", clientMsgNo);
  if (typeof accepted.messageId !== "string") throw new Error("fixture create did not return a message ID");
  const store = await openChannelStore(join(root, "state"));
  let storeClosed = false;
  let restarted: Awaited<ReturnType<typeof createCoreHarness>> | undefined;
  try {
    store.setBotUid(bot.uid);
    store.enqueueOutbox({
      id: rowId,
      botUid: bot.uid,
      channelId: target.channelId,
      channelType: target.channelType,
      taskId,
      kind: "text",
      text: "stale accepted body",
      path: null,
      fileKind: null,
      clientMsgNo,
      streamMessageId,
      segmentIndex: 0,
      desiredVersion: 1,
      ackedVersion: 0,
    });
    expect(store.beginTextCreate(rowId)).toBe(true);
    expect(store.updateTextOutbox(rowId, "latest persisted body", 2)).toBe(true);
    const beforeRestart = store.getOutbox(rowId);
    expect(beforeRestart?.state).toBe("unknown");
    expect(beforeRestart?.attempts).toBe(0);
    expect(beforeRestart?.text).toBe("latest persisted body");
    store.close();
    storeClosed = true;

    const workers = new FakeWorkerFactory();
    restarted = await createCoreHarness({ root, bot, transport, workers });
    await waitForStatus(restarted.service, target, (status) => status.outbox === 0);

    expect(workers.prompts).toHaveLength(0);
    const recoverySends = textSends(transport).filter(({ clientMsgNo: value }) => value === clientMsgNo);
    expect(recoverySends).toHaveLength(2);
    expect(recoverySends[0]?.clientMsgNo).toBe(recoverySends[1]?.clientMsgNo);
    expect(transport.listRemoteText(target)).toHaveLength(1);
    expect(transport.visibleText(target)).toBe("latest persisted body");
    expect(textEdits(transport).at(-1)?.messageId).toBe(accepted.messageId);
    expect(textEdits(transport).at(-1)?.text).toBe("latest persisted body");
  } finally {
    if (restarted) await closeCoreHarness(restarted);
    if (!storeClosed) store.close();
    await rm(root, { recursive: true, force: true });
  }
});




test("an empty final snapshot closes an existing assistant message without sending a blank message", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const partial = "content that is later cleared";
  const task = inbound({
    messageId: "stream-empty-final",
    channelId: "dm-stream-empty-final",
    text: "clear the response",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-empty-final", partial, false));
      await transport.waitForRemoteText((message) => message.text === partial);
      emit(assistantText("assistant-empty-final", "", true));
      emit(assistantText("assistant-empty-new", "", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await flushPromises();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    const finalMessage = transport.listRemoteText(task).find(({ text }) => text === "");
    expect(finalMessage).toBeDefined();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("");
    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport)).toHaveLength(1);
    expect(textEdits(transport)[0]?.messageId).toBe(finalMessage?.messageId);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("two assistant messages with identical text keep independent remote identities", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const task = inbound({
    messageId: "stream-identical-messages",
    channelId: "dm-stream-identical-messages",
    text: "produce two identical responses",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-one", "same response", false));
      emit(assistantText("assistant-one", "same response", true));
      emit(assistantText("assistant-two", "same response", false));
      emit(assistantText("assistant-two", "same response", true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await flushPromises();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    expect(await flushUntilRemoteCount(transport, task, 2)).toBe(true);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const remote = transport.listRemoteText(task);
    expect(remote).toHaveLength(2);
    expect(remote.map(({ text }) => text)).toEqual(["same response", "same response"]);
    expect(new Set(remote.map(({ messageId }) => messageId)).size).toBe(2);
    expect(textSends(transport)).toHaveLength(2);
    expect(new Set(textSends(transport).map(({ clientMsgNo }) => clientMsgNo)).size).toBe(2);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});


test("a slow assistant edit never overlaps a newer edit and cannot leave an old snapshot visible", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const releaseFirstEdit = transport.holdNextEdit();
  const allowFinal = Promise.withResolvers<void>();
  const finishPrompt = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-slow-edit",
    channelId: "dm-stream-slow-edit",
    text: "serialize response edits",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-slow", "base", false));
      await transport.waitForRemoteText((message) => message.text === "base");
      emit(assistantText("assistant-slow", "older snapshot", false));
      await allowFinal.promise;
      emit(assistantText("assistant-slow", "new final snapshot", true));
      await finishPrompt.promise;
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === "base");
    vi.advanceTimersByTime(1_000);
    await flushPromises();

    const firstEdit = textEdits(transport)[0];
    expect(firstEdit).toBeDefined();
    if (!firstEdit) return;
    expect(firstEdit.text).toBe("older snapshot");
    expect(transport.maxConcurrentTextRequests).toBe(1);

    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    expect(textEdits(transport)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe("base");

    releaseFirstEdit();
    for (let attempt = 0; attempt < 8 && textEdits(transport).length < 2; attempt += 1) {
      await flushPromises();
    }
    const edits = textEdits(transport);
    expect(edits).toHaveLength(2);
    expect(edits[1]?.text).toBe("new final snapshot");
    expect(edits[1]?.messageId).toBe(firstEdit.messageId);
    expect(transport.maxConcurrentTextRequests).toBe(1);
    expect(edits[1]!.startedAt - edits[0]!.startedAt).toBeGreaterThanOrEqual(1_000);
    expect(transport.visibleText(task)).toBe("new final snapshot");

    finishPrompt.resolve();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
  } finally {
    allowFinal.resolve();
    finishPrompt.resolve();
    releaseFirstEdit();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a failed final edit is retried after reconnect along its known remote message id without rerunning the agent", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const partial = "partial before reconnect";
  const final = "final after reconnect";
  const allowFinal = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-edit-reconnect",
    channelId: "dm-stream-edit-reconnect",
    text: "retry the final response",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-reconnect", partial, false));
      await transport.waitForRemoteText((message) => message.text === partial);
      await allowFinal.promise;
      emit(assistantText("assistant-reconnect", final, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === partial);
    transport.setOffline();
    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();

    const failedEdit = textEdits(transport).find(({ error }) => error !== undefined);
    expect(failedEdit).toBeDefined();
    if (!failedEdit) return;
    expect(transport.listRemoteText(task)).toHaveLength(1);
    expect(transport.visibleText(task)).toBe(partial);
    expect(textSends(transport)).toHaveLength(1);

    await transport.reconnect();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    const delivered = transport.remoteTextMessages.get(failedEdit.messageId!);
    expect(delivered?.text).toBe(final);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport).every(({ messageId }) => messageId === failedEdit.messageId)).toBe(true);
    expect(transport.visibleText(task)).toBe(final);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a restart resumes a failed final edit by remote id and never prompts the completed task again", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const partial = "partial before restart";
  const final = "final after restart";
  const allowFinal = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-edit-restart",
    channelId: "dm-stream-edit-restart",
    text: "restart while final response is pending",
  });
  const firstWorkers = new FakeWorkerFactory(async ({ emit }) => {
    emit(assistantText("assistant-restart", partial, false));
    await transport.waitForRemoteText((message) => message.text === partial);
    await allowFinal.promise;
    emit(assistantText("assistant-restart", final, true));
  });
  const first = await createCoreHarness({ transport, workers: firstWorkers });
  let failedEdit: TextRequest | null = null;
  try {
    await first.service.handleInbound(task);
    await first.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === partial);
    transport.setOffline();
    allowFinal.resolve();
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    failedEdit = textEdits(transport).find(({ error }) => error !== undefined) ?? null;
    expect(failedEdit).not.toBeNull();
    if (!failedEdit) return;
  } finally {
    allowFinal.resolve();
    vi.useRealTimers();
    await closeCoreHarness(first, { remove: false });
  }
  const failedEditRecord = failedEdit;
  if (!failedEditRecord) return;
  vi.useFakeTimers();

  const restartedWorkers = new FakeWorkerFactory();
  const restarted = await createCoreHarness({
    root: first.root,
    bot: first.bot,
    transport,
    workers: restartedWorkers,
  });
  try {
    expect(restartedWorkers.prompts).toHaveLength(0);
    await transport.reconnect();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    const delivered = transport.remoteTextMessages.get(failedEditRecord.messageId!);
    expect(delivered?.text).toBe(final);
    await waitForStatus(
      restarted.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(textSends(transport)).toHaveLength(1);
    expect(textEdits(transport).every(({ messageId }) => messageId === failedEditRecord.messageId)).toBe(true);
    expect(transport.visibleText(task)).toBe(final);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(restarted);
  }
});

test("a long UTF-8 snapshot is split at 3000 bytes and a shorter final snapshot clears stale tail fragments", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const longBody = "🙂".repeat(800);
  const final = "short final";
  const task = inbound({
    messageId: "stream-long-utf8",
    channelId: "dm-stream-long-utf8",
    text: "preserve a long response",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-long", longBody, false));
      await transport.waitForRemoteText(() => transport.listRemoteText(task).length >= 2);
      emit(assistantText("assistant-long", final, true));
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await flushPromises();
    expect(await flushUntilRemoteCount(transport, task, 2)).toBe(true);
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    const finalMessage = transport.listRemoteText(task).find(({ text }) => text === final);
    expect(finalMessage).toBeDefined();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    const remote = transport.listRemoteText(task);
    expect(remote.length).toBeGreaterThan(1);
    expect(remote.every(({ text }) => Buffer.byteLength(text, "utf8") <= 3_000)).toBe(true);
    expect(transport.visibleText(task)).toBe(final);
    expect(textSends(transport).length).toBeGreaterThan(1);
    expect(textEdits(transport).some(({ text }) => text === "")).toBe(true);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a long reply cuts at paragraph breaks and never reshuffles finished segments as it streams", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const paragraph = `${"这是一个完整的段落，句末标点齐全。".repeat(20)}\n`;
  const firstSnapshot = paragraph.repeat(5);
  const secondSnapshot = paragraph.repeat(9);
  const emitMore = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const task = inbound({
    messageId: "stream-natural-break",
    channelId: "dm-stream-natural-break",
    text: "stream a paragraph reply",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-natural", firstSnapshot, false));
      await emitMore.promise;
      emit(assistantText("assistant-natural", secondSnapshot, false));
      await finish.promise;
      emit(assistantText("assistant-natural", secondSnapshot, true));
    },
  });
  try {
    const run = harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    expect(await flushUntilRemoteCount(transport, task, 3)).toBe(true);
    const settled = transport.listRemoteText(task).map(({ messageId, text }) => ({ messageId, text }));
    expect(settled).toHaveLength(3);
    // 两段完整的段落窗口各 2042 bytes 且结尾是换行，而不是在句子中间量到 3000
    expect(settled.slice(0, 2).map(({ text }) => Buffer.byteLength(text, "utf8"))).toEqual([2_042, 2_042]);
    expect(settled.slice(0, 2).every(({ text }) => text.endsWith("\n"))).toBe(true);

    emitMore.resolve();
    for (let attempt = 0; attempt < 10 && transport.listRemoteText(task).length < 5; attempt += 1) {
      vi.advanceTimersByTime(1_000);
      await flushPromises();
    }
    const grown = transport.listRemoteText(task);
    expect(grown).toHaveLength(5);
    // 已由满窗口定界的段保持原样：同一条远端消息、同一段文本
    expect(grown.slice(0, 2).map(({ messageId, text }) => ({ messageId, text }))).toEqual(settled.slice(0, 2));

    finish.resolve();
    await run;
    await waitForStatus(harness.service, task, (status) => status.state === "completed" && status.outbox === 0);
    const final = transport.listRemoteText(task);
    expect(final.map(({ text }) => text).join("")).toBe(secondSnapshot);
    expect(final.every(({ text }) => Buffer.byteLength(text, "utf8") <= 3_000)).toBe(true);
    expect(final.at(-1)?.text).toBe(paragraph);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("cancel aborts an in-flight assistant edit promptly and preserves its latest snapshot for reconnect", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  const releaseEdit = transport.holdNextEdit();
  const stopped = Promise.withResolvers<void>();
  const partial = "visible before cancel";
  const latest = "latest snapshot before cancel";
  const target = { channelId: "dm-stream-cancel", channelType: 1 as const };
  const task = inbound({ messageId: "stream-cancel-task", ...target, text: "cancel stream" });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, onStop, isStopped }) => {
      emit(assistantText("assistant-cancel", partial, false));
      await transport.waitForRemoteText((message) => message.text === partial);
      emit(assistantText("assistant-cancel", latest, false));
      onStop(() => stopped.resolve());
      await stopped.promise;
      if (isStopped()) throw new Error("fixture worker cancelled");
    },
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await transport.waitForRemoteText((message) => message.text === partial);
    await flushPromises();
    vi.advanceTimersByTime(1_000);
    await flushPromises();
    const editStarted = textEdits(transport).find(({ error }) => error === undefined);
    expect(editStarted).toBeDefined();
    if (!editStarted) return;

    transport.setOffline();
    const cancel = harness.service.handleInbound(
      inbound({ messageId: "stream-cancel-command", ...target, text: "/cancel" }),
    );
    const cancelFinishedPromise = Promise.race([
      cancel.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 500)),
    ]);
    await flushPromises();
    vi.advanceTimersByTime(500);
    await flushPromises();
    expect(await cancelFinishedPromise).toBe(true);
    await stopped.promise;
    await flushPromises();
    expect(editStarted.error).toMatch(/aborted|cancel/i);
    expect(transport.remoteTextMessages.get(editStarted.messageId!)?.text).toBe(partial);

    await transport.reconnect();
    vi.advanceTimersByTime(2_000);
    await flushPromises();
    const delivered = transport.remoteTextMessages.get(editStarted.messageId!);
    expect(delivered?.text).toBe(latest);
    expect(transport.visibleText(task)).toContain(latest);
  } finally {
    stopped.resolve();
    releaseEdit();
    transport.online = true;
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("stop aborts an in-flight assistant edit without waiting for the network gate", async () => {
  const transport = new FakeTransport();
  const releaseEdit = transport.holdNextEdit();
  const stopped = Promise.withResolvers<void>();
  const editStartedPromise = transport.waitForTextRequest(
    (request) => request.operation === "edit" && request.error === undefined,
  );
  const task = inbound({
    messageId: "stream-stop-task",
    channelId: "dm-stream-stop",
    text: "stop stream",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, onStop }) => {
      emit(assistantText("assistant-stop", "before stop", false));
      await transport.waitForRemoteText((message) => message.text === "before stop");
      emit(assistantText("assistant-stop", "pending stop", false));
      onStop(() => stopped.resolve());
      await stopped.promise;
    },
  });
  let stopPromise: Promise<void> | undefined;
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    const editStarted = await editStartedPromise;

    stopPromise = harness.service.stop();
    const stopFinishedPromise = Promise.race([
      stopPromise.then(() => true),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 500)),
    ]);
    expect(await stopFinishedPromise).toBe(true);
    expect(editStarted.error).toMatch(/aborted|cancel/i);
  } finally {
    stopped.resolve();
    releaseEdit();
    if (stopPromise) await stopPromise.catch(() => {});
    await closeCoreHarness(harness);
  }
}, 5_000);
