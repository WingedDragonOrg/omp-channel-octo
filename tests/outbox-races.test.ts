import { expect, test, vi } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createOutboxDelivery } from "../src/outbox.ts";
import { openChannelStore } from "../src/store.ts";
import { FakeTransport } from "./fixtures/core-fakes.ts";

test("a late failure of old content cannot exhaust the newer snapshot's retry budget", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-outbox-race-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport();
  const target = { channelType: 1 as const, channelId: "stale-failure" };
  const receipt = await transport.sendText(target, "original", "stable-message");
  store.enqueueOutbox({
    id: "row", botUid: transport.bot.uid, ...target, taskId: null, kind: "text",
    text: "old snapshot", path: null, fileKind: null, clientMsgNo: "stable-message",
    streamMessageId: "stream", remoteMessageId: receipt.messageId!, desiredVersion: 1, ackedVersion: 0,
  });
  for (let i = 0; i < 5; i++) store.failTextOutbox("row", 1, "earlier failure");
  const attempted = Promise.withResolvers<void>();
  const failure = Promise.withResolvers<void>();
  const edit = transport.editText.bind(transport);
  transport.editText = async (...args) => {
    if (args[2] === "old snapshot") {
      attempted.resolve();
      await failure.promise;
    }
    await edit(...args);
  };
  let started = true;
  const outbox = createOutboxDelivery({ store, transport, botUid: () => transport.bot.uid, started: () => started });
  vi.useFakeTimers();
  try {
    const draining = outbox.flush(target);
    await attempted.promise;
    store.updateTextOutbox("row", "new authoritative snapshot", 2);
    failure.reject(new Error("late old-content failure"));
    await draining;
    vi.advanceTimersByTime(1_000);
    await outbox.settle();
    expect(transport.visibleText(target)).toBe("new authoritative snapshot");
    expect(store.getOutbox("row")?.state).toBe("sent");
  } finally {
    failure.resolve();
    started = false;
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    vi.useRealTimers();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("shutdown cancellation does not spend the last delivery attempt or discard pending text", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-outbox-stop-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport();
  const target = { channelType: 1 as const, channelId: "shutdown-retry" };
  const receipt = await transport.sendText(target, "original", "stable-message");
  store.enqueueOutbox({
    id: "row", botUid: transport.bot.uid, ...target, taskId: null, kind: "text",
    text: "pending final", path: null, fileKind: null, clientMsgNo: "stable-message",
    streamMessageId: "stream", remoteMessageId: receipt.messageId!, desiredVersion: 1, ackedVersion: 0,
  });
  for (let i = 0; i < 5; i++) store.failTextOutbox("row", 1, "earlier failure");
  const attempted = Promise.withResolvers<void>();
  const edit = transport.editText.bind(transport);
  transport.editText = async (_target, _messageId, _text, signal) => {
    attempted.resolve();
    await new Promise<void>((_, reject) => {
      if (signal?.aborted) reject(new Error("aborted"));
      else signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    });
  };
  let started = true;
  const outbox = createOutboxDelivery({ store, transport, botUid: () => transport.bot.uid, started: () => started });
  vi.useFakeTimers();
  try {
    const draining = outbox.flush(target);
    await attempted.promise;
    started = false;
    outbox.abortAll();
    await draining;
    expect(store.getOutbox("row")?.state).toBe("unknown");
    expect(store.getOutbox("row")?.attempts).toBe(5);
    outbox.stopTimers();
    transport.editText = edit;
    started = true;
    await outbox.flush(target);
    expect(transport.visibleText(target)).toBe("pending final");
    expect(store.getOutbox("row")?.state).toBe("sent");
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

test("shutdown interrupts a hung non-stream text send and the pending row recovers exactly once", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-outbox-text-stop-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport();
  const target = { channelType: 1 as const, channelId: "nonstream-stop" };
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const original = transport.sendText.bind(transport);
  let deliverySignal: AbortSignal | undefined;
  transport.sendText = async (_peer, _text, _clientMsgNo, signal) => {
    deliverySignal = signal;
    entered.resolve();
    return new Promise<never>((_, reject) => {
      if (signal?.aborted) {
        reject(new Error("delivery aborted"));
        return;
      }
      signal?.addEventListener("abort", () => reject(new Error("delivery aborted")), { once: true });
      void release.promise.then(() => reject(new Error("released by test teardown")));
    });
  };
  let started = true;
  const outbox = createOutboxDelivery({ store, transport, botUid: () => transport.bot.uid, started: () => started });
  try {
    outbox.enqueueText(target, "durable notice", null);
    await entered.promise;
    const draining = outbox.flush(target);
    const rowId = store.listPendingOutbox(transport.bot.uid, target)[0]?.id;
    expect(rowId).toBeDefined();
    started = false;
    outbox.abortAll();
    expect(deliverySignal?.aborted).toBe(true);
    // The drain must settle from the abort alone, before the test releases the hung send.
    await draining;
    expect(store.getOutbox(rowId!)?.state).toBe("pending");
    expect(store.getOutbox(rowId!)?.attempts).toBe(0);
    transport.sendText = original;
    started = true;
    await outbox.flush(target);
    expect(transport.visibleText(target)).toBe("durable notice");
    expect(transport.sent.filter((message) => message.text === "durable notice")).toHaveLength(1);
    expect(store.getOutbox(rowId!)?.state).toBe("sent");
  } finally {
    started = false;
    release.resolve();
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("shutdown interrupts a hung file send and the pending row recovers exactly once", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-outbox-file-stop-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport();
  const target = { channelType: 1 as const, channelId: "file-stop" };
  const spoolPath = join(root, "clip.txt");
  await writeFile(spoolPath, "payload");
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const original = transport.sendFile.bind(transport);
  let deliverySignal: AbortSignal | undefined;
  transport.sendFile = async (_peer, _path, _kind, _clientMsgNo, signal) => {
    deliverySignal = signal;
    entered.resolve();
    return new Promise<never>((_, reject) => {
      if (signal?.aborted) {
        reject(new Error("delivery aborted"));
        return;
      }
      signal?.addEventListener("abort", () => reject(new Error("delivery aborted")), { once: true });
      void release.promise.then(() => reject(new Error("released by test teardown")));
    });
  };
  let started = true;
  const outbox = createOutboxDelivery({ store, transport, botUid: () => transport.bot.uid, started: () => started });
  try {
    store.enqueueOutbox({
      id: "file-row", botUid: transport.bot.uid, ...target, taskId: null, kind: "file",
      text: null, path: spoolPath, fileKind: "file", clientMsgNo: "stable-file",
    });
    const draining = outbox.flush(target);
    await entered.promise;
    started = false;
    outbox.abortAll();
    expect(deliverySignal?.aborted).toBe(true);
    // The drain must settle from the abort alone, before the test releases the hung send.
    await draining;
    expect(store.getOutbox("file-row")?.state).toBe("pending");
    expect(store.getOutbox("file-row")?.attempts).toBe(0);
    transport.sendFile = original;
    started = true;
    await outbox.flush(target);
    expect(transport.sentFiles.filter((file) => file.path === spoolPath)).toHaveLength(1);
    expect(store.getOutbox("file-row")?.state).toBe("sent");
  } finally {
    started = false;
    release.resolve();
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("stopping during the unknown-delivery notice never starts the real send", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-outbox-notice-stop-"));
  const store = await openChannelStore(root);
  const transport = new FakeTransport();
  const target = { channelType: 1 as const, channelId: "notice-stop" };
  store.enqueueOutbox({
    id: "row", botUid: transport.bot.uid, ...target, taskId: null, kind: "text",
    text: "durable retry", path: null, fileKind: null, clientMsgNo: "stable-real",
  });
  store.markOutboxFailure("row", "earlier outage");
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const original = transport.sendText.bind(transport);
  const calls: Array<{ clientMsgNo: string; signal: AbortSignal | undefined }> = [];
  transport.sendText = async (_peer, _text, clientMsgNo, signal) => {
    calls.push({ clientMsgNo, signal });
    entered.resolve();
    return new Promise<never>((_, reject) => {
      if (signal?.aborted) {
        reject(new Error("delivery aborted"));
        return;
      }
      signal?.addEventListener("abort", () => reject(new Error("delivery aborted")), { once: true });
      void release.promise.then(() => reject(new Error("released by test teardown")));
    });
  };
  let started = true;
  const outbox = createOutboxDelivery({ store, transport, botUid: () => transport.bot.uid, started: () => started });
  try {
    const draining = outbox.flush(target, true);
    await entered.promise;
    expect(calls[0]?.clientMsgNo).toBe("delivery-unknown-row");
    started = false;
    outbox.abortAll();
    expect(calls[0]?.signal?.aborted).toBe(true);
    await draining;
    expect(calls.filter((call) => call.clientMsgNo === "stable-real")).toHaveLength(0);
    expect(store.getOutbox("row")?.state).toBe("unknown");
    expect(store.getOutbox("row")?.attempts).toBe(1);
    transport.sendText = original;
    started = true;
    await outbox.flush(target, true);
    expect(transport.sent.filter((message) => message.text === "durable retry")).toHaveLength(1);
    expect(store.getOutbox("row")?.state).toBe("sent");
  } finally {
    started = false;
    release.resolve();
    outbox.abortAll();
    outbox.stopTimers();
    await outbox.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
