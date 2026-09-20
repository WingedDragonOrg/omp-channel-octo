import { expect, test, vi } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
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
