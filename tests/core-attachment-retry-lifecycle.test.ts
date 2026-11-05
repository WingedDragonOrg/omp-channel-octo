import { expect, test, vi } from "bun:test";
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import { readFile, readdir, rm } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, InboundMessage } from "../src/contracts.ts";
import { attachmentDirectory, safeAttachmentName } from "../src/inbound/attachments.ts";
import { OctoTransport } from "../src/octo/index.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const MAX_BYTES = 1024;
const FILE_BYTES = new Uint8Array([16, 32, 48, 64]);
const RETRY_DELAYS_MS = new Set([200, 400]);

interface PendingHttpRequest {
  respond(status: number, bytes?: Uint8Array): void;
}

interface LocalAttachmentServer {
  readonly url: string;
  readonly requestCount: number;
  waitForRequest(ordinal: number): Promise<PendingHttpRequest>;
  close(): Promise<void>;
}
interface RetryDelayWatcher {
  readonly delay: Promise<number>;
  restore(): void;
}

interface PendingHttpRequestWaiter {
  readonly promise: Promise<PendingHttpRequest>;
  resolve(request: PendingHttpRequest): void;
}

async function createLocalAttachmentServer(): Promise<LocalAttachmentServer> {
  const requests: PendingHttpRequest[] = [];
  const waiters = new Map<number, PendingHttpRequestWaiter>();
  const server = createServer((_request, response) => {
    const pending: PendingHttpRequest = {
      respond(status, bytes = new Uint8Array()) {
        response.writeHead(status, { "content-length": bytes.byteLength });
        response.end(bytes);
      },
    };
    requests.push(pending);
    waiters.get(requests.length)?.resolve(pending);
  });
  const listening = Promise.withResolvers<void>();
  server.once("error", listening.reject);
  server.listen(0, "127.0.0.1", () => {
    server.off("error", listening.reject);
    listening.resolve();
  });
  await listening.promise;
  const address = server.address();
  if (address === null || typeof address === "string") {
    server.close();
    throw new Error("local attachment server did not bind a TCP address");
  }
  return {
    url: `http://127.0.0.1:${address.port}/attachment`,
    get requestCount() {
      return requests.length;
    },
    waitForRequest(ordinal) {
      const existing = requests[ordinal - 1];
      if (existing !== undefined) return Promise.resolve(existing);
      let waiter = waiters.get(ordinal);
      if (waiter === undefined) {
        const deferred = Promise.withResolvers<PendingHttpRequest>();
        waiter = { promise: deferred.promise, resolve: deferred.resolve };
        waiters.set(ordinal, waiter);
      }
      return waiter.promise;
    },
    close: () => {
      const closed = Promise.withResolvers<void>();
      server.close(error => error ? closed.reject(error) : closed.resolve());
      server.closeAllConnections();
      return closed.promise;
    },
  };
}

function installRealDownloader(harness: CoreHarness): void {
  const octo = new OctoTransport({ apiUrl: "http://127.0.0.1/", botToken: "fixture-only" });
  const download = octo.downloadAttachment.bind(octo);
  harness.transport.downloadAttachment = async request => {
    harness.transport.downloadRequests.push({ url: request.url, path: request.path, maxBytes: request.maxBytes });
    return await download(request);
  };
}

function watchRetryDelay(): RetryDelayWatcher {
  const observed = Promise.withResolvers<number>();
  const original = globalThis.setTimeout.bind(globalThis);
  const spy = vi.spyOn(globalThis, "setTimeout");
  const implementation = (...args: Parameters<typeof globalThis.setTimeout>) => {
    const timeout = args[1];
    if (typeof timeout === "number" && RETRY_DELAYS_MS.has(timeout)) observed.resolve(timeout);
    return original(...args);
  };
  spy.mockImplementation(implementation as typeof globalThis.setTimeout);
  return { delay: observed.promise, restore: () => spy.mockRestore() };
}

function attachmentMessage(
  url: string,
  messageId: string,
  attachments: readonly { name: string; url?: string }[] = [{ name: "retry.bin" }],
): InboundMessage {
  return inbound({
    channelType: 1,
    channelId: "dm-attachment-retry-lifecycle",
    messageId,
    text: "inspect these files",
    attachments: attachments.map(({ name, url: attachmentUrl }) => ({
      kind: "file" as const,
      name,
      size: 0,
      durationSeconds: 0,
      url: attachmentUrl ?? url,
    })),
  });
}

async function expectNoTask(harness: CoreHarness, message: InboundMessage): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    expect(store.getTaskByMessageId(harness.bot.uid, message, message.messageId)).toBeNull();
  } finally {
    store.close();
  }
}

async function waitForRefusal(harness: CoreHarness, target: ChatTarget): Promise<void> {
  const refusal = "附件过多或下载超时，未接收任务。";
  await harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
    sentTarget.channelType === target.channelType
    && sentTarget.channelId === target.channelId
    && text === refusal);
  await waitForStatus(harness.service, target, status => status.outbox === 0);
}

test("the 120-second message budget includes prior files and cancels a later download without leaving a task or spool", async () => {
  const server = await createLocalAttachmentServer();
  vi.useFakeTimers();
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MAX_BYTES } });
  installRealDownloader(harness);
  const target = { channelType: 1 as const, channelId: "dm-attachment-retry-lifecycle" };
  const message = attachmentMessage(server.url, "retry-message-budget", [
    { name: "first.bin" },
    { name: "second.bin" },
    { name: "third.bin" },
  ]);
  const directory = attachmentDirectory(harness.stateDir, message, message.messageId);
  const receipt = harness.service.receive(message);

  try {
    // Fake time advances the actual 120-second message budget; no wall-clock wait is involved.
    const first = await server.waitForRequest(1);
    setTimeout(() => first.respond(200, FILE_BYTES), 59_900);
    vi.advanceTimersByTime(59_900);

    const second = await server.waitForRequest(2);
    setTimeout(() => second.respond(200, FILE_BYTES), 59_900);
    vi.advanceTimersByTime(59_900);
    await server.waitForRequest(3);

    vi.advanceTimersByTime(200);
    await receipt.ackReady;
    await waitForRefusal(harness, target);
    expect(server.requestCount).toBe(3);
    vi.advanceTimersByTime(1_000);
    expect(server.requestCount).toBe(3);

    expect(harness.transport.downloadRequests.map(({ url }) => url)).toEqual([
      server.url,
      server.url,
      server.url,
    ]);
    expect(harness.workers.prompts).toHaveLength(0);
    await expectNoTask(harness, message);
    expect(existsSync(directory)).toBe(false);
    expect(harness.transport.sent.map(({ text }) => text).join("\n")).not.toContain(server.url);
  } finally {
    await closeCoreHarness(harness);
    await server.close();
    vi.useRealTimers();
  }
});

test("user cancellation aborts a transient-failure retry wait without a later HTTP attempt", async () => {
  const server = await createLocalAttachmentServer();
  vi.useFakeTimers();
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MAX_BYTES } });
  installRealDownloader(harness);
  const downloadSettled = Promise.withResolvers<void>();
  const download = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.downloadAttachment = async request => {
    try {
      return await download(request);
    } finally {
      downloadSettled.resolve();
    }
  };
  const target = { channelType: 1 as const, channelId: "dm-attachment-retry-lifecycle" };
  const message = attachmentMessage(server.url, "retry-message-cancelled");
  const directory = attachmentDirectory(harness.stateDir, message, message.messageId);
  let retryWatch: RetryDelayWatcher | undefined;

  try {
    const receipt = harness.service.receive(message);
    const attempt = await server.waitForRequest(1);
    retryWatch = watchRetryDelay();
    attempt.respond(503);
    expect(await retryWatch.delay).toBe(200);
    retryWatch.restore();
    retryWatch = undefined;


    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "retry-message-cancel-command",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await receipt.ackReady;
    await downloadSettled.promise;
    // Stopping waits for the cancelled ingress preparation and its file cleanup.
    await harness.service.stop();
    expect(server.requestCount).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);
    await expectNoTask(harness, message);
    expect(existsSync(directory)).toBe(false);
    vi.advanceTimersByTime(1_000);
    expect(server.requestCount).toBe(1);
    expect(harness.transport.sent.map(({ text }) => text).join("\n")).not.toContain(server.url);
  } finally {
    retryWatch?.restore();
    await closeCoreHarness(harness);
    await server.close();
    vi.useRealTimers();
  }
});

test("stopping during a retry wait aborts it; redelivery after restart downloads once and creates one task", async () => {
  const server = await createLocalAttachmentServer();
  vi.useFakeTimers();
  const first = await createCoreHarness({ attachments: { enabled: true, maxBytes: MAX_BYTES } });
  installRealDownloader(first);
  const target = { channelType: 1 as const, channelId: "dm-attachment-retry-lifecycle" };
  const message = attachmentMessage(server.url, "retry-message-restart");
  const directory = attachmentDirectory(first.stateDir, message, message.messageId);
  const firstReceipt = first.service.receive(message);
  const firstAckOutcome = firstReceipt.ackReady.then(
    () => ({ kind: "fulfilled" as const }),
    error => ({ kind: "rejected" as const, error }),
  );
  let firstStopped = false;
  let restarted: CoreHarness | undefined;
  let retryWatch: RetryDelayWatcher | undefined;

  try {
    const attempt = await server.waitForRequest(1);
    retryWatch = watchRetryDelay();
    attempt.respond(503);
    expect(await retryWatch.delay).toBe(200);
    retryWatch.restore();
    retryWatch = undefined;

    await first.service.stop();
    firstStopped = true;
    const stoppedOutcome = await firstAckOutcome;
    expect(stoppedOutcome.kind).toBe("rejected");
    expect(stoppedOutcome.kind === "rejected" ? String(stoppedOutcome.error) : "").toMatch(/stop|shutdown|cancel|abort/iu);
    expect(server.requestCount).toBe(1);
    expect(first.workers.prompts).toHaveLength(0);
    expect(existsSync(directory)).toBe(false);

    vi.advanceTimersByTime(1_000);
    expect(server.requestCount).toBe(1);

    const transport = new FakeTransport({ bot: first.bot });
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      attachments: { enabled: true, maxBytes: MAX_BYTES },
    });
    installRealDownloader(restarted);

    const redelivery = restarted.service.receive(message);
    const successfulAttempt = await server.waitForRequest(2);
    successfulAttempt.respond(200, FILE_BYTES);
    expect(server.requestCount).toBe(2);
    await redelivery.ackReady;
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, target, status => status.state === "completed");

    const finalPath = join(directory, safeAttachmentName("retry.bin", 0, "file", server.url));
    expect(new Uint8Array(await readFile(finalPath))).toEqual(FILE_BYTES);
    expect(await readdir(directory)).toEqual([safeAttachmentName("retry.bin", 0, "file", server.url)]);
    expect(restarted.transport.downloadRequests).toHaveLength(1);
    expect(restarted.workers.prompts).toHaveLength(1);
    const store = await openChannelStore(restarted.stateDir);
    try {
      expect(store.getTaskByMessageId(restarted.bot.uid, target, message.messageId)).not.toBeNull();
    } finally {
      store.close();
    }

    const duplicate = restarted.service.receive(message);
    await duplicate.ackReady;
    expect(server.requestCount).toBe(2);
    expect(restarted.transport.downloadRequests).toHaveLength(1);
    expect(restarted.workers.prompts).toHaveLength(1);
  } finally {
    retryWatch?.restore();
    if (!firstStopped) await first.service.stop().catch(() => {});
    if (restarted !== undefined) await closeCoreHarness(restarted, { remove: false });
    await rm(first.root, { recursive: true, force: true });
    await server.close();
    vi.useRealTimers();
  }
});

