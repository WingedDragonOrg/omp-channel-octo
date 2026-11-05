import { Database } from "bun:sqlite";
import { expect, test, vi } from "bun:test";
import {
  mkdir,
  readFile,
  readdir,
  rename,
  symlink,
  unlink,
  utimes,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import type { BotCardAction, BotEvent, ChatTarget, InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { attachmentDirectory, safeAttachmentName } from "../src/inbound/attachments.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForIngressJob,
} from "./fixtures/core-fakes.ts";
import type { CoreHarness, FakePrompt, SentCard } from "./fixtures/core-fakes.ts";

const MEDIA_LIMIT = 1024 * 1024;

async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`did not observe ${label}`);
}
async function flushUntil(check: () => boolean, label: string): Promise<void> {
  for (let turn = 0; turn < 100; turn += 1) {
    if (check()) return;
    await Promise.resolve();
  }
  throw new Error(`did not observe ${label}`);
}


async function flushMicrotasks(turns = 40): Promise<void> {
  for (let index = 0; index < turns; index += 1) await Promise.resolve();
}

function cardNodes(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) return value.flatMap(cardNodes);
  if (value === null || typeof value !== "object") return [];
  const record = value as Record<string, unknown>;
  return [record, ...Object.values(record).flatMap(cardNodes)];
}

function configChoice(card: SentCard, value: string): BotCardAction {
  const nodes = cardNodes(card.frame.card);
  const input = nodes.find(node => node.type === "Input.ChoiceSet");
  const submit = nodes.find(node =>
    node.type === "Action.Submit"
    && typeof node.id === "string"
    && node.id.startsWith("cfg:mode:"));
  if (input === undefined || submit === undefined) {
    throw new Error("mode config card is missing its choice input or submit action");
  }
  const inputId = String(input.id);
  return {
    ...card.target,
    messageId: card.messageId,
    actionId: String(submit.id),
    operatorUid: "owner-1",
    inputs: { [inputId]: value },
  };
}

async function createAskPrompt(): Promise<FakePrompt> {
  return async ({ append, promptContext }) => {
    const ask = promptContext?.tools?.find(({ name }) => name === "ask");
    if (ask === undefined) throw new Error("ask host tool missing");
    const result = await ask.execute({
      questions: [{
        id: "card-accept-question",
        question: "Which option?",
        options: [{ label: "first" }, { label: "second" }],
      }],
    }, new AbortController().signal);
    await append("answer.log", result);
  };
}

async function createCardAction(
  harness: CoreHarness,
  family: "ask" | "config",
  target: ChatTarget,
): Promise<BotCardAction> {
  if (family === "ask") {
    const sentCard = harness.transport.waitForCardSend(card =>
      card.target.channelType === target.channelType && card.target.channelId === target.channelId);
    await harness.service.submitInbound(inbound({
      ...target,
      messageId: "card-accept-ask-prompt",
      text: "start a durable ask card",
    }));
    await harness.workers.waitForPromptCount(1);
    const card = await sentCard;
    return {
      ...target,
      messageId: card.messageId,
      actionId: "ask_opt_1",
      operatorUid: "owner-1",
      inputs: {},
    };
  }

  const menu = inbound({
    ...target,
    messageId: "card-accept-config-menu",
    text: "/mode",
  });
  const sentCard = harness.transport.waitForCardSend(card =>
    card.target.channelType === target.channelType && card.target.channelId === target.channelId);
  await harness.service.submitInbound(menu);
  await waitForIngressJob(harness, menu);
  return configChoice(await sentCard, "queue");
}

function installCardAcceptanceFailure(harness: CoreHarness): void {
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    database.exec(`
      CREATE TRIGGER fail_persistent_card_ingress
      BEFORE INSERT ON ingress_jobs
      WHEN NEW.source_kind = 'bot-event'
      BEGIN
        SELECT RAISE(ABORT, 'fixture persistent card ingress failure');
      END;
    `);
  } finally {
    database.close();
  }
}

async function exhaustCardEventRetries(
  harness: CoreHarness,
  event: BotEvent,
): Promise<void> {
  // Production allows five attempts with a 5 s backoff. Advance the real service event loop's timer
  // deterministically, re-delivering the unacknowledged event as the server would.
  const attemptsFetchedAtCursor = (): number =>
    harness.transport.eventFetches.filter(cursor => cursor === 0).length;
  harness.transport.pushBotEvent(event);
  await flushUntil(() => attemptsFetchedAtCursor() >= 1, "initial card event poll");
  await flushMicrotasks();
  for (let attempt = 2; attempt <= 5; attempt += 1) {
    vi.advanceTimersByTime(5_000);
    await flushMicrotasks();
    await flushUntil(
      () => attemptsFetchedAtCursor() >= attempt,
      `card event retry ${attempt}`,
    );
    harness.transport.pushBotEvent(event);
    await flushMicrotasks();
  }
  // Observe the consumer leave its terminal callback and begin another poll. The pending path must
  // retain cursor 0; without this barrier the assertions could run while exhaustion is still in flight.
  for (let turn = 0; turn < 10 && harness.transport.eventFetches.length < 6; turn += 1) {
    vi.advanceTimersByTime(5_000);
    await flushMicrotasks();
  }
  await flushUntil(
    () => harness.transport.eventFetches.length >= 6,
    "card event poll after retry exhaustion",
  );
}

async function readCardCursor(harness: CoreHarness): Promise<number> {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getBotEventCursor(harness.bot.uid);
  } finally {
    store.close();
  }
}

for (const family of ["ask", "config"] as const) {
  test(`persistent ${family}-card acceptance failure after retry exhaustion keeps the event unacked`, async () => {
    const transport = new FakeTransport();
    transport.cardSupported = true;
    const harness = await createCoreHarness({
      transport,
      ...(family === "ask" ? { prompt: await createAskPrompt() } : {}),
    });
    const target: ChatTarget = { channelType: 1, channelId: `dm-card-accept-${family}` };
    const eventId = family === "ask" ? 7301 : 7302;
    try {
      const action = await createCardAction(harness, family, target);
      installCardAcceptanceFailure(harness);
      vi.useFakeTimers();
      await exhaustCardEventRetries(harness, cardActionEvent(eventId, action));
      expect(harness.transport.eventFetches).toEqual([0, 0, 0, 0, 0, 0]);
      expect(harness.transport.ackedEvents).not.toContain(eventId);
      expect(await readCardCursor(harness)).toBe(0);
      const store = await openChannelStore(harness.stateDir);
      try {
        expect(store.getIngressJobBySource({
          botUid: harness.bot.uid,
          sourceKind: "bot-event",
          sourceId: String(eventId),
        })).toBeNull();
        if (family === "ask") {
          const pending = store.listPendingQuestions(harness.bot.uid, target);
          expect(pending).toHaveLength(1);
          expect(store.getQuestion(pending[0]!.id)?.state).toBe("pending");
          expect(JSON.parse(pending[0]!.payload)).toMatchObject({ sourceId: "card-accept-question" });
        } else {
          expect(store.getChatMode(harness.bot.uid, target)).toBe("steer");
        }
      } finally {
        store.close();
      }
    } finally {
      vi.useRealTimers();
      await closeCoreHarness(harness);
    }
  });
}


test("cancel preserves an accepted ordered config-card action and releases it to apply", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport, attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-cancel-config-action" };
  const mediaUrl = "https://cdn.example.test/cancel-config-held.txt";
  const bytes = new TextEncoder().encode("held attachment");
  harness.transport.attachmentBodies.set(mediaUrl, bytes);
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.downloadAttachment = async request => {
    downloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(request.signal?.reason ?? new Error("fixture download cancelled"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseDownload.promise, aborted.promise]);
    return originalDownload(request);
  };
  const menu = inbound({ ...target, messageId: "cancel-config-menu", text: "/mode" });
  const sentCard = harness.transport.waitForCardSend(card => card.target.channelId === target.channelId);
  const eventId = 7310;
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(menu);
    await waitForIngressJob(harness, menu);
    const action = configChoice(await sentCard, "queue");
    const media = inbound({
      ...target,
      messageId: "cancel-config-held-media",
      text: "prepare this attachment before the card operation",
      attachments: [{
        kind: "file",
        name: "held.txt",
        size: bytes.byteLength,
        durationSeconds: 0,
        url: mediaUrl,
      }],
    });
    const preparing = harness.service.receive(media);
    await downloadStarted.promise;
    const preparingOutcome = preparing.ackReady.then(() => "ready" as const, () => "rejected" as const);

    harness.transport.pushBotEvent(cardActionEvent(eventId, action));
    await waitUntil(() => harness.transport.ackedEvents.includes(eventId), "config-card event ACK");
    store = await openChannelStore(harness.stateDir);
    const key = { botUid: harness.bot.uid, sourceKind: "bot-event" as const, sourceId: String(eventId) };
    await waitUntil(() => store!.getIngressJobBySource(key)?.state === "accepted", "durable config-card ingress");
    expect(store.getIngressJobBySource(key)).toMatchObject({ lane: "ordered", ackReady: true, state: "accepted" });

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "cancel-config-card-action",
      text: "/cancel",
    }));
    await cancel.ackReady;
    expect(await preparingOutcome).toBe("ready");

    await waitUntil(() => store!.getChatMode(harness.bot.uid, target) === "queue", "config action after cancel");
    expect(store.getIngressJobBySource(key)).toMatchObject({ state: "handed-off", ackReady: true });
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
  } finally {
    releaseDownload.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

interface ArchiveSchedulePort {
  generation(target: ChatTarget): number;
  schedule(target: ChatTarget, groupId: string, shortId: string): void;
}

function archiveSchedule(harness: CoreHarness): ArchiveSchedulePort {
  const service = harness.service as unknown as { threadArchives: ArchiveSchedulePort };
  return service.threadArchives;
}

function threadMediaMessage(
  harness: CoreHarness,
  shortId: string,
  url: string,
  size: number,
): InboundMessage {
  return inbound({
    channelType: 5,
    channelId: `group-allowed____${shortId}`,
    messageId: `thread-media-${shortId}`,
    text: "inspect this thread attachment",
    mentionedUserIds: [harness.bot.uid],
    attachments: [{ kind: "file", name: "thread.txt", size, durationSeconds: 0, url }],
  });
}

test("a held thread attachment cancels its archive schedule only after durable admission", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const shortId = "archive-after-admission";
  const message = threadMediaMessage(
    harness,
    shortId,
    "https://cdn.example.test/archive-after-admission.txt",
    4,
  );
  const target = { channelType: message.channelType, channelId: message.channelId };
  const archive = archiveSchedule(harness);
  archive.schedule(target, "group-allowed", shortId);
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.attachmentBodies.set(message.attachments[0]!.url, new TextEncoder().encode("file"));
  harness.transport.downloadAttachment = async request => {
    downloadStarted.resolve();
    await releaseDownload.promise;
    return originalDownload(request);
  };

  try {
    const receipt = harness.service.receive(message);
    await downloadStarted.promise;
    const store = await openChannelStore(harness.stateDir);
    try {
      const job = store.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
      });
      expect(job).toMatchObject({ state: "preparing", ackReady: false });
      expect(archive.generation(target)).toBe(0);
      releaseDownload.resolve();
      await receipt.ackReady;
      expect(archive.generation(target)).toBe(1);
    } finally {
      store.close();
    }
  } finally {
    releaseDownload.resolve();
    await closeCoreHarness(harness);
  }
});

test("stopping held thread media before ACK-ready leaves its archive schedule intact", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const shortId = "archive-stop-before-admission";
  const message = threadMediaMessage(
    harness,
    shortId,
    "https://cdn.example.test/archive-stop-before-admission.txt",
    4,
  );
  const target = { channelType: message.channelType, channelId: message.channelId };
  const archive = archiveSchedule(harness);
  archive.schedule(target, "group-allowed", shortId);
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  harness.transport.attachmentBodies.set(message.attachments[0]!.url, new TextEncoder().encode("file"));
  harness.transport.downloadAttachment = async request => {
    downloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(request.signal?.reason ?? new Error("fixture shutdown"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseDownload.promise, aborted.promise]);
    throw new Error("shutdown should abort before attachment bytes are returned");
  };
  const receipt = harness.service.receive(message);
  const ackOutcome = receipt.ackReady.then(() => "ready" as const, () => "rejected" as const);

  try {
    await downloadStarted.promise;
    expect(archive.generation(target)).toBe(0);
    await harness.service.stop();
    expect(await ackOutcome).toBe("rejected");
    expect(archive.generation(target)).toBe(0);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
      })).toMatchObject({ state: "preparing", ackReady: false, spoolPaths: [] });
    } finally {
      store.close();
    }
  } finally {
    releaseDownload.resolve();
    await closeCoreHarness(harness);
  }
});

test("cancelled partial multi-attachment preparation removes only files it created", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-partial-media-cancel" };
  const messageId = "partial-media-cancel";
  const existingUrl = "https://cdn.example.test/preexisting.txt";
  const createdUrl = "https://cdn.example.test/new-owned.txt";
  const heldUrl = "https://cdn.example.test/held-third.txt";
  const existingBytes = new TextEncoder().encode("preexisting");
  const createdBytes = new TextEncoder().encode("owned by this preparation");
  const partialBytes = new TextEncoder().encode("partial");
  const attachments = [
    { kind: "file" as const, name: "preexisting.txt", size: existingBytes.byteLength, durationSeconds: 0, url: existingUrl },
    { kind: "file" as const, name: "created.txt", size: createdBytes.byteLength, durationSeconds: 0, url: createdUrl },
    { kind: "file" as const, name: "held.txt", size: 100, durationSeconds: 0, url: heldUrl },
  ];
  const message = inbound({ ...target, messageId, text: "inspect all three files", attachments });
  const directory = attachmentDirectory(harness.stateDir, message, messageId);
  const existingPath = join(directory, safeAttachmentName("preexisting.txt", 0, "file", existingUrl));
  const newlyCreatedPath = join(directory, safeAttachmentName("created.txt", 1, "file", createdUrl));
  await mkdir(dirname(existingPath), { recursive: true });
  await writeFile(existingPath, existingBytes);
  harness.transport.attachmentBodies.set(createdUrl, createdBytes);
  const thirdDownloadStarted = Promise.withResolvers<void>();
  const releaseThirdDownload = Promise.withResolvers<void>();
  const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.downloadAttachment = async request => {
    if (request.url !== heldUrl) return originalDownload(request);
    await writeFile(request.path, partialBytes);
    thirdDownloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(request.signal?.reason ?? new Error("fixture media cancelled"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseThirdDownload.promise, aborted.promise]);
    return partialBytes.byteLength;
  };

  try {
    const preparing = harness.service.receive(message);
    await thirdDownloadStarted.promise;
    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "cancel-partial-media",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await preparing.ackReady;
    // Cancellation ACK is intentionally immediate; wait for materializer cleanup before inspecting spool files.
    await waitUntil(
      async () => {
        const entries = await readdir(directory);
        return entries.length === 1 && entries[0] === safeAttachmentName("preexisting.txt", 0, "file", existingUrl);
      },
      "cancelled preparation cleanup",
    );
    expect(await readFile(existingPath)).toEqual(Buffer.from(existingBytes));
    await expect(readFile(newlyCreatedPath)).rejects.toThrow();
    expect(await readdir(directory)).toEqual([safeAttachmentName("preexisting.txt", 0, "file", existingUrl)]);
  } finally {
    releaseThirdDownload.resolve();
    await closeCoreHarness(harness);
  }
});

test("startup sweeping protects an ACKed spool owned by an unknown ingress job", async () => {
  const first = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-unknown-spool-owner" };
  const url = "https://cdn.example.test/unknown-spool.txt";
  const bytes = new TextEncoder().encode("worker may still reference this path");
  const message = inbound({
    ...target,
    messageId: "unknown-spool-message",
    text: "read the durable media",
    attachments: [{ kind: "file", name: "unknown.txt", size: bytes.byteLength, durationSeconds: 0, url }],
  });
  const dispatchGate = Promise.withResolvers<void>();
  if (!Reflect.set(first.service, "startupGate", dispatchGate)) {
    throw new Error("could not install deterministic ingress dispatch gate");
  }
  first.transport.attachmentBodies.set(url, bytes);
  const spoolPath = join(
    attachmentDirectory(first.stateDir, message, message.messageId),
    safeAttachmentName("unknown.txt", 0, "file", url),
  );
  let firstStopped = false;
  let restarted: CoreHarness | undefined;

  try {
    const receipt = first.service.receive(message);
    await receipt.ackReady;
    const store = await openChannelStore(first.stateDir);
    let jobId: number;
    try {
      const job = store.getIngressJobBySource({
        botUid: first.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
      });
      if (job === null) throw new Error("prepared media ingress job disappeared");
      expect(job).toMatchObject({ state: "accepted", ackReady: true, spoolPaths: [spoolPath] });
      jobId = job.id;
      expect(store.markIngressDispatchIntent(jobId)).toBe(true);
      expect(store.markIngressUnknown(jobId)).toBe(true);
      expect(store.getIngressJob(jobId)).toMatchObject({ state: "unknown", ackReady: true, spoolPaths: [spoolPath] });
    } finally {
      store.close();
    }
    const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    await utimes(dirname(spoolPath), old, old);
    dispatchGate.resolve();
    await first.service.stop();
    firstStopped = true;

    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport: new FakeTransport({ bot: first.bot }),
      attachments: { enabled: true, maxBytes: MEDIA_LIMIT },
    });
    expect(await readFile(spoolPath)).toEqual(Buffer.from(bytes));
  } finally {
    dispatchGate.resolve();
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeCoreHarness(restarted ?? first);
  }
});

test("a readdir failure during spool enumeration cannot make media ACK-ready", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-spool-enumeration-failure" };
  const url = "https://cdn.example.test/enumeration-failure.txt";
  const bytes = new TextEncoder().encode("complete but not yet durably enumerated");
  const message = inbound({
    ...target,
    messageId: "enumeration-failure-message",
    text: "read the complete media",
    attachments: [{ kind: "file", name: "complete.txt", size: bytes.byteLength, durationSeconds: 0, url }],
  });
  harness.transport.attachmentBodies.set(url, bytes);
  const spoolPath = join(
    attachmentDirectory(harness.stateDir, message, message.messageId),
    safeAttachmentName("complete.txt", 0, "file", url),
  );
  const materializer = (harness.service as unknown as {
    materializer: { spoolPaths(message: InboundMessage): Promise<string[]> };
  }).materializer;
  const enumerate = materializer.spoolPaths.bind(materializer);
  materializer.spoolPaths = async currentMessage => {
    const directory = attachmentDirectory(harness.stateDir, currentMessage, currentMessage.messageId);
    const displaced = `${directory}.readdir-failure-fixture`;
    await rename(directory, displaced);
    try {
      // A self-referential symlink makes readdir fail with ELOOP on every privilege level.
      await symlink(directory, directory);
      return await enumerate(currentMessage);
    } finally {
      await unlink(directory).catch(() => {});
      await rename(displaced, directory);
    }
  };

  try {
    const receipt = harness.service.receive(message);
    await expect(receipt.ackReady).rejects.toThrow();
    expect(await readFile(spoolPath)).toEqual(Buffer.from(bytes));
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
      })).toMatchObject({ state: "preparing", ackReady: false, spoolPaths: [] });
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});
