import { Database } from "bun:sqlite";
import { expect, test, vi } from "bun:test";
import {
  mkdir,
  readFile,
  readdir,
  rename,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { CONFIG_MENU_TTL_MS } from "../src/commands/command-cards.ts";
import type { BotCardAction, BotEvent, ChatTarget, InboundMessage } from "../src/contracts.ts";
import { attachmentDirectory, safeAttachmentName } from "../src/inbound/attachments.ts";
import { recvIngressSourceId, type IngressValidatedConfigAction } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForIngressJob,
} from "./fixtures/core-fakes.ts";
import type { CoreHarness, SentCard } from "./fixtures/core-fakes.ts";

const MEDIA_LIMIT = 1024 * 1024;

async function waitUntil(
  check: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 4_000,
  describeFailure?: () => string,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await check()) return;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  const details = describeFailure?.();
  throw new Error(`did not observe ${label}${details ? ` (${details})` : ""}`);
}
async function waitForPromise<T>(promise: Promise<T>, label: string): Promise<T> {
  let result:
    | { kind: "fulfilled"; value: T }
    | { kind: "rejected"; error: unknown }
    | undefined;
  void promise.then(
    value => { result = { kind: "fulfilled", value }; },
    error => { result = { kind: "rejected", error }; },
  );
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (result?.kind === "fulfilled") return result.value;
    if (result?.kind === "rejected") throw result.error;
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  throw new Error(`did not settle ${label}`);
}

async function flushTurns(turns = 8): Promise<void> {
  for (let index = 0; index < turns; index += 1) {
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
}

function cardNodes(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) return value.flatMap(cardNodes);
  if (value === null || typeof value !== "object") return [];
  const record = value as Record<string, unknown>;
  return [record, ...Object.values(record).flatMap(cardNodes)];
}

function modeChoice(card: SentCard, value: string): BotCardAction {
  const nodes = cardNodes(card.frame.card);
  const input = nodes.find(node => node.type === "Input.ChoiceSet");
  const submit = nodes.find(node =>
    node.type === "Action.Submit"
    && typeof node.id === "string"
    && node.id.startsWith("cfg:mode:"));
  if (input === undefined || submit === undefined) {
    throw new Error("mode config card is missing its choice input or submit action");
  }
  return {
    ...card.target,
    messageId: card.messageId,
    actionId: String(submit.id),
    operatorUid: "owner-1",
    inputs: { [String(input.id)]: value },
  };
}

async function openModeCard(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
): Promise<SentCard> {
  const sent = harness.transport.waitForCardSend(card =>
    card.target.channelType === target.channelType && card.target.channelId === target.channelId);
  const message = inbound({ ...target, messageId, text: "/mode" });
  await harness.service.submitInbound(message);
  await waitForIngressJob(harness, message);
  return sent;
}

function receiveSource(harness: CoreHarness, message: InboundMessage) {
  return {
    botUid: harness.bot.uid,
    sourceKind: "recv" as const,
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
  };
}

function eventSource(harness: CoreHarness, eventId: number) {
  return { botUid: harness.bot.uid, sourceKind: "bot-event" as const, sourceId: String(eventId) };
}

async function waitForEventAck(transport: FakeTransport, eventId: number): Promise<void> {
  await waitUntil(() => transport.ackedEvents.includes(eventId), `Bot event ${eventId} ACK`);
}

function cardAcceptanceTrigger(harness: CoreHarness, operation: "install" | "remove"): void {
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    database.exec(operation === "install"
      ? `CREATE TRIGGER fail_final_config_ingress
         BEFORE INSERT ON ingress_jobs
         WHEN NEW.source_kind = 'bot-event'
         BEGIN SELECT RAISE(ABORT, 'fixture config ingress insertion failure'); END;`
      : "DROP TRIGGER IF EXISTS fail_final_config_ingress");
  } finally {
    database.close();
  }
}

function releaseConfigReservations(harness: CoreHarness): void {
  const service = harness.service as unknown as {
    ingressConfigReservations?: Map<string, { release(): void }>;
  };
  for (const reservation of service.ingressConfigReservations?.values() ?? []) reservation.release();
}

interface ArchiveSchedulePort {
  generation(target: ChatTarget): number;
  schedule(target: ChatTarget, groupId: string, shortId: string): void;
  run(target: ChatTarget, progressTail: Promise<void>): Promise<void>;
}

function archiveSchedule(harness: CoreHarness): ArchiveSchedulePort {
  return (harness.service as unknown as { threadArchives: ArchiveSchedulePort }).threadArchives;
}

test("cancel releases a text mode reservation so an accepted config-card action applies", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    attachments: { enabled: true, maxBytes: MEDIA_LIMIT },
  });
  const target: ChatTarget = { channelType: 1, channelId: "dm-cancel-mode-reservation" };
  const card = await openModeCard(harness, target, "cancel-mode-menu");
  const mediaStarted = Promise.withResolvers<void>();
  harness.transport.downloadAttachment = async request => {
    mediaStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(request.signal?.reason ?? new Error("fixture media cancelled"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    await aborted.promise;
    throw new Error("cancelled download unexpectedly completed");
  };
  const media = inbound({
    ...target,
    messageId: "cancel-mode-held-media",
    text: "must not become a task",
    attachments: [{
      kind: "file",
      name: "held.txt",
      size: 4,
      durationSeconds: 0,
      url: "https://cdn.example.test/cancel-mode-held.txt",
    }],
  });
  const textMode = inbound({ ...target, messageId: "cancel-mode-text-command", text: "/mode queue" });
  const eventId = 7461;
  const acceptedEvent = cardActionEvent(eventId, modeChoice(card, "queue"));
  let store: Awaited<ReturnType<typeof openChannelStore>> | undefined;

  try {
    const preparing = harness.service.receive(media);
    await mediaStarted.promise;
    const textModeReceipt = harness.service.receive(textMode);
    await textModeReceipt.ackReady;

    harness.transport.pushBotEvent(acceptedEvent);
    await waitForEventAck(harness.transport, eventId);
    store = await openChannelStore(harness.stateDir);
    await waitUntil(() => store!.getIngressJobBySource(eventSource(harness, eventId))?.state === "accepted",
      "durable config-card action acceptance");
    expect(store.getIngressJobBySource(eventSource(harness, eventId))).toMatchObject({
      state: "accepted",
      lane: "ordered",
      ackReady: true,
      payload: { kind: "bot-event", eventId, acceptedAction: { kind: "mode", value: "queue" } },
    });

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "cancel-mode-reservation",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await preparing.ackReady;
    await waitUntil(() =>
      store!.getIngressJobBySource(receiveSource(harness, media))?.state === "cancelled"
      && store!.getIngressJobBySource(receiveSource(harness, textMode))?.state === "cancelled",
    "media and text mode cancellation");
    await waitUntil(() =>
      store!.getChatMode(harness.bot.uid, target) === "queue"
      && store!.getIngressJobBySource(eventSource(harness, eventId))?.state === "handed-off",
    "accepted card action after cancellation");

    expect(harness.transport.ackedEvents).toEqual([eventId]);
    expect(store.getIngressJobBySource(eventSource(harness, eventId))).toMatchObject({
      state: "handed-off",
      sourceKind: "bot-event",
      sourceId: String(eventId),
      ackReady: true,
      consumerKind: "operation",
      consumerId: `bot-event:${eventId}`,
    });
    expect(store.getBotEventCursor(harness.bot.uid)).toBe(eventId);
    expect(store.getTaskByMessageId(harness.bot.uid, target, media.messageId)).toBeNull();
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.cardEdits.filter(edit => edit.messageId === card.messageId)).toHaveLength(1);
  } finally {
    releaseConfigReservations(harness);
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a retryable media preparation failure retains its same-chat order ahead of later input", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-media-retry-order" };
  const url = "https://cdn.example.test/media-retry-order.txt";
  const bytes = new TextEncoder().encode("first accepted media");
  const media = inbound({
    ...target,
    messageId: "media-retry-first",
    text: "inspect the first media",
    attachments: [{ kind: "file", name: "first.txt", size: bytes.byteLength, durationSeconds: 0, url }],
  });
  harness.transport.attachmentBodies.set(url, bytes);
  const spoolPath = join(
    attachmentDirectory(harness.stateDir, media, media.messageId),
    safeAttachmentName("first.txt", 0, "file", url),
  );
  const materializer = (harness.service as unknown as {
    materializer: { spoolPaths(message: InboundMessage): Promise<string[]> };
  }).materializer;
  const enumerate = materializer.spoolPaths.bind(materializer);
  let failEnumeration = true;
  materializer.spoolPaths = async currentMessage => {
    if (!failEnumeration || currentMessage.messageId !== media.messageId) return await enumerate(currentMessage);
    failEnumeration = false;
    const directory = attachmentDirectory(harness.stateDir, currentMessage, currentMessage.messageId);
    const displaced = `${directory}.retry-order-fixture`;
    await rename(directory, displaced);
    try {
      await symlink(directory, directory);
      return await enumerate(currentMessage);
    } finally {
      await unlink(directory).catch(() => {});
      await rename(displaced, directory);
    }
  };
  const later = inbound({
    ...target,
    messageId: "media-retry-later-text",
    text: "later same-chat message",
  });
  const dispatchGate = Promise.withResolvers<void>();
  let store: ChannelStore | undefined;

  try {
    const firstAttempt = harness.service.receive(media);
    const firstAttemptOutcome = await waitForPromise(
      firstAttempt.ackReady.then(
        () => "ready" as const,
        () => "rejected" as const,
      ),
      "failed media preparation",
    );
    expect(firstAttemptOutcome).toBe("rejected");
    await flushTurns();
    expect(await readFile(spoolPath)).toEqual(Buffer.from(bytes));

    if (!Reflect.set(harness.service, "startupGate", dispatchGate)) {
      throw new Error("could not install deterministic ingress dispatch gate");
    }
    const laterReceipt = harness.service.receive(later);
    await waitForPromise(laterReceipt.ackReady, "later input ACK readiness");
    store = await openChannelStore(harness.stateDir);
    await flushTurns();
    expect(store.getIngressJobBySource(receiveSource(harness, media))).toMatchObject({
      state: "preparing",
      ackReady: false,
      spoolPaths: [],
    });
    expect(store.getIngressJobBySource(receiveSource(harness, later))).toMatchObject({
      state: "accepted",
      ackReady: true,
      consumerKind: null,
    });
    expect(store.getTaskByMessageId(harness.bot.uid, target, media.messageId)).toBeNull();
    expect(store.getTaskByMessageId(harness.bot.uid, target, later.messageId)).toBeNull();
    expect(harness.workers.prompts).toEqual([]);

    const redelivery = harness.service.receive(media);
    await waitForPromise(redelivery.ackReady, "media redelivery ACK readiness");
    expect(store.getIngressJobBySource(receiveSource(harness, media))).toMatchObject({
      state: "accepted",
      ackReady: true,
      spoolPaths: [spoolPath],
    });
    expect(harness.transport.downloadRequests.map(request => request.url)).toEqual([url]);
    dispatchGate.resolve();
    await waitUntil(
      () => {
        const mediaJob = store?.getIngressJobBySource(receiveSource(harness, media));
        const laterJob = store?.getIngressJobBySource(receiveSource(harness, later));
        return mediaJob?.state === "handed-off"
          && laterJob?.state === "handed-off"
          && harness.workers.prompts.length >= 2;
      },
      "media retry and later-message handoffs with two prompts",
      3_000,
      () => {
        const mediaJob = store?.getIngressJobBySource(receiveSource(harness, media));
        const laterJob = store?.getIngressJobBySource(receiveSource(harness, later));
        return [
          `media=${mediaJob?.state ?? "missing"}/${mediaJob?.consumerId ?? "no-consumer"}`,
          `later=${laterJob?.state ?? "missing"}/${laterJob?.consumerId ?? "no-consumer"}`,
          `prompts=${JSON.stringify(harness.workers.prompts.map(prompt => prompt.text))}`,
        ].join(", ");
      },
    );

    expect(harness.workers.prompts.map(prompt => prompt.text)).toEqual([
      expect.stringContaining("inspect the first media"),
      "later same-chat message",
    ]);
    expect(store.getTaskByMessageId(harness.bot.uid, target, media.messageId)).not.toBeNull();
    expect(store.getTaskByMessageId(harness.bot.uid, target, later.messageId)).not.toBeNull();
    expect(harness.workers.prompts).toHaveLength(2);
    expect(await readFile(spoolPath)).toEqual(Buffer.from(bytes));
  } finally {
    dispatchGate.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a validated config-card event remains retryable after its menu expires", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const target: ChatTarget = { channelType: 1, channelId: "dm-card-expiry-retry" };
  const card = await openModeCard(harness, target, "card-expiry-menu");
  const eventId = 7462;
  const event: BotEvent = cardActionEvent(eventId, modeChoice(card, "queue"));
  const expiryAdvanceMs = CONFIG_MENU_TTL_MS + 1_000;
  const firstInsertFailure = Promise.withResolvers<void>();
  const ingress = (harness.service as unknown as {
    ingress: {
      acceptBotEvent(
        action: BotCardAction,
        acceptedEventId: number,
        actionSnapshot: IngressValidatedConfigAction,
      ): "accepted" | "duplicate" | "retry";
    };
  }).ingress;
  const accept = ingress.acceptBotEvent.bind(ingress);
  let acceptanceAttempts = 0;
  ingress.acceptBotEvent = (action, acceptedEventId, actionSnapshot) => {
    acceptanceAttempts += 1;
    const result = accept(action, acceptedEventId, actionSnapshot);
    if (result === "retry") firstInsertFailure.resolve();
    return result;
  };
  let store: ChannelStore | undefined;

  try {
    cardAcceptanceTrigger(harness, "install");
    harness.transport.pushBotEvent(event);
    await waitForPromise(firstInsertFailure.promise, "first config-card ingress retry");
    await flushTurns(20);
    expect(harness.transport.ackedEvents).toEqual([]);
    expect(acceptanceAttempts).toBe(1);

    cardAcceptanceTrigger(harness, "remove");
    const pollsBeforeRetry = harness.transport.eventFetches.filter(cursor => cursor === 0).length;
    vi.advanceTimersByTime(5_000);
    await flushTurns();
    await waitUntil(
      () => harness.transport.eventFetches.filter(cursor => cursor === 0).length > pollsBeforeRetry,
      "event consumer retry poll at the original cursor",
    );
    vi.advanceTimersByTime(expiryAdvanceMs);
    await flushTurns();
    harness.transport.pushBotEvent(event);
    await waitForEventAck(harness.transport, eventId);

    store = await openChannelStore(harness.stateDir);
    await waitUntil(() => store!.getIngressJobBySource(eventSource(harness, eventId))?.state === "handed-off",
      "retried config-card handoff after menu expiry");
    expect(acceptanceAttempts).toBe(2);
    expect(harness.transport.ackedEvents).toEqual([eventId]);
    expect(store.getBotEventCursor(harness.bot.uid)).toBe(eventId);
    expect(store.getIngressJobBySource(eventSource(harness, eventId))).toMatchObject({
      state: "handed-off",
      sourceKind: "bot-event",
      sourceId: String(eventId),
      ackReady: true,
      consumerKind: "operation",
      consumerId: `bot-event:${eventId}`,
    });
    expect(store.getChatMode(harness.bot.uid, target)).toBe("queue");
    expect(harness.transport.cardEdits.filter(edit => edit.messageId === card.messageId)).toHaveLength(1);
    expect(harness.transport.sent.some(message => message.text.includes("这张配置卡片已失效"))).toBe(false);
  } finally {
    vi.useRealTimers();
    cardAcceptanceTrigger(harness, "remove");
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("cancelling after download while spool enumeration is held removes only the new file", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MEDIA_LIMIT } });
  const target: ChatTarget = { channelType: 1, channelId: "dm-cancel-enumerating-media" };
  const existingUrl = "https://cdn.example.test/enumerating-existing.txt";
  const createdUrl = "https://cdn.example.test/enumerating-created.txt";
  const existingBytes = new TextEncoder().encode("prior owned file");
  const createdBytes = new TextEncoder().encode("created by the cancelled admission");
  const message = inbound({
    ...target,
    messageId: "cancel-after-download-enumerating",
    text: "inspect both files",
    attachments: [
      { kind: "file", name: "existing.txt", size: existingBytes.byteLength, durationSeconds: 0, url: existingUrl },
      { kind: "file", name: "created.txt", size: createdBytes.byteLength, durationSeconds: 0, url: createdUrl },
    ],
  });
  const directory = attachmentDirectory(harness.stateDir, message, message.messageId);
  const existingName = safeAttachmentName("existing.txt", 0, "file", existingUrl);
  const createdName = safeAttachmentName("created.txt", 1, "file", createdUrl);
  const existingPath = join(directory, existingName);
  const createdPath = join(directory, createdName);
  await mkdir(dirname(existingPath), { recursive: true });
  await writeFile(existingPath, existingBytes);
  harness.transport.attachmentBodies.set(createdUrl, createdBytes);
  const enumerationStarted = Promise.withResolvers<void>();
  const releaseEnumeration = Promise.withResolvers<void>();
  const materializer = (harness.service as unknown as {
    materializer: { spoolPaths(message: InboundMessage): Promise<string[]> };
  }).materializer;
  const enumerate = materializer.spoolPaths.bind(materializer);
  materializer.spoolPaths = async currentMessage => {
    if (currentMessage.messageId === message.messageId) {
      enumerationStarted.resolve();
      await releaseEnumeration.promise;
    }
    return await enumerate(currentMessage);
  };
  let store: ChannelStore | undefined;

  try {
    const preparing = harness.service.receive(message);
    await waitForPromise(enumerationStarted.promise, "post-download spool enumeration");
    expect((await readdir(directory)).sort()).toEqual([existingName, createdName].sort());
    expect(await readFile(createdPath)).toEqual(Buffer.from(createdBytes));

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "cancel-during-spool-enumeration",
      text: "/cancel",
    }));
    await waitForPromise(cancel.ackReady, "cancel ACK readiness");
    store = await openChannelStore(harness.stateDir);
    await waitUntil(() => store!.getIngressJobBySource(receiveSource(harness, message))?.state === "cancelled",
      "durable cancellation while spool paths are being enumerated");
    releaseEnumeration.resolve();
    await waitForPromise(preparing.ackReady, "cancelled media ACK readiness");
    await waitUntil(async () => !(await readdir(directory)).includes(createdName), "cancelled spool cleanup");

    expect(await readdir(directory)).toEqual([existingName]);
    expect(await readFile(existingPath)).toEqual(Buffer.from(existingBytes));
    const createdFileState = await readFile(createdPath).then(
      () => "present" as const,
      () => "missing" as const,
    );
    expect(createdFileState).toBe("missing");
    expect(store.getIngressJobBySource(receiveSource(harness, message))).toMatchObject({
      state: "cancelled",
      ackReady: true,
      spoolPaths: [],
    });
    expect(harness.transport.downloadRequests.map(request => request.url)).toEqual([createdUrl]);
    expect(harness.workers.prompts).toEqual([]);
    expect(store.getTaskByMessageId(harness.bot.uid, target, message.messageId)).toBeNull();
  } finally {
    releaseEnumeration.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("redelivery of an unknown terminal ingress does not cancel a later archive plan", async () => {
  const harness = await createCoreHarness();
  const shortId = "unknown-redelivery-archive";
  const message = inbound({
    channelType: 5,
    channelId: `group-allowed____${shortId}`,
    messageId: "unknown-terminal-redelivery",
    text: "this accepted input has an uncertain outcome",
    mentionedUserIds: [harness.bot.uid],
  });
  const target = { channelType: message.channelType, channelId: message.channelId };
  const dispatchGate = Promise.withResolvers<void>();
  const archive = archiveSchedule(harness);
  let store: Awaited<ReturnType<typeof openChannelStore>> | undefined;

  try {
    if (!Reflect.set(harness.service, "startupGate", dispatchGate)) {
      throw new Error("could not install deterministic ingress dispatch gate");
    }
    const first = harness.service.receive(message);
    await first.ackReady;
    store = await openChannelStore(harness.stateDir);
    const key = receiveSource(harness, message);
    const accepted = store.getIngressJobBySource(key);
    if (accepted === null) throw new Error("initial receive ingress was not durably accepted");
    expect(accepted).toMatchObject({ state: "accepted", ackReady: true, spoolPaths: [] });
    expect(store.markIngressDispatchIntent(accepted.id)).toBe(true);
    expect(store.markIngressUnknown(accepted.id)).toBe(true);
    expect(store.getIngressJob(accepted.id)).toMatchObject({ state: "unknown", ackReady: true });

    dispatchGate.resolve();
    await waitUntil(
      () => (harness.service as unknown as { ingress: { pendingCount(): number } }).ingress.pendingCount() === 0,
      "uncertain receive candidate terminal settlement",
    );
    const generationAfterAdmission = archive.generation(target);
    archive.schedule(target, "group-allowed", shortId);

    const redelivery = harness.service.receive(message);
    await redelivery.ackReady;
    await waitUntil(
      () => (harness.service as unknown as { ingress: { pendingCount(): number } }).ingress.pendingCount() === 0,
      "unknown redelivery settlement",
    );
    expect(archive.generation(target)).toBe(generationAfterAdmission);
    expect(store.getIngressJobBySource(key)).toMatchObject({ state: "unknown", ackReady: true });
    expect(store.getTaskByMessageId(harness.bot.uid, target, message.messageId)).toBeNull();
    expect(harness.workers.prompts).toEqual([]);

    await archive.run(target, Promise.resolve());
    expect(harness.transport.managementCalls.filter(call => call.action === "thread-archive")).toEqual([
      { action: "thread-archive", groupId: "group-allowed", shortId },
    ]);
  } finally {
    dispatchGate.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});
