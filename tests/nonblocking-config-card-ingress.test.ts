import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ConfigActionSuspendedError,
  createConfigMenuDesk,
  type ConfigCardActionSnapshot,
} from "../src/commands/command-cards.ts";
import type { BotCardAction, BotIdentity } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import { FakeTransport } from "./fixtures/core-fakes.ts";

const BOT: BotIdentity = { uid: "config-ingress-bot", name: "Config ingress" };


function findSubmitId(value: unknown): string | null {
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = findSubmitId(item);
      if (found !== null) return found;
    }
    return null;
  }
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  if (record.type === "Action.Submit" && typeof record.id === "string") return record.id;
  for (const child of Object.values(record)) {
    const found = findSubmitId(child);
    if (found !== null) return found;
  }
  return null;
}

test("a later accepted text setting wins after an earlier card action finishes", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-config-reservation-"));
  const store = await openChannelStore(root);
  store.setOwnerId(BOT.uid, "owner-1");
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const accepted: ConfigCardActionSnapshot[] = [];
  const desk = createConfigMenuDesk({
    store,
    transport,
    botUid: () => BOT.uid,
    stopping: () => false,
    chatAllowed: () => true,
    getAcceptedEvent: () => false,
    applyChoice: async (_kind, target, value) => {
      entered.resolve();
      await release.promise;
      store.setChatMode(BOT.uid, target, value === "queue" ? "queue" : "steer");
      return "card-applied";
    },
    acceptAction: (snapshot) => { accepted.push(snapshot); return "accepted"; },
    enqueueText: () => {},
  });
  const target = { channelType: 1 as const, channelId: "dm-config-reservation" };
  const ticket = desk.begin(target, "mode");
  try {
    await desk.open(target, {
      kind: "mode",
      current: "steer",
      choices: [{ title: "Queue", value: "queue" }],
      plain: "choose mode",
    }, ticket);
    const card = transport.sentCards[0];
    if (card === undefined) throw new Error("config card was not sent");
    const actionId = findSubmitId(card.frame.card);
    if (actionId === null) throw new Error("submit action missing");
    const action: BotCardAction = {
      ...target,
      messageId: card.messageId,
      actionId,
      operatorUid: "owner-1",
      inputs: { config_value: "queue" },
    };
    expect(await desk.handleAction(action, 23)).toBe("handled");
    const snapshot = accepted[0];
    if (snapshot === undefined) throw new Error("accepted action snapshot missing");
    const actionReservation = desk.reserveApply("mode", target, "bot-event:23");
    const checkpoint = { dispatchIntent: false, appliedResult: null as string | null };
    const actionRun = desk.resumeAcceptedAction(snapshot, actionReservation, {
      get dispatchIntent() { return checkpoint.dispatchIntent; },
      get appliedResult() { return checkpoint.appliedResult; },
      markDispatchIntent: () => { checkpoint.dispatchIntent = true; return true; },
      markAppliedResult: (result) => { checkpoint.appliedResult = result; return true; },
      markUnknown: () => {},
    });
    await entered.promise;
    const textReservation = desk.reserveApply("mode", target, "recv:24");
    const textRun = desk.serialize("mode", target, () => {
      store.setChatMode(BOT.uid, target, "steer");
      return "text-applied";
    });
    release.resolve();
    await Promise.all([actionRun, textRun]);
    expect(store.getChatMode(BOT.uid, target)).toBe("steer");
    await desk.resumeAcceptedAction(
      snapshot,
      desk.reserveApply("mode", target, "bot-event:23"),
      {
        get dispatchIntent() { return checkpoint.dispatchIntent; },
        get appliedResult() { return checkpoint.appliedResult; },
        markDispatchIntent: () => { checkpoint.dispatchIntent = true; return true; },
        markAppliedResult: (result) => { checkpoint.appliedResult = result; return true; },
        markUnknown: () => {},
      },
    );
    expect(store.getChatMode(BOT.uid, target)).toBe("steer");
    textReservation.release();
  } finally {
    release.resolve();
    ticket.release();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
// Duplicate durable event sources must be recognized before a stale/missing in-memory menu can emit a reopen reply.
test("config card duplicate event is acknowledged before menu lookup or reapplication", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-config-ingress-"));
  const store = await openChannelStore(root);
  store.setOwnerId(BOT.uid, "owner-1");
  const transport = new FakeTransport();
  const texts: string[] = [];
  let applyCalls = 0;
  let acceptCalls = 0;
  const desk = createConfigMenuDesk({
    store,
    transport,
    botUid: () => BOT.uid,
    stopping: () => false,
    chatAllowed: () => true,
    getAcceptedEvent: (eventId: number) => eventId === 17,
    applyChoice: async () => { applyCalls += 1; return "applied"; },
    acceptAction: () => { acceptCalls += 1; return "accepted"; },
    enqueueText: (_target, text) => { texts.push(text); },
  });
  const action: BotCardAction = {
    channelType: 1,
    channelId: "dm-config-ingress",
    messageId: "old-card-message",
    actionId: "cfg:switch:00000000-0000-4000-8000-000000000001",
    operatorUid: "owner-1",
    inputs: { config_value: "fixture/fast" },
  };
  try {
    expect(await desk.handleAction(action, 17)).toBe("handled");
    expect(texts).toEqual([]);
    expect(acceptCalls).toBe(0);
    expect(applyCalls).toBe(0);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("stopping before config dispatch preserves the accepted action checkpoint", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-config-stop-"));
  const store = await openChannelStore(root);
  store.setOwnerId(BOT.uid, "owner-1");
  const target = { channelType: 1 as const, channelId: "dm-config-stop" };
  store.setChatMode(BOT.uid, target, "steer");
  let stopping = true;
  let dispatchIntent = false;
  let unknown = false;
  const desk = createConfigMenuDesk({
    store,
    transport: new FakeTransport(),
    botUid: () => BOT.uid,
    stopping: () => stopping,
    chatAllowed: () => true,
    getAcceptedEvent: () => false,
    applyChoice: async () => {
      store.setChatMode(BOT.uid, target, "queue");
      return "applied";
    },
    acceptAction: () => "accepted",
    enqueueText: () => {},
  });
  const snapshot: ConfigCardActionSnapshot = {
    eventId: 51,
    kind: "mode",
    actionId: "cfg:mode:00000000-0000-4000-8000-000000000001",
    messageId: "config-card",
    target,
    ownerUid: "owner-1",
    value: "queue",
    cardSeq: 1,
  };
  try {
    await expect(desk.resumeAcceptedAction(
      snapshot,
      desk.reserveApply("mode", target, "bot-event:51"),
      {
        dispatchIntent,
        appliedResult: null,
        markDispatchIntent: () => { dispatchIntent = true; return true; },
        markAppliedResult: () => true,
        markUnknown: () => { unknown = true; },
      },
    )).rejects.toBeInstanceOf(ConfigActionSuspendedError);
    expect(store.getChatMode(BOT.uid, target)).toBe("steer");
    expect(dispatchIntent).toBe(false);
    expect(unknown).toBe(false);
  } finally {
    stopping = false;
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("config dispatch-intent without a durable result is never replayed", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-config-unknown-"));
  const store = await openChannelStore(root);
  store.setOwnerId(BOT.uid, "owner-1");
  const target = { channelType: 1 as const, channelId: "dm-config-unknown" };
  store.setChatMode(BOT.uid, target, "steer");
  let unknown = false;
  const desk = createConfigMenuDesk({
    store,
    transport: new FakeTransport(),
    botUid: () => BOT.uid,
    stopping: () => false,
    chatAllowed: () => true,
    getAcceptedEvent: () => false,
    applyChoice: async () => {
      store.setChatMode(BOT.uid, target, "queue");
      return "applied";
    },
    acceptAction: () => "accepted",
    enqueueText: () => {},
  });
  const snapshot: ConfigCardActionSnapshot = {
    eventId: 52,
    kind: "mode",
    actionId: "cfg:mode:00000000-0000-4000-8000-000000000002",
    messageId: "config-card",
    target,
    ownerUid: "owner-1",
    value: "queue",
    cardSeq: 1,
  };
  try {
    await desk.resumeAcceptedAction(snapshot, desk.reserveApply("mode", target, "bot-event:52"), {
      dispatchIntent: true,
      appliedResult: null,
      markDispatchIntent: () => true,
      markAppliedResult: () => true,
      markUnknown: () => { unknown = true; },
    });
    expect(store.getChatMode(BOT.uid, target)).toBe("steer");
    expect(unknown).toBe(true);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
