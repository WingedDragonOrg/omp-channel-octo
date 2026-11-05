import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CommandCardView } from "../src/cards/command-card.ts";
import { COMMAND_INPUT_ID } from "../src/cards/command-card.ts";
import { createBotEventLoop, type BotEventLoop, type BotEventOutcome } from "../src/bot-events.ts";
import {
  createConfigMenuDesk,
  type ConfigApplyReservation,
  type ConfigCardActionSnapshot,
  type ConfigMenuDeps,
} from "../src/commands/command-cards.ts";
import type {
  BotCardAction,
  BotEvent,
  BotEventFetch,
  CardFrame,
  ChatTarget,
  InboundMessage,
} from "../src/contracts.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import {
  createQuestionDesk,
  type QuestionDesk,
  type QuestionDeskDeps,
  type TextQuestionConsumeOutcome,
  type TextQuestionRouteSnapshot,
} from "../src/questions.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { FakeTransport, inbound } from "./fixtures/core-fakes.ts";

const BOT_UID = "nonblocking-domain-bot";
const OWNER_UID = "owner-1";
const TARGET: ChatTarget = { channelType: 1, channelId: "question-owner" };

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 20; index += 1) await Promise.resolve();
}



async function withStore(
  name: string,
  run: (store: ChannelStore, root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), `${name}-`));
  const store = await openChannelStore(root);
  store.setBotUid(BOT_UID);
  store.setOwnerId(BOT_UID, OWNER_UID);
  store.ensureBinding({
    botUid: BOT_UID,
    ...TARGET,
    sessionId: "session-1",
    sessionFile: null,
    cwd: root,
  });
  try {
    await run(store, root);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

function seedTask(store: ChannelStore, id: string): TaskRecord {
  const task: TaskRecord = {
    id,
    botUid: BOT_UID,
    ...TARGET,
    messageId: `message-${id}`,
    senderId: OWNER_UID,
    text: `task-${id}`,
    sessionId: "session-1",
    cwd: "/tmp",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
  const claim: InboundClaim = {
    botUid: BOT_UID,
    target: TARGET,
    messageId: task.messageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };
  expect(store.acceptTask(claim, task)).toBe(true);
  return task;
}

function seedQuestion(
  store: ChannelStore,
  id: string,
  task: TaskRecord,
  options: readonly string[] = [],
): void {
  store.createQuestion({
    id,
    taskId: task.id,
    botUid: BOT_UID,
    ...TARGET,
    payload: JSON.stringify({
      sourceId: id,
      question: `question-${id}`,
      options: options.map((label) => ({ label })),
      multi: false,
    }),
  });
}

function questionDesk(
  store: ChannelStore,
  transport: FakeTransport = new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture" } }),
  operations: Promise<void>[] = [],
): { desk: QuestionDesk; texts: string[]; wakeTexts: string[] } {
  const texts: string[] = [];
  const wakeTexts: string[] = [];
  const pendingNotices = new Map<string, string>();
  const deps: QuestionDeskDeps = {
    store,
    transport,
    botUid: () => BOT_UID,
    stopping: () => false,
    enqueueText: (_target, text) => { texts.push(text); },
    persistWakeTurn: (_message, text, sourceId) => {
      wakeTexts.push(text);
      return sourceId;
    },
    publishWakeTurn: () => {},
    persistNotice: (_message, text, sourceId) => {
      pendingNotices.set(sourceId, text);
      return sourceId;
    },
    publishNotice: () => {
      texts.push(...pendingNotices.values());
      pendingNotices.clear();
    },
    operationSet: (operation) => { operations.push(operation); },
    askTimeoutMs: () => 60_000,
    displayNames: async (uids) => new Map(uids.map((uid) => [uid, uid])),
    stopTurnForQuestion: () => {},
    wakeTurn: () => {},
    memberAnswerAllowed: () => false,
  };
  return { desk: createQuestionDesk(deps), texts, wakeTexts };
}

function durableQuestionDesk(
  store: ChannelStore,
  transport: FakeTransport,
): {
  desk: QuestionDesk;
  wakeTaskIds: string[];
  publishedWakeTaskIds: string[];
  texts: string[];
  operations: Promise<void>[];
} {
  const wakeTaskIds: string[] = [];
  const publishedWakeTaskIds: string[] = [];
  const texts: string[] = [];
  const operations: Promise<void>[] = [];
  const desk = createQuestionDesk({
    store,
    transport,
    botUid: () => BOT_UID,
    stopping: () => false,
    enqueueText: (_target, text) => { texts.push(text); },
    persistWakeTurn: (message, text, sourceId) => {
      const taskId = `ask-wake:${sourceId}`;
      const messageId = `answer:${taskId}`;
      const senderId = store.getOwnerId(BOT_UID)?.trim() || BOT_UID;
      const binding = store.getBinding(BOT_UID, message);
      if (binding === null) throw new Error("question wake has no durable session binding");
      const task: TaskRecord = {
        id: taskId,
        botUid: BOT_UID,
        channelType: message.channelType,
        channelId: message.channelId,
        messageId,
        senderId,
        text,
        sessionId: binding.sessionId,
        cwd: binding.cwd,
        state: "queued",
        resultText: null,
        errorText: null,
        messageSeq: message.messageSeq,
        historyEligible: true,
        relaySuppressed: false,
      };
      const claim: InboundClaim = {
        botUid: BOT_UID,
        target: message,
        messageId,
        senderId,
        text,
        supported: true,
        kind: "task",
      };
      const existing = store.getTask(taskId);
      if (existing === null && !store.acceptTask(claim, task)) {
        throw new Error("question wake task could not be durably accepted");
      }
      const persisted = store.getTask(taskId);
      if (persisted === null || persisted.text !== text || persisted.messageId !== messageId) {
        throw new Error("stable question wake identity conflicts with a different task");
      }
      wakeTaskIds.push(taskId);
      return taskId;
    },
    publishWakeTurn: (taskId) => { publishedWakeTaskIds.push(taskId); },
    persistNotice: (_message, _text, sourceId) => sourceId,
    publishNotice: () => {},
    operationSet: (operation) => { operations.push(operation); },
    askTimeoutMs: () => 60_000,
    displayNames: async (uids) => new Map(uids.map((uid) => [uid, uid])),
    stopTurnForQuestion: () => {},
    wakeTurn: () => {},
    memberAnswerAllowed: () => false,
  });
  return { desk, wakeTaskIds, publishedWakeTaskIds, texts, operations };
}

function persistQuestionAnswer(
  desk: QuestionDesk,
  message: InboundMessage,
  text: string,
  snapshot: Extract<TextQuestionRouteSnapshot, { kind: "questions" }>,
  invalidation?: "session-switched",
): TextQuestionConsumeOutcome {
  const prepared = desk.prepareTextSnapshot(message, text, snapshot, invalidation);
  if (!prepared.persist()) throw new Error("question answer persistence refused");
  return prepared.publishAfterCommit();
}

// Catches re-querying "the current question" after admission: the old text must stay bound to Q1.
test("a frozen text answer cannot answer Q2 after Q1 closes", async () => {
  await withStore("oco-question-freeze", async (store) => {
    const q1Task = seedTask(store, "task-q1");
    seedQuestion(store, "q1", q1Task, ["one", "two"]);
    const { desk } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    expect(snapshot).toEqual({ kind: "questions", questionIds: ["q1"] });

    expect(store.answerQuestion("q1", BOT_UID, TARGET, "2", OWNER_UID)).toBe(true);
    const q2Task = seedTask(store, "task-q2");
    seedQuestion(store, "q2", q2Task);

    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    expect(persistQuestionAnswer(
      desk,
      inbound({ ...TARGET, messageId: "old-text", senderId: OWNER_UID, text: "one" }),
      "one",
      snapshot,
    )).toBe("closed");
    expect(store.getQuestion("q1")?.answer).toBe("2");
    expect(store.getQuestion("q2")?.state).toBe("pending");
  });
});

// Catches deriving multi-question semantics from the subset still pending at consumption time.
test("text interpretation keeps the frozen multi-question shape after another question closes", async () => {
  await withStore("oco-question-frozen-set-shape", async (store) => {
    const task = seedTask(store, "task-frozen-set");
    seedQuestion(store, "frozen-a", task, ["one", "two"]);
    seedQuestion(store, "frozen-b", task);
    const { desk } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question set snapshot missing");
    expect(snapshot.questionIds).toEqual(["frozen-a", "frozen-b"]);
    expect(store.answerQuestion("frozen-b", BOT_UID, TARGET, "other answer", OWNER_UID)).toBe(true);

    const prepared = desk.prepareTextSnapshot(
      inbound({ ...TARGET, messageId: "frozen-set-digit", senderId: OWNER_UID, text: "9" }),
      "9",
      snapshot,
    );
    expect(prepared.persist()).toBe(true);
    expect(store.getQuestion("frozen-a")?.answer).toBe("9");
    expect(store.getQuestion("frozen-b")?.answer).toBe("other answer");
    expect(prepared.publishAfterCommit()).toBe("answered");
  });
});

// Catches treating a separately answered frozen qid as silent consumption.
test("a question answered elsewhere during preparation reports the frozen text as stale", async () => {
  await withStore("oco-question-answered-elsewhere", async (store) => {
    const task = seedTask(store, "task-answer-race");
    seedQuestion(store, "answer-race-q", task);
    const { desk, texts } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    expect(store.answerQuestion("answer-race-q", BOT_UID, TARGET, "card answer", OWNER_UID)).toBe(true);

    const prepared = desk.prepareTextSnapshot(
      inbound({ ...TARGET, messageId: "stale-text-answer", senderId: OWNER_UID, text: "text answer" }),
      "text answer",
      snapshot,
    );
    expect(prepared.persist()).toBe(true);
    expect(prepared.publishAfterCommit()).toBe("closed");
    expect(texts.at(-1)).toContain("失效");
    expect(store.getQuestion("answer-race-q")?.answer).toBe("card answer");
  });
});


// Catches reusing a prepared answer object after handoff rollback with a stale persisted cache.
test("the same prepared answer retries its CAS after the enclosing handoff rolls back", async () => {
  await withStore("oco-question-prepared-retry", async (store) => {
    const task = seedTask(store, "task-prepared-retry");
    seedQuestion(store, "prepared-retry-q", task);
    const { desk } = questionDesk(store);
    const message = inbound({
      ...TARGET,
      messageId: "prepared-retry-message",
      senderId: OWNER_UID,
      text: "retry answer",
    });
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    const accepted = store.acceptIngressJob({
      botUid: BOT_UID,
      sourceKind: "recv",
      sourceId: recvIngressSourceId(TARGET.channelType, TARGET.channelId, message.messageId),
      ...TARGET,
      messageId: message.messageId,
      payload: { kind: "recv", message },
      routeSnapshot: {
        purpose: snapshot,
        sessionGeneration: 1,
        binding: { sessionId: "session-1", cwd: "/tmp", sessionFile: null },
        barrierJobId: null,
      },
      lane: "ordered",
      state: "accepted",
      ackReady: true,
      operationCheckpoint: null,
    }).job;
    const prepared = desk.prepareTextSnapshot(message, message.text, snapshot);
    expect(store.handoffIngressJob(
      accepted.id,
      { kind: "ask", id: prepared.consumerId },
      () => prepared.persist() && false,
    )).toBe(false);
    expect(store.getQuestion("prepared-retry-q")?.state).toBe("pending");

    expect(store.handoffIngressJob(
      accepted.id,
      { kind: "ask", id: prepared.consumerId },
      () => prepared.persist(),
    )).toBe(true);
    expect(store.getQuestion("prepared-retry-q")?.answer).toBe("retry answer");
    expect(prepared.publishAfterCommit()).toBe("answered");
  });
});

// Catches accepting an ended old-session answer after a switch occurred during preparation.
test("a late frozen question is rejected after the persisted session switch", async () => {
  await withStore("oco-question-late-session-switch", async (store) => {
    const task = seedTask(store, "task-session-switch");
    seedQuestion(store, "old-session-q", task);
    const { desk, texts } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    expect(store.cancelQuestion("old-session-q", "ended")).toBe(true);
    const switchedAt = Date.now() + 1;
    store.ensureBinding({
      botUid: BOT_UID,
      ...TARGET,
      sessionId: "session-2",
      sessionFile: null,
      cwd: "/tmp",
      sessionSwitchedAt: switchedAt,
    });

    const prepared = desk.prepareTextSnapshot(
      inbound({ ...TARGET, messageId: "old-session-answer", senderId: OWNER_UID, text: "late answer" }),
      "late answer",
      snapshot,
    );
    expect(prepared.persist()).toBe(true);
    expect(store.getQuestion("old-session-q")?.state).toBe("cancelled");
    expect(prepared.publishAfterCommit()).toBe("closed");
    expect(texts.at(-1)).toContain("失效");
  });
});

// Catches partial set consumption and silent fallthrough: every qid frozen for the set is settled together.
test("a frozen question set answers only its exact qids", async () => {
  await withStore("oco-question-set", async (store) => {
    const task = seedTask(store, "task-set");
    seedQuestion(store, "set-a", task);
    seedQuestion(store, "set-b", task);
    const { desk } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question set snapshot missing");

    expect(persistQuestionAnswer(
      desk,
      inbound({ ...TARGET, messageId: "set-answer", senderId: OWNER_UID, text: "ship tonight" }),
      "ship tonight",
      snapshot,
    )).toBe("answered");
    expect(store.getQuestion("set-a")?.answer).toBe("ship tonight");
    expect(store.getQuestion("set-b")?.answer).toBe("ship tonight");
  });
});

// Catches publishing a waiter/card before the outer ingress handoff commits.
test("rolled-back ask handoff keeps the question pending and publishes nothing", async () => {
  await withStore("oco-question-handoff-rollback", async (store) => {
    const task = seedTask(store, "task-rollback");
    seedQuestion(store, "rollback-q", task);
    store.setQuestionCard("rollback-q", "rollback-card");
    const operations: Promise<void>[] = [];
    const transport = new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture" } });
    const { desk, texts } = questionDesk(store, transport, operations);
    const message = inbound({
      ...TARGET,
      messageId: "rollback-answer",
      senderId: OWNER_UID,
      text: "durable answer",
    });
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    const accepted = store.acceptIngressJob({
      botUid: BOT_UID,
      sourceKind: "recv",
      sourceId: recvIngressSourceId(TARGET.channelType, TARGET.channelId, message.messageId),
      ...TARGET,
      messageId: message.messageId,
      payload: { kind: "recv", message },
      routeSnapshot: {
        purpose: snapshot,
        sessionGeneration: 1,
        binding: { sessionId: "session-1", cwd: "/tmp", sessionFile: null },
        barrierJobId: null,
      },
      lane: "ordered",
      state: "accepted",
      ackReady: true,
      operationCheckpoint: null,
    }).job;
    const rolledBack = desk.prepareTextSnapshot(message, message.text, snapshot);
    expect(store.handoffIngressJob(
      accepted.id,
      { kind: "ask", id: rolledBack.consumerId },
      () => rolledBack.persist() && false,
    )).toBe(false);
    expect(store.getQuestion("rollback-q")?.state).toBe("pending");
    expect(operations).toHaveLength(0);
    expect(texts).toHaveLength(0);

    expect(store.handoffIngressJob(
      accepted.id,
      { kind: "ask", id: rolledBack.consumerId },
      () => rolledBack.persist(),
    )).toBe(true);
    expect(store.getQuestion("rollback-q")?.state).toBe("answered");
    expect(operations).toHaveLength(0);
    expect(rolledBack.publishAfterCommit()).toBe("answered");
    expect(operations).toHaveLength(1);
    await Promise.all(operations);
  });
});

// Catches treating explicit cancellation like lifecycle closure and accepting a forbidden late answer.
test("a cancelled frozen question consumes late text without changing its closed answer", async () => {
  await withStore("oco-question-cancelled", async (store) => {
    const task = seedTask(store, "task-cancelled");
    seedQuestion(store, "cancelled-q", task);
    const { desk, texts } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    expect(store.cancelQuestion("cancelled-q", "cancelled")).toBe(true);

    expect(persistQuestionAnswer(
      desk,
      inbound({ ...TARGET, messageId: "cancelled-answer", senderId: OWNER_UID, text: "too late" }),
      "too late",
      snapshot,
    )).toBe("closed");
    expect(store.getQuestion("cancelled-q")?.state).toBe("cancelled");
    expect(texts.at(-1)).toContain("失效");
  });
});

// Catches rejecting lifecycle-ended questions whose established contract accepts one late answer.
test("an ended frozen question accepts one late answer by exact qid CAS", async () => {
  await withStore("oco-question-ended", async (store) => {
    const task = seedTask(store, "task-ended");
    seedQuestion(store, "ended-q", task);
    const { desk } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");
    expect(store.cancelQuestion("ended-q", "ended")).toBe(true);

    expect(persistQuestionAnswer(
      desk,
      inbound({ ...TARGET, messageId: "ended-answer", senderId: OWNER_UID, text: "late value" }),
      "late value",
      snapshot,
    )).toBe("answered");
    expect(store.getQuestion("ended-q")).toEqual(expect.objectContaining({
      state: "answered",
      answer: "late value",
      closedReason: "ended",
    }));
  });
});

// Catches a deliberate session switch letting an admitted old-session answer mutate an open question.
test("session invalidation consumes a frozen answer without changing the question", async () => {
  await withStore("oco-question-switch", async (store) => {
    const task = seedTask(store, "task-switch");
    seedQuestion(store, "before-switch", task);
    const { desk } = questionDesk(store);
    const snapshot = desk.captureTextRoute(TARGET);
    if (snapshot.kind !== "questions") throw new Error("question snapshot missing");

    expect(persistQuestionAnswer(
      desk,
      inbound({ ...TARGET, messageId: "late-old-session", senderId: OWNER_UID, text: "late" }),
      "late",
      snapshot,
      "session-switched",
    )).toBe("session-switched");
    expect(store.getQuestion("before-switch")?.state).toBe("pending");
  });
});

class HeldEditTransport extends FakeTransport {
  readonly editStarted = Promise.withResolvers<void>();
  readonly releaseEdit = Promise.withResolvers<void>();

  override async editCard(
    target: ChatTarget,
    messageId: string,
    frame: CardFrame,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    this.editStarted.resolve();
    await this.releaseEdit.promise;
    await super.editCard(target, messageId, frame, cardSeq, signal);
  }
}

// Catches putting card HTTP back on event acceptance: a replay is ACKed while the first redraw is held.
test("an ask CAS and replay are accepted while the first card redraw remains held", async () => {
  await withStore("oco-question-held-render", async (store) => {
    const task = seedTask(store, "task-card");
    seedQuestion(store, "card-question", task, ["yes", "no"]);
    store.setQuestionCard("card-question", "ask-card-message");
    const action: BotCardAction = {
      ...TARGET,
      messageId: "ask-card-message",
      actionId: "ask_opt_1",
      operatorUid: OWNER_UID,
      inputs: {},
    };
    const transport = new HeldEditTransport({ bot: { uid: BOT_UID, name: "Fixture" } });
    transport.fetchBotEvents = async ({ afterEventId }) =>
      afterEventId >= 2 ? null : [1, 2]
        .filter((eventId) => eventId > afterEventId)
        .map((eventId) => ({ eventId, kind: "card_action" as const, action }));
    const operations: Promise<void>[] = [];
    const { desk } = questionDesk(store, transport, operations);
    const loop = createBotEventLoop({
      store,
      transport,
      botUid: () => BOT_UID,
      stopping: () => false,
      onCardAction: (cardAction) => ({ ackReady: desk.answerFromCard(cardAction) }),
    });
    try {
      loop.start();
      await transport.editStarted.promise;
      await flushMicrotasks();
      expect(store.getQuestion("card-question")?.answer).toBe("1");
      expect(transport.ackedEvents).toEqual([1, 2]);
      expect(store.getBotEventCursor(BOT_UID)).toBe(2);
    } finally {
      transport.releaseEdit.resolve();
      await loop.stop();
      await Promise.all(operations);
    }
  });
});

class FixedEventTransport extends FakeTransport {
  constructor(readonly events: readonly BotEvent[]) {
    super({ bot: { uid: BOT_UID, name: "Fixture" } });
  }

  override async fetchBotEvents(request: BotEventFetch): Promise<BotEvent[] | null> {
    this.eventFetches.push(request.afterEventId);
    const remaining = this.events.filter(({ eventId }) => eventId > request.afterEventId);
    return remaining.length === 0 ? null : [...remaining];
  }
}


class AckObservedEventTransport extends FixedEventTransport {
  readonly acknowledged = Promise.withResolvers<number>();

  override async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    await super.ackBotEvent(eventId, signal);
    this.acknowledged.resolve(eventId);
  }
}

type CardWakeRestartScenario = "single" | "set" | "closed";

async function verifyCardWakeRestartScenario(
  scenario: CardWakeRestartScenario,
  eventId: number,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), `oco-card-wake-${scenario}-`));
  let store = await openChannelStore(root);
  let loop: BotEventLoop | null = null;
  try {
    store.setBotUid(BOT_UID);
    store.setOwnerId(BOT_UID, OWNER_UID);
    store.ensureBinding({
      botUid: BOT_UID,
      ...TARGET,
      sessionId: "session-card-wake",
      sessionFile: null,
      cwd: root,
    });
    const task = seedTask(store, `task-card-wake-${scenario}`);
    const questionIds = scenario === "set" ? ["wake-set-1", "wake-set-2"] : [`wake-${scenario}`];
    for (const [index, id] of questionIds.entries()) {
      store.createQuestion({
        id,
        taskId: task.id,
        botUid: BOT_UID,
        ...TARGET,
        payload: JSON.stringify({
          sourceId: id,
          question: `question-${id}`,
          options: scenario === "set" ? [] : [{ label: "yes" }, { label: "no" }],
          multi: false,
          ...(scenario === "set" ? { cardIndex: index + 1, cardSize: questionIds.length } : {}),
        }),
      });
    }
    store.markQuestionsSettled(questionIds);
    const cardMessageId = `card-wake-${scenario}`;
    if (questionIds.length === 1) store.setQuestionCard(questionIds[0]!, cardMessageId);
    else store.setQuestionsCard(questionIds, cardMessageId);
    if (scenario === "closed") expect(store.cancelQuestion(questionIds[0]!, "ended")).toBe(true);

    const action: BotCardAction = {
      ...TARGET,
      messageId: cardMessageId,
      actionId: scenario === "set" ? "ask_submit#1" : "ask_opt_2",
      operatorUid: OWNER_UID,
      inputs: scenario === "set"
        ? { ask_text_1: "target-one", ask_text_2: "target-two" }
        : {},
    };
    const event: BotEvent = { eventId, kind: "card_action", action };
    const trigger = new Database(store.path);
    trigger.run(
      "CREATE TRIGGER fail_question_wake BEFORE INSERT ON tasks "
      + "WHEN NEW.message_id LIKE 'answer:%' "
      + "BEGIN SELECT RAISE(ABORT, 'injected ask wake insert failure'); END;",
    );
    trigger.close();

    const firstTransport = new FixedEventTransport([event]);
    const first = durableQuestionDesk(store, firstTransport);
    const firstOutcome = Promise.withResolvers<BotEventOutcome>();
    loop = createBotEventLoop({
      store,
      transport: firstTransport,
      botUid: () => BOT_UID,
      stopping: () => false,
      retryDelayMs: 60_000,
      onCardAction: (cardAction) => {
        const ackReady = first.desk.answerFromCard(cardAction);
        void ackReady.then(firstOutcome.resolve, firstOutcome.reject);
        return { ackReady };
      },
    });
    loop.start();
    expect(await firstOutcome.promise).toBe("retry");
    await loop.stop();
    loop = null;
    await Promise.all(first.operations);

    const initialState = scenario === "closed" ? "cancelled" : "pending";
    expect(questionIds.map((id) => store.getQuestion(id)?.state)).toEqual(
      questionIds.map(() => initialState),
    );
    expect(firstTransport.ackedEvents).toEqual([]);
    expect(store.getBotEventCursor(BOT_UID)).toBe(0);
    if (scenario === "closed") {
      expect(store.getQuestion(questionIds[0]!)?.closedReason).toBe("ended");
      expect(store.getQuestion(questionIds[0]!)?.answer).toBeNull();
    }
    expect(first.wakeTaskIds).toEqual([]);
    expect(first.publishedWakeTaskIds).toEqual([]);
    expect(first.operations).toEqual([]);
    expect(store.getIngressJobBySource({
      botUid: BOT_UID,
      sourceKind: "bot-event",
      sourceId: String(eventId),
    })).toBeNull();

    store.close();
    store = await openChannelStore(root);
    const removeFailure = new Database(store.path);
    removeFailure.run("DROP TRIGGER fail_question_wake;");
    removeFailure.close();

    const retryTransport = new AckObservedEventTransport([event]);
    const retry = durableQuestionDesk(store, retryTransport);
    loop = createBotEventLoop({
      store,
      transport: retryTransport,
      botUid: () => BOT_UID,
      stopping: () => false,
      onCardAction: (cardAction) => ({ ackReady: retry.desk.answerFromCard(cardAction) }),
    });
    loop.start();
    expect(await retryTransport.acknowledged.promise).toBe(eventId);
    await flushMicrotasks();
    await loop.stop();
    loop = null;
    await Promise.all(retry.operations);

    const expectedAnswers = scenario === "set" ? ["target-one", "target-two"] : ["2"];
    expect(questionIds.map((id) => store.getQuestion(id)?.state)).toEqual(
      questionIds.map(() => "answered"),
    );
    expect(questionIds.map((id) => store.getQuestion(id)?.answer)).toEqual(expectedAnswers);
    expect(retry.wakeTaskIds).toHaveLength(1);
    expect(retry.publishedWakeTaskIds).toEqual(retry.wakeTaskIds);
    expect(store.getTask(retry.wakeTaskIds[0]!)?.state).toBe("queued");
    expect(store.getIngressJobBySource({
      botUid: BOT_UID,
      sourceKind: "bot-event",
      sourceId: String(eventId),
    })).toEqual(expect.objectContaining({ state: "handed-off", consumerKind: "ask" }));
    expect(store.getBotEventCursor(BOT_UID)).toBe(eventId);

    const operationsBeforeReplay = retry.operations.length;
    const replayAction = { ...action, eventId };
    expect(await retry.desk.answerFromCard(replayAction)).toBe("handled");
    await Promise.all(retry.operations.slice(operationsBeforeReplay));
    expect(retry.wakeTaskIds).toHaveLength(1);
    expect(retry.publishedWakeTaskIds).toEqual([
      retry.wakeTaskIds[0]!,
      retry.wakeTaskIds[0]!,
    ]);
  } finally {
    await loop?.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

test("ask card CAS and wake task survive an atomic bot-event restart", async () => {
  await verifyCardWakeRestartScenario("single", 801);
  await verifyCardWakeRestartScenario("set", 802);
  await verifyCardWakeRestartScenario("closed", 803);
});
// Catches messageId-based event dedup and waiting for post-handoff model/card work before cursor ACK.
test("same-card event ids are accepted independently while handed-off work is held", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-card-event-source-"));
  const store = await openChannelStore(root);
  const action = (eventId: number): BotEvent => ({
    eventId,
    kind: "card_action",
    action: {
      ...TARGET,
      messageId: "same-card-message",
      actionId: "cfg:switch:00000000-0000-4000-8000-000000000001",
      operatorUid: OWNER_UID,
      inputs: { [COMMAND_INPUT_ID]: eventId === 41 ? "fixture/fast" : "default" },
    },
  });
  const transport = new FixedEventTransport([action(41), action(42)]);
  const businessOperations: Promise<void>[] = [];
  const heldBusiness = Promise.withResolvers<void>();
  const accepted: number[] = [];
  const loop = createBotEventLoop({
    store,
    transport,
    botUid: () => BOT_UID,
    stopping: () => false,
    onCardAction: (_cardAction: BotCardAction, eventId: number) => {
      accepted.push(eventId);
      if (eventId === 41) businessOperations.push(heldBusiness.promise);
      return { ackReady: Promise.resolve<BotEventOutcome>("handled") };
    },
  });
  try {
    loop.start();
    await flushMicrotasks();
    expect(accepted).toEqual([41, 42]);
    expect(transport.ackedEvents).toEqual([41, 42]);
    expect(store.getBotEventCursor(BOT_UID)).toBe(42);
  } finally {
    heldBusiness.resolve();
    await Promise.all(businessOperations);
    await loop.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

function commandCardView(): CommandCardView {
  return {
    kind: "switch",
    current: "default",
    plain: "plain fallback",
    choices: [{ title: "fast", value: "fixture/fast" }],
  };
}

function actionId(frame: CardFrame): string {
  const stack: unknown[] = [frame.card];
  while (stack.length > 0) {
    const value = stack.pop();
    if (Array.isArray(value)) {
      stack.push(...value);
      continue;
    }
    if (value === null || typeof value !== "object") continue;
    const node = value as Record<string, unknown>;
    if (node.type === "Action.Submit" && typeof node.id === "string") return node.id;
    stack.push(...Object.values(node));
  }
  throw new Error("command card action missing");
}

// Catches acknowledging only after model switching/probing or terminal card HTTP finishes.
test("a command-card event returns after durable handoff while model work stays held", async () => {
  await withStore("oco-command-card-handoff", async (store) => {
    const transport = new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture" } });
    transport.cardSupported = true;
    const releaseApply = Promise.withResolvers<void>();
    const applyStarted = Promise.withResolvers<void>();
    const accepted: Array<{ snapshot: ConfigCardActionSnapshot; reservation: ConfigApplyReservation }> = [];
    const operations: Promise<void>[] = [];
    const deps: ConfigMenuDeps = {
      store,
      transport,
      botUid: () => BOT_UID,
      stopping: () => false,
      getAcceptedEvent: () => false,
      chatAllowed: () => true,
      applyChoice: async () => {
        applyStarted.resolve();
        await releaseApply.promise;
        return "applied";
      },
      enqueueText: () => {},
      acceptAction: (snapshot, _action, reservation) => {
        accepted.push({ snapshot, reservation });
        return "accepted";
      },
    };
    const desk = createConfigMenuDesk(deps);
    const ticket = desk.begin(TARGET, "switch");
    await desk.open(TARGET, commandCardView(), ticket);
    ticket.release();
    const card = transport.sentCards[0];
    if (card === undefined) throw new Error("command card missing");

    const resultPromise = desk.handleAction({
      ...TARGET,
      messageId: card.messageId,
      actionId: actionId(card.frame),
      operatorUid: OWNER_UID,
      inputs: { [COMMAND_INPUT_ID]: "fixture/fast" },
    }, 73);
    try {
      const result = await Promise.race([resultPromise, Promise.resolve("held" as const)]);
      expect(result).toBe("handled");
      expect(accepted).toEqual([expect.objectContaining({
        snapshot: expect.objectContaining({
          eventId: 73,
          messageId: card.messageId,
          value: "fixture/fast",
        }),
        reservation: expect.objectContaining({
          run: expect.any(Function),
          release: expect.any(Function),
        }),
      })]);
      const acceptedAction = accepted[0];
      if (acceptedAction === undefined) throw new Error("accepted config snapshot missing");
      const checkpoint = { dispatchIntent: false, appliedResult: null as string | null };
      operations.push(desk.resumeAcceptedAction(acceptedAction.snapshot, acceptedAction.reservation, {
        get dispatchIntent() { return checkpoint.dispatchIntent; },
        get appliedResult() { return checkpoint.appliedResult; },
        markDispatchIntent: () => { checkpoint.dispatchIntent = true; return true; },
        markAppliedResult: (result) => { checkpoint.appliedResult = result; return true; },
        markUnknown: () => {},
      }));
      await applyStarted.promise;
      expect(operations).toHaveLength(1);
    } finally {
      releaseApply.resolve();
      await resultPromise;
      await Promise.all(operations);
    }
  });
});
