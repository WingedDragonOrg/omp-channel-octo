import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import {
  recvIngressSourceId,
  type IngressJobInput,
  type IngressOperationCheckpoint,
  type IngressPayload,
  type IngressRoutePurpose,
  type IngressRouteSnapshot,
  type IngressSourceKey,
  type IngressValidatedConfigAction,
} from "../src/inbound/ingress-types.ts";


function routeSnapshot(
  purpose: IngressRoutePurpose = { kind: "ordinary" },
  barrierJobId: number | null = null,
): IngressRouteSnapshot {
  return {
    purpose,
    sessionGeneration: 4,
    binding: { sessionId: "session-4", cwd: "/work/chat", sessionFile: "/state/session.json" },
    barrierJobId,
  };
}

function cardActionPayload(
  eventId: number,
  actionId: string,
  acceptedAction: IngressValidatedConfigAction | null = null,
): IngressPayload {
  return {
    kind: "bot-event",
    eventId,
    action: {
      messageId: "shared-card-message",
      channelType: 2,
      channelId: "group-1",
      actionId,
      operatorUid: "owner-1",
      inputs: {},
    },
    acceptedAction,
  };
}

function receivePayload(
  messageId: string,
  text = "review this",
  channelType: 1 | 2 | 5 = 2,
  channelId = "group-1",
): IngressPayload {
  return {
    kind: "recv",
    message: {
      channelType,
      channelId,
      messageId,
      senderId: "owner-1",
      text,
      mentionedUserIds: ["bot-1"],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: 12,
      messageTimestamp: 1_800_000_000_000,
    },
  };
}

function input(overrides: Partial<IngressJobInput> = {}): IngressJobInput {
  const value: IngressJobInput = {
    botUid: "bot-1",
    sourceKind: "recv",
    sourceId: "recv-1",
    channelType: 2,
    channelId: "group-1",
    messageId: "recv-1",
    payload: receivePayload("recv-1"),
    routeSnapshot: routeSnapshot(),
    lane: "ordered",
    state: "accepted",
    ackReady: true,
    operationCheckpoint: null,
    ...overrides,
  };
  if (value.sourceKind === "recv" && value.messageId !== null) {
    value.sourceId = recvIngressSourceId(value.channelType, value.channelId, value.messageId);
  }
  return value;
}


test("source identity deduplicates within its trusted namespace, not by card message id", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-source-"));
  try {
    const store = await openChannelStore(root);
    const jobs = store;
    try {
      const received = jobs.acceptIngressJob(input({
        messageId: "shared-card-message",
        payload: receivePayload("shared-card-message"),
      }));
      const duplicateReceive = jobs.acceptIngressJob(input({
        messageId: "shared-card-message",
        payload: receivePayload("shared-card-message", "redelivery changed body"),
      }));
      const otherChat = jobs.acceptIngressJob(input({
        channelId: "group-2",
        messageId: "shared-card-message",
        payload: receivePayload("shared-card-message", "same id in another chat", 2, "group-2"),
      }));
      const firstClick = jobs.acceptIngressJob(input({
        sourceKind: "bot-event",
        sourceId: "71",
        messageId: "shared-card-message",
        payload: cardActionPayload(71, "approve-first"),
        lane: "control",
      }));
      const secondClick = jobs.acceptIngressJob(input({
        sourceKind: "bot-event",
        sourceId: "72",
        messageId: "shared-card-message",
        payload: cardActionPayload(72, "approve-second"),
        lane: "control",
      }));
      const otherBot = jobs.acceptIngressJob(input({
        botUid: "bot-2",
        messageId: "shared-card-message",
        payload: receivePayload("shared-card-message"),
      }));

      expect(received.created).toBe(true);
      expect(duplicateReceive.created).toBe(false);
      expect(duplicateReceive.job.id).toBe(received.job.id);
      expect(duplicateReceive.job.payload).toEqual(received.job.payload);
      expect(firstClick.created).toBe(true);
      expect(secondClick.created).toBe(true);
      expect(otherChat.created).toBe(true);
      expect(otherBot.created).toBe(true);
      expect(new Set([received.job.id, otherChat.job.id, firstClick.job.id, secondClick.job.id, otherBot.job.id]).size).toBe(5);
      expect(jobs.getIngressJobBySource({ botUid: "bot-1", sourceKind: "bot-event", sourceId: "71" })?.payload)
        .toEqual(cardActionPayload(71, "approve-first"));
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a pre-authorization cancellation persists a payload-free tombstone that wins over redelivery", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-cancel-tombstone-"));
  try {
    const store = await openChannelStore(root);
    const jobs = store;
    const key: IngressSourceKey = {
      botUid: "bot-1",
      sourceKind: "recv",
      sourceId: recvIngressSourceId(2, "group-1", "cancelled-message"),
    };
    try {
      jobs.cancelIngressJobBySource(key, { channelType: 2, channelId: "group-1", messageId: "cancelled-message" });
      const tombstone = jobs.getIngressJobBySource(key);
      expect(tombstone).toMatchObject({
        ...key,
        channelType: 2,
        channelId: "group-1",
        messageId: "cancelled-message",
        payload: null,
        routeSnapshot: null,
        preparedText: null,
        spoolPaths: [],
        ackReady: true,
        state: "cancelled",
      });

      const redelivery = jobs.acceptIngressJob(input({
        messageId: "cancelled-message",
        payload: receivePayload("cancelled-message"),
      }));
      expect(redelivery.created).toBe(false);
      expect(tombstone).not.toBeNull();
      if (!tombstone) throw new Error("cancelled source identity was not retained");
      expect(redelivery.job.id).toBe(tombstone.id);
      expect(redelivery.job.state).toBe("cancelled");
      expect(redelivery.job.payload).toBeNull();
      expect(jobs.listRecoverableIngressJobs("bot-1")).toEqual([]);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("media is not ACK-ready until prepared text and local spool paths survive reopen", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-media-ready-"));
  try {
    let store = await openChannelStore(root);
    let jobs = store;
    const mediaPayload = receivePayload("media-1");
    if (mediaPayload.kind !== "recv") throw new Error("expected receive payload");
    mediaPayload.message.attachments.push({
      url: "https://media.example/image.png",
      kind: "image",
      name: "image.png",
      size: 128,
      durationSeconds: 0,
    });
    try {
      const initial = jobs.acceptIngressJob(input({
        messageId: "media-1",
        payload: mediaPayload,
        state: "preparing",
        ackReady: false,
      }));
      const reopenedBeforePreparation = await openChannelStore(root);
      store.close();
      store = reopenedBeforePreparation;
      jobs = store;
      expect(jobs.getIngressJob(initial.job.id)).toMatchObject({
        state: "preparing",
        ackReady: false,
        payload: mediaPayload,
      });
      const duplicateWhilePreparing = jobs.acceptIngressJob(input({
        messageId: "media-1",
        payload: mediaPayload,
        state: "accepted",
        ackReady: true,
      }));
      expect(initial.job.ackReady).toBe(false);
      expect(duplicateWhilePreparing.job.ackReady).toBe(false);
      expect(duplicateWhilePreparing.created).toBe(false);
      expect(jobs.listRecoverableIngressJobs("bot-1").map((job) => job.id)).toContain(initial.job.id);

      expect(jobs.markIngressPrepared(initial.job.id, "image: /tmp/oco-ingress-spool/image.png", ["/tmp/oco-ingress-spool/image.png"])).toBe(true);
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    jobs = store;
    try {
      const recovered = jobs.getIngressJobBySource({
        botUid: "bot-1",
        sourceKind: "recv",
        sourceId: recvIngressSourceId(2, "group-1", "media-1"),
      });
      expect(recovered).toMatchObject({
        ackReady: true,
        preparedText: "image: /tmp/oco-ingress-spool/image.png",
        spoolPaths: ["/tmp/oco-ingress-spool/image.png"],
      });
      expect(recovered).not.toBeNull();
      if (!recovered) throw new Error("prepared media ingress job was lost on reopen");
      expect(jobs.listRecoverableIngressJobs("bot-1").map((job) => job.id)).toContain(recovered.id);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("waiting command checkpoints and exact question-or-none route snapshots survive recovery", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-checkpoint-"));
  try {
    let store = await openChannelStore(root);
    let jobs = store;
    const waitingCheckpoint: IngressOperationCheckpoint = {
      kind: "command",
      action: "thread-merge",
      argument: "",
      phase: "waiting-resource",
      expectedBinding: { sessionId: "session-4", cwd: "/work/chat", sessionFile: "/state/session.json" },
    };
    const acceptedConfigAction: IngressValidatedConfigAction = {
      kind: "mode",
      actionId: "mode-queue",
      messageId: "shared-card-message",
      target: { channelType: 2, channelId: "group-1" },
      ownerUid: "owner-1",
      value: "queue",
      cardSeq: 4,
    };
    try {
      const waiting = jobs.acceptIngressJob(input({
        messageId: "waiting-merge",
        payload: receivePayload("waiting-merge", "/thread merge"),
        routeSnapshot: routeSnapshot({ kind: "command", action: "thread-merge" }, 77),
      }));
      expect(jobs.saveIngressCheckpoint(waiting.job.id, waitingCheckpoint)).toBe(true);

      const frozenQuestions = jobs.acceptIngressJob(input({
        messageId: "answer-q1",
        payload: receivePayload("answer-q1", "answer from chat"),
        routeSnapshot: routeSnapshot({ kind: "questions", questionIds: ["question-q1", "question-q3"] }),
      }));
      jobs.acceptIngressJob(input({
        messageId: "ordinary-before-q2",
        payload: receivePayload("ordinary-before-q2", "ordinary text"),
        routeSnapshot: routeSnapshot({ kind: "ordinary" }),
      }));
      jobs.acceptIngressJob(input({
        sourceKind: "bot-event",
        sourceId: "91",
        messageId: "shared-card-message",
        payload: cardActionPayload(91, "mode-queue", acceptedConfigAction),
        routeSnapshot: routeSnapshot({ kind: "none" }),
        lane: "control",
      }));
      store.createQuestion({
        id: "question-q2",
        taskId: "task-later",
        botUid: "bot-1",
        channelType: 2,
        channelId: "group-1",
        payload: "a later question must not change the frozen route",
      });
      expect(jobs.markIngressDispatchIntent(frozenQuestions.job.id)).toBe(true);
      expect(jobs.markIngressUnknown(frozenQuestions.job.id)).toBe(true);
      expect(jobs.countPendingIngressJobs("bot-1")).toBeGreaterThan(0);
    } finally {
      store.close();
    }

    store = await openChannelStore(root);
    jobs = store;
    try {
      const recovered = jobs.listRecoverableIngressJobs("bot-1");
      expect(recovered.map((job) => job.messageId)).toEqual(["waiting-merge", "ordinary-before-q2", "shared-card-message"]);
      expect(recovered[0]).toMatchObject({
        state: "accepted",
        operationCheckpoint: waitingCheckpoint,
        routeSnapshot: routeSnapshot({ kind: "command", action: "thread-merge" }, 77),
      });
      expect(recovered[1]?.routeSnapshot).toEqual(routeSnapshot({ kind: "ordinary" }));
      expect(recovered[2]?.routeSnapshot).toEqual(routeSnapshot({ kind: "none" }));
      expect(recovered[2]?.payload).toMatchObject({
        kind: "bot-event",
        eventId: 91,
        acceptedAction: acceptedConfigAction,
      });
      expect(jobs.getIngressJobBySource({
        botUid: "bot-1",
        sourceKind: "recv",
        sourceId: recvIngressSourceId(2, "group-1", "answer-q1"),
      })).toMatchObject({
        state: "unknown",
        routeSnapshot: routeSnapshot({ kind: "questions", questionIds: ["question-q1", "question-q3"] }),
      });
      expect(recovered.map((job) => job.messageId)).not.toContain("answer-q1");
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("config-action dispatch and applied-result checkpoints use compare-and-set recovery boundaries", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-config-action-checkpoint-"));
  try {
    let store = await openChannelStore(root);
    const acceptedAction: IngressValidatedConfigAction = {
      kind: "mode",
      actionId: "mode-queue",
      messageId: "shared-card-message",
      target: { channelType: 2, channelId: "group-1" },
      ownerUid: "owner-1",
      value: "queue",
      cardSeq: 4,
    };
    try {
      const accepted = store.acceptIngressJob(input({
        sourceKind: "bot-event",
        sourceId: "201",
        messageId: "shared-card-message",
        payload: cardActionPayload(201, acceptedAction.actionId, acceptedAction),
        routeSnapshot: routeSnapshot({ kind: "command", action: "config:mode" }),
        lane: "control",
        operationCheckpoint: { kind: "config-action", phase: "accepted" },
      }));

      expect(store.markIngressConfigActionDispatchIntent(accepted.job.id)).toBe(true);
      expect(store.markIngressConfigActionDispatchIntent(accepted.job.id)).toBe(false);
      expect(store.getIngressJob(accepted.job.id)).toMatchObject({
        state: "dispatch-intent",
        operationCheckpoint: { kind: "config-action", phase: "dispatch-intent" },
      });
      expect(store.markIngressConfigActionApplied(accepted.job.id, "Mode set to queue")).toBe(true);
      expect(store.markIngressConfigActionApplied(accepted.job.id, "Mode set to queue")).toBe(false);
      expect(store.getIngressJob(accepted.job.id)).toMatchObject({
        state: "accepted",
        operationCheckpoint: {
          kind: "config-action",
          phase: "applied",
          appliedResult: "Mode set to queue",
        },
      });

      const reopened = await openChannelStore(root);
      store.close();
      store = reopened;
      expect(store.getIngressJob(accepted.job.id)).toMatchObject({
        state: "accepted",
        operationCheckpoint: {
          kind: "config-action",
          phase: "applied",
          appliedResult: "Mode set to queue",
        },
      });

      const unknown = store.acceptIngressJob(input({
        sourceKind: "bot-event",
        sourceId: "202",
        messageId: "shared-card-message",
        payload: cardActionPayload(202, acceptedAction.actionId, acceptedAction),
        routeSnapshot: routeSnapshot({ kind: "command", action: "config:mode" }),
        lane: "control",
        operationCheckpoint: { kind: "config-action", phase: "accepted" },
      }));
      expect(store.markIngressConfigActionDispatchIntent(unknown.job.id)).toBe(true);
      expect(store.markIngressUnknown(unknown.job.id)).toBe(true);
      expect(store.markIngressConfigActionApplied(unknown.job.id, "not safely known")).toBe(false);
      expect(store.listRecoverableIngressJobs("bot-1").map((job) => job.id)).not.toContain(unknown.job.id);

      const unknownReopened = await openChannelStore(root);
      store.close();
      store = unknownReopened;
      expect(store.getIngressJob(unknown.job.id)).toMatchObject({
        state: "unknown",
        operationCheckpoint: { kind: "config-action", phase: "dispatch-intent" },
      });
      expect(store.listRecoverableIngressJobs("bot-1").map((job) => job.id)).not.toContain(unknown.job.id);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("ask answer CAS, recovery claim, late wake task, and ingress handoff share one transaction", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-ask-effects-"));
  try {
    let store = await openChannelStore(root);
    const target = { channelType: 2 as const, channelId: "group-1" };
    try {
      const liveQuestionId = "question-live-ask";
      store.createQuestion({
        id: liveQuestionId,
        taskId: "task-live-ask",
        botUid: "bot-1",
        ...target,
        payload: JSON.stringify({
          toolCallId: "tool-call-live",
          askGroup: "ask-group-live",
          sourceId: "source-live",
          question: "Should this run?",
          options: [],
        }),
      });
      const liveAnswer = store.acceptIngressJob(input({
        messageId: "answer-live-ask",
        payload: receivePayload("answer-live-ask", "yes"),
        routeSnapshot: routeSnapshot({ kind: "questions", questionIds: [liveQuestionId] }),
      }));
      const liveConsumerId = `questions:${JSON.stringify([liveQuestionId])}`;
      expect(store.handoffIngressJob(liveAnswer.job.id, { kind: "ask", id: liveConsumerId }, () =>
        store.answerQuestions("bot-1", target, [{ id: liveQuestionId, answer: "yes", answeredBy: "owner-1" }]).length === 1,
      )).toBe(true);
      expect(store.getQuestion(liveQuestionId)).toMatchObject({
        state: "answered",
        answer: "yes",
        answeredBy: "owner-1",
        settledAt: null,
      });
      expect(store.getTaskByMessageId("bot-1", target, "answer-live-ask")).toBeNull();
      expect(store.getIngressJob(liveAnswer.job.id)).toMatchObject({
        state: "handed-off",
        messageId: "answer-live-ask",
        consumerKind: "ask",
        consumerId: liveConsumerId,
      });

      const reopened = await openChannelStore(root);
      store.close();
      store = reopened;
      const recoveredLiveQuestion = store.getQuestion(liveQuestionId);
      expect(recoveredLiveQuestion).not.toBeNull();
      if (!recoveredLiveQuestion) throw new Error("answered live question was not durable");
      expect(recoveredLiveQuestion).toMatchObject({
        state: "answered",
        answer: "yes",
        answeredBy: "owner-1",
        settledAt: null,
      });
      expect(JSON.parse(recoveredLiveQuestion.payload)).toMatchObject({ toolCallId: "tool-call-live" });
      expect(store.getTaskByMessageId("bot-1", target, "answer-live-ask")).toBeNull();
      expect(store.getIngressJob(liveAnswer.job.id)).toMatchObject({
        state: "handed-off",
        messageId: "answer-live-ask",
        consumerKind: "ask",
        consumerId: liveConsumerId,
      });

      const lateQuestionId = "question-late-ask";
      store.createQuestion({
        id: lateQuestionId,
        taskId: "task-late-ask",
        botUid: "bot-1",
        ...target,
        payload: JSON.stringify({
          toolCallId: "tool-call-late",
          askGroup: "ask-group-late",
          sourceId: "source-late",
          question: "Which option?",
          options: [],
        }),
      });
      store.markQuestionsSettled([lateQuestionId]);
      const lateAnswer = store.acceptIngressJob(input({
        messageId: "answer-late-ask",
        payload: receivePayload("answer-late-ask", "option B"),
        routeSnapshot: routeSnapshot({ kind: "questions", questionIds: [lateQuestionId] }),
      }));
      const wakeText = `Question ${lateQuestionId}: Which option?\nAnswer (owner-1): option B`;
      const claim = {
        botUid: "bot-1",
        target,
        messageId: "answer-late-ask",
        senderId: "owner-1",
        text: wakeText,
        supported: true,
        kind: "task" as const,
      };
      const wakeTask = {
        id: "ask-wake:answer-late-ask:question-late-ask",
        botUid: "bot-1",
        ...target,
        messageId: "answer-late-ask",
        senderId: "owner-1",
        text: wakeText,
        sessionId: "session-4",
        cwd: "/work/chat",
        state: "queued" as const,
        resultText: null,
        errorText: null,
        messageSeq: 13,
        historyEligible: true,
        relaySuppressed: false,
      };
      const persistLateAnswerAndWake = (): boolean => {
        const answered = store.answerQuestions("bot-1", target, [{
          id: lateQuestionId,
          answer: "option B",
          answeredBy: "owner-1",
        }]);
        return answered.length === 1 && store.acceptTask(claim, wakeTask);
      };

      expect(() => store.handoffIngressJob(
        lateAnswer.job.id,
        { kind: "task", id: wakeTask.id },
        () => {
          expect(store.answerQuestions("bot-1", target, [{
            id: lateQuestionId,
            answer: "option B",
            answeredBy: "owner-1",
          }])).toEqual([lateQuestionId]);
          expect(store.acceptTask(claim, wakeTask)).toBe(true);
          throw new Error("forced failure after ask wake persistence");
        },
      )).toThrow("forced failure after ask wake persistence");
      expect(store.getQuestion(lateQuestionId)).toMatchObject({ state: "pending" });
      expect(store.getTaskByMessageId("bot-1", target, "answer-late-ask")).toBeNull();
      expect(store.getIngressJob(lateAnswer.job.id)).toMatchObject({ state: "accepted", messageId: "answer-late-ask" });

      expect(store.handoffIngressJob(lateAnswer.job.id, { kind: "task", id: wakeTask.id }, persistLateAnswerAndWake)).toBe(true);
      expect(store.getQuestion(lateQuestionId)).toMatchObject({
        state: "answered",
        answer: "option B",
        answeredBy: "owner-1",
      });
      expect(store.getTaskByMessageId("bot-1", target, "answer-late-ask")).toMatchObject({
        id: wakeTask.id,
        text: wakeText,
      });
      expect(store.getIngressJob(lateAnswer.job.id)).toMatchObject({
        state: "handed-off",
        messageId: "answer-late-ask",
        consumerKind: "task",
        consumerId: wakeTask.id,
      });

      const invalidQuestionId = "question-invalid-ask";
      store.createQuestion({
        id: invalidQuestionId,
        taskId: "task-invalid-ask",
        botUid: "bot-1",
        ...target,
        payload: JSON.stringify({
          toolCallId: "tool-call-invalid",
          askGroup: "ask-group-invalid",
          sourceId: "source-invalid",
          question: "Choose a valid option",
          options: [{ label: "A" }, { label: "B" }],
        }),
      });
      const invalidAnswer = store.acceptIngressJob(input({
        messageId: "answer-invalid-ask",
        payload: receivePayload("answer-invalid-ask", "99"),
        routeSnapshot: routeSnapshot({ kind: "questions", questionIds: [invalidQuestionId] }),
      }));
      const rejectionOutbox = {
        id: "ask-rejection:answer-invalid-ask",
        botUid: "bot-1",
        ...target,
        taskId: null,
        kind: "text" as const,
        text: "答案无效：请回复选项编号或直接输入答案。",
        path: null,
        fileKind: null,
        clientMsgNo: "ask-rejection:answer-invalid-ask",
      };
      expect(store.handoffIngressJob(
        invalidAnswer.job.id,
        { kind: "outbox", id: rejectionOutbox.id },
        () => {
          store.enqueueOutbox(rejectionOutbox);
          return true;
        },
      )).toBe(true);
      expect(store.getQuestion(invalidQuestionId)).toMatchObject({ state: "pending" });
      expect(store.getOutbox(rejectionOutbox.id)).toMatchObject({
        state: "pending",
        text: rejectionOutbox.text,
        clientMsgNo: rejectionOutbox.clientMsgNo,
      });
      expect(store.getIngressJob(invalidAnswer.job.id)).toMatchObject({
        state: "handed-off",
        messageId: "answer-invalid-ask",
        consumerKind: "outbox",
        consumerId: rejectionOutbox.id,
      });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("ask-card acceptance, answer CAS, and consumer handoff commit or roll back as one event", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-atomic-ask-card-"));
  try {
    let store = await openChannelStore(root);
    const target = { channelType: 2 as const, channelId: "group-1" };
    const questionId = "question-card-event";
    const eventInput: IngressJobInput = {
      botUid: "bot-1",
      sourceKind: "bot-event",
      sourceId: "901",
      ...target,
      messageId: "shared-card-message",
      payload: cardActionPayload(901, "ask-answer"),
      routeSnapshot: routeSnapshot({ kind: "none" }),
      lane: "control",
      state: "accepted",
      ackReady: true,
      operationCheckpoint: null,
    };
    const key: IngressSourceKey = {
      botUid: eventInput.botUid,
      sourceKind: eventInput.sourceKind,
      sourceId: eventInput.sourceId,
    };
    const consumer = { kind: "ask" as const, id: `questions:${JSON.stringify([questionId])}` };
    try {
      store.createQuestion({
        id: questionId,
        taskId: "task-card-event",
        botUid: "bot-1",
        ...target,
        payload: JSON.stringify({
          toolCallId: "tool-call-card-event",
          askGroup: "ask-group-card-event",
          sourceId: "source-card-event",
          question: "Should this proceed?",
          options: [],
        }),
      });

      expect(store.acceptAndHandoffIngressJob(eventInput, consumer, () => false)).toBe(false);
      expect(store.getIngressJobBySource(key)).toBeNull();
      expect(store.getQuestion(questionId)).toMatchObject({ state: "pending" });

      expect(() => store.acceptAndHandoffIngressJob(eventInput, consumer, () => {
        expect(store.answerQuestion(questionId, "bot-1", target, "yes", "owner-1")).toBe(true);
        throw new Error("forced failure after card answer CAS");
      })).toThrow("forced failure after card answer CAS");
      expect(store.getIngressJobBySource(key)).toBeNull();
      expect(store.getQuestion(questionId)).toMatchObject({ state: "pending" });

      let publishCount = 0;
      expect(store.acceptAndHandoffIngressJob(eventInput, consumer, () => {
        publishCount += 1;
        return store.answerQuestion(questionId, "bot-1", target, "yes", "owner-1");
      })).toBe(true);
      expect(publishCount).toBe(1);
      expect(store.getIngressJobBySource(key)).toMatchObject({
        state: "handed-off",
        messageId: "shared-card-message",
        consumerKind: "ask",
        consumerId: consumer.id,
      });
      expect(store.getQuestion(questionId)).toMatchObject({
        state: "answered",
        answer: "yes",
        answeredBy: "owner-1",
      });

      const reopened = await openChannelStore(root);
      store.close();
      store = reopened;
      expect(store.getQuestion(questionId)).toMatchObject({ state: "answered", answer: "yes", answeredBy: "owner-1" });
      expect(store.acceptAndHandoffIngressJob(eventInput, consumer, () => {
        publishCount += 1;
        return false;
      })).toBe(true);
      expect(publishCount).toBe(1);
      expect(store.getIngressJobBySource(key)).toMatchObject({ state: "handed-off", consumerId: consumer.id });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("consumer persistence and ingress handoff roll back together on a synchronous failure", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-handoff-rollback-"));
  try {
    const store = await openChannelStore(root);
    const jobs = store;
    const target = { channelType: 2 as const, channelId: "group-1" };
    const claim = {
      botUid: "bot-1",
      target,
      messageId: "handoff-message",
      senderId: "owner-1",
      text: "review this",
      supported: true,
      kind: "task" as const,
    };
    const task = {
      id: "task-from-ingress",
      botUid: "bot-1",
      channelType: 2 as const,
      channelId: "group-1",
      messageId: "handoff-message",
      senderId: "owner-1",
      text: "review this",
      sessionId: "session-4",
      cwd: "/work/chat",
      state: "queued" as const,
      resultText: null,
      errorText: null,
      messageSeq: 12,
      historyEligible: true,
      relaySuppressed: false,
    };
    try {
      const accepted = jobs.acceptIngressJob(input({
        messageId: "handoff-message",
        payload: receivePayload("handoff-message"),
      }));
      expect(() => jobs.handoffIngressJob(accepted.job.id, { kind: "task", id: task.id }, () => {
        expect(store.acceptTask(claim, task)).toBe(true);
        throw new Error("forced failure after task persistence");
      })).toThrow("forced failure after task persistence");

      expect(store.getTaskByMessageId("bot-1", target, "handoff-message")).toBeNull();
      expect(jobs.getIngressJob(accepted.job.id)).toMatchObject({
        state: "accepted",
        payload: receivePayload("handoff-message"),
        consumerKind: null,
        consumerId: null,
      });
      expect(jobs.handoffIngressJob(accepted.job.id, { kind: "task", id: task.id }, () => store.acceptTask(claim, task))).toBe(true);
      expect(store.getTaskByMessageId("bot-1", target, "handoff-message")).not.toBeNull();
      expect(jobs.getIngressJob(accepted.job.id)).toMatchObject({
        state: "handed-off",
        payload: null,
        routeSnapshot: accepted.job.routeSnapshot,
        operationCheckpoint: null,
        consumerKind: "task",
        consumerId: task.id,
      });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("a durable refusal outbox atomically makes unprepared ingress ACK-ready", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-refusal-handoff-"));
  try {
    const store = await openChannelStore(root);
    try {
      const accepted = store.acceptIngressJob(input({
        messageId: "media-refused",
        payload: receivePayload("media-refused"),
        state: "preparing",
        ackReady: false,
      }));
      const outbox = {
        id: "media-refusal-outbox",
        botUid: "bot-1",
        channelType: 2 as const,
        channelId: "group-1",
        taskId: null,
        kind: "text" as const,
        text: "The attachment could not be processed.",
        path: null,
        fileKind: null,
        clientMsgNo: "media-refusal-message",
      };

      expect(() => store.handoffIngressJob(accepted.job.id, { kind: "outbox", id: outbox.id }, () => {
        store.enqueueOutbox(outbox);
        throw new Error("forced failure after refusal outbox persistence");
      })).toThrow("forced failure after refusal outbox persistence");
      expect(store.getOutbox(outbox.id)).toBeNull();
      expect(store.getIngressJob(accepted.job.id)).toMatchObject({
        state: "preparing",
        ackReady: false,
      });

      expect(store.handoffIngressJob(accepted.job.id, { kind: "outbox", id: outbox.id }, () => {
        store.enqueueOutbox(outbox);
        return true;
      })).toBe(true);
      expect(store.getOutbox(outbox.id)).toMatchObject({ state: "pending", text: outbox.text });
      expect(store.getIngressJob(accepted.job.id)).toMatchObject({
        state: "handed-off",
        ackReady: true,
        payload: null,
        consumerKind: "outbox",
        consumerId: outbox.id,
      });
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("opening a legacy database preserves pending rows without treating them as ingress recovery work", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-legacy-migration-"));
  const path = join(root, "channel.sqlite");
  try {
    const legacy = new Database(path);
    try {
      legacy.exec(`
        CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        INSERT INTO meta (key, value) VALUES ('legacy-marker', 'keep-me');
        CREATE TABLE owners (
          bot_uid TEXT PRIMARY KEY NOT NULL,
          owner_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO owners (bot_uid, owner_id, created_at, updated_at)
        VALUES ('legacy-bot', 'legacy-owner', 11, 12);
        CREATE TABLE inbound (
          bot_uid TEXT NOT NULL,
          channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
          channel_id TEXT NOT NULL,
          message_id TEXT NOT NULL,
          sender_id TEXT NOT NULL,
          text TEXT NOT NULL,
          supported INTEGER NOT NULL,
          kind TEXT NOT NULL CHECK(kind IN ('task', 'command')),
          command_state TEXT NOT NULL DEFAULT 'pending' CHECK(command_state IN ('pending', 'completed', 'failed')),
          task_id TEXT,
          accepted_at INTEGER NOT NULL
        );
        INSERT INTO inbound(
          bot_uid, channel_type, channel_id, message_id, sender_id, text, supported, kind, command_state, task_id, accepted_at
        ) VALUES('legacy-bot', 2, 'legacy-group', 'legacy-command', 'legacy-owner', '/thread merge', 1, 'command', 'pending', NULL, 123);
      `);
    } finally {
      legacy.close();
    }

    let store = await openChannelStore(root);
    let jobs = store;
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      expect(store.getCommandClaimState("legacy-bot", { channelType: 2, channelId: "legacy-group" }, "legacy-command")).toBe("pending");
      expect(jobs.listRecoverableIngressJobs("legacy-bot")).toEqual([]);
      const migrated = jobs.acceptIngressJob(input({
        botUid: "legacy-bot",
        channelId: "legacy-group",
        messageId: "new-recv",
        payload: receivePayload("new-recv", "new legacy-chat message", 2, "legacy-group"),
      }));
      expect(migrated.created).toBe(true);
      store.close();
    } catch (error) {
      store.close();
      throw error;
    }

    store = await openChannelStore(root);
    jobs = store;
    try {
      expect(store.getOwnerId("legacy-bot")).toBe("legacy-owner");
      expect(store.getCommandClaimState("legacy-bot", { channelType: 2, channelId: "legacy-group" }, "legacy-command")).toBe("pending");
      expect(jobs.listRecoverableIngressJobs("legacy-bot").map((job) => job.messageId)).toEqual(["new-recv"]);
    } finally {
      store.close();
    }

    const reopened = new Database(path);
    try {
      expect(reopened.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get()).toEqual({ value: "keep-me" });
      expect(reopened.query("SELECT sender_id, text, command_state, accepted_at FROM inbound WHERE message_id = 'legacy-command'").get())
        .toEqual({ sender_id: "legacy-owner", text: "/thread merge", command_state: "pending", accepted_at: 123 });
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
