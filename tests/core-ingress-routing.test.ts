import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import type { ChannelService, InboundMessage } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";


type MaterializerSeam = {
  materializeForIngress(
    jobId: number,
    message: InboundMessage,
    text?: string,
    signal?: AbortSignal,
  ): Promise<unknown>;
};

type PreparedQuestionConsumption = {
  consumerId: string;
  persist(): boolean;
  publishAfterCommit(): unknown;
};

type QuestionSnapshotSeam = {
  prepareTextSnapshot(...args: unknown[]): PreparedQuestionConsumption;
};

type CoreIngressSeams = {
  materializer: MaterializerSeam;
  questions: QuestionSnapshotSeam;
};


function seams(service: ChannelService): CoreIngressSeams {
  return service as unknown as CoreIngressSeams;
}

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

async function waitForIngressState(
  stateDir: string,
  message: InboundMessage,
  expected: string,
): Promise<void> {
  const database = new Database(`${stateDir}/channel.sqlite`, { readonly: true });
  try {
    await waitUntil(() => {
      const row = database.query(`
        SELECT state
        FROM ingress_jobs
        WHERE channel_type = ?1 AND channel_id = ?2 AND message_id = ?3
      `).get(message.channelType, message.channelId, message.messageId) as { state?: string } | null;
      return row?.state === expected;
    }, `ingress ${message.messageId} state ${expected}`);
  } finally {
    database.close();
  }
}

function askTool(context: { tools?: readonly { name: string; execute(args: Record<string, unknown>, signal: AbortSignal): Promise<string> }[] } | undefined) {
  const ask = context?.tools?.find(({ name }) => name === "ask");
  if (!ask) throw new Error("ask host tool missing");
  return ask;
}

test("an input frozen with no pending question remains ordinary when a question opens during preparation", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const openQuestion = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text !== "start the question later") throw new Error(`unexpected prompt: ${text}`);
      promptStarted.resolve();
      await openQuestion.promise;
      await askTool(promptContext).execute({
        questions: [{ id: "later-question", question: "Question opened after ingress admission" }],
      }, new AbortController().signal);
    },
  });
  const target = { channelType: 1 as const, channelId: "dm-ingress-route-none" };
  const initial = inbound({
    ...target,
    messageId: "ingress-route-none-initial",
    text: "start the question later",
  });
  const oldInput = inbound({
    ...target,
    messageId: "ingress-route-none-old-input",
    text: "this was ordinary before Q2 existed",
  });
  const materializeStarted = Promise.withResolvers<void>();
  const releaseMaterialize = Promise.withResolvers<void>();

  try {
    const initialReceipt = harness.service.receive(initial);
    await initialReceipt.ackReady;
    await promptStarted.promise;

    const core = seams(harness.service);
    const materialize = core.materializer.materializeForIngress.bind(core.materializer);
    core.materializer.materializeForIngress = async (jobId, message, text, signal) => {
      if (message.messageId === oldInput.messageId) {
        materializeStarted.resolve();
        await releaseMaterialize.promise;
      }
      return materialize(jobId, message, text, signal);
    };

    const oldReceipt = harness.service.receive(oldInput);
    await materializeStarted.promise;
    openQuestion.resolve();
    const opened = await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 1);
    const questionId = opened.pendingQuestionIds[0]!;

    releaseMaterialize.resolve();
    await oldReceipt.ackReady;
    await waitUntil(
      () => harness.workers.workers[0]?.steerCalls.some(text => text === oldInput.text) === true,
      "old ordinary input steering",
    );

    const after = await harness.service.status(target);
    expect(after.pendingQuestionIds).toEqual([questionId]);
    expect(harness.workers.workers[0]?.steerCalls).toEqual([oldInput.text]);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getQuestion(questionId)?.state).toBe("pending");
      expect(store.getLatestTask(harness.bot.uid, target)?.messageId).toBe(initial.messageId);
    } finally {
      store.close();
    }

    const answer = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-route-none-q2-answer",
      text: "answer Q2",
    }));
    await answer.ackReady;
    await waitForStatus(harness.service, target,
      status => status.state === "completed" && status.pendingQuestionIds.length === 0);
  } finally {
    openQuestion.resolve();
    releaseMaterialize.resolve();
    await closeCoreHarness(harness);
  }
});

test("a frozen question route cannot retarget an old reply to a newer pending question", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ promptContext, text }) => {
      if (text !== "open Q1") throw new Error(`unexpected prompt: ${text}`);
      await askTool(promptContext).execute({
        questions: [{ id: "q1", question: "Original Q1" }],
      }, new AbortController().signal);
    },
  });
  const target = { channelType: 1 as const, channelId: "dm-ingress-route-exact-question" };
  const initial = inbound({
    ...target,
    messageId: "ingress-route-q1-initial",
    text: "open Q1",
  });
  const oldReply = inbound({
    ...target,
    messageId: "ingress-route-q1-old-reply",
    text: "reply captured for Q1 only",
  });
  let mutationObserved = false;

  try {
    const initialReceipt = harness.service.receive(initial);
    await initialReceipt.ackReady;
    const q1Status = await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 1);
    const q1Id = q1Status.pendingQuestionIds[0]!;

    const core = seams(harness.service);
    expect(typeof core.questions.prepareTextSnapshot).toBe("function");
    const prepare = core.questions.prepareTextSnapshot.bind(core.questions);
    const mutationStore = await openChannelStore(harness.stateDir);
    try {
      const q1 = mutationStore.getQuestion(q1Id);
      if (!q1) throw new Error("Q1 row missing");
      core.questions.prepareTextSnapshot = (...args) => {
        const message = args[0] as InboundMessage;
        if (!mutationObserved && message.messageId === oldReply.messageId) {
          if (!mutationStore.answerQuestion(q1Id, harness.bot.uid, target, "answered elsewhere", "owner-1")) {
            throw new Error("fixture failed to close Q1");
          }
          mutationStore.createQuestion({
            id: "ingress-route-q2",
            botUid: harness.bot.uid,
            taskId: q1.taskId,
            ...target,
            payload: JSON.stringify({ question: "Newer Q2", toolCallId: "ingress-route-q2-call" }),
          });
          mutationObserved = true;
        }
        return prepare(...args);
      };
      const oldReceipt = harness.service.receive(oldReply);
      await oldReceipt.ackReady;
      await waitUntil(() => mutationObserved, "question replacement during exact-route dispatch");
    } finally {
      mutationStore.close();
    }
    await waitForIngressState(harness.stateDir, oldReply, "handed-off");

    const after = await openChannelStore(harness.stateDir);
    try {
      expect(after.getQuestion(q1Id)).toMatchObject({ state: "answered", answer: "answered elsewhere" });
      expect(after.getQuestion("ingress-route-q2")?.state).toBe("pending");
      expect(after.getLatestTask(harness.bot.uid, target)?.messageId).toBe(initial.messageId);
    } finally {
      after.close();
    }
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.workers[0]?.steerCalls).toEqual([]);

    const redelivery = harness.service.receive(oldReply);
    await redelivery.ackReady;
    const finalStore = await openChannelStore(harness.stateDir);
    try {
      expect(finalStore.getQuestion("ingress-route-q2")?.state).toBe("pending");
    } finally {
      finalStore.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});
