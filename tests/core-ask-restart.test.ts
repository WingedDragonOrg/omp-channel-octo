import { expect, test } from "bun:test";
import { rm } from "node:fs/promises";
import type { BotCardAction, ChatTarget } from "../src/contracts";
import { createQuestionDesk } from "../src/questions";
import { openChannelStore, type ChannelStore } from "../src/store";
import type { TaskRecord } from "../src/store/store-records.ts";
import {
  FakeTransport,
  type CoreHarness,
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

/**
 * 重启前后的 ask：第一段进程用真服务把问题发出去然后停机，第二段进程用真 store、真卡片构造
 * 认领遗留问题行（服务侧端口换成记录器；`beginRecovery` 由服务在 worker.resume 前调用）。
 */
const TARGET: ChatTarget = { channelId: "dm-restart-question", channelType: 1 };
const OWNER = "owner-1";
const BOT_UID = "bot-restart-question";

/** 一次 ask 的参数：续跑里 OMP 重新发起的就是同一份。 */
function args(questions: Array<Record<string, unknown>>): Record<string, unknown> {
  return { questions };
}

type Answers = Array<{ id: string; questionId: string; answer: string; answeredBy: string | null }>;

function answers(json: string): Answers {
  return (JSON.parse(json) as { answers: Answers }).answers;
}

interface InterruptedAsk {
  readonly root: string;
  readonly stateDir: string;
  readonly questionIds: readonly string[];
  readonly cardMessageId: string;
  /** 停机前那段进程的传输记录：卡片发送与改写都留在这里。 */
  readonly transport: FakeTransport;
}

/**
 * 第一段进程：真服务里发起一次 ask，随后按停机收场（`closeCoreHarness` → `service.stop()`）。
 * 停机只撤回内存等待者，问题行与卡片留在库里，交给恢复端认领。
 */
async function interruptAsk(
  questions: Array<Record<string, unknown>>,
  options: {
    /** OMP 给这次工具调用的身份：宿主把它透传给 `ask`，retry 重执行时同一个 id。 */
    toolCallId?: string;
    beforeStop?: (harness: CoreHarness) => Promise<void>;
  } = {},
): Promise<InterruptedAsk> {
  const transport = new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture Bot" } });
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    ask: { timeoutMs: 600_000 },
    prompt: async ({ promptContext }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      try {
        await ask.execute(args(questions), new AbortController().signal, options.toolCallId);
      } catch {
        // 停机撤回了等待：问题保持待答，由恢复端认领。
      }
    },
  });
  let questionIds: string[] = [];
  let cardMessageId: string | null = null;
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "restart-ask", ...TARGET, text: "ask the owner" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      TARGET,
      (status) => status.pendingQuestionIds.length === questions.length,
    );
    questionIds = [...pending.pendingQuestionIds];
    cardMessageId = transport.sentCards[0]?.messageId ?? null;
    await options.beforeStop?.(harness);
  } finally {
    await closeCoreHarness(harness, { remove: false });
  }
  if (cardMessageId === null) throw new Error("ask card was not sent");
  return { root: harness.root, stateDir: harness.stateDir, questionIds, cardMessageId, transport };
}

interface DeskRecord {
  readonly texts: Array<{ target: ChatTarget; text: string; taskId: string | null }>;
  readonly wakes: Array<{ target: ChatTarget; text: string }>;
  readonly stops: string[];
}

/** 第二段进程的问答子系统：真 store、真卡片构造，服务侧端口换成记录器。 */
function resumeDesk(store: ChannelStore, options: { stopping?: boolean; timeoutMs?: number } = {}) {
  const transport = new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture Bot" } });
  transport.cardSupported = true;
  const record: DeskRecord = { texts: [], wakes: [], stops: [] };
  const desk = createQuestionDesk({
    store,
    transport,
    botUid: () => BOT_UID,
    stopping: () => options.stopping ?? false,
    enqueueText: (target, text, taskId) => {
      record.texts.push({ target, text, taskId });
    },
    operationSet: () => {},
    commandClaim: (message) => ({
      botUid: BOT_UID,
      target: message,
      messageId: message.messageId,
      senderId: message.senderId,
      text: message.text,
      supported: message.supported,
      kind: "command",
    }),
    askTimeoutMs: () => options.timeoutMs ?? 600_000,
    displayNames: async (uids) => new Map(uids.map((uid) => [uid, uid])),
    stopTurnForQuestion: (taskId) => {
      record.stops.push(taskId);
    },
    wakeTurn: (target, text) => {
      record.wakes.push({ target, text });
    },
    memberAnswerAllowed: () => false,
  });
  return { desk, transport, record };
}

/** 恢复端的任务行：同一个聊天最后持久接受的那个。 */
function latestTask(store: ChannelStore): TaskRecord {
  const task = store.getLatestTask(BOT_UID, TARGET);
  if (task === null) throw new Error("task row missing");
  return task;
}

/** 一次卡片点击，按 Bot 事件队列交付的形状。 */
function click(messageId: string, actionId: string, inputs: Record<string, string> = {}): BotCardAction {
  return {
    messageId,
    channelType: TARGET.channelType,
    channelId: TARGET.channelId,
    actionId,
    operatorUid: OWNER,
    inputs,
  };
}

async function withStore<T>(interrupted: InterruptedAsk, work: (store: ChannelStore) => Promise<T>): Promise<T> {
  const store = await openChannelStore(interrupted.stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
    await rm(interrupted.root, { recursive: true, force: true });
  }
}

const SINGLE = [
  {
    id: "deployment-target",
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
  },
];
const SET = [
  { id: "target", question: "Which deployment target?", options: [{ label: "staging" }, { label: "production" }] },
  { id: "window", question: "When do we ship?" },
];

test("停机不吊销待答问题：恢复端认领原行，原卡片点击照旧作答", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const [row] = store.listTaskQuestions(task.id);
    if (row === undefined) throw new Error("question row missing");
    expect(row.state).toBe("pending");
    expect(row.cardMessageId).toBe(interrupted.cardMessageId);

    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal);

    // 认领：不新建行、不重发卡。
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual([row.id]);
    expect(transport.sentCards).toHaveLength(0);

    expect(await desk.answerFromCard(click(interrupted.cardMessageId, "ask_opt_2"))).toBe("handled");
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: row.id, answer: "2", answeredBy: OWNER },
    ]);
    // 改写的是原来那张卡，没有第二张。
    const edit = await transport.waitForCardEdit(({ messageId }) => messageId === interrupted.cardMessageId);
    expect(edit.cardSeq).toBeGreaterThan(1);
    expect(transport.sentCards).toHaveLength(0);
  });
});

test("恢复端重挂后普通回复即答案，且不另起回合", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal);

    const answered = await desk.answerPendingQuestion(
      inbound({ messageId: "restart-reply", ...TARGET, text: "2" }),
      "2",
    );
    expect(answered).toBe(true);
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(record.wakes).toHaveLength(0);
    expect(transport.sent).toHaveLength(0);
  });
});

test("停机窗口内的答复由认领方消费，且同一身份只消费一次", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport, record } = resumeDesk(store);
    desk.beginRecovery(task.id);

    // 续跑的 ask 还没重挂：这条答复不能另起一个回合，它属于即将认领这行的那次调用。
    const answered = await desk.answerPendingQuestion(
      inbound({ messageId: "restart-race", ...TARGET, text: "2" }),
      "2",
    );
    expect(answered).toBe(true);
    expect(record.wakes).toHaveLength(0);
    expect(store.getQuestion(rowId)?.state).toBe("answered");

    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal);
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(transport.sentCards).toHaveLength(0);
    expect(store.listTaskQuestions(task.id)).toHaveLength(1);

    // 认领一次即消费：同一任务里以后的同类提问重新建行，不会拿到旧答案。
    const abandoned = new AbortController();
    abandoned.abort();
    const later = desk.askQuestions(task, TARGET, args(SINGLE), abandoned.signal);
    await expect(later).rejects.toThrow();
    const rows = store.listTaskQuestions(task.id);
    expect(rows).toHaveLength(2);
    expect(rows[1]!.id).not.toBe(rowId);
    // 答案已经交付给那次调用：任务收束时不会再补投一遍。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(record.wakes).toHaveLength(0);
  });
});

test("停机窗口内的答复在任务收束时补投新回合", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    // 答复在续跑 ask 重挂之前落地：先不另起回合，等认领。
    await desk.answerPendingQuestion(inbound({ messageId: "restart-orphan", ...TARGET, text: "2" }), "2");
    expect(record.wakes).toHaveLength(0);
    // 续跑回合始终没来认领就收束：这时补投，答复与署名都不会丢。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(record.wakes).toHaveLength(1);
    expect(record.wakes[0]?.text).toContain("production");
    expect(record.wakes[0]?.text).toContain(OWNER);
    expect(store.getQuestion(rowId)?.state).toBe("answered");
  });
});

test("显式 /cancel 的问题在重启后仍是终态，不被认领复活", async () => {
  const interrupted = await interruptAsk(SINGLE, {
    beforeStop: async (harness) => {
      await harness.service.handleInbound(
        inbound({ messageId: "restart-cancel", ...TARGET, text: "/cancel" }),
      );
    },
  });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
    // 显式取消同时封闭卡片。
    const closed = await interrupted.transport.waitForCardEdit(
      ({ messageId }) => messageId === interrupted.cardMessageId,
    );
    expect(JSON.stringify(closed.frame.card)).toContain("任务已取消");

    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const abandoned = new AbortController();
    abandoned.abort();
    const call = desk.askQuestions(task, TARGET, args(SINGLE), abandoned.signal);
    await expect(call).rejects.toThrow();
    const rows = store.listTaskQuestions(task.id);
    expect(rows).toHaveLength(2);
    expect(rows[0]?.id).toBe(rowId);
    expect(rows[0]?.state).toBe("cancelled");
    expect(rows[1]!.id).not.toBe(rowId);
    expect(transport.sentCards).toHaveLength(0);
  });
});

test("多问卡跨重启保持部分答案、原行与卡片", async () => {
  const interrupted = await interruptAsk(SET, {
    beforeStop: async (harness) => {
      harness.transport.pushBotEvent(
        cardActionEvent(11, click(interruptedCardMessageId(harness), "ask_submit", { ask_choices_1: "1" })),
      );
      await waitForStatus(harness.service, TARGET, (status) => status.pendingQuestionIds.length === 1);
    },
  });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const before = store.listTaskQuestions(task.id);
    expect(before.map(({ state }) => state)).toEqual(["answered", "pending"]);

    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(task, TARGET, args(SET), new AbortController().signal);
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual(before.map(({ id }) => id));
    expect(transport.sentCards).toHaveLength(0);

    // 上一段进程已经把卡片改写到第 2 代，恢复端接着同一张卡的代次继续。
    const submit = click(interrupted.cardMessageId, "ask_submit#2", { ask_text_2: "tonight" });
    expect(await desk.answerFromCard(submit)).toBe("handled");
    expect(answers(await call)).toEqual([
      { id: "target", questionId: before[0]!.id, answer: "1", answeredBy: OWNER },
      { id: "window", questionId: before[1]!.id, answer: "tonight", answeredBy: OWNER },
    ]);
    const edit = await transport.waitForCardEdit(({ messageId }) => messageId === interrupted.cardMessageId);
    // 帧序号必须严格大于上一段进程在这张卡上用过的值，否则服务器会拒收这次编辑。
    const lastSeq = interrupted.transport.cardEdits.at(-1)?.cardSeq ?? 0;
    expect(edit.cardSeq).toBeGreaterThan(lastSeq);
    expect(store.listTaskQuestions(task.id).map(({ state }) => state)).toEqual(["answered", "answered"]);
  });
});

test("答复已落盘但结果从未交付：同一个工具调用重复执行拿回原结果，不重新提问", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-undelivered" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    // 旧进程在把结果交回 OMP 之前就死了：行上的答复已经落盘，等待者却没有兑现（结果没交付）。
    expect(store.answerQuestion(rowId, BOT_UID, TARGET, "2", OWNER)).toBe(true);

    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(
      task,
      TARGET,
      args(SINGLE),
      new AbortController().signal,
      "call-undelivered",
    );
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id)).toHaveLength(1);
    expect(transport.sentCards).toHaveLength(0);
  });
});

test("多问卡全部答复落盘、结果未交付时，整组认领而不重新提问", async () => {
  const interrupted = await interruptAsk(SET, { toolCallId: "call-set-undelivered" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const [firstRow, secondRow] = interrupted.questionIds;
    const landed = store.answerQuestions(BOT_UID, TARGET, [
      { id: firstRow!, answer: "1", answeredBy: OWNER },
      { id: secondRow!, answer: "tonight", answeredBy: OWNER },
    ]);
    expect(landed).toHaveLength(2);

    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(
      task,
      TARGET,
      args(SET),
      new AbortController().signal,
      "call-set-undelivered",
    );
    expect(answers(await call)).toEqual([
      { id: "target", questionId: firstRow!, answer: "1", answeredBy: OWNER },
      { id: "window", questionId: secondRow!, answer: "tonight", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual([firstRow!, secondRow!]);
    expect(transport.sentCards).toHaveLength(0);
  });
});

test("同一次工具调用的重复执行是幂等的，即使上次已经交回结果", async () => {
  const interrupted = await interruptAsk(SINGLE, {
    toolCallId: "call-replay",
    beforeStop: async (harness) => {
      const card = interruptedCardMessageId(harness);
      harness.transport.pushBotEvent(cardActionEvent(21, click(card, "ask_opt_2")));
      await waitForStatus(harness.service, TARGET, (status) => status.state === "completed");
    },
  });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    // OMP 可能还没把这轮的结果落盘，同一个 toolCallId 的重执行必须拿回同一份答案。
    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-replay");
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id)).toHaveLength(1);
    expect(transport.sentCards).toHaveLength(0);
  });
});

test("已交付的历史不被新的工具调用复用", async () => {
  const interrupted = await interruptAsk(SINGLE, {
    toolCallId: "call-done",
    beforeStop: async (harness) => {
      const card = interruptedCardMessageId(harness);
      harness.transport.pushBotEvent(cardActionEvent(22, click(card, "ask_opt_2")));
      await waitForStatus(harness.service, TARGET, (status) => status.state === "completed");
    },
  });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const abandoned = new AbortController();
    abandoned.abort();
    await expect(
      desk.askQuestions(task, TARGET, args(SINGLE), abandoned.signal, "call-new"),
    ).rejects.toThrow();
    // 新的调用重新提问，不会拿旧答案顶替。
    const rows = store.listTaskQuestions(task.id);
    expect(rows).toHaveLength(2);
    expect(rows[1]!.id).not.toBe(rowId);
    expect(rows[0]!.state).toBe("answered");
    expect(transport.sentCards).toHaveLength(0);
  });
});

/** 第一段进程发出的那张卡片的远端 id。 */
function interruptedCardMessageId(harness: CoreHarness): string {
  const card = harness.transport.sentCards[0];
  if (card === undefined) throw new Error("ask card was not sent");
  return card.messageId;
}

test("任务不再恢复时 cancelForTask 关闭它遗留的问题与卡片", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    desk.cancelForTask(task.id);
    desk.cancelForTask(task.id);
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
    const edit = await transport.waitForCardEdit(({ messageId }) => messageId === interrupted.cardMessageId);
    expect(JSON.stringify(edit.frame.card)).toContain("任务已中断");
  });
});

test("恢复后没有重发 ask 就收束的任务不留幽灵接收者", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    // 续跑回合没有重新发起那次 ask 就结束：遗留的等待随任务收束关闭。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
    const edit = await transport.waitForCardEdit(({ messageId }) => messageId === interrupted.cardMessageId);
    expect(JSON.stringify(edit.frame.card)).toContain("任务已结束");
    // 下一条用户文本因此不会被当成旧问题的答案。
    const answered = await desk.answerPendingQuestion(
      inbound({ messageId: "restart-ghost", ...TARGET, text: "换一个任务" }),
      "换一个任务",
    );
    expect(answered).toBe(false);
  });
});

test("认领后超时的等待保持待答，迟到答复仍唤醒回合", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, record } = resumeDesk(store, { timeoutMs: 20 });
    desk.beginRecovery(task.id);
    await expect(desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal)).rejects.toThrow();
    expect(record.stops).toEqual([task.id]);
    expect(store.getQuestion(rowId)?.state).toBe("pending");
    // 超时停住回合后任务收束，但认领过的等待不被关闭：owner 之后的答复唤醒新回合。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(store.getQuestion(rowId)?.state).toBe("pending");
    await desk.answerPendingQuestion(inbound({ messageId: "restart-late", ...TARGET, text: "2" }), "2");
    expect(record.wakes).toHaveLength(1);
  });
});

test("停机中的中止保留问题行，非停机中止照常取消", async () => {
  for (const scenario of [
    { stopping: true, expected: "pending" as const },
    { stopping: false, expected: "cancelled" as const },
  ]) {
    const interrupted = await interruptAsk(SINGLE);
    await withStore(interrupted, async (store) => {
      const task = latestTask(store);
      const { desk } = resumeDesk(store, { stopping: scenario.stopping });
      const controller = new AbortController();
      const call = desk.askQuestions(task, TARGET, args(SINGLE), controller.signal);
      const started = store.listTaskQuestions(task.id).at(-1);
      controller.abort();
      await expect(call).rejects.toThrow();
      expect(started?.id).not.toBe(interrupted.questionIds[0]);
      expect(store.getQuestion(started!.id)?.state).toBe(scenario.expected);
    });
  }
});
