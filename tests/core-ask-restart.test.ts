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
  /** 每次停回合交出的待取消旧问题行：服务侧本应与停止意图写进同一事务。 */
  readonly stopIntents: Array<{ taskId: string; ids: readonly string[] }>;
}

/** 第二段进程的问答子系统：真 store、真卡片构造，服务侧端口换成记录器。 */
function resumeDesk(
  store: ChannelStore,
  options: { stopping?: boolean; timeoutMs?: number; transport?: FakeTransport } = {},
) {
  const transport = options.transport ?? new FakeTransport({ bot: { uid: BOT_UID, name: "Fixture Bot" } });
  transport.cardSupported = true;
  const record: DeskRecord = { texts: [], wakes: [], stops: [], stopIntents: [] };
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
    stopTurnForQuestion: (taskId, cancelQuestionIds) => {
      record.stops.push(taskId);
      record.stopIntents.push({ taskId, ids: cancelQuestionIds });
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
    const { desk, transport, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-replay");
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id)).toHaveLength(1);
    expect(transport.sentCards).toHaveLength(0);

    // 已经被这一次重放消费：另一个身份的同类提问不许再拿走这行。
    const abandoned = new AbortController();
    abandoned.abort();
    await expect(
      desk.askQuestions(task, TARGET, args(SINGLE), abandoned.signal, "call-other"),
    ).rejects.toThrow();
    expect(store.listTaskQuestions(task.id)).toHaveLength(2);
    // 结果已经交付过，任务收束时不再补投同一个答复。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(record.wakes).toHaveLength(0);
  });
});

test("落定写不进库：答复不当作交付，本次 ask 明确失败", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    // 与停机前那段进程共用一条传输：卡片 id 才会接着编，新卡不撞旧卡的号。
    const { desk, transport } = resumeDesk(store, { transport: interrupted.transport });
    Object.assign(store, {
      markQuestionsSettled: () => {
        throw new Error("settle write failed");
      },
    });
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-settle-fail");
    const { messageId } = await transport.waitForCardSend(({ messageId: id }) => id !== interrupted.cardMessageId);
    expect(await desk.answerFromCard(click(messageId, "ask_opt_2"))).toBe("handled");
    await expect(call).rejects.toThrow("settle write failed");
    // 答案本身已经记在行上，丢的只是「这次交付已落定」的凭据。
    expect(store.listTaskQuestions(task.id).at(-1)!.state).toBe("answered");
  });
});

test("超时路径的落定再失败也不掩盖原来的超时错误", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const { desk } = resumeDesk(store, { timeoutMs: 20 });
    Object.assign(store, {
      markQuestionsSettled: () => {
        throw new Error("settle write failed");
      },
    });
    await expect(
      desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-timeout-settle-fail"),
    ).rejects.toThrow("ask wait stopped: the turn was halted after the question timeout");
  });
});

test("语义认领的身份写不进库：本次 ask 明确失败，行保持可重试且不发新卡", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-adopt-fail" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const cardsBefore = transport.sentCards.length;
    // 故障注入：身份写回失败（事务因缺行回滚）。
    const healthyWrite = store.updateQuestionPayloads.bind(store);
    Object.assign(store, {
      updateQuestionPayloads: () => {
        throw new Error("question payload update touched 0 of 1 rows");
      },
    });
    let failure: unknown;
    try {
      await desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-adopt-fail-next");
    } catch (error) {
      failure = error;
    }
    expect(String(failure)).toContain("question payload update touched 0 of 1 rows");
    // 没有消费恢复组：行仍是原样、仍待答，一个新行、一张新卡都没有。
    const rows = store.listTaskQuestions(task.id);
    expect(rows.map(({ id }) => id)).toEqual([rowId]);
    expect(rows[0]!.state).toBe("pending");
    expect(payloadOf(store, rowId)).toMatchObject({ toolCallId: "call-adopt-fail" });
    expect(transport.sentCards).toHaveLength(cardsBefore);
    expect(record.wakes).toHaveLength(0);
    // 写回恢复后，同一个调用重执行仍能认回原行：没有新题、没有新卡。
    Object.assign(store, { updateQuestionPayloads: healthyWrite });
    const healthy = resumeDesk(store, { transport });
    healthy.desk.beginRecovery(task.id);
    const retry = healthy.desk.askQuestions(
      task, TARGET, args(SINGLE), new AbortController().signal, "call-adopt-fail-next");
    // 复用原行就是复用原卡：点击落在停机前那张卡上。
    expect(await healthy.desk.answerFromCard(click(interrupted.cardMessageId, "ask_opt_2")))
      .toBe("handled");
    expect(answers(await retry)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual([rowId]);
    expect(transport.sentCards).toHaveLength(cardsBefore);
    expect(payloadOf(store, rowId)).toMatchObject({ toolCallId: "call-adopt-fail-next" });
  });
});

test("同一个工具调用 id 重执行但参数不同：明确失败，不另起新题", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-args-drift" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, transport } = resumeDesk(store);
    desk.beginRecovery(task.id);
    const cardsBefore = transport.sentCards.length;
    await expect(
      desk.askQuestions(
        task,
        TARGET,
        args([{ id: "deployment-target", question: "Another deployment target?", options: [{ label: "staging" }, { label: "production" }] }]),
        new AbortController().signal,
        "call-args-drift",
      ),
    ).rejects.toThrow("同一工具调用的重执行与原提问不一致");
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual([rowId]);
    expect(transport.sentCards).toHaveLength(cardsBefore);
  });
});

test("重复执行认回窗口里的答复后，任务收束时不再补投", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-window-replay" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    // 答复在重挂之前落地：先不另起回合，留给这次执行认领。
    await desk.answerPendingQuestion(inbound({ messageId: "restart-window-replay", ...TARGET, text: "2" }), "2");
    expect(record.wakes).toHaveLength(0);
    const call = desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-window-replay");
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    // 答复已经交付给这次执行：任务收束时不能再补投一遍。
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(record.wakes).toHaveLength(0);
  });
});

test("重复执行的等待超时后，待答行不被收束关闭，迟到答复仍唤醒回合", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-replay-timeout" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, record } = resumeDesk(store, { timeoutMs: 20 });
    desk.beginRecovery(task.id);
    await expect(
      desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-replay-timeout"),
    ).rejects.toThrow();
    expect(record.stops).toEqual([task.id]);
    expect(store.getQuestion(rowId)?.state).toBe("pending");
    desk.rejectForTask(task.id, new Error("task finished"));
    expect(store.getQuestion(rowId)?.state).toBe("pending");
    await desk.answerPendingQuestion(inbound({ messageId: "restart-late-replay", ...TARGET, text: "2" }), "2");
    expect(record.wakes).toHaveLength(1);
  });
});

test("同形的两个被中断调用各自认回自己的行", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    // 旧的进程里有两个同形调用同时挂着（并行工具调用），各自带着自己的工具调用 id。
    // 两张卡共用一个传输，卡片 id 因此在同一个命名空间里递增，不会互相顶替。
    const first = resumeDesk(store, { transport: interrupted.transport });
    const firstCall = first.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-a");
    const rowA = store.listTaskQuestions(task.id).at(-1)!.id;
    const cardA = await first.transport.waitForCardSend(({ messageId }) => messageId !== interrupted.cardMessageId)
      .then(({ messageId }) => messageId);
    const second = resumeDesk(store, { transport: first.transport });
    const secondCall = second.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-b");
    const rowB = store.listTaskQuestions(task.id).at(-1)!.id;
    const cardB = await second.transport
      .waitForCardSend(({ messageId }) => messageId !== cardA && messageId !== interrupted.cardMessageId)
      .then(({ messageId }) => messageId);
    expect(rowA).not.toBe(rowB);
    expect(cardA).not.toBe(cardB);

    const resumed = resumeDesk(store);
    resumed.desk.beginRecovery(task.id);
    const replayA = resumed.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-a");
    expect(await resumed.desk.answerFromCard(click(cardA, "ask_opt_2"))).toBe("handled");
    expect(answers(await replayA)).toEqual([
      { id: "deployment-target", questionId: rowA, answer: "2", answeredBy: OWNER },
    ]);
    const replayB = resumed.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-b");
    expect(await resumed.desk.answerFromCard(click(cardB, "ask_opt_2"))).toBe("handled");
    expect(answers(await replayB)).toEqual([
      { id: "deployment-target", questionId: rowB, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id).map(({ id }) => id)).toEqual([...interrupted.questionIds, rowA, rowB]);
    void firstCall;
    void secondCall;
  });
});

test("语义认领把新调用身份写回行：再次重启后同一个 id 仍认回原结果", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-adopted-source" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const adopted = resumeDesk(store);
    adopted.desk.beginRecovery(task.id);
    // 兜底续跑换了工具调用 id：语义认领 A 留下的行。
    const call = adopted.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-adopted");
    expect(await adopted.desk.answerFromCard(click(interrupted.cardMessageId, "ask_opt_2"))).toBe("handled");
    expect(answers(await call)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    // 认领者身份已经落盘。
    expect(payloadOf(store, rowId).toolCallId).toBe("call-adopted");

    // 再一次重启：同一个工具调用 id 的重执行仍然拿回同一份答案。
    const later = resumeDesk(store);
    later.desk.beginRecovery(task.id);
    const replay = later.desk.askQuestions(task, TARGET, args(SINGLE), new AbortController().signal, "call-adopted");
    expect(answers(await replay)).toEqual([
      { id: "deployment-target", questionId: rowId, answer: "2", answeredBy: OWNER },
    ]);
    expect(store.listTaskQuestions(task.id)).toHaveLength(1);
    expect(later.transport.sentCards).toHaveLength(0);
  });
});

test("重启窗口里被答复后 /cancel：不再补投任何新回合", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    const { desk, record } = resumeDesk(store);
    desk.beginRecovery(task.id);
    // 答复在续跑 ask 重挂之前落地（先不另起回合）。
    await desk.answerPendingQuestion(inbound({ messageId: "restart-cancel-window", ...TARGET, text: "2" }), "2");
    expect(record.wakes).toHaveLength(0);
    // 显式取消：该聊天的恢复快照全部作废，已答复的行也不该被补投。
    desk.cancelForTarget(TARGET);
    desk.rejectForTask(task.id, new Error("task cancelled"));
    expect(record.wakes).toHaveLength(0);
    expect(store.getQuestion(rowId)?.state).toBe("answered");
  });
});

/** 一段提问行的 payload，按行读回。 */
function payloadOf(store: ChannelStore, questionId: string): Record<string, unknown> {
  const row = store.getQuestion(questionId);
  if (row === null) throw new Error("question row missing");
  return JSON.parse(row.payload) as Record<string, unknown>;
}

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

test("提问超时把未认领的旧问题交给服务落库：再次恢复后 A 不可答、B 仍可答", async () => {
  const interrupted = await interruptAsk(SINGLE, { toolCallId: "call-old-a" });
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowA = interrupted.questionIds[0]!;
    // 与停机前那段进程共用一条传输：新问题的卡片不会撞旧卡的号。
    const { desk, transport, record } = resumeDesk(store, {
      timeoutMs: 20,
      transport: interrupted.transport,
    });
    desk.beginRecovery(task.id);
    // 新的一轮提问是另一个问题：停机留下的 A 不会被这次调用认领。
    const fresh = [{
      id: "rollout-window",
      question: "Which rollout window?",
      options: [{ label: "tonight" }, { label: "tomorrow" }],
    }];
    await expect(
      desk.askQuestions(task, TARGET, args(fresh), new AbortController().signal, "call-new-b"),
    ).rejects.toThrow("ask wait stopped");
    // 契约：停回合时交出的名单里只有未认领且仍待答的旧行 A；本轮超时的 B 不在其中（它要保持可答）。
    expect(record.stops).toEqual([task.id]);
    expect(record.stopIntents).toEqual([{ taskId: task.id, ids: [rowA] }]);
    const rowB = store.listTaskQuestions(task.id).at(-1)!.id;
    expect(rowB).not.toBe(rowA);
    expect(store.getQuestion(rowB)?.state).toBe("pending");
    // 服务侧：这份 id 与停止意图写进同一事务——真调用 store，不模拟。
    expect(store.markTaskStopIntent(task.id, "question_timeout", record.stopIntents[0]!.ids)).toBe(true);
    // 同一事务的效果：旧行 A 被取消，本轮超时的 B 仍可答。
    expect(store.getQuestion(rowA)?.state).toBe("cancelled");
    expect(store.getQuestion(rowB)?.state).toBe("pending");
    // 再次恢复：A 是取消掉的历史，B 照旧等答复，迟到答复唤醒新回合。
    const again = resumeDesk(store, { transport });
    again.desk.beginRecovery(task.id);
    const cardB = (await transport.waitForCardSend(({ messageId }) => messageId !== interrupted.cardMessageId))
      .messageId;
    expect(await again.desk.answerFromCard(click(interrupted.cardMessageId, "ask_opt_2"))).toBe("handled");
    expect(store.getQuestion(rowA)?.state).toBe("cancelled");
    expect(store.getQuestion(rowA)?.answer).toBeNull();
    expect(again.record.texts.map(({ text }) => text)).toContain("该问题已失效，本次点击未作为答案。");
    expect(await again.desk.answerFromCard(click(cardB, "ask_opt_1"))).toBe("handled");
    expect(store.getQuestion(rowB)?.state).toBe("answered");
    expect(again.record.wakes).toHaveLength(1);
    // 收束投影：桌面在 A 还待答时建过快照、此刻行已被 Store 取消——它的卡片仍要补上关闭。
    desk.rejectForTask(task.id, new Error("task finished"));
    const closed = await transport.waitForCardEdit(({ messageId, frame }) =>
      messageId === interrupted.cardMessageId && JSON.stringify(frame.card).includes("任务已结束"));
    expect(closed.messageId).toBe(interrupted.cardMessageId);
  });
});

test("Store 已按终态取消的提问，卡片仍要被收尾关闭", async () => {
  const interrupted = await interruptAsk(SINGLE);
  await withStore(interrupted, async (store) => {
    const task = latestTask(store);
    const rowId = interrupted.questionIds[0]!;
    // 任务落终态的那段进程把行取消掉，随后被杀：卡片还挂在用户那里。
    store.cancelQuestion(rowId);
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
    const { desk, transport } = resumeDesk(store);
    desk.cancelForTask(task.id);
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
    const edit = await transport.waitForCardEdit(({ messageId }) => messageId === interrupted.cardMessageId);
    expect(JSON.stringify(edit.frame.card)).toContain("任务已中断");
    // 已取消的行不因任务收尾复活：迟到的答复照样无处可落。
    const answered = await desk.answerPendingQuestion(
      inbound({ messageId: "store-cancelled", ...TARGET, text: "2" }),
      "2",
    );
    expect(answered).toBe(false);
    expect(store.getQuestion(rowId)?.state).toBe("cancelled");
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
