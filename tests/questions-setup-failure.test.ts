import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { InboundMessage } from "../src/contracts.ts";
import { createQuestionDesk, type QuestionDeskDeps } from "../src/questions.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { FakeTransport, inbound } from "./fixtures/core-fakes.ts";

const ASK_TIMEOUT_MS = 1_000;
const FAULT = "injected second-question insert fault";

/** Refuses the INSERT of every question after the first within one task, via the shared schema. */
function installSecondQuestionFault(db: Database): void {
  db.exec(`CREATE TRIGGER fail_second_question_insert
    BEFORE INSERT ON questions
    BEGIN
      SELECT RAISE(ABORT, '${FAULT}')
      WHERE (SELECT COUNT(*) FROM questions WHERE task_id = NEW.task_id) >= 1;
    END;`);
}

function dropSecondQuestionFault(db: Database): void {
  db.exec("DROP TRIGGER IF EXISTS fail_second_question_insert");
}

test("a set ask failing on the second question leaves no orphan, no leaked stop timer, and the next ask still completes", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-questions-setup-"));
  const storePath = join(root, "channel.sqlite");
  const store = await openChannelStore(root);
  // 独立的第二条连接只负责装/拆故障；被测的问桌子系统走的仍是那条真实 store 连接。
  const injector = new Database(storePath);
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const target = { channelType: 1 as const, channelId: "setup-failure" };
  const stoppedTurns: string[] = [];
  const operations: Promise<void>[] = [];
  const deps: QuestionDeskDeps = {
    store,
    transport,
    botUid: () => transport.bot.uid,
    stopping: () => false,
    enqueueText: () => {},
    operationSet: (operation) => { operations.push(operation); },
    commandClaim: (message: InboundMessage): InboundClaim => ({
      botUid: transport.bot.uid,
      target: message,
      messageId: message.messageId,
      senderId: message.senderId,
      text: message.text,
      supported: message.supported,
      kind: "command",
    }),
    askTimeoutMs: () => ASK_TIMEOUT_MS,
    displayNames: () => Promise.resolve(new Map<string, string>()),
    stopTurnForQuestion: (taskId) => {
      stoppedTurns.push(taskId);
    },
    wakeTurn: () => {},
    memberAnswerAllowed: () => false,
  };
  const desk = createQuestionDesk(deps);
  const makeTask = (id: string): TaskRecord => ({
    id,
    botUid: transport.bot.uid,
    ...target,
    messageId: `${id}-message`,
    senderId: "owner-1",
    text: "ask",
    sessionId: `${id}-session`,
    cwd: root,
    state: "running",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relayChannelType: null,
    relayChannelId: null,
    relayThreadName: null,
  });
  const args = {
    questions: [
      { id: "q-alpha", question: "第一个问题？" },
      { id: "q-beta", question: "第二个问题？" },
    ],
  };

  installSecondQuestionFault(injector);
  vi.useFakeTimers();
  try {
    const failed = desk.askQuestions(makeTask("task-failed"), target, args, new AbortController().signal);
    // 原始落库错误原样透出，不被吞掉或改写。
    await expect(failed).rejects.toThrow(FAULT);

    // 第一题不得滞留为可回答的孤儿行。
    expect(store.pendingQuestionIds(transport.bot.uid, target)).toEqual([]);

    // 第一题泄漏的计时器不得在世：越过等待超时也不许事后停掉该回合。
    vi.advanceTimersByTime(ASK_TIMEOUT_MS * 2);
    expect(stoppedTurns).toEqual([]);

    // 失败之后聊天里没有可回答的问题：普通回复不再被当作答案吞掉。
    const orphanReply = inbound({ ...target, text: "1" });
    expect(await desk.answerPendingQuestion(orphanReply, orphanReply.text)).toBe(false);

    // 移除注入故障：下一次普通整组提问仍能投递、被答复并完整返回全部答案。
    dropSecondQuestionFault(injector);
    const recovered = desk.askQuestions(makeTask("task-recovered"), target, args, new AbortController().signal);
    const reply = inbound({ ...target, messageId: "answer-1", text: "一起回答" });
    expect(await desk.answerPendingQuestion(reply, reply.text)).toBe(true);
    const raw = await recovered;
    const { answers } = JSON.parse(raw) as {
      answers: Array<{ id: string; questionId: string; answer: string; answeredBy: string | null }>;
    };
    expect(answers.map(({ id }) => id)).toEqual(["q-alpha", "q-beta"]);
    expect(answers.map(({ answer }) => answer)).toEqual(["一起回答", "一起回答"]);
    expect(answers.every(({ answeredBy }) => answeredBy === reply.senderId)).toBe(true);

    // 成功完成也不许残留计时器：越过超时仍不得停任何回合。
    vi.advanceTimersByTime(ASK_TIMEOUT_MS * 2);
    expect(stoppedTurns).toEqual([]);
  } finally {
    desk.stop();
    await Promise.allSettled(operations);
    vi.useRealTimers();
    dropSecondQuestionFault(injector);
    injector.close();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
