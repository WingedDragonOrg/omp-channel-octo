import { randomUUID } from "node:crypto";
import { throwIfAborted } from "./abort.ts";
import {
  askAnswerText,
  buildAskCard,
  buildAskCardSet,
  parseAskActionId,
  resolveAskAnswer,
  resolveAskSetAnswer,
  type AskCardFrame,
  type AskCardOption,
  type AskCardQuestion,
  type AskCardSetQuestion,
  type AskCardSetState,
  type AskCardState,
} from "./ask-card.ts";
import type { BotEventOutcome } from "./bot-events.ts";
import type {
  BotCardAction,
  ChannelTransport,
  ChatTarget,
  InboundMessage,
} from "./contracts.ts";
import { safeErrorMessage } from "./errors.ts";
import type { ChannelStore } from "./store.ts";
import type { InboundClaim, QuestionRecord, TaskRecord } from "./store-records.ts";
import { targetKey } from "./target.ts";

const QUESTION_TIMEOUT_MS = 10 * 60 * 1_000;
/** Card rewrite budget; a card edit never gates the answer, which is durable before it starts. */
const ASK_CARD_EDIT_TIMEOUT_MS = 5_000;
const ASK_CARD_SEND_TIMEOUT_MS = 10_000;
/** Long-poll hold and the pause between rounds that returned immediately or failed. */

type QuestionWaiter = {
  taskId: string;
  resolve: (answer: string) => void;
  reject: (error: Error) => void;
};


/** One validated `ask` question before it owns a store row. */
type AskSpec = {
  /** Row id; the question identity for every later lookup. */
  id: string;
  /** Caller-provided question id, echoed back as `id`; the row id answers `questionId`. */
  sourceId: string | null;
  /** Stored payload: what the card builders, the text path and the click path read back. */
  payload: Record<string, unknown>;
};

/** Parsed question payload; `null` when the stored row cannot be read. */
function parseQuestionPayload(question: QuestionRecord): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(question.payload);
    if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** One question payload as the card understands it: text, options, multi select. */
function askQuestionFromPayload(payload: Record<string, unknown>): AskCardQuestion | null {
  const text = typeof payload.question === "string" ? payload.question.trim() : "";
  if (text.length === 0) return null;
  const raw = Array.isArray(payload.options) ? payload.options : [];
  const options: AskCardOption[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const option = entry as Record<string, unknown>;
    const label = typeof option.label === "string" ? option.label.trim() : "";
    if (label.length === 0) continue;
    const description = typeof option.description === "string" ? option.description.trim() : "";
    options.push(description.length > 0 ? { label, description } : { label });
  }
  return { question: text, options, multi: payload.multi === true };
}

/**
 * The 1-based position a question holds in its multi-question card, when it belongs to one. The
 * per-question input ids of a set frame are numbered by it.
 */
function questionPosition(payload: Record<string, unknown>): { index: number; size: number } | null {
  const index = payload.cardIndex;
  const size = payload.cardSize;
  if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 1) return null;
  if (typeof size !== "number" || !Number.isSafeInteger(size) || size < index) return null;
  return { index, size };
}

/**
 * 问答子系统需要的那部分服务状态。问题行、等待者与卡片状态都留在本模块内，服务只经
 * `QuestionDesk` 使用它；出站文字与后台操作的跟踪由服务注入，卡片编辑因此仍受停机约束。
 */
export interface QuestionDeskDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  botUid(): string | undefined;
  stopping(): boolean;
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  /** 后台卡片编辑等操作交给服务统一跟踪，停机时等待它们结束。 */
  operationSet(operation: Promise<void>): void;
  /** 与文本回复路径共用的持久 claim，保证一条消息只作一次答案。 */
  commandClaim(message: InboundMessage, name: string): InboundClaim;
}

export interface QuestionDesk {
  /** 待回答问题的一条普通回复即答案；没有待回答问题返回 false。 */
  answerPendingQuestion(message: InboundMessage, text: string): Promise<boolean>;
  /** `ask` host tool：提问并等到答案（含卡片、文本回退与超时）。 */
  askQuestions(
    task: TaskRecord,
    target: ChatTarget,
    args: Record<string, unknown>,
    signal: AbortSignal,
  ): Promise<string>;
  /** Bot 事件队列的一次卡片点击；`retry` 表示保留游标重读。 */
  answerFromCard(action: BotCardAction): Promise<BotEventOutcome>;
  /** 任务结束或失败：释放该任务的全部等待者。 */
  rejectForTask(taskId: string, error: Error): void;
  /** 取消一个聊天的问题并封闭其卡片（`/cancel`）。 */
  cancelForTarget(target: ChatTarget): void;
  /** 服务停机：清掉问题计时器。 */
  stop(): void;
}

export function createQuestionDesk(deps: QuestionDeskDeps): QuestionDesk {
  const questionWaiters = new Map<string, QuestionWaiter>();
  const questionTimers = new Map<string, NodeJS.Timeout>();
  /** 每张问题卡的帧序号，服务器要求严格递增。 */
  const askCardSeq = new Map<string, number>();
  /** 每张卡片的 action-id 代次：必须保持可回答的帧据此换新 id。 */
  const askCardGeneration = new Map<string, number>();

  function currentBotUid(): string {
    const uid = deps.botUid();
    if (uid === undefined) throw new Error("Bot 身份未注册");
    return uid;
  }

/**
 * A plain reply in the chat holding the pending question is the answer itself: a valid
 * option index selects that option, any other text is returned verbatim as the free-form
 * answer. Returns false when nothing is pending, so the text stays ordinary task input.
 */
async function answerPendingQuestion(message: InboundMessage, text: string): Promise<boolean> {
  if (text.length === 0) return false;
  const pending = deps.store.listPendingQuestions(currentBotUid(), message);
  const question = pending[0];
  if (!question) return false;
  const claim = deps.commandClaim(message, "answer");
  if (!deps.store.claimCommand(claim)) return true;
  try {
    if (pending.length > 1) {
      answerQuestionSetFromText(message, pending, text);
      deps.store.completeCommand(claim);
      return true;
    }
    let normalized: string;
    try {
      normalized = normalizeAnswer(question, text);
    } catch {
      await deps.enqueueText(message, "答案无效：请回复选项编号或直接输入答案。", null);
      deps.store.completeCommand(claim);
      return true;
    }
    if (!applyQuestionAnswer(question, normalized)) {
      await deps.enqueueText(message, "问题已失效，本次回复未作为答案记录。", null);
      deps.store.completeCommand(claim);
      return true;
    }
    deps.store.completeCommand(claim);
    return true;
  } catch {
    deps.store.failCommand(claim);
    throw new Error("本地状态盘写入失败，未能记录答案。");
  }
}

/** Groups question rows by the card that carried them; a row without a card carries none. */
function questionsByCard(rows: readonly QuestionRecord[]): Map<string, QuestionRecord[]> {
  const cards = new Map<string, QuestionRecord[]>();
  for (const question of rows) {
    const messageId = question.cardMessageId;
    if (messageId === null) continue;
    const existing = cards.get(messageId);
    if (existing === undefined) cards.set(messageId, [question]);
    else existing.push(question);
  }
  return cards;
}

/**
 * One text reply answers a whole multi-question card: it becomes the answer of every question
 * still open — each interpreting it under its own rule — and the ask returns at once instead of
 * waiting for the remaining questions one by one. The answers land as one compare-and-set per
 * row inside one transaction, and each card involved is rewritten once.
 */
function answerQuestionSetFromText(
  message: InboundMessage,
  pending: readonly QuestionRecord[],
  text: string,
): void {
  const botUid = currentBotUid();
  const answers = pending.map((question) => {
    let answer = text;
    try {
      answer = normalizeAnswer(question, text);
    } catch {
      // A numeric reply that does not name this question's options is still an answer as typed.
    }
    return { id: question.id, answer };
  });
  const landed = deps.store.answerQuestions(botUid, message, answers);
  const byId = new Map(answers.map(({ id, answer }) => [id, answer]));
  for (const id of landed) releaseQuestion(id, byId.get(id) ?? text);
  for (const [messageId, rows] of questionsByCard(pending)) {
    forgetAskGeneration(messageId);
    const first = rows[0];
    if (first === undefined) continue;
    if (rows.length === 1) {
      rewriteAskCard(first, { kind: "answered", answer: byId.get(first.id) ?? text });
    } else {
      const fresh = deps.store.listQuestionsByCard(botUid, message, messageId);
      rewriteAskCardSet(fresh, { kind: "closed", note: null }, null);
    }
  }
}

function normalizeAnswer(question: { payload: string }, answer: string): string {
  const payload = JSON.parse(question.payload) as Record<string, unknown>;
  const options = payload.options;
  if (!Array.isArray(options) || options.length === 0) return answer;
  const numeric = /^\d+(?:\s*,\s*\d+)*$/u.test(answer);
  if (!numeric) return answer;
  const values = answer.split(",").map((value) => Number(value.trim()));
  if (payload.multi !== true && values.length !== 1) throw new Error("multiple options are not allowed");
  if (new Set(values).size !== values.length || values.some((value) => value < 1 || value > options.length)) {
    throw new Error("option is out of range");
  }
  return values.join(",");
}

async function askQuestions(
  task: TaskRecord,
  target: ChatTarget,
  args: Record<string, unknown>,
  signal: AbortSignal,
): Promise<string> {
  if (!Array.isArray(args.questions) || args.questions.length === 0) {
    throw new Error("questions must be a non-empty array");
  }
  const questions = args.questions as unknown[];
  const specs = questions.map((value, index) =>
    askSpec(task, value, questions.length > 1 ? index + 1 : null, questions.length));
  const answers = specs.length === 1
    ? [await askSingleQuestion(task, target, specs[0]!, signal)]
    : await askQuestionSet(task, target, specs, signal);
  return JSON.stringify({ answers });
}


/**
 * Validates one `ask` question and mints its row. A set question records its 1-based position
 * so every later frame keeps the same per-question input ids.
 */
function askSpec(task: TaskRecord, value: unknown, cardIndex: number | null, cardSize: number): AskSpec {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid question");
  }
  const question = value as Record<string, unknown>;
  if (typeof question.question !== "string" || question.question.trim().length === 0) {
    throw new Error("question text is required");
  }
  if (
    question.id !== undefined &&
    (typeof question.id !== "string" || question.id.trim().length === 0)
  ) {
    throw new Error("question id must be a non-empty string");
  }
  if (question.multi !== undefined && typeof question.multi !== "boolean") {
    throw new Error("question multi must be boolean");
  }
  if (question.options !== undefined) {
    if (!Array.isArray(question.options) || question.options.length === 0) {
      throw new Error("question options must be non-empty");
    }
    for (const option of question.options) {
      if (!option || typeof option !== "object" || Array.isArray(option)) {
        throw new Error("invalid question option");
      }
      const optionRecord = option as Record<string, unknown>;
      if (typeof optionRecord.label !== "string" || optionRecord.label.trim().length === 0) {
        throw new Error("question option label is required");
      }
    }
  }
  const id = `${task.id}:${randomUUID()}`;
  const sourceId = typeof question.id === "string" ? question.id : null;
  const payload: Record<string, unknown> = {
    id,
    sourceId,
    question: question.question,
    options: question.options,
    multi: question.multi === true,
    recommended: question.recommended,
  };
  if (cardIndex !== null) {
    payload.cardIndex = cardIndex;
    payload.cardSize = cardSize;
  }
  return { id, sourceId, payload };
}

/**
 * Asks one question on a card of its own — or in plain text when the deployment carries no
 * interactive frames — and resolves with the recorded answer.
 */
async function askSingleQuestion(
  task: TaskRecord,
  target: ChatTarget,
  spec: AskSpec,
  signal: AbortSignal,
): Promise<{ id: string; questionId: string; answer: string }> {
  const { id, sourceId, payload } = spec;
  deps.store.createQuestion({
    id,
    taskId: task.id,
    botUid: currentBotUid(),
    ...target,
    payload: JSON.stringify(payload),
  });
  const completion = Promise.withResolvers<string>();
  void completion.promise.catch(() => {});
  questionWaiters.set(id, {
    taskId: task.id,
    resolve: completion.resolve,
    reject: completion.reject,
  });
  const abort = () => {
    deps.store.cancelQuestion(id);
    questionWaiters.delete(id);
    completion.reject(new Error("question cancelled"));
  };
  signal.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(() => {
    if (!deps.store.expireQuestion(id)) return;
    questionTimers.delete(id);
    questionWaiters.delete(id);
    const expired = deps.store.getQuestion(id);
    if (expired !== null) {
      rewriteAskCard(expired, { kind: "inactive", note: "问题已过期：未在 10 分钟内收到答案。" });
      forgetAskGeneration(expired.cardMessageId);
    }
    completion.reject(new Error("question timed out"));
  }, QUESTION_TIMEOUT_MS);
  questionTimers.set(id, timer);
  try {
    throwIfAborted(signal);
    await deliverQuestion(payload, target, id, task.id, signal);
    throwIfAborted(signal);
    const answer = await completion.promise;
    return { id: sourceId ?? id, questionId: id, answer };
  } finally {
    signal.removeEventListener("abort", abort);
    clearTimeout(timer);
    questionTimers.delete(id);
    questionWaiters.delete(id);
    forgetAskGeneration(deps.store.getQuestion(id)?.cardMessageId ?? null);
    deps.store.cancelQuestion(id);
  }
}

/**
 * Asks a whole set on one card: every question is created and waits at once, one submission
 * answers the questions it filled, and the result is produced once every question is settled.
 * A deployment that cannot carry the set frame — no interactive profile, a frame its limits
 * refuse, a failed send — falls back to asking the questions one at a time, so no answer is
 * ever lost to a card that could not be rendered.
 */
async function askQuestionSet(
  task: TaskRecord,
  target: ChatTarget,
  specs: readonly AskSpec[],
  signal: AbortSignal,
): Promise<Array<{ id: string; questionId: string; answer: string }>> {
  const completions = specs.map(() => Promise.withResolvers<string>());
  const cleanups: Array<() => void> = [];
  specs.forEach((spec, index) => {
    deps.store.createQuestion({
      id: spec.id,
      taskId: task.id,
      botUid: currentBotUid(),
      ...target,
      payload: JSON.stringify(spec.payload),
    });
    const completion = completions[index]!;
    void completion.promise.catch(() => {});
    questionWaiters.set(spec.id, {
      taskId: task.id,
      resolve: completion.resolve,
      reject: completion.reject,
    });
    const abort = () => {
      deps.store.cancelQuestion(spec.id);
      questionWaiters.delete(spec.id);
      completion.reject(new Error("question cancelled"));
    };
    signal.addEventListener("abort", abort, { once: true });
    cleanups.push(() => signal.removeEventListener("abort", abort));
    const timer = setTimeout(() => {
      if (!deps.store.expireQuestion(spec.id)) return;
      questionTimers.delete(spec.id);
      questionWaiters.delete(spec.id);
      const expired = deps.store.getQuestion(spec.id);
      if (expired !== null) {
        closeQuestionCard(expired, "问题已过期：未在 10 分钟内收到全部答案。");
      }
      completion.reject(new Error("question timed out"));
    }, QUESTION_TIMEOUT_MS);
    questionTimers.set(spec.id, timer);
  });
  try {
    throwIfAborted(signal);
    const messageId = await deliverQuestionSet(specs, target, signal);
    throwIfAborted(signal);
    if (messageId !== null) {
      deps.store.setQuestionsCard(specs.map(({ id }) => id), messageId);
      const settled = await Promise.all(completions.map(({ promise }) => promise));
      return specs.map(({ id, sourceId }, index) => ({
        id: sourceId ?? id,
        questionId: id,
        answer: settled[index] ?? "",
      }));
    }
    // The set frame was refused: ask each question exactly like a single-question ask would. A
    // text reply can settle the remaining rows before their turn, so only still-open ones are
    // delivered and every resolved completion is read back in question order.
    const answers: Array<{ id: string; questionId: string; answer: string }> = [];
    for (const [index, spec] of specs.entries()) {
      if (deps.store.getQuestion(spec.id)?.state === "pending") {
        await deliverQuestion(spec.payload, target, spec.id, task.id, signal);
      }
      const answer = await completions[index]!.promise;
      answers.push({ id: spec.sourceId ?? spec.id, questionId: spec.id, answer });
    }
    return answers;
  } finally {
    for (const spec of specs) {
      clearTimeout(questionTimers.get(spec.id));
      questionTimers.delete(spec.id);
      questionWaiters.delete(spec.id);
      forgetAskGeneration(deps.store.getQuestion(spec.id)?.cardMessageId ?? null);
      deps.store.cancelQuestion(spec.id);
    }
    for (const cleanup of cleanups) cleanup();
  }
}

/**
 * Sends the one card carrying a whole set. `null` means the deployment could not take it — no
 * interactive profile, a frame its limits refuse, or a failed send — and the caller asks the
 * questions one at a time instead.
 */
async function deliverQuestionSet(
  specs: readonly AskSpec[],
  target: ChatTarget,
  signal: AbortSignal,
): Promise<string | null> {
  const entries: AskCardSetQuestion[] = [];
  const payloads: Record<string, unknown>[] = [];
  for (const spec of specs) {
    const ask = askQuestionFromPayload(spec.payload);
    if (ask === null) return null;
    entries.push({ question: ask, answer: null });
    payloads.push(spec.payload);
  }
  const frame = buildAskCardSet(entries, { kind: "open", generation: 1 }, formatQuestionSet(payloads));
  try {
    const receipt = await deps.transport.sendCard(
      target,
      frame,
      randomUUID(),
      AbortSignal.any([signal, AbortSignal.timeout(ASK_CARD_SEND_TIMEOUT_MS)]),
    );
    return receipt?.messageId ?? null;
  } catch (error) {
    if (signal.aborted) throw error;
    console.error(
      `[ask-card] set send failed target=${targetKey(target)}：${safeErrorMessage(error, "send failed")}`,
    );
    return null;
  }
}

function formatQuestion(question: Record<string, unknown>): string {
  const entries = Array.isArray(question.options) ? question.options : [];
  // A question of a set is answered by the same rule as the whole set, even when its card was
  // refused and it ends up asked on its own: one text reply settles every open question.
  const rule = questionPosition(question) !== null
    ? "直接回复本消息即作为整组答案（一次回答全部问题）。"
    : entries.length === 0
      ? "直接回复即可，内容会作为答案返回。"
      : question.multi === true
        ? "直接回复编号（如 1,3）作为答案，或自填答案。"
        : "直接回复选项编号作为答案，或自填答案。";
  return `${questionPrompt(question)}${rule}`;
}

/**
 * The plain-text fallback of a set card: one block per question, then how a text reply counts
 * for the whole set. A reply is the answer of every open question at once.
 */
function formatQuestionSet(payloads: readonly Record<string, unknown>[]): string {
  const blocks = payloads.map((payload) => questionPrompt(payload));
  return `${blocks.join("\n")}直接回复本消息即作为整组答案（一次回答全部问题）；也可以点击卡片一次提交。`;
}

/** One question block: its heading, numbered options and, with them, their descriptions. */
function questionPrompt(question: Record<string, unknown>): string {
  const entries = Array.isArray(question.options) ? question.options : [];
  const list = entries
    .map((option, index) => {
      if (!option || typeof option !== "object" || Array.isArray(option)) return `${index + 1}.`;
      const value = option as Record<string, unknown>;
      const label = typeof value.label === "string" ? value.label : "";
      const description = typeof value.description === "string" ? ` — ${value.description}` : "";
      return `${index + 1}. ${label}${description}`;
    })
    .join("\n");
  const position = questionPosition(question);
  const heading = position === null ? "问题：" : `问题 ${position.index}/${position.size}：`;
  return `${heading}${question.question as string}\n${list}${list ? "\n" : ""}`;
}

function rejectForTask(taskId: string, error: Error): void {
  for (const [id, waiter] of questionWaiters) {
    if (waiter.taskId !== taskId) continue;
    questionWaiters.delete(id);
    clearTimeout(questionTimers.get(id));
    questionTimers.delete(id);
    forgetAskGeneration(deps.store.getQuestion(id)?.cardMessageId ?? null);
    waiter.reject(error);
  }
}

/**
 * Posts one question: the interactive card when the deployment carries interactive frames,
 * otherwise the plain text the channel has always sent. Both carry the same question text, so a
 * client that cannot render the card shows the question instead of nothing.
 */
async function deliverQuestion(
  payload: Record<string, unknown>,
  target: ChatTarget,
  questionId: string,
  taskId: string,
  signal: AbortSignal,
): Promise<void> {
  const ask = askQuestionFromPayload(payload);
  if (ask !== null) {
    try {
      const receipt = await deps.transport.sendCard(
        target,
        buildAskCard(ask, { kind: "pending", generation: 1 }, formatQuestion(payload)),
        randomUUID(),
        AbortSignal.any([signal, AbortSignal.timeout(ASK_CARD_SEND_TIMEOUT_MS)]),
      );
      if (receipt?.messageId !== undefined) {
        deps.store.setQuestionCard(questionId, receipt.messageId);
        return;
      }
    } catch (error) {
      if (signal.aborted) throw error;
      console.error(`[ask-card] send failed target=${targetKey(target)}：${safeErrorMessage(error, "send failed")}`);
    }
  }
  deps.enqueueText(target, formatQuestion(payload), taskId);
}

/**
 * Records one answer durably and releases the waiting task. The compare-and-set on the question
 * row is the single arbitration point between a text reply and a card click: whoever loses it
 * must not answer a second time.
 */
function applyQuestionAnswer(question: QuestionRecord, answer: string): boolean {
  const botUid = deps.botUid();
  if (!botUid) return false;
  if (!deps.store.answerQuestion(question.id, botUid, question, answer)) return false;
  releaseQuestion(question.id, answer);
  forgetAskGeneration(question.cardMessageId);
  rewriteAskCard(question, { kind: "answered", answer });
  return true;
}

/** Releases the waiting task of one recorded answer; the store CAS already decided it landed. */
function releaseQuestion(questionId: string, answer: string): void {
  const waiter = questionWaiters.get(questionId);
  if (!waiter) return;
  questionWaiters.delete(questionId);
  clearTimeout(questionTimers.get(questionId));
  questionTimers.delete(questionId);
  waiter.resolve(answer);
}

/**
 * Rewrites one question card. Best-effort by contract: the state it renders is already durable
 * in the question row, so a failed edit changes nothing but the card's appearance.
 */
function rewriteAskCard(question: QuestionRecord, state: AskCardState, hint?: string): void {
  const messageId = question.cardMessageId;
  if (messageId === null) return;
  const payload = parseQuestionPayload(question);
  const ask = payload === null ? null : askQuestionFromPayload(payload);
  if (ask === null) return;
  const note = state.kind === "answered"
    ? `已回复：${askAnswerText(ask, state.answer)}`
    : state.kind === "inactive"
      ? state.note
      : hint;
  const base = formatQuestion(payload ?? {});
  const frame = buildAskCard(ask, state, note === undefined ? base : `${base}\n${note}`);
  submitAskCardEdit(question, messageId, frame);
}

/**
 * Rewrites the shared card of a multi-question ask: answered questions show their answer, the
 * rest keep their inputs while the frame is open, and a closed frame carries none. Best-effort
 * like every card rewrite; `hint` is appended to the fallback text of this edit only.
 */
function rewriteAskCardSet(
  rows: readonly QuestionRecord[],
  state: AskCardSetState,
  hint: string | null,
): void {
  const first = rows[0];
  const messageId = first?.cardMessageId ?? null;
  if (first === undefined || messageId === null) return;
  const entries: AskCardSetQuestion[] = [];
  const payloads: Record<string, unknown>[] = [];
  for (const row of rows) {
    const payload = parseQuestionPayload(row);
    const ask = payload === null ? null : askQuestionFromPayload(payload);
    if (payload === null || ask === null) return;
    entries.push({ question: ask, answer: row.state === "answered" ? row.answer ?? "" : null });
    payloads.push(payload);
  }
  const base = formatQuestionSet(payloads);
  const note = hint ?? (state.kind === "closed" ? state.note : null);
  const frame = buildAskCardSet(entries, state, note === null ? base : `${base}\n${note}`);
  submitAskCardEdit(first, messageId, frame);
}

/** Sends one card edit with the next sequence; failures are logged and never gate an answer. */
function submitAskCardEdit(target: ChatTarget, messageId: string, frame: AskCardFrame): void {
  const seq = (askCardSeq.get(messageId) ?? 1) + 1;
  askCardSeq.set(messageId, seq);
  const operation = deps.transport
    .editCard(target, messageId, frame, seq, AbortSignal.timeout(ASK_CARD_EDIT_TIMEOUT_MS))
    .catch((error: unknown) => {
      console.error(
        `[ask-card] edit failed target=${targetKey(target)}：${safeErrorMessage(error, "edit failed")}`,
      );
    });
  deps.operationSet(operation);
}

/**
 * A click that must not answer leaves the question open. The server accepts one click per
 * `(message, action, operator)`, so answering it later needs a frame whose action ids differ —
 * this is where the next generation comes from.
 */
function retryAskCard(question: QuestionRecord, generation: number, hint: string): void {
  const messageId = question.cardMessageId;
  if (messageId !== null) {
    rewriteAskCard(question, { kind: "pending", generation: nextAskGeneration(messageId, generation) }, hint);
  }
  deps.enqueueText(question, hint, null);
}

/** The set-frame twin of `retryAskCard`: one fresh generation for the whole card, one hint. */
function retryAskCardSet(rows: readonly QuestionRecord[], generation: number, hint: string): void {
  const first = rows[0];
  const messageId = first?.cardMessageId ?? null;
  if (first === undefined || messageId === null) return;
  rewriteAskCardSet(rows, { kind: "open", generation: nextAskGeneration(messageId, generation) }, hint);
  deps.enqueueText(first, hint, null);
}

/**
 * The generation the next frame of this card must carry: strictly above the click that just
 * arrived and every id the card issued before, because a consumed id is never accepted again.
 */
function nextAskGeneration(messageId: string, seen: number): number {
  const next = Math.max(seen, askCardGeneration.get(messageId) ?? 0) + 1;
  askCardGeneration.set(messageId, next);
  return next;
}

/** Drops the action-id generation of a card that reached its end. */
function forgetAskGeneration(cardMessageId: string | null): void {
  if (cardMessageId !== null) askCardGeneration.delete(cardMessageId);
}

/**
 * Closes the card one question belongs to: a set card gets a single rewrite covering every row
 * it carries, a single card its own closed frame, and a question that was asked in plain text
 * has no card to rewrite at all.
 */
function closeQuestionCard(question: QuestionRecord, note: string): void {
  const botUid = deps.botUid();
  const messageId = question.cardMessageId;
  if (botUid === undefined || messageId === null) return;
  forgetAskGeneration(messageId);
  const rows = deps.store.listQuestionsByCard(botUid, question, messageId);
  if (rows.length <= 1) {
    rewriteAskCard(question, { kind: "inactive", note });
    return;
  }
  rewriteAskCardSet(rows, { kind: "closed", note }, null);
}

/**
 * Applies one card click. Only the owner's click can answer; a foreign operator, an unknown
 * action id or a card whose question already closed is acknowledged and dropped so the event
 * queue keeps moving.
 */
async function answerFromCard(action: BotCardAction): Promise<BotEventOutcome> {
  const botUid = deps.botUid();
  if (!botUid || deps.stopping()) return "retry";
  const ownerId = deps.store.getOwnerId(botUid)?.trim() ?? "";
  if (ownerId.length === 0 || action.operatorUid !== ownerId) return "ignored";
  const target: ChatTarget = { channelType: action.channelType, channelId: action.channelId };
  let rows: QuestionRecord[];
  try {
    rows = deps.store.listQuestionsByCard(botUid, target, action.messageId);
  } catch {
    return "retry";
  }
  const first = rows[0];
  if (first === undefined) return "ignored";
  if (rows.length === 1) return answerSingleCardClick(first, action);
  return answerSetCardClick(target, rows, action);
}

/**
 * Applies one click on a single-question card. An answered question can only be reached by a
 * replay of the click that answered it (the rewrite removed its buttons), so it stays silent; a
 * question that never got an answer is worth telling the operator about.
 */
function answerSingleCardClick(question: QuestionRecord, action: BotCardAction): BotEventOutcome {
  if (question.state === "answered") return "handled";
  if (question.state !== "pending") {
    deps.enqueueText(question, "该问题已失效，本次点击未作为答案。", null);
    return "handled";
  }
  const ref = parseAskActionId(action.actionId);
  const payload = parseQuestionPayload(question);
  const ask = payload === null ? null : askQuestionFromPayload(payload);
  if (ref === null || ask === null) return "handled";
  const resolved = resolveAskAnswer(ask, ref, action.inputs);
  if (resolved.kind === "invalid") {
    retryAskCard(question, ref.generation, "未收到内容：请填写后重新提交，或直接回复本消息作答。");
    return "handled";
  }
  let answer: string;
  try {
    answer = normalizeAnswer(question, resolved.answer);
  } catch {
    retryAskCard(question, ref.generation, "答案无效：请重新选择，或直接回复本消息作答。");
    return "handled";
  }
  try {
    applyQuestionAnswer(question, answer);
  } catch {
    return "retry";
  }
  return "handled";
}

/**
 * Applies one submission of a multi-question card: every question the submission filled is
 * compare-and-set in a single transaction, the questions it left empty stay open, and the card
 * is rewritten once — closed when nothing waits anymore, otherwise re-rendered behind fresh
 * action ids. A replay of an already consumed submission changes nothing.
 */
function answerSetCardClick(
  target: ChatTarget,
  rows: readonly QuestionRecord[],
  action: BotCardAction,
): BotEventOutcome {
  const pending = rows.filter(({ state }) => state === "pending");
  if (pending.length === 0) {
    if (!rows.every(({ state }) => state === "answered")) {
      deps.enqueueText(target, "该问题已失效，本次点击未作为答案。", null);
    }
    return "handled";
  }
  const ref = parseAskActionId(action.actionId);
  if (ref === null || ref.kind !== "submit") return "handled";
  const filled: Array<{ id: string; answer: string }> = [];
  for (const question of pending) {
    const payload = parseQuestionPayload(question);
    const ask = payload === null ? null : askQuestionFromPayload(payload);
    const position = payload === null ? null : questionPosition(payload);
    if (ask === null || position === null) continue;
    const answer = resolveAskSetAnswer(ask, position.index, action.inputs);
    if (answer !== null) filled.push({ id: question.id, answer });
  }
  if (filled.length === 0) {
    retryAskCardSet(rows, ref.generation, "未收到内容：请至少填写一题后重新提交，或直接回复本消息作答。");
    return "handled";
  }
  let landed: string[];
  try {
    landed = deps.store.answerQuestions(currentBotUid(), target, filled);
  } catch {
    return "retry";
  }
  const byId = new Map(filled.map(({ id, answer }) => [id, answer]));
  for (const id of landed) releaseQuestion(id, byId.get(id) ?? "");
  const fresh = deps.store.listQuestionsByCard(currentBotUid(), target, action.messageId);
  if (fresh.some(({ state }) => state === "pending")) {
    rewriteAskCardSet(
      fresh,
      { kind: "open", generation: nextAskGeneration(action.messageId, ref.generation) },
      null,
    );
  } else {
    forgetAskGeneration(action.messageId);
    rewriteAskCardSet(fresh, { kind: "closed", note: null }, null);
  }
  return "handled";
}

  function cancelForTarget(target: ChatTarget): void {
    const botUid = deps.botUid();
    if (botUid === undefined) return;
    const openQuestions = deps.store.listPendingQuestions(botUid, target);
    deps.store.cancelQuestions(botUid, target);
    for (const [messageId, rows] of questionsByCard(openQuestions)) {
      forgetAskGeneration(messageId);
      const first = rows[0];
      if (first === undefined) continue;
      if (rows.length === 1) {
        rewriteAskCard(first, { kind: "inactive", note: "任务已取消：该问题不再接受回答。" });
      } else {
        rewriteAskCardSet(rows, { kind: "closed", note: "任务已取消：该问题不再接受回答。" }, null);
      }
    }
  }

  function stop(): void {
    for (const timer of questionTimers.values()) clearTimeout(timer);
    questionTimers.clear();
  }

  return {
    answerPendingQuestion,
    askQuestions,
    answerFromCard,
    rejectForTask,
    cancelForTarget,
    stop,
  };
}
