/**
 * Interactive `ask` cards: a question — or a whole set of them — rendered as an octo/v2 frame so
 * the owner answers from the card, or by replying in the chat.
 *
 * A single question keeps its own card: one option per button for a single choice, a selectable
 * list plus one submit for a multiple choice. Several questions share one card — every question
 * carries its own inputs and the whole set is answered by one submit. Every option question also
 * carries a free-form field whose value wins over the selection when it is filled.
 *
 * The frame is an ordinary message as well — `plain` is what the text-only path would have sent
 * — and a plain reply in the chat still answers (see `CoreService.answerPendingQuestion`).
 * Nothing here performs I/O: the builders and the action decoders are pure so both callers stay
 * testable.
 */

export const ASK_CARD_PROFILE = "octo/v2";
export const ASK_TEXT_INPUT_ID = "ask_text";
export const ASK_CHOICES_INPUT_ID = "ask_choices";

const OPTION_ACTION_PREFIX = "ask_opt_";
const SUBMIT_ACTION_PREFIX = "ask_submit";
/** Buttons are titles in a row; the full label stays in the card body. */
const MAX_ACTION_TITLE_CHARS = 60;
const OPTION_ACTION_ID = new RegExp(`^${OPTION_ACTION_PREFIX}(\\d+)(?:#(\\d+))?$`, "u");
const SUBMIT_ACTION_ID = new RegExp(`^${SUBMIT_ACTION_PREFIX}(?:#(\\d+))?$`, "u");
/** One set runs one submit; its title says the click answers every question at once. */
const SET_SUBMIT_TITLE = "提交全部答案";
const OTHER_INPUT_LABEL = "其他（选填，填写后以它为准）";
const OTHER_INPUT_PLACEHOLDER = "自定义答案";
const ANSWER_INPUT_LABEL = "自填答案";
const ANSWER_INPUT_PLACEHOLDER = "输入答案";

export interface AskCardOption {
  label: string;
  description?: string;
}

export interface AskCardQuestion {
  question: string;
  options: readonly AskCardOption[];
  multi: boolean;
}

/**
 * `generation` versions the action ids. The server accepts a given
 * `(message_id, action_id, operator_uid)` click exactly once, so a click that must not consume
 * the question (an empty free-form submit) is followed by a re-rendered frame whose ids differ.
 */
export type AskCardState =
  | { kind: "pending"; generation: number }
  | { kind: "answered"; answer: string }
  | { kind: "inactive"; note: string };

/** One question of a set frame together with the answer recorded for it so far. */
export interface AskCardSetQuestion {
  question: AskCardQuestion;
  /** Recorded answer; `null` while the question still waits for one. */
  answer: string | null;
}

/**
 * A set frame is open while at least one question has no answer: those questions carry inputs
 * and the card carries one submit whose id versions the generation. A closed frame renders no
 * inputs and no actions; `note` explains why it closed when that is worth saying.
 */
export type AskCardSetState =
  | { kind: "open"; generation: number }
  | { kind: "closed"; note: string | null };

export interface AskCardFrame {
  card: Record<string, unknown>;
  plain: string;
  profile: "octo/v2";
}

export type AskActionRef =
  | { kind: "option"; index: number; generation: number }
  | { kind: "submit"; generation: number };

export type AskAnswer = { kind: "answer"; answer: string } | { kind: "invalid" };

/** Escapes the Markdown openers a TextBlock would otherwise interpret as links. */
function cardText(text: string): string {
  return text.replace(/\\/gu, "\\\\").replace(/\[/gu, "\\[");
}

function actionTitle(index: number, label: string): string {
  const prefix = `${index}. `;
  const trimmed = label.trim();
  const room = MAX_ACTION_TITLE_CHARS - prefix.length;
  const title = trimmed.length > room ? `${trimmed.slice(0, Math.max(1, room - 1))}…` : trimmed;
  return `${prefix}${title}`;
}

function optionActionId(index: number, generation: number): string {
  const base = `${OPTION_ACTION_PREFIX}${index}`;
  return generation > 1 ? `${base}#${generation}` : base;
}

function submitActionId(generation: number): string {
  return generation > 1 ? `${SUBMIT_ACTION_PREFIX}#${generation}` : SUBMIT_ACTION_PREFIX;
}

/** Parses the action id of a click; `null` for ids this module never issued. */
export function parseAskActionId(actionId: string): AskActionRef | null {
  const option = OPTION_ACTION_ID.exec(actionId);
  if (option) {
    const index = Number(option[1]);
    const generation = option[2] === undefined ? 1 : Number(option[2]);
    if (!Number.isSafeInteger(index) || index < 1) return null;
    if (!Number.isSafeInteger(generation) || generation < 1) return null;
    return { kind: "option", index, generation };
  }
  const submit = SUBMIT_ACTION_ID.exec(actionId);
  if (submit) {
    const generation = submit[1] === undefined ? 1 : Number(submit[1]);
    if (!Number.isSafeInteger(generation) || generation < 1) return null;
    return { kind: "submit", generation };
  }
  return null;
}

/** Renders a recorded answer back into the frame: option indexes become their labels. */
export function askAnswerText(question: AskCardQuestion, answer: string): string {
  if (question.options.length === 0) return answer;
  const indexes = answer.split(",").map((value) => value.trim());
  if (!indexes.every((value) => /^\d+$/u.test(value))) return answer;
  const labels = indexes.map((value) => {
    const index = Number(value);
    const option = question.options[index - 1];
    return option ? `${index}. ${option.label.trim()}` : value;
  });
  return labels.join("；");
}

/** Reads a comma-separated option selection; `null` when it does not name options of `question`. */
function selectionAnswer(question: AskCardQuestion, raw: string): string | null {
  const parts = raw.split(",").map((part) => part.trim()).filter((part) => part.length > 0);
  if (parts.length === 0) return null;
  if (parts.length > 1 && !question.multi) return null;
  for (const part of parts) {
    if (!/^\d+$/u.test(part)) return null;
    const index = Number(part);
    if (index < 1 || index > question.options.length) return null;
  }
  return [...new Set(parts)].join(",");
}

/**
 * Decodes one click into the answer the question store records. Option buttons answer with their
 * position; a submit reads the free-form text — which wins over the selection when filled — and
 * otherwise the selected indexes (`Input.ChoiceSet`, comma separated on the wire). An empty
 * submission is `invalid`: the question stays open and the caller re-renders with new action ids.
 */
export function resolveAskAnswer(
  question: AskCardQuestion,
  ref: AskActionRef,
  inputs: Readonly<Record<string, string>>,
): AskAnswer {
  if (ref.kind === "option") {
    if (ref.index < 1 || ref.index > question.options.length) return { kind: "invalid" };
    return { kind: "answer", answer: String(ref.index) };
  }
  const text = (inputs[ASK_TEXT_INPUT_ID] ?? "").trim();
  if (question.options.length === 0) {
    return text.length === 0 ? { kind: "invalid" } : { kind: "answer", answer: text };
  }
  if (text.length > 0) return { kind: "answer", answer: text };
  if (!question.multi) return { kind: "invalid" };
  const selected = selectionAnswer(question, inputs[ASK_CHOICES_INPUT_ID] ?? "");
  return selected === null ? { kind: "invalid" } : { kind: "answer", answer: selected };
}

/**
 * Decodes one question of a set frame by its 1-based ordinal. A filled free-form field answers
 * as typed; otherwise the selection answers with the option ordinal(s) it names. `null` means
 * this submission left the question empty — it stays open and the card offers fresh inputs.
 */
export function resolveAskSetAnswer(
  question: AskCardQuestion,
  ordinal: number,
  inputs: Readonly<Record<string, string>>,
): string | null {
  const text = (inputs[`${ASK_TEXT_INPUT_ID}_${ordinal}`] ?? "").trim();
  if (text.length > 0) return text;
  if (question.options.length === 0) return null;
  return selectionAnswer(question, inputs[`${ASK_CHOICES_INPUT_ID}_${ordinal}`] ?? "");
}

/** The numbered option list every frame renders: full labels plus optional descriptions. */
function optionLines(question: AskCardQuestion): Record<string, unknown>[] {
  const body: Record<string, unknown>[] = [];
  question.options.forEach((option, index) => {
    body.push({
      type: "TextBlock",
      text: `**${index + 1}.** ${cardText(option.label.trim())}`,
      wrap: true,
      spacing: "Small",
    });
    const description = option.description?.trim() ?? "";
    if (description.length > 0) {
      body.push({ type: "TextBlock", text: cardText(description), wrap: true, spacing: "None" });
    }
  });
  return body;
}

function questionBody(question: AskCardQuestion): Record<string, unknown>[] {
  return [
    {
      type: "TextBlock",
      text: `问题：${cardText(question.question)}`,
      wrap: true,
      weight: "Bolder",
    },
    ...optionLines(question),
  ];
}

function choiceSetInput(question: AskCardQuestion, id: string): Record<string, unknown> {
  return {
    type: "Input.ChoiceSet",
    id,
    label: question.multi ? "可多选" : "单选",
    isMultiSelect: question.multi,
    choices: question.options.map((option, index) => ({
      title: `${index + 1}. ${option.label.trim()}`,
      value: String(index + 1),
    })),
  };
}

function answerInput(id: string, hasOptions: boolean): Record<string, unknown> {
  return {
    type: "Input.Text",
    id,
    label: hasOptions ? OTHER_INPUT_LABEL : ANSWER_INPUT_LABEL,
    placeholder: hasOptions ? OTHER_INPUT_PLACEHOLDER : ANSWER_INPUT_PLACEHOLDER,
  };
}

function pendingInputs(question: AskCardQuestion): Record<string, unknown>[] {
  if (question.options.length === 0) {
    return [answerInput(ASK_TEXT_INPUT_ID, false)];
  }
  if (question.multi) {
    return [choiceSetInput(question, ASK_CHOICES_INPUT_ID), answerInput(ASK_TEXT_INPUT_ID, true)];
  }
  return [answerInput(ASK_TEXT_INPUT_ID, true)];
}

function pendingActions(
  question: AskCardQuestion,
  generation: number,
): Record<string, unknown>[] {
  if (question.options.length === 0 || question.multi) {
    return [{ type: "Action.Submit", id: submitActionId(generation), title: "提交", style: "positive" }];
  }
  return [
    ...question.options.map((option, index) => ({
      type: "Action.Submit",
      id: optionActionId(index + 1, generation),
      title: actionTitle(index + 1, option.label),
    })),
    { type: "Action.Submit", id: submitActionId(generation), title: "提交其他答案", style: "positive" },
  ];
}

/**
 * Builds the frame for one question; `plain` is the caller's text rendering of the same
 * question, so the card and the text-only path stay one message instead of two competing ones.
 */
export function buildAskCard(
  question: AskCardQuestion,
  state: AskCardState,
  plain: string,
): AskCardFrame {
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body: questionBody(question),
  };
  if (state.kind === "pending") {
    const inputs = pendingInputs(question);
    if (inputs.length > 0) card.body = [...questionBody(question), ...inputs];
    card.actions = pendingActions(question, state.generation);
  } else {
    const note = state.kind === "answered"
      ? `已回复：${cardText(askAnswerText(question, state.answer))}`
      : state.note;
    card.body = [...questionBody(question), { type: "TextBlock", text: note, wrap: true, spacing: "Medium" }];
  }
  return { card, plain, profile: ASK_CARD_PROFILE };
}

function setQuestionBody(
  entry: AskCardSetQuestion,
  ordinal: number,
  total: number,
  open: boolean,
): Record<string, unknown>[] {
  const { question } = entry;
  const body: Record<string, unknown>[] = [
    {
      type: "TextBlock",
      text: `问题 ${ordinal}/${total}：${cardText(question.question)}`,
      wrap: true,
      weight: "Bolder",
      spacing: ordinal === 1 ? "None" : "Medium",
    },
    ...optionLines(question),
  ];
  if (entry.answer !== null) {
    body.push({
      type: "TextBlock",
      text: `已回复：${cardText(askAnswerText(question, entry.answer))}`,
      wrap: true,
      spacing: "Medium",
    });
    return body;
  }
  if (!open) return body;
  if (question.options.length > 0) {
    body.push(choiceSetInput(question, `${ASK_CHOICES_INPUT_ID}_${ordinal}`));
  }
  body.push(answerInput(`${ASK_TEXT_INPUT_ID}_${ordinal}`, question.options.length > 0));
  return body;
}

/**
 * Builds the frame that carries every question of one `ask` call. A set frame is answered by a
 * single submit, so its questions stay independent inputs rather than immediate answer buttons.
 */
export function buildAskCardSet(
  questions: readonly AskCardSetQuestion[],
  state: AskCardSetState,
  plain: string,
): AskCardFrame {
  const total = questions.length;
  const open = state.kind === "open" && questions.some(({ answer }) => answer === null);
  const body: Record<string, unknown>[] = [];
  if (open) {
    body.push({
      type: "TextBlock",
      text: `共 ${total} 题：填写后一次提交；也可以直接回复本消息作答。`,
      wrap: true,
      spacing: "None",
    });
  }
  questions.forEach((entry, position) => {
    body.push(...setQuestionBody(entry, position + 1, total, open));
  });
  if (state.kind === "closed" && state.note !== null) {
    body.push({ type: "TextBlock", text: cardText(state.note), wrap: true, spacing: "Medium" });
  }
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body,
  };
  if (open && state.kind === "open") {
    card.actions = [{
      type: "Action.Submit",
      id: submitActionId(state.generation),
      title: SET_SUBMIT_TITLE,
      style: "positive",
    }];
  }
  return { card, plain, profile: ASK_CARD_PROFILE };
}
