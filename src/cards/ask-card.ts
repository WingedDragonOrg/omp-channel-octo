/**
 * Interactive `ask` cards: a question — or a whole set of them — rendered as an octo/v2 frame so
 * the owner answers from the card, or by replying in the chat.
 *
 * A single question keeps its own card: one option per button for a single choice, a selectable
 * list plus one submit for a multiple choice. Every option button stands in an `ActionSet` row
 * of its own inside the body: body actionsets stack vertically, while the card-level `actions`
 * area draws one horizontal row that clips longer labels at the card edge. Several questions
 * share one card — every question carries its own inputs and the whole set is answered by one
 * submit. Every option question also carries a free-form field whose value wins over the
 * selection when it is filled.
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
const OPTION_ACTION_ID = new RegExp(`^${OPTION_ACTION_PREFIX}(\\d+)(?:#(\\d+))?$`, "u");
const SUBMIT_ACTION_ID = new RegExp(`^${SUBMIT_ACTION_PREFIX}(?:#(\\d+))?$`, "u");
/** A set submission records the filled answers and leaves the other questions open. */
const SET_SUBMIT_TITLE = "提交已填答案";
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
  | { kind: "answered"; answer: string; answeredBy: string | null }
  | { kind: "inactive"; note: string };

/** One question of a set frame together with the answer recorded for it so far. */
export interface AskCardSetQuestion {
  question: AskCardQuestion;
  /** Recorded answer; `null` while the question still waits for one. */
  answer: string | null;
  /** Who answered it; `null` while open or when the row predates attribution. */
  answeredBy: string | null;
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

/**
 * Display names of the uids an answered line attributes (uid → name), resolved by the caller.
 * A uid the map does not carry — an unresolved lookup, a row written before attribution — renders
 * as the uid itself, so a failed lookup degrades to the pre-name wording instead of hiding who
 * answered. The uid stays the identity everywhere; this map only changes the spelling.
 */
export type AnswererNames = ReadonlyMap<string, string>;

export type AskActionRef =
  | { kind: "option"; index: number; generation: number }
  | { kind: "submit"; generation: number };

export type AskAnswer = { kind: "answer"; answer: string } | { kind: "invalid" };

/** Escapes the Markdown openers a TextBlock would otherwise interpret as links. */
function cardText(text: string): string {
  return text.replace(/\\/gu, "\\\\").replace(/\[/gu, "\\[");
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

/** The answered sentence's parts: who answered (empty for a row written before attribution) and the answer body. */
function answeredParts(
  question: AskCardQuestion,
  answer: string,
  answeredBy: string | null,
  names?: AnswererNames,
): { answerer: string; body: string } {
  const uid = answeredBy?.trim() ?? "";
  const name = uid.length === 0 ? "" : names?.get(uid)?.trim() ?? "";
  // 名字是署名，也是卡片文本里的一段数据：解析不到就回退成 uid，署名始终在，身份不变。
  return { answerer: name.length > 0 ? name : uid, body: askAnswerText(question, answer) };
}

/**
 * The sentence the text fallback (a frame's `plain`) shows for a recorded answer. A message body
 * has no style of its own, so the name is Markdown-bold there — the real client renders it
 * (measured 2026-09-19: the probe's rendered name came out bold with no stray asterisks).
 */
export function askAnsweredText(
  question: AskCardQuestion,
  answer: string,
  answeredBy: string | null,
  names?: AnswererNames,
): string {
  const { answerer, body } = answeredParts(question, answer, answeredBy, names);
  return answerer.length === 0 ? `已回复：${body}` : `已由 **${answerer}** 回复：${body}`;
}

/**
 * The answered row as the card paints it: the answerer is a `TextRun` carrying its own weight, so
 * the name is bold by style rather than by Markdown syntax in the text (bold confirmed on the real
 * client 2026-09-19). A row without attribution stays one plain text line.
 */
function answeredCardElement(
  question: AskCardQuestion,
  answer: string,
  answeredBy: string | null,
  names: AnswererNames | undefined,
): Record<string, unknown> {
  const { answerer, body } = answeredParts(question, answer, answeredBy, names);
  if (answerer.length === 0) {
    return { type: "TextBlock", text: cardText(`已回复：${body}`), wrap: true, spacing: "Small", color: "Good" };
  }
  return {
    type: "RichTextBlock",
    spacing: "Small",
    inlines: [
      { type: "TextRun", text: cardText("已由 "), color: "Good" },
      { type: "TextRun", text: cardText(answerer), weight: "Bolder", color: "Good" },
      { type: "TextRun", text: cardText(` 回复：${body}`), color: "Good" },
    ],
  };
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

/** Full option labels and descriptions, with single-choice actions kept beside their option. */
function optionLines(question: AskCardQuestion, generation?: number): Record<string, unknown>[] {
  const body: Record<string, unknown>[] = [];
  question.options.forEach((option, index) => {
    const items: Record<string, unknown>[] = [{
      type: "TextBlock",
      text: `**${index + 1}.** ${cardText(option.label.trim())}`,
      wrap: true,
      spacing: "Small",
      weight: generation === undefined ? "Default" : "Bolder",
      isSubtle: generation === undefined,
    }];
    const description = option.description?.trim() ?? "";
    if (description.length > 0) {
      items.push({ type: "TextBlock", text: cardText(description), wrap: true, spacing: "None", isSubtle: true });
    }
    if (generation !== undefined && !question.multi) {
      body.push({
        type: "ColumnSet",
        spacing: "Default",
        separator: index > 0,
        columns: [
          { type: "Column", width: "stretch", items },
          {
            type: "Column",
            width: "auto",
            spacing: "Small",
            items: [{
              type: "ActionSet",
              actions: [{
                type: "Action.Submit",
                id: optionActionId(index + 1, generation),
                title: `${index + 1}. 选择`,
              }],
            }],
          },
        ],
      });
    } else {
      body.push(...items);
    }
  });
  return body;
}

function questionBody(question: AskCardQuestion, generation?: number): Record<string, unknown>[] {
  return [
    {
      type: "TextBlock",
      text: cardText(question.question),
      wrap: true,
      weight: "Bolder",
      size: "Medium",
    },
    ...(generation !== undefined && question.multi ? [] : optionLines(question, generation)),
  ];
}

function choiceSetInput(question: AskCardQuestion, id: string): Record<string, unknown> {
  return {
    type: "Input.ChoiceSet",
    id,
    label: question.multi ? "可多选" : "单选",
    isMultiSelect: question.multi,
    style: "expanded",
    wrap: true,
    choices: question.options.map((option, index) => ({
      title: `**${index + 1}. ${cardText(option.label.trim())}**${option.description?.trim() ? `  \n${cardText(option.description.trim())}` : ""}`,
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
    isMultiline: !hasOptions,
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

/**
 * The card-level action area holds only the submit; option actions live beside their
 * explanations in body ActionSets so the client stacks them instead of clipping a shared row.
 */
function pendingActions(
  question: AskCardQuestion,
  generation: number,
): Record<string, unknown>[] {
  const freeFormOnly = question.options.length === 0 || question.multi;
  return [{
    type: "Action.Submit",
    id: submitActionId(generation),
    title: freeFormOnly ? "提交" : "提交其他答案",
    style: "positive",
  }];
}

/**
 * Builds the frame for one question; `plain` is the caller's text rendering of the same
 * question, so the card and the text-only path stay one message instead of two competing ones.
 * `names` spells the answerer of an answered frame — its `plain` is built by the caller, so the
 * caller must resolve the same names into it.
 */
export function buildAskCard(
  question: AskCardQuestion,
  state: AskCardState,
  plain: string,
  names?: AnswererNames,
): AskCardFrame {
  const body = questionBody(question, state.kind === "pending" ? state.generation : undefined);
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body,
  };
  if (state.kind === "pending") {
    body.push(...pendingInputs(question));
    card.actions = pendingActions(question, state.generation);
  } else {
    const note = state.kind === "answered"
      ? answeredCardElement(question, state.answer, state.answeredBy, names)
      : { type: "TextBlock", text: state.note, wrap: true, spacing: "Medium" };
    body.splice(1, 0, note);
  }
  return { card, plain, profile: ASK_CARD_PROFILE };
}

function setQuestionBody(
  entry: AskCardSetQuestion,
  ordinal: number,
  total: number,
  open: boolean,
  names?: AnswererNames,
): Record<string, unknown>[] {
  const { question } = entry;
  const body: Record<string, unknown>[] = [
    {
      type: "TextBlock",
      text: `${ordinal} / ${total}  ${cardText(question.question)}`,
      wrap: true,
      weight: "Bolder",
      size: "Medium",
      separator: ordinal > 1,
      spacing: ordinal === 1 ? "Small" : "Medium",
    },
    ...(open && entry.answer === null ? [] : optionLines(question)),
  ];
  if (entry.answer !== null) {
    body.splice(1, 0, answeredCardElement(question, entry.answer, entry.answeredBy, names));
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
  names?: AnswererNames,
): AskCardFrame {
  const total = questions.length;
  const remaining = questions.reduce((count, { answer }) => count + Number(answer === null), 0);
  const open = state.kind === "open" && remaining > 0;
  const body: Record<string, unknown>[] = [];
  if (open) {
    body.push({
      type: "TextBlock",
      text: `待回答 ${remaining} / ${total} · 填写后提交，也可直接回复消息作答。`,
      wrap: true,
      spacing: "None",
      isSubtle: true,
    });
  }
  questions.forEach((entry, position) => {
    body.push(...setQuestionBody(entry, position + 1, total, open, names));
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
