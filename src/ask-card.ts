/**
 * Interactive `ask` card: one question rendered as an octo/v2 frame so a single click answers it.
 *
 * The frame is an ordinary message as well — `plain` is the question text the text-only path
 * would have sent, a client that cannot render the card shows it verbatim, and a plain reply in
 * the chat is still the answer (see `CoreService.answerPendingQuestion`). Nothing here performs
 * I/O: the builder and the action decoder are pure so both callers stay testable.
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

/**
 * Decodes one click into the answer the question store records. Option buttons answer with their
 * position; the submit button reads the free-form text (`Input.Text`) or the selected indexes
 * (`Input.ChoiceSet` multi select, comma separated on the wire). An empty submission is
 * `invalid` — the question stays open and the caller re-renders with new action ids.
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
  const raw = question.multi && question.options.length > 0
    ? inputs[ASK_CHOICES_INPUT_ID] ?? ""
    : inputs[ASK_TEXT_INPUT_ID] ?? "";
  const value = raw.trim();
  if (value.length === 0) return { kind: "invalid" };
  if (question.multi && question.options.length > 0) {
    const indexes = value.split(",").map((part) => part.trim()).filter((part) => part.length > 0);
    if (indexes.length === 0) return { kind: "invalid" };
    if (!indexes.every((part) => /^\d+$/u.test(part)
      && Number(part) >= 1
      && Number(part) <= question.options.length)) {
      return { kind: "invalid" };
    }
    return { kind: "answer", answer: [...new Set(indexes)].join(",") };
  }
  return { kind: "answer", answer: value };
}

function questionBody(question: AskCardQuestion): Record<string, unknown>[] {
  const body: Record<string, unknown>[] = [
    {
      type: "TextBlock",
      text: `问题：${cardText(question.question)}`,
      wrap: true,
      weight: "Bolder",
    },
  ];
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

function pendingInputs(question: AskCardQuestion): Record<string, unknown>[] {
  if (question.options.length === 0) {
    return [{ type: "Input.Text", id: ASK_TEXT_INPUT_ID, label: "自填答案", placeholder: "输入答案" }];
  }
  if (question.multi) {
    return [{
      type: "Input.ChoiceSet",
      id: ASK_CHOICES_INPUT_ID,
      label: "可多选",
      isMultiSelect: true,
      choices: question.options.map((option, index) => ({
        title: `${index + 1}. ${option.label.trim()}`,
        value: String(index + 1),
      })),
    }];
  }
  return [];
}

function pendingActions(
  question: AskCardQuestion,
  generation: number,
): Record<string, unknown>[] {
  if (question.options.length === 0) {
    return [{ type: "Action.Submit", id: submitActionId(generation), title: "提交", style: "positive" }];
  }
  if (question.multi) {
    return [{ type: "Action.Submit", id: submitActionId(generation), title: "提交", style: "positive" }];
  }
  return question.options.map((option, index) => ({
    type: "Action.Submit",
    id: optionActionId(index + 1, generation),
    title: actionTitle(index + 1, option.label),
  }));
}

/**
 * Builds the frame for `state`; `plain` is the caller's text rendering of the same question, so
 * the card and the text-only path stay one message instead of two competing ones.
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
