/**
 * Owner configuration cards: `/switch` and `/mode` answer with an octo/v2 frame that carries one
 * compact choice list and one submit, so the owner applies a value without retyping the command.
 * The builders are pure. The caller owns everything else: the choice catalog and its bounds, the
 * validation of the returned value, and the decision whether a surface may render a card at all
 * — both commands stay owner-only even where the card itself is visible to everyone.
 *
 * Choice values and titles are copied into the frame verbatim — a title is native option text,
 * not TextBlock markdown. The strings a TextBlock paints (the current value, the result text)
 * are escaped the way the ask card escapes them. A consumed or expired card cannot be clicked a
 * second time (the server accepts a given `(message_id, action_id, operator_uid)` click exactly
 * once), so the terminal frame carries no inputs and no actions — only the outcome sentence the
 * caller built.
 */

import type { CardFrame } from "../contracts.ts";

export type CommandCardKind = "switch" | "mode";

/** The one `Input.ChoiceSet` of a configuration card; the caller reads the click by this id. */
export const COMMAND_INPUT_ID = "config_value";

export interface CommandCardView {
  kind: CommandCardKind;
  current: string;
  choices: readonly { title: string; value: string }[];
  plain: string;
}

const KIND_TEXT: Record<CommandCardKind, { title: string; label: string; note: string; command: string }> = {
  switch: {
    title: "切换本聊天模型",
    label: "模型",
    note: "选择「default」清除本聊天的模型覆盖，之后启动的回合跟随 OMP 默认；不会重置正在运行的模型。",
    command: "/switch",
  },
  mode: {
    title: "追加模式",
    label: "追加方式",
    note: "steer：运行中的新消息注入当前回合；queue：排到当前回合之后再执行。",
    command: "/mode",
  },
};

/**
 * Escapes the Markdown openers a TextBlock would otherwise interpret as links, the same way the
 * ask card does. The double escaping is not self-explanatory, so the two TextBlock display strings
 * — the current value and the result text — share this one place. Choice titles are native option
 * text rather than TextBlock markdown and are deliberately not escaped.
 */
function cardText(text: string): string {
  return text.replace(/\\/gu, "\\\\").replace(/\[/gu, "\\[");
}

/**
 * The interactive frame: what the chat uses now, what the choice means, the list, one submit.
 * `plain` is the caller's text rendering of the same view and is carried unchanged.
 */
export function buildCommandCard(view: CommandCardView, actionId: string): CardFrame {
  const copy = KIND_TEXT[view.kind];
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body: [
      { type: "TextBlock", text: copy.title, wrap: true, weight: "Bolder", size: "Medium" },
      { type: "TextBlock", text: `当前：${cardText(view.current)}`, wrap: true, spacing: "Small" },
      { type: "TextBlock", text: copy.note, wrap: true, spacing: "Small", isSubtle: true },
      {
        type: "Input.ChoiceSet",
        id: COMMAND_INPUT_ID,
        label: copy.label,
        style: "compact",
        isMultiSelect: false,
        wrap: true,
        choices: view.choices.map(choice => ({ title: choice.title, value: choice.value })),
        // A compact select paints nothing without a value or placeholder. Preselect the current
        // value only when it is one of the supplied choices; never guess a default — a silent
        // preselect would reset a setting the owner never touched.
        ...(view.choices.some(choice => choice.value === view.current)
          ? { value: view.current }
          : { placeholder: "请选择…" }),
      },
      {
        type: "TextBlock",
        text: `仅所有者可提交；卡片过期或已提交后，重新发送 ${copy.command} 可再次打开。`,
        wrap: true,
        spacing: "Small",
        isSubtle: true,
      },
    ],
    actions: [{ type: "Action.Submit", id: actionId, title: "应用", style: "positive" }],
  };
  return { card, plain: view.plain, profile: "octo/v2" };
}

/** The spent, expired or refused card: same heading, the outcome, no controls of any kind. */
export function buildCommandCardResult(kind: CommandCardKind, text: string): CardFrame {
  const copy = KIND_TEXT[kind];
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body: [
      { type: "TextBlock", text: copy.title, wrap: true, weight: "Bolder", size: "Medium" },
      { type: "TextBlock", text: cardText(text), wrap: true, spacing: "Small" },
      {
        type: "TextBlock",
        text: `重新发送 ${copy.command} 可再次打开配置卡片。`,
        wrap: true,
        spacing: "Small",
        isSubtle: true,
      },
    ],
  };
  return { card, plain: text, profile: "octo/v2" };
}
