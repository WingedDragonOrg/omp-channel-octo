/**
 * Owner-confirmed onboarding cards for an unlisted group or CommunityTopic. The card is only a
 * presentation surface; the inbound grant desk owns its action identity, bindings, and durable
 * operation. Terminal frames remove every action so a completed card cannot be clicked again.
 */

import type { CardFrame } from "../contracts.ts";

export const CHAT_GRANT_ACTION_PREFIX = "grant-chat:";

const CHAT_GRANT_ACTION_ID =
  /^grant-chat:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

export function isChatGrantActionId(actionId: string): boolean {
  return CHAT_GRANT_ACTION_ID.test(actionId);
}

function scopeName(channelType: 2 | 5): "本群" | "本子区" {
  return channelType === 5 ? "本子区" : "本群";
}

function cardText(text: string): string {
  return text.replace(/\\/gu, "\\\\").replace(/\[/gu, "\\[");
}

export function chatGrantPrompt(channelType: 2 | 5, botName: string): string {
  const scope = scopeName(channelType);
  return `${scope}未开通。回复「@${botName} 开通本群」即可开通${scope}。`;
}

export function buildChatGrantCard(channelType: 2 | 5, botName: string, actionId: string): CardFrame {
  const scope = scopeName(channelType);
  const plain = chatGrantPrompt(channelType, botName);
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body: [
      { type: "TextBlock", text: `${scope}未开通`, wrap: true, weight: "Bolder", size: "Medium" },
      {
        type: "TextBlock",
        text: "开通后，群成员可以 @Bot 发起任务；命令仍仅 Bot owner 可用。",
        wrap: true,
        spacing: "Small",
      },
      {
        type: "TextBlock",
        text: "只有 Bot owner 可以确认开通。触发这张卡片的原消息不会执行。",
        wrap: true,
        spacing: "Small",
        isSubtle: true,
      },
      {
        type: "TextBlock",
        text: `确认后，请重新 @${cardText(botName)} 发送任务。`,
        wrap: true,
        spacing: "Small",
        isSubtle: true,
      },
    ],
    actions: [{ type: "Action.Submit", id: actionId, title: `开通${scope}`, style: "positive" }],
  };
  return { card, plain, profile: "octo/v2" };
}

export function buildChatGrantCardResult(channelType: 2 | 5, text: string): CardFrame {
  const scope = scopeName(channelType);
  const card: Record<string, unknown> = {
    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
    type: "AdaptiveCard",
    version: "1.5",
    body: [
      { type: "TextBlock", text: `${scope}开通结果`, wrap: true, weight: "Bolder", size: "Medium" },
      { type: "TextBlock", text: cardText(text), wrap: true, spacing: "Small" },
    ],
  };
  return { card, plain: text, profile: "octo/v2" };
}
