import type { ExecutionCardCapabilities } from "../execution-card.ts";
import type { ExecutionSnapshot } from "../execution-tracker.ts";
import type { BotCardAction, CardFrame, CardProfile } from "../contracts.ts";
import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import { OctoError } from "./error.ts";
import { buildExecutionCard } from "../execution-card.ts";
import { INTERACTIVE_CARD_MESSAGE_TYPE } from "./content.ts";
import {
  MAX_HTTP_BODY_BYTES,
  apiUrl,
  authHeaders,
  httpStatusError,
  parseJson,
  parseJsonWithInt64,
  requestWithTimeout,
} from "./http.ts";

/** 进度卡与问答卡的投递 profile 与版本：卡片能力按它协商，封套按它封边。 */
export const CARD_PROFILE = "octo/v1";
export const CARD_VERSION = "1.5";

/**
 * 卡片与 Bot 事件封套：能力协商（profile / 元素 / 动作 / 上限）、帧封套的节点与体积检查，
 * 以及 `POST /v1/bot/events` 事件（卡片点击）的解析。这里只描述线上形状，不实现卡片布局。
 */
export function stringArray(value: unknown, required = false): string[] | undefined | null {
  if (value === undefined) return required ? null : undefined;
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) return null;
  return value as string[];
}

export function capabilityLimit(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || (value as number) <= 0) {
    throw new OctoError("Octo card profile limits contain an invalid value");
  }
  return value as number;
}

export type NormalizedCardLimits = NonNullable<ExecutionCardCapabilities["limits"]>;

export function cardProfileLimits(value: unknown): NormalizedCardLimits | undefined | null {
  if (value === undefined) return undefined;
  if (!isJsonRecord(value)) return null;
  const maxNodes = capabilityLimit(value.max_nodes);
  const maxDepth = capabilityLimit(value.max_depth);
  const maxPayloadBytes = capabilityLimit(value.max_payload_bytes);
  return {
    ...(maxNodes === undefined ? {} : { maxNodes }),
    ...(maxDepth === undefined ? {} : { maxDepth }),
    ...(maxPayloadBytes === undefined ? {} : { maxPayloadBytes }),
  };
}

export function cardProfileCapabilities(value: unknown): ExecutionCardCapabilities | null {
  if (!isJsonRecord(value)) return null;
  const profile = value;
  const profiles = stringArray(profile.profiles, true);
  const elements = stringArray(profile.elements);
  const actions = stringArray(profile.actions);
  if (profiles === null || profiles === undefined || elements === null || actions === null) return null;
  const config = isJsonRecord(profile.config) ? profile.config : {};
  let limits: NormalizedCardLimits | undefined | null;
  try {
    limits = cardProfileLimits(profile.limits);
  } catch (error) {
    if (error instanceof OctoError) return null;
    throw error;
  }
  if (limits === null) return null;
  const displayEnabled = (profile.enabled === true || profile.enabled === 1)
    && config.card_enabled === true
    && config.display_enabled === true;
  const enabled = displayEnabled && config.reasoning_enabled === true;
  const interactive = displayEnabled
    && config.interaction_enabled === true
    && profiles.includes("octo/v2");
  return {
    enabled,
    interactive,
    profiles,
    cardVersion: typeof profile.card_version === "string" ? profile.card_version : "",
    ...(elements === undefined ? {} : { elements }),
    ...(actions === undefined ? {} : { actions }),
    ...(limits === undefined ? {} : { limits }),
  };
}

/** `octo/v2` needs the interaction switch; `octo/v1` display frames need the existing gate. */
export function cardProfileSupported(
  capabilities: ExecutionCardCapabilities,
  profile: CardProfile,
): boolean {
  if (!capabilities.profiles.includes(profile)) return false;
  return profile === "octo/v2" ? capabilities.interactive : capabilities.enabled;
}

export const BOT_EVENT_MAX_LIMIT = 100;
/** The server clamps the hold to [0, 30]; staying below keeps a cancellation responsive. */
export const BOT_EVENT_MAX_WAIT_SECONDS = 25;
export const BOT_EVENT_TIMEOUT_MARGIN_MS = 10_000;

export function eventString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** Event ids are queue counters; they arrive as JSON numbers and are compared, never computed. */
export function botEventId(value: unknown): number | null {
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return value;
  if (typeof value === "string" && /^\d+$/u.test(value)) {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Parses the frozen `card_action` envelope. Anything that does not match exactly — another event
 * type, a missing field, an unknown channel — is left to the caller as an unclassified event so
 * it can be acknowledged instead of blocking the queue.
 */
export function cardActionFromEvent(event: JsonRecord): BotCardAction | null {
  if (event.event_type !== "card_action" || !isJsonRecord(event.event_data)) return null;
  const data = event.event_data;
  const messageId = eventString(data.message_id);
  const channelId = eventString(data.channel_id);
  const actionId = eventString(data.action_id);
  const operatorUid = eventString(data.operator_uid);
  const channelType = data.channel_type;
  if (messageId === null || channelId === null || actionId === null || operatorUid === null) {
    return null;
  }
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) return null;
  const inputs: Record<string, string> = {};
  if (isJsonRecord(data.inputs)) {
    for (const [key, value] of Object.entries(data.inputs)) {
      if (typeof value === "string") inputs[key] = value;
    }
  }
  return { messageId, channelId, channelType, actionId, operatorUid, inputs };
}

export type CardStats = {
  nodes: number;
  maxDepth: number;
};

export function cardStats(value: unknown, depth = 0): CardStats {
  if (Array.isArray(value)) {
    let nodes = 0;
    let maxDepth = depth;
    for (const child of value) {
      const childStats = cardStats(child, depth);
      nodes += childStats.nodes;
      maxDepth = Math.max(maxDepth, childStats.maxDepth);
    }
    return { nodes, maxDepth };
  }
  if (value === null || typeof value !== "object") return { nodes: 0, maxDepth: depth };
  let nodes = 1;
  let maxDepth = depth + 1;
  for (const child of Object.values(value)) {
    const childStats = cardStats(child, depth + 1);
    nodes += childStats.nodes;
    maxDepth = Math.max(maxDepth, childStats.maxDepth);
  }
  return { nodes, maxDepth };
}

export function cardWithinLimits(
  envelope: JsonRecord,
  limits: NormalizedCardLimits,
): boolean {
  const maxPayloadBytes = limits.maxPayloadBytes;
  if (maxPayloadBytes !== undefined
    && Buffer.byteLength(JSON.stringify(envelope), "utf8") > maxPayloadBytes) {
    return false;
  }
  const stats = cardStats(envelope);
  return (limits.maxNodes === undefined || stats.nodes <= limits.maxNodes)
    && (limits.maxDepth === undefined || stats.maxDepth <= limits.maxDepth);
}

export function executionCardEnvelope(
  snapshot: ExecutionSnapshot,
  capabilities: ExecutionCardCapabilities,
  options: { cardSeq?: number; transient?: boolean } = {},
): JsonRecord | null {
  const built = buildExecutionCard(snapshot, capabilities);
  if (!built) return null;
  return cardFrameEnvelope(
    { card: built.card, plain: built.plain, profile: CARD_PROFILE },
    capabilities,
    options,
  );
}

/**
 * Builds the wire envelope every card path shares (progress and ask cards alike) and refuses
 * frames the deployment's declared limits cannot carry.
 */
export function cardFrameEnvelope(
  frame: CardFrame,
  capabilities: ExecutionCardCapabilities,
  options: { cardSeq?: number; transient?: boolean } = {},
): JsonRecord | null {
  const envelope: JsonRecord = {
    type: INTERACTIVE_CARD_MESSAGE_TYPE,
    profile: frame.profile,
    card_version: CARD_VERSION,
    card: frame.card,
    plain: frame.plain,
  };
  if (options.cardSeq !== undefined) envelope.card_seq = options.cardSeq;
  if (options.transient === true) envelope.transient = true;
  return cardWithinLimits(envelope, capabilities.limits ?? {}) ? envelope : null;
}

export async function cardProfileCapabilitiesFromApi(
  apiBase: string,
  botToken: string,
  signal: AbortSignal | undefined,
): Promise<ExecutionCardCapabilities | null> {
  const response = await requestWithTimeout(
    apiUrl(apiBase, "/v1/bot/card/profile"),
    { method: "GET", headers: authHeaders(botToken) },
    "card profile",
    signal,
    MAX_HTTP_BODY_BYTES,
  );
  if (response.status === 404) return null;
  if (response.status < 200 || response.status >= 300) {
    throw httpStatusError("card profile", response.status);
  }
  try {
    return cardProfileCapabilities(parseJson(response.body, "card profile"));
  } catch (error) {
    if (error instanceof OctoError) return null;
    throw error;
  }
}
