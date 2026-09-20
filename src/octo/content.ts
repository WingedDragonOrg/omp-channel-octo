import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import { OctoError } from "./error.ts";
import type {
  HistoryMessage,
  InboundAttachment,
  InboundAttachmentKind,
  InboundMessage,
  InboundQuote,
} from "../contracts.ts";

/**
 * 入站消息的语义层：按 payload 类型表提取文本与附件、剥离 Bot mention、展开合并转发与富文本，
 * 并把一行历史消息归一成同一个 `InboundMessage` 形状。这里不含任何网络与连接逻辑。
 */
export const IMAGE_MESSAGE_TYPE = 2;
const GIF_MESSAGE_TYPE = 3;
const VOICE_MESSAGE_TYPE = 4;
const VIDEO_MESSAGE_TYPE = 5;
const LOCATION_MESSAGE_TYPE = 6;
const CONTACT_CARD_MESSAGE_TYPE = 7;
export const FILE_MESSAGE_TYPE = 8;
/** Not an octo-lib enum value, but clients do send files as type 9. */
const FILE_ALIAS_MESSAGE_TYPE = 9;
const MERGE_FORWARD_MESSAGE_TYPE = 11;
export const TEXT_MESSAGE_TYPE = 1;
const RICH_TEXT_MESSAGE_TYPE = 14;
export const INTERACTIVE_CARD_MESSAGE_TYPE = 17;
/**
 * Types whose payload text *is* the message input: an empty text is a legitimate, if
 * contentless, turn (`@bot` with nothing else), so these stay supported and run normally
 * instead of being reported as an unsupported message type. Types outside this set carry
 * their input as media, so they stay supported only when something was actually extracted.
 */
const TEXT_SHAPED_MESSAGE_TYPES: ReadonlySet<number> = new Set([
  TEXT_MESSAGE_TYPE,
  RICH_TEXT_MESSAGE_TYPE,
  INTERACTIVE_CARD_MESSAGE_TYPE,
]);
const RICH_TEXT_IMAGE_BLOCK = "image";
const RICH_TEXT_IMAGE_PLACEHOLDER = "[图片]";
/** 交互卡片 `plain` 缺失时的降级文本，与 octo-server Decision 8 的 `[卡片]` 占位符一致。 */
const CARD_PLACEHOLDER = "[卡片]";
const MERGE_FORWARD_HEADER = "[合并转发]";
const MAX_MERGE_FORWARD_MESSAGES = 30;
const MAX_MERGE_FORWARD_CODE_POINTS = 4_000;
const MAX_RICH_TEXT_CODE_POINTS = 2_000;
/** 引用块只承载文本，超出部分截断：引用是上下文，不是第二份正文。 */
const MAX_QUOTED_CODE_POINTS = 2_000;

type DecodedInbound = {
  setting: number;
  senderId: string;
  rawChannelId: string;
  channelType: number;
  messageId: bigint;
  messageSeq: number;
  timestamp: number;
  payload: unknown;
};

export const MAX_TEXT_BYTES = 3_000;

type MentionEntity = { uid: string; offset: number; length: number };

function mentionEntities(payload: JsonRecord): MentionEntity[] {
  const mention = payload.mention;
  if (!isJsonRecord(mention) || !Array.isArray(mention.entities)) return [];
  const entities: MentionEntity[] = [];
  for (const raw of mention.entities) {
    if (!isJsonRecord(raw) || typeof raw.uid !== "string" || raw.uid.length === 0) continue;
    if (!Number.isSafeInteger(raw.offset) || !Number.isSafeInteger(raw.length)) continue;
    const offset = raw.offset as number;
    const length = raw.length as number;
    if (offset < 0 || length <= 0) continue;
    entities.push({ uid: raw.uid, offset, length });
  }
  return entities;
}

function mentionUids(payload: JsonRecord): string[] {
  const mention = payload.mention;
  if (!isJsonRecord(mention)) return [];
  const entityUids: string[] = [];
  if (Array.isArray(mention.entities)) {
    for (const raw of mention.entities) {
      if (!isJsonRecord(raw) || typeof raw.uid !== "string" || raw.uid.length === 0) continue;
      if (!entityUids.includes(raw.uid)) entityUids.push(raw.uid);
    }
  }
  if (entityUids.length > 0) return entityUids;
  const uids: string[] = [];
  if (Array.isArray(mention.uids)) {
    for (const raw of mention.uids) {
      if (typeof raw !== "string" || raw.length === 0 || uids.includes(raw)) continue;
      uids.push(raw);
    }
  }
  return uids;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

/**
 * Removes the bot mention wherever the client placed it. Group clients render
 * `@bot /command args` and `/command args @bot` alike, and a mention may also
 * arrive as a bare uid list without spans.
 */
function stripBotMention(
  text: string,
  botUid: string,
  botName: string,
  entities: readonly MentionEntity[],
  mentionedUserIds: readonly string[],
): string {
  const spans = entities
    .filter((entity) => entity.uid === botUid
      && entity.offset + entity.length <= text.length
      && text.slice(entity.offset, entity.offset + entity.length).startsWith("@"))
    .sort((left, right) => right.offset - left.offset);
  if (spans.length > 0) {
    let stripped = text;
    for (const span of spans) {
      stripped = stripped.slice(0, span.offset) + stripped.slice(span.offset + span.length);
    }
    return trimMentionSeam(
      stripped,
      spans.some((span) => span.offset === 0),
      spans.some((span) => span.offset + span.length === text.length),
    );
  }
  if (!mentionedUserIds.includes(botUid)) return text;
  const tokens = [botName, botUid].filter((value) => value.length > 0).map(escapeRegExp);
  if (tokens.length === 0) return text;
  const pattern = new RegExp(`(^|\\s)@(?:${tokens.join("|")})(?=$|[\\s:：])`, "gu");
  let touchesStart = false;
  let touchesEnd = false;
  const stripped = text.replace(pattern, (match: string, lead: string, offset: number) => {
    if (offset === 0) touchesStart = true;
    if (offset + match.length === text.length) touchesEnd = true;
    return lead;
  });
  return stripped === text ? text : trimMentionSeam(stripped, touchesStart, touchesEnd);
}

function trimMentionSeam(text: string, touchesStart: boolean, touchesEnd: boolean): string {
  let trimmed = text;
  if (touchesStart) trimmed = trimmed.replace(/^[\s:：]+/u, "");
  if (touchesEnd) trimmed = trimmed.replace(/\s+$/u, "");
  return trimmed;
}

/**
 * Sequences and second timestamps arrive as unsigned 32-bit integers. Values that cannot
 * take part in history accounting (missing/zero) collapse to the `0` unknown sentinel.
 */
function knownIntOrZero(value: number): number {
  return Number.isSafeInteger(value) && value > 0 ? value : 0;
}

type ExtractedContent = {
  text: string;
  attachments: InboundAttachment[];
};

/**
 * Absolute http/https URL, or `undefined` when the value is missing, relative, or uses
 * another scheme. Octo falls back to a relative `file/preview/...` path when it cannot
 * mint a download URL, and that route needs a login the bot does not have — so relative
 * references are unusable, never resolved against a guessed base.
 */
export function absoluteHttpUrl(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length === 0) return undefined;
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return undefined;
  } catch {
    return undefined;
  }
  return value;
}

export function textField(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/** Client-declared numbers are advisory: anything not finite and positive counts as unknown. */
function declaredNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}

/** Types 8/9 carry the name and its extension in separate fields on some clients. */
function fileName(payload: JsonRecord): string {
  const name = textField(payload.name);
  const extension = textField(payload.extension).replace(/^\./u, "").trim();
  if (name.length === 0 || !/^[A-Za-z0-9]{1,12}$/u.test(extension)) return name;
  return name.toLowerCase().endsWith(`.${extension.toLowerCase()}`) ? name : `${name}.${extension}`;
}

/** Types 2/8/9 carry the user's words in `caption` and fall back to `content`. */
function captionOrContent(payload: JsonRecord): string {
  const caption = textField(payload.caption);
  return caption.length > 0 ? caption : textField(payload.content);
}

/** Voice and video clients disagree on the duration field, so the first usable one wins. */
function declaredDuration(payload: JsonRecord): number {
  for (const field of ["second", "timeTrad", "duration"] as const) {
    const seconds = declaredNumber(payload[field]);
    if (seconds > 0) return seconds;
  }
  return 0;
}

/**
 * Records one attachment candidate. The URL is taken verbatim: whether it is usable is
 * decided by `normalizedMessage` (live path) and `downloadAttachment` (fetch path), so
 * this table only ever answers "what did the payload point at".
 */
function pushAttachment(
  list: InboundAttachment[],
  url: unknown,
  kind: InboundAttachmentKind,
  fields: { name?: unknown; size?: unknown; durationSeconds?: number } = {},
): void {
  if (typeof url !== "string" || url.length === 0) return;
  list.push({
    url,
    kind,
    name: textField(fields.name),
    size: declaredNumber(fields.size),
    durationSeconds: declaredNumber(fields.durationSeconds),
  });
}

/** Type 6 coordinates live under two naming conventions across clients. */
function locationText(payload: JsonRecord): string {
  const latitude = scalarText(payload.latitude ?? payload.lat);
  const longitude = scalarText(payload.longitude ?? payload.lng ?? payload.lon);
  const details = [scalarText(payload.address), scalarText(payload.title)]
    .filter((part) => part.length > 0);
  const coordinates = latitude.length === 0 && longitude.length === 0 ? "" : `${latitude},${longitude}`;
  const line = coordinates.length === 0 ? "" : `[位置信息: ${coordinates}]`;
  return [line, ...details].filter((part) => part.length > 0).join(" ");
}

/** Type 7 renders one line so the agent sees the contact without a download. */
function contactText(payload: JsonRecord): string {
  const name = textField(payload.name);
  const uid = textField(payload.uid);
  if (name.length === 0 && uid.length === 0) return "";
  if (name.length === 0) return `[名片: ${uid}]`;
  if (uid.length === 0) return `[名片: ${name}]`;
  return `[名片: ${name} (${uid})]`;
}

/** Coordinates may arrive as numbers or as strings; both keep their written form. */
function scalarText(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return "";
}

/** Counts code points rather than UTF-16 units, so a cap means what it says. */
function codePointLength(text: string): number {
  let length = text.length;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    const next = text.charCodeAt(index + 1);
    if (code >= 0xd800 && code <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
      length -= 1;
      index += 1;
    }
  }
  return length;
}

/** Truncates at a code point boundary without splitting a surrogate pair. */
function truncateCodePoints(text: string, maximum: number): string {
  let seen = 0;
  let offset = 0;
  for (const character of text) {
    if (seen >= maximum) return text.slice(0, offset);
    seen += 1;
    offset += character.length;
  }
  return text;
}

/** Media that a forwarded chat or an unreadable block cannot deliver is named, not fetched. */
function mediaPlaceholder(type: unknown): string {
  switch (type) {
    case IMAGE_MESSAGE_TYPE:
    case GIF_MESSAGE_TYPE:
      return "[图片]";
    case VOICE_MESSAGE_TYPE:
      return "[语音]";
    case VIDEO_MESSAGE_TYPE:
      return "[视频]";
    case FILE_MESSAGE_TYPE:
    case FILE_ALIAS_MESSAGE_TYPE:
      return "[文件]";
    default:
      return "";
  }
}

/**
 * Type 11 carries a forwarded chat: a header line plus one line per inner message,
 * expanded exactly one level deep and bounded in message count and total text. Inner
 * media never downloads — it stays a placeholder — and a forward inside a forward is
 * rendered as the header alone instead of recursing.
 */
function mergeForwardText(payload: JsonRecord): string {
  const names = new Map<string, string>();
  const users: unknown[] = Array.isArray(payload.users) ? payload.users : [];
  for (const raw of users) {
    if (!isJsonRecord(raw)) continue;
    const uid = textField(raw.uid);
    const name = textField(raw.name);
    if (uid.length > 0 && name.length > 0) names.set(uid, name);
  }
  const messages: unknown[] = Array.isArray(payload.msgs) ? payload.msgs : [];
  const lines: string[] = [MERGE_FORWARD_HEADER];
  let budget = MAX_MERGE_FORWARD_CODE_POINTS;
  let position = 0;
  const limit = Math.min(messages.length, MAX_MERGE_FORWARD_MESSAGES);
  for (; position < limit; position += 1) {
    const raw = messages[position];
    const line = isJsonRecord(raw) ? forwardedLine(raw, names) : undefined;
    if (line === undefined) continue;
    const cost = codePointLength(line) + 1;
    if (cost > budget) break;
    budget -= cost;
    lines.push(line);
  }
  const omitted = messages.length - position;
  if (omitted > 0) lines.push(`…（其余 ${omitted} 条省略）`);
  return lines.join("\n");
}

/** One inner line of a forwarded chat: `sender: text`. */
function forwardedLine(message: JsonRecord, names: ReadonlyMap<string, string>): string | undefined {
  const payload = message.payload;
  if (!isJsonRecord(payload)) return undefined;
  const text = forwardedMessageText(payload);
  if (text.length === 0) return undefined;
  const sender = textField(message.from_uid);
  const label = names.get(sender) ?? sender;
  return label.length === 0 ? text : `${label}: ${text}`;
}

/** Inner text of one forwarded message; nested media and forwards never expand further. */
function forwardedMessageText(payload: JsonRecord): string {
  if (payload.type === MERGE_FORWARD_MESSAGE_TYPE) return MERGE_FORWARD_HEADER;
  const content = messageContent(payload);
  const text = content === undefined ? "" : content.text.trim();
  return text.length > 0 ? text : mediaPlaceholder(payload.type);
}

/**
 * Type 14 keeps the server-generated `plain` as the authoritative text, while the blocks
 * still decide which images and files travel with the message. Without `plain` (old
 * clients, or a client-sent payload the server has not rewritten) the blocks are
 * flattened with `[图片]` standing in for every embedded image.
 */
function richTextContent(payload: JsonRecord): ExtractedContent {
  const attachments: InboundAttachment[] = [];
  const parts: string[] = [];
  if (Array.isArray(payload.content)) {
    const blocks: unknown[] = payload.content;
    for (const raw of blocks) {
      if (!isJsonRecord(raw)) continue;
      if (raw.type === RICH_TEXT_IMAGE_BLOCK) {
        pushAttachment(attachments, raw.url, "image", { name: raw.name, size: raw.size });
        parts.push(RICH_TEXT_IMAGE_PLACEHOLDER);
        continue;
      }
      // 未知块按 octo-lib 的宽容解析处理：有 text 就取 text，其余忽略（写入端 write-strict
      // 会拒绝未知块类型，所以这里只可能是前向兼容的降级路径）。
      const text = textField(raw.text);
      if (text.length > 0) parts.push(text);
    }
  } else {
    // Before blocks existed, `content` was the whole text.
    const legacy = textField(payload.content);
    if (legacy.length > 0) parts.push(legacy);
  }
  const plain = textField(payload.plain);
  return {
    text: plain.length > 0 ? plain : truncateCodePoints(parts.join(""), MAX_RICH_TEXT_CODE_POINTS),
    attachments,
  };
}

/**
 * Type table for one inbound payload: the text and media a message contributes. The text
 * is exactly what the client or server wrote (no mention stripping here — that applies to
 * the extracted text afterwards), and `undefined` means the type yields no usable input,
 * which is what keeps the "unsupported message type" refusal for stickers and notifications.
 */
function messageContent(payload: JsonRecord): ExtractedContent | undefined {
  const attachments: InboundAttachment[] = [];
  switch (payload.type) {
    case TEXT_MESSAGE_TYPE:
      return { text: textField(payload.content), attachments };
    case IMAGE_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "image", { name: payload.name, size: payload.size });
      return { text: captionOrContent(payload), attachments };
    case GIF_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "image", { name: payload.name });
      return { text: "", attachments };
    case VOICE_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "audio", {
        name: payload.name,
        durationSeconds: declaredDuration(payload),
      });
      return { text: "", attachments };
    case VIDEO_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "video", {
        name: payload.name,
        size: payload.size,
        durationSeconds: declaredDuration(payload),
      });
      return { text: "", attachments };
    case LOCATION_MESSAGE_TYPE:
      return { text: locationText(payload), attachments };
    case CONTACT_CARD_MESSAGE_TYPE:
      return { text: contactText(payload), attachments };
    case FILE_MESSAGE_TYPE:
    case FILE_ALIAS_MESSAGE_TYPE:
      pushAttachment(attachments, payload.url, "file", { name: fileName(payload), size: payload.size });
      return { text: captionOrContent(payload), attachments };
    case MERGE_FORWARD_MESSAGE_TYPE:
      return { text: mergeForwardText(payload), attachments };
    case RICH_TEXT_MESSAGE_TYPE:
      return richTextContent(payload);
    case INTERACTIVE_CARD_MESSAGE_TYPE: {
      // 服务端保证 `plain` 非空；真为空时给占位符，避免把空正文交给 Agent。
      const plain = textField(payload.plain);
      return { text: plain.length > 0 ? plain : CARD_PLACEHOLDER, attachments };
    }
    default:
      return undefined;
  }
}

/**
 * A reply carries the quoted message inline: `payload.reply.payload` is a full payload of
 * its own (the real client sends the original type, blocks, and `plain`), so the same type
 * table reads it. Only depth 1 is read — a quoted message that itself quotes something
 * contributes just its own content — and the quoted text is bounded like rich text.
 */
function quotedMessage(
  payload: JsonRecord,
): { quote: InboundQuote; attachments: InboundAttachment[] } | null {
  const reply = payload.reply;
  if (!isJsonRecord(reply)) return null;
  const quotedPayload = reply.payload;
  if (!isJsonRecord(quotedPayload)) return null;
  const content = messageContent(quotedPayload);
  if (content === undefined) return null;
  // 引用里的相对 URL 与正文一样不可用：它们不参与「这条引用有没有内容」的判定。
  const attachments = content.attachments.filter(
    (attachment) => absoluteHttpUrl(attachment.url) !== undefined,
  );
  const text = truncateCodePoints(content.text, MAX_QUOTED_CODE_POINTS).trim();
  if (text.length === 0 && attachments.length === 0) return null;
  const senderName = reply.from_name;
  return {
    quote: { senderName: typeof senderName === "string" ? senderName : "", text },
    attachments,
  };
}

export function normalizedMessage(data: DecodedInbound, botUid: string, botName: string): InboundMessage {
  if (!(data.channelType === 1 || data.channelType === 2 || data.channelType === 5)) {
    throw new OctoError(`Octo message channel type ${data.channelType} is unsupported`);
  }
  if (!isJsonRecord(data.payload)) throw new OctoError("Octo message payload has invalid schema");
  const content = messageContent(data.payload);
  const quoted = quotedMessage(data.payload);
  // Unusable references never become attachments, so a message left with neither text nor
  // attachments stays unsupported — the same answer as before attachments existed. Text-shaped
  // payloads are the exception: `@bot` alone still runs a normal (empty) turn.
  const attachments: InboundAttachment[] = [];
  const seenUrls = new Set<string>();
  for (const attachment of [...content?.attachments ?? [], ...quoted?.attachments ?? []]) {
    const url = absoluteHttpUrl(attachment.url);
    // 引用消息里的同一张图不重复落盘；回复自己的附件时以本消息的条目为准。
    if (url === undefined || seenUrls.has(url)) continue;
    seenUrls.add(url);
    attachments.push(attachment);
  }
  const entities = mentionEntities(data.payload);
  const mentionedUserIds = mentionUids(data.payload);
  const text = stripBotMention(content?.text ?? "", botUid, botName, entities, mentionedUserIds);
  const payloadType = data.payload["type"];
  const textShaped = typeof payloadType === "number" && TEXT_SHAPED_MESSAGE_TYPES.has(payloadType);
  const supported = content !== undefined
    && (textShaped || text.trim().length > 0 || attachments.length > 0);
  const channelId = data.channelType === 1 ? data.senderId : data.rawChannelId;
  if (channelId.length === 0) throw new OctoError("Octo group message has no channel id");
  return {
    channelId,
    channelType: data.channelType as InboundMessage["channelType"],
    messageId: data.messageId.toString(),
    senderId: data.senderId,
    text,
    mentionedUserIds,
    attachments,
    quoted: quoted?.quote ?? null,
    supported,
    messageSeq: knownIntOrZero(data.messageSeq),
    messageTimestamp: knownIntOrZero(data.timestamp) * 1000,
  };
}

export function historyFromResponse(value: unknown, botUid: string): HistoryMessage[] {
  if (!isJsonRecord(value) || !Array.isArray(value.messages)) {
    throw new OctoError("Octo message sync response has invalid schema");
  }
  const messages: HistoryMessage[] = [];
  for (const entry of value.messages) {
    const message = historyMessage(entry, botUid);
    if (message) messages.push(message);
  }
  return messages.sort((left, right) => left.seq - right.seq);
}

function historyMessage(value: unknown, botUid: string): HistoryMessage | undefined {
  if (!isJsonRecord(value)) return undefined;
  const seq = value.message_seq;
  const senderId = value.from_uid;
  const timestamp = value.timestamp;
  const payload = value.payload;
  if (!Number.isSafeInteger(seq) || (seq as number) <= 0) return undefined;
  if (typeof senderId !== "string" || senderId.length === 0 || senderId === botUid) return undefined;
  if (!Number.isSafeInteger(timestamp) || (timestamp as number) <= 0) return undefined;
  if (typeof payload !== "string") return undefined;
  const envelope = decodedHistoryPayload(payload);
  if (!isJsonRecord(envelope)) return undefined;
  // Same type table as the live path. History itself never downloads anything, but an
  // attachment-bearing message keeps its absolute download reference so it survives as a
  // real history entry — otherwise a file that carries no caption vanishes from "recent
  // messages" altogether, and a later 「这个」 has nothing to associate with. Relative or
  // otherwise unusable URLs are dropped here, exactly like the live path drops them.
  const content = messageContent(envelope);
  const text = content?.text ?? "";
  const attachments = (content?.attachments ?? [])
    .filter((attachment) => absoluteHttpUrl(attachment.url) !== undefined)
    .map((attachment) => ({
      kind: attachment.kind,
      name: attachment.name,
      size: attachment.size,
      url: attachment.url,
    }));
  if (text.trim().length === 0 && attachments.length === 0) return undefined;
  const entry: HistoryMessage = {
    seq: seq as number,
    senderId,
    text,
    timestamp: (timestamp as number) * 1000,
  };
  // 无附件的消息不带这个键：形状与引入元数据之前完全一致。
  if (attachments.length > 0) entry.attachments = attachments;
  return entry;
}

function decodedHistoryPayload(payload: string): unknown {
  try {
    return JSON.parse(Buffer.from(payload, "base64").toString("utf8")) as unknown;
  } catch {
    return undefined;
  }
}
