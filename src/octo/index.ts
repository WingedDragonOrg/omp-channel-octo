import { createDecipheriv, createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import { basename, dirname, extname } from "node:path";
import { Readable } from "node:stream";
import { WebSocket } from "ws";
import { generateKeyPair, sharedKey } from "curve25519-js";
import { OctoError } from "./error.ts";
import {
  BOT_EVENT_MAX_LIMIT,
  BOT_EVENT_MAX_WAIT_SECONDS,
  BOT_EVENT_TIMEOUT_MARGIN_MS,
  CARD_PROFILE,
  CARD_VERSION,
  botEventId,
  cardActionFromEvent,
  cardFrameEnvelope,
  cardProfileCapabilities,
  cardProfileCapabilitiesFromApi,
  cardProfileSupported,
  executionCardEnvelope,
} from "./cards.ts";
import {
  MAX_GROUP_CONTEXT_BYTES,
  botApiRequest,
  contextFromResponse,
  contextPath,
  groupInfoFromResponse,
  groupPath,
  mentionPrefFromResponse,
  memberFromResponse,
  membersFromResponse,
  parseVersion,
  serverErrorCode,
  serverErrorMessage,
  threadFromResponse,
  threadPath,
  threadsFromResponse,
} from "./api.ts";
import {
  PresignedUpload,
  ATTACHMENT_PART_BYTES,
  MAX_HTTP_BODY_BYTES,
  HANDSHAKE_TIMEOUT_MS,
  requiredString,
  apiUrl,
  authHeaders,
  readBody,
  parseJson,
  parseJsonWithInt64,
  requestDeadline,
  storedBytes,
  requestWithTimeout,
  httpStatusError,
  responseUrl,
  webSocketUrl,
  imageInfo,
  readFileHeader,
} from "./http.ts";

import {
  IMAGE_MESSAGE_TYPE,
  FILE_MESSAGE_TYPE,
  TEXT_MESSAGE_TYPE,
  INTERACTIVE_CARD_MESSAGE_TYPE,
  MAX_TEXT_BYTES,
  normalizedMessage,
  historyFromResponse,
  absoluteHttpUrl,
  textField,
} from "./content.ts";

import {
  Registration,
  WireFrame,
  Curve25519KeyPair,
  CONNACK_PACKET,
  RECV_PACKET,
  PING_PACKET,
  PONG_PACKET,
  DISCONNECT_PACKET,
  PROTOCOL_VERSION,
  AES_BLOCK_BYTES,
  ByteReader,
  FrameDecoder,
  encodeControlFrame,
  toBytes,
  deriveAesKey,
  decryptPayload,
  encodeConnect,
  encodeRecvAck,
  decodeUtf8,
} from "./protocol.ts";

import type {
  AttachmentDownload,
  BotCardAction,
  BotEvent,
  BotEventFetch,
  BotIdentity,
  CardFrame,
  CardProfile,
  ChannelTransport,
  ChatTarget,
  DeleteThreadOptions,
  GroupContext,
  GroupInfo,
  GroupInfoPatch,
  GroupMember,
  GroupMentionPref,
  HistoryFetch,
  HistoryMessage,
  InboundAttachment,
  InboundAttachmentKind,
  InboundMessage,
  InboundQuote,
  SendReceipt,
  ThreadSummary,
} from "../contracts.ts";
import { ATTACHMENT_DOWNLOAD_TIMEOUT_MS, AttachmentError } from "../attachments.ts";
import { isJsonRecord, type JsonRecord } from "./type-guards.ts";
import { buildExecutionCard, type ExecutionCardCapabilities } from "../execution-card.ts";
import type { ExecutionSnapshot } from "../execution-tracker.ts";
import { threadParentGroupId } from "../thread-target.ts";

type Logger = (message: string) => void;

export type OctoTransportOptions = {
  apiUrl: string;
  botToken: string;
  wsUrl?: string;
  logger?: Logger;
};



type SocketState = {
  socket: WebSocket;
  decoder: FrameDecoder;
  clientKey: Curve25519KeyPair;
  authenticated: boolean;
  serverVersion: number;
  aesKey: Buffer | undefined;
  aesIv: Buffer | undefined;
  handshakeTimer: NodeJS.Timeout | undefined;
  pingTimer: NodeJS.Timeout | undefined;
  pongTimer: NodeJS.Timeout | undefined;
  closed: boolean;
};


/** Audio MIME types the transcription endpoint understands; anything else goes as opaque bytes. */
const AUDIO_CONTENT_TYPES: Record<string, string> = {
  ".amr": "audio/amr",
  ".m4a": "audio/mp4",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
  ".ogg": "audio/ogg",
  ".aac": "audio/aac",
};
/** Transcribing anything larger is pointless and would only buffer a big file in memory. */
const MAX_TRANSCRIBE_BYTES = 20 * 1024 * 1024;
/**
 * A dropped or kicked socket reconnects with an exponential backoff that doubles from the base
 * until the cap, jittered downward so several bots kicked at the same moment do not return in
 * lockstep. Only registration/authentication failures stop the transport for good.
 */
const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;
const RECONNECT_JITTER_MIN = 0.75;
const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;

/**
 * `POST /v1/bot/messages/sync` answers with base64-encoded JSON payloads that follow the
 * same shape as WS frames. Rows are skipped individually when they carry no extractable
 * text or come from the bot itself, so one malformed row cannot fail the whole page.
 */



export class OctoTransport implements ChannelTransport {
  private readonly apiBase: string;
  private readonly botToken: string;
  private readonly configuredWsUrl: string | undefined;
  private readonly logger: Logger;
  private registration: Registration | undefined;
  private started = false;
  private stopped = true;
  private startPromise: Promise<void> | undefined;
  private startResolve: (() => void) | undefined;
  private startReject: ((error: Error) => void) | undefined;
  private onMessage: ((message: InboundMessage) => Promise<void>) | undefined;
  private onOnline: (() => void) | undefined;
  private currentSocket: SocketState | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private reconnectAttempt = 0;
  private inboundChain: Promise<void> = Promise.resolve();
  private cardCapabilities: ExecutionCardCapabilities | undefined;

  constructor(options: OctoTransportOptions) {
    if (!options.apiUrl || !options.botToken) throw new OctoError("Octo API URL and bot token are required");
    this.apiBase = options.apiUrl;
    this.botToken = options.botToken;
    this.configuredWsUrl = options.wsUrl;
    this.logger = options.logger ?? (() => {});
  }

  async initialize(): Promise<BotIdentity> {
    const registration = await this.registerBot();
    this.registration = registration;
    return {
      uid: registration.uid,
      name: registration.name,
      ...(registration.ownerUid === undefined ? {} : { ownerUid: registration.ownerUid }),
    };
  }

  start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void> {
    if (!this.registration) return Promise.reject(new OctoError("Octo transport must initialize before start"));
    if (this.startPromise) return this.startPromise;
    this.started = true;
    this.stopped = false;
    this.onMessage = onMessage;
    this.onOnline = onOnline;
    const deferred = Promise.withResolvers<void>();
    this.startPromise = deferred.promise;
    this.startResolve = deferred.resolve;
    this.startReject = deferred.reject;
    void this.openSocket(false);
    return deferred.promise;
  }

  async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    validateTarget(target);
    if (Buffer.byteLength(text, "utf8") > MAX_TEXT_BYTES) {
      throw new OctoError(`Octo text message exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
    }
    return this.sendMessage(target, { type: TEXT_MESSAGE_TYPE, content: text }, clientMsgNo, signal);
  }

  async editText(
    target: ChatTarget,
    messageId: string,
    text: string,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo text message id is required");
    if (Buffer.byteLength(text, "utf8") > MAX_TEXT_BYTES) {
      throw new OctoError(`Octo text message exceeds ${MAX_TEXT_BYTES} UTF-8 bytes`);
    }
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/message/edit"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          message_id: messageId,
          channel_id: target.channelId,
          channel_type: target.channelType,
          content_edit: JSON.stringify({ type: TEXT_MESSAGE_TYPE, content: text }),
        }),
      },
      "message edit",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("message edit", response.status);
    }
  }

  async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    validateTarget(target);
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/typing"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
        }),
      },
      "typing",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("typing", response.status);
  }

  async sendProgressCard(
    target: ChatTarget,
    progress: ExecutionSnapshot,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    const capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal);
    this.cardCapabilities = capabilities ?? undefined;
    if (!capabilities) return null;
    const content = executionCardEnvelope(progress, capabilities);
    if (!content) return null;
    return this.sendMessage(target, content, clientMsgNo, signal);
  }

  async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ExecutionSnapshot,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo progress card message id is required");
    if (!Number.isSafeInteger(cardSeq) || cardSeq <= 0) {
      throw new OctoError("Octo progress card sequence must be a positive safe integer");
    }
    let capabilities = this.cardCapabilities;
    if (!capabilities) {
      capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal) ?? undefined;
      this.cardCapabilities = capabilities;
    }
    if (!capabilities) {
      throw new OctoError("Octo execution card profile does not support progress cards");
    }
    const content = executionCardEnvelope(progress, capabilities, {
      cardSeq,
      ...(progress.state === "running" ? { transient: true } : {}),
    });
    if (!content) {
      throw new OctoError("Octo execution card profile does not support this progress card");
    }
    await this.postCardEdit(target, messageId, content, signal);
  }

  async sendCard(
    target: ChatTarget,
    frame: CardFrame,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    validateTarget(target);
    const capabilities = await this.cardCapabilitiesFor(signal);
    if (!capabilities || !cardProfileSupported(capabilities, frame.profile)) return null;
    const content = cardFrameEnvelope(frame, capabilities);
    if (!content) return null;
    return this.sendMessage(target, content, clientMsgNo, signal);
  }

  async editCard(
    target: ChatTarget,
    messageId: string,
    frame: CardFrame,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    validateTarget(target);
    if (messageId.length === 0) throw new OctoError("Octo card message id is required");
    if (!Number.isSafeInteger(cardSeq) || cardSeq <= 0) {
      throw new OctoError("Octo card sequence must be a positive safe integer");
    }
    const capabilities = await this.cardCapabilitiesFor(signal);
    if (!capabilities || !cardProfileSupported(capabilities, frame.profile)) {
      throw new OctoError("Octo card profile does not support this card");
    }
    const content = cardFrameEnvelope(frame, capabilities, { cardSeq });
    if (!content) throw new OctoError("Octo card profile does not support this card");
    await this.postCardEdit(target, messageId, content, signal);
  }

  async fetchBotEvents(fetch: BotEventFetch): Promise<BotEvent[] | null> {
    const afterEventId = Number.isSafeInteger(fetch.afterEventId) && fetch.afterEventId > 0
      ? fetch.afterEventId
      : 0;
    const limit = Math.min(Math.max(Math.trunc(fetch.limit), 1), BOT_EVENT_MAX_LIMIT);
    const waitSeconds = Math.min(Math.max(Math.trunc(fetch.waitSeconds), 0), BOT_EVENT_MAX_WAIT_SECONDS);
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/events"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: afterEventId, limit, wait: waitSeconds }),
      },
      "bot events",
      fetch.signal,
      MAX_HTTP_BODY_BYTES,
      waitSeconds * 1000 + BOT_EVENT_TIMEOUT_MARGIN_MS,
    );
    if (response.status === 404) return null;
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("bot events", response.status);
    }
    const value = parseJsonWithInt64(response.body, "bot events");
    if (!isJsonRecord(value) || !Array.isArray(value.results)) {
      throw new OctoError("Octo bot events response has invalid schema");
    }
    const events: BotEvent[] = [];
    for (const entry of value.results) {
      if (!isJsonRecord(entry)) continue;
      const eventId = botEventId(entry.event_id);
      if (eventId === null) continue;
      const action = cardActionFromEvent(entry);
      events.push(action ? { eventId, kind: "card_action", action } : { eventId, kind: "other" });
    }
    return events;
  }

  async ackBotEvent(eventId: number, signal?: AbortSignal): Promise<void> {
    if (!Number.isSafeInteger(eventId) || eventId < 0) {
      throw new OctoError("Octo bot event id must be a non-negative safe integer");
    }
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, `/v1/bot/events/${eventId}/ack`),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: "{}",
      },
      "bot event ack",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("bot event ack", response.status);
    }
  }

  /**
   * Card capability snapshot. A probe that answered is reused for the life of the process; a
   * failed probe is retried, so a transient outage cannot degrade the chat to text-only cards.
   */
  private async cardCapabilitiesFor(signal?: AbortSignal): Promise<ExecutionCardCapabilities | undefined> {
    if (this.cardCapabilities) return this.cardCapabilities;
    const capabilities = await cardProfileCapabilitiesFromApi(this.apiBase, this.botToken, signal);
    if (capabilities) this.cardCapabilities = capabilities;
    return capabilities ?? undefined;
  }

  /** Shared card edit path: progress and ask cards differ only in the frame they build. */
  private async postCardEdit(
    target: ChatTarget,
    messageId: string,
    content: JsonRecord,
    signal?: AbortSignal,
  ): Promise<void> {
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/message/edit"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          message_id: messageId,
          channel_id: target.channelId,
          channel_type: target.channelType,
          content_edit: JSON.stringify(content),
        }),
      },
      "message edit",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) {
      throw httpStatusError("message edit", response.status);
    }
  }

  async fetchGroupMessages(target: ChatTarget, fetch: HistoryFetch): Promise<HistoryMessage[]> {
    validateTarget(target);
    if (target.channelType === 1) throw new OctoError("DM does not support group message history");
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/messages/sync"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
          start_message_seq: fetch.sinceSeq,
          end_message_seq: fetch.untilSeq,
          limit: fetch.limit,
          pull_mode: 1,
        }),
      },
      "message sync",
      fetch.signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("message sync", response.status);
    return historyFromResponse(parseJson(response.body, "message sync"), this.registration?.uid ?? "");
  }

  /**
   * Streams one inbound attachment to `path` and returns the stored byte count. A message
   * that was already accepted once reuses the file left by that delivery instead of
   * downloading again, and a failed transfer leaves neither a partial file nor a temp file.
   */
  async downloadAttachment(request: AttachmentDownload): Promise<number> {
    const stored = await storedBytes(request.path);
    if (stored > 0) return stored;
    const url = absoluteHttpUrl(request.url);
    if (url === undefined) {
      throw new AttachmentError("failed", "Octo attachment URL is not an absolute http(s) URL");
    }
    const deadline = requestDeadline(request.signal, ATTACHMENT_DOWNLOAD_TIMEOUT_MS);
    const temporary = `${request.path}.part-${randomBytes(ATTACHMENT_PART_BYTES).toString("hex")}`;
    try {
      await mkdir(dirname(request.path), { recursive: true, mode: 0o700 });
      const response = await fetch(url, { signal: deadline.signal, redirect: "follow" });
      if (response.status < 200 || response.status >= 300) {
        throw new AttachmentError("failed", `Octo attachment download answered HTTP ${response.status}`);
      }
      const declared = Number(response.headers.get("content-length"));
      if (Number.isFinite(declared) && declared > request.maxBytes) {
        throw new AttachmentError(
          "oversize",
          `Octo attachment declares ${declared} bytes, over the ${request.maxBytes} byte limit`,
        );
      }
      if (!response.body) throw new AttachmentError("failed", "Octo attachment response has no body");
      const handle = await open(temporary, "w", 0o600);
      let written = 0;
      try {
        const reader = response.body.getReader();
        while (true) {
          const chunk = await reader.read();
          if (chunk.done) break;
          written += chunk.value.byteLength;
          if (written > request.maxBytes) {
            throw new AttachmentError("oversize", `Octo attachment exceeds the ${request.maxBytes} byte limit`);
          }
          let offset = 0;
          while (offset < chunk.value.byteLength) {
            const { bytesWritten } = await handle.write(
              chunk.value,
              offset,
              chunk.value.byteLength - offset,
            );
            if (bytesWritten <= 0) throw new AttachmentError("failed", "Octo attachment write stalled");
            offset += bytesWritten;
          }
        }
      } finally {
        await handle.close();
      }
      // Only a fully written file becomes the attachment a task prompt may point at.
      await rename(temporary, request.path);
      return written;
    } catch (error) {
      await rm(temporary, { force: true }).catch(() => undefined);
      if (error instanceof AttachmentError) throw error;
      if (deadline.timedOut() || deadline.aborted()) {
        throw new AttachmentError("timeout", "Octo attachment download timed out");
      }
      throw new AttachmentError("failed", "Octo attachment download failed");
    } finally {
      deadline.release();
    }
  }

  /**
   * Best-effort server-side transcription of a saved audio file. The endpoint forwards to
   * the deployment's ASR service; when that service is missing or unhappy the audio file
   * still reaches the agent, so every failure degrades to "no transcription".
   */
  async transcribeAudio(path: string, signal?: AbortSignal): Promise<string | null> {
    const deadline = requestDeadline(signal, ATTACHMENT_DOWNLOAD_TIMEOUT_MS);
    try {
      // Real voice notes are small; anything past the cap is not worth buffering into memory
      // and no ASR service would accept it.
      if ((await stat(path)).size > MAX_TRANSCRIBE_BYTES) return null;
      const audio = await readFile(path);
      const name = basename(path);
      const contentType = AUDIO_CONTENT_TYPES[extname(name).toLowerCase()] ?? "application/octet-stream";
      const form = new FormData();
      form.append("mode", "edit");
      form.append("audio", new Blob([audio], { type: contentType }), name);
      const response = await fetch(apiUrl(this.apiBase, "/v1/bot/voice/transcribe"), {
        method: "POST",
        headers: authHeaders(this.botToken),
        body: form,
        signal: deadline.signal,
      });
      // Drain the body either way: an unread stream would hold the connection open.
      const body = await readBody(response, MAX_HTTP_BODY_BYTES);
      if (response.status < 200 || response.status >= 300) return null;
      const value = parseJson(body, "voice transcribe");
      if (!isJsonRecord(value)) return null;
      const text = textField(value.text);
      return text.trim().length > 0 ? text : null;
    } catch {
      return null;
    } finally {
      deadline.release();
    }
  }

  async getGroupContext(target: ChatTarget): Promise<GroupContext | null> {
    validateTarget(target);
    if (target.channelType === 1) return null;
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, contextPath(target.channelId, target.channelType)),
      { method: "GET", headers: authHeaders(this.botToken) },
      "group context",
      undefined,
      MAX_GROUP_CONTEXT_BYTES,
    );
    if (response.status === 404) {
      return { present: false, content: "", version: 0, updatedAt: null, updatedBy: "" };
    }
    if (response.status < 200 || response.status >= 300) throw httpStatusError("group context", response.status);
    return contextFromResponse(parseJson(response.body, "group context"));
  }

  async updateGroupContext(target: ChatTarget, content: string, signal?: AbortSignal): Promise<{ version: number }> {
    validateTarget(target);
    if (target.channelType === 1) throw new OctoError("DM does not support group context updates");
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, contextPath(target.channelId, target.channelType)),
      {
        method: "PUT",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({ content }),
      },
      "group context update",
      signal,
      MAX_GROUP_CONTEXT_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("group context update", response.status);
    return parseVersion(parseJson(response.body, "group context update"));
  }

  async getGroupInfo(groupId: string, signal?: AbortSignal): Promise<GroupInfo> {
    return groupInfoFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group info",
        groupPath(groupId),
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getGroupMentionPref(groupId: string, signal?: AbortSignal): Promise<GroupMentionPref> {
    return mentionPrefFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group mention preference",
        `${groupPath(groupId)}/mention_pref`,
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getGroupMembers(groupId: string, signal?: AbortSignal): Promise<GroupMember[]> {
    return membersFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "group members",
        `${groupPath(groupId)}/members`,
        "GET",
        undefined,
        signal,
      ),
      "group members",
    );
  }

  async updateGroupInfo(groupId: string, patch: GroupInfoPatch, signal?: AbortSignal): Promise<void> {
    const body: JsonRecord = {};
    if (patch.name !== undefined) body.name = patch.name;
    if (patch.notice !== undefined) body.notice = patch.notice;
    if (Object.keys(body).length === 0) {
      throw new OctoError("Octo group update requires a name or a notice", { permanent: true });
    }
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "group info update",
      `${groupPath(groupId)}/info`,
      "PUT",
      body,
      signal,
    );
  }

  async listThreads(groupId: string, signal?: AbortSignal): Promise<ThreadSummary[]> {
    return threadsFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread list",
        `${groupPath(groupId)}/threads`,
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async createThread(groupId: string, name: string, signal?: AbortSignal): Promise<ThreadSummary> {
    return threadFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread create",
        `${groupPath(groupId)}/threads`,
        "POST",
        { name },
        signal,
      ),
    );
  }

  async getThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<ThreadSummary> {
    return threadFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread info",
        threadPath(groupId, shortId),
        "GET",
        undefined,
        signal,
      ),
    );
  }

  async getThreadMembers(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
  ): Promise<GroupMember[]> {
    return membersFromResponse(
      await botApiRequest(
        this.apiBase,
        this.botToken,
        "thread members",
        `${threadPath(groupId, shortId)}/members`,
        "GET",
        undefined,
        signal,
      ),
      "thread members",
    );
  }

  async joinThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread join",
      `${threadPath(groupId, shortId)}/join`,
      "POST",
      undefined,
      signal,
    );
  }

  async leaveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread leave",
      `${threadPath(groupId, shortId)}/leave`,
      "POST",
      undefined,
      signal,
    );
  }

  async deleteThread(
    groupId: string,
    shortId: string,
    signal?: AbortSignal,
    options: DeleteThreadOptions = {},
  ): Promise<void> {
    await botApiRequest(
      this.apiBase,
      this.botToken,
      "thread delete",
      threadPath(groupId, shortId),
      "DELETE",
      undefined,
      signal,
      options,
    );
  }

  async sendFile(
    target: ChatTarget,
    path: string,
    kind: "image" | "file",
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    validateTarget(target);
    if (kind !== "image" && kind !== "file") {
      throw new OctoError("Octo file kind is unsupported", { permanent: true });
    }
    const file = await stat(path);
    if (!file.isFile()) throw new OctoError("Octo file path is not a regular file", { permanent: true });
    if (!Number.isSafeInteger(file.size) || file.size < 0) throw new OctoError("Octo file size is invalid", { permanent: true });
    const name = basename(path);
    if (name.length === 0) throw new OctoError("Octo file name is empty", { permanent: true });
    const header = await readFileHeader(path);
    const detected = imageInfo(header);
    if (kind === "image" && !detected) {
      throw new OctoError("Octo image bytes have an unsupported MIME format or invalid dimensions", { permanent: true });
    }
    const contentType = kind === "image" ? detected!.contentType : "application/octet-stream";
    const presigned = await this.requestPresigned(name, file.size, contentType, signal);
    await this.uploadFile(path, file.size, presigned, signal);
    const payload: JsonRecord = {
      type: kind === "image" ? IMAGE_MESSAGE_TYPE : FILE_MESSAGE_TYPE,
      url: presigned.downloadUrl,
      name,
      size: file.size,
    };
    if (kind === "image") {
      payload.width = detected!.width;
      payload.height = detected!.height;
    }
    return this.sendMessage(target, payload, clientMsgNo, signal);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.cardCapabilities = undefined;
    this.started = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const socket = this.currentSocket;
    this.currentSocket = undefined;
    if (socket) await this.closeSocketAndWait(socket);
    const reject = this.startReject;
    this.startReject = undefined;
    this.startResolve = undefined;
    this.startPromise = undefined;
    if (reject) reject(new OctoError("Octo transport stopped"));
  }

  private async registerBot(): Promise<Registration> {
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/register"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: "{}",
      },
      "bot register",
      undefined,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("bot register", response.status);
    const value = parseJson(response.body, "bot register");
    if (!isJsonRecord(value)) throw new OctoError("Octo bot register response has invalid schema");
    const uid = requiredString(value.robot_id, "robot_id");
    const name = typeof value.name === "string" && value.name.length > 0 ? value.name : uid;
    const imToken = requiredString(value.im_token, "im_token");
    const ownerUid = typeof value.owner_uid === "string" && value.owner_uid.trim().length > 0
      ? value.owner_uid.trim()
      : undefined;
    const advertisedWsUrl = typeof value.ws_url === "string" ? value.ws_url : undefined;
    const wsUrl = this.configuredWsUrl ?? requiredString(advertisedWsUrl, "ws_url");
    webSocketUrl(wsUrl, "ws_url");
    return {
      uid,
      name,
      imToken,
      wsUrl,
      ...(ownerUid === undefined ? {} : { ownerUid }),
    };
  }

  private async requestPresigned(
    name: string,
    size: number,
    contentType: string,
    signal: AbortSignal | undefined,
  ): Promise<PresignedUpload> {
    const url = new URL(apiUrl(this.apiBase, "/v1/bot/upload/presigned"));
    url.searchParams.set("filename", name);
    url.searchParams.set("fileSize", String(size));
    url.searchParams.set("contentType", contentType);
    const response = await requestWithTimeout(
      url.toString(),
      { method: "GET", headers: authHeaders(this.botToken) },
      "presigned upload",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("presigned upload", response.status);
    const value = parseJson(response.body, "presigned upload");
    if (!isJsonRecord(value)) throw new OctoError("Octo presigned upload response has invalid schema");
    return {
      uploadUrl: responseUrl(value.uploadUrl, "uploadUrl"),
      downloadUrl: responseUrl(value.downloadUrl, "downloadUrl"),
      contentType: requiredString(value.contentType, "contentType"),
      contentDisposition: typeof value.contentDisposition === "string" ? value.contentDisposition : undefined,
    };
  }

  private async uploadFile(
    path: string,
    size: number,
    presigned: PresignedUpload,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const stream = createReadStream(path);
    const headers: Record<string, string> = {
      "Content-Type": presigned.contentType,
      "Content-Length": String(size),
      ...(presigned.contentDisposition ? { "Content-Disposition": presigned.contentDisposition } : {}),
    };
    const response = await requestWithTimeout(
      presigned.uploadUrl,
      {
        method: "PUT",
        headers,
        body: Readable.toWeb(stream) as unknown as BodyInit,
        duplex: "half",
      } as RequestInit & { duplex: "half" },
      "presigned upload transfer",
      signal,
      MAX_HTTP_BODY_BYTES,
    ).catch((error: unknown) => {
      stream.destroy();
      throw error;
    });
    if (response.status < 200 || response.status >= 300) throw httpStatusError("presigned upload transfer", response.status);
  }

  private async sendMessage(
    target: ChatTarget,
    payload: JsonRecord,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const response = await requestWithTimeout(
      apiUrl(this.apiBase, "/v1/bot/sendMessage"),
      {
        method: "POST",
        headers: { ...authHeaders(this.botToken), "Content-Type": "application/json" },
        body: JSON.stringify({
          channel_id: target.channelId,
          channel_type: target.channelType,
          payload,
          client_msg_no: clientMsgNo,
        }),
      },
      "send message",
      signal,
      MAX_HTTP_BODY_BYTES,
    );
    if (response.status < 200 || response.status >= 300) throw httpStatusError("send message", response.status);
    const value = parseJsonWithInt64(response.body, "send message");
    if (!isJsonRecord(value)) throw new OctoError("Octo send message response has invalid schema");
    if (value.message_id === undefined || value.message_id === null) return {};
    if (typeof value.message_id !== "string" && typeof value.message_id !== "number") {
      throw new OctoError("Octo send message response has invalid message_id");
    }
    return { messageId: String(value.message_id) };
  }

  private async openSocket(reconnecting: boolean): Promise<void> {
    if (this.stopped || !this.registration) return;
    if (reconnecting) {
      try {
        this.registration = await this.registerBot();
        if (this.stopped) return;
      } catch (error) {
        if (this.isPermanent(error)) {
          this.failPermanently(error);
          return;
        }
        this.logger(this.safeError(error, "Octo re-registration failed"));
        this.scheduleReconnect();
        return;
      }
    }
    const registration = this.registration;
    let ws: WebSocket;
    try {
      ws = new WebSocket(registration.wsUrl);
    } catch (error) {
      this.logger(this.safeError(error, "Octo WebSocket connection failed"));
      this.scheduleReconnect();
      return;
    }
    ws.binaryType = "arraybuffer";
    const state: SocketState = {
      socket: ws,
      decoder: new FrameDecoder(),
      clientKey: generateKeyPair(randomBytes(32)),
      authenticated: false,
      serverVersion: PROTOCOL_VERSION,
      aesKey: undefined,
      aesIv: undefined,
      handshakeTimer: undefined,
      pingTimer: undefined,
      pongTimer: undefined,
      closed: false,
    };
    this.currentSocket = state;
    state.handshakeTimer = setTimeout(() => {
      this.logger("Octo WebSocket handshake timed out");
      this.closeSocket(state);
    }, HANDSHAKE_TIMEOUT_MS);
    ws.on("open", () => {
      if (this.currentSocket !== state || this.stopped) return;
      try {
        ws.send(encodeConnect(registration, state.clientKey));
      } catch (error) {
        this.logger(this.safeError(error, "Octo WebSocket handshake failed"));
        this.closeSocket(state);
      }
    });
    ws.on("message", (data) => {
      if (this.currentSocket !== state || this.stopped) return;
      try {
        const frames = state.decoder.push(toBytes(data));
        for (const frame of frames) this.handleFrame(state, frame);
      } catch (error) {
        this.logger(this.safeError(error, "Octo WebSocket frame failed"));
        this.closeSocket(state);
      }
    });
    ws.on("error", (error) => {
      if (this.currentSocket === state && !this.stopped) this.logger(this.safeError(error, "Octo WebSocket error"));
    });
    ws.on("close", () => {
      this.finalizeSocket(state);
    });
  }
  private handleFrame(state: SocketState, frame: WireFrame): void {
    switch (frame.packetType) {
      case CONNACK_PACKET:
        this.handleConnack(state, frame);
        return;
      case RECV_PACKET:
        if (!state.authenticated || !state.aesKey || !state.aesIv) return;
        this.handleRecv(state, frame.body);
        return;
      case PING_PACKET:
        if (state.socket.readyState === WebSocket.OPEN) state.socket.send(encodeControlFrame(PONG_PACKET));
        return;
      case PONG_PACKET:
        clearTimeout(state.pongTimer);
        state.pongTimer = undefined;
        return;
      case DISCONNECT_PACKET:
        this.handleDisconnect(state, frame.body);
        return;
      default:
        throw new OctoError(`Octo WebSocket packet type ${frame.packetType} is unsupported`);
    }
  }

  private handleConnack(state: SocketState, frame: WireFrame): void {
    if (state.authenticated) throw new OctoError("Octo WebSocket sent duplicate CONNACK");
    const body = new ByteReader(frame.body);
    const hasServerVersion = (frame.flags & 0x01) !== 0;
    const serverVersion = hasServerVersion ? body.byte() : 0;
    void body.int64();
    const reason = body.byte();
    if (reason !== 1) {
      const message = reason === 0
        ? "Octo WebSocket was kicked while connecting; another login holds this bot session"
        : `Octo WebSocket authentication failed with reason ${reason}`;
      this.failPermanently(new OctoError(message, { permanent: true }));
      return;
    }
    const serverKeyText = body.string();
    const saltText = body.string();
    if (serverVersion >= 4) void body.int64();
    let serverKey: Uint8Array;
    try {
      serverKey = Uint8Array.from(Buffer.from(serverKeyText, "base64"));
    } catch {
      throw new OctoError("Octo CONNACK server key is invalid");
    }
    if (serverKey.byteLength !== 32) throw new OctoError("Octo CONNACK server key has invalid length");
    const iv = Buffer.from(saltText, "utf8");
    if (iv.byteLength < AES_BLOCK_BYTES) throw new OctoError("Octo CONNACK salt has invalid length");
    state.serverVersion = serverVersion;
    state.aesKey = deriveAesKey(state.clientKey.private, serverKey);
    state.aesIv = iv.subarray(0, AES_BLOCK_BYTES);
    state.authenticated = true;
    clearTimeout(state.handshakeTimer);
    state.handshakeTimer = undefined;
    clearInterval(state.pingTimer);
    state.pingTimer = setInterval(() => {
      if (state.socket.readyState !== WebSocket.OPEN) {
        this.closeSocket(state);
        return;
      }
      if (state.pongTimer !== undefined) {
        this.logger("Octo WebSocket heartbeat timed out");
        this.closeSocket(state);
        return;
      }
      state.pongTimer = setTimeout(() => {
        state.pongTimer = undefined;
        this.closeSocket(state);
      }, PONG_TIMEOUT_MS);
      state.socket.send(encodeControlFrame(PING_PACKET));
    }, PING_INTERVAL_MS);
    this.reconnectAttempt = 0;
    const resolve = this.startResolve;
    if (resolve) {
      this.startResolve = undefined;
      this.startReject = undefined;
      resolve();
    }
    try {
      this.onOnline?.();
    } catch (error) {
      this.logger(this.safeError(error, "Octo online callback failed"));
    }
  }
  /**
   * The server kicks a live connection when another login for the same bot takes over, when the
   * session expires, or when the cluster moves the connection. That is not an authentication
   * failure: drop the socket and let the reconnect schedule re-register with backoff.
   */
  private handleDisconnect(state: SocketState, frameBody: Uint8Array): void {
    let reason = 0;
    let detail = "";
    try {
      const reader = new ByteReader(frameBody);
      if (frameBody.byteLength > 0) reason = reader.byte();
      if (reason !== 0) detail = reader.string();
    } catch {
      reason = 0;
      detail = "";
    }
    const message = reason === 0
      ? "Octo WebSocket was kicked by the server; reconnecting"
      : `Octo WebSocket disconnected with reason ${reason}; reconnecting`;
    this.logger(detail.length > 0 ? `${message} (${detail})` : message);
    this.closeSocket(state);
  }

  private handleRecv(state: SocketState, frameBody: Uint8Array): void {
    const reader = new ByteReader(frameBody);
    let header = "";
    try {
      const setting = reader.byte();
      void reader.string();
      const senderId = reader.string();
      const rawChannelId = reader.string();
      const channelType = reader.byte();
      header = `channelType=${channelType} sender=${senderId} channel=${rawChannelId}`;
      if (channelType === 1
        && rawChannelId.includes("@")
        && !rawChannelId.includes(this.registration?.uid ?? "")) {
        this.logger(`Octo DM channel id failed the bot identity check (${header})`);
        return;
      }
      if (state.serverVersion >= 3) void reader.int32();
      void reader.string();
      const messageId = reader.int64();
      const messageSeq = reader.int32();
      const timestamp = reader.int32();
      if ((setting & 0x08) !== 0) void reader.string();
      const payload = decryptPayload(reader.bytes(), state.aesKey!, state.aesIv!);
      const normalized = normalizedMessage(
        { setting, senderId, rawChannelId, channelType, messageId, messageSeq, timestamp, payload },
        this.registration?.uid ?? "",
        this.registration?.name ?? "",
      );
      const handler = this.onMessage;
      if (!handler) return;
      this.inboundChain = this.inboundChain.then(async () => {
        try {
          await handler(normalized);
          if (state.socket.readyState === WebSocket.OPEN && this.currentSocket === state) {
            state.socket.send(encodeRecvAck(messageId, messageSeq));
          }
        } catch (error) {
          this.logger(this.safeError(error, `Octo inbound handler failed for message ${messageId.toString()}`));
        }
      });
    } catch (error) {
      const detail = error instanceof OctoError ? error.message : "Octo inbound message is invalid";
      this.logger(header.length > 0 ? `${detail} (${header})` : detail);
    }
  }

  private closeSocket(state: SocketState): void {
    if (state.closed) return;
    this.clearSocketTimers(state);
    try {
      if (state.socket.readyState === WebSocket.OPEN || state.socket.readyState === WebSocket.CONNECTING) {
        state.socket.close();
      } else {
        this.finalizeSocket(state);
      }
    } catch {
      this.finalizeSocket(state);
    }
  }
  private async closeSocketAndWait(state: SocketState): Promise<void> {
    if (state.closed || state.socket.readyState === WebSocket.CLOSED) {
      state.closed = true;
      return;
    }
    const deferred = Promise.withResolvers<void>();
    let done = false;
    let timer: NodeJS.Timeout | undefined;
    const finish = (): void => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      state.socket.off("close", finish);
      deferred.resolve();
    };
    state.socket.once("close", finish);
    this.closeSocket(state);
    if (!done) {
      timer = setTimeout(() => {
        state.socket.terminate();
        this.finalizeSocket(state);
        finish();
      }, 1_000);
      await deferred.promise;
    }
  }

  private finalizeSocket(state: SocketState): void {
    if (state.closed) return;
    state.closed = true;
    this.clearSocketTimers(state);
    if (this.currentSocket !== state) return;
    this.currentSocket = undefined;
    if (!this.stopped) this.scheduleReconnect();
  }

  private clearSocketTimers(state: SocketState): void {
    clearTimeout(state.handshakeTimer);
    clearInterval(state.pingTimer);
    clearTimeout(state.pongTimer);
    state.handshakeTimer = undefined;
    state.pingTimer = undefined;
    state.pongTimer = undefined;
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) return;
    const exponential = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** Math.min(this.reconnectAttempt, 20));
    const delay = Math.round(exponential * (RECONNECT_JITTER_MIN + Math.random() * (1 - RECONNECT_JITTER_MIN)));
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.openSocket(true);
    }, delay);
  }

  private failPermanently(error: unknown): void {
    const octoError = error instanceof OctoError ? error : new OctoError("Octo transport authentication failed", { permanent: true });
    this.stopped = true;
    this.cardCapabilities = undefined;
    this.started = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    const state = this.currentSocket;
    this.currentSocket = undefined;
    if (state) this.closeSocket(state);
    const reject = this.startReject;
    this.startReject = undefined;
    this.startResolve = undefined;
    if (reject) reject(octoError);
    else this.logger(octoError.message);
  }

  private isPermanent(error: unknown): boolean {
    return error instanceof OctoError && error.permanent;
  }

  private safeError(error: unknown, fallback: string): string {
    if (error instanceof OctoError) return error.message;
    return fallback;
  }
}

function validateTarget(target: ChatTarget): void {
  if (!target || typeof target.channelId !== "string" || target.channelId.length === 0) {
    throw new OctoError("Octo target channel id is required");
  }
  const channelType = target.channelType as number;
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) {
    throw new OctoError("Octo target channel type is unsupported");
  }
}

export function createOctoTransport(options: OctoTransportOptions): ChannelTransport {
  return new OctoTransport(options);
}
