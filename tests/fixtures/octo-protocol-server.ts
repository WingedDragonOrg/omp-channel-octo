import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { createCipheriv, createHash, randomBytes } from "node:crypto";
import { WebSocketServer, WebSocket, type RawData } from "ws";
import { generateKeyPair, sharedKey } from "curve25519-js";

const BOT_TOKEN = "bf-test-token";
const BOT_UID = "octo-bot-uid";
const BOT_NAME = "Octo Test Bot";
const OWNER_UID = "owner-user-uid";
const OWNER_CHANNEL_ID = OWNER_UID;
const SERVER_VERSION = 4;
const AES_SALT = "octo-test-salt!!";
const FIRST_MESSAGE_ID = 9_007_199_254_740_993n;

const DEFAULT_CARD_PROFILE: FixtureCardProfile = {
  enabled: true,
  profiles: ["octo/v1", "octo/v2"],
  card_version: "1.5",
  elements: [
    "TextBlock",
    "RichTextBlock",
    "Image",
    "ImageSet",
    "Container",
    "ColumnSet",
    "FactSet",
    "Table",
    "ActionSet",
  ],
  actions: ["Action.OpenUrl", "Action.ToggleVisibility", "Action.CopyToClipboard"],
  config: {
    card_enabled: true,
    display_enabled: true,
    interaction_enabled: false,
    reasoning_enabled: true,
    reasoning_template_ref: { id: "progress", version: "1" },
  },
};
async function sleep(ms: number): Promise<void> {
  if (ms <= 0) return;
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  await promise;
}


export interface FixtureConnectPacket {
  version: number;
  deviceFlag: number;
  deviceId: string;
  uid: string;
  token: string;
  clientTimestamp: bigint;
  clientKey: string;
}

export interface FixtureAck {
  messageId: string;
  messageSeq: number;
}

export interface FixtureConnection {
  socket: WebSocket;
  wsPath: string;
  connect?: FixtureConnectPacket;
  ready: boolean;
  closed: boolean;
  recvAcks: FixtureAck[];
  aesKey?: string;
  aesIv?: string;
}

export interface FixtureGroupContext {
  content: string;
  present?: boolean;
  version: number;
  updatedAt: string | null;
  updatedBy: string;
}

export interface FixtureGroupContextRequest {
  path: string;
  target: string;
  authorization: string | undefined;
}

export interface FixtureGroupContextUpdate {
  path: string;
  target: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
}

export interface FixtureManagementRequest {
  method: string;
  path: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
  /** Client-side port of the TCP connection this request arrived on; new port = new connection. */
  remotePort: number | undefined;
}

/** Thread record returned by the fixture's group/thread management routes. */
export interface FixtureManagementThread {
  short_id: string;
  group_no: string;
  channel_id: string;
  channel_type: number;
  name: string;
  creator_uid: string;
  status: number;
  member_count: number;
  message_count: number;
  created_at: string;
  updated_at: string;
}

type ManagementRoute =
  | { kind: "group-info"; groupId: string }
  | { kind: "group-members"; groupId: string }
  | { kind: "group-info-update"; groupId: string }
  | { kind: "group-mention-pref"; groupId: string }
  | { kind: "user-info"; uid: string }
  | { kind: "thread-list"; groupId: string }
  | { kind: "thread-create"; groupId: string }
  | { kind: "thread-info"; groupId: string; shortId: string }
  | { kind: "thread-delete"; groupId: string; shortId: string }
  | { kind: "thread-members"; groupId: string; shortId: string }
  | { kind: "thread-join"; groupId: string; shortId: string }
  | { kind: "thread-leave"; groupId: string; shortId: string };

const FIXTURE_MANAGEMENT_CREATED_AT = "2026-09-12T00:00:00.000Z";
const FIXTURE_GROUP_NAME = "Fixture Group";

export interface FixtureSentMessage {
  authorization: string | undefined;
  body: Record<string, unknown>;
}
export interface FixtureTypingRequest {
  path: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
}

export interface FixtureCardProfileRequest {
  path: string;
  authorization: string | undefined;
}
export interface FixtureMessageEdit {
  path: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
}

export interface FixtureMessageSyncRequest {
  path: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
}

export interface FixtureBotEventRequest {
  path: string;
  authorization: string | undefined;
  body: Record<string, unknown>;
}

export type FixtureCardProfile = Record<string, unknown>;


export interface FixturePresignRequest {
  path: string;
  authorization: string | undefined;
  filename: string;
  fileSize: number;
  contentType: string | undefined;
}

export interface FixtureUploadRequest {
  path: string;
  authorization: string | undefined;
  contentType: string | undefined;
  contentLength: string | undefined;
  contentDisposition: string | undefined;
  body: Buffer;
  accepted: boolean;
}

export interface FixtureTranscriptionRequest {
  path: string;
  authorization: string | undefined;
  contentType: string | undefined;
  /** Raw multipart body, so tests can assert the `mode` and `audio` parts. */
  body: Buffer;
}

export interface OctoProtocolFixtureOptions {
  /** HTTP response statuses for successive register calls. */
  registerStatuses?: number[];
  /** One status used when registerStatuses is exhausted. */
  registerStatus?: number;
  /** Tokens issued by successive successful registrations. */
  registrationTokens?: string[];
  /** CONNACK reason codes for successive WebSocket connections. */
  connackReasonCodes?: number[];
  /** Delay before the fixture sends CONNACK, in milliseconds. */
  connackDelayMs?: number;
  /** Query suffix advertised in the register response's ws_url. */
  advertisedWsQuery?: string;
  /** Context response keyed by group id or full thread target. Null means 404. */
  groupContexts?: Record<string, FixtureGroupContext | null>;
  /** Optional status override keyed by group id or full thread target. */
  groupContextStatuses?: Record<string, number>;
  /** Optional raw response body keyed by group id or full thread target. */
  groupContextBodies?: Record<string, unknown>;
  /** Status overrides for context update keyed by group id or full thread target. */
  groupContextPutStatuses?: Record<string, number>;
  /** Versions returned from successful context updates. */
  groupContextPutVersions?: Record<string, number>;
  /** Status override for bot group/thread management routes, keyed by `${METHOD} ${pathname}`. */
  managementStatuses?: Record<string, number>;
  /** Raw response body for bot group/thread management routes, keyed by `${METHOD} ${pathname}`. */
  managementBodies?: Record<string, unknown>;
  /** Status for presigned URL discovery. */
  presignStatus?: number;
  /** Offset applied only to the upload server's expected byte count. */
  uploadExpectedSizeDelta?: number;
  /** Content-Type returned by presign when supplied. */
  presignedContentType?: string;
  /** Content-Disposition returned by presign when supplied. */
  presignedContentDisposition?: string;
  /** Omit optional Content-Disposition from the presign response. */
  omitPresignedContentDisposition?: boolean;
  /** Ignore client heartbeat packets to exercise the client's pong watchdog. */
  ignorePong?: boolean;
  /** Response status for GET /v1/bot/card/profile. */
  cardProfileStatus?: number;
  /** Raw capability manifest returned by GET /v1/bot/card/profile. */
  cardProfileBody?: FixtureCardProfile;
  /** Response status for POST /v1/bot/typing. */
  typingStatus?: number;
  /** Response status for POST /v1/bot/events. */
  botEventsStatus?: number;
  /** `results` arrays returned by POST /v1/bot/events, consumed in call order. */
  botEventBatches?: unknown[][];
  /** Response status for POST /v1/bot/events/:id/ack. */
  botEventAckStatus?: number;
  /** Response status for POST /v1/bot/message/edit. */
  messageEditStatus?: number;
  /**
   * Raw body answered by POST /v1/bot/message/edit when `messageEditStatus` is a failure, with
   * `messageEditErrorContentType` as its type. Omitted keeps the fixture's JSON error envelope;
   * a page body reproduces what a deployment's edge answers on its way to the bot API.
   */
  messageEditErrorBody?: string;
  messageEditErrorContentType?: string;
  /** Delay before responding to POST /v1/bot/sendMessage. */
  sendMessageDelayMs?: number;
  /** Delay before responding to POST /v1/bot/message/edit. */
  messageEditDelayMs?: number;
  /** Delay before responding to POST /v1/bot/messages/sync. */
  messageSyncDelayMs?: number;
}


export interface SendInboundOptions {
  /** Raw WebSocket-message chunk sizes; omitted means one complete message. */
  fragmentSizes?: number[];
}

export interface FixtureInboundMessage {
  fromUid: string;
  channelId?: string;
  channelType: 1 | 2 | 5;
  text: string;
  mention?: Record<string, unknown>;
  /**
   * Full payload override. When supplied it replaces the default `{ type: 1, content }`
   * body and `text` is ignored; a separately supplied `mention` is still merged on top.
   */
  payload?: Record<string, unknown>;
  messageId?: string;
  messageSeq?: number;
  timestamp?: number;
}

interface Frame {
  packetType: number;
  body: Uint8Array;
}

class Writer {
  private readonly bytes: number[] = [];

  byte(value: number): this {
    this.bytes.push(value & 0xff);
    return this;
  }

  int32(value: number): this {
    const buf = Buffer.allocUnsafe(4);
    buf.writeUInt32BE(value >>> 0);
    this.bytes.push(...buf);
    return this;
  }

  int64(value: bigint): this {
    const buf = Buffer.allocUnsafe(8);
    buf.writeBigUInt64BE(value);
    this.bytes.push(...buf);
    return this;
  }

  string(value: string): this {
    const encoded = Buffer.from(value, "utf8");
    if (encoded.byteLength > 0xffff) {
      throw new Error(`fixture string exceeds WuKongIM uint16 length: ${encoded.byteLength}`);
    }
    const len = Buffer.allocUnsafe(2);
    len.writeUInt16BE(encoded.byteLength);
    this.bytes.push(...len, ...encoded);
    return this;
  }

  raw(value: Uint8Array): this {
    this.bytes.push(...value);
    return this;
  }

  finish(): Uint8Array {
    return Uint8Array.from(this.bytes);
  }
}

class Reader {
  private offset = 0;

  constructor(private readonly data: Uint8Array) {}

  byte(): number {
    this.require(1);
    return this.data[this.offset++]!;
  }

  int32(): number {
    this.require(4);
    const value = new DataView(this.data.buffer, this.data.byteOffset + this.offset, 4).getUint32(0);
    this.offset += 4;
    return value;
  }

  int64(): bigint {
    this.require(8);
    const value = Buffer.from(this.data.buffer, this.data.byteOffset + this.offset, 8).readBigUInt64BE();
    this.offset += 8;
    return value;
  }

  string(): string {
    const length = this.int16();
    this.require(length);
    const value = Buffer.from(this.data.buffer, this.data.byteOffset + this.offset, length).toString("utf8");
    this.offset += length;
    return value;
  }

  remaining(): Uint8Array {
    return this.data.slice(this.offset);
  }

  private int16(): number {
    this.require(2);
    const value = new DataView(this.data.buffer, this.data.byteOffset + this.offset, 2).getUint16(0);
    this.offset += 2;
    return value;
  }

  private require(length: number): void {
    if (this.offset + length > this.data.byteLength) {
      throw new Error(`truncated fixture packet at ${this.offset}, need ${length} bytes`);
    }
  }
}

function encodeVariableLength(length: number): number[] {
  const bytes: number[] = [];
  do {
    let digit = length % 0x80;
    length = Math.floor(length / 0x80);
    if (length > 0) digit |= 0x80;
    bytes.push(digit);
  } while (length > 0);
  return bytes;
}

function encodeFrame(packetType: number, body: Uint8Array): Uint8Array {
  return Uint8Array.from([
    packetType << 4,
    ...encodeVariableLength(body.byteLength),
    ...body,
  ]);
}

function readVariableLength(data: Uint8Array, start: number): { value: number; next: number } | undefined {
  let value = 0;
  let multiplier = 1;
  let offset = start;
  for (let i = 0; i < 5; i += 1) {
    if (offset >= data.byteLength) return undefined;
    const digit = data[offset++]!;
    value += (digit & 0x7f) * multiplier;
    if ((digit & 0x80) === 0) return { value, next: offset };
    multiplier *= 0x80;
  }
  throw new Error("fixture variable-length integer exceeds WuKongIM limit");
}

function decodeFrames(data: Uint8Array): Frame[] {
  const frames: Frame[] = [];
  let offset = 0;
  while (offset < data.byteLength) {
    const header = data[offset++]!;
    const packetType = header >> 4;
    if (packetType === 7 || packetType === 8) {
      frames.push({ packetType, body: new Uint8Array() });
      continue;
    }
    const remaining = readVariableLength(data, offset);
    if (!remaining) throw new Error("fixture received incomplete frame length");
    offset = remaining.next;
    if (offset + remaining.value > data.byteLength) {
      throw new Error("fixture received incomplete frame body");
    }
    frames.push({
      packetType,
      body: data.slice(offset, offset + remaining.value),
    });
    offset += remaining.value;
  }
  return frames;
}

function rawToBytes(data: RawData): Uint8Array {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (Array.isArray(data)) {
    const buffers = data.map((part) => Buffer.from(part));
    return Uint8Array.from(Buffer.concat(buffers));
  }
  return Uint8Array.from(data);
}

function encryptPayload(payload: Record<string, unknown>, aesKey: string, aesIv: string): Uint8Array {
  const cipher = createCipheriv("aes-128-cbc", Buffer.from(aesKey, "utf8"), Buffer.from(aesIv, "utf8"));
  const ciphertext = Buffer.concat([
    cipher.update(JSON.stringify(payload), "utf8"),
    cipher.final(),
  ]).toString("base64");
  return Buffer.from(ciphertext, "utf8");
}

function connectPacketFromBody(body: Uint8Array): FixtureConnectPacket {
  const reader = new Reader(body);
  return {
    version: reader.byte(),
    deviceFlag: reader.byte(),
    deviceId: reader.string(),
    uid: reader.string(),
    token: reader.string(),
    clientTimestamp: reader.int64(),
    clientKey: reader.string(),
  };
}

function deriveAesKey(clientKey: string, serverPrivateKey: Uint8Array): string {
  const clientPublicKey = Uint8Array.from(Buffer.from(clientKey, "base64"));
  const secret = sharedKey(serverPrivateKey, clientPublicKey);
  return createHash("md5")
    .update(Buffer.from(secret).toString("base64"), "utf8")
    .digest("hex")
    .slice(0, 16);
}


function encodeConnackFrame(reasonCode: number, serverPublicKey: string): Uint8Array {
  const body = new Writer()
    .byte(SERVER_VERSION)
    .int64(0n)
    .byte(reasonCode)
    .string(serverPublicKey)
    .string(AES_SALT)
    .int64(1n)
    .finish();
  return Uint8Array.from([
    (2 << 4) | 0x01,
    ...encodeVariableLength(body.byteLength),
    ...body,
  ]);
}

function encodeInboundFrame(
  connection: FixtureConnection,
  message: FixtureInboundMessage,
): Uint8Array {
  if (!connection.connect || !connection.ready || !connection.aesKey || !connection.aesIv) {
    throw new Error("fixture cannot send an inbound message before CONNACK");
  }
  const messageId = message.messageId ?? "9007199254740993";
  const messageSeq = message.messageSeq ?? 1;
  const timestamp = message.timestamp ?? Math.floor(Date.now() / 1000);
  const payload: Record<string, unknown> = message.payload
    ? { ...message.payload, ...(message.mention ? { mention: message.mention } : {}) }
    : {
      type: 1,
      content: message.text,
      ...(message.mention ? { mention: message.mention } : {}),
    };
  const body = new Writer()
    .byte(0)
    .string("")
    .string(message.fromUid)
    .string(message.channelId ?? "")
    .byte(message.channelType)
    .int32(0)
    .string("")
    .int64(BigInt(messageId))
    .int32(messageSeq)
    .int32(timestamp)
    .raw(encryptPayload(payload, connection.aesKey, connection.aesIv))
    .finish();
  return encodeFrame(5, body);
}

function decodeRecvack(body: Uint8Array): FixtureAck {
  const reader = new Reader(body);
  return {
    messageId: reader.int64().toString(),
    messageSeq: reader.int32(),
  };
}

export class OctoProtocolFixture {
  readonly httpServer: Server;
  readonly wsServer: WebSocketServer;
  readonly registerRequests: Array<{
    path: string;
    authorization: string | undefined;
    body: Record<string, unknown>;
  }> = [];
  readonly sentMessages: FixtureSentMessage[] = [];
  readonly typingRequests: FixtureTypingRequest[] = [];
  readonly cardProfileRequests: FixtureCardProfileRequest[] = [];
  readonly messageEdits: FixtureMessageEdit[] = [];
  readonly messagesSyncRequests: FixtureMessageSyncRequest[] = [];
  readonly botEventRequests: FixtureBotEventRequest[] = [];
  readonly botEventAcks: Array<{ path: string; authorization: string | undefined }> = [];
  /** Latest type-1 text body accepted by message_id. */
  readonly latestEditedTextByMessageId = new Map<string, string>();

  readonly groupContextRequests: FixtureGroupContextRequest[] = [];
  readonly groupContextUpdates: FixtureGroupContextUpdate[] = [];
  readonly managementRequests: FixtureManagementRequest[] = [];
  /** uid → 显示名；未登记的 uid 与线上的「查无此人」一样返回 HTTP 500。 */
  readonly userNames = new Map<string, string>();
  readonly presignRequests: FixturePresignRequest[] = [];
  readonly uploadRequests: FixtureUploadRequest[] = [];
  readonly transcriptionRequests: FixtureTranscriptionRequest[] = [];
  readonly downloadRequests: Array<{ path: string; authorization: string | undefined }> = [];
  readonly connections: FixtureConnection[] = [];
  readonly apiUrl: string;
  readonly wsUrl: string;
  readonly botToken = BOT_TOKEN;
  readonly botUid = BOT_UID;
  readonly botName = BOT_NAME;
  readonly ownerUid = OWNER_UID;
  private readonly groupContexts: Map<string, FixtureGroupContext | null>;
  private readonly groupContextBodies: Map<string, unknown>;
  private readonly options: OctoProtocolFixtureOptions;
  private readonly issuedTokens: string[] = [];
  private readonly messageIdsByClientNo = new Map<string, string>();
  private readonly presignedUploads = new Map<string, {
    fileSize: number;
    contentType: string;
    contentDisposition: string | undefined;
  }>();
  private readonly downloads = new Map<string, {
    body: Buffer;
    contentType: string;
    status: number;
    location: string | undefined;
  }>();
  private registerCount = 0;
  private uploadCount = 0;
  private nextMessageId = FIRST_MESSAGE_ID;
  private readonly managedGroupInfo = new Map<string, { name: string; notice: string }>();
  private readonly managedThreads = new Map<string, FixtureManagementThread[]>();
  private nextThreadSequence = 0;
  private messageSyncStatus = 200;
  private messageSyncBody: unknown = { messages: [] };
  private transcriptionStatus = 200;
  private transcriptionBody: unknown = { status: 200, text: "fixture transcription" };
  private started = false;

  private constructor(options: OctoProtocolFixtureOptions, httpServer: Server, wsServer: WebSocketServer, port: number) {
    this.options = options;
    this.groupContexts = new Map(Object.entries(options.groupContexts ?? {}));
    this.groupContextBodies = new Map(Object.entries(options.groupContextBodies ?? {}));
    this.httpServer = httpServer;
    this.wsServer = wsServer;
    this.apiUrl = `http://127.0.0.1:${port}`;
    this.wsUrl = `ws://127.0.0.1:${port}`;
  }

  static async start(options: OctoProtocolFixtureOptions = {}): Promise<OctoProtocolFixture> {
    const httpServer = createServer();
    const wsServer = new WebSocketServer({ server: httpServer });
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    httpServer.once("error", reject);
    httpServer.listen(0, "127.0.0.1", () => resolve());
    await promise;
    const address = httpServer.address();
    if (!address || typeof address === "string") {
      throw new Error("fixture HTTP server did not expose a TCP port");
    }
    const fixture = new OctoProtocolFixture(options, httpServer, wsServer, address.port);
    fixture.installHandlers();
    fixture.started = true;
    return fixture;
  }


  async close(): Promise<void> {
    for (const connection of this.connections) {
      if (connection.socket.readyState === WebSocket.OPEN || connection.socket.readyState === WebSocket.CONNECTING) {
        connection.socket.terminate();
      }
    }
    if (this.started) {
      const wsClosed = Promise.withResolvers<void>();
      this.wsServer.close(() => wsClosed.resolve());
      const server = this.httpServer as Server & {
        closeIdleConnections?: () => void;
        closeAllConnections?: () => void;
      };
      server.closeIdleConnections?.();
      server.closeAllConnections?.();
      const httpClosed = Promise.withResolvers<void>();
      server.close(() => httpClosed.resolve());
      await Promise.all([wsClosed.promise, httpClosed.promise]);
    }
    this.started = false;
  }

  /**
   * Send a DISCONNECT frame (packet type 9) over a live connection, the way the server does when it
   * kicks a session: one reason byte plus the reason text WuKongIM carries along.
   */
  kickConnection(index = 0, reason = 12, detail = "connect kicked"): void {
    const connection = this.connections[index];
    if (!connection || connection.socket.readyState !== WebSocket.OPEN) {
      throw new Error("fixture connection is not open");
    }
    const body = new Writer().byte(reason).string(detail).finish();
    connection.socket.send(encodeFrame(9, body));
  }

  closeConnection(index = 0): void {
    const connection = this.connections[index];
    if (!connection) throw new Error(`fixture connection ${index} does not exist`);
    connection.socket.close();
  }
  sendDisconnect(connectionOrIndex: FixtureConnection | number, reasonCode = 0): void {
    const connection = typeof connectionOrIndex === "number"
      ? this.connections[connectionOrIndex]
      : connectionOrIndex;
    if (!connection) throw new Error("fixture connection does not exist");
    const frame = encodeFrame(9, new Writer().byte(reasonCode).string("").finish());
    connection.socket.send(frame);
  }

  sendInbound(connectionOrIndex: FixtureConnection | number, message: FixtureInboundMessage, options: SendInboundOptions = {}): void {
    this.sendInboundBatch(connectionOrIndex, [message], options);
  }

  sendInboundBatch(
    connectionOrIndex: FixtureConnection | number,
    messages: FixtureInboundMessage[],
    options: SendInboundOptions = {},
  ): void {
    const connection = typeof connectionOrIndex === "number"
      ? this.connections[connectionOrIndex]
      : connectionOrIndex;
    if (!connection) throw new Error("fixture connection does not exist");
    const frame = Buffer.concat(messages.map((message) => Buffer.from(encodeInboundFrame(connection, message))));
    this.sendChunks(connection.socket, frame, options.fragmentSizes);
  }

  async waitForConnection(count: number, timeoutMs = 2_000): Promise<FixtureConnection> {
    const connection = await waitForFixture(
      () => this.connections[count - 1],
      timeoutMs,
      `fixture did not receive WebSocket connection ${count}`,
    );
    return connection;
  }
  getLatestEditedText(messageId: string): string | undefined {
    return this.latestEditedTextByMessageId.get(messageId);
  }

  /**
   * Response returned by POST /v1/bot/messages/sync. Objects are JSON-serialized; a string
   * is written verbatim so tests can exercise malformed-JSON handling.
   */
  setMessagesSync(body: unknown, status = 200): void {
    this.messageSyncBody = body;
    this.messageSyncStatus = status;
  }

  /**
   * Registers the body served by GET /fixture-download/<key>. Unregistered keys answer 404;
   * a non-200 status answers with that status and an empty body, plus a `location` header when
   * one is supplied so tests can exercise redirect following.
   */
  serveDownload(
    key: string,
    options: { body: Uint8Array | string; contentType?: string; status?: number; location?: string },
  ): void {
    this.downloads.set(key, {
      body: typeof options.body === "string" ? Buffer.from(options.body, "utf8") : Buffer.from(options.body),
      contentType: options.contentType ?? "application/octet-stream",
      status: options.status ?? 200,
      location: options.location,
    });
  }

  /**
   * Response returned by POST /v1/bot/voice/transcribe; defaults to `{ status: 200, text }`.
   * Objects are JSON-serialized; a string is written verbatim so tests can exercise malformed-JSON
   * handling. Non-2xx statuses answer with a JSON error body instead.
   */
  setTranscription(body: unknown, status = 200): void {
    this.transcriptionBody = body;
    this.transcriptionStatus = status;
  }

  private managementRouteFor(method: string, url: URL): ManagementRoute | null {
    const pathname = url.pathname;
    const group = /^\/v1\/bot\/groups\/([^/]+)$/.exec(pathname);
    if (group?.[1] !== undefined && method === "GET") {
      return { kind: "group-info", groupId: decodeURIComponent(group[1]) };
    }
    const members = /^\/v1\/bot\/groups\/([^/]+)\/members$/.exec(pathname);
    if (members?.[1] !== undefined && method === "GET") {
      return { kind: "group-members", groupId: decodeURIComponent(members[1]) };
    }
    const info = /^\/v1\/bot\/groups\/([^/]+)\/info$/.exec(pathname);
    if (info?.[1] !== undefined && method === "PUT") {
      return { kind: "group-info-update", groupId: decodeURIComponent(info[1]) };
    }
    const pref = /^\/v1\/bot\/groups\/([^/]+)\/mention_pref$/.exec(pathname);
    if (pref?.[1] !== undefined && method === "GET") {
      return { kind: "group-mention-pref", groupId: decodeURIComponent(pref[1]) };
    }
    if (pathname === "/v1/bot/user/info" && method === "GET") {
      return { kind: "user-info", uid: url.searchParams.get("uid") ?? "" };
    }
    const threads = /^\/v1\/bot\/groups\/([^/]+)\/threads$/.exec(pathname);
    if (threads?.[1] !== undefined) {
      if (method === "GET") return { kind: "thread-list", groupId: decodeURIComponent(threads[1]) };
      if (method === "POST") return { kind: "thread-create", groupId: decodeURIComponent(threads[1]) };
      return null;
    }
    const thread = /^\/v1\/bot\/groups\/([^/]+)\/threads\/([^/]+)$/.exec(pathname);
    if (thread?.[1] !== undefined && thread[2] !== undefined) {
      const groupId = decodeURIComponent(thread[1]);
      const shortId = decodeURIComponent(thread[2]);
      if (method === "GET") return { kind: "thread-info", groupId, shortId };
      if (method === "DELETE") return { kind: "thread-delete", groupId, shortId };
      return null;
    }
    const action = /^\/v1\/bot\/groups\/([^/]+)\/threads\/([^/]+)\/(members|join|leave)$/.exec(pathname);
    if (action?.[1] !== undefined && action[2] !== undefined) {
      const groupId = decodeURIComponent(action[1]);
      const shortId = decodeURIComponent(action[2]);
      if (method === "GET" && action[3] === "members") return { kind: "thread-members", groupId, shortId };
      if (method === "POST" && action[3] === "join") return { kind: "thread-join", groupId, shortId };
      if (method === "POST" && action[3] === "leave") return { kind: "thread-leave", groupId, shortId };
    }
    return null;
  }

  private threadOf(groupId: string, shortId: string): FixtureManagementThread | undefined {
    return (this.managedThreads.get(groupId) ?? []).find((thread) => thread.short_id === shortId);
  }

  private respondManagement(
    route: ManagementRoute,
    body: Record<string, unknown>,
    response: ServerResponse,
  ): void {
    const groupId = "groupId" in route ? route.groupId : "";
    const stored = this.managedGroupInfo.get(groupId);
    switch (route.kind) {
      case "group-info": {
        writeJson(response, 200, {
          group_no: groupId,
          name: stored?.name ?? FIXTURE_GROUP_NAME,
          notice: stored?.notice ?? "",
          creator: OWNER_UID,
          status: 1,
          created_at: FIXTURE_MANAGEMENT_CREATED_AT,
        });
        return;
      }
      case "group-members": {
        writeJson(response, 200, [
          { uid: OWNER_UID, name: "Fixture Owner", role: 1, robot: 0, created_at: FIXTURE_MANAGEMENT_CREATED_AT },
          { uid: BOT_UID, name: BOT_NAME, role: 0, robot: 1, created_at: FIXTURE_MANAGEMENT_CREATED_AT },
        ]);
        return;
      }
      case "group-mention-pref": {
        // 默认与群开关允许、bot owner 尚未开启的线上默认一致：仍需 @Bot。
        writeJson(response, 200, { no_mention: 0, group_allow_no_mention: 1, effective: false });
        return;
      }
      case "user-info": {
        const name = this.userNames.get(route.uid);
        if (name === undefined) {
          // 线上查不到/查不了的用户是 HTTP 失败，不是空对象。
          writeJson(response, 500, {
            error: { code: "err.server.bot_api.query_failed", message: "服务器内部错误。" },
            msg: "服务器内部错误。",
            status: 500,
          });
          return;
        }
        writeJson(response, 200, {
          uid: route.uid,
          name,
          avatar: `https://fixture.invalid/users/${route.uid}/avatar`,
        });
        return;
      }
      case "group-info-update": {
        const name = typeof body.name === "string" ? body.name : undefined;
        const notice = typeof body.notice === "string" ? body.notice : undefined;
        if (name === undefined && notice === undefined) {
          writeJson(response, 400, { error: { message: "请求参数有误。" }, msg: "请求参数有误。" });
          return;
        }
        this.managedGroupInfo.set(groupId, {
          name: name ?? stored?.name ?? FIXTURE_GROUP_NAME,
          notice: notice ?? stored?.notice ?? "",
        });
        writeJson(response, 200, { ok: true });
        return;
      }
      case "thread-list": {
        writeJson(response, 200, this.managedThreads.get(groupId) ?? []);
        return;
      }
      case "thread-create": {
        const shortId = `thread-${this.nextThreadSequence}`;
        this.nextThreadSequence += 1;
        const thread: FixtureManagementThread = {
          short_id: shortId,
          group_no: groupId,
          channel_id: `${groupId}____${shortId}`,
          channel_type: 5,
          name: typeof body.name === "string" ? body.name : "",
          creator_uid: BOT_UID,
          status: 1,
          member_count: 1,
          message_count: 0,
          created_at: FIXTURE_MANAGEMENT_CREATED_AT,
          updated_at: FIXTURE_MANAGEMENT_CREATED_AT,
        };
        this.managedThreads.set(groupId, [...(this.managedThreads.get(groupId) ?? []), thread]);
        writeJson(response, 200, thread);
        return;
      }
      case "thread-info": {
        const thread = this.threadOf(groupId, route.shortId);
        if (!thread) {
          writeJson(response, 400, {
            error: { code: "err.server.bot_api.request_invalid", message: "请求参数有误。" },
            msg: "请求参数有误。",
            status: 400,
          });
          return;
        }
        writeJson(response, 200, thread);
        return;
      }
      case "thread-delete": {
        this.managedThreads.set(
          groupId,
          (this.managedThreads.get(groupId) ?? []).filter((thread) => thread.short_id !== route.shortId),
        );
        writeJson(response, 200, { status: 200 });
        return;
      }
      case "thread-members": {
        writeJson(response, 200, [
          { uid: BOT_UID, name: BOT_NAME, role: 1, created_at: FIXTURE_MANAGEMENT_CREATED_AT },
        ]);
        return;
      }
      default: {
        writeJson(response, 200, { status: 200 });
        return;
      }
    }
  }


  private installHandlers(): void {
    this.httpServer.on("request", (request, response) => {
      void this.handleHttpRequest(request, response);
    });
    this.wsServer.on("connection", (socket, request) => {
      const connection: FixtureConnection = {
        socket,
        wsPath: request.url ?? "/",
        ready: false,
        closed: false,
        recvAcks: [],
      };
      this.connections.push(connection);
      socket.on("message", (data) => {
        this.handleClientFrames(connection, rawToBytes(data));
      });
      socket.on("close", () => {
        connection.closed = true;
      });
    });
  }

  private async handleHttpRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", this.apiUrl);
    const rawRequestBody = await readRawBody(request);
    const uploadPath = request.method === "PUT" && url.pathname.startsWith("/fixture-upload/");
    const transcriptionPath = request.method === "POST" && url.pathname === "/v1/bot/voice/transcribe";
    const parsedBody = uploadPath || transcriptionPath
      ? {}
      : rawRequestBody.byteLength > 0
        ? JSON.parse(rawRequestBody.toString("utf8")) as Record<string, unknown>
        : {};
    const authorization = request.headers.authorization;

    if (request.method === "GET" && url.pathname.startsWith("/fixture-download/")) {
      const key = decodeURIComponent(url.pathname.slice("/fixture-download/".length));
      this.downloadRequests.push({ path: `${url.pathname}${url.search}`, authorization });
      const download = this.downloads.get(key);
      if (!download) {
        writeJson(response, 404, { error: "not found" });
        return;
      }
      if (download.status !== 200) {
        response.writeHead(
          download.status,
          download.location === undefined ? {} : { location: download.location },
        );
        response.end();
        return;
      }
      response.writeHead(200, {
        "content-type": download.contentType,
        "content-length": String(download.body.byteLength),
      });
      response.end(download.body);
      return;
    }

    if (request.method === "POST" && url.pathname === "/v1/bot/register") {
      this.registerRequests.push({ path: `${url.pathname}${url.search}`, authorization, body: parsedBody });
      this.registerCount += 1;
      const status = authorization === `Bearer ${BOT_TOKEN}`
        ? this.options.registerStatuses?.[this.registerCount - 1]
          ?? this.options.registerStatus
          ?? 200
        : 401;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "invalid bot token" });
        return;
      }
      const token = this.options.registrationTokens?.[this.registerCount - 1]
        ?? `im-token-${this.registerCount}`;
      this.issuedTokens.push(token);
      writeJson(response, 200, {
        robot_id: BOT_UID,
        name: BOT_NAME,
        im_token: token,
        ws_url: `${this.wsUrl}${this.options.advertisedWsQuery ?? ""}`,
        api_url: this.apiUrl,
        owner_uid: OWNER_UID,
        owner_channel_id: OWNER_CHANNEL_ID,
      });
      return;
    }

    if (request.method === "GET" && url.pathname === "/v1/bot/upload/presigned") {
      const filename = url.searchParams.get("filename") ?? "";
      const fileSize = Number(url.searchParams.get("fileSize") ?? "NaN");
      const requestedContentType = url.searchParams.get("contentType") ?? undefined;
      this.presignRequests.push({
        path: `${url.pathname}${url.search}`,
        authorization,
        filename,
        fileSize,
        contentType: requestedContentType,
      });
      const status = authorization === `Bearer ${BOT_TOKEN}`
        ? this.options.presignStatus ?? 200
        : 401;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "presign failed" });
        return;
      }
      const uploadPath = `/fixture-upload/${this.uploadCount++}`;
      const contentType = this.options.presignedContentType
        ?? requestedContentType
        ?? "application/octet-stream";
      const contentDisposition = this.options.omitPresignedContentDisposition
        ? undefined
        : this.options.presignedContentDisposition ?? `inline; filename="${filename}"`;
      this.presignedUploads.set(uploadPath, {
        fileSize: fileSize + (this.options.uploadExpectedSizeDelta ?? 0),
        contentType,
        contentDisposition,
      });
      writeJson(response, 200, {
        uploadUrl: `${this.apiUrl}${uploadPath}`,
        downloadUrl: `${this.apiUrl}/fixture-download/${this.uploadCount}`,
        contentType,
        ...(contentDisposition === undefined ? {} : { contentDisposition }),
      });
      return;
    }

    if (uploadPath) {
      const expected = this.presignedUploads.get(url.pathname);
      const contentType = typeof request.headers["content-type"] === "string"
        ? request.headers["content-type"]
        : undefined;
      const contentLength = typeof request.headers["content-length"] === "string"
        ? request.headers["content-length"]
        : undefined;
      const contentDisposition = typeof request.headers["content-disposition"] === "string"
        ? request.headers["content-disposition"]
        : undefined;
      const accepted = authorization === undefined
        && expected !== undefined
        && contentType === expected.contentType
        && contentLength === String(expected.fileSize)
        && contentDisposition === expected.contentDisposition
        && rawRequestBody.byteLength === expected.fileSize;
      this.uploadRequests.push({
        path: url.pathname,
        authorization,
        contentType,
        contentLength,
        contentDisposition,
        body: rawRequestBody,
        accepted,
      });
      if (!accepted) {
        writeJson(response, 403, { error: "presigned upload headers or size mismatch" });
        return;
      }
      writeJson(response, 200, { ok: true });
      return;
    }

    if (transcriptionPath) {
      this.transcriptionRequests.push({
        path: url.pathname,
        authorization,
        contentType: typeof request.headers["content-type"] === "string"
          ? request.headers["content-type"]
          : undefined,
        body: rawRequestBody,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      if (this.transcriptionStatus < 200 || this.transcriptionStatus >= 300) {
        writeJson(response, this.transcriptionStatus, { error: "transcription failed" });
        return;
      }
      response.writeHead(this.transcriptionStatus, { "content-type": "application/json" });
      response.end(
        typeof this.transcriptionBody === "string"
          ? this.transcriptionBody
          : JSON.stringify(this.transcriptionBody),
      );
      return;
    }

    if (request.method === "GET" && url.pathname === "/v1/bot/card/profile") {
      this.cardProfileRequests.push({
        path: `${url.pathname}${url.search}`,
        authorization,
      });
      const status = authorization === `Bearer ${BOT_TOKEN}`
        ? this.options.cardProfileStatus ?? 200
        : 401;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "card profile failed" });
        return;
      }
      writeJson(response, 200, this.options.cardProfileBody ?? DEFAULT_CARD_PROFILE);
      return;
    }

    const managementRoute = this.managementRouteFor(request.method ?? "GET", url);
    if (managementRoute !== null) {
      this.managementRequests.push({
        method: request.method ?? "",
        path: url.pathname,
        authorization,
        body: parsedBody,
        remotePort: request.socket.remotePort,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: { message: "invalid bot token" } });
        return;
      }
      const key = `${request.method} ${url.pathname}`;
      const overrideStatus = this.options.managementStatuses?.[key];
      if (overrideStatus !== undefined && (overrideStatus < 200 || overrideStatus >= 300)) {
        writeJson(response, overrideStatus, this.options.managementBodies?.[key] ?? {
          error: { message: "management route failed" },
          msg: "management route failed",
        });
        return;
      }
      const overrideBody = this.options.managementBodies?.[key];
      if (overrideBody !== undefined) {
        writeJson(response, 200, overrideBody);
        return;
      }
      this.respondManagement(managementRoute, parsedBody, response);
      return;
    }

    const threadPath = /^\/v1\/bot\/groups\/([^/]+)\/threads\/([^/]+)\/md$/.exec(url.pathname);
    const groupPath = /^\/v1\/bot\/groups\/([^/]+)\/md$/.exec(url.pathname);
    let target: string | undefined;
    if (threadPath) {
      const channelId = threadPath[1];
      const threadId = threadPath[2];
      if (channelId !== undefined && threadId !== undefined) {
        target = `${decodeURIComponent(channelId)}____${decodeURIComponent(threadId)}`;
      }
    } else if (groupPath) {
      const channelId = groupPath[1];
      if (channelId !== undefined) target = decodeURIComponent(channelId);
    }
    if ((request.method === "GET" || request.method === "PUT") && target !== undefined) {
      if (request.method === "PUT") {
        this.groupContextUpdates.push({
          path: url.pathname,
          target,
          authorization,
          body: parsedBody,
        });
        if (authorization !== `Bearer ${BOT_TOKEN}`) {
          writeJson(response, 401, { error: "invalid bot token" });
          return;
        }
        const status = this.options.groupContextPutStatuses?.[target] ?? 200;
        if (status < 200 || status >= 300) {
          writeJson(response, status, { error: "group context update failure" });
          return;
        }
        if (typeof parsedBody.content !== "string") {
          writeJson(response, 400, { error: "content is required" });
          return;
        }
        const version = this.options.groupContextPutVersions?.[target] ?? 1;
        this.groupContextBodies.delete(target);
        this.groupContexts.set(target, {
          content: parsedBody.content,
          present: parsedBody.content.length > 0,
          version,
          updatedAt: new Date().toISOString(),
          updatedBy: BOT_UID,
        });
        writeJson(response, 200, { version });
        return;
      }

      this.groupContextRequests.push({
        path: url.pathname,
        target,
        authorization,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const status = this.options.groupContextStatuses?.[target];
      if (status !== undefined && (status < 200 || status >= 300)) {
        writeJson(response, status, { error: "group context failure" });
        return;
      }
      const rawBody = this.groupContextBodies.get(target);
      if (rawBody !== undefined) {
        writeJson(response, 200, rawBody);
        return;
      }
      const context = this.groupContexts.get(target);
      if (context === undefined || context === null) {
        writeJson(response, 404, { error: "group context not found" });
        return;
      }
      writeJson(response, 200, {
        content: context.content,
        version: context.version,
        updated_at: context.updatedAt,
        updated_by: context.updatedBy,
      });
      return;
    }
    if (request.method === "POST" && url.pathname === "/v1/bot/typing") {
      this.typingRequests.push({
        path: `${url.pathname}${url.search}`,
        authorization,
        body: parsedBody,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const status = this.options.typingStatus ?? 200;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "typing failed" });
        return;
      }
      writeJson(response, 200, { ok: true });
      return;
    }

    if (request.method === "POST" && url.pathname === "/v1/bot/message/edit") {
      this.messageEdits.push({
        path: `${url.pathname}${url.search}`,
        authorization,
        body: parsedBody,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const status = this.options.messageEditStatus ?? 200;
      if (status < 200 || status >= 300) {
        const body = this.options.messageEditErrorBody;
        if (body === undefined) {
          writeJson(response, status, { error: "message edit failed" });
        } else {
          response.writeHead(status, {
            "content-type": this.options.messageEditErrorContentType ?? "application/json",
          });
          response.end(body);
        }
        return;
      }
      const responseDelay = this.options.messageEditDelayMs ?? 0;
      if (responseDelay > 0) await sleep(responseDelay);
      const messageId = parsedBody.message_id;
      const contentEdit = parsedBody.content_edit;
      if (typeof messageId === "string" && typeof contentEdit === "string") {
        try {
          const payload = JSON.parse(contentEdit) as Record<string, unknown>;
          if (payload.type === 1 && typeof payload.content === "string") {
            this.latestEditedTextByMessageId.set(messageId, payload.content);
          }
        } catch {
          // Keep the raw request for protocol assertions; the real server rejects malformed JSON.
        }
      }
      writeJson(response, 200, { ok: true });
      return;
    }

    if (request.method === "POST" && url.pathname === "/v1/bot/messages/sync") {
      this.messagesSyncRequests.push({
        path: `${url.pathname}${url.search}`,
        authorization,
        body: parsedBody,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const responseDelay = this.options.messageSyncDelayMs ?? 0;
      if (responseDelay > 0) await sleep(responseDelay);
      response.writeHead(this.messageSyncStatus, { "content-type": "application/json" });
      response.end(
        typeof this.messageSyncBody === "string"
          ? this.messageSyncBody
          : JSON.stringify(this.messageSyncBody),
      );
      return;
    }

    if (request.method === "POST" && url.pathname === "/v1/bot/sendMessage") {
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      this.sentMessages.push({ authorization, body: parsedBody });
      const responseDelay = this.options.sendMessageDelayMs ?? 0;
      if (responseDelay > 0) await sleep(responseDelay);

      const clientMsgNo = typeof parsedBody.client_msg_no === "string"
        ? parsedBody.client_msg_no
        : "";
      let messageId = this.messageIdsByClientNo.get(clientMsgNo);
      if (!messageId) {
        messageId = this.nextMessageId.toString();
        this.nextMessageId += 1n;
        if (clientMsgNo) this.messageIdsByClientNo.set(clientMsgNo, messageId);
      }
      response.writeHead(200, { "content-type": "application/json" });
      // Keep this as a numeric JSON token to prove the client protects int64 precision.
      response.end(`{"message_id":${messageId}}`);
      return;
    }

    if (request.method === "POST" && url.pathname === "/v1/bot/events") {
      this.botEventRequests.push({
        path: `${url.pathname}${url.search}`,
        authorization,
        body: parsedBody,
      });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const status = this.options.botEventsStatus ?? 200;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "bot events failed" });
        return;
      }
      const batches = this.options.botEventBatches ?? [];
      const index = Math.min(this.botEventBatchIndex, Math.max(0, batches.length - 1));
      this.botEventBatchIndex += 1;
      writeJson(response, 200, { status: 1, results: batches[index] ?? [] });
      return;
    }

    const ackMatch = request.method === "POST"
      ? /^\/v1\/bot\/events\/(\d+)\/ack$/u.exec(url.pathname)
      : null;
    if (ackMatch !== null) {
      this.botEventAcks.push({ path: url.pathname, authorization });
      if (authorization !== `Bearer ${BOT_TOKEN}`) {
        writeJson(response, 401, { error: "invalid bot token" });
        return;
      }
      const status = this.options.botEventAckStatus ?? 200;
      if (status < 200 || status >= 300) {
        writeJson(response, status, { error: "bot event ack failed" });
        return;
      }
      writeJson(response, 200, { status: 200 });
      return;
    }

    writeJson(response, 404, { error: "not found" });
  }

  private botEventBatchIndex = 0;


  private handleClientFrames(connection: FixtureConnection & { aesKey?: string; aesIv?: string }, data: Uint8Array): void {
    let frames: Frame[];
    try {
      frames = decodeFrames(data);
    } catch {
      connection.socket.close();
      return;
    }
    for (const frame of frames) {
      if (frame.packetType === 1) {
        try {
          connection.connect = connectPacketFromBody(frame.body);
        } catch {
          connection.socket.close();
          continue;
        }
        const connectionIndex = this.connections.indexOf(connection);
        const expectedToken = this.issuedTokens.at(-1);
        const configuredReason = this.options.connackReasonCodes?.[connectionIndex];
        const reasonCode = configuredReason
          ?? (connection.connect.token === expectedToken ? 1 : 2);
        const serverKeyPair = generateKeyPair(randomBytes(32));
        const serverPublicKey = Buffer.from(serverKeyPair.public).toString("base64");
        if (reasonCode === 1) {
          connection.aesKey = deriveAesKey(connection.connect.clientKey, serverKeyPair.private);
          connection.aesIv = AES_SALT;
        }
        const frameBytes = encodeConnackFrame(reasonCode, serverPublicKey);
        const delay = this.options.connackDelayMs ?? 0;
        setTimeout(() => {
          if (connection.socket.readyState !== WebSocket.OPEN) return;
          connection.ready = reasonCode === 1;
          connection.socket.send(frameBytes);
          if (reasonCode !== 1) {
            setTimeout(() => connection.socket.close(), 20);
          }
        }, delay);
      } else if (frame.packetType === 6) {
        try {
          connection.recvAcks.push(decodeRecvack(frame.body));
        } catch {
          connection.socket.close();
        }
      } else if (frame.packetType === 7 && !this.options.ignorePong) {
        connection.socket.send(Uint8Array.from([8 << 4]));
      }
    }
  }

  private sendChunks(socket: WebSocket, data: Uint8Array, sizes?: number[]): void {
    if (!sizes || sizes.length === 0) {
      socket.send(Buffer.from(data));
      return;
    }
    let offset = 0;
    for (const requestedSize of sizes) {
      if (offset >= data.byteLength) break;
      const size = Math.max(1, Math.min(requestedSize, data.byteLength - offset));
      socket.send(Buffer.from(data.slice(offset, offset + size)));
      offset += size;
    }
    if (offset < data.byteLength) socket.send(Buffer.from(data.slice(offset)));
  }
}

async function readRawBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function writeJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

export async function waitForFixture<T>(
  read: () => T | undefined,
  timeoutMs = 2_000,
  description = "fixture condition was not met",
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value !== undefined) return value;
    const tick = Promise.withResolvers<void>();
    setTimeout(tick.resolve, 5);
    await tick.promise;
  }
  throw new Error(description);
}

export async function createOctoProtocolFixture(options: OctoProtocolFixtureOptions = {}): Promise<OctoProtocolFixture> {
  return OctoProtocolFixture.start(options);
}
