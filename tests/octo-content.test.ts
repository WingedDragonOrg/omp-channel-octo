import { afterEach, describe, expect, test } from "bun:test";
import { createServer, type Server } from "node:http";
import { mkdtemp, open as openFile, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AttachmentError } from "../src/attachments.ts";
import type { ChannelTransport, InboundMessage } from "../src/contracts.ts";
import { createOctoTransport } from "../src/octo/index.ts";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type OctoProtocolFixture,
} from "./fixtures/octo-protocol-server.ts";

/** Every inbound case is a group message; only the encrypted payload varies. */
const GROUP_CHANNEL = "group-content";

let fixture: OctoProtocolFixture | undefined;
let transport: ChannelTransport | undefined;
let temporaryDirectory: string | undefined;
let streamingServer: Server | undefined;

afterEach(async () => {
  await transport?.stop().catch(() => {});
  await fixture?.close().catch(() => {});
  const server = streamingServer;
  streamingServer = undefined;
  if (server) {
    server.closeAllConnections?.();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  if (temporaryDirectory) await rm(temporaryDirectory, { recursive: true, force: true });
  fixture = undefined;
  transport = undefined;
  temporaryDirectory = undefined;
});

/**
 * Registration plus a live WebSocket, mirroring how the resident channel connects. Messages
 * arrive through the real RECV path, so the assertions cover wire decoding too.
 */
async function startFixtureTransport(
  onMessage: (message: InboundMessage) => Promise<void>,
): Promise<{ current: OctoProtocolFixture; connection: FixtureConnection }> {
  const current = await createOctoProtocolFixture();
  fixture = current;
  const channel = createOctoTransport({ apiUrl: current.apiUrl, botToken: current.botToken });
  transport = channel;
  await channel.initialize();
  const started = channel.start(onMessage, () => {});
  const connection = await current.waitForConnection(1);
  await waitForFixture(
    () => connection.ready ? connection : undefined,
    2_000,
    "Octo fixture connection did not reach CONNACK ready",
  );
  await started;
  return { current, connection };
}

/** Registration only: download, transcription and history calls never need the socket. */
async function startRegisteredFixture(): Promise<{
  current: OctoProtocolFixture;
  channel: ChannelTransport;
}> {
  const current = await createOctoProtocolFixture();
  fixture = current;
  const channel = createOctoTransport({ apiUrl: current.apiUrl, botToken: current.botToken });
  transport = channel;
  await channel.initialize();
  return { current, channel };
}

/** Delivers one payload per message and waits for every normalized message to arrive. */
async function deliverInbound(
  current: OctoProtocolFixture,
  connection: FixtureConnection,
  inbox: InboundMessage[],
  payloads: readonly Record<string, unknown>[],
): Promise<InboundMessage[]> {
  for (const payload of payloads) {
    current.sendInbound(connection, {
      fromUid: current.ownerUid,
      channelId: GROUP_CHANNEL,
      channelType: 2,
      text: "",
      payload,
    });
  }
  return waitForFixture(
    () => inbox.length === payloads.length ? inbox : undefined,
    5_000,
    `${payloads.length} inbound messages were not delivered (received ${inbox.length})`,
  );
}

function observed(messages: readonly InboundMessage[]): Array<{
  text: string;
  supported: boolean;
  attachments: InboundMessage["attachments"];
}> {
  return messages.map((message) => ({
    text: message.text,
    supported: message.supported,
    attachments: message.attachments,
  }));
}

function attachment(
  url: string,
  kind: "image" | "file" | "audio" | "video",
  fields: { name?: string; size?: number; durationSeconds?: number } = {},
): InboundMessage["attachments"][number] {
  return {
    url,
    kind,
    name: fields.name ?? "",
    size: fields.size ?? 0,
    durationSeconds: fields.durationSeconds ?? 0,
  };
}

async function expectAttachmentFailure(
  attempt: Promise<unknown>,
  reason: AttachmentError["reason"],
): Promise<void> {
  const error = await attempt.then(
    () => undefined,
    (thrown: unknown) => thrown,
  );
  expect(error).toBeInstanceOf(AttachmentError);
  expect((error as AttachmentError).reason).toBe(reason);
}

/**
 * Raw HTTP server answering with a chunked body: the response carries no Content-Length, so
 * the download pipeline has to catch the oversize while streaming instead of up front.
 */
async function startStreamingServer(totalBytes: number): Promise<string> {
  const server = createServer((_request, response) => {
    response.writeHead(200, { "content-type": "application/octet-stream" });
    for (let sent = 0; sent < totalBytes; sent += 512) response.write(Buffer.alloc(512, 9));
    response.end();
  });
  server.on("error", () => undefined);
  streamingServer = server;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("streaming test server has no TCP port");
  return `http://127.0.0.1:${address.port}/stream`;
}

describe("Octo inbound message content", () => {
  test("extracts text and attachments for every supported message type", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });
    const asset = (key: string): string => `${current.apiUrl}/fixture-download/${key}`;
    const payloads: Record<string, unknown>[] = [
      { type: 1, content: "纯文本" },
      { type: 2, url: asset("caption.png"), name: "caption.png", caption: "图片说明" },
      { type: 2, url: asset("content.png"), content: "回退到 content" },
      // 线上真实 type 2 payload 带 `size`（图片消息）：声明值要带出来做超限预检。
      { type: 2, url: asset("sized.webp"), name: "sized.webp", size: 1_618_014, width: 1370, height: 1148 },
      { type: 3, url: asset("animation.gif") },
      { type: 4, url: asset("second.amr"), name: "second.amr", second: 7 },
      { type: 4, url: asset("trad.amr"), timeTrad: 4 },
      { type: 4, url: asset("duration.amr"), duration: 9 },
      { type: 4, url: asset("precedence.amr"), second: 0, timeTrad: 3, duration: 8 },
      { type: 5, url: asset("clip.mp4"), name: "clip.mp4", size: 2048, timeTrad: 12 },
      { type: 6, latitude: 31.23, longitude: 121.47, address: "上海市", title: "外滩" },
      { type: 7, name: "张三", uid: "contact-1" },
      { type: 8, url: asset("doc.pdf"), name: "doc.pdf", size: 4096, caption: "看文档" },
      // 部分客户端把文件名与后缀分成两个字段发送：存盘名要补回后缀。
      { type: 9, url: asset("alias.zip"), name: "alias", extension: "zip", size: 8192 },
      {
        type: 11,
        users: [{ uid: "member-a", name: "阿甲" }],
        msgs: [
          { from_uid: "member-a", timestamp: 1, payload: { type: 1, content: "第一句" } },
          { from_uid: "member-b", timestamp: 2, payload: { type: 2, url: asset("inner.png") } },
        ],
      },
      {
        type: 14,
        plain: "服务端纯文本",
        content: [
          { type: "text", text: "块内文本" },
          { type: "image", url: asset("inline.png"), name: "inline.png", size: 333 },
        ],
      },
      {
        type: 14,
        content: [
          { type: "text", text: "只有块" },
          { type: "image", url: asset("fallback.png") },
          // 未知块类型（写入端 write-strict 会拒绝，这里只可能是前向兼容）：按宽容解析只取 text。
          { type: "video", url: asset("blocks.mp4"), text: "未知块文本" },
        ],
      },
      { type: 17, plain: "卡片正文" },
      // 服务端保证 plain 非空；缺失时按 octo-server Decision 8 的占位符降级。
      { type: 17, card: { type: "AdaptiveCard", version: "1.5", body: [] } },
    ];

    const messages = await deliverInbound(current, connection, inbox, payloads);

    expect(observed(messages)).toEqual([
      { text: "纯文本", supported: true, attachments: [] },
      {
        text: "图片说明",
        supported: true,
        attachments: [attachment(asset("caption.png"), "image", { name: "caption.png" })],
      },
      {
        text: "回退到 content",
        supported: true,
        attachments: [attachment(asset("content.png"), "image")],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("sized.webp"), "image", { name: "sized.webp", size: 1_618_014 })],
      },
      { text: "", supported: true, attachments: [attachment(asset("animation.gif"), "image")] },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("second.amr"), "audio", { name: "second.amr", durationSeconds: 7 })],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("trad.amr"), "audio", { durationSeconds: 4 })],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("duration.amr"), "audio", { durationSeconds: 9 })],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("precedence.amr"), "audio", { durationSeconds: 3 })],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("clip.mp4"), "video", {
          name: "clip.mp4",
          size: 2048,
          durationSeconds: 12,
        })],
      },
      { text: "[位置信息: 31.23,121.47] 上海市 外滩", supported: true, attachments: [] },
      { text: "[名片: 张三 (contact-1)]", supported: true, attachments: [] },
      {
        text: "看文档",
        supported: true,
        attachments: [attachment(asset("doc.pdf"), "file", { name: "doc.pdf", size: 4096 })],
      },
      {
        text: "",
        supported: true,
        attachments: [attachment(asset("alias.zip"), "file", { name: "alias.zip", size: 8192 })],
      },
      // 合并转发只摊平文本：内层图片是占位符，绝不变成外层附件。
      { text: "[合并转发]\n阿甲: 第一句\nmember-b: [图片]", supported: true, attachments: [] },
      {
        text: "服务端纯文本",
        supported: true,
        attachments: [attachment(asset("inline.png"), "image", { name: "inline.png", size: 333 })],
      },
      {
        text: "只有块[图片]未知块文本",
        supported: true,
        attachments: [attachment(asset("fallback.png"), "image")],
      },
      { text: "卡片正文", supported: true, attachments: [] },
      { text: "[卡片]", supported: true, attachments: [] },
    ]);
  });

  test("bounds a merge-forward by message count and code point budget", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });
    const inner = (index: number, content: string): Record<string, unknown> => ({
      from_uid: `member-${index}`,
      timestamp: index,
      payload: { type: 1, content },
    });
    const many = Array.from({ length: 35 }, (_value, index) => inner(index, `第 ${index + 1} 条`));
    // 每条整行约 1.5k code point：预算只容得下两条，其余按省略标注，已展开的行不截断。
    const long = Array.from({ length: 6 }, (_value, index) => inner(index, `${String(index).repeat(1_500)}尾巴`));

    const messages = await deliverInbound(current, connection, inbox, [
      { type: 11, msgs: many },
      { type: 11, msgs: long },
    ]);

    const cappedLines = (messages[0]?.text ?? "").split("\n");
    expect(cappedLines).toHaveLength(32);
    expect(cappedLines[0]).toBe("[合并转发]");
    expect(cappedLines[1]).toBe("member-0: 第 1 条");
    expect(cappedLines[30]).toBe("member-29: 第 30 条");
    expect(cappedLines[31]).toBe("…（其余 5 条省略）");

    const budgetedLines = (messages[1]?.text ?? "").split("\n");
    expect(budgetedLines).toHaveLength(4);
    expect(budgetedLines[0]).toBe("[合并转发]");
    expect(budgetedLines[3]).toBe("…（其余 4 条省略）");
    for (const line of budgetedLines.slice(1, -1)) {
      expect((line ?? "").endsWith("尾巴")).toBe(true);
    }
  });

  test("keeps stickers unsupported with no text and no attachments", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });
    const asset = (key: string): string => `${current.apiUrl}/fixture-download/${key}`;

    const messages = await deliverInbound(current, connection, inbox, [
      { type: 12, url: asset("sticker.png") },
      { type: 13, url: asset("emoji.png") },
    ]);

    expect(observed(messages)).toEqual([
      { text: "", supported: false, attachments: [] },
      { text: "", supported: false, attachments: [] },
    ]);
  });

  test("drops attachment URLs that are not absolute http(s) URLs", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });

    const messages = await deliverInbound(current, connection, inbox, [
      { type: 2, url: "data:image/png;base64,AAAA", caption: "数据 URL" },
      // 服务端在取不到对象存储直链时回退的相对路径，以及没有 scheme 的根相对路径。
      { type: 2, url: "file/preview/relative.png" },
      { type: 2, url: "/v1/file/preview/rooted.png" },
      { type: 8, url: { href: `${current.apiUrl}/fixture-download/object.pdf` }, name: "object.pdf", caption: "对象 URL" },
    ]);

    expect(observed(messages)).toEqual([
      { text: "数据 URL", supported: true, attachments: [] },
      { text: "", supported: false, attachments: [] },
      { text: "", supported: false, attachments: [] },
      { text: "对象 URL", supported: true, attachments: [] },
    ]);
  });

  test("reads the quoted message of a reply as text plus its own media", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });
    const quotedImage = `${current.apiUrl}/fixture-download/quoted.png`;

    const messages = await deliverInbound(current, connection, inbox, [
      // 线上真实回复：自己的 type 1 文本 + 内联的整条被引用 payload（这里是被引用消息
      // 的 type 14 富文本，图片块带绝对 CDN 直链）。
      {
        type: 1,
        content: "现在能看到这个图片了吗",
        reply: {
          root_mid: "original-message",
          message_id: "original-message",
          message_seq: 12,
          from_uid: current.ownerUid,
          from_name: "董振兴",
          payload: {
            type: 14,
            plain: "@O-小爱 [图片]这个开关要打开",
            content: [
              { type: "text", text: "@O-小爱 " },
              { type: "image", url: quotedImage, width: 980, height: 1258 },
              { type: "text", text: "这个开关要打开" },
            ],
            mention: { uids: [current.botUid], entities: [{ uid: current.botUid, offset: 0, length: 5 }] },
          },
        },
      },
      // 只回复不写字：正文为空，内容全部来自被引用的消息。
      {
        type: 1,
        content: "",
        reply: {
          message_id: "original-message",
          from_name: "董振兴",
          payload: { type: 2, url: quotedImage, caption: "被引用的图片说明" },
        },
      },
      // 被引用消息只有不可下载的相对 URL，也没有文本：整条引用不成立。
      {
        type: 1,
        content: "这条引用不可用",
        reply: { from_name: "董振兴", payload: { type: 2, url: "file/preview/relative.png" } },
      },
      // 自己的图片与引用的图片是同一个 URL：只保留一条附件。
      {
        type: 2,
        url: quotedImage,
        reply: { from_name: "董振兴", payload: { type: 2, url: quotedImage, caption: "同一个文件" } },
      },
    ]);

    expect(
      messages.map((message) => ({
        text: message.text,
        supported: message.supported,
        quoted: message.quoted,
        attachments: message.attachments,
      })),
    ).toEqual([
      {
        text: "现在能看到这个图片了吗",
        supported: true,
        quoted: { senderName: "董振兴", text: "@O-小爱 [图片]这个开关要打开" },
        attachments: [attachment(quotedImage, "image")],
      },
      {
        text: "",
        supported: true,
        quoted: { senderName: "董振兴", text: "被引用的图片说明" },
        attachments: [attachment(quotedImage, "image")],
      },
      { text: "这条引用不可用", supported: true, quoted: null, attachments: [] },
      {
        text: "",
        supported: true,
        quoted: { senderName: "董振兴", text: "同一个文件" },
        attachments: [attachment(quotedImage, "image")],
      },
    ]);
  });

  test("strips the bot mention from extracted text", async () => {
    const inbox: InboundMessage[] = [];
    const { current, connection } = await startFixtureTransport(async (message) => {
      inbox.push(message);
    });
    const asset = (key: string): string => `${current.apiUrl}/fixture-download/${key}`;
    const mentionToken = `@${current.botName}`;

    const messages = await deliverInbound(current, connection, inbox, [
      {
        type: 2,
        url: asset("mention.png"),
        name: "mention.png",
        caption: `${mentionToken} 看这个`,
        mention: {
          entities: [{ uid: current.botUid, offset: 0, length: mentionToken.length }],
        },
      },
      // 没有 span、只有一个 uid 列表时按 `@名字` 剥离。
      {
        type: 8,
        url: asset("mention.pdf"),
        name: "mention.pdf",
        caption: `@${current.botUid} 看文档`,
        mention: { uids: [current.botUid] },
      },
      // 别人的 mention 不是剥离对象。
      {
        type: 1,
        content: "@助理 帮忙看看",
        mention: { entities: [{ uid: "member-a", offset: 0, length: "@助理".length }] },
      },
      // 只 @ 不说话：文本类型本身仍是可处理的输入，不能被判成「不支持的消息类型」。
      {
        type: 1,
        content: mentionToken,
        mention: { entities: [{ uid: current.botUid, offset: 0, length: mentionToken.length }] },
      },
    ]);

    expect(observed(messages)).toEqual([
      {
        text: "看这个",
        supported: true,
        attachments: [attachment(asset("mention.png"), "image", { name: "mention.png" })],
      },
      {
        text: "看文档",
        supported: true,
        attachments: [attachment(asset("mention.pdf"), "file", { name: "mention.pdf" })],
      },
      { text: "@助理 帮忙看看", supported: true, attachments: [] },
      { text: "", supported: true, attachments: [] },
    ]);
  });
});

describe("Octo inbound attachment download", () => {
  test("stores a fixture download and reuses the stored file", async () => {
    const { current, channel } = await startRegisteredFixture();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-content-"));
    const bytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02, 0x03]);
    current.serveDownload("image", { body: bytes, contentType: "image/png" });
    const target = join(temporaryDirectory, "0-shot.png");
    const url = `${current.apiUrl}/fixture-download/image`;

    expect(await channel.downloadAttachment({ url, path: target, maxBytes: 1_024 }))
      .toBe(bytes.byteLength);
    expect((await readFile(target)).equals(bytes)).toBe(true);
    // 对象存储直链是公开读：下载请求不带任何鉴权头。
    expect(current.downloadRequests).toEqual([
      { path: "/fixture-download/image", authorization: undefined },
    ]);

    // 同一目标路径已有非空文件时直接复用，既不重发请求也不留临时文件。
    expect(await channel.downloadAttachment({ url, path: target, maxBytes: 1_024 }))
      .toBe(bytes.byteLength);
    expect(current.downloadRequests).toHaveLength(1);
    expect(await readdir(temporaryDirectory)).toEqual(["0-shot.png"]);
  });

  test("classifies download failures and leaves no temporary file", async () => {
    const { current, channel } = await startRegisteredFixture();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-content-"));
    const directory = join(temporaryDirectory, "failures");
    const destination = (name: string): string => join(directory, name);

    // 未注册的 key 由 fixture 回 404。
    await expectAttachmentFailure(
      channel.downloadAttachment({
        url: `${current.apiUrl}/fixture-download/missing`,
        path: destination("missing.png"),
        maxBytes: 1_024,
      }),
      "failed",
    );

    // Content-Length 预检：响应头声明的长度已超限，无需读完响应体。
    current.serveDownload("declared", { body: Buffer.alloc(4_096, 7) });
    await expectAttachmentFailure(
      channel.downloadAttachment({
        url: `${current.apiUrl}/fixture-download/declared`,
        path: destination("declared.bin"),
        maxBytes: 1_024,
      }),
      "oversize",
    );

    // 流式累计：chunked 响应没有 Content-Length，只有读的时候才能发现超限。
    const streamed = await startStreamingServer(4_096);
    const streamedPath = join(temporaryDirectory, "streamed", "streamed.bin");
    // 同一个响应在限额内能完整落盘，超限断言就不是"请求本身就坏"蒙对的。
    expect(await channel.downloadAttachment({ url: streamed, path: streamedPath, maxBytes: 8_192 }))
      .toBe(4_096);
    await expectAttachmentFailure(
      channel.downloadAttachment({ url: streamed, path: destination("streamed.bin"), maxBytes: 1_024 }),
      "oversize",
    );

    // 调用方 signal 已中止：归类为超时，而不是普通失败。
    const controller = new AbortController();
    controller.abort();
    await expectAttachmentFailure(
      channel.downloadAttachment({
        url: `${current.apiUrl}/fixture-download/aborted`,
        path: destination("aborted.bin"),
        maxBytes: 1_024,
        signal: controller.signal,
      }),
      "timeout",
    );

    expect(await readdir(directory)).toEqual([]);
  });

  test("transcribes a stored audio file through the fixture endpoint", async () => {
    const { current, channel } = await startRegisteredFixture();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-content-"));
    const audioPath = join(temporaryDirectory, "voice.m4a");
    await writeFile(audioPath, Buffer.from("audio-bytes", "utf8"));
    current.setTranscription({ status: 200, text: "hello" });

    expect(await channel.transcribeAudio(audioPath)).toBe("hello");

    expect(current.transcriptionRequests).toHaveLength(1);
    const request = current.transcriptionRequests[0]!;
    expect(request.path).toBe("/v1/bot/voice/transcribe");
    expect(request.authorization).toBe(`Bearer ${current.botToken}`);
    expect(request.contentType ?? "").toContain("multipart/form-data");
    const body = request.body.toString("utf8");
    expect(body).toContain('name="mode"');
    expect(body).toContain('name="audio"');
    expect(body).toContain('filename="voice.m4a"');
    expect(body).toContain("Content-Type: audio/mp4");
    expect(body).toContain("audio-bytes");
    expect(/name="mode"\r?\n\r?\nedit/u.test(body)).toBe(true);

    current.setTranscription({ error: "asr unavailable" }, 503);
    expect(await channel.transcribeAudio(audioPath)).toBeNull();

    current.setTranscription({ status: 200, text: "   " });
    expect(await channel.transcribeAudio(audioPath)).toBeNull();

    // 本地读不到文件时直接放弃，不向服务端发起请求。
    expect(await channel.transcribeAudio(join(temporaryDirectory, "missing.m4a"))).toBeNull();
    expect(current.transcriptionRequests).toHaveLength(3);

    // 超过 20 MiB 的音频不值得读进内存，也不会有 ASR 能处理：直接放弃。
    const oversized = join(temporaryDirectory, "huge.amr");
    const handle = await openFile(oversized, "w");
    await handle.truncate(21 * 1024 * 1024);
    await handle.close();
    expect(await channel.transcribeAudio(oversized)).toBeNull();
    expect(current.transcriptionRequests).toHaveLength(3);
  });
});

describe("Octo group history backfill", () => {
  test("keeps only extractable text and never downloads", async () => {
    const { current, channel } = await startRegisteredFixture();
    const asset = (key: string): string => `${current.apiUrl}/fixture-download/${key}`;
    const encoded = (payload: Record<string, unknown>): string =>
      Buffer.from(JSON.stringify(payload), "utf8").toString("base64");
    current.setMessagesSync({
      messages: [
        {
          message_seq: 11,
          from_uid: "member-a",
          timestamp: 1_757_656_000,
          payload: encoded({ type: 2, url: asset("history.png"), caption: "历史图片说明" }),
        },
        {
          message_seq: 12,
          from_uid: "member-b",
          timestamp: 1_757_656_001,
          payload: encoded({
            type: 11,
            users: [{ uid: "member-a", name: "阿甲" }],
            msgs: [{ from_uid: "member-a", timestamp: 1, payload: { type: 1, content: "转发内容" } }],
          }),
        },
        {
          message_seq: 13,
          from_uid: "member-c",
          timestamp: 1_757_656_002,
          payload: encoded({
            type: 14,
            plain: "富文本正文",
            content: [{ type: "image", url: asset("history-inline.png") }],
          }),
        },
        {
          message_seq: 14,
          from_uid: "member-d",
          timestamp: 1_757_656_003,
          payload: encoded({ type: 12, url: asset("sticker.png") }),
        },
      ],
    });

    const history = await channel.fetchGroupMessages(
      { channelId: GROUP_CHANNEL, channelType: 2 },
      { sinceSeq: 0, untilSeq: 0, limit: 50 },
    );

    expect(history).toEqual([
      { seq: 11, senderId: "member-a", text: "历史图片说明", timestamp: 1_757_656_000_000 },
      { seq: 12, senderId: "member-b", text: "[合并转发]\n阿甲: 转发内容", timestamp: 1_757_656_001_000 },
      { seq: 13, senderId: "member-c", text: "富文本正文", timestamp: 1_757_656_002_000 },
    ]);
    expect(current.downloadRequests).toHaveLength(0);
  });
});
