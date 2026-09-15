import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import {
  ATTACHMENT_DOWNLOAD_BUDGET_MS,
  AttachmentError,
  attachmentDirectory,
  safeAttachmentName,
} from "../src/attachments.ts";
import type {
  AttachmentConfig,
  BotIdentity,
  ChannelCommandHandler,
  ChannelService,
  ChatTarget,
  CoreServiceConfig,
  InboundAttachment,
  InboundAttachmentKind,
  InboundMessage,
} from "../src/contracts.ts";
import { createChannelService } from "../src/service.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  readText,
  waitForStatus,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";

const MAX_BYTES = 1024 * 1024;
/** 12 字节的假 PNG 头：落盘字节数必须与 fixture 注册的远端字节完全一致。 */
const IMAGE_BYTES = new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3]);
const AUDIO_BYTES = new Uint8Array([35, 33, 65, 77, 82, 10, 1, 2]);
const KEPT_BYTES = new Uint8Array([1, 2, 3, 4, 5]);

const ATTACHMENT_HEADER = "附件（已保存到本机；需要时用 read 打开）：";
const REFUSAL_DISABLED = "附件接收已关闭，未接收任务。";
/** `maxBytes` 为整数 MB 时超限文案不带小数。 */
const REFUSAL_OVERSIZE = "附件超过 1 MB 上限，未接收任务。";
const REFUSAL_OVER_BUDGET = "附件过多或下载超时，未接收任务。";
const REFUSAL_DOWNLOAD_FAILED = "附件下载失败，未接收任务。";
const REFUSAL_UNSUPPORTED = "不支持的消息类型，不会启动任务。";

interface AttachmentHarness {
  readonly root: string;
  readonly stateDir: string;
  readonly bot: BotIdentity;
  readonly transport: FakeTransport;
  readonly workers: FakeWorkerFactory;
  readonly service: ChannelService;
}

interface AttachmentHarnessOptions {
  /** `null` 表示 config 里完全不写 `attachments`（未配置）；默认开启且上限 1 MiB。 */
  attachments?: AttachmentConfig | null;
  /** `null` 表示不写 ownerId，改由第一条非空文本私聊完成认领。 */
  ownerId?: string | null;
  prompt?: FakePrompt;
  /** 注册给服务的自定义命令 handler（观察型 / 拦截型都走这里）。 */
  commandHandlers?: readonly ChannelCommandHandler[];
  /** 在 `service.start()` 之前准备状态目录，例如造出陈旧的附件目录。 */
  beforeStart?: (stateDir: string) => Promise<void>;
}

/**
 * `createCoreHarness` 不暴露 `attachments` 配置，因此这里按 `core-boundaries` 的既有方式
 * 自建 config + `FakeTransport` + `createChannelService`，其余等待手段复用 core-fakes。
 */
async function createAttachmentHarness(
  options: AttachmentHarnessOptions = {},
): Promise<AttachmentHarness> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-attachments-")));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const bot: BotIdentity = { uid: `bot-${root.split("/").at(-1) ?? "fixture"}`, name: "Fixture Bot" };
  const transport = new FakeTransport({ bot });
  const workers = new FakeWorkerFactory(options.prompt);
  const config: CoreServiceConfig = {
    ...(options.ownerId === null ? {} : { ownerId: options.ownerId ?? "owner-1" }),
    allowedGroupIds: ["group-allowed"],
    stateDir,
    homeDir: root,
    ...(options.attachments === null
      ? {}
      : { attachments: options.attachments ?? { enabled: true, maxBytes: MAX_BYTES } }),
  };
  if (options.beforeStart) await options.beforeStart(stateDir);
  const service = await createChannelService({
    config,
    transport,
    workerFactory: workers,
    ...(options.commandHandlers === undefined ? {} : { commandHandlers: options.commandHandlers }),
  });
  await service.start();
  return { root, stateDir, bot, transport, workers, service };
}

async function closeAttachmentHarness(harness: AttachmentHarness): Promise<void> {
  await harness.service.stop();
  await rm(harness.root, { recursive: true, force: true });
}

function attachment(
  url: string,
  kind: InboundAttachmentKind,
  name: string,
  overrides: Partial<InboundAttachment> = {},
): InboundAttachment {
  return { url, kind, name, size: 0, durationSeconds: 0, ...overrides };
}

/** 服务会写入的路径：`<stateDir>/attachments/<type>-<channelId>/<messageId>/<index>-<name>`。 */
function savedPath(
  harness: AttachmentHarness,
  target: ChatTarget,
  messageId: string,
  index: number,
  name: string,
  kind: InboundAttachmentKind,
  url: string,
): string {
  return join(
    messageDirectory(harness, target, messageId),
    safeAttachmentName(name, index, kind, url),
  );
}

function messageDirectory(
  harness: AttachmentHarness,
  target: ChatTarget,
  messageId: string,
): string {
  return attachmentDirectory(harness.stateDir, target, messageId);
}

/** 提示词里的附件路径就是任务真正会 `read` 的路径，因此直接从中取回。 */
function attachmentPathsFromPrompt(prompt: string): string[] {
  return [...prompt.matchAll(/→ (.+)$/gmu)].map((match) => match[1]!.trim());
}

function withDatabase<T>(harness: AttachmentHarness, read: (database: Database) => T): T {
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    return read(database);
  } finally {
    database.close();
  }
}

function taskCount(harness: AttachmentHarness, target: ChatTarget): number {
  return withDatabase(
    harness,
    (database) =>
      (database
        .query(
          "SELECT COUNT(*) AS count FROM tasks WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?",
        )
        .get(harness.bot.uid, target.channelType, target.channelId) as { count: number } | null)
        ?.count ?? 0,
  );
}

function ownerCount(harness: AttachmentHarness): number {
  return withDatabase(
    harness,
    (database) =>
      (database
        .query("SELECT COUNT(*) AS count FROM owners WHERE bot_uid = ?")
        .get(harness.bot.uid) as { count: number } | null)?.count ?? 0,
  );
}

/** 回复走 fire-and-forget 的 outbox，等它落定后才数得出「只回复一次」。 */
async function settleReplies(harness: AttachmentHarness, target: ChatTarget): Promise<void> {
  await waitForStatus(harness.service, target, (status) => status.outbox === 0);
}

function replyCount(harness: AttachmentHarness, text: string): number {
  return harness.transport.sent.filter((message) => message.text === text).length;
}

async function expectSameBytes(path: string, expected: Uint8Array): Promise<void> {
  expect(Buffer.from(await readFile(path)).equals(Buffer.from(expected))).toBe(true);
}

function gate(): { readonly promise: Promise<void>; release(): void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

test("图片无文本：恰好一条任务，提示词含附件块与绝对路径，落盘字节与远端一致", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/photo.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-image-only",
        ...target,
        text: "",
        attachments: [attachment(url, "image", "截图.png", { size: IMAGE_BYTES.byteLength })],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt).toContain(ATTACHMENT_HEADER);
    expect(prompt).toContain("1. image 截图.png");
    expect(prompt.startsWith(ATTACHMENT_HEADER)).toBe(true);

    const paths = attachmentPathsFromPrompt(prompt);
    expect(paths).toHaveLength(1);
    expect(isAbsolute(paths[0]!)).toBe(true);
    expect(paths[0]!.startsWith(harness.stateDir)).toBe(true);
    await expectSameBytes(paths[0]!, IMAGE_BYTES);

    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(harness.transport.downloadRequests[0]!.maxBytes).toBe(MAX_BYTES);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("回复带图的消息：引用正文与引用附件一起进入同一条任务", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/quoted.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-quoted-reply",
        ...target,
        text: "现在能看到这个图片了吗",
        quoted: { senderName: "董振兴", text: "@O-小爱 [图片]这个开关要打开" },
        attachments: [attachment(url, "image", "image.png", { size: IMAGE_BYTES.byteLength })],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt).toContain("现在能看到这个图片了吗");
    expect(prompt).toContain("[引用 董振兴]\n@O-小爱 [图片]这个开关要打开");
    expect(prompt).toContain(ATTACHMENT_HEADER);

    const paths = attachmentPathsFromPrompt(prompt);
    expect(paths).toHaveLength(1);
    await expectSameBytes(paths[0]!, IMAGE_BYTES);
    expect(harness.transport.downloadRequests).toHaveLength(1);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("只引用文本时引用块进入正文，不带引用的文本任务逐字不变", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-quote-only",
        ...target,
        text: "接着说",
        quoted: { senderName: "董振兴", text: "上一句" },
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(
      inbound({ messageId: "attach-plain-no-quote", ...target, text: "  普通文本 " }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(harness.workers.prompts.map((prompt) => prompt.text)).toEqual([
      "接着说\n\n[引用 董振兴]\n上一句",
      "  普通文本 ",
    ]);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("文字 + 图片：仍是一条任务，提示词同时含用户文本与附件块", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/caption.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-with-text",
        ...target,
        text: "看下这张图",
        attachments: [attachment(url, "image", "caption.png", { size: IMAGE_BYTES.byteLength })],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt.startsWith("看下这张图\n\n")).toBe(true);
    expect(prompt).toContain(ATTACHMENT_HEADER);
    expect(prompt).toContain("1. image caption.png");
    expect(taskCount(harness, target)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(1);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("语音：提示词含 [语音转写] 文本与音频路径", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/voice.amr";
  const path = savedPath(harness, target, "attach-voice", 0, "voice.amr", "audio", url);
  harness.transport.attachmentBodies.set(url, AUDIO_BYTES);
  harness.transport.transcriptionByPath.set(path, "帮我看下这个页面的报错");
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-voice",
        ...target,
        text: "",
        attachments: [attachment(url, "audio", "voice.amr", { size: AUDIO_BYTES.byteLength, durationSeconds: 3 })],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt).toContain("[语音转写] 帮我看下这个页面的报错");
    expect(prompt).toContain("1. audio voice.amr");
    expect(attachmentPathsFromPrompt(prompt)).toEqual([path]);
    await expectSameBytes(path, AUDIO_BYTES);
    expect(harness.transport.transcriptionRequests).toEqual([path]);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("语音转写失败：任务照常接受，只保留音频路径", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/broken.amr";
  const path = savedPath(harness, target, "attach-voice-failed", 0, "broken.amr", "audio", url);
  harness.transport.attachmentBodies.set(url, AUDIO_BYTES);
  harness.transport.transcriptionError = new Error("ASR 服务不可用");
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "attach-voice-failed",
        ...target,
        text: "",
        attachments: [attachment(url, "audio", "broken.amr", { size: AUDIO_BYTES.byteLength })],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt).not.toContain("[语音转写]");
    expect(attachmentPathsFromPrompt(prompt)).toEqual([path]);
    await expectSameBytes(path, AUDIO_BYTES);
    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(0);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("声明大小超限：回复超限文案、不发起下载、不建任务、重投不重复回复", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/huge.bin";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const message = inbound({
    messageId: "attach-oversize",
    ...target,
    text: "发个大文件",
    attachments: [attachment(url, "file", "huge.bin", { size: MAX_BYTES + 1 })],
  });
  try {
    await harness.service.handleInbound(message);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_OVERSIZE)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);

    await harness.service.handleInbound(message);
    await settleReplies(harness, target);
    expect(replyCount(harness, REFUSAL_OVERSIZE)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(0);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("下载失败：回复失败文案、清理空目录、不建任务、重投不重复下载", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const message = inbound({
    messageId: "attach-download-failed",
    ...target,
    text: "看下附件",
    attachments: [attachment("https://cdn.example.test/missing.png", "image", "missing.png")],
  });
  try {
    await harness.service.handleInbound(message);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);

    await harness.service.handleInbound(message);
    await settleReplies(harness, target);
    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(1);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("不支持的消息类型：回复固定文案、无任务与下载，重投不重复回复", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  // 贴纸（类型表之外）在传输层就是 supported=false、无附件。
  const sticker = inbound({
    messageId: "unsupported-sticker",
    ...target,
    text: "",
    supported: false,
    attachments: [],
  });
  try {
    await harness.service.handleInbound(sticker);
    await settleReplies(harness, target);
    await harness.service.handleInbound(sticker);
    await settleReplies(harness, target);

    // 第二次投递命中同一 messageId 的 command claim，所以拒绝只出现一次。
    expect(replyCount(harness, REFUSAL_UNSUPPORTED)).toBe(1);
    expect(taskCount(harness, target)).toBe(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.downloadRequests).toHaveLength(0);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("文件附件在提示词里渲染为 file 行并原样落盘", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/quarterly.pdf";
  const bytes = new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37]);
  harness.transport.attachmentBodies.set(url, bytes);
  const message = inbound({
    messageId: "attach-pdf",
    ...target,
    text: "看下文档",
    attachments: [attachment(url, "file", "quarterly.pdf", { size: bytes.byteLength })],
  });
  try {
    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    const path = savedPath(harness, target, message.messageId, 0, "quarterly.pdf", "file", url);
    const prompt = harness.workers.prompts[0]!.text;
    expect(attachmentPathsFromPrompt(prompt)).toEqual([path]);
    expect(prompt).toContain(`1. file quarterly.pdf ${bytes.byteLength} B → ${path}`);
    await expectSameBytes(path, bytes);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("附件目录与文件权限分别是 0700 与 0600", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/private.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const message = inbound({
    messageId: "attach-permissions",
    ...target,
    text: "看图",
    attachments: [attachment(url, "image", "private.png", { size: IMAGE_BYTES.byteLength })],
  });
  try {
    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    const directory = messageDirectory(harness, message, message.messageId);
    const path = savedPath(harness, target, message.messageId, 0, "private.png", "image", url);
    // 附件是私有数据：目录同机可读但不可列给别人，文件只有属主可读写。
    expect((await stat(directory)).mode & 0o777).toBe(0o700);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("单条附件超时：按「下载失败」文案拒绝，不建任务也不落盘", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  // 单条 60 秒超时由传输层归类为 timeout；只有整条消息的 120 秒预算耗尽才算「超限」。
  harness.transport.attachmentFailure = new AttachmentError("timeout", "download timed out");
  const message = inbound({
    messageId: "attach-timeout",
    ...target,
    text: "看下附件",
    attachments: [attachment("https://cdn.example.test/slow.bin", "file", "slow.bin")],
  });
  try {
    await harness.service.handleInbound(message);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(1);
    expect(replyCount(harness, REFUSAL_OVER_BUDGET)).toBe(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("整条消息的下载预算耗尽：按「过多或超时」文案拒绝，不建任务也不落盘", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/stalled.bin";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  harness.transport.downloadHangs = true;
  const message = inbound({
    messageId: "attach-budget",
    ...target,
    text: "看下这个大文件",
    attachments: [attachment(url, "file", "stalled.bin", { size: IMAGE_BYTES.byteLength })],
  });
  vi.useFakeTimers();
  try {
    const pending = harness.service.handleInbound(message);
    // 假定时器只接管计时：真实的 fs 步骤照常完成，用真实 I/O 让出事件循环直到下载开始。
    for (let tick = 0; tick < 200 && harness.transport.downloadRequests.length === 0; tick += 1) {
      await stat(harness.stateDir);
    }
    expect(harness.transport.downloadRequests).toHaveLength(1);

    vi.advanceTimersByTime(ATTACHMENT_DOWNLOAD_BUDGET_MS);
    vi.useRealTimers();
    await pending;
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_OVER_BUDGET)).toBe(1);
    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);
  } finally {
    vi.useRealTimers();
    await closeAttachmentHarness(harness);
  }
});

test("语音转写耗尽下载预算：整条任务按「过多或超时」拒绝", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/voice.amr";
  harness.transport.attachmentBodies.set(url, AUDIO_BYTES);
  harness.transport.transcriptionHangs = true;
  const message = inbound({
    messageId: "attach-budget-audio",
    ...target,
    text: "听下这段语音",
    attachments: [attachment(url, "audio", "voice.amr", { size: AUDIO_BYTES.byteLength })],
  });
  vi.useFakeTimers();
  try {
    const pending = harness.service.handleInbound(message);
    for (let tick = 0; tick < 200 && harness.transport.transcriptionRequests.length === 0; tick += 1) {
      await stat(harness.stateDir);
    }
    expect(harness.transport.transcriptionRequests).toHaveLength(1);

    vi.advanceTimersByTime(ATTACHMENT_DOWNLOAD_BUDGET_MS);
    vi.useRealTimers();
    await pending;
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_OVER_BUDGET)).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
  } finally {
    vi.useRealTimers();
    await closeAttachmentHarness(harness);
  }
});

test("附件数量超过 8：回复超限文案、零下载、不建任务", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const attachments = Array.from({ length: 9 }, (_unused, index) => {
    const url = `https://cdn.example.test/many-${index}.png`;
    harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
    return attachment(url, "image", `many-${index}.png`, { size: IMAGE_BYTES.byteLength });
  });
  const message = inbound({
    messageId: "attach-too-many",
    ...target,
    text: "",
    attachments,
  });
  try {
    await harness.service.handleInbound(message);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_OVER_BUDGET)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);

    await harness.service.handleInbound(message);
    await settleReplies(harness, target);
    expect(replyCount(harness, REFUSAL_OVER_BUDGET)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(0);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("attachments.enabled=false：有文本走纯文本任务，无文本回复已关闭文案", async () => {
  const harness = await createAttachmentHarness({
    attachments: { enabled: false, maxBytes: MAX_BYTES },
  });
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/disabled.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "disabled-with-text",
        ...target,
        text: "看下这张图",
        attachments: [attachment(url, "image", "disabled.png")],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["看下这张图"]);
    expect(harness.transport.downloadRequests).toHaveLength(0);

    const silent = inbound({
      messageId: "disabled-without-text",
      ...target,
      text: "",
      attachments: [attachment(url, "image", "disabled.png")],
    });
    await harness.service.handleInbound(silent);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DISABLED)).toBe(1);
    expect(taskCount(harness, target)).toBe(1);
    expect(existsSync(messageDirectory(harness, silent, silent.messageId))).toBe(false);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("attachments 未配置：与关闭一致，纯文本任务照跑且无附件块", async () => {
  const harness = await createAttachmentHarness({ attachments: null });
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/unconfigured.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "unconfigured-with-text",
        ...target,
        text: "看下这张图",
        attachments: [attachment(url, "image", "unconfigured.png")],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["看下这张图"]);

    await harness.service.handleInbound(
      inbound({
        messageId: "unconfigured-without-text",
        ...target,
        text: "",
        attachments: [attachment(url, "image", "unconfigured.png")],
      }),
    );
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DISABLED)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(taskCount(harness, target)).toBe(1);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("非 owner、未 @ 与未允许群的图片消息不产生任何下载", async () => {
  const harness = await createAttachmentHarness();
  const url = "https://cdn.example.test/denied.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const image = [attachment(url, "image", "denied.png")];
  const rejected: InboundMessage[] = [
    inbound({
      messageId: "deny-other-dm",
      channelId: "dm-other",
      channelType: 1,
      senderId: "other-user",
      text: "",
      attachments: image,
    }),
    inbound({
      messageId: "deny-unmentioned",
      channelId: "group-allowed",
      channelType: 2,
      senderId: "owner-1",
      text: "",
      attachments: image,
    }),
    inbound({
      messageId: "deny-unallowed-group",
      channelId: "group-denied",
      channelType: 2,
      senderId: "owner-1",
      mentionedUserIds: [harness.bot.uid],
      text: "",
      attachments: image,
    }),
  ];
  try {
    for (const message of rejected) {
      await harness.service.handleInbound(message);
    }

    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    for (const message of rejected) {
      expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);
    }
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("运行中的回合收到 caption + 图片：steering 注入组装正文且不新建任务", async () => {
  const started = gate();
  const url = "https://cdn.example.test/steer.png";
  const steered: string[] = [];
  const harness = await createAttachmentHarness({
    prompt: async ({ text, waitForAppend }) => {
      if (text !== "长任务") return;
      started.release();
      steered.push((await waitForAppend()).text);
    },
  });
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    const first = harness.service.handleInbound(
      inbound({ messageId: "steer-attach-first", ...target, text: "长任务" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({
        messageId: "steer-attach-image",
        ...target,
        text: "看下这个报错",
        attachments: [attachment(url, "image", "报错.png", { size: IMAGE_BYTES.byteLength })],
      }),
    );
    await first;
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["长任务"]);
    expect(taskCount(harness, target)).toBe(1);
    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(steered).toHaveLength(1);
    expect(steered[0]!).toContain("看下这个报错");
    expect(steered[0]!).toContain(ATTACHMENT_HEADER);
    const path = savedPath(harness, target, "steer-attach-image", 0, "报错.png", "image", url);
    expect(attachmentPathsFromPrompt(steered[0]!)).toEqual([path]);
    await expectSameBytes(path, IMAGE_BYTES);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("命令消息带附件不下载：/status 与 /queue <文本>", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/command.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "command-status",
        ...target,
        text: "/status",
        attachments: [attachment(url, "image", "command.png")],
      }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "command-queue",
        ...target,
        text: "/queue 看下这个报错",
        attachments: [attachment(url, "image", "command.png")],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.transport.downloadRequests).toHaveLength(0);
    // /queue 的正文仍是命令参数本身，不带附件块。
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["看下这个报错"]);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("未注册的 /xxx 带附件：按普通任务处理，附件照常落盘并进正文", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/unregistered.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const text = "/notregistered 看下这张图";
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "command-unregistered",
        ...target,
        text,
        attachments: [attachment(url, "image", "unregistered.png", { size: IMAGE_BYTES.byteLength })],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    // 未注册的 /xxx 不是命令，仍是一次普通任务：附件同样被取回并写进正文。
    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
    const prompt = harness.workers.prompts[0]!.text;
    expect(prompt).toContain(text);
    expect(prompt).toContain(ATTACHMENT_HEADER);
    const path = savedPath(harness, target, "command-unregistered", 0, "unregistered.png", "image", url);
    expect(attachmentPathsFromPrompt(prompt)).toEqual([path]);
    await expectSameBytes(path, IMAGE_BYTES);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("已接受任务的消息重投：不重新下载、不重复回复、已落盘文件保留", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/repost.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const message = inbound({
    messageId: "repost-accepted",
    ...target,
    text: "看图",
    attachments: [attachment(url, "image", "repost.png", { size: IMAGE_BYTES.byteLength })],
  });
  try {
    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await settleReplies(harness, target);
    const path = savedPath(harness, target, message.messageId, 0, "repost.png", "image", url);
    const sentAfterFirst = harness.transport.sent.length;

    await harness.service.handleInbound(message);
    await settleReplies(harness, target);

    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
    expect(harness.transport.sent).toHaveLength(sentAfterFirst);
    await expectSameBytes(path, IMAGE_BYTES);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("半失败只清理本次新建的文件：同 messageId 的既有文件与目录保留", async () => {
  const harness = await createAttachmentHarness();
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const keptUrl = "https://cdn.example.test/kept.png";
  const missingUrl = "https://cdn.example.test/missing.pdf";
  harness.transport.attachmentBodies.set(keptUrl, KEPT_BYTES);
  const failing = attachment(missingUrl, "file", "missing.pdf", { size: 4 });
  try {
    // 既有文件：同 messageId 目录里已落盘、已被接受任务引用的那份。
    // 直接预置而不是先跑一条被接受的消息：真实重投会在 claim 处去重、根本走不到这条清理路径。
    const existing = inbound({
      messageId: "half-failed-existing",
      ...target,
      text: "先看旧图",
      attachments: [attachment(keptUrl, "image", "kept.png", { size: KEPT_BYTES.byteLength })],
    });
    const keptPath = savedPath(harness, target, existing.messageId, 0, "kept.png", "image", keptUrl);
    await mkdir(messageDirectory(harness, existing, existing.messageId), { recursive: true });
    await writeFile(keptPath, KEPT_BYTES);

    await harness.service.handleInbound(
      inbound({
        messageId: existing.messageId,
        ...target,
        text: "先看旧图",
        attachments: [
          attachment(keptUrl, "image", "kept.png", { size: KEPT_BYTES.byteLength }),
          failing,
        ],
      }),
    );
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(1);
    expect(existsSync(messageDirectory(harness, existing, existing.messageId))).toBe(true);
    await expectSameBytes(keptPath, KEPT_BYTES);
    // 既有文件被复用，只有失败的第二个附件发起过下载。
    expect(harness.transport.downloadRequests.map(({ url }) => url)).toEqual([missingUrl]);

    // 全新目录：本次新建的文件必须被删、空目录被移除。
    const fresh = inbound({
      messageId: "half-failed-fresh",
      ...target,
      text: "",
      attachments: [
        attachment(keptUrl, "image", "kept.png", { size: KEPT_BYTES.byteLength }),
        failing,
      ],
    });
    const createdPath = savedPath(harness, target, fresh.messageId, 0, "kept.png", "image", keptUrl);
    await harness.service.handleInbound(fresh);
    await settleReplies(harness, target);

    expect(replyCount(harness, REFUSAL_DOWNLOAD_FAILED)).toBe(2);
    expect(existsSync(createdPath)).toBe(false);
    expect(existsSync(messageDirectory(harness, fresh, fresh.messageId))).toBe(false);
    expect(harness.transport.downloadRequests.map(({ url }) => url)).toEqual([
      missingUrl,
      keptUrl,
      missingUrl,
    ]);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("服务启动清理超过 14 天的附件目录，保留期内的目录不动", async () => {
  const channel = "1-dm-owner";
  const harness = await createAttachmentHarness({
    beforeStart: async (stateDir) => {
      const stale = join(stateDir, "attachments", channel, "stale-message");
      await mkdir(stale, { recursive: true });
      await writeFile(join(stale, "0-old.png"), "old");
      const fifteenDaysAgo = new Date(Date.now() - 15 * 24 * 60 * 60 * 1000);
      await utimes(stale, fifteenDaysAgo, fifteenDaysAgo);
      const fresh = join(stateDir, "attachments", channel, "fresh-message");
      await mkdir(fresh, { recursive: true });
      await writeFile(join(fresh, "0-new.png"), "new");
    },
  });
  try {
    expect(existsSync(join(harness.stateDir, "attachments", channel, "stale-message"))).toBe(false);
    expect(
      await readFile(join(harness.stateDir, "attachments", channel, "fresh-message", "0-new.png"), "utf8"),
    ).toBe("new");
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("未认领状态下的图片私聊静默，认领后同样的图片成为任务", async () => {
  const harness = await createAttachmentHarness({ ownerId: null });
  const target: ChatTarget = { channelId: "dm-unclaimed", channelType: 1 };
  const url = "https://cdn.example.test/claim.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const senderId = "alice";
  try {
    await harness.service.handleInbound(
      inbound({
        messageId: "unclaimed-image",
        ...target,
        senderId,
        text: "",
        attachments: [attachment(url, "image", "claim.png")],
      }),
    );
    await settleReplies(harness, target);

    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(harness.transport.sent).toHaveLength(0);
    expect(ownerCount(harness)).toBe(0);
    expect(harness.workers.prompts).toHaveLength(0);

    await harness.service.handleInbound(
      inbound({ messageId: "unclaimed-text", ...target, senderId, text: "开始工作" }),
    );
    await settleReplies(harness, target);
    expect(ownerCount(harness)).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);

    await harness.service.handleInbound(
      inbound({
        messageId: "claimed-image",
        ...target,
        senderId,
        text: "",
        attachments: [attachment(url, "image", "claim.png")],
      }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.workers.prompts[0]!.text).toContain(ATTACHMENT_HEADER);
    expect(harness.transport.downloadRequests).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("回答问题时的附件不下载也不落盘：答案路径优先于附件组装", async () => {
  const harness = await createAttachmentHarness({
    prompt: async ({ append, promptContext, text }) => {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool missing");
      const result = await ask.execute(
        {
          questions: [
            {
              id: "deploy-target",
              question: `确认部署目标？(${text})`,
              options: [{ label: "staging" }, { label: "production" }],
            },
          ],
        },
        new AbortController().signal,
      );
      await append("answer.log", result);
    },
  });
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/answer.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const answer = inbound({
    messageId: "answer-with-image",
    ...target,
    text: "2",
    attachments: [attachment(url, "image", "answer.png", { size: IMAGE_BYTES.byteLength })],
  });
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "answer-ask-start", ...target, text: "需要确认部署目标" }),
    );
    await harness.workers.waitForPromptCount(1);
    const pending = await waitForStatus(
      harness.service,
      target,
      (status) => status.pendingQuestionIds.length === 1,
    );

    await harness.service.handleInbound(answer);
    const completed = await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );

    // 附件既没下载也没落盘，消息只作为答案被消费。
    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(existsSync(messageDirectory(harness, answer, answer.messageId))).toBe(false);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(taskCount(harness, target)).toBe(1);
    expect(JSON.parse(await readText(join(harness.root, "tmp", "answer.log")))).toEqual({
      answers: [{ id: "deploy-target", questionId: pending.pendingQuestionIds[0]!, answer: "2" }],
    });
  } finally {
    await closeAttachmentHarness(harness);
  }
});

test("注册的观察型 handler 带附件不下载，命令语义与正文不受影响", async () => {
  const observed: Array<{ name: string; argument: string }> = [];
  const harness = await createAttachmentHarness({
    commandHandlers: [
      {
        name: "log",
        intercept: false,
        handle: (context) => {
          observed.push({ name: context.name, argument: context.argument });
        },
      },
    ],
  });
  const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
  const url = "https://cdn.example.test/observed.png";
  harness.transport.attachmentBodies.set(url, IMAGE_BYTES);
  const message = inbound({
    messageId: "observed-with-image",
    ...target,
    text: "/log 带附件的观察命令",
    attachments: [attachment(url, "image", "observed.png", { size: IMAGE_BYTES.byteLength })],
  });
  try {
    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(observed).toEqual([{ name: "log", argument: "带附件的观察命令" }]);
    // 观察型 handler 拿到的是原始命令文本，附件不参与组装、不产生外网请求。
    expect(harness.transport.downloadRequests).toHaveLength(0);
    expect(existsSync(messageDirectory(harness, message, message.messageId))).toBe(false);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["/log 带附件的观察命令"]);
  } finally {
    await closeAttachmentHarness(harness);
  }
});
