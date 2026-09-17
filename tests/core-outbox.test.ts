import { expect, test, vi } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, SendReceipt } from "../src/contracts.ts";
import { splitUtf8 } from "../src/outbox";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  FakeTransport,
  waitForStatus,
} from "./fixtures/core-fakes";

function isBody(text: string, expected: string): boolean {
  return text === expected;
}

async function flushPromises(): Promise<void> {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    await Promise.resolve();
  }
}

/**
 * A server that keeps refusing one particular body with a transient-shaped HTTP 500 while every
 * other message to the same chat goes through. Models the live failure where one delivery can
 * never be confirmed.
 */
class RefusingBodyTransport extends FakeTransport {
  failBody: string | undefined;

  override async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    if (text === this.failBody) {
      const error = Object.assign(
        new Error(
          "Octo send message failed with HTTP 500 (err.server.bot_api.send_failed): internal error",
        ),
        { status: 500 },
      );
      this.attempts.push({ target: { ...target }, text, clientMsgNo, error: error.message });
      throw error;
    }
    return super.sendText(target, text, clientMsgNo, signal);
  }
}

test("a completed task is not rerun when an assistant body outbox item is resent after reconnect", async () => {
  const transport = new FakeTransport({
    bot: { uid: "bot-outbox-single", name: "Outbox Bot" },
    online: false,
    startOnline: false,
  });
  const body = "offline assistant body";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append, emit, text }) => {
      await append("effects.log", `${text}\n`);
      emit(assistantText("assistant-offline-body", body, true));
    },
  });
  try {
    const message = inbound({
      messageId: "outbox-single",
      channelId: "dm-outbox-single",
      channelType: 1,
      text: "offline work",
    });
    await harness.service.handleInbound(message);
    await harness.workers.waitForPromptCount(1);
    const completedWithOutbox = await waitForStatus(
      harness.service,
      message,
      (status) => status.state === "completed" && status.outbox > 0,
    );
    expect(completedWithOutbox.outbox).toBeGreaterThan(0);
    const failedHead = await transport.waitForAttempt((attempt) => Boolean(attempt.error));
    expect(failedHead.text).toBe(body);

    await transport.reconnect();
    await waitForStatus(harness.service, message, (status) => status.outbox === 0);

    const headAttempts = transport.attempts.filter(({ clientMsgNo }) => clientMsgNo === failedHead.clientMsgNo);
    expect(headAttempts.length).toBeGreaterThanOrEqual(2);
    expect(headAttempts.some(({ error }) => Boolean(error))).toBe(true);
    expect(headAttempts.at(-1)?.error).toBeUndefined();
    const successfulBodies = transport.sent.filter(({ text }) => isBody(text, body));
    expect(successfulBodies).toHaveLength(1);
    expect((await readText(join(harness.root, "tmp", "effects.log"))).trim()).toBe(
      "offline work",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a body that fails once is retried by itself and the next reply follows without a reconnect", async () => {
  vi.useFakeTimers();
  const transport = new FakeTransport({
    bot: { uid: "bot-outbox-order", name: "Outbox Bot" },
  });
  const firstBody = "first assistant body";
  const secondBody = "second assistant body";
  transport.failOnceWhen(({ text }) => text === firstBody);
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, text }) => {
      emit(assistantText(
        text === "first" ? "assistant-first-body" : "assistant-second-body",
        text === "first" ? firstBody : secondBody,
        true,
      ));
    },
  });
  try {
    const target = { channelId: "dm-outbox-order", channelType: 1 as const };
    const first = inbound({ messageId: "outbox-first", ...target, text: "first" });
    const second = inbound({ messageId: "outbox-second", ...target, text: "second" });
    await harness.service.handleInbound(first);
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      first,
      (status) => status.state === "completed" && status.outbox > 0,
    );
    await harness.service.handleInbound(second);
    await harness.workers.waitForPromptCount(2);

    for (let elapsed = 0; elapsed < 60_000; elapsed += 250) {
      vi.advanceTimersByTime(250);
      await flushPromises();
      if (transport.sent.some(({ text }) => text === secondBody)) break;
    }
    // The failed body is retried by itself (stable client ID) and the queue keeps its order.
    expect(
      transport.sent
        .filter(({ text }) => isBody(text, firstBody) || isBody(text, secondBody))
        .map(({ text }) => text),
    ).toEqual([firstBody, secondBody]);
    const firstAttempts = transport.attempts.filter(({ text }) => isBody(text, firstBody));
    expect(firstAttempts.length).toBeGreaterThanOrEqual(2);
    expect(new Set(firstAttempts.map(({ clientMsgNo }) => clientMsgNo)).size).toBe(1);
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a body the server keeps refusing is given up within a bounded budget and never mutes later replies", async () => {
  vi.useFakeTimers();
  const firstBody = "body the server keeps refusing";
  const secondBody = "body after the refused one";
  const transport = new RefusingBodyTransport({
    bot: { uid: "bot-outbox-budget", name: "Outbox Bot" },
  });
  transport.failBody = firstBody;
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, text }) => {
      emit(assistantText(
        text === "first" ? "assistant-budget-first" : "assistant-budget-second",
        text === "first" ? firstBody : secondBody,
        true,
      ));
    },
  });
  try {
    const target = { channelId: "dm-outbox-budget", channelType: 1 as const };
    const first = inbound({ messageId: "budget-first", ...target, text: "first" });
    const second = inbound({ messageId: "budget-second", ...target, text: "second" });
    await harness.service.handleInbound(first);
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(
      harness.service,
      first,
      (status) => status.state === "completed" && status.outbox > 0,
    );
    // The first body is still being retried; the second message arrives behind it and must get
    // its delivery once the refused one is given up — no reconnect, no manual cleanup.
    await harness.service.handleInbound(second);
    await harness.workers.waitForPromptCount(2);

    let delivered = false;
    for (let elapsed = 0; elapsed < 300_000 && !delivered; elapsed += 500) {
      vi.advanceTimersByTime(500);
      await flushPromises();
      delivered = transport.sent.some(({ text }) => isBody(text, secondBody));
    }

    // Bounded: retries back off instead of resending once per second for hours. Five minutes of
    // attempted retries stay within a handful of attempts.
    const firstAttempts = transport.attempts.filter(({ text }) => isBody(text, firstBody));
    expect(firstAttempts.length).toBeGreaterThanOrEqual(2);
    expect(firstAttempts.length).toBeLessThanOrEqual(8);
    expect(new Set(firstAttempts.map(({ clientMsgNo }) => clientMsgNo)).size).toBe(1);
    expect(delivered).toBe(true);

    await waitForStatus(
      harness.service,
      target,
      (status) => status.outbox === 0 && status.unknownOutbox === 0,
    );
    // The abandoned row stays on disk with the real failure reason, and no longer occupies the queue.
    const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
    try {
      const row = database
        .query("SELECT state, last_error FROM outbox WHERE client_msg_no = ?")
        .get(firstAttempts[0]!.clientMsgNo) as { state?: string; last_error?: string } | null;
      expect(row?.state).toBe("dead");
      expect(row?.last_error).toContain("HTTP 500");
    } finally {
      database.close();
    }
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});


test("a restart delivers the pending outbox the previous process left behind", async () => {
  const bot = { uid: "bot-outbox-restart", name: "Outbox Bot" };
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-outbox-restart-")));
  const body = "body pending across the restart";
  const message = inbound({
    messageId: "outbox-restart",
    channelId: "dm-outbox-restart",
    text: "run while the transport is down",
  });
  try {
    const offline = new FakeTransport({ bot, online: false, startOnline: false });
    const first = await createCoreHarness({
      root,
      bot,
      transport: offline,
      prompt: async ({ emit }) => {
        emit(assistantText("assistant-outbox-restart", body, true));
      },
    });
    try {
      await first.service.handleInbound(message);
      await first.workers.waitForPromptCount(1);
      await waitForStatus(
        first.service,
        message,
        (status) => status.state === "completed" && status.outbox > 0,
      );
    } finally {
      await closeCoreHarness(first, { remove: false });
    }

    const online = new FakeTransport({ bot });
    const second = await createCoreHarness({ root, bot, transport: online });
    try {
      expect(second.workers.prompts).toHaveLength(0);
      await waitForStatus(second.service, message, (status) => status.outbox === 0);
      expect(online.sent.some(({ text }) => isBody(text, body))).toBe(true);
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("offline media remains retryable after repeated unknown attempts and exposes delivery uncertainty", async () => {
  let runFile: (() => Promise<void>) | undefined;
  let fileToolReady: (() => void) | undefined;
  const fileReady = new Promise<void>((resolve) => {
    fileToolReady = resolve;
  });
  const transport = new FakeTransport({
    bot: { uid: "bot-outbox-file", name: "Outbox Bot" },
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ promptContext }) => {
      const fileTool = promptContext?.tools?.find(({ name }) => name === "octo_send_file");
      if (!fileTool) throw new Error("file tool missing");
      const signal = new AbortController().signal;
      await new Promise<void>((resolve, reject) => {
        runFile = async () => {
          try {
            await fileTool.execute({ path: "payload.bin", kind: "file" }, signal);
            resolve();
          } catch (error) {
            reject(error);
          }
        };
        fileToolReady?.();
      });
    },
  });
  try {
    const cwd = join(harness.root, "media cwd");
    await mkdir(cwd, { recursive: true });
    await writeFile(join(cwd, "payload.bin"), "fixture payload", "utf8");
    const target = inbound({
      messageId: "outbox-file",
      channelId: "dm-outbox-file",
      text: "send a file",
    });
    await harness.service.handleInbound(
      inbound({ ...target, messageId: "outbox-file-bind", text: `/new ${cwd}` }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "idle");
    await harness.service.handleInbound(target);
    await harness.workers.waitForPromptCount(1);
    await fileReady;
    transport.setOffline();
    const executeFile = runFile;
    if (!executeFile) throw new Error("file tool was not prepared");
    await executeFile();
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.outbox > 0,
    );
    for (let attempt = 0; attempt < 80 && transport.fileAttempts.length < 3; attempt += 1) {
      await Bun.sleep(50);
    }
    expect(transport.fileAttempts.length).toBeGreaterThanOrEqual(3);
    expect((await harness.service.status(target)).unknownOutbox).toBeGreaterThan(0);

    await transport.reconnect();
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    expect(transport.sentFiles).toHaveLength(1);
    expect(
      transport.sent.some(({ text }) => text.includes("送达状态未知")),
    ).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a body cuts inside its own window at the highest available natural boundary", () => {
  // 换行优先于句末标点、逗号与空白
  expect(splitUtf8("ab\ncd。ef，gh", 12)).toEqual(["ab\n", "cd。ef，gh"]);
  // 没有换行时取句末标点
  expect(splitUtf8("abcdef。gh，ij", 10)).toEqual(["abcdef。", "gh，ij"]);
  // 没有句末标点时退回逗号
  expect(splitUtf8("上下，左右前后，甲乙", 12)).toEqual(["上下，", "左右前后", "，甲乙"]);
  // 英文没有标点时按空白断词，不把词切成两半
  expect(splitUtf8("keep words whole", 10)).toEqual(["keep ", "words ", "whole"]);
});

test("ascii punctuation counts as a cut only where it ends a clause", () => {
  // URL 里的点后面不是空白，不算断点，只能硬切
  expect(splitUtf8("https://example.com/abcdef", 20)).toEqual(["https://example.com/", "abcdef"]);
  // 小数里的点同理，真正的断点是后面的空白
  expect(splitUtf8("value 3.14 stays whole", 12)).toEqual(["value 3.14 ", "stays whole"]);
});

test("a 3000-byte body cuts at the paragraph, sentence, or comma boundary of its own window", () => {
  const paragraphs = `${"字".repeat(900)}\n${"文".repeat(900)}`;
  expect(splitUtf8(paragraphs).map((chunk) => Buffer.byteLength(chunk, "utf8"))).toEqual([2_701, 2_700]);
  expect(splitUtf8(paragraphs)[0]).toBe(`${"字".repeat(900)}\n`);

  const sentences = `${"字".repeat(950)}。`.repeat(2);
  expect(splitUtf8(sentences)).toEqual([`${"字".repeat(950)}。`, `${"字".repeat(950)}。`]);

  const clauses = "一二三，".repeat(300);
  expect(splitUtf8(clauses).map((chunk) => Buffer.byteLength(chunk, "utf8"))).toEqual([3_000, 600]);
  expect(splitUtf8(clauses)[0]).toBe("一二三，".repeat(250));
});

test("only a body without any boundary hard-cuts at the byte limit", () => {
  expect(splitUtf8("x".repeat(6_500)).map((chunk) => Buffer.byteLength(chunk, "utf8"))).toEqual([3_000, 3_000, 500]);
});

test("no cut goes through a multi-byte character", () => {
  expect(splitUtf8("字".repeat(10), 9)).toEqual(["字字字", "字字字", "字字字", "字"]);
  expect(splitUtf8("🙂".repeat(5), 10)).toEqual(["🙂🙂", "🙂🙂", "🙂"]);
  const emojiStraddle = `${"e".repeat(2_997)}${"🙂".repeat(11)}`;
  expect(splitUtf8(emojiStraddle)).toEqual(["e".repeat(2_997), "🙂".repeat(11)]);
});

test("a boundary decided by a full window never moves when the body grows", () => {
  const text = "句子内容。".repeat(500);
  const characters = Array.from(text);
  const finalChunks = splitUtf8(text);
  expect(finalChunks.length).toBeGreaterThan(2);
  for (const share of [0.25, 0.5, 0.75, 0.95]) {
    const chunks = splitUtf8(characters.slice(0, Math.floor(characters.length * share)).join(""));
    // 还在累计的最后一段之外，此前切出的每一段都必须逐字保持
    expect(chunks.slice(0, -1)).toEqual(finalChunks.slice(0, chunks.length - 1));
  }
  expect(finalChunks.join("")).toBe(text);
  expect(finalChunks.every((chunk) => Buffer.byteLength(chunk, "utf8") <= 3_000)).toBe(true);
});
