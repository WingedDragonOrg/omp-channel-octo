import { expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
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

test("a failed earlier assistant body blocks later replies until the next reconnect", async () => {
  let bodyFailures = 0;
  const transport = new FakeTransport({
    bot: { uid: "bot-outbox-order", name: "Outbox Bot" },
    online: false,
    startOnline: false,
  });
  const firstBody = "first assistant body";
  const secondBody = "second assistant body";
  transport.failOnceWhen(({ text }) => {
    if (isBody(text, firstBody) && bodyFailures === 0) {
      bodyFailures += 1;
      return true;
    }
    return false;
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ append, emit, text }) => {
      await append("effects.log", `${text}\n`);
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
    await harness.service.handleInbound(second);
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, first, (status) => status.state === "completed");
    await waitForStatus(harness.service, second, (status) => status.state === "completed");

    await transport.reconnect();
    const failedFirst = await transport.waitForAttempt(
      (attempt) => Boolean(attempt.error) && isBody(attempt.text, firstBody),
    );
    expect(transport.sent.filter(({ text }) => isBody(text, firstBody) || isBody(text, secondBody))).toHaveLength(0);

    await transport.reconnect();
    await waitForStatus(harness.service, second, (status) => status.outbox === 0);
    const bodyAttempts = transport.attempts.filter(({ text }) => isBody(text, firstBody) || isBody(text, secondBody));
    const firstAttempts = bodyAttempts.filter(({ text }) => isBody(text, firstBody));
    const secondAttempts = bodyAttempts.filter(({ text }) => isBody(text, secondBody));
    expect(firstAttempts.length).toBeGreaterThanOrEqual(2);
    expect(firstAttempts.every(({ clientMsgNo }) => clientMsgNo === failedFirst.clientMsgNo)).toBe(true);
    expect(firstAttempts.some(({ error }) => error === undefined)).toBe(true);
    expect(secondAttempts.length).toBeGreaterThanOrEqual(1);
    expect(new Set(secondAttempts.map(({ clientMsgNo }) => clientMsgNo)).size).toBe(1);
    expect(secondAttempts[0]?.clientMsgNo).not.toBe(failedFirst.clientMsgNo);
    expect(transport.sent.filter(({ text }) => isBody(text, firstBody) || isBody(text, secondBody)).map(({ text }) => text)).toEqual([
      firstBody,
      secondBody,
    ]);
    expect((await readText(join(harness.root, "tmp", "effects.log"))).trim().split("\n")).toEqual([
      "first",
      "second",
    ]);
  } finally {
    await closeCoreHarness(harness);
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
