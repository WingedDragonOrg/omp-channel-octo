import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import type { ChatTarget, InboundAttachment } from "../src/contracts.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";
import {
  FORK_DM,
  FORK_GROUP,
  ForkTestTransport,
  ForkTestWorkerFactory,
  gate,
  ownerMessage,
  seedBinding,
  waitForForkBoundary,
} from "./fixtures/core-fork.ts";

const targetForFirstThread = (): ChatTarget & { channelType: 5 } => ({
  channelId: `${FORK_GROUP.channelId}____thread-1`,
  channelType: 5,
});

const textsFor = (harness: CoreHarness, target: ChatTarget): string[] =>
  harness.transport.sent
    .filter(({ target: sent }) =>
      sent.channelType === target.channelType && sent.channelId === target.channelId)
    .map(({ text }) => text);

async function seedSourceTurn(harness: CoreHarness, messageId: string): Promise<string> {
  await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, messageId, "seed exact source session"));
  await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "completed");
  const status = await harness.service.status(FORK_GROUP);
  if (!status.sessionId) throw new Error("source session was not persisted");
  return status.sessionId;
}

test("/tan snapshots a held source and runs the materialized request only in the fork target", async () => {
  const sourceStarted = gate();
  const releaseSource = gate();
  const targetStarted = gate();
  const workers = new ForkTestWorkerFactory(async ({ emit, text }) => {
    if (text === "hold source while tan runs") {
      sourceStarted.resolve();
      await releaseSource.promise;
      return;
    }
    if (text === "inspect the independent incident") {
      targetStarted.resolve();
      emit(assistantText("tan-target-result", "target-only result", true));
    }
  });
  workers.holdForkStop = true;
  const transport = new ForkTestTransport();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const command = ownerMessage(
    harness,
    FORK_GROUP,
    "tan-held-source-command",
    "/tan inspect the independent incident",
  );
  let tanRequest: Promise<void> | undefined;
  try {
    await harness.service.submitInbound(
      ownerMessage(harness, FORK_GROUP, "tan-held-source-turn", "hold source while tan runs"),
    );
    await waitForForkBoundary(sourceStarted.promise, "held source start");
    const sourceWorker = workers.workers[0]!;
    const sourceBefore = await harness.service.status(FORK_GROUP);

    tanRequest = harness.service.submitInbound(command);
    await waitForForkBoundary(workers.forkStopStarted.promise, "tan fork preparation stop");
    expect(sourceWorker.stopped).toBe(false);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(sourceBefore.sessionId);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["hold source while tan runs"]);

    workers.releaseForkStop.resolve();
    await tanRequest;
    await waitForForkBoundary(targetStarted.promise, "tan target task start");
    const target = targetForFirstThread();
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.outbox === 0);

    expect(workers.prompts.map(({ text }) => text)).toEqual([
      "hold source while tan runs",
      "inspect the independent incident",
    ]);
    expect(workers.prompts[1]?.sessionId).not.toBe(sourceBefore.sessionId);
    expect(sourceWorker.stopped).toBe(false);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(sourceBefore.sessionId);
    expect((await harness.service.status(FORK_GROUP)).state).toBe("running");
    expect(textsFor(harness, target)).toContain("target-only result");
    expect(textsFor(harness, FORK_GROUP).join("\n")).toContain("thread-1");

    await harness.service.submitInbound(command);
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect(workers.prompts).toHaveLength(2);
  } finally {
    releaseSource.resolve();
    workers.releaseForkStop.resolve();
    await tanRequest?.catch(() => {});
    await closeCoreHarness(harness);
  }
}, 15_000);

test("/tan is owner-only group/thread control and an empty request only returns usage", async () => {
  const transport = new ForkTestTransport();
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const unlisted: ChatTarget = { channelId: "group-tan-unlisted", channelType: 2 };
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-permission-source");
    await seedBinding(harness, FORK_DM, harness.root, "tan-private-source");
    await seedBinding(harness, unlisted, harness.root, "tan-unlisted-source");

    await harness.service.submitInbound({
      ...ownerMessage(harness, FORK_GROUP, "tan-member", "/tan forbidden member request"),
      senderId: "member-2",
    });
    await harness.service.submitInbound({
      ...ownerMessage(harness, FORK_GROUP, "tan-no-mention", "/tan missing mention"),
      mentionedUserIds: [],
    });
    await harness.service.submitInbound(ownerMessage(harness, unlisted, "tan-unlisted", "/tan unlisted"));
    await harness.service.submitInbound(ownerMessage(harness, FORK_DM, "tan-private", "/tan private"));
    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "tan-empty", "/tan   "));
    await transport.waitForRemoteText((message) =>
      message.target.channelType === FORK_GROUP.channelType
      && message.target.channelId === FORK_GROUP.channelId
      && message.text.includes("/tan <"));
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.outbox === 0);

    expect(transport.createAttempts).toBe(0);
    expect(workers.forkAttempts).toHaveLength(0);
    expect(workers.prompts).toHaveLength(0);
    expect(textsFor(harness, FORK_GROUP).join("\n")).toContain("/tan <");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/tan does not answer a source pending ask and dispatches a fresh target task", async () => {
  const sourceAskStarted = gate();
  const targetStarted = gate();
  const workers = new ForkTestWorkerFactory(async ({ promptContext, text }) => {
    if (text === "open source question") {
      const ask = promptContext?.tools?.find(({ name }) => name === "ask");
      if (!ask) throw new Error("ask host tool is missing");
      sourceAskStarted.resolve();
      await ask.execute(
        { questions: [{ question: "source-only choice?" }] },
        new AbortController().signal,
        "tan-source-ask",
      );
      return;
    }
    if (text === "investigate without answering source") targetStarted.resolve();
  });
  const transport = new ForkTestTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  try {
    await harness.service.submitInbound(
      ownerMessage(harness, FORK_GROUP, "tan-pending-ask-source", "open source question"),
    );
    await waitForForkBoundary(sourceAskStarted.promise, "source pending ask");
    const sourceQuestionIds = (await harness.service.status(FORK_GROUP)).pendingQuestionIds;
    expect(sourceQuestionIds).toHaveLength(1);

    await harness.service.submitInbound(ownerMessage(
      harness,
      FORK_GROUP,
      "tan-while-source-asks",
      "/tan investigate without answering source",
    ));
    await waitForForkBoundary(targetStarted.promise, "tan target while source asks");
    const target = targetForFirstThread();
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(workers.prompts.map(({ text }) => text)).toEqual([
      "open source question",
      "investigate without answering source",
    ]);
    expect((await harness.service.status(FORK_GROUP)).pendingQuestionIds).toEqual(sourceQuestionIds);
    expect((await harness.service.status(target)).pendingQuestionIds).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
}, 15_000);

test("/tan materializes its argument, quote, and attachment once into the target task", async () => {
  const bytes = new TextEncoder().encode("tan attachment fixture");
  const url = "https://cdn.example.test/tan-evidence.txt";
  const attachment: InboundAttachment = {
    url,
    kind: "file",
    name: "tan-evidence.txt",
    size: bytes.byteLength,
    durationSeconds: 0,
  };
  const transport = new ForkTestTransport();
  transport.attachmentBodies.set(url, bytes);
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
    attachments: { enabled: true, maxBytes: 4096 },
  });
  const message = inbound({
    ...FORK_GROUP,
    messageId: "tan-materialized-once",
    senderId: "owner-1",
    text: "/tan review the evidence",
    mentionedUserIds: [harness.bot.uid],
    quoted: { senderName: "Alice", text: "the original incident report" },
    attachments: [attachment],
  });
  try {
    await seedSourceTurn(harness, "tan-materialized-source");
    await harness.service.submitInbound(message);
    await workers.waitForPromptCount(2);
    const target = targetForFirstThread();
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    const prompt = workers.prompts[1]?.text ?? "";
    expect(prompt.startsWith("review the evidence")).toBe(true);
    expect(prompt).not.toMatch(/^\/tan(?:\s|$)/u);
    expect(prompt).toContain("[引用 Alice]\nthe original incident report");
    expect(prompt).toContain("附件（已保存到本机；需要时用 read 打开）：");
    expect(prompt).toContain(`1. file tan-evidence.txt ${bytes.byteLength} B → `);
    const savedPath = prompt.match(/→ (.+)$/mu)?.[1]?.trim();
    if (!savedPath) throw new Error("materialized attachment path was absent from target prompt");
    expect(Buffer.from(await readFile(savedPath)).equals(Buffer.from(bytes))).toBe(true);

    await harness.service.submitInbound(message);
    expect(transport.downloadRequests).toHaveLength(1);
    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect(workers.prompts).toHaveLength(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/tan preparation failure is terminal for the duplicate input and leaves the source unchanged", async () => {
  const transport = new ForkTestTransport();
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const command = ownerMessage(harness, FORK_GROUP, "tan-preparation-failure", "/tan cannot prepare");
  try {
    const sourceSessionId = await seedSourceTurn(harness, "tan-failure-source");
    workers.failCreateWith(new Error("fixture native fork preparation failed"));

    await harness.service.submitInbound(command);
    await transport.waitForRemoteText((message) =>
      message.target.channelType === FORK_GROUP.channelType
      && message.target.channelId === FORK_GROUP.channelId
      && message.text.includes("失败"));
    const creationCountAfterFailure = workers.creations.length;

    await harness.service.submitInbound(command);
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.outbox === 0);

    expect(workers.creations).toHaveLength(creationCountAfterFailure);
    expect(transport.createAttempts).toBe(0);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["seed exact source session"]);
    expect((await harness.service.status(FORK_GROUP)).sessionId).toBe(sourceSessionId);
    expect(textsFor(harness, FORK_GROUP).filter((text) => text.includes("失败"))).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("source /cancel aborts in-flight /tan preparation and does not dispatch the target request", async () => {
  const sourceStarted = gate();
  const releaseSource = gate();
  const workers = new ForkTestWorkerFactory(async ({ text }) => {
    if (text === "hold source for tan cancellation") {
      sourceStarted.resolve();
      await releaseSource.promise;
    }
  });
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const harness = await createCoreHarness({
    transport,
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let tanRequest: Promise<void> | undefined;
  try {
    await harness.service.submitInbound(ownerMessage(
      harness,
      FORK_GROUP,
      "tan-cancel-source",
      "hold source for tan cancellation",
    ));
    await waitForForkBoundary(sourceStarted.promise, "tan cancellation source start");
    tanRequest = harness.service.submitInbound(ownerMessage(
      harness,
      FORK_GROUP,
      "tan-cancel-operation",
      "/tan request that must not run",
    )).catch(() => {});
    await waitForForkBoundary(transport.createThreadStarted.promise, "tan remote create boundary");

    await harness.service.submitInbound(ownerMessage(harness, FORK_GROUP, "tan-cancel-command", "/cancel"));
    transport.releaseCreateThread.resolve();
    await tanRequest;

    expect(transport.createAttempts).toBe(1);
    expect(workers.forkAttempts).toHaveLength(1);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["hold source for tan cancellation"]);
    await waitForStatus(harness.service, FORK_GROUP, (status) => status.state === "cancelled");
    await transport.waitForRemoteText((message) =>
      message.target.channelType === FORK_GROUP.channelType
      && message.target.channelId === FORK_GROUP.channelId
      && message.text.includes("TAN 已取消"));
  } finally {
    releaseSource.resolve();
    transport.releaseCreateThread.resolve();
    await tanRequest?.catch(() => {});
    await closeCoreHarness(harness);
  }
}, 15_000);
