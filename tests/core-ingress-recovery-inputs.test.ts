import { expect, test } from "bun:test";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { AttachmentConfig, ChannelStatus, ChatTarget, InboundMessage } from "../src/contracts.ts";
import { recvIngressSourceId, type IngressJobRecord } from "../src/inbound/ingress-types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";

const OWNER = "owner-1";
const TARGET = { channelType: 1 as const, channelId: "dm-recovery-inputs" };
const WAIT_TIMEOUT_MS = 3_000;

async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + WAIT_TIMEOUT_MS;
  while (performance.now() < deadline) {
    if (await check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`timed out waiting for ${label} after ${WAIT_TIMEOUT_MS} ms`);
}

async function awaitStep<T>(label: string, operation: Promise<T>): Promise<T> {
  let outcome: { kind: "fulfilled"; value: T } | { kind: "rejected"; error: unknown } | undefined;
  void operation.then(
    value => { outcome = { kind: "fulfilled", value }; },
    error => { outcome = { kind: "rejected", error }; },
  );
  await waitUntil(() => outcome !== undefined, label);
  if (outcome?.kind === "rejected") throw outcome.error;
  if (outcome === undefined) throw new Error(`missing result after ${label}`);
  return outcome.value;
}

async function waitForHarnessStatus(
  service: CoreHarness["service"],
  target: ChatTarget,
  predicate: (status: ChannelStatus) => boolean,
  label: string,
): Promise<ChannelStatus> {
  let latest: ChannelStatus | undefined;
  await waitUntil(async () => {
    latest = await awaitStep(`${label} status read`, service.status(target));
    return predicate(latest);
  }, label);
  if (latest === undefined) throw new Error(`status missing after ${label}`);
  return latest;
}


async function observedState(promise: Promise<unknown>, turns = 4): Promise<"pending" | "fulfilled" | "rejected"> {
  let state: "pending" | "fulfilled" | "rejected" = "pending";
  void promise.then(
    () => { state = "fulfilled"; },
    () => { state = "rejected"; },
  );
  for (let turn = 0; turn < turns; turn += 1) {
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  return state;
}

interface UnknownSessionFixture {
  readonly restarted: CoreHarness;
  readonly target: ChatTarget;
  readonly cwd: string;
  readonly oldSessionId: string;
  readonly switchJobId: number;
}

async function createUnknownSessionFixture(options: {
  prompt?: FakePrompt;
  attachments?: AttachmentConfig;
  beforeUnknownSwitch?: (harness: CoreHarness, target: ChatTarget) => Promise<void>;
  restartTransport?: (bot: CoreHarness["bot"]) => FakeTransport;
} = {}): Promise<UnknownSessionFixture> {
  const workers = new FakeWorkerFactory(options.prompt);
  const firstStartup = createCoreHarness({
    workers,
    ...(options.attachments === undefined ? {} : { attachments: options.attachments }),
  });
  let first: CoreHarness;
  try {
    first = await awaitStep("initial CoreService startup", firstStartup);
  } catch (error) {
    void firstStartup.then(harness => closeCoreHarness(harness)).catch(() => {});
    throw error;
  }
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  const cwd = join(first.root, "recovery-inputs-cwd");
  let firstStopped = false;
  let restarted: CoreHarness | undefined;
  let restartPromise: Promise<CoreHarness> | undefined;

  try {
    await mkdir(cwd, { recursive: true });
    const initialMessage = inbound({
      ...TARGET,
      messageId: "recovery-inputs-initial-new",
      text: `/new ${cwd}`,
    });
    const initial = first.service.receive(initialMessage);
    await awaitStep("initial /new ACK", initial.ackReady);
    await waitUntilIngressState(first, initialMessage, "handed-off");
    const initialStatus = await waitForHarnessStatus(
      first.service,
      TARGET,
      status => status.sessionId !== null,
      "initial session binding",
    );
    const oldSessionId = initialStatus.sessionId;
    if (oldSessionId === null) throw new Error("initial session was not bound");
    if (options.beforeUnknownSwitch !== undefined) {
      await options.beforeUnknownSwitch(first, TARGET);
    }

    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
      throw new Error("fixture candidate interrupted with the service");
    };
    const switchMessage = inbound({
      ...TARGET,
      messageId: "recovery-inputs-unknown-new",
      text: `/new ${cwd}`,
    });
    const switching = first.service.receive(switchMessage);
    await awaitStep("unknown /new ACK", switching.ackReady);
    await awaitStep("candidate worker creation gate", candidateStarted.promise);

    const setup = await openChannelStore(first.stateDir);
    let switchJobId = 0;
    try {
      const switchJob = setup.getIngressJobBySource({
        botUid: first.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(TARGET.channelType, TARGET.channelId, switchMessage.messageId),
      });
      if (switchJob === null) throw new Error("unknown switch ingress was not persisted");
      switchJobId = switchJob.id;
      const candidateLedgerId = `new-ingress-${switchJob.id}`;
      if (!setup.recordIngressWorker(switchJob.id, {
        id: candidateLedgerId,
        botUid: first.bot.uid,
        sessionId: "recovery-inputs-candidate-session",
        sessionFile: join(cwd, "candidate.session"),
        pid: 4242,
        pgid: 4242,
        identity: "recovery-inputs-candidate-process",
        cwd,
        moveTargetCwd: null,
        state: "running",
      })) throw new Error("candidate worker ledger was not linked to the switch");
      setup.markWorkerStopped(candidateLedgerId);
    } finally {
      setup.close();
    }

    const stopping = first.service.stop();
    releaseCandidate.resolve();
    await awaitStep("stopping the interrupted CoreService", stopping);
    firstStopped = true;
    workers.beforeCreate = undefined;

    const transport = options.restartTransport?.(first.bot);
    restartPromise = createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers,
      ...(options.attachments === undefined ? {} : { attachments: options.attachments }),
      ...(transport === undefined ? {} : { transport }),
    });
    restarted = await awaitStep("restarting CoreService with the unknown barrier", restartPromise);
    restartPromise = undefined;
    const store = await openChannelStore(restarted.stateDir);
    try {
      await waitUntil(() => store.getIngressJobBySource({
        botUid: first.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(TARGET.channelType, TARGET.channelId, switchMessage.messageId),
      })?.state === "unknown", "unknown session-switch barrier");
    } finally {
      store.close();
    }
    return { restarted, target: TARGET, cwd, oldSessionId, switchJobId };
  } catch (error) {
    releaseCandidate.resolve();
    if (!firstStopped) {
      await awaitStep("stopping CoreService after fixture setup failure", first.service.stop()).catch(() => {});
    }
    if (restarted !== undefined) await awaitStep("cleaning up after fixture setup failure", closeCoreHarness(restarted));
    else if (restartPromise !== undefined) {
      void restartPromise
        .then(harness => closeCoreHarness(harness), () => rm(first.root, { recursive: true, force: true }))
        .catch(() => {});
    } else await rm(first.root, { recursive: true, force: true });
    throw error;
  }
}

function ingressSourceId(message: InboundMessage): string {
  return recvIngressSourceId(message.channelType, message.channelId, message.messageId);
}

async function closeUnknownSessionFixture(fixture: UnknownSessionFixture): Promise<void> {
  await awaitStep("stopping the recovery CoreService", closeCoreHarness(fixture.restarted));
}

async function waitUntilIngressState(
  harness: CoreHarness,
  message: InboundMessage,
  expected: IngressJobRecord["state"],
): Promise<IngressJobRecord> {
  const store = await openChannelStore(harness.stateDir);
  try {
    let observed = store.getIngressJobBySource({
      botUid: harness.bot.uid,
      sourceKind: "recv",
      sourceId: ingressSourceId(message),
    });
    await waitUntil(() => {
      observed = store.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: ingressSourceId(message),
      });
      return observed?.state === expected;
    }, `ingress ${message.messageId} state ${expected}`);
    if (observed === null) throw new Error(`ingress ${message.messageId} was not persisted`);
    return observed;
  } finally {
    store.close();
  }
}

test("preparing attachment remains unacknowledged through recovery and redelivery cannot execute it", async () => {
  const downloadStarted = Promise.withResolvers<void>();
  const downloadFinished = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const bytes = new Uint8Array([0x52, 0x45, 0x43, 0x4f, 0x56, 0x45, 0x52, 0x59]);
  let transport: FakeTransport | undefined;
  let downloadCount = 0;
  let downloadedBytes: Uint8Array | undefined;

  const fixture = await createUnknownSessionFixture({
    attachments: { enabled: true, maxBytes: 1024 * 1024 },
    restartTransport: bot => {
      const nextTransport = new FakeTransport({ bot });
      nextTransport.downloadAttachment = async request => {
        downloadCount += 1;
        downloadStarted.resolve();
        await releaseDownload.promise;
        await writeFile(request.path, bytes, { mode: 0o600 });
        downloadedBytes = new Uint8Array(await readFile(request.path));
        downloadFinished.resolve();
        return bytes.byteLength;
      };
      transport = nextTransport;
      return nextTransport;
    },
  });
  const mediaUrl = "https://cdn.example.test/recovery-pending-media.bin";
  const message = inbound({
    ...fixture.target,
    messageId: "recovery-inputs-preparing-media",
    text: "inspect this attachment after recovery",
    attachments: [{
      kind: "file",
      name: "recovery-evidence.bin",
      size: bytes.byteLength,
      durationSeconds: 0,
      url: mediaUrl,
    }],
  });
  let released = false;

  try {
    const activeTransport = transport;
    if (activeTransport === undefined) throw new Error("restart transport was not created");
    const receipt = fixture.restarted.service.receive(message);
    const ackOutcome = receipt.ackReady.then(() => "fulfilled" as const, () => "rejected" as const);
    await awaitStep("attachment download started", downloadStarted.promise);
    expect(await observedState(receipt.ackReady)).toBe("pending");

    const store = await openChannelStore(fixture.restarted.stateDir);
    try {
      const preparing = store.getIngressJobBySource({
        botUid: fixture.restarted.bot.uid,
        sourceKind: "recv",
        sourceId: ingressSourceId(message),
      });
      expect(preparing).toMatchObject({
        messageId: message.messageId,
        state: "preparing",
        ackReady: false,
        consumerKind: null,
        consumerId: null,
      });
      expect(preparing?.payload).toMatchObject({ kind: "recv", message: { messageId: message.messageId } });
      expect(preparing?.routeSnapshot?.barrierJobId).toBe(fixture.switchJobId);
    } finally {
      store.close();
    }

    const duplicateBeforeRecovery = fixture.restarted.service.receive(message);
    expect(await observedState(duplicateBeforeRecovery.ackReady)).toBe("pending");
    expect(downloadCount).toBe(1);

    const recovery = fixture.restarted.service.receive(inbound({
      ...fixture.target,
      messageId: "recovery-inputs-explicit-new",
      text: `/new ${fixture.cwd}`,
    }));
    await awaitStep("explicit recovery /new ACK", recovery.ackReady);
    await waitForHarnessStatus(
      fixture.restarted.service,
      fixture.target,
      status => status.sessionId !== null && status.sessionId !== fixture.oldSessionId,
      "recovered session binding",
    );
    await waitUntil(
      () => activeTransport.sent.some(({ text }) => text.includes(message.messageId)),
      "identity-specific not-executed recovery notice",
    );

    // Recovery may durably decide not to run the message, but it cannot ACK a body whose attachment
    // is still only a URL and whose preparation has not completed.
    expect(await observedState(receipt.ackReady)).toBe("pending");
    const duplicateAfterRecovery = fixture.restarted.service.receive(message);
    expect(await observedState(duplicateAfterRecovery.ackReady)).toBe("pending");
    expect(downloadCount).toBe(1);

    releaseDownload.resolve();
    released = true;
    await awaitStep("attachment download completion", downloadFinished.promise);
    expect(downloadedBytes).toEqual(bytes);
    expect(await awaitStep("preparing message ACK after disposition", ackOutcome)).toBe("fulfilled");
    await awaitStep("pre-recovery duplicate ACK", duplicateBeforeRecovery.ackReady);
    await awaitStep("post-recovery duplicate ACK", duplicateAfterRecovery.ackReady);

    const after = await openChannelStore(fixture.restarted.stateDir);
    try {
      const disposition = after.getIngressJobBySource({
        botUid: fixture.restarted.bot.uid,
        sourceKind: "recv",
        sourceId: ingressSourceId(message),
      });
      expect(disposition).toMatchObject({
        messageId: message.messageId,
        ackReady: true,
        payload: {
          kind: "recv",
          message: {
            messageId: message.messageId,
            attachments: [{ url: mediaUrl, name: "recovery-evidence.bin" }],
          },
        },
      });
      if (disposition === null) throw new Error("prepared recovery input was not persisted");
      const persistedSpool = disposition.spoolPaths[0];
      if (persistedSpool === undefined) throw new Error("recovery attachment spool was not persisted");
      expect(new Uint8Array(await readFile(persistedSpool))).toEqual(bytes);
      expect(after.getTaskByMessageId(fixture.restarted.bot.uid, message, message.messageId)).toBeNull();
    } finally {
      after.close();
    }

    const notices = activeTransport.sent.filter(({ target, text }) =>
      target.channelType === fixture.target.channelType
      && target.channelId === fixture.target.channelId
      && text.includes(message.messageId));
    expect(notices).toHaveLength(1);
    expect(notices[0]?.text).toMatch(/(?:未|没有|尚未).{0,12}(?:执行|派发)/u);
    expect(notices[0]?.text).toMatch(/(?:重发|重新发送)/u);
    expect(notices[0]?.text).toContain("忽略");
    expect(notices[0]?.text).not.toContain(mediaUrl);
    expect(fixture.restarted.workers.prompts.some(({ text }) => text.includes(message.text))).toBe(false);
    expect(downloadCount).toBe(1);
  } finally {
    if (!released) releaseDownload.resolve();
    await closeUnknownSessionFixture(fixture);
  }
});


interface PreparedQuestionConsumption {
  consumerId: string;
  persist(): boolean;
  publishAfterCommit(): unknown;
}

interface QuestionSnapshotSeam {
  prepareTextSnapshot(...args: unknown[]): PreparedQuestionConsumption;
}

test("a recovery-held ask answer keeps its original question identity and lifecycle", async () => {
  let seedTaskId: string | undefined;
  const fixture = await createUnknownSessionFixture({
    beforeUnknownSwitch: async harness => {
      const seed = inbound({
        ...TARGET,
        messageId: "recovery-inputs-ask-task",
        text: "create the question-owning task",
      });
      await awaitStep("seed ask-task ACK", harness.service.receive(seed).ackReady);
      await waitUntil(() => harness.workers.prompts.length >= 1, "seed ask-task prompt");
      await waitForHarnessStatus(harness.service, TARGET, status => status.state === "completed", "seed task completion");
      const store = await openChannelStore(harness.stateDir);
      try {
        const task = store.getTaskByMessageId(harness.bot.uid, seed, seed.messageId);
        if (task === null) throw new Error("question owner task is missing");
        seedTaskId = task.id;
      } finally {
        store.close();
      }
    },
  });
  const originalQuestionId = "recovery-inputs-original-question";
  const newerQuestionId = "recovery-inputs-newer-question";
  const originalReply = inbound({
    ...fixture.target,
    messageId: "recovery-inputs-original-answer",
    text: "2",
  });
  const promptCountBeforeRecovery = fixture.restarted.workers.prompts.length;
  let questionRetargetRaceObserved = false;
  let mutationStore: ChannelStore | undefined;

  try {
    if (seedTaskId === undefined) throw new Error("question owner task was not created");
    const setup = await openChannelStore(fixture.restarted.stateDir);
    try {
      setup.createQuestion({
        id: originalQuestionId,
        taskId: seedTaskId,
        botUid: fixture.restarted.bot.uid,
        ...fixture.target,
        payload: JSON.stringify({
          id: "deployment-target",
          question: "Which deployment target?",
          options: [{ label: "staging" }, { label: "production" }],
          toolCallId: "recovery-inputs-q1-call",
        }),
      });
    } finally {
      setup.close();
    }
    const replyReceipt = fixture.restarted.service.receive(originalReply);
    await awaitStep("ask answer ingress ACK", replyReceipt.ackReady);
    const accepted = await waitUntilIngressState(fixture.restarted, originalReply, "accepted");
    expect(accepted).toMatchObject({
      messageId: originalReply.messageId,
      ackReady: true,
      consumerKind: null,
      consumerId: null,
      routeSnapshot: {
        purpose: { kind: "questions", questionIds: [originalQuestionId] },
        barrierJobId: fixture.switchJobId,
      },
    });

    mutationStore = await openChannelStore(fixture.restarted.stateDir);
    if (!mutationStore.answerQuestion(originalQuestionId, fixture.restarted.bot.uid, fixture.target, "1", OWNER)) {
      throw new Error("fixture failed to settle the original question");
    }
    const desk = (fixture.restarted.service as unknown as { questions: QuestionSnapshotSeam }).questions;
    const prepare = desk.prepareTextSnapshot.bind(desk);
    desk.prepareTextSnapshot = (...args) => {
      const message = args[0] as InboundMessage;
      if (!questionRetargetRaceObserved && message.messageId === originalReply.messageId) {
        mutationStore!.createQuestion({
          id: newerQuestionId,
          taskId: seedTaskId!,
          botUid: fixture.restarted.bot.uid,
          ...fixture.target,
          payload: JSON.stringify({ question: "Newer Q2", toolCallId: "recovery-inputs-q2-call" }),
        });
        questionRetargetRaceObserved = true;
      }
      return prepare(...args);
    };

    const recovery = fixture.restarted.service.receive(inbound({
      ...fixture.target,
      messageId: "recovery-inputs-ask-explicit-new",
      text: `/new ${fixture.cwd}`,
    }));
    await awaitStep("ask recovery /new ACK", recovery.ackReady);
    await waitForHarnessStatus(
      fixture.restarted.service,
      fixture.target,
      status => status.sessionId !== null && status.sessionId !== fixture.oldSessionId,
      "ask recovery session binding",
    );
    await waitUntil(() => questionRetargetRaceObserved, "frozen original-question consumption");
    await waitUntil(
      () => fixture.restarted.transport.sent.some(({ text }) =>
        text === "问题已因会话切换失效，本次回复未作为答案记录。"),
      "session-switch invalidation answer notice",
    );

    const store = await openChannelStore(fixture.restarted.stateDir);
    try {
      const questionJob = store.getIngressJobBySource({
        botUid: fixture.restarted.bot.uid,
        sourceKind: "recv",
        sourceId: ingressSourceId(originalReply),
      });
      expect(questionJob).toMatchObject({
        state: "handed-off",
        consumerKind: "ask",
        consumerId: expect.stringContaining(originalQuestionId),
      });
      expect(store.getQuestion(originalQuestionId)).toMatchObject({ state: "answered", answer: "1" });
      expect(store.getQuestion(newerQuestionId)).toMatchObject({ state: "pending", answer: null });
      expect(store.getTaskByMessageId(fixture.restarted.bot.uid, originalReply, originalReply.messageId)).toBeNull();
    } finally {
      store.close();
    }
    expect(fixture.restarted.workers.prompts).toHaveLength(promptCountBeforeRecovery);
    const duplicate = fixture.restarted.service.receive(originalReply);
    await awaitStep("duplicate ask answer ACK", duplicate.ackReady);
    await waitUntilIngressState(fixture.restarted, originalReply, "handed-off");
    expect(fixture.restarted.transport.sent.filter(({ text }) =>
      text === "问题已因会话切换失效，本次回复未作为答案记录。")).toHaveLength(1);
    const finalStore = await openChannelStore(fixture.restarted.stateDir);
    try {
      expect(finalStore.getQuestion(originalQuestionId)).toMatchObject({ state: "answered", answer: "1" });
      expect(finalStore.getQuestion(newerQuestionId)).toMatchObject({ state: "pending", answer: null });
    } finally {
      finalStore.close();
    }
  } finally {
    mutationStore?.close();
    await closeUnknownSessionFixture(fixture);
  }
});
