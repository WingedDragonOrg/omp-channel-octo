import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import { attachmentDirectory } from "../src/inbound/attachments.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";


async function waitUntil(check: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`did not observe ${label}`);
}

function registerFixtureThread(transport: FakeTransport, groupId: string, shortId: string): void {
  const channelId = `${groupId}____${shortId}`;
  const timestamp = "2026-11-04T00:00:00.000Z";
  transport.setThreads(groupId, [{
    shortId,
    groupId,
    channelId,
    name: "Ingress recovery thread",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: timestamp,
    updatedAt: timestamp,
  }]);
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

async function settleTurns(count = 40): Promise<void> {
  for (let turn = 0; turn < count; turn += 1) {
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
}

const suspendedPrompt: FakePrompt = async ({ isStopped, onStop }) => {
  const stopped = Promise.withResolvers<void>();
  onStop(stopped.resolve);
  if (isStopped()) stopped.resolve();
  await stopped.promise;
  throw new Error("fixture prompt stopped for restart");
};

function attachmentPath(prompt: string): string {
  const match = / → (\/[^\n]+)$/mu.exec(prompt);
  if (!match?.[1]) throw new Error(`attachment path missing from prompt: ${prompt}`);
  return match[1];
}

async function closeOrRemove(harness: CoreHarness | undefined, root: string): Promise<void> {
  if (harness) {
    await closeCoreHarness(harness);
    return;
  }
  await rm(root, { recursive: true, force: true });
}

test("redelivery after a lost ACK reuses one durable consumer", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async () => {
      promptStarted.resolve();
      await releasePrompt.promise;
    },
  });
  const target = { channelType: 1 as const, channelId: "dm-ingress-ack-lost" };
  const message = inbound({
    ...target,
    messageId: "ingress-ack-lost-message",
    text: "execute exactly once despite redelivery",
  });

  try {
    const first = harness.service.receive(message);
    await first.ackReady;
    await promptStarted.promise;

    const redelivery = harness.service.receive(message);
    await redelivery.ackReady;
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "execute exactly once despite redelivery",
    ]);

    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getLatestTask(harness.bot.uid, target)?.messageId).toBe(message.messageId);
    } finally {
      store.close();
    }

    releasePrompt.resolve();
    await waitForStatus(harness.service, target, status => status.state === "completed");
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    releasePrompt.resolve();
    await closeCoreHarness(harness);
  }
});

test("the same server message id in different chats creates two independent consumers", async () => {
  const harness = await createCoreHarness();
  const sharedMessageId = "ingress-shared-server-id";
  const first = inbound({
    channelType: 1,
    channelId: "dm-ingress-shared-a",
    messageId: sharedMessageId,
    text: "work for chat A",
  });
  const second = inbound({
    channelType: 1,
    channelId: "dm-ingress-shared-b",
    messageId: sharedMessageId,
    text: "work for chat B",
  });

  try {
    const firstReceipt = harness.service.receive(first);
    const secondReceipt = harness.service.receive(second);
    await Promise.all([firstReceipt.ackReady, secondReceipt.ackReady]);
    await waitUntil(() => harness.workers.prompts.length === 2, "both cross-chat consumers");
    expect(harness.workers.prompts.map(({ text }) => text).sort()).toEqual(
      ["work for chat A", "work for chat B"].sort(),
    );
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getLatestTask(harness.bot.uid, first)?.messageId).toBe(sharedMessageId);
      expect(store.getLatestTask(harness.bot.uid, second)?.messageId).toBe(sharedMessageId);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an unacknowledged interrupted media preparation is accepted once after redelivery", async () => {
  const url = "https://cdn.example.test/unacked-ingress.bin";
  const bytes = new Uint8Array([1, 3, 3, 7]);
  const first = await createCoreHarness({ attachments: { enabled: true, maxBytes: 1024 } });
  const downloadStarted = Promise.withResolvers<void>();
  first.transport.downloadAttachment = async (request) => {
    downloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(new Error("fixture download interrupted before durable preparation"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    return aborted.promise;
  };
  const message = inbound({
    channelType: 1,
    channelId: "dm-ingress-unacked-media",
    messageId: "ingress-unacked-media",
    text: "recover this media",
    attachments: [{
      kind: "file",
      name: "unacked.bin",
      size: bytes.byteLength,
      durationSeconds: 0,
      url,
    }],
  });
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const receipt = first.service.receive(message);
    const ackOutcome = receipt.ackReady.then(
      () => "fulfilled" as const,
      () => "rejected" as const,
    );
    await downloadStarted.promise;
    expect(await observedState(receipt.ackReady)).toBe("pending");

    await first.service.stop();
    firstStopped = true;
    expect(await ackOutcome).toBe("rejected");
    expect(first.workers.prompts).toHaveLength(0);

    const transport = new FakeTransport({ bot: first.bot });
    transport.attachmentBodies.set(url, bytes);
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      attachments: { enabled: true, maxBytes: 1024 },
    });
    const redelivery = restarted.service.receive(message);
    await redelivery.ackReady;
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, message, status => status.state === "completed");

    expect(transport.downloadRequests).toHaveLength(1);
    expect(restarted.workers.prompts[0]?.text).toContain("recover this media");
    expect(restarted.workers.prompts).toHaveLength(1);
  } finally {
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});

test("a partial attachment left by a process crash is removed and never reused as a complete spool", async () => {
  const url = "https://cdn.example.test/crashed-ingress.bin";
  const bytes = new Uint8Array([9, 8, 7, 6, 5]);
  const partial = new Uint8Array([9, 8]);
  const first = await createCoreHarness({ attachments: { enabled: true, maxBytes: 1024 } });
  const downloadStarted = Promise.withResolvers<void>();
  first.transport.downloadAttachment = async request => {
    await writeFile(request.path, partial);
    downloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = (): void => aborted.reject(new Error("fixture download interrupted"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    return aborted.promise;
  };
  const message = inbound({
    channelType: 1,
    channelId: "dm-ingress-crashed-media",
    messageId: "ingress-crashed-media",
    text: "read the complete attachment",
    attachments: [{
      kind: "file",
      name: "crashed.bin",
      size: bytes.byteLength,
      durationSeconds: 0,
      url,
    }],
  });
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const receipt = first.service.receive(message);
    await downloadStarted.promise;
    expect(await observedState(receipt.ackReady)).toBe("pending");
    await first.service.stop();
    firstStopped = true;

    const store = await openChannelStore(first.stateDir);
    let jobId: number;
    try {
      const job = store.getIngressJobBySource({
        botUid: first.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
      });
      if (job === null) throw new Error("preparing ingress job disappeared");
      jobId = job.id;
    } finally {
      store.close();
    }

    const key = createHash("sha256")
      .update(JSON.stringify([
        jobId,
        message.channelType,
        message.channelId,
        message.messageId,
        0,
        url,
      ]))
      .digest("hex")
      .slice(0, 32);
    const directory = attachmentDirectory(first.stateDir, message, message.messageId);
    const orphanName = `.omp-ingress-${key}-crashed.part`;
    await mkdir(directory, { recursive: true });
    await writeFile(join(directory, orphanName), partial);

    const transport = new FakeTransport({ bot: first.bot });
    transport.attachmentBodies.set(url, bytes);
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      attachments: { enabled: true, maxBytes: 1024 },
    });
    const redelivery = restarted.service.receive(message);
    await redelivery.ackReady;
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, message, status => status.state === "completed");

    const path = attachmentPath(restarted.workers.prompts[0]?.text ?? "");
    expect(new Uint8Array(await readFile(path))).toEqual(bytes);
    expect(transport.downloadRequests).toHaveLength(1);
    expect(await readdir(directory)).not.toContain(orphanName);
  } finally {
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});

test("acknowledged media recovery uses the local spool and resumes the exact session without replaying the prompt", async () => {
  const url = "https://cdn.example.test/durable-ingress.txt";
  const bytes = new TextEncoder().encode("durable ingress media");
  const first = await createCoreHarness({
    attachments: { enabled: true, maxBytes: 1024 },
    prompt: suspendedPrompt,
  });
  first.transport.attachmentBodies.set(url, bytes);
  const target = { channelType: 1 as const, channelId: "dm-ingress-media-restart" };
  const message = inbound({
    ...target,
    messageId: "ingress-media-restart",
    text: "read the durable file",
    attachments: [{
      kind: "file",
      name: "durable.txt",
      size: bytes.byteLength,
      durationSeconds: 0,
      url,
    }],
  });
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const receipt = first.service.receive(message);
    await receipt.ackReady;
    await first.workers.waitForPromptCount(1);
    const originalPrompt = first.workers.prompts[0]!;
    const path = attachmentPath(originalPrompt.text);
    expect(new Uint8Array(await Bun.file(path).arrayBuffer())).toEqual(bytes);

    const store = await openChannelStore(first.stateDir);
    try {
      expect(store.getLatestTask(first.bot.uid, target)?.text).toContain(path);
    } finally {
      store.close();
    }

    await closeCoreHarness(first, { remove: false });
    firstStopped = true;

    const transport = new FakeTransport({ bot: first.bot });
    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport,
      workers,
      attachments: { enabled: true, maxBytes: 1024 },
    });
    await workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, target, status => status.state === "completed");

    expect(transport.downloadRequests).toHaveLength(0);
    expect(workers.prompts.map(({ text }) => text)).toEqual(["/retry"]);
    expect(workers.prompts[0]?.sessionId).toBe(originalPrompt.sessionId);
    expect(workers.prompts.some(({ text }) => text.includes("read the durable file"))).toBe(false);
    expect(new Uint8Array(await Bun.file(path).arrayBuffer())).toEqual(bytes);

    const ackLostRedelivery = restarted.service.receive(message);
    await ackLostRedelivery.ackReady;
    expect(transport.downloadRequests).toHaveLength(0);
    expect(workers.prompts).toHaveLength(1);
  } finally {
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});

test("an acknowledged merge interrupted before a host ledger recovers one waiting operation", async () => {
  const first = await createCoreHarness();
  const thread = { channelType: 5 as const, channelId: "group-allowed____ingress-merge-recovery" };
  registerFixtureThread(first.transport, "group-allowed", "ingress-merge-recovery");
  const workspace = join(first.root, "merge-recovery-workspace");
  await mkdir(workspace, { recursive: true });
  const setup = await openChannelStore(first.stateDir);
  setup.ensureBinding({
    botUid: first.bot.uid,
    ...thread,
    cwd: workspace,
    sessionId: "ingress-merge-recovery-session",
    sessionFile: null,
    cwdPinned: true,
  });
  setup.close();
  const planningStarted = Promise.withResolvers<void>();
  const port = first.worktreePort;
  if (!port) throw new Error("expected worktree port");
  port.planMerge = async (_request, signal) => {
    planningStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(signal?.reason ?? new Error("merge planning interrupted"));
    if (signal?.aborted) rejectAbort();
    else signal?.addEventListener("abort", rejectAbort, { once: true });
    return aborted.promise;
  };
  const message = inbound({
    ...thread,
    messageId: "ingress-waiting-merge-recovery",
    text: "/thread merge",
    mentionedUserIds: [first.bot.uid],
  });
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const receipt = first.service.receive(message);
    await receipt.ackReady;
    await planningStarted.promise;
    expect(first.worktrees?.merges).toEqual([]);

    await first.service.stop();
    firstStopped = true;

    const transport = new FakeTransport({ bot: first.bot });
    registerFixtureThread(transport, "group-allowed", "ingress-merge-recovery");
    restarted = await createCoreHarness({ root: first.root, bot: first.bot, transport });
    await waitUntil(() => (restarted?.worktrees?.merges.length ?? 0) === 1, "recovered merge execution");
    await restarted.transport.waitForManagementCalls(({ action }) => action === "thread-archive");

    const redelivery = restarted.service.receive(message);
    await redelivery.ackReady;
    expect(restarted.worktrees?.merges).toHaveLength(1);
  } finally {
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});

test("an acknowledged new before dispatch intent restores its barrier before a later ordinary message", async () => {
  const first = await createCoreHarness();
  const dispatchGate = Promise.withResolvers<void>();
  if (!Reflect.set(first.service, "startupGate", dispatchGate)) {
    throw new Error("could not install deterministic ingress dispatch gate");
  }
  const target = { channelType: 1 as const, channelId: "dm-ingress-new-recovery" };
  const cwd = join(first.root, "new-recovery-cwd");
  await mkdir(cwd, { recursive: true });
  const switchMessage = inbound({
    ...target,
    messageId: "ingress-new-recovery-command",
    text: `/new ${cwd}`,
  });
  const laterMessage = inbound({
    ...target,
    messageId: "ingress-new-recovery-later",
    text: "run only in the recovered replacement session",
  });
  const switchKey = {
    botUid: first.bot.uid,
    sourceKind: "recv" as const,
    sourceId: recvIngressSourceId(target.channelType, target.channelId, switchMessage.messageId),
  };
  const laterKey = {
    botUid: first.bot.uid,
    sourceKind: "recv" as const,
    sourceId: recvIngressSourceId(target.channelType, target.channelId, laterMessage.messageId),
  };
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const switching = first.service.receive(switchMessage);
    await switching.ackReady;
    const later = first.service.receive(laterMessage);
    await later.ackReady;
    const store = await openChannelStore(first.stateDir);
    try {
      const switchJob = store.getIngressJobBySource(switchKey);
      const laterJob = store.getIngressJobBySource(laterKey);
      expect(switchJob).toMatchObject({ state: "accepted", ackReady: true });
      expect(switchJob?.operationCheckpoint).toMatchObject({ kind: "command", phase: "barrier-installed" });
      expect(laterJob).toMatchObject({ state: "accepted", ackReady: true });
      expect(laterJob?.routeSnapshot?.barrierJobId).toBe(switchJob?.id);
      expect(first.workers.creations).toHaveLength(0);
    } finally {
      store.close();
    }

    const stopping = first.service.stop();
    dispatchGate.resolve();
    await stopping;
    firstStopped = true;

    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    restarted = await createCoreHarness({ root: first.root, bot: first.bot, workers });
    await waitUntil(() => workers.prompts.length === 1, "ordinary message after recovered new");
    await waitUntil(
      () => restarted!.service.status(target).then(status => status.cwd === cwd),
      "recovered session binding",
    );
    expect(workers.prompts[0]?.cwd).toBe(cwd);

    const creations = workers.creations.length;
    const redelivery = restarted.service.receive(switchMessage);
    await redelivery.ackReady;
    expect(workers.creations).toHaveLength(creations);
    expect(workers.prompts).toHaveLength(1);
  } finally {
    dispatchGate.resolve();
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});

test("a stopped worker allows a fresh /new to recover an unknown transition after restart", async () => {
  const firstWorkers = new FakeWorkerFactory();
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  const first = await createCoreHarness({ workers: firstWorkers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-new-recovery" };
  const cwd = join(first.root, "new-recovery-cwd");
  await mkdir(cwd, { recursive: true });
  const initialMessage = inbound({
    ...target,
    messageId: "ingress-new-recovery-initial",
    text: `/new ${cwd}`,
  });
  const initial = first.service.receive(initialMessage);
  await initial.ackReady;
  const initialStatus = await waitForStatus(first.service, target, status => status.sessionId !== null);
  const oldSessionId = initialStatus.sessionId;
  if (oldSessionId === null) throw new Error("initial session was not bound");
  const oldWorker = firstWorkers.workers[0];
  if (oldWorker === undefined) throw new Error("initial session worker was not created");
  firstWorkers.beforeCreate = async () => {
    candidateStarted.resolve();
    await releaseCandidate.promise;
    throw new Error("fixture candidate interrupted with the service");
  };
  const switchMessage = inbound({
    ...target,
    messageId: "ingress-new-recovery-command",
    text: `/new ${cwd}`,
  });
  const laterMessage = inbound({
    ...target,
    messageId: "ingress-new-recovery-later",
    text: "run only in the recovered replacement session",
  });
  let restarted: CoreHarness | undefined;
  let firstStopped = false;

  try {
    const switching = first.service.receive(switchMessage);
    await switching.ackReady;
    await candidateStarted.promise;
    expect(oldWorker.stopped).toBe(true);
    const candidateStore = await openChannelStore(first.stateDir);
    try {
      const switchingJob = candidateStore.getIngressJobBySource({
        botUid: first.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, switchMessage.messageId),
      });
      if (switchingJob === null) throw new Error("unknown switch ingress was not persisted");
      const candidateLedgerId = `new-ingress-${switchingJob.id}`;
      if (!candidateStore.recordIngressWorker(switchingJob.id, {
        id: candidateLedgerId,
        botUid: first.bot.uid,
        sessionId: "ingress-recovery-candidate-session",
        sessionFile: join(cwd, "candidate.session"),
        pid: 4242,
        pgid: 4242,
        identity: "fixture-candidate-process",
        cwd,
        moveTargetCwd: null,
        state: "running",
      })) throw new Error("candidate worker ledger was not linked to the switch");
      candidateStore.markWorkerStopped(candidateLedgerId);
    } finally {
      candidateStore.close();
    }
    const later = first.service.receive(laterMessage);
    await later.ackReady;
    expect(firstWorkers.prompts).toHaveLength(0);

    const stopping = first.service.stop();
    releaseCandidate.resolve();
    await stopping;
    firstStopped = true;

    const workers = firstWorkers;
    workers.beforeCreate = undefined;
    restarted = await createCoreHarness({ root: first.root, bot: first.bot, workers });
    const store = await openChannelStore(restarted.stateDir);
    try {
      const switchKey = {
        botUid: first.bot.uid,
        sourceKind: "recv" as const,
        sourceId: recvIngressSourceId(target.channelType, target.channelId, switchMessage.messageId),
      };
      const laterKey = {
        botUid: first.bot.uid,
        sourceKind: "recv" as const,
        sourceId: recvIngressSourceId(target.channelType, target.channelId, laterMessage.messageId),
      };
      await waitUntil(
        () => store.getIngressJobBySource(switchKey)?.state === "unknown",
        "unknown new dispatch outcome",
      );
      const switchJob = store.getIngressJobBySource(switchKey);
      const laterJob = store.getIngressJobBySource(laterKey);
      expect(laterJob).toMatchObject({ state: "accepted", ackReady: true });
      expect(laterJob?.routeSnapshot?.barrierJobId).toBe(switchJob?.id);
      expect(workers.prompts).toHaveLength(0);

      const creations = workers.creations.length;
      const redelivery = restarted.service.receive(switchMessage);
      await redelivery.ackReady;
      expect(workers.creations).toHaveLength(creations);
    } finally {
      store.close();
    }
    const recovered = restarted;
    if (recovered === undefined) throw new Error("restart harness was not created");
    const recoveryMessage = inbound({
      ...target,
      messageId: "ingress-new-recovery-explicit",
      text: `/new ${cwd}`,
    });
    const recoveryReceipt = recovered.service.receive(recoveryMessage);
    await recoveryReceipt.ackReady;
    const recoveredStatus = await waitForStatus(
      recovered.service,
      target,
      status => status.sessionId !== null && status.sessionId !== oldSessionId,
    );
    await settleTurns();
    const recoveredSessionId = recoveredStatus.sessionId;
    if (recoveredSessionId === null) throw new Error("recovery did not publish a session");
    expect(recoveredSessionId).not.toBe(oldSessionId);
    expect(recoveredStatus.cwd).toBe(cwd);

    const notice = recovered.transport.sent.find(({ target: sentTarget, text }) =>
      sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && (text.includes(laterMessage.messageId) || text.includes(laterMessage.text)));
    expect(notice).toBeDefined();
    expect(notice?.text).toMatch(/(?:未|没有|尚未).{0,12}(?:执行|派发)/u);
    expect(notice?.text).toMatch(/(?:重发|重新发送)/u);
    expect(notice?.text).toContain("忽略");

    const freshMessage = inbound({
      ...target,
      messageId: "ingress-new-recovery-fresh",
      text: "continue the conversation in the recovered session",
    });
    const freshReceipt = recovered.service.receive(freshMessage);
    await freshReceipt.ackReady;
    await waitUntil(
      () => workers.prompts.some(({ text }) => text.includes(freshMessage.text)),
      "fresh message prompt in recovered session",
    );
    const freshPrompt = workers.prompts.find(({ text }) => text.includes(freshMessage.text));
    expect(freshPrompt).toBeDefined();
    expect(freshPrompt?.sessionId).toBe(recoveredSessionId);
    expect(freshPrompt?.cwd).toBe(cwd);
    expect(workers.prompts.some(({ text }) => text.includes(laterMessage.text))).toBe(false);

    const noticeCount = recovered.transport.sent.filter(({ target: sentTarget, text }) =>
      sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && (text.includes(laterMessage.messageId) || text.includes(laterMessage.text))).length;
    const promptCount = workers.prompts.length;
    const duplicate = recovered.service.receive(laterMessage);
    await duplicate.ackReady;
    await settleTurns();
    expect(workers.prompts).toHaveLength(promptCount);
    expect(recovered.transport.sent.filter(({ target: sentTarget, text }) =>
      sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && (text.includes(laterMessage.messageId) || text.includes(laterMessage.text))))
      .toHaveLength(noticeCount);
  } finally {
    if (!firstStopped) await first.service.stop().catch(() => {});
    await closeOrRemove(restarted, first.root);
  }
});
