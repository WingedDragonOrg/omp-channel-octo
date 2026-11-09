import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { IngressCoordinator, type IngressCoordinatorDeps } from "../src/inbound/ingress-coordinator.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import type {
  IngressJobInput,
  IngressJobRecord,
  IngressSourceKey,
} from "../src/inbound/ingress-types.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

function sourceKey(key: IngressSourceKey): string {
  return JSON.stringify([key.botUid, key.sourceKind, key.sourceId]);
}

test("restored unknown session transitions remain barriers for later ordered ingress", async () => {
  const target = { channelType: 1 as const, channelId: "dm-ingress-restored-unknown" };
  const sourceId = recvIngressSourceId(target.channelType, target.channelId, "unknown-new");
  const now = Date.now();
  const unknown: IngressJobRecord = {
    botUid: "bot-1",
    sourceKind: "recv",
    sourceId,
    ...target,
    scheduleOrder: 1,
    id: 1,
    messageId: "unknown-new",
    payload: null,
    routeSnapshot: {
      purpose: { kind: "command", action: "new" },
      sessionGeneration: 0,
      binding: null,
      barrierJobId: null,
    },
    preparedText: null,
    spoolPaths: [],
    ackReady: true,
    lane: "ordered",
    state: "unknown",
    operationCheckpoint: {
      kind: "command",
      action: "new",
      argument: "",
      phase: "dispatch-intent",
      expectedBinding: null,
      workerLedgerId: "new-ingress-1",
    },
    consumerKind: null,
    consumerId: null,
    createdAt: now,
    updatedAt: now,
  };
  const jobs = new Map<number, IngressJobRecord>([[unknown.id, unknown]]);
  const bySource = new Map([[sourceKey(unknown), unknown.id]]);
  let nextId = 2;
  const store = {
    listRecoverableIngressJobs: () => [],
    getIngressJob: (id: number) => jobs.get(id) ?? null,
    getIngressJobBySource: (key: IngressSourceKey) => {
      const id = bySource.get(sourceKey(key));
      return id === undefined ? null : jobs.get(id) ?? null;
    },
    acceptIngressJob: (input: IngressJobInput) => {
      const existingId = bySource.get(sourceKey(input));
      if (existingId !== undefined) return { job: jobs.get(existingId)!, created: false };
      const job: IngressJobRecord = {
        ...input,
        scheduleOrder: nextId,
        id: nextId++,
        preparedText: null,
        spoolPaths: [],
        consumerKind: null,
        consumerId: null,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      };
      jobs.set(job.id, job);
      bySource.set(sourceKey(job), job.id);
      return { job, created: true };
    },
    handoffIngressJob: (
      id: number,
      consumer: { kind: IngressJobRecord["consumerKind"]; id: string },
      persist: () => boolean,
    ) => {
      const job = jobs.get(id);
      if (job === undefined || !persist()) return false;
      job.state = "handed-off";
      job.consumerKind = consumer.kind;
      job.consumerId = consumer.id;
      return true;
    },
  };
  let dispatches = 0;
  let idleNotifications = 0;
  const coordinator = new IngressCoordinator({
    store: store as unknown as IngressCoordinatorDeps["store"],
    botUid: () => "bot-1",
    classify: () => ({
      lane: "ordered",
      action: null,
      argument: "",
      preparesBody: false,
      sessionBarrier: false,
      cancelsEarlier: false,
    }),
    authorize: async () => ({
      kind: "accepted",
      purpose: { kind: "none" },
      preparesBody: false,
      checkpoint: null,
    }),
    bindingSnapshot: () => null,
    prepare: async () => { throw new Error("no body preparation expected"); },
    dispatch: async job => {
      dispatches += 1;
      return { consumer: { kind: "operation", id: job.sourceId } };
    },
    awaitDispatchReady: async () => {},
    onIdle: () => { idleNotifications += 1; },
  });

  try {
    coordinator.restore("bot-1", [unknown]);
    const later = inbound({
      ...target,
      messageId: "after-unknown-new",
      text: "ordered input must stay behind the unknown transition",
    });
    const receipt = coordinator.receive(later);
    await receipt.ackReady;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
    expect(dispatches).toBe(0);
    expect(store.getIngressJobBySource({
      botUid: "bot-1",
      sourceKind: "recv",
      sourceId: recvIngressSourceId(target.channelType, target.channelId, later.messageId),
    })?.state).toBe("accepted");

    expect(coordinator.releaseSessionBarrier(unknown.id)).toBe(true);
    await coordinator.settle();
    expect(dispatches).toBe(1);
    expect(idleNotifications).toBe(0);
  } finally {
    coordinator.seal();
    await coordinator.settle();
  }
});

test("ingress idle notification follows durable handoff and candidate removal", async () => {
  const target = { channelType: 1 as const, channelId: "dm-ingress-idle-notification" };
  const jobs = new Map<number, IngressJobRecord>();
  const bySource = new Map<string, number>();
  let nextId = 1;
  const store = {
    getIngressJob(id: number): IngressJobRecord | null {
      return jobs.get(id) ?? null;
    },
    getIngressJobBySource(key: IngressSourceKey): IngressJobRecord | null {
      const id = bySource.get(sourceKey(key));
      return id === undefined ? null : jobs.get(id) ?? null;
    },
    acceptIngressJob(input: IngressJobInput): { job: IngressJobRecord; created: boolean } {
      const key = sourceKey(input);
      const existingId = bySource.get(key);
      if (existingId !== undefined) return { job: jobs.get(existingId)!, created: false };
      const now = Date.now();
      const job: IngressJobRecord = {
        ...input,
        scheduleOrder: nextId,
        id: nextId++,
        preparedText: null,
        spoolPaths: [],
        consumerKind: null,
        consumerId: null,
        createdAt: now,
        updatedAt: now,
      };
      jobs.set(job.id, job);
      bySource.set(key, job.id);
      return { job, created: true };
    },
    handoffIngressJob(
      id: number,
      consumer: { kind: IngressJobRecord["consumerKind"]; id: string },
      persist: () => boolean,
    ): boolean {
      const job = jobs.get(id);
      if (job === undefined || !persist()) return false;
      job.state = "handed-off";
      job.consumerKind = consumer.kind;
      job.consumerId = consumer.id;
      return true;
    },
  };
  const completion = Promise.withResolvers<void>();
  const dispatchStarted = Promise.withResolvers<void>();
  let coordinator!: IngressCoordinator;
  let idleNotifications = 0;
  let observedPendingCount = -1;
  let observedJobState: IngressJobRecord["state"] | undefined;
  coordinator = new IngressCoordinator({
    store: store as unknown as IngressCoordinatorDeps["store"],
    botUid: () => "bot-1",
    classify: () => ({
      lane: "ordered",
      action: null,
      argument: "",
      preparesBody: false,
      sessionBarrier: false,
      cancelsEarlier: false,
    }),
    authorize: async () => ({
      kind: "accepted",
      purpose: { kind: "none" },
      preparesBody: false,
      checkpoint: null,
    }),
    bindingSnapshot: () => null,
    prepare: async () => { throw new Error("no body preparation expected"); },
    dispatch: async job => {
      dispatchStarted.resolve();
      return {
        consumer: { kind: "operation", id: job.sourceId },
        completion: completion.promise,
      };
    },
    awaitDispatchReady: async () => {},
    onIdle: () => {
      idleNotifications += 1;
      observedPendingCount = coordinator.pendingCount();
      observedJobState = jobs.get(1)?.state;
    },
  });

  try {
    const receipt = coordinator.receive(inbound({
      ...target,
      messageId: "idle-after-handoff",
      text: "ordinary input",
    }));
    await receipt.ackReady;
    await dispatchStarted.promise;
    expect(coordinator.pendingCount()).toBe(1);
    expect(idleNotifications).toBe(0);

    completion.resolve();
    await coordinator.settle();

    expect(idleNotifications).toBe(1);
    expect(observedPendingCount).toBe(0);
    expect(observedJobState).toBe("handed-off");
  } finally {
    completion.resolve();
    coordinator.seal();
    await coordinator.settle();
  }
});


function sentText(
  messages: readonly { target: { channelId: string; channelType: number }; text: string }[],
  target: { channelId: string; channelType: number },
  startIndex = 0,
): string {
  return messages
    .slice(startIndex)
    .filter(({ target: actual }) => actual.channelId === target.channelId && actual.channelType === target.channelType)
    .map(({ text }) => text)
    .join("\n");
}

async function waitUntil(check: () => boolean, label: string): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`did not observe ${label}`);
}

type DirectChat = { channelType: 1; channelId: string };

async function bindingFor(harness: CoreHarness, target: DirectChat) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getBinding(harness.bot.uid, target);
  } finally {
    store.close();
  }
}



test("control cancel does not abort a dispatched candidate when its cancellation CAS loses", async () => {
  const byId = new Map<number, IngressJobRecord>();
  const bySource = new Map<string, number>();
  let nextId = 1;
  const store = {
    acceptIngressJob(input: IngressJobInput): { job: IngressJobRecord; created: boolean } {
      const existingId = bySource.get(sourceKey(input));
      if (existingId !== undefined) return { job: byId.get(existingId)!, created: false };
      const now = Date.now();
      const job: IngressJobRecord = {
        ...input,
        scheduleOrder: nextId,
        id: nextId++,
        preparedText: null,
        spoolPaths: [],
        consumerKind: null,
        consumerId: null,
        createdAt: now,
        updatedAt: now,
      };
      byId.set(job.id, job);
      bySource.set(sourceKey(job), job.id);
      return { job, created: true };
    },
    getIngressJob(id: number): IngressJobRecord | null {
      return byId.get(id) ?? null;
    },
    getIngressJobBySource(key: IngressSourceKey): IngressJobRecord | null {
      const id = bySource.get(sourceKey(key));
      return id === undefined ? null : byId.get(id) ?? null;
    },
    listRecoverableIngressJobs(botUid: string): IngressJobRecord[] {
      return [...byId.values()].filter(job =>
        job.botUid === botUid
        && (job.state === "accepted" || job.state === "preparing" || job.state === "dispatch-intent"),
      );
    },
    markIngressPrepared(): boolean {
      return false;
    },
    markIngressDispatchIntent(id: number): boolean {
      const job = byId.get(id);
      if (job?.state !== "accepted") return false;
      job.state = "dispatch-intent";
      return true;
    },
    markIngressConfigActionDispatchIntent(): boolean {
      return false;
    },
    markIngressConfigActionApplied(): boolean {
      return false;
    },
    markIngressUnknown(id: number): boolean {
      const job = byId.get(id);
      if (job === undefined || (job.state !== "accepted" && job.state !== "dispatch-intent")) return false;
      job.state = "unknown";
      return true;
    },
    cancelIngressJob(id: number): boolean {
      const job = byId.get(id);
      if (job === undefined || (job.state !== "accepted" && job.state !== "preparing")) return false;
      job.state = "cancelled";
      return true;
    },
    cancelIngressJobBySource(key: IngressSourceKey): boolean {
      const id = bySource.get(sourceKey(key));
      return id === undefined ? false : this.cancelIngressJob(id);
    },
    handoffIngressJob(id: number, consumer: { kind: IngressJobRecord["consumerKind"]; id: string }, persist: () => boolean): boolean {
      const job = byId.get(id);
      if (job === undefined || !persist()) return false;
      job.state = "handed-off";
      job.consumerKind = consumer.kind as IngressJobRecord["consumerKind"];
      job.consumerId = consumer.id;
      return true;
    },
  };
  const dispatchStarted = Promise.withResolvers<void>();
  const finishDispatch = Promise.withResolvers<void>();
  let dispatchedSignal: AbortSignal | undefined;
  const coordinator = new IngressCoordinator({
    store: store as unknown as IngressCoordinatorDeps["store"],
    botUid: () => "bot-1",
    classify: message => message.text === "/cancel"
      ? {
          lane: "control",
          action: "cancel",
          argument: "",
          preparesBody: false,
          sessionBarrier: false,
          cancelsEarlier: true,
        }
      : {
          lane: "ordered",
          action: null,
          argument: "",
          preparesBody: false,
          sessionBarrier: false,
          cancelsEarlier: false,
        },
    authorize: async (message, signal) => {
      if (message.text !== "/cancel") dispatchedSignal = signal;
      return { kind: "accepted", purpose: { kind: "none" }, preparesBody: false, checkpoint: null };
    },
    bindingSnapshot: () => null,
    prepare: async () => { throw new Error("no body preparation expected"); },
    dispatch: async job => {
      const message = job.payload?.kind === "recv" ? job.payload.message : null;
      if (message?.text !== "/cancel") {
        dispatchStarted.resolve();
        return {
          consumer: { kind: "operation", id: job.sourceId },
          completion: finishDispatch.promise,
        };
      }
      return { consumer: { kind: "operation", id: job.sourceId } };
    },
    awaitDispatchReady: async () => {},
  });
  const original = inbound({
    channelType: 1,
    channelId: "dm-ingress-cancel-cas",
    messageId: "ingress-cancel-cas-original",
    text: "long operation already dispatched",
  });

  try {
    const originalReceipt = coordinator.receive(original);
    await originalReceipt.ackReady;
    await dispatchStarted.promise;
    const originalJob = store.getIngressJobBySource({
      botUid: "bot-1",
      sourceKind: "recv",
      sourceId: JSON.stringify([1, original.channelId, original.messageId]),
    });
    if (originalJob === null) throw new Error("accepted ingress job is missing");
    expect(store.markIngressDispatchIntent(originalJob.id)).toBe(true);

    const cancelReceipt = coordinator.receive(inbound({
      channelType: original.channelType,
      channelId: original.channelId,
      messageId: "ingress-cancel-cas-control",
      text: "/cancel",
    }));
    await cancelReceipt.ackReady;

    expect(dispatchedSignal?.aborted).toBe(false);
    expect(store.getIngressJob(originalJob.id)?.state).toBe("dispatch-intent");

    finishDispatch.resolve();
    await coordinator.settle();
    expect(store.getIngressJob(originalJob.id)?.state).toBe("handed-off");
    expect(dispatchedSignal?.aborted).toBe(false);
  } finally {
    finishDispatch.resolve();
    coordinator.seal();
    await coordinator.settle();
  }
});

test("cancelled /new stays protected until worker stop is confirmed, then recovers without replay", async () => {
  const workers = new FakeWorkerFactory();
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  const releaseStop = Promise.withResolvers<void>();
  let stopStarted = false;
  let stopConfirmed = false;
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-cancel-new" };
  const cwd = join(harness.root, "cancel-new-target");

  try {
    await mkdir(cwd, { recursive: true });
    const initialMessage = inbound({
      ...target,
      messageId: "ingress-cancel-new-initial-session",
      text: `/new ${cwd}`,
    });
    const initialOutputStart = harness.transport.sent.length;
    const initial = harness.service.receive(initialMessage);
    await initial.ackReady;
    await waitUntil(
      () => sentText(harness.transport.sent, target, initialOutputStart).includes("已创建会话"),
      "initial session binding",
    );
    const originalBinding = await bindingFor(harness, target);
    if (originalBinding?.sessionFile === null || originalBinding === null) {
      throw new Error("initial /new did not create a bound session");
    }
    expect((await harness.service.status(target)).sessionId).toBe(originalBinding.sessionId);

    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
    };
    workers.stopHandler = async () => {
      stopStarted = true;
      await releaseStop.promise;
      stopConfirmed = true;
    };
    const switching = inbound({
      ...target,
      messageId: "ingress-cancel-new-operation",
      text: `/new ${cwd}`,
    });
    const switchingOutputStart = harness.transport.sent.length;
    const switchingReceipt = harness.service.receive(switching);
    await switchingReceipt.ackReady;
    await candidateStarted.promise;

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-cancel-new-control",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await waitUntil(
      () => workers.creations.at(-1)?.signal?.aborted === true,
      "new worker cancellation",
    );
    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    await waitUntil(() => stopStarted, "cancelled new worker process-group stop");
    const cancelledCandidateSessionId = workers.workers.at(-1)?.sessionId;
    if (!cancelledCandidateSessionId) throw new Error("cancelled candidate worker did not start");

    const blockedOutputStart = harness.transport.sent.length;
    const blockedRetry = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-new-before-stop-confirmation",
      text: `/new ${cwd}`,
    }));
    await blockedRetry.ackReady;
    await waitUntil(
      () => /等待|\/stop|\/cancel/.test(sentText(harness.transport.sent, target, blockedOutputStart)),
      "wait-or-stop guidance while the worker process group is unconfirmed",
    );
    const blockedText = sentText(harness.transport.sent, target, blockedOutputStart);
    expect(blockedText).toMatch(/等待|\/stop|\/cancel/);
    expect(blockedText).not.toContain("已创建会话");
    expect(workers.creations).toHaveLength(2);
    expect(await bindingFor(harness, target)).toMatchObject({
      sessionId: originalBinding.sessionId,
      cwd,
    });
    expect((await harness.service.status(target)).sessionId).toBe(originalBinding.sessionId);
    expect(sentText(harness.transport.sent, target, switchingOutputStart))
      .not.toContain("无法确认新会话是否完整创建");

    releaseStop.resolve();
    await waitUntil(
      () => stopConfirmed
        && sentText(harness.transport.sent, target, switchingOutputStart)
          .includes("无法确认新会话是否完整创建"),
      "unknown result after confirmed worker stop",
    );
    expect(sentText(harness.transport.sent, target, switchingOutputStart)).not.toContain("已创建会话");
    expect(workers.creations).toHaveLength(2);
    expect(await bindingFor(harness, target)).toMatchObject({
      sessionId: originalBinding.sessionId,
      cwd,
    });

    const recoveryOutputStart = harness.transport.sent.length;
    const recovery = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-new-after-confirmed-worker-stop",
      text: `/new ${cwd}`,
    }));
    await recovery.ackReady;
    await waitUntil(
      () => sentText(harness.transport.sent, target, recoveryOutputStart).includes("已创建会话"),
      "new independent session after confirmed stop",
    );
    const recoveredBinding = await bindingFor(harness, target);
    if (recoveredBinding?.sessionFile === null || recoveredBinding === null) {
      throw new Error("recovery /new did not create a bound session");
    }
    expect(recoveredBinding.cwd).toBe(cwd);
    expect(recoveredBinding.sessionId).not.toBe(originalBinding.sessionId);
    expect(recoveredBinding.sessionId).not.toBe(cancelledCandidateSessionId);
    expect((await harness.service.status(target)).sessionId).toBe(recoveredBinding.sessionId);
    expect(workers.creations).toHaveLength(3);

    const redelivery = harness.service.receive(switching);
    await redelivery.ackReady;
    expect(workers.creations).toHaveLength(3);
    expect(await bindingFor(harness, target)).toMatchObject({
      sessionId: recoveredBinding.sessionId,
      cwd,
    });
  } finally {
    releaseCandidate.resolve();
    releaseStop.resolve();
    await closeCoreHarness(harness);
  }
});

test("failed session worker stop keeps /new protected and never reports recovery success", async () => {
  const workers = new FakeWorkerFactory();
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  let stopAttempted = false;
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-failed-cancel-new" };
  const cwd = join(harness.root, "failed-cancel-new-target");

  try {
    await mkdir(cwd, { recursive: true });
    const initialOutputStart = harness.transport.sent.length;
    const initial = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-failed-cancel-new-initial",
      text: `/new ${cwd}`,
    }));
    await initial.ackReady;
    await waitUntil(
      () => sentText(harness.transport.sent, target, initialOutputStart).includes("已创建会话"),
      "initial session binding before failed stop",
    );
    const originalBinding = await bindingFor(harness, target);
    if (originalBinding?.sessionFile === null || originalBinding === null) {
      throw new Error("initial /new did not create a bound session");
    }
    workers.stopHandler = async () => {
      stopAttempted = true;
      throw new Error("fixture process-group exit was not confirmed");
    };

    workers.beforeCreate = async () => {
      candidateStarted.resolve();
      await releaseCandidate.promise;
    };
    const switching = inbound({
      ...target,
      messageId: "ingress-failed-cancel-new-operation",
      text: `/new ${cwd}`,
    });
    const switchingOutputStart = harness.transport.sent.length;
    const switchingReceipt = harness.service.receive(switching);
    await switchingReceipt.ackReady;
    await candidateStarted.promise;
    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-failed-cancel-new-control",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await waitUntil(
      () => workers.creations.at(-1)?.signal?.aborted === true,
      "new worker cancellation before failed stop",
    );

    releaseCandidate.resolve();
    workers.beforeCreate = undefined;
    await waitUntil(() => stopAttempted, "failed worker process-group stop");
    await waitUntil(
      () => sentText(harness.transport.sent, target, switchingOutputStart)
        .includes("无法确认新会话是否完整创建"),
      "unknown result after failed worker stop",
    );
    const unknownText = sentText(harness.transport.sent, target, switchingOutputStart);
    expect(unknownText).not.toContain("已创建会话");

    const blockedOutputStart = harness.transport.sent.length;
    const retry = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-new-after-failed-worker-stop",
      text: `/new ${cwd}`,
    }));
    await retry.ackReady;
    await waitUntil(
      () => /等待|\/stop|\/cancel/.test(sentText(harness.transport.sent, target, blockedOutputStart)),
      "continued wait-or-stop guidance after failed process-group stop",
    );
    const blockedText = sentText(harness.transport.sent, target, blockedOutputStart);
    expect(blockedText).toMatch(/等待|\/stop|\/cancel/);
    expect(blockedText).not.toContain("已创建会话");
    expect(workers.creations).toHaveLength(2);
    expect(await bindingFor(harness, target)).toMatchObject({
      sessionId: originalBinding.sessionId,
      cwd,
    });
    expect((await harness.service.status(target)).sessionId).toBe(originalBinding.sessionId);

    const redelivery = harness.service.receive(switching);
    await redelivery.ackReady;
    expect(workers.creations).toHaveLength(2);
    expect(await bindingFor(harness, target)).toMatchObject({
      sessionId: originalBinding.sessionId,
      cwd,
    });
  } finally {
    releaseCandidate.resolve();
    await closeCoreHarness(harness);
  }
});
