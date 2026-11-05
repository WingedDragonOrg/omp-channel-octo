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
): string {
  return messages
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

test("same-chat cancel waits for session worker process-group stop and reports unknown safely", async () => {
  const workers = new FakeWorkerFactory();
  const candidateStarted = Promise.withResolvers<void>();
  const releaseCandidate = Promise.withResolvers<void>();
  const releaseStop = Promise.withResolvers<void>();
  let stopStarted = false;
  let stopConfirmed = false;
  workers.beforeCreate = async () => {
    candidateStarted.resolve();
    await releaseCandidate.promise;
  };
  workers.stopHandler = async () => {
    stopStarted = true;
    await releaseStop.promise;
    stopConfirmed = true;
  };
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-cancel-new" };
  const cwd = join(harness.root, "cancel-new-target");
  await mkdir(cwd, { recursive: true });
  const switching = inbound({
    ...target,
    messageId: "ingress-cancel-new-operation",
    text: `/new ${cwd}`,
  });

  try {
    const switchingReceipt = harness.service.receive(switching);
    await switchingReceipt.ackReady;
    await candidateStarted.promise;

    const accepted = await (async () => {
      const store = await openChannelStore(harness.stateDir);
      try {
        const job = store.getIngressJobBySource({
          botUid: harness.bot.uid,
          sourceKind: "recv",
          sourceId: recvIngressSourceId(target.channelType, target.channelId, switching.messageId),
        });
        if (job === null) throw new Error("new ingress intent is missing");
        expect(job.state).toBe("dispatch-intent");
        expect(job.operationCheckpoint?.kind).toBe("command");
        if (job.operationCheckpoint?.kind !== "command") throw new Error("new checkpoint is missing");
        expect(job.operationCheckpoint.workerLedgerId).toBe(`new-ingress-${job.id}`);
        return job;
      } finally {
        store.close();
      }
    })();

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-cancel-new-control",
      text: "/cancel",
    }));
    await cancel.ackReady;
    await waitUntil(() => workers.creations[0]?.signal?.aborted === true, "new worker cancellation");
    expect(workers.creations[0]?.signal?.aborted).toBe(true);

    releaseCandidate.resolve();
    await waitUntil(() => stopStarted, "cancelled new worker stop");
    expect(sentText(harness.transport.sent, target)).not.toContain("无法确认受管操作");
    releaseStop.resolve();
    await waitUntil(
      () => sentText(harness.transport.sent, target).includes("无法确认受管操作已完成或停止"),
      "truthful cancel result after worker stop",
    );
    expect(stopConfirmed).toBe(true);

    const recoveredStore = await openChannelStore(harness.stateDir);
    try {
      const unknown = recoveredStore.getIngressJob(accepted.id);
      expect(unknown?.state).toBe("unknown");
      expect(unknown?.operationCheckpoint?.kind === "command"
        ? unknown.operationCheckpoint.workerLedgerId
        : null).toBe(`new-ingress-${accepted.id}`);
      expect(recoveredStore.getOutbox(`ingress:${accepted.id}:receipt:0`)?.text)
        .toContain("无法确认新会话是否完整创建");
    } finally {
      recoveredStore.close();
    }

    const redelivery = harness.service.receive(switching);
    await redelivery.ackReady;
    expect(workers.creations).toHaveLength(1);

    const retryCwd = join(harness.root, "unblocked-retry-target");
    await mkdir(retryCwd, { recursive: true });
    const retryMessage = inbound({
      ...target,
      messageId: "ingress-new-after-unknown-worker",
      text: `/new ${retryCwd}`,
    });
    const retry = harness.service.receive(retryMessage);
    await retry.ackReady;
    await waitUntil(
      () => sentText(harness.transport.sent, target).includes("目录存在未确认停止的 worker"),
      "same-session refusal while unknown worker resources remain protected",
    );
    expect(workers.creations).toHaveLength(1);
    const laterMessage = inbound({
      ...target,
      messageId: "ingress-ordinary-after-unknown-new",
      text: "remain behind the unknown session transition",
    });
    const later = harness.service.receive(laterMessage);
    await later.ackReady;

    const finalStore = await openChannelStore(harness.stateDir);
    try {
      const refusalJob = finalStore.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, retryMessage.messageId),
      });
      const laterJob = finalStore.getIngressJobBySource({
        botUid: harness.bot.uid,
        sourceKind: "recv",
        sourceId: recvIngressSourceId(target.channelType, target.channelId, laterMessage.messageId),
      });
      expect(refusalJob?.state).toBe("handed-off");
      expect(laterJob).toMatchObject({ state: "accepted", ackReady: true });
      expect(laterJob?.routeSnapshot?.barrierJobId).toBe(accepted.id);
    } finally {
      finalStore.close();
    }
    expect(workers.creations).toHaveLength(1);
    expect(workers.prompts).toHaveLength(0);
  } finally {
    releaseCandidate.resolve();
    releaseStop.resolve();
    await closeCoreHarness(harness);
  }
});
