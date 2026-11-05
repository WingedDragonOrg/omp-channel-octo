import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { WorkerLedgerRecord } from "../src/store/store-records.ts";
import { inbound } from "./fixtures/core-fakes.ts";

test("session worker ledger is transactionally linked to its exact ingress intent and paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "core-ingress-worker-link-"));
  const store = await openChannelStore(root);
  const cwd = join(root, "new-session");
  const message = inbound({
    channelType: 1,
    channelId: "dm-ingress-worker-link",
    messageId: "ingress-worker-link-message",
    text: `/new ${cwd}`,
  });
  const accepted = store.acceptIngressJob({
    botUid: "bot-ingress-worker-link",
    sourceKind: "recv",
    sourceId: recvIngressSourceId(message.channelType, message.channelId, message.messageId),
    channelType: message.channelType,
    channelId: message.channelId,
    messageId: message.messageId,
    payload: { kind: "recv", message },
    routeSnapshot: {
      purpose: { kind: "command", action: "new" },
      sessionGeneration: 0,
      binding: null,
      barrierJobId: null,
    },
    lane: "ordered",
    state: "accepted",
    ackReady: true,
    operationCheckpoint: {
      kind: "command",
      action: "new",
      argument: cwd,
      phase: "waiting-session",
      expectedBinding: null,
      resourceCwds: [cwd],
    },
  });
  const workerId = `new-ingress-${accepted.job.id}`;
  const intent = {
    kind: "command" as const,
    action: "new",
    argument: cwd,
    phase: "dispatch-intent" as const,
    expectedBinding: null,
    resourceCwds: [cwd],
    workerLedgerId: workerId,
  };
  const worker: WorkerLedgerRecord = {
    id: workerId,
    botUid: accepted.job.botUid,
    sessionId: "session-ingress-worker-link",
    sessionFile: join(cwd, "session.jsonl"),
    pid: 4123,
    pgid: 4123,
    identity: "fixture-process-identity",
    cwd,
    moveTargetCwd: null,
    state: "running",
  };

  try {
    expect(store.markIngressCommandDispatchIntent(accepted.job.id, intent)).toBe(true);
    expect(store.recordIngressWorker(accepted.job.id, { ...worker, id: "wrong-worker-id" })).toBe(false);
    expect(store.recordIngressWorker(accepted.job.id, { ...worker, cwd: join(root, "unrecorded") })).toBe(false);
    expect(store.getWorkerLedger(worker.botUid, workerId)).toBeNull();
    expect(store.recordIngressWorker(accepted.job.id, worker)).toBe(true);
    expect(store.getWorkerLedger(worker.botUid, workerId)).toEqual(worker);

    const sourceCwd = join(root, "move-source");
    const targetCwd = join(root, "move-target");
    const sessionFile = join(sourceCwd, "existing-session.jsonl");
    const moveMessage = inbound({
      channelType: 1,
      channelId: "dm-ingress-worker-link",
      messageId: "ingress-worker-move-link-message",
      text: `/move ${targetCwd}`,
    });
    const expectedBinding = {
      sessionId: "existing-session",
      cwd: sourceCwd,
      sessionFile,
    };
    const moveAccepted = store.acceptIngressJob({
      botUid: worker.botUid,
      sourceKind: "recv",
      sourceId: recvIngressSourceId(moveMessage.channelType, moveMessage.channelId, moveMessage.messageId),
      channelType: moveMessage.channelType,
      channelId: moveMessage.channelId,
      messageId: moveMessage.messageId,
      payload: { kind: "recv", message: moveMessage },
      routeSnapshot: {
        purpose: { kind: "command", action: "move" },
        sessionGeneration: 0,
        binding: expectedBinding,
        barrierJobId: null,
      },
      lane: "ordered",
      state: "accepted",
      ackReady: true,
      operationCheckpoint: {
        kind: "command",
        action: "move",
        argument: targetCwd,
        phase: "waiting-session",
        expectedBinding,
        resourceCwds: [sourceCwd, targetCwd],
      },
    });
    const moveWorkerId = `move-ingress-${moveAccepted.job.id}`;
    const moveIntent = {
      kind: "command" as const,
      action: "move",
      argument: targetCwd,
      phase: "dispatch-intent" as const,
      expectedBinding,
      resourceCwds: [sourceCwd, targetCwd],
      workerLedgerId: moveWorkerId,
    };
    const moveWorker: WorkerLedgerRecord = {
      ...worker,
      id: moveWorkerId,
      sessionId: expectedBinding.sessionId,
      sessionFile,
      cwd: sourceCwd,
      moveTargetCwd: targetCwd,
    };
    expect(store.markIngressCommandDispatchIntent(moveAccepted.job.id, moveIntent)).toBe(true);
    expect(store.recordIngressWorker(moveAccepted.job.id, { ...moveWorker, sessionId: "wrong-session" })).toBe(false);
    expect(store.recordIngressWorker(moveAccepted.job.id, { ...moveWorker, moveTargetCwd: join(root, "wrong-target") })).toBe(false);
    expect(store.getWorkerLedger(worker.botUid, moveWorkerId)).toBeNull();
    expect(store.recordIngressWorker(moveAccepted.job.id, moveWorker)).toBe(true);
    expect(store.getWorkerLedger(worker.botUid, moveWorkerId)).toEqual(moveWorker);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
