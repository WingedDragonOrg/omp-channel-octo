import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore, type ChannelStore, type OutboxInsert, type TanOperation } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";

const botUid = "tan-store-bot";
const source = { channelType: 2 as const, channelId: "tan-source-group" };
const target = { channelType: 5 as const, channelId: "tan-source-group____fork" };

async function withState(work: (stateDir: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-store-tan-"));
  const stateDir = join(root, "state");
  try {
    await work(stateDir);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function forkOperationId(
  messageId: string,
  uid = botUid,
  origin: { channelType: 2 | 5; channelId: string } = source,
): string {
  return `fork-${createHash("sha256")
    .update(JSON.stringify(["thread-fork", uid, origin.channelType, origin.channelId, `command:${messageId}`]))
    .digest("hex")}`;
}

function commandClaim(
  messageId = "tan-source-message",
  uid = botUid,
  origin: { channelType: 2 | 5; channelId: string } = source,
): InboundClaim {
  return {
    botUid: uid,
    target: origin,
    messageId,
    senderId: "owner-1",
    text: `/tan request from ${messageId}`,
    supported: true,
    kind: "command",
  };
}

function operation(
  messageId = "tan-source-message",
  overrides: Partial<TanOperation> = {},
): TanOperation {
  const operationBotUid = overrides.botUid ?? botUid;
  const sourceChannelType = overrides.sourceChannelType ?? source.channelType;
  const sourceChannelId = overrides.sourceChannelId ?? source.channelId;
  return {
    id: forkOperationId(messageId, operationBotUid, {
      channelType: sourceChannelType,
      channelId: sourceChannelId,
    }),
    botUid: operationBotUid,
    sourceChannelType,
    sourceChannelId,
    messageId,
    senderId: "owner-1",
    text: `materialized request from ${messageId}\n\n[附件]\n/tmp/input.txt`,
    sourceSessionId: "source-session-exact",
    sourceSessionFile: "/sessions/source-session-exact.jsonl",
    cwd: "/work/shared",
    phase: "pending",
    error: null,
    ...overrides,
  };
}

function receiptRecords(
  value: TanOperation,
  kind: "failure" | "cancel-result",
  texts: readonly string[],
): OutboxInsert[] {
  return texts.map((text, index) => {
    const id = `tan:${value.id}:${kind}:${index}`;
    return {
      id,
      botUid: value.botUid,
      channelType: value.sourceChannelType,
      channelId: value.sourceChannelId,
      taskId: null,
      kind: "text",
      text,
      path: null,
      fileKind: null,
      clientMsgNo: id,
    };
  });
}

function taskFor(op: TanOperation): { claim: InboundClaim; task: TaskRecord } {
  const messageId = `dispatch:tan:${op.id}`;
  const text = op.text;
  return {
    claim: {
      botUid: op.botUid,
      target,
      messageId,
      senderId: op.senderId,
      text,
      supported: true,
      kind: "task",
    },
    task: {
      ...target,
      id: `task:${messageId}`,
      botUid: op.botUid,
      messageId,
      senderId: op.senderId,
      text,
      sessionId: "fork-session-exact",
      cwd: op.cwd,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relaySuppressed: false,
    },
  };
}

function acceptOperation(store: ChannelStore, value: TanOperation): void {
  const claim = commandClaim(value.messageId, value.botUid, {
    channelType: value.sourceChannelType,
    channelId: value.sourceChannelId,
  });
  expect(store.claimCommand(claim)).toBe(true);
  expect(store.acceptTanOperation(claim, value)).toBe(true);
}

test("TAN acceptance completes its command claim atomically and duplicate delivery preserves the first snapshot", async () => {
  await withState(async (stateDir) => {
    const claim = commandClaim();
    const value = operation();
    let store = await openChannelStore(stateDir);
    expect(store.claimCommand(claim)).toBe(true);
    store.close();

    const databasePath = join(stateDir, "channel.sqlite");
    const reject = new Database(databasePath);
    reject.exec(`CREATE TRIGGER reject_tan_acceptance BEFORE INSERT ON tan_operations
      BEGIN SELECT RAISE(ABORT, 'injected TAN acceptance failure'); END;`);
    reject.close();

    store = await openChannelStore(stateDir);
    expect(() => store.acceptTanOperation(claim, value)).toThrow("injected TAN acceptance failure");
    expect(store.getCommandClaimState(botUid, source, claim.messageId)).toBe("pending");
    expect(store.getTanOperation(botUid, value.id)).toBeNull();
    store.close();

    const repair = new Database(databasePath);
    repair.exec("DROP TRIGGER reject_tan_acceptance");
    repair.close();

    store = await openChannelStore(stateDir);
    expect(store.acceptTanOperation(claim, value)).toBe(true);
    expect(store.getCommandClaimState(botUid, source, claim.messageId)).toBe("completed");
    expect(store.acceptTanOperation(claim, {
      ...value,
      text: "replacement must not land",
      sourceSessionId: "replacement-session",
      sourceSessionFile: "/sessions/replacement.jsonl",
      cwd: "/replacement",
    })).toBe(false);
    expect(store.getTanOperation(botUid, value.id)).toEqual(value);
    store.close();

    store = await openChannelStore(stateDir);
    expect(store.getTanOperation(botUid, value.id)).toEqual(value);
    store.close();
  });
});

test("same message ID in different source chats keeps distinct fork operation identities", async () => {
  await withState(async (stateDir) => {
    const otherSource = { channelType: 5 as const, channelId: "tan-source-group____origin" };
    const first = operation("same-message-id");
    const second = operation("same-message-id", {
      sourceChannelType: otherSource.channelType,
      sourceChannelId: otherSource.channelId,
    });
    expect(first.id).not.toBe(second.id);

    const store = await openChannelStore(stateDir);
    acceptOperation(store, first);
    acceptOperation(store, second);
    expect(store.getCommandClaimState(botUid, source, first.messageId)).toBe("completed");
    expect(store.getCommandClaimState(botUid, otherSource, second.messageId)).toBe("completed");
    expect(store.getTanOperation(botUid, first.id)).toEqual(first);
    expect(store.getTanOperation(botUid, second.id)).toEqual(second);
    expect(store.listPendingTanOperations(botUid)).toEqual([first, second]);
    store.close();
  });
});

test("a pending TAN command can resume after reopen without rewriting its inbound body", async () => {
  await withState(async (stateDir) => {
    const claim = commandClaim("materialization-crash");
    let store = await openChannelStore(stateDir);
    expect(store.claimCommand(claim)).toBe(true);
    store.close();

    store = await openChannelStore(stateDir);
    expect(store.reclaimTanCommand({ ...claim, text: "redelivery body must not overwrite" })).toBe(false);
    expect(store.reclaimTanCommand(claim)).toBe(true);
    expect(store.getCommandClaimState(botUid, source, claim.messageId)).toBeNull();
    expect(store.claimCommand(claim)).toBe(true);
    const value = operation(claim.messageId);
    expect(store.acceptTanOperation(claim, value)).toBe(true);
    expect(store.reclaimTanCommand(claim)).toBe(false);
    store.close();

    const probe = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
    try {
      expect(probe.query(
        "SELECT text, command_state FROM inbound WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?",
      ).get(botUid, source.channelType, source.channelId, claim.messageId)).toEqual({
        text: claim.text,
        command_state: "completed",
      });
    } finally {
      probe.close();
    }
  });
});

test("slash-leading TAN tasks persist the exact user body and reject a prewrapped replacement", async () => {
  await withState(async (stateDir) => {
    const store = await openChannelStore(stateDir);
    const acceptedOperation = operation("slash-raw", {
      text: "/review src/store\n\n[附件]\n/tmp/review.patch",
    });
    acceptOperation(store, acceptedOperation);
    const acceptedTarget = taskFor(acceptedOperation);
    expect(store.acceptTanTask(
      acceptedTarget.claim,
      acceptedOperation.id,
      acceptedTarget.task,
    )).toBe(true);
    expect(store.getTanOperation(botUid, acceptedOperation.id)?.text).toBe(acceptedOperation.text);
    expect(store.getTask(acceptedTarget.task.id)?.text).toBe(acceptedOperation.text);

    const guardedOperation = operation("slash-guard", { text: "/status --full" });
    acceptOperation(store, guardedOperation);
    const guardedTarget = taskFor(guardedOperation);
    const wrappedTask = {
      ...guardedTarget.task,
      text: `用户请求：\n${guardedOperation.text}`,
    };
    expect(store.acceptTanTask(guardedTarget.claim, guardedOperation.id, wrappedTask)).toBe(false);
    expect(store.getTanOperation(botUid, guardedOperation.id)?.phase).toBe("pending");
    expect(store.getTaskByMessageId(botUid, target, guardedTarget.claim.messageId)).toBeNull();
    store.close();
  });
});

test("TAN cancellation and target task acceptance are one serialized durable decision", async () => {
  await withState(async (stateDir) => {
    let store = await openChannelStore(stateDir);
    const cancelled = operation("cancel-wins");
    acceptOperation(store, cancelled);
    expect(store.cancelTanOperation(botUid, cancelled.id)).toBe(true);
    expect(store.cancelTanOperation(botUid, cancelled.id)).toBe(false);
    const cancelledTarget = taskFor(cancelled);
    expect(store.acceptTanTask(cancelledTarget.claim, cancelled.id, cancelledTarget.task)).toBe(false);
    expect(store.getTaskByMessageId(botUid, target, cancelledTarget.claim.messageId)).toBeNull();
    expect(store.listCancelledTanOperations(botUid)).toEqual([{ ...cancelled, phase: "cancelled" }]);
    store.close();
    store = await openChannelStore(stateDir);
    expect(store.listCancelledTanOperations(botUid)).toEqual([{ ...cancelled, phase: "cancelled" }]);
    const cancelError = "远端创建结果未能确认；不会重复创建。";
    const cancellationRecords = receiptRecords(cancelled, "cancel-result", [
      "取消已记录；远端子区创建结果未能确认，不会重复创建。",
      "分叉操作 " + cancelled.id,
    ]);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      cancelled.id,
      "cancelled",
      cancelError,
      cancellationRecords,
    )).toBe(true);
    expect(store.listCancelledTanOperations(botUid)).toEqual([]);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      cancelled.id,
      "cancelled",
      "late outcome must not rewrite",
      receiptRecords(cancelled, "cancel-result", ["late outcome must not rewrite"]),
    )).toBe(false);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      cancelled.id,
      "failed",
      "late failure must not replace cancellation",
      receiptRecords(cancelled, "failure", ["late failure must not replace cancellation"]),
    )).toBe(false);
    expect(store.getTanOperation(botUid, cancelled.id)).toEqual({
      ...cancelled,
      phase: "cancelled",
      error: cancelError,
    });
    expect(store.getOutbox(cancellationRecords[0]!.id)).toMatchObject({
      id: cancellationRecords[0]!.id,
      text: cancellationRecords[0]!.text,
      state: "pending",
    });

    const dispatched = operation("accept-wins");
    acceptOperation(store, dispatched);
    const dispatchedTarget = taskFor(dispatched);
    expect(store.acceptTanTask(dispatchedTarget.claim, dispatched.id, dispatchedTarget.task)).toBe(true);
    expect(store.acceptTanTask(dispatchedTarget.claim, dispatched.id, dispatchedTarget.task)).toBe(false);
    expect(store.cancelTanOperation(botUid, dispatched.id)).toBe(false);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      dispatched.id,
      "failed",
      "late failure must not replace handoff",
      receiptRecords(dispatched, "failure", ["late failure must not replace handoff"]),
    )).toBe(false);
    expect(store.getTanOperation(botUid, dispatched.id)).toEqual({ ...dispatched, phase: "done" });
    expect(store.getTaskByMessageId(botUid, target, dispatchedTarget.claim.messageId)).toMatchObject({
      id: dispatchedTarget.task.id,
      sessionId: dispatchedTarget.task.sessionId,
      cwd: dispatchedTarget.task.cwd,
      text: dispatched.text,
    });
    store.close();

    store = await openChannelStore(stateDir);
    expect(store.getTanOperation(botUid, cancelled.id)).toMatchObject({
      phase: "cancelled",
      error: cancelError,
    });
    expect(store.getTanOperation(botUid, dispatched.id)?.phase).toBe("done");
    expect(store.getTask(dispatchedTarget.task.id)?.sessionId).toBe("fork-session-exact");
    expect(store.getOutbox(cancellationRecords[1]!.id)?.text).toBe(cancellationRecords[1]!.text);
    store.close();
  });
});

test("target task insertion rolls back the TAN terminal transition", async () => {
  await withState(async (stateDir) => {
    const value = operation("task-rollback");
    let store = await openChannelStore(stateDir);
    acceptOperation(store, value);
    store.close();

    const databasePath = join(stateDir, "channel.sqlite");
    const reject = new Database(databasePath);
    reject.exec(`CREATE TRIGGER reject_tan_task BEFORE INSERT ON tasks
      BEGIN SELECT RAISE(ABORT, 'injected TAN task failure'); END;`);
    reject.close();

    store = await openChannelStore(stateDir);
    const targetTask = taskFor(value);
    expect(() => store.acceptTanTask(targetTask.claim, value.id, targetTask.task))
      .toThrow("injected TAN task failure");
    expect(store.getTanOperation(botUid, value.id)?.phase).toBe("pending");
    expect(store.getTaskByMessageId(botUid, target, targetTask.claim.messageId)).toBeNull();
    store.close();

    const repair = new Database(databasePath);
    repair.exec("DROP TRIGGER reject_tan_task");
    repair.close();
    store = await openChannelStore(stateDir);
    expect(store.acceptTanTask(targetTask.claim, value.id, targetTask.task)).toBe(true);
    expect(store.getTanOperation(botUid, value.id)?.phase).toBe("done");
    store.close();
  });
});

test("cancelled TAN result remains recoverable until its stable receipt commits", async () => {
  await withState(async (stateDir) => {
    const value = operation("cancel-result-atomic");
    const records = receiptRecords(value, "cancel-result", [
      "TAN 已取消；远端子区创建结果未知，不会重试。",
    ]);
    let store = await openChannelStore(stateDir);
    acceptOperation(store, value);
    expect(store.cancelTanOperation(botUid, value.id)).toBe(true);
    store.close();

    const databasePath = join(stateDir, "channel.sqlite");
    const reject = new Database(databasePath);
    reject.exec(`CREATE TRIGGER reject_tan_cancel_receipt BEFORE INSERT ON outbox
      BEGIN SELECT RAISE(ABORT, 'injected TAN cancel receipt failure'); END;`);
    reject.close();

    store = await openChannelStore(stateDir);
    expect(store.listCancelledTanOperations(botUid)).toEqual([{ ...value, phase: "cancelled" }]);
    expect(() => store.finishTanOperationWithOutbox(
      botUid,
      value.id,
      "cancelled",
      "TAN 已取消；远端子区创建结果未知，不会重试。",
      records,
    )).toThrow("injected TAN cancel receipt failure");
    expect(store.getTanOperation(botUid, value.id)).toEqual({
      ...value,
      phase: "cancelled",
      error: null,
    });
    expect(store.getOutbox(records[0]!.id)).toBeNull();
    expect(store.listCancelledTanOperations(botUid)).toEqual([{ ...value, phase: "cancelled" }]);
    store.close();

    const repair = new Database(databasePath);
    repair.exec("DROP TRIGGER reject_tan_cancel_receipt");
    repair.close();
    store = await openChannelStore(stateDir);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      value.id,
      "cancelled",
      "TAN 已取消；远端子区创建结果未知，不会重试。",
      records,
    )).toBe(true);
    expect(store.listCancelledTanOperations(botUid)).toEqual([]);
    expect(store.getOutbox(records[0]!.id)?.text).toBe(records[0]!.text);
    store.close();
  });
});

test("TAN failure status and stable receipt outbox commit or roll back together", async () => {
  await withState(async (stateDir) => {
    const value = operation("failure-receipt-atomic");
    const records = receiptRecords(value, "failure", [
      "TAN 分叉任务未能派发：",
      "准备状态已确认失败。",
    ]);
    let store = await openChannelStore(stateDir);
    acceptOperation(store, value);
    store.close();

    const databasePath = join(stateDir, "channel.sqlite");
    const reject = new Database(databasePath);
    reject.exec(`CREATE TRIGGER reject_tan_failure_receipt BEFORE INSERT ON outbox
      WHEN NEW.id = '${records[1]!.id}'
      BEGIN SELECT RAISE(ABORT, 'injected TAN receipt failure'); END;`);
    reject.close();

    store = await openChannelStore(stateDir);
    expect(() => store.finishTanOperationWithOutbox(
      botUid,
      value.id,
      "failed",
      "fork preparation failed",
      records,
    )).toThrow("injected TAN receipt failure");
    expect(store.getTanOperation(botUid, value.id)?.phase).toBe("pending");
    expect(store.getOutbox(records[0]!.id)).toBeNull();
    expect(store.getOutbox(records[1]!.id)).toBeNull();
    store.close();

    const repair = new Database(databasePath);
    repair.exec("DROP TRIGGER reject_tan_failure_receipt");
    repair.close();
    store = await openChannelStore(stateDir);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      value.id,
      "failed",
      "fork preparation failed",
      records,
    )).toBe(true);
    expect(store.getTanOperation(botUid, value.id)).toMatchObject({
      phase: "failed",
      error: "fork preparation failed",
    });
    expect(store.getOutbox(records[0]!.id)?.text).toBe(records[0]!.text);
    expect(store.getOutbox(records[1]!.id)?.text).toBe(records[1]!.text);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      value.id,
      "failed",
      "late duplicate",
      records,
    )).toBe(false);
    store.close();

    store = await openChannelStore(stateDir);
    expect(store.getTanOperation(botUid, value.id)).toMatchObject({
      phase: "failed",
      error: "fork preparation failed",
    });
    expect(store.getOutbox(records[1]!.id)?.text).toBe(records[1]!.text);
    store.close();
  });
});

test("pending recovery is bot-scoped, terminal states survive reopen, and additive schema keeps existing data", async () => {
  await withState(async (stateDir) => {
    await mkdir(stateDir, { recursive: true });
    const databasePath = join(stateDir, "channel.sqlite");
    const legacy = new Database(databasePath);
    legacy.exec("CREATE TABLE legacy_notes (note TEXT NOT NULL); INSERT INTO legacy_notes VALUES ('keep me');");
    legacy.close();

    let store = await openChannelStore(stateDir);
    const first = operation("shared-id");
    const otherBot = operation("shared-id", { botUid: "other-bot" });
    const failed = operation("failed-operation");
    acceptOperation(store, first);
    acceptOperation(store, otherBot);
    acceptOperation(store, failed);
    expect(store.finishTanOperationWithOutbox(
      botUid,
      failed.id,
      "failed",
      "fork preparation failed",
      receiptRecords(failed, "failure", ["fork preparation failed"]),
    )).toBe(true);

    expect(store.listPendingTanOperations(botUid)).toEqual([first]);
    expect(store.listPendingTanOperations("other-bot")).toEqual([otherBot]);
    store.finishTanOperation(botUid, first.id);
    expect(store.getTanOperation(botUid, failed.id)).toEqual({
      ...failed,
      phase: "failed",
      error: "fork preparation failed",
    });
    store.close();

    store = await openChannelStore(stateDir);
    expect(store.listPendingTanOperations(botUid)).toEqual([]);
    expect(store.listPendingTanOperations("other-bot")).toEqual([otherBot]);
    expect(store.getTanOperation(botUid, first.id)?.phase).toBe("done");
    expect(store.getTanOperation(botUid, failed.id)).toEqual({
      ...failed,
      phase: "failed",
      error: "fork preparation failed",
    });
    expect(store.getTanOperation("other-bot", otherBot.id)).toEqual(otherBot);
    store.close();

    const probe = new Database(databasePath, { readonly: true });
    try {
      expect(probe.query("SELECT note FROM legacy_notes").get()).toEqual({ note: "keep me" });
    } finally {
      probe.close();
    }
  });
});
