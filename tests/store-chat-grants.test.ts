import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { ChatGrantOperationInput, InboundClaim, TaskRecord } from "../src/store/store-records.ts";

const botUid = "bot-chat-grant";
const group: ChatTarget = { channelType: 2, channelId: "group-chat-grant" };

function operation(overrides: Partial<ChatGrantOperationInput> = {}): ChatGrantOperationInput {
  return {
    actionId: "grant-action-1",
    eventId: 101,
    botUid,
    ownerId: "owner-1",
    actorId: "owner-1",
    channelType: 2,
    channelId: group.channelId,
    cardMessageId: "grant-card-message",
    resultMessageId: "grant-result-client-id",
    ...overrides,
  };
}

async function withState<T>(work: (stateDir: string) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-chat-grant-"));
  const stateDir = join(root, "state");
  try {
    return await work(stateDir);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("an accepted chat grant and its recoverable receipt progress survive reopening", async () => {
  await withState(async (stateDir) => {
    const store = await openChannelStore(stateDir);
    store.setOwnerId(botUid, "owner-1");
    try {
      expect(store.acceptChatGrantOperation(operation())).toBe(true);
      expect(store.listAllowedChats(botUid)).toMatchObject([
        { botUid, channelType: 2, channelId: group.channelId, grantedBy: "owner-1" },
      ]);
      expect(store.getChatGrantOperation(botUid, "grant-action-1")).toMatchObject({
        ...operation(),
        configMirrorState: "pending",
        configMirrorError: null,
        resultState: "pending",
        cardEditSeq: 1,
      });
    } finally {
      store.close();
    }

    const reopened = await openChannelStore(stateDir);
    try {
      // An already accepted operation remains recoverable even if ownership changes after acceptance.
      reopened.setOwnerId(botUid, "owner-2");
      expect(reopened.getChatGrantOperationByEvent(botUid, 101)).toMatchObject({
        actionId: "grant-action-1",
        eventId: 101,
        ownerId: "owner-1",
        actorId: "owner-1",
        cardMessageId: "grant-card-message",
        resultMessageId: "grant-result-client-id",
        configMirrorState: "pending",
        resultState: "pending",
        cardEditSeq: 1,
      });
      expect(reopened.listPendingChatGrantOperations(botUid)).toHaveLength(1);
      expect(() => reopened.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toThrow();
      expect(reopened.markChatGrantResultDelivered(botUid, "grant-action-1")).toBe(false);
      expect(reopened.markChatGrantConfigMirror(botUid, "grant-action-1", "succeeded")).toBe(true);
      expect(reopened.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toBe(2);
      expect(reopened.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toBe(3);
      expect(reopened.markChatGrantConfigMirror(botUid, "grant-action-1", "failed", "late error")).toBe(false);
      expect(reopened.listPendingChatGrantOperations(botUid)).toHaveLength(1);
    } finally {
      reopened.close();
    }

    const recovered = await openChannelStore(stateDir);
    try {
      expect(recovered.getChatGrantOperation(botUid, "grant-action-1")).toMatchObject({
        configMirrorState: "succeeded",
        configMirrorError: null,
        resultState: "pending",
        cardEditSeq: 3,
      });
      expect(recovered.listPendingChatGrantOperations(botUid)).toHaveLength(1);
      expect(recovered.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toBe(4);
      expect(recovered.markChatGrantResultDelivered(botUid, "grant-action-1")).toBe(true);
      expect(recovered.markChatGrantResultDelivered(botUid, "grant-action-1")).toBe(false);
      expect(() => recovered.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toThrow();
      expect(recovered.listPendingChatGrantOperations(botUid)).toEqual([]);
      expect(recovered.getChatGrantOperation(botUid, "grant-action-1")).toMatchObject({
        configMirrorState: "succeeded",
        configMirrorError: null,
        resultState: "delivered",
        cardEditSeq: 4,
      });
    } finally {
      recovered.close();
    }
  });
});

test("a failed config mirror remains recoverable until its result is delivered", async () => {
  await withState(async (stateDir) => {
    const store = await openChannelStore(stateDir);
    store.setOwnerId(botUid, "owner-1");
    try {
      expect(store.acceptChatGrantOperation(operation())).toBe(true);
      expect(store.markChatGrantConfigMirror(botUid, "grant-action-1", "failed", "config unavailable")).toBe(true);
      expect(store.getChatGrantOperation(botUid, "grant-action-1")).toMatchObject({
        configMirrorState: "failed",
        configMirrorError: "config unavailable",
        resultState: "pending",
      });
      expect(store.listPendingChatGrantOperations(botUid)).toHaveLength(1);
      expect(store.listAllowedChats(botUid)).toHaveLength(1);
    } finally {
      store.close();
    }

    const resumed = await openChannelStore(stateDir);
    try {
      expect(resumed.getChatGrantOperation(botUid, "grant-action-1")).toMatchObject({
        configMirrorState: "failed",
        configMirrorError: "config unavailable",
        resultState: "pending",
      });
      expect(resumed.listPendingChatGrantOperations(botUid)).toHaveLength(1);
      expect(resumed.nextChatGrantCardEditSeq(botUid, "grant-action-1")).toBe(2);
      expect(resumed.markChatGrantResultDelivered(botUid, "grant-action-1")).toBe(true);
      expect(resumed.listPendingChatGrantOperations(botUid)).toEqual([]);
    } finally {
      resumed.close();
    }
  });
});

test("duplicate action and event deliveries across store connections grant only once", async () => {
  await withState(async (stateDir) => {
    const first = await openChannelStore(stateDir);
    first.setOwnerId(botUid, "owner-1");
    const second = await openChannelStore(stateDir);
    try {
      const grants = await Promise.all([
        Promise.resolve().then(() => first.acceptChatGrantOperation(operation())),
        Promise.resolve().then(() => second.acceptChatGrantOperation(operation())),
      ]);
      expect(grants.sort()).toEqual([false, true]);
      expect(second.acceptChatGrantOperation(operation({
        actionId: "different-action-same-event",
      }))).toBe(false);
      expect(second.acceptChatGrantOperation(operation({
        actionId: "grant-action-1",
        eventId: 102,
      }))).toBe(false);
      expect(second.getChatGrantOperationByEvent(botUid, 101)).toMatchObject({
        actionId: "grant-action-1",
        resultMessageId: "grant-result-client-id",
      });
      expect(second.getChatGrantOperation(botUid, "different-action-same-event")).toBeNull();
      expect(second.listAllowedChats(botUid)).toMatchObject([
        { channelId: group.channelId, grantedBy: "owner-1" },
      ]);
      expect(second.listAllowedChats(botUid)).toHaveLength(1);
    } finally {
      second.close();
      first.close();
    }
  });
});

test("grant acceptance rejects a non-owner and atomically rolls back on authorization failure", async () => {
  await withState(async (stateDir) => {
    const store = await openChannelStore(stateDir);
    const database = new Database(join(stateDir, "channel.sqlite"));
    store.setOwnerId(botUid, "owner-1");
    try {
      expect(store.acceptChatGrantOperation(operation({ actorId: "member-1" }))).toBe(false);
      expect(store.acceptChatGrantOperation(operation({
        actionId: "stale-owner-card",
        ownerId: "owner-2",
        actorId: "owner-2",
      }))).toBe(false);
      expect(store.getChatGrantOperation(botUid, "stale-owner-card")).toBeNull();
      expect(store.listAllowedChats(botUid)).toEqual([]);
      expect(store.getChatGrantOperation(botUid, "grant-action-1")).toBeNull();
      expect(store.listAllowedChats(botUid)).toEqual([]);

      database.exec(`
        CREATE TRIGGER reject_allowed_chat_grant
        BEFORE INSERT ON allowed_chats
        BEGIN
          SELECT RAISE(ABORT, 'injected grant failure');
        END;
      `);
      expect(() => store.acceptChatGrantOperation(operation())).toThrow("injected grant failure");
      expect(store.getChatGrantOperation(botUid, "grant-action-1")).toBeNull();
      expect(store.listAllowedChats(botUid)).toEqual([]);
      database.exec("DROP TRIGGER reject_allowed_chat_grant");

      expect(store.acceptChatGrantOperation(operation())).toBe(true);
      expect(store.listAllowedChats(botUid)).toHaveLength(1);
    } finally {
      database.close();
      store.close();
    }
  });
});

test("a database without the grant operation table opens, preserves grants, and reopens after migration", async () => {
  await withState(async (stateDir) => {
    const initial = await openChannelStore(stateDir);
    initial.setOwnerId(botUid, "owner-1");
    initial.grantAllowedChat({
      botUid,
      channelType: 2,
      channelId: group.channelId,
      grantedBy: "owner-1",
    });
    initial.close();

    const legacyDb = new Database(join(stateDir, "channel.sqlite"));
    try {
      legacyDb.exec("DROP TABLE chat_grant_operations");
    } finally {
      legacyDb.close();
    }

    const migrated = await openChannelStore(stateDir);
    try {
      expect(migrated.listAllowedChats(botUid)).toMatchObject([
        { channelId: group.channelId, grantedBy: "owner-1" },
      ]);
      expect(migrated.acceptChatGrantOperation(operation())).toBe(true);
    } finally {
      migrated.close();
    }

    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.listAllowedChats(botUid)).toHaveLength(1);
      expect(reopened.getChatGrantOperationByEvent(botUid, 101)).toMatchObject({
        actionId: "grant-action-1",
        configMirrorState: "pending",
        resultState: "pending",
        cardEditSeq: 1,
      });
    } finally {
      reopened.close();
    }
  });
});

test("command claim state is read-only, scoped, and failed claims retryable", async () => {
  await withState(async (stateDir) => {
    const claim: InboundClaim = {
      botUid,
      target: group,
      messageId: "grant-passphrase-message",
      senderId: "owner-1",
      text: "@Bot 开通本群",
      supported: true,
      kind: "command",
    };
    const taskClaim: InboundClaim = {
      ...claim,
      messageId: "grant-task-message",
      text: "ordinary task",
      kind: "task",
    };
    const task: TaskRecord = {
      ...group,
      id: "grant-inbound-task",
      botUid,
      messageId: taskClaim.messageId,
      senderId: taskClaim.senderId,
      text: taskClaim.text,
      sessionId: "grant-inbound-session",
      cwd: "/tmp",
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relayChannelType: null,
      relayChannelId: null,
      relayThreadName: null,
    };

    const store = await openChannelStore(stateDir);
    try {
      expect(store.getCommandClaimState(botUid, group, claim.messageId)).toBeNull();
      expect(store.claimCommand(claim)).toBe(true);
      expect(store.getCommandClaimState(botUid, group, claim.messageId)).toBe("pending");
      expect(store.claimCommand(claim)).toBe(false);
    } finally {
      store.close();
    }

    const recovered = await openChannelStore(stateDir);
    try {
      // Pending claims survive restart and remain suppressed rather than being concurrently reclaimed.
      expect(recovered.getCommandClaimState(botUid, group, claim.messageId)).toBe("pending");
      expect(recovered.claimCommand(claim)).toBe(false);

      const failedClaim: InboundClaim = {
        ...claim,
        messageId: "grant-failed-retry-message",
      };
      expect(recovered.getCommandClaimState(botUid, group, failedClaim.messageId)).toBeNull();
      expect(recovered.claimCommand(failedClaim)).toBe(true);
      recovered.failCommand(failedClaim);
      expect(recovered.getCommandClaimState(botUid, group, failedClaim.messageId)).toBe("failed");
      expect(recovered.claimCommand(failedClaim)).toBe(true);
      expect(recovered.getCommandClaimState(botUid, group, failedClaim.messageId)).toBe("pending");
      recovered.completeCommand(failedClaim);
      expect(recovered.getCommandClaimState(botUid, group, failedClaim.messageId)).toBe("completed");
      expect(recovered.claimCommand(failedClaim)).toBe(false);

      expect(recovered.acceptTask(taskClaim, task)).toBe(true);
      expect(recovered.getCommandClaimState(botUid, group, taskClaim.messageId)).toBeNull();
      expect(recovered.getCommandClaimState(botUid, group, "different-message")).toBeNull();
      expect(recovered.getCommandClaimState("different-bot", group, claim.messageId)).toBeNull();
      expect(recovered.getCommandClaimState(
        botUid,
        { channelType: 2, channelId: "different-chat" },
        claim.messageId,
      )).toBeNull();
    } finally {
      recovered.close();
    }
  });
});
