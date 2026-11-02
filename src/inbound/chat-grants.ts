/**
 * Owns the short-lived binding of an onboarding card and the durable owner-grant operation behind
 * its click. A card action is never authority by itself: the process-local send receipt binds it to
 * one Bot, owner, exact chat, and remote card; after durable acceptance the SQLite operation alone
 * is sufficient to resume its mirror and result across a restart.
 */

import { randomUUID } from "node:crypto";
import type { BotEventOutcome } from "../bot-events.ts";
import {
  buildChatGrantCard,
  buildChatGrantCardResult,
  CHAT_GRANT_ACTION_PREFIX,
  chatGrantPrompt,
  isChatGrantActionId,
} from "../cards/chat-grant-card.ts";
import type {
  AllowlistWriteback,
  BotCardAction,
  BotIdentity,
  ChannelTransport,
  ChatTarget,
  InboundMessage,
} from "../contracts.ts";
import { safeErrorMessage } from "../errors.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ChatGrantOperationInput, ChatGrantOperationRecord } from "../store/store-records.ts";
import { targetKey } from "../target.ts";

const CARD_OPERATION_TIMEOUT_MS = 10_000;
const GRANT_CARD_TTL_MS = 15 * 60 * 1_000;
const MAX_GRANT_CARDS = 200;
const OWNER_NOTICE_TTL_MS = 10 * 60 * 1_000;
const MAX_OWNER_NOTICE_KEYS = 200;

interface GrantCardSession {
  readonly actionId: string;
  readonly botUid: string;
  readonly ownerId: string;
  readonly target: ChatTarget & { channelType: 2 | 5 };
  readonly expiresAt: number;
  state: "sending" | "ready" | "failed" | "accepted";
  cardMessageId: string | null;
}

export interface ChatGrantDeskDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  readonly bot: () => BotIdentity | undefined;
  readonly stopping: () => boolean;
  readonly shutdownSignal: AbortSignal;
  readonly allowlistWriteback: AllowlistWriteback | undefined;
  readonly isChatPermitted: (target: ChatTarget) => boolean;
  readonly enqueueText: (target: ChatTarget, text: string, taskId: string | null) => void;
  readonly now?: () => number;
}

export interface ChatGrantDesk {
  offer(message: InboundMessage, bot: BotIdentity): Promise<void>;
  handleAction(eventId: number, action: BotCardAction): Promise<BotEventOutcome>;
  onRetriesExhausted(eventId: number, action: BotCardAction): Promise<"handled" | "pending">;
  recoverPending(botUid: string): Promise<void>;
}

function actionTarget(action: BotCardAction): ChatTarget {
  return { channelType: action.channelType, channelId: action.channelId };
}

function validGrantTarget(target: ChatTarget): target is ChatTarget & { channelType: 2 | 5 } {
  return (target.channelType === 2 || target.channelType === 5) && target.channelId.trim().length > 0;
}

function grantScope(channelType: 2 | 5): "本群" | "本子区" {
  return channelType === 5 ? "本子区" : "本群";
}

function successText(operation: ChatGrantOperationRecord, botName: string): string {
  const scope = grantScope(operation.channelType);
  return operation.configMirrorState === "failed"
    ? `已开通${scope}；配置文件写回失败，本次开通仅在状态盘生效。`
    : `已开通${scope}，后续 @${botName} 即可正常使用。`;
}

function withinBudget<T>(
  shutdownSignal: AbortSignal,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const timeout = AbortSignal.timeout(CARD_OPERATION_TIMEOUT_MS);
  const signal = AbortSignal.any([shutdownSignal, timeout]);
  const aborted = Promise.withResolvers<never>();
  const onAbort = (): void => aborted.reject(signal.reason ?? new Error("card operation timed out"));
  if (signal.aborted) onAbort();
  else signal.addEventListener("abort", onAbort, { once: true });
  return Promise.race([operation(signal), aborted.promise]).finally(() => {
    signal.removeEventListener("abort", onAbort);
  });
}

function sameOperationBinding(
  operation: ChatGrantOperationRecord,
  input: ChatGrantOperationInput,
): boolean {
  return operation.actionId === input.actionId
    && operation.botUid === input.botUid
    && operation.ownerId === input.ownerId
    && operation.actorId === input.actorId
    && operation.ownerId === operation.actorId
    && operation.channelType === input.channelType
    && operation.channelId === input.channelId
    && operation.cardMessageId === input.cardMessageId;
}

export function createChatGrantDesk(deps: ChatGrantDeskDeps): ChatGrantDesk {
  const now = deps.now ?? Date.now;
  const sessions = new Map<string, GrantCardSession>();
  const ownerNoticeAt = new Map<string, number>();
  const operations = new Map<string, Promise<void>>();

  function register(session: GrantCardSession): void {
    for (const [actionId, current] of sessions) {
      if (current.expiresAt <= now()) sessions.delete(actionId);
    }
    while (sessions.size >= MAX_GRANT_CARDS) {
      const oldest = sessions.keys().next().value;
      if (oldest === undefined) break;
      sessions.delete(oldest);
    }
    sessions.set(session.actionId, session);
  }

  function lookup(actionId: string): GrantCardSession | undefined {
    const session = sessions.get(actionId);
    if (session === undefined) return undefined;
    if (session.expiresAt <= now()) {
      sessions.delete(actionId);
      return undefined;
    }
    return session;
  }

  function targetFromOperation(operation: ChatGrantOperationRecord): ChatTarget {
    return { channelType: operation.channelType, channelId: operation.channelId };
  }

  function operationMatchesAction(
    operation: ChatGrantOperationRecord,
    action: BotCardAction,
  ): boolean {
    return operation.actionId === action.actionId
      && operation.actorId === action.operatorUid.trim()
      && operation.channelType === action.channelType
      && operation.channelId === action.channelId
      && operation.cardMessageId === action.messageId;
  }

  function existingOperation(input: ChatGrantOperationInput): ChatGrantOperationRecord | null {
    const byEvent = deps.store.getChatGrantOperationByEvent(input.botUid, input.eventId);
    if (byEvent !== null && byEvent.actionId !== input.actionId) return null;
    const byAction = deps.store.getChatGrantOperation(input.botUid, input.actionId);
    if (byEvent !== null && byAction !== null && byEvent.actionId !== byAction.actionId) return null;
    const operation = byEvent ?? byAction;
    return operation !== null && sameOperationBinding(operation, input) ? operation : null;
  }

  async function sendTransientText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
  ): Promise<void> {
    await withinBudget(deps.shutdownSignal, (signal) =>
      deps.transport.sendText(target, text, clientMsgNo, signal),
    );
  }

  async function sendUnlistedFallback(message: InboundMessage, botName: string): Promise<void> {
    if (!validGrantTarget(message)) return;
    deps.enqueueText(message, chatGrantPrompt(message.channelType, botName), null);
  }

  async function offer(message: InboundMessage, bot: BotIdentity): Promise<void> {
    if (!validGrantTarget(message)) return;
    const botUid = deps.stopping() ? "" : bot.uid.trim();
    const ownerId = botUid.length === 0 ? "" : (deps.store.getOwnerId(botUid) ?? "").trim();
    const botName = bot.name.trim().length > 0 ? bot.name.trim() : bot.uid;
    if (botUid.length === 0 || ownerId.length === 0 || deps.stopping()) {
      await sendUnlistedFallback(message, botName);
      return;
    }

    const actionId = `${CHAT_GRANT_ACTION_PREFIX}${randomUUID()}`;
    const session: GrantCardSession = {
      actionId,
      botUid,
      ownerId,
      target: { channelType: message.channelType, channelId: message.channelId },
      expiresAt: now() + GRANT_CARD_TTL_MS,
      state: "sending",
      cardMessageId: null,
    };
    register(session);

    let receipt: Awaited<ReturnType<ChannelTransport["sendCard"]>> = null;
    try {
      receipt = await withinBudget(deps.shutdownSignal, (signal) =>
        deps.transport.sendCard(
          message,
          buildChatGrantCard(message.channelType, botName, actionId),
          randomUUID(),
          signal,
        ),
      );
    } catch (error) {
      console.error(
        `[chat-grant] card send failed target=${targetKey(message)}：${safeErrorMessage(error, "send failed")}`,
      );
    }

    const messageId = receipt?.messageId?.trim();
    if (!messageId || deps.stopping()) {
      session.state = "failed";
      await sendUnlistedFallback(message, botName);
      return;
    }
    session.cardMessageId = messageId;
    session.state = "ready";
  }

  async function findActionOperation(
    botUid: string,
    eventId: number,
    actionId: string,
  ): Promise<ChatGrantOperationRecord | null> {
    const byEvent = deps.store.getChatGrantOperationByEvent(botUid, eventId);
    if (byEvent !== null && byEvent.actionId !== actionId) return null;
    return deps.store.getChatGrantOperation(botUid, actionId);
  }

  async function processOperationRecord(record: ChatGrantOperationRecord): Promise<void> {
    const current = operations.get(record.actionId);
    if (current !== undefined) return await current;
    const work = processOperationRecordOnce(record);
    operations.set(record.actionId, work);
    try {
      await work;
    } finally {
      if (operations.get(record.actionId) === work) operations.delete(record.actionId);
    }
  }

  async function markMirror(
    record: ChatGrantOperationRecord,
    state: "succeeded" | "failed",
    error?: string,
  ): Promise<ChatGrantOperationRecord> {
    deps.store.markChatGrantConfigMirror(record.botUid, record.actionId, state, error);
    const updated = deps.store.getChatGrantOperation(record.botUid, record.actionId);
    if (updated === null || updated.configMirrorState === "pending") {
      throw new Error("chat grant config mirror state was not persisted");
    }
    return updated;
  }

  async function processOperationRecordOnce(record: ChatGrantOperationRecord): Promise<void> {
    let operation = deps.store.getChatGrantOperation(record.botUid, record.actionId);
    if (operation === null) throw new Error("accepted chat grant operation disappeared");
    if (operation.configMirrorState === "pending") {
      if (deps.allowlistWriteback === undefined) {
        operation = await markMirror(operation, "succeeded");
      } else {
        let mirrorError: unknown;
        try {
          await deps.allowlistWriteback.append(operation.channelId);
        } catch (error) {
          mirrorError = error;
        }
        if (mirrorError === undefined) {
          operation = await markMirror(operation, "succeeded");
        } else {
          const detail = safeErrorMessage(mirrorError, "config write-back failed").slice(0, 240);
          console.error(
            `[chat-grant] config write-back failed target=${targetKey(operation)}：${detail}`,
          );
          operation = await markMirror(operation, "failed", detail);
        }
      }
    }
    if (operation.resultState === "delivered") return;

    const bot = deps.bot();
    const botName = bot?.name.trim() || operation.botUid;
    const text = successText(operation, botName);
    let cardEdited = false;
    try {
      const cardSeq = deps.store.nextChatGrantCardEditSeq(operation.botUid, operation.actionId);
      await withinBudget(deps.shutdownSignal, (signal) =>
        deps.transport.editCard(
          targetFromOperation(operation),
          operation.cardMessageId,
          buildChatGrantCardResult(operation.channelType, text),
          cardSeq,
          signal,
        ),
      );
      cardEdited = true;
    } catch (error) {
      console.error(
        `[chat-grant] card result edit failed target=${targetKey(operation)}：${safeErrorMessage(error, "edit failed")}`,
      );
    }

    if (!cardEdited) {
      await withinBudget(deps.shutdownSignal, (signal) =>
        deps.transport.sendText(
          targetFromOperation(operation),
          text,
          operation.resultMessageId,
          signal,
        ),
      );
    }
    deps.store.markChatGrantResultDelivered(operation.botUid, operation.actionId);
    const finished = deps.store.getChatGrantOperation(operation.botUid, operation.actionId);
    if (finished === null || finished.resultState !== "delivered") {
      throw new Error("chat grant result delivery was not persisted");
    }
  }

  async function staleCard(eventId: number, action: BotCardAction, bot: BotIdentity): Promise<void> {
    const target = actionTarget(action);
    if (!validGrantTarget(target)) return;
    const scope = grantScope(target.channelType);
    const botName = bot.name.trim().length > 0 ? bot.name.trim() : bot.uid;
    const text = deps.isChatPermitted(target)
      ? `${scope}已开通。`
      : `这张${scope}开通卡已失效，请重新 @${botName} 获取开通方式。`;
    await sendTransientText(target, text, `chat-grant-stale-${eventId}`);
  }

  function ownerNoticeKey(target: ChatTarget, actorId: string): string {
    return `${targetKey(target)}:${actorId}`;
  }

  function canNotifyOwner(target: ChatTarget, actorId: string): boolean {
    const key = ownerNoticeKey(target, actorId);
    const sentAt = ownerNoticeAt.get(key);
    if (sentAt !== undefined && now() - sentAt < OWNER_NOTICE_TTL_MS) return false;
    if (ownerNoticeAt.size >= MAX_OWNER_NOTICE_KEYS) {
      const oldest = ownerNoticeAt.keys().next().value;
      if (oldest !== undefined) ownerNoticeAt.delete(oldest);
    }
    return true;
  }

  function noteOwnerNotice(target: ChatTarget, actorId: string): void {
    ownerNoticeAt.set(ownerNoticeKey(target, actorId), now());
  }

  async function sendOwnerOnlyNotice(
    target: ChatTarget,
    eventId: number,
    actorId: string,
  ): Promise<boolean> {
    if (!canNotifyOwner(target, actorId)) return true;
    try {
      await sendTransientText(target, "仅 Bot owner 可以开通。", `chat-grant-owner-${eventId}`);
      noteOwnerNotice(target, actorId);
      return true;
    } catch {
      return false;
    }
  }

  function queueOwnerOnlyNotice(target: ChatTarget, actorId: string): boolean {
    if (!canNotifyOwner(target, actorId)) return true;
    try {
      deps.enqueueText(target, "仅 Bot owner 可以开通。", null);
      noteOwnerNotice(target, actorId);
      return true;
    } catch {
      return false;
    }
  }

  async function handleAction(eventId: number, action: BotCardAction): Promise<BotEventOutcome> {
    if (!isChatGrantActionId(action.actionId)) return "ignored";
    if (!Number.isSafeInteger(eventId) || eventId <= 0 || action.messageId.trim().length === 0) return "ignored";
    const bot = deps.stopping() ? undefined : deps.bot();
    if (!bot) return "retry";
    const target = actionTarget(action);
    if (!validGrantTarget(target)) return "ignored";

    let existing: ChatGrantOperationRecord | null;
    try {
      existing = await findActionOperation(bot.uid, eventId, action.actionId);
    } catch {
      return "retry";
    }
    if (existing !== null) {
      if (!operationMatchesAction(existing, action)) return "ignored";
      try {
        await processOperationRecord(existing);
        return "handled";
      } catch (error) {
        console.error(
          `[chat-grant] accepted operation pending target=${targetKey(existing)}：${safeErrorMessage(error, "delivery pending")}`,
        );
        return "pending";
      }
    }

    let ownerId: string;
    try {
      ownerId = (deps.store.getOwnerId(bot.uid) ?? "").trim();
    } catch {
      return "retry";
    }
    if (ownerId.length === 0) return "ignored";

    const session = lookup(action.actionId);
    if (session === undefined) {
      if (action.operatorUid.trim() !== ownerId) return "ignored";
      try {
        await staleCard(eventId, action, bot);
        return "handled";
      } catch {
        return "retry";
      }
    }
    if (
      session.botUid !== bot.uid
      || session.ownerId !== ownerId
      || session.target.channelType !== action.channelType
      || session.target.channelId !== action.channelId
    ) return "ignored";
    const actorId = action.operatorUid.trim();
    if (session.state === "sending") {
      if (actorId === ownerId) return "retry";
      return queueOwnerOnlyNotice(target, actorId) ? "handled" : "retry";
    }
    if (session.state === "failed") return "handled";
    if (session.cardMessageId !== action.messageId) return "ignored";
    if (actorId !== ownerId) {
      return await sendOwnerOnlyNotice(target, eventId, actorId) ? "handled" : "retry";
    }

    const input: ChatGrantOperationInput = {
      actionId: session.actionId,
      eventId,
      botUid: session.botUid,
      ownerId: session.ownerId,
      actorId,
      channelType: session.target.channelType,
      channelId: session.target.channelId,
      cardMessageId: session.cardMessageId,
      resultMessageId: randomUUID(),
    };
    let operation: ChatGrantOperationRecord | null = null;
    try {
      if (deps.store.acceptChatGrantOperation(input)) {
        operation = deps.store.getChatGrantOperation(input.botUid, input.actionId);
        if (operation === null) throw new Error("accepted chat grant operation was not readable");
      } else {
        operation = existingOperation(input);
      }
    } catch (error) {
      try {
        operation = existingOperation(input);
      } catch {
        operation = null;
      }
      if (operation === null) {
        console.error(
          `[chat-grant] durable acceptance failed target=${targetKey(target)}：${safeErrorMessage(error, "store write failed")}`,
        );
        return "retry";
      }
    }
    if (operation === null || !operationMatchesAction(operation, action)) return "ignored";
    session.state = "accepted";
    try {
      await processOperationRecord(operation);
      sessions.delete(session.actionId);
      return "handled";
    } catch (error) {
      console.error(
        `[chat-grant] accepted operation pending target=${targetKey(operation)}：${safeErrorMessage(error, "delivery pending")}`,
      );
      return "pending";
    }
  }

  async function onRetriesExhausted(
    eventId: number,
    action: BotCardAction,
  ): Promise<"handled" | "pending"> {
    if (!isChatGrantActionId(action.actionId)) return "handled";
    const bot = deps.bot();
    if (!bot) return "pending";
    const target = actionTarget(action);
    if (!validGrantTarget(target)) return "handled";

    let accepted: ChatGrantOperationRecord | null;
    try {
      const byEvent = deps.store.getChatGrantOperationByEvent(bot.uid, eventId);
      const byAction = deps.store.getChatGrantOperation(bot.uid, action.actionId);
      accepted = byEvent ?? byAction;
    } catch {
      // If acceptance cannot be ruled out, retain the queue event rather than ACKing a partial grant.
      return "pending";
    }
    if (accepted !== null) {
      try {
        await processOperationRecord(accepted);
        return "handled";
      } catch (error) {
        console.error(
          `[chat-grant] accepted operation remains pending target=${targetKey(accepted)}：${safeErrorMessage(error, "delivery pending")}`,
        );
        return "pending";
      }
    }

    let currentOwner: string;
    try {
      currentOwner = (deps.store.getOwnerId(bot.uid) ?? "").trim();
    } catch {
      return "pending";
    }
    if (currentOwner.length === 0) return "handled";
    const session = lookup(action.actionId);
    if (session === undefined) {
      if (action.operatorUid.trim() !== currentOwner) return "handled";
      try {
        await staleCard(eventId, action, bot);
        return "handled";
      } catch {
        return "pending";
      }
    }
    if (
      session.botUid !== bot.uid
      || session.ownerId !== currentOwner
      || session.target.channelType !== action.channelType
      || session.target.channelId !== action.channelId
    ) return "handled";
    if (session.state === "failed") return "handled";
    const actorId = action.operatorUid.trim();
    if (actorId !== currentOwner) {
      if (session.state === "sending") {
        return queueOwnerOnlyNotice(target, actorId) ? "handled" : "pending";
      }
      if (session.state !== "ready" || session.cardMessageId !== action.messageId) return "handled";
      if (await sendOwnerOnlyNotice(target, eventId, actorId)) return "handled";
      return queueOwnerOnlyNotice(target, actorId) ? "handled" : "pending";
    }
    if (session.state === "sending" || session.state === "accepted") return "pending";
    if (session.state !== "ready" || session.cardMessageId !== action.messageId) return "handled";

    const scope = grantScope(session.target.channelType);
    const botName = bot.name.trim().length > 0 ? bot.name.trim() : bot.uid;
    const text = `${scope}开通失败，请重新 @${botName} 获取开通方式。`;
    try {
      await sendTransientText(
        session.target,
        text,
        `chat-grant-failed-${eventId}`,
      );
    } catch (error) {
      console.error(
        `[chat-grant] failure notice failed target=${targetKey(session.target)}：${safeErrorMessage(error, "send failed")}`,
      );
      try {
        deps.enqueueText(session.target, text, null);
      } catch (queueError) {
        console.error(
          `[chat-grant] failure notice could not be queued target=${targetKey(session.target)}：${safeErrorMessage(queueError, "queue failed")}`,
        );
        return "pending";
      }
    }
    session.state = "failed";
    return "handled";
  }

  async function recoverPending(botUid: string): Promise<void> {
    let pending: ChatGrantOperationRecord[];
    try {
      pending = deps.store.listPendingChatGrantOperations(botUid);
    } catch (error) {
      console.error(
        `[chat-grant] pending operations could not be loaded：${safeErrorMessage(error, "store read failed")}`,
      );
      return;
    }
    for (const operation of pending) {
      if (deps.stopping()) return;
      try {
        await processOperationRecord(operation);
      } catch (error) {
        console.error(
          `[chat-grant] pending operation will resume from Bot event queue target=${targetKey(operation)}：${safeErrorMessage(error, "delivery pending")}`,
        );
      }
    }
  }

  return { offer, handleAction, onRetriesExhausted, recoverPending };
}
