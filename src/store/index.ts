/**
 * `ChannelStore` 门面：SQLite 之上的一层持久化 API。物理形状与 db 辅助在 `store-schema.ts`，
 * 记录类型在 `store-records.ts`，各领域读写分布在 `store-chats/tasks/cards/ledgers/outbox/questions.ts`。
 */

import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ChatAppendMode, ChatTarget } from "../contracts.ts";
import type { BindingRecord, InboundClaim, TaskRecord, RestartNoticeRecord, ProgressCardRecord, OutboxRecord, OutboxInsert, QuestionRecord, WorkerLedgerRecord, ContextLedgerRecord, HistoryLedgerRecord, AllowedChatRecord } from "./store-records.ts";
import { applySchema } from "./store-schema.ts";
import { claimOwnerEnrollment, clearChatModel, ensureBinding, getBinding, getBotUid, getChatMode, getChatModel, getOwnerId, grantAllowedChat, hasOwnerEnrollment, listAllowedChats, setBotUid, setChatMode, setChatModel, setOwnerId, updateBindingSession } from "./store-chats.ts";
import { acceptTask, addRestartNotice, cancelQueued, claimCommand, clearRestartNotice, completeCommand, countQueuedTasks, failCommand, getActiveTask, getLatestTask, getTask, listQueuedTasks, listRestartNotices, markCancelled, markCompleted, markFailed, markInterrupted, markInterruptedOnStart, markStarted, releaseClaim } from "./store-tasks.ts";
import { clearProgressCard, listProgressCards, listWorkerLedger, markWorkerMoveTarget, markWorkerStopped, recordProgressCardAttempt, recordWorker, saveProgressCard } from "./store-cards.ts";
import { advanceHistoryLedger, clearContextLedger, commitContextInjection, commitHistoryInjection, getContextLedger, getHistoryLedger, prepareContextInjection, prepareHistoryInjection, pruneHistoryLedger } from "./store-ledgers.ts";
import { acknowledgeTextOutbox, beginTextCreate, bindTextOutbox, countPendingOutbox, countUnknownOutbox, enqueueOutbox, failTextOutbox, getBotEventCursor, getOutbox, listPendingOutbox, listPendingOutboxTargets, listTextStream, markOutboxDead, markOutboxFailure, markOutboxSent, setBotEventCursor, updateTextOutbox } from "./store-outbox.ts";
import { answerQuestion, answerQuestions, cancelQuestion, cancelQuestions, createQuestion, getQuestion, listPendingQuestions, listQuestionsByCard, pendingQuestionIds, setQuestionCard, setQuestionsCard } from "./store-questions.ts";

export class ChannelStore {
  private readonly db: Database;

  private constructor(readonly path: string) {
    this.db = new Database(path);
    applySchema(this.db);
  }

  static async open(path: string): Promise<ChannelStore> {
    await mkdir(dirname(path), { recursive: true });
    return new ChannelStore(path);
  }

  close(): void {
    this.db.close(false);
  }

  setBotUid(botUid: string): void {
    setBotUid(this.db, botUid);
  }

  getBotUid(): string | null {
    return getBotUid(this.db);
  }

  getOwnerId(botUid: string): string | null {
    return getOwnerId(this.db, botUid);
  }

  setOwnerId(botUid: string, ownerId: string): void {
    setOwnerId(this.db, botUid, ownerId);
  }

  hasOwnerEnrollment(botUid: string, target: ChatTarget, messageId: string): boolean {
    return hasOwnerEnrollment(this.db, botUid, target, messageId);
  }

  claimOwnerEnrollment(
    botUid: string,
    target: ChatTarget,
    messageId: string,
    senderId: string,
    text: string,
    outbox: OutboxInsert,
  ): boolean {
    return claimOwnerEnrollment(this.db, botUid, target, messageId, senderId, text, outbox);
  }

  /**
   * Records one chat the owner enabled at runtime. Idempotent: a replayed passphrase or a second
   * grant keeps the first row, so `granted_at`/`granted_by` stay the original decision.
   */

  grantAllowedChat(record: {
    botUid: string;
    channelType: 2 | 5;
    channelId: string;
    grantedBy: string;
  }): boolean {
    return grantAllowedChat(this.db, record);
  }

  listAllowedChats(botUid: string): AllowedChatRecord[] {
    return listAllowedChats(this.db, botUid);
  }

  ensureBinding(binding: BindingRecord): void {
    ensureBinding(this.db, binding);
  }

  getBinding(botUid: string, target: ChatTarget): BindingRecord | null {
    return getBinding(this.db, botUid, target);
  }

  acceptTask(claim: InboundClaim, task: TaskRecord): boolean {
    return acceptTask(this.db, claim, task);
  }

  claimCommand(claim: InboundClaim): boolean {
    return claimCommand(this.db, claim);
  }

  completeCommand(claim: InboundClaim): void {
    completeCommand(this.db, claim);
  }

  failCommand(claim: InboundClaim): void {
    failCommand(this.db, claim);
  }

  /**
   * The chat's append mode, defaulting to `steer` for every chat that never set one.
   * A stored value is durable across restarts; there is no per-bot default row.
   */

  getChatMode(botUid: string, target: ChatTarget): ChatAppendMode {
    return getChatMode(this.db, botUid, target);
  }

  setChatMode(botUid: string, target: ChatTarget, mode: ChatAppendMode): void {
    setChatMode(this.db, botUid, target, mode);
  }

  /** The chat's `provider/id` model, or null when it follows OMP's default. */
  getChatModel(botUid: string, target: ChatTarget): string | null {
    return getChatModel(this.db, botUid, target);
  }

  setChatModel(botUid: string, target: ChatTarget, model: string): void {
    setChatModel(this.db, botUid, target, model);
  }

  clearChatModel(botUid: string, target: ChatTarget): void {
    clearChatModel(this.db, botUid, target);
  }

  /**
   * Drops a command-class inbound claim so the same message can be claimed again,
   * used when a claim was taken for a delivery path that then fell back.
   */

  releaseClaim(claim: InboundClaim): void {
    releaseClaim(this.db, claim);
  }

  getTask(taskId: string): TaskRecord | null {
    return getTask(this.db, taskId);
  }

  getLatestTask(botUid: string, target: ChatTarget): TaskRecord | null {
    return getLatestTask(this.db, botUid, target);
  }

  getActiveTask(botUid: string, target: ChatTarget): TaskRecord | null {
    return getActiveTask(this.db, botUid, target);
  }

  countQueuedTasks(botUid: string, target: ChatTarget): number {
    return countQueuedTasks(this.db, botUid, target);
  }

  listQueuedTasks(): TaskRecord[] {
    return listQueuedTasks(this.db);
  }

  markStarted(taskId: string): boolean {
    return markStarted(this.db, taskId);
  }

  markCompleted(taskId: string, resultText: string | null): void {
    markCompleted(this.db, taskId, resultText);
  }

  markFailed(taskId: string, errorText: string): void {
    markFailed(this.db, taskId, errorText);
  }

  markCancelled(taskId: string, errorText = "cancelled"): void {
    markCancelled(this.db, taskId, errorText);
  }

  markInterrupted(taskId: string, errorText: string): void {
    markInterrupted(this.db, taskId, errorText);
  }

  markInterruptedOnStart(botUid: string): TaskRecord[] {
    return markInterruptedOnStart(this.db, botUid);
  }

  /**
   * Records the card frame the server accepted. Reads back through {@link listProgressCards} when
   * a later process has to finalize a card this one cannot.
   */

  saveProgressCard(record: Omit<ProgressCardRecord, "attempts">): void {
    saveProgressCard(this.db, record);
  }

  listProgressCards(botUid: string): ProgressCardRecord[] {
    return listProgressCards(this.db, botUid);
  }

  /**
   * Counts a spent recovery attempt and returns the new total. The sequence advances too: an
   * attempt whose response was lost may already have been accepted, so the next one must stay
   * strictly above every sequence the server has seen.
   */

  recordProgressCardAttempt(botUid: string, taskId: string, messageId: string): number {
    return recordProgressCardAttempt(this.db, botUid, taskId, messageId);
  }

  clearProgressCard(botUid: string, taskId: string, messageId: string): void {
    clearProgressCard(this.db, botUid, taskId, messageId);
  }

  addRestartNotice(record: RestartNoticeRecord): void {
    addRestartNotice(this.db, record);
  }

  listRestartNotices(botUid: string): RestartNoticeRecord[] {
    return listRestartNotices(this.db, botUid);
  }

  clearRestartNotice(id: string): void {
    clearRestartNotice(this.db, id);
  }

  cancelQueued(botUid: string, target: ChatTarget): string[] {
    return cancelQueued(this.db, botUid, target);
  }

  recordWorker(worker: WorkerLedgerRecord): void {
    recordWorker(this.db, worker);
  }

  markWorkerMoveTarget(id: string, cwd: string): void {
    markWorkerMoveTarget(this.db, id, cwd);
  }

  listWorkerLedger(botUid: string): WorkerLedgerRecord[] {
    return listWorkerLedger(this.db, botUid);
  }

  updateBindingSession(botUid: string, target: ChatTarget, sessionId: string, sessionFile: string | null): void {
    updateBindingSession(this.db, botUid, target, sessionId, sessionFile);
  }

  getContextLedger(
    botUid: string,
    target: ChatTarget,
    sessionId: string,
  ): ContextLedgerRecord | null {
    return getContextLedger(this.db, botUid, target, sessionId);
  }

  prepareContextInjection(
    botUid: string,
    target: ChatTarget,
    sessionId: string,
    version: number,
    requestedAttemptId?: string,
  ): ContextLedgerRecord {
    return prepareContextInjection(this.db, botUid, target, sessionId, version, requestedAttemptId);
  }

  commitContextInjection(
    botUid: string,
    target: ChatTarget,
    sessionId: string,
    version: number,
    attemptId: string,
  ): boolean {
    return commitContextInjection(this.db, botUid, target, sessionId, version, attemptId);
  }

  clearContextLedger(botUid: string, target: ChatTarget, sessionId?: string): void {
    clearContextLedger(this.db, botUid, target, sessionId);
  }

  getHistoryLedger(botUid: string, target: ChatTarget, sessionId: string): HistoryLedgerRecord | null {
    return getHistoryLedger(this.db, botUid, target, sessionId);
  }

  /**
   * Start or extend the pending injection for this (chat, session).
   * - existing pending row: keep its attemptId and previousSeq, raise lastSeq to max(existing, lastSeq)
   * - otherwise: previousSeq = committed row's lastSeq (or null), fresh attemptId, phase = pending
   * - invalid identity (channelType not 2/5, empty ids, non-integer/negative seq) throws
   */

  prepareHistoryInjection(
    botUid: string,
    target: ChatTarget,
    sessionId: string,
    lastSeq: number,
  ): HistoryLedgerRecord {
    return prepareHistoryInjection(this.db, botUid, target, sessionId, lastSeq);
  }

  /** CAS commit; true only when phase/staleness/attempt all match. */

  commitHistoryInjection(
    botUid: string,
    target: ChatTarget,
    sessionId: string,
    lastSeq: number,
    attemptId: string,
  ): boolean {
    return commitHistoryInjection(this.db, botUid, target, sessionId, lastSeq, attemptId);
  }

  /** Delete rows of the same chat whose session_id differs from keepSessionId. Returns deleted count. */

  pruneHistoryLedger(botUid: string, target: ChatTarget, keepSessionId: string): number {
    return pruneHistoryLedger(this.db, botUid, target, keepSessionId);
  }

  /**
   * Steered message accounting: raise last_seq to max(current, seq), creating a committed
   * row when absent. An existing row keeps its phase, so a pending replay window stays open.
   */

  advanceHistoryLedger(botUid: string, target: ChatTarget, sessionId: string, seq: number): void {
    advanceHistoryLedger(this.db, botUid, target, sessionId, seq);
  }

  markWorkerStopped(id: string): void {
    markWorkerStopped(this.db, id);
  }

  enqueueOutbox(record: OutboxInsert): void {
    enqueueOutbox(this.db, record);
  }

  getOutbox(id: string): OutboxRecord | null {
    return getOutbox(this.db, id);
  }

  beginTextCreate(id: string): boolean {
    return beginTextCreate(this.db, id);
  }

  listTextStream(botUid: string, taskId: string, streamMessageId: string): OutboxRecord[] {
    return listTextStream(this.db, botUid, taskId, streamMessageId);
  }

  updateTextOutbox(id: string, text: string, desiredVersion: number): boolean {
    return updateTextOutbox(this.db, id, text, desiredVersion);
  }

  bindTextOutbox(id: string, remoteMessageId: string): boolean {
    return bindTextOutbox(this.db, id, remoteMessageId);
  }

  acknowledgeTextOutbox(
    id: string,
    attemptedVersion: number,
    remoteMessageId: string | null,
  ): boolean {
    return acknowledgeTextOutbox(this.db, id, attemptedVersion, remoteMessageId);
  }

  failTextOutbox(id: string, attemptedVersion: number, errorText: string): boolean {
    return failTextOutbox(this.db, id, attemptedVersion, errorText);
  }

  listPendingOutbox(botUid: string, target?: ChatTarget): OutboxRecord[] {
    return listPendingOutbox(this.db, botUid, target);
  }

  listPendingOutboxTargets(botUid: string): ChatTarget[] {
    return listPendingOutboxTargets(this.db, botUid);
  }

  countPendingOutbox(botUid: string, target: ChatTarget): number {
    return countPendingOutbox(this.db, botUid, target);
  }

  countUnknownOutbox(botUid: string, target: ChatTarget): number {
    return countUnknownOutbox(this.db, botUid, target);
  }

  markOutboxSent(id: string): void {
    markOutboxSent(this.db, id);
  }

  markOutboxFailure(id: string, errorText: string): void {
    markOutboxFailure(this.db, id, errorText);
  }

  markOutboxDead(id: string, errorText: string): void {
    markOutboxDead(this.db, id, errorText);
  }

  createQuestion(record: Omit<QuestionRecord, "state" | "answer" | "answeredBy" | "cardMessageId">): void {
    createQuestion(this.db, record);
  }

  getQuestion(id: string): QuestionRecord | null {
    return getQuestion(this.db, id);
  }

  listPendingQuestions(botUid: string, target: ChatTarget): QuestionRecord[] {
    return listPendingQuestions(this.db, botUid, target);
  }

  answerQuestion(id: string, botUid: string, target: ChatTarget, answer: string, answeredBy: string | null): boolean {
    return answerQuestion(this.db, id, botUid, target, answer, answeredBy);
  }

  /**
   * Applies one answer per question inside a single transaction. Every row still compares-and-sets
   * on its own `pending` state, so a question that another path already answered is left alone;
   * the returned ids are the ones that landed.
   */

  answerQuestions(
    botUid: string,
    target: ChatTarget,
    answers: ReadonlyArray<{ id: string; answer: string; answeredBy: string | null }>,
  ): string[] {
    return answerQuestions(this.db, botUid, target, answers);
  }

  cancelQuestions(botUid: string, target: ChatTarget): void {
    cancelQuestions(this.db, botUid, target);
  }

  cancelQuestion(id: string): boolean {
    return cancelQuestion(this.db, id);
  }

  pendingQuestionIds(botUid: string, target: ChatTarget): string[] {
    return pendingQuestionIds(this.db, botUid, target);
  }

  /** Records the remote message id once the server accepted the question card. */

  setQuestionCard(id: string, messageId: string): void {
    setQuestionCard(this.db, id, messageId);
  }

  /** Records the same card message id on every row of one multi-question card. */

  setQuestionsCard(ids: readonly string[], messageId: string): void {
    setQuestionsCard(this.db, ids, messageId);
  }

  /**
   * Every question row one card message carries, oldest first. The state is deliberately
   * unfiltered: a click on an answered or expired card is a distinct outcome from a click on a
   * foreign card, and one submission of a multi-question card needs all of its rows.
   */

  listQuestionsByCard(botUid: string, target: ChatTarget, messageId: string): QuestionRecord[] {
    return listQuestionsByCard(this.db, botUid, target, messageId);
  }

  getBotEventCursor(botUid: string): number {
    return getBotEventCursor(this.db, botUid);
  }

  /** Advances the Bot event cursor; a lower id never overwrites a higher one. */

  setBotEventCursor(botUid: string, eventId: number): void {
    setBotEventCursor(this.db, botUid, eventId);
  }
}

export async function openChannelStore(stateDir: string): Promise<ChannelStore> {
  return ChannelStore.open(`${stateDir}/channel.sqlite`);
}
