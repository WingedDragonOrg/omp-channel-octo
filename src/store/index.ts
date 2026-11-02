/**
 * `ChannelStore` 门面：SQLite 之上的一层持久化 API。物理形状与 db 辅助在 `store-schema.ts`，
 * 记录类型在 `store-records.ts`，各领域读写分布在 `store-chats/grants/tasks/cards/workers/ledgers/outbox/questions.ts`。
 */

import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ChatAppendMode, ChatTarget } from "../contracts.ts";
import type { BindingRecord, InboundClaim, TaskRecord, TaskRestartRecovery, TaskStopIntent, ProgressCardRecord, OutboxRecord, OutboxInsert, TextStreamOutboxCommit, QuestionCloseReason, QuestionInsert, QuestionRecord, WorkerLedgerRecord, ContextLedgerRecord, HistoryLedgerRecord, AllowedChatRecord, ChatGrantOperationInput, ChatGrantOperationRecord, CommandClaimState } from "./store-records.ts";
import { applySchema } from "./store-schema.ts";
import { claimOwnerEnrollment, clearChatModel, ensureBinding, getBinding, getBotUid, getChatMode, getChatModel, getOwnerId, grantAllowedChat, hasOwnerEnrollment, listAllowedChats, setBotUid, setChatMode, setChatModel, setOwnerId, updateBindingSession } from "./store-chats.ts";
import { acceptTask, cancelQueued, claimCommand, clearRestartNotices, clearTaskStopIntent, completeCommand, countQueuedTasks, failCommand, getActiveTask, getCommandClaimState, getLatestTask, getTask, isTaskResuming, listQueuedTasks, markCancelled, markCompleted, markFailed, markInterrupted, markStarted, markTaskPromptAccepted, markTaskPromptStarted, markTaskStopIntent, prepareTasksForRestart, releaseClaim } from "./store-tasks.ts";
import { clearProgressCard, listProgressCards, recordProgressCardAttempt, saveProgressCard } from "./store-cards.ts";
import { listWorkerLedger, markWorkerStopped, recordWorker } from "./store-workers.ts";
import { advanceHistoryLedger, clearContextLedger, commitContextInjection, commitHistoryInjection, getContextLedger, getHistoryLedger, prepareContextInjection, prepareHistoryInjection, pruneHistoryLedger } from "./store-ledgers.ts";
import { acknowledgeTextOutbox, beginTextCreate, bindTextOutbox, commitTextStream, countPendingOutbox, countUnknownOutbox, enqueueOutbox, failTextOutbox, getBotEventCursor, getOutbox, listPendingOutbox, listPendingOutboxTargets, listTextStream, markOutboxDead, markOutboxFailure, markOutboxSent, setBotEventCursor, updateTextOutbox } from "./store-outbox.ts";
import { answerClosedQuestion, answerQuestion, answerQuestions, cancelQuestion, cancelQuestions, cancelTaskQuestions, createQuestion, getQuestion, listPendingQuestions, listQuestionsByCard, listTaskQuestions, markQuestionsSettled, pendingQuestionIds, reserveQuestionCardSeq, setQuestionCard, setQuestionsCard, updateQuestionPayloads } from "./store-questions.ts";
import { acceptChatGrantOperation, getChatGrantOperation, getChatGrantOperationByEvent, listPendingChatGrantOperations, markChatGrantConfigMirror, markChatGrantResultDelivered, nextChatGrantCardEditSeq } from "./store-grants.ts";

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

  /** Atomically records the accepted owner action and grants its target chat. */
  acceptChatGrantOperation(operation: ChatGrantOperationInput): boolean {
    return acceptChatGrantOperation(this.db, operation);
  }

  getChatGrantOperation(botUid: string, actionId: string): ChatGrantOperationRecord | null {
    return getChatGrantOperation(this.db, botUid, actionId);
  }

  getChatGrantOperationByEvent(botUid: string, eventId: number): ChatGrantOperationRecord | null {
    return getChatGrantOperationByEvent(this.db, botUid, eventId);
  }

  /** Operations with an unfinished config mirror or result receipt, oldest first. */
  listPendingChatGrantOperations(botUid: string): ChatGrantOperationRecord[] {
    return listPendingChatGrantOperations(this.db, botUid);
  }

  markChatGrantConfigMirror(botUid: string, actionId: string, state: "succeeded" | "failed", error?: string): boolean {
    return markChatGrantConfigMirror(this.db, botUid, actionId, state, error);
  }

  markChatGrantResultDelivered(botUid: string, actionId: string): boolean {
    return markChatGrantResultDelivered(this.db, botUid, actionId);
  }

  nextChatGrantCardEditSeq(botUid: string, actionId: string): number {
    return nextChatGrantCardEditSeq(this.db, botUid, actionId);
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

  getCommandClaimState(botUid: string, target: ChatTarget, messageId: string): CommandClaimState | null {
    return getCommandClaimState(this.db, botUid, target, messageId);
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

  /**
   * 启动恢复：一次事务内把该 Bot 遗留的在途任务规范化，返回 `stopped`（本次按持久停止意图结算的行，
   * 附带原意图）、`uncertain`（本次 fail-closed 的已派发行）与 `queued`（按接受顺序待执行）三组。
   * 只动状态，不调度、不取消提问；`session_id` 等元数据随行保留，`queued` 里用
   * {@link isTaskResuming} 决定重放原文还是续接会话。停止行的说明文案由调用方传入，与在线路径同源。
   */

  prepareTasksForRestart(botUid: string, reasonTexts: Record<TaskStopIntent, string>): TaskRestartRecovery {
    return prepareTasksForRestart(this.db, botUid, reasonTexts);
  }

  /**
   * 记下用户已确认的停止意图（先落库再动手）：`interrupt`/`question_timeout` 在进程被 SIGKILL 时
   * 仍是重启恢复的依据。只对仍在途的行生效，终态行静默 no-op 返回 false（此时连题也不动）。
   * `cancelQuestionIds` 只给 `question_timeout` 传（恢复快照里没人接手的遗留 pending 题）：与意图写入
   * 同一事务取消，逐行校验归属与状态；本回合正在等的那批题不传进来、保持可答。
   */

  markTaskStopIntent(taskId: string, intent: TaskStopIntent, cancelQuestionIds?: readonly string[]): boolean {
    return markTaskStopIntent(this.db, taskId, intent, cancelQuestionIds);
  }

  /** 停止意图被真实工作结果覆盖时清除（无条件清 NULL）；cancel 由在线路径直接落终态，不走这里。 */

  clearTaskStopIntent(taskId: string): void {
    clearTaskStopIntent(this.db, taskId);
  }

  /**
   * 发出 prompt 前调用：置 `prompt_started` 并记录真实会话身份（同时清掉旧的接受回执）。行必须
   * 处于 `started` 且未标记过，否则抛错（拿错行或重复标记都会覆盖恢复所需的会话身份）。
   */

  markTaskPromptStarted(taskId: string, sessionId: string): void {
    markTaskPromptStarted(this.db, taskId, sessionId);
  }

  /**
   * 收到 worker 的 `prompt_accepted` 事件时调用：确认这次派发真的进了会话。只要求派发标记已落账，
   * 晚到或重复到达都幂等。
   */

  markTaskPromptAccepted(taskId: string): void {
    markTaskPromptAccepted(this.db, taskId);
  }

  /** 在途任务是否必须续接既有会话（prompt 已落账且已收到 OMP 接受回执）；终态任务一律为 false。 */

  isTaskResuming(taskId: string): boolean {
    return isTaskResuming(this.db, taskId);
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

  clearRestartNotices(botUid: string): void {
    clearRestartNotices(this.db, botUid);
  }

  cancelQueued(botUid: string, target: ChatTarget): string[] {
    return cancelQueued(this.db, botUid, target);
  }

  recordWorker(worker: WorkerLedgerRecord): void {
    recordWorker(this.db, worker);
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
  commitTextStream(stream: TextStreamOutboxCommit): boolean {
    return commitTextStream(this.db, stream);
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

  createQuestion(record: QuestionInsert): void {
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

  /**
   * 一条关闭后到达的答案：把 `cancelled`/`expired` 行 CAS 成 `answered`（记提交人与关闭原因）。
   * 这是迟到答复的唯一仲裁点——同一提交重投与重启重放都只有一个赢家，其余读到 `answered` 后静默。
   */

  answerClosedQuestion(id: string, botUid: string, target: ChatTarget, answer: string, answeredBy: string | null): boolean {
    return answerClosedQuestion(this.db, id, botUid, target, answer, answeredBy);
  }

  cancelQuestions(botUid: string, target: ChatTarget, reason: QuestionCloseReason): void {
    cancelQuestions(this.db, botUid, target, reason);
  }

  cancelQuestion(id: string, reason: QuestionCloseReason): boolean {
    return cancelQuestion(this.db, id, reason);
  }

  /**
   * 取消一个任务的全部待答提问并记下关闭原因，返回取消的行数；只把 `pending` 行置 `cancelled`，
   * 不涉卡片 UI。服务侧收尾（关卡片）需要「pending ∪ cancelled」这批行时，用这一条替掉逐行 CAS：
   * 单语句原子，不会在中途崩溃时留下半批仍可答的旧卡。
   */

  cancelTaskQuestions(taskId: string, reason: QuestionCloseReason): number {
    return cancelTaskQuestions(this.db, taskId, reason);
  }

  pendingQuestionIds(botUid: string, target: ChatTarget): string[] {
    return pendingQuestionIds(this.db, botUid, target);
  }

  /**
   * 该卡下一帧要用的编辑序号：单事务内取同卡同聊天已用过的最大值 + 1，并写回该卡的所有行。序号
   * 持久在行上，重启不会归零；首帧用的是 1，因此下一次编辑至少从 2 起。
   */

  reserveQuestionCardSeq(botUid: string, target: ChatTarget, cardMessageId: string): number {
    return reserveQuestionCardSeq(this.db, botUid, target, cardMessageId);
  }

  /**
   * 一轮 ask 调用的收场：只给尚未落定（`settled_at IS NULL`）的行盖时间，重复调用不改写已有时间戳，
   * 空数组 no-op。停机挂起时不调用，正是重开进程要认领的那批行。
   */

  markQuestionsSettled(ids: readonly string[]): void {
    markQuestionsSettled(this.db, ids);
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
   * 按行改写提问行的 payload（认领者身份写回）：每行带自己的 payload，一次事务内整批更新，只写
   * payload 列。All-or-none：行数对不上（行被并发删掉）抛错并回滚整批，绝不留下半组新身份；返回
   * 写成功的行数（等于入参行数）。
   */

  updateQuestionPayloads(updates: ReadonlyArray<{ id: string; payload: string }>): number {
    return updateQuestionPayloads(this.db, updates);
  }

  /**
   * Every question row one card message carries, oldest first. The state is deliberately
   * unfiltered: a click on an answered or expired card is a distinct outcome from a click on a
   * foreign card, and one submission of a multi-question card needs all of its rows.
   */

  listQuestionsByCard(botUid: string, target: ChatTarget, messageId: string): QuestionRecord[] {
    return listQuestionsByCard(this.db, botUid, target, messageId);
  }

  /**
   * 一个任务的全部提问行，写入顺序（created_at, rowid），状态不过滤：恢复时既能看到仍然挂着的
   * 等待，也能看到已经答复/取消的行，据此决定继续等还是重开问题。
   */

  listTaskQuestions(taskId: string): QuestionRecord[] {
    return listTaskQuestions(this.db, taskId);
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
