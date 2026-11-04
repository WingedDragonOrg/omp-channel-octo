/**
 * `ChannelStore` 门面：SQLite 之上的一层持久化 API。物理形状与 db 辅助在 `store-schema.ts`，
 * 记录类型在 `store-records.ts`，各领域读写分布在 `store-chats/grants/tasks/cards/workers/ledgers/outbox/questions/references.ts`。
 */

import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ChatAppendMode, ChatTarget, ThreadListenLevel, WorkerGoal, WorktreeMergeOutcome } from "../contracts.ts";
import type { BindingRecord, GoalSessionRecord, GoalStopIntent, GoalTaskRecord, GoalTaskRequest, InboundClaim, TaskRecord, TaskRestartRecovery, TaskStopIntent, ProgressCardRecord, OutboxRecord, OutboxInsert, TextStreamOutboxCommit, QuestionCloseReason, QuestionInsert, QuestionRecord, WorkerLedgerRecord, ContextLedgerRecord, HistoryLedgerRecord, AllowedChatRecord, ChatGrantOperationInput, ChatGrantOperationRecord, CommandClaimState, ThreadWorkspaceRecord, ThreadWorkspaceIntentRecord, ThreadWorkspaceIntentInput, ThreadForkInput, ThreadForkRecord, ThreadListenRecord, ThreadMergeEventRecord, HostMergeLeaseRecord, HostMergeLeaseInput, HostRemovalLeaseInput, HostRemovalLeaseRecord, HostThreadRemovalOutcome } from "./store-records.ts";
import type { LoopBinding, LoopInput, LoopTask, LoopTaskKey, LoopTaskRoute, LoopThreadCreation, LoopWorkspace } from "../loop/types.ts";
import { applySchema } from "./store-schema.ts";
import { claimOwnerEnrollment, clearChatModel, clearThreadModel, ensureBinding, getBinding, getBotUid, getChatMode, getChatModel, getOwnerId, getThreadModel, grantAllowedChat, hasOwnerEnrollment, listAllowedChats, resolveChatModel, setBotUid, setChatMode, setChatModel, setOwnerId, setThreadModel, updateBindingSession } from "./store-chats.ts";
import { acceptCommitTask, acceptTask, cancelQueued, claimCommand, clearRestartNotices, clearTaskStopIntent, completeCommand, countQueuedTasks, failCommand, getActiveTask, getCommandClaimState, getLatestTask, getTask, getTaskByMessageId, isCommitTask, isTaskResuming, listQueuedTasks, markCancelled, markCompleted, markFailed, markInterrupted, markStarted, markTaskPromptAccepted, markTaskPromptStarted, markTaskStopIntent, prepareTasksForRestart, releaseClaim, suppressTaskRelay } from "./store-tasks.ts";
import { bindGoalTaskSession, clearGoalSession, finishGoalTaskProgress, getGoalSession, getGoalTask, isGoalTaskResuming, markGoalTaskAccepted, markGoalTaskAgentAccepted, markGoalTaskDispatched, saveGoalSessionProgress, stopGoalContinuation, updateGoalTaskProgress } from "./store-goals.ts";
import { clearProgressCard, listProgressCards, recordProgressCardAttempt, saveProgressCard } from "./store-cards.ts";
import { getWorkerLedger, listWorkerLedger, markWorkerStopped, recordWorker } from "./store-workers.ts";
import { beginHostRemovalStep, completeHostMergeLease, getHostMergeLeaseBySourceId, getHostRemovalLeaseBySourceId, listHostMergeLeases, listHostRemovalLeases, markHostMergeLeaseUnknown, markHostMergeProcessExited, markHostRemovalLeaseUnknown, markHostRemovalProcessExited, persistHostMergeOutcome, persistHostRemovalOutcome, recordHostMergeProcess, recordHostRemovalProcess, releaseHostRemovalLease, reserveHostMergeLease, reserveHostRemovalLease, settleHostMergeLease, settleHostRemovalLease, type HostMergeLeaseCompletion } from "./store-host-merges.ts";
import { advanceHistoryLedger, clearContextLedger, commitContextInjection, commitHistoryInjection, getContextLedger, getHistoryLedger, prepareContextInjection, prepareHistoryInjection, pruneHistoryLedger } from "./store-ledgers.ts";
import { acknowledgeTextOutbox, beginTextCreate, bindTextOutbox, commitTextStream, countPendingOutbox, countUnknownOutbox, enqueueOutbox, failTextOutbox, getBotEventCursor, getOutbox, listPendingOutbox, listPendingOutboxTargets, listTextStream, markOutboxDead, markOutboxFailure, markOutboxSent, setBotEventCursor, updateTextOutbox } from "./store-outbox.ts";
import { answerClosedQuestion, answerQuestion, answerQuestions, cancelQuestion, cancelQuestions, cancelTaskQuestions, createQuestion, getQuestion, listPendingQuestions, listQuestionsByCard, listTaskQuestions, markQuestionsSettled, pendingQuestionIds, reserveQuestionCardSeq, setQuestionCard, setQuestionsCard, updateQuestionPayloads } from "./store-questions.ts";
import { acceptChatGrantOperation, getChatGrantOperation, getChatGrantOperationByEvent, listPendingChatGrantOperations, markChatGrantConfigMirror, markChatGrantResultDelivered, nextChatGrantCardEditSeq } from "./store-grants.ts";
import { addThreadWorkspaceIntentChannel, completeThreadWorkspaceIntent, failThreadWorkspaceIntent, getThreadWorkspace, getThreadWorkspaceIntent, isThreadWorkspaceIntentBlocked, isWorkspaceDirectoryOwned, listThreadWorkspaceIntents, listThreadWorkspaces, saveThreadWorkspace, saveThreadWorkspaceIntent, saveThreadWorkspaces, workspaceDirectories } from "./store-workspaces.ts";
import { completeThreadFork, getThreadFork, getThreadForkByTarget, listThreadForks, saveThreadFork } from "./store-forks.ts";
import { beginThreadMerge, claimThreadMergeDispatchIntent, ensureThreadListen, getThreadListen, getThreadMergeEvent, isThreadMergeEventPending, listPendingThreadMergeEvents, listThreadMergeEvents, markThreadMergeEventDelivered, saveThreadMergeEvent, setThreadListen } from "./store-thread-listen.ts";
import { addThreadReferences, deleteThreadReferences, getThreadReferences, isThreadReferencesDeleted } from "./store-references.ts";
import * as storeWatch from "./store-watch.ts";
import type { WatchCheckCompletion, WatchCreateResult, WatchListItem, WatchRecord } from "../watch/types.ts";
import type { ThreadReference, ThreadReferences } from "../references/types.ts";
import { bindLoopTask, bindLoopWorkspace, claimLoopInputIntent, commitLoopScan, completeLoopThreadCreation, getLoopBinding, getLoopInput, getLoopTaskRoute, getLoopThreadCreation, isLoopInputObserved, listLoopBindings, listLoopInputs, listLoopThreadCreations, listLoopThreadTasks, markLoopInputDelivered, markLoopInputFault, markLoopInputPending, markLoopInputUnknown, markLoopThreadCreationUnknown, prepareLoopInput, recordLoopThreadAllocated, reserveLoopThreadCreation } from "./store-loop.ts";

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

  saveThreadWorkspace(record: ThreadWorkspaceRecord): void {
    saveThreadWorkspace(this.db, record);
  }

  saveThreadWorkspaces(records: readonly ThreadWorkspaceRecord[]): void {
    saveThreadWorkspaces(this.db, records);
  }

  getThreadWorkspace(botUid: string, target: ChatTarget): ThreadWorkspaceRecord | null {
    return getThreadWorkspace(this.db, botUid, target);
  }

  listThreadWorkspaces(botUid: string): ThreadWorkspaceRecord[] {
    return listThreadWorkspaces(this.db, botUid);
  }

  saveThreadWorkspaceIntent(record: ThreadWorkspaceIntentInput): void {
    saveThreadWorkspaceIntent(this.db, record);
  }

  addThreadWorkspaceIntentChannel(botUid: string, id: string, channelId: string): void {
    addThreadWorkspaceIntentChannel(this.db, botUid, id, channelId);
  }

  completeThreadWorkspaceIntent(botUid: string, id: string): void {
    completeThreadWorkspaceIntent(this.db, botUid, id);
  }

  failThreadWorkspaceIntent(botUid: string, id: string, error: string | null = null): void {
    failThreadWorkspaceIntent(this.db, botUid, id, error);
  }

  getThreadWorkspaceIntent(botUid: string, id: string): ThreadWorkspaceIntentRecord | null {
    return getThreadWorkspaceIntent(this.db, botUid, id);
  }
  saveThreadFork(record: ThreadForkInput): void {
    saveThreadFork(this.db, record);
  }

  getThreadFork(botUid: string, id: string): ThreadForkRecord | null {
    return getThreadFork(this.db, botUid, id);
  }

  getThreadForkByTarget(botUid: string, target: ChatTarget): ThreadForkRecord | null {
    return getThreadForkByTarget(this.db, botUid, target);
  }

  listThreadForks(botUid: string): ThreadForkRecord[] {
    return listThreadForks(this.db, botUid);
  }

  completeThreadFork(botUid: string, id: string): ThreadForkRecord {
    return completeThreadFork(this.db, botUid, id);
  }

  listThreadWorkspaceIntents(botUid: string, groupId: string): ThreadWorkspaceIntentRecord[] {
    return listThreadWorkspaceIntents(this.db, botUid, groupId);
  }

  isThreadWorkspaceIntentBlocked(botUid: string, target: ChatTarget): boolean {
    return isThreadWorkspaceIntentBlocked(this.db, botUid, target);
  }

  isWorkspaceDirectoryOwned(cwd: string): boolean {
    return isWorkspaceDirectoryOwned(this.db, cwd);
  }

  workspaceDirectories(cwd: string): string[] {
    return workspaceDirectories(this.db, cwd);
  }

  ensureThreadListen(record: Omit<ThreadListenRecord, "mergePending">): ThreadListenRecord {
    return ensureThreadListen(this.db, record);
  }

  getThreadListen(botUid: string, target: ChatTarget): ThreadListenRecord | null {
    return getThreadListen(this.db, botUid, target);
  }

  setThreadListen(botUid: string, target: ChatTarget, listen: ThreadListenLevel): ThreadListenRecord {
    return setThreadListen(this.db, botUid, target, listen);
  }

  getThreadReferences(botUid: string, target: ChatTarget): ThreadReferences {
    return getThreadReferences(this.db, botUid, target);
  }

  addThreadReferences(botUid: string, target: ChatTarget, references: readonly ThreadReference[]): void {
    addThreadReferences(this.db, botUid, target, references);
  }

  isThreadReferencesDeleted(botUid: string, target: ChatTarget): boolean {
    return isThreadReferencesDeleted(this.db, botUid, target);
  }

  deleteThreadReferences(botUid: string, target: ChatTarget): void {
    deleteThreadReferences(this.db, botUid, target);
  }

  beginThreadMerge(botUid: string, target: ChatTarget): void {
    beginThreadMerge(this.db, botUid, target);
  }

  saveThreadMergeEvent(record: ThreadMergeEventRecord): void {
    saveThreadMergeEvent(this.db, record);
  }

  getThreadMergeEvent(eventId: string): ThreadMergeEventRecord | null {
    return getThreadMergeEvent(this.db, eventId);
  }

  listThreadMergeEvents(botUid: string): ThreadMergeEventRecord[] {
    return listThreadMergeEvents(this.db, botUid);
  }

  listPendingThreadMergeEvents(botUid: string): ThreadMergeEventRecord[] {
    return listPendingThreadMergeEvents(this.db, botUid);
  }

  isThreadMergeEventPending(eventId: string): boolean {
    return isThreadMergeEventPending(this.db, eventId);
  }

  claimThreadMergeDispatchIntent(eventId: string): boolean {
    return claimThreadMergeDispatchIntent(this.db, eventId);
  }

  markThreadMergeEventDelivered(eventId: string): void {
    markThreadMergeEventDelivered(this.db, eventId);
  }

  acceptTask(claim: InboundClaim, task: TaskRecord, goalRequest?: GoalTaskRequest): boolean {
    return acceptTask(this.db, claim, task, goalRequest);
  }

  suppressTaskRelay(taskId: string): void {
    suppressTaskRelay(this.db, taskId);
  }

  getGoalTask(taskId: string): GoalTaskRecord | null {
    return getGoalTask(this.db, taskId);
  }

  getGoalSession(botUid: string, target: ChatTarget, sessionId: string): GoalSessionRecord | null {
    return getGoalSession(this.db, botUid, target, sessionId);
  }
  bindGoalTaskSession(taskId: string, expectedSessionId: string, sessionId: string, sessionFile: string): void {
    bindGoalTaskSession(this.db, taskId, expectedSessionId, sessionId, sessionFile);
  }

  clearGoalSession(botUid: string, target: ChatTarget, sessionId: string): void {
    clearGoalSession(this.db, botUid, target, sessionId);
  }

  markGoalTaskDispatched(taskId: string, sessionId: string, dispatchOp?: "create" | "resume" | "recover"): void {
    markGoalTaskDispatched(this.db, taskId, sessionId, dispatchOp);
  }

  markGoalTaskAccepted(taskId: string, goalId: string): void {
    markGoalTaskAccepted(this.db, taskId, goalId);
  }
  markGoalTaskAgentAccepted(taskId: string): void {
    markGoalTaskAgentAccepted(this.db, taskId);
  }

  updateGoalTaskProgress(taskId: string, goal: WorkerGoal | null): void {
    updateGoalTaskProgress(this.db, taskId, goal);
  }

  finishGoalTaskProgress(taskId: string, goal: WorkerGoal): void {
    finishGoalTaskProgress(this.db, taskId, goal);
  }

  saveGoalSessionProgress(botUid: string, target: ChatTarget, sessionId: string, goal: WorkerGoal): void {
    saveGoalSessionProgress(this.db, botUid, target, sessionId, goal);
  }

  isGoalTaskResuming(taskId: string): boolean {
    return isGoalTaskResuming(this.db, taskId);
  }

  stopGoalContinuation(
    botUid: string,
    target: ChatTarget,
    sessionId: string | null,
    intent: GoalStopIntent,
  ): string[] {
    return stopGoalContinuation(this.db, botUid, target, sessionId, intent);
  }

  acceptCommitTask(claim: InboundClaim, task: TaskRecord): boolean {
    return acceptCommitTask(this.db, claim, task);
  }

  isCommitTask(taskId: string): boolean {
    return isCommitTask(this.db, taskId);
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

  /** The chat's explicit model preference; null means no local override. */
  getChatModel(botUid: string, target: ChatTarget): string | null {
    return getChatModel(this.db, botUid, target);
  }

  setChatModel(botUid: string, target: ChatTarget, model: string): void {
    setChatModel(this.db, botUid, target, model);
  }

  clearChatModel(botUid: string, target: ChatTarget): void {
    clearChatModel(this.db, botUid, target);
  }

  getThreadModel(botUid: string, groupId: string): string | null {
    return getThreadModel(this.db, botUid, groupId);
  }

  setThreadModel(botUid: string, groupId: string, model: string): void {
    setThreadModel(this.db, botUid, groupId, model);
  }

  clearThreadModel(botUid: string, groupId: string): void {
    clearThreadModel(this.db, botUid, groupId);
  }

  resolveChatModel(
    botUid: string,
    target: ChatTarget,
  ): { model: string | null; source: "chat" | "thread" | "omp" } {
    return resolveChatModel(this.db, botUid, target);
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
  getTaskByMessageId(botUid: string, target: ChatTarget, messageId: string): TaskRecord | null {
    return getTaskByMessageId(this.db, botUid, target, messageId);
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
  getWorkerLedger(botUid: string, id: string): WorkerLedgerRecord | null {
    return getWorkerLedger(this.db, botUid, id);
  }
  reserveHostMergeLease(input: HostMergeLeaseInput): void {
    reserveHostMergeLease(this.db, input);
  }

  recordHostMergeProcess(id: string, process: { pid: number; pgid: number; identity: string }): void {
    recordHostMergeProcess(this.db, id, process);
  }

  markHostMergeProcessExited(id: string): void {
    markHostMergeProcessExited(this.db, id);
  }

  markHostMergeLeaseUnknown(id: string): void {
    markHostMergeLeaseUnknown(this.db, id);
  }

  completeHostMergeLease(id: string, completion: HostMergeLeaseCompletion): void {
    completeHostMergeLease(this.db, id, completion);
  }

  persistHostMergeOutcome(id: string, outcome: WorktreeMergeOutcome): void {
    persistHostMergeOutcome(this.db, id, outcome);
  }

  settleHostMergeLease(id: string): void {
    settleHostMergeLease(this.db, id);
  }

  getHostMergeLeaseBySourceId(botUid: string, target: ChatTarget, sourceId: string): HostMergeLeaseRecord | null {
    return getHostMergeLeaseBySourceId(this.db, botUid, target, sourceId);
  }

  listHostMergeLeases(botUid: string): HostMergeLeaseRecord[] {
    return listHostMergeLeases(this.db, botUid);
  }

  reserveHostRemovalLease(input: HostRemovalLeaseInput): void {
    reserveHostRemovalLease(this.db, input);
  }

  beginHostRemovalStep(id: string, step: "worktree" | "branch", cwd: string): void {
    beginHostRemovalStep(this.db, id, step, cwd);
  }

  recordHostRemovalProcess(
    id: string,
    process: { pid: number; pgid: number; identity: string; cwd: string },
  ): void {
    recordHostRemovalProcess(this.db, id, process);
  }

  markHostRemovalProcessExited(id: string): void {
    markHostRemovalProcessExited(this.db, id);
  }

  markHostRemovalLeaseUnknown(id: string): void {
    markHostRemovalLeaseUnknown(this.db, id);
  }

  persistHostRemovalOutcome(id: string, outcome: HostThreadRemovalOutcome): void {
    persistHostRemovalOutcome(this.db, id, outcome);
  }

  releaseHostRemovalLease(id: string): void {
    releaseHostRemovalLease(this.db, id);
  }

  settleHostRemovalLease(id: string): void {
    settleHostRemovalLease(this.db, id);
  }

  getHostRemovalLeaseBySourceId(
    botUid: string,
    target: ChatTarget,
    sourceId: string,
  ): HostRemovalLeaseRecord | null {
    return getHostRemovalLeaseBySourceId(this.db, botUid, target, sourceId);
  }

  listHostRemovalLeases(botUid: string): HostRemovalLeaseRecord[] {
    return listHostRemovalLeases(this.db, botUid);
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

  createWatch(record: WatchRecord): WatchCreateResult {
    return storeWatch.createWatch(this.db, record);
  }

  claimDueWatches(now: number, limit = 2, botUid?: string): WatchRecord[] {
    return storeWatch.claimDueWatches(this.db, now, limit, botUid);
  }

  setWatchProcess(id: string, generation: number, process: WatchRecord["process"]): boolean {
    return storeWatch.setWatchProcess(this.db, id, generation, process);
  }

  finishWatchCheck(id: string, generation: number, completion: WatchCheckCompletion, finishedAt: number): WatchRecord | null {
    return storeWatch.finishWatchCheck(this.db, id, generation, completion, finishedAt);
  }

  stopWatches(botUid: string, target: ChatTarget, id: string | "all", now: number, reason: string): WatchRecord[] {
    return storeWatch.stopWatches(this.db, botUid, target, id, now, reason);
  }

  wakeAllowed(id: string, now: number): boolean {
    return storeWatch.wakeAllowed(this.db, id, now);
  }

  markWatchWakeTask(id: string, taskId: string, now?: number): boolean {
    return storeWatch.markWatchWakeTask(this.db, id, taskId, now);
  }

  markWatchWakeDispatched(id: string, taskId: string, now: number): boolean {
    return storeWatch.markWatchWakeDispatched(this.db, id, taskId, now);
  }

  markWatchWakeFailed(id: string, reason: string): boolean {
    return storeWatch.markWatchWakeFailed(this.db, id, reason);
  }

  deleteWatchChat(botUid: string, target: ChatTarget, now: number): void {
    storeWatch.deleteWatchChat(this.db, botUid, target, now);
  }

  isWatchChatDeleted(botUid: string, target: ChatTarget): boolean {
    return storeWatch.isWatchChatDeleted(this.db, botUid, target);
  }

  getWatch(botUid: string, id: string): WatchRecord | null {
    return storeWatch.getWatch(this.db, botUid, id);
  }

  listWatches(botUid: string, target: ChatTarget, limit: number, offset: number): WatchListItem[] {
    return storeWatch.listWatches(this.db, botUid, target, limit, offset);
  }

  listWatchRecords(botUid?: string): WatchRecord[] {
    return storeWatch.listWatchRecords(this.db, botUid);
  }

  listActiveWatchRecords(botUid?: string): WatchRecord[] {
    return storeWatch.listActiveWatchRecords(this.db, botUid);
  }

  listWatchProcessRecords(botUid?: string): WatchRecord[] {
    return storeWatch.listWatchProcessRecords(this.db, botUid);
  }

  markWatchUnknown(id: string, generation: number, reason: string, now: number): boolean {
    return storeWatch.markWatchUnknown(this.db, id, generation, reason, now);
  }

  recoverWatchProcess(id: string, generation: number, now: number): WatchRecord | null {
    return storeWatch.recoverWatchProcess(this.db, id, generation, now);
  }

  expireWatchRecords(now: number): void {
    storeWatch.expireWatchRecords(this.db, now);
  }

  getWatchMatchEvent(id: string): storeWatch.WatchMatchEvent | null {
    return storeWatch.getWatchMatchEvent(this.db, id);
  }

  getBotEventCursor(botUid: string): number {
    return getBotEventCursor(this.db, botUid);
  }

  /** Advances the Bot event cursor; a lower id never overwrites a higher one. */

  setBotEventCursor(botUid: string, eventId: number): void {
    setBotEventCursor(this.db, botUid, eventId);
  }

  getLoopBinding(botUid: string, groupId: string): LoopBinding | null {
    return getLoopBinding(this.db, botUid, groupId);
  }

  listLoopBindings(botUid: string): LoopBinding[] {
    return listLoopBindings(this.db, botUid);
  }

  bindLoopWorkspace(
    botUid: string,
    groupId: string,
    workspace: LoopWorkspace,
    tasks: readonly LoopTask[],
  ): { binding: LoopBinding; changed: boolean } {
    return bindLoopWorkspace(this.db, botUid, groupId, workspace, tasks);
  }

  commitLoopScan(binding: LoopBinding, tasks: readonly LoopTask[]): boolean {
    return commitLoopScan(this.db, binding, tasks);
  }

  listLoopInputs(botUid: string): LoopInput[] {
    return listLoopInputs(this.db, botUid);
  }

  getLoopInput(id: string): LoopInput | null {
    return getLoopInput(this.db, id);
  }

  prepareLoopInput(id: string): LoopInput | null {
    return prepareLoopInput(this.db, id);
  }

  isLoopInputObserved(input: LoopInput): boolean {
    return isLoopInputObserved(this.db, input);
  }


  claimLoopInputIntent(id: string, target: ChatTarget): boolean {
    return claimLoopInputIntent(this.db, id, target);
  }

  markLoopInputDelivered(id: string): void {
    markLoopInputDelivered(this.db, id);
  }

  markLoopInputPending(id: string): void {
    markLoopInputPending(this.db, id);
  }

  markLoopInputUnknown(id: string): void {
    markLoopInputUnknown(this.db, id);
  }

  markLoopInputFault(id: string, notified: boolean): boolean {
    return markLoopInputFault(this.db, id, notified);
  }

  getLoopTaskRoute(key: LoopTaskKey): LoopTaskRoute | null {
    return getLoopTaskRoute(this.db, key);
  }

  bindLoopTask(key: LoopTaskKey, threadChannelId: string): LoopTaskRoute {
    return bindLoopTask(this.db, key, threadChannelId);
  }

  listLoopThreadTasks(botUid: string, groupId: string, threadChannelId: string): LoopTaskRoute[] {
    return listLoopThreadTasks(this.db, botUid, groupId, threadChannelId);
  }

  getLoopThreadCreation(key: LoopTaskKey): LoopThreadCreation | null {
    return getLoopThreadCreation(this.db, key);
  }

  reserveLoopThreadCreation(key: LoopTaskKey, operationId: string): LoopThreadCreation {
    return reserveLoopThreadCreation(this.db, key, operationId);
  }

  recordLoopThreadAllocated(key: LoopTaskKey, operationId: string, threadChannelId: string): void {
    recordLoopThreadAllocated(this.db, key, operationId, threadChannelId);
  }

  completeLoopThreadCreation(key: LoopTaskKey, operationId: string): boolean {
    return completeLoopThreadCreation(this.db, key, operationId);
  }

  markLoopThreadCreationUnknown(key: LoopTaskKey, operationId: string, error: string): void {
    markLoopThreadCreationUnknown(this.db, key, operationId, error);
  }

  listLoopThreadCreations(botUid: string): LoopThreadCreation[] {
    return listLoopThreadCreations(this.db, botUid);
  }
}

export async function openChannelStore(stateDir: string): Promise<ChannelStore> {
  return ChannelStore.open(`${stateDir}/channel.sqlite`);
}
