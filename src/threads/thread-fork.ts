import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { ChannelTransport, ChatTarget, InboundMessage, ThreadSummary, WorkerFactory } from "../contracts.ts";
import { safeDisplayText } from "../cards/execution-text.ts";
import { OctoError } from "../octo/error.ts";
import { isTransientServerFailure } from "../errors.ts";
import { loadValidatedSessionState } from "../session/project-session.ts";
import type { ChannelStore } from "../store/index.ts";
import type { TaskRecord, TanOperation, ThreadForkRecord, WorkerLedgerRecord } from "../store/store-records.ts";
import type { DestructiveResourceLease, TaskScheduler } from "../tasks/task-scheduler.ts";
import { targetKey, taskTarget } from "../target.ts";
import { openTrackedWorker } from "../runtime/worker-lease.ts";
import { canonicalExistingDirectory } from "../workspace/workdir.ts";
import { threadChannelId, threadParentGroupId, threadShortId } from "./thread-target.ts";

const FORK_NAME_LIMIT = 200;
const FORK_LEDGER_PREFIX = "thread-fork:";
const FORK_UNCERTAIN_PREP_TEXT = "分叉准备进程退出结果未确认；候选会话保持不可用，本次不会重试。";
const FORK_UNKNOWN_CREATE_TEXT = "远端子区创建结果不明；本次不会重复创建，目标保持不可用。";
const FORK_KNOWN_CREATE_FAILURE_TEXT = "远端明确拒绝创建子区；本次不会自动重试。";
const FORK_RECOVERY_TEXT = "分叉操作结果尚未确认；目标保持不可用，本次不会重复分叉或创建。";

interface ForkSource {
  readonly target: ChatTarget & { channelType: 2 | 5 };
  readonly groupId: string;
  readonly sessionId: string;
  readonly sessionFile: string;
  readonly cwd: string;
}

interface ActiveFork {
  readonly target: ChatTarget;
  readonly controller: AbortController;
  readonly operation: Promise<unknown>;
}

export interface ThreadForkControllerDeps {
  readonly store: Pick<
    ChannelStore,
    | "getBinding"
    | "saveThreadFork"
    | "getThreadFork"
    | "listThreadForks"
    | "saveThreadWorkspaceIntent"
    | "getThreadWorkspaceIntent"
    | "listThreadWorkspaceIntents"
    | "addThreadWorkspaceIntentChannel"
    | "failThreadWorkspaceIntent"
    | "completeThreadFork"
    | "getWorkerLedger"
    | "recordWorker"
    | "markWorkerStopped"
  >;
  readonly workerFactory: WorkerFactory;
  readonly transport: Pick<
    ChannelTransport,
    "getGroupInfo" | "getThread" | "createThread" | "updateGroupContext"
  >;
  readonly scheduler: Pick<TaskScheduler, "tryAcquireSharedResources">;
  readonly botUid: () => string | undefined;
  readonly stateDir: string;
  readonly shutdownSignal: AbortSignal;
  readonly blockDirectory: (cwd: string) => void;
}

/** Shared command/tool orchestration for durable, prompt-free native session forks. */
export class ThreadForkController {
  private readonly active = new Map<string, ActiveFork>();
  /** Held until the fork is ready, or permanently if its preparation process is uncertain. */
  private readonly sharedLeases = new Map<string, DestructiveResourceLease>();
  /** A successful HTTP response known only in memory after a local persistence error. */
  private readonly unpersistedRemoteResults = new Map<string, ThreadSummary>();

  constructor(private readonly deps: ThreadForkControllerDeps) {}

  command(message: InboundMessage, argument: string, onIntentReady?: () => void): Promise<string> {
    const requestedName = argument.trim().length === 0 ? undefined : argument;
    return this.start(message, `command:${message.messageId}`, requestedName, undefined, onIntentReady);
  }

  /** Stable operation key shared by `/fork` and `/tan` for the same command message. */
  commandOperationId(message: InboundMessage): string {
    if (!this.sourceLocation(message)) throw new Error("/tan 仅支持已授权主群或有效子区");
    const botUid = this.deps.botUid();
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法分叉会话");
    return this.operationId(botUid, message, `command:${message.messageId}`);
  }

  async tan(operation: TanOperation): Promise<ThreadForkRecord> {
    const botUid = this.deps.botUid();
    if (botUid === undefined || botUid !== operation.botUid) {
      throw new Error("Bot 身份不可用，无法恢复 TAN 分叉");
    }
    const target = {
      channelType: operation.sourceChannelType,
      channelId: operation.sourceChannelId,
    };
    const sourceLocation = this.sourceLocation(target);
    if (!sourceLocation) throw new Error("/tan 仅支持主群或有效子区");
    if (
      operation.id !== this.operationId(botUid, target, `command:${operation.messageId}`)
    ) {
      throw new Error("TAN operation identity does not match its captured source");
    }
    const capturedSource: ForkSource = {
      target: sourceLocation.target,
      groupId: sourceLocation.groupId,
      sessionId: operation.sourceSessionId,
      sessionFile: operation.sourceSessionFile,
      cwd: operation.cwd,
    };
    const current = this.active.get(operation.id);
    if (current) {
      await current.operation;
    } else {
      await this.runActive<string>(operation.id, target, undefined, signal =>
        this.runById(botUid, sourceLocation, operation.id, undefined, signal, capturedSource));
    }
    const record = this.deps.store.getThreadFork(botUid, operation.id);
    if (
      record === null
      || record.phase !== "ready"
      || record.sourceSessionId !== capturedSource.sessionId
      || record.sourceSessionFile !== capturedSource.sessionFile
      || record.cwd !== await canonicalExistingDirectory(capturedSource.cwd)
    ) {
      throw new Error(record?.error ?? "TAN 分叉未能发布为 ready");
    }
    return record;
  }

  tool(
    task: TaskRecord,
    args: Record<string, unknown>,
    signal: AbortSignal,
    toolCallId?: string,
  ): Promise<string> {
    if (toolCallId === undefined || toolCallId.length === 0) {
      return Promise.reject(new Error("fork tool requires the native toolCallId"));
    }
    const name = this.readToolName(args);
    return this.start(taskTarget(task), `tool:${toolCallId}`, name, signal);
  }

  get running(): boolean {
    return this.active.size > 0;
  }

  runningTargets(): string[] {
    return [...new Set([...this.active.values()].map(operation => targetKey(operation.target)))];
  }

  abortTarget(target: ChatTarget, reason: Error): boolean {
    let aborted = false;
    const key = targetKey(target);
    for (const operation of this.active.values()) {
      if (targetKey(operation.target) !== key) continue;
      operation.controller.abort(reason);
      aborted = true;
    }
    return aborted;
  }

  async settleTarget(target: ChatTarget): Promise<void> {
    const key = targetKey(target);
    const operations = [...this.active.values()]
      .filter(operation => targetKey(operation.target) === key)
      .map(operation => operation.operation);
    await Promise.allSettled(operations);
  }

  abortAll(reason: Error): void {
    for (const operation of this.active.values()) operation.controller.abort(reason);
  }

  async settle(): Promise<void> {
    await Promise.allSettled([...this.active.values()].map(operation => operation.operation));
  }

  /** Must run after every existing worker-ledger recovery and before queued tasks are staged. */
  async recoverStartup(botUid: string, tanOperationIds: ReadonlySet<string> = new Set()): Promise<void> {
    for (const record of this.deps.store.listThreadForks(botUid)) {
      const isTan = tanOperationIds.has(record.id);
      if (record.phase === "preparing") {
        const recovered = await this.recoverPreparing(
          record,
          this.deps.store.getWorkerLedger(botUid, this.ledgerId(record)) ?? undefined,
        );
        if (recovered.phase === "prepared") {
          if (isTan) {
            const active = this.runActive(record.id, {
              channelType: record.sourceChannelType,
              channelId: record.sourceChannelId,
            }, undefined, signal => this.continueAfterPreparation(recovered, signal));
            await active.catch(() => {});
          } else {
            this.terminalizePrepared(recovered);
          }
        }
        continue;
      }
      if (record.phase === "prepared") {
        if (isTan) {
          const active = this.runActive(record.id, {
            channelType: record.sourceChannelType,
            channelId: record.sourceChannelId,
          }, undefined, signal => this.continueAfterPreparation(record, signal));
          await active.catch(() => {});
        } else {
          this.terminalizePrepared(record);
        }
        continue;
      }
      if (record.phase === "creating") {
        this.deps.store.saveThreadFork({
          ...record,
          error: record.thread
            ? "远端子区已确认，但绑定尚未完成；不会重复创建，请显式恢复。"
            : "远端创建结果不确定；不会自动重试创建，请显式恢复。",
        });
        continue;
      }
      if (record.phase === "remote-created") {
        const active = this.runActive(record.id, {
          channelType: record.sourceChannelType,
          channelId: record.sourceChannelId,
        }, undefined, signal => this.finishRemoteCreated(record, signal));
        await active.catch(() => {});
      }
    }
  }

  private start(
    target: ChatTarget,
    operationKey: string,
    requestedName?: string,
    externalSignal?: AbortSignal,
    onIntentReady?: () => void,
  ): Promise<string> {
    const botUid = this.deps.botUid();
    if (botUid === undefined) return Promise.reject(new Error("Bot 身份未注册，无法分叉会话"));
    const source = this.sourceLocation(target);
    if (!source) return Promise.reject(new Error("/fork 仅支持已授权主群或子区"));
    const id = this.operationId(botUid, target, operationKey);
    const current = this.active.get(id);
    if (current) return current.operation as Promise<string>;
    return this.runActive<string>(id, target, externalSignal, signal =>
      this.runById(botUid, source, id, requestedName, signal, undefined, onIntentReady));
  }


  private runActive<T>(
    id: string,
    target: ChatTarget,
    externalSignal: AbortSignal | undefined,
    work: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const current = this.active.get(id);
    if (current) return current.operation as Promise<T>;
    const controller = new AbortController();
    const listeners: Array<() => void> = [];
    const abortFrom = (signal: AbortSignal, reason: Error): void => {
      const abort = (): void => controller.abort(signal.reason ?? reason);
      if (signal.aborted) abort();
      else {
        signal.addEventListener("abort", abort, { once: true });
        listeners.push(() => signal.removeEventListener("abort", abort));
      }
    };
    abortFrom(this.deps.shutdownSignal, new Error("channel stopping"));
    if (externalSignal !== undefined) abortFrom(externalSignal, new Error("fork tool cancelled"));
    const operation = work(controller.signal);
    const active = { target: { ...target }, controller, operation };
    this.active.set(id, active);
    const cleanup = (): void => {
      for (const remove of listeners) remove();
      if (this.active.get(id) === active) this.active.delete(id);
    };
    void operation.then(cleanup, cleanup);
    return operation;
  }

  private async runById(
    botUid: string,
    sourceLocation: Pick<ForkSource, "target" | "groupId">,
    id: string,
    requestedName: string | undefined,
    signal: AbortSignal,
    capturedSource?: ForkSource,
    onIntentReady?: () => void,
  ): Promise<string> {
    this.throwIfAborted(signal);
    // The durable identity lookup precedes source-binding validation: a redelivery after /new must
    // continue or report the captured operation rather than silently forking the new session.
    const existing = this.deps.store.getThreadFork(botUid, id);
    if (existing !== null) {
      if (
        existing.sourceChannelType !== sourceLocation.target.channelType
        || existing.sourceChannelId !== sourceLocation.target.channelId
        || existing.groupId !== sourceLocation.groupId
      ) {
        throw new Error("fork operation identity does not match its captured source");
      }
      if (
        capturedSource !== undefined
        && (
          existing.sourceSessionId !== capturedSource.sessionId
          || existing.sourceSessionFile !== capturedSource.sessionFile
          || existing.cwd !== await canonicalExistingDirectory(capturedSource.cwd)
        )
      ) {
        throw new Error("TAN source snapshot does not match its durable fork operation");
      }
      if (requestedName !== undefined && this.validateName(requestedName) !== existing.name) {
        throw new Error("同一分叉操作的子区名称不能更改");
      }
      onIntentReady?.();
      return this.continueExisting(existing, signal);
    }

    const blockedIntent = this.deps.store.listThreadWorkspaceIntents(botUid, sourceLocation.groupId)
      .find(intent => intent.status === "preparing");
    if (blockedIntent !== undefined) {
      throw new Error("该群已有未完成的子区创建操作；先恢复或确认原操作，再发起新的分叉。");
    }

    const requested = requestedName === undefined ? undefined : this.validateName(requestedName);
    const source = capturedSource === undefined
      ? await this.resolveSource(sourceLocation.target, sourceLocation.groupId)
      : {
          ...capturedSource,
          cwd: await canonicalExistingDirectory(capturedSource.cwd),
        };
    this.throwIfAborted(signal);
    const name = requested ?? await this.defaultName(source, signal);
    this.throwIfAborted(signal);
    const lease = await this.deps.scheduler.tryAcquireSharedResources(this.resourceOwner(id), [source.cwd]);
    if (!lease) throw new Error("源目录正被未确认 worker 或破坏性操作占用；本次未开始分叉。");
    this.sharedLeases.set(id, lease);

    let record: ThreadForkRecord = {
      id,
      botUid,
      sourceChannelType: source.target.channelType,
      sourceChannelId: source.target.channelId,
      sourceSessionId: source.sessionId,
      sourceSessionFile: source.sessionFile,
      cwd: source.cwd,
      groupId: source.groupId,
      name,
      preparationDir: join(this.deps.stateDir, "fork-preparations", id),
      preparationStarted: false,
      phase: "preparing",
      sessionId: null,
      sessionFile: null,
      thread: null,
      error: null,
    };
    try {
      // Revalidate the exact captured source after publishing shared resource use; this does not
      // inspect or replace a newer chat binding created by an explicit concurrent /new.
      await this.validateSnapshot(record.sourceSessionFile, record.sourceSessionId, record.cwd);
    } catch (error) {
      this.releaseSharedLease(id);
      throw error;
    }
    try {
      this.deps.store.saveThreadFork(record);
    } catch (error) {
      this.releaseSharedLease(id);
      throw new Error("无法持久记录分叉操作，未启动原生准备。", { cause: error });
    }
    try {
      this.deps.store.saveThreadWorkspaceIntent({
        id,
        botUid,
        groupId: record.groupId,
        kind: "workspace",
        creatorChannelType: record.sourceChannelType,
        creatorChannelId: record.sourceChannelId,
        status: "preparing",
        channelIds: [],
      });
    } catch (error) {
      record = { ...record, phase: "failed", error: "分叉创建意图无法持久化，未启动原生准备。" };
      try {
        this.deps.store.saveThreadFork(record);
      } catch {
        // The initial preparing row and absent intent still prohibit an automatic retry by ID.
      }
      this.releaseSharedLease(id);
      throw new Error(record.error!, { cause: error });
    }
    onIntentReady?.();
    record = await this.prepare(record, signal);
    return this.continueAfterPreparation(record, signal);
  }

  private async continueExisting(record: ThreadForkRecord, signal: AbortSignal): Promise<string> {
    if (record.phase === "ready") return this.formatReady(record);
    if (record.phase === "failed") {
      throw new Error(this.formatPartialFailure(record, record.error ?? FORK_RECOVERY_TEXT));
    }
    if (record.phase === "preparing") {
      const ledger = this.deps.store.getWorkerLedger(record.botUid, this.ledgerId(record)) ?? undefined;
      record = await this.recoverPreparing(record, ledger);
      if (record.phase !== "prepared") throw new Error(this.formatPartialFailure(record, record.error ?? FORK_RECOVERY_TEXT));
    }
    if (record.phase === "creating") {
      const thread = this.unpersistedRemoteResults.get(record.id);
      if (thread === undefined || !this.validRemoteThread(thread, record.groupId)) {
        throw new Error(this.formatPartialFailure(record, FORK_UNKNOWN_CREATE_TEXT));
      }
      record = { ...record, phase: "remote-created", thread, error: null };
      this.deps.store.saveThreadFork(record);
      this.unpersistedRemoteResults.delete(record.id);
      this.deps.store.addThreadWorkspaceIntentChannel(record.botUid, record.id, thread.channelId);
    }
    return this.continueAfterPreparation(record, signal);
  }

  private async continueAfterPreparation(record: ThreadForkRecord, signal: AbortSignal): Promise<string> {
    if (record.phase === "prepared") {
      await this.ensureSharedLease(record);
      if (signal.aborted) {
        if (this.deps.shutdownSignal.aborted) throw this.abortError(signal);
        record = this.failBeforeRemote(record, "分叉操作已取消；未创建远端子区。", true);
        throw new Error(this.formatPartialFailure(record, record.error ?? "分叉操作已取消；未创建远端子区。"));
      }
      record = await this.createRemoteThread(record, signal);
    }
    if (record.phase === "remote-created") {
      record = await this.finishRemoteCreated(record, signal);
    }
    if (record.phase === "ready") return this.formatReady(record);
    throw new Error(this.formatPartialFailure(record, record.error ?? FORK_RECOVERY_TEXT));
  }

  private async prepare(record: ThreadForkRecord, signal: AbortSignal): Promise<ThreadForkRecord> {
    let tracked: Awaited<ReturnType<typeof openTrackedWorker>> | undefined;
    let candidate: { sessionId: string; sessionFile: string } | undefined;
    let confirmedStopped = false;
    let processUncertain = false;
    const markBlocked = (cwd: string): void => {
      processUncertain = true;
      this.deps.blockDirectory(cwd);
    };
    try {
      await mkdir(record.preparationDir, { recursive: true, mode: 0o700 });
      record = { ...record, preparationStarted: true };
      this.deps.store.saveThreadFork(record);
      tracked = await openTrackedWorker({
        workerFactory: this.deps.workerFactory,
        store: this.deps.store,
        worker: {
          cwd: record.cwd,
          sessionDir: record.preparationDir,
          forkFrom: record.sourceSessionFile,
          signal,
        },
        ledger: {
          id: this.ledgerId(record),
          botUid: record.botUid,
          sessionId: record.id,
          sessionFile: null,
        },
        blockDirectory: markBlocked,
      });
      const state = await this.waitForAbort(tracked.worker.refreshState(), signal);
      const loaded = await this.validateSnapshot(state.sessionFile, state.sessionId, record.cwd);
      if (
        state.sessionId === record.sourceSessionId
        || loaded.sessionFile === record.sourceSessionFile
      ) throw new Error("native fork reused the source session identity or file");
      candidate = { sessionId: state.sessionId, sessionFile: loaded.sessionFile };
      record = { ...record, sessionId: candidate.sessionId, sessionFile: candidate.sessionFile, error: null };
      // Candidate identity is durable before attempting to stop the worker or finalize its ledger.
      this.deps.store.saveThreadFork(record);
      confirmedStopped = await tracked.close(true);
      tracked = undefined;
      if (!confirmedStopped) {
        processUncertain = true;
        markBlocked(record.cwd);
        record = { ...record, phase: "preparing", error: FORK_UNCERTAIN_PREP_TEXT };
        this.deps.store.saveThreadFork(record);
        throw new Error(FORK_UNCERTAIN_PREP_TEXT);
      }
      record = { ...record, phase: "prepared", error: null };
      this.deps.store.saveThreadFork(record);
      return record;
    } catch (error) {
      if (tracked !== undefined) {
        try {
          confirmedStopped = await tracked.close(true);
        } catch {
          confirmedStopped = false;
        }
        tracked = undefined;
      }
      if (!confirmedStopped && tracked === undefined && candidate !== undefined && !processUncertain) {
        // A ledger finalization error can leave process shutdown uncertain; keep the candidate and
        // shared-use block until startup verifies the persisted stopped ledger.
        markBlocked(record.cwd);
        record = {
          ...record,
          sessionId: candidate.sessionId,
          sessionFile: candidate.sessionFile,
          phase: "preparing",
          error: FORK_RECOVERY_TEXT,
        };
        try {
          this.deps.store.saveThreadFork(record);
        } catch {
          // The already-persisted candidate still prevents another native fork.
        }
        throw new Error(this.formatPartialFailure(record, FORK_RECOVERY_TEXT), { cause: error });
      }
      if (!confirmedStopped && processUncertain) {
        if (candidate !== undefined) {
          record = { ...record, sessionId: candidate.sessionId, sessionFile: candidate.sessionFile };
        }
        record = { ...record, phase: "preparing", error: FORK_UNCERTAIN_PREP_TEXT };
        try {
          this.deps.store.saveThreadFork(record);
        } catch {
          // The original preparing intent remains fail-closed if candidate persistence also failed.
        }
        throw new Error(this.formatPartialFailure(record, FORK_UNCERTAIN_PREP_TEXT), { cause: error });
      }
      record = this.failBeforeRemote(record, "原生分叉准备失败；本次不会重复启动准备进程。", true);
      throw new Error(this.formatPartialFailure(record, record.error ?? "原生分叉准备失败。"), { cause: error });
    }
  }

  private async createRemoteThread(record: ThreadForkRecord, signal: AbortSignal): Promise<ThreadForkRecord> {
    if (signal.aborted) {
      if (this.deps.shutdownSignal.aborted) throw this.abortError(signal);
      record = this.failBeforeRemote(record, "分叉操作已取消；未创建远端子区。", true);
      throw new Error(this.formatPartialFailure(record, record.error ?? "分叉操作已取消；未创建远端子区。"));
    }
    record = { ...record, phase: "creating", error: null };
    this.deps.store.saveThreadFork(record);
    let thread: ThreadSummary;
    try {
      thread = await this.deps.transport.createThread(record.groupId, record.name, signal);
    } catch (error) {
      const definitiveRejection = error instanceof OctoError
        && error.status !== undefined
        && error.permanent
        && error.code !== undefined
        && !isTransientServerFailure(error);
      const message = definitiveRejection ? FORK_KNOWN_CREATE_FAILURE_TEXT : FORK_UNKNOWN_CREATE_TEXT;
      record = { ...record, phase: definitiveRejection ? "failed" : "creating", error: message };
      this.deps.store.saveThreadFork(record);
      if (definitiveRejection) this.deps.store.failThreadWorkspaceIntent(record.botUid, record.id, message);
      this.releaseSharedLease(record.id);
      throw new Error(this.formatPartialFailure(record, message), { cause: error });
    }

    if (!this.validRemoteThread(thread, record.groupId)) {
      const message = "远端返回的子区身份与计划父群不一致；目标不会绑定，且不会重试创建。";
      record = { ...record, phase: "failed", thread, error: message };
      this.deps.store.saveThreadFork(record);
      this.deps.store.failThreadWorkspaceIntent(record.botUid, record.id, message);
      this.unpersistedRemoteResults.delete(record.id);
      this.releaseSharedLease(record.id);
      throw new Error(this.formatPartialFailure(record, message));
    }

    record = { ...record, phase: "remote-created", thread, error: null };
    let recordPersisted = false;
    try {
      this.deps.store.saveThreadFork(record);
      recordPersisted = true;
      this.unpersistedRemoteResults.delete(record.id);
      this.deps.store.addThreadWorkspaceIntentChannel(record.botUid, record.id, thread.channelId);
    } catch (error) {
      if (!recordPersisted) this.unpersistedRemoteResults.set(record.id, thread);
      throw new Error(this.formatPartialFailure(record, "远端子区已创建，但本地持久记录失败；目标保持不可用。"), { cause: error });
    }
    if (signal.aborted) {
      throw new Error(this.formatPartialFailure(record, "远端子区已创建，操作已中断；保留部分结果供安全恢复。"));
    }
    return record;
  }
  private async finishRemoteCreated(record: ThreadForkRecord, signal: AbortSignal): Promise<ThreadForkRecord> {
    const botUid = this.deps.botUid();
    if (botUid === undefined || botUid !== record.botUid) {
      throw new Error(this.formatPartialFailure(record, "Bot 身份不可用，无法完成分叉绑定"));
    }
    if (record.phase !== "remote-created" || !record.thread || !record.sessionId || !record.sessionFile) {
      throw new Error(this.formatPartialFailure(record, FORK_RECOVERY_TEXT));
    }
    try {
      await this.ensureSharedLease(record);
      this.throwIfAborted(signal);
    } catch (error) {
      throw new Error(this.formatPartialFailure(record, "已知远端子区尚未完成本地绑定；目标保持不可用。"), { cause: error });
    }
    let loaded: Awaited<ReturnType<typeof this.validateSnapshot>>;
    try {
      loaded = await this.validateSnapshot(record.sessionFile, record.sessionId, record.cwd);
    } catch (error) {
      throw new Error(this.formatPartialFailure(record, "分叉候选 session 在发布前验证失败；目标保持不可用。"), { cause: error });
    }
    if (
      record.sessionId === record.sourceSessionId
      || loaded.sessionFile !== record.sessionFile
      || loaded.sessionFile === record.sourceSessionFile
    ) {
      throw new Error(this.formatPartialFailure(record, "分叉候选 session 在发布前验证失败；目标保持不可用。"));
    }
    if (!this.validRemoteThread(record.thread, record.groupId)) {
      throw new Error(this.formatPartialFailure(record, "已知远端子区身份与分叉父群不一致；目标保持不可用。"));
    }
    const target: ChatTarget = { channelType: 5, channelId: record.thread.channelId };
    try {
      this.deps.store.addThreadWorkspaceIntentChannel(record.botUid, record.id, record.thread.channelId);
      await this.deps.transport.updateGroupContext(
        target,
        `用途：沿用分叉源的共享目录 ${record.cwd}`,
        signal,
      );
      this.throwIfAborted(signal);
      const ready = this.deps.store.completeThreadFork(record.botUid, record.id);
      this.releaseSharedLease(record.id);
      return ready;
    } catch (error) {
      const message = signal.aborted
        ? "已知远端子区尚未完成本地绑定；操作已中断，保留部分结果供安全恢复。"
        : "已知远端子区的说明或绑定尚未完成；目标保持不可用，可安全恢复而不会重新创建。";
      const latest = this.deps.store.getThreadFork(record.botUid, record.id) ?? record;
      if (latest.phase === "remote-created") {
        try {
          this.deps.store.saveThreadFork({ ...latest, error: message });
        } catch {
          // The remote-created row itself is already the durable no-recreate boundary.
        }
      }
      throw new Error(this.formatPartialFailure(latest, message), { cause: error });
    }
  }

  private async recoverPreparing(
    record: ThreadForkRecord,
    ledger: WorkerLedgerRecord | undefined,
  ): Promise<ThreadForkRecord> {
    if (ledger === undefined) {
      if (record.preparationStarted) {
        this.deps.blockDirectory(record.cwd);
        return record;
      }
      return this.failBeforeRemote(record, "原生分叉准备尚未启动；本次不会重试。", true);
    }
    if (ledger.state !== "stopped") {
      this.deps.blockDirectory(record.cwd);
      return record;
    }
    if (record.sessionId === null || record.sessionFile === null) {
      const failed = { ...record, phase: "failed" as const, error: "准备进程已停止，但没有可验证的新 session；不会重新分叉。" };
      this.deps.store.saveThreadFork(failed);
      this.deps.store.failThreadWorkspaceIntent(record.botUid, record.id, failed.error);
      return failed;
    }
    let loaded: Awaited<ReturnType<typeof this.validateSnapshot>>;
    try {
      loaded = await this.validateSnapshot(record.sessionFile, record.sessionId, record.cwd);
      if (
        record.sessionId === record.sourceSessionId
        || loaded.sessionFile !== record.sessionFile
        || loaded.sessionFile === record.sourceSessionFile
      ) {
        throw new Error("candidate identity mismatch");
      }
    } catch {
      const failed = { ...record, phase: "failed" as const, error: "准备 session 未通过身份核验；不会重新分叉。" };
      this.deps.store.saveThreadFork(failed);
      this.deps.store.failThreadWorkspaceIntent(record.botUid, record.id, failed.error);
      return failed;
    }
    const prepared = { ...record, phase: "prepared" as const, error: null };
    this.deps.store.saveThreadFork(prepared);
    try {
      await this.ensureSharedLease(prepared);
    } catch {
      // Durable prepared state plus the workspace intent prevents publication without a lease.
    }
    return prepared;
  }

  private async ensureSharedLease(record: ThreadForkRecord): Promise<void> {
    if (this.sharedLeases.has(record.id)) return;
    const lease = await this.deps.scheduler.tryAcquireSharedResources(this.resourceOwner(record.id), [record.cwd]);
    if (!lease) throw new Error("源目录当前不可安全共享；分叉目标保持不可用。");
    this.sharedLeases.set(record.id, lease);
  }

  private terminalizePrepared(record: ThreadForkRecord): ThreadForkRecord {
    return this.failBeforeRemote(
      record,
      "原生分叉准备已完成，但远端子区尚未创建；重启后不会自动续建。",
      true,
    );
  }

  private failBeforeRemote(
    record: ThreadForkRecord,
    message: string,
    failIntent: boolean,
  ): ThreadForkRecord {
    const failed = { ...record, phase: "failed" as const, error: message };
    this.deps.store.saveThreadFork(failed);
    if (failIntent && this.deps.store.getThreadWorkspaceIntent(record.botUid, record.id) !== null) {
      this.deps.store.failThreadWorkspaceIntent(record.botUid, record.id, message);
    }
    this.releaseSharedLease(record.id);
    return failed;
  }

  private async resolveSource(target: ChatTarget & { channelType: 2 | 5 }, groupId: string): Promise<ForkSource> {
    const botUid = this.deps.botUid();
    if (botUid === undefined) throw new Error("Bot 身份未注册，无法读取分叉来源");
    const binding = this.deps.store.getBinding(botUid, target);
    if (!binding || !binding.sessionId || !binding.sessionFile) {
      throw new Error("当前聊天没有可验证的持久 session 文件，未开始分叉。");
    }
    const cwd = await canonicalExistingDirectory(binding.cwd);
    const loaded = await this.validateSnapshot(binding.sessionFile, binding.sessionId, cwd);
    return {
      target,
      groupId,
      sessionId: loaded.sessionId,
      sessionFile: loaded.sessionFile,
      cwd,
    };
  }

  private async validateSnapshot(
    sessionFile: string,
    sessionId: string,
    cwd: string,
  ): Promise<{ sessionId: string; sessionFile: string }> {
    const loaded = await loadValidatedSessionState(sessionFile, sessionId);
    const expectedCwd = await canonicalExistingDirectory(cwd);
    if (loaded.state.cwd !== expectedCwd) throw new Error("session file cwd does not match its binding");
    return { sessionId: loaded.state.sessionId, sessionFile: loaded.state.sessionFile };
  }

  private async defaultName(source: ForkSource, signal: AbortSignal): Promise<string> {
    const existingName = source.target.channelType === 2
      ? (await this.deps.transport.getGroupInfo(source.groupId, signal)).name
      : (await this.deps.transport.getThread(
          source.groupId,
          threadShortId(source.target.channelId)!,
          signal,
        )).name;
    const printable = existingName.replace(/[\p{Cc}\p{Cf}]+/gu, " ").replace(/\s+/gu, " ").trim();
    const base = printable.length === 0 ? "会话" : printable;
    const suffix = " 分叉";
    return `${base.slice(0, FORK_NAME_LIMIT - suffix.length).trimEnd()}${suffix}`;
  }

  private readToolName(args: Record<string, unknown>): string | undefined {
    for (const key of Object.keys(args)) {
      if (key !== "name") throw new Error(`fork contains unsupported field: ${key}`);
    }
    if (args.name === undefined) return undefined;
    if (typeof args.name !== "string") throw new Error("name must be a printable thread name");
    return this.validateName(args.name);
  }

  private validateName(value: string): string {
    const name = value.trim();
    if (
      name.length === 0
      || name.length > FORK_NAME_LIMIT
      || /[\p{Cc}\p{Cf}]/u.test(name)
    ) {
      throw new Error(`子区名称必须为 1-${FORK_NAME_LIMIT} 个 printable 字符`);
    }
    return name;
  }

  private sourceLocation(target: ChatTarget): Pick<ForkSource, "target" | "groupId"> | null {
    if (target.channelType === 2 && target.channelId.length > 0) {
      return { target: { ...target, channelType: 2 }, groupId: target.channelId };
    }
    if (target.channelType !== 5) return null;
    const groupId = threadParentGroupId(target.channelId);
    const shortId = threadShortId(target.channelId);
    if (
      groupId === null
      || shortId === null
      || threadChannelId(groupId, shortId) !== target.channelId
    ) {
      return null;
    }
    return { target: { ...target, channelType: 5 }, groupId };
  }

  private validRemoteThread(thread: ThreadSummary, groupId: string): boolean {
    return typeof thread.shortId === "string"
      && thread.shortId.length > 0
      && typeof thread.name === "string"
      && thread.name.length > 0
      && thread.groupId === groupId
      && threadParentGroupId(thread.channelId) === groupId
      && threadShortId(thread.channelId) === thread.shortId
      && threadChannelId(groupId, thread.shortId) === thread.channelId;
  }

  private operationId(botUid: string, target: ChatTarget, operationKey: string): string {
    return `fork-${createHash("sha256")
      .update(JSON.stringify(["thread-fork", botUid, target.channelType, target.channelId, operationKey]))
      .digest("hex")}`;
  }

  private ledgerId(record: ThreadForkRecord): string {
    return `${FORK_LEDGER_PREFIX}${record.id}`;
  }

  private resourceOwner(id: string): string {
    return `thread-fork:${id}`;
  }

  private releaseSharedLease(id: string): void {
    this.sharedLeases.get(id)?.complete();
    this.sharedLeases.delete(id);
  }

  private formatReady(record: ThreadForkRecord): string {
    if (!record.thread || !record.sessionId) throw new Error(FORK_RECOVERY_TEXT);
    return `已创建子区 ${record.thread.name}（${record.thread.shortId}）；session_id=${record.sessionId}；cwd=${record.cwd}。目录共享，分叉历史已就绪，等待新消息。`;
  }
  private formatPartialFailure(record: ThreadForkRecord, message: string): string {
    const details: string[] = [];
    const sessionId = safeDisplayText(record.sessionId, 128);
    if (sessionId) details.push(`session_id=${sessionId}`);
    const shortId = safeDisplayText(record.thread?.shortId, 128);
    if (shortId) details.push(`short_id=${shortId}`);
    const name = safeDisplayText(record.thread?.name, 200);
    if (name) details.push(`name=${name}`);
    const cwd = safeDisplayText(record.cwd, 384);
    if (cwd) details.push(`cwd=${cwd}`);
    return details.length === 0 ? message : `${message} 已知部分结果：${details.join("；")}。`;
  }


  private throwIfAborted(signal: AbortSignal): void {
    if (signal.aborted) throw this.abortError(signal);
  }

  private abortError(signal: AbortSignal): Error {
    return signal.reason instanceof Error ? signal.reason : new Error("fork operation aborted");
  }

  private async waitForAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) throw this.abortError(signal);
    let onAbort: (() => void) | undefined;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => reject(this.abortError(signal));
      signal.addEventListener("abort", onAbort, { once: true });
    });
    try {
      return await Promise.race([operation, aborted]);
    } finally {
      if (onAbort) signal.removeEventListener("abort", onAbort);
    }
  }
}
