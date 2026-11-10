/**
 * 会话绑定的完整生命周期：聊天会话身份与 cwd 的解析、绑定行的核对与重绑，以及 `/new`、`/move`
 * 两条命令各自的候选 worker 台账、状态验证与回执时序。
 *
 * 模块不依赖 `service.ts`：持久层（绑定行、worker 台账、聊天模型）、worker 工厂与配置按原样注入。
 * 按 session 与工作区资源的租约本体由 `TaskScheduler` 持有，运行中任务与 blocked 集合仍是 `CoreService`
 * 的进程内状态，分别经 `tryAcquireCommandSession`、`hasRuntime`、`isDirectoryBlocked`/`blockDirectory`
 * 端口跨越；模块内不保存副本，所以被 block 的目录在绑定探测与任务调度两侧保持同一个语义。
 */

import { randomUUID } from "node:crypto";
import { join } from "node:path";
import type {
  BotIdentity,
  ChatTarget,
  CoreServiceConfig,
  WorkerFactory,
} from "../contracts.ts";
import { safeErrorMessage } from "../errors.ts";
import { modelWithLevel } from "../commands/model-switch.ts";
import type { Outbox } from "../outbox.ts";
import {
  findMovedSession,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./project-session.ts";
import { ensureProjectSessionDir, projectSessionDir, sessionsRoot } from "./session-dir.ts";
import { withStoppedWorker, type WorkerSession } from "../runtime/worker-lease.ts";
import type { ChannelStore } from "../store/index.ts";
import type { BindingRecord, ThreadWorkspaceRecord, WorkerLedgerRecord } from "../store/store-records.ts";
import type { SessionLease } from "../tasks/task-scheduler.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";
import {
  canonicalExistingDirectory,
  existingSessionFile,
  resolveWorkDirectory,
  stripOuterQuotes,
} from "../workspace/workdir.ts";
import { worktreeMatchesOwnership } from "../workspace/worktree.ts";

/**
 * 生命周期可以触及的自有状态之外的一切：持久化的绑定行/worker 台账/聊天模型、命令启动候选
 * worker 用的工厂、会话目录布局所依据的配置、绑定键所落的 bot 身份（getter：start 之后才知
 * 道），以及 `CoreService` 侧围绕 session 的状态——运行中任务、blocked 集合、session 租约与聊天出站队列。
 */
export interface SessionLifecycleDeps {
  readonly store: Pick<
    ChannelStore,
    | "getBinding"
    | "getThreadWorkspace"
    | "workspaceDirectories"
    | "ensureBinding"
    | "ensureBindingIfMatches"
    | "ensureBindingIfMatchesIfNoUnresolvedSessionTransition"
    | "getActiveTask"
    | "resolveChatModel"
    | "clearContextLedger"
    | "recordWorker"
    | "recordIngressWorker"
    | "markWorkerStopped"
  >;
  readonly workerFactory: WorkerFactory;
  readonly config: CoreServiceConfig;
  /** 绑定行按键的 bot 身份；start 之前不存在。 */
  readonly bot: () => BotIdentity | undefined;
  /** 该聊天是否有进程内运行任务——`/new`、`/move` 起手的 busy guard。 */
  readonly hasRuntime: (target: ChatTarget) => boolean;
  /** 该 cwd 是否因 worker 无法确认停止而被 block。 */
  readonly isDirectoryBlocked: (cwd: string) => boolean;
  /** Do not infer a session move while an accepted/unknown ingress transition owns the binding. */
  readonly hasUnresolvedSessionTransition: (target: ChatTarget) => boolean;
  /** 把该 cwd 标为不可用（worker 无法确认停止时）。 */
  readonly blockDirectory: (cwd: string) => void;
  /** Worker-stop uncertainty owned by one durable ingress session command. */
  readonly blockIngressDirectory: (jobId: number, cwd: string) => void;
  /**
   * 为当前聊天的 session（未绑定时按聊天标识）获取命令租约；当命令命中 thread_workspaces 的
   * entry/project 资源组（或绑定已 pin 的工作区）时，同时租下展开后的工作区资源键。普通 cwd 上的
   * 其他 session 不因此互相阻塞；`cwds` 仍用于拒绝仍被未确认停止的 worker 阻塞的目录。
   */
  readonly tryAcquireCommandSession: (
    target: ChatTarget,
    cwds: readonly string[],
  ) => Promise<SessionLease | undefined>;
  /** 为该聊天排一条回执文本，与其他命令路径一致。 */
  readonly enqueueText: Outbox["enqueueText"];
  /** Clears volatile reference evidence after a successfully committed fresh session switch. */
  readonly resetReferenceContext: (target: ChatTarget) => void;
}

export interface SessionCommandIngressHooks {
  readonly signal: AbortSignal;
  readonly jobId: number;
  readonly workerLedgerId: string;
  readonly stopping: () => boolean;
  readonly matchesExpectedBinding: (binding: BindingRecord | null) => boolean;
  readonly checkpointWaitingSession: (resourceCwds: readonly string[]) => boolean;
  readonly acquireSessionLease: (resourceCwds: readonly string[]) => Promise<SessionLease | undefined>;
  readonly prepareRecovery?: (
    resourceCwds: readonly string[],
  ) => Promise<{ kind: "ready" | "blocked" | "stale"; message?: string }>;
  readonly markDispatchIntent: (workerLedgerId: string, resourceCwds: readonly string[]) => boolean;
  /** Atomically persists any binding/context update, stable receipt, command claim and ingress handoff. */
  readonly handoffOutcome: (
    binding: BindingRecord | null,
    text: string,
    clearContext: boolean,
  ) => boolean;
  /** Persists an exact unknown-outcome receipt while retaining the ingress resource checkpoint. */
  readonly failUnknown: (text: string, resourceCwds: readonly string[]) => boolean;
}

export async function threadWorkspaceIsAvailable(workspace: ThreadWorkspaceRecord): Promise<boolean> {
  if (workspace.status !== "ready" || workspace.projects.length === 0) return false;
  if (workspace.layout === "shared" && workspace.entryPath === null) return false;
  if (workspace.entryPath !== null) {
    const entryPath = await canonicalExistingDirectory(workspace.entryPath).catch(() => null);
    if (entryPath !== workspace.entryPath) return false;
  }
  for (const project of workspace.projects) {
    if (!project.created || project.path === null || project.branch === null) return false;
    const projectPath = await canonicalExistingDirectory(project.path).catch(() => null);
    if (projectPath !== project.path) return false;
  }
  return true;
}

export async function threadWorkspaceIdentityMatches(workspace: ThreadWorkspaceRecord): Promise<boolean> {
  for (const project of workspace.projects) {
    if (project.path === null || project.branch === null ||
      !(await worktreeMatchesOwnership(project.path, project.repo, project.branch))) return false;
  }
  return true;
}

export class SessionLifecycle {
  constructor(private readonly deps: SessionLifecycleDeps) {}

  /**
   * 绑定行与磁盘上的会话文件核对一次：会话被外部（OMP 原生 `/move`、别的进程）搬走时把绑定
   * 重绑过去；写失败仍返回原绑定（调用方据此判定「当前绑定未改变」），被 block 的目录不再探测。
   */
  async reconcileExternalBinding(
    target: ChatTarget,
    binding: BindingRecord | null = this.deps.bot() === undefined
      ? null
      : this.deps.store.getBinding(this.deps.bot()!.uid, target),
  ): Promise<BindingRecord | null> {
    if (!binding || !binding.sessionFile) return binding;
    if (this.deps.hasUnresolvedSessionTransition(target)) return binding;
    if (this.deps.isDirectoryBlocked(binding.cwd)) return binding;
    const moved = await findMovedSession(binding, { roots: await this.sessionRootsFor(binding.cwd) });
    if (
      !moved ||
      (moved.sessionId === binding.sessionId &&
        moved.cwd === binding.cwd &&
        moved.sessionFile === binding.sessionFile)
    ) {
      return binding;
    }
    const rebound: BindingRecord = {
      ...binding,
      sessionId: moved.sessionId,
      cwd: moved.cwd,
      sessionFile: moved.sessionFile,
    };
    try {
      if (!this.deps.store.ensureBindingIfMatchesIfNoUnresolvedSessionTransition(rebound, binding)) {
        return this.deps.store.getBinding(binding.botUid, target);
      }
    } catch {
      return binding;
    }
    return rebound;
  }

  /**
   * 该聊天后续 worker 的默认 cwd：已校验的会话目录优先；共享工作区缺失时 fail closed。旧式单工作树
   * 保留其父群 cwd 回退，其它 cwd 顺序保持兼容。
   */
  async defaultWorkDirectory(target: ChatTarget): Promise<string> {
    const botUid = this.deps.bot()?.uid;
    const binding = botUid === undefined ? null : this.deps.store.getBinding(botUid, target);
    const workspace = botUid === undefined || target.channelType !== 5
      ? null
      : this.deps.store.getThreadWorkspace(botUid, target);
    let legacyFallback = false;
    if (workspace !== null) {
      const related = new Set(binding === null ? [] : this.deps.store.workspaceDirectories(binding.cwd));
      const bindingUsesWorkspace = binding === null
        || binding.cwdPinned === true
        || (workspace.entryPath !== null && related.has(workspace.entryPath))
        || workspace.projects.some((project) => project.path !== null && related.has(project.path));
      if (bindingUsesWorkspace) {
        const workspaceAvailable = await threadWorkspaceIsAvailable(workspace);
        legacyFallback = !workspaceAvailable
          && workspace.layout === "single"
          && workspace.legacyWorktreeName !== undefined
          && workspace.status === "ready";
        if (!workspaceAvailable && !legacyFallback) {
          throw new Error("子区工作区未就绪或资源已缺失");
        }
        if (!legacyFallback) {
          if (binding !== null) {
            const bindingCwd = await canonicalExistingDirectory(binding.cwd).catch(() => null);
            if (bindingCwd !== binding.cwd) throw new Error("子区工作区 cwd 已缺失");
            return bindingCwd;
          }
          // A ready owner record can exist before the first binding; bootstrap in owned resources,
          // never by falling through to the parent thread's cwd.
          const workspaceCwd = workspace.entryPath ?? workspace.projects[0]?.path ?? null;
          if (workspaceCwd === null) throw new Error("子区工作区路径缺失");
          return canonicalExistingDirectory(workspaceCwd);
        }
      }
    }
    if (binding?.sessionFile && !legacyFallback) return canonicalExistingDirectory(binding.cwd);
    if (binding?.cwdPinned === true) {
      try {
        return await canonicalExistingDirectory(binding.cwd);
      } catch {
        // An owned workspace was rejected above; retain the legacy fallback for unowned pins.
      }
    }
    if (target.channelType === 5) {
      const parentId = threadParentGroupId(target.channelId);
      const parent = parentId === null || botUid === undefined
        ? null
        : this.deps.store.getBinding(botUid, { channelId: parentId, channelType: 2 });
      if (parent) {
        try {
          return await canonicalExistingDirectory(parent.cwd);
        } catch {
          // The parent cwd is unusable: fall back to the default work directory.
        }
      }
    }
    return resolveWorkDirectory("", this.deps.config);
  }

  /**
   * 探测 worker 用的 cwd：该聊天自己的目录（与其回合一致），什么都解析不出来时为 `null`。
   */
  async probeDirectory(target: ChatTarget): Promise<string | null> {
    try {
      const botUid = this.deps.bot()?.uid;
      const workspace = botUid === undefined || target.channelType !== 5
        ? null
        : this.deps.store.getThreadWorkspace(botUid, target);
      if (workspace !== null) {
        return await this.defaultWorkDirectory(target).catch(() => null);
      }
      return await this.defaultWorkDirectory(target).catch(() =>
        resolveWorkDirectory("", this.deps.config),
      );
    } catch {
      return null;
    }
  }

  /**
   * `/new`（`/reset` 同义）：为聊天建一个新的可恢复 OMP 会话并把绑定换过去。候选 worker 先按新
   * 目录起、验证确实带出新会话文件后再停掉；任何一步失败都不会改动现有绑定，确认不了停止的
   * 目录被 block 以便调度避开。回执带上验证后的 cwd 与候选 worker 自报的模型。
   */
  async handleNew(
    target: ChatTarget,
    argument: string,
    ingress?: SessionCommandIngressHooks,
  ): Promise<void> {
    const bot = this.deps.bot();
    const resources: string[] = [];
    let previous: BindingRecord | null = null;
    let dispatchIntent = false;
    const ensureOpen = (): void => {
      if (ingress?.signal.aborted) {
        throw ingress.signal.reason instanceof Error ? ingress.signal.reason : new Error("session command aborted");
      }
    };
    const complete = (text: string, binding: BindingRecord | null = null, clearContext = false): void => {
      if (ingress !== undefined) {
        if (!ingress.handoffOutcome(binding, text, clearContext)) {
          throw new Error("session command outcome could not be handed off");
        }
        return;
      }
      if (binding !== null) {
        if (!this.deps.store.ensureBindingIfMatches(binding, previous, { clearSessionInitialization: true })) {
          throw new Error("会话绑定已变化，拒绝覆盖较新的绑定。");
        }
        if (clearContext) {
          this.deps.store.clearContextLedger(binding.botUid, target);
          this.deps.resetReferenceContext(target);
        }
      }
      this.deps.enqueueText(target, text, null);
    };
    const failUnknown = (text: string): void => {
      if (ingress === undefined) {
        this.deps.enqueueText(target, text, null);
        return;
      }
      if (ingress.stopping()) throw new Error("session command suspended during shutdown");
      if (!ingress.failUnknown(text, resources)) {
        throw new Error("unknown session command outcome could not be persisted");
      }
    };

    ensureOpen();
    if (bot && (this.deps.store.getActiveTask(bot.uid, target) || this.deps.hasRuntime(target))) {
      complete("当前聊天有运行中或排队任务，请先 /cancel。");
      return;
    }
    previous = await this.reconcileExternalBinding(target);
    ensureOpen();
    if (ingress !== undefined && !ingress.matchesExpectedBinding(previous)) {
      complete("当前会话绑定已变化，本次 /new 未执行。");
      return;
    }
    const hasExplicitCwd = argument.trim().length > 0;
    let cwd: string;
    try {
      const trimmed = argument.trim();
      if (
        (trimmed.length > 0 && stripOuterQuotes(argument).length === 0) ||
        (trimmed.length === 0 && argument.length > 0)
      ) {
        throw new Error("空引号不是有效目录");
      }
      cwd = trimmed.length > 0
        ? await resolveWorkDirectory(argument, this.deps.config)
        : await this.defaultWorkDirectory(target);
    } catch (error) {
      ensureOpen();
      complete(`目录无效：${safeErrorMessage(error, "无法访问该目录")}`);
      return;
    }
    ensureOpen();
    resources.push(cwd);
    if (ingress !== undefined && !ingress.checkpointWaitingSession(resources)) {
      throw new Error("session command checkpoint could not be persisted");
    }
    if (ingress !== undefined && ingress.prepareRecovery !== undefined) {
      const recovery = await ingress.prepareRecovery(resources);
      ensureOpen();
      if (recovery.kind !== "ready") {
        complete(recovery.message ?? "无法确认此前的会话切换已停止；请等待，或发送 /stop、/cancel 后重试 /new。");
        return;
      }
    }
    const commandLease = ingress === undefined
      ? await this.deps.tryAcquireCommandSession(target, resources)
      : await ingress.acquireSessionLease(resources);
    ensureOpen();
    if (!commandLease) {
      complete("相关会话仍在运行，或停止状态尚未确认；请等待，或发送 /stop、/cancel 后重试 /new。");
      return;
    }
    try {
      await commandLease.ready;
      ensureOpen();
      if (
        ingress !== undefined
        && !ingress.matchesExpectedBinding(
          bot === undefined ? null : this.deps.store.getBinding(bot.uid, target),
        )
      ) {
        complete("当前会话绑定已变化，本次 /new 未执行。");
        return;
      }
      const sessionId = randomUUID();
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.deps.config), cwd);
      const ledgerId = ingress?.workerLedgerId ?? `new-${sessionId}`;
      const ledgerSessionFile = join(sessionDir, `${sessionId}.jsonl`);
      if (ingress !== undefined) {
        if (!ingress.markDispatchIntent(ledgerId, resources)) {
          ensureOpen();
          throw new Error("session dispatch intent could not be persisted");
        }
        dispatchIntent = true;
      }
      let created: { fresh: ProjectSessionBinding; model: string | null };
      try {
        const model = this.deps.store.resolveChatModel(this.deps.bot()!.uid, target).model;
        created = await withStoppedWorker(
          {
            workerFactory: this.deps.workerFactory,
            store: this.deps.store,
            worker: {
              cwd,
              sessionDir,
              ...(model === null ? {} : { model }),
            },
            ledger: {
              id: ledgerId,
              botUid: this.deps.bot()!.uid,
              sessionId,
              sessionFile: ledgerSessionFile,
            },
            ...(ingress === undefined ? {} : {
              recordWorker: (worker: WorkerLedgerRecord) =>
                this.deps.store.recordIngressWorker(ingress.jobId, worker),
            }),
            blockDirectory: cwd => ingress === undefined
              ? this.deps.blockDirectory(cwd)
              : this.deps.blockIngressDirectory(ingress.jobId, cwd),
          },
          async worker => {
            const freshState: ProjectSessionState = await worker.refreshState();
            const previousForFresh: ProjectSessionBinding = previous ?? {
              sessionId: "",
              cwd,
              sessionFile: null,
            };
            const fresh = await verifyFreshSession(previousForFresh, freshState, cwd);
            const model = await this.createdSessionModel(worker);
            return { fresh, model };
          },
          ingress?.signal,
        );
      } catch {
        if (dispatchIntent) {
          failUnknown("无法确认新会话是否完整创建；相关目录保持保护，本次不会自动重试。");
        } else {
          complete("无法创建可恢复的 OMP 会话，当前绑定未改变。");
        }
        return;
      }
      const binding: BindingRecord = {
        botUid: this.deps.bot()!.uid,
        channelId: target.channelId,
        channelType: target.channelType,
        sessionId: created.fresh.sessionId,
        cwd: created.fresh.cwd,
        sessionFile: created.fresh.sessionFile,
        cwdPinned: hasExplicitCwd ? false : previous?.cwdPinned === true,
        // 主动换会话的唯一痕迹：问答卡据此拒绝切换前提出的旧问题的迟到提交（`/move` 保持会话
        // 身份，因此不写这里）。切换失败走下面的 catch，绑定与这个时间都不落。
        sessionSwitchedAt: Date.now(),
      };
      const receipt =
        `已创建会话 ${binding.sessionId}，cwd=${binding.cwd}${created.model === null ? "" : `，模型=${created.model}`}`;
      try {
        complete(receipt, binding, true);
      } catch {
        if (dispatchIntent) {
          failUnknown("新会话已创建但本地绑定结果未能确认；相关目录保持保护，本次不会自动重试。");
          return;
        }
        throw new Error("本地状态盘写入失败，会话未切换。");
      }
    } finally {
      commandLease.complete();
    }
  }

  /**
   * `/move <目录>`：在 worker 出生时将目标 cwd 持久化到台账，用 OMP 原生 `/move` 迁移并验证后，
   * 由 worker scope 确认退出和台账更新，再把迁移结果提交为绑定。失败时源与目标 cwd 都要确认停止。
   */
  async handleMove(
    target: ChatTarget,
    argument: string,
    ingress?: SessionCommandIngressHooks,
  ): Promise<void> {
    const bot = this.deps.bot();
    const resources: string[] = [];
    let dispatchIntent = false;
    let previous: BindingRecord | null = null;
    const ensureOpen = (): void => {
      if (ingress?.signal.aborted) {
        throw ingress.signal.reason instanceof Error ? ingress.signal.reason : new Error("session command aborted");
      }
    };
    const complete = (text: string, binding: BindingRecord | null = null): void => {
      if (ingress !== undefined) {
        if (!ingress.handoffOutcome(binding, text, false)) {
          throw new Error("session command outcome could not be handed off");
        }
        return;
      }
      if (binding !== null) {
        if (!this.deps.store.ensureBindingIfMatches(binding, previous, { clearSessionInitialization: true })) {
          throw new Error("会话绑定已变化，拒绝覆盖较新的绑定。");
        }
      }
      this.deps.enqueueText(target, text, null);
    };
    const failUnknown = (text: string): void => {
      if (ingress === undefined) {
        this.deps.enqueueText(target, text, null);
        return;
      }
      if (ingress.stopping()) throw new Error("session command suspended during shutdown");
      if (!ingress.failUnknown(text, resources)) {
        throw new Error("unknown session command outcome could not be persisted");
      }
    };

    ensureOpen();
    if (bot && (this.deps.store.getActiveTask(bot.uid, target) || this.deps.hasRuntime(target))) {
      complete("当前聊天有运行中或排队任务，请先 /cancel。");
      return;
    }
    previous = await this.reconcileExternalBinding(target);
    ensureOpen();
    if (ingress !== undefined && !ingress.matchesExpectedBinding(previous)) {
      complete("当前会话绑定已变化，本次 /move 未执行。");
      return;
    }
    if (!previous) {
      complete("当前聊天没有可迁移的会话，请先 /new。");
      return;
    }
    let cwd: string;
    try {
      const trimmed = argument.trim();
      if (
        trimmed.length === 0 ||
        stripOuterQuotes(argument).length === 0 ||
        (trimmed.length > 0 && stripOuterQuotes(argument).length === 0)
      ) {
        throw new Error("必须指定目标目录");
      }
      cwd = await resolveWorkDirectory(argument, this.deps.config);
    } catch (error) {
      ensureOpen();
      complete(`目录无效：${safeErrorMessage(error, "无法访问该目录")}`);
      return;
    }
    ensureOpen();
    resources.push(previous.cwd, cwd);
    if (!previous.sessionFile) {
      complete("当前绑定没有可恢复的精确会话文件，未迁移。");
      return;
    }
    const sessionFile = await existingSessionFile(previous.sessionFile);
    ensureOpen();
    if (!sessionFile) {
      complete("绑定的 OMP 会话文件不存在，未迁移。");
      return;
    }
    if (ingress !== undefined && !ingress.checkpointWaitingSession(resources)) {
      throw new Error("session command checkpoint could not be persisted");
    }
    const commandLease = ingress === undefined
      ? await this.deps.tryAcquireCommandSession(target, resources)
      : await ingress.acquireSessionLease(resources);
    ensureOpen();
    if (!commandLease) {
      complete("相关会话仍在运行，或停止状态尚未确认；请等待，或发送 /stop、/cancel 后再重试。");
      return;
    }
    try {
      await commandLease.ready;
      ensureOpen();
      if (
        ingress !== undefined
        && !ingress.matchesExpectedBinding(
          bot === undefined ? null : this.deps.store.getBinding(bot.uid, target),
        )
      ) {
        complete("当前会话绑定已变化，本次 /move 未执行。");
        return;
      }
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.deps.config), cwd);
      const ledgerId = ingress?.workerLedgerId ?? `move-${randomUUID()}`;
      if (ingress !== undefined) {
        if (!ingress.markDispatchIntent(ledgerId, resources)) {
          ensureOpen();
          throw new Error("session dispatch intent could not be persisted");
        }
        dispatchIntent = true;
      }
      let moved: ProjectSessionBinding;
      try {
        const model = this.deps.store.resolveChatModel(this.deps.bot()!.uid, target).model;
        moved = await withStoppedWorker(
          {
            workerFactory: this.deps.workerFactory,
            store: this.deps.store,
            worker: {
              cwd: previous.cwd,
              sessionDir,
              sessionFile,
              ...(model === null ? {} : { model }),
            },
            ledger: {
              id: ledgerId,
              botUid: this.deps.bot()!.uid,
              sessionId: previous.sessionId,
              sessionFile,
            },
            moveTargetCwd: cwd,
            ...(ingress === undefined ? {} : {
              recordWorker: (worker: WorkerLedgerRecord) =>
                this.deps.store.recordIngressWorker(ingress.jobId, worker),
            }),
            blockDirectory: cwd => ingress === undefined
              ? this.deps.blockDirectory(cwd)
              : this.deps.blockIngressDirectory(ingress.jobId, cwd),
          },
          async worker => {
            await worker.prompt(`/move ${cwd}`, () => {});
            const movedState: ProjectSessionState = await worker.refreshState();
            return verifyMovedSession(previous, movedState, cwd);
          },
          ingress?.signal,
        );
      } catch {
        if (dispatchIntent) {
          failUnknown("无法确认 OMP 原生 /move 是否完成；源与目标目录保持保护，本次不会自动重试。");
        } else {
          complete("无法验证 OMP 原生 /move，当前绑定未改变。");
        }
        return;
      }
      const binding: BindingRecord = {
        ...previous,
        sessionId: moved.sessionId,
        cwd: moved.cwd,
        sessionFile: moved.sessionFile,
        // An explicit /move leaves an isolated thread workspace behind.
        cwdPinned: false,
      };
      try {
        complete(`已迁移会话 ${binding.sessionId}，cwd=${binding.cwd}`, binding);
      } catch {
        if (dispatchIntent) {
          failUnknown("OMP 原生 /move 已验证，但本地绑定结果未能提交；源与目标目录保持保护，本次不会自动重试。");
          return;
        }
        throw new Error("本地状态盘写入失败，会话未迁移。");
      }
    } finally {
      commandLease.complete();
    }
  }

  /** 找回旧会话的候选根：项目目录在前，改动前的扁平根与工厂自己的根在后。 */
  private async sessionRootsFor(cwd: string): Promise<readonly string[]> {
    // 项目目录在前，改动前的扁平根留在候选里，旧会话仍能被找回。
    const roots = [
      projectSessionDir(sessionsRoot(this.deps.config), cwd),
      join(this.deps.config.stateDir, "sessions"),
    ];
    if (!this.deps.workerFactory.sessionRootsFor) return roots;
    try {
      return [...roots, ...(await this.deps.workerFactory.sessionRootsFor(cwd))];
    } catch {
      return roots;
    }
  }

  /**
   * The model the freshly created `/new`/`/reset` session actually came up with, asked of its own
   * candidate worker through the same `worker.models()` state query `/switch` and `/status` use: the stored
   * chat model when `--model` carried one, OMP's own default otherwise — it can never report
   * `default` as a model. A failed or empty answer degrades the creation receipt to its model-less
   * form instead of failing `/new` or inventing a value.
   */
  private async createdSessionModel(worker: WorkerSession): Promise<string | null> {
    const catalog = await worker.models().catch(() => null);
    if (catalog === null || catalog.current === null) return null;
    return modelWithLevel(catalog.current, catalog.level);
  }
}
