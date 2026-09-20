/**
 * 会话绑定的完整生命周期：聊天会话身份与 cwd 的解析、绑定行的核对与重绑，以及 `/new`、`/move`
 * 两条命令各自的候选 worker 台账、状态验证与回执时序。
 *
 * 模块不依赖 `service.ts`：持久层（绑定行、worker 台账、聊天模型）、worker 工厂与配置按原样注入。
 * 按 cwd 的租约本体由 `TaskScheduler` 持有，运行中任务与 blocked 集合仍是 `CoreService` 的进程内
 * 状态，分别经 `tryAcquireCommandDirectories`、`hasRuntime`、`isDirectoryBlocked`/`blockDirectory`
 * 端口跨越；模块内不保存副本，所以被 block 的目录在绑定探测与任务调度两侧保持同一个语义。
 */

import { randomUUID } from "node:crypto";
import { join } from "node:path";
import type {
  BotIdentity,
  ChatTarget,
  CoreServiceConfig,
  OmpWorker,
  WorkerFactory,
  WorkerProcess,
} from "./contracts.ts";
import { safeErrorMessage } from "./errors.ts";
import { modelWithLevel } from "./model-switch.ts";
import type { Outbox } from "./outbox.ts";
import {
  findMovedSession,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "./project-session.ts";
import { ensureProjectSessionDir, projectSessionDir, sessionsRoot } from "./session-dir.ts";
import type { ChannelStore } from "./store.ts";
import type { BindingRecord } from "./store-records.ts";
import type { DirectoryLease } from "./task-scheduler.ts";
import { threadParentGroupId } from "./thread-target.ts";
import {
  canonicalExistingDirectory,
  existingSessionFile,
  resolveWorkDirectory,
  stripOuterQuotes,
} from "./workdir.ts";

/**
 * 生命周期可以触及的自有状态之外的一切：持久化的绑定行/worker 台账/聊天模型、命令启动候选
 * worker 用的工厂、会话目录布局所依据的配置、绑定键所落的 bot 身份（getter：start 之后才知
 * 道），以及 `CoreService` 侧围绕目录的状态——运行中任务、blocked 集合、cwd 租约与聊天出站队列。
 */
export interface SessionLifecycleDeps {
  readonly store: Pick<
    ChannelStore,
    | "getBinding"
    | "ensureBinding"
    | "getActiveTask"
    | "getChatModel"
    | "clearContextLedger"
    | "recordWorker"
    | "markWorkerMoveTarget"
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
  /** 把该 cwd 标为不可用（worker 无法确认停止时）。 */
  readonly blockDirectory: (cwd: string) => void;
  /**
   * 为一次目录级命令租下全部给定 cwd，任一忙碌时返回 `undefined`（由 `TaskScheduler` 的目录
   * 租约链、运行中任务与 blocked 集合共同判定）。租约在命令的 finally 里释放。
   */
  readonly tryAcquireCommandDirectories: (cwds: readonly string[]) => DirectoryLease | undefined;
  /** 为该聊天排一条回执文本，与其他命令路径一致。 */
  readonly enqueueText: Outbox["enqueueText"];
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
      this.deps.store.ensureBinding(rebound);
    } catch {
      return binding;
    }
    return rebound;
  }

  /**
   * 该聊天后续 worker 的默认 cwd：已校验的会话目录优先；隔离工作区的 pin 目录失效时继续往下
   * 走；子区回退到父群绑定的目录（父群目录不可用则同样往下）；最后落到配置的默认工作目录。
   */
  async defaultWorkDirectory(target: ChatTarget): Promise<string> {
    const botUid = this.deps.bot()?.uid;
    const binding = botUid === undefined ? null : this.deps.store.getBinding(botUid, target);
    if (binding?.sessionFile) return canonicalExistingDirectory(binding.cwd);
    if (binding?.cwdPinned === true) {
      try {
        return await canonicalExistingDirectory(binding.cwd);
      } catch {
        // An isolated workspace that no longer exists falls back to the ordinary order below.
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
  async handleNew(target: ChatTarget, argument: string): Promise<void> {
    const bot = this.deps.bot();
    if (bot && (this.deps.store.getActiveTask(bot.uid, target) || this.deps.hasRuntime(target))) {
      await this.deps.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
      return;
    }
    const previous = await this.reconcileExternalBinding(target);
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
      await this.deps.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    const commandLease = this.deps.tryAcquireCommandDirectories([cwd]);
    if (!commandLease) {
      await this.deps.enqueueText(target, "目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
      const sessionId = randomUUID();
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.deps.config), cwd);
      const ledgerId = `new-${sessionId}`;
      const ledgerSessionFile = join(sessionDir, `${sessionId}.jsonl`);
      let spawned: WorkerProcess | undefined;
      let candidate: OmpWorker | undefined;
      let stopped = false;
      let persistenceFailed = false;
      try {
        const chatModel = this.deps.store.getChatModel(this.deps.bot()!.uid, target);
        candidate = await this.deps.workerFactory.create({
          cwd,
          sessionDir,
          ...(chatModel === null ? {} : { model: chatModel }),
          onSpawn: (processInfo) => {
            spawned = processInfo;
            this.deps.store.recordWorker({
              id: ledgerId,
              botUid: this.deps.bot()!.uid,
              sessionId,
              sessionFile: ledgerSessionFile,
              pid: processInfo.pid,
              pgid: processInfo.pgid,
              identity: processInfo.identity,
              cwd: processInfo.cwd,
              moveTargetCwd: null,
              state: "running",
            });
          },
        });
        const freshState: ProjectSessionState = await candidate.refreshState();
        const previousForFresh: ProjectSessionBinding = previous ?? {
          sessionId: "",
          cwd,
          sessionFile: null,
        };
        const fresh = await verifyFreshSession(previousForFresh, freshState, cwd);
        const model = await this.createdSessionModel(candidate);
        stopped = await candidate.stop();
        if (!stopped) {
          this.deps.blockDirectory(cwd);
          throw new Error("worker did not stop");
        }
        this.deps.store.markWorkerStopped(ledgerId);
        const binding: BindingRecord = {
          botUid: this.deps.bot()!.uid,
          channelId: target.channelId,
          channelType: target.channelType,
          sessionId: fresh.sessionId,
          cwd: fresh.cwd,
          sessionFile: fresh.sessionFile,
        };
        try {
          this.deps.store.ensureBinding(binding);
        } catch {
          persistenceFailed = true;
          throw new Error("本地状态盘写入失败，会话未切换。");
        }
        this.deps.store.clearContextLedger(this.deps.bot()!.uid, target);
        await this.deps.enqueueText(
          target,
          `已创建会话 ${binding.sessionId}，cwd=${binding.cwd}${model === null ? "" : `，模型=${model}`}`,
          null,
        );
      } catch {
        if (candidate && !stopped) {
          const verified = await candidate.stop().catch(() => false);
          if (verified) {
            this.deps.store.markWorkerStopped(ledgerId);
          } else {
            this.deps.blockDirectory(cwd);
          }
        } else if (spawned && !stopped) {
          const verified = await this.deps.workerFactory.recover(spawned).catch(() => false);
          if (verified) {
            this.deps.store.markWorkerStopped(ledgerId);
          } else {
            this.deps.blockDirectory(cwd);
          }
        }
        if (persistenceFailed) throw new Error("本地状态盘写入失败，会话未切换。");
        await this.deps.enqueueText(target, "无法创建可恢复的 OMP 会话，当前绑定未改变。", null);
      }
    } finally {
      commandLease.complete();
    }
  }

  /**
   * `/move <目录>`：让候选 worker 用 OMP 原生 `/move` 把当前会话迁到新 cwd——迁移台账先落
   * `moveTargetCwd` 再发 prompt，崩溃恢复才能同时避开源与目标目录——验证会话 id 与目标 cwd 后
   * 停掉 worker，绑定换成迁移结果并清掉隔离工作区的 pin。失败时两处 cwd 都要确认停止。
   */
  async handleMove(target: ChatTarget, argument: string): Promise<void> {
    const bot = this.deps.bot();
    if (bot && (this.deps.store.getActiveTask(bot.uid, target) || this.deps.hasRuntime(target))) {
      await this.deps.enqueueText(target, "当前聊天有运行中或排队任务，请先 /cancel。", null);
      return;
    }
    const previous = await this.reconcileExternalBinding(target);
    if (!previous) {
      await this.deps.enqueueText(target, "当前聊天没有可迁移的会话，请先 /new。", null);
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
      await this.deps.enqueueText(target, `目录无效：${safeErrorMessage(error, "无法访问该目录")}`, null);
      return;
    }
    if (!previous.sessionFile) {
      await this.deps.enqueueText(target, "当前绑定没有可恢复的精确会话文件，未迁移。", null);
      return;
    }
    const sessionFile = await existingSessionFile(previous.sessionFile);
    if (!sessionFile) {
      await this.deps.enqueueText(target, "绑定的 OMP 会话文件不存在，未迁移。", null);
      return;
    }
    const commandLease = this.deps.tryAcquireCommandDirectories([previous.cwd, cwd]);
    if (!commandLease) {
      await this.deps.enqueueText(target, "源目录或目标目录当前被任务或未确认会话占用，请稍后重试。", null);
      return;
    }
    try {
      await commandLease.ready;
      const sessionDir = await ensureProjectSessionDir(sessionsRoot(this.deps.config), cwd);
      const ledgerId = `move-${randomUUID()}`;
      let spawned: WorkerProcess | undefined;
      let candidate: OmpWorker | undefined;
      let stopped = false;
      let persistenceFailed = false;
      try {
        const chatModel = this.deps.store.getChatModel(this.deps.bot()!.uid, target);
        candidate = await this.deps.workerFactory.create({
          cwd: previous.cwd,
          sessionDir,
          sessionFile,
          ...(chatModel === null ? {} : { model: chatModel }),
          onSpawn: (processInfo) => {
            spawned = processInfo;
            this.deps.store.recordWorker({
              id: ledgerId,
              botUid: this.deps.bot()!.uid,
              sessionId: previous.sessionId,
              sessionFile,
              pid: processInfo.pid,
              pgid: processInfo.pgid,
              identity: processInfo.identity,
              cwd: processInfo.cwd,
              moveTargetCwd: null,
              state: "running",
            });
          },
        });
        this.deps.store.markWorkerMoveTarget(ledgerId, cwd);
        await candidate.prompt(`/move ${cwd}`, () => {});
        const movedState: ProjectSessionState = await candidate.refreshState();
        const moved = await verifyMovedSession(previous, movedState, cwd);
        stopped = await candidate.stop();
        if (!stopped) {
          this.deps.blockDirectory(previous.cwd);
          this.deps.blockDirectory(cwd);
          throw new Error("worker did not stop");
        }
        this.deps.store.markWorkerStopped(ledgerId);
        const binding: BindingRecord = {
          ...previous,
          sessionId: moved.sessionId,
          cwd: moved.cwd,
          sessionFile: moved.sessionFile,
          // An explicit /move leaves an isolated thread workspace behind.
          cwdPinned: false,
        };
        try {
          this.deps.store.ensureBinding(binding);
        } catch {
          persistenceFailed = true;
          throw new Error("本地状态盘写入失败，会话未迁移。");
        }
        await this.deps.enqueueText(target, `已迁移会话 ${binding.sessionId}，cwd=${binding.cwd}`, null);
      } catch {
        if (candidate && !stopped) {
          const verified = await candidate.stop().catch(() => false);
          if (verified) {
            this.deps.store.markWorkerStopped(ledgerId);
          } else {
            this.deps.blockDirectory(previous.cwd);
            this.deps.blockDirectory(cwd);
          }
        } else if (spawned && !stopped) {
          const verified = await this.deps.workerFactory.recover(spawned).catch(() => false);
          if (verified) {
            this.deps.store.markWorkerStopped(ledgerId);
          } else {
            this.deps.blockDirectory(previous.cwd);
            this.deps.blockDirectory(cwd);
          }
        }
        if (persistenceFailed) throw new Error("本地状态盘写入失败，会话未迁移。");
        await this.deps.enqueueText(target, "无法验证 OMP 原生 /move，当前绑定未改变。", null);
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
  private async createdSessionModel(worker: OmpWorker): Promise<string | null> {
    const catalog = await worker.models().catch(() => null);
    if (catalog === null || catalog.current === null) return null;
    return modelWithLevel(catalog.current, catalog.level);
  }
}
