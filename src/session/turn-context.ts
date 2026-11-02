/**
 * 维护单个任务的共享群说明事务，以及成员/固定操作指引的临时 agent context。
 * `service.ts` 的 `runTask` 依次调用 `load` → `workerScope` → `prepare` → `complete`；
 * steering 的 context 写入独立 overlay，普通输入先暂存，OMP 确认消息进入模型上下文后才生效，
 * 避免覆盖 extension 同时写入的共享上下文 receipt。
 *
 * 模块不依赖 `service.ts`：store、transport、stateDir、bot 身份和本回合目标都由 `CoreService`
 * 以窄端口注入。一个 task 一个 `TurnContext`：`load()` 取共享快照，`prepare(sessionId,
 * agentContext)` 在绑定会话确认之后发布本回合 sidecar，`complete(sessionId, localOnly)` 只在
 * 任务普通成功分支结算共享说明（校验 prepared receipt 与 marker CAS，然后清理 sidecar）。
 * 取消/中断/失败路径不调用它，本回合的 sidecar 保留给下一次重放。
 */

import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import type { ChannelTransport, ChatTarget, ContextInjection, GroupContext, WorkerOptions } from "../contracts.ts";
import {
  normalizeGroupContext,
  emptyGroupContext,
  preparedReceiptMatches,
  readContextSidecar,
  readAgentContextOverlay,
  removeContextSidecar,
  writeBoundSession,
  writeContextSidecar,
  writeAgentContextOverlay,
  type ContextSidecar,
  type StagedAgentContext,
} from "./context-sidecar.ts";
import type { AgentContext } from "./agent-context.ts";
import type { ChannelStore } from "../store/index.ts";

/**
 * What one turn may touch outside its own state: its shared-context ledger and read, plus bound
 * session and agent-context sidecar files keyed by the current bot/chat identity.
 */
export interface TurnContextDeps {
  readonly store: Pick<
    ChannelStore,
    "getContextLedger" | "prepareContextInjection" | "commitContextInjection"
  >;
  readonly transport: Pick<ChannelTransport, "getGroupContext">;
  readonly stateDir: string;
  readonly botUid: string;
  readonly target: ChatTarget;
}

export class TurnContext {
  /** `load()` 取到的完整群/子区快照。 */
  private snapshot: GroupContext | null = null;
  /** 私聊不读取或持久化共享群说明。 */
  private hasSharedContext = false;
  /** 本回合的注入内容：`prepare` 的产物，也是 `promptContext` 的来源。 */
  private injection: ContextInjection | undefined;
  /** 本回合 sidecar 的精确路径；未发布或已清理时为 undefined。 */
  private sidecarPath: string | undefined;
  /** Distinguishes this task's overlay from a late writer from an older turn in the same session. */
  private turnId: string | undefined;
  /** 成功分支是否需要结算 ledger：`injectionDecision` 为 unchanged 时为 false。 */
  private needsCommit = false;
  /** scope 侧车目录：同一个回合里 scope、bound session 与 sidecar 复用同一份路径。 */
  private root: string | undefined;

  constructor(private readonly deps: TurnContextDeps) {}

  /**
   * 读取本回合的群说明快照；私聊只使用该门面发布会话作用域，不触碰群说明 API。
   */
  async load(): Promise<GroupContext | null> {
    if (this.deps.target.channelType === 1) {
      this.snapshot = emptyGroupContext();
      this.hasSharedContext = false;
      return null;
    }
    this.snapshot = normalizeGroupContext(await this.deps.transport.getGroupContext(this.deps.target));
    this.hasSharedContext = true;
    return this.snapshot;
  }

  /** 组装群/子区 worker options 时的 scope。 */
  get workerScope(): NonNullable<WorkerOptions["contextScope"]> {
    return {
      sidecarRoot: this.sidecarRoot(),
      botUid: this.deps.botUid,
      target: { ...this.deps.target },
    };
  }

  /**
   * 在 `verify` + `updateBindingSession` 之后发布本回合：判定注入方式（首次/版本变更/不重复
   * 注入）、写 bound session 与本回合 sidecar，并返回交给 `worker.prompt` 的注入内容。
   * 群说明版本回退直接抛错，任务不启动。
   */
  async prepare(sessionId: string, agentContext: AgentContext): Promise<ContextInjection | undefined> {
    const snapshot = this.snapshot ?? emptyGroupContext();
    const target = this.deps.target;
    const botUid = this.deps.botUid;
    let injection: ContextInjection | undefined;
    let injectionDecision: ContextInjection["injectionDecision"] = "unchanged";
    let attemptId: string = randomUUID();
    const turnId = randomUUID();
    let phase: "pending" | "committed" = "committed";
    let previousVersion: number | undefined;

    if (this.hasSharedContext) {
      const existingLedger = this.deps.store.getContextLedger(botUid, target, sessionId);
      if (existingLedger && existingLedger.version > snapshot.version) {
        throw new Error("群说明版本回退，未启动任务");
      }
      let ledger = existingLedger;
      if (!ledger || ledger.version < snapshot.version) {
        ledger = this.deps.store.prepareContextInjection(botUid, target, sessionId, snapshot.version);
      }
      previousVersion = ledger.previousVersion ?? undefined;
      injectionDecision = ledger.phase === "committed" && ledger.version === snapshot.version
        ? "unchanged"
        : previousVersion === undefined
          ? "initial"
          : "version-change";
      const explanation = injectionDecision === "version-change" && previousVersion !== undefined
        ? `群说明版本从 ${previousVersion} 更新为 ${snapshot.version}，注入最新完整快照。`
        : injectionDecision === "initial"
          ? "首次注入当前群/线程共享说明完整快照。"
          : `群说明版本 ${snapshot.version} 未变化，本轮不重复注入正文。`;
      injection = {
        snapshot: { ...snapshot },
        injectionDecision,
        attemptId: ledger.attemptId,
        explanation,
      };
      attemptId = ledger.attemptId;
      phase = ledger.phase;
      this.injection = injection;
      this.needsCommit = injectionDecision !== "unchanged";
    }

    const sidecar: ContextSidecar = {
      schemaVersion: 1,
      botUid,
      target: { ...target },
      turnId,
      sessionId,
      snapshot: { ...snapshot },
      injectionDecision,
      attemptId,
      marker: {
        phase,
        botUid,
        target: { ...target },
        sessionId,
        version: snapshot.version,
        attemptId,
      },
      ...(previousVersion === undefined ? {} : { previousVersion }),
      agentContext,
    };
    // The scope publishes the session this turn belongs to before its first model request.
    await writeBoundSession(this.sidecarRoot(), {
      botUid,
      target: { ...target },
      sessionId,
    });
    this.sidecarPath = await writeContextSidecar(this.sidecarRoot(), sidecar);
    this.turnId = turnId;
    return injection;
  }

  /** Replace only the current transient agent context; receipt writes live in a separate file. */
  async updateAgentContext(agentContext: AgentContext): Promise<AgentContext | undefined> {
    const sidecarPath = this.sidecarPath;
    if (!sidecarPath) throw new Error("本回合 agent context 尚未发布");
    const sidecar = await this.currentTurnSidecar(sidecarPath);
    const overlay = await readAgentContextOverlay(sidecarPath, sidecar);
    await writeAgentContextOverlay(sidecarPath, sidecar, agentContext, overlay?.pending ?? []);
    return overlay?.agentContext ?? sidecar.agentContext;
  }

  /** Stage a steering snapshot; OMP exposes it only after its user message appears in model context. */
  async stageAgentContext(
    messageId: string,
    text: string,
    agentContext: AgentContext,
  ): Promise<AgentContext | undefined> {
    const sidecarPath = this.sidecarPath;
    if (!sidecarPath) throw new Error("本回合 agent context 尚未发布");
    const sidecar = await this.currentTurnSidecar(sidecarPath);
    const overlay = await readAgentContextOverlay(sidecarPath, sidecar);
    const previous = overlay?.agentContext ?? sidecar.agentContext;
    const pending: StagedAgentContext[] = [
      ...(overlay?.pending ?? []),
      { messageId, text, agentContext },
    ];
    await writeAgentContextOverlay(sidecarPath, sidecar, previous ?? agentContext, pending);
    return previous;
  }

  /** Remove a staged input that OMP refused; other accepted/pending inputs remain intact. */
  async discardStagedAgentContext(messageId: string): Promise<void> {
    const sidecarPath = this.sidecarPath;
    if (!sidecarPath) return;
    const sidecar = await this.currentTurnSidecar(sidecarPath);
    const overlay = await readAgentContextOverlay(sidecarPath, sidecar);
    if (!overlay) return;
    const pending = overlay.pending.filter((entry) => entry.messageId !== messageId);
    if (pending.length === overlay.pending.length) return;
    await writeAgentContextOverlay(sidecarPath, sidecar, overlay.agentContext, pending);
  }

  private async currentTurnSidecar(sidecarPath: string): Promise<ContextSidecar> {
    const sidecar = await readContextSidecar(sidecarPath);
    if (
      !this.turnId
      || sidecar.turnId !== this.turnId
      || sidecar.botUid !== this.deps.botUid
      || sidecar.target.channelType !== this.deps.target.channelType
      || sidecar.target.channelId !== this.deps.target.channelId
    ) {
      throw new Error("本回合 agent context 身份不匹配");
    }
    return sidecar;
  }

  /**
   * 任务普通成功分支的结算：`needsCommit && !localOnly` 时校验 prepared receipt 与 marker
   * CAS，随后无论是否提交都清理本回合的 sidecar。`sessionId` 必须是当时 worker 的会话，不是
   * `prepare` 缓存下来的那个；本地命令回合（无 agent 生成，不会写 prepared receipt）跳过提交
   * 但仍清理 sidecar。取消/中断/失败路径不调用本方法，sidecar 保留待重放。
   */
  async complete(sessionId: string, localOnly: boolean): Promise<void> {
    if (this.needsCommit && !localOnly) {
      const sidecarPath = this.sidecarPath;
      const injection = this.injection;
      if (!sidecarPath || !injection) {
        throw new Error("群说明注入状态缺失，未提交");
      }
      const sidecar = await readContextSidecar(sidecarPath);
      if (
        sidecar.marker.phase !== "pending"
        || sidecar.botUid !== this.deps.botUid
        || sidecar.sessionId !== sessionId
        || sidecar.target.channelType !== this.deps.target.channelType
        || sidecar.target.channelId !== this.deps.target.channelId
        || sidecar.snapshot.version !== injection.snapshot.version
        || sidecar.turnId !== this.turnId
        || sidecar.attemptId !== injection.attemptId
        || !sidecar.prepared
        || !preparedReceiptMatches(sidecar, sidecar.prepared)
      ) {
        throw new Error("群说明注入未获得匹配的 prepared receipt，未提交");
      }
      if (!this.deps.store.commitContextInjection(
        this.deps.botUid,
        this.deps.target,
        sessionId,
        injection.snapshot.version,
        injection.attemptId,
      )) {
        throw new Error("群说明 marker CAS 提交失败，保留待重放状态");
      }
    }
    if (this.sidecarPath) {
      const sidecar = await readContextSidecar(this.sidecarPath).catch(() => undefined);
      if (sidecar?.turnId === this.turnId) {
        await removeContextSidecar(this.sidecarPath).catch(() => {});
      }
      this.sidecarPath = undefined;
      this.turnId = undefined;
    }
  }

  /** 一个 scope 一个目录：bot 与目标摘要成稳定的路径，文件名不含用户输入。 */
  private sidecarRoot(): string {
    this.root ??= join(
      this.deps.stateDir,
      "context-sidecars",
      createHash("sha256")
        .update(
          `${this.deps.botUid}\u0000${this.deps.target.channelType}\u0000${this.deps.target.channelId}`,
          "utf8",
        )
        .digest("hex"),
    );
    return this.root;
  }
}
