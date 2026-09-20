/**
 * 单个任务的群说明回合事务：快照读取、injection ledger 的 prepare/commit、bound session 与
 * 本回合 sidecar 的发布/清理都在这里。`service.ts` 的 `runTask` 只按顺序调用
 * `load` → `workerScope` → `prepare` → `complete`，不再持有 sidecar 路径、ledger 决策或
 * prepared receipt 的校验细节。
 *
 * 模块不依赖 `service.ts`：context ledger 的三条 store 读写、群说明读取、stateDir、bot 身份与
 * 本回合目标都由 `CoreService` 以窄端口注入。一个 task 一个 `TurnContext`：`load()` 取快照，
 * `prepare(sessionId)` 在绑定会话确认之后发布本回合 sidecar 并返回注入内容，
 * `complete(sessionId, localOnly)` 只在任务普通成功分支结算（校验 prepared receipt 与 marker
 * CAS，然后清理 sidecar）；取消/中断/失败路径不调用它，本回合的 sidecar 保留给下一次重放。
 */

import { createHash } from "node:crypto";
import { join } from "node:path";
import type { ChannelTransport, ChatTarget, ContextInjection, GroupContext, WorkerOptions } from "./contracts.ts";
import {
  normalizeGroupContext,
  preparedReceiptMatches,
  readContextSidecar,
  removeContextSidecar,
  writeBoundSession,
  writeContextSidecar,
  type ContextSidecar,
} from "./context-sidecar.ts";
import type { ChannelStore } from "./store.ts";

/**
 * What one turn may touch outside its own state: the context ledger rows, one group-context read,
 * and the identity/paths those rows are keyed by.
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
  /** 本回合的注入内容：`prepare` 的产物，也是 `promptContext` 的来源。 */
  private injection: ContextInjection | undefined;
  /** 本回合 sidecar 的精确路径；未发布或已清理时为 undefined。 */
  private sidecarPath: string | undefined;
  /** 成功分支是否需要结算 ledger：`injectionDecision` 为 unchanged 时为 false。 */
  private needsCommit = false;
  /** scope 侧车目录：同一个回合里 scope、bound session 与 sidecar 复用同一份路径。 */
  private root: string | undefined;

  constructor(private readonly deps: TurnContextDeps) {}

  /**
   * 读取本回合的群说明快照。这里失败即启动阶段的读取失败，调用方在它返回之后才把
   * `startupPhase` 推进到 startup。
   */
  async load(): Promise<GroupContext | null> {
    this.snapshot = normalizeGroupContext(await this.deps.transport.getGroupContext(this.deps.target));
    return this.snapshot;
  }

  /** 组装群/子区 worker options 时的 scope。 */
  get workerScope(): WorkerOptions["contextScope"] {
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
  async prepare(sessionId: string): Promise<ContextInjection | undefined> {
    const snapshot = this.snapshot;
    if (!snapshot) throw new Error("群说明快照缺失");
    const target = this.deps.target;
    const botUid = this.deps.botUid;
    const existingLedger = this.deps.store.getContextLedger(botUid, target, sessionId);
    if (existingLedger && existingLedger.version > snapshot.version) {
      throw new Error("群说明版本回退，未启动任务");
    }
    let ledger = existingLedger;
    if (!ledger || ledger.version < snapshot.version) {
      ledger = this.deps.store.prepareContextInjection(botUid, target, sessionId, snapshot.version);
    }
    const previousVersion = ledger.previousVersion;
    const injectionDecision: ContextInjection["injectionDecision"] = ledger.phase === "committed"
      && ledger.version === snapshot.version
      ? "unchanged"
      : previousVersion === null
        ? "initial"
        : "version-change";
    const explanation = injectionDecision === "version-change" && previousVersion !== null
      ? `群说明版本从 ${previousVersion} 更新为 ${snapshot.version}，注入最新完整快照。`
      : injectionDecision === "initial"
        ? "首次注入当前群/线程共享说明完整快照。"
        : `群说明版本 ${snapshot.version} 未变化，本轮不重复注入正文。`;
    const injection: ContextInjection = {
      snapshot: { ...snapshot },
      injectionDecision,
      attemptId: ledger.attemptId,
      explanation,
    };
    this.injection = injection;
    this.needsCommit = injectionDecision !== "unchanged";
    const sidecar: ContextSidecar = {
      schemaVersion: 1,
      botUid,
      target: { ...target },
      sessionId,
      snapshot: { ...snapshot },
      injectionDecision,
      attemptId: ledger.attemptId,
      marker: {
        phase: ledger.phase,
        botUid,
        target: { ...target },
        sessionId,
        version: snapshot.version,
        attemptId: ledger.attemptId,
      },
      ...(previousVersion === null ? {} : { previousVersion }),
    };
    // The scope publishes the session this turn belongs to; it stays as the
    // scope's current session so sub-sessions OMP opens in the same worker
    // are never mistaken for a channel turn.
    await writeBoundSession(this.sidecarRoot(), {
      botUid,
      target: { ...target },
      sessionId,
    });
    this.sidecarPath = await writeContextSidecar(this.sidecarRoot(), sidecar);
    return injection;
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
      await removeContextSidecar(this.sidecarPath).catch(() => {});
      this.sidecarPath = undefined;
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
