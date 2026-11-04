/**
 * 维护单个任务的共享群说明事务，以及成员输入所需的临时 agent context。
 * `service.ts` 的 `runTask` 依次调用 `load` → `workerScope` → `prepare` → `complete`；
 * steering 的 context 写入独立 overlay，普通输入先暂存，OMP 确认消息进入模型上下文后才生效，
 * 避免覆盖 extension 同时写入的共享上下文 receipt。
 *
 * 模块不依赖 `service.ts`：store、transport、stateDir、bot 身份和本回合目标都由 `CoreService`
 * 以窄端口注入。一个 task 一个 `TurnContext`：`load()` 取共享快照，`prepare(sessionId,
 * agentContext)` 在绑定会话确认之后发布本回合 sidecar，`complete(sessionId, localOnly)` 结算
 * 共享说明并保留会话快照，供下一普通 turn 稳定复用。
 * 取消/中断/失败路径不调用它，本回合的 sidecar 留给同一 session 重放。
 */

import { createHash, randomUUID } from "node:crypto";
import { access, mkdir, readFile, readdir, rm, watch } from "node:fs/promises";
import { join } from "node:path";
import type { ChannelTransport, ChatTarget, ContextInjection, GroupContext, WorkerOptions } from "../contracts.ts";
import type { AgentContext } from "./agent-context.ts";
import type { ChannelStore } from "../store/index.ts";
import {
  FRESH_CONTEXT_READ_SCHEMA_VERSION,
  freshContextReadPaths,
  isContextSidecarObject,
  normalizeGroupContext,
  emptyGroupContext,
  preparedReceiptMatches,
  contextSidecarPath,
  readContextSidecar,
  writeBoundSession,
  readAgentContextOverlay,
  writeContextSidecar,
  writeAgentContextOverlay,
  writeFreshContextReadFile,
  type ContextSidecar,
  type FreshContextReadPaths,
  type StagedAgentContext,
  type FreshContextReadResponse,
} from "./context-sidecar.ts";

const FRESH_CONTEXT_READ_TIMEOUT_MS = 5_000;
const FRESH_CONTEXT_READ_POLL_MS = 20;
const FRESH_CONTEXT_REQUEST_SUFFIX = ".request.json";

function waitForSignal(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  const deferred = Promise.withResolvers<void>();
  const finish = (): void => {
    clearTimeout(timer);
    signal.removeEventListener("abort", finish);
    deferred.resolve();
  };
  const timer = setTimeout(finish, milliseconds);
  signal.addEventListener("abort", finish, { once: true });
  if (signal.aborted) finish();
  return deferred.promise;
}

function waitForAbortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason ?? new Error("fresh context read aborted"));
  const aborted = Promise.withResolvers<never>();
  const onAbort = (): void => aborted.reject(signal.reason ?? new Error("fresh context read aborted"));
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  return Promise.race([operation, aborted.promise]).finally(() => {
    signal.removeEventListener("abort", onAbort);
  });
}

async function waitForFile(path: string, signal: AbortSignal): Promise<boolean> {
  while (!signal.aborted) {
    try {
      await access(path);
      return true;
    } catch {
      await waitForSignal(FRESH_CONTEXT_READ_POLL_MS, signal);
    }
  }
  return false;
}
async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}


function sameTarget(left: ChatTarget, right: ChatTarget): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}


/**
 * What one turn may touch outside its own state: its shared-context ledger and read, plus bound
 * session and agent-context sidecar files keyed by the current bot/chat identity.
 */
export interface TurnContextDeps {
  readonly store: Pick<
    ChannelStore,
    "getContextLedger" | "prepareContextInjection" | "commitContextInjection" | "clearContextLedger"
  >;
  readonly transport: Pick<ChannelTransport, "getGroupContext">;
  readonly stateDir: string;
  readonly botUid: string;
  readonly target: ChatTarget;
  readonly cliReady: boolean;
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
  /** 成功 compaction 的 native preserveData，下一 task 可据此恢复同一完整快照。 */
  private compactedContextReceipt: ContextSidecar["compactedContext"];
  /** Fresh candidate awaiting proof of a committed native compaction entry. */
  private pendingCompaction: ContextSidecar["pendingCompaction"];

  constructor(private readonly deps: TurnContextDeps) {}

  /**
   * 读取本回合的群说明快照；私聊只使用该门面发布会话作用域，不触碰群说明 API。
   */
  async load(sessionId: string): Promise<GroupContext | null> {
    if (this.deps.target.channelType === 1) {
      this.snapshot = emptyGroupContext();
      this.hasSharedContext = false;
      return null;
    }
    const path = contextSidecarPath(this.sidecarRoot(), sessionId);
    let cached: ContextSidecar | undefined;
    try {
      await access(path);
      cached = await readContextSidecar(path);
      if (
        cached.botUid !== this.deps.botUid
        || cached.sessionId !== sessionId
        || !sameTarget(cached.target, this.deps.target)
      ) {
        throw new Error("群说明会话快照身份不匹配");
      }
    } catch (error) {
      if (!isContextSidecarObject(error) || error.code !== "ENOENT") throw error;
    }
    if (!cached) {
      this.compactedContextReceipt = undefined;
      this.pendingCompaction = undefined;
      this.deps.store.clearContextLedger(this.deps.botUid, this.deps.target, sessionId);
      this.snapshot = normalizeGroupContext(await this.deps.transport.getGroupContext(this.deps.target));
    } else {
      this.compactedContextReceipt = cached.compactedContext;
      this.pendingCompaction = cached.pendingCompaction;
      const compactedSnapshot = cached.compactedContext?.snapshot;
      let effectiveSnapshot = cached.snapshot;
      if (compactedSnapshot && compactedSnapshot.version >= cached.snapshot.version) {
        if (
          compactedSnapshot.version === cached.snapshot.version
          && (
            compactedSnapshot.present !== cached.snapshot.present
            || compactedSnapshot.content !== cached.snapshot.content
          )
        ) {
          throw new Error("同版本群说明 sidecar 与压缩快照内容不一致");
        }
        this.commitCompactedContext(sessionId, compactedSnapshot.version);
        effectiveSnapshot = compactedSnapshot;
      }
      this.snapshot = normalizeGroupContext(effectiveSnapshot);
    }
    this.hasSharedContext = true;
    return this.snapshot;
  }


  /** A committed native compaction is durable evidence that its fresh snapshot reached history. */
  private commitCompactedContext(sessionId: string, version: number): void {
    const existing = this.deps.store.getContextLedger(this.deps.botUid, this.deps.target, sessionId);
    if (existing && existing.version > version) throw new Error("群说明压缩快照版本回退");
    const ledger = !existing || existing.version < version
      ? this.deps.store.prepareContextInjection(this.deps.botUid, this.deps.target, sessionId, version)
      : existing;
    if (
      ledger.phase === "pending"
      && !this.deps.store.commitContextInjection(
        this.deps.botUid,
        this.deps.target,
        sessionId,
        version,
        ledger.attemptId,
      )
    ) {
      throw new Error("压缩已提交快照未能结算群说明台账");
    }
  }

  /** 组装群/子区 worker options 时的 scope。 */
  get workerScope(): NonNullable<WorkerOptions["contextScope"]> {
    return {
      sidecarRoot: this.sidecarRoot(),
      botUid: this.deps.botUid,
      target: { ...this.deps.target },
      cliReady: this.deps.cliReady,
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
      ...(this.compactedContextReceipt === undefined ? {} : { compactedContext: this.compactedContextReceipt }),
      ...(this.pendingCompaction === undefined ? {} : { pendingCompaction: this.pendingCompaction }),
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

  /**
   * Serve exact-turn fresh reads requested by the OMP extension during compaction.
   * The worker and host exchange private files under the current turn directory; no
   * listener or transport capability is exposed to the extension process.
   */
  async serveFreshContextReads(signal: AbortSignal): Promise<void> {
    if (!this.hasSharedContext) return;
    const sidecarPath = this.sidecarPath;
    const turnId = this.turnId;
    if (!sidecarPath || !turnId) return;

    let directory: string;
    try {
      directory = freshContextReadPaths(this.sidecarRoot(), turnId, randomUUID()).directory;
    } catch {
      console.error("[context] fresh compaction read bridge has an invalid turn identity");
      return;
    }

    const scanRequests = async (): Promise<void> => {
      const entries = await readdir(directory, { withFileTypes: true }).catch((error: unknown) => {
        if (isContextSidecarObject(error) && error.code === "ENOENT") return [];
        throw error;
      });
      const requests = entries
        .filter(entry => entry.isFile() && entry.name.endsWith(FRESH_CONTEXT_REQUEST_SUFFIX))
        .sort((left, right) => left.name.localeCompare(right.name));
      for (const entry of requests) {
        if (signal.aborted) break;
        const requestId = entry.name.slice(0, -FRESH_CONTEXT_REQUEST_SUFFIX.length);
        try {
          await this.respondToFreshContextRequest(
            freshContextReadPaths(this.sidecarRoot(), turnId, requestId),
            requestId,
            sidecarPath,
            turnId,
            signal,
          );
        } catch {
          if (!signal.aborted) {
            console.error("[context] fresh compaction read bridge could not serve a request");
          }
        }
      }
    };

    try {
      await mkdir(directory, { recursive: true });
      const watcher = watch(directory, { signal });
      await scanRequests();
      for await (const event of watcher) {
        if (signal.aborted) break;
        const filename = event.filename?.toString();
        if (filename === undefined || filename.endsWith(FRESH_CONTEXT_REQUEST_SUFFIX)) {
          await scanRequests();
        }
      }
    } catch {
      if (!signal.aborted) console.error("[context] fresh compaction read bridge stopped unexpectedly");
    } finally {
      await rm(directory, { recursive: true, force: true }).catch(() => {});
    }
  }

  private async respondToFreshContextRequest(
    paths: FreshContextReadPaths,
    requestId: string,
    sidecarPath: string,
    turnId: string,
    signal: AbortSignal,
  ): Promise<void> {
    const sidecar = await this.currentTurnSidecar(sidecarPath);
    const identity = {
      schemaVersion: FRESH_CONTEXT_READ_SCHEMA_VERSION,
      requestId,
      botUid: this.deps.botUid,
      target: { ...this.deps.target },
      sessionId: sidecar.sessionId,
      turnId,
    } as const;
    const fail = async (error: string): Promise<void> => {
      await writeFreshContextReadFile(paths.response, { ...identity, error } satisfies FreshContextReadResponse);
    };

    try {
      const request = JSON.parse(await readFile(paths.request, "utf8")) as unknown;
      if (
        !isContextSidecarObject(request)
        || request.schemaVersion !== FRESH_CONTEXT_READ_SCHEMA_VERSION
        || request.requestId !== requestId
        || request.botUid !== identity.botUid
        || request.sessionId !== identity.sessionId
        || request.turnId !== identity.turnId
        || !isContextSidecarObject(request.target)
        || request.target.channelType !== identity.target.channelType
        || request.target.channelId !== identity.target.channelId
      ) {
        await fail("fresh context read identity mismatch");
        return;
      }
    } catch {
      if (!(await fileExists(paths.cancel))) await fail("fresh context read request is unavailable");
      return;
    }

    const requestController = new AbortController();
    const watcherController = new AbortController();
    const timeoutSignal = AbortSignal.timeout(FRESH_CONTEXT_READ_TIMEOUT_MS);
    const readSignal = AbortSignal.any([signal, requestController.signal, timeoutSignal]);
    let cancelled = false;
    const cancellationWatch = waitForFile(
      paths.cancel,
      AbortSignal.any([signal, watcherController.signal]),
    ).then(found => {
      if (found && !signal.aborted) {
        cancelled = true;
        requestController.abort(new Error("fresh context read cancelled"));
      }
    });

    try {
      if (await fileExists(paths.cancel)) return;
      const snapshot = normalizeGroupContext(await waitForAbortable(
        Promise.resolve().then(() => this.deps.transport.getGroupContext(this.deps.target, readSignal)),
        readSignal,
      ));
      if (cancelled || signal.aborted) return;
      const current = await this.currentTurnSidecar(sidecarPath);
      if (current.turnId !== turnId || current.sessionId !== identity.sessionId) {
        await fail("fresh context read turn changed");
        return;
      }
      if (await fileExists(paths.cancel)) return;
      await writeFreshContextReadFile(paths.response, { ...identity, snapshot } satisfies FreshContextReadResponse);
    } catch {
      if (!cancelled && !signal.aborted && !(await fileExists(paths.cancel))) {
        console.error("[context] fresh group context read failed; compaction will be cancelled");
        await fail("fresh group context read failed").catch(() => {});
      }
    } finally {
      watcherController.abort();
      requestController.abort();
      await cancellationWatch;
      await rm(paths.request, { force: true }).catch(() => {});
    }
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
   * CAS。结算后保留会话 snapshot sidecar，供同一 binding 的后续普通 turn 稳定复用。
   * `sessionId` 必须是当时 worker 的会话；本地命令回合不结算。取消/失败路径不调用本方法，
   * pending sidecar 留给同一 session 重放。
   */
  async complete(sessionId: string, localOnly: boolean): Promise<void> {
    if (this.needsCommit && !localOnly) {
      const sidecarPath = this.sidecarPath;
      const injection = this.injection;
      if (!sidecarPath || !injection) {
        throw new Error("群说明注入状态缺失，未提交");
      }
      const sidecar = await readContextSidecar(sidecarPath);
      const compacted = sidecar.compactedContext;
      if (compacted && compacted.snapshot.version >= injection.snapshot.version) {
        if (
          sidecar.marker.phase !== "pending"
          || sidecar.botUid !== this.deps.botUid
          || sidecar.sessionId !== sessionId
          || !sameTarget(sidecar.target, this.deps.target)
          || sidecar.turnId !== this.turnId
        ) {
          throw new Error("压缩快照收据与当前会话不匹配");
        }
        this.commitCompactedContext(sessionId, compacted.snapshot.version);
      } else {
        if (
          sidecar.marker.phase !== "pending"
          || sidecar.botUid !== this.deps.botUid
          || sidecar.sessionId !== sessionId
          || !sameTarget(sidecar.target, this.deps.target)
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
    }
    if (this.sidecarPath) {
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
