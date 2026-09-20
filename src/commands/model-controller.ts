/**
 * `/switch` 的命令路径：聊天的模型命令语义、一次性探测 worker 的生命周期与回执文案集中在这里。
 * 选择器的解析（`provider/id`、模型 id/子串、`@角色`、`:level`）由 OMP 自己的 `/switch` 完成，
 * 目录渲染与文本边界在 `./model-switch.ts`。
 *
 * 模块不依赖 `service.ts`：聊天目录（`probeDirectory`）、运行中会话（`liveWorker`）、
 * 出站队列（`enqueueText`）与被阻塞目录（`blockDirectory`）都由 `CoreService` 以端口注入，
 * 由它在构造函数里创建一次本控制器。
 */

import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ChatTarget,
  InboundMessage,
  OmpWorker,
  WorkerFactory,
  WorkerModelCatalog,
  WorkerModelSwitch,
  WorkerProcess,
} from "../contracts.ts";
import { boundedSelectorText, formatModelCatalog, modelWithLevel } from "./model-switch.ts";
import type { ChannelStore } from "../store/index.ts";

const SWITCH_PROBE_FAILED_TEXT = "无法从 OMP 读取模型信息，请稍后重试。";
const SWITCH_CWD_UNAVAILABLE_TEXT = "无法解析该聊天的工作目录，/switch 暂不可用。";

/** One bounded line for a selector OMP could not resolve. */
function notFoundModelText(selector: string): string {
  return `未找到模型 "${boundedSelectorText(selector)}"；发送 /switch 查看可用模型列表。`;
}

/** A `/switch` probe whose chat directory cannot be resolved; reported apart from worker failures. */
class SwitchDirectoryError extends Error {
  constructor() {
    super("switch probe directory unavailable");
    this.name = "SwitchDirectoryError";
  }
}

/**
 * What the controller may touch outside its own state: the persisted chat settings and worker
 * ledger, the worker factory that backs a probe, the bot identity those rows are keyed by, the
 * live session and chat directory as `CoreService` already resolves them, and the chat's outbox.
 */
export interface ModelSwitchControllerDeps {
  readonly store: ChannelStore;
  readonly workerFactory: WorkerFactory;
  /** The bot identity chat settings are keyed by; a getter because it is learned after start. */
  readonly botUid: () => string;
  /** The worker of the chat's live run, while its OMP session is still up. */
  readonly liveWorker: (target: ChatTarget) => OmpWorker | undefined;
  /**
   * The cwd a probe worker runs in — the chat's own directory — or `null` when nothing usable
   * resolves. Resolution (bindings, isolated workspaces, the configured default) stays in
   * `CoreService`.
   */
  readonly probeDirectory: (target: ChatTarget) => Promise<string | null>;
  /** Marks a cwd unusable after a worker there could not be confirmed stopped. */
  readonly blockDirectory: (cwd: string) => void;
  /** Queues one text message for the chat; the command path awaits it as it always has. */
  readonly enqueueText: (target: ChatTarget, text: string, taskId: string | null) => void;
}

export class ModelSwitchController {
  constructor(private readonly deps: ModelSwitchControllerDeps) {}

  /**
   * `/switch [选择器]`: no argument reports the model this chat is on plus every model OMP offers;
   * `default` drops the chat's model; anything else is handed to OMP's own `/switch`, which
   * resolves `provider/id`, a model id or substring, `@role`, and an optional `:level` suffix
   * against its live catalog. The resolved `provider/id:level` is stored for the chat, so workers
   * started later (tasks, `/new`, `/reset`, `/move`) get it as `--model` — and when a turn is
   * already running, the same dispatch switches that session in place, because OMP consumes the
   * command locally instead of starting an agent turn.
   */
  async handleSwitch(message: InboundMessage, argument: string): Promise<void> {
    const botUid = this.deps.botUid();
    const requested = argument.trim();
    const live = this.deps.liveWorker(message);
    if (requested.length === 0) {
      let catalog: WorkerModelCatalog;
      try {
        catalog = live
          ? await live.models()
          : await this.withProbeWorker(message, worker => worker.models());
      } catch (error) {
        await this.replySwitchFailure(message, error);
        return;
      }
      await this.deps.enqueueText(message, this.describeChatModel(message, live !== undefined, catalog), null);
      return;
    }
    if (requested.toLowerCase() === "default" && !requested.includes("/")) {
      await this.resetChatModel(message);
      return;
    }
    // A selector is a single token; catching a stray one here keeps a typo from starting a worker.
    if (/\s/u.test(requested)) {
      await this.deps.enqueueText(message, notFoundModelText(requested), null);
      return;
    }
    let outcome: WorkerModelSwitch;
    try {
      outcome = live
        ? await live.switchModel(requested)
        : await this.withProbeWorker(message, worker => worker.switchModel(requested));
    } catch (error) {
      await this.replySwitchFailure(message, error);
      return;
    }
    if (!outcome.ok) {
      await this.deps.enqueueText(message, notFoundModelText(requested), null);
      return;
    }
    const model = modelWithLevel(outcome.model, outcome.level);
    this.deps.store.setChatModel(botUid, message, model);
    await this.deps.enqueueText(
      message,
      live === undefined
        ? `已切换本聊天模型为 ${model}，从下一回合开始生效。`
        : `已切换本聊天模型为 ${model}（运行中会话已同步切换）。`,
      null,
    );
  }

  /**
   * The empty `/switch` answer: the live session's model while a turn runs (that is the model in
   * use), otherwise this chat's stored model, otherwise the model OMP would start a fresh session
   * with — followed by every model OMP currently offers.
   */
  private describeChatModel(target: ChatTarget, running: boolean, catalog: WorkerModelCatalog): string {
    const stored = this.deps.store.getChatModel(this.deps.botUid(), target);
    const live = catalog.current === null ? null : modelWithLevel(catalog.current, catalog.level);
    if (running && live !== null) {
      return formatModelCatalog({
        current: live,
        label:
          stored === null
            ? "运行中会话"
            : stored === live
              ? "本聊天指定"
              : `运行中会话；本聊天模型 ${stored}`,
        models: catalog.available,
      });
    }
    if (stored !== null) {
      return formatModelCatalog({ current: stored, label: "本聊天指定", models: catalog.available });
    }
    return formatModelCatalog({ current: live, label: "OMP 默认", models: catalog.available });
  }

  /**
   * `/switch default`: drop this chat's model so later workers start without `--model` and follow
   * OMP's own default selection. A running session is left alone — `/switch @default` is the
   * native way to move it.
   */
  private async resetChatModel(target: ChatTarget): Promise<void> {
    const previous = this.deps.store.getChatModel(this.deps.botUid(), target);
    if (previous === null) {
      await this.deps.enqueueText(target, "本聊天未指定模型，继续跟随 OMP 默认。", null);
      return;
    }
    this.deps.store.clearChatModel(this.deps.botUid(), target);
    let current: string | null = null;
    if (this.deps.liveWorker(target) === undefined) {
      const catalog = await this.withProbeWorker(target, worker => worker.models()).catch(() => null);
      if (catalog !== null && catalog.current !== null) current = modelWithLevel(catalog.current, catalog.level);
    }
    await this.deps.enqueueText(
      target,
      `已恢复跟随 OMP 默认模型${current === null ? "" : `（当前为 ${current}）`}。`,
      null,
    );
  }

  /**
   * One throwaway worker serves a `/switch` probe: fresh (no `--resume`), no prompt of our own
   * beyond the switch dispatch, and a `mkdtemp` session directory it never shares with a chat
   * session, so it neither reads nor writes a running turn's transcript. It runs in the chat's own
   * directory — same OMP profile, config, and credentials as that chat's turns — is recorded in
   * the worker ledger like `/new`'s candidate worker, and is removed afterwards.
   */
  private async withProbeWorker<T>(target: ChatTarget, run: (worker: OmpWorker) => Promise<T>): Promise<T> {
    const cwd = await this.deps.probeDirectory(target);
    if (cwd === null) throw new SwitchDirectoryError();
    const sessionDir = await mkdtemp(join(tmpdir(), "omp-channel-octo-switch-"));
    const sessionId = randomUUID();
    const ledgerId = `switch-${sessionId}`;
    let spawned: WorkerProcess | undefined;
    let stopped = false;
    try {
      const candidate = await this.deps.workerFactory.create({
        cwd,
        sessionDir,
        onSpawn: (processInfo) => {
          spawned = processInfo;
          this.deps.store.recordWorker({
            id: ledgerId,
            botUid: this.deps.botUid(),
            sessionId,
            sessionFile: join(sessionDir, `${sessionId}.jsonl`),
            pid: processInfo.pid,
            pgid: processInfo.pgid,
            identity: processInfo.identity,
            cwd: processInfo.cwd,
            moveTargetCwd: null,
            state: "running",
          });
        },
      });
      try {
        return await run(candidate);
      } finally {
        stopped = await candidate.stop().catch(() => false);
        if (stopped) this.deps.store.markWorkerStopped(ledgerId);
        else this.deps.blockDirectory(cwd);
      }
    } catch (error) {
      if (!stopped && spawned) {
        const verified = await this.deps.workerFactory.recover(spawned).catch(() => false);
        if (verified) this.deps.store.markWorkerStopped(ledgerId);
        else this.deps.blockDirectory(cwd);
      }
      throw error;
    } finally {
      await rm(sessionDir, { recursive: true, force: true }).catch(() => {});
    }
  }

  /** A probe failure as one bounded line: no working directory, or no usable OMP answer. */
  private async replySwitchFailure(target: ChatTarget, error: unknown): Promise<void> {
    await this.deps.enqueueText(
      target,
      error instanceof SwitchDirectoryError ? SWITCH_CWD_UNAVAILABLE_TEXT : SWITCH_PROBE_FAILED_TEXT,
      null,
    );
  }
}
