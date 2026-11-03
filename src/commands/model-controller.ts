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
import type { CommandCardView } from "../cards/command-card.ts";
import type { ConfigMenuPort } from "./command-cards.ts";
import type {
  ChatTarget,
  InboundMessage,
  WorkerFactory,
  WorkerModelCatalog,
  WorkerModelSwitch,
} from "../contracts.ts";
import {
  boundedSelectorText,
  formatModelCatalog,
  modelCatalogCurrent,
  modelPolicyDescription,
  modelWithLevel,
  switchCardChoices,
  type ModelSelection,
} from "./model-switch.ts";
import type { ChannelStore } from "../store/index.ts";
import type { WorkerSession } from "../runtime/worker-lease.ts";
import { openTrackedWorker } from "../runtime/worker-lease.ts";

const SWITCH_PROBE_FAILED_TEXT = "无法从 OMP 读取模型信息，请稍后重试。";
const SWITCH_CWD_UNAVAILABLE_TEXT = "无法解析该聊天的工作目录，/switch 暂不可用。";

/** One bounded line for a selector OMP could not resolve. */
function notFoundModelText(selector: string): string {
  return `未找到模型 "${boundedSelectorText(selector)}"；发送 /switch 查看可用模型列表。`;
}
/** A thread-default selector OMP could not resolve. */
function notFoundThreadModelText(selector: string): string {
  return `未找到模型 "${boundedSelectorText(selector)}"；请使用 /thread model 查看本群默认。`;
}

function threadModelFailureText(error: unknown): string {
  return error instanceof SwitchDirectoryError
    ? "无法解析该主群的工作目录，/thread model 暂不可用。"
    : SWITCH_PROBE_FAILED_TEXT;
}

const THREAD_MODEL_CHAT_ONLY_TEXT = "/thread model 仅可在 owner 的主群中使用，不支持 DM 或子区。";

/** A `/switch` probe whose chat directory cannot be resolved; reported apart from worker failures. */
class SwitchDirectoryError extends Error {
  constructor() {
    super("switch probe directory unavailable");
    this.name = "SwitchDirectoryError";
  }
}

function switchFailureText(error: unknown): string {
  return error instanceof SwitchDirectoryError ? SWITCH_CWD_UNAVAILABLE_TEXT : SWITCH_PROBE_FAILED_TEXT;
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
  readonly liveWorker: (target: ChatTarget) => WorkerSession | undefined;
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
  /**
   * 配置卡片菜单：无参数命令把回执渲染成一张可选卡片，卡片不可用时按同一段文本回退；非空
   * 命令让旧卡片失效并按到达顺序落地。生命周期在 `./command-cards.ts`，这里只调用窄面。
   */
  readonly menu: ConfigMenuPort;
}

export class ModelSwitchController {
  constructor(private readonly deps: ModelSwitchControllerDeps) {}

  /**
   * `/switch [选择器]`: no argument reports OMP's observed current model separately from the
   * policy for the next worker and every model OMP offers. `default` drops the chat's model;
   * anything else is handed to OMP's own `/switch`, which resolves `provider/id`, a model id or
   * substring, `@role`, and an optional `:level` suffix against its live catalog. The resolved
   * `provider/id:level` is stored for the chat, so workers started later receive it as `--model`.
   * When a turn is already running, OMP consumes the command locally and switches that session.
   */
  async handleSwitch(message: InboundMessage, argument: string): Promise<void> {
    const requested = argument.trim();
    if (requested.length === 0) {
      // 先拿到代次票据再查询：这次探测期间被更新的同类命令会让它作废，迟到的目录不得发布新卡。
      const ticket = this.deps.menu.begin(message, "switch");
      try {
        const live = this.deps.liveWorker(message);
        let catalog: WorkerModelCatalog;
        try {
          catalog = live
            ? await live.models()
            : await this.withProbeWorker(message, worker => worker.models());
        } catch (error) {
          if (ticket.current()) await this.deps.enqueueText(message, switchFailureText(error), null);
          return;
        }
        await this.deps.menu.open(message, this.switchMenuView(message, live !== undefined, catalog), ticket);
      } finally {
        ticket.release();
      }
      return;
    }
    // 非空命令先让本聊天旧的模型卡片失效，再按到达顺序应用：卡片点击走同一段应用逻辑，
    // 因此更慢的旧探测不会覆盖更新的选择。
    this.deps.menu.invalidate(message, "switch");
    const receipt = await this.deps.menu.serialize("switch", message, () => this.applySwitch(message, requested));
    await this.deps.enqueueText(message, receipt, null);
  }

  /** Owner's `/thread model` control path; it never uses or changes the parent group's live worker. */
  async handleThreadModel(target: InboundMessage, argument: string): Promise<void> {
    if (target.channelType !== 2) {
      await this.deps.enqueueText(target, THREAD_MODEL_CHAT_ONLY_TEXT, null);
      return;
    }
    const requested = argument.trim();
    const receipt = await this.deps.menu.serialize("switch", target, async () => {
      const botUid = this.deps.botUid();
      if (requested.length === 0) {
        const model = this.deps.store.getThreadModel(botUid, target.channelId);
        return `本群子区默认模型：${model ?? "不指定（由 OMP 自行决定）"}。`;
      }
      if (requested.toLowerCase() === "clear") {
        this.deps.store.clearThreadModel(botUid, target.channelId);
        return "已清除本群子区默认模型；无聊天显式覆盖的子区下次启动由 OMP 自行决定，运行中会话保持原模型。";
      }
      if (/\s/u.test(requested)) return notFoundThreadModelText(requested);
      let outcome: WorkerModelSwitch;
      try {
        outcome = await this.withProbeWorker(target, worker => worker.switchModel(requested));
      } catch (error) {
        return threadModelFailureText(error);
      }
      if (!outcome.ok) return notFoundThreadModelText(requested);
      const model = modelWithLevel(outcome.model, outcome.level);
      this.deps.store.setThreadModel(botUid, target.channelId, model);
      return `已设置本群子区默认模型为 ${model}；无聊天显式覆盖的子区从下次 worker 启动生效，运行中会话保持原模型。`;
    });
    await this.deps.enqueueText(target, receipt, null);
  }

  /**
   * Apply one `/switch` request — a selector, or `default` — and return the receipt verbatim. The
   * text command and a card click share this path: both go through OMP's own resolution and persist
   * the same resolved value. Every failure converges on the existing single-line Chinese text and
   * changes no state, so a card click can never claim a switch OMP refused.
   */
  async applySwitch(target: ChatTarget, requested: string): Promise<string> {
    const live = this.deps.liveWorker(target);
    if (requested.toLowerCase() === "default" && !requested.includes("/")) {
      return await this.resetChatModel(target);
    }
    // A selector is a single token; catching a stray one here keeps a typo from starting a worker.
    if (/\s/u.test(requested)) return notFoundModelText(requested);
    let outcome: WorkerModelSwitch;
    try {
      outcome = live
        ? await live.switchModel(requested)
        : await this.withProbeWorker(target, worker => worker.switchModel(requested));
    } catch (error) {
      return switchFailureText(error);
    }
    if (!outcome.ok) return notFoundModelText(requested);
    const model = modelWithLevel(outcome.model, outcome.level);
    this.deps.store.setChatModel(this.deps.botUid(), target, model);
    return live === undefined
      ? `已切换本聊天模型为 ${model}，从下一回合开始生效。`
      : `已切换本聊天模型为 ${model}（运行中会话已同步切换）。`;
  }

  /**
   * The empty `/switch` card reports OMP's observed model separately from the policy for the next
   * worker; `plain` carries the same information if cards are unavailable.
   */
  private switchMenuView(target: ChatTarget, running: boolean, catalog: WorkerModelCatalog): CommandCardView {
    const { current, label, policy } = this.chatModel(target, running, catalog);
    return {
      kind: "switch",
      current: modelCatalogCurrent(current, label),
      nextModel: modelPolicyDescription(policy.model, policy.source),
      choices: switchCardChoices(catalog.available),
      plain: formatModelCatalog({ current, label, models: catalog.available, policy }),
    };
  }

  /** The observed current model is live only while a worker is running; otherwise it is probe-only. */
  private chatModel(
    target: ChatTarget,
    running: boolean,
    catalog: WorkerModelCatalog,
  ): { current: string | null; label: string; policy: ModelSelection } {
    const policy = this.deps.store.resolveChatModel(this.deps.botUid(), target);
    const current = catalog.current === null ? null : modelWithLevel(catalog.current, catalog.level);
    return {
      current,
      label: running ? "运行中会话" : "临时探测（不代表恢复会话）",
      policy,
    };
  }

  /**
   * `/switch default` removes only the chat override. A thread then inherits its group's policy;
   * with no applicable setting the next worker starts without `--model`, including on resume.
   */
  private async resetChatModel(target: ChatTarget): Promise<string> {
    const botUid = this.deps.botUid();
    const hadOverride = this.deps.store.getChatModel(botUid, target) !== null;
    if (hadOverride) this.deps.store.clearChatModel(botUid, target);
    const policy = this.deps.store.resolveChatModel(botUid, target);
    const action = hadOverride ? "已清除本聊天显式模型" : "本聊天没有显式模型覆盖";
    return `${action}；下次启动策略：${modelPolicyDescription(policy.model, policy.source)}，运行中会话保持原模型。`;
  }


  /**
   * One throwaway worker serves a `/switch` probe: fresh (no `--resume`), no prompt of our own
   * beyond the switch dispatch, and a `mkdtemp` session directory it never shares with a chat
   * session, so it neither reads nor writes a running turn's transcript. It runs in the chat's own
   * directory — same OMP profile, config, and credentials as that chat's turns — is recorded in
   * the worker ledger like `/new`'s candidate worker, and is removed afterwards.
   */
  private async withProbeWorker<T>(
    target: ChatTarget,
    run: (worker: WorkerSession) => Promise<T>,
  ): Promise<T> {
    const cwd = await this.deps.probeDirectory(target);
    if (cwd === null) throw new SwitchDirectoryError();
    const sessionDir = await mkdtemp(join(tmpdir(), "omp-channel-octo-switch-"));
    const sessionId = randomUUID();
    const ledgerId = `switch-${sessionId}`;
    try {
      const lease = await openTrackedWorker({
        workerFactory: this.deps.workerFactory,
        store: this.deps.store,
        worker: { cwd, sessionDir },
        ledger: {
          id: ledgerId,
          botUid: this.deps.botUid(),
          sessionId,
          sessionFile: join(sessionDir, `${sessionId}.jsonl`),
        },
        blockDirectory: this.deps.blockDirectory,
      });
      let result: T;
      try {
        result = await run(lease.worker);
      } catch (error) {
        await lease.close(true);
        throw error;
      }
      await lease.close();
      return result;
    } finally {
      await rm(sessionDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}
