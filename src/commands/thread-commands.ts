/**
 * `/thread` 家族命令：`/thread <任务>` 把意图交给助手创建/派发子区；`/thread merge` 则由宿主直接
 * 执行 Worktrunk pipeline。命令 claim、普通任务接管、回执与调度仍通过 `CoreService` 端口完成。
 */

import { createHash } from "node:crypto";
import type { InboundMessage, WorktreeMergeSummary, WorktreePort } from "../contracts.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { BindingRecord, HostMergeLeaseRecord, InboundClaim } from "../store/store-records.ts";
import { WorktreeMergeExecutionError } from "../threads/thread-merge.ts";
import { worktrunkUnavailableText } from "../workspace/worktrunk.ts";

import { safeDisplayText } from "../cards/execution-text.ts";
function stableUuid(namespace: string, sourceId: string): string {
  const bytes = createHash("sha256").update(`${namespace}\0${sourceId}`).digest();
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const value = bytes.subarray(0, 16).toString("hex");
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
}

function mergeReceiptId(botUid: string, target: InboundMessage, sourceId: string): string {
  return stableUuid(
    "thread-merge-receipt",
    JSON.stringify([botUid, target.channelType, target.channelId, sourceId]),
  );
}
const THREAD_USAGE = [
  "用法：/thread <任务>；无项目需求时沿用普通子区；单项目可沿用 worktree_name，或用 projects=[{name,repo,base?}] 指定仓库（repo 用绝对规范路径，省略 layout 表示 single）；多项目必须列出 projects 并明确选择 layout=shared|separate；",
  "background 只写简短用途，不放任务正文、SDD 或验收标准；创建后必须确认整批 ready=true 才可派发，未就绪时报告资源结果并停止；",
  "之后用 thread-dispatch 派发任务；子区内 /thread merge [补充说明] 由宿主执行默认合并与清理；仅实际合并失败后派发一次修复任务，并保留补充说明。",
].join("");
const THREAD_GROUP_ONLY_TEXT = "/thread 需要在群聊中使用。";
const THREAD_NESTED_TEXT = "当前会话已是子区，不能再建子区。";
const THREAD_MERGE_THREAD_ONLY_TEXT = "/thread merge 只在子区中使用。";
const THREAD_MERGE_NO_WORKSPACE_TEXT =
  "当前子区未绑定隔离工作区，/thread merge 不可用。";
const THREAD_MERGE_UNCONFIGURED_TEXT =
  "本部署未配置隔离工作区能力，/thread merge 不可用。";
const THREAD_MERGE_MULTI_PROJECT_TEXT = "当前为多项目工作区，不支持 /thread merge。";

/**
 * Instruction block for a `/thread` message: the command hands the intent over instead of
 * creating anything, and the assistant names the sub-thread and its workspace itself, then drives
 * `octo_management`. The dispatch body always follows SDD shape (goal, scope, constraints,
 * acceptance): a requirement clear enough to generate the SDD from is dispatched without a
 * clarification round, so the thread generates the SDD and starts at once; anything else is
 * dispatched after at most the few SDD-shaped answers it takes to hand it over, with the remaining
 * clarification left to the thread (guided by a clarification skill when the environment has one).
 * Completion notices follow the persisted listen policy for the target thread.
 */
function threadIntentPrompt(intent: string): string {
  return [
    "用户希望把下面这件事放进隔离子区处理，而不是在当前会话直接执行。",
    "先理解意图，确定简洁的子区名称与工作区计划：无项目/Worktree 需求时可建普通子区；单项目可沿用 worktree_name，若需指定仓库或 base 则用 projects=[{name,repo,base?}] 且省略 layout（表示 single）；多项目必须列出每个项目并明确选择 layout=shared 或 separate（每项 repo 为绝对规范路径，base 为可选 Git 起始 ref；shared 是一个子区，separate 是每个项目一个子区）。",
    "项目、repo 或多项目 layout 无法从意图中可靠确定时，只向 owner 问最少必要的问题；不要臆造 repo 路径，也不要用当前 cwd 代替项目 repo。",
    "调用 octo_management create-thread 时传入完整 projects/layout；background 只写简短用途，不放任务正文、SDD 或验收标准。创建结果会包含实际成功资源路径，不要预先臆造路径。",
    "必须等待 create-thread 返回整批 ready=true 后才能调用 thread-dispatch；若 ready=false，说明各项目/子区的成功与失败资源并停止，不派发任何实现任务。",
    "整批就绪后，再按 short_id（separate 时逐项目对应）调用 thread-dispatch。派发正文按 SDD 格式写（目标、范围、约束与验收标准）：",
    "- 需求简单或非常明确时，保留全部意图并按 SDD 格式写清，让子区直接生成 SDD 并立刻开工；",
    "- 否则先按 SDD 要素写已知信息，并在子区内用最少问答补齐 owner 决策，必要时引导使用 spec-design 等技能；详细的需求澄清在子区内进行，不要留在当前会话。",
    "background 不要在 dispatch 时重写；SDD、任务细节和验收标准只放在派发正文。",
    "派发后的完成与合并摘要由子区持久化 listen 策略控制。",
    "完成后简要汇报子区名称、short_id 与真实工作区路径。",
    "",
    "原始意图：",
    intent,
  ].join("\n");
}

/**
 * `/thread model` is a control command only when `model` is its complete first word. Similar task
 * names such as `model-fix` stay on the ordinary thread-intent path.
 */
function readThreadModelArgument(argument: string): string | undefined {
  const trimmed = argument.trimStart();
  if (!/^model(?:$|\s)/iu.test(trimmed)) return undefined;
  return trimmed.slice("model".length).trim();
}

/**
 * `/thread` 参数按首词识别合并意图：首词是 `merge`（其后为行尾或非字母数字、下划线、连字符
 * 的分隔符——`merge-fix` 这类名字不算）时返回去掉分隔符并单行化后的补充说明（可为空串），
 * 其余情况返回 `undefined`。
 */
function readThreadMergeNote(argument: string): string | undefined {
  const trimmed = argument.trim();
  if (!/^merge(?:$|[^\p{L}\p{N}_-])/iu.test(trimmed)) return undefined;
  const note = trimmed
    .slice("merge".length)
    .replace(/^[\s,，、:：]+/u, "")
    .replace(/[\p{Cc}\p{Cf}]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return note.length > 200 ? `${note.slice(0, 199)}…` : note;
}

function boundedMergeError(error: unknown): string {
  return safeDisplayText(error instanceof Error ? error.message : String(error), 200) ?? "未知错误";
}

function threadMergeRepairPrompt(
  workspacePath: string,
  note: string,
  error: string,
  originalCommand: string,
): string {
  return [
    "用户已确认合并当前子区工作区。宿主实际执行默认 wt merge pipeline 时失败；请在原工作区检查并修复这次失败所需的最小问题。",
    "保留当前仓库与工作区，不要自行运行 wt；修复后调用 octo_management 的 thread-merge 动作让宿主重试。向 owner 说明修复和最终结果。",
    "失败与修复过程只留在本子区，不要向创建聊天发送失败或修复通知。",
    "",
    "原始 /thread merge 命令（敏感内容已遮盖）：",
    originalCommand,
    "",
    "宿主返回的实际执行错误：",
    error,
    ...(note.length === 0 ? [] : ["", "owner 补充说明：", note]),
    "",
    "隔离工作区：",
    workspacePath,
  ].join("\n");
}

function threadMergeReceipt(summary: WorktreeMergeSummary & { archive?: "scheduled" }): string {
  const workspace = summary.removed
    ? "已清理工作区与已合并分支"
    : "按参数保留工作区";
  const archive = summary.archive === "scheduled" ? "；回执送达后将归档子区" : "";
  return `子区合并成功：${summary.branch} → ${summary.target}；${workspace}${archive}。`;
}

/**
 * `/thread` 命令可触及的状态端口：绑定/所有权资格、命令 claim、隔离合并执行、稳定回执与归档，
 * 以及常规任务接管。宿主合并的调度与目录租约由 `CoreService` 持有。
 */
export interface ThreadCommandsDeps {
  readonly store: Pick<ChannelStore, "getBinding" | "getThreadWorkspace" | "getTaskByMessageId" | "getCommandClaimState" | "completeCommand" | "releaseClaim">;
  /** Read or change the group default using the same serialized model-config path as `/switch`. */
  readonly handleThreadModel: (message: InboundMessage, argument: string) => Promise<void>;
  /** Ensures creation provenance/default policy before a merge-intent mutation. */
  readonly ensureThreadListen: (message: InboundMessage) => Promise<void>;
  /** 阻止未就绪或已缺失工作区的命令改写当前回合或创建任务。 */
  readonly refuseIncompleteThreadWorkspace: (message: InboundMessage) => Promise<boolean>;
  /** 绑定行按键的 bot 身份；start 之前不存在，命令只在 start 之后到达。 */
  readonly botUid: () => string | undefined;
  /** 隔离工作区能力：缺席即本部署未配置，合并命令只回文案。 */
  readonly worktrees?: WorktreePort;
  readonly archiveGeneration: (target: InboundMessage) => number;
  readonly stopping: () => boolean;
  readonly executeHostMerge: (
    message: InboundMessage,
    binding: BindingRecord,
    receiptOutboxId: string,
    archiveGeneration: number,
    operatorNote: string,
  ) => Promise<WorktreeMergeSummary & { archive?: "scheduled" }>;
  /** 将一条已持久化成功回执入队；id 由命令 sourceId 稳定派生。 */
  readonly enqueueMergeReceipt: (target: InboundMessage, id: string, text: string) => void;
  /** 命令结果已持久交付后释放对应 outcome lease。 */
  readonly settleHostMerge: (target: InboundMessage, sourceId: string) => void;
  /** 启动恢复成功结果时，重建严格 receipt barrier 后的归档排定。 */
  readonly recoverMergeArchive: (target: InboundMessage, receiptOutboxId: string) => void;
  /** 以 strict receipt barrier 异步启动已经排定的归档。 */
  readonly runThreadArchive: (target: InboundMessage) => void;
  /**
   * 原始命令 ID 作为稳定 operationId 读取已有成功收据：非 null 表示该操作已成功，
   * 直接返回而不再执行 wt、不改新操作的 pending；回执/归档由既有 lease/outbox 负责。
   */
  readonly replayThreadMergeSuccess: (
    target: InboundMessage,
    operationId: string,
  ) => Promise<string | null>;
  /** 唯一组装点：`/thread` 的正文与随消息发来的图片/引用一起进入任务。 */
  readonly materialize: (message: InboundMessage, text?: string) => Promise<string | null>;
  /** 命令 claim 与普通命令同一路径；仅 `/thread <任务>` 会在接受任务前释放。 */
  readonly commandClaim: (message: InboundMessage, name: string) => InboundClaim;
  /** 与普通消息同一条的门后 typing 包装。 */
  readonly withMessageTyping: <T>(message: InboundMessage, work: () => Promise<T>) => Promise<T>;
  /** 运行中的回合接受 steering 时把指令并入该回合，否则返回 false 走排队任务。 */
  readonly appendToRunningTask: (message: InboundMessage, text: string) => Promise<boolean>;
  /** 把修复上下文按普通 durable task 语义接受；schedule=false 留给 startup restart recovery。 */
  readonly acceptTask: (message: InboundMessage, text: string, schedule?: boolean) => Promise<boolean>;
  /** 回一条资格/用法文案。 */
  readonly enqueueText: Outbox["enqueueText"];
}

export class ThreadCommands {
  constructor(private readonly deps: ThreadCommandsDeps) {}

  /**
   * `/thread <任务>`: hand the intent to the assistant instead of creating a sub-thread here. The
   * message carries an instruction block, so the agent reads the intent, names the sub-thread and
   * its workspace itself, and drives `octo_management`: a simple or unambiguous intent is
   * dispatched as the work itself so the thread starts at once, an underspecified one after at
   * most the few answers it takes to hand it over, with the detailed, SDD-shaped clarification
   * left to the thread. The body follows the chat's append mode like any other message the
   * assistant has to read: a running turn that accepts steering gets the instruction inside that
   * turn, otherwise it becomes a durable task (`queue` mode included) that waits behind the run.
   * Only groups grow threads, so DMs and threads themselves are refused.
   */
  async handle(message: InboundMessage, argument: string): Promise<void> {
    const modelArgument = readThreadModelArgument(argument);
    if (modelArgument !== undefined) {
      await this.deps.handleThreadModel(message, modelArgument);
      return;
    }
    const mergeNote = readThreadMergeNote(argument);
    if (mergeNote !== undefined) {
      await this.handleThreadMergeCommand(message, mergeNote);
      return;
    }
    if (message.channelType === 5) {
      await this.deps.enqueueText(message, THREAD_NESTED_TEXT, null);
      return;
    }
    if (message.channelType !== 2) {
      await this.deps.enqueueText(message, THREAD_GROUP_ONLY_TEXT, null);
      return;
    }
    const intent = argument.trim();
    if (intent.length === 0) {
      await this.deps.enqueueText(message, THREAD_USAGE, null);
      return;
    }
    // 这条命令的正文就是交给助手的任务正文：先释放命令 claim，再走与普通任务同一条组装点
    // （claim + 下载 + 转写 + 附件块），否则随消息发来的图片只剩 `[图片]` 占位。
    this.deps.store.releaseClaim(this.deps.commandClaim(message, "thread"));
    await this.deps.withMessageTyping(message, async () => {
      const request = await this.deps.materialize(message, intent);
      if (request === null) return;
      const body = threadIntentPrompt(request);
      if (await this.deps.appendToRunningTask(message, body)) return;
      await this.deps.acceptTask(message, body);
    });
  }

  /** Resume only durable command delivery; a known merge outcome is never executed again. */
  async recoverHostMergeOutcome(lease: HostMergeLeaseRecord): Promise<void> {
    const outcome = lease.outcome;
    if (!outcome) throw new Error("host merge command recovery requires a persisted outcome");
    const target: InboundMessage = {
      channelType: lease.channelType,
      channelId: lease.channelId,
      messageId: lease.sourceId,
      senderId: lease.senderId ?? "",
      text: lease.commandText ?? "",
      mentionedUserIds: [],
      attachments: [],
      quoted: null,
      supported: true,
      messageSeq: lease.messageSeq ?? 0,
      messageTimestamp: 0,
    };
    if (outcome.kind === "succeeded") {
      if (lease.commandText === null) throw new Error("host merge success recovery is missing its original command");
      const receiptId = lease.receiptOutboxId ?? mergeReceiptId(lease.botUid, target, lease.sourceId);
      this.deps.enqueueMergeReceipt(target, receiptId, threadMergeReceipt({
        ...outcome.summary,
        ...(outcome.summary.removed ? { archive: "scheduled" as const } : {}),
      }));
      if (outcome.summary.removed) this.deps.recoverMergeArchive(target, receiptId);
      this.completeRecoveredMergeCommand(lease.botUid, target);
      this.deps.settleHostMerge(target, lease.sourceId);
      return;
    }
    if (lease.commandText === null) throw new Error("host merge repair recovery is missing its original command");
    await this.acceptMergeRepairTask({
      message: target,
      sourceId: lease.sourceId,
      workspacePath: lease.cwd,
      note: lease.operatorNote ?? "",
      error: safeDisplayText(outcome.diagnostic, 200) ?? "未知错误",
      originalCommand: lease.commandText,
      schedule: false,
    });
    this.completeRecoveredMergeCommand(lease.botUid, target);
    this.deps.settleHostMerge(target, lease.sourceId);
  }

  private completeRecoveredMergeCommand(botUid: string, message: InboundMessage): void {
    const claim = this.deps.commandClaim(message, "thread");
    this.deps.store.completeCommand(claim);
    if (this.deps.store.getCommandClaimState(botUid, message, message.messageId) !== "completed") {
      throw new Error("recovered host merge command completion was not persisted");
    }
  }

  /**
   * `/thread merge`: host-first；资格拒绝只回复，不接任务。实际 wt pipeline 成功后回稳定 outbox
   * receipt；只有 shared core 明确标记为执行失败时，才用稳定合成消息创建一次修复 task。
   */
  private async handleThreadMergeCommand(message: InboundMessage, mergeNote: string): Promise<void> {
    if (message.channelType !== 5) {
      await this.deps.enqueueText(message, THREAD_MERGE_THREAD_ONLY_TEXT, null);
      return;
    }
    const replay = await this.deps.replayThreadMergeSuccess(message, message.messageId);
    if (replay !== null) return;
    const archiveGeneration = this.deps.archiveGeneration(message);
    const ownership = this.deps.store.getThreadWorkspace(this.deps.botUid()!, message);
    if (ownership?.layout === "shared") {
      await this.deps.enqueueText(message, THREAD_MERGE_MULTI_PROJECT_TEXT, null);
      return;
    }
    if (await this.deps.refuseIncompleteThreadWorkspace(message)) return;
    const worktrees = this.deps.worktrees;
    if (!worktrees) {
      await this.deps.enqueueText(message, THREAD_MERGE_UNCONFIGURED_TEXT, null);
      return;
    }
    const binding = this.deps.store.getBinding(this.deps.botUid()!, message);
    if (!binding || binding.cwdPinned !== true) {
      await this.deps.enqueueText(message, THREAD_MERGE_NO_WORKSPACE_TEXT, null);
      return;
    }
    const available = await worktrees.worktrunk();
    if (!available.available) {
      await this.deps.enqueueText(
        message,
        `${worktrunkUnavailableText()}，/thread merge 不可用。`,
        null,
      );
      return;
    }
    await this.deps.ensureThreadListen(message);
    const receiptOutboxId = mergeReceiptId(this.deps.botUid()!, message, message.messageId);
    const note = safeDisplayText(mergeNote, 200) ?? "";
    const originalCommand = safeDisplayText(message.text, 800)
      ?? `/thread merge${note.length === 0 ? "" : ` ${note}`}`;
    await this.deps.withMessageTyping(message, async () => {
      let summary: WorktreeMergeSummary & { archive?: "scheduled" };
      try {
        summary = await this.deps.executeHostMerge(
          message,
          binding,
          receiptOutboxId,
          archiveGeneration,
          note,
        );
      } catch (error) {
        if (this.deps.stopping()) return;
        if (error instanceof WorktreeMergeExecutionError) {
          await this.acceptMergeRepairTask({
            message,
            sourceId: message.messageId,
            workspacePath: binding.cwd,
            note,
            error: boundedMergeError(error),
            originalCommand,
          });
          this.deps.settleHostMerge(message, message.messageId);
          return;
        }
        await this.deps.enqueueText(
          message,
          `合并未完成且不会自动重试：${boundedMergeError(error)}。请先核对目标分支与工作区，再决定是否重新发起。`,
          null,
        );
        return;
      }
      if (this.deps.stopping()) return;
      this.deps.enqueueMergeReceipt(message, receiptOutboxId, threadMergeReceipt(summary));
      this.deps.runThreadArchive(message);
      this.deps.settleHostMerge(message, message.messageId);
    });
  }

  private async acceptMergeRepairTask(input: {
    readonly message: InboundMessage;
    readonly sourceId: string;
    readonly workspacePath: string;
    readonly note: string;
    readonly error: string;
    readonly originalCommand: string;
    readonly schedule?: boolean;
  }): Promise<void> {
    const repairMessage: InboundMessage = {
      ...input.message,
      messageId: `thread-merge-repair:${input.sourceId}`,
      text: input.originalCommand,
      mentionedUserIds: [],
      attachments: [],
      quoted: null,
    };
    const prompt = threadMergeRepairPrompt(
      input.workspacePath,
      input.note,
      input.error,
      input.originalCommand,
    );
    const accepted = await this.deps.acceptTask(repairMessage, prompt, input.schedule);
    if (accepted) return;
    const botUid = this.deps.botUid();
    const existing = botUid === undefined
      ? null
      : this.deps.store.getTaskByMessageId(botUid, repairMessage, repairMessage.messageId);
    if (existing) return;
    await this.deps.enqueueText(
      input.message,
      "合并已实际失败，但修复任务未能持久接收；合并结果已保留待恢复，未重新执行合并。",
      null,
    );
    throw new Error("merge repair task was not durably accepted");
  }
}
