/**
 * `/thread` 家族的命令所有者：`/thread <任务>` 把意图交给助手去新建隔离子区，`/thread merge`
 * 把 owner 的合并确认交给助手去执行——两条命令都只组装交给助手的指令正文，并按该聊天的追加
 * 模式交付（steering 或排队任务），自己既不创建子区也不运行合并；用法与资格文案（群聊限定、
 * 子区限定、未绑定工作区、未配置能力）同样留在这里。
 *
 * 模块不依赖 `service.ts`：命令 claim 的释放、附件与引用的唯一组装点、运行中回合的并入、任务
 * 接受与回执投递都由 `CoreService` 以端口注入——正文因此走的仍是普通消息那条路径，命令本身
 * 不另开一套组装或调度。bot 身份用 getter（start 之后才存在），隔离工作区能力由部署决定，
 * 缺席时合并命令只回一条文案。
 */

import type { InboundMessage, WorktreePort } from "../contracts.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { InboundClaim } from "../store/store-records.ts";
import { worktrunkUnavailableText } from "../workspace/worktrunk.ts";

const THREAD_USAGE = [
  "用法：/thread <任务>；无项目需求时沿用普通子区；单项目可沿用 worktree_name，或用 projects=[{name,repo,base?}] 指定仓库（repo 用绝对规范路径，省略 layout 表示 single）；多项目必须列出 projects 并明确选择 layout=shared|separate；",
  "background 只写简短用途，不放任务正文、SDD 或验收标准；创建后必须确认整批 ready=true 才可派发，未就绪时报告资源结果并停止；",
  "之后用 thread-dispatch 派发任务；子区内 /thread merge [补充说明] 即确认合并回目标分支并默认清理隔离工作区与已合并分支，保留需说明理由。",
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

/**
 * Instruction block for a `/thread merge` message: like the creation prompt it hands the intent
 * to the assistant instead of acting itself. The command is the owner's confirmation to merge and
 * clean up, so the body names the default pipeline (stages and commits the workspace changes,
 * squashes, rebases onto the target branch, fast-forwards it, then removes the worktree and its
 * merged branch) as the expected behaviour and asks for a stated reason plus an explicit
 * `remove:false` whenever the workspace must be kept; it also states that the removal takes the
 * sub-thread's cwd with it as a known result that must not be repeated as a caveat or used as a
 * reason to keep the workspace. A trailing note after the `merge` keyword is the owner's own
 * supplement and is carried along verbatim. The merge itself is executed by the host through
 * `octo_management`'s `thread-merge`, and a default-pipeline merge also schedules the sub-thread's
 * archive after the turn's own reply is delivered — so the body tells the assistant not to archive
 * by itself and to report that disposition. Every outcome, including a conflict that must be
 * resolved before retrying, is reported to the owner.
 */
function threadMergePrompt(workspacePath: string, note: string): string {
  return [
    "用户要求把本子区（隔离工作区）已完成的工作合并回目标分支，走本机 Worktrunk（wt）的 merge pipeline。",
    "这条命令即代表 owner 确认合并并清理：除非有明确必要理由并向 owner 说明，直接按默认参数执行。",
    "默认 pipeline 会暂存并提交工作区改动、squash、rebase 到目标分支、fast-forward，合并成功后删除工作区与已合并分支；执行前先确认本子区的工作已收尾。",
    "要保留工作区时必须说明必要理由，并显式传 remove:false；目标分支等其他参数确有需要时同样显式传入并说明理由。",
    "工作区被删除后本子区 cwd 随之失效，这是该命令的既定结果、owner 已知：不要在汇报里反复说明这一点，也不要把 cwd 失效当作保留工作区的理由——保留工作区的必要理由应是尚未收尾的具体工作（例如部署、验收）。",
    "合并由宿主执行：调用 octo_management 的 thread-merge 动作，不要自己运行 wt。",
    "合并成功后宿主按本子区当前 listen 策略向创建聊天回传单一摘要；默认 pipeline 会排定在本回合最后一条回复送达后归档，不要自行再调 thread-archive。",
    "成功后在本子区说明目标分支与处置即可；失败时（例如 rebase 冲突或 pre-merge 检查失败）只在本子区修复再重试，不向创建聊天发失败或修复通知。",
    ...(note.length === 0 ? [] : ["", "owner 补充说明：", note]),
    "",
    "隔离工作区：",
    workspacePath,
  ].join("\n");
}

/**
 * `/thread` 家族命令可以触及的自有状态之外的一切：绑定行与命令 claim 的持久层、公用的消息
 * 组装点（附件与引用随正文进入任务）、命令路径共用的 claim 与 typing 租约包装、运行中回合
 * 的并入、任务接受与回执投递，以及 bot 身份（getter：start 之后才知道）与部署可选的隔离
 * 工作区能力。
 */
export interface ThreadCommandsDeps {
  readonly store: Pick<ChannelStore, "getBinding" | "getThreadWorkspace" | "releaseClaim">;
  /** Ensures creation provenance/default policy before a merge-intent mutation. */
  readonly ensureThreadListen: (message: InboundMessage) => Promise<void>;
  /** A confirmed merge intent suppresses task notices until a successful merge event is saved. */
  readonly beginThreadMerge: (message: InboundMessage) => void;
  /** 阻止未就绪或已缺失工作区的命令改写当前回合或创建任务。 */
  readonly refuseIncompleteThreadWorkspace: (message: InboundMessage) => Promise<boolean>;
  /** 绑定行按键的 bot 身份；start 之前不存在，命令只在 start 之后到达。 */
  readonly botUid: () => string | undefined;
  /** 隔离工作区能力：缺席即本部署未配置，合并命令只回文案。 */
  readonly worktrees?: WorktreePort;
  /** 唯一组装点：`/thread` 的正文与随消息发来的图片/引用一起进入任务。 */
  readonly materialize: (message: InboundMessage, text?: string) => Promise<string | null>;
  /** 命令 claim：与普通命令同一条持久化路径，命令自己组装正文前先释放它。 */
  readonly commandClaim: (message: InboundMessage, name: string) => InboundClaim;
  /** 与普通消息同一条的门后 typing 包装。 */
  readonly withMessageTyping: <T>(message: InboundMessage, work: () => Promise<T>) => Promise<T>;
  /** 运行中的回合接受 steering 时把指令并入该回合，否则返回 false 走排队任务。 */
  readonly appendToRunningTask: (message: InboundMessage, text: string) => Promise<boolean>;
  /** 把组装好的正文接受为该聊天的任务；返回是否真正落盘。 */
  readonly acceptTask: (message: InboundMessage, text: string) => Promise<boolean>;
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

  /**
   * `/thread merge`: a sub-thread that is bound to an isolated workspace can hand the merge
   * instruction to the assistant; an executable local Worktrunk must exist, because merging has no
   * fallback. Every other caller — the main group, DMs, a thread without a bound workspace, a
   * deployment without the capability or a missing wt — gets one prompt and no
   * task. The command runs nothing itself: the prompt is delivered like any other message the
   * assistant has to read (steering when the running turn accepts it, otherwise a durable task),
   * and the assistant drives `octo_management`'s `thread-merge`.
   */
  private async handleThreadMergeCommand(message: InboundMessage, mergeNote: string): Promise<void> {
    if (message.channelType !== 5) {
      await this.deps.enqueueText(message, THREAD_MERGE_THREAD_ONLY_TEXT, null);
      return;
    }
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
    this.deps.beginThreadMerge(message);
    this.deps.store.releaseClaim(this.deps.commandClaim(message, "thread"));
    await this.deps.withMessageTyping(message, async () => {
      const body = threadMergePrompt(binding.cwd, mergeNote);
      if (await this.deps.appendToRunningTask(message, body)) return;
      await this.deps.acceptTask(message, body);
    });
  }
}
