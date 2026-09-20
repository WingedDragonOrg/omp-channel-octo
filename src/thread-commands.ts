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

import type { InboundMessage, WorktreePort } from "./contracts.ts";
import type { Outbox } from "./outbox.ts";
import type { ChannelStore } from "./store.ts";
import type { InboundClaim } from "./store-records.ts";
import { worktrunkUnavailableText } from "./worktrunk.ts";

const THREAD_USAGE =
  "用法：/thread <任务>；助手会先明确意图、确定子区名称与 worktree_name，然后新建隔离子区并把任务派发进去；子区内 /thread merge [补充说明] 即确认把本区工作合并回目标分支并清理隔离工作区（默认删除工作区与已合并分支，保留需说明理由；补充说明随合并指令一起交给助手）。";
const THREAD_GROUP_ONLY_TEXT = "/thread 需要在群聊中使用。";
const THREAD_NESTED_TEXT = "当前会话已是子区，不能再建子区。";
const THREAD_MERGE_THREAD_ONLY_TEXT = "/thread merge 只在子区中使用。";
const THREAD_MERGE_NO_WORKSPACE_TEXT =
  "当前子区未绑定隔离工作区，/thread merge 不可用。";
const THREAD_MERGE_UNCONFIGURED_TEXT =
  "本部署未配置隔离工作区能力，/thread merge 不可用。";

/**
 * Instruction block for a `/thread` message: the command hands the intent over instead of
 * creating anything, and the assistant names the sub-thread and its workspace itself, then drives
 * `octo_management`. The dispatch body always follows SDD shape (goal, scope, constraints,
 * acceptance): a requirement clear enough to generate the SDD from is dispatched without a
 * clarification round, so the thread generates the SDD and starts at once; anything else is
 * dispatched after at most the few SDD-shaped answers it takes to hand it over, with the remaining
 * clarification left to the thread (guided by a clarification skill when the environment has one).
 * Detailed clarification never happens in the originating chat. The dispatch's `notify` flag stays
 * the assistant's own judgment: it asks for a completion relay only for hand-offs it will not keep
 * watching.
 */
function threadIntentPrompt(intent: string): string {
  return [
    "用户希望把下面这件事放进一个新的隔离子区处理，而不是在当前会话直接执行。",
    "先理解意图，并据此确定子区名称（简洁、能说明这件事）与 worktree_name（ASCII 小写连字符，如 fix-login-flow）。",
    "需求简单或非常明确时不要在这里澄清；其余情况先按 SDD 的要素（目标、范围、约束与验收标准）向 owner 问最少必要的问题（够派发即可，不要在当前会话展开细节），再派发。",
    "然后调用 octo_management：create-thread（name=子区名称，worktree_name=你定的名字）创建隔离子区，",
    "再用 thread-dispatch（short_id=该子区）把任务派发进去；派发的任务正文按 SDD 格式写（目标、范围、约束与验收标准），需求明确程度决定子区能否直接生成 SDD：",
    "- 简单或非常明确（目标与验收标准能直接写清、足以直接生成 SDD、没有需要 owner 决策的歧义或取舍）：直接按 SDD 格式写清要做什么（可整理原始意图，但不要丢信息），让子区直接生成 SDD 并立刻开工；",
    "- 否则：按 SDD 格式写明已知的目标、范围、约束与验收标准，并要求子区先按 SDD 格式把尚不明确的需求澄清成 SDD（可实施的设计）再动手，需要 owner 拍板的问题在子区内用 ask 提出；环境里有 spec-design 等需求澄清或设计技能时，引导子区用该技能完成澄清。",
    "派发时自行判断是否要回执：只有你派发后不会再盯着这份活、又需要在它结束（完成/失败/取消/中断）时知道结果，才给 thread-dispatch 传 notify=true；你会在子区里持续交互、或结果不需要回报时不要开（默认 false，不开就不回报）。",
    "详细的需求澄清在子区内进行：不要在当前会话执行这件事，也不要在当前会话做过于详细的需求澄清。",
    "完成后简要汇报子区名称、short_id 与工作区路径。",
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
    "合并成功后宿主会按默认 pipeline 自动排定归档本子区（在本回合最后一条回复送达后执行），不要自己再调 thread-archive；显式保留工作区时不会归档。向 owner 汇报时说明这一处置即可。",
    "成功后向 owner 汇报目标分支与结果细节；失败时（例如 rebase 冲突或 pre-merge 检查失败）先在本子区处置再重试，并把最终结果告诉 owner。",
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
  readonly store: Pick<ChannelStore, "getBinding" | "releaseClaim">;
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
   * instruction to the assistant; a compatible local Worktrunk must exist, because merging has no
   * fallback. Every other caller — the main group, DMs, a thread without a bound workspace, a
   * deployment without the capability, a missing or incompatible wt — gets one prompt and no
   * task. The command runs nothing itself: the prompt is delivered like any other message the
   * assistant has to read (steering when the running turn accepts it, otherwise a durable task),
   * and the assistant drives `octo_management`'s `thread-merge`.
   */
  private async handleThreadMergeCommand(message: InboundMessage, mergeNote: string): Promise<void> {
    if (message.channelType !== 5) {
      await this.deps.enqueueText(message, THREAD_MERGE_THREAD_ONLY_TEXT, null);
      return;
    }
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
        `${worktrunkUnavailableText(available)}，/thread merge 不可用。`,
        null,
      );
      return;
    }
    this.deps.store.releaseClaim(this.deps.commandClaim(message, "thread"));
    await this.deps.withMessageTyping(message, async () => {
      const body = threadMergePrompt(binding.cwd, mergeNote);
      if (await this.deps.appendToRunningTask(message, body)) return;
      await this.deps.acceptTask(message, body);
    });
  }
}
