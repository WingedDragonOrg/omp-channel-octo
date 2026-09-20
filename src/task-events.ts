/**
 * 一个 task 回合里 worker 事件的消费策略：通知帧（`notice`）的投递与级别标签、本地命令回合
 * 标记、assistant 文本按 messageId 聚合成 `resultText`，以及「停止后仍有工作」的归因；真正的
 * 执行事件（工具/待办/回合边界）原样转交进度卡。
 *
 * 模块不依赖 `service.ts`：投递、进度卡、关停标志与 steer 消费回执都由 `CoreService` 以窄端口
 * 注入；`control` 就是 `runTask` 的运行时对象本身，`workAfterStop` 的置位因此直接写回服务。
 * 事件路径全程同步，只做入队与记账，不引入新的异步边界。
 */

import type { ChatTarget, WorkerEvent } from "./contracts.ts";
import type { Outbox } from "./outbox.ts";
import type { TaskProgress } from "./progress-cards.ts";

/**
 * `runTask` 运行时对象的窄视图：TaskEvents 只读 `cancelRequested` / `stopCause`，
 * 唯一写入是 `workAfterStop`。
 */
export interface TaskEventControl {
  cancelRequested: boolean;
  stopCause: "interrupt" | "question" | null;
  workAfterStop: boolean;
}

export interface TaskEventsDeps {
  target: ChatTarget;
  taskId: string;
  control: TaskEventControl;
  /** 服务是否已进入关停：关停后到达的事件一律丢弃。 */
  stopping: () => boolean;
  outbox: Pick<Outbox, "enqueueStreamText">;
  progress: Pick<TaskProgress, "apply">;
  /**
   * 研究完整记录的落盘端口（`/autoresearch-history` 的数据源）：事件上的 report 只在这里
   * 被消费一次，落盘失败由服务自己记账——本模块只负责在权威帧上同步调用一次。
   */
  persistResearchReport: (report: string) => void;
  steerConsumed: (event: Extract<WorkerEvent, { type: "steer_consumed" }>) => void;
}

/**
 * 一个 task 的事件所有者：`onEvent` 是实例箭头属性，可直接交给 worker 的 prompt；
 * `localOnly` 与 `resultText` 供回合结算（context commit 门限与结果落库）读取。
 */
export class TaskEvents {
  private readonly assistantTexts = new Map<string, string>();
  /**
   * 本地命令回合标记（如 `/autoresearch` 的开关/清理）：无 agent 生成，OMP 不会写群说明
   * prepared receipt。置位后本回合跳过 context commit，但 sidecar 照常清理、ledger 保持
   * pending，下一个真实生成回合仍以 initial 注入并用真实 receipt 提交。
   */
  private localCommand = false;

  constructor(private readonly deps: TaskEventsDeps) {}

  readonly onEvent = (event: WorkerEvent): void => {
    if (this.deps.control.cancelRequested || this.deps.stopping()) return;
    if (event.type === "notice") {
      // 扩展的通知帧（如 /autoresearch 的 enable/off/拒绝原因）是信息性 UI：原样投递到
      // 对应会话、带上级别标签，但它不是回合产出——不进 assistantTexts/resultText、不开
      // 进度卡，也绝不能改 workAfterStop，让 /interrupt 的归因保持诚实。
      if (event.messageId.trim().length === 0 || event.text.length === 0) return;
      const label = event.level === "error"
        ? "OMP 错误："
        : event.level === "warning"
          ? "OMP 警告："
          : "OMP：";
      this.deps.outbox.enqueueStreamText(this.deps.target, this.deps.taskId, {
        type: "text",
        messageId: `notice:${event.messageId}`,
        text: `${label}${event.text}`,
        final: true,
      });
      return;
    }
    if (event.type === "local_command") {
      // no-agent 完成帧：标记本回合不产生 prepared receipt，commit 门限据此豁免；同样
      // 在 workAfterStop 之前返回，既不算工作产出也不触发进度。
      this.localCommand = true;
      return;
    }
    if (this.deps.control.stopCause === "question") return;
    if (this.deps.control.stopCause !== null && event.type !== "turn_end" && event.type !== "run_end") {
      this.deps.control.workAfterStop = true;
    }
    if (event.type === "text") {
      // 空 messageId 一律丢弃；空 final 文本仍更新聚合与出站（收尾帧照常落编辑）。
      if (event.messageId.trim().length === 0 || (event.text.length === 0 && !event.final)) return;
      this.assistantTexts.set(event.messageId, event.text);
      this.deps.outbox.enqueueStreamText(this.deps.target, this.deps.taskId, event);
      return;
    }
    if (event.type === "steer_consumed") {
      // 消费回执交给服务：它按 FIFO 的投递队列配对并记账。
      this.deps.steerConsumed(event);
      return;
    }
    if (event.type === "research_update") {
      // 完整记录按 bot+聊天落盘，供 /autoresearch-history 导出；报告只在权威 state
      // （init/log）生成的帧上出现，落盘失败由服务自己记账，不拖垮本任务。
      if (typeof event.report === "string" && event.report.length > 0) {
        this.deps.persistResearchReport(event.report);
      }
      // 快照交给进度卡：它按任务保留最近一次，steer 滚卡时结转。
      this.deps.progress.apply(event);
      return;
    }
    this.deps.progress.apply(event);
  };

  /** 本回合是否只有本地命令、没有任何 agent 生成。 */
  get localOnly(): boolean {
    return this.localCommand;
  }

  /**
   * 回合产出：按 messageId 聚合的 assistant 文本，插入顺序即各消息首次出现的顺序，段间以
   * 空行分隔；没有任何产出（含只有 notice / 本地命令的回合）为 null。
   */
  get resultText(): string | null {
    return this.assistantTexts.size > 0 ? [...this.assistantTexts.values()].join("\n\n") : null;
  }
}
