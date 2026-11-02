import { WorkerGoalError } from "../contracts.ts";
import { safeDisplayText } from "../cards/execution-text.ts";
import type { GoalSessionRecord } from "../store/store-records.ts";

export const GOAL_USAGE =
  "用法：/goal [show|set [--budget N] <目标>|pause|resume|drop|budget [N|off]]。原生目标预算不能事后修改。";

export type GoalCommand =
  | { op: "show" }
  | { op: "set"; objective: string; tokenBudget?: number }
  | { op: "pause" }
  | { op: "resume" }
  | { op: "drop" }
  | { op: "budget-show" }
  | { op: "budget-set"; value: string };

export type GoalCommandParse = { command: GoalCommand } | { error: string };

export function parseGoalCommand(argument: string): GoalCommandParse {
  const input = argument.trim();
  if (input.length === 0 || input === "show") return { command: { op: "show" } };
  const match = /^(\S+)(?:\s+([\s\S]*))?$/u.exec(input);
  if (!match) return { error: GOAL_USAGE };
  const action = match[1]!;
  const tail = (match[2] ?? "").trim();
  switch (action) {
    case "set": {
      if (tail.length === 0) return { error: GOAL_USAGE };
      const match = /^--budget\s+(\S+)\s+([\s\S]+)$/u.exec(tail);
      if (match) {
        const tokenBudget = Number(match[1]);
        const objective = match[2]!.trim();
        if (!/^\d+$/u.test(match[1]!) || !Number.isSafeInteger(tokenBudget) || tokenBudget <= 0 || objective.length === 0) {
          return { error: "目标预算必须是正整数，且目标内容不能为空。" };
        }
        return { command: { op: "set", objective, tokenBudget } };
      }
      if (tail.startsWith("--budget")) return { error: "目标预算参数无效；" + GOAL_USAGE };
      return { command: { op: "set", objective: tail } };
    }
    case "pause":
      return tail.length === 0 ? { command: { op: "pause" } } : { error: GOAL_USAGE };
    case "resume":
      return tail.length === 0 ? { command: { op: "resume" } } : { error: GOAL_USAGE };
    case "drop":
      return tail.length === 0 ? { command: { op: "drop" } } : { error: GOAL_USAGE };
    case "budget":
      if (tail.length === 0) return { command: { op: "budget-show" } };
      return /^(?:off|\d+)$/u.test(tail)
        ? { command: { op: "budget-set", value: tail } }
        : { error: GOAL_USAGE };
    default:
      return { error: GOAL_USAGE };
  }
}

export function formatGoalSession(session: GoalSessionRecord | null): string {
  if (session === null) return "当前会话没有原生目标。";
  const objective = safeDisplayText(session.objective, 320) ?? "（目标内容不可显示）";
  const status = session.status ?? (session.goalId === null ? "queued" : "unknown");
  const budget = session.tokenBudget === null ? "off" : String(session.tokenBudget);
  const goalId = safeDisplayText(session.goalId, 128) ?? "待原生接受";
  return `goal=${goalId}\nobjective=${objective}\nstatus=${status}\ntokensUsed=${session.tokensUsed}\ntimeUsedSeconds=${session.timeUsedSeconds}\ntokenBudget=${budget}`;
}

/** Only stable WorkerGoalError categories cross the service boundary; native payload text stays private. */
export function goalErrorText(error: unknown): string | null {
  if (!(error instanceof WorkerGoalError)) return null;
  switch (error.code) {
    case "unsupported":
      return "当前 OMP 不支持原生目标 RPC，未启动普通模型任务。";
    case "disabled":
      return "OMP 原生目标功能未启用，未调用模型。";
    case "already-exists":
      return "当前会话已有原生目标，不能替换；请使用 /goal show、/goal pause 或 /goal drop。";
    case "already-active":
      return "原生目标已在运行；没有重复启动任务。";
    case "identity-mismatch":
      return "当前会话的原生目标 ID 与已记录目标不一致；已停止自动恢复。";
    case "continuation-disabled":
      return "当前 worker 未启用原生目标自动续跑；未继续执行目标。";
    case "invalid-request":
      return "原生目标请求无效，未启动任务。";
    case "invalid-response":
      return "OMP 原生目标状态无效，已停止自动执行。";
    case "native-error":
      return safeDisplayText(error.message, 240) ?? "OMP 原生目标操作失败。";
  }
}
