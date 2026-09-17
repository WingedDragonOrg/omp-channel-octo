/**
 * 任务失败分类:把 worker 创建成功之前的启动失败,以及启动之后的执行失败,收敛成固定类别的
 * 聊天文案、任务行前缀与日志细节。类别的判据是类型化原因码(`TaskStartupError`),阶段只做
 * 兜底;路径、错误对象与协议载荷都不出现在任何用户可见面。
 */

import { TaskStartupError, type TaskStartupCode } from "./contracts.ts";
import { failureText } from "./errors.ts";

/** 启动阶段:群说明读取(仅群/子区)之后、worker 创建之前的一切都算 `startup`。 */
export type TaskStartupPhase = "context" | "startup";

export type TaskFailureKind = "context" | "binding" | "worker" | "generic";

export type TaskFailure = {
  readonly kind: TaskFailureKind;
  /** 具体原因码;没有类型化错误时是该类别的类码。 */
  readonly code: string;
  /** 单行、有界、无秘密的原因文本:进服务日志与 owner 侧任务行。 */
  readonly detail: string;
  /** 聊天里那一句。 */
  readonly notice: string;
  /** `tasks.error_text`。 */
  readonly row: string;
};

/** 原因码里属于「绑定状态不可用」的一档:owner 可用 `/new`、`/move` 直接修好。 */
const BINDING_CODES: Partial<Record<TaskStartupCode, true>> = {
  "cwd-missing": true,
  "session-invalid": true,
};

/** 任务已判定失败但不需要启动分类时的聊天文案(与既有实现逐字一致)。 */
export const GENERIC_TASK_FAILURE_NOTICE = "任务执行失败；未能可靠完成。";

const KIND_WORDS: Record<TaskFailureKind, {
  readonly notice: string;
  readonly row: string;
  readonly classCode: string;
  readonly fallback: string;
}> = {
  context: {
    notice: "群说明读取失败，未启动任务。",
    row: "群说明读取失败",
    classCode: "context-read",
    fallback: "群说明读取失败",
  },
  binding: {
    notice: "任务未启动：绑定的工作目录或会话不可用",
    row: "绑定不可用",
    classCode: "binding-unavailable",
    fallback: "绑定的工作目录或会话不可用",
  },
  worker: {
    notice: "任务未启动：OMP 运行进程启动失败",
    row: "worker 启动失败",
    classCode: "worker-start",
    fallback: "OMP 运行进程启动失败",
  },
  generic: {
    notice: GENERIC_TASK_FAILURE_NOTICE,
    row: "任务执行失败",
    classCode: "task-failed",
    fallback: "执行失败",
  },
};

function describeTaskFailure(kind: TaskFailureKind, code: string | undefined, error: unknown): TaskFailure {
  const words = KIND_WORDS[kind];
  const resolved = code ?? words.classCode;
  const detail = failureText(error, words.fallback);
  return {
    kind,
    code: resolved,
    detail,
    notice: kind === "context" || kind === "generic"
      ? words.notice
      : `${words.notice}（${resolved}）。`,
    row: `${words.row}（${resolved}）：${detail}`,
  };
}

/**
 * Classifies a failure raised before a worker existed. A typed code wins; without one the phase
 * decides, because a group context failure is the only class that describes "we never got as far
 * as the worker".
 */
export function classifyStartupFailure(error: unknown, phase: TaskStartupPhase): TaskFailure {
  if (error instanceof TaskStartupError) {
    return describeTaskFailure(BINDING_CODES[error.code] === true ? "binding" : "worker", error.code, error);
  }
  return describeTaskFailure(phase === "context" ? "context" : "worker", undefined, error);
}

/** Classifies a failure raised after the worker existed: the chat notice stays the generic one. */
export function classifyTaskFailure(error: unknown): TaskFailure {
  return describeTaskFailure("generic", undefined, error);
}
