/**
 * 任务失败分类:把 worker 创建成功之前的启动失败,以及启动之后的执行失败,收敛成固定类别的
 * 聊天文案、任务行前缀与日志细节。类别的判据是类型化原因码(`TaskStartupError`),阶段只做
 * 兜底;路径、错误对象与协议载荷都不出现在任何用户可见面。
 */

import { TaskStartupError, type TaskStartupCode } from "../contracts.ts";
import { failureText } from "../errors.ts";

/** Startup read phases precede worker startup and have distinct, non-sensitive user notices. */
export type TaskStartupPhase = "context" | "members" | "startup";

export type TaskFailureKind = "context" | "members" | "binding" | "worker" | "generic";

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
  members: {
    notice: "成员信息读取失败，未启动任务。",
    row: "成员信息读取失败",
    classCode: "member-read",
    fallback: "成员信息读取失败",
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

function safeMemberFailureDetail(error: unknown): string {
  let message = "";
  try {
    message = error instanceof Error ? error.message : "";
  } catch {
    return "成员 API 请求失败";
  }
  if (message === "成员 API 查询超时") return "成员 API 查询超时";
  if (message === "成员 API 响应格式无效") return "成员 API 响应格式无效";
  if (message === "线程成员查询身份无效") return "成员查询目标无效";
  return "成员 API 请求失败";
}

function describeTaskFailure(kind: TaskFailureKind, code: string | undefined, error: unknown): TaskFailure {
  const words = KIND_WORDS[kind];
  const resolved = code ?? words.classCode;
  // Member API errors are external response bodies; only fixed, recognized classifications may
  // enter logs or durable task rows.
  const detail = kind === "members"
    ? safeMemberFailureDetail(error)
    : failureText(error, words.fallback);
  return {
    kind,
    code: resolved,
    detail,
    notice: kind === "context" || kind === "members" || kind === "generic"
      ? words.notice
      : `${words.notice}（${resolved}）。`,
    row: `${words.row}（${resolved}）：${detail}`,
  };
}

/**
 * A typed worker startup error wins except during member reads, whose external error details are
 * never trusted as log or task-row content.
 */
export function classifyStartupFailure(error: unknown, phase: TaskStartupPhase): TaskFailure {
  if (phase !== "members" && error instanceof TaskStartupError) {
    return describeTaskFailure(BINDING_CODES[error.code] === true ? "binding" : "worker", error.code, error);
  }
  const kind = phase === "context"
    ? "context"
    : phase === "members"
      ? "members"
      : "worker";
  return describeTaskFailure(kind, undefined, error);
}

/** A failure raised after the worker existed includes its original exception message in chat. */
export function classifyTaskFailure(error: unknown): TaskFailure {
  const failure = describeTaskFailure("generic", undefined, error);
  return {
    ...failure,
    notice: `${failure.notice}\n${error instanceof Error ? error.message : String(error)}`,
  };
}
