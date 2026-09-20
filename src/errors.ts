/**
 * 跨模块共享的错误最小面：把 `unknown` 收敛成带 `code` 的 `Error`，供需要按 errno 分类
 * 的模块（Bot 独占锁、出站投递）共用。
 */
export type AppError = Error & { code?: string };

export function asAppError(error: unknown): AppError {
  return error instanceof Error ? error : new Error("operation failed");
}

/**
 * 日志里的错误只写固定文案：错误对象可能携带传输层或状态盘的内部细节，绝不进日志与卡片。
 * 调用方拿到的是可读的兜底说明，真实错误仍按各自的错误路径处理。
 */
export function safeErrorMessage(_error: unknown, fallback: string): string {
  return fallback;
}

/**
 * 服务端瞬时失败：任何 5xx，或内部 500 被 bot API 包在线缆 400 上的 `store_failed`。
 * 与 `isPermanentOutboxError` 同口径：字段结构化读取，夹具可以脱离传输模块复现同样的形状。
 */
export function isTransientServerFailure(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { code?: unknown; status?: unknown };
  if (typeof candidate.status === "number" && candidate.status >= 500) return true;
  return candidate.code === "err.server.bot_api.store_failed";
}

/**
 * Shutdown interrupted an inbound message before it was durably accepted. The handler rejects with
 * it so the transport never ACKs a swallowed message; the same message is redelivered after restart.
 * Defined here, once, because the permission gate (`chat-authorization`) and the attachment
 * materializer both raise it and `service.ts` classifies it with `instanceof`.
 */
export class InboundShutdownError extends Error {
  constructor() {
    super("channel 正在停止，消息未接受");
    this.name = "InboundShutdownError";
  }
}

/**
 * 任务启动失败的有界原因文本：单行化、清除控制字符、截断到固定长度，供服务日志与 owner 侧
 * 任务行使用。它只放行本仓库构造的消息（原因码、HTTP 状态、路径），不携带错误对象本身、
 * 协议载荷或凭据；群消息里仍是各类别的固定文案。
 */
export const FAILURE_TEXT_LIMIT = 200;

export function failureText(error: unknown, fallback: string): string {
  const raw = error instanceof Error ? error.message : typeof error === "string" ? error : "";
  const flat = raw.replace(/[\p{Cc}\p{Cf}]+/gu, " ").replace(/\s+/gu, " ").trim();
  if (flat.length === 0) return fallback;
  return flat.length <= FAILURE_TEXT_LIMIT ? flat : `${flat.slice(0, FAILURE_TEXT_LIMIT - 1)}…`;
}
