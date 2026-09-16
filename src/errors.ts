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
