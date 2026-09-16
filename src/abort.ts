/** 把已取消的信号转成异常，让调用方走各自的失败路径。 */
export function throwIfAborted(signal: AbortSignal, message = "question cancelled"): void {
  if (signal.aborted) throw new Error(message);
}
