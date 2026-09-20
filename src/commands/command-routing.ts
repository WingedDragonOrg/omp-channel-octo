/**
 * 命令路由词汇表：入站正文的判定、内建命令的别名归属与已注册 handler 的索引都在这里，
 * 调度方只用一份解析结果决定走 builtin、走已注册 handler，还是当作普通任务。
 *
 * 模块不触 store、不做 I/O：只提供纯函数与类型。内建 action 的别名表只有一张，
 * canonical 类型从表值推导，避免识别（是否内建）与分派（走哪个 handler）各写一遍。
 */

import type { ChannelCommandHandler, InboundMessage } from "../contracts.ts";

/**
 * 已注册命令 handler 按规范化名（去首尾空白、转小写）建索引；名称非法或重复注册时抛出
 * 注册错误，调用方在打开 store 之前完成校验。
 */
export function normalizeCommandHandlers(
  handlers: readonly ChannelCommandHandler[] | undefined,
): ReadonlyMap<string, ChannelCommandHandler> {
  const normalized = new Map<string, ChannelCommandHandler>();
  for (const handler of handlers ?? []) {
    if (
      typeof handler !== "object" ||
      handler === null ||
      typeof handler.name !== "string" ||
      typeof handler.intercept !== "boolean" ||
      typeof handler.handle !== "function"
    ) {
      throw new Error("命令 handler 注册无效");
    }
    const name = handler.name.trim().toLowerCase();
    if (name.length === 0 || name.startsWith("/") || /\s/u.test(name)) {
      throw new Error("命令 handler 名称无效");
    }
    if (normalized.has(name)) {
      throw new Error(`命令 handler 重复注册：${name}`);
    }
    normalized.set(name, handler);
  }
  return normalized;
}

/** 正文是否以斜杠命令开头（`/` 紧跟非空白）；纯判定，不看是否内建或已注册。 */
export function isCommand(text: string): boolean {
  return /^\/\S+/.test(text.trim());
}

/** 拆出命令名（小写）与参数；不是命令时返回 `null`。 */
export function parseCommand(text: string): { name: string; argument: string } | null {
  const match = /^\/([^\s]+)(?:\s+([\s\S]*))?$/.exec(text.trim());
  if (!match) return null;
  return { name: match[1]!.toLowerCase(), argument: match[2] ?? "" };
}

/**
 * 内建命令名 → canonical action 的唯一别名表：`new`/`reset` 与 `cancel`/`stop` 是仅有的
 * 同义写法，其余命令名与自身同名。
 *
 * 原生 OMP 命令（如 `/retry`）刻意不进表：表里没有的名字既不是内建、也不一定已注册，会按
 * 普通任务经唯一组装点交给 OMP，由 OMP 自己的命令分派处理；把原生命令收进来就等于从 OMP
 * 手里抢走它。
 */
const BUILTIN_ALIASES = {
  new: "new",
  reset: "new",
  move: "move",
  thread: "thread",
  cancel: "cancel",
  stop: "cancel",
  status: "status",
  help: "help",
  restart: "restart",
  mode: "mode",
  queue: "queue",
  interrupt: "interrupt",
  switch: "switch",
  "autoresearch-history": "autoresearch-history",
} as const;

/** 内建命令的 canonical action，从别名表的值推导，识别与分派共用同一份定义。 */
export type BuiltinAction = (typeof BUILTIN_ALIASES)[keyof typeof BUILTIN_ALIASES];

/**
 * 每个 canonical action 一个 handler：`message` 是原始入站消息（含正文与目标），`arg` 是
 * 解析出的参数。返回 `true` 表示请求本回合排定重启；其余返回值不参与判定。
 */
export type BuiltinHandlers = Record<
  BuiltinAction,
  (message: InboundMessage, arg: string) => void | Promise<void | boolean>
>;

/**
 * 解析出的命令名所属的 canonical action；不属于任何内建命令时返回 `undefined`。
 * 以自有属性判定，`/constructor`、`/toString` 这类原型成员不会被当成内建命令。
 */
export function resolveBuiltinAction(name: string): BuiltinAction | undefined {
  return Object.hasOwn(BUILTIN_ALIASES, name)
    ? BUILTIN_ALIASES[name as keyof typeof BUILTIN_ALIASES]
    : undefined;
}
