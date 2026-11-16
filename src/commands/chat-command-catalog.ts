import { stripVTControlCharacters } from "node:util";
import {
  BUILTIN_COMMAND_ENTRIES,
  resolveBuiltinAction,
  type BuiltinAction,
} from "./command-routing.ts";
import type {
  ChannelCommandHandler,
  ChatCommandSuggestion,
  ChatCommandsSnapshot,
  ChatTarget,
  OmpAvailableCommand,
} from "../contracts.ts";

const MAX_SNAPSHOT_BYTES = 512 * 1024;
const encoder = new TextEncoder();

type BuiltinCommandPresentation = {
  description: string;
  hint?: string;
  subcommands?: ChatCommandSuggestion["subcommands"];
};
const BUILTIN_PRESENTATION: Record<BuiltinAction, BuiltinCommandPresentation> = {
  new: { description: "创建新会话", hint: "[目录]" },
  move: { description: "保留当前会话并迁移工作目录", hint: "<目录>" },
  bind: {
    description: "绑定当前主群的 Loop workspace",
    hint: "loop <workspace>",
    subcommands: [{
      name: "loop",
      description: "绑定当前主群的 Loop workspace",
      hint: "<Loop workspace 名称、slug、ID 或同源 Fleet issue/project 链接>",
    }],
  },
  thread: { description: "在当前群或子区使用子区命令" },
  fork: { description: "分叉当前会话历史到新子区", hint: "[子区名称]" },
  cancel: { description: "取消当前任务并清除排队消息" },
  review: {
    description: "在独立会话审查当前项目变更",
    hint: "[审查重点]",
    subcommands: [{ name: "stop", description: "停止当前聊天的审查" }],
  },
  commit: {
    description: "提交当前绑定项目",
    hint: "[stop]",
    subcommands: [{ name: "stop", description: "停止当前聊天的提交" }],
  },
  status: { description: "查看当前状态" },
  help: { description: "查看帮助" },
  goal: {
    description: "管理本会话的原生目标",
    hint: "[show|set|pause|resume|drop|budget]",
    subcommands: [
      { name: "show", description: "查看当前目标状态" },
      { name: "set", description: "设置目标", hint: "[--budget N] <目标>" },
      { name: "pause", description: "暂停目标" },
      { name: "resume", description: "恢复目标" },
      { name: "drop", description: "清除目标" },
      { name: "budget", description: "查看或设置预算", hint: "[N|off]" },
    ],
  },
  btw: { description: "在当前绑定会话旁路提问", hint: "<问题>" },
  restart: { description: "重启 channel，在途任务重启后续接", hint: "[now]" },
  mode: {
    description: "查看或设置新消息的追加方式",
    hint: "[steer|queue]",
    subcommands: [
      { name: "steer", description: "立即追加到当前回合" },
      { name: "queue", description: "排到当前回合之后" },
    ],
  },
  queue: { description: "将消息排到当前任务之后", hint: "<文本>" },
  interrupt: { description: "中断当前回合并保留排队消息" },
  watch: {
    description: "查看或停止当前聊天的 watch",
    hint: "[list|<id>|stop <id|all>]",
    subcommands: [
      { name: "list", description: "列出当前聊天的 watch" },
      { name: "stop", description: "停止 watch", hint: "<id|all>" },
    ],
  },
  switch: { description: "查看或切换本聊天的模型", hint: "[provider|模型]" },
  "autoresearch-history": { description: "导出本聊天最近一次研究的完整记录" },
};

function routeIsAvailable(action: BuiltinAction, target: ChatTarget): boolean {
  if (action === "bind") return target.channelType === 2;
  if (action === "thread" || action === "fork") return target.channelType !== 1;
  return true;
}

function threadSuggestion(target: ChatTarget): ChatCommandSuggestion {
  if (target.channelType === 2) {
    return {
      name: "thread",
      source: "channel",
      description: "新建隔离子区并派发任务，或设置子区默认模型",
      hint: "<任务>",
      subcommands: [{ name: "model", description: "在主群设置、查询或清除子区默认模型", hint: "[选择器|clear]" }],
    };
  }
  return {
    name: "thread",
    source: "channel",
    description: "管理当前子区工作区",
    subcommands: [
      { name: "merge", description: "合并隔离工作区" },
      { name: "remove", description: "清理当前子区工作区与分支", hint: "[--force]" },
    ],
  };
}

function channelSuggestions(
  target: ChatTarget,
  handlers: ReadonlyMap<string, ChannelCommandHandler>,
): ChatCommandSuggestion[] {
  const grouped = new Map<BuiltinAction, string[]>();
  for (const entry of BUILTIN_COMMAND_ENTRIES) {
    if (!routeIsAvailable(entry.action, target)) continue;
    const names = grouped.get(entry.action);
    if (names) names.push(entry.name);
    else grouped.set(entry.action, [entry.name]);
  }

  const commands: ChatCommandSuggestion[] = [];
  for (const [action, names] of grouped) {
    if (action === "thread") {
      commands.push(threadSuggestion(target));
      continue;
    }
    const presentation = BUILTIN_PRESENTATION[action];
    const aliases = names.filter(name => name !== action);
    commands.push({
      name: action,
      source: "channel",
      description: presentation.description,
      ...(presentation.hint === undefined ? {} : { hint: presentation.hint }),
      ...(aliases.length === 0 ? {} : { aliases }),
      ...(presentation.subcommands === undefined ? {} : { subcommands: presentation.subcommands }),
    });
  }

  if (target.channelType !== 1) {
    commands.push({
      name: "tan",
      source: "channel",
      description: "分叉当前会话并在新子区独立执行",
      hint: "<用户请求>",
    });
  }

  for (const [name] of handlers) {
    // Builtins and /tan are dispatched before custom handlers; listing a shadowed handler would
    // advertise behavior that the channel router cannot reach.
    if (resolveBuiltinAction(name) !== undefined || name === "tan") continue;
    commands.push({ name, source: "channel" });
  }
  return commands;
}

function safeMetadataText(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const safe = stripVTControlCharacters(value).replace(/[\u0000-\u001f\u007f]/gu, " ").trim();
  if (safe.length === 0) return undefined;
  return safe;
}

function validCommandName(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= 128
    && value.trim() === value
    && !/[\s\u0000-\u001f\u007f/]/u.test(value);
}
function validSubcommandName(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.trim() === value
    && !/[\u0000-\u001f\u007f/]/u.test(value);
}

function ompSuggestion(command: OmpAvailableCommand, channelNames: ReadonlySet<string>): ChatCommandSuggestion | null {
  if (command.source === "extension" || !validCommandName(command.name)) return null;
  const aliases = [...new Set((command.aliases ?? []).filter(alias =>
    validCommandName(alias)
      && alias.toLowerCase() !== command.name.toLowerCase()
      && !channelNames.has(alias.toLowerCase()),
  ))];
  const subcommands = (command.subcommands ?? [])
    .filter(subcommand => validSubcommandName(subcommand.name))
    // OMP's top-level availability excludes TUI-only builtins. This specific subcommand is a
    // verified exception: it opens an interactive configuration editor unavailable to channel.
    .filter(subcommand => !(command.name.toLowerCase() === "advisor" && subcommand.name.toLowerCase() === "configure"))
    .map(subcommand => {
      const description = safeMetadataText(subcommand.description);
      const hint = safeMetadataText(subcommand.usage);
      return {
        name: subcommand.name,
        ...(description === undefined ? {} : { description }),
        ...(hint === undefined ? {} : { hint }),
      };
    });
  const description = safeMetadataText(command.description);
  const hint = safeMetadataText(command.hint);
  return {
    name: command.name,
    source: "omp",
    ...(description === undefined ? {} : { description }),
    ...(hint === undefined ? {} : { hint }),
    ...(aliases.length === 0 ? {} : { aliases }),
    ...(subcommands.length === 0 ? {} : { subcommands }),
  };
}

function snapshotBytes(target: ChatTarget, commands: readonly ChatCommandSuggestion[], omp: ChatCommandsSnapshot["omp"]): number {
  return encoder.encode(JSON.stringify({
    version: 1,
    channelType: target.channelType,
    channelId: target.channelId,
    commands,
    omp,
  })).byteLength;
}

/** Channel routes are authoritative; OMP commands with a colliding name are deliberately omitted. */
export function buildChatCommandsSnapshot(
  target: ChatTarget,
  handlers: ReadonlyMap<string, ChannelCommandHandler>,
  ompCommands?: readonly OmpAvailableCommand[],
): ChatCommandsSnapshot {
  const channel = channelSuggestions(target, handlers);
  if (ompCommands === undefined) {
    return { version: 1, ...target, commands: channel, omp: "unavailable" };
  }

  const reservedNames = new Set<string>();
  for (const entry of BUILTIN_COMMAND_ENTRIES) reservedNames.add(entry.name.toLowerCase());
  reservedNames.add("tan");
  for (const [name] of handlers) reservedNames.add(name.toLowerCase());
  for (const command of channel) {
    reservedNames.add(command.name.toLowerCase());
    for (const alias of command.aliases ?? []) reservedNames.add(alias.toLowerCase());
  }
  const seen = new Set<string>(reservedNames);
  const omp: ChatCommandSuggestion[] = [];
  for (const command of ompCommands) {
    const suggestion = ompSuggestion(command, reservedNames);
    if (suggestion === null) continue;
    const name = suggestion.name.toLowerCase();
    if (seen.has(name)) continue;
    seen.add(name);
    omp.push(suggestion);
  }

  const commands: ChatCommandSuggestion[] = [...channel, ...omp];
  if (snapshotBytes(target, commands, "available") > MAX_SNAPSHOT_BYTES) {
    return { version: 1, ...target, commands: channel, omp: "unavailable" };
  }
  return { version: 1, ...target, commands, omp: "available" };
}
