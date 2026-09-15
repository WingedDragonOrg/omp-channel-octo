import { constants as fsConstants } from "node:fs";
import { access, chmod, mkdir, readFile, stat, unlink, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, normalize, resolve } from "node:path";
import type { CoreServiceConfig } from "./contracts.ts";

export const DEFAULT_CONFIG_PATH = "~/.config/omp-channel-octo/config.json";
export const DEFAULT_BOT_TOKEN_ENV = "OCTO_BOT_TOKEN";
export const DEFAULT_API_URL = "https://im.deepminer.com.cn";

const CONFIG_KEYS = new Set([
  "apiUrl",
  "wsUrl",
  "botToken",
  "botTokenEnv",
  "ownerId",
  "allowedGroupIds",
  "ompCommand",
  "ompExtraArgs",
  "stateDir",
  "startupTimeoutMs",
  "cancelTimeoutMs",
  "restart",
]);
const TOKEN_ENV_PATTERN = /^(?:OCTO_|OMP_CHANNEL_OCTO_)[A-Za-z_][A-Za-z0-9_]*$/u;
const MIN_TIMEOUT_MS = 100;
const MAX_TIMEOUT_MS = 300_000;

export interface AppConfig extends CoreServiceConfig {
  apiUrl: string;
  wsUrl?: string;
  botToken: string;
  botTokenEnv: string;
  botTokenSource: "config" | "environment";
  ompCommand: string;
  ompExtraArgs: string[];
  startupTimeoutMs?: number;
  cancelTimeoutMs?: number;
  /** Deployment restart command, e.g. ["super", "-y", "restart", "omp-channel-octo"]. */
  restartCommand?: string[];
}

export interface ConfigParseOptions {
  env?: NodeJS.ProcessEnv;
  homeDir?: string;
}

function configError(message: string): Error {
  return new Error(`配置无效：${message}`);
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw configError(`${field} 必须是非空字符串`);
  }
  return value.trim();
}

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw configError(`${field} 必须是字符串`);
  const trimmed = value.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

function homePath(home: string | undefined): string {
  const value = home ?? homedir();
  if (typeof value !== "string" || value.trim().length === 0 || !isAbsolute(value)) {
    throw configError("homeDir 必须是绝对路径");
  }
  return normalize(value);
}

function parseUrl(value: unknown, field: string, protocols: readonly string[]): string {
  const raw = requireString(value, field);
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw configError(`${field} 必须是有效 URL`);
  }
  if (!protocols.includes(url.protocol)) {
    throw configError(`${field} 必须使用 ${protocols.join("/")} 协议`);
  }
  if (url.username || url.password) {
    throw configError(`${field} 不得包含内嵌凭证`);
  }
  return url.toString().replace(/\/+$/u, "");
}

export function resolveLocalPath(value: unknown, field: string, home = homedir()): string {
  const raw = requireString(value, field);
  const base = homePath(home);
  if (raw === "~") return base;
  if (raw.startsWith("~/")) return normalize(join(base, raw.slice(2)));
  if (raw.startsWith("~")) throw configError(`${field} 不支持其他用户的 ~name 路径`);
  if (!isAbsolute(raw)) throw configError(`${field} 必须是绝对路径或 ~/ 路径`);
  return normalize(raw);
}

export function resolveConfigPath(path = DEFAULT_CONFIG_PATH, home = homedir(), cwd = process.cwd()): string {
  const raw = requireString(path, "配置文件路径");
  if (raw === "~" || raw.startsWith("~/") || raw.startsWith("~")) {
    return resolveLocalPath(raw, "配置文件路径", home);
  }
  return normalize(isAbsolute(raw) ? raw : resolve(cwd, raw));
}
function parseTimeout(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "number" || !Number.isInteger(value) || value < MIN_TIMEOUT_MS || value > MAX_TIMEOUT_MS) {
    throw configError(`${field} 必须是 ${MIN_TIMEOUT_MS} 到 ${MAX_TIMEOUT_MS} 的整数毫秒数`);
  }
  return value;
}

function parseBotTokenEnv(value: unknown): string {
  const name = requireString(value, "botTokenEnv");
  if (!TOKEN_ENV_PATTERN.test(name)) {
    throw configError("botTokenEnv 只能使用 OCTO_ 或 OMP_CHANNEL_OCTO_ 前缀及有效环境变量名");
  }
  return name;
}

function parseRestartCommand(value: unknown): string[] | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw configError("restart 必须是 JSON 对象");
  }
  const document = value as Record<string, unknown>;
  for (const key of Object.keys(document)) {
    if (key !== "command") throw configError(`不支持字段 restart.${key}`);
  }
  if (document.command === undefined) return undefined;
  if (!Array.isArray(document.command) || document.command.length === 0) {
    throw configError("restart.command 必须是非空字符串数组");
  }
  return document.command.map((entry, index) => requireString(entry, `restart.command[${index}]`));
}

export function parseConfig(input: unknown, options: ConfigParseOptions = {}): AppConfig {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw configError("根值必须是 JSON 对象");
  }
  const document = input as Record<string, unknown>;
  for (const key of Object.keys(document)) {
    if (!CONFIG_KEYS.has(key)) throw configError(`不支持字段 ${key}`);
  }

  const home = homePath(options.homeDir);
  const apiUrl = document.apiUrl === undefined
    ? DEFAULT_API_URL
    : parseUrl(document.apiUrl, "apiUrl", ["http:", "https:"]);
  const wsUrl = document.wsUrl === undefined ? undefined : parseUrl(document.wsUrl, "wsUrl", ["ws:", "wss:"]);
  const hasConfiguredToken = document.botToken !== undefined;
  const botTokenEnv = document.botTokenEnv === undefined
    ? DEFAULT_BOT_TOKEN_ENV
    : parseBotTokenEnv(document.botTokenEnv);
  if (hasConfiguredToken && document.botTokenEnv !== undefined) {
    throw configError("botToken 与 botTokenEnv 只能配置一个");
  }

  let botToken: string;
  let botTokenSource: AppConfig["botTokenSource"];
  if (hasConfiguredToken) {
    botToken = requireString(document.botToken, "botToken");
    botTokenSource = "config";
  } else {
    const environment = options.env ?? process.env;
    botToken = requireString(environment[botTokenEnv], `环境变量 ${botTokenEnv}`);
    botTokenSource = "environment";
  }

  const ownerId = optionalString(document.ownerId, "ownerId");
  if (!Array.isArray(document.allowedGroupIds)) throw configError("allowedGroupIds 必须是字符串数组");
  const allowedGroupIds = document.allowedGroupIds.map((value, index) =>
    requireString(value, `allowedGroupIds[${index}]`),
  );
  if (new Set(allowedGroupIds).size !== allowedGroupIds.length) {
    throw configError("allowedGroupIds 不能重复");
  }

  const ompCommand = requireString(document.ompCommand, "ompCommand");
  const ompExtraArgs = document.ompExtraArgs === undefined ? [] : document.ompExtraArgs;
  if (!Array.isArray(ompExtraArgs) || ompExtraArgs.some((value) => typeof value !== "string")) {
    throw configError("ompExtraArgs 必须是字符串数组");
  }

  const startupTimeoutMs = parseTimeout(document.startupTimeoutMs, "startupTimeoutMs");
  const cancelTimeoutMs = parseTimeout(document.cancelTimeoutMs, "cancelTimeoutMs");
  const restartCommand = parseRestartCommand(document.restart);

  return {
    apiUrl,
    ...(wsUrl === undefined ? {} : { wsUrl }),
    botToken,
    botTokenEnv,
    botTokenSource,
    ...(ownerId === undefined ? {} : { ownerId }),
    allowedGroupIds,
    ompCommand,
    ompExtraArgs: [...ompExtraArgs],
    stateDir: resolveLocalPath(document.stateDir, "stateDir", home),
    ...(startupTimeoutMs === undefined ? {} : { startupTimeoutMs }),
    ...(cancelTimeoutMs === undefined ? {} : { cancelTimeoutMs }),
    ...(restartCommand === undefined ? {} : { restartCommand }),
  };
}

export async function loadConfig(
  path = DEFAULT_CONFIG_PATH,
  options: ConfigParseOptions = {},
): Promise<AppConfig> {
  const resolvedPath = resolveConfigPath(path, options.homeDir ?? homedir());
  let text: string;
  let fileMode: number;
  try {
    const info = await stat(resolvedPath);
    if (!info.isFile()) throw new Error("not a file");
    fileMode = info.mode;
    text = await readFile(resolvedPath, "utf8");
  } catch {
    throw new Error("无法读取配置文件");
  }

  let document: unknown;
  try {
    document = JSON.parse(text);
  } catch {
    throw new Error("配置无效：JSON 解析失败");
  }
  if (
    document !== null
    && typeof document === "object"
    && !Array.isArray(document)
    && (document as Record<string, unknown>).botToken !== undefined
    && (fileMode & 0o077) !== 0
  ) {
    throw new Error("配置无效：包含 botToken 的配置文件必须是私有权限");
  }
  return parseConfig(document, options);
}

export async function ensureStateDirectory(config: Pick<AppConfig, "stateDir">): Promise<void> {
  const path = config.stateDir;
  try {
    await mkdir(path, { recursive: true, mode: 0o700 });
    await chmod(path, 0o700);
    const info = await stat(path);
    if (!info.isDirectory()) throw new Error("not a directory");
    await access(path, fsConstants.W_OK | fsConstants.X_OK);
    const probe = join(path, `.write-check-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`);
    try {
      await writeFile(probe, "ok", { encoding: "utf8", flag: "wx", mode: 0o600 });
    } finally {
      await unlink(probe).catch(() => {});
    }
  } catch {
    throw new Error("状态目录不可用或不可写");
  }
}

export function toCoreConfig(config: AppConfig): CoreServiceConfig {
  return {
    ...(config.ownerId === undefined ? {} : { ownerId: config.ownerId }),
    allowedGroupIds: [...config.allowedGroupIds],
    stateDir: config.stateDir,
  };
}
