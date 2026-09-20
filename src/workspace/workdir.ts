import { constants } from "node:fs";
import { access, mkdir, realpath, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";
import type { CoreServiceConfig } from "../contracts.ts";

/**
 * 任务工作目录的解析规则：`~` / `~/…` / 绝对路径（可带一层引号），空参数落到 `<home>/tmp`；
 * 结果一律 realpath 并校验可读可进入，避免同一目录以不同写法被当成两个目录。
 */
export async function canonicalExistingDirectory(path: string): Promise<string> {
  const canonical = await realpath(path);
  const details = await stat(canonical);
  if (!details.isDirectory()) throw new Error("路径不是目录");
  await access(canonical, constants.R_OK | constants.X_OK);
  return canonical;
}
export function stripOuterQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length >= 2) {
    const first = trimmed[0];
    const last = trimmed.at(-1);
    if ((first === "'" || first === '"') && last === first) {
      return trimmed.slice(1, -1);
    }
  }
  return trimmed;
}

export async function resolveWorkDirectory(argument: string, config: CoreServiceConfig): Promise<string> {
  const home = config.homeDir ?? homedir();
  const raw = stripOuterQuotes(argument);
  if (raw.length === 0) {
    const defaultDirectory = join(home, "tmp");
    await mkdir(defaultDirectory, { recursive: true });
    return canonicalExistingDirectory(defaultDirectory);
  }
  if (raw === "~") {
    return canonicalExistingDirectory(home);
  }
  if (raw.startsWith("~/")) {
    return canonicalExistingDirectory(join(home, raw.slice(2)));
  }
  if (raw.startsWith("~")) {
    throw new Error("不支持其他用户的 ~name 路径");
  }
  if (!isAbsolute(raw)) {
    throw new Error("目录必须是绝对路径或 ~/ 路径");
  }
  return canonicalExistingDirectory(raw);
}
export async function existingSessionFile(path: string): Promise<string | null> {
  try {
    const details = await stat(path);
    return details.isFile() ? path : null;
  } catch {
    return null;
  }
}
