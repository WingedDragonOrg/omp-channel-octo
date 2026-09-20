/**
 * 会话目录布局：每个项目（任务的 canonical cwd）一个子目录。
 *
 * 所有 worker 都带自己的 `--session-dir`；同一个根下按 cwd 隔离，不同聊天与隔离子区工作区的
 * 会话不会混在同一个目录里。根由配置的 `sessionsDir` 给出（默认 `~/.oco/sessions`），
 * 未配置时回退到 `<stateDir>/sessions`（改动前的布局，供手工构造的配置与旧会话恢复使用）。
 */

import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { basename, join, resolve } from "node:path";

import type { CoreServiceConfig } from "../contracts.ts";

/** 项目目录名的可读部分上限；超出部分由路径摘要区分。 */
const PROJECT_LABEL_MAX_CHARS = 60;
/** 路径摘要长度；同一个 basename 的不同项目靠它区分。 */
const PROJECT_DIGEST_CHARS = 8;

/** 存放所有项目会话目录的根。 */
export function sessionsRoot(config: Pick<CoreServiceConfig, "sessionsDir" | "stateDir">): string {
  return config.sessionsDir ?? join(config.stateDir, "sessions");
}

/** 单个项目的目录名：cwd 的 basename 加上 canonical 路径摘要，重名目录与改名工作区互不混淆。 */
export function projectSessionKey(cwd: string): string {
  const canonical = resolve(cwd);
  const label = basename(canonical)
    .replace(/[^A-Za-z0-9._-]+/gu, "-")
    .replace(/^[.-]+/u, "")
    .slice(0, PROJECT_LABEL_MAX_CHARS);
  const digest = createHash("sha256").update(canonical).digest("hex").slice(0, PROJECT_DIGEST_CHARS);
  return `${label.length === 0 ? "project" : label}-${digest}`;
}

/** `<root>/<projectKey>/`。 */
export function projectSessionDir(root: string, cwd: string): string {
  return join(root, projectSessionKey(cwd));
}

/** `<root>/<projectKey>/`，与状态目录同样的私有权限。 */
export async function ensureProjectSessionDir(root: string, cwd: string): Promise<string> {
  const directory = projectSessionDir(root, cwd);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  return directory;
}
