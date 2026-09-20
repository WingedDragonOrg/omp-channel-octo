/**
 * `/switch` 的渲染与边界：把模型目录渲染成聊天里可读、有界的回执，并把选择器文本收进错误行。
 * 选择器的解析（`provider/id`、模型 id/子串、`@角色`、`:level`）由 OMP 自己的 `/switch` 完成，
 * 探测 worker、持久化与命令路径在 `service.ts`。
 */

import type { WorkerModelInfo } from "../contracts.ts";

/** Rendering bound: an oversized provider catalog still yields one readable message. */
export const MODEL_CATALOG_MAX_MODELS = 80;
/** Longest selector echoed back in an error line. */
export const MODEL_SELECTOR_MAX_CHARS = 60;

/** The chat's stored model value: canonical `provider/id` plus its thinking level when known. */
export function modelWithLevel(model: string, level: string | null): string {
  return level === null ? model : `${model}:${level}`;
}

/**
 * A selector as it may appear in an error line: control characters dropped, whitespace collapsed,
 * and length bounded, so no user input can stretch or break a receipt.
 */
export function boundedSelectorText(selector: string, max = MODEL_SELECTOR_MAX_CHARS): string {
  const cleaned = selector.replace(/[\p{Cc}\p{Cf}]+/gu, " ").replace(/\s+/gu, " ").trim();
  return cleaned.length <= max ? cleaned : `${cleaned.slice(0, max)}…`;
}

/**
 * The empty `/switch` answer: the model the chat is on followed by every available model, grouped
 * by provider and truncated at {@link MODEL_CATALOG_MAX_MODELS}. `current` already carries its
 * level; `label` says where the value comes from (a running session, this chat's setting, OMP's
 * default).
 */
export function formatModelCatalog(input: {
  current: string | null;
  label: string;
  models: readonly WorkerModelInfo[];
}): string {
  const lines: string[] = [
    input.current === null ? "当前模型：未知（OMP 未报告）" : `当前模型：${input.current}（${input.label}）`,
  ];
  if (input.models.length === 0) {
    lines.push("OMP 当前没有可用模型。");
    return lines.join("\n");
  }
  lines.push(`可用模型（来自 OMP 已配置 provider，共 ${input.models.length} 个）：`);
  const groups = new Map<string, string[]>();
  let rendered = 0;
  let truncated = false;
  for (const model of input.models) {
    if (rendered >= MODEL_CATALOG_MAX_MODELS) {
      truncated = true;
      break;
    }
    const ids = groups.get(model.provider);
    if (ids) ids.push(model.id);
    else groups.set(model.provider, [model.id]);
    rendered += 1;
  }
  for (const [provider, ids] of groups) lines.push(`${provider}：${ids.join("、")}`);
  if (truncated) lines.push(`…（共 ${input.models.length} 个）`);
  return lines.join("\n");
}
