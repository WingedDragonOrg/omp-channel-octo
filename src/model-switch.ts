/**
 * `/switch` 的渲染与边界：把模型目录渲染成聊天里可读、有界的回执，并把选择器文本收进错误行。
 * 选择器的解析（`provider/id`、模型 id/子串、`@角色`、`:level`）由 OMP 自己的 `/switch` 完成，
 * 探测 worker、持久化与命令路径在 `service.ts`。
 */

import type { WorkerModelInfo } from "./contracts.ts";

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
 * The current model as one value: the model plus where it comes from, or the one sentence OMP gets
 * when it reports no model. The text fallback's first line and the card's `current` share this, so
 * the two can never disagree.
 */
export function modelCatalogCurrent(current: string | null, label: string): string {
  return current === null ? "未知（OMP 未报告）" : `${current}（${label}）`;
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
  const lines: string[] = [`当前模型：${modelCatalogCurrent(input.current, input.label)}`];
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

/**
 * One catalog entry as an OMP selector: `provider/id` is what the native `/switch` resolves, so a
 * card choice and the same text typed by hand take the same path. Entries that cannot be a single
 * selector token (an empty or whitespace-bearing part) are dropped — the card never issues a value
 * the text command would refuse.
 */
function modelSelector(model: WorkerModelInfo): string | null {
  const selector = `${model.provider}/${model.id}`;
  if (model.provider.length === 0 || model.id.length === 0 || /\s/u.test(selector)) return null;
  return selector;
}

/**
 * The `/switch` card's choices: `default` plus every catalog entry usable as a selector, capped
 * like the text catalog at {@link MODEL_CATALOG_MAX_MODELS}. Values are the selectors verbatim
 * (exactly what the click applies), titles are the same selectors bounded for display.
 */
export function switchCardChoices(
  models: readonly WorkerModelInfo[],
): { title: string; value: string }[] {
  const choices: { title: string; value: string }[] = [
    { title: "跟随 OMP 默认（清除本聊天模型）", value: "default" },
  ];
  const seen = new Set<string>();
  for (const model of models) {
    if (choices.length > MODEL_CATALOG_MAX_MODELS) break;
    const selector = modelSelector(model);
    if (selector === null || seen.has(selector)) continue;
    seen.add(selector);
    choices.push({ title: boundedSelectorText(selector), value: selector });
  }
  return choices;
}
