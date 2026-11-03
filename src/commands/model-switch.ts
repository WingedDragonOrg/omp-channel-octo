/**
 * `/switch` 的渲染与边界：把模型目录渲染成聊天里可读、有界的回执，并把选择器文本收进错误行。
 * 选择器的解析（`provider/id`、模型 id/子串、`@角色`、`:level`）由 OMP 自己的 `/switch` 完成，
 * 探测 worker、持久化与命令路径在 `service.ts`。
 */

import type { WorkerModelInfo } from "../contracts.ts";

/** Rendering bound: an oversized provider catalog still yields one readable message. */
export const MODEL_CATALOG_MAX_MODELS = 100;
/** Longest selector echoed back in an error line. */
export const MODEL_SELECTOR_MAX_CHARS = 60;

/** The chat's stored model value: canonical `provider/id` plus its thinking level when known. */
export function modelWithLevel(model: string, level: string | null): string {
  return level === null ? model : `${model}:${level}`;
}

export type ModelSelectionSource = "chat" | "thread" | "omp";

/** Format the configured worker-start policy without presenting it as a live model. */
export function modelPolicyDescription(
  model: string | null,
  source: ModelSelectionSource,
): string {
  if (model === null) return "不指定（由 OMP 自行决定）";
  const label = source === "chat"
    ? "本聊天指定"
    : source === "thread"
      ? "主群子区默认"
      : "OMP 自行决定";
  return `${model}（${label}）`;
}

export interface ModelSelection {
  model: string | null;
  source: ModelSelectionSource;
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

/** The shared scope/count line, visible on both the card and its text fallback. */
export function modelCatalogSummary(models: readonly WorkerModelInfo[], provider?: string): string {
  const total = selectableModels(models).length;
  const scope = provider === undefined ? "全部 provider" : `provider ${provider}`;
  const count = total > MODEL_CATALOG_MAX_MODELS
    ? `仅展示前 ${MODEL_CATALOG_MAX_MODELS} 个，共 ${total} 个；其余模型未列出`
    : `展示 ${total} 个，共 ${total} 个`;
  const guidance = total > MODEL_CATALOG_MAX_MODELS
    ? "\n使用 /switch <provider> 按 provider 筛选，或直接 /switch provider/model。"
    : "";
  return `可用模型（${scope}；${count}）${guidance}`;
}

/** The current model and the same bounded catalog the card offers, grouped by provider. */
export function formatModelCatalog(input: {
  current: string | null;
  label: string;
  models: readonly WorkerModelInfo[];
  policy: ModelSelection;
  provider?: string;
}): string {
  const lines = [
    `当前模型：${modelCatalogCurrent(input.current, input.label)}`,
    `下次启动策略：${modelPolicyDescription(input.policy.model, input.policy.source)}`,
    modelCatalogSummary(input.models, input.provider),
  ];
  const groups = new Map<string, string[]>();
  const models = selectableModels(input.models).slice(0, MODEL_CATALOG_MAX_MODELS);
  for (const model of models) {
    const ids = groups.get(model.provider);
    if (ids) ids.push(model.id);
    else groups.set(model.provider, [model.id]);
  }
  for (const [provider, ids] of groups) lines.push(`${provider}：${ids.join("、")}`);
  if (models.length === 0) lines.push("OMP 当前没有可用模型。");
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

/** Valid unique model options in OMP order, before either surface applies its display limit. */
function selectableModels(models: readonly WorkerModelInfo[]): WorkerModelInfo[] {
  const seen = new Set<string>();
  return models.filter(model => {
    const selector = modelSelector(model);
    if (selector === null || seen.has(selector)) return false;
    seen.add(selector);
    return true;
  });
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
    { title: "清除本聊天覆盖（子区继承主群默认，否则由 OMP 决定）", value: "default" },
  ];
  for (const model of selectableModels(models).slice(0, MODEL_CATALOG_MAX_MODELS)) {
    const selector = `${model.provider}/${model.id}`;
    choices.push({ title: boundedSelectorText(selector), value: selector });
  }
  return choices;
}
