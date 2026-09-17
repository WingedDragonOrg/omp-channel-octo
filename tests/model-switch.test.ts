import { expect, test } from "bun:test";
import type { WorkerModelInfo } from "../src/contracts";
import {
  MODEL_CATALOG_MAX_MODELS,
  boundedSelectorText,
  formatModelCatalog,
  modelWithLevel,
} from "../src/model-switch";

const MODELS: WorkerModelInfo[] = [
  { provider: "deepminer", id: "claude-opus-5", name: "Claude Opus 5" },
  { provider: "deepminer", id: "tencent/glm-5.3", name: "glm-5.3" },
  { provider: "octo", id: "claude-opus-5", name: "Claude Opus 5 · OCTO" },
  { provider: "fixture", id: "fast" },
];

test("a stored model value keeps its thinking level", () => {
  expect(modelWithLevel("fixture/fast", "max")).toBe("fixture/fast:max");
  expect(modelWithLevel("fixture/fast", null)).toBe("fixture/fast");
});

test("a selector echoed in an error line stays single-line and bounded", () => {
  expect(boundedSelectorText("  two   words  ")).toBe("two words");
  expect(boundedSelectorText("bad\u0007selector")).toBe("bad selector");
  const long = boundedSelectorText("x".repeat(200));
  expect(long.length).toBe(61);
  expect(long.endsWith("…")).toBe(true);
});

test("the catalog reply groups ids by provider and names where the current model comes from", () => {
  const text = formatModelCatalog({ current: "fixture/default:max", label: "本聊天指定", models: MODELS });
  expect(text.split("\n")).toEqual([
    "当前模型：fixture/default:max（本聊天指定）",
    "可用模型（来自 OMP 已配置 provider，共 4 个）：",
    "deepminer：claude-opus-5、tencent/glm-5.3",
    "octo：claude-opus-5",
    "fixture：fast",
  ]);
});

test("a report without a current model, an empty catalog, and an oversized catalog", () => {
  expect(formatModelCatalog({ current: null, label: "OMP 默认", models: MODELS }).split("\n")[0]).toBe(
    "当前模型：未知（OMP 未报告）",
  );
  expect(formatModelCatalog({ current: "fixture/default", label: "OMP 默认", models: [] })).toBe(
    ["当前模型：fixture/default（OMP 默认）", "OMP 当前没有可用模型。"].join("\n"),
  );

  const many: WorkerModelInfo[] = Array.from({ length: MODEL_CATALOG_MAX_MODELS + 3 }, (_, index) => ({
    provider: "fixture",
    id: `m${index}`,
  }));
  const text = formatModelCatalog({ current: null, label: "OMP 默认", models: many });
  const lines = text.split("\n");
  expect(lines[1]).toBe(`可用模型（来自 OMP 已配置 provider，共 ${many.length} 个）：`);
  const renderedIds = lines.filter(line => line.startsWith("fixture：")).flatMap(line => line.slice("fixture：".length).split("、"));
  expect(renderedIds.length).toBe(MODEL_CATALOG_MAX_MODELS);
  expect(lines.at(-1)).toBe(`…（共 ${many.length} 个）`);
});
