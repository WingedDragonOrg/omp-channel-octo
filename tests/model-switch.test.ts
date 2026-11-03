import { expect, test } from "bun:test";
import type { WorkerModelInfo } from "../src/contracts";
import {
  MODEL_CATALOG_MAX_MODELS,
  boundedSelectorText,
  formatModelCatalog,
  modelCatalogCurrent,
  modelWithLevel,
  switchCardChoices,
} from "../src/commands/model-switch";

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

test("the catalog reports observed state, the next-worker policy, and grouped providers", () => {
  const text = formatModelCatalog({
    current: "fixture/default:max",
    label: "临时探测（不代表恢复会话）",
    models: MODELS,
    policy: { model: "other/fast:max", source: "thread" },
  });
  expect(text).toContain("当前模型：fixture/default:max");
  expect(text).toContain("下次启动策略：other/fast:max（主群子区默认）");
  expect(text).toContain("deepminer：claude-opus-5、tencent/glm-5.3");
  expect(text).toContain("octo：claude-opus-5");
  expect(text).toContain("fixture：fast");
});

test("a missing observed model and an empty or oversized catalog stay explicit", () => {
  const missing = formatModelCatalog({
    current: null,
    label: "临时探测（不代表恢复会话）",
    models: MODELS,
    policy: { model: null, source: "omp" },
  });
  expect(missing).toContain("当前模型：未知（OMP 未报告）");
  expect(missing).toContain("下次启动策略：不指定（由 OMP 自行决定）");
  const empty = formatModelCatalog({
    current: "fixture/default",
    label: "临时探测（不代表恢复会话）",
    models: [],
    policy: { model: null, source: "omp" },
  });
  expect(empty).toContain("OMP 当前没有可用模型。");

  const many: WorkerModelInfo[] = Array.from({ length: MODEL_CATALOG_MAX_MODELS + 3 }, (_, index) => ({
    provider: "fixture",
    id: `m${index}`,
  }));
  const text = formatModelCatalog({
    current: null,
    label: "临时探测（不代表恢复会话）",
    models: many,
    policy: { model: null, source: "omp" },
  });
  const lines = text.split("\n");
  const renderedIds = lines.filter(line => line.startsWith("fixture：")).flatMap(line => line.slice("fixture：".length).split("、"));
  expect(renderedIds.length).toBe(MODEL_CATALOG_MAX_MODELS);
  expect(text).not.toContain("m100");
  expect(text).toContain("仅展示前 100 个，共 103 个");
});


test("the card offers only selectors the text command would accept", () => {
  const values = switchCardChoices([
    { provider: "fixture", id: "fast" },
    { provider: "fixture", id: "fast" },
    { provider: "", id: "orphan" },
    { provider: "fixture", id: "two words" },
    { provider: "deepminer", id: "tencent/glm-5.3" },
  ]).map(choice => choice.value);
  expect(values).toEqual(["default", "fixture/fast", "deepminer/tencent/glm-5.3"]);
  const many: WorkerModelInfo[] = Array.from({ length: MODEL_CATALOG_MAX_MODELS + 5 }, (_, index) => ({
    provider: "fixture",
    id: `m${index}`,
  }));
  expect(switchCardChoices(many)).toHaveLength(MODEL_CATALOG_MAX_MODELS + 1);
});

test("the catalog includes the observed current model before its next-worker policy", () => {
  const current = modelCatalogCurrent("fixture/default:max", "运行中会话");
  const text = formatModelCatalog({
    current: "fixture/default:max",
    label: "运行中会话",
    models: MODELS,
    policy: { model: "fixture/default:max", source: "chat" },
  });
  expect(text.split("\n")[0]).toContain(current);
  expect(text).toContain("下次启动策略：fixture/default:max（本聊天指定）");
  expect(modelCatalogCurrent(null, "OMP 默认")).toBe("未知（OMP 未报告）");
});
