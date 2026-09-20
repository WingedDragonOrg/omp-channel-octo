import assert from "node:assert/strict";
import { buildExecutionCard, type ExecutionCardCapabilities } from "../../src/cards/execution-card.ts";
import { createExecutionTracker, type ExecutionState, type ExecutionTodoPhase } from "../../src/cards/execution-tracker.ts";
import { buildCommandCard, buildCommandCardResult } from "../../src/cards/command-card.ts";

// Synthetic events and UTC clock; frozen workload, not an optimization target.
const start = Date.UTC(2026, 0, 1, 10);
const capabilities: ExecutionCardCapabilities = {
  enabled: true, interactive: true, profiles: ["octo/v1", "octo/v2"], cardVersion: "1.5",
  elements: ["TextBlock", "RichTextBlock", "Container", "ColumnSet", "ActionSet"],
  actions: ["Action.ToggleVisibility"],
  limits: { maxNodes: 200, maxDepth: 16, maxPayloadBytes: 28_672 },
};

function progress(id: string, state: ExecutionState = "running", caps = capabilities) {
  const tracker = createExecutionTracker({ runId: id, initialAtMs: start });
  tracker.apply({ type: "turn_start", turnId: "turn-1", atMs: start });
  if (id !== "starting") {
    const phases: ExecutionTodoPhase[] = [{ name: "准备", tasks: [
      { content: "读取项目配置并确认影响范围", status: "completed" },
      { content: "核对接口与兼容约束", status: "completed" },
    ] }, { name: "实现与验收", tasks: [
      { content: "调整移动端布局并保留完整说明", status: state === "completed" ? "completed" : "in_progress" },
      { content: "检查窄屏下长中文与英文选项", status: state === "completed" ? "completed" : "pending" },
      { content: "验证服务恢复后的交互状态", status: state === "completed" ? "completed" : "blocked", ...(state === "completed" ? {} : { blocker: "等待测试环境恢复" }) },
    ] }];
    if (id === "long-run") phases.push({ name: "回归", tasks: Array.from({ length: 28 }, (_, i) => ({ content: `验证第 ${i + 1} 项兼容场景与边界条件`, status: "pending" as const })) });
    tracker.apply({ type: "todo_update", atMs: start + 10, phases });
    for (let i = 0; i < 7; i++) {
      tracker.apply({ type: "tool_start", toolCallId: `step-${i}`, toolName: i === 6 ? "ask" : "read", summary: i === 6 ? "等待确认发布范围" : `检查第 ${i + 1} 项配置与兼容性`, atMs: start + 1000 * (i + 1) });
      if (i < 6) tracker.apply({ type: "tool_end", toolCallId: `step-${i}`, status: i === 4 ? "failed" : "succeeded", ...(i === 4 ? { errorCategory: "unavailable" as const } : {}), atMs: start + 1000 * (i + 1) + 500 });
    }
    tracker.apply({ type: "subagent_start", subagentId: "review", agent: "reviewer", state: "running", summary: "检查边界条件", atMs: start + 8000 });
    tracker.apply({ type: "subagent_update", subagentId: "review", state: "running", currentTool: "read", toolCount: 3, atMs: start + 9000 });
    tracker.apply({ type: "subagent_start", subagentId: "verify", agent: "task", state: "running", summary: "核对协议兼容性", atMs: start + 8000 });
    tracker.apply({ type: "subagent_end", subagentId: "verify", state: "completed", atMs: start + 9000 });
  }
  if (id === "research") tracker.apply({ type: "research_update", atMs: start + 10_000, research: {
    sessionId: 1, name: "render-latency", goal: "降低卡片渲染延迟", segment: 1,
    metricName: "render_ms", metricUnit: "ms", direction: "lower", baseline: 100, best: 80,
    completedRuns: 3, maxExperiments: 10, phase: "running", activeRun: 4, latestMetric: 80, totalRuns: 3,
    recent: [100, 110, 80].map((metric, i) => ({ segment: 1, runNumber: i + 1, metric, status: i === 1 ? "discard" : "keep", description: ["建立基线", "验证布局方案", "减少重复渲染"][i]!, flagged: false })),
  } });
  if (state !== "running") tracker.apply({ type: "run_end", state, atMs: start + 12_000, ...(state === "failed" ? { errorCategory: "unavailable" as const } : {}) });
  const result = buildExecutionCard(tracker.snapshot(start + 12_000), caps);
  assert(result, `Unsendable progress fixture: ${id}`);
  return { id, kind: "progress", active: true, questions: [], frame: { ...result, profile: "octo/v1" } };
}

export const uxCardCases = [
  progress("starting"), progress("running"), progress("completed", "completed"),
  progress("failed", "failed"), progress("cancelled", "cancelled"), progress("long-run"), progress("research"),
  progress("no-toggle", "running", { ...capabilities, actions: [] }),
  progress("text-only", "running", { ...capabilities, elements: ["TextBlock"], actions: [] }),
  { id: "mode", kind: "command", active: true, questions: [], frame: buildCommandCard({ kind: "mode", current: "steer", choices: [{ title: "steer · 并入当前回合", value: "steer" }, { title: "queue · 排队执行", value: "queue" }], plain: "当前追加模式：steer" }, "cfg:mode:fixture") },
  { id: "switch", kind: "command", active: true, questions: [], frame: buildCommandCard({ kind: "switch", current: "example/analysis", choices: [{ title: "default · 跟随默认模型", value: "default" }, { title: "example/analysis", value: "example/analysis" }, { title: "example/fast-response", value: "example/fast-response" }], plain: "当前模型：example/analysis" }, "cfg:switch:fixture") },
  { id: "mode-applied", kind: "command", active: false, questions: [], frame: buildCommandCardResult("mode", "追加模式已设为 queue。") },
];
