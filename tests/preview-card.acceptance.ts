import { expect, test } from "bun:test";
import { DEFAULT_API_URL } from "../src/config.ts";
import type { ChatTarget } from "../src/contracts.ts";
import { buildExecutionCard, type ExecutionCardCapabilities } from "../src/execution-card.ts";
import {
  createExecutionTracker,
  type ExecutionSnapshot,
  type ExecutionTracker,
} from "../src/execution-tracker.ts";
import { createOctoTransport } from "../src/octo/index.ts";
import type { ResearchRun, ResearchSnapshot } from "../src/research.ts";

function firstNonEmpty(...values: readonly (string | undefined)[]): string | undefined {
  for (const value of values) {
    const trimmed = value?.trim();
    if (trimmed) return trimmed;
  }
  return undefined;
}

function previewChannelType(): ChatTarget["channelType"] {
  switch (firstNonEmpty(process.env.OCTO_PREVIEW_CHANNEL_TYPE) ?? "1") {
    case "1":
      return 1;
    case "2":
      return 2;
    case "5":
      return 5;
    default:
      throw new Error("OCTO_PREVIEW_CHANNEL_TYPE 必须是 1、2 或 5");
  }
}

const previewCapabilities: ExecutionCardCapabilities = {
  enabled: true,
  interactive: true,
  profiles: ["octo/v1", "octo/v2"],
  cardVersion: "1.5",
  elements: ["TextBlock", "RichTextBlock", "Container", "ColumnSet", "ActionSet"],
  actions: ["Action.OpenUrl", "Action.ToggleVisibility", "Action.CopyToClipboard"],
  limits: {
    maxNodes: 200,
    maxDepth: 16,
    maxPayloadBytes: 524_288,
  },
};

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : null;
}

function walk(value: unknown, visit: (node: JsonRecord) => void): void {
  if (Array.isArray(value)) {
    for (const child of value) walk(child, visit);
    return;
  }
  const record = asRecord(value);
  if (!record) return;
  visit(record);
  for (const child of Object.values(record)) walk(child, visit);
}

function cardNodes(card: JsonRecord): JsonRecord[] {
  const result: JsonRecord[] = [];
  walk(card, node => result.push(node));
  return result;
}

function cardText(card: JsonRecord): string {
  const text: string[] = [];
  walk(card, node => {
    if (typeof node.text === "string") text.push(node.text);
  });
  return text.join("\n");
}

function nodeById(card: JsonRecord, id: string): JsonRecord {
  const node = cardNodes(card).find(candidate => candidate.id === id);
  if (!node) throw new Error(`card node ${id} was not rendered`);
  return node;
}

/** Labels of the todo rows the reader actually sees, in card order. */
function visibleTodoTaskLabels(card: JsonRecord): string[] {
  const todoDetail = cardNodes(card).find(node => node.id === "todo_detail");
  if (!todoDetail) return [];
  const labels: string[] = [];
  for (const row of cardNodes(todoDetail)) {
    if (row.type !== "ColumnSet" || row.isVisible === false) continue;
    const columns = row.columns as JsonRecord[] | undefined;
    if (columns?.[0]?.width !== "80px") continue;
    const body = (columns[1]?.items as JsonRecord[] | undefined)?.[0];
    const text = typeof body?.text === "string" ? body.text : cardText(body ?? {});
    labels.push(text);
  }
  return labels;
}

type PreviewToolTerminalState = "succeeded" | "failed" | "cancelled";
type PreviewErrorCategory = "worker" | "cancelled";

/** The live preview shows real wall-clock step times, so the fixture replays on a recent clock. */
const PREVIEW_STARTED_AT = Date.now() - 120_000;

function addPreviewTool(
  tracker: ExecutionTracker,
  options: {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly atMs: number;
    readonly summary: string;
    readonly status?: PreviewToolTerminalState;
    readonly endAtMs?: number;
    readonly errorCategory?: PreviewErrorCategory;
  },
): void {
  tracker.apply({
    type: "tool_start",
    toolCallId: options.toolCallId,
    toolName: options.toolName,
    atMs: PREVIEW_STARTED_AT + options.atMs,
    summary: options.summary,
  });
  if (options.status === undefined || options.endAtMs === undefined) return;
  tracker.apply({
    type: "tool_end",
    toolCallId: options.toolCallId,
    atMs: PREVIEW_STARTED_AT + options.endAtMs,
    status: options.status,
    summary: options.summary,
    ...(options.errorCategory === undefined ? {} : { errorCategory: options.errorCategory }),
  });
}

function addPreviewSubagent(
  tracker: ExecutionTracker,
  options: {
    readonly subagentId: string;
    readonly agent: string;
    readonly atMs: number;
    readonly summary: string;
    readonly endAtMs?: number;
    readonly state?: "completed" | "failed" | "cancelled";
  },
): void {
  tracker.apply({
    type: "subagent_start",
    subagentId: options.subagentId,
    agent: options.agent,
    state: "running",
    atMs: PREVIEW_STARTED_AT + options.atMs,
    summary: options.summary,
  });
  if (options.state === undefined || options.endAtMs === undefined) return;
  tracker.apply({
    type: "subagent_end",
    subagentId: options.subagentId,
    state: options.state,
    atMs: PREVIEW_STARTED_AT + options.endAtMs,
    summary: options.summary,
  });
}

/** One aggregated `subagent_progress` frame: the child's reported activity while it runs. */
function reportSubagentProgress(
  tracker: ExecutionTracker,
  options: {
    readonly subagentId: string;
    readonly atMs: number;
    readonly summary: string;
    readonly currentTool?: string;
    readonly toolCount?: number;
  },
): void {
  tracker.apply({
    type: "subagent_update",
    subagentId: options.subagentId,
    state: "running",
    atMs: PREVIEW_STARTED_AT + options.atMs,
    summary: options.summary,
    ...(options.currentTool === undefined ? {} : { currentTool: options.currentTool }),
    ...(options.toolCount === undefined ? {} : { toolCount: options.toolCount }),
  });
}

function previewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-acceptance", initialAtMs: PREVIEW_STARTED_AT });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: PREVIEW_STARTED_AT });
  addPreviewTool(tracker, {
    toolCallId: "preview-read",
    toolName: "read",
    atMs: 100,
    endAtMs: 220,
    status: "succeeded",
    summary: "读取项目配置",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-write",
    toolName: "write",
    atMs: 260,
    endAtMs: 380,
    status: "succeeded",
    summary: "写入预览数据",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-edit",
    toolName: "edit",
    atMs: 420,
    endAtMs: 540,
    status: "failed",
    errorCategory: "worker",
    summary: "修改卡片布局",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-bash",
    toolName: "bash",
    atMs: 580,
    endAtMs: 700,
    status: "cancelled",
    errorCategory: "cancelled",
    summary: "bun run typecheck && bun test tests/core-presentation.test.ts",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-old-task",
    toolName: "task",
    atMs: 740,
    endAtMs: 860,
    status: "failed",
    errorCategory: "worker",
    summary: "子代理 · reviewer",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-await",
    toolName: "subagent_await",
    atMs: 900,
    summary: "等待子代理 · verifier",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-task",
    toolName: "task",
    atMs: 980,
    endAtMs: 1_120,
    status: "succeeded",
    summary: "子代理 · scout",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-stop",
    toolName: "subagent_stop",
    atMs: 1_140,
    endAtMs: 1_180,
    status: "cancelled",
    errorCategory: "cancelled",
    summary: "停止子代理 · cleanup",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-running-task",
    toolName: "task",
    atMs: 1_200,
    summary: "子代理 · analyst",
  });
  // The subagent section is driven by the real lifecycle frames; the `task` rows above are what
  // the trace records while those children run.
  addPreviewSubagent(tracker, {
    subagentId: "preview-reviewer",
    agent: "reviewer",
    atMs: 740,
    endAtMs: 860,
    state: "failed",
    summary: "复核变更",
  });
  addPreviewSubagent(tracker, {
    subagentId: "preview-verifier",
    agent: "verifier",
    atMs: 900,
    summary: "验证实现",
  });
  reportSubagentProgress(tracker, {
    subagentId: "preview-verifier",
    atMs: 940,
    summary: "等待授权后继续",
    currentTool: "read",
    toolCount: 2,
  });
  addPreviewSubagent(tracker, {
    subagentId: "preview-scout",
    agent: "scout",
    atMs: 980,
    endAtMs: 1_120,
    state: "completed",
    summary: "统计源码",
  });
  addPreviewSubagent(tracker, {
    subagentId: "preview-cleanup",
    agent: "cleanup",
    atMs: 1_140,
    endAtMs: 1_180,
    state: "cancelled",
    summary: "停止子代理",
  });
  addPreviewSubagent(tracker, {
    subagentId: "preview-analyst",
    agent: "analyst",
    atMs: 1_200,
    summary: "分析执行轨迹",
  });
  reportSubagentProgress(tracker, {
    subagentId: "preview-analyst",
    atMs: 1_260,
    summary: "汇总工具样式",
    currentTool: "bash",
    toolCount: 3,
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-hub-send",
    toolName: "hub",
    atMs: 1_160,
    endAtMs: 1_300,
    status: "failed",
    errorCategory: "worker",
    summary: "IRC → preview-agent · 未收到回复",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-ask",
    toolName: "ask",
    atMs: 1_340,
    summary: "等待用户回答 · 2 个问题",
  });
  tracker.apply({
    type: "todo_update",
    atMs: PREVIEW_STARTED_AT + 1_360,
    op: "start",
    phases: [
      {
        name: "准备阶段",
        tasks: [
          { content: "读取项目配置", status: "completed" },
          { content: "确认卡片能力", status: "completed" },
          { content: "清理旧预览数据", status: "abandoned" },
        ],
      },
      {
        name: "执行阶段",
        tasks: [
          { content: "启动子代理检查", status: "completed" },
          { content: "汇总混合工具样式", status: "in_progress" },
          { content: "等待授权后继续", status: "blocked", blocker: "等待用户确认" },
        ],
      },
      {
        name: "验收阶段",
        tasks: [
          { content: "验证成功状态", status: "completed" },
          { content: "验证等待状态", status: "pending" },
          { content: "验证失败状态", status: "in_progress" },
        ],
      },
      {
        name: "交付阶段",
        tasks: [
          { content: "发送真实预览", status: "in_progress" },
          { content: "确认移动端布局", status: "cancelled" },
          { content: "记录验收结果", status: "pending" },
        ],
      },
    ],
  });
  return tracker.snapshot(PREVIEW_STARTED_AT + 1_800);
}

function compactPreviewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-budget", initialAtMs: PREVIEW_STARTED_AT });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: PREVIEW_STARTED_AT });
  addPreviewSubagent(tracker, {
    subagentId: "compact-child",
    agent: "compact",
    atMs: 1,
    endAtMs: 2,
    state: "completed",
    summary: "压缩检查",
  });
  addPreviewTool(tracker, {
    toolCallId: "compact-trace",
    toolName: "read",
    atMs: 2,
    endAtMs: 3,
    status: "succeeded",
    summary: "旧轨迹",
  });
  for (let index = 0; index < 32; index += 1) {
    addPreviewTool(tracker, {
      toolCallId: `compact-visible-${index}`,
      toolName: "read",
      atMs: 10 + index,
      endAtMs: 11 + index,
      status: "succeeded",
      summary: `可见步骤 ${index}`,
    });
  }
  return tracker.snapshot(PREVIEW_STARTED_AT + 100);
}

function terminalPreviewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-terminal", initialAtMs: PREVIEW_STARTED_AT });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: PREVIEW_STARTED_AT });
  addPreviewTool(tracker, {
    toolCallId: "terminal-subagent",
    toolName: "task",
    atMs: 1,
    summary: "子代理 · terminal",
  });
  addPreviewSubagent(tracker, {
    subagentId: "terminal-child",
    agent: "terminal",
    atMs: 1,
    summary: "终态检查",
  });
  tracker.apply({ type: "run_end", atMs: PREVIEW_STARTED_AT + 200, state: "completed" });
  return tracker.snapshot(PREVIEW_STARTED_AT + 200);
}

// ——— autoresearch 迭代卡预览夹具 ———
// 与 tests/execution-card.test.ts 相同的共享研究类型；经真实 tracker.apply 投递
// research_update 帧生成本地预览快照。

function previewResearchRun(overrides: Partial<ResearchRun> & { readonly runNumber: number | null }): ResearchRun {
  return {
    segment: 1,
    metric: 800,
    status: "keep",
    description: typeof overrides.runNumber === "number" ? `实验 ${overrides.runNumber}` : "旧版本记录",
    flagged: false,
    ...overrides,
  };
}

/**
 * Nine recorded runs in one segment with honest aggregates: baseline is the first unflagged
 * keep, best the directional extreme of the unflagged keeps, and the flagged run (#8) is what
 * keeps best at 640 despite its lower metric. Improvement over the baseline is exactly 20%.
 * The leading legacy row predates run numbering and keeps its null run number.
 */
function makePreviewResearch(overrides: Partial<ResearchSnapshot> = {}): ResearchSnapshot {
  const recent: ResearchRun[] = [
    // The legacy row leads: old records predate run numbering and keep their null number.
    previewResearchRun({ runNumber: null, metric: 835, status: "discard", description: "旧版本丢弃记录" }),
    previewResearchRun({ runNumber: 1, metric: 800, description: "建立基线测量" }),
    previewResearchRun({ runNumber: 2, metric: 780, description: "减少序列化拷贝" }),
    previewResearchRun({ runNumber: 3, metric: 910, status: "discard", description: "验证激进缓存" }),
    previewResearchRun({ runNumber: 4, metric: 0, status: "crash", description: "基准进程崩溃退出" }),
    previewResearchRun({ runNumber: 5, metric: 700, description: "增加索引" }),
    previewResearchRun({ runNumber: 6, metric: 640, description: "复用连接池" }),
    previewResearchRun({ runNumber: 7, metric: 645, status: "checks_failed", description: "通过基准但检查失败" }),
    previewResearchRun({ runNumber: 8, metric: 620, description: "复测连接池配置", flagged: true }),
  ];
  return {
    sessionId: 7,
    name: "p95-latency",
    goal: "把 API P95 延迟降到 400ms 以下",
    segment: 1,
    metricName: "p95",
    metricUnit: "ms",
    direction: "lower",
    baseline: 800,
    best: 640,
    completedRuns: 9,
    maxExperiments: 50,
    phase: "running",
    activeRun: 10,
    latestMetric: 620,
    recent,
    totalRuns: 9,
    ...overrides,
  };
}

/** The worker's research frame, delivered to the tracker as an ExecutionEvent by the RPC bridge. */
function applyResearchUpdate(tracker: ExecutionTracker, research: unknown, atMs: number): void {
  // 预览夹具始终喂合法载荷;断言跨过 unknown,真正的运行时校验由 tracker 内的 sanitize 执行。
  tracker.apply({
    type: "research_update",
    atMs,
    research: research as ResearchSnapshot,
  });
}

/** Status glyph leading every compact run row; free of any locale wording. */
const RESEARCH_ROW_TEXT = /^[✓×-] · /u;

function researchBandItems(card: JsonRecord): JsonRecord[] {
  const band = cardNodes(card).find(node => node.id === "research_recent_runs");
  if (!band) return [];
  return (band.items as JsonRecord[] | undefined) ?? [];
}

/** Rows of the recent-run window, identified by their status glyph rather than any wording. */
function researchRunRows(card: JsonRecord): JsonRecord[] {
  return researchBandItems(card).filter(item => (
    item.type === "TextBlock" && typeof item.text === "string" && RESEARCH_ROW_TEXT.test(item.text)
  ));
}

/** The omitted-count figure, read as a bare number wherever the card carries it. */
function omittedResearchCount(card: JsonRecord): number | null {
  const band = cardNodes(card).find(node => node.id === "research_recent_runs");
  if (band) {
    const [first] = researchBandItems(card);
    if (!first || (typeof first.text === "string" && RESEARCH_ROW_TEXT.test(first.text))) return 0;
    const match = typeof first.text === "string" ? first.text.match(/\d+/u) : null;
    return match === null ? null : Number(match[0]);
  }
  const line = cardNodes(card).find(node => node.id === "research_recent_summary");
  if (!line) return null;
  const text = typeof line.text === "string" ? line.text : cardText(line);
  const match = text.match(/\d+/u);
  return match === null ? null : Number(match[0]);
}

/**
 * A live research loop mid-run: the baseline benchmark finished, round 10 is executing, and nine
 * recorded runs wait behind the recent-run toggle. The oldest kept record has no run number
 * (legacy row), the newest window shows six rows, and three are omitted.
 */
function researchPreviewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-research", initialAtMs: PREVIEW_STARTED_AT });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: PREVIEW_STARTED_AT });
  addPreviewTool(tracker, {
    toolCallId: "research-baseline",
    toolName: "bash",
    atMs: 100,
    endAtMs: 420,
    status: "succeeded",
    summary: "autoresearch 基线基准",
  });
  addPreviewTool(tracker, {
    toolCallId: "research-experiment",
    toolName: "bash",
    atMs: 500,
    summary: "运行实验基准 · 第 10 轮",
  });
  applyResearchUpdate(tracker, makePreviewResearch(), 600);
  return tracker.snapshot(PREVIEW_STARTED_AT + 700);
}

/** Wall clock for the long-run fixture: the reported card had run for 14m 35s. */
const LONG_RUN_STARTED_AT = Date.now() - 14 * 60_000 - 35_000;

/**
 * The reported card: 96 turns, 116 tool calls, and the todo list from the screenshot, where the
 * budget had left two rows under Hardening and bare titles for Tests, Docs, and Verification.
 */
function longRunPreviewSnapshot(): ExecutionSnapshot {
  const tracker = createExecutionTracker({ runId: "preview-card-long-run", initialAtMs: LONG_RUN_STARTED_AT });
  for (let turn = 0; turn < 96; turn += 1) {
    const atMs = LONG_RUN_STARTED_AT + turn * 9_000;
    tracker.apply({ type: "turn_start", turnId: `turn-${turn}`, atMs });
    tracker.apply({ type: "turn_end", turnId: `turn-${turn}`, atMs: atMs + 4_000 });
  }
  for (let index = 0; index < 116; index += 1) {
    const toolCallId = `step-${index}`;
    const atMs = LONG_RUN_STARTED_AT + 1_000 + index * 7_500;
    tracker.apply({
      type: "tool_start",
      toolCallId,
      toolName: index % 3 === 0 ? "bash" : index % 3 === 1 ? "read" : "edit",
      atMs,
      summary: `步骤 ${index} · src/octo/index.ts`,
    });
    tracker.apply({ type: "tool_end", toolCallId, atMs: atMs + 1_200, status: "succeeded" });
  }
  tracker.apply({
    type: "todo_update",
    atMs: LONG_RUN_STARTED_AT + 840_000,
    op: "start",
    phases: [
      {
        name: "Hardening",
        tasks: [
          { content: "Transport: error code + fresh-connection option", status: "in_progress" },
          { content: "Service: bounded retry on transient delete failure", status: "pending" },
        ],
      },
      {
        name: "Tests",
        tasks: [
          { content: "Cover the new retry path", status: "pending" },
          { content: "Re-run lifecycle suite", status: "pending" },
        ],
      },
      {
        name: "Docs",
        tasks: [{ content: "Record the option in the design spec", status: "pending" }],
      },
      {
        name: "Verification",
        tasks: [
          { content: "Send the live preview card", status: "pending" },
          { content: "Confirm on the real client", status: "pending" },
        ],
      },
    ],
  });
  return tracker.snapshot(LONG_RUN_STARTED_AT + 875_000);
}

test("preview snapshot shows every subagent state by default", () => {
  const built = buildExecutionCard(previewSnapshot(), previewCapabilities);
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const subagentDetail = nodeById(card, "subagent_detail");
  const rows = (subagentDetail.items as JsonRecord[]).filter(
    item => item.type === "ColumnSet" && item.isVisible !== false,
  );
  expect(rows).toHaveLength(2);
  const visibleText = rows.map(row => cardText(row)).join("\n");
  expect(visibleText).toContain("verifier");
  expect(visibleText).toContain("等待授权后继续");
  expect(visibleText).toContain("进行中");
  expect(visibleText).toContain("read");
  expect(visibleText).toContain("2 次调用");
  expect(visibleText).toContain("analyst");
  expect(visibleText).toContain("汇总工具样式");
  expect(visibleText).toContain("bash");
  expect(visibleText).toContain("3 次调用");
  // State and tool activity only: a subagent row carries no per-child clock or duration.
  expect(visibleText).not.toMatch(/\d{2}:\d{2}:\d{2}/u);
  expect(visibleText).not.toMatch(/\d+ms|\d+\.\d+s/u);
  expect(visibleText).not.toContain("reviewer");
  expect(visibleText).not.toContain("scout");
  expect(visibleText).not.toContain("cleanup");
  const hiddenSummary = nodeById(card, "subagent_hidden_summary");
  expect(hiddenSummary.isVisible).not.toBe(false);
  expect(cardText(hiddenSummary)).toContain("已隐藏 3 个子代理，共 5 个子代理");
  // The todo list is the run's contract and stays whole; the execution trace is what folded away.
  const todoDetail = nodeById(card, "todo_detail");
  expect(cardText(todoDetail)).not.toContain("已隐藏");
  expect(cardNodes(card).some(node => (
    typeof node.id === "string" && node.id.startsWith("todo_omitted_task_")
  ))).toBe(false);
  expect(visibleTodoTaskLabels(card)).toEqual([
    "读取项目配置",
    "确认卡片能力",
    "清理旧预览数据",
    "启动子代理检查",
    "汇总混合工具样式",
    "等待授权后继续 · 等待用户确认",
    "验证成功状态",
    "验证等待状态",
    "验证失败状态",
    "发送真实预览",
    "确认移动端布局",
    "记录验收结果",
  ]);
  expect(cardNodes(card).some(node => node.id === "btn_expand_todo")).toBe(false);
  expect(cardNodes(card).some(node => node.id === "btn_expand_subagents")).toBe(true);
  // 11 tool calls against a 200-node budget: the trace sheds its own rows first, so whether its
  // expand control survives depends on how much detail the fit search had left.
  expect(nodeById(card, "timeline_detail")).toBeDefined();
  expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: true });
  expect(nodeById(card, "timeline_section").isVisible).not.toBe(false);
  expect(cardText(nodeById(card, "timeline_detail"))).toContain("已隐藏前");
});
test("preview rich fallback keeps the compact history band readable without ToggleVisibility", () => {
  const built = buildExecutionCard(previewSnapshot(), {
    ...previewCapabilities,
    actions: ["Action.OpenUrl"],
  });
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const timeline = nodeById(card, "timeline_detail");
  const richRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
  expect(richRows.length).toBeGreaterThan(0);
  expect(richRows.every(row => row.isVisible !== false)).toBe(true);
  // Without the toggle action, hiding is pointless: the trimmed rows render as visible compact
  // lines inside the section's grey band.
  const traceBand = nodeById(card, "timeline_omitted_steps");
  expect(traceBand).toMatchObject({ type: "Container", style: "emphasis" });
  expect(traceBand.isVisible).not.toBe(false);
  const bandRows = traceBand.items as JsonRecord[];
  expect(bandRows.length).toBeGreaterThan(0);
  expect(bandRows.every(row => (
    row.type === "TextBlock" && row.wrap === true && typeof row.text === "string" && row.text.includes("·")
  ))).toBe(true);
  expect(cardText(timeline)).toContain("已隐藏前");
  expect(cardText(traceBand)).toContain("修改卡片布局");
  expect(cardNodes(card).some(node => node.id === "btn_expand_trace")).toBe(false);
  expect(cardNodes(card).some(node => node.id === "btn_expand_subagents")).toBe(false);
});

test("preview flat fallback keeps all tool and subagent summaries", () => {
  const built = buildExecutionCard(previewSnapshot(), {
    ...previewCapabilities,
    elements: ["TextBlock"],
    actions: ["Action.OpenUrl"],
  });
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  expect(cardNodes(card).some(node => node.id === "subagent_detail")).toBe(false);
  const text = cardText(card);
  expect(text).toContain("子代理 · reviewer");
  expect(text).toContain("等待子代理 · verifier");
  expect(text).toContain("停止子代理 · cleanup");
  expect(text).toContain("子代理 · analyst");
});

test("preview budget fallback exposes compact trace and subagent groups", () => {
  const built = buildExecutionCard(compactPreviewSnapshot(), {
    ...previewCapabilities,
    limits: { maxNodes: 1_000, maxDepth: 16, maxPayloadBytes: 1_000_000 },
  });
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const trace = nodeById(card, "timeline_omitted_steps");
  const subagents = nodeById(card, "subagent_omitted_steps");
  for (const band of [trace, subagents]) {
    const rows = band.items as JsonRecord[];
    expect(rows.every(item => item.type === "TextBlock" && item.wrap === true)).toBe(true);
    expect(rows.every(item => typeof item.text === "string" && item.text.startsWith("✓\u00a0· "))).toBe(true);
  }
  expect(cardText(subagents)).toContain("compact");
  expect(cardText(subagents)).toContain("压缩检查");
  expect(cardText(trace)).toContain("旧轨迹");
});

test("preview terminal cards collapse detail sections", () => {
  const built = buildExecutionCard(terminalPreviewSnapshot(), previewCapabilities);
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  expect(nodeById(card, "subagent_section")).toMatchObject({ isVisible: false });
  expect(nodeById(card, "timeline_section")).toMatchObject({ isVisible: false });
  expect(nodeById(card, "btn_expand_progress")).toMatchObject({ isVisible: true });
});

test("preview long run keeps the todo handoff and compresses the trace", () => {
  const built = buildExecutionCard(longRunPreviewSnapshot(), previewCapabilities);
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  expect(visibleTodoTaskLabels(card)).toEqual([
    "Transport: error code + fresh-connection option",
    "Service: bounded retry on transient delete failure",
    "Cover the new retry path",
    "Re-run lifecycle suite",
    "Record the option in the design spec",
    "Send the live preview card",
    "Confirm on the real client",
  ]);
  const phaseContainers = (nodeById(card, "todo_detail").items as JsonRecord[])
    .filter(item => item.type === "Container");
  expect(phaseContainers).toHaveLength(4);
  for (const phase of phaseContainers) {
    expect(cardText(phase)).toContain("\n");
    expect(cardNodes(phase).some(node => node.type === "ColumnSet")).toBe(true);
  }
  const timeline = nodeById(card, "timeline_detail");
  expect(cardText(timeline)).toContain("已隐藏前");
  const richRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
  expect(richRows.length).toBeGreaterThan(0);
  expect(richRows.map(row => cardText(row)).join("\n")).toMatch(/\d{2}:\d{2}:\d{2} · 已完成/u);
});

test("preview research card keeps the summary visible and folds recent runs independently", () => {
  const built = buildExecutionCard(researchPreviewSnapshot(), previewCapabilities);
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const summary = nodeById(card, "research_summary");
  expect(summary.isVisible).not.toBe(false);
  const text = cardText(summary);
  expect(text).toContain("把 API P95 延迟降到 400ms 以下");
  expect(text).toContain("p95");
  expect(text).toMatch(/\b800\b/u);
  expect(text).toMatch(/\b640\b/u);
  expect(text).toMatch(/20(?:\.0*)?%/u);
  expect(text).toMatch(/\b9\s*\/\s*50\b/u);
  expect(text).toContain("/autoresearch-history");

  // Nine recorded runs: the newest six stay behind the research fold; the folded line and the
  // omitted count are read as bare numbers, whatever the wording around them.
  const foldedDigits = cardText(nodeById(card, "research_recent_summary")).match(/\d+/u);
  expect(foldedDigits).not.toBeNull();
  expect(Number(foldedDigits![0])).toBe(6);
  const band = nodeById(card, "research_recent_runs");
  expect(band.isVisible).toBe(false);
  const bandText = cardText(band);
  expect(researchRunRows(card)).toHaveLength(6);
  expect(omittedResearchCount(card)).toBe(3);
  expect(bandText).toMatch(/\b640\b/u);
  expect(bandText).toContain("复用连接池");
  expect(bandText).toContain("复测连接池配置");
  expect(bandText).toMatch(/标|flagged/iu);
  // The window is the newest six: the baseline pair and the legacy row stay out of it.
  expect(bandText).not.toContain("建立基线测量");
  expect(bandText).not.toContain("旧版本丢弃记录");

  // The research fold is its own control: the running card shows both, each with its own ids.
  expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: true });
  expect(nodeById(card, "btn_expand_research")).toMatchObject({ isVisible: true });
  expect(nodeById(card, "timeline_section").isVisible).not.toBe(false);
  expect(cardText(nodeById(card, "timeline_detail"))).toContain("autoresearch 基线基准");
});

const botToken = firstNonEmpty(process.env.BOT_TOKEN, process.env.OCTO_BOT_TOKEN);
const runPreviewTest = botToken === undefined ? test.skip : test;

async function sendLivePreviewCard(snapshot: ExecutionSnapshot, label: string): Promise<void> {
  const configuredTargetId = firstNonEmpty(process.env.OCTO_PREVIEW_TARGET_UID, process.env.OCTO_OWNER_ID);
  const transport = createOctoTransport({
    apiUrl: firstNonEmpty(process.env.OCTO_API_URL) ?? DEFAULT_API_URL,
    botToken: botToken!,
  });
  try {
    // 常驻服务在场时注册会踢掉它的 WS 连接；目标已配置（或经 OCTO_OWNER_ID 已知）就直接发卡。
    // sendProgressCard 自取卡片能力，不依赖 initialize。仅在目标未知时才注册换取 owner UID。
    const targetId = configuredTargetId ?? (await transport.initialize()).ownerUid;
    if (!targetId) {
      throw new Error("注册响应未提供 owner UID；请设置 OCTO_PREVIEW_TARGET_UID（或 OCTO_OWNER_ID）");
    }
    const receipt = await transport.sendProgressCard(
      { channelId: targetId, channelType: previewChannelType() },
      snapshot,
      `preview-card-${Date.now()}-${process.pid}`,
    );
    expect(receipt?.messageId).toEqual(expect.any(String));
    console.log(`${label} 已发送到 ${targetId}，请在 Octo 中打开最新卡片查看。`);
  } finally {
    await transport.stop();
  }
}

/**
 * Opt-in live acceptance test. The filename intentionally stays outside the
 * default *.test.ts discovery set because this test sends a real Octo message.
 */
runPreviewTest(
  "sends a comprehensive live preview card with tool, subagent, and todo states",
  async () => {
    const preview = previewSnapshot();
    expect(preview.tools.map(tool => tool.toolName)).toEqual([
      "read",
      "write",
      "edit",
      "bash",
      "task",
      "subagent_await",
      "task",
      "subagent_stop",
      "task",
      "hub",
      "ask",
    ]);
    expect(preview.tools.map(tool => tool.state)).toEqual([
      "succeeded",
      "succeeded",
      "failed",
      "cancelled",
      "failed",
      "running",
      "succeeded",
      "cancelled",
      "running",
      "failed",
      "running",
    ]);
    expect(preview.todos).toHaveLength(4);
    expect(preview.todos.reduce((count, phase) => count + phase.tasks.length, 0)).toBe(12);
    expect(new Set(preview.todos.flatMap(phase => phase.tasks.map(task => task.status)))).toEqual(new Set([
      "completed",
      "in_progress",
      "pending",
      "blocked",
      "abandoned",
      "cancelled",
    ]));
    expect(preview.subagents.map(subagent => [subagent.agent, subagent.state])).toEqual([
      ["reviewer", "failed"],
      ["verifier", "running"],
      ["scout", "completed"],
      ["cleanup", "cancelled"],
      ["analyst", "running"],
    ]);
    await sendLivePreviewCard(
      preview,
      "综合预览卡片（11 次 tool 调用、9 类工具、5 个子代理事件、4 个 Todo 阶段、12 个任务及成功/等待/失败/取消/进行中状态）",
    );
  },
  { timeout: 30_000 },
);

runPreviewTest(
  "sends the long-run preview card that keeps the todo handoff readable",
  async () => {
    const preview = longRunPreviewSnapshot();
    expect(preview.phaseCount).toBe(96);
    expect(preview.toolCount).toBe(116);
    expect(preview.todos.reduce((count, phase) => count + phase.tasks.length, 0)).toBe(7);
    await sendLivePreviewCard(
      preview,
      "长任务预览卡片（96 个阶段、116 次工具调用、截图中的任务清单；轨迹行带 HH:mm:ss）",
    );
  },
  { timeout: 30_000 },
);

runPreviewTest(
  "sends the autoresearch progress card with summary and folded recent runs",
  async () => {
    const preview = researchPreviewSnapshot();
    expect(preview.toolCount).toBe(2);
    // 断言先行：研究卡面缺失时在发送前即失败，未完成实现不会发出半截卡片。
    const built = buildExecutionCard(preview, previewCapabilities);
    expect(built).not.toBeNull();
    const card = built!.card as JsonRecord;
    const summary = cardText(nodeById(card, "research_summary"));
    expect(summary).toContain("把 API P95 延迟降到 400ms 以下");
    expect(summary).toMatch(/\b640\b/u);
    expect(summary).toContain("/autoresearch-history");
    const band = nodeById(card, "research_recent_runs");
    expect(band.isVisible).toBe(false);
    expect(cardText(band)).toContain("复用连接池");
    expect(omittedResearchCount(card)).toBe(3);
    await sendLivePreviewCard(
      preview,
      "研究进度预览卡片（目标与指标摘要、基线 800 最佳 640 改善 20%、运行中第 10 轮、最近 6 条折叠省略 3 条、完整记录入口 /autoresearch-history）",
    );
  },
  { timeout: 30_000 },
);
