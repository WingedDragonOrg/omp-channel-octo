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
  addPreviewTool(tracker, {
    toolCallId: "compact-subagent",
    toolName: "task",
    atMs: 1,
    endAtMs: 2,
    status: "succeeded",
    summary: "子代理 · compact",
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
  tracker.apply({ type: "run_end", atMs: PREVIEW_STARTED_AT + 200, state: "completed" });
  return tracker.snapshot(PREVIEW_STARTED_AT + 200);
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
  expect(visibleText).toContain("等待子代理 · verifier");
  expect(visibleText).toContain("子代理 · analyst");
  expect(visibleText).not.toContain("子代理 · reviewer");
  expect(visibleText).not.toContain("子代理 · scout");
  expect(visibleText).not.toContain("停止子代理 · cleanup");
  const hiddenSummary = nodeById(card, "subagent_hidden_summary");
  expect(hiddenSummary.isVisible).not.toBe(false);
  expect(cardText(hiddenSummary)).toContain("已隐藏 3 个子代理");
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
  expect(cardNodes(card).some(node => node.id === "btn_expand_trace")).toBe(true);
  expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: true });
  expect(nodeById(card, "timeline_section").isVisible).not.toBe(false);
  expect(cardText(nodeById(card, "timeline_detail"))).toContain("已隐藏前");
});
test("preview rich fallback expands older trace rows without ToggleVisibility", () => {
  const built = buildExecutionCard(previewSnapshot(), {
    ...previewCapabilities,
    actions: ["Action.OpenUrl"],
  });
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const timeline = nodeById(card, "timeline_detail");
  const items = timeline.items as JsonRecord[];
  const richRows = items.filter(item => item.type === "ColumnSet");
  const olderRows = items.filter(item => (
    item.type === "TextBlock"
    && typeof item.id === "string"
    && item.id.startsWith("timeline_older_step_")
  ));
  expect(richRows.length).toBeGreaterThan(0);
  expect(richRows.every(row => row.isVisible !== false)).toBe(true);
  // Without the toggle action, hiding is pointless: older rows render as visible compact lines.
  expect(olderRows.length).toBeGreaterThan(0);
  expect(olderRows.every(row => row.isVisible !== false)).toBe(true);
  expect(olderRows.every(row => typeof row.text === "string" && row.text.includes("·"))).toBe(true);
  expect(cardText(timeline)).toContain("修改卡片布局");
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
  expect(cardText(subagents)).toContain("子代理 · compact");
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

const botToken = firstNonEmpty(process.env.BOT_TOKEN, process.env.OCTO_BOT_TOKEN);
const runPreviewTest = botToken === undefined ? test.skip : test;

async function sendLivePreviewCard(snapshot: ExecutionSnapshot, label: string): Promise<void> {
  const configuredTargetId = firstNonEmpty(process.env.OCTO_PREVIEW_TARGET_UID, process.env.OCTO_OWNER_ID);
  const transport = createOctoTransport({
    apiUrl: firstNonEmpty(process.env.OCTO_API_URL) ?? DEFAULT_API_URL,
    botToken: botToken!,
  });
  try {
    const identity = await transport.initialize();
    const targetId = configuredTargetId ?? identity.ownerUid;
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
    await sendLivePreviewCard(
      preview,
      "综合预览卡片（11 次 tool 调用、9 类工具、4 个 Todo 阶段、12 个任务及成功/等待/失败/取消/进行中状态）",
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
