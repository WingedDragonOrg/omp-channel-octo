import { expect, test } from "bun:test";
import { DEFAULT_API_URL } from "../src/config.ts";
import type { ChatTarget } from "../src/contracts.ts";
import {
  buildExecutionCard,
  createExecutionTracker,
  type ExecutionCardCapabilities,
  type ExecutionTracker,
} from "../src/execution-card.ts";
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

type PreviewToolTerminalState = "succeeded" | "failed" | "cancelled";
type PreviewErrorCategory = "worker" | "cancelled";

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
    atMs: options.atMs,
    summary: options.summary,
  });
  if (options.status === undefined || options.endAtMs === undefined) return;
  tracker.apply({
    type: "tool_end",
    toolCallId: options.toolCallId,
    atMs: options.endAtMs,
    status: options.status,
    summary: options.summary,
    ...(options.errorCategory === undefined ? {} : { errorCategory: options.errorCategory }),
  });
}

function previewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-acceptance", initialAtMs: 0 });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: 0 });
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
    atMs: 1_360,
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
  return tracker.snapshot(1_800);
}

function compactPreviewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-budget", initialAtMs: 0 });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: 0 });
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
  return tracker.snapshot(100);
}

function terminalPreviewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-terminal", initialAtMs: 0 });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: 0 });
  addPreviewTool(tracker, {
    toolCallId: "terminal-subagent",
    toolName: "task",
    atMs: 1,
    summary: "子代理 · terminal",
  });
  tracker.apply({ type: "run_end", atMs: 200, state: "completed" });
  return tracker.snapshot(200);
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
  const todoDetail = nodeById(card, "todo_detail");
  const todoSummary = nodeById(card, "todo_hidden_summary");
  const todoOmitted = cardNodes(card).find(node => (
    typeof node.id === "string"
    && node.id.startsWith("todo_omitted_task_")
    && node.isVisible === false
  ));
  expect(todoOmitted).toBeDefined();
  if (!todoOmitted) throw new Error("hidden Todo row was not rendered");
  const preparationPhase = (todoDetail.items as JsonRecord[]).find(item => (
    item.type === "Container" && cardText(item).includes("准备阶段")
  ));
  expect(preparationPhase).toBeDefined();
  if (!preparationPhase) throw new Error("preparation phase was not rendered");
  expect(cardNodes(preparationPhase).some(node => node.id === todoOmitted.id)).toBe(true);
  expect(todoSummary.isVisible).not.toBe(false);
  expect(cardText(todoDetail)).toContain("已隐藏");
  expect(cardNodes(card).some(node => node.id === "btn_expand_todo")).toBe(true);
  expect(cardNodes(card).some(node => node.id === "btn_collapse_todo")).toBe(true);
  expect(cardNodes(card).some(node => node.id === "btn_expand_subagents")).toBe(true);
  expect(cardNodes(card).some(node => node.id === "btn_expand_trace")).toBe(true);
  expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: true });
  expect(nodeById(card, "timeline_section").isVisible).not.toBe(false);

});
test("preview rich fallback expands older trace rows without ToggleVisibility", () => {
  const built = buildExecutionCard(previewSnapshot(), {
    ...previewCapabilities,
    actions: ["Action.OpenUrl"],
  });
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  const timeline = nodeById(card, "timeline_detail");
  const rows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
  expect(rows).toHaveLength(6);
  expect(rows.every(row => row.isVisible !== false)).toBe(true);
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
  expect((trace.items as JsonRecord[]).every(item => item.type === "ColumnSet")).toBe(true);
  expect((subagents.items as JsonRecord[]).every(item => item.type === "ColumnSet")).toBe(true);
  expect(cardText(subagents)).toContain("子代理 · compact");
});

test("preview terminal cards collapse detail sections", () => {
  const built = buildExecutionCard(terminalPreviewSnapshot(), previewCapabilities);
  expect(built).not.toBeNull();
  const card = built!.card as JsonRecord;
  expect(nodeById(card, "subagent_section")).toMatchObject({ isVisible: false });
  expect(nodeById(card, "timeline_section")).toMatchObject({ isVisible: false });
  expect(nodeById(card, "btn_expand_progress")).toMatchObject({ isVisible: true });
});

const botToken = firstNonEmpty(process.env.BOT_TOKEN, process.env.OCTO_BOT_TOKEN);
const runPreviewTest = botToken === undefined ? test.skip : test;

/**
 * Opt-in live acceptance test. The filename intentionally stays outside the
 * default *.test.ts discovery set because this test sends a real Octo message.
 */
runPreviewTest(
  "sends a comprehensive live preview card with tool, subagent, and todo states",
  async () => {
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
      const receipt = await transport.sendProgressCard(
        { channelId: targetId, channelType: previewChannelType() },
        preview,
        `preview-card-${Date.now()}-${process.pid}`,
      );
      expect(receipt?.messageId).toEqual(expect.any(String));
      console.log(`综合预览卡片已发送到 ${targetId}，包含 11 次 tool 调用、9 类工具、4 个 Todo 阶段、12 个任务及成功/等待/失败/取消/进行中状态，请在 Octo 中打开最新卡片查看。`);
    } finally {
      await transport.stop();
    }
  },
  { timeout: 30_000 },
);
