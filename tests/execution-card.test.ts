import { describe, expect, test } from "bun:test";
import {
  buildExecutionCard,
  createExecutionTracker,
  type ExecutionCardCapabilities,
  type ExecutionEvent,
  type ExecutionState,
  type ExecutionTracker,
} from "../src/execution-card.ts";

type JsonRecord = Record<string, unknown>;

const fullCapabilities: ExecutionCardCapabilities = {
  enabled: true,
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

function asRecord(value: unknown): JsonRecord | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonRecord)
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

function nodes(card: JsonRecord): JsonRecord[] {
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
  const node = nodes(card).find(candidate => candidate.id === id);
  if (!node) throw new Error(`card node ${id} was not rendered`);
  return node;
}

function toggleActionById(card: JsonRecord, id: string): JsonRecord {
  const action = nodes(card).find(candidate => candidate.type === "Action.ToggleVisibility" && candidate.id === id);
  if (!action) throw new Error(`toggle action ${id} was not rendered`);
  return action;
}

function targetElements(action: JsonRecord): JsonRecord[] {
  if (!Array.isArray(action.targetElements)) throw new Error("toggle action has no targetElements");
  return action.targetElements.map(target => {
    const record = asRecord(target);
    if (!record || typeof record.elementId !== "string" || typeof record.isVisible !== "boolean") {
      throw new Error("toggle target is not a complete element visibility object");
    }
    return record;
  });
}

function cardStats(value: unknown, depth = 0): { nodes: number; maxDepth: number } {
  if (Array.isArray(value)) {
    return value.reduce(
      (stats, child) => {
        const childStats = cardStats(child, depth);
        return {
          nodes: stats.nodes + childStats.nodes,
          maxDepth: Math.max(stats.maxDepth, childStats.maxDepth),
        };
      },
      { nodes: 0, maxDepth: depth },
    );
  }
  const record = asRecord(value);
  if (!record) return { nodes: 0, maxDepth: depth };
  let result = { nodes: 1, maxDepth: depth + 1 };
  for (const child of Object.values(record)) {
    const childStats = cardStats(child, depth + 1);
    result = {
      nodes: result.nodes + childStats.nodes,
      maxDepth: Math.max(result.maxDepth, childStats.maxDepth),
    };
  }
  return result;
}

function payloadBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function startTurn(tracker: ExecutionTracker, turnId: string, atMs: number): void {
  tracker.apply({ type: "turn_start", turnId, atMs });
}

function startTool(
  tracker: ReturnType<typeof createExecutionTracker>,
  toolCallId: string,
  toolName: string,
  atMs: number,
  summary?: string,
): void {
  const event: ExecutionEvent = {
    type: "tool_start",
    toolCallId,
    toolName,
    atMs,
    ...(summary === undefined ? {} : { summary }),
  };
  tracker.apply(event);
}

describe("execution tracker", () => {
  test("projects turn phases, tool counts, elapsed time, and terminal state", () => {
    const tracker = createExecutionTracker({ runId: "run-1" });

    startTurn(tracker, "turn-1", 1_000);
    tracker.apply({
      type: "tool_start",
      toolCallId: "call-1",
      toolName: "read",
      atMs: 1_100,
      summary: "src/index.ts",
    });

    const running = tracker.snapshot(1_200);
    expect(running).toMatchObject({
      runId: "run-1",
      state: "running",
      startedAtMs: 1_000,
      endedAtMs: null,
      elapsedMs: 200,
      phaseCount: 1,
      toolCount: 1,
    });
    expect(running.tools).toEqual([
      expect.objectContaining({
        toolCallId: "call-1",
        toolName: "read",
        state: "running",
        startedAtMs: 1_100,
        endedAtMs: null,
        durationMs: 100,
      }),
    ]);

    tracker.apply({
      type: "tool_end",
      toolCallId: "call-1",
      atMs: 1_350,
      status: "succeeded",
      summary: "12 行",
    });
    tracker.apply({ type: "turn_end", turnId: "turn-1", atMs: 1_400 });

    // turn_end closes one phase, not the run. Continuation turns still belong
    // to this execution and must increase the visible phase count.
    expect(tracker.snapshot(1_450)).toMatchObject({ state: "running", phaseCount: 1 });
    startTurn(tracker, "turn-2", 1_500);
    expect(tracker.snapshot(1_550)).toMatchObject({ state: "running", phaseCount: 2, elapsedMs: 550 });

    const terminal = tracker.apply({
      type: "run_end",
      atMs: 1_600,
      state: "completed",
    });
    expect(terminal).toMatchObject({
      runId: "run-1",
      state: "completed",
      startedAtMs: 1_000,
      endedAtMs: 1_600,
      elapsedMs: 600,
      phaseCount: 2,
      toolCount: 1,
    });
    expect(terminal.tools[0]).toMatchObject({
      toolCallId: "call-1",
      state: "succeeded",
      endedAtMs: 1_350,
      durationMs: 250,
      summary: "12 行",
    });
  });

  test("matches parallel tool starts and ends by toolCallId without crossing durations or states", () => {
    const tracker = createExecutionTracker({ runId: "parallel" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-a", "read", 10, "a.txt");
    startTool(tracker, "tool-b", "grep", 20, "query scope");

    tracker.apply({
      type: "tool_end",
      toolCallId: "tool-b",
      atMs: 35,
      status: "failed",
      errorCategory: "timeout",
    });
    expect(tracker.snapshot(40).tools).toEqual([
      expect.objectContaining({
        toolCallId: "tool-a",
        toolName: "read",
        state: "running",
        startedAtMs: 10,
        endedAtMs: null,
        durationMs: 30,
        summary: "a.txt",
      }),
      expect.objectContaining({
        toolCallId: "tool-b",
        toolName: "grep",
        state: "failed",
        startedAtMs: 20,
        endedAtMs: 35,
        durationMs: 15,
        errorCategory: "timeout",
        summary: "query scope",
      }),
    ]);

    tracker.apply({
      type: "tool_end",
      toolCallId: "tool-a",
      atMs: 80,
      status: "succeeded",
      summary: "1 个文件",
    });
    expect(tracker.snapshot(80).tools).toEqual([
      expect.objectContaining({ toolCallId: "tool-a", state: "succeeded", durationMs: 70 }),
      expect.objectContaining({ toolCallId: "tool-b", state: "failed", durationMs: 15, errorCategory: "timeout" }),
    ]);
  });

  test("keeps todo state structured and only changes it from typed todo_update phases", () => {
    const tracker = createExecutionTracker({ runId: "todos" });
    startTurn(tracker, "turn-1", 0);
    const initialPhases = [
      {
        name: "Foundation",
        tasks: [
          { content: "Wire storage", status: "in_progress" as const },
          { content: "Wait for access", status: "blocked" as const, blocker: "等待授权" },
        ],
      },
      {
        name: "Verification",
        tasks: [{ content: "Run checks", status: "pending" as const }],
      },
    ];
    tracker.apply({ type: "todo_update", atMs: 10, op: "init", phases: initialPhases });

    // A todo tool's human-readable content is deliberately not a state input.
    // The typed details snapshot above is the only source of todo projection.
    startTool(tracker, "todo-call", "todo", 20, "completed: 1/3; raw result is not state");
    tracker.apply({
      type: "tool_end",
      toolCallId: "todo-call",
      atMs: 30,
      status: "succeeded",
      summary: "已完成 1/3",
    });
    const blockedSnapshot = tracker.snapshot(30);
    expect(blockedSnapshot.todos).toEqual(initialPhases);
    const blockedRendered = buildExecutionCard(blockedSnapshot, fullCapabilities);
    expect(blockedRendered).not.toBeNull();
    const blockedCard = blockedRendered!.card as JsonRecord;
    expect(cardText(blockedCard)).toContain("等待授权");
    const todoDetail = nodeById(blockedCard, "todo_detail");
    const phaseContainers = (todoDetail.items as JsonRecord[]).filter(item => item.type === "Container");
    expect(phaseContainers).toHaveLength(2);
    const foundationItems = phaseContainers[0]?.items as JsonRecord[];
    expect(foundationItems).toHaveLength(3);
    const foundationHeader = foundationItems[0]!;
    expect(foundationHeader.type).toBe("ColumnSet");
    const phaseColumns = foundationHeader.columns as JsonRecord[];
    expect(phaseColumns).toHaveLength(2);
    expect(cardText(phaseColumns[0]!)).toBe("├─");
    const phasePrefix = (phaseColumns[0]?.items as JsonRecord[])[0]!;
    expect(phasePrefix.fontType).toBe("Monospace");
    expect(cardText(phaseColumns[1]!)).toBe("Foundation");
    const foundationRows = foundationItems.slice(1);
    expect(foundationRows.every(row => row.type === "ColumnSet")).toBe(true);
    const firstTaskColumns = foundationRows[0]?.columns as JsonRecord[];
    expect(firstTaskColumns).toHaveLength(2);
    const taskPrefix = (firstTaskColumns[0]?.items as JsonRecord[])[0]!;
    expect(taskPrefix.fontType).toBe("Monospace");
    expect((firstTaskColumns[0]?.items as JsonRecord[])[0]?.text).toBe("│\u00a0├─");
    expect(cardText(firstTaskColumns[1]!)).toContain("● Wire storage");
    expect(cardText(foundationRows[1]!)).toContain("- Wait for access · 等待授权");
    expect(cardText(blockedCard)).not.toContain("│  ├─");
    expect(blockedRendered!.plain).toContain("├─ Foundation");
    expect(blockedRendered!.plain).toContain("│  ├─ ● Wire storage");
    expect(blockedRendered!.plain).toContain("│  └─ - Wait for access · 等待授权");
    expect(blockedRendered!.plain).toContain("   └─ - Run checks");

    const updatedPhases = [
      {
        name: "Foundation",
        tasks: [
          { content: "Wire storage", status: "completed" as const },
          { content: "Wait for access", status: "abandoned" as const },
        ],
      },
      {
        name: "Verification",
        tasks: [{ content: "Run checks", status: "in_progress" as const }],
      },
    ];
    tracker.apply({ type: "todo_update", atMs: 40, op: "done", phases: updatedPhases });
    expect(tracker.snapshot(40).todos).toEqual(updatedPhases);

    const rendered = buildExecutionCard(tracker.snapshot(40), fullCapabilities);
    expect(rendered).not.toBeNull();
    const text = cardText(rendered!.card as JsonRecord);
    expect(text).toContain("Wire storage");
    expect(text).toContain("Wait for access");
    expect(text).toContain("Run checks");
    expect(text).toContain("Foundation");
    expect(text).not.toContain("│  ├─");
    expect(text).not.toContain("[已完成]");
    expect(text).not.toContain("[进行中]");
    expect(text).not.toContain("[等待]");
    expect(text).not.toContain("[已阻塞]");
    expect(text).not.toContain("[已放弃]");
    expect(text).not.toContain("等待授权");
    expect(rendered!.plain).toContain("├─ Foundation");
    expect(rendered!.plain).toContain("│  ├─ ✓ Wire storage");
    expect(rendered!.plain).toContain("│  └─ × Wait for access");
    expect(rendered!.plain).toContain("   └─ ● Run checks");
  });

  test("run_end is authoritative for completed, failed, and cancelled terminal states", () => {
    const cases: Array<{ state: Exclude<ExecutionState, "running">; toolState: "succeeded" | "failed" | "cancelled" }> = [
      { state: "completed", toolState: "succeeded" },
      { state: "failed", toolState: "failed" },
      { state: "cancelled", toolState: "cancelled" },
    ];

    for (const { state, toolState } of cases) {
      const tracker = createExecutionTracker({ runId: `terminal-${state}` });
      startTurn(tracker, "turn-1", 100);
      startTool(tracker, "tool-1", "bash", 110, "bun test");
      tracker.apply({
        type: "tool_end",
        toolCallId: "tool-1",
        atMs: 120,
        status: toolState,
        ...(state === "failed" ? { errorCategory: "worker" as const } : {}),
      });
      const snapshot = tracker.apply({ type: "run_end", atMs: 130, state });
      expect(snapshot.state).toBe(state);
      expect(snapshot.endedAtMs).toBe(130);
      expect(snapshot.elapsedMs).toBe(30);
      expect(snapshot.tools[0]?.state).toBe(toolState);
    }
  });

  test("does not retain raw thinking/result fields or secret material in the projected snapshot", () => {
    const tracker = createExecutionTracker({ runId: "safe" });
    startTurn(tracker, "turn-1", 0);
    const unsafeEvent = {
      type: "tool_start",
      toolCallId: "unsafe-1",
      toolName: "bash",
      atMs: 1,
      summary: "Authorization: Bearer super-secret-token command",
      rawThinking: "SECRET_RAW_THINKING",
      rawResult: "SECRET_RAW_TOOL_RESULT",
    } as unknown as ExecutionEvent;
    tracker.apply(unsafeEvent);
    tracker.apply({
      type: "tool_end",
      toolCallId: "unsafe-1",
      atMs: 2,
      status: "failed",
      errorCategory: "permission",
    });

    const snapshot = tracker.snapshot(2);
    const rendered = buildExecutionCard(snapshot, fullCapabilities);
    expect(rendered).not.toBeNull();
    const serialized = JSON.stringify({ snapshot, card: rendered!.card, plain: rendered!.plain });
    expect(serialized).not.toContain("SECRET_RAW_THINKING");
    expect(serialized).not.toContain("SECRET_RAW_TOOL_RESULT");
    expect(serialized).not.toContain("super-secret-token");
    expect(serialized).toContain("permission");
  });
});

describe("execution card rendering", () => {
  test("renders header metrics and safe per-tool durations", () => {
    const tracker = createExecutionTracker({ runId: "metrics" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "one", "read", 100, "src/a.ts");
    tracker.apply({ type: "tool_end", toolCallId: "one", atMs: 250, status: "succeeded", summary: "12 行" });
    tracker.apply({ type: "turn_end", turnId: "turn-1", atMs: 300 });
    startTurn(tracker, "turn-2", 400);
    startTool(tracker, "two", "grep", 500, "query");
    tracker.apply({ type: "tool_end", toolCallId: "two", atMs: 900, status: "succeeded", summary: "3 项结果" });
    const snapshot = tracker.apply({ type: "run_end", atMs: 1_600, state: "completed" });
    const rendered = buildExecutionCard(snapshot, fullCapabilities);

    expect(rendered).not.toBeNull();
    const text = cardText(rendered!.card as JsonRecord);
    expect(text).toContain("2 个阶段");
    expect(text).toContain("2 次工具调用");
    expect(text).toContain("1.6s");
    expect(text).toContain("150ms");
    expect(text).toContain("400ms");
    expect(text).not.toContain("raw");
    expect(rendered!.plain).toContain("处理进度");
  });

  test("renders a progress notification with activity and todo details", () => {
    const tracker = createExecutionTracker({ runId: "progress" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "subagent-1", "task", 100, "梳理实现方案");
    tracker.apply({
      type: "todo_update",
      atMs: 120,
      op: "start",
      phases: [{
        name: "实现",
        tasks: [{ content: "更新进度通知", status: "in_progress" }],
      }],
    });
    const rendered = buildExecutionCard(tracker.snapshot(1_600), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(cardText(card)).toContain("执行轨迹");
    expect(cardText(card)).toContain("任务清单");
    expect(cardText(card)).toContain("运行子代理");
    expect(cardText(card)).toContain("梳理实现方案");
    expect(nodes(card).some(node => node.id === "execution_metrics")).toBe(false);
  });
  test("renders semantic labels for special tools in the execution trace", () => {
    const tracker = createExecutionTracker({ runId: "special-tool-rendering" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "hub-1", "hub", 1, "IRC → agent-a · 等待回复");
    startTool(tracker, "task-1", "task", 2, "子代理 · scout");
    startTool(tracker, "todo-1", "todo", 3, "任务清单 · 完成");
    startTool(tracker, "ask-1", "ask", 4, "等待用户回答 · 1 个问题");

    const rendered = buildExecutionCard(tracker.snapshot(5), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const textNodes = nodes(card);
    expect(cardText(card)).toContain("IRC → agent-a · 等待回复");
    expect(cardText(card)).toContain("子代理 · scout");
    expect(cardText(card)).toContain("任务清单 · 完成");
    expect(cardText(card)).toContain("等待用户回答 · 1 个问题");
    expect(textNodes.some(node => node.text === "IRC 协作")).toBe(true);
    expect(textNodes.some(node => node.text === "询问用户")).toBe(true);
  });

  test("renders subagent activity with expand control only when older activity exists", () => {
    const tracker = createExecutionTracker({ runId: "subagents" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 4; index += 1) {
      startTool(tracker, `subagent-${index}`, "task", 100 + index, `子代理步骤 ${index + 1}`);
    }
    startTool(tracker, "tool-1", "read", 200, "src/service.ts");
    const rendered = buildExecutionCard(tracker.snapshot(1_600), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const subagents = nodeById(card, "subagent_detail");
    const timeline = nodeById(card, "timeline_detail");
    expect(subagents).toMatchObject({ type: "Container", isVisible: true });
    const subagentItems = subagents.items as JsonRecord[];
    const visibleRows = subagentItems.filter(item => item.type === "ColumnSet" && item.isVisible !== false);
    const hiddenRows = subagentItems.filter(item => item.type === "ColumnSet" && item.isVisible === false);
    expect(visibleRows).toHaveLength(2);
    expect(hiddenRows).toHaveLength(2);
    expect(visibleRows.map(row => cardText(row)).join("\n")).toContain("子代理步骤 3");
    expect(visibleRows.map(row => cardText(row)).join("\n")).toContain("子代理步骤 4");
    expect(hiddenRows.map(row => cardText(row)).join("\n")).toContain("子代理步骤 1");
    expect(cardText(timeline)).toContain("src/service.ts");
    expect(cardText(timeline)).not.toContain("子代理步骤 1");

    expect(nodeById(card, "btn_collapse_subagents")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_subagents")).toMatchObject({ isVisible: true });
    const collapseAction = toggleActionById(card, "subagents_collapse");
    expect(collapseAction.title).toBe("收起");
    expect(targetElements(collapseAction)).toEqual([
      { elementId: "subagent_hidden_summary", isVisible: true },
      { elementId: "subagent_older_step_0", isVisible: false },
      { elementId: "subagent_older_step_1", isVisible: false },
      { elementId: "btn_collapse_subagents", isVisible: false },
      { elementId: "btn_expand_subagents", isVisible: true },
    ]);
    const expandAction = toggleActionById(card, "subagents_expand");
    expect(expandAction.title).toBe("展开");
    expect(targetElements(expandAction)).toEqual([
      { elementId: "subagent_hidden_summary", isVisible: false },
      { elementId: "subagent_older_step_0", isVisible: true },
      { elementId: "subagent_older_step_1", isVisible: true },
      { elementId: "btn_collapse_subagents", isVisible: true },
      { elementId: "btn_expand_subagents", isVisible: false },
    ]);
  });

  test("keeps subagent activity visible without ToggleVisibility support", () => {
    const tracker = createExecutionTracker({ runId: "subagents-fallback" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "subagent-1", "task", 100, "检查回退布局");
    const rendered = buildExecutionCard(tracker.snapshot(1_600), {
      ...fullCapabilities,
      actions: ["Action.OpenUrl"],
    });

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodeById(card, "subagent_detail")).toMatchObject({ isVisible: true });
    expect(nodes(card).some(node => node.id === "btn_collapse_subagents")).toBe(false);
    expect(cardText(card)).toContain("检查回退布局");
  });

  test("orders subagents before trace and omits toggles when three steps fit", () => {
    const tracker = createExecutionTracker({ runId: "section-layout" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "subagent-1", "task", 100, "执行子代理");
    startTool(tracker, "tool-1", "read", 200, "读取文件");
    const rendered = buildExecutionCard(tracker.snapshot(1_600), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const body = card.body as JsonRecord[];
    const subagentIndex = body.findIndex(node => node.id === "subagent_section");
    const timelineIndex = body.findIndex(node => node.id === "timeline_section");
    expect(subagentIndex).toBeGreaterThanOrEqual(0);
    expect(timelineIndex).toBeGreaterThanOrEqual(0);
    expect(subagentIndex < timelineIndex).toBe(true);

    const subagentSection = nodeById(card, "subagent_section");
    const subagentItems = subagentSection.items as JsonRecord[];
    expect(subagentItems[0]).toMatchObject({ type: "ColumnSet" });
    expect(subagentItems[1]).toMatchObject({ id: "subagent_detail" });
    const subagentHeader = subagentItems[0]!;
    expect(nodes(subagentHeader).some(node => node.id === "btn_collapse_subagents")).toBe(false);
    expect(nodes(subagentHeader).some(node => node.id === "btn_expand_subagents")).toBe(false);

    const timelineSection = nodeById(card, "timeline_section");
    const timelineItems = timelineSection.items as JsonRecord[];
    expect(timelineItems[0]).toMatchObject({ type: "ColumnSet" });
    expect(timelineItems[1]).toMatchObject({ id: "timeline_detail" });
    const timelineHeader = timelineItems[0]!;
    expect(nodes(timelineHeader).some(node => node.id === "btn_collapse_trace")).toBe(false);
    expect(nodes(timelineHeader).some(node => node.id === "btn_expand_trace")).toBe(false);
    expect(nodes(card).some(node => node.type === "Action.ToggleVisibility")).toBe(false);
  });

  test("renders step panels with aligned status metadata", () => {
    const tracker = createExecutionTracker({ runId: "tool-row" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "generate_image", 1_000, "写入文件");
    const rendered = buildExecutionCard(tracker.snapshot(36_200), fullCapabilities);
    expect(rendered).not.toBeNull();

    const timeline = nodeById(rendered!.card as JsonRecord, "timeline_detail");
    const step = (timeline.items as JsonRecord[]).find(item => item.type === "ColumnSet");
    expect(step).toBeDefined();
    if (!step) throw new Error("tool step was not rendered");

    const columns = step.columns as JsonRecord[];
    expect(columns).toHaveLength(1);
    expect(columns[0]).toMatchObject({ width: "stretch" });

    const panel = (columns[0]?.items as JsonRecord[])[0];
    expect(panel).toMatchObject({ type: "Container", style: "accent", spacing: "Small" });
    const row = nodes(panel!).find(item => item.type === "ColumnSet");
    expect(row).toMatchObject({ spacing: "None" });
    const rowColumns = row?.columns as JsonRecord[];
    expect(rowColumns).toHaveLength(3);
    expect(rowColumns[0]).toMatchObject({ width: "auto", verticalContentAlignment: "Center" });
    expect(rowColumns[0]?.items).toEqual([
      expect.objectContaining({ text: "●", color: "Accent", size: "Medium", spacing: "None" }),
    ]);
    expect(rowColumns[1]).toMatchObject({ width: "stretch" });
    expect(rowColumns[1]?.items).toEqual([
      expect.objectContaining({ text: "写入文件", weight: "Bolder", spacing: "None" }),
      expect.objectContaining({ text: "generate_image", isSubtle: true, size: "Small", spacing: "None" }),
    ]);
    expect(rowColumns[2]).toMatchObject({ width: "auto", verticalContentAlignment: "Center" });
    expect(rowColumns[2]?.items).toEqual([
      expect.objectContaining({
        text: "进行中 · 35.2s",
        horizontalAlignment: "Right",
        isSubtle: true,
        size: "Small",
        spacing: "None",
      }),
    ]);
  });

  test("keeps todos visible while older execution details expand", () => {
    const tracker = createExecutionTracker({ runId: "fold" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 4; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, "read", index * 10 + 10, `步骤 ${index + 1}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: index * 10 + 20, status: "succeeded" });
    }
    tracker.apply({
      type: "todo_update",
      atMs: 65,
      op: "init",
      phases: [{ name: "Plan", tasks: [{ content: "Keep this visible", status: "in_progress" }] }],
    });
    const snapshot = tracker.snapshot(70);
    const rendered = buildExecutionCard(snapshot, fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;

    const timeline = nodeById(card, "timeline_detail");
    const todos = nodeById(card, "todo_detail");
    expect(timeline.isVisible).toBe(true);
    const timelineRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
    expect(timelineRows.filter(row => row.isVisible !== false)).toHaveLength(3);
    expect(timelineRows.filter(row => row.isVisible === false)).toHaveLength(1);
    expect(todos.isVisible).not.toBe(false);
    expect(cardText(todos)).toContain("Keep this visible");
    expect(nodes(card).some(node => node.id === "collapsed_steps")).toBe(false);

    const collapseTargets = targetElements(toggleActionById(card, "trace_collapse"));
    const expandTargets = targetElements(toggleActionById(card, "trace_expand"));
    const collapseIds = new Set(collapseTargets.map(target => target.elementId as string));
    const expandIds = new Set(expandTargets.map(target => target.elementId as string));
    expect(collapseIds).toEqual(expandIds);
    expect(collapseIds).toEqual(new Set([
      "timeline_hidden_summary",
      "timeline_older_step_0",
      "btn_collapse_trace",
      "btn_expand_trace",
    ]));
    expect(collapseIds.has("todo_detail")).toBe(false);
    expect(nodeById(card, "btn_collapse_trace")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_trace")).toMatchObject({ isVisible: true });

    for (const id of collapseIds) {
      const collapsedTarget = collapseTargets.find(target => target.elementId === id)!;
      const expandedTarget = expandTargets.find(target => target.elementId === id)!;
      expect(collapsedTarget.isVisible).toBe(!expandedTarget.isVisible);
    }
  });
  test("collapses completed progress details behind a header expand control", () => {
    const tracker = createExecutionTracker({ runId: "terminal-collapse" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "完成步骤");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    startTool(tracker, "subagent-1", "task", 3, "完成子代理");
    tracker.apply({ type: "tool_end", toolCallId: "subagent-1", atMs: 4, status: "succeeded" });
    tracker.apply({
      type: "todo_update",
      atMs: 5,
      op: "init",
      phases: [{ name: "Plan", tasks: [{ content: "完成任务", status: "completed" }] }],
    });
    const snapshot = tracker.apply({ type: "run_end", atMs: 6, state: "completed" });
    const rendered = buildExecutionCard(snapshot, fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodeById(card, "execution_status").isVisible).not.toBe(false);
    expect(nodeById(card, "todo_detail")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "subagent_section")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "timeline_section")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_progress")).toMatchObject({ isVisible: true });

    const expandAction = toggleActionById(card, "progress_expand");
    expect(expandAction.title).toBe("展开");
    expect(targetElements(expandAction)).toEqual([
      { elementId: "todo_detail", isVisible: true },
      { elementId: "subagent_section", isVisible: true },
      { elementId: "timeline_section", isVisible: true },
      { elementId: "btn_collapse_progress", isVisible: true },
      { elementId: "btn_expand_progress", isVisible: false },
    ]);
    const collapseAction = toggleActionById(card, "progress_collapse");
    expect(collapseAction.title).toBe("收起");
    expect(targetElements(collapseAction)).toEqual([
      { elementId: "todo_detail", isVisible: false },
      { elementId: "subagent_section", isVisible: false },
      { elementId: "timeline_section", isVisible: false },
      { elementId: "btn_collapse_progress", isVisible: false },
      { elementId: "btn_expand_progress", isVisible: true },
    ]);
  });

  test("omits ToggleVisibility when the negotiated action capability is absent", () => {
    const tracker = createExecutionTracker({ runId: "no-toggle" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "a.ts");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    const snapshot = tracker.apply({ type: "run_end", atMs: 3, state: "completed" });
    const rendered = buildExecutionCard(snapshot, {
      ...fullCapabilities,
      actions: ["Action.OpenUrl"],
    });

    expect(rendered).not.toBeNull();
    const types = new Set(nodes(rendered!.card as JsonRecord).map(node => node.type).filter((type): type is string => typeof type === "string"));
    expect(types.has("Action.ToggleVisibility")).toBe(false);
    expect(types.has("ActionSet")).toBe(false);
    expect(cardText(rendered!.card as JsonRecord)).toContain("a.ts");
  });

  test("uses local element/action/limit defaults when optional profile fields are omitted", () => {
    const tracker = createExecutionTracker({ runId: "defaults" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "a.ts");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    const snapshot = tracker.apply({ type: "run_end", atMs: 3, state: "completed" });
    const rendered = buildExecutionCard(snapshot, {
      enabled: true,
      profiles: ["octo/v1"],
      cardVersion: "1.5",
    });

    expect(rendered).not.toBeNull();
    expect(cardText(rendered!.card as JsonRecord)).toContain("a.ts");
  });
  test("does not emit a card when profile, TextBlock, or minimum budgets cannot support it", () => {
    const tracker = createExecutionTracker({ runId: "disabled" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "a.ts");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    const snapshot = tracker.apply({ type: "run_end", atMs: 3, state: "completed" });

    expect(buildExecutionCard(snapshot, { ...fullCapabilities, enabled: false })).toBeNull();
    expect(buildExecutionCard(snapshot, { ...fullCapabilities, profiles: ["other/v9"] })).toBeNull();
    expect(buildExecutionCard(snapshot, { ...fullCapabilities, cardVersion: "1.4" })).toBeNull();
    expect(buildExecutionCard(snapshot, { ...fullCapabilities, elements: [] })).toBeNull();

    // TextBlock is the only required element. A server that omits layout
    // elements still gets the actual supported flat representation.
    const flat = buildExecutionCard(snapshot, { ...fullCapabilities, elements: ["TextBlock"] });
    expect(flat).not.toBeNull();
    const flatTypes = new Set(nodes(flat!.card as JsonRecord).map(node => node.type));
    expect(flatTypes.has("Container")).toBe(false);
    expect(flatTypes.has("ColumnSet")).toBe(false);
    expect(flatTypes.has("ActionSet")).toBe(false);
    expect(cardText(flat!.card as JsonRecord)).toContain("a.ts");

    expect(buildExecutionCard(snapshot, {
      ...fullCapabilities,
      limits: { maxNodes: 1, maxDepth: 1, maxPayloadBytes: 128 },
    })).toBeNull();
    expect(buildExecutionCard(snapshot, {
      ...fullCapabilities,
      limits: { maxNodes: 0 },
    })).toBeNull();
    expect(buildExecutionCard(snapshot, {
      ...fullCapabilities,
      limits: { maxNodes: 1.5 },
    })).toBeNull();
  });

  test("stays within the negotiated node, depth, and payload budgets", () => {
    const tracker = createExecutionTracker({ runId: "budgets" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 48; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, index % 2 === 0 ? "read" : "grep", index * 2 + 1, `file-${index}.ts`);
      tracker.apply({
        type: "tool_end",
        toolCallId,
        atMs: index * 2 + 2,
        status: "succeeded",
        summary: `${index} 个结果`,
      });
    }
    const snapshot = tracker.apply({ type: "run_end", atMs: 200, state: "completed" });
    const limits = { maxNodes: 80, maxDepth: 16, maxPayloadBytes: 8_192 };
    const rendered = buildExecutionCard(snapshot, { ...fullCapabilities, limits });

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const stats = cardStats(card);
    expect(stats.nodes).toBeLessThanOrEqual(limits.maxNodes);
    expect(stats.maxDepth).toBeLessThanOrEqual(limits.maxDepth);
    const wirePayload = {
      type: 17,
      profile: "octo/v1",
      card_version: "1.5",
      card,
      plain: rendered!.plain,
    };
    expect(payloadBytes(wirePayload)).toBeLessThanOrEqual(limits.maxPayloadBytes);
    const wireStats = cardStats(wirePayload);
    expect(wireStats.nodes).toBeLessThanOrEqual(limits.maxNodes);
    expect(wireStats.maxDepth).toBeLessThanOrEqual(limits.maxDepth);
  });

  test("uses unified status markers and state-specific colors", () => {
    const runningTracker = createExecutionTracker({ runId: "colors-running" });
    startTurn(runningTracker, "turn-1", 0);
    startTool(runningTracker, "running-tool", "read", 1, "a.ts");
    const running = buildExecutionCard(runningTracker.snapshot(2), fullCapabilities);
    expect(running).not.toBeNull();
    const runningNodes = nodes(running!.card as JsonRecord);
    const runningGlyphs = runningNodes.filter(node => node.text === "●");
    expect(runningGlyphs).toHaveLength(2);
    expect(runningGlyphs.every(node => node.color === "Accent")).toBe(true);
    const runningRow = runningNodes.find(node => (
      node.type === "ColumnSet"
      && cardText(node).includes("a.ts")
    ));
    expect(runningRow).toBeDefined();
    const runningStatus = runningNodes.find(node => node.text === "进行中");
    expect(runningStatus).toMatchObject({ color: "Accent" });

    const completedTracker = createExecutionTracker({ runId: "colors-completed" });
    startTurn(completedTracker, "turn-1", 0);
    startTool(completedTracker, "completed-tool", "read", 1, "完成步骤");
    completedTracker.apply({ type: "tool_end", toolCallId: "completed-tool", atMs: 2, status: "succeeded" });
    const completed = buildExecutionCard(
      completedTracker.apply({ type: "run_end", atMs: 3, state: "completed" }),
      fullCapabilities,
    );
    expect(completed).not.toBeNull();
    const completedNodes = nodes(completed!.card as JsonRecord);
    const completedRow = completedNodes.find(node => (
      node.type === "Container"
      && node.style === "good"
      && cardText(node).includes("完成步骤")
    ));
    expect(completedRow).toBeDefined();
    const completedGlyphs = completedNodes.filter(node => node.text === "✓");
    expect(completedGlyphs).toHaveLength(2);
    expect(completedGlyphs.every(node => node.color === "Good")).toBe(true);

    const waitingTracker = createExecutionTracker({ runId: "colors-waiting" });
    startTurn(waitingTracker, "turn-1", 0);
    startTool(waitingTracker, "waiting-tool", "subagent_await", 1, "等待子代理");
    const waiting = buildExecutionCard(waitingTracker.snapshot(2), fullCapabilities);
    expect(waiting).not.toBeNull();
    const waitingNodes = nodes(waiting!.card as JsonRecord);
    const waitingRow = waitingNodes.find(node => (
      node.type === "Container"
      && node.style === "default"
      && cardText(node).includes("等待子代理")
    ));
    expect(waitingRow).toBeDefined();
    const waitingGlyph = waitingNodes.find(node => node.text === "-" && node.color === "Default");
    expect(waitingGlyph).toBeDefined();
    const waitingStatus = nodes(waitingRow!).find(node => (
      typeof node.text === "string" && node.text.startsWith("等待 · ")
    ));
    expect(waitingStatus).toMatchObject({ color: "Default" });

    const failedTracker = createExecutionTracker({ runId: "colors-failed" });
    startTurn(failedTracker, "turn-1", 0);
    startTool(failedTracker, "failed-tool", "bash", 1, "bun test");
    failedTracker.apply({
      type: "tool_end",
      toolCallId: "failed-tool",
      atMs: 2,
      status: "failed",
      errorCategory: "worker",
    });
    const failed = buildExecutionCard(
      failedTracker.apply({ type: "run_end", atMs: 3, state: "failed" }),
      fullCapabilities,
    );
    expect(failed).not.toBeNull();
    const failedNodes = nodes(failed!.card as JsonRecord);
    expect(failedNodes.filter(node => node.text === "×")).toHaveLength(2);
    expect(failedNodes.filter(node => node.text === "×").every(node => node.color === "Attention")).toBe(true);
    const failedRow = failedNodes.find(node => (
      node.type === "Container"
      && node.style === "attention"
      && cardText(node).includes("bun test")
    ));
    expect(failedRow).toBeDefined();
    const failedStatus = failedNodes.find(node => node.text === "失败");
    expect(failedStatus).toMatchObject({ color: "Attention" });
  });
  test("uses column-set timeline steps for web renderer state surfaces", () => {
    const tracker = createExecutionTracker({ runId: "web-progress-layout" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "completed-tool", "read", 1, "完成步骤");
    tracker.apply({ type: "tool_end", toolCallId: "completed-tool", atMs: 2, status: "succeeded" });
    startTool(tracker, "running-tool", "bash", 3, "进行中步骤");

    const rendered = buildExecutionCard(tracker.snapshot(4), fullCapabilities);
    expect(rendered).not.toBeNull();
    const timeline = nodeById(rendered!.card as JsonRecord, "timeline_detail");
    const directChildren = Array.isArray(timeline.items)
      ? timeline.items.map(asRecord).filter((item): item is JsonRecord => item !== null)
      : [];
    expect(directChildren).toHaveLength(2);
    expect(directChildren.every(child => child.type === "ColumnSet")).toBe(true);

    const statePanels = directChildren.flatMap(child => nodes(child).filter(candidate => (
      candidate.type === "Container" && ["good", "accent"].includes(String(candidate.style))
    )));
    expect(statePanels).toHaveLength(2);
  });
  test("uses standard spacing for rich progress cards", () => {
    const tracker = createExecutionTracker({ runId: "standard-spacing" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "标准步骤");

    const rendered = buildExecutionCard(tracker.snapshot(2), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodeById(card, "execution_status")).toMatchObject({ spacing: "Medium" });
    expect(nodeById(card, "timeline_section")).toMatchObject({ spacing: "Medium" });
    expect(nodeById(card, "timeline_detail")).toMatchObject({ spacing: "Small" });

    const panel = nodes(card).find(candidate => (
      candidate.type === "Container"
      && candidate.style === "accent"
      && cardText(candidate).includes("标准步骤")
    ));
    expect(panel).toMatchObject({ spacing: "Small" });
  });

  test("shows only the three most recent steps by default", () => {
    const tracker = createExecutionTracker({ runId: "recent-steps" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 7; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, "read", index * 10 + 1, `步骤 ${index}`);
      tracker.apply({
        type: "tool_end",
        toolCallId,
        atMs: index * 10 + 2,
        status: "succeeded",
      });
    }

    const rendered = buildExecutionCard(
      tracker.apply({ type: "run_end", atMs: 100, state: "completed" }),
      fullCapabilities,
    );
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const timeline = nodeById(card, "timeline_detail");
    expect(timeline.isVisible).toBe(true);
    const timelineItems = timeline.items as JsonRecord[];
    const visibleRows = timelineItems.filter(item => item.type === "ColumnSet" && item.isVisible !== false);
    const hiddenRows = timelineItems.filter(item => item.type === "ColumnSet" && item.isVisible === false);
    expect(visibleRows).toHaveLength(3);
    expect(hiddenRows).toHaveLength(4);
    expect(nodeById(card, "timeline_hidden_summary").isVisible).not.toBe(false);

    const visibleText = visibleRows.map(row => cardText(row)).join("\n");
    const hiddenText = hiddenRows.map(row => cardText(row)).join("\n");
    expect(visibleText).not.toContain("步骤 0");
    expect(visibleText).not.toContain("步骤 1");
    expect(visibleText).not.toContain("步骤 2");
    expect(visibleText).not.toContain("步骤 3");
    expect(visibleText).toContain("步骤 4");
    expect(visibleText).toContain("步骤 6");
    expect(hiddenText).toContain("步骤 0");
    expect(hiddenText).toContain("步骤 3");
    expect(nodes(card).some(node => node.id === "collapsed_steps")).toBe(false);

    expect(nodeById(card, "btn_collapse_trace")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_trace")).toMatchObject({ isVisible: true });
    const expandTargets = targetElements(toggleActionById(card, "trace_expand"));
    expect(expandTargets.find(target => target.elementId === "timeline_hidden_summary")).toMatchObject({ isVisible: false });
    const olderTargets = expandTargets.filter(target => (
      typeof target.elementId === "string" && target.elementId.startsWith("timeline_older_step_")
    ));
    expect(olderTargets).toHaveLength(4);
    expect(olderTargets.every(target => target.isVisible === true)).toBe(true);
  });
  test("prioritizes current and blocked todo tasks while preserving order and reporting omissions", () => {
    const tracker = createExecutionTracker({ runId: "todo-budget" });
    startTurn(tracker, "turn-1", 0);
    const tasks = Array.from({ length: 110 }, (_unused, index) => ({
      content: `task-${index}`,
      status: index === 0
        ? ("in_progress" as const)
        : index === 55
          ? ("blocked" as const)
          : index === 109
            ? ("completed" as const)
            : ("pending" as const),
      ...(index === 55 ? { blocker: "等待外部依赖" } : {}),
    }));
    tracker.apply({
      type: "todo_update",
      atMs: 1,
      op: "init",
      phases: [{ name: "Plan", tasks }],
    });
    const snapshot = tracker.apply({ type: "run_end", atMs: 2, state: "completed" });
    const rendered = buildExecutionCard(snapshot, {
      ...fullCapabilities,
      limits: { maxNodes: 200, maxDepth: 16, maxPayloadBytes: 32_768 },
    });

    expect(rendered).not.toBeNull();
    const text = cardText(rendered!.card as JsonRecord);
    expect(text).toContain("task-0");
    expect(text).toContain("task-55");
    expect(text).toContain("task-109");
    expect(text).toContain("等待外部依赖");
    expect(text.indexOf("task-0")).toBeLessThan(text.indexOf("task-55"));
    expect(text.indexOf("task-55")).toBeLessThan(text.indexOf("task-109"));
    expect(text).toContain("共 110 个任务");
    const hidden = text.match(/已隐藏(?:前)? (\d+) 个任务/);
    expect(hidden).not.toBeNull();
    expect(Number(hidden?.[1])).toBeGreaterThan(0);
  });
});
