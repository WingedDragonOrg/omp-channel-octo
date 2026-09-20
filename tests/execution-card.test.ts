import { describe, expect, test } from "bun:test";
import { buildExecutionCard, type ExecutionCardCapabilities } from "../src/cards/execution-card.ts";
import type { ResearchRun, ResearchSnapshot } from "../src/research/index.ts";
import {
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  stripCardSummaries,
  type ExecutionEvent,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "../src/cards/execution-tracker.ts";

type JsonRecord = Record<string, unknown>;

const fullCapabilities: ExecutionCardCapabilities = {
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

/** Labels of the todo rows the reader actually sees, in card order. */
function visibleTodoTaskLabels(card: JsonRecord): string[] {
  const todoDetail = nodes(card).find(node => node.id === "todo_detail");
  if (!todoDetail) return [];
  const labels: string[] = [];
  for (const row of nodes(todoDetail)) {
    if (row.type !== "ColumnSet" || row.isVisible === false) continue;
    const columns = row.columns as JsonRecord[] | undefined;
    if (columns?.[0]?.width !== "80px") continue;
    const body = (columns[1]?.items as JsonRecord[] | undefined)?.[0];
    const text = typeof body?.text === "string" ? body.text : cardText(body ?? {});
    labels.push(text);
  }
  return labels;
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

function clockAt(atMs: number): string {
  const at = new Date(atMs);
  return [at.getHours(), at.getMinutes(), at.getSeconds()]
    .map(part => String(part).padStart(2, "0"))
    .join(":");
}

/** The worker's `subagent_lifecycle` with status `started` / `subagent_progress` arrival. */
function startSubagent(
  tracker: ExecutionTracker,
  subagentId: string,
  agent: string,
  atMs: number,
  summary?: string,
): void {
  tracker.apply({
    type: "subagent_start",
    subagentId,
    agent,
    state: "running",
    atMs,
    ...(summary === undefined ? {} : { summary }),
  });
}

function endSubagent(
  tracker: ExecutionTracker,
  subagentId: string,
  atMs: number,
  state: "completed" | "failed" | "cancelled",
  summary?: string,
): void {
  tracker.apply({
    type: "subagent_end",
    subagentId,
    state,
    atMs,
    ...(summary === undefined ? {} : { summary }),
  });
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
    const firstTaskMarker = (firstTaskColumns[0]?.items as JsonRecord[])[0]!;
    expect(firstTaskMarker).toMatchObject({
      color: "Accent",
      fontType: "Monospace",
      size: "Small",
      text: "│\u00a0├─\u2009[•]",
      wrap: false,
    });
    expect(firstTaskColumns[0]).toMatchObject({ width: "80px", verticalContentAlignment: "Center" });
    expect(firstTaskColumns[1]).toMatchObject({ width: "stretch", verticalContentAlignment: "Center" });
    expect(cardText(firstTaskColumns[1]!)).toBe("Wire storage");
    const blockedTaskColumns = foundationRows[1]?.columns as JsonRecord[];
    expect(blockedTaskColumns).toHaveLength(2);
    expect((blockedTaskColumns[0]?.items as JsonRecord[])[0]).toMatchObject({
      text: "│\u00a0└─\u2009[-]",
      color: "Default",
      fontType: "Monospace",
      size: "Small",
      wrap: false,
    });
    expect(blockedTaskColumns[0]).toMatchObject({ width: "80px", verticalContentAlignment: "Center" });
    expect(blockedTaskColumns[1]).toMatchObject({ width: "stretch", verticalContentAlignment: "Center" });
    expect(cardText(blockedTaskColumns[1]!)).toBe("Wait for access · 等待授权");
    expect(cardText(blockedCard)).not.toContain("│  ├─");
    expect(blockedRendered!.plain).toContain("├─ Foundation");
    expect(blockedRendered!.plain).toContain("│  ├─ • Wire storage");
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
    expect(rendered!.plain).toContain("   └─ • Run checks");
  });

  test("renders cancelled todo text with a strikethrough", () => {
    const snapshot = {
      runId: "cancelled-todo",
      state: "running",
      startedAtMs: 0,
      endedAtMs: null,
      elapsedMs: 100,
      phaseCount: 0,
      toolCount: 0,
      tools: [],
      todos: [{
        name: "Plan",
        tasks: [{ content: "Remove obsolete task", status: "cancelled" }],
      }],
    } as unknown as ExecutionSnapshot;
    const rendered = buildExecutionCard(snapshot, fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const todoDetail = nodeById(card, "todo_detail");
    const taskRow = nodes(todoDetail).find(node => (
      node.type === "ColumnSet" && cardText(node).includes("Remove obsolete task")
    ));
    expect(taskRow).toBeDefined();
    if (!taskRow) throw new Error("cancelled todo row was not rendered");
    const columns = taskRow.columns as JsonRecord[];
    expect((columns[0]?.items as JsonRecord[])[0]).toMatchObject({
      text: "\u00a0\u00a0└─\u2009[-]",
      color: "Default",
      fontType: "Monospace",
      size: "Small",
      wrap: false,
    });
    expect((columns[1]?.items as JsonRecord[])[0]).toMatchObject({
      type: "RichTextBlock",
      spacing: "None",
      inlines: [
        expect.objectContaining({
          text: "Remove obsolete task",
          color: "Default",
          strikethrough: true,
        }),
      ],
    });
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
    expect(textNodes.some(node => node.text === "询问用户")).toBe(true);
  });

  test("keeps live subagents as their own band above the trace", () => {
    const tracker = createExecutionTracker({ runId: "subagents" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 4; index += 1) {
      startSubagent(tracker, `child-${index}`, `agent-${index + 1}`, 100 + index, `子代理步骤 ${index + 1}`);
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
    expect(visibleRows).toHaveLength(4);
    const visibleText = visibleRows.map(row => cardText(row)).join("\n");
    expect(visibleText).toContain("agent-1");
    expect(visibleText).toContain("子代理步骤 1");
    expect(visibleText).toContain("agent-4");
    expect(visibleText).toContain("进行中");
    // Subagents are their own section, never rows of the trace window.
    expect(cardText(timeline)).toContain("src/service.ts");
    expect(cardText(timeline)).not.toContain("agent-1");
    expect(nodeById(card, "subagent_section").isVisible).not.toBe(false);
    expect(cardText(nodeById(card, "subagent_section"))).toContain("子代理 · 4");
  });

  test("move a finished subagent into the expandable history band", () => {
    const tracker = createExecutionTracker({ runId: "subagent-history" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "CountSrc", "scout", 100, "统计源码文件");
    tracker.apply({
      type: "subagent_update",
      subagentId: "CountSrc",
      state: "running",
      atMs: 200,
      summary: "扫描 src",
      currentTool: "glob",
      toolCount: 2,
    });
    endSubagent(tracker, "CountSrc", 400, "completed", "统计源码文件");
    startSubagent(tracker, "Verify", "reviewer", 500, "复核结果");
    const rendered = buildExecutionCard(tracker.snapshot(1_600), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const liveRows = (nodeById(card, "subagent_detail").items as JsonRecord[])
      .filter(item => item.type === "ColumnSet" && item.isVisible !== false);
    expect(liveRows).toHaveLength(1);
    expect(cardText(liveRows[0]!)).toContain("reviewer");
    const hiddenSummary = nodeById(card, "subagent_hidden_summary");
    expect(cardText(hiddenSummary)).toContain("已隐藏 1 个子代理，共 2 个子代理");
    const historyBand = nodeById(card, "subagent_omitted_steps");
    expect(historyBand).toMatchObject({ type: "Container", style: "emphasis", isVisible: false });
    expect(cardText(historyBand)).toContain("scout");
    expect(cardText(historyBand)).toContain("已完成");
    // The last reported activity survives into the history row; the child's own prompt does not.
    expect(cardText(historyBand)).toContain("统计源码文件");
    expect(nodes(card).some(node => node.id === "btn_expand_subagents")).toBe(true);
  });

  test("renders subagent state with its tool activity and no per-child timing", () => {
    const tracker = createExecutionTracker({ runId: "subagent-state-only" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "Live", "scout", 100, "统计源码");
    tracker.apply({
      type: "subagent_update",
      subagentId: "Live",
      state: "running",
      atMs: 140,
      summary: "扫描 src",
      currentTool: "glob",
      toolCount: 2,
    });
    startSubagent(tracker, "Done2", "reviewer", 200, "复核变更");
    endSubagent(tracker, "Done2", 900, "completed", "复核变更");
    const rendered = buildExecutionCard(tracker.snapshot(1_000), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const text = cardText(nodeById(card, "subagent_detail"));
    expect(text).toContain("scout");
    expect(text).toContain("扫描 src");
    expect(text).toContain("进行中");
    expect(text).toContain("glob");
    expect(text).toContain("2 次调用");
    // State and activity only: no per-child clock and no duration.
    expect(text).not.toMatch(/\d{2}:\d{2}:\d{2}/u);
    expect(text).not.toMatch(/\d+ms|\d+\.\d+s|\d+m \d+s/u);
    expect(rendered!.plain).toContain("scout · 扫描 src · 进行中 · glob · 2 次调用");
  });

  test("ends a subagent only when its own terminal event arrives", () => {
    const tracker = createExecutionTracker({ runId: "subagent-terminal" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "Failed", "scout", 100, "检查实现");
    endSubagent(tracker, "Failed", 300, "failed");
    startSubagent(tracker, "Cancelled", "sonic", 400, "整理数据");
    endSubagent(tracker, "Cancelled", 500, "cancelled");
    // Late progress and a repeated start cannot revive a finished child.
    tracker.apply({ type: "subagent_update", subagentId: "Failed", state: "running", atMs: 600, summary: "不应出现" });
    startSubagent(tracker, "Failed", "scout", 700, "不应出现");
    const snapshot = tracker.snapshot(800);

    expect(snapshot.subagents.map(subagent => [subagent.subagentId, subagent.state])).toEqual([
      ["Failed", "failed"],
      ["Cancelled", "cancelled"],
    ]);
    expect(snapshot.subagents[0]!.summary).toBe("检查实现");
    expect(snapshot.subagentCount).toBe(2);
  });
  test("settles a still-live subagent when the run ends without its terminal event", () => {
    const tracker = createExecutionTracker({ runId: "subagent-run-end" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "Orphan", "scout", 100, "中断前的工作");
    tracker.apply({ type: "run_end", atMs: 500, state: "cancelled" });

    const snapshot = tracker.snapshot(500);
    expect(snapshot.subagents[0]).toMatchObject({
      subagentId: "Orphan",
      agent: "scout",
      state: "cancelled",
      summary: "中断前的工作",
    });
  });
  test("round-trips subagents through the durable card payload", () => {
    const tracker = createExecutionTracker({ runId: "subagent-restore" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "Done", "scout", 400, "完成的工作");
    endSubagent(tracker, "Done", 900, "completed", "完成的工作");
    startSubagent(tracker, "Live", "verifier", 1_200, "重启时仍在跑");
    tracker.apply({
      type: "subagent_update",
      subagentId: "Live",
      state: "running",
      atMs: 1_250,
      summary: "重启时仍在跑",
      currentTool: "read",
      toolCount: 4,
    });
    const encoded = encodeExecutionSnapshot(tracker.snapshot(1_300));

    const restored = restoreExecutionSnapshot(encoded, { state: "cancelled", atMs: 2_000 });
    expect(restored).not.toBeNull();
    expect(restored!.state).toBe("cancelled");
    expect(restored!.subagents.map(subagent => [subagent.subagentId, subagent.agent, subagent.state])).toEqual([
      ["Done", "scout", "completed"],
      ["Live", "verifier", "cancelled"],
    ]);
    expect(restored!.subagents[1]).toMatchObject({ currentTool: "read", toolCount: 4 });
    expect(restored!.subagents[0]).not.toHaveProperty("toolCount");
    const rendered = buildExecutionCard(restored!, fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    // A child the restart could not observe ending is settled by the run's own terminal frame,
    // so the restored card lists it in the history band with the state the run ended in.
    const historyBand = cardText(nodeById(card, "subagent_omitted_steps"));
    expect(historyBand).toContain("scout");
    expect(historyBand).toContain("已完成");
    expect(historyBand).toContain("verifier");
    expect(historyBand).toContain("已取消");
  });
  test("keeps the compact history band visible when ToggleVisibility is unavailable", () => {
    const tracker = createExecutionTracker({ runId: "trace-no-toggle" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 4; index += 1) {
      startTool(tracker, `trace-${index}`, "read", 100 + index, `轨迹步骤 ${index + 1}`);
    }
    const rendered = buildExecutionCard(tracker.snapshot(1_600), {
      ...fullCapabilities,
      actions: ["Action.OpenUrl"],
    });

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const timeline = nodeById(card, "timeline_detail");
    const richRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
    expect(richRows).toHaveLength(3);
    // Without the toggle action there is nothing to hide behind: the trimmed row stays readable
    // in the grey band instead of folding away.
    const band = nodeById(card, "timeline_omitted_steps");
    expect(band).toMatchObject({ type: "Container", style: "emphasis" });
    expect(band.isVisible).not.toBe(false);
    expect(cardText(band)).toContain("轨迹步骤 1");
    expect(cardText(timeline)).toContain("轨迹步骤 4");
    expect(cardText(timeline)).not.toContain("轨迹步骤 1");
    expect(nodes(card).some(node => node.id === "btn_expand_trace")).toBe(false);
  });
  test("uses the same compact row layout for omitted subagents and trace steps", () => {
    const tracker = createExecutionTracker({ runId: "compact-subagents" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "verifier", "reviewer", 1, "验证实现");
    endSubagent(tracker, "verifier", 2, "completed", "验证实现");
    startTool(tracker, "old-trace", "read", 3, "旧轨迹");
    for (let index = 0; index < 32; index += 1) {
      const toolCallId = `visible-${index}`;
      startTool(tracker, toolCallId, "read", 10 + index, `步骤 ${index}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: 11 + index, status: "succeeded" });
    }

    const rendered = buildExecutionCard(tracker.snapshot(100), {
      ...fullCapabilities,
      limits: { maxNodes: 1_000, maxDepth: 16, maxPayloadBytes: 1_000_000 },
    });
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const omittedTrace = nodeById(card, "timeline_omitted_steps");
    const omittedSubagents = nodeById(card, "subagent_omitted_steps");
    const traceRow = (omittedTrace.items as JsonRecord[])[0]!;
    const subagentRow = (omittedSubagents.items as JsonRecord[])[0]!;

    // Both history groups render as the same grey emphasis band as the panel rows above them, as
    // one compact text line per step so hidden rows stay affordable next to user-visible content.
    expect(omittedTrace).toMatchObject({ type: "Container", style: "emphasis", spacing: "Small" });
    expect(omittedSubagents).toMatchObject({ type: "Container", style: "emphasis", spacing: "Small" });
    // The trace band is the whole compact tier in source order: the rows the rich window trimmed
    // away first, then the budget-pruned old steps.
    const traceBandText = cardText(omittedTrace);
    expect(traceBandText).toContain("步骤 0");
    expect(traceBandText).toContain("旧轨迹");
    expect(traceBandText.indexOf("步骤 0")).toBeLessThan(traceBandText.indexOf("旧轨迹"));
    // A finished child renders once: its row lives in the band, not again as a bare line.
    expect(nodes(card).filter(node => (
      typeof node.text === "string" && node.text.includes("reviewer · 验证实现")
    ))).toHaveLength(1);

    for (const row of [traceRow, subagentRow]) {
      expect(row).toMatchObject({ type: "TextBlock", wrap: true, spacing: "Small" });
      expect(row.text).toMatch(/^[✓×•-]\u00a0· /u);
    }
    expect(subagentRow).toMatchObject({
      color: "Good",
      text: "✓\u00a0· reviewer · 验证实现 · 已完成",
    });
    expect(cardText(subagentRow)).toContain("reviewer");
  });

  test("keeps filled containers out of the step list so the client keeps the history band grey", () => {
    // The web client's agent-progress layer repaints every filled Container that is a direct child
    // of #timeline_detail as a failed-step status chip (danger fill + border, inline fill cleared),
    // which turned the omitted-step history band red. The band therefore lives beside the step list.
    const tracker = createExecutionTracker({ runId: "history-band-placement" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "old-trace", "read", 1, "最早步骤");
    for (let index = 0; index < 33; index += 1) {
      const toolCallId = `step-${index}`;
      startTool(tracker, toolCallId, "read", 10 + index, `步骤 ${index}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: 11 + index, status: "succeeded" });
    }

    const rendered = buildExecutionCard(tracker.snapshot(100), {
      ...fullCapabilities,
      limits: { maxNodes: 1_000, maxDepth: 16, maxPayloadBytes: 1_000_000 },
    });
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const traceSection = nodeById(card, "timeline_section");
    const traceDetail = nodeById(card, "timeline_detail");
    const historyBand = nodeById(card, "timeline_omitted_steps");

    expect(historyBand).toMatchObject({ type: "Container", style: "emphasis", isVisible: false });
    expect(cardText(historyBand)).toContain("最早步骤");
    const traceSectionIds = (traceSection.items as JsonRecord[])
      .map(item => item.id)
      .filter((id): id is string => typeof id === "string");
    expect(traceSectionIds).toEqual(["timeline_detail", "timeline_omitted_steps"]);
    expect((traceDetail.items as JsonRecord[]).map(item => item.id)).not.toContain("timeline_omitted_steps");

    // Same guarantee under the real 200-node budget, where more steps stay in the hidden summary.
    const budgeted = buildExecutionCard(tracker.snapshot(100), fullCapabilities);
    expect(budgeted).not.toBeNull();
    for (const built of [card, budgeted!.card as JsonRecord]) {
      const stepList = nodeById(built, "timeline_detail");
      const filledStepListChildren = (stepList.items as JsonRecord[]).filter(
        item => item.type === "Container" && item.style !== undefined,
      );
      expect(filledStepListChildren).toEqual([]);
    }
  });

  test("keeps the compact tier inside the grey history bands", () => {
    const tracker = createExecutionTracker({ runId: "compact-band-scope" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "Done1", "scout", 1, "统计源码");
    endSubagent(tracker, "Done1", 2, "completed", "统计源码");
    startSubagent(tracker, "Live", "verifier", 3, "验证实现");
    for (let index = 0; index < 6; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, "read", 10 + index, `步骤 ${index}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: 11 + index, status: "succeeded" });
    }

    const rendered = buildExecutionCard(tracker.snapshot(100), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(cardStats(card).nodes).toBeLessThanOrEqual(fullCapabilities.limits!.maxNodes! - 1);

    // Inside the detail containers only the summaries stay: every compact row sits in a grey band,
    // so no step line is left bare on the card background.
    const stepListChildren = nodeById(card, "timeline_detail").items as JsonRecord[];
    expect(stepListChildren.filter(item => item.type === "TextBlock").map(item => item.id))
      .toEqual(["timeline_hidden_summary"]);
    const subagentChildren = nodeById(card, "subagent_detail").items as JsonRecord[];
    expect(subagentChildren.filter(item => item.type === "TextBlock").map(item => item.id))
      .toEqual(["subagent_hidden_summary"]);
    const traceBand = nodeById(card, "timeline_omitted_steps");
    const subagentBand = nodeById(card, "subagent_omitted_steps");
    for (const band of [traceBand, subagentBand]) {
      expect(band).toMatchObject({ type: "Container", style: "emphasis", isVisible: false });
      const rows = band.items as JsonRecord[];
      expect(rows.length).toBeGreaterThan(0);
      expect(rows.every(row => row.type === "TextBlock" && row.wrap === true)).toBe(true);
    }
    expect(cardText(traceBand)).toContain("步骤 0");
    // A finished child renders once: its row lives in the band, not again as a bare line.
    expect(nodes(card).filter(node => (
      typeof node.text === "string" && node.text.includes("scout · 统计源码")
    ))).toHaveLength(1);

    // The section controls move exactly these bands, in both directions.
    expect(targetElements(toggleActionById(card, "trace_expand"))
      .find(target => target.elementId === "timeline_omitted_steps")).toMatchObject({ isVisible: true });
    expect(targetElements(toggleActionById(card, "trace_collapse"))
      .find(target => target.elementId === "timeline_omitted_steps")).toMatchObject({ isVisible: false });
    expect(targetElements(toggleActionById(card, "subagents_expand"))
      .find(target => target.elementId === "subagent_omitted_steps")).toMatchObject({ isVisible: true });
    expect(targetElements(toggleActionById(card, "subagents_collapse"))
      .find(target => target.elementId === "subagent_omitted_steps")).toMatchObject({ isVisible: false });
  });

  test("keeps waiting markers out of markdown list rendering", () => {
    const tracker = createExecutionTracker({ runId: "waiting-marker" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "verifier", "subagent_await", 900, "等待子代理 · verifier");
    const snapshot = tracker.snapshot(1_800);
    const rendered = buildExecutionCard(snapshot, fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodes(card).find(node => node.text === "-")).toMatchObject({ color: "Default" });
    expect(cardText(card)).not.toContain("- 等待子代理 · verifier");

    const flat = buildExecutionCard(snapshot, { ...fullCapabilities, elements: ["TextBlock"] });
    expect(flat).not.toBeNull();
    const flatText = cardText(flat!.card as JsonRecord);
    expect(flatText).toContain(`-\u00a0· ${clockAt(900)} · 等待子代理`);
    expect(flatText).toContain("等待 · 900ms");
  });

  test("keeps subagent activity visible without ToggleVisibility support", () => {
    const tracker = createExecutionTracker({ runId: "subagents-fallback" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "child-1", "scout", 100, "检查回退布局");
    startSubagent(tracker, "child-2", "reviewer", 200, "复核回退布局");
    endSubagent(tracker, "child-2", 300, "completed", "复核回退布局");
    const rendered = buildExecutionCard(tracker.snapshot(1_600), {
      ...fullCapabilities,
      actions: ["Action.OpenUrl"],
    });

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodeById(card, "subagent_detail")).toMatchObject({ isVisible: true });
    expect(nodes(card).some(node => node.id === "btn_collapse_subagents")).toBe(false);
    expect(cardText(card)).toContain("检查回退布局");
    // Without the toggle action, hiding would strand the rows: the finished child stays readable
    // in the visible grey history band instead of a collapsed one.
    const band = nodeById(card, "subagent_omitted_steps");
    expect(band).toMatchObject({ type: "Container", style: "emphasis" });
    expect(band.isVisible).not.toBe(false);
    expect(cardText(band)).toContain("reviewer");
    expect(cardText(band)).toContain("已完成");
    expect(nodes(card).some(node => node.id === "subagent_older_step_0")).toBe(false);
  });

  test("orders subagents before trace and omits toggles when three steps fit", () => {
    const tracker = createExecutionTracker({ runId: "section-layout" });
    startTurn(tracker, "turn-1", 0);
    startSubagent(tracker, "child-1", "scout", 100, "执行子代理");
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
    const toggleActionIds = nodes(card)
      .filter(node => node.type === "Action.ToggleVisibility")
      .map(node => node.id);
    expect(toggleActionIds.sort()).toEqual(["progress_collapse", "progress_expand"]);
  });

  test("renders step panels with an aligned marker, body, and status stack", () => {
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
    expect(rowColumns).toHaveLength(2);
    expect(rowColumns[0]).toMatchObject({ width: "24px", verticalContentAlignment: "Center" });
    expect(rowColumns[0]?.items).toEqual([
      expect.objectContaining({ text: "•", color: "Accent", size: "Small", spacing: "None" }),
    ]);
    expect(rowColumns[1]).toMatchObject({ width: "stretch" });
    expect(rowColumns[1]?.items).toEqual([
      expect.objectContaining({ text: "写入文件", weight: "Bolder", spacing: "None" }),
      expect.objectContaining({ text: "generate_image", isSubtle: true, size: "Small", spacing: "None" }),
      expect.objectContaining({
        text: `${clockAt(1_000)} · 进行中 · 35.2s`,
        isSubtle: true,
        size: "Small",
        spacing: "None",
        color: "Accent",
      }),
    ]);
  });
  test("keeps long timeline summaries in the stretch body column", () => {
    const tracker = createExecutionTracker({ runId: "tool-row-long-summary" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "hub-1", "hub", 1_000, "IRC → preview-agent · 未收到回复");
    tracker.apply({
      type: "tool_end",
      toolCallId: "hub-1",
      atMs: 1_140,
      status: "failed",
      errorCategory: "worker",
    });
    const rendered = buildExecutionCard(tracker.snapshot(1_140), fullCapabilities);
    expect(rendered).not.toBeNull();

    const timeline = nodeById(rendered!.card as JsonRecord, "timeline_detail");
    const step = (timeline.items as JsonRecord[]).find(item => item.type === "ColumnSet");
    expect(step).toBeDefined();
    if (!step) throw new Error("long-summary tool step was not rendered");
    const outerColumn = (step.columns as JsonRecord[])[0]!;
    const panel = (outerColumn.items as JsonRecord[])[0]!;
    const row = nodes(panel).find(item => item.type === "ColumnSet")!;
    const rowColumns = row.columns as JsonRecord[];
    expect(rowColumns).toHaveLength(2);
    expect(rowColumns[0]).toMatchObject({ width: "24px" });
    expect((rowColumns[1]?.items as JsonRecord[]).map(item => item.text)).toEqual([
      "IRC → preview-agent · 未收到回复",
      "IRC 协作",
      `${clockAt(1_000)} · 失败 · 执行器错误 · 140ms`,
    ]);
  });

  test("keeps stored step times in the terminal card replayed after a restart", () => {
    const tracker = createExecutionTracker({ runId: "restart-replay", initialAtMs: 1_000 });
    startTurn(tracker, "turn-1", 1_000);
    startTool(tracker, "read", "read", 1_200, "读取配置");
    tracker.apply({ type: "tool_end", toolCallId: "read", atMs: 1_400, status: "succeeded" });

    const restored = restoreExecutionSnapshot(encodeExecutionSnapshot(tracker.snapshot(1_500)), {
      state: "cancelled",
      atMs: 900_000,
    });
    expect(restored).not.toBeNull();
    const rendered = buildExecutionCard(restored!, fullCapabilities);
    expect(rendered).not.toBeNull();
    const status = nodes(rendered!.card as JsonRecord).find(node => (
      typeof node.text === "string"
      && node.text.startsWith(`${clockAt(1_200)} · 已完成 · `)
    ));
    expect(status).toBeDefined();
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
    const timelineItems = timeline.items as JsonRecord[];
    const richRows = timelineItems.filter(item => item.type === "ColumnSet");
    expect(richRows).toHaveLength(3);
    expect(richRows.every(row => row.isVisible !== false)).toBe(true);
    // The trimmed step sits in the section's grey band, which is what the expand control reveals.
    const historyBand = nodeById(card, "timeline_omitted_steps");
    expect(historyBand).toMatchObject({ type: "Container", style: "emphasis", isVisible: false });
    expect(cardText(historyBand)).toContain("步骤 1");
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
      "timeline_omitted_steps",
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
  test("offers the whole-card collapse control while the run is still active", () => {
    const tracker = createExecutionTracker({ runId: "running-collapse" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "读取文件");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    startSubagent(tracker, "child-1", "scout", 3, "运行子代理");
    tracker.apply({
      type: "todo_update",
      atMs: 4,
      op: "init",
      phases: [{ name: "执行阶段", tasks: [{ content: "进行中任务", status: "in_progress" }] }],
    });
    const snapshot = tracker.snapshot(5);
    expect(snapshot.state).toBe("running");
    const rendered = buildExecutionCard(snapshot, fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(nodeById(card, "todo_detail").isVisible).not.toBe(false);
    expect(nodeById(card, "subagent_section").isVisible).not.toBe(false);
    expect(nodeById(card, "timeline_section").isVisible).not.toBe(false);

    const header = nodeById(card, "octo-execution-trace-header");
    expect(nodes(header).some(node => node.id === "btn_collapse_progress")).toBe(true);
    expect(nodeById(card, "btn_collapse_progress")).toMatchObject({ isVisible: true });
    expect(nodeById(card, "btn_expand_progress")).toMatchObject({ isVisible: false });

    const collapseAction = toggleActionById(card, "progress_collapse");
    expect(collapseAction.title).toBe("收起");
    expect(targetElements(collapseAction)).toEqual([
      { elementId: "todo_detail", isVisible: false },
      { elementId: "subagent_section", isVisible: false },
      { elementId: "timeline_section", isVisible: false },
      { elementId: "btn_collapse_progress", isVisible: false },
      { elementId: "btn_expand_progress", isVisible: true },
    ]);
    const expandAction = toggleActionById(card, "progress_expand");
    expect(targetElements(expandAction)).toEqual([
      { elementId: "todo_detail", isVisible: true },
      { elementId: "subagent_section", isVisible: true },
      { elementId: "timeline_section", isVisible: true },
      { elementId: "btn_collapse_progress", isVisible: true },
      { elementId: "btn_expand_progress", isVisible: false },
    ]);
  });

  test("collapses completed progress details behind a header expand control", () => {
    const tracker = createExecutionTracker({ runId: "terminal-collapse" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "完成步骤");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    startSubagent(tracker, "child-1", "scout", 3, "完成子代理");
    endSubagent(tracker, "child-1", 4, "completed", "完成子代理");
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
      interactive: false,
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
    const runningGlyphs = runningNodes.filter(node => node.text === "•");
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
      && node.style === "emphasis"
      && cardText(node).includes("等待子代理")
    ));
    expect(waitingRow).toBeDefined();
    const waitingGlyph = waitingNodes.find(node => node.text === "-" && node.color === "Default");
    expect(waitingGlyph).toBeDefined();
    const waitingStatus = nodes(waitingRow!).find(node => (
      typeof node.text === "string" && node.text.startsWith(`${clockAt(1)} · 等待 · `)
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
    expect(visibleRows).toHaveLength(3);
    expect(nodeById(card, "timeline_hidden_summary").isVisible).not.toBe(false);

    const visibleText = visibleRows.map(row => cardText(row)).join("\n");
    expect(visibleText).not.toContain("步骤 0");
    expect(visibleText).not.toContain("步骤 1");
    expect(visibleText).not.toContain("步骤 2");
    expect(visibleText).not.toContain("步骤 3");
    expect(visibleText).toContain("步骤 4");
    expect(visibleText).toContain("步骤 6");
    // The four trimmed steps are the grey band the expand control reveals.
    const historyBand = nodeById(card, "timeline_omitted_steps");
    expect(historyBand).toMatchObject({ type: "Container", style: "emphasis", isVisible: false });
    const bandText = cardText(historyBand);
    expect(bandText).toContain("步骤 0");
    expect(bandText).toContain("步骤 3");
    expect(nodes(card).some(node => node.id === "collapsed_steps")).toBe(false);

    expect(nodeById(card, "btn_collapse_trace")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_trace")).toMatchObject({ isVisible: true });
    const expandTargets = targetElements(toggleActionById(card, "trace_expand"));
    expect(expandTargets.find(target => target.elementId === "timeline_hidden_summary")).toMatchObject({ isVisible: false });
    expect(expandTargets.find(target => target.elementId === "timeline_omitted_steps")).toMatchObject({ isVisible: true });
  });
  test("keeps only recent trace rows rich while older rows stay compact", () => {
    const tracker = createExecutionTracker({ runId: "compact-older-trace" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 4; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, "read", index * 10 + 1, `步骤 ${index}`);
      tracker.apply({
        type: "tool_end",
        toolCallId,
        atMs: index * 10 + 2,
        status: "succeeded",
      });
    }

    const rendered = buildExecutionCard(tracker.snapshot(100), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const timeline = nodeById(card, "timeline_detail");
    const visibleRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
    expect(visibleRows).toHaveLength(3);
    expect(visibleRows.every(row => row.isVisible !== false)).toBe(true);
    expect(nodes(visibleRows[0]!).some(node => node.type === "Container" && node.style === "good")).toBe(true);
    // The trimmed step keeps the compact single-line tier, inside the section's grey band.
    const bandRows = nodeById(card, "timeline_omitted_steps").items as JsonRecord[];
    expect(bandRows).toHaveLength(1);
    expect(bandRows[0]).toMatchObject({ type: "TextBlock", wrap: true });
    expect(cardText(bandRows[0]!)).toContain("步骤 0");
  });
  test("keeps the todo list whole and trims the execution trace first", () => {
    const tracker = createExecutionTracker({ runId: "trace-budget-toggle" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "read", "read", 100, "读取项目结构");
    tracker.apply({ type: "tool_end", toolCallId: "read", atMs: 200, status: "succeeded" });
    startTool(tracker, "write", "write", 220, "写入配置");
    tracker.apply({ type: "tool_end", toolCallId: "write", atMs: 320, status: "succeeded" });
    startTool(tracker, "edit", "edit", 340, "修改卡片布局");
    tracker.apply({ type: "tool_end", toolCallId: "edit", atMs: 460, status: "failed" });
    startTool(tracker, "bash", "bash", 480, "运行回归检查");
    tracker.apply({ type: "tool_end", toolCallId: "bash", atMs: 600, status: "cancelled" });
    startTool(tracker, "reviewer", "task", 620, "子代理 · reviewer");
    tracker.apply({ type: "tool_end", toolCallId: "reviewer", atMs: 740, status: "failed" });
    startSubagent(tracker, "reviewer-child", "reviewer", 620, "复核变更");
    endSubagent(tracker, "reviewer-child", 740, "failed", "复核变更");
    startTool(tracker, "verifier", "subagent_await", 780, "等待子代理 · verifier");
    startSubagent(tracker, "verifier-child", "verifier", 780, "验证变更");
    startTool(tracker, "scout", "task", 860, "子代理 · scout");
    tracker.apply({ type: "tool_end", toolCallId: "scout", atMs: 1_000, status: "succeeded" });
    startSubagent(tracker, "scout-child", "scout", 860, "统计源码");
    endSubagent(tracker, "scout-child", 1_000, "completed", "统计源码");
    startTool(tracker, "hub", "hub", 1_020, "IRC → preview-agent · 未收到回复");
    tracker.apply({ type: "tool_end", toolCallId: "hub", atMs: 1_160, status: "failed" });
    startTool(tracker, "ask", "ask", 1_180, "等待用户回答 · 2 个问题");
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
            { content: "确认移动端布局", status: "pending" },
            { content: "记录验收结果", status: "pending" },
          ],
        },
      ],
    });

    const rendered = buildExecutionCard(tracker.snapshot(1_800), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    // The todo list is the run's contract: at the default budget every task stays visible.
    const todoDetail = nodeById(card, "todo_detail");
    expect(cardText(todoDetail)).not.toContain("已隐藏");
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
    // The trace is the budget donor: its older rows fold into the grey history band.
    const subagentDetail = nodeById(card, "subagent_detail");
    expect(cardText(subagentDetail)).toContain("已隐藏 2 个子代理，共 3 个子代理");
    // The live child keeps its rich row; the finished ones stay behind the history band.
    const liveSubagentRows = (subagentDetail.items as JsonRecord[])
      .filter(item => item.type === "ColumnSet" && item.isVisible !== false);
    expect(liveSubagentRows).toHaveLength(1);
    expect(cardText(liveSubagentRows[0]!)).toContain("verifier");
    const hiddenSubagentSteps = nodeById(subagentDetail, "subagent_omitted_steps");
    expect(hiddenSubagentSteps).toMatchObject({ isVisible: false });
    expect(cardText(hiddenSubagentSteps)).toContain("统计源码");
    const timelineSection = nodeById(card, "timeline_section");
    const timelineHeader = (timelineSection.items as JsonRecord[])[0]!;
    expect(nodes(timelineHeader).some(node => node.id === "btn_expand_trace")).toBe(true);
    const timeline = nodeById(card, "timeline_detail");
    expect(cardText(timeline)).toContain("已隐藏前");

    // Under a tighter budget the trace sheds another row rather than the todo list.
    const fullyBudgeted = buildExecutionCard(
      tracker.snapshot(1_800),
      {
        ...fullCapabilities,
        limits: { ...fullCapabilities.limits, maxNodes: 195 },
      },
    );
    expect(fullyBudgeted).not.toBeNull();
    const fullyBudgetedCard = fullyBudgeted!.card as JsonRecord;
    expect(visibleTodoTaskLabels(fullyBudgetedCard)).toHaveLength(12);
    expect(nodes(fullyBudgetedCard).some(node => node.id === "btn_expand_trace")).toBe(true);
    expect(nodes(fullyBudgetedCard).some(node => node.id === "btn_expand_subagents")).toBe(true);
  });
  test("centres the visible todo band on the handoff instead of the list ends", () => {
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
    const card = rendered!.card as JsonRecord;
    const labels = visibleTodoTaskLabels(card);
    // The band starts on the in-progress task and runs contiguously through the open tasks, so the
    // completed ↔ open boundary reads top-down instead of the middle being punched out.
    expect(labels.slice(0, 4)).toEqual(["task-0", "task-1", "task-2", "task-3"]);
    expect(labels.length).toBeGreaterThan(8);
    expect(labels.length).toBeLessThan(110);
    // A blocked task is pinned wherever it sits, so waiting work never disappears.
    expect(labels).toContain("task-55 · 等待外部依赖");
    const text = cardText(card);
    expect(text).toContain("共 110 个任务");
    const hidden = text.match(/已隐藏 (\d+) 个任务/);
    expect(hidden).not.toBeNull();
    expect(Number(hidden?.[1])).toBeGreaterThan(0);
  });
  test("drops phase rows that have neither visible nor expandable tasks", () => {
    // A phase title with nothing under it is noise: the card must render a phase only when it
    // actually shows a task or offers a hidden row to expand.
    const tracker = createExecutionTracker({ runId: "bare-phase" });
    startTurn(tracker, "turn-1", 0);
    tracker.apply({
      type: "todo_update",
      atMs: 1,
      op: "init",
      phases: [
        {
          name: "Active phase",
          tasks: [
            { content: "active-0", status: "in_progress" },
            { content: "active-1", status: "pending" },
          ],
        },
        {
          name: "Middle phase",
          tasks: Array.from({ length: 50 }, (_unused, index) => ({
            content: `middle-${index}`,
            status: "pending" as const,
          })),
        },
        {
          name: "Far phase",
          tasks: Array.from({ length: 40 }, (_unused, index) => ({
            content: `far-${index}`,
            status: "pending" as const,
          })),
        },
      ],
    });
    const rendered = buildExecutionCard(tracker.snapshot(2), fullCapabilities);

    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const todoDetail = nodeById(card, "todo_detail");
    const todoText = cardText(todoDetail);
    expect(todoText).toContain("Active phase");
    expect(todoText).toContain("active-0");
    expect(todoText).toContain("Middle phase");
    // The whole far phase sits past the visible band and past the expandable hidden rows.
    expect(todoText).not.toContain("Far phase");
    const phaseContainers = (todoDetail.items as JsonRecord[]).filter(item => item.type === "Container");
    expect(phaseContainers).not.toHaveLength(0);
    for (const phase of phaseContainers) {
      expect(nodes(phase).some(node => node.type === "ColumnSet")).toBe(true);
    }
  });
  test("keeps a long run's todo handoff readable under the real node budget", () => {
    // The reported card: 96 turns, 116 tool calls, and a todo list gutted to two rows with bare
    // phase titles left behind. The trace must be the content that gives way.
    const tracker = createExecutionTracker({ runId: "hardening-run", initialAtMs: 1_000 });
    for (let turn = 0; turn < 96; turn += 1) {
      tracker.apply({ type: "turn_start", turnId: `turn-${turn}`, atMs: 1_000 + turn * 9_000 });
      tracker.apply({ type: "turn_end", turnId: `turn-${turn}`, atMs: 1_000 + turn * 9_000 + 4_000 });
    }
    for (let index = 0; index < 116; index += 1) {
      const toolCallId = `tool-${index}`;
      const atMs = 2_000 + index * 7_500;
      startTool(tracker, toolCallId, "bash", atMs, `步骤 ${index}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: atMs + 1_200, status: "succeeded" });
    }
    tracker.apply({
      type: "todo_update",
      atMs: 800_000,
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

    const rendered = buildExecutionCard(tracker.snapshot(870_000), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    // The handoff band stays readable: the in-progress task and every neighbour around it.
    expect(visibleTodoTaskLabels(card)).toEqual([
      "Transport: error code + fresh-connection option",
      "Service: bounded retry on transient delete failure",
      "Cover the new retry path",
      "Re-run lifecycle suite",
      "Record the option in the design spec",
      "Send the live preview card",
      "Confirm on the real client",
    ]);
    // No bare phase titles: every rendered phase carries at least one task row.
    const phaseContainers = (nodeById(card, "todo_detail").items as JsonRecord[])
      .filter(item => item.type === "Container");
    expect(phaseContainers).toHaveLength(4);
    for (const phase of phaseContainers) {
      expect(nodes(phase).some(node => node.type === "ColumnSet")).toBe(true);
    }
    // The trace is what compresses: newest steps keep their panels and the rest fold away.
    const timeline = nodeById(card, "timeline_detail");
    const timelineText = cardText(timeline);
    expect(timelineText).toContain("已隐藏前");
    expect(timelineText).toContain("步骤 115");
    const richRows = (timeline.items as JsonRecord[]).filter(item => item.type === "ColumnSet");
    expect(richRows.length).toBeLessThanOrEqual(3);
    expect(cardStats(card).nodes).toBeLessThanOrEqual(fullCapabilities.limits!.maxNodes! - 1);
  });
  test("offers an expand control for the budget-hidden tasks nearest the band", () => {
    const tracker = createExecutionTracker({ runId: "todo-expand" });
    startTurn(tracker, "turn-1", 0);
    tracker.apply({
      type: "todo_update",
      atMs: 1,
      op: "init",
      phases: [{
        name: "Plan",
        tasks: Array.from({ length: 110 }, (_unused, index) => ({
          content: `task-${index}`,
          status: "pending" as const,
        })),
      }],
    });

    const rendered = buildExecutionCard(tracker.snapshot(2), {
      ...fullCapabilities,
      limits: { maxNodes: 200, maxDepth: 16, maxPayloadBytes: 32_768 },
    });
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    const labels = visibleTodoTaskLabels(card);
    expect(labels[0]).toBe("task-0");
    const summary = nodeById(card, "todo_hidden_summary");
    expect(summary.isVisible).not.toBe(false);
    expect(cardText(summary)).toContain("共 110 个任务");

    // The hidden rows continue the band, so expanding grows the readable run outward instead of
    // jumping to the far end of the list.
    const omittedRows = nodes(card).filter(node => (
      typeof node.id === "string" && node.id.startsWith("todo_omitted_task_")
    ));
    expect(omittedRows.length).toBeGreaterThan(0);
    expect(omittedRows.every(row => row.isVisible === false)).toBe(true);
    expect(cardText(omittedRows[0]!)).toContain(`task-${labels.length}`);
    const firstOmitted = omittedRows[0]!;
    expect(firstOmitted).toMatchObject({ type: "ColumnSet" });
    const omittedTaskColumns = firstOmitted.columns as JsonRecord[];
    expect(omittedTaskColumns).toHaveLength(2);
    expect(omittedTaskColumns[0]).toMatchObject({ width: "80px" });
    expect((omittedTaskColumns[0]?.items as JsonRecord[])[0]).toMatchObject({
      fontType: "Monospace",
      size: "Small",
      wrap: false,
    });
    const todoDetail = nodeById(card, "todo_detail");
    const phaseContainers = (todoDetail.items as JsonRecord[]).filter(item => (
      item.type === "Container" && item.id === undefined
    ));
    expect(phaseContainers.some(phase => nodes(phase).some(node => (
      typeof node.id === "string" && node.id.startsWith("todo_omitted_task_")
    )))).toBe(true);
    expect((todoDetail.items as JsonRecord[]).some(item => (
      typeof item.id === "string" && item.id.startsWith("todo_omitted_task_")
    ))).toBe(false);

    const expand = toggleActionById(card, "todo_expand");
    expect(expand.title).toBe("展开");
    const expandedTodoTargets = targetElements(expand).filter(target => (
      typeof target.elementId === "string" && target.elementId.startsWith("todo_omitted_task_")
    ));
    expect(expandedTodoTargets).toHaveLength(omittedRows.length);
    expect(expandedTodoTargets.every(target => target.isVisible === true)).toBe(true);
    const collapse = toggleActionById(card, "todo_collapse");
    expect(collapse.title).toBe("收起");
    const collapsedTodoTargets = targetElements(collapse).filter(target => (
      typeof target.elementId === "string" && target.elementId.startsWith("todo_omitted_task_")
    ));
    expect(collapsedTodoTargets).toHaveLength(expandedTodoTargets.length);
    expect(collapsedTodoTargets.every(target => target.isVisible === false)).toBe(true);
  });
});

// ——— autoresearch 迭代卡 ———
// 共享类型来自 src/research/index.ts；夹具按合同诚实地自洽聚合，经既有 tracker.apply 投递
// research_update 帧，只断言消费者可观察的快照与卡面。

function researchRun(overrides: Partial<ResearchRun> & { readonly runNumber: number | null }): ResearchRun {
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
 * The shared research fixture: eight recorded runs in one segment, with aggregate values that
 * honestly match them — baseline is the first unflagged keep, best the directional extreme of the
 * unflagged keeps, and the flagged run (#8) is what keeps best at 640 despite its lower metric.
 */
function makeResearchFixture(overrides: Partial<ResearchSnapshot> = {}): ResearchSnapshot {
  const recent: ResearchRun[] = [
    researchRun({ runNumber: 1, metric: 800, description: "建立基线测量" }),
    researchRun({ runNumber: 2, metric: 780, description: "减少序列化拷贝" }),
    researchRun({ runNumber: 3, metric: 910, status: "discard", description: "验证激进缓存" }),
    researchRun({ runNumber: 4, metric: 0, status: "crash", description: "基准进程崩溃退出" }),
    researchRun({ runNumber: 5, metric: 700, description: "增加索引" }),
    researchRun({ runNumber: 6, metric: 640, description: "复用连接池" }),
    researchRun({ runNumber: 7, metric: 645, status: "checks_failed", description: "通过基准但检查失败" }),
    researchRun({ runNumber: 8, metric: 620, description: "复测连接池配置", flagged: true }),
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
    completedRuns: 8,
    maxExperiments: 50,
    phase: "running",
    activeRun: 9,
    latestMetric: 620,
    recent,
    totalRuns: 8,
    ...overrides,
  };
}

/** The worker's research frame, delivered to the tracker as an ExecutionEvent by the RPC bridge. */
function applyResearchUpdate(
  tracker: ExecutionTracker,
  research: unknown,
  atMs: number,
  report?: string,
): ExecutionSnapshot {
  // 测试故意喂畸形载荷验证 sanitize 的丢弃边界;这里的断言跨过 unknown,真正的运行时校验
  // 由 tracker 内的 sanitize 执行——与 RPC 解码边界同理。
  return tracker.apply({
    type: "research_update",
    atMs,
    research: research as ResearchSnapshot,
    ...(report === undefined ? {} : { report }),
  });
}

/** Status glyph leading every compact run row; free of any locale wording. */
const RESEARCH_ROW_TEXT = /^[✓×-] · /u;

function researchBandItems(card: JsonRecord): JsonRecord[] {
  const band = nodes(card).find(node => node.id === "research_recent_runs");
  if (!band) return [];
  return (band.items as JsonRecord[] | undefined) ?? [];
}

/** Rows of the recent-run window, identified by their status glyph rather than any wording. */
function researchRunRows(card: JsonRecord): JsonRecord[] {
  return researchBandItems(card).filter(item => (
    item.type === "TextBlock" && typeof item.text === "string" && RESEARCH_ROW_TEXT.test(item.text)
  ));
}

/**
 * The omitted-count figure, wherever the card carries it: with a rendered window it is the
 * band's single non-row line; with the window fully trimmed it stands alone where the fold was.
 * Only the number is read, never the phrasing around it.
 */
function omittedResearchCount(card: JsonRecord): number | null {
  const band = nodes(card).find(node => node.id === "research_recent_runs");
  if (band) {
    const [first] = researchBandItems(card);
    if (!first || (typeof first.text === "string" && RESEARCH_ROW_TEXT.test(first.text))) return 0;
    const match = typeof first.text === "string" ? first.text.match(/\d+/u) : null;
    return match === null ? null : Number(match[0]);
  }
  const line = nodes(card).find(node => node.id === "research_recent_summary");
  if (!line) return null;
  const text = typeof line.text === "string" ? line.text : cardText(line);
  const match = text.match(/\d+/u);
  return match === null ? null : Number(match[0]);
}

/** A running card carrying one research frame, for layout-only assertions. */
function buildResearchCard(
  research: unknown,
  capabilities: ExecutionCardCapabilities = fullCapabilities,
): JsonRecord {
  const tracker = createExecutionTracker({ runId: "research-card" });
  startTurn(tracker, "turn-1", 0);
  startTool(tracker, "tool-1", "bash", 10, "运行基准");
  tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 20, status: "succeeded" });
  applyResearchUpdate(tracker, research, 30);
  const rendered = buildExecutionCard(tracker.snapshot(40), capabilities);
  expect(rendered).not.toBeNull();
  return rendered!.card as JsonRecord;
}

describe("autoresearch research tracking", () => {
  test("projects research_update into the snapshot and keeps it through run_end", () => {
    const tracker = createExecutionTracker({ runId: "research-track" });
    startTurn(tracker, "turn-1", 0);
    const snapshot = applyResearchUpdate(tracker, makeResearchFixture(), 100, "REPORT_ONLY_MARKER_NEVER_IN_SNAPSHOT");
    const research = snapshot.research;
    expect(research).toBeDefined();

    // The full-record report rides the event for the delivery layer; the snapshot the card is
    // rebuilt from never retains it, so a restart cannot resurrect the megabyte payload.
    expect(JSON.stringify(snapshot)).not.toContain("REPORT_ONLY_MARKER_NEVER_IN_SNAPSHOT");

    // Re-applying the same research version is deterministic: the steer-time card carry-over
    // re-seeds a new tracker with the last research snapshot, so a repeat must not drift.
    const reapplied = applyResearchUpdate(tracker, makeResearchFixture(), 120);
    expect(reapplied.research).toEqual(snapshot.research);

    // The projection is sticky: later non-research frames never drop it from the snapshot.
    startTool(tracker, "post-research-tool", "bash", 150, "后续实验");
    expect(tracker.snapshot(160).research).toEqual(reapplied.research);

    // A later frame replaces the projection wholesale instead of merging runs into it.
    const replaced = applyResearchUpdate(
      tracker,
      makeResearchFixture({ phase: "limit_reached", best: 600, latestMetric: 600, activeRun: null }),
      200,
    );
    expect(replaced.research).toMatchObject({ phase: "limit_reached", best: 600 });

    const terminal = tracker.apply({ type: "run_end", atMs: 300, state: "completed" });
    expect(terminal.state).toBe("completed");
    expect(terminal.research).toMatchObject({ name: "p95-latency", best: 600, phase: "limit_reached" });
  });

  test("redacts secrets and bounds text inside research fields", () => {
    const tracker = createExecutionTracker({ runId: "research-sanitize" });
    startTurn(tracker, "turn-1", 0);
    const longDescription = `复用连接池 ${"细".repeat(400)}\n附加 ghp_ABCDEFGHIJKLMNO1`;
    const snapshot = applyResearchUpdate(tracker, makeResearchFixture({
      goal: "压低延迟 Authorization: Bearer sk-live-token-123 泄露检查",
      recent: [researchRun({ runNumber: 1, metric: 640, description: longDescription })],
      completedRuns: 1,
      totalRuns: 1,
      best: 640,
      latestMetric: 640,
      activeRun: null,
    }), 50);
    const research = snapshot.research;
    expect(research).toBeDefined();
    expect(research!.goal).toContain("[redacted]");
    expect(research!.goal).not.toContain("sk-live-token-123");
    const description = research!.recent[0]!.description;
    expect(description).not.toContain("ghp_ABCDEFGHIJKLMNO1");
    expect(description).not.toContain("\n");
    expect(description.length).toBeLessThanOrEqual(241);
    expect(description).toContain("…");
    expect(JSON.stringify(snapshot)).not.toContain("sk-live-token-123");
    expect(JSON.stringify(snapshot)).not.toContain("ghp_ABCDEFGHIJKLMNO1");
  });

  test("drops malformed runs, caps the recent window in record order, and rejects unsalvageable snapshots", () => {
    const tracker = createExecutionTracker({ runId: "research-validity" });
    startTurn(tracker, "turn-1", 0);

    // Per-run drops: an unknown status and a non-finite metric can neither be aggregated nor
    // rendered, while a null runNumber is a legitimate legacy row that must be kept as-is.
    const mixed = applyResearchUpdate(tracker, makeResearchFixture({
      recent: [
        researchRun({ runNumber: 1, metric: 800 }),
        researchRun({ runNumber: null, metric: 790, description: "旧版本记录" }),
        { ...researchRun({ runNumber: 2 }), status: "weird" as never },
        { ...researchRun({ runNumber: 3 }), metric: Number.NaN },
        researchRun({ runNumber: 4, metric: 700 }),
      ],
    }), 10);
    expect(mixed.research).toBeDefined();
    expect(mixed.research!.recent.map(run => [run.runNumber, run.metric])).toEqual([
      [1, 800],
      [null, 790],
      [4, 700],
    ]);

    // The recent window is bounded by record order: past 32 runs only the newest 32 stay, and
    // a null runNumber inside the window is preserved rather than dropped or renumbered.
    const many = applyResearchUpdate(tracker, makeResearchFixture({
      recent: Array.from({ length: 35 }, (_unused, index) => (
        researchRun({ runNumber: index === 3 ? null : index + 1, metric: 800 - index })
      )),
      completedRuns: 35,
      totalRuns: 35,
    }), 20);
    const widowedRecent = many.research!.recent;
    expect(widowedRecent).toHaveLength(32);
    expect(widowedRecent[0]!.runNumber).toBeNull();
    expect(widowedRecent[0]!.metric).toBe(797);
    expect(widowedRecent[31]!.runNumber).toBe(35);

    // Whole-snapshot rejection is narrow: direction is a closed enum the aggregate math depends
    // on, and the metric name anchors every label the card renders. A missing goal is a
    // legitimate study setup and simply normalizes to an empty string.
    const badDirection = applyResearchUpdate(tracker, makeResearchFixture({ direction: "sideways" as never }), 30);
    expect(badDirection.research).toBeUndefined();
    const noMetric = applyResearchUpdate(tracker, makeResearchFixture({ metricName: " " }), 40);
    expect(noMetric.research).toBeUndefined();
    const noGoal = applyResearchUpdate(tracker, makeResearchFixture({ goal: "   " }), 50);
    expect(noGoal.research).toBeDefined();
    expect(noGoal.research!.goal).toBe("");
  });

  test("restores research through the durable payload and stays compatible with old payloads", () => {
    // The stored payload may predate sanitization at encode time; restore replays it through a
    // fresh tracker, so the same redaction rules apply as in the live run.
    const legacyShaped = JSON.stringify({
      runId: "research-restore",
      startedAtMs: 0,
      tools: [],
      todos: [],
      subagents: [],
      research: {
        ...makeResearchFixture({ phase: "awaiting_log", activeRun: null }),
        goal: "压低延迟 token=abc123xyz 附带",
      },
    });
    const restored = restoreExecutionSnapshot(legacyShaped, { state: "completed", atMs: 300 });
    expect(restored).not.toBeNull();
    expect(restored!.state).toBe("completed");
    const research = restored!.research;
    expect(research).toBeDefined();
    expect(research).toMatchObject({ baseline: 800, best: 640, totalRuns: 8, phase: "awaiting_log" });
    expect(research!.goal).toContain("[redacted]");
    expect(research!.goal).not.toContain("abc123xyz");
    const rendered = buildExecutionCard(restored!, fullCapabilities);
    expect(rendered).not.toBeNull();
    expect(cardText(rendered!.card as JsonRecord)).toContain("640");

    // 旧快照兼容：payloads written before research tracking have no research field at all and
    // still restore into the same terminal card.
    const legacyTracker = createExecutionTracker({ runId: "legacy" });
    startTurn(legacyTracker, "turn-1", 0);
    startTool(legacyTracker, "tool-1", "read", 10, "旧任务");
    const legacyTerminal = legacyTracker.apply({ type: "run_end", atMs: 20, state: "completed" });
    const legacyRestored = restoreExecutionSnapshot(encodeExecutionSnapshot(legacyTerminal), { state: "completed", atMs: 30 });
    expect(legacyRestored).not.toBeNull();
    expect(legacyRestored!.research).toBeUndefined();
    expect(buildExecutionCard(legacyRestored!, fullCapabilities)).not.toBeNull();

    // 可空 name/goal：较旧或被 strip 的载荷合法地携带 null，恢复路径不得因此丢失整份研究。
    const namelessShaped = JSON.stringify({
      runId: "research-restore-nameless",
      startedAtMs: 0,
      tools: [],
      todos: [],
      subagents: [],
      research: { ...makeResearchFixture({ phase: "awaiting_log", activeRun: null }), name: null, goal: null },
    });
    const nameless = restoreExecutionSnapshot(namelessShaped, { state: "completed", atMs: 300 });
    expect(nameless).not.toBeNull();
    expect(nameless!.research).toMatchObject({ baseline: 800, best: 640, totalRuns: 8 });
  });

  test("stripCardSummaries removes every research free-text field but keeps the numbers", () => {
    const tracker = createExecutionTracker({ runId: "research-strip" });
    startTurn(tracker, "turn-1", 0);
    applyResearchUpdate(tracker, makeResearchFixture(), 100);
    const terminal = tracker.apply({ type: "run_end", atMs: 200, state: "completed" });

    const stripped = stripCardSummaries(terminal);
    const research = stripped.research;
    expect(research).toBeDefined();
    // The edge retry shows the aggregate math; name, goal, and the model-authored experiment
    // notes are the free text the inspection rules trip over, exactly like tool summaries.
    expect(research!.name).toBe("");
    expect(research!.goal).toBe("");
    for (const run of research!.recent) {
      expect(run.description).toBe("");
    }
    expect(research).toMatchObject({
      direction: "lower",
      baseline: 800,
      best: 640,
      completedRuns: 8,
      maxExperiments: 50,
      totalRuns: 8,
      phase: "running",
    });
    expect(research!.recent[5]).toMatchObject({ runNumber: 6, metric: 640, status: "keep" });
    expect(research!.recent[7]).toMatchObject({ runNumber: 8, metric: 620, status: "keep", flagged: true });

    const rendered = buildExecutionCard(stripped, fullCapabilities);
    expect(rendered).not.toBeNull();
    const text = cardText(rendered!.card as JsonRecord);
    expect(text).not.toContain("复用连接池");
    expect(text).not.toContain("把 API P95 延迟降到 400ms 以下");
    expect(text).toContain("640");
  });
});

describe("autoresearch research card", () => {
  /** A terminal card whose loop logged the eighth run and already knows the ninth run's number. */
  function terminalResearchCard(): JsonRecord {
    const tracker = createExecutionTracker({ runId: "research-card-terminal" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "bash", 10, "运行基准");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 20, status: "succeeded" });
    tracker.apply({
      type: "todo_update",
      atMs: 30,
      op: "init",
      phases: [{ name: "研究", tasks: [{ content: "运行第 9 轮实验", status: "completed" }] }],
    });
    applyResearchUpdate(tracker, makeResearchFixture({ phase: "awaiting_log", activeRun: 9 }), 40);
    tracker.apply({ type: "run_end", atMs: 50, state: "completed" });
    const rendered = buildExecutionCard(tracker.snapshot(50), fullCapabilities);
    expect(rendered).not.toBeNull();
    return rendered!.card as JsonRecord;
  }

  test("keeps the summary always visible with recent runs behind their own toggle", () => {
    const card = terminalResearchCard();
    const summary = nodeById(card, "research_summary");
    expect(summary.isVisible).not.toBe(false);
    const text = cardText(summary);
    expect(text).toContain("把 API P95 延迟降到 400ms 以下");
    expect(text).toContain("p95");
    expect(text).toMatch(/\b800\b/u);
    expect(text).toMatch(/\b640\b/u);
    // The improvement equals twenty percent however it is formatted; only the math is pinned.
    expect(text).toMatch(/20(?:\.0*)?%/u);
    expect(text).toMatch(/\b8\s*\/\s*50\b/u);
    // The loop's next round number shows whenever it is known, not only while phase says running.
    expect(text).toMatch(/\b9\b/u);
    expect(text).toContain("/autoresearch-history");
    // The execution card is the task clock: once the run settles, the research title reports the
    // run's terminal state instead of replaying the loop's own stale phase — the data stays.
    expect(text).toContain("已完成");
    expect(text).not.toContain("等待记录");
    expect(text).not.toContain("运行中");

    // The task's own terminal state is untouched: research phase and run state share the card.
    expect(nodeById(card, "execution_status").isVisible).not.toBe(false);
    expect(cardText(nodeById(card, "execution_status"))).toContain("已完成");

    // Terminal folding tucks todos and the trace away but never the research summary.
    expect(nodeById(card, "todo_detail").isVisible).toBe(false);

    // Recent runs fold behind their own toggle: collapsed line visible, band hidden.
    const recent = nodeById(card, "research_recent_summary");
    expect(recent.isVisible).not.toBe(false);
    const foldedDigits = cardText(recent).match(/\d+/u);
    expect(foldedDigits).not.toBeNull();
    expect(Number(foldedDigits![0])).toBe(6);
    const band = nodeById(card, "research_recent_runs");
    expect(band.isVisible).toBe(false);
    expect(researchRunRows(card)).toHaveLength(6);
    expect(omittedResearchCount(card)).toBe(2);

    const researchIds: Record<string, true> = {
      research_recent_summary: true,
      research_recent_runs: true,
      btn_expand_research: true,
      btn_collapse_research: true,
    };
    const sortedResearchIds = Object.keys(researchIds).sort();
    const expandTargets = targetElements(toggleActionById(card, "research_expand"));
    const collapseTargets = targetElements(toggleActionById(card, "research_collapse"));
    expect(expandTargets.map(target => String(target.elementId)).sort()).toEqual(sortedResearchIds);
    expect(collapseTargets.map(target => String(target.elementId)).sort()).toEqual(sortedResearchIds);
    expect(expandTargets.find(target => target.elementId === "research_recent_runs")).toMatchObject({ isVisible: true });
    expect(collapseTargets.find(target => target.elementId === "research_recent_runs")).toMatchObject({ isVisible: false });
    expect(nodeById(card, "btn_expand_research")).toMatchObject({ isVisible: true });
    expect(nodeById(card, "btn_collapse_research")).toMatchObject({ isVisible: false });

    // Neither fold reaches the other's territory: progress folding ignores research ids, and
    // research folding never touches todo, subagent, trace, or the overall progress controls.
    for (const actionId of ["progress_expand", "progress_collapse"]) {
      for (const target of targetElements(toggleActionById(card, actionId))) {
        expect(String(target.elementId).startsWith("research_")).toBe(false);
      }
    }
    const foreignIds: Record<string, true> = {
      todo_detail: true,
      subagent_section: true,
      timeline_section: true,
      btn_collapse_progress: true,
      btn_expand_progress: true,
    };
    for (const target of [...expandTargets, ...collapseTargets]) {
      expect(foreignIds[String(target.elementId)] ?? false).toBe(false);
    }

    // While the task still runs, the title reports the loop's own phase instead of a run outcome.
    const runningCard = buildResearchCard(makeResearchFixture({ phase: "awaiting_log", activeRun: 9 }));
    const runningText = cardText(nodeById(runningCard, "research_summary"));
    expect(runningText).toContain("等待记录");
    expect(runningText).not.toContain("已完成");
  });

  test("renders the newest runs with their metrics and flags the ones that need it", () => {
    const card = terminalResearchCard();
    const bandText = cardText(nodeById(card, "research_recent_runs"));
    expect(researchRunRows(card)).toHaveLength(6);
    // The visible window is the newest six runs (#3 through #8): every metric and note in the
    // window shows, the two oldest runs drop out entirely.
    for (const metric of ["910", "700", "640", "645", "620"]) {
      expect(bandText).toContain(metric);
    }
    expect(bandText).toMatch(/\b0\b/u);
    for (const note of ["验证激进缓存", "基准进程崩溃退出", "增加索引", "复用连接池", "通过基准但检查失败", "复测连接池配置"]) {
      expect(bandText).toContain(note);
    }
    expect(bandText).not.toContain("建立基线测量");
    expect(bandText).not.toContain("减少序列化拷贝");
    // The flagged run is called out so its lower number is not mistaken for a kept best.
    expect(bandText).toMatch(/标|flagged/iu);

    // A null runNumber is a legitimate legacy record: the row keeps it without inventing an id.
    const legacyCard = buildResearchCard(makeResearchFixture({
      recent: [researchRun({ runNumber: null, metric: 640, description: "旧版本记录" })],
      completedRuns: 1,
      totalRuns: 1,
      latestMetric: 640,
      activeRun: null,
    }));
    const legacyBandText = cardText(nodeById(legacyCard, "research_recent_runs"));
    expect(legacyBandText).toContain("旧版本记录");
    expect(legacyBandText).toContain("640");
    expect(legacyBandText).not.toContain("#");
  });

  test("handles zero, negative, and large finite baselines", () => {
    // 零基线:higher 方向 0→5 无百分比可给——两个数照常显示,绝不出现无穷。
    const zeroBaseline = buildResearchCard(makeResearchFixture({
      direction: "higher",
      metricName: "throughput",
      metricUnit: "rps",
      baseline: 0,
      best: 5,
      latestMetric: 5,
      completedRuns: 2,
      totalRuns: 2,
      maxExperiments: 20,
      activeRun: null,
      recent: [
        researchRun({ runNumber: 1, metric: 0 }),
        researchRun({ runNumber: 2, metric: 5 }),
      ],
    }));
    const zeroText = cardText(nodeById(zeroBaseline, "research_summary"));
    expect(zeroText).toContain("throughput");
    expect(zeroText).toMatch(/\b0\b/u);
    expect(zeroText).toMatch(/\b5\b/u);
    expect(zeroText).not.toContain("∞");
    expect(zeroText).not.toContain("Infinity");
    expect(zeroText).not.toContain("NaN");
    expect(zeroText).not.toContain("%");

    // 负基线:higher 方向 -10→-5 是 +50% 改善;幅度按 |baseline| 归一,绝不反号成 -50%。
    const negativeBaseline = buildResearchCard(makeResearchFixture({
      direction: "higher",
      metricName: "loss",
      metricUnit: "",
      baseline: -10,
      best: -5,
      latestMetric: -5,
      completedRuns: 2,
      totalRuns: 2,
      activeRun: null,
      recent: [
        researchRun({ runNumber: 1, metric: -10 }),
        researchRun({ runNumber: 2, metric: -5 }),
      ],
    }));
    const negativeText = cardText(nodeById(negativeBaseline, "research_summary"));
    expect(negativeText).toMatch(/50(?:\.0*)?%/u);
    expect(negativeText).not.toMatch(/-50(?:\.0*)?%/u);

    // Normalizing first preserves a representable 200% improvement even when the raw difference overflows.
    const overflowing = buildResearchCard(makeResearchFixture({
      direction: "lower",
      baseline: 1e308,
      best: -1e308,
      latestMetric: -1e308,
      completedRuns: 2,
      totalRuns: 2,
      activeRun: null,
      recent: [
        researchRun({ runNumber: 1, metric: 1e308 }),
        researchRun({ runNumber: 2, metric: -1e308 }),
      ],
    }));
    const overflowText = cardText(nodeById(overflowing, "research_summary"));
    expect(overflowText).not.toContain("Infinity");
    expect(overflowText).not.toContain("∞");
    expect(overflowText).toMatch(/200(?:\.0*)?%/u);
  });

  test("renders a fresh research state without inventing numbers it does not have", () => {
    const card = buildResearchCard(makeResearchFixture({
      baseline: null,
      best: null,
      phase: "ready",
      completedRuns: 0,
      maxExperiments: null,
      activeRun: null,
      latestMetric: null,
      recent: [],
      totalRuns: 0,
    }));
    const text = cardText(nodeById(card, "research_summary"));
    expect(text).toContain("把 API P95 延迟降到 400ms 以下");
    expect(text).toContain("/autoresearch-history");
    // Zero recorded rounds is spelled out as the number 0, never as a non-finite placeholder.
    expect(text).toMatch(/\b0\b/u);
    expect(text).not.toContain("%");
    expect(text).not.toContain("null");
    expect(text).not.toContain("NaN");
    // No recorded runs yet: nothing to fold, so neither the band nor the toggle exists.
    expect(nodes(card).some(node => node.id === "research_recent_runs")).toBe(false);
    expect(nodes(card).some(node => node.id === "btn_expand_research")).toBe(false);
  });

  test("keeps research readable when toggles or layout elements are unavailable", () => {
    const noToggle = buildResearchCard(makeResearchFixture({ activeRun: null }), {
      ...fullCapabilities,
      actions: ["Action.OpenUrl"],
    });
    expect(nodes(noToggle).some(node => node.id === "btn_expand_research")).toBe(false);
    expect(nodes(noToggle).some(node => node.type === "Action.ToggleVisibility")).toBe(false);
    const band = nodeById(noToggle, "research_recent_runs");
    expect(band.isVisible).not.toBe(false);
    // 波段直接可见时,再说"已折叠"自相矛盾:折叠提示只跟折叠控件一起出现。
    expect(nodes(noToggle).some(node => node.id === "research_recent_summary")).toBe(false);
    expect(researchRunRows(noToggle)).toHaveLength(6);
    expect(cardText(band)).toContain("复测连接池配置");
    expect(cardText(nodeById(noToggle, "research_summary"))).toContain("把 API P95 延迟降到 400ms 以下");

    const flat = buildResearchCard(makeResearchFixture({ activeRun: null }), {
      ...fullCapabilities,
      elements: ["TextBlock"],
      actions: ["Action.OpenUrl"],
    });
    const flatText = cardText(flat);
    expect(flatText).toContain("把 API P95 延迟降到 400ms 以下");
    expect(flatText).toContain("640");
    expect(flatText).toContain("复用连接池");
    expect(flatText).toContain("/autoresearch-history");

    const tracker = createExecutionTracker({ runId: "research-flat-budget" });
    applyResearchUpdate(tracker, makeResearchFixture({ activeRun: null }), 10);
    const constrained = buildExecutionCard(tracker.snapshot(20), {
      ...fullCapabilities,
      elements: ["TextBlock"],
      actions: [],
      limits: { ...fullCapabilities.limits, maxNodes: 18 },
    });
    expect(constrained).not.toBeNull();
    expect(cardText(constrained!.card as JsonRecord)).toContain("把 API P95 延迟降到 400ms 以下");
    expect(constrained!.plain).toContain("640");
    expect(constrained!.plain).toContain("/autoresearch-history");
    expect((constrained!.plain.match(/#\d+/gu) ?? []).length).toBeLessThan(6);
  });

  test("keeps the research summary and todo handoff while the trace folds under the node budget", () => {
    // The reported long run, plus a live research loop: 96 turns, 116 tool calls, a 7-task todo
    // handoff, and 8 recorded research runs against the real 200-node envelope.
    const tracker = createExecutionTracker({ runId: "research-budget", initialAtMs: 1_000 });
    for (let turn = 0; turn < 96; turn += 1) {
      tracker.apply({ type: "turn_start", turnId: `turn-${turn}`, atMs: 1_000 + turn * 9_000 });
      tracker.apply({ type: "turn_end", turnId: `turn-${turn}`, atMs: 1_000 + turn * 9_000 + 4_000 });
    }
    for (let index = 0; index < 116; index += 1) {
      const toolCallId = `tool-${index}`;
      const atMs = 2_000 + index * 7_500;
      startTool(tracker, toolCallId, "bash", atMs, `步骤 ${index}`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: atMs + 1_200, status: "succeeded" });
    }
    tracker.apply({
      type: "todo_update",
      atMs: 800_000,
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
    applyResearchUpdate(tracker, makeResearchFixture({ activeRun: null }), 810_000);

    const rendered = buildExecutionCard(tracker.snapshot(870_000), fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;
    expect(cardStats(card).nodes).toBeLessThanOrEqual(fullCapabilities.limits!.maxNodes! - 1);

    // The research summary is never the thing the budget search sacrifices.
    const summary = nodeById(card, "research_summary");
    expect(summary.isVisible).not.toBe(false);
    const summaryText = cardText(summary);
    expect(summaryText).toContain("把 API P95 延迟降到 400ms 以下");
    expect(summaryText).toContain("640");
    expect(summaryText).toContain("/autoresearch-history");

    // The todo handoff stays readable: the trace, not the todo list or the research summary, is
    // the budget donor.
    const labels = visibleTodoTaskLabels(card);
    expect(labels).toContain("Transport: error code + fresh-connection option");
    expect(labels).toContain("Service: bounded retry on transient delete failure");
    expect(cardText(nodeById(card, "timeline_detail"))).toContain("已隐藏前");

    // Whatever rung the recent-run ladder landed on — all the way down to zero rows — the omitted
    // count honestly reports how many of the recorded runs are not shown.
    const rows = researchRunRows(card);
    expect(rows.length).toBeLessThanOrEqual(6);
    expect(omittedResearchCount(card)).toBe(8 - rows.length);
    if (rows.length === 0) {
      expect(nodes(card).some(node => node.id === "btn_expand_research")).toBe(false);
    }
  });

  test("degrades the recent-run window through 6/3/1/0 and keeps the omitted count exact", () => {
    const tracker = createExecutionTracker({ runId: "research-ladder" });
    startTurn(tracker, "turn-1", 0);
    for (let index = 0; index < 40; index += 1) {
      const toolCallId = `tool-${index}`;
      startTool(tracker, toolCallId, "read", index * 2 + 1, `file-${index}.ts`);
      tracker.apply({ type: "tool_end", toolCallId, atMs: index * 2 + 2, status: "succeeded" });
    }
    tracker.apply({
      type: "todo_update",
      atMs: 90,
      op: "init",
      phases: [
        {
          name: "准备",
          tasks: [
            { content: "读取配置", status: "completed" },
            { content: "确认基准", status: "completed" },
            { content: "清理缓存", status: "abandoned" },
            { content: "同步依赖", status: "completed" },
          ],
        },
        {
          name: "实验",
          tasks: [
            { content: "运行第 33 轮", status: "in_progress" },
            { content: "记录指标", status: "pending" },
            { content: "对比基线", status: "pending" },
            { content: "更新笔记", status: "pending" },
          ],
        },
        {
          name: "交付",
          tasks: [
            { content: "汇总结果", status: "pending" },
            { content: "发送通知", status: "pending" },
            { content: "归档记录", status: "pending" },
            { content: "复核变更", status: "pending" },
          ],
        },
      ],
    });
    // One hundred recorded rounds in the segment, of which only the newest 32 are carried in
    // recent: the omitted count must be honest about all one hundred, not just the window.
    applyResearchUpdate(tracker, makeResearchFixture({
      recent: Array.from({ length: 32 }, (_unused, index) => (
        researchRun({ runNumber: index + 69, metric: 800 - index * 3 })
      )),
      completedRuns: 100,
      totalRuns: 100,
      latestMetric: 707,
      activeRun: null,
    }), 100);
    const snapshot = tracker.snapshot(120);

    let sawViableRung = false;
    let sawDegradedRung = false;
    for (const maxNodes of [200, 150, 130, 110, 90, 70, 50]) {
      const rendered = buildExecutionCard(snapshot, {
        ...fullCapabilities,
        limits: { ...fullCapabilities.limits, maxNodes },
      });
      if (rendered === null) continue;
      sawViableRung = true;
      const card = rendered!.card as JsonRecord;
      expect(cardStats(card).nodes).toBeLessThanOrEqual(maxNodes - 1);
      // The summary survives every rung; only the recent-run window gives way.
      const summary = nodeById(card, "research_summary");
      expect(summary.isVisible).not.toBe(false);
      expect(cardText(summary)).toContain("把 API P95 延迟降到 400ms 以下");
      const rows = researchRunRows(card);
      expect(rows.length).toBeLessThanOrEqual(6);
      // 展示 R 行时省略数必须按当前段全部 100 轮计算：94/97/99/100。
      expect(omittedResearchCount(card)).toBe(100 - rows.length);
      if (rows.length === 6) continue;
      sawDegradedRung = true;
      if (rows.length === 0) {
        // No rows left to expand: the fold controls go away instead of hiding an empty band.
        expect(nodes(card).some(node => node.id === "btn_expand_research")).toBe(false);
      }
    }
    expect(sawViableRung).toBe(true);
    expect(sawDegradedRung).toBe(true);
  });
});
