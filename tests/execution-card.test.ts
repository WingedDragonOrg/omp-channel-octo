import { describe, expect, test } from "bun:test";
import {
  buildExecutionCard,
  createExecutionTracker,
  encodeExecutionSnapshot,
  restoreExecutionSnapshot,
  type ExecutionCardCapabilities,
  type ExecutionEvent,
  type ExecutionSnapshot,
  type ExecutionState,
  type ExecutionTracker,
} from "../src/execution-card.ts";

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

  test("renders all subagent activity independently of the recent trace window", () => {
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
    expect(visibleRows).toHaveLength(4);
    expect(hiddenRows).toHaveLength(0);
    const visibleText = visibleRows.map(row => cardText(row)).join("\n");
    expect(visibleText).toContain("子代理步骤 1");
    expect(visibleText).toContain("子代理步骤 2");
    expect(visibleText).toContain("子代理步骤 3");
    expect(visibleText).toContain("子代理步骤 4");
    expect(cardText(timeline)).toContain("src/service.ts");
    expect(cardText(timeline)).not.toContain("子代理步骤 1");
    expect(nodes(card).some(node => node.id === "btn_collapse_subagents")).toBe(false);
    expect(nodes(card).some(node => node.id === "btn_expand_subagents")).toBe(false);
  });
  test("expands older rich rows when ToggleVisibility is unavailable", () => {
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
    const items = timeline.items as JsonRecord[];
    const richRows = items.filter(item => item.type === "ColumnSet");
    const compactRows = items.filter(item => (
      item.type === "TextBlock" && typeof item.text === "string" && item.text.includes("轨迹步骤 1")
    ));
    expect(richRows).toHaveLength(3);
    expect(compactRows).toHaveLength(1);
    expect(compactRows.every(row => row.isVisible !== false)).toBe(true);
    expect(cardText(timeline)).toContain("轨迹步骤 1");
    expect(cardText(timeline)).toContain("轨迹步骤 4");
    expect(nodes(card).some(node => node.id === "btn_expand_trace")).toBe(false);
  });
  test("uses the same compact row layout for omitted subagents and trace steps", () => {
    const tracker = createExecutionTracker({ runId: "compact-subagents" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "verifier", "task", 1, "子代理 · verifier");
    tracker.apply({ type: "tool_end", toolCallId: "verifier", atMs: 2, status: "succeeded" });
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

    for (const row of [traceRow, subagentRow]) {
      expect(row).toMatchObject({ type: "TextBlock", wrap: true, spacing: "Small" });
      expect(row.text).toMatch(/^[✓×•-]\u00a0· /u);
    }
    expect(subagentRow).toMatchObject({
      color: "Good",
      text: "✓\u00a0· 运行子代理 · 子代理 · verifier · 已完成 · 1ms",
    });
    expect(cardText(subagentRow)).toContain("子代理 · verifier");
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
    const olderRows = timelineItems.filter(item => (
      item.type === "TextBlock" && item.id === "timeline_older_step_0"
    ));
    expect(richRows).toHaveLength(3);
    expect(richRows.every(row => row.isVisible !== false)).toBe(true);
    expect(olderRows).toHaveLength(1);
    expect(olderRows[0]).toMatchObject({ isVisible: false, wrap: true });
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
  test("offers the whole-card collapse control while the run is still active", () => {
    const tracker = createExecutionTracker({ runId: "running-collapse" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 1, "读取文件");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 2, status: "succeeded" });
    startTool(tracker, "subagent-1", "task", 3, "运行子代理");
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
    const olderRows = timelineItems.filter(item => (
      item.type === "TextBlock"
      && typeof item.id === "string"
      && item.id.startsWith("timeline_older_step_")
    ));
    expect(visibleRows).toHaveLength(3);
    expect(olderRows).toHaveLength(4);
    expect(olderRows.every(row => row.isVisible === false)).toBe(true);
    expect(nodeById(card, "timeline_hidden_summary").isVisible).not.toBe(false);

    const visibleText = visibleRows.map(row => cardText(row)).join("\n");
    const hiddenText = olderRows.map(row => cardText(row)).join("\n");
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
    const items = timeline.items as JsonRecord[];
    const visibleRows = items.filter(item => item.type === "ColumnSet");
    const olderRows = items.filter(item => (
      item.type === "TextBlock"
      && typeof item.id === "string"
      && item.id.startsWith("timeline_older_step_")
    ));
    expect(visibleRows).toHaveLength(3);
    expect(visibleRows.every(row => row.isVisible !== false)).toBe(true);
    expect(nodes(visibleRows[0]!).some(node => node.type === "Container" && node.style === "good")).toBe(true);
    expect(olderRows).toHaveLength(1);
    expect(olderRows[0]).toMatchObject({ type: "TextBlock", isVisible: false, wrap: true });
    expect(cardText(olderRows[0]!)).toContain("步骤 0");
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
    startTool(tracker, "verifier", "subagent_await", 780, "等待子代理 · verifier");
    startTool(tracker, "scout", "task", 860, "子代理 · scout");
    tracker.apply({ type: "tool_end", toolCallId: "scout", atMs: 1_000, status: "succeeded" });
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
    expect(cardText(subagentDetail)).toContain("等待子代理 · verifier");
    const hiddenSubagentSteps = nodeById(subagentDetail, "subagent_omitted_steps");
    expect(hiddenSubagentSteps).toMatchObject({ isVisible: false });
    expect(cardText(hiddenSubagentSteps)).toContain("子代理 · scout");
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
