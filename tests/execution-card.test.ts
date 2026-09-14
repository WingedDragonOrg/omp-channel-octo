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
    expect(cardText(blockedRendered!.card as JsonRecord)).toContain("等待授权");

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
    expect(text).not.toContain("等待授权");
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

  test("uses a compact hierarchy with aligned status metadata for tool rows", () => {
    const tracker = createExecutionTracker({ runId: "tool-row" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "generate_image", 1_000, "写入文件");
    const rendered = buildExecutionCard(tracker.snapshot(36_200), fullCapabilities);
    expect(rendered).not.toBeNull();

    const timeline = nodeById(rendered!.card as JsonRecord, "timeline_detail");
    const row = (timeline.items as JsonRecord[]).find(item => item.type === "ColumnSet");
    expect(row).toMatchObject({ spacing: "Small" });
    const columns = row?.columns as JsonRecord[];
    expect(columns).toHaveLength(3);
    expect(columns[0]).toMatchObject({ width: "auto", verticalContentAlignment: "Center" });
    expect(columns[1]?.items).toEqual([
      expect.objectContaining({ text: "写入文件", weight: "Bolder", spacing: "None" }),
      expect.objectContaining({ text: "generate_image", isSubtle: true, size: "Small", spacing: "None" }),
    ]);
    expect(columns[2]?.items).toEqual([
      expect.objectContaining({
        text: "进行中 · 35.2s",
        horizontalAlignment: "Right",
        isSubtle: true,
        size: "Small",
        spacing: "None",
      }),
    ]);
  });

  test("keeps todos visible while terminal execution details are collapsed", () => {
    const tracker = createExecutionTracker({ runId: "fold" });
    startTurn(tracker, "turn-1", 0);
    startTool(tracker, "tool-1", "read", 10, "a.ts");
    tracker.apply({ type: "tool_end", toolCallId: "tool-1", atMs: 20, status: "succeeded", summary: "完成" });
    tracker.apply({
      type: "todo_update",
      atMs: 25,
      op: "init",
      phases: [{ name: "Plan", tasks: [{ content: "Keep this visible", status: "in_progress" }] }],
    });
    const snapshot = tracker.apply({ type: "run_end", atMs: 30, state: "completed" });
    const rendered = buildExecutionCard(snapshot, fullCapabilities);
    expect(rendered).not.toBeNull();
    const card = rendered!.card as JsonRecord;

    const timeline = nodeById(card, "timeline_detail");
    const collapsed = nodeById(card, "collapsed_steps");
    const todos = nodeById(card, "todo_detail");
    expect(timeline.isVisible).toBe(false);
    expect(collapsed.isVisible).toBe(true);
    expect(todos.isVisible).not.toBe(false);
    expect(cardText(todos)).toContain("Keep this visible");

    const collapseTargets = targetElements(toggleActionById(card, "trace_collapse"));
    const expandTargets = targetElements(toggleActionById(card, "trace_expand"));
    const collapseIds = new Set(collapseTargets.map(target => target.elementId as string));
    const expandIds = new Set(expandTargets.map(target => target.elementId as string));
    expect(collapseIds).toEqual(expandIds);
    expect(collapseIds).toEqual(new Set(["timeline_detail", "collapsed_steps", "btn_collapse_trace", "btn_expand_trace"]));
    expect(collapseIds.has("todo_detail")).toBe(false);

    for (const id of collapseIds) {
      const collapsedTarget = collapseTargets.find(target => target.elementId === id)!;
      const expandedTarget = expandTargets.find(target => target.elementId === id)!;
      expect(collapsedTarget.isVisible).toBe(!expandedTarget.isVisible);
    }
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
  });

  test("uses semantic colors for running and terminal status markers", () => {
    const runningTracker = createExecutionTracker({ runId: "colors-running" });
    startTurn(runningTracker, "turn-1", 0);
    startTool(runningTracker, "running-tool", "read", 1, "a.ts");
    const running = buildExecutionCard(runningTracker.snapshot(2), fullCapabilities);
    expect(running).not.toBeNull();
    const runningGlyph = nodes(running!.card as JsonRecord).find(node => node.text === "◉");
    expect(runningGlyph).toMatchObject({ color: "Accent" });
    expect(nodes(running!.card as JsonRecord).filter(node => node.text === "◉")).toHaveLength(1);
    const runningStatus = nodes(running!.card as JsonRecord).find(node => node.text === "进行中");
    expect(runningStatus).toMatchObject({ color: "Accent" });

    const terminalTracker = createExecutionTracker({ runId: "colors-terminal" });
    startTurn(terminalTracker, "turn-1", 0);
    startTool(terminalTracker, "failed-tool", "bash", 1, "bun test");
    terminalTracker.apply({
      type: "tool_end",
      toolCallId: "failed-tool",
      atMs: 2,
      status: "failed",
      errorCategory: "worker",
    });
    const terminal = buildExecutionCard(
      terminalTracker.apply({ type: "run_end", atMs: 3, state: "failed" }),
      fullCapabilities,
    );
    expect(terminal).not.toBeNull();
    const failedGlyph = nodes(terminal!.card as JsonRecord).find(node => node.text === "○");
    expect(failedGlyph).toMatchObject({ color: "Attention" });
    const failedStatus = nodes(terminal!.card as JsonRecord).find(node => node.text === "失败");
    expect(failedStatus).toMatchObject({ color: "Attention" });
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
