/**
 * 执行状态机与快照:一次任务回合里的工具、子代理与 todo 进度,由 OMP 事件折叠成
 * `ExecutionSnapshot`,并在重启后从持久化 JSON 复原。这里只维护状态和安全边界
 * (长度、阈值、脱敏),卡片如何呈现由 `./execution-render.ts` 与 `./execution-card.ts` 决定。
 */

export type ExecutionState = "running" | "completed" | "failed" | "cancelled";
export type ExecutionToolState = "running" | "succeeded" | "failed" | "cancelled";
export type ExecutionErrorCategory =
  | "timeout"
  | "permission"
  | "worker"
  | "cancelled"
  | "unavailable"
  | "unknown";

export type ExecutionSubagentState = "pending" | "running" | "completed" | "failed" | "cancelled";

export type TodoStatus = "pending" | "in_progress" | "completed" | "abandoned" | "blocked" | "cancelled";
export type TodoOperation = "init" | "start" | "done" | "rm" | "drop" | "block" | "unblock" | "append" | "view";

export interface ExecutionTodoTask {
  readonly content: string;
  readonly status: TodoStatus;
  readonly blocker?: string;
}

export interface ExecutionTodoPhase {
  readonly name: string;
  readonly tasks: readonly ExecutionTodoTask[];
}

export type ExecutionEvent =
  | {
      readonly type: "turn_start";
      readonly turnId: string | number;
      readonly atMs: number;
    }
  | {
      readonly type: "turn_end";
      readonly turnId?: string | number;
      readonly atMs: number;
    }
  | {
      readonly type: "tool_start";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly atMs: number;
      readonly summary?: string;
    }
  | {
      readonly type: "tool_end";
      readonly toolCallId: string;
      readonly atMs: number;
      readonly status: Exclude<ExecutionToolState, "running">;
      readonly summary?: string;
      readonly errorCategory?: ExecutionErrorCategory;
    }
  | {
      readonly type: "todo_update";
      readonly atMs: number;
      readonly op?: TodoOperation;
      readonly phases: readonly ExecutionTodoPhase[];
    }
  | {
      readonly type: "run_end";
      readonly atMs: number;
      readonly state: Exclude<ExecutionState, "running">;
      readonly errorCategory?: ExecutionErrorCategory;
    }
  /**
   * Real subagent lifecycle as reported by the worker's `subagent_lifecycle`/`subagent_progress`
   * frames: the child's state is what the event says, never what a tool name suggests. `started`
   * means the child exists, and only `completed`/`failed`/`cancelled` end it.
   */
  | {
      readonly type: "subagent_start";
      readonly subagentId: string;
      readonly agent: string;
      readonly state: Extract<ExecutionSubagentState, "pending" | "running">;
      readonly atMs: number;
      readonly summary?: string;
    }
  | {
      readonly type: "subagent_update";
      readonly subagentId: string;
      readonly state: Extract<ExecutionSubagentState, "pending" | "running">;
      readonly atMs: number;
      readonly summary?: string;
      readonly currentTool?: string;
      readonly toolCount?: number;
    }
  | {
      readonly type: "subagent_end";
      readonly subagentId: string;
      readonly state: Extract<ExecutionSubagentState, "completed" | "failed" | "cancelled">;
      readonly atMs: number;
      readonly summary?: string;
    };

export interface ExecutionToolSnapshot {
  readonly toolCallId: string;
  readonly toolName: string;
  readonly state: ExecutionToolState;
  readonly startedAtMs: number;
  readonly endedAtMs: number | null;
  readonly durationMs: number | null;
  readonly summary?: string;
  readonly errorCategory?: ExecutionErrorCategory;
}

/**
 * One subagent as the card reports it: who it is, what it is for, the lifecycle state OMP last
 * reported, and the tool activity that state is made of. Timing is deliberately absent — the card
 * tracks a child's overall state and current tool, never its clock or duration.
 */
export interface ExecutionSubagentSnapshot {
  readonly subagentId: string;
  readonly agent: string;
  readonly state: ExecutionSubagentState;
  readonly summary?: string;
  readonly currentTool?: string;
  readonly toolCount?: number;
}

export interface ExecutionSnapshot {
  readonly runId: string;
  readonly state: ExecutionState;
  readonly startedAtMs: number | null;
  readonly endedAtMs: number | null;
  readonly elapsedMs: number;
  readonly phaseCount: number;
  readonly toolCount: number;
  readonly subagentCount: number;
  readonly tools: readonly ExecutionToolSnapshot[];
  readonly subagents: readonly ExecutionSubagentSnapshot[];
  readonly todos: readonly ExecutionTodoPhase[];
}

export interface ExecutionTracker {
  apply(event: ExecutionEvent): ExecutionSnapshot;
  snapshot(nowMs?: number): ExecutionSnapshot;
}

export interface ExecutionTrackerOptions {
  readonly runId?: string;
  readonly initialAtMs?: number;
}

interface MutablePhase {
  readonly turnId: string | number;
  readonly startedAtMs: number;
  endedAtMs: number | null;
}

interface MutableTool {
  readonly toolCallId: string;
  readonly toolName: string;
  readonly startedAtMs: number;
  state: ExecutionToolState;
  endedAtMs: number | null;
  summary: string | undefined;
  errorCategory: ExecutionErrorCategory | undefined;
}

interface MutableSubagent {
  readonly subagentId: string;
  agent: string;
  state: ExecutionSubagentState;
  summary: string | undefined;
  currentTool: string | undefined;
  toolCount: number | undefined;
}

const MAX_LOCAL_VISIBLE_TEXT_CHARS = 240;
const MAX_LOCAL_PHASES = 64;
const MAX_LOCAL_TASKS = 128;
const ALLOWED_ERROR_CATEGORIES: Record<ExecutionErrorCategory, true> = {
  timeout: true,
  permission: true,
  worker: true,
  cancelled: true,
  unavailable: true,
  unknown: true,
};
const ALLOWED_TODO_STATUSES: Record<TodoStatus, true> = {
  pending: true,
  in_progress: true,
  completed: true,
  abandoned: true,
  blocked: true,
  cancelled: true,
};

export function finiteTime(value: number): number | null {
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function eventTime(value: number): number {
  return finiteTime(value) ?? 0;
}

function nonNegativeDuration(startedAtMs: number, endedAtMs: number): number {
  return Math.max(0, endedAtMs - startedAtMs);
}

function truncateDisplayText(value: string, maximum = MAX_LOCAL_VISIBLE_TEXT_CHARS): string {
  let codePointCount = 0;
  let codeUnitOffset = 0;
  for (const character of value) {
    if (codePointCount >= maximum) return `${value.slice(0, codeUnitOffset)}…`;
    codePointCount += 1;
    codeUnitOffset += character.length;
  }
  return value;
}

function redactSensitiveText(value: string): string {
  let redacted = value;
  redacted = redacted.replace(
    /((?:authorization|api[-_.]?key|client[-_.]?secret|cookie|credentials?|pass(?:word|wd)|secret|session[-_.]?token|sig(?:nature)?|token)\s*[:=]\s*)(?:bearer\s+)?[^\s,;&]+/giu,
    "$1[redacted]",
  );
  redacted = redacted.replace(/(\b(?:bearer|basic|digest|token)\s+)[^\s,;&]+/giu, "$1[redacted]");
  redacted = redacted.replace(
    /([?&](?:access[-_.]?(?:key|token)|api[-_.]?key|authorization|password|passwd|secret|sig(?:nature)?|token|x[-_.]?api[-_.]?key)\s*=)[^&#\s]*/giu,
    "$1[redacted]",
  );
  redacted = redacted.replace(/\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{6,}\b/gu, "[redacted]");
  redacted = redacted.replace(/\bAKIA[0-9A-Z]{16}\b/gu, "[redacted]");
  return redacted;
}

function safeDisplayText(value: unknown, maximum = MAX_LOCAL_VISIBLE_TEXT_CHARS): string | undefined {
  if (typeof value !== "string" || value.length === 0) return undefined;
  const normalized = redactSensitiveText(value)
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  if (normalized.length === 0) return undefined;
  return truncateDisplayText(normalized, maximum);
}

function safeToolName(value: unknown): string {
  const normalized = safeDisplayText(value, 64);
  if (!normalized) return "工具";
  return normalized;
}

function safeErrorCategory(value: unknown, fallback: ExecutionErrorCategory = "unknown"): ExecutionErrorCategory {
  return typeof value === "string" && Object.prototype.hasOwnProperty.call(ALLOWED_ERROR_CATEGORIES, value)
    ? (value as ExecutionErrorCategory)
    : fallback;
}

function safeTodoStatus(value: unknown): TodoStatus {
  return typeof value === "string" && Object.prototype.hasOwnProperty.call(ALLOWED_TODO_STATUSES, value)
    ? (value as TodoStatus)
    : "pending";
}

function copyTodoPhases(phases: readonly ExecutionTodoPhase[]): ExecutionTodoPhase[] {
  const copied: ExecutionTodoPhase[] = [];
  const entries = Array.isArray(phases) ? phases : [];
  for (const phase of entries.slice(0, MAX_LOCAL_PHASES)) {
    const phaseName = safeDisplayText(phase?.name, 96);
    if (!phaseName) continue;
    const tasks: ExecutionTodoTask[] = [];
    for (const task of (Array.isArray(phase?.tasks) ? phase.tasks : []).slice(0, MAX_LOCAL_TASKS)) {
      const content = safeDisplayText(task?.content, 240);
      if (!content) continue;
      const blocker = safeDisplayText(task?.blocker, 160);
      const normalizedTask: ExecutionTodoTask = {
        content,
        status: safeTodoStatus(task?.status),
        ...(blocker === undefined ? {} : { blocker }),
      };
      tasks.push(normalizedTask);
    }
    copied.push({ name: phaseName, tasks });
  }
  return copied;
}

function copySnapshotTodos(todos: readonly ExecutionTodoPhase[]): ExecutionTodoPhase[] {
  return todos.map(phase => ({
    name: phase.name,
    tasks: phase.tasks.map(task => ({
      content: task.content,
      status: task.status,
      ...(task.blocker === undefined ? {} : { blocker: task.blocker }),
    })),
  }));
}

function currentTime(lastAtMs: number, nowMs: number | undefined): number {
  if (nowMs === undefined) return lastAtMs;
  return Math.max(lastAtMs, finiteTime(nowMs) ?? lastAtMs);
}

class ExecutionTrackerState implements ExecutionTracker {
  readonly #runId: string;
  readonly #phases: MutablePhase[] = [];
  readonly #tools = new Map<string, MutableTool>();
  readonly #toolOrder: string[] = [];
  readonly #subagents = new Map<string, MutableSubagent>();
  readonly #subagentOrder: string[] = [];
  #todos: ExecutionTodoPhase[] = [];
  #startedAtMs: number | null;
  #endedAtMs: number | null = null;
  #state: ExecutionState = "running";
  #lastAtMs: number;

  constructor(options: ExecutionTrackerOptions = {}) {
    this.#runId = safeDisplayText(options.runId, 96) ?? "octo-progress";
    this.#startedAtMs = options.initialAtMs === undefined ? null : finiteTime(options.initialAtMs);
    this.#lastAtMs = this.#startedAtMs ?? 0;
  }

  apply(event: ExecutionEvent): ExecutionSnapshot {
    if (!event || typeof event !== "object") return this.snapshot();
    const atMs = eventTime(event.atMs);
    this.#lastAtMs = Math.max(this.#lastAtMs, atMs);
    switch (event.type) {
      case "turn_start":
        this.#startTurn(event.turnId, atMs);
        break;
      case "turn_end":
        this.#endTurn(event.turnId, atMs);
        break;
      case "tool_start":
        this.#startTool(event, atMs);
        break;
      case "tool_end":
        this.#endTool(event, atMs);
        break;
      case "todo_update":
        this.#updateTodos(event);
        break;
      case "subagent_start":
        this.#startSubagent(event, atMs);
        break;
      case "subagent_update":
        this.#updateSubagent(event, atMs);
        break;
      case "subagent_end":
        this.#endSubagent(event, atMs);
        break;
      case "run_end":
        this.#endRun(event, atMs);
        break;
    }
    return this.snapshot(this.#lastAtMs);
  }

  snapshot(nowMs?: number): ExecutionSnapshot {
    const observedAtMs = currentTime(this.#lastAtMs, nowMs);
    const elapsedMs = this.#startedAtMs === null
      ? 0
      : nonNegativeDuration(this.#startedAtMs, this.#endedAtMs ?? Math.max(observedAtMs, this.#startedAtMs));
    const tools = this.#toolOrder.flatMap(toolCallId => {
      const tool = this.#tools.get(toolCallId);
      if (!tool) return [];
      const endedAtMs = tool.endedAtMs;
      const durationMs = endedAtMs === null
        ? nonNegativeDuration(tool.startedAtMs, Math.max(observedAtMs, tool.startedAtMs))
        : nonNegativeDuration(tool.startedAtMs, endedAtMs);
      const snapshot: ExecutionToolSnapshot = {
        toolCallId: tool.toolCallId,
        toolName: tool.toolName,
        state: tool.state,
        startedAtMs: tool.startedAtMs,
        endedAtMs,
        durationMs,
        ...(tool.summary === undefined ? {} : { summary: tool.summary }),
        ...(tool.errorCategory === undefined ? {} : { errorCategory: tool.errorCategory }),
      };
      return [snapshot];
    });
    return {
      runId: this.#runId,
      state: this.#state,
      startedAtMs: this.#startedAtMs,
      endedAtMs: this.#endedAtMs,
      elapsedMs,
      phaseCount: this.#phases.length,
      toolCount: this.#toolOrder.length,
      subagentCount: this.#subagentOrder.length,
      tools,
      subagents: this.#subagentOrder.flatMap(subagentId => {
        const subagent = this.#subagents.get(subagentId);
        if (!subagent) return [];
        return [{
          subagentId: subagent.subagentId,
          agent: subagent.agent,
          state: subagent.state,
          ...(subagent.summary === undefined ? {} : { summary: subagent.summary }),
          ...(subagent.currentTool === undefined ? {} : { currentTool: subagent.currentTool }),
          ...(subagent.toolCount === undefined ? {} : { toolCount: subagent.toolCount }),
        }];
      }),
      todos: copySnapshotTodos(this.#todos),
    };
  }

  #startTurn(turnId: string | number, atMs: number): void {
    if (this.#state !== "running") return;
    this.#ensureStarted(atMs);
    const existing = this.#phases.find(phase => phase.turnId === turnId && phase.endedAtMs === null);
    if (existing) return;
    this.#phases.push({ turnId, startedAtMs: atMs, endedAtMs: null });
  }

  #endTurn(turnId: string | number | undefined, atMs: number): void {
    const phase = turnId === undefined
      ? [...this.#phases].reverse().find(candidate => candidate.endedAtMs === null)
      : [...this.#phases].reverse().find(candidate => candidate.turnId === turnId && candidate.endedAtMs === null);
    if (phase) phase.endedAtMs = Math.max(phase.startedAtMs, atMs);
  }

  #startTool(event: Extract<ExecutionEvent, { type: "tool_start" }>, atMs: number): void {
    if (this.#state !== "running") return;
    this.#ensureStarted(atMs);
    if (this.#tools.has(event.toolCallId)) return;
    const tool: MutableTool = {
      toolCallId: safeDisplayText(event.toolCallId, 96) ?? `tool-${this.#toolOrder.length + 1}`,
      toolName: safeToolName(event.toolName),
      startedAtMs: atMs,
      state: "running",
      endedAtMs: null,
      summary: safeDisplayText(event.summary),
      errorCategory: undefined,
    };
    this.#tools.set(event.toolCallId, tool);
    this.#toolOrder.push(event.toolCallId);
  }

  #endTool(event: Extract<ExecutionEvent, { type: "tool_end" }>, atMs: number): void {
    const tool = this.#tools.get(event.toolCallId);
    if (!tool || tool.endedAtMs !== null) return;
    tool.state = event.status;
    tool.endedAtMs = Math.max(tool.startedAtMs, atMs);
    const summary = safeDisplayText(event.summary);
    if (summary !== undefined) tool.summary = summary;
    if (event.status === "failed" || event.status === "cancelled") {
      tool.errorCategory = safeErrorCategory(event.errorCategory, event.status === "cancelled" ? "cancelled" : "unknown");
    }
  }

  #updateTodos(event: Extract<ExecutionEvent, { type: "todo_update" }>): void {
    if (this.#state !== "running") return;
    this.#todos = copyTodoPhases(event.phases);
  }

  #startSubagent(event: Extract<ExecutionEvent, { type: "subagent_start" }>, atMs: number): void {
    const subagent = this.#subagent(event.subagentId, atMs);
    if (!subagent) return;
    if (subagent.state === "completed" || subagent.state === "failed" || subagent.state === "cancelled") return;
    subagent.state = event.state;
    // Only a real start may name the child; a row that exists from an earlier frame keeps its name.
    subagent.agent = safeDisplayText(event.agent, 64) ?? subagent.agent;
    const summary = safeDisplayText(event.summary);
    if (summary !== undefined) subagent.summary = summary;
  }

  #updateSubagent(event: Extract<ExecutionEvent, { type: "subagent_update" }>, atMs: number): void {
    const subagent = this.#subagent(event.subagentId, atMs);
    if (!subagent || subagent.state !== "pending" && subagent.state !== "running") return;
    subagent.state = event.state;
    const summary = safeDisplayText(event.summary);
    if (summary !== undefined) subagent.summary = summary;
    const currentTool = safeDisplayText(event.currentTool, 64);
    if (currentTool !== undefined) subagent.currentTool = currentTool;
    if (typeof event.toolCount === "number" && Number.isSafeInteger(event.toolCount) && event.toolCount >= 0) {
      subagent.toolCount = event.toolCount;
    }
  }

  #endSubagent(event: Extract<ExecutionEvent, { type: "subagent_end" }>, atMs: number): void {
    const subagent = this.#subagent(event.subagentId, atMs);
    if (!subagent) return;
    if (subagent.state === "completed" || subagent.state === "failed" || subagent.state === "cancelled") {
      // A terminal frame is the authoritative end of that run; it is never overwritten.
      return;
    }
    subagent.state = event.state;
    const summary = safeDisplayText(event.summary);
    if (summary !== undefined) subagent.summary = summary;
  }

  /**
   * Resolve the child a frame belongs to, registering it on first sight. A lifecycle frame can
   * arrive without its `started` predecessor — a mid-turn spawn the worker only observed after the
   * fact, or a replay of a durable payload — and dropping such a child would hide a real run.
   */
  #subagent(subagentIdValue: unknown, atMs: number): MutableSubagent | undefined {
    if (this.#state !== "running") return undefined;
    const subagentId = safeDisplayText(subagentIdValue, 96);
    if (subagentId === undefined) return undefined;
    const existing = this.#subagents.get(subagentId);
    if (existing) return existing;
    const subagent: MutableSubagent = {
      subagentId,
      agent: "子代理",
      state: "pending",
      summary: undefined,
      currentTool: undefined,
      toolCount: undefined,
    };
    this.#subagents.set(subagentId, subagent);
    this.#subagentOrder.push(subagentId);
    return subagent;
  }

  #endRun(event: Extract<ExecutionEvent, { type: "run_end" }>, atMs: number): void {
    if (this.#state !== "running") return;
    this.#ensureStarted(atMs);
    this.#state = event.state;
    this.#endedAtMs = atMs;
    for (const phase of this.#phases) {
      if (phase.endedAtMs === null) phase.endedAtMs = atMs;
    }
    for (const tool of this.#tools.values()) {
      if (tool.endedAtMs !== null) continue;
      tool.endedAtMs = atMs;
      tool.state = event.state === "cancelled" ? "cancelled" : event.state === "failed" ? "failed" : "succeeded";
      if (tool.state === "cancelled") tool.errorCategory = "cancelled";
      else if (tool.state === "failed") tool.errorCategory = safeErrorCategory(event.errorCategory, "unknown");
    }
    // A subagent that never reported a terminal frame cannot stay live on a settled card; the
    // turn's own outcome is the only evidence left after the worker stopped reporting.
    for (const subagent of this.#subagents.values()) {
      if (subagent.state === "completed" || subagent.state === "failed" || subagent.state === "cancelled") continue;
      subagent.state = event.state === "completed" ? "completed" : event.state === "cancelled" ? "cancelled" : "failed";
    }
  }

  #ensureStarted(atMs: number): void {
    if (this.#startedAtMs === null) this.#startedAtMs = atMs;
  }
}

export function createExecutionTracker(options: ExecutionTrackerOptions = {}): ExecutionTracker {
  return new ExecutionTrackerState(options);
}

/**
 * The same snapshot without the free-text step summaries the card prints under each step. A
 * deployment's edge inspects every card body and refuses one whose text matches its injection
 * rules, and a bounded command line can do that on its own, so a card refused that way is retried
 * in this shape: run state, counts, todos and subagent rows stay, and the command text does not.
 */
export function stripCardSummaries(snapshot: ExecutionSnapshot): ExecutionSnapshot {
  return {
    ...snapshot,
    tools: snapshot.tools.map((tool) => ({
      toolCallId: tool.toolCallId,
      toolName: tool.toolName,
      state: tool.state,
      startedAtMs: tool.startedAtMs,
      endedAtMs: tool.endedAtMs,
      durationMs: tool.durationMs,
      ...(tool.errorCategory === undefined ? {} : { errorCategory: tool.errorCategory }),
    })),
    subagents: snapshot.subagents.map((subagent) => ({
      subagentId: subagent.subagentId,
      agent: subagent.agent,
      state: subagent.state,
      ...(subagent.currentTool === undefined ? {} : { currentTool: subagent.currentTool }),
      ...(subagent.toolCount === undefined ? {} : { toolCount: subagent.toolCount }),
    })),
  };
}

/**
 * Serializes a snapshot for durable storage. A process that did not observe the run (for example
 * the one that starts after a restart interrupted it) can then replay the same trajectory and
 * submit one terminal card edit instead of leaving the client on the last running state.
 */
export function encodeExecutionSnapshot(snapshot: ExecutionSnapshot): string {
  return JSON.stringify(snapshot);
}

/**
 * Rebuilds a terminal snapshot from a stored payload by replaying it through a fresh tracker, so
 * the same sanitizing, capping, and terminal-state rules apply as in the live run. Returns null
 * when the payload is not a usable snapshot; callers drop such rows rather than guess.
 */
export function restoreExecutionSnapshot(
  encoded: string,
  options: { readonly state: Exclude<ExecutionState, "running">; readonly atMs: number },
): ExecutionSnapshot | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(encoded);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== "object") return null;
  const record = parsed as {
    runId?: unknown;
    startedAtMs?: unknown;
    tools?: unknown;
    subagents?: unknown;
    todos?: unknown;
  };
  if (!Array.isArray(record.tools) || !Array.isArray(record.todos)) return null;
  const startedAtMs = typeof record.startedAtMs === "number" ? finiteTime(record.startedAtMs) : null;
  const tracker = createExecutionTracker({
    runId: typeof record.runId === "string" ? record.runId : "octo-progress",
    ...(startedAtMs === null ? {} : { initialAtMs: startedAtMs }),
  });
  const entries = record.tools;
  for (let index = 0; index < entries.length; index += 1) {
    const value = entries[index];
    if (value === null || typeof value !== "object") continue;
    const tool = value as Record<string, unknown>;
    const toolCallId = typeof tool.toolCallId === "string" && tool.toolCallId.length > 0
      ? tool.toolCallId
      : `stored-${index}`;
    const summary = typeof tool.summary === "string" ? tool.summary : undefined;
    const startedToolAtMs = typeof tool.startedAtMs === "number" ? eventTime(tool.startedAtMs) : 0;
    tracker.apply({
      type: "tool_start",
      toolCallId,
      toolName: typeof tool.toolName === "string" ? tool.toolName : "",
      atMs: startedToolAtMs,
      ...(summary === undefined ? {} : { summary }),
    });
    const state = savedToolState(tool.state);
    const endedAtMs = typeof tool.endedAtMs === "number" ? finiteTime(tool.endedAtMs) : null;
    if (state === null || endedAtMs === null) continue;
    tracker.apply({
      type: "tool_end",
      toolCallId,
      atMs: endedAtMs,
      status: state,
      ...(summary === undefined ? {} : { summary }),
      ...(state === "failed" || state === "cancelled"
        ? { errorCategory: safeErrorCategory(tool.errorCategory, state === "cancelled" ? "cancelled" : "unknown") }
        : {}),
    });
  }
  if (record.todos.length > 0) {
    tracker.apply({
      type: "todo_update",
      atMs: options.atMs,
      phases: record.todos as readonly ExecutionTodoPhase[],
    });
  }
  // The stored rows carry identity, summary, lifecycle state, and the tool activity the card
  // shows; replaying them keeps the restored card's subagent band identical to the live one.
  for (const value of Array.isArray(record.subagents) ? record.subagents : []) {
    if (value === null || typeof value !== "object") continue;
    const subagent = value as Record<string, unknown>;
    if (typeof subagent.subagentId !== "string" || subagent.subagentId.length === 0) continue;
    const state = savedSubagentState(subagent.state);
    if (state === null) continue;
    const summary = typeof subagent.summary === "string" ? subagent.summary : undefined;
    const currentTool = typeof subagent.currentTool === "string" ? subagent.currentTool : undefined;
    const toolCount = typeof subagent.toolCount === "number" ? subagent.toolCount : undefined;
    tracker.apply({
      type: "subagent_start",
      subagentId: subagent.subagentId,
      agent: typeof subagent.agent === "string" ? subagent.agent : "",
      state: "running",
      atMs: options.atMs,
      ...(summary === undefined ? {} : { summary }),
    });
    if (state === "completed" || state === "failed" || state === "cancelled") {
      tracker.apply({
        type: "subagent_end",
        subagentId: subagent.subagentId,
        state,
        atMs: options.atMs,
      });
      continue;
    }
    if (currentTool !== undefined || toolCount !== undefined) {
      tracker.apply({
        type: "subagent_update",
        subagentId: subagent.subagentId,
        state,
        atMs: options.atMs,
        ...(currentTool === undefined ? {} : { currentTool }),
        ...(toolCount === undefined ? {} : { toolCount }),
      });
    }
  }
  return tracker.apply({ type: "run_end", atMs: options.atMs, state: options.state });
}

function savedToolState(value: unknown): Exclude<ExecutionToolState, "running"> | null {
  return value === "succeeded" || value === "failed" || value === "cancelled" ? value : null;
}

function savedSubagentState(value: unknown): ExecutionSubagentState | null {
  switch (value) {
    case "pending":
    case "running":
    case "completed":
    case "failed":
    case "cancelled":
      return value;
    default:
      return null;
  }
}
