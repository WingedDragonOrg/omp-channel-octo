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

/**
 * The tracker's input. Turn, tool, todo, and run events come from the generic RPC stream;
 * `subagent_start`/`subagent_update`/`subagent_end` come from the worker's `subagent_lifecycle`
 * and `subagent_progress` frames, so a subagent's state is what the event says rather than what a
 * tool name suggests: `started` means the child exists, and only `completed`/`failed`/`aborted`
 * end it.
 */
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
   * Real subagent lifecycle as reported by the OMP worker's `subagent_lifecycle` frames. The run's
   * state is whatever the event says, never what a tool name suggests: `started` means the child
   * exists, and only `completed`/`failed`/`cancelled` end it.
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

export interface ExecutionCardLimits {
  readonly maxNodes?: number;
  readonly maxDepth?: number;
  readonly maxPayloadBytes?: number;
}

export interface ExecutionCardCapabilities {
  readonly enabled: boolean;
  /** Deployment accepts `octo/v2` frames: `Action.Submit` plus `Input.*` (interaction switch). */
  readonly interactive: boolean;
  readonly profiles: readonly string[];
  readonly cardVersion: string;
  readonly elements?: readonly string[];
  readonly actions?: readonly string[];
  readonly limits?: ExecutionCardLimits;
}

export interface AdaptiveCard {
  readonly $schema: string;
  readonly type: "AdaptiveCard";
  readonly version: "1.5";
  readonly body: readonly Record<string, unknown>[];
  readonly metadata: {
    readonly octo_layout: "agent_progress_v1";
  };
  readonly [key: string]: unknown;
}

export interface ExecutionCardResult {
  readonly card: AdaptiveCard;
  readonly plain: string;
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

const DEFAULT_ELEMENTS: readonly string[] = ["TextBlock", "Container"];
const TOOL_DISPLAY_LABELS: Record<string, string> = {
  read: "读取文件",
  write: "写入文件",
  edit: "修改文件",
  bash: "运行命令",
  grep: "搜索文件",
  glob: "搜索文件",
  ask: "询问用户",
  ast_grep: "结构化搜索",
  ast_edit: "结构化修改",
  debug: "调试程序",
  eval: "运行代码",
  github: "GitHub 操作",
  lsp: "代码智能",
  checkpoint: "建立检查点",
  rewind: "恢复检查点",
  context_notes: "上下文说明",
  new_context: "切换上下文",
  security_scan: "安全扫描",
  hub: "IRC 协作",
  irc: "IRC 协作",
  think: "思考",
  yield: "提交结果",
  web_search: "搜索网页",
  memory_edit: "编辑记忆",
  retain: "保存记忆",
  recall: "检索记忆",
  reflect: "整理记忆",
  learn: "沉淀经验",
  manage_skill: "管理技能",
  agent: "运行子代理",
  delegate: "运行子代理",
  run_subagent: "运行子代理",
  spawn_agent: "运行子代理",
  task: "运行子代理",
  subagent: "运行子代理",
  subagent_start: "启动子代理",
  subagent_await: "等待子代理",
  subagent_stop: "停止子代理",
  todo: "更新任务",
};
const ADAPTIVE_CARD_SCHEMA = "http://adaptivecards.io/schemas/adaptive-card.json";
const REQUIRED_ELEMENTS = ["TextBlock"] as const;
const TOGGLE_ACTION = "Action.ToggleVisibility";
const SUCCESS_GLYPH = "✓";
const WAITING_GLYPH = "-";
const FAILURE_GLYPH = "×";
const RUNNING_GLYPH = "•";
// A fixed pixel slot prevents mobile glyph metrics from moving task content.
const TODO_MARKER_COLUMN_WIDTH = "80px";
// A fixed pixel slot keeps every tool row's marker on one column.
const TOOL_MARKER_COLUMN_WIDTH = "24px";
const WAITING_TOOL_NAMES: Record<string, true> = {
  subagent_await: true,
  wait: true,
  waiting: true,
};
const MAX_LOCAL_VISIBLE_TEXT_CHARS = 240;
const MAX_LOCAL_PHASES = 64;
const MAX_LOCAL_TASKS = 128;
const MAX_VISIBLE_TOOLS = 3;
const MAX_RENDERED_TOOLS = 32;
const MAX_COMPACT_TOOL_ROWS = 8;
/** Hidden-row budgets tried in order by the card fit search. */
const HIDDEN_ROW_LIMITS = [MAX_COMPACT_TOOL_ROWS, 4, 2];
const MAX_VISIBLE_TODO_TASKS = 96;
/**
 * Visible todo-band rungs tried after the execution trace has been compressed to its floor. The
 * rungs step by a few rows each so the search stays cheap, and every rung still carries the
 * handoff band; the final attempt keeps the hidden-task summary alone.
 */
const TODO_BAND_LADDER = [32, 24, 16, 12, 8, 6, 4, 2] as const;
/**
 * Expandable hidden todo rows. They are the omitted tasks closest to the handoff band, so this is
 * how much context around the boundary the reader can still recover on a budget-trimmed card.
 */
const MAX_EXPANDED_TODO_TASKS = 6;
const DEFAULT_MAX_NODES = 200;
const DEFAULT_MAX_DEPTH = 16;
const DEFAULT_MAX_PAYLOAD_BYTES = 512 * 1024;
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

function finiteTime(value: number): number | null {
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

interface JsonNode {
  [key: string]: unknown;
}

interface CardStats {
  readonly nodes: number;
  readonly maxDepth: number;
}

interface ResolvedCardLimits {
  readonly maxNodes: number;
  readonly maxDepth: number;
  readonly maxPayloadBytes: number;
}

function positiveLimit(value: number | undefined, fallback: number): number | null {
  if (value === undefined) return fallback;
  return Number.isSafeInteger(value) && value > 0 ? value : null;
}

function resolveCardLimits(capabilities: ExecutionCardCapabilities): ResolvedCardLimits | null {
  const requested = capabilities.limits;
  const maxNodes = positiveLimit(requested?.maxNodes, DEFAULT_MAX_NODES);
  const maxDepth = positiveLimit(requested?.maxDepth, DEFAULT_MAX_DEPTH);
  const maxPayloadBytes = positiveLimit(requested?.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES);
  if (maxNodes === null || maxDepth === null || maxPayloadBytes === null) return null;
  return { maxNodes, maxDepth, maxPayloadBytes };
}

function supportsElement(capabilities: ExecutionCardCapabilities, element: string): boolean {
  const elements = capabilities.elements ?? DEFAULT_ELEMENTS;
  return elements.includes(element);
}

function supportsAction(capabilities: ExecutionCardCapabilities, action: string): boolean {
  return capabilities.actions?.includes(action) ?? false;
}

function capabilitiesAllowCard(capabilities: ExecutionCardCapabilities): boolean {
  if (!capabilities.enabled || capabilities.cardVersion !== "1.5" || !capabilities.profiles.includes("octo/v1")) return false;
  return REQUIRED_ELEMENTS.every(element => supportsElement(capabilities, element));
}

function formatDuration(durationMs: number): string {
  const safeDuration = Number.isFinite(durationMs) ? Math.max(0, durationMs) : 0;
  if (safeDuration < 1_000) return `${Number.isInteger(safeDuration) ? safeDuration : safeDuration.toFixed(1)}ms`;
  const totalSeconds = safeDuration / 1_000;
  if (totalSeconds < 60) return `${totalSeconds.toFixed(1)}s`;
  const totalMinutes = Math.floor(totalSeconds / 60);
  const seconds = Math.floor(totalSeconds % 60);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  return `${totalMinutes}m ${seconds}s`;
}

function padTwo(value: number): string {
  return String(value).padStart(2, "0");
}

/** Local wall-clock label of a step; null when the snapshot carries no usable start time. */
function clockLabel(atMs: number): string | null {
  if (finiteTime(atMs) === null || atMs <= 0) return null;
  const at = new Date(atMs);
  return `${padTwo(at.getHours())}:${padTwo(at.getMinutes())}:${padTwo(at.getSeconds())}`;
}

function executionStateLabel(state: ExecutionState): string {
  switch (state) {
    case "running":
      return "进行中";
    case "completed":
      return "已完成";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
  }
}

function executionStateGlyph(state: ExecutionState | ExecutionToolState | ExecutionSubagentState): string {
  switch (state) {
    case "running":
      return RUNNING_GLYPH;
    case "completed":
    case "succeeded":
      return SUCCESS_GLYPH;
    case "failed":
      return FAILURE_GLYPH;
    case "cancelled":
      return WAITING_GLYPH;
    case "pending":
      return WAITING_GLYPH;
  }
}

function toolStateGlyph(tool: ExecutionToolSnapshot): string {
  if (tool.state === "running" && isWaitingTool(tool.toolName)) return WAITING_GLYPH;
  return executionStateGlyph(tool.state);
}

function toolStateLabel(state: ExecutionToolState): string {
  switch (state) {
    case "running":
      return "进行中";
    case "succeeded":
      return "已完成";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
  }
}

function executionTone(
  state: ExecutionState | ExecutionToolState | ExecutionSubagentState,
): "Accent" | "Good" | "Attention" | "Default" {
  switch (state) {
    case "running":
      return "Accent";
    case "completed":
    case "succeeded":
      return "Good";
    case "cancelled":
    case "pending":
      return "Default";
    case "failed":
      return "Attention";
  }
}

function displayToolName(toolName: string): string {
  const normalized = toolName.toLowerCase();
  return TOOL_DISPLAY_LABELS[toolName] ?? TOOL_DISPLAY_LABELS[normalized] ?? toolName;
}

function isWaitingTool(toolName: string): boolean {
  return WAITING_TOOL_NAMES[toolName.toLowerCase()] === true;
}

function toolStatusLabel(tool: ExecutionToolSnapshot): string {
  if (tool.state === "running" && isWaitingTool(tool.toolName)) return "等待";
  return toolStateLabel(tool.state);
}

function subagentStateLabel(state: ExecutionSubagentState): string {
  switch (state) {
    case "pending":
      return "待启动";
    case "running":
      return "进行中";
    case "completed":
      return "已完成";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
  }
}

/**
 * One child's line: who it is, what it is for, the lifecycle state, and the tool activity that
 * state is made of. No clock and no duration — timing is not part of the overall state.
 */
function subagentLine(subagent: ExecutionSubagentSnapshot, includeGlyph = false): string {
  const parts: string[] = [];
  if (includeGlyph) parts.push(executionStateGlyph(subagent.state));
  parts.push(subagent.agent);
  if (subagent.summary) parts.push(subagent.summary);
  parts.push(subagentStateLabel(subagent.state));
  if (subagent.currentTool) parts.push(subagent.currentTool);
  if (subagent.toolCount !== undefined) parts.push(`${subagent.toolCount} 次调用`);
  return parts.join(" · ");
}

function toolVisualTone(tool: ExecutionToolSnapshot): "Accent" | "Good" | "Attention" | "Default" {
  if (tool.state === "running" && isWaitingTool(tool.toolName)) return "Default";
  return executionTone(tool.state);
}

function todoStatusGlyph(status: TodoStatus): string {
  switch (status) {
    case "in_progress":
      return RUNNING_GLYPH;
    case "completed":
      return SUCCESS_GLYPH;
    case "abandoned":
      return FAILURE_GLYPH;
    case "pending":
    case "blocked":
    case "cancelled":
      return WAITING_GLYPH;
  }
}

function todoTone(status: TodoStatus): "Accent" | "Good" | "Attention" | "Default" {
  switch (status) {
    case "in_progress":
      return "Accent";
    case "completed":
      return "Good";
    case "abandoned":
      return "Attention";
    case "pending":
    case "blocked":
    case "cancelled":
      return "Default";
  }
}

function errorCategoryLabel(category: ExecutionErrorCategory): string {
  switch (category) {
    case "timeout":
      return "超时";
    case "permission":
      return "权限受限";
    case "worker":
      return "执行器错误";
    case "cancelled":
      return "已取消";
    case "unavailable":
      return "服务不可用";
    case "unknown":
      return "未知错误";
  }
}

function toolLine(tool: ExecutionToolSnapshot, includeGlyph = false, includeTime = false): string {
  const parts: string[] = [];
  if (includeGlyph) parts.push(toolStateGlyph(tool));
  if (includeTime) {
    const started = clockLabel(tool.startedAtMs);
    if (started !== null) parts.push(started);
  }
  parts.push(displayToolName(tool.toolName));
  if (tool.summary) parts.push(tool.summary);
  parts.push(toolStatusLabel(tool));
  if (tool.state === "failed" && tool.errorCategory) parts.push(errorCategoryLabel(tool.errorCategory));
  if (tool.durationMs !== null) parts.push(formatDuration(tool.durationMs));
  return parts.join(" · ");
}

function markdownSafeToolLine(tool: ExecutionToolSnapshot): string {
  const line = toolLine(tool, true, true);
  const glyph = toolStateGlyph(tool);
  if (glyph !== WAITING_GLYPH || !line.startsWith(`${glyph} `)) return line;
  return `${glyph}\u00a0${line.slice(glyph.length + 1)}`;
}

// Shared by budget-pruned trace and subagent rows. These rows are the compact tier: one text
// line each, which is what keeps the hidden-row budget affordable next to the visible panels.
function makeCompactToolRow(
  tool: ExecutionToolSnapshot,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  return makeTextBlock(`${toolStateGlyph(tool)}\u00a0· ${toolLine(tool)}`, {
    color: toolVisualTone(tool),
    spacing: "Small",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
  });
}

/** Compact tier of a subagent row: the same single-line shape as an omitted trace step. */
function makeCompactSubagentRow(
  subagent: ExecutionSubagentSnapshot,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  return makeTextBlock(`${executionStateGlyph(subagent.state)}\u00a0· ${subagentLine(subagent)}`, {
    color: executionTone(subagent.state),
    spacing: "Small",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
  });
}

function todoPhaseText(phase: ExecutionTodoPhase, phaseIndex: number, phaseCount: number): string {
  const branch = phaseIndex === phaseCount - 1 ? "└─" : "├─";
  return `${branch} ${phase.name}`;
}

function todoTaskLabel(task: ExecutionTodoTask): string {
  const blocker = task.blocker === undefined ? "" : ` · ${task.blocker}`;
  return `${task.content}${blocker}`;
}

function makeTodoTaskBody(task: ExecutionTodoTask, richTextSupported: boolean): JsonNode {
  const text = todoTaskLabel(task);
  const tone = todoTone(task.status);
  if (task.status === "cancelled" && richTextSupported) {
    return {
      type: "RichTextBlock",
      spacing: "None",
      inlines: [{
        type: "TextRun",
        text,
        color: tone,
        strikethrough: true,
      }],
    };
  }
  return makeTextBlock(text, { color: tone, spacing: "None" });
}

function todoTaskContent(task: ExecutionTodoTask): string {
  return `${todoStatusGlyph(task.status)} ${todoTaskLabel(task)}`;
}

function todoTaskText(
  task: ExecutionTodoTask,
  phaseIndex: number,
  phaseCount: number,
  taskIndex: number,
  taskCount: number,
): string {
  const phaseIndent = phaseIndex === phaseCount - 1 ? "   " : "│  ";
  const branch = taskIndex === taskCount - 1 ? "└─" : "├─";
  return `${phaseIndent}${branch} ${todoTaskContent(task)}`;
}

function makeTodoTaskRow(
  task: ExecutionTodoTask,
  phaseIndex: number,
  phaseCount: number,
  taskIndex: number,
  taskCount: number,
  richTextSupported: boolean,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  const continuation = phaseIndex === phaseCount - 1 ? "\u00a0" : "│";
  const branch = taskIndex === taskCount - 1 ? "└─" : "├─";
  const tone = todoTone(task.status);
  return {
    type: "ColumnSet",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
    spacing: "None",
    columns: [
      {
        type: "Column",
        width: TODO_MARKER_COLUMN_WIDTH,
        verticalContentAlignment: "Center",
        items: [makeTextBlock(`${continuation}\u00a0${branch}\u2009[${todoStatusGlyph(task.status)}]`, {
          color: tone,
          fontType: "Monospace",
          size: "Small",
          spacing: "None",
          wrap: false,
        })],
      },
      {
        type: "Column",
        width: "stretch",
        verticalContentAlignment: "Center",
        items: [makeTodoTaskBody(task, richTextSupported)],
      },
    ],
  };
}

function makeTodoPhaseRow(
  phase: ExecutionTodoPhase,
  phaseIndex: number,
  phaseCount: number,
): JsonNode {
  const branch = phaseIndex === phaseCount - 1 ? "└─" : "├─";
  return {
    type: "ColumnSet",
    spacing: "None",
    columns: [
      {
        type: "Column",
        width: "auto",
        verticalContentAlignment: "Center",
        items: [makeTextBlock(branch, { fontType: "Monospace", spacing: "None" })],
      },
      {
        type: "Column",
        width: "stretch",
        verticalContentAlignment: "Center",
        items: [makeTextBlock(phase.name, { weight: "Bolder", spacing: "None" })],
      },
    ],
  };
}

function todoLines(
  todos: readonly ExecutionTodoPhase[],
  hiddenCount = 0,
  totalCount = 0,
): string[] {
  const lines: string[] = [];
  if (hiddenCount > 0) lines.push(`已隐藏 ${hiddenCount} 个任务，共 ${totalCount} 个任务`);
  const visiblePhases = todos.filter(phase => phase.tasks.length > 0);
  const phaseCount = visiblePhases.length;
  for (let phaseIndex = 0; phaseIndex < phaseCount; phaseIndex += 1) {
    const phase = visiblePhases[phaseIndex]!;
    lines.push(todoPhaseText(phase, phaseIndex, phaseCount));
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      lines.push(todoTaskText(phase.tasks[taskIndex]!, phaseIndex, phaseCount, taskIndex, phase.tasks.length));
    }
  }
  return lines;
}

function makeTodoItems(
  allTodos: readonly ExecutionTodoPhase[],
  todos: readonly ExecutionTodoPhase[],
  omittedTodos: readonly ExecutionTodoPhase[],
  omittedRowIds: ReadonlySet<string>,
  hiddenCount: number,
  totalCount: number,
  structuredLayout: boolean,
  richTextSupported: boolean,
): JsonNode[] {
  const items: JsonNode[] = [];
  if (hiddenCount > 0) {
    items.push(makeTextBlock(`已隐藏 ${hiddenCount} 个任务，共 ${totalCount} 个任务`, {
      id: "todo_hidden_summary",
      isSubtle: true,
      spacing: "Small",
    }));
  }

  const phaseEntries: Array<{
    readonly sourceIndex: number;
    readonly phase: ExecutionTodoPhase;
    readonly visibleTasks: ReadonlySet<ExecutionTodoTask>;
  }> = [];
  for (let sourceIndex = 0; sourceIndex < allTodos.length; sourceIndex += 1) {
    const phase = allTodos[sourceIndex]!;
    const visibleTasks = new Set(todos[sourceIndex]?.tasks ?? []);
    // A phase row with nothing under it is noise: keep the phase only when it actually renders a
    // visible task or a hidden row the reader can expand.
    const hasRenderedTask = phase.tasks.some((_task, taskIndex) => (
      visibleTasks.has(phase.tasks[taskIndex]!)
      || (structuredLayout && omittedRowIds.has(`todo_omitted_task_${sourceIndex}_${taskIndex}`))
    ));
    if (!hasRenderedTask) continue;
    phaseEntries.push({ sourceIndex, phase, visibleTasks });
  }

  let renderedOmittedCount = 0;
  for (let phaseIndex = 0; phaseIndex < phaseEntries.length; phaseIndex += 1) {
    const entry = phaseEntries[phaseIndex]!;
    const phaseItems: JsonNode[] = [
      structuredLayout
        ? makeTodoPhaseRow(entry.phase, phaseIndex, phaseEntries.length)
        : makeTextBlock(todoPhaseText(entry.phase, phaseIndex, phaseEntries.length), {
          weight: "Bolder",
          spacing: "None",
        }),
    ];
    for (let taskIndex = 0; taskIndex < entry.phase.tasks.length; taskIndex += 1) {
      const task = entry.phase.tasks[taskIndex]!;
      if (entry.visibleTasks.has(task)) {
        phaseItems.push(structuredLayout
          ? makeTodoTaskRow(task, phaseIndex, phaseEntries.length, taskIndex, entry.phase.tasks.length, richTextSupported)
          : makeTextBlock(
            todoTaskText(task, phaseIndex, phaseEntries.length, taskIndex, entry.phase.tasks.length),
            { color: todoTone(task.status), spacing: "None" },
          ));
        continue;
      }
      const omittedId = `todo_omitted_task_${entry.sourceIndex}_${taskIndex}`;
      if (!structuredLayout || !omittedRowIds.has(omittedId)) continue;
      phaseItems.push(makeTodoTaskRow(
        task,
        phaseIndex,
        phaseEntries.length,
        taskIndex,
        entry.phase.tasks.length,
        richTextSupported,
        {
          id: omittedId,
          isVisible: false,
        },
      ));
      renderedOmittedCount += 1;
    }
    items.push({
      type: "Container",
      spacing: "Small",
      items: phaseItems,
    });
  }

  const omittedCount = omittedTodos.reduce((count, phase) => count + phase.tasks.length, 0);
  if (renderedOmittedCount < omittedCount) {
    items.push(makeTextBlock(`还有 ${omittedCount - renderedOmittedCount} 个任务受卡片大小限制`, {
      isSubtle: true,
      spacing: "Small",
    }));
  }
  return items;
}

/**
 * Ids of the hidden rows the expand control restores: the omitted tasks closest to the handoff
 * anchor, so expanding grows the readable band outward instead of jumping to the list's ends.
 */
function todoOmittedTaskIds(
  allTodos: readonly ExecutionTodoPhase[],
  omittedTodos: readonly ExecutionTodoPhase[],
  limit: number,
): string[] {
  const anchor = handoffAnchorIndex(allTodos);
  const candidates: Array<{ readonly id: string; readonly index: number; readonly distance: number }> = [];
  let flatIndex = 0;
  for (let sourceIndex = 0; sourceIndex < allTodos.length; sourceIndex += 1) {
    const phase = allTodos[sourceIndex]!;
    const omittedTasks = new Set(omittedTodos[sourceIndex]?.tasks ?? []);
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      const index = flatIndex;
      flatIndex += 1;
      if (!omittedTasks.has(phase.tasks[taskIndex]!)) continue;
      candidates.push({
        id: `todo_omitted_task_${sourceIndex}_${taskIndex}`,
        index,
        distance: anchor < 0 ? index : Math.abs(index - anchor),
      });
    }
  }
  candidates.sort((left, right) => left.distance - right.distance || left.index - right.index);
  return candidates
    .slice(0, Math.max(0, Math.floor(limit)))
    .sort((left, right) => left.index - right.index)
    .map(candidate => candidate.id);
}

function makeTextBlock(text: string, options: JsonNode = {}): JsonNode {
  return {
    type: "TextBlock",
    text,
    wrap: true,
    ...options,
  };
}

function makeToolRow(
  tool: ExecutionToolSnapshot,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  const displayName = displayToolName(tool.toolName);
  const tone = toolVisualTone(tool);
  const mainItems = tool.summary === undefined
    ? [makeTextBlock(displayName, { weight: "Bolder", spacing: "None" })]
    : [
        makeTextBlock(tool.summary, { weight: "Bolder", spacing: "None" }),
        makeTextBlock(displayName, { isSubtle: true, size: "Small", spacing: "None" }),
      ];
  const statusParts: string[] = [];
  const started = clockLabel(tool.startedAtMs);
  if (started !== null) statusParts.push(started);
  statusParts.push(toolStatusLabel(tool));
  if (tool.state === "failed" && tool.errorCategory) statusParts.push(errorCategoryLabel(tool.errorCategory));
  if (tool.durationMs !== null) statusParts.push(formatDuration(tool.durationMs));
  const glyph = toolStateGlyph(tool);
  // Keep timeline rows as direct ColumnSet children; nest the colored panel with its marker inside.
  return {
    type: "ColumnSet",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
    spacing: "None",
    columns: [
      {
        type: "Column",
        width: "stretch",
        items: [
          {
            type: "Container",
            style: tone === "Default" ? "emphasis" : tone.toLowerCase(),
            spacing: "Small",
            items: [
              {
                type: "ColumnSet",
                spacing: "None",
                columns: [
                  {
                    type: "Column",
                    width: TOOL_MARKER_COLUMN_WIDTH,
                    verticalContentAlignment: "Center",
                    items: [makeTextBlock(glyph, {
                      spacing: "None",
                      color: tone,
                      fontType: "Monospace",
                      size: "Small",
                      wrap: false,
                    })],
                  },
                  {
                    type: "Column",
                    width: "stretch",
                    items: [
                      ...mainItems,
                      makeTextBlock(statusParts.join(" · "), {
                        isSubtle: true,
                        size: "Small",
                        spacing: "None",
                        color: tone,
                      }),
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  };
}

/**
 * Rich tier of a subagent row: agent name headline, its purpose, then the lifecycle state with the
 * tool it is currently running and how many it has used. Rows for subagents still on the wire stay
 * the default visible band, so this mirrors the tool row layout — without the row's clock/duration.
 */
function makeSubagentRow(
  subagent: ExecutionSubagentSnapshot,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  const tone = executionTone(subagent.state);
  return {
    type: "ColumnSet",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
    spacing: "None",
    columns: [
      {
        type: "Column",
        width: "stretch",
        items: [
          {
            type: "Container",
            style: tone === "Default" ? "emphasis" : tone.toLowerCase(),
            spacing: "Small",
            items: [
              {
                type: "ColumnSet",
                spacing: "None",
                columns: [
                  {
                    type: "Column",
                    width: TOOL_MARKER_COLUMN_WIDTH,
                    verticalContentAlignment: "Center",
                    items: [makeTextBlock(executionStateGlyph(subagent.state), {
                      spacing: "None",
                      color: tone,
                      fontType: "Monospace",
                      size: "Small",
                      wrap: false,
                    })],
                  },
                  {
                    type: "Column",
                    width: "stretch",
                    items: [
                      makeTextBlock(subagent.agent, { weight: "Bolder", spacing: "None" }),
                      ...(subagent.summary === undefined
                        ? []
                        : [makeTextBlock(subagent.summary, { isSubtle: true, size: "Small", spacing: "None" })]),
                      makeTextBlock([
                        subagentStateLabel(subagent.state),
                        ...(subagent.currentTool === undefined ? [] : [subagent.currentTool]),
                        ...(subagent.toolCount === undefined ? [] : [`${subagent.toolCount} 次调用`]),
                      ].join(" · "), {
                        isSubtle: true,
                        size: "Small",
                        spacing: "None",
                        color: tone,
                      }),
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  };
}

/**
 * Omitted-step history band. It renders as a sibling of `timeline_detail` instead of a child:
 * the web client's agent-progress layer repaints every filled Container that sits directly under
 * `#timeline_detail` as a failed-step status chip (danger fill + border, inline fill cleared),
 * which turned this grey band into a red block. Outside that container the band keeps the
 * section's emphasis fill, matching the subagent history band.
 */
function makeTraceHistoryBand(tools: readonly ExecutionToolSnapshot[]): JsonNode {
  return {
    type: "Container",
    id: "timeline_omitted_steps",
    isVisible: false,
    style: "emphasis",
    spacing: "Small",
    items: tools.map(tool => makeCompactToolRow(tool)),
  };
}

function makeTimelineItems(
  tools: readonly ExecutionToolSnapshot[],
  hiddenCount: number,
  richLayout: boolean,
  emptyText = "正在准备",
  olderTools: readonly ExecutionToolSnapshot[] = [],
  hasOmittedSteps = false,
  hideOlder = true,
): JsonNode[] {
  const items: JsonNode[] = [];
  if (hiddenCount > 0) {
    items.push(makeTextBlock(`已隐藏前 ${hiddenCount} 个步骤`, {
      ...(richLayout ? { id: "timeline_hidden_summary" } : {}),
      isSubtle: true,
      spacing: "Small",
    }));
  }
  if (tools.length === 0 && olderTools.length === 0) {
    if (!hasOmittedSteps) {
      items.push(makeTextBlock(emptyText, { isSubtle: true, spacing: "Small" }));
    }
  } else if (richLayout) {
    items.push(...tools.map(tool => makeToolRow(tool)));
    items.push(...olderTools.map((tool, index) => makeCompactToolRow(tool, {
      id: `timeline_older_step_${index}`,
      ...(hideOlder ? { isVisible: false } : {}),
    })));
  } else {
    items.push(...tools.map(tool => makeTextBlock(markdownSafeToolLine(tool), { spacing: "Small" })));
  }
  return items;
}

function makeTodoContainer(
  allTodos: readonly ExecutionTodoPhase[],
  todos: readonly ExecutionTodoPhase[],
  hiddenCount: number,
  totalCount: number,
  structuredLayout: boolean,
  richTextSupported: boolean,
  omittedTodos: readonly ExecutionTodoPhase[] = [],
  actionSets: readonly JsonNode[] = [],
  omittedRowIds: ReadonlySet<string> = new Set<string>(),
): JsonNode | null {
  if (!allTodos.some(phase => phase.tasks.length > 0)) return null;
  const heading = structuredLayout && actionSets.length > 0
    ? makeSectionHeader("任务清单", actionSets)
    : makeTextBlock("任务清单", { weight: "Bolder", size: "Medium", spacing: "None" });
  return {
    type: "Container",
    id: "todo_detail",
    style: "emphasis",
    spacing: "Medium",
    items: [
      heading,
      ...makeTodoItems(
        allTodos,
        todos,
        omittedTodos,
        omittedRowIds,
        hiddenCount,
        totalCount,
        structuredLayout,
        richTextSupported,
      ),
    ],
  };
}

/**
 * The subagent section. It is fed by the OMP subagent events alone: live children render as rich
 * rows, finished ones sink into the expandable history band, and the count in the heading is the
 * number of subagents the session actually reported.
 */
function makeSubagentContainer(
  subagents: readonly ExecutionSubagentSnapshot[],
  hiddenCount: number,
  totalCount: number,
  detailVisible: boolean,
  olderSubagents: readonly ExecutionSubagentSnapshot[] = [],
  omittedSubagents: readonly ExecutionSubagentSnapshot[] = [],
  hideOlder = true,
): JsonNode {
  const items: JsonNode[] = [];
  if (hiddenCount > 0) {
    items.push(makeTextBlock(`已隐藏 ${hiddenCount} 个子代理，共 ${totalCount} 个子代理`, {
      id: "subagent_hidden_summary",
      isSubtle: true,
      spacing: "Small",
    }));
  }
  if (subagents.length === 0 && olderSubagents.length === 0 && omittedSubagents.length === 0) {
    items.push(makeTextBlock("暂无可见子代理活动", { isSubtle: true, spacing: "Small" }));
  } else {
    items.push(...subagents.map(subagent => makeSubagentRow(subagent)));
    items.push(...olderSubagents.map((subagent, index) => makeCompactSubagentRow(subagent, {
      id: `subagent_older_step_${index}`,
      ...(hideOlder ? { isVisible: false } : {}),
    })));
    if (omittedSubagents.length > 0) {
      items.push({
        type: "Container",
        id: "subagent_omitted_steps",
        isVisible: false,
        style: "emphasis",
        spacing: "Small",
        items: omittedSubagents.map(subagent => makeCompactSubagentRow(subagent)),
      });
    }
  }
  return {
    type: "Container",
    id: "subagent_detail",
    isVisible: detailVisible,
    spacing: "Small",
    items,
  };
}

function makeSectionHeader(title: string, actionSets: readonly JsonNode[]): JsonNode {
  const columns: JsonNode[] = [
    {
      type: "Column",
      width: "stretch",
      verticalContentAlignment: "Center",
      items: [makeTextBlock(title, { weight: "Bolder", size: "Medium", spacing: "None" })],
    },
  ];
  if (actionSets.length > 0) {
    columns.push({
      type: "Column",
      width: "auto",
      verticalContentAlignment: "Center",
      items: actionSets,
    });
  }
  return { type: "ColumnSet", spacing: "None", columns };
}

function makeCollapsibleSection(
  id: string,
  heading: string,
  details: readonly JsonNode[],
  actionSets: readonly JsonNode[],
  isVisible = true,
): JsonNode {
  return {
    type: "Container",
    id,
    ...(isVisible ? {} : { isVisible: false }),
    spacing: "Medium",
    items: [makeSectionHeader(heading, actionSets), ...details],
  };
}

function makeVisibilityTargets(
  summaryId: string,
  hiddenElementIds: readonly string[],
  collapseButtonId: string,
  expandButtonId: string,
): { readonly collapsed: JsonNode[]; readonly expanded: JsonNode[] } {
  return {
    collapsed: [
      { elementId: summaryId, isVisible: true },
      ...hiddenElementIds.map(elementId => ({ elementId, isVisible: false })),
      { elementId: collapseButtonId, isVisible: false },
      { elementId: expandButtonId, isVisible: true },
    ],
    expanded: [
      { elementId: summaryId, isVisible: false },
      ...hiddenElementIds.map(elementId => ({ elementId, isVisible: true })),
      { elementId: collapseButtonId, isVisible: true },
      { elementId: expandButtonId, isVisible: false },
    ],
  };
}

function makeToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
  if (hiddenElementIds.length === 0) return [];
  const targets = makeVisibilityTargets(
    "timeline_hidden_summary",
    hiddenElementIds,
    "btn_collapse_trace",
    "btn_expand_trace",
  );
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_trace",
      isVisible: false,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "trace_collapse",
          title: "收起",
          targetElements: targets.collapsed,
        },
      ],
    },
    {
      type: "ActionSet",
      id: "btn_expand_trace",
      isVisible: true,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "trace_expand",
          title: "展开",
          targetElements: targets.expanded,
        },
      ],
    },
  ];
}

function makeSubagentToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
  if (hiddenElementIds.length === 0) return [];
  const targets = makeVisibilityTargets(
    "subagent_hidden_summary",
    hiddenElementIds,
    "btn_collapse_subagents",
    "btn_expand_subagents",
  );
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_subagents",
      isVisible: false,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "subagents_collapse",
          title: "收起",
          targetElements: targets.collapsed,
        },
      ],
    },
    {
      type: "ActionSet",
      id: "btn_expand_subagents",
      isVisible: true,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "subagents_expand",
          title: "展开",
          targetElements: targets.expanded,
        },
      ],
    },
  ];
}
function makeTodoToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
  if (hiddenElementIds.length === 0) return [];
  const targets = makeVisibilityTargets(
    "todo_hidden_summary",
    hiddenElementIds,
    "btn_collapse_todo",
    "btn_expand_todo",
  );
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_todo",
      isVisible: false,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "todo_collapse",
          title: "收起",
          targetElements: targets.collapsed,
        },
      ],
    },
    {
      type: "ActionSet",
      id: "btn_expand_todo",
      isVisible: true,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "todo_expand",
          title: "展开",
          targetElements: targets.expanded,
        },
      ],
    },
  ];
}
function makeProgressToggleSets(
  detailElementIds: readonly string[],
  initiallyExpanded: boolean,
): JsonNode[] {
  if (detailElementIds.length === 0) return [];
  const collapsedTargets = [
    ...detailElementIds.map(elementId => ({ elementId, isVisible: false })),
    { elementId: "btn_collapse_progress", isVisible: false },
    { elementId: "btn_expand_progress", isVisible: true },
  ];
  const expandedTargets = [
    ...detailElementIds.map(elementId => ({ elementId, isVisible: true })),
    { elementId: "btn_collapse_progress", isVisible: true },
    { elementId: "btn_expand_progress", isVisible: false },
  ];
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_progress",
      isVisible: initiallyExpanded,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "progress_collapse",
          title: "收起",
          targetElements: collapsedTargets,
        },
      ],
    },
    {
      type: "ActionSet",
      id: "btn_expand_progress",
      isVisible: !initiallyExpanded,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "progress_expand",
          title: "展开",
          targetElements: expandedTargets,
        },
      ],
    },
  ];
}

function cardStats(value: unknown, depth = 0): CardStats {
  if (Array.isArray(value)) {
    let nodes = 0;
    let maxDepth = depth;
    for (const child of value) {
      const childStats = cardStats(child, depth);
      nodes += childStats.nodes;
      maxDepth = Math.max(maxDepth, childStats.maxDepth);
    }
    return { nodes, maxDepth };
  }
  if (value === null || typeof value !== "object") return { nodes: 0, maxDepth: depth };
  let nodes = 1;
  let maxDepth = depth + 1;
  for (const child of Object.values(value)) {
    const childStats = cardStats(child, depth + 1);
    nodes += childStats.nodes;
    maxDepth = Math.max(maxDepth, childStats.maxDepth);
  }
  return { nodes, maxDepth };
}

function cardWithinLimits(card: AdaptiveCard, plain: string, limits: ResolvedCardLimits): boolean {
  // The deployment measures the wire envelope, not the bare card, so the fit search must count
  // the frame wrapper too — a candidate one node over the card budget would never be sent.
  const envelope = {
    type: 17,
    profile: "octo/v1",
    card_version: "1.5",
    card,
    plain,
  };
  const stats = cardStats(envelope);
  return stats.nodes <= limits.maxNodes
    && stats.maxDepth <= limits.maxDepth
    && new TextEncoder().encode(JSON.stringify(envelope)).byteLength <= limits.maxPayloadBytes;
}

function makeHeader(richLayout: boolean, actionSets: readonly JsonNode[] = []): JsonNode[] {
  const title = makeTextBlock("处理进度", { weight: "Bolder", size: "Large", spacing: "None" });
  const subtitle = makeTextBlock("OMP 执行", { isSubtle: true, size: "Small", spacing: "Small" });
  if (richLayout) {
    return [
      {
        type: "ColumnSet",
        spacing: "None",
        columns: [
          {
            type: "Column",
            width: "stretch",
            items: [title, subtitle],
          },
          {
            type: "Column",
            width: "auto",
            verticalContentAlignment: "Center",
            items: [
              makeTextBlock("✦", {
                color: "Accent",
                size: "ExtraLarge",
                horizontalAlignment: "Right",
                spacing: "None",
              }),
              ...actionSets,
            ],
          },
        ],
      },
    ];
  }
  return [title, subtitle];
}

function makeHeaderContainer(
  snapshot: ExecutionSnapshot,
  richLayout: boolean,
  actionSets: readonly JsonNode[] = [],
): JsonNode {
  return {
    type: "Container",
    id: "octo-execution-trace-header",
    style: snapshot.state === "failed"
      ? "attention"
      : snapshot.state === "cancelled"
        ? "default"
        : snapshot.state === "completed"
          ? "good"
          : "emphasis",
    bleed: true,
    spacing: "None",
    items: makeHeader(richLayout, actionSets),
  };
}

function metricCount(value: number): string {
  if (!Number.isSafeInteger(value) || value < 0) return "0";
  return String(value);
}

function makeStatusContainer(
  snapshot: ExecutionSnapshot,
  metrics: string,
  richLayout: boolean,
): JsonNode {
  const status = executionStateLabel(snapshot.state);
  const glyph = executionStateGlyph(snapshot.state);
  const tone = executionTone(snapshot.state);
  if (richLayout) {
    return {
      type: "Container",
      id: "execution_status",
      spacing: "Medium",
      items: [
        {
          type: "ColumnSet",
          spacing: "None",
          columns: [
            {
              type: "Column",
              width: "auto",
              verticalContentAlignment: "Center",
              items: [makeTextBlock(glyph, { color: tone, size: "Medium", spacing: "None" })],
            },
            {
              type: "Column",
              width: "stretch",
              items: [
                makeTextBlock(status, { color: tone, weight: "Bolder", spacing: "None" }),
                makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
              ],
            },
            {
              type: "Column",
              width: "auto",
              verticalContentAlignment: "Center",
              items: [
                makeTextBlock(formatDuration(snapshot.elapsedMs), {
                  color: tone,
                  weight: "Bolder",
                  horizontalAlignment: "Right",
                  spacing: "None",
                }),
              ],
            },
          ],
        },
      ],
    };
  }
  return {
    type: "Container",
    id: "execution_status",
    spacing: "Medium",
    items: [
      makeTextBlock(`${glyph} ${status}`, { color: tone, weight: "Bolder", spacing: "None" }),
      makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
    ],
  };
}


function executionNotice(
  snapshot: ExecutionSnapshot,
  todos: readonly ExecutionTodoPhase[],
): { readonly text: string; readonly style: "default" | "attention" } | null {
  let blockedCount = 0;
  let firstBlocker: string | undefined;
  for (const phase of todos) {
    for (const task of phase.tasks) {
      if (task.status !== "blocked") continue;
      blockedCount += 1;
      if (firstBlocker === undefined && task.blocker !== undefined) firstBlocker = task.blocker;
    }
  }
  if (blockedCount > 0) {
    return {
      text: firstBlocker === undefined
        ? `${WAITING_GLYPH} ${blockedCount} 个任务等待处理`
        : `${WAITING_GLYPH} ${blockedCount} 个任务等待处理 · ${firstBlocker}`,
      style: "default",
    };
  }

  let failedCount = 0;
  let firstError: ExecutionErrorCategory | undefined;
  for (const tool of snapshot.tools) {
    if (tool.state !== "failed") continue;
    failedCount += 1;
    if (firstError === undefined) firstError = tool.errorCategory ?? "unknown";
  }
  if (failedCount > 0) {
    return {
      text: `${FAILURE_GLYPH} ${failedCount} 个步骤失败 · ${errorCategoryLabel(firstError ?? "unknown")}`,
      style: "attention",
    };
  }
  return null;
}

function makeNotice(notice: { readonly text: string; readonly style: "default" | "attention" }): JsonNode {
  return {
    type: "Container",
    style: notice.style,
    spacing: "Medium",
    items: [
      makeTextBlock(notice.text, {
        color: notice.style === "attention" ? "Attention" : "Default",
        weight: "Bolder",
        spacing: "None",
      }),
    ],
  };
}

function buildCandidate(
  snapshot: ExecutionSnapshot,
  capabilities: ExecutionCardCapabilities,
  visibleTools: readonly ExecutionToolSnapshot[],
  visibleTodos: readonly ExecutionTodoPhase[],
  omittedTodos: readonly ExecutionTodoPhase[],
  totalTodoTasks: number,
  hiddenRowLimit: number,
): ExecutionCardResult {
  const todoRowLimit = Math.min(hiddenRowLimit, MAX_EXPANDED_TODO_TASKS);
  const hasContainer = supportsElement(capabilities, "Container");
  const hasColumnSet = supportsElement(capabilities, "ColumnSet");
  const richTextSupported = supportsElement(capabilities, "RichTextBlock");
  const richLayout = hasContainer && hasColumnSet;
  const stackedLayout = hasContainer;
  const canToggle = richLayout
    && supportsElement(capabilities, "ActionSet")
    && supportsAction(capabilities, TOGGLE_ACTION);
  // Snapshots written before subagent tracking existed (and hand-built fixtures) simply have no
  // subagent field; they render as a run with zero children.
  const subagents = Array.isArray(snapshot.subagents) ? snapshot.subagents : [];
  const hiddenTools = Math.max(0, snapshot.tools.length - visibleTools.length);
  const recentTools = visibleTools.slice(Math.max(0, visibleTools.length - MAX_VISIBLE_TOOLS));
  const omittedTools = snapshot.tools.slice(0, hiddenTools);
  const visibleTraceTools = richLayout ? recentTools : visibleTools;
  const olderTraceTools = richLayout
    ? visibleTools.filter(tool => !recentTools.includes(tool))
    : [];
  const omittedTraceTools = richLayout ? omittedTools : [];
  const cappedOmittedTraceTools = omittedTraceTools.length <= MAX_COMPACT_TOOL_ROWS ? omittedTraceTools : [];
  const expandableOmittedTraceTools = cappedOmittedTraceTools.slice(
    Math.max(0, cappedOmittedTraceTools.length - hiddenRowLimit),
  );
  const totalSubagents = richLayout ? subagents.length : 0;
  // Live children (the ones still on the wire) are the default rich band; finished ones are the
  // history the toggle can bring back. Both come from the OMP subagent events, not tool names.
  const visibleSubagents = richLayout
    ? subagents.filter(subagent => subagent.state === "running" || subagent.state === "pending")
    : [];
  const olderSubagents = richLayout
    ? subagents.filter(subagent => subagent.state !== "running" && subagent.state !== "pending")
    : [];
  const cappedOlderSubagents = olderSubagents.length <= MAX_COMPACT_TOOL_ROWS ? olderSubagents : [];
  const expandableOmittedSubagents = cappedOlderSubagents.slice(
    Math.max(0, cappedOlderSubagents.length - hiddenRowLimit),
  );
  const totalTraceTools = snapshot.tools.length;
  const hiddenTraceTools = Math.max(0, totalTraceTools - visibleTraceTools.length);
  const hiddenSubagents = Math.max(0, totalSubagents - visibleSubagents.length);
  const visibleTodoTasks = visibleTodos.reduce((count, phase) => count + phase.tasks.length, 0);
  const hiddenTodoTasks = Math.max(0, totalTodoTasks - visibleTodoTasks);
  const elapsed = formatDuration(snapshot.elapsedMs);
  const metrics = `${snapshot.phaseCount} 个阶段 · ${snapshot.toolCount} 次工具调用 · ${elapsed}`;
  const notice = executionNotice(snapshot, visibleTodos);
  const canToggleTraceHistory = canToggle && expandableOmittedTraceTools.length > 0;
  const traceHistoryBand = canToggleTraceHistory
    ? makeTraceHistoryBand(expandableOmittedTraceTools)
    : null;
  const timelineItems = makeTimelineItems(
    visibleTraceTools,
    hiddenTraceTools,
    richLayout,
    visibleSubagents.length > 0 ? "暂无其他工具活动" : "正在准备",
    olderTraceTools,
    canToggleTraceHistory,
    canToggle,
  );
  const hasRenderedSubagentActivity = visibleSubagents.length > 0
    || olderSubagents.length > 0
    || hiddenSubagents > 0;
  const subagentContainer = richLayout && hasRenderedSubagentActivity
    ? makeSubagentContainer(
      visibleSubagents,
      hiddenSubagents,
      totalSubagents,
      true,
      olderSubagents,
      canToggle ? expandableOmittedSubagents : [],
      canToggle,
    )
    : null;
  const traceHiddenIds = [
    ...olderTraceTools.map((_tool, index) => `timeline_older_step_${index}`),
    ...(expandableOmittedTraceTools.length > 0 ? ["timeline_omitted_steps"] : []),
  ];
  const subagentHiddenIds = expandableOmittedSubagents.length > 0 ? ["subagent_omitted_steps"] : [];
  const traceToggleSets = canToggle ? makeToggleSets(traceHiddenIds) : [];
  const subagentToggleSets = canToggle && subagentContainer
    ? makeSubagentToggleSets(subagentHiddenIds)
    : [];
  const expandableOmittedTodos = canToggle && hiddenRowLimit > 0 ? omittedTodos : [];
  const todoHiddenIds = canToggle
    ? todoOmittedTaskIds(snapshot.todos, expandableOmittedTodos, todoRowLimit)
    : [];
  const todoToggleSets = todoHiddenIds.length > 0
    ? makeTodoToggleSets(todoHiddenIds)
    : [];
  const todoContainer = hasContainer
    ? makeTodoContainer(
      snapshot.todos,
      visibleTodos,
      hiddenTodoTasks,
      totalTodoTasks,
      richLayout,
      richTextSupported,
      expandableOmittedTodos,
      todoToggleSets,
      new Set(todoHiddenIds),
    )
    : null;
  const terminal = snapshot.state !== "running";
  const detailElementIds = [
    ...(todoContainer === null ? [] : ["todo_detail"]),
    ...(subagentContainer === null ? [] : ["subagent_section"]),
    "timeline_section",
  ];
  // Terminal cards open collapsed behind the header expand control; a running
  // card keeps its live detail open and offers the matching collapse control.
  const overallToggleSets = canToggle
    ? makeProgressToggleSets(detailElementIds, !terminal)
    : [];
  const overallDetailsVisible = !terminal || overallToggleSets.length === 0;
  const body: JsonNode[] = [];

  if (stackedLayout) {
    body.push(
      makeHeaderContainer(snapshot, richLayout, overallToggleSets),
      makeStatusContainer(snapshot, metrics, richLayout),
    );
    if (notice) body.push(makeNotice(notice));
    if (todoContainer) {
      body.push(overallDetailsVisible ? todoContainer : { ...todoContainer, isVisible: false });
    }
    if (richLayout) {
      const timelineDetail: JsonNode = {
        type: "Container",
        id: "timeline_detail",
        isVisible: true,
        spacing: "Small",
        items: timelineItems,
      };
      if (subagentContainer) {
        body.push(makeCollapsibleSection(
          "subagent_section",
          `子代理 · ${totalSubagents}`,
          [subagentContainer],
          subagentToggleSets,
          overallDetailsVisible,
        ));
      }
      body.push(makeCollapsibleSection(
        "timeline_section",
        "执行轨迹",
        traceHistoryBand === null ? [timelineDetail] : [timelineDetail, traceHistoryBand],
        traceToggleSets,
        overallDetailsVisible,
      ));
    } else {
      body.push({
        type: "Container",
        id: "timeline_detail",
        isVisible: true,
        spacing: "Medium",
        items: [
          makeTextBlock("执行轨迹", { weight: "Bolder", size: "Medium", spacing: "None" }),
          ...timelineItems,
        ],
      });
    }
  } else {
    body.push(
      ...makeHeader(false),
      makeTextBlock(`${executionStateGlyph(snapshot.state)} ${executionStateLabel(snapshot.state)}`, {
        weight: "Bolder",
        color: executionTone(snapshot.state),
        spacing: "Medium",
      }),
      makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
      makeTextBlock(
        `阶段 ${metricCount(snapshot.phaseCount)} · 工具 ${metricCount(snapshot.toolCount)} · 任务 ${metricCount(totalTodoTasks)} · 耗时 ${elapsed}`,
        { isSubtle: true, size: "Small", spacing: "Small" },
      ),
    );
    if (notice) {
      body.push(makeTextBlock(notice.text, {
        color: notice.style === "attention" ? "Attention" : "Default",
        weight: "Bolder",
        spacing: "Medium",
      }));
    }
    if (visibleTodoTasks > 0) {
      body.push(...todoLines(visibleTodos, hiddenTodoTasks, totalTodoTasks).map(line => makeTextBlock(line, { spacing: "Small" })));
    }
    if (subagents.length > 0) {
      body.push(makeTextBlock(
        `子代理 · ${subagents.length}`,
        { weight: "Bolder", size: "Medium", spacing: "Medium" },
      ));
      body.push(...subagents.map(subagent => makeCompactSubagentRow(subagent)));
    }
    body.push(makeTextBlock("执行轨迹", { weight: "Bolder", size: "Medium", spacing: "Medium" }), ...timelineItems);
  }


  const card: AdaptiveCard = {
    $schema: ADAPTIVE_CARD_SCHEMA,
    type: "AdaptiveCard",
    version: "1.5",
    body,
    metadata: { octo_layout: "agent_progress_v1" },
  };
  const plainLines = [
    `处理进度 · ${executionStateLabel(snapshot.state)} · ${metrics}`,
    "OMP 自动执行",
    ...(notice ? [notice.text] : []),
    ...visibleTools.map(tool => markdownSafeToolLine(tool)),
    ...(hiddenTools > 0 ? [`已隐藏前 ${hiddenTools} 个步骤，共 ${snapshot.tools.length} 个步骤`] : []),
    ...subagents.map(subagent => subagentLine(subagent, true)),
    ...(hiddenSubagents > 0 ? [`已隐藏 ${hiddenSubagents} 个子代理，共 ${totalSubagents} 个子代理`] : []),
    ...todoLines(visibleTodos, hiddenTodoTasks, totalTodoTasks),
  ];
  // The rich card keeps finished subagents behind the section toggle; the plain rendering has no
  // toggle, so it lists every subagent the session reported.
  if (visibleTools.length === 0 && visibleTodoTasks === 0 && subagents.length === 0) plainLines.push("正在准备");
  return { card, plain: plainLines.join("\n") };
}

/**
 * Index (in source order across phases) of the task the run stands on: the first in-progress task,
 * else the first still-open task, else the tail of a settled list. The visible band grows outward
 * from here, so the completed ↔ open boundary survives a budget trim.
 */
function handoffAnchorIndex(todos: readonly ExecutionTodoPhase[]): number {
  let openIndex = -1;
  let index = 0;
  for (const phase of todos) {
    for (const task of phase.tasks) {
      if (task.status === "in_progress") return index;
      if (openIndex < 0 && (task.status === "blocked" || task.status === "pending")) openIndex = index;
      index += 1;
    }
  }
  if (index === 0) return -1;
  return openIndex >= 0 ? openIndex : index - 1;
}

function trimTodos(
  todos: readonly ExecutionTodoPhase[],
  maximumTasks: number,
): { readonly visible: ExecutionTodoPhase[]; readonly omitted: ExecutionTodoPhase[] } {
  const taskRefs: Array<{ readonly task: ExecutionTodoTask }> = [];
  for (let phaseIndex = 0; phaseIndex < todos.length; phaseIndex += 1) {
    const phase = todos[phaseIndex]!;
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      taskRefs.push({ task: phase.tasks[taskIndex]! });
    }
  }

  const target = Math.min(taskRefs.length, Math.max(0, Math.floor(maximumTasks)));
  if (target === taskRefs.length) {
    return {
      visible: todos.map(phase => ({ name: phase.name, tasks: phase.tasks.slice() })),
      omitted: todos.map(phase => ({ name: phase.name, tasks: [] })),
    };
  }
  if (target === 0) {
    return {
      visible: todos.map(phase => ({ name: phase.name, tasks: [] })),
      omitted: todos.map(phase => ({ name: phase.name, tasks: phase.tasks.slice() })),
    };
  }

  const selected = new Array<boolean>(taskRefs.length).fill(false);
  let selectedCount = 0;
  const select = (refIndex: number): void => {
    if (selectedCount >= target || selected[refIndex]) return;
    selected[refIndex] = true;
    selectedCount += 1;
  };

  // The card is read where the run currently stands. Pin active work and blockers, then grow a
  // window outward from the handoff anchor — completed side first — so the boundary between done
  // and open work and its neighbours stay readable instead of the middle being punched out.
  // Re-sort by source index below so the card always preserves the todo's original order.
  for (let refIndex = 0; refIndex < taskRefs.length && selectedCount < target; refIndex += 1) {
    const status = taskRefs[refIndex]!.task.status;
    if (status === "in_progress" || status === "blocked") select(refIndex);
  }
  const anchor = handoffAnchorIndex(todos);
  if (anchor >= 0) {
    select(anchor);
    for (let distance = 1; distance <= taskRefs.length && selectedCount < target; distance += 1) {
      select(anchor - distance);
      select(anchor + distance);
    }
  }
  for (let refIndex = 0; refIndex < taskRefs.length && selectedCount < target; refIndex += 1) {
    select(refIndex);
  }

  const visible: ExecutionTodoPhase[] = [];
  const omitted: ExecutionTodoPhase[] = [];
  let refIndex = 0;
  for (let phaseIndex = 0; phaseIndex < todos.length; phaseIndex += 1) {
    const phase = todos[phaseIndex]!;
    const visibleTasks: ExecutionTodoTask[] = [];
    const omittedTasks: ExecutionTodoTask[] = [];
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      const task = taskRefs[refIndex]!.task;
      if (selected[refIndex]) visibleTasks.push(task);
      else omittedTasks.push(task);
      refIndex += 1;
    }
    visible.push({ name: phase.name, tasks: visibleTasks });
    omitted.push({ name: phase.name, tasks: omittedTasks });
  }
  return { visible, omitted };
}

export function buildExecutionCard(
  snapshot: ExecutionSnapshot,
  capabilities: ExecutionCardCapabilities,
): ExecutionCardResult | null {
  if (!capabilitiesAllowCard(capabilities)) return null;
  const limits = resolveCardLimits(capabilities);
  if (!limits) return null;
  const totalTools = snapshot.tools.length;
  const totalTodoTasks = snapshot.todos.reduce((count, phase) => count + phase.tasks.length, 0);

  const attempt = (
    visibleTodoCount: number,
    visibleToolCounts: readonly number[],
  ): ExecutionCardResult | null => {
    const trimmedTodos = trimTodos(snapshot.todos, visibleTodoCount);
    for (const visibleToolCount of visibleToolCounts) {
      const visibleTools = snapshot.tools.slice(Math.max(0, totalTools - visibleToolCount));
      // Visible content wins over expandable hidden rows: keep this much detail and shrink the
      // hidden-row budget until the card fits.
      for (const hiddenRowLimit of HIDDEN_ROW_LIMITS) {
        const candidate = buildCandidate(
          snapshot,
          capabilities,
          visibleTools,
          trimmedTodos.visible,
          trimmedTodos.omitted,
          totalTodoTasks,
          hiddenRowLimit,
        );
        if (cardWithinLimits(candidate.card, candidate.plain, limits)) return candidate;
      }
    }
    return null;
  };

  // The todo list is the run's contract; the trace is a rolling log whose older rows the reader
  // can still expand. The trace therefore yields first: shed tool rows and hidden-row budget while
  // the todo list stays whole, and only shrink the visible band once the trace is at its floor.
  const maximumVisibleTodos = Math.min(totalTodoTasks, MAX_VISIBLE_TODO_TASKS);
  const toolCounts: number[] = [];
  for (let count = Math.min(totalTools, MAX_RENDERED_TOOLS); count >= 0; count -= 1) {
    toolCounts.push(count);
  }
  const wholeList = attempt(maximumVisibleTodos, toolCounts);
  if (wholeList) return wholeList;

  // Largest readable band first, keeping room for the newest steps while they still fit.
  const floorToolCounts = [...new Set([Math.min(totalTools, MAX_VISIBLE_TOOLS), 0])];
  for (const band of TODO_BAND_LADDER) {
    if (band >= maximumVisibleTodos) continue;
    const candidate = attempt(band, floorToolCounts);
    if (candidate) return candidate;
  }
  return attempt(0, floorToolCounts);
}
