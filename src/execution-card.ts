export type ExecutionState = "running" | "completed" | "failed" | "cancelled";
export type ExecutionToolState = "running" | "succeeded" | "failed" | "cancelled";
export type ExecutionErrorCategory =
  | "timeout"
  | "permission"
  | "worker"
  | "cancelled"
  | "unavailable"
  | "unknown";

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

export interface ExecutionSnapshot {
  readonly runId: string;
  readonly state: ExecutionState;
  readonly startedAtMs: number | null;
  readonly endedAtMs: number | null;
  readonly elapsedMs: number;
  readonly phaseCount: number;
  readonly toolCount: number;
  readonly tools: readonly ExecutionToolSnapshot[];
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
const SUBAGENT_TOOL_NAMES: Record<string, true> = {
  agent: true,
  delegate: true,
  run_subagent: true,
  spawn_agent: true,
  subagent: true,
  subagent_await: true,
  subagent_start: true,
  subagent_stop: true,
  task: true,
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
const MAX_VISIBLE_TODO_TASKS = 96;
const MAX_EXPANDED_TODO_TASKS = 4;
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
      tools,
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
  }

  #ensureStarted(atMs: number): void {
    if (this.#startedAtMs === null) this.#startedAtMs = atMs;
  }
}

export function createExecutionTracker(options: ExecutionTrackerOptions = {}): ExecutionTracker {
  return new ExecutionTrackerState(options);
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

function executionStateGlyph(state: ExecutionState | ExecutionToolState): string {
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
  state: ExecutionState | ExecutionToolState,
): "Accent" | "Good" | "Attention" | "Default" {
  switch (state) {
    case "running":
      return "Accent";
    case "completed":
    case "succeeded":
      return "Good";
    case "cancelled":
      return "Default";
    case "failed":
      return "Attention";
  }
}

function displayToolName(toolName: string): string {
  const normalized = toolName.toLowerCase();
  return TOOL_DISPLAY_LABELS[toolName] ?? TOOL_DISPLAY_LABELS[normalized] ?? toolName;
}

function isSubagentTool(toolName: string): boolean {
  return SUBAGENT_TOOL_NAMES[toolName.toLowerCase()] === true;
}
function isActiveSubagent(tool: ExecutionToolSnapshot): boolean {
  return isSubagentTool(tool.toolName) && tool.state === "running";
}

function isWaitingTool(toolName: string): boolean {
  return WAITING_TOOL_NAMES[toolName.toLowerCase()] === true;
}

function toolStatusLabel(tool: ExecutionToolSnapshot): string {
  if (tool.state === "running" && isWaitingTool(tool.toolName)) return "等待";
  return toolStateLabel(tool.state);
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

function toolLine(tool: ExecutionToolSnapshot, includeGlyph = false): string {
  const parts = includeGlyph ? [toolStateGlyph(tool), displayToolName(tool.toolName)] : [displayToolName(tool.toolName)];
  if (tool.summary) parts.push(tool.summary);
  parts.push(toolStatusLabel(tool));
  if (tool.state === "failed" && tool.errorCategory) parts.push(errorCategoryLabel(tool.errorCategory));
  if (tool.durationMs !== null) parts.push(formatDuration(tool.durationMs));
  return parts.join(" · ");
}

function markdownSafeToolLine(tool: ExecutionToolSnapshot): string {
  const line = toolLine(tool, true);
  const glyph = toolStateGlyph(tool);
  if (glyph !== WAITING_GLYPH || !line.startsWith(`${glyph} `)) return line;
  return `${glyph}\u00a0${line.slice(glyph.length + 1)}`;
}

// Shared by budget-pruned trace and subagent rows.
function makeCompactToolRow(
  tool: ExecutionToolSnapshot,
  options: { readonly id?: string; readonly isVisible?: boolean } = {},
): JsonNode {
  const tone = toolVisualTone(tool);
  return {
    type: "ColumnSet",
    ...(options.id === undefined ? {} : { id: options.id }),
    ...(options.isVisible === undefined ? {} : { isVisible: options.isVisible }),
    spacing: "Small",
    columns: [
      {
        type: "Column",
        width: "auto",
        verticalContentAlignment: "Center",
        items: [makeTextBlock(`${toolStateGlyph(tool)}\u00a0`, {
          color: tone,
          fontType: "Monospace",
          size: "Small",
          spacing: "None",
        })],
      },
      {
        type: "Column",
        width: "stretch",
        verticalContentAlignment: "Center",
        items: [makeTextBlock(`· ${toolLine(tool)}`, {
          color: tone,
          spacing: "None",
        })],
      },
    ],
  };
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
    readonly omittedTasks: ReadonlySet<ExecutionTodoTask>;
  }> = [];
  for (let sourceIndex = 0; sourceIndex < allTodos.length; sourceIndex += 1) {
    const phase = allTodos[sourceIndex]!;
    const visibleTasks = new Set(todos[sourceIndex]?.tasks ?? []);
    const omittedTasks = new Set(omittedTodos[sourceIndex]?.tasks ?? []);
    if (!phase.tasks.some(task => visibleTasks.has(task) || (structuredLayout && omittedTasks.has(task)))) continue;
    phaseEntries.push({ sourceIndex, phase, visibleTasks, omittedTasks });
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
      } else if (
        structuredLayout
        && entry.omittedTasks.has(task)
        && renderedOmittedCount < MAX_EXPANDED_TODO_TASKS
      ) {
        phaseItems.push(makeTodoTaskRow(
          task,
          phaseIndex,
          phaseEntries.length,
          taskIndex,
          entry.phase.tasks.length,
          richTextSupported,
          {
            id: `todo_omitted_task_${entry.sourceIndex}_${taskIndex}`,
            isVisible: false,
          },
        ));
        renderedOmittedCount += 1;
      }
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

function todoOmittedTaskIds(
  allTodos: readonly ExecutionTodoPhase[],
  omittedTodos: readonly ExecutionTodoPhase[],
): string[] {
  const ids: string[] = [];
  let renderedCount = 0;
  for (let sourceIndex = 0; sourceIndex < allTodos.length; sourceIndex += 1) {
    const phase = allTodos[sourceIndex]!;
    const omittedTasks = new Set(omittedTodos[sourceIndex]?.tasks ?? []);
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      if (!omittedTasks.has(phase.tasks[taskIndex]!)) continue;
      if (renderedCount >= MAX_EXPANDED_TODO_TASKS) return ids;
      ids.push(`todo_omitted_task_${sourceIndex}_${taskIndex}`);
      renderedCount += 1;
    }
  }
  return ids;
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
  const statusParts = [toolStatusLabel(tool)];
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
                    width: "24px",
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

function makeTimelineItems(
  tools: readonly ExecutionToolSnapshot[],
  hiddenCount: number,
  richLayout: boolean,
  emptyText = "正在准备",
  olderTools: readonly ExecutionToolSnapshot[] = [],
  omittedTools: readonly ExecutionToolSnapshot[] = [],
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
    if (omittedTools.length === 0) {
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
  if (omittedTools.length > 0) {
    items.push({
      type: "Container",
      id: "timeline_omitted_steps",
      isVisible: false,
      style: "emphasis",
      spacing: "Small",
      items: omittedTools.map(tool => makeCompactToolRow(tool)),
    });
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
      ...makeTodoItems(allTodos, todos, omittedTodos, hiddenCount, totalCount, structuredLayout, richTextSupported),
    ],
  };
}

function makeSubagentContainer(
  tools: readonly ExecutionToolSnapshot[],
  hiddenCount: number,
  totalCount: number,
  detailVisible: boolean,
  olderTools: readonly ExecutionToolSnapshot[] = [],
  omittedTools: readonly ExecutionToolSnapshot[] = [],
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
  if (tools.length === 0 && olderTools.length === 0 && omittedTools.length === 0) {
    items.push(makeTextBlock("暂无可见子代理活动", { isSubtle: true, spacing: "Small" }));
  } else {
    items.push(...tools.map(tool => makeToolRow(tool)));
    items.push(...olderTools.map((tool, index) => makeCompactToolRow(tool, {
      id: `subagent_older_step_${index}`,
      ...(hideOlder ? { isVisible: false } : {}),
    })));
    if (omittedTools.length > 0) {
      items.push({
        type: "Container",
        id: "subagent_omitted_steps",
        isVisible: false,
        style: "emphasis",
        spacing: "Small",
        items: omittedTools.map(tool => makeCompactToolRow(tool)),
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
  detail: JsonNode,
  actionSets: readonly JsonNode[],
  isVisible = true,
): JsonNode {
  return {
    type: "Container",
    id,
    ...(isVisible ? {} : { isVisible: false }),
    spacing: "Medium",
    items: [makeSectionHeader(heading, actionSets), detail],
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
function makeProgressToggleSets(detailElementIds: readonly string[]): JsonNode[] {
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
      isVisible: false,
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
      isVisible: true,
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

function wirePayloadBytes(card: AdaptiveCard, plain: string): number {
  const envelope = {
    type: 17,
    profile: "octo/v1",
    card_version: "1.5",
    card,
    plain,
  };
  return new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
}

function cardWithinLimits(card: AdaptiveCard, plain: string, limits: ResolvedCardLimits): boolean {
  const stats = cardStats(card);
  return stats.nodes <= limits.maxNodes
    && stats.maxDepth <= limits.maxDepth
    && wirePayloadBytes(card, plain) <= limits.maxPayloadBytes;
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
): ExecutionCardResult {
  const hasContainer = supportsElement(capabilities, "Container");
  const hasColumnSet = supportsElement(capabilities, "ColumnSet");
  const richTextSupported = supportsElement(capabilities, "RichTextBlock");
  const richLayout = hasContainer && hasColumnSet;
  const stackedLayout = hasContainer;
  const canToggle = richLayout
    && supportsElement(capabilities, "ActionSet")
    && supportsAction(capabilities, TOGGLE_ACTION);
  const hiddenTools = Math.max(0, snapshot.tools.length - visibleTools.length);
  const recentTools = visibleTools.slice(Math.max(0, visibleTools.length - MAX_VISIBLE_TOOLS));
  const omittedTools = snapshot.tools.slice(0, hiddenTools);
  const totalSubagents = richLayout
    ? snapshot.tools.filter(tool => isSubagentTool(tool.toolName)).length
    : 0;
  const visibleSubagents = richLayout
    ? snapshot.tools.filter(isActiveSubagent)
    : [];
  const olderSubagents: readonly ExecutionToolSnapshot[] = [];
  const visibleTraceTools = richLayout
    ? recentTools.filter(tool => !isSubagentTool(tool.toolName))
    : visibleTools;
  const olderTraceTools = richLayout
    ? visibleTools.filter(tool => !isSubagentTool(tool.toolName) && !recentTools.includes(tool))
    : [];
  const omittedTraceTools = richLayout
    ? omittedTools.filter(tool => !isSubagentTool(tool.toolName))
    : [];
  const expandableOmittedTraceTools = omittedTraceTools.length <= MAX_COMPACT_TOOL_ROWS
    ? omittedTraceTools
    : [];
  const omittedSubagentTools = richLayout
    ? [
        ...omittedTools.filter(tool => isSubagentTool(tool.toolName) && !isActiveSubagent(tool)),
        ...visibleTools.filter(tool => isSubagentTool(tool.toolName) && !isActiveSubagent(tool)),
      ]
    : [];
  const expandableOmittedSubagentTools = omittedSubagentTools.length <= MAX_COMPACT_TOOL_ROWS
    ? omittedSubagentTools
    : [];
  const totalTraceTools = richLayout ? snapshot.tools.length - totalSubagents : snapshot.tools.length;
  const hiddenTraceTools = Math.max(0, totalTraceTools - visibleTraceTools.length);
  const hiddenSubagents = Math.max(0, totalSubagents - visibleSubagents.length);
  const visibleTodoTasks = visibleTodos.reduce((count, phase) => count + phase.tasks.length, 0);
  const hiddenTodoTasks = Math.max(0, totalTodoTasks - visibleTodoTasks);
  const elapsed = formatDuration(snapshot.elapsedMs);
  const metrics = `${snapshot.phaseCount} 个阶段 · ${snapshot.toolCount} 次工具调用 · ${elapsed}`;
  const notice = executionNotice(snapshot, visibleTodos);
  const timelineItems = makeTimelineItems(
    visibleTraceTools,
    hiddenTraceTools,
    richLayout,
    visibleSubagents.length > 0 ? "暂无其他工具活动" : "正在准备",
    olderTraceTools,
    canToggle ? expandableOmittedTraceTools : [],
    canToggle,
  );
  const hasRenderedSubagentActivity = visibleSubagents.length > 0
    || olderSubagents.length > 0
    || expandableOmittedSubagentTools.length > 0;
  const subagentContainer = richLayout && hasRenderedSubagentActivity
    ? makeSubagentContainer(
      visibleSubagents,
      hiddenSubagents,
      totalSubagents,
      true,
      olderSubagents,
      canToggle ? expandableOmittedSubagentTools : [],
      canToggle,
    )
    : null;
  const traceHiddenIds = [
    ...olderTraceTools.map((_tool, index) => `timeline_older_step_${index}`),
    ...(expandableOmittedTraceTools.length > 0 ? ["timeline_omitted_steps"] : []),
  ];
  const subagentHiddenIds = [
    ...olderSubagents.map((_tool, index) => `subagent_older_step_${index}`),
    ...(expandableOmittedSubagentTools.length > 0 ? ["subagent_omitted_steps"] : []),
  ];
  const traceToggleSets = canToggle ? makeToggleSets(traceHiddenIds) : [];
  const subagentToggleSets = canToggle && subagentContainer
    ? makeSubagentToggleSets(subagentHiddenIds)
    : [];
  const expandableOmittedTodos = canToggle ? omittedTodos : [];
  const todoHiddenIds = canToggle ? todoOmittedTaskIds(snapshot.todos, expandableOmittedTodos) : [];
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
    )
    : null;
  const terminal = snapshot.state !== "running";
  const detailElementIds = [
    ...(todoContainer === null ? [] : ["todo_detail"]),
    ...(subagentContainer === null ? [] : ["subagent_section"]),
    "timeline_section",
  ];
  const overallToggleSets = terminal && canToggle
    ? makeProgressToggleSets(detailElementIds)
    : [];
  const overallDetailsVisible = overallToggleSets.length === 0;
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
          subagentContainer,
          subagentToggleSets,
          overallDetailsVisible,
        ));
      }
      body.push(makeCollapsibleSection(
        "timeline_section",
        "执行轨迹",
        timelineDetail,
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
    ...todoLines(visibleTodos, hiddenTodoTasks, totalTodoTasks),
  ];
  if (visibleTools.length === 0 && visibleTodoTasks === 0) plainLines.push("正在准备");
  return { card, plain: plainLines.join("\n") };
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

  // Keep active work and blockers before filling the remaining budget with
  // recent context, then older context. Re-sort by source index below so the
  // card always preserves the todo's original relative order.
  for (let refIndex = 0; refIndex < taskRefs.length && selectedCount < target; refIndex += 1) {
    const status = taskRefs[refIndex]!.task.status;
    if (status === "in_progress" || status === "blocked") select(refIndex);
  }
  for (let refIndex = taskRefs.length - 1; refIndex >= 0 && selectedCount < target; refIndex -= 1) {
    select(refIndex);
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
  let visibleToolCount = Math.min(totalTools, MAX_RENDERED_TOOLS);
  let visibleTodoCount = Math.min(totalTodoTasks, MAX_VISIBLE_TODO_TASKS);
  while (visibleToolCount >= 0 && visibleTodoCount >= 0) {
    const visibleTools = snapshot.tools.slice(Math.max(0, totalTools - visibleToolCount));
    const trimmedTodos = trimTodos(snapshot.todos, visibleTodoCount);
    const candidate = buildCandidate(
      snapshot,
      capabilities,
      visibleTools,
      trimmedTodos.visible,
      trimmedTodos.omitted,
      totalTodoTasks,
    );
    if (cardWithinLimits(candidate.card, candidate.plain, limits)) return candidate;
    // Keep execution activity visible; reduce the less volatile todo detail first.
    if (visibleTodoCount > 0) {
      visibleTodoCount -= 1;
      continue;
    }
    if (visibleToolCount > 0) {
      visibleToolCount -= 1;
      continue;
    }
    break;
  }
  return null;
}
