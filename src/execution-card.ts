export type ExecutionState = "running" | "completed" | "failed" | "cancelled";
export type ExecutionToolState = "running" | "succeeded" | "failed" | "cancelled";
export type ExecutionErrorCategory =
  | "timeout"
  | "permission"
  | "worker"
  | "cancelled"
  | "unavailable"
  | "unknown";

export type TodoStatus = "pending" | "in_progress" | "completed" | "abandoned" | "blocked";
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
  todo: "更新任务",
};
const ADAPTIVE_CARD_SCHEMA = "http://adaptivecards.io/schemas/adaptive-card.json";
const REQUIRED_ELEMENTS = ["TextBlock"] as const;
const TOGGLE_ACTION = "Action.ToggleVisibility";
const MAX_LOCAL_VISIBLE_TEXT_CHARS = 240;
const MAX_LOCAL_PHASES = 64;
const MAX_LOCAL_TASKS = 128;
const MAX_VISIBLE_TOOLS = 12;
const MAX_VISIBLE_TODO_TASKS = 96;
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
  const safeDuration = Math.max(0, durationMs);
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

function toolStateGlyph(state: ExecutionToolState): string {
  switch (state) {
    case "running":
      return "◉";
    case "succeeded":
      return "●";
    case "failed":
    case "cancelled":
      return "○";
  }
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

function executionTone(state: ExecutionState | ExecutionToolState): "Accent" | "Good" | "Attention" | "Warning" {
  switch (state) {
    case "running":
      return "Accent";
    case "completed":
    case "succeeded":
      return "Good";
    case "cancelled":
      return "Warning";
    case "failed":
      return "Attention";
  }
}

function displayToolName(toolName: string): string {
  return TOOL_DISPLAY_LABELS[toolName] ?? toolName;
}

function todoStateLabel(status: TodoStatus): string {
  switch (status) {
    case "pending":
      return "待处理";
    case "in_progress":
      return "进行中";
    case "completed":
      return "已完成";
    case "abandoned":
      return "已放弃";
    case "blocked":
      return "已阻塞";
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
  const parts = includeGlyph ? [toolStateGlyph(tool.state), displayToolName(tool.toolName)] : [displayToolName(tool.toolName)];
  if (tool.summary) parts.push(tool.summary);
  if (tool.state === "running") parts.push("进行中");
  else {
    parts.push(toolStateLabel(tool.state));
    if (tool.state === "failed" && tool.errorCategory) parts.push(errorCategoryLabel(tool.errorCategory));
  }
  if (tool.durationMs !== null) parts.push(formatDuration(tool.durationMs));
  return parts.join(" · ");
}

function todoLines(
  todos: readonly ExecutionTodoPhase[],
  hiddenCount = 0,
  totalCount = 0,
): string[] {
  const lines: string[] = [];
  if (hiddenCount > 0) lines.push(`已隐藏 ${hiddenCount} 个任务，共 ${totalCount} 个任务`);
  for (const phase of todos) {
    lines.push(phase.name);
    for (const task of phase.tasks) {
      const blocker = task.blocker === undefined ? "" : ` · ${task.blocker}`;
      lines.push(`• [${todoStateLabel(task.status)}] ${task.content}${blocker}`);
    }
  }
  return lines;
}

function makeTextBlock(text: string, options: JsonNode = {}): JsonNode {
  return {
    type: "TextBlock",
    text,
    wrap: true,
    ...options,
  };
}

function makeToolRow(tool: ExecutionToolSnapshot): JsonNode {
  return {
    type: "ColumnSet",
    spacing: "None",
    columns: [
      {
        type: "Column",
        width: "auto",
        items: [makeTextBlock(toolStateGlyph(tool.state), { spacing: "None", color: executionTone(tool.state) })],
      },
      {
        type: "Column",
        width: "stretch",
        items: [makeTextBlock(toolLine(tool), { spacing: "None" })],
      },
    ],
  };
}

function makeTimelineItems(
  tools: readonly ExecutionToolSnapshot[],
  hiddenCount: number,
  richLayout: boolean,
): JsonNode[] {
  const items: JsonNode[] = [];
  if (hiddenCount > 0) items.push(makeTextBlock(`已隐藏前 ${hiddenCount} 个步骤`, { isSubtle: true, spacing: "None" }));
  if (tools.length === 0) {
    items.push(makeTextBlock("正在准备", { isSubtle: true, spacing: "None" }));
  } else if (richLayout) {
    items.push(...tools.map(makeToolRow));
  } else {
    items.push(...tools.map(tool => makeTextBlock(toolLine(tool, true), { spacing: "Small" })));
  }
  return items;
}

function makeTodoContainer(
  todos: readonly ExecutionTodoPhase[],
  hiddenCount: number,
  totalCount: number,
): JsonNode | null {
  if (todos.length === 0 || todos.every(phase => phase.tasks.length === 0)) return null;
  const lines = todoLines(todos, hiddenCount, totalCount);
  return {
    type: "Container",
    id: "todo_detail",
    spacing: "Medium",
    items: [
      makeTextBlock("任务", { weight: "Bolder", spacing: "None" }),
      ...lines.map(line => makeTextBlock(line, { spacing: "Small" })),
    ],
  };
}

function makeToggleSets(detailVisible: boolean, hasCollapsedSteps: boolean): JsonNode[] {
  const targets = [
    { elementId: "timeline_detail", isVisible: false },
    ...(hasCollapsedSteps ? [{ elementId: "collapsed_steps", isVisible: true }] : []),
    { elementId: "btn_collapse_trace", isVisible: false },
    { elementId: "btn_expand_trace", isVisible: true },
  ];
  const expandTargets = targets.map(target => ({ ...target, isVisible: !target.isVisible }));
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_trace",
      isVisible: detailVisible,
      horizontalAlignment: "Right",
      spacing: "Medium",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "trace_collapse",
          title: "收起执行详情",
          targetElements: targets,
        },
      ],
    },
    {
      type: "ActionSet",
      id: "btn_expand_trace",
      isVisible: !detailVisible,
      horizontalAlignment: "Right",
      spacing: "Medium",
      actions: [
        {
          type: TOGGLE_ACTION,
          id: "trace_expand",
          title: "展开执行详情",
          targetElements: expandTargets,
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

function makeHeader(
  status: string,
  metrics: string,
  state: ExecutionState,
  richLayout: boolean,
  stackedLayout: boolean,
): JsonNode[] {
  const statusBlock = makeTextBlock(status, {
    weight: "Bolder",
    spacing: "None",
    color: executionTone(state),
  });
  if (richLayout) {
    return [
      {
        type: "ColumnSet",
        spacing: "None",
        columns: [
          {
            type: "Column",
            width: "stretch",
            items: [
              makeTextBlock("处理进度", { weight: "Bolder", size: "Large", spacing: "None" }),
              makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
            ],
          },
          {
            type: "Column",
            width: "auto",
            items: [statusBlock],
          },
        ],
      },
    ];
  }
  if (stackedLayout) {
    return [
      makeTextBlock("处理进度", { weight: "Bolder", size: "Large", spacing: "None" }),
      makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
      statusBlock,
    ];
  }
  return [
    makeTextBlock(`处理进度 · ${status}`, {
      weight: "Bolder",
      color: executionTone(state),
      spacing: "None",
    }),
    makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
  ];
}

function makeHeaderContainer(
  snapshot: ExecutionSnapshot,
  metrics: string,
  richLayout: boolean,
  stackedLayout: boolean,
): JsonNode {
  return {
    type: "Container",
    id: "octo-execution-trace-header",
    style: snapshot.state === "failed"
      ? "attention"
      : snapshot.state === "cancelled"
        ? "warning"
        : snapshot.state === "completed"
          ? "good"
          : "emphasis",
    bleed: true,
    spacing: "None",
    items: makeHeader(executionStateLabel(snapshot.state), metrics, snapshot.state, richLayout, stackedLayout),
  };
}

function buildCandidate(
  snapshot: ExecutionSnapshot,
  capabilities: ExecutionCardCapabilities,
  visibleTools: readonly ExecutionToolSnapshot[],
  visibleTodos: readonly ExecutionTodoPhase[],
  totalTodoTasks: number,
): ExecutionCardResult {
  const hasContainer = supportsElement(capabilities, "Container");
  const hasColumnSet = supportsElement(capabilities, "ColumnSet");
  const richLayout = hasContainer && hasColumnSet;
  const stackedLayout = hasContainer;
  const canToggle = richLayout
    && supportsElement(capabilities, "ActionSet")
    && supportsAction(capabilities, TOGGLE_ACTION);
  const terminal = snapshot.state !== "running";
  const detailVisible = !(canToggle && terminal && visibleTools.length > 0);
  const hiddenTools = Math.max(0, snapshot.tools.length - visibleTools.length);
  const visibleTodoTasks = visibleTodos.reduce((count, phase) => count + phase.tasks.length, 0);
  const hiddenTodoTasks = Math.max(0, totalTodoTasks - visibleTodoTasks);
  const elapsed = formatDuration(snapshot.elapsedMs);
  const metrics = `${snapshot.phaseCount} 个阶段 · ${snapshot.toolCount} 次工具调用 · ${elapsed}`;
  const timelineItems = makeTimelineItems(visibleTools, hiddenTools, richLayout);
  const body: JsonNode[] = [];

  if (stackedLayout) {
    body.push(makeHeaderContainer(snapshot, metrics, richLayout, stackedLayout));
    body.push({
      type: "Container",
      id: "timeline_detail",
      isVisible: detailVisible,
      spacing: "Medium",
      items: timelineItems,
    });
  } else {
    body.push(
      makeTextBlock(`处理进度 · ${executionStateLabel(snapshot.state)}`, {
        weight: "Bolder",
        color: executionTone(snapshot.state),
        spacing: "None",
      }),
      makeTextBlock(metrics, { isSubtle: true, size: "Small", spacing: "Small" }),
      ...timelineItems,
    );
  }

  if (richLayout && detailVisible === false && visibleTools.length > 0) {
    body.push({
      type: "Container",
      id: "collapsed_steps",
      isVisible: true,
      spacing: "Medium",
      items: [makeToolRow(visibleTools[visibleTools.length - 1]!)],
    });
  }

  const todoContainer = hasContainer
    ? makeTodoContainer(visibleTodos, hiddenTodoTasks, totalTodoTasks)
    : null;
  if (todoContainer) {
    body.push(todoContainer);
  } else if (!hasContainer && visibleTodoTasks > 0) {
    body.push(...todoLines(visibleTodos, hiddenTodoTasks, totalTodoTasks).map(line => makeTextBlock(line, { spacing: "Small" })));
  }
  if (canToggle) body.push(...makeToggleSets(detailVisible, detailVisible === false && visibleTools.length > 0));

  const card: AdaptiveCard = {
    $schema: ADAPTIVE_CARD_SCHEMA,
    type: "AdaptiveCard",
    version: "1.5",
    body,
    metadata: { octo_layout: "agent_progress_v1" },
  };
  const plainLines = [
    `处理进度 · ${executionStateLabel(snapshot.state)} · ${metrics}`,
    ...visibleTools.map(tool => toolLine(tool, true)),
    ...(hiddenTools > 0 ? [`已隐藏前 ${hiddenTools} 个步骤，共 ${snapshot.tools.length} 个步骤`] : []),
    ...todoLines(visibleTodos, hiddenTodoTasks, totalTodoTasks),
  ];
  if (plainLines.length === 1) plainLines.push("正在准备");
  return { card, plain: plainLines.join("\n") };
}

function trimTodos(todos: readonly ExecutionTodoPhase[], maximumTasks: number): ExecutionTodoPhase[] {
  const taskRefs: Array<{ readonly task: ExecutionTodoTask }> = [];
  for (let phaseIndex = 0; phaseIndex < todos.length; phaseIndex += 1) {
    const phase = todos[phaseIndex]!;
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      taskRefs.push({ task: phase.tasks[taskIndex]! });
    }
  }

  const target = Math.min(taskRefs.length, Math.max(0, Math.floor(maximumTasks)));
  if (target === taskRefs.length) {
    return todos.map(phase => ({ name: phase.name, tasks: phase.tasks.slice() }));
  }
  if (target === 0) return [];

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

  const trimmed: ExecutionTodoPhase[] = [];
  let refIndex = 0;
  for (let phaseIndex = 0; phaseIndex < todos.length; phaseIndex += 1) {
    const phase = todos[phaseIndex]!;
    const tasks: ExecutionTodoTask[] = [];
    for (let taskIndex = 0; taskIndex < phase.tasks.length; taskIndex += 1) {
      if (selected[refIndex]) tasks.push(taskRefs[refIndex]!.task);
      refIndex += 1;
    }
    if (tasks.length > 0) trimmed.push({ name: phase.name, tasks });
  }
  return trimmed;
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
  let visibleToolCount = Math.min(totalTools, MAX_VISIBLE_TOOLS);
  let visibleTodoCount = Math.min(totalTodoTasks, MAX_VISIBLE_TODO_TASKS);
  while (visibleToolCount >= 0 && visibleTodoCount >= 0) {
    const visibleTools = snapshot.tools.slice(Math.max(0, totalTools - visibleToolCount));
    const visibleTodos = trimTodos(snapshot.todos, visibleTodoCount);
    const candidate = buildCandidate(snapshot, capabilities, visibleTools, visibleTodos, totalTodoTasks);
    if (cardWithinLimits(candidate.card, candidate.plain, limits)) return candidate;
    if (visibleToolCount > 0) {
      visibleToolCount -= 1;
      continue;
    }
    if (visibleTodoCount > 0) {
      visibleTodoCount -= 1;
      continue;
    }
    break;
  }
  return null;
}
