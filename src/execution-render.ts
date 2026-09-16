/**
 * 卡片渲染原语:文本格式化(时长、时间、状态标签与色调)、工具/todo 行与容器的元素工厂,
 * 以及按能力协商出的上限检查。这里只把快照与配置变成 AdaptiveCard 元素,不做取整与布局
 * 取舍——那是 `./execution-card.ts` 的事。
 */

import type { ExecutionToolSnapshot, ExecutionTodoPhase, ExecutionTodoTask, ExecutionState, ExecutionToolState, ExecutionSubagentState, ExecutionSubagentSnapshot, TodoStatus, ExecutionErrorCategory, ExecutionSnapshot } from "./execution-tracker.ts";
import { finiteTime } from "./execution-tracker.ts";
import type { AdaptiveCard, ExecutionCardCapabilities } from "./execution-card.ts";





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
const REQUIRED_ELEMENTS = ["TextBlock"] as const;
export const TOGGLE_ACTION = "Action.ToggleVisibility";
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
const DEFAULT_MAX_NODES = 200;
const DEFAULT_MAX_DEPTH = 16;
const DEFAULT_MAX_PAYLOAD_BYTES = 512 * 1024;
export interface JsonNode {
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

export function resolveCardLimits(capabilities: ExecutionCardCapabilities): ResolvedCardLimits | null {
  const requested = capabilities.limits;
  const maxNodes = positiveLimit(requested?.maxNodes, DEFAULT_MAX_NODES);
  const maxDepth = positiveLimit(requested?.maxDepth, DEFAULT_MAX_DEPTH);
  const maxPayloadBytes = positiveLimit(requested?.maxPayloadBytes, DEFAULT_MAX_PAYLOAD_BYTES);
  if (maxNodes === null || maxDepth === null || maxPayloadBytes === null) return null;
  return { maxNodes, maxDepth, maxPayloadBytes };
}

export function supportsElement(capabilities: ExecutionCardCapabilities, element: string): boolean {
  const elements = capabilities.elements ?? DEFAULT_ELEMENTS;
  return elements.includes(element);
}

export function supportsAction(capabilities: ExecutionCardCapabilities, action: string): boolean {
  return capabilities.actions?.includes(action) ?? false;
}

export function capabilitiesAllowCard(capabilities: ExecutionCardCapabilities): boolean {
  if (!capabilities.enabled || capabilities.cardVersion !== "1.5" || !capabilities.profiles.includes("octo/v1")) return false;
  return REQUIRED_ELEMENTS.every(element => supportsElement(capabilities, element));
}

export function formatDuration(durationMs: number): string {
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

export function executionStateLabel(state: ExecutionState): string {
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

export function executionStateGlyph(state: ExecutionState | ExecutionToolState | ExecutionSubagentState): string {
  switch (state) {
    case "running":
      return RUNNING_GLYPH;
    case "completed":
    case "succeeded":
      return SUCCESS_GLYPH;
    case "failed":
      return FAILURE_GLYPH;
    case "pending":
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

export function executionTone(
  state: ExecutionState | ExecutionToolState | ExecutionSubagentState,
): "Accent" | "Good" | "Attention" | "Default" {
  switch (state) {
    case "running":
      return "Accent";
    case "completed":
    case "succeeded":
      return "Good";
    case "pending":
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
export function subagentLine(subagent: ExecutionSubagentSnapshot, includeGlyph = false): string {
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

export function markdownSafeToolLine(tool: ExecutionToolSnapshot): string {
  const line = toolLine(tool, true, true);
  const glyph = toolStateGlyph(tool);
  if (glyph !== WAITING_GLYPH || !line.startsWith(`${glyph} `)) return line;
  return `${glyph}\u00a0${line.slice(glyph.length + 1)}`;
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

export function todoLines(
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
export function todoOmittedTaskIds(
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

export function makeTextBlock(text: string, options: JsonNode = {}): JsonNode {
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
 * Omitted-step history band. It renders as a sibling of `timeline_detail` instead of a child:
 * the web client's agent-progress layer repaints every filled Container that sits directly under
 * `#timeline_detail` as a failed-step status chip (danger fill + border, inline fill cleared),
 * which turned this grey band into a red block. Outside that container the band keeps the
 * section's emphasis fill, matching the subagent history band.
 */
export function makeTraceHistoryBand(tools: readonly ExecutionToolSnapshot[]): JsonNode {
  return {
    type: "Container",
    id: "timeline_omitted_steps",
    isVisible: false,
    style: "emphasis",
    spacing: "Small",
    items: tools.map(tool => makeCompactToolRow(tool)),
  };
}

export function makeTimelineItems(
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

export function makeTodoContainer(
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
 * The subagent section. It is fed by the OMP subagent events alone: live children render as rich
 * rows, finished ones sink into the expandable history band, and the count in the heading is the
 * number of subagents the session actually reported.
 */
export function makeSubagentContainer(
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

export function makeCollapsibleSection(
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

export function makeToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
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

export function makeSubagentToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
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
export function makeTodoToggleSets(hiddenElementIds: readonly string[]): JsonNode[] {
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
export function makeProgressToggleSets(
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

export function cardWithinLimits(card: AdaptiveCard, plain: string, limits: ResolvedCardLimits): boolean {
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

export function makeHeader(richLayout: boolean, actionSets: readonly JsonNode[] = []): JsonNode[] {
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

export function makeHeaderContainer(
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

export function metricCount(value: number): string {
  if (!Number.isSafeInteger(value) || value < 0) return "0";
  return String(value);
}

export function makeStatusContainer(
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


export function executionNotice(
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

export function makeNotice(notice: { readonly text: string; readonly style: "default" | "attention" }): JsonNode {
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

/**
 * Index (in source order across phases) of the task the run stands on: the first in-progress task,
 * else the first still-open task, else the tail of a settled list. The visible band grows outward
 * from here, so the completed ↔ open boundary survives a budget trim.
 */
export function handoffAnchorIndex(todos: readonly ExecutionTodoPhase[]): number {
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
