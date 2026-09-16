/**
 * 进度卡组装:把快照按预算装配成一张 AdaptiveCard——分区、可见行与隐藏行预算、
 * 贴合卡面上限的降级顺序,以及公开入口 `buildExecutionCard`。
 */

import type { ExecutionSnapshot, ExecutionTodoPhase, ExecutionTodoTask, ExecutionToolSnapshot } from "./execution-tracker.ts";
import {
  TOGGLE_ACTION,
  capabilitiesAllowCard,
  cardWithinLimits,
  executionNotice,
  executionStateGlyph,
  executionStateLabel,
  executionTone,
  formatDuration,
  handoffAnchorIndex,
  isActiveSubagent,
  isSubagentTool,
  makeCollapsibleSection,
  makeHeader,
  makeHeaderContainer,
  makeNotice,
  makeProgressToggleSets,
  makeStatusContainer,
  makeSubagentContainer,
  makeSubagentToggleSets,
  makeTextBlock,
  makeTimelineItems,
  makeTodoContainer,
  makeTodoToggleSets,
  makeToggleSets,
  makeTraceHistoryBand,
  markdownSafeToolLine,
  metricCount,
  resolveCardLimits,
  supportsAction,
  supportsElement,
  todoLines,
  todoOmittedTaskIds,
} from "./execution-render.ts";
import type {
  JsonNode,
} from "./execution-render.ts";

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
const ADAPTIVE_CARD_SCHEMA = "http://adaptivecards.io/schemas/adaptive-card.json";
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
  const cappedOmittedTraceTools = omittedTraceTools.length <= MAX_COMPACT_TOOL_ROWS ? omittedTraceTools : [];
  const expandableOmittedTraceTools = cappedOmittedTraceTools.slice(
    Math.max(0, cappedOmittedTraceTools.length - hiddenRowLimit),
  );
  const omittedSubagentTools = richLayout
    ? [
        ...omittedTools.filter(tool => isSubagentTool(tool.toolName) && !isActiveSubagent(tool)),
        ...visibleTools.filter(tool => isSubagentTool(tool.toolName) && !isActiveSubagent(tool)),
      ]
    : [];
  const cappedOmittedSubagentTools = omittedSubagentTools.length <= MAX_COMPACT_TOOL_ROWS
    ? omittedSubagentTools
    : [];
  const expandableOmittedSubagentTools = cappedOmittedSubagentTools.slice(
    Math.max(0, cappedOmittedSubagentTools.length - hiddenRowLimit),
  );
  const totalTraceTools = richLayout ? snapshot.tools.length - totalSubagents : snapshot.tools.length;
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
