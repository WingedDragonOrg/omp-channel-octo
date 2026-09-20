/**
 * 研究迭代卡渲染:把 tracker 注入的研究快照渲染成常显摘要、可折叠的最近记录窗口与诚实的
 * 省略计数。措辞是展示选择,可观察契约只有结构与数字——摘要常显且从不被预算牺牲、最近窗口
 * 按 6/3/1/0 降级、省略数恒定等于当前段全部记录数减去实际展示行数。
 */

import { executionStateLabel, makeTextBlock, TOGGLE_ACTION, type JsonNode } from "./execution-render.ts";
import type { ExecutionState } from "./execution-tracker.ts";
import type { ResearchRun, ResearchSnapshot } from "./research.ts";

const RESEARCH_STATUS_LABELS: Record<ResearchRun["status"], string> = {
  keep: "保留",
  discard: "丢弃",
  crash: "崩溃",
  checks_failed: "检查未通过",
};
const RESEARCH_STATUS_GLYPHS: Record<ResearchRun["status"], string> = {
  keep: "✓",
  discard: "-",
  crash: "×",
  checks_failed: "×",
};
const RESEARCH_PHASE_LABELS: Record<ResearchSnapshot["phase"], string> = {
  ready: "就绪",
  running: "运行中",
  awaiting_log: "等待记录",
  limit_reached: "已达上限",
};

/** 研究全记录的入口命令,卡片只指路、不内置交互。 */
export const RESEARCH_ENTRY_COMMAND = "/autoresearch-history";
/** 最近记录窗口的降级阶梯:摘要从不牺牲,只有展示行数让位。 */
export const RESEARCH_ROW_LADDER = [6, 3, 1, 0] as const;

/** 方向敏感的基线→最佳改善百分比;零基线、缺值或溢出时返回 null——绝不渲染负号错位或无穷。 */
function improvementPercent(research: ResearchSnapshot): number | null {
  const { baseline, best, direction } = research;
  if (baseline === null || best === null || baseline === 0) return null;
  // Normalize before subtraction so finite extremes do not overflow an otherwise representable gain.
  const magnitude = Math.abs(baseline);
  const change = best / magnitude - baseline / magnitude;
  const percent = (direction === "lower" ? -change : change) * 100;
  return Number.isFinite(percent) ? percent : null;
}

function researchRunLine(run: ResearchRun, metricUnit: string): string {
  // null 是合法旧记录编号:照常渲染这行,但绝不臆造一个号码。
  const id = run.runNumber === null ? "未编号" : `#${run.runNumber}`;
  const unit = metricUnit.length > 0 ? ` ${metricUnit}` : "";
  const parts = [
    `${RESEARCH_STATUS_GLYPHS[run.status]} · ${id} · ${String(run.metric)}${unit} · ${RESEARCH_STATUS_LABELS[run.status]}`,
  ];
  if (run.flagged) parts.push("已标记");
  if (run.description.length > 0) parts.push(run.description);
  return parts.join(" · ");
}

/** 常显摘要的文本行:goal 为空整行省略,缺值字段整段省略,零基线不出改善。 */
export function researchSummaryLines(research: ResearchSnapshot, executionState: ExecutionState): string[] {
  const lines: string[] = [];
  const title = ["研究进度"];
  if (research.name.length > 0) title.push(research.name);
  // 执行卡才是任务时钟:任务已结算时不复述研究 loop 的相位(那一刻的快照旧说 running),
  // 改用执行终态词;只有任务还在跑时才显示原生 research.phase。
  title.push(executionState === "running" ? RESEARCH_PHASE_LABELS[research.phase] : executionStateLabel(executionState));
  // 原生 start 未必知道编号:end 才带回 activeRun,而 phase 已翻页——编号非空就显示,
  // ready/limit 的清理由数据侧负责。
  if (research.activeRun !== null) title.push(`第 ${research.activeRun} 轮`);
  lines.push(title.join(" · "));
  if (research.goal.length > 0) lines.push(`目标 ${research.goal}`);
  let metricLine = `指标 ${research.metricName}`;
  if (research.metricUnit.length > 0) metricLine += ` (${research.metricUnit})`;
  const metricParts = [metricLine, research.direction === "lower" ? "越低越好" : "越高越好"];
  if (research.latestMetric !== null) metricParts.push(`最新 ${String(research.latestMetric)}`);
  lines.push(metricParts.join(" · "));
  const score: string[] = [];
  if (research.baseline !== null) score.push(`基线 ${String(research.baseline)}`);
  if (research.best !== null) score.push(`最佳 ${String(research.best)}`);
  const percent = improvementPercent(research);
  if (percent !== null) score.push(`改善 ${percent.toFixed(1)}%`);
  if (score.length > 0) lines.push(score.join(" · "));
  let roundsLine = `轮次 ${research.completedRuns}`;
  if (research.maxExperiments !== null) roundsLine += `/${research.maxExperiments}`;
  roundsLine += ` · 累计 ${research.totalRuns} 条`;
  lines.push(roundsLine);
  lines.push(`完整记录 ${RESEARCH_ENTRY_COMMAND}`);
  return lines;
}

export interface ResearchRecentWindow {
  readonly rows: readonly ResearchRun[];
  readonly omitted: number;
}

/** 最近窗口:展示时序上最新的 rowLimit 行,省略数按当前段全部记录计——不只按 recent 窗口。 */
export function researchRecentWindow(research: ResearchSnapshot, rowLimit: number): ResearchRecentWindow {
  const limit = Math.max(0, Math.floor(rowLimit));
  const rows = research.recent.slice(Math.max(0, research.recent.length - limit));
  return { rows, omitted: Math.max(0, research.completedRuns - rows.length) };
}

function makeResearchToggleSets(): JsonNode[] {
  const collapsed = [
    { elementId: "research_recent_summary", isVisible: true },
    { elementId: "research_recent_runs", isVisible: false },
    { elementId: "btn_collapse_research", isVisible: false },
    { elementId: "btn_expand_research", isVisible: true },
  ];
  const expanded = [
    { elementId: "research_recent_summary", isVisible: false },
    { elementId: "research_recent_runs", isVisible: true },
    { elementId: "btn_collapse_research", isVisible: true },
    { elementId: "btn_expand_research", isVisible: false },
  ];
  return [
    {
      type: "ActionSet",
      id: "btn_collapse_research",
      isVisible: false,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [{
        type: TOGGLE_ACTION,
        id: "research_collapse",
        title: "收起近期记录",
        targetElements: collapsed,
      }],
    },
    {
      type: "ActionSet",
      id: "btn_expand_research",
      isVisible: true,
      horizontalAlignment: "Right",
      spacing: "None",
      actions: [{
        type: TOGGLE_ACTION,
        id: "research_expand",
        title: "展开近期记录",
        targetElements: expanded,
      }],
    },
  ];
}

export interface ResearchCardItemsOptions {
  readonly canToggle: boolean;
  readonly rowLimit: number;
  readonly executionState: ExecutionState;
}

/**
 * 富卡研究区块:常显摘要容器,折叠行与历史带是顶层兄弟节点,折叠只换研究自己的四个 id,
 * 与总折叠、轨迹折叠互不染指。无 Toggle 能力时历史带直接可见、不出按钮;窗口被预算压到
 * 零行时省略计数单独留在折叠行的位置。
 */
export function makeResearchCardItems(research: ResearchSnapshot, options: ResearchCardItemsOptions): JsonNode[] {
  const summaryItems: JsonNode[] = researchSummaryLines(research, options.executionState).map((line, index) => makeTextBlock(line, {
    ...(index === 0 ? { weight: "Bolder", size: "Medium" } : {}),
    spacing: index === 0 ? "None" : "Small",
    wrap: true,
  }));
  const window = researchRecentWindow(research, options.rowLimit);
  const toggleSets = options.canToggle && window.rows.length > 0 ? makeResearchToggleSets() : [];
  const summary: JsonNode = {
    type: "Container",
    id: "research_summary",
    isVisible: true,
    spacing: "Medium",
    items: [...summaryItems, ...toggleSets],
  };
  if (window.rows.length === 0) {
    // 研究尚未开始记录时连省略计数都没有;预算把窗口压到零行时,计数单独可见。
    if (window.omitted === 0) return [summary];
    return [
      summary,
      makeTextBlock(`已省略 ${window.omitted} 条记录`, {
        id: "research_recent_summary",
        isSubtle: true,
        spacing: "None",
        wrap: true,
      }),
    ];
  }
  const bandItems: JsonNode[] = [];
  if (window.omitted > 0) {
    bandItems.push(makeTextBlock(`已省略 ${window.omitted} 条记录`, { isSubtle: true, spacing: "Small", wrap: true }));
  }
  for (const run of window.rows) {
    bandItems.push(makeTextBlock(researchRunLine(run, research.metricUnit), { spacing: "Small", wrap: true }));
  }
  return [
    summary,
    // 折叠提示只配折叠控件:无 Toggle 时波段直接可见,再讲『已折叠』自相矛盾还多占节点。
    ...(options.canToggle
      ? [makeTextBlock(`已折叠 ${window.rows.length} 条最近记录`, {
        id: "research_recent_summary",
        isSubtle: true,
        spacing: "None",
        wrap: true,
      })]
      : []),
    {
      type: "Container",
      id: "research_recent_runs",
      style: "emphasis",
      isVisible: !options.canToggle,
      spacing: "Small",
      items: bandItems,
    },
  ];
}

/** 无布局元素的纯文本降级:摘要行与最近窗口一起落成连续文本,不出折叠控件。 */
export function makeResearchFlatLines(research: ResearchSnapshot, rowLimit: number, executionState: ExecutionState): string[] {
  const lines = researchSummaryLines(research, executionState);
  const window = researchRecentWindow(research, rowLimit);
  if (window.omitted > 0) lines.push(`已省略 ${window.omitted} 条记录`);
  for (const run of window.rows) {
    lines.push(researchRunLine(run, research.metricUnit));
  }
  return lines;
}
