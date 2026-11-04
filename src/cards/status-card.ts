import type { CardFrame, ChannelStatus } from "../contracts.ts";
import { safeDisplayText } from "./execution-text.ts";
import type { SessionContextStatus } from "../session/status-context.ts";
import type { ThreadReferences } from "../references/types.ts";
import type { RepositoryChange } from "../workspace/git-status.ts";

export interface StatusCardInput {
  status: ChannelStatus;
  operation: string | null;
  goal: string;
  modelPolicy: string;
  liveModel: string;
  context: SessionContextStatus;
  repositories: readonly RepositoryChange[];
  references: ThreadReferences;
}

function display(value: unknown, fallback = "无"): string {
  return safeDisplayText(value, 240) ?? fallback;
}

function compactTokens(value: number): string {
  if (value < 1_000) return String(Math.round(value));
  if (value < 10_000) return `${(Math.round(value / 100) / 10).toFixed(1)}k`;
  if (value < 1_000_000) return `${Math.round(value / 1_000)}k`;
  return `${Math.round(value / 100_000) / 10}M`;
}

function percent(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "";
  const rounded = Math.round(value * 10) / 10;
  return `${Number.isInteger(rounded) ? rounded.toFixed(0) : rounded.toFixed(1)}%`;
}

function contextMetric(context: SessionContextStatus): string {
  const tokens = context.usedTokens;
  const window = context.contextWindow;
  const hasTokens = tokens !== null && Number.isFinite(tokens) && tokens >= 0;
  const hasWindow = window !== null && Number.isFinite(window) && window > 0;
  if (!hasTokens && !hasWindow) return "未知";

  const used = hasTokens ? compactTokens(tokens) : "已用未知";
  const limit = hasWindow ? compactTokens(window) : "上限未知";
  const ratio = hasWindow ? percent(context.percent) : "比例未知";
  const ratioNote = ratio.length > 0 ? ratio : hasTokens ? "比例未知" : "";
  return `${used} / ${limit}${ratioNote.length > 0 ? ` · ${ratioNote}` : ""}`;
}

function readableObservedAt(value: string | null): string {
  if (value === null) return "时间未知";
  const parts = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})$/u.exec(value);
  if (parts === null || !Number.isFinite(Date.parse(value))) return "时间未知";
  const offset = parts[3] === "Z" ? "+00:00" : parts[3];
  return `${parts[1]} ${parts[2]} ${offset}`;
}

function contextFreshness(context: SessionContextStatus): string {
  if (context.source === "live") {
    const stale = context.stale ? " · 会话文件已变化" : "";
    return `OMP 原生估算 · 采样于 ${readableObservedAt(context.observedAt)}${stale}`;
  }
  if (context.source === "persisted") {
    const stale = context.stale ? " · 会话文件已变化，作为历史参考" : "";
    return `OMP 原生估算 · 最后记录于 ${readableObservedAt(context.observedAt)} · 非实时${stale}`;
  }
  return `未知 · ${display(context.note, "未提供说明")}`;
}

function section(text: string): Record<string, unknown> {
  return { type: "TextBlock", text, size: "Medium", weight: "Bolder", wrap: true, spacing: "Medium" };
}

function row(text: string, spacing: "Small" | "Default" = "Small"): Record<string, unknown> {
  return { type: "TextBlock", text, wrap: true, spacing };
}

function repositoryPlain(change: RepositoryChange): string {
  const name = display(change.name, "工作区");
  const path = display(change.path, "路径未就绪");
  if (change.state !== "git") return `repository=${name} path=${path} state=${change.state}`;
  return [
    `repository=${name}`,
    `path=${path}`,
    "state=git",
    `baseline=${change.baseline ?? "unknown"}`,
    `+${change.added}/-${change.deleted}`,
    `binaryFiles=${change.binaryFiles}`,
    `untrackedFiles=${change.untrackedFiles}（未计入行数）`,
  ].join(" ");
}

function repositoryRows(change: RepositoryChange): Record<string, unknown>[] {
  const name = display(change.name, "工作区");
  const path = display(change.path, "路径未就绪");
  if (change.state === "non-git") return [row(`${name} · ${path} · 非 Git 工作区`)];
  if (change.state === "unavailable") return [row(`${name} · ${path} · 状态不可用`, "Default")];

  const baseline = change.baseline === "empty-tree" ? "空树基线（尚无提交）" : "相对 HEAD";
  return [
    row(`${name} · ${path}`),
    row(`${baseline} · +${change.added} / -${change.deleted} 行`),
    row(`二进制文件 ${change.binaryFiles} 个 · 未跟踪文件 ${change.untrackedFiles} 个（未计入行数）`),
  ];
}

function safeReferenceUrl(value: string): string | undefined {
  return safeDisplayText(value, value.length * 2);
}

function referenceRows(references: ThreadReferences): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  for (const url of references.pr) {
    const href = safeReferenceUrl(url);
    const label = display(url, "链接不可用");
    rows.push(row(href === undefined ? `PR · ${label}` : `[PR · ${label}](${href})`));
  }
  for (const url of references.mr) {
    const href = safeReferenceUrl(url);
    const label = display(url, "链接不可用");
    rows.push(row(href === undefined ? `MR · ${label}` : `[MR · ${label}](${href})`));
  }
  return rows;
}

function referencePlainLines(references: ThreadReferences): string[] {
  return [
    ...references.pr.map(url => `reference=PR · ${safeReferenceUrl(url) ?? "链接不可用"}`),
    ...references.mr.map(url => `reference=MR · ${safeReferenceUrl(url) ?? "链接不可用"}`),
  ];
}

export function buildStatusCard(input: StatusCardInput): CardFrame {
  const { status, context, references } = input;
  const session = display(status.sessionId);
  const cwd = display(status.cwd);
  const operation = display(input.operation ?? "none", "none");
  const goal = display(input.goal, "none");
  const modelPolicy = display(input.modelPolicy, "不指定（由 OMP 自行决定）");
  const liveModel = display(input.liveModel, "无运行中会话");
  const contextModel = display(context.model, "无");
  const metric = contextMetric(context);
  const freshness = contextFreshness(context);

  const plainLines = [
    `session=${session}`,
    `cwd=${cwd}`,
    `state=${status.state}`,
    `operation=${operation}`,
    `goal=${goal}`,
    `mode=${status.mode}`,
    `model=${modelPolicy}（下次启动策略）`,
    `liveModel=${liveModel}`,
    `queued=${status.queued}`,
    `outbox=${status.outbox}`,
    `unknown=${status.unknownOutbox}`,
    `pendingQuestionIds=${status.pendingQuestionIds.join(",")}`,
    `context=${metric}`,
    `contextSource=${context.source}`,
    `contextObservedAt=${readableObservedAt(context.observedAt)}`,
    `contextStale=${context.stale}`,
    `contextNote=${display(context.note, "未知")}`,
    `contextModel=${contextModel}`,
    ...input.repositories.map(repositoryPlain),
    ...referencePlainLines(references),
  ].filter(line => line.length > 0);

  const body: Record<string, unknown>[] = [
    section("会话上下文"),
  ];

  if (metric !== "未知" && context.source === "live") {
    body.push({ type: "TextBlock", text: metric, size: "Large", weight: "Bolder", color: "Accent", wrap: true });
  } else if (metric !== "未知" && context.source === "persisted") {
    body.push({ type: "TextBlock", text: metric, size: "Large", weight: "Bolder", isSubtle: true, wrap: true });
    body.push(row(`模型 ${contextModel}`));
  } else if (metric !== "未知") {
    body.push(row(metric));
  } else {
    body.push(row("当前占用未知"));
  }
  body.push(row(freshness));

  body.push(section("会话与任务"));
  body.push(
    row(`会话 ${session}`),
    row(`工作目录 ${cwd}`),
    row(`状态 ${status.state} · 操作 ${operation} · 目标 ${goal}`),
    row(`追加方式 ${status.mode} · 下次启动 ${modelPolicy}`),
    row(`当前会话模型 ${liveModel}`),
    row(`队列 ${status.queued} · 待投递 ${status.outbox} · 未知状态 ${status.unknownOutbox}`),
    row(status.pendingQuestionIds.length === 0
      ? "待回答问题：无"
      : `待回答问题：${status.pendingQuestionIds.map(id => display(id, "未知")).join("、")}`),
  );

  body.push(section("工作区 Git"));
  if (input.repositories.length === 0) body.push(row("无可查询的工作区"));
  else for (const repository of input.repositories) body.push(...repositoryRows(repository));

  const referencesBody = referenceRows(references);
  if (referencesBody.length > 0) {
    body.push(section("关联 PR / MR"));
    body.push(...referencesBody);
  }

  return {
    card: { type: "AdaptiveCard", version: "1.5", body },
    plain: plainLines.join("\n"),
    profile: "octo/v1",
  };
}
