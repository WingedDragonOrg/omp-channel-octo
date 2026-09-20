/**
 * 研究数据：把原生 autoresearch 工具（init_experiment / run_experiment / log_experiment）
 * 的权威 details 折叠成一次 prompt 的研究快照流，供执行卡与 /autoresearch-history 消费。
 *
 * 三条纪律：
 * - 只有成功且形状合法的原生详情、且与一次已观察 start 配对、按 start 序号不旧于最后
 *   一次已接受的完成，才更新权威状态；args、文本、isError 结果、非原生工具、陈旧或
 *   重放的 completion 都不会被推测成实验记录。
 * - 上游 `state.bestMetric` 是当前段基线（第一个未标记 keep），绝不是最佳值；最佳值这里
 *   按方向从未标记 keep 里取极值。聚合只看当前段，其他段的记录只进总账与全记录报告。
 * - 事件与报告只含白名单字段，绝不带出 ASI、notes、命令行、原始输出或本机路径。
 */

import { safeDisplayText } from "./execution-text.ts";
import { isRecord } from "./omp/rpc.ts";

/** 一条已记录的实验，跨 segment 的全记录报告复用同一形状。 */
export interface ResearchRun {
	/** 上游旧记录允许 null（未编号迁移行）；绝不伪造编号。 */
	readonly runNumber: number | null;
	readonly segment: number;
	readonly metric: number;
	readonly status: "keep" | "discard" | "crash" | "checks_failed";
	readonly description: string;
	readonly flagged: boolean;
}

/** 一次 prompt 观察到的研究状态；report 只挂在事件上，不进此快照。 */
export interface ResearchSnapshot {
	readonly sessionId: number | null;
	readonly name: string;
	readonly goal: string;
	readonly segment: number;
	readonly metricName: string;
	readonly metricUnit: string;
	readonly direction: "lower" | "higher";
	readonly baseline: number | null;
	readonly best: number | null;
	readonly completedRuns: number;
	readonly maxExperiments: number | null;
	readonly phase: "ready" | "running" | "awaiting_log" | "limit_reached";
	readonly activeRun: number | null;
	readonly latestMetric: number | null;
	/** 当前 segment 的新近记录，按时序最多保留 {@link RECENT_RESEARCH_RUNS} 条。 */
	readonly recent: readonly ResearchRun[];
	readonly totalRuns: number;
}

/** ExecutionEvent 的 research_update 分支所携带的形状（union 在 execution-tracker.ts）。 */
export interface ResearchUpdateEvent {
	readonly type: "research_update";
	readonly atMs: number;
	readonly research: ResearchSnapshot;
	readonly report?: string;
}

/** `recent` 窗口：当前 segment 时序上最新的记录条数。 */
export const RECENT_RESEARCH_RUNS = 32;
/** 上游单次 state.results 的硬上限：超过即拒绝整份 state，避免伪造聚合。 */
export const MAX_RESEARCH_STATE_RESULTS = 10_000;
/** 脱敏全记录报告的字节上限（UTF-8），报告生成与存储/读取共用同一值。 */
export const MAX_RESEARCH_REPORT_BYTES = 1_048_576;
/** 研究字段的展示文本净化上限（复用 execution-text 的默认 240）。 */
export const RESEARCH_STRING_CHARS = 240;

/** 状态与阶段令牌：Record + hasOwn，原型链上的名字（constructor/toString）不是合法值。 */
const RESEARCH_RUN_STATUSES: Record<string, ResearchRun["status"]> = {
	keep: "keep",
	discard: "discard",
	crash: "crash",
	checks_failed: "checks_failed",
};
const RESEARCH_PHASES: Record<string, ResearchSnapshot["phase"]> = {
	ready: "ready",
	running: "running",
	awaiting_log: "awaiting_log",
	limit_reached: "limit_reached",
};

/** 三个原生研究工具：只有它们的详情可能进入权威状态。 */
const NATIVE_RESEARCH_TOOLS: Record<string, true> = {
	init_experiment: true,
	run_experiment: true,
	log_experiment: true,
};

function asInteger(value: unknown): number | undefined {
	return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function asIntegerOrNull(value: unknown): number | null {
	return asInteger(value) ?? null;
}

function asFiniteOrNull(value: unknown): number | null {
	return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** 自由文本统一净化：非字符串或净化为空归一为 ""（研究文本字段允许留空）。 */
function displayText(value: unknown): string {
	return typeof value === "string" ? (safeDisplayText(value, RESEARCH_STRING_CHARS) ?? "") : "";
}

/** 必填文本：净化后必须非空，否则整份输入作废（metricName 是聚合键）。 */
function requiredDisplayText(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const text = safeDisplayText(value, RESEARCH_STRING_CHARS);
	return text === undefined || text.length === 0 ? undefined : text;
}

function sanitizeRun(value: unknown): ResearchRun | undefined {
	if (!isRecord(value)) return undefined;
	const status =
		typeof value.status === "string" && Object.hasOwn(RESEARCH_RUN_STATUSES, value.status)
			? RESEARCH_RUN_STATUSES[value.status]!
			: undefined;
	const metric = typeof value.metric === "number" && Number.isFinite(value.metric) ? value.metric : undefined;
	const segment = asInteger(value.segment);
	const description = typeof value.description === "string" ? displayText(value.description) : undefined;
	if (status === undefined || metric === undefined || segment === undefined || description === undefined) return undefined;
	// null 是合法旧记录；非 null 必须是有效整数，绝不臆造编号。
	if (value.runNumber !== null && value.runNumber !== undefined && asInteger(value.runNumber) === undefined) return undefined;
	return {
		runNumber: value.runNumber === undefined ? null : asIntegerOrNull(value.runNumber),
		segment,
		metric,
		status,
		description,
		flagged: value.flagged === true,
	};
}

/**
 * tracker/restore 共用的快照净化：整份输入非对象、direction/phase 非枚举、metricName
 * 净化为空、recent 非数组、segment/completedRuns/totalRuns 非有效整数 → 返回 undefined；
 * 其余数值字段非有限置 null 并整数化；name/goal/metricUnit 归一为可能为空的字符串。
 * recent 是有界窗口：先取末尾 RECENT_RESEARCH_RUNS 条原始行再逐条净化，并只留属于
 * 当前 segment 的行——这是恢复/入站边界，不为坏行回填更早历史。
 */
export function sanitizeResearchSnapshot(value: unknown): ResearchSnapshot | undefined {
	if (!isRecord(value)) return undefined;
	const direction = value.direction === "lower" || value.direction === "higher" ? value.direction : undefined;
	const phase =
		typeof value.phase === "string" && Object.hasOwn(RESEARCH_PHASES, value.phase) ? RESEARCH_PHASES[value.phase]! : undefined;
	const metricName = requiredDisplayText(value.metricName);
	const segment = asInteger(value.segment);
	const completedRuns = asInteger(value.completedRuns);
	const totalRuns = asInteger(value.totalRuns);
	if (direction === undefined || phase === undefined || metricName === undefined || segment === undefined) return undefined;
	if (completedRuns === undefined || totalRuns === undefined) return undefined;
	if (!Array.isArray(value.recent)) return undefined;
	const recent: ResearchRun[] = [];
	for (const raw of value.recent.slice(-RECENT_RESEARCH_RUNS)) {
		const run = sanitizeRun(raw);
		if (run === undefined || run.segment !== segment) continue;
		recent.push(run);
	}
	return {
		sessionId: asIntegerOrNull(value.sessionId),
		name: displayText(value.name),
		goal: displayText(value.goal),
		segment,
		metricName,
		metricUnit: displayText(value.metricUnit),
		direction,
		baseline: asFiniteOrNull(value.baseline),
		best: asFiniteOrNull(value.best),
		completedRuns,
		maxExperiments: asIntegerOrNull(value.maxExperiments),
		phase,
		activeRun: asIntegerOrNull(value.activeRun),
		latestMetric: asFiniteOrNull(value.latestMetric),
		recent,
		totalRuns,
	};
}

/** 从合法上游 ExperimentState 归约出的权威研究面；快照的 phase/pending 由 tracker 注入。 */
type DerivedResearchState = {
	name: string;
	goal: string;
	metricName: string;
	metricUnit: string;
	direction: "lower" | "higher";
	segment: number;
	maxExperiments: number | null;
	sessionId: number | null;
	baseline: number | null;
	best: number | null;
	completedRuns: number;
	totalRuns: number;
	recent: readonly ResearchRun[];
	/** 全部已净化记录（跨 segment，按时序）；报告的全记录从这里生成。 */
	allRuns: readonly ResearchRun[];
};

/**
 * 权威 state 归约：results 任一行非法即整份拒绝——原生历史是权威的，丢弃坏行再聚合
 * 会让 completedRuns/totalRuns/baseline 静默回退。恢复边界的宽松行丢弃只在
 * sanitizeResearchSnapshot，不在这里。
 */
function deriveResearchState(value: unknown): DerivedResearchState | undefined {
	if (!isRecord(value)) return undefined;
	if (!Array.isArray(value.results) || value.results.length > MAX_RESEARCH_STATE_RESULTS) return undefined;
	const direction = value.bestDirection === "lower" || value.bestDirection === "higher" ? value.bestDirection : undefined;
	const metricName = requiredDisplayText(value.metricName);
	const segment = asInteger(value.currentSegment);
	if (direction === undefined || metricName === undefined || segment === undefined) return undefined;
	const allRuns: ResearchRun[] = [];
	for (const row of value.results) {
		const run = sanitizeRun(row);
		if (run === undefined) return undefined;
		allRuns.push(run);
	}
	const current = allRuns.filter(run => run.segment === segment);
	// 基线 = 当前段第一个未标记 keep（时序）；上游 state.bestMetric 正是这个值，
	// 但这里重算而不听信：它名义上是"最佳值"，直接当 best 用会把基线错报成最佳。
	const keepUnflagged = current.filter(run => run.status === "keep" && !run.flagged);
	const baseline = keepUnflagged.length > 0 ? keepUnflagged[0]!.metric : null;
	let best: number | null = null;
	for (const run of keepUnflagged) {
		if (best === null || (direction === "lower" ? run.metric < best : run.metric > best)) best = run.metric;
	}
	return {
		name: displayText(value.name),
		goal: displayText(value.goal),
		metricName,
		metricUnit: displayText(value.metricUnit),
		direction,
		segment,
		maxExperiments: asIntegerOrNull(value.maxExperiments),
		sessionId: asIntegerOrNull(value.sessionId),
		baseline,
		best,
		completedRuns: current.length,
		totalRuns: allRuns.length,
		recent: current.slice(-RECENT_RESEARCH_RUNS),
		allRuns,
	};
}

/** run_experiment 成功 end 里唯一被采信的字段：pending 轮次号与解析主指标。 */
function parseRunEndDetails(result: unknown): { runNumber: number; parsedPrimary: number | null } | undefined {
	if (!isRecord(result) || !isRecord(result.details)) return undefined;
	const runNumber = asInteger(result.details.runNumber);
	if (runNumber === undefined) return undefined;
	const raw = result.details.parsedPrimary;
	if (raw === null || raw === undefined) return { runNumber, parsedPrimary: null };
	if (typeof raw !== "number" || !Number.isFinite(raw)) return undefined;
	return { runNumber, parsedPrimary: raw };
}

function parseInitDetails(result: unknown): DerivedResearchState | undefined {
	if (!isRecord(result) || !isRecord(result.details)) return undefined;
	return deriveResearchState(result.details.state);
}

function parseLogDetails(result: unknown): { state: DerivedResearchState; loggedMetric: number } | undefined {
	if (!isRecord(result) || !isRecord(result.details)) return undefined;
	const state = deriveResearchState(result.details.state);
	const experiment = isRecord(result.details.experiment) ? result.details.experiment : undefined;
	const loggedMetric = typeof experiment?.metric === "number" && Number.isFinite(experiment.metric) ? experiment.metric : undefined;
	if (state === undefined || loggedMetric === undefined) return undefined;
	return { state, loggedMetric };
}

/** Markdown 自由文本转义：description/goal/name 来自模型，不能破坏列表结构；`<`/`>` 同逃，防被识别成 HTML tag。 */
function escapeMarkdown(value: string): string {
	return value.replace(/[\\`*_[\]<>()|]/gu, "\\$&");
}

function reportText(value: string): string {
	return escapeMarkdown(safeDisplayText(value, RESEARCH_STRING_CHARS) ?? "");
}

function reportMetric(value: number | null): string {
	return value === null ? "—" : String(value);
}

function reportRunLine(run: ResearchRun): string {
	const id = run.runNumber === null ? "未编号" : `#${run.runNumber}`;
	const flag = run.flagged ? "（已标记）" : "";
	const description = reportText(run.description);
	// 跨段行的数值列保持中性：原生旧 row 未保留当时的主指标名，不套本名/单位。
	return `- 段 ${run.segment} · ${id} · ${run.status} · 指标值 ${run.metric}${description ? ` · ${description}` : ""}${flag}`;
}

function byteLength(text: string): number {
	return Buffer.byteLength(text, "utf8");
}

/**
 * 脱敏全记录报告：摘要 + 跨越全部 segment 的记录行。生成即按预算从最新行往回取——
 * 不先展开全部历史；超限时明确标注省略了多少条更早记录，绝不切半行或半字符，也绝不
 * 只保留最旧的一段。
 */
export function buildResearchReport(snapshot: ResearchSnapshot, recordedRuns: readonly ResearchRun[]): string {
	const metricUnit = reportText(snapshot.metricUnit);
	const head = [
		`# 研究记录：${reportText(snapshot.name) || "autoresearch"}`,
		"",
		...(snapshot.goal ? [`目标：${reportText(snapshot.goal)}`, ""] : []),
		`- 指标：${reportText(snapshot.metricName)}（${metricUnit || "无单位"}，${snapshot.direction === "lower" ? "越低越好" : "越高越好"}）`,
		`- 当前段：${snapshot.segment}；基线：${reportMetric(snapshot.baseline)}；最佳：${reportMetric(snapshot.best)}`,
		`- 已完成：${snapshot.completedRuns} / ${snapshot.maxExperiments === null ? "无上限" : snapshot.maxExperiments}；总记录：${snapshot.totalRuns}`,
		"",
		"## 全部记录",
	].join("\n");
	const omission = (count: number) => `…（报告上限 1MiB：已省略更早的 ${count} 条记录）`;
	// 预算里预留省略行本身：数字位数只影响几个字节，向上取整留足。
	const markerBudget = byteLength(`${omission(recordedRuns.length)}\n`);
	const budget = MAX_RESEARCH_REPORT_BYTES - byteLength(`${head}\n\n`) - markerBudget;
	const newestFirst: string[] = [];
	let used = 0;
	for (let index = recordedRuns.length - 1; index >= 0; index--) {
		const line = reportRunLine(recordedRuns[index]!);
		const bytes = byteLength(`${line}\n`);
		if (used + bytes > budget) break;
		used += bytes;
		newestFirst.push(line);
	}
	const kept = newestFirst.reverse();
	let omitted = recordedRuns.length - kept.length;
	const assemble = (count: number, rows: readonly string[]): string =>
		rows.length === 0
			? count > 0
				? `${head}\n\n${omission(count)}\n`
				: `${head}\n\n（尚无已记录的运行）\n`
			: count > 0
				? `${head}\n\n${omission(count)}\n${rows.join("\n")}\n`
				: `${head}\n\n${rows.join("\n")}\n`;
	let report = assemble(omitted, kept);
	// 兜底防线：预算算错或数字位数多估时继续丢最旧的整行——绝不裁半行，也绝不越过上限。
	while (byteLength(report) > MAX_RESEARCH_REPORT_BYTES && kept.length > 0) {
		kept.shift();
		omitted++;
		report = assemble(omitted, kept);
	}
	return report;
}

/**
 * 一次 prompt 的研究流状态机：start/end 只更新 phase/pending、绝不添已记录轮次；
 * init/log 的权威 state 才改写历史并生成报告。三个原生工具的 end 都必须与本 prompt
 * 内观察到的 start 按 toolName+toolCallId 配对消费——无 start 的陈旧/幽灵帧、重放帧
 * 一律拒绝；每个 start 记内部递增序号，完成只允许晚于最后一次已接受完成的序号，
 * 所以较早发起的完成永远回退不了较新的权威状态（run end 也不能覆盖更晚落盘的 log）。
 * 每个真正成功的 init/log 都重新生成报告，完整历史修正不会被去重吞掉。
 */
export class ResearchTracker {
	#state: DerivedResearchState | undefined;
	#phase: ResearchSnapshot["phase"] = "ready";
	#activeRun: number | null = null;
	#latestMetric: number | null = null;
	#startSeq = 0;
	/** 最后一次被接受（真正生效）的完成的 start 序号。 */
	#latestSeq = 0;
	/** 已观察 start、尚未消费的三个原生工具调用（toolName:toolCallId → start 序号）。 */
	#pendingCalls = new Map<string, number>();

	get #settledPhase(): ResearchSnapshot["phase"] {
		const state = this.#state;
		if (state !== undefined && state.maxExperiments !== null && state.completedRuns >= state.maxExperiments) return "limit_reached";
		return "ready";
	}

	#snapshot(): ResearchSnapshot | undefined {
		const state = this.#state;
		if (state === undefined) return undefined;
		return {
			sessionId: state.sessionId,
			name: state.name,
			goal: state.goal,
			segment: state.segment,
			metricName: state.metricName,
			metricUnit: state.metricUnit,
			direction: state.direction,
			baseline: state.baseline,
			best: state.best,
			completedRuns: state.completedRuns,
			maxExperiments: state.maxExperiments,
			phase: this.#phase,
			activeRun: this.#activeRun,
			latestMetric: this.#latestMetric,
			recent: state.recent,
			totalRuns: state.totalRuns,
		};
	}

	#emit(withReport: boolean): ResearchUpdateEvent | undefined {
		const research = this.#snapshot();
		if (research === undefined) return undefined;
		const event: { type: "research_update"; atMs: number; research: ResearchSnapshot; report?: string } = {
			type: "research_update",
			atMs: Date.now(),
			research,
		};
		if (withReport) event.report = buildResearchReport(research, this.#state?.allRuns ?? []);
		return event;
	}

	/** 三个原生工具都记 start；只有 run_experiment 的 start 同时移动 phase。 */
	startTool(toolName: string, toolCallId: string): ResearchUpdateEvent | undefined {
		if (Object.hasOwn(NATIVE_RESEARCH_TOOLS, toolName)) this.#pendingCalls.set(`${toolName}:${toolCallId}`, ++this.#startSeq);
		if (toolName !== "run_experiment") return undefined;
		if (this.#state === undefined) return undefined;
		this.#phase = "running";
		this.#activeRun = null;
		return this.#emit(false);
	}

	endTool(toolName: string, toolCallId: string, result: unknown, isError: boolean): ResearchUpdateEvent | undefined {
		if (!Object.hasOwn(NATIVE_RESEARCH_TOOLS, toolName)) return undefined;
		const key = `${toolName}:${toolCallId}`;
		const seq = this.#pendingCalls.get(key);
		// 没有匹配 start 的 end 是陈旧/幽灵帧；配过对的 start 一次消费，重放即拒。
		if (seq === undefined) return undefined;
		this.#pendingCalls.delete(key);
		// 较早发起的完成不得晚于、也不得等于最后一次已生效的完成——否则就是在回退权威状态。
		if (seq <= this.#latestSeq) return undefined;
		if (toolName === "run_experiment") {
			if (isError) {
				// 已崩塌的基准就此结束：phase 落回 settled，不添历史、不带报告、
				// 更不采纳崩溃 run 自带的指标。
				if (this.#state === undefined) return undefined;
				this.#latestSeq = seq;
				this.#phase = this.#settledPhase;
				this.#activeRun = null;
				return this.#emit(false);
			}
			const run = parseRunEndDetails(result);
			if (run === undefined) return undefined;
			this.#latestSeq = seq;
			this.#phase = "awaiting_log";
			this.#activeRun = run.runNumber;
			if (run.parsedPrimary !== null) this.#latestMetric = run.parsedPrimary;
			return this.#emit(false);
		}
		if (toolName === "log_experiment") {
			// isError 的工具结果永远不权威；无 details / 畸形 state 同样不编造。
			if (isError) return undefined;
			const logged = parseLogDetails(result);
			if (logged === undefined) return undefined;
			this.#latestSeq = seq;
			this.#state = logged.state;
			this.#phase = this.#settledPhase;
			this.#activeRun = null;
			this.#latestMetric = logged.loggedMetric;
			return this.#emit(true);
		}
		// init_experiment：会话建立/重配/换段的权威快照。
		if (isError) return undefined;
		const state = parseInitDetails(result);
		if (state === undefined) return undefined;
		this.#latestSeq = seq;
		this.#state = state;
		this.#phase = this.#settledPhase;
		this.#activeRun = null;
		// 新近指标只记本 prompt 观察到的运行：续会话的历史 metric 不冒充最新。
		this.#latestMetric = null;
		return this.#emit(true);
	}
}
