import { createHash } from "node:crypto";
import type { ChatTarget } from "../contracts.ts";
import type { ChannelStore, ReferenceHistoryEntry, ReferenceHistorySnapshot } from "../store/index.ts";
import { canonicalReferenceUrl, extractReferenceCandidates, sanitizeReferenceEvidence, sanitizeReferenceText } from "./links.ts";
import type { MatchedReferenceHistory, ReferenceCandidate, ReferenceEvidence, ReferenceJudge, ThreadReference } from "./types.ts";

const MAX_ACTIVE_REQUESTS = 2;
const MAX_WAITING_BATCHES = 100;
const MAX_RECENT_MESSAGES = 4;
const MAX_RECENT_CODE_POINTS = 4000;
const MAX_THREAD_CODE_POINTS = 1000;
const MAX_TASK_CODE_POINTS = 3000;
const MAX_CANDIDATE_URL_CODE_POINTS = 2048;
const MAX_CANDIDATE_EXCERPT_CODE_POINTS = 1000;
const MAX_SOURCE_VERSIONS_PER_THREAD = 128;
const MAX_TASK_REQUIREMENTS_PER_THREAD = 128;
const MIN_WORK_OBJECT_PROBABILITY = 0.75;
const MAX_MATCHED_HISTORY_CODE_POINTS = 4000;
const THREAD_CONTEXT_LOAD_TIMEOUT_MS = 5_000;
const REPORT_URL_TOKEN = /https?:\/\/[^\s<>"'`]+/giu;
const REPORT_URL_MARKER = "[matched candidate URL omitted]";

interface RecentMessage {
  role: "user" | "assistant";
  sourceKey: string;
  taskId?: string;
  sourceTextFingerprint?: string;
  text: string;
}

interface SourceVersion {
  fingerprint: string;
  version: number;
}

interface TaskRequirements {
  initial: string;
  additions: string[];
}

interface ThreadState {
  description: string;
  name?: string;
  tasks: Map<string, TaskRequirements>;
  recent: RecentMessage[];
  versions: Map<string, SourceVersion>;
}
type EvidenceState = Pick<ThreadState, "description" | "name" | "tasks" | "recent">;

interface ReferenceThreadContext {
  description: string;
  name?: string;
}


interface ReferenceBatch {
  botUid: string;
  target: ChatTarget;
  targetKey: string;
  sourceKey: string;
  taskId?: string;
  version: number;
  evidenceState: EvidenceState;
  historySnapshot?: ReferenceHistorySnapshot;
  evidence: ReferenceEvidence;
}

interface ActiveBatch {
  batch: ReferenceBatch;
  controller: AbortController;
}

export class ThreadReferenceCoordinator {
  private readonly states = new Map<string, ThreadState>();
  private readonly waiting: ReferenceBatch[] = [];
  private readonly active = new Set<ActiveBatch>();
  private stopped = false;
  private nextVersion = 0;

  constructor(private readonly options: {
    store: Pick<ChannelStore, "getThreadReferences" | "addThreadReferences" | "isThreadReferencesDeleted" | "deleteThreadReferences">
      & Partial<Pick<ChannelStore, "captureReferenceHistorySnapshot" | "listReferenceHistory">>;
    botUid: () => string | undefined;
    judge?: ReferenceJudge;
    loadThreadContext?: (
      target: ChatTarget,
      taskId: string | undefined,
      signal: AbortSignal,
    ) => Promise<ReferenceThreadContext | null>;
  }) {}

  observeUser(target: ChatTarget, messageId: string, text: string, taskId?: string, sourceText?: string): void {
    this.observe("user", target, messageId, text, taskId, sourceText);
  }

  observeAssistant(target: ChatTarget, taskId: string, messageId: string, text: string): void {
    this.observe("assistant", target, messageId, text, taskId);
  }

  setThreadContext(target: ChatTarget, description: string, name?: string): void {
    const botUid = this.options.botUid();
    const identity = this.identity(target, botUid);
    if (!botUid || !identity) return;
    try {
      if (this.options.store.isThreadReferencesDeleted(botUid, target)) return;
    } catch {
      return;
    }
    const state = this.stateFor(identity.key);
    if (!state) return;
    state.description = truncateEvidence(description, MAX_THREAD_CODE_POINTS);
    if (name === undefined) delete state.name;
    else state.name = truncateEvidence(name, MAX_THREAD_CODE_POINTS);
  }

  resetContext(target: ChatTarget): void {
    const identity = this.identity(target);
    if (!identity) return;
    const state = this.states.get(identity.key);
    if (!state) return;
    state.tasks.clear();
    state.recent = [];
  }

  deleteThread(target: ChatTarget): void {
    if (target.channelType !== 5 || target.channelId.length === 0) return;
    const botUid = this.options.botUid();
    if (!botUid) return;

    const targetKey = this.targetKey(botUid, target);
    for (let index = this.waiting.length - 1; index >= 0; index -= 1) {
      if (this.waiting[index]!.targetKey === targetKey) this.waiting.splice(index, 1);
    }
    for (const active of this.active) {
      if (active.batch.targetKey === targetKey) active.controller.abort();
    }
    this.states.delete(targetKey);
    this.options.store.deleteThreadReferences(botUid, target);
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.waiting.length = 0;
    for (const active of this.active) active.controller.abort();
    this.states.clear();
  }

  private observe(role: "user" | "assistant", target: ChatTarget, messageId: string, text: string, taskId?: string, sourceText?: string): void {
    if (this.stopped || !this.options.judge || target.channelType !== 5 || target.channelId.length === 0 || messageId.length === 0) return;
    const botUid = this.options.botUid();
    if (!botUid) return;
    try {
      if (this.options.store.isThreadReferencesDeleted(botUid, target)) return;
    } catch {
      return;
    }

    const identity = this.identity(target, botUid);
    if (!identity) return;
    const state = this.stateFor(identity.key);
    if (!state) return;

    const sourceKey = role === "assistant"
      ? JSON.stringify([role, taskId ?? null, messageId])
      : JSON.stringify([role, messageId]);
    const fingerprint = createHash("sha256").update(text).digest("hex");
    const previous = state.versions.get(sourceKey);
    if (previous?.fingerprint === fingerprint) return;

    const version = ++this.nextVersion;
    state.versions.delete(sourceKey);
    state.versions.set(sourceKey, { fingerprint, version });
    while (state.versions.size > MAX_SOURCE_VERSIONS_PER_THREAD) {
      const oldest = state.versions.keys().next().value;
      if (oldest === undefined) break;
      state.versions.delete(oldest);
    }
    this.invalidateOlderVersions(identity.key, sourceKey);

    if (role === "user" && text.trimStart().startsWith("/")) return;
    this.rememberRecent(state, role, sourceKey, text, taskId, sourceText);
    if (role === "user" && taskId) this.rememberRequirement(state, taskId, sourceText ?? text);

    if (!/https?:\/\//iu.test(text)) return;
    let evidence: ReferenceEvidence;
    try {
      const saved = this.options.store.getThreadReferences(botUid, target);
      const known = new Set([...saved.mr, ...saved.pr]);
      const candidates = extractReferenceCandidates(text, known);
      if (candidates.length === 0) return;
      evidence = this.buildEvidence(state, taskId, candidates);
    } catch {
      return;
    }

    if (this.waiting.length >= MAX_WAITING_BATCHES) return;
    let historySnapshot: ReferenceHistorySnapshot | undefined;
    try {
      historySnapshot = this.options.store.captureReferenceHistorySnapshot?.(botUid, target);
    } catch {
      // The coordinator remains useful without persisted history if a snapshot cannot be captured.
    }
    const taskSnapshot = taskId === undefined ? undefined : state.tasks.get(taskId);
    const evidenceTasks = new Map<string, TaskRequirements>();
    if (taskId !== undefined && taskSnapshot !== undefined) {
      evidenceTasks.set(taskId, { initial: taskSnapshot.initial, additions: [...taskSnapshot.additions] });
    }
    const evidenceState: EvidenceState = {
      description: state.description,
      ...(state.name === undefined ? {} : { name: state.name }),
      tasks: evidenceTasks,
      recent: state.recent.map((message) => ({ ...message })),
    };
    const batch: ReferenceBatch = {
      botUid,
      target: { channelType: target.channelType, channelId: target.channelId },
      targetKey: identity.key,
      sourceKey,
      ...(taskId === undefined ? {} : { taskId }),
      version,
      evidenceState,
      ...(historySnapshot === undefined ? {} : { historySnapshot }),
      evidence,
    };
    this.waiting.push(batch);
    this.schedulePump();
  }

  private rememberRecent(
    state: ThreadState,
    role: "user" | "assistant",
    sourceKey: string,
    text: string,
    taskId?: string,
    sourceText?: string,
  ): void {
    const existing = state.recent.findIndex((message) => message.sourceKey === sourceKey);
    if (existing >= 0) state.recent.splice(existing, 1);
    const sourceTextFingerprint = sourceText === undefined
      ? undefined
      : createHash("sha256").update(sourceText).digest("hex");
    state.recent.push({
      role,
      sourceKey,
      ...(taskId === undefined ? {} : { taskId }),
      ...(sourceTextFingerprint === undefined ? {} : { sourceTextFingerprint }),
      text: truncateEvidence(text, MAX_RECENT_CODE_POINTS),
    });
    if (state.recent.length > MAX_RECENT_MESSAGES) state.recent.splice(0, state.recent.length - MAX_RECENT_MESSAGES);
  }

  private rememberRequirement(state: ThreadState, taskId: string, text: string): void {
    if (text.length === 0) return;
    let task = state.tasks.get(taskId);
    if (!task) {
      const initialPrefix = "Initial accepted requirement:\n";
      task = { initial: truncateEvidence(text, MAX_TASK_CODE_POINTS - codePointLength(initialPrefix)), additions: [] };
      state.tasks.set(taskId, task);
      while (state.tasks.size > MAX_TASK_REQUIREMENTS_PER_THREAD) {
        const oldest = state.tasks.keys().next().value;
        if (oldest === undefined) break;
        state.tasks.delete(oldest);
      }
      return;
    }

    state.tasks.delete(taskId);
    state.tasks.set(taskId, task);
    const prefix = task.initial.length === 0 ? "Initial accepted requirement:\n" : "\nAdditional accepted requirement:\n";
    const current = this.renderTask(task);
    const remaining = MAX_TASK_CODE_POINTS - codePointLength(current) - codePointLength(prefix);
    if (remaining <= 0) return;
    const additional = truncateEvidence(text, remaining);
    if (additional.length > 0) task.additions.push(additional);
  }

  private buildEvidence(
    state: EvidenceState,
    taskId: string | undefined,
    candidates: readonly ReferenceCandidate[],
    historyEntries: readonly ReferenceHistoryEntry[] = [],
    threadContext?: ReferenceThreadContext | null,
  ): ReferenceEvidence {
    const threadParts: string[] = [];
    const threadName = threadContext?.name ?? state.name;
    const threadDescription = threadContext === undefined
      ? state.description
      : threadContext?.description ?? "";
    if (threadName) threadParts.push(`Thread name: ${threadName}`);
    if (threadDescription) threadParts.push(`Thread description: ${threadDescription}`);
    if (threadContext === null) threadParts.push("Current thread background snapshot is unavailable.");
    else if (threadContext !== undefined && threadDescription.length === 0) {
      threadParts.push("Current thread background snapshot has no content.");
    }

    const recentByContent = new Map<string, ReferenceEvidence["recent"][number]>();
    const quotedSourceFingerprints = new Set<string>();
    for (const recentMessage of state.recent) {
      if (
        recentMessage.role === "user"
        && recentMessage.taskId !== undefined
        && recentMessage.sourceTextFingerprint !== undefined
      ) {
        quotedSourceFingerprints.add(JSON.stringify([recentMessage.taskId, recentMessage.sourceTextFingerprint]));
      }
    }
    for (const message of historyEntries.slice(-MAX_RECENT_MESSAGES)) {
      if (
        message.role === "user"
        && message.taskId !== null
        && quotedSourceFingerprints.has(JSON.stringify([
          message.taskId,
          createHash("sha256").update(message.text).digest("hex"),
        ]))
      ) {
        continue;
      }
      recentByContent.set(JSON.stringify([message.role, message.text]), { role: message.role, text: message.text });
    }
    for (const { role, text } of state.recent.slice(-MAX_RECENT_MESSAGES)) {
      recentByContent.set(JSON.stringify([role, text]), { role, text });
    }
    const recent = [...recentByContent.values()].slice(-MAX_RECENT_MESSAGES);
    const task = taskId === undefined ? undefined : state.tasks.get(taskId);
    const matchedHistory = this.buildMatchedHistory(historyEntries, candidates, taskId);
    const evidence = sanitizeReferenceEvidence({
      thread: threadParts.length > 0 ? threadParts.join("\n") : "Thread name and current background are unavailable.",
      task: task ? this.renderTask(task) : "Accepted task requirement is unavailable.",
      recent,
      matchedHistory,
      candidates,
    });

    let recentRemaining = MAX_RECENT_CODE_POINTS;
    const boundedRecent: Array<ReferenceEvidence["recent"][number]> = [];
    for (const message of [...evidence.recent].reverse()) {
      if (recentRemaining <= 0) break;
      const text = truncateEvidence(message.text, recentRemaining);
      if (text.length === 0) continue;
      boundedRecent.push({ role: message.role, text });
      recentRemaining -= codePointLength(text);
    }
    boundedRecent.reverse();

    return {
      thread: truncateEvidence(evidence.thread, MAX_THREAD_CODE_POINTS),
      task: truncateEvidence(evidence.task, MAX_TASK_CODE_POINTS),
      recent: boundedRecent,
      matchedHistory: this.boundMatchedHistory(evidence.matchedHistory ?? []),
      candidates: evidence.candidates.filter((candidate) => codePointLength(candidate.url) <= MAX_CANDIDATE_URL_CODE_POINTS)
        .map((candidate) => ({ ...candidate, excerpt: truncateEvidence(candidate.excerpt, MAX_CANDIDATE_EXCERPT_CODE_POINTS) })),
    };
  }

  private buildMatchedHistory(
    entries: readonly ReferenceHistoryEntry[],
    candidates: readonly ReferenceCandidate[],
    currentTaskId: string | undefined,
  ): MatchedReferenceHistory[] {
    const requirementsByTask = new Map<string, string[]>();
    for (const entry of entries) {
      if (entry.role !== "user" || !entry.taskId || entry.taskId === currentTaskId) continue;
      const requirements = requirementsByTask.get(entry.taskId) ?? [];
      requirements.push(entry.text);
      requirementsByTask.set(entry.taskId, requirements);
    }

    const reportsByCandidate = new Map<string, { taskId: string; excerpt: string }>();
    for (const entry of entries) {
      if (entry.role !== "assistant" || !entry.taskId || entry.taskId === currentTaskId) continue;
      for (const candidate of candidates) {
        const excerpt = referenceReportExcerpt(entry.text, candidate.url);
        if (excerpt !== null) reportsByCandidate.set(candidate.id, { taskId: entry.taskId, excerpt });
      }
    }

    const matched: MatchedReferenceHistory[] = [];
    for (const candidate of candidates) {
      const report = reportsByCandidate.get(candidate.id);
      if (!report) continue;
      const requirements = requirementsByTask.get(report.taskId) ?? [];
      const taskRequirement = requirements.length > 0
        ? `Historical task user requirements:\n${requirements.map((text, index) =>
          `${index === 0 ? "Initial" : "Additional"} accepted user requirement:\n${text}`,
        ).join("\n")}`
        : "Historical task user requirement is unavailable.";
      matched.push({
        candidateId: candidate.id,
        url: candidate.url,
        taskRequirement,
        reportExcerpt: report.excerpt,
      });
    }
    return matched;
  }

  private boundMatchedHistory(entries: readonly MatchedReferenceHistory[]): MatchedReferenceHistory[] {
    let remaining = MAX_MATCHED_HISTORY_CODE_POINTS;
    const bounded: MatchedReferenceHistory[] = [];
    for (let index = 0; index < entries.length && remaining > 0; index += 1) {
      const entry = entries[index]!;
      const share = Math.max(1, Math.floor(remaining / (entries.length - index)));
      const reportExcerpt = truncateReportExcerpt(
        entry.reportExcerpt,
        Math.min(MAX_CANDIDATE_EXCERPT_CODE_POINTS, Math.max(1, Math.floor(share / 2))),
      );
      const taskRequirement = truncateEvidence(
        entry.taskRequirement,
        Math.min(MAX_TASK_CODE_POINTS, Math.max(0, share - codePointLength(reportExcerpt))),
      );
      bounded.push({ ...entry, taskRequirement, reportExcerpt });
      remaining -= codePointLength(taskRequirement) + codePointLength(reportExcerpt);
    }
    return bounded;
  }



  private renderTask(task: TaskRequirements): string {
    let output = task.initial.length > 0 ? `Initial accepted requirement:\n${task.initial}` : "";
    for (const addition of task.additions) {
      const section = `${output.length > 0 ? "\n" : ""}Additional accepted requirement:\n${addition}`;
      const remaining = MAX_TASK_CODE_POINTS - codePointLength(output);
      if (remaining <= 0) break;
      output += takeCodePoints(section, remaining);
    }
    return takeCodePoints(output, MAX_TASK_CODE_POINTS);
  }

  private identity(target: ChatTarget, botUid = this.options.botUid()): { key: string } | null {
    if (!botUid || target.channelType !== 5 || target.channelId.length === 0) return null;
    return { key: this.targetKey(botUid, target) };
  }

  private targetKey(botUid: string, target: ChatTarget): string {
    return JSON.stringify([botUid, target.channelType, target.channelId]);
  }

  private stateFor(key: string): ThreadState | null {
    if (this.stopped || !this.options.judge) return null;
    let state = this.states.get(key);
    if (!state) {
      state = { description: "", tasks: new Map(), recent: [], versions: new Map() };
      this.states.set(key, state);
    }
    return state;
  }

  private invalidateOlderVersions(targetKey: string, sourceKey: string): void {
    for (let index = this.waiting.length - 1; index >= 0; index -= 1) {
      const batch = this.waiting[index]!;
      if (batch.targetKey === targetKey && batch.sourceKey === sourceKey) this.waiting.splice(index, 1);
    }
    for (const active of this.active) {
      if (active.batch.targetKey === targetKey && active.batch.sourceKey === sourceKey) active.controller.abort();
    }
  }

  private schedulePump(): void {
    if (this.stopped) return;
    this.pump();
  }

  private pump(): void {
    while (!this.stopped && this.active.size < MAX_ACTIVE_REQUESTS && this.waiting.length > 0) {
      const batch = this.waiting.shift()!;
      if (!this.isCurrent(batch)) continue;
      try {
        if (this.options.store.isThreadReferencesDeleted(batch.botUid, batch.target)) continue;
        const saved = this.options.store.getThreadReferences(batch.botUid, batch.target);
        const known = new Set([...saved.mr, ...saved.pr]);
        const candidates = batch.evidence.candidates.filter((candidate) => !known.has(candidate.url));
        if (candidates.length === 0) continue;
        const active: ActiveBatch = {
          batch: { ...batch, evidence: { ...batch.evidence, candidates } },
          controller: new AbortController(),
        };
        this.active.add(active);
        queueMicrotask(() => { void this.run(active); });
      } catch {
        continue;
      }
    }
  }

  private async run(active: ActiveBatch): Promise<void> {
    try {
      const batch = active.batch;
      if (this.stopped || active.controller.signal.aborted || !this.isCurrent(batch)) return;
      const judge = this.options.judge;
      if (!judge) return;

      let threadContext: ReferenceThreadContext | null | undefined;
      if (this.options.loadThreadContext) {
        threadContext = null;
        const timeout = new AbortController();
        const timer = setTimeout(() => {
          timeout.abort(new DOMException("Thread background load deadline exceeded", "TimeoutError"));
        }, THREAD_CONTEXT_LOAD_TIMEOUT_MS);
        const signal = AbortSignal.any([active.controller.signal, timeout.signal]);
        try {
          threadContext = await awaitSignal(
            Promise.resolve(this.options.loadThreadContext(batch.target, batch.taskId, signal)),
            signal,
          );
          if (threadContext) {
            this.setThreadContext(batch.target, threadContext.description, threadContext.name);
          }
        } catch {
          // Background context is best-effort; it must never hold a chat event or service shutdown.
        } finally {
          clearTimeout(timer);
        }
      }
      if (this.stopped || active.controller.signal.aborted || !this.isCurrent(batch)) return;

      let historyEntries: ReferenceHistoryEntry[] = [];
      if (batch.historySnapshot !== undefined) {
        try {
          historyEntries = this.options.store.listReferenceHistory?.(
            batch.botUid,
            batch.target,
            batch.evidence.candidates.map((candidate) => candidate.url),
            batch.historySnapshot,
          ) ?? [];
        } catch {
          // Missing or unreadable persisted context is evidence absence, not a chat failure.
        }
      }
      const evidence = this.buildEvidence(
        batch.evidenceState,
        batch.taskId,
        batch.evidence.candidates,
        historyEntries,
        threadContext,
      );
      const judgments = await judge(evidence, active.controller.signal);
      if (this.stopped || active.controller.signal.aborted || !this.isCurrent(batch)) return;
      if (this.options.store.isThreadReferencesDeleted(batch.botUid, batch.target)) return;

      const valid = this.validatedReferences(batch.evidence.candidates, judgments);
      if (valid.length > 0) this.options.store.addThreadReferences(batch.botUid, batch.target, valid);
    } catch {
      // External decision and best-effort local binding must never fail the chat event path.
    } finally {
      this.active.delete(active);
      this.schedulePump();
    }
  }

  private isCurrent(batch: ReferenceBatch): boolean {
    const state = this.states.get(batch.targetKey);
    return !this.stopped && state?.versions.get(batch.sourceKey)?.version === batch.version;
  }

  private validatedReferences(candidates: readonly ReferenceCandidate[], answerValue: unknown): ThreadReference[] {
    if (!Array.isArray(answerValue)) return [];
    const candidatesById = new Map(candidates.map((candidate) => [candidate.id, candidate]));
    const answersById = new Map<string, Record<string, unknown>[]>();
    for (const answer of answerValue as unknown[]) {
      if (!answer || typeof answer !== "object" || Array.isArray(answer)) continue;
      const record = answer as Record<string, unknown>;
      if (typeof record.id !== "string" || !candidatesById.has(record.id)) continue;
      const answers = answersById.get(record.id) ?? [];
      answers.push(record);
      answersById.set(record.id, answers);
    }

    const references: ThreadReference[] = [];
    for (const [id, answers] of answersById) {
      if (answers.length !== 1) continue;
      const answer = answers[0]!;
      if (
        answer.choice !== "work_object" ||
        typeof answer.probability !== "number" ||
        !Number.isFinite(answer.probability) ||
        answer.probability <= MIN_WORK_OBJECT_PROBABILITY ||
        answer.probability > 1
      ) {
        continue;
      }
      const candidate = candidatesById.get(id)!;
      references.push({ kind: candidate.kind, url: candidate.url });
    }
    return references;
  }
}

function awaitSignal<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason ?? new DOMException("Reference context load aborted", "AbortError"));
  const result = Promise.withResolvers<T>();
  const abort = (): void => {
    signal.removeEventListener("abort", abort);
    result.reject(signal.reason ?? new DOMException("Reference context load aborted", "AbortError"));
  };
  signal.addEventListener("abort", abort, { once: true });
  promise.then(
    (value) => {
      signal.removeEventListener("abort", abort);
      result.resolve(value);
    },
    (error: unknown) => {
      signal.removeEventListener("abort", abort);
      result.reject(error);
    },
  );
  return result.promise;
}

function referenceReportExcerpt(text: string, candidateUrl: string): string | null {
  const safeText = sanitizeReferenceText(text);
  REPORT_URL_TOKEN.lastIndex = 0;
  for (const match of safeText.matchAll(REPORT_URL_TOKEN)) {
    if (canonicalReferenceUrl(match[0]) !== candidateUrl) continue;
    const start = codePointLength(safeText.slice(0, match.index ?? 0));
    const end = start + codePointLength(match[0]);
    const markerLength = codePointLength(REPORT_URL_MARKER);
    const halfWindow = Math.floor((MAX_CANDIDATE_EXCERPT_CODE_POINTS - markerLength) / 2);
    const before = sliceCodePoints(safeText, Math.max(0, start - halfWindow), start);
    const after = sliceCodePoints(safeText, end, end + halfWindow);
    return `${before}${REPORT_URL_MARKER}${after}`;
  }
  return null;
}
function truncateReportExcerpt(value: string, maximum: number): string {
  if (maximum <= 0) return "";
  const sanitized = sanitizeReferenceText(value);
  const length = codePointLength(sanitized);
  if (length <= maximum) return sanitized;
  const markerIndex = sanitized.indexOf(REPORT_URL_MARKER);
  if (markerIndex < 0) return truncateEvidence(sanitized, maximum);

  const markerLength = codePointLength(REPORT_URL_MARKER);
  const truncation = "…[truncated]";
  const truncationLength = codePointLength(truncation);
  const available = maximum - markerLength - truncationLength * 2;
  if (available <= 0) return takeCodePoints(truncation, maximum);
  const markerStart = codePointLength(sanitized.slice(0, markerIndex));
  const markerEnd = markerStart + markerLength;
  const beforeBudget = Math.floor(available / 2);
  const afterBudget = available - beforeBudget;
  const beforeStart = Math.max(0, markerStart - beforeBudget);
  const afterEnd = Math.min(length, markerEnd + afterBudget);
  return `${beforeStart > 0 ? truncation : ""}${sliceCodePoints(sanitized, beforeStart, markerStart)}`
    + `${REPORT_URL_MARKER}${sliceCodePoints(sanitized, markerEnd, afterEnd)}`
    + `${afterEnd < length ? truncation : ""}`;
}


function sliceCodePoints(value: string, start: number, end: number): string {
  let offset = 0;
  let index = 0;
  let startOffset = value.length;
  let endOffset = value.length;
  for (const character of value) {
    if (index === start) startOffset = offset;
    if (index === end) {
      endOffset = offset;
      break;
    }
    offset += character.length;
    index += 1;
  }
  if (start >= index && start > 0 && startOffset === value.length) return "";
  return value.slice(startOffset, endOffset);
}

function codePointLength(value: string): number {
  let length = 0;
  for (const _character of value) length += 1;
  return length;
}

function takeCodePoints(value: string, maximum: number): string {
  if (maximum <= 0) return "";
  let offset = 0;
  let count = 0;
  for (const character of value) {
    if (count >= maximum) return value.slice(0, offset);
    offset += character.length;
    count += 1;
  }
  return value;
}

function truncateEvidence(value: string, maximum: number): string {
  if (maximum <= 0) return "";
  const sanitized = sanitizeReferenceText(value);
  if (codePointLength(sanitized) <= maximum) return sanitized;
  const marker = "…[truncated]";
  const markerLength = codePointLength(marker);
  if (maximum <= markerLength) return takeCodePoints(marker, maximum);
  return `${takeCodePoints(sanitized, maximum - markerLength)}${marker}`;
}
