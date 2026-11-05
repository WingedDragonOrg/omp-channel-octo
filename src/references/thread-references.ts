import { createHash } from "node:crypto";
import type { ChatTarget } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import { extractReferenceCandidates, sanitizeReferenceEvidence, sanitizeReferenceText } from "./links.ts";
import type { ReferenceCandidate, ReferenceEvidence, ReferenceJudge, ThreadReference } from "./types.ts";

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

interface RecentMessage {
  role: "user" | "assistant";
  sourceKey: string;
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

interface ReferenceBatch {
  botUid: string;
  target: ChatTarget;
  targetKey: string;
  sourceKey: string;
  version: number;
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
    store: Pick<ChannelStore, "getThreadReferences" | "addThreadReferences" | "isThreadReferencesDeleted" | "deleteThreadReferences">;
    botUid: () => string | undefined;
    judge?: ReferenceJudge;
  }) {}

  observeUser(target: ChatTarget, messageId: string, text: string, taskId?: string): void {
    this.observe("user", target, messageId, text, taskId);
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

  private observe(role: "user" | "assistant", target: ChatTarget, messageId: string, text: string, taskId?: string): void {
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
    this.rememberRecent(state, role, sourceKey, text);
    if (role === "user" && taskId) this.rememberRequirement(state, taskId, text);

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

    const batch: ReferenceBatch = {
      botUid,
      target: { channelType: target.channelType, channelId: target.channelId },
      targetKey: identity.key,
      sourceKey,
      version,
      evidence,
    };
    if (this.waiting.length >= MAX_WAITING_BATCHES) return;
    this.waiting.push(batch);
    this.schedulePump();
  }

  private rememberRecent(state: ThreadState, role: "user" | "assistant", sourceKey: string, text: string): void {
    const existing = state.recent.findIndex((message) => message.sourceKey === sourceKey);
    if (existing >= 0) state.recent.splice(existing, 1);
    state.recent.push({ role, sourceKey, text: truncateEvidence(text, MAX_RECENT_CODE_POINTS) });
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

  private buildEvidence(state: ThreadState, taskId: string | undefined, candidates: readonly ReferenceCandidate[]): ReferenceEvidence {
    const threadParts: string[] = [];
    if (state.name) threadParts.push(`Thread name: ${state.name}`);
    if (state.description) threadParts.push(`Thread description: ${state.description}`);
    const recent = state.recent.slice(-MAX_RECENT_MESSAGES).map(({ role, text }) => ({ role, text }));
    const task = taskId === undefined ? undefined : state.tasks.get(taskId);
    const evidence = sanitizeReferenceEvidence({
      thread: threadParts.length > 0 ? threadParts.join("\n") : "Thread name and description are unavailable.",
      task: task ? this.renderTask(task) : "Accepted task requirement is unavailable.",
      recent,
      candidates,
    });

    let recentRemaining = MAX_RECENT_CODE_POINTS;
    const boundedRecent: Array<ReferenceEvidence["recent"][number]> = [];
    for (const message of evidence.recent) {
      if (recentRemaining <= 0) break;
      const text = truncateEvidence(message.text, recentRemaining);
      if (text.length === 0) continue;
      boundedRecent.push({ role: message.role, text });
      recentRemaining -= codePointLength(text);
    }
    return {
      thread: truncateEvidence(evidence.thread, MAX_THREAD_CODE_POINTS),
      task: truncateEvidence(evidence.task, MAX_TASK_CODE_POINTS),
      recent: boundedRecent,
      candidates: evidence.candidates.filter((candidate) => codePointLength(candidate.url) <= MAX_CANDIDATE_URL_CODE_POINTS)
        .map((candidate) => ({ ...candidate, excerpt: truncateEvidence(candidate.excerpt, MAX_CANDIDATE_EXCERPT_CODE_POINTS) })),
    };
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
      if (this.stopped || active.controller.signal.aborted || !this.isCurrent(active.batch)) return;
      const judge = this.options.judge;
      if (!judge) return;
      const judgments = await judge(active.batch.evidence, active.controller.signal);
      if (this.stopped || active.controller.signal.aborted || !this.isCurrent(active.batch)) return;
      if (this.options.store.isThreadReferencesDeleted(active.batch.botUid, active.batch.target)) return;

      const valid = this.validatedReferences(active.batch.evidence.candidates, judgments);
      if (valid.length > 0) this.options.store.addThreadReferences(active.batch.botUid, active.batch.target, valid);
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
