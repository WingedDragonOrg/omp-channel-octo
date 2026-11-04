import { realpath, stat } from "node:fs/promises";
import type { OmpWorker, WorkerContextUsage } from "../contracts.ts";
import { loadValidatedSessionState } from "./project-session.ts";
import type { SessionContextSnapshotRecord } from "../store/store-records.ts";
import type { ChannelStore } from "../store/index.ts";
import type { ChatTarget } from "../contracts.ts";

export interface SessionContextBinding extends ChatTarget {
  botUid: string;
  sessionId: string;
  sessionFile: string | null;
  cwd: string;
}

export type SessionContextSource = "live" | "persisted" | "unavailable";

export interface SessionContextStatus {
  source: SessionContextSource;
  stale: boolean;
  usedTokens: number | null;
  contextWindow: number | null;
  percent: number | null;
  model: string | null;
  observedAt: string | null;
  note: string;
}

export type SessionContextStore = Pick<ChannelStore, "getSessionContextSnapshot" | "saveSessionContextSnapshot">;
export type LiveSessionContextWorker = Pick<OmpWorker, "sessionId" | "sessionFile" | "contextUsage">;

interface ValidatedBinding {
  sessionId: string;
  sessionFile: string;
  cwd: string;
}

interface SessionFileFingerprint {
  size: number;
  mtimeMs: number;
}

const UNKNOWN: Omit<SessionContextStatus, "note"> = {
  source: "unavailable",
  stale: false,
  usedTokens: null,
  contextWindow: null,
  percent: null,
  model: null,
  observedAt: null,
};

function unavailable(note: string): SessionContextStatus {
  return { ...UNKNOWN, note };
}


async function validateBinding(binding: SessionContextBinding): Promise<ValidatedBinding | null> {
  if (
    binding.botUid.length === 0
    || binding.sessionId.length === 0
    || binding.sessionFile === null
    || binding.sessionFile.length === 0
    || binding.cwd.length === 0
  ) {
    return null;
  }
  const loaded = await loadValidatedSessionState(binding.sessionFile, binding.sessionId);
  const canonicalFile = await realpath(binding.sessionFile);
  const canonicalCwd = await realpath(binding.cwd);
  if (loaded.state.sessionFile !== canonicalFile || loaded.state.cwd !== canonicalCwd) return null;
  return loaded.state;
}

async function fingerprint(path: string): Promise<SessionFileFingerprint> {
  const details = await stat(path);
  if (!details.isFile()) throw new Error("bound OMP session is not a regular file");
  return { size: details.size, mtimeMs: details.mtimeMs };
}

function snapshotMatchesBinding(
  snapshot: SessionContextSnapshotRecord,
  binding: SessionContextBinding,
  validated: ValidatedBinding,
): boolean {
  return snapshot.botUid === binding.botUid
    && snapshot.channelType === binding.channelType
    && snapshot.channelId === binding.channelId
    && snapshot.sessionId === validated.sessionId
    && snapshot.sessionFile === validated.sessionFile
    && snapshot.cwd === validated.cwd;
}

function persistedStatus(
  snapshot: SessionContextSnapshotRecord,
  fileChanged: boolean,
): SessionContextStatus {
  const hasMeasurements = snapshot.usedTokens !== null || snapshot.contextWindow !== null || snapshot.percent !== null;
  const detail = fileChanged
    ? "会话文件在采样后已变化；以下为历史记录，非当前值。"
    : hasMeasurements
      ? "以下为最后一次记录的 OMP 原生上下文估算，非实时值。"
      : "OMP 当时未提供可用上下文用量；以下模型信息仅为历史记录，非实时值。";
  const calculatedPercent = snapshot.percent === null && snapshot.usedTokens !== null && snapshot.contextWindow !== null
    ? snapshot.usedTokens / snapshot.contextWindow * 100
    : null;
  return {
    source: "persisted",
    stale: fileChanged,
    usedTokens: snapshot.usedTokens,
    contextWindow: snapshot.contextWindow,
    percent: snapshot.percent ?? calculatedPercent,
    model: snapshot.model,
    observedAt: snapshot.observedAt,
    note: `最后记录于 ${snapshot.observedAt}；${calculatedPercent === null ? "" : "比例由 tokens/contextWindow 计算；"}${detail}`,
  };
}

async function readPersisted(
  binding: SessionContextBinding,
  store: SessionContextStore,
): Promise<SessionContextStatus> {
  const snapshot = store.getSessionContextSnapshot(binding.botUid, binding);
  if (!snapshot) return unavailable("没有与当前绑定会话匹配的上下文快照。");
  let validated: ValidatedBinding;
  let file: SessionFileFingerprint;
  try {
    const state = await validateBinding(binding);
    if (!state) return unavailable("当前绑定缺少有效的 session ID、session 文件或工作目录。");
    validated = state;
    if (!snapshotMatchesBinding(snapshot, binding, validated)) {
      return unavailable("保存的上下文快照与当前绑定的 session 身份不符。");
    }
    file = await fingerprint(validated.sessionFile);
  } catch {
    return unavailable("当前绑定的 OMP session 文件不存在或身份校验失败。");
  }
  return persistedStatus(snapshot, file.size !== snapshot.fileSize || file.mtimeMs !== snapshot.fileMtimeMs);
}


/** Reads one non-prompting live OMP estimate or the last identity-matched durable snapshot. */
export async function readSessionContext(
  binding: SessionContextBinding | null,
  live: LiveSessionContextWorker | undefined,
  signal: AbortSignal,
  store: SessionContextStore,
): Promise<SessionContextStatus> {
  signal.throwIfAborted();
  if (binding === null) return unavailable("当前聊天尚未绑定 OMP session。");
  if (!live) return readPersisted(binding, store);

  let validated: ValidatedBinding;
  let before: SessionFileFingerprint;
  try {
    const state = await validateBinding(binding);
    if (!state) return unavailable("当前绑定缺少有效的 session ID、session 文件或工作目录。");
    validated = state;
    if (
      live.sessionId !== validated.sessionId
      || await realpath(live.sessionFile) !== validated.sessionFile
    ) {
      return unavailable("运行中的 worker 与当前绑定的 session 身份不符。");
    }
    before = await fingerprint(validated.sessionFile);
  } catch {
    return unavailable("当前绑定的 OMP session 文件不存在或身份校验失败。");
  }
  signal.throwIfAborted();

  let usage: WorkerContextUsage;
  let observedAt: string;
  try {
    usage = await live.contextUsage(signal);
    observedAt = new Date().toISOString();
  } catch {
    signal.throwIfAborted();
    const persisted = await readPersisted(binding, store);
    if (persisted.source === "persisted") {
      return { ...persisted, stale: true, note: `${persisted.note}；实时读取失败，未启动新 worker。` };
    }
    return unavailable("无法读取 OMP 原生上下文快照，且没有可用的历史记录。");
  }

  if (
    usage.sessionId !== validated.sessionId
    || await realpath(usage.sessionFile).catch(() => "") !== validated.sessionFile
  ) {
    return unavailable("OMP 返回的 session 身份与当前绑定不符。");
  }

  const usedTokens = usage.tokens;
  const contextWindow = usage.contextWindow;
  const calculatedPercent = usage.percent === null && usedTokens !== null && contextWindow !== null
    ? usedTokens / contextWindow * 100
    : null;
  const percent = usage.percent ?? calculatedPercent;
  const model = usage.model;
  if (usedTokens !== null || contextWindow !== null || usage.percent !== null || model !== null) {
    store.saveSessionContextSnapshot({
      botUid: binding.botUid,
      channelType: binding.channelType,
      channelId: binding.channelId,
      sessionId: validated.sessionId,
      sessionFile: validated.sessionFile,
      cwd: validated.cwd,
      usedTokens,
      contextWindow,
      percent: usage.percent,
      model,
      observedAt,
      fileSize: before.size,
      fileMtimeMs: before.mtimeMs,
    });
  }

  const hasEstimate = usedTokens !== null || contextWindow !== null || usage.percent !== null;
  const note = !hasEstimate
    ? "OMP 已响应，但未提供可用的原生上下文估算。"
    : `OMP 原生上下文估算（非 provider 精确计量）${calculatedPercent === null ? "" : "；比例由 tokens/contextWindow 计算"}；采样于 ${observedAt}。`;
  return {
    source: "live",
    stale: false,
    usedTokens,
    contextWindow,
    percent,
    model,
    observedAt,
    note,
  };
}
