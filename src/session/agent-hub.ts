import { access, realpath, readdir } from "node:fs/promises";
import { basename, dirname, isAbsolute, relative, resolve, sep } from "node:path";
import type {
  ChatAgentsSnapshot,
  ChatTarget,
  OmpSubagentMessages,
  OmpSubagentSnapshot,
  OmpWorker,
} from "../contracts.ts";

type LiveAgentHubWorker = Pick<OmpWorker, "sessionFile" | "sessionId" | "getSubagents" | "getSubagentMessages">;

interface SessionTranscript {
  entries: unknown[];
  messages: unknown[];
  sessionInit: Record<string, unknown> | undefined;
  sessionId: string | undefined;
  hasConversation: boolean;
}

interface LiveAgent {
  snapshot: OmpSubagentSnapshot;
  transcript: OmpSubagentMessages;
}

const MAIN_AGENT_ID = "Main";

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function errorCode(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error && typeof error.code === "string"
    ? error.code
    : undefined;
}

function sessionEntry(line: string, sessionFile: string): unknown {
  try {
    return JSON.parse(line) as unknown;
  } catch {
    throw new Error(`OMP transcript contains an invalid JSONL record: ${sessionFile}`);
  }
}

async function readSessionTranscript(sessionFile: string, signal: AbortSignal): Promise<SessionTranscript> {
  const entries: unknown[] = [];
  const messages: unknown[] = [];
  let sessionInit: Record<string, unknown> | undefined;
  let sessionId: string | undefined;
  let hasConversation = false;
  let pending = "";
  const decoder = new TextDecoder();

  for await (const chunk of Bun.file(sessionFile).stream()) {
    signal.throwIfAborted();
    pending += decoder.decode(chunk, { stream: true });
    let newline = pending.indexOf("\n");
    while (newline !== -1) {
      const line = pending.slice(0, newline).replace(/\r$/u, "");
      pending = pending.slice(newline + 1);
      if (line.trim().length > 0) {
        const entry = sessionEntry(line, sessionFile);
        entries.push(entry);
        const record = recordOf(entry);
        if (record?.type === "session" && typeof record.id === "string") sessionId ??= record.id;
        if (record?.type === "session_init") sessionInit ??= record;
        if (record?.type === "message") {
          if (!("message" in record)) throw new Error(`OMP message entry has no message: ${sessionFile}`);
          messages.push(record.message);
          hasConversation = true;
        } else if (record?.type === "custom_message") {
          hasConversation = true;
        }
      }
      newline = pending.indexOf("\n");
    }
  }

  pending += decoder.decode();
  if (pending.trim().length > 0) {
    const entry = sessionEntry(pending.replace(/\r$/u, ""), sessionFile);
    entries.push(entry);
    const record = recordOf(entry);
    if (record?.type === "session_init") sessionInit ??= record;
    if (record?.type === "session" && typeof record.id === "string") sessionId ??= record.id;
    if (record?.type === "message") {
      if (!("message" in record)) throw new Error(`OMP message entry has no message: ${sessionFile}`);
      messages.push(record.message);
      hasConversation = true;
    } else if (record?.type === "custom_message") {
      hasConversation = true;
    }
  }

  return { entries, messages, sessionInit, sessionId, hasConversation };
}

function inArtifactsRoot(sessionFile: string, artifactsRoot: string): boolean {
  const relativePath = relative(resolve(artifactsRoot), resolve(sessionFile));
  return relativePath.length > 0
    && relativePath !== ".."
    && !relativePath.startsWith(`..${sep}`)
    && !isAbsolute(relativePath);
}

function descriptionFromSessionInit(sessionInit: Record<string, unknown> | undefined): string | null {
  return typeof sessionInit?.task === "string" ? sessionInit.task : null;
}

function agentFromSessionInit(sessionInit: Record<string, unknown> | undefined, fallback: string): string {
  return typeof sessionInit?.agent === "string" ? sessionInit.agent : fallback;
}

function snapshotFromTranscript(
  id: string,
  parentId: string | null,
  transcript: SessionTranscript,
  status: string,
  agentFallback: string,
): ChatAgentsSnapshot["agents"][number] {
  return {
    id,
    parentId,
    agent: agentFromSessionInit(transcript.sessionInit, agentFallback),
    status,
    description: descriptionFromSessionInit(transcript.sessionInit),
    activity: transcript.entries,
    messages: transcript.messages,
  };
}

function snapshotFromLiveAgent(
  live: LiveAgent,
  parentId: string | null,
): ChatAgentsSnapshot["agents"][number] {
  const { snapshot, transcript } = live;
  const activity = [...transcript.entries];
  if (snapshot.progress !== undefined) {
    activity.push({ type: "subagent_progress", progress: snapshot.progress });
  }
  return {
    id: snapshot.id,
    parentId,
    agent: snapshot.agent,
    status: snapshot.status,
    description: snapshot.description ?? snapshot.task ?? snapshot.assignment ?? null,
    activity,
    messages: [...transcript.messages],
  };
}

function advisorSlug(filename: string): string | undefined {
  if (filename === "__advisor.jsonl") return "";
  if (filename.startsWith("__advisor.") && filename.endsWith(".jsonl")) {
    return filename.slice("__advisor.".length, -".jsonl".length);
  }
  return undefined;
}

async function hasTombstone(sessionFile: string): Promise<boolean> {
  try {
    await access(`${sessionFile}.tombstone`);
    return true;
  } catch (error) {
    if (errorCode(error) === "ENOENT") return false;
    throw error;
  }
}

async function readArtifacts(
  directory: string,
  parentId: string | null,
  liveBySessionFile: Map<string, LiveAgent>,
  signal: AbortSignal,
  agents: ChatAgentsSnapshot["agents"],
): Promise<void> {
  signal.throwIfAborted();
  let entries: Array<{ name: string; isFile(): boolean; isDirectory(): boolean }>;
  try {
    entries = await readdir(directory, { withFileTypes: true, encoding: "utf8" });
  } catch (error) {
    if (errorCode(error) === "ENOENT" || errorCode(error) === "ENOTDIR") return;
    throw error;
  }

  const childDirectories = new Set<string>();
  for (const entry of entries) if (entry.isDirectory()) childDirectories.add(entry.name);

  for (const entry of entries) {
    signal.throwIfAborted();
    if (!entry.isFile() || !entry.name.endsWith(".jsonl") || entry.name.includes(".bak")) continue;
    const sessionFile = resolve(directory, entry.name);
    const slug = advisorSlug(entry.name);
    if (slug !== undefined) {
      const transcript = await readSessionTranscript(sessionFile, signal);
      const owner = parentId ?? MAIN_AGENT_ID;
      const id = `${owner}/advisor${slug.length === 0 ? "" : `:${slug}`}`;
      agents.push(snapshotFromTranscript(id, parentId, transcript, "parked", "advisor"));
      continue;
    }

    const id = entry.name.slice(0, -".jsonl".length);
    const live = liveBySessionFile.get(sessionFile);
    if (live !== undefined) {
      if (live.snapshot.id !== id) throw new Error("OMP live subagent identity does not match its artifact path");
      agents.push(snapshotFromLiveAgent(live, parentId));
      liveBySessionFile.delete(sessionFile);
    } else {
      const transcript = await readSessionTranscript(sessionFile, signal);
      const aborted = await hasTombstone(sessionFile);
      if (aborted || transcript.sessionInit !== undefined || transcript.hasConversation) {
        agents.push(snapshotFromTranscript(id, parentId, transcript, aborted ? "aborted" : "parked", id));
      }
    }

    if (childDirectories.has(id)) {
      await readArtifacts(resolve(directory, id), id, liveBySessionFile, signal, agents);
    }
  }
}

function parentIdForSessionFile(sessionFile: string, artifactsRoot: string): string | null {
  const parentDirectory = resolve(dirname(sessionFile));
  return parentDirectory === resolve(artifactsRoot) ? null : basename(parentDirectory);
}

export async function readChatAgentsSnapshot(options: {
  target: ChatTarget;
  sessionId: string;
  sessionFile: string;
  rootStatus: string;
  liveWorker?: LiveAgentHubWorker;
  signal: AbortSignal;
}): Promise<ChatAgentsSnapshot> {
  const { target, sessionId, sessionFile, rootStatus, liveWorker, signal } = options;
  signal.throwIfAborted();
  const rootSessionFile = await realpath(resolve(sessionFile));
  const artifactsRoot = rootSessionFile.endsWith(".jsonl") ? rootSessionFile.slice(0, -".jsonl".length) : "";
  const rootTranscript = await readSessionTranscript(rootSessionFile, signal);
  if (rootTranscript.sessionId !== sessionId) {
    throw new Error("Bound OMP session file does not match the chat session identity");
  }
  if (liveWorker !== undefined) {
    const liveRootSessionFile = await realpath(resolve(liveWorker.sessionFile));
    if (liveWorker.sessionId !== sessionId || liveRootSessionFile !== rootSessionFile) {
      throw new Error("Running OMP worker does not match the bound chat session");
    }
    if (liveWorker.getSubagents === undefined || liveWorker.getSubagentMessages === undefined) {
      throw new Error("Running OMP worker does not support Agent Hub reads");
    }
  }
  const liveBySessionFile = new Map<string, LiveAgent>();
  if (liveWorker !== undefined) {
    const subagents = await liveWorker.getSubagents!(signal);
    for (const snapshot of subagents) {
      signal.throwIfAborted();
      const transcript = await liveWorker.getSubagentMessages!(snapshot.id, signal);
      const transcriptSessionFile = await realpath(resolve(transcript.sessionFile));
      if (
        snapshot.sessionFile !== undefined
        && await realpath(resolve(snapshot.sessionFile)) !== transcriptSessionFile
      ) {
        throw new Error("OMP subagent roster and transcript refer to different session files");
      }
      if (!inArtifactsRoot(transcriptSessionFile, artifactsRoot)) continue;
      liveBySessionFile.set(transcriptSessionFile, { snapshot, transcript });
    }
  }

  const agents: ChatAgentsSnapshot["agents"] = [
    snapshotFromTranscript("main", null, rootTranscript, rootStatus, "main"),
  ];
  if (artifactsRoot.length > 0) {
    await readArtifacts(artifactsRoot, null, liveBySessionFile, signal, agents);
  }
  for (const [sessionFile, live] of liveBySessionFile) {
    if (!inArtifactsRoot(sessionFile, artifactsRoot)) {
      throw new Error("OMP live subagent transcript does not belong to the bound session");
    }
    agents.push(snapshotFromLiveAgent(live, parentIdForSessionFile(sessionFile, artifactsRoot)));
  }
  signal.throwIfAborted();
  return { version: 1, channelType: target.channelType, channelId: target.channelId, agents };
}
