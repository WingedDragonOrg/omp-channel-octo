import { expect, test } from "bun:test";
import { appendFile, mkdir, mkdtemp, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type { ChatTarget, OmpWorker, WorkerOptions } from "../src/contracts.ts";
import { readSessionContext } from "../src/session/status-context.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { SessionContextSnapshotRecord } from "../src/store/store-records.ts";
import { FakeWorkerFactory } from "./fixtures/core-fakes.ts";

const contextSample = {
  tokens: 4_096,
  contextWindow: 8_192,
  percent: 50,
  model: "fixture/context",
};

interface ContextBinding extends ChatTarget {
  botUid: string;
  sessionId: string;
  sessionFile: string;
  cwd: string;
}

interface SessionFileFingerprint {
  size: number;
  mtimeMs: number;
}

function boundSession(
  botUid: string,
  target: ChatTarget,
  sessionId: string,
  sessionFile: string,
  cwd: string,
): ContextBinding {
  return { ...target, botUid, sessionId, sessionFile, cwd };
}

async function writeSession(sessionFile: string, sessionId: string, cwd: string): Promise<void> {
  await mkdir(dirname(sessionFile), { recursive: true });
  await writeFile(
    sessionFile,
    `${JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: "2026-11-04T09:00:00.000Z", cwd })}\n`,
    "utf8",
  );
}

function snapshot(
  binding: ContextBinding,
  file: SessionFileFingerprint,
  overrides: Partial<SessionContextSnapshotRecord> = {},
): SessionContextSnapshotRecord {
  return {
    ...binding,
    usedTokens: contextSample.tokens,
    contextWindow: contextSample.contextWindow,
    percent: contextSample.percent,
    model: contextSample.model,
    observedAt: "2026-11-04T09:00:00.000Z",
    fileSize: file.size,
    fileMtimeMs: file.mtimeMs,
    ...overrides,
  };
}

async function createLiveWorker(root: string, factory = new FakeWorkerFactory()) {
  const cwd = join(root, "workspace");
  const sessionDir = join(root, "sessions");
  await Promise.all([mkdir(cwd, { recursive: true }), mkdir(sessionDir, { recursive: true })]);
  factory.contextUsage = contextSample;
  const options: WorkerOptions = { cwd, sessionDir, onSpawn: () => {} };
  const worker = await factory.create(options);
  const state = await worker.refreshState();
  return {
    factory,
    worker,
    binding: boundSession("bot-context", { channelType: 1, channelId: "dm-context" }, state.sessionId, state.sessionFile, state.cwd),
  };
}

test("live context uses one native worker snapshot and persists its exact session identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-live-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  let worker: OmpWorker | undefined;
  try {
    const live = await createLiveWorker(root);
    worker = live.worker;

    const status = await readSessionContext(live.binding, live.worker, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "live",
      stale: false,
      usedTokens: 4_096,
      contextWindow: 8_192,
      percent: 50,
      model: "fixture/context",
      note: expect.stringContaining("估算"),
    });
    expect(Date.parse(status.observedAt!)).not.toBeNaN();
    expect(live.factory.workers).toHaveLength(1);
    expect(live.factory.prompts).toHaveLength(0);
    expect(live.factory.workers[0]?.contextUsageReads).toBe(1);
    expect(store.getSessionContextSnapshot(live.binding.botUid, live.binding)).toMatchObject({
      ...live.binding,
      sessionId: live.binding.sessionId,
      sessionFile: live.binding.sessionFile,
      cwd: live.binding.cwd,
      usedTokens: 4_096,
      contextWindow: 8_192,
      percent: 50,
      model: "fixture/context",
      observedAt: status.observedAt,
    });
  } finally {
    if (worker) await worker.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("live context remains readable while the task prompt is held and does not settle it", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-held-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const factory = new FakeWorkerFactory(async () => {
    started.resolve();
    await release.promise;
  });
  let worker: OmpWorker | undefined;
  try {
    const live = await createLiveWorker(root, factory);
    worker = live.worker;
    let promptSettled = false;
    const prompt = live.worker.prompt("held task", () => {}).finally(() => {
      promptSettled = true;
    });
    await started.promise;

    const status = await readSessionContext(live.binding, live.worker, new AbortController().signal, store);

    expect(status.source).toBe("live");
    expect(status.usedTokens).toBe(4_096);
    expect(promptSettled).toBe(false);
    expect(live.factory.workers[0]?.stopped).toBe(false);
    expect(live.factory.workers).toHaveLength(1);
    expect(live.factory.prompts.map(({ text }) => text)).toEqual(["held task"]);
    expect(live.factory.workers[0]?.contextUsageReads).toBe(1);
    release.resolve();
    await prompt;
  } finally {
    release.resolve();
    if (worker) await worker.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("idle context reuses the exact unchanged snapshot and labels it non-realtime", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-idle-session-context-")));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelType: 5, channelId: "group-context____topic-1" };
  const sessionFile = join(root, "sessions", "bound-session.jsonl");
  const binding = boundSession("bot-context", target, "bound-session", sessionFile, join(root, "workspace"));
  try {
    await mkdir(binding.cwd, { recursive: true });
    await writeSession(sessionFile, binding.sessionId, binding.cwd);
    store.saveSessionContextSnapshot(snapshot(binding, await stat(sessionFile)));

    const status = await readSessionContext(binding, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "persisted",
      stale: false,
      usedTokens: 4_096,
      contextWindow: 8_192,
      percent: 50,
      model: "fixture/context",
      observedAt: "2026-11-04T09:00:00.000Z",
      note: expect.stringContaining("非实时"),
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("partial live context is preserved and the percent can be derived only from tokens and window", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-partial-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  let worker: OmpWorker | undefined;
  try {
    const live = await createLiveWorker(root);
    worker = live.worker;
    live.factory.contextUsage = {
      tokens: 4_096,
      contextWindow: 8_192,
      percent: null,
      model: "fixture/context",
    };

    const liveStatus = await readSessionContext(live.binding, live.worker, new AbortController().signal, store);
    const saved = store.getSessionContextSnapshot(live.binding.botUid, live.binding);
    const idleStatus = await readSessionContext(live.binding, undefined, new AbortController().signal, store);

    expect(liveStatus).toMatchObject({ source: "live", usedTokens: 4_096, contextWindow: 8_192, percent: 50 });
    expect(saved).toMatchObject({ usedTokens: 4_096, contextWindow: 8_192, percent: null });
    expect(idleStatus).toMatchObject({ source: "persisted", usedTokens: 4_096, contextWindow: 8_192, percent: 50 });
    expect(idleStatus.note).toContain("比例由 tokens/contextWindow 计算");
  } finally {
    if (worker) await worker.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("known context tokens survive when the window and percentage are unavailable", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-context-without-window-"));
  const store = await openChannelStore(join(root, "state"));
  let worker: OmpWorker | undefined;
  try {
    const live = await createLiveWorker(root);
    worker = live.worker;
    live.factory.contextUsage = {
      tokens: 3_072,
      contextWindow: null,
      percent: null,
      model: "fixture/context",
    };

    const status = await readSessionContext(live.binding, live.worker, new AbortController().signal, store);
    const saved = store.getSessionContextSnapshot(live.binding.botUid, live.binding);

    expect(status).toMatchObject({ source: "live", usedTokens: 3_072, contextWindow: null, percent: null });
    expect(saved).toMatchObject({ usedTokens: 3_072, contextWindow: null, percent: null });
  } finally {
    if (worker) await worker.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("session history usage and compaction totals cannot invent a current context snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-unknown-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelType: 1, channelId: "dm-context-unknown" };
  const sessionFile = join(root, "sessions", "history-only.jsonl");
  const binding = boundSession("bot-context", target, "history-only", sessionFile, join(root, "workspace"));
  try {
    await mkdir(binding.cwd, { recursive: true });
    await writeSession(sessionFile, binding.sessionId, binding.cwd);
    await appendFile(sessionFile, `${JSON.stringify({
      type: "message",
      id: "assistant-entry",
      parentId: null,
      timestamp: "2026-11-04T09:01:00.000Z",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "completed" }],
        usage: { input: 121_000, output: 8_000, cacheRead: 24_000, cacheWrite: 0, totalTokens: 153_000 },
      },
    })}\n${JSON.stringify({
      type: "compaction",
      id: "compaction-entry",
      parentId: "assistant-entry",
      timestamp: "2026-11-04T09:02:00.000Z",
      tokensBefore: 121_000,
      tokensAfter: 2_048,
    })}\n`, "utf8");

    const status = await readSessionContext(binding, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "unavailable",
      stale: false,
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a snapshot from another session identity is unavailable, not displayed as current", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-mismatched-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelType: 1, channelId: "dm-context-rebound" };
  const sessionFile = join(root, "sessions", "current.jsonl");
  const cwd = join(root, "workspace");
  const binding = boundSession("bot-context", target, "current-session", sessionFile, cwd);
  try {
    await mkdir(cwd, { recursive: true });
    await writeSession(sessionFile, binding.sessionId, cwd);
    store.saveSessionContextSnapshot(snapshot(binding, await stat(sessionFile), {
      sessionId: "previous-session",
      sessionFile: join(root, "sessions", "previous.jsonl"),
      cwd: join(root, "previous-workspace"),
      usedTokens: 7_777,
      percent: 95,
    }));

    const status = await readSessionContext(binding, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "unavailable",
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a compacted JSONL invalidates the sample as a current value but preserves it as stale history", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-compacted-session-context-")));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelType: 5, channelId: "group-context____compacted" };
  const sessionFile = join(root, "sessions", "compacted.jsonl");
  const binding = boundSession("bot-context", target, "compacted-session", sessionFile, join(root, "workspace"));
  try {
    await mkdir(binding.cwd, { recursive: true });
    await writeSession(sessionFile, binding.sessionId, binding.cwd);
    store.saveSessionContextSnapshot(snapshot(binding, await stat(sessionFile)));
    await appendFile(sessionFile, `${JSON.stringify({
      type: "compaction",
      id: "new-compaction",
      parentId: null,
      timestamp: "2026-11-04T10:00:00.000Z",
      tokensBefore: 121_000,
      tokensAfter: 2_048,
    })}\n`, "utf8");

    const status = await readSessionContext(binding, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "persisted",
      stale: true,
      usedTokens: 4_096,
      contextWindow: 8_192,
      percent: 50,
      model: "fixture/context",
      observedAt: "2026-11-04T09:00:00.000Z",
    });
    expect(status.note).toMatch(/历史|变化|陈旧/u);
    expect(status.usedTokens).not.toBe(2_048);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a missing bound session JSONL makes a saved sample unavailable", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-missing-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  const target: ChatTarget = { channelType: 2, channelId: "group-context-missing" };
  const sessionFile = join(root, "sessions", "missing.jsonl");
  const binding = boundSession("bot-context", target, "missing-session", sessionFile, join(root, "workspace"));
  try {
    await mkdir(binding.cwd, { recursive: true });
    store.saveSessionContextSnapshot(snapshot(binding, { size: 128, mtimeMs: 1_794_092_400_000 }));

    const status = await readSessionContext(binding, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "unavailable",
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("an unbound chat has unknown context without starting a worker", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-unbound-session-context-"));
  const store = await openChannelStore(join(root, "state"));
  try {
    const status = await readSessionContext(null, undefined, new AbortController().signal, store);

    expect(status).toMatchObject({
      source: "unavailable",
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
      observedAt: null,
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
