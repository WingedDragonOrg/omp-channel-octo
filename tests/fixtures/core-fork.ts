import { readFile, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type {
  ChatTarget,
  InboundMessage,
  OmpWorker,
  ThreadSummary,
  WorkerOptions,
} from "../../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../../src/store/index.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
  type FakePrompt,
} from "./core-fakes.ts";

export const FORK_GROUP: ChatTarget & { channelType: 2 } = {
  channelId: "group-fork",
  channelType: 2,
};
export const FORK_DM: ChatTarget & { channelType: 1 } = {
  channelId: "dm-fork",
  channelType: 1,
};

export function gate(): { promise: Promise<void>; resolve(): void } {
  const result = Promise.withResolvers<void>();
  return { promise: result.promise, resolve: result.resolve };
}

export async function waitForForkBoundary(promise: Promise<void>, description: string): Promise<void> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(
    () => timeout.reject(new Error(`fork test boundary timed out: ${description}`)),
    5_000,
  );
  try {
    await Promise.race([promise, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

export function ownerMessage(
  harness: Pick<CoreHarness, "bot">,
  target: ChatTarget,
  messageId: string,
  text: string,
): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [harness.bot.uid],
  });
}

export interface ForkWorkerAttempt {
  sourceSessionFile: string;
  cwd: string;
  model: string | undefined;
  signal: AbortSignal | undefined;
}

export interface ForkWorkerResult extends ForkWorkerAttempt {
  sessionId: string;
  sessionFile: string;
}

/** Fake `--fork` at the WorkerFactory boundary while keeping later target `--resume` real. */
export class ForkTestWorkerFactory extends FakeWorkerFactory {
  readonly forkAttempts: ForkWorkerAttempt[] = [];
  readonly forks: ForkWorkerResult[] = [];
  readonly forkWorkerOptions: WorkerOptions[] = [];
  readonly forkStarted = Promise.withResolvers<void>();
  readonly forkStopStarted = Promise.withResolvers<void>();
  readonly releaseForkStop = Promise.withResolvers<void>();
  holdForkStop = false;
  forkStopResult: boolean | undefined;

  constructor(prompt?: FakePrompt) {
    super(prompt);
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const forkOptions = options as WorkerOptions & {
      forkFrom?: string;
      signal?: AbortSignal;
    };
    const sourceSessionFile = forkOptions.forkFrom;
    if (sourceSessionFile === undefined) return super.create(options);

    const attempt: ForkWorkerAttempt = {
      sourceSessionFile,
      cwd: options.cwd,
      model: options.model,
      signal: forkOptions.signal,
    };
    this.forkAttempts.push(attempt);
    this.forkWorkerOptions.push(options);
    this.forkStarted.resolve();
    const worker = await super.create(options);
    const state = await worker.refreshState();
    const original = await readFile(sourceSessionFile, "utf8");
    const [sourceHeader = "{}", ...history] = original.split("\n");
    const parsed = JSON.parse(sourceHeader) as Record<string, unknown>;
    await writeFile(
      state.sessionFile,
      `${JSON.stringify({ ...parsed, type: "session", id: state.sessionId, cwd: state.cwd })}\n${history.join("\n")}`,
      "utf8",
    );
    this.forks.push({ ...attempt, sessionId: state.sessionId, sessionFile: state.sessionFile });
    const stop = worker.stop.bind(worker);
    worker.stop = async () => {
      this.forkStopStarted.resolve();
      if (this.holdForkStop) await this.releaseForkStop.promise;
      if (this.forkStopResult !== undefined) return this.forkStopResult;
      return stop();
    };
    return worker;
  }
}

/** Transport controls the create-thread boundary, including timeout-after-server-commit ambiguity. */
export class ForkTestTransport extends FakeTransport {
  createAttempts = 0;
  holdCreateThread = false;
  lastCreateSignal: AbortSignal | undefined;
  readonly createThreadStarted = Promise.withResolvers<void>();
  readonly releaseCreateThread = Promise.withResolvers<void>();
  threadResultOverride: ThreadSummary | undefined;
  afterCreateFailure: Error | undefined;
  override async createThread(
    groupId: string,
    name: string,
    signal?: AbortSignal,
  ): Promise<ThreadSummary> {
    this.createAttempts += 1;
    this.lastCreateSignal = signal;
    if (this.holdCreateThread) {
      this.createThreadStarted.resolve();
      const released = Promise.withResolvers<void>();
      const aborted = Promise.withResolvers<never>();
      const onAbort = (): void => aborted.reject(signal?.reason ?? new Error("thread creation aborted"));
      if (signal?.aborted) onAbort();
      else signal?.addEventListener("abort", onAbort, { once: true });
      void this.releaseCreateThread.promise.then(released.resolve, released.reject);
      try {
        await Promise.race([released.promise, aborted.promise]);
      } finally {
        signal?.removeEventListener("abort", onAbort);
      }
      if (signal?.aborted) throw signal.reason ?? new Error("thread creation aborted");
    }
    const thread = await super.createThread(groupId, name, signal);
    const failure = this.afterCreateFailure;
    this.afterCreateFailure = undefined;
    if (failure !== undefined) throw failure;
    const returned = this.threadResultOverride ?? thread;
    this.threadResultOverride = undefined;
    return returned;
  }
}

export async function withStore<T>(
  harness: CoreHarness,
  work: (store: ChannelStore) => T | Promise<T>,
): Promise<T> {
  const store = await openChannelStore(harness.stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

export async function seedBinding(
  harness: CoreHarness,
  target: ChatTarget,
  cwd: string,
  sessionId: string,
): Promise<string> {
  const canonicalCwd = await realpath(cwd);
  const sessionFile = join(harness.root, `${sessionId}.jsonl`);
  await writeFile(
    sessionFile,
    `${JSON.stringify({ type: "session", id: sessionId, cwd: canonicalCwd, previousSessionFiles: [] })}\n`,
    "utf8",
  );
  await withStore(harness, (store) => store.ensureBinding({
    botUid: harness.bot.uid,
    ...target,
    sessionId,
    cwd: canonicalCwd,
    sessionFile,
  }));
  return sessionFile;
}

export function threadSummary(
  groupId: string,
  shortId: string,
  name = `Topic ${shortId}`,
): ThreadSummary {
  return {
    shortId,
    groupId,
    channelId: `${groupId}____${shortId}`,
    name,
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-03T00:00:00.000Z",
    updatedAt: "2026-11-03T00:00:00.000Z",
  };
}
