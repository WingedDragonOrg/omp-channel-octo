import { expect, test, vi } from "bun:test";
import type { ChatTarget, OmpWorker, ProgressCard, SendReceipt, WorkerOptions } from "../src/contracts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  type FakePromptContext,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

const flushMicrotasks = async (): Promise<void> => {
  for (let step = 0; step < 8; step += 1) await Promise.resolve();
};

type TypingCall = {
  target: ChatTarget;
  signal: AbortSignal;
  aborted: boolean;
};

type ProgressCardSend = {
  target: ChatTarget;
  progress: ProgressCard;
  clientMsgNo: string;
};

type ProgressCardEdit = {
  target: ChatTarget;
  messageId: string;
  progress: ProgressCard;
  cardSeq: number;
};

class PresentationTransport extends FakeTransport {
  readonly typingCalls: TypingCall[] = [];
  readonly progressCardSends: ProgressCardSend[] = [];
  readonly progressCardEdits: ProgressCardEdit[] = [];
  failProgressCards = false;
  private readonly progressEditWaiters: Array<{
    predicate: (edit: ProgressCardEdit) => boolean;
    resolve: (edit: ProgressCardEdit) => void;
  }> = [];
  async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    const controller = signal ? undefined : new AbortController();
    const effectiveSignal = signal ?? controller!.signal;
    const call: TypingCall = { target, signal: effectiveSignal, aborted: effectiveSignal.aborted };
    effectiveSignal.addEventListener("abort", () => {
      call.aborted = true;
    }, { once: true });
    this.typingCalls.push(call);
    if (effectiveSignal.aborted) throw new Error("fixture typing aborted");
  }

  async sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    if (signal?.aborted) throw new Error("fixture progress card aborted");
    if (this.failProgressCards) throw new Error("fixture progress card unavailable");
    this.progressCardSends.push({ target, progress: { ...progress }, clientMsgNo });
    return { messageId: `progress-card-${this.progressCardSends.length}` };
  }

  async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw new Error("fixture progress card edit aborted");
    if (this.failProgressCards) throw new Error("fixture progress card unavailable");
    const edit = { target, messageId, progress: { ...progress }, cardSeq };
    this.progressCardEdits.push(edit);
    for (const waiter of this.progressEditWaiters) {
      if (waiter.predicate(edit)) waiter.resolve(edit);
    }
  }

  waitForProgressCard(predicate: (edit: ProgressCardEdit) => boolean): Promise<ProgressCardEdit> {
    const existing = this.progressCardEdits.find(predicate);
    if (existing) return Promise.resolve(existing);
    const deferred = Promise.withResolvers<ProgressCardEdit>();
    this.progressEditWaiters.push({ predicate, resolve: deferred.resolve });
    return deferred.promise;
  }
}
class FailingTerminalProgressTransport extends PresentationTransport {
  readonly cardCreated = Promise.withResolvers<void>();
  terminalEditAttempts = 0;

  override async sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    const receipt = await super.sendProgressCard(target, progress, clientMsgNo, signal);
    this.cardCreated.resolve();
    return receipt;
  }
  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (progress.state !== "running") {
      this.terminalEditAttempts += 1;
      throw new Error("fixture terminal progress edit unavailable");
    }
    return super.editProgressCard(target, messageId, progress, cardSeq, signal);
  }
}

class DelayedStartupTransport extends PresentationTransport {
  readonly contextStarted = Promise.withResolvers<void>();
  private readonly contextGate = Promise.withResolvers<void>();

  override async getGroupContext(target: ChatTarget) {
    this.contextStarted.resolve();
    await this.contextGate.promise;
    return super.getGroupContext(target);
  }

  releaseContext(): void {
    this.contextGate.resolve();
  }
}

class DelayedWorkerFactory extends FakeWorkerFactory {
  readonly createStarted = Promise.withResolvers<void>();
  private readonly createGate = Promise.withResolvers<void>();

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    this.createStarted.resolve();
    await this.createGate.promise;
    return super.create(options);
  }

  releaseCreate(): void {
    this.createGate.resolve();
  }
}

class AbortableEditTransport extends PresentationTransport {
  readonly runningEditStarted = Promise.withResolvers<void>();
  runningEditAbortSeen = false;

  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (progress.state === "running") {
      this.runningEditStarted.resolve();
      await new Promise<void>((_resolve, reject) => {
        if (!signal) {
          reject(new Error("running progress edit lacked an abort signal"));
          return;
        }
        const abort = () => {
          this.runningEditAbortSeen = true;
          reject(new Error("running progress edit aborted"));
        };
        if (signal.aborted) abort();
        else signal.addEventListener("abort", abort, { once: true });
      });
    }
    await super.editProgressCard(target, messageId, progress, cardSeq, signal);
  }
}
class AbortableTypingTransport extends PresentationTransport {
  readonly typingStarted = Promise.withResolvers<void>();
  typingAbortSeen = false;

  override async sendTyping(target: ChatTarget, signal?: AbortSignal): Promise<void> {
    await super.sendTyping(target, signal);
    this.typingStarted.resolve();
    await new Promise<void>((_resolve, reject) => {
      if (!signal) {
        reject(new Error("typing request lacked an abort signal"));
        return;
      }
      const abort = () => {
        this.typingAbortSeen = true;
        reject(new Error("typing request aborted"));
      };
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
    });
  }



}

test("a complete assistant text event is one lossless message without lifecycle chatter", async () => {
  const transport = new PresentationTransport();
  const body = "Use `inline code` here.\n\nKeep this as a second paragraph.\n  Preserve spacing.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent({ type: "turn_start", turnId: "turn-1", atMs: 1 });
      emitEvent(assistantText("assistant-lossless", body, true));
      emitEvent({ type: "turn_end", turnId: "turn-1", atMs: 2 });
    },
  });
  const task = inbound({
    messageId: "presentation-lossless",
    channelId: "dm-presentation-lossless",
    text: "render this",
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.sent.map(({ text }) => text)).toEqual([body]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("separate assistant text events are delivered as separate messages", async () => {
  const transport = new PresentationTransport();
  const first = "First assistant message with `one`.\n\nFirst paragraph two.";
  const second = "Second assistant message with `two`.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent(assistantText("assistant-first", first, true));
      emitEvent(assistantText("assistant-second", second, true));
    },
  });
  const task = inbound({
    messageId: "presentation-multiple",
    channelId: "dm-presentation-multiple",
    text: "produce two messages",
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.sent.map(({ text }) => text)).toEqual([first, second]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a task with no assistant text does not fabricate a reply", async () => {
  const transport = new PresentationTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent({ type: "turn_start", turnId: "turn-silent", atMs: 1 });
      emitEvent({
        type: "tool_start",
        toolCallId: "shell-1",
        toolName: "shell",
        atMs: 2,
        summary: "tool_execution_start shell",
      });
      emitEvent({
        type: "tool_end",
        toolCallId: "shell-1",
        atMs: 3,
        status: "succeeded",
        summary: "tool_execution_end shell",
      });
      emitEvent({ type: "turn_end", turnId: "turn-silent", atMs: 4 });
    },
  });
  const task = inbound({
    messageId: "presentation-no-text",
    channelId: "dm-presentation-no-text",
    text: "do silent work",
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.sent).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("tool progress uses one editable card, keeps typing alive, and closes completed", async () => {
  const transport = new PresentationTransport();
  let promptStarted!: () => void;
  const promptReady = new Promise<void>((resolve) => {
    promptStarted = resolve;
  });
  vi.useFakeTimers();
  let release!: () => void;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const body = "The tool finished; here is the result.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent({
        type: "tool_start",
        toolCallId: "shell-1",
        toolName: "shell",
        atMs: 1,
        summary: "tool_execution_start shell",
      });
      promptStarted();
      await held;
      emitEvent({
        type: "tool_end",
        toolCallId: "shell-1",
        atMs: 2,
        status: "succeeded",
        summary: "tool_execution_end shell",
      });
      emitEvent(assistantText("assistant-progress", body, true));
    },
  });
  const task = inbound({
    messageId: "presentation-progress-card",
    channelId: "dm-presentation-progress-card",
    text: "run a tool",
  });
  const terminalCardPromise = transport.waitForProgressCard(
    (edit) => edit.progress.state === "completed",
  );
  try {
    await harness.service.handleInbound(task);
    await promptReady;
    await Promise.resolve();
    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(2);


    release();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    await Promise.resolve();

    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.progressCardSends[0]?.progress.state).toBe("running");
    const terminal = (await terminalCardPromise).progress;
    expect(terminal.state).toBe("completed");
    expect(transport.progressCardEdits.map(({ messageId }) => messageId)).toEqual(
      transport.progressCardEdits.map(() => "progress-card-1"),
    );
    expect(transport.progressCardEdits.map(({ cardSeq }) => cardSeq)).toEqual(
      [...transport.progressCardEdits].map(({ cardSeq }) => cardSeq).sort((a, b) => a - b),
    );
    expect(transport.sent.map(({ text }) => text)).toEqual([body]);
    const typingCount = transport.typingCalls.length;
    vi.advanceTimersByTime(6_000);
    await Promise.resolve();
    expect(transport.typingCalls).toHaveLength(typingCount);
  } finally {
    release();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("todo phases and tool summaries remain on the terminal completed card", async () => {
  vi.useFakeTimers();
  const transport = new PresentationTransport();
  const phases = [
    {
      name: "Plan",
      tasks: [
        { content: "Wire streaming", status: "in_progress" as const },
        { content: "Verify delivery", status: "pending" as const },
      ],
    },
  ];
  const body = "streaming is ready";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit({ type: "turn_start", turnId: "turn-todo", atMs: 100 });
      emit({ type: "todo_update", atMs: 110, op: "init", phases });
      emit({
        type: "tool_start",
        toolCallId: "tool-todo",
        toolName: "shell",
        atMs: 120,
        summary: "run checks",
      });
      emit({
        type: "tool_end",
        toolCallId: "tool-todo",
        atMs: 130,
        status: "succeeded",
        summary: "checks passed",
      });
      emit({ type: "run_end", atMs: 140, state: "completed" });
      emit(assistantText("assistant-todo", body, true));
    },
  });
  const task = inbound({
    messageId: "presentation-todo-terminal",
    channelId: "dm-presentation-todo-terminal",
    text: "run checks",
  });
  const terminalCardPromise = transport.waitForProgressCard(
    (edit) => edit.progress.state === "completed",
  );
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    const terminal = (await terminalCardPromise).progress;

    expect(transport.progressCardSends).toHaveLength(1);
    expect(terminal.state).toBe("completed");
    expect(terminal.todos).toEqual(phases);
    expect(terminal.tools[0]).toMatchObject({
      toolCallId: "tool-todo",
      state: "succeeded",
      summary: "checks passed",
    });
    expect(transport.sent.map(({ text }) => text)).toEqual([body]);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("failed terminal execution events preserve todo phases and error category", async () => {
  vi.useFakeTimers();
  const transport = new PresentationTransport();
  const phases = [
    { name: "Repair", tasks: [{ content: "Retry request", status: "blocked" as const, blocker: "权限" }] },
  ];
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit({ type: "turn_start", turnId: "turn-failed", atMs: 200 });
      emit({ type: "todo_update", atMs: 210, op: "block", phases });
      emit({
        type: "tool_start",
        toolCallId: "tool-failed",
        toolName: "network",
        atMs: 220,
        summary: "request",
      });
      emit({
        type: "tool_end",
        toolCallId: "tool-failed",
        atMs: 230,
        status: "failed",
        errorCategory: "permission",
        summary: "permission denied",
      });
      emit({ type: "run_end", atMs: 240, state: "failed", errorCategory: "permission" });
      throw new Error("fixture execution failed");
    },
  });
  const task = inbound({
    messageId: "presentation-failed-terminal",
    channelId: "dm-presentation-failed-terminal",
    text: "run blocked request",
  });
  const terminalCardPromise = transport.waitForProgressCard(
    (edit) => edit.progress.state === "failed",
  );
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "failed" && status.outbox === 0,
    );
    const terminal = (await terminalCardPromise).progress;
    expect(terminal.state).toBe("failed");
    expect(terminal.todos).toEqual(phases);
    expect(terminal.tools[0]).toMatchObject({
      state: "failed",
      errorCategory: "permission",
    });
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("a running tool card only re-renders on real events and stops at terminal", async () => {
  const transport = new PresentationTransport();
  const promptStarted = Promise.withResolvers<void>();
  const workerGate = Promise.withResolvers<void>();
  let emitLater: FakePromptContext["emit"] | undefined;
  const task = inbound({
    messageId: "presentation-card-idle",
    channelId: "dm-presentation-card-idle",
    text: "hold a tool",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emitLater = emit;
      const startedAt = Date.now();
      emit({ type: "turn_start", turnId: "turn-idle", atMs: startedAt });
      emit({
        type: "tool_start",
        toolCallId: "tool-idle",
        toolName: "shell",
        atMs: Date.now(),
        summary: "long running",
      });
      promptStarted.resolve();
      await workerGate.promise;
    },
  });
  try {
    vi.useFakeTimers();
    await harness.service.handleInbound(task);
    await promptStarted.promise;
    await flushMicrotasks();
    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.progressCardEdits).toHaveLength(0);

    vi.advanceTimersByTime(60_000);
    await flushMicrotasks();
    expect(transport.progressCardEdits).toHaveLength(0);

    emitLater?.({
      type: "tool_end",
      toolCallId: "tool-idle",
      atMs: Date.now(),
      status: "succeeded",
      summary: "long running",
    });
    vi.advanceTimersByTime(1_000);
    await flushMicrotasks();
    expect(transport.progressCardEdits).toHaveLength(1);
    expect(transport.progressCardEdits[0]?.progress.tools[0]?.state).toBe("succeeded");

    workerGate.resolve();
    for (let step = 0; step < 10 && transport.progressCardEdits.at(-1)?.progress.state !== "completed"; step += 1) {
      vi.advanceTimersByTime(1_000);
      await flushMicrotasks();
    }
    const terminal = transport.progressCardEdits.at(-1);
    expect(terminal?.progress.state).toBe("completed");
    const editCount = transport.progressCardEdits.length;
    vi.advanceTimersByTime(120_000);
    await flushMicrotasks();
    expect(transport.progressCardEdits).toHaveLength(editCount);
  } finally {
    workerGate.resolve();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
}, 10_000);

test("a terminal progress edit failure is bounded and disposed on service stop", async () => {
  const transport = new FailingTerminalProgressTransport();
  const task = inbound({
    messageId: "presentation-terminal-edit-failure",
    channelId: "dm-presentation-terminal-edit-failure",
    text: "dispose a failed terminal card",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      const now = Date.now();
      emit({
        type: "tool_start",
        toolCallId: "tool-terminal-failure",
        toolName: "shell",
        atMs: now,
        summary: "terminal failure",
      });
      await transport.cardCreated.promise;
      await Promise.resolve();
      emit({ type: "run_end", atMs: Date.now(), state: "completed" });
    },
  });
  let stopped = false;
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    for (let attempt = 0; attempt < 100 && transport.terminalEditAttempts === 0; attempt += 1) {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
    }
    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.terminalEditAttempts).toBe(1);

    await harness.service.stop();
    stopped = true;
    const attemptsAfterStop = transport.terminalEditAttempts;
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
    expect(transport.terminalEditAttempts).toBe(attemptsAfterStop);
  } finally {
    if (!stopped) await harness.service.stop().catch(() => {});
    await closeCoreHarness(harness);
  }
}, 5_000);

test("typing and the progress card are cancelled when an active task is cancelled", async () => {
  vi.useFakeTimers();
  const transport = new AbortableTypingTransport();
  let promptStarted!: () => void;
  const promptReady = new Promise<void>((resolve) => {
    promptStarted = resolve;
  });
  let releaseOnStop!: () => void;
  const stopped = new Promise<void>((resolve) => {
    releaseOnStop = resolve;
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent, onStop, isStopped }) => {
      emitEvent({
        type: "tool_start",
        toolCallId: "shell-1",
        toolName: "shell",
        atMs: 1,
        summary: "tool_execution_start shell",
      });
      promptStarted();
      onStop(releaseOnStop);
      await stopped;
      if (isStopped()) throw new Error("fixture worker cancelled");
    },
  });
  const target = { channelId: "dm-presentation-cancel", channelType: 1 as const };
  try {
    const task = inbound({ messageId: "presentation-cancel-task", ...target, text: "cancel tool" });
    await harness.service.handleInbound(task);
    await promptReady;
    await Promise.resolve();
    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    await transport.typingStarted.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "presentation-cancel-command", ...target, text: "/cancel" }),
    );
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "cancelled" && status.outbox === 0,
    );
    await Promise.resolve();

    expect(transport.progressCardEdits.at(-1)?.progress.state).toBe("cancelled");
    expect(transport.typingAbortSeen).toBe(true);
    const typingCount = transport.typingCalls.length;
    vi.advanceTimersByTime(6_000);
    await Promise.resolve();
    expect(transport.typingCalls).toHaveLength(typingCount);
  } finally {
    releaseOnStop();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});
test("typing and the progress card are cleaned up when the worker fails", async () => {
  vi.useFakeTimers();
  const transport = new PresentationTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent({
        type: "tool_start",
        toolCallId: "shell-1",
        toolName: "shell",
        atMs: 1,
        summary: "tool_execution_start shell",
      });
      throw new Error("fixture worker failed");
    },
  });
  const task = inbound({
    messageId: "presentation-failure",
    channelId: "dm-presentation-failure",
    text: "fail tool",
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "failed" && status.outbox === 0,
    );
    await Promise.resolve();

    expect(transport.progressCardEdits.at(-1)?.progress.state).toBe("failed");
    const typingCount = transport.typingCalls.length;
    vi.advanceTimersByTime(6_000);
    await Promise.resolve();
    expect(transport.typingCalls).toHaveLength(typingCount);
  } finally {
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a progress-card API failure does not block the assistant body", async () => {
  const transport = new PresentationTransport();
  transport.failProgressCards = true;
  const body = "正文仍然要送达。";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent }) => {
      emitEvent({
        type: "tool_start",
        toolCallId: "shell-1",
        toolName: "shell",
        atMs: 1,
        summary: "tool_execution_start shell",
      });
      emitEvent(assistantText("assistant-card-failure", body, true));
    },
  });
  const task = inbound({
    messageId: "presentation-card-failure",
    channelId: "dm-presentation-card-failure",
    text: "tool with unavailable status card",
  });
  try {
    await harness.service.handleInbound(task);
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.sent.map(({ text }) => text)).toEqual([body]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("typing starts before group context and worker startup and renews", async () => {
  vi.useFakeTimers();
  const transport = new DelayedStartupTransport();
  const workers = new DelayedWorkerFactory();
  const harness = await createCoreHarness({ transport, workers });
  const task = inbound({
    messageId: "presentation-startup-typing",
    channelId: "group-allowed",
    channelType: 2,
    text: "wait for startup",
    mentionedUserIds: [transport.bot.uid],
  });
  try {
    await harness.service.handleInbound(task);
    await transport.contextStarted.promise;
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(1);
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(2);

    transport.releaseContext();
    await workers.createStarted.promise;
    vi.advanceTimersByTime(3_000);
    await Promise.resolve();
    expect(transport.typingCalls.length).toBeGreaterThanOrEqual(3);

    workers.releaseCreate();
    await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "completed" && status.outbox === 0,
    );
    const typingCount = transport.typingCalls.length;
    vi.advanceTimersByTime(6_000);
    await Promise.resolve();
    expect(transport.typingCalls).toHaveLength(typingCount);
    vi.useRealTimers();
  } finally {
    transport.releaseContext();
    workers.releaseCreate();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("an aborted running card edit still submits the terminal card state", async () => {
  vi.useFakeTimers();
  let releaseWorker!: () => void;
  const workerGate = new Promise<void>((resolve) => {
    releaseWorker = resolve;
  });
  const transport = new AbortableEditTransport();
  const workers = new FakeWorkerFactory(async ({ emit, onStop, isStopped }) => {
    emit({ type: "turn_start", turnId: "turn-abort", atMs: 1 });
    emit({
      type: "tool_start",
      toolCallId: "shell-1",
      toolName: "shell",
      atMs: 2,
      summary: "shell · 运行中",
    });
    emit({ type: "turn_end", turnId: "turn-abort", atMs: 3 });
    onStop(releaseWorker);
    await workerGate;
    if (isStopped()) throw new Error("fixture worker cancelled");
  });
  const harness = await createCoreHarness({ transport, workers });
  const target = { channelId: "dm-presentation-abort-edit", channelType: 1 as const };
  const task = inbound({ messageId: "presentation-abort-edit-task", ...target, text: "abort edit" });
  try {
    await harness.service.handleInbound(task);
    await workers.waitForPromptCount(1);
    await Promise.resolve();
    await Promise.resolve();
    vi.advanceTimersByTime(800);
    await transport.runningEditStarted.promise;

    await harness.service.handleInbound(
      inbound({ messageId: "presentation-abort-edit-cancel", ...target, text: "/cancel" }),
    );
    vi.useRealTimers();
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "cancelled" && status.outbox === 0,
    );
    await Promise.resolve();

    expect(transport.runningEditAbortSeen).toBe(true);
    const terminal = transport.progressCardEdits.at(-1);
    expect(terminal?.progress.state).toBe("cancelled");
    expect(terminal?.messageId).toBe("progress-card-1");
    expect(transport.progressCardEdits.map(({ cardSeq }) => cardSeq)).toEqual(
      [...transport.progressCardEdits].map(({ cardSeq }) => cardSeq).sort((a, b) => a - b),
    );
  } finally {
    releaseWorker();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

 
