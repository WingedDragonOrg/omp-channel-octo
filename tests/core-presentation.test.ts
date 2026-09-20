import { expect, test, vi } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ChatTarget,
  OmpWorker,
  ProgressCard,
  SendReceipt,
  WorkerNoticeEvent,
  WorkerOptions,
} from "../src/contracts";
import { OctoError } from "../src/octo/error";
import { openChannelStore } from "../src/store";
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

/**
 * One extension notice frame as `WorkerNoticeEvent` carries it (e.g. `/autoresearch` enable /
 * off / dirty-worktree refusal): the chat renders these, but they are not turn output.
 */
const noticeEvent = (
  messageId: string,
  text: string,
  level: "info" | "warning" | "error",
): WorkerNoticeEvent => ({ type: "notice", messageId, text, level });

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
class EdgeRefusedEditTransport extends PresentationTransport {
  /** Edits the deployment's edge refused, in order, before the card fell back to a safe shape. */
  readonly refusedEdits: ProgressCardEdit[] = [];

  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (this.refusedEdits.length === 0 && progress.tools.some((tool) => tool.summary !== undefined)) {
      this.refusedEdits.push({ target, messageId, progress, cardSeq });
      throw new OctoError("Octo message edit request failed with HTTP 403 (non-bot-API response body)", {
        status: 403,
        permanent: true,
      });
    }
    return super.editProgressCard(target, messageId, progress, cardSeq, signal);
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

test("a steered message closes the current card and the next round opens a new one", async () => {
  const transport = new PresentationTransport();
  const promptReady = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit: emitEvent, waitForAppend }) => {
      emitEvent({ type: "turn_start", turnId: "round-1-turn-1", atMs: 1 });
      emitEvent({
        type: "tool_start",
        toolCallId: "read-1",
        toolName: "read",
        atMs: 2,
        summary: "tool_execution_start read",
      });
      emitEvent({ type: "tool_end", toolCallId: "read-1", atMs: 3, status: "succeeded" });
      emitEvent({
        type: "todo_update",
        atMs: 4,
        op: "init",
        phases: [
          {
            name: "Plan",
            tasks: [
              { content: "Wire streaming", status: "in_progress" },
              { content: "Verify delivery", status: "pending" },
            ],
          },
        ],
      });
      emitEvent({ type: "turn_end", turnId: "round-1-turn-1", atMs: 4 });
      emitEvent({ type: "turn_start", turnId: "round-1-turn-2", atMs: 5 });
      emitEvent({
        type: "tool_start",
        toolCallId: "grep-1",
        toolName: "grep",
        atMs: 6,
        summary: "tool_execution_start grep",
      });
      emitEvent({ type: "tool_end", toolCallId: "grep-1", atMs: 7, status: "succeeded" });
      promptReady.resolve();
      await waitForAppend();
      // The appended message must close the card of the round it interrupted before the next
      // round opens the card that sits below it.
      await transport.waitForProgressCard((edit) => edit.progress.state === "completed");
      emitEvent({ type: "turn_end", turnId: "round-1-turn-2", atMs: 8 });
      emitEvent({ type: "turn_start", turnId: "round-2-turn-1", atMs: 9 });
      emitEvent({
        type: "tool_start",
        toolCallId: "bash-1",
        toolName: "bash",
        atMs: 10,
        summary: "tool_execution_start bash",
      });
      emitEvent({ type: "tool_end", toolCallId: "bash-1", atMs: 11, status: "succeeded" });
      emitEvent(assistantText("assistant-round-2", "second round done", true));
    },
  });
  const target = { channelId: "dm-steer-card", channelType: 1 as const };
  try {
    const running = harness.service.handleInbound(
      inbound({ messageId: "steer-card-first", ...target, text: "run the first tool" }),
    );
    await promptReady.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "steer-card-second", ...target, text: "second note" }),
    );
    await running;
    await waitForStatus(
      harness.service,
      target,
      (status) => status.state === "completed" && status.outbox === 0,
    );

    expect(transport.progressCardSends).toHaveLength(2);
    const [firstCard, secondCard] = transport.progressCardSends;
    expect(firstCard!.clientMsgNo).not.toBe(secondCard!.clientMsgNo);
    // The card opens on the round's first tool event, and the round's later work lands on it.
    expect(firstCard!.progress.tools.map((tool) => tool.toolCallId)).toEqual(["read-1"]);
    // The new card tracks the round it belongs to: its own phases and tools only.
    expect(secondCard!.progress.phaseCount).toBe(1);
    expect(secondCard!.progress.tools.map((tool) => tool.toolCallId)).toEqual(["bash-1"]);
    // The task's todo list carries over: it belongs to the task, not to one round.
    expect(secondCard!.progress.todos.map((phase) => phase.name)).toEqual(["Plan"]);

    const closedFirstCard = transport.progressCardEdits.filter(
      (edit) => edit.messageId === "progress-card-1",
    );
    expect(closedFirstCard.map((edit) => edit.progress.state)).toEqual(["completed"]);
    expect(closedFirstCard[0]!.progress.tools.map((tool) => tool.toolCallId)).toEqual([
      "read-1",
      "grep-1",
    ]);
    expect(transport.progressCardEdits.filter((edit) => edit.messageId === "progress-card-2"))
      .not.toHaveLength(0);
    expect(transport.sent.map(({ text }) => text)).toEqual(["second round done"]);
  } finally {
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

test("an edge-refused card body is retried without step summaries and still closes", async () => {
  const transport = new EdgeRefusedEditTransport();
  const promptStarted = Promise.withResolvers<void>();
  const workerGate = Promise.withResolvers<void>();
  let emitLater: FakePromptContext["emit"] | undefined;
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  const task = inbound({
    messageId: "presentation-card-edge-refusal",
    channelId: "dm-presentation-card-edge-refusal",
    text: "run a command the edge dislikes",
  });
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emitLater = emit;
      emit({ type: "turn_start", turnId: "turn-edge", atMs: Date.now() });
      emit({
        type: "todo_update",
        atMs: Date.now(),
        phases: [{ name: "核对", tasks: [{ content: "核对卡片", status: "in_progress" }] }],
      });
      emit({
        type: "tool_start",
        toolCallId: "tool-edge",
        toolName: "shell",
        atMs: Date.now(),
        summary: "cat > /tmp/probe.ts <<'EOF'",
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

    emitLater?.({
      type: "tool_end",
      toolCallId: "tool-edge",
      atMs: Date.now(),
      status: "succeeded",
      summary: "cat > /tmp/probe.ts <<'EOF'",
    });
    vi.advanceTimersByTime(1_000);
    await flushMicrotasks();
    expect(transport.refusedEdits).toHaveLength(1);
    expect(transport.refusedEdits[0]?.messageId).toBe("progress-card-1");
    expect(transport.refusedEdits[0]?.progress.tools.map((tool) => tool.summary)).toEqual([
      "cat > /tmp/probe.ts <<'EOF'",
    ]);
    expect(transport.progressCardEdits).toHaveLength(0);

    vi.advanceTimersByTime(1_000);
    await flushMicrotasks();
    const retried = transport.progressCardEdits[0];
    expect(retried?.messageId).toBe("progress-card-1");
    expect(retried?.progress.tools.map((tool) => [tool.toolName, tool.state])).toEqual([
      ["shell", "succeeded"],
    ]);
    expect(Object.hasOwn(retried!.progress.tools[0]!, "summary")).toBe(false);
    expect(retried?.progress.todos[0]?.tasks.map((todo) => [todo.content, todo.status])).toEqual([
      ["核对卡片", "in_progress"],
    ]);

    workerGate.resolve();
    for (let step = 0; step < 10 && transport.progressCardEdits.at(-1)?.progress.state !== "completed"; step += 1) {
      vi.advanceTimersByTime(1_000);
      await flushMicrotasks();
    }
    const terminal = transport.progressCardEdits.at(-1);
    expect(terminal?.progress.state).toBe("completed");
    expect(Object.hasOwn(terminal!.progress.tools[0]!, "summary")).toBe(false);
    expect(transport.progressCardSends).toHaveLength(1);
    expect(transport.refusedEdits).toHaveLength(1);

    const lines = logged.mock.calls.map((call) => String(call[0] ?? ""));
    expect(lines.some((line) => line.includes("运行中编辑失败"))).toBe(true);
    expect(lines.some((line) => line.includes("改用不带步骤摘要的形态重试"))).toBe(true);
  } finally {
    workerGate.resolve();
    logged.mockRestore();
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
}, 10_000);

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

test("a notice-only command outcome is delivered to the chat without assistant output or a progress card", async () => {
  const transport = new PresentationTransport();
  const noticeText =
    "Worktree is dirty (src/index.ts). Commit or stash these changes before starting autoresearch — a fresh autoresearch/* branch needs a clean baseline.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      // Mirrors `/autoresearch` on a dirty worktree: the extension notifies and settles
      // with agentInvoked:false — no turn frames, no assistant text.
      emit(noticeEvent("autoresearch-notify-dirty", noticeText, "error"));
    },
  });
  const task = inbound({
    messageId: "notice-only-dirty",
    channelId: "dm-notice-only-dirty",
    text: "/autoresearch",
  });
  try {
    try {
      await harness.service.handleInbound(task);
      await waitForStatus(
        harness.service,
        task,
        (status) => status.state === "completed" && status.outbox === 0,
      );

      const delivered = transport.listRemoteText(task);
      expect(delivered).toHaveLength(1);
      const noticeMessage = delivered[0]!;
      expect(noticeMessage.text).toContain(noticeText);
      // An error notice must be visibly more than its bare body: the severity rides along.
      expect(noticeMessage.text).not.toBe(noticeText);
      // Nothing ran: a notice-only outcome may not open a progress card.
      expect(transport.progressCardSends).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }

    const store = await openChannelStore(join(harness.root, "state"));
    try {
      const record = store.getLatestTask(harness.bot.uid, task);
      expect(record?.state).toBe("completed");
      // A notice is presentation, not turn output: the task result stays empty.
      expect(record?.resultText).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    await rm(harness.root, { recursive: true, force: true });
  }
});

test("assistant text and repeated notices stay separate messages and a replayed notice id is not duplicated", async () => {
  const transport = new PresentationTransport();
  const firstBody = "first assistant segment\nsecond assistant segment";
  const secondBody = "second assistant message closes the turn";
  const noticeText = "Autoresearch enabled—describe what to optimize in your next message.";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit }) => {
      emit({ type: "turn_start", turnId: "notice-mixed-turn", atMs: 1 });
      emit(assistantText("assistant-first", "first assistant segment", false));
      emit(noticeEvent("autoresearch-notify-enabled", noticeText, "info"));
      emit(assistantText("assistant-first", firstBody, true));
      // The stream may replay the same notify frame id; the chat still shows one message.
      emit(noticeEvent("autoresearch-notify-enabled", noticeText, "info"));
      emit(assistantText("assistant-second", secondBody, true));
      emit({ type: "turn_end", turnId: "notice-mixed-turn", atMs: 2 });
    },
  });
  const task = inbound({
    messageId: "notice-interleaved",
    channelId: "dm-notice-interleaved",
    text: "/autoresearch tune autoresearch.sh",
  });
  try {
    try {
      await harness.service.handleInbound(task);
      await waitForStatus(
        harness.service,
        task,
        (status) => status.state === "completed" && status.outbox === 0,
      );

      const delivered = transport.listRemoteText(task);
      expect(delivered).toHaveLength(3);
      const noticeMessages = delivered.filter(({ text }) => text.includes(noticeText));
      expect(noticeMessages).toHaveLength(1);
      const noticeMessage = noticeMessages[0]!;
      expect(noticeMessage.text.indexOf(noticeText)).toBe(noticeMessage.text.lastIndexOf(noticeText));
      expect(delivered.some(({ text }) => text === firstBody)).toBe(true);
      expect(delivered.some(({ text }) => text === secondBody)).toBe(true);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }

    const store = await openChannelStore(join(harness.root, "state"));
    try {
      const record = store.getLatestTask(harness.bot.uid, task);
      // Notices stay out of the assistant result even when interleaved with real output.
      expect(record?.resultText).toBe(`${firstBody}\n\n${secondBody}`);
    } finally {
      store.close();
    }
  } finally {
    await rm(harness.root, { recursive: true, force: true });
  }
}, 10_000);

test("a notice pending in the outbox survives a service restart without rerunning the task", async () => {
  const bot = { uid: "bot-notice-restart", name: "Notice Bot" };
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-notice-restart-")));
  const noticeText = "Autoresearch mode disabled";
  const message = inbound({
    messageId: "notice-restart",
    channelId: "dm-notice-restart",
    text: "/autoresearch off",
  });
  try {
    const offline = new FakeTransport({ bot, online: false, startOnline: false });
    const first = await createCoreHarness({
      root,
      bot,
      transport: offline,
      prompt: async ({ emit }) => {
        // `/autoresearch off` is local-only: one notify frame, no agent turn.
        emit(noticeEvent("autoresearch-notify-disabled", noticeText, "info"));
      },
    });
    try {
      await first.service.handleInbound(message);
      await first.workers.waitForPromptCount(1);
      const stalled = await waitForStatus(
        first.service,
        message,
        (status) => status.state === "completed",
      );
      // The offline transport keeps the notice parked in the durable outbox.
      expect(stalled.outbox).toBeGreaterThan(0);
    } finally {
      await closeCoreHarness(first, { remove: false });
    }

    const online = new FakeTransport({ bot });
    const second = await createCoreHarness({ root, bot, transport: online });
    try {
      await waitForStatus(second.service, message, (status) => status.outbox === 0);
      expect(
        online.listRemoteText(message).filter(({ text }) => text.includes(noticeText)),
      ).toHaveLength(1);
      // Recovery replays the outbox row it found; it never respawns the settled run.
      expect(second.workers.prompts).toHaveLength(0);
    } finally {
      await closeCoreHarness(second);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a notice arriving after /interrupt is delivered without flipping the turn to completed", async () => {
  const transport = new PresentationTransport();
  const noticeText = "Autoresearch mode disabled";
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, waitForAbort }) => {
      emit({ type: "turn_start", turnId: "notice-abort-turn", atMs: 1 });
      await waitForAbort();
      // A notify frame that lands while the turn winds down is information, not work.
      emit(noticeEvent("autoresearch-notify-abort", noticeText, "info"));
      emit({ type: "turn_end", turnId: "notice-abort-turn", atMs: 2 });
    },
  });
  const task = inbound({
    messageId: "notice-abort-task",
    channelId: "dm-notice-abort",
    text: "hold the turn",
  });
  try {
    await harness.service.handleInbound(task);
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({
        messageId: "notice-abort-command",
        channelId: task.channelId,
        channelType: task.channelType,
        text: "/interrupt",
      }),
    );
    const settled = await waitForStatus(
      harness.service,
      task,
      (status) => status.state === "interrupted" || status.state === "completed",
    );
    expect(settled.state).toBe("interrupted");
    // Informational UI still reaches the chat as its own message.
    await transport.waitForRemoteText(
      (message) =>
        message.target.channelType === task.channelType
        && message.target.channelId === task.channelId
        && message.text.includes(noticeText),
    );
    expect(
      transport.listRemoteText(task).filter(({ text }) => text.includes(noticeText)),
    ).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

 
