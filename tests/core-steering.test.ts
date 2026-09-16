import { expect, test } from "bun:test";
import { join } from "node:path";
import type {
  OmpWorker,
  PromptContext,
  WorkerEvent,
  WorkerOptions,
} from "../src/contracts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness } from "./fixtures/core-fakes";

function gate(): { readonly promise: Promise<void>; release(): void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

/** Models an OMP worker that refuses steering, forcing the durable-task fallback. */
class RefusingSteerFactory extends FakeWorkerFactory {
  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const worker = await super.create(options);
    return {
      get sessionFile() {
        return worker.sessionFile;
      },
      get sessionId() {
        return worker.sessionId;
      },
      prompt: (text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext) =>
        context === undefined ? worker.prompt(text, onEvent) : worker.prompt(text, onEvent, context),
      steer: async () => {
        throw new Error("OMP worker is not streaming a turn");
      },
      abortTurn: () => worker.abortTurn(),
      refreshState: () => worker.refreshState(),
      stop: () => worker.stop(),
    };
  }
}

test("a message during a run steers that turn instead of becoming a task", async () => {
  const started = gate();
  const harness = await createCoreHarness({
    prompt: async ({ append, text, waitForAppend }) => {
      await append("steer.log", `start:${text}\n`);
      started.release();
      const appended = await waitForAppend();
      await append("steer.log", `steered:${appended.text}\n`);
      await append("steer.log", `end:${text}\n`);
    },
  });
  try {
    const target = { channelId: "dm-steer", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "steer-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "steer-first", ...target, text: "first task" }),
    );
    await started.promise;

    await harness.service.handleInbound(
      inbound({ messageId: "steer-second", ...target, text: "second note" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "steer-second", ...target, text: "second note" }),
    );
    await first;

    const status = await waitForStatus(harness.service, target, (next) => next.state === "completed");
    expect(status.state).toBe("completed");
    expect(status.queued).toBe(0);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["first task"]);
    expect(await readText(join(harness.root, "tmp", "steer.log"))).toBe(
      "start:first task\nsteered:second note\nend:first task\n",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a steered message reports the moment it joins the running turn", async () => {
  const started = gate();
  const harness = await createCoreHarness({
    prompt: async ({ emit, text, waitForAppend }) => {
      if (text !== "first task") return;
      started.release();
      const appended = await waitForAppend();
      emit({ type: "steer_consumed", text: appended.text, atMs: Date.now() });
    },
  });
  try {
    const target = { channelId: "dm-steer-receipt", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "receipt-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "receipt-first", ...target, text: "first task" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "receipt-second", ...target, text: "second note with a longer body" }),
    );
    await first;
    await waitForStatus(harness.service, target, (next) => next.state === "completed");

    expect(
      harness.transport.sent
        .map(({ text }) => text)
        .filter((text) => text.startsWith("已并入当前回合")),
    ).toEqual(["已并入当前回合 ▸ second note with a l…"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("queue mode turns a message during a run into a task that follows it", async () => {
  const started = gate();
  const release = gate();
  const harness = await createCoreHarness({
    prompt: async ({ append, text, onStop }) => {
      if (text !== "hold") return;
      started.release();
      onStop(() => release.release());
      await append("queue.log", `start:${text}\n`);
      await release.promise;
    },
  });
  try {
    const target = { channelId: "dm-queue-mode", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "queue-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queue-mode", ...target, text: "/mode queue" }),
    );
    expect((await harness.service.status(target)).mode).toBe("queue");

    const first = harness.service.handleInbound(
      inbound({ messageId: "queue-first", ...target, text: "hold" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "queue-second", ...target, text: "second task" }),
    );
    expect((await harness.service.status(target)).queued).toBe(1);

    release.release();
    await first;
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["hold", "second task"]);
    expect(await readText(join(harness.root, "tmp", "queue.log"))).toBe("start:hold\n");
    // The message that waited for the run gets one start receipt; the immediate one gets none.
    expect(
      harness.transport.sent
        .map(({ text }) => text)
        .filter((text) => text.startsWith("开始执行排队消息")),
    ).toEqual(["开始执行排队消息 ▸ second task"]);
  } finally {
    release.release();
    await closeCoreHarness(harness);
  }
});

test("/queue forces a task behind the run even in steer mode", async () => {
  const started = gate();
  const release = gate();
  const harness = await createCoreHarness({
    prompt: async ({ append, text, waitForAppend, onStop }) => {
      if (text !== "hold") return;
      started.release();
      onStop(() => release.release());
      let appended = "none";
      try {
        appended = (await waitForAppend()).text;
      } catch {
        appended = "closed";
      }
      await append("force-queue.log", `start:${text}:${appended}\n`);
      await release.promise;
    },
  });
  try {
    const target = { channelId: "dm-force-queue", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "force-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "force-first", ...target, text: "hold" }),
    );
    await started.promise;

    await harness.service.handleInbound(
      inbound({ messageId: "force-queued", ...target, text: "/queue queued note" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "force-steered", ...target, text: "steered note" }),
    );
    release.release();
    await first;
    await harness.workers.waitForPromptCount(2);

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["hold", "queued note"]);
    expect(await readText(join(harness.root, "tmp", "force-queue.log"))).toBe(
      "start:hold:steered note\n",
    );
  } finally {
    release.release();
    await closeCoreHarness(harness);
  }
});

test("a refused steer falls back to a durable task instead of dropping the message", async () => {
  const started = gate();
  const release = gate();
  const workers = new RefusingSteerFactory(async ({ text, onStop }) => {
    if (text !== "hold") return;
    started.release();
    onStop(() => release.release());
    await release.promise;
  });
  const harness = await createCoreHarness({ workers });
  try {
    const target = { channelId: "dm-steer-refused", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "refused-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "refused-first", ...target, text: "hold" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "refused-second", ...target, text: "kept as a task" }),
    );
    expect((await harness.service.status(target)).queued).toBe(1);

    release.release();
    await first;
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "hold",
      "kept as a task",
    ]);
  } finally {
    release.release();
    await closeCoreHarness(harness);
  }
});

test("/interrupt aborts the running turn without stopping the worker", async () => {
  const started = gate();
  const harness = await createCoreHarness({
    prompt: async ({ append, isStopped, text, waitForAbort }) => {
      await append("interrupt.log", `start:${text}\n`);
      started.release();
      await waitForAbort();
      await append("interrupt.log", `aborted:${text}:stopped=${isStopped()}\n`);
    },
  });
  try {
    const target = { channelId: "dm-interrupt", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "interrupt-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "interrupt-run", ...target, text: "long task" }),
    );
    await started.promise;
    expect(harness.workers.workers.at(-1)?.stopped).toBe(false);

    await harness.service.handleInbound(
      inbound({ messageId: "interrupt-command", ...target, text: "/interrupt" }),
    );
    await first;

    const status = await waitForStatus(
      harness.service,
      target,
      (next) => next.state === "interrupted",
    );
    expect(status.state).toBe("interrupted");
    expect(status.queued).toBe(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(await readText(join(harness.root, "tmp", "interrupt.log"))).toBe(
      "start:long task\naborted:long task:stopped=false\n",
    );
    expect(
      harness.transport.sent.some(({ text }) => text.includes("已中断当前回合")),
    ).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/interrupt keeps queued work running and reports completion when it produces output", async () => {
  const started = gate();
  const harness = await createCoreHarness({
    prompt: async ({ append, emit, text, waitForAbort }) => {
      if (text !== "hold") {
        await append("interrupt-queue.log", `queued:${text}\n`);
        return;
      }
      started.release();
      await waitForAbort();
      emit({ type: "text", messageId: "message-1", text: "continued after abort", final: true } as WorkerEvent);
      await append("interrupt-queue.log", "continued\n");
    },
  });
  try {
    const target = { channelId: "dm-interrupt-queue", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "iq-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "iq-hold", ...target, text: "hold" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "iq-queued", ...target, text: "/queue follow up" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "iq-interrupt", ...target, text: "/interrupt" }),
    );
    await first;
    await harness.workers.waitForPromptCount(2);

    const status = await waitForStatus(harness.service, target, (next) => next.state === "completed");
    expect(status.state).toBe("completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["hold", "follow up"]);
    expect(harness.transport.sent.some(({ text }) => text === "continued after abort")).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/interrupt without a running turn reports nothing to interrupt", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-interrupt-idle", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "idle-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "idle-interrupt", ...target, text: "/interrupt" }),
    );
    expect(
      harness.transport.sent.some(({ text }) => text.includes("没有正在运行的回合")),
    ).toBe(true);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/stop cancels the running task and clears queued work like /cancel", async () => {
  const started = gate();
  const release = gate();
  const harness = await createCoreHarness({
    prompt: async ({ text, onStop }) => {
      if (text !== "hold") return;
      started.release();
      onStop(() => release.release());
      await release.promise;
    },
  });
  try {
    const target = { channelId: "dm-stop", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "stop-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "stop-hold", ...target, text: "hold" }),
    );
    await started.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "stop-queued", ...target, text: "/queue queued work" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "stop-command", ...target, text: "/stop" }),
    );
    release.release();
    await first;

    const status = await waitForStatus(
      harness.service,
      target,
      (next) => next.state === "cancelled" && next.queued === 0,
    );
    expect(status.state).toBe("cancelled");
    expect(status.queued).toBe(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(harness.transport.sent.some(({ text }) => text.includes("已取消"))).toBe(true);
  } finally {
    release.release();
    await closeCoreHarness(harness);
  }
});

test("append mode is per chat and survives a restart", async () => {
  const target = { channelId: "dm-mode-durable", channelType: 1 as const };
  const other = { channelId: "dm-mode-other", channelType: 1 as const };
  const first = await createCoreHarness();
  let restarted: CoreHarness | undefined;
  let firstClosed = false;
  try {
    expect((await first.service.status(target)).mode).toBe("steer");
    await first.service.handleInbound(
      inbound({ messageId: "mode-bind", ...target, text: "/new" }),
    );
    await first.service.handleInbound(
      inbound({ messageId: "mode-set", ...target, text: "/mode queue" }),
    );
    await first.service.handleInbound(
      inbound({ messageId: "mode-report", ...target, text: "/mode" }),
    );
    await first.service.handleInbound(
      inbound({ messageId: "mode-status", ...target, text: "/status" }),
    );

    expect((await first.service.status(target)).mode).toBe("queue");
    expect((await first.service.status(other)).mode).toBe("steer");
    expect(first.transport.sent.some(({ text }) => text.includes("当前追加模式：queue"))).toBe(true);
    expect(first.transport.sent.some(({ text }) => text.includes("mode=queue"))).toBe(true);
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;

    restarted = await createCoreHarness({ root: first.root });
    expect((await restarted.service.status(target)).mode).toBe("queue");
    expect((await restarted.service.status(other)).mode).toBe("steer");
  } finally {
    if (!firstClosed) await closeCoreHarness(first, { remove: false });
    if (restarted) await closeCoreHarness(restarted);
    else await closeCoreHarness(first);
  }
});
