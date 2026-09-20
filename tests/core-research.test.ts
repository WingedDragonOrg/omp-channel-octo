import { expect, test } from "bun:test";
import { readFile, writeFile } from "node:fs/promises";
import type { ChatTarget, ProgressCard, SendReceipt, WorkerEvent } from "../src/contracts.ts";
import { researchReportsRoot } from "../src/research-report.ts";
import type { ResearchSnapshot } from "../src/research.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const baseResearch = (overrides: Partial<ResearchSnapshot> = {}): ResearchSnapshot => ({
  sessionId: 7,
  name: "lr-sweep",
  goal: "minimize validation loss",
  segment: 1,
  metricName: "val_loss",
  metricUnit: "loss",
  direction: "lower",
  baseline: 12,
  best: 12,
  completedRuns: 0,
  maxExperiments: 8,
  phase: "running",
  activeRun: null,
  latestMetric: null,
  recent: [],
  totalRuns: 0,
  ...overrides,
});

const researchUpdate = (
  research: ResearchSnapshot,
  report?: string,
): WorkerEvent => ({
  type: "research_update",
  atMs: Date.now(),
  research,
  ...(report === undefined ? {} : { report }),
});

const toolStart = (toolCallId: string): WorkerEvent =>
  ({ type: "tool_start", toolCallId, toolName: "Bash", atMs: Date.now() });

function gate(): { readonly promise: Promise<void>; release(): void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

/** Polls until `predicate` holds; resolves false on timeout so failures read as assertions. */
async function waitUntil(predicate: () => boolean, timeoutMs = 2_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  return predicate();
}

interface ProgressCardSend {
  messageId: string;
  progress: ProgressCard;
}

interface ProgressCardEdit {
  messageId: string;
  progress: ProgressCard;
  cardSeq: number;
}

/** A transport that carries the progress card the way production does: one send, then edits. */
class ResearchCardTransport extends FakeTransport {
  readonly progressCardSends: ProgressCardSend[] = [];
  readonly progressCardEdits: ProgressCardEdit[] = [];

  override async sendProgressCard(
    _target: ChatTarget,
    progress: ProgressCard,
    _clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    if (signal?.aborted) throw new Error("fixture progress card aborted");
    const messageId = `research-card-${this.progressCardSends.length + 1}`;
    this.progressCardSends.push({ messageId, progress });
    return { messageId };
  }

  override async editProgressCard(
    _target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) throw new Error("fixture progress card edit aborted");
    this.progressCardEdits.push({ messageId, progress, cardSeq });
  }

  /** The card's final visible payload: its send plus every edit, in order. */
  lastFrame(messageId: string): ProgressCard {
    const frames: ProgressCard[] = [
      ...this.progressCardSends.filter((send) => send.messageId === messageId).map(({ progress }) => progress),
      ...this.progressCardEdits.filter((edit) => edit.messageId === messageId).map(({ progress }) => progress),
    ];
    return frames.at(-1)!;
  }
}

test("research updates start one progress card and update its rounds in place", async () => {
  const transport = new ResearchCardTransport();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, text }) => {
      if (text !== "run the sweep") return;
      emit(researchUpdate(baseResearch({ activeRun: 1 })));
      emit(researchUpdate(baseResearch({
        completedRuns: 2,
        latestMetric: 9.5,
        best: 9.5,
        totalRuns: 2,
        recent: [
          { runNumber: 1, segment: 1, metric: 10, status: "keep", description: "lr=0.10", flagged: false },
          // Legacy rows carry no run number; they still count and still render.
          { runNumber: null, segment: 1, metric: 9.5, status: "keep", description: "lr=0.05", flagged: false },
        ],
      })));
    },
  });
  try {
    const target = { channelId: "dm-research-card", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "research-card-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "research-card-task", ...target, text: "run the sweep" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => transport.progressCardEdits.some((edit) => edit.progress.state !== "running"));

    expect(transport.progressCardSends).toHaveLength(1);
    const send = transport.progressCardSends[0]!;
    // The card starts from the research event itself, showing the live run.
    expect(send.progress.research?.activeRun).toBe(1);
    expect(transport.progressCardEdits.length).toBeGreaterThanOrEqual(1);
    for (const edit of transport.progressCardEdits) {
      expect(edit.messageId).toBe(send.messageId);
    }
    const research = transport.lastFrame(send.messageId).research;
    expect(research).toMatchObject({
      name: "lr-sweep",
      metricName: "val_loss",
      direction: "lower",
      baseline: 12,
      best: 9.5,
      completedRuns: 2,
      latestMetric: 9.5,
      totalRuns: 2,
      phase: "running",
    });
    // The unnumbered legacy row survives the round-trip instead of being dropped.
    expect(research?.recent).toHaveLength(2);
    expect(research?.recent[1]?.runNumber).toBeNull();
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a steered roll keeps the research band on the next card; a fresh task starts without one", async () => {
  const transport = new ResearchCardTransport();
  const started = gate();
  const afterRoll = gate();
  const harness = await createCoreHarness({
    transport,
    prompt: async ({ emit, text, waitForAppend, waitForAbort }) => {
      if (text === "research run") {
        emit(researchUpdate(baseResearch({
          completedRuns: 1,
          latestMetric: 10,
          best: 10,
          totalRuns: 1,
          recent: [
            { runNumber: 1, segment: 1, metric: 10, status: "keep", description: "lr=0.10", flagged: false },
          ],
        })));
        started.release();
        // A failure before the steer must not pin the turn on waitForAppend: stop() fires
        // the interrupt waiter, and the prompt exits instead of stalling harness teardown.
        const arrival = await Promise.race([
          waitForAppend().then(() => "steered"),
          waitForAbort().then(() => "aborted"),
        ]);
        if (arrival !== "steered") return;
        // The steer resolves waitForAppend in the same tick it lands, so round-two events
        // could run before the service finished rolling the card. Wait for the observed
        // roll instead of racing it.
        await afterRoll.promise;
        emit(toolStart("tc-rolled"));
        return;
      }
      if (text === "independent chore") {
        emit(toolStart("tc-plain"));
      }
    },
  });
  try {
    const target = { channelId: "dm-research-roll", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "research-roll-bind", ...target, text: "/new" }),
    );
    const first = harness.service.handleInbound(
      inbound({ messageId: "research-roll-task", ...target, text: "research run" }),
    );
    await started.promise;
    // Barrier one: the research card actually exists before its owner gets steered.
    const cardReady = await waitUntil(() => transport.progressCardSends.length === 1);
    expect(cardReady).toBe(true);
    await harness.service.handleInbound(
      inbound({ messageId: "research-roll-steer", ...target, text: "more context" }),
    );
    // Barrier two: the roll really happened — the first card closed with a terminal edit.
    const rollDone = await waitUntil(() => transport.progressCardEdits.some(
      (edit) => edit.messageId === transport.progressCardSends[0]!.messageId
        && edit.progress.state !== "running",
    ));
    expect(rollDone).toBe(true);
    afterRoll.release();
    await first;
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(
      inbound({ messageId: "research-roll-plain", ...target, text: "independent chore" }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => transport.progressCardSends.length === 3
      && transport.progressCardSends.every(
        (send) => transport.progressCardEdits.some(
          (edit) => edit.messageId === send.messageId && edit.progress.state !== "running",
        ),
      ));

    expect(transport.progressCardSends).toHaveLength(3);
    const rolled = transport.lastFrame(transport.progressCardSends[1]!.messageId).research;
    expect(rolled).toMatchObject({ completedRuns: 1, latestMetric: 10, best: 10 });
    expect(transport.lastFrame(transport.progressCardSends[2]!.messageId).research).toBeUndefined();
  } finally {
    afterRoll.release();
    await closeCoreHarness(harness);
  }
});

const REPORT_ALPHA = [
  "# 研究完整记录",
  "",
  "目标: SECRET-ALPHA-MARKER 最小化 val_loss",
  "",
  "| # | val_loss | 结论 |",
  "| --- | --- | --- |",
  "| 1 | 10.000 | keep |",
  "| 未编号 | 9.500 | keep |",
  "",
].join("\n");

test("/autoresearch-history exports the running chat's latest report without touching the model", async () => {
  const busy = gate();
  const emitted = gate();
  const harness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text !== "run the study") return;
      emit(researchUpdate(baseResearch({ completedRuns: 2, latestMetric: 9.5, best: 9.5, totalRuns: 2 }), REPORT_ALPHA));
      emitted.release();
      await busy.promise;
    },
  });
  const deliverTask = async (): Promise<void> => {
    await harness.service.handleInbound(
      inbound({ messageId: "alpha-bind", channelId: "dm-alpha", channelType: 1, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "alpha-task", channelId: "dm-alpha", channelType: 1, text: "run the study" }),
    );
  };
  try {
    const target = { channelId: "dm-alpha", channelType: 1 as const };
    const task = deliverTask();
    await emitted.promise;
    await harness.service.handleInbound(
      inbound({ messageId: "alpha-export", ...target, text: "/autoresearch-history" }),
    );
    const delivered = await waitUntil(() => harness.transport.sentFiles.length === 1);
    expect(delivered).toBe(true);
    const file = harness.transport.sentFiles[0]!;
    expect(file.target).toMatchObject(target);
    expect(file.kind).toBe("file");
    // The export carries the report bytes verbatim, read back from the real file on disk.
    expect(await readFile(file.path, "utf8")).toBe(REPORT_ALPHA);
    // The command never became steering text or a second prompt.
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["run the study"]);
    await task;
  } finally {
    busy.release();
    await closeCoreHarness(harness);
  }
});

test("a redelivered /autoresearch-history ships the report once", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text !== "measure the baseline") return;
      emit(researchUpdate(baseResearch({ completedRuns: 1, latestMetric: 10, best: 10, totalRuns: 1 }), REPORT_ALPHA));
    },
  });
  try {
    const target = { channelId: "dm-alpha-redeliver", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "redeliver-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "redeliver-task", ...target, text: "measure the baseline" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(
      inbound({ messageId: "redeliver-export", ...target, text: "/autoresearch-history" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "redeliver-export", ...target, text: "/autoresearch-history" }),
    );
    const delivered = await waitUntil(() => harness.transport.sentFiles.length === 1);
    expect(delivered).toBe(true);
    expect(harness.transport.sentFiles).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("members and unrelated chats cannot pull a chat's research report", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text !== "run the study") return;
      emit(researchUpdate(baseResearch({ completedRuns: 2, latestMetric: 9.5, best: 9.5, totalRuns: 2 }), REPORT_ALPHA));
    },
  });
  try {
    const researchChat = { channelId: "dm-secret-alpha", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "secret-bind", ...researchChat, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "secret-task", ...researchChat, text: "run the study" }),
    );
    await waitForStatus(harness.service, researchChat, (status) => status.state === "completed");

    const group = { channelId: "group-allowed", channelType: 2 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "secret-member",
        ...group,
        senderId: "member-9",
        mentionedUserIds: [harness.bot.uid],
        text: "/autoresearch-history",
      }),
    );
    const otherChat = { channelId: "dm-secret-other", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "secret-other-export", ...otherChat, text: "/autoresearch-history" }),
    );

    const groupNotice = await waitUntil(
      () => harness.transport.sent.some(
        ({ target, text }) => target.channelId === group.channelId && text.length > 0,
      ),
    );
    expect(groupNotice).toBe(true);
    const groupTexts = harness.transport.sent
      .filter(({ target }) => target.channelId === group.channelId)
      .map(({ text }) => text);
    expect(groupTexts.join("\n")).not.toContain("SECRET-ALPHA-MARKER");
    expect(harness.transport.sentFiles.some(({ target }) => target.channelId === group.channelId)).toBe(false);

    const hintArrived = await waitUntil(
      () => harness.transport.sent.some(({ target }) => target.channelId === otherChat.channelId),
    );
    expect(hintArrived).toBe(true);
    const otherTexts = harness.transport.sent
      .filter(({ target }) => target.channelId === otherChat.channelId)
      .map(({ text }) => text);
    expect(otherTexts.join("\n")).not.toContain("SECRET-ALPHA-MARKER");
    expect(harness.transport.sentFiles.some(({ target }) => target.channelId === otherChat.channelId)).toBe(false);
    // Only the research task ever prompted the model.
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["run the study"]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a chat with no research gets a short hint instead of a model call", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-no-research", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "empty-bind", ...target, text: "/new" }),
    );
    // Let the /new receipt drain so only the export can add a reply afterwards.
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    const sentBefore = harness.transport.sent.length;
    await harness.service.handleInbound(
      inbound({ messageId: "empty-export", ...target, text: "/autoresearch-history" }),
    );
    const hintArrived = await waitUntil(() => harness.transport.sent.length > sentBefore);
    expect(hintArrived).toBe(true);
    expect(harness.transport.sentFiles).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

const REPORT_RESTART = [
  "# 研究完整记录",
  "",
  "目标: RESTART-MARKER 最小化 val_loss",
  "",
  "| # | val_loss | 结论 |",
  "| --- | --- | --- |",
  "| 1 | 11.250 | keep |",
  "",
].join("\n");

test("a restarted channel still exports the report the previous process persisted", async () => {
  const firstHarness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text !== "survive a restart") return;
      emit(researchUpdate(baseResearch({ completedRuns: 1, latestMetric: 11.25, best: 11.25, totalRuns: 1 }), REPORT_RESTART));
    },
  });
  const target = { channelId: "dm-restart-research", channelType: 1 as const };
  try {
    await firstHarness.service.handleInbound(
      inbound({ messageId: "persist-bind", ...target, text: "/new" }),
    );
    await firstHarness.service.handleInbound(
      inbound({ messageId: "persist-task", ...target, text: "survive a restart" }),
    );
    await waitForStatus(firstHarness.service, target, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(firstHarness, { remove: false });
  }

  const restarted = await createCoreHarness({ root: firstHarness.root, bot: firstHarness.bot });
  try {
    await restarted.service.handleInbound(
      inbound({ messageId: "persist-export", ...target, text: "/autoresearch-history" }),
    );
    const delivered = await waitUntil(() => restarted.transport.sentFiles.length === 1);
    expect(delivered).toBe(true);
    const file = restarted.transport.sentFiles[0]!;
    expect(file.kind).toBe("file");
    expect(await readFile(file.path, "utf8")).toBe(REPORT_RESTART);
    expect(restarted.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a report write failure never takes the producing task down", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text !== "study with broken storage") return;
      emit(researchUpdate(baseResearch({ completedRuns: 1, latestMetric: 10, best: 10, totalRuns: 1 }), REPORT_ALPHA));
      emit(assistantText("analysis-1", "analysis still delivered", true));
    },
  });
  try {
    // The reports root is a plain file: every ordered write fails before its first fs step.
    await writeFile(researchReportsRoot(harness.stateDir), "blocked", { mode: 0o600 });
    const target = { channelId: "dm-broken-reports", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "broken-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "broken-task", ...target, text: "study with broken storage" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(
      harness.transport.sent
        .filter(({ target: sent }) => sent.channelId === target.channelId)
        .map(({ text }) => text),
    ).toContain("analysis still delivered");
  } finally {
    await closeCoreHarness(harness);
  }
});

const REPORT_V1 = [
  "# 研究完整记录",
  "",
  "目标: SPOOL-V1-MARKER 最小化 val_loss",
  "",
  "| # | val_loss | 结论 |",
  "| --- | --- | --- |",
  "| 1 | 12.750 | keep |",
  "",
].join("\n");

const REPORT_V2 = [
  "# 研究完整记录",
  "",
  "目标: SPOOL-V2-MARKER 最小化 val_loss",
  "",
  "| # | val_loss | 结论 |",
  "| --- | --- | --- |",
  "| 1 | 12.750 | keep |",
  "| 2 | 9.125 | keep |",
  "",
].join("\n");

test("an enqueued export keeps its bytes when a newer report lands", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ emit, text }) => {
      if (text === "first study") {
        emit(researchUpdate(baseResearch({ completedRuns: 1, latestMetric: 12.75, best: 12.75, totalRuns: 1 }), REPORT_V1));
      }
      if (text === "second study") {
        emit(researchUpdate(baseResearch({ name: "gamma", completedRuns: 2, latestMetric: 9.125, best: 9.125, totalRuns: 2 }), REPORT_V2));
      }
    },
  });
  try {
    const target = { channelId: "dm-spool", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "spool-bind", ...target, text: "/new" }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "spool-first", ...target, text: "first study" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(
      inbound({ messageId: "spool-export-v1", ...target, text: "/autoresearch-history" }),
    );
    const firstDelivered = await waitUntil(() => harness.transport.sentFiles.length === 1);
    expect(firstDelivered).toBe(true);
    const firstExport = harness.transport.sentFiles[0]!;
    expect(await readFile(firstExport.path, "utf8")).toBe(REPORT_V1);

    await harness.service.handleInbound(
      inbound({ messageId: "spool-second", ...target, text: "second study" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await harness.service.handleInbound(
      inbound({ messageId: "spool-export-v2", ...target, text: "/autoresearch-history" }),
    );
    const secondDelivered = await waitUntil(() => harness.transport.sentFiles.length === 2);
    expect(secondDelivered).toBe(true);
    expect(await readFile(harness.transport.sentFiles[1]!.path, "utf8")).toBe(REPORT_V2);
    // The queued attachment was an immutable spool copy: the rewrite never reached it.
    const archived = await readFile(firstExport.path, "utf8");
    expect(archived).toBe(REPORT_V1);
    expect(archived).not.toContain("SPOOL-V2-MARKER");
  } finally {
    await closeCoreHarness(harness);
  }
});
