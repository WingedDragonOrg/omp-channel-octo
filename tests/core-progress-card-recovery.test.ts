import { expect, test } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BotIdentity, ChatTarget, ProgressCard, SendReceipt } from "../src/contracts";
import { createExecutionTracker, encodeExecutionSnapshot } from "../src/execution-card";
import { ChannelStore, type InboundClaim, type ProgressCardRecord, type TaskRecord } from "../src/store";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";

type RecordedCardEdit = {
  target: ChatTarget;
  messageId: string;
  progress: ProgressCard;
  cardSeq: number;
};

class RecoveryTransport extends FakeTransport {
  readonly progressCardEdits: RecordedCardEdit[] = [];
  readonly progressCardSends: Array<{
    target: ChatTarget;
    progress: ProgressCard;
    clientMsgNo: string;
    messageId: string;
  }> = [];
  failProgressCardEdits = false;
  override async sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
  ): Promise<SendReceipt | null> {
    const messageId = `card-${this.progressCardSends.length + 1}`;
    this.progressCardSends.push({ target, progress: { ...progress }, clientMsgNo, messageId });
    return { messageId };
  }
  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
  ): Promise<void> {
    this.progressCardEdits.push({ target, messageId, progress: { ...progress }, cardSeq });
    if (this.failProgressCardEdits) throw new Error("fixture recovery edit unavailable");
  }
}

type SeededCard = {
  taskId: string;
  messageId: string;
  target: ChatTarget;
};

/**
 * Writes the state a process that died mid-turn leaves behind: the task it was running plus the
 * card frame the server had already accepted. `snapshot` overrides the stored payload verbatim so
 * a test can plant a payload the service must reject.
 */
async function seedOpenCard(
  root: string,
  botUid: string,
  options: {
    cardSeq: number;
    taskState: "started" | "completed" | "failed";
    tools: number;
    snapshot?: string;
  },
): Promise<SeededCard> {
  const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
  try {
    store.setBotUid(botUid);
    store.setOwnerId(botUid, "owner-1");
    const target: ChatTarget = { channelId: "dm-owner", channelType: 1 };
    const taskId = `interrupted-${options.taskState}-${options.cardSeq}`;
    const claim: InboundClaim = {
      botUid,
      target,
      messageId: `message-${taskId}`,
      senderId: "owner-1",
      text: "fixture work",
      supported: true,
      kind: "task",
    };
    store.ensureBinding({
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      sessionId: "session-1",
      cwd: root,
      sessionFile: null,
    });
    const task: TaskRecord = {
      id: taskId,
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId: claim.messageId,
      senderId: "owner-1",
      text: claim.text,
      sessionId: "session-1",
      cwd: root,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: true,
    };
    expect(store.acceptTask(claim, task)).toBe(true);
    expect(store.markStarted(taskId)).toBe(true);
    if (options.taskState === "completed") store.markCompleted(taskId, "done");
    if (options.taskState === "failed") store.markFailed(taskId, "boom");

    const tracker = createExecutionTracker({ runId: taskId, initialAtMs: 1_000 });
    tracker.apply({ type: "turn_start", turnId: 1, atMs: 1_000 });
    for (let index = 0; index < options.tools; index += 1) {
      tracker.apply({
        type: "tool_start",
        toolCallId: `call-${index}`,
        toolName: "bash",
        atMs: 2_000 + index,
        summary: `fixture step ${index}`,
      });
      tracker.apply({
        type: "tool_end",
        toolCallId: `call-${index}`,
        atMs: 2_100 + index,
        status: "succeeded",
        summary: `fixture step ${index}`,
      });
    }
    const messageId = `card-${taskId}`;
    store.saveProgressCard({
      botUid,
      taskId,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      lastSeq: options.cardSeq,
      snapshot: options.snapshot ?? encodeExecutionSnapshot(tracker.snapshot(3_000)),
    });
    return { taskId, messageId, target };
  } finally {
    store.close();
  }
}

async function openCards(root: string, botUid: string): Promise<ProgressCardRecord[]> {
  const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
  try {
    return store.listProgressCards(botUid);
  } finally {
    store.close();
  }
}

async function withRoot(run: (root: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-card-")));
  try {
    await run(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

/** Boots a service over an existing state directory, the way a restart does. */
async function restartWith(
  root: string,
  bot: BotIdentity,
  transport: RecoveryTransport,
): Promise<CoreHarness> {
  return createCoreHarness({ root, bot, transport });
}

async function prepareRoot(root: string): Promise<BotIdentity> {
  const first = await createCoreHarness({ root });
  const bot = first.bot;
  await closeCoreHarness(first, { remove: false });
  return bot;
}

test("a turn interrupted by a restart gets one terminal card edit", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    const seeded = await seedOpenCard(root, bot.uid, { cardSeq: 7, taskState: "started", tools: 3 });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits).toHaveLength(1);
      const edit = transport.progressCardEdits[0]!;
      expect(edit.messageId).toBe(seeded.messageId);
      expect(edit.cardSeq).toBe(8);
      expect(edit.progress.state).toBe("cancelled");
      expect(edit.progress.runId).toBe(seeded.taskId);
      expect(edit.progress.tools.map((tool) => tool.summary)).toEqual([
        "fixture step 0",
        "fixture step 1",
        "fixture step 2",
      ]);
      expect(edit.progress.endedAtMs).toBeNumber();
      expect(edit.progress.elapsedMs).toBeGreaterThan(0);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }
  });
});

test("a finished turn whose terminal edit never landed is closed as completed", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    await seedOpenCard(root, bot.uid, { cardSeq: 4, taskState: "completed", tools: 2 });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits).toHaveLength(1);
      expect(transport.progressCardEdits[0]!.progress.state).toBe("completed");
      expect(transport.progressCardEdits[0]!.cardSeq).toBe(5);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }
  });
});

test("a failed recovery keeps the row and the next start retries it", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    const seeded = await seedOpenCard(root, bot.uid, { cardSeq: 2, taskState: "started", tools: 1 });

    const failing = new RecoveryTransport({ bot });
    failing.failProgressCardEdits = true;
    const first = await restartWith(root, bot, failing);
    await closeCoreHarness(first, { remove: false });
    const afterFailure = await openCards(root, bot.uid);
    expect(afterFailure).toHaveLength(1);
    expect(afterFailure[0]!.attempts).toBe(1);
    expect(afterFailure[0]!.messageId).toBe(seeded.messageId);
    // The failed attempt may already have been accepted, so the next one must stay above it.
    expect(afterFailure[0]!.lastSeq).toBe(3);

    const retry = new RecoveryTransport({ bot });
    const second = await restartWith(root, bot, retry);
    try {
      expect(retry.progressCardEdits).toHaveLength(1);
      expect(retry.progressCardEdits[0]!.messageId).toBe(seeded.messageId);
      expect(retry.progressCardEdits[0]!.cardSeq).toBe(4);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(second, { remove: false });
    }
  });
});

test("recovery attempts stay bounded", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    await seedOpenCard(root, bot.uid, { cardSeq: 3, taskState: "failed", tools: 1 });
    let attempts = 0;
    for (let start = 0; start < 6; start += 1) {
      const transport = new RecoveryTransport({ bot });
      transport.failProgressCardEdits = true;
      const harness = await restartWith(root, bot, transport);
      await closeCoreHarness(harness, { remove: false });
      const rows = await openCards(root, bot.uid);
      if (rows.length === 0) break;
      attempts = rows[0]!.attempts;
      expect(attempts).toBe(start + 1);
    }
    expect(attempts).toBeGreaterThan(0);
    expect(attempts).toBeLessThan(6);
    expect(await openCards(root, bot.uid)).toHaveLength(0);
  });
});

test("an unusable stored payload is dropped without touching the card", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    await seedOpenCard(root, bot.uid, {
      cardSeq: 9,
      taskState: "started",
      tools: 1,
      snapshot: "{\"not\":\"a snapshot\"}",
    });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits).toHaveLength(0);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }
  });
});

test("a live turn stores its card row and clears it once the terminal edit lands", async () => {
  await withRoot(async (root) => {
    const transport = new RecoveryTransport({ bot: { uid: "bot-live-card", name: "Fixture Bot" } });
    const promptStarted = Promise.withResolvers<void>();
    const held = Promise.withResolvers<void>();
    const harness = await createCoreHarness({
      root,
      bot: transport.bot,
      transport,
      prompt: async ({ emit }) => {
        emit({
          type: "tool_start",
          toolCallId: "shell-1",
          toolName: "shell",
          atMs: 1,
          summary: "tool_execution_start shell",
        });
        promptStarted.resolve();
        await held.promise;
        emit({
          type: "tool_end",
          toolCallId: "shell-1",
          atMs: 2,
          status: "succeeded",
          summary: "tool_execution_end shell",
        });
        emit(assistantText("live-card-body", "done", true));
      },
    });
    try {
      const task = inbound({ messageId: "live-card-message", text: "run a tool" });
      await harness.service.handleInbound(task);
      await promptStarted.promise;

      const running = await openCards(root, harness.bot.uid);
      expect(running).toHaveLength(1);
      expect(running[0]!.messageId).toBe(transport.progressCardSends[0]!.messageId);
      expect(running[0]!.channelId).toBe(task.channelId);
      expect(running[0]!.channelType).toBe(task.channelType);
      expect(running[0]!.lastSeq).toBeGreaterThanOrEqual(1);
      expect(running[0]!.attempts).toBe(0);

      held.resolve();
      await waitForStatus(harness.service, task, (status) => status.state === "completed" && status.outbox === 0);
      expect(await openCards(root, harness.bot.uid)).toHaveLength(0);
    } finally {
      held.resolve();
      await closeCoreHarness(harness, { remove: false });
    }
  });
});
