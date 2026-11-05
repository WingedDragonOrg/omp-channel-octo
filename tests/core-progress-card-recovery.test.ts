import { Database } from "bun:sqlite";
import { expect, test, vi } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type { BotIdentity, ChatTarget, ChannelTransport, ProgressCard, SendReceipt } from "../src/contracts";
import { createExecutionTracker, encodeExecutionSnapshot, restoreExecutionSnapshot, restoreExecutionTracker } from "../src/cards/execution-tracker";
import { ProgressCards, type TaskProgress } from "../src/cards/progress-cards";
import { ChannelStore } from "../src/store";
import type { InboundClaim, ProgressCardRecord, TaskRecord } from "../src/store/store-records";
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
  private readonly sendWaiters: Array<() => void> = [];
  private readonly editWaiters: Array<{
    predicate: (edit: RecordedCardEdit) => boolean;
    resolve: (edit: RecordedCardEdit) => void;
  }> = [];
  override async sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
  ): Promise<SendReceipt | null> {
    const messageId = `card-${this.progressCardSends.length + 1}`;
    this.progressCardSends.push({ target, progress: { ...progress }, clientMsgNo, messageId });
    for (const resolve of this.sendWaiters.splice(0)) resolve();
    return { messageId };
  }
  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
  ): Promise<void> {
    const edit = { target, messageId, progress: { ...progress }, cardSeq };
    this.progressCardEdits.push(edit);
    for (const waiter of this.editWaiters.splice(0)) {
      if (waiter.predicate(edit)) waiter.resolve(edit);
      else this.editWaiters.push(waiter);
    }
    if (this.failProgressCardEdits) throw new Error("fixture recovery edit unavailable");
  }
  /** Awaits the first created card. The caller resumes on the frame the service recorded. */
  waitForProgressSend(): Promise<void> {
    if (this.progressCardSends.length > 0) return Promise.resolve();
    const { promise, resolve } = Promise.withResolvers<void>();
    this.sendWaiters.push(() => resolve());
    return promise;
  }

  /** Awaits the next edit matching `predicate`; an already recorded one counts. */
  waitForProgressEdit(predicate: (edit: RecordedCardEdit) => boolean): Promise<RecordedCardEdit> {
    const recorded = this.progressCardEdits.find(predicate);
    if (recorded) return Promise.resolve(recorded);
    const { promise, resolve } = Promise.withResolvers<RecordedCardEdit>();
    this.editWaiters.push({ predicate, resolve });
    return promise;
  }
}

type SeededCard = {
  taskId: string;
  messageId: string;
  /** Further cards of the same task, i.e. rounds the process died between. */
  extraMessageIds: string[];
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
    /** Cards the same task opened after this one; a run opens one per user round. */
    extraCards?: number;
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
      relaySuppressed: false,
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
    const snapshot = options.snapshot ?? encodeExecutionSnapshot(tracker.snapshot(3_000));
    store.saveProgressCard({
      botUid,
      taskId,
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      lastSeq: options.cardSeq,
      snapshot,
    });
    const extraMessageIds: string[] = [];
    for (let index = 0; index < (options.extraCards ?? 0); index += 1) {
      const extraMessageId = `${messageId}-round-${index + 2}`;
      store.saveProgressCard({
        botUid,
        taskId,
        channelType: target.channelType,
        channelId: target.channelId,
        messageId: extraMessageId,
        lastSeq: options.cardSeq,
        snapshot,
      });
      extraMessageIds.push(extraMessageId);
    }
    return { taskId, messageId, extraMessageIds, target };
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

test("recovery keeps a native commit HEAD result and an unknown outcome", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    const headResult = "提交已完成。HEAD=aaaaaaaa→bbbbbbbb。";
    const headContent = {
      message: "fix: preserve recovered commit details\n\nThe title and message body survive restart.",
      changelog: "### Fixed\n\n- Keep the committed changelog entry.",
    };
    const unknownResult = "无法确认提交进程已退出；提交结果未知。";

    const headTracker = createExecutionTracker({ runId: "recovered-commit-head", initialAtMs: 1_000 });
    headTracker.apply({ type: "commit_progress", atMs: 1_100, summary: "正在提交" });
    headTracker.apply({
      type: "commit_result",
      atMs: 2_000,
      summary: headResult,
      content: headContent,
    } as unknown as Parameters<typeof headTracker.apply>[0]);
    const headSnapshot = headTracker.apply({ type: "run_end", atMs: 2_100, state: "completed" });

    const unknownTracker = createExecutionTracker({ runId: "recovered-commit-unknown", initialAtMs: 1_000 });
    unknownTracker.apply({ type: "commit_progress", atMs: 1_100, summary: "正在提交" });
    unknownTracker.apply({ type: "commit_result", atMs: 2_000, summary: unknownResult });
    const unknownSnapshot = unknownTracker.apply({ type: "run_end", atMs: 2_100, state: "failed" });

    const headSeed = await seedOpenCard(root, bot.uid, {
      cardSeq: 11,
      taskState: "completed",
      tools: 0,
      snapshot: encodeExecutionSnapshot(headSnapshot),
    });
    const unknownSeed = await seedOpenCard(root, bot.uid, {
      cardSeq: 12,
      taskState: "failed",
      tools: 0,
      snapshot: encodeExecutionSnapshot(unknownSnapshot),
    });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits).toHaveLength(2);
      const headEdit = transport.progressCardEdits.find(
        edit => edit.messageId === headSeed.messageId,
      );
      expect(headEdit?.progress.state).toBe("completed");
      expect(headEdit?.progress.commit?.result).toBe(headResult);
      expect(headEdit?.progress.commit?.content).toEqual(headContent);

      const unknownEdit = transport.progressCardEdits.find(
        edit => edit.messageId === unknownSeed.messageId,
      );
      expect(unknownEdit?.progress.state).toBe("failed");
      expect(unknownEdit?.progress.commit?.result).toBe(unknownResult);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
    }
  });
});

test("an interrupted commit card without a recorded result closes as unknown, not stopped", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    const tracker = createExecutionTracker({ runId: "interrupted-commit", initialAtMs: 1_000 });
    tracker.apply({ type: "commit_progress", atMs: 1_100, summary: "正在提交" });
    const running = tracker.snapshot(1_800);
    const seeded = await seedOpenCard(root, bot.uid, {
      cardSeq: 13,
      taskState: "started",
      tools: 0,
      snapshot: encodeExecutionSnapshot(running),
    });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits).toHaveLength(1);
      const edit = transport.progressCardEdits[0]!;
      expect(edit.messageId).toBe(seeded.messageId);
      expect(edit.progress.state).toBe("cancelled");
      expect(edit.progress.commit?.result).toContain("提交结果未知");
      expect(edit.progress.commit?.result).not.toContain("已停止");
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

test("every card a task left open is closed on the next start", async () => {
  await withRoot(async (root) => {
    const bot = await prepareRoot(root);
    const seeded = await seedOpenCard(root, bot.uid, {
      cardSeq: 5,
      taskState: "started",
      tools: 2,
      extraCards: 1,
    });
    const transport = new RecoveryTransport({ bot });
    const harness = await restartWith(root, bot, transport);
    try {
      expect(transport.progressCardEdits.map((edit) => edit.messageId).sort()).toEqual(
        [seeded.messageId, ...seeded.extraMessageIds].sort(),
      );
      // Each card carries its own sequence, so both advance one past what they had stored.
      expect(transport.progressCardEdits.map((edit) => edit.cardSeq)).toEqual([6, 6]);
      expect(transport.progressCardEdits.map((edit) => edit.progress.state)).toEqual([
        "cancelled",
        "cancelled",
      ]);
      expect(await openCards(root, bot.uid)).toHaveLength(0);
    } finally {
      await closeCoreHarness(harness, { remove: false });
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
      await harness.service.submitInbound(task);
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

test("a database written before per-round cards keeps its rows and gains card identity", async () => {
  await withRoot(async (root) => {
    const path = join(root, "state", "channel.sqlite");
    await mkdir(dirname(path), { recursive: true });
    // The shape a run wrote before it could open several cards: one row per task.
    const legacy = new Database(path);
    legacy.exec(`
      CREATE TABLE progress_cards (
        bot_uid TEXT NOT NULL,
        task_id TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        last_seq INTEGER NOT NULL CHECK(last_seq >= 1),
        snapshot TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, task_id)
      );
      INSERT INTO progress_cards(
        bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
      ) VALUES('bot-legacy-cards', 'task-legacy', 1, 'dm-legacy', 'card-legacy-1', 4, '{"state":"running"}', 2, 7);
    `);
    legacy.close();

    const store = await ChannelStore.open(path);
    try {
      const migrated = store.listProgressCards("bot-legacy-cards");
      expect(migrated).toHaveLength(1);
      expect(migrated[0]!.messageId).toBe("card-legacy-1");
      expect(migrated[0]!.lastSeq).toBe(4);
      expect(migrated[0]!.attempts).toBe(2);

      // Cards of one task are separate rows now, so a round closes without its successor.
      store.saveProgressCard({
        botUid: "bot-legacy-cards",
        taskId: "task-legacy",
        channelType: 1,
        channelId: "dm-legacy",
        messageId: "card-legacy-2",
        lastSeq: 1,
        snapshot: "{\"state\":\"running\"}",
      });
      expect(store.listProgressCards("bot-legacy-cards")).toHaveLength(2);
      store.clearProgressCard("bot-legacy-cards", "task-legacy", "card-legacy-1");
      expect(store.listProgressCards("bot-legacy-cards").map((card) => card.messageId)).toEqual([
        "card-legacy-2",
      ]);
    } finally {
      store.close();
    }

    const reopened = await ChannelStore.open(path);
    try {
      expect(reopened.listProgressCards("bot-legacy-cards").map((card) => card.messageId)).toEqual([
        "card-legacy-2",
      ]);
    } finally {
      reopened.close();
    }
    const schema = new Database(path);
    try {
      const row = schema
        .query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'progress_cards'")
        .get() as { sql?: string } | undefined;
      expect(row?.sql ?? "").toContain("PRIMARY KEY(bot_uid, task_id, message_id)");
    } finally {
      schema.close();
    }
  });
});

/**
 * 续接契约：一个会被本进程续接的任务，其遗留卡片不被收尾，而是由续接的一轮接着编辑——同一张卡、
 * 同一个递增序号、上一进程的 todo/工具/子代理/研究原样保留。这些用例直接喂 `ProgressCards`，
 * 不经过服务与 worker 夹层：判定与去留都在卡片域自己的边界上。
 */

type CardEdit = {
  target: ChatTarget;
  messageId: string;
  progress: ProgressCard;
  cardSeq: number;
};

/** 一次卡片创建请求：恢复与续接都不该产生它。 */
type CardSend = { progress: ProgressCard; clientMsgNo: string };

/** 上一进程留下的那一轮：任务、卡片身份、它当时数到的阶段，以及那一行的载荷。 */
type SeededRound = {
  taskId: string;
  messageId: string;
  lastSeq: number;
  phaseCount: number;
  snapshot: string;
};

/** 一次续接用例的现场：卡片域本身，加上它发出的创建请求与被编辑的卡片帧。 */
type ResumeDomain = {
  cards: ProgressCards;
  sends: CardSend[];
  edits: CardEdit[];
};

/**
 * 卡片域的窄端口：`ProgressCardsDeps` 只要求这两条投递能力。`refuseFirstEdit` 复现网关对正文的
 * 那次拒绝（4xx 且没有 bot API 的 `error.code`），卡片必须改用不带步骤摘要的形态继续编辑。
 */
function cardTransport(options: { refuseFirstEdit?: boolean } = {}): {
  transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard">;
  sends: CardSend[];
  edits: CardEdit[];
} {
  const sends: CardSend[] = [];
  const edits: CardEdit[] = [];
  const transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard"> = {
    async sendProgressCard(_target, progress, clientMsgNo) {
      sends.push({ progress, clientMsgNo });
      return { messageId: `new-card-${sends.length}` };
    },
    async editProgressCard(target, messageId, progress, cardSeq) {
      const edit = { target, messageId, progress: { ...progress }, cardSeq };
      edits.push(edit);
      if (options.refuseFirstEdit && edits.length === 1) {
        throw Object.assign(new Error("gateway refused the card body"), { status: 400 });
      }
    },
  };
  return { transport, sends, edits };
}

/** 运行中的编辑由合并定时器驱动：推进假时钟让这一刻的编辑排上，再等出站链排空。 */
async function flushCardEdits(progress: TaskProgress, edits: CardEdit[], count: number): Promise<void> {
  vi.advanceTimersByTime(2_000);
  await progress.tail;
  expect(edits).toHaveLength(count);
}

const RESUMED_TARGET: ChatTarget = { channelId: "dm-owner", channelType: 1 };
const RESUMED_TODOS = [
  {
    name: "复现",
    tasks: [
      { content: "复现中断现场", status: "completed" as const },
      { content: "接管进度卡", status: "in_progress" as const },
    ],
  },
];
const RESUMED_RESEARCH = {
  sessionId: 7,
  name: "搜索加速",
  goal: "把 P95 压到 1 秒",
  segment: 2,
  metricName: "p95_ms",
  metricUnit: "ms",
  direction: "lower" as const,
  baseline: 1_800,
  best: 940,
  completedRuns: 3,
  maxExperiments: 20,
  phase: "running" as const,
  activeRun: 4,
  latestMetric: 940,
  recent: [],
  totalRuns: 3,
};

/**
 * 上一进程被 kill 时的现场：一个 `started` 任务、一行它正在编辑的卡片，以及一份仍带着运行中步骤、
 * 运行中子代理、todo 与研究带的快照。`resumable` 只差第二阶段标记：写下它的行在恢复时会被续接，
 * 只发出 prompt 却不知道有没有被接受的行不会。
 */
async function seedResumableRound(
  root: string,
  botUid: string,
  options: { cardSeq: number; resumable: boolean },
): Promise<SeededRound> {
  const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
  try {
    store.setBotUid(botUid);
    store.setOwnerId(botUid, "owner-1");
    const taskId = `resuming-${options.cardSeq}-${options.resumable ? "accepted" : "pending"}`;
    const claim: InboundClaim = {
      botUid,
      target: RESUMED_TARGET,
      messageId: `message-${taskId}`,
      senderId: "owner-1",
      text: "fixture work",
      supported: true,
      kind: "task",
    };
    store.ensureBinding({
      botUid,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
      sessionId: "session-1",
      cwd: root,
      sessionFile: null,
    });
    const task: TaskRecord = {
      id: taskId,
      botUid,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
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
      relaySuppressed: false,
    };
    expect(store.acceptTask(claim, task)).toBe(true);
    expect(store.markStarted(taskId)).toBe(true);
    store.markTaskPromptStarted(taskId, "session-1");
    if (options.resumable) store.markTaskPromptAccepted(taskId);

    const tracker = createExecutionTracker({ runId: taskId, initialAtMs: 1_000 });
    tracker.apply({ type: "turn_start", turnId: 1, atMs: 1_000 });
    tracker.apply({ type: "todo_update", atMs: 1_100, phases: RESUMED_TODOS });
    for (let index = 0; index < 2; index += 1) {
      tracker.apply({ type: "tool_start", toolCallId: `call-${index}`, toolName: "bash", atMs: 1_200 + index });
      tracker.apply({ type: "tool_end", toolCallId: `call-${index}`, atMs: 1_250 + index, status: "succeeded" });
    }
    // 进程死时仍在运行的行：它们不可能再从任何进程收到结束帧。
    tracker.apply({ type: "tool_start", toolCallId: "call-stale", toolName: "bash", atMs: 1_500 });
    tracker.apply({
      type: "subagent_start",
      subagentId: "child-stale",
      agent: "scout",
      state: "running",
      atMs: 1_600,
      summary: "中断前的子代理",
    });
    tracker.apply({ type: "research_update", atMs: 1_700, research: RESUMED_RESEARCH });
    const payload = tracker.snapshot(1_800);
    const messageId = `card-${taskId}`;
    const snapshot = encodeExecutionSnapshot(payload);
    store.saveProgressCard({
      botUid,
      taskId,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
      messageId,
      lastSeq: options.cardSeq,
      snapshot,
    });
    return { taskId, messageId, lastSeq: options.cardSeq, phaseCount: payload.phaseCount, snapshot };
  } finally {
    store.close();
  }
}

/**
 * 卡片出站由合并定时器驱动：用例里用假时钟推进它，出站链仍然按原来的顺序排空。时钟只接管定时器
 * （`Date.now()` 仍走真实时间），所以快照里那两个毫秒时间戳（`startedAtMs` + `elapsedMs`）仍然是
 * 判定「上一进程最后被看见的一刻」的唯一依据。
 */
async function withResumeDomain(
  botUid: string,
  scenario: { cardSeq: number; resumable: boolean; refuseFirstEdit?: boolean },
  run: (seeded: SeededRound, store: ChannelStore, domain: ResumeDomain) => Promise<void>,
): Promise<void> {
  await withRoot(async (root) => {
    const seeded = await seedResumableRound(root, botUid, scenario);
    const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
    try {
      const { transport, sends, edits } = cardTransport(scenario);
      vi.useFakeTimers();
      await run(seeded, store, { cards: resumeDomain(store, botUid, transport), sends, edits });
    } finally {
      vi.useRealTimers();
      store.close();
    }
  });
}

function resumeDomain(
  store: ChannelStore,
  botUid: string,
  transport: Pick<ChannelTransport, "sendProgressCard" | "editProgressCard">,
): ProgressCards {
  return new ProgressCards({
    store,
    transport,
    botUid: () => botUid,
    stopping: () => false,
    trackOperation: () => {},
  });
}

test("a resumable turn keeps its card and the resumed run edits it in place", async () => {
  await withResumeDomain("bot-resume-cards", { cardSeq: 7, resumable: true }, async (seeded, store, domain) => {
    await domain.cards.recover();
    // 没有终态补发：读者眼前那张卡原地不动，行也还在（续接的一轮还要用它）。
    expect(domain.edits).toHaveLength(0);
    expect(domain.sends).toHaveLength(0);
    const held = store.listProgressCards("bot-resume-cards");
    expect(held).toHaveLength(1);
    expect(held[0]!.messageId).toBe(seeded.messageId);
    expect(held[0]!.lastSeq).toBe(7);

    const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
    progress.apply({
      type: "tool_start",
      toolCallId: "call-live",
      toolName: "bash",
      atMs: 2_000,
      summary: "续接后的第一步",
    });
    await flushCardEdits(progress, domain.edits, 1);
    const resumed = domain.edits[0]!;
    // 同一张卡、同一个序号序列：不重发创建，也不从 1 重新计数。
    expect(resumed.messageId).toBe(seeded.messageId);
    expect(domain.sends).toHaveLength(0);
    expect(resumed.cardSeq).toBe(8);
    expect(resumed.progress.state).toBe("running");
    expect(resumed.progress.phaseCount).toBe(seeded.phaseCount);
    expect(resumed.progress.todos).toEqual(RESUMED_TODOS);
    expect(resumed.progress.tools.map((tool) => [tool.toolCallId, tool.state])).toEqual([
      ["call-0", "succeeded"],
      ["call-1", "succeeded"],
      ["call-stale", "cancelled"],
      ["call-live", "running"],
    ]);
    // 遗留的运行中步骤按上一进程最后看见它的那一刻收束：停机时间不算进这一步的耗时。
    expect(resumed.progress.tools[2]!.durationMs).toBe(300);
    expect(resumed.progress.subagents.map((child) => [child.subagentId, child.state])).toEqual([
      ["child-stale", "cancelled"],
    ]);
    expect(resumed.progress.research).toMatchObject({ metricName: "p95_ms", completedRuns: 3 });
    // 落盘的行跟着走：下一次恢复会从 9 继续，而不是回到 8。
    expect(store.listProgressCards("bot-resume-cards")[0]!.lastSeq).toBe(8);

    progress.finish("completed");
    await flushCardEdits(progress, domain.edits, 2);
    const closed = domain.edits[1]!;
    expect(closed.messageId).toBe(seeded.messageId);
    expect(closed.cardSeq).toBe(9);
    expect(closed.progress.state).toBe("completed");
    expect(closed.progress.tools.every((tool) => tool.state !== "running")).toBe(true);
    expect(closed.progress.subagents.every((child) => child.state !== "running")).toBe(true);
    expect(store.listProgressCards("bot-resume-cards")).toHaveLength(0);
  });
});

test("a resumed round the gateway refuses keeps editing the same card in the degraded shape", async () => {
  await withResumeDomain(
    "bot-resume-degraded",
    { cardSeq: 5, resumable: true, refuseFirstEdit: true },
    async (seeded, store, domain) => {
      await domain.cards.recover();
      expect(domain.edits).toHaveLength(0);
      const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
      progress.apply({
        type: "tool_start",
        toolCallId: "call-live",
        toolName: "bash",
        atMs: 2_000,
        summary: "被拒正文里的一步",
      });
      await flushCardEdits(progress, domain.edits, 1);
      // 第一次编辑被网关拒绝（4xx 且没有 code）：同一张卡改用不带步骤摘要的形态重试，序号继续往上走。
      expect(domain.edits[0]!.progress.tools.some((tool) => tool.summary === "被拒正文里的一步")).toBe(true);
      expect(domain.sends).toHaveLength(0);
      await flushCardEdits(progress, domain.edits, 2);
      expect(domain.edits.map((edit) => [edit.messageId, edit.cardSeq])).toEqual([
        [seeded.messageId, 6],
        [seeded.messageId, 7],
      ]);
      expect(domain.edits[1]!.progress.tools.every((tool) => tool.summary === undefined)).toBe(true);
      expect(store.listProgressCards("bot-resume-degraded")[0]!.lastSeq).toBe(7);
    },
  );
});

test("a card the recovery could not take over is created under a fresh key segment", async () => {
  await withResumeDomain("bot-resume-malformed", { cardSeq: 5, resumable: true }, async (seeded, store, domain) => {
    // 最后一行载荷已经不可用：接管无从谈起，行被丢弃，剩下的只有「这批 key 已经用过」这一事实。
    store.saveProgressCard({
      botUid: "bot-resume-malformed",
      taskId: seeded.taskId,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
      messageId: seeded.messageId,
      lastSeq: seeded.lastSeq,
      snapshot: "{\"not\":\"a snapshot\"}",
    });

    await domain.cards.recover();
    expect(domain.edits).toHaveLength(0);
    expect(store.listProgressCards("bot-resume-malformed")).toHaveLength(0);

    const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
    progress.apply({ type: "tool_start", toolCallId: "call-live", toolName: "bash", atMs: 2_000 });
    await progress.tail;
    // 新卡照常在原聊天开出来，但 idempotency key 不再从上一进程用过的 `progress-<taskId>-0` 数起：
    // 服务端的去重窗口里那是一个已经接受过的 key，复用会让这次创建被当成重试。
    expect(domain.sends).toHaveLength(1);
    expect(domain.sends[0]!.clientMsgNo.startsWith(`progress-${seeded.taskId}-`)).toBe(true);
    expect(domain.sends[0]!.clientMsgNo).not.toBe(`progress-${seeded.taskId}-0`);
    expect(domain.edits).toHaveLength(0);

    // 第二个事件才产生这一轮的第一次增量编辑：新卡的序号从它自己的 1 往后走，与上一进程无关。
    progress.apply({ type: "tool_end", toolCallId: "call-live", atMs: 2_100, status: "succeeded" });
    await flushCardEdits(progress, domain.edits, 1);
    expect(domain.edits[0]!.messageId).toBe("new-card-1");
    expect(domain.edits[0]!.cardSeq).toBe(2);
    progress.finish("completed");
    await flushCardEdits(progress, domain.edits, 2);
    expect(store.listProgressCards("bot-resume-malformed")).toHaveLength(0);
  });
});

test("a resumable task with no surviving card row still opens under a fresh key segment", async () => {
  await withResumeDomain("bot-resume-rowless", { cardSeq: 6, resumable: true }, async (seeded, store, domain) => {
    // 上一轮的终态编辑已经清掉它的行，而新一轮还没产生开卡进度；恢复期一行卡片都读不到。
    store.clearProgressCard("bot-resume-rowless", seeded.taskId, seeded.messageId);
    expect(store.listProgressCards("bot-resume-rowless")).toHaveLength(0);
    // 真实启动会在这里先把 started 归一化为 queued；`startTask` 之后直接问 `isTaskResuming`
    // 决定这一轮是否换 key 段，所以恢复期有没有卡片行都不影响结论。
    store.prepareTasksForRestart("bot-resume-rowless", {
      cancel: "用户取消当前回合",
      interrupt: "用户中断当前回合",
      question_timeout: "等待回答超时，回合已停止",
    });
    expect(store.isTaskResuming(seeded.taskId)).toBe(true);

    await domain.cards.recover();
    expect(domain.edits).toHaveLength(0);
    const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
    progress.apply({ type: "tool_start", toolCallId: "call-live", toolName: "bash", atMs: 2_000 });
    await progress.tail;
    // 新卡照常开在原聊天，但 key 不能回到 `progress-<taskId>-0`：那是上一轮已经用过的 key，
    // 服务端的去重窗口里它会寻址到那张已经收尾的旧卡，而不是开出一张新的。
    expect(domain.sends).toHaveLength(1);
    expect(domain.sends[0]!.clientMsgNo.startsWith(`progress-${seeded.taskId}-`)).toBe(true);
    expect(domain.sends[0]!.clientMsgNo).not.toBe(`progress-${seeded.taskId}-0`);
  });
});

test("a roll right after a resume keeps the task's research band and todos", async () => {
  await withResumeDomain("bot-resume-research", { cardSeq: 4, resumable: true }, async (seeded, store, domain) => {
    await domain.cards.recover();
    const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
    // 恢复后没有新的 research_update 就 steer：研究带与 todo 都来自接管的快照，结转进新的一轮。
    progress.roll();
    await progress.tail;
    expect(domain.edits.map((edit) => [edit.messageId, edit.progress.state])).toEqual([
      [seeded.messageId, "completed"],
    ]);

    progress.apply({ type: "tool_start", toolCallId: "call-live", toolName: "bash", atMs: 2_000 });
    await progress.tail;
    expect(domain.sends).toHaveLength(1);
    const carried = domain.sends[0]!.progress;
    expect(carried.todos).toEqual(RESUMED_TODOS);
    expect(carried.research).toMatchObject({ metricName: "p95_ms", completedRuns: 3, best: 940 });
    expect(carried.tools.map((tool) => tool.toolCallId)).toEqual(["call-live"]);
    // 旧行随被顶掉那一轮的终态编辑清掉，只剩新卡这一行。
    expect(store.listProgressCards("bot-resume-research").map((card) => card.messageId)).toEqual([
      "new-card-1",
    ]);
  });
});

test("a prompt the worker never accepted is closed instead of resumed", async () => {
  await withResumeDomain("bot-resume-refused", { cardSeq: 3, resumable: false }, async (seeded, store, domain) => {
    // 恢复期这条行会被 fail-closed（prompt 是否被接受未知）；卡片域不看行是否「活跃」，只看它
    // 会不会被续接，所以它同样不该接管这张卡。
    expect(store.isTaskResuming(seeded.taskId)).toBe(false);

    await domain.cards.recover();
    expect(domain.sends).toHaveLength(0);
    expect(domain.edits).toHaveLength(1);
    expect(domain.edits[0]!.messageId).toBe(seeded.messageId);
    expect(domain.edits[0]!.cardSeq).toBe(4);
    expect(domain.edits[0]!.progress.state).toBe("cancelled");
    expect(store.listProgressCards("bot-resume-refused")).toHaveLength(0);
  });
});

test("the newest open round resumes while a rolled-away round is closed", async () => {
  await withResumeDomain("bot-resume-rounds", { cardSeq: 7, resumable: true }, async (seeded, store, domain) => {
    // 进程死在 steer 之后、被滚掉那一轮的终态编辑落地之前：同一任务两行都还开着。
    const currentMessageId = `${seeded.messageId}-round-2`;
    store.saveProgressCard({
      botUid: "bot-resume-rounds",
      taskId: seeded.taskId,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
      messageId: currentMessageId,
      lastSeq: 4,
      snapshot: encodeExecutionSnapshot(
        createExecutionTracker({ runId: seeded.taskId, initialAtMs: 1_000 }).snapshot(1_800),
      ),
    });
    // 旧轮的迟到重写（终态编辑落地、或补发失败把 attempts 与序号推上去）落在新轮之后：
    // 行的新旧只看创建序（rowid），旧轮不能因为写得晚就变成「当前那一轮」。
    store.saveProgressCard({
      botUid: "bot-resume-rounds",
      taskId: seeded.taskId,
      channelType: RESUMED_TARGET.channelType,
      channelId: RESUMED_TARGET.channelId,
      messageId: seeded.messageId,
      lastSeq: seeded.lastSeq,
      snapshot: seeded.snapshot,
    });

    await domain.cards.recover();
    // 被滚掉的那轮已经由后一轮回答过：按 `roll` 给的结局收尾；当前那轮原地保留。
    expect(domain.sends).toHaveLength(0);
    expect(domain.edits.map((edit) => [edit.messageId, edit.cardSeq, edit.progress.state])).toEqual([
      [seeded.messageId, 8, "completed"],
    ]);
    expect(store.listProgressCards("bot-resume-rounds").map((card) => card.messageId)).toEqual([
      currentMessageId,
    ]);

    const progress = domain.cards.startTask(seeded.taskId, RESUMED_TARGET);
    progress.apply({ type: "tool_start", toolCallId: "call-live", toolName: "bash", atMs: 2_000 });
    await flushCardEdits(progress, domain.edits, 2);
    expect(domain.edits[1]!.messageId).toBe(currentMessageId);
    expect(domain.edits[1]!.cardSeq).toBe(5);
    progress.finish("completed");
    await flushCardEdits(progress, domain.edits, 3);
    expect(domain.edits[2]!.messageId).toBe(currentMessageId);
    expect(domain.edits[2]!.cardSeq).toBe(6);
    expect(store.listProgressCards("bot-resume-rounds")).toHaveLength(0);
  });
});

test("a suspended turn is resumed onto the same card and finishes on it", async () => {
  const bot: BotIdentity = { uid: "bot-suspend-card", name: "Fixture Bot" };
  const target: ChatTarget = { channelId: "dm-suspend-card", channelType: 1 };
  const continueResume = Promise.withResolvers<void>();
  const firstTransport = new RecoveryTransport({ bot });
  // 第一进程：开卡后一直挂着的回合。停机把它挂起（不是取消），卡片行留在盘上。
  const first = await createCoreHarness({
    bot,
    transport: firstTransport,
    prompt: async ({ emit, isStopped, onStop }) => {
      emit({
        type: "tool_start",
        toolCallId: "before-restart",
        toolName: "bash",
        atMs: Date.now(),
        summary: "重启前的一步",
      });
      await new Promise<void>((resolve) => {
        onStop(resolve);
        if (isStopped()) resolve();
      });
      if (isStopped()) throw new Error("OMP prompt cancelled");
    },
  });
  try {
    await first.service.submitInbound(inbound({ messageId: "suspend-card", ...target, text: "开卡后挂起" }));
    await first.workers.waitForPromptCount(1);
    // 让服务端已经接受的这一帧真的落进状态盘，再停机：挂起的回合留给下一进程的是这张卡。
    await firstTransport.waitForProgressSend();
  } finally {
    await closeCoreHarness(first, { remove: false });
  }
  const rows = await openCards(first.root, bot.uid);
  expect(rows).toHaveLength(1);
  const seeded = { messageId: rows[0]!.messageId, lastSeq: rows[0]!.lastSeq };

  const transport = new RecoveryTransport({ bot });
  const restarted = await createCoreHarness({
    root: first.root,
    bot,
    transport,
    prompt: async ({ text, emit }) => {
      // 续接不重放原文：新进程派发的是 OMP 自己的 /retry。
      if (text !== "/retry") return;
      emit({ type: "tool_start", toolCallId: "after-restart", toolName: "bash", atMs: Date.now() });
      // 停到运行中的编辑真的发出来，再让回合收尾，于是两次编辑的序号都能被断言。
      await continueResume.promise;
      emit({
        type: "tool_end",
        toolCallId: "after-restart",
        atMs: Date.now(),
        status: "succeeded",
        summary: "续接后的一步",
      });
      emit(assistantText("resumed-card-body", "done", true));
    },
  });
  try {
    const running = await transport.waitForProgressEdit((edit) => (
      edit.progress.tools.some((tool) => tool.toolCallId === "after-restart")
    ));
    // 同一张卡、序号严格接在存储值之后，而且重启前那一步的历史仍在卡上（它的结束帧再也不会来）。
    expect(running.messageId).toBe(seeded.messageId);
    expect(running.cardSeq).toBe(seeded.lastSeq + 1);
    expect(running.progress.tools.map((tool) => [tool.toolCallId, tool.state])).toEqual([
      ["before-restart", "cancelled"],
      ["after-restart", "running"],
    ]);
    continueResume.resolve();
    await waitForStatus(restarted.service, target, (status) => status.state === "completed" && status.outbox === 0);

    // 全程没有第二张卡、没有任何一次取消：终态编辑落在同一张卡上，随后清掉那一行。
    expect(transport.progressCardSends).toHaveLength(0);
    const edits = transport.progressCardEdits;
    expect(edits.map((edit) => edit.cardSeq)).toEqual(edits.map((_, index) => seeded.lastSeq + 1 + index));
    expect(edits.every((edit) => edit.messageId === seeded.messageId)).toBe(true);
    expect(edits.every((edit) => edit.progress.state !== "cancelled")).toBe(true);
    expect(edits.at(-1)!.progress.state).toBe("completed");
    expect(edits.at(-1)!.progress.tools.map((tool) => [tool.toolCallId, tool.state])).toEqual([
      ["before-restart", "cancelled"],
      ["after-restart", "succeeded"],
    ]);
    expect(await openCards(first.root, bot.uid)).toHaveLength(0);
  } finally {
    continueResume.resolve();
    await closeCoreHarness(restarted, { remove: false });
  }
});

test("a replayed tool call reopens the row recovery had settled", () => {
  const tracker = createExecutionTracker({ runId: "replay", initialAtMs: 500 });
  tracker.apply({ type: "tool_start", toolCallId: "done", toolName: "read", atMs: 600, summary: "第一次" });
  tracker.apply({ type: "tool_end", toolCallId: "done", atMs: 700, status: "succeeded" });
  tracker.apply({ type: "tool_start", toolCallId: "interrupted", toolName: "bash", atMs: 800, summary: "中断前" });
  const encoded = encodeExecutionSnapshot(tracker.snapshot(900));

  const resumed = restoreExecutionTracker(encoded, { state: "running", atMs: 900_000 })!;
  expect(resumed.snapshot(900_000).tools.map((tool) => [tool.toolCallId, tool.state])).toEqual([
    ["done", "succeeded"],
    ["interrupted", "cancelled"],
  ]);

  // OMP 原生 /retry 复用同一个 toolCallId：真实重放把这一行重新打开，之后的状态与耗时都来自真实事件。
  resumed.apply({ type: "tool_start", toolCallId: "interrupted", toolName: "bash", atMs: 1_000, summary: "重放" });
  expect(
    resumed.snapshot(1_050).tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs, tool.durationMs, tool.summary]),
  ).toEqual([
    ["done", "succeeded", 700, 100, "第一次"],
    ["interrupted", "running", null, 50, "重放"],
  ]);
  resumed.apply({ type: "tool_end", toolCallId: "interrupted", atMs: 1_200, status: "succeeded" });
  expect(resumed.snapshot(1_300).tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs])).toEqual([
    ["done", "succeeded", 700],
    ["interrupted", "succeeded", 1_200],
  ]);

  // 真实结束过的行不会被重开：那条同名 start 不是一次真的执行，行保留自己的结束时刻。
  resumed.apply({ type: "tool_start", toolCallId: "done", toolName: "read", atMs: 1_400 });
  expect(resumed.snapshot(1_500).tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs])).toEqual([
    ["done", "succeeded", 700],
    ["interrupted", "succeeded", 1_200],
  ]);
});

test("a retried tool id keeps its reopening licence across two restarts", () => {
  const first = createExecutionTracker({ runId: "twice", initialAtMs: 100 });
  first.apply({ type: "tool_start", toolCallId: "call-1", toolName: "bash", atMs: 200, summary: "中断前" });
  // 第一次恢复把上一进程遗留的运行中行收束成「推断结束」。
  const firstResume = restoreExecutionTracker(encodeExecutionSnapshot(first.snapshot(300)), {
    state: "running",
    atMs: 900_000,
  })!;
  // 收束之后又来了别的真实事件，于是这一行以 cancelled 写进持久载荷——标记必须一起睡进快照，
  // 否则它在第二次重启后就丢了重开资格。
  firstResume.apply({ type: "tool_start", toolCallId: "call-2", toolName: "read", atMs: 400 });
  const persisted = encodeExecutionSnapshot(firstResume.snapshot(500));

  const secondResume = restoreExecutionTracker(persisted, { state: "running", atMs: 900_000 })!;
  expect(secondResume.snapshot(900_000).tools.map((tool) => [tool.toolCallId, tool.state, tool.recoverySettled])).toEqual([
    ["call-1", "cancelled", true],
    ["call-2", "cancelled", true],
  ]);
  // 同一次真实重放（原生 /retry 复用同一个 toolCallId）仍然把这一行重开。
  secondResume.apply({ type: "tool_start", toolCallId: "call-1", toolName: "bash", atMs: 1_000, summary: "重放" });
  expect(secondResume.snapshot(1_050).tools.map((tool) => [tool.toolCallId, tool.state, tool.summary])).toEqual([
    ["call-1", "running", "重放"],
    ["call-2", "cancelled", undefined],
  ]);
  secondResume.apply({ type: "tool_end", toolCallId: "call-1", atMs: 1_100, status: "succeeded" });
  expect(
    secondResume.snapshot(1_200).tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs, tool.recoverySettled]),
  ).toEqual([
    ["call-1", "succeeded", 1_100, undefined],
    ["call-2", "cancelled", 500, true],
  ]);
  // 真正结束过的行不再有资格：真实结果一旦落地，标记就消失了。
  secondResume.apply({ type: "tool_start", toolCallId: "call-1", toolName: "bash", atMs: 1_300 });
  expect(secondResume.snapshot(1_400).tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs])).toEqual([
    ["call-1", "succeeded", 1_100],
    ["call-2", "cancelled", 500],
  ]);
});

test("the shared replay reopens a running round and still ends a terminal one", () => {
  const tracker = createExecutionTracker({ runId: "seam", initialAtMs: 100 });
  tracker.apply({ type: "turn_start", turnId: 1, atMs: 100 });
  tracker.apply({ type: "tool_start", toolCallId: "done", toolName: "read", atMs: 110 });
  tracker.apply({ type: "tool_end", toolCallId: "done", atMs: 120, status: "succeeded" });
  tracker.apply({ type: "tool_start", toolCallId: "live", toolName: "bash", atMs: 130 });
  const encoded = encodeExecutionSnapshot(tracker.snapshot(200));

  const resumed = restoreExecutionTracker(encoded, { state: "running", atMs: 900_000 });
  expect(resumed).not.toBeNull();
  const running = resumed!.snapshot(900_000);
  expect(running.state).toBe("running");
  expect(running.endedAtMs).toBeNull();
  expect(running.phaseCount).toBe(1);
  expect(running.tools.map((tool) => [tool.toolCallId, tool.state, tool.endedAtMs])).toEqual([
    ["done", "succeeded", 120],
    ["live", "cancelled", 200],
  ]);

  const closed = restoreExecutionSnapshot(encoded, { state: "failed", atMs: 900_000 });
  expect(closed!.state).toBe("failed");
  expect(closed!.endedAtMs).toBe(900_000);
  expect(closed!.phaseCount).toBe(0);
  expect(closed!.tools.map((tool) => [tool.toolCallId, tool.state])).toEqual([
    ["done", "succeeded"],
    ["live", "failed"],
  ]);
});
