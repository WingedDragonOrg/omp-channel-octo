import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts";
import { type ChannelStore, openChannelStore } from "../src/store";
import type { InboundClaim, TaskRecord } from "../src/store/store-records";

// 重启恢复的持久层契约：在途任务重排回 queued、prompt 标记决定「重放输入」还是「续接会话」、
// 终态永不重跑、旧库的歧义 started 行开库只记账不改状态、由启动恢复结算为 interrupted。
const botUid = "bot-restart";
const target: ChatTarget = { channelId: "dm-restart", channelType: 1 };

async function withStore<T>(
  work: (store: ChannelStore, stateDir: string) => T | Promise<T>,
): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-restart-store-"));
  const stateDir = join(root, "state");
  const store = await openChannelStore(stateDir);
  try {
    return await work(store, stateDir);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

function claimFor(messageId: string, text: string): InboundClaim {
  return { botUid, target, messageId, senderId: "owner-1", text, supported: true, kind: "task" };
}

function taskFor(messageId: string, taskId: string, overrides: Partial<TaskRecord> = {}): TaskRecord {
  return {
    id: taskId,
    botUid,
    channelType: target.channelType,
    channelId: target.channelId,
    messageId,
    senderId: "owner-1",
    text: `${taskId} 原始输入`,
    sessionId: `session-${taskId}`,
    cwd: "/tmp/restart-cwd",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relayChannelType: null,
    relayChannelId: null,
    relayThreadName: null,
    ...overrides,
  };
}

function accept(store: ChannelStore, task: TaskRecord): void {
  expect(store.acceptTask(claimFor(task.messageId, task.text), task)).toBe(true);
}

test("重启准备把在途任务按接受顺序重排为 queued，并保留恢复身份与终态", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "t1"));
    expect(store.markStarted("t1")).toBe(true);
    accept(store, taskFor("m2", "t2", {
      relayChannelType: 5,
      relayChannelId: "thread-relay",
      relayThreadName: "派发线程",
      messageSeq: 7,
    }));
    accept(store, taskFor("m3", "t3"));
    expect(store.markStarted("t3")).toBe(true);
    store.markTaskPromptStarted("t3", "session-live-t3");
    store.markTaskPromptAccepted("t3");
    accept(store, taskFor("m4", "t4"));
    store.markCompleted("t4", "已完成结果");
    accept(store, taskFor("m5", "t5"));
    store.markCancelled("t5");
    accept(store, taskFor("m6", "t6"));
    store.markFailed("t6", "执行失败详情");
    accept(store, taskFor("m7", "t7"));
    expect(store.markStarted("t7")).toBe(true);
    store.markInterrupted("t7", "用户中断当前回合");
    accept(store, { ...taskFor("m8", "other-task"), botUid: "bot-other" });
    expect(store.markStarted("other-task")).toBe(true);

    const recovered = store.prepareTasksForRestart(botUid);

    expect(recovered.queued.map((task) => task.id)).toEqual(["t1", "t2", "t3"]);
    expect(recovered.queued.map((task) => task.state)).toEqual(["queued", "queued", "queued"]);
    expect(recovered.queued.map((task) => task.text)).toEqual([
      "t1 原始输入",
      "t2 原始输入",
      "t3 原始输入",
    ]);
    expect(recovered.uncertain).toEqual([]);
    // 没有 prompt 标记的 started 行按未发出处理（可安全重放原文），已有接受回执的行才续接原会话。
    expect(store.isTaskResuming("t1")).toBe(false);
    expect(store.isTaskResuming("t2")).toBe(false);
    expect(store.isTaskResuming("t3")).toBe(true);
    expect(store.getTask("t3")?.sessionId).toBe("session-live-t3");
    // 接受时的顺序即恢复顺序，relay 元数据与消息序号随行保留。
    expect(store.listQueuedTasks().map((task) => task.id)).toEqual(["t1", "t2", "t3"]);
    expect(store.getTask("t2")).toMatchObject({
      relayChannelType: 5,
      relayChannelId: "thread-relay",
      relayThreadName: "派发线程",
      messageSeq: 7,
    });
    // 显式终态绝不再进入队列，也不被当成可续接的任务。
    expect(store.getTask("t4")).toMatchObject({ state: "completed", resultText: "已完成结果" });
    expect(store.getTask("t5")).toMatchObject({ state: "cancelled" });
    expect(store.getTask("t6")).toMatchObject({ state: "failed", errorText: "执行失败详情" });
    expect(store.getTask("t7")).toMatchObject({ state: "interrupted", errorText: "用户中断当前回合" });
    for (const id of ["t4", "t5", "t6", "t7"]) expect(store.isTaskResuming(id)).toBe(false);
    // 别的 Bot 的在途任务不属于这次恢复。
    expect(store.getTask("other-task")?.state).toBe("running");
  }));

test("重复启动收敛：同一集合与顺序，恢复会话不被替换", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "t1"));
    expect(store.markStarted("t1")).toBe(true);
    accept(store, taskFor("m2", "t2"));
    expect(store.markStarted("t2")).toBe(true);
    store.markTaskPromptStarted("t2", "session-live-t2");
    store.markTaskPromptAccepted("t2");

    const first = store.prepareTasksForRestart(botUid).queued.map((task) => task.id);
    expect(first).toEqual(["t1", "t2"]);
    // 第二次重启：上一次已经把两行重新拉起，恢复必须得到同一集合与顺序。
    for (const id of first) expect(store.markStarted(id)).toBe(true);
    const second = store.prepareTasksForRestart(botUid);
    expect(second.queued.map((task) => task.id)).toEqual(["t1", "t2"]);
    expect(second.queued.map((task) => task.state)).toEqual(["queued", "queued"]);
    expect(store.isTaskResuming("t2")).toBe(true);
    expect(store.getTask("t2")?.sessionId).toBe("session-live-t2");
  }));

test("重启准备不动等待中的提问，也不释放 inbound 认领", () =>
  withStore((store) => {
    const task = taskFor("m1", "t1");
    accept(store, task);
    store.createQuestion({
      id: "q-open",
      taskId: "t1",
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      payload: "要继续吗？",
    });
    expect(store.markStarted("t1")).toBe(true);

    expect(store.prepareTasksForRestart(botUid).queued.map((row) => row.id)).toEqual(["t1"]);

    expect(store.getQuestion("q-open")?.state).toBe("pending");
    // 认领仍在，重投的触发消息依旧是重复消息而不是新任务。
    expect(store.acceptTask(claimFor("m1", task.text), task)).toBe(false);
  }));

test("markTaskPromptStarted 只写在 started 行上，且拒绝覆盖已记录的会话", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "t1"));
    expect(() => store.markTaskPromptStarted("t1", "session-live")).toThrow();
    expect(() => store.markTaskPromptAccepted("t1")).toThrow();
    expect(store.isTaskResuming("t1")).toBe(false);
    expect(store.getTask("t1")?.sessionId).toBe("session-t1");

    expect(store.markStarted("t1")).toBe(true);
    store.markTaskPromptStarted("t1", "session-live");
    expect(store.getTask("t1")?.sessionId).toBe("session-live");
    // 没有 OMP 的接受回执就还不能续接。
    expect(store.isTaskResuming("t1")).toBe(false);
    store.markTaskPromptAccepted("t1");
    expect(store.isTaskResuming("t1")).toBe(true);
    // 同一任务重复到达的接受事件是幂等的。
    store.markTaskPromptAccepted("t1");

    // 已经发出过 prompt 的任务只能续接，同一行再来一次必须报错且不改写恢复身份。
    expect(() => store.markTaskPromptStarted("t1", "session-other")).toThrow();
    expect(store.getTask("t1")?.sessionId).toBe("session-live");
    expect(() => store.markTaskPromptStarted("no-such-task", "session-live")).toThrow();
    expect(() => store.markTaskPromptAccepted("no-such-task")).toThrow();
  }));

test("已发出 prompt 却缺少接受回执的任务在恢复时 fail-closed 为 interrupted", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "no-prompt"));
    expect(store.markStarted("no-prompt")).toBe(true);
    accept(store, taskFor("m2", "accepted"));
    expect(store.markStarted("accepted")).toBe(true);
    store.markTaskPromptStarted("accepted", "session-accepted");
    store.markTaskPromptAccepted("accepted");
    accept(store, taskFor("m3", "unaccepted"));
    expect(store.markStarted("unaccepted")).toBe(true);
    store.markTaskPromptStarted("unaccepted", "session-unaccepted");

    const recovered = store.prepareTasksForRestart(botUid);

    // 未发 prompt 的行重放原文，有回执的行续接；缺回执的行既不重放也不续接，走 uncertain 交回服务结算。
    expect(recovered.queued.map((task) => [task.id, task.state])).toEqual([
      ["no-prompt", "queued"],
      ["accepted", "queued"],
    ]);
    expect(recovered.uncertain.map((task) => [task.id, task.state])).toEqual([
      ["unaccepted", "interrupted"],
    ]);
    expect(store.isTaskResuming("no-prompt")).toBe(false);
    expect(store.isTaskResuming("accepted")).toBe(true);
    expect(recovered.uncertain[0]).toMatchObject({
      text: "unaccepted 原始输入",
      sessionId: "session-unaccepted",
    });
    expect(store.getTask("unaccepted")).toMatchObject({
      state: "interrupted",
      text: "unaccepted 原始输入",
      sessionId: "session-unaccepted",
    });
    expect(store.getTask("unaccepted")?.errorText).toContain("unknown");
    expect(store.isTaskResuming("unaccepted")).toBe(false);
    // 下一次重启不再重复上报已经结算的行，也不重复动它。
    const next = store.prepareTasksForRestart(botUid);
    expect(next.queued.map((task) => task.id)).toEqual(["no-prompt", "accepted"]);
    expect(next.uncertain).toEqual([]);
  }));

test("prompt 标记跨进程重开依然可见", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-restart-store-"));
  const stateDir = join(root, "state");
  try {
    const first = await openChannelStore(stateDir);
    accept(first, taskFor("m1", "t1"));
    expect(first.markStarted("t1")).toBe(true);
    first.markTaskPromptStarted("t1", "session-live");
    first.markTaskPromptAccepted("t1");
    first.close();

    const second = await openChannelStore(stateDir);
    try {
      expect(second.isTaskResuming("t1")).toBe(true);
      expect(second.getTask("t1")?.sessionId).toBe("session-live");
      // 补列之后重开不会再跑一次性迁移，也不会重排或改写这一行。
      expect(second.getTask("t1")?.state).toBe("running");
    } finally {
      second.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("旧库升级只记账不改状态，启动恢复才结算歧义 started 行", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-restart-store-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const legacy = new Database(join(stateDir, "channel.sqlite"));
  try {
    legacy.exec(`
      CREATE TABLE tasks (
        id TEXT PRIMARY KEY NOT NULL,
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        text TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cwd TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'interrupted')),
        accepted_at INTEGER NOT NULL,
        started_at INTEGER,
        finished_at INTEGER,
        result_text TEXT,
        error_text TEXT
      );
    `);
    const insert = legacy.query(
      `INSERT INTO tasks(id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd, state, accepted_at)
       VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    insert.run("legacy-queued", botUid, 1, target.channelId, "legacy-m1", "owner-1", "legacy queued work", "session-legacy-queued", "/tmp/legacy-cwd", "queued", 1);
    insert.run("legacy-started", botUid, 1, target.channelId, "legacy-m2", "owner-1", "legacy started work", "session-legacy-started", "/tmp/legacy-cwd", "started", 2);
    insert.run("legacy-done", botUid, 1, target.channelId, "legacy-m3", "owner-1", "legacy done work", "session-legacy-done", "/tmp/legacy-cwd", "completed", 3);
  } finally {
    legacy.close();
  }

  try {
    const upgraded = await openChannelStore(stateDir);
    // 只补列与记账：旧进程可能还在跑这些任务，开库不得改状态、不得写终态字段。
    expect(upgraded.getTask("legacy-started")).toMatchObject({
      state: "running",
      text: "legacy started work",
      sessionId: "session-legacy-started",
      errorText: null,
      resultText: null,
    });
    expect(upgraded.getTask("legacy-done")?.state).toBe("completed");

    // 启动恢复（service 拿到 Bot 锁、回收旧 worker 之后）才把接受状态未知的行结算成 interrupted。
    const legacyRecovery = upgraded.prepareTasksForRestart(botUid);
    expect(legacyRecovery.queued.map((task) => task.id)).toEqual(["legacy-queued"]);
    expect(legacyRecovery.uncertain.map((task) => task.id)).toEqual(["legacy-started"]);
    expect(upgraded.getTask("legacy-started")).toMatchObject({
      state: "interrupted",
      text: "legacy started work",
      sessionId: "session-legacy-started",
    });

    // 升级后的新行不再受一次性迁移影响：没有 prompt 标记的 started 行保持可恢复。
    accept(upgraded, taskFor("m-new", "new-task"));
    expect(upgraded.markStarted("new-task")).toBe(true);
    upgraded.close();

    const reopened = await openChannelStore(stateDir);
    try {
      expect(reopened.getTask("legacy-started")?.state).toBe("interrupted");
      expect(reopened.getTask("new-task")?.state).toBe("running");
      // 升级后的库支持完整的派发/接受两阶段标记，续接判定照常工作。
      reopened.markTaskPromptStarted("new-task", "session-new-task");
      reopened.markTaskPromptAccepted("new-task");
      const recovered = reopened.prepareTasksForRestart(botUid);
      expect(recovered.queued.map((task) => task.id)).toEqual(["legacy-queued", "new-task"]);
      expect(recovered.uncertain).toEqual([]);
      expect(reopened.isTaskResuming("new-task")).toBe(true);
      expect(reopened.isTaskResuming("legacy-queued")).toBe(false);
    } finally {
      reopened.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("反复打开旧库不改任务状态，也不写终态字段", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-restart-store-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const legacy = new Database(join(stateDir, "channel.sqlite"));
  try {
    legacy.exec(`
      CREATE TABLE tasks (
        id TEXT PRIMARY KEY NOT NULL,
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        text TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cwd TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'interrupted')),
        accepted_at INTEGER NOT NULL,
        started_at INTEGER,
        finished_at INTEGER,
        result_text TEXT,
        error_text TEXT
      );
    `);
    legacy
      .query(
        `INSERT INTO tasks(id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd, state, accepted_at)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?)`,
      )
      .run("still-running", botUid, 1, target.channelId, "running-m1", "owner-1", "still running work", "session-still-running", "/tmp/legacy-cwd", 1);
  } finally {
    legacy.close();
  }

  const readState = async () => {
    const store = await openChannelStore(stateDir);
    try {
      return store.getTask("still-running");
    } finally {
      store.close();
    }
  };
  try {
    // CLI/status 之类的开库会反复发生，都不该碰旧服务仍在跑的任务。
    expect(await readState()).toMatchObject({ state: "running", errorText: null, resultText: null });
    expect(await readState()).toMatchObject({ state: "running", errorText: null, resultText: null });
    const db = new Database(join(stateDir, "channel.sqlite"));
    try {
      expect(
        db.query("SELECT started_at, finished_at, error_text FROM tasks WHERE id = 'still-running'").get(),
      ).toEqual({ started_at: null, finished_at: null, error_text: null });
    } finally {
      db.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("进度卡按创建顺序返回，旧轮的迟到重写不会越过新一轮", () =>
  withStore((store, stateDir) => {
    // 两张卡是多轮的创建顺序；message_id 与排序无关。
    const save = (messageId: string, lastSeq: number) => store.saveProgressCard({
      botUid,
      taskId: "t1",
      channelType: target.channelType,
      channelId: target.channelId,
      messageId,
      lastSeq,
      snapshot: "{}",
    });
    save("card-z", 1);
    save("card-a", 2);

    const db = new Database(join(stateDir, "channel.sqlite"));
    try {
      db.exec("UPDATE progress_cards SET updated_at = 1");
    } finally {
      db.close();
    }
    expect(store.listProgressCards(botUid).map((card) => card.messageId)).toEqual(["card-z", "card-a"]);

    // 旧轮（card-z）重试后重写：updated_at 变新、rowid 不变，因此它仍排在新轮之前。
    save("card-z", 2);
    expect(store.listProgressCards(botUid).map((card) => [card.messageId, card.lastSeq])).toEqual([
      ["card-z", 2],
      ["card-a", 2],
    ]);
  }));

test("卡片序号按卡持久递增：attach 记 1，reserve 原子 +1 且不按时钟", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "t1"));
    const ask = (id: string) => store.createQuestion({
      id,
      taskId: "t1",
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      payload: "{}",
    });
    ask("q1");
    ask("q2");
    store.setQuestionsCard(["q1", "q2"], "card-1");

    // 首帧就是 1，所以首次编辑从 2 开始；同一张卡的所有行共享一个计数器。
    expect(store.reserveQuestionCardSeq(botUid, target, "card-1")).toBe(2);
    expect(store.reserveQuestionCardSeq(botUid, target, "card-1")).toBe(3);

    // 还没有行的卡：首帧已用掉 1，因此首次 reserve 返回 2，绝不撞号
    //（卡上带 NULL 旧行的情况走一次性高水位 bootstrap，见下一条用例）。
    expect(store.reserveQuestionCardSeq(botUid, target, "card-unknown")).toBe(2);

    // 换了卡就换了计数器：新卡从 1 之后再接，老卡的进度不受影响。
    store.setQuestionCard("q1", "card-2");
    expect(store.reserveQuestionCardSeq(botUid, target, "card-2")).toBe(2);
    expect(store.reserveQuestionCardSeq(botUid, target, "card-1")).toBe(4);
  }));

test("旧库提问行的序号首次预留取高水位，之后持久单调且不动业务状态", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-restart-store-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const legacy = new Database(join(stateDir, "channel.sqlite"));
  try {
    legacy.exec(`
      CREATE TABLE questions (
        id TEXT PRIMARY KEY NOT NULL,
        task_id TEXT NOT NULL,
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
        channel_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending', 'answered', 'cancelled', 'expired')),
        answer TEXT,
        answered_by TEXT,
        created_at INTEGER NOT NULL,
        answered_at INTEGER,
        card_message_id TEXT
      );
    `);
    legacy
      .query(
        `INSERT INTO questions(id, task_id, bot_uid, channel_type, channel_id, payload, state, created_at, card_message_id)
         VALUES('legacy-question', 'legacy-task', ?, 1, ?, '{}', 'pending', 1, 'legacy-card')`,
      )
      .run(botUid, target.channelId);
  } finally {
    legacy.close();
  }

  try {
    const first = await openChannelStore(stateDir);
    let bootstrapped = 0;
    try {
      // 旧线上版本是「内存序号 + 时间戳起点」，序号能到 1.7e12 量级：高水位必须取自当前时间，
      // 绝不能预设一个小常数。
      bootstrapped = first.reserveQuestionCardSeq(botUid, target, "legacy-card");
      expect(bootstrapped).toBeGreaterThan(Date.now() - 60_000);
      // 高水位只在首次预留时取一次，之后按持久值严格递增。
      expect(first.reserveQuestionCardSeq(botUid, target, "legacy-card")).toBe(bootstrapped + 1);
      // 迁移只补列：旧行的业务状态与「未落定」语义原样保留，交给启动恢复认领。
      expect(first.getQuestion("legacy-question")).toMatchObject({ state: "pending", settledAt: null });
    } finally {
      first.close();
    }

    const second = await openChannelStore(stateDir);
    try {
      expect(second.reserveQuestionCardSeq(botUid, target, "legacy-card")).toBe(bootstrapped + 2);
    } finally {
      second.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("markQuestionsSettled 只给未收场的行落时间，幂等且空数组 no-op", () =>
  withStore((store, stateDir) => {
    accept(store, taskFor("m1", "t1"));
    for (const id of ["q1", "q2"]) {
      store.createQuestion({
        id,
        taskId: "t1",
        botUid,
        channelType: target.channelType,
        channelId: target.channelId,
        payload: "{}",
      });
    }
    expect(store.listTaskQuestions("t1").map((question) => question.settledAt)).toEqual([null, null]);

    store.markQuestionsSettled([]);
    expect(store.getQuestion("q1")?.settledAt).toBeNull();

    store.markQuestionsSettled(["q1", "no-such-row"]);
    expect(typeof store.getQuestion("q1")?.settledAt).toBe("number");
    expect(store.getQuestion("q2")?.settledAt).toBeNull();

    // 已经收场过的行不会被刷新：时间戳是第一次收场的证据。
    const db = new Database(join(stateDir, "channel.sqlite"));
    try {
      db.exec("UPDATE questions SET settled_at = 12345 WHERE id = 'q1'");
    } finally {
      db.close();
    }
    store.markQuestionsSettled(["q1"]);
    const probe = new Database(join(stateDir, "channel.sqlite"));
    try {
      expect(probe.query("SELECT settled_at FROM questions WHERE id = 'q1'").get()).toEqual({
        settled_at: 12345,
      });
    } finally {
      probe.close();
    }
  }));

test("listTaskQuestions 按写入顺序返回同一任务的全部提问行", () =>
  withStore((store) => {
    accept(store, taskFor("m1", "t1"));
    accept(store, taskFor("m2", "t2"));
    for (const [id, taskId] of [["q1", "t1"], ["q2", "t1"], ["q-other", "t2"]] as const) {
      store.createQuestion({
        id,
        taskId,
        botUid,
        channelType: target.channelType,
        channelId: target.channelId,
        payload: `${id} 的问题`,
      });
    }
    store.answerQuestion("q1", botUid, target, "答案", "owner-1");

    expect(store.listTaskQuestions("t1").map((question) => [question.id, question.state])).toEqual([
      ["q1", "answered"],
      ["q2", "pending"],
    ]);
    expect(store.listTaskQuestions("t2").map((question) => question.id)).toEqual(["q-other"]);
    expect(store.listTaskQuestions("no-such-task")).toEqual([]);
  }));
