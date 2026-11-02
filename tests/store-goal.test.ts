import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, WorkerGoal } from "../src/contracts";
import { openChannelStore, type ChannelStore } from "../src/store";
import type { GoalTaskRequest, InboundClaim, TaskRecord, TaskStopIntent } from "../src/store/store-records";

const botUid = "bot-goal-store";
const target: ChatTarget = { channelType: 1, channelId: "dm-goal-store" };

async function withStore<T>(work: (store: ChannelStore, stateDir: string) => T | Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-goal-store-"));
  const stateDir = join(root, "state");
  const store = await openChannelStore(stateDir);
  try {
    return await work(store, stateDir);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

function claim(messageId: string, text: string): InboundClaim {
  return { botUid, target, messageId, senderId: "owner-goal", text, supported: true, kind: "task" };
}

function task(messageId: string, id: string, sessionId: string, text = `display-only:${id}`): TaskRecord {
  return {
    id,
    botUid,
    ...target,
    messageId,
    senderId: "owner-goal",
    text,
    sessionId,
    cwd: "/tmp/goal-store",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relayChannelType: null,
    relayChannelId: null,
    relayThreadName: null,
  };
}

function acceptGoal(store: ChannelStore, id: string, sessionId: string, request: GoalTaskRequest): void {
  const value = task(`message-${id}`, id, sessionId);
  expect(store.acceptTask(claim(value.messageId, value.text), value, request)).toBe(true);
}

const stopReasons: Record<TaskStopIntent, string> = {
  interrupt: "interrupted",
  question_timeout: "ask timed out",
};

function goal(id: string, status: WorkerGoal["status"] = "active"): WorkerGoal {
  return {
    id,
    objective: "Persist the native goal",
    status,
    tokensUsed: 18,
    timeUsedSeconds: 9,
  };
}

test("goal request is durably dispatched before acceptance and late acceptance cannot restore revoked authorization", () =>
  withStore((store) => {
    acceptGoal(store, "create-goal", "session-goal", {
      op: "create",
      objective: "Persist the native goal",
      tokenBudget: 600,
    });
    expect(store.getGoalTask("create-goal")).toMatchObject({
      sessionId: "session-goal",
      request: { op: "create", objective: "Persist the native goal", tokenBudget: 600 },
      dispatched: false,
      accepted: false,
      acceptedGoalId: null,
    });
    expect(store.markStarted("create-goal")).toBe(true);
    expect(() => store.markGoalTaskAccepted("create-goal", "goal-exact")).toThrow();

    store.markGoalTaskDispatched("create-goal", "session-goal");
    expect(store.getGoalTask("create-goal")?.dispatched).toBe(true);
    expect(() => store.markGoalTaskDispatched("create-goal", "other-session")).toThrow(/session/u);
    expect(store.stopGoalContinuation(botUid, target, "session-goal", "pause")).toEqual([]);

    store.markGoalTaskAccepted("create-goal", "goal-exact");
    expect(store.getGoalTask("create-goal")).toMatchObject({
      dispatched: true,
      accepted: true,
      acceptedGoalId: "goal-exact",
      agentAccepted: false,
    });
    store.markGoalTaskAgentAccepted("create-goal");
    expect(store.getGoalTask("create-goal")?.agentAccepted).toBe(true);
    expect(store.getGoalSession(botUid, target, "session-goal")).toMatchObject({
      goalId: "goal-exact",
      continuationAuthorized: false,
      stopIntent: "pause",
    });
    expect(() => store.markGoalTaskAccepted("create-goal", "different-goal")).toThrow(/identity|goal id/u);

    store.updateGoalTaskProgress("create-goal", goal("goal-exact"));
    expect(store.getGoalSession(botUid, target, "session-goal")).toMatchObject({
      status: "active",
      continuationAuthorized: false,
      stopIntent: "pause",
    });
    expect(() => store.updateGoalTaskProgress("create-goal", goal("different-goal"))).toThrow(/identity|goal id/u);
  }));

test("restart recovers only accepted goal requests by exact session and fails closed without receipt", () =>
  withStore((store) => {
    acceptGoal(store, "accepted-goal", "session-accepted", { op: "create", objective: "resume me" });
    expect(store.markStarted("accepted-goal")).toBe(true);
    store.markGoalTaskDispatched("accepted-goal", "session-accepted");
    store.markGoalTaskAccepted("accepted-goal", "goal-accepted");

    acceptGoal(store, "uncertain-goal", "session-uncertain", { op: "create", objective: "do not replay me" });
    expect(store.markStarted("uncertain-goal")).toBe(true);
    store.markGoalTaskDispatched("uncertain-goal", "session-uncertain");

    acceptGoal(store, "not-yet-dispatched", "session-pending", { op: "create", objective: "safe to dispatch" });

    const recovered = store.prepareTasksForRestart(botUid, stopReasons);
    expect(recovered.queued.map(({ id }) => id)).toEqual(["accepted-goal", "not-yet-dispatched"]);
    expect(recovered.uncertain.map(({ id }) => id)).toEqual(["uncertain-goal"]);
    expect(store.getTask("uncertain-goal")?.state).toBe("interrupted");
    expect(store.getGoalTask("uncertain-goal")).toMatchObject({ dispatched: true, accepted: false });
    expect(store.isGoalTaskResuming("accepted-goal")).toBe(true);
    expect(store.getGoalTask("accepted-goal")).toMatchObject({
      sessionId: "session-accepted",
      acceptedGoalId: "goal-accepted",
    });
    expect(store.isGoalTaskResuming("not-yet-dispatched")).toBe(false);
    expect(store.markStarted("accepted-goal")).toBe(true);
    store.markGoalTaskDispatched("accepted-goal", "session-accepted", "recover");
    expect(store.getGoalTask("accepted-goal")).toMatchObject({
      dispatchOp: "recover",
      dispatched: true,
      accepted: true,
      acceptedGoalId: "goal-accepted",
    });

    store.stopGoalContinuation(botUid, target, "session-accepted", "drop");
    expect(store.prepareTasksForRestart(botUid, stopReasons).queued.map(({ id }) => id)).toEqual([
      "not-yet-dispatched",
    ]);
    expect(store.isGoalTaskResuming("accepted-goal")).toBe(false);
  }));

test("goal stops cancel queued goal tasks without cancelling ordinary queued tasks", () =>
  withStore((store) => {
    acceptGoal(store, "create-paused", "session-shared", { op: "create", objective: "hold" });
    expect(store.markStarted("create-paused")).toBe(true);
    store.markGoalTaskDispatched("create-paused", "session-shared");
    store.markGoalTaskAccepted("create-paused", "goal-shared");
    store.finishGoalTaskProgress("create-paused", goal("goal-shared", "paused"));
    store.markCompleted("create-paused", null);

    const ordinary = task("ordinary", "ordinary-queued", "session-shared", "ordinary work");
    expect(store.acceptTask(claim(ordinary.messageId, ordinary.text), ordinary)).toBe(true);
    acceptGoal(store, "resume-queued", "session-shared", { op: "resume" });

    expect(store.stopGoalContinuation(botUid, target, "session-shared", "pause")).toEqual(["resume-queued"]);
    expect(store.getTask("resume-queued")?.state).toBe("cancelled");
    expect(store.getTask("ordinary-queued")?.state).toBe("queued");
    expect(store.countQueuedTasks(botUid, target)).toBe(1);
    expect(store.getGoalSession(botUid, target, "session-shared")).toMatchObject({
      goalId: "goal-shared",
      continuationAuthorized: false,
      stopIntent: "pause",
      status: "paused",
    });
  }));

test("a pending drop intent revokes recovery without fabricating native status", () =>
  withStore((store) => {
    acceptGoal(store, "drop-pending", "session-drop-pending", {
      op: "create",
      objective: "drop only after native confirmation",
    });
    expect(store.markStarted("drop-pending")).toBe(true);
    store.markGoalTaskDispatched("drop-pending", "session-drop-pending");
    store.markGoalTaskAccepted("drop-pending", "goal-drop-pending");
    store.updateGoalTaskProgress("drop-pending", goal("goal-drop-pending", "active"));

    expect(store.stopGoalContinuation(botUid, target, "session-drop-pending", "drop")).toEqual([]);
    expect(store.getGoalSession(botUid, target, "session-drop-pending")).toMatchObject({
      status: "active",
      continuationAuthorized: false,
      stopIntent: "drop",
    });
    store.finishGoalTaskProgress("drop-pending", goal("goal-drop-pending", "dropped"));
    expect(store.getGoalSession(botUid, target, "session-drop-pending")).toMatchObject({
      status: "dropped",
      continuationAuthorized: false,
      stopIntent: "drop",
    });
  }));

test("opening the goal schema twice preserves every legacy task column and row", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-goal-legacy-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const path = join(stateDir, "channel.sqlite");
  const legacy = new Database(path);
  try {
    legacy.exec(`CREATE TABLE tasks (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL,
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      session_id TEXT NOT NULL,
      cwd TEXT NOT NULL,
      state TEXT NOT NULL,
      accepted_at INTEGER NOT NULL,
      started_at INTEGER,
      finished_at INTEGER,
      result_text TEXT,
      error_text TEXT
    )`);
    legacy.query(`INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
      "legacy-task", botUid, 1, target.channelId, "legacy-message", "owner-goal", "legacy payload",
      "legacy-session", "/tmp/legacy", "queued", 17, null, null, null, null,
    );
  } finally {
    legacy.close();
  }

  try {
    for (let pass = 0; pass < 2; pass += 1) {
      const store = await openChannelStore(stateDir);
      try {
        const db = new Database(path, { readonly: true });
        try {
          expect(db.query("SELECT id, bot_uid, channel_type, channel_id, message_id, sender_id, text, session_id, cwd, state, accepted_at, started_at, finished_at, result_text, error_text FROM tasks WHERE id = 'legacy-task'").get()).toEqual({
            id: "legacy-task",
            bot_uid: botUid,
            channel_type: 1,
            channel_id: target.channelId,
            message_id: "legacy-message",
            sender_id: "owner-goal",
            text: "legacy payload",
            session_id: "legacy-session",
            cwd: "/tmp/legacy",
            state: "queued",
            accepted_at: 17,
            started_at: null,
            finished_at: null,
            result_text: null,
            error_text: null,
          });
          expect(db.query("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('goal_tasks', 'goal_sessions') ORDER BY name").all()).toEqual([
            { name: "goal_sessions" },
            { name: "goal_tasks" },
          ]);
        } finally {
          db.close();
        }
        if (pass === 0) {
          const goalTask = task("new-goal-message", "new-goal-task", "new-goal-session");
          expect(store.acceptTask(
            claim(goalTask.messageId, goalTask.text),
            goalTask,
            { op: "create", objective: "migrate then persist", tokenBudget: 90 },
          )).toBe(true);
          expect(store.markStarted(goalTask.id)).toBe(true);
          store.markGoalTaskDispatched(goalTask.id, goalTask.sessionId);
          store.markGoalTaskAccepted(goalTask.id, "goal-after-migration");
        } else {
          expect(store.getGoalTask("new-goal-task")).toMatchObject({
            sessionId: "new-goal-session",
            request: { op: "create", objective: "migrate then persist", tokenBudget: 90 },
            dispatched: true,
            accepted: true,
            acceptedGoalId: "goal-after-migration",
          });
          expect(store.getGoalSession(botUid, target, "new-goal-session")).toMatchObject({
            goalId: "goal-after-migration",
            objective: "migrate then persist",
            continuationAuthorized: true,
          });
        }
      } finally {
        store.close();
      }
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("interrupting an ordinary turn cancels queued goal work but preserves ordinary queued tasks", () =>
  withStore((store) => {
    acceptGoal(store, "goal-before-interrupt", "session-interrupt", {
      op: "create",
      objective: "keep ordinary work",
    });
    expect(store.markStarted("goal-before-interrupt")).toBe(true);
    store.markGoalTaskDispatched("goal-before-interrupt", "session-interrupt");
    store.markGoalTaskAccepted("goal-before-interrupt", "goal-interrupt");
    store.finishGoalTaskProgress("goal-before-interrupt", goal("goal-interrupt", "paused"));
    store.markCompleted("goal-before-interrupt", null);

    const active = task("active-ordinary", "active-ordinary", "session-interrupt", "active ordinary");
    expect(store.acceptTask(claim(active.messageId, active.text), active)).toBe(true);
    expect(store.markStarted(active.id)).toBe(true);
    const ordinary = task("ordinary-queued", "ordinary-queued", "session-interrupt", "preserve me");
    expect(store.acceptTask(claim(ordinary.messageId, ordinary.text), ordinary)).toBe(true);
    acceptGoal(store, "goal-queued-on-interrupt", "session-interrupt", { op: "resume" });

    expect(store.markTaskStopIntent(active.id, "question_timeout")).toBe(true);
    expect(store.getTask("goal-queued-on-interrupt")?.state).toBe("cancelled");
    expect(store.getTask("ordinary-queued")?.state).toBe("queued");
    expect(store.getGoalSession(botUid, target, "session-interrupt")).toMatchObject({
      continuationAuthorized: false,
      stopIntent: "question_timeout",
    });
  }));
test("a paused recovery snapshot preserves a previously authorized native continuation", () =>
  withStore((store) => {
    acceptGoal(store, "paused-snapshot", "session-paused-snapshot", {
      op: "create",
      objective: "recover a paused snapshot",
    });
    expect(store.markStarted("paused-snapshot")).toBe(true);
    store.markGoalTaskDispatched("paused-snapshot", "session-paused-snapshot");
    store.markGoalTaskAccepted("paused-snapshot", "goal-paused-snapshot");
    store.updateGoalTaskProgress("paused-snapshot", goal("goal-paused-snapshot", "active"));
    expect(store.getGoalSession(botUid, target, "session-paused-snapshot")?.continuationAuthorized).toBe(true);

    store.updateGoalTaskProgress("paused-snapshot", goal("goal-paused-snapshot", "paused"));
    expect(store.getGoalSession(botUid, target, "session-paused-snapshot")).toMatchObject({
      status: "paused",
      continuationAuthorized: true,
    });
    const recovery = store.prepareTasksForRestart(botUid, stopReasons);
    expect(recovery.queued.map(({ id }) => id)).toEqual(["paused-snapshot"]);
    expect(recovery.goalStopped).toHaveLength(0);
    expect(store.isGoalTaskResuming("paused-snapshot")).toBe(true);
  }));
