import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, WorkerProcess } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import type { ScheduleArtifact, ScheduleRecord } from "../src/scheduler/types.ts";

const botUid = "schedule-store-bot";
const source: ChatTarget = { channelType: 2, channelId: "schedule-store-group" };
const other: ChatTarget = { channelType: 5, channelId: "schedule-store-group____topic" };
const processIdentity: WorkerProcess = {
  pid: 41_231,
  pgid: 41_231,
  identity: "pid=41231;start=Tue Nov 20 10:00:00 2026",
  cwd: "/tmp",
};

function acceptSourceTask(
  store: ChannelStore,
  taskId: string,
  target: ChatTarget,
  messageId: string,
): void {
  const task: TaskRecord = {
    id: taskId,
    botUid,
    ...target,
    messageId,
    senderId: "schedule-owner",
    text: "direct schedule request",
    sessionId: `session-${taskId}`,
    cwd: "/tmp",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relaySuppressed: false,
  };
  const claim: InboundClaim = {
    botUid,
    target,
    messageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };
  if (!store.acceptTask(claim, task)) throw new Error(`could not durably accept fixture task ${taskId}`);
}

async function withStore<T>(work: (root: string, store: ChannelStore) => Promise<T> | T): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "oco-schedule-store-"));
  const store = await openChannelStore(root);
  try {
    acceptSourceTask(store, "source-task", source, "source-message");
    acceptSourceTask(store, "source-task-2", source, "source-task-2-message");
    acceptSourceTask(store, "other-task", other, "other-task-message");
    return await work(root, store);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

function schedule(overrides: Partial<ScheduleRecord> = {}): ScheduleRecord {
  const sourceTaskId = overrides.sourceTaskId ?? "source-task";
  const sourceMessageId = overrides.sourceMessageId
    ?? (sourceTaskId === "source-task" ? "source-message" : `${sourceTaskId}-message`);
  return {
    id: overrides.id ?? "schedule-1",
    botUid,
    name: "daily report",
    cron: "0 9 * * 1-5",
    timezone: "UTC",
    cwd: "/tmp",
    spec: { kind: "agent", prompt: "Prepare the report" },
    source,
    sourceMessageId,
    sourceRequesterUid: "schedule-owner",
    sourceKind: "direct",
    sourceTaskId,
    toolCallId: "schedule-create-call",
    target: source,
    timeoutMs: 1_800_000,
    createdAt: 1_000,
    status: "active",
    ...overrides,
  };
}

function artifact(overrides: Partial<ScheduleArtifact> = {}): ScheduleArtifact {
  return {
    name: "report.txt",
    type: "file",
    path: "/tmp/report.txt",
    target: source,
    ...overrides,
  };
}

test("the schedule identity migration conservatively fences existing tasks", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-schedule-migration-"));
  let store: ChannelStore | null = await openChannelStore(root);
  try {
    acceptSourceTask(store, "legacy-task", source, "legacy-message");
    const databasePath = store.path;
    store.close();
    store = null;

    const legacyDatabase = new Database(databasePath);
    try {
      legacyDatabase.exec("ALTER TABLE tasks DROP COLUMN schedule_create_identity_mixed");
    } finally {
      legacyDatabase.close(false);
    }

    store = await openChannelStore(root);
    const migratedStore = store;
    expect(() => migratedStore.createSchedule(schedule({
      id: "legacy-schedule",
      sourceTaskId: "legacy-task",
      sourceMessageId: "legacy-message",
      toolCallId: "legacy-create-call",
    }))).toThrow(/permission denied/u);
    expect(migratedStore.listSchedules(botUid, source, 20, 0)).toEqual([]);
  } finally {
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a mixed-identity schedule fence survives restart and blocks duplicate create replay", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-schedule-fence-restart-"));
  let store: ChannelStore | null = await openChannelStore(root);
  try {
    acceptSourceTask(store, "source-task", source, "source-message");
    expect(store.createSchedule(schedule()).kind).toBe("created");
    expect(store.markTaskScheduleIdentityMixed("source-task")).toBe(true);
    store.close();
    store = null;

    store = await openChannelStore(root);
    const reopenedStore = store;
    expect(() => reopenedStore.createSchedule(schedule({ id: "replayed-id" })))
      .toThrow(/permission denied/u);
    expect(reopenedStore.listSchedules(botUid, source, 20, 0)).toHaveLength(1);
  } finally {
    store?.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("schedule creation is idempotent by source task and native tool call and lists only the source chat", async () =>
  withStore(async (_root, store) => {
    const first = store.createSchedule(schedule());
    expect(first.kind).toBe("created");

    const duplicate = store.createSchedule(schedule({
      id: "replayed-id",
      name: "changed retry payload",
      sourceTaskId: "source-task",
      toolCallId: "schedule-create-call",
    }));
    expect(duplicate.kind).toBe("duplicate");
    expect(duplicate.record.id).toBe("schedule-1");

    expect(store.createSchedule(schedule({
      id: "other-source",
      source: other,
      sourceTaskId: "other-task",
      toolCallId: "other-call",
    })).kind).toBe("created");
    expect(store.listSchedules(botUid, source, 20, 0).map(({ id }) => id)).toEqual(["schedule-1"]);
    expect(store.getSchedule(botUid, "schedule-1")).toMatchObject({
      source,
      sourceMessageId: "source-message",
      sourceRequesterUid: "schedule-owner",
      sourceKind: "direct",
      target: source,
      spec: { kind: "agent", prompt: "Prepare the report" },
    });
  }));
test("occurrences are unique and atomically skipped for overlap or exhausted global concurrency", async () =>
  withStore(async (_root, store) => {
    store.createSchedule(schedule());
    store.createSchedule(schedule({
      id: "schedule-2",
      sourceTaskId: "source-task-2",
      toolCallId: "schedule-create-call-2",
    }));

    const first = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 1);
    if (first.kind !== "claimed") throw new Error("expected a new schedule occurrence");
    expect(first.run).toMatchObject({ scheduleId: "schedule-1", scheduledAt: 60_000, status: "claimed" });
    const duplicate = store.claimScheduleOccurrence("schedule-1", 60_000, 60_002, 1);
    if (duplicate.kind !== "duplicate") throw new Error("expected the existing occurrence");
    expect(duplicate.run.id).toBe(first.run.id);

    const overlap = store.claimScheduleOccurrence("schedule-1", 120_000, 120_001, 1);
    if (overlap.kind !== "skipped") throw new Error("expected the overlapping occurrence to be skipped");
    expect(overlap.run.status).toBe("skipped");
    const saturated = store.claimScheduleOccurrence("schedule-2", 120_000, 120_001, 1);
    if (saturated.kind !== "skipped") throw new Error("expected global concurrency to be exhausted");
    expect(saturated.run.status).toBe("skipped");

    expect(store.beginScheduleDispatch(first.run.id, 60_010)).toBe(true);
    expect(store.setScheduleProcess(first.run.id, processIdentity)).toBe(true);
    expect(store.markScheduleProcessExited(first.run.id, 60_020)).toBe(true);
    expect(store.settleScheduleRun(first.run.id, { status: "success", resultText: "done" }, 60_021)?.status).toBe("success");
  }));
test("unsettled stop lookup finds an unknown process outside the paginated run history", async () =>
  withStore(async (_root, store) => {
    store.createSchedule(schedule());
    const claim = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 2);
    if (claim.kind !== "claimed") throw new Error("expected a new occurrence claim");
    const unknown = claim.run;
    expect(store.beginScheduleDispatch(unknown.id, 60_002)).toBe(true);
    expect(store.setScheduleProcess(unknown.id, processIdentity)).toBe(true);
    expect(store.settleScheduleRun(unknown.id, { status: "unknown" }, 60_003)?.status).toBe("unknown");

    for (let minute = 2; minute <= 102; minute += 1) {
      expect(store.claimScheduleOccurrence(
        "schedule-1",
        minute * 60_000,
        minute * 60_000 + 1,
        2,
      ).kind).toBe("skipped");
    }

    expect(store.listScheduleRuns(botUid, "schedule-1", 100, 0).some(run => run.id === unknown.id)).toBe(false);
    expect(store.listScheduleRunsNeedingStop(botUid, "schedule-1").map(run => run.id)).toEqual([unknown.id]);
  }));
test("stopping a schedule preserves late prompt acceptance and fences late completion", async () =>
  withStore(async (_root, store) => {
    store.createSchedule(schedule());
    const claim = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 2);
    if (claim.kind !== "claimed") throw new Error("expected a new schedule occurrence");
    const run = claim.run;
    expect(store.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(store.setScheduleProcess(run.id, processIdentity)).toBe(true);

    expect(store.setScheduleStatus(botUid, "schedule-1", source, "stopped", 60_003)).toMatchObject({ status: "stopped" });
    expect(store.markSchedulePromptAccepted(run.id, 60_004)).toBe(true);
    expect(store.getScheduleRun(botUid, run.id)).toMatchObject({
      stopRequested: true,
      process: processIdentity,
      promptAccepted: true,
    });
    expect(store.settleScheduleRun(run.id, { status: "success", resultText: "late" }, 60_005)?.status).toBe("unknown");
    expect(store.getSchedule(botUid, "schedule-1")?.status).toBe("stopped");
  }));
test("process, exact agent session, and declared artifacts survive reopening without replaying dispatch", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-schedule-store-recovery-"));
  let store = await openChannelStore(root);
  try {
    acceptSourceTask(store, "source-task", source, "source-message");
    store.createSchedule(schedule());
    const claim = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 1);
    if (claim.kind !== "claimed") throw new Error("expected a recoverable occurrence");
    const run = claim.run;
    expect(store.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(store.setScheduleProcess(run.id, processIdentity)).toBe(true);
    expect(store.setScheduleAgentSession(run.id, "session-exact", "/tmp/session-exact.jsonl")).toBe(true);
    expect(store.markSchedulePromptAccepted(run.id, 60_003)).toBe(true);
    expect(store.addScheduleArtifact(run.id, artifact(), "artifact-call", 60_004)).toBe(true);

    store.close();
    store = await openChannelStore(root);
    expect(store.getScheduleRun(botUid, run.id)).toMatchObject({
      process: processIdentity,
      sessionId: "session-exact",
      sessionFile: "/tmp/session-exact.jsonl",
      promptAccepted: true,
      artifacts: [expect.objectContaining({ name: "report.txt", target: source })],
    });
    expect(store.listRecoverableScheduleRuns(botUid).map(({ id }) => id)).toEqual([run.id]);
    expect(store.claimScheduleOccurrence("schedule-1", 120_000, 120_001, 1).kind).toBe("skipped");
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
test("artifact registration rejects a target other than the plan's authorized destination", async () =>
  withStore(async (_root, store) => {
    store.createSchedule(schedule());
    const claim = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 1);
    if (claim.kind !== "claimed") throw new Error("expected an occurrence for artifact registration");
    const run = claim.run;
    expect(store.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(store.addScheduleArtifact(run.id, artifact({ target: other }), "artifact-call", 60_003)).toBe(false);
    expect(store.getScheduleRun(botUid, run.id)?.artifacts).toEqual([]);
  }));

