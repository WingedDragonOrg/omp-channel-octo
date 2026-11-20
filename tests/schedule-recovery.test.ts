import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, WorkerFactory, WorkerProcess } from "../src/contracts.ts";
import type { InboundClaim, TaskRecord } from "../src/store/store-records.ts";
import { FakeWorkerFactory } from "./fixtures/core-fakes.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import { ScheduleCoordinator } from "../src/scheduler/index.ts";
import type { ScheduleClock, ScheduleRecord, ScheduleScriptRequest, ScheduleScriptResult, ScheduleScriptRunner } from "../src/scheduler/types.ts";
import { projectSessionDir } from "../src/session/session-dir.ts";

const botUid = "schedule-recovery-bot";
const target: ChatTarget = { channelType: 1, channelId: "schedule-owner" };
const processIdentity: WorkerProcess = {
  pid: 41_231,
  pgid: 41_231,
  identity: "pid=41231;start=Tue Nov 20 10:00:00 2026",
  cwd: "/tmp",
};

async function withStoreRoot<T>(
  work: (root: string, store: () => ChannelStore, reopen: () => Promise<ChannelStore>) => Promise<T>,
): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "oco-schedule-recovery-"));
  let currentStore = await openChannelStore(root);
  try {
    return await work(root, () => currentStore, async () => {
      currentStore.close();
      currentStore = await openChannelStore(root);
      return currentStore;
    });
  } finally {
    currentStore.close();
    await rm(root, { recursive: true, force: true });
  }
}

async function waitForSettledRun(store: ChannelStore, runId: string) {
  for (let turn = 0; turn < 50; turn += 1) {
    const run = store.getScheduleRun(botUid, runId);
    if (run?.stage === "settled" && run.endedAt !== null) return run;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
  }
  throw new Error(`schedule run ${runId} did not settle`);
}

function schedule(overrides: Partial<ScheduleRecord> = {}): ScheduleRecord {
  return {
    id: "schedule-1",
    botUid,
    name: "minute recovery check",
    cron: "* * * * *",
    timezone: "UTC",
    cwd: "/tmp",
    spec: { kind: "script", runner: { kind: "shell", script: "printf 'ran\\n'" } },
    source: target,
    sourceMessageId: "source-message",
    sourceRequesterUid: "schedule-owner",
    sourceKind: "direct",
    sourceTaskId: "source-task",
    toolCallId: "schedule-create-call",
    target,
    timeoutMs: 1_800_000,
    createdAt: 1,
    status: "active",
    ...overrides,
  };
}

function taskRecord(id: string): TaskRecord {
  return {
    ...target,
    id,
    botUid,
    messageId: `message-${id}`,
    senderId: "schedule-owner",
    text: "Stop the schedule.",
    sessionId: `session-${id}`,
    cwd: "/tmp",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relaySuppressed: false,
  };
}

function createScheduleForAcceptedSource(store: ChannelStore, record: ScheduleRecord): void {
  const task: TaskRecord = {
    ...record.source,
    id: record.sourceTaskId,
    botUid: record.botUid,
    messageId: record.sourceMessageId,
    senderId: record.sourceRequesterUid ?? `synthetic-${record.source.channelId}`,
    text: "Create this schedule.",
    sessionId: `session-${record.sourceTaskId}`,
    cwd: record.cwd,
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: false,
    relaySuppressed: false,
  };
  const claim: InboundClaim = {
    botUid: record.botUid,
    target: record.source,
    messageId: record.sourceMessageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };
  if (!store.acceptTask(claim, task)) throw new Error("schedule source fixture task was not accepted");
  if (store.createSchedule(record).kind !== "created") throw new Error("schedule fixture was not created");
}

async function agentSessionFixture(
  sessionRoot: string,
  cwd: string,
  scheduleId: string,
  runId: string,
) {
  const sessionId = "schedule-session-734";
  const sessionDir = join(projectSessionDir(sessionRoot, cwd), "scheduler", scheduleId, runId);
  const sessionFile = join(sessionDir, `${sessionId}.jsonl`);
  await mkdir(cwd, { recursive: true });
  await mkdir(sessionDir, { recursive: true, mode: 0o700 });
  await writeFile(sessionFile, `${JSON.stringify({ type: "session", version: 3, id: sessionId, cwd })}\n`, "utf8");
  return { sessionDir, sessionId, sessionFile };
}

class ManualScheduleClock implements ScheduleClock {
  private readonly timers = new Map<NodeJS.Timeout, { callback: () => void; at: number }>();
  private nextId = 0;

  constructor(private currentTime: number) {}

  now(): number {
    return this.currentTime;
  }

  setTimeout(callback: () => void, delayMs: number): NodeJS.Timeout {
    const timer = { id: ++this.nextId } as unknown as NodeJS.Timeout;
    this.timers.set(timer, { callback, at: this.currentTime + Math.max(0, delayMs) });
    return timer;
  }

  clearTimeout(timer: NodeJS.Timeout): void {
    this.timers.delete(timer);
  }

  fireNext(repetitions = 1): void {
    const next = [...this.timers.entries()].sort(([, left], [, right]) => left.at - right.at)[0];
    if (!next) throw new Error("no scheduled timer");
    const [timer, entry] = next;
    this.currentTime = entry.at;
    this.timers.delete(timer);
    for (let index = 0; index < repetitions; index += 1) entry.callback();
  }
}

function recoveryWorkerFactory(recover: WorkerFactory["recover"]): WorkerFactory {
  return {
    create: async () => { throw new Error("unexpected OMP worker creation in script recovery test"); },
    createCommit: async () => { throw new Error("unexpected commit worker creation in script recovery test"); },
    recover,
  };
}

function coordinatorOptions(
  root: string,
  store: ChannelStore,
  clock: ScheduleClock,
  runScript: ScheduleScriptRunner,
  recover: WorkerFactory["recover"] = async () => true,
) {
  return {
    store,
    workerFactory: recoveryWorkerFactory(recover),
    botUid: () => botUid,
    stateDir: root,
    sessionRoot: join(root, "sessions"),
    clock,
    runScript,
    createRecord: async () => { throw new Error("schedule creation is outside this recovery test"); },
    classifyAccess: async (task: TaskRecord, record: ScheduleRecord) =>
      task.botUid === record.botUid
      && task.channelType === record.source.channelType
      && task.channelId === record.source.channelId
        ? "source"
        : "denied",
    validateExecution: async () => true,
    cliReady: () => true,
  };
}

test("duplicate delivery of one UTC occurrence produces one script side effect", async () =>
  withStoreRoot(async (root, getStore) => {
    const store = getStore();
    createScheduleForAcceptedSource(store, schedule());
    const clock = new ManualScheduleClock(59_000);
    let scriptRuns = 0;
    const entered = Promise.withResolvers<void>();
    const runScript: ScheduleScriptRunner = async ({ onSpawn, onExit }) => {
      scriptRuns += 1;
      entered.resolve();
      onSpawn(processIdentity);
      onExit();
      return { kind: "completed", exitCode: 0, stdout: "ran", stderr: "" };
    };
    const coordinator = new ScheduleCoordinator(coordinatorOptions(root, store, clock, runScript));
    try {
      await coordinator.start();
      clock.fireNext(2);
      await entered.promise;
      await coordinator.stop();

      const occurrence = store.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 2);
      expect(scriptRuns).toBe(1);
      expect(occurrence.kind).toBe("duplicate");
      if (occurrence.kind !== "duplicate") throw new Error("the occurrence should already be claimed");
      expect(store.getScheduleRun(botUid, occurrence.run.id)).toMatchObject({
        scheduledAt: 60_000,
        status: "success",
      });
    } finally {
      await coordinator.stop();
    }
  }));

test("a dispatched occurrence with an unknown result is not replayed after restart", async () =>
  withStoreRoot(async (root, getStore, reopen) => {
    const firstStore = getStore();
    createScheduleForAcceptedSource(firstStore, schedule());
    const claim = firstStore.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 2);
    expect(claim.kind).toBe("claimed");
    if (claim.kind !== "claimed") throw new Error("expected a new occurrence claim");
    const run = claim.run;
    expect(firstStore.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(firstStore.setScheduleProcess(run.id, processIdentity)).toBe(true);
    await reopen();

    let scriptRuns = 0;
    const runScript: ScheduleScriptRunner = async () => {
      scriptRuns += 1;
      return { kind: "completed", exitCode: 0, stdout: "replayed", stderr: "" };
    };
    const clock = new ManualScheduleClock(61_000);
    const coordinator = new ScheduleCoordinator(coordinatorOptions(
      root,
      getStore(),
      clock,
      runScript,
      async () => true,
    ));
    try {
      await coordinator.start();
      await coordinator.stop();

      const recovered = getStore().getScheduleRun(botUid, run.id);
      const sameOccurrence = getStore().claimScheduleOccurrence("schedule-1", 60_000, 61_001, 2);
      const nextOccurrence = getStore().claimScheduleOccurrence("schedule-1", 120_000, 120_001, 2);
      expect(scriptRuns).toBe(0);
      expect(recovered).toMatchObject({ scheduledAt: 60_000, status: "unknown" });
      expect(sameOccurrence).toMatchObject({ kind: "duplicate", run: { id: run.id } });
      expect(nextOccurrence).toMatchObject({ kind: "skipped", run: { status: "skipped" } });
    } finally {
      await coordinator.stop();
    }
  }));

test("the schedule host tool stop waits for a dispatched script's process-exit handshake", async () =>
  withStoreRoot(async (root, getStore) => {
    const store = getStore();
    createScheduleForAcceptedSource(store, schedule());
    const clock = new ManualScheduleClock(59_000);
    const entered = Promise.withResolvers<ScheduleScriptRequest>();
    const aborted = Promise.withResolvers<void>();
    const result = Promise.withResolvers<ScheduleScriptResult>();
    let scriptRuns = 0;
    const runScript: ScheduleScriptRunner = async (request) => {
      scriptRuns += 1;
      entered.resolve(request);
      if (request.signal.aborted) aborted.resolve();
      else request.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      return result.promise;
    };
    const coordinator = new ScheduleCoordinator(coordinatorOptions(root, store, clock, runScript));
    try {
      await coordinator.start();
      clock.fireNext();
      const running = await entered.promise;

      let stopResolved = false;
      const stopTool = coordinator.createTool(taskRecord("schedule-stop-task"));
      const stopping = stopTool.execute(
        { action: "stop", schedule_id: "schedule-1" },
        new AbortController().signal,
        "schedule-stop-call",
      ).then((outcome) => {
        stopResolved = true;
        return outcome;
      });
      await aborted.promise;
      expect(store.getSchedule(botUid, "schedule-1")?.status).toBe("stopped");
      expect(running.signal.aborted).toBe(true);
      expect(stopResolved).toBe(false);

      running.onSpawn(processIdentity);
      expect(store.getScheduleRun(botUid, running.run.id)).toMatchObject({
        stopRequested: true,
        process: processIdentity,
      });
      expect(stopResolved).toBe(false);

      running.onExit();
      result.resolve({ kind: "cancelled", processExited: true, stdout: "", stderr: "" });
      const stopped = JSON.parse(await stopping) as {
        kind: string;
        schedule: { status: string };
        runs: Array<{ id: string; status: string }>;
      };
      expect(stopped).toMatchObject({ kind: "stopped", schedule: { status: "stopped" } });
      expect(stopped.runs).toContainEqual(expect.objectContaining({ id: running.run.id, status: "unknown" }));
      expect(store.getScheduleRun(botUid, running.run.id)).toMatchObject({
        status: "unknown",
        processExited: true,
        errorText: expect.stringContaining("uncertain"),
      });
      expect(scriptRuns).toBe(1);
    } finally {
      await coordinator.stop();
    }
  }));

test("an old process identity that cannot be confirmed stays protected and blocks another run", async () =>
  withStoreRoot(async (root, getStore, reopen) => {
    const initialStore = getStore();
    createScheduleForAcceptedSource(initialStore, schedule());
    const claim = initialStore.claimScheduleOccurrence("schedule-1", 60_000, 60_001, 2);
    expect(claim.kind).toBe("claimed");
    if (claim.kind !== "claimed") throw new Error("expected a new occurrence claim");
    const run = claim.run;
    expect(initialStore.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(initialStore.setScheduleProcess(run.id, processIdentity)).toBe(true);
    await reopen();

    const recoveredProcesses: WorkerProcess[] = [];
    let scriptRuns = 0;
    const coordinator = new ScheduleCoordinator(coordinatorOptions(
      root,
      getStore(),
      new ManualScheduleClock(61_000),
      async () => {
        scriptRuns += 1;
        return { kind: "completed", exitCode: 0, stdout: "unsafe replay", stderr: "" };
      },
      async (process) => {
        recoveredProcesses.push(process);
        return false;
      },
    ));
    try {
      await coordinator.start();
      await coordinator.stop();

      expect(recoveredProcesses).toEqual([processIdentity]);
      expect(scriptRuns).toBe(0);
      expect(getStore().getScheduleRun(botUid, run.id)).toMatchObject({
        status: "unknown",
        process: processIdentity,
      });
      expect(getStore().claimScheduleOccurrence("schedule-1", 120_000, 120_001, 2)).toMatchObject({
        kind: "skipped",
        run: { status: "skipped" },
      });
    } finally {
      await coordinator.stop();
    }
  }));
test("accepted agent recovery resumes the bound session without replaying its original prompt", async () =>
  withStoreRoot(async (root, getStore, reopen) => {
    const workspace = join(root, "agent-workspace");
    await mkdir(workspace, { recursive: true });
    const cwd = await realpath(workspace);
    const sessionRootPath = join(root, "sessions");
    await mkdir(sessionRootPath, { recursive: true, mode: 0o700 });
    const sessionRoot = await realpath(sessionRootPath);
    const initialStore = getStore();
    createScheduleForAcceptedSource(initialStore, schedule({
      id: "agent-schedule",
      name: "agent recovery",
      cwd,
      spec: { kind: "agent", prompt: "Prepare the recovered report" },
      sourceTaskId: "agent-source-task",
      toolCallId: "agent-create-call",
    }));
    const claim = initialStore.claimScheduleOccurrence("agent-schedule", 60_000, 60_001, 2);
    if (claim.kind !== "claimed") throw new Error("expected a new agent occurrence claim");
    const run = claim.run;
    const { sessionDir, sessionId, sessionFile } = await agentSessionFixture(
      sessionRoot,
      cwd,
      "agent-schedule",
      run.id,
    );
    const process = { ...processIdentity, cwd };
    expect(initialStore.beginScheduleDispatch(run.id, 60_002)).toBe(true);
    expect(initialStore.setScheduleProcess(run.id, process)).toBe(true);
    expect(initialStore.setScheduleAgentSession(run.id, sessionId, sessionFile)).toBe(true);
    expect(initialStore.markSchedulePromptAccepted(run.id, 60_003)).toBe(true);
    await reopen();

    const resumeEffect = Promise.withResolvers<void>();
    const workerFactory = new FakeWorkerFactory(async ({ text, append, emit, sessionId: resumedSessionId }) => {
      await append("effects.log", `${text}\n`);
      emit({
        type: "text",
        messageId: `schedule-resume-${resumedSessionId}`,
        text: "recovered report",
        final: true,
      });
      resumeEffect.resolve();
    });
    workerFactory.recovered = true;
    const coordinator = new ScheduleCoordinator({
      store: getStore(),
      workerFactory,
      botUid: () => botUid,
      stateDir: root,
      sessionRoot,
      clock: new ManualScheduleClock(61_000),
      createRecord: async () => { throw new Error("schedule creation is outside this recovery test"); },
      classifyAccess: async () => "source",
      validateExecution: async () => true,
      cliReady: () => true,
    });
    try {
      await coordinator.start();
      await resumeEffect.promise;
      const settledRun = await waitForSettledRun(getStore(), run.id);
      await coordinator.stop();

      expect(workerFactory.creations[0]).toMatchObject({ cwd, sessionDir, sessionFile });
      expect(workerFactory.workers[0]?.sessionId).toBe(sessionId);
      expect(workerFactory.prompts.map(({ text }) => text)).toEqual(["/retry"]);
      expect(await readFile(join(cwd, "effects.log"), "utf8")).toBe("/retry\n");
      expect(settledRun).toMatchObject({
        status: "success",
        sessionId,
        sessionFile,
        promptAccepted: true,
        resultText: expect.stringContaining("recovered report"),
      });
    } finally {
      await coordinator.stop();
    }
  }));
