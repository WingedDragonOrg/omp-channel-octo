import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import type { BotCardAction, ChatTarget } from "../src/contracts";
import type {
  BindingRecord,
  InboundClaim,
  TaskRecord,
  ThreadWorkspaceIntentInput,
  ThreadWorkspaceRecord,
} from "../src/store/store-records";
import { openChannelStore, type ChannelStore } from "../src/store";
import {
  cardActionEvent,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  submitAndWaitForIngressHandoff,
  type CoreHarness,
  waitForStatus,
} from "./fixtures/core-fakes";

const bot = { uid: "bot-core-workspace-ownership", name: "Workspace Fixture Bot" };
type ThreadTarget = ChatTarget & { channelType: 5 };
const thread: ThreadTarget = { channelId: "group-tools____thread-1", channelType: 5 };
const mergeNotice = "当前为多项目工作区，不支持 /thread merge。";
const unavailableNotice = "当前子区工作区未就绪或资源已缺失，未接收任务。";

function ownerMessage(
  harness: CoreHarness,
  target: ThreadTarget,
  messageId: string,
  text: string,
) {
  return inbound({
    messageId,
    ...target,
    senderId: "owner-1",
    text,
    mentionedUserIds: [harness.bot.uid],
  });
}

function workspaceRecord(
  target: ThreadTarget,
  entryPath: string | null,
  projects: readonly { name: string; path: string; created?: boolean }[],
  status: ThreadWorkspaceRecord["status"] = "ready",
  ownerBotUid = bot.uid,
  layout: ThreadWorkspaceRecord["layout"] = "shared",
): ThreadWorkspaceRecord {
  return {
    botUid: ownerBotUid,
    channelType: 5,
    channelId: target.channelId,
    layout,
    status,
    entryPath,
    projects: projects.map(({ name, path, created = true }) => ({
      name,
      repo: `/repos/${name}`,
      path,
      branch: `thread-${name}`,
      created,
    })),
    error: status === "failed" ? "fixture workspace preparation failed" : null,
  };
}

async function withStore<T>(
  root: string,
  work: (store: ChannelStore) => T | Promise<T>,
): Promise<T> {
  const store = await openChannelStore(join(root, "state"));
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

function taskRow(target: ThreadTarget, id: string, cwd: string): TaskRecord {
  const messageId = `message-${id}`;
  const text = `task ${id}`;
  return {
    id,
    botUid: bot.uid,
    channelType: target.channelType,
    channelId: target.channelId,
    messageId,
    senderId: "owner-1",
    text,
    sessionId: `session-${id}`,
    cwd,
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
}

function seedQueuedTask(store: ChannelStore, task: TaskRecord): void {
  const claim: InboundClaim = {
    botUid: task.botUid,
    target: { channelType: task.channelType, channelId: task.channelId },
    messageId: task.messageId,
    senderId: task.senderId,
    text: task.text,
    supported: true,
    kind: "task",
  };
  expect(store.acceptTask(claim, task)).toBe(true);
}

async function waitForTaskState(root: string, taskId: string, state: TaskRecord["state"]): Promise<TaskRecord> {
  const store = await openChannelStore(join(root, "state"));
  try {
    for (let attempt = 0; attempt < 10_000; attempt += 1) {
      const task = store.getTask(taskId);
      if (task?.state === state) return task;
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
  } finally {
    store.close();
  }
  throw new Error(`task ${taskId} did not reach ${state}`);
}

async function waitForLatestTask(
  root: string,
  botUid: string,
  target: ChatTarget,
  predicate: (task: TaskRecord) => boolean,
): Promise<TaskRecord> {
  const store = await openChannelStore(join(root, "state"));
  try {
    for (let attempt = 0; attempt < 10_000; attempt += 1) {
      const task = store.getLatestTask(botUid, target);
      if (task !== null && predicate(task)) return task;
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
  } finally {
    store.close();
  }
  throw new Error(`latest task for ${target.channelType}:${target.channelId} did not reach expectation`);
}

function waitForNotice(harness: CoreHarness, target: ChatTarget, text: string) {
  const earlierRequests = new Set(harness.transport.textRequests);
  return harness.transport.waitForTextRequest((request) =>
    !earlierRequests.has(request)
    && request.operation === "send"
    && request.text === text
    && request.target.channelType === target.channelType
    && request.target.channelId === target.channelId,
  );
}

async function expectIngressReceipt(root: string, jobId: number): Promise<void> {
  expect(await withStore(root, (store) => store.getOutbox(`ingress:${jobId}:receipt:0`))).not.toBeNull();
}

test("preparing and failed thread ownership blocks inbound work before steering", async () => {
  const started = Promise.withResolvers<void>();
  let appendedText: string | null = null;
  const harness = await createCoreHarness({
    bot,
    prompt: async ({ waitForAppend, onStop }) => {
      const stopped = Promise.withResolvers<void>();
      onStop(stopped.resolve);
      started.resolve();
      const appended = waitForAppend().then((message) => {
        appendedText = message.text;
      });
      await Promise.race([appended, stopped.promise]);
    },
  });
  try {
    const rawEntry = join(harness.root, "thread-entry");
    const rawProject = join(rawEntry, "api");
    await mkdir(rawProject, { recursive: true });
    const entry = await realpath(rawEntry);
    const project = await realpath(rawProject);
    const newJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "ownership-new", `/new ${entry}`),
    );
    await expectIngressReceipt(harness.root, newJob.id);

    const preparing = workspaceRecord(thread, entry, [{ name: "api", path: project, created: false }], "preparing");
    await withStore(harness.root, (store) => store.saveThreadWorkspace(preparing));
    const preparingNotice = waitForNotice(harness, thread, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "ownership-preparing", "must not start"),
    );
    await preparingNotice;
    expect(await withStore(harness.root, (store) => store.getLatestTask(bot.uid, thread))).toBeNull();

    const failed = workspaceRecord(thread, entry, [{ name: "api", path: project, created: false }], "failed");
    await withStore(harness.root, (store) => store.saveThreadWorkspace(failed));
    const failedNotice = waitForNotice(harness, thread, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "ownership-failed", "must still not start"),
    );
    await failedNotice;
    expect(await withStore(harness.root, (store) => store.getLatestTask(bot.uid, thread))).toBeNull();

    const ready = workspaceRecord(thread, entry, [{ name: "api", path: project }]);
    await withStore(harness.root, (store) => store.saveThreadWorkspace(ready));
    const startJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "ownership-running", "hold turn"),
    );
    if (startJob.consumerKind !== "task" || startJob.consumerId === null) {
      throw new Error("running workspace input was not durably accepted as a task");
    }
    await started.promise;
    const running = await waitForTaskState(harness.root, startJob.consumerId, "running");

    await withStore(harness.root, (store) => store.saveThreadWorkspace({ ...ready, status: "failed" }));
    const steerNotice = waitForNotice(harness, thread, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "ownership-steer", "must not steer"),
    );
    await steerNotice;

    expect(appendedText).toBeNull();
    expect(await withStore(harness.root, (store) => store.getLatestTask(bot.uid, thread)?.id)).toBe(running.id);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["hold turn"]);
    expect(harness.transport.sent.some(({ text }) => text === unavailableNotice)).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a ready shared owner without a binding bootstraps its first task inside the entry", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-first-binding-"));
  let harness: CoreHarness | undefined;
  const rawEntry = join(root, "thread-entry");
  const rawProject = join(rawEntry, "api");
  await mkdir(rawProject, { recursive: true });
  const entry = await realpath(rawEntry);
  const project = await realpath(rawProject);
  try {
    await withStore(root, (store) =>
      store.saveThreadWorkspace(workspaceRecord(thread, entry, [{ name: "api", path: project }])),
    );
    harness = await createCoreHarness({ root, bot });
    const firstJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "first-workspace-task", "first work"),
    );
    if (firstJob.consumerKind !== "task" || firstJob.consumerId === null) {
      throw new Error("first workspace input was not durably accepted as a task");
    }
    const completed = await waitForTaskState(root, firstJob.consumerId, "completed");
    expect(completed.cwd).toBe(entry);

    expect(await withStore(root, (store) => store.getBinding(bot.uid, thread)?.cwd)).toBe(entry);
    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, thread)?.cwd)).toBe(entry);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["first work"]);
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("startup recovery fails closed when a ready shared project has disappeared", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-missing-"));
  let harness: CoreHarness | undefined;
  const target = { channelId: "group-tools____thread-missing", channelType: 5 as const };
  const entryPath = join(root, "thread-entry");
  const projectPath = join(entryPath, "api");
  await mkdir(projectPath, { recursive: true });
  const entry = await realpath(entryPath);
  const project = await realpath(projectPath);
  const task = taskRow(target, "missing-project-task", project);
  const binding: BindingRecord = {
    botUid: bot.uid,
    channelId: target.channelId,
    channelType: 5,
    sessionId: task.sessionId,
    cwd: entry,
    sessionFile: null,
    cwdPinned: true,
  };
  try {
    await withStore(root, (store) => {
      store.ensureBinding(binding);
      store.saveThreadWorkspace(workspaceRecord(target, entry, [{ name: "api", path: project }]));
      seedQueuedTask(store, task);
    });
    await rm(project, { recursive: true, force: true });
    harness = await createCoreHarness({ root, bot });

    const failed = await waitForTaskState(root, task.id, "failed");
    expect(failed.errorText).toBe(unavailableNotice);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(await withStore(root, (store) => store.getBinding(bot.uid, target))).toMatchObject({
      sessionId: task.sessionId,
      cwd: entry,
      cwdPinned: true,
    });
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("a missing explicit single-project workspace stays blocked instead of falling back to the parent", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-explicit-single-missing-"));
  const parentPath = join(root, "parent-cwd");
  const projectPath = join(root, "explicit-project");
  await Promise.all([
    mkdir(parentPath, { recursive: true }),
    mkdir(projectPath, { recursive: true }),
  ]);
  const parentCwd = await realpath(parentPath);
  const project = await realpath(projectPath);
  const target: ThreadTarget = { channelId: "group-tools____thread-explicit-single-missing", channelType: 5 };
  const record = workspaceRecord(target, null, [{ name: "api", path: project }], "ready", bot.uid, "single");
  const binding: BindingRecord = {
    botUid: bot.uid,
    channelId: target.channelId,
    channelType: 5,
    sessionId: "explicit-single-session",
    cwd: project,
    sessionFile: null,
    cwdPinned: true,
  };
  let harness: CoreHarness | undefined;
  try {
    await withStore(root, (store) => {
      store.ensureBinding({
        botUid: bot.uid,
        channelId: "group-tools",
        channelType: 2,
        sessionId: "parent-session",
        cwd: parentCwd,
        sessionFile: null,
        cwdPinned: false,
      });
      store.ensureBinding(binding);
      store.saveThreadWorkspace(record);
    });
    await rm(projectPath, { recursive: true, force: true });
    harness = await createCoreHarness({ root, bot });

    const unavailable = waitForNotice(harness, target, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "explicit-single-missing-inbound", "must stay blocked"),
    );
    await unavailable;

    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, target))).toBeNull();
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.some(({ text }) => text === unavailableNotice)).toBe(true);
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, target)?.legacyWorktreeName))
      .toBeUndefined();
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("/new and /move preserve workspace ownership while explicit cwd changes detach the binding", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-retain-"));
  let harness: CoreHarness | undefined;
  const rawEntry = join(root, "thread-entry");
  const rawProject = join(rawEntry, "api");
  const newCwd = join(root, "new-cwd");
  const movedCwd = join(root, "moved-cwd");
  await Promise.all([
    mkdir(rawProject, { recursive: true }),
    mkdir(newCwd, { recursive: true }),
    mkdir(movedCwd, { recursive: true }),
  ]);
  const entry = await realpath(rawEntry);
  const project = await realpath(rawProject);
  const canonicalNewCwd = await realpath(newCwd);
  const canonicalMovedCwd = await realpath(movedCwd);
  const workspace = workspaceRecord(thread, entry, [{ name: "api", path: project }]);
  const binding: BindingRecord = {
    botUid: bot.uid,
    channelId: thread.channelId,
    channelType: 5,
    sessionId: "workspace-retained-session",
    cwd: entry,
    sessionFile: null,
    cwdPinned: true,
  };
  try {
    await withStore(root, (store) => {
      store.ensureBinding(binding);
      store.saveThreadWorkspace(workspace);
    });
    harness = await createCoreHarness({ root, bot });

    const defaultNew = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "retained-new-default", "/new"),
    );
    await expectIngressReceipt(root, defaultNew.id);
    expect(await withStore(root, (store) => store.getBinding(bot.uid, thread))).toMatchObject({
      cwd: entry,
      cwdPinned: true,
    });
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, thread))).toEqual(workspace);

    await rm(project, { recursive: true, force: true });
    const explicitNew = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "retained-new-explicit", `/new ${canonicalNewCwd}`),
    );
    await expectIngressReceipt(root, explicitNew.id);
    const explicitNewBinding = await withStore(root, (store) => store.getBinding(bot.uid, thread));
    expect(explicitNewBinding).toMatchObject({ cwd: canonicalNewCwd });
    expect(explicitNewBinding?.cwdPinned).not.toBe(true);
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, thread))).toEqual(workspace);

    const moved = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "retained-move-explicit", `/move ${canonicalMovedCwd}`),
    );
    await expectIngressReceipt(root, moved.id);
    const movedBinding = await withStore(root, (store) => store.getBinding(bot.uid, thread));
    expect(movedBinding).toMatchObject({ cwd: canonicalMovedCwd });
    expect(movedBinding?.cwdPinned).not.toBe(true);
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, thread))).toEqual(workspace);
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("an unconfirmed worker inside one shared project blocks only overlapping tasks after restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-blocked-"));
  let harness: CoreHarness | undefined;
  const target: ThreadTarget = { channelId: "group-tools____thread-blocked", channelType: 5 };
  const laterTarget: ThreadTarget = { channelId: "group-tools____thread-blocked-later", channelType: 5 };
  const entryPath = join(root, "thread-entry");
  const projectAPath = join(entryPath, "api");
  const projectASourcePath = join(projectAPath, "src");
  const projectBPath = join(entryPath, "web");
  await Promise.all([
    mkdir(projectASourcePath, { recursive: true }),
    mkdir(projectBPath, { recursive: true }),
  ]);
  const entry = await realpath(entryPath);
  const projectA = await realpath(projectAPath);
  const projectASource = await realpath(projectASourcePath);
  const projectB = await realpath(projectBPath);
  const task = taskRow(target, "blocked-project-task", projectASource);
  const laterTask = taskRow(laterTarget, "persisted-independent-project-task", projectB);
  const binding: BindingRecord = {
    botUid: bot.uid,
    channelId: target.channelId,
    channelType: 5,
    sessionId: task.sessionId,
    cwd: projectASource,
    sessionFile: null,
    cwdPinned: true,
  };
  const laterBinding: BindingRecord = {
    botUid: bot.uid,
    channelId: laterTarget.channelId,
    channelType: 5,
    sessionId: laterTask.sessionId,
    cwd: projectB,
    sessionFile: null,
    cwdPinned: true,
  };
  try {
    await withStore(root, (store) => {
      store.ensureBinding(binding);
      store.ensureBinding(laterBinding);
      store.saveThreadWorkspace(workspaceRecord(target, entry, [
        { name: "api", path: projectA },
        { name: "web", path: projectB },
      ]));
      store.recordWorker({
        id: "unconfirmed-api-worker",
        botUid: bot.uid,
        sessionId: "old-shared-worker-session",
        sessionFile: join(projectASource, "old-session.jsonl"),
        pid: 4242,
        pgid: 4242,
        identity: "fixture-unconfirmed-shared-worker",
        cwd: projectASource,
        moveTargetCwd: null,
        state: "running",
      });
      seedQueuedTask(store, task);
      seedQueuedTask(store, laterTask);
    });

    const workers = new FakeWorkerFactory();
    workers.recovered = false;
    harness = await createCoreHarness({ root, bot, workers });
    await waitForStatus(harness.service, target, (status) => status.queued === 1);
    await waitForStatus(harness.service, laterTarget, (status) => status.state === "completed");

    expect(workers.prompts.map(({ text }) => text)).toEqual(["task persisted-independent-project-task"]);
    expect(await withStore(root, (store) => store.getTask(task.id)?.state)).toBe("queued");
    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, laterTarget)?.state)).toBe("completed");
    expect(await withStore(root, (store) => store.listQueuedTasks())).toEqual([expect.objectContaining({ id: task.id })]);
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("shared /thread merge returns the exact notice without probing or creating a task", async () => {
  const harness = await createCoreHarness({ bot });
  try {
    const rawEntry = join(harness.root, "thread-entry");
    const rawApi = join(rawEntry, "api");
    const rawWeb = join(rawEntry, "web");
    await Promise.all([
      mkdir(rawApi, { recursive: true }),
      mkdir(rawWeb, { recursive: true }),
    ]);
    const entry = await realpath(rawEntry);
    const api = await realpath(rawApi);
    const web = await realpath(rawWeb);
    const bindJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "shared-merge-bind", `/new ${entry}`),
    );
    await expectIngressReceipt(harness.root, bindJob.id);
    await withStore(harness.root, (store) =>
      store.saveThreadWorkspace(workspaceRecord(thread, entry, [
        { name: "api", path: api },
        { name: "web", path: web },
      ])),
    );

    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected the fake worktree port");
    let worktrunkCalls = 0;
    const originalWorktrunk = worktrees.worktrunk.bind(worktrees);
    worktrees.worktrunk = async () => {
      worktrunkCalls += 1;
      return originalWorktrunk();
    };
    const promptCount = harness.workers.prompts.length;
    const notice = waitForNotice(harness, thread, mergeNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "shared-merge-command", "/thread merge"),
    );
    await notice;

    expect(worktrunkCalls).toBe(0);
    expect(harness.workers.prompts).toHaveLength(promptCount);
    expect(harness.transport.sent.some(({ text }) => text === mergeNotice)).toBe(true);
    expect(worktrees.merges).toHaveLength(0);
    expect(await withStore(harness.root, (store) => store.getLatestTask(bot.uid, thread))).toBeNull();
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a failed shared-workspace recovery releases asks so a late card answer wakes again", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-recovery-"));
  const entryPath = join(root, "thread-entry");
  const projectPath = join(entryPath, "api");
  await mkdir(projectPath, { recursive: true });
  const entry = await realpath(entryPath);
  const project = await realpath(projectPath);
  const firstTransport = new FakeTransport({ bot });
  firstTransport.cardSupported = true;
  let first: CoreHarness | undefined;
  let restarted: CoreHarness | undefined;
  try {
    first = await createCoreHarness({
      root,
      bot,
      transport: firstTransport,
      prompt: async ({ promptContext }) => {
        const ask = promptContext?.tools?.find(({ name }) => name === "ask");
        if (!ask) throw new Error("ask host tool missing");
        try {
          await ask.execute({
            questions: [{
              id: "recovered-workspace-question",
              question: "Should the recovered task continue?",
              options: [{ label: "yes" }, { label: "no" }],
            }],
          }, new AbortController().signal);
        } catch {
          return;
        }
      },
    });
    const bindJob = await submitAndWaitForIngressHandoff(
      first,
      ownerMessage(first, thread, "recovery-bind", `/new ${entry}`),
    );
    await expectIngressReceipt(root, bindJob.id);
    await withStore(root, (store) =>
      store.saveThreadWorkspace(workspaceRecord(thread, entry, [{ name: "api", path: project }])),
    );
    const recoveryJob = await submitAndWaitForIngressHandoff(
      first,
      ownerMessage(first, thread, "recovery-ask", "ask before restart"),
    );
    if (recoveryJob.consumerKind !== "task" || recoveryJob.consumerId === null) {
      throw new Error("recovery ask was not durably accepted as a task");
    }
    await waitForTaskState(root, recoveryJob.consumerId, "running");
    await first.workers.waitForPromptCount(1);
    const waiting = await waitForStatus(
      first.service,
      thread,
      (status) => status.pendingQuestionIds.length === 1,
    );
    const questionId = waiting.pendingQuestionIds[0]!;
    const card = firstTransport.sentCards[0];
    if (!card) throw new Error("recovered ask card was not sent");
    const interruptedTaskId = recoveryJob.consumerId;

    await closeCoreHarness(first, { remove: false });
    first = undefined;
    await withStore(root, (store) => {
      const workspace = store.getThreadWorkspace(bot.uid, thread);
      if (!workspace) throw new Error("shared workspace record missing");
      store.saveThreadWorkspace({
        ...workspace,
        status: "preparing",
        projects: workspace.projects.map((projectRecord) => ({ ...projectRecord, created: false })),
      });
    });

    const resumedTransport = new FakeTransport({ bot });
    resumedTransport.cardSupported = true;
    const resumedPrompts: string[] = [];
    restarted = await createCoreHarness({
      root,
      bot,
      transport: resumedTransport,
      prompt: async ({ text }) => {
        resumedPrompts.push(text);
      },
    });
    await waitForTaskState(root, interruptedTaskId, "failed");
    expect(await withStore(root, (store) => store.getQuestion(questionId))).toMatchObject({
      state: "cancelled",
      closedReason: "ended",
    });

    await withStore(root, (store) => {
      const workspace = store.getThreadWorkspace(bot.uid, thread);
      if (!workspace) throw new Error("shared workspace record missing");
      store.saveThreadWorkspace({
        ...workspace,
        status: "ready",
        projects: workspace.projects.map((projectRecord) => ({ ...projectRecord, created: true })),
      });
    });
    const lateAnswer: BotCardAction = {
      messageId: card.messageId,
      channelType: thread.channelType,
      channelId: thread.channelId,
      actionId: "ask_opt_1",
      operatorUid: "owner-1",
      inputs: {},
    };
    resumedTransport.pushBotEvent(cardActionEvent(801, lateAnswer));
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(
      restarted.service,
      thread,
      (status) => status.state === "completed" && status.pendingQuestionIds.length === 0,
    );
    await resumedTransport.waitForAck(801);

    expect(resumedPrompts).toHaveLength(1);
    expect(resumedPrompts[0]).toContain("[ask 答复]");
    expect(resumedPrompts[0]).toContain("Should the recovered task continue?");
    expect(resumedPrompts[0]).toContain("yes");
  } finally {
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (first) await closeCoreHarness(first, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
});
test("a late ask-card wake cannot steer a shared project after one project disappears", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-late-ask-"));
  const entryPath = join(root, "thread-entry");
  const projectPath = join(entryPath, "api");
  await mkdir(projectPath, { recursive: true });
  const entry = await realpath(entryPath);
  const project = await realpath(projectPath);
  const caseBot = { uid: `${bot.uid}-${basename(root)}`, name: bot.name };
  const transport = new FakeTransport({ bot: caseBot });
  transport.cardSupported = true;
  const holdStarted = Promise.withResolvers<void>();
  let appendedText: string | null = null;
  let harness: CoreHarness | undefined;
  try {
    harness = await createCoreHarness({
      root,
      bot: caseBot,
      transport,
      ask: { timeoutMs: 40 },
      prompt: async ({ text, promptContext, waitForAppend, waitForAbort, onStop }) => {
        if (text === "ask before timeout") {
          const ask = promptContext?.tools?.find(({ name }) => name === "ask");
          if (!ask) throw new Error("ask host tool missing");
          try {
            await ask.execute({
              questions: [{
                id: "late-shared-answer",
                question: "Should the work continue?",
                options: [{ label: "yes" }, { label: "no" }],
              }],
            }, new AbortController().signal);
          } catch {
            await waitForAbort();
          }
          return;
        }
        if (text === "/hold current turn") {
          const stopped = Promise.withResolvers<void>();
          onStop(stopped.resolve);
          holdStarted.resolve();
          const appended = waitForAppend().then((message) => {
            appendedText = message.text;
          });
          await Promise.race([appended, stopped.promise]);
        }
      },
    });
    const bindJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "late-ask-bind", `/new ${entry}`),
    );
    await expectIngressReceipt(root, bindJob.id);
    await withStore(root, (store) =>
      store.saveThreadWorkspace(
        workspaceRecord(thread, entry, [{ name: "api", path: project }], "ready", caseBot.uid),
      ),
    );
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "late-ask-start", "ask before timeout"),
    );
    await waitForStatus(
      harness.service,
      thread,
      (status) => status.state === "interrupted" && status.pendingQuestionIds.length === 1,
    );
    const card = transport.sentCards[0];
    if (!card) throw new Error("timed-out ask card was not sent");

    // Plain text would answer the still-pending ask; use an unregistered slash input to start a separate turn.
    const holdJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, thread, "late-ask-hold", "/hold current turn"),
    );
    if (holdJob.consumerKind !== "task" || holdJob.consumerId === null) {
      throw new Error("held shared turn was not durably accepted as a task");
    }
    await holdStarted.promise;
    await waitForTaskState(root, holdJob.consumerId, "running");
    const activeTaskId = holdJob.consumerId;
    await rm(projectPath, { recursive: true, force: true });

    const unavailable = waitForNotice(harness, thread, unavailableNotice);
    const lateAnswer: BotCardAction = {
      messageId: card.messageId,
      channelType: thread.channelType,
      channelId: thread.channelId,
      actionId: "ask_opt_1",
      operatorUid: "owner-1",
      inputs: {},
    };
    transport.pushBotEvent(cardActionEvent(802, lateAnswer));
    await unavailable;
    await transport.waitForAck(802);
    const wakeTask = await waitForLatestTask(
      root,
      caseBot.uid,
      thread,
      (task) => task.id !== activeTaskId && task.messageId.startsWith("answer:"),
    );
    const cancelledWake = await waitForTaskState(root, wakeTask.id, "cancelled");

    expect(appendedText).toBeNull();
    expect(harness.workers.prompts).toHaveLength(2);
    expect(cancelledWake.text).toContain("Should the work continue?");
    expect(cancelledWake.text).toContain("答案（由 owner-1 回复）：1. yes");
    expect(await withStore(root, (store) => store.getTask(activeTaskId)?.state)).toBe("running");
    expect(await withStore(root, (store) => store.getLatestTask(caseBot.uid, thread)?.id)).toBe(wakeTask.id);
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("thread outcome relay cannot steer a shared owner after a project disappears", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-relay-"));
  const entryPath = join(root, "shared-entry");
  const projectAPath = join(entryPath, "api");
  const projectBPath = join(entryPath, "web");
  const sourceCwdPath = join(root, "source-workspace");
  await Promise.all([
    mkdir(projectAPath, { recursive: true }),
    mkdir(projectBPath, { recursive: true }),
    mkdir(sourceCwdPath, { recursive: true }),
  ]);
  const entry = await realpath(entryPath);
  const projectA = await realpath(projectAPath);
  const projectB = await realpath(projectBPath);
  const sourceCwd = await realpath(sourceCwdPath);
  const sourceTarget: ThreadTarget = { channelId: "group-tools____thread-relay-source", channelType: 5 };
  const activeTask = taskRow(thread, "relay-shared-active", projectA);
  activeTask.text = "hold shared turn";
  const sourceTask: TaskRecord = {
    ...taskRow(sourceTarget, "relay-notification-source", sourceCwd),
    text: "complete relay source",
    messageId: "dispatch:relay-notification-source",
    relaySuppressed: false,
  };
  const activeBinding: BindingRecord = {
    botUid: bot.uid,
    channelId: thread.channelId,
    channelType: thread.channelType,
    sessionId: activeTask.sessionId,
    cwd: projectA,
    sessionFile: null,
    cwdPinned: true,
  };
  const sourceBinding: BindingRecord = {
    botUid: bot.uid,
    channelId: sourceTarget.channelId,
    channelType: sourceTarget.channelType,
    sessionId: sourceTask.sessionId,
    cwd: sourceCwd,
    sessionFile: null,
    cwdPinned: true,
  };
  const activeStarted = Promise.withResolvers<void>();
  const sourceStarted = Promise.withResolvers<void>();
  const finishSource = Promise.withResolvers<void>();
  let appendedText: string | null = null;
  let harness: CoreHarness | undefined;
  try {
    await withStore(root, (store) => {
      store.ensureBinding(activeBinding);
      store.ensureBinding(sourceBinding);
      store.saveThreadWorkspace(workspaceRecord(thread, entry, [
        { name: "api", path: projectA },
        { name: "web", path: projectB },
      ]));
      store.ensureThreadListen({
        botUid: bot.uid,
        channelId: sourceTarget.channelId,
        creatorChannelType: thread.channelType,
        creatorChannelId: thread.channelId,
        threadName: "source",
        listen: "task",
      });
      seedQueuedTask(store, activeTask);
      seedQueuedTask(store, sourceTask);
    });
    harness = await createCoreHarness({
      root,
      bot,
      prompt: async ({ text, waitForAppend, onStop }) => {
        if (text === "hold shared turn") {
          const stopped = Promise.withResolvers<void>();
          onStop(stopped.resolve);
          activeStarted.resolve();
          const appended = waitForAppend().then((message) => {
            appendedText = message.text;
          });
          await Promise.race([appended, stopped.promise]);
        } else if (text === "complete relay source") {
          sourceStarted.resolve();
          await finishSource.promise;
        }
      },
    });
    await Promise.all([activeStarted.promise, sourceStarted.promise]);
    await rm(projectBPath, { recursive: true, force: true });

    const unavailable = harness.transport.waitForTextRequest((request) =>
      request.operation === "send"
      && request.text === unavailableNotice
      && request.target.channelType === thread.channelType
      && request.target.channelId === thread.channelId,
    );
    finishSource.resolve();
    await waitForTaskState(root, sourceTask.id, "completed");
    await unavailable;

    expect(appendedText).toBeNull();
    expect(harness.workers.prompts).toHaveLength(2);
    expect(await withStore(root, (store) => store.getTask(activeTask.id)?.state)).toBe("running");
    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, thread)?.id)).toBe(activeTask.id);
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});
test("an unmapped preparing workspace intent refuses unknown thread work before task creation", async () => {
  const harness = await createCoreHarness({ bot });
  const unknown: ThreadTarget = { channelId: "group-tools____thread-unknown-preparing", channelType: 5 };
  try {
    const intent: ThreadWorkspaceIntentInput = {
      id: "intent-unknown-thread",
      botUid: bot.uid,
      groupId: "group-tools",
      status: "preparing",
      channelIds: [],
      error: null,
    };
    await withStore(harness.root, (store) => store.saveThreadWorkspaceIntent(intent));
    const notice = waitForNotice(harness, unknown, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, unknown, "unknown-preparing-inbound", "must stay blocked"),
    );
    await notice;

    expect(await withStore(harness.root, (store) => store.getLatestTask(bot.uid, unknown))).toBeNull();
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.some(({ target, text }) =>
      target.channelType === unknown.channelType
      && target.channelId === unknown.channelId
      && text === unavailableNotice,
    )).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a same-group unmapped intent does not block a legacy thread with an existing binding", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-legacy-intent-"));
  const cwdPath = join(root, "legacy-cwd");
  await mkdir(cwdPath, { recursive: true });
  const cwd = await realpath(cwdPath);
  let harness: CoreHarness | undefined;
  try {
    harness = await createCoreHarness({ root, bot });
    const legacy: ThreadTarget = { channelId: "group-tools____thread-existing-legacy", channelType: 5 };
    const bindJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, legacy, "legacy-intent-bind", `/new ${cwd}`),
    );
    await expectIngressReceipt(root, bindJob.id);
    await withStore(root, (store) => store.saveThreadWorkspaceIntent({
      id: "intent-unmapped-with-legacy",
      botUid: bot.uid,
      groupId: "group-tools",
      status: "preparing",
      channelIds: [],
    }));

    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, legacy, "legacy-intent-task", "keep legacy work available"),
    );
    await waitForStatus(harness.service, legacy, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["keep legacy work available"]);
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, legacy))).toBeNull();
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("an explicit /new can recover a mapped thread whose ownership row was never persisted", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-unpersisted-new-"));
  const cwdPath = join(root, "outside-workspace");
  await mkdir(cwdPath, { recursive: true });
  const cwd = await realpath(cwdPath);
  let harness: CoreHarness | undefined;
  const target: ThreadTarget = { channelId: "group-tools____thread-unpersisted-owner", channelType: 5 };
  try {
    harness = await createCoreHarness({ root, bot });
    await withStore(root, (store) => {
      store.saveThreadWorkspaceIntent({
        id: "intent-unpersisted-owner",
        botUid: bot.uid,
        groupId: "group-tools",
        status: "preparing",
        channelIds: [],
      });
      store.addThreadWorkspaceIntentChannel(bot.uid, "intent-unpersisted-owner", target.channelId);
    });

    const notice = waitForNotice(harness, target, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "unpersisted-owner-blocked", "wait for ownership"),
    );
    await notice;
    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, target))).toBeNull();

    const newJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "unpersisted-owner-new", `/new ${cwd}`),
    );
    await expectIngressReceipt(root, newJob.id);
    expect(await withStore(root, (store) => store.getBinding(bot.uid, target))).toMatchObject({ cwd });

    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "unpersisted-owner-task", "continue outside unpersisted resources"),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["continue outside unpersisted resources"]);
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, target))).toBeNull();
    expect(await withStore(root, (store) => store.getThreadWorkspaceIntent(bot.uid, "intent-unpersisted-owner")))
      .toMatchObject({ status: "preparing", channelIds: [target.channelId] });
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});

test("a failed mapped workspace can be exited with explicit /new outside its owned paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-core-workspace-failed-detach-"));
  const entryPath = join(root, "failed-entry");
  const projectPath = join(entryPath, "api");
  const outsidePath = join(root, "outside-workspace");
  await Promise.all([
    mkdir(projectPath, { recursive: true }),
    mkdir(outsidePath, { recursive: true }),
  ]);
  const entry = await realpath(entryPath);
  const project = await realpath(projectPath);
  const outside = await realpath(outsidePath);
  let harness: CoreHarness | undefined;
  const target: ThreadTarget = { channelId: "group-tools____thread-failed-detach", channelType: 5 };
  try {
    harness = await createCoreHarness({ root, bot });
    const bindJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "failed-detach-bind", `/new ${entry}`),
    );
    await expectIngressReceipt(root, bindJob.id);
    const failed = workspaceRecord(target, entry, [
      { name: "api", path: project, created: false },
    ], "failed");
    await withStore(root, (store) => {
      store.saveThreadWorkspaceIntent({
        id: "intent-failed-detach",
        botUid: bot.uid,
        groupId: "group-tools",
        status: "preparing",
        channelIds: [],
      });
      store.addThreadWorkspaceIntentChannel(bot.uid, "intent-failed-detach", target.channelId);
      store.saveThreadWorkspace(failed);
      store.failThreadWorkspaceIntent(bot.uid, "intent-failed-detach", failed.error);
    });

    const notice = waitForNotice(harness, target, unavailableNotice);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "failed-detach-refused", "must be refused first"),
    );
    await notice;
    expect(await withStore(root, (store) => store.getLatestTask(bot.uid, target))).toBeNull();
    expect(harness.transport.sent.some(({ text }) => text === unavailableNotice)).toBe(true);

    const newJob = await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "failed-detach-new", `/new ${outside}`),
    );
    await expectIngressReceipt(root, newJob.id);
    await submitAndWaitForIngressHandoff(
      harness,
      ownerMessage(harness, target, "failed-detach-task", "work outside failed ownership"),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["work outside failed ownership"]);
    expect(await withStore(root, (store) => store.getBinding(bot.uid, target))).toMatchObject({ cwd: outside });
    expect(await withStore(root, (store) => store.getThreadWorkspace(bot.uid, target))).toEqual(failed);
    expect(await withStore(root, (store) => store.getThreadWorkspaceIntent(bot.uid, "intent-failed-detach")))
      .toMatchObject({ status: "failed", channelIds: [target.channelId] });
  } finally {
    if (harness) await closeCoreHarness(harness);
    else await rm(root, { recursive: true, force: true });
  }
});
