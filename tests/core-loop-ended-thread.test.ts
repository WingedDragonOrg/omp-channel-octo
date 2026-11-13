import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import type { ChatTarget, InboundMessage, ThreadSummary } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { LoopInput, LoopTask, LoopWorkspace } from "../src/loop/types.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  FakeTransport,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-allowed";
const PARENT: ChatTarget = { channelType: 2, channelId: GROUP_ID };
const WORKSPACE: LoopWorkspace = {
  workspaceId: "loop-ended-workspace",
  name: "Ended route tasks",
  slug: "ended-route-tasks",
};

function task(
  taskId: string,
  title: string,
  overrides: Partial<Omit<LoopTask, "taskId" | "title">> = {},
): LoopTask {
  return {
    taskId,
    title,
    description: "fixture task description",
    status: "open",
    priority: "normal",
    assigneeType: "member",
    assigneeId: "fleet-owner-1",
    assigneeOctoUid: "owner-1",
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-03T00:00:00Z",
    updatedAt: "2026-11-04T00:00:00Z",
    ...overrides,
  };
}

function loopFixture(tasks: LoopTask[] = []): FakeLoopReadPort {
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(WORKSPACE.workspaceId, WORKSPACE);
  loop.tasks.set(WORKSPACE.workspaceId, tasks);
  return loop;
}
function parentMessage(botUid: string, messageId: string, text: string): InboundMessage {
  return inbound({
    channelType: 2,
    channelId: GROUP_ID,
    messageId,
    text,
    senderId: "owner-1",
    mentionedUserIds: [botUid],
  });
}

function bindMessage(botUid: string, messageId: string): InboundMessage {
  return parentMessage(botUid, messageId, `/bind loop ${WORKSPACE.workspaceId}`);
}

function childMessage(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
): InboundMessage {
  return inbound({
    ...target,
    messageId,
    text,
    senderId: "owner-1",
    mentionedUserIds: [harness.bot.uid],
  });
}

function threadTarget(thread: ThreadSummary): ChatTarget {
  return { channelType: 5, channelId: thread.channelId };
}

function sidecarRoot(stateDir: string, botUid: string, target: ChatTarget): string {
  const identity = `${botUid}\u0000${target.channelType}\u0000${target.channelId}`;
  return join(
    stateDir,
    "context-sidecars",
    createHash("sha256").update(identity, "utf8").digest("hex"),
  );
}

function promptCount(harness: CoreHarness, target: ChatTarget): number {
  const root = sidecarRoot(harness.stateDir, harness.bot.uid, target);
  return harness.workers.prompts.filter((prompt) => prompt.sidecarRoot === root).length;
}

function promptForTask(harness: CoreHarness, target: ChatTarget, text: string) {
  const root = sidecarRoot(harness.stateDir, harness.bot.uid, target);
  const prompt = harness.workers.prompts.find(
    (candidate) => candidate.sidecarRoot === root && candidate.text.includes(text),
  );
  if (!prompt) throw new Error(`no prompt for ${target.channelId} containing ${text}`);
  return prompt;
}

async function waitUntil(
  predicate: () => boolean,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (predicate()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForSignal<T>(signal: Promise<T>, description: string): Promise<T> {
  let value!: T;
  let resolved = false;
  void signal.then((result) => {
    value = result;
    resolved = true;
  });
  await waitUntil(() => resolved, description);
  return value;
}

async function bindWorkspace(harness: CoreHarness, messageId: string): Promise<void> {
  await harness.service.submitInbound(bindMessage(harness.bot.uid, messageId));
  await waitUntil(() => promptCount(harness, PARENT) >= 1, "Loop baseline in the parent");
  await waitForStatus(
    harness.service,
    PARENT,
    (status) => status.state === "completed" && status.queued === 0,
  );
}

async function createLinkedThread(
  harness: CoreHarness,
  taskId: string,
  name: string,
  options: { readonly cwd?: string; readonly repo?: string } = {},
): Promise<{ thread: ThreadSummary; target: ChatTarget }> {
  const thread = await harness.transport.createThread(GROUP_ID, name);
  const target = threadTarget(thread);
  const store = await openChannelStore(harness.stateDir);
  try {
    store.bindLoopTask({
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId,
    }, thread.channelId);
    if (options.cwd !== undefined) {
      await mkdir(options.cwd, { recursive: true });
      store.ensureBinding({
        botUid: harness.bot.uid,
        ...target,
        cwd: options.cwd,
        sessionId: `session-${thread.shortId}`,
        sessionFile: null,
        cwdPinned: true,
      });
    }
    if (options.repo !== undefined && options.cwd !== undefined) {
      await mkdir(options.repo, { recursive: true });
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        channelType: 5,
        channelId: thread.channelId,
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{
          name: "fixture-project",
          repo: options.repo,
          path: options.cwd,
          branch: `branch-${thread.shortId}`,
          created: true,
        }],
        error: null,
      });
    }
  } finally {
    store.close();
  }
  return { thread, target };
}

interface PersistedLoopInput {
  id: string;
  taskId: string | null;
  kind: LoopInput["kind"];
  state: LoopInput["state"];
  target: ChatTarget | null;
}

/**
 * `listLoopInputs` intentionally exposes only pending/intent rows to the consumer. Assertions on
 * accepted delivery must inspect the durable table directly, matching how the consumer reads it.
 */
function persistedLoopInputs(harness: CoreHarness): PersistedLoopInput[] {
  const db = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    const rows = db.query(
      `SELECT id, task_id, kind, state, target_channel_type, target_channel_id
       FROM loop_inputs WHERE bot_uid = ? ORDER BY sequence ASC`,
    ).all(harness.bot.uid) as Array<{
      id: string;
      task_id: string | null;
      kind: LoopInput["kind"];
      state: LoopInput["state"];
      target_channel_type: number | null;
      target_channel_id: string | null;
    }>;
    return rows.map((row) => ({
      id: row.id,
      taskId: row.task_id,
      kind: row.kind,
      state: row.state,
      target: row.target_channel_type === null || row.target_channel_id === null
        ? null
        : {
          channelType: row.target_channel_type as ChatTarget["channelType"],
          channelId: row.target_channel_id,
        },
    }));
  } finally {
    db.close();
  }
}

async function latestUpdatedInput(
  harness: CoreHarness,
  taskId: string,
): Promise<PersistedLoopInput | undefined> {
  return persistedLoopInputs(harness)
    .filter((input) => input.taskId === taskId && input.kind === "updated")
    .at(-1);
}

async function waitForDeliveredInput(
  harness: CoreHarness,
  taskId: string,
  target: ChatTarget,
): Promise<PersistedLoopInput> {
  let found: PersistedLoopInput | undefined;
  await waitUntil(() => {
    found = persistedLoopInputs(harness)
      .filter((input) => input.taskId === taskId && input.kind === "updated")
      .at(-1);
    return found?.state === "delivered"
      && found.target?.channelType === target.channelType
      && found.target.channelId === target.channelId;
  }, "delivered Loop input");
  return found!;
}

async function expectDeliveredToParent(
  harness: CoreHarness,
  taskId: string,
  child: ChatTarget,
): Promise<PersistedLoopInput> {
  const input = await latestUpdatedInput(harness, taskId);
  expect(input).toBeDefined();
  expect(input?.state).toBe("delivered");
  expect(input?.target).toEqual(PARENT);
  const store = await openChannelStore(harness.stateDir);
  try {
    expect(store.getTaskByMessageId(harness.bot.uid, PARENT, `loop:${input!.id}`)).not.toBeNull();
    expect(store.getTaskByMessageId(harness.bot.uid, child, `loop:${input!.id}`)).toBeNull();
    expect(store.getLoopTaskRoute({
      botUid: harness.bot.uid,
      groupId: GROUP_ID,
      workspaceId: WORKSPACE.workspaceId,
      taskId,
    })?.threadChannelId).toBe(child.channelId);
    return input!;
  } finally {
    store.close();
  }
}
async function persistedTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getTaskByMessageId(harness.bot.uid, target, messageId);
  } finally {
    store.close();
  }
}

async function waitForTaskState(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  state: string,
) {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    const task = await persistedTask(harness, target, messageId);
    if (task?.state === state) return task;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`timed out waiting for task ${messageId} to reach ${state}`);
}

async function taskCountByMessageId(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
): Promise<number> {
  const db = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    const row = db.query(
      `SELECT COUNT(*) AS count FROM tasks
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?`,
    ).get(harness.bot.uid, target.channelType, target.channelId, messageId) as
      { count: number } | null;
    return row?.count ?? 0;
  } finally {
    db.close();
  }
}

async function waitForArchiveFailureNotice(
  harness: CoreHarness,
  target: ChatTarget,
): Promise<void> {
  await waitUntil(() => harness.transport.sent.some((message) =>
    message.target.channelType === target.channelType
    && message.target.channelId === target.channelId
    && message.text.startsWith("归档失败：")), "failed archive result");
}

function expectEndedEventPrompt(
  harness: CoreHarness,
  thread: ThreadSummary,
  taskTitle: string,
) {
  const prompt = promptForTask(harness, PARENT, taskTitle);
  expect(prompt.text).toContain(GROUP_ID);
  expect(prompt.text).toContain(thread.channelId);
  expect(prompt.text).not.toContain("在当前关联子区处理任务变化");
  expect(prompt.text).not.toContain("dispatch work to its associated child");
  return prompt;
}

test("archived Loop associations stay on the parent after restart and human reactivation", async () => {
  const taskId = "loop-archived-restart";
  const loop = loopFixture([task(taskId, "Before archive")]);
  const first = await createCoreHarness({ loop });
  let second: CoreHarness | undefined;
  let firstClosed = false;
  try {
    await bindWorkspace(first, "archived-route-bind");
    const { thread, target } = await createLinkedThread(first, taskId, "Archived task");
    await first.transport.archiveThread(GROUP_ID, thread.shortId);

    const changedOwnerTitle = "Archived task with a new assignee";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, changedOwnerTitle, {
      assigneeId: "replacement-owner",
      assigneeOctoUid: "replacement-owner",
      updatedAt: "2026-11-05T00:00:00Z",
    })]);
    await first.service.submitInbound(bindMessage(first.bot.uid, "archived-route-owner-change"));
    await waitUntil(() => promptCount(first, PARENT) >= 2, "archived task update in parent");
    await waitForStatus(
      first.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(first, target)).toBe(0);
    const changedInput = await expectDeliveredToParent(first, taskId, target);
    expectEndedEventPrompt(first, thread, changedOwnerTitle);

    const root = first.root;
    const bot = first.bot;
    const scansBeforeRestart = loop.taskListCalls.length;
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;

    // The archived thread is now active again, as if a human message reopened it.
    const reopenedTransport = new FakeTransport({ bot });
    reopenedTransport.setThreads(GROUP_ID, [{ ...thread, status: 1 }]);
    second = await createCoreHarness({ root, bot, transport: reopenedTransport, loop });
    await waitUntil(
      () => loop.taskListCalls.length > scansBeforeRestart,
      "restarted Loop snapshot scan",
    );
    await second.service.submitInbound(bindMessage(bot.uid, "archived-route-restart-rescan"));
    expect(second.workers.prompts).toEqual([]);

    await second.service.submitInbound(childMessage(
      second,
      target,
      "archived-route-human-reactivation",
      "Manual follow-up after reopening the child",
    ));
    await waitUntil(() => promptCount(second!, target) >= 1, "manual child follow-up");
    await waitForStatus(
      second.service,
      target,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptForTask(second, target, "Manual follow-up after reopening").text)
      .toContain("Manual follow-up after reopening");

    const afterRestartTitle = "Reopened child still reports to parent";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, afterRestartTitle, {
      assigneeId: "another-owner",
      assigneeOctoUid: "another-owner",
      updatedAt: "2026-11-06T00:00:00Z",
    })]);
    await second.service.submitInbound(bindMessage(bot.uid, "archived-route-after-restart"));
    await waitUntil(() => promptCount(second!, PARENT) >= 1, "post-restart Loop update in parent");
    await waitForStatus(
      second.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(second, target)).toBe(1);
    await expectDeliveredToParent(second, taskId, target);
    expectEndedEventPrompt(second, thread, afterRestartTitle);
    expect(await taskCountByMessageId(second, PARENT, `loop:${changedInput.id}`)).toBe(1);
    expect(await taskCountByMessageId(second, target, `loop:${changedInput.id}`)).toBe(0);
  } finally {
    if (second !== undefined) {
      await closeCoreHarness(second);
    } else if (firstClosed) {
      await rm(first.root, { recursive: true, force: true });
    } else {
      await closeCoreHarness(first);
    }
  }
});

test("successful thread merge routes later Loop changes to the parent even when archive fails", async () => {
  const taskId = "loop-merge-ended";
  const loop = loopFixture([task(taskId, "Before merge")]);
  const harness = await createCoreHarness({ loop });
  try {
    await bindWorkspace(harness, "merge-ended-bind");
    const cwd = join(harness.root, "merge-ended-worktree");
    const { thread, target } = await createLinkedThread(
      harness,
      taskId,
      "Merged task",
      { cwd },
    );
    harness.transport.failArchiveTimes([new Error("fixture archive failure")]);

    await harness.service.submitInbound(childMessage(
      harness,
      target,
      "merge-ended-command",
      "/thread merge",
    ));
    await harness.transport.waitForManagementCalls((call) => call.action === "thread-archive");
    await waitForArchiveFailureNotice(harness, target);
    expect(harness.worktrees?.merges).toHaveLength(1);
    expect((await harness.transport.getThread(GROUP_ID, thread.shortId)).status).toBe(1);

    const title = "Change after successful merge";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, title, { updatedAt: "2026-11-05T00:00:00Z" })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "merge-ended-loop-change"));
    await waitUntil(() => harness.workers.prompts.some(
      (prompt) => prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(title),
    ), "merged task change in parent");
    await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(harness, target)).toBe(0);
    await expectDeliveredToParent(harness, taskId, target);
    expectEndedEventPrompt(harness, thread, title);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("successful thread removal routes later Loop changes to the parent even when archive fails", async () => {
  const taskId = "loop-remove-ended";
  const loop = loopFixture([task(taskId, "Before removal")]);
  const harness = await createCoreHarness({ loop });
  try {
    await bindWorkspace(harness, "remove-ended-bind");
    const cwd = join(harness.root, "remove-ended-worktree");
    const repo = join(harness.root, "remove-ended-repo");
    const { thread, target } = await createLinkedThread(
      harness,
      taskId,
      "Removed task",
      { cwd, repo },
    );
    harness.transport.failArchiveTimes([new Error("fixture archive failure")]);

    await harness.service.submitInbound(childMessage(
      harness,
      target,
      "remove-ended-command",
      "/thread remove",
    ));
    await harness.transport.waitForManagementCalls((call) => call.action === "thread-archive");
    await waitForArchiveFailureNotice(harness, target);
    expect(harness.worktrees?.removals).toHaveLength(1);
    expect((await harness.transport.getThread(GROUP_ID, thread.shortId)).status).toBe(1);

    const title = "Change after successful removal";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, title, { updatedAt: "2026-11-05T00:00:00Z" })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "remove-ended-loop-change"));
    await waitUntil(() => harness.workers.prompts.some(
      (prompt) => prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(title),
    ), "removed task change in parent");
    await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(harness, target)).toBe(0);
    await expectDeliveredToParent(harness, taskId, target);
    expectEndedEventPrompt(harness, thread, title);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a Loop change during a successful thread merge does not start the child", async () => {
  const taskId = "loop-merge-in-flight";
  const loop = loopFixture([task(taskId, "Before in-flight merge")]);
  const harness = await createCoreHarness({ loop });
  const mergeStarted = Promise.withResolvers<void>();
  const releaseMerge = Promise.withResolvers<void>();
  try {
    await bindWorkspace(harness, "merge-in-flight-bind");
    const cwd = join(harness.root, "merge-in-flight-worktree");
    const { thread, target } = await createLinkedThread(
      harness,
      taskId,
      "In-flight merge task",
      { cwd },
    );
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("fixture worktrees unavailable");
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request) => {
      mergeStarted.resolve();
      await releaseMerge.promise;
      return mergeWorktree(request);
    };

    const mergeCommand = harness.service.submitInbound(childMessage(
      harness,
      target,
      "merge-in-flight-command",
      "/thread merge",
    ));
    void mergeCommand.catch(() => {});
    await waitForSignal(mergeStarted.promise, "host merge execution");

    const title = "Loop update while merge is in flight";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, title, { updatedAt: "2026-11-05T00:00:00Z" })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "merge-in-flight-loop-change"));
    await waitUntil(() => harness.workers.prompts.some(
      (prompt) => prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(title),
    ), "in-flight merge task change in parent");
    await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(harness, target)).toBe(0);
    await expectDeliveredToParent(harness, taskId, target);
    expectEndedEventPrompt(harness, thread, title);

    releaseMerge.resolve();
    await mergeCommand;
    await harness.transport.waitForManagementCalls((call) => call.action === "thread-archive");
  } finally {
    releaseMerge.resolve();
    await closeCoreHarness(harness);
  }
});


test("an ended child Loop event stays queued behind a pending parent ask", async () => {
  const taskId = "loop-ended-pending-ask";
  const loop = loopFixture([task(taskId, "Baseline ask task")]);
  const askStarted = Promise.withResolvers<void>();
  let promptCountForAsk = 0;
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      promptCountForAsk += 1;
      if (promptCountForAsk !== 1) return;
      const ask = context.promptContext?.tools?.find((tool) => tool.name === "ask");
      if (!ask) throw new Error("ask tool unavailable in fixture prompt");
      askStarted.resolve();
      await ask.execute(
        { questions: [{ id: "ended-loop-pending-question", question: "Which option?" }] },
        new AbortController().signal,
      );
    },
  });
  try {
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "ended-ask-bind"));
    await waitUntil(() => promptCount(harness, PARENT) >= 1, "parent Loop ask prompt");
    await waitForSignal(askStarted.promise, "parent ask tool start");
    const waiting = await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.pendingQuestionIds.length === 1,
    );

    const { thread, target } = await createLinkedThread(harness, taskId, "Pending ask task");
    await harness.transport.archiveThread(GROUP_ID, thread.shortId);
    const title = "Ended child update while parent asks";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, title, { updatedAt: "2026-11-05T00:00:00Z" })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "ended-ask-loop-change"));

    const queued = await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.queued === 1 && status.pendingQuestionIds.length === 1,
    );
    expect(queued.pendingQuestionIds).toEqual(waiting.pendingQuestionIds);
    expect(promptCount(harness, PARENT)).toBe(1);
    expect(promptCount(harness, target)).toBe(0);
    await waitForDeliveredInput(harness, taskId, PARENT);
    await expectDeliveredToParent(harness, taskId, target);
    await harness.service.submitInbound(parentMessage(
      harness.bot.uid,
      "ended-ask-human-answer",
      "The owner's explicit answer",
    ));
    await waitUntil(() => promptCount(harness, PARENT) >= 2, "queued ended-route input after answer");
    await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.state === "completed"
        && status.queued === 0
        && status.pendingQuestionIds.length === 0,
    );
    expect(promptForTask(harness, PARENT, title).text).toContain(title);
    await expectDeliveredToParent(harness, taskId, target);
    expect(promptCount(harness, target)).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("archive during child-target validation reroutes the not-yet-accepted Loop event", async () => {
  const taskId = "loop-ended-validation-race";
  const loop = loopFixture([task(taskId, "Before validation race")]);
  const harness = await createCoreHarness({ loop });
  const validationEntered = Promise.withResolvers<void>();
  const releaseValidation = Promise.withResolvers<void>();
  try {
    await bindWorkspace(harness, "validation-race-bind");
    const { thread, target } = await createLinkedThread(harness, taskId, "Validation race task");
    harness.transport.beforeGetThread = async () => {
      validationEntered.resolve();
      await releaseValidation.promise;
    };

    const title = "Loop update racing with archive";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, title, { updatedAt: "2026-11-05T00:00:00Z" })]);
    const bind = harness.service.submitInbound(bindMessage(harness.bot.uid, "validation-race-loop-change"));
    await waitForSignal(validationEntered.promise, "child-target validation");
    await harness.transport.archiveThread(GROUP_ID, thread.shortId);
    harness.transport.beforeGetThread = undefined;
    releaseValidation.resolve();
    await bind;

    await waitUntil(() => promptCount(harness, PARENT) >= 2, "raced Loop event in parent");
    await waitForStatus(
      harness.service,
      PARENT,
      (status) => status.state === "completed" && status.queued === 0,
    );
    expect(promptCount(harness, target)).toBe(0);
    await expectDeliveredToParent(harness, taskId, target);
    expectEndedEventPrompt(harness, thread, title);
  } finally {
    releaseValidation.resolve();
    await closeCoreHarness(harness);
  }
});

test("an undispatched child Loop task is transferred to the parent when the child is archived", async () => {
  const taskId = "loop-transfer-queued";
  const loop = loopFixture([task(taskId, "Before queued transfer")]);
  const blocked = Promise.withResolvers<void>();
  const childPromptStarted = Promise.withResolvers<void>();
  let childCwd = "";
  const harness = await createCoreHarness({
    loop,
    prompt: async (context) => {
      if (context.cwd === childCwd) {
        childPromptStarted.resolve();
        await blocked.promise;
      }
    },
  });
  try {
    await bindWorkspace(harness, "transfer-queued-bind");
    childCwd = join(harness.root, "transfer-queued-child");
    const { thread, target } = await createLinkedThread(
      harness,
      taskId,
      "Transfer queued task",
      { cwd: childCwd },
    );
    await harness.service.submitInbound(childMessage(
      harness,
      target,
      "transfer-queued-blocker",
      "Child work that blocks the queue",
    ));
    await waitForSignal(childPromptStarted.promise, "blocking child prompt");

    const queuedTitle = "Queued child update before archive";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, queuedTitle, {
      updatedAt: "2026-11-05T00:00:00Z",
    })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "transfer-queued-loop-change"));
    await waitForStatus(
      harness.service,
      target,
      (status) => status.queued === 1,
    );
    const queuedInput = await waitForDeliveredInput(harness, taskId, target);
    expect(queuedInput.state).toBe("delivered");
    expect(queuedInput.target).toEqual(target);
    const childTask = await persistedTask(harness, target, `loop:${queuedInput!.id}`);
    expect(childTask?.state).toBe("queued");

    await harness.transport.archiveThread(GROUP_ID, thread.shortId);
    const laterTitle = "Trigger parent handoff";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, laterTitle, {
      updatedAt: "2026-11-06T00:00:00Z",
    })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "transfer-queued-after-archive"));

    await waitUntil(
      () => harness.workers.prompts.some((prompt) =>
        prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(queuedTitle)),
      "transferred queued input in the parent",
    );
    await waitUntil(
      () => harness.workers.prompts.some((prompt) =>
        prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(laterTitle)),
      "post-archive input in the parent",
    );
    blocked.resolve();
    const blocker = await waitForTaskState(
      harness,
      target,
      "transfer-queued-blocker",
      "completed",
    );
    expect(blocker.text).toContain("Child work that blocks the queue");

    const childPrompts = harness.workers.prompts.filter((prompt) =>
      prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, target));
    expect(childPrompts.map((prompt) => prompt.text).join("\n"))
      .not.toContain(queuedTitle);
    expect(childPrompts.map((prompt) => prompt.text).join("\n"))
      .not.toContain(laterTitle);
    const cancelled = await persistedTask(harness, target, `loop:${queuedInput!.id}`);
    expect(cancelled?.state).toBe("cancelled");
    await expectDeliveredToParent(harness, taskId, target);
  } finally {
    blocked.resolve();
    await closeCoreHarness(harness);
  }
});

test("restart finishes the transfer of an undispatched ended-child Loop task", async () => {
  const taskId = "loop-transfer-restart";
  const loop = loopFixture([task(taskId, "Before restart transfer")]);
  const childPromptStarted = Promise.withResolvers<void>();
  let childCwd = "";
  const first = await createCoreHarness({
    loop,
    prompt: async (context) => {
      if (context.cwd !== childCwd) return;
      childPromptStarted.resolve();
      await new Promise<void>((resolve) => context.onStop(resolve));
    },
  });
  let second: CoreHarness | undefined;
  let firstClosed = false;
  try {
    await bindWorkspace(first, "transfer-restart-bind");
    childCwd = join(first.root, "transfer-restart-child");
    const { thread, target } = await createLinkedThread(
      first,
      taskId,
      "Restart transfer task",
      { cwd: childCwd },
    );
    await first.service.submitInbound(childMessage(
      first,
      target,
      "transfer-restart-blocker",
      "Child work that survives until restart",
    ));
    await waitForSignal(childPromptStarted.promise, "restart blocking child prompt");

    const queuedTitle = "Queued child update before restart";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, queuedTitle, {
      updatedAt: "2026-11-05T00:00:00Z",
    })]);
    await first.service.submitInbound(bindMessage(first.bot.uid, "transfer-restart-loop-change"));
    await waitForStatus(
      first.service,
      target,
      (status) => status.queued === 1,
    );
    const queuedInput = await waitForDeliveredInput(first, taskId, target);
    expect(queuedInput.state).toBe("delivered");
    expect(queuedInput.target).toEqual(target);
    expect((await persistedTask(first, target, `loop:${queuedInput!.id}`))?.state).toBe("queued");

    // The archive succeeded before the crash window closed; only the handoff is still pending.
    const store = await openChannelStore(first.stateDir);
    try {
      store.markLoopThreadRoutesEnding(
        first.bot.uid,
        GROUP_ID,
        thread.channelId,
        "archived",
        "ended",
      );
    } finally {
      store.close();
    }

    const root = first.root;
    const bot = first.bot;
    await closeCoreHarness(first, { remove: false });
    firstClosed = true;

    // A human reopened the child, but the ended route fact and queued handoff survive the restart.
    const reopened = new FakeTransport({ bot });
    reopened.setThreads(GROUP_ID, [{ ...thread, status: 1 }]);
    second = await createCoreHarness({ root, bot, transport: reopened, loop });
    await waitUntil(
      () => second!.workers.prompts.some((prompt) =>
        prompt.sidecarRoot === sidecarRoot(second!.stateDir, bot.uid, PARENT)
        && prompt.text.includes(queuedTitle)),
      "startup transfer of the queued input",
    );
    expect(second.workers.prompts.filter((prompt) =>
      prompt.sidecarRoot === sidecarRoot(second!.stateDir, bot.uid, target)
      && prompt.text.includes(queuedTitle)).length).toBe(0);
    expect((await persistedTask(second, target, `loop:${queuedInput!.id}`))?.state).toBe("cancelled");

    const laterTitle = "Post-restart parent update";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, laterTitle, {
      updatedAt: "2026-11-06T00:00:00Z",
    })]);
    await second.service.submitInbound(bindMessage(bot.uid, "transfer-restart-after"));
    await waitUntil(
      () => second!.workers.prompts.some((prompt) =>
        prompt.sidecarRoot === sidecarRoot(second!.stateDir, bot.uid, PARENT)
        && prompt.text.includes(laterTitle)),
      "post-restart Loop update in the parent",
    );
    expect(await taskCountByMessageId(second, PARENT, `loop:${queuedInput.id}`)).toBe(1);
    expect(await taskCountByMessageId(second, target, `loop:${queuedInput.id}`)).toBe(1);
    expect((await persistedTask(second, target, `loop:${queuedInput.id}`))?.state).toBe("cancelled");
    await expectDeliveredToParent(second, taskId, target);
  } finally {
    if (second !== undefined) {
      await closeCoreHarness(second);
    } else if (firstClosed) {
      await rm(first.root, { recursive: true, force: true });
    } else {
      await closeCoreHarness(first);
    }
  }
});

test("legacy loop storage migration preserves routes and inputs across reopen", async () => {
  const botUid = "legacy-loop-bot";
  const root = await mkdtemp(join(tmpdir(), "omp-loop-migration-"));
  const key = {
    botUid,
    groupId: GROUP_ID,
    workspaceId: WORKSPACE.workspaceId,
    taskId: "legacy-route-task",
  };
  try {
    const legacy = new Database(join(root, "channel.sqlite"));
    try {
      legacy.exec(`
        CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        INSERT INTO meta (key, value) VALUES ('legacy-marker', 'preserved');
        CREATE TABLE legacy_notes (note TEXT NOT NULL);
        INSERT INTO legacy_notes (note) VALUES ('kept');
      `);
    } finally {
      legacy.close();
    }

    let store = await openChannelStore(root);
    try {
      store.setBotUid(botUid);
      store.setOwnerId(botUid, "owner-1");
      store.bindLoopWorkspace(botUid, GROUP_ID, WORKSPACE, [
        task("legacy-route-task", "Legacy route task", {
          assigneeId: "owner-1",
          assigneeOctoUid: "owner-1",
        }),
      ]);
      store.bindLoopTask(key, `${GROUP_ID}____legacy-child`);
      expect(store.listLoopInputs(botUid)).toHaveLength(1);
    } finally {
      store.close();
    }

    // The real pre-feature database has no route-ending table; reopening must add it additively.
    const drop = new Database(join(root, "channel.sqlite"));
    try {
      drop.exec("DROP TABLE IF EXISTS loop_route_endings");
    } finally {
      drop.close();
    }

    for (let attempt = 1; attempt <= 2; attempt += 1) {
      store = await openChannelStore(root);
      try {
        expect(store.getOwnerId(botUid)).toBe("owner-1");
        expect(store.getLoopBinding(botUid, GROUP_ID)).toMatchObject({
          workspaceId: WORKSPACE.workspaceId,
          version: 1,
        });
        expect(store.getLoopTaskRoute(key)).toMatchObject({
          threadChannelId: `${GROUP_ID}____legacy-child`,
          version: 1,
        });
        expect(store.listLoopInputs(botUid)).toHaveLength(1);
        expect(store.listLoopTaskRouteEndings(botUid)).toEqual([]);
        expect(store.getLoopTaskRouteEnding(key)).toBeNull();
      } finally {
        store.close();
      }
    }

    const migrated = new Database(join(root, "channel.sqlite"), { readonly: true });
    try {
      expect(migrated.query("SELECT value FROM meta WHERE key = 'legacy-marker'").get())
        .toEqual({ value: "preserved" });
      expect(migrated.query("SELECT note FROM legacy_notes").get()).toEqual({ note: "kept" });
      expect(migrated.query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'loop_route_endings'",
      ).get()).toEqual({ name: "loop_route_endings" });
    } finally {
      migrated.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a started child Loop task before prompt dispatch transfers instead of dispatching", async () => {
  const taskId = "loop-started-before-dispatch";
  const loop = loopFixture([task(taskId, "Before started transfer")]);
  const workerStarted = Promise.withResolvers<void>();
  const releaseWorkers = Promise.withResolvers<void>();
  const harness = await createCoreHarness({ loop });
  try {
    await bindWorkspace(harness, "started-dispatch-bind");
    const { thread, target } = await createLinkedThread(
      harness,
      taskId,
      "Started dispatch task",
    );
    // Hold worker creation after markStarted but before prompt_started is recorded.
    harness.workers.beforeCreate = async () => {
      workerStarted.resolve();
      await releaseWorkers.promise;
    };

    const queuedTitle = "Started but undispatched child update";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, queuedTitle, {
      updatedAt: "2026-11-05T00:00:00Z",
    })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "started-dispatch-loop-change"));
    await waitForSignal(workerStarted.promise, "blocked child worker startup");
    const startedInput = await waitForDeliveredInput(harness, taskId, target);
    expect((await persistedTask(harness, target, `loop:${startedInput.id}`))?.state).toBe("running");

    await harness.transport.archiveThread(GROUP_ID, thread.shortId);
    const laterTitle = "Trigger started-task transfer";
    loop.tasks.set(WORKSPACE.workspaceId, [task(taskId, laterTitle, {
      updatedAt: "2026-11-06T00:00:00Z",
    })]);
    await harness.service.submitInbound(bindMessage(harness.bot.uid, "started-dispatch-after-archive"));
    await waitForDeliveredInput(harness, taskId, PARENT);

    expect((await persistedTask(harness, target, `loop:${startedInput.id}`))?.state)
      .toBe("cancelled");
    expect(await taskCountByMessageId(harness, PARENT, `loop:${startedInput.id}`)).toBe(1);
    releaseWorkers.resolve();
    await waitUntil(
      () => harness.workers.prompts.some((prompt) =>
        prompt.sidecarRoot === sidecarRoot(harness.stateDir, harness.bot.uid, PARENT)
        && prompt.text.includes(queuedTitle)),
      "transferred started input in the parent",
    );
    expect(promptCount(harness, target)).toBe(0);
  } finally {
    releaseWorkers.resolve();
    await closeCoreHarness(harness);
  }
});
