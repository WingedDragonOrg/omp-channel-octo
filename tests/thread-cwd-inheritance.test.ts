import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, ThreadSummary } from "../src/contracts";
import { threadParentGroupId, threadShortId } from "../src/threads/thread-target.ts";
import type { LoopTask, LoopWorkspace } from "../src/loop/types.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeLoopReadPort,
  FakeTransport,
  inbound,
  submitAndWaitForIngressHandoff,
  waitForStatus,
  type CoreHarness,
  type PromptRecord,
} from "./fixtures/core-fakes";
import { ChannelStore } from "../src/store";


/**
 * `docs/specs/2026-09-12-octo-channel-design.md` (default work directory order) and the
 * acceptance matrix in `local://thread-cwd-inheritance-cases.md`.
 */
const ALLOWED_GROUP_IDS = ["group-a", "group-a____thread-1", "group-b"];
const GROUP: ChatTarget = { channelId: "group-a", channelType: 2 };
const THREAD: ChatTarget = { channelId: "group-a____thread-1", channelType: 5 };
const LOOP_WORKSPACE: LoopWorkspace = {
  workspaceId: "loop-workspace-child-chat",
  name: "Child chat workspace",
  slug: "child-chat",
};
const LOOP_TASK_UNKNOWN = "child-chat-unknown-allocation";
const LOOP_TASK_PARTIAL = "child-chat-known-partial-allocation";
const UNKNOWN_CREATE_TEXT = "create a Loop child whose allocation is uncertain";
const PARTIAL_CREATE_TEXT = "create a Loop child with a partial workspace";
const LOCAL_THREAD: ChatTarget = { channelId: "group-a____known-local-child", channelType: 5 };
const UNRELATED_THREAD: ChatTarget = { channelId: "group-a____unrelated-child", channelType: 5 };

function loopTask(taskId: string): LoopTask {
  return {
    taskId,
    title: taskId,
    description: null,
    status: "open",
    priority: "normal",
    assigneeType: null,
    assigneeId: null,
    assigneeOctoUid: null,
    projectId: null,
    parentTaskId: null,
    startDate: null,
    dueDate: null,
    createdAt: "2026-11-10T00:00:00Z",
    updatedAt: "2026-11-10T00:00:00Z",
  };
}

class UnknownFirstCreateTransport extends FakeTransport {
  private loseFirstCreateResponse = true;

  override async createThread(
    groupId: string,
    name: string,
    signal?: AbortSignal,
  ): Promise<ThreadSummary> {
    const thread = await super.createThread(groupId, name, signal);
    if (this.loseFirstCreateResponse) {
      this.loseFirstCreateResponse = false;
      throw new Error("connection closed after remote allocation");
    }
    return thread;
  }
}

async function withStore<T>(
  harness: CoreHarness,
  inspect: (store: ChannelStore) => T | Promise<T>,
): Promise<T> {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    return await inspect(store);
  } finally {
    store.close();
  }
}


function createHarness(): Promise<CoreHarness> {
  return createCoreHarness({ allowedGroupIds: ALLOWED_GROUP_IDS });
}

async function createWorkDirectory(harness: CoreHarness, name: string): Promise<string> {
  const path = join(harness.root, name);
  await mkdir(path, { recursive: true });
  return path;
}

/** Narrows a status report to a bound session id so session-identity comparisons stay honest. */
function sessionIdOf(status: { sessionId: string | null }): string {
  if (status.sessionId === null) throw new Error("expected the chat to have a bound session");
  return status.sessionId;
}

/** Groups and threads only accept owner messages that mention the bot and sit in the allowlist. */
function deliver(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
): Promise<void> {
  return submitAndWaitForIngressHandoff(harness, inbound({
    messageId,
    channelId: target.channelId,
    channelType: target.channelType,
    text,
    ...(target.channelType === 1 ? {} : { mentionedUserIds: [harness.bot.uid] }),
  })).then(() => {});
}

/** Sends a task, waits for completion, and returns the worker prompt that task produced. */
async function runTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
): Promise<PromptRecord> {
  const expected = harness.workers.prompts.length + 1;
  await deliver(harness, target, messageId, text);
  await harness.workers.waitForPromptCount(expected);
  await waitForStatus(harness.service, target, (status) => status.state === "completed");
  const prompt = harness.workers.prompts[expected - 1];
  if (!prompt) throw new Error("expected a recorded worker prompt for the completed task");
  return prompt;
}

async function runFailingTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
): Promise<void> {
  await deliver(harness, target, messageId, text);
  await waitForStatus(harness.service, target, (status) => status.state === "failed");
}

const ISOLATE_PROMPT_TEXT = "spin off an isolated thread";

/** Group worker that turns one thread into an isolated workspace through `octo_management`. */
function createIsolationHarness(): Promise<CoreHarness> {
  return createCoreHarness({
    allowedGroupIds: ALLOWED_GROUP_IDS,
    prompt: async ({ promptContext, text }) => {
      if (text !== ISOLATE_PROMPT_TEXT) return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("expected octo_management in a group prompt");
      await management.execute(
        { action: "create-thread", name: "isolated case", worktree_name: "isolated-case" },
        new AbortController().signal,
      );
    },
  });
}

/** Creates one isolated thread in the group and returns its chat target and workspace directory. */
async function isolateThread(
  harness: CoreHarness,
): Promise<{ target: ChatTarget; workspace: string; threadId: string }> {
  const provider = harness.worktrees;
  if (!provider) throw new Error("expected a worktree port in the harness");
  await runTask(harness, GROUP, "isolate-create", ISOLATE_PROMPT_TEXT);
  const request = provider.requests.at(-1);
  if (!request) throw new Error("expected the isolate request to be recorded");
  return {
    target: { channelId: `group-a____${request.threadId}`, channelType: 5 },
    workspace: join(harness.root, "worktrees", request.threadId),
    threadId: request.threadId,
  };
}

test("C1 a thread's first task inherits the parent group cwd without sharing its session", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c1-parent-new", `/new ${dirA}`);
    const parentBefore = await harness.service.status(GROUP);
    expect(parentBefore.cwd).toBe(dirA);
    const parentSessionId = sessionIdOf(parentBefore);

    const prompt = await runTask(harness, THREAD, "c1-thread-task", "thread work");
    expect(prompt.text).toBe("thread work");
    expect(prompt.cwd).toBe(dirA);

    const thread = await harness.service.status(THREAD);
    expect(thread.cwd).toBe(dirA);
    expect(sessionIdOf(thread)).not.toBe(parentSessionId);

    const parentAfter = await harness.service.status(GROUP);
    expect(parentAfter.cwd).toBe(parentBefore.cwd);
    expect(sessionIdOf(parentAfter)).toBe(parentSessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C2 a thread without a parent binding starts in the default work directory", async () => {
  const harness = await createHarness();
  try {
    const prompt = await runTask(harness, THREAD, "c2-thread-task", "orphan thread work");
    expect(prompt.cwd).toBe(join(harness.root, "tmp"));
    const thread = await harness.service.status(THREAD);
    expect(thread.cwd).toBe(join(harness.root, "tmp"));
  } finally {
    await closeCoreHarness(harness);
  }
});
test("an unknown Loop allocation and its no-ID workspace intent do not block child conversations", async () => {
  const transport = new UnknownFirstCreateTransport({
    bot: { uid: "bot-child-loop-allocation", name: "Child chat fixture" },
  });
  const loop = new FakeLoopReadPort();
  loop.workspaces.set(LOOP_WORKSPACE.workspaceId, LOOP_WORKSPACE);
  loop.tasks.set(LOOP_WORKSPACE.workspaceId, [
    loopTask(LOOP_TASK_UNKNOWN),
    loopTask(LOOP_TASK_PARTIAL),
  ]);
  const createResults = new Map<string, {
    creation_state?: string;
    allocated_channel_id?: string | null;
  }>();
  let projectRepo = "";
  let harness = await createCoreHarness({
    allowedGroupIds: ALLOWED_GROUP_IDS,
    transport,
    loop,
    prompt: async ({ promptContext, text }) => {
      const taskId = text === UNKNOWN_CREATE_TEXT
        ? LOOP_TASK_UNKNOWN
        : text === PARTIAL_CREATE_TEXT
          ? LOOP_TASK_PARTIAL
          : null;
      if (taskId === null) return;
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("expected octo_management in the group prompt");
      const result = await management.execute(
        {
          action: "create-thread",
          name: `Loop ${taskId}`,
          task_id: taskId,
          worktree_name: taskId,
          projects: [{ name: "app", repo: projectRepo }],
        },
        new AbortController().signal,
        `child-loop-create-${taskId}`,
      );
      createResults.set(taskId, JSON.parse(result) as {
        creation_state?: string;
        allocated_channel_id?: string | null;
      });
    },
  });
  try {
    const localCwd = await createWorkDirectory(harness, "known-local-child-cwd");
    await deliver(harness, LOCAL_THREAD, "loop-child-owned-new", `/new ${localCwd}`);
    await waitForStatus(
      harness.service,
      LOCAL_THREAD,
      (status) => status.state === "idle" && status.cwd === localCwd,
    );

    const parentCwd = await createWorkDirectory(harness, "loop-parent-cwd");
    await deliver(harness, GROUP, "loop-child-parent-new", `/new ${parentCwd}`);
    await waitForStatus(
      harness.service,
      GROUP,
      (status) => status.state === "idle" && status.cwd === parentCwd,
    );
    projectRepo = join(parentCwd, "loop-child-project");
    await mkdir(projectRepo, { recursive: true });
    execFileSync("git", ["init", "--quiet", projectRepo]);
    await deliver(harness, GROUP, "loop-child-bind-workspace", `/bind loop ${LOOP_WORKSPACE.workspaceId}`);

    await runTask(harness, GROUP, "loop-child-create-unknown", UNKNOWN_CREATE_TEXT);
    expect(createResults.get(LOOP_TASK_UNKNOWN)).toMatchObject({ creation_state: "unknown" });
    await withStore(harness, (store) => {
      expect(store.getLoopThreadCreation({
        botUid: harness.bot.uid,
        groupId: GROUP.channelId,
        workspaceId: LOOP_WORKSPACE.workspaceId,
        taskId: LOOP_TASK_UNKNOWN,
      })).toMatchObject({ state: "unknown", threadChannelId: null });
      const preparingWorkspaceIntents = store.listThreadWorkspaceIntents(harness.bot.uid, GROUP.channelId)
        .filter(
          (intent) => intent.kind === "workspace" && intent.status === "preparing" && intent.channelIds.length === 0,
        );
      expect(preparingWorkspaceIntents).toHaveLength(1);
      expect(preparingWorkspaceIntents[0]?.id).not.toMatch(/^thread-create:/u);
    });

    if (!harness.worktrees) throw new Error("expected the isolated workspace fixture");
    harness.worktrees.failure = new Error("fixture worktree creation failed");
    await runTask(harness, GROUP, "loop-child-create-partial", PARTIAL_CREATE_TEXT);
    const partial = createResults.get(LOOP_TASK_PARTIAL);
    expect(partial).toMatchObject({ creation_state: "allocated" });
    const partialChannelId = partial?.allocated_channel_id;
    expect(typeof partialChannelId).toBe("string");
    if (typeof partialChannelId !== "string") throw new Error("partial remote thread ID was not retained");
    await withStore(harness, (store) => {
      expect(store.getLoopThreadCreation({
        botUid: harness.bot.uid,
        groupId: GROUP.channelId,
        workspaceId: LOOP_WORKSPACE.workspaceId,
        taskId: LOOP_TASK_PARTIAL,
      })).toMatchObject({
        state: "allocated",
        threadChannelId: partialChannelId,
      });
      expect(store.getThreadWorkspace(harness.bot.uid, {
        channelType: 5,
        channelId: partialChannelId,
      })).toMatchObject({ status: "failed" });
    });

    const root = harness.root;
    await closeCoreHarness(harness, { remove: false });
    harness = await createCoreHarness({
      root,
      transport,
      loop,
      allowedGroupIds: ALLOWED_GROUP_IDS,
    });
    await withStore(harness, (store) => {
      expect(store.getLoopThreadCreation({
        botUid: harness.bot.uid,
        groupId: GROUP.channelId,
        workspaceId: LOOP_WORKSPACE.workspaceId,
        taskId: LOOP_TASK_UNKNOWN,
      })).toMatchObject({ state: "unknown", threadChannelId: null });
      const preparingWorkspaceIntents = store.listThreadWorkspaceIntents(harness.bot.uid, GROUP.channelId)
        .filter(
          (intent) => intent.kind === "workspace" && intent.status === "preparing" && intent.channelIds.length === 0,
        );
      expect(preparingWorkspaceIntents).toHaveLength(1);
      expect(preparingWorkspaceIntents[0]?.id).not.toMatch(/^thread-create:/u);
      expect(store.getLoopThreadCreation({
        botUid: harness.bot.uid,
        groupId: GROUP.channelId,
        workspaceId: LOOP_WORKSPACE.workspaceId,
        taskId: LOOP_TASK_PARTIAL,
      })).toMatchObject({ state: "allocated", threadChannelId: partialChannelId });
    });
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toHaveLength(2);

    const unrelated = await runTask(
      harness,
      UNRELATED_THREAD,
      "loop-child-unrelated-conversation",
      "independent child work proceeds",
    );
    expect(unrelated.text).toBe("independent child work proceeds");
    expect(unrelated.cwd).toBe(parentCwd);

    const owned = await runTask(
      harness,
      LOCAL_THREAD,
      "loop-child-known-conversation",
      "known local child work proceeds",
    );
    expect(owned.text).toBe("known local child work proceeds");
    expect(owned.cwd).toBe(localCwd);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C3 a thread binding without a verified session re-reads the parent cwd and persists it", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    const dirB = await createWorkDirectory(harness, "dirB");
    await deliver(harness, GROUP, "c3-parent-new-a", `/new ${dirA}`);

    harness.transport.failGroupContextWith(new Error("fixture context unavailable"));
    await runFailingTask(harness, THREAD, "c3-thread-fail", "thread work that never starts");
    const failed = await harness.service.status(THREAD);
    expect(failed.state).toBe("failed");
    expect(failed.cwd).toBe(dirA);
    expect(harness.workers.prompts).toHaveLength(0);

    harness.transport.failGroupContextWith(undefined);
    await deliver(harness, GROUP, "c3-parent-new-b", `/new ${dirB}`);
    expect((await harness.service.status(GROUP)).cwd).toBe(dirB);

    const prompt = await runTask(harness, THREAD, "c3-thread-task", "thread work after the parent moved");
    expect(prompt.cwd).toBe(dirB);
    expect((await harness.service.status(THREAD)).cwd).toBe(dirB);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C4 a thread with a verified session keeps its cwd and session when the parent switches", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    const dirB = await createWorkDirectory(harness, "dirB");
    await deliver(harness, GROUP, "c4-parent-new-a", `/new ${dirA}`);

    const first = await runTask(harness, THREAD, "c4-thread-first", "first thread turn");
    expect(first.cwd).toBe(dirA);
    const established = await harness.service.status(THREAD);
    expect(established.cwd).toBe(dirA);
    const establishedSessionId = sessionIdOf(established);
    expect(establishedSessionId).toBe(first.sessionId);

    await deliver(harness, GROUP, "c4-parent-new-b", `/new ${dirB}`);
    expect((await harness.service.status(GROUP)).cwd).toBe(dirB);

    const untouched = await harness.service.status(THREAD);
    expect(untouched.cwd).toBe(dirA);
    expect(sessionIdOf(untouched)).toBe(establishedSessionId);

    const second = await runTask(harness, THREAD, "c4-thread-second", "second thread turn");
    expect(second.cwd).toBe(dirA);
    expect(second.sessionId).toBe(establishedSessionId);
    expect((await harness.service.status(THREAD)).cwd).toBe(dirA);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C5 a valid thread without a parent binding starts in the default directory", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c5-parent-new", `/new ${dirA}`);

    const unboundThread: ChatTarget = { channelId: "group-b____thread-5", channelType: 5 };
    const prompt = await runTask(
      harness,
      unboundThread,
      "c5-unbound-parent-task",
      "thread whose parent group has no binding",
    );
    expect(prompt.cwd).toBe(join(harness.root, "tmp"));
    expect((await harness.service.status(unboundThread)).cwd).toBe(join(harness.root, "tmp"));

    expect((await harness.service.status(GROUP)).cwd).toBe(dirA);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C6 malformed thread ids remain invalid for parent-directory resolution", () => {
  for (const channelId of ["group-a", "____thread"]) {
    expect(threadParentGroupId(channelId)).toBeNull();
    expect(threadShortId(channelId)).toBeNull();
  }
});

test("C7 a vanished parent directory falls back to the default directory without rejecting the task", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c7-parent-new", `/new ${dirA}`);
    await rm(dirA, { recursive: true, force: true });

    const prompt = await runTask(harness, THREAD, "c7-thread-task", "work after the parent directory vanished");
    expect(prompt.text).toBe("work after the parent directory vanished");
    expect(prompt.cwd).toBe(join(harness.root, "tmp"));
    const thread = await harness.service.status(THREAD);
    expect(thread.state).toBe("completed");
    expect(thread.cwd).toBe(join(harness.root, "tmp"));
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C8 /new without a path inherits the parent cwd for a fresh thread and keeps it afterwards", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    const dirB = await createWorkDirectory(harness, "dirB");
    await deliver(harness, GROUP, "c8-parent-new-a", `/new ${dirA}`);

    await deliver(harness, THREAD, "c8-thread-new", "/new");
    const inherited = await harness.service.status(THREAD);
    expect(inherited.cwd).toBe(dirA);
    const inheritedSessionId = sessionIdOf(inherited);

    await deliver(harness, GROUP, "c8-parent-new-b", `/new ${dirB}`);
    expect((await harness.service.status(GROUP)).cwd).toBe(dirB);

    await deliver(harness, THREAD, "c8-thread-new-again", "/new");
    const refreshed = await harness.service.status(THREAD);
    expect(refreshed.cwd).toBe(dirA);
    expect(sessionIdOf(refreshed)).not.toBe(inheritedSessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C9 direct messages and groups keep their existing no-argument /new semantics", async () => {
  const harness = await createHarness();
  try {
    const dm: ChatTarget = { channelId: "dm-owner", channelType: 1 };
    await deliver(harness, dm, "c9-dm-new", "/new");
    expect((await harness.service.status(dm)).cwd).toBe(join(harness.root, "tmp"));

    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c9-group-new-a", `/new ${dirA}`);
    const bound = await harness.service.status(GROUP);
    expect(bound.cwd).toBe(dirA);
    const boundSessionId = sessionIdOf(bound);

    await deliver(harness, GROUP, "c9-group-new-default", "/new");
    const reused = await harness.service.status(GROUP);
    expect(reused.cwd).toBe(dirA);
    expect(sessionIdOf(reused)).not.toBe(boundSessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C10 thread inheritance stays isolated from the parent and keeps explicit thread sessions", async () => {
  const harness = await createHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    const dirB = await createWorkDirectory(harness, "dirB");
    const dirX = await createWorkDirectory(harness, "dirX");
    await deliver(harness, GROUP, "c10-parent-new-a", `/new ${dirA}`);
    const parentSessionId = sessionIdOf(await harness.service.status(GROUP));

    const inherited = await runTask(harness, THREAD, "c10-thread-task", "thread turn");
    expect(inherited.cwd).toBe(dirA);
    expect(sessionIdOf(await harness.service.status(THREAD))).not.toBe(parentSessionId);

    const calls = harness.transport.groupContextCalls;
    expect(
      calls.some(({ channelId, channelType }) => channelType === 5 && channelId === THREAD.channelId),
    ).toBe(true);
    expect(
      calls.some(({ channelId, channelType }) => channelType === 2 && channelId === GROUP.channelId),
    ).toBe(false);

    await deliver(harness, THREAD, "c10-thread-new-x", `/new ${dirX}`);
    const explicit = await harness.service.status(THREAD);
    expect(explicit.cwd).toBe(dirX);
    const explicitSessionId = sessionIdOf(explicit);

    await deliver(harness, GROUP, "c10-parent-new-b", `/new ${dirB}`);
    expect((await harness.service.status(GROUP)).cwd).toBe(dirB);

    const after = await runTask(harness, THREAD, "c10-thread-task-after", "thread turn after the parent changed");
    expect(after.cwd).toBe(dirX);
    expect(after.sessionId).toBe(explicitSessionId);
    expect((await harness.service.status(THREAD)).cwd).toBe(dirX);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C11 an isolated thread keeps its pinned workspace when the parent switches directories", async () => {
  const harness = await createIsolationHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c11-parent-new-a", `/new ${dirA}`);
    const isolated = await isolateThread(harness);
    expect(harness.worktrees?.requests).toEqual([
      { baseDir: dirA, label: "isolated-case", threadId: isolated.threadId },
    ]);

    const dirB = await createWorkDirectory(harness, "dirB");
    await deliver(harness, GROUP, "c11-parent-new-b", `/new ${dirB}`);
    expect((await harness.service.status(GROUP)).cwd).toBe(dirB);

    const prompt = await runTask(harness, isolated.target, "c11-thread-task", "thread turn");
    expect(prompt.cwd).toBe(isolated.workspace);
    expect((await harness.service.status(isolated.target)).cwd).toBe(isolated.workspace);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("C12 a pinned thread whose workspace vanished falls back to the parent group cwd", async () => {
  const harness = await createIsolationHarness();
  try {
    const dirA = await createWorkDirectory(harness, "dirA");
    await deliver(harness, GROUP, "c12-parent-new", `/new ${dirA}`);
    const isolated = await isolateThread(harness);
    await rm(isolated.workspace, { recursive: true, force: true });

    const prompt = await runTask(harness, isolated.target, "c12-thread-task", "thread turn");
    expect(prompt.cwd).toBe(dirA);
    expect((await harness.service.status(isolated.target)).cwd).toBe(dirA);
  } finally {
    await closeCoreHarness(harness);
  }
});
