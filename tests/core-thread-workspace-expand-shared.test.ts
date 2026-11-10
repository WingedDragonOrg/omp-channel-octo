import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  ChatTarget,
  HostTool,
  ThreadSummary,
  ThreadWorkspace,
  WorktreeRemovalOutcome,
  WorktreeRemovalRequest,
  ThreadWorkspaceRequest,
} from "../src/contracts.ts";
import { ChannelStore } from "../src/store/index.ts";
import type { ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import {
  FakeTransport,
  closeCoreHarness,
  createCoreHarness,
  FakeWorktrees,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const BOT_NAME = "Workspace Expand Fixture Bot";
function fixtureBot() {
  return { uid: `bot-thread-workspace-expand-${randomUUID()}`, name: BOT_NAME };
}
const GROUP_ID = "group-tools";
const GROUP_TARGET = { channelId: GROUP_ID, channelType: 2 as const };
const SHORT_ID = "shared-expand";
const THREAD = { channelId: `${GROUP_ID}____${SHORT_ID}`, channelType: 5 as const };
const INITIAL_PROJECT_NAME = "api";
const INITIAL_PROJECT_BRANCH = "existing-api-branch";

type PromptGate = {
  started: Promise<void>;
  actionFinished: Promise<void>;
  release(): void;
  run(action: () => Promise<void>): Promise<void>;
};

function createPromptGate(): PromptGate {
  const started = Promise.withResolvers<void>();
  const actionFinished = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  return {
    started: started.promise,
    actionFinished: actionFinished.promise,
    release: () => release.resolve(),
    async run(action): Promise<void> {
      started.resolve();
      try {
        await action();
      } finally {
        actionFinished.resolve();
        await release.promise;
      }
    },
  };
}

async function waitForPromptAction(
  harness: CoreHarness,
  target: ChatTarget,
  text: string,
  prompt: PromptGate,
): Promise<void> {
  await prompt.started;
  expect(harness.workers.prompts.filter((record) => record.text === text)).toHaveLength(1);
  await prompt.actionFinished;
  const running = await waitForStatus(harness.service, target, (status) => status.state === "running");
  expect(running.state).toBe("running");
}

async function finishPrompt(
  harness: CoreHarness,
  target: ChatTarget,
  prompt: PromptGate,
) {
  prompt.release();
  return await waitForStatus(
    harness.service,
    target,
    (status) => status.state === "completed" || status.state === "failed",
  );
}

function managementTool(promptContext: { tools?: HostTool[] } | undefined): HostTool {
  const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
  if (!management) throw new Error("octo_management host tool missing from the real prompt context");
  return management;
}

async function runGit(directory: string, args: string[]): Promise<void> {
  const child = Bun.spawn(["git", "-C", directory, ...args], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "pipe",
  });
  const [stderr, exitCode] = await Promise.all([
    new Response(child.stderr as ReadableStream<Uint8Array>).text(),
    child.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed (${exitCode}): ${stderr.trim()}`);
  }
}

async function gitOutput(directory: string, args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", directory, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed (${exitCode}): ${stderr.trim()}`);
  }
  return stdout.trim();
}

async function createGitRepo(repo: string, name: string): Promise<void> {
  await mkdir(repo, { recursive: true });
  await writeFile(join(repo, "origin.txt"), `${name}\n`, "utf8");
  await runGit(repo, ["init", "-q", "-b", "main"]);
  await runGit(repo, ["add", "."]);
  await runGit(repo, [
    "-c",
    "user.email=fixture@example.com",
    "-c",
    "user.name=Fixture",
    "commit",
    "-q",
    "-m",
    "initial fixture commit",
  ]);
}

class RealGitWorktrees extends FakeWorktrees {
  afterCreate?: (workspace: ThreadWorkspace) => Promise<void>;
  override async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    const { onPlanned, ...recordedRequest } = request;
    this.requests.push({ ...recordedRequest });
    if (this.failure) throw this.failure;
    if (!request.path) throw new Error("the shared workspace must provide an owned project path");

    const repo = await realpath(request.baseDir);
    const cwd = request.path;
    const branch = request.label;
    onPlanned?.({ cwd, branch, repo });
    request.lifecycle?.beforeSpawn(repo);

    let child: Bun.Subprocess;
    try {
      child = Bun.spawn([
        "git",
        "-C",
        repo,
        "worktree",
        "add",
        "-q",
        "-b",
        branch,
        cwd,
        ...(request.base === undefined ? [] : [request.base]),
      ], {
        detached: true,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "pipe",
      });
    } catch (error) {
      request.lifecycle?.onNotSpawned();
      throw error;
    }
    request.lifecycle?.onSpawn({
      pid: child.pid,
      pgid: child.pid,
      identity: `fixture-git-worktree-${child.pid}`,
      cwd: repo,
    });
    const [stderr, exitCode] = await Promise.all([
      new Response(child.stderr as ReadableStream<Uint8Array>).text(),
      child.exited,
    ]);
    request.lifecycle?.onConfirmedExit();
    if (exitCode !== 0) {
      throw new Error(`git worktree add failed (${exitCode}): ${stderr.trim()}`);
    }
    const workspace = { cwd: await realpath(cwd), branch };
    await this.afterCreate?.(workspace);
    return workspace;
  }

  override async remove(request: WorktreeRemovalRequest): Promise<WorktreeRemovalOutcome> {
    this.removals.push({ ...request });
    if (this.removeFailure) throw this.removeFailure;
    if (!request.repo) throw new Error("real Git cleanup requires the recorded repository");
    const lifecycle = request.lifecycle?.("worktree");
    lifecycle?.beforeSpawn(request.repo);

    let child: Bun.Subprocess;
    try {
      child = Bun.spawn([
        "git",
        "-C",
        request.repo,
        "worktree",
        "remove",
        ...(request.force === true ? ["--force"] : []),
        request.path,
      ], {
        detached: true,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "pipe",
      });
    } catch (error) {
      lifecycle?.onNotSpawned();
      throw error;
    }
    lifecycle?.onSpawn({
      pid: child.pid,
      pgid: child.pid,
      identity: `fixture-git-remove-${child.pid}`,
      cwd: request.repo,
    });
    const [stderr, exitCode] = await Promise.all([
      new Response(child.stderr as ReadableStream<Uint8Array>).text(),
      child.exited,
    ]);
    lifecycle?.onConfirmedExit();
    if (exitCode !== 0) {
      throw new Error(`git worktree remove failed (${exitCode}): ${stderr.trim()}`);
    }
    return { kind: "removed", via: "git", branchOutcome: "kept" };
  }
}

class ControlledCancellationWorktrees extends FakeWorktrees {
  readonly createStarted = Promise.withResolvers<string>();
  readonly releaseCreate = Promise.withResolvers<void>();
  abortObserved = false;

  override async create(
    request: ThreadWorkspaceRequest,
    signal?: AbortSignal,
  ): Promise<ThreadWorkspace> {
    const { onPlanned, ...recordedRequest } = request;
    this.requests.push({ ...recordedRequest });
    if (this.failure) throw this.failure;
    if (!request.path) throw new Error("the controlled workspace must have a planned project path");

    const cwd = request.path;
    const branch = request.label;
    const repo = await realpath(request.baseDir);
    onPlanned?.({ cwd, branch, repo });
    request.lifecycle?.beforeSpawn(repo);
    const aborted = Promise.withResolvers<void>();
    const onAbort = (): void => {
      this.abortObserved = true;
      request.lifecycle?.onUnknown();
      aborted.resolve();
    };
    if (signal?.aborted) onAbort();
    else signal?.addEventListener("abort", onAbort, { once: true });
    this.createStarted.resolve(cwd);
    try {
      const outcome = await Promise.race([
        aborted.promise.then(() => "aborted" as const),
        this.releaseCreate.promise.then(() => "released" as const),
      ]);
      if (outcome === "aborted") {
        throw signal?.reason ?? new Error("controlled worktree creation was cancelled");
      }
      return { cwd, branch };
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }
}

const EXPAND_TEXT = "add the web project to this shared workspace";
const INFO_TEXT = "inspect the current shared workspace";
const DELETE_DURING_EXPANSION_TEXT = "delete the thread while its expansion is pending";
const DELETE_AFTER_EXPANSION_TEXT = "delete the thread after its expansion completes";

async function waitForAddedProject(
  harness: CoreHarness,
  projectName: string,
): Promise<ThreadWorkspaceRecord> {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    const deadline = performance.now() + 10_000;
    while (performance.now() < deadline) {
      const workspace = store.getThreadWorkspace(harness.bot.uid, THREAD);
      if (
        workspace?.status === "ready"
        && workspace.projects.some((project) =>
          project.name === projectName && project.created && project.path !== null)
      ) {
        return workspace;
      }
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    throw new Error(`shared workspace did not persist ready project ${projectName}: ${JSON.stringify({
      workspace: store.getThreadWorkspace(harness.bot.uid, THREAD),
      operation: store.getLatestThreadWorkspaceOperation(harness.bot.uid, THREAD),
    })}`);
  } finally {
    store.close();
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (
      typeof error === "object"
      && error !== null
      && "code" in error
      && error.code === "ENOENT"
    ) return false;
    throw error;
  }
}

type SharedWorkspaceFixture = {
  workspaceRoot: string;
  entryPath: string;
  existingProjectPath: string;
  initialProject: ThreadWorkspaceRecord["projects"][number];
};

async function seedExistingSharedWorkspace(
  harness: CoreHarness,
  workspaceName: string,
  existingRepo: string,
  newRepo: string,
): Promise<SharedWorkspaceFixture> {
  const workspaceRoot = join(harness.stateDir, "thread-workspaces", workspaceName);
  const entryPath = join(workspaceRoot, GROUP_ID);
  const existingProjectPath = join(entryPath, INITIAL_PROJECT_NAME);
  const initialProject: ThreadWorkspaceRecord["projects"][number] = {
    name: INITIAL_PROJECT_NAME,
    repo: existingRepo,
    path: existingProjectPath,
    branch: INITIAL_PROJECT_BRANCH,
    created: true,
  };
  await Promise.all([
    createGitRepo(existingRepo, "existing API project"),
    createGitRepo(newRepo, "new web project"),
  ]);
  await mkdir(entryPath, { recursive: true });
  await runGit(existingRepo, [
    "worktree",
    "add",
    "-q",
    "-b",
    INITIAL_PROJECT_BRANCH,
    existingProjectPath,
  ]);

  const thread: ThreadSummary = {
    shortId: SHORT_ID,
    groupId: GROUP_ID,
    channelId: THREAD.channelId,
    name: "Existing shared workspace",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-10T00:00:00.000Z",
    updatedAt: "2026-11-10T00:00:00.000Z",
  };
  harness.transport.setThreads(GROUP_ID, [thread]);

  const workspace: ThreadWorkspaceRecord = {
    botUid: harness.bot.uid,
    channelType: 5,
    channelId: THREAD.channelId,
    layout: "shared",
    status: "ready",
    entryPath,
    workspaceRoot,
    projects: [initialProject],
    error: null,
  };
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      channelId: THREAD.channelId,
      channelType: 5,
      sessionId: "existing-shared-session",
      cwd: entryPath,
      sessionFile: null,
      cwdPinned: true,
    });
    store.saveThreadWorkspace(workspace);
  } finally {
    store.close();
  }
  return { workspaceRoot, entryPath, existingProjectPath, initialProject };
}

test("thread-workspace-add appends a project to an owned shared workspace and thread-info reports the same ownership", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-core-thread-workspace-expand-")));
  const bot = fixtureBot();
  let harness: CoreHarness | undefined;
  let addResult: string | undefined;
  let threadInfo: string | undefined;
  let toolError: string | undefined;
  const workspaceName = "expand-fixture";
  const existingRepo = join(root, "repos", "api");
  const newRepo = join(root, "repos", "web");
  let createStarted = false;
  const releaseCreate = Promise.withResolvers<void>();
  const expansionPrompt = createPromptGate();
  const infoPrompt = createPromptGate();

  try {
    const worktrees = new RealGitWorktrees(root);
    const createWorktree = worktrees.create.bind(worktrees);
    worktrees.create = async (request) => {
      createStarted = true;
      await releaseCreate.promise;
      return createWorktree(request);
    };
    harness = await createCoreHarness({
      root,
      bot,
      allowedGroupIds: [GROUP_ID],
      worktrees,
      prompt: async ({ promptContext, text }) => {
        if (text !== EXPAND_TEXT && text !== INFO_TEXT) return;
        const prompt = text === EXPAND_TEXT ? expansionPrompt : infoPrompt;
        await prompt.run(async () => {
          try {
            const management = managementTool(promptContext);
            const signal = new AbortController().signal;
            if (text === EXPAND_TEXT) {
              addResult = await management.execute({
                action: "thread-workspace-add",
                short_id: SHORT_ID,
                worktree_name: workspaceName,
                projects: [{ name: "web", repo: newRepo }],
              }, signal);
            } else {
              threadInfo = await management.execute({ action: "thread-info" }, signal);
            }
          } catch (error) {
            toolError = error instanceof Error ? error.message : String(error);
          }
        });
      },
    });

    const fixture = await seedExistingSharedWorkspace(
      harness,
      workspaceName,
      existingRepo,
      newRepo,
    );
    const { workspaceRoot, entryPath, existingProjectPath, initialProject } = fixture;

    await harness.service.submitInbound(inbound({
      ...THREAD,
      messageId: "expand-existing-shared-workspace",
      senderId: "owner-1",
      text: EXPAND_TEXT,
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(harness, THREAD, EXPAND_TEXT, expansionPrompt);

    expect(toolError).toBeUndefined();
    if (addResult === undefined) throw new Error("shared workspace expansion was not accepted");
    const result = JSON.parse(addResult) as {
      accepted: boolean;
      status: "queued";
      queued: boolean;
    };
    expect(result).toMatchObject({ accepted: true, status: "queued", queued: true });
    expect(createStarted).toBe(false);
    const expansionStatus = await finishPrompt(harness, THREAD, expansionPrompt);
    expect(expansionStatus.state).toBe("completed");
    for (let attempt = 0; attempt < 10_000 && !createStarted; attempt += 1) {
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    expect(createStarted).toBe(true);
    releaseCreate.resolve();

    const persisted = await waitForAddedProject(harness, "web");
    expect(persisted).not.toBeNull();
    expect(persisted).toMatchObject({
      botUid: harness.bot.uid,
      channelId: THREAD.channelId,
      layout: "shared",
      status: "ready",
      entryPath,
      workspaceRoot,
      error: null,
    });
    expect(persisted?.projects).toHaveLength(2);
    expect(persisted?.projects[0]).toEqual(initialProject);
    const addedProject = persisted?.projects.find(({ name }) => name === "web");
    expect(addedProject).toMatchObject({
      name: "web",
      repo: newRepo,
      branch: workspaceName,
      created: true,
    });
    if (!addedProject?.path) throw new Error("persisted added project omitted its owned path");
    const addedProjectPath = addedProject.path;
    expect(addedProjectPath).not.toBe(existingProjectPath);
    expect(addedProjectPath.startsWith(`${entryPath}/`)).toBe(true);
    expect((await stat(addedProjectPath)).isDirectory()).toBe(true);
    expect(await gitOutput(addedProjectPath, ["rev-parse", "--show-toplevel"])).toBe(addedProjectPath);
    expect(await gitOutput(addedProjectPath, ["branch", "--show-current"])).toBe(workspaceName);
    expect(await readFile(join(addedProjectPath, "origin.txt"), "utf8")).toBe("new web project\n");

    await harness.service.submitInbound(inbound({
      ...THREAD,
      messageId: "inspect-expanded-shared-workspace",
      senderId: "owner-1",
      text: INFO_TEXT,
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(harness, THREAD, INFO_TEXT, infoPrompt);
    const infoStatus = await finishPrompt(harness, THREAD, infoPrompt);
    expect(infoStatus.state).toBe("completed");
    expect(toolError).toBeUndefined();
    if (threadInfo === undefined) throw new Error("thread-info returned no workspace result");

    const info = JSON.parse(threadInfo) as {
      channel_id: string;
      workspace?: {
        layout: string;
        status: string;
        projects: Array<{ name: string; repo: string; path: string; branch: string; created: boolean }>;
      };
    };
    expect(info.channel_id).toBe(THREAD.channelId);
    expect(info.workspace).toBeDefined();
    expect(info.workspace).toMatchObject({
      layout: "shared",
      status: "ready",
      projects: persisted!.projects.map(({ name, repo, path, branch, created }) => ({
        name,
        repo,
        path,
        branch,
        created,
      })),
    });
    expect(worktrees.requests).toContainEqual(expect.objectContaining({
      threadId: SHORT_ID,
      label: workspaceName,
      baseDir: newRepo,
      path: join(entryPath, "web"),
    }));
  } finally {
    releaseCreate.resolve();
    expansionPrompt.release();
    infoPrompt.release();
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });
test("thread-delete preserves pending shared expansion ownership and later cleans its recorded Git worktrees", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-core-thread-workspace-expand-delete-")));
  const bot = fixtureBot();
  let harness: CoreHarness | undefined;
  const workspaceName = "expand-delete-fixture";
  const existingRepo = join(root, "repos", "api");
  const newRepo = join(root, "repos", "web");
  let addResult: string | undefined;
  let deletionDuringResult: string | undefined;
  let deletionAfterResult: string | undefined;
  let expansionError: string | undefined;
  let deletionDuringError: string | undefined;
  let deletionAfterError: string | undefined;
  let createdWorktree: ThreadWorkspace | undefined;
  const expansionPrompt = createPromptGate();
  const deletionDuringPrompt = createPromptGate();
  const deletionAfterPrompt = createPromptGate();
  const releaseCreatedWorktree = Promise.withResolvers<void>();
  const worktrees = new RealGitWorktrees(root);
  worktrees.afterCreate = async (workspace) => {
    createdWorktree = workspace;
    await releaseCreatedWorktree.promise;
  };

  try {
    harness = await createCoreHarness({
      root,
      bot,
      worktrees,
      prompt: async ({ promptContext, text }) => {
        const prompt = text === EXPAND_TEXT
          ? expansionPrompt
          : text === DELETE_DURING_EXPANSION_TEXT
            ? deletionDuringPrompt
            : text === DELETE_AFTER_EXPANSION_TEXT ? deletionAfterPrompt : undefined;
        if (!prompt) return;
        await prompt.run(async () => {
          try {
            const management = managementTool(promptContext);
            const signal = new AbortController().signal;
            if (text === EXPAND_TEXT) {
              addResult = await management.execute({
                action: "thread-workspace-add",
                short_id: SHORT_ID,
                worktree_name: workspaceName,
                projects: [{ name: "web", repo: newRepo }],
              }, signal);
            } else {
              const result = await management.execute({
                action: "thread-delete",
                short_id: SHORT_ID,
              }, signal);
              if (text === DELETE_DURING_EXPANSION_TEXT) deletionDuringResult = result;
              else deletionAfterResult = result;
            }
          } catch (error) {
            const detail = error instanceof Error ? error.message : String(error);
            if (text === EXPAND_TEXT) expansionError = detail;
            else if (text === DELETE_DURING_EXPANSION_TEXT) deletionDuringError = detail;
            else deletionAfterError = detail;
          }
        });
      },
    });
    const fixture = await seedExistingSharedWorkspace(
      harness,
      workspaceName,
      existingRepo,
      newRepo,
    );
    const { workspaceRoot, entryPath, existingProjectPath, initialProject } = fixture;

    await harness.service.submitInbound(inbound({
      ...THREAD,
      messageId: "accept-expansion-before-concurrent-delete",
      senderId: "owner-1",
      text: EXPAND_TEXT,
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(harness, THREAD, EXPAND_TEXT, expansionPrompt);
    expect(expansionError).toBeUndefined();
    if (addResult === undefined) throw new Error("shared workspace expansion was not accepted");
    expect(JSON.parse(addResult)).toMatchObject({ accepted: true, queued: true });
    expect(worktrees.requests).toHaveLength(0);
    expect((await finishPrompt(harness, THREAD, expansionPrompt)).state).toBe("completed");

    const creationDeadline = performance.now() + 10_000;
    while (createdWorktree === undefined && performance.now() < creationDeadline) {
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    if (!createdWorktree) throw new Error("expansion did not create the real Git worktree");
    const addedProjectPath = createdWorktree.cwd;
    expect(await gitOutput(addedProjectPath, ["rev-parse", "--show-toplevel"])).toBe(addedProjectPath);
    expect(await gitOutput(addedProjectPath, ["branch", "--show-current"])).toBe(workspaceName);

    await harness.service.submitInbound(inbound({
      ...GROUP_TARGET,
      messageId: "delete-thread-during-expansion",
      senderId: "owner-1",
      text: DELETE_DURING_EXPANSION_TEXT,
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(
      harness,
      GROUP_TARGET,
      DELETE_DURING_EXPANSION_TEXT,
      deletionDuringPrompt,
    );
    expect(deletionDuringError).toContain("子区工作区追加尚未结算");
    expect(deletionDuringResult).toBeUndefined();
    expect((await finishPrompt(harness, GROUP_TARGET, deletionDuringPrompt)).state).toBe("completed");
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-delete")).toHaveLength(0);
    expect(worktrees.removals).toEqual([]);
    expect(await realpath(existingProjectPath)).toBe(existingProjectPath);
    expect(await realpath(addedProjectPath)).toBe(addedProjectPath);

    const pendingStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      const ownership = pendingStore.getThreadWorkspace(harness.bot.uid, THREAD);
      expect(ownership).toMatchObject({
        layout: "shared",
        status: "ready",
        entryPath,
        workspaceRoot,
      });
      expect(ownership?.projects).toEqual([initialProject]);
      const operation = pendingStore.getLatestThreadWorkspaceOperation(harness.bot.uid, THREAD);
      expect(operation?.status).toBe("running");
      expect(operation?.candidateWorkspace.projects.find(({ name }) => name === "web")).toMatchObject({
        name: "web",
        repo: newRepo,
        path: addedProjectPath,
        branch: workspaceName,
        created: false,
      });
    } finally {
      pendingStore.close();
    }

    releaseCreatedWorktree.resolve();
    const persisted = await waitForAddedProject(harness, "web");
    expect(persisted).toMatchObject({
      layout: "shared",
      status: "ready",
      entryPath,
      workspaceRoot,
    });
    expect(persisted.projects).toHaveLength(2);
    expect(persisted.projects.find(({ name }) => name === "web")).toMatchObject({
      repo: newRepo,
      path: addedProjectPath,
      branch: workspaceName,
      created: true,
    });

    await harness.service.submitInbound(inbound({
      ...GROUP_TARGET,
      messageId: "delete-thread-after-expansion-settles",
      senderId: "owner-1",
      text: DELETE_AFTER_EXPANSION_TEXT,
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(
      harness,
      GROUP_TARGET,
      DELETE_AFTER_EXPANSION_TEXT,
      deletionAfterPrompt,
    );
    expect(deletionAfterError).toBeUndefined();
    expect(deletionAfterResult).toContain(`已删除线程 ${SHORT_ID}`);
    expect((await finishPrompt(harness, GROUP_TARGET, deletionAfterPrompt)).state).toBe("completed");
    expect(worktrees.removals).toHaveLength(2);
    expect(worktrees.removals).toContainEqual(expect.objectContaining({
      path: existingProjectPath,
      repo: existingRepo,
      branch: INITIAL_PROJECT_BRANCH,
      force: false,
    }));
    expect(worktrees.removals).toContainEqual(expect.objectContaining({
      path: addedProjectPath,
      repo: newRepo,
      branch: workspaceName,
      force: false,
    }));
    expect(await pathExists(existingProjectPath)).toBe(false);
    expect(await pathExists(addedProjectPath)).toBe(false);
  } finally {
    releaseCreatedWorktree.resolve();
    expansionPrompt.release();
    deletionDuringPrompt.release();
    deletionAfterPrompt.release();
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("thread-workspace-add is cancelled in the target chat without deleting confirmed or unknown resources", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-core-thread-workspace-expand-cancel-")));
  const bot = fixtureBot();
  const worktrees = new ControlledCancellationWorktrees(root);
  const expansionPrompt = createPromptGate();
  let harness: CoreHarness | undefined;
  let addResult: string | undefined;
  let toolError: string | undefined;
  const workspaceName = "expand-cancel-fixture";
  const existingRepo = join(root, "repos", "api");
  const newRepo = join(root, "repos", "web");

  try {
    harness = await createCoreHarness({
      root,
      bot,
      allowedGroupIds: [GROUP_ID],
      worktrees,
      prompt: async ({ promptContext, text }) => {
        if (text !== "start workspace expansion for cancellation") return;
        await expansionPrompt.run(async () => {
          try {
            addResult = await managementTool(promptContext).execute({
              action: "thread-workspace-add",
              short_id: SHORT_ID,
              worktree_name: workspaceName,
              projects: [{ name: "web", repo: newRepo }],
            }, new AbortController().signal);
          } catch (error) {
            toolError = error instanceof Error ? error.message : String(error);
          }
        });
      },
    });
    const fixture = await seedExistingSharedWorkspace(
      harness,
      workspaceName,
      existingRepo,
      newRepo,
    );
    const { existingProjectPath, entryPath } = fixture;

    await harness.service.submitInbound(inbound({
      ...GROUP_TARGET,
      messageId: "start-workspace-expansion-for-cancel",
      senderId: "owner-1",
      text: "start workspace expansion for cancellation",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForPromptAction(
      harness,
      GROUP_TARGET,
      "start workspace expansion for cancellation",
      expansionPrompt,
    );
    expect(toolError).toBeUndefined();
    if (addResult === undefined) throw new Error("workspace expansion was not accepted");
    expect(JSON.parse(addResult)).toMatchObject({ accepted: true, status: "queued", queued: true });
    const plannedProjectPath = await worktrees.createStarted.promise;
    expect(plannedProjectPath).toBe(join(entryPath, "web"));

    const cancelReply = harness.transport.waitForRemoteText(({ target, text }) =>
      target.channelType === THREAD.channelType
      && target.channelId === THREAD.channelId
      && (text.includes("取消") || text.includes("中止") || text.includes("无法确认")));
    await harness.service.submitInbound(inbound({
      ...THREAD,
      messageId: "cancel-running-workspace-expansion",
      senderId: "owner-1",
      text: "/cancel",
      mentionedUserIds: [harness.bot.uid],
    }));
    await cancelReply;
    expect(worktrees.abortObserved).toBe(true);

    const operationStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    let operation = operationStore.getLatestThreadWorkspaceOperation(harness.bot.uid, THREAD);
    try {
      for (let attempt = 0; attempt < 10_000 && operation?.status !== "unknown"; attempt += 1) {
        const turn = Promise.withResolvers<void>();
        setImmediate(turn.resolve);
        await turn.promise;
        operation = operationStore.getLatestThreadWorkspaceOperation(harness.bot.uid, THREAD);
      }
    } finally {
      operationStore.close();
    }
    expect(operation).toMatchObject({
      status: "unknown",
      candidateWorkspace: { layout: "shared", status: "preparing" },
    });
    expect(operation?.candidateWorkspace.projects.find(({ name }) => name === "web")).toMatchObject({
      repo: newRepo,
      path: plannedProjectPath,
      branch: workspaceName,
      created: false,
    });
    expect(worktrees.removals).toEqual([]);
    expect(await pathExists(plannedProjectPath)).toBe(false);
    expect(await realpath(existingProjectPath)).toBe(existingProjectPath);
    expect(await gitOutput(existingProjectPath, ["branch", "--show-current"]))
      .toBe(INITIAL_PROJECT_BRANCH);
    expect((await finishPrompt(harness, GROUP_TARGET, expansionPrompt)).state).toBe("completed");
  } finally {
    worktrees.releaseCreate.resolve();
    expansionPrompt.release();
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("a cancelled safe workspace expansion is not replayed after restart", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-core-thread-workspace-expand-cancel-restart-")));
  const bot = fixtureBot();
  const workspaceName = "expand-cancel-restart-fixture";
  const existingRepo = join(root, "repos", "api");
  const newRepo = join(root, "repos", "web");
  const createAttemptsPath = join(root, "workspace-create-attempts");
  const createAbortedPath = join(root, "workspace-create-aborted");
  const sourceMessageId = "start-workspace-expansion-before-restart";
  const cancelMessageId = "cancel-workspace-expansion-before-restart";
  let setup: CoreHarness | undefined;
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;

  try {
    setup = await createCoreHarness({
      root,
      bot,
      allowedGroupIds: [GROUP_ID],
      worktrees: new FakeWorktrees(root),
    });
    const fixture = await seedExistingSharedWorkspace(setup, workspaceName, existingRepo, newRepo);
    await closeCoreHarness(setup, { remove: false });
    setup = undefined;

    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const childSource = `
      import { appendFileSync, writeFileSync } from "node:fs";
      import { join } from "node:path";
      import { createCoreHarness, FakeTransport, FakeWorktrees, inbound } from ${JSON.stringify(fixtureUrl)};

      const root = process.env.WORKSPACE_CANCEL_ROOT;
      if (!root) throw new Error("WORKSPACE_CANCEL_ROOT is missing");
      const bot = ${JSON.stringify(bot)};
      const groupId = ${JSON.stringify(GROUP_ID)};
      const shortId = ${JSON.stringify(SHORT_ID)};
      const groupTarget = { channelId: groupId, channelType: 2 };
      const threadTarget = { channelId: groupId + "____" + shortId, channelType: 5 };
      const sourceText = "start workspace expansion before restart";
      const attemptsPath = join(root, "workspace-create-attempts");
      const abortedPath = join(root, "workspace-create-aborted");
      const thread = {
        shortId,
        groupId,
        channelId: threadTarget.channelId,
        name: "Workspace cancellation restart fixture",
        creatorUid: "owner-1",
        status: 1,
        memberCount: 1,
        messageCount: 0,
        createdAt: "2026-11-10T00:00:00.000Z",
        updatedAt: "2026-11-10T00:00:00.000Z",
      };
      const transport = new FakeTransport({ bot });
      transport.setThreads(groupId, [thread]);

      class HeldSafePhaseWorktrees extends FakeWorktrees {
        async create(request, signal) {
          const { onPlanned, ...recordedRequest } = request;
          this.requests.push({ ...recordedRequest });
          appendFileSync(attemptsPath, "create\\n", "utf8");
          if (!signal) throw new Error("workspace operation did not provide a cancellation signal");
          const aborted = Promise.withResolvers();
          const onAbort = () => {
            writeFileSync(abortedPath, "aborted", "utf8");
            aborted.resolve();
          };
          if (signal.aborted) onAbort();
          else signal.addEventListener("abort", onAbort, { once: true });
          await aborted.promise;
          const hold = Promise.withResolvers();
          process.stdin.once("data", hold.resolve);
          await hold.promise;
          signal.removeEventListener("abort", onAbort);
          return { cwd: request.path, branch: request.label };
        }
      }

      const waitForFile = async (path, label) => {
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          if (await Bun.file(path).exists()) return;
          await new Promise(resolve => setImmediate(resolve));
        }
        throw new Error("timed out waiting for " + label);
      };
      const service = await createCoreHarness({
        root,
        bot,
        transport,
        allowedGroupIds: [groupId],
        worktrees: new HeldSafePhaseWorktrees(root),
        prompt: async ({ promptContext, text }) => {
          if (text !== sourceText) return;
          const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
          if (!management) throw new Error("octo_management host tool missing");
          await management.execute({
            action: "thread-workspace-add",
            short_id: shortId,
            worktree_name: ${JSON.stringify(workspaceName)},
            projects: [{ name: "web", repo: ${JSON.stringify(newRepo)} }],
          }, new AbortController().signal);
        },
      });

      const sourceReceipt = service.service.receive(inbound({
        ...groupTarget,
        messageId: ${JSON.stringify(sourceMessageId)},
        senderId: "owner-1",
        text: sourceText,
        mentionedUserIds: [bot.uid],
      }));
      await sourceReceipt.ackReady;
      await waitForFile(attemptsPath, "the safe-phase workspace create call");

      const cancelReceipt = service.service.receive(inbound({
        ...threadTarget,
        messageId: ${JSON.stringify(cancelMessageId)},
        senderId: "owner-1",
        text: "/cancel",
        mentionedUserIds: [bot.uid],
      }));
      await cancelReceipt.ackReady;
      await waitForFile(abortedPath, "the workspace operation's cancellation signal");

      const hold = Promise.withResolvers();
      process.stdin.once("data", hold.resolve);
      await hold.promise;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: { ...process.env, WORKSPACE_CANCEL_ROOT: root },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const waitForChildFile = async (path: string, label: string): Promise<void> => {
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        if (await pathExists(path)) return;
        if (child?.exitCode !== null) {
          throw new Error(`cancellation restart fixture exited before ${label}: ${await childStderr}`);
        }
        const turn = Promise.withResolvers<void>();
        setImmediate(turn.resolve);
        await turn.promise;
      }
      throw new Error(`timed out waiting for ${label}`);
    };
    await waitForChildFile(createAttemptsPath, "the workspace create call");
    await waitForChildFile(createAbortedPath, "the accepted workspace cancellation");
    expect(await readFile(createAttemptsPath, "utf8")).toBe("create\n");

    child.kill("SIGKILL");
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;

    const interruptedStore = await ChannelStore.open(join(root, "state", "channel.sqlite"));
    try {
      const interrupted = interruptedStore.getLatestThreadWorkspaceOperation(bot.uid, THREAD);
      expect(interrupted).toMatchObject({ status: "running", phase: "project-not-started" });
      expect(interrupted?.candidateWorkspace.projects.find(({ name }) => name === "web"))
        .toMatchObject({ created: false, branch: null });
    } finally {
      interruptedStore.close();
    }

    const restartTransport = new FakeTransport({ bot });
    restartTransport.setThreads(GROUP_ID, [{
      shortId: SHORT_ID,
      groupId: GROUP_ID,
      channelId: THREAD.channelId,
      name: "Workspace cancellation restart fixture",
      creatorUid: "owner-1",
      status: 1,
      memberCount: 1,
      messageCount: 0,
      createdAt: "2026-11-10T00:00:00.000Z",
      updatedAt: "2026-11-10T00:00:00.000Z",
    }]);
    const restartWorktrees = new FakeWorktrees(root);
    restarted = await createCoreHarness({
      root,
      bot,
      transport: restartTransport,
      allowedGroupIds: [GROUP_ID],
      worktrees: restartWorktrees,
    });

    const operationStore = await ChannelStore.open(join(restarted.stateDir, "channel.sqlite"));
    try {
      let operation = operationStore.getLatestThreadWorkspaceOperation(bot.uid, THREAD);
      for (
        let attempt = 0;
        attempt < 10_000 && (operation?.status === "queued" || operation?.status === "running");
        attempt += 1
      ) {
        const turn = Promise.withResolvers<void>();
        setImmediate(turn.resolve);
        await turn.promise;
        operation = operationStore.getLatestThreadWorkspaceOperation(bot.uid, THREAD);
      }
      expect(operation?.status).toBe("cancelled");
      expect(operation?.candidateWorkspace.projects.find(({ name }) => name === "web"))
        .toMatchObject({ created: false, branch: null });
      expect(restartWorktrees.requests).toHaveLength(0);
      expect(operationStore.getThreadWorkspace(bot.uid, THREAD)).toMatchObject({
        layout: "shared",
        status: "ready",
        entryPath: fixture.entryPath,
        projects: [fixture.initialProject],
      });
      expect(await pathExists(join(fixture.entryPath, "web"))).toBe(false);
    } finally {
      operationStore.close();
    }
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (setup) await closeCoreHarness(setup, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });
