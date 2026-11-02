import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, sep } from "node:path";
import type {
  ChatTarget,
  HostTool,
  WorktreeMergeRequest,
  WorktreePort,
} from "../src/contracts.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  type CoreHarness,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-multi-project-workspaces";
const GROUP_TARGET = { channelId: GROUP_ID, channelType: 2 as const };
const TASK_SECRET = "不得进入 THREAD.md 的原始用户任务细节";
const SHARED_BACKGROUND = "对两个服务仓库进行联合维护";
const SHARED_DISPATCH = "读取共享线程中的两个仓库";
const SEPARATE_DISPATCHES = ["读取项目一", "读取项目二"] as const;

interface ProjectOutcome {
  name: string;
  repo: string;
  path: string | null;
  branch: string | null;
  created: boolean;
  error?: string;
}

interface SharedOutcome {
  short_id: string;
  channel_id: string;
  layout: "shared";
  ready: boolean;
  cwd: string | null;
  projects: ProjectOutcome[];
  error?: string;
}

interface SeparateThreadOutcome {
  short_id: string;
  channel_id: string;
  ready: boolean;
  cwd: string | null;
  projects: ProjectOutcome[];
  error?: string;
}

interface SeparateOutcome {
  layout: "separate";
  ready: boolean;
  threads: SeparateThreadOutcome[];
  error?: string;
}

interface DispatchedOutcome {
  dispatched: boolean;
  delivery: "steered" | "queued";
  short_id: string;
  channel_id: string;
}

class WorkspaceTransport extends FakeTransport {
  readonly contextWrites: Array<{ target: ChatTarget; content: string }> = [];
  failThreadContextWrites = false;

  override async updateGroupContext(
    target: ChatTarget,
    content: string,
    signal?: AbortSignal,
  ): Promise<{ version: number }> {
    this.contextWrites.push({ target: { ...target }, content });
    if (this.failThreadContextWrites && target.channelType === 5) {
      throw new Error("fixture thread description write failed");
    }
    return super.updateGroupContext(target, content, signal);
  }
}

const TRANSLATING_OMP_STUB = String.raw`set -e
[ "$1" = "worktree" ] && [ "$2" = "add" ] || exit 64
[ "$4" = "-b" ] && [ "$6" = "-C" ] || exit 64
exec git -C "$7" worktree add "$3" -b "$5" -q`;

async function createGitWorktreePort(root: string): Promise<WorktreePort> {
  const command = join(root, "omp-worktree-translator");
  await writeFile(command, `#!/bin/sh\n${TRANSLATING_OMP_STUB}\n`, "utf8");
  await chmod(command, 0o755);
  return createWorktreePort({
    command,
    worktrunkCandidates: [join(root, "missing-wt")],
    createTimeoutMs: 20_000,
    removeTimeoutMs: 20_000,
  });
}

async function createWorkspaceHarness(
  prompt: FakePrompt,
  options: {
    transport?: WorkspaceTransport;
    instrument?: (port: WorktreePort) => WorktreePort;
  } = {},
): Promise<{ harness: CoreHarness; transport: WorkspaceTransport }> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-multi-project-")));
  try {
    const transport = options.transport ?? new WorkspaceTransport();
    const realPort = await createGitWorktreePort(root);
    const worktreePort = options.instrument?.(realPort) ?? realPort;
    const harness = await createCoreHarness({
      root,
      bot: { uid: `bot-multi-project-${root.split(sep).at(-1)}`, name: "Fixture Bot" },
      allowedGroupIds: [GROUP_ID],
      prompt,
      transport,
      worktreePort,
    });
    return { harness, transport };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function createRepo(
  root: string,
  name: string,
  originText: string,
  options: { commit?: boolean } = {},
): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo, { recursive: true });
  await writeFile(join(repo, "origin.txt"), originText, "utf8");
  await runGit(repo, ["init", "-q", "-b", "main"]);
  if (options.commit ?? true) {
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
  return realpath(repo);
}

async function runGit(directory: string, args: string[]): Promise<string> {
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

async function isGitRepository(directory: string): Promise<boolean> {
  try {
    await runGit(directory, ["rev-parse", "--show-toplevel"]);
    return true;
  } catch {
    return false;
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function managementTool(promptContext: { tools?: HostTool[] } | undefined): HostTool {
  const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
  if (!management) throw new Error("octo_management host tool missing from the real prompt context");
  return management;
}

function toolSignal(): AbortSignal {
  return new AbortController().signal;
}

async function createThread(
  tool: HostTool,
  args: Record<string, unknown>,
): Promise<string> {
  return tool.execute({ action: "create-thread", ...args }, toolSignal());
}

async function dispatchThread(
  tool: HostTool,
  shortId: string,
  text: string,
): Promise<DispatchedOutcome> {
  return JSON.parse(
    await tool.execute({ action: "thread-dispatch", short_id: shortId, text }, toolSignal()),
  ) as DispatchedOutcome;
}

async function sendGroupTask(harness: CoreHarness, messageId: string, text: string): Promise<void> {
  await harness.service.handleInbound(inbound({
    ...GROUP_TARGET,
    messageId,
    text,
    mentionedUserIds: [harness.bot.uid],
  }));
}

async function sendThreadTask(
  harness: CoreHarness,
  channelId: string,
  messageId: string,
  text: string,
): Promise<void> {
  await harness.service.handleInbound(inbound({
    channelId,
    channelType: 5,
    messageId,
    text,
    mentionedUserIds: [harness.bot.uid],
  }));
}

function isInside(parent: string, path: string): boolean {
  const child = relative(parent, path);
  return child.length > 0 && child !== ".." && !child.startsWith(`..${sep}`);
}

function promptCompletionLatch(): { completion: Promise<void>; complete: () => void } {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { completion: promise, complete: resolve };
}

async function finishGroupTask(
  harness: CoreHarness,
  promptCompletion: Promise<void>,
): Promise<void> {
  await promptCompletion;
  const status = await waitForStatus(
    harness.service,
    GROUP_TARGET,
    (current) => current.state === "completed" || current.state === "failed",
  );
  expect(status.state).toBe("completed");
}

async function finishThreadTask(harness: CoreHarness, channelId: string): Promise<void> {
  const status = await waitForStatus(
    harness.service,
    { channelId, channelType: 5 },
    (current) => current.state === "completed" || current.state === "failed",
  );
  expect(status.state).toBe("completed");
}

test("shared multi-project thread has one real entry and dispatch reads both actual source repos", async () => {
  let created: SharedOutcome | undefined;
  let dispatched: DispatchedOutcome | undefined;
  let accessed: { cwd: string; first: string; second: string } | undefined;
  let operationError: string | undefined;
  let childError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ cwd, promptContext, text }) => {
    const isParentTask = text.startsWith("create shared workspace:");
    try {
      const management = managementTool(promptContext);
      if (text.startsWith("create shared workspace:")) {
        const result = JSON.parse(await createThread(management, {
          name: "Shared Project Workspace",
          layout: "shared",
          worktree_name: "multi-project-seed",
          projects: [
            { name: "project-one", repo: firstRepo },
            { name: "project-two", repo: secondRepo },
          ],
          background: SHARED_BACKGROUND,
        })) as SharedOutcome;
        if (
          result.layout !== "shared" ||
          result.ready !== true ||
          typeof result.cwd !== "string" ||
          !Array.isArray(result.projects) ||
          result.projects.length !== 2 ||
          result.projects.some((project) =>
            project.created !== true ||
            typeof project.path !== "string" ||
            !project.branch?.startsWith("multi-project-seed-")
          )
        ) {
          operationError = "shared create-thread response was not ready with two created project paths";
          return;
        }
        if (typeof result.short_id !== "string" || typeof result.channel_id !== "string") {
          operationError = "shared create-thread response omitted its thread identity";
          return;
        }
        const entry = result.cwd;
        if (await realpath(entry) !== entry || await isGitRepository(entry)) {
          operationError = "shared thread cwd was not its real non-repository entry";
          return;
        }
        for (const project of result.projects) {
          const path = project.path;
          if (
            !path ||
            !isInside(entry, path) ||
            await realpath(path) !== path ||
            await runGit(path, ["rev-parse", "--show-toplevel"]) !== path
          ) {
            operationError = `shared project ${project.name} is not a real worktree below its entry`;
            return;
          }
        }
        created = result;
        const outcome = await dispatchThread(management, result.short_id, SHARED_DISPATCH);
        if (!outcome.dispatched || outcome.channel_id !== result.channel_id) {
          operationError = "shared thread dispatch did not target the created thread";
          return;
        }
        dispatched = outcome;
        return;
      }
      if (text === SHARED_DISPATCH) {
        try {
          const firstPath = created?.projects[0]?.path;
          const secondPath = created?.projects[1]?.path;
          if (!firstPath || !secondPath) throw new Error("shared project paths missing in dispatch task");
          accessed = {
            cwd,
            first: await readFile(join(firstPath, "origin.txt"), "utf8"),
            second: await readFile(join(secondPath, "origin.txt"), "utf8"),
          };
        } catch (error) {
          childError = error instanceof Error ? error.message : String(error);
        }
      }
    } catch (error) {
      operationError = error instanceof Error ? error.message : String(error);
    }
    finally {
      if (isParentTask) parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "shared-create", `create shared workspace: ${TASK_SECRET}`);
    await finishGroupTask(harness, parentPrompt.completion);
    expect(operationError).toBeUndefined();
    if (!created) throw new Error("shared create-thread result was not captured");
    expect(created.layout).toBe("shared");
    expect(created.ready).toBe(true);
    expect(created.projects).toHaveLength(2);
    expect(dispatched?.dispatched).toBe(true);
    await harness.workers.waitForPromptCount(2);
    await finishThreadTask(harness, created.channel_id);
    expect(childError).toBeUndefined();

    expect(created.cwd).not.toBeNull();
    const entry = created.cwd!;
    expect(await realpath(entry)).toBe(entry);
    expect(isInside(join(harness.stateDir, "thread-workspaces"), entry)).toBe(true);
    expect(await isGitRepository(entry)).toBe(false);
    expect(created.projects).toHaveLength(2);
    for (const [index, project] of created.projects.entries()) {
      expect(project.created).toBe(true);
      expect(project.repo).toBe(index === 0 ? firstRepo : secondRepo);
      expect(project.path).not.toBeNull();
      const path = project.path!;
      expect(isInside(entry, path)).toBe(true);
      expect(await realpath(path)).toBe(path);
      expect(await runGit(path, ["rev-parse", "--show-toplevel"])).toBe(path);
    }
    expect(created.projects[0]?.path).not.toBe(created.projects[1]?.path);
    expect(dispatched).toMatchObject({ dispatched: true, delivery: "queued", channel_id: created.channel_id });
    expect(accessed).toEqual({ cwd: entry, first: "origin-one\n", second: "origin-two\n" });

    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(SHARED_BACKGROUND);
    expect(context?.content).toContain("project-one");
    expect(context?.content).toContain("project-two");
    expect(context?.content).toContain(created.projects[0]!.path!);
    expect(context?.content).toContain(created.projects[1]!.path!);
    expect(context?.content).not.toContain(TASK_SECRET);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("legacy single-thread dispatch preserves parent cwd fallback after its worktree disappears", async () => {
  let createdPath: string | undefined;
  let createdChannelId: string | undefined;
  let dispatchOutcome: DispatchedOutcome | undefined;
  let operationError: string | undefined;
  let childCwd: string | undefined;
  let childError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ cwd, promptContext, text }) => {
    const isParentTask = text === "create a legacy thread and dispatch after worktree removal";
    try {
      if (isParentTask) {
        const management = managementTool(promptContext);
        const result = JSON.parse(await createThread(management, {
          name: "Legacy Workspace",
          worktree_name: "legacy-workspace",
        })) as {
          short_id: string;
          channel_id: string;
          ready: boolean;
          worktree?: { path: string; branch: string };
        };
        if (
          result.ready !== true ||
          typeof result.short_id !== "string" ||
          typeof result.channel_id !== "string" ||
          typeof result.worktree?.path !== "string"
        ) {
          throw new Error("legacy create-thread response did not include its ready worktree");
        }
        createdPath = result.worktree.path;
        createdChannelId = result.channel_id;
        await rm(createdPath, { recursive: true, force: true });
        dispatchOutcome = await dispatchThread(
          management,
          result.short_id,
          "legacy missing worktree fallback",
        );
        return;
      }
      if (text === "legacy missing worktree fallback") childCwd = cwd;
    } catch (error) {
      if (isParentTask) {
        operationError = error instanceof Error ? error.message : String(error);
      } else {
        childError = error instanceof Error ? error.message : String(error);
      }
    } finally {
      if (isParentTask) parentPrompt.complete();
    }
  });
  try {
    const sourceRepo = await createRepo(harness.root, "legacy-source", "legacy parent cwd\n");
    await harness.service.handleInbound(inbound({
      ...GROUP_TARGET,
      messageId: "legacy-workspace-bind",
      text: `/new ${sourceRepo}`,
      mentionedUserIds: [harness.bot.uid],
    }));
    await sendGroupTask(harness, "legacy-workspace-create", "create a legacy thread and dispatch after worktree removal");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(operationError).toBeUndefined();
    expect(createdPath).toBeDefined();
    expect(createdChannelId).toBeDefined();
    expect(dispatchOutcome).toMatchObject({
      dispatched: true,
      delivery: "queued",
      channel_id: createdChannelId,
    });
    expect(await pathExists(createdPath!)).toBe(false);
    await harness.workers.waitForPromptCount(2);
    await finishThreadTask(harness, createdChannelId!);
    expect(childError).toBeUndefined();
    expect(childCwd).toBe(sourceRepo);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread dispatch follows an explicit cwd rebind after an owned project disappears", async () => {
  let created: SharedOutcome | undefined;
  let oldProjectPath: string | undefined;
  let dispatchOutcome: DispatchedOutcome | undefined;
  let boundDispatchError: string | undefined;
  let operationError: string | undefined;
  let childCwd: string | undefined;
  let childMarker: string | undefined;
  let childError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ cwd, promptContext, text }) => {
    const isParentTask = text === "create shared workspace, detach binding, and dispatch";
    try {
      if (isParentTask) {
        const management = managementTool(promptContext);
        const result = JSON.parse(await createThread(management, {
          name: "Detached Shared Workspace",
          layout: "shared",
          projects: [{ name: "project-one", repo: sourceRepo }],
        })) as SharedOutcome;
        if (
          result.ready !== true ||
          typeof result.short_id !== "string" ||
          typeof result.channel_id !== "string" ||
          typeof result.projects[0]?.path !== "string"
        ) {
          throw new Error("shared create-thread response did not include a ready project");
        }
        created = result;
        oldProjectPath = result.projects[0].path;
        await rm(oldProjectPath, { recursive: true, force: true });
        try {
          await dispatchThread(management, result.short_id, "must not run in a missing shared workspace");
        } catch (error) {
          boundDispatchError = error instanceof Error ? error.message : String(error);
        }
        if (boundDispatchError === undefined) {
          throw new Error("dispatch was accepted while still bound to a missing shared project");
        }
        await harness.service.handleInbound(inbound({
          channelId: result.channel_id,
          channelType: 5,
          messageId: "detach-missing-shared-workspace",
          text: `/new ${outsidePath}`,
          mentionedUserIds: [harness.bot.uid],
        }));
        dispatchOutcome = await dispatchThread(
          management,
          result.short_id,
          "read detached cwd marker",
        );
        return;
      }
      if (text === "read detached cwd marker") {
        childCwd = cwd;
        childMarker = await readFile(join(cwd, "dispatch.marker"), "utf8");
      }
    } catch (error) {
      if (isParentTask) {
        operationError = error instanceof Error ? error.message : String(error);
      } else {
        childError = error instanceof Error ? error.message : String(error);
      }
    } finally {
      if (isParentTask) parentPrompt.complete();
    }
  });
  let sourceRepo = "";
  let outsidePath = "";
  try {
    sourceRepo = await createRepo(harness.root, "detached-source", "source repo\n");
    outsidePath = join(harness.root, "explicit-outside");
    await mkdir(outsidePath, { recursive: true });
    await writeFile(join(outsidePath, "dispatch.marker"), "explicitly rebound\n", "utf8");
    await sendGroupTask(
      harness,
      "detached-workspace-dispatch",
      "create shared workspace, detach binding, and dispatch",
    );
    await finishGroupTask(harness, parentPrompt.completion);

    expect(operationError).toBeUndefined();
    if (!created) throw new Error("detached shared workspace result was not captured");
    expect(created.ready).toBe(true);
    expect(boundDispatchError).toContain("子区工作区未就绪");
    expect(oldProjectPath).toBeDefined();
    expect(await pathExists(oldProjectPath!)).toBe(false);
    expect(dispatchOutcome).toMatchObject({
      dispatched: true,
      delivery: "queued",
      channel_id: created.channel_id,
    });
    await harness.workers.waitForPromptCount(2);
    await finishThreadTask(harness, created.channel_id);
    expect(childError).toBeUndefined();
    expect(childCwd).toBe(outsidePath);
    expect(childMarker).toBe("explicitly rebound\n");
  } finally {
    await closeCoreHarness(harness);
  }
});


test("separate multi-project threads each dispatch from their own actual repository worktree", async () => {
  let created: SeparateOutcome | undefined;
  const scheduledPaths = new Map<string, string>();
  const accesses: Array<{ text: string; cwd: string; content: string }> = [];
  let operationError: string | undefined;
  let childError: string | undefined;
  const dispatchOutcomes: DispatchedOutcome[] = [];
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ cwd, promptContext, text }) => {
    const isParentTask = text.startsWith("create separate workspaces:");
    try {
      const management = managementTool(promptContext);
      if (text.startsWith("create separate workspaces:")) {
        const result = JSON.parse(await createThread(management, {
          name: "Separate Project Workspaces",
          layout: "separate",
          worktree_name: "multi-project-seed",
          projects: [
            { name: "project-one", repo: firstRepo },
            { name: "project-two", repo: secondRepo },
          ],
          background: "独立维护两个服务仓库",
        })) as SeparateOutcome;
        if (
          result.layout !== "separate" ||
          result.ready !== true ||
          !Array.isArray(result.threads) ||
          result.threads.length !== 2 ||
          result.threads.some((thread) =>
            thread.ready !== true ||
            typeof thread.channel_id !== "string" ||
            typeof thread.cwd !== "string" ||
            !Array.isArray(thread.projects) ||
            thread.projects.length !== 1 ||
            thread.projects[0]?.created !== true ||
            typeof thread.projects[0]?.path !== "string" ||
            !thread.projects[0]?.branch?.startsWith("multi-project-seed-")
          )
        ) {
          operationError = "separate create-thread response was not ready with two project threads";
          return;
        }
        for (const thread of result.threads) {
          const path = thread.projects[0]?.path;
          if (
            typeof thread.short_id !== "string" ||
            typeof path !== "string" ||
            thread.cwd !== path ||
            await realpath(path) !== path ||
            await runGit(path, ["rev-parse", "--show-toplevel"]) !== path
          ) {
            operationError = `separate thread ${thread.short_id} has no real project worktree cwd`;
            return;
          }
        }
        created = result;
        for (const [index, thread] of result.threads.entries()) {
          const dispatchText = SEPARATE_DISPATCHES[index];
          const path = thread.projects[0]?.path;
          if (!dispatchText || !path) {
            operationError = `separate thread ${thread.short_id} has no project path`;
            return;
          }
          scheduledPaths.set(dispatchText, path);
          const dispatched = await dispatchThread(management, thread.short_id, dispatchText);
          dispatchOutcomes.push(dispatched);
          if (!dispatched.dispatched || dispatched.channel_id !== thread.channel_id) {
            operationError = `dispatch did not target separate thread ${thread.short_id}`;
            return;
          }
        }
        return;
      }
      if (scheduledPaths.has(text)) {
        try {
          accesses.push({ text, cwd, content: await readFile(join(cwd, "origin.txt"), "utf8") });
        } catch (error) {
          childError = error instanceof Error ? error.message : String(error);
        }
      }
    } catch (error) {
      operationError = error instanceof Error ? error.message : String(error);
    } finally {
      if (isParentTask) parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "separate-create", `create separate workspaces: ${TASK_SECRET}`);
    await finishGroupTask(harness, parentPrompt.completion);
    expect(operationError).toBeUndefined();
    if (!created) throw new Error("separate create-thread result was not captured");
    expect(created.layout).toBe("separate");
    expect(created.ready).toBe(true);
    expect(created.threads).toHaveLength(2);
    expect(dispatchOutcomes).toHaveLength(2);
    expect(dispatchOutcomes.every(({ dispatched }) => dispatched)).toBe(true);
    await harness.workers.waitForPromptCount(3);
    for (const thread of created.threads) await finishThreadTask(harness, thread.channel_id);
    expect(childError).toBeUndefined();
    const expectedOrigins = new Map<string, { repo: string; content: string }>([
      [SEPARATE_DISPATCHES[0]!, { repo: firstRepo, content: "origin-one\n" }],
      [SEPARATE_DISPATCHES[1]!, { repo: secondRepo, content: "origin-two\n" }],
    ]);
    expect(accesses).toHaveLength(2);
    for (const access of accesses) {
      const expected = expectedOrigins.get(access.text);
      const expectedPath = scheduledPaths.get(access.text);
      if (!expected || !expectedPath) throw new Error(`unexpected separate project dispatch: ${access.text}`);
      const thread = created.threads.find(({ projects }) => projects[0]?.path === expectedPath);
      if (!thread || !thread.projects[0]) throw new Error(`no separate thread owns ${expectedPath}`);
      expect(access.cwd).toBe(expectedPath);
      expect(await runGit(access.cwd, ["rev-parse", "--show-toplevel"])).toBe(access.cwd);
      expect(access.content).toBe(expected.content);
      expect(thread.projects[0].repo).toBe(expected.repo);
      expect(thread.ready).toBe(true);
      const context = await transport.getGroupContext({ channelId: thread.channel_id, channelType: 5 });
      expect(context?.content).toContain(thread.short_id);
      expect(context?.content).toContain(expectedPath);
      expect(context?.content).not.toContain(TASK_SECRET);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a partial shared workspace batch stays unready and refuses thread dispatch", async () => {
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  let dispatchError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create partial workspace") return;
    try {
      const management = managementTool(promptContext);
      const result = JSON.parse(await createThread(management, {
        name: "Partial Workspace",
        layout: "shared",
        projects: [
          { name: "project-one", repo: firstRepo },
          { name: "project-two", repo: secondRepo },
        ],
        background: "保留已成功创建的项目并说明失败项",
      })) as SharedOutcome;
      if (
        result.layout !== "shared" ||
        result.ready !== false ||
        !Array.isArray(result.projects) ||
        result.projects.length !== 2 ||
        result.projects[0]?.created !== true ||
        typeof result.projects[0]?.path !== "string" ||
        result.projects[1]?.created !== false ||
        typeof result.projects[1]?.error !== "string"
      ) {
        callbackError = `partial create-thread response did not preserve per-project outcomes: ${JSON.stringify(result)}`;
        return;
      }
      const successfulPath = result.projects[0]?.path;
      if (
        typeof result.short_id !== "string" ||
        !successfulPath ||
        typeof result.projects[1]?.error !== "string" ||
        result.projects[1].error.length === 0 ||
        await realpath(successfulPath) !== successfulPath ||
        await runGit(successfulPath, ["rev-parse", "--show-toplevel"]) !== successfulPath
      ) {
        callbackError = "partial response did not include a real successful worktree and explicit failure";
        return;
      }
      created = result;
      try {
        await dispatchThread(management, result.short_id, "此任务不得进入未就绪线程");
      } catch (error) {
        dispatchError = error instanceof Error ? error.message : String(error);
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    }
    finally {
      parentPrompt.complete();
    }
  }, {
    instrument: (port) => ({
      create: (request, signal) =>
        request.baseDir === secondRepo
          ? Promise.reject(new Error("fixture second project worktree creation failed"))
          : port.create(request, signal),
      merge: (request, signal) => port.merge(request, signal),
      remove: (request, signal) => port.remove(request, signal),
      worktrunk: (signal) => port.worktrunk(signal),
    }),
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "partial-create", "create partial workspace");
    await finishGroupTask(harness, parentPrompt.completion);
    expect(callbackError).toBeUndefined();
    if (!created) throw new Error("partial create-thread result was not captured");

    expect(created.ready).toBe(false);
    expect(created.projects).toHaveLength(2);
    expect(created.projects[0]).toMatchObject({ name: "project-one", repo: firstRepo, created: true });
    expect(created.projects[0]?.path).not.toBeNull();
    expect(created.projects[1]).toMatchObject({
      name: "project-two",
      repo: secondRepo,
      path: null,
      branch: null,
      created: false,
    });
    expect(created.projects[1]?.error).toContain("fixture second project worktree creation failed");
    expect(typeof created.projects[1]?.error).toBe("string");
    expect(created.projects[1]?.error?.length).toBeGreaterThan(0);
    expect(typeof dispatchError).toBe("string");
    expect(dispatchError?.length).toBeGreaterThan(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(await isGitRepository(created.projects[0]!.path!)).toBe(true);

    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(created.projects[0]!.path!);
    if (created.projects[1]?.path) expect(context?.content).not.toContain(created.projects[1].path);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed thread description write leaves created projects unready and undispatchable", async () => {
  const transport = new WorkspaceTransport();
  transport.failThreadContextWrites = true;
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  let dispatchError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create workspace with failed instructions") return;
    try {
      const management = managementTool(promptContext);
      const result = JSON.parse(await createThread(management, {
        name: "Unpublished Workspace",
        layout: "shared",
        projects: [
          { name: "project-one", repo: firstRepo },
          { name: "project-two", repo: secondRepo },
        ],
        background: "仅展示创建成功的项目路径",
      })) as SharedOutcome;
      if (
        result.layout !== "shared" ||
        result.ready !== false ||
        !Array.isArray(result.projects) ||
        result.projects.length !== 2 ||
        result.projects.some((project) => project.created !== true || typeof project.path !== "string")
      ) {
        callbackError = "description failure response did not preserve the created resources as unready";
        return;
      }
      if (
        typeof result.short_id !== "string" ||
        result.projects.some((project) => !project.path || project.path.length === 0)
      ) {
        callbackError = "description failure response omitted the thread or project paths";
        return;
      }
      for (const project of result.projects) {
        const path = project.path;
        if (!path || await realpath(path) !== path || await runGit(path, ["rev-parse", "--show-toplevel"]) !== path) {
          callbackError = `description failure project ${project.name} is not a real worktree`;
          return;
        }
      }
      created = result;
      try {
        await dispatchThread(management, result.short_id, "不能派发");
      } catch (error) {
        dispatchError = error instanceof Error ? error.message : String(error);
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    }
    finally {
      parentPrompt.complete();
    }
  }, { transport });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "context-write-failure", "create workspace with failed instructions");
    await finishGroupTask(harness, parentPrompt.completion);
    expect(callbackError).toBeUndefined();

    if (!created) throw new Error("failed-instruction create-thread result was not captured");
    expect(created.ready).toBe(false);
    expect(created.projects).toHaveLength(2);
    expect(created.projects.every(({ created: wasCreated }) => wasCreated)).toBe(true);
    expect(created.projects.every(({ path }) => typeof path === "string")).toBe(true);
    expect(typeof dispatchError).toBe("string");
    expect(dispatchError?.length).toBeGreaterThan(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(transport.contextWrites.some(({ target }) => target.channelId === created!.channel_id)).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("host thread-merge rejects a shared workspace before calling the real WorktreePort merge", async () => {
  let created: SharedOutcome | undefined;
  let createError: string | undefined;
  let mergeError: string | undefined;
  const mergeCalls: WorktreeMergeRequest[] = [];
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    const isParentTask = text === "create workspace before merge attempt";
    try {
      const management = managementTool(promptContext);
      if (text === "create workspace before merge attempt") {
        const result = JSON.parse(await createThread(management, {
          name: "Shared Merge Guard",
          layout: "shared",
          projects: [
            { name: "project-one", repo: firstRepo },
            { name: "project-two", repo: secondRepo },
          ],
        })) as SharedOutcome;
        if (
          result.layout !== "shared" ||
          result.ready !== true ||
          typeof result.cwd !== "string" ||
          !Array.isArray(result.projects) ||
          result.projects.length !== 2 ||
          result.projects.some((project) => project.created !== true || typeof project.path !== "string") ||
          typeof result.short_id !== "string" ||
          typeof result.channel_id !== "string"
        ) {
          createError = "shared merge guard creation did not return a ready thread and two worktrees";
          return;
        }
        for (const project of result.projects) {
          const path = project.path;
          if (
            !path ||
            !isInside(result.cwd, path) ||
            await realpath(path) !== path ||
            await runGit(path, ["rev-parse", "--show-toplevel"]) !== path
          ) {
            createError = `shared merge guard project ${project.name} is not a real worktree`;
            return;
          }
        }
        if (await realpath(result.cwd) !== result.cwd || await isGitRepository(result.cwd)) {
          createError = "shared merge guard cwd is not its real non-repository entry";
          return;
        }
        created = result;
        return;
      }
      if (text === "attempt shared merge") {
        try {
          await management.execute({ action: "thread-merge" }, toolSignal());
        } catch (error) {
          mergeError = error instanceof Error ? error.message : String(error);
        }
      }
    } catch (error) {
      if (text === "attempt shared merge") {
        mergeError = error instanceof Error ? error.message : String(error);
      } else {
        createError = error instanceof Error ? error.message : String(error);
      }
    }
    finally {
      if (isParentTask) parentPrompt.complete();
    }
  }, {
    instrument: (port) => ({
      create: (request, signal) => port.create(request, signal),
      merge: (request, signal) => {
        mergeCalls.push({ ...request });
        return port.merge(request, signal);
      },
      remove: (request, signal) => port.remove(request, signal),
      worktrunk: (signal) => port.worktrunk(signal),
    }),
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "merge-guard-create", "create workspace before merge attempt");
    await finishGroupTask(harness, parentPrompt.completion);
    expect(createError).toBeUndefined();
    if (!created) throw new Error("shared merge guard workspace was not created");
    expect(created.layout).toBe("shared");
    expect(created.ready).toBe(true);
    await sendThreadTask(harness, created.channel_id, "merge-guard-attempt", "attempt shared merge");
    await harness.workers.waitForPromptCount(2);
    await finishThreadTask(harness, created.channel_id);

    expect(mergeError).toBe("当前为多项目工作区，不支持 /thread merge。");
    expect(mergeCalls).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread-delete keeps a dirty shared project worktree and its user changes", async () => {
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const dirtyContents = "uncommitted user change\n";
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create and delete dirty workspace") return;
    try {
      const management = managementTool(promptContext);
      const result = JSON.parse(await createThread(management, {
        name: "Dirty Shared Workspace",
        layout: "shared",
        projects: [
          { name: "project-one", repo: firstRepo },
          { name: "project-two", repo: secondRepo },
        ],
      })) as SharedOutcome;
      if (
        result.layout !== "shared" ||
        result.ready !== true ||
        typeof result.cwd !== "string" ||
        !Array.isArray(result.projects) ||
        result.projects.length !== 2 ||
        result.projects.some((project) => project.created !== true || typeof project.path !== "string")
      ) {
        callbackError = "dirty guard fixture did not create a ready shared workspace";
        return;
      }
      const dirtyPath = result.projects[0]?.path;
      if (
        !dirtyPath ||
        !isInside(result.cwd, dirtyPath) ||
        await realpath(dirtyPath) !== dirtyPath ||
        await runGit(dirtyPath, ["rev-parse", "--show-toplevel"]) !== dirtyPath
      ) {
        callbackError = "dirty guard fixture project is not a real worktree";
        return;
      }
      created = result;
      await writeFile(join(dirtyPath, "origin.txt"), dirtyContents, "utf8");
      await management.execute({ action: "thread-delete", short_id: result.short_id }, toolSignal());
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    }
    finally {
      parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "source-one", "origin-one\n");
    secondRepo = await createRepo(harness.root, "source-two", "origin-two\n");
    await sendGroupTask(harness, "dirty-workspace-delete", "create and delete dirty workspace");
    await finishGroupTask(harness, parentPrompt.completion);
    expect(callbackError).toBeUndefined();
    if (!created) throw new Error("dirty workspace result was not captured");
    const dirtyPath = created.projects[0]?.path;
    if (!dirtyPath) throw new Error("dirty shared project path was not returned");
    expect(await pathExists(dirtyPath)).toBe(true);
    expect(await realpath(dirtyPath)).toBe(dirtyPath);
    expect(await readFile(join(dirtyPath, "origin.txt"), "utf8")).toBe(dirtyContents);
    expect(await runGit(dirtyPath, ["rev-parse", "--show-toplevel"])).toBe(dirtyPath);
    expect(await readFile(join(firstRepo, "origin.txt"), "utf8")).toBe("origin-one\n");
    expect(await pathExists(secondRepo)).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("workspace ownership write failure leaves its remote thread non-dispatchable across restart", async () => {
  let firstRepo = "";
  let secondRepo = "";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  let initialDispatchError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create workspace with unavailable SQLite ownership") return;
    try {
      const management = managementTool(promptContext);
      created = JSON.parse(await createThread(management, {
        name: "SQLite Ownership Failure",
        layout: "shared",
        projects: [
          { name: "project-one", repo: firstRepo },
          { name: "project-two", repo: secondRepo },
        ],
      })) as SharedOutcome;
      try {
        await dispatchThread(management, created.short_id, "must not dispatch");
      } catch (error) {
        initialDispatchError = error instanceof Error ? error.message : String(error);
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    }
    finally {
      parentPrompt.complete();
    }
  });
  let database: Database | undefined;
  let harnessClosed = false;
  let restarted: CoreHarness | undefined;
  let restartDispatchError: string | undefined;
  try {
    firstRepo = await createRepo(harness.root, "sqlite-source-one", "one\n");
    secondRepo = await createRepo(harness.root, "sqlite-source-two", "two\n");
    database = new Database(join(harness.stateDir, "channel.sqlite"));
    database.exec(`
      CREATE TRIGGER reject_thread_workspace_insert
      BEFORE INSERT ON thread_workspaces
      BEGIN
        SELECT RAISE(ABORT, 'fixture thread workspace insert rejected');
      END
    `);

    await sendGroupTask(
      harness,
      "sqlite-workspace-row-failure",
      "create workspace with unavailable SQLite ownership",
    );
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created) throw new Error("SQLite failure result was not captured");
    expect(created.ready).toBe(false);
    expect(created.error).toContain("fixture thread workspace insert rejected");
    expect(created.projects).toHaveLength(2);
    expect(created.projects.every((project) => !project.created && project.path === null)).toBe(true);
    expect(created.cwd).not.toBeNull();
    if (!created.cwd) throw new Error("failed shared workspace did not report its planned entry path");
    expect(await pathExists(created.cwd)).toBe(false);
    expect(initialDispatchError).toContain("未就绪");
    expect(await transport.listThreads(GROUP_ID)).toHaveLength(1);
    expect(transport.contextWrites.filter(({ target }) => target.channelType === 5)).toHaveLength(0);
    for (const repo of [firstRepo, secondRepo]) {
      const worktrees = await runGit(repo, ["worktree", "list", "--porcelain"]);
      expect(worktrees.split("\n").filter((line) => line.startsWith("worktree "))).toHaveLength(1);
    }

    await closeCoreHarness(harness, { remove: false });
    harnessClosed = true;
    const restartPrompt = promptCompletionLatch();
    restarted = await createCoreHarness({
      root: harness.root,
      bot: harness.bot,
      transport,
      allowedGroupIds: [GROUP_ID],
      prompt: async ({ promptContext, text }) => {
        if (text !== "try dispatch after restart") return;
        try {
          await dispatchThread(
            managementTool(promptContext),
            created!.short_id,
            "must remain blocked after restart",
          );
        } catch (error) {
          restartDispatchError = error instanceof Error ? error.message : String(error);
        }
        finally {
          restartPrompt.complete();
        }
      },
      ...(harness.worktreePort === undefined ? {} : { worktreePort: harness.worktreePort }),
    });
    await sendGroupTask(restarted, "sqlite-restart-dispatch", "try dispatch after restart");
    await finishGroupTask(restarted, restartPrompt.completion);
    expect(restartDispatchError).toContain("未就绪");
  } finally {
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (!harnessClosed) await closeCoreHarness(harness, { remove: false });
    database?.close();
    await rm(harness.root, { recursive: true, force: true });
  }
});

test("shared workspaces reject a state directory inside a Git worktree before creating resources", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-git-state-dir-")));
  const transport = new WorkspaceTransport();
  const parentPrompt = promptCompletionLatch();
  let createError: string | undefined;
  let harness: CoreHarness | undefined;
  try {
    await runGit(root, ["init", "-q", "-b", "main"]);
    const firstRepo = await createRepo(root, "nested-source-one", "origin-one\n");
    const secondRepo = await createRepo(root, "nested-source-two", "origin-two\n");
    const worktreePort = await createGitWorktreePort(root);
    harness = await createCoreHarness({
      root,
      bot: { uid: `bot-git-state-dir-${root.split(sep).at(-1)}`, name: "Fixture Bot" },
      allowedGroupIds: [GROUP_ID],
      transport,
      worktreePort,
      prompt: async ({ promptContext, text }) => {
        if (text !== "create shared workspace under repository state") return;
        try {
          await createThread(managementTool(promptContext), {
            name: "Git-Contained State Directory",
            layout: "shared",
            projects: [
              { name: "project-one", repo: firstRepo },
              { name: "project-two", repo: secondRepo },
            ],
          });
        } catch (error) {
          createError = error instanceof Error ? error.message : String(error);
        } finally {
          parentPrompt.complete();
        }
      },
    });
    await sendGroupTask(harness, "git-state-dir-create", "create shared workspace under repository state");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(createError).toContain("共享工作区入口位于 Git worktree 内");
    expect(createError?.length).toBeLessThanOrEqual(200);
    expect(await transport.listThreads(GROUP_ID)).toHaveLength(0);
    expect(transport.managementCalls.filter(({ action }) => action === "create-thread")).toHaveLength(0);
    expect(await pathExists(join(harness.stateDir, "thread-workspaces"))).toBe(false);
    for (const repo of [firstRepo, secondRepo]) {
      const worktrees = await runGit(repo, ["worktree", "list", "--porcelain"]);
      expect(worktrees.split("\n").filter((line) => line.startsWith("worktree "))).toHaveLength(1);
    }
  } finally {
    if (harness) await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
});
