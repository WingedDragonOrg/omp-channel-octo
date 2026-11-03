import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, readdir, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, relative, sep } from "node:path";
import type {
  ChatTarget,
  HostTool,
  WorktreeMergeRequest,
  WorktreePort,
} from "../src/contracts.ts";
import { ChannelStore } from "../src/store/index.ts";
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

interface SingleOutcome {
  short_id: string;
  channel_id: string;
  layout: "single";
  ready: boolean;
  cwd: string | null;
  projects: ProjectOutcome[];
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

async function storedThreadWorkspace(harness: CoreHarness, channelId: string) {
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    return store.getThreadWorkspace(harness.bot.uid, { channelId, channelType: 5 });
  } finally {
    store.close();
  }
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
          worktree_name: "Consolidate Authentication / Services",
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
            project.branch !== "consolidate-authentication-services"
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
            path !== join(entry, project.name) ||
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
    expect(basename(entry)).toBe("consolidate-authentication-services");
    expect(await realpath(entry)).toBe(entry);
    expect(isInside(join(harness.stateDir, "thread-workspaces"), entry)).toBe(true);
    expect(await isGitRepository(entry)).toBe(false);
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.status).toBe("ready");
    expect(persisted?.entryPath).toBe(entry);
    expect(persisted?.projects.map(({ name, repo, path, branch }) => ({ name, repo, path, branch })))
      .toEqual(created.projects.map(({ name, repo, path, branch }) => ({ name, repo, path, branch })));
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
          worktree_name: "detach-after-project-missing",
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
  const taskSlug = "separate-services-with-one-task-name";
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
          worktree_name: taskSlug,
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
            thread.projects[0]?.branch !== taskSlug
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
            path !== `${thread.projects[0]?.repo}.${taskSlug}` ||
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
      const persisted = await storedThreadWorkspace(harness, thread.channel_id);
      expect(persisted?.status).toBe("ready");
      expect(persisted?.entryPath).toBeNull();
      expect(persisted?.projects[0]?.path).toBe(expectedPath);
      expect(persisted?.projects[0]?.branch).toBe(taskSlug);
      const context = await transport.getGroupContext({ channelId: thread.channel_id, channelType: 5 });
      expect(context?.content).toContain(thread.short_id);
      expect(context?.content).toContain(expectedPath);
      expect(context?.content).not.toContain(TASK_SECRET);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("single explicit project uses the complete task slug in its repository worktree path", async () => {
  const taskSlug = "single-project-refresh-session-auth";
  let created: SingleOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create a single project workspace") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Single Project Workspace",
        worktree_name: taskSlug,
        projects: [{ name: "admin-web", repo }],
      })) as SingleOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "single-source", "single origin\n");
    await sendGroupTask(harness, "single-workspace-create", "create a single project workspace");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created) throw new Error("single project workspace result was not captured");
    expect(created.layout).toBe("single");
    expect(created.ready).toBe(true);
    const path = `${repo}.${taskSlug}`;
    expect(created.cwd).toBe(path);
    expect(created.projects).toMatchObject([
      { name: "admin-web", repo, path, branch: taskSlug, created: true },
    ]);
    expect(await realpath(path)).toBe(path);
    expect(await runGit(path, ["rev-parse", "--show-toplevel"])).toBe(path);
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.entryPath).toBeNull();
    expect(persisted?.projects[0]?.path).toBe(path);
    expect(persisted?.projects[0]?.branch).toBe(taskSlug);
    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(path);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("shared project paths and branches use task and project names, not project order", async () => {
  const taskSlugs = [
    "maintain-webhook-authentication",
    "maintain-webhook-authentication-reordered",
  ] as const;
  let outcomes: Array<{ taskSlug: string; result: SharedOutcome }> = [];
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create shared projects in both orders") return;
    try {
      const management = managementTool(promptContext);
      const projects = [
        { name: "Admin Web", repo: adminRepo },
        { name: "Customer Web", repo: customerRepo },
      ];
      outcomes = [];
      for (const [index, orderedProjects] of [projects, [...projects].reverse()].entries()) {
        const taskSlug = taskSlugs[index];
        if (!taskSlug) throw new Error("task slug fixture is missing");
        const result = JSON.parse(await createThread(management, {
          name: `Shared Web Projects ${index + 1}`,
          layout: "shared",
          worktree_name: taskSlug,
          projects: orderedProjects,
        })) as SharedOutcome;
        outcomes.push({ taskSlug, result });
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let adminRepo = "";
  let customerRepo = "";
  try {
    adminRepo = await createRepo(harness.root, "admin-source", "admin origin\n");
    customerRepo = await createRepo(harness.root, "customer-source", "customer origin\n");
    await sendGroupTask(harness, "ordered-shared-create", "create shared projects in both orders");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    expect(outcomes).toHaveLength(2);
    const projectSlugs: Record<string, string> = {
      "Admin Web": "admin-web",
      "Customer Web": "customer-web",
    };
    for (const { taskSlug, result } of outcomes) {
      expect(result.ready).toBe(true);
      expect(result.layout).toBe("shared");
      if (!result.cwd) throw new Error("shared workspace omitted its entry path");
      expect(basename(result.cwd)).toBe(taskSlug);
      const pathsByDisplayName = result.projects.map((project) => {
        const projectSlug = projectSlugs[project.name];
        if (!projectSlug || !project.path) throw new Error(`unexpected project name: ${project.name}`);
        expect(project.path).toBe(join(result.cwd!, projectSlug));
        expect(project.branch).toBe(taskSlug);
        expect(project.repo).toBe(project.name === "Admin Web" ? adminRepo : customerRepo);
        return [project.name, basename(project.path)] as const;
      }).sort(([left], [right]) => left.localeCompare(right));
      expect(pathsByDisplayName).toEqual([
        ["Admin Web", "admin-web"],
        ["Customer Web", "customer-web"],
      ]);
      const persisted = await storedThreadWorkspace(harness, result.channel_id);
      expect(persisted?.entryPath).toBe(result.cwd);
      expect(persisted?.projects.map(({ name, path, branch }) => ({ name, path, branch })))
        .toEqual(result.projects.map(({ name, path, branch }) => ({ name, path, branch })));
      const context = await transport.getGroupContext({ channelId: result.channel_id, channelType: 5 });
      for (const project of result.projects) expect(context?.content).toContain(project.path!);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("shared projects from the same repository keep independent slugs and suffix only the colliding branch", async () => {
  const taskSlug = "shared-backend-contract-refresh";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create same-repository shared projects") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Shared Backend Projects",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [
          { name: "backend-baseline", repo },
          { name: "backend-feature", repo },
        ],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "shared-backend-source", "backend origin\n");
    await sendGroupTask(harness, "same-repository-shared-create", "create same-repository shared projects");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd) throw new Error("same-repository shared workspace was not created");
    expect(created.ready).toBe(true);
    expect(basename(created.cwd)).toBe(taskSlug);
    const actualProjects = created.projects.map((project) => ({
      name: project.name,
      repo: project.repo,
      path: project.path,
      branch: project.branch,
    }));
    expect(actualProjects).toEqual([
      {
        name: "backend-baseline",
        repo,
        path: join(created.cwd, "backend-baseline"),
        branch: taskSlug,
      },
      {
        name: "backend-feature",
        repo,
        path: join(created.cwd, "backend-feature"),
        branch: `${taskSlug}-2`,
      },
    ]);
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.entryPath).toBe(created.cwd);
    expect(persisted?.projects.map(({ name, repo: projectRepo, path, branch }) => ({
      name,
      repo: projectRepo,
      path,
      branch,
    }))).toEqual(actualProjects);
    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    for (const project of actualProjects) expect(context?.content).toContain(project.path!);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("explicit projects without worktree_name are rejected before remote allocation", async () => {
  let missingSeedErrors: string[] = [];
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "omit the explicit task name") return;
    try {
      const management = managementTool(promptContext);
      const missingSeedRequests = [
        {
          name: "Missing Task Slug",
          layout: "shared",
          projects: [
            { name: "admin-web", repo: firstRepo },
            { name: "customer-web", repo: secondRepo },
          ],
        },
        {
          name: "Missing Single Task Slug",
          projects: [{ name: "admin-web", repo: firstRepo }],
        },
      ];
      missingSeedErrors = [];
      for (const request of missingSeedRequests) {
        const error = await createThread(management, request).then(
          () => "resolved",
          (failure: Error) => failure.message,
        );
        missingSeedErrors.push(error);
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "missing-seed-one", "one\n");
    secondRepo = await createRepo(harness.root, "missing-seed-two", "two\n");
    await sendGroupTask(harness, "missing-task-slug", "omit the explicit task name");
    await finishGroupTask(harness, parentPrompt.completion);
    expect(callbackError).toBeUndefined();

    expect(missingSeedErrors).toHaveLength(2);
    for (const error of missingSeedErrors) {
      expect(error).not.toBe("resolved");
      expect(error).toContain("worktree_name");
      expect(error).toMatch(/任务|task/iu);
    }
    expect(await transport.listThreads(GROUP_ID)).toHaveLength(0);
    expect(await pathExists(join(harness.stateDir, "thread-workspaces"))).toBe(false);
    for (const repo of [firstRepo, secondRepo]) {
      const worktrees = await runGit(repo, ["worktree", "list", "--porcelain"]);
      expect(worktrees.split("\n").filter((line) => line.startsWith("worktree "))).toHaveLength(1);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("invalid project and task slugs explain how to fix names without allocating resources", async () => {
  let nameErrors: string[] = [];
  let taskSlugError: string | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "reject invalid project names") return;
    try {
      const management = managementTool(promptContext);
      const invalidProjects = [
        [{ name: "admin-web", repo: firstRepo }, { name: "admin-web", repo: secondRepo }],
        [{ name: "Admin Web", repo: firstRepo }, { name: "admin-web", repo: secondRepo }],
        [{ name: "后台管理", repo: firstRepo }, { name: "customer-web", repo: secondRepo }],
        [{ name: "a".repeat(101), repo: firstRepo }, { name: "customer-web", repo: secondRepo }],
      ];
      nameErrors = [];
      for (const [index, projects] of invalidProjects.entries()) {
        const error = await createThread(management, {
          name: `Invalid Project Names ${index + 1}`,
          layout: "shared",
          worktree_name: "reject-invalid-project-names",
          projects,
        }).then(
          () => "resolved",
          (failure: Error) => failure.message,
        );
        nameErrors.push(error);
      }
      taskSlugError = await createThread(management, {
        name: "Overlong Task Slug",
        layout: "shared",
        worktree_name: "t".repeat(101),
        projects: [
          { name: "admin-web", repo: firstRepo },
          { name: "customer-web", repo: secondRepo },
        ],
      }).then(
        () => "resolved",
        (failure: Error) => failure.message,
      );
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "invalid-name-one", "one\n");
    secondRepo = await createRepo(harness.root, "invalid-name-two", "two\n");
    await sendGroupTask(harness, "invalid-project-names", "reject invalid project names");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    expect(nameErrors).toHaveLength(4);
    for (const error of nameErrors) {
      expect(error).not.toBe("resolved");
      expect(error.length).toBeLessThanOrEqual(200);
    }
    expect(taskSlugError).not.toBe("resolved");
    expect(taskSlugError).toContain("worktree_name");
    expect(taskSlugError).toContain("100");
    expect(await transport.listThreads(GROUP_ID)).toHaveLength(0);
    expect(await pathExists(join(harness.stateDir, "thread-workspaces"))).toBe(false);
    for (const repo of [firstRepo, secondRepo]) {
      const worktrees = await runGit(repo, ["worktree", "list", "--porcelain"]);
      expect(worktrees.split("\n").filter((line) => line.startsWith("worktree "))).toHaveLength(1);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("occupied shared entry names use the full slug plus a suffix without taking the existing directory", async () => {
  const taskSlug = "task".repeat(25);
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create beside an occupied task entry") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Occupied Shared Entry",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [
          { name: "admin-web", repo: firstRepo },
          { name: "customer-web", repo: secondRepo },
        ],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "occupied-entry-one", "one\n");
    secondRepo = await createRepo(harness.root, "occupied-entry-two", "two\n");
    const occupiedEntry = join(harness.stateDir, "thread-workspaces", taskSlug);
    await mkdir(occupiedEntry, { recursive: true });
    await writeFile(join(occupiedEntry, "user-owned.txt"), "keep this directory\n", "utf8");

    await sendGroupTask(harness, "occupied-entry-create", "create beside an occupied task entry");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd) throw new Error("occupied-entry result did not include the allocated entry");
    expect(created.cwd).toBe(`${occupiedEntry}-2`);
    expect(basename(created.cwd)).toBe(`${taskSlug}-2`);
    expect(basename(created.cwd).length).toBe(102);
    expect(await readFile(join(occupiedEntry, "user-owned.txt"), "utf8")).toBe("keep this directory\n");
    expect(await isGitRepository(occupiedEntry)).toBe(false);
    expect(created.ready).toBe(true);
    expect(created.projects.map(({ name, path, branch }) => ({ name, path: basename(path!), branch })))
      .toEqual([
        { name: "admin-web", path: "admin-web", branch: taskSlug },
        { name: "customer-web", path: "customer-web", branch: taskSlug },
      ]);
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.entryPath).toBe(created.cwd);
    expect(persisted?.projects.map(({ name, path, branch }) => ({ name, path, branch })))
      .toEqual(created.projects.map(({ name, path, branch }) => ({ name, path, branch })));
    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    for (const project of created.projects) expect(context?.content).toContain(project.path!);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("thread deletion preserves an external empty directory at a planned shared entry path", async () => {
  const taskSlug = "revoke-unowned-entry-plan";
  let created: SharedOutcome | undefined;
  let occupiedEntry: string | undefined;
  let callbackError: string | undefined;
  let deleteResult: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create and delete after entry collision") return;
    try {
      const management = managementTool(promptContext);
      created = JSON.parse(await createThread(management, {
        name: "Entry Plan Revocation Failure",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [{ name: "admin-web", repo }],
      })) as SharedOutcome;
      deleteResult = await management.execute(
        { action: "thread-delete", short_id: created.short_id },
        toolSignal(),
      );
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  let database: Database | undefined;
  try {
    repo = await createRepo(harness.root, "entry-revocation-source", "origin\n");
    await mkdir(join(harness.stateDir, "thread-workspaces"), { recursive: true });
    database = new Database(join(harness.stateDir, "channel.sqlite"));
    database.exec(`
      CREATE TABLE protected_shared_entry_path (path TEXT PRIMARY KEY);
      CREATE TRIGGER reject_shared_entry_replan
      BEFORE UPDATE OF entry_path ON thread_workspaces
      WHEN EXISTS (
        SELECT 1 FROM protected_shared_entry_path WHERE path = OLD.entry_path
      ) AND NEW.entry_path IS NOT OLD.entry_path
      BEGIN
        SELECT RAISE(ABORT, 'fixture occupied shared entry cannot be replanned');
      END
    `);
    const collisionDatabase = database;
    const candidateStore = Reflect.get(harness.service, "store");
    if (!(candidateStore instanceof ChannelStore)) throw new Error("harness store was not available");
    const saveThreadWorkspace = candidateStore.saveThreadWorkspace.bind(candidateStore);
    candidateStore.saveThreadWorkspace = (record) => {
      saveThreadWorkspace(record);
      if (record.entryPath === null || occupiedEntry !== undefined) return;
      occupiedEntry = record.entryPath;
      collisionDatabase.query(
        "INSERT INTO protected_shared_entry_path (path) VALUES (?)",
      ).run(occupiedEntry);
      mkdirSync(occupiedEntry);
    };

    await sendGroupTask(harness, "entry-revocation-create", "create and delete after entry collision");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created) throw new Error("thread workspace result was not captured");
    expect(deleteResult).toContain("已删除线程");
    if (occupiedEntry === undefined) throw new Error("planned shared entry was not intercepted");
    expect(await pathExists(occupiedEntry)).toBe(true);
    expect(await realpath(occupiedEntry)).toBe(occupiedEntry);
    expect(await readdir(occupiedEntry)).toEqual([]);
    const worktreeList = await runGit(repo, ["worktree", "list", "--porcelain"]);
    expect(worktreeList.split("\n").filter((line) => line.startsWith("worktree ")))
      .toEqual([`worktree ${repo}`]);
    expect(await transport.listThreads(GROUP_ID)).toHaveLength(0);
  } finally {
    database?.close();
    await closeCoreHarness(harness);
  }
});

test("preparing and failed persisted entries reserve their names even when directories are absent", async () => {
  const taskSlug = "reserve-unfinished-workspace-entry";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create after persisted entry reservations") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Use Next Unreserved Entry",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [{ name: "admin-web", repo }],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "reserved-entry-source", "origin\n");
    const entryParent = join(harness.stateDir, "thread-workspaces");
    const reserved = [
      { channelId: `${GROUP_ID}____preparing-reservation`, status: "preparing" as const, suffix: "" },
      { channelId: `${GROUP_ID}____failed-reservation`, status: "failed" as const, suffix: "-2" },
    ];
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      for (const reservation of reserved) {
        store.saveThreadWorkspace({
          botUid: harness.bot.uid,
          channelType: 5,
          channelId: reservation.channelId,
          layout: "shared",
          status: reservation.status,
          entryPath: join(entryParent, `${taskSlug}${reservation.suffix}`),
          projects: [{ name: "admin-web", repo, path: null, branch: null, created: false }],
          error: reservation.status === "failed" ? "previous allocation failed" : null,
        });
      }
    } finally {
      store.close();
    }

    await sendGroupTask(harness, "reserved-entry-create", "create after persisted entry reservations");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd) throw new Error("reserved-entry result did not include its actual entry");
    expect(created.cwd).toBe(join(entryParent, `${taskSlug}-3`));
    expect(created.ready).toBe(true);
    for (const reservation of reserved) {
      const reservedPath = join(entryParent, `${taskSlug}${reservation.suffix}`);
      expect(await pathExists(reservedPath)).toBe(false);
      const persisted = await storedThreadWorkspace(harness, reservation.channelId);
      expect(persisted?.entryPath).toBe(reservedPath);
      expect(persisted?.status).toBe(reservation.status);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("concurrent shared creations reserve distinct readable entries and project paths", async () => {
  const taskSlug = "concurrent-search-index-fix";
  let created: SharedOutcome[] = [];
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create two shared workspaces concurrently") return;
    try {
      const management = managementTool(promptContext);
      created = await Promise.all([0, 1].map(async (index) =>
        JSON.parse(await createThread(management, {
          name: `Concurrent Shared Workspace ${index + 1}`,
          layout: "shared",
          worktree_name: taskSlug,
          projects: [{ name: "search-api", repo }],
        })) as SharedOutcome
      ));
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "concurrent-source", "origin\n");
    await sendGroupTask(harness, "concurrent-entry-create", "create two shared workspaces concurrently");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    expect(created).toHaveLength(2);
    expect(created.every(({ ready }) => ready)).toBe(true);
    const entries = created.map(({ cwd }) => {
      if (!cwd) throw new Error("concurrent shared result omitted its entry");
      return basename(cwd);
    }).sort();
    expect(entries).toEqual([taskSlug, `${taskSlug}-2`]);
    const branches = created.map(({ projects }) => projects[0]?.branch).sort();
    expect(branches).toEqual([taskSlug, `${taskSlug}-2`]);
    expect(new Set(created.map(({ cwd }) => cwd)).size).toBe(2);
    for (const result of created) {
      if (!result.cwd || !result.projects[0]?.path) throw new Error("concurrent result omitted its project path");
      expect(result.projects[0].path).toBe(join(result.cwd, "search-api"));
      expect(await realpath(result.projects[0].path)).toBe(result.projects[0].path);
      expect(await runGit(result.projects[0].path, ["rev-parse", "--show-toplevel"]))
        .toBe(result.projects[0].path);
      const persisted = await storedThreadWorkspace(harness, result.channel_id);
      expect(persisted?.entryPath).toBe(result.cwd);
      expect(persisted?.projects[0]?.path).toBe(result.projects[0].path);
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an occupied shared project path is preserved while ownership follows the suffixed actual path", async () => {
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  let occupiedPath: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create beside an occupied project path") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Project Path Collision",
        layout: "shared",
        worktree_name: "resolve-project-path-collision",
        projects: [{ name: "Admin Web", repo }],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  }, {
    instrument: (port) => ({
      create: async (request, signal) => {
        if (request.path !== undefined && occupiedPath === undefined) {
          occupiedPath = request.path;
          await mkdir(request.path);
          await writeFile(join(request.path, "user-owned.txt"), "keep project path\n", "utf8");
        }
        return port.create(request, signal);
      },
      merge: (request, signal) => port.merge(request, signal),
      remove: (request, signal) => port.remove(request, signal),
      worktrunk: (signal) => port.worktrunk(signal),
    }),
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "project-path-source", "origin\n");
    await sendGroupTask(harness, "occupied-project-path-create", "create beside an occupied project path");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd || !created.projects[0]?.path || !occupiedPath) {
      throw new Error("project collision result omitted actual workspace paths");
    }
    const actualPath = join(created.cwd, "admin-web-2");
    expect(occupiedPath).toBe(join(created.cwd, "admin-web"));
    expect(created.projects[0].path).toBe(actualPath);
    expect(created.projects[0].branch).toBe("resolve-project-path-collision");
    expect(await readFile(join(occupiedPath, "user-owned.txt"), "utf8")).toBe("keep project path\n");
    expect(await realpath(actualPath)).toBe(actualPath);
    expect(await runGit(actualPath, ["rev-parse", "--show-toplevel"])).toBe(actualPath);
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.projects[0]?.path).toBe(actualPath);
    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(`Admin Web: ${actualPath}`);
    expect(context?.content?.split("\n")).not.toContain(`- Admin Web: ${occupiedPath}`);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("shared entry allocation skips occupied files and symlinks without replacing them", async () => {
  const cases = [
    { taskSlug: "entry-occupied-by-file", kind: "file" },
    { taskSlug: "entry-occupied-by-symlink", kind: "symlink" },
  ] as const;
  let outcomes: SharedOutcome[] = [];
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "allocate after file and symlink collisions") return;
    try {
      const management = managementTool(promptContext);
      outcomes = [];
      for (const entry of cases) {
        outcomes.push(JSON.parse(await createThread(management, {
          name: `Occupied ${entry.kind} Entry`,
          layout: "shared",
          worktree_name: entry.taskSlug,
          projects: [{ name: "worker-app", repo }],
        })) as SharedOutcome);
      }
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "occupied-non-directory-source", "origin\n");
    const entryParent = join(harness.stateDir, "thread-workspaces");
    const symlinkTarget = join(harness.root, "symlink-target");
    await mkdir(entryParent, { recursive: true });
    await mkdir(symlinkTarget);
    for (const entry of cases) {
      const occupiedPath = join(entryParent, entry.taskSlug);
      if (entry.kind === "file") {
        await writeFile(occupiedPath, "user-owned file\n", "utf8");
      } else {
        await symlink(symlinkTarget, occupiedPath);
      }
    }

    await sendGroupTask(harness, "occupied-non-directory-create", "allocate after file and symlink collisions");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    expect(outcomes).toHaveLength(2);
    for (const [index, entry] of cases.entries()) {
      const result = outcomes[index];
      if (!result?.cwd) throw new Error(`occupied ${entry.kind} result omitted its entry`);
      const occupiedPath = join(entryParent, entry.taskSlug);
      expect(result.cwd).toBe(`${occupiedPath}-2`);
      expect(result.ready).toBe(true);
      if (entry.kind === "file") {
        expect(await readFile(occupiedPath, "utf8")).toBe("user-owned file\n");
      } else {
        expect(await realpath(occupiedPath)).toBe(symlinkTarget);
      }
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a 100-character project slug is retained in its shared directory name", async () => {
  const projectName = "P".repeat(100);
  const projectSlug = "p".repeat(100);
  const taskSlug = "shared-project-slug-boundary";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create the maximum-length project slug") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Maximum Project Slug",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [{ name: projectName, repo }],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let repo = "";
  try {
    repo = await createRepo(harness.root, "maximum-project-slug-source", "origin\n");
    await sendGroupTask(harness, "maximum-project-slug-create", "create the maximum-length project slug");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd || !created.projects[0]?.path) {
      throw new Error("maximum project slug result omitted its workspace path");
    }
    expect(created.ready).toBe(true);
    expect(created.projects[0].name).toBe(projectName);
    expect(created.projects[0].path).toBe(join(created.cwd, projectSlug));
    expect(basename(created.projects[0].path)).toHaveLength(100);
    expect(await runGit(created.projects[0].path, ["rev-parse", "--show-toplevel"]))
      .toBe(created.projects[0].path);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a branch collision changes only the branch, not a shared project's stable directory", async () => {
  const taskSlug = "repair-webhook-authentication";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  const parentPrompt = promptCompletionLatch();
  const { harness, transport } = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create shared project after branch collision") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Shared Branch Collision",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [
          { name: "Admin Web", repo: firstRepo },
          { name: "Customer Web", repo: secondRepo },
        ],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  });
  let firstRepo = "";
  let secondRepo = "";
  try {
    firstRepo = await createRepo(harness.root, "branch-collision-one", "one\n");
    secondRepo = await createRepo(harness.root, "branch-collision-two", "two\n");
    await runGit(firstRepo, ["branch", taskSlug]);
    await sendGroupTask(harness, "branch-collision-create", "create shared project after branch collision");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd) throw new Error("branch collision result omitted its shared entry");
    const first = created.projects.find(({ name }) => name === "Admin Web");
    const second = created.projects.find(({ name }) => name === "Customer Web");
    expect(first).toMatchObject({
      path: join(created.cwd, "admin-web"),
      branch: `${taskSlug}-2`,
      created: true,
    });
    expect(second).toMatchObject({
      path: join(created.cwd, "customer-web"),
      branch: taskSlug,
      created: true,
    });
    const persisted = await storedThreadWorkspace(harness, created.channel_id);
    expect(persisted?.projects.map(({ name, path, branch }) => ({ name, path, branch })))
      .toEqual(created.projects.map(({ name, path, branch }) => ({ name, path, branch })));
    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(`Admin Web: ${first?.path}`);
    expect(context?.content).toContain(`Customer Web: ${second?.path}`);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the actual shared project candidate is persisted before Git creates its worktree", async () => {
  const taskSlug = "persist-worktree-plan-before-create";
  let created: SharedOutcome | undefined;
  let callbackError: string | undefined;
  let planned: { entryPath: string | null; path: string | null; branch: string | null; created: boolean } | undefined;
  const parentPrompt = promptCompletionLatch();
  let harness!: CoreHarness;
  const setup = await createWorkspaceHarness(async ({ promptContext, text }) => {
    if (text !== "create a persistently planned worktree") return;
    try {
      created = JSON.parse(await createThread(managementTool(promptContext), {
        name: "Persist Plan Before Git",
        layout: "shared",
        worktree_name: taskSlug,
        projects: [{ name: "admin-web", repo }],
      })) as SharedOutcome;
    } catch (error) {
      callbackError = error instanceof Error ? error.message : String(error);
    } finally {
      parentPrompt.complete();
    }
  }, {
    instrument: (port) => ({
      create: async (request, signal) => {
        const originalOnPlanned = request.onPlanned;
        return port.create({
          ...request,
          onPlanned: (candidate) => {
            originalOnPlanned?.(candidate);
            const database = new Database(join(harness.stateDir, "channel.sqlite"));
            try {
              const row = database.query(
                "SELECT entry_path, projects FROM thread_workspaces WHERE bot_uid = ? AND channel_id = ?",
              ).get(harness.bot.uid, `${GROUP_ID}____${request.threadId}`) as
                | { entry_path: string | null; projects: string }
                | null;
              if (!row) throw new Error("planned worktree ownership was not persisted");
              const [project] = JSON.parse(row.projects) as Array<{
                path: string | null;
                branch: string | null;
                created: boolean;
              }>;
              planned = {
                entryPath: row.entry_path,
                path: project?.path ?? null,
                branch: project?.branch ?? null,
                created: project?.created ?? true,
              };
            } finally {
              database.close();
            }
          },
        }, signal);
      },
      merge: (request, signal) => port.merge(request, signal),
      remove: (request, signal) => port.remove(request, signal),
      worktrunk: (signal) => port.worktrunk(signal),
    }),
  });
  harness = setup.harness;
  let repo = "";
  try {
    repo = await createRepo(harness.root, "planned-worktree-source", "origin\n");
    await sendGroupTask(harness, "planned-worktree-create", "create a persistently planned worktree");
    await finishGroupTask(harness, parentPrompt.completion);

    expect(callbackError).toBeUndefined();
    if (!created?.cwd || !created.projects[0]?.path) {
      throw new Error("planned worktree result omitted its actual paths");
    }
    expect(planned).toEqual({
      entryPath: created.cwd,
      path: created.projects[0].path,
      branch: created.projects[0].branch,
      created: false,
    });
    expect(created.projects[0].path).toBe(join(created.cwd, "admin-web"));
    expect(created.ready).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("restart keeps historical UUID entry and numbered project paths unchanged", async () => {
  let repo = "";
  let restarted: CoreHarness | undefined;
  let firstHarnessClosed = false;
  const { harness, transport } = await createWorkspaceHarness(async () => {});
  const legacyEntry = join(
    harness.stateDir,
    "thread-workspaces",
    "d9428888-122b-4f81-9a2c-83b3e9f2da50",
  );
  const legacyProjectPath = join(legacyEntry, "project-1-admin-web");
  const legacyBranch = "legacy-task-1-admin-web";
  try {
    repo = await createRepo(harness.root, "legacy-history-source", "legacy origin\n");
    const worktrees = harness.worktreePort;
    if (!worktrees) throw new Error("historical fixture requires a real WorktreePort");
    await mkdir(legacyEntry, { recursive: true });
    const existing = await worktrees.create({
      baseDir: repo,
      label: legacyBranch,
      threadId: "legacy-thread",
      path: legacyProjectPath,
    });
    const thread = await transport.createThread(GROUP_ID, "Historical Workspace", toolSignal());
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        channelType: 5,
        channelId: thread.channelId,
        layout: "shared",
        status: "ready",
        entryPath: legacyEntry,
        projects: [{
          name: "Admin Web",
          repo,
          path: existing.cwd,
          branch: existing.branch,
          created: true,
        }],
        error: null,
      });
      store.ensureBinding({
        botUid: harness.bot.uid,
        channelType: 5,
        channelId: thread.channelId,
        sessionId: "legacy-workspace-session",
        cwd: legacyEntry,
        sessionFile: null,
        cwdPinned: true,
      });
    } finally {
      store.close();
    }
    await transport.updateGroupContext(
      { channelId: thread.channelId, channelType: 5 },
      `用途：历史工作区\n- Admin Web: ${legacyProjectPath}`,
    );

    await closeCoreHarness(harness, { remove: false });
    firstHarnessClosed = true;
    restarted = await createCoreHarness({
      root: harness.root,
      bot: harness.bot,
      transport,
      allowedGroupIds: [GROUP_ID],
      prompt: async () => {},
      worktreePort: worktrees,
    });

    expect(await pathExists(legacyEntry)).toBe(true);
    expect(await pathExists(legacyProjectPath)).toBe(true);
    expect(await realpath(legacyProjectPath)).toBe(legacyProjectPath);
    expect(await readFile(join(legacyProjectPath, "origin.txt"), "utf8")).toBe("legacy origin\n");
    expect(await runGit(legacyProjectPath, ["branch", "--show-current"])).toBe(legacyBranch);
    const persisted = await storedThreadWorkspace(restarted, thread.channelId);
    expect(persisted).toMatchObject({
      layout: "shared",
      status: "ready",
      entryPath: legacyEntry,
      projects: [{
        name: "Admin Web",
        repo,
        path: legacyProjectPath,
        branch: legacyBranch,
        created: true,
      }],
    });
    const context = await transport.getGroupContext({ channelId: thread.channelId, channelType: 5 });
    expect(context?.content).toContain(legacyProjectPath);
  } finally {
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (!firstHarnessClosed) await closeCoreHarness(harness, { remove: false });
    await rm(harness.root, { recursive: true, force: true });
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
        worktree_name: "keep-partial-project-workspace",
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
    const failedProject = created.projects[1];
    if (!created.cwd || !failedProject?.path) {
      throw new Error("failed project did not retain its planned shared path");
    }
    expect(failedProject).toMatchObject({
      name: "project-two",
      repo: secondRepo,
      branch: null,
      created: false,
    });
    expect(failedProject.path).toBe(join(created.cwd, "project-two"));
    expect(await pathExists(failedProject.path)).toBe(false);
    expect(created.projects[1]?.error).toContain("fixture second project worktree creation failed");
    expect(typeof created.projects[1]?.error).toBe("string");
    expect(created.projects[1]?.error?.length).toBeGreaterThan(0);
    expect(typeof dispatchError).toBe("string");
    expect(dispatchError?.length).toBeGreaterThan(0);
    expect(harness.workers.prompts).toHaveLength(1);
    expect(await isGitRepository(created.projects[0]!.path!)).toBe(true);

    const context = await transport.getGroupContext({ channelId: created.channel_id, channelType: 5 });
    expect(context?.content).toContain(created.projects[0]!.path!);
    expect(context?.content).not.toContain(failedProject.name);
    expect(context?.content).not.toContain(failedProject.path);
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
        worktree_name: "publish-project-paths-only",
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
          worktree_name: "reject-shared-merge",
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
        worktree_name: "protect-dirty-project-worktree",
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
        worktree_name: "persist-project-ownership-first",
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
    expect(created.projects.every((project) => !project.created)).toBe(true);
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
            worktree_name: "reject-state-path-inside-repository",
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
