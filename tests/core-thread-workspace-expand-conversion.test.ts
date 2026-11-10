import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import type { ChatTarget, ThreadWorkspace, ThreadWorkspaceRequest } from "../src/contracts.ts";
import { ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorktrees,
  inbound,
  type CoreHarness,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-thread-workspace-expand";
const GROUP_TARGET = { channelId: GROUP_ID, channelType: 2 as const };
const DIRTY_CONTENT = "uncommitted legacy worktree content\n";
const PRIME_TEXT = "establish the original session";

function fixtureBot(name: string) {
  return { uid: `bot-thread-workspace-expand-${randomUUID()}`, name };
}

type WorkspaceProject = {
  name: string;
  repo: string;
  path: string | null;
  branch: string | null;
  created: boolean;
};

type WorkspaceView = {
  layout: "single" | "shared";
  status: "ready" | "failed" | "preparing";
  ready: boolean;
  cwd: string | null;
  projects: WorkspaceProject[];
};

type WorkspaceOperationView = {
  status: string;
  phase: string;
  projects: WorkspaceProject[];
  error: string | null;
};

type ExpansionReceipt = { accepted: true; status: "queued" };

type HeldPrompt = {
  started: Promise<void>;
  actionFinished: Promise<void>;
  release(): void;
  run(action: () => Promise<void>): Promise<void>;
};

function createHeldPrompt() {
  const started = Promise.withResolvers<void>();
  const actionFinished = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  return {
    started: started.promise,
    actionFinished: actionFinished.promise,
    release: () => release.resolve(),
    async run(action: () => Promise<void>): Promise<void> {
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

async function waitForHeldPrompt(
  harness: CoreHarness,
  target: ChatTarget,
  text: string,
  prompt: HeldPrompt,
): Promise<void> {
  await prompt.started;
  expect(harness.workers.prompts.filter((record) => record.text === text)).toHaveLength(1);
  await prompt.actionFinished;
  const running = await waitForStatus(harness.service, target, (status) => status.state === "running");
  expect(running.state).toBe("running");
}

async function finishHeldPrompt(
  harness: CoreHarness,
  target: ChatTarget,
  prompt: HeldPrompt,
) {
  prompt.release();
  return await waitForStatus(
    harness.service,
    target,
    (status) => status.state === "completed" || status.state === "failed",
  );
}

class GitWorktreeFixturePort extends FakeWorktrees {
  createCalls = 0;
  readonly createdPaths: string[] = [];
  unknownAfterCreate = false;

  constructor(private readonly fixtureRoot: string) {
    super(fixtureRoot);
  }

  override async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
    this.createCalls += 1;
    const { onPlanned, ...recordedRequest } = request;
    this.requests.push({ ...recordedRequest });
    if (this.failure) throw this.failure;

    const repo = await realpath(request.baseDir);
    const cwd = request.path ?? join(this.fixtureRoot, "worktrees", request.threadId);
    const branch = request.label.toLowerCase()
      .replace(/[^a-z0-9]+/gu, "-")
      .replace(/^-+|-+$/gu, "");
    onPlanned?.({ cwd, branch, repo });
    await mkdir(dirname(cwd), { recursive: true });
    request.lifecycle?.beforeSpawn(repo);
    await git(repo, "worktree", "add", "-q", "-b", branch, cwd, request.base ?? "HEAD");
    const canonicalCwd = await realpath(cwd);
    this.createdPaths.push(canonicalCwd);

    if (this.unknownAfterCreate) {
      request.lifecycle?.onUnknown();
      throw new Error("fixture create result became unknown after creating a linked worktree");
    }

    request.lifecycle?.onConfirmedExit();
    return { cwd: canonicalCwd, branch };
  }
}

async function git(cwd: string, ...args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", cwd, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr.trim()}`);
  return stdout.trim();
}

async function createRepo(root: string, name: string): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo, { recursive: true });
  await writeFile(join(repo, "origin.txt"), `${name} origin\n`, "utf8");
  await git(repo, "init", "-q", "-b", "main");
  await git(repo, "add", ".");
  await git(
    repo,
    "-c",
    "user.email=fixture@example.com",
    "-c",
    "user.name=Fixture",
    "commit",
    "-q",
    "-m",
    "fixture source",
  );
  return realpath(repo);
}
async function waitForWorkspaceOutcome(
  root: string,
  botUid: string,
  target: { channelId: string; channelType: 5 },
  expectedStatus: "ready" | "failed" | "preparing" | "non-ready",
  sessionId: string,
  originalCwd: string,
): Promise<void> {
  const deadline = performance.now() + 10_000;
  const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
  try {
    while (performance.now() < deadline) {
      const workspace = store.getThreadWorkspace(botUid, target);
      const binding = store.getBinding(botUid, target);
      const operation = expectedStatus === "non-ready"
        ? store.getLatestThreadWorkspaceOperation(botUid, target)
        : null;
      const workspaceIsExpected = expectedStatus === "non-ready"
        ? operation?.status === "unknown"
          && operation.candidateWorkspace.layout === "shared"
          && operation.candidateWorkspace.status !== "ready"
        : workspace?.layout === "shared" && workspace.status === expectedStatus;
      if (
        workspaceIsExpected
        && binding?.sessionId === sessionId
        && (
          expectedStatus === "ready"
            ? workspace !== null && workspace.entryPath !== null && binding.cwd === workspace.entryPath
            : binding.cwd === originalCwd
        )
      ) {
        return;
      }
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    throw new Error(`thread workspace expansion did not reach ${expectedStatus}: ${JSON.stringify({
      workspace: store.getThreadWorkspace(botUid, target),
      binding: store.getBinding(botUid, target),
      operation: store.getLatestThreadWorkspaceOperation(botUid, target),
    })}`);
  } finally {
    store.close();
  }
}

async function sendThreadTask(
  harness: CoreHarness,
  target: { channelId: string; channelType: 5 },
  messageId: string,
  text: string,
): Promise<void> {
  await harness.service.submitInbound(inbound({
    ...target,
    messageId,
    senderId: "owner-1",
    text,
    mentionedUserIds: [harness.bot.uid],
  }));
}

async function createExistingSingleProjectFixture(harness: CoreHarness) {
  const sourceRepo = await createRepo(harness.root, "legacy-source");
  const additionalRepo = await createRepo(harness.root, "added-source");
  const legacyWorktree = join(harness.root, "legacy-project-worktree");
  const legacyBranch = "legacy-existing-branch";
  await git(sourceRepo, "worktree", "add", "-q", "-b", legacyBranch, legacyWorktree);
  const canonicalLegacyWorktree = await realpath(legacyWorktree);
  await writeFile(join(canonicalLegacyWorktree, "dirty.txt"), DIRTY_CONTENT, "utf8");

  const thread = await harness.transport.createThread(GROUP_ID, "Existing single-project thread");
  const threadTarget = { channelId: thread.channelId, channelType: 5 as const };
  const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...GROUP_TARGET,
      sessionId: "group-session",
      cwd: sourceRepo,
      sessionFile: null,
      cwdPinned: true,
    });
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...threadTarget,
      sessionId: "original-thread-session-id",
      cwd: canonicalLegacyWorktree,
      sessionFile: null,
      cwdPinned: true,
    });
    store.saveThreadWorkspace({
      botUid: harness.bot.uid,
      ...threadTarget,
      layout: "single",
      status: "ready",
      workspaceRoot: null,
      entryPath: null,
      projects: [{
        name: "legacy-project",
        repo: sourceRepo,
        path: canonicalLegacyWorktree,
        branch: legacyBranch,
        created: true,
      }],
      error: null,
    });
  } finally {
    store.close();
  }
  return { sourceRepo, additionalRepo, legacyWorktree: canonicalLegacyWorktree, legacyBranch, threadTarget };
}

async function primeOriginalThreadSession(
  harness: CoreHarness,
  target: { channelId: string; channelType: 5 },
  prompt: HeldPrompt,
): Promise<string> {
  await sendThreadTask(harness, target, "prime-original-thread-session", PRIME_TEXT);
  await waitForHeldPrompt(harness, target, PRIME_TEXT, prompt);
  const status = await finishHeldPrompt(harness, target, prompt);
  if (status.state !== "completed" || !status.sessionId) {
    throw new Error("initial thread task did not complete with a session");
  }
  return status.sessionId;
}

test("adding a project converts a single thread to shared without moving or replacing its worktree session", async () => {
  let additionalRepo = "";
  let expansionReceipt: ExpansionReceipt | undefined;
  let operationError: string | undefined;
  let queriedWorkspace: WorkspaceView | undefined;
  let queryError: string | undefined;
  const primePrompt = createHeldPrompt();
  const expansionPrompt = createHeldPrompt();
  const queryPrompt = createHeldPrompt();
  const root = await mkdtemp(join(tmpdir(), "omp-thread-workspace-expand-success-"));
  const bot = fixtureBot("Workspace Expansion Fixture");
  const transport = new FakeTransport({ bot });
  const worktrees = new GitWorktreeFixturePort(root);
  const harness = await createCoreHarness({
    root,
    bot,
    transport,
    worktreePort: worktrees,
    allowedGroupIds: [GROUP_ID],
    prompt: async ({ text, promptContext }) => {
      if (
        text !== PRIME_TEXT
        && text !== "expand the existing thread workspace"
        && text !== "query expanded thread workspace"
      ) return;
      const prompt = text === PRIME_TEXT
        ? primePrompt
        : text === "expand the existing thread workspace" ? expansionPrompt : queryPrompt;
      await prompt.run(async () => {
        if (text === PRIME_TEXT) return;
        const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
        if (!management) throw new Error("octo_management host tool missing");
        const signal = new AbortController().signal;
        if (text === "expand the existing thread workspace") {
          try {
            expansionReceipt = JSON.parse(await management.execute({
              action: "thread-workspace-add",
              worktree_name: "expand-fixture",
              projects: [{ name: "added-project", repo: additionalRepo }],
            }, signal)) as ExpansionReceipt;
          } catch (error) {
            operationError = error instanceof Error ? error.message : String(error);
          }
          return;
        }
        try {
          const info = JSON.parse(await management.execute({ action: "thread-info" }, signal)) as {
            workspace?: WorkspaceView;
          };
          queriedWorkspace = info.workspace;
        } catch (error) {
          queryError = error instanceof Error ? error.message : String(error);
        }
      });
    },
  });

  try {
    const fixture = await createExistingSingleProjectFixture(harness);
    additionalRepo = fixture.additionalRepo;
    const {
      sourceRepo,
      legacyWorktree: canonicalLegacyWorktree,
      legacyBranch,
      threadTarget,
    } = fixture;
    const originalSessionId = await primeOriginalThreadSession(harness, threadTarget, primePrompt);

    await sendThreadTask(
      harness,
      threadTarget,
      "expand-existing-workspace",
      "expand the existing thread workspace",
    );
    await waitForHeldPrompt(
      harness,
      threadTarget,
      "expand the existing thread workspace",
      expansionPrompt,
    );
    expect(operationError).toBeUndefined();
    expect(expansionReceipt).toMatchObject({ accepted: true, status: "queued" });
    const expansionStatus = await finishHeldPrompt(harness, threadTarget, expansionPrompt);
    expect(expansionStatus.state).toBe("completed");

    await waitForWorkspaceOutcome(
      harness.root,
      harness.bot.uid,
      threadTarget,
      "ready",
      originalSessionId,
      canonicalLegacyWorktree,
    );
    await sendThreadTask(harness, threadTarget, "query-expanded-workspace", "query expanded thread workspace");
    await waitForHeldPrompt(
      harness,
      threadTarget,
      "query expanded thread workspace",
      queryPrompt,
    );
    const queryStatus = await finishHeldPrompt(harness, threadTarget, queryPrompt);
    expect(queryStatus.state).toBe("completed");
    expect(queryError).toBeUndefined();
    expect(queriedWorkspace).toMatchObject({ layout: "shared", status: "ready", ready: true });

    const workspace = queriedWorkspace;
    if (!workspace?.cwd) throw new Error("thread-info omitted the expanded workspace entry");
    const workspaceCwd = workspace.cwd;
    expect(workspace.projects).toHaveLength(2);
    const addedProject = workspace.projects.find(({ name }) => name === "added-project");
    expect(addedProject).toMatchObject({
      repo: additionalRepo,
      created: true,
    });
    if (!addedProject?.path || !addedProject.branch) {
      throw new Error("expanded workspace omitted the added project's Git worktree ownership");
    }
    expect(await git(addedProject.path, "rev-parse", "--show-toplevel")).toBe(addedProject.path);
    expect(await git(addedProject.path, "branch", "--show-current")).toBe(addedProject.branch);
    const registeredWorktrees = (await git(additionalRepo, "worktree", "list", "--porcelain")).split("\n");
    expect(registeredWorktrees).toContain(`worktree ${addedProject.path}`);
    expect(registeredWorktrees).toContain(`branch refs/heads/${addedProject.branch}`);
    expect(workspaceCwd).not.toBe(canonicalLegacyWorktree);
    const legacyProject = workspace.projects.find(({ name }) => name === "legacy-project");
    expect(legacyProject).toMatchObject({
      repo: sourceRepo,
      path: canonicalLegacyWorktree,
      branch: legacyBranch,
      created: true,
    });
    expect(await git(canonicalLegacyWorktree, "branch", "--show-current")).toBe(legacyBranch);
    expect(await readFile(join(canonicalLegacyWorktree, "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);

    const entryAlias = join(workspaceCwd, "legacy-project");
    expect(await realpath(entryAlias)).toBe(canonicalLegacyWorktree);
    expect(await readFile(join(entryAlias, "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);
    expect(await realpath(workspaceCwd)).toBe(workspaceCwd);

    const migratedStatus = await harness.service.status(threadTarget);
    expect(migratedStatus).toMatchObject({
      cwd: workspaceCwd,
      sessionId: originalSessionId,
    });
  } finally {
    primePrompt.release();
    expansionPrompt.release();
    queryPrompt.release();
    await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("a failed expansion stays queryable and preserves the original worktree and session", async () => {
  let additionalRepo = "";
  let expansionReceipt: ExpansionReceipt | undefined;
  let operationError: string | undefined;
  let queriedWorkspace: WorkspaceView | undefined;
  let queryError: string | undefined;
  const primePrompt = createHeldPrompt();
  const expansionPrompt = createHeldPrompt();
  const queryPrompt = createHeldPrompt();
  const harness = await createCoreHarness({
    bot: fixtureBot("Workspace Expansion Failure Fixture"),
    allowedGroupIds: [GROUP_ID],
    prompt: async ({ text, promptContext }) => {
      if (
        text !== PRIME_TEXT
        && text !== "fail the existing thread workspace"
        && text !== "query failed thread workspace"
      ) return;
      const prompt = text === PRIME_TEXT
        ? primePrompt
        : text === "fail the existing thread workspace" ? expansionPrompt : queryPrompt;
      await prompt.run(async () => {
        if (text === PRIME_TEXT) return;
        const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
        if (!management) throw new Error("octo_management host tool missing");
        const signal = new AbortController().signal;
        if (text === "fail the existing thread workspace") {
          try {
            expansionReceipt = JSON.parse(await management.execute({
              action: "thread-workspace-add",
              worktree_name: "expand-fixture",
              projects: [{ name: "added-project", repo: additionalRepo }],
            }, signal)) as ExpansionReceipt;
          } catch (error) {
            operationError = error instanceof Error ? error.message : String(error);
          }
          return;
        }
        try {
          const info = JSON.parse(await management.execute({
            action: "thread-info",
            short_id: "thread-1",
          }, signal)) as {
            workspace?: WorkspaceView;
          };
          queriedWorkspace = info.workspace;
        } catch (error) {
          queryError = error instanceof Error ? error.message : String(error);
        }
      });
    },
  });

  try {
    const fixture = await createExistingSingleProjectFixture(harness);
    additionalRepo = fixture.additionalRepo;
    const { sourceRepo, legacyWorktree, legacyBranch, threadTarget } = fixture;
    const worktrees = harness.worktrees;
    if (!worktrees) throw new Error("expected the isolated fake WorktreePort");
    worktrees.failure = new Error("fixture worktree creation failed");
    const originalSessionId = await primeOriginalThreadSession(harness, threadTarget, primePrompt);

    await sendThreadTask(
      harness,
      threadTarget,
      "fail-existing-workspace-expansion",
      "fail the existing thread workspace",
    );
    await waitForHeldPrompt(
      harness,
      threadTarget,
      "fail the existing thread workspace",
      expansionPrompt,
    );
    expect(operationError).toBeUndefined();
    expect(expansionReceipt).toMatchObject({ accepted: true, status: "queued" });
    const expansionStatus = await finishHeldPrompt(harness, threadTarget, expansionPrompt);
    expect(expansionStatus.state).toBe("completed");

    await waitForWorkspaceOutcome(
      harness.root,
      harness.bot.uid,
      threadTarget,
      "failed",
      originalSessionId,
      legacyWorktree,
    );
    await harness.service.submitInbound(inbound({
      ...GROUP_TARGET,
      messageId: "query-failed-workspace",
      senderId: "owner-1",
      text: "query failed thread workspace",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForHeldPrompt(harness, GROUP_TARGET, "query failed thread workspace", queryPrompt);
    const queryStatus = await finishHeldPrompt(harness, GROUP_TARGET, queryPrompt);
    expect(queryStatus.state).toBe("completed");
    expect(queryError).toBeUndefined();
    expect(queriedWorkspace).toMatchObject({ layout: "shared", status: "failed", ready: false });

    const workspace = queriedWorkspace;
    if (!workspace) throw new Error("thread-info omitted the failed workspace record");
    expect(workspace.projects.find(({ name }) => name === "legacy-project")).toMatchObject({
      repo: sourceRepo,
      path: legacyWorktree,
      branch: legacyBranch,
      created: true,
    });
    expect(await git(legacyWorktree, "branch", "--show-current")).toBe(legacyBranch);
    expect(await readFile(join(legacyWorktree, "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);
    expect(await harness.service.status(threadTarget)).toMatchObject({
      cwd: legacyWorktree,
      sessionId: originalSessionId,
    });
  } finally {
    primePrompt.release();
    expansionPrompt.release();
    queryPrompt.release();
    await closeCoreHarness(harness);
  }
}, { timeout: 30_000 });

test("an unknown add-project result is not replayed or reported ready after service restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-thread-workspace-expand-restart-"));
  const bot = fixtureBot("Workspace Expansion Restart Fixture");
  const transport = new FakeTransport({ bot });
  const worktrees = new GitWorktreeFixturePort(root);
  worktrees.unknownAfterCreate = true;
  let additionalRepo = "";
  let expansionReceipt: ExpansionReceipt | undefined;
  let operationError: string | undefined;
  let queriedWorkspace: WorkspaceView | undefined;
  let queryError: string | undefined;
  let queriedWorkspaceOperation: WorkspaceOperationView | undefined;
  const primePrompt = createHeldPrompt();
  const expansionPrompt = createHeldPrompt();
  const queryPrompt = createHeldPrompt();
  const prompt: FakePrompt = async ({ text, promptContext }) => {
    if (
      text !== PRIME_TEXT
      && text !== "accept expanded workspace"
      && text !== "query workspace after restart"
    ) return;
    const promptGate = text === PRIME_TEXT
      ? primePrompt
      : text === "accept expanded workspace" ? expansionPrompt : queryPrompt;
    await promptGate.run(async () => {
      if (text === PRIME_TEXT) return;
      const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
      if (!management) throw new Error("octo_management host tool missing");
      const signal = new AbortController().signal;
      if (text === "accept expanded workspace") {
        try {
          expansionReceipt = JSON.parse(await management.execute({
            action: "thread-workspace-add",
            worktree_name: "expand-fixture",
            projects: [{ name: "added-project", repo: additionalRepo }],
          }, signal)) as ExpansionReceipt;
        } catch (error) {
          operationError = error instanceof Error ? error.message : String(error);
        }
        return;
      }
      try {
        const info = JSON.parse(await management.execute({
          action: "thread-info",
          short_id: "thread-1",
        }, signal)) as {
          workspace?: WorkspaceView;
          workspace_operation?: WorkspaceOperationView;
        };
        queriedWorkspace = info.workspace;
        queriedWorkspaceOperation = info.workspace_operation;
      } catch (error) {
        queryError = error instanceof Error ? error.message : String(error);
      }
    });
  };

  let first: CoreHarness | undefined;
  let restarted: CoreHarness | undefined;
  let firstClosed = false;
  try {
    first = await createCoreHarness({
      root,
      bot,
      transport,
      allowedGroupIds: [GROUP_ID],
      prompt,
      worktreePort: worktrees,
    });
    const fixture = await createExistingSingleProjectFixture(first);
    additionalRepo = fixture.additionalRepo;
    const { sourceRepo, legacyWorktree, legacyBranch, threadTarget } = fixture;
    const originalSessionId = await primeOriginalThreadSession(first, threadTarget, primePrompt);

    await sendThreadTask(first, threadTarget, "accept-workspace-expansion", "accept expanded workspace");
    await waitForHeldPrompt(first, threadTarget, "accept expanded workspace", expansionPrompt);
    expect(operationError).toBeUndefined();
    expect(expansionReceipt).toMatchObject({ accepted: true, status: "queued" });
    const acceptedTask = await finishHeldPrompt(first, threadTarget, expansionPrompt);
    expect(acceptedTask.state).toBe("completed");
    await waitForWorkspaceOutcome(
      root,
      bot.uid,
      threadTarget,
      "non-ready",
      originalSessionId,
      legacyWorktree,
    );
    expect(worktrees.createCalls).toBe(1);
    expect(worktrees.createdPaths).toHaveLength(1);
    const unknownWorktreePath = worktrees.createdPaths[0]!;
    expect(await git(unknownWorktreePath, "rev-parse", "--show-toplevel")).toBe(unknownWorktreePath);
    const unknownWorktreeBranch = await git(unknownWorktreePath, "branch", "--show-current");
    const registeredWorktrees = (await git(additionalRepo, "worktree", "list", "--porcelain")).split("\n");
    expect(registeredWorktrees).toContain(`worktree ${unknownWorktreePath}`);
    expect(registeredWorktrees).toContain(`branch refs/heads/${unknownWorktreeBranch}`);

    await closeCoreHarness(first, { remove: false });
    firstClosed = true;
    restarted = await createCoreHarness({
      root,
      bot,
      transport,
      allowedGroupIds: [GROUP_ID],
      prompt,
      worktreePort: worktrees,
    });
    expect(worktrees.createCalls).toBe(1);
    expect(worktrees.createdPaths).toHaveLength(1);

    await restarted.service.submitInbound(inbound({
      ...GROUP_TARGET,
      messageId: "query-after-workspace-restart",
      senderId: "owner-1",
      text: "query workspace after restart",
      mentionedUserIds: [bot.uid],
    }));
    await waitForHeldPrompt(restarted, GROUP_TARGET, "query workspace after restart", queryPrompt);
    const queryStatus = await finishHeldPrompt(restarted, GROUP_TARGET, queryPrompt);
    expect(queryStatus.state).toBe("completed");
    expect(queryError).toBeUndefined();
    expect(queriedWorkspace).toMatchObject({
      layout: "single",
      status: "ready",
      ready: true,
      cwd: legacyWorktree,
    });
    expect(queriedWorkspaceOperation).toMatchObject({ status: "unknown" });
    expect(queriedWorkspaceOperation?.projects.find(({ name }) => name === "added-project"))
      .toMatchObject({
        name: "added-project",
        path: unknownWorktreePath,
        branch: unknownWorktreeBranch,
        created: false,
      });
    expect(worktrees.createCalls).toBe(1);
    expect(await git(unknownWorktreePath, "rev-parse", "--show-toplevel")).toBe(unknownWorktreePath);
    expect((await git(additionalRepo, "worktree", "list", "--porcelain")).split("\n"))
      .toContain(`worktree ${unknownWorktreePath}`);

    const workspace = queriedWorkspace;
    if (!workspace) throw new Error("thread-info omitted the recovered workspace record");
    expect(workspace.projects.find(({ name }) => name === "legacy-project")).toMatchObject({
      repo: sourceRepo,
      path: legacyWorktree,
      branch: legacyBranch,
      created: true,
    });
    expect(await git(legacyWorktree, "branch", "--show-current")).toBe(legacyBranch);
    expect(await readFile(join(legacyWorktree, "dirty.txt"), "utf8")).toBe(DIRTY_CONTENT);
    expect(await restarted.service.status(threadTarget)).toMatchObject({
      cwd: legacyWorktree,
      sessionId: originalSessionId,
    });
  } finally {
    primePrompt.release();
    expansionPrompt.release();
    queryPrompt.release();
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (first && !firstClosed) await closeCoreHarness(first, { remove: false });
    await rm(root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });
