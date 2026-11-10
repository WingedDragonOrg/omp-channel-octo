import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import type {
  ChatTarget,
  HostTool,
  OmpWorker,
  ThreadSummary,
  ThreadWorkspace,
  ThreadWorkspaceRequest,
  WorkerOptions,
} from "../src/contracts.ts";
import type { ProjectSessionState } from "../src/session/project-session.ts";
import { ChannelStore } from "../src/store/index.ts";
import type {
  BindingRecord,
  ThreadWorkspaceOperationRecord,
  ThreadWorkspaceRecord,
  WorkerLedgerRecord,
} from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  FakeWorktrees,
  inbound,
  type CoreHarness,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-thread-workspace-expand-review";
const GROUP_TARGET = { channelId: GROUP_ID, channelType: 2 as const };
const EXPAND_TEXT = "expand the existing thread workspace for review";
const PRIME_TEXT = "create the existing thread session for review";
const LEGACY_BRANCH = "review-existing-branch";
const LEGACY_DIRTY_CONTENT = "keep this pre-existing thread work\n";
const THREAD_WORKSPACE_NAME = "review-expansion";

type MoveFault = "refreshState" | "verification" | "worker-stop";

type ExpansionState = {
  shortId: string;
  repo: string;
  receipt?: unknown;
  error?: string;
};

class MoveFaultWorkerFactory extends FakeWorkerFactory {
  readonly moveDispatches: string[] = [];

  constructor(prompt: FakePrompt, private readonly fault: MoveFault) {
    super(prompt);
  }

  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const worker = await super.create(options);
    let moveDispatched = false;
    return new Proxy(worker, {
      get: (target, property) => {
        if (property === "prompt") {
          return async (
            text: string,
            onEvent: Parameters<OmpWorker["prompt"]>[1],
            context?: Parameters<OmpWorker["prompt"]>[2],
          ): Promise<void> => {
            if (text.startsWith("/move ")) {
              moveDispatched = true;
              this.moveDispatches.push(text);
            }
            await target.prompt(text, onEvent, context);
          };
        }
        if (property === "refreshState") {
          return async (): Promise<ProjectSessionState> => {
            if (moveDispatched && this.fault === "refreshState") {
              throw new Error("fixture refreshState failed after native /move dispatch");
            }
            const state = await target.refreshState();
            if (moveDispatched && this.fault === "verification") {
              return { ...state, cwd: options.cwd };
            }
            return state;
          };
        }
        if (property === "stop") {
          return async (): Promise<boolean> => {
            if (moveDispatched && this.fault === "worker-stop") {
              await target.stop();
              throw new Error("fixture worker stop failed after native /move dispatch");
            }
            return target.stop();
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as OmpWorker;
  }
}

class RealGitWorktrees extends FakeWorktrees {
  constructor(private readonly fixtureRoot: string) {
    super(fixtureRoot);
  }

  override async create(request: ThreadWorkspaceRequest): Promise<ThreadWorkspace> {
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
    request.lifecycle?.onConfirmedExit();
    return { cwd: await realpath(cwd), branch };
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
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed (${exitCode}): ${stderr.trim()}`);
  return stdout.trim();
}

async function createGitRepo(root: string, name: string): Promise<string> {
  const repo = join(root, "repos", name);
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
    "initial fixture commit",
  );
  return realpath(repo);
}

function managementTool(promptContext: { tools?: HostTool[] } | undefined): HostTool {
  const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
  if (!management) throw new Error("octo_management host tool missing from the real prompt context");
  return management;
}

type FixtureOptions = {
  moveFault?: MoveFault;
  worktrees?: (root: string) => FakeWorktrees;
};

type ReviewFixture = {
  root: string;
  harness: CoreHarness;
  bot: CoreHarness["bot"];
  thread: ThreadSummary;
  target: { channelId: string; channelType: 5 };
  shortId: string;
  sourceRepo: string;
  legacyPath: string;
  additionalRepo: string;
  originalBinding: BindingRecord;
  expansion: ExpansionState;
  worktrees: FakeWorktrees;
};

async function createReviewFixture(options: FixtureOptions = {}): Promise<ReviewFixture> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-thread-expand-review-move-")));
  const bot = { uid: `bot-thread-expand-review-${randomUUID()}`, name: "Thread Expansion Review Fixture" };
  const sourceRepo = await createGitRepo(root, "legacy-source");
  const additionalRepo = await createGitRepo(root, "additional-source");
  const legacyPath = join(root, "legacy-project-worktree");
  await git(sourceRepo, "worktree", "add", "-q", "-b", LEGACY_BRANCH, legacyPath);
  const canonicalLegacyPath = await realpath(legacyPath);
  await writeFile(join(canonicalLegacyPath, "dirty.txt"), LEGACY_DIRTY_CONTENT, "utf8");

  const expansion: ExpansionState = { shortId: "", repo: additionalRepo };
  const prompt: FakePrompt = async ({ text, promptContext }) => {
    if (text !== EXPAND_TEXT) return;
    try {
      const result = await managementTool(promptContext).execute({
        action: "thread-workspace-add",
        short_id: expansion.shortId,
        worktree_name: THREAD_WORKSPACE_NAME,
        projects: [{ name: "added-project", repo: expansion.repo }],
      }, new AbortController().signal);
      expansion.receipt = JSON.parse(result) as unknown;
    } catch (error) {
      expansion.error = error instanceof Error ? error.message : String(error);
      throw error;
    }
  };
  const workers = options.moveFault === undefined
    ? new FakeWorkerFactory(prompt)
    : new MoveFaultWorkerFactory(prompt, options.moveFault);
  const worktrees = options.worktrees?.(root) ?? new RealGitWorktrees(root);
  const transport = new FakeTransport({ bot });
  const harness = await createCoreHarness({
    root,
    bot,
    transport,
    workers,
    worktreePort: worktrees,
    allowedGroupIds: [GROUP_ID],
  });

  try {
    const thread = await transport.createThread(GROUP_ID, "Existing single-project review thread");
    expansion.shortId = thread.shortId;
    const target = { channelId: thread.channelId, channelType: 5 as const };
    const store = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    try {
      store.ensureBinding({
        botUid: bot.uid,
        ...GROUP_TARGET,
        sessionId: "review-parent-session",
        cwd: sourceRepo,
        sessionFile: null,
        cwdPinned: true,
      });
      store.ensureBinding({
        botUid: bot.uid,
        ...target,
        sessionId: "review-original-thread-session",
        cwd: canonicalLegacyPath,
        sessionFile: null,
        cwdPinned: true,
      });
      store.saveThreadWorkspace({
        botUid: bot.uid,
        ...target,
        layout: "single",
        status: "ready",
        workspaceRoot: null,
        entryPath: null,
        projects: [{
          name: "legacy-project",
          repo: sourceRepo,
          path: canonicalLegacyPath,
          branch: LEGACY_BRANCH,
          created: true,
        }],
        error: null,
      });
    } finally {
      store.close();
    }

    await harness.service.submitInbound(inbound({
      ...target,
      messageId: "prime-review-thread-session",
      senderId: "owner-1",
      text: PRIME_TEXT,
      mentionedUserIds: [bot.uid],
    }));
    const primed = await waitForStatus(
      harness.service,
      target,
      status => status.state === "completed" || status.state === "failed",
    );
    if (primed.state !== "completed" || primed.sessionId === null) {
      throw new Error("fixture failed to create the original thread session");
    }

    const snapshotStore = await ChannelStore.open(join(harness.stateDir, "channel.sqlite"));
    let originalBinding: BindingRecord | null;
    try {
      originalBinding = snapshotStore.getBinding(bot.uid, target);
    } finally {
      snapshotStore.close();
    }
    if (originalBinding === null || originalBinding.sessionFile === null) {
      throw new Error("fixture did not persist the original session file");
    }
    return {
      root,
      harness,
      bot,
      thread,
      target,
      shortId: thread.shortId,
      sourceRepo,
      legacyPath: canonicalLegacyPath,
      additionalRepo,
      originalBinding,
      expansion,
      worktrees,
    };
  } catch (error) {
    await closeCoreHarness(harness, { remove: false });
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function submitExpansion(fixture: ReviewFixture): Promise<void> {
  await fixture.harness.service.submitInbound(inbound({
    ...GROUP_TARGET,
    messageId: `expand-review-${randomUUID()}`,
    senderId: "owner-1",
    text: EXPAND_TEXT,
    mentionedUserIds: [fixture.bot.uid],
  }));
  const status = await waitForStatus(
    fixture.harness.service,
    GROUP_TARGET,
    candidate => candidate.state === "completed" || candidate.state === "failed",
  );
  expect(status.state).toBe("completed");
  expect(fixture.expansion.error).toBeUndefined();
  expect(fixture.expansion.receipt).toMatchObject({ accepted: true, status: "queued" });
}

type ReviewSnapshot = {
  operation: ThreadWorkspaceOperationRecord | null;
  workspace: ThreadWorkspaceRecord | null;
  binding: BindingRecord | null;
  operationBlocked: boolean;
  ledger: WorkerLedgerRecord | null;
};

async function readSnapshot(
  root: string,
  botUid: string,
  target: ChatTarget,
): Promise<ReviewSnapshot> {
  const store = await ChannelStore.open(join(root, "state", "channel.sqlite"));
  try {
    const operation = store.getLatestThreadWorkspaceOperation(botUid, target);
    return {
      operation,
      workspace: store.getThreadWorkspace(botUid, target),
      binding: store.getBinding(botUid, target),
      operationBlocked: store.isThreadWorkspaceOperationBlocked(botUid, target),
      ledger: operation?.workerLedgerId === null || operation?.workerLedgerId === undefined
        ? null
        : store.getWorkerLedger(botUid, operation.workerLedgerId),
    };
  } finally {
    store.close();
  }
}

async function waitForTerminalOperation(
  root: string,
  botUid: string,
  target: ChatTarget,
): Promise<ReviewSnapshot> {
  const deadline = performance.now() + 15_000;
  while (performance.now() < deadline) {
    const snapshot = await readSnapshot(root, botUid, target);
    if (
      snapshot.operation !== null
      && ["complete", "failed", "cancelled", "unknown"].includes(snapshot.operation.status)
    ) return snapshot;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`thread workspace operation did not settle: ${JSON.stringify(await readSnapshot(root, botUid, target))}`);
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

async function readSessionHeader(path: string): Promise<{ id?: unknown; cwd?: unknown }> {
  const firstLine = (await readFile(path, "utf8")).split("\n", 1)[0] ?? "";
  return JSON.parse(firstLine) as { id?: unknown; cwd?: unknown };
}

for (const fault of ["refreshState", "verification", "worker-stop"] as const) {
  test(`a non-cancellation ${fault} failure after native /move stays unknown and retains the original ownership`, async () => {
    const fixture = await createReviewFixture({ moveFault: fault });
    try {
      await submitExpansion(fixture);
      const snapshot = await waitForTerminalOperation(
        fixture.root,
        fixture.bot.uid,
        fixture.target,
      );
      const operation = snapshot.operation;
      expect(operation).not.toBeNull();
      if (operation === null) throw new Error("workspace operation disappeared");

      const workerFactory = fixture.harness.workers as MoveFaultWorkerFactory;
      expect(workerFactory.moveDispatches).toHaveLength(1);
      expect(operation).toMatchObject({
        status: "unknown",
        phase: "move-dispatch-intent",
        workerLedgerId: expect.any(String),
        expectedBinding: {
          sessionId: fixture.originalBinding.sessionId,
          cwd: fixture.legacyPath,
          sessionFile: fixture.originalBinding.sessionFile,
        },
        expectedWorkspace: {
          layout: "single",
          projects: [{
            name: "legacy-project",
            path: fixture.legacyPath,
            branch: LEGACY_BRANCH,
            created: true,
          }],
        },
      });
      expect(snapshot.workspace).toMatchObject({
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{ path: fixture.legacyPath, branch: LEGACY_BRANCH, created: true }],
      });
      expect(snapshot.binding).toMatchObject({
        sessionId: fixture.originalBinding.sessionId,
        cwd: fixture.legacyPath,
        sessionFile: fixture.originalBinding.sessionFile,
      });
      expect(snapshot.operationBlocked).toBe(true);

      const operationWorkspace = operation.candidateWorkspace;
      expect(operationWorkspace.workspaceRoot).not.toBeNull();
      expect(operationWorkspace.entryPath).not.toBeNull();
      const entryPath = operationWorkspace.entryPath!;
      const addedProject = operationWorkspace.projects.find(({ name }) => name === "added-project");
      expect(addedProject).toMatchObject({
        repo: fixture.additionalRepo,
        path: join(entryPath, "added-project"),
        created: true,
      });
      expect(addedProject?.branch).toBeTruthy();
      if (addedProject === undefined || addedProject.path === null) {
        throw new Error("confirmed project ownership has no path");
      }
      expect(await git(addedProject.path, "rev-parse", "--show-toplevel")).toBe(addedProject.path);
      expect(await realpath(join(entryPath, "legacy-project"))).toBe(fixture.legacyPath);
      expect(await readSessionHeader(fixture.originalBinding.sessionFile!)).toMatchObject({
        id: fixture.originalBinding.sessionId,
        cwd: entryPath,
      });
      expect(snapshot.ledger).toMatchObject({
        sessionId: fixture.originalBinding.sessionId,
        sessionFile: fixture.originalBinding.sessionFile,
        cwd: fixture.legacyPath,
        moveTargetCwd: entryPath,
        state: fault === "worker-stop" ? "running" : "stopped",
      });
    } finally {
      await closeCoreHarness(fixture.harness, { remove: false });
      await rm(fixture.root, { recursive: true, force: true });
    }
  }, { timeout: 30_000 });
}

test("a confirmed shared root remains owned when creating its entry fails", async () => {
  const fixture = await createReviewFixture();
  const originalUpdate = ChannelStore.prototype.updateThreadWorkspaceOperation;
  let injectedEntryPath: string | undefined;
  ChannelStore.prototype.updateThreadWorkspaceOperation = function (
    this: ChannelStore,
    operation: ThreadWorkspaceOperationRecord,
  ) {
    const updated = originalUpdate.call(this, operation);
    if (operation.phase === "entry-root-created" && injectedEntryPath === undefined) {
      injectedEntryPath = operation.candidateWorkspace.entryPath ?? undefined;
      if (injectedEntryPath === undefined) throw new Error("entry path missing at root confirmation");
      writeFileSync(injectedEntryPath, "occupied before mkdir\n", "utf8");
    }
    return updated;
  };

  try {
    await submitExpansion(fixture);
    const snapshot = await waitForTerminalOperation(fixture.root, fixture.bot.uid, fixture.target);
    const ownedRoot = snapshot.workspace?.workspaceRoot;
    if (typeof ownedRoot !== "string") {
      throw new Error("failed expansion did not preserve shared-root ownership");
    }
    expect(snapshot.operation).toMatchObject({
      status: "failed",
      phase: "entry-root-created",
      candidateWorkspace: {
        layout: "shared",
        workspaceRoot: ownedRoot,
        entryPath: injectedEntryPath,
      },
    });
    expect(snapshot.workspace).toMatchObject({
      layout: "shared",
      status: "failed",
      workspaceRoot: ownedRoot,
      entryPath: injectedEntryPath,
      projects: expect.arrayContaining([expect.objectContaining({
        name: "legacy-project",
        path: fixture.legacyPath,
        branch: LEGACY_BRANCH,
        created: true,
      })]),
    });
    expect(snapshot.binding).toMatchObject({
      sessionId: fixture.originalBinding.sessionId,
      cwd: fixture.legacyPath,
      sessionFile: fixture.originalBinding.sessionFile,
    });
    expect((await stat(ownedRoot)).isDirectory()).toBe(true);
    expect(await readFile(injectedEntryPath!, "utf8")).toBe("occupied before mkdir\n");
    expect(await git(fixture.legacyPath, "branch", "--show-current")).toBe(LEGACY_BRANCH);
    expect(await readFile(join(fixture.legacyPath, "dirty.txt"), "utf8")).toBe(LEGACY_DIRTY_CONTENT);
  } finally {
    ChannelStore.prototype.updateThreadWorkspaceOperation = originalUpdate;
    await closeCoreHarness(fixture.harness, { remove: false });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, { timeout: 30_000 });

test("restart after entry mkdir but before its durable phase update keeps the operation unknown and does not create a project or move the session", async () => {
  const fixture = await createReviewFixture();
  const markerPath = join(fixture.root, "entry-mkdir-crash-marker.json");
  let child: Bun.Subprocess | undefined;
  let childStderr: Promise<string> | undefined;
  let restarted: CoreHarness | undefined;
  let fixtureClosed = false;

  try {
    await closeCoreHarness(fixture.harness, { remove: false });
    fixtureClosed = true;

    const fixtureUrl = new URL("./fixtures/core-fakes.ts", import.meta.url).href;
    const storeUrl = new URL("../src/store/index.ts", import.meta.url).href;
    const childSource = `
      import { writeFileSync } from "node:fs";
      const { ChannelStore } = await import(${JSON.stringify(storeUrl)});
      const { createCoreHarness, FakeTransport, FakeWorktrees, inbound } = await import(${JSON.stringify(fixtureUrl)});
      const root = process.env.REVIEW_EXPANSION_ROOT;
      const marker = process.env.REVIEW_ENTRY_MARKER;
      if (!root || !marker) throw new Error("restart fixture paths are missing");
      const originalUpdate = ChannelStore.prototype.updateThreadWorkspaceOperation;
      ChannelStore.prototype.updateThreadWorkspaceOperation = function(operation) {
        if (operation.phase === "entry-created") {
          writeFileSync(marker, JSON.stringify({
            workspaceRoot: operation.candidateWorkspace.workspaceRoot,
            entryPath: operation.candidateWorkspace.entryPath,
          }), "utf8");
          process.kill(process.pid, "SIGKILL");
        }
        return originalUpdate.call(this, operation);
      };
      const bot = ${JSON.stringify(fixture.bot)};
      const transport = new FakeTransport({ bot });
      transport.setThreads(${JSON.stringify(GROUP_ID)}, [${JSON.stringify(fixture.thread)}]);
      const harness = await createCoreHarness({
        root,
        bot,
        transport,
        allowedGroupIds: [${JSON.stringify(GROUP_ID)}],
        worktrees: new FakeWorktrees(root),
        prompt: async ({ text, promptContext }) => {
          if (text !== ${JSON.stringify(EXPAND_TEXT)}) return;
          const management = promptContext?.tools?.find(({ name }) => name === "octo_management");
          if (!management) throw new Error("octo_management host tool missing");
          await management.execute({
            action: "thread-workspace-add",
            short_id: ${JSON.stringify(fixture.shortId)},
            worktree_name: ${JSON.stringify(THREAD_WORKSPACE_NAME)},
            projects: [{ name: "added-project", repo: ${JSON.stringify(fixture.additionalRepo)} }],
          }, new AbortController().signal);
        },
      });
      // This child process must load the test modules into its own isolated runtime.
      const holdGate = Promise.withResolvers();
      process.stdin.once("data", holdGate.resolve);
      const hold = holdGate.promise;
      const receipt = harness.service.receive(inbound({
        channelId: ${JSON.stringify(GROUP_ID)},
        channelType: 2,
        messageId: "crash-after-entry-mkdir",
        senderId: "owner-1",
        text: ${JSON.stringify(EXPAND_TEXT)},
        mentionedUserIds: [bot.uid],
      }));
      await receipt.ackReady;
      await hold;
    `;
    child = Bun.spawn([process.execPath, "-e", childSource], {
      env: {
        ...process.env,
        REVIEW_EXPANSION_ROOT: fixture.root,
        REVIEW_ENTRY_MARKER: markerPath,
      },
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    });
    childStderr = new Response(child.stderr as ReadableStream<Uint8Array>).text();

    const deadline = performance.now() + 15_000;
    while (performance.now() < deadline && !(await pathExists(markerPath))) {
      if (child.exitCode !== null) {
        throw new Error(`crash fixture exited before entry mkdir: ${await childStderr}`);
      }
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    expect(await pathExists(markerPath)).toBe(true);
    expect(await child.exited).not.toBe(0);
    await childStderr;
    child = undefined;
    const marker = JSON.parse(await readFile(markerPath, "utf8")) as {
      workspaceRoot: string | null;
      entryPath: string | null;
    };
    expect(marker.workspaceRoot).toBeTruthy();
    expect(marker.entryPath).toBeTruthy();

    const interrupted = await readSnapshot(fixture.root, fixture.bot.uid, fixture.target);
    expect(interrupted.operation).toMatchObject({
      status: "running",
      phase: "entry-create-intent",
      candidateWorkspace: {
        workspaceRoot: marker.workspaceRoot,
        entryPath: marker.entryPath,
      },
    });
    expect((await stat(marker.workspaceRoot!)).isDirectory()).toBe(true);
    expect((await stat(marker.entryPath!)).isDirectory()).toBe(true);
    expect(await readdir(marker.entryPath!)).toEqual([]);

    const restartTransport = new FakeTransport({ bot: fixture.bot });
    restartTransport.setThreads(GROUP_ID, [fixture.thread]);
    const restartWorktrees = new FakeWorktrees(fixture.root);
    restarted = await createCoreHarness({
      root: fixture.root,
      bot: fixture.bot,
      transport: restartTransport,
      allowedGroupIds: [GROUP_ID],
      worktrees: restartWorktrees,
    });

    const recovered = await waitForTerminalOperation(fixture.root, fixture.bot.uid, fixture.target);
    expect(recovered.operation).toMatchObject({
      status: "unknown",
      candidateWorkspace: {
        layout: "shared",
        workspaceRoot: marker.workspaceRoot,
        entryPath: marker.entryPath,
      },
    });
    expect(recovered.workspace).toMatchObject({
      layout: "single",
      status: "ready",
      entryPath: null,
      projects: [{ path: fixture.legacyPath, branch: LEGACY_BRANCH, created: true }],
    });
    expect(recovered.binding).toMatchObject({
      sessionId: fixture.originalBinding.sessionId,
      cwd: fixture.legacyPath,
      sessionFile: fixture.originalBinding.sessionFile,
    });
    expect(recovered.operationBlocked).toBe(true);
    expect(restartWorktrees.requests).toHaveLength(0);
    expect(restarted.workers.prompts.some(({ text }) => text.startsWith("/move "))).toBe(false);
    expect((await stat(marker.workspaceRoot!)).isDirectory()).toBe(true);
    expect((await stat(marker.entryPath!)).isDirectory()).toBe(true);
    expect(await readdir(marker.entryPath!)).toEqual([]);
    expect(await git(fixture.legacyPath, "branch", "--show-current")).toBe(LEGACY_BRANCH);
    expect(await readFile(join(fixture.legacyPath, "dirty.txt"), "utf8")).toBe(LEGACY_DIRTY_CONTENT);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (childStderr) await childStderr.catch(() => "");
    if (restarted) await closeCoreHarness(restarted, { remove: false });
    if (!fixtureClosed) await closeCoreHarness(fixture.harness, { remove: false });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, { timeout: 45_000 });
