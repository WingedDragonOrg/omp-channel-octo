import { Database } from "bun:sqlite";
import { expect, test, vi } from "bun:test";
import { mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type {
  ChatTarget,
  CommitResult,
  CommitWorker,
  CommitWorkerOptions,
  ProgressCard,
  RestartPort,
  SendReceipt,
} from "../src/contracts.ts";
import { buildExecutionCard, type ExecutionCardCapabilities } from "../src/cards/execution-card.ts";
import { formatDuration } from "../src/cards/execution-render.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { BindingRecord, TaskRecord, ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  FakeWorkerFactory,
  inbound,
  type CoreHarness,
  type FakePrompt,
  waitForStatus,
} from "./fixtures/core-fakes.ts";

const HEAD_A = "a".repeat(40);
const HEAD_B = "b".repeat(40);
const HEAD_C = "c".repeat(40);
const SUCCESS_RESULT: CommitResult = { exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_B };
const STOPPED_RESULT: CommitResult = { exitCode: 130, headBefore: HEAD_A, headAfter: HEAD_A };
const noOpPrompt: FakePrompt = async () => {};

class CommitCall implements CommitWorker {
  runCalls = 0;
  stopCalls = 0;

  constructor(
    readonly index: number,
    readonly options: CommitWorkerOptions,
    private readonly factory: ControlledCommitFactory,
  ) {}

  /** Reports one already-filtered native stage, exactly as the worker would after its gate opens. */
  emitProgress(summary: string): void {
    this.options.onProgress?.({ type: "commit_progress", atMs: Date.now(), summary });
  }

  run(): Promise<CommitResult> {
    this.runCalls += 1;
    return this.factory.runHandler(this);
  }

  stop(): Promise<boolean> {
    this.stopCalls += 1;
    return this.factory.stopHandler(this);
  }
}

/** The service and SQLite/scheduler remain real; only the lower native CLI worker is controlled. */
class ControlledCommitFactory {
  readonly calls: CommitCall[] = [];
  createHandler: (call: CommitCall) => Promise<void> = async () => {};
  runHandler: (call: CommitCall) => Promise<CommitResult> = async () => SUCCESS_RESULT;
  stopHandler: (call: CommitCall) => Promise<boolean> = async () => true;

  async createCommit(options: CommitWorkerOptions): Promise<CommitWorker> {
    const call = new CommitCall(this.calls.length, options, this);
    this.calls.push(call);
    options.onSpawn({
      pid: 900_000 + call.index,
      pgid: 900_000 + call.index,
      identity: `fixture-commit-${call.index}`,
      cwd: options.cwd,
    });
    await this.createHandler(call);
    return call;
  }
}

class CommitTestWorkerFactory extends FakeWorkerFactory {
  constructor(prompt: FakePrompt, private readonly commits: ControlledCommitFactory) {
    super(prompt);
  }

  override createCommit(options: CommitWorkerOptions): Promise<CommitWorker> {
    return this.commits.createCommit(options);
  }
}

interface RecordedProgressSend {
  target: ChatTarget;
  progress: ProgressCard;
  clientMsgNo: string;
  messageId: string;
}

interface RecordedProgressEdit {
  target: ChatTarget;
  messageId: string;
  progress: ProgressCard;
  cardSeq: number;
}

type ProgressSendHandler = (
  target: ChatTarget,
  progress: ProgressCard,
  clientMsgNo: string,
  signal?: AbortSignal,
) => Promise<SendReceipt | null>;

type ProgressEditHandler = (
  target: ChatTarget,
  messageId: string,
  progress: ProgressCard,
  cardSeq: number,
  signal?: AbortSignal,
) => Promise<void>;

/** Records native commit cards so the test reads the real service-issued frames. */
class CommitProgressTransport extends FakeTransport {
  readonly progressSends: RecordedProgressSend[] = [];
  readonly progressEdits: RecordedProgressEdit[] = [];
  progressSendHandler: ProgressSendHandler | undefined;
  progressEditHandler: ProgressEditHandler | undefined;

  override async sendProgressCard(
    target: ChatTarget,
    progress: ProgressCard,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt | null> {
    const messageId = `commit-progress-${this.progressSends.length + 1}`;
    this.progressSends.push({ target: { ...target }, progress: { ...progress }, clientMsgNo, messageId });
    if (this.progressSendHandler) {
      return this.progressSendHandler(target, progress, clientMsgNo, signal);
    }
    return { messageId };
  }

  override async editProgressCard(
    target: ChatTarget,
    messageId: string,
    progress: ProgressCard,
    cardSeq: number,
    signal?: AbortSignal,
  ): Promise<void> {
    this.progressEdits.push({
      target: { ...target },
      messageId,
      progress: { ...progress },
      cardSeq,
    });
    if (this.progressEditHandler) {
      await this.progressEditHandler(target, messageId, progress, cardSeq, signal);
    }
  }
}

const commitCardCapabilities: ExecutionCardCapabilities = {
  enabled: true,
  interactive: true,
  profiles: ["octo/v1", "octo/v2"],
  cardVersion: "1.5",
  elements: ["TextBlock", "RichTextBlock", "Container", "ColumnSet", "ActionSet"],
  actions: ["Action.OpenUrl", "Action.ToggleVisibility", "Action.CopyToClipboard"],
  limits: { maxNodes: 200, maxDepth: 16, maxPayloadBytes: 524_288 },
};

function renderedProgressText(progress: ProgressCard): string {
  const rendered = buildExecutionCard(progress, commitCardCapabilities);
  if (!rendered) throw new Error("commit progress card did not build");
  const text: string[] = [];
  const visit = (value: unknown): void => {
    if (Array.isArray(value)) {
      for (const child of value) visit(child);
      return;
    }
    if (value === null || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    if (typeof record.text === "string") text.push(record.text);
    for (const child of Object.values(record)) visit(child);
  };
  visit(rendered.card);
  return text.join("\n");
}

function latestProgress(transport: CommitProgressTransport): ProgressCard | undefined {
  return transport.progressEdits.at(-1)?.progress ?? transport.progressSends.at(-1)?.progress;
}

async function flushMicrotasks(rounds = 20): Promise<void> {
  for (let index = 0; index < rounds; index += 1) await Promise.resolve();
}

interface CommitHarness {
  harness: CoreHarness;
  commits: ControlledCommitFactory;
}

async function createCommitHarness(options: {
  root?: string;
  bot?: CoreHarness["bot"];
  transport?: FakeTransport;
  prompt?: FakePrompt;
  restart?: RestartPort;
} = {}): Promise<CommitHarness> {
  const commits = new ControlledCommitFactory();
  const workers = new CommitTestWorkerFactory(options.prompt ?? noOpPrompt, commits);
  const harness = await createCoreHarness({
    ...(options.root === undefined ? {} : { root: options.root }),
    ...(options.bot === undefined ? {} : { bot: options.bot }),
    ...(options.transport === undefined ? {} : { transport: options.transport }),
    ...(options.restart === undefined ? {} : { restart: options.restart }),
    workers,
  });
  return { harness, commits };
}


async function withStore<T>(harness: CoreHarness, work: (store: ChannelStore) => T | Promise<T>): Promise<T> {
  const store = await openChannelStore(harness.stateDir);
  try {
    return await work(store);
  } finally {
    store.close();
  }
}

async function bind(harness: CoreHarness, target: ChatTarget, cwd: string, pinned = false): Promise<void> {
  const binding: BindingRecord = {
    botUid: harness.bot.uid,
    ...target,
    sessionId: `session-${target.channelType}-${target.channelId}`,
    cwd,
    sessionFile: null,
    ...(pinned ? { cwdPinned: true } : {}),
  };
  await withStore(harness, (store) => store.ensureBinding(binding));
}

async function bindWithSession(
  harness: CoreHarness,
  target: ChatTarget,
  cwd: string,
  sessionId: string,
): Promise<void> {
  const sessionFile = join(harness.root, `${sessionId}.jsonl`);
  await writeFile(sessionFile, `${JSON.stringify({
    type: "session",
    id: sessionId,
    cwd,
    previousSessionFiles: [],
  })}\n`, "utf8");
  const binding: BindingRecord = {
    botUid: harness.bot.uid,
    ...target,
    sessionId,
    cwd,
    sessionFile,
  };
  await withStore(harness, (store) => store.ensureBinding(binding));
}

function workspaceRecord(
  harness: CoreHarness,
  target: ChatTarget & { channelType: 5 },
  layout: ThreadWorkspaceRecord["layout"],
  entryPath: string | null,
  projects: readonly { name: string; repo: string; path: string; branch: string }[],
): ThreadWorkspaceRecord {
  return {
    botUid: harness.bot.uid,
    channelType: 5,
    channelId: target.channelId,
    layout,
    status: "ready",
    entryPath,
    projects: projects.map((project) => ({ ...project, created: true })),
    error: null,
  };
}

async function saveWorkspace(harness: CoreHarness, record: ThreadWorkspaceRecord): Promise<void> {
  await withStore(harness, (store) => store.saveThreadWorkspace(record));
}

function ownerMessage(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
  senderId = "owner-1",
) {
  return inbound({
    ...target,
    messageId,
    senderId,
    text,
    mentionedUserIds: target.channelType === 1 ? [] : [harness.bot.uid],
  });
}

async function send(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
  senderId = "owner-1",
): Promise<void> {
  await harness.service.handleInbound(ownerMessage(harness, target, messageId, text, senderId));
}

async function waitUntil(check: () => boolean | Promise<boolean>, description: string): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function latestTask(harness: CoreHarness, target: ChatTarget): Promise<TaskRecord | null> {
  return withStore(harness, (store) => store.getLatestTask(harness.bot.uid, target));
}


async function waitForReply(
  harness: CoreHarness,
  target: ChatTarget,
  predicate: (text: string) => boolean,
): Promise<string> {
  let found: string | undefined;
  await waitUntil(() => {
    found = harness.transport.sent.find((message) =>
      message.target.channelType === target.channelType
      && message.target.channelId === target.channelId
      && predicate(message.text),
    )?.text;
    return found !== undefined;
  }, `reply for ${target.channelType}:${target.channelId}`);
  return found!;
}

async function createRepo(root: string, name: string): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo, { recursive: true });
  await writeFile(join(repo, "tracked.txt"), "initial\n", "utf8");
  await runGit(repo, ["init", "-q", "-b", "main"]);
  await runGit(repo, ["add", "."]);
  await runGit(repo, [
    "-c", "user.email=fixture@example.test",
    "-c", "user.name=Commit Fixture",
    "commit", "-q", "-m", "initial fixture commit",
  ]);
  return realpath(repo);
}

async function addLinkedWorktree(repo: string, path: string, branch: string): Promise<string> {
  await mkdir(join(path, ".."), { recursive: true });
  await runGit(repo, ["worktree", "add", "-q", "-b", branch, path]);
  return realpath(path);
}

async function runGit(cwd: string, args: string[]): Promise<string> {
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

async function taskMarkers(harness: CoreHarness, taskId: string): Promise<{ prompt_started: number; prompt_accepted: number }> {
  const db = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
  try {
    return db.query(
      "SELECT prompt_started, prompt_accepted FROM tasks WHERE id = ?",
    ).get(taskId) as { prompt_started: number; prompt_accepted: number };
  } finally {
    db.close();
  }
}

function installWorkspaceBinding(
  harness: CoreHarness,
  target: ChatTarget & { channelType: 5 },
  cwd: string,
  pinned = true,
): Promise<void> {
  return bind(harness, target, cwd, pinned);
}

test("/commit only accepts an exact start or stop form and requires an existing binding", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-no-binding" };
  try {
    await send(harness, target, "commit-bad-args", "/commit extra");
    await waitForReply(harness, target, (text) => text.includes("用法") && text.includes("/commit stop"));
    expect(await latestTask(harness, target)).toBeNull();
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, target))).toBeNull();

    await send(harness, target, "commit-no-binding", "/commit");
    await waitForReply(harness, target, (text) => text.includes("绑定") && text.includes("/new"));
    expect(await latestTask(harness, target)).toBeNull();
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, target))).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/commit stop rejects trailing arguments instead of stopping", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-stop-usage" };
  try {
    await send(harness, target, "commit-stop-extra-args", "/commit stop extra");
    await waitForReply(harness, target, (text) => text.includes("用法") && text.includes("/commit stop"));
    expect(await latestTask(harness, target)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a non-owner group command never creates a commit task or native worker", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 2 as const, channelId: "group-a" };
  try {
    await send(harness, target, "commit-non-owner", "/commit", "member-1");
    expect(await latestTask(harness, target)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/commit uses the native worker as a durable task without prompting an OMP session", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-native" };
  try {
    const repo = await createRepo(harness.root, "main-checkout");
    const boundSubdirectory = join(repo, "nested");
    await mkdir(boundSubdirectory, { recursive: true });
    await bind(harness, target, boundSubdirectory);
    let ledgerAtRun = false;
    commits.runHandler = async () => {
      const ledger = await withStore(harness, (store) => store.listWorkerLedger(harness.bot.uid));
      ledgerAtRun = ledger.some((record) => record.cwd === repo && record.state === "running");
      return SUCCESS_RESULT;
    };

    await send(harness, target, "commit-native", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const task = await latestTask(harness, target);
    expect(task?.text).toBe("/commit");
    expect(task?.cwd).toBe(boundSubdirectory);
    expect(task?.state).toBe("completed");
    expect(task?.historyEligible).toBe(false);
    expect(commits.calls).toHaveLength(1);
    expect(ledgerAtRun).toBe(true);
    expect(commits.calls[0]?.options.cwd).toBe(repo);
    expect(typeof commits.calls[0]?.options.onSpawn).toBe("function");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
    expect(await taskMarkers(harness, task!.id)).toEqual({ prompt_started: 1, prompt_accepted: 0 });
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));
    expect(reply).not.toContain("stderr");
    await waitForReply(harness, target, (text) => text.includes("开始提交"));
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a native commit worker receives the thread's inherited model", async () => {
  const { harness, commits } = await createCommitHarness();
  const group = { channelType: 2 as const, channelId: "group-a" };
  const target = { channelType: 5 as const, channelId: "group-a____commit-thread-model" };
  try {
    const repo = await createRepo(harness.root, "thread-model-commit-checkout");
    await send(harness, group, "commit-thread-model-policy", "/thread model fixture/fast:high");
    await bind(harness, target, repo);

    await send(harness, target, "commit-thread-model-run", "/commit");
    await waitForStatus(harness.service, target, status => status.state === "completed");

    expect(commits.calls).toHaveLength(1);
    expect(commits.calls[0]?.options.model).toBe("fixture/fast:high");
  } finally {
    await closeCoreHarness(harness);
  }
});


test("duplicate delivery of one /commit message creates only one durable commit task", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-deduplicated" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "deduplicated-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;
    const message = ownerMessage(harness, target, "same-commit-delivery", "/commit");

    const first = harness.service.handleInbound(message);
    const duplicate = harness.service.handleInbound(message);
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "single commit run");
    expect(commits.calls).toHaveLength(1);

    runGate.resolve(SUCCESS_RESULT);
    await Promise.all([first, duplicate]);
    expect(commits.calls).toHaveLength(1);

    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const db = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
    try {
      const row = db.query(
        "SELECT COUNT(*) AS count FROM tasks WHERE bot_uid = ? AND channel_type = ? AND channel_id = ? AND message_id = ?",
      ).get(harness.bot.uid, target.channelType, target.channelId, message.messageId) as { count: number };
      expect(row.count).toBe(1);
    } finally {
      db.close();
    }
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("no-change and nonzero native results report the observed HEAD without exposing process output", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-results" };
  try {
    const repo = await createRepo(harness.root, "results-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => call.index === 0
      ? { exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_A }
      : { exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_C };

    await send(harness, target, "commit-no-change", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const unchanged = await waitForReply(harness, target, (text) => text.includes("HEAD") && text.includes(HEAD_A.slice(0, 8)));
    expect(unchanged).toContain("没有");

    await send(harness, target, "commit-exit-one", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "failed");
    const failed = await waitForReply(harness, target, (text) => text.includes("退出码 1"));
    expect(failed).toContain(HEAD_C.slice(0, 8));
    expect(failed).not.toContain("stderr");
    expect(commits.calls).toHaveLength(2);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("shared workspace and rebinding into one of its projects are rejected without Git changes", async () => {
  const { harness, commits } = await createCommitHarness();
  const sharedTarget = { channelType: 5 as const, channelId: "group-a____commit-shared" };
  const childTarget = { channelType: 5 as const, channelId: "group-a____commit-shared-child" };
  try {
    const repoA = await createRepo(harness.root, "shared-repo-a");
    const repoB = await createRepo(harness.root, "shared-repo-b");
    const entry = join(harness.root, "shared-entry");
    await mkdir(entry, { recursive: true });
    const projectA = await addLinkedWorktree(repoA, join(entry, "api"), "shared-api");
    const projectB = await addLinkedWorktree(repoB, join(entry, "web"), "shared-web");
    await writeFile(join(projectA, "uncommitted.txt"), "leave untouched\n", "utf8");
    const statusA = await runGit(projectA, ["status", "--porcelain"]);
    const headA = await runGit(projectA, ["rev-parse", "HEAD"]);
    const record = (target: ChatTarget & { channelType: 5 }) => workspaceRecord(
      harness,
      target,
      "shared",
      entry,
      [
        { name: "api", repo: repoA, path: projectA, branch: "shared-api" },
        { name: "web", repo: repoB, path: projectB, branch: "shared-web" },
      ],
    );
    await saveWorkspace(harness, record(sharedTarget));
    await saveWorkspace(harness, record(childTarget));
    await installWorkspaceBinding(harness, sharedTarget, entry);
    // This persisted binding models /new or /move into a project child of the shared entry.
    await installWorkspaceBinding(harness, childTarget, projectA, false);

    await send(harness, sharedTarget, "commit-shared-entry", "/commit");
    await send(harness, childTarget, "commit-shared-child", "/commit");
    const refusal = await waitForReply(harness, childTarget, (text) => text.length > 0);
    expect(refusal).not.toContain("开始提交");

    expect(await latestTask(harness, sharedTarget)).toBeNull();
    expect(await latestTask(harness, childTarget)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(await runGit(projectA, ["rev-parse", "HEAD"])).toBe(headA);
    expect(await runGit(projectA, ["status", "--porcelain"])).toBe(statusA);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a thread rebound outside its shared workspace commits the current binding only", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-rebound" };
  try {
    const sharedRepoA = await createRepo(harness.root, "stale-shared-repo-a");
    const sharedRepoB = await createRepo(harness.root, "stale-shared-repo-b");
    const currentRepo = await createRepo(harness.root, "current-bound-repo");
    const entry = join(harness.root, "stale-shared-entry");
    await mkdir(entry, { recursive: true });
    const projectA = await addLinkedWorktree(sharedRepoA, join(entry, "api"), "stale-api");
    const projectB = await addLinkedWorktree(sharedRepoB, join(entry, "web"), "stale-web");
    await saveWorkspace(harness, workspaceRecord(harness, target, "shared", entry, [
      { name: "api", repo: sharedRepoA, path: projectA, branch: "stale-api" },
      { name: "web", repo: sharedRepoB, path: projectB, branch: "stale-web" },
    ]));
    // The binding is the selected workspace; the old record is not permission to guess its project.
    await bind(harness, target, currentRepo, false);
    commits.runHandler = async () => SUCCESS_RESULT;

    await send(harness, target, "commit-rebound-outside", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(commits.calls).toHaveLength(1);
    expect(commits.calls[0]?.options.cwd).toBe(currentRepo);
    expect((await latestTask(harness, target))?.cwd).toBe(currentRepo);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("commit keeps the persisted linked-worktree binding instead of following a moved OMP session", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-session-moved" };
  try {
    const repo = await createRepo(harness.root, "session-moved-main-checkout");
    const worktree = await addLinkedWorktree(repo, join(harness.root, "session-moved-linked-worktree"), "session-moved");
    const sessionId = "session-moved-binding";
    const originalSessionFile = join(worktree, "session.jsonl");
    await writeFile(originalSessionFile, `${JSON.stringify({
      type: "session",
      id: sessionId,
      cwd: worktree,
      previousSessionFiles: [],
    })}\n`, "utf8");
    const movedSessionFile = join(harness.stateDir, "sessions", "moved", "session.jsonl");
    await mkdir(join(harness.stateDir, "sessions", "moved"), { recursive: true });
    await writeFile(movedSessionFile, `${JSON.stringify({
      type: "session",
      id: sessionId,
      cwd: repo,
      previousSessionFiles: [originalSessionFile],
    })}\n`, "utf8");
    await rm(originalSessionFile);
    await saveWorkspace(harness, workspaceRecord(harness, target, "separate", null, [
      { name: "only", repo, path: worktree, branch: "session-moved" },
    ]));
    await withStore(harness, (store) => store.ensureBinding({
      botUid: harness.bot.uid,
      ...target,
      sessionId,
      cwd: worktree,
      sessionFile: originalSessionFile,
      cwdPinned: true,
    }));
    commits.runHandler = async () => SUCCESS_RESULT;

    await send(harness, target, "commit-session-moved", "/commit");
    await waitForReply(harness, target, (text) => text.includes("HEAD") || text.includes("绑定目录"));

    expect(commits.calls).toHaveLength(1);
    expect(commits.calls[0]?.options.cwd).toBe(worktree);
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, target))).toMatchObject({
      cwd: worktree,
      sessionFile: originalSessionFile,
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("two persisted worktrees of the same repository are rejected", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-multi-worktree" };
  try {
    const repo = await createRepo(harness.root, "worktree-source");
    const first = await addLinkedWorktree(repo, join(harness.root, "worktree-one"), "commit-one");
    const second = await addLinkedWorktree(repo, join(harness.root, "worktree-two"), "commit-two");
    const headBefore = await runGit(first, ["rev-parse", "HEAD"]);
    const statusBefore = await runGit(first, ["status", "--porcelain"]);
    await saveWorkspace(harness, workspaceRecord(harness, target, "separate", null, [
      { name: "one", repo, path: first, branch: "commit-one" },
      { name: "two", repo, path: second, branch: "commit-two" },
    ]));
    await installWorkspaceBinding(harness, target, first);

    await send(harness, target, "commit-multiple-worktrees", "/commit");
    await waitForReply(harness, target, (text) => text.includes("多") || text.includes("worktree"));

    expect(await latestTask(harness, target)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(await runGit(first, ["rev-parse", "HEAD"])).toBe(headBefore);
    expect(await runGit(first, ["status", "--porcelain"])).toBe(statusBefore);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("multiple persisted repository roots are rejected without native execution", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-multi-repo" };
  try {
    const repoA = await createRepo(harness.root, "multi-repo-a");
    const repoB = await createRepo(harness.root, "multi-repo-b");
    const first = await addLinkedWorktree(repoA, join(harness.root, "multi-repo-one"), "multi-repo-one");
    const second = await addLinkedWorktree(repoB, join(harness.root, "multi-repo-two"), "multi-repo-two");
    const headBefore = await runGit(first, ["rev-parse", "HEAD"]);
    const statusBefore = await runGit(first, ["status", "--porcelain"]);
    await saveWorkspace(harness, workspaceRecord(harness, target, "separate", null, [
      { name: "one", repo: repoA, path: first, branch: "multi-repo-one" },
      { name: "two", repo: repoB, path: second, branch: "multi-repo-two" },
    ]));
    await installWorkspaceBinding(harness, target, first);

    await send(harness, target, "commit-multiple-repositories", "/commit");
    await waitForReply(harness, target, (text) => text.includes("多") || text.includes("仓库"));

    expect(await latestTask(harness, target)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(await runGit(first, ["rev-parse", "HEAD"])).toBe(headBefore);
    expect(await runGit(first, ["status", "--porcelain"])).toBe(statusBefore);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("one separate linked worktree remains eligible when its source repo has a main checkout", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-single-worktree" };
  try {
    const repo = await createRepo(harness.root, "single-worktree-source");
    const worktree = await addLinkedWorktree(repo, join(harness.root, "single-worktree"), "commit-single");
    const listed = await runGit(repo, ["worktree", "list", "--porcelain"]);
    expect(listed.split("\n").filter((line) => line.startsWith("worktree "))).toHaveLength(2);
    await saveWorkspace(harness, workspaceRecord(harness, target, "separate", null, [
      { name: "only", repo, path: worktree, branch: "commit-single" },
    ]));
    await installWorkspaceBinding(harness, target, worktree);
    commits.runHandler = async () => SUCCESS_RESULT;

    await send(harness, target, "commit-single-worktree", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(commits.calls).toHaveLength(1);
    expect(commits.calls[0]?.options.cwd).toBe(worktree);
    expect((await latestTask(harness, target))?.state).toBe("completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("different linked worktree roots from one source repository can commit independently", async () => {
  const { harness, commits } = await createCommitHarness();
  const firstTarget = { channelType: 5 as const, channelId: "group-a____commit-linked-one" };
  const secondTarget = { channelType: 5 as const, channelId: "group-a____commit-linked-two" };
  const firstRun = Promise.withResolvers<CommitResult>();
  const secondRun = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "independent-worktree-source");
    const first = await addLinkedWorktree(repo, join(harness.root, "independent-one"), "independent-one");
    const second = await addLinkedWorktree(repo, join(harness.root, "independent-two"), "independent-two");
    await saveWorkspace(harness, workspaceRecord(harness, firstTarget, "separate", null, [
      { name: "one", repo, path: first, branch: "independent-one" },
    ]));
    await saveWorkspace(harness, workspaceRecord(harness, secondTarget, "separate", null, [
      { name: "two", repo, path: second, branch: "independent-two" },
    ]));
    await installWorkspaceBinding(harness, firstTarget, first);
    await installWorkspaceBinding(harness, secondTarget, second);
    commits.runHandler = async (call) => call.index === 0 ? firstRun.promise : secondRun.promise;

    await send(harness, firstTarget, "commit-linked-one", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "first worktree commit");
    await send(harness, secondTarget, "commit-linked-two", "/commit");
    await waitUntil(() => commits.calls.length === 2 && commits.calls[1]!.runCalls === 1, "second worktree commit");

    expect(commits.calls.map(({ options }) => options.cwd)).toEqual([first, second]);
    firstRun.resolve(SUCCESS_RESULT);
    secondRun.resolve({ exitCode: 0, headBefore: HEAD_B, headAfter: HEAD_C });
    await waitForStatus(harness.service, firstTarget, (status) => status.state === "completed");
    await waitForStatus(harness.service, secondTarget, (status) => status.state === "completed");
  } finally {
    firstRun.resolve(SUCCESS_RESULT);
    secondRun.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("a commit is rejected while another chat has an ordinary writer in the same repository", async () => {
  const entered = Promise.withResolvers<void>();
  const promptStopped = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ onStop }) => {
    entered.resolve();
    onStop(promptStopped.resolve);
    await promptStopped.promise;
  };
  const { harness, commits } = await createCommitHarness({ prompt });
  const runningTarget = { channelType: 1 as const, channelId: "dm-ordinary-writer" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-while-busy" };
  try {
    const repo = await createRepo(harness.root, "busy-checkout");
    const writerSubdirectory = join(repo, "nested");
    await mkdir(writerSubdirectory, { recursive: true });
    await bindWithSession(harness, runningTarget, repo, "ordinary-writer-session");
    await bind(harness, commitTarget, writerSubdirectory);
    await send(harness, runningTarget, "ordinary-writer-running", "hold the writer");
    await entered.promise;
    await send(harness, commitTarget, "commit-while-busy", "/commit");
    await waitForReply(harness, commitTarget, (text) => text.includes("忙"));

    expect(await latestTask(harness, commitTarget)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a queued writer in the commit worktree rejects another chat's commit", async () => {
  const entered = Promise.withResolvers<void>();
  const promptStopped = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ onStop }) => {
    entered.resolve();
    onStop(promptStopped.resolve);
    await promptStopped.promise;
  };
  const { harness, commits } = await createCommitHarness({ prompt });
  const queuedTarget = { channelType: 1 as const, channelId: "dm-queued-worktree-writer" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-with-queued-writer" };
  try {
    const queuedRepo = await createRepo(harness.root, "queued-writer-current-repo");
    const otherRepo = await createRepo(harness.root, "queued-writer-active-repo");
    await bindWithSession(harness, queuedTarget, otherRepo, "queued-writer-active-session");
    await bind(harness, commitTarget, queuedRepo);
    await withStore(harness, (store) => store.setChatMode(harness.bot.uid, queuedTarget, "queue"));
    await send(harness, queuedTarget, "queued-writer-active", "hold the earlier task");
    await entered.promise;

    await bindWithSession(harness, queuedTarget, queuedRepo, "queued-writer-pending-session");
    await send(harness, queuedTarget, "queued-writer-pending", "pending write");
    await waitForStatus(harness.service, queuedTarget, (status) => status.queued === 1);
    await send(harness, commitTarget, "commit-with-queued-writer", "/commit");
    await waitForReply(harness, commitTarget, (text) => text.includes("忙"));

    expect(await latestTask(harness, commitTarget)).toBeNull();
    expect(commits.calls).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a running commit excludes other same-repository tasks until it finishes", async () => {
  const { harness, commits } = await createCommitHarness();
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-exclusive" };
  const waitingTarget = { channelType: 1 as const, channelId: "dm-writer-waits" };
  const commitResult = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "exclusive-checkout");
    const writerSubdirectory = join(repo, "nested");
    await mkdir(writerSubdirectory, { recursive: true });
    await bind(harness, commitTarget, repo);
    await bind(harness, waitingTarget, writerSubdirectory);

    await send(harness, commitTarget, "commit-exclusive", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit worker run");
    await send(harness, waitingTarget, "ordinary-waits-for-commit", "must wait");
    await waitForStatus(harness.service, waitingTarget, (status) => status.state === "queued");
    expect(harness.workers.prompts).toHaveLength(0);

    commitResult.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, waitingTarget, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["must wait"]);
  } finally {
    commitResult.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("a native goal holds its Git root lease against another chat's commit", async () => {
  const goalEntered = Promise.withResolvers<void>();
  const releaseGoal = Promise.withResolvers<void>();
  const { harness, commits } = await createCommitHarness();
  const goalTarget = { channelType: 1 as const, channelId: "dm-goal-holds-repo" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-behind-goal" };
  try {
    const repo = await createRepo(harness.root, "goal-exclusive-checkout");
    await bindWithSession(harness, goalTarget, repo, "goal-exclusive-session");
    await bind(harness, commitTarget, repo);
    harness.workers.goalHandler = async ({ request, emit, sessionId }) => {
      if (request.op !== "create") throw new Error("expected a structured create request");
      const goal = {
        id: `fixture-goal-${sessionId}`,
        objective: request.objective,
        status: "active" as const,
        tokensUsed: 0,
        timeUsedSeconds: 0,
      };
      emit({ type: "goal_accepted", goalId: goal.id });
      emit({ type: "goal_updated", goal });
      goalEntered.resolve();
      await releaseGoal.promise;
      const completed = { ...goal, status: "complete" as const };
      emit({ type: "goal_updated", goal: completed });
      return { goal: completed, state: { enabled: true, mode: "exiting", goal: completed } };
    };

    await send(harness, goalTarget, "goal-holds-repo", "/goal set hold the shared repository");
    await goalEntered.promise;
    await waitForStatus(harness.service, goalTarget, (status) => status.state === "running");
    await send(harness, commitTarget, "commit-behind-goal", "/commit");
    await waitForReply(harness, commitTarget, (text) => text.includes("忙"));

    expect(await latestTask(harness, commitTarget)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.goalRuns).toHaveLength(1);

    releaseGoal.resolve();
    await waitForStatus(harness.service, goalTarget, (status) => status.state === "completed");
  } finally {
    releaseGoal.resolve();
    await closeCoreHarness(harness);
  }
});

test("a goal request is durably queued behind a running commit in the same Git root", async () => {
  const { harness, commits } = await createCommitHarness();
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-before-goal" };
  const goalTarget = { channelType: 1 as const, channelId: "dm-goal-after-commit" };
  const runGate = Promise.withResolvers<CommitResult>();
  const objective = "wait behind the native commit";
  try {
    const repo = await createRepo(harness.root, "commit-before-goal-checkout");
    await bindWithSession(harness, commitTarget, repo, "commit-before-goal-session");
    await bindWithSession(harness, goalTarget, repo, "goal-after-commit-session");
    commits.runHandler = async () => runGate.promise;

    await send(harness, commitTarget, "commit-before-goal", "/commit");
    await waitUntil(
      () => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1,
      "commit worker run",
    );
    await send(harness, goalTarget, "goal-after-commit", `/goal set ${objective}`);
    await waitForStatus(harness.service, goalTarget, (status) => status.state === "queued");

    const queuedGoal = await latestTask(harness, goalTarget);
    if (queuedGoal === null) throw new Error("goal task was not durably accepted");
    const persistedGoal = await withStore(harness, (store) => store.getGoalTask(queuedGoal.id));
    expect(persistedGoal?.request).toEqual({ op: "create", objective });
    // The async repository-root probe has no completion event; keep the commit gated while it settles.
    await new Promise<void>((resolve) => setTimeout(resolve, 50));
    expect((await latestTask(harness, goalTarget))?.state).toBe("queued");
    expect(harness.workers.goalRuns).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);

    runGate.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, commitTarget, (status) => status.state === "completed");
    await harness.workers.waitForGoalRunCount(1);
    await waitForStatus(harness.service, goalTarget, (status) => status.state === "completed");
    expect(harness.workers.goalRuns).toHaveLength(1);
    expect(harness.workers.goalRuns[0]?.request).toEqual({ op: "create", objective });
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("ordinary sessions sharing the same cwd still start in parallel", async () => {
  const bothEntered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let entered = 0;
  const prompt: FakePrompt = async () => {
    entered += 1;
    if (entered >= 2) bothEntered.resolve();
    await release.promise;
  };
  const { harness } = await createCommitHarness({ prompt });
  const first = { channelType: 1 as const, channelId: "dm-ordinary-same-cwd-a" };
  const second = { channelType: 1 as const, channelId: "dm-ordinary-same-cwd-b" };
  try {
    const repo = await createRepo(harness.root, "ordinary-parallel-checkout");
    await bind(harness, first, repo);
    await bind(harness, second, repo);
    await send(harness, first, "ordinary-parallel-one", "one");
    await send(harness, second, "ordinary-parallel-two", "two");
    await waitUntil(() => harness.workers.prompts.length === 2, "two concurrent ordinary prompts");
    await bothEntered.promise;
    release.resolve();
    await waitForStatus(harness.service, first, (status) => status.state === "completed");
    await waitForStatus(harness.service, second, (status) => status.state === "completed");
  } finally {
    release.resolve();
    await closeCoreHarness(harness);
  }
});

test("a commit also leases the complete persistent workspace resource group", async () => {
  const { harness, commits } = await createCommitHarness();
  const commitTarget = { channelType: 5 as const, channelId: "group-a____commit-resource-root" };
  const writerTarget = { channelType: 1 as const, channelId: "dm-commit-resource-child" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "resource-source");
    const worktree = await addLinkedWorktree(repo, join(harness.root, "resource-worktree"), "resource-branch");
    const child = join(worktree, "nested");
    await mkdir(child, { recursive: true });
    await saveWorkspace(harness, workspaceRecord(harness, commitTarget, "separate", null, [
      { name: "only", repo, path: worktree, branch: "resource-branch" },
    ]));
    await installWorkspaceBinding(harness, commitTarget, worktree);
    await bindWithSession(harness, writerTarget, child, "resource-child-session");
    commits.runHandler = async () => runGate.promise;

    await send(harness, commitTarget, "commit-resource-group", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, writerTarget, "writer-in-project-child", "nested project work");
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "queued");
    expect(harness.workers.prompts).toHaveLength(0);

    runGate.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "completed");
    expect(harness.workers.prompts.map(({ cwd, text }) => ({ cwd, text }))).toEqual([
      { cwd: child, text: "nested project work" },
    ]);
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("/cancel stops the current commit and cancels its queued chat tasks", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-cancel-commit" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "cancel-commit-checkout");
    await bindWithSession(harness, target, repo, "cancel-commit-session");
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => {
      runGate.resolve(STOPPED_RESULT);
      return true;
    };

    await send(harness, target, "cancel-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    const commitTask = await latestTask(harness, target);
    await send(harness, target, "cancel-commit-queued-task", "cancel this queued write");
    const queuedTask = await latestTask(harness, target);
    expect(commitTask?.text).toBe("/commit");
    expect(queuedTask?.text).toBe("cancel this queued write");
    await waitForStatus(harness.service, target, (status) => status.queued === 1);

    await send(harness, target, "cancel-commit-command", "/cancel");
    await waitForStatus(harness.service, target, (status) => status.state === "cancelled" && status.queued === 0);

    expect(commits.calls[0]?.stopCalls).toBe(1);
    expect(await withStore(harness, (store) => store.getTask(commitTask!.id)?.state)).toBe("cancelled");
    expect(await withStore(harness, (store) => store.getTask(queuedTask!.id)?.state)).toBe("cancelled");
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    runGate.resolve(STOPPED_RESULT);
    await closeCoreHarness(harness);
  }
});

test("/interrupt directs an active commit to /commit stop without aborting its native worker", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-interrupt-commit" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "interrupt-commit-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;

    await send(harness, target, "interrupt-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, target, "interrupt-commit-command", "/interrupt");
    const reply = await waitForReply(harness, target, (text) => text.includes("/commit stop"));

    expect(reply).toContain("提交");
    expect(commits.calls[0]?.stopCalls).toBe(0);
    expect(await latestTask(harness, target)).toMatchObject({ text: "/commit", state: "running" });
    runGate.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("an unconfirmed commit stop returns promptly and keeps repository descendants queued", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const commitTarget = { channelType: 1 as const, channelId: "dm-unconfirmed-stop" };
  const writerTarget = { channelType: 1 as const, channelId: "dm-unconfirmed-writer" };
  const retryTarget = { channelType: 1 as const, channelId: "dm-unconfirmed-retry" };
  const runGate = Promise.withResolvers<CommitResult>();
  let stopCommand: Promise<void> | undefined;
  try {
    const repo = await createRepo(harness.root, "unconfirmed-stop-checkout");
    const child = join(repo, "nested");
    await mkdir(child, { recursive: true });
    await bind(harness, commitTarget, repo);
    await bindWithSession(harness, writerTarget, child, "unconfirmed-writer-session");
    await bind(harness, retryTarget, repo);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => false;

    await send(harness, commitTarget, "unconfirmed-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    stopCommand = send(harness, commitTarget, "unconfirmed-commit-stop", "/commit stop");
    await waitUntil(() => commits.calls[0]!.stopCalls === 1, "unconfirmed stop request");
    const stopReply = await waitForReply(harness, commitTarget, (text) => text.includes("无法确认") && text.includes("停止"));
    await stopCommand;
    expect(stopReply).toContain("不会重试");

    await send(harness, writerTarget, "unconfirmed-writer", "must remain blocked");
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "queued");
    expect(harness.workers.prompts).toHaveLength(0);

    runGate.resolve({ exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_A });
    await waitUntil(
      () => latestProgress(transport)?.commit?.result !== undefined,
      "unknown commit card result",
    );
    expect(await latestTask(harness, writerTarget)).toMatchObject({ state: "queued" });
    expect(harness.workers.prompts).toHaveLength(0);

    await send(harness, retryTarget, "unconfirmed-commit-retry", "/commit");
    await waitForReply(harness, retryTarget, (text) => text.includes("忙") || text.includes("无法"));
    expect(commits.calls).toHaveLength(1);
  } finally {
    runGate.resolve({ exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_A });
    await stopCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("a worker-initiated deferred restart waits for an active commit then runs when it ends", async () => {
  const requests: number[] = [];
  const toolCalled = Promise.withResolvers<void>();
  let restartResult = "";
  const prompt: FakePrompt = async ({ promptContext }) => {
    const tool = (promptContext?.tools ?? []).find(({ name }) => name === "channel_control");
    if (!tool) throw new Error("channel_control host tool missing");
    restartResult = await tool.execute({ action: "restart" }, new AbortController().signal);
    toolCalled.resolve();
  };
  const { harness, commits } = await createCommitHarness({
    prompt,
    restart: { request: async () => { requests.push(Date.now()); } },
  });
  const commitTarget = { channelType: 1 as const, channelId: "dm-deferred-restart-commit" };
  const agentTarget = { channelType: 1 as const, channelId: "dm-deferred-restart-agent" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const commitRepo = await createRepo(harness.root, "deferred-restart-commit-repo");
    const agentRepo = await createRepo(harness.root, "deferred-restart-agent-repo");
    await bind(harness, commitTarget, commitRepo);
    await bindWithSession(harness, agentTarget, agentRepo, "deferred-restart-agent-session");
    commits.runHandler = async () => runGate.promise;

    await send(harness, commitTarget, "deferred-restart-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, agentTarget, "deferred-restart-agent", "schedule deferred restart");
    await toolCalled.promise;
    await waitForStatus(harness.service, agentTarget, (status) => status.state === "completed");
    expect(restartResult).toContain("已排定重启");
    expect(requests).toHaveLength(0);

    runGate.resolve(SUCCESS_RESULT);
    await waitUntil(() => requests.length === 1, "deferred restart after commit");
    expect(requests).toHaveLength(1);
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("/commit stop preserves an unborn-HEAD commit even when cancellation makes the CLI exit nonzero", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-stop-unborn-head" };
  const runGate = Promise.withResolvers<CommitResult>();
  const changedResult: CommitResult = { exitCode: 1, headBefore: null, headAfter: HEAD_C };
  try {
    const repo = join(harness.root, "unborn-stop-checkout");
    await mkdir(repo, { recursive: true });
    await runGit(repo, ["init", "-q", "-b", "main"]);
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => {
      runGate.resolve(changedResult);
      return true;
    };

    await send(harness, target, "commit-unborn-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "unborn commit run");
    await send(harness, target, "commit-unborn-stop", "/commit stop");
    await waitForStatus(harness.service, target, (status) => status.state === "cancelled");
    const reply = await waitForReply(
      harness,
      target,
      (text) => text.includes(HEAD_C.slice(0, 8)) && text.includes("停止"),
    );

    expect(reply).toContain(HEAD_C.slice(0, 8));
    expect(reply).not.toContain("已停止当前提交");
    expect(await latestTask(harness, target)).toMatchObject({ state: "cancelled" });
  } finally {
    runGate.resolve(changedResult);
    await closeCoreHarness(harness);
  }
});

test("/commit stop does not claim to stop a CLI run that already completed successfully", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-stop-completed-race" };
  const runGate = Promise.withResolvers<CommitResult>();
  const completedResult: CommitResult = { exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_C };
  try {
    const repo = await createRepo(harness.root, "stop-completed-race-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => {
      runGate.resolve(completedResult);
      return true;
    };

    await send(harness, target, "commit-completed-race-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, target, "commit-completed-race-stop", "/commit stop");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_C.slice(0, 8)));

    expect(reply).toContain("HEAD");
    expect(reply).not.toContain("已停止");
    expect(await latestTask(harness, target)).toMatchObject({ state: "completed" });
  } finally {
    runGate.resolve(completedResult);
    await closeCoreHarness(harness);
  }
});

test("/new and /move cannot acquire repository command leases during a commit", async () => {
  const { harness, commits } = await createCommitHarness();
  const commitTarget = { channelType: 1 as const, channelId: "dm-command-lease-commit" };
  const newTarget = { channelType: 1 as const, channelId: "dm-new-during-commit" };
  const moveTarget = { channelType: 1 as const, channelId: "dm-move-during-commit" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "command-lease-commit-repo");
    const destination = await createRepo(harness.root, "command-lease-move-destination");
    await bind(harness, commitTarget, repo);
    await bindWithSession(harness, moveTarget, repo, "move-during-commit-session");
    commits.runHandler = async () => runGate.promise;
    const priorCreations = harness.workers.creations.length;

    await send(harness, commitTarget, "command-lease-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, newTarget, "new-during-commit", `/new ${repo}`);
    await waitForReply(harness, newTarget, (text) => text.includes("相关会话") || text.includes("目录存在"));
    await send(harness, moveTarget, "move-during-commit", `/move ${destination}`);
    await waitForReply(harness, moveTarget, (text) => text.includes("相关会话") || text.includes("目标目录"));

    expect(harness.workers.creations).toHaveLength(priorCreations);
    expect(await withStore(harness, (store) => store.getBinding(harness.bot.uid, moveTarget))).toMatchObject({
      cwd: repo,
    });
    expect(commits.calls[0]?.stopCalls).toBe(0);
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("an active /new command lease blocks a concurrent commit to the same repository", async () => {
  const { harness, commits } = await createCommitHarness();
  const newTarget = { channelType: 1 as const, channelId: "dm-new-locks-repo" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-waits-new" };
  const createReached = Promise.withResolvers<void>();
  const createGate = Promise.withResolvers<void>();
  let newCommand: Promise<void> | undefined;
  try {
    const repo = await createRepo(harness.root, "new-command-active-repo");
    await bind(harness, commitTarget, repo);
    const originalCreate = harness.workers.create.bind(harness.workers);
    harness.workers.create = async (options) => {
      const worker = await originalCreate(options);
      createReached.resolve();
      await createGate.promise;
      return worker;
    };

    newCommand = send(harness, newTarget, "new-command-active", `/new ${repo}`);
    await createReached.promise;
    await send(harness, commitTarget, "commit-during-new", "/commit");
    await waitUntil(
      () => commits.calls.length > 0 || harness.transport.sent.some((message) =>
        message.target.channelId === commitTarget.channelId && message.text.includes("忙"),
      ),
      "commit admission while /new holds the repository",
    );

    expect(commits.calls).toHaveLength(0);
  } finally {
    createGate.resolve();
    await newCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("thread deletion cannot remove a legacy pinned worktree while another chat commits it", async () => {
  const toolFinished = Promise.withResolvers<string>();
  const prompt: FakePrompt = async ({ promptContext, text }) => {
    if (text !== "delete the legacy pinned thread") return;
    const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
    if (!management) throw new Error("management tool missing");
    toolFinished.resolve(await management.execute(
      { action: "thread-delete", short_id: "legacy-pinned" },
      new AbortController().signal,
    ));
  };
  const { harness, commits } = await createCommitHarness({ prompt });
  const commitTarget = { channelType: 1 as const, channelId: "dm-legacy-worktree-commit" };
  const groupTarget = { channelType: 2 as const, channelId: "group-a" };
  const threadTarget = { channelType: 5 as const, channelId: "group-a____legacy-pinned" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "legacy-worktree-delete-repo");
    const agentRepo = await createRepo(harness.root, "legacy-thread-delete-agent-repo");
    await bind(harness, commitTarget, repo);
    await bind(harness, threadTarget, repo, true);
    await bindWithSession(harness, groupTarget, agentRepo, "legacy-thread-delete-agent-session");
    if (!harness.worktrees) throw new Error("expected configured worktree removal");
    commits.runHandler = async () => runGate.promise;

    await send(harness, commitTarget, "legacy-thread-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, groupTarget, "legacy-thread-delete-tool", "delete the legacy pinned thread");
    const result = await toolFinished.promise;
    await waitForStatus(harness.service, groupTarget, (status) => status.state === "completed");

    expect(result).toContain("资源已保留");
    expect(harness.worktrees.removals).toHaveLength(0);
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("a commit is blocked by an unconfirmed ordinary worker recorded in a repository subdirectory", async () => {
  const entered = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const prompt: FakePrompt = async () => {
    entered.resolve();
    await releasePrompt.promise;
  };
  const { harness, commits } = await createCommitHarness({ prompt });
  const writerTarget = { channelType: 1 as const, channelId: "dm-unconfirmed-subdirectory-writer" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-after-unconfirmed-writer" };
  try {
    const repo = await createRepo(harness.root, "unconfirmed-subdirectory-repo");
    const child = join(repo, "nested");
    await mkdir(child, { recursive: true });
    await bindWithSession(harness, writerTarget, child, "unconfirmed-subdirectory-session");
    await bind(harness, commitTarget, repo);
    await send(harness, writerTarget, "unconfirmed-subdirectory-task", "hold the worker");
    await entered.promise;
    const worker = harness.workers.workers[0]!;
    worker.stop = async () => {
      releasePrompt.resolve();
      return false;
    };
    await send(harness, writerTarget, "cancel-unconfirmed-subdirectory-task", "/cancel");
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "cancelled");
    expect(worker.stopped).toBe(false);

    await send(harness, commitTarget, "commit-after-unconfirmed-subdirectory", "/commit");
    await waitForReply(harness, commitTarget, (text) => text.includes("忙") || text.includes("无法"));
    expect(commits.calls).toHaveLength(0);
  } finally {
    releasePrompt.resolve();
    await closeCoreHarness(harness);
  }
});

test("a failed Git root probe does not let a same-repository writer bypass an active commit", async () => {
  const { harness, commits } = await createCommitHarness();
  const commitTarget = { channelType: 1 as const, channelId: "dm-git-probe-commit" };
  const writerTarget = { channelType: 1 as const, channelId: "dm-git-probe-writer" };
  const runGate = Promise.withResolvers<CommitResult>();
  let configPath: string | undefined;
  let originalConfig: string | undefined;
  try {
    const repo = await createRepo(harness.root, "git-probe-commit-repo");
    const child = join(repo, "nested");
    await mkdir(child, { recursive: true });
    configPath = join(repo, ".git", "config");
    originalConfig = await readFile(configPath, "utf8");
    await bind(harness, commitTarget, repo);
    await bindWithSession(harness, writerTarget, child, "git-probe-writer-session");
    commits.runHandler = async () => runGate.promise;

    await send(harness, commitTarget, "git-probe-commit-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");

    const invalidConfig = `${originalConfig}\nnot a valid git config entry\n`;
    await writeFile(configPath, invalidConfig, "utf8");
    const probe = Bun.spawn(["git", "-C", child, "rev-parse", "--show-toplevel"], {
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(probe.stdout).text(),
      new Response(probe.stderr).text(),
      probe.exited,
    ]);
    expect(exitCode).not.toBe(0);
    expect(stderr).not.toMatch(/not a git repository/i);
    expect(stdout).toBe("");

    await send(harness, writerTarget, "git-probe-writer", "must not bypass the commit");
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "queued");
    expect(harness.workers.prompts).toHaveLength(0);

    runGate.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, commitTarget, (status) => status.state === "completed");
    await waitForStatus(harness.service, writerTarget, (status) => status.state === "failed");
    const reply = await waitForReply(
      harness,
      writerTarget,
      (text) => text.includes("Git 仓库状态无法确认"),
    );
    expect(reply).toContain("Git 仓库状态无法确认");
    expect(harness.workers.prompts).toHaveLength(0);
    await writeFile(configPath, originalConfig, "utf8");
  } finally {
    if (configPath !== undefined && originalConfig !== undefined) {
      await writeFile(configPath, originalConfig, "utf8");
    }
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("/commit stop is idempotent, stops only this commit, and leaves ordinary queued work intact", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-stop" };
  const runGate = Promise.withResolvers<CommitResult>();
  const stopGate = Promise.withResolvers<boolean>();
  try {
    const repo = await createRepo(harness.root, "stop-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => stopGate.promise;

    await send(harness, target, "commit-stop-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, target, "ordinary-queued-behind-commit", "keep this queued task");
    await waitForStatus(harness.service, target, (status) => status.queued === 1);

    const firstStop = send(harness, target, "commit-stop-first", "/commit stop");
    await waitUntil(() => commits.calls[0]!.stopCalls === 1, "first stop request");
    const secondStop = send(harness, target, "commit-stop-second", "/commit stop");
    await waitUntil(async () => {
      const store = await openChannelStore(join(harness.stateDir));
      try {
        return store.getCommandClaimState(harness.bot.uid, target, "commit-stop-second") === "pending";
      } finally {
        store.close();
      }
    }, "second stop claim");
    stopGate.resolve(true);
    runGate.resolve(STOPPED_RESULT);
    await Promise.all([firstStop, secondStop]);

    expect(commits.calls[0]?.stopCalls).toBe(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed" && status.queued === 0);
    expect(harness.workers.prompts.map(({ text }) => text)).toContain("keep this queued task");
    const stopReplies = harness.transport.sent.filter((message) =>
      message.target.channelId === target.channelId && message.text.includes("提交") && message.text.includes("停止"),
    );
    expect(stopReplies.length).toBeGreaterThanOrEqual(2);
  } finally {
    runGate.resolve(STOPPED_RESULT);
    stopGate.resolve(true);
    await closeCoreHarness(harness);
  }
});

test("a late stop does not hide a commit that already advanced HEAD", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-stop-race" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "stop-race-checkout");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => {
      runGate.resolve({ exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_C });
      return true;
    };

    await send(harness, target, "commit-race-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await send(harness, target, "commit-race-stop", "/commit stop");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_C.slice(0, 8)));
    expect(reply).toContain(HEAD_C.slice(0, 8));
    expect(await latestTask(harness, target)).toMatchObject({ state: "completed" });
  } finally {
    runGate.resolve(STOPPED_RESULT);
    await closeCoreHarness(harness);
  }
});

test("/commit stop never stops a different chat's commit", async () => {
  const { harness, commits } = await createCommitHarness();
  const first = { channelType: 1 as const, channelId: "dm-commit-stop-first-chat" };
  const second = { channelType: 1 as const, channelId: "dm-commit-stop-second-chat" };
  const firstRun = Promise.withResolvers<CommitResult>();
  const secondRun = Promise.withResolvers<CommitResult>();
  const firstStop = Promise.withResolvers<boolean>();
  try {
    const firstRepo = await createRepo(harness.root, "stop-first-repo");
    const secondRepo = await createRepo(harness.root, "stop-second-repo");
    await bind(harness, first, firstRepo);
    await bind(harness, second, secondRepo);
    commits.runHandler = async (call) => call.index === 0 ? firstRun.promise : secondRun.promise;
    commits.stopHandler = async (call) => call.index === 0 ? firstStop.promise : true;

    await send(harness, first, "commit-first-chat", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "first commit run");
    await send(harness, second, "commit-second-chat", "/commit");
    await waitUntil(() => commits.calls.length === 2 && commits.calls[1]!.runCalls === 1, "second commit run");

    const stop = send(harness, first, "stop-first-chat", "/commit stop");
    await waitUntil(() => commits.calls[0]!.stopCalls === 1, "first chat stop");
    firstStop.resolve(true);
    firstRun.resolve(STOPPED_RESULT);
    await stop;
    expect(commits.calls[1]?.stopCalls).toBe(0);
    expect(await harness.service.status(second)).toMatchObject({ state: "running" });

    secondRun.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, second, (status) => status.state === "completed");
    expect(await latestTask(harness, second)).toMatchObject({ state: "completed" });
  } finally {
    firstStop.resolve(true);
    firstRun.resolve(STOPPED_RESULT);
    secondRun.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("stop without an active commit reports idle without creating a task", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-stop-idle" };
  try {
    const repo = await createRepo(harness.root, "idle-stop-checkout");
    await bind(harness, target, repo);
    await send(harness, target, "stop-idle", "/commit stop");
    await waitForReply(harness, target, (text) => text.includes("没有") && text.includes("提交"));
    expect(await latestTask(harness, target)).toBeNull();
    expect(commits.calls).toHaveLength(0);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/commit stop does not interrupt a normal task in the same chat", async () => {
  const entered = Promise.withResolvers<void>();
  const stopped = Promise.withResolvers<void>();
  const prompt: FakePrompt = async ({ onStop }) => {
    entered.resolve();
    onStop(stopped.resolve);
    await stopped.promise;
  };
  const { harness, commits } = await createCommitHarness({ prompt });
  const target = { channelType: 1 as const, channelId: "dm-commit-stop-normal-task" };
  try {
    const repo = await createRepo(harness.root, "stop-normal-task-checkout");
    await bind(harness, target, repo);
    await send(harness, target, "normal-task-before-stop", "ordinary task");
    await entered.promise;

    await send(harness, target, "stop-normal-task", "/commit stop");
    await waitForReply(harness, target, (text) => text.includes("没有") && text.includes("提交"));
    expect(await harness.service.status(target)).toMatchObject({ state: "running" });
    expect(await latestTask(harness, target)).toMatchObject({ text: "ordinary task", state: "running" });
    expect(harness.workers.workers[0]?.stopped).toBe(false);
    expect(commits.calls).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("stopping during native worker creation prevents run and confirms the starting process stopped", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-starting-stop" };
  const createReached = Promise.withResolvers<void>();
  const createGate = Promise.withResolvers<void>();
  try {
    const repo = await createRepo(harness.root, "starting-stop-checkout");
    await bind(harness, target, repo);
    commits.createHandler = async () => {
      createReached.resolve();
      await createGate.promise;
    };

    await send(harness, target, "commit-starting", "/commit");
    await createReached.promise;
    const stop = send(harness, target, "commit-starting-stop", "/commit stop");
    await waitUntil(
      () => withStore(harness, (store) =>
        store.getCommandClaimState(harness.bot.uid, target, "commit-starting-stop") !== null,
      ),
      "starting-stage stop command claim",
    );
    createGate.resolve();
    await stop;
    await waitForStatus(harness.service, target, (status) => status.state === "cancelled");
    expect(commits.calls[0]?.runCalls).toBe(0);
    expect(commits.calls[0]?.stopCalls).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    createGate.resolve();
    await closeCoreHarness(harness);
  }
});

test("shutdown fail-closes an in-flight commit and restart never replays its command", async () => {
  const first = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-restart" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(first.harness.root, "restart-checkout");
    await bind(first.harness, target, repo);
    first.commits.runHandler = async () => runGate.promise;
    first.commits.stopHandler = async () => {
      runGate.resolve(STOPPED_RESULT);
      return true;
    };

    await send(first.harness, target, "commit-before-shutdown", "/commit");
    await waitUntil(
      () => first.commits.calls.length === 1 && first.commits.calls[0]!.runCalls === 1,
      "commit run before shutdown",
    );
  } finally {
    await closeCoreHarness(first.harness, { remove: false });
  }

  const rowAfterStop = await withStore(first.harness, (store) => store.getLatestTask(first.harness.bot.uid, target));
  expect(rowAfterStop?.state).toBe("running");
  expect(await taskMarkers(first.harness, rowAfterStop!.id)).toEqual({ prompt_started: 1, prompt_accepted: 0 });

  const restarted = await createCommitHarness({ root: first.harness.root, bot: first.harness.bot });
  try {
    await waitForStatus(restarted.harness.service, target, (status) => status.state === "interrupted");
    expect(await latestTask(restarted.harness, target)).toMatchObject({ state: "interrupted", text: "/commit" });
    expect(restarted.commits.calls).toHaveLength(0);
    expect(restarted.harness.workers.prompts).toHaveLength(0);
    expect(restarted.harness.workers.creations).toHaveLength(0);
    expect(harnessTexts(restarted.harness, target).join("\n")).toContain("无法确认 OMP 是否已接受");
  } finally {
    await closeCoreHarness(restarted.harness);
  }
});
test("an undispatched durable commit stays queued when restart finds an unconfirmed worker in its worktree", async () => {
  const first = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-recovery-blocked" };
  let repo: string;
  try {
    repo = await createRepo(first.harness.root, "recovery-blocked-commit-repo");
    await bind(first.harness, target, repo);
  } finally {
    await closeCoreHarness(first.harness, { remove: false });
  }

  const taskId = "undispatched-commit-recovery";
  const messageId = "undispatched-commit-message";
  const sessionId = `session-${target.channelType}-${target.channelId}`;
  await withStore(first.harness, (store) => {
    store.recordWorker({
      id: "unconfirmed-ordinary-worker",
      botUid: first.harness.bot.uid,
      sessionId: "unconfirmed-worker-session",
      sessionFile: join(repo, "unknown-session.jsonl"),
      pid: 4242,
      pgid: 4242,
      identity: "fixture-unconfirmed-worker",
      cwd: repo,
      moveTargetCwd: null,
      state: "running",
    });
    const claim = {
      botUid: first.harness.bot.uid,
      target,
      messageId,
      senderId: "owner-1",
      text: "/commit",
      supported: true,
      kind: "command" as const,
    };
    expect(store.claimCommand(claim)).toBe(true);
    expect(store.acceptCommitTask(claim, {
      id: taskId,
      botUid: first.harness.bot.uid,
      ...target,
      messageId,
      senderId: "owner-1",
      text: "/commit",
      sessionId,
      cwd: repo,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relaySuppressed: false,
    })).toBe(true);
  });

  const restarted = await createCommitHarness({ root: first.harness.root, bot: first.harness.bot });
  try {
    await waitForStatus(restarted.harness.service, target, (status) => status.state === "queued");
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    expect(await latestTask(restarted.harness, target)).toMatchObject({
      id: taskId,
      state: "queued",
      text: "/commit",
    });
    expect(await taskMarkers(restarted.harness, taskId)).toEqual({ prompt_started: 0, prompt_accepted: 0 });
    expect(restarted.commits.calls).toHaveLength(0);
    expect(restarted.harness.workers.prompts).toHaveLength(0);
    expect(restarted.harness.workers.creations).toHaveLength(0);
  } finally {
    await closeCoreHarness(restarted.harness);
  }
});

test("a recovered undispatched commit runs after it reacquires its own session and repository leases", async () => {
  const first = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-recovery-runs" };
  let repo: string;
  try {
    repo = await createRepo(first.harness.root, "recovery-runs-commit-repo");
    await bind(first.harness, target, repo);
  } finally {
    await closeCoreHarness(first.harness, { remove: false });
  }

  const taskId = "undispatched-commit-recovery-runs";
  const messageId = "undispatched-commit-recovery-runs-message";
  const sessionId = `session-${target.channelType}-${target.channelId}`;
  await withStore(first.harness, (store) => {
    const claim = {
      botUid: first.harness.bot.uid,
      target,
      messageId,
      senderId: "owner-1",
      text: "/commit",
      supported: true,
      kind: "command" as const,
    };
    expect(store.claimCommand(claim)).toBe(true);
    expect(store.acceptCommitTask(claim, {
      id: taskId,
      botUid: first.harness.bot.uid,
      ...target,
      messageId,
      senderId: "owner-1",
      text: "/commit",
      sessionId,
      cwd: repo,
      state: "queued",
      resultText: null,
      errorText: null,
      messageSeq: 0,
      historyEligible: false,
      relaySuppressed: false,
    })).toBe(true);
  });

  const restarted = await createCommitHarness({ root: first.harness.root, bot: first.harness.bot });
  try {
    await waitForStatus(restarted.harness.service, target, (status) => status.state === "completed");
    expect(await latestTask(restarted.harness, target)).toMatchObject({
      id: taskId,
      state: "completed",
      text: "/commit",
    });
    expect(restarted.commits.calls).toHaveLength(1);
    expect(restarted.commits.calls[0]?.runCalls).toBe(1);
  } finally {
    await closeCoreHarness(restarted.harness);
  }
});


function harnessTexts(harness: CoreHarness, target: ChatTarget): string[] {
  return harness.transport.sent
    .filter((message) => message.target.channelType === target.channelType && message.target.channelId === target.channelId)
    .map(({ text }) => text);
}

test("commit stop releases a pending host merge only after native commit settlement", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 5 as const, channelId: "group-a____commit-before-host-merge" };
  const runGate = Promise.withResolvers<CommitResult>();
  let merge: Promise<void> | undefined;
  let mergeTypingSent = false;
  try {
    const repo = await createRepo(harness.root, "host-merge-after-commit-source");
    const workspace = await addLinkedWorktree(repo, join(harness.root, "host-merge-after-commit"), "host-merge-after-commit");
    await bind(harness, target, workspace, true);
    commits.runHandler = async () => runGate.promise;
    commits.stopHandler = async () => {
      runGate.resolve(STOPPED_RESULT);
      return true;
    };
    await send(harness, target, "commit-before-host-merge", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "native commit run");

    const worktrees = harness.worktreePort!;
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      expect(commits.calls[0]!.stopCalls).toBeGreaterThan(0);
      expect((await latestTask(harness, target))?.state).toBe("cancelled");
      return mergeWorktree(request, signal);
    };
    const sendTyping = harness.transport.sendTyping.bind(harness.transport);
    harness.transport.sendTyping = async (chat, signal) => {
      await sendTyping(chat, signal);
      if (chat.channelId === target.channelId) mergeTypingSent = true;
    };
    merge = send(harness, target, "host-merge-after-commit", "/thread merge");
    await waitUntil(() => mergeTypingSent, "host merge reservation");
    expect(harness.worktrees!.merges).toEqual([]);

    await send(harness, target, "stop-before-host-merge", "/commit stop");
    await merge;
    await waitForStatus(harness.service, target, (status) => status.state === "cancelled" && status.outbox === 0);
    expect(harness.worktrees!.merges).toHaveLength(1);
    expect(harness.worktrees!.merges[0]).toMatchObject({ cwd: workspace, target: "main" });
    expect(harness.workers.prompts).toEqual([]);
  } finally {
    runGate.resolve(STOPPED_RESULT);
    await merge;
    await closeCoreHarness(harness);
  }
});

test("a host merge and another chat's commit can run on the same linked worktree", async () => {
  const { harness, commits } = await createCommitHarness();
  const mergeTarget = { channelType: 5 as const, channelId: "group-a____host-merge-before-commit" };
  const commitTarget = { channelType: 1 as const, channelId: "dm-commit-during-host-merge" };
  const finishMerge = Promise.withResolvers<void>();
  let merge: Promise<void> | undefined;
  let mergeStarted = false;
  try {
    const repo = await createRepo(harness.root, "commit-during-host-merge-source");
    const workspace = await addLinkedWorktree(repo, join(harness.root, "commit-during-host-merge"), "commit-during-host-merge");
    const child = join(workspace, "src");
    await mkdir(child);
    // The merge does not reserve the linked worktree against the independent commit.
    await bind(harness, mergeTarget, workspace, true);
    await bind(harness, commitTarget, child);
    const worktrees = harness.worktreePort!;
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request, signal) => {
      mergeStarted = true;
      await finishMerge.promise;
      return mergeWorktree(request, signal);
    };

    merge = send(harness, mergeTarget, "host-merge-before-commit", "/thread merge");
    await waitUntil(() => mergeStarted, "host merge run");
    await send(harness, commitTarget, "commit-during-host-merge", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "commit during host merge");
    await waitForStatus(harness.service, commitTarget, (status) => status.state === "completed");

    finishMerge.resolve();
    await merge;
    expect(harness.worktrees!.merges).toHaveLength(1);
    expect(harness.worktrees!.merges[0]).toMatchObject({ cwd: workspace, target: "main" });
  } finally {
    finishMerge.resolve();
    await merge;
    await closeCoreHarness(harness);
  }
});

test("a rejected pending commit creation releases stop and shutdown waiters", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-commit-create-reject-stop" };
  const createReached = Promise.withResolvers<void>();
  const createGate = Promise.withResolvers<void>();
  let shutdown: Promise<void> | undefined;
  let stopped = false;
  try {
    const repo = await createRepo(harness.root, "commit-create-reject-stop");
    await bind(harness, target, repo);
    harness.workers.recovered = true;
    commits.createHandler = async () => {
      createReached.resolve();
      await createGate.promise;
      throw new Error("fixture native commit creation failed");
    };

    await send(harness, target, "commit-create-reject-start", "/commit");
    await createReached.promise;
    const stopCommand = send(harness, target, "commit-create-reject-stop", "/commit stop");
    await waitUntil(
      () => withStore(harness, (store) =>
        store.getCommandClaimState(harness.bot.uid, target, "commit-create-reject-stop") === "pending",
      ),
      "commit stop command claim while creation is pending",
    );
    const nextTurn = Promise.withResolvers<void>();
    setImmediate(nextTurn.resolve);
    await nextTurn.promise;
    const shutdownPromise = harness.service.stop();
    shutdown = shutdownPromise;
    createGate.reject(new Error("release pending native commit creation"));

    let shutdownSettled = false;
    let shutdownError: unknown;
    void Promise.all([stopCommand, shutdownPromise]).then(
      () => {
        shutdownSettled = true;
      },
      (error: unknown) => {
        shutdownError = error;
        shutdownSettled = true;
      },
    );
    await waitUntil(() => shutdownSettled, "commit stop, task, and shutdown settlement");
    expect(shutdownError).toBeUndefined();
    expect(commits.calls[0]?.runCalls).toBe(0);
    expect(harness.workers.prompts).toHaveLength(0);
    await closeCoreHarness(harness);
    stopped = true;
  } finally {
    createGate.reject(new Error("release pending native commit creation"));
    if (!stopped) {
      shutdown ??= harness.service.stop();
      void shutdown.catch(() => {});
      await rm(harness.root, { recursive: true, force: true });
    }
  }
});

test("a deferred restart waits for the host merge result receipt", async () => {
  const requestsAtReceiptDelivery: boolean[] = [];
  const mergeStarted = Promise.withResolvers<void>();
  const finishMerge = Promise.withResolvers<void>();
  const restartScheduled = Promise.withResolvers<void>();
  let runningTargetsDuringMerge: string[] = [];
  let restartResult = "";
  const prompt: FakePrompt = async ({ promptContext }) => {
    const tool = (promptContext?.tools ?? []).find(({ name }) => name === "channel_control");
    if (!tool) throw new Error("channel_control host tool missing");
    const status = JSON.parse(
      await tool.execute({ action: "status" }, new AbortController().signal),
    ) as { running_targets: string[] };
    runningTargetsDuringMerge = status.running_targets;
    restartResult = await tool.execute({ action: "restart" }, new AbortController().signal);
    restartScheduled.resolve();
  };
  let currentHarness: CoreHarness | undefined;
  const { harness } = await createCommitHarness({
    prompt,
    restart: {
      request: async () => {
        requestsAtReceiptDelivery.push(
          currentHarness?.transport.sent.some(({ target, text }) =>
            target.channelType === 5
            && target.channelId === "group-allowed____host-restart-deferred"
            && text.startsWith("子区合并成功："),
          ) ?? false,
        );
      },
    },
  });
  currentHarness = harness;
  const thread = { channelType: 5 as const, channelId: "group-allowed____host-restart-deferred" };
  const agent = { channelType: 1 as const, channelId: "dm-host-restart-deferred" };
  const workspace = join(harness.root, "host-restart-workspace");
  let mergeCommand: Promise<void> | undefined;
  try {
    const repo = await createRepo(harness.root, "host-restart-agent-repo");
    await mkdir(workspace);
    await bind(harness, thread, workspace, true);
    await bindWithSession(harness, agent, repo, "host-restart-agent-session");

    const worktrees = harness.worktrees!;
    const mergeWorktree = worktrees.merge.bind(worktrees);
    worktrees.merge = async (request) => {
      mergeStarted.resolve();
      await finishMerge.promise;
      return mergeWorktree(request);
    };

    mergeCommand = send(harness, thread, "host-restart-merge", "/thread merge");
    await mergeStarted.promise;
    await send(harness, agent, "host-restart-agent", "schedule restart during host merge");
    await restartScheduled.promise;
    await waitForStatus(harness.service, agent, (status) => status.state === "completed");

    expect(restartResult).toContain("已排定重启");
    expect(runningTargetsDuringMerge).toContain("5:group-allowed____host-restart-deferred");
    expect(requestsAtReceiptDelivery).toHaveLength(0);

    finishMerge.resolve();
    await mergeCommand;
    await waitForReply(
      harness,
      thread,
      (text) => text.startsWith("子区合并成功："),
    );
    await waitUntil(() => requestsAtReceiptDelivery.length === 1, "deferred restart after merge receipt");
    expect(requestsAtReceiptDelivery).toEqual([true]);
  } finally {
    finishMerge.resolve();
    await mergeCommand?.catch(() => {});
    await closeCoreHarness(harness);
  }
});

test("queued slash text matching the commit command remains an ordinary OMP prompt", async () => {
  const { harness, commits } = await createCommitHarness();
  const target = { channelType: 1 as const, channelId: "dm-queued-commit-text" };
  try {
    const repo = await createRepo(harness.root, "queued-commit-text-repo");
    await bind(harness, target, repo);

    await send(harness, target, "queue-commit-looking-text", "/queue /commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["/commit"]);
    expect(commits.calls).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a commit opens no progress card until the native worker reports its first stage", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-lazy" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "commit-card-lazy-repo");
    await bind(harness, target, repo);
    commits.runHandler = async () => runGate.promise;

    await send(harness, target, "commit-card-lazy-start", "/commit");
    await waitUntil(() => commits.calls.length === 1 && commits.calls[0]!.runCalls === 1, "commit run");
    await flushMicrotasks();
    expect(transport.progressSends).toHaveLength(0);

    commits.calls[0]!.emitProgress("正在提交");
    await waitUntil(() => transport.progressSends.length === 1, "first commit card");
    expect(transport.progressSends[0]!.target).toEqual(target);
    expect(transport.progressSends[0]!.progress.commit?.summary).toBe("正在提交");
    expect(transport.progressSends[0]!.progress.state).toBe("running");

    runGate.resolve(SUCCESS_RESULT);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    await closeCoreHarness(harness);
  }
});

test("one commit card receives merged stages, the real HEAD result, and native commit content", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-success" };
  const runGate = Promise.withResolvers<CommitResult>();
  const message = "feat: add compact commit cards\n\nKeep the title and body from the native commit.";
  const changelog = "### Added\n\n- Show the commit message and changelog entry.";
  const commitResult: CommitResult = {
    ...SUCCESS_RESULT,
    content: { message, changelog },
  };
  try {
    const repo = await createRepo(harness.root, "commit-card-success-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      call.emitProgress("正在创建提交");
      await runGate.promise;
      return commitResult;
    };

    vi.useFakeTimers();
    await send(harness, target, "commit-card-success-start", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "commit run");
    await waitUntil(() => transport.progressSends.length === 1, "commit card send");
    await flushMicrotasks();

    vi.advanceTimersByTime(1_000);
    await waitUntil(() => transport.progressEdits.length >= 1, "merged commit card edit");
    const first = transport.progressSends[0]!;
    const merged = transport.progressEdits[0]!;
    expect(merged.messageId).toBe(first.messageId);
    expect(merged.progress.state).toBe("running");
    expect(merged.progress.commit?.recentSteps).toEqual(["正在提交", "正在创建提交"]);

    runGate.resolve(commitResult);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => latestProgress(transport)?.commit?.result !== undefined, "terminal commit card");
    const terminal = transport.progressEdits.at(-1)!;
    expect(terminal.messageId).toBe(first.messageId);
    expect(terminal.progress.state).toBe("completed");
    expect(terminal.progress.commit?.result).toContain(HEAD_B.slice(0, 8));
    expect(terminal.progress.commit?.content).toEqual({ message, changelog });
    expect(renderedProgressText(terminal.progress)).toContain(message);
    expect(renderedProgressText(terminal.progress)).toContain(changelog);
    expect(renderedProgressText(terminal.progress)).toContain(HEAD_B.slice(0, 8));
    expect(terminal.progress.tools).toEqual([]);
    expect(terminal.progress.todos).toEqual([]);
    expect(transport.progressSends).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.workers.creations).toHaveLength(0);
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));
    expect(reply).toContain(HEAD_B.slice(0, 8));
    const task = await latestTask(harness, target);
    expect(task?.state).toBe("completed");
    expect(await taskMarkers(harness, task!.id)).toEqual({ prompt_started: 1, prompt_accepted: 0 });
  } finally {
    runGate.resolve(commitResult);
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a silent native commit refreshes elapsed time without adding stages", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-heartbeat" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "commit-card-heartbeat-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      await runGate.promise;
      return SUCCESS_RESULT;
    };

    vi.useFakeTimers();
    await send(harness, target, "commit-card-heartbeat-start", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "commit run");
    await waitUntil(() => transport.progressSends.length === 1, "commit card send");
    await flushMicrotasks();
    const editsBeforeRefresh = transport.progressEdits.length;

    vi.advanceTimersByTime(11_000);
    await waitUntil(
      () => transport.progressEdits.length > editsBeforeRefresh,
      "silent commit elapsed refresh",
    );
    const refreshed = transport.progressEdits.at(-1)!;
    expect(refreshed.progress.state).toBe("running");
    expect(refreshed.progress.commit?.summary).toBe("正在提交");
    expect(refreshed.progress.commit?.recentSteps).toEqual(["正在提交"]);
    expect(refreshed.progress.elapsedMs).toBeGreaterThanOrEqual(10_000);
    expect(renderedProgressText(refreshed.progress)).toContain(
      formatDuration(refreshed.progress.elapsedMs),
    );
    expect(transport.progressSends).toHaveLength(1);
  } finally {
    runGate.resolve(SUCCESS_RESULT);
    vi.useRealTimers();
    await closeCoreHarness(harness);
  }
});

test("a commit with no new HEAD completes its card without claiming a creation", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-no-change" };
  try {
    const repo = await createRepo(harness.root, "commit-card-no-change-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在创建提交");
      return { exitCode: 0, headBefore: HEAD_A, headAfter: HEAD_A };
    };

    await send(harness, target, "commit-card-no-change-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const reply = await waitForReply(
      harness,
      target,
      (text) => text.includes(HEAD_A.slice(0, 8)) && text.includes("没有新提交"),
    );
    expect(reply).toContain(HEAD_A.slice(0, 8));

    await waitUntil(() => latestProgress(transport)?.commit?.result !== undefined, "no-change card result");
    const final = latestProgress(transport)!;
    expect(final.state).toBe("completed");
    expect(final.commit?.result).toContain("没有新提交");
    expect(final.commit?.result).toContain(HEAD_A.slice(0, 8));
    expect(await latestTask(harness, target)).toMatchObject({ state: "completed" });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed native commit that advanced HEAD keeps that outcome on the card", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-failed-head" };
  try {
    const repo = await createRepo(harness.root, "commit-card-failed-head-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在创建提交");
      return { exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_B };
    };

    await send(harness, target, "commit-card-failed-head-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "failed");
    const reply = await waitForReply(
      harness,
      target,
      (text) => text.includes(HEAD_B.slice(0, 8)) && text.includes("失败"),
    );
    expect(reply).toMatch(/保留|不会回滚/u);

    await waitUntil(() => latestProgress(transport)?.commit?.result !== undefined, "failed card result");
    const final = latestProgress(transport)!;
    expect(final.state).toBe("failed");
    expect(final.commit?.result).toContain("失败");
    expect(final.commit?.result).toContain(HEAD_B.slice(0, 8));
    expect(renderedProgressText(final)).not.toContain("已停止");
    expect(await latestTask(harness, target)).toMatchObject({ state: "failed" });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/commit stop settles the card from the observed HEAD, not from the request alone", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-stopped" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "commit-card-stopped-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      return runGate.promise;
    };
    commits.stopHandler = async () => {
      runGate.resolve({ exitCode: 130, headBefore: HEAD_A, headAfter: HEAD_B });
      return true;
    };

    await send(harness, target, "commit-card-stopped-start", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "commit run");
    await send(harness, target, "commit-card-stopped-stop", "/commit stop");
    await waitForStatus(harness.service, target, (status) => status.state === "cancelled");
    const reply = await waitForReply(
      harness,
      target,
      (text) => text.includes(HEAD_B.slice(0, 8)) && text.includes("停止"),
    );
    expect(reply).not.toContain("已完成");

    await waitUntil(() => latestProgress(transport)?.commit?.result !== undefined, "stopped card result");
    const final = latestProgress(transport)!;
    expect(final.state).toBe("cancelled");
    expect(final.commit?.result).toContain("停止");
    expect(final.commit?.result).toContain(HEAD_B.slice(0, 8));
    expect(await latestTask(harness, target)).toMatchObject({ state: "cancelled" });
  } finally {
    runGate.resolve({ exitCode: 130, headBefore: HEAD_A, headAfter: HEAD_A });
    await closeCoreHarness(harness);
  }
});

test("an unconfirmed stop never lets the commit card claim it stopped", async () => {
  const transport = new CommitProgressTransport();
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-unknown-stop" };
  const runGate = Promise.withResolvers<CommitResult>();
  try {
    const repo = await createRepo(harness.root, "commit-card-unknown-stop-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      return runGate.promise;
    };
    commits.stopHandler = async () => false;

    await send(harness, target, "commit-card-unknown-stop-start", "/commit");
    await waitUntil(() => commits.calls[0]?.runCalls === 1, "commit run");
    await send(harness, target, "commit-card-unknown-stop-stop", "/commit stop");
    await waitForReply(harness, target, (text) => text.includes("无法确认"));

    runGate.resolve({ exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_A });
    await waitUntil(
      () => latestProgress(transport)?.commit?.result !== undefined,
      "unknown commit card result",
    );
    const final = latestProgress(transport)!;
    expect(final.state).not.toBe("running");
    expect(final.commit?.result).toMatch(/未知|无法确认/u);
    expect(renderedProgressText(final)).not.toContain("已停止");
    expect(await latestTask(harness, target)).not.toMatchObject({ state: "cancelled" });
  } finally {
    runGate.resolve({ exitCode: 1, headBefore: HEAD_A, headAfter: HEAD_A });
    await closeCoreHarness(harness);
  }
});

test("an unavailable progress card never blocks the native commit result text", async () => {
  const transport = new CommitProgressTransport();
  transport.progressSendHandler = async () => null;
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-unavailable" };
  try {
    const repo = await createRepo(harness.root, "commit-card-unavailable-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      return SUCCESS_RESULT;
    };

    await send(harness, target, "commit-card-unavailable-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => transport.progressSends.length >= 1, "unavailable card send attempt");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));

    expect(reply).toContain(HEAD_B.slice(0, 8));
    expect(transport.progressSends.length).toBeGreaterThanOrEqual(1);
    expect(transport.progressEdits).toHaveLength(0);
    expect(commits.calls[0]?.runCalls).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a throwing progress card send never blocks the native commit result text", async () => {
  const transport = new CommitProgressTransport();
  transport.progressSendHandler = async () => {
    throw new Error("fixture commit card send unavailable");
  };
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-send-failure" };
  try {
    const repo = await createRepo(harness.root, "commit-card-send-failure-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      return SUCCESS_RESULT;
    };

    await send(harness, target, "commit-card-send-failure-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => transport.progressSends.length >= 1, "throwing card send attempt");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));

    expect(reply).toContain(HEAD_B.slice(0, 8));
    expect(transport.progressSends.length).toBeGreaterThanOrEqual(1);
    expect(transport.progressEdits).toHaveLength(0);
    expect(commits.calls[0]?.runCalls).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failing progress card edit never blocks the native commit result text", async () => {
  const transport = new CommitProgressTransport();
  transport.progressEditHandler = async () => {
    throw new Error("fixture commit card edit unavailable");
  };
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-edit-failure" };
  try {
    const repo = await createRepo(harness.root, "commit-card-edit-failure-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      return SUCCESS_RESULT;
    };

    await send(harness, target, "commit-card-edit-failure-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    await waitUntil(() => transport.progressEdits.length >= 1, "failing card edit attempt");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));

    expect(reply).toContain(HEAD_B.slice(0, 8));
    expect(transport.progressSends).toHaveLength(1);
    expect(transport.progressEdits.length).toBeGreaterThanOrEqual(1);
    expect(commits.calls[0]?.runCalls).toBe(1);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a pending progress card send does not hold the native commit or its text receipt", async () => {
  const transport = new CommitProgressTransport();
  const releaseSend = Promise.withResolvers<SendReceipt | null>();
  const sendStarted = Promise.withResolvers<void>();
  transport.progressSendHandler = async () => {
    sendStarted.resolve();
    return releaseSend.promise;
  };
  const { harness, commits } = await createCommitHarness({ transport });
  const target = { channelType: 1 as const, channelId: "dm-commit-card-pending-send" };
  let runFinished = false;
  try {
    const repo = await createRepo(harness.root, "commit-card-pending-send-repo");
    await bind(harness, target, repo);
    commits.runHandler = async (call) => {
      call.emitProgress("正在提交");
      await sendStarted.promise;
      runFinished = true;
      return SUCCESS_RESULT;
    };

    await send(harness, target, "commit-card-pending-send-start", "/commit");
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const reply = await waitForReply(harness, target, (text) => text.includes(HEAD_B.slice(0, 8)));

    expect(runFinished).toBe(true);
    expect(reply).toContain(HEAD_B.slice(0, 8));
    expect(commits.calls[0]?.runCalls).toBe(1);
    expect(transport.progressSends.length).toBeGreaterThanOrEqual(1);
    expect(transport.progressEdits).toHaveLength(0);

    // The card request may resolve (or hit its own bound) later; the task and its receipt were
    // already delivered above and must not wait for it.
    releaseSend.resolve({ messageId: "held-commit-card" });
    await flushMicrotasks();
  } finally {
    releaseSend.resolve(null);
    await closeCoreHarness(harness);
  }
});
