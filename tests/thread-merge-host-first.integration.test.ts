import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, SendReceipt } from "../src/contracts.ts";
import type { ThreadWorkspaceRecord } from "../src/store/store-records.ts";
import { openChannelStore } from "../src/store/index.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  type CoreHarness,
  type FakePrompt,
} from "./fixtures/core-fakes.ts";

const GROUP_ID = "group-allowed";
/** wt-compatible fixture; the merge stages below call real Git and do not claim to run Worktrunk. */
const WT_PIPELINE_FIXTURE = String.raw`if [ "$1" = "--version" ]; then printf 'wt v0.80.0\n'; exit 0; fi
if [ "$1" = "config" ] && [ "$2" = "state" ] && [ "$3" = "default-branch" ]; then
  printf 'main\n'
  exit 0
fi
[ "$1" = "merge" ] || exit 64
branch=$(git branch --show-current)
target=main
if [ -n "$4" ]; then target="$4"; fi
common_dir=$(git rev-parse --path-format=absolute --git-common-dir)
repo=$(dirname "$common_dir")
worktree=$(pwd -P)
git add -A
base=$(git merge-base "$target" HEAD)
git reset --soft "$base"
if ! git diff --cached --quiet; then git commit -qm 'squash thread merge'; fi
git rebase "$target" >&2 || exit $?
if [ -x "$repo/.worktrunk-pre-merge" ]; then "$repo/.worktrunk-pre-merge" || exit $?; fi
git -C "$repo" merge --ff-only "$branch" >&2 || exit $?
git -C "$repo" worktree remove "$worktree" || exit $?
git -C "$repo" branch -d "$branch" >/dev/null || exit $?
printf '{"branch":"%s","target":"%s","squashed":true,"rebased":true,"committed":true,"removed":true}\n' "$branch" "$target"`;

type MergeScenario = "success" | "conflict" | "hook";
type MergeEvent =
  | { kind: "send"; target: ChatTarget; text: string }
  | { kind: "archive"; shortId: string };

class MergeTracingTransport extends FakeTransport {
  readonly events: MergeEvent[] = [];

  override async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const receipt = await super.sendText(target, text, clientMsgNo, signal);
    this.events.push({ kind: "send", target: { ...target }, text });
    return receipt;
  }

  override async archiveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    this.events.push({ kind: "archive", shortId });
    await super.archiveThread(groupId, shortId, signal);
  }
}

interface MergeWorld {
  readonly root: string;
  readonly repo: string;
  readonly worktree: string;
  readonly branch: string;
  readonly base: string;
  readonly thread: ChatTarget;
  readonly hookFailureMarker?: string;
  readonly hookRuns?: string;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
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
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function createMergeWorld(root: string, scenario: MergeScenario): Promise<MergeWorld> {
  const repo = join(root, "source-repo");
  const worktree = join(root, "thread-worktree");
  const branch = `thread-${scenario}`;
  const thread = { channelId: `${GROUP_ID}____${scenario}`, channelType: 5 as const };
  await mkdir(repo, { recursive: true });
  await runGit(repo, ["init", "-q", "-b", "main"]);
  await runGit(repo, ["config", "user.name", "Merge Fixture"]);
  await runGit(repo, ["config", "user.email", "merge-fixture@example.test"]);
  await writeFile(join(repo, "shared.txt"), "base\n", "utf8");
  await runGit(repo, ["add", "."]);
  await runGit(repo, ["commit", "-q", "-m", "base"]);
  const base = await runGit(repo, ["rev-parse", "HEAD"]);
  await runGit(repo, ["worktree", "add", "-q", "-b", branch, worktree, "main"]);

  if (scenario === "conflict") {
    await writeFile(join(worktree, "shared.txt"), "thread version\n", "utf8");
    await runGit(worktree, ["add", "shared.txt"]);
    await runGit(worktree, ["commit", "-q", "-m", "thread edit"]);
    await writeFile(join(repo, "shared.txt"), "main version\n", "utf8");
    await runGit(repo, ["add", "shared.txt"]);
    await runGit(repo, ["commit", "-q", "-m", "main edit"]);
  } else {
    await writeFile(join(worktree, "feature-one.txt"), "one\n", "utf8");
    await runGit(worktree, ["add", "feature-one.txt"]);
    await runGit(worktree, ["commit", "-q", "-m", "feature one"]);
    await writeFile(join(worktree, "feature-two.txt"), "two\n", "utf8");
    await runGit(worktree, ["add", "feature-two.txt"]);
    await runGit(worktree, ["commit", "-q", "-m", "feature two"]);
  }

  if (scenario === "hook") {
    const hookFailureMarker = join(root, "fail-pre-merge-hook");
    const hookRuns = join(root, "pre-merge-hook-runs");
    const hook = join(repo, ".worktrunk-pre-merge");
    await writeFile(hookFailureMarker, "fail once\n", "utf8");
    await writeFile(
      hook,
      `#!/bin/sh\nprintf 'run\\n' >> ${shellQuote(hookRuns)}\nif [ -e ${shellQuote(hookFailureMarker)} ]; then\n  echo 'pre-merge hook blocked by fixture marker' >&2\n  exit 17\nfi\n`,
      "utf8",
    );
    await chmod(hook, 0o700);
    return { root, repo: await realpath(repo), worktree, branch, base, thread, hookFailureMarker, hookRuns };
  }
  return { root, repo: await realpath(repo), worktree, branch, base, thread };
}

async function writeWorktrunkFixture(root: string): Promise<string> {
  const wt = join(root, "wt-fixture");
  await writeFile(wt, `#!/bin/sh\n${WT_PIPELINE_FIXTURE}\n`, "utf8");
  await chmod(wt, 0o700);
  return wt;
}

async function seedThreadWorkspace(harness: CoreHarness, world: MergeWorld): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    store.ensureBinding({
      botUid: harness.bot.uid,
      ...world.thread,
      cwd: world.worktree,
      sessionId: `session-${world.branch}`,
      sessionFile: null,
      cwdPinned: true,
    });
    const ownership: ThreadWorkspaceRecord = {
      botUid: harness.bot.uid,
      channelType: 5,
      channelId: world.thread.channelId,
      layout: "single",
      status: "ready",
      entryPath: null,
      projects: [{
        name: "fixture project",
        repo: world.repo,
        path: world.worktree,
        branch: world.branch,
        created: true,
      }],
      error: null,
    };
    store.saveThreadWorkspace(ownership);
  } finally {
    store.close();
  }
}

async function createHarness(
  root: string,
  world: MergeWorld,
  transport: MergeTracingTransport,
  prompt?: FakePrompt,
): Promise<CoreHarness> {
  const harnessRoot = join(root, "harness");
  await mkdir(harnessRoot, { recursive: true });
  const wt = await writeWorktrunkFixture(root);
  const harness = await createCoreHarness({
    root: harnessRoot,
    transport,
    worktreePort: createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [wt],
    }),
    ...(prompt === undefined ? {} : { prompt }),
  });
  await seedThreadWorkspace(harness, world);
  return harness;
}

async function sendMerge(harness: CoreHarness, world: MergeWorld): Promise<void> {
  await harness.service.handleInbound(inbound({
    ...world.thread,
    messageId: `merge-${world.branch}`,
    text: "/thread merge",
    mentionedUserIds: [harness.bot.uid],
  }));
}

async function waitForArchive(transport: MergeTracingTransport, shortId: string): Promise<void> {
  await transport.waitForManagementCalls(
    (call) => call.action === "thread-archive" && call.shortId === shortId,
  );
}

function expectReceiptBeforeArchive(transport: MergeTracingTransport, target: ChatTarget): void {
  const receipt = transport.events.findIndex(
    (event) => event.kind === "send" && event.target.channelId === target.channelId,
  );
  const archive = transport.events.findIndex(
    (event) => event.kind === "archive" && event.shortId === target.channelId.slice(target.channelId.indexOf("____") + 4),
  );
  expect(receipt).toBeGreaterThanOrEqual(0);
  expect(archive).toBeGreaterThan(receipt);
}

test("real Git default merge runs in the host, squashes and removes before receipt-gated archive", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-merge-")));
  const world = await createMergeWorld(root, "success");
  const transport = new MergeTracingTransport();
  const notices: string[] = [];
  let harness: CoreHarness | undefined;
  try {
    harness = await createHarness(root, world, transport, async ({ text }) => {
      if (!text.startsWith("[子区合并回传]")) {
        throw new Error("a successful host merge must not create a repair worker");
      }
      notices.push(text);
    });
    await sendMerge(harness, world);
    await waitForArchive(transport, "success");
    await harness.workers.waitForPromptCount(1);

    expect(harness.workers.workers).toHaveLength(1);
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("目标分支：main");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getLatestTask(harness.bot.uid, world.thread)).toBeNull();
      expect(store.getThreadWorkspace(harness.bot.uid, world.thread)?.projects).toEqual([
        { name: "fixture project", repo: world.repo, path: null, branch: null, created: false },
      ]);
    } finally {
      store.close();
    }
    expect(existsSync(world.worktree)).toBe(false);
    expect(await runGit(world.repo, ["branch", "--list", world.branch])).toBe("");
    expect(await readFile(join(world.repo, "feature-one.txt"), "utf8")).toBe("one\n");
    expect(await readFile(join(world.repo, "feature-two.txt"), "utf8")).toBe("two\n");
    expect(await runGit(world.repo, ["rev-list", "--count", "main", "--not", world.base])).toBe("1");
    expectReceiptBeforeArchive(transport, world.thread);
  } finally {
    if (harness) await closeCoreHarness(harness);
    await rm(root, { recursive: true, force: true });
  }
});

test("identity and intent refusals reply without mergePending, tasks or notices", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-merge-")));
  const world = await createMergeWorld(root, "success");
  const transport = new MergeTracingTransport();
  const otherRepo = join(root, "other-repo");
  await mkdir(otherRepo, { recursive: true });
  await runGit(otherRepo, ["init", "-q", "-b", "main"]);
  const blockedThread = { channelId: `${GROUP_ID}____blocked`, channelType: 5 as const };
  let harness: CoreHarness | undefined;
  try {
    harness = await createHarness(root, world, transport, async () => {
      throw new Error("a refused merge must not create a worker");
    });

    // The primary checkout has no linked-worktree identity; begin must stay unset on refusal.
    const primary = await openChannelStore(harness.stateDir);
    try {
      primary.ensureBinding({
        botUid: harness.bot.uid,
        ...world.thread,
        cwd: world.repo,
        sessionId: "session-primary",
        sessionFile: null,
        cwdPinned: true,
      });
      primary.saveThreadWorkspace({
        botUid: harness.bot.uid,
        channelType: 5,
        channelId: world.thread.channelId,
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{
          name: "fixture project",
          repo: world.repo,
          path: world.repo,
          branch: "main",
          created: true,
        }],
        error: null,
      });
    } finally {
      primary.close();
    }
    await sendMerge(harness, world);
    await transport.waitForAttempt(({ text }) => text.includes("不是独立的 git worktree"));

    // A recorded source repository that does not own the linked worktree is refused as well.
    const mismatch = await openChannelStore(harness.stateDir);
    try {
      mismatch.ensureBinding({
        botUid: harness.bot.uid,
        ...world.thread,
        cwd: world.worktree,
        sessionId: "session-linked",
        sessionFile: null,
        cwdPinned: true,
      });
      mismatch.saveThreadWorkspace({
        botUid: harness.bot.uid,
        channelType: 5,
        channelId: world.thread.channelId,
        layout: "single",
        status: "ready",
        entryPath: null,
        projects: [{
          name: "fixture project",
          repo: otherRepo,
          path: world.worktree,
          branch: world.branch,
          created: true,
        }],
        error: null,
      });
    } finally {
      mismatch.close();
    }
    await harness.service.handleInbound(inbound({
      ...world.thread,
      messageId: "merge-repo-mismatch",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await transport.waitForAttempt(({ text }) =>
      text.includes("隔离工作区所属仓库与所有权记录不匹配")
    );

    // A preparing workspace intent blocks an unbound thread before any merge intent.
    const blocked = await openChannelStore(harness.stateDir);
    try {
      blocked.saveThreadWorkspaceIntent({
        id: "blocked-intent",
        botUid: harness.bot.uid,
        groupId: GROUP_ID,
        status: "preparing",
        channelIds: [blockedThread.channelId],
      });
    } finally {
      blocked.close();
    }
    await harness.service.handleInbound(inbound({
      ...blockedThread,
      messageId: "merge-blocked",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await transport.waitForAttempt(
      ({ text }) => text === "当前子区工作区未就绪或资源已缺失，未接收任务。",
    );

    const verify = await openChannelStore(harness.stateDir);
    try {
      expect(verify.getThreadListen(harness.bot.uid, world.thread)?.mergePending ?? false).toBe(false);
      expect(verify.getThreadListen(harness.bot.uid, blockedThread)).toBeNull();
      expect(verify.getLatestTask(harness.bot.uid, world.thread)).toBeNull();
      expect(verify.getLatestTask(harness.bot.uid, blockedThread)).toBeNull();
      expect(verify.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    } finally {
      verify.close();
    }
    expect(harness.workers.workers).toEqual([]);
  } finally {
    if (harness) await closeCoreHarness(harness);
    await rm(root, { recursive: true, force: true });
  }
});

async function runRepairScenario(scenario: "conflict" | "hook"): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-thread-merge-")));
  const world = await createMergeWorld(root, scenario);
  const transport = new MergeTracingTransport();
  const repairContexts: string[] = [];
  const notices: string[] = [];
  let harness: CoreHarness | undefined;
  try {
    harness = await createHarness(root, world, transport, async ({ cwd, text, promptContext, emit }) => {
      if (text.startsWith("[子区合并回传]")) {
        notices.push(text);
        return;
      }
      repairContexts.push(text);
      expect(cwd).toBe(world.worktree);
      expect(existsSync(world.worktree)).toBe(true);
      expect(await runGit(world.worktree, ["branch", "--show-current"])).toBe(
        scenario === "conflict" ? "" : world.branch,
      );
      if (scenario === "conflict") {
        expect(await runGit(world.worktree, ["status", "--porcelain"])).toContain("UU shared.txt");
        await writeFile(join(world.worktree, "shared.txt"), "repaired merge\n", "utf8");
        await runGit(world.worktree, ["add", "shared.txt"]);
        await runGit(world.worktree, ["-c", "core.editor=true", "rebase", "--continue"]);
      } else {
        const marker = world.hookFailureMarker;
        const hookRuns = world.hookRuns;
        if (!marker || !hookRuns) throw new Error("hook scenario did not create its failure fixture");
        expect(existsSync(marker)).toBe(true);
        expect(await readFile(hookRuns, "utf8")).toBe("run\n");
        await rm(marker, { force: true });
      }
      const management = (promptContext?.tools ?? []).find(({ name }) => name === "octo_management");
      if (!management) throw new Error("repair worker did not receive octo_management");
      const callId = "merge-repair-tool-call";
      const signal = new AbortController().signal;
      const result = await management.execute({ action: "thread-merge" }, signal, callId);
      const retried = JSON.parse(
        await management.execute({ action: "thread-merge" }, signal, callId),
      ) as { merged: boolean; replayed?: boolean; removed?: boolean };
      expect(retried).toMatchObject({ merged: true, replayed: true, removed: true });
      expect(JSON.parse(result)).toMatchObject({ merged: true, removed: true });
      management.onResultAccepted?.(callId);
      emit(assistantText("repaired-merge-result", "合并修复完成，工作区与分支已清理。", true));
    });
    await sendMerge(harness, world);
    await harness.workers.waitForPromptCount(1);
    await waitForArchive(transport, scenario);
    await harness.workers.waitForPromptCount(2);

    expect(harness.workers.workers).toHaveLength(2);
    expect(notices).toHaveLength(1);
    expect(notices[0]).toContain("目标分支：main");
    expect(repairContexts).toHaveLength(1);
    expect(repairContexts[0]?.length).toBeLessThanOrEqual(4_000);
    if (scenario === "conflict") {
      expect(repairContexts[0]).toContain("CONFLICT");
      expect(repairContexts[0]).toContain("shared.txt");
      expect(await readFile(join(world.repo, "shared.txt"), "utf8")).toBe("repaired merge\n");
    } else {
      expect(repairContexts[0]).toContain("pre-merge hook blocked");
      expect(await readFile(join(world.hookRuns!), "utf8")).toBe("run\nrun\n");
      expect(await readFile(join(world.repo, "feature-one.txt"), "utf8")).toBe("one\n");
      expect(await readFile(join(world.repo, "feature-two.txt"), "utf8")).toBe("two\n");
    }
    expect(existsSync(world.worktree)).toBe(false);
    expect(await runGit(world.repo, ["branch", "--list", world.branch])).toBe("");
    expectReceiptBeforeArchive(transport, world.thread);
  } finally {
    if (harness) await closeCoreHarness(harness);
    await rm(root, { recursive: true, force: true });
  }
}

test("a real rebase conflict preserves the worktree for one repair agent and host retry", async () => {
  await runRepairScenario("conflict");
});

test("a real pre-merge hook failure preserves the worktree for one repair agent and host retry", async () => {
  await runRepairScenario("hook");
});
