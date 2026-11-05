import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, unlink, watch, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { HostRemovalLeaseRecord } from "../src/store/store-records.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";
import { groupAlive } from "../src/omp/process.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  submitAndWaitForIngressHandoff,
} from "./fixtures/core-fakes.ts";

class RemovalTransport extends FakeTransport {
  receiptDeliveredBeforeArchive = false;

  override async archiveThread(groupId: string, shortId: string, signal?: AbortSignal): Promise<void> {
    this.receiptDeliveredBeforeArchive = this.sent.some(({ target, text }) =>
      target.channelId === `${groupId}____${shortId}` && text.includes("归档"),
    );
    await super.archiveThread(groupId, shortId, signal);
  }
}

async function git(cwd: string, ...args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", cwd, ...args], {
    stdin: "ignore", stdout: "pipe", stderr: "pipe",
    env: { ...process.env, GIT_CONFIG_GLOBAL: "/dev/null", GIT_CONFIG_NOSYSTEM: "1" },
  });
  const [out, err, code] = await Promise.all([
    new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
  ]);
  if (code !== 0) throw new Error(`git ${args.join(" ")}: ${err}`);
  return out.trim();
}

async function repository(root: string, name: string): Promise<string> {
  const repo = join(root, name);
  await mkdir(repo);
  await git(repo, "init", "-q", "-b", "main");
  await git(repo, "config", "user.name", "Removal Fixture");
  await git(repo, "config", "user.email", "removal@example.test");
  await writeFile(join(repo, "base.txt"), "base\n");
  await git(repo, "add", ".");
  await git(repo, "commit", "-qm", "base");
  return repo;
}
async function waitForImmediateTurn(): Promise<void> {
  const turn = Promise.withResolvers<void>();
  setImmediate(turn.resolve);
  await turn.promise;
}

async function waitForHostRemovalLease(
  store: ChannelStore,
  botUid: string,
  target: { channelId: string; channelType: 5 },
  sourceId: string,
  predicate: (lease: HostRemovalLeaseRecord) => boolean,
): Promise<HostRemovalLeaseRecord> {
  const deadline = performance.now() + 5_000;
  let observed = store.getHostRemovalLeaseBySourceId(botUid, target, sourceId);
  while (performance.now() < deadline) {
    if (observed !== null && predicate(observed)) return observed;
    await waitForImmediateTurn();
    observed = store.getHostRemovalLeaseBySourceId(botUid, target, sourceId);
  }
  throw new Error(`host removal lease did not reach expected state for ${sourceId}`);
}

async function waitForProcessGroupExit(pgid: number): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    if (!groupAlive(pgid)) return;
    await waitForImmediateTurn();
  }
  throw new Error(`host removal process group ${pgid} remained alive after cancellation`);
}

// A command routed to the agent, or a force path that leaves the branch, breaks this lifecycle.
test("real host remove protects unmerged work, then force deletes its branch without merging", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-remove-integration-")));
  const transport = new RemovalTransport();
  const harness = await createCoreHarness({
    root,
    transport,
    worktreePort: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [] }),
  });
  const store = await openChannelStore(harness.stateDir);
  const thread = { channelType: 5 as const, channelId: "group-allowed____remove-real" };
  try {
    const repo = await repository(root, "source");
    const worktree = join(root, "owned");
    await git(repo, "worktree", "add", "-qb", "feature", worktree);
    const originalHead = await git(repo, "rev-parse", "HEAD");
    await writeFile(join(worktree, "feature.txt"), "unique committed work\n");
    await git(worktree, "add", ".");
    await git(worktree, "commit", "-qm", "unique feature");
    const featureHead = await git(worktree, "rev-parse", "HEAD");
    store.ensureBinding({ botUid: harness.bot.uid, ...thread, cwd: worktree,
      sessionId: "remove-session", sessionFile: null, cwdPinned: true });
    store.saveThreadWorkspace({ botUid: harness.bot.uid, ...thread, layout: "single",
      status: "ready", entryPath: null, error: null,
      projects: [{ name: "feature", repo, path: worktree, branch: "feature", created: true }] });
    const send = (id: string, text: string) => harness.service.submitInbound(inbound({
      ...thread, messageId: id, text, mentionedUserIds: [harness.bot.uid],
    }));
    const protectedReceipt = harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelId === thread.channelId
      && sentTarget.channelType === thread.channelType
      && text.includes("--force"),
    );
    await send("remove-protected", "/thread remove");
    await protectedReceipt;
    expect(existsSync(worktree)).toBe(true);
    expect(await git(repo, "rev-parse", "refs/heads/feature")).toBe(featureHead);
    expect(harness.transport.managementCalls).toEqual([]);
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(({ text }) => text.includes("--force"))).toBe(true);

    await writeFile(join(worktree, "untracked.txt"), "uncommitted work\n");
    const completeReceipt = harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelId === thread.channelId
      && sentTarget.channelType === thread.channelType
      && text.startsWith("本地隔离工作区与记录分支清理完成"),
    );
    await send("remove-forced", "/thread remove --force");
    expect((await completeReceipt).text).toContain("本地隔离工作区与记录分支清理完成");
    // Filesystem proof before observing the fixture's archive request.
    expect(existsSync(worktree)).toBe(false);
    expect(await git(repo, "branch", "--list", "feature")).toBe("");
    expect(await git(repo, "rev-parse", "HEAD")).toBe(originalHead);
    expect(existsSync(join(repo, "feature.txt"))).toBe(false);
    await harness.transport.waitForManagementCalls(call => call.action === "thread-archive");
    expect(transport.receiptDeliveredBeforeArchive).toBe(true);
    expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
    expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);
    expect(harness.workers.creations).toEqual([]);
    const duplicateMessage = inbound({
      ...thread,
      messageId: "remove-forced",
      text: "/thread remove --force",
      mentionedUserIds: [harness.bot.uid],
    });
    await submitAndWaitForIngressHandoff(harness, duplicateMessage);
    expect(harness.transport.managementCalls.filter(call => call.action === "thread-archive")).toHaveLength(1);
    expect(harness.transport.managementCalls.some(call => call.action === "thread-delete")).toBe(false);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

// Clearing project ownership before rmdir must not hide unknown shared-entry files or old branches.
test("real shared remove records missing and removed projects but archives only after its entry is empty", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-remove-shared-integration-")));
  const harness = await createCoreHarness({
    root,
    worktreePort: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [] }),
  });
  const store = await openChannelStore(harness.stateDir);
  const thread = { channelType: 5 as const, channelId: "group-allowed____remove-shared-real" };
  try {
    const repoA = await repository(root, "source-a");
    const repoB = await repository(root, "source-b");
    const headA = await git(repoA, "rev-parse", "HEAD");
    const headB = await git(repoB, "rev-parse", "HEAD");
    const entry = join(root, "entry");
    await mkdir(entry);
    const pathA = join(entry, "a");
    const pathB = join(entry, "b");
    await git(repoA, "worktree", "add", "-qb", "thread-a", pathA);
    await git(repoB, "worktree", "add", "-qb", "thread-b", pathB);
    await git(repoA, "worktree", "remove", pathA);
    const unknown = join(entry, "owner-note.txt");
    await writeFile(unknown, "must survive force\n");
    const detached = join(root, "user-directory");
    await mkdir(detached);
    await writeFile(join(detached, "keep.txt"), "user content\n");
    store.ensureBinding({ botUid: harness.bot.uid, ...thread, cwd: detached,
      sessionId: "moved-session", sessionFile: null, cwdPinned: false });
    store.saveThreadWorkspace({ botUid: harness.bot.uid, ...thread, layout: "shared",
      status: "ready", entryPath: entry, error: null, projects: [
        { name: "a", repo: repoA, path: pathA, branch: "thread-a", created: true },
        { name: "b", repo: repoB, path: pathB, branch: "thread-b", created: true },
      ] });
    const send = (id: string) => harness.service.submitInbound(inbound({
      ...thread, messageId: id, text: "/thread remove --force", mentionedUserIds: [harness.bot.uid],
    }));
    const partialReceipt = harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelId === thread.channelId
      && sentTarget.channelType === thread.channelType
      && text.includes("本地工作区清理未完整完成"),
    );
    await send("shared-partial");
    expect((await partialReceipt).text).toContain("本地工作区清理未完整完成");
    expect(existsSync(pathB)).toBe(false);
    expect(await git(repoA, "branch", "--list", "thread-a")).toBe("");
    expect(await git(repoB, "branch", "--list", "thread-b")).toBe("");
    expect(existsSync(unknown)).toBe(true);
    expect(existsSync(join(detached, "keep.txt"))).toBe(true);
    expect(harness.transport.managementCalls).toEqual([]);
    expect(store.getThreadWorkspace(harness.bot.uid, thread)?.entryPath).toBe(entry);
    await unlink(unknown);
    const completeReceipt = harness.transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelId === thread.channelId
      && sentTarget.channelType === thread.channelType
      && text.startsWith("本地隔离工作区与记录分支清理完成"),
    );
    await send("shared-complete");
    expect((await completeReceipt).text).toContain("本地隔离工作区与记录分支清理完成");
    expect(existsSync(entry)).toBe(false);
    await harness.transport.waitForManagementCalls(call => call.action === "thread-archive");
    expect(await git(repoA, "rev-parse", "HEAD")).toBe(headA);
    expect(await git(repoB, "rev-parse", "HEAD")).toBe(headB);
    expect(harness.workers.prompts).toEqual([]);
    expect(store.getLatestTask(harness.bot.uid, thread)).toBeNull();
    expect(store.listThreadMergeEvents(harness.bot.uid)).toEqual([]);

  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});

// Cancellation must own the entire external removal process, not just abandon its promise.
test("cancel stops a real host removal process group before releasing the worktree", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-remove-cancel-integration-")));
  const marker = join(root, "removal-started");
  const wt = join(root, "blocking-wt");
  await writeFile(wt, `#!/bin/sh
if [ "$1" = "--version" ]; then printf 'wt v0.80.0\\n'; exit 0; fi
[ "$1" = "remove" ] || exit 64
printf '%s\\n' "$$" > '${marker}'
sleep 60 &
wait
`, { mode: 0o700 });
  const harness = await createCoreHarness({
    root,
    worktreePort: createWorktreePort({ command: "/no/omp", worktrunkCandidates: [wt] }),
  });
  const store = await openChannelStore(harness.stateDir);
  const thread = { channelType: 5 as const, channelId: "group-allowed____remove-cancel-real" };
  let pgid: number | undefined;
  try {
    const repo = await repository(root, "source");
    const worktree = join(root, "owned");
    await git(repo, "worktree", "add", "-qb", "feature", worktree);
    const mainHead = await git(repo, "rev-parse", "HEAD");
    store.ensureBinding({ botUid: harness.bot.uid, ...thread, cwd: worktree,
      sessionId: "cancel-session", sessionFile: null, cwdPinned: true });
    store.saveThreadWorkspace({ botUid: harness.bot.uid, ...thread, layout: "single",
      status: "ready", entryPath: null, error: null,
      projects: [{ name: "feature", repo, path: worktree, branch: "feature", created: true }] });
    // The marker comes from a real child process; await filesystem events rather than advancing time.
    const started = (async (): Promise<number> => {
      for await (const event of watch(root, { signal: AbortSignal.timeout(5_000) })) {
        if (event.filename !== "removal-started" || !existsSync(marker)) continue;
        const pid = Number((await readFile(marker, "utf8")).trim());
        if (pid > 0) return pid;
      }
      throw new Error("Removal process did not publish its PID");
    })();
    const removalMessage = inbound({
      ...thread,
      messageId: "remove-blocking",
      text: "/thread remove",
      mentionedUserIds: [harness.bot.uid],
    });
    await harness.service.submitInbound(removalMessage);
    const removalPgid = await started;
    pgid = removalPgid;
    const runningLease = await waitForHostRemovalLease(
      store,
      harness.bot.uid,
      thread,
      removalMessage.messageId,
      lease => lease.phase === "running",
    );
    expect(runningLease.pgid).toBe(removalPgid);
    expect(groupAlive(removalPgid)).toBe(true);
    await harness.service.submitInbound(inbound({
      ...thread, messageId: "cancel-removal", text: "/cancel", mentionedUserIds: [harness.bot.uid],
    }));
    await waitForProcessGroupExit(removalPgid);
    await waitForHostRemovalLease(
      store,
      harness.bot.uid,
      thread,
      removalMessage.messageId,
      lease => lease.phase === "unknown" || lease.outcome !== null,
    );
    expect(existsSync(worktree)).toBe(true);
    expect(await git(repo, "rev-parse", "refs/heads/feature")).toBe(mainHead);
    expect(await git(repo, "rev-parse", "HEAD")).toBe(mainHead);
    expect(harness.transport.managementCalls).toEqual([]);
    expect(harness.workers.creations).toEqual([]);
    expect(store.getThreadWorkspace(harness.bot.uid, thread)?.projects[0]).toMatchObject({
      path: worktree, branch: "feature",
    });
  } finally {
    store.close();
    await closeCoreHarness(harness);
    if (pgid !== undefined && groupAlive(pgid)) {
      try {
        process.kill(-pgid, "SIGKILL");
      } catch {
        // The service may have reaped the process group during teardown.
      }
    }
  }
}, 15_000);
