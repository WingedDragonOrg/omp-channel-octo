import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { access, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts";
import { openChannelStore } from "../src/store";
import { projectSessionDir, sessionsRoot } from "../src/session/session-dir";
import { renderReviewPrompt } from "../src/tasks/review-prompt";
import type { ReviewSnapshot } from "../src/review/snapshot";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness, FakePromptContext } from "./fixtures/core-fakes";

const DM = { channelId: "dm-review-command", channelType: 1 as const };
const PRIMARY_INPUT = "PRIMARY_TASK_ONLY: hold this task until released";
const REVIEW_FOCUS = "REVIEW_FOCUS_ONLY: inspect the lock boundary";
const PRIMARY_OUTPUT = "PRIMARY_TASK_RESULT_ONLY";
const REVIEW_OUTPUT = "INDEPENDENT_REVIEW_RESULT_ONLY";
const WAIT_TURNS = 1_500;

type Gate = { readonly promise: Promise<void>; readonly resolve: () => void };

function gate(): Gate {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, resolve: () => resolve() };
}

/** Every test wait, including fixture hand-offs, has a finite event-loop-turn budget. */
function nextTurn(): Promise<void> {
  // Real subprocess and SQLite I/O need wall-clock time; fake timers cannot advance them.
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, 1);
  return promise;
}

async function within<T>(operation: Promise<T>, label: string, maxTurns = WAIT_TURNS): Promise<T> {
  type Outcome = { kind: "fulfilled"; value: T } | { kind: "rejected"; error: unknown };
  const completion = Promise.withResolvers<Outcome>();
  void operation.then(
    value => completion.resolve({ kind: "fulfilled", value }),
    error => completion.resolve({ kind: "rejected", error }),
  );
  for (let turn = 0; turn < maxTurns; turn += 1) {
    const outcome = await Promise.race([completion.promise, nextTurn().then(() => null)]);
    if (outcome === null) continue;
    if (outcome.kind === "fulfilled") return outcome.value;
    throw outcome.error;
  }
  throw new Error(`${label} did not settle within ${maxTurns} event-loop turns`);
}

async function waitUntil(predicate: () => boolean | Promise<boolean>, label: string): Promise<void> {
  for (let turn = 0; turn < WAIT_TURNS; turn += 1) {
    if (await within(Promise.resolve(predicate()), label)) return;
    await nextTurn();
  }
  throw new Error(`${label} was not observed within ${WAIT_TURNS} event-loop turns`);
}

async function waitForText(
  harness: CoreHarness,
  after: number,
  predicate: (text: string) => boolean,
  label: string,
  expectedTarget: ChatTarget = DM,
): Promise<string> {
  for (let turn = 0; turn < WAIT_TURNS; turn += 1) {
    const match = harness.transport.sent
      .slice(after)
      .find(({ target, text }) =>
        target.channelId === expectedTarget.channelId
        && target.channelType === expectedTarget.channelType
        && predicate(text));
    if (match) return match.text;
    await nextTurn();
  }
  throw new Error(`${label} was not sent within ${WAIT_TURNS} event-loop turns`);
}

async function progressCardAt(
  harness: CoreHarness,
  index: number,
  label: string,
) {
  await waitUntil(() => harness.transport.reviewProgressCards.length > index, label);
  const card = harness.transport.reviewProgressCards[index];
  if (!card) throw new Error(`${label} was not recorded by the progress-card transport`);
  return card;
}

async function waitForProgressCardState(
  harness: CoreHarness,
  messageId: string,
  state: "running" | "completed" | "failed" | "cancelled",
  label: string,
) {
  await waitUntil(
    () => harness.transport.reviewProgressCardEdits.some(edit =>
      edit.messageId === messageId && edit.progress.state === state),
    label,
  );
  const edit = harness.transport.reviewProgressCardEdits.find(candidate =>
    candidate.messageId === messageId && candidate.progress.state === state);
  if (!edit) throw new Error(`${label} was not recorded by the progress-card transport`);
  return edit;
}

/**
 * The review-owned session directory is removed only after the worker, resource lease, and
 * progress-card tail have settled. Observe that lifecycle milestone instead of sleeping.
 */
async function waitForReviewRuntimeCleanup(
  harness: CoreHarness,
  messageId: string,
  label: string,
): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    await waitUntil(async () => {
      const job = store.listTerminalReviewJobs(harness.bot.uid)
        .find(review => review.messageId === messageId);
      if (!job) return false;
      const sessionDirectory = join(
        projectSessionDir(sessionsRoot(harness.config), job.cwd),
        "reviews",
        job.id,
      );
      try {
        await access(sessionDirectory);
        return false;
      } catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ENOENT") return true;
        throw error;
      }
    }, label);
  } finally {
    store.close();
  }
}


async function boundBinding(harness: CoreHarness) {
  const store = await openChannelStore(harness.stateDir);
  try {
    const binding = store.getBinding(harness.bot.uid, DM);
    if (!binding) throw new Error("test task did not establish a chat binding");
    return binding;
  } finally {
    store.close();
  }
}

async function latestTask(harness: CoreHarness) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getLatestTask(harness.bot.uid, DM);
  } finally {
    store.close();
  }
}

async function commandClaimState(harness: CoreHarness, messageId: string) {
  const store = await openChannelStore(harness.stateDir);
  try {
    return store.getCommandClaimState(harness.bot.uid, DM, messageId);
  } finally {
    store.close();
  }
}

async function runGit(root: string, args: string[]): Promise<void> {
  const child = Bun.spawn(["git", "-C", root, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const output = Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  let result: [number, string, string];
  try {
    result = await within(output, `git ${args.join(" ")}`, 20_000);
  } catch (error) {
    child.kill();
    await within(child.exited, "git fixture process exit", 2_000).catch(() => {});
    throw error;
  }
  const [exitCode, stdout, stderr] = result;
  if (exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
  }
}

async function createReviewHarness(prompt: (context: FakePromptContext) => Promise<void>): Promise<CoreHarness> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-core-review-"));
  try {
    await runGit(root, ["init", "--quiet"]);
    await writeFile(join(root, ".gitignore"), "state/\n", "utf8");
    await writeFile(join(root, "tracked.ts"), "export const fixture = true;\n", "utf8");
    await runGit(root, ["add", ".gitignore", "tracked.ts"]);
    await runGit(root, [
      "-c", "user.name=Review Fixture",
      "-c", "user.email=review-fixture@example.invalid",
      "-c", "commit.gpgsign=false",
      "commit", "--quiet", "-m", "fixture baseline",
    ]);
    return await within(createCoreHarness({ root, prompt }), "CoreService harness startup");
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function settle(operation: Promise<void> | undefined): Promise<void> {
  if (!operation) return;
  await within(operation.catch(() => {}), "test operation cleanup").catch(() => {});
}

async function waitForPrimaryStatus(harness: CoreHarness, state: "running" | "completed"): Promise<void> {
  await within(
    waitForStatus(harness.service, DM, status => status.state === state && status.queued === 0),
    `primary task ${state} status`,
  );
}

function heldPrompt(
  prompt: FakePromptContext,
  released: Promise<void>,
  output: string,
  messageId: string,
  label: string,
): Promise<void> {
  return Promise.race([
    released.then(() => {
      prompt.emit(assistantText(messageId, output, true));
    }),
    prompt.waitForAbort().then(() => {
      throw new Error(`${label} prompt was aborted`);
    }),
  ]);
}

test("review runs in a separate session while the main task is held and /status stays responsive", async () => {
  const primaryStarted = Promise.withResolvers<FakePromptContext>();
  const reviewStarted = Promise.withResolvers<FakePromptContext>();
  const releasePrimary = gate();
  const releaseReview = gate();
  const prompt = async (context: FakePromptContext): Promise<void> => {
    if (context.text.includes(PRIMARY_INPUT)) {
      primaryStarted.resolve(context);
      return heldPrompt(context, releasePrimary.promise, PRIMARY_OUTPUT, "review-primary-result", "primary");
    }
    if (context.text.includes(REVIEW_FOCUS)) {
      reviewStarted.resolve(context);
      return heldPrompt(context, releaseReview.promise, REVIEW_OUTPUT, "review-side-result", "review");
    }
    throw new Error(`unexpected fixture prompt: ${context.text}`);
  };

  let harness: CoreHarness | undefined;
  let primaryCommand: Promise<void> | undefined;
  let reviewCommand: Promise<void> | undefined;
  let statusCommand: Promise<void> | undefined;
  try {
    harness = await createReviewHarness(prompt);
    primaryCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-primary-task",
      text: PRIMARY_INPUT,
    }));
    await within(primaryCommand, "primary inbound acknowledgement");
    const primaryPrompt = await within(primaryStarted.promise, "primary fake worker start");
    await waitForPrimaryStatus(harness, "running");
    const mainWorker = harness.workers.workers.find(worker => worker.sessionId === primaryPrompt.sessionId);
    expect(mainWorker).toBeDefined();
    const bindingBefore = await boundBinding(harness);

    const reviewStart = harness.transport.sent.length;
    reviewCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-independent-request",
      text: `/review ${REVIEW_FOCUS}`,
    }));
    await within(reviewCommand, "review inbound acknowledgement while primary is held");
    const reviewPrompt = await within(reviewStarted.promise, "independent review worker start");
    const reviewWorker = harness.workers.workers.find(worker => worker.sessionId === reviewPrompt.sessionId);

    expect(reviewWorker).toBeDefined();
    expect(reviewPrompt.sessionId).not.toBe(primaryPrompt.sessionId);
    expect(reviewPrompt.text).toContain(REVIEW_FOCUS);
    expect(reviewPrompt.text).not.toContain("/review");
    expect(primaryPrompt.text).not.toContain("/review");
    expect(primaryPrompt.text).not.toContain(REVIEW_FOCUS);
    expect(mainWorker!.steerCalls).toEqual([]);
    expect(mainWorker!.abortCalls).toBe(0);
    expect(mainWorker!.stopped).toBe(false);
    expect(harness.workers.creations.length).toBeGreaterThanOrEqual(2);

    const statusStart = harness.transport.sent.length;
    statusCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-status-during-run",
      text: "/status",
    }));
    await within(statusCommand, "/status inbound acknowledgement while review worker is held");
    const statusReply = await waitForText(
      harness,
      statusStart,
      text => text.includes("state=running") && text.includes(`session=${bindingBefore.sessionId}`),
      "running status reply",
    );
    expect(statusReply).toContain(`session=${bindingBefore.sessionId}`);

    releaseReview.resolve();
    const reviewReply = await waitForText(
      harness,
      reviewStart,
      text => text.includes(REVIEW_OUTPUT),
      "independent review result",
    );
    expect(reviewReply).toContain(REVIEW_OUTPUT);
    await waitUntil(() => reviewWorker!.stopped, "review worker cleanup");
    expect(mainWorker!.stopped).toBe(false);
    expect(mainWorker!.abortCalls).toBe(0);
    expect(mainWorker!.steerCalls).toEqual([]);
    expect(await harness.service.status(DM)).toMatchObject({
      sessionId: bindingBefore.sessionId,
      cwd: bindingBefore.cwd,
      state: "running",
      queued: 0,
    });
    expect(await boundBinding(harness)).toMatchObject({
      sessionId: bindingBefore.sessionId,
      sessionFile: bindingBefore.sessionFile,
      cwd: bindingBefore.cwd,
    });

    releasePrimary.resolve();
    await waitForPrimaryStatus(harness, "completed");
    await waitForText(
      harness,
      reviewStart,
      text => text.includes(PRIMARY_OUTPUT),
      "primary task result",
    );
    const task = await latestTask(harness);
    expect(task?.resultText).toBe(PRIMARY_OUTPUT);
    expect(task?.resultText).not.toContain(REVIEW_OUTPUT);
  } finally {
    releaseReview.resolve();
    releasePrimary.resolve();
    await settle(statusCommand);
    await settle(reviewCommand);
    await settle(primaryCommand);
    if (harness) {
      await within(closeCoreHarness(harness), "CoreService harness shutdown").catch(() => {});
    }
  }
});

test("duplicate review delivery is idempotent, concurrent review is busy, and stop leaves the primary task running", async () => {
  const primaryStarted = Promise.withResolvers<FakePromptContext>();
  const reviewStarted = Promise.withResolvers<FakePromptContext>();
  const releasePrimary = gate();
  const releaseReview = gate();
  const prompt = async (context: FakePromptContext): Promise<void> => {
    if (context.text.includes(PRIMARY_INPUT)) {
      primaryStarted.resolve(context);
      return heldPrompt(context, releasePrimary.promise, PRIMARY_OUTPUT, "review-stop-primary-result", "primary");
    }
    if (context.text.includes(REVIEW_FOCUS)) {
      reviewStarted.resolve(context);
      return heldPrompt(context, releaseReview.promise, REVIEW_OUTPUT, "review-stop-side-result", "review");
    }
    throw new Error(`unexpected fixture prompt: ${context.text}`);
  };

  let harness: CoreHarness | undefined;
  let primaryCommand: Promise<void> | undefined;
  let reviewCommand: Promise<void> | undefined;
  let duplicateCommand: Promise<void> | undefined;
  let concurrentCommand: Promise<void> | undefined;
  let stopCommand: Promise<void> | undefined;
  try {
    harness = await createReviewHarness(prompt);
    primaryCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-stop-primary-task",
      text: PRIMARY_INPUT,
    }));
    await within(primaryCommand, "primary inbound acknowledgement");
    const primaryPrompt = await within(primaryStarted.promise, "primary fake worker start");
    await waitForPrimaryStatus(harness, "running");
    const mainWorker = harness.workers.workers.find(worker => worker.sessionId === primaryPrompt.sessionId);
    expect(mainWorker).toBeDefined();
    const bindingBefore = await boundBinding(harness);

    const firstReview = inbound({
      ...DM,
      messageId: "review-stop-duplicate-input",
      text: `/review ${REVIEW_FOCUS}`,
    });
    reviewCommand = harness.service.submitInbound(firstReview);
    await within(reviewCommand, "review inbound acknowledgement");
    const reviewPrompt = await within(reviewStarted.promise, "independent review worker start");
    const reviewWorker = harness.workers.workers.find(worker => worker.sessionId === reviewPrompt.sessionId);
    expect(reviewWorker).toBeDefined();
    expect(await commandClaimState(harness, firstReview.messageId)).not.toBeNull();

    const creationsBeforeDuplicates = harness.workers.creations.length;
    const promptsBeforeDuplicates = harness.workers.prompts.length;
    duplicateCommand = harness.service.submitInbound(firstReview);
    await within(duplicateCommand, "duplicate review inbound acknowledgement");
    const busyStart = harness.transport.sent.length;
    concurrentCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-stop-concurrent-input",
      text: "/review a second simultaneous audit",
    }));
    await within(concurrentCommand, "concurrent review inbound acknowledgement");
    const busyReply = await waitForText(
      harness,
      busyStart,
      text => /忙|进行中|处理|并发|busy/i.test(text),
      "concurrent-review busy reply",
    );
    expect(busyReply).toMatch(/忙|进行中|处理|并发|busy/i);
    expect(harness.workers.creations).toHaveLength(creationsBeforeDuplicates);
    expect(harness.workers.prompts).toHaveLength(promptsBeforeDuplicates);

    const stopStart = harness.transport.sent.length;
    stopCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "review-stop-command",
      text: "/review stop",
    }));
    await within(stopCommand, "/review stop inbound acknowledgement");
    await waitUntil(() => reviewWorker!.stopped, "review worker stop");
    await waitForText(
      harness,
      stopStart,
      text => /审查|review|停止|中断|取消/i.test(text),
      "review stop acknowledgement",
    );

    expect(mainWorker!.stopped).toBe(false);
    expect(mainWorker!.abortCalls).toBe(0);
    expect(mainWorker!.steerCalls).toEqual([]);
    expect(reviewWorker!.stopped).toBe(true);
    expect(await harness.service.status(DM)).toMatchObject({
      sessionId: bindingBefore.sessionId,
      cwd: bindingBefore.cwd,
      state: "running",
      queued: 0,
    });
    expect(await boundBinding(harness)).toMatchObject({
      sessionId: bindingBefore.sessionId,
      sessionFile: bindingBefore.sessionFile,
      cwd: bindingBefore.cwd,
    });
    expect(harness.transport.sent.slice(stopStart).some(({ text }) => text.includes(REVIEW_OUTPUT))).toBe(false);

    releasePrimary.resolve();
    await waitForPrimaryStatus(harness, "completed");
    await waitForText(
      harness,
      stopStart,
      text => text.includes(PRIMARY_OUTPUT),
      "primary task result after review stop",
    );
    expect((await latestTask(harness))?.resultText).toBe(PRIMARY_OUTPUT);
  } finally {
    releaseReview.resolve();
    releasePrimary.resolve();
    await settle(stopCommand);
    await settle(concurrentCommand);
    await settle(duplicateCommand);
    await settle(reviewCommand);
    await settle(primaryCommand);
    if (harness) {
      await within(closeCoreHarness(harness), "CoreService harness shutdown").catch(() => {});
    }
  }
});

test("review prompt redacts credentials from focus and group background", () => {
  const snapshot: ReviewSnapshot = { capturedAt: "2026-11-04T00:00:00.000Z", projects: [] };
  const prompt = renderReviewPrompt(
    snapshot,
    '重点 {"apiKey":"focus-secret","password":"focus-password"}',
    '群背景 {"client_secret":"context-secret","token":"context-token"}',
  );

  expect(prompt).not.toContain("focus-secret");
  expect(prompt).not.toContain("focus-password");
  expect(prompt).not.toContain("context-secret");
  expect(prompt).not.toContain("context-token");
  expect(prompt).toContain("已脱敏");
});

test("restart releases only orphaned review claims and preserves accepted and stop command identities", async () => {
  const root = await mkdtemp(join(tmpdir(), "review-claim-restart-"));
  const botUid = "bot-review-claims";
  const orphan = {
    botUid,
    target: DM,
    messageId: "orphan-review",
    senderId: "owner-1",
    text: "/review\tfocus",
    supported: true,
    kind: "command" as const,
  };
  const acceptedClaim = { ...orphan, messageId: "accepted-review", text: "/review focus" };
  const stopClaim = { ...orphan, messageId: "stop-review", text: "/review stop" };
  try {
    const first = await openChannelStore(root);
    expect(first.claimCommand(orphan)).toBe(true);
    expect(first.claimCommand(acceptedClaim)).toBe(true);
    expect(first.acceptReviewJob(acceptedClaim, {
      id: "review-job-1", botUid, ...DM, messageId: acceptedClaim.messageId, senderId: "owner-1",
      focus: "focus", projects: [{ name: "repo", path: root }], cwd: root,
      sessionId: "review-session-1", sessionFile: null, state: "accepted",
      dispatchStarted: false, promptAccepted: false, stopRequested: false,
      resultText: null, errorText: null,
    })).toBe(true);
    expect(first.claimCommand(stopClaim)).toBe(true);
    expect(first.requestReviewStopForClaim(stopClaim, "review-job-1")).toBe(true);
    first.close();

    const restarted = await openChannelStore(root);
    restarted.releaseOrphanedReviewClaims(botUid);
    expect(restarted.getCommandClaimState(botUid, DM, orphan.messageId)).toBeNull();
    expect(restarted.claimCommand(orphan)).toBe(true);
    expect(restarted.getCommandClaimState(botUid, DM, acceptedClaim.messageId)).toBe("completed");
    expect(restarted.claimCommand(acceptedClaim)).toBe(false);
    expect(restarted.getCommandClaimState(botUid, DM, stopClaim.messageId)).toBe("completed");
    expect(restarted.claimCommand(stopClaim)).toBe(false);
    restarted.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("group review refuses to start when required group context cannot be read", async () => {
  const group = { channelId: "group-review-context-failure", channelType: 2 as const };
  const harness = await createCoreHarness({ allowedGroupIds: [group.channelId] });
  try {
    await harness.service.submitInbound(inbound({
      ...group,
      messageId: "bind-group-before-review",
      text: "bind this group",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, group, status => status.state === "completed");
    const promptsBeforeReview = harness.workers.prompts.length;
    harness.transport.failGroupContextWith(new Error("group context unavailable"));
    const sentStart = harness.transport.sent.length;

    await harness.service.submitInbound(inbound({
      ...group,
      messageId: "review-context-failure",
      text: "/review inspect context boundary",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForText(
      harness,
      sentStart,
      text => text === "无法读取必要聊天背景，代码审查未启动。",
      "review group-context refusal",
      group,
    );
    expect(harness.workers.prompts).toHaveLength(promptsBeforeReview);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("legacy database migration adds review storage, preserves rows, and remains idempotent", async () => {
  const root = await mkdtemp(join(tmpdir(), "review-jobs-migration-"));
  const botUid = "bot-review-migration";
  const legacyPath = join(root, "channel.sqlite");
  try {
    const legacy = new Database(legacyPath);
    try {
      legacy.exec(`
        CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
        INSERT INTO meta (key, value) VALUES ('legacy-marker', 'preserved');
        CREATE TABLE owners (
          bot_uid TEXT PRIMARY KEY NOT NULL,
          owner_id TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO owners (bot_uid, owner_id, created_at, updated_at)
        VALUES ('bot-existing', 'owner-existing', 1, 1);
      `);
      expect(
        legacy.query("SELECT name FROM sqlite_master WHERE type='table' AND name='review_jobs'").get(),
      ).toBeNull();
    } finally {
      legacy.close();
    }

    const upgraded = await openChannelStore(root);
    try {
      expect(upgraded.getOwnerId("bot-existing")).toBe("owner-existing");
      const firstClaim = {
        botUid,
        target: DM,
        messageId: "legacy-migration-review-1",
        senderId: "owner-1",
        text: "/review first",
        supported: true,
        kind: "command" as const,
      };
      expect(upgraded.claimCommand(firstClaim)).toBe(true);
      expect(upgraded.acceptReviewJob(firstClaim, {
        id: "review-after-migration-1",
        botUid,
        ...DM,
        messageId: firstClaim.messageId,
        senderId: firstClaim.senderId,
        focus: "first",
        projects: [{ name: "repo", path: root }],
        cwd: root,
        sessionId: "review-session-1",
        sessionFile: null,
        state: "accepted",
        dispatchStarted: false,
        promptAccepted: false,
        stopRequested: false,
        resultText: null,
        errorText: null,
      })).toBe(true);
      expect(upgraded.getReviewJob("review-after-migration-1")).toMatchObject({
        id: "review-after-migration-1",
        botUid,
        messageId: firstClaim.messageId,
        projects: [{ name: "repo", path: root }],
      });
    } finally {
      upgraded.close();
    }

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.getOwnerId("bot-existing")).toBe("owner-existing");
      expect(reopened.getReviewJob("review-after-migration-1")).toMatchObject({
        id: "review-after-migration-1",
        state: "accepted",
      });
      const secondTarget = { channelId: "review-migration-second-chat", channelType: 1 as const };
      const secondClaim = {
        botUid,
        target: secondTarget,
        messageId: "legacy-migration-review-2",
        senderId: "owner-1",
        text: "/review second",
        supported: true,
        kind: "command" as const,
      };
      expect(reopened.claimCommand(secondClaim)).toBe(true);
      expect(reopened.acceptReviewJob(secondClaim, {
        id: "review-after-migration-2",
        botUid,
        ...secondTarget,
        messageId: secondClaim.messageId,
        senderId: secondClaim.senderId,
        focus: "second",
        projects: [{ name: "repo", path: root }],
        cwd: root,
        sessionId: "review-session-2",
        sessionFile: null,
        state: "accepted",
        dispatchStarted: false,
        promptAccepted: false,
        stopRequested: false,
        resultText: null,
        errorText: null,
      })).toBe(true);
      expect(reopened.getReviewJob("review-after-migration-2")?.focus).toBe("second");
    } finally {
      reopened.close();
    }

    const migrated = new Database(legacyPath);
    try {
      expect(migrated.query("SELECT value FROM meta WHERE key='legacy-marker'").get()).toEqual({
        value: "preserved",
      });
      expect(migrated.query("SELECT owner_id FROM owners WHERE bot_uid='bot-existing'").get()).toEqual({
        owner_id: "owner-existing",
      });
      expect(
        migrated.query("SELECT name FROM sqlite_master WHERE type='table' AND name='review_jobs'").get(),
      ).toEqual({ name: "review_jobs" });
    } finally {
      migrated.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("review progress cards have an independent lifecycle beside a held task and text survives card-send failure", async () => {
  const primaryStarted = Promise.withResolvers<FakePromptContext>();
  const completedReviewStarted = Promise.withResolvers<FakePromptContext>();
  const stoppedReviewStarted = Promise.withResolvers<FakePromptContext>();
  const releasePrimary = gate();
  const releaseCompletedReview = gate();
  const releaseStoppedReview = gate();
  const primaryText = "CARD_PRIMARY_TASK";
  const completedFocus = "CARD_REVIEW_COMPLETED";
  const failedFocus = "CARD_REVIEW_FAILED";
  const stoppedFocus = "CARD_REVIEW_STOPPED";
  const failedCardFocus = "CARD_REVIEW_CARD_SEND_FAILURE";
  const prompt = async (context: FakePromptContext): Promise<void> => {
    if (context.text.includes(primaryText)) {
      context.emit({
        type: "tool_start",
        toolCallId: "primary-card-tool",
        toolName: "bash",
        atMs: Date.now(),
      });
      primaryStarted.resolve(context);
      return heldPrompt(context, releasePrimary.promise, "CARD_PRIMARY_RESULT", "primary-card-result", "primary");
    }
    if (context.text.includes(completedFocus)) {
      completedReviewStarted.resolve(context);
      return heldPrompt(context, releaseCompletedReview.promise, "CARD_REVIEW_RESULT", "card-review-result", "review");
    }
    if (context.text.includes(failedFocus)) throw new Error("fixture review prompt failed");
    if (context.text.includes(stoppedFocus)) {
      stoppedReviewStarted.resolve(context);
      return heldPrompt(context, releaseStoppedReview.promise, "CARD_STOPPED_RESULT", "card-stopped-result", "review");
    }
    if (context.text.includes(failedCardFocus)) {
      context.emit(assistantText("card-send-failure-result", "CARD_TEXT_FALLBACK_RESULT", true));
      return;
    }
    throw new Error(`unexpected card fixture prompt: ${context.text}`);
  };

  let harness: CoreHarness | undefined;
  let primaryCommand: Promise<void> | undefined;
  let completedCommand: Promise<void> | undefined;
  let failedCommand: Promise<void> | undefined;
  let stoppedCommand: Promise<void> | undefined;
  let stopCommand: Promise<void> | undefined;
  let failedCardCommand: Promise<void> | undefined;
  try {
    harness = await createReviewHarness(prompt);
    harness.transport.reviewProgressCardSupported = true;

    primaryCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-primary-held",
      text: primaryText,
    }));
    await within(primaryCommand, "card primary inbound acknowledgement");
    await within(primaryStarted.promise, "card primary worker start");
    const primaryTask = await latestTask(harness);
    expect(primaryTask).not.toBeNull();
    const primaryCard = await progressCardAt(harness, 0, "primary running progress card");
    expect(primaryCard.progress.state).toBe("running");
    expect(primaryCard.progress.runId).toBe(primaryTask!.id);

    completedCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-review-completed",
      text: `/review ${completedFocus}`,
    }));
    await within(completedCommand, "completed review card inbound acknowledgement");
    await within(completedReviewStarted.promise, "completed review prompt start");
    const completedCard = await progressCardAt(harness, 1, "review running progress card");
    expect(completedCard.progress.state).toBe("running");
    expect(completedCard.messageId).not.toBe(primaryCard.messageId);
    expect(await harness.service.status(DM)).toMatchObject({ state: "running", queued: 0 });

    releaseCompletedReview.resolve();
    await waitForText(
      harness,
      0,
      text => text.includes("CARD_REVIEW_RESULT"),
      "completed review result text",
    );
    const completedEdit = await waitForProgressCardState(
      harness,
      completedCard.messageId,
      "completed",
      "review completed progress-card edit",
    );
    expect(completedEdit.target).toEqual(DM);
    expect(primaryCard.progress.state).toBe("running");
    await waitForReviewRuntimeCleanup(harness, "card-review-completed", "completed review lane cleanup");

    failedCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-review-failed",
      text: `/review ${failedFocus}`,
    }));
    await within(failedCommand, "failed review card inbound acknowledgement");
    const failedCard = await progressCardAt(harness, 2, "failed review progress card");
    const failedEdit = await waitForProgressCardState(
      harness,
      failedCard.messageId,
      "failed",
      "review failed progress-card edit",
    );
    expect(failedEdit.target).toEqual(DM);
    await waitForReviewRuntimeCleanup(harness, "card-review-failed", "failed review lane cleanup");

    stoppedCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-review-stopped",
      text: `/review ${stoppedFocus}`,
    }));
    await within(stoppedCommand, "stopped review card inbound acknowledgement");
    await within(stoppedReviewStarted.promise, "stopped review prompt start");
    const stoppedCard = await progressCardAt(harness, 3, "stopped review progress card");
    expect(stoppedCard.progress.state).toBe("running");
    stopCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-review-stop-command",
      text: "/review stop",
    }));
    await within(stopCommand, "stop review with progress-card inbound acknowledgement");
    const stoppedEdit = await waitForProgressCardState(
      harness,
      stoppedCard.messageId,
      "cancelled",
      "review stopped progress-card edit",
    );
    expect(stoppedEdit.target).toEqual(DM);
    await waitForReviewRuntimeCleanup(harness, "card-review-stopped", "stopped review lane cleanup");

    const ids = [
      primaryCard.messageId,
      completedCard.messageId,
      failedCard.messageId,
      stoppedCard.messageId,
    ];
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set([
      primaryCard.progress.runId,
      completedCard.progress.runId,
      failedCard.progress.runId,
      stoppedCard.progress.runId,
    ]).size).toBe(ids.length);
    expect(harness.transport.reviewProgressCardEdits
      .some(edit => edit.messageId === primaryCard.messageId && edit.progress.state !== "running")).toBe(false);

    harness.transport.reviewProgressCardSendFailure = new Error("fixture progress card create failed");
    const sendsBeforeFailure = harness.transport.reviewProgressCardSendAttempts;
    const textStart = harness.transport.sent.length;
    failedCardCommand = harness.service.submitInbound(inbound({
      ...DM,
      messageId: "card-send-failure-review",
      text: `/review ${failedCardFocus}`,
    }));
    await within(failedCardCommand, "review with progress-card transport-failure inbound acknowledgement");
    await waitForText(
      harness,
      textStart,
      text => text.includes("已受理独立代码审查"),
      "review text receipt after progress-card failure",
    );
    await waitForText(
      harness,
      textStart,
      text => text.includes("CARD_TEXT_FALLBACK_RESULT"),
      "review result after progress-card failure",
    );
    expect(harness.transport.reviewProgressCardSendAttempts).toBeGreaterThan(sendsBeforeFailure);

    releasePrimary.resolve();
    await waitForPrimaryStatus(harness, "completed");
    await waitForProgressCardState(
      harness,
      primaryCard.messageId,
      "completed",
      "primary completed progress-card edit",
    );
  } finally {
    releasePrimary.resolve();
    releaseCompletedReview.resolve();
    releaseStoppedReview.resolve();
    await settle(stopCommand);
    await settle(failedCardCommand);
    await settle(stoppedCommand);
    await settle(failedCommand);
    await settle(completedCommand);
    await settle(primaryCommand);
    if (harness) await within(closeCoreHarness(harness), "card CoreService harness shutdown").catch(() => {});
  }
});
