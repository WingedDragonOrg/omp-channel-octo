import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { ChatTarget, RestartPort } from "../src/contracts";
import type { ChannelStore } from "../src/store";
import { openChannelStore } from "../src/store";
import type { TaskRecord } from "../src/store/store-records";
import {
  FakeTransport,
  FakeWorkerFactory,
  closeCoreHarness,
  type CoreHarness,
  type FakePrompt,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

/** A transport whose startup fails after the recovery batch was already scheduled. */
class FailingStartTransport extends FakeTransport {
  async start(): Promise<void> {
    throw new Error("fixture transport failed to start");
  }
}

function recordingPort(): { port: RestartPort; calls: () => number } {
  let calls = 0;
  return {
    port: {
      request: async (): Promise<void> => {
        calls += 1;
      },
    },
    calls: () => calls,
  };
}

const textsFor = (harness: CoreHarness, target: ChatTarget): string[] =>
  harness.transport.sent
    .filter(({ target: sent }) => sent.channelId === target.channelId && sent.channelType === target.channelType)
    .map(({ text }) => text);

/**
 * A turn that stays in flight until its chat stops, then fails like the real RPC worker does when
 * `stop()` rejects the active prompt. That is the state a restart (graceful or crash) interrupts
 * in: the task row must keep its startup/prompt markers for the next process to resume.
 */
const suspendedPrompt: FakePrompt = async ({ isStopped, onStop }) => {
  await new Promise<void>((resolve) => {
    onStop(resolve);
    if (isStopped()) resolve();
  });
  if (isStopped()) throw new Error("OMP prompt cancelled");
};

/** The prompt handler a restarted process uses: record the dispatched text, then settle. */
const recordingPrompt: FakePrompt = async ({ text, append }) => {
  await append("effects.log", `${text}\n`);
};

async function withStore<T>(root: string, work: (store: ChannelStore) => T): Promise<T> {
  const store = await openChannelStore(join(root, "state"));
  try {
    return work(store);
  } finally {
    store.close();
  }
}

function seedQueuedTask(store: ChannelStore, task: TaskRecord): void {
  store.acceptTask(
    {
      botUid: task.botUid,
      target: { channelType: task.channelType, channelId: task.channelId },
      messageId: task.messageId,
      senderId: task.senderId,
      text: task.text,
      supported: true,
      kind: "task",
    },
    task,
  );
}

function taskRow(overrides: Partial<TaskRecord> & Pick<TaskRecord, "id" | "botUid" | "channelId" | "channelType">): TaskRecord {
  return {
    messageId: `message-${overrides.id}`,
    senderId: "owner-1",
    text: "seeded work",
    sessionId: "session-seeded",
    cwd: "/tmp",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relayChannelType: null,
    relayChannelId: null,
    relayThreadName: null,
    ...overrides,
  };
}

test("a restart without a detected supervisor is refused instead of guessed", async () => {
  const harness = await createCoreHarness();
  const target: ChatTarget = { channelId: "dm-restart-unconfigured", channelType: 1 };
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "restart-unconfigured", ...target, text: "/restart" }),
    );
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    expect(textsFor(harness, target).some((text) => text.includes("未检测到守护进程"))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("restart arguments other than now are rejected with usage", async () => {
  const target: ChatTarget = { channelId: "dm-restart-usage", channelType: 1 };
  const { port, calls } = recordingPort();
  const harness = await createCoreHarness({ restart: port });
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "restart-usage", ...target, text: "/restart maybe" }),
    );
    await waitForStatus(harness.service, target, (status) => status.outbox === 0);
    expect(textsFor(harness, target).join("\n")).toContain("用法：/restart [now]");
    expect(calls()).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a restart is accepted while work is running and never cancels it", async () => {
  const busy: ChatTarget = { channelId: "dm-restart-busy", channelType: 1 };
  const requester: ChatTarget = { channelId: "dm-restart-requester", channelType: 1 };
  const { port, calls } = recordingPort();
  const harness = await createCoreHarness({ restart: port, prompt: suspendedPrompt });
  try {
    await harness.service.handleInbound(inbound({ messageId: "restart-busy-task", ...busy, text: "hold" }));
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({ messageId: "restart-while-busy", ...requester, text: "/restart" }),
    );
    await waitForStatus(harness.service, requester, (status) => status.outbox === 0);
    expect(calls()).toBe(1);
    expect(textsFor(harness, requester).join("\n")).toContain("正在重启 channel");
    const row = await withStore(harness.root, (store) =>
      store.getLatestTask(harness.bot.uid, busy),
    );
    expect(row?.state).toBe("running");
    expect(await withStore(harness.root, (store) => store.isTaskResuming(row!.id))).toBe(true);
    expect(textsFor(harness, busy).join("\n")).not.toContain("任务已取消");
    // `now` stays valid syntax and behaves identically.
    await harness.service.handleInbound(
      inbound({ messageId: "restart-now-again", ...requester, text: "/restart now" }),
    );
    await waitForStatus(harness.service, requester, (status) => status.outbox === 0);
    expect(calls()).toBe(2);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a restart resumes the interrupted turn in its own session without replaying its input", async () => {
  const target: ChatTarget = { channelId: "dm-resume", channelType: 1 };
  const first = await createCoreHarness({
    prompt: async (context) => {
      context.emit({ type: "text", messageId: "assistant-1", text: "第一步已保存", final: true });
      await suspendedPrompt(context);
    },
  });
  try {
    await first.service.handleInbound(inbound({ messageId: "resume-running", ...target, text: "长任务第一步" }));
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, target, (status) =>
      status.outbox === 0 && textsFor(first, target).includes("第一步已保存"));
  } finally {
    await closeCoreHarness(first, { remove: false });
  }
  const settled = await withStore(first.root, (store) => {
    const row = store.getLatestTask(first.bot.uid, target);
    return { state: row?.state, resumable: row ? store.isTaskResuming(row.id) : false };
  });
  // Stopping preserves the startup state: the row stays in flight with its prompt accepted.
  expect(settled.state).toBe("running");
  expect(settled.resumable).toBe(true);
  const bindingBefore = await withStore(first.root, (store) => store.getBinding(first.bot.uid, target));

  const restarted = await createCoreHarness({
    root: first.root,
    bot: first.bot,
    prompt: async (context) => {
      await recordingPrompt(context);
      context.emit({ type: "text", messageId: "assistant-1", text: "接着完成后续步骤", final: true });
    },
  });
  try {
    await waitForStatus(restarted.service, target, (status) => status.state === "completed" && status.outbox === 0);
    expect(restarted.workers.prompts.map((prompt) => prompt.text)).toEqual(["/retry"]);
    expect((await readText(join(first.root, "tmp", "effects.log"))).trim().split("\n")).toEqual(["/retry"]);
    // 续接沿用同一会话：绑定行没有被换掉。
    expect(await withStore(restarted.root, (store) => store.getBinding(first.bot.uid, target)))
      .toEqual(bindingBefore);
    const delivered = textsFor(restarted, target).join("\n");
    expect(delivered).toContain("接着完成后续步骤");
    expect(delivered).not.toContain("因重启中断");
    expect(delivered).not.toContain("任务已取消");
    expect(delivered).not.toContain("任务已中断");
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("queued work survives a restart and runs behind the resumed turn", async () => {
  const target: ChatTarget = { channelId: "dm-resume-queue", channelType: 1 };
  const first = await createCoreHarness({ prompt: suspendedPrompt });
  try {
    await first.service.handleInbound(inbound({ messageId: "queue-running", ...target, text: "running step" }));
    await first.workers.waitForPromptCount(1);
    await first.service.handleInbound(
      inbound({ messageId: "queue-second", ...target, text: "/queue 排队步骤" }),
    );
    await waitForStatus(first.service, target, (status) => status.queued === 1);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({
    root: first.root,
    bot: first.bot,
    prompt: recordingPrompt,
  });
  try {
    await waitForStatus(
      restarted.service,
      target,
      (status) => status.state === "completed" && status.queued === 0 && status.outbox === 0,
    );
    expect(restarted.workers.prompts.map((prompt) => prompt.text)).toEqual(["/retry", "排队步骤"]);
    expect((await readText(join(first.root, "tmp", "effects.log"))).trim().split("\n")).toEqual([
      "/retry",
      "排队步骤",
    ]);
    // Both recovered rows are terminal and nothing is left queued.
    await restarted.service.stop();
    const rows = await withStore(restarted.root, (store) => ({
      latest: store.getLatestTask(first.bot.uid, target)?.state,
      queued: store.listQueuedTasks().length,
    }));
    expect(rows.latest).toBe("completed");
    expect(rows.queued).toBe(0);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("an explicit cancel stays terminal across a restart and is never resumed", async () => {
  const target: ChatTarget = { channelId: "dm-cancel-restart", channelType: 1 };
  const first = await createCoreHarness({ prompt: suspendedPrompt });
  try {
    await first.service.handleInbound(inbound({ messageId: "cancel-running", ...target, text: "cancel me" }));
    await first.workers.waitForPromptCount(1);
    await first.service.handleInbound(inbound({ messageId: "cancel-command", ...target, text: "/cancel" }));
    await waitForStatus(first.service, target, (status) => status.state === "cancelled" && status.outbox === 0);
    expect(textsFor(first, target).join("\n")).toContain("当前任务已取消");
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    // Stopping settles every scheduled recovery before the negative assertions run.
    await restarted.service.stop();
    expect(restarted.workers.prompts).toHaveLength(0);
    expect(await withStore(restarted.root, (store) => store.getLatestTask(first.bot.uid, target)?.state))
      .toBe("cancelled");
    expect(textsFor(restarted, target)).toEqual([]);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("legacy restart-notice rows are consumed silently", async () => {
  const target: ChatTarget = { channelId: "dm-restart-notice", channelType: 1 };
  const first = await createCoreHarness();
  const botUid = first.bot.uid;
  await closeCoreHarness(first, { remove: false });
  await withStore(first.root, (store) => {
    store.addRestartNotice({
      id: "legacy-notice",
      botUid,
      channelType: target.channelType,
      channelId: target.channelId,
      requestedAt: Date.now(),
    });
  });

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await restarted.service.stop();
    expect(textsFor(restarted, target)).toEqual([]);
    expect(await withStore(restarted.root, (store) => store.listRestartNotices(botUid))).toEqual([]);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("an unverifiable recovery input is settled interrupted instead of replayed", async () => {
  const target: ChatTarget = { channelId: "dm-uncertain", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });
  await withStore(first.root, (store) => {
    seedQueuedTask(
      store,
      taskRow({ id: "uncertain-task", botUid: first.bot.uid, ...target, cwd: join(first.root, "tmp") }),
    );
    store.markStarted("uncertain-task");
    // prompt 已落标记、OMP 是否接受未知（prompt_accepted 保持 0）：不得回放，也不得误续接。
    store.markTaskPromptStarted("uncertain-task", "session-uncertain");
  });

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await waitForStatus(restarted.service, target, (status) => status.state === "interrupted" && status.outbox === 0);
    expect(textsFor(restarted, target).join("\n")).toContain("任务未自动恢复");
    await restarted.service.stop();
    expect(restarted.workers.prompts).toHaveLength(0);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a recovery whose session file is gone fails closed without rebinding", async () => {
  const target: ChatTarget = { channelId: "dm-missing-session", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });
  const missingSessionFile = join(first.root, "sessions", "gone.jsonl");
  await withStore(first.root, (store) => {
    store.ensureBinding({
      botUid: first.bot.uid,
      channelType: target.channelType,
      channelId: target.channelId,
      sessionId: "session-gone",
      cwd: join(first.root, "tmp"),
      sessionFile: missingSessionFile,
    });
    seedQueuedTask(
      store,
      taskRow({
        id: "missing-session-task",
        botUid: first.bot.uid,
        ...target,
        sessionId: "session-gone",
        cwd: join(first.root, "tmp"),
      }),
    );
    store.markStarted("missing-session-task");
    store.markTaskPromptStarted("missing-session-task", "session-gone");
    store.markTaskPromptAccepted("missing-session-task");
  });

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await waitForStatus(restarted.service, target, (status) => status.state === "failed" && status.outbox === 0);
    expect(textsFor(restarted, target).join("\n")).toContain("任务未启动：绑定的工作目录或会话不可用（session-invalid）。");
    await restarted.service.stop();
    expect(restarted.workers.prompts).toHaveLength(0);
    expect(existsSync(missingSessionFile)).toBe(false);
    const binding = await withStore(restarted.root, (store) => store.getBinding(first.bot.uid, target));
    expect(binding?.sessionId).toBe("session-gone");
    expect(binding?.sessionFile).toBe(missingSessionFile);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a failed startup never runs recovered tasks", async () => {
  const target: ChatTarget = { channelId: "dm-startup-failed", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });
  await withStore(first.root, (store) => {
    seedQueuedTask(
      store,
      taskRow({ id: "startup-failed-task", botUid: first.bot.uid, ...target, cwd: join(first.root, "tmp") }),
    );
  });

  const workers = new FakeWorkerFactory();
  const transport = new FailingStartTransport({ bot: first.bot, startOnline: true });
  let failure: unknown;
  try {
    await createCoreHarness({ root: first.root, bot: first.bot, transport, workers });
  } catch (error) {
    failure = error;
  }
  expect(failure).toBeInstanceOf(Error);
  // 启动门拒绝后，已排定的恢复任务一次都不执行：没有 worker，行仍在队列等下一次进程。
  expect(workers.creations).toHaveLength(0);
  expect(workers.prompts).toHaveLength(0);
  expect(await withStore(first.root, (store) => store.getTask("startup-failed-task")?.state)).toBe("queued");
  await rm(first.root, { recursive: true, force: true });
});

test("an explicit interrupt stays terminal when the process stops before the turn unwinds", async () => {
  const target: ChatTarget = { channelId: "dm-interrupt-stop", channelType: 1 };
  const first = await createCoreHarness({
    prompt: async ({ waitForAbort, isStopped, onStop }) => {
      // 等 /interrupt 的中止信号，然后像真 worker 一样让停机把这条 prompt 打成拒绝。
      await waitForAbort();
      await new Promise<void>((resolve) => {
        onStop(resolve);
        if (isStopped()) resolve();
      });
      if (isStopped()) throw new Error("OMP prompt cancelled");
    },
  });
  try {
    await first.service.handleInbound(inbound({ messageId: "interrupt-stop-task", ...target, text: "hold" }));
    await first.workers.waitForPromptCount(1);
    await first.service.handleInbound(
      inbound({ messageId: "interrupt-stop-command", ...target, text: "/interrupt" }),
    );
    await waitForStatus(first.service, target, (status) => status.outbox === 0);
    expect(textsFor(first, target).join("\n")).toContain("已中断当前回合");
    await first.service.stop();
  } finally {
    await closeCoreHarness(first, { remove: false });
  }
  expect(await withStore(first.root, (store) => store.getLatestTask(first.bot.uid, target)?.state))
    .toBe("interrupted");

  // 用户的中断不能被一次重启续上：新进程既不续接也不重放。
  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await restarted.service.stop();
    expect(restarted.workers.prompts).toHaveLength(0);
    expect(await withStore(restarted.root, (store) => store.getLatestTask(first.bot.uid, target)?.state))
      .toBe("interrupted");
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a resumed group turn keeps its context injection receipt and commits it", async () => {
  const group: ChatTarget = { channelId: "group-a", channelType: 2 };
  const context = {
    content: "Group A instructions",
    version: 3,
    updatedAt: "2026-09-12T00:00:00.000Z",
    updatedBy: "admin-a",
  };
  const first = await createCoreHarness({ prompt: suspendedPrompt });
  let sessionId = "";
  try {
    first.transport.setGroupContext(group, context);
    await first.service.handleInbound(
      inbound({
        messageId: "group-resume-task",
        ...group,
        text: "群任务",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await first.workers.waitForPromptCount(1);
    sessionId = first.workers.prompts[0]?.sessionId ?? "";
    expect(sessionId.length).toBeGreaterThan(0);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const transport = new FakeTransport({ bot: first.bot, startOnline: true });
  transport.setGroupContext(group, context);
  const restarted = await createCoreHarness({
    root: first.root,
    bot: first.bot,
    transport,
    prompt: recordingPrompt,
  });
  try {
    await waitForStatus(restarted.service, group, (status) => status.state === "completed" && status.outbox === 0);
    expect(restarted.workers.prompts.map((prompt) => prompt.text)).toEqual(["/retry"]);
    // 续跑回合沿用了同一会话的注入事务，并在真实回合结束时提交 ledger。
    const ledger = await withStore(restarted.root, (store) =>
      store.getContextLedger(first.bot.uid, group, sessionId),
    );
    expect(ledger?.phase).toBe("committed");
    expect(ledger?.version).toBe(context.version);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a recovery whose pinned workspace is gone fails closed without rebinding", async () => {
  const target: ChatTarget = { channelId: "dm-pinned-gone", channelType: 1 };
  const freshTarget: ChatTarget = { channelId: "dm-pinned-gone-fresh", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });
  const pinnedDir = join(first.root, "workspace-gone");
  await mkdir(pinnedDir, { recursive: true });
  const sessionFile = join(first.root, "sessions", "pinned.jsonl");
  await mkdir(dirname(sessionFile), { recursive: true });
  await writeFile(
    sessionFile,
    `${JSON.stringify({ type: "session", version: 3, id: "session-pinned", cwd: pinnedDir })}\n`,
    "utf8",
  );
  await withStore(first.root, (store) => {
    store.ensureBinding({
      botUid: first.bot.uid,
      channelType: target.channelType,
      channelId: target.channelId,
      sessionId: "session-pinned",
      cwd: pinnedDir,
      sessionFile,
      cwdPinned: true,
    });
    seedQueuedTask(
      store,
      taskRow({
        id: "pinned-gone-task",
        botUid: first.bot.uid,
        ...target,
        sessionId: "session-pinned",
        cwd: pinnedDir,
      }),
    );
    store.markStarted("pinned-gone-task");
    store.markTaskPromptStarted("pinned-gone-task", "session-pinned");
    store.markTaskPromptAccepted("pinned-gone-task");
    // A new task keeps the ordinary pinned-workspace fallback: no verified session yet, so the
    // default work directory is resolved instead of failing the acceptance.
    store.ensureBinding({
      botUid: first.bot.uid,
      channelType: freshTarget.channelType,
      channelId: freshTarget.channelId,
      sessionId: "session-fresh",
      cwd: pinnedDir,
      sessionFile: null,
      cwdPinned: true,
    });
  });
  await rm(pinnedDir, { recursive: true, force: true });

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await waitForStatus(restarted.service, target, (status) => status.state === "failed" && status.outbox === 0);
    // 原绑定原样保留：没有换会话、没有把 cwd 落到默认目录、也没有重建会话文件。
    const binding = await withStore(restarted.root, (store) => store.getBinding(first.bot.uid, target));
    expect(binding?.sessionId).toBe("session-pinned");
    expect(binding?.sessionFile).toBe(sessionFile);
    expect(binding?.cwd).toBe(pinnedDir);
    expect(JSON.parse(await readText(sessionFile))).toMatchObject({ id: "session-pinned", cwd: pinnedDir });

    await restarted.service.handleInbound(
      inbound({ messageId: "pinned-gone-fresh", ...freshTarget, text: "new work" }),
    );
    await waitForStatus(restarted.service, freshTarget, (status) => status.state === "completed" && status.outbox === 0);
    const freshBinding = await withStore(restarted.root, (store) => store.getBinding(first.bot.uid, freshTarget));
    expect(freshBinding?.cwd).toBe(join(first.root, "tmp"));
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("an unconfirmed worker death keeps the task queued instead of failing it", async () => {
  const blocked: ChatTarget = { channelId: "dm-blocked", channelType: 1 };
  const later: ChatTarget = { channelId: "dm-blocked-later", channelType: 1 };
  const first = await createCoreHarness();
  await closeCoreHarness(first, { remove: false });
  const cwd = join(first.root, "tmp");
  await withStore(first.root, (store) => {
    store.recordWorker({
      id: "ledger-unknown",
      botUid: first.bot.uid,
      sessionId: "session-unknown",
      sessionFile: join(cwd, "unknown.jsonl"),
      pid: 4242,
      pgid: 4242,
      identity: "fixture-unknown",
      cwd,
      moveTargetCwd: null,
      state: "running",
    });
    seedQueuedTask(
      store,
      taskRow({ id: "blocked-task", botUid: first.bot.uid, ...blocked, sessionId: "session-unknown", cwd }),
    );
  });

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    // The same directory must not accept a second writer while the old group is unconfirmed.
    await restarted.service.handleInbound(
      inbound({ messageId: "blocked-later-message", ...later, text: "later work" }),
    );
    await waitForStatus(restarted.service, later, (status) => status.queued === 1);
    await restarted.service.stop();
    expect(restarted.workers.prompts).toHaveLength(0);
    const rows = await withStore(restarted.root, (store) => ({
      blocked: store.getTask("blocked-task")?.state,
      later: store.getLatestTask(first.bot.uid, later)?.state,
      queued: store.listQueuedTasks().length,
    }));
    expect(rows.blocked).toBe("queued");
    expect(rows.later).toBe("queued");
    expect(rows.queued).toBe(2);
    expect(textsFor(restarted, blocked)).toEqual([]);
    expect(textsFor(restarted, later)).toEqual([]);
  } finally {
    await closeCoreHarness(restarted);
  }
});
