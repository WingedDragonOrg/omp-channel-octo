import { Database } from "bun:sqlite";
import { mkdtemp, rm, writeFile, chmod } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, vi } from "bun:test";
import { TaskStartupError } from "../src/contracts.ts";
import { createWorkerFactory } from "../src/omp/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";

type TaskRow = { id: string; state: string; error_text: string | null };

function taskRow(harness: CoreHarness, messageId: string): TaskRow {
  const database = new Database(join(harness.stateDir, "channel.sqlite"));
  try {
    const row = database
      .query("SELECT id, state, error_text FROM tasks WHERE message_id = ?")
      .get(messageId) as TaskRow | null;
    if (!row) throw new Error(`no task row for ${messageId}`);
    return row;
  } finally {
    database.close();
  }
}

function sentTexts(harness: CoreHarness): string[] {
  return harness.transport.sent.map(({ text }) => text);
}

function taskLogLines(calls: ReadonlyArray<readonly unknown[]>): string[] {
  return calls
    .map(args => String(args[0] ?? ""))
    .filter(line => line.startsWith("[task] "));
}

test("a group-context read failure keeps its own notice and records the real reason", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ["group-startup-context"] });
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    const group = { channelId: "group-startup-context", channelType: 2 as const };
    harness.transport.failGroupContextWith(new Error("Octo group context request failed with HTTP 500"));
    await harness.service.handleInbound(
      inbound({
        messageId: "startup-context-failure",
        ...group,
        text: "must not run",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const status = await waitForStatus(harness.service, group, (next) => next.state === "failed");
    expect(status.state).toBe("failed");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(sentTexts(harness)).toContain("群说明读取失败，未启动任务。");

    const row = taskRow(harness, "startup-context-failure");
    expect(row.state).toBe("failed");
    expect(row.error_text).toStartWith("群说明读取失败（context-read）：");
    expect(row.error_text).toContain("HTTP 500");

    const lines = taskLogLines(logged.mock.calls);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("启动失败");
    expect(lines[0]).toContain("kind=context");
    expect(lines[0]).toContain("code=context-read");
    expect(lines[0]).toContain("phase=context");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
});

test("a bound workspace that is gone reports the binding class with its code", async () => {
  const harness = await createCoreHarness({ allowedGroupIds: ["group-startup-binding"] });
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    const group = { channelId: "group-startup-binding", channelType: 2 as const };
    harness.workers.failCreateWith(
      new TaskStartupError("cwd-missing", "OMP worker cwd does not exist: /gone/workspace"),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "startup-binding-failure",
        ...group,
        text: "must not run either",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const status = await waitForStatus(harness.service, group, (next) => next.state === "failed");
    expect(status.state).toBe("failed");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(sentTexts(harness)).toContain("任务未启动：绑定的工作目录或会话不可用（cwd-missing）。");

    const row = taskRow(harness, "startup-binding-failure");
    expect(row.error_text).toStartWith("绑定不可用（cwd-missing）：");
    expect(row.error_text).toContain("cwd-missing");
    expect(row.error_text).toContain("/gone/workspace");

    const lines = taskLogLines(logged.mock.calls);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("kind=binding");
    expect(lines[0]).toContain("code=cwd-missing");
    expect(lines[0]).toContain("phase=startup");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
});

test("a worker failure in a private chat never reports the group-context wording", async () => {
  const harness = await createCoreHarness();
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    const direct = { channelId: harness.bot.uid, channelType: 1 as const };
    harness.workers.failCreateWith(new Error("ui.loop-blocked"));
    await harness.service.handleInbound(
      inbound({ messageId: "startup-private-failure", ...direct, text: "private task" }),
    );
    const status = await waitForStatus(harness.service, direct, (next) => next.state === "failed");
    expect(status.state).toBe("failed");
    const texts = sentTexts(harness);
    expect(texts.some((text) => text.includes("群说明读取失败"))).toBe(false);
    expect(texts).toContain("任务未启动：OMP 运行进程启动失败（worker-start）。");

    const row = taskRow(harness, "startup-private-failure");
    expect(row.error_text).toStartWith("worker 启动失败（worker-start）：");
    expect(row.error_text).toContain("ui.loop-blocked");

    const lines = taskLogLines(logged.mock.calls);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("kind=worker");
    expect(lines[0]).toContain("phase=startup");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
});

test("a failure after the worker exists keeps the generic notice and records the real reason", async () => {
  const harness = await createCoreHarness({
    allowedGroupIds: ["group-startup-runtime"],
    prompt: async () => {
      throw new Error("OMP worker already has a prompt in progress");
    },
  });
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    const group = { channelId: "group-startup-runtime", channelType: 2 as const };
    await harness.service.handleInbound(
      inbound({
        messageId: "task-runtime-failure",
        ...group,
        text: "run and fail",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const status = await waitForStatus(harness.service, group, (next) => next.state === "failed");
    expect(status.state).toBe("failed");
    expect(sentTexts(harness)).toContain("任务执行失败；未能可靠完成。");

    const row = taskRow(harness, "task-runtime-failure");
    expect(row.error_text).toStartWith("任务执行失败（task-failed）：");
    expect(row.error_text).toContain("already has a prompt in progress");

    const lines = taskLogLines(logged.mock.calls);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("执行失败");
    expect(lines[0]).toContain("kind=generic");
    expect(lines[0]).toContain("code=task-failed");
  } finally {
    logged.mockRestore();
    await closeCoreHarness(harness);
  }
});

test("the worker factory classifies a missing cwd before spawning anything", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "oco-startup-"));
  try {
    const factory = createWorkerFactory({ ompCommand: "omp" });
    const error = await factory
      .create({
        cwd: join(workspace, "gone"),
        sessionDir: workspace,
        onSpawn: () => {
          throw new Error("a worker must not be spawned for a missing cwd");
        },
      })
      .then(
        () => undefined,
        (failure: unknown) => failure,
      );
    expect(error).toBeInstanceOf(TaskStartupError);
    expect((error as TaskStartupError).code).toBe("cwd-missing");
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test("the worker factory reports not-ready when the child never becomes ready", async () => {
  const workspace = await mkdtemp(join(tmpdir(), "oco-startup-"));
  const script = join(workspace, "never-ready.sh");
  try {
    await writeFile(script, "#!/bin/sh\nsleep 30\n", "utf8");
    await chmod(script, 0o755);
    const factory = createWorkerFactory({ ompCommand: script, startupTimeoutMs: 300 });
    const error = await factory
      .create({ cwd: workspace, sessionDir: workspace, onSpawn: () => {} })
      .then(
        () => undefined,
        (failure: unknown) => failure,
      );
    expect(error).toBeInstanceOf(TaskStartupError);
    expect((error as TaskStartupError).code).toBe("not-ready");
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});
