import { expect, test } from "bun:test";
import { mkdtemp, realpath, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  ensureProjectSessionDir,
  projectSessionDir,
  projectSessionKey,
  sessionsRoot,
} from "../src/session/session-dir";
import { closeCoreHarness, createCoreHarness, inbound } from "./fixtures/core-fakes";

test("项目目录名对同一个 cwd 稳定，同名目录互不混淆", () => {
  const first = join("/workspace", "app");
  const same = join("/workspace", "app");
  const other = join("/elsewhere", "app");

  expect(projectSessionKey(first)).toBe(projectSessionKey(same));
  expect(projectSessionKey(first)).not.toBe(projectSessionKey(other));
  expect(projectSessionKey(first)).toMatch(/^app-[0-9a-f]{8}$/u);
  expect(projectSessionKey(join("/workspace", "工作区"))).toMatch(/^project-[0-9a-f]{8}$/u);
});

test("ensureProjectSessionDir 以私有权限创建项目目录", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-channel-octo-session-dir-")));
  try {
    const cwd = join(root, "project");
    const rootDir = join(root, "sessions");
    const directory = await ensureProjectSessionDir(rootDir, cwd);

    expect(directory).toBe(projectSessionDir(rootDir, cwd));
    const info = await stat(directory);
    expect(info.isDirectory()).toBe(true);
    expect(info.mode & 0o777).toBe(0o700);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("sessionsRoot 未配置时回退到状态目录，配置优先", () => {
  expect(sessionsRoot({ stateDir: "/state" })).toBe(join("/state", "sessions"));
  expect(sessionsRoot({ stateDir: "/state", sessionsDir: "/oco/sessions" })).toBe("/oco/sessions");
});

test("新建会话与任务都把 worker 的会话落在该项目目录下", async () => {
  const harness = await createCoreHarness();
  try {
    const cwd = await realpath(await mkdtemp(join(harness.root, "project-")));
    const target = { channelId: "dm-project-session-dir", channelType: 1 as const };
    const root = sessionsRoot(harness.config);
    const projectDir = projectSessionDir(root, cwd);

    await harness.service.handleInbound(
      inbound({ messageId: "project-session-dir-new", ...target, text: `/new ${cwd}` }),
    );
    const newWorker = harness.workers.workers.at(-1);
    expect(newWorker).toBeDefined();
    expect(dirname(newWorker!.sessionFile)).toBe(projectDir);

    await harness.service.handleInbound(
      inbound({ messageId: "project-session-dir-task", ...target, text: "运行一个回合" }),
    );
    await harness.workers.waitForPromptCount(1);
    const taskWorker = harness.workers.workers.at(-1);
    expect(taskWorker).toBeDefined();
    expect(dirname(taskWorker!.sessionFile)).toBe(projectDir);
  } finally {
    await closeCoreHarness(harness);
  }
});
