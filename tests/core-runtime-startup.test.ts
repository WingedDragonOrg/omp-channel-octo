import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, realpath, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { EnsureAgentRuntimeResult } from "../src/runtime/agent-runtime.ts";
import { openChannelStore } from "../src/store/index.ts";
import { closeCoreHarness, createCoreHarness, inbound, submitAndWaitForIngressHandoff, type CoreHarness } from "./fixtures/core-fakes.ts";

test("a v2 runtime starts and completes a real group worker", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-startup-normal-")));
  const cwd = join(root, "tmp");
  const command = join(root, "fixture-omp");
  const bot = { uid: "fixture-normal", name: "Fixture Bot" };
  const target = { channelId: "group-normal", channelType: 2 as const };
  let harness: CoreHarness | undefined;
  let runtime: EnsureAgentRuntimeResult | undefined;
  try {
    await mkdir(cwd, { recursive: true });
    const fixture = join(import.meta.dir, "fixtures/omp-rpc-worker.ts");
    await writeFile(command, `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(fixture)} --fixture-scenario normal --fixture-context-receipt "$@"\n`);
    await chmod(command, 0o755);
    harness = await createCoreHarness({ root, bot, allowedGroupIds: [target.channelId], agentRuntime: { ompCommand: command, startupTimeoutMs: 5_000 } });
    const service = harness.service as unknown as {
      agentRuntime: EnsureAgentRuntimeResult;
      scheduler: { waitForTaskSettlement(taskId: string): Promise<void> };
    };
    runtime = service.agentRuntime;
    expect(runtime.client.protocolVersion).toBe(2);
    await submitAndWaitForIngressHandoff(harness, inbound({
      ...target, messageId: "normal-group", text: "start", mentionedUserIds: [bot.uid],
    }));
    const store = await openChannelStore(harness.stateDir);
    try {
      const task = store.getLatestTask(bot.uid, target)!;
      await service.scheduler.waitForTaskSettlement(task.id);
      expect(store.getTask(task.id)?.state).toBe("completed");
      const output = await harness.transport.waitForRemoteText(message => message.target.channelId === target.channelId && message.text.includes("fixture"));
      expect(output.text).toContain("fixture");
      const snapshot = (await runtime.client.listWorkers()).find(worker => worker.taskId === task.id);
      expect(snapshot?.dispatches).toHaveLength(1);
      expect(snapshot?.dispatches[0]?.state).toBe("completed");
    } finally { store.close(); }
  } finally {
    if (harness) await closeCoreHarness(harness, { remove: false });
    if (runtime) await runtime.handle.shutdown();
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);

for (const scenario of ["exited", "exited-lost", "cwd-missing", "spawn-failed"] as const) {
  test(`group receives ${scenario} when its real runtime worker cannot start`, async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), "oco-startup-group-")));
    const cwd = join(root, "tmp");
    const command = join(root, scenario === "spawn-failed" ? "absent" : "omp");
    const bot = { uid: `fixture-${scenario}`, name: "Fixture Bot" };
    const target = { channelId: `group-${scenario}`, channelType: 2 as const };
    let harness: CoreHarness | undefined;
    let runtime: EnsureAgentRuntimeResult | undefined;
    try {
      await mkdir(cwd, { recursive: true });
      if (scenario !== "spawn-failed") {
        await writeFile(command, "#!/bin/sh\nsleep 0.2\nprintf 'private-database-password-42\\n' >&2\nexit 23\n");
        await chmod(command, 0o755);
      }
      harness = await createCoreHarness({ root, bot, allowedGroupIds: [target.channelId], agentRuntime: { ompCommand: command, startupTimeoutMs: 3_000 } });
      const service = harness.service as unknown as {
        agentRuntime: EnsureAgentRuntimeResult;
        scheduler: { waitForTaskSettlement(taskId: string): Promise<void> };
      };
      runtime = service.agentRuntime;
      const create = runtime.client.createWorker.bind(runtime.client);
      if (scenario === "cwd-missing") {
        runtime.client.createWorker = async (...args) => {
          await rename(cwd, `${cwd}-removed`);
          return await create(...args);
        };
      }
      if (scenario === "exited-lost") {
        runtime.client.createWorker = async (...args) => {
          try { return await create(...args); }
          catch { throw new Error("creation response lost"); }
        };
      }
      await submitAndWaitForIngressHandoff(harness, inbound({ ...target, messageId: `startup-${scenario}`, text: "start", mentionedUserIds: [bot.uid] }));
      const store = await openChannelStore(harness.stateDir);
      try {
        const task = store.getLatestTask(bot.uid, target)!;
        await service.scheduler.waitForTaskSettlement(task.id);
        expect(store.getTask(task.id)?.state).toBe("failed");
        const notice = await harness.transport.waitForRemoteText(message => message.target.channelId === target.channelId && message.text.includes("任务未启动"));
        expect(notice.text).toContain(scenario === "exited-lost" ? "exited" : scenario);
        expect(notice.text).not.toContain("private-database-password-42");
        if (scenario.startsWith("exited")) expect(notice.text).toContain("退出码 23");
        const snapshots = await runtime.client.listWorkers();
        expect(snapshots).toHaveLength(1);
        expect(snapshots[0]?.activeDispatchId).toBeNull();
        expect(snapshots[0]?.dispatches).toHaveLength(0);
        expect(snapshots[0]?.taskId).toBe(task.id);
        expect(snapshots[0]?.runtimeId).toBe(runtime.handle.runtimeId);
        expect(snapshots[0]?.startupCwd).toBe(cwd);
        expect(snapshots[0]?.startupFenced).toBe(true);
        if (scenario === "cwd-missing" || scenario === "spawn-failed") {
          expect(snapshots[0]?.stage).toBe("not-spawned");
          expect(snapshots[0]?.process).toBeNull();
        }
        await expect(create({
          taskId: task.id, runId: "late", workerOptions: { cwd, sessionDir: cwd },
        })).rejects.toThrow("startup is already settled");
        if (scenario === "spawn-failed") {
          await closeCoreHarness(harness, { remove: false });
          harness = await createCoreHarness({
            root, bot, allowedGroupIds: [target.channelId],
            agentRuntime: { ompCommand: command, startupTimeoutMs: 3_000 },
          });
          const restarted = harness.service as unknown as {
            agentRuntime: EnsureAgentRuntimeResult;
            scheduler: { waitForTaskSettlement(taskId: string): Promise<void> };
          };
          runtime = restarted.agentRuntime;
          await submitAndWaitForIngressHandoff(harness, inbound({
            ...target, messageId: "after-restart", text: "new task", mentionedUserIds: [bot.uid],
          }));
          const next = store.getLatestTask(bot.uid, target)!;
          expect(next.id).not.toBe(task.id);
          await restarted.scheduler.waitForTaskSettlement(next.id);
          expect(store.getTask(next.id)?.state).toBe("failed");
          expect((await runtime.client.listWorkers()).filter(worker => worker.taskId === task.id)).toHaveLength(1);
        }
      } finally { store.close(); }
    } finally {
      if (harness) await closeCoreHarness(harness, { remove: false });
      if (runtime) await runtime.handle.shutdown();
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);
}


for (const conflict of ["foreign", "cwd", "other-run", "dispatch", "ledger"] as const) {
  test(`group startup remains unconfirmed with ${conflict}`, async () => {
    const root = await realpath(await mkdtemp(join(tmpdir(), "oco-startup-conflict-")));
    const cwd = join(root, "tmp");
    const bot = { uid: `fixture-${conflict}`, name: "Fixture Bot" };
    const target = { channelId: `group-${conflict}`, channelType: 2 as const };
    let harness: CoreHarness | undefined;
    let runtime: EnsureAgentRuntimeResult | undefined;
    let store: Awaited<ReturnType<typeof openChannelStore>> | undefined;
    try {
      await mkdir(cwd, { recursive: true });
      harness = await createCoreHarness({
        root, bot, allowedGroupIds: [target.channelId],
        agentRuntime: { ompCommand: join(root, "absent"), startupTimeoutMs: 3_000 },
      });
      const service = harness.service as unknown as {
        agentRuntime: EnsureAgentRuntimeResult;
        scheduler: { waitForTaskSettlement(taskId: string): Promise<void> };
      };
      runtime = service.agentRuntime;
      store = await openChannelStore(harness.stateDir);
      const list = runtime.client.listWorkers.bind(runtime.client);
      runtime.client.listWorkers = async () => {
        const snapshots = await list();
        const snapshot = snapshots[0]!;
        if (conflict === "foreign") return [{ ...snapshot, runtimeId: "foreign-runtime" }];
        if (conflict === "cwd") return [{ ...snapshot, startupCwd: join(root, "elsewhere") }];
        if (conflict === "other-run") return [snapshot, { ...snapshot, runId: "other-run", stage: "active" }];
        if (conflict === "dispatch") return [{
          ...snapshot, dispatches: [{ dispatchId: "uncertain", kind: "prompt", state: "unknown", nextSeq: 0 }],
        }];
        store!.recordWorker({
          id: snapshot.taskId, botUid: "foreign-bot", sessionId: "foreign-session", sessionFile: null,
          pid: 123456789, pgid: 123456789, identity: "foreign-process", cwd,
          moveTargetCwd: null, runtimeId: "foreign-runtime", state: "running",
        });
        return snapshots;
      };
      await submitAndWaitForIngressHandoff(harness, inbound({
        ...target, messageId: `conflict-${conflict}`, text: "start", mentionedUserIds: [bot.uid],
      }));
      const task = store.getLatestTask(bot.uid, target)!;
      await service.scheduler.waitForTaskSettlement(task.id);
      expect(store.getTask(task.id)?.state).toBe("running");
      expect(harness.transport.sent.some(message => message.target.channelId === target.channelId
        && message.text.includes("任务未启动"))).toBe(false);
    } finally {
      store?.close();
      if (harness) await closeCoreHarness(harness, { remove: false });
      if (runtime) await runtime.handle.shutdown();
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);
}
const legacyCheckout = process.env.OCO_TEST_LEGACY_CHECKOUT;
test.skipIf(!legacyCheckout)("an actual legacy v1 runtime keeps its active worker and accepts new work", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-v1-startup-")));
  const cwd = join(root, "tmp");
  const oldCwd = join(root, "old");
  const command = join(root, "fixture-omp");
  const bot = { uid: "fixture-legacy-startup", name: "Fixture Bot" };
  const target = { channelId: "group-legacy", channelType: 2 as const };
  let harness: CoreHarness | undefined;
  let oldRuntime: EnsureAgentRuntimeResult | undefined;
  try {
    await mkdir(cwd, { recursive: true });
    await mkdir(oldCwd, { recursive: true });
    const fixture = join(import.meta.dir, "fixtures/omp-rpc-worker.ts");
    await writeFile(command, `#!/bin/sh\nif [ -f ${JSON.stringify(join(root, "fail"))} ] && [ "$PWD" != ${JSON.stringify(oldCwd)} ]; then printf 'private-legacy-stderr' >&2; sleep 0.2; exit 23; fi\nif [ "$PWD" = ${JSON.stringify(oldCwd)} ]; then exec ${JSON.stringify(process.execPath)} ${JSON.stringify(fixture)} --fixture-scenario interrupt "$@"; fi\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(fixture)} --fixture-scenario normal --fixture-context-receipt "$@"\n`);
    await chmod(command, 0o755);
    // The source path deliberately selects a separately checked-out historical runtime.
    const legacy = await import(join(legacyCheckout!, "src/runtime/agent-runtime.ts"));
    oldRuntime = await legacy.ensureAgentRuntime({ stateDir: join(root, "state"), botUid: bot.uid, ompCommand: command, startupTimeoutMs: 5_000 });
    const held = await oldRuntime!.client.createWorker({ taskId: "legacy-held", runId: "legacy-run", workerOptions: { cwd: oldCwd, sessionDir: oldCwd } });
    await held.dispatch({ dispatchId: "legacy-dispatch", kind: "prompt", text: "hold" });
    const oldPid = held.process!.pid;
    await oldRuntime!.client.disconnect();
    harness = await createCoreHarness({ root, bot, allowedGroupIds: [target.channelId], agentRuntime: { ompCommand: command, startupTimeoutMs: 5_000 } });
    const service = harness.service as unknown as { agentRuntime: EnsureAgentRuntimeResult; scheduler: { waitForTaskSettlement(id: string): Promise<void> } };
    let runtime = service.agentRuntime;
    expect(runtime.client.protocolVersion).toBe(1);
    expect(runtime.handle.runtimeId).toBe(oldRuntime!.handle.runtimeId);
    await submitAndWaitForIngressHandoff(harness, inbound({ ...target, messageId: "legacy-new", text: "new task", mentionedUserIds: [bot.uid] }));
    await harness.transport.waitForRemoteText(message => message.target.channelId === target.channelId && message.text.includes("fixture"));
    const store = await openChannelStore(harness.stateDir);
    try {
      const task = store.getLatestTask(bot.uid, target)!;
      await service.scheduler.waitForTaskSettlement(task.id);
      expect(store.getTask(task.id)?.state).toBe("completed");
      await writeFile(join(root, "fail"), "fail");
      await submitAndWaitForIngressHandoff(harness, inbound({ ...target, messageId: "legacy-fail", text: "next task", mentionedUserIds: [bot.uid] }));
      const failure = await harness.transport.waitForRemoteText(message => message.target.channelId === target.channelId && message.text.includes("结果未确认"));
      expect(failure.text).toContain("worker-start");
      expect(failure.text).not.toContain("private-legacy-stderr");
      const uncertain = store.getLatestTask(bot.uid, target)!;
      await service.scheduler.waitForTaskSettlement(uncertain.id);
      expect(store.getTask(uncertain.id)?.state).toBe("running");
      await closeCoreHarness(harness, { remove: false });
      harness = await createCoreHarness({ root, bot, allowedGroupIds: [target.channelId], agentRuntime: { ompCommand: command, startupTimeoutMs: 5_000 } });
      runtime = (harness.service as unknown as { agentRuntime: EnsureAgentRuntimeResult }).agentRuntime;
      expect(store.getTask(uncertain.id)?.state).toBe("running");
      const uncertainRuns = (await runtime.client.listWorkers()).filter(worker => worker.taskId === uncertain.id);
      expect(uncertainRuns).toHaveLength(1);
      expect(uncertainRuns[0]?.dispatches).toHaveLength(0);
    } finally { store.close(); }
    const snapshot = (await runtime.client.listWorkers()).find(worker => worker.taskId === "legacy-held");
    expect(snapshot?.stage).toBe("active");
    expect(snapshot?.process?.pid).toBe(oldPid);
    expect(process.kill(oldPid, 0)).toBe(true);
  } finally {
    if (harness) await closeCoreHarness(harness, { remove: false });
    if (oldRuntime) await oldRuntime.handle.shutdown();
    await rm(root, { recursive: true, force: true });
  }
}, 30_000);
