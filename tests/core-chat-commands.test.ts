import { existsSync } from "node:fs";
import { mkdir, realpath, rm } from "node:fs/promises";
import { join } from "node:path";
import { expect, test } from "bun:test";
import type { ChatTarget } from "../src/contracts.ts";
import type { ChannelStore } from "../src/store/index.ts";
import {
  createCommandCatalogHarness,
  type TestOmpAvailableCommand,
} from "./command-catalog-test-seam";

function seedBinding(
  store: ChannelStore,
  botUid: string,
  target: ChatTarget,
  cwd: string,
  sessionId = `session-${target.channelId}`,
): void {
  store.ensureBinding({ botUid, ...target, sessionId, sessionFile: null, cwd, cwdPinned: true });
}

test("command suggestions use the exact enabled chat cwd and never invoke a task prompt", async () => {
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["project-chat-a", "project-chat-b"],
    availableCommands: async ({ cwd }) => [{ name: cwd.endsWith("project-a") ? "project-a-only" : "project-b-only", source: "file" }],
  });
  const projectA = join(harness.root, "project-a");
  const projectB = join(harness.root, "project-b");
  await mkdir(projectA, { recursive: true });
  await mkdir(projectB, { recursive: true });
  const targetA = { channelType: 2, channelId: "project-chat-a" } as const;
  const targetB = { channelType: 2, channelId: "project-chat-b" } as const;
  seedBinding(harness.store, harness.bot.uid, targetA, projectA);
  seedBinding(harness.store, harness.bot.uid, targetB, projectB);

  try {
    const beforeA = harness.store.getBinding(harness.bot.uid, targetA);
    const beforeB = harness.store.getBinding(harness.bot.uid, targetB);
    const signal = new AbortController().signal;
    const snapshotA = await harness.service.chatCommandsSnapshot(targetA, signal);
    const snapshotB = await harness.service.chatCommandsSnapshot(targetB, signal);

    expect(snapshotA?.commands).toContainEqual({ name: "project-a-only", source: "omp" });
    expect(snapshotA?.commands.some(command => command.name === "project-b-only")).toBe(false);
    expect(snapshotB?.commands).toContainEqual({ name: "project-b-only", source: "omp" });
    expect(snapshotB?.commands.some(command => command.name === "project-a-only")).toBe(false);
    expect(snapshotA?.omp).toBe("available");
    expect(snapshotB?.omp).toBe("available");
    expect(harness.store.getBinding(harness.bot.uid, targetA)).toEqual(beforeA);
    expect(harness.store.getBinding(harness.bot.uid, targetB)).toEqual(beforeB);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.workers.workers).toHaveLength(2);
    expect(harness.workers.workers.every(worker => worker.stopped)).toBe(true);
    expect(harness.store.listWorkerLedger(harness.bot.uid).every(ledger => ledger.state === "stopped")).toBe(true);
  } finally {
    await harness.close();
  }
});

test("unavailable OMP preserves channel suggestions and retains an uncertain probe directory", async () => {
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async (): Promise<readonly TestOmpAvailableCommand[]> => {
      throw new Error("OMP command catalog RPC failed");
    },
    stopConfirmed: () => false,
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  let probeDirectory: string | undefined;
  try {
    const snapshot = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    probeDirectory = harness.workers.creations[0]?.sessionDir;

    expect(snapshot?.omp).toBe("unavailable");
    expect(snapshot?.commands.some(command => command.source === "channel")).toBe(true);
    expect(snapshot?.commands.some(command => command.name === "project-command")).toBe(false);
    expect(harness.workers.workers[0]?.stopped).toBe(false);
    expect(probeDirectory && existsSync(probeDirectory)).toBe(true);
    expect(harness.store.listWorkerLedger(harness.bot.uid).some(ledger => ledger.state !== "stopped")).toBe(true);
  } finally {
    await harness.close();
    if (probeDirectory) await rm(probeDirectory, { recursive: true, force: true });
  }
});

test("a binding change during catalog discovery cannot return the old project's commands", async () => {
  const { promise: started, resolve: markStarted } = Promise.withResolvers<void>();
  const { promise: result, resolve: release } = Promise.withResolvers<readonly TestOmpAvailableCommand[]>();
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async () => {
      markStarted();
      return await result;
    },
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  const oldCwd = join(harness.root, "old-project");
  const newCwd = join(harness.root, "new-project");
  await mkdir(oldCwd, { recursive: true });
  await mkdir(newCwd, { recursive: true });
  seedBinding(harness.store, harness.bot.uid, target, oldCwd, "old-session");

  try {
    const request = harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    await started;
    seedBinding(harness.store, harness.bot.uid, target, newCwd, "new-session");
    release([{ name: "old-project-only", source: "file" }]);
    const snapshot = await request;

    expect(snapshot?.omp).toBe("unavailable");
    expect(snapshot?.commands.some(command => command.name === "old-project-only")).toBe(false);
    expect(harness.workers.workers[0]?.stopped).toBe(true);
  } finally {
    release([]);
    await harness.close();
  }
});

test("request cancellation settles the probe worker before the command snapshot rejects", async () => {
  const { promise: started, resolve: markStarted } = Promise.withResolvers<void>();
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async (_options, signal) => {
      markStarted();
      const { promise, reject } = Promise.withResolvers<readonly TestOmpAvailableCommand[]>();
      const fail = (): void => reject(signal?.reason ?? new Error("probe cancelled"));
      signal?.addEventListener("abort", fail, { once: true });
      return await promise;
    },
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  try {
    const controller = new AbortController();
    const request = harness.service.chatCommandsSnapshot(target, controller.signal);
    await started;
    controller.abort(new Error("HTTP request cancelled"));

    await expect(request).rejects.toThrow("HTTP request cancelled");
    expect(harness.workers.workers[0]?.stopped).toBe(true);
    expect(harness.store.listWorkerLedger(harness.bot.uid).every(ledger => ledger.state === "stopped")).toBe(true);
  } finally {
    await harness.close();
  }
});

test("only an owner DM identity or an enabled group/thread can request a command catalog", async () => {
  const harness = await createCommandCatalogHarness({ allowedGroupIds: ["group-enabled"] });
  const canonicalRoot = await realpath(harness.root);
  const ownerTarget = { channelType: 1, channelId: "owner-1" } as const;
  const unrelatedDm = { channelType: 1, channelId: "other-user" } as const;
  const unlistedGroup = { channelType: 2, channelId: "group-unlisted" } as const;
  const enabledThread = { channelType: 5, channelId: "group-enabled____thread-3" } as const;
  seedBinding(harness.store, harness.bot.uid, ownerTarget, harness.root);
  seedBinding(harness.store, harness.bot.uid, enabledThread, harness.root);

  try {
    const signal = new AbortController().signal;
    expect(await harness.service.chatCommandsSnapshot(ownerTarget, signal)).not.toBeNull();
    expect(await harness.service.chatCommandsSnapshot(unrelatedDm, signal)).toBeNull();
    expect(await harness.service.chatCommandsSnapshot(unlistedGroup, signal)).toBeNull();
    expect(await harness.service.chatCommandsSnapshot(enabledThread, signal)).not.toBeNull();
    expect(harness.workers.creations.map(creation => creation.cwd)).toEqual([canonicalRoot, canonicalRoot]);
  } finally {
    await harness.close();
  }
});

test("OMP suggestions reserve hidden channel routes in an owner DM", async () => {
  const harness = await createCommandCatalogHarness({
    availableCommands: async () => [
      { name: "thread", source: "skill" },
      { name: "fork", source: "skill" },
      { name: "bind", source: "file" },
      { name: "project-command", source: "file", aliases: ["thread", "fork", "bind"] },
    ],
  });
  const target = { channelType: 1, channelId: "owner-1" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);
  try {
    const snapshot = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    expect(snapshot?.commands.some(command => ["thread", "fork", "bind"].includes(command.name))).toBe(false);
    expect(snapshot?.commands.find(command => command.name === "project-command")).toMatchObject({ source: "omp" });
    expect(snapshot?.commands.find(command => command.name === "project-command")?.aliases).toBeUndefined();
  } finally {
    await harness.close();
  }
});

test("channel commands win over OMP aliases while project and skill suggestions remain visible", async () => {
  const commands: TestOmpAvailableCommand[] = [
    { name: "new", source: "builtin" },
    { name: "reset", source: "skill" },
    { name: "custom", source: "file" },
    { name: "retry", source: "builtin" },
    { name: "file-command", source: "file", description: "Project command" },
    { name: "skill-command", source: "skill", hint: "<topic>" },
    { name: "interactive-extension", source: "extension", description: "Requires a UI" },
    {
      name: "memory",
      source: "skill",
      subcommands: [{ name: "memory mm list", description: "List memories", usage: "memory mm list [limit]" }],
    },
    {
      name: "advisor",
      source: "builtin",
      subcommands: [
        { name: "configure", description: "Open configuration editor (TUI)", usage: "advisor configure" },
        { name: "status", description: "Show status", usage: "advisor status" },
      ],
    },
  ];
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    commandHandlers: [{ name: "custom", intercept: false, handle: async () => undefined }],
    availableCommands: async () => commands,
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  try {
    const snapshot = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    const commandNames = snapshot?.commands.map(command => command.name) ?? [];
    expect(snapshot?.commands.find(command => command.name === "new")).toMatchObject({
      source: "channel",
      aliases: ["reset"],
    });
    expect(commandNames.filter(name => name === "new")).toHaveLength(1);
    expect(commandNames.filter(name => name === "custom")).toHaveLength(1);
    expect(commandNames).toContain("tan");
    expect(commandNames.filter(name => name === "reset")).toHaveLength(0);
    expect(snapshot?.commands.find(command => command.name === "retry")?.source).toBe("omp");
    expect(snapshot?.commands.find(command => command.name === "file-command")?.source).toBe("omp");
    expect(snapshot?.commands.find(command => command.name === "skill-command")?.source).toBe("omp");
    expect(commandNames).not.toContain("interactive-extension");
    expect(snapshot?.commands.find(command => command.name === "custom")?.source).toBe("channel");
    const subcommands = (name: string) => snapshot?.commands.find(command => command.name === name)?.subcommands ?? [];
    expect(subcommands("goal").map(command => command.name)).toEqual(["show", "set", "pause", "resume", "drop", "budget"]);
    expect(subcommands("goal").find(command => command.name === "set")?.hint).toBe("[--budget N] <目标>");
    expect(subcommands("mode").map(command => command.name)).toEqual(["steer", "queue"]);
    expect(subcommands("commit").map(command => command.name)).toEqual(["stop"]);
    expect(subcommands("review").map(command => command.name)).toEqual(["stop"]);
    expect(subcommands("watch").map(command => command.name)).toEqual(["list", "stop"]);
    expect(subcommands("watch").find(command => command.name === "stop")?.hint).toBe("<id|all>");
    expect(subcommands("bind")).toMatchObject([{
      name: "loop",
      hint: "<Loop workspace 名称、slug、ID 或同源 Fleet issue/project 链接>",
    }]);
    expect(subcommands("thread")).toMatchObject([{ name: "model", hint: "[选择器|clear]" }]);
    expect(snapshot?.commands.find(command => command.name === "advisor")?.subcommands).toEqual([
      { name: "status", description: "Show status", hint: "advisor status" },
    ]);
    expect(snapshot?.commands.find(command => command.name === "memory")?.subcommands).toEqual([
      { name: "memory mm list", description: "List memories", hint: "memory mm list [limit]" },
    ]);
  } finally {
    await harness.close();
  }
});

test("125-command OMP catalogs fit the HTTP limit without dropping commands or metadata", async () => {
  const commands: TestOmpAvailableCommand[] = Array.from({ length: 125 }, (_, index) => ({
    name: `project-command-${String(index).padStart(3, "0")}`,
    source: index % 2 === 0 ? "file" : "skill",
    description: `Description ${"d".repeat(400)}`,
    hint: `<${"argument".repeat(12)}>`,
    ...(index === 0
      ? {
          aliases: ["project-command-alias"],
          subcommands: [{
            name: "project command subcommand",
            description: "Nested command",
            usage: "project command subcommand <value>",
          }],
        }
      : {}),
  }));
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async () => commands,
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  try {
    const snapshot = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    const ompSuggestions = snapshot?.commands.filter(command => command.source === "omp") ?? [];
    const responseBytes = new TextEncoder().encode(JSON.stringify(snapshot)).byteLength;
    expect(ompSuggestions).toHaveLength(125);
    expect(ompSuggestions[0]?.name).toBe("project-command-000");
    expect(ompSuggestions.at(-1)?.name).toBe("project-command-124");
    expect(ompSuggestions.every(command =>
      command.description === `Description ${"d".repeat(400)}` && command.hint === `<${"argument".repeat(12)}>`
    )).toBe(true);
    expect(ompSuggestions[0]).toMatchObject({
      aliases: ["project-command-alias"],
      subcommands: [{
        name: "project command subcommand",
        description: "Nested command",
        hint: "project command subcommand <value>",
      }],
    });
    expect(responseBytes).toBeLessThanOrEqual(512 * 1024);
  } finally {
    await harness.close();
  }
});

test("oversized OMP catalogs are unavailable instead of being partially advertised", async () => {
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async () => [{
      name: "oversized-skill",
      source: "skill",
      description: "d".repeat(512 * 1024),
    }],
  });
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  try {
    const snapshot = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    expect(snapshot?.omp).toBe("unavailable");
    expect(snapshot?.commands.some(command => command.source === "omp")).toBe(false);
    expect(snapshot?.commands.some(command => command.name === "status" && command.source === "channel")).toBe(true);
  } finally {
    await harness.close();
  }
});


test("service shutdown aborts and joins its probe before store closure", async () => {
  const { promise: started, resolve: markStarted } = Promise.withResolvers<void>();
  const { promise: aborted, resolve: markAborted } = Promise.withResolvers<void>();
  const { promise: stopStarted, resolve: markStopStarted } = Promise.withResolvers<void>();
  const { promise: releaseStop, resolve: allowStop } = Promise.withResolvers<void>();
  const harness = await createCommandCatalogHarness({
    allowedGroupIds: ["group-a"],
    availableCommands: async (_options, signal) => {
      markStarted();
      const { promise, reject } = Promise.withResolvers<readonly TestOmpAvailableCommand[]>();
      signal?.addEventListener("abort", () => {
        markAborted();
        reject(signal.reason ?? new Error("service stopped"));
      }, { once: true });
      return await promise;
    },
  });
  harness.workers.stopHandler = async () => {
    markStopStarted();
    await releaseStop;
  };
  const target = { channelType: 2, channelId: "group-a" } as const;
  seedBinding(harness.store, harness.bot.uid, target, harness.root);

  let shutdownCompleted = false;
  try {
    const request = harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    await started;
    const shutdown = harness.stop().then(() => {
      shutdownCompleted = true;
    });
    await aborted;
    await stopStarted;
    expect(shutdownCompleted).toBe(false);

    allowStop();
    await expect(request).rejects.toThrow();
    await shutdown;
    expect(harness.workers.workers[0]?.stopped).toBe(true);
    expect(harness.store.listWorkerLedger(harness.bot.uid).every(ledger => ledger.state === "stopped")).toBe(true);
  } finally {
    allowStop();
    await harness.close();
  }
});
