import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createWorkerFactory } from "../src/omp/index";
import type { OmpWorker } from "../src/contracts";
import {
  findMovedSession,
  loadValidatedSessionState,
  verifyFreshSession,
  verifyMovedSession,
  type ProjectSessionBinding,
  type ProjectSessionState,
} from "../src/project-session";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
} from "./fixtures/core-fakes";
type RefreshableWorker = OmpWorker & {
  refreshState(): Promise<ProjectSessionState>;
};

function hasRefreshState(worker: OmpWorker): worker is RefreshableWorker {
  return "refreshState" in worker && typeof worker.refreshState === "function";
}

/**
 * Binding regressions for the channel-level project/session commands.
 * These are intentionally written before the command wiring so the current
 * implementation demonstrates the missing behavior rather than passing by
 * construction.
 */
test("/new without a path reuses the currently bound directory and rotates the session", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-bindings", channelType: 1 as const };
  const project = await realpath(await mkdtemp(join(harness.root, "project-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-new-explicit", ...target, text: `/new "${project}"` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-new-reuse", ...target, text: "/new" }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(project);
    expect(after.sessionId).not.toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/reset without a path creates a fresh session in the existing directory", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-reset", channelType: 1 as const };
  const project = await realpath(await mkdtemp(join(harness.root, "project-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-reset-explicit", ...target, text: `/new ${project}` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-reset-reuse", ...target, text: "/reset" }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(project);
    expect(after.sessionId).not.toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/move keeps the session identity while changing the bound directory", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-move", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const destination = await realpath(await mkdtemp(join(harness.root, "destination-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-move-source", ...target, text: `/new ${source}` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-move-destination", ...target, text: `/move ${destination}` }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(destination);
    expect(after.sessionId).toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("different chats retain independent project/session bindings", async () => {
  const harness = await createCoreHarness();
  const first = { channelId: "dm-project-first", channelType: 1 as const };
  const second = { channelId: "dm-project-second", channelType: 1 as const };
  const firstDirectory = await realpath(await mkdtemp(join(harness.root, "first-")));
  const secondDirectory = await realpath(await mkdtemp(join(harness.root, "second-")));
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-isolated-first", ...first, text: `/new ${firstDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-isolated-second", ...second, text: `/new ${secondDirectory}` }),
    );

    const firstStatus = await harness.service.status(first);
    const secondStatus = await harness.service.status(second);
    expect(firstStatus.cwd).toBe(firstDirectory);
    expect(secondStatus.cwd).toBe(secondDirectory);
    expect(firstStatus.sessionId).not.toBe(secondStatus.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});


test("a rejected move leaves the prior binding untouched", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-project-move-failure", channelType: 1 as const };
  const source = await realpath(await mkdtemp(join(harness.root, "source-")));
  const missing = join(harness.root, "does-not-exist");
  try {
    await harness.service.handleInbound(
      inbound({ messageId: "bindings-failure-source", ...target, text: `/new ${source}` }),
    );
    const before = await harness.service.status(target);

    await harness.service.handleInbound(
      inbound({ messageId: "bindings-failure-missing", ...target, text: `/move ${missing}` }),
    );
    const after = await harness.service.status(target);

    expect(after.cwd).toBe(before.cwd);
    expect(after.sessionId).toBe(before.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("OMP refreshState returns the authoritative session file, identity, and header cwd", async () => {
  const tempDir = await realpath(await mkdtemp(join(tmpdir(), "omp-project-state-")));
  const logPath = join(tempDir, "fixture.log");
  const fixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");
  const factory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [
      fixturePath,
      "--fixture-scenario",
      "normal",
      "--fixture-log",
      logPath,
    ],
    startupTimeoutMs: 2_000,
    cancelTimeoutMs: 800,
  });
  let worker: OmpWorker | undefined;
  try {
    worker = await factory.create({
      cwd: tempDir,
      sessionDir: tempDir,
      onSpawn: () => {},
    });
    if (!hasRefreshState(worker)) throw new Error("worker does not expose refreshState");
    const state = await worker.refreshState();
    expect(state.sessionId).toBe(worker.sessionId);
    expect(state.sessionFile).toBe(worker.sessionFile);
    expect(state.cwd).toBe(tempDir);
  } finally {
    await worker?.stop().catch(() => false);
    await rm(tempDir, { recursive: true, force: true });
  }
});
test("external move discovery follows only the exact session identity and breadcrumb", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-scan-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const movedDirectory = join(managedRoot, "moved");
  const decoyDirectory = join(managedRoot, "decoy");
  const oldSessionFile = join(source, "session.jsonl");
  const movedSessionFile = join(movedDirectory, "session.jsonl");
  const decoySessionFile = join(decoyDirectory, "session.jsonl");
  try {
    await mkdir(movedDirectory, { recursive: true });
    await mkdir(decoyDirectory, { recursive: true });
    await writeFile(
      movedSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-1",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await writeFile(
      decoySessionFile,
      `${JSON.stringify({ type: "session", id: "session-1", cwd: destination })}\n`,
      "utf8",
    );
    const binding: ProjectSessionBinding = {
      sessionId: "session-1",
      cwd: source,
      sessionFile: oldSessionFile,
    };
    const found = await findMovedSession(binding, { roots: [managedRoot] });
    expect(found).toEqual({
      sessionId: "session-1",
      cwd: destination,
      sessionFile: movedSessionFile,
    });
    expect(await findMovedSession(binding, { roots: [managedRoot, movedDirectory] })).toEqual({
      sessionId: "session-1",
      cwd: destination,
      sessionFile: movedSessionFile,
    });

    const secondMovedDirectory = join(root, "managed", "moved-again");
    await mkdir(secondMovedDirectory, { recursive: true });
    await writeFile(
      join(secondMovedDirectory, "session.jsonl"),
      `${JSON.stringify({
        type: "session",
        id: "session-1",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    expect(await findMovedSession(binding, { roots: [managedRoot] })).toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("native move and fresh-session verification require matching persisted headers", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-verify-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const sessionFile = join(root, "session.jsonl");
  const previous: ProjectSessionBinding = {
    sessionId: "session-old",
    cwd: source,
    sessionFile: join(source, "old.jsonl"),
  };
  try {
    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-old", cwd: destination })}\n`,
      "utf8",
    );
    const movedState: ProjectSessionState = {
      sessionId: previous.sessionId,
      sessionFile,
      cwd: destination,
    };
    await expect(verifyMovedSession(previous, movedState, destination)).resolves.toEqual({
      sessionId: "session-old",
      cwd: destination,
      sessionFile,
    });

    const freshState: ProjectSessionState = {
      sessionId: "session-new",
      sessionFile,
      cwd: destination,
    };
    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-new", cwd: destination })}\n`,
      "utf8",
    );
    await expect(verifyFreshSession(previous, freshState, destination)).resolves.toEqual({
      sessionId: "session-new",
      cwd: destination,
      sessionFile,
    });

    await writeFile(
      sessionFile,
      `${JSON.stringify({ type: "session", id: "session-other", cwd: destination })}\n`,
      "utf8",
    );
    await expect(verifyFreshSession(previous, freshState, destination)).rejects.toThrow("session header");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("validated session loading accepts a title slot and stops at the first session record", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-loader-")));
  const cwd = await realpath(await mkdtemp(join(root, "cwd-")));
  const sessionFile = join(root, "session.jsonl");
  try {
    await writeFile(
      sessionFile,
      [
        JSON.stringify({ type: "title", v: 1, title: "session title", updatedAt: new Date().toISOString(), pad: "" }),
        JSON.stringify({ type: "session", id: "session-loader", cwd, previousSessionFiles: ["/tmp/old.jsonl"] }),
        `${"{not-json-after-header}\\n".repeat(1000)}`,
      ].join("\n"),
      "utf8",
    );
    await expect(loadValidatedSessionState(sessionFile, "session-loader")).resolves.toEqual({
      state: {
        sessionId: "session-loader",
        sessionFile,
        cwd,
      },
      previousSessionFiles: ["/tmp/old.jsonl"],
    });
    await expect(loadValidatedSessionState(sessionFile, "wrong-id")).rejects.toThrow("identity");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounded discovery filters basenames before applying the candidate file limit", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-basename-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const oldSessionFile = join(source, "session.jsonl");
  const candidateSessionFile = join(root, "session.jsonl");
  try {
    await writeFile(join(root, "unrelated.jsonl"), `${"{not-json}\\n".repeat(2000)}`, "utf8");
    await writeFile(
      candidateSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-basename",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-basename", cwd: source, sessionFile: oldSessionFile },
        { roots: [root], maxFiles: 1 },
      ),
    ).resolves.toEqual({
      sessionId: "session-basename",
      cwd: destination,
      sessionFile: candidateSessionFile,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("bounded discovery does not traverse artifact subdirectories", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-artifact-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const artifactDirectory = join(managedRoot, "session");
  const artifactSessionFile = join(artifactDirectory, "session.jsonl");
  const directDecoySessionFile = join(managedRoot, "session.jsonl");
  try {
    await mkdir(artifactDirectory, { recursive: true });
    await writeFile(
      directDecoySessionFile,
      `${JSON.stringify({ type: "session", id: "different-session", cwd: destination })}\n`,
      "utf8",
    );
    await writeFile(
      artifactSessionFile,
      `${JSON.stringify({
        type: "session",
        id: "session-artifact",
        cwd: destination,
        previousSessionFiles: [join(source, "session.jsonl")],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-artifact", cwd: source, sessionFile: join(source, "session.jsonl") },
        { roots: [managedRoot] },
      ),
    ).resolves.toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounded discovery fails closed when the directory candidate limit is exceeded", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-directory-limit-")));
  const source = await realpath(await mkdtemp(join(root, "source-")));
  const destination = await realpath(await mkdtemp(join(root, "destination-")));
  const managedRoot = join(root, "managed");
  const sessionDirectory = join(managedRoot, "project");
  const extraDirectory = join(managedRoot, "other-project");
  const oldSessionFile = join(source, "session.jsonl");
  try {
    await mkdir(sessionDirectory, { recursive: true });
    await mkdir(extraDirectory, { recursive: true });
    await writeFile(
      join(sessionDirectory, "session.jsonl"),
      `${JSON.stringify({
        type: "session",
        id: "session-directory-limit",
        cwd: destination,
        previousSessionFiles: [oldSessionFile],
      })}\n`,
      "utf8",
    );
    await expect(
      findMovedSession(
        { sessionId: "session-directory-limit", cwd: source, sessionFile: oldSessionFile },
        { roots: [managedRoot], maxDirectories: 1 },
      ),
    ).resolves.toBeUndefined();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("OMP factory exposes profile-aware session roots without importing OMP runtime helpers", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-")));
  const xdgData = join(root, "xdg-data");
  const customAgent = join(root, "custom-agent");
  const environmentKeys = ["OMP_PROFILE", "PI_PROFILE", "PI_CONFIG_DIR", "PI_CODING_AGENT_DIR", "XDG_DATA_HOME"] as const;
  const previousEnvironment = new Map<string, string | undefined>(
    environmentKeys.map(key => [key, process.env[key]]),
  );
  try {
    await mkdir(join(xdgData, "omp", "profiles", "work"), { recursive: true });
    await mkdir(join(xdgData, "omp", "profiles", "cli"), { recursive: true });
    process.env.OMP_PROFILE = "work";
    process.env.PI_PROFILE = "ignored";
    process.env.PI_CONFIG_DIR = ".omp-test";
    process.env.PI_CODING_AGENT_DIR = customAgent;
    process.env.XDG_DATA_HOME = xdgData;
    const cliFactory = createWorkerFactory({
      ompCommand: process.execPath,
      extraArgs: ["--profile", "cli"],
    });
    expect(cliFactory.sessionRoots).toEqual([
      join(xdgData, "omp", "profiles", "cli", "sessions"),
    ]);

    process.env.OMP_PROFILE = "";
    const defaultFactory = createWorkerFactory({ ompCommand: process.execPath });
    expect(defaultFactory.sessionRoots).toEqual([join(customAgent, "sessions")]);
  } finally {
    for (const key of environmentKeys) {
      const value = previousEnvironment.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
});
