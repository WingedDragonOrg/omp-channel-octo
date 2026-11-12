import { expect, test, vi } from "bun:test";
import { randomBytes } from "node:crypto";
import { chmod, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

interface ServiceSnapshot {
  version: 1;
  sampledAt: string;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
}

interface ChatTarget {
  channelType: number;
  channelId: string;
}

interface ChatStatusSnapshot extends ChatTarget {
  version: 1;
  sampledAt: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
}

interface StatusServerOptions {
  listenAddress: string;
  port: number;
  /** Direct bearer token; wins over `tokenFile` when both are supplied. */
  token?: string;
  tokenFile?: string;
  statusSnapshot: () => ServiceSnapshot;
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot;
}

interface StatusServer {
  port: number;
  stop: () => void | Promise<void>;
}

type StartStatusServer = (options: StatusServerOptions) => Promise<StatusServer>;

const STATUS_SERVER_MODULE = "../src/status/server.ts";
const TOKEN = randomBytes(32).toString("hex");
const SERVICE_SNAPSHOT: ServiceSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T08:30:00.000Z",
  pid: 4321,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 2,
  queuedTasks: 3,
};

async function loadStartStatusServer(): Promise<StartStatusServer> {
  let statusModule: { startStatusServer?: unknown } | undefined;
  let importFailure: unknown;
  try {
    statusModule = await import(STATUS_SERVER_MODULE) as { startStatusServer?: unknown };
  } catch (error) {
    importFailure = error;
  }

  // Keep a missing entrypoint as a focused test assertion rather than a module-load failure.
  expect(statusModule).toBeDefined();
  if (!statusModule) {
    const detail = importFailure instanceof Error ? importFailure.message : String(importFailure);
    throw new Error(`Expected ${STATUS_SERVER_MODULE} to export startStatusServer; dynamic import failed: ${detail}`);
  }

  expect(typeof statusModule.startStatusServer).toBe("function");
  return statusModule.startStatusServer as StartStatusServer;
}

async function createTokenFixture(mode = 0o600): Promise<{ tokenFile: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), "oco-status-server-"));
  try {
    await chmod(root, 0o700);
    const tokenFile = join(root, "status-token");
    await writeFile(tokenFile, TOKEN, { mode });
    await chmod(tokenFile, mode);
    return { tokenFile, cleanup: () => rm(root, { recursive: true, force: true }) };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function createTokenFifoFixture(): Promise<{ tokenFile: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), "oco-status-server-fifo-"));
  try {
    await chmod(root, 0o700);
    const tokenFile = join(root, "status-token");
    if (Bun.spawnSync(["mkfifo", tokenFile]).exitCode !== 0) {
      throw new Error("Unable to create FIFO token fixture");
    }
    await chmod(tokenFile, 0o600);
    return { tokenFile, cleanup: () => rm(root, { recursive: true, force: true }) };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

const FIFO_SUBPROCESS_TIMEOUT_MS = 3_000;
const FIFO_REJECTION_MAX_MS = 1_000;

interface IsolatedScriptResult {
  timedOut: boolean;
  exitCode: number;
  stdout: string;
  stderr: string;
}

async function runIsolatedScript(script: string): Promise<IsolatedScriptResult> {
  const child = Bun.spawn([process.execPath, "-e", script], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdoutPromise = new Response(child.stdout).text();
  const stderrPromise = new Response(child.stderr).text();
  const { promise: timeoutPromise, resolve: resolveTimeout } = Promise.withResolvers<{ timedOut: true }>();
  // A real deadline is needed because a blocked FIFO open cannot be driven by fake timers.
  const timer = setTimeout(() => resolveTimeout({ timedOut: true }), FIFO_SUBPROCESS_TIMEOUT_MS);
  const outcome = await Promise.race([
    child.exited.then(exitCode => ({ timedOut: false as const, exitCode })),
    timeoutPromise,
  ]);
  clearTimeout(timer);

  let exitCode: number;
  if (outcome.timedOut) {
    if (child.exitCode === null) child.kill("SIGKILL");
    exitCode = await child.exited;
  } else {
    exitCode = outcome.exitCode;
  }
  const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
  return { timedOut: outcome.timedOut, exitCode, stdout, stderr };
}

function statusServerFifoReportScript(tokenFile: string): string {
  const serverModule = new URL(STATUS_SERVER_MODULE, import.meta.url).href;
  return `
    void (async () => {
      // Import the checked-out module in an isolated process so a blocking FIFO open is killable.
      const { startStatusServer } = await import(${JSON.stringify(serverModule)});
      const originalServe = Bun.serve;
      const createdServers = [];
      let listenerStarts = 0;
      Bun.serve = (...args) => {
        listenerStarts += 1;
        const server = Reflect.apply(originalServe, Bun, args);
        createdServers.push(server);
        return server;
      };
      const startedAt = performance.now();
      let outcome;
      let error = "";
      try {
        await startStatusServer({
          listenAddress: "127.0.0.1",
          port: 0,
          tokenFile: ${JSON.stringify(tokenFile)},
          statusSnapshot: () => ({
            version: 1,
            sampledAt: "2026-11-11T08:30:00.000Z",
            pid: 4321,
            botUid: "bot-17",
            ownerUid: "owner-17",
            runningSessions: 0,
            queuedTasks: 0,
          }),
          chatStatusSnapshot: target => ({
            ...target,
            version: 1,
            sampledAt: "2026-11-11T08:31:00.000Z",
            state: "none",
            model: null,
            cwd: null,
            queuedTasks: 0,
            pendingQuestions: 0,
          }),
        });
        outcome = "resolved";
      } catch (caught) {
        const parts = [];
        let current = caught;
        while (current instanceof Error) {
          parts.push(current.message);
          current = "cause" in current ? current.cause : undefined;
        }
        if (current !== undefined) parts.push(String(current));
        outcome = "rejected";
        error = parts.join(" ");
      } finally {
        Bun.serve = originalServe;
        await Promise.all(createdServers.map(server => server.stop(true)));
      }
      process.stdout.write(JSON.stringify({
        outcome,
        error,
        listenerStarts,
        elapsedMs: performance.now() - startedAt,
      }) + "\\n");
    })().catch(error => {
      console.error(error);
      process.exitCode = 1;
    });
  `;
}

function serverOptions(
  credentials: { token?: string; tokenFile?: string },
  statusSnapshot: () => ServiceSnapshot = () => SERVICE_SNAPSHOT,
): StatusServerOptions {
  return {
    // Tests bind only to loopback; production listen addresses are supplied by configuration.
    listenAddress: "127.0.0.1",
    port: 0,
    ...credentials,
    statusSnapshot,
    chatStatusSnapshot: target => ({
      version: 1,
      sampledAt: "2026-11-11T08:31:00.000Z",
      channelType: target.channelType,
      channelId: target.channelId,
      state: "none",
      model: null,
      cwd: null,
      queuedTasks: 0,
      pendingQuestions: 0,
    }),
  };
}

function expectNoToken(value: string, surface: string): void {
  if (value.includes(TOKEN)) throw new Error(`${surface} disclosed the bearer token`);
}

function errorDetails(error: unknown): string {
  const parts: string[] = [];
  let current = error;
  while (current instanceof Error) {
    parts.push(current.message);
    if ("code" in current && current.code !== undefined) parts.push(String(current.code));
    current = "cause" in current ? current.cause : undefined;
  }
  if (current !== undefined) parts.push(String(current));
  return parts.join(" ");
}

test("status server binds localhost and authenticates real HTTP requests with its private token file", async () => {
  const fixture = await createTokenFixture();
  let server: StatusServer | undefined;
  try {
    expect((await stat(fixture.tokenFile)).mode & 0o777).toBe(0o600);
    const startStatusServer = await loadStartStatusServer();
    server = await startStatusServer(serverOptions({ tokenFile: fixture.tokenFile }));
    expect(server.port).toBeGreaterThan(0);

    const url = `http://127.0.0.1:${server.port}/v1/status`;
    const authorized = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(authorized.status).toBe(200);
    const authorizedBody = await authorized.text();
    expectNoToken(authorizedBody, "Authorized response");
    expect(JSON.parse(authorizedBody)).toEqual(SERVICE_SNAPSHOT);

    for (const authorization of [undefined, "Bearer invalid-status-token"]) {
      const response = await fetch(url, {
        headers: authorization === undefined ? {} : { authorization },
      });
      expect(response.status).toBe(401);
      const body = await response.text();
      expectNoToken(body, "Unauthorized response");
      expect(JSON.parse(body)).toEqual({ error: { code: "unauthorized" } });
    }
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }
});

test("a direct token takes precedence over the configured token file", async () => {
  const fixture = await createTokenFixture();
  const fileToken = randomBytes(32).toString("hex");
  await writeFile(fixture.tokenFile, fileToken, { mode: 0o600 });
  let server: StatusServer | undefined;
  try {
    const startStatusServer = await loadStartStatusServer();
    server = await startStatusServer(serverOptions({ token: TOKEN, tokenFile: fixture.tokenFile }));
    const url = `http://127.0.0.1:${server.port}/v1/status`;

    const direct = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(direct.status).toBe(200);
    const directBody = await direct.text();
    expectNoToken(directBody, "Direct-token response");
    if (directBody.includes(fileToken)) throw new Error("Direct-token response disclosed the token file content");
    expect(JSON.parse(directBody)).toEqual(SERVICE_SNAPSHOT);

    const fromFile = await fetch(url, { headers: { authorization: `Bearer ${fileToken}` } });
    expect(fromFile.status).toBe(401);
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }
});

test("status server refuses to start without a direct token or token file", async () => {
  const startStatusServer = await loadStartStatusServer();
  let server: StatusServer | undefined;
  let startupError: unknown;
  try {
    server = await startStatusServer(serverOptions({}));
  } catch (error) {
    startupError = error;
  } finally {
    await server?.stop();
  }

  expect(server).toBeUndefined();
  expect(startupError).toBeDefined();
  expect(errorDetails(startupError)).toMatch(/token|凭据|credential/iu);
});

test("replacing a private token revokes the old token without restarting the server", async () => {
  const fixture = await createTokenFixture();
  let server: StatusServer | undefined;
  try {
    const startStatusServer = await loadStartStatusServer();
    server = await startStatusServer(serverOptions({ tokenFile: fixture.tokenFile }));
    const url = `http://127.0.0.1:${server.port}/v1/status`;
    const replacement = randomBytes(32).toString("hex");
    await writeFile(fixture.tokenFile, replacement, { mode: 0o600 });
    const old = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
    const current = await fetch(url, { headers: { authorization: `Bearer ${replacement}` } });
    expect(old.status).toBe(401);
    expect(current.status).toBe(200);
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }
});

test("stopping the status server closes its listener so a subsequent connection is refused", async () => {
  const fixture = await createTokenFixture();
  let server: StatusServer | undefined;
  try {
    const startStatusServer = await loadStartStatusServer();
    server = await startStatusServer(serverOptions({ tokenFile: fixture.tokenFile }));
    const url = `http://127.0.0.1:${server.port}/v1/status`;
    const response = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(response.status).toBe(200);
    await response.body?.cancel();

    const stoppedServer = server;
    server = undefined;
    await stoppedServer.stop();

    let connectionFailure: unknown;
    try {
      await fetch(url, { signal: AbortSignal.timeout(3_000) });
    } catch (error) {
      connectionFailure = error;
    }
    expect(connectionFailure).toBeDefined();
    expect(errorDetails(connectionFailure)).toContain("ECONNREFUSED");
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }
});

test("status server refuses a token file with group or other access and keeps the startup error secret-free", async () => {
  const fixture = await createTokenFixture(0o644);
  let server: StatusServer | undefined;
  let startupError: unknown;
  try {
    expect((await stat(fixture.tokenFile)).mode & 0o777).toBe(0o644);
    const startStatusServer = await loadStartStatusServer();
    try {
      server = await startStatusServer(serverOptions({ tokenFile: fixture.tokenFile }));
    } catch (error) {
      startupError = error;
    }
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }

  expect(startupError).toBeDefined();
  expectNoToken(errorDetails(startupError), "Unsafe token-file startup error");
});

test.skipIf(process.platform === "win32")("status server promptly rejects a FIFO token file before starting its listener", async () => {
  const fixture = await createTokenFifoFixture();
  try {
    const details = await stat(fixture.tokenFile);
    expect(details.isFIFO()).toBe(true);
    expect(details.mode & 0o777).toBe(0o600);

    const child = await runIsolatedScript(statusServerFifoReportScript(fixture.tokenFile));
    expect(child.timedOut).toBe(false);
    expect(child.exitCode).toBe(0);
    expectNoToken(child.stderr, "FIFO-token subprocess error");
    const report = JSON.parse(child.stdout.trim()) as {
      outcome: string;
      error: string;
      listenerStarts: number;
      elapsedMs: number;
    };
    expect(report.outcome).toBe("rejected");
    expectNoToken(report.error, "FIFO-token startup error");
    expect(report.listenerStarts).toBe(0);
    expect(report.elapsedMs).toBeLessThan(FIFO_REJECTION_MAX_MS);
  } finally {
    await fixture.cleanup();
  }
});

test.skipIf(process.platform !== "darwin")("refuses a private-mode token file that grants other users access through an ACL", async () => {
  const fixture = await createTokenFixture();
  let server: StatusServer | undefined;
  try {
    const chmod = Bun.spawnSync(["/bin/chmod", "+a", "everyone allow read", fixture.tokenFile]);
    expect(chmod.exitCode).toBe(0);
    const startStatusServer = await loadStartStatusServer();
    await expect(startStatusServer(serverOptions({ tokenFile: fixture.tokenFile })).then(value => {
      server = value;
    })).rejects.toThrow();
  } finally {
    await server?.stop();
    await fixture.cleanup();
  }
});

test("snapshot exceptions do not expose or print the bearer token", async () => {
  const fixture = await createTokenFixture();
  const loggedErrors: string[] = [];
  const errorSpy = vi.spyOn(console, "error").mockImplementation((...args) => {
    loggedErrors.push(args.map(String).join(" "));
  });
  let server: StatusServer | undefined;
  try {
    const startStatusServer = await loadStartStatusServer();
    server = await startStatusServer(serverOptions({ tokenFile: fixture.tokenFile }, () => {
      throw new Error(TOKEN);
    }));

    const response = await fetch(`http://127.0.0.1:${server.port}/v1/status`, {
      headers: { authorization: `Bearer ${TOKEN}` },
    });
    expect(response.status).toBe(500);
    const body = await response.text();
    expectNoToken(body, "Snapshot error response");
    expect(JSON.parse(body)).toEqual({ error: { code: "internal_error" } });
    expectNoToken(loggedErrors.join("\n"), "Server error log");
  } finally {
    await server?.stop();
    errorSpy.mockRestore();
    await fixture.cleanup();
  }
});
