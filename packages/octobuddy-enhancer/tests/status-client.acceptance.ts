import { chmod, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";

type StatusSnapshot = {
  version: 1;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
  sampledAt: string;
};

type ChatTarget = { channelType: 1 | 2 | 5; channelId: string };

type ChatStatusSnapshot = {
  version: 1;
  sampledAt: string;
  channelType: 1 | 2 | 5;
  channelId: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
};

type StatusResult =
  | { state: "online"; snapshot: StatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: StatusSnapshot };

type ChatStatusResult =
  | { state: "online"; snapshot: ChatStatusSnapshot }
  | { state: "unconfigured" | "offline" | "unauthorized" | "invalid"; lastSuccess?: ChatStatusSnapshot };

type StatusClientModule = {
  createStatusClient(options: { configPath: string }): {
    get(signal?: AbortSignal): Promise<StatusResult>;
    getChat(target: ChatTarget, signal?: AbortSignal): Promise<ChatStatusResult>;
  };
};

type LocalHttpFixture = {
  baseUrl: string;
  stop(): void;
};

const bearerToken = "test-channel-status-token-0123456789abcdef";
const snapshot: StatusSnapshot = {
  version: 1,
  pid: 47291,
  botUid: "bot-17",
  ownerUid: "owner/17",
  runningSessions: 6,
  queuedTasks: 3,
  sampledAt: "2026-11-11T10:14:30.000Z",
};

const privateChatSnapshot: ChatStatusSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T10:15:00.000Z",
  channelType: 1,
  channelId: "owner/17",
  state: "idle",
  model: "openai/gpt-6",
  cwd: "/Users/test/work/private",
  queuedTasks: 0,
  pendingQuestions: 2,
};

const groupChatSnapshot: ChatStatusSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T10:15:05.000Z",
  channelType: 2,
  channelId: "group + 7",
  state: "running",
  model: "anthropic/claude-sonnet",
  cwd: "/Users/test/work/group",
  queuedTasks: 2,
  pendingQuestions: 1,
};

const threadChatSnapshot: ChatStatusSnapshot = {
  version: 1,
  sampledAt: "2026-11-11T10:15:10.000Z",
  channelType: 5,
  channelId: "group + 7____thread/42",
  state: "queued",
  model: null,
  cwd: "/Users/test/work/thread",
  queuedTasks: 4,
  pendingQuestions: 0,
};

function expectChatStatusQuery(client: ReturnType<StatusClientModule["createStatusClient"]>): void {
  expect(typeof client.getChat, "createStatusClient must expose getChat(target, signal?)").toBe("function");
}

// Intentionally dynamic so the acceptance test can assert the missing production module at runtime.
async function loadStatusClient(): Promise<StatusClientModule> {
  const moduleUrl = new URL("../src/host/status-client.ts", import.meta.url).href;
  const imported: unknown = await import(moduleUrl).catch((error: unknown) => {
    expect(error, "status-client.ts must exist and be dynamically importable").toBeUndefined();
    return undefined;
  });

  expect(imported, "status-client.ts must export the status client").toBeDefined();
  expect(typeof (imported as StatusClientModule).createStatusClient).toBe("function");
  return imported as StatusClientModule;
}

async function withTempDirectory(exercise: (directory: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-status-client-"));
  try {
    await exercise(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

async function withLocalHttpFixture(
  fetch: (request: Request) => Response | Promise<Response>,
  exercise: (fixture: LocalHttpFixture) => Promise<void>,
): Promise<void> {
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch });
  let stopped = false;
  const fixture: LocalHttpFixture = {
    baseUrl: `http://127.0.0.1:${server.port}`,
    stop() {
      if (stopped) return;
      stopped = true;
      server.stop(true);
    },
  };

  try {
    await exercise(fixture);
  } finally {
    fixture.stop();
  }
}

async function writeConfig(configPath: string, baseUrl: string, mode = 0o600): Promise<void> {
  await writeFile(configPath, JSON.stringify({ baseUrl, token: bearerToken }), { encoding: "utf8", mode });
  await chmod(configPath, mode);
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

function fifoConfigReportScript(configPath: string): string {
  const clientModule = new URL("../src/host/status-client.ts", import.meta.url).href;
  return `
    void (async () => {
      // Import the checked-out module inside the isolated process so a blocked FIFO open is killable.
      const { createStatusClient } = await import(${JSON.stringify(clientModule)});
      const originalFetch = globalThis.fetch;
      let requestCount = 0;
      globalThis.fetch = async () => {
        requestCount += 1;
        throw new Error("unexpected network request");
      };
      const startedAt = performance.now();
      let report;
      try {
        const result = await createStatusClient({ configPath: ${JSON.stringify(configPath)} }).get();
        report = { outcome: "returned", state: result.state, error: "" };
      } catch (caught) {
        const parts = [];
        let current = caught;
        while (current instanceof Error) {
          parts.push(current.message);
          current = "cause" in current ? current.cause : undefined;
        }
        if (current !== undefined) parts.push(String(current));
        report = { outcome: "threw", state: "", error: parts.join(" ") };
      } finally {
        globalThis.fetch = originalFetch;
      }
      report.requestCount = requestCount;
      report.elapsedMs = performance.now() - startedAt;
      process.stdout.write(JSON.stringify(report) + "\\n");
    })().catch(error => {
      console.error(error);
      process.exitCode = 1;
    });
  `;
}

function statusResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

test("returns a real localhost snapshot, then marks it stale after the connection drops", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async (directory) => {
    await withLocalHttpFixture((request) => {
      if (request.url !== `${new URL(request.url).origin}/v1/status`) return new Response(null, { status: 404 });
      if (request.headers.get("authorization") !== `Bearer ${bearerToken}`) {
        return statusResponse({ error: { code: "unauthorized" } }, 401);
      }
      return statusResponse(snapshot);
    }, async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const client = clientModule.createStatusClient({ configPath });

      expect(await client.get()).toEqual({ state: "online", snapshot });
      fixture.stop();
      expect(await client.get()).toEqual({ state: "offline", lastSuccess: snapshot });
    });
  });
}, 10_000);

test("queries exact chat identities and keeps snapshots separate while switching chats", async () => {
  const clientModule = await loadStatusClient();
  const chats: { target: ChatTarget; snapshot: ChatStatusSnapshot }[] = [
    { target: { channelType: 1, channelId: "owner/17" }, snapshot: privateChatSnapshot },
    { target: { channelType: 2, channelId: "group + 7" }, snapshot: groupChatSnapshot },
    { target: { channelType: 5, channelId: "group + 7____thread/42" }, snapshot: threadChatSnapshot },
  ];
  const requests: {
    method: string;
    pathname: string;
    channelType: string | null;
    channelId: string | null;
    authorization: string | null;
  }[] = [];

  await withTempDirectory(async (directory) => {
    await withLocalHttpFixture((request) => {
      const url = new URL(request.url);
      requests.push({
        method: request.method,
        pathname: url.pathname,
        channelType: url.searchParams.get("channelType"),
        channelId: url.searchParams.get("channelId"),
        authorization: request.headers.get("authorization"),
      });
      if (request.headers.get("authorization") !== `Bearer ${bearerToken}`) {
        return statusResponse({ error: { code: "unauthorized" } }, 401);
      }
      if (request.method !== "GET" || url.pathname !== "/v1/chats/status") {
        return statusResponse({ error: { code: "not_found" } }, 404);
      }

      const chat = chats.find(({ target }) =>
        url.searchParams.get("channelType") === String(target.channelType) &&
        url.searchParams.get("channelId") === target.channelId
      );
      return chat
        ? statusResponse(chat.snapshot)
        : statusResponse({ error: { code: "invalid_request" } }, 400);
    }, async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const client = clientModule.createStatusClient({ configPath });
      expectChatStatusQuery(client);

      const results = [];
      for (const { target } of chats) {
        results.push(await client.getChat(target));
      }

      expect(results).toEqual(chats.map(({ snapshot }) => ({ state: "online", snapshot })));
      expect(requests).toEqual(chats.map(({ target }) => ({
        method: "GET",
        pathname: "/v1/chats/status",
        channelType: String(target.channelType),
        channelId: target.channelId,
        authorization: `Bearer ${bearerToken}`,
      })));
    });
  });
}, 10_000);

test("does not treat non-success chat status responses as online", async () => {
  const clientModule = await loadStatusClient();
  const target: ChatTarget = { channelType: 2, channelId: "group-error" };
  const authorizationHeaders: (string | null)[] = [];
  let responseCount = 0;

  await withTempDirectory(async (directory) => {
    await withLocalHttpFixture((request) => {
      authorizationHeaders.push(request.headers.get("authorization"));
      responseCount += 1;
      return responseCount === 1
        ? statusResponse({ error: { code: "unauthorized" } }, 401)
        : statusResponse({ error: { code: "unavailable" } }, 503);
    }, async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const client = clientModule.createStatusClient({ configPath });
      expectChatStatusQuery(client);

      expect(await client.getChat(target)).toEqual({ state: "unauthorized" });
      expect((await client.getChat(target)).state).not.toBe("online");
      expect(authorizationHeaders).toEqual([`Bearer ${bearerToken}`, `Bearer ${bearerToken}`]);
    });
  });
}, 10_000);

test("rejects a chat snapshot whose response identity differs from the requested chat", async () => {
  const clientModule = await loadStatusClient();
  const target: ChatTarget = { channelType: 2, channelId: "current-group" };
  const otherChatSnapshot: ChatStatusSnapshot = {
    ...groupChatSnapshot,
    channelType: 5,
    channelId: "different-group____thread",
  };

  await withTempDirectory(async (directory) => {
    await withLocalHttpFixture(() => statusResponse(otherChatSnapshot), async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const client = clientModule.createStatusClient({ configPath });
      expectChatStatusQuery(client);

      expect(await client.getChat(target)).toEqual({ state: "invalid" });
    });
  });
}, 10_000);


test("classifies an HTTP 401 from the localhost status endpoint as unauthorized", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async (directory) => {
    let authorization: string | null = null;
    await withLocalHttpFixture((request) => {
      authorization = request.headers.get("authorization");
      return statusResponse({ error: { code: "unauthorized" } }, 401);
    }, async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const client = clientModule.createStatusClient({ configPath });

      expect(await client.get()).toEqual({ state: "unauthorized" });
      expect(authorization).toBe(`Bearer ${bearerToken}`);
    });
  });
}, 10_000);

test("reports missing configuration without making an HTTP request", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async (directory) => {
    let requestCount = 0;
    await withLocalHttpFixture(() => {
      requestCount += 1;
      return statusResponse(snapshot);
    }, async () => {
      const client = clientModule.createStatusClient({ configPath: join(directory, "missing.json") });

      expect(await client.get()).toEqual({ state: "unconfigured" });
      expect(requestCount).toBe(0);
    });
  });
}, 10_000);

test.skipIf(process.platform === "win32")("rejects a FIFO client configuration promptly without making a network request", async () => {
  await withTempDirectory(async directory => {
    const configPath = join(directory, "status-client.json");
    expect(Bun.spawnSync(["mkfifo", configPath]).exitCode).toBe(0);
    expect((await stat(configPath)).isFIFO()).toBe(true);
    await chmod(configPath, 0o600);
    expect((await stat(configPath)).mode & 0o777).toBe(0o600);

    const child = await runIsolatedScript(fifoConfigReportScript(configPath));
    expect(child.timedOut).toBe(false);
    expect(child.exitCode).toBe(0);
    expect(child.stderr).not.toContain(bearerToken);
    const report = JSON.parse(child.stdout.trim()) as {
      outcome: string;
      state: string;
      error: string;
      requestCount: number;
      elapsedMs: number;
    };
    expect(report.outcome).toBe("returned");
    expect(report.state).toBe("invalid");
    expect(report.error).not.toContain(bearerToken);
    expect(report.requestCount).toBe(0);
    expect(report.elapsedMs).toBeLessThan(FIFO_REJECTION_MAX_MS);
  });
}, 10_000);

test("does not follow a localhost redirect or forward the bearer token to its target", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async (directory) => {
    let targetRequests = 0;
    let targetAuthorization: string | null = null;
    await withLocalHttpFixture((request) => {
      targetRequests += 1;
      targetAuthorization = request.headers.get("authorization");
      return statusResponse(snapshot);
    }, async (target) => {
      let sourceAuthorization: string | null = null;
      await withLocalHttpFixture((request) => {
        sourceAuthorization = request.headers.get("authorization");
        return Response.redirect(`${target.baseUrl}/v1/status`, 302);
      }, async (source) => {
        const configPath = join(directory, "status-client.json");
        await writeConfig(configPath, source.baseUrl);
        const client = clientModule.createStatusClient({ configPath });

        expect((await client.get()).state).not.toBe("online");
        expect(sourceAuthorization).toBe(`Bearer ${bearerToken}`);
        expect(targetRequests).toBe(0);
        expect(targetAuthorization).toBeNull();
      });
    });
  });
}, 10_000);

test("rejects a non-private configuration before connecting to its localhost endpoint", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async (directory) => {
    let requestCount = 0;
    await withLocalHttpFixture(() => {
      requestCount += 1;
      return statusResponse(snapshot);
    }, async (fixture) => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl, 0o644);
      expect((await stat(configPath)).mode & 0o777).toBe(0o644);
      const client = clientModule.createStatusClient({ configPath });

      expect(await client.get()).toEqual({ state: "invalid" });
      expect(requestCount).toBe(0);
    });
  });
}, 10_000);

test.skipIf(process.platform !== "darwin")("rejects a client configuration that grants other users access through an ACL", async () => {
  const clientModule = await loadStatusClient();
  await withTempDirectory(async directory => {
    await withLocalHttpFixture(() => statusResponse(snapshot), async fixture => {
      const configPath = join(directory, "status-client.json");
      await writeConfig(configPath, fixture.baseUrl);
      const chmod = Bun.spawnSync(["/bin/chmod", "+a", "everyone allow read", configPath]);
      expect(chmod.exitCode).toBe(0);
      expect(await clientModule.createStatusClient({ configPath }).get()).toEqual({ state: "invalid" });
    });
  });
});
