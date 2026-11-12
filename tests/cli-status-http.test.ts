import { expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type OctoProtocolFixture,
} from "./fixtures/octo-protocol-server.ts";

const CHECKOUT_ROOT = resolve(import.meta.dir, "..");
const LISTEN_ADDRESS = "127.0.0.1";
const READY_TIMEOUT_MS = 5_000;

type StatusHttpConfig = {
  listenAddress: string;
  port: number;
  tokenFile: string;
};

async function reserveLoopbackPort(): Promise<number> {
  const server = createServer();
  const listening = once(server, "listening");
  server.listen(0, LISTEN_ADDRESS);
  await listening;
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("temporary listener did not expose its TCP port");
  }
  const closed = once(server, "close");
  server.close();
  await closed;
  return address.port;
}

const STATUS_ENV_KEYS = [
  "OMP_CHANNEL_OCTO_HTTP_ENABLED",
  "OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS",
  "OMP_CHANNEL_OCTO_HTTP_PORT",
  "OMP_CHANNEL_OCTO_HTTP_TOKEN",
  "OMP_CHANNEL_OCTO_HTTP_TOKEN_FILE",
] as const;

function cliEnvironment(home: string, statusEnv: Record<string, string>): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value === undefined || (STATUS_ENV_KEYS as readonly string[]).includes(key)) continue;
    env[key] = value;
  }
  return { ...env, HOME: home, TZ: "UTC", ...statusEnv };
}

async function startCliRun(
  home: string,
  fixture: OctoProtocolFixture,
  options: { statusHttp?: StatusHttpConfig; statusEnv?: Record<string, string> } = {},
): Promise<{ child: Bun.Subprocess; output: Promise<[string, string]> }> {
  const configPath = join(home, "config.json");
  const config: Record<string, unknown> = {
    apiUrl: fixture.apiUrl,
    botToken: fixture.botToken,
    allowedGroupIds: [],
    stateDir: join(home, "state"),
    ompCommand: process.execPath,
  };
  if (options.statusHttp !== undefined) config.statusHttp = options.statusHttp;
  await writeFile(configPath, JSON.stringify(config), { encoding: "utf8", mode: 0o600 });
  await chmod(configPath, 0o600);

  const child = Bun.spawn(
    [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "run", "--config", configPath],
    {
      cwd: CHECKOUT_ROOT,
      stdout: "pipe",
      stderr: "pipe",
      env: cliEnvironment(home, options.statusEnv ?? {}),
    },
  );
  const output = Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { child, output };
}

async function waitForReadyConnection(fixture: OctoProtocolFixture): Promise<void> {
  const connection = await fixture.waitForConnection(1, READY_TIMEOUT_MS);
  await waitForFixture(
    () => connection.ready ? connection : undefined,
    READY_TIMEOUT_MS,
    "CLI did not authenticate its WebSocket connection to the isolated protocol fixture",
  );
}

// The listener belongs to a separate real CLI process; fake timers cannot advance its startup.
async function waitForStatusListener(url: string, child: Bun.Subprocess): Promise<void> {
  const deadline = Date.now() + READY_TIMEOUT_MS;
  while (Date.now() < deadline && child.exitCode === null) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(500) });
      await response.body?.cancel();
      return;
    } catch {
      await Bun.sleep(20);
    }
  }
  throw new Error("CLI status listener did not become ready");
}

async function fetchStatus(
  url: string,
  authorization?: string,
): Promise<Response> {
  try {
    return await fetch(url, {
      headers: authorization === undefined ? {} : { authorization },
      signal: AbortSignal.timeout(1_000),
    });
  } catch (error) {
    expect(error, "the configured status HTTP listener must answer a real HTTP request").toBeUndefined();
    throw error;
  }
}

// This bounds waiting on a separate CLI subprocess; fake timers cannot advance its exit.
async function waitForChildExit(child: Bun.Subprocess, timeoutMs = READY_TIMEOUT_MS): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("CLI child did not exit after SIGTERM")), timeoutMs);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}


function errorCode(error: unknown): string | undefined {
  if (error === null || typeof error !== "object") return undefined;
  if ("code" in error && typeof error.code === "string") return error.code;
  return "cause" in error ? errorCode(error.cause) : undefined;
}

async function expectConnectionRefused(port: number): Promise<void> {
  let failure: unknown;
  try {
    const response = await fetch(`http://${LISTEN_ADDRESS}:${port}/v1/status`, {
      signal: AbortSignal.timeout(1_000),
    });
    await response.body?.cancel();
  } catch (error) {
    failure = error;
  }
  expect(errorCode(failure)).toBe("ECONNREFUSED");
}

async function stopChild(child: Bun.Subprocess | undefined): Promise<void> {
  if (child && child.exitCode === null) {
    child.kill("SIGKILL");
    await child.exited;
  }
}

test("real CLI run serves an authenticated status snapshot and closes its listener on SIGTERM", async () => {
  let home: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    home = await mkdtemp(join(tmpdir(), "oco-cli-status-http-"));
    const token = randomBytes(32).toString("hex");
    const tokenFile = join(home, "status-token");
    await writeFile(tokenFile, token, { encoding: "utf8", mode: 0o600 });
    await chmod(tokenFile, 0o600);

    fixture = await createOctoProtocolFixture();
    const port = await reserveLoopbackPort();
    const started = await startCliRun(home, fixture, {
      statusHttp: {
        listenAddress: LISTEN_ADDRESS,
        port,
        tokenFile,
      },
    });
    child = started.child;
    output = started.output;
    await waitForReadyConnection(fixture);
    expect(fixture.registerRequests).toHaveLength(1);

    const statusUrl = `http://${LISTEN_ADDRESS}:${port}/v1/status`;
    await waitForStatusListener(statusUrl, child);
    const authorized = await fetchStatus(statusUrl, `Bearer ${token}`);
    expect(authorized.status).toBe(200);
    const authorizedBody = await authorized.text();
    expect(authorizedBody).not.toContain(token);
    const snapshot: unknown = JSON.parse(authorizedBody);
    expect(snapshot).toMatchObject({ version: 1, pid: child.pid });
    expect(snapshot).toHaveProperty("pid", child.pid);

    const detailsUrl = `http://${LISTEN_ADDRESS}:${port}/v1/chats/details?channelType=2&channelId=fixture-group`;
    const detailsResponse = await fetchStatus(detailsUrl, `Bearer ${token}`);
    expect(detailsResponse.status).toBe(200);
    expect(await detailsResponse.json()).toMatchObject({
      version: 1,
      channelType: 2,
      channelId: "fixture-group",
      context: { source: "unavailable", usedTokens: null, contextWindow: null },
      references: { pr: [], mr: [] },
      tasks: [],
    });
    expect((await fetchStatus(detailsUrl)).status).toBe(401);

    for (const authorization of [undefined, `Bearer ${token}-wrong`]) {
      const response = await fetchStatus(statusUrl, authorization);
      expect(response.status).toBe(401);
      const body = await response.text();
      expect(JSON.parse(body)).toEqual({ error: { code: expect.any(String) } });
      expect(body).not.toContain(String(child.pid));
      expect(body).not.toContain(token);
    }

    child.kill("SIGTERM");
    expect(await waitForChildExit(child)).toBe(0);
    await expectConnectionRefused(port);

    const [stdout, stderr] = await output;
    expect(stdout).not.toContain(token);
    expect(stderr).not.toContain(token);
  } finally {
    await stopChild(child);
    if (output !== undefined) await output;
    if (fixture !== undefined) await fixture.close();
    if (home !== undefined) await rm(home, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI run leaves the status port closed when no token is configured", async () => {
  let home: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    home = await mkdtemp(join(tmpdir(), "oco-cli-status-disabled-"));
    fixture = await createOctoProtocolFixture();
    const port = await reserveLoopbackPort();
    const started = await startCliRun(home, fixture);
    child = started.child;
    output = started.output;
    await waitForReadyConnection(fixture);
    expect(fixture.registerRequests).toHaveLength(1);

    await expectConnectionRefused(port);

    child.kill("SIGTERM");
    expect(await waitForChildExit(child)).toBe(0);
  } finally {
    await stopChild(child);
    if (output !== undefined) await output;
    if (fixture !== undefined) await fixture.close();
    if (home !== undefined) await rm(home, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI run starts the status listener from environment variables alone", async () => {
  let home: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    home = await mkdtemp(join(tmpdir(), "oco-cli-status-env-"));
    const token = randomBytes(32).toString("hex");
    fixture = await createOctoProtocolFixture();
    const port = await reserveLoopbackPort();
    const started = await startCliRun(home, fixture, {
      statusEnv: {
        OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS: LISTEN_ADDRESS,
        OMP_CHANNEL_OCTO_HTTP_PORT: String(port),
        OMP_CHANNEL_OCTO_HTTP_TOKEN: token,
      },
    });
    child = started.child;
    output = started.output;
    await waitForReadyConnection(fixture);
    expect(fixture.registerRequests).toHaveLength(1);

    const statusUrl = `http://${LISTEN_ADDRESS}:${port}/v1/status`;
    await waitForStatusListener(statusUrl, child);
    const authorized = await fetchStatus(statusUrl, `Bearer ${token}`);
    expect(authorized.status).toBe(200);
    const authorizedBody = await authorized.text();
    expect(authorizedBody).not.toContain(token);
    expect(JSON.parse(authorizedBody)).toMatchObject({ version: 1, pid: child.pid });

    for (const authorization of [undefined, `Bearer ${token}-wrong`]) {
      const response = await fetchStatus(statusUrl, authorization);
      expect(response.status).toBe(401);
      const body = await response.text();
      expect(body).not.toContain(token);
      expect(body).not.toContain(String(child.pid));
    }

    child.kill("SIGTERM");
    expect(await waitForChildExit(child)).toBe(0);
    await expectConnectionRefused(port);

    const [stdout, stderr] = await output;
    expect(stdout).not.toContain(token);
    expect(stderr).not.toContain(token);

    // Environment configuration is not persisted into the operator's config file.
    const configText = await readFile(join(home, "config.json"), "utf8");
    expect(configText).not.toContain("statusHttp");
    expect(configText).not.toContain(token);
  } finally {
    await stopChild(child);
    if (output !== undefined) await output;
    if (fixture !== undefined) await fixture.close();
    if (home !== undefined) await rm(home, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI run keeps the channel alive and reports a missing status token", async () => {
  let home: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    home = await mkdtemp(join(tmpdir(), "oco-cli-status-no-token-"));
    fixture = await createOctoProtocolFixture();
    const port = await reserveLoopbackPort();
    const started = await startCliRun(home, fixture, {
      statusEnv: {
        OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS: LISTEN_ADDRESS,
        OMP_CHANNEL_OCTO_HTTP_PORT: String(port),
      },
    });
    child = started.child;
    output = started.output;
    await waitForReadyConnection(fixture);
    expect(fixture.registerRequests).toHaveLength(1);
    expect(child.exitCode).toBeNull();

    await expectConnectionRefused(port);

    child.kill("SIGTERM");
    expect(await waitForChildExit(child)).toBe(0);

    const [stdout, stderr] = await output;
    const combined = `${stdout}\n${stderr}`;
    expect(combined).not.toMatch(/HTTP 状态接口已启动/u);
    // The operator must see which credential is missing; no value is required or printed.
    expect(combined).toMatch(/[^\n]*\[status\][^\n]*token/iu);
  } finally {
    await stopChild(child);
    if (output !== undefined) await output;
    if (fixture !== undefined) await fixture.close();
    if (home !== undefined) await rm(home, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI run disables the status listener when the environment says false", async () => {
  let home: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    home = await mkdtemp(join(tmpdir(), "oco-cli-status-off-"));
    fixture = await createOctoProtocolFixture();
    const port = await reserveLoopbackPort();
    const token = randomBytes(32).toString("hex");
    const started = await startCliRun(home, fixture, {
      statusEnv: {
        OMP_CHANNEL_OCTO_HTTP_ENABLED: "false",
        OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS: LISTEN_ADDRESS,
        OMP_CHANNEL_OCTO_HTTP_PORT: String(port),
        OMP_CHANNEL_OCTO_HTTP_TOKEN: token,
      },
    });
    child = started.child;
    output = started.output;
    await waitForReadyConnection(fixture);
    expect(fixture.registerRequests).toHaveLength(1);

    await expectConnectionRefused(port);

    child.kill("SIGTERM");
    expect(await waitForChildExit(child)).toBe(0);

    const [stdout, stderr] = await output;
    const combined = `${stdout}\n${stderr}`;
    expect(combined).not.toMatch(/HTTP 状态接口已启动/u);
    expect(combined).not.toContain(token);
  } finally {
    await stopChild(child);
    if (output !== undefined) await output;
    if (fixture !== undefined) await fixture.close();
    if (home !== undefined) await rm(home, { recursive: true, force: true });
  }
}, 20_000);
