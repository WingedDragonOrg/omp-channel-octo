import { expect, test } from "bun:test";
import { chmod, mkdtemp, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, parseConfig } from "../src/runtime/config.ts";

const EXAMPLE_ADDRESS = "198.51.100.17";
// A server credential is at least 32 random bytes encoded as hexadecimal (64 characters).
const STATUS_TOKEN = "0123456789abcdef".repeat(4);
const ENV_TOKEN = "fedcba9876543210".repeat(4);
const HOME = "/home/status-config-fixture";
const DEFAULT_STATUS_HTTP = { listenAddress: "0.0.0.0", port: 8765 } as const;

const STATUS_ENVIRONMENT = {
  enabled: "OMP_CHANNEL_OCTO_HTTP_ENABLED",
  listenAddress: "OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS",
  port: "OMP_CHANNEL_OCTO_HTTP_PORT",
  token: "OMP_CHANNEL_OCTO_HTTP_TOKEN",
  tokenFile: "OMP_CHANNEL_OCTO_HTTP_TOKEN_FILE",
} as const;

type StatusEnvironment = Partial<Record<keyof typeof STATUS_ENVIRONMENT, string>>;

function statusEnvironment(values: StatusEnvironment = {}): Record<string, string> {
  const env: Record<string, string> = {};
  for (const key of Object.keys(STATUS_ENVIRONMENT) as Array<keyof typeof STATUS_ENVIRONMENT>) {
    const value = values[key];
    if (value !== undefined) env[STATUS_ENVIRONMENT[key]] = value;
  }
  return env;
}

function configDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    botToken: "fixture-bot-token",
    allowedGroupIds: [],
    ompCommand: "omp",
    stateDir: "/tmp/status-config-fixture",
    ...overrides,
  };
}

function expectStatusHttpValidationFailure(statusHttp: unknown): void {
  let caught: unknown;
  try {
    parseConfig(configDocument({ statusHttp }), { env: {}, homeDir: HOME });
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeInstanceOf(Error);
}

function expectStatusEnvironmentFailure(env: Record<string, string>): Error {
  let caught: unknown;
  try {
    parseConfig(configDocument(), { env, homeDir: HOME });
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeInstanceOf(Error);
  return caught as Error;
}

async function writeConfig(path: string, document: unknown, mode = 0o600): Promise<void> {
  await writeFile(path, `${JSON.stringify(document, null, 2)}\n`, { encoding: "utf8", mode });
  await chmod(path, mode);
}

async function relativeFilesUnder(root: string): Promise<string[]> {
  const files: string[] = [];
  const pending: Array<{ directory: string; prefix: string }> = [{ directory: root, prefix: "" }];
  while (pending.length > 0) {
    const { directory, prefix } = pending.pop()!;
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        pending.push({ directory: join(directory, entry.name), prefix: relative });
      } else if (entry.isFile()) {
        files.push(relative);
      }
    }
  }
  return files.sort();
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

function configFifoReportScript(configPath: string, homeDir: string): string {
  const configModule = new URL("../src/runtime/config.ts", import.meta.url).href;
  return `
    void (async () => {
      // Import the checked-out module inside the isolated child process.
      const { loadConfig } = await import(${JSON.stringify(configModule)});
      const startedAt = performance.now();
      let report;
      try {
        await loadConfig(${JSON.stringify(configPath)}, { env: {}, homeDir: ${JSON.stringify(homeDir)} });
        report = { outcome: "resolved", error: "" };
      } catch (error) {
        const parts = [];
        let current = error;
        while (current instanceof Error) {
          parts.push(current.message);
          current = "cause" in current ? current.cause : undefined;
        }
        if (current !== undefined) parts.push(String(current));
        report = { outcome: "rejected", error: parts.join(" ") };
      }
      report.elapsedMs = performance.now() - startedAt;
      process.stdout.write(JSON.stringify(report) + "\\n");
    })().catch(error => {
      console.error(error);
      process.exitCode = 1;
    });
  `;
}

test.skipIf(process.platform === "win32")("loadConfig promptly rejects a FIFO status token file without disclosing the token", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-fifo-"));
  const configPath = join(directory, "config.json");
  const tokenFile = join(directory, "status.token");

  try {
    expect(Bun.spawnSync(["mkfifo", tokenFile]).exitCode).toBe(0);
    expect((await stat(tokenFile)).isFIFO()).toBe(true);
    await chmod(tokenFile, 0o600);
    await writeConfig(configPath, configDocument({
      statusHttp: { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile },
    }));

    const child = await runIsolatedScript(configFifoReportScript(configPath, directory));
    expect(child.timedOut).toBe(false);
    expect(child.exitCode).toBe(0);
    expect(child.stderr).not.toContain(STATUS_TOKEN);
    const report = JSON.parse(child.stdout.trim()) as { outcome: string; error: string; elapsedMs: number };
    expect(report.outcome).toBe("rejected");
    expect(report.elapsedMs).toBeLessThan(FIFO_REJECTION_MAX_MS);
    expect(report.error).not.toContain(STATUS_TOKEN);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("statusHttp defaults to the enabled all-interface listener without credentials", () => {
  const parsed = parseConfig(configDocument(), { env: {}, homeDir: HOME });
  expect(parsed.statusHttp).toMatchObject(DEFAULT_STATUS_HTTP);
  expect(parsed.statusHttp?.token).toBeUndefined();
  expect(parsed.statusHttp?.tokenFile).toBeUndefined();
});

test("OMP_CHANNEL_OCTO_HTTP_ENABLED=false disables the listener even when a token is configured", () => {
  const parsed = parseConfig(
    configDocument({
      statusHttp: { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile: "/var/private/status.token" },
    }),
    { env: statusEnvironment({ enabled: "false", token: ENV_TOKEN }), homeDir: HOME },
  );
  // Presence of `statusHttp` is the enabled signal; explicit disable removes the listener entirely.
  expect(parsed.statusHttp).toBeUndefined();
});

test("status environment values override every configured statusHttp field", () => {
  const parsed = parseConfig(
    configDocument({
      statusHttp: {
        listenAddress: EXAMPLE_ADDRESS,
        port: 43127,
        tokenFile: "/var/private/status.token",
      },
    }),
    {
      env: statusEnvironment({
        listenAddress: "127.0.0.1",
        port: "43128",
        tokenFile: "~/environment-status.token",
      }),
      homeDir: HOME,
    },
  );

  expect(parsed.statusHttp).toMatchObject({
    listenAddress: "127.0.0.1",
    port: 43128,
    tokenFile: join(HOME, "environment-status.token"),
  });
});

test("status environment values fill only the fields they set", () => {
  const parsed = parseConfig(
    configDocument({
      statusHttp: {
        listenAddress: EXAMPLE_ADDRESS,
        port: 43127,
        tokenFile: "/var/private/status.token",
      },
    }),
    { env: statusEnvironment({ port: "43128" }), homeDir: HOME },
  );

  expect(parsed.statusHttp).toMatchObject({
    listenAddress: EXAMPLE_ADDRESS,
    port: 43128,
    tokenFile: "/var/private/status.token",
  });
});

test("statusHttp block fields fall back individually to defaults", () => {
  const cases: ReadonlyArray<readonly [unknown, Record<string, unknown>]> = [
    [{}, DEFAULT_STATUS_HTTP],
    [{ listenAddress: "127.0.0.1" }, { ...DEFAULT_STATUS_HTTP, listenAddress: "127.0.0.1" }],
    [{ port: 43127 }, { ...DEFAULT_STATUS_HTTP, port: 43127 }],
    [
      { tokenFile: "~/.oco/status/status.token" },
      { ...DEFAULT_STATUS_HTTP, tokenFile: join(HOME, ".oco/status/status.token") },
    ],
  ];

  for (const [statusHttp, expected] of cases) {
    const parsed = parseConfig(configDocument({ statusHttp }), { env: {}, homeDir: HOME });
    expect(parsed.statusHttp).toMatchObject(expected);
    expect(parsed.statusHttp?.token).toBeUndefined();
  }
});

test("a direct environment token enables the listener when statusHttp is omitted", () => {
  const parsed = parseConfig(configDocument(), {
    env: statusEnvironment({ token: ENV_TOKEN }),
    homeDir: HOME,
  });

  expect(parsed.statusHttp).toMatchObject({ ...DEFAULT_STATUS_HTTP, token: ENV_TOKEN });
});

test("statusHttp accepts general IP bind addresses and resolves absolute and home-relative token files", () => {
  // These addresses test syntax only; the parser does not infer which network interface is trusted.
  const addresses = [EXAMPLE_ADDRESS, "203.0.113.8", "127.0.0.1", "0.0.0.0", "::1", "::"] as const;
  const tokenFileCases = [
    ["/var/private/oco/status.token", "/var/private/oco/status.token"],
    ["~/.oco/status/status.token", join(HOME, ".oco/status/status.token")],
  ] as const;

  for (const listenAddress of addresses) {
    for (const [tokenFile, resolvedTokenFile] of tokenFileCases) {
      const parsed = parseConfig(
        configDocument({ statusHttp: { listenAddress, port: 43127, tokenFile } }),
        { env: {}, homeDir: HOME },
      );
      expect(parsed).toMatchObject({
        statusHttp: { listenAddress, port: 43127, tokenFile: resolvedTokenFile },
      });
    }
  }
});

const invalidStatusHttp: ReadonlyArray<readonly [string, unknown]> = [
  ["a null block", null],
  ["an array block", []],
  ["a primitive block", true],
  ["a whitespace-only listenAddress", { listenAddress: " ", port: 43127, tokenFile: "/tmp/status.token" }],
  ["a malformed IP literal", { listenAddress: "300.1.1.1", port: 43127, tokenFile: "/tmp/status.token" }],
  ["a URL instead of a bind address", { listenAddress: "http://198.51.100.17", port: 43127, tokenFile: "/tmp/status.token" }],
  ["a CIDR instead of a single bind address", { listenAddress: "198.51.100.17/24", port: 43127, tokenFile: "/tmp/status.token" }],
  ["a string port", { listenAddress: EXAMPLE_ADDRESS, port: "43127", tokenFile: "/tmp/status.token" }],
  ["a fractional port", { listenAddress: EXAMPLE_ADDRESS, port: 43127.5, tokenFile: "/tmp/status.token" }],
  ["port zero", { listenAddress: EXAMPLE_ADDRESS, port: 0, tokenFile: "/tmp/status.token" }],
  ["a port above the TCP range", { listenAddress: EXAMPLE_ADDRESS, port: 65536, tokenFile: "/tmp/status.token" }],
  ["a relative token file", { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile: "status.token" }],
  ["a token path for another user's home", { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile: "~someone/status.token" }],
  ["an inline token instead of only a tokenFile", { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile: "/tmp/status.token", token: STATUS_TOKEN }],
  ["an unsupported statusHttp field", { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile: "/tmp/status.token", enabled: true }],
];

for (const [description, statusHttp] of invalidStatusHttp) {
  test(`statusHttp rejects ${description}`, () => {
    expectStatusHttpValidationFailure(statusHttp);
  });
}

const invalidStatusEnvironment: ReadonlyArray<readonly [string, StatusEnvironment]> = [
  ["a non-integer port", { port: "43127.5" }],
  ["a non-numeric port", { port: "abc" }],
  ["a port below the TCP range", { port: "0" }],
  ["a port above the TCP range", { port: "65536" }],
  ["a URL instead of a bind address", { listenAddress: "http://127.0.0.1" }],
  ["a malformed IP literal", { listenAddress: "300.1.1.1" }],
  ["a relative token file", { tokenFile: "status.token" }],
  ["a token path for another user's home", { tokenFile: "~someone/status.token" }],
];

for (const [description, env] of invalidStatusEnvironment) {
  test(`status environment rejects ${description}`, () => {
    expectStatusEnvironmentFailure(statusEnvironment(env));
  });
}

const invalidStatusEnvironmentTokens = [
  ["a short token", "abc"],
  ["an odd-length hexadecimal token", "a".repeat(63)],
  ["a non-hexadecimal token", "g".repeat(64)],
] as const;

for (const [description, token] of invalidStatusEnvironmentTokens) {
  test(`status environment token rejects ${description} without echoing it`, () => {
    const error = expectStatusEnvironmentFailure(statusEnvironment({ token }));
    expect(error.message).not.toContain(token);
  });
}

test("loadConfig enables the listener by default without creating any status file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-default-"));
  const configPath = join(directory, "config.json");

  try {
    await writeConfig(configPath, configDocument());

    const loaded = await loadConfig(configPath, { env: {}, homeDir: directory });
    expect(loaded.statusHttp).toMatchObject(DEFAULT_STATUS_HTTP);
    expect(loaded.statusHttp?.token).toBeUndefined();
    expect(loaded.statusHttp?.tokenFile).toBeUndefined();

    // The only file is the operator's config: no generated token file or credential directory.
    expect(await relativeFilesUnder(directory)).toEqual(["config.json"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig ignores a missing token file when a direct environment token is set", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-env-token-"));
  const configPath = join(directory, "config.json");
  const missingTokenFile = join(directory, "missing.status.token");

  try {
    await writeConfig(configPath, configDocument());

    const loaded = await loadConfig(configPath, {
      env: statusEnvironment({ token: ENV_TOKEN, tokenFile: "~/missing.status.token" }),
      homeDir: directory,
    });

    expect(loaded.statusHttp?.token).toBe(ENV_TOKEN);
    await expect(stat(missingTokenFile)).rejects.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig falls back to a private token file configured through the environment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-env-file-"));
  const configPath = join(directory, "config.json");
  const tokenFile = join(directory, "status.token");

  try {
    await writeFile(tokenFile, STATUS_TOKEN, { encoding: "utf8", mode: 0o600 });
    await chmod(tokenFile, 0o600);
    await writeConfig(configPath, configDocument());

    const loaded = await loadConfig(configPath, {
      env: statusEnvironment({ tokenFile: "~/status.token" }),
      homeDir: directory,
    });

    expect(loaded.statusHttp).toMatchObject({ ...DEFAULT_STATUS_HTTP, tokenFile });
    expect(loaded.statusHttp?.token).toBeUndefined();
    expect(JSON.stringify(loaded)).not.toContain(STATUS_TOKEN);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig rejects an environment token file that is missing or unsafe", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-env-bad-file-"));
  const configPath = join(directory, "config.json");

  try {
    await writeConfig(configPath, configDocument());

    const missingTokenFile = join(directory, "missing.status.token");
    await expect(
      loadConfig(configPath, {
        env: statusEnvironment({ tokenFile: missingTokenFile }),
        homeDir: directory,
      }),
    ).rejects.toThrow();

    const unsafeTokenFile = join(directory, "unsafe.status.token");
    await writeFile(unsafeTokenFile, STATUS_TOKEN, { encoding: "utf8", mode: 0o644 });
    await chmod(unsafeTokenFile, 0o644);
    await expect(
      loadConfig(configPath, {
        env: statusEnvironment({ tokenFile: unsafeTokenFile }),
        homeDir: directory,
      }),
    ).rejects.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig rejects an explicitly configured status token file that does not exist", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-missing-file-"));
  const configPath = join(directory, "config.json");
  const tokenFile = join(directory, "missing.status.token");

  try {
    await writeConfig(configPath, configDocument({
      statusHttp: { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile },
    }));

    await expect(loadConfig(configPath, { env: {}, homeDir: directory })).rejects.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig keeps the status token in a private separate file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-"));
  const configPath = join(directory, "config.json");
  const tokenFile = join(directory, "status.token");
  const statusHttp = { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile };

  try {
    await writeFile(tokenFile, STATUS_TOKEN, { encoding: "utf8", mode: 0o600 });
    await chmod(tokenFile, 0o600);
    await writeConfig(configPath, configDocument({ statusHttp }));

    const loaded = await loadConfig(configPath, { env: {}, homeDir: directory });

    expect(loaded).toMatchObject({ statusHttp });
    expect(loaded.statusHttp?.token).toBeUndefined();
    expect(JSON.stringify(loaded)).not.toContain(STATUS_TOKEN);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("loadConfig refuses a status token file readable by group or others", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-"));
  const configPath = join(directory, "config.json");
  const tokenFile = join(directory, "status.token");

  try {
    await writeFile(tokenFile, STATUS_TOKEN, { encoding: "utf8", mode: 0o640 });
    await chmod(tokenFile, 0o640);
    await writeConfig(configPath, configDocument({
      statusHttp: { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile },
    }));

    let caught: unknown;
    try {
      await loadConfig(configPath, { env: {}, homeDir: directory });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(Error);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test.skipIf(process.platform !== "darwin")("loadConfig rejects a status token accessible through an ACL", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-status-config-acl-"));
  try {
    const configPath = join(directory, "config.json");
    const tokenFile = join(directory, "status.token");
    await writeFile(tokenFile, STATUS_TOKEN, { mode: 0o600 });
    await chmod(tokenFile, 0o600);
    expect(Bun.spawnSync(["/bin/chmod", "+a", "everyone allow read", tokenFile]).exitCode).toBe(0);
    await writeConfig(configPath, configDocument({
      statusHttp: { listenAddress: EXAMPLE_ADDRESS, port: 43127, tokenFile },
    }));
    await expect(loadConfig(configPath, { env: {}, homeDir: directory })).rejects.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
