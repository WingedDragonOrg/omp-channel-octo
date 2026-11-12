import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, cp, mkdir, mkdtemp, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { loadStatusEnvironment } from "../src/launcher/status-environment.ts";

const packageDir = resolve(import.meta.dir, "..");
const launcherCli = join(packageDir, "src/cli.ts");
const hostCli = join(packageDir, "src/host/cli.ts");
const monitorCli = join(packageDir, "src/launcher/monitor.ts");
const testPreload = join(import.meta.dir, "launcher.test-preload.ts");
const fixture = join(import.meta.dir, "launcher.fixture.ts");
const decoder = new TextDecoder();

interface FixtureApp {
  appPath: string;
  launchLog: string;
  stopFile: string;
  stateDir: string;
  env: NodeJS.ProcessEnv;
}

interface TestSubprocess {
  pid: number;
  exitCode: number | null;
  exited: Promise<number>;
  stdin?: { write(value: string): number | Promise<number>; flush(): number | Promise<number> } | undefined;
  stdout?: ReadableStream<Uint8Array> | undefined;
  stderr?: ReadableStream<Uint8Array> | undefined;
  kill(signal?: NodeJS.Signals): void;
}

interface CapturedProcess {
  child: TestSubprocess;
  output: () => { stdout: string; stderr: string };
  outputDone: Promise<void>;
  finished: () => boolean;
  sendInput: (text: string) => void;
}

interface ListenerFixture {
  pid: number;
  port: number;
  stopFile: string;
  child: TestSubprocess;
  env: NodeJS.ProcessEnv;
}

interface DecoyFixture {
  appPath: string;
  pid: number;
  child: TestSubprocess;
}

interface MonitorProcess {
  pid: number;
  exitCode: number | null;
  exited: Promise<number>;
  stdout: ReadableStream<Uint8Array>;
  kill(signal?: NodeJS.Signals): void;
}

interface MonitorFixture {
  child: MonitorProcess;
  output: () => string;
  outputDone: Promise<void>;
}

interface RecordedProcessIdentity {
  pid: number;
  startedAt: string;
}

const FIXTURE_STATUS_ENVIRONMENT = {
  OCTOBUDDY_HTTP_BASE_URL: "http://127.0.0.1:1",
  OCTOBUDDY_HTTP_TOKEN: "synthetic-launcher-fixture-status-token-0123456789abcdef",
};

function isolatedEnvironment(directory: string): NodeJS.ProcessEnv {
  return {
    PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
    HOME: directory,
    TMPDIR: directory,
    XDG_CONFIG_HOME: join(directory, "config"),
    XDG_DATA_HOME: join(directory, "data"),
    XDG_STATE_HOME: join(directory, "state-home"),
    LC_ALL: "C",
    TZ: "UTC",
    // Both values make the launcher skip the real package .envrc during generic lifecycle tests.
    ...FIXTURE_STATUS_ENVIRONMENT,
  };
}

function withoutFixtureStatusEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const result = { ...env };
  delete result.OCTOBUDDY_HTTP_BASE_URL;
  delete result.OCTOBUDDY_HTTP_TOKEN;
  return result;
}

async function makeFixtureApp(directory: string): Promise<FixtureApp> {
  const canonicalDirectory = await realpath(directory);
  const appPath = join(canonicalDirectory, "OctoBuddy.app");
  const executable = join(appPath, "Contents", "MacOS", "OctoBuddy");
  const launchLog = join(canonicalDirectory, "app-launches.jsonl");
  const stopFile = join(canonicalDirectory, "stop-fixture-app");
  const stateDir = join(canonicalDirectory, "state");
  const runner = join(canonicalDirectory, "octobuddy-app-fixture.ts");
  const rendererDirectory = join(appPath, "Contents", "Resources", "communication-renderer");
  const rendererUrl = pathToFileURL(join(rendererDirectory, "index.html")).href;
  await mkdir(join(appPath, "Contents", "MacOS"), { recursive: true });
  await mkdir(rendererDirectory, { recursive: true });
  await writeFile(join(appPath, "Contents", "Info.plist"), [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<plist version=\"1.0\"><dict>",
    "<key>CFBundleExecutable</key><string>OctoBuddy</string>",
    "<key>CFBundleIdentifier</key><string>local.octobuddy.launcher.fixture</string>",
    "<key>CFBundleName</key><string>OctoBuddy</string>",
    "<key>CFBundlePackageType</key><string>APPL</string>",
    "<key>CFBundleVersion</key><string>1</string>",
    "</dict></plist>",
  ].join("\n"), "utf8");
  await writeFile(join(rendererDirectory, "index.html"), "<!doctype html><title>OctoBuddy fixture</title>\n", "utf8");

  const appFixtureArgs = ["--launch-log", launchLog, "--stop-file", stopFile, "--renderer-url", rendererUrl];
  await writeFile(runner, [
    `import { runFixture } from ${JSON.stringify(fixture)};`,
    `await runFixture("app", [...${JSON.stringify(appFixtureArgs)}, ...process.argv.slice(1)]);`,
    "",
  ].join("\n"), "utf8");
  const build = Bun.spawnSync([
    process.execPath, "build", runner, "--compile", "--outfile", executable,
  ], {
    env: isolatedEnvironment(directory),
    stdout: "pipe",
    stderr: "pipe",
  });
  if (build.exitCode !== 0) {
    throw new Error(`failed to compile isolated OctoBuddy fixture: ${decoder.decode(build.stderr)}`);
  }
  await chmod(executable, 0o755);

  return {
    appPath,
    launchLog,
    stopFile,
    stateDir,
    env: isolatedEnvironment(directory),
  };
}

function launcherCommand(app: FixtureApp, ...extraArgs: string[]): string[] {
  return [
    process.execPath,
    launcherCli,
    "--app-path", app.appPath,
    "--state-dir", app.stateDir,
    ...extraArgs,
  ];
}

const envrcStatusBaseUrl = "http://127.0.0.1:8765";
const envrcStatusToken = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
const inheritedStatusToken = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";

function statusEnvrc(baseUrl: string, token: string): string {
  return `export OCTOBUDDY_HTTP_BASE_URL='${baseUrl}'\nexport OCTOBUDDY_HTTP_TOKEN='${token}'\n`;
}

async function makeStatusEnvironmentPackage(directory: string, envrc: string): Promise<string> {
  const root = join(directory, "enhancer-package");
  await mkdir(root, { recursive: true, mode: 0o700 });
  for (const name of ["src", "plugins", "vendor"]) {
    await cp(join(packageDir, name), join(root, name), { recursive: true });
  }
  await symlink(join(packageDir, "node_modules"), join(root, "node_modules"));
  await writeFile(join(root, ".envrc"), envrc.endsWith("\n") ? envrc : `${envrc}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await chmod(join(root, ".envrc"), 0o600);
  return root;
}

async function preAcceptCdpConsent(stateDir: string): Promise<void> {
  await mkdir(stateDir, { recursive: true, mode: 0o700 });
  await writeFile(
    join(stateDir, "cdp-consent.json"),
    JSON.stringify({ version: 1, accepted: true, acceptedAt: new Date().toISOString() }),
    { encoding: "utf8", mode: 0o600 },
  );
}

function readProcessEnvironment(pid: number, names: readonly string[]): Record<string, string | undefined> {
  const result = Bun.spawnSync(["/bin/ps", "eww", "-p", String(pid)], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    stdout: "pipe",
    stderr: "pipe",
  });
  expect(result.exitCode).toBe(0);
  const text = decoder.decode(result.stdout);
  const values: Record<string, string | undefined> = {};
  for (const name of names) {
    const match = new RegExp(`(?:^|\\s)${name}=([^\\s]+)`).exec(text);
    values[name] = match?.[1];
  }
  return values;
}


async function collect(stream: ReadableStream<Uint8Array>, append: (text: string) => void): Promise<void> {
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    append(decoder.decode(value, { stream: true }));
  }
}

function startInteractive(command: string[], env: NodeJS.ProcessEnv): CapturedProcess {
  const child = Bun.spawn(command, {
    env,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
  let stdout = "";
  let stderr = "";
  let finished = false;
  const outputDone = Promise.all([
    collect(child.stdout, (text) => { stdout += text; }),
    collect(child.stderr, (text) => { stderr += text; }),
  ]).then(() => undefined);
  void child.exited.then(() => { finished = true; });
  return {
    child,
    output: () => ({ stdout, stderr }),
    outputDone,
    finished: () => finished,
    sendInput: (text) => {
      try {
        const input = child.stdin;
        if (!input) return;
        void Promise.resolve(input.write(text)).then(() => input.flush()).catch(() => {});
      } catch {
        // A concurrent invocation may exit on the single-instance lock before reading stdin.
      }
    },
  };
}

async function run(command: string[], env: NodeJS.ProcessEnv): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const child = Bun.spawn(command, { env, stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { exitCode, stdout, stderr };
}

async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 6_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error(`condition was not met within ${timeoutMs}ms`);
}

async function outputUntil(child: CapturedProcess, pattern: RegExp): Promise<void> {
  await waitUntil(() => pattern.test(`${child.output().stdout}\n${child.output().stderr}`) || child.finished());
  if (child.finished()) await child.outputDone;
  expect(`${child.output().stdout}\n${child.output().stderr}`).toMatch(pattern);
}
async function promptOrExit(child: CapturedProcess, pattern: RegExp): Promise<boolean> {
  await waitUntil(() => pattern.test(`${child.output().stdout}\n${child.output().stderr}`) || child.finished());
  if (child.finished()) await child.outputDone;
  return pattern.test(`${child.output().stdout}\n${child.output().stderr}`);
}
function capturedOutput(child: CapturedProcess): string {
  return `${child.output().stdout}\n${child.output().stderr}`.replace(/\s+/g, " ");
}

interface RecordedLaunch {
  pid: number;
  port: number;
  statusEnvironment?: { baseUrlPresent: boolean; tokenPresent: boolean };
}

async function readLaunches(path: string): Promise<RecordedLaunch[]> {
  try {
    const contents = await readFile(path, "utf8");
    return contents.split("\n").filter(Boolean).map((line) => JSON.parse(line) as RecordedLaunch);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

async function readLauncherState(stateDir: string): Promise<string | undefined> {
  try {
    const status = JSON.parse(await readFile(join(stateDir, "status.json"), "utf8")) as { state?: unknown };
    return typeof status.state === "string" ? status.state : undefined;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

function processIsAlive(pid: number): boolean {
  const result = Bun.spawnSync(["ps", "-p", String(pid), "-o", "stat="], { stdout: "pipe", stderr: "ignore" });
  if (result.exitCode !== 0) return false;
  const state = decoder.decode(result.stdout).trim();
  return state.length > 0 && !state.startsWith("Z");
}

function processIsStopped(pid: number): boolean {
  const result = Bun.spawnSync(["ps", "-p", String(pid), "-o", "stat="], { stdout: "pipe", stderr: "ignore" });
  if (result.exitCode !== 0) return false;
  return /^[Tt]/.test(decoder.decode(result.stdout).trim());
}

async function stopFixtureApp(app: FixtureApp, pids: number[] = []): Promise<void> {
  await writeFile(app.stopFile, "stop\n");
  await Promise.all(pids.map((pid) => waitUntil(() => !processIsAlive(pid), 4_000).catch(() => {})));
}

function jsonMessages(text: string): Array<Record<string, unknown>> {
  return text.split("\n").flatMap((line) => {
    try {
      const parsed: unknown = JSON.parse(line);
      return parsed !== null && typeof parsed === "object" ? [parsed as Record<string, unknown>] : [];
    } catch {
      return [];
    }
  });
}

async function processStartIdentity(pid: number): Promise<string> {
  const result = Bun.spawnSync(["ps", "-p", String(pid), "-o", "lstart="], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    stdout: "pipe",
    stderr: "pipe",
  });
  expect(result.exitCode).toBe(0);
  return decoder.decode(result.stdout).trim();
}

function currentProcessStartIdentity(pid: number): string | undefined {
  const result = Bun.spawnSync(["/bin/ps", "-p", String(pid), "-o", "lstart="], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    stdout: "pipe",
    stderr: "ignore",
  });
  if (result.exitCode !== 0) return undefined;
  return decoder.decode(result.stdout).trim() || undefined;
}

async function startListener(directory: string): Promise<ListenerFixture> {
  const marker = join(directory, "listener.json");
  const stopFile = join(directory, "stop-listener");
  const env = isolatedEnvironment(directory);
  const child = Bun.spawn([process.execPath, fixture, "listener", "--port", "0", "--marker", marker, "--stop-file", stopFile], {
    env,
    stdin: "ignore",
    stdout: "ignore",
    stderr: "pipe",
  });
  try {
    await waitUntil(() => existsSync(marker));
  } catch (error) {
    if (child.exitCode === null) child.kill("SIGTERM");
    await child.exited;
    throw error;
  }
  const result = JSON.parse(await readFile(marker, "utf8")) as { pid: number; port: number };
  return { ...result, stopFile, child, env };
}

function processCommandName(pid: number): string {
  const result = Bun.spawnSync(["ps", "-p", String(pid), "-o", "comm="], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    stdout: "pipe",
    stderr: "pipe",
  });
  if (result.exitCode !== 0) throw new Error(`cannot read process command for PID ${pid}`);
  return basename(decoder.decode(result.stdout).trim());
}

async function startNamedDecoy(directory: string): Promise<DecoyFixture> {
  const appPath = join(directory, "Unrelated OctoBuddy.app");
  const executable = join(appPath, "Contents", "MacOS", "OctoBuddy");
  await mkdir(join(appPath, "Contents", "MacOS"), { recursive: true });
  await writeFile(join(appPath, "Contents", "Info.plist"), [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<plist version=\"1.0\"><dict>",
    "<key>CFBundleExecutable</key><string>OctoBuddy</string>",
    "<key>CFBundleIdentifier</key><string>local.unrelated.octobuddy.fixture</string>",
    "<key>CFBundleName</key><string>OctoBuddy</string>",
    "<key>CFBundlePackageType</key><string>APPL</string>",
    "</dict></plist>",
  ].join("\n"), "utf8");
  await writeFile(executable, await readFile("/bin/sleep"));
  await chmod(executable, 0o755);
  const child = Bun.spawn([executable, "60"], {
    env: isolatedEnvironment(directory),
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
  });
  try {
    await waitUntil(() => {
      try {
        return processCommandName(child.pid) === "OctoBuddy";
      } catch {
        return false;
      }
    }, 8_000);
  } catch (error) {
    if (child.exitCode === null) child.kill("SIGTERM");
    await child.exited;
    throw error;
  }
  return { appPath, pid: child.pid, child };
}


function startLockRaceLauncher(app: FixtureApp, role: string, gateDirectory: string): CapturedProcess {
  const command = launcherCommand(app);
  const env = {
    ...app.env,
    OCTOBUDDY_TEST_LOCK_FILE: join(app.stateDir, "launcher.lock"),
    OCTOBUDDY_TEST_LOCK_GATE: gateDirectory,
    OCTOBUDDY_TEST_LOCK_ROLE: role,
  };
  return startInteractive([command[0]!, "--preload", testPreload, ...command.slice(1)], env);
}

async function readStateProcess(
  path: string,
  pidKey = "pid",
  startedAtKey = "startedAt",
): Promise<RecordedProcessIdentity | undefined> {
  try {
    const value: unknown = JSON.parse(await readFile(path, "utf8"));
    if (!value || typeof value !== "object") return undefined;
    const record = value as Record<string, unknown>;
    if (!Number.isSafeInteger(record[pidKey]) || typeof record[startedAtKey] !== "string") return undefined;
    return { pid: record[pidKey] as number, startedAt: record[startedAtKey] as string };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" || error instanceof SyntaxError) return undefined;
    throw error;
  }
}

function isRecordedProcessAlive(identity: RecordedProcessIdentity): boolean {
  return processIsAlive(identity.pid) && currentProcessStartIdentity(identity.pid) === identity.startedAt;
}

async function signalRecordedProcess(
  identity: RecordedProcessIdentity,
  signal: NodeJS.Signals = "SIGTERM",
): Promise<void> {
  if (!isRecordedProcessAlive(identity)) return;
  try {
    process.kill(identity.pid, signal);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
}

function startMonitorProcess(stateDir: string, directory: string): MonitorFixture {
  const child = Bun.spawn([process.execPath, monitorCli, "--state-dir", stateDir], {
    env: isolatedEnvironment(directory),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "ignore",
  });
  let output = "";
  const outputDone = collect(child.stdout, (text) => { output += text; });
  return { child, output: () => output, outputDone };
}

async function waitForMonitorReady(monitor: MonitorFixture): Promise<void> {
  await waitUntil(() => {
    const messages = jsonMessages(monitor.output());
    return messages.some((message) => message.type === "ready" || message.type === "error")
      || monitor.child.exitCode !== null;
  }, 8_000);
  const messages = jsonMessages(monitor.output());
  const error = messages.find((message) => message.type === "error");
  if (error) throw new Error(typeof error.message === "string" ? error.message : "monitor failed to start");
  expect(messages.some((message) => message.type === "ready")).toBe(true);
}

async function assertHostRejectsIdentity(
  listener: ListenerFixture,
  expectedPid: number,
  startedAt: string,
): Promise<void> {
  const result = await run([
    process.execPath,
    hostCli,
    "--port", String(listener.port),
    "--pid", String(expectedPid),
    "--started-at", startedAt,
  ], listener.env);
  const messages = jsonMessages(result.stdout);
  expect(result.exitCode).not.toBe(0);
  expect(messages.some((message) => message.type === "error" && typeof message.message === "string" && message.message.length > 0)).toBe(true);
  expect(processIsAlive(listener.pid)).toBe(true);
}

test("rejects a CDP port whose listening process is not the expected OctoBuddy PID", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-owner-"));
  let listener: ListenerFixture | undefined;
  try {
    listener = await startListener(directory);
    const unrelatedPid = process.pid;
    await assertHostRejectsIdentity(listener, unrelatedPid, await processStartIdentity(unrelatedPid));
  } finally {
    if (listener) {
      await writeFile(listener.stopFile, "stop\n");
      await listener.child.exited;
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 15_000);

test("rejects a listener process whose PID matches but start generation is stale", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-generation-"));
  let listener: ListenerFixture | undefined;
  try {
    listener = await startListener(directory);
    const actualStart = await processStartIdentity(listener.pid);
    const staleStart = actualStart.replace(/(\d{4})$/, (_year, year: string) => String(Number(year) - 1));
    expect(staleStart).not.toBe(actualStart);
    await assertHostRejectsIdentity(listener, listener.pid, staleStart);
  } finally {
    if (listener) {
      await writeFile(listener.stopFile, "stop\n");
      await listener.child.exited;
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 15_000);

test("records a stopped target after normal exit without affecting unrelated processes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-monitor-exit-"));
  const stateDir = join(directory, "state");
  let target: ListenerFixture | undefined;
  let survivor: ListenerFixture | undefined;
  let monitor: MonitorFixture | undefined;
  try {
    await mkdir(stateDir, { recursive: true, mode: 0o700 });
    const targetDir = join(directory, "target");
    const survivorDir = join(directory, "survivor");
    await mkdir(targetDir);
    await mkdir(survivorDir);
    target = await startListener(targetDir);
    survivor = await startListener(survivorDir);
    await writeFile(join(stateDir, "launcher.lock"), JSON.stringify({
      controllerPid: process.pid,
      controllerStartedAt: await processStartIdentity(process.pid),
    }), { encoding: "utf8", mode: 0o600 });
    await writeFile(join(stateDir, "target.json"), JSON.stringify({
      pid: target.pid,
      startedAt: await processStartIdentity(target.pid),
      port: target.port,
    }), "utf8");

    monitor = startMonitorProcess(stateDir, directory);
    await waitForMonitorReady(monitor);
    target.child.kill("SIGTERM");
    await target.child.exited;
    await waitUntil(async () => {
      const state = await readLauncherState(stateDir);
      return state === "stopped" || state === "error";
    }, 8_000);
    const state = await readLauncherState(stateDir);
    expect(state).toBe("stopped");
    expect(state).not.toBe("error");
    expect(processIsAlive(survivor.pid)).toBe(true);
    await waitUntil(() => monitor!.child.exitCode !== null, 8_000);
    await monitor.child.exited;
    await monitor.outputDone;
  } finally {
    for (const listener of [target, survivor]) {
      if (listener && processIsAlive(listener.pid)) listener.child.kill("SIGTERM");
    }
    if (monitor && processIsAlive(monitor.child.pid)) monitor.child.kill("SIGTERM");
    if (monitor) {
      await waitUntil(() => monitor!.child.exitCode !== null, 15_000).catch(() => {});
      await monitor.outputDone;
    }
    if (target) await target.child.exited;
    if (survivor) await survivor.child.exited;
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);

test("an unrelated OctoBuddy-named bundle does not block the explicitly selected app", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-unrelated-app-"));
  let app: FixtureApp | undefined;
  let decoy: DecoyFixture | undefined;
  let launcher: CapturedProcess | undefined;
  try {
    app = await makeFixtureApp(directory);
    decoy = await startNamedDecoy(directory);
    expect(decoy.appPath).not.toBe(app.appPath);
    expect(processCommandName(decoy.pid)).toBe("OctoBuddy");

    launcher = startInteractive(launcherCommand(app), app.env);
    await outputUntil(launcher, /继续启用 CDP？\[y\/N\]/);
    launcher.sendInput("y\n");
    await waitUntil(async () => (await readLaunches(app!.launchLog)).length > 0);

    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    expect(processIsAlive(decoy.pid)).toBe(true);
    expect(processIsAlive(launches[0]!.pid)).toBe(true);
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    if (decoy && processIsAlive(decoy.pid)) decoy.child.kill("SIGTERM");
    if (decoy) await decoy.child.exited;
    if (launcher && processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    await rm(directory, { recursive: true, force: true });
  }
}, 20_000);

// This exercises the CLI's piped-stdin automation path; the packaged Dock entry must show its own native consent UI.
test("does not start OctoBuddy until the user confirms local CDP exposure", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-consent-"));
  let app: FixtureApp | undefined;
  let launcher: CapturedProcess | undefined;
  try {
    app = await makeFixtureApp(directory);
    launcher = startInteractive(launcherCommand(app), app.env);
    await outputUntil(launcher, /CDP|调试端口|远程调试/i);
    expect(capturedOutput(launcher)).toMatch(/CDP|调试端口|远程调试/i);
    expect(capturedOutput(launcher)).toMatch(/本机|本地|local/i);
    expect(capturedOutput(launcher)).toMatch(/连接|读取|控制|访问|connect|access|control/i);
    expect(await readLaunches(app.launchLog)).toHaveLength(0);

    launcher.sendInput("n\n");
    await launcher.child.exited;
    await launcher.outputDone;
    expect(await readLaunches(app.launchLog)).toHaveLength(0);
    expect(`${launcher.output().stdout}\n${launcher.output().stderr}`).toMatch(/CDP|调试|debug/i);
  } finally {
    if (app) await stopFixtureApp(app);
    if (launcher && processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    await rm(directory, { recursive: true, force: true });
  }
}, 15_000);

test("a second launcher lifecycle ignores old state and monitor reports host failure without closing OctoBuddy", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-consent-memory-"));
  let app: FixtureApp | undefined;
  const launchers: CapturedProcess[] = [];
  const targetProcesses: RecordedProcessIdentity[] = [];
  const hostProcesses: RecordedProcessIdentity[] = [];
  const monitorProcesses: RecordedProcessIdentity[] = [];
  const prompt = /继续启用 CDP？\[y\/N\]/;
  try {
    app = await makeFixtureApp(directory);
    const first = startInteractive(launcherCommand(app), app.env);
    launchers.push(first);
    await outputUntil(first, prompt);
    first.sendInput("y\n");
    await waitUntil(async () => (await readLaunches(app!.launchLog)).length === 1);
    await waitUntil(async () => {
      const state = await readLauncherState(app!.stateDir);
      return state === "ready" || state === "error";
    }, 15_000);
    expect(await readLauncherState(app.stateDir)).toBe("ready");

    const oldTarget = await readStateProcess(join(app.stateDir, "target.json"));
    const oldHost = await readStateProcess(join(app.stateDir, "host.json"));
    const oldMonitor = await readStateProcess(join(app.stateDir, "launcher.lock"), "monitorPid", "monitorStartedAt");
    if (!oldTarget || !oldHost || !oldMonitor) {
      throw new Error("first launcher did not record its target, host, and monitor identities");
    }
    targetProcesses.push(oldTarget);
    hostProcesses.push(oldHost);
    monitorProcesses.push(oldMonitor);

    await signalRecordedProcess(oldMonitor, "SIGSTOP");
    await waitUntil(() => processIsStopped(oldMonitor.pid), 3_000);
    await writeFile(app.stopFile, "stop\n");
    await waitUntil(() => first.finished(), 8_000);
    await first.child.exited;
    await first.outputDone;
    expect(await readLauncherState(app.stateDir)).toBe("stopped");
    await rm(app.stopFile, { force: true });
    expect(await readStateProcess(join(app.stateDir, "target.json"))).toEqual(oldTarget);
    expect(await readStateProcess(join(app.stateDir, "host.json"))).toEqual(oldHost);
    expect(isRecordedProcessAlive(oldTarget)).toBe(false);
    expect(existsSync(join(app.stateDir, "target.json"))).toBe(true);
    expect(existsSync(join(app.stateDir, "host.json"))).toBe(true);
    expect(existsSync(join(app.stateDir, "monitor-stop"))).toBe(true);
    await signalRecordedProcess(oldMonitor, "SIGKILL");
    await waitUntil(() => !isRecordedProcessAlive(oldMonitor), 8_000);
    await signalRecordedProcess(oldHost);
    await waitUntil(() => !isRecordedProcessAlive(oldHost), 8_000);

    const second = startInteractive(launcherCommand(app), app.env);
    launchers.push(second);
    await waitUntil(async () => prompt.test(capturedOutput(second))
      || (await readLaunches(app!.launchLog)).length >= 2
      || second.finished(), 10_000);
    expect(prompt.test(capturedOutput(second))).toBe(false);
    await waitUntil(async () => (await readLaunches(app!.launchLog)).length === 2, 10_000);
    await waitUntil(async () => {
      const state = await readLauncherState(app!.stateDir);
      return state === "ready" || state === "error";
    }, 15_000);
    expect(await readLauncherState(app.stateDir)).toBe("ready");

    const currentTarget = await readStateProcess(join(app.stateDir, "target.json"));
    const currentHost = await readStateProcess(join(app.stateDir, "host.json"));
    const currentMonitor = await readStateProcess(join(app.stateDir, "launcher.lock"), "monitorPid", "monitorStartedAt");
    if (!currentTarget || !currentHost || !currentMonitor) {
      throw new Error("second launcher did not record its target, host, and monitor identities");
    }
    targetProcesses.push(currentTarget);
    hostProcesses.push(currentHost);
    monitorProcesses.push(currentMonitor);
    expect(isRecordedProcessAlive(currentTarget)).toBe(true);
    expect(isRecordedProcessAlive(currentHost)).toBe(true);
    expect(isRecordedProcessAlive(currentMonitor)).toBe(true);

    second.child.kill("SIGKILL");
    await second.child.exited;
    await second.outputDone;
    expect(isRecordedProcessAlive(currentMonitor)).toBe(true);
    await signalRecordedProcess(currentHost);
    await waitUntil(() => !isRecordedProcessAlive(currentHost), 8_000);
    await waitUntil(async () => (await readLauncherState(app!.stateDir)) === "error", 8_000);
    const status = JSON.parse(await readFile(join(app.stateDir, "status.json"), "utf8")) as {
      state?: unknown;
      message?: unknown;
    };
    expect(status.state).toBe("error");
    expect(status.message).toEqual(expect.stringMatching(/CDP remains enabled/i));
    expect(isRecordedProcessAlive(currentTarget)).toBe(true);
    await waitUntil(() => !isRecordedProcessAlive(currentMonitor), 15_000);
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    for (const identity of hostProcesses) await signalRecordedProcess(identity);
    for (const identity of monitorProcesses) await signalRecordedProcess(identity, "SIGKILL");
    for (const identity of targetProcesses) await signalRecordedProcess(identity);
    for (const launcher of launchers) {
      if (processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    }
    await Promise.all(launchers.map(async (launcher) => {
      await launcher.child.exited;
      await launcher.outputDone;
    }));
    for (const identity of [...targetProcesses, ...hostProcesses, ...monitorProcesses]) {
      await waitUntil(() => !isRecordedProcessAlive(identity), 8_000).catch(() => {});
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);

test("a declined CDP prompt is not remembered as accepted consent", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-consent-refused-"));
  let app: FixtureApp | undefined;
  const launchers: CapturedProcess[] = [];
  const prompt = /继续启用 CDP？\[y\/N\]/;
  try {
    app = await makeFixtureApp(directory);
    const first = startInteractive(launcherCommand(app), app.env);
    launchers.push(first);
    await outputUntil(first, prompt);
    first.sendInput("n\n");
    await first.child.exited;
    await first.outputDone;
    expect(await readLaunches(app.launchLog)).toHaveLength(0);

    const second = startInteractive(launcherCommand(app), app.env);
    launchers.push(second);
    await waitUntil(async () => prompt.test(capturedOutput(second))
      || (await readLaunches(app!.launchLog)).length > 0
      || second.finished(), 10_000);
    expect(prompt.test(capturedOutput(second))).toBe(true);
    expect(await readLaunches(app.launchLog)).toHaveLength(0);
    second.sendInput("n\n");
    await second.child.exited;
    await second.outputDone;
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    for (const launcher of launchers) {
      if (processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 25_000);

test("concurrent launcher entries create only one OctoBuddy process", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-single-"));
  let app: FixtureApp | undefined;
  const launchers: CapturedProcess[] = [];
  try {
    app = await makeFixtureApp(directory);
    const prompt = /CDP|调试端口|远程调试/i;
    const first = startInteractive(launcherCommand(app), app.env);
    const second = startInteractive(launcherCommand(app), app.env);
    launchers.push(first, second);
    if (await promptOrExit(first, prompt)) first.sendInput("y\n");
    if (await promptOrExit(second, prompt)) second.sendInput("y\n");

    await waitUntil(async () => (await readLaunches(app!.launchLog)).length > 0);
    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    const launchedApp = launches[0]!;
    expect(launchedApp.port).toBeGreaterThan(0);
    await Promise.race([first.child.exited, second.child.exited]);
    expect(await readLaunches(app.launchLog)).toHaveLength(1);
    expect(processIsAlive(launchedApp.pid)).toBe(true);
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    for (const launcher of launchers) {
      if (processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 20_000);

test("concurrent launchers cannot both reclaim the same stale lock", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-stale-lock-race-"));
  let app: FixtureApp | undefined;
  let gateDirectory: string | undefined;
  const launchers: CapturedProcess[] = [];
  try {
    app = await makeFixtureApp(directory);
    gateDirectory = join(directory, "lock-unlink-gates");
    await mkdir(app.stateDir, { recursive: true, mode: 0o700 });
    await mkdir(gateDirectory);
    const lockFile = join(app.stateDir, "launcher.lock");
    await writeFile(lockFile, JSON.stringify({
      controllerPid: 2_147_483_647,
      controllerStartedAt: "previous-controller-generation",
    }), { encoding: "utf8", mode: 0o600 });

    const first = startLockRaceLauncher(app, "first", gateDirectory);
    const second = startLockRaceLauncher(app, "second", gateDirectory);
    launchers.push(first, second);
    const roles = ["first", "second"] as const;
    const waitingPath = (role: string): string => join(gateDirectory!, `${role}.unlink-waiting`);
    const prompt = /CDP|调试端口|远程调试/i;
    await waitUntil(() => roles.some((role) => existsSync(waitingPath(role)))
      || prompt.test(capturedOutput(first))
      || prompt.test(capturedOutput(second))
      || (first.finished() && second.finished()), 10_000);

    let leaderRole = roles.find((role) => existsSync(waitingPath(role)))
      ?? (prompt.test(capturedOutput(first)) ? "first" : undefined)
      ?? (prompt.test(capturedOutput(second)) ? "second" : undefined);
    if (!leaderRole && first.finished() && second.finished()) {
      await Promise.all([first.outputDone, second.outputDone]);
    }
    if (!leaderRole) throw new Error(`neither launcher reached its consent prompt\n${capturedOutput(first)}\n${capturedOutput(second)}`);

    const leader = leaderRole === "first" ? first : second;
    const contender = leaderRole === "first" ? second : first;
    if (existsSync(waitingPath(leaderRole))) {
      await writeFile(join(gateDirectory, `${leaderRole}.unlink-release`), "release\n");
    }
    await outputUntil(leader, prompt);
    leader.sendInput("y\n");
    await waitUntil(async () => (await readLaunches(app!.launchLog)).length === 1, 15_000);

    for (const role of roles) {
      await writeFile(join(gateDirectory, `${role}.unlink-release`), "release\n");
    }
    await waitUntil(async () => contender.finished()
      || (await readLaunches(app!.launchLog)).length > 1
      || prompt.test(capturedOutput(contender)), 10_000).catch(() => {});
    if (prompt.test(capturedOutput(contender))) contender.sendInput("y\n");
    await waitUntil(async () => contender.finished()
      || (await readLaunches(app!.launchLog)).length > 1, 10_000).catch(() => {});

    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    expect(processIsAlive(launches[0]!.pid)).toBe(true);
  } finally {
    if (gateDirectory) {
      await mkdir(gateDirectory, { recursive: true });
      for (const role of ["first", "second"]) {
        await writeFile(join(gateDirectory, `${role}.unlink-release`), "release\n").catch(() => {});
      }
    }
    for (const launcher of launchers) {
      if (processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    }
    await Promise.all(launchers.map(async (launcher) => {
      await launcher.child.exited;
      await launcher.outputDone;
    }));
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    await rm(directory, { recursive: true, force: true });
  }
}, 45_000);


test("Finder launch passes .envrc status values only to the enhancer host", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-status-env-"));
  let app: FixtureApp | undefined;
  let launcher: CapturedProcess | undefined;
  const hosts: RecordedProcessIdentity[] = [];
  const monitors: RecordedProcessIdentity[] = [];
  try {
    const packageRoot = await makeStatusEnvironmentPackage(
      directory,
      statusEnvrc(envrcStatusBaseUrl, envrcStatusToken),
    );
    app = await makeFixtureApp(directory);
    await preAcceptCdpConsent(app.stateDir);
    // The inherited environment supplies only the token; the private .envrc supplies the base URL.
    const launcherEnvironment = withoutFixtureStatusEnvironment(app.env);
    launcherEnvironment.OCTOBUDDY_HTTP_TOKEN = inheritedStatusToken;
    launcher = startInteractive([
      process.execPath,
      join(packageRoot, "src", "cli.ts"),
      "--dock",
      "--app-path", app.appPath,
      "--state-dir", app.stateDir,
    ], launcherEnvironment);

    await waitUntil(async () => {
      const state = await readLauncherState(app!.stateDir);
      return state === "ready" || state === "error";
    }, 20_000);
    expect(await readLauncherState(app.stateDir)).toBe("ready");

    const host = await readStateProcess(join(app.stateDir, "host.json"));
    if (host) hosts.push(host);
    const monitor = await readStateProcess(join(app.stateDir, "launcher.lock"), "monitorPid", "monitorStartedAt");
    if (monitor) monitors.push(monitor);
    if (!host) throw new Error("launcher did not record its enhancer host identity");

    const hostEnvironment = readProcessEnvironment(host.pid, [
      "OCTOBUDDY_HTTP_BASE_URL",
      "OCTOBUDDY_HTTP_TOKEN",
    ]);
    expect(hostEnvironment.OCTOBUDDY_HTTP_BASE_URL).toBe(envrcStatusBaseUrl);
    expect(hostEnvironment.OCTOBUDDY_HTTP_TOKEN).toBe(inheritedStatusToken);

    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    expect(launches[0]!.statusEnvironment).toEqual({ baseUrlPresent: false, tokenPresent: false });
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    if (launcher && processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    if (launcher) {
      await launcher.child.exited;
      await launcher.outputDone;
    }
    for (const identity of hosts) await signalRecordedProcess(identity);
    for (const identity of monitors) await signalRecordedProcess(identity, "SIGKILL");
    for (const identity of [...hosts, ...monitors]) {
      await waitUntil(() => !isRecordedProcessAlive(identity), 8_000).catch(() => {});
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);

test("Finder launch evaluates .envrc with standard shell expansion only for the enhancer host", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-status-env-shell-"));
  let app: FixtureApp | undefined;
  let launcher: CapturedProcess | undefined;
  const hosts: RecordedProcessIdentity[] = [];
  const monitors: RecordedProcessIdentity[] = [];
  try {
    const envrc = [
      "export OCTOBUDDY_STATUS_TEST_HOST='127.0.0.1'",
      "export OCTOBUDDY_HTTP_BASE_URL=\"http://${OCTOBUDDY_STATUS_TEST_HOST}:${OCTOBUDDY_STATUS_TEST_PORT:-8765}\"",
      "export OCTOBUDDY_STATUS_TOKEN_HEAD='00112233445566778899aabbccddeeff'",
      "export OCTOBUDDY_HTTP_TOKEN=\"$(printf '%s%s' \"$OCTOBUDDY_STATUS_TOKEN_HEAD\" '00112233445566778899aabbccddeeff')\"",
    ].join("\n");
    const packageRoot = await makeStatusEnvironmentPackage(directory, envrc);
    app = await makeFixtureApp(directory);
    await preAcceptCdpConsent(app.stateDir);
    launcher = startInteractive([
      process.execPath,
      join(packageRoot, "src", "cli.ts"),
      "--dock",
      "--app-path", app.appPath,
      "--state-dir", app.stateDir,
    ], withoutFixtureStatusEnvironment(app.env));

    await waitUntil(async () => {
      const state = await readLauncherState(app!.stateDir);
      return state === "ready" || state === "error";
    }, 20_000);
    expect(await readLauncherState(app.stateDir)).toBe("ready");

    const host = await readStateProcess(join(app.stateDir, "host.json"));
    if (host) hosts.push(host);
    const monitor = await readStateProcess(join(app.stateDir, "launcher.lock"), "monitorPid", "monitorStartedAt");
    if (monitor) monitors.push(monitor);
    if (!host) throw new Error("launcher did not record its enhancer host identity");

    const hostEnvironment = readProcessEnvironment(host.pid, [
      "OCTOBUDDY_HTTP_BASE_URL",
      "OCTOBUDDY_HTTP_TOKEN",
    ]);
    expect(hostEnvironment.OCTOBUDDY_HTTP_BASE_URL).toBe(envrcStatusBaseUrl);
    expect(hostEnvironment.OCTOBUDDY_HTTP_TOKEN).toBe(envrcStatusToken);
    expect(`${launcher.output().stdout}\n${launcher.output().stderr}`).not.toContain(envrcStatusToken);

    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    expect(launches[0]!.statusEnvironment).toEqual({ baseUrlPresent: false, tokenPresent: false });
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    if (launcher && processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    if (launcher) {
      await launcher.child.exited;
      await launcher.outputDone;
    }
    for (const identity of hosts) await signalRecordedProcess(identity);
    for (const identity of monitors) await signalRecordedProcess(identity, "SIGKILL");
    for (const identity of [...hosts, ...monitors]) {
      await waitUntil(() => !isRecordedProcessAlive(identity), 8_000).catch(() => {});
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);

test("fails with a generic error when the private .envrc cannot be evaluated", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-status-env-failure-"));
  try {
    const envrcPath = join(directory, ".envrc");
    await writeFile(envrcPath, [
      "export OCTOBUDDY_HTTP_BASE_URL='http://127.0.0.1:8765'",
      `export OCTOBUDDY_HTTP_TOKEN='${envrcStatusToken}'`,
      'echo "$OCTOBUDDY_HTTP_TOKEN"',
      'echo "$OCTOBUDDY_HTTP_TOKEN" >&2',
      "false",
    ].join("\n"), { encoding: "utf8", mode: 0o600 });
    await chmod(envrcPath, 0o600);

    let message = "";
    try {
      await loadStatusEnvironment({ envrcPath, env: {} });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("status environment file could not be evaluated");
    expect(message).not.toContain(envrcStatusToken);
    expect(message).not.toContain("http://127.0.0.1:8765");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 10_000);

test("CLI launch loads .envrc status values from the isolated package into the host", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octobuddy-launcher-status-env-cli-"));
  let app: FixtureApp | undefined;
  let launcher: CapturedProcess | undefined;
  const hosts: RecordedProcessIdentity[] = [];
  const monitors: RecordedProcessIdentity[] = [];
  try {
    const packageRoot = await makeStatusEnvironmentPackage(
      directory,
      statusEnvrc(envrcStatusBaseUrl, envrcStatusToken),
    );
    app = await makeFixtureApp(directory);
    await preAcceptCdpConsent(app.stateDir);
    launcher = startInteractive([
      process.execPath,
      join(packageRoot, "src", "cli.ts"),
      "--app-path", app.appPath,
      "--state-dir", app.stateDir,
    ], withoutFixtureStatusEnvironment(app.env));

    await waitUntil(async () => {
      const state = await readLauncherState(app!.stateDir);
      return state === "ready" || state === "error";
    }, 20_000);
    expect(await readLauncherState(app.stateDir)).toBe("ready");

    const host = await readStateProcess(join(app.stateDir, "host.json"));
    if (host) hosts.push(host);
    const monitor = await readStateProcess(join(app.stateDir, "launcher.lock"), "monitorPid", "monitorStartedAt");
    if (monitor) monitors.push(monitor);
    if (!host) throw new Error("launcher did not record its enhancer host identity");

    const hostEnvironment = readProcessEnvironment(host.pid, [
      "OCTOBUDDY_HTTP_BASE_URL",
      "OCTOBUDDY_HTTP_TOKEN",
    ]);
    expect(hostEnvironment.OCTOBUDDY_HTTP_BASE_URL).toBe(envrcStatusBaseUrl);
    expect(hostEnvironment.OCTOBUDDY_HTTP_TOKEN).toBe(envrcStatusToken);

    const launches = await readLaunches(app.launchLog);
    expect(launches).toHaveLength(1);
    expect(launches[0]!.statusEnvironment).toEqual({ baseUrlPresent: false, tokenPresent: false });
  } finally {
    if (app) await stopFixtureApp(app, (await readLaunches(app.launchLog)).map((launch) => launch.pid));
    if (launcher && processIsAlive(launcher.child.pid)) launcher.child.kill("SIGTERM");
    if (launcher) {
      await launcher.child.exited;
      await launcher.outputDone;
    }
    for (const identity of hosts) await signalRecordedProcess(identity);
    for (const identity of monitors) await signalRecordedProcess(identity, "SIGKILL");
    for (const identity of [...hosts, ...monitors]) {
      await waitUntil(() => !isRecordedProcessAlive(identity), 8_000).catch(() => {});
    }
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);
