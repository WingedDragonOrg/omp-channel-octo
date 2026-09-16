#!/usr/bin/env bun
import { constants as fsConstants } from "node:fs";
import { access, mkdtemp, rm, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, normalize, resolve } from "node:path";
import { createOctoTransport } from "./octo/index.ts";
import { createOctoCliCredentialProvisioner } from "./octo-cli.ts";
import { createWorkerFactory } from "./omp/index.ts";
import {
  DEFAULT_CONFIG_PATH,
  ensureStateDirectory,
  loadConfig,
  ocoLogsDir,
  resolveConfigPath,
  toCoreConfig,
} from "./config.ts";
import { createAllowlistWriteback } from "./config-writeback.ts";
import {
  DEFAULT_LAUNCHD_LABEL,
  DEFAULT_LAUNCHD_PLIST_PATH,
  installLaunchdPlist,
  uninstallLaunchdPlist,
} from "./launchd.ts";
import {
  DEFAULT_LOG_TAIL_LINES,
  ensureLogsDirectory,
  followLog,
  installLogTee,
  newestLogFile,
  pruneLogFiles,
  readLogTail,
  type LogTee,
} from "./logs.ts";
import { createSupervisorClient, type SupervisorClient, type SupervisorTarget } from "./supervisor.ts";
import { createChannelService } from "./service.ts";
import { createOmpWorktreePort } from "./worktree.ts";
import type { ChannelService, OmpWorker } from "./contracts.ts";

/** The checkout this CLI runs from; supervisor detection is scoped to it. */
const CHECKOUT_ROOT = dirname(import.meta.dir);
const PROGRAM_NAMES: Record<string, true> = { oco: true, "omp-channel-octo": true };

/**
 * Bun resolves the symlink before the script sees `argv`, so the invoked name only survives in
 * the shell's `_`. Any other value (a direct `bun run src/cli.ts`, the launcher, a supervisor
 * stack) falls back to the short name.
 */
function programName(): string {
  const invoked = basename(process.env._ ?? "");
  return PROGRAM_NAMES[invoked] === true ? invoked : "oco";
}

function helpText(name: string): string {
  return `用法：
  ${name} start [--config PATH] [--omp-command PATH]      前台运行（守护进程托管）
  ${name} run [--config PATH] [--omp-command PATH]        调试运行（不探测守护进程）
  ${name} check [--config PATH]                           校验配置与 fresh worker
  ${name} restart [--program NAME] [--timeout SECONDS]    经守护进程重启
  ${name} status                                          守护进程状态与路径
  ${name} logs [--tail N] [--follow|-f]                   服务日志
  ${name} test --card                                     发送真实预览卡
  ${name} launchd install|uninstall [--plist PATH]        launchd plist

配置 ${resolveConfigPath(DEFAULT_CONFIG_PATH)} · 日志 ${ocoLogsDir()} · 凭证 botToken/OCTO_BOT_TOKEN
`;
}

const TEST_USAGE = "用法：test --card；目前只有预览卡验收测试。";

interface ParsedArgs {
  readonly values: ReadonlyMap<string, string>;
  readonly flags: ReadonlySet<string>;
}

/** Strict option parsing: unknown flags and stray positionals are refused, not ignored. */
function parseArgs(
  args: readonly string[],
  options: { values?: readonly string[]; flags?: readonly string[] } = {},
): ParsedArgs {
  const valueNames = new Set(options.values ?? []);
  const flagNames = new Set(options.flags ?? []);
  const values = new Map<string, string>();
  const flags = new Set<string>();
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]!;
    if (argument === "--help" || argument === "-h") continue;
    if (!argument.startsWith("-")) throw new Error(`不支持的参数：${argument}`);
    const equals = argument.indexOf("=");
    const flag = equals >= 0 ? argument.slice(0, equals) : argument;
    if (flagNames.has(flag)) {
      if (equals >= 0) throw new Error(`${flag} 不接受取值`);
      flags.add(flag);
      continue;
    }
    if (!valueNames.has(flag)) throw new Error(`不支持的参数：${argument}`);
    if (values.has(flag)) throw new Error(`${flag} 只能指定一次`);
    const inline = equals >= 0 ? argument.slice(equals + 1) : undefined;
    const value = inline ?? args[index + 1];
    if (value === undefined || value.length === 0 || (inline === undefined && value.startsWith("-"))) {
      throw new Error(`${flag} 需要参数`);
    }
    if (inline === undefined) index += 1;
    values.set(flag, value);
  }
  return { values, flags };
}

function positiveInteger(value: string | undefined, flag: string): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${flag} 必须是正整数`);
  return parsed;
}

function resolvePath(path: string): string {
  if (path === "~") return homedir();
  if (path.startsWith("~/")) return normalize(join(homedir(), path.slice(2)));
  if (path.startsWith("~")) throw new Error("路径不支持其他用户的 ~name");
  return isAbsolute(path) ? normalize(path) : resolve(path);
}

async function isExecutable(path: string): Promise<boolean> {
  try {
    const info = await stat(path);
    if (!info.isFile()) return false;
    await access(path, fsConstants.X_OK);
    return true;
  } catch {
    return false;
  }
}

export async function resolveExecutable(command: string, environment: NodeJS.ProcessEnv = process.env): Promise<string> {
  if (typeof command !== "string" || command.trim().length === 0 || command.includes("\u0000")) {
    throw new Error("可执行文件配置无效");
  }
  const requested = command.trim();
  const candidates: string[] = [];
  if (isAbsolute(requested) || requested.includes("/")) {
    candidates.push(isAbsolute(requested) ? requested : resolve(requested));
  } else {
    for (const directory of (environment.PATH ?? "").split(":").filter((entry) => entry.length > 0)) {
      candidates.push(join(directory, requested));
    }
  }
  for (const candidate of candidates) {
    if (await isExecutable(candidate)) return normalize(candidate);
  }
  throw new Error(`可执行文件不可用：${requested}`);
}

/** Supervisor client for the CLI commands; missing `super` is reported, never guessed around. */
async function supervisorClient(): Promise<SupervisorClient> {
  let command: string;
  try {
    command = await resolveExecutable("super");
  } catch {
    throw new Error("未找到 super 可执行文件：restart/status 需要本机的 Super 守护进程");
  }
  return createSupervisorClient({ command });
}

/** Best-effort detection for the running service: no supervisor must never block startup. */
async function detectSelfTarget(): Promise<{ supervisor: SupervisorClient; target: SupervisorTarget } | undefined> {
  try {
    const supervisor = await supervisorClient();
    const target = await supervisor.detectSelf(process.pid);
    return target === undefined ? undefined : { supervisor, target };
  } catch (error) {
    console.error(`[restart] 守护进程探测失败：${error instanceof Error ? error.message : String(error)}`);
    return undefined;
  }
}

const UNCONFIGURED_OWNER_NOTICE =
  "配置未提供 owner；若状态中尚无持久 owner，将等待首次有效私聊绑定。";

async function runCheck(configPath: string | undefined): Promise<void> {
  const config = await loadConfig(configPath);
  await ensureStateDirectory(config);
  const ompCommand = await resolveExecutable(config.ompCommand);
  const transport = createOctoTransport({
    apiUrl: config.apiUrl,
    ...(config.wsUrl === undefined ? {} : { wsUrl: config.wsUrl }),
    botToken: config.botToken,
    logger: (message) => console.error(message),
  });
  const workerFactory = createWorkerFactory({
    ompCommand,
    extraArgs: config.ompExtraArgs,
    ...(config.startupTimeoutMs === undefined ? {} : { startupTimeoutMs: config.startupTimeoutMs }),
    ...(config.cancelTimeoutMs === undefined ? {} : { cancelTimeoutMs: config.cancelTimeoutMs }),
  });
  let worker: OmpWorker | undefined;
  let checkDirectory: string | undefined;
  try {
    const identity = await transport.initialize();
    if (identity.uid.trim().length === 0 || identity.name.trim().length === 0) {
      throw new Error("Octo User Bot 身份无效");
    }
    checkDirectory = await mkdtemp(join(config.stateDir, ".check-"));
    worker = await workerFactory.create({
      cwd: checkDirectory,
      sessionDir: checkDirectory,
      onSpawn: () => {},
    });
    const stopped = await worker.stop();
    if (!stopped) throw new Error("OMP RPC worker 未能可靠停止");
    worker = undefined;
    console.log("检查通过：Octo User Bot 注册成功，OMP RPC fresh worker 已启动并停止（未调用模型）。");
    if (config.ownerId === undefined) console.log(UNCONFIGURED_OWNER_NOTICE);
  } finally {
    if (worker) await worker.stop().catch(() => false);
    if (checkDirectory) await rm(checkDirectory, { recursive: true, force: true }).catch(() => {});
    await transport.stop().catch(() => {});
  }
}

const RESTART_WATCHDOG_MS = 15_000;

type ServiceMode = "start" | "run";

/**
 * The channel process itself. `start` is what a supervisor hosts: it detects the daemon that
 * runs it and exposes the restart port, so a turn can make its own edits live. `run` is the
 * debugging entry point and stays out of the daemon's way.
 */
async function runService(
  configPath: string | undefined,
  ompCommandOverride: string | undefined,
  mode: ServiceMode,
): Promise<void> {
  const config = await loadConfig(configPath);
  await ensureStateDirectory(config);
  const ompCommand = await resolveExecutable(ompCommandOverride ?? config.ompCommand);
  let logTee: LogTee | undefined;
  try {
    await ensureLogsDirectory();
    logTee = installLogTee();
    const removed = await pruneLogFiles();
    if (removed.length > 0) console.log(`[logs] 已清理 ${removed.length} 个超过保留期的日志文件`);
  } catch (error) {
    console.error(`[logs] 无法准备日志目录：${error instanceof Error ? error.message : String(error)}`);
  }
  const transport = createOctoTransport({
    apiUrl: config.apiUrl,
    ...(config.wsUrl === undefined ? {} : { wsUrl: config.wsUrl }),
    botToken: config.botToken,
    logger: (message) => console.error(message),
  });
  const cliCredentials = createOctoCliCredentialProvisioner({
    botToken: config.botToken,
    apiUrl: config.apiUrl,
    ...(config.homeDir === undefined ? {} : { homeDir: config.homeDir }),
    logger: (message) => console.error(message),
  });
  const workerFactory = createWorkerFactory({
    ompCommand,
    extraArgs: config.ompExtraArgs,
    ...(config.startupTimeoutMs === undefined ? {} : { startupTimeoutMs: config.startupTimeoutMs }),
    ...(config.cancelTimeoutMs === undefined ? {} : { cancelTimeoutMs: config.cancelTimeoutMs }),
    cliEnv: () => cliCredentials.env(),
  });
  const supervised = mode === "start" ? await detectSelfTarget() : undefined;
  if (mode === "run") {
    console.log("[run] 调试运行：不探测守护进程，/restart 与 channel_control.restart 不可用。");
  } else if (supervised) {
    console.log(`[restart] 守护进程程序：${supervised.target.program}（super）`);
  } else {
    console.log("[restart] 未检测到守护进程：/restart 与 channel_control.restart 会明确拒绝。");
  }
  let service: ChannelService | undefined;
  let startPromise: Promise<void> | undefined;
  const shutdown = Promise.withResolvers<void>();
  let shutdownRequested = false;
  const onSignal = () => {
    if (shutdownRequested) return;
    shutdownRequested = true;
    shutdown.resolve();
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  try {
    service = await createChannelService({
      config: toCoreConfig(config),
      transport,
      workerFactory,
      worktrees: createOmpWorktreePort({ command: ompCommand }),
      cliCredentials,
      // Only a config that came from a file can be written back; a hand-built config has no path.
      ...(config.configPath === undefined
        ? {}
        : { allowlistWriteback: createAllowlistWriteback(config.configPath) }),
      ...(supervised === undefined
        ? {}
        : {
            restart: {
              request: async (): Promise<void> => {
                try {
                  const argv = supervised.supervisor.detachedRestartArgv(supervised.target);
                  const child = Bun.spawn(argv, {
                    cwd: process.cwd(),
                    env: process.env,
                    detached: true,
                    stdio: ["ignore", "ignore", "inherit"],
                  });
                  child.unref();
                  console.error(`[restart] 已执行 ${argv.join(" ")}（pid ${child.pid}）`);
                  // The supervisor stops this process (it sends SIGTERM); warn when the command
                  // returns without doing so instead of tearing the service down unmanaged.
                  const watchdog = setTimeout(() => {
                    if (!shutdownRequested) {
                      console.error(
                        `[restart] ${argv.join(" ")} 未停止本进程，channel 继续运行；如需强制退出请发送 SIGTERM。`,
                      );
                    }
                  }, RESTART_WATCHDOG_MS);
                  watchdog.unref();
                } catch (error) {
                  console.error(
                    `[restart] 重启命令启动失败：${error instanceof Error ? error.message : String(error)}`,
                  );
                }
              },
            },
          }),
    });
    startPromise = service.start();
    const first = await Promise.race([
      startPromise.then(() => "started" as const),
      shutdown.promise.then(() => "shutdown" as const),
    ]);
    if (first === "shutdown") {
      await service.stop();
      await startPromise.catch(() => {});
      return;
    }
    if (config.ownerId === undefined) console.log(UNCONFIGURED_OWNER_NOTICE);
    await shutdown.promise;
  } finally {
    try {
      if (service) {
        await service.stop();
      } else {
        await transport.stop().catch(() => {});
      }
    } finally {
      process.removeListener("SIGINT", onSignal);
      process.removeListener("SIGTERM", onSignal);
      if (logTee) await logTee.close().catch(() => {});
    }
  }
}

async function runRestart(programFlag: string | undefined, timeoutSeconds: number | undefined): Promise<void> {
  const supervisor = await supervisorClient();
  let target: SupervisorTarget;
  if (programFlag !== undefined) {
    target = { kind: "super", program: programFlag };
  } else {
    const matches = await supervisor.detectCheckout(CHECKOUT_ROOT);
    if (matches.length === 0) {
      throw new Error(`未检测到守护进程程序：确认 channel 由 Super 托管，或用 --program 指定程序名`);
    }
    if (matches.length > 1) {
      throw new Error(
        `检测到多个候选程序：${matches.map((entry) => entry.program).join("、")}；请用 --program 指定`,
      );
    }
    target = matches[0]!;
  }
  const timeout = timeoutSeconds ?? 60;
  const before = await supervisor.info(target.program).catch(() => undefined);
  const child = Bun.spawn(supervisor.restartArgv(target, timeout), {
    stdout: "inherit",
    stderr: "inherit",
    stdin: "ignore",
  });
  const code = await child.exited;
  if (code !== 0) throw new Error(`重启失败：super 退出码 ${code}`);
  const after = await supervisor.info(target.program).catch(() => undefined);
  console.log(`守护进程：super（程序 ${target.program}）`);
  if (after) console.log(`状态：${after.state}`);
  if (before?.pid !== undefined || after?.pid !== undefined) {
    console.log(`PID：${before?.pid ?? "未知"} → ${after?.pid ?? "未知"}`);
  }
  if (before?.pid !== undefined && after?.pid !== undefined && before.pid === after.pid) {
    console.error(`警告：PID 未变化（${after.pid}），进程可能没有真正重启。`);
  }
}

async function processUptime(pid: number): Promise<string | undefined> {
  try {
    const child = Bun.spawn(["ps", "-o", "etime=", "-p", String(pid)], {
      stdout: "pipe",
      stderr: "ignore",
      stdin: "ignore",
    });
    const [code, text] = await Promise.all([child.exited, new Response(child.stdout).text()]);
    if (code !== 0) return undefined;
    const value = text.trim();
    return value.length === 0 ? undefined : value;
  } catch {
    return undefined;
  }
}

async function runStatus(): Promise<void> {
  const configPath = resolveConfigPath(DEFAULT_CONFIG_PATH);
  const logFile = await newestLogFile();
  try {
    const supervisor = await supervisorClient();
    const matches = await supervisor.detectCheckout(CHECKOUT_ROOT);
    const target = matches.length === 1 ? matches[0]! : undefined;
    if (target) {
      const info = await supervisor.info(target.program).catch(() => undefined);
      console.log(`守护进程：super（程序 ${target.program}）`);
      console.log(`状态：${info?.state ?? "未知"}`);
      if (info?.pid !== undefined) {
        const uptime = await processUptime(info.pid);
        console.log(`PID：${info.pid}${uptime === undefined ? "" : `（运行 ${uptime}）`}`);
      }
    } else if (matches.length > 1) {
      console.log(`守护进程：多个候选程序 ${matches.map((entry) => entry.program).join("、")}`);
    } else {
      console.log("守护进程：未检测到（前台运行或非 Super 托管）");
    }
  } catch (error) {
    console.log(`守护进程：未检测到（${error instanceof Error ? error.message : String(error)}）`);
  }
  console.log(`配置：${configPath}`);
  console.log(`日志：${logFile ?? `${ocoLogsDir()}（尚未产生日志）`}`);
}

async function runLogs(tail: number | undefined, follow: boolean): Promise<void> {
  const directory = ocoLogsDir();
  const file = await newestLogFile(directory);
  if (file === undefined) throw new Error(`尚未产生日志：${directory}`);
  const text = await readLogTail(file, tail ?? DEFAULT_LOG_TAIL_LINES);
  if (text.length > 0) process.stdout.write(`${text}\n`);
  if (!follow) return;
  const controller = new AbortController();
  const onSignal = () => controller.abort();
  process.on("SIGINT", onSignal);
  try {
    await followLog({ write: (chunk) => process.stdout.write(chunk), signal: controller.signal, directory });
  } finally {
    process.removeListener("SIGINT", onSignal);
  }
}

/**
 * The preview card can only be judged on the real client, so the acceptance test sends it. The
 * checkout's `.envrc` supplies the token and target, which makes the command work from a shell
 * that never loaded direnv.
 */
async function runTest(card: boolean): Promise<void> {
  if (!card) throw new Error(TEST_USAGE);
  const bun = await resolveExecutable("bun");
  const script = 'set -a; if [ -f ./.envrc ]; then . ./.envrc; fi; set +a; exec "$@"';
  const child = Bun.spawn(
    ["/bin/sh", "-c", script, "oco-test", bun, "test", join(CHECKOUT_ROOT, "tests", "preview-card.acceptance.ts")],
    { cwd: CHECKOUT_ROOT, stdout: "inherit", stderr: "inherit", stdin: "inherit" },
  );
  const code = await child.exited;
  if (code !== 0) process.exitCode = code;
}

async function runLaunchdInstall(configPath: string | undefined, plistPath: string | undefined): Promise<void> {
  const selectedConfigPath = resolveConfigPath(configPath ?? DEFAULT_CONFIG_PATH);
  const config = await loadConfig(selectedConfigPath);
  await ensureStateDirectory(config);
  const ompCommand = await resolveExecutable(config.ompCommand);
  const path = resolvePath(plistPath ?? DEFAULT_LAUNCHD_PLIST_PATH);
  const executable = resolve(process.execPath);
  const script = resolve(process.argv[1] ?? join(import.meta.dir, "cli.ts"));
  const logsDir = await ensureLogsDirectory();
  const environmentVariables = config.botTokenSource === "environment"
    ? { [config.botTokenEnv]: config.botToken }
    : undefined;
  await installLaunchdPlist(path, {
    label: DEFAULT_LAUNCHD_LABEL,
    programArguments: [
      executable,
      "run",
      script,
      "start",
      "--config",
      selectedConfigPath,
      "--omp-command",
      ompCommand,
    ],
    workingDirectory: dirname(selectedConfigPath),
    stdoutPath: join(logsDir, "launchd.out.log"),
    stderrPath: join(logsDir, "launchd.err.log"),
    ...(environmentVariables === undefined ? {} : { environmentVariables }),
    runAtLoad: true,
    keepAlive: true,
  });
  console.log(`已写入 launchd plist：${path}`);
}

async function runLaunchdUninstall(plistPath: string | undefined): Promise<void> {
  const path = resolvePath(plistPath ?? DEFAULT_LAUNCHD_PLIST_PATH);
  await uninstallLaunchdPlist(path);
  console.log(`已删除 launchd plist（若存在）：${path}`);
}

export async function main(args: string[]): Promise<void> {
  const name = programName();
  const command = args[0] ?? "help";
  if (command === "help" || command === "--help" || command === "-h" || args.includes("--help") || args.includes("-h")) {
    console.log(helpText(name));
    return;
  }
  if (command === "check") {
    await runCheck(parseArgs(args.slice(1), { values: ["--config"] }).values.get("--config"));
    return;
  }
  if (command === "start" || command === "run") {
    const parsed = parseArgs(args.slice(1), { values: ["--config", "--omp-command"] });
    await runService(parsed.values.get("--config"), parsed.values.get("--omp-command"), command);
    return;
  }
  if (command === "restart") {
    const parsed = parseArgs(args.slice(1), { values: ["--program", "--timeout"] });
    await runRestart(parsed.values.get("--program"), positiveInteger(parsed.values.get("--timeout"), "--timeout"));
    return;
  }
  if (command === "status") {
    parseArgs(args.slice(1));
    await runStatus();
    return;
  }
  if (command === "logs") {
    const parsed = parseArgs(args.slice(1), { values: ["--tail"], flags: ["--follow", "-f"] });
    await runLogs(
      positiveInteger(parsed.values.get("--tail"), "--tail"),
      parsed.flags.has("--follow") || parsed.flags.has("-f"),
    );
    return;
  }
  if (command === "test") {
    await runTest(parseArgs(args.slice(1), { flags: ["--card"] }).flags.has("--card"));
    return;
  }
  if (command === "launchd") {
    const action = args[1];
    if (action === "install") {
      const parsed = parseArgs(args.slice(2), { values: ["--config", "--plist"] });
      await runLaunchdInstall(parsed.values.get("--config"), parsed.values.get("--plist"));
      return;
    }
    if (action === "uninstall") {
      const parsed = parseArgs(args.slice(2), { values: ["--plist"] });
      await runLaunchdUninstall(parsed.values.get("--plist"));
      return;
    }
  }
  throw new Error(`不支持的命令：${command}\n${helpText(name)}`);
}

if (import.meta.main) {
  await main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : "命令执行失败");
    process.exitCode = 1;
  });
}
