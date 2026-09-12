#!/usr/bin/env bun
import { constants as fsConstants } from "node:fs";
import { access, mkdtemp, rm, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, normalize, resolve } from "node:path";
import { createOctoTransport } from "./octo/index.ts";
import { createWorkerFactory } from "./omp/index.ts";
import {
  DEFAULT_API_URL,
  DEFAULT_CONFIG_PATH,
  ensureStateDirectory,
  loadConfig,
  resolveConfigPath,
  toCoreConfig,
} from "./config.ts";
import {
  DEFAULT_LAUNCHD_LABEL,
  DEFAULT_LAUNCHD_PLIST_PATH,
  installLaunchdPlist,
  uninstallLaunchdPlist,
} from "./launchd.ts";
import { createChannelService } from "./service.ts";
import type { ChannelService, OmpWorker } from "./contracts.ts";

const HELP = `用法：
  omp-channel-octo check [--config PATH]
  omp-channel-octo serve [--config PATH] [--omp-command PATH]
  omp-channel-octo launchd install [--config PATH] [--plist PATH]
  omp-channel-octo launchd uninstall [--plist PATH]

配置默认路径：${DEFAULT_CONFIG_PATH}
默认 API：${DEFAULT_API_URL}/；ownerId 可省略，未配置时首次有效私聊可能绑定 owner。
凭证可放在仅所有者可读的配置文件 botToken 中，或使用 botTokenEnv（默认 OCTO_BOT_TOKEN）。
launchd install 只写入本地 0600 plist，不调用 launchctl；环境凭证会安全写入该 plist 供 launchd 持久使用。
`;

type ParsedOption = {
  value: string | undefined;
  present: boolean;
};

function option(args: string[], flag: string): ParsedOption {
  let value: string | undefined;
  let present = false;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]!;
    if (argument === flag) {
      if (present) throw new Error(`${flag} 只能指定一次`);
      const next = args[index + 1];
      if (!next || next.startsWith("-")) throw new Error(`${flag} 需要参数`);
      present = true;
      value = next;
      index++;
      continue;
    }
    if (argument.startsWith(`${flag}=`)) {
      if (present) throw new Error(`${flag} 只能指定一次`);
      const next = argument.slice(flag.length + 1);
      if (next.length === 0) throw new Error(`${flag} 需要参数`);
      present = true;
      value = next;
    }
  }
  return { value, present };
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
    throw new Error("OMP 可执行文件配置无效");
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
  throw new Error("OMP 可执行文件不可用");
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

async function runServe(configPath: string | undefined, ompCommandOverride: string | undefined): Promise<void> {
  const config = await loadConfig(configPath);
  await ensureStateDirectory(config);
  const ompCommand = await resolveExecutable(ompCommandOverride ?? config.ompCommand);
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
    }
  }
}

async function runLaunchdInstall(configPath: string | undefined, plistPath: string | undefined): Promise<void> {
  const selectedConfigPath = resolveConfigPath(configPath ?? DEFAULT_CONFIG_PATH);
  const config = await loadConfig(selectedConfigPath);
  await ensureStateDirectory(config);
  const ompCommand = await resolveExecutable(config.ompCommand);
  const path = resolvePath(plistPath ?? DEFAULT_LAUNCHD_PLIST_PATH);
  const executable = resolve(process.execPath);
  const script = resolve(process.argv[1] ?? join(import.meta.dir, "cli.ts"));
  const environmentVariables = config.botTokenSource === "environment"
    ? { [config.botTokenEnv]: config.botToken }
    : undefined;
  await installLaunchdPlist(path, {
    label: DEFAULT_LAUNCHD_LABEL,
    programArguments: [
      executable,
      "run",
      script,
      "serve",
      "--config",
      selectedConfigPath,
      "--omp-command",
      ompCommand,
    ],
    workingDirectory: dirname(selectedConfigPath),
    stdoutPath: join(config.stateDir, "channel.stdout.log"),
    stderrPath: join(config.stateDir, "channel.stderr.log"),
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

function rejectUnknownOptions(args: string[], allowed: readonly string[]): void {
  const accepted = new Set(allowed);
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]!;
    if (argument === "--help" || argument === "-h") continue;
    if (!argument.startsWith("-")) {
      if (argument === "check" || argument === "serve" || argument === "start" || argument === "launchd") continue;
      if (argument === "install" || argument === "uninstall") continue;
      throw new Error(`不支持的参数：${argument}`);
    }
    const equalsIndex = argument.indexOf("=");
    const flag = equalsIndex >= 0 ? argument.slice(0, equalsIndex) : argument;
    if (!accepted.has(flag)) throw new Error(`不支持的参数：${argument}`);
    if (equalsIndex < 0) index++;
  }
}

export async function main(args: string[]): Promise<void> {
  const command = args[0] ?? "help";
  if (command === "help" || command === "--help" || command === "-h" || args.includes("--help") || args.includes("-h")) {
    console.log(HELP);
    return;
  }
  if (command === "check") {
    rejectUnknownOptions(args.slice(1), ["--config"]);
    await runCheck(option(args, "--config").value);
    return;
  }
  if (command === "serve" || command === "start") {
    rejectUnknownOptions(args.slice(1), ["--config", "--omp-command"]);
    await runServe(option(args, "--config").value, option(args, "--omp-command").value);
    return;
  }
  if (command === "launchd") {
    const action = args[1];
    if (action === "install") {
      rejectUnknownOptions(args.slice(2), ["--config", "--plist"]);
      await runLaunchdInstall(option(args, "--config").value, option(args, "--plist").value);
      return;
    }
    if (action === "uninstall") {
      rejectUnknownOptions(args.slice(2), ["--plist"]);
      await runLaunchdUninstall(option(args, "--plist").value);
      return;
    }
  }
  throw new Error(`不支持的命令：${command}\n${HELP}`);
}

if (import.meta.main) {
  await main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : "命令执行失败");
    process.exitCode = 1;
  });
}
