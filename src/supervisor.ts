/**
 * Local supervisor detection shared by the `oco` CLI and the channel's restart path.
 *
 * Which supervisor runs this checkout — and under what program name — is a property of the
 * machine, not of the channel configuration, so it is detected instead of configured. Every
 * judgement here is parse-and-match over the supervisor's own CLI output; the process calls are
 * injected, so detection is testable without a real daemon.
 */
import { realpath } from "node:fs/promises";

/** A program as the supervisor describes it in its declarative export. */
export interface SupervisorProgram {
  readonly name: string;
  readonly command: string;
  readonly args: readonly string[];
  readonly cwd: string | undefined;
}

/** The detected supervisor and the program that hosts this checkout. */
export interface SupervisorTarget {
  readonly kind: "super";
  readonly program: string;
}

export interface SupervisorInfo {
  readonly state: string;
  readonly pid: number | undefined;
}

export interface SupervisorRunResult {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

export type SupervisorRun = (argv: readonly string[]) => Promise<SupervisorRunResult>;

export interface SupervisorClientOptions {
  /** Resolved supervisor executable. */
  readonly command: string;
  readonly run?: SupervisorRun;
  readonly timeoutMs?: number;
}

export interface SupervisorClient {
  /** Programs the supervisor currently declares. */
  listPrograms(): Promise<SupervisorProgram[]>;
  /** Live state of one program. */
  info(program: string): Promise<SupervisorInfo>;
  /**
   * Target that hosts `selfPid`, verified against each candidate's live PID. `undefined` means
   * this process is not supervised by a known supervisor — never a guess.
   */
  detectSelf(selfPid: number): Promise<SupervisorTarget | undefined>;
  /**
   * Target that belongs to `checkoutRoot`. Returns every match so the caller can refuse an
   * ambiguous machine instead of picking one.
   */
  detectCheckout(checkoutRoot: string): Promise<SupervisorTarget[]>;
  /** Foreground restart used by the CLI: waits for the program to become healthy. */
  restartArgv(target: SupervisorTarget, timeoutSeconds: number): string[];
  /** Detached restart used by the channel itself: the supervisor stops this process. */
  detachedRestartArgv(target: SupervisorTarget): string[];
}

const DEFAULT_RUN_TIMEOUT_MS = 10_000;
const CLI_HINTS = ["cli.ts", "/oco", " oco "];

/**
 * True when the program's command line reaches this project's CLI, either through the checkout
 * path, the linked `oco` bin, or the `cli.ts` entry point a supervisor stack names directly.
 */
function mentionsChannelCli(program: SupervisorProgram, checkoutRoot: string | undefined): boolean {
  const line = [program.command, ...program.args].join(" ");
  if (checkoutRoot !== undefined && line.includes(checkoutRoot)) return true;
  return CLI_HINTS.some((hint) => line.includes(hint));
}

async function normalizePath(path: string): Promise<string> {
  try {
    return await realpath(path);
  } catch {
    return path;
  }
}

export function parseSuperExport(value: unknown): SupervisorProgram[] {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("super export 输出不是 JSON 对象");
  }
  const services = (value as Record<string, unknown>).services;
  if (!Array.isArray(services)) throw new Error("super export 缺少 services 数组");
  const programs: SupervisorProgram[] = [];
  for (const entry of services) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const record = entry as Record<string, unknown>;
    const name = typeof record.name === "string" ? record.name.trim() : "";
    if (name.length === 0) continue;
    programs.push({
      name,
      command: typeof record.command === "string" ? record.command : "",
      args: Array.isArray(record.args)
        ? record.args.filter((argument): argument is string => typeof argument === "string")
        : [],
      cwd: typeof record.cwd === "string" && record.cwd.length > 0 ? record.cwd : undefined,
    });
  }
  return programs;
}

export function parseSuperInfo(text: string): SupervisorInfo | undefined {
  const state = /^State:\s*(\S+)/mu.exec(text)?.[1];
  const pidText = /^PID:\s*(\d+)/mu.exec(text)?.[1];
  if (state === undefined && pidText === undefined) return undefined;
  return {
    state: state ?? "unknown",
    pid: pidText === undefined ? undefined : Number(pidText),
  };
}

async function runSuperCommand(
  command: string,
  argv: readonly string[],
  timeoutMs: number,
): Promise<SupervisorRunResult> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const timeoutMessage = `${command} ${argv.join(" ")} 超时（${timeoutMs}ms）`;
  try {
    const child = Bun.spawn([command, ...argv], {
      stdout: "pipe",
      stderr: "pipe",
      stdin: "ignore",
      signal: controller.signal,
    });
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    // Aborting kills the child, which reports a signal exit code instead of an error.
    if (controller.signal.aborted) throw new Error(timeoutMessage);
    return { code, stdout, stderr };
  } catch (error) {
    if (controller.signal.aborted) throw new Error(timeoutMessage);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export function createSupervisorClient(options: SupervisorClientOptions): SupervisorClient {
  const timeoutMs = options.timeoutMs ?? DEFAULT_RUN_TIMEOUT_MS;
  const run = options.run ?? ((argv: readonly string[]) => runSuperCommand(options.command, argv, timeoutMs));

  async function infallible(argv: readonly string[]): Promise<SupervisorRunResult> {
    const result = await run(argv);
    if (result.code !== 0) {
      const detail = result.stderr.trim() || result.stdout.trim() || `退出码 ${result.code}`;
      throw new Error(`${options.command} ${argv.join(" ")} 失败：${detail}`);
    }
    return result;
  }

  return {
    async listPrograms(): Promise<SupervisorProgram[]> {
      const result = await infallible(["export", "--format", "json"]);
      let document: unknown;
      try {
        document = JSON.parse(result.stdout);
      } catch {
        throw new Error("super export 输出不是合法 JSON");
      }
      return parseSuperExport(document);
    },

    async info(program: string): Promise<SupervisorInfo> {
      const result = await infallible(["info", program]);
      const info = parseSuperInfo(result.stdout);
      if (!info) throw new Error(`无法解析 super info ${program} 的输出`);
      return info;
    },

    async detectSelf(selfPid: number): Promise<SupervisorTarget | undefined> {
      const programs = await this.listPrograms();
      for (const program of programs) {
        if (!mentionsChannelCli(program, undefined)) continue;
        let info: SupervisorInfo;
        try {
          info = await this.info(program.name);
        } catch {
          continue;
        }
        if (info.pid === selfPid) return { kind: "super", program: program.name };
      }
      return undefined;
    },

    async detectCheckout(checkoutRoot: string): Promise<SupervisorTarget[]> {
      const root = await normalizePath(checkoutRoot);
      const programs = await this.listPrograms();
      const matches: SupervisorTarget[] = [];
      for (const program of programs) {
        if (!mentionsChannelCli(program, root)) continue;
        if (program.cwd === undefined) continue;
        if (await normalizePath(program.cwd) !== root) continue;
        matches.push({ kind: "super", program: program.name });
      }
      return matches;
    },

    restartArgv(target: SupervisorTarget, timeoutSeconds: number): string[] {
      return [
        options.command,
        "-y",
        "restart",
        "--wait-healthy",
        "--timeout",
        String(timeoutSeconds),
        target.program,
      ];
    },

    detachedRestartArgv(target: SupervisorTarget): string[] {
      return [options.command, "-y", "restart", target.program];
    },
  };
}
