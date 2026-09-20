import { chmod, mkdir } from "node:fs/promises";
import { octoCliConfigDir } from "./config.ts";

/**
 * Per-bot octo-cli credential for OMP workers.
 *
 * OMP tasks run with a shell, so an agent that holds a bot credential can reach the whole Octo
 * Bot API through `octo-cli` — including endpoints this channel never wraps in a host tool. The
 * channel therefore stores this bot's token in its own octo-cli config directory and points the
 * worker at it: the agent acts as this bot, while the raw token never enters the worker
 * environment. One directory per bot keeps identities apart once a host serves several bots.
 *
 * Provisioning is best effort. A missing or failing `octo-cli` only means the agent gets no CLI
 * identity; it must never block startup or a task.
 */

const DEFAULT_COMMAND = "octo-cli";
/** `auth login` writes one small file; anything slower than this is broken, not busy. */
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_ERROR_CHARS = 200;
/** Profile name inside the per-bot store; the directory already isolates one bot per store. */
const PROFILE = "oco";

/** Environment names the OMP worker accepts from the credential store; see `workerEnv`. */
export const OCTO_CLI_ENV_ALLOWLIST: Record<string, true> = {
  OCTO_CONFIG_DIR: true,
  OCTO_API_BASE_URL: true,
  OCTO_BOT_ID: true,
};

export interface OctoCliCredentialOptions {
  /** Raw bot token; only ever written to the child's stdin. */
  botToken: string;
  /** Channel API base URL, handed to the agent's CLI as its base. */
  apiUrl: string;
  homeDir?: string;
  command?: string;
  timeoutMs?: number;
  logger?: (message: string) => void;
}

export interface OctoCliCredentialProvisioner {
  /**
   * Stores this bot's token in its own octo-cli config directory and publishes the worker
   * environment. Never throws: an unusable store leaves the agent without a CLI identity.
   */
  provision(botUid: string): Promise<void>;
  /** Environment additions for OMP workers; undefined until provisioning has succeeded. */
  env(): Record<string, string> | undefined;
}

function oneLine(value: string): string {
  const single = value.replace(/\s+/gu, " ").trim();
  return single.length > MAX_ERROR_CHARS ? `${single.slice(0, MAX_ERROR_CHARS)}…` : single;
}

/** Child environment: the host's, without any octo credential the login must not inherit. */
function childEnvironment(storeDir: string, apiUrl: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(Bun.env)) {
    if (value === undefined || key.startsWith("OCTO_") || key.startsWith("OMP_CHANNEL_OCTO_")) {
      continue;
    }
    env[key] = value;
  }
  env["OCTO_CONFIG_DIR"] = storeDir;
  env["OCTO_API_BASE_URL"] = apiUrl;
  return env;
}

/** Narrow view of the login child; Bun's spawn overloads carry a union the call site cannot use. */
type LoginChild = {
  readonly stdout: ReadableStream<Uint8Array>;
  readonly stderr: ReadableStream<Uint8Array>;
  readonly exited: Promise<number>;
  kill(signal?: number | string): void;
};

export function createOctoCliCredentialProvisioner(
  options: OctoCliCredentialOptions,
): OctoCliCredentialProvisioner {
  const command = options.command ?? DEFAULT_COMMAND;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const logger = options.logger ?? ((message: string) => console.error(message));
  let workerEnv: Record<string, string> | undefined;

  async function login(botUid: string, storeDir: string): Promise<{ ok: true } | { error: string }> {
    let child: LoginChild;
    try {
      child = Bun.spawn(
        [command, "auth", "login", "--profile", PROFILE, "--bot-id", botUid, "--with-token"],
        {
          stdin: new TextEncoder().encode(options.botToken),
          stdout: "pipe",
          stderr: "pipe",
          env: childEnvironment(storeDir, options.apiUrl),
        },
      ) as unknown as LoginChild;
    } catch (error) {
      return { error: `无法执行 ${command}：${oneLine(error instanceof Error ? error.message : String(error))}` };
    }
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill();
    }, timeoutMs);
    try {
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
        child.exited,
      ]);
      if (timedOut) return { error: `${command} auth login 超时（${timeoutMs}ms）` };
      if (exitCode !== 0) {
        const detail = stderr.trim().length > 0 ? stderr : stdout;
        return {
          error:
            detail.trim().length > 0
              ? oneLine(detail)
              : `${command} auth login 退出码 ${exitCode}`,
        };
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(stdout);
      } catch {
        return { error: "octo-cli auth login 未返回 JSON" };
      }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        return { error: "octo-cli auth login 响应格式无效" };
      }
      const record = parsed as Record<string, unknown>;
      const data = record["data"];
      const robotId =
        data && typeof data === "object" && !Array.isArray(data)
          ? (data as Record<string, unknown>)["robot_id"]
          : undefined;
      if (record["ok"] !== true) return { error: "octo-cli auth login 报告失败" };
      if (robotId !== botUid) return { error: "octo-cli 凭据归属与当前 bot 不一致" };
      return { ok: true };
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async provision(botUid: string): Promise<void> {
      try {
        const storeDir = octoCliConfigDir(botUid, options.homeDir);
        await mkdir(storeDir, { recursive: true, mode: 0o700 });
        await chmod(storeDir, 0o700);
        const result = await login(botUid, storeDir);
        if ("error" in result) {
          workerEnv = undefined;
          logger(`[octo-cli] bot ${botUid} 的 CLI 凭据不可用：${result.error}`);
          return;
        }
        workerEnv = {
          OCTO_CONFIG_DIR: storeDir,
          OCTO_API_BASE_URL: options.apiUrl,
          OCTO_BOT_ID: botUid,
        };
        logger(`[octo-cli] bot ${botUid} 的 CLI 凭据已就绪：${storeDir}`);
      } catch (error) {
        workerEnv = undefined;
        logger(
          `[octo-cli] bot ${botUid} 的 CLI 凭据准备失败：${oneLine(error instanceof Error ? error.message : String(error))}`,
        );
      }
    },
    env(): Record<string, string> | undefined {
      return workerEnv === undefined ? undefined : { ...workerEnv };
    },
  };
}
