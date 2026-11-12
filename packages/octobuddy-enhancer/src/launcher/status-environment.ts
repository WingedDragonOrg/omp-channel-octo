import { execFile } from "node:child_process";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { promisify } from "node:util";
import { hasExtendedAcl } from "../host/private-file-acl.ts";

export const STATUS_ENVIRONMENT_NAMES = [
  "OCTOBUDDY_HTTP_BASE_URL",
  "OCTOBUDDY_HTTP_TOKEN",
] as const;

export type StatusEnvironmentName = (typeof STATUS_ENVIRONMENT_NAMES)[number];
export type StatusEnvironment = Partial<Record<StatusEnvironmentName, string>>;

const execFileAsync = promisify(execFile);
const ENVRC_MARKER = "__OCTOBUDDY_STATUS_ENV__";

async function assertPrivateEnvrc(path: string): Promise<"missing" | "private"> {
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("status environment file cannot be verified on this platform");

  let file;
  try {
    file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "missing";
    throw new Error("status environment file is not readable");
  }

  try {
    const info = await file.stat();
    if (
      !info.isFile() ||
      info.uid !== uid ||
      (info.mode & 0o7777) !== 0o600 ||
      (await hasExtendedAcl(file.fd))
    ) {
      throw new Error("status environment file must be owned by the current user and mode 0600");
    }
  } finally {
    await file.close().catch(() => undefined);
  }
  return "private";
}

async function evaluateEnvrc(path: string, env: NodeJS.ProcessEnv): Promise<StatusEnvironment> {
  // The trusted .envrc executes with normal shell semantics. Its output is discarded; only the final marker line
  // carries the two values, and any shell failure is reported without stdout or stderr.
  const script = [
    "set -a",
    '. "$1" >/dev/null 2>&1 || exit 1',
    "set +x",
    `command printf '\\n${ENVRC_MARKER}%s\\000%s\\000' "\${OCTOBUDDY_HTTP_BASE_URL-}" "\${OCTOBUDDY_HTTP_TOKEN-}"`,
  ].join("\n");

  let stdout: string;
  try {
    ({ stdout } = await execFileAsync("/bin/sh", ["-c", script, "sh", path], {
      env,
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
    }));
  } catch {
    throw new Error("status environment file could not be evaluated");
  }

  const markerIndex = stdout.lastIndexOf(ENVRC_MARKER);
  if (markerIndex < 0) throw new Error("status environment file could not be evaluated");
  const [baseUrl, token] = stdout.slice(markerIndex + ENVRC_MARKER.length).split("\0");
  const values: StatusEnvironment = {};
  if (baseUrl !== undefined && baseUrl.length > 0) values.OCTOBUDDY_HTTP_BASE_URL = baseUrl;
  if (token !== undefined && token.length > 0) values.OCTOBUDDY_HTTP_TOKEN = token;
  return values;
}

export async function loadStatusEnvironment(options: {
  envrcPath?: string;
  env: NodeJS.ProcessEnv;
}): Promise<StatusEnvironment> {
  const inherited: StatusEnvironment = {};
  for (const name of STATUS_ENVIRONMENT_NAMES) {
    const value = options.env[name];
    if (value !== undefined && value.length > 0) inherited[name] = value;
  }
  if (inherited.OCTOBUDDY_HTTP_BASE_URL !== undefined && inherited.OCTOBUDDY_HTTP_TOKEN !== undefined) {
    return inherited;
  }
  if (options.envrcPath === undefined) return inherited;

  const state = await assertPrivateEnvrc(options.envrcPath);
  if (state === "missing") return inherited;
  const envrc = await evaluateEnvrc(options.envrcPath, options.env);

  const values: StatusEnvironment = { ...inherited };
  for (const name of STATUS_ENVIRONMENT_NAMES) {
    if (values[name] !== undefined) continue;
    const value = envrc[name];
    if (value !== undefined) values[name] = value;
  }
  return values;
}

export function withStatusEnvironment(
  env: NodeJS.ProcessEnv,
  values: StatusEnvironment,
): NodeJS.ProcessEnv {
  const result = { ...env };
  for (const name of STATUS_ENVIRONMENT_NAMES) {
    const value = values[name];
    if (value === undefined) delete result[name];
    else result[name] = value;
  }
  return result;
}

export function withoutStatusEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const result = { ...env };
  for (const name of STATUS_ENVIRONMENT_NAMES) delete result[name];
  return result;
}
