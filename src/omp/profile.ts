/**
 * OMP 启动环境:profile 名与参数推导、~/.omp 与项目 .env 的读取、agent 目录与
 * session root 解析,以及要传给 worker 的环境变量白名单。这里不启动进程。
 */

import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import type { WorkerContextScope } from "../contracts.ts";
import { OCTO_CLI_ENV_ALLOWLIST } from "../runtime/octo-cli.ts";
import { loadValidatedSessionState, type ProjectSessionState } from "../session/project-session.ts";

export const VALUE_FLAGS_TO_STRIP = new Set([
	"--mode",
	"--approval-mode",
	"--cwd",
	"--session-dir",
	"--resume",
	"--session",
	"-r",
]);
export const FLAGS_WITHOUT_VALUE_TO_STRIP = new Set(["--no-extensions"]);
export const PROFILE_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
export const WINDOWS_RESERVED_BASENAME_RE = /^(?:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\..*)?$/iu;

export function normalizeProfileName(profile: string | undefined): string | undefined {
	const normalized = profile?.trim();
	if (!normalized || normalized === "default") return undefined;
	if (
		normalized === "." ||
		normalized === ".." ||
		normalized.endsWith(".") ||
		!PROFILE_NAME_RE.test(normalized) ||
		WINDOWS_RESERVED_BASENAME_RE.test(normalized)
	)
		throw new Error(`Invalid OMP profile "${profile}"`);
	return normalized;
}

export type RootEnvironment = Record<string, string | undefined>;

export type ProfileSelection = {
	profile: string | undefined;
	inheritedProfile: string | undefined;
};

export const ROOT_ENV_KEYS = new Set([
	"HOME",
	"NODE_ENV",
	"OMP_PROFILE",
	"PI_PROFILE",
	"OMP_CONFIG_DIR",
	"PI_CONFIG_DIR",
	"OMP_CODING_AGENT_DIR",
	"PI_CODING_AGENT_DIR",
	"XDG_DATA_HOME",
]);
export const ROOT_ENV_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/u;
export const PROFILE_STRING_VALUE_FLAGS = new Set([
	"--cwd",
	"--config",
	"--add-dir",
	"--mode",
	"--fork",
	"--provider",
	"--model",
	"--smol",
	"--slow",
	"--plan",
	"--prewalk-into",
	"--plan-yolo-into",
	"--max-time",
	"--service-tier",
	"--api-key",
	"--system-prompt",
	"--append-system-prompt",
	"--provider-session-id",
	"--prompt-cache-key",
	"--session-dir",
	"--models",
	"--tools",
	"--thinking",
	"--export",
	"--hook",
	"--extension",
	"-e",
	"--trusted-extension",
	"--plugin-dir",
	"--skills",
	"--approval-mode",
]);
export const PROFILE_OPTIONAL_VALUE_FLAGS = new Set(["--resume", "-r", "--session"]);

export function parseRootEnvLine(line: string): { key: string; value: string } | undefined {
	const trimmed = line.trim();
	if (!trimmed || trimmed.startsWith("#")) return undefined;
	const equals = trimmed.indexOf("=");
	if (equals < 0) return undefined;
	let key = trimmed.slice(0, equals).trim();
	const exported = key.match(/^export[ \t]+(.*)$/u);
	if (exported) key = exported[1]!.trim();
	if (!ROOT_ENV_NAME_RE.test(key)) return undefined;
	const raw = trimmed.slice(equals + 1).replace(/^[ \t]+/u, "");
	const quote = raw[0];
	if (quote === '"' || quote === "'" || quote === "`") {
		let close = raw.indexOf(quote, 1);
		while (close !== -1 && raw[close - 1] === "\\") close = raw.indexOf(quote, close + 1);
		const value = close === -1 ? raw.slice(1) : raw.slice(1, close);
		return value.includes("\0") ? undefined : { key, value };
	}
	const comment = raw.search(/[ \t]#/u);
	const value = (comment < 0 ? raw : raw.slice(0, comment)).trimEnd();
	return value.includes("\0") ? undefined : { key, value };
}

export function readRootEnvFile(filePath: string, mapOmpToPi = true): Record<string, string> {
	let contents: string;
	try {
		contents = readFileSync(filePath, "utf8");
	} catch {
		return {};
	}
	const values: Record<string, string> = {};
	for (const line of contents.split("\n")) {
		const parsed = parseRootEnvLine(line);
		if (parsed) values[parsed.key] = parsed.value;
	}
	if (mapOmpToPi) {
		for (const key of Object.keys(values)) {
			if (key.startsWith("OMP_")) values[`PI_${key.slice(4)}`] = values[key]!;
		}
	}
	return values;
}

export function rootEnvironment(): RootEnvironment {
	const environment: RootEnvironment = {};
	for (const [key, value] of Object.entries(process.env)) environment[key] = value;
	return environment;
}

export function applyRootEnvFile(environment: RootEnvironment, filePath: string): void {
	for (const [key, value] of Object.entries(readRootEnvFile(filePath))) {
		if (ROOT_ENV_KEYS.has(key) && !environment[key]) environment[key] = value;
	}
}

export function expandRootEnvValues(values: Record<string, string>, environment: RootEnvironment): Record<string, string> {
	const expanded: Record<string, string> = {};
	for (const [key, value] of Object.entries(values)) {
		expanded[key] = value.replace(
			/(\\)?\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))/gu,
			(match, escaped: string | undefined, braced: string | undefined, bare: string | undefined) => {
				if (escaped) return match.slice(1);
				const name = braced ?? bare;
				if (!name) return match;
				return environment[name] ?? expanded[name] ?? "";
			},
		);
	}
	return expanded;
}

export function applyProjectDotenv(environment: RootEnvironment, cwd: string): void {
	const nodeEnv = environment.NODE_ENV || "development";
	const files = [".env", `.env.${nodeEnv}`, ".env.local", `.env.${nodeEnv}.local`];
	const loaded: Record<string, string> = {};
	for (const file of files) {
		const values = readRootEnvFile(join(cwd, file), false);
		const expanded = expandRootEnvValues(values, { ...loaded, ...environment });
		for (const [key, value] of Object.entries(expanded)) loaded[key] = value;
	}
	for (const [key, value] of Object.entries(loaded)) {
		if (!environment[key]) environment[key] = value;
	}
}

export function inheritedProfileName(environment: RootEnvironment): string | undefined {
	try {
		return normalizeProfileName(environment.OMP_PROFILE !== undefined ? environment.OMP_PROFILE : environment.PI_PROFILE);
	} catch {
		return undefined;
	}
}

export function profileSelection(args: readonly string[], environment: RootEnvironment): ProfileSelection {
	let selected: string | undefined;
	let explicit = false;
	for (let index = 0; index < args.length; index += 1) {
		const arg = args[index];
		if (arg === undefined) continue;
		if (arg === "--") break;
		if (arg === "--profile") {
			const value = args[index + 1];
			if (!value || value.startsWith("-")) throw new Error("--profile requires a profile name");
			selected = value;
			explicit = true;
			index += 1;
			continue;
		}
		if (arg.startsWith("--profile=")) {
			const value = arg.slice("--profile=".length);
			if (!value) throw new Error("--profile requires a profile name");
			selected = value;
			explicit = true;
			continue;
		}
		if (PROFILE_STRING_VALUE_FLAGS.has(arg)) {
			const next = args[index + 1];
			if (arg !== "--plan" || (next !== undefined && !next.startsWith("-"))) index += 1;
			continue;
		}
		if (PROFILE_OPTIONAL_VALUE_FLAGS.has(arg)) {
			const next = args[index + 1];
			if (next !== undefined && !next.startsWith("-")) index += 1;
		}
	}
	const inherited = inheritedProfileName(environment);
	return { profile: explicit ? normalizeProfileName(selected) : inherited, inheritedProfile: inherited };
}

export function profileDerivedAgentDir(
	cwd: string,
	configRoot: string,
	environment: RootEnvironment,
	selection: ProfileSelection,
): boolean {
	if (selection.profile !== undefined || !environment.PI_CODING_AGENT_DIR) return false;
	const source = environment.PI_PROFILE !== undefined ? normalizeProfileName(environment.PI_PROFILE) : selection.inheritedProfile;
	if (!source) return false;
	return (
		resolve(cwd, environment.PI_CODING_AGENT_DIR) ===
		resolve(join(configRoot, "profiles", source, "agent"))
	);
}

export function resolveOmpSessionRootForCwd(cwd: string, extraArgs: readonly string[]): string {
	const environment = rootEnvironment();
	applyProjectDotenv(environment, cwd);
	const selection = profileSelection(extraArgs, environment);
	const home = homedir();
	const baseConfigRoot = join(home, environment.PI_CONFIG_DIR || ".omp");
	const initialConfigRoot = join(baseConfigRoot, ...(selection.profile ? ["profiles", selection.profile] : []));
	const initialDefaultAgent = join(initialConfigRoot, "agent");
	const configuredAgentDir =
		selection.profile || profileDerivedAgentDir(cwd, baseConfigRoot, environment, selection)
			? undefined
			: environment.PI_CODING_AGENT_DIR;
	const initialAgentDir = configuredAgentDir ? resolve(cwd, configuredAgentDir) : initialDefaultAgent;

	applyRootEnvFile(environment, join(cwd, ".env"));
	applyRootEnvFile(environment, join(initialAgentDir, ".env"));
	applyRootEnvFile(environment, join(initialConfigRoot, ".env"));
	applyRootEnvFile(environment, join(home, ".env"));

	const baseConfigRootAfterEnv = join(home, environment.PI_CONFIG_DIR || ".omp");
	const configRoot = join(baseConfigRootAfterEnv, ...(selection.profile ? ["profiles", selection.profile] : []));
	const defaultAgentDir = join(configRoot, "agent");
	const configuredAgent = profileDerivedAgentDir(cwd, baseConfigRootAfterEnv, environment, selection)
		? undefined
		: environment.PI_CODING_AGENT_DIR;
	const agentDir = selection.profile || !configuredAgent ? defaultAgentDir : resolve(cwd, configuredAgent);
	let sessionsRoot = agentDir;
	if ((process.platform === "darwin" || process.platform === "linux") && resolve(agentDir) === resolve(defaultAgentDir)) {
		const xdgDataHome = environment.XDG_DATA_HOME;
		if (xdgDataHome) {
			const xdgAppRoot = join(resolve(cwd, xdgDataHome), "omp");
			if (selection.profile) {
				const xdgProfileRoot = join(xdgAppRoot, "profiles", selection.profile);
				if (existsSync(xdgProfileRoot)) sessionsRoot = xdgProfileRoot;
			} else if (existsSync(xdgAppRoot)) {
				sessionsRoot = xdgAppRoot;
			}
		}
	}
	return resolve(join(sessionsRoot, "sessions"));
}

export function stripEnforcedArgs(args: readonly string[]): string[] {
	const output: string[] = [];
	for (let index = 0; index < args.length; index++) {
		const arg = args[index]!;
		const equalsIndex = arg.indexOf("=");
		const flagName = equalsIndex >= 0 ? arg.slice(0, equalsIndex) : arg;
		if (VALUE_FLAGS_TO_STRIP.has(flagName)) {
			if (equalsIndex < 0) index++;
			continue;
		}
		if (FLAGS_WITHOUT_VALUE_TO_STRIP.has(flagName)) continue;
		output.push(arg);
	}
	return output;
}

/**
 * Environment names an OMP child may receive on top of the inherited one. Everything else with
 * an `OCTO_`/`OMP_CHANNEL_OCTO_` prefix stays stripped, so handing over a bot token by mistake
 * cannot put this channel's credential into an agent shell.
 */
export function workerEnv(scope?: WorkerContextScope, cliEnv?: Record<string, string>): Record<string, string> {
	const env: Record<string, string> = {};
	for (const [key, value] of Object.entries(Bun.env)) {
		if (key.startsWith("OCTO_") || key.startsWith("OMP_CHANNEL_OCTO_")) continue;
		if (value !== undefined) env[key] = value;
	}
	if (cliEnv) {
		for (const [key, value] of Object.entries(cliEnv)) {
			if (OCTO_CLI_ENV_ALLOWLIST[key] !== true) continue;
			env[key] = value;
		}
	}
	if (
		!scope
		|| scope.target.channelType === 1
		|| !isAbsolute(scope.sidecarRoot)
		|| scope.sidecarRoot.length === 0
		|| scope.botUid.length === 0
		|| scope.target.channelId.length === 0
	) {
		return env;
	}
	env.OMP_CHANNEL_OCTO_CONTEXT_ROOT = scope.sidecarRoot;
	env.OMP_CHANNEL_OCTO_CONTEXT_BOT_UID = scope.botUid;
	env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE = String(scope.target.channelType);
	env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID = scope.target.channelId;
	return env;
}

