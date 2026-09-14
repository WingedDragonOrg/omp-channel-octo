import { existsSync, readFileSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { loadValidatedSessionState, type ProjectSessionState } from "../project-session.ts";
import type {
	HostTool,
	OmpWorker,
	PromptContext,
	WorkerEvent,
	WorkerFactory,
	WorkerOptions,
	WorkerProcess,
} from "../contracts.ts";
import type { ExecutionEvent, ExecutionTodoPhase, TodoOperation, TodoStatus } from "../execution-card.ts";

type RpcFrame = Record<string, unknown>;
type RpcCommand = RpcFrame & { type: string };

type RpcChild = {
	readonly pid: number;
	readonly stdin: {
		write(data: string | Uint8Array): unknown;
		end?: () => unknown;
	};
	readonly stdout: ReadableStream<Uint8Array>;
	readonly stderr: ReadableStream<Uint8Array>;
	readonly exited: Promise<number>;
	kill(signal?: NodeJS.Signals): void;
};

type ProcessSnapshot = {
	identity: string;
	pgid: number;
};

type PendingResponse = {
	resolve: (frame: RpcFrame) => void;
	reject: (error: Error) => void;
	timer: NodeJS.Timeout;
};

type HostCall = {
	requestId: string;
	controller: AbortController;
};

type PromptRun = {
	requestId: string;
	onEvent: (event: WorkerEvent) => void;
	resolve: () => void;
	reject: (error: Error) => void;
	completion: Promise<void>;
	settled: boolean;
	assistantMessageEnded: boolean;
	assistantMessageId: string | undefined;
	assistantMessageKey: string | undefined;
	assistantMessageText: string;
	assistantMessageCounter: number;
	completedAssistantMessageIds: Set<string>;
	completedAssistantMessageKeys: Set<string>;
	toolSummaries: Map<string, string>;
	nextTurnId: number;
	activeTurnId: string | number | undefined;
};

type WorkerFactoryOptions = {
	ompCommand: string;
	extraArgs?: string[];
	startupTimeoutMs?: number;
	cancelTimeoutMs?: number;
};

const DEFAULT_STARTUP_TIMEOUT_MS = 10_000;
const DEFAULT_CANCEL_TIMEOUT_MS = 10_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const PROCESS_POLL_INTERVAL_MS = 20;
const SAFE_MIN_PID = 2;
const PASSIVE_UI_METHODS = new Set(["notify", "setStatus", "setWidget", "setTitle", "set_editor_text", "open_url"]);
const VALUE_FLAGS_TO_STRIP = new Set([
	"--mode",
	"--approval-mode",
	"--cwd",
	"--session-dir",
	"--resume",
	"--session",
	"-r",
]);
const PROFILE_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/u;
const WINDOWS_RESERVED_BASENAME_RE = /^(?:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\..*)?$/iu;

function normalizeProfileName(profile: string | undefined): string | undefined {
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

type RootEnvironment = Record<string, string | undefined>;

type ProfileSelection = {
	profile: string | undefined;
	inheritedProfile: string | undefined;
};

const ROOT_ENV_KEYS = new Set([
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
const ROOT_ENV_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/u;
const PROFILE_STRING_VALUE_FLAGS = new Set([
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
const PROFILE_OPTIONAL_VALUE_FLAGS = new Set(["--resume", "-r", "--session"]);

function parseRootEnvLine(line: string): { key: string; value: string } | undefined {
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

function readRootEnvFile(filePath: string, mapOmpToPi = true): Record<string, string> {
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

function rootEnvironment(): RootEnvironment {
	const environment: RootEnvironment = {};
	for (const [key, value] of Object.entries(process.env)) environment[key] = value;
	return environment;
}

function applyRootEnvFile(environment: RootEnvironment, filePath: string): void {
	for (const [key, value] of Object.entries(readRootEnvFile(filePath))) {
		if (ROOT_ENV_KEYS.has(key) && !environment[key]) environment[key] = value;
	}
}

function expandRootEnvValues(values: Record<string, string>, environment: RootEnvironment): Record<string, string> {
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

function applyProjectDotenv(environment: RootEnvironment, cwd: string): void {
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

function inheritedProfileName(environment: RootEnvironment): string | undefined {
	try {
		return normalizeProfileName(environment.OMP_PROFILE !== undefined ? environment.OMP_PROFILE : environment.PI_PROFILE);
	} catch {
		return undefined;
	}
}

function profileSelection(args: readonly string[], environment: RootEnvironment): ProfileSelection {
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

function profileDerivedAgentDir(
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

function resolveOmpSessionRootForCwd(cwd: string, extraArgs: readonly string[]): string {
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

/**
 * The watchdog is deliberately a separate process group.  If the channel
 * process is killed, the watchdog remains alive long enough to terminate the
 * RPC worker's group and any tool descendants.  It also reaps descendants
 * after an unexpected worker-parent exit.
 */
const WATCHDOG_SCRIPT = String.raw`
const watchdogArgs = process.argv.slice(-4);
const parentPid = Number(watchdogArgs[0]);
const workerPid = Number(watchdogArgs[1]);
const workerPgid = Number(watchdogArgs[2]);
const parentIdentity = watchdogArgs[3] ?? "";
const decoder = new TextDecoder();
let terminating = false;
let killDeadline = 0;

function processAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid < 2) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error && typeof error === "object" && error.code === "EPERM";
  }
}

function processStart(pid) {
  try {
    const result = Bun.spawnSync(["/bin/ps", "-p", String(pid), "-o", "lstart="]);
    if (result.exitCode !== 0) return "";
    return decoder.decode(result.stdout).trim();
  } catch {
    return "";
  }
}

function parentAlive() {
  if (!processAlive(parentPid)) return false;
  if (!parentIdentity) return true;
  const current = processStart(parentPid);
  return current.length > 0 && ("pid=" + parentPid + ";start=" + current) === parentIdentity;
}

function groupAlive() {
  if (!Number.isSafeInteger(workerPgid) || workerPgid < 2) return false;
  try {
    process.kill(-workerPgid, 0);
    return true;
  } catch (error) {
    return error && typeof error === "object" && error.code === "EPERM";
  }
}

function signalGroup(signal) {
  if (!groupAlive()) return false;
  try {
    process.kill(-workerPgid, signal);
    return true;
  } catch {
    return false;
  }
}

function beginTermination() {
  if (!terminating) {
    terminating = true;
    killDeadline = Date.now() + 10000;
    signalGroup("SIGTERM");
  }
  if (!groupAlive()) {
    process.exit(0);
    return;
  }
  if (Date.now() >= killDeadline) signalGroup("SIGKILL");
}

process.stdout.write("__omp_watchdog_ready__\n");
setInterval(() => {
  if (terminating) {
    beginTermination();
    return;
  }
  if (!parentAlive() || !processAlive(workerPid)) beginTermination();
}, 100);
`;

function asError(value: unknown, fallback: string): Error {
	return value instanceof Error ? value : new Error(typeof value === "string" ? value : fallback);
}

function isRecord(value: unknown): value is RpcFrame {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

/*
 * Source attribution for the decoder below:
 * @oh-my-pi/pi-coding-agent version 18.1.18
 * Source paths:
 * - node_modules/@oh-my-pi/pi-coding-agent/src/modes/rpc/rpc-frame.ts
 *   (decoder helpers and RpcFrameDecoder, lines 120-190)
 * - node_modules/@oh-my-pi/pi-coding-agent/src/modes/rpc/rpc-types.ts
 *   (RpcChunkFrame, lines 152-159)
 *
 * MIT License
 *
 * Copyright (c) 2025 Mario Zechner
 * Copyright (c) 2025-2026 Can Bölük
 * Copyright (c) 2026 Stencil Labs, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/** Maximum UTF-8 size of one newline-delimited RPC frame, including the newline. */
const MAX_RPC_FRAME_BYTES = 1024 * 1024;
/** Maximum UTF-8 size of one logical frame reassembled by protocol v2. */
const MAX_RPC_REASSEMBLED_BYTES = 64 * 1024 * 1024;

const RPC_CHUNK_PAYLOAD_BYTES = 256 * 1024;

interface PendingRpcChunks {
	chunkId: string;
	count: number;
	byteLength: number;
	nextIndex: number;
	chunks: Buffer[];
	receivedBytes: number;
}

interface RpcChunkFrame {
	type: "rpc_chunk";
	chunkId: string;
	index: number;
	count: number;
	byteLength: number;
	data: string;
}

function isRpcChunkFrame(value: unknown): value is RpcChunkFrame {
	return isRecord(value) && value.type === "rpc_chunk";
}

function decodeBase64(data: unknown): Buffer {
	if (
		typeof data !== "string" ||
		data.length === 0 ||
		!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(data)
	)
		throw new Error("invalid rpc chunk data");
	const bytes = Buffer.from(data, "base64");
	if (bytes.toString("base64") !== data) throw new Error("invalid rpc chunk data");
	return bytes;
}

/** Reassemble protocol v2 chunk frames after each JSONL line has been parsed. */
class RpcFrameDecoder {
	#pending: PendingRpcChunks | undefined;

	push(value: unknown): object | undefined {
		if (!isRpcChunkFrame(value)) {
			if (this.#pending) throw new Error("rpc chunk sequence interrupted");
			if (!isRecord(value)) throw new Error("rpc frame must be an object");
			return value;
		}
		const { chunkId, index, count, byteLength } = value;
		if (
			typeof chunkId !== "string" ||
			chunkId.length === 0 ||
			chunkId.length > 128 ||
			!Number.isSafeInteger(index) ||
			!Number.isSafeInteger(count) ||
			!Number.isSafeInteger(byteLength) ||
			index < 0 ||
			count < 2 ||
			count > Math.ceil(MAX_RPC_REASSEMBLED_BYTES / RPC_CHUNK_PAYLOAD_BYTES) ||
			index >= count ||
			byteLength < MAX_RPC_FRAME_BYTES ||
			byteLength > MAX_RPC_REASSEMBLED_BYTES
		)
			throw new Error("invalid rpc chunk metadata");
		const bytes = decodeBase64(value.data);
		if (bytes.byteLength > RPC_CHUNK_PAYLOAD_BYTES) throw new Error("rpc chunk payload exceeds the transport limit");

		if (!this.#pending) {
			if (index !== 0) throw new Error("rpc chunk sequence must start at index 0");
			this.#pending = { chunkId, count, byteLength, nextIndex: 0, chunks: [], receivedBytes: 0 };
		}
		const pending = this.#pending;
		if (
			pending.chunkId !== chunkId ||
			pending.count !== count ||
			pending.byteLength !== byteLength ||
			pending.nextIndex !== index
		)
			throw new Error("rpc chunk sequence mismatch");
		pending.chunks.push(bytes);
		pending.receivedBytes += bytes.byteLength;
		pending.nextIndex++;
		if (pending.receivedBytes > pending.byteLength) throw new Error("rpc chunk sequence exceeds declared length");
		if (pending.nextIndex < pending.count) return undefined;
		if (pending.receivedBytes !== pending.byteLength) throw new Error("rpc chunk sequence length mismatch");

		this.#pending = undefined;
		const decoded = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(pending.chunks));
		const frame: unknown = JSON.parse(decoded);
		if (!isRecord(frame)) throw new Error("rpc frame must be an object");
		return frame;
	}
}

function isSafeGroupId(pgid: number): boolean {
	return Number.isSafeInteger(pgid) && pgid >= SAFE_MIN_PID && pgid !== process.pid;
}

function errorFromResponse(frame: RpcFrame, command: string): Error {
	const message = typeof frame.error === "string" ? frame.error : `RPC command ${command} failed`;
	const code = typeof frame.code === "string" ? ` [${frame.code}]` : "";
	return new Error(`${message}${code}`);
}

function stripEnforcedArgs(args: readonly string[]): string[] {
	const output: string[] = [];
	for (let index = 0; index < args.length; index++) {
		const arg = args[index]!;
		const equalsIndex = arg.indexOf("=");
		const flagName = equalsIndex >= 0 ? arg.slice(0, equalsIndex) : arg;
		if (VALUE_FLAGS_TO_STRIP.has(flagName)) {
			if (equalsIndex < 0) index++;
			continue;
		}
		output.push(arg);
	}
	return output;
}

function workerEnv(): Record<string, string> {
	const env: Record<string, string> = {};
	for (const [key, value] of Object.entries(Bun.env)) {
		if (key.startsWith("OCTO_") || key.startsWith("OMP_CHANNEL_OCTO_")) continue;
		if (value !== undefined) env[key] = value;
	}
	return env;
}

async function queryProcess(pid: number): Promise<ProcessSnapshot | undefined> {
	if (!Number.isSafeInteger(pid) || pid < SAFE_MIN_PID) return undefined;
	if (process.platform === "win32") return undefined;
	let child: RpcChild;
	try {
		child = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "lstart=", "-o", "pgid="], {
			stdin: "ignore",
			stdout: "pipe",
			stderr: "ignore",
		}) as unknown as RpcChild;
	} catch {
		return undefined;
	}
	const [output, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited.catch(() => -1)]);
	if (exitCode !== 0) return undefined;
	const match = output.trim().match(/^(.*?)\s+(\d+)$/s);
	if (!match) return undefined;
	const startTime = match[1];
	const pgidText = match[2];
	if (startTime === undefined || pgidText === undefined) return undefined;
	const pgid = Number(pgidText);
	if (!Number.isSafeInteger(pgid) || pgid < SAFE_MIN_PID) return undefined;
	return { identity: `pid=${pid};start=${startTime.trim()}`, pgid };
}

function processAlive(pid: number): boolean {
	if (!Number.isSafeInteger(pid) || pid < SAFE_MIN_PID) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if (error && typeof error === "object" && "code" in error && error.code === "EPERM") return true;
		return false;
	}
}

function groupAlive(pgid: number): boolean {
	if (!isSafeGroupId(pgid)) return false;
	try {
		process.kill(-pgid, 0);
		return true;
	} catch (error) {
		return typeof error === "object" && error !== null && "code" in error && error.code === "EPERM";
	}
}

function signalGroup(pgid: number, signal: NodeJS.Signals): boolean {
	if (process.platform === "win32" || !isSafeGroupId(pgid)) return false;
	try {
		process.kill(-pgid, signal);
		return true;
	} catch {
		return false;
	}
}

async function waitForExit(child: RpcChild, timeoutMs: number): Promise<boolean> {
	let timer: NodeJS.Timeout;
	const timeout = new Promise<boolean>(resolve => {
		timer = setTimeout(() => resolve(false), Math.max(0, timeoutMs));
	});
	const exited = child.exited.then(
		() => true,
		() => true,
	);
	const result = await Promise.race([exited, timeout]);
	clearTimeout(timer!);
	return result;
}

async function waitForGroupGone(pgid: number, timeoutMs: number): Promise<boolean> {
	if (!isSafeGroupId(pgid)) return false;
	const deadline = Date.now() + Math.max(0, timeoutMs);
	while (groupAlive(pgid) && Date.now() < deadline) await Bun.sleep(PROCESS_POLL_INTERVAL_MS);
	return !groupAlive(pgid);
}

async function terminateProcessGroup(pgid: number, cancelTimeoutMs: number): Promise<boolean> {
	if (process.platform === "win32" || !isSafeGroupId(pgid)) return false;
	signalGroup(pgid, "SIGTERM");
	if (await waitForGroupGone(pgid, cancelTimeoutMs)) return true;
	signalGroup(pgid, "SIGKILL");
	return waitForGroupGone(pgid, Math.min(1_000, cancelTimeoutMs));
}

async function terminateOwnedProcess(child: RpcChild, pgid: number, cancelTimeoutMs: number): Promise<boolean> {
	const safeGroup = process.platform !== "win32" && isSafeGroupId(pgid);
	if (safeGroup) signalGroup(pgid, "SIGTERM");
	else {
		try {
			child.kill("SIGTERM");
		} catch {
			// The child may have exited between the identity check and this call.
		}
	}
	await waitForExit(child, cancelTimeoutMs);
	let groupStopped = safeGroup ? await waitForGroupGone(pgid, cancelTimeoutMs) : process.platform === "win32" && !processAlive(child.pid);
	if (!groupStopped) {
		if (safeGroup) signalGroup(pgid, "SIGKILL");
		try {
			child.kill("SIGKILL");
		} catch {
			// Already exited is acceptable; group verification below is decisive.
		}
		await waitForExit(child, Math.min(1_000, cancelTimeoutMs));
		groupStopped = safeGroup
			? await waitForGroupGone(pgid, Math.min(1_000, cancelTimeoutMs))
			: process.platform === "win32" && !processAlive(child.pid);
	}
	return groupStopped;
}

async function createWatchdog(childPid: number, workerPgid: number, parentIdentity: string): Promise<RpcChild | undefined> {
	if (process.platform === "win32") return undefined;
	let watchdog: RpcChild;
	try {
		watchdog = Bun.spawn(
			[
				process.execPath,
				"-e",
				WATCHDOG_SCRIPT,
				String(process.pid),
				String(childPid),
				String(workerPgid),
				parentIdentity,
			],
			{
				stdin: "ignore",
				env: workerEnv(),
				stdout: "pipe",
				stderr: "ignore",
				detached: true,
			},
		) as unknown as RpcChild;
	} catch {
		return undefined;
	}
	const reader = watchdog.stdout.getReader();
	const decoder = new TextDecoder();
	const ready = (async () => {
		let output = "";
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) return false;
				output += decoder.decode(result.value, { stream: true });
				if (output.includes("__omp_watchdog_ready__")) return true;
			}
		} catch {
			return false;
		}
	})();
	let timer: NodeJS.Timeout;
	const timeout = new Promise<boolean>(resolve => {
		timer = setTimeout(() => resolve(false), 1_000);
	});
	const isReady = await Promise.race([ready, timeout]);
	clearTimeout(timer!);
	if (!isReady) {
		try {
			watchdog.kill("SIGKILL");
		} catch {
			// The watchdog may have exited after a syntax or startup failure.
		}
		return undefined;
	}
	await reader.cancel().catch(() => {});
	return watchdog;
}
function stopWatchdog(watchdog: RpcChild | undefined): void {
	if (!watchdog) return;
	try {
		watchdog.kill("SIGTERM");
	} catch {
		// The watchdog normally exits itself when the worker group is gone.
	}
}

function hostResult(text: string): RpcFrame {
	return { content: [{ type: "text", text }] };
}
function modelErrorFromFrame(frame: RpcFrame): Error | undefined {
	const candidates: unknown[] = [];
	if (frame.type === "message_end") candidates.push(frame.message);
	if (frame.type === "agent_end" && Array.isArray(frame.messages)) candidates.push(...frame.messages);
	for (const candidate of candidates) {
		if (!isRecord(candidate)) continue;
		const stopReason = candidate.stopReason ?? candidate.stop_reason;
		if (stopReason !== "error") continue;
		const detail =
			typeof candidate.errorMessage === "string"
				? candidate.errorMessage
				: typeof candidate.error === "string"
					? candidate.error
					: "";
		return new Error(detail ? `OMP model reported an error: ${detail}` : "OMP model reported an error");
	}
	return undefined;
}

const TODO_STATUSES: ReadonlySet<TodoStatus> = new Set([
	"pending",
	"in_progress",
	"completed",
	"abandoned",
	"blocked",
]);
const TODO_OPERATIONS: ReadonlySet<TodoOperation> = new Set([
	"init",
	"start",
	"done",
	"rm",
	"drop",
	"block",
	"unblock",
	"append",
	"view",
]);

function asTodoStatus(value: unknown): TodoStatus | undefined {
	return typeof value === "string" && TODO_STATUSES.has(value as TodoStatus) ? (value as TodoStatus) : undefined;
}

function asTodoOperation(value: unknown): TodoOperation | undefined {
	return typeof value === "string" && TODO_OPERATIONS.has(value as TodoOperation) ? (value as TodoOperation) : undefined;
}

function asAssistantMessageKey(message: RpcFrame): string | undefined {
	const id = message.id;
	return typeof id === "string" && id.length > 0 ? id : undefined;
}

function assistantText(message: RpcFrame): string {
	if (!Array.isArray(message.content)) return "";
	return message.content
		.filter(item => isRecord(item) && item.type === "text" && typeof item.text === "string")
		.map(item => (item as RpcFrame).text as string)
		.join("");
}

function emitExecutionEvent(active: PromptRun, event: ExecutionEvent): void {
	active.onEvent(event);
}

function allocateAssistantMessage(active: PromptRun, key: string | undefined): string {
	const messageId = `assistant-${++active.assistantMessageCounter}`;
	active.assistantMessageId = messageId;
	active.assistantMessageKey = key;
	active.assistantMessageText = "";
	active.assistantMessageEnded = false;
	return messageId;
}

function ensureAssistantMessage(active: PromptRun, message: RpcFrame | undefined): string | undefined {
	const key = message ? asAssistantMessageKey(message) : undefined;
	if (!active.assistantMessageId || active.assistantMessageEnded || (key && active.assistantMessageKey && key !== active.assistantMessageKey)) {
		if (key && active.completedAssistantMessageKeys.has(key)) return undefined;
		return allocateAssistantMessage(active, key);
	}
	if (key && !active.assistantMessageKey) active.assistantMessageKey = key;
	return active.assistantMessageId;
}

function turnIdValue(value: unknown): string | number | undefined {
	if (typeof value === "string" && value.length > 0) return value;
	return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function eventTime(value: unknown): number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : Date.now();
}

function safeToolName(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const name = value.trim();
	return /^[A-Za-z0-9_.:-]{1,64}$/u.test(name) ? name : undefined;
}

function safeToolCallId(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const id = value.trim();
	return id.length > 0 ? id : undefined;
}

const PATH_SUMMARY_TOOLS: ReadonlySet<string> = new Set(["read", "glob", "grep", "write", "edit"]);
const SAFE_SUMMARY_COMPONENT = /^[A-Za-z0-9._-]{1,64}$/u;

function safeToolSummary(toolName: string, args: unknown): string | undefined {
	if (!PATH_SUMMARY_TOOLS.has(toolName) || !isRecord(args) || typeof args.path !== "string") return undefined;
	let path = args.path.trim();
	if (path.startsWith("skill://")) path = path.slice("skill://".length);
	const queryStart = path.search(/[?#]/u);
	if (queryStart >= 0) path = path.slice(0, queryStart);
	path = path.replaceAll("\\", "/");
	const slash = path.lastIndexOf("/");
	let component = path.slice(slash + 1);
	const selector = component.indexOf(":");
	if (selector >= 0) component = component.slice(0, selector);
	component = component.trim();
	return SAFE_SUMMARY_COMPONENT.test(component) ? component : undefined;
}

function todoDetailsFromResult(result: unknown): { phases: ExecutionTodoPhase[]; op: TodoOperation | undefined } | undefined {
	if (!isRecord(result) || !isRecord(result.details) || !Array.isArray(result.details.phases)) return undefined;
	const phases: ExecutionTodoPhase[] = [];
	for (const rawPhase of result.details.phases) {
		if (!isRecord(rawPhase) || typeof rawPhase.name !== "string" || !Array.isArray(rawPhase.tasks)) return undefined;
		const tasks: ExecutionTodoPhase["tasks"][number][] = [];
		for (const rawTask of rawPhase.tasks) {
			if (!isRecord(rawTask) || typeof rawTask.content !== "string") return undefined;
			const status = asTodoStatus(rawTask.status);
			if (!status) return undefined;
			if (typeof rawTask.blocker === "string") {
				tasks.push({ content: rawTask.content, status, blocker: rawTask.blocker });
			} else {
				tasks.push({ content: rawTask.content, status });
			}
		}
		phases.push({ name: rawPhase.name, tasks });
	}
	return { phases, op: asTodoOperation(result.details.op) };
}


function describeUiFailure(method: string): Error {
	return new Error(`OMP requested unsupported interactive UI (${method}); worker stopped safely`);
}


class RpcWorker implements OmpWorker {
	#child: RpcChild;
	#watchdog: RpcChild | undefined;
	#pgid: number;
	#startupTimeoutMs: number;
	#cancelTimeoutMs: number;
	#newSessionOnStart: boolean;
	#decoder = new RpcFrameDecoder();
	#protocolV2 = false;
	#ready = false;
	#readyV2Supported = false;
	#closed = false;
	#stopping = false;
	#stopPromise: Promise<boolean> | undefined;
	#requestCounter = 0;
	#pending = new Map<string, PendingResponse>();
	#activePrompt: PromptRun | undefined;
	#activeTools = new Map<string, HostTool>();
	#hostCalls = new Map<string, HostCall>();
	#sessionFile = "";
	#sessionId = "";
	#readyPromise: Promise<void>;
	#readyResolve: () => void;
	#readyReject: (error: Error) => void;

	constructor(
		child: RpcChild,
		watchdog: RpcChild | undefined,
		pgid: number,
		startupTimeoutMs: number,
		cancelTimeoutMs: number,
		newSessionOnStart: boolean,
	) {
		this.#child = child;
		this.#watchdog = watchdog;
		this.#pgid = pgid;
		this.#startupTimeoutMs = startupTimeoutMs;
		this.#cancelTimeoutMs = cancelTimeoutMs;
		this.#newSessionOnStart = newSessionOnStart;
		const ready = Promise.withResolvers<void>();
		this.#readyPromise = ready.promise;
		this.#readyResolve = () => ready.resolve();
		this.#readyReject = error => ready.reject(error);
		void this.#readStdout();
		void this.#readStderr();
		void this.#child.exited.then(
			code => this.#onChildExit(code),
			error => this.#onFatal(asError(error, "OMP worker exited")),
		);
	}

	get sessionFile(): string {
		return this.#sessionFile;
	}

	get sessionId(): string {
		return this.#sessionId;
	}

	async initialize(): Promise<void> {
		await this.#waitWithTimeout(this.#readyPromise, this.#startupTimeoutMs, "OMP worker did not become ready");
		if (this.#readyV2Supported) {
			const negotiated = await this.#request({ type: "negotiate_protocol", protocolVersion: 2 }, this.#startupTimeoutMs);
			this.#assertSuccess(negotiated, "negotiate_protocol");
			const data = negotiated.data;
			if (!isRecord(data) || data.protocolVersion !== 2) throw new Error("OMP RPC protocol negotiation returned an invalid version");
			this.#protocolV2 = true;
		}
		if (this.#newSessionOnStart) {
			const newSession = await this.#request({ type: "new_session" }, this.#startupTimeoutMs);
			this.#assertSuccess(newSession, "new_session");
			const data = newSession.data;
			if (!isRecord(data) || data.cancelled !== false) throw new Error("OMP RPC new_session was cancelled");
		}
		await this.refreshState();
	}

	async refreshState(): Promise<ProjectSessionState> {
		const stateResponse = await this.#request({ type: "get_state" }, this.#startupTimeoutMs);
		this.#assertSuccess(stateResponse, "get_state");
		const state = stateResponse.data;
		if (
			!isRecord(state) ||
			typeof state.sessionFile !== "string" ||
			state.sessionFile.length === 0 ||
			typeof state.sessionId !== "string" ||
			state.sessionId.length === 0
		)
			throw new Error("OMP RPC get_state did not return a session file and session ID");
		const loaded = await loadValidatedSessionState(state.sessionFile, state.sessionId);
		this.#sessionFile = loaded.state.sessionFile;
		this.#sessionId = loaded.state.sessionId;
		return loaded.state;
	}

	async prompt(text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
		if (this.#closed) throw new Error("OMP worker is not running");
		if (this.#activePrompt) throw new Error("OMP worker already has a prompt in progress");
		if (typeof text !== "string") throw new Error("OMP prompt text must be a string");
		const completion = Promise.withResolvers<void>();
		const run: PromptRun = {
			requestId: "",
			onEvent,
			resolve: () => completion.resolve(),
			reject: error => completion.reject(error),
			completion: completion.promise,
			settled: false,
			assistantMessageEnded: false,
			assistantMessageId: undefined,
			assistantMessageKey: undefined,
			assistantMessageText: "",
			assistantMessageCounter: 0,
			completedAssistantMessageIds: new Set(),
			completedAssistantMessageKeys: new Set(),
			toolSummaries: new Map(),
			nextTurnId: 0,
			activeTurnId: undefined,
		};
		this.#activePrompt = run;
		try {
			await this.#installHostTools(context?.tools ?? []);
			const response = await this.#request(
				{ type: "prompt", message: text },
				DEFAULT_REQUEST_TIMEOUT_MS,
				requestId => {
					run.requestId = requestId;
				},
			);
			this.#assertSuccess(response, "prompt");
			if (response.id !== run.requestId) throw new Error("OMP prompt acknowledgement did not contain the expected request ID");
			const data = response.data;
			if (isRecord(data) && data.agentInvoked === false) this.#settlePrompt(run);
			await run.completion;
		} catch (error) {
			this.#rejectPrompt(run, asError(error, "OMP prompt failed"));
			throw error;
		} finally {
			this.#abortHostCalls(new Error("prompt finished"));
			await this.#clearHostTools();
			if (this.#activePrompt === run) this.#activePrompt = undefined;
		}
	}

	async stop(): Promise<boolean> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#stopPromise = this.#stopInternal();
		return this.#stopPromise;
	}


	async #stopInternal(): Promise<boolean> {
		this.#stopping = true;
		this.#abortHostCalls(new Error("worker stopped"));
		const active = this.#activePrompt;
		if (active) this.#rejectPrompt(active, new Error("OMP prompt cancelled"));
		if (!this.#closed) {
			try {
				const abort = await this.#request({ type: "abort" }, Math.min(1_000, this.#cancelTimeoutMs));
				this.#assertSuccess(abort, "abort");
			} catch {
				// Group termination below is authoritative if the control request
				// cannot be acknowledged.
			}
		}
		this.#closed = true;
		this.#rejectPending(new Error("OMP worker stopped"));
		const stopped = await terminateOwnedProcess(this.#child, this.#pgid, this.#cancelTimeoutMs);
		if (stopped) stopWatchdog(this.#watchdog);
		return stopped;
	}

	async #installHostTools(tools: readonly HostTool[]): Promise<void> {
		const definitions: RpcFrame[] = [];
		const nextTools = new Map<string, HostTool>();
		for (const tool of tools) {
			if (!tool || typeof tool.name !== "string" || tool.name.trim().length === 0) throw new Error("Host tool name is required");
			if (nextTools.has(tool.name)) throw new Error(`Duplicate host tool: ${tool.name}`);
			if (typeof tool.description !== "string" || !tool.parameters || typeof tool.parameters !== "object")
				throw new Error(`Invalid host tool definition: ${tool.name}`);
			nextTools.set(tool.name, tool);
			definitions.push({ name: tool.name, description: tool.description, parameters: tool.parameters, loadMode: "essential" });
		}
		const response = await this.#request({ type: "set_host_tools", tools: definitions }, DEFAULT_REQUEST_TIMEOUT_MS);
		this.#assertSuccess(response, "set_host_tools");
		this.#activeTools.clear();
		for (const [name, tool] of nextTools) this.#activeTools.set(name, tool);
	}

	async #clearHostTools(): Promise<void> {
		this.#abortHostCalls(new Error("host tools cleared"));
		this.#activeTools.clear();
		if (this.#closed) return;
		try {
			const response = await this.#request({ type: "set_host_tools", tools: [] }, DEFAULT_REQUEST_TIMEOUT_MS);
			this.#assertSuccess(response, "set_host_tools");
		} catch {
			// The worker may have exited between turn completion and cleanup.
		}
	}

	#assertSuccess(frame: RpcFrame, command: string): void {
		if (frame.success !== true) throw errorFromResponse(frame, command);
	}

	async #request(command: RpcCommand, timeoutMs: number, onId?: (id: string) => void): Promise<RpcFrame> {
		if (this.#closed) throw new Error("OMP worker is not running");
		const id = `omp-${++this.#requestCounter}`;
		onId?.(id);
		const frame = { ...command, id };
		return new Promise<RpcFrame>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(new Error(`Timed out waiting for OMP RPC ${command.type}`));
			}, timeoutMs);
			this.#pending.set(id, { resolve, reject, timer });
			try {
				this.#writeFrame(frame);
			} catch (error) {
				clearTimeout(timer);
				this.#pending.delete(id);
				reject(asError(error, `Could not send OMP RPC ${command.type}`));
			}
		});
	}

	#writeFrame(frame: RpcFrame): void {
		if (this.#closed) throw new Error("OMP worker is not running");
		this.#child.stdin.write(`${JSON.stringify(frame)}\n`);
	}

	async #waitWithTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
		let timer: NodeJS.Timeout;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new Error(message)), timeoutMs);
		});
		try {
			return await Promise.race([promise, timeout]);
		} finally {
			clearTimeout(timer!);
		}
	}

	async #readStdout(): Promise<void> {
		const reader = this.#child.stdout.getReader();
		const decoder = new TextDecoder("utf-8", { fatal: true });
		let buffered = "";
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) break;
				buffered += decoder.decode(result.value, { stream: true });
				for (;;) {
					const newline = buffered.indexOf("\n");
					if (newline < 0) break;
					const line = buffered.slice(0, newline);
					buffered = buffered.slice(newline + 1);
					if (!line.trim()) continue;
					const value: unknown = JSON.parse(line);
					if (!isRecord(value)) throw new Error("OMP RPC frame must be an object");
					this.#handlePhysicalFrame(value);
				}
			}
			buffered += decoder.decode();
			if (buffered.trim().length > 0) throw new Error("OMP RPC output ended with an unterminated frame");
			if (!this.#ready) this.#readyReject(new Error("OMP output ended before ready"));
			else if (!this.#stopping) this.#onFatal(new Error("OMP output ended unexpectedly"));
		} catch (error) {
			if (!this.#ready) this.#readyReject(asError(error, "OMP RPC output could not be decoded"));
			else if (!this.#stopping) this.#onFatal(asError(error, "OMP RPC output could not be decoded"));
		}
	}

	async #readStderr(): Promise<void> {
		const reader = this.#child.stderr.getReader();
		try {
			for (;;) {
				const result = await reader.read();
				if (result.done) break;
			}
		} catch {
			// Stderr is deliberately discarded to keep credentials out of errors
			// and logs while still draining the pipe to avoid child deadlocks.
		}
	}

	#handlePhysicalFrame(value: RpcFrame): void {
		if (!this.#ready && value.type === "ready") {
			this.#ready = true;
			this.#readyV2Supported =
				Array.isArray(value.supportedProtocolVersions) && value.supportedProtocolVersions.some(version => version === 2);
			this.#readyResolve();
			return;
		}
		let frame: object | undefined;
		try {
			frame = this.#decoder.push(value);
		} catch (error) {
			this.#onFatal(asError(error, "OMP RPC frame reassembly failed"));
			return;
		}
		if (frame && isRecord(frame)) this.#handleFrame(frame);
	}

	#handleFrame(frame: RpcFrame): void {
		if (frame.type === "response") {
			this.#handleResponse(frame);
			return;
		}
		if (frame.type === "extension_ui_request") {
			this.#handleUiRequest(frame);
			return;
		}
		if (frame.type === "host_tool_call") {
			this.#handleHostToolCall(frame);
			return;
		}
		if (frame.type === "host_tool_cancel") {
			this.#handleHostToolCancel(frame);
			return;
		}
		const active = this.#activePrompt;
		if (!active) return;
		const modelError = modelErrorFromFrame(frame);
		if (modelError) {
			this.#rejectPrompt(active, modelError);
			return;
		}
		if (frame.type === "agent_end") {
			if (frame.willContinue !== true) this.#settlePrompt(active);
			return;
		}
		if (frame.type === "prompt_result" && frame.agentInvoked === false) {
			this.#settlePrompt(active);
			return;
		}
		this.#emitWorkerEvent(active, frame);
	}

	#handleResponse(frame: RpcFrame): void {
		const id = typeof frame.id === "string" ? frame.id : undefined;
		if (id) {
			const pending = this.#pending.get(id);
			if (pending) {
				this.#pending.delete(id);
				clearTimeout(pending.timer);
				pending.resolve(frame);
				return;
			}
			const active = this.#activePrompt;
			if (active && active.requestId === id && frame.success === false) {
				this.#rejectPrompt(active, errorFromResponse(frame, "prompt"));
				return;
			}
		}
	}

	#handleUiRequest(frame: RpcFrame): void {
		const method = typeof frame.method === "string" ? frame.method : "unknown";
		if (PASSIVE_UI_METHODS.has(method)) return;
		try {
			if (typeof frame.id === "string" && !this.#closed) this.#writeFrame({ type: "extension_ui_response", id: frame.id, cancelled: true });
		} catch {
			// The fatal path below still rejects the prompt and stops the group.
		}
		const active = this.#activePrompt;
		if (active) this.#rejectPrompt(active, describeUiFailure(method));
		void this.stop();
	}

	#handleHostToolCall(frame: RpcFrame): void {
		const requestId = typeof frame.id === "string" ? frame.id : "";
		const toolName = typeof frame.toolName === "string" ? frame.toolName : "";
		if (!requestId) return;
		const tool = this.#activeTools.get(toolName);
		if (!tool) {
			this.#sendHostToolResult(requestId, true, `Unknown host tool: ${toolName || "<missing>"}`);
			return;
		}
		const controller = new AbortController();
		const call: HostCall = { requestId, controller };
		this.#hostCalls.set(requestId, call);
		const args = isRecord(frame.arguments) ? frame.arguments : {};
		void Promise.resolve()
			.then(() => tool.execute(args, controller.signal))
			.then(
				text => this.#sendHostToolResult(requestId, false, typeof text === "string" ? text : String(text)),
				error => this.#sendHostToolResult(requestId, true, asError(error, "Host tool failed").message),
			)
			.finally(() => {
				if (this.#hostCalls.get(requestId) === call) this.#hostCalls.delete(requestId);
			});
	}

	#handleHostToolCancel(frame: RpcFrame): void {
		const targetId = typeof frame.targetId === "string" ? frame.targetId : "";
		if (!targetId) return;
		const call = this.#hostCalls.get(targetId);
		if (call) call.controller.abort(new Error("OMP cancelled host tool"));
	}

	#sendHostToolResult(requestId: string, isError: boolean, text: string): void {
		if (this.#closed) return;
		try {
			this.#writeFrame({ type: "host_tool_result", id: requestId, isError, result: hostResult(text) });
		} catch {
			// Process termination will reject the active prompt.
		}
	}

	#emitWorkerEvent(active: PromptRun, frame: RpcFrame): void {
		try {
			if (frame.type === "message_start") {
				const message = frame.message;
				if (isRecord(message) && message.role === "assistant") {
					const key = asAssistantMessageKey(message);
					if (!(key && active.completedAssistantMessageKeys.has(key))) ensureAssistantMessage(active, message);
				}
				return;
			}
			if (frame.type === "message_update") {
				const message = frame.message;
				if (!isRecord(message) || message.role !== "assistant") return;
				const assistantMessageEvent = frame.assistantMessageEvent;
				if (
					!isRecord(assistantMessageEvent) ||
					assistantMessageEvent.type !== "text_delta" ||
					typeof assistantMessageEvent.delta !== "string"
				) {
					return;
				}
				const messageId = ensureAssistantMessage(active, message);
				if (!messageId) return;
				active.assistantMessageText += assistantMessageEvent.delta;
				active.onEvent({
					type: "text",
					messageId,
					text: active.assistantMessageText,
					final: false,
				});
				return;
			}
			if (frame.type === "message_end") {
				const message = frame.message;
				if (!isRecord(message) || message.role !== "assistant") return;
				const key = asAssistantMessageKey(message);
				if (key && active.completedAssistantMessageKeys.has(key)) return;
				if (active.assistantMessageEnded && (!key || active.assistantMessageKey === key)) return;
				const messageId = ensureAssistantMessage(active, message);
				if (!messageId || active.assistantMessageEnded || active.completedAssistantMessageIds.has(messageId)) return;
				const text = assistantText(message);
				active.assistantMessageText = text;
				active.assistantMessageEnded = true;
				active.completedAssistantMessageIds.add(messageId);
				if (key) active.completedAssistantMessageKeys.add(key);
				active.onEvent({ type: "text", messageId, text, final: true });
			}
			if (frame.type === "turn_start") {
				const upstreamTurnId = turnIdValue(frame.turnIndex);
				const turnId = upstreamTurnId ?? active.nextTurnId++;
				if (typeof turnId === "number") active.nextTurnId = Math.max(active.nextTurnId, Math.floor(turnId) + 1);
				active.activeTurnId = turnId;
				emitExecutionEvent(active, { type: "turn_start", turnId, atMs: eventTime(frame.timestamp) });
				return;
			}
			if (frame.type === "turn_end") {
				const turnId = turnIdValue(frame.turnIndex) ?? active.activeTurnId;
				const atMs = eventTime(frame.timestamp);
				if (turnId === undefined) emitExecutionEvent(active, { type: "turn_end", atMs });
				else emitExecutionEvent(active, { type: "turn_end", turnId, atMs });
				active.activeTurnId = undefined;
				return;
			}
			if (frame.type === "tool_execution_start") {
				const toolCallId = safeToolCallId(frame.toolCallId);
				const toolName = safeToolName(frame.toolName);
				if (!toolCallId || !toolName) return;
				const summary = safeToolSummary(toolName, frame.args);
				if (summary) active.toolSummaries.set(toolCallId, summary);
				if (summary === undefined) {
					emitExecutionEvent(active, { type: "tool_start", toolCallId, toolName, atMs: Date.now() });
				} else {
					emitExecutionEvent(active, { type: "tool_start", toolCallId, toolName, atMs: Date.now(), summary });
				}
				return;
			}
			if (frame.type === "tool_execution_end") {
				const toolCallId = safeToolCallId(frame.toolCallId);
				const toolName = safeToolName(frame.toolName);
				if (!toolCallId || !toolName) return;
				const summary = active.toolSummaries.get(toolCallId);
				active.toolSummaries.delete(toolCallId);
				const failed = frame.isError === true;
				const atMs = Date.now();
				if (failed) {
					if (summary === undefined) {
						emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "failed", errorCategory: "unknown" });
					} else {
						emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "failed", errorCategory: "unknown", summary });
					}
				} else if (summary === undefined) {
					emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "succeeded" });
				} else {
					emitExecutionEvent(active, { type: "tool_end", toolCallId, atMs, status: "succeeded", summary });
				}
				if (!failed && toolName === "todo") {
					const details = todoDetailsFromResult(frame.result);
					if (details) {
						const todoEvent =
							details.op === undefined
								? { type: "todo_update" as const, atMs, phases: details.phases }
								: { type: "todo_update" as const, atMs, op: details.op, phases: details.phases };
						emitExecutionEvent(active, todoEvent);
					}
				}
			}
		} catch (error) {
			this.#rejectPrompt(active, asError(error, "OMP event callback failed"));
		}
	}
	#settlePrompt(run: PromptRun): void {
		if (run.settled) return;
		run.settled = true;
		run.resolve();
	}

	#rejectPrompt(run: PromptRun, error: Error): void {
		if (run.settled) return;
		run.settled = true;
		run.reject(error);
	}

	#abortHostCalls(reason: Error): void {
		for (const call of this.#hostCalls.values()) call.controller.abort(reason);
		this.#hostCalls.clear();
	}

	#rejectPending(error: Error): void {
		for (const pending of this.#pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.#pending.clear();
	}

	#onChildExit(code: number): void {
		if (this.#stopping || this.#closed) return;
		const error = new Error(`OMP worker exited with code ${code}`);
		this.#onFatal(error);
	}

	#onFatal(error: Error): void {
		if (this.#closed) return;
		this.#closed = true;
		this.#readyReject(error);
		this.#rejectPending(error);
		this.#abortHostCalls(error);
		if (this.#activePrompt) this.#rejectPrompt(this.#activePrompt, error);
		void terminateOwnedProcess(this.#child, this.#pgid, this.#cancelTimeoutMs).then(stopped => {
			if (stopped) stopWatchdog(this.#watchdog);
		});
	}
}

class OmpWorkerFactory implements WorkerFactory {
	#options: WorkerFactoryOptions;
	#startupTimeoutMs: number;
	#cancelTimeoutMs: number;

	constructor(options: WorkerFactoryOptions) {
		if (!options || typeof options.ompCommand !== "string" || options.ompCommand.length === 0)
			throw new Error("ompCommand is required");
		this.#options = { ...options, extraArgs: [...(options.extraArgs ?? [])] };
		this.#startupTimeoutMs = options.startupTimeoutMs && options.startupTimeoutMs > 0 ? options.startupTimeoutMs : DEFAULT_STARTUP_TIMEOUT_MS;
		this.#cancelTimeoutMs = options.cancelTimeoutMs && options.cancelTimeoutMs > 0 ? options.cancelTimeoutMs : DEFAULT_CANCEL_TIMEOUT_MS;
	}
	async sessionRootsFor(cwd: string): Promise<readonly string[]> {
		if (!isAbsolute(cwd)) return [];
		return [resolveOmpSessionRootForCwd(resolve(cwd), this.#options.extraArgs ?? [])];
	}

	async create(options: WorkerOptions): Promise<OmpWorker> {

		if (!options || !isAbsolute(options.cwd) || !isAbsolute(options.sessionDir))
			throw new Error("OMP worker cwd and sessionDir must be absolute");
		if (!existsSync(options.cwd)) throw new Error(`OMP worker cwd does not exist: ${options.cwd}`);
		if (!existsSync(options.sessionDir)) throw new Error(`OMP worker sessionDir does not exist: ${options.sessionDir}`);
		const cwd = realpathSync(options.cwd);
		const sessionDir = realpathSync(options.sessionDir);
		if (!existsSync(sessionDir)) throw new Error(`OMP worker sessionDir does not exist: ${sessionDir}`);
		const command = [
			this.#options.ompCommand,
			...stripEnforcedArgs(this.#options.extraArgs ?? []),
			"--mode",
			"rpc",
			"--approval-mode",
			"yolo",
			"--cwd",
			cwd,
			"--session-dir",
			sessionDir,
		];
		if (options.sessionFile) command.push("--resume", resolve(options.sessionFile));
		let child: RpcChild;
		try {
			child = Bun.spawn(command, {
				cwd,
				stdin: "pipe",
				stdout: "pipe",
				env: workerEnv(),
				stderr: "pipe",
				detached: process.platform !== "win32",
			}) as unknown as RpcChild;
		} catch (error) {
			throw asError(error, "Could not spawn OMP RPC worker");
		}
		const parentSnapshot = await queryProcess(process.pid);
		const snapshot = await queryProcess(child.pid);
		if (!snapshot) {
			signalGroup(child.pid, "SIGKILL");
			try {
				child.kill("SIGKILL");
			} catch {
				// Spawn failure cleanup is best effort; no worker is returned.
			}
			throw new Error("Could not verify the OMP worker process identity");
		}
		const watchdog = await createWatchdog(child.pid, snapshot.pgid, parentSnapshot?.identity ?? "");
		if (process.platform !== "win32" && !watchdog) {
			await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			throw new Error("Could not start the OMP parent-death watchdog");
		}
		const processInfo: WorkerProcess = { pid: child.pid, pgid: snapshot.pgid, identity: snapshot.identity, cwd };
		try {
			options.onSpawn(processInfo);
		} catch (error) {
			const stopped = await terminateOwnedProcess(child, snapshot.pgid, this.#cancelTimeoutMs);
			if (stopped) stopWatchdog(watchdog);
			throw error;
		}
		const worker = new RpcWorker(child, watchdog, snapshot.pgid, this.#startupTimeoutMs, this.#cancelTimeoutMs, !options.sessionFile);
		try {
			await worker.initialize();
			return worker;
		} catch (error) {
			await worker.stop();
			throw asError(error, "OMP RPC worker failed during startup");
		}
	}

	async recover(processInfo: WorkerProcess): Promise<boolean> {
		if (!processInfo || !isSafeGroupId(processInfo.pgid) || !isAbsolute(processInfo.cwd) || !processInfo.identity) return false;
		if (!existsSync(processInfo.cwd)) return false;
		const snapshot = await queryProcess(processInfo.pid);
		if (snapshot) {
			if (snapshot.pgid !== processInfo.pgid || snapshot.identity !== processInfo.identity) return false;
			if (!groupAlive(processInfo.pgid)) return true;
			return terminateProcessGroup(processInfo.pgid, this.#cancelTimeoutMs);
		}
		// A missing worker PID is recoverable only when the whole persisted group
		// is definitely gone.  A live or permission-denied group is ambiguous.
		return !groupAlive(processInfo.pgid);
	}
}

export function createWorkerFactory(options: WorkerFactoryOptions): WorkerFactory {
	return new OmpWorkerFactory(options);
}
