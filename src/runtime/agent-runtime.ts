import { createHash, randomBytes, randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rm } from "node:fs/promises";
import {
  closeSync,
  fsyncSync,
  openSync,
  realpathSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { Database } from "bun:sqlite";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import type {
  OmpAvailableCommand,
  OmpSubagentMessages,
  OmpSubagentSnapshot,
  OmpWorker,
  HostTool,
  PromptContext,
  WorkerContextUsage,
  WorkerEvent,
  WorkerFactory,
  WorkerGoalResult,
  WorkerModelCatalog,
  WorkerModelSwitch,
  WorkerOptions,
  WorkerProcess,
  TaskStartupCode,
} from "../contracts.ts";
import { TaskStartupError, WorkerGoalError, WorkerSteerRejectedError } from "../contracts.ts";
import type { ProjectSessionState } from "../session/project-session.ts";
import { createWorkerFactory } from "../omp/index.ts";
import { OCTO_CLI_ENV_ALLOWLIST } from "./octo-cli.ts";
import { queryProcess } from "../omp/process.ts";
import { IS_AOT_BINARY } from "./aot-runtime.ts";
import {
  AGENT_RUNTIME_PROTOCOL_VERSION,
  type AgentRuntimeAttachWorkerInput,
  type AgentRuntimeCreateWorkerInput,
  type AgentRuntimeDispatchInput,
  type AgentRuntimeDispatchSnapshot,
  type AgentRuntimeDispatchState,
  type AgentRuntimePromptContext,
  type AgentRuntimeWorkerSnapshot,
} from "./agent-runtime-protocol.ts";

const MAX_FRAME_BYTES = 16 * 1024 * 1024;
const SOCKET_CONNECT_TIMEOUT_MS = 1_500;
const DAEMON_START_TIMEOUT_MS = 15_000;
const SESSION_BUSY_ERROR = "该 OMP session 已由另一个 runtime run 占用";

type RuntimePaths = {
  runtimeDir: string;
  socketDir: string;
  socketPath: string;
  manifestPath: string;
  ledgerPath: string;
  lockPath: string;
  configPath: string;
};

type RuntimeManifest = {
  protocolVersion: number;
  botUid: string;
  runtimeId: string;
  pid: number;
  processIdentity: string;
  socketPath: string;
  secret: string;
  startedAt: number;
};

type Mutable<T> = { -readonly [Key in keyof T]: T[Key] };

type StoredDispatch = Mutable<AgentRuntimeDispatchSnapshot> & {
  events: WorkerEvent[];
  fingerprint: string;
};

type StoredWorker = Mutable<AgentRuntimeWorkerSnapshot> & {
  sessionKeys: string[];
  dispatchMap: Record<string, StoredDispatch>;
};

type WorkerRecord = {
  snapshot: StoredWorker;
  worker?: OmpWorker;
  calls: Map<string, PendingHostCall>;
  cancelledCalls: Map<string, PendingHostCall>;
  acceptedToolResults: Map<string, AcceptedHostToolResult>;
  hostToolResultHashes: Map<string, Map<string, string[]>>;
};

type PendingHostCall = {
  callId: string;
  dispatchId: string;
  toolName: string;
  args: Record<string, unknown>;
  toolCallId?: string;
  controller: AbortController;
  resolve: (text: string) => void;
  reject: (error: Error) => void;
  settled: boolean;
};
type AcceptedHostToolResult = {
  ackId: string;
  dispatchId: string;
  toolName: string;
  toolCallId: string;
  resultHash: string;
};

type RuntimeConnection = {
  socket: Socket;
  epoch: number;
  buffer: string;
  watchers: Map<string, number>;
  requests: Map<string, AbortController>;
};

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  abort?: () => void;
};

type RuntimeClientConnection = {
  socket: Socket | undefined;
  connectingSocket: Socket | undefined;
  epoch: number;
  pending: Map<string, PendingRequest>;
  eventListeners: Set<(event: RuntimeEvent) => void>;
  closed: boolean;
  available: boolean;
  error?: Error;
  reconnecting: Promise<void> | undefined;
};

type RuntimeClientIdentity = Pick<
  RuntimeManifest,
  "protocolVersion" | "botUid" | "runtimeId" | "pid" | "processIdentity" | "socketPath" | "secret"
> & { manifestPath: string };

type RuntimeWorkerRegistration = {
  input: AttachWorkerInput;
  snapshot?: AgentRuntimeWorkerSnapshot;
  attachedEpoch?: number;
};

type RuntimeWatch = {
  readonly key: string;
  readonly taskId: string;
  readonly runId: string;
  readonly dispatchId: string;
  cursor: number;
  readonly onEvent: AgentRuntimeWatchEvent;
  readonly promise: Promise<AgentRunState>;
  readonly resolve: (state: AgentRunState) => void;
  readonly reject: (error: Error) => void;
  armedEpoch: number | undefined;
  pendingEvents: RuntimeEvent[];
  settled: boolean;
};

type ExecutingHostTool = {
  readonly taskId: string;
  readonly runId: string;
  readonly dispatchId: string;
  readonly toolCallId?: string;
  readonly controller: AbortController;
  result?: { readonly isError: boolean; readonly result: string };
  delivery?: Promise<void> | undefined;
};

class RuntimeConnectionLostError extends Error {
  constructor() {
    super("Agent runtime channel disconnected");
    this.name = "RuntimeConnectionLostError";
  }
}

class RuntimeIdentityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RuntimeIdentityError";
  }
}

type RuntimeEvent = {
  event: string;
  taskId: string;
  runId: string;
  dispatchId?: string;
  seq?: number;
  value?: unknown;
};

type WireMessage = Record<string, unknown>;

type CreateWorkerInput = AgentRuntimeCreateWorkerInput;
type AttachWorkerInput = AgentRuntimeAttachWorkerInput;

export interface AgentRuntimeLaunchOptions {
  readonly stateDir: string;
  readonly botUid: string;
  readonly ompCommand: string;
  readonly ompExtraArgs?: readonly string[];
  readonly startupTimeoutMs?: number;
  readonly cancelTimeoutMs?: number;
  readonly cliEnv?: Readonly<Record<string, string>>;
}

export interface AgentRuntimeHandle {
  readonly runtimeId: string;
  readonly pid: number;
  readonly socketPath: string;
  /** Explicit administrative shutdown. Channel shutdown must only disconnect its client. */
  shutdown(): Promise<void>;
}

export interface EnsureAgentRuntimeResult {
  readonly handle: AgentRuntimeHandle;
  readonly client: AgentRuntimeClient;
}

export interface AgentRunState extends AgentRuntimeDispatchSnapshot {
  readonly state: AgentRuntimeDispatchState;
}

export type AgentRuntimeWatchEvent = (event: WorkerEvent, seq: number) => void;

export interface AgentRuntimeServerOptions extends AgentRuntimeLaunchOptions {
  readonly runtimeDir?: string;
  readonly socketPath?: string;
  readonly ledgerPath?: string;
  readonly manifestPath?: string;
}

function safeId(value: string, name: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 256 || value.includes("\0")) {
    throw new Error(`${name} is invalid`);
  }
  return value;
}

function safeCliEnv(values: Readonly<Record<string, string>> | undefined, botUid: string): Record<string, string> | undefined {
  if (!values) return undefined;
  const safe: Record<string, string> = {};
  for (const [key, value] of Object.entries(values)) {
    if (OCTO_CLI_ENV_ALLOWLIST[key] === true && typeof value === "string") safe[key] = value;
  }
  if (safe.OCTO_BOT_ID !== undefined && safe.OCTO_BOT_ID !== botUid) {
    throw new Error("OMP CLI credentials belong to a different bot");
  }
  return Object.keys(safe).length === 0 ? undefined : safe;
}
function pathsFor(stateDir: string, botUid: string): RuntimePaths {
  const canonicalStateDir = resolve(stateDir);
  const digest = createHash("sha256").update(canonicalStateDir).update("\0").update(botUid).digest("hex").slice(0, 24);
  const runtimeDir = join(canonicalStateDir, "agent-runtime", digest);
  const socketDir = join(tmpdir(), `oco-agent-runtime-${process.getuid?.() ?? 0}-${digest}`);
  const socketPath = join(socketDir, "runtime.sock");
  return {
    runtimeDir,
    socketDir,
    socketPath,
    manifestPath: join(runtimeDir, "runtime.json"),
    ledgerPath: join(runtimeDir, "workers.json"),
    lockPath: join(runtimeDir, "runtime-lock.sqlite"),
    configPath: join(runtimeDir, "daemon.json"),
  };
}

function atomicWriteJson(path: string, value: unknown): void {
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  const fd = openSync(temporary, "wx", 0o600);
  try {
    writeFileSync(fd, `${JSON.stringify(value)}\n`, "utf8");
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temporary, path);
}

function writeLine(socket: Socket, value: unknown): void {
  if (socket.destroyed || !socket.writable) return;
  socket.write(`${JSON.stringify(value)}\n`);
}

function parseJsonLine(line: string): WireMessage {
  const parsed: unknown = JSON.parse(line);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Invalid runtime protocol frame");
  return parsed as WireMessage;
}

function toolDescriptor(tool: HostTool): { name: string; description: string; parameters: Record<string, unknown> } {
  return { name: tool.name, description: tool.description, parameters: tool.parameters };
}

function promptContextForWire(context: PromptContext | undefined): AgentRuntimePromptContext | undefined {
  if (!context) return undefined;
  return {
    ...(context.contextInjection === undefined ? {} : { contextInjection: context.contextInjection }),
    ...(context.tools === undefined ? {} : { tools: context.tools.map(toolDescriptor) }),
  };
}

function fingerprint(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}
function hashHostToolResult(result: string): string {
  return createHash("sha256").update(result).digest("hex");
}

function dispatchSnapshot(dispatch: StoredDispatch): AgentRuntimeDispatchSnapshot {
  return {
    dispatchId: dispatch.dispatchId,
    kind: dispatch.kind,
    state: dispatch.state,
    nextSeq: dispatch.nextSeq,
    ...(dispatch.result === undefined ? {} : { result: dispatch.result }),
    ...(dispatch.error === undefined ? {} : { error: dispatch.error }),
  };
}

function workerSnapshot(record: StoredWorker): AgentRuntimeWorkerSnapshot {
  return {
    botUid: record.botUid,
    taskId: record.taskId,
    runId: record.runId,
    runtimeId: record.runtimeId,
    sessionId: record.sessionId,
    sessionFile: record.sessionFile,
    process: record.process,
    stage: record.stage,
    activeDispatchId: record.activeDispatchId,
    dispatches: Object.values(record.dispatchMap).map(dispatchSnapshot),
    ...(record.startupFailure === undefined ? {} : { startupFailure: record.startupFailure }),
    ...(record.startupCwd === undefined ? {} : { startupCwd: record.startupCwd }),
    ...(record.startupFenced === undefined ? {} : { startupFenced: record.startupFenced }),
  };
}

function errorText(error: unknown): string {
  if (error instanceof WorkerGoalError) return `${error.code}: ${error.message}`;
  if (error instanceof WorkerSteerRejectedError) return error.message;
  return error instanceof Error ? error.message : String(error);
}

function errorFromText(text: string): Error {
  const goal = text.match(/^(unsupported|disabled|already-exists|already-active|identity-mismatch|invalid-request|invalid-response|continuation-disabled|native-error): (.*)$/u);
  if (goal) return new WorkerGoalError(goal[1] as ConstructorParameters<typeof WorkerGoalError>[0], goal[2] ?? "OMP goal RPC failed");
  if (text.startsWith("steer-rejected: ")) return new WorkerSteerRejectedError(text.slice("steer-rejected: ".length));
  return new Error(text);
}

function assertObject(value: unknown, name: string): asserts value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} is invalid`);
}
function assertStoredWorker(value: unknown, botUid: string): asserts value is StoredWorker {
  assertObject(value, "Agent runtime ledger worker");
  if (
    value.botUid !== botUid
    || typeof value.taskId !== "string"
    || typeof value.runId !== "string"
    || typeof value.runtimeId !== "string"
    || typeof value.sessionId !== "string"
    || typeof value.sessionFile !== "string"
    || !["spawn-intent", "not-spawned", "identity-verified", "omp-ready", "active", "stopping", "exited-confirmed", "unknown"].includes(String(value.stage))
    || (value.activeDispatchId !== null && typeof value.activeDispatchId !== "string")
  ) {
    throw new Error("Agent runtime ledger worker identity or state is invalid");
  }
  safeId(value.taskId, "taskId");
  safeId(value.runId, "runId");
  if (value.sessionKeys !== undefined && (!Array.isArray(value.sessionKeys) || value.sessionKeys.some(key => typeof key !== "string"))) {
    throw new Error("Agent runtime ledger session ownership is invalid");
  }
  if (value.process !== null) {
    assertObject(value.process, "Agent runtime ledger process identity");
    const processInfo = value.process;
    if (
      typeof processInfo.pid !== "number"
      || !Number.isSafeInteger(processInfo.pid)
      || processInfo.pid <= 0
      || typeof processInfo.pgid !== "number"
      || !Number.isSafeInteger(processInfo.pgid)
      || processInfo.pgid <= 0
      || typeof processInfo.identity !== "string"
      || processInfo.identity.length === 0
      || typeof processInfo.cwd !== "string"
      || !isAbsolute(processInfo.cwd)
    ) {
      throw new Error("Agent runtime ledger process identity is invalid");
    }
  }
  assertObject(value.dispatchMap, "Agent runtime ledger dispatch map");
  for (const [dispatchId, entry] of Object.entries(value.dispatchMap)) {
    safeId(dispatchId, "dispatchId");
    assertObject(entry, "Agent runtime ledger dispatch");
    if (
      entry.dispatchId !== dispatchId
      || !["prompt", "resume", "goal"].includes(String(entry.kind))
      || !["active", "completed", "failed", "unknown"].includes(String(entry.state))
      || typeof entry.nextSeq !== "number"
      || !Number.isSafeInteger(entry.nextSeq)
      || entry.nextSeq < 1
      || typeof entry.fingerprint !== "string"
      || !Array.isArray(entry.events)
    ) {
      throw new Error("Agent runtime ledger dispatch state is invalid");
    }
  }
  if (value.startupFailure !== undefined) {
    const startupFailure = value.startupFailure;
    assertObject(startupFailure, "Agent runtime ledger startup failure");
    if (
      typeof startupFailure.code !== "string"
      || startupFailure.code.length > 64
      || (startupFailure.diagnostic !== undefined
        && (typeof startupFailure.diagnostic !== "string" || startupFailure.diagnostic.length > 256))
    ) {
      throw new Error("Agent runtime ledger startup failure is invalid");
    }
  }
  if (value.stage === "not-spawned") {
    assertObject(value.startupFailure, "Agent runtime pre-spawn failure");
    if (value.process !== null || typeof value.startupCwd !== "string" || !isAbsolute(value.startupCwd)
      || value.activeDispatchId !== null || Object.keys(value.dispatchMap).length !== 0
      || !["spawn-failed", "cwd-missing", "session-dir-missing"].includes(String(value.startupFailure.code))) {
      throw new Error("Agent runtime pre-spawn proof is invalid");
    }
  }
  if (value.startupFenced === true && (
    (value.stage !== "not-spawned" && value.stage !== "exited-confirmed")
    || typeof value.startupCwd !== "string" || !isAbsolute(value.startupCwd)
    || value.startupFailure === undefined || value.activeDispatchId !== null
    || Object.keys(value.dispatchMap).length !== 0
    || (value.stage === "not-spawned" && value.process !== null)
  )) throw new Error("Agent runtime startup fence is invalid");
}


function isTerminal(state: AgentRuntimeDispatchState): boolean {
  return state === "completed" || state === "failed" || state === "unknown";
}

export async function startAgentRuntimeServer(options: AgentRuntimeServerOptions): Promise<AgentRuntimeServer> {
  const server = new AgentRuntimeServer(options);
  await server.start();
  return server;
}

export class AgentRuntimeServer {
  readonly runtimeId = randomUUID();
  readonly paths: RuntimePaths;
  readonly botUid: string;
  readonly secret = randomBytes(32).toString("base64url");
  readonly #options: AgentRuntimeServerOptions;
  readonly #factory: WorkerFactory;
  readonly #workers = new Map<string, WorkerRecord>();
  readonly #sessionOwners = new Map<string, string>();
  readonly #connections = new Set<RuntimeConnection>();
  readonly #server: Server;
  #lockDb: Database | undefined;
  #current: RuntimeConnection | undefined;
  #manifest: RuntimeManifest | undefined;
  #closed = false;
  #closePromise: Promise<void> | undefined;

  #cliEnv: Record<string, string> | undefined;

  constructor(options: AgentRuntimeServerOptions) {
    this.#options = options;
    this.botUid = safeId(options.botUid, "botUid");
    this.paths = pathsFor(options.stateDir, this.botUid);
    this.#cliEnv = safeCliEnv(options.cliEnv, this.botUid);
    this.#factory = createWorkerFactory({
      ompCommand: options.ompCommand,
      extraArgs: [...(options.ompExtraArgs ?? [])],
      ...(options.startupTimeoutMs === undefined ? {} : { startupTimeoutMs: options.startupTimeoutMs }),
      ...(options.cancelTimeoutMs === undefined ? {} : { cancelTimeoutMs: options.cancelTimeoutMs }),
      cliEnv: () => this.#cliEnv,
    });
    this.#server = createServer(socket => this.#onConnection(socket));
  }

  get pid(): number {
    return process.pid;
  }

  get socketPath(): string {
    return this.#options.socketPath ?? this.paths.socketPath;
  }

  get closed(): boolean {
    return this.#closed;
  }

  async start(): Promise<void> {
    const paths = this.paths;
    await mkdir(paths.runtimeDir, { recursive: true, mode: 0o700 });
    await chmod(paths.runtimeDir, 0o700);
    const socketDir = dirname(this.socketPath);
    await mkdir(socketDir, { recursive: true, mode: 0o700 });
    await chmod(socketDir, 0o700);
    const previous = await readManifestIfPresent(this.#options.manifestPath ?? paths.manifestPath);
    if (previous) {
      if (previous.botUid !== this.botUid) throw new Error("Agent runtime manifest belongs to another Bot");
      const actual = await queryProcess(previous.pid);
      if (actual && actual.identity === previous.processIdentity) {
        throw new Error("Agent runtime is already active");
      }
    }
    this.#acquireRuntimeLock();
    await this.#recoverPreviousRuntime(previous !== undefined);
    await rm(this.socketPath, { force: true });
    const processSnapshot = await queryProcess(process.pid);
    if (!processSnapshot) throw new Error("Could not verify agent runtime process identity");
    this.#manifest = {
      protocolVersion: AGENT_RUNTIME_PROTOCOL_VERSION,
      botUid: this.botUid,
      runtimeId: this.runtimeId,
      pid: process.pid,
      processIdentity: processSnapshot.identity,
      socketPath: this.socketPath,
      secret: this.secret,
      startedAt: Date.now(),
    };
    atomicWriteJson(this.#options.manifestPath ?? paths.manifestPath, this.#manifest);
    this.#persistLedger();
    await new Promise<void>((resolvePromise, rejectPromise) => {
      this.#server.once("error", rejectPromise);
      this.#server.listen(this.socketPath, () => {
        this.#server.off("error", rejectPromise);
        resolvePromise();
      });
    });
    await chmod(this.socketPath, 0o600);
  }

  async shutdown(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    this.#closePromise = (async () => {
      for (const record of this.#workers.values()) {
        if (!record.worker || record.snapshot.stage === "exited-confirmed") continue;
        record.snapshot.stage = "stopping";
        this.#persistLedger();
        const stopped = await record.worker.stop().catch(() => false);
        if (stopped) this.#markExited(record);
        else {
          record.snapshot.stage = "unknown";
          this.#markActiveRunsUnknown(record);
          this.#persistLedger();
        }
      }
      for (const connection of this.#connections) connection.socket.destroy();
      this.#connections.clear();
      await new Promise<void>(resolvePromise => this.#server.close(() => resolvePromise()));
      await rm(this.socketPath, { force: true }).catch(() => {});
      if (this.#manifest) {
        const current = await this.#readManifest().catch(() => undefined);
        if (current?.runtimeId === this.runtimeId) await rm(this.#options.manifestPath ?? this.paths.manifestPath, { force: true });
      }
      this.#lockDb?.exec("COMMIT");
      this.#lockDb?.close();
      this.#lockDb = undefined;
    })();
    return this.#closePromise;
  }

  #acquireRuntimeLock(): void {
    this.#lockDb = new Database(this.paths.lockPath);
    this.#lockDb.exec("PRAGMA busy_timeout = 100; BEGIN EXCLUSIVE");
  }

  async #recoverPreviousRuntime(requireLedger: boolean): Promise<void> {
    const path = this.#options.ledgerPath ?? this.paths.ledgerPath;
    let stored: StoredWorker[] = [];
    try {
      const value: unknown = JSON.parse(await readFile(path, "utf8"));
      if (!Array.isArray(value)) throw new Error("Agent runtime ledger is invalid");
      stored = [];
      for (const entry of value) {
        assertStoredWorker(entry, this.botUid);
        stored.push(entry);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        if (requireLedger) throw new Error("Previous runtime manifest exists but worker ownership ledger is missing");
      } else {
        throw error;
      }
    }
    for (const previous of stored) {
      const record: WorkerRecord = {
        snapshot: { ...previous, runtimeId: this.runtimeId, sessionKeys: [...(previous.sessionKeys ?? [])] },
        calls: new Map(),
        cancelledCalls: new Map(),
        acceptedToolResults: new Map(),
        hostToolResultHashes: new Map(),
      };
      if (record.snapshot.sessionKeys.length === 0) {
        if (record.snapshot.sessionFile) record.snapshot.sessionKeys.push(`file:${resolve(record.snapshot.sessionFile)}`);
        if (record.snapshot.sessionId) record.snapshot.sessionKeys.push(`session:${record.snapshot.sessionId}`);
      }
      if (!record.snapshot.process && record.snapshot.stage !== "not-spawned") {
        record.snapshot.stage = "unknown";
      } else if (record.snapshot.process && record.snapshot.stage !== "exited-confirmed") {
        const stopped = await this.#factory.recover(record.snapshot.process).catch(() => false);
        record.snapshot.stage = stopped ? "exited-confirmed" : "unknown";
      }
      for (const dispatch of Object.values(record.snapshot.dispatchMap)) {
        if (dispatch.state === "active") {
          dispatch.state = "unknown";
          dispatch.error = "Agent runtime exited; task outcome is unknown and requires cold recovery";
        }
      }
      if (record.snapshot.stage === "unknown") {
        for (const sessionKey of record.snapshot.sessionKeys) this.#sessionOwners.set(sessionKey, this.#workerKey(record.snapshot.taskId, record.snapshot.runId));
      }
      record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
      if (Object.values(record.snapshot.dispatchMap).some(dispatch => dispatch.state === "unknown")) {
        record.snapshot.activeDispatchId = null;
      }
      this.#workers.set(this.#workerKey(record.snapshot.taskId, record.snapshot.runId), record);
    }
    this.#persistLedger();
  }

  #readManifest(): Promise<RuntimeManifest> {
    return readFile(this.#options.manifestPath ?? this.paths.manifestPath, "utf8").then(text => JSON.parse(text) as RuntimeManifest);
  }

  #persistLedger(): void {
    const path = this.#options.ledgerPath ?? this.paths.ledgerPath;
    const stored = [...this.#workers.values()].map(record => ({
      ...record.snapshot,
      dispatchMap: Object.fromEntries(Object.entries(record.snapshot.dispatchMap).map(([id, dispatch]) => [
        id,
        { ...dispatch, events: [] },
      ])),
    }));
    atomicWriteJson(path, stored);
  }

  #workerKey(taskId: string, runId: string): string {
    return `${taskId}\0${runId}`;
  }

  #send(connection: RuntimeConnection, message: unknown): void {
    const current = this.#current;
    if (connection !== current || connection.epoch !== current?.epoch) return;
    writeLine(connection.socket, message);
  }

  #sendEvent(event: RuntimeEvent): void {
    const connection = this.#current;
    if (!connection) return;
    if (event.event === "run-event" && event.dispatchId && typeof event.seq === "number") {
      const key = `${event.taskId}\0${event.runId}\0${event.dispatchId}`;
      const cursor = connection.watchers.get(key);
      if (cursor === undefined || event.seq <= cursor) return;
      connection.watchers.set(key, event.seq);
    }
    this.#send(connection, { kind: "event", epoch: connection.epoch, ...event });
  }

  #onConnection(socket: Socket): void {
    const connection: RuntimeConnection = { socket, epoch: 0, buffer: "", watchers: new Map(), requests: new Map() };
    this.#connections.add(connection);
    socket.setEncoding("utf8");
    let welcomed = false;
    const onData = (chunk: string): void => {
      connection.buffer += chunk;
      if (Buffer.byteLength(connection.buffer, "utf8") > MAX_FRAME_BYTES) {
        socket.destroy(new Error("Agent runtime frame exceeds limit"));
        return;
      }
      for (;;) {
        const newline = connection.buffer.indexOf("\n");
        if (newline < 0) break;
        const line = connection.buffer.slice(0, newline);
        connection.buffer = connection.buffer.slice(newline + 1);
        if (!line) continue;
        try {
          const message = parseJsonLine(line);
          if (!welcomed) {
            if (message.kind !== "hello" || message.version !== AGENT_RUNTIME_PROTOCOL_VERSION || message.botUid !== this.botUid || message.secret !== this.secret) {
              socket.destroy(new Error("Agent runtime authentication failed"));
              return;
            }
            welcomed = true;
            const previous = this.#current;
            const epoch = (previous?.epoch ?? this.#epoch) + 1;
            this.#epoch = epoch;
            connection.epoch = epoch;
            this.#current = connection;
            if (previous && previous !== connection) previous.socket.destroy();
            writeLine(socket, {
              kind: "welcome",
              version: AGENT_RUNTIME_PROTOCOL_VERSION,
              runtimeId: this.runtimeId,
              epoch,
              pid: process.pid,
              workers: [...this.#workers.values()].map(record => workerSnapshot(record.snapshot)),
            });
            continue;
          }
          if (message.kind !== "request" || message.epoch !== connection.epoch || connection !== this.#current) continue;
          const id = typeof message.id === "string" ? message.id : "";
          const operation = typeof message.operation === "string" ? message.operation : "";
          if (!id || !operation) continue;
          void this.#handleRequest(connection, id, operation, message.args);
        } catch {
          socket.destroy(new Error("Invalid agent runtime protocol frame"));
          return;
        }
      }
    };
    socket.on("data", onData);
    socket.on("close", () => {
      this.#connections.delete(connection);
      if (this.#current === connection) this.#current = undefined;
    });
    socket.on("error", () => {});
  }

  #epoch = 0;

  async #handleRequest(connection: RuntimeConnection, id: string, operation: string, args: unknown): Promise<void> {
    const controller = new AbortController();
    connection.requests.set(id, controller);
    try {
      const value = await this.#operate(connection, operation, args, controller.signal);
      this.#send(connection, { kind: "response", id, ok: true, value });
    } catch (error) {
      this.#send(connection, {
        kind: "response", id, ok: false, error: errorText(error),
        ...(error instanceof TaskStartupError
          ? { startupFailure: { code: error.code, diagnostic: error.diagnostic } } : {}),
      });
    } finally {
      connection.requests.delete(id);
    }
  }

  async #operate(connection: RuntimeConnection, operation: string, args: unknown, signal: AbortSignal): Promise<unknown> {
    assertObject(args, "request arguments");
    if (operation === "list-workers") {
      await this.#refreshProcessStates();
      return [...this.#workers.values()].map(record => workerSnapshot(record.snapshot));
    }
    if (operation === "create-worker") return this.#createWorker(args as unknown as CreateWorkerInput, signal);
    if (operation === "attach-worker") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      await this.#assertLiveIdentity(record);
      this.#resendPendingCalls(record);
      this.#resendCancelledCalls(record);
      this.#resendAcceptedToolResults(record);
      return workerSnapshot(record.snapshot);
    }
    if (operation === "detach-worker") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      // Detach only drops the channel attachment. In-flight host/ask calls stay pending and are resent by attach-worker.
      return { detached: true, stage: record.snapshot.stage };
    }
    if (operation === "dispatch") return this.#dispatch(args as unknown as { taskId: string; runId: string; dispatch: ServerDispatchInput });
    if (operation === "watch-run") {
      const input = args as unknown as { taskId: string; runId: string; dispatchId: string; fromSeq: number };
      if (!Number.isSafeInteger(input.fromSeq) || input.fromSeq < 0) throw new Error("fromSeq must be a non-negative safe integer");
      const record = this.#getRecord(input);
      const dispatch = record.snapshot.dispatchMap[input.dispatchId];
      if (!dispatch) throw new Error("Agent runtime dispatch does not exist");
      const key = `${input.taskId}\0${input.runId}\0${input.dispatchId}`;
      connection.watchers.set(key, input.fromSeq);
      for (let index = input.fromSeq; index < dispatch.events.length; index++) {
        const seq = index + 1;
        this.#sendEvent({ event: "run-event", taskId: input.taskId, runId: input.runId, dispatchId: input.dispatchId, seq, value: dispatch.events[index] });
      }
      if (isTerminal(dispatch.state)) {
        this.#sendEvent({ event: "run-terminal", taskId: input.taskId, runId: input.runId, dispatchId: input.dispatchId, value: dispatchSnapshot(dispatch) });
      }
      return dispatchSnapshot(dispatch);
    }
    if (operation === "run-state") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      const dispatch = record.snapshot.dispatchMap[String(args.dispatchId)];
      if (!dispatch) throw new Error("Agent runtime dispatch does not exist");
      return dispatchSnapshot(dispatch);
    }
    if (operation === "host-tool-result") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      const callId = String(args.callId);
      const call = record.calls.get(callId);
      if (!call || call.settled) {
        record.cancelledCalls.delete(callId);
        return { accepted: false };
      }
      const isError = args.isError === true;
      const result = String(args.result ?? (isError ? "Host tool failed" : ""));
      this.#rememberHostToolResult(record, call.dispatchId, call.toolName, call.toolCallId, result);
      call.settled = true;
      if (isError) call.reject(new Error(result));
      else call.resolve(result);
      record.calls.delete(callId);
      record.cancelledCalls.delete(callId);
      return { accepted: true };
    }
    if (operation === "host-tool-cancel-ack") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      const callId = String(args.callId ?? "");
      const call = record.cancelledCalls.get(callId);
      if (!call) return { accepted: false };
      if (call.dispatchId !== String(args.dispatchId ?? "")) throw new Error("Host tool cancellation identity does not match");
      record.cancelledCalls.delete(callId);
      return { accepted: true };
    }
    if (operation === "host-tool-result-accepted-ack") {
      const record = this.#getRecord(args as unknown as AttachWorkerInput);
      const ackId = String(args.ackId ?? "");
      const accepted = record.acceptedToolResults.get(ackId);
      if (!accepted) return { accepted: false };
      if (
        accepted.dispatchId !== String(args.dispatchId ?? "")
        || accepted.toolCallId !== String(args.toolCallId ?? "")
        || accepted.resultHash !== String(args.resultHash ?? "")
      ) throw new Error("Host tool result acceptance identity does not match");
      record.acceptedToolResults.delete(ackId);
      return { accepted: true };
    }
    if (operation === "worker-operation") {
      return this.#workerOperation(args as unknown as { taskId: string; runId: string; method: string; args: unknown[] }, signal);
    }
    if (operation === "stop-worker") return this.#stopWorker(args as unknown as AttachWorkerInput);
    if (operation === "cancel-run") {
      const input = args as unknown as { taskId: string; runId: string; dispatchId: string };
      const record = this.#getRecord(input);
      const dispatch = record.snapshot.dispatchMap[safeId(input.dispatchId, "dispatchId")];
      if (!dispatch || dispatch.state !== "active" || record.snapshot.activeDispatchId !== dispatch.dispatchId || !record.worker) {
        return { cancelled: false };
      }
      await record.worker.abortTurn();
      return { cancelled: true };
    }
    if (operation === "update-cli-env") {
      this.#cliEnv = safeCliEnv(args.cliEnv as Readonly<Record<string, string>> | undefined, this.botUid);
      return { updated: true };
    }
    if (operation === "cancel-request") {
      const targetId = String(args.targetRequestId ?? "");
      const target = connection.requests.get(targetId);
      target?.abort(new Error("Agent runtime operation cancelled"));
      return { cancelled: target !== undefined };
    }
    if (operation === "shutdown") {
      queueMicrotask(() => void this.shutdown());
      return { shuttingDown: true };
    }
    throw new Error(`Unknown agent runtime operation: ${operation}`);
  }


  async #createWorker(input: CreateWorkerInput, signal?: AbortSignal): Promise<AgentRuntimeWorkerSnapshot> {
    const taskId = safeId(input.taskId, "taskId");
    const runId = safeId(input.runId, "runId");
    const key = this.#workerKey(taskId, runId);
    if (this.#workers.has(key)) throw new Error("Agent runtime run already exists");
    if ([...this.#workers.values()].some(record => record.snapshot.taskId === taskId && record.snapshot.startupFenced)) {
      throw new Error("Agent runtime task startup is already settled");
    }
    const options = input.workerOptions;
    if (!options || !options.cwd || !options.sessionDir) throw new Error("Agent runtime worker options are invalid");
    if (options.contextScope && options.contextScope.botUid !== this.botUid) throw new Error("Worker bot identity does not match runtime identity");
    const sessionKeys = options.sessionFile ? [`file:${resolve(options.sessionFile)}`] : [];
    if (sessionKeys.some(sessionKey => this.#sessionOwners.has(sessionKey))) throw new Error(SESSION_BUSY_ERROR);
    const snapshot: StoredWorker = {
      botUid: this.botUid,
      taskId,
      runId,
      runtimeId: this.runtimeId,
      sessionId: "",
      sessionFile: options.sessionFile ?? "",
      process: null,
      stage: "spawn-intent",
      activeDispatchId: null,
      dispatches: [],
      startupCwd: options.cwd,
      sessionKeys,
      dispatchMap: {},
    };
    const record: WorkerRecord = {
      snapshot,
      calls: new Map(),
      cancelledCalls: new Map(),
      acceptedToolResults: new Map(),
      hostToolResultHashes: new Map(),
    };
    this.#workers.set(key, record);
    for (const sessionKey of sessionKeys) this.#sessionOwners.set(sessionKey, key);
    this.#persistLedger();
    try {
      const worker = await this.#factory.create({
        ...options,
        ...(signal === undefined ? {} : { signal }),
        onSpawn: processInfo => {
          snapshot.process = processInfo;
          snapshot.stage = "identity-verified";
          this.#persistLedger();
          this.#sendEvent({ event: "worker-spawn", taskId, runId, value: processInfo });
        },
      } as WorkerOptions);
      record.worker = worker;
      snapshot.sessionId = worker.sessionId;
      snapshot.sessionFile = worker.sessionFile;
      const observedKeys = [
        ...(snapshot.sessionFile ? [`file:${resolve(snapshot.sessionFile)}`] : []),
        ...(snapshot.sessionId ? [`session:${snapshot.sessionId}`] : []),
      ];
      if (observedKeys.some(sessionKey => {
        const owner = this.#sessionOwners.get(sessionKey);
        return owner !== undefined && owner !== key;
      })) throw new Error(SESSION_BUSY_ERROR);
      snapshot.sessionKeys = [...new Set([...sessionKeys, ...observedKeys])];
      for (const sessionKey of snapshot.sessionKeys) this.#sessionOwners.set(sessionKey, key);
      if (!snapshot.process) throw new Error("OMP worker started without a verified process identity");
      snapshot.stage = "omp-ready";
      this.#persistLedger();
      return workerSnapshot(snapshot);
    } catch (error) {
      if (error instanceof TaskStartupError) {
        snapshot.startupFailure = {
          code: error.code,
          ...(error.diagnostic === undefined ? {} : { diagnostic: error.diagnostic.slice(0, 256) }),
        };
      }
      if (record.worker) {
        const stopped = await record.worker.stop().catch(() => false);
        if (stopped) this.#markExited(record);
        else snapshot.stage = "unknown";
      } else if (snapshot.process) {
        const stopped = await this.#factory.recover(snapshot.process).catch(() => false);
        if (stopped) this.#markExited(record);
        else snapshot.stage = "unknown";
      } else if (input.fenceStartupFailure && error instanceof TaskStartupError
        && (error.code === "spawn-failed" || error.code === "cwd-missing" || error.code === "session-dir-missing")) {
        snapshot.stage = "not-spawned";
        this.#releaseSession(record);
      } else {
        snapshot.stage = "unknown";
      }
      if (input.fenceStartupFailure && snapshot.startupFailure !== undefined
        && (snapshot.stage === "not-spawned" || snapshot.stage === "exited-confirmed")
        && ![...this.#workers.values()].some(other => other !== record && other.snapshot.taskId === taskId)) {
        snapshot.startupFenced = true;
      }
      this.#persistLedger();
      throw error;
    }
  }

  #getRecord(input: { taskId: string; runId: string; sessionId?: string }): WorkerRecord {
    const record = this.#workers.get(this.#workerKey(safeId(input.taskId, "taskId"), safeId(input.runId, "runId")));
    if (!record || record.snapshot.botUid !== this.botUid || record.snapshot.runtimeId !== this.runtimeId) {
      throw new Error("Agent runtime worker identity does not match");
    }
    if (input.sessionId !== undefined && record.snapshot.sessionId !== input.sessionId) throw new Error("Agent runtime session identity does not match");
    return record;
  }

  async #assertLiveIdentity(record: WorkerRecord): Promise<void> {
    if (record.snapshot.stage === "exited-confirmed") return;
    if (record.snapshot.stage === "unknown" || !record.snapshot.process || !record.worker) {
      throw new Error("Agent runtime worker identity is unverified");
    }
    const actual = await queryProcess(record.snapshot.process.pid);
    if (actual && actual.identity === record.snapshot.process.identity && actual.pgid === record.snapshot.process.pgid) return;
    const recovered = await this.#factory.recover(record.snapshot.process).catch(() => false);
    record.snapshot.stage = recovered ? "exited-confirmed" : "unknown";
    this.#markActiveRunsUnknown(record);
    if (recovered) this.#releaseSession(record);
    this.#persistLedger();
    throw new Error(recovered ? "Agent runtime worker has exited; cold recovery is required" : "Agent runtime worker process identity is unknown");
  }

  async #refreshProcessStates(): Promise<void> {
    for (const record of this.#workers.values()) {
      if (record.snapshot.stage !== "active" && record.snapshot.stage !== "omp-ready") continue;
      if (!record.worker || !record.snapshot.process) continue;
      const actual = await queryProcess(record.snapshot.process.pid);
      if (actual && actual.identity === record.snapshot.process.identity && actual.pgid === record.snapshot.process.pgid) continue;
      const stopped = await this.#factory.recover(record.snapshot.process).catch(() => false);
      record.snapshot.stage = stopped ? "exited-confirmed" : "unknown";
      this.#markActiveRunsUnknown(record);
      if (stopped) this.#releaseSession(record);
      this.#persistLedger();
    }
  }

  #markExited(record: WorkerRecord): void {
    record.snapshot.stage = "exited-confirmed";
    delete record.worker;
    this.#releaseSession(record);
    for (const call of record.calls.values()) {
      call.settled = true;
      call.reject(new Error("OMP worker stopped before the host tool completed"));
    }
    record.calls.clear();
    for (const dispatch of Object.values(record.snapshot.dispatchMap)) {
      if (dispatch.state === "active") {
        dispatch.state = "failed";
        dispatch.error = "OMP worker stopped while a dispatch was active";
      }
    }
    record.snapshot.activeDispatchId = null;
    record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
    this.#persistLedger();
  }

  #markActiveRunsUnknown(record: WorkerRecord): void {
    for (const dispatch of Object.values(record.snapshot.dispatchMap)) {
      if (dispatch.state === "active") {
        dispatch.state = "unknown";
        dispatch.error = "OMP worker process identity could not be confirmed";
      }
    }
    record.snapshot.activeDispatchId = null;
    record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
  }

  #releaseSession(record: WorkerRecord): void {
    for (const key of record.snapshot.sessionKeys) {
      if (this.#sessionOwners.get(key) === this.#workerKey(record.snapshot.taskId, record.snapshot.runId)) this.#sessionOwners.delete(key);
    }
  }

  async #stopWorker(input: AttachWorkerInput): Promise<boolean> {
    const record = this.#getRecord(input);
    if (!record.worker) return record.snapshot.stage === "exited-confirmed";
    record.snapshot.stage = "stopping";
    this.#persistLedger();
    const stopped = await record.worker.stop();
    if (stopped) this.#markExited(record);
    else {
      record.snapshot.stage = "unknown";
      this.#markActiveRunsUnknown(record);
      this.#persistLedger();
    }
    return stopped;
  }

  async #dispatch(input: { taskId: string; runId: string; dispatch: ServerDispatchInput }): Promise<AgentRunState> {
    const record = this.#getRecord(input);
    if (record.snapshot.stage !== "omp-ready" && record.snapshot.stage !== "active") throw new Error("Agent runtime worker is not ready");
    const dispatchInput = input.dispatch;
    const dispatchId = safeId(dispatchInput.dispatchId, "dispatchId");
    const prior = record.snapshot.dispatchMap[dispatchId];
    const digest = fingerprint(dispatchInput);
    if (prior) {
      if (prior.fingerprint !== digest) throw new Error("Agent runtime dispatch ID was reused with different input");
      return dispatchSnapshot(prior) as AgentRunState;
    }
    if (record.snapshot.activeDispatchId !== null) throw new Error("OMP session already has an active dispatch");
    if (!record.worker) throw new Error("Agent runtime worker is not available");
    if (dispatchInput.kind === "prompt" && typeof dispatchInput.text !== "string") throw new Error("Prompt text is required");
    if (dispatchInput.kind === "goal" && !dispatchInput.goal) throw new Error("Goal operation is required");
    const dispatch: StoredDispatch = {
      dispatchId,
      kind: dispatchInput.kind,
      state: "active",
      nextSeq: 1,
      events: [],
      fingerprint: digest,
    };
    record.snapshot.dispatchMap[dispatchId] = dispatch;
    record.snapshot.activeDispatchId = dispatchId;
    record.snapshot.stage = "active";
    record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
    this.#persistLedger();
    void this.#executeDispatch(record, dispatch, dispatchInput);
    return dispatchSnapshot(dispatch) as AgentRunState;
  }

  async #executeDispatch(record: WorkerRecord, dispatch: StoredDispatch, input: ServerDispatchInput): Promise<void> {
    const worker = record.worker;
    if (!worker) return;
    const onEvent = (event: WorkerEvent): void => {
      const seq = dispatch.events.length + 1;
      dispatch.events.push(event);
      dispatch.nextSeq = seq + 1;
      record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
      this.#sendEvent({ event: "run-event", taskId: record.snapshot.taskId, runId: record.snapshot.runId, dispatchId: dispatch.dispatchId, seq, value: event });
    };
    const context = this.#contextWithHostBridge(record, dispatch, input.context);
    try {
      let result: unknown;
      switch (input.kind) {
        case "prompt":
          await worker.prompt(input.text!, onEvent, context);
          break;
        case "resume":
          await worker.resume(onEvent, context);
          break;
        case "goal":
          result = await worker.runGoal(input.goal!, onEvent, context);
          break;
      }
      dispatch.state = "completed";
      if (result !== undefined) dispatch.result = result;
    } catch (error) {
      dispatch.state = "failed";
      dispatch.error = errorText(error);
    } finally {
      if (record.snapshot.activeDispatchId === dispatch.dispatchId) record.snapshot.activeDispatchId = null;
      record.hostToolResultHashes.delete(dispatch.dispatchId);
      if (record.snapshot.stage === "active") record.snapshot.stage = "omp-ready";
      record.snapshot.dispatches = Object.values(record.snapshot.dispatchMap).map(dispatchSnapshot);
      this.#persistLedger();
      this.#sendEvent({ event: "run-terminal", taskId: record.snapshot.taskId, runId: record.snapshot.runId, dispatchId: dispatch.dispatchId, value: dispatchSnapshot(dispatch) });
    }
  }


  #contextWithHostBridge(record: WorkerRecord, dispatch: StoredDispatch, context: AgentRuntimePromptContext | undefined): PromptContext | undefined {
    if (!context) return undefined;
    const tools: HostTool[] | undefined = context.tools?.map(descriptor => ({
      ...descriptor,
      execute: (args, signal, toolCallId) => this.#callHostTool(record, dispatch, descriptor.name, args, signal, toolCallId),
      onResultAccepted: (toolCallId, acceptedHash) => {
        const resultHashes = record.hostToolResultHashes.get(dispatch.dispatchId);
        if (!resultHashes) return;
        const resultKey = JSON.stringify([descriptor.name, toolCallId]);
        const hashes = resultHashes.get(resultKey);
        if (!hashes) return;
        // Within this dispatch/tool/call identity, identical result digests are interchangeable.
        const matchIndex = hashes.indexOf(acceptedHash);
        if (matchIndex === -1) return;
        hashes.splice(matchIndex, 1);
        if (hashes.length === 0) resultHashes.delete(resultKey);
        if (resultHashes.size === 0) record.hostToolResultHashes.delete(dispatch.dispatchId);
        const resultHash = acceptedHash;
        const accepted: AcceptedHostToolResult = {
          ackId: randomUUID(),
          dispatchId: dispatch.dispatchId,
          toolName: descriptor.name,
          toolCallId,
          resultHash,
        };
        record.acceptedToolResults.set(accepted.ackId, accepted);
        this.#sendAcceptedToolResult(record, accepted);
      },
    }));
    return {
      ...(context.contextInjection === undefined ? {} : { contextInjection: context.contextInjection }),
      ...(tools === undefined ? {} : { tools }),
    };
  }
  #rememberHostToolResult(
    record: WorkerRecord,
    dispatchId: string,
    toolName: string,
    toolCallId: string | undefined,
    result: string,
  ): void {
    if (toolCallId === undefined) return;
    let hashesByCall = record.hostToolResultHashes.get(dispatchId);
    if (!hashesByCall) {
      hashesByCall = new Map();
      record.hostToolResultHashes.set(dispatchId, hashesByCall);
    }
    const key = JSON.stringify([toolName, toolCallId]);
    const hashes = hashesByCall.get(key) ?? [];
    hashes.push(hashHostToolResult(result));
    hashesByCall.set(key, hashes);
  }

  #callHostTool(
    record: WorkerRecord,
    dispatch: StoredDispatch,
    toolName: string,
    args: Record<string, unknown>,
    signal: AbortSignal,
    toolCallId?: string,
  ): Promise<string> {
    if (signal.aborted) {
      const error = signal.reason instanceof Error ? signal.reason : new Error("Host tool cancelled");
      this.#rememberHostToolResult(record, dispatch.dispatchId, toolName, toolCallId, error.message);
      return Promise.reject(error);
    }
    const callId = randomUUID();
    const pending = Promise.withResolvers<string>();
    const call: PendingHostCall = {
      callId,
      dispatchId: dispatch.dispatchId,
      toolName,
      args,
      ...(toolCallId ? { toolCallId } : {}),
      controller: new AbortController(),
      resolve: pending.resolve,
      reject: pending.reject,
      settled: false,
    };
    const onAbort = (): void => {
      if (call.settled) return;
      const error = signal.reason instanceof Error ? signal.reason : new Error("Host tool cancelled");
      call.controller.abort(error);
      record.cancelledCalls.set(callId, call);
      this.#sendCancelledHostCall(record, call);
      call.settled = true;
      this.#rememberHostToolResult(record, call.dispatchId, call.toolName, call.toolCallId, error.message);
      call.reject(error);
      record.calls.delete(callId);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    record.calls.set(callId, call);
    this.#sendHostCall(record, call);
    return pending.promise.finally(() => {
      signal.removeEventListener("abort", onAbort);
      record.calls.delete(callId);
    });
  }

  #sendHostCall(record: WorkerRecord, call: PendingHostCall): void {
    if (call.settled) return;
    this.#sendEvent({
      event: "host-tool-call",
      taskId: record.snapshot.taskId,
      runId: record.snapshot.runId,
      dispatchId: call.dispatchId,
      value: { callId: call.callId, toolName: call.toolName, args: call.args, ...(call.toolCallId ? { toolCallId: call.toolCallId } : {}), cancelled: call.controller.signal.aborted },
    });
  }

  #resendPendingCalls(record?: WorkerRecord): void {
    const records = record ? [record] : [...this.#workers.values()];
    for (const item of records) for (const call of item.calls.values()) this.#sendHostCall(item, call);
  }
  #resendCancelledCalls(record: WorkerRecord): void {
    for (const call of record.cancelledCalls.values()) this.#sendCancelledHostCall(record, call);
  }

  #sendCancelledHostCall(record: WorkerRecord, call: PendingHostCall): void {
    this.#sendEvent({
      event: "host-tool-cancel",
      taskId: record.snapshot.taskId,
      runId: record.snapshot.runId,
      dispatchId: call.dispatchId,
      value: { callId: call.callId },
    });
  }

  #resendAcceptedToolResults(record: WorkerRecord): void {
    for (const accepted of record.acceptedToolResults.values()) this.#sendAcceptedToolResult(record, accepted);
  }

  #sendAcceptedToolResult(record: WorkerRecord, accepted: AcceptedHostToolResult): void {
    this.#sendEvent({
      event: "host-tool-result-accepted",
      taskId: record.snapshot.taskId,
      runId: record.snapshot.runId,
      dispatchId: accepted.dispatchId,
      value: accepted,
    });
  }

  async #workerOperation(
    input: { taskId: string; runId: string; method: string; args: unknown[] },
    signal?: AbortSignal,
  ): Promise<unknown> {
    const record = this.#getRecord(input);
    if (!record.worker || record.snapshot.stage === "exited-confirmed" || record.snapshot.stage === "unknown") {
      throw new Error("Agent runtime worker is not available");
    }
    const worker = record.worker;
    const args = input.args ?? [];
    switch (input.method) {
      case "refreshState": return worker.refreshState();
      case "btw": return worker.btw(String(args[0]), signal);
      case "goal": return worker.goal(args[0] as "get" | "pause" | "drop");
      case "steer": return worker.steer(String(args[0]));
      case "abortTurn": return worker.abortTurn();
      case "contextUsage": return worker.contextUsage(signal);
      case "getSubagents": return worker.getSubagents?.(signal) ?? [];
      case "getSubagentMessages": return worker.getSubagentMessages?.(String(args[0]), signal);
      case "models": return worker.models();
      case "availableCommands": return worker.availableCommands?.(signal) ?? [];
      case "switchModel": return worker.switchModel(String(args[0]));
      default: throw new Error(`Unsupported OMP worker operation: ${input.method}`);
    }
  }
}

export async function ensureAgentRuntime(options: AgentRuntimeLaunchOptions): Promise<EnsureAgentRuntimeResult> {
  safeId(options.botUid, "botUid");
  const paths = pathsFor(options.stateDir, options.botUid);
  await mkdir(paths.runtimeDir, { recursive: true, mode: 0o700 });
  await chmod(paths.runtimeDir, 0o700);
  await mkdir(paths.socketDir, { recursive: true, mode: 0o700 });
  await chmod(paths.socketDir, 0o700);
  const connect = async (manifest: RuntimeManifest): Promise<EnsureAgentRuntimeResult> => {
    const client = await connectManifest(manifest, options.botUid, Math.max(1_000, options.startupTimeoutMs ?? DAEMON_START_TIMEOUT_MS), paths.manifestPath);
    await client.updateCliEnv(options.cliEnv);
    return { handle: handleFor(manifest), client };
  };
  const existing = await readManifestIfPresent(paths.manifestPath);
  if (existing) {
    const processSnapshot = await queryProcess(existing.pid);
    if (processSnapshot && processSnapshot.identity === existing.processIdentity) return connect(existing);
  }

  const config = {
    stateDir: resolve(options.stateDir),
    botUid: options.botUid,
    ompCommand: options.ompCommand,
    ompExtraArgs: [...(options.ompExtraArgs ?? [])],
    ...(options.startupTimeoutMs === undefined ? {} : { startupTimeoutMs: options.startupTimeoutMs }),
    ...(options.cancelTimeoutMs === undefined ? {} : { cancelTimeoutMs: options.cancelTimeoutMs }),
    ...(safeCliEnv(options.cliEnv, options.botUid) === undefined ? {} : { cliEnv: safeCliEnv(options.cliEnv, options.botUid) }),
  };
  atomicWriteJson(paths.configPath, config);
  await chmod(paths.configPath, 0o600);
  const command = IS_AOT_BINARY
    ? [resolve(dirname(realpathSync(process.execPath)), "oco-agent-runtime"), paths.configPath]
    : [process.execPath, fileURLToPath(new URL("./agent-runtime-entry.ts", import.meta.url)), paths.configPath];
  const child = Bun.spawn(command, {
    cwd: process.cwd(),
    env: process.env,
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
    detached: true,
  });
  child.unref();

  const deadline = Date.now() + Math.max(1_000, options.startupTimeoutMs ?? DAEMON_START_TIMEOUT_MS);
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const manifest = await readManifestIfPresent(paths.manifestPath);
      if (manifest) {
        if (manifest.botUid !== options.botUid) throw new Error("Agent runtime identity does not match");
        const processSnapshot = await queryProcess(manifest.pid);
        if (processSnapshot && processSnapshot.identity === manifest.processIdentity) {
          return await connect(manifest);
        }
      }
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(50);
  }
  throw new Error(`Agent runtime did not become available${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

export async function connectAgentRuntime(options: { readonly stateDir: string; readonly botUid: string }): Promise<{ handle: AgentRuntimeHandle; client: AgentRuntimeClient }> {
  const paths = pathsFor(options.stateDir, safeId(options.botUid, "botUid"));
  const manifest = await readManifestIfPresent(paths.manifestPath);
  if (!manifest) throw new Error("Agent runtime is not running");
  if (manifest.botUid !== options.botUid) throw new Error("Agent runtime identity does not match");
  const processSnapshot = await queryProcess(manifest.pid);
  if (!processSnapshot || processSnapshot.identity !== manifest.processIdentity) throw new Error("Agent runtime process identity is unknown");
  return { handle: handleFor(manifest), client: await connectManifest(manifest, options.botUid, SOCKET_CONNECT_TIMEOUT_MS, paths.manifestPath) };
}

async function readManifestIfPresent(path: string): Promise<RuntimeManifest | undefined> {
  try {
    const value: unknown = JSON.parse(await readFile(path, "utf8"));
    assertObject(value, "Agent runtime manifest");
    if ((value.protocolVersion !== 1 && value.protocolVersion !== AGENT_RUNTIME_PROTOCOL_VERSION) || typeof value.botUid !== "string" || typeof value.runtimeId !== "string" || typeof value.pid !== "number" || typeof value.processIdentity !== "string" || typeof value.socketPath !== "string" || typeof value.secret !== "string") {
      throw new Error("Agent runtime manifest is invalid");
    }
    return value as RuntimeManifest;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

function handleFor(manifest: RuntimeManifest): AgentRuntimeHandle {
  return {
    runtimeId: manifest.runtimeId,
    pid: manifest.pid,
    socketPath: manifest.socketPath,
    shutdown: async () => {
      const current = await queryProcess(manifest.pid);
      if (!current || current.identity !== manifest.processIdentity) throw new Error("Agent runtime process identity is unknown; refusing shutdown");
      process.kill(manifest.pid, "SIGTERM");
    },
  };
}

async function connectManifest(manifest: RuntimeManifest, botUid: string, timeoutMs: number, manifestPath: string): Promise<AgentRuntimeClient> {
  return AgentRuntimeClient.connect({
    protocolVersion: manifest.protocolVersion,
    socketPath: manifest.socketPath,
    secret: manifest.secret,
    botUid,
    runtimeId: manifest.runtimeId,
    pid: manifest.pid,
    processIdentity: manifest.processIdentity,
    manifestPath,
    timeoutMs,
  });
}

export class AgentRuntimeClient {
  readonly runtimeId: string;
  readonly pid: number;
  readonly #connection: RuntimeClientConnection = {
    socket: undefined,
    connectingSocket: undefined,
    epoch: 0,
    pending: new Map(),
    eventListeners: new Set(),
    closed: false,
    available: false,
    reconnecting: undefined,
  };
  readonly #identity: RuntimeClientIdentity;
  readonly #botUid: string;
  readonly #tools = new Map<string, Map<string, HostTool>>();
  readonly #registrations = new Map<string, RuntimeWorkerRegistration>();
  readonly #executingTools = new Map<string, ExecutingHostTool>();
  readonly #watches = new Map<string, Set<RuntimeWatch>>();
  readonly #acceptedResultAcks = new Set<string>();
  readonly #clientEventListener: (event: RuntimeEvent) => void;
  #requestCounter = 0;
  #ready = false;

  private constructor(identity: RuntimeClientIdentity) {
    this.#identity = identity;
    this.runtimeId = identity.runtimeId;
    this.pid = identity.pid;
    this.#botUid = identity.botUid;
    this.#clientEventListener = event => this.#handleEvent(event);
    this.#connection.eventListeners.add(this.#clientEventListener);
  }
  get protocolVersion(): number {
    return this.#identity.protocolVersion;
  }


  static async connect(options: {
    socketPath: string;
    secret: string;
    botUid: string;
    runtimeId: string;
    pid: number;
    processIdentity: string;
    manifestPath: string;
    timeoutMs: number;
    protocolVersion?: number;
  }): Promise<AgentRuntimeClient> {
    const client = new AgentRuntimeClient({
      protocolVersion: options.protocolVersion ?? AGENT_RUNTIME_PROTOCOL_VERSION,
      botUid: options.botUid,
      runtimeId: options.runtimeId,
      pid: options.pid,
      processIdentity: options.processIdentity,
      socketPath: options.socketPath,
      secret: options.secret,
      manifestPath: options.manifestPath,
    });
    try {
      await client.#openSocket(options.timeoutMs);
      client.#connection.available = true;
      client.#ready = true;
      return client;
    } catch (error) {
      await client.disconnect();
      throw error;
    }
  }

  async #verifyRuntimeIdentity(): Promise<void> {
    let manifest: RuntimeManifest | undefined;
    try {
      manifest = await readManifestIfPresent(this.#identity.manifestPath);
    } catch {
      throw new RuntimeIdentityError("Agent runtime manifest identity is unknown; refusing reconnection");
    }
    if (
      !manifest
      || manifest.protocolVersion !== this.#identity.protocolVersion
      || manifest.botUid !== this.#identity.botUid
      || manifest.runtimeId !== this.#identity.runtimeId
      || manifest.pid !== this.#identity.pid
      || manifest.processIdentity !== this.#identity.processIdentity
      || manifest.socketPath !== this.#identity.socketPath
      || manifest.secret !== this.#identity.secret
    ) {
      throw new RuntimeIdentityError("Agent runtime identity changed; refusing reconnection");
    }
    const processSnapshot = await queryProcess(this.#identity.pid).catch(() => undefined);
    if (!processSnapshot || processSnapshot.identity !== this.#identity.processIdentity) {
      throw new RuntimeIdentityError("Agent runtime process identity is unknown; refusing reconnection");
    }
  }

  async #openSocket(timeoutMs: number): Promise<Socket> {
    if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
    await this.#verifyRuntimeIdentity();
    if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
    const socket = createConnection(this.#identity.socketPath);
    socket.setEncoding("utf8");
    this.#connection.connectingSocket = socket;
    const welcome = Promise.withResolvers<{ readonly epoch: number }>();
    let receivedWelcome = false;
    let buffer = "";
    const failSocket = (error: Error): void => {
      welcome.reject(error);
      socket.destroy();
    };

    socket.on("connect", () => {
      writeLine(socket, {
        kind: "hello",
        version: this.#identity.protocolVersion,
        secret: this.#identity.secret,
        botUid: this.#identity.botUid,
      });
    });
    socket.on("data", chunk => {
      if (socket !== this.#connection.connectingSocket && socket !== this.#connection.socket) return;
      buffer += String(chunk);
      if (Buffer.byteLength(buffer, "utf8") > MAX_FRAME_BYTES) {
        failSocket(new Error("Agent runtime frame exceeds limit"));
        return;
      }
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) break;
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (!line) continue;
        try {
          const message = parseJsonLine(line);
          if (!receivedWelcome) {
            if (
              message.kind !== "welcome"
              || message.version !== this.#identity.protocolVersion
              || message.runtimeId !== this.#identity.runtimeId
              || message.pid !== this.#identity.pid
              || !Number.isSafeInteger(message.epoch)
            ) {
              throw new RuntimeIdentityError("Agent runtime welcome identity does not match");
            }
            receivedWelcome = true;
            this.#connection.epoch = message.epoch as number;
            welcome.resolve({ epoch: message.epoch as number });
            continue;
          }
          if (message.kind === "response") {
            const id = String(message.id ?? "");
            const pending = this.#connection.pending.get(id);
            if (!pending) continue;
            this.#connection.pending.delete(id);
            pending.abort?.();
            if (message.ok === true) pending.resolve(message.value);
            else if (message.startupFailure && typeof message.startupFailure === "object") {
              const failure = message.startupFailure as { code: TaskStartupCode; diagnostic?: string };
              pending.reject(new TaskStartupError(failure.code, String(message.error), failure.diagnostic));
            } else pending.reject(errorFromText(String(message.error ?? "Agent runtime operation failed")));
            continue;
          }
          if (message.kind === "event" && message.epoch === this.#connection.epoch) {
            const event = message as unknown as RuntimeEvent;
            for (const listener of this.#connection.eventListeners) listener(event);
          }
        } catch (error) {
          failSocket(error instanceof Error ? error : new Error("Invalid runtime response"));
          return;
        }
      }
    });
    socket.on("error", error => {
      welcome.reject(error);
      this.#handleSocketLoss(socket);
    });
    socket.on("close", () => {
      welcome.reject(new RuntimeConnectionLostError());
      this.#handleSocketLoss(socket);
    });

    const timer = setTimeout(() => welcome.reject(new Error("Agent runtime connection timed out")), timeoutMs);
    try {
      await welcome.promise;
      await this.#verifyRuntimeIdentity();
      if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
      if (this.#connection.connectingSocket !== socket || socket.destroyed) throw new RuntimeConnectionLostError();
      this.#connection.connectingSocket = undefined;
      this.#connection.socket = socket;
      return socket;
    } catch (error) {
      if (this.#connection.connectingSocket === socket) this.#connection.connectingSocket = undefined;
      socket.destroy();
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  #handleSocketLoss(socket: Socket): void {
    const wasCurrent = this.#connection.socket === socket;
    const wasConnecting = this.#connection.connectingSocket === socket;
    if (!wasCurrent && !wasConnecting) return;
    if (wasCurrent) {
      this.#connection.socket = undefined;
      this.#connection.available = false;
      for (const watches of this.#watches.values()) {
        for (const watch of watches) {
          watch.armedEpoch = undefined;
          watch.pendingEvents = [];
        }
      }
    }
    if (wasConnecting) this.#connection.connectingSocket = undefined;
    if (wasCurrent) {
      const error = new RuntimeConnectionLostError();
      for (const pending of this.#connection.pending.values()) {
        pending.abort?.();
        pending.reject(error);
      }
      this.#connection.pending.clear();
    }
    if (this.#ready && !this.#connection.closed) void this.#startReconnect().catch(() => {});
  }

  #startReconnect(): Promise<void> {
    if (this.#connection.closed) return Promise.reject(this.#connection.error ?? new Error("Agent runtime channel disconnected"));
    if (this.#connection.reconnecting) return this.#connection.reconnecting;
    const reconnecting = this.#reconnectUntilConnected().catch(error => {
      const failure = error instanceof Error ? error : new Error(String(error));
      this.#terminate(failure);
      throw failure;
    });
    this.#connection.reconnecting = reconnecting;
    void reconnecting.then(
      () => this.#finishReconnect(reconnecting),
      () => this.#finishReconnect(reconnecting),
    );
    return reconnecting;
  }

  #finishReconnect(reconnecting: Promise<void>): void {
    if (this.#connection.reconnecting !== reconnecting) return;
    this.#connection.reconnecting = undefined;
    if (!this.#connection.closed && (!this.#connection.socket || !this.#connection.available)) {
      void this.#startReconnect().catch(() => {});
    }
  }

  async #reconnectUntilConnected(): Promise<void> {
    let delayMs = 50;
    while (!this.#connection.closed) {
      await Bun.sleep(delayMs);
      if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
      let socket: Socket;
      try {
        socket = await this.#openSocket(SOCKET_CONNECT_TIMEOUT_MS);
      } catch (error) {
        if (error instanceof RuntimeIdentityError) throw error;
        if (this.#connection.closed) throw this.#connection.error ?? error;
        delayMs = Math.min(delayMs * 2, 1_000);
        continue;
      }
      try {
        await this.#restoreAttachments(socket);
        await this.#restoreWatches(socket);
      } catch (error) {
        if (this.#connection.socket !== socket || error instanceof RuntimeConnectionLostError) {
          delayMs = Math.min(delayMs * 2, 1_000);
          continue;
        }
        throw error;
      }
      if (this.#connection.socket === socket && !socket.destroyed) {
        this.#connection.available = true;
        return;
      }
      delayMs = Math.min(delayMs * 2, 1_000);
    }
    throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
  }

  async #restoreAttachments(socket: Socket): Promise<void> {
    for (const registration of [...this.#registrations.values()]) {
      try {
        const snapshot = await this.#requestOnSocket(socket, "attach-worker", registration.input) as AgentRuntimeWorkerSnapshot;
        const key = this.#runKey(registration.input.taskId, registration.input.runId);
        if (this.#registrations.get(key) !== registration) continue;
        registration.snapshot = snapshot;
        registration.attachedEpoch = this.#connection.epoch;
      } catch (error) {
        if (this.#connection.socket !== socket || error instanceof RuntimeConnectionLostError) throw error;
        this.#discardRegistration(registration, error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  async #restoreWatches(socket: Socket): Promise<void> {
    for (const watches of [...this.#watches.values()]) {
      const active = [...watches].filter(watch => !watch.settled);
      const first = active[0];
      if (!first) continue;
      for (const watch of active) {
        watch.armedEpoch = undefined;
        watch.pendingEvents = [];
      }
      const fromSeq = Math.min(...active.map(watch => watch.cursor));
      const state = await this.#requestOnSocket(socket, "watch-run", {
        taskId: first.taskId,
        runId: first.runId,
        dispatchId: first.dispatchId,
        fromSeq,
      }) as AgentRunState;
      for (const watch of active) this.#armWatch(watch, this.#connection.epoch);
      if (isTerminal(state.state)) {
        for (const watch of active) this.#resolveWatch(watch, state);
      }
    }
  }

  async #waitForConnection(): Promise<Socket> {
    if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
    const current = this.#connection.socket;
    if (this.#connection.available && current && !current.destroyed && current.writable) return current;
    await this.#startReconnect();
    if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
    const connected = this.#connection.socket;
    if (!this.#connection.available || !connected || connected.destroyed || !connected.writable) {
      throw new RuntimeConnectionLostError();
    }
    return connected;
  }

  async #request(operation: string, args: unknown, signal?: AbortSignal): Promise<unknown> {
    const socket = await this.#waitForConnection();
    return this.#requestOnSocket(socket, operation, args, signal);
  }

  async #requestRetryable(operation: string, args: unknown, signal?: AbortSignal): Promise<unknown> {
    for (;;) {
      try {
        return await this.#request(operation, args, signal);
      } catch (error) {
        if (!(error instanceof RuntimeConnectionLostError)) throw error;
        await this.#waitForConnection();
      }
    }
  }

  async #requestOnSocket(socket: Socket, operation: string, args: unknown, signal?: AbortSignal): Promise<unknown> {
    if (this.#connection.closed) throw this.#connection.error ?? new Error("Agent runtime channel disconnected");
    if (signal?.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("Operation cancelled");
    if (this.#connection.socket !== socket || socket.destroyed || !socket.writable) throw new RuntimeConnectionLostError();
    const id = `${process.pid}:${++this.#requestCounter}:${randomUUID()}`;
    const pending = Promise.withResolvers<unknown>();
    let abort: (() => void) | undefined;
    if (signal) {
      abort = () => {
        if (!this.#connection.pending.delete(id)) return;
        void this.#requestOnSocket(socket, "cancel-request", { targetRequestId: id }).catch(() => {});
        pending.reject(signal.reason instanceof Error ? signal.reason : new Error("Operation cancelled"));
      };
      signal.addEventListener("abort", abort, { once: true });
    }
    this.#connection.pending.set(id, {
      resolve: pending.resolve,
      reject: pending.reject,
      abort: () => signal?.removeEventListener("abort", abort!),
    });
    writeLine(socket, { kind: "request", id, epoch: this.#connection.epoch, operation, args });
    return pending.promise;
  }

  async listWorkers(): Promise<readonly AgentRuntimeWorkerSnapshot[]> {
    return await this.#requestRetryable("list-workers", {}) as AgentRuntimeWorkerSnapshot[];
  }

  async createWorker(input: CreateWorkerInput, hostTools: readonly HostTool[] = [], signal?: AbortSignal): Promise<AgentRuntimeWorker> {
    safeId(input.taskId, "taskId");
    safeId(input.runId, "runId");
    this.#setTools(input.taskId, input.runId, hostTools);
    let processSnapshot: AgentRuntimeWorkerSnapshot;
    try {
      processSnapshot = await this.#request("create-worker", input, signal) as AgentRuntimeWorkerSnapshot;
    } catch (error) {
      if (error instanceof RuntimeConnectionLostError) {
        throw new Error("Agent runtime worker creation outcome is unknown; refusing to retry", { cause: error });
      }
      throw error;
    }
    this.#registerWorker(processSnapshot);
    return new AgentRuntimeWorker(this, processSnapshot);
  }

  async attachWorker(input: AttachWorkerInput, hostTools: readonly HostTool[] = []): Promise<AgentRuntimeWorker> {
    safeId(input.taskId, "taskId");
    safeId(input.runId, "runId");
    const key = this.#runKey(input.taskId, input.runId);
    this.#setTools(input.taskId, input.runId, hostTools);
    const registration = this.#registrations.get(key) ?? { input };
    registration.input = input;
    this.#registrations.set(key, registration);
    try {
      const snapshot = await this.#requestRetryable("attach-worker", input) as AgentRuntimeWorkerSnapshot;
      registration.snapshot = snapshot;
      registration.attachedEpoch = this.#connection.epoch;
      return new AgentRuntimeWorker(this, snapshot);
    } catch (error) {
      if (this.#registrations.get(key) === registration) {
        this.#discardRegistration(registration, error instanceof Error ? error : new Error(String(error)));
      }
      throw error;
    }
  }

  /** Drops local tool registration only; runtime-owned host calls are not cancelled or settled. */
  async detachWorker(taskId: string, runId: string): Promise<void> {
    const key = this.#runKey(taskId, runId);
    this.#tools.delete(key);
    this.#registrations.delete(key);
  }

  async disconnect(): Promise<void> {
    this.#terminate(new Error("Agent runtime channel disconnected"));
  }

  #terminate(error: Error): void {
    if (this.#connection.closed) return;
    this.#connection.closed = true;
    this.#connection.available = false;
    this.#connection.error = error;
    this.#connection.eventListeners.delete(this.#clientEventListener);
    const socket = this.#connection.socket;
    const connectingSocket = this.#connection.connectingSocket;
    this.#connection.socket = undefined;
    this.#connection.connectingSocket = undefined;
    for (const pending of this.#connection.pending.values()) {
      pending.abort?.();
      pending.reject(error);
    }
    this.#connection.pending.clear();
    for (const watches of this.#watches.values()) {
      for (const watch of watches) this.#rejectWatch(watch, error);
    }
    socket?.end();
    socket?.destroy();
    connectingSocket?.destroy();
  }

  async dispatch(
    snapshot: AgentRuntimeWorkerSnapshot,
    input: RuntimeDispatchInput,
  ): Promise<AgentRunState> {
    if (input.context?.tools !== undefined) this.#setTools(snapshot.taskId, snapshot.runId, input.context.tools);
    const wireInput = {
      ...input,
      ...(input.context === undefined ? {} : { context: promptContextForWire(input.context) }),
    };
    try {
      return await this.#request("dispatch", { taskId: snapshot.taskId, runId: snapshot.runId, dispatch: wireInput }) as AgentRunState;
    } catch (error) {
      if (!(error instanceof RuntimeConnectionLostError)) throw error;
      try {
        await this.#waitForConnection();
        return await this.#requestRetryable("run-state", {
          taskId: snapshot.taskId,
          runId: snapshot.runId,
          dispatchId: input.dispatchId,
        }) as AgentRunState;
      } catch (queryError) {
        throw new Error(`Agent runtime dispatch outcome is unknown; refusing to resend ${input.dispatchId}`, { cause: queryError });
      }
    }
  }

  async watchRun(
    snapshot: AgentRuntimeWorkerSnapshot,
    dispatchId: string,
    fromSeq: number,
    onEvent: AgentRuntimeWatchEvent,
  ): Promise<AgentRunState> {
    if (!Number.isSafeInteger(fromSeq) || fromSeq < 0) throw new Error("fromSeq must be a non-negative safe integer");
    const terminal = Promise.withResolvers<AgentRunState>();
    const key = `${snapshot.taskId}\0${snapshot.runId}\0${dispatchId}`;
    const watch: RuntimeWatch = {
      key,
      taskId: snapshot.taskId,
      runId: snapshot.runId,
      dispatchId,
      cursor: fromSeq,
      onEvent,
      promise: terminal.promise,
      resolve: terminal.resolve,
      reject: terminal.reject,
      armedEpoch: undefined,
      pendingEvents: [],
      settled: false,
    };
    void watch.promise.catch(() => {});
    const watches = this.#watches.get(key) ?? new Set<RuntimeWatch>();
    watches.add(watch);
    this.#watches.set(key, watches);
    let socket: Socket;
    try {
      socket = await this.#waitForConnection();
      if (!watch.settled && watch.armedEpoch !== this.#connection.epoch) {
        await this.#subscribeWatch(watch, socket);
      }
      return await watch.promise;
    } catch (error) {
      if (error instanceof RuntimeConnectionLostError && !watch.settled) {
        try {
          socket = await this.#waitForConnection();
          if (!watch.settled && watch.armedEpoch !== this.#connection.epoch) {
            await this.#subscribeWatch(watch, socket);
          }
          return await watch.promise;
        } catch (reconnectError) {
          this.#rejectWatch(watch, reconnectError instanceof Error ? reconnectError : new Error(String(reconnectError)));
          throw reconnectError;
        }
      }
      this.#rejectWatch(watch, error instanceof Error ? error : new Error(String(error)));
      throw error;
    } finally {
      watches.delete(watch);
      if (watches.size === 0) this.#watches.delete(key);
    }
  }

  async runState(snapshot: AgentRuntimeWorkerSnapshot, dispatchId: string): Promise<AgentRunState> {
    return await this.#requestRetryable("run-state", { taskId: snapshot.taskId, runId: snapshot.runId, dispatchId }) as AgentRunState;
  }

  async workerOperation(snapshot: AgentRuntimeWorkerSnapshot, method: string, args: unknown[] = [], signal?: AbortSignal): Promise<unknown> {
    return this.#request("worker-operation", { taskId: snapshot.taskId, runId: snapshot.runId, method, args }, signal);
  }

  async stopWorker(snapshot: AgentRuntimeWorkerSnapshot): Promise<boolean> {
    const socket = this.#connection.socket;
    if (
      this.#connection.closed
      || !this.#connection.available
      || !socket
      || socket.destroyed
      || !socket.writable
    ) return false;
    try {
      return await this.#requestOnSocket(socket, "stop-worker", { taskId: snapshot.taskId, runId: snapshot.runId }) as boolean;
    } catch (error) {
      if (
        error instanceof RuntimeConnectionLostError
        || this.#connection.closed
        || !this.#connection.available
      ) return false;
      throw error;
    }
  }

  async cancelRun(snapshot: AgentRuntimeWorkerSnapshot, dispatchId: string): Promise<boolean> {
    const result = await this.#request("cancel-run", { taskId: snapshot.taskId, runId: snapshot.runId, dispatchId }) as { cancelled: boolean };
    return result.cancelled;
  }

  async updateCliEnv(cliEnv: Readonly<Record<string, string>> | undefined): Promise<void> {
    await this.#requestRetryable("update-cli-env", { cliEnv: safeCliEnv(cliEnv, this.#botUid) });
  }

  #registerWorker(snapshot: AgentRuntimeWorkerSnapshot): void {
    const input: AttachWorkerInput = {
      taskId: snapshot.taskId,
      runId: snapshot.runId,
      sessionId: snapshot.sessionId,
    };
    this.#registrations.set(this.#runKey(snapshot.taskId, snapshot.runId), {
      input,
      snapshot,
      attachedEpoch: this.#connection.epoch,
    });
  }

  #setTools(taskId: string, runId: string, tools: readonly HostTool[]): void {
    this.#tools.set(this.#runKey(taskId, runId), new Map(tools.map(tool => [tool.name, tool])));
  }

  #runKey(taskId: string, runId: string): string {
    return `${taskId}\0${runId}`;
  }

  #resolveWatch(watch: RuntimeWatch, state: AgentRunState): void {
    if (watch.settled) return;
    watch.settled = true;
    watch.resolve(state);
  }

  #rejectWatch(watch: RuntimeWatch, error: Error): void {
    if (watch.settled) return;
    watch.settled = true;
    watch.reject(error);
  }
  #discardRegistration(registration: RuntimeWorkerRegistration, error: Error): void {
    const key = this.#runKey(registration.input.taskId, registration.input.runId);
    if (this.#registrations.get(key) === registration) {
      this.#registrations.delete(key);
      this.#tools.delete(key);
    }
    for (const watches of this.#watches.values()) {
      for (const watch of watches) {
        if (watch.taskId === registration.input.taskId && watch.runId === registration.input.runId) {
          this.#rejectWatch(watch, error);
        }
      }
    }
  }

  async #subscribeWatch(watch: RuntimeWatch, socket: Socket): Promise<void> {
    watch.armedEpoch = undefined;
    watch.pendingEvents = [];
    const state = await this.#requestOnSocket(socket, "watch-run", {
      taskId: watch.taskId,
      runId: watch.runId,
      dispatchId: watch.dispatchId,
      fromSeq: watch.cursor,
    }) as AgentRunState;
    this.#armWatch(watch, this.#connection.epoch);
    if (isTerminal(state.state)) this.#resolveWatch(watch, state);
  }

  #armWatch(watch: RuntimeWatch, epoch: number): void {
    watch.armedEpoch = epoch;
    const pending = watch.pendingEvents;
    watch.pendingEvents = [];
    pending.sort((left, right) => {
      if (left.event === "run-event" && right.event === "run-event") return (left.seq ?? 0) - (right.seq ?? 0);
      if (left.event === "run-event") return -1;
      if (right.event === "run-event") return 1;
      return 0;
    });
    for (const event of pending) this.#consumeWatchEvent(watch, event);
  }

  #consumeWatchEvent(watch: RuntimeWatch, event: RuntimeEvent): void {
    if (watch.settled) return;
    if (event.event === "run-event" && typeof event.seq === "number" && event.seq > watch.cursor) {
      try {
        watch.onEvent(event.value as WorkerEvent, event.seq);
        watch.cursor = event.seq;
      } catch (error) {
        this.#rejectWatch(watch, error instanceof Error ? error : new Error(String(error)));
      }
    } else if (event.event === "run-terminal") {
      this.#resolveWatch(watch, event.value as AgentRunState);
    }
  }

  async #resolveHostTool(event: RuntimeEvent): Promise<void> {
    const value = event.value as { callId: string; toolName: string; args: Record<string, unknown>; toolCallId?: string; cancelled?: boolean };
    const existing = this.#executingTools.get(value.callId);
    if (existing) {
      if (existing.result) await this.#deliverHostToolResult(value.callId, existing).catch(() => {});
      return;
    }
    const tool = this.#tools.get(this.#runKey(event.taskId, event.runId))?.get(value.toolName);
    if (!tool) return;
    const controller = new AbortController();
    const execution: ExecutingHostTool = {
      taskId: event.taskId,
      runId: event.runId,
      dispatchId: event.dispatchId ?? "",
      ...(value.toolCallId === undefined ? {} : { toolCallId: value.toolCallId }),
      controller,
    };
    this.#executingTools.set(value.callId, execution);
    if (value.cancelled) controller.abort(new Error("OMP cancelled host tool"));
    try {
      execution.result = { isError: false, result: String(await tool.execute(value.args, controller.signal, value.toolCallId)) };
    } catch (error) {
      execution.result = { isError: true, result: errorText(error) };
    }
    await this.#deliverHostToolResult(value.callId, execution).catch(() => {});
  }

  #deliverHostToolResult(callId: string, execution: ExecutingHostTool): Promise<void> {
    if (!execution.result) return Promise.resolve();
    if (execution.delivery) return execution.delivery;
    const result = execution.result;
    const delivery = this.#requestRetryable("host-tool-result", {
      taskId: execution.taskId,
      runId: execution.runId,
      callId,
      isError: result.isError,
      result: result.result,
    }).then(() => {
      if (this.#executingTools.get(callId) === execution) this.#executingTools.delete(callId);
    }).finally(() => {
      if (execution.delivery === delivery) execution.delivery = undefined;
    });
    execution.delivery = delivery;
    return delivery;
  }

  #handleEvent(event: RuntimeEvent): void {
    if (event.event === "run-event" && typeof event.seq === "number" && event.dispatchId) {
      const key = `${event.taskId}\0${event.runId}\0${event.dispatchId}`;
      for (const watch of this.#watches.get(key) ?? []) {
        if (watch.settled) continue;
        if (watch.armedEpoch !== this.#connection.epoch) watch.pendingEvents.push(event);
        else this.#consumeWatchEvent(watch, event);
      }
    } else if (event.event === "run-terminal" && event.dispatchId) {
      const key = `${event.taskId}\0${event.runId}\0${event.dispatchId}`;
      for (const watch of this.#watches.get(key) ?? []) {
        if (watch.settled) continue;
        if (watch.armedEpoch !== this.#connection.epoch) watch.pendingEvents.push(event);
        else this.#consumeWatchEvent(watch, event);
      }
      for (const [callId, execution] of this.#executingTools) {
        if (execution.taskId === event.taskId && execution.runId === event.runId && execution.dispatchId === event.dispatchId && execution.result) {
          this.#executingTools.delete(callId);
        }
      }
    } else if (event.event === "host-tool-call") void this.#resolveHostTool(event);
    else if (event.event === "host-tool-cancel") {
      const value = event.value as { callId: string };
      const execution = this.#executingTools.get(value.callId);
      if (
        execution
        && execution.taskId === event.taskId
        && execution.runId === event.runId
        && execution.dispatchId === event.dispatchId
      ) {
        execution.controller.abort(new Error("OMP cancelled host tool"));
        void this.#requestRetryable("host-tool-cancel-ack", {
          taskId: event.taskId,
          runId: event.runId,
          dispatchId: event.dispatchId,
          callId: value.callId,
        }).catch(() => {});
      }
    } else if (event.event === "host-tool-result-accepted") {
      const value = event.value as { ackId: string; toolName: string; toolCallId?: string; resultHash?: string };
      if (
        typeof value.ackId !== "string"
        || typeof value.toolCallId !== "string"
        || typeof value.resultHash !== "string"
        || !/^[0-9a-f]{64}$/u.test(value.resultHash)
        || this.#acceptedResultAcks.has(value.ackId)
      ) return;
      const tool = this.#tools.get(this.#runKey(event.taskId, event.runId))?.get(value.toolName);
      if (!tool) return;
      this.#acceptedResultAcks.add(value.ackId);
      if (tool.onResultAccepted) {
        try {
          tool.onResultAccepted(value.toolCallId, value.resultHash);
        } catch (error) {
          this.#acceptedResultAcks.delete(value.ackId);
          throw error;
        }
      }
      for (const [callId, execution] of this.#executingTools) {
        if (
          execution.taskId === event.taskId
          && execution.runId === event.runId
          && execution.dispatchId === event.dispatchId
          && execution.toolCallId === value.toolCallId
        ) this.#executingTools.delete(callId);
      }
      void this.#requestRetryable("host-tool-result-accepted-ack", {
        taskId: event.taskId,
        runId: event.runId,
        dispatchId: event.dispatchId,
        ackId: value.ackId,
        toolCallId: value.toolCallId,
        resultHash: value.resultHash,
      }).catch(() => {}).finally(() => this.#acceptedResultAcks.delete(value.ackId));
    }
  }
}

export class AgentRuntimeWorker implements OmpWorker {
  readonly #client: AgentRuntimeClient;
  readonly snapshot: AgentRuntimeWorkerSnapshot;
  readonly process: WorkerProcess | null;
  readonly sessionFile: string;
  readonly sessionId: string;

  constructor(client: AgentRuntimeClient, snapshot: AgentRuntimeWorkerSnapshot) {
    this.#client = client;
    this.snapshot = snapshot;
    this.process = snapshot.process;
    this.sessionId = snapshot.sessionId;
    this.sessionFile = snapshot.sessionFile;
  }

  async refreshState(): Promise<ProjectSessionState> { return await this.#client.workerOperation(this.snapshot, "refreshState") as ProjectSessionState; }
  async btw(question: string, signal?: AbortSignal): Promise<string> { return await this.#client.workerOperation(this.snapshot, "btw", [question, undefined], signal) as string; }
  async goal(op: "get" | "pause" | "drop"): Promise<WorkerGoalResult> { return await this.#client.workerOperation(this.snapshot, "goal", [op]) as WorkerGoalResult; }
  async steer(text: string): Promise<void> { await this.#client.workerOperation(this.snapshot, "steer", [text]); }
  async abortTurn(): Promise<void> { await this.#client.workerOperation(this.snapshot, "abortTurn"); }
  async contextUsage(signal?: AbortSignal): Promise<WorkerContextUsage> { return await this.#client.workerOperation(this.snapshot, "contextUsage", [undefined], signal) as WorkerContextUsage; }
  async getSubagents(signal?: AbortSignal): Promise<readonly OmpSubagentSnapshot[]> { return await this.#client.workerOperation(this.snapshot, "getSubagents", [undefined], signal) as readonly OmpSubagentSnapshot[]; }
  async getSubagentMessages(subagentId: string, signal?: AbortSignal): Promise<OmpSubagentMessages> { return await this.#client.workerOperation(this.snapshot, "getSubagentMessages", [subagentId, undefined], signal) as OmpSubagentMessages; }
  async models(): Promise<WorkerModelCatalog> { return await this.#client.workerOperation(this.snapshot, "models") as WorkerModelCatalog; }
  async availableCommands(signal?: AbortSignal): Promise<readonly OmpAvailableCommand[]> { return await this.#client.workerOperation(this.snapshot, "availableCommands", [undefined], signal) as readonly OmpAvailableCommand[]; }
  async switchModel(selector: string): Promise<WorkerModelSwitch> { return await this.#client.workerOperation(this.snapshot, "switchModel", [selector]) as WorkerModelSwitch; }
  async stop(): Promise<boolean> { return this.#client.stopWorker(this.snapshot); }

  async dispatch(input: RuntimeDispatchInput): Promise<AgentRunState> {
    return this.#client.dispatch(this.snapshot, input);
  }

  async cancelRun(dispatchId: string): Promise<boolean> {
    return this.#client.cancelRun(this.snapshot, dispatchId);
  }

  async watchRun(dispatchId: string, fromSeq: number, onEvent: AgentRuntimeWatchEvent): Promise<AgentRunState> {
    return this.#client.watchRun(this.snapshot, dispatchId, fromSeq, onEvent);
  }

  async runState(dispatchId: string): Promise<AgentRunState> {
    return this.#client.runState(this.snapshot, dispatchId);
  }

  async prompt(text: string, onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    const dispatchId = randomUUID();
    await this.dispatch({ dispatchId, kind: "prompt", text, ...(context === undefined ? {} : { context }) });
    const state = await this.watchRun(dispatchId, 0, onEventWithSeq(onEvent));
    if (state.state === "failed" || state.state === "unknown") throw errorFromText(state.error ?? "OMP prompt outcome is unknown");
  }

  async resume(onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<void> {
    const dispatchId = randomUUID();
    await this.dispatch({ dispatchId, kind: "resume", ...(context === undefined ? {} : { context }) });
    const state = await this.watchRun(dispatchId, 0, onEventWithSeq(onEvent));
    if (state.state === "failed" || state.state === "unknown") throw errorFromText(state.error ?? "OMP resume outcome is unknown");
  }

  async runGoal(request: Parameters<OmpWorker["runGoal"]>[0], onEvent: (event: WorkerEvent) => void, context?: PromptContext): Promise<WorkerGoalResult> {
    const dispatchId = randomUUID();
    await this.dispatch({ dispatchId, kind: "goal", goal: request, ...(context === undefined ? {} : { context }) });
    const state = await this.watchRun(dispatchId, 0, onEventWithSeq(onEvent));
    if (state.state === "failed" || state.state === "unknown") throw errorFromText(state.error ?? "OMP goal outcome is unknown");
    return state.result as WorkerGoalResult;
  }
}

function onEventWithSeq(onEvent: (event: WorkerEvent) => void): AgentRuntimeWatchEvent {
  return event => onEvent(event);
}

type RuntimeDispatchInput = AgentRuntimeDispatchInput;
type ServerDispatchInput = Omit<RuntimeDispatchInput, "context"> & {
  readonly context?: AgentRuntimePromptContext;
};
