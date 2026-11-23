import { createHash, randomBytes, randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rm } from "node:fs/promises";
import {
  closeSync,
  fsyncSync,
  openSync,
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
} from "../contracts.ts";
import { TaskStartupError, WorkerGoalError, WorkerSteerRejectedError } from "../contracts.ts";
import type { ProjectSessionState } from "../session/project-session.ts";
import { createWorkerFactory } from "../omp/index.ts";
import { OCTO_CLI_ENV_ALLOWLIST } from "./octo-cli.ts";
import { queryProcess } from "../omp/process.ts";
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
  /** Private, bounded OMP startup detail retained for local diagnosis, never sent over the runtime protocol. */
  startupFailure?: { code: string; diagnostic?: string };
};

type WorkerRecord = {
  snapshot: StoredWorker;
  worker?: OmpWorker;
  calls: Map<string, PendingHostCall>;
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
  socket: Socket;
  epoch: number;
  buffer: string;
  pending: Map<string, PendingRequest>;
  eventListeners: Set<(event: RuntimeEvent) => void>;
  closed: boolean;
};

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
    || !["spawn-intent", "identity-verified", "omp-ready", "active", "stopping", "exited-confirmed", "unknown"].includes(String(value.stage))
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
      };
      if (record.snapshot.sessionKeys.length === 0) {
        if (record.snapshot.sessionFile) record.snapshot.sessionKeys.push(`file:${resolve(record.snapshot.sessionFile)}`);
        if (record.snapshot.sessionId) record.snapshot.sessionKeys.push(`session:${record.snapshot.sessionId}`);
      }
      if (!record.snapshot.process) {
        record.snapshot.stage = "unknown";
      } else if (record.snapshot.stage !== "exited-confirmed") {
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
      this.#send(connection, { kind: "response", id, ok: false, error: errorText(error) });
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
      if (!call || call.settled) return { accepted: false };
      call.settled = true;
      if (args.isError === true) call.reject(new Error(String(args.result ?? "Host tool failed")));
      else call.resolve(String(args.result ?? ""));
      record.calls.delete(callId);
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
      sessionKeys,
      dispatchMap: {},
    };
    const record: WorkerRecord = { snapshot, calls: new Map() };
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
      } else {
        snapshot.stage = "unknown";
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
      onResultAccepted: toolCallId => {
        this.#sendEvent({ event: "host-tool-result-accepted", taskId: record.snapshot.taskId, runId: record.snapshot.runId, dispatchId: dispatch.dispatchId, value: { toolCallId, toolName: descriptor.name } });
      },
    }));
    return {
      ...(context.contextInjection === undefined ? {} : { contextInjection: context.contextInjection }),
      ...(tools === undefined ? {} : { tools }),
    };
  }

  #callHostTool(
    record: WorkerRecord,
    dispatch: StoredDispatch,
    toolName: string,
    args: Record<string, unknown>,
    signal: AbortSignal,
    toolCallId?: string,
  ): Promise<string> {
    if (signal.aborted) return Promise.reject(signal.reason instanceof Error ? signal.reason : new Error("Host tool cancelled"));
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
      const error = signal.reason instanceof Error ? signal.reason : new Error("Host tool cancelled");
      call.controller.abort(error);
      this.#sendEvent({ event: "host-tool-cancel", taskId: record.snapshot.taskId, runId: record.snapshot.runId, dispatchId: dispatch.dispatchId, value: { callId } });
      if (!call.settled) {
        call.settled = true;
        call.reject(error);
        record.calls.delete(callId);
      }
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
    const client = await connectManifest(manifest, options.botUid, Math.max(1_000, options.startupTimeoutMs ?? DAEMON_START_TIMEOUT_MS));
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
  const entryPath = fileURLToPath(new URL("./agent-runtime-entry.ts", import.meta.url));
  const child = Bun.spawn([process.execPath, entryPath, paths.configPath], {
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
        if (manifest.botUid !== options.botUid || manifest.protocolVersion !== AGENT_RUNTIME_PROTOCOL_VERSION) throw new Error("Agent runtime identity does not match");
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
  if (manifest.botUid !== options.botUid || manifest.protocolVersion !== AGENT_RUNTIME_PROTOCOL_VERSION) throw new Error("Agent runtime identity does not match");
  const processSnapshot = await queryProcess(manifest.pid);
  if (!processSnapshot || processSnapshot.identity !== manifest.processIdentity) throw new Error("Agent runtime process identity is unknown");
  return { handle: handleFor(manifest), client: await connectManifest(manifest, options.botUid, SOCKET_CONNECT_TIMEOUT_MS) };
}

async function readManifestIfPresent(path: string): Promise<RuntimeManifest | undefined> {
  try {
    const value: unknown = JSON.parse(await readFile(path, "utf8"));
    assertObject(value, "Agent runtime manifest");
    if (value.protocolVersion !== AGENT_RUNTIME_PROTOCOL_VERSION || typeof value.botUid !== "string" || typeof value.runtimeId !== "string" || typeof value.pid !== "number" || typeof value.processIdentity !== "string" || typeof value.socketPath !== "string" || typeof value.secret !== "string") {
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

async function connectManifest(manifest: RuntimeManifest, botUid: string, timeoutMs: number): Promise<AgentRuntimeClient> {
  return AgentRuntimeClient.connect({ socketPath: manifest.socketPath, secret: manifest.secret, botUid, runtimeId: manifest.runtimeId, timeoutMs });
}

export class AgentRuntimeClient {
  readonly runtimeId: string;
  readonly pid: number;
  readonly #connection: RuntimeClientConnection;
  readonly #botUid: string;
  readonly #tools = new Map<string, Map<string, HostTool>>();
  readonly #executingTools = new Map<string, AbortController>();
  readonly #clientEventListener: (event: RuntimeEvent) => void;
  #requestCounter = 0;

  private constructor(runtimeId: string, pid: number, botUid: string, connection: RuntimeClientConnection) {
    this.runtimeId = runtimeId;
    this.pid = pid;
    this.#botUid = botUid;
    this.#connection = connection;
    this.#clientEventListener = event => this.#handleEvent(event);
    this.#connection.eventListeners.add(this.#clientEventListener);

  }
  static async connect(options: { socketPath: string; secret: string; botUid: string; runtimeId: string; timeoutMs: number }): Promise<AgentRuntimeClient> {
    const socket = createConnection(options.socketPath);
    socket.setEncoding("utf8");
    const connection: RuntimeClientConnection = {
      socket,
      epoch: 0,
      buffer: "",
      pending: new Map(),
      eventListeners: new Set(),
      closed: false,
    };
    const clientReady = Promise.withResolvers<{ runtimeId: string; epoch: number; pid: number }>();
    const timer = setTimeout(() => clientReady.reject(new Error("Agent runtime connection timed out")), options.timeoutMs);
    socket.on("connect", () => writeLine(socket, { kind: "hello", version: AGENT_RUNTIME_PROTOCOL_VERSION, secret: options.secret, botUid: options.botUid }));
    socket.on("data", chunk => {
      connection.buffer += String(chunk);
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
          if (message.kind === "welcome") {
            if (message.version !== AGENT_RUNTIME_PROTOCOL_VERSION || message.runtimeId !== options.runtimeId || typeof message.epoch !== "number" || typeof message.pid !== "number") throw new Error("Agent runtime welcome identity does not match");
            connection.epoch = message.epoch;
            clientReady.resolve({ runtimeId: String(message.runtimeId), epoch: message.epoch, pid: message.pid });
            continue;
          }
          if (message.kind === "response") {
            const id = String(message.id ?? "");
            const pending = connection.pending.get(id);
            if (!pending) continue;
            connection.pending.delete(id);
            pending.abort?.();
            if (message.ok === true) pending.resolve(message.value);
            else pending.reject(errorFromText(String(message.error ?? "Agent runtime operation failed")));
            continue;
          }
          if (message.kind === "event" && message.epoch === connection.epoch) {
            const event = message as unknown as RuntimeEvent;
            for (const listener of connection.eventListeners) listener(event);
          }
        } catch (error) {
          socket.destroy(error instanceof Error ? error : new Error("Invalid runtime response"));
          return;
        }
      }
    });
    socket.on("error", error => {
      connection.closed = true;
      clientReady.reject(error);
      for (const pending of connection.pending.values()) pending.reject(error);
      connection.pending.clear();
    });
    socket.on("close", () => {
      connection.closed = true;
      for (const pending of connection.pending.values()) pending.reject(new Error("Agent runtime channel disconnected"));
      connection.pending.clear();
    });
    try {
      const welcome = await clientReady.promise;
      return new AgentRuntimeClient(welcome.runtimeId, welcome.pid, options.botUid, connection);
    } finally {
      clearTimeout(timer);
    }
  }

  async listWorkers(): Promise<readonly AgentRuntimeWorkerSnapshot[]> {
    return await this.#request("list-workers", {}) as AgentRuntimeWorkerSnapshot[];
  }

  async createWorker(input: CreateWorkerInput, hostTools: readonly HostTool[] = [], signal?: AbortSignal): Promise<AgentRuntimeWorker> {
    safeId(input.taskId, "taskId");
    safeId(input.runId, "runId");
    this.#setTools(input.taskId, input.runId, hostTools);
    const processSnapshot = await this.#request("create-worker", input, signal) as AgentRuntimeWorkerSnapshot;
    return new AgentRuntimeWorker(this, processSnapshot);
  }

  async attachWorker(input: AttachWorkerInput, hostTools: readonly HostTool[] = []): Promise<AgentRuntimeWorker> {
    safeId(input.taskId, "taskId");
    safeId(input.runId, "runId");
    this.#setTools(input.taskId, input.runId, hostTools);
    const snapshot = await this.#request("attach-worker", input) as AgentRuntimeWorkerSnapshot;
    return new AgentRuntimeWorker(this, snapshot);
  }

  /** Drops local tool registration only; runtime-owned host calls are not cancelled or settled. */
  async detachWorker(taskId: string, runId: string): Promise<void> {
    await this.#request("detach-worker", { taskId, runId });
    this.#tools.delete(this.#runKey(taskId, runId));
  }

  async disconnect(): Promise<void> {
    if (this.#connection.closed) return;
    this.#connection.closed = true;
    this.#connection.eventListeners.delete(this.#clientEventListener);
    this.#connection.socket.end();
    this.#connection.socket.destroy();
    this.#executingTools.clear();
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
    return await this.#request("dispatch", { taskId: snapshot.taskId, runId: snapshot.runId, dispatch: wireInput }) as AgentRunState;
  }

  async watchRun(
    snapshot: AgentRuntimeWorkerSnapshot,
    dispatchId: string,
    fromSeq: number,
    onEvent: AgentRuntimeWatchEvent,
  ): Promise<AgentRunState> {
    if (!Number.isSafeInteger(fromSeq) || fromSeq < 0) throw new Error("fromSeq must be a non-negative safe integer");
    const terminal = Promise.withResolvers<AgentRunState>();
    let cursor = fromSeq;
    const eventListener = (event: RuntimeEvent): void => {
      if (event.taskId !== snapshot.taskId || event.runId !== snapshot.runId || event.dispatchId !== dispatchId) return;
      if (event.event === "run-event" && typeof event.seq === "number" && event.seq > cursor) {
        cursor = event.seq;
        try {
          onEvent(event.value as WorkerEvent, event.seq);
        } catch (error) {
          terminal.reject(error instanceof Error ? error : new Error(String(error)));
        }
      } else if (event.event === "run-terminal") {
        terminal.resolve(event.value as AgentRunState);
      }
    };
    const onClose = (): void => terminal.reject(new Error("Agent runtime channel disconnected"));
    this.#connection.socket.once("close", onClose);
    this.#connection.eventListeners.add(eventListener);
    try {
      const state = await this.#request("watch-run", { taskId: snapshot.taskId, runId: snapshot.runId, dispatchId, fromSeq }) as AgentRunState;
      if (isTerminal(state.state)) terminal.resolve(state);
      return await terminal.promise;
    } finally {
      this.#connection.socket.off("close", onClose);
      this.#connection.eventListeners.delete(eventListener);
    }
  }

  async runState(snapshot: AgentRuntimeWorkerSnapshot, dispatchId: string): Promise<AgentRunState> {
    return await this.#request("run-state", { taskId: snapshot.taskId, runId: snapshot.runId, dispatchId }) as AgentRunState;
  }

  async workerOperation(snapshot: AgentRuntimeWorkerSnapshot, method: string, args: unknown[] = [], signal?: AbortSignal): Promise<unknown> {
    return this.#request("worker-operation", { taskId: snapshot.taskId, runId: snapshot.runId, method, args }, signal);
  }

  async stopWorker(snapshot: AgentRuntimeWorkerSnapshot): Promise<boolean> {
    return await this.#request("stop-worker", { taskId: snapshot.taskId, runId: snapshot.runId }) as boolean;
  }

  async cancelRun(snapshot: AgentRuntimeWorkerSnapshot, dispatchId: string): Promise<boolean> {
    const result = await this.#request("cancel-run", { taskId: snapshot.taskId, runId: snapshot.runId, dispatchId }) as { cancelled: boolean };
    return result.cancelled;
  }

  async #request(operation: string, args: unknown, signal?: AbortSignal): Promise<unknown> {
    if (this.#connection.closed || this.#connection.socket.destroyed) throw new Error("Agent runtime channel is disconnected");
    if (signal?.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("Operation cancelled");
    const id = `${process.pid}:${++this.#requestCounter}:${randomUUID()}`;
    const pending = Promise.withResolvers<unknown>();
    let abort: (() => void) | undefined;
    if (signal) {
      abort = () => {
        void this.#request("cancel-request", { targetRequestId: id }).catch(() => {});
        pending.reject(signal.reason instanceof Error ? signal.reason : new Error("Operation cancelled"));
        this.#connection.pending.delete(id);
      };
      signal.addEventListener("abort", abort, { once: true });
    }
    this.#connection.pending.set(id, {
      resolve: pending.resolve,
      reject: pending.reject,
      abort: () => signal?.removeEventListener("abort", abort!),
    });
    writeLine(this.#connection.socket, { kind: "request", id, epoch: this.#connection.epoch, operation, args });
    return pending.promise;
  }
  async updateCliEnv(cliEnv: Readonly<Record<string, string>> | undefined): Promise<void> {
    await this.#request("update-cli-env", { cliEnv: safeCliEnv(cliEnv, this.#botUid) });
  }

  #setTools(taskId: string, runId: string, tools: readonly HostTool[]): void {
    this.#tools.set(this.#runKey(taskId, runId), new Map(tools.map(tool => [tool.name, tool])));
  }

  #runKey(taskId: string, runId: string): string {
    return `${taskId}\0${runId}`;
  }


  async #resolveHostTool(event: RuntimeEvent): Promise<void> {
    const value = event.value as { callId: string; toolName: string; args: Record<string, unknown>; toolCallId?: string; cancelled?: boolean };
    const tool = this.#tools.get(this.#runKey(event.taskId, event.runId))?.get(value.toolName);
    if (!tool) return;
    const previous = this.#executingTools.get(value.callId);
    if (previous) return;
    const controller = new AbortController();
    this.#executingTools.set(value.callId, controller);
    if (value.cancelled) controller.abort(new Error("OMP cancelled host tool"));
    try {
      const result = await tool.execute(value.args, controller.signal, value.toolCallId);
      await this.#request("host-tool-result", { taskId: event.taskId, runId: event.runId, callId: value.callId, isError: false, result: String(result) });
    } catch (error) {
      await this.#request("host-tool-result", { taskId: event.taskId, runId: event.runId, callId: value.callId, isError: true, result: errorText(error) }).catch(() => {});
    } finally {
      this.#executingTools.delete(value.callId);
    }
  }

  #handleEvent(event: RuntimeEvent): void {
    if (event.event === "host-tool-call") void this.#resolveHostTool(event);
    else if (event.event === "host-tool-cancel") {
      const value = event.value as { callId: string };
      this.#executingTools.get(value.callId)?.abort(new Error("OMP cancelled host tool"));
    } else if (event.event === "host-tool-result-accepted") {
      const value = event.value as { toolName: string; toolCallId: string };
      this.#tools.get(this.#runKey(event.taskId, event.runId))?.get(value.toolName)?.onResultAccepted?.(value.toolCallId);
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
