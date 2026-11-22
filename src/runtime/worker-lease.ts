import type {
  AgentRuntimeClient,
  AgentRuntimeWorker,
} from "./agent-runtime.ts";
import type { AgentRuntimeWorkerOptions } from "./agent-runtime-protocol.ts";
import type {
  CommitWorker,
  CommitWorkerOptions,
  HostTool,
  OmpWorker,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { WorkerLedgerInput, WorkerLedgerRecord } from "../store/store-records.ts";

export type WorkerSession = Omit<OmpWorker, "stop">;
export type AgentRuntimeWorkerSession = Omit<AgentRuntimeWorker, "stop">;
export type CommitSession = Omit<CommitWorker, "stop">;

export interface TrackedAgentWorker {
  readonly worker: AgentRuntimeWorkerSession;
  readonly snapshot: AgentRuntimeWorker["snapshot"];
  /** Stops and finalizes the runtime-owned worker; never falls back to channel-side recovery. */
  close(recoverOnFailure?: boolean): Promise<boolean>;
  /** Releases the channel attachment without stopping or finalizing the runtime-owned process. */
  detach(): Promise<void>;
}

export type OpenTrackedAgentWorkerOptions = {
  readonly client: AgentRuntimeClient;
  readonly runtimeId: string;
  readonly botUid: string;
  readonly taskId: string;
  readonly runId: string;
  readonly worker:
    | { readonly kind: "create"; readonly options: AgentRuntimeWorkerOptions }
    | { readonly kind: "attach"; readonly sessionId: string };
  readonly hostTools?: readonly HostTool[];
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly blockDirectory: (cwd: string) => void;
};


export interface TrackedWorker {
  readonly worker: WorkerSession;
  /**
   * The first call selects recovery. All calls share the stop and ledger-finalization promise,
   * including a rejected finalization.
   */
  close(recoverOnFailure?: boolean): Promise<boolean>;
}

export interface TrackedCommit {
  readonly worker: CommitSession;
  /** Shares the stop and ledger-finalization promise with every caller. */
  close(recoverOnFailure?: boolean): Promise<boolean>;
}

export interface OpenTrackedWorkerOptions {
  readonly workerFactory: WorkerFactory;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly worker: Omit<WorkerOptions, "onSpawn">;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd" | "runtimeId">;
  readonly moveTargetCwd?: string;
  readonly recordWorker?: (worker: WorkerLedgerRecord) => boolean;
  readonly blockDirectory: (cwd: string) => void;
}

export interface OpenTrackedCommitOptions {
  readonly workerFactory: WorkerFactory;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly worker: Omit<CommitWorkerOptions, "onSpawn">;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd" | "runtimeId">;
  readonly blockDirectory: (cwd: string) => void;
}

interface StoppableWorker {
  stop(): Promise<boolean>;
}

interface OpenTrackedProcessOptions<TWorker extends StoppableWorker> {
  readonly workerFactory: Pick<WorkerFactory, "recover">;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly create: (onSpawn: (process: WorkerProcess) => void) => Promise<TWorker>;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd" | "runtimeId">;
  readonly moveTargetCwd?: string;
  readonly recordWorker?: (worker: WorkerLedgerRecord) => boolean;
  readonly blockDirectory: (cwd: string) => void;
}

interface TrackedProcess<TWorker extends StoppableWorker> {
  readonly worker: Omit<TWorker, "stop">;
  close(recoverOnFailure?: boolean): Promise<boolean>;
}

function blockUnconfirmedProcess(
  options: Pick<OpenTrackedProcessOptions<StoppableWorker>, "blockDirectory" | "moveTargetCwd">,
  spawned: WorkerProcess | undefined,
): void {
  if (spawned) options.blockDirectory(spawned.cwd);
  if (options.moveTargetCwd !== undefined) options.blockDirectory(options.moveTargetCwd);
}

async function openTrackedProcess<TWorker extends StoppableWorker>(
  options: OpenTrackedProcessOptions<TWorker>,
): Promise<TrackedProcess<TWorker>> {
  let spawned: WorkerProcess | undefined;
  let worker: TWorker;
  try {
    worker = await options.create((processInfo) => {
      spawned = processInfo;
      const ledger = {
        ...options.ledger,
        ...processInfo,
        runtimeId: null,
        moveTargetCwd: options.moveTargetCwd ?? null,
        state: "running" as const,
      };
      if (options.recordWorker !== undefined) {
        if (!options.recordWorker(ledger)) {
          throw new Error("spawned worker does not match its ingress dispatch-intent");
        }
      } else {
        options.store.recordWorker(ledger);
      }
    });
  } catch (error) {
    if (spawned) {
      let stopped = false;
      try {
        stopped = await options.workerFactory.recover(spawned);
      } catch {
        // An unconfirmed process remains blocked below.
      }
      if (stopped) {
        try {
          options.store.markWorkerStopped(options.ledger.id);
        } catch {
          // Preserve the original create failure.
        }
      } else {
        blockUnconfirmedProcess(options, spawned);
      }
    }
    throw error;
  }

  let closePromise: Promise<boolean> | undefined;
  const close = (recoverOnFailure = false): Promise<boolean> => {
    if (!closePromise) {
      closePromise = (async () => {
        let stopped = false;
        try {
          stopped = await worker.stop();
        } catch {
          // A rejected stop is not confirmation that the process group has exited.
        }
        if (!stopped && recoverOnFailure && spawned) {
          try {
            stopped = await options.workerFactory.recover(spawned);
          } catch {
            // An unconfirmed process remains blocked below.
          }
        }
        if (stopped) options.store.markWorkerStopped(options.ledger.id);
        else blockUnconfirmedProcess(options, spawned);
        return stopped;
      })();
    }
    return closePromise;
  };

  return { worker, close };
}

export async function openTrackedWorker(options: OpenTrackedWorkerOptions): Promise<TrackedWorker> {
  return openTrackedProcess({
    workerFactory: options.workerFactory,
    store: options.store,
    create: (onSpawn) => options.workerFactory.create({ ...options.worker, onSpawn }),
    ledger: options.ledger,
    ...(options.moveTargetCwd === undefined ? {} : { moveTargetCwd: options.moveTargetCwd }),
    ...(options.recordWorker === undefined ? {} : { recordWorker: options.recordWorker }),
    blockDirectory: options.blockDirectory,
  });
}

/** Tracks a worker owned by the independent runtime; ordinary channel close uses detach, not stop. */
export async function openTrackedAgentWorker(
  options: OpenTrackedAgentWorkerOptions,
): Promise<TrackedAgentWorker> {
  let worker = options.worker.kind === "create"
    ? await options.client.createWorker({
        taskId: options.taskId,
        runId: options.runId,
        workerOptions: options.worker.options,
      }, options.hostTools)
    : await options.client.attachWorker({
        taskId: options.taskId,
        runId: options.runId,
        sessionId: options.worker.sessionId,
      }, options.hostTools);
  const snapshot = worker.snapshot;
  const processInfo = snapshot.process;
  const attachSessionId = options.worker.kind === "attach" ? options.worker.sessionId : undefined;
  const valid = snapshot.botUid === options.botUid
    && snapshot.taskId === options.taskId
    && snapshot.runId === options.runId
    && snapshot.runtimeId === options.runtimeId
    && processInfo !== null
    && snapshot.sessionId === worker.sessionId
    && snapshot.sessionFile === worker.sessionFile
    && (attachSessionId === undefined || snapshot.sessionId === attachSessionId)
    && worker.process !== null
    && worker.process.pid === processInfo.pid
    && worker.process.pgid === processInfo.pgid
    && worker.process.identity === processInfo.identity
    && worker.process.cwd === processInfo.cwd
    && (snapshot.stage === "omp-ready" || snapshot.stage === "active");
  if (!valid || processInfo === null) {
    const cwd = processInfo?.cwd
      ?? (options.worker.kind === "create" ? options.worker.options.cwd : undefined);
    if (cwd !== undefined) options.blockDirectory(cwd);
    if (options.worker.kind === "create") {
      await worker.stop().catch(() => false);
    } else {
      await options.client.detachWorker(options.taskId, options.runId).catch(() => {});
    }
    throw new Error("agent runtime returned an unverified worker identity");
  }
  const ledger: WorkerLedgerInput = {
    id: options.taskId,
    botUid: options.botUid,
    sessionId: snapshot.sessionId,
    sessionFile: snapshot.sessionFile,
    ...processInfo,
    moveTargetCwd: null,
    runtimeId: options.runtimeId,
    state: "running",
  };
  try {
    options.store.recordWorker(ledger);
  } catch (error) {
    options.blockDirectory(processInfo.cwd);
    if (options.worker.kind === "create") {
      const stopped = await worker.stop().catch(() => false);
      if (stopped) options.store.markWorkerStopped(options.taskId);
    } else {
      await options.client.detachWorker(options.taskId, options.runId).catch(() => {});
    }
    throw error;
  }

  let detachPromise: Promise<void> | undefined;
  let closePromise: Promise<boolean> | undefined;
  let detached = false;
  const detach = (): Promise<void> => {
    if (closePromise) return closePromise.then(() => {});
    if (!detachPromise) {
      detachPromise = options.client.detachWorker(options.taskId, options.runId).then(() => {
        detached = true;
      });
    }
    return detachPromise;
  };
  const close = (_recoverOnFailure = false): Promise<boolean> => {
    if (!closePromise) {
      closePromise = (async () => {
        let stopped = false;
        try {
          await detachPromise?.catch(() => {});
          if (detached) {
            worker = await options.client.attachWorker({
              taskId: options.taskId,
              runId: options.runId,
              sessionId: snapshot.sessionId,
            });
            detached = false;
          }
          stopped = await worker.stop();
        } catch {
          // A failed runtime stop is not confirmation that its process group exited.
        }
        if (stopped) options.store.markWorkerStopped(options.taskId);
        else options.blockDirectory(processInfo.cwd);
        return stopped;
      })();
    }
    return closePromise;
  };
  return {
    get worker() {
      return worker;
    },
    get snapshot() {
      return worker.snapshot;
    },
    close,
    detach,
  };
}

export async function openTrackedCommit(options: OpenTrackedCommitOptions): Promise<TrackedCommit> {
  return openTrackedProcess({
    workerFactory: options.workerFactory,
    store: options.store,
    create: (onSpawn) => options.workerFactory.createCommit({ ...options.worker, onSpawn }),
    ledger: options.ledger,
    blockDirectory: options.blockDirectory,
  });
}

/** Returns the candidate result only after the worker stops and its ledger is finalized. */
export async function withStoppedWorker<T>(
  options: OpenTrackedWorkerOptions,
  run: (worker: WorkerSession) => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  const lease = await openTrackedWorker({
    ...options,
    worker: signal === undefined ? options.worker : { ...options.worker, signal },
  });
  let onAbort: (() => void) | undefined;
  let abortPromise: Promise<never> | undefined;
  if (signal !== undefined) {
    const abort = Promise.withResolvers<never>();
    abortPromise = abort.promise;
    onAbort = () => abort.reject(
      signal.reason instanceof Error ? signal.reason : new Error("worker operation aborted"),
    );
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  }
  const operation = Promise.resolve().then(() => {
    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : new Error("worker operation aborted");
    }
    return run(lease.worker);
  });
  try {
    const result = abortPromise === undefined
      ? await operation
      : await Promise.race([operation, abortPromise]);
    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : new Error("worker operation aborted");
    }
    if (!(await lease.close())) throw new Error("worker did not stop");
    return result;
  } catch (error) {
    const stopped = await lease.close(signal?.aborted === true);
    if (signal?.aborted) {
      await operation.catch(() => {});
      if (!stopped) throw new Error("worker did not stop after cancellation");
    }
    throw error;
  } finally {
    if (signal !== undefined && onAbort !== undefined) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}
