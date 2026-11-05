import type {
  CommitWorker,
  CommitWorkerOptions,
  OmpWorker,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { WorkerLedgerRecord } from "../store/store-records.ts";

export type WorkerSession = Omit<OmpWorker, "stop">;
export type CommitSession = Omit<CommitWorker, "stop">;

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
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
  readonly moveTargetCwd?: string;
  readonly recordWorker?: (worker: WorkerLedgerRecord) => boolean;
  readonly blockDirectory: (cwd: string) => void;
}

export interface OpenTrackedCommitOptions {
  readonly workerFactory: WorkerFactory;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly worker: Omit<CommitWorkerOptions, "onSpawn">;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
  readonly blockDirectory: (cwd: string) => void;
}

interface StoppableWorker {
  stop(): Promise<boolean>;
}

interface OpenTrackedProcessOptions<TWorker extends StoppableWorker> {
  readonly workerFactory: Pick<WorkerFactory, "recover">;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly create: (onSpawn: (process: WorkerProcess) => void) => Promise<TWorker>;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
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
