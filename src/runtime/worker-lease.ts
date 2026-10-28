import type {
  OmpWorker,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { WorkerLedgerRecord } from "../store/store-records.ts";

export type WorkerSession = Omit<OmpWorker, "stop">;

export interface TrackedWorker {
  readonly worker: WorkerSession;
  /**
   * The first call selects recovery. All calls share the stop and ledger-finalization promise,
   * including a rejected finalization.
   */
  close(recoverOnFailure?: boolean): Promise<boolean>;
}

export interface OpenTrackedWorkerOptions {
  readonly workerFactory: WorkerFactory;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerStopped">;
  readonly worker: Omit<WorkerOptions, "onSpawn">;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state" | "moveTargetCwd">;
  readonly moveTargetCwd?: string;
  readonly blockDirectory: (cwd: string) => void;
}

function blockUnconfirmedWorker(
  options: OpenTrackedWorkerOptions,
  spawned: WorkerProcess | undefined,
): void {
  if (spawned) options.blockDirectory(spawned.cwd);
  if (options.moveTargetCwd !== undefined) options.blockDirectory(options.moveTargetCwd);
}

export async function openTrackedWorker(options: OpenTrackedWorkerOptions): Promise<TrackedWorker> {
  let spawned: WorkerProcess | undefined;
  let worker: OmpWorker;
  try {
    worker = await options.workerFactory.create({
      ...options.worker,
      onSpawn: (processInfo) => {
        spawned = processInfo;
        options.store.recordWorker({
          ...options.ledger,
          ...processInfo,
          moveTargetCwd: options.moveTargetCwd ?? null,
          state: "running",
        });
      },
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
        blockUnconfirmedWorker(options, spawned);
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
        else blockUnconfirmedWorker(options, spawned);
        return stopped;
      })();
    }
    return closePromise;
  };

  return { worker, close };
}

/** Returns the candidate result only after the worker stops and its ledger is finalized. */
export async function withStoppedWorker<T>(
  options: OpenTrackedWorkerOptions,
  run: (worker: WorkerSession) => Promise<T>,
): Promise<T> {
  const lease = await openTrackedWorker(options);
  let result: T;
  try {
    result = await run(lease.worker);
  } catch (error) {
    await lease.close();
    throw error;
  }
  if (!(await lease.close())) throw new Error("worker did not stop");
  return result;
}
