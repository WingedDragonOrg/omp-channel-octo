import type {
  OmpWorker,
  WorkerFactory,
  WorkerOptions,
  WorkerProcess,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type { WorkerLedgerRecord } from "../store/store-records.ts";

export interface TrackedWorker {
  readonly worker: OmpWorker;
  markMoveTarget(cwd: string): void;
  close(): Promise<boolean>;
}

export interface OpenTrackedWorkerOptions {
  readonly workerFactory: WorkerFactory;
  readonly store: Pick<ChannelStore, "recordWorker" | "markWorkerMoveTarget" | "markWorkerStopped">;
  readonly worker: Omit<WorkerOptions, "onSpawn">;
  readonly ledger: Omit<WorkerLedgerRecord, keyof WorkerProcess | "state">;
  readonly blockDirectories: readonly string[];
  readonly blockDirectory: (cwd: string) => void;
}

function blockDirectories(options: OpenTrackedWorkerOptions): void {
  for (const cwd of options.blockDirectories) {
    try {
      options.blockDirectory(cwd);
    } catch {
      // Best-effort cleanup must not replace the worker lifecycle result.
    }
  }
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
        blockDirectories(options);
      }
    }
    throw error;
  }

  return {
    worker,
    markMoveTarget: (cwd) => options.store.markWorkerMoveTarget(options.ledger.id, cwd),
    close: async () => {
      let stopped = false;
      try {
        stopped = await worker.stop();
      } catch {
        // A rejected stop is not confirmation that the process group has exited.
      }
      if (stopped) options.store.markWorkerStopped(options.ledger.id);
      else blockDirectories(options);
      return stopped;
    },
  };
}
