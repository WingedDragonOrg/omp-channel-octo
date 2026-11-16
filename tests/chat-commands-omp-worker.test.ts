import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "bun:test";
import { createWorkerFactory } from "../src/omp/index.ts";
import type { OmpWorker, WorkerOptions } from "../src/contracts.ts";

interface OmpCommandMetadata {
  name: string;
  source: "builtin" | "file" | "skill";
  description?: string;
  hint?: string;
}

interface CommandCatalogWorker extends OmpWorker {
  availableCommands(signal?: AbortSignal): Promise<readonly OmpCommandMetadata[]>;
  subscribeAvailableCommands(listener: (commands: readonly OmpCommandMetadata[]) => void): () => void;
}

const fixturePath = resolve(import.meta.dir, "fixtures/command-catalog-rpc-worker.ts");

test("OMP command metadata comes from RPC and live available_commands_update frames", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "omp-command-catalog-rpc-"));
  const factory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [fixturePath],
    startupTimeoutMs: 2_000,
    cancelTimeoutMs: 800,
  });
  const options: WorkerOptions = {
    cwd: tempDir,
    sessionDir: tempDir,
    onSpawn: () => undefined,
  };
  let worker: OmpWorker | undefined;
  let stopped = false;

  try {
    worker = await factory.create(options);
    const commandWorker = worker as CommandCatalogWorker;
    const update = Promise.withResolvers<readonly OmpCommandMetadata[]>();
    const unsubscribe = commandWorker.subscribeAvailableCommands(commands => {
      if (commands.some(command => command.name === "updated-skill-command")) update.resolve(commands);
    });
    try {
      const initial = await commandWorker.availableCommands();
      expect(initial).toEqual([{
        name: "project-file-command",
        source: "file",
        description: "Project command",
        hint: "<topic>",
      }]);

      const updated = await update.promise;
      expect(updated).toEqual([{
        name: "updated-skill-command",
        source: "skill",
        description: "Updated command",
        hint: "<query>",
      }]);
    } finally {
      unsubscribe();
    }
  } finally {
    if (worker !== undefined) {
      stopped = await worker.stop();
      expect(stopped).toBe(true);
    }
    if (stopped || worker === undefined) await rm(tempDir, { recursive: true, force: true });
  }
});
