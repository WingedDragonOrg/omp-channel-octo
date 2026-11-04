import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { WorktreeMergeLifecycle } from "../src/contracts.ts";
import { groupAlive } from "../src/omp/process.ts";
import { runCommand } from "../src/workspace/worktrunk.ts";

test("registers the verified process identity before the watchdog can fail", async () => {
  const directory = await mkdtemp(join(tmpdir(), "octo-worktrunk-guard-"));
  try {
    const marker = join(directory, "must-not-run");
    const events: string[] = [];
    let registeredPgid = 0;
    const lifecycle: WorktreeMergeLifecycle = {
      beforeSpawn: () => { events.push("reserved"); },
      onSpawn: (identity) => {
        registeredPgid = identity.pgid;
        events.push("spawned");
      },
      onConfirmedExit: () => { events.push("exited"); },
      complete: () => {},
      settle: () => {},
      onNotSpawned: () => { events.push("not-spawned"); },
      onUnknown: () => { events.push("unknown"); },
    };

    const failure = await runCommand(
      [process.execPath, "-e", `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "ran")`],
      {
        cwd: directory,
        timeoutMs: 5_000,
        parentDeathWatchdog: true,
        operation: "Watch check",
        watchdogSpawn: () => {
          throw new Error("watchdog spawn rejected");
        },
        lifecycle,
      },
    ).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(failure).toBeInstanceOf(Error);
    expect(events).toEqual(["reserved", "spawned", "not-spawned"]);
    expect(registeredPgid).toBeGreaterThan(1);
    expect(groupAlive(registeredPgid)).toBe(false);
    expect(existsSync(marker)).toBe(false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 10_000);
