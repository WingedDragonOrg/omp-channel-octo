import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { runRestartBenchmark } from "./restart.ts";

const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-aot-benchmark-")));
const binary = join(root, "dist", "oco");
const runtimeAssets = join(dirname(binary), "runtime", "omp");

try {
  const build = Bun.spawn([process.execPath, join(checkout, "scripts/build-aot.ts"), binary], {
    cwd: checkout,
    stdin: "ignore",
    stdout: "inherit",
    stderr: "inherit",
  });
  const buildCode = await build.exited;
  assert.equal(buildCode, 0, "AOT channel build succeeds");
  assert.ok(existsSync(join(dirname(binary), "oco-watchdog")), "AOT watchdog companion is built");

  const extensionPath = join(runtimeAssets, "context-extension.ts");
  const goalConfigPath = join(runtimeAssets, "goal-rpc.yml");
  assert.ok(!extensionPath.includes("$bunfs"), "OMP extension is an external filesystem asset");
  assert.ok(!goalConfigPath.includes("$bunfs"), "OMP goal config is an external filesystem asset");

  const loader = Bun.spawn([
    process.execPath,
    join(checkout, "tests/fixtures/aot-extension-loader.ts"),
    extensionPath,
    goalConfigPath,
  ], { cwd: root, stdin: "ignore", stdout: "inherit", stderr: "inherit" });
  assert.equal(await loader.exited, 0, "an independent Bun/OMP process loads the external extension and goal config");

  await runRestartBenchmark(binary);
  console.log("AOT offline CLI, watchdog, extension assets, and normal message path passed");
} finally {
  await rm(root, { recursive: true, force: true });
}
