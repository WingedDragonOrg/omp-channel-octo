import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { analyzeArchitecture } from "./architecture-metrics.ts";
import { measureRouting } from "./autoresearch-routing.ts";
import { measureWorker } from "./autoresearch-worker.ts";

// Fixed offline workload: real service/SQLite plus real RPC child processes.
// Fake external Bot/model boundaries; never register the resident Bot or call a
// model. Wall-clock deadlines are hang guards, not measurements. Temporary paths
// and process IDs do not enter scores. Behavioral regressions invalidate a run.
const routing = await measureRouting();
const worker = await measureWorker();
assert.equal(routing.length, 5);
assert.equal(worker.length, 5);
const checks = [...routing, ...worker];
for (const result of checks) {
  console.log(`${result.passed ? "PASS" : "FAIL"} ${result.name}${result.detail ? `: ${result.detail}` : ""}`);
}
assert.ok(checks.every(result => result.passed), "offline behavioral workload failed");

// Scan all production modules (including future domain modules), never just the
// current hotspot. Benchmark code and fixtures do not contribute to the score.
const root = new URL("../../", import.meta.url);
const paths = [...new Bun.Glob("src/**/*.{ts,tsx}").scanSync({ cwd: fileURLToPath(root), onlyFiles: true })]
  .filter(path => !path.endsWith(".d.ts"))
  .sort();
const sources = new Map<string, string>();
for (const path of paths) sources.set(path, await Bun.file(new URL(path, root)).text());
const result = analyzeArchitecture(sources);
// Runtime cycles compromise extraction boundaries; do not trade them for a
// smaller peak. This gate is part of the frozen harness, not an optimisation knob.
assert.equal(result.metrics.runtime_cycle_modules, 0, "runtime dependency cycle introduced");
for (const item of result.hotspots.slice(0, 8)) console.log(`HOTSPOT ${item.path} complexity=${item.complexity}`);
for (const [name, value] of Object.entries(result.metrics)) console.log(`METRIC ${name}=${value}`);
console.log(`METRIC behavior_checks_passed=${checks.length}`);
