import assert from "node:assert/strict";
import { measureRouting } from "./autoresearch-routing.ts";
import { measureWorker } from "./autoresearch-worker.ts";

// An integration capability score, not a latency benchmark. A completed measurement
// exits successfully even when capabilities are missing; infrastructure errors fail.
// Keep these ten cases fixed while improving the adapter so scores stay comparable.
const routing = await measureRouting();
const worker = await measureWorker();
assert.equal(routing.length, 5, "routing workload must execute all five cases");
assert.equal(worker.length, 5, "worker workload must execute all five cases");
const results = [...routing, ...worker];
assert.equal(new Set(results.map(result => result.name)).size, 10, "case names must be unique");
for (const result of results) {
  console.log(`${result.passed ? "PASS" : "GAP"} ${result.name}${result.detail ? `: ${result.detail}` : ""}`);
}
const routingGaps = routing.filter(result => !result.passed).length;
const workerGaps = worker.filter(result => !result.passed).length;
console.log(`METRIC adaptation_gaps=${routingGaps + workerGaps}`);
console.log(`METRIC routing_gaps=${routingGaps}`);
console.log(`METRIC worker_gaps=${workerGaps}`);
console.log(`METRIC checks_passed=${10 - routingGaps - workerGaps}`);
console.log("METRIC checks_total=10");
