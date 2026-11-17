import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const extensionPath = process.argv[2];
const goalConfigPath = process.argv[3];
assert.ok(extensionPath, "compiled context extension path is required");
assert.ok(goalConfigPath, "compiled goal config path is required");

// The absolute module is the compiled artifact under test; a static import cannot verify its disk loading.
const extension = await import(pathToFileURL(extensionPath).href);
assert.equal(typeof extension.default, "function", "external OMP extension exports its loader");
const registered: string[] = [];
extension.default({ on: (event: string) => registered.push(event) });
assert.deepEqual(registered, [
  "before_agent_start",
  "input",
  "context",
  "session_before_compact",
  "session.compacting",
]);

assert.equal(await readFile(goalConfigPath, "utf8"), "goal:\n  continuationModes:\n    - rpc\n");
console.log("AOT external OMP assets loaded");
