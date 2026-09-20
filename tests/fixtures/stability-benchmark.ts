import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

// Freeze this workload during optimization. Additional regressions belong in other tests.
const suites = [
  "tests/core-recovery.test.ts",
  "tests/core-context-recovery.test.ts",
  "tests/core-progress-card-recovery.test.ts",
  "tests/core-outbox.test.ts",
  "tests/core-streaming.test.ts",
  "tests/core-cancel.test.ts",
  "tests/core-concurrency.test.ts",
  "tests/core-startup-failure.test.ts",
  "tests/omp-worker.test.ts",
  "tests/omp-watchdog.test.ts",
];
const rounds = 3;
const expectedCases = 100;
const root = resolve(import.meta.dir, "../..");
const scratch = await mkdtemp(join(tmpdir(), "oco-stability-"));
let failures = 0;
let runnerErrors = 0;
let executed = 0;
let passed = 0;

function countAttribute(tag: string, name: string): number {
  const match = new RegExp(`\\b${name}="(\\d+)"`).exec(tag);
  if (!match) throw new Error(`Missing JUnit counter: ${name}`);
  const value = Number(match[1]);
  if (!Number.isSafeInteger(value)) throw new Error(`Invalid JUnit counter: ${name}`);
  return value;
}

try {
  await mkdir(join(scratch, "home"));
  for (let round = 1; round <= rounds; round++) {
    const report = join(scratch, `round-${round}.xml`);
    const result = spawnSync(process.execPath, [
      "test",
      "--no-orphans",
      "--preload", "./tests/fixtures/stability-preload.ts",
      "--seed=20260920",
      "--retry=0",
      "--max-concurrency=1",
      "--reporter=junit",
      `--reporter-outfile=${report}`,
      ...suites.map((suite) => `./${suite}`),
    ], {
      cwd: root,
      env: {
        PATH: process.env.PATH ?? "/usr/bin:/bin",
        HOME: join(scratch, "home"),
        TMPDIR: scratch,
        TZ: "UTC",
        LANG: "C",
        LC_ALL: "C",
        NO_COLOR: "1",
      },
      encoding: "utf8",
      timeout: 120_000,
      killSignal: "SIGTERM",
      maxBuffer: 8 * 1024 * 1024,
    });
    try {
      const xml = await readFile(report, "utf8");
      const summary = /<testsuites\b[^>]*>/.exec(xml)?.[0];
      if (!summary || !xml.trimEnd().endsWith("</testsuites>")) {
        throw new Error("Incomplete JUnit report");
      }
      const tests = countAttribute(summary, "tests");
      const failed = countAttribute(summary, "failures");
      const skipped = countAttribute(summary, "skipped");
      const errors = /\berrors="/.test(summary) ? countAttribute(summary, "errors") : 0;
      if (failed + skipped + errors > tests) throw new Error("Inconsistent JUnit counters");
      failures += failed;
      runnerErrors += errors;
      executed += tests - skipped;
      passed += tests - failed - skipped - errors;
      console.log(`Round ${round}/${rounds}: tests=${tests} failures=${failed} skipped=${skipped} errors=${errors}`);
      const files = new Set([...xml.matchAll(/<testsuite\b[^>]*\bfile="([^"]+)"/g)].map((match) => match[1]));
      if (tests !== expectedCases || skipped !== 0 || files.size !== suites.length || suites.some((suite) => !files.has(suite))) {
        throw new Error("Fixed workload changed, skipped tests, or missing suites");
      }
      if (result.error || result.signal || (result.status !== 0 && failed + errors === 0)) {
        throw new Error(`Runner failed: ${result.error?.message ?? result.signal ?? result.status}`);
      }
      if (failed + errors > 0) {
        process.stderr.write(result.stdout ?? "");
        process.stderr.write(result.stderr ?? "");
      }
    } catch (error) {
      runnerErrors++;
      console.error(`Round ${round}: ${error instanceof Error ? error.message : String(error)}`);
      process.stderr.write(result.stdout ?? "");
      process.stderr.write(result.stderr ?? "");
    }
  }
} finally {
  await rm(scratch, { recursive: true, force: true });
}

// Failed assertions and broken/incomplete runs both make the primary metric worse.
console.log(`METRIC stability_failures=${failures + runnerErrors}`);
console.log(`METRIC cases_passed=${passed}`);
console.log(`METRIC cases_executed=${executed}`);
console.log(`METRIC runner_errors=${runnerErrors}`);
process.exitCode = failures + runnerErrors === 0 && executed === rounds * expectedCases ? 0 : 1;
