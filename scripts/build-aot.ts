import { spawnSync } from "node:child_process";
import { chmod, mkdir, copyFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const executablePath = resolve(process.argv[2] ?? join(projectRoot, "dist", "oco"));
const watchdogPath = join(dirname(executablePath), "oco-watchdog");
const runtimeOmpDir = join(dirname(executablePath), "runtime", "omp");

function runBun(args: string[]): void {
  const result = spawnSync(process.execPath, args, {
    cwd: projectRoot,
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`bun ${args[0]} failed with exit code ${result.status}`);
}

await mkdir(dirname(executablePath), { recursive: true });
await mkdir(runtimeOmpDir, { recursive: true });

runBun([
  "build",
  "--compile",
  "--aot",
  "--define",
  "OMP_CHANNEL_OCTO_AOT=true",
  join(projectRoot, "src", "cli.ts"),
  "--outfile",
  executablePath,
]);
runBun([
  "build",
  "--compile",
  "--aot",
  join(projectRoot, "src", "omp", "watchdog-entry.ts"),
  "--outfile",
  watchdogPath,
]);

runBun([
  "build",
  "--target=bun",
  join(projectRoot, "src", "omp", "context-extension.ts"),
  "--outfile",
  join(runtimeOmpDir, "context-extension.ts"),
]);
await copyFile(join(projectRoot, "src", "omp", "goal-rpc.yml"), join(runtimeOmpDir, "goal-rpc.yml"));
await chmod(executablePath, 0o755);
await chmod(watchdogPath, 0o755);
console.log(`Built AOT channel: ${executablePath}`);
console.log(`Built AOT watchdog: ${watchdogPath}`);
console.log(`External OMP assets: ${runtimeOmpDir}`);
