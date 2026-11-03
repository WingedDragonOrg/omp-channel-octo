import { appendFileSync, readFileSync, writeFileSync } from "node:fs";

interface FixtureConfig {
  scenario: "commit" | "commit-exit-1" | "fail" | "hang" | "commit-then-hang" | "interruptible-hang";
  pidPath: string;
  readyPath?: string;
  descendantPidPath?: string;
  argvPath?: string;
  cwdPath?: string;
  startedPath?: string;
  outputBytes?: number;
  exitCode?: number;
}

function flag(name: string): string | undefined {
  const argv = process.argv.slice(2);
  for (let index = argv.length - 1; index >= 0; index -= 1) {
    const value = argv[index];
    if (value === name) return argv[index + 1];
    if (value?.startsWith(`${name}=`)) return value.slice(name.length + 1);
  }
  return undefined;
}

function finishWith(code: number): never {
  process.exitCode = code;
  // Returning to Bun lets it flush piped output before exit.
  throw new Error(`fixture-exit:${code}`);
}

function runGit(args: string[]): number {
  const result = Bun.spawnSync(["git", ...args], {
    cwd: process.cwd(),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  if (result.exitCode !== 0) {
    const error = new TextDecoder().decode(result.stderr);
    if (error.length > 0) process.stderr.write(error);
  }
  return result.exitCode;
}

function makeCommit(): void {
  const addExit = runGit(["add", "-A"]);
  if (addExit !== 0) finishWith(addExit);
  const commitExit = runGit(["-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "native commit fixture"]);
  if (commitExit !== 0) finishWith(commitExit);
  process.stdout.write("Commit created.\n");
}

function spawnDescendant(path: string, ignoreSigterm: boolean): void {
  const script = [
    'const fs = require("node:fs");',
    ...(ignoreSigterm ? ['process.on("SIGTERM", () => {});'] : []),
    `fs.writeFileSync(${JSON.stringify(path)}, String(process.pid));`,
    "setInterval(() => {}, 1_000);",
  ].join("\n");
  Bun.spawn([process.execPath, "-e", script], {
    stdin: "ignore",
    stdout: "ignore",
    stderr: "ignore",
  });
}

async function writeLarge(stream: NodeJS.WriteStream, bytes: number): Promise<void> {
  const chunk = "x".repeat(Math.min(Math.max(bytes, 0), 64 * 1024));
  let remaining = Math.max(bytes, 0);
  while (remaining > 0) {
    const part = chunk.slice(0, Math.min(chunk.length, remaining));
    if (!stream.write(part)) await new Promise<void>(resolve => stream.once("drain", resolve));
    remaining -= part.length;
  }
}

async function main(): Promise<void> {
  const configPath = flag("--fixture-config");
  if (!configPath) throw new Error("native commit fixture requires --fixture-config");
  const config = JSON.parse(readFileSync(configPath, "utf8")) as FixtureConfig;
  const argv = process.argv.slice(2);
  if (!argv.includes("commit")) throw new Error("native commit fixture was not invoked with the commit subcommand");
  if (config.startedPath) appendFileSync(config.startedPath, `${process.pid}\n`);
  writeFileSync(config.pidPath, String(process.pid));
  if (config.argvPath) writeFileSync(config.argvPath, JSON.stringify(argv));
  if (config.cwdPath) writeFileSync(config.cwdPath, process.cwd());

  if (config.scenario === "hang" || config.scenario === "commit-then-hang") {
    process.on("SIGTERM", () => {});
  }

  if (config.scenario === "commit" || config.scenario === "commit-exit-1" || config.scenario === "commit-then-hang") {
    makeCommit();
  }

  if (config.scenario === "fail") {
    const bytes = config.outputBytes ?? 1_048_576;
    await Promise.all([writeLarge(process.stdout, bytes), writeLarge(process.stderr, bytes)]);
    process.exitCode = config.exitCode ?? 7;
    return;
  }

  if (config.scenario === "commit-exit-1") {
    process.exitCode = 1;
    return;
  }

  if (config.scenario === "hang" || config.scenario === "commit-then-hang" || config.scenario === "interruptible-hang") {
    if (config.descendantPidPath) {
      spawnDescendant(config.descendantPidPath, config.scenario !== "interruptible-hang");
      const deadline = Date.now() + 2_000;
      while (Date.now() < deadline) {
        try {
          readFileSync(config.descendantPidPath, "utf8");
          break;
        } catch {
          await Bun.sleep(10);
        }
      }
    }
    if (config.readyPath) writeFileSync(config.readyPath, String(process.pid));
    await new Promise<never>(() => {});
  }

  process.exitCode = 0;
}

main().catch(error => {
  if (error instanceof Error && error.message.startsWith("fixture-exit:")) return;
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 2;
});
