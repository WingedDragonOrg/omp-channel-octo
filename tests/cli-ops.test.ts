import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CHECKOUT_ROOT = resolve(import.meta.dir, "..");
const CLI = resolve(import.meta.dir, "../src/cli.ts");

interface OpsRoot {
  readonly root: string;
  readonly home: string;
  readonly binDir: string;
  readonly callsPath: string;
}

/**
 * A stand-in for the real `super` client: it records every invocation and answers the calls the
 * CLI makes — the declarative export, one program's live state, and restart.
 */
async function createOpsRoot(prefix: string): Promise<OpsRoot> {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const home = join(root, "home");
  const binDir = join(root, "bin");
  const callsPath = join(root, "super-calls.log");
  const counterPath = join(root, "pid-counter");
  await mkdir(binDir, { recursive: true });
  await mkdir(home, { recursive: true });
  await writeFile(counterPath, "0", "utf8");
  const checkout = await realpath(CHECKOUT_ROOT);
  await writeFile(
    join(binDir, "super"),
    `#!/bin/sh
printf '%s\\n' "$*" >> '${callsPath}'
for arg in "$@"; do
  case "$arg" in
    export)
      printf '{"prune":false,"services":[{"name":"omp-channel-octo","command":"/bin/sh","args":["-c",". ./.envrc; exec bun run src/cli.ts start --config /tmp/oco-ops-config.json"],"cwd":"${checkout}"}]}'
      exit 0 ;;
    info)
      count=$(cat '${counterPath}')
      count=$((count + 1))
      printf '%s' "$count" > '${counterPath}'
      printf 'PID: %s\\nState: Healthy\\n' "$((1000 + count))"
      exit 0 ;;
    restart)
      exit 0 ;;
  esac
done
exit 2
`,
    { mode: 0o755 },
  );
  return { root, home, binDir, callsPath };
}

function cliEnvironment(ops: OpsRoot, path = `${ops.binDir}:${process.env.PATH ?? ""}`): Record<string, string> {
  return { ...process.env as Record<string, string>, HOME: ops.home, PATH: path };
}

async function runCli(
  args: readonly string[],
  ops: OpsRoot,
  env = cliEnvironment(ops),
): Promise<{ code: number; stdout: string; stderr: string }> {
  const child = Bun.spawn([process.execPath, CLI, ...args], { stdout: "pipe", stderr: "pipe", env });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

test("restart detects the program itself, waits for health, and reports the PID change", async () => {
  const ops = await createOpsRoot("oco-cli-restart-");
  try {
    const result = await runCli(["restart"], ops);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain("守护进程：super（程序 omp-channel-octo）");
    expect(result.stdout).toContain("状态：Healthy");
    expect(result.stdout).toContain("PID：1001 → 1002");
    expect(result.stderr).not.toContain("警告");

    const calls = await readFile(ops.callsPath, "utf8");
    expect(calls).toContain("-y restart --wait-healthy --timeout 60 omp-channel-octo");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("restart refuses to guess when no supervisor program matches this checkout", async () => {
  const ops = await createOpsRoot("oco-cli-restart-none-");
  try {
    await writeFile(
      join(ops.binDir, "super"),
      `#!/bin/sh
printf '%s\\n' "$*" >> '${ops.callsPath}'
printf '{"prune":false,"services":[{"name":"other","command":"/usr/bin/node","args":["server.js"],"cwd":"/srv/other"}]}'
exit 0
`,
      { mode: 0o755 },
    );
    const result = await runCli(["restart"], ops);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("未检测到守护进程程序");
    expect(await readFile(ops.callsPath, "utf8")).not.toContain("--wait-healthy");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("a missing super client is reported instead of failing silently", async () => {
  const ops = await createOpsRoot("oco-cli-restart-missing-");
  try {
    const result = await runCli(["restart"], ops, cliEnvironment(ops, "/nonexistent-bin"));
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("未找到 super 可执行文件");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("status reports the supervisor state, the config path, and the current log file", async () => {
  const ops = await createOpsRoot("oco-cli-status-");
  try {
    const logsDir = join(ops.home, ".oco", "logs");
    await mkdir(logsDir, { recursive: true });
    const logFile = join(logsDir, "channel-2026-09-16.log");
    await writeFile(logFile, "[2026-09-16 10:00:00] boot\n", "utf8");

    const result = await runCli(["status"], ops);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain("守护进程：super（程序 omp-channel-octo）");
    expect(result.stdout).toContain("状态：Healthy");
    expect(result.stdout).toContain("PID：1001");
    expect(result.stdout).toContain(`配置：${join(ops.home, ".oco", "config", "config.json")}`);
    expect(result.stdout).toContain(`日志：${logFile}`);
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("logs prints the tail of the newest service log", async () => {
  const ops = await createOpsRoot("oco-cli-logs-");
  try {
    const logsDir = join(ops.home, ".oco", "logs");
    await mkdir(logsDir, { recursive: true });
    await writeFile(
      join(logsDir, "channel-2026-09-16.log"),
      "[2026-09-16 10:00:00] first\n[2026-09-16 10:00:01] second\n[2026-09-16 10:00:02] third\n",
      "utf8",
    );
    const result = await runCli(["logs", "--tail", "2"], ops);
    expect(result.code).toBe(0);
    expect(result.stdout).toBe("[2026-09-16 10:00:01] second\n[2026-09-16 10:00:02] third\n");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("logs without any file says where it looked", async () => {
  const ops = await createOpsRoot("oco-cli-logs-empty-");
  try {
    const result = await runCli(["logs"], ops);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain(join(ops.home, ".oco", "logs"));
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("test --card runs the acceptance file through the checkout's envrc", async () => {
  const ops = await createOpsRoot("oco-cli-test-");
  try {
    // The real `bun` would send a card; the stand-in only reports what it was asked to run and
    // which variables it received, so the env handling is observable without touching Octo.
    await writeFile(
      join(ops.binDir, "bun"),
      `#!/bin/sh
echo "args: $*"
echo "token-set: \${OCTO_BOT_TOKEN:+yes}"
echo "target-set: \${OCTO_PREVIEW_TARGET_UID:+yes}"
`,
      { mode: 0o755 },
    );
    const result = await runCli(["test", "--card"], ops);
    expect(result.code).toBe(0);
    expect(result.stdout).toContain("test");
    expect(result.stdout).toContain(join(CHECKOUT_ROOT, "tests", "preview-card.acceptance.ts"));
    if (existsSync(join(CHECKOUT_ROOT, ".envrc"))) {
      expect(result.stdout).toContain("token-set: yes");
    }
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("test without a flag reports the usage instead of running anything", async () => {
  const ops = await createOpsRoot("oco-cli-test-usage-");
  try {
    const result = await runCli(["test"], ops);
    expect(result.code).toBe(1);
    expect(result.stderr).toContain("test --card");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});

test("the retired serve verb and unknown flags are refused with the usage text", async () => {
  const ops = await createOpsRoot("oco-cli-usage-");
  try {
    const retired = await runCli(["serve"], ops);
    expect(retired.code).toBe(1);
    expect(retired.stderr).toContain("不支持的命令：serve");
    expect(retired.stderr).toContain("start [--config PATH]");

    const unknown = await runCli(["logs", "--limit", "5"], ops);
    expect(unknown.code).toBe(1);
    expect(unknown.stderr).toContain("不支持的参数：--limit");
  } finally {
    await rm(ops.root, { recursive: true, force: true });
  }
});
