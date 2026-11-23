import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { runRestartBenchmark } from "./restart.ts";
import { connectAgentRuntime, type EnsureAgentRuntimeResult } from "../../src/runtime/agent-runtime.ts";

const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-aot-benchmark-")));
const binary = join(root, "dist", "oco");
const runtimeAssets = join(dirname(binary), "runtime", "omp");

async function connectToAotRuntime(
  child: Bun.Subprocess,
  stateDir: string,
  botUid: string,
): Promise<EnsureAgentRuntimeResult> {
  const deadline = Date.now() + 15_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`AOT agent runtime exited before becoming available: ${String(lastError)}`);
    }
    try {
      return await connectAgentRuntime({ stateDir, botUid });
    } catch (error) {
      lastError = error;
      await Bun.sleep(25);
    }
  }
  throw new Error(`AOT agent runtime did not become available: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
}

async function waitForChildExit(child: Bun.Subprocess, timeoutMs = 5_000): Promise<number> {
  const { promise, reject } = Promise.withResolvers<never>();
  const timer = setTimeout(() => reject(new Error("AOT agent runtime did not exit")), timeoutMs);
  try {
    return await Promise.race([child.exited, promise]);
  } finally {
    clearTimeout(timer);
  }
}

async function verifyAotRuntimeRestart(runtimeExecutable: string, root: string): Promise<void> {
  const stateDir = join(root, "aot-agent-runtime-state");
  const configPath = join(root, "aot-agent-runtime.json");
  const botUid = "aot-agent-runtime-benchmark";
  await writeFile(configPath, JSON.stringify({ stateDir, botUid, ompCommand: "/bin/false" }), { mode: 0o600 });
  // queryProcess fingerprints `ps lstart`; match the connector's timezone and locale.
  const env: NodeJS.ProcessEnv = {
    HOME: root,
    PATH: "/usr/bin:/bin",
    ...(process.env.LANG === undefined ? {} : { LANG: process.env.LANG }),
    ...(process.env.LC_ALL === undefined ? {} : { LC_ALL: process.env.LC_ALL }),
    ...(process.env.LC_TIME === undefined ? {} : { LC_TIME: process.env.LC_TIME }),
    ...(process.env.TZ === undefined ? {} : { TZ: process.env.TZ }),
  };
  const daemons: Bun.Subprocess[] = [];
  let connected: EnsureAgentRuntimeResult | undefined;
  try {
    const launch = (): Bun.Subprocess => {
      const child = Bun.spawn([runtimeExecutable, configPath], {
        cwd: root,
        env,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "inherit",
      });
      daemons.push(child);
      return child;
    };
    const first = launch();
    connected = await connectToAotRuntime(first, stateDir, botUid);
    assert.equal(connected.handle.pid, first.pid, "The standalone AOT daemon owns the runtime process");
    const firstRuntimeId = connected.handle.runtimeId;
    await connected.client.disconnect();
    await connected.handle.shutdown();
    assert.equal(await waitForChildExit(first), 0, "The standalone AOT daemon shuts down cleanly");
    connected = undefined;

    const second = launch();
    connected = await connectToAotRuntime(second, stateDir, botUid);
    assert.equal(connected.handle.pid, second.pid, "Cold start uses the standalone AOT daemon executable");
    assert.notEqual(connected.handle.runtimeId, firstRuntimeId, "A cold daemon restart creates a new runtime identity");
    await connected.client.disconnect();
    await connected.handle.shutdown();
    assert.equal(await waitForChildExit(second), 0, "The restarted AOT daemon shuts down cleanly");
    connected = undefined;
  } finally {
    if (connected) {
      await connected.client.disconnect().catch(() => {});
      await connected.handle.shutdown().catch(() => {});
    }
    for (const child of daemons) {
      if (child.exitCode !== null) continue;
      child.kill("SIGTERM");
      try {
        await waitForChildExit(child);
      } catch {
        child.kill("SIGKILL");
        await child.exited;
      }
    }
  }
}

const agentRuntimeExecutable = join(dirname(binary), "oco-agent-runtime");
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

  assert.ok(existsSync(agentRuntimeExecutable), "AOT agent runtime daemon is a separate executable");
  const extensionPath = join(runtimeAssets, "context-extension.ts");
  const goalConfigPath = join(runtimeAssets, "goal-rpc.yml");
  const ratchetConfigPath = join(runtimeAssets, "ratchet-rpc.yml");
  assert.ok(existsSync(ratchetConfigPath), "AOT ratchet worker config is packaged");
  assert.ok(!extensionPath.includes("$bunfs"), "OMP extension is an external filesystem asset");
  assert.ok(!goalConfigPath.includes("$bunfs"), "OMP goal config is an external filesystem asset");

  const loader = Bun.spawn([
    process.execPath,
    join(checkout, "tests/fixtures/aot-extension-loader.ts"),
    extensionPath,
    goalConfigPath,
  ], { cwd: root, stdin: "ignore", stdout: "inherit", stderr: "inherit" });
  assert.equal(await loader.exited, 0, "an independent Bun/OMP process loads the external extension and goal config");
  const previewTestPath = join(dirname(binary), "tests", "preview-card.acceptance.test.ts");
  assert.ok(existsSync(previewTestPath), "AOT preview test is a packaged external Bun test asset");
  await verifyAotRuntimeRestart(agentRuntimeExecutable, root);
  const previewBin = join(root, "preview-bin");
  const previewArgsPath = join(root, "preview-bun-args.txt");
  const previewEnvPath = join(root, "preview-env-status.txt");
  const explicitEnvPath = join(root, "external-preview.env");
  await mkdir(previewBin, { recursive: true });
  await writeFile(join(root, ".envrc"), "OCTO_BOT_TOKEN=checkout-token\nOCTO_PREVIEW_TARGET_UID=checkout-target\n");
  await writeFile(explicitEnvPath, "OCTO_BOT_TOKEN=explicit-token\n");
  await writeFile(join(previewBin, "bun"), `#!/bin/sh
printf '%s\\n' "$@" >> '${previewArgsPath}'
printf '%s|%s\\n' "\${OCTO_BOT_TOKEN:+set}" "\${OCTO_PREVIEW_TARGET_UID:+set}" >> '${previewEnvPath}'
`, { mode: 0o755 });
  const previewBaseEnv: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    PATH: `${previewBin}:${process.env.PATH ?? ""}`,
  };
  delete previewBaseEnv.OCTO_BOT_TOKEN;
  delete previewBaseEnv.OCTO_PREVIEW_TARGET_UID;
  delete previewBaseEnv.OMP_CHANNEL_OCTO_ENV_FILE;
  for (const env of [
    previewBaseEnv,
    { ...previewBaseEnv, OMP_CHANNEL_OCTO_ENV_FILE: explicitEnvPath },
  ]) {
    const preview = Bun.spawn([binary, "test", "--card"], {
      cwd: root,
      env,
      stdin: "ignore",
      stdout: "ignore",
      stderr: "pipe",
    });
    const [previewCode, previewError] = await Promise.all([
      preview.exited,
      new Response(preview.stderr).text(),
    ]);
    assert.equal(previewCode, 0, `AOT test --card runs the packaged test through Bun: ${previewError}`);
  }
  assert.deepEqual((await readFile(previewArgsPath, "utf8")).trim().split("\n"), [
    "test",
    previewTestPath,
    "test",
    previewTestPath,
  ]);
  assert.deepEqual((await readFile(previewEnvPath, "utf8")).trim().split("\n"), ["set|set", "set|"]);

  const help = Bun.spawn([binary, "--help"], { cwd: root, stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [helpCode, helpText, helpError] = await Promise.all([
    help.exited,
    new Response(help.stdout).text(),
    new Response(help.stderr).text(),
  ]);
  assert.equal(helpCode, 0, `AOT help succeeds: ${helpError}`);
  assert.match(helpText, /test --card/, "AOT help retains the preview command");
  const superBin = join(root, "bin");
  const superCalls = join(root, "super-calls.log");
  await mkdir(superBin, { recursive: true });
  const exportDocument = JSON.stringify({
    prune: false,
    services: [{ name: "aot-benchmark", command: binary, args: ["start"], cwd: root }],
  });
  await writeFile(join(superBin, "super"), `#!/bin/sh
printf '%s\\n' "$*" >> '${superCalls}'
if [ "$1" = export ]; then printf '%s' '${exportDocument}'; exit 0; fi
if [ "$1" = info ]; then printf 'State: Healthy\\nPID: %s\\n' "$$"; exit 0; fi
case " $* " in *" restart "*) exit 0;; esac
exit 2
`, { mode: 0o755 });
  const supervisorEnv = {
    ...process.env,
    HOME: root,
    PATH: `${superBin}:${process.env.PATH ?? ""}`,
  };
  const status = Bun.spawn([binary, "status"], {
    cwd: root,
    env: supervisorEnv,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [statusCode, statusOutput, statusError] = await Promise.all([
    status.exited,
    new Response(status.stdout).text(),
    new Response(status.stderr).text(),
  ]);
  assert.equal(statusCode, 0, `AOT status succeeds: ${statusError}`);
  assert.match(statusOutput, /程序 aot-benchmark/, "AOT status detects the executable outside its working directory");

  const restart = Bun.spawn([binary, "restart"], {
    cwd: root,
    env: supervisorEnv,
    stdin: "ignore",
    stdout: "ignore",
    stderr: "pipe",
  });
  const [restartCode, restartError] = await Promise.all([
    restart.exited,
    new Response(restart.stderr).text(),
  ]);
  assert.equal(restartCode, 0, `AOT restart detects the executable outside its working directory: ${restartError}`);
  assert.match(await readFile(superCalls, "utf8"), /-y restart --wait-healthy --timeout 60 aot-benchmark/);



  await runRestartBenchmark(binary);
  console.log("AOT offline CLI, standalone agent-runtime cold start/restart, supervisor detection, watchdog, extension assets, and normal message path passed");
} finally {
  await rm(root, { recursive: true, force: true });
}
