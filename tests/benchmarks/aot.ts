import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
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
  console.log("AOT offline CLI, supervisor detection, watchdog, extension assets, and normal message path passed");
} finally {
  await rm(root, { recursive: true, force: true });
}
