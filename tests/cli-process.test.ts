import { expect, test } from "bun:test";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { cliEnvironment, observeCli } from "./fixtures/cli-process.ts";

// The subprocess owns the polluted environment; the shared test runner's environment stays intact.
test("CLI fixtures disable inherited HTTP status configuration", async () => {
  const child = Bun.spawn([process.execPath, "-e", `
    import { cliEnvironment } from ${JSON.stringify(resolve(import.meta.dir, "fixtures/cli-process.ts"))};
    import { parseConfig } from ${JSON.stringify(resolve(import.meta.dir, "../src/runtime/config.ts"))};
    const config = parseConfig({ botToken: "fixture-token", allowedGroupIds: [], ompCommand: process.execPath }, {
      env: cliEnvironment(${JSON.stringify(tmpdir())}),
    });
    console.log(JSON.stringify(config.statusHttp ?? null));
  `], {
    env: {
      ...process.env,
      OMP_CHANNEL_OCTO_HTTP_ENABLED: "true",
      OMP_CHANNEL_OCTO_HTTP_LISTEN_ADDRESS: "0.0.0.0",
      OMP_CHANNEL_OCTO_HTTP_PORT: "8765",
      OMP_CHANNEL_OCTO_HTTP_TOKEN: "ab".repeat(32),
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  expect(code, stderr).toBe(0);
  expect(JSON.parse(stdout)).toBeNull();
});

test("CLI fixture waits report early exit code and drained output", async () => {
  const child = Bun.spawn([process.execPath, "-e", `
    console.log("fixture startup output");
    console.error("fixture startup failure");
    process.exit(23);
  `], { env: cliEnvironment(tmpdir()), stdout: "pipe", stderr: "pipe" });
  const observed = observeCli(child);
  try {
    const error = await observed.waitFor(() => undefined, 5_000, "expected CLI response")
      .then(() => undefined, (error: unknown) => error);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain("expected CLI response: CLI exited with code 23");
    expect((error as Error).message).toContain("fixture startup output");
    expect((error as Error).message).toContain("fixture startup failure");
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await observed.output;
  }
});
