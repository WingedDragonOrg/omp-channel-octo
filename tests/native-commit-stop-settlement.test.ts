import { expect, test } from "bun:test";
import { resolve } from "node:path";

const fixturePath = resolve(import.meta.dir, "fixtures/native-commit-stop-settlement.ts");

test("stop=false promptly rejects run as unknown and preserves the watchdog", async () => {
  const child = Bun.spawn([process.execPath, fixturePath], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  let finished = false;
  try {
    const completion = Promise.all([child.exited, stdout, stderr]).then(result => {
      finished = true;
      return result;
    });
    const result = await Promise.race([
      completion,
      Bun.sleep(15_000).then(() => undefined),
    ]);
    if (!result) throw new Error("settlement fixture did not exit");
    const [exitCode, output, errorOutput] = result;
    if (exitCode !== 0) throw new Error(`settlement fixture exited ${exitCode}: ${errorOutput}`);
    const payload = JSON.parse(output.trim()) as {
      stopResult: boolean;
      runStatus: string;
      groupAliveAfterStop: boolean;
      watchdogAliveAfterStop: boolean;
      watchdogReaped: boolean;
    };
    expect(payload).toEqual({
      stopResult: false,
      runStatus: "rejected",
      groupAliveAfterStop: true,
      watchdogAliveAfterStop: true,
      watchdogReaped: true,
    });
  } finally {
    if (!finished) {
      child.kill("SIGKILL");
      await child.exited;
    }
    await Promise.all([stdout.catch(() => ""), stderr.catch(() => "")]);
  }
}, 30_000);
