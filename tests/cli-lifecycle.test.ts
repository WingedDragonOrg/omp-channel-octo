import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type OctoProtocolFixture,
} from "./fixtures/octo-protocol-server.ts";

async function waitForChildExit(child: Bun.Subprocess, timeoutMs = 5_000): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  // This integration test controls an external CLI process, so a real timeout is required to bound a hang.
  const timer = setTimeout(() => timeout.reject(new Error("CLI child did not exit in time")), timeoutMs);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

test("real CLI serve handles a status message and exits cleanly on SIGTERM", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let outputPromise: Promise<[string, string]> | undefined;
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-lifecycle-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const configPath = join(currentRoot, "config.json");
    await writeFile(
      configPath,
      JSON.stringify({
        apiUrl: currentFixture.apiUrl,
        botToken: currentFixture.botToken,
        ownerId: currentFixture.ownerUid,
        allowedGroupIds: [],
        stateDir: join(currentRoot, "state"),
        ompCommand: process.execPath,
      }),
      { encoding: "utf8", mode: 0o600 },
    );

    const spawned = Bun.spawn(
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "serve", "--config", configPath],
      { stdout: "pipe", stderr: "pipe" },
    );
    child = spawned;
    const stdout = new Response(spawned.stdout).text();
    const stderr = new Response(spawned.stderr).text();
    const capturedOutput = Promise.all([stdout, stderr]);
    outputPromise = capturedOutput;

    const connection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => connection.ready ? connection : undefined,
      5_000,
      "CLI did not authenticate its WebSocket connection",
    );
    currentFixture.sendInbound(connection, {
      fromUid: currentFixture.ownerUid,
      channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
      channelType: 1,
      text: "/status",
      messageId: "9007199254741999",
    });

    const statusReply = await waitForFixture(
      () => currentFixture.sentMessages.find(({ body }) => body.channel_id === currentFixture.ownerUid),
      5_000,
      "CLI did not send the status response through REST",
    );
    expect(statusReply.body).toMatchObject({
      channel_id: currentFixture.ownerUid,
      channel_type: 1,
      payload: {
        type: 1,
        content: expect.stringContaining("state=none"),
      },
    });

    spawned.kill("SIGTERM");
    const exitCode = await waitForChildExit(spawned);
    const [stdoutText, stderrText] = await capturedOutput;
    expect(exitCode).toBe(0);
    await waitForFixture(
      () => connection.closed ? connection : undefined,
      5_000,
      "CLI did not close the Octo WebSocket connection",
    );
    expect(connection.closed).toBe(true);
    expect(stdoutText).not.toContain(currentFixture.botToken);
    expect(stderrText).not.toContain(currentFixture.botToken);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (outputPromise) await outputPromise.catch(() => {});
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
});
