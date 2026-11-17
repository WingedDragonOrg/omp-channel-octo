import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import { openChannelStore } from "../src/store/index.ts";
import { cliEnvironment, observeCli, type ObservedCli } from "./fixtures/cli-process.ts";
import { createOctoProtocolFixture } from "./fixtures/octo-protocol-server.ts";

test("CLI migrates the existing owner into the default state directory on startup", async () => {
  const home = await mkdtemp(join(tmpdir(), "oco-state-cli-"));
  const fixture = await createOctoProtocolFixture();
  let child: Bun.Subprocess | undefined;
  let observed: ObservedCli | undefined;
  try {
    const oldState = join(home, ".local", "state", "omp-channel-octo");
    await mkdir(oldState, { recursive: true });
    const store = await openChannelStore(oldState);
    store.setBotUid(fixture.botUid);
    store.setOwnerId(fixture.botUid, fixture.ownerUid);
    store.close();
    const config = join(home, "config.json");
    await writeFile(config, JSON.stringify({
      apiUrl: fixture.apiUrl,
      botToken: fixture.botToken,
      allowedGroupIds: [],
      ompCommand: process.execPath,
    }), { mode: 0o600 });
    child = Bun.spawn([process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", config], {
      env: cliEnvironment(home),
      stdout: "pipe",
      stderr: "pipe",
    });
    observed = observeCli(child);
    await observed.waitFor(
      () => {
        const connection = fixture.connections[0];
        return connection?.ready ? connection : undefined;
      },
      10_000,
      "CLI did not connect",
    );
    const migrated = new Database(join(home, ".oco", "state", "channel.sqlite"), { readonly: true });
    try {
      expect(migrated.query("SELECT owner_id FROM owners WHERE bot_uid = ?").get(fixture.botUid))
        .toEqual({ owner_id: fixture.ownerUid });
    } finally {
      migrated.close();
    }
    child.kill("SIGTERM");
    const exitCode = await child.exited;
    const [stdout, stderr] = await observed.output;
    expect(exitCode, `${stdout}\n${stderr}`).toBe(0);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    await observed?.output.catch(() => undefined);
    await fixture.close();
    await rm(home, { recursive: true, force: true });
  }
}, 30_000);
