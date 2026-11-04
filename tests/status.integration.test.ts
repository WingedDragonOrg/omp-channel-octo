import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureCardProfile,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "./fixtures/octo-protocol-server.ts";

function payloadOf(message: FixtureSentMessage): Record<string, unknown> {
  const value = message.body.payload;
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function textOf(message: FixtureSentMessage): string {
  const content = payloadOf(message).content;
  return typeof content === "string" ? content : "";
}
function visibleCardText(value: unknown): string[] {
  if (Array.isArray(value)) return value.flatMap(visibleCardText);
  if (!value || typeof value !== "object") return [];
  const node = value as Record<string, unknown>;
  return [
    ...(typeof node.text === "string" ? [node.text] : []),
    ...Object.values(node).flatMap(visibleCardText),
  ];
}

function cardProfile(elements: string[], cardVersion = "1.5"): FixtureCardProfile {
  return {
    enabled: true,
    profiles: ["octo/v1", "octo/v2"],
    card_version: cardVersion,
    elements,
    actions: [],
    config: {
      card_enabled: true,
      display_enabled: true,
      interaction_enabled: false,
      reasoning_enabled: true,
    },
  };
}

async function waitForChildExit(child: Bun.Subprocess, timeoutMs = 5_000): Promise<number> {
  // Fake timers cannot advance a separately spawned CLI process; bound this real process wait.
  const { promise, reject } = Promise.withResolvers<never>();
  const timer = setTimeout(() => reject(new Error("status CLI child did not exit in time")), timeoutMs);
  try {
    return await Promise.race([child.exited, promise]);
  } finally {
    clearTimeout(timer);
  }
}

interface StatusCliRun {
  fixture: OctoProtocolFixture;
  statusMessage: FixtureSentMessage;
  root: string;
  child: Bun.Subprocess;
  output: Promise<[string, string]>;
}

async function runStatusWithProfile(elements: string[], cardVersion = "1.5"): Promise<StatusCliRun> {
  const root = await mkdtemp(join(tmpdir(), "octo-status-integration-"));
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let output: Promise<[string, string]> | undefined;
  try {
    fixture = await createOctoProtocolFixture({ cardProfileBody: cardProfile(elements, cardVersion) });
    const configPath = join(root, "config.json");
    await writeFile(configPath, JSON.stringify({
      apiUrl: fixture.apiUrl,
      botToken: fixture.botToken,
      ownerId: fixture.ownerUid,
      allowedGroupIds: [],
      stateDir: join(root, "state"),
      ompCommand: process.execPath,
    }), { encoding: "utf8", mode: 0o600 });

    const spawned = Bun.spawn(
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "run", "--config", configPath],
      {
        stdout: "pipe",
        stderr: "pipe",
        env: { ...process.env, HOME: root, TZ: "UTC" },
      },
    );
    child = spawned;
    output = Promise.all([
      new Response(spawned.stdout).text(),
      new Response(spawned.stderr).text(),
    ]);

    const connection = await fixture.waitForConnection(1, 5_000);
    await waitForFixture(() => connection.ready ? connection : undefined, 5_000, "CLI Octo connection readiness");
    fixture.sendInbound(connection, {
      fromUid: fixture.ownerUid,
      channelId: `${fixture.ownerUid}@${fixture.botUid}`,
      channelType: 1,
      text: "/status",
      messageId: "9007199254744501",
    });

    const statusMessage = await waitForFixture(
      () => fixture!.sentMessages.find(message =>
        message.body.channel_id === fixture!.ownerUid
        && (payloadOf(message).type === 17 || textOf(message).includes("session=")),
      ),
      5_000,
      "CLI /status card or text response",
    );
    return { fixture, statusMessage, root, child: spawned, output: output! };
  } catch (error) {
    if (child !== undefined) {
      child.kill("SIGTERM");
      try {
        await waitForChildExit(child);
      } catch {
        child.kill("SIGKILL");
        await child.exited;
      }
      await output?.catch(() => undefined);
    }
    await fixture?.close();
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

async function closeRun(result: StatusCliRun): Promise<void> {
  try {
    result.child.kill("SIGTERM");
    let exitCode: number | undefined;
    let exitFailed = false;
    let exitFailure: unknown;
    try {
      exitCode = await waitForChildExit(result.child);
    } catch (error) {
      exitFailed = true;
      exitFailure = error;
      result.child.kill("SIGKILL");
      await result.child.exited;
    }
    const [stdout, stderr] = await result.output;
    const logs = `${stdout}\n${stderr}`;
    expect(logs).not.toContain(result.fixture.botToken);
    if (exitFailed) throw exitFailure;
    expect(exitCode, logs).toBe(0);
  } finally {
    await result.fixture.close();
    await rm(result.root, { recursive: true, force: true });
  }
}

test("real CLI sends /status as an octo/v1 Type-17 card through the encrypted local Octo fixture", async () => {
  const run = await runStatusWithProfile(["TextBlock"]);
  try {
    const payload = payloadOf(run.statusMessage);
    expect(payload.type).toBe(17);
    expect(payload.profile).toBe("octo/v1");
    expect(payload.card_version).toBe("1.5");
    expect(payload.plain).toEqual(expect.stringContaining("session=无"));
    expect(payload.plain).toEqual(expect.stringContaining("state=none"));
    const card = payload.card as Record<string, unknown>;
    expect(card.type).toBe("AdaptiveCard");
    const body = card.body as Array<Record<string, unknown>>;
    expect(body.every(node => node.type === "TextBlock")).toBe(true);
    const visible = visibleCardText(body).join("\n");
    expect(visible).toContain("会话");
    expect(visible).toContain("任务");
    expect(run.fixture.cardProfileRequests).toHaveLength(1);
  } finally {
    await closeRun(run);
  }
});

test("real CLI falls back to outbox text when the Octo profile omits TextBlock", async () => {
  const run = await runStatusWithProfile([]);
  try {
    const payload = payloadOf(run.statusMessage);
    expect(payload.type).toBe(1);
    expect(textOf(run.statusMessage)).toContain("session=无");
    expect(textOf(run.statusMessage)).toContain("state=none");
    expect(run.fixture.cardProfileRequests).toHaveLength(1);
  } finally {
    await closeRun(run);
  }
});

test("real CLI falls back to outbox text when the Octo profile predates card version 1.5", async () => {
  const run = await runStatusWithProfile(["TextBlock"], "1.4");
  try {
    const payload = payloadOf(run.statusMessage);
    expect(payload.type).toBe(1);
    expect(textOf(run.statusMessage)).toContain("session=无");
    expect(textOf(run.statusMessage)).toContain("state=none");
    expect(run.fixture.cardProfileRequests).toHaveLength(1);
    expect(run.fixture.sentMessages.filter(message =>
      message.body.channel_id === run.fixture.ownerUid && payloadOf(message).type === 17,
    )).toHaveLength(0);
  } finally {
    await closeRun(run);
  }
});
