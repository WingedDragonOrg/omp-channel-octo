import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureSentMessage,
  type OctoProtocolFixture,
} from "./fixtures/octo-protocol-server.ts";

function sentText(message: FixtureSentMessage): string {
  const payload = message.body.payload;
  if (!payload || typeof payload !== "object" || !("content" in payload)) return "";
  const content: unknown = payload.content;
  return typeof content === "string" ? content : "";
}

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

const CHECKOUT_ROOT = resolve(import.meta.dir, "..");

/**
 * Environment for the spawned CLI: it writes its own logs under `$HOME/.oco`, so the tests keep
 * that inside their temp root instead of the developer's home.
 */
function cliEnvironment(root: string, binDir?: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    HOME: root,
    ...(binDir === undefined ? {} : { PATH: `${binDir}:${process.env.PATH ?? ""}` }),
  };
}

/** Working directories of the OMP RPC workers whose startup lines appear in a fixture-log slice. */
function workerStartupCwds(logText: string): string[] {
  const cwds: string[] = [];
  for (const line of logText.split("\n")) {
    if (line.trim().length === 0) continue;
    let entry: unknown;
    try {
      entry = JSON.parse(line);
    } catch {
      continue; // A torn tail line is not evidence about worker starts.
    }
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) continue;
    const record = entry as Record<string, unknown>;
    if (record.kind === "startup" && typeof record.cwd === "string") cwds.push(record.cwd);
  }
  return cwds;
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
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot) },
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

test("a second start refuses before it registers the Bot and leaves the running instance serving", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let outputPromise: Promise<[string, string]> | undefined;
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-duplicate-start-"));
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
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot) },
    );
    child = spawned;
    const stdout = new Response(spawned.stdout).text();
    const stderr = new Response(spawned.stderr).text();
    outputPromise = Promise.all([stdout, stderr]);
    const connection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => connection.ready ? connection : undefined,
      5_000,
      "CLI did not authenticate its WebSocket connection",
    );

    const duplicate = Bun.spawn(
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot) },
    );
    const [duplicateCode, , duplicateErr] = await Promise.all([
      waitForChildExit(duplicate, 10_000),
      new Response(duplicate.stdout).text(),
      new Response(duplicate.stderr).text(),
    ]);
    expect(duplicateCode).toBe(1);
    expect(duplicateErr).toContain("同一 Bot 已有 channel 实例运行");
    // Registering again is what would cut the online instance off, so the refusal must precede it.
    expect(currentFixture.registerRequests).toHaveLength(1);
    expect(spawned.exitCode).toBeNull();

    currentFixture.sendInbound(connection, {
      fromUid: currentFixture.ownerUid,
      channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
      channelType: 1,
      text: "/status",
      messageId: "9007199254742201",
    });
    await waitForFixture(
      () => currentFixture.sentMessages.some(({ body }) => body.channel_id === currentFixture.ownerUid),
      5_000,
      "the running instance stopped answering after the refused duplicate start",
    );

    spawned.kill("SIGTERM");
    expect(await waitForChildExit(spawned)).toBe(0);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (outputPromise) await outputPromise.catch(() => {});
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI runs the owner's group command with the bot mention placed before or after it", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let outputPromise: Promise<[string, string]> | undefined;
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-group-command-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const groupId = "group-mention-placement";
    const configPath = join(currentRoot, "config.json");
    await writeFile(
      configPath,
      JSON.stringify({
        apiUrl: currentFixture.apiUrl,
        botToken: currentFixture.botToken,
        ownerId: currentFixture.ownerUid,
        allowedGroupIds: [groupId],
        stateDir: join(currentRoot, "state"),
        ompCommand: process.execPath,
      }),
      { encoding: "utf8", mode: 0o600 },
    );

    const spawned = Bun.spawn(
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot) },
    );
    child = spawned;
    const stdout = new Response(spawned.stdout).text();
    const stderr = new Response(spawned.stderr).text();
    outputPromise = Promise.all([stdout, stderr]);

    const connection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => connection.ready ? connection : undefined,
      5_000,
      "CLI did not authenticate its WebSocket connection",
    );

    const botPrefix = `@${currentFixture.botName}`;
    const mentionEntity = {
      entities: [{ uid: currentFixture.botUid, offset: "/status ".length, length: botPrefix.length }],
    };
    currentFixture.sendInboundBatch(connection, [
      {
        fromUid: "other-member",
        channelId: groupId,
        channelType: 2,
        text: `/status ${botPrefix}`,
        mention: mentionEntity,
        messageId: "9007199254742101",
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: groupId,
        channelType: 2,
        text: "/status",
        messageId: "9007199254742102",
      },
      {
        fromUid: "other-member",
        channelId: `other-member@${currentFixture.botUid}`,
        channelType: 1,
        text: "/status",
        messageId: "9007199254742103",
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: groupId,
        channelType: 2,
        text: `${botPrefix} /status`,
        mention: { entities: [{ uid: currentFixture.botUid, offset: 0, length: botPrefix.length }] },
        messageId: "9007199254742104",
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: groupId,
        channelType: 2,
        text: `/status ${botPrefix}`,
        mention: { uids: [currentFixture.botUid] },
        messageId: "9007199254742105",
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
        channelType: 1,
        text: "/status",
        messageId: "9007199254742106",
      },
    ]);

    await waitForFixture(
      () => currentFixture.sentMessages.length === 3 ? currentFixture.sentMessages : undefined,
      5_000,
      "CLI did not answer the two owner commands and the sentinel status request",
    );
    // Every inbound frame was acknowledged after handling, so a rejected message cannot still be answered later.
    await waitForFixture(
      () => connection.recvAcks.length === 6 ? connection.recvAcks : undefined,
      5_000,
      "CLI did not acknowledge every inbound frame",
    );

    const replies = currentFixture.sentMessages.map((message) => ({
      channelId: message.body.channel_id,
      content: sentText(message),
    }));
    expect(currentFixture.sentMessages).toHaveLength(3);
    expect(replies.filter(({ channelId }) => channelId === groupId)).toHaveLength(2);
    expect(replies.every(({ content }) => content.includes("state=none"))).toBe(true);
    expect(replies.filter(({ channelId }) => channelId === currentFixture.ownerUid)).toHaveLength(1);
    expect(replies.some(({ channelId }) => channelId === "other-member")).toBe(false);

    spawned.kill("SIGTERM");
    expect(await waitForChildExit(spawned)).toBe(0);
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

test("real CLI restarts through the detected supervisor program and confirms it after coming back", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  const children: Bun.Subprocess[] = [];
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-restart-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const markerPath = join(currentRoot, "super-restart-ran");
    const fakeBin = join(currentRoot, "bin");
    await mkdir(fakeBin, { recursive: true });
    // Stands in for the real `super` client: it answers the two calls detection makes (the program
    // list and one program's live PID) and, on restart, stops the CLI the way a supervisor does.
    const fakeSuper = join(fakeBin, "super");
    await writeFile(
      fakeSuper,
      `#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    export)
      printf '{"prune":false,"services":[{"name":"omp-channel-octo","command":"/bin/sh","args":["-c",". ./.envrc; exec bun run src/cli.ts start"],"cwd":"/tmp/oco-test-not-this-checkout"}]}'
      exit 0 ;;
    info)
      printf 'PID: %s\\nState: Healthy\\n' "$PPID"
      exit 0 ;;
    restart)
      printf restarted > '${markerPath}'
      kill -TERM "$PPID"
      exit 0 ;;
  esac
done
exit 2
`,
      { mode: 0o755 },
    );
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
    const cliArgs = [
      process.execPath,
      resolve(import.meta.dir, "../src/cli.ts"),
      "start",
      "--config",
      configPath,
    ];
    const cliEnv = cliEnvironment(currentRoot, fakeBin);

    const first = Bun.spawn(cliArgs, { stdout: "pipe", stderr: "pipe", env: cliEnv });
    children.push(first);
    const firstOutput = Promise.all([new Response(first.stdout).text(), new Response(first.stderr).text()]);
    const firstConnection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => firstConnection.ready ? firstConnection : undefined,
      5_000,
      "first CLI did not authenticate its WebSocket connection",
    );
    currentFixture.sendInbound(firstConnection, {
      fromUid: currentFixture.ownerUid,
      channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
      channelType: 1,
      text: "/restart",
      messageId: "9007199254742001",
    });

    const acknowledgement = await waitForFixture(
      () => currentFixture.sentMessages.find((message) => sentText(message).includes("正在重启 channel")),
      5_000,
      "CLI did not acknowledge the restart request",
    );
    expect(sentText(acknowledgement)).toContain("正在重启 channel");

    expect(await waitForChildExit(first, 10_000)).toBe(0);
    await firstOutput.catch(() => {});
    await waitForFixture(
      () => existsSync(markerPath) ? true : undefined,
      5_000,
      "the detected supervisor program did not restart the CLI",
    );

    const beforeRestart = currentFixture.sentMessages.length;
    const second = Bun.spawn(cliArgs, { stdout: "pipe", stderr: "pipe", env: cliEnv });
    children.push(second);
    const secondOutput = Promise.all([new Response(second.stdout).text(), new Response(second.stderr).text()]);
    const secondConnection = await currentFixture.waitForConnection(2, 5_000);
    await waitForFixture(
      () => secondConnection.ready ? secondConnection : undefined,
      5_000,
      "restarted CLI did not authenticate its WebSocket connection",
    );
    const confirmation = await waitForFixture(
      () =>
        currentFixture.sentMessages
          .slice(beforeRestart)
          .find((message) => sentText(message).includes("channel 已重启完成")),
      8_000,
      "restarted CLI did not confirm the restart in the requesting chat",
    );
    expect(sentText(confirmation)).toContain("channel 已重启完成");

    second.kill("SIGTERM");
    expect(await waitForChildExit(second)).toBe(0);
    await secondOutput.catch(() => {});
  } finally {
    for (const child of children) {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        await child.exited;
      }
    }
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI run stays out of the supervisor's way and refuses a restart", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  const children: Bun.Subprocess[] = [];
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-run-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const fakeBin = join(currentRoot, "bin");
    const callsPath = join(currentRoot, "super-calls.log");
    await mkdir(fakeBin, { recursive: true });
    // A supervisor this checkout could be detected through: `run` must never consult it.
    await writeFile(
      join(fakeBin, "super"),
      `#!/bin/sh
printf '%s\\n' "$*" >> '${callsPath}'
printf '{"prune":false,"services":[{"name":"omp-channel-octo","command":"/bin/sh","args":["-c",". ./.envrc; exec bun run src/cli.ts start"],"cwd":"${CHECKOUT_ROOT}"}]}'
exit 0
`,
      { mode: 0o755 },
    );
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
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "run", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot, fakeBin) },
    );
    children.push(spawned);

    const connection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => connection.ready ? connection : undefined,
      5_000,
      "the debugging CLI did not authenticate its WebSocket connection",
    );
    currentFixture.sendInbound(connection, {
      fromUid: currentFixture.ownerUid,
      channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
      channelType: 1,
      text: "/restart",
      messageId: "9007199254742101",
    });
    const reply = await waitForFixture(
      () => currentFixture.sentMessages.find((message) => sentText(message).includes("无法重启 channel")),
      5_000,
      "the debugging CLI did not refuse the restart request",
    );
    expect(sentText(reply)).toContain("未检测到守护进程");
    expect(existsSync(callsPath)).toBe(false);

    spawned.kill("SIGTERM");
    expect(await waitForChildExit(spawned)).toBe(0);
  } finally {
    for (const child of children) {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        await child.exited;
      }
    }
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
}, 20_000);

test("real CLI gives a CommunityTopic thread the parent group's work directory", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  const children: Bun.Subprocess[] = [];
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-thread-cwd-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const groupId = "group-thread-cwd";
    const threadId = "topic-inherit-1";
    const threadChannelId = `${groupId}____${threadId}`;
    const parentDir = join(currentRoot, "parent-dir");
    await mkdir(parentDir, { recursive: true });
    // macOS temp roots are symlinked (/var -> /private/var) and the CLI persists realpaths.
    const parentCwd = await realpath(parentDir);
    const rpcLogPath = join(currentRoot, "omp-rpc.log");
    const stateDir = join(currentRoot, "state");
    const fixtureScenario = "normal";
    const fixtureSessionId = `fixture-session-${fixtureScenario}`;
    // The RPC fixture speaks the host's RPC protocol but cannot run the real OMP extension that
    // writes the prepared-context receipt.  Seed the steady state "this thread's session already
    // committed its shared context" (the fixture always reports `fixtureSessionId`) so the task
    // exercises work-directory inheritance instead of the extension handshake it cannot satisfy.
    await mkdir(stateDir, { recursive: true });
    const seed = new Database(join(stateDir, "channel.sqlite"));
    seed.run(`CREATE TABLE IF NOT EXISTS session_context_ledger (
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
        channel_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version >= 0),
        phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
        attempt_id TEXT NOT NULL,
        previous_version INTEGER CHECK(previous_version IS NULL OR previous_version >= 0),
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
      )`);
    seed.run(
      `INSERT INTO session_context_ledger(
         bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version, updated_at
       ) VALUES(?, 5, ?, ?, 0, 'committed', ?, NULL, ?)`,
      [currentFixture.botUid, threadChannelId, fixtureSessionId, "thread-cwd-seed", Date.now()],
    );
    seed.close();

    const configPath = join(currentRoot, "config.json");
    await writeFile(
      configPath,
      JSON.stringify({
        apiUrl: currentFixture.apiUrl,
        botToken: currentFixture.botToken,
        ownerId: currentFixture.ownerUid,
        allowedGroupIds: [groupId, threadChannelId],
        stateDir,
        ompCommand: process.execPath,
        ompExtraArgs: [
          resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts"),
          "--fixture-scenario",
          fixtureScenario,
          "--fixture-log",
          rpcLogPath,
        ],
        startupTimeoutMs: 5_000,
        cancelTimeoutMs: 2_000,
      }),
      { encoding: "utf8", mode: 0o600 },
    );
    const cliArgs = [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath];
    const cliEnv = cliEnvironment(currentRoot);
    const botPrefix = `@${currentFixture.botName}`;
    const botMention = { entities: [{ uid: currentFixture.botUid, offset: 0, length: botPrefix.length }] };

    const first = Bun.spawn(cliArgs, { stdout: "pipe", stderr: "pipe", env: cliEnv });
    children.push(first);
    const firstOutput = Promise.all([new Response(first.stdout).text(), new Response(first.stderr).text()]);
    const firstConnection = await currentFixture.waitForConnection(1, 5_000);
    await waitForFixture(
      () => firstConnection.ready ? firstConnection : undefined,
      5_000,
      "CLI did not authenticate its WebSocket connection",
    );

    // The owner binds the parent group to an explicit directory; that binding is the inheritance source.
    currentFixture.sendInbound(firstConnection, {
      fromUid: currentFixture.ownerUid,
      channelId: groupId,
      channelType: 2,
      text: `${botPrefix} /new ${parentDir}`,
      mention: botMention,
      messageId: "9007199254744001",
    });
    const created = await waitForFixture(
      () => currentFixture.sentMessages.find((message) => message.body.channel_id === groupId),
      10_000,
      "the parent group /new command did not get a reply",
    );
    expect(sentText(created)).toContain(`cwd=${parentCwd}`);

    // A plain thread task inherits that directory and starts a real worker child process inside it.
    const logSizeBeforeTask = existsSync(rpcLogPath) ? (await readFile(rpcLogPath)).length : 0;
    currentFixture.sendInbound(firstConnection, {
      fromUid: currentFixture.ownerUid,
      channelId: threadChannelId,
      channelType: 5,
      text: `${botPrefix} 线程任务`,
      mention: botMention,
      messageId: "9007199254744002",
    });
    const taskReply = await waitForFixture(
      () =>
        currentFixture.sentMessages.find(
          (message) =>
            message.body.channel_id === threadChannelId && sentText(message).includes("fixture completed"),
        ),
      10_000,
      "the thread task did not complete through the OMP worker",
    );
    expect(sentText(taskReply)).toContain("fixture completed");
    const threadWorkerLog = (await readFile(rpcLogPath)).subarray(logSizeBeforeTask).toString("utf8");
    expect(workerStartupCwds(threadWorkerLog)).toEqual([parentCwd]);
    // Inheriting the directory must not inherit the parent group's shared context: the thread reads its own THREAD.md.
    expect(
      currentFixture.groupContextRequests
        .filter((request) => request.target === threadChannelId)
        .map((request) => request.path),
    ).toEqual([`/v1/bot/groups/${groupId}/threads/${threadId}/md`]);

    // /status reports the inherited binding.
    const beforeStatus = currentFixture.sentMessages.length;
    currentFixture.sendInbound(firstConnection, {
      fromUid: currentFixture.ownerUid,
      channelId: threadChannelId,
      channelType: 5,
      text: `${botPrefix} /status`,
      mention: botMention,
      messageId: "9007199254744003",
    });
    const status = await waitForFixture(
      () =>
        currentFixture.sentMessages
          .slice(beforeStatus)
          .find((message) => message.body.channel_id === threadChannelId && sentText(message).includes("cwd=")),
      10_000,
      "the thread /status command did not report its session",
    );
    expect(sentText(status)).toContain(`cwd=${parentCwd}`);

    // The inherited binding and the completed task survive a restart.
    first.kill("SIGTERM");
    expect(await waitForChildExit(first, 10_000)).toBe(0);
    await firstOutput.catch(() => {});

    const second = Bun.spawn(cliArgs, { stdout: "pipe", stderr: "pipe", env: cliEnv });
    children.push(second);
    const secondOutput = Promise.all([new Response(second.stdout).text(), new Response(second.stderr).text()]);
    const secondConnection = await currentFixture.waitForConnection(2, 5_000);
    await waitForFixture(
      () => secondConnection.ready ? secondConnection : undefined,
      5_000,
      "restarted CLI did not authenticate its WebSocket connection",
    );
    const beforeRestartStatus = currentFixture.sentMessages.length;
    currentFixture.sendInbound(secondConnection, {
      fromUid: currentFixture.ownerUid,
      channelId: threadChannelId,
      channelType: 5,
      text: `${botPrefix} /status`,
      mention: botMention,
      messageId: "9007199254744004",
    });
    const restartedStatus = await waitForFixture(
      () =>
        currentFixture.sentMessages
          .slice(beforeRestartStatus)
          .find((message) => message.body.channel_id === threadChannelId && sentText(message).includes("cwd=")),
      10_000,
      "the restarted CLI did not report the thread session",
    );
    const restartedText = sentText(restartedStatus);
    expect(restartedText).toContain(`cwd=${parentCwd}`);
    // The task closed as completed before the restart instead of failing or being interrupted.
    expect(restartedText).toContain("state=completed");

    second.kill("SIGTERM");
    expect(await waitForChildExit(second, 10_000)).toBe(0);
    await secondOutput.catch(() => {});
  } finally {
    for (const child of children) {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        await child.exited;
      }
    }
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
}, 30_000);

test("real CLI creates a thread's isolated workspace through the resolved OMP command", async () => {
  let root: string | undefined;
  let fixture: OctoProtocolFixture | undefined;
  let child: Bun.Subprocess | undefined;
  let outputPromise: Promise<[string, string]> | undefined;
  try {
    const currentRoot = await mkdtemp(join(tmpdir(), "octo-cli-thread-worktree-"));
    root = currentRoot;
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    const groupId = "group-thread-worktree";
    // The group works in a real repository, so the workspace has a branch to start from.
    const repoDir = join(currentRoot, "project");
    await mkdir(repoDir);
    await writeFile(join(repoDir, "file.txt"), "hello\n", "utf8");
    const runGit = async (args: string[]): Promise<void> => {
      const git = Bun.spawn(["git", "-C", repoDir, ...args], {
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      });
      const stderr = await new Response(git.stderr).text();
      if ((await git.exited) !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
    };
    await runGit(["init", "-q", "-b", "main"]);
    await runGit(["add", "."]);
    await runGit([
      "-c",
      "user.email=test@example.com",
      "-c",
      "user.name=Fixture",
      "commit",
      "-q",
      "-m",
      "init",
    ]);
    const repoCwd = await realpath(repoDir);
    // One executable stands in for the OMP CLI: `worktree add` performs the real git command that
    // the deployment's `omp worktree add` wraps, and every other invocation starts the RPC worker
    // fixture, so the service path under test is exactly the deployed one.
    const ompWrapper = join(currentRoot, "omp");
    await writeFile(
      ompWrapper,
      `#!/bin/sh
if [ "$1" = "worktree" ] && [ "$2" = "add" ]; then
  shift 2
  exec git -C "$5" worktree add "$1" -b "$3" -q
fi
exec '${process.execPath}' "$@"
`,
      { mode: 0o755 },
    );
    const rpcLogPath = join(currentRoot, "omp-rpc.log");
    const stateDir = join(currentRoot, "state");
    const fixtureScenario = "host-tool";
    // The RPC fixture cannot run the real OMP extension that writes the prepared-context receipt,
    // so seed the steady state "this session already committed its shared context" (the fixture
    // always reports `fixture-session-<scenario>`) and let the task reach the management call.
    const fixtureSessionId = `fixture-session-${fixtureScenario}`;
    await mkdir(stateDir, { recursive: true });
    const seed = new Database(join(stateDir, "channel.sqlite"));
    seed.run(`CREATE TABLE IF NOT EXISTS session_context_ledger (
        bot_uid TEXT NOT NULL,
        channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
        channel_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version >= 0),
        phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
        attempt_id TEXT NOT NULL,
        previous_version INTEGER CHECK(previous_version IS NULL OR previous_version >= 0),
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
      )`);
    seed.run(
      `INSERT INTO session_context_ledger(
         bot_uid, channel_type, channel_id, session_id, version, phase, attempt_id, previous_version, updated_at
       ) VALUES(?, 2, ?, ?, 0, 'committed', ?, NULL, ?)`,
      [currentFixture.botUid, groupId, fixtureSessionId, "thread-worktree-seed", Date.now()],
    );
    seed.close();

    const configPath = join(currentRoot, "config.json");
    await writeFile(
      configPath,
      JSON.stringify({
        apiUrl: currentFixture.apiUrl,
        botToken: currentFixture.botToken,
        ownerId: currentFixture.ownerUid,
        allowedGroupIds: [groupId],
        stateDir,
        ompCommand: ompWrapper,
        ompExtraArgs: [
          resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts"),
          "--fixture-scenario",
          fixtureScenario,
          "--fixture-log",
          rpcLogPath,
          "--host-tool-name",
          "octo_management",
          "--host-tool-args",
          JSON.stringify({ action: "create-thread", name: "隔离线程", worktree_name: "cli-workspace" }),
        ],
        startupTimeoutMs: 5_000,
        cancelTimeoutMs: 2_000,
      }),
      { encoding: "utf8", mode: 0o600 },
    );

    const spawned = Bun.spawn(
      [process.execPath, resolve(import.meta.dir, "../src/cli.ts"), "start", "--config", configPath],
      { stdout: "pipe", stderr: "pipe", env: cliEnvironment(currentRoot) },
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
    const botPrefix = `@${currentFixture.botName}`;
    const botMention = { entities: [{ uid: currentFixture.botUid, offset: 0, length: botPrefix.length }] };

    // Bind the group to the repository: the task's base directory is then a real checkout.
    currentFixture.sendInbound(connection, {
      fromUid: currentFixture.ownerUid,
      channelId: groupId,
      channelType: 2,
      text: `${botPrefix} /new ${repoDir}`,
      mention: botMention,
      messageId: "9007199254745001",
    });
    const bound = await waitForFixture(
      () => currentFixture.sentMessages.find((message) => message.body.channel_id === groupId),
      10_000,
      "the parent group /new command did not get a reply",
    );
    expect(sentText(bound)).toContain(`cwd=${repoCwd}`);

    // The task calls the management tool; the host must materialize the workspace through the
    // resolved OMP command and answer with the created worktree instead of `worktree_error`.
    currentFixture.sendInbound(connection, {
      fromUid: currentFixture.ownerUid,
      channelId: groupId,
      channelType: 2,
      text: `${botPrefix} 建一个隔离线程`,
      mention: botMention,
      messageId: "9007199254745002",
    });
    const reply = await waitForFixture(
      () =>
        currentFixture.sentMessages.find(
          (message) =>
            message.body.channel_id === groupId && sentText(message).includes("fixture tool returned:"),
        ),
      15_000,
      "the group task did not return the octo_management result",
    );
    const replyText = sentText(reply);
    expect(replyText).not.toContain("worktree_error");
    const resultJson = replyText
      .slice(replyText.indexOf("fixture tool returned:") + "fixture tool returned:".length)
      .trim();
    const created = JSON.parse(resultJson) as { worktree?: { path: string; branch: string } };
    expect(created.worktree?.branch).toBe("cli-workspace");
    const workspacePath = created.worktree?.path ?? "";
    expect(workspacePath.length).toBeGreaterThan(0);
    // The reported workspace is a real git worktree on disk.
    expect(existsSync(join(workspacePath, ".git"))).toBe(true);
    expect(
      currentFixture.managementRequests.some(
        (request) => request.method === "POST" && request.path === `/v1/bot/groups/${groupId}/threads`,
      ),
    ).toBe(true);

    spawned.kill("SIGTERM");
    expect(await waitForChildExit(spawned)).toBe(0);
  } finally {
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    if (outputPromise) await outputPromise.catch(() => {});
    if (fixture) await fixture.close();
    if (root) await rm(root, { recursive: true, force: true });
  }
}, 30_000);
