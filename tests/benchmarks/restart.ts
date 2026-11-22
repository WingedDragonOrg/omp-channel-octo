import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, realpath, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection, type FixtureSentMessage } from "../fixtures/octo-protocol-server.ts";

// Functional benchmark, not a latency benchmark. Fixed inputs and checkpoints;
// only loopback HTTP/WebSocket and real local CLI/RPC child processes are used.
// Missing continuity is measured, while broken setup, duplicate effects, leaked
// workers, or concurrent session writers fail the run. No production code mocks.
const checkout = resolve(import.meta.dir, "../..");
const observationMs = 3_000;
type Audit = { kind: string; pid: number; sessionFile: string; name?: string; text?: string; resumed?: boolean };
type Task = { state: string; result_text: string | null };
type RuntimeIdentity = { path: string; botUid: string; runtimeId: string; pid: number; processIdentity: string };
type Result = { seamless: number; resumed: number; queued: number; notices: number; overlaps: number; duplicates: number };
function auditRows(path: string): Audit[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Audit);
}
function text(message: FixtureSentMessage): string {
  const payload = message.body.payload as { content?: unknown } | undefined;
  return typeof payload?.content === "string" ? payload.content : "";
}
function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
async function exited(child: Bun.Subprocess, timeoutMs = 10_000): Promise<number> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([child.exited, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Benchmark CLI exit deadline exceeded")), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}
async function scenario(mode: "graceful" | "crash", cliExecutable?: string): Promise<Result> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-restart-benchmark-")));
  const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
  const children: Bun.Subprocess[] = [];
  const outputs: Promise<string[]>[] = [];
  const auditPath = join(root, "audit.jsonl");
  let db: Database | undefined;
  let succeeded = false;
  let cleanupRuntime: (() => Promise<void>) | undefined;
  try {
    const bin = join(root, "bin");
    await mkdir(bin);
    await mkdir(join(root, "tmp"));
    // Isolate host CLI discovery and credentials; never contact a resident daemon.
    await writeFile(join(bin, "octo-cli"), "#!/bin/sh\nexit 127\n", { mode: 0o755 });
    await writeFile(join(bin, "super"), `#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    export) printf '{"prune":false,"services":[{"name":"benchmark","command":"bun","args":["src/cli.ts","start"],"cwd":"/benchmark"}]}'; exit 0 ;;
    info) printf 'PID: %s\\nState: Healthy\\n' "$PPID"; exit 0 ;;
    restart) kill -TERM "$PPID"; exit 0 ;;
  esac
done
exit 2
`, { mode: 0o755 });
    const configPath = join(root, "config.json");
    await writeFile(configPath, JSON.stringify({
      apiUrl: fixture.apiUrl, botToken: fixture.botToken, ownerId: fixture.ownerUid,
      allowedGroupIds: [], stateDir: join(root, "state"), sessionsDir: join(root, "sessions"),
      ompCommand: process.execPath,
      ompExtraArgs: [join(checkout, "tests/fixtures/restart-benchmark-worker.ts"), "--audit", auditPath],
      startupTimeoutMs: 5_000, cancelTimeoutMs: 2_000,
      groupHistory: { enabled: false, maxMessages: 50 },
    }), { mode: 0o600 });
    const env = { HOME: root, PATH: `${bin}:/usr/bin:/bin`, TMPDIR: root, LANG: "C", TZ: "UTC" };
    async function launch(): Promise<{ child: Bun.Subprocess; connection: FixtureConnection }> {
      const cliCommand = cliExecutable === undefined
        ? [process.execPath, join(checkout, "src/cli.ts"), "start", "--config", configPath]
        : [cliExecutable, "start", "--config", configPath];
      const child = Bun.spawn(cliCommand, {
        cwd: root, env, stdin: "ignore", stdout: "pipe", stderr: "pipe",
      });
      children.push(child);
      outputs.push(Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]));
      const connection = await fixture.waitForConnection(children.length, 10_000);
      await waitForFixture(() => connection.ready ? true : undefined, 5_000, "CLI did not authenticate");
      return { child, connection };
    }
    function send(connection: FixtureConnection, body: string, id: string): void {
      fixture.sendInbound(connection, { fromUid: fixture.ownerUid, channelId: `${fixture.ownerUid}@${fixture.botUid}`, channelType: 1, text: body, messageId: id });
    }
    function task(id: string): Task | null {
      return db!.query("SELECT state, result_text FROM tasks WHERE message_id = ?").get(id) as Task | null;
    }
    async function runtimeManifest(): Promise<RuntimeIdentity | undefined> {
      const runtimeRoot = join(root, "state", "agent-runtime");
      let entries;
      try {
        entries = await readdir(runtimeRoot, { withFileTypes: true });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
        throw error;
      }
      const paths = entries.filter(entry => entry.isDirectory())
        .map(entry => join(runtimeRoot, entry.name, "runtime.json"))
        .filter(existsSync);
      assert.ok(paths.length <= 1, "The isolated state directory contains multiple agent runtimes");
      if (paths.length === 0) return undefined;
      const path = paths[0]!;
      const value = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
      assert.equal(value.botUid, fixture.botUid, "Runtime manifest belongs to a different fixture Bot");
      assert.ok(typeof value.runtimeId === "string" && value.runtimeId.length > 0);
      assert.ok(typeof value.pid === "number" && Number.isSafeInteger(value.pid) && value.pid > 1);
      assert.ok(typeof value.processIdentity === "string" && value.processIdentity.length > 0);
      return {
        path,
        botUid: value.botUid as string,
        runtimeId: value.runtimeId as string,
        pid: value.pid as number,
        processIdentity: value.processIdentity as string,
      };
    }
    async function processIdentity(pid: number): Promise<string | undefined> {
      const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "lstart="], {
        stdout: "pipe", stderr: "ignore", env: { ...process.env, TZ: "UTC" },
      });
      const [output, code] = await Promise.all([new Response(ps.stdout).text(), ps.exited]);
      const started = output.trim();
      return code === 0 && started.length > 0 ? `pid=${pid};start=${started}` : undefined;
    }
    async function processCommand(pid: number): Promise<string> {
      const ps = Bun.spawn(["/bin/ps", "-p", String(pid), "-o", "command="], { stdout: "pipe", stderr: "ignore" });
      const [command] = await Promise.all([new Response(ps.stdout).text(), ps.exited]);
      return command;
    }
    async function waitForProcessExit(pid: number, identity: string, description: string): Promise<void> {
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline && await processIdentity(pid) === identity) await Bun.sleep(20);
      assert.notEqual(await processIdentity(pid), identity, description);
    }
    async function stopRuntimeForColdRecovery(runtime: RuntimeIdentity, worker: Audit, workerIdentity: string): Promise<void> {
      assert.equal(await processIdentity(runtime.pid), runtime.processIdentity, "Runtime process identity changed before shutdown");
      assert.equal(await processIdentity(worker.pid), workerIdentity, "OMP worker identity changed before runtime shutdown");
      const command = await processCommand(runtime.pid);
      assert.ok(command.includes(join(root, "state", "agent-runtime")), "Refusing to signal a PID outside this fixture's runtime");
      process.kill(runtime.pid, "SIGKILL");
      await waitForProcessExit(runtime.pid, runtime.processIdentity, "Agent runtime did not exit after SIGKILL");
      await waitForProcessExit(worker.pid, workerIdentity, "Runtime exit did not confirm the old OMP worker exit");
    }
    cleanupRuntime = async (): Promise<void> => {
      const runtime = await runtimeManifest().catch(() => undefined);
      if (!runtime || await processIdentity(runtime.pid) !== runtime.processIdentity) return;
      const runtimePath = join(root, "state", "agent-runtime");
      if (!(await processCommand(runtime.pid)).includes(runtimePath)) return;
      process.kill(runtime.pid, "SIGTERM");
      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline && await processIdentity(runtime.pid) === runtime.processIdentity) await Bun.sleep(20);
      if (await processIdentity(runtime.pid) === runtime.processIdentity
        && (await processCommand(runtime.pid)).includes(runtimePath)) {
        process.kill(runtime.pid, "SIGKILL");
        const killDeadline = Date.now() + 3_000;
        while (Date.now() < killDeadline && await processIdentity(runtime.pid) === runtime.processIdentity) await Bun.sleep(20);
      }
      assert.notEqual(await processIdentity(runtime.pid), runtime.processIdentity, "Benchmark leaked its isolated agent runtime");
    };
    const first = await launch();
    db = new Database(join(root, "state/channel.sqlite"), { readonly: true });
    // A completed turn proves the fixture and delivery work before interruption.
    send(first.connection, "benchmark:completed", "1001");
    await waitForFixture(() => task("1001")?.state === "completed" && fixture.sentMessages.some(m => text(m) === "BENCHMARK_COMPLETED_DONE") ? true : undefined, 10_000, "Control turn did not complete");
    send(first.connection, "benchmark:running", "1002");
    await waitForFixture(() => auditRows(auditPath).some(r => r.kind === "checkpoint") && fixture.sentMessages.some(m => text(m) === "BENCHMARK_CHECKPOINT") ? true : undefined, 10_000, "Running turn never reached its durable checkpoint");
    assert.equal(task("1002")?.state, "started");
    send(first.connection, "/queue benchmark:queued", "1003");
    await waitForFixture(() => task("1003")?.state === "queued" ? true : undefined, 5_000, "Queued turn was not durably accepted");
    const beforeRestart = fixture.sentMessages.length;
    const bindingBefore = db.query("SELECT session_id, session_file FROM chats WHERE channel_type = 1").get();
    const oldWorker = auditRows(auditPath).filter(r => r.kind === "startup").at(-1)!;
    assert.ok(alive(oldWorker.pid), "Interruption must occur with a live RPC worker");
    const oldWorkerIdentity = await processIdentity(oldWorker.pid);
    assert.ok(oldWorkerIdentity, "Could not verify the old RPC worker identity");
    const oldRuntime = await runtimeManifest();
    assert.ok(oldRuntime, "The isolated agent runtime manifest is missing");
    assert.equal(await processIdentity(oldRuntime.pid), oldRuntime.processIdentity, "Runtime manifest PID identity did not match the operating system");
    if (mode === "graceful") {
      send(first.connection, "/restart now", "1004");
      assert.equal(await exited(first.child), 0);
    } else {
      first.child.kill("SIGKILL");
      await exited(first.child);
    }
    assert.equal(await processIdentity(oldRuntime.pid), oldRuntime.processIdentity, "Channel restart exited the independent runtime");
    assert.equal(await processIdentity(oldWorker.pid), oldWorkerIdentity, "Channel restart exited the runtime-owned OMP worker");
    await stopRuntimeForColdRecovery(oldRuntime, oldWorker, oldWorkerIdentity);
    assert.notEqual(await processIdentity(oldWorker.pid), oldWorkerIdentity, "The old OMP worker was not confirmed dead before cold recovery");
    const second = await launch();
    const recoveredRuntime = await runtimeManifest();
    assert.ok(recoveredRuntime, "Cold recovery did not launch a replacement runtime");
    assert.notEqual(recoveredRuntime.runtimeId, oldRuntime.runtimeId, "Cold recovery reused the exited runtime identity");
    await Bun.sleep(observationMs);
    assert.equal(second.child.exitCode, null, "Replacement CLI exited unexpectedly");
    const rows = auditRows(auditPath);
    const count = (name: string) => rows.filter(r => r.kind === "effect" && r.name === name).length;
    const overlaps = rows.filter(r => r.kind === "overlap").length;
    const duplicates = ["completed", "begin", "finish", "queued"].reduce((sum, name) => sum + Math.max(0, count(name) - 1), 0);
    assert.equal(overlaps, 0, "Same session opened while its previous worker was alive");
    assert.equal(duplicates, 0, "A committed side effect was executed twice");
    assert.notEqual(await processIdentity(oldWorker.pid), oldWorkerIdentity, "Old RPC worker survived runtime recovery");
    assert.ok(rows.some(r => r.kind === "startup" && r.resumed === true), "Cold recovery did not reopen the original OMP session file");
    assert.equal(rows.filter(r => r.kind === "session_reset").length, 0, "Recovery reset the persistent session");
    assert.deepEqual(db.query("SELECT session_id, session_file FROM chats WHERE channel_type = 1").get(), bindingBefore, "Restart replaced the chat session");
    const delivered = fixture.sentMessages.slice(beforeRestart).map(text);
    const resumed = Number(count("finish") === 1 && delivered.includes("BENCHMARK_RUNNING_DONE") && task("1002")?.state === "completed");
    const queued = Number(count("queued") === 1 && delivered.includes("BENCHMARK_QUEUED_DONE") && task("1003")?.state === "completed");
    const notices = delivered.filter(value => /重启完成|因重启中断|请重新发送|任务已取消|任务已中断|任务执行失败|任务未启动|Retrying the last failed turn|Nothing to retry/.test(value)).length;
    if (resumed && queued) {
      assert.ok(rows.findIndex(r => r.kind === "effect" && r.name === "finish") < rows.findIndex(r => r.kind === "effect" && r.name === "queued"), "Queued work overtook the interrupted turn");
    }
    const result = { seamless: Number(resumed === 1 && queued === 1 && notices === 0), resumed, queued, notices, overlaps, duplicates };
    console.log(`CASE ${mode} ${JSON.stringify(result)}`);
    // Positive control AFTER measurement: /retry must consume the saved checkpoint.
    // The benchmark never uses this message to award automatic-resumption credit.
    if (!resumed) {
      send(second.connection, "/retry", "1005");
      await waitForFixture(() => fixture.sentMessages.some(m => text(m) === "BENCHMARK_RUNNING_DONE") ? true : undefined, 10_000, "Native retry control did not resume the checkpoint");
      await waitForFixture(() => task("1005")?.state === "completed" ? true : undefined, 5_000, "Retry control did not settle");
    }
    // Redelivery and another restart must not repeat already-committed work.
    send(second.connection, "benchmark:completed", "1001");
    send(second.connection, "benchmark:running", "1002");
    await waitForFixture(() => ["1001", "1002"].every(id => second.connection.recvAcks.some(ack => ack.messageId === id)) ? true : undefined, 5_000, "Redelivered messages were not acknowledged");
    second.child.kill("SIGTERM");
    assert.equal(await exited(second.child), 0);
    const third = await launch();
    send(third.connection, "benchmark:completed", "1001");
    send(third.connection, "benchmark:running", "1002");
    await waitForFixture(() => ["1001", "1002"].every(id => third.connection.recvAcks.some(ack => ack.messageId === id)) ? true : undefined, 5_000, "Post-restart redelivery was not acknowledged");
    await Bun.sleep(observationMs);
    const finalRows = auditRows(auditPath);
    for (const name of ["completed", "begin", "finish"]) {
      assert.equal(finalRows.filter(r => r.kind === "effect" && r.name === name).length, 1, `${name} effect must occur exactly once across restarts`);
    }
    assert.equal(finalRows.filter(r => r.kind === "overlap").length, 0);
    third.child.kill("SIGTERM");
    assert.equal(await exited(third.child), 0);
    await waitForFixture(() => auditRows(auditPath).filter(r => r.kind === "startup").every(r => !alive(r.pid)) ? true : undefined, 5_000, "Benchmark leaked an RPC worker");
    succeeded = true;
    return result;
  } finally {
    for (const child of children) {
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        try { await exited(child, 5_000); } catch { child.kill("SIGKILL"); await child.exited; }
      }
    }
    await cleanupRuntime?.();
    db?.close();
    await fixture.close();
    // Only PIDs recorded by this fixture, and still identifiable by its unique
    // audit argv, are eligible for emergency cleanup after a broken experiment.
    for (const row of auditRows(auditPath).filter(r => r.kind === "startup")) {
      if (!alive(row.pid)) continue;
      const ps = Bun.spawn(["/bin/ps", "-p", String(row.pid), "-o", "command="], { stdout: "pipe" });
      const command = await new Response(ps.stdout).text();
      await ps.exited;
      if (command.includes(auditPath)) process.kill(row.pid, "SIGKILL");
    }
    const logs = (await Promise.all(outputs)).flat().join("\n");
    if (!succeeded) console.error(logs.replaceAll(fixture.botToken, "[fixture-token]"));
    assert.ok(!logs.includes(fixture.botToken), "CLI leaked its fixture credential");
    await rm(root, { recursive: true, force: true });
  }
}

export async function runRestartBenchmark(cliExecutable?: string): Promise<void> {
  const results: Result[] = [];
  for (const mode of ["graceful", "crash"] as const) results.push(await scenario(mode, cliExecutable));
  const total = (key: keyof Result) => results.reduce((sum, result) => sum + result[key], 0);
  console.log(`METRIC seamless_restart_cases=${total("seamless")}`);
  console.log(`METRIC resumed_tasks=${total("resumed")}`);
  console.log(`METRIC recovered_queued_tasks=${total("queued")}`);
  console.log(`METRIC disruption_notices=${total("notices")}`);
  console.log(`METRIC session_overlaps=${total("overlaps")}`);
  console.log(`METRIC duplicate_effects=${total("duplicates")}`);
}

if (import.meta.main) await runRestartBenchmark();
