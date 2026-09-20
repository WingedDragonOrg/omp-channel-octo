import assert from "node:assert/strict";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createChannelService } from "../../src/service.ts";
import { createWorkerFactory } from "../../src/omp/index.ts";
import { FakeTransport, inbound } from "./core-fakes.ts";

// Measures channel retention, not the installed OMP runtime's memory footprint.
// Fixed 3-second observation window, 30 samples; no live transport or model.
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-resource-bench-")));
const pids: number[] = [];
const transport = new FakeTransport({ bot: { uid: "resource-benchmark", name: "Benchmark" } });
transport.cardSupported = true;
const factory = createWorkerFactory({
  ompCommand: process.execPath,
  extraArgs: [join(import.meta.dir, "resource-rpc.ts")],
  startupTimeoutMs: 5000,
  cancelTimeoutMs: 1000,
});
await mkdir(join(root, "state"));
const service = await createChannelService({
  config: { ownerId: "owner-1", allowedGroupIds: [], stateDir: join(root, "state"), homeDir: root },
  transport,
  workerFactory: {
    recover: (info) => factory.recover(info),
    sessionRootsFor: async () => [],
    create: (options) => factory.create({ ...options, onSpawn: (info) => {
      pids.push(info.pid);
      options.onSpawn(info);
    } }),
  },
});
const target = { channelType: 1 as const, channelId: "resource-wait" };
async function until(check: () => Promise<boolean>): Promise<void> {
  const deadline = performance.now() + 10000;
  while (!(await check())) {
    assert(performance.now() < deadline, "benchmark lifecycle deadline exceeded");
    await Bun.sleep(20);
  }
}
async function sample() {
  const child = Bun.spawn(["ps", "-axo", "pid=,rss="], { stdout: "pipe", stderr: "pipe" });
  const text = await new Response(child.stdout).text();
  assert.equal(await child.exited, 0, "ps sampling failed");
  let count = 0, rss = 0;
  for (const line of text.trim().split("\n")) {
    const [pid, kb] = line.trim().split(/\s+/).map(Number);
    if (pid !== undefined && pids.includes(pid)) { count++; rss += kb ?? 0; }
  }
  return { count, rss };
}
let occupancy = 0, rss = 0;
try {
  await service.start();
  await service.handleInbound(inbound({ ...target, messageId: "start", text: "ask resource question" }));
  await until(async () => (await service.status(target)).pendingQuestionIds.length === 1);
  const before = await service.status(target);
  const started = performance.now();
  for (let i = 1; i <= 30; i++) {
    await Bun.sleep(Math.max(0, started + i * 100 - performance.now()));
    const s = await sample();
    occupancy += s.count * 100;
    rss += s.rss;
    assert.equal((await service.status(target)).pendingQuestionIds.length, 1, "question lost while waiting");
  }
  const reply = inbound({ ...target, messageId: "answer", text: "benchmark-answer" });
  const resumed = performance.now();
  await service.handleInbound(reply);
  await until(async () => (await service.status(target)).state === "completed");
  await until(async () => transport.sent.some(x => x.text.includes("benchmark-answer")));
  const resumeMs = performance.now() - resumed;
  const after = await service.status(target);
  assert.equal(after.sessionId, before.sessionId, "answer changed session identity");
  assert.equal(after.pendingQuestionIds.length, 0);
  await until(async () => (await sample()).count === 0);
  const count = pids.length;
  await service.handleInbound(reply);
  await Bun.sleep(200);
  assert.equal(pids.length, count, "duplicate answer started another worker");
  await service.stop();
  assert.equal((await sample()).count, 0, "worker leaked after shutdown");
  console.log(`METRIC waiting_worker_ms=${occupancy}`);
  console.log(`METRIC waiting_worker_rss_kib=${(rss / 30).toFixed(2)}`);
  console.log(`METRIC answer_completion_ms=${resumeMs.toFixed(2)}`);
  console.log(`METRIC workers_spawned=${pids.length}`);
} finally {
  await service.stop();
  await rm(root, { recursive: true, force: true });
}
