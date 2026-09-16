import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createChannelService } from "../src/service.ts";
import {
  FakeTransport,
  FakeWorkerFactory,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

test("a second instance for the same Bot refuses before it registers the Bot again", async () => {
  const first = await createCoreHarness();
  const transport = new FakeTransport({ bot: first.bot });
  const second = await createChannelService({
    config: first.config,
    transport,
    workerFactory: new FakeWorkerFactory(),
  });
  try {
    await expect(second.start()).rejects.toThrow("同一 Bot 已有 channel 实例运行");
    expect(transport.initializeCalls).toBe(0);

    // The instance that owns the lock is untouched and keeps serving its chats.
    const message = inbound({ messageId: "lock-owner-still-serving", channelId: "dm-lock" });
    await first.service.handleInbound(message);
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, message, (status) => status.state === "completed");
  } finally {
    await second.stop();
    await closeCoreHarness(first);
  }
});

test("a lock left behind by a dead process does not block the next start", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-lock-"));
  const bot = { uid: "bot-stale-lock", name: "Stale Lock Bot" };
  const digest = createHash("sha256").update(bot.uid, "utf8").digest("hex");
  const lockPath = join(tmpdir(), "omp-channel-octo-locks", `${digest}.lock`);
  const dead = Bun.spawn(["true"], { stdio: ["ignore", "ignore", "ignore"] });
  await dead.exited;
  await mkdir(dirname(lockPath), { recursive: true });
  await writeFile(lockPath, JSON.stringify({ pid: dead.pid, botUid: bot.uid }), { encoding: "utf8", mode: 0o600 });
  try {
    const harness = await createCoreHarness({ root, bot });
    try {
      const lock = JSON.parse(await readFile(lockPath, "utf8")) as { pid?: unknown };
      expect(lock.pid).toBe(process.pid);
    } finally {
      await closeCoreHarness(harness);
    }
  } finally {
    await rm(lockPath, { force: true }).catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});
