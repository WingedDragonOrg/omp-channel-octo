import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  BotIdentity,
  ChannelService,
  ChatTarget,
  CoreServiceConfig,
  InboundMessage,
  SendReceipt,
} from "../src/contracts";
import { createChannelService } from "../src/service";
import {
  FakeTransport,
  FakeWorkerFactory,
  inbound,
} from "./fixtures/core-fakes";

const STARTUP_PREFIX = "omp-channel-octo 已启动";
const STOP_PREFIX = "omp-channel-octo 正在停止";

/*
 * These are integration tests for wall-clock lifecycle budgets: the production deadline must be
 * measured against the platform clock (SQLite/transport work cannot be driven by fake timers).
 * `within` only bounds observation so a missing lifecycle event fails at the assertion instead of
 * hanging the worker; it is not a sleep. Everything else yields through `flushTasks`.
 */

interface Gate {
  readonly promise: Promise<void>;
  readonly release: () => void;
}

function gate(): Gate {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, release: resolve };
}

/** Bounded observation helper: a missing lifecycle event fails fast instead of hanging the suite. */
async function within<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(
    () => timeout.reject(new Error(`${label} did not happen within ${timeoutMs}ms`)),
    timeoutMs,
  );
  try {
    return await Promise.race([promise, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

/** Yields one event-loop turn without sleeping; enough for fire-and-forget callbacks to run. */
async function flushTasks(): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setImmediate(resolve);
  await promise;
}

async function waitUntil(
  predicate: () => boolean,
  label: string,
  attempts = 10_000,
): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (predicate()) return;
    await flushTasks();
  }
  throw new Error(`${label} did not happen after ${attempts} event-loop turns`);
}

function ownerTarget(ownerId: string): ChatTarget {
  return { channelId: ownerId, channelType: 1 };
}
function lifecycleBot(label: string, ownerUid?: string): BotIdentity {
  return {
    uid: `bot-lifecycle-${label}-${randomUUID()}`,
    name: "Lifecycle Bot",
    ...(ownerUid === undefined ? {} : { ownerUid }),
  };
}

/** The event payload carries the event's local time with offset and the serving process PID. */
function expectEventDetails(text: string): void {
  expect(text).toContain(String(process.pid));
  expect(text).toMatch(/[+-]\d{2}:?\d{2}/u);
}

function startupNotices(transport: LifecycleTransport): ObservedSend[] {
  return transport.sends.filter((send) => send.text.startsWith(STARTUP_PREFIX));
}

function stopNotices(transport: LifecycleTransport): ObservedSend[] {
  return transport.sends.filter((send) => send.text.startsWith(STOP_PREFIX));
}

function outboxRowCount(stateDir: string): number {
  const database = new Database(join(stateDir, "channel.sqlite"));
  try {
    const row = database.query("SELECT COUNT(*) AS count FROM outbox").get() as { count: number };
    return row.count;
  } finally {
    database.close();
  }
}

function firstPendingOutboxText(stateDir: string): string | null {
  const database = new Database(join(stateDir, "channel.sqlite"));
  try {
    const row = database
      .query(
        "SELECT text FROM outbox WHERE state IN ('pending', 'unknown') ORDER BY rowid ASC LIMIT 1",
      )
      .get() as { text?: string | null } | null;
    return row?.text ?? null;
  } finally {
    database.close();
  }
}

interface ObservedSend {
  readonly target: ChatTarget;
  readonly text: string;
  readonly signal: AbortSignal | undefined;
}

/**
 * Records each `sendText` invocation at the transport boundary before delegation, so a refused
 * notice is still observable as exactly one attempt, and lifecycle notices are distinguishable
 * from ordinary outbox traffic without assuming anything about the fixture's `sent` filtering.
 */
class LifecycleTransport extends FakeTransport {
  readonly sends: ObservedSend[] = [];
  readonly events: string[] = [];
  stopCalls = 0;
  private readonly sendWaiters: Array<{
    predicate: (send: ObservedSend) => boolean;
    resolve: (send: ObservedSend) => void;
  }> = [];

  protected recordSend(send: ObservedSend): void {
    this.sends.push(send);
    this.events.push(
      send.text.startsWith(STARTUP_PREFIX)
        ? "startup"
        : send.text.startsWith(STOP_PREFIX)
          ? "stop"
          : "text",
    );
    for (let index = this.sendWaiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.sendWaiters[index]!;
      if (waiter.predicate(send)) waiter.resolve(send);
    }
  }

  waitForSend(predicate: (send: ObservedSend) => boolean): Promise<ObservedSend> {
    const existing = this.sends.find(predicate);
    if (existing) return Promise.resolve(existing);
    const { promise, resolve } = Promise.withResolvers<ObservedSend>();
    this.sendWaiters.push({ predicate, resolve });
    return promise;
  }

  override async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    this.recordSend({ target: { ...target }, text, signal });
    return super.sendText(target, text, clientMsgNo, signal);
  }

  override async stop(): Promise<void> {
    this.stopCalls += 1;
    this.events.push("transport-stop");
    await super.stop();
  }
}

class GatedStartTransport extends LifecycleTransport {
  readonly startEntered = gate();
  readonly startGate = Promise.withResolvers<void>();

  override async start(
    onMessage: (message: InboundMessage) => Promise<void>,
    onOnline: () => void,
  ): Promise<void> {
    this.startEntered.release();
    await this.startGate.promise;
    return super.start(onMessage, onOnline);
  }
}
/** Holds the startup transport request open while stop races with the start completion path. */
class StartupGateTransport extends LifecycleTransport {
  readonly releaseStartup = gate();
  startupPending = false;
  stopWhileStartupPending = false;

  override async sendText(
    target: ChatTarget,
    text: string,
    _clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    this.recordSend({ target: { ...target }, text, signal });
    if (text.startsWith(STARTUP_PREFIX)) {
      this.startupPending = true;
      try {
        await this.releaseStartup.promise;
      } finally {
        this.startupPending = false;
      }
    }
    return { messageId: `startup-gate-${this.sends.length}` };
  }

  override async stop(): Promise<void> {
    this.stopWhileStartupPending ||= this.startupPending;
    await super.stop();
  }
}

/** Holds the stop send so the test can observe inbound and worker ordering at the boundary. */
class StopGateTransport extends LifecycleTransport {
  readonly releaseStopNotice = gate();

  override async sendText(
    target: ChatTarget,
    text: string,
    clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    if (!text.startsWith(STOP_PREFIX)) {
      return super.sendText(target, text, clientMsgNo, signal);
    }
    this.recordSend({ target: { ...target }, text, signal });
    await this.releaseStopNotice.promise;
    return { messageId: `stop-gate-${this.sends.length}` };
  }
}


class InitializeFailureTransport extends LifecycleTransport {
  override async initialize(): Promise<BotIdentity> {
    throw new Error("fixture authentication failed");
  }
}

/** Reports a send and never settles, even when the caller's signal aborts. */
class UnresponsiveTransport extends LifecycleTransport {
  override async sendText(
    target: ChatTarget,
    text: string,
    _clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    this.recordSend({ target: { ...target }, text, signal });
    // Deliberately ignores the caller's signal: only a caller-side race can bound this.
    return await Promise.withResolvers<SendReceipt>().promise;
  }
}

interface HangingEntry {
  readonly send: ObservedSend;
  settled: boolean;
  release: () => void;
}

/**
 * Holds every send at the transport boundary until released, modelling an outbox head that is
 * in flight and unanswered while the service attempts its lifecycle notice.
 */
class HangingRecordingTransport extends LifecycleTransport {
  hang = true;
  private readonly pending: HangingEntry[] = [];

  override async sendText(
    target: ChatTarget,
    text: string,
    _clientMsgNo: string,
    signal?: AbortSignal,
  ): Promise<SendReceipt> {
    const send: ObservedSend = { target: { ...target }, text, signal };
    this.recordSend(send);
    if (!this.hang) return { messageId: `hanging-${this.sends.length}` };
    const entry: HangingEntry = { send, settled: false, release: () => {} };
    const held = Promise.withResolvers<void>();
    entry.release = () => {
      entry.settled = true;
      held.resolve();
    };
    this.pending.push(entry);
    await held.promise;
    return { messageId: `hanging-${this.sends.length}` };
  }

  releaseAll(): void {
    this.hang = false;
    for (const entry of this.pending.splice(0)) entry.release();
  }

  isSettled(send: ObservedSend): boolean {
    return this.pending.find((entry) => entry.send === send)?.settled ?? true;
  }
}


interface LifecycleFixture {
  readonly root: string;
  readonly stateDir: string;
  readonly transport: LifecycleTransport;
  readonly workers: FakeWorkerFactory;
  readonly service: ChannelService;
}

interface LifecycleFixtureOptions {
  root?: string;
  bot?: BotIdentity;
  transport?: LifecycleTransport;
  workers?: FakeWorkerFactory;
  /** `null` omits the configured owner so only the persisted owner can answer. */
  ownerId?: string | null;
}

/** Builds an unstarted service so tests control the exact moment `start()` runs. */
async function createLifecycleFixture(
  options: LifecycleFixtureOptions = {},
): Promise<LifecycleFixture> {
  const rootPath =
    options.root ?? (await mkdtemp(join(tmpdir(), "omp-channel-octo-lifecycle-")));
  const root = await realpath(rootPath);
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const bot = options.bot ?? lifecycleBot("fixture");
  const transport = options.transport ?? new LifecycleTransport({ bot });
  const workers = options.workers ?? new FakeWorkerFactory();
  const config: CoreServiceConfig = {
    ...(options.ownerId === null ? {} : { ownerId: options.ownerId ?? "owner-lifecycle" }),
    allowedGroupIds: [],
    stateDir,
    homeDir: root,
  };
  const service = await createChannelService({
    config,
    transport,
    workerFactory: workers,
  });
  return { root, stateDir, transport, workers, service };
}

async function dispose(
  fixture: LifecycleFixture,
  options: { remove?: boolean } = {},
): Promise<void> {
  try {
    await within(fixture.service.stop(), 5_000, "fixture stop");
  } catch {
    // A failed assertion must not turn cleanup into the report's only failure.
  }
  if (options.remove ?? true) {
    await rm(fixture.root, { recursive: true, force: true });
  }
}

test("startup notice targets only the configured owner's DM once, with local time and PID", async () => {
  const bot = lifecycleBot("configured", "registration-owner");
  const transport = new LifecycleTransport({ bot });
  const fixture = await createLifecycleFixture({
    bot,
    transport,
    ownerId: "owner-configured",
  });
  try {
    await fixture.service.start();
    const owner = ownerTarget("owner-configured");
    const starts = startupNotices(transport);
    expect(starts).toHaveLength(1);
    expect(starts[0]!.target).toEqual(owner);
    expect(starts[0]!.text.startsWith(STARTUP_PREFIX)).toBe(true);
    expectEventDetails(starts[0]!.text);
    expect(
      transport.sends.some((send) => send.target.channelId === "registration-owner"),
    ).toBe(false);
    expect(
      transport.sends.every(
        (send) => send.target.channelType === 1 && send.target.channelId === "owner-configured",
      ),
    ).toBe(true);

    // A second `start()` and a transport reconnect are not new startup events.
    await fixture.service.start();
    await transport.reconnect();
    await flushTasks();
    expect(startupNotices(transport)).toHaveLength(1);

    // A lifecycle notice never starts OMP, creates a session/task, or sends a card.
    expect(fixture.workers.creations).toHaveLength(0);
    expect(fixture.workers.prompts).toHaveLength(0);
    expect(transport.sentCards).toHaveLength(0);
    expect(await fixture.service.status(owner)).toMatchObject({ state: "none", queued: 0 });

    await fixture.service.stop();
    const stops = stopNotices(transport);
    expect(stops).toHaveLength(1);
    expect(stops[0]!.target).toEqual(owner);
    expect(stops[0]!.text.startsWith(STOP_PREFIX)).toBe(true);
    expectEventDetails(stops[0]!.text);
    // Neither lifecycle event enters the persistent outbox, successful or not.
    expect(outboxRowCount(fixture.stateDir)).toBe(0);
  } finally {
    await dispose(fixture);
  }
});

test("persisted owner receives notices, and a later configured owner takes precedence", async () => {
  const bot = lifecycleBot("persisted");

  const firstTransport = new LifecycleTransport({ bot });
  const first = await createLifecycleFixture({
    bot,
    transport: firstTransport,
    ownerId: "owner-persisted",
  });
  try {
    await first.service.start();
    expect(startupNotices(firstTransport).map((send) => send.target)).toEqual([
      ownerTarget("owner-persisted"),
    ]);
    await first.service.stop();
    expect(stopNotices(firstTransport).map((send) => send.target)).toEqual([
      ownerTarget("owner-persisted"),
    ]);
  } finally {
    await dispose(first, { remove: false });
  }

  // Same Bot and state disk, no configured owner: the persisted enrollment still answers.
  const secondTransport = new LifecycleTransport({ bot });
  const second = await createLifecycleFixture({
    root: first.root,
    bot,
    transport: secondTransport,
    ownerId: null,
  });
  try {
    await second.service.start();
    expect(startupNotices(secondTransport).map((send) => send.target)).toEqual([
      ownerTarget("owner-persisted"),
    ]);
    await second.service.stop();
  } finally {
    await dispose(second, { remove: false });
  }

  // A configured owner is written durably at startup and wins over the previous enrollment.
  const thirdTransport = new LifecycleTransport({ bot });
  const third = await createLifecycleFixture({
    root: first.root,
    bot,
    transport: thirdTransport,
    ownerId: "owner-configured",
  });
  try {
    await third.service.start();
    expect(startupNotices(thirdTransport).map((send) => send.target)).toEqual([
      ownerTarget("owner-configured"),
    ]);
    expect(
      thirdTransport.sends.some((send) => send.target.channelId === "owner-persisted"),
    ).toBe(false);
  } finally {
    await dispose(third);
  }
});

test("a ready channel without a bound owner skips both lifecycle notices", async () => {
  const bot = lifecycleBot("no-owner-stop", "registration-owner");
  const transport = new LifecycleTransport({ bot });
  const fixture = await createLifecycleFixture({ bot, transport, ownerId: null });
  try {
    await fixture.service.start();
    await fixture.service.stop();
    expect(startupNotices(transport)).toHaveLength(0);
    expect(stopNotices(transport)).toHaveLength(0);
    expect(transport.sends).toHaveLength(0);
  } finally {
    await dispose(fixture);
  }
});

test("an unclaimed channel skips the startup notice and the first claim never backfills it", async () => {
  const bot = lifecycleBot("unclaimed", "registration-owner");
  const transport = new LifecycleTransport({ bot });
  const fixture = await createLifecycleFixture({ bot, transport, ownerId: null });
  try {
    await fixture.service.start();
    expect(transport.sends).toHaveLength(0);

    await fixture.service.handleInbound(
      inbound({
        channelId: "dm-late-owner",
        senderId: "late-owner",
        text: "hello from the first private sender",
      }),
    );
    const acknowledgment = await within(
      transport.waitForSend(
        (send) => send.target.channelType === 1 && send.target.channelId === "dm-late-owner",
      ),
      1_000,
      "first-claim receipt",
    );
    expect(acknowledgment.text.startsWith(STARTUP_PREFIX)).toBe(false);

    await flushTasks();
    expect(startupNotices(transport)).toHaveLength(0);
    await fixture.service.start();
    await transport.reconnect();
    await flushTasks();
    expect(startupNotices(transport)).toHaveLength(0);

    await fixture.service.stop();
    expect(stopNotices(transport).map((send) => send.target)).toEqual([
      ownerTarget("late-owner"),
    ]);
    // Bot registration's `ownerUid` is not a channel enrollment and never receives anything.
    expect(
      transport.sends.some((send) => send.target.channelId === "registration-owner"),
    ).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("startup notice waits for transport readiness and a rejected start notifies nobody", async () => {
  const bot = lifecycleBot("gate");
  const transport = new GatedStartTransport({ bot });
  const fixture = await createLifecycleFixture({ bot, transport, ownerId: "owner-gate" });
  try {
    const starting = fixture.service.start();
    await transport.startEntered.promise;
    await flushTasks();
    // The connection is not open yet: the startup event does not exist.
    expect(startupNotices(transport)).toHaveLength(0);

    transport.startGate.resolve();
    await within(starting, 1_000, "gated start");
    expect(startupNotices(transport)).toHaveLength(1);
    expect(startupNotices(transport)[0]!.target).toEqual(ownerTarget("owner-gate"));
  } finally {
    await dispose(fixture);
  }

  const failingBot = lifecycleBot("gate-failure");
  const failingTransport = new GatedStartTransport({ bot: failingBot });
  const failing = await createLifecycleFixture({
    bot: failingBot,
    transport: failingTransport,
    ownerId: "owner-gate",
  });
  try {
    const starting = failing.service.start();
    await failingTransport.startEntered.promise;
    expect(startupNotices(failingTransport)).toHaveLength(0);
    failingTransport.startGate.reject(new Error("fixture transport start failed"));
    await expect(starting).rejects.toThrow("fixture transport start failed");
    expect(startupNotices(failingTransport)).toHaveLength(0);
    await failing.service.stop();
    // A start that never became ready has no stop event either.
    expect(stopNotices(failingTransport)).toHaveLength(0);
  } finally {
    await dispose(failing);
  }
});

test("a start that fails before the transport opens sends neither startup nor stop notice", async () => {
  const bot = lifecycleBot("init-failure");
  const transport = new InitializeFailureTransport({ bot });
  const fixture = await createLifecycleFixture({ bot, transport, ownerId: "owner-gate" });
  try {
    await expect(fixture.service.start()).rejects.toThrow("fixture authentication failed");
    expect(transport.sends).toHaveLength(0);
    await fixture.service.stop();
    expect(transport.sends).toHaveLength(0);
  } finally {
    await dispose(fixture);
  }
});

test("stop cannot close the transport while a startup notice is still in flight", async () => {
  const bot = lifecycleBot("start-stop-race");
  const transport = new StartupGateTransport({ bot });
  const fixture = await createLifecycleFixture({
    bot,
    transport,
    ownerId: "owner-start-stop-race",
  });
  try {
    let startSettled = false;
    const starting = fixture.service.start().then(
      () => {
        startSettled = true;
      },
      () => {
        startSettled = true;
      },
    );
    await within(
      transport.waitForSend((send) => send.text.startsWith(STARTUP_PREFIX)),
      1_000,
      "pending startup notice",
    );

    let stopSettled = false;
    let stopError: unknown;
    const stopping = fixture.service.stop().then(
      () => {
        stopSettled = true;
      },
      (error: unknown) => {
        stopSettled = true;
        stopError = error;
      },
    );
    // Let an uncoordinated stop run through its immediate cleanup while the notice remains held.
    for (let turn = 0; turn < 8; turn += 1) await flushTasks();
    expect(startSettled).toBe(false);
    expect(stopSettled).toBe(false);
    expect(transport.stopCalls).toBe(0);
    expect(transport.stopWhileStartupPending).toBe(false);

    transport.releaseStartup.release();
    await within(Promise.all([starting, stopping]), 3_000, "startup/stop race settlement");
    expect(startupNotices(transport)).toHaveLength(1);
    expect(stopError).toBeUndefined();
    expect(transport.stopWhileStartupPending).toBe(false);
    expect(transport.stopCalls).toBe(1);
  } finally {
    transport.releaseStartup.release();
    await dispose(fixture);
  }
});

test("stop notice comes once after inbound closes and before worker stop or transport close", async () => {
  const bot = lifecycleBot("stop");
  const transport = new StopGateTransport({ bot });
  const promptStarted = gate();
  const workers = new FakeWorkerFactory(async ({ waitForAbort, onStop }) => {
    promptStarted.release();
    onStop(() => transport.events.push("worker-stop"));
    await waitForAbort();
  });
  const fixture = await createLifecycleFixture({
    bot,
    transport,
    workers,
    ownerId: "owner-stop",
  });
  const target = ownerTarget("owner-stop");
  try {
    await fixture.service.start();
    expect(startupNotices(transport)).toHaveLength(1);
    await fixture.service.handleInbound(
      inbound({
        channelId: target.channelId,
        senderId: target.channelId,
        text: "hold this turn open until stop",
      }),
    );
    await within(promptStarted.promise, 1_500, "worker prompt start");

    // Hold the transport call so ordering is observable without replacing the real worker fake.
    const stopping = fixture.service.stop();
    const repeated = fixture.service.stop();
    const stopNotice = await within(
      transport.waitForSend((send) => send.text.startsWith(STOP_PREFIX)),
      1_500,
      "stop notice before worker stop",
    );
    expect(stopNotice.target).toEqual(target);
    expect(stopNotice.signal?.aborted ?? false).toBe(false);
    expect(transport.events).not.toContain("worker-stop");
    expect(transport.stopCalls).toBe(0);

    await expect(
      transport.deliver(
        inbound({
          channelId: target.channelId,
          senderId: target.channelId,
          text: "late inbound during stop",
        }),
      ),
    ).rejects.toThrow();
    expect(stopNotices(transport)).toHaveLength(1);

    transport.releaseStopNotice.release();
    await within(stopping, 3_000, "stopped");
    await within(repeated, 3_000, "repeated stop");
    expect(stopNotices(transport)).toHaveLength(1);
    expect(transport.events.filter((event) => event === "stop")).toHaveLength(1);
    expect(transport.events.indexOf("stop")).toBeLessThan(
      transport.events.indexOf("worker-stop"),
    );
    expect(transport.events.indexOf("worker-stop")).toBeLessThan(
      transport.events.indexOf("transport-stop"),
    );
    expect(transport.stopCalls).toBe(1);
    expectEventDetails(stopNotice.text);
  } finally {
    transport.releaseStopNotice.release();
    await dispose(fixture);
  }
});

test("a refused startup notice is one attempt, not retried or persisted, and startup still succeeds", async () => {
  const bot = lifecycleBot("offline");
  const transport = new LifecycleTransport({ bot, online: false });
  const fixture = await createLifecycleFixture({
    bot,
    transport,
    ownerId: "owner-offline",
  });
  const target = ownerTarget("owner-offline");
  try {
    await fixture.service.start();
    const starts = startupNotices(transport);
    expect(starts).toHaveLength(1);
    expect(transport.attempts).toHaveLength(1);
    expect(transport.attempts[0]!.error).toBeDefined();
    expect(await fixture.service.status(target)).toMatchObject({ state: "none", queued: 0 });
    expect(outboxRowCount(fixture.stateDir)).toBe(0);

    // Reconnecting flushes the durable outbox only; the failed notice has no row to replay.
    await transport.reconnect();
    await flushTasks();
    expect(startupNotices(transport)).toHaveLength(1);
    expect(outboxRowCount(fixture.stateDir)).toBe(0);

    await fixture.service.stop();
    expect(stopNotices(transport)).toHaveLength(1);
    expect(outboxRowCount(fixture.stateDir)).toBe(0);
  } finally {
    await dispose(fixture);
  }
});

test("a transport that never answers costs each lifecycle event its own bounded wait, with no retry", async () => {
  const bot = lifecycleBot("hang");
  const transport = new UnresponsiveTransport({ bot });
  const fixture = await createLifecycleFixture({
    bot,
    transport,
    ownerId: "owner-hang",
  });
  try {
    const startStartedAt = Date.now();
    await within(fixture.service.start(), 4_000, "start with unresponsive transport");
    const startElapsed = Date.now() - startStartedAt;
    expect(startElapsed).toBeGreaterThanOrEqual(1_800);
    expect(startElapsed).toBeLessThan(3_000);
    expect(startupNotices(transport)).toHaveLength(1);

    await flushTasks();
    expect(startupNotices(transport)).toHaveLength(1);
    expect(outboxRowCount(fixture.stateDir)).toBe(0);
    expect(await fixture.service.status(ownerTarget("owner-hang"))).toMatchObject({
      state: "none",
    });

    const stopStartedAt = Date.now();
    await within(fixture.service.stop(), 4_000, "stop with unresponsive transport");
    const stopElapsed = Date.now() - stopStartedAt;
    expect(stopElapsed).toBeGreaterThanOrEqual(1_800);
    expect(stopElapsed).toBeLessThan(3_000);
    expect(stopNotices(transport)).toHaveLength(1);

    await flushTasks();
    expect(stopNotices(transport)).toHaveLength(1);
    expect(transport.stopCalls).toBe(1);
  } finally {
    await dispose(fixture);
  }
}, { timeout: 20_000 });

test("a stuck outbox head does not delay the startup notice", async () => {
  const bot = lifecycleBot("outbox");
  const offline = new LifecycleTransport({ bot, online: false });
  const first = await createLifecycleFixture({
    bot,
    transport: offline,
    ownerId: "owner-outbox",
  });
  try {
    await first.service.start();
    // `/status` enqueues a durable reply; staying offline leaves it as the queue head.
    await first.service.handleInbound(
      inbound({
        channelId: "dm-outbox-owner",
        senderId: "owner-outbox",
        text: "/status",
      }),
    );
    await waitUntil(
      () => outboxRowCount(first.stateDir) > 0,
      "persisted outbox head",
    );
    expect(firstPendingOutboxText(first.stateDir)).not.toBeNull();
  } finally {
    await first.service.stop();
  }

  const hanging = new HangingRecordingTransport({ bot });
  const second = await createLifecycleFixture({
    root: first.root,
    bot,
    transport: hanging,
    ownerId: null,
  });
  try {
    const starting = second.service.start();
    const head = await within(
      hanging.waitForSend((send) => send.target.channelId === "dm-outbox-owner"),
      2_000,
      "stuck outbox head",
    );
    const notice = await within(
      hanging.waitForSend((send) => send.text.startsWith(STARTUP_PREFIX)),
      1_500,
      "startup notice beside a stuck outbox head",
    );
    expect(notice.target).toEqual(ownerTarget("owner-outbox"));
    expect(hanging.isSettled(head)).toBe(false);
    expect(hanging.isSettled(notice)).toBe(false);

    hanging.releaseAll();
    await within(starting, 2_000, "start after releasing the outbox head");
    expect(startupNotices(hanging)).toHaveLength(1);
    await second.service.stop();
    expect(stopNotices(hanging)).toHaveLength(1);
  } finally {
    hanging.releaseAll();
    await dispose(second);
  }
});
