import { expect, test } from "bun:test";
import { readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import type { OmpWorker, WorkerOptions } from "../src/contracts";
import { openChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";
import type { CoreHarness } from "./fixtures/core-fakes";

const DM = { channelId: "dm-switch", channelType: 1 as const };

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, resolve };
}

/** Probe workers use a throwaway temp session dir; a finished command leaves none behind. */
async function probeDirs(): Promise<string[]> {
  const entries = await readdir(tmpdir());
  return entries.filter(entry => entry.startsWith("omp-channel-octo-switch-"));
}

async function sentText(harness: CoreHarness): Promise<string> {
  const last = harness.transport.sent.at(-1);
  return last?.text ?? "";
}

test("an empty /switch reports the current model and every available model", async () => {
  const harness = await createCoreHarness();
  try {
    const before = await probeDirs();
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-list", text: "/switch" }));
    const report = await sentText(harness);
    expect(report).toContain("当前模型：fixture/default");
    expect(report).toContain("下次启动策略：不指定（由 OMP 自行决定）");
    expect(report).toContain("fixture：default、fast");
    expect(report).toContain("other：fast");
    // The probe is a throwaway fresh worker: one creation, stopped, no ledger row, no temp dir.
    expect(harness.workers.creations.length).toBe(1);
    expect(harness.workers.workers[0]?.stopped).toBe(true);
    expect(await probeDirs()).toEqual(before);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.listWorkerLedger(harness.bot.uid)).toEqual([]);
      expect(store.getChatModel(harness.bot.uid, DM)).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/switch <selector> stores what OMP resolved and the next turn starts with it", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-set", text: "/switch fixture/fast" }));
    expect(await sentText(harness)).toBe("已切换本聊天模型为 fixture/fast，从下一回合开始生效。");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, DM)).toBe("fixture/fast");
    } finally {
      store.close();
    }
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-task", text: "run the fixture" }));
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-status", text: "/status" }));
    expect(await sentText(harness)).toContain("model=fixture/fast");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/switch resolves role aliases and thinking-level suffixes through OMP", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-role", text: "/switch @fast" }));
    expect(await sentText(harness)).toBe("已切换本聊天模型为 other/fast:max，从下一回合开始生效。");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-level", text: "/switch fixture/fast:high" }));
    expect(await sentText(harness)).toBe("已切换本聊天模型为 fixture/fast:high，从下一回合开始生效。");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, DM)).toBe("fixture/fast:high");
    } finally {
      store.close();
    }
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-role-task", text: "run the fixture" }));
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("the switched model survives a service restart", async () => {
  const first = await createCoreHarness();
  const root = first.root;
  try {
    await first.service.handleInbound(inbound({ ...DM, messageId: "switch-persist", text: "/switch fixture/fast" }));
    expect(first.transport.sent.some(({ text }) => text.includes("fixture/fast"))).toBe(true);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }
  const restarted = await createCoreHarness({ root });
  try {
    await restarted.service.handleInbound(inbound({ ...DM, messageId: "switch-after-restart", text: "run the fixture" }));
    await restarted.workers.waitForPromptCount(1);
    expect(restarted.workers.creations.at(-1)?.model).toBe("fixture/fast");
    await restarted.service.handleInbound(inbound({ ...DM, messageId: "switch-status-after", text: "/status" }));
    expect(await sentText(restarted)).toContain("model=fixture/fast");
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a selector OMP rejects changes nothing", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-miss", text: "/switch nope" }));
    expect(await sentText(harness)).toBe('未找到模型 "nope"；发送 /switch 查看可用模型列表。');

    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, DM)).toBeNull();
    } finally {
      store.close();
    }
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-miss-task", text: "run the fixture" }));
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a selector with whitespace is answered without starting a worker", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-split", text: "/switch two words" }));
    expect(await sentText(harness)).toBe('未找到模型 "two words"；发送 /switch 查看可用模型列表。');
    expect(harness.workers.creations).toEqual([]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/switch default goes back to OMP's default model", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-before-default", text: "/switch fixture/fast" }));
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-default", text: "/switch default" }));
    expect(await sentText(harness)).toContain("下次启动策略：不指定（由 OMP 自行决定）");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, DM)).toBeNull();
    } finally {
      store.close();
    }
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-default-task", text: "run the fixture" }));
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();
  } finally {
    await closeCoreHarness(harness);
  }
});

test("switching mid-turn moves the running session and the next worker", async () => {
  const running = deferred();
  const finish = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "long-running") {
        running.resolve();
        await finish.promise;
      }
      await append("effects.log", `${text}\n`);
    },
  });
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-run", text: "long-running" }));
    await running.promise;
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-mid-run", text: "/switch fixture/fast" }));
    expect(await sentText(harness)).toBe("已切换本聊天模型为 fixture/fast（运行中会话已同步切换）。");
    // The running turn's own worker took the dispatch — no probe worker was started for it.
    expect(harness.workers.creations.length).toBe(1);
    expect(harness.workers.workers[0]?.switchDispatches).toEqual(["fixture/fast"]);
    finish.resolve();
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-run-next", text: "next turn" }));
    await harness.workers.waitForPromptCount(2);
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("an empty /switch during a turn reports the running session's model", async () => {
  const running = deferred();
  const finish = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "long-running") {
        running.resolve();
        await finish.promise;
      }
      await append("effects.log", `${text}\n`);
    },
  });
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-live-run", text: "long-running" }));
    await running.promise;
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-live-switch", text: "/switch @fast" }));
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-live-list", text: "/switch" }));
    const text = await sentText(harness);
    expect(text).toContain("当前模型：other/fast:max（运行中会话）");
    expect(text).toContain("下次启动策略：other/fast:max（本聊天指定）");
    // The running session answered the report itself, so no probe worker was started.
    expect(harness.workers.creations.length).toBe(1);
    finish.resolve();
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");
  } finally {
    await closeCoreHarness(harness);
  }
});

class FailingProbeFactory extends FakeWorkerFactory {
  override async create(_options: WorkerOptions): Promise<OmpWorker> {
    throw new Error("probe worker unavailable");
  }
}

test("a failed probe reports a bounded error and changes nothing", async () => {
  const harness = await createCoreHarness({ workers: new FailingProbeFactory() });
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-probe-fail", text: "/switch" }));
    expect(await sentText(harness)).toBe("无法从 OMP 读取模型信息，请稍后重试。");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-probe-fail-set", text: "/switch fixture/fast" }));
    expect(await sentText(harness)).toBe("无法从 OMP 读取模型信息，请稍后重试。");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.getChatModel(harness.bot.uid, DM)).toBeNull();
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed model probe recovers its worker when stop cannot confirm termination", async () => {
  class RecoverableProbeFactory extends FakeWorkerFactory {
    private terminate: (() => Promise<boolean>) | undefined;

    override async create(options: WorkerOptions): Promise<OmpWorker> {
      const worker = await super.create(options);
      this.terminate = worker.stop.bind(worker);
      worker.stop = async () => false;
      return worker;
    }

    override async recover(): Promise<boolean> {
      return this.terminate!();
    }
  }

  const workers = new RecoverableProbeFactory();
  workers.failModelsWith(new Error("model query failed"));
  const harness = await createCoreHarness({ workers });
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-recover", text: "/switch" }));
    expect(await sentText(harness)).toBe("无法从 OMP 读取模型信息，请稍后重试。");
    expect(workers.workers[0]?.stopped).toBe(true);
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.listWorkerLedger(harness.bot.uid)).toEqual([]);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a successful model probe keeps its catalog when stop cannot confirm termination", async () => {
  class UnconfirmedStopProbeFactory extends FakeWorkerFactory {
    override async create(options: WorkerOptions): Promise<OmpWorker> {
      const worker = await super.create(options);
      worker.stop = async () => false;
      return worker;
    }
  }

  const harness = await createCoreHarness({ workers: new UnconfirmedStopProbeFactory() });
  try {
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-stop-unconfirmed", text: "/switch" }));
    const catalog = await sentText(harness);
    expect(catalog).toContain("fixture/default");
    expect(catalog).toContain("fast");
    const store = await openChannelStore(harness.stateDir);
    try {
      expect(store.listWorkerLedger(harness.bot.uid)).toMatchObject([{ state: "running" }]);
      await harness.service.handleInbound(
        inbound({ ...DM, messageId: "switch-stop-blocked-task", text: "wait for confirmed cleanup" }),
      );
      await waitForStatus(harness.service, DM, status => status.state === "queued");
      expect(harness.workers.prompts).toEqual([]);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a deployment without any available model says so", async () => {
  const harness = await createCoreHarness();
  try {
    harness.workers.catalog = { current: "fixture/default", level: null, available: [] };
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-no-models", text: "/switch" }));
    const report = await sentText(harness);
    expect(report).toContain("当前模型：fixture/default（临时探测");
    expect(report).toContain("下次启动策略：不指定（由 OMP 自行决定）");
    expect(report).toContain("OMP 当前没有可用模型。");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "switch-no-models-set", text: "/switch fixture/fast" }));
    expect(await sentText(harness)).toBe('未找到模型 "fixture/fast"；发送 /switch 查看可用模型列表。');
  } finally {
    await closeCoreHarness(harness);
  }
});

test("default clears a model even when OMP advertises a provider named default", async () => {
  const harness = await createCoreHarness();
  const store = await openChannelStore(harness.stateDir);
  try {
    harness.workers.catalog.available.push({ provider: "default", id: "provider-model" });
    store.setChatModel(harness.bot.uid, DM, "fixture/fast");
    await harness.service.handleInbound(inbound({ ...DM, messageId: "default-provider", text: "/switch DEFAULT" }));
    expect(store.getChatModel(harness.bot.uid, DM)).toBeNull();
    expect(harness.workers.workers.flatMap(worker => worker.switchDispatches)).toEqual([]);
    expect(harness.transport.sentCards).toEqual([]);
    await harness.service.handleInbound(inbound({ ...DM, messageId: "default-provider-task", text: "run" }));
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.creations.at(-1)?.model).toBeUndefined();
  } finally { store.close(); await closeCoreHarness(harness); }
});

test("non-exact provider tokens retain OMP model-id and substring resolution without extra probes", async () => {
  const harness = await createCoreHarness();
  const store = await openChannelStore(harness.stateDir);
  try {
    harness.workers.catalog.available = [{ provider: "openai-codex", id: "unique-model" }];
    for (const [index, selector] of ["unique-model", "unique", "openai"].entries()) {
      await harness.service.handleInbound(inbound({ ...DM, messageId: `native-selector-${index}`, text: `/switch ${selector}` }));
      expect(store.getChatModel(harness.bot.uid, DM)).toBe("openai-codex/unique-model");
      expect(harness.workers.creations).toHaveLength(index + 1);
      expect(harness.workers.workers[index]?.switchDispatches).toEqual([selector]);
      expect(harness.workers.workers[index]?.stopped).toBe(true);
    }
    await harness.service.handleInbound(inbound({ ...DM, messageId: "case-provider", text: "/switch OPENAI-CODEX" }));
    expect(harness.workers.workers.at(-1)?.switchDispatches).toEqual(["OPENAI-CODEX"]);
    expect(harness.transport.sentCards).toEqual([]);
  } finally { store.close(); await closeCoreHarness(harness); }
});
