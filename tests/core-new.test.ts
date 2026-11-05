import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { openChannelStore, type ChannelStore } from "../src/store";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  submitAndWaitForIngressHandoff,
  waitForStatus,
} from "./fixtures/core-fakes";

test("/new without a path initially uses ~/tmp, then reuses that cwd with a fresh session", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-default", ...target, text: "/new" }),
    );
    const status = await waitForStatus(
      harness.service,
      target,
      (next) => next.cwd === join(harness.root, "tmp") && next.sessionId !== null,
    );

    expect(status.cwd).toBe(join(harness.root, "tmp"));
    expect(status.sessionId).toEqual(expect.any(String));
    expect(status.state).toBe("idle");
    expect((await stat(join(harness.root, "tmp"))).isDirectory()).toBe(true);

    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-default-refresh", ...target, text: "/new" }),
    );
    const refreshedStatus = await waitForStatus(
      harness.service,
      target,
      (next) => next.sessionId !== status.sessionId,
    );
    expect(refreshedStatus.sessionId).not.toBe(status.sessionId);
    expect(refreshedStatus.cwd).toBe(status.cwd);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new records the session switch on the binding, and later session writes keep it", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "switch-before-new", ...target, text: "hello" }),
    );
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    const store = await openChannelStore(harness.stateDir);
    try {
      // 首次绑定没有这段历史：问题不属于任何一次主动换会话。
      expect(store.getBinding(harness.bot.uid, target)?.sessionSwitchedAt ?? null).toBeNull();
      const previousSessionId = (await harness.service.status(target)).sessionId;
      const before = Date.now();
      await submitAndWaitForIngressHandoff(
        harness,
        inbound({ messageId: "switch-new", ...target, text: "/new" }),
      );
      const switchedStatus = await waitForStatus(
        harness.service,
        target,
        (status) => status.sessionId !== previousSessionId,
      );
      const switched = store.getBinding(harness.bot.uid, target)?.sessionSwitchedAt ?? null;
      expect(switchedStatus.sessionId).not.toBe(previousSessionId);
      expect(switched).not.toBeNull();
      expect(switched!).toBeGreaterThanOrEqual(before);

      // 之后普通任务重写绑定（同一会话）不得抹掉它。
      await submitAndWaitForIngressHandoff(
        harness,
        inbound({ messageId: "switch-after-new", ...target, text: "run this" }),
      );
      await harness.workers.waitForPromptCount(2);
      await waitForStatus(harness.service, target, (status) => status.state === "completed");
      expect(store.getBinding(harness.bot.uid, target)?.sessionSwitchedAt).toBe(switched);
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new accepts a real quoted directory; invalid paths preserve the prior binding and no-arg reuses its cwd", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    const explicitPath = join(harness.root, "work with spaces");
    await mkdir(explicitPath, { recursive: true });
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({
        messageId: "new-explicit",
        ...target,
        text: `/new "${explicitPath}"`,
      }),
    );
    const explicitStatus = await waitForStatus(
      harness.service,
      target,
      (status) => status.cwd === explicitPath && status.sessionId !== null,
    );
    expect(explicitStatus.cwd).toBe(explicitPath);

    const missingPath = join(harness.root, "does-not-exist");
    const invalidPathReply = harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId &&
        sentTarget.channelType === target.channelType &&
        /目录|路径|不存在|失败|无效/.test(text),
    );
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({
        messageId: "new-missing",
        ...target,
        text: `/new ${missingPath}`,
      }),
    );
    await invalidPathReply;
    const afterInvalid = await harness.service.status(target);
    expect(afterInvalid.cwd).toBe(explicitPath);
    expect(afterInvalid.sessionId).toBe(explicitStatus.sessionId);
    expect(
      harness.transport.sent.some(
        ({ target: sentTarget, text }) =>
          sentTarget.channelId === target.channelId &&
          /目录|路径|不存在|失败|无效/.test(text),
      ),
    ).toBe(true);

    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-reuse-explicit-cwd", ...target, text: "/new" }),
    );
    const reusedStatus = await waitForStatus(
      harness.service,
      target,
      (status) => status.sessionId !== explicitStatus.sessionId,
    );
    expect(reusedStatus.cwd).toBe(explicitPath);
    expect(reusedStatus.sessionId).not.toBe(explicitStatus.sessionId);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new is rejected while the current chat has a running task", async () => {
  let releaseTask: (() => void) | undefined;
  const harness = await createCoreHarness({
    prompt: async ({ onStop }) => {
      await new Promise<void>((resolve) => {
        releaseTask = resolve;
        onStop(resolve);
      });
    },
  });
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-bind-before-run", ...target, text: "/new" }),
    );
    await waitForStatus(harness.service, target, (status) => status.sessionId !== null);
    const firstTask = harness.service.submitInbound(
      inbound({ messageId: "new-running-task", ...target, text: "long work" }),
    );
    await firstTask;
    await harness.workers.waitForPromptCount(1);
    const busyReply = harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId &&
        sentTarget.channelType === target.channelType &&
        /cancel|取消|忙|运行/.test(text),
    );
    await harness.service.submitInbound(
      inbound({
        messageId: "new-while-running",
        ...target,
        text: `/new ${join(harness.root, "another")}`,
      }),
    );
    await busyReply;
    const status = await harness.service.status(target);
    expect(status.state).toBe("running");
    expect(status.cwd).toBe(join(harness.root, "tmp"));
    expect(
      harness.transport.sent.some(
        ({ target: sentTarget, text }) =>
          sentTarget.channelId === target.channelId && /cancel|取消|忙|运行/.test(text),
      ),
    ).toBe(true);

    releaseTask?.();
    await waitForStatus(harness.service, target, (next) => next.state === "completed");
    await firstTask;
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new reports the fresh session's actual model next to the session id and cwd", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-model-default", ...target, text: "/new" }),
    );
    const status = await waitForStatus(harness.service, target, (next) => next.sessionId !== null);
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes(status.sessionId!),
    );
    // No chat override: the worker starts without `--model`, so the receipt carries OMP's default.
    expect(harness.transport.sent.at(-1)?.text).toBe(
      `已创建会话 ${status.sessionId}，cwd=${status.cwd}，模型=fixture/default`,
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new after /switch reports the chat model with its thinking level, matching the worker", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-switch-first", ...target, text: "/switch fixture/fast:high" }),
    );
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes("fixture/fast:high"),
    );
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-model-switched", ...target, text: "/new" }),
    );
    const status = await waitForStatus(harness.service, target, (next) => next.sessionId !== null);
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes(status.sessionId!),
    );
    expect(harness.transport.sent.at(-1)?.text).toBe(
      `已创建会话 ${status.sessionId}，cwd=${status.cwd}，模型=fixture/fast:high`,
    );
    // The receipt reports what the session's worker was actually started with.
    expect(harness.workers.creations.at(-1)?.model).toBe("fixture/fast:high");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/reset shares the creation receipt and reports the model too", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "reset-model", ...target, text: "/reset" }),
    );
    const status = await waitForStatus(harness.service, target, (next) => next.sessionId !== null);
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes(status.sessionId!),
    );
    expect(harness.transport.sent.at(-1)?.text).toBe(
      `已创建会话 ${status.sessionId}，cwd=${status.cwd}，模型=fixture/default`,
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new does not replace the binding when worker-ledger finalization fails", async () => {
  const harness = await createCoreHarness();
  const target = { channelId: "dm-new-ledger-finalization", channelType: 1 as const };
  const source = join(harness.root, "source");
  const destination = join(harness.root, "destination");
  let store: ChannelStore | undefined;
  let failureDb: Database | undefined;
  try {
    await mkdir(source);
    await mkdir(destination);
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-ledger-source", ...target, text: `/new ${source}` }),
    );
    await waitForStatus(harness.service, target, (status) => status.cwd === source && status.sessionId !== null);
    store = await openChannelStore(harness.stateDir);
    const before = store.getBinding(harness.bot.uid, target);
    expect(before).not.toBeNull();

    failureDb = new Database(join(harness.stateDir, "channel.sqlite"));
    failureDb.query(`
      CREATE TRIGGER fail_worker_stop
      BEFORE UPDATE OF state ON worker_ledger
      WHEN NEW.state = 'stopped'
      BEGIN
        SELECT RAISE(FAIL, 'fixture worker-ledger finalization failed');
      END
    `).run();
    const failureReply = harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId &&
        /无法|未能|失败/.test(text) &&
        !text.includes("已创建会话"),
    );
    await harness.service.submitInbound(
      inbound({ messageId: "new-ledger-failure", ...target, text: `/new ${destination}` }),
    );
    await failureReply;

    expect(store.getBinding(harness.bot.uid, target)).toEqual(before);
    expect(harness.transport.sent.at(-1)?.text).toMatch(/无法|未能|失败/);
    expect(harness.transport.sent.at(-1)?.text).not.toContain("已创建会话");
  } finally {
    if (failureDb !== undefined) {
      failureDb.query("DROP TRIGGER fail_worker_stop").run();
      failureDb.close();
    }
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("/new keeps its receipt model-free rather than inventing one when OMP cannot answer", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    harness.workers.failModelsWith(new Error("rpc gone"));
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-model-unreachable", ...target, text: "/new" }),
    );
    const status = await waitForStatus(harness.service, target, (next) => next.sessionId !== null);
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes(status.sessionId!),
    );
    // The session was created; only the model display degrades, to the original receipt.
    expect(status.state).toBe("idle");
    expect(harness.transport.sent.at(-1)?.text).toBe(
      `已创建会话 ${status.sessionId}，cwd=${status.cwd}`,
    );
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/new keeps its receipt model-free when OMP reports no current model", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    harness.workers.catalog = { current: null, level: null, available: [] };
    await submitAndWaitForIngressHandoff(
      harness,
      inbound({ messageId: "new-model-unknown", ...target, text: "/new" }),
    );
    const status = await waitForStatus(harness.service, target, (next) => next.sessionId !== null);
    await harness.transport.waitForRemoteText(
      ({ target: sentTarget, text }) =>
        sentTarget.channelId === target.channelId && text.includes(status.sessionId!),
    );
    expect(harness.transport.sent.at(-1)?.text).toBe(
      `已创建会话 ${status.sessionId}，cwd=${status.cwd}`,
    );
  } finally {
    await closeCoreHarness(harness);
  }
});
test("/new in a shared workspace does not wait for a held task in another project", async () => {
  const heldStarted = Promise.withResolvers<void>();
  const releaseHeld = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    prompt: async ({ text }) => {
      if (text === "hold owned project") {
        heldStarted.resolve();
        await releaseHeld.promise;
      }
    },
  });
  const first = { channelId: "group-tools____owned-new-api", channelType: 5 as const };
  const second = { channelId: "group-tools____owned-new-web", channelType: 5 as const };
  const entry = join(harness.root, "shared-workspace");
  const api = join(entry, "api");
  const web = join(entry, "web");
  try {
    await mkdir(api, { recursive: true });
    await mkdir(web, { recursive: true });
    for (const [target, path, messageId] of [
      [first, api, "owned-new-api"],
      [second, web, "owned-new-web"],
    ] as const) {
      await submitAndWaitForIngressHandoff(
        harness,
        inbound({
          messageId,
          ...target,
          text: `/new ${path}`,
          mentionedUserIds: [harness.bot.uid],
        }),
      );
      await waitForStatus(
        harness.service,
        target,
        (status) => status.cwd === path && status.sessionId !== null,
      );
    }
    const store = await openChannelStore(harness.stateDir);
    try {
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        ...first,
        layout: "shared",
        status: "ready",
        entryPath: entry,
        projects: [
          { name: "api", repo: harness.root, path: api, branch: "api", created: true },
          { name: "web", repo: harness.root, path: web, branch: "web", created: true },
        ],
        error: null,
      });
    } finally {
      store.close();
    }

    const heldTask = harness.service.submitInbound(inbound({
      messageId: "owned-new-held",
      ...first,
      text: "hold owned project",
      mentionedUserIds: [harness.bot.uid],
    }));
    await heldTask;
    await heldStarted.promise;
    const previous = await harness.service.status(second);

    await submitAndWaitForIngressHandoff(
      harness,
      inbound({
        messageId: "owned-new-independent",
        ...second,
        text: `/new ${web}`,
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const renewed = await waitForStatus(
      harness.service,
      second,
      (status) => status.sessionId !== previous.sessionId,
    );
    expect(renewed.sessionId).not.toBe(previous.sessionId);
    expect(renewed.cwd).toBe(web);
    expect((await harness.service.status(first)).state).toBe("running");

    releaseHeld.resolve();
    await heldTask;
    await waitForStatus(harness.service, first, status => status.state === "completed");
  } finally {
    releaseHeld.resolve();
    await closeCoreHarness(harness);
  }
});
