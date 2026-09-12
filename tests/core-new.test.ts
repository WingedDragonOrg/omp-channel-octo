import { expect, test } from "bun:test";
import { mkdir, stat } from "node:fs/promises";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes";

test("/new without a path initially uses ~/tmp, then reuses that cwd with a fresh session", async () => {
  const harness = await createCoreHarness();
  try {
    const target = { channelId: "dm-owner", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "new-default", ...target, text: "/new" }),
    );

    const status = await harness.service.status(target);
    expect(status.cwd).toBe(join(harness.root, "tmp"));
    expect(status.sessionId).toEqual(expect.any(String));
    expect(status.state).toBe("idle");
    expect((await stat(join(harness.root, "tmp"))).isDirectory()).toBe(true);

    await harness.service.handleInbound(
      inbound({ messageId: "new-default-refresh", ...target, text: "/new" }),
    );
    const refreshedStatus = await harness.service.status(target);
    expect(refreshedStatus.sessionId).not.toBe(status.sessionId);
    expect(refreshedStatus.cwd).toBe(status.cwd);
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
    await harness.service.handleInbound(
      inbound({
        messageId: "new-explicit",
        ...target,
        text: `/new "${explicitPath}"`,
      }),
    );
    const explicitStatus = await harness.service.status(target);
    expect(explicitStatus.cwd).toBe(explicitPath);

    const missingPath = join(harness.root, "does-not-exist");
    await harness.service.handleInbound(
      inbound({
        messageId: "new-missing",
        ...target,
        text: `/new ${missingPath}`,
      }),
    );
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

    await harness.service.handleInbound(
      inbound({ messageId: "new-reuse-explicit-cwd", ...target, text: "/new" }),
    );
    const reusedStatus = await harness.service.status(target);
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
    await harness.service.handleInbound(
      inbound({ messageId: "new-bind-before-run", ...target, text: "/new" }),
    );
    const firstTask = harness.service.handleInbound(
      inbound({ messageId: "new-running-task", ...target, text: "long work" }),
    );
    await harness.workers.waitForPromptCount(1);
    await harness.service.handleInbound(
      inbound({
        messageId: "new-while-running",
        ...target,
        text: `/new ${join(harness.root, "another")}`,
      }),
    );
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
