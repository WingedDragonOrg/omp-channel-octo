import { expect, test } from "bun:test";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

test("durable inbound dedupe prevents a duplicate side effect after restart", async () => {
  const first = await createCoreHarness();
  const message = inbound({
    messageId: "durable-message",
    channelId: "dm-durable",
    channelType: 1,
    text: "write exactly once",
  });
  try {
    await first.service.handleInbound(message);
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, message, (status) => status.state === "completed");
    const firstEffects = await readText(join(first.root, "tmp", "effects.log"));
    expect(firstEffects.trim().split("\n")).toEqual(["write exactly once"]);
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await restarted.service.handleInbound(message);
    const status = await restarted.service.status(message);
    expect(status.state).toBe("completed");
    expect(restarted.workers.prompts).toHaveLength(0);
    expect((await readText(join(first.root, "tmp", "effects.log"))).trim().split("\n")).toEqual([
      "write exactly once",
    ]);
  } finally {
    await closeCoreHarness(restarted);
  }
});

test("a command with the same durable message id is not applied again after restart", async () => {
  const first = await createCoreHarness();
  const target = { channelId: "dm-command-durable", channelType: 1 as const };
  const command = inbound({
    messageId: "durable-new-command",
    ...target,
    text: "/new",
  });
  let originalSession: string | null = null;
  try {
    await first.service.handleInbound(command);
    originalSession = (await first.service.status(target)).sessionId;
  } finally {
    await closeCoreHarness(first, { remove: false });
  }

  const restarted = await createCoreHarness({ root: first.root, bot: first.bot });
  try {
    await restarted.service.handleInbound(command);
    const status = await restarted.service.status(target);
    expect(status.sessionId).toBe(originalSession);
    expect(status.cwd).toBe(join(first.root, "tmp"));
  } finally {
    await closeCoreHarness(restarted);
  }
});
