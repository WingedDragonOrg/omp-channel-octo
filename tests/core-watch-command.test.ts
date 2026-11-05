import { expect, test } from "bun:test";
import { closeCoreHarness, createCoreHarness, inbound } from "./fixtures/core-fakes.ts";

// Routing /watch through OMP would create work when the owner only requests local watch state.
test("owner watch list stays on the host control path without starting a task", async () => {
  const harness = await createCoreHarness();
  const message = inbound({ messageId: "watch-list", text: "/watch list" });
  try {
    const reply = harness.transport.waitForRemoteText(sent =>
      sent.target.channelId === message.channelId
      && sent.target.channelType === message.channelType
      && /watch|监控|观察/u.test(sent.text));
    await harness.service.submitInbound(message);
    await reply;
    await harness.service.stop();
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(row => /watch|监控|观察/u.test(row.text))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
