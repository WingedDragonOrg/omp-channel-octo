import { expect, test } from "bun:test";
import { closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes.ts";

// Routing /watch through OMP would create work when the owner only requests local watch state.
test("owner watch list stays on the host control path without starting a task", async () => {
  const harness = await createCoreHarness();
  const message = inbound({ messageId: "watch-list", text: "/watch list" });
  try {
    await harness.service.handleInbound(message);
    await waitForStatus(harness.service, message, status => status.state !== "running" && status.state !== "queued");
    await harness.service.stop();
    expect(harness.workers.prompts).toEqual([]);
    expect(harness.transport.sent.some(row => /watch|监控|观察/u.test(row.text))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});
