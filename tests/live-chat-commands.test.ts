import { expect, test } from "bun:test";
import type { ChannelService } from "../src/contracts.ts";
import { FakeWorkerFactory, inbound } from "./fixtures/core-fakes.ts";
import { createCommandCatalogHarness, createCommandWorkerFactory } from "./command-catalog-test-seam.ts";

test("command requests read the exact running worker without starting another session", async () => {
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const workers = new FakeWorkerFactory(async () => {
    started.resolve();
    await release.promise;
  });
  let current = "first-live-command";
  const factory = createCommandWorkerFactory(workers, async () => [{ name: current, source: "file" }]);
  const harness = await createCommandCatalogHarness({ workerFactory: factory });
  const target = { channelType: 1, channelId: "owner-1" } as const;
  const service = harness.service as ChannelService;
  try {
    await service.submitInbound(inbound({ ...target, senderId: "owner-1", messageId: "live-catalog", text: "hold" }));
    await started.promise;
    const first = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    current = "updated-live-command";
    const second = await harness.service.chatCommandsSnapshot(target, new AbortController().signal);
    expect(first?.commands.some(command => command.name === "first-live-command")).toBe(true);
    expect(second?.commands.some(command => command.name === "updated-live-command")).toBe(true);
    expect(workers.workers).toHaveLength(1);
    expect(workers.workers[0]?.stopped).toBe(false);
  } finally {
    release.resolve();
    await harness.close();
  }
});
