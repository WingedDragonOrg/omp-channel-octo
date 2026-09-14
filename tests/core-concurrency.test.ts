import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

test("tasks bound to the same normalized directory never overlap", async () => {
  let releaseFirst!: () => void;
  let firstStarted!: () => void;
  const firstStartedSignal = new Promise<void>((resolve) => {
    firstStarted = resolve;
  });
  const firstReleaseSignal = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      const label = text.includes("first") ? "first" : "second";
      await append("order.log", `start:${label}\n`);
      if (label === "first") {
        firstStarted();
        await firstReleaseSignal;
      }
      await append("order.log", `end:${label}\n`);
    },
  });
  try {
    const sharedDirectory = join(harness.root, "shared workdir");
    await mkdir(sharedDirectory, { recursive: true });
    const firstTarget = { channelId: "dm-first", channelType: 1 as const };
    const secondTarget = { channelId: "dm-second", channelType: 1 as const };
    await harness.service.handleInbound(
      inbound({ messageId: "bind-first", ...firstTarget, text: `/new ${sharedDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "bind-second", ...secondTarget, text: `/new ${sharedDirectory}` }),
    );

    await harness.service.handleInbound(
      inbound({ messageId: "work-first", ...firstTarget, text: "first" }),
    );
    await firstStartedSignal;
    await harness.service.handleInbound(
      inbound({ messageId: "work-second", ...secondTarget, text: "second" }),
    );
    expect(harness.workers.prompts).toHaveLength(1);

    releaseFirst();
    await waitForStatus(
      harness.service,
      firstTarget,
      (status) => status.state === "completed",
    );
    await waitForStatus(
      harness.service,
      secondTarget,
      (status) => status.state === "completed",
    );

    const order = (await readText(join(sharedDirectory, "order.log")))
      .trim()
      .split("\n");
    expect(order).toEqual(["start:first", "end:first", "start:second", "end:second"]);
  } finally {
    releaseFirst();
    await closeCoreHarness(harness);
  }
});
