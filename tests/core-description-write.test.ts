import { expect, test } from "bun:test";
import type { GroupContext, HostTool } from "../src/contracts.ts";
import { closeCoreHarness, createCoreHarness, FakeTransport, inbound, waitForStatus } from "./fixtures/core-fakes.ts";

const target = { channelType: 2 as const, channelId: "group-tools" };
const signal = new AbortController().signal;
const initial: GroupContext = { present: true, content: "Original description", version: 4, updatedAt: null, updatedBy: "owner-1" };

async function withManagement(run: (tool: HostTool, transport: FakeTransport) => Promise<void>) {
  let failure: unknown;
  const harness = await createCoreHarness({
    prompt: async ({ promptContext }) => {
      const tool = promptContext?.tools?.find(candidate => candidate.name === "octo_management");
      if (!tool) throw new Error("Missing scoped management tool");
      try { await run(tool, harness.transport); } catch (error) { failure = error; throw error; }
    },
  });
  harness.transport.setGroupContext(target, initial);
  try {
    await harness.service.handleInbound(inbound({ ...target, messageId: "description-guard", text: "maintain current description", mentionedUserIds: [harness.bot.uid] }));
    const status = await waitForStatus(harness.service, target, value => value.state === "completed" || value.state === "failed");
    if (failure) throw failure;
    expect(status.state).toBe("completed");
  } finally { await closeCoreHarness(harness); }
}

test("description-write requires an explicit successful tool read, not merely injected context", async () => {
  await withManagement(async (tool, transport) => {
    await expect(tool.execute({ action: "description-write", content: "unread replacement" }, signal)).rejects.toThrow(/description-read/);
    expect(await transport.getGroupContext(target)).toMatchObject({ content: "Original description", version: 4 });
  });
});

test.each([
  { content: "External changed description", version: 4 },
  { content: "Original description", version: 5 },
])("description-write rejects a read whose server content or version changed: %j", async change => {
  await withManagement(async (tool, transport) => {
    await tool.execute({ action: "description-read" }, signal);
    transport.setGroupContext(target, { ...initial, ...change });
    await expect(tool.execute({ action: "description-write", content: "stale replacement" }, signal)).rejects.toThrow(/description-read/);
    expect(await transport.getGroupContext(target)).toMatchObject(change);
    await expect(tool.execute({ action: "description-write", content: "retry without read" }, signal)).rejects.toThrow(/description-read/);
    await tool.execute({ action: "description-read" }, signal);
    await tool.execute({ action: "description-write", content: "fresh replacement" }, signal);
    expect(await transport.getGroupContext(target)).toMatchObject({ content: "fresh replacement", version: change.version + 1 });
  });
});

test("each successful description write consumes its prior read", async () => {
  await withManagement(async (tool, transport) => {
    await tool.execute({ action: "description-read" }, signal);
    await tool.execute({ action: "description-write", content: "first replacement" }, signal);
    await expect(tool.execute({ action: "description-write", content: "second replacement without read" }, signal)).rejects.toThrow(/description-read/);
    expect(await transport.getGroupContext(target)).toMatchObject({ content: "first replacement", version: 5 });
  });
});

test("parallel description writes cannot reuse the same read to overwrite each other", async () => {
  await withManagement(async (tool, transport) => {
    await tool.execute({ action: "description-read" }, signal);
    const results = await Promise.allSettled([
      tool.execute({ action: "description-write", content: "first parallel writer" }, signal),
      tool.execute({ action: "description-write", content: "second parallel writer" }, signal),
    ]);
    expect(results[0]?.status).toBe("fulfilled");
    expect(results[1]?.status).toBe("rejected");
    expect(await transport.getGroupContext(target)).toMatchObject({ content: "first parallel writer", version: 5 });
  });
});
