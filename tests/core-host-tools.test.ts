import { expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
} from "./fixtures/core-fakes";

test("group dispatch freezes scoped read/write/file tools to the current target and cwd", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, cwd, promptContext }) => {
      const tools = promptContext?.tools ?? [];
      const names = tools.map(({ name }) => name).sort();
      if (names.join(",") !== "ask,octo_group_read,octo_group_write,octo_send_file") {
        throw new Error(`unexpected host tools: ${names.join(",")}`);
      }
      const readTool = tools.find(({ name }) => name === "octo_group_read");
      const writeTool = tools.find(({ name }) => name === "octo_group_write");
      const fileTool = tools.find(({ name }) => name === "octo_send_file");
      if (!readTool || !writeTool || !fileTool) {
        throw new Error("required host tool missing");
      }
      const signal = new AbortController().signal;
      const before = await readTool.execute({}, signal);
      const writeResult = await writeTool.execute({ content: "updated by worker" }, signal);
      const fileResult = await fileTool.execute(
        { path: "picture.png", kind: "image" },
        signal,
      );
      await append("tool-effects.log", `${before}|${writeResult}|${fileResult}|${cwd}\n`);
    },
  });
  try {
    const target = { channelId: "group-tools", channelType: 2 as const };
    const cwd = join(harness.root, "tool cwd");
    await mkdir(cwd, { recursive: true });
    await writeFile(join(cwd, "picture.png"), "fixture image bytes", "utf8");
    harness.transport.setGroupContext(target, {
      content: "initial group context",
      version: 4,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "group-admin",
    });
    await harness.service.handleInbound(
      inbound({ messageId: "tools-bind", ...target, text: `/new ${cwd}`, mentionedUserIds: [harness.bot.uid] }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "tools-work",
        ...target,
        text: "use the group tools",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");

    const updated = await harness.transport.getGroupContext(target);
    expect(updated?.content).toBe("updated by worker");
    expect(updated?.version).toBe(5);
    expect(harness.transport.sentFiles).toHaveLength(1);
    expect(harness.transport.sentFiles[0]).toMatchObject({
      target,
      kind: "image",
      clientMsgNo: expect.any(String),
    });
    expect(harness.transport.sentFiles[0]?.path).toContain(join(harness.stateDir, "spool"));
    expect(harness.transport.sentFiles[0]?.path).toMatch(/picture\.png$/u);
    const effects = await readText(join(cwd, "tool-effects.log"));
    expect(effects).toContain("initial group context");
    expect(effects).toContain(cwd);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("private prompts receive ask and file tools but no group context tools", async () => {
  const harness = await createCoreHarness({
    prompt: async ({ append, promptContext, text }) => {
      const names = (promptContext?.tools ?? []).map(({ name }) => name).sort();
      if (names.join(",") !== "ask,octo_send_file") {
        throw new Error(`unexpected private host tools: ${names.join(",")}`);
      }
      if (names.includes("octo_group_read") || names.includes("octo_group_write")) {
        throw new Error("private prompt unexpectedly received group tools");
      }
      await append("private-tools.log", text);
    },
  });
  try {
    const target = { channelId: "dm-no-tools", channelType: 1 as const };
    harness.transport.setGroupContext(target, {
      content: "private must not read this",
      version: 1,
      updatedAt: null,
      updatedBy: "group-admin",
    });
    await harness.service.handleInbound(
      inbound({ messageId: "private-no-tools", ...target, text: "private prompt" }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, (status) => status.state === "completed");
    expect(harness.transport.groupContextCalls).toHaveLength(0);
    expect(await readText(join(harness.root, "tmp", "private-tools.log"))).toBe(
      "private prompt",
    );
  } finally {
    await closeCoreHarness(harness);
  }
});
