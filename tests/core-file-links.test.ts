import { expect, test } from "bun:test";
import { mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeTransport,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";
import type { CoreHarness } from "./fixtures/core-fakes.ts";

const GROUP: ChatTarget = { channelType: 2, channelId: "group-zed-links" };
const SSH_THREAD: ChatTarget = { channelType: 5, channelId: "group-zed-links____ssh" };
const PLAIN_THREAD: ChatTarget = { channelType: 5, channelId: "group-zed-links____plain" };
const LOCAL_DM: ChatTarget = { channelType: 1, channelId: "dm-zed-local" };
const OTHER_DM: ChatTarget = { channelType: 1, channelId: "dm-zed-other" };

function sameTarget(actual: ChatTarget, expected: ChatTarget): boolean {
  return actual.channelType === expected.channelType && actual.channelId === expected.channelId;
}

function incoming(harness: CoreHarness, target: ChatTarget, messageId: string, text: string) {
  return inbound({
    messageId,
    channelId: target.channelId,
    channelType: target.channelType,
    text,
    ...(target.channelType === 1 ? {} : { mentionedUserIds: [harness.bot.uid] }),
  });
}

async function bindCwd(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  cwd: string,
): Promise<void> {
  await mkdir(cwd, { recursive: true });
  await harness.service.submitInbound(incoming(harness, target, messageId, `/new ${cwd}`));
  await waitForStatus(harness.service, target, (status) => status.cwd === cwd);
}

async function completeTask(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  text: string,
): Promise<void> {
  await harness.service.submitInbound(incoming(harness, target, messageId, text));
  await waitForStatus(
    harness.service,
    target,
    (status) => status.state === "completed" && status.outbox === 0,
  );
}

test("channel SSH configuration resolves each thread against its own cwd", async () => {
  const transport = new FakeTransport();
  const partialSnapshot = "Notes: [meeting](notes/2026-11";
  const partialBody = "Notes: ";
  const rawBody = [
    "Notes: [meeting](notes/2026-11.md)",
    "Web: https://example.com/guide",
    "Inline: `[snippet](notes/inline.md)`",
    "```md",
    "[example](notes/example.md)",
    "```",
  ].join("\n");
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP.channelId, SSH_THREAD.channelId, PLAIN_THREAD.channelId],
    transport,
    fileLinks: {
      resolvers: [{ type: "zed", mode: "ssh", hostname: "my-mini", username: "dev", port: 2222 }],
    },
    prompt: async ({ text, emit }) => {
      if (text === "stream a Zed link") {
        emit(assistantText("assistant-ssh-link", partialSnapshot, false));
        await transport.waitForRemoteText(
          (message) => sameTarget(message.target, SSH_THREAD) && message.text === partialBody,
        );
        emit(assistantText("assistant-ssh-link", rawBody, true));
        return;
      }
      if (text === "same links in another thread") {
        emit(assistantText("assistant-plain-link", rawBody, true));
        return;
      }
      throw new Error(`unexpected prompt: ${text}`);
    },
  });

  try {
    const parentCwd = join(harness.root, "parent-workspace");
    const threadCwd = join(harness.root, "ssh-thread-workspace");
    const plainCwd = join(harness.root, "plain-thread-workspace");
    await bindCwd(harness, GROUP, "zed-parent-bind", parentCwd);
    await bindCwd(harness, SSH_THREAD, "zed-ssh-thread-bind", threadCwd);
    await bindCwd(harness, PLAIN_THREAD, "zed-plain-thread-bind", plainCwd);

    await completeTask(harness, SSH_THREAD, "zed-ssh-stream", "stream a Zed link");

    const expectedBody = [
      `Notes: [meeting](zed://ssh/dev@my-mini:2222${join(threadCwd, "notes/2026-11.md")})`,
      "Web: https://example.com/guide",
      "Inline: `[snippet](notes/inline.md)`",
      "```md",
      "[example](notes/example.md)",
      "```",
    ].join("\n");
    expect(harness.workers.prompts.find(({ text }) => text === "stream a Zed link")?.cwd)
      .toBe(threadCwd);
    expect(transport.listRemoteText(SSH_THREAD).filter(({ text }) => text === expectedBody))
      .toHaveLength(1);

    const threadRequests = transport.textRequests.filter(
      (request) => sameTarget(request.target, SSH_THREAD)
        && (request.text === partialBody || request.text === expectedBody),
    );
    expect(threadRequests.map(({ operation, text }) => ({ operation, text }))).toEqual([
      { operation: "send", text: partialBody },
      { operation: "edit", text: expectedBody },
    ]);
    expect(threadRequests[1]?.messageId).toBe(threadRequests[0]?.messageId);

    await completeTask(harness, PLAIN_THREAD, "zed-other-thread", "same links in another thread");
    expect(harness.workers.prompts.find(({ text }) => text === "same links in another thread")?.cwd)
      .toBe(plainCwd);
    expect(transport.listRemoteText(PLAIN_THREAD).filter(({ text }) =>
      text.includes(`zed://ssh/dev@my-mini:2222${join(plainCwd, "notes/2026-11.md")}`)))
      .toHaveLength(1);
    const store = await openChannelStore(harness.stateDir);
    try {
      const task = store.getLatestTask(harness.bot.uid, PLAIN_THREAD);
      if (!task) throw new Error("expected the task to be persisted");
      expect(store.getTaskFileLinkContext(task.id)).toEqual({
        taskId: task.id,
        cwd: plainCwd,
        resolvers: [{ type: "zed", mode: "ssh", hostname: "my-mini", username: "dev", port: 2222 }],
      });
    } finally {
      store.close();
    }
  } finally {
    await closeCoreHarness(harness);
  }
});

test("channel local configuration resolves each chat against its bound cwd", async () => {
  const target = LOCAL_DM;
  const other = OTHER_DM;
  const body = "Read [guide](docs/guide.md)";
  const harness = await createCoreHarness({
    fileLinks: { resolvers: [{ type: "zed", mode: "local" }] },
    prompt: async ({ text, emit }) => {
      if (text !== "show a local file") throw new Error(`unexpected prompt: ${text}`);
      emit(assistantText(`assistant-local-${text}`, body, true));
    },
  });

  try {
    const cwd = join(harness.root, "local-workspace");
    const otherCwd = join(harness.root, "other-workspace");
    await bindCwd(harness, target, "zed-local-bind", cwd);
    await bindCwd(harness, other, "zed-other-bind", otherCwd);

    await completeTask(harness, target, "zed-local-task", "show a local file");
    await completeTask(harness, other, "zed-other-task", "show a local file");

    expect(harness.workers.prompts.map(({ cwd: promptCwd }) => promptCwd)).toEqual([cwd, otherCwd]);
    expect(harness.transport.listRemoteText(target).filter(
      ({ text }) => text === `Read [guide](zed://file${join(cwd, "docs/guide.md")})`,
    )).toHaveLength(1);
    expect(harness.transport.listRemoteText(other).filter(
      ({ text }) => text === `Read [guide](zed://file${join(otherCwd, "docs/guide.md")})`))
      .toHaveLength(1);
  } finally {
    await closeCoreHarness(harness);
  }
});
test("a resolved link crossing the UTF-8 limit stays whole in one streamed message segment", async () => {
  const body = `${"x".repeat(2_990)} [guide](docs/cross-boundary.md)`;
  const harness = await createCoreHarness({
    fileLinks: { resolvers: [{ type: "zed", mode: "local" }] },
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-cross-boundary", body, true));
    },
  });

  try {
    const cwd = join(harness.root, "cross-boundary-workspace");
    await bindCwd(harness, LOCAL_DM, "zed-cross-boundary-bind", cwd);
    await completeTask(harness, LOCAL_DM, "zed-cross-boundary-task", "show a link at the segment edge");

    const resolvedLink = `[guide](zed://file${join(cwd, "docs/cross-boundary.md")})`;
    const segments = harness.transport.listRemoteText(LOCAL_DM)
      .map(({ text }) => text)
      .filter((text) => text.includes("x".repeat(100)) || text.includes("zed://file"));
    expect(segments).toEqual(["x".repeat(2_990) + " ", resolvedLink]);
    expect(segments.every((text) => Buffer.byteLength(text, "utf8") <= 3_000)).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a failed delivery after restart keeps the original task resolver snapshot and raw result", async () => {
  const transport = new FakeTransport({ online: false, startOnline: false });
  const rawBody = "Open [restart notes](docs/restart.md)";
  const first = await createCoreHarness({
    transport,
    fileLinks: { resolvers: [{
      type: "zed",
      mode: "ssh",
      hostname: "before-restart",
      username: "dev",
      port: 2200,
    }] },
    prompt: async ({ emit }) => {
      emit(assistantText("assistant-restart-links", rawBody, true));
    },
  });
  let restarted: CoreHarness | undefined;
  let firstClosed = false;
  let taskId = "";

  try {
    const cwd = join(first.root, "restart-link-workspace");
    await bindCwd(first, LOCAL_DM, "zed-restart-link-bind", cwd);
    await first.service.submitInbound(incoming(first, LOCAL_DM, "zed-restart-link-task", "show a link"));
    await waitForStatus(first.service, LOCAL_DM, (status) => status.state === "completed");

    const firstStore = await openChannelStore(first.stateDir);
    try {
      const task = firstStore.getLatestTask(first.bot.uid, LOCAL_DM);
      expect(task?.resultText).toBe(rawBody);
      if (!task) throw new Error("expected the completed task to be persisted");
      taskId = task.id;
      expect(firstStore.getTaskFileLinkContext(task.id)).toEqual({
        taskId: task.id,
        cwd,
        resolvers: [{
          type: "zed",
          mode: "ssh",
          hostname: "before-restart",
          username: "dev",
          port: 2200,
        }],
      });
    } finally {
      firstStore.close();
    }

    await closeCoreHarness(first, { remove: false });
    firstClosed = true;
    const restartedTransport = new FakeTransport({ bot: first.bot });
    const oldResolved = `Open [restart notes](zed://ssh/dev@before-restart:2200${join(cwd, "docs/restart.md")})`;
    restartedTransport.failOnceWhen(({ text }) => text === oldResolved);
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport: restartedTransport,
      fileLinks: { resolvers: [{
        type: "zed",
        mode: "ssh",
        hostname: "after-restart",
        username: "other",
      }] },
    });
    await waitForStatus(restarted.service, LOCAL_DM, (status) =>
      status.state === "completed" && status.outbox === 0,
    );

    expect(restartedTransport.attempts.filter(({ text }) => text === oldResolved).map(({ error }) => error))
      .toEqual(["fixture transport unavailable", undefined]);
    expect(restartedTransport.listRemoteText(LOCAL_DM).map(({ text }) => text))
      .toContain(oldResolved);
    expect(restartedTransport.listRemoteText(LOCAL_DM).map(({ text }) => text))
      .not.toContain(`Open [restart notes](zed://ssh/other@after-restart${join(cwd, "docs/restart.md")})`);
    const restartedStore = await openChannelStore(restarted.stateDir);
    try {
      expect(restartedStore.getTask(taskId)?.resultText).toBe(rawBody);
      expect(restartedStore.getTaskFileLinkContext(taskId)?.resolvers[0]?.hostname)
        .toBe("before-restart");
    } finally {
      restartedStore.close();
    }
  } finally {
    if (restarted !== undefined) await closeCoreHarness(restarted);
    else if (firstClosed) await rm(first.root, { recursive: true, force: true });
    else await closeCoreHarness(first);
  }
});
