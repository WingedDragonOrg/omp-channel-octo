import { expect, test } from "bun:test";
import { mkdir } from "node:fs/promises";
import { join, sep } from "node:path";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import type { ChatTarget } from "../src/contracts.ts";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  readText,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";

const TASK_TIMEOUT_MS = 4_000;

function deferred() {
  const { promise, resolve } = Promise.withResolvers<void>();
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>, description: string): Promise<T> {
  const { promise: timeout, reject } = Promise.withResolvers<T>();
  const timer = setTimeout(
    () => reject(new Error(`timed out waiting for ${description}`)),
    TASK_TIMEOUT_MS,
  );
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function nextEventLoopTurn(): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setImmediate(resolve);
  await promise;
}

async function waitForReplies(
  harness: CoreHarness,
  target: ChatTarget,
  text: string,
): Promise<void> {
  const message = await within(
    harness.transport.waitForRemoteText(({ target: sentTarget, text: sentText }) =>
      sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && sentText === text,
    ),
    `reply ${text} in ${target.channelId}`,
  );
  expect(message.target).toMatchObject(target);
  expect(message.text).toBe(text);
}

async function waitForTerminalTasks(
  harness: CoreHarness,
  targets: readonly ChatTarget[],
): Promise<void> {
  await Promise.all(targets.map(async (target) => {
    await within(
      waitForStatus(harness.service, target, (status) =>
        status.state === "completed"
        || status.state === "failed"
        || status.state === "cancelled"
        || status.state === "interrupted"),
      `task cleanup in ${target.channelId}`,
    ).catch(() => undefined);
  }));
}

test("different sessions in the same normalized directory run concurrently", async () => {
  const firstStarted = deferred();
  const releaseFirst = deferred();
  const secondFinished = deferred();
  let firstFinished = false;
  const harness = await createCoreHarness({
    prompt: async ({ append, emit, text }) => {
      const label = text === "first task" ? "first" : "second";
      await append("order.log", `start:${label}\n`);
      if (label === "first") {
        firstStarted.resolve();
        await releaseFirst.promise;
      }
      await append("order.log", `end:${label}\n`);
      emit(assistantText(`assistant-${label}`, `reply:${label}`, true));
      if (label === "first") firstFinished = true;
      else secondFinished.resolve();
    },
  });
  const firstTarget = { channelId: "dm-first", channelType: 1 as const };
  const secondTarget = { channelId: "dm-second", channelType: 1 as const };
  const submitted: ChatTarget[] = [];
  try {
    const sharedDirectory = join(harness.root, "shared workdir");
    await mkdir(sharedDirectory, { recursive: true });
    await mkdir(join(harness.root, "directory alias"), { recursive: true });
    const equivalentDirectory =
      `${join(harness.root, "directory alias")}${sep}..${sep}shared workdir`;
    await harness.service.handleInbound(
      inbound({ messageId: "bind-first", ...firstTarget, text: `/new ${sharedDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "bind-second", ...secondTarget, text: `/new ${equivalentDirectory}` }),
    );

    await harness.service.handleInbound(
      inbound({ messageId: "work-first", ...firstTarget, text: "first task" }),
    );
    submitted.push(firstTarget);
    await within(firstStarted.promise, "first task to start");
    await harness.service.handleInbound(
      inbound({ messageId: "work-second", ...secondTarget, text: "second task" }),
    );
    submitted.push(secondTarget);

    await within(secondFinished.promise, "second task to finish while the first is held");
    await within(
      waitForStatus(harness.service, secondTarget, (status) => status.state === "completed"),
      "second chat task to complete while the first is held",
    );
    expect(firstFinished).toBe(false);
    expect(await readText(join(sharedDirectory, "order.log"))).toBe(
      "start:first\nstart:second\nend:second\n",
    );
    await waitForReplies(harness, secondTarget, "reply:second");
    expect(
      harness.transport.listRemoteText(firstTarget).some(({ text }) => text === "reply:second"),
    ).toBe(false);

    releaseFirst.resolve();
    await within(
      waitForStatus(harness.service, firstTarget, (status) => status.state === "completed"),
      "first chat task to complete",
    );
    await waitForReplies(harness, firstTarget, "reply:first");
    const [firstPrompt, secondPrompt] = harness.workers.prompts;
    expect(firstPrompt?.sessionId).toBeDefined();
    expect(secondPrompt?.sessionId).toBeDefined();
    expect(secondPrompt?.sessionId).not.toBe(firstPrompt?.sessionId);
    expect(
      harness.transport.listRemoteText(secondTarget).some(({ text }) => text === "reply:first"),
    ).toBe(false);
    expect(await readText(join(sharedDirectory, "order.log"))).toBe(
      "start:first\nstart:second\nend:second\nend:first\n",
    );
  } finally {
    releaseFirst.resolve();
    await waitForTerminalTasks(harness, submitted);
    await closeCoreHarness(harness);
  }
});

// A workspace ownership record must not turn independent sessions into directory writers.
for (const scenario of [
  { name: "parent runs before child", first: "parent", second: "api", parentFirst: true },
  { name: "child runs before parent", first: "api", second: "parent", parentFirst: false },
  { name: "sibling projects", first: "api", second: "web", parentFirst: false },
  { name: "shared entry and project", first: "entry", second: "api", parentFirst: false },
  { name: "same owned cwd", first: "api", second: "api", parentFirst: false },
  { name: "project and descendant", first: "api", second: "src", parentFirst: false },
] as const) {
  test(`independent workspace sessions finish concurrently: ${scenario.name}`, async () => {
    const started = deferred();
    const release = deferred();
    const secondFinished = deferred();
    const harness = await createCoreHarness({
      prompt: async ({ append, emit, text }) => {
        if (text === "held work") {
          await append("effects.log", "held:start\n");
          started.resolve();
          await release.promise;
          await append("effects.log", "held:end\n");
        } else {
          await append("effects.log", "independent:done\n");
          emit(assistantText("independent-reply", "independent completed", true));
          secondFinished.resolve();
        }
      },
    });
    const parent = { channelType: 2 as const, channelId: "group-tools" };
    const child = { channelType: 5 as const, channelId: "group-tools____owned" };
    const sibling = { channelType: 5 as const, channelId: "group-tools____other" };
    const first = scenario.parentFirst ? parent : child;
    const second = scenario.second === "parent" ? parent : sibling;
    const paths = {
      parent: join(harness.root, "repos"),
      entry: join(harness.root, "repos", "shared"),
      api: join(harness.root, "repos", "shared", "api"),
      web: join(harness.root, "repos", "shared", "web"),
      src: join(harness.root, "repos", "shared", "api", "src"),
    };
    let store: ChannelStore | undefined;
    const message = (target: ChatTarget, messageId: string, text: string) =>
      inbound({ ...target, messageId, text, mentionedUserIds: [harness.bot.uid] });
    try {
      await mkdir(paths.src, { recursive: true });
      await mkdir(paths.web, { recursive: true });
      await harness.service.handleInbound(message(first, "bind-held", `/new ${paths[scenario.first]}`));
      await harness.service.handleInbound(message(second, "bind-independent", `/new ${paths[scenario.second]}`));
      store = await openChannelStore(harness.stateDir);
      store.saveThreadWorkspace({
        botUid: harness.bot.uid,
        ...child,
        layout: "shared",
        status: "ready",
        entryPath: paths.entry,
        projects: [
          { name: "api", repo: paths.parent, path: paths.api, branch: "api", created: true },
          { name: "web", repo: paths.parent, path: paths.web, branch: "web", created: true },
        ],
        error: null,
      });
      await harness.service.handleInbound(message(first, "held-task", "held work"));
      await within(started.promise, "held worker prompt");
      await harness.service.handleInbound(message(second, "independent-task", "independent work"));
      await within(secondFinished.promise, "independent worker effects before held worker exits");
      await within(
        waitForStatus(harness.service, second, status => status.state === "completed"),
        "independent completion",
      );
      expect(await readText(join(paths[scenario.second], "effects.log"))).toContain("independent:done\n");
      expect(await readText(join(paths[scenario.first], "effects.log"))).not.toContain("held:end");
      await waitForReplies(harness, second, "independent completed");
      expect(harness.workers.prompts[0]?.sessionId).not.toBe(harness.workers.prompts[1]?.sessionId);
    } finally {
      release.resolve();
      await waitForTerminalTasks(harness, [first, second]);
      store?.close();
      await closeCoreHarness(harness);
    }
  });
}

test("tasks in one chat remain queued in arrival order", async () => {
  const firstStarted = deferred();
  const releaseFirst = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, emit, text }) => {
      const label = text === "first task" ? "first" : "second";
      await append("order.log", `start:${label}\n`);
      if (label === "first") {
        firstStarted.resolve();
        await releaseFirst.promise;
      }
      await append("order.log", `end:${label}\n`);
      emit(assistantText(`assistant-${label}`, `reply:${label}`, true));
    },
  });
  const target = { channelId: "dm-queued", channelType: 1 as const };
  const submitted: ChatTarget[] = [];
  try {
    const sharedDirectory = join(harness.root, "shared workdir");
    await mkdir(sharedDirectory, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "queue-bind", ...target, text: `/new ${sharedDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({ messageId: "queue-mode", ...target, text: "/mode queue" }),
    );

    await harness.service.handleInbound(
      inbound({ messageId: "queue-first", ...target, text: "first task" }),
    );
    submitted.push(target);
    await within(firstStarted.promise, "first queued-chat task to start");
    await harness.service.handleInbound(
      inbound({ messageId: "queue-second", ...target, text: "second task" }),
    );
    submitted.push(target);

    expect(harness.workers.prompts.map(({ text }) => text)).toEqual(["first task"]);
    releaseFirst.resolve();
    await within(
      waitForStatus(harness.service, target, (status) => status.state === "completed"),
      "queued-chat tasks to complete",
    );
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "first task",
      "second task",
    ]);
    expect(harness.workers.prompts[1]?.sessionId).toBe(harness.workers.prompts[0]?.sessionId);
    expect(await readText(join(sharedDirectory, "order.log"))).toBe(
      "start:first\nend:first\nstart:second\nend:second\n",
    );
  } finally {
    releaseFirst.resolve();
    await waitForTerminalTasks(harness, submitted);
    await closeCoreHarness(harness);
  }
});

test("different chats bound to the same session do not overlap", async () => {
  const firstStarted = deferred();
  const releaseFirst = deferred();
  const harness = await createCoreHarness({
    prompt: async ({ append, text }) => {
      if (text === "prime shared session") return;
      const label = text === "first shared-session task" ? "first" : "second";
      await append("shared-session-order.log", `start:${label}\n`);
      if (label === "first") {
        firstStarted.resolve();
        await releaseFirst.promise;
      }
      await append("shared-session-order.log", `end:${label}\n`);
    },
  });
  const firstTarget = { channelId: "dm-session-first", channelType: 1 as const };
  const secondTarget = { channelId: "dm-session-second", channelType: 1 as const };
  const submitted: ChatTarget[] = [];
  let store: ChannelStore | undefined;
  try {
    const sharedDirectory = join(harness.root, "shared session workdir");
    await mkdir(sharedDirectory, { recursive: true });
    await harness.service.handleInbound(
      inbound({ messageId: "session-bind", ...firstTarget, text: `/new ${sharedDirectory}` }),
    );
    await harness.service.handleInbound(
      inbound({
        messageId: "session-prime",
        ...firstTarget,
        text: "prime shared session",
      }),
    );
    await within(
      waitForStatus(harness.service, firstTarget, (status) => status.state === "completed"),
      "shared-session setup task to complete",
    );

    store = await openChannelStore(harness.stateDir);
    const firstBinding = store.getBinding(harness.bot.uid, firstTarget);
    if (firstBinding?.sessionFile === null || firstBinding === null) {
      throw new Error("setup task did not persist its OMP session file");
    }
    store.ensureBinding({
      ...firstBinding,
      channelId: secondTarget.channelId,
      channelType: secondTarget.channelType,
      sessionSwitchedAt: null,
    });
    expect(store.getBinding(harness.bot.uid, secondTarget)).toMatchObject({
      sessionId: firstBinding.sessionId,
      cwd: firstBinding.cwd,
      sessionFile: firstBinding.sessionFile,
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "shared-session-first",
        ...firstTarget,
        text: "first shared-session task",
      }),
    );
    submitted.push(firstTarget);
    await within(firstStarted.promise, "first shared-session task to start");
    await harness.service.handleInbound(
      inbound({
        messageId: "shared-session-second",
        ...secondTarget,
        text: "second shared-session task",
      }),
    );
    submitted.push(secondTarget);

    for (let turn = 0; turn < 8; turn += 1) await nextEventLoopTurn();
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "prime shared session",
      "first shared-session task",
    ]);
    expect(await harness.service.status(secondTarget)).toMatchObject({ state: "queued" });

    releaseFirst.resolve();
    await within(
      waitForStatus(harness.service, firstTarget, (status) => status.state === "completed"),
      "first shared-session task to complete",
    );
    await within(
      waitForStatus(harness.service, secondTarget, (status) => status.state === "completed"),
      "second shared-session task to complete",
    );
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "prime shared session",
      "first shared-session task",
      "second shared-session task",
    ]);
    expect(harness.workers.prompts[2]?.sessionId).toBe(harness.workers.prompts[1]?.sessionId);
    expect(await readText(join(sharedDirectory, "shared-session-order.log"))).toBe(
      "start:first\nend:first\nstart:second\nend:second\n",
    );
  } finally {
    releaseFirst.resolve();
    await waitForTerminalTasks(harness, submitted);
    store?.close();
    await closeCoreHarness(harness);
  }
});
