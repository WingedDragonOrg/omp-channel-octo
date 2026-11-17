import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import type {
  WatchSource,
  WatchSourceCursor,
  WatchSourceEvent,
  WatchSourceFactory,
  WatchSourceObservation,
  WatchSourceResource,
  WatchSourceSnapshot,
} from "../src/watch/source-types.ts";
import type { ChatTarget, PromptContext } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarnessOptions,
} from "./fixtures/core-fakes.ts";


const target = { channelType: 2 as const, channelId: "group-allowed" };
const WATCH_CONFIG = {
  enabled: true as const,
  protocol: "openai-decisions" as const,
  baseUrl: "https://watch.example",
  model: "watch-fixture",
  apiKeyEnv: "OMP_CHANNEL_OCTO_WATCH_KEY",
  apiKey: "fixture-watch-key",
};
const consumeCondition = "Consume a new comment or review when it adds a concrete review criterion.";
const endCondition = "The subscribed request has been merged.";
const taskScope = "Continue only the original review task; do not modify files or expand its scope.";

type FixturePoll = Extract<WatchSourceObservation, { kind: "success" }>;

interface FixtureWatchSourceState {
  source: WatchSource;
  publish(events: WatchSourceEvent[], nextSnapshot?: WatchSourceSnapshot): void;
  readonly initializeCalls: number;
  readonly pollCalls: number;
}

function createFixtureWatchSource(): FixtureWatchSourceState {
  const versions: WatchSourceCursor["versions"] = {
    comments: {},
    reviews: {},
    notes: {},
  };
  const queued: FixturePoll[] = [];
  let initializeCalls = 0;
  let pollCalls = 0;
  const waiters = new Set<{ finish(batch: FixturePoll): void }>();
  let currentSnapshot: WatchSourceSnapshot = {
    state: "open",
    merged: false,
    title: "Fixture pull request",
    headSha: "fixture-head",
  };
  const snapshot = (): WatchSourceSnapshot => structuredClone(currentSnapshot);
  const cursor = (): WatchSourceCursor => ({
    schema: 1,
    resource: "fixture://source",
    baselineStartedAt: 0,
    transitionSequence: 0,
    snapshot: snapshot(),
    versions: structuredClone(versions),
  });
  const emptyBatch = (): FixturePoll => ({
    kind: "success",
    cursor: cursor(),
    snapshot: snapshot(),
    events: [],
  });

  return {
    source: {
      async initialize(_signal) {
        initializeCalls += 1;
        return emptyBatch();
      },
      poll(_cursor, signal) {
        pollCalls += 1;
        const batch = queued.shift();
        if (batch) return Promise.resolve(batch);
        if (signal.aborted) return Promise.resolve(emptyBatch());
        const pending = Promise.withResolvers<FixturePoll>();
        const waiter = {
          finish(result: FixturePoll) {
            signal.removeEventListener("abort", onAbort);
            waiters.delete(waiter);
            pending.resolve(result);
          },
        };
        const onAbort = () => waiter.finish(emptyBatch());
        waiters.add(waiter);
        signal.addEventListener("abort", onAbort, { once: true });
        if (signal.aborted) onAbort();
        return pending.promise;
      },
    },
    get initializeCalls() {
      return initializeCalls;
    },
    get pollCalls() {
      return pollCalls;
    },
    publish(events, nextSnapshot) {
      if (nextSnapshot) currentSnapshot = structuredClone(nextSnapshot);
      for (const event of events) {
        const collection = event.type === "comment"
          ? versions.comments
          : event.type === "review"
            ? versions.reviews
            : event.type === "note"
              ? versions.notes
              : null;
        if (collection) collection[event.identity] = event.version;
      }
      const batch: FixturePoll = {
        kind: "success",
        cursor: cursor(),
        snapshot: snapshot(),
        events,
      };
      const waiter = waiters.values().next().value as { finish(batch: FixturePoll): void } | undefined;
      if (waiter) waiter.finish(batch);
      else queued.push(batch);
    },
  };
}

type FixtureSourceFactory = WatchSourceFactory;
type SourceHarnessOptions = Omit<CoreHarnessOptions, "watchSourceFactory"> & {
  watchSourceFactory: FixtureSourceFactory;
};

function createSourceHarness(options: SourceHarnessOptions) {
  return createCoreHarness(options);
}

type WatchTool = NonNullable<PromptContext["tools"]>[number];

function watchTool(promptContext: PromptContext | undefined): WatchTool {
  const tool = promptContext?.tools?.find(entry => entry.name === "watch");
  if (!tool) throw new Error("watch host tool unavailable");
  return tool;
}

function sourceCreateArgs(
  resource: string,
  eventTypes: string[],
  timeout = 60,
  decisionConsent = true,
) {
  return {
    action: "create",
    resource,
    eventTypes,
    consumeCondition,
    endCondition,
    decisionConsent,
    timeout,
    taskScope,
  };
}

async function eventually<T>(read: () => T | null | undefined | false, label = "source watch integration"): Promise<T> {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    const value = read();
    if (value) return value;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`${label} did not settle`);
}

function relevantQueuedWake(store: ChannelStore, botUid: string, chat: ChatTarget) {
  return store.listQueuedTasks().find(task =>
    task.botUid === botUid &&
    task.channelType === chat.channelType &&
    task.channelId === chat.channelId &&
    task.messageId.startsWith("watch:"),
  );
}

function waitForTaskState(store: ChannelStore, taskId: string, state: string, label?: string): Promise<unknown> {
  return eventually(() => store.getTask(taskId)?.state === state ? true : undefined, label);
}

const githubResource = "github://can1357/oh-my-pi/pull/19544";
const gitlabResource = "gitlab://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70";

test("source watch wakes independently for two events and host tools manage both GitHub and GitLab subscriptions", async () => {
  const githubSource = createFixtureWatchSource();
  const gitlabSource = createFixtureWatchSource();
  const factoryCalls: WatchSourceResource[] = [];
  const firstWakeStarted = Promise.withResolvers<void>();
  const releaseFirstWake = Promise.withResolvers<void>();
  const wakeTexts: string[] = [];
  let createResults: { githubId: string; gitlabId: string; discovery: unknown; list: unknown; github: unknown; gitlab: unknown; stopped: unknown } | undefined;
  const sourceText = "持续关注此 PR 与 MR 的新评论和 review，并在相关事件时继续当前审查。授权只把对应事件内容发给已配置的 Decisions 模型。";
  const harness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: resource => {
      factoryCalls.push(resource);
      if (resource.provider === "github" && resource.owner === "can1357" &&
          resource.repo === "oh-my-pi" && resource.number === 19544) return githubSource.source;
      if (resource.provider === "gitlab" && resource.host === "codex.mlamp.cn" &&
          resource.projectPath === "dmwork/octo-marketplace" && resource.iid === 70) return gitlabSource.source;
      throw new Error(`unexpected fixture resource: ${JSON.stringify(resource)}`);
    },
    watchJudge: async ({ condition }) => ({
      kind: "judgment",
      choice: condition === consumeCondition ? "satisfied" : "not_satisfied",
      probability: 0.99,
    }),
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) {
        wakeTexts.push(text);
        if (wakeTexts.length === 1) {
          firstWakeStarted.resolve();
          await releaseFirstWake.promise;
        }
        return;
      }

      const tool = watchTool(promptContext);
      const signal = new AbortController().signal;
      const discovery = JSON.parse(await tool.execute({ action: "discover" }, signal));
      const github = JSON.parse(await tool.execute(
        sourceCreateArgs(githubResource, ["comment", "review"]), signal, "github-source-watch-create",
      )) as { id: string };
      const gitlab = JSON.parse(await tool.execute(
        sourceCreateArgs(gitlabResource, ["note"]), signal, "gitlab-source-watch-create",
      )) as { id: string };
      const list = JSON.parse(await tool.execute({ action: "list" }, signal)) as { watches: Array<{ id: string }> };
      const githubDetail = JSON.parse(await tool.execute({ action: "get", id: github.id }, signal));
      const gitlabDetail = JSON.parse(await tool.execute({ action: "get", id: gitlab.id }, signal));
      const stopped = JSON.parse(await tool.execute({ action: "stop", id: gitlab.id }, signal));
      createResults = {
        githubId: github.id,
        gitlabId: gitlab.id,
        discovery,
        list,
        github: githubDetail,
        gitlab: gitlabDetail,
        stopped,
      };
    },
  });
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "source-watch-create",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    expect(createResults).toBeDefined();
    expect((createResults!.list as { watches: Array<{ id: string }> }).watches.map(watch => watch.id))
      .toEqual(expect.arrayContaining([createResults!.githubId, createResults!.gitlabId]));
    expect(JSON.stringify(createResults!.github)).toContain(githubResource);
    expect(JSON.stringify(createResults!.gitlab)).toContain(gitlabResource);
    expect(JSON.stringify(createResults!.stopped)).toContain(createResults!.gitlabId);
    expect(factoryCalls).toEqual(expect.arrayContaining([
      { provider: "github", host: "github.com", owner: "can1357", repo: "oh-my-pi", number: 19544 },
      { provider: "gitlab", host: "codex.mlamp.cn", projectPath: "dmwork/octo-marketplace", iid: 70 },
    ]));

    store = await openChannelStore(harness.stateDir);
    githubSource.publish([{
      identity: "github-comment-101",
      type: "comment",
      version: "1",
      body: { text: "first unique review criterion" },
    }]);
    await firstWakeStarted.promise;
    githubSource.publish([{
      identity: "github-review-202",
      type: "review",
      version: "1",
      body: { text: "second unique review criterion" },
    }]);
    await eventually(() => store!.countQueuedTasks(harness.bot.uid, target) > 0 ? true : undefined);
    releaseFirstWake.resolve();
    await harness.workers.waitForPromptCount(3);

    expect(wakeTexts).toHaveLength(2);
    expect(wakeTexts[0]).toContain("first unique review criterion");
    expect(wakeTexts[1]).toContain("second unique review criterion");
    expect(store.getSourceWatch(harness.bot.uid, createResults!.githubId)?.state).toBe("watching");
  } finally {
    releaseFirstWake.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

for (const boundary of ["stop", "deadline"] as const) {
  test(`source watch ${boundary} prevents a queued event from reaching its first prompt`, async () => {
    const source = createFixtureWatchSource();
    const busy = Promise.withResolvers<void>();
    const busyStarted = Promise.withResolvers<void>();
    const sourceText = `authorize source watch before ${boundary}`;
    const busyText = "hold the chat queue before the watch wake";
    const watchCreated = Promise.withResolvers<{ id: string; tool: WatchTool }>();
    const unexpectedWakeTexts: string[] = [];
    const harness = await createSourceHarness({
      decisions: WATCH_CONFIG,
      watchSourceFactory: () => source.source,
      watchJudge: async ({ condition }) => ({
        kind: "judgment",
        choice: condition === consumeCondition ? "satisfied" : "not_satisfied",
        probability: 0.99,
      }),
      prompt: async ({ text, promptContext }) => {
        if (text === sourceText) {
          const tool = watchTool(promptContext);
          const created = JSON.parse(await tool.execute(
            sourceCreateArgs(githubResource, ["comment"], 60),
            new AbortController().signal,
            `${boundary}-source-watch-create`,
          )) as { id: string };
          watchCreated.resolve({ id: created.id, tool });
        } else if (text === busyText) {
          busyStarted.resolve();
          await busy.promise;
        } else {
          unexpectedWakeTexts.push(text);
        }
      },
    });
    let store: ChannelStore | undefined;

    try {
      await harness.service.submitInbound(inbound({
        messageId: `${boundary}-source-watch-source`,
        ...target,
        text: sourceText,
        mentionedUserIds: [harness.bot.uid],
      }));
      await harness.workers.waitForPromptCount(1);
      await waitForStatus(harness.service, target, status => status.state === "completed");
      const created = await watchCreated.promise;
      await harness.service.submitInbound(inbound({
        messageId: `${boundary}-busy-chat-task`,
        ...target,
        text: busyText,
        mentionedUserIds: [harness.bot.uid],
      }));
      await busyStarted.promise;

      store = await openChannelStore(harness.stateDir);
      source.publish([{
        identity: `${boundary}-comment-1`,
        type: "comment",
        version: "1",
        body: { text: "must not reach an undispatched prompt" },
      }]);
      const queuedWake = await eventually(
        () => relevantQueuedWake(store!, harness.bot.uid, target),
        `${boundary}: source wake queued`,
      );
      if (boundary === "stop") {
        const event = store!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
          .find(candidate => candidate.wakeTaskId === queuedWake.id);
        if (!event) throw new Error("queued source wake has no accepted event");
        expect(event.wakeStatus).toBe("accepted");
        expect(() => store!.markSourceWakeDispatched(
          event.id,
          queuedWake.id,
          store!.getTask(queuedWake.id)!.sessionId,
          Date.now(),
        )).toThrow(/not started/);
        expect(store!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
          .find(candidate => candidate.id === event.id)?.wakeStatus).toBe("accepted");
      }

      if (boundary === "stop") {
        await created.tool.execute(
          { action: "stop", id: created.id },
          new AbortController().signal,
          "stop-source-watch-before-dispatch",
        );
      } else {
        const record = store!.getSourceWatch(harness.bot.uid, created.id);
        if (!record) throw new Error("deadline source watch disappeared");
        store!.expireSourceWatches(record.deadlineAt);
      }
      const terminalState = boundary === "stop" ? "stopped" : "expired";
      await eventually(
        () => store!.getSourceWatch(harness.bot.uid, created.id)?.state === terminalState ? true : undefined,
        `${boundary}: terminal source state`,
      );

      busy.resolve();
      await waitForTaskState(store!, queuedWake.id, "cancelled", `${boundary}: revoked task cancellation`);
      await eventually(() => store!.getTaskByMessageId(
        harness.bot.uid,
        target,
        `${boundary}-busy-chat-task`,
      )?.state === "completed" ? true : undefined, `${boundary}: busy task completion`);
      expect(unexpectedWakeTexts).toEqual([]);
      expect(harness.workers.prompts.map(prompt => prompt.text)).toEqual([sourceText, busyText]);
    } finally {
      busy.resolve();
      store?.close();
      await closeCoreHarness(harness);
    }
  });
}

test("source discovery exposes GitHub and GitLab capabilities while semantic data consent is required before creation", async () => {
  const source = createFixtureWatchSource();
  let sourceFactoryCalls = 0;
  let discoveryText = "";
  let createRejected = false;
  let listedWatchCount = -1;
  const sourceText = "请持续关注这个 GitHub PR 的新评论。";
  const harness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: () => {
      sourceFactoryCalls += 1;
      return source.source;
    },
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const tool = watchTool(promptContext);
      const signal = new AbortController().signal;
      const discovery = await tool.execute({ action: "discover" }, signal);
      discoveryText = discovery.toLowerCase();
      try {
        await tool.execute(
          sourceCreateArgs(githubResource, ["comment"], 60, false),
          signal,
          "missing-source-data-consent",
        );
      } catch {
        createRejected = true;
      }
      const list = JSON.parse(await tool.execute({ action: "list" }, signal)) as { watches: unknown[] };
      listedWatchCount = list.watches.length;
    },
  });

  try {
    await harness.service.submitInbound(inbound({
      messageId: "source-watch-discovery",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");

    for (const capability of [
      "github://",
      "gitlab://",
      "pull/",
      "merge_requests/",
      "review",
      "notes",
      "pagination is best-effort",
      "changes deleted, overwritten, or delayed between scans may be missed",
    ]) {
      expect(discoveryText).toContain(capability);
    }
    expect(createRejected).toBe(true);
    expect(listedWatchCount).toBe(0);
    expect(sourceFactoryCalls).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("natural end preserves a pending event's accepted wake", async () => {
  const source = createFixtureWatchSource();
  const busy = Promise.withResolvers<void>();
  const busyStarted = Promise.withResolvers<void>();
  const sourceText = "create a subscription before natural end";
  const busyText = "hold the queue while the subscription ends";
  const firstBody = "a useful comment before merge";
  const endBody = "the request has now merged";
  const createdPromise = Promise.withResolvers<{ id: string }>();
  const wakeTexts: string[] = [];
  const harness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: () => source.source,
    watchJudge: async ({ condition, stdout }) => {
      const body = JSON.parse(stdout).body as { text: string };
      const end = condition === endCondition;
      return {
        kind: "judgment",
        choice: end
          ? body.text === endBody ? "satisfied" : "insufficient_evidence"
          : body.text === firstBody ? "satisfied" : "not_satisfied",
        probability: 0.99,
      };
    },
    prompt: async ({ text, promptContext }) => {
      if (text === sourceText) {
        const created = JSON.parse(await watchTool(promptContext).execute(
          sourceCreateArgs(githubResource, ["comment"]),
          new AbortController().signal,
          "natural-end-source-create",
        )) as { id: string };
        createdPromise.resolve(created);
      } else if (text === busyText) {
        busyStarted.resolve();
        await busy.promise;
      } else {
        wakeTexts.push(text);
      }
    },
  });
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "natural-end-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    await harness.service.submitInbound(inbound({
      messageId: "natural-end-busy-task",
      ...target,
      text: busyText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await busyStarted.promise;

    store = await openChannelStore(harness.stateDir);
    source.publish([{
      identity: "comment-before-natural-end",
      type: "comment",
      version: "1",
      body: { text: firstBody },
    }]);
    const acceptedWake = await eventually(() => {
      const event = store!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
        .find(candidate => candidate.identity === "comment-before-natural-end");
      return event?.wakeStatus === "accepted" ? event : undefined;
    }, "natural end: first event wake acceptance");

    source.publish([{
      identity: "comment-that-ends-watch",
      type: "comment",
      version: "1",
      body: { text: endBody },
    }]);
    await eventually(
      () => store!.getSourceWatch(harness.bot.uid, created.id)?.state === "ended" ? true : undefined,
      "natural end: watch terminal state",
    );

    const preserved = store.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
      .find(event => event.id === acceptedWake.id);
    expect(preserved).toMatchObject({ state: "ready", wakeStatus: "accepted", wakeTaskId: acceptedWake.wakeTaskId });

    busy.resolve();
    await harness.workers.waitForPromptCount(3);
    expect(wakeTexts).toHaveLength(1);
    expect(wakeTexts[0]).toContain(firstBody);
  } finally {
    busy.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("recovered ended watch settles its unresolved consume judgment and immediately dispatches the wake", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "create a recoverable subscription";
  const eventBody = "the end condition is already satisfied";
  const createdPromise = Promise.withResolvers<{ id: string }>();
  let initialConsumeCalls = 0;
  let firstHarness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: () => source.source,
    watchJudge: async ({ condition, stdout }) => {
      const body = JSON.parse(stdout).body as { text?: string };
      if (condition === consumeCondition) {
        initialConsumeCalls += 1;
        return { kind: "judgment", choice: "insufficient_evidence", probability: 0.99 };
      }
      return {
        kind: "judgment",
        choice: body.text === eventBody ? "satisfied" : "not_satisfied",
        probability: 0.99,
      };
    },
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const created = JSON.parse(await watchTool(promptContext).execute(
        sourceCreateArgs(githubResource, ["comment"]),
        new AbortController().signal,
        "ended-recovery-source-create",
      )) as { id: string };
      createdPromise.resolve(created);
    },
  });
  let store: ChannelStore | undefined;
  let recovery: Awaited<ReturnType<typeof createSourceHarness>> | undefined;
  let recoveryStore: ChannelStore | undefined;

  try {
    await firstHarness.service.submitInbound(inbound({
      messageId: "ended-recovery-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [firstHarness.bot.uid],
    }));
    await firstHarness.workers.waitForPromptCount(1);
    await waitForStatus(firstHarness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    source.publish([{
      identity: "end-satisfied-consume-unresolved",
      type: "comment",
      version: "1",
      body: { text: eventBody },
    }]);

    store = await openChannelStore(firstHarness.stateDir);
    const unresolved = await eventually(() => {
      const event = store!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
        .find(candidate => candidate.identity === "end-satisfied-consume-unresolved");
      return event?.endState === "satisfied" && event.consumeState === "insufficient_evidence" ? event : undefined;
    }, "recovery: ended event with unresolved consume");
    expect(store.getSourceWatch(firstHarness.bot.uid, created.id)?.state).toBe("ended");
    expect(initialConsumeCalls).toBe(1);

    await firstHarness.service.stop();
    expect(store.markSourceEventFailure(created.id, unresolved.id, "retry-on-recovery", Date.now())).toBe(true);
    await closeCoreHarness(firstHarness, { remove: false });

    const recoveredSource = createFixtureWatchSource();
    recovery = await createSourceHarness({
      root: firstHarness.root,
      decisions: WATCH_CONFIG,
      watchSourceFactory: () => recoveredSource.source,
      watchJudge: async () => ({
        kind: "judgment",
        choice: "satisfied",
        probability: 0.99,
      }),
    });
    await recovery.service.start();
    await recovery.workers.waitForPromptCount(1);
    recoveryStore = await openChannelStore(recovery.stateDir);
    const recoveredEvent = await eventually(() => {
      const current = recoveryStore!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
        .find(candidate => candidate.identity === "end-satisfied-consume-unresolved");
      return current && (current.wakeStatus === "accepted" || current.wakeStatus === "dispatched")
        ? current
        : undefined;
    }, "recovery: retried consume and accepted wake");
    expect(recoveredEvent).toMatchObject({ consumeState: "satisfied", endState: "satisfied", wakeStatus: "dispatched" });
    expect(recoveredSource.initializeCalls).toBe(0);
    expect(recoveredSource.pollCalls).toBe(0);
    expect(recovery.workers.prompts[0]?.text).toContain(eventBody);
  } finally {
    store?.close();
    recoveryStore?.close();
    if (recovery) await closeCoreHarness(recovery);
    else await closeCoreHarness(firstHarness);
  }
});

test("recovery refuses to forward a pending event to a different authorized Decisions profile", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "create a profile-bound subscription";
  const createdPromise = Promise.withResolvers<{ id: string }>();
  const firstHarness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: () => source.source,
    watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const created = JSON.parse(await watchTool(promptContext).execute(
        sourceCreateArgs(githubResource, ["comment"]),
        new AbortController().signal,
        "profile-bound-source-create",
      )) as { id: string };
      createdPromise.resolve(created);
    },
  });
  let recovery: Awaited<ReturnType<typeof createSourceHarness>> | undefined;
  let recoveryStore: ChannelStore | undefined;
  let judgeCalls = 0;

  try {
    await firstHarness.service.submitInbound(inbound({
      messageId: "profile-bound-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [firstHarness.bot.uid],
    }));
    await firstHarness.workers.waitForPromptCount(1);
    await waitForStatus(firstHarness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    await firstHarness.service.stop();
    await closeCoreHarness(firstHarness, { remove: false });

    const recoveredSource = createFixtureWatchSource();
    recovery = await createSourceHarness({
      root: firstHarness.root,
      decisions: { ...WATCH_CONFIG, model: "different-model" },
      watchSourceFactory: () => recoveredSource.source,
      watchJudge: async () => {
        judgeCalls += 1;
        return { kind: "judgment", choice: "satisfied", probability: 0.99 };
      },
    });
    await recovery.service.start();
    await eventually(() => recoveredSource.pollCalls > 0 ? true : undefined);
    recoveredSource.publish([{
      identity: "profile-change-event",
      type: "comment",
      version: "1",
      body: { text: "must not leave the authorized Decisions profile" },
    }]);
    recoveryStore = await openChannelStore(recovery.stateDir);
    const event = await eventually(() => {
      const current = recoveryStore!.listSourceWatchEvents(created.id, Number.POSITIVE_INFINITY)
        .find(candidate => candidate.identity === "profile-change-event");
      return current?.error === "decision-profile-mismatch" ? current : undefined;
    });
    expect(event.error).toBe("decision-profile-mismatch");
    expect(judgeCalls).toBe(0);
    expect(event.wakeStatus).toBe("none");
  } finally {
    recoveryStore?.close();
    if (recovery) await closeCoreHarness(recovery);
    else await closeCoreHarness(firstHarness);
  }
});
type SourceEndWhen = {
  field: "state" | "merged" | "title" | "headSha";
  equals: string | boolean;
};

function deterministicSourceCreateArgs(endWhen: SourceEndWhen) {
  return {
    action: "create",
    resource: githubResource,
    eventTypes: ["snapshot"],
    taskScope,
    decisionConsent: false,
    decisionBudget: 0,
    timeout: 60,
    endWhen,
  };
}

test("deterministic endWhen ends on a matching baseline without Decisions or a wake", async () => {
  const rules = [
    { field: "state", equals: "open" },
    { field: "merged", equals: false },
    { field: "title", equals: "Fixture pull request" },
    { field: "headSha", equals: "fixture-head" },
  ] as const satisfies readonly SourceEndWhen[];
  const source = createFixtureWatchSource();
  const sourceText = "create deterministic snapshot watches";
  const created: Array<{ id: string; state: string }> = [];
  let exposedEndWhenSchema: unknown;
  let exposedWatchSchema: Record<string, unknown> | undefined;
  const harness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const tool = watchTool(promptContext);
      exposedEndWhenSchema = (tool.parameters.properties as Record<string, unknown> | undefined)?.endWhen;
      exposedWatchSchema = tool.parameters;
      for (const [index, endWhen] of rules.entries()) {
        created.push(JSON.parse(await tool.execute(
          deterministicSourceCreateArgs(endWhen),
          new AbortController().signal,
          `deterministic-baseline-source-watch-${index}`,
        )) as { id: string; state: string });
      }
    },
  });
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "deterministic-baseline-source-watches",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    store = await openChannelStore(harness.stateDir);
    expect(exposedEndWhenSchema).toMatchObject({
      type: "object",
      properties: {
        field: { enum: ["state", "merged", "title", "headSha"] },
        equals: expect.any(Object),
      },
      required: ["field", "equals"],
    });
    expect(exposedWatchSchema).toMatchObject({
      not: { required: ["endWhen", "endCondition"] },
    });

    expect(created.map(watch => watch.state)).toEqual(["ended", "ended", "ended", "ended"]);
    expect(source.initializeCalls).toBe(rules.length);
    for (const [index, endWhen] of rules.entries()) {
      const watch = store.getSourceWatch(harness.bot.uid, created[index]!.id);
      expect(watch).toMatchObject({
        state: "ended",
        decisionUsed: 0,
        spec: {
          endWhen,
          consumeCondition: null,
          endCondition: null,
          decisionConsent: false,
          decisionProfile: null,
          decisionBudget: 0,
        },
      });
      const events = store.listSourceWatchEvents(created[index]!.id, Number.POSITIVE_INFINITY);
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        body: {
          state: "open",
          merged: false,
          title: "Fixture pull request",
          headSha: "fixture-head",
        },
        state: "ready",
        consumeState: "not_satisfied",
        endState: "satisfied",
        wakeTaskId: null,
        wakeStatus: "none",
      });
    }
    expect(relevantQueuedWake(store, harness.bot.uid, target)).toBeUndefined();
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a deterministic snapshot transition can both end its watch and retain its independent wake", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "create a deterministic merge watch";
  const busyText = "hold the chat queue before the ending snapshot";
  const busy = Promise.withResolvers<void>();
  const busyStarted = Promise.withResolvers<void>();
  const createdPromise = Promise.withResolvers<{ id: string; state: string }>();
  const wakeTexts: string[] = [];
  const harness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text === sourceText) {
        createdPromise.resolve(JSON.parse(await watchTool(promptContext).execute(
          deterministicSourceCreateArgs({ field: "merged", equals: true }),
          new AbortController().signal,
          "deterministic-transition-source-watch",
        )) as { id: string; state: string });
      } else if (text === busyText) {
        busyStarted.resolve();
        await busy.promise;
      } else {
        wakeTexts.push(text);
      }
    },
  });
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "deterministic-transition-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    expect(created.state).toBe("watching");

    await harness.service.submitInbound(inbound({
      messageId: "deterministic-transition-busy",
      ...target,
      text: busyText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await busyStarted.promise;

    store = await openChannelStore(harness.stateDir);
    await eventually(() => source.pollCalls > 0 ? true : undefined);
    source.publish([], {
      state: "closed",
      merged: true,
      title: "Fixture pull request",
      headSha: "fixture-head",
    });
    await eventually(
      () => store!.getSourceWatch(harness.bot.uid, created.id)?.state === "ended" ? true : undefined,
      "deterministic end after snapshot transition",
    );
    const event = await eventually(() => store!.listSourceWatchEvents(
      created.id,
      Number.POSITIVE_INFINITY,
    ).find(candidate =>
      candidate.type === "snapshot" &&
      candidate.endState === "satisfied" &&
      candidate.wakeTaskId !== null));
    expect(event).toMatchObject({
      body: {
        previous: {
          state: "open",
          merged: false,
          title: "Fixture pull request",
          headSha: "fixture-head",
        },
        current: {
          state: "closed",
          merged: true,
          title: "Fixture pull request",
          headSha: "fixture-head",
        },
      },
      state: "ready",
      consumeState: "deterministic",
      endState: "satisfied",
      wakeStatus: "accepted",
    });
    expect(store.getSourceWatch(harness.bot.uid, created.id)).toMatchObject({
      state: "ended",
      decisionUsed: 0,
      spec: {
        endWhen: { field: "merged", equals: true },
        decisionConsent: false,
        decisionProfile: null,
      },
    });
    const wake = await eventually(() => relevantQueuedWake(store!, harness.bot.uid, target));
    expect(event.wakeTaskId).toBe(wake.id);
    expect(wake.messageId.startsWith("watch:")).toBe(true);

    busy.resolve();
    await harness.workers.waitForPromptCount(3);
    await waitForTaskState(store!, wake.id, "completed");
    expect(wakeTexts).toHaveLength(1);
    expect(wakeTexts[0]).toContain('"merged":true');
  } finally {
    busy.resolve();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a deterministic endWhen rule survives restart and ends on the next observed transition", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "create a restartable deterministic watch";
  const createdPromise = Promise.withResolvers<{ id: string }>();
  const firstHarness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const created = JSON.parse(await watchTool(promptContext).execute(
        deterministicSourceCreateArgs({ field: "state", equals: "closed" }),
        new AbortController().signal,
        "deterministic-restart-source-watch",
      )) as { id: string };
      createdPromise.resolve(created);
    },
  });
  let firstStore: ChannelStore | undefined;
  let recovery: Awaited<ReturnType<typeof createSourceHarness>> | undefined;
  let recoveryStore: ChannelStore | undefined;
  const recoveredSource = createFixtureWatchSource();
  const wakeTexts: string[] = [];

  try {
    await firstHarness.service.submitInbound(inbound({
      messageId: "deterministic-restart-source",
      ...target,
      text: sourceText,
      mentionedUserIds: [firstHarness.bot.uid],
    }));
    await firstHarness.workers.waitForPromptCount(1);
    await waitForStatus(firstHarness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    firstStore = await openChannelStore(firstHarness.stateDir);
    await eventually(() => source.pollCalls > 0 ? true : undefined);
    expect(firstStore.getSourceWatch(firstHarness.bot.uid, created.id)).toMatchObject({
      state: "watching",
      spec: { endWhen: { field: "state", equals: "closed" }, decisionConsent: false },
    });
    firstStore.close();
    firstStore = undefined;
    await closeCoreHarness(firstHarness, { remove: false });

    recovery = await createSourceHarness({
      root: firstHarness.root,
      watch: { enabled: true },
      watchSourceFactory: () => recoveredSource.source,
      prompt: async ({ text }) => {
        wakeTexts.push(text);
      },
    });
    recoveryStore = await openChannelStore(recovery.stateDir);
    await eventually(() => recoveredSource.pollCalls > 0 ? true : undefined);
    expect(recoveryStore.getSourceWatch(firstHarness.bot.uid, created.id)).toMatchObject({
      state: "watching",
      snapshot: { state: "open" },
      spec: { endWhen: { field: "state", equals: "closed" } },
    });
    recoveredSource.publish([], {
      state: "closed",
      merged: false,
      title: "Fixture pull request",
      headSha: "fixture-head",
    });
    await eventually(
      () => recoveryStore!.getSourceWatch(firstHarness.bot.uid, created.id)?.state === "ended"
        ? true
        : undefined,
      "recovered deterministic end",
    );
    const event = await eventually(() => recoveryStore!.listSourceWatchEvents(
      created.id,
      Number.POSITIVE_INFINITY,
    ).find(candidate =>
      candidate.endState === "satisfied" &&
      candidate.wakeTaskId !== null));
    expect(event).toMatchObject({
      body: {
        previous: { state: "open" },
        current: { state: "closed" },
      },
      consumeState: "deterministic",
      endState: "satisfied",
    });
    expect(recoveredSource.initializeCalls).toBe(0);
    expect(recoveryStore.getSourceWatch(firstHarness.bot.uid, created.id)).toMatchObject({
      state: "ended",
      decisionUsed: 0,
      spec: { endWhen: { field: "state", equals: "closed" }, decisionConsent: false },
    });
    await recovery.workers.waitForPromptCount(1);
    const wakeTaskId = event.wakeTaskId;
    if (!wakeTaskId) throw new Error("recovered deterministic event lost its wake task");
    await waitForTaskState(recoveryStore!, wakeTaskId, "completed");
    expect(wakeTexts).toHaveLength(1);
    expect(wakeTexts[0]).toContain('"state":"closed"');
  } finally {
    firstStore?.close();
    recoveryStore?.close();
    if (recovery) await closeCoreHarness(recovery);
    else await closeCoreHarness(firstHarness);
  }
});

test("source watch create rejects endWhen together with endCondition", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "reject conflicting source end rules";
  let createError = "";
  const harness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      try {
        await watchTool(promptContext).execute({
          ...deterministicSourceCreateArgs({ field: "state", equals: "closed" }),
          endCondition,
        }, new AbortController().signal, "conflicting-source-end-rules");
      } catch (error) {
        createError = error instanceof Error ? error.message : String(error);
      }
    },
  });

  try {
    await harness.service.submitInbound(inbound({
      messageId: "conflicting-source-end-rules",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    expect(createError).toMatch(/endWhen.*endCondition|endCondition.*endWhen/);
    expect(source.initializeCalls).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("source watch rejects endWhen values that do not match the selected snapshot field", async () => {
  const invalidRules: unknown[] = [
    { field: "state", equals: true },
    { field: "merged", equals: "true" },
    { field: "title", equals: false },
    { field: "headSha", equals: 1 },
    { field: "unknown", equals: "closed" },
    { field: "state", equals: "closed", extra: true },
    null,
  ];
  const source = createFixtureWatchSource();
  const sourceText = "reject invalid deterministic end rules";
  const errors: string[] = [];
  const harness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const tool = watchTool(promptContext);
      for (const [index, endWhen] of invalidRules.entries()) {
        try {
          await tool.execute({
            ...deterministicSourceCreateArgs({ field: "state", equals: "closed" }),
            endWhen,
          }, new AbortController().signal, `invalid-end-when-${index}`);
        } catch (error) {
          errors.push(error instanceof Error ? error.message : String(error));
        }
      }
    },
  });

  try {
    await harness.service.submitInbound(inbound({
      messageId: "invalid-source-end-rules",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    expect(errors).toHaveLength(invalidRules.length);
    expect(errors.every(message => message.includes("endWhen"))).toBe(true);
    expect(source.initializeCalls).toBe(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("legacy source watch specs without endWhen recover it as unset", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "create a source watch with a legacy spec";
  const createdPromise = Promise.withResolvers<{ id: string }>();
  const firstHarness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: () => source.source,
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      const created = JSON.parse(await watchTool(promptContext).execute({
        action: "create",
        resource: githubResource,
        eventTypes: ["comment"],
        taskScope,
        decisionConsent: false,
        decisionBudget: 0,
        timeout: 60,
      }, new AbortController().signal, "legacy-source-watch-create")) as { id: string };
      createdPromise.resolve(created);
    },
  });
  const recoveredSource = createFixtureWatchSource();
  const wakeTexts: string[] = [];
  let firstStore: ChannelStore | undefined;
  let recovery: Awaited<ReturnType<typeof createSourceHarness>> | undefined;
  let recoveryStore: ChannelStore | undefined;

  try {
    await firstHarness.service.submitInbound(inbound({
      messageId: "legacy-source-watch",
      ...target,
      text: sourceText,
      mentionedUserIds: [firstHarness.bot.uid],
    }));
    await firstHarness.workers.waitForPromptCount(1);
    await waitForStatus(firstHarness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    firstStore = await openChannelStore(firstHarness.stateDir);
    await eventually(() => source.pollCalls > 0 ? true : undefined);
    const stored = firstStore.getSourceWatch(firstHarness.bot.uid, created.id);
    if (!stored) throw new Error("legacy source watch was not persisted");
    const legacySpec = JSON.parse(JSON.stringify(stored.spec)) as Record<string, unknown>;
    delete legacySpec.endWhen;
    firstStore.close();
    firstStore = undefined;
    await closeCoreHarness(firstHarness, { remove: false });

    const legacyDatabase = new Database(`${firstHarness.stateDir}/channel.sqlite`);
    try {
      const updated = legacyDatabase.query(
        "UPDATE source_watches SET spec_json = ? WHERE id = ?",
      ).run(JSON.stringify(legacySpec), created.id);
      expect(updated.changes).toBe(1);
    } finally {
      legacyDatabase.close();
    }

    recovery = await createSourceHarness({
      root: firstHarness.root,
      watch: { enabled: true },
      watchSourceFactory: () => recoveredSource.source,
      prompt: async ({ text }) => {
        wakeTexts.push(text);
      },
    });
    recoveryStore = await openChannelStore(recovery.stateDir);
    await eventually(() => recoveredSource.pollCalls > 0 ? true : undefined);
    expect(recoveryStore.getSourceWatch(firstHarness.bot.uid, created.id)?.spec.endWhen).toBeNull();
    recoveredSource.publish([{
      identity: "legacy-spec-comment",
      type: "comment",
      version: "1",
      body: { text: "the legacy subscription still consumes selected comments" },
    }], {
      state: "closed",
      merged: false,
      title: "Fixture pull request",
      headSha: "fixture-head",
    });
    const event = await eventually(() => recoveryStore!.listSourceWatchEvents(
      created.id,
      Number.POSITIVE_INFINITY,
    ).find(candidate =>
      candidate.identity === "legacy-spec-comment" &&
      candidate.wakeTaskId !== null));
    expect(event).toMatchObject({
      consumeState: "deterministic",
      endState: "not_satisfied",
    });
    expect(recoveryStore.getSourceWatch(firstHarness.bot.uid, created.id)).toMatchObject({
      state: "watching",
      decisionUsed: 0,
      spec: { endWhen: null, decisionConsent: false },
    });
    await recovery.workers.waitForPromptCount(1);
    if (!event.wakeTaskId) throw new Error("legacy selected event did not retain its wake");
    await waitForTaskState(recoveryStore!, event.wakeTaskId, "completed");
    expect(wakeTexts).toHaveLength(1);
    expect(wakeTexts[0]).toContain("legacy subscription still consumes selected comments");
  } finally {
    firstStore?.close();
    recoveryStore?.close();
    if (recovery) await closeCoreHarness(recovery);
    else await closeCoreHarness(firstHarness);
  }
});

test("a matching end snapshot does not silently drop a subscribed comment in the same source observation", async () => {
  const snapshotFirstSource = createFixtureWatchSource();
  const commentFirstSource = createFixtureWatchSource();
  const sourceText = "create source watches for event-order coverage";
  const createdPromise = Promise.withResolvers<Array<{ id: string }>>();
  const wakeTexts: string[] = [];
  const harness = await createSourceHarness({
    watch: { enabled: true },
    watchSourceFactory: resource => {
      if (resource.provider === "github" && resource.number === 19544) return snapshotFirstSource.source;
      if (resource.provider === "github" && resource.number === 19545) return commentFirstSource.source;
      throw new Error(`unexpected fixture resource: ${JSON.stringify(resource)}`);
    },
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) {
        wakeTexts.push(text);
        return;
      }
      const tool = watchTool(promptContext);
      const created: Array<{ id: string }> = [];
      for (const [index, resource] of [githubResource, "github://can1357/oh-my-pi/pull/19545"].entries()) {
        created.push(JSON.parse(await tool.execute({
          ...deterministicSourceCreateArgs({ field: "merged", equals: true }),
          resource,
          eventTypes: ["comment"],
        }, new AbortController().signal, `same-observation-watch-${index}`)) as { id: string });
      }
      createdPromise.resolve(created);
    },
  });
  let store: ChannelStore | undefined;
  let orderingDatabase: Database | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "same-observation-source-watches",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    const created = await createdPromise.promise;
    store = await openChannelStore(harness.stateDir);

    // Pin the otherwise-random event UUID ordering so both source event orders are exercised.
    orderingDatabase = new Database(`${harness.stateDir}/channel.sqlite`);
    orderingDatabase.run(`
      CREATE TRIGGER force_same_observation_event_order
      AFTER INSERT ON source_watch_events
      WHEN NEW.identity IN (
        'snapshot-first-snapshot',
        'snapshot-first-comment',
        'comment-first-comment',
        'comment-first-snapshot'
      )
      BEGIN
        UPDATE source_watch_events SET id = CASE NEW.identity
          WHEN 'snapshot-first-snapshot' THEN '00000000-0000-4000-8000-000000000001'
          WHEN 'snapshot-first-comment' THEN '00000000-0000-4000-8000-000000000002'
          WHEN 'comment-first-comment' THEN '00000000-0000-4000-8000-000000000003'
          WHEN 'comment-first-snapshot' THEN '00000000-0000-4000-8000-000000000004'
        END WHERE id = NEW.id;
      END;
    `);

    await Promise.all([
      eventually(() => snapshotFirstSource.pollCalls > 0 ? true : undefined, "snapshot-first watch polling"),
      eventually(() => commentFirstSource.pollCalls > 0 ? true : undefined, "comment-first watch polling"),
    ]);

    const openSnapshot = {
      state: "open",
      merged: false,
      title: "Fixture pull request",
      headSha: "fixture-head",
    };
    const mergedSnapshot = {
      state: "closed",
      merged: true,
      title: "Fixture pull request",
      headSha: "fixture-head",
    };
    snapshotFirstSource.publish([
      {
        identity: "snapshot-first-snapshot",
        type: "snapshot",
        version: "transition:1",
        body: { previous: openSnapshot, current: mergedSnapshot },
      },
      {
        identity: "snapshot-first-comment",
        type: "comment",
        version: "1",
        body: { text: "comment observed before the matching end snapshot" },
      },
    ], mergedSnapshot);
    commentFirstSource.publish([
      {
        identity: "comment-first-comment",
        type: "comment",
        version: "1",
        body: { text: "comment observed before the matching end snapshot" },
      },
      {
        identity: "comment-first-snapshot",
        type: "snapshot",
        version: "transition:1",
        body: { previous: openSnapshot, current: mergedSnapshot },
      },
    ], mergedSnapshot);

    await Promise.all(created.map(({ id }) => eventually(
      () => store!.getSourceWatch(harness.bot.uid, id)?.state === "ended" ? true : undefined,
      "same-observation source watch end",
    )));

    const comments = await Promise.all(created.map(({ id }, index) => {
      const identity = index === 0 ? "snapshot-first-comment" : "comment-first-comment";
      return eventually(() => {
        const event = store!.listSourceWatchEvents(id, Number.POSITIVE_INFINITY)
          .find(candidate => candidate.identity === identity);
        return event && event.wakeTaskId !== null && event.wakeStatus === "accepted" &&
          event.state === "ready" && event.endState === "not_satisfied" ? event : undefined;
      }, "same-observation comment wake association");
    }));
    for (const event of comments) {
      expect(event).toMatchObject({
        state: "ready",
        consumeState: "deterministic",
        endState: "not_satisfied",
        wakeStatus: "accepted",
      });
      expect(event.wakeTaskId).not.toBeNull();
    }
    const snapshots = created.map(({ id }, index) => {
      const identity = index === 0 ? "snapshot-first-snapshot" : "comment-first-snapshot";
      return store!.listSourceWatchEvents(id, Number.POSITIVE_INFINITY)
        .find(candidate => candidate.identity === identity);
    });
    expect(snapshots.map(event => event?.endState)).toEqual(["satisfied", "satisfied"]);

    await harness.workers.waitForPromptCount(3);
    for (const event of comments) {
      if (!event.wakeTaskId) throw new Error("consumed source comment has no wake task");
      await waitForTaskState(store, event.wakeTaskId, "completed");
    }
    expect(wakeTexts).toHaveLength(2);
    expect(wakeTexts.every(text => text.includes("comment observed before the matching end snapshot"))).toBe(true);
  } finally {
    orderingDatabase?.close();
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("script watch creation rejects a source-only endWhen rule instead of ignoring it", async () => {
  const source = createFixtureWatchSource();
  const sourceText = "reject source endWhen on a script watch";
  let createResult: string | undefined;
  let createError: string | undefined;
  const harness = await createSourceHarness({
    decisions: WATCH_CONFIG,
    watchSourceFactory: () => source.source,
    watchCollector: async () => ({ kind: "success", stdout: "still waiting" }),
    watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    prompt: async ({ text, promptContext }) => {
      if (text !== sourceText) return;
      try {
        createResult = await watchTool(promptContext).execute({
          action: "create",
          script: "printf 'fixture observation'",
          condition: "The fixture condition is satisfied.",
          interval: 60,
          timeout: 60,
          endWhen: { field: "state", equals: "closed" },
        }, new AbortController().signal, "script-watch-with-source-endWhen");
      } catch (error) {
        createError = error instanceof Error ? error.message : String(error);
      }
    },
  });
  let store: ChannelStore | undefined;

  try {
    await harness.service.submitInbound(inbound({
      messageId: "script-watch-source-end-when",
      ...target,
      text: sourceText,
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");
    store = await openChannelStore(harness.stateDir);

    expect(createError).toMatch(/endWhen/i);
    expect(createResult).toBeUndefined();
    expect(store.listWatches(harness.bot.uid, target, 50, 0)).toEqual([]);
    expect(source.initializeCalls).toBe(0);
  } finally {
    store?.close();
    await closeCoreHarness(harness);
  }
});
