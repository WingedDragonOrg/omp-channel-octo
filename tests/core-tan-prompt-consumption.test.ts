import { symlink, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { normalizedMessage, TEXT_MESSAGE_TYPE } from "../src/octo/content.ts";
import type { ChatTarget, OmpWorker, WorkerOptions } from "../src/contracts.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";
import {
  FORK_GROUP,
  ForkTestTransport,
  ForkTestWorkerFactory,
  ownerMessage,
  seedBinding,
  withStore,
} from "./fixtures/core-fork.ts";

const targetFor = (shortId: string): ChatTarget & { channelType: 5 } => ({
  channelType: 5,
  channelId: `${FORK_GROUP.channelId}____${shortId}`,
});
async function seedAliasedBinding(
  harness: CoreHarness,
  sessionId: string,
): Promise<{ canonicalCwd: string; aliasCwd: string }> {
  const canonicalCwd = await realpath(harness.root);
  const aliasCwd = join(harness.root, "source-workspace-link");
  const sessionFile = join(harness.root, `${sessionId}.jsonl`);
  await symlink(canonicalCwd, aliasCwd);
  await writeFile(
    sessionFile,
    `${JSON.stringify({ type: "session", version: 3, id: sessionId, cwd: canonicalCwd })}\n`,
  );
  await withStore(harness, store => store.ensureBinding({
    botUid: harness.bot.uid,
    ...FORK_GROUP,
    sessionId,
    cwd: aliasCwd,
    sessionFile,
  }));
  return { canonicalCwd, aliasCwd };
}

function slashTanMessage(harness: CoreHarness, messageId: bigint) {
  const rawText = `@${harness.bot.name} /tan /status`;
  return normalizedMessage({
    setting: 0,
    senderId: "owner-1",
    rawChannelId: FORK_GROUP.channelId,
    channelType: 2,
    messageId,
    messageSeq: 0,
    timestamp: 0,
    payload: {
      type: TEXT_MESSAGE_TYPE,
      content: rawText,
      mention: {
        entities: [{ uid: harness.bot.uid, offset: 0, length: `@${harness.bot.name}`.length }],
      },
    },
  }, harness.bot.uid, harness.bot.name);
}



test("slash-leading TAN text stays raw in durable rows and is wrapped only for the first worker prompt", async () => {
  const workers = new ForkTestWorkerFactory();
  const harness = await createCoreHarness({
    transport: new ForkTestTransport(),
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const target = targetFor("thread-1");
  try {
    const { canonicalCwd, aliasCwd } = await seedAliasedBinding(harness, "tan-slash-source-session");
    const message = slashTanMessage(harness, 123n);
    expect(message.text).toBe("/tan /status");
    await harness.service.handleInbound(message);
    await workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "completed");

    expect(workers.prompts[0]?.text).toBe("用户请求：\n/status");
    await withStore(harness, store => {
      const operation = store.listThreadForks(harness.bot.uid)[0];
      expect(operation).toBeDefined();
      if (!operation) throw new Error("TAN fork record was not persisted");
      expect(store.getTanOperation(harness.bot.uid, operation.id)?.text).toBe("/status");
      expect(store.getTanOperation(harness.bot.uid, operation.id)?.cwd).toBe(canonicalCwd);
      expect(store.getBinding(harness.bot.uid, FORK_GROUP)?.cwd).toBe(aliasCwd);
      expect(store.getTaskByMessageId(harness.bot.uid, target, `dispatch:tan:${operation.id}`)?.text).toBe("/status");
    });
  } finally {
    await closeCoreHarness(harness);
  }
}, 15_000);

test("a restarted queued TAN task gets the same slash-safe worker prompt without changing its durable text", async () => {
  const target = targetFor("thread-1");
  const targetCreateStarted = Promise.withResolvers<void>();
  const releaseTargetCreate = Promise.withResolvers<void>();
  const firstWorkers = new ForkTestWorkerFactory();
  let createCount = 0;
  firstWorkers.beforeCreate = async () => {
    createCount += 1;
    if (createCount === 2) {
      targetCreateStarted.resolve();
      await releaseTargetCreate.promise;
    }
  };
  const first = await createCoreHarness({
    transport: new ForkTestTransport(),
    workers: firstWorkers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let restarted: CoreHarness | undefined;
  try {
    await seedBinding(first, FORK_GROUP, first.root, "tan-restart-source-session");
    await first.service.handleInbound(slashTanMessage(first, 124n));
    await targetCreateStarted.promise;

    const id = await withStore(first, store => {
      const fork = store.listThreadForks(first.bot.uid)
        .find(record => record.thread?.channelId === target.channelId);
      expect(fork).toBeDefined();
      if (!fork) throw new Error("TAN fork record was not persisted before target worker creation");
      expect(store.getTanOperation(first.bot.uid, fork.id)?.phase).toBe("done");
      const task = store.getTaskByMessageId(first.bot.uid, target, `dispatch:tan:${fork.id}`);
      expect(task?.state).toBe("running");
      expect(task?.text).toBe("/status");
      return fork.id;
    });
    expect(firstWorkers.prompts).toHaveLength(0);

    const stopping = closeCoreHarness(first, { remove: false });
    releaseTargetCreate.resolve();
    await stopping;

    const workers = new FakeWorkerFactory();
    workers.recovered = true;
    restarted = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      workers,
      allowedGroupIds: [FORK_GROUP.channelId],
    });
    await workers.waitForPromptCount(1);
    await waitForStatus(restarted.service, target, status => status.state === "completed");

    expect(workers.prompts[0]?.text).toBe("用户请求：\n/status");
    await withStore(restarted, store => {
      expect(store.getTanOperation(first.bot.uid, id)?.text).toBe("/status");
      expect(store.getTaskByMessageId(first.bot.uid, target, `dispatch:tan:${id}`)?.text).toBe("/status");
    });
  } finally {
    releaseTargetCreate.resolve();
    if (restarted) await closeCoreHarness(restarted);
    else await closeCoreHarness(first);
  }
}, 15_000);

test("TAN refuses a source binding replaced while attachment materialization is in flight", async () => {
  const transport = new ForkTestTransport();
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const download = transport.downloadAttachment.bind(transport);
  transport.downloadAttachment = async request => {
    downloadStarted.resolve();
    await releaseDownload.promise;
    return download(request);
  };
  const harness = await createCoreHarness({
    transport,
    allowedGroupIds: [FORK_GROUP.channelId],
    attachments: { enabled: true, maxBytes: 1024 },
  });
  const message = inbound({
    ...FORK_GROUP,
    messageId: "tan-source-binding-race",
    senderId: "owner-1",
    text: "/tan inspect the attachment",
    mentionedUserIds: [harness.bot.uid],
    attachments: [{
      url: "https://cdn.example.test/tan-source-race.txt",
      kind: "file",
      name: "tan-source-race.txt",
      size: 12,
      durationSeconds: 0,
    }],
  });
  let request: Promise<void> | undefined;
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-source-race-original");
    request = harness.service.handleInbound(message);
    await downloadStarted.promise;
    const cwd = await realpath(harness.root);
    const replacementSessionId = "tan-source-race-replacement";
    const replacementSessionFile = join(harness.root, `${replacementSessionId}.jsonl`);
    await writeFile(
      replacementSessionFile,
      `${JSON.stringify({ type: "session", version: 3, id: replacementSessionId, cwd })}\n`,
    );
    await withStore(harness, store => {
      store.updateBindingSession(
        harness.bot.uid,
        FORK_GROUP,
        replacementSessionId,
        replacementSessionFile,
      );
    });
    releaseDownload.resolve();
    await request;

    await withStore(harness, store => {
      expect(store.listThreadForks(harness.bot.uid)).toHaveLength(0);
      expect(store.listPendingTanOperations(harness.bot.uid)).toHaveLength(0);
      expect(store.getCommandClaimState(harness.bot.uid, FORK_GROUP, message.messageId)).toBe("completed");
    });
  } finally {
    releaseDownload.resolve();
    if (request) await Promise.allSettled([request]);
    await closeCoreHarness(harness);
  }
}, 15_000);

test("cancel waits for a known remote TAN allocation and durably reports it without dispatch", async () => {
  class PostCreateGateTransport extends ForkTestTransport {
    readonly remoteCreated = Promise.withResolvers<void>();
    readonly abortObserved = Promise.withResolvers<void>();
    readonly releaseReturn = Promise.withResolvers<void>();

    override async createThread(groupId: string, name: string, signal?: AbortSignal) {
      const thread = await super.createThread(groupId, name, signal);
      this.remoteCreated.resolve();
      const observeAbort = (): void => this.abortObserved.resolve();
      if (signal?.aborted) observeAbort();
      else signal?.addEventListener("abort", observeAbort, { once: true });
      try {
        await this.releaseReturn.promise;
      } finally {
        signal?.removeEventListener("abort", observeAbort);
      }
      return thread;
    }
  }
  const transport = new PostCreateGateTransport();
  const harness = await createCoreHarness({
    transport,
    workers: new ForkTestWorkerFactory(),
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let restarted: CoreHarness | undefined;
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-cancel-remote-source");
    const request = harness.service.handleInbound(slashTanMessage(harness, 126n));
    await transport.remoteCreated.promise;
    const cancellation = harness.service.handleInbound(
      ownerMessage(harness, FORK_GROUP, "tan-cancel-remote", "/cancel"),
    );
    await transport.abortObserved.promise;
    transport.releaseReturn.resolve();
    await Promise.all([request, cancellation]);

    expect(transport.createAttempts).toBe(1);
    await withStore(harness, store => {
      const fork = store.listThreadForks(harness.bot.uid)[0];
      expect(fork?.phase).toBe("remote-created");
      if (!fork?.thread) throw new Error("known remote TAN allocation was not retained");
      const operation = store.getTanOperation(harness.bot.uid, fork.id);
      expect(operation?.phase).toBe("cancelled");
      expect(operation?.error).toContain(`子区 ${fork.thread.name}`);
      expect(store.getTaskByMessageId(
        harness.bot.uid,
        { channelType: 5, channelId: fork.thread.channelId },
        `dispatch:tan:${fork.id}`,
      )).toBeNull();
      expect(store.getOutbox(`tan:${fork.id}:cancel-result:0`)?.text).toContain("已创建但尚未完成本地绑定");
    });
    const stopping = closeCoreHarness(harness, { remove: false });
    await stopping;
    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({
      root: harness.root,
      bot: harness.bot,
      transport: new ForkTestTransport(),
      workers: restartWorkers,
      allowedGroupIds: [FORK_GROUP.channelId],
    });
    expect(restartWorkers.prompts).toHaveLength(0);
    await withStore(restarted, store => {
      const fork = store.listThreadForks(harness.bot.uid)[0];
      expect(fork?.phase).toBe("ready");
      if (!fork?.thread) throw new Error("known cancelled remote TAN allocation was not recovered");
      expect(store.getTanOperation(harness.bot.uid, fork.id)?.phase).toBe("cancelled");
      expect(store.getTaskByMessageId(
        harness.bot.uid,
        { channelType: 5, channelId: fork.thread.channelId },
        `dispatch:tan:${fork.id}`,
      )).toBeNull();
    });
  } finally {
    transport.releaseReturn.resolve();
    if (restarted) await closeCoreHarness(restarted);
    else await closeCoreHarness(harness);
  }
}, 15_000);

test("cancel during unknown remote-create outcome reports it and restart never recreates", async () => {
  const transport = new ForkTestTransport();
  transport.holdCreateThread = true;
  const harness = await createCoreHarness({
    transport,
    workers: new ForkTestWorkerFactory(),
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  let restarted: CoreHarness | undefined;
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-cancel-unknown-source");
    const request = harness.service.handleInbound(slashTanMessage(harness, 128n));
    await transport.createThreadStarted.promise;
    const cancellation = harness.service.handleInbound(
      ownerMessage(harness, FORK_GROUP, "tan-cancel-unknown", "/cancel"),
    );
    await Promise.all([request, cancellation]);

    expect(transport.createAttempts).toBe(1);
    await withStore(harness, store => {
      const fork = store.listThreadForks(harness.bot.uid)[0];
      expect(fork?.phase).toBe("creating");
      expect(fork?.thread).toBeNull();
      if (!fork) throw new Error("unknown remote-create result was not durably retained");
      const operation = store.getTanOperation(harness.bot.uid, fork.id);
      expect(operation?.phase).toBe("cancelled");
      expect(operation?.error).toContain("结果未知");
      expect(store.getOutbox(`tan:${fork.id}:cancel-result:0`)?.text).toContain("不会重试创建");
    });

    const stopping = closeCoreHarness(harness, { remove: false });
    await stopping;
    const restartTransport = new ForkTestTransport();
    const restartWorkers = new FakeWorkerFactory();
    restartWorkers.recovered = true;
    restarted = await createCoreHarness({
      root: harness.root,
      bot: harness.bot,
      transport: restartTransport,
      workers: restartWorkers,
      allowedGroupIds: [FORK_GROUP.channelId],
    });
    expect(restartTransport.createAttempts).toBe(0);
    expect(restartWorkers.prompts).toHaveLength(0);
    await withStore(restarted, store => {
      const fork = store.listThreadForks(harness.bot.uid)[0];
      expect(fork?.phase).toBe("creating");
      expect(fork?.thread).toBeNull();
      if (!fork) throw new Error("unknown TAN fork was not retained after restart");
      expect(store.getTanOperation(harness.bot.uid, fork.id)?.phase).toBe("cancelled");
    });
  } finally {
    transport.releaseCreateThread.resolve();
    if (restarted) await closeCoreHarness(restarted);
    else await closeCoreHarness(harness);
  }
}, 15_000);

class NoAcceptanceWorkerFactory extends ForkTestWorkerFactory {
  override async create(options: WorkerOptions): Promise<OmpWorker> {
    const worker = await super.create(options);
    const prompt = worker.prompt.bind(worker);
    worker.prompt = async (text, onEvent, context) => {
      await prompt(text, event => {
        if (event.type !== "prompt_accepted") onEvent(event);
      }, context);
    };
    return worker;
  }
}

test("TAN prompt without a prompt_accepted receipt stays unknown instead of completed", async () => {
  const workers = new NoAcceptanceWorkerFactory();
  const harness = await createCoreHarness({
    transport: new ForkTestTransport(),
    workers,
    allowedGroupIds: [FORK_GROUP.channelId],
  });
  const target = targetFor("thread-1");
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-unknown-source-session");
    await harness.service.handleInbound(slashTanMessage(harness, 127n));
    await workers.waitForPromptCount(1);
    await waitForStatus(harness.service, target, status => status.state === "interrupted");

    await withStore(harness, store => {
      const fork = store.listThreadForks(harness.bot.uid)[0];
      expect(fork).toBeDefined();
      if (!fork) throw new Error("TAN fork record was not persisted");
      const task = store.getTaskByMessageId(
        harness.bot.uid,
        target,
        `dispatch:tan:${fork.id}`,
      );
      expect(task?.state).toBe("interrupted");
      expect(task?.errorText).toContain("无法确认");
      expect(store.getTanOperation(harness.bot.uid, fork.id)?.phase).toBe("done");
    });
  } finally {
    await closeCoreHarness(harness);
  }
}, 15_000);

test("mention-normalized TAN materialization is coalesced and cancellation leaves a durable terminal claim", async () => {
  const transport = new ForkTestTransport();
  transport.downloadHangs = true;
  const downloadStarted = Promise.withResolvers<void>();
  const download = transport.downloadAttachment.bind(transport);
  transport.downloadAttachment = request => {
    downloadStarted.resolve();
    return download(request);
  };
  const harness = await createCoreHarness({
    transport,
    allowedGroupIds: [FORK_GROUP.channelId],
    attachments: { enabled: true, maxBytes: 1024 },
  });
  const message = inbound({
    ...FORK_GROUP,
    messageId: "tan-mention-cancel-download",
    senderId: "owner-1",
    // Octo's inbound normalizer removes the structural @Bot span before service command parsing.
    text: "/tan inspect the attached file",
    mentionedUserIds: [harness.bot.uid],
    attachments: [{
      url: "https://cdn.example.test/tan-cancel.txt",
      kind: "file",
      name: "tan-cancel.txt",
      size: 12,
      durationSeconds: 0,
    }],
  });
  let first: Promise<void> | undefined;
  let duplicate: Promise<void> | undefined;
  try {
    await seedBinding(harness, FORK_GROUP, harness.root, "tan-cancel-source-session");
    const firstRequest = harness.service.handleInbound(message);
    first = firstRequest;
    await downloadStarted.promise;
    expect(transport.downloadRequests).toHaveLength(1);

    const duplicateRequest = harness.service.handleInbound(message);
    duplicate = duplicateRequest;
    await harness.service.handleInbound(ownerMessage(harness, FORK_GROUP, "tan-mention-cancel", "/cancel"));
    await Promise.all([firstRequest, duplicateRequest]);

    // Redelivery after cancellation is suppressed by the completed command claim, not re-materialized.
    await harness.service.handleInbound(message);
    expect(transport.downloadRequests).toHaveLength(1);
    await withStore(harness, store => {
      expect(store.getCommandClaimState(harness.bot.uid, FORK_GROUP, message.messageId)).toBe("completed");
      expect(store.listThreadForks(harness.bot.uid)).toHaveLength(0);
    });
  } finally {
    await Promise.allSettled([first, duplicate].filter((value): value is Promise<void> => value !== undefined));
    await closeCoreHarness(harness);
  }
}, 15_000);
