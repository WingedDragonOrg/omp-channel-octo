import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { attachmentDirectory } from "../src/inbound/attachments.ts";
import type { AttachmentDownload } from "../src/contracts.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes.ts";

const MAX_BYTES = 1024 * 1024;
const FILE_BYTES = new Uint8Array([16, 32, 48, 64, 80, 96, 112, 128]);

/**
 * 停机中断仍处于准备阶段的入站必须拒绝 ackReady：传输层不能确认一条尚未可靠受理的消息。
 * 状态盘已关闭一类的错误不算有界取消。
 */
const SHUTDOWN_FLAVORED = /(stopping|shutdown|停止|正在停止|cancell?ed|aborted?)/iu;
const DATABASE_CLOSED = /(sqlite|database|closed)/iu;

function sentToChat(harness: CoreHarness, target: { channelId: string; channelType: number }) {
  return harness.transport.sent.filter(({ target: sentTarget }) =>
    sentTarget.channelId === target.channelId && sentTarget.channelType === target.channelType,
  );
}

function attemptsToChat(harness: CoreHarness, target: { channelId: string; channelType: number }) {
  return harness.transport.attempts.filter(({ target: attemptTarget }) =>
    attemptTarget.channelId === target.channelId && attemptTarget.channelType === target.channelType,
  );
}

interface InboundOutcome {
  readonly kind: "resolved" | "rejected";
  readonly error?: unknown;
}

interface ObservedInbound {
  /** 始终解决的观察链；真正的兑现结果经 `outcome()` 取回。 */
  readonly settled: Promise<void>;
  outcome(): InboundOutcome | null;
}

/** 立即挂接拒绝捕获，杜绝未处理拒绝；`outcome()` 在入站了结前恒为 null。 */
function observeInbound(promise: Promise<void>): ObservedInbound {
  let outcome: InboundOutcome | null = null;
  const settled = promise.then(
    () => {
      outcome = { kind: "resolved" };
    },
    (error: unknown) => {
      outcome = { kind: "rejected", error };
    },
  );
  return { settled, outcome: () => outcome };
}

/** Shutdown releases the attachment claim before closing the store, permitting redelivery. */
test("stop 中止在途附件下载：入站以有界取消拒绝、claim 回滚，重启后同一消息恰好执行一次", async () => {
  const first = await createCoreHarness({
    attachments: { enabled: true, maxBytes: MAX_BYTES },
  });
  const url = "https://cdn.example.test/report.bin";
  first.transport.attachmentBodies.set(url, FILE_BYTES);
  // The explicit release also lets teardown finish if shutdown cancellation regresses.
  const enteredDownload = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const originalDownload = first.transport.downloadAttachment.bind(first.transport);
  first.transport.downloadAttachment = async (request: AttachmentDownload): Promise<number> => {
    enteredDownload.resolve();
    await Promise.race([
      releaseDownload.promise,
      new Promise<never>((_resolve, reject) => {
        if (request.signal?.aborted) reject(new Error("fixture attachment download aborted by shutdown"));
        else {
          request.signal?.addEventListener(
            "abort",
            () => reject(new Error("fixture attachment download aborted by shutdown")),
            { once: true },
          );
        }
      }),
    ]);
    return originalDownload(request);
  };
  const message = inbound({
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [first.bot.uid],
    messageId: "shutdown-attachment-1",
    text: "总结这份文件",
    attachments: [
      { url, kind: "file", name: "report.bin", size: FILE_BYTES.byteLength, durationSeconds: 0 },
    ],
  });
  let observed: ObservedInbound | undefined;
  let second: CoreHarness | undefined;
  try {
    observed = observeInbound(first.service.submitInbound(message));
    await enteredDownload.promise;
    await first.service.stop();
    // stop 必须等未受理入站清理完（含组装中断与命令 claim 回滚）才关闭状态盘。
    expect(observed.outcome()).not.toBeNull();
    const outcome = observed.outcome()!;
    // 准备阶段尚未可靠受理，因此 ackReady 必须拒绝，且是有界取消而不是数据库已关闭。
    expect(outcome.kind).toBe("rejected");
    const errorText = outcome.error instanceof Error
      ? `${outcome.error.name}: ${outcome.error.message}`
      : String(outcome.error);
    expect(errorText).not.toMatch(DATABASE_CLOSED);
    expect(errorText).toMatch(SHUTDOWN_FLAVORED);
    // 入站未接受，因此没有派工，也没有向被拒绝的聊天发送业务回复；owner 启停通知是独立事件。
    expect(sentToChat(first, message)).toHaveLength(0);
    expect(attemptsToChat(first, message)).toHaveLength(0);
    // 中断清理先于状态盘关闭完成：临时附件目录已不存在。
    expect(existsSync(attachmentDirectory(first.stateDir, message, message.messageId))).toBe(false);

    // 命令 claim 随中断回滚：同一消息在重启后的服务上恰好执行一次。
    second = await createCoreHarness({
      root: first.root,
      attachments: { enabled: true, maxBytes: MAX_BYTES },
    });
    second.transport.attachmentBodies.set(url, FILE_BYTES);
    await second.service.submitInbound(message);
    await second.workers.waitForPromptCount(1);
    await waitForStatus(
      second.service,
      { channelId: message.channelId, channelType: message.channelType },
      (status) => status.state === "completed",
    );
    expect(second.workers.prompts).toHaveLength(1);
    expect(second.workers.prompts[0]!.text).toContain("report.bin");
  } finally {
    // 自有门禁一律在 finally 放行：先兑现中止支路时这是无害的 no-op。
    releaseDownload.resolve();
    await observed?.settled;
    if (second !== undefined) {
      await closeCoreHarness(second);
    } else {
      await closeCoreHarness(first);
    }
  }
});

/**
 * 同一生命周期契约的普通入站面：一条不带附件的群消息被免@查询（普通入站路径上真实的异
 * 步门禁）挂住时 stop，同样必须先中止并拒绝尚未兑现的 ackReady、等清理完再关状态盘。
 * 查询返回「需要 @」而悄悄丢弃也是同一类丢失。
 */
test("stop 中止普通入站的异步门禁：有界取消拒绝而非静默 ACK，重启后重投恰好执行一次", async () => {
  const first = await createCoreHarness();
  const enteredMentionPref = Promise.withResolvers<void>();
  const releaseMentionPref = Promise.withResolvers<void>();
  first.transport.getGroupMentionPref = async (_groupId, signal) => {
    enteredMentionPref.resolve();
    await Promise.race([
      releaseMentionPref.promise,
      new Promise<never>((_resolve, reject) => {
        if (signal?.aborted) reject(new Error("fixture mention preference aborted by shutdown"));
        else {
          signal?.addEventListener(
            "abort",
            () => reject(new Error("fixture mention preference aborted by shutdown")),
            { once: true },
          );
        }
      }),
    ]);
    return { effective: true };
  };
  const message = inbound({
    channelId: "group-allowed",
    channelType: 2,
    messageId: "shutdown-ordinary-1",
    text: "整理一下群里的要点",
    mentionedUserIds: [],
  });
  let observed: ObservedInbound | undefined;
  let second: CoreHarness | undefined;
  try {
    observed = observeInbound(first.service.submitInbound(message));
    await enteredMentionPref.promise;
    await first.service.stop();
    expect(observed.outcome()).not.toBeNull();
    const outcome = observed.outcome()!;
    expect(outcome.kind).toBe("rejected");
    const errorText = outcome.error instanceof Error
      ? `${outcome.error.name}: ${outcome.error.message}`
      : String(outcome.error);
    expect(errorText).not.toMatch(DATABASE_CLOSED);
    expect(errorText).toMatch(SHUTDOWN_FLAVORED);
    expect(sentToChat(first, message)).toHaveLength(0);
    expect(attemptsToChat(first, message)).toHaveLength(0);

    const restarted = await createCoreHarness({ root: first.root });
    second = restarted;
    restarted.transport.setGroupMentionPref("group-allowed", { effective: true });
    await restarted.service.submitInbound(message);
    await restarted.workers.waitForPromptCount(1);
    await waitForStatus(
      restarted.service,
      { channelId: message.channelId, channelType: message.channelType },
      (status) => status.state === "completed",
    );
    expect(restarted.workers.prompts).toHaveLength(1);
    expect(restarted.workers.prompts[0]!.text).toContain("整理一下群里的要点");
  } finally {
    releaseMentionPref.resolve();
    await observed?.settled;
    if (second !== undefined) {
      await closeCoreHarness(second);
    } else {
      await closeCoreHarness(first);
    }
  }
});

/**
 * 停机窗口内新到达的入站同样不能以静默 resolve 被 ACK：进入即拒绝、有界取消风味，
 * 不写历史、不建任务，也不向被拒绝的群聊天发送业务回复；owner 启停通知是独立事件。
 */
test("stop 之后到达的入站立即以有界取消拒绝，不产生任何副作用", async () => {
  const harness = await createCoreHarness();
  try {
    await harness.service.stop();
    const observed = observeInbound(
      harness.service.submitInbound(
        inbound({
          channelId: "group-allowed",
          channelType: 2,
          mentionedUserIds: [harness.bot.uid],
          messageId: "shutdown-late-arrival-1",
          text: "停机后到达的消息",
        }),
      ),
    );
    await observed.settled;
    const outcome = observed.outcome();
    expect(outcome).not.toBeNull();
    expect(outcome!.kind).toBe("rejected");
    const errorText = outcome!.error instanceof Error
      ? `${outcome!.error.name}: ${outcome!.error.message}`
      : String(outcome!.error);
    expect(errorText).not.toMatch(DATABASE_CLOSED);
    expect(errorText).toMatch(SHUTDOWN_FLAVORED);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(sentToChat(harness, { channelId: "group-allowed", channelType: 2 })).toHaveLength(0);
    expect(attemptsToChat(harness, { channelId: "group-allowed", channelType: 2 })).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a mention decision resolving after stop cannot acknowledge unaccepted input", async () => {
  const harness = await createCoreHarness();
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  harness.transport.getGroupMentionPref = async () => {
    entered.resolve();
    await release.promise;
    return { effective: false };
  };
  let observed: ObservedInbound | undefined;
  try {
    observed = observeInbound(harness.service.submitInbound(inbound({
      channelType: 2,
      channelId: "group-allowed",
      mentionedUserIds: [],
      text: "ordinary input",
    })));
    await entered.promise;
    const stopping = harness.service.stop();
    release.resolve();
    await stopping;
    expect(observed.outcome()?.kind).toBe("rejected");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(sentToChat(harness, { channelId: "group-allowed", channelType: 2 })).toHaveLength(0);
  } finally {
    release.resolve();
    await observed?.settled;
    await closeCoreHarness(harness);
  }
});

test("a download resolving during stop cleans the newly materialized file", async () => {
  const harness = await createCoreHarness({ attachments: { enabled: true, maxBytes: MAX_BYTES } });
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const url = "https://cdn.example.test/late.bin";
  harness.transport.attachmentBodies.set(url, FILE_BYTES);
  const download = harness.transport.downloadAttachment.bind(harness.transport);
  harness.transport.downloadAttachment = async ({ signal: _signal, ...request }) => {
    entered.resolve();
    await release.promise;
    // The completed I/O result wins its race with cancellation.
    return download(request);
  };
  const message = inbound({
    channelId: "group-allowed",
    channelType: 2,
    mentionedUserIds: [harness.bot.uid],
    text: "late attachment",
    attachments: [{ url, kind: "file", name: "late.bin", size: FILE_BYTES.length, durationSeconds: 0 }],
  });
  let observed: ObservedInbound | undefined;
  try {
    observed = observeInbound(harness.service.submitInbound(message));
    await entered.promise;
    const stopping = harness.service.stop();
    release.resolve();
    await stopping;
    expect(observed.outcome()?.kind).toBe("rejected");
    expect(existsSync(attachmentDirectory(harness.stateDir, message, message.messageId))).toBe(false);
    expect(harness.workers.prompts).toHaveLength(0);
    expect(sentToChat(harness, message)).toHaveLength(0);
  } finally {
    release.resolve();
    await observed?.settled;
    await closeCoreHarness(harness);
  }
});
