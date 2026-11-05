import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget, InboundMessage } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore, type ReferenceHistorySnapshot } from "../src/store/index.ts";
import type { InboundClaim, TaskRecord, TextStreamOutboxCommit } from "../src/store/store-records.ts";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import type { IngressConsumer } from "../src/inbound/ingress-types.ts";

const botUid = "reference-history-bot";
const target: ChatTarget = { channelType: 5, channelId: "reference-group____thread-current" };
const candidateUrl = "https://git.example/acme/project/-/merge_requests/42";
const similarUrl = "https://git.example/acme/project/-/merge_requests/420";
const fairnessCandidateUrl = "https://git.example/acme/project/-/merge_requests/77";
const fairnessOtherCandidateUrl = "https://git.example/acme/project/-/merge_requests/88";
type ReferenceHistoryEntry = { role: "user" | "assistant"; text: string; taskId: string | null };
type ReferenceHistoryStore = ChannelStore & {
  captureReferenceHistorySnapshot(botUid: string, target: ChatTarget): ReferenceHistorySnapshot;
  listReferenceHistory(
    botUid: string,
    target: ChatTarget,
    candidateUrls: readonly string[],
    snapshot?: ReferenceHistorySnapshot,
  ): ReferenceHistoryEntry[];
};

async function withState(work: (stateDir: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-reference-history-"));
  const stateDir = join(root, "state");
  try {
    await work(stateDir);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function addTask(
  store: ChannelStore,
  options: {
    id: string;
    botUid?: string;
    target?: ChatTarget;
    messageId: string;
    userText: string;
    sourceKind?: "recv" | "synthetic";
  },
): void {
  const taskTarget = options.target ?? target;
  const taskBotUid = options.botUid ?? botUid;
  const claim: InboundClaim = {
    botUid: taskBotUid,
    target: taskTarget,
    messageId: options.messageId,
    senderId: "owner-1",
    text: options.userText,
    supported: true,
    kind: "task",
  };
  const task: TaskRecord = {
    ...taskTarget,
    id: options.id,
    botUid: taskBotUid,
    messageId: options.messageId,
    senderId: "owner-1",
    // The worker body is deliberately distinct: reference history must return persisted visible
    // inbound text, not prompt materialization, system context, or attachment paths.
    text: `worker prompt for ${options.id}\n[system context]\n/tmp/materialized-attachment.txt`,
    sessionId: `session-${options.id}`,
    cwd: "/tmp/reference-history",
    state: "queued",
    resultText: null,
    errorText: null,
    messageSeq: 0,
    historyEligible: true,
    relaySuppressed: false,
  };
  expect(store.acceptTask(claim, task)).toBe(true);
  persistIngressHandoff(
    store,
    inboundMessage(taskTarget, taskBotUid, options.messageId, options.userText),
    taskBotUid,
    options.sourceKind ?? "recv",
    { kind: "task", id: task.id },
  );
}

function inboundMessage(
  messageTarget: ChatTarget,
  messageBotUid: string,
  messageId: string,
  text: string,
): InboundMessage {
  return {
    ...messageTarget,
    messageId,
    senderId: "owner-1",
    text,
    mentionedUserIds: [messageBotUid],
    attachments: [],
    quoted: null,
    supported: true,
    messageSeq: 1,
    messageTimestamp: Date.now(),
  };
}

function persistIngressHandoff(
  store: ChannelStore,
  message: InboundMessage,
  messageBotUid: string,
  sourceKind: "recv" | "synthetic",
  consumer: IngressConsumer,
  persistConsumer: () => boolean = () => true,
): void {
  const { job } = store.acceptIngressJob({
    botUid: messageBotUid,
    sourceKind,
    sourceId: sourceKind === "recv"
      ? recvIngressSourceId(message.channelType, message.channelId, message.messageId)
      : message.messageId,
    channelType: message.channelType,
    channelId: message.channelId,
    messageId: message.messageId,
    payload: sourceKind === "recv"
      ? { kind: "recv", message }
      : { kind: "synthetic", message },
    routeSnapshot: {
      purpose: { kind: "ordinary" },
      sessionGeneration: 0,
      binding: null,
      barrierJobId: null,
    },
    lane: "ordered",
    state: "accepted",
    ackReady: true,
    operationCheckpoint: null,
  });
  expect(store.handoffIngressJob(job.id, consumer, persistConsumer)).toBe(true);
}

function acceptSteering(store: ChannelStore, messageId: string, text: string): void {
  const message = inboundMessage(target, botUid, messageId, text);
  const claim: InboundClaim = {
    botUid,
    target,
    messageId,
    senderId: message.senderId,
    text,
    supported: true,
    kind: "command",
  };
  expect(store.claimCommand(claim)).toBe(true);
  persistIngressHandoff(store, message, botUid, "recv", { kind: "steering", id: messageId }, () => {
    store.completeCommand(claim);
    return true;
  });
}


function commitStream(
  store: ChannelStore,
  options: {
    taskId: string;
    botUid?: string;
    target?: ChatTarget;
    streamMessageId: string;
    segments: readonly { text: string; mention: null }[];
    final: boolean;
  },
): void {
  const commit: TextStreamOutboxCommit = {
    botUid: options.botUid ?? botUid,
    target: options.target ?? target,
    taskId: options.taskId,
    streamMessageId: options.streamMessageId,
    segments: options.segments,
    final: options.final,
    forceFinalSentinel: false,
  };
  expect(store.commitTextStream(commit)).toBe(true);
}

test("重开 SQLite 后仅当前 Bot/线程的候选 MR 历史返回原始要求与完整 final 报告", () =>
  withState(async (stateDir) => {
    const writer = await openChannelStore(stateDir);
    try {
      addTask(writer, {
        id: "candidate-old-task",
        messageId: "candidate-old-message",
        userText: `请审查这个 MR：${candidateUrl}`,
      });
      commitStream(writer, {
        taskId: "candidate-old-task",
        streamMessageId: "candidate-final-stream",
        // The canonical MR URL spans two persisted outbox segments; the detail URL is the same
        // reference as the canonical candidate URL.
        segments: [
          { text: "最终报告：请查看 https://git.example/acme/project/-/merge_requests/", mention: null },
          { text: "42/diffs?view=parallel。\n审查结论：变更符合要求。", mention: null },
        ],
        final: true,
      });

      for (let index = 0; index < 4; index += 1) {
        commitStream(writer, {
          taskId: "candidate-old-task",
          streamMessageId: `candidate-newer-stream-${index}`,
          segments: [{ text: `同任务后续 final ${index}`, mention: null }],
          final: true,
        });
      }
      acceptSteering(writer, "accepted-steering-message", `追加要求：也请查看 ${candidateUrl}`);


      addTask(writer, {
        id: "similar-old-task",
        messageId: "similar-old-message",
        userText: `请看另一个 MR：${similarUrl}`,
      });
      commitStream(writer, {
        taskId: "similar-old-task",
        streamMessageId: "similar-final-stream",
        segments: [{ text: `相似编号报告：${similarUrl}`, mention: null }],
        final: true,
      });

      addTask(writer, {
        id: "nonfinal-old-task",
        messageId: "nonfinal-old-message",
        userText: "请帮我检查普通代码风格。",
      });
      commitStream(writer, {
        taskId: "nonfinal-old-task",
        streamMessageId: "nonfinal-stream",
        segments: [{ text: `未完成草稿：${candidateUrl}`, mention: null }],
        final: false,
      });

      addTask(writer, {
        id: "notice-task",
        messageId: "notice-message",
        userText: "普通的用户要求，不含候选 URL。",
      });
      commitStream(writer, {
        taskId: "notice-task",
        streamMessageId: "notice-task:notice:notice-event",
        segments: [{ text: `OMP 通知文本：${candidateUrl}`, mention: null }],
        final: true,
      });

      addTask(writer, {
        id: "ftp-decoy-task",
        messageId: "ftp-decoy-message",
        userText: `镜像地址 ftp://files.example.invalid/${candidateUrl}`,
      });
      addTask(writer, {
        id: "watch-synthetic-task",
        messageId: "watch:watch-reference-id",
        sourceKind: "synthetic",
        userText: `系统 watch wake 正文：${candidateUrl}`,
      });
      addTask(writer, {
        id: "dispatch-synthetic-task",
        messageId: "dispatch:dispatch-reference-id",
        sourceKind: "synthetic",
        userText: `用户指派的子区任务：${candidateUrl}`,
      });

      addTask(writer, {
        id: "fairness-other-task",
        messageId: "fairness-other-message",
        userText: "请查看另一个持久报告。",
      });
      commitStream(writer, {
        taskId: "fairness-other-task",
        streamMessageId: "fairness-other-final",
        segments: [{ text: `候选报告：${fairnessOtherCandidateUrl}`, mention: null }],
        final: true,
      });
      for (let index = 0; index < 9; index += 1) {
        const taskId = `fairness-common-task-${index}`;
        addTask(writer, {
          id: taskId,
          messageId: `fairness-common-message-${index}`,
          userText: `候选报告请求：${fairnessCandidateUrl}`,
        });
        commitStream(writer, {
          taskId,
          streamMessageId: `fairness-common-final-${index}`,
          segments: [{ text: `候选报告：${fairnessCandidateUrl}`, mention: null }],
          final: true,
        });
      }

      // Bury the matching and similar-number tasks outside ordinary recent history. The matching
      // old report and its own inbound request must still be found by the candidate URL.
      for (let index = 0; index < 40; index += 1) {
        const id = `recent-task-${index}`;
        addTask(writer, {
          id,
          messageId: `recent-message-${index}`,
          userText: `近期可见用户消息 ${index}`,
        });
      }

      // Recent user inputs alone must not leave an older similar-number final response inside
      // the independent recent-assistant window.
      for (const index of [36, 37, 38, 39]) {
        commitStream(writer, {
          taskId: `recent-task-${index}`,
          streamMessageId: `recent-final-stream-${index}`,
          segments: [{ text: `近期无关最终回复 ${index}`, mention: null }],
          final: true,
        });
      }

      addTask(writer, {
        id: "other-bot-task",
        botUid: "reference-history-other-bot",
        messageId: "other-bot-message",
        userText: `其他 Bot 的同 URL 要求：${candidateUrl}`,
      });
      commitStream(writer, {
        botUid: "reference-history-other-bot",
        taskId: "other-bot-task",
        streamMessageId: "other-bot-final-stream",
        segments: [{ text: `其他 Bot 的报告：${candidateUrl}`, mention: null }],
        final: true,
      });

      const otherThread: ChatTarget = { channelType: 5, channelId: "reference-group____thread-other" };
      addTask(writer, {
        id: "other-thread-task",
        target: otherThread,
        messageId: "other-thread-message",
        userText: `同 Bot 其他子区的同 URL 要求：${candidateUrl}`,
      });
      commitStream(writer, {
        target: otherThread,
        taskId: "other-thread-task",
        streamMessageId: "other-thread-final-stream",
        segments: [{ text: `其他子区的报告：${candidateUrl}`, mention: null }],
        final: true,
      });

      for (const taskId of [
        "candidate-old-task",
        "similar-old-task",
        "notice-task",
        "fairness-other-task",
        ...Array.from({ length: 9 }, (_, index) => `fairness-common-task-${index}`),
        ...[36, 37, 38, 39].map((index) => `recent-task-${index}`),
        "other-bot-task",
        "other-thread-task",
      ]) {
        writer.markCompleted(taskId, null);
      }
    } finally {
      writer.close();
    }
    // A newly opened store reads the persisted inbound/outbox evidence, rather than in-memory
    // stream state from the writer connection.
    const reopened = await openChannelStore(stateDir);
    try {
      const historyStore = reopened as ReferenceHistoryStore;
      const history = historyStore.listReferenceHistory(botUid, target, [candidateUrl]);

      expect(history).toContainEqual({
        role: "user",
        text: `请审查这个 MR：${candidateUrl}`,
        taskId: "candidate-old-task",
      });
      expect(history).toContainEqual({
        role: "user",
        text: `追加要求：也请查看 ${candidateUrl}`,
        taskId: null,
      });
      expect(history).toContainEqual({
        role: "assistant",
        text: "最终报告：请查看 https://git.example/acme/project/-/merge_requests/42/diffs?view=parallel。\n审查结论：变更符合要求。",
        taskId: "candidate-old-task",
      });
      expect(history).toContainEqual({
        role: "user",
        text: "近期可见用户消息 39",
        taskId: "recent-task-39",
      });
      expect(history.some((entry) => entry.taskId === "similar-old-task")).toBe(false);
      expect(history.some((entry) => entry.taskId === "nonfinal-old-task" && entry.role === "assistant")).toBe(false);
      expect(history.some((entry) => entry.text.includes("未完成草稿"))).toBe(false);
      expect(history.some((entry) => entry.taskId === "other-bot-task")).toBe(false);
      expect(history.some((entry) => entry.taskId === "other-thread-task")).toBe(false);
      expect(history.every((entry) => !entry.text.includes("worker prompt") && !entry.text.includes("materialized-attachment"))).toBe(true);
      expect(history).toContainEqual({
        role: "user",
        text: `用户指派的子区任务：${candidateUrl}`,
        taskId: "dispatch-synthetic-task",
      });
      expect(history.some((entry) => entry.taskId === "watch-synthetic-task")).toBe(false);
      expect(history.some((entry) => entry.taskId === "notice-task")).toBe(false);
      expect(history.some((entry) => entry.taskId === "ftp-decoy-task")).toBe(false);

      const fairHistory = historyStore.listReferenceHistory(botUid, target, [
        fairnessCandidateUrl,
        fairnessOtherCandidateUrl,
      ]);
      expect(fairHistory).toContainEqual({
        role: "user",
        text: "请查看另一个持久报告。",
        taskId: "fairness-other-task",
      });
      expect(fairHistory).toContainEqual({
        role: "assistant",
        text: `候选报告：${fairnessOtherCandidateUrl}`,
        taskId: "fairness-other-task",
      });
    } finally {
      reopened.close();
    }
  }));

test("历史快照保留已受理输入但排除快照后更新的助手结果", () =>
  withState(async (stateDir) => {
    const store = await openChannelStore(stateDir);
    try {
      const currentTaskId = "snapshot-current-task";
      const currentText = `当前审查要求：${candidateUrl}`;
      addTask(store, {
        id: currentTaskId,
        messageId: "snapshot-current-message",
        userText: currentText,
      });
      commitStream(store, {
        taskId: currentTaskId,
        streamMessageId: "snapshot-current-final",
        segments: [{ text: "尚未完成的流式草稿", mention: null }],
        final: false,
      });

      const historyStore = store as ReferenceHistoryStore;
      const snapshot = historyStore.captureReferenceHistorySnapshot(botUid, target);

      commitStream(store, {
        taskId: currentTaskId,
        streamMessageId: "snapshot-current-final",
        segments: [{ text: `未来助手结果：${candidateUrl}`, mention: null }],
        final: true,
      });
      store.markCompleted(currentTaskId, null);

      const futureTaskId = "snapshot-future-task";
      addTask(store, {
        id: futureTaskId,
        messageId: "snapshot-future-message",
        userText: `快照后要求：${candidateUrl}`,
      });
      commitStream(store, {
        taskId: futureTaskId,
        streamMessageId: "snapshot-future-final",
        segments: [{ text: `快照后助手结果：${candidateUrl}`, mention: null }],
        final: true,
      });
      store.markCompleted(futureTaskId, null);

      const history = historyStore.listReferenceHistory(botUid, target, [candidateUrl], snapshot);
      expect(history).toContainEqual({
        role: "user",
        text: currentText,
        taskId: currentTaskId,
      });
      expect(history.some((entry) => entry.taskId === currentTaskId && entry.role === "assistant")).toBe(false);
      expect(history.some((entry) => entry.taskId === futureTaskId)).toBe(false);
    } finally {
      store.close();
    }
  }));
