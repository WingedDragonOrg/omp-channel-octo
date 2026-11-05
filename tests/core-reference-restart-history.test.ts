import { expect, test } from "bun:test";
import { mkdtemp, mkdir, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChannelService, InboundMessage } from "../src/contracts.ts";
import { createChannelService } from "../src/service.ts";
import type { DecisionFetch, MatchedReferenceHistory, ReferenceEvidence } from "../src/references/types.ts";
import { openChannelStore } from "../src/store/index.ts";
import { assistantText, FakeTransport, FakeWorkerFactory, inbound } from "./fixtures/core-fakes.ts";

type DecisionEvidence = ReferenceEvidence & { matchedHistory: readonly MatchedReferenceHistory[] };

function isDecisionEvidence(value: unknown): value is DecisionEvidence {
  if (!isReferenceEvidence(value) || !("matchedHistory" in value) || !Array.isArray(value.matchedHistory)) {
    return false;
  }
  return value.matchedHistory.every(history =>
    history !== null
    && typeof history === "object"
    && "candidateId" in history
    && typeof history.candidateId === "string"
    && "url" in history
    && typeof history.url === "string"
    && "taskRequirement" in history
    && typeof history.taskRequirement === "string"
    && "reportExcerpt" in history
    && typeof history.reportExcerpt === "string",
  );
}

const target = { channelType: 5 as const, channelId: "group-tools____restart-history" };
const mergeRequest = "https://gitlab.example/group/service/-/merge_requests/41";
const secondMergeRequest = "https://gitlab.example/group/service/-/merge_requests/42";
const thirdMergeRequest = "https://gitlab.example/group/service/-/merge_requests/43";

function isReferenceEvidence(value: unknown): value is ReferenceEvidence {
  if (
    value === null
    || typeof value !== "object"
    || !("thread" in value)
    || typeof value.thread !== "string"
    || !("task" in value)
    || typeof value.task !== "string"
    || !("recent" in value)
    || !Array.isArray(value.recent)
    || !("candidates" in value)
    || !Array.isArray(value.candidates)
  ) {
    return false;
  }
  return value.recent.every(message =>
    message !== null
    && typeof message === "object"
    && "role" in message
    && (message.role === "user" || message.role === "assistant")
    && "text" in message
    && typeof message.text === "string",
  ) && value.candidates.every(candidate =>
    candidate !== null
    && typeof candidate === "object"
    && "id" in candidate
    && typeof candidate.id === "string"
    && "kind" in candidate
    && (candidate.kind === "mr" || candidate.kind === "pr")
    && "url" in candidate
    && typeof candidate.url === "string"
    && "excerpt" in candidate
    && typeof candidate.excerpt === "string",
  );
}

function evidenceFrom(body: Record<string, unknown>): DecisionEvidence {
  const parsed: unknown = JSON.parse(String(body.input));
  if (
    parsed === null
    || typeof parsed !== "object"
    || !("evidence" in parsed)
    || !isDecisionEvidence(parsed.evidence)
  ) {
    throw new Error("Decision request omitted valid evidence");
  }
  return parsed.evidence;
}

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await predicate()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function waitForSignal(promise: Promise<void>, description: string): Promise<void> {
  let finished = false;
  const timeout = Promise.withResolvers<never>();
  void (async () => {
    const deadline = performance.now() + 3_000;
    while (!finished && performance.now() < deadline) {
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    if (!finished) timeout.reject(new Error(`Timed out waiting for ${description}`));
  })();
  try {
    await Promise.race([promise, timeout.promise]);
  } finally {
    finished = true;
  }
}

async function submitAndWaitForNew(
  service: ChannelService,
  botUid: string,
  messageId: string,
  root: string,
): Promise<void> {
  const previous = await service.status(target);
  const expectedCwd = await realpath(root);
  await waitForSignal(service.submitInbound(inbound({
    ...target,
    messageId,
    text: `/new ${root}`,
    mentionedUserIds: [botUid],
  })), `new-session receipt ${messageId}`);
  await waitForCondition(async () => {
    const status = await service.status(target);
    return status.cwd === expectedCwd && status.sessionId !== previous.sessionId;
  }, `new session ${messageId}`);
}

async function submitAndWaitForTask(
  service: ChannelService,
  workers: FakeWorkerFactory,
  message: InboundMessage,
): Promise<void> {
  const promptCount = workers.prompts.length + 1;
  await waitForSignal(service.submitInbound(message), `inbound receipt ${message.messageId}`);
  await waitForCondition(() => workers.prompts.length >= promptCount, `worker prompt ${message.messageId}`);
  await waitForCondition(async () => {
    const status = await service.status(target);
    return status.state === "completed" && status.queued === 0;
  }, `task completion ${message.messageId}`);
}

test("restores source task and assistant final for a multi-MR Decision after service restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-restart-history-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const originalRequirement = "ORIGINAL_REQUIREMENT_BEGIN preserve nested alias validation while migrating the YAML parser ORIGINAL_REQUIREMENT_END";
  const assistantReport = `FULL_ASSISTANT_REPORT_BEGIN nested aliases were verified with focused regression coverage FULL_ASSISTANT_REPORT_END ${mergeRequest}/diffs?token=fixture-private-token`;
  const secondRequirement = "SECOND_REQUIREMENT_BEGIN preserve the parser's error contract while migrating its metadata checks SECOND_REQUIREMENT_END";
  const secondReport = `SECOND_FULL_REPORT_BEGIN migrated metadata checks and verified the error contract SECOND_FULL_REPORT_END ${secondMergeRequest}/diffs`;
  const thirdRequirement = "THIRD_REQUIREMENT_BEGIN add regression coverage for unsupported project metadata THIRD_REQUIREMENT_END";
  const thirdReport = `THIRD_FULL_REPORT_BEGIN exercised unsupported metadata and confirmed the fallback THIRD_FULL_REPORT_END ${thirdMergeRequest}/diffs`;
  const postRestartMarker = "POST_RESTART_MULTI_MR_REQUEST";
  const postRestartBackground = "POST_RESTART_BACKGROUND: the compatibility report is authoritative.";
  const requiredEvidenceByUrl: Record<string, { taskRequirement: string; reportStart: string; reportEnd: string }> = {
    [mergeRequest]: {
      taskRequirement: originalRequirement,
      reportStart: "FULL_ASSISTANT_REPORT_BEGIN",
      reportEnd: "FULL_ASSISTANT_REPORT_END",
    },
    [secondMergeRequest]: {
      taskRequirement: secondRequirement,
      reportStart: "SECOND_FULL_REPORT_BEGIN",
      reportEnd: "SECOND_FULL_REPORT_END",
    },
    [thirdMergeRequest]: {
      taskRequirement: thirdRequirement,
      reportStart: "THIRD_FULL_REPORT_BEGIN",
      reportEnd: "THIRD_FULL_REPORT_END",
    },
  };
  const reportByRequirement: Record<string, string> = {
    [originalRequirement]: assistantReport,
    [secondRequirement]: secondReport,
    [thirdRequirement]: thirdReport,
  };
  const failureUrl = "https://gitlab.example/group/service/-/merge_requests/99";
  const requests: Array<{ evidence: DecisionEvidence }> = [];
  const failureStarted = Promise.withResolvers<void>();
  const releaseFailure = Promise.withResolvers<void>();
  const failureFinished = Promise.withResolvers<void>();
  const releaseContextRead = Promise.withResolvers<void>();
  const timeline: string[] = [];
  let failureResponseSent = false;
  let delayNextContextRead = false;
  let contextReadStarted = false;
  let firstService: ChannelService | undefined;
  let restartedService: ChannelService | undefined;

  const transport = new FakeTransport({ bot: { uid: "reference-restart-bot", name: "Reference Bot" } });
  transport.setThreads("group-tools", [{
    shortId: "restart-history",
    groupId: "group-tools",
    channelId: target.channelId,
    name: "Restart history",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-05",
    updatedAt: "2026-11-05",
  }]);
  transport.setGroupContext(target, {
    content: "INITIAL_BACKGROUND: keep parser compatibility.",
    version: 1,
    updatedAt: null,
    updatedBy: "owner-1",
  });
  const realGetGroupContext = transport.getGroupContext.bind(transport);
  transport.getGroupContext = async readTarget => {
    const delayed = delayNextContextRead && readTarget.channelId === target.channelId;
    if (delayed) {
      delayNextContextRead = false;
      contextReadStarted = true;
      timeline.push("context-read-started");
      await releaseContextRead.promise;
    }
    const context = await realGetGroupContext(readTarget);
    if (delayed) timeline.push("context-read-completed");
    return context;
  };

  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      const evidence = evidenceFrom(body);
      const serialized = JSON.stringify(evidence);
      const currentUserText = evidence.recent
        .filter(message => message.role === "user")
        .map(message => message.text)
        .join("\n");
      requests.push({ evidence });
      if (serialized.includes(postRestartMarker)) timeline.push("post-restart-decision-requested");

      if (evidence.candidates.some(candidate => candidate.url === failureUrl)) {
        failureStarted.resolve();
        await releaseFailure.promise;
        failureResponseSent = true;
        failureFinished.resolve();
        return Response.json({ error: "fixture Decision service unavailable" }, { status: 503 });
      }

      const questions = body.questions as Array<{ name: string }>;
      return Response.json({
        model: "fixture-model",
        answers: questions.map(question => {
          const candidate = evidence.candidates.find(item => item.id === question.name);
          const requiredEvidence = candidate === undefined ? undefined : requiredEvidenceByUrl[candidate.url];
          const relatedHistory = evidence.matchedHistory.find(history =>
            history.candidateId === question.name && history.url === candidate?.url,
          );
          const workObject = candidate !== undefined
            && requiredEvidence !== undefined
            && relatedHistory !== undefined
            && relatedHistory.taskRequirement.includes(requiredEvidence.taskRequirement)
            && relatedHistory.reportExcerpt.includes(requiredEvidence.reportStart)
            && relatedHistory.reportExcerpt.includes(requiredEvidence.reportEnd)
            && relatedHistory.reportExcerpt.includes("[matched candidate URL omitted]")
            && currentUserText.includes(postRestartMarker)
            && serialized.includes(postRestartBackground);
          return {
            name: question.name,
            type: "choice",
            choice: workObject ? "work_object" : "insufficient_evidence",
            confidence: workObject ? 0.76 : 0.99,
            probabilities: [
              { value: "work_object", probability: workObject ? 0.76 : 0.005 },
              { value: "reference_or_unrelated", probability: workObject ? 0.12 : 0.005 },
              { value: "insufficient_evidence", probability: workObject ? 0.12 : 0.99 },
            ],
          };
        }),
      });
    },
  });

  const decisionsFetch: DecisionFetch = (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    return fetch(`${server.url.origin}${new URL(url).pathname}`, init);
  };
  const config = {
    ownerId: "owner-1",
    allowedGroupIds: ["group-tools"],
    stateDir,
    homeDir: root,
    decisions: {
      enabled: true as const,
      protocol: "openai-decisions" as const,
      baseUrl: "https://decision.example",
      model: "fixture-model",
      apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
      apiKey: "fixture-decision-key",
    },
  };
  const createWorkers = (): FakeWorkerFactory => new FakeWorkerFactory(async ({ text, emit }) => {
    emit(assistantText(
      `restart-${text}`,
      reportByRequirement[text] ?? "Fixture task completed.",
      true,
    ));
  });
  const createService = (workerFactory: FakeWorkerFactory) => createChannelService({
    config,
    transport,
    workerFactory,
    decisionsFetch,
  });

  const firstWorkers = createWorkers();
  try {
    const initialService = await createService(firstWorkers);
    firstService = initialService;
    await initialService.start();
    await submitAndWaitForNew(initialService, transport.bot.uid, "restart-history-initial-bind", root);
    for (const [messageId, requirement, url] of [
      ["restart-history-source-a", originalRequirement, mergeRequest],
      ["restart-history-source-b", secondRequirement, secondMergeRequest],
      ["restart-history-source-c", thirdRequirement, thirdMergeRequest],
    ] as const) {
      await submitAndWaitForTask(initialService, firstWorkers, inbound({
        ...target,
        messageId,
        text: requirement,
        mentionedUserIds: [transport.bot.uid],
      }));
      await waitForCondition(
        () => requests.some(({ evidence }) => evidence.candidates.some(candidate => candidate.url === url)),
        `initial assistant-final decision for ${url}`,
      );
    }

    const failureMessage = inbound({
      ...target,
      messageId: "restart-history-failing-decision",
      text: `This Decision failure must not block the chat: ${failureUrl}`,
      mentionedUserIds: [transport.bot.uid],
    });
    await waitForSignal(initialService.submitInbound(failureMessage), "failing Decision inbound receipt");
    await waitForSignal(failureStarted.promise, "held failing Decision request");
    await waitForCondition(
      () => firstWorkers.prompts.some(prompt => prompt.text.includes(failureUrl)),
      "task prompt while Decision is held",
    );
    await waitForCondition(async () => {
      const status = await initialService.status(target);
      return status.state === "completed" && status.queued === 0;
    }, "chat task completion before Decision failure response");
    expect(failureResponseSent).toBe(false);
    releaseFailure.resolve();
    await waitForSignal(failureFinished.promise, "failing Decision response");
    await initialService.stop();
    firstService = undefined;

    const beforeRestartStore = await openChannelStore(stateDir);
    try {
      expect(beforeRestartStore.getThreadReferences(transport.bot.uid, target)).toEqual({ mr: [], pr: [] });
    } finally {
      beforeRestartStore.close();
    }

    const restartedWorkers = firstWorkers;
    const restarted = await createService(restartedWorkers);
    restartedService = restarted;
    await restarted.start();
    await submitAndWaitForNew(restarted, transport.bot.uid, "restart-history-rebind", root);
    transport.setGroupContext(target, {
      content: postRestartBackground,
      version: 2,
      updatedAt: "2026-11-05T00:00:00.000Z",
      updatedBy: "owner-1",
    });
    delayNextContextRead = true;
    const currentRequest = `${postRestartMarker}: inspect these three merge requests:\n${"x".repeat(4_200)}\nMR_41_NEARBY ${mergeRequest}\nMR_42_NEARBY ${secondMergeRequest}\nMR_43_NEARBY ${thirdMergeRequest}`;
    const expectedPromptCount = restartedWorkers.prompts.length + 1;
    timeline.push("post-restart-inbound-submitted");
    await waitForSignal(restarted.submitInbound(inbound({
      ...target,
      messageId: "restart-history-current-list",
      text: currentRequest,
      mentionedUserIds: [transport.bot.uid],
    })), "post-restart multi-MR inbound receipt");
    await waitForCondition(() => contextReadStarted, "post-restart background read after observation");
    releaseContextRead.resolve();
    await waitForCondition(
      () => restartedWorkers.prompts.length >= expectedPromptCount,
      "post-restart task prompt",
    );
    await waitForCondition(async () => {
      const status = await restarted.status(target);
      return status.state === "completed" && status.queued === 0;
    }, "post-restart task completion");
    await waitForCondition(
      () => requests.some(({ evidence }) => JSON.stringify(evidence).includes(postRestartMarker)),
      "post-restart multi-MR Decision request",
    );

    const postRestartEvidence = requests.find(({ evidence }) => JSON.stringify(evidence).includes(postRestartMarker))!.evidence;
    expect(postRestartEvidence.candidates.map(candidate => candidate.url)).toEqual([mergeRequest, secondMergeRequest, thirdMergeRequest]);
    expect(postRestartEvidence.candidates[0]?.excerpt).toContain("MR_41_NEARBY");
    expect(postRestartEvidence.candidates[1]?.excerpt).toContain("MR_42_NEARBY");
    expect(postRestartEvidence.candidates[2]?.excerpt).toContain("MR_43_NEARBY");
    const matchedHistory = postRestartEvidence.matchedHistory;
    expect(matchedHistory).toHaveLength(3);
    const historyByUrl = new Map(matchedHistory.map(history => [history.url, history]));
    for (const [url, requirement, reportStart, reportEnd] of [
      [mergeRequest, originalRequirement, "FULL_ASSISTANT_REPORT_BEGIN", "FULL_ASSISTANT_REPORT_END"],
      [secondMergeRequest, secondRequirement, "SECOND_FULL_REPORT_BEGIN", "SECOND_FULL_REPORT_END"],
      [thirdMergeRequest, thirdRequirement, "THIRD_FULL_REPORT_BEGIN", "THIRD_FULL_REPORT_END"],
    ] as const) {
      const history = historyByUrl.get(url);
      const candidate = postRestartEvidence.candidates.find(item => item.url === url);
      expect(history).toBeDefined();
      expect(history?.candidateId).toBe(candidate?.id);
      expect(history?.taskRequirement).toContain(requirement);
      expect(history?.reportExcerpt).toContain(reportStart);
      expect(history?.reportExcerpt).toContain(reportEnd);
      expect(history?.url).toBe(url);
      expect(history?.reportExcerpt).toContain("[matched candidate URL omitted]");
      expect(history?.reportExcerpt).not.toContain(url);
    }
    const serializedEvidence = JSON.stringify(postRestartEvidence);
    expect(serializedEvidence).toContain(postRestartBackground);
    expect(serializedEvidence).not.toContain("fixture-private-token");
    expect(postRestartEvidence.recent.some(message => message.role === "user" && message.text.includes(postRestartMarker))).toBe(true);
    const submittedIndex = timeline.indexOf("post-restart-inbound-submitted");
    const contextStartedIndex = timeline.indexOf("context-read-started");
    const contextCompletedIndex = timeline.indexOf("context-read-completed");
    const decisionRequestedIndex = timeline.indexOf("post-restart-decision-requested");
    expect(submittedIndex).toBeGreaterThanOrEqual(0);
    expect(contextStartedIndex).toBeGreaterThan(submittedIndex);
    expect(contextCompletedIndex).toBeGreaterThan(contextStartedIndex);
    expect(decisionRequestedIndex).toBeGreaterThan(contextCompletedIndex);
    const liveStore = await openChannelStore(stateDir);
    try {
      await waitForCondition(
        () => {
          const references = liveStore.getThreadReferences(transport.bot.uid, target).mr;
          return references.includes(mergeRequest)
            && references.includes(secondMergeRequest)
            && references.includes(thirdMergeRequest);
        },
        "three real coordinator SQLite reference writes",
      );
    } finally {
      liveStore.close();
    }

    await restarted.stop();
    restartedService = undefined;
    const persistedStore = await openChannelStore(stateDir);
    try {
      const references = persistedStore.getThreadReferences(transport.bot.uid, target);
      expect(references.mr).toHaveLength(3);
      expect(references.mr).toEqual(expect.arrayContaining([mergeRequest, secondMergeRequest, thirdMergeRequest]));
      expect(references.pr).toEqual([]);
    } finally {
      persistedStore.close();
    }
  } finally {
    releaseFailure.resolve();
    releaseContextRead.resolve();
    await restartedService?.stop();
    await firstService?.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});
