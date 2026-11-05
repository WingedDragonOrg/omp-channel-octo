import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChannelService, ChannelStatus, ChatTarget, InboundMessage } from "../src/contracts.ts";
import { WorkerSteerRejectedError } from "../src/contracts.ts";
import { createChannelService } from "../src/service.ts";
import { parseConfig, toCoreConfig } from "../src/runtime/config.ts";
import type { DecisionFetch, MatchedReferenceHistory, ReferenceEvidence, ThreadReferences } from "../src/references/types.ts";
import { createOutboxDelivery } from "../src/outbox.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  assistantText,
  FakeTransport,
  FakeWorkerFactory,
  FakeWorktrees,
  inbound,
  waitForIngressJob,
} from "./fixtures/core-fakes.ts";

type HistoricalDecisionEvidence = ReferenceEvidence & { matchedHistory: readonly MatchedReferenceHistory[] };

const target = { channelType: 5 as const, channelId: "group-tools____references" };
const canonical = "https://github.com/example/service/pull/17";

function referenceTransport(name: string): FakeTransport {
  return new FakeTransport({ bot: { uid: `reference-${name}`, name: "Reference Bot" } });
}

function threadRecord(shortId: string, name: string) {
  return {
    shortId,
    groupId: "group-tools",
    channelId: `group-tools____${shortId}`,
    name,
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-04",
    updatedAt: "2026-11-04",
  };
}

function decisionFetchFor(origin: string): DecisionFetch {
  return (input, init) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    return fetch(`${origin}${new URL(url).pathname}`, init);
  };
}

function workObjectResponse(body: Record<string, unknown>): Response {
  const questions = body.questions as Array<{ name: string }>;
  return Response.json({
    model: "fixture-model",
    answers: questions.map(question => ({
      name: question.name,
      type: "choice",
      choice: "work_object",
      confidence: 0.99,
      probabilities: [
        { value: "work_object", probability: 0.99 },
        { value: "reference_or_unrelated", probability: 0.005 },
        { value: "insufficient_evidence", probability: 0.005 },
      ],
    })),
  });
}

function evidenceFrom(body: Record<string, unknown>): Record<string, unknown> {
  const parsed: unknown = JSON.parse(String(body.input));
  if (parsed === null || typeof parsed !== "object" || !("evidence" in parsed)) {
    throw new Error("decision request omitted evidence");
  }
  const evidence = parsed.evidence;
  if (evidence === null || typeof evidence !== "object") {
    throw new Error("decision request evidence is not an object");
  }
  return Object.fromEntries(Object.entries(evidence));
}

function stringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const result: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") return undefined;
    result.push(item);
  }
  return result;
}

function threadReferencesFrom(value: unknown): ThreadReferences | undefined {
  if (value === null || typeof value !== "object" || !("references" in value)) return undefined;
  const references = value.references;
  if (
    references === null
    || typeof references !== "object"
    || Array.isArray(references)
    || !("mr" in references)
    || !("pr" in references)
  ) {
    return undefined;
  }
  const mr = stringArray(references.mr);
  const pr = stringArray(references.pr);
  return mr === undefined || pr === undefined ? undefined : { mr, pr };
}

function threadInfoChannelId(value: unknown): string | undefined {
  if (value === null || typeof value !== "object" || !("channel_id" in value)) return undefined;
  return typeof value.channel_id === "string" ? value.channel_id : undefined;
}

function hasThreadReference(value: unknown, kind: keyof ThreadReferences, url: string): boolean {
  return threadReferencesFrom(value)?.[kind].includes(url) === true;
}

function decisionsConfig(root: string) {
  return {
    ownerId: "owner-1",
    allowedGroupIds: ["group-tools"],
    stateDir: join(root, "state"),
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
  throw new Error(`timed out waiting for ${description}`);
}

async function waitForSignal(
  promise: Promise<void>,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  let finished = false;
  const timedOut = Promise.withResolvers<never>();
  const deadline = performance.now() + timeoutMs;
  void (async () => {
    while (!finished && performance.now() < deadline) {
      const turn = Promise.withResolvers<void>();
      setImmediate(turn.resolve);
      await turn.promise;
    }
    if (!finished) timedOut.reject(new Error(`timed out waiting for ${description}`));
  })();
  try {
    await Promise.race([promise, timedOut.promise]);
  } finally {
    finished = true;
  }
}

async function waitForPromptCount(
  workers: FakeWorkerFactory,
  count: number,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  await waitForCondition(() => workers.prompts.length >= count, description, timeoutMs);
}

async function waitForStatusCondition(
  service: ChannelService,
  chatTarget: ChatTarget,
  predicate: (status: ChannelStatus) => boolean,
  description: string,
  timeoutMs = 3_000,
): Promise<void> {
  await waitForCondition(async () => predicate(await service.status(chatTarget)), description, timeoutMs);
}

async function submitAndWaitForTask(
  service: ChannelService,
  workers: FakeWorkerFactory,
  chatTarget: ChatTarget,
  message: InboundMessage,
): Promise<void> {
  const promptCount = workers.prompts.length + 1;
  await waitForSignal(service.submitInbound(message), `inbound receipt ${message.messageId}`);
  await waitForPromptCount(workers, promptCount, `prompt for ${message.messageId}`);
  await waitForStatusCondition(
    service,
    chatTarget,
    status => status.state === "completed" && status.queued === 0,
    `completion of ${message.messageId}`,
  );
}

async function submitAndWaitForNew(
  service: ChannelService,
  chatTarget: ChatTarget,
  message: InboundMessage,
  cwd: string,
): Promise<void> {
  const previous = await service.status(chatTarget);
  const expectedCwd = await realpath(cwd);
  await waitForSignal(service.submitInbound(message), `inbound receipt ${message.messageId}`);
  await waitForStatusCondition(
    service,
    chatTarget,
    status => status.cwd === expectedCwd && status.sessionId !== previous.sessionId,
    `new binding for ${message.messageId}`,
  );
}

/*
 * /thread-info is observed from a real host-tool call in the worker. Each poll
 * waits for the exact command's prompt and completion, not merely the handler
 * receipt from submitInbound.
 */
async function waitForThreadInfo(
  service: ChannelService,
  workers: FakeWorkerFactory,
  chatTarget: ChatTarget,
  botUid: string,
  readInfo: () => unknown,
  predicate: (info: unknown) => boolean,
): Promise<unknown> {
  const deadline = performance.now() + 3_500;
  const messagePrefix = randomUUID();
  for (let attempt = 0; attempt < 100 && performance.now() < deadline; attempt += 1) {
    const firstNewPrompt = workers.prompts.length;
    const previousInfo = readInfo();
    const messageId = `reference-info-${messagePrefix}-${attempt}`;
    await waitForSignal(service.submitInbound(inbound({
      ...chatTarget,
      messageId,
      text: "/queue show references",
      mentionedUserIds: [botUid],
    })), `inbound receipt ${messageId}`, Math.max(1, deadline - performance.now()));
    await waitForCondition(
      () => workers.prompts.slice(firstNewPrompt).some(prompt => prompt.text === "show references"),
      `thread-info prompt ${messageId}`,
      Math.max(1, deadline - performance.now()),
    );
    await waitForStatusCondition(
      service,
      chatTarget,
      status => status.state === "completed" && status.queued === 0,
      `thread-info completion ${messageId}`,
      Math.max(1, deadline - performance.now()),
    );
    const info = readInfo();
    if (info !== previousInfo && predicate(info)) return info;
  }
  throw new Error("thread-info did not reach the expected reference state within 3500 ms");
}


test("enabled decisions resolve only the explicitly selected operator credential", () => {
  let failure: unknown;
  let result: unknown;
  try {
    const config = parseConfig({
      botToken: "fixture-bot-token", allowedGroupIds: [], ompCommand: "omp", stateDir: "/tmp/reference-config",
      decisions: { enabled: true, protocol: "openai-decisions", baseUrl: "https://decision.example/proxy", model: "fixture-model", apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY" },
    }, { env: { OMP_CHANNEL_OCTO_DECISION_KEY: "explicit-fixture-key", OPENAI_API_KEY: "must-not-be-used" } });
    result = toCoreConfig(config);
  } catch (error) { failure = error; }
  expect(failure).toBeUndefined();
  expect(result).toMatchObject({ decisions: { enabled: true, protocol: "openai-decisions", baseUrl: "https://decision.example/proxy", model: "fixture-model", apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY", apiKey: "explicit-fixture-key" } });
});

for (const protocol of ["openai-decisions", "typesafe-systemone"] as const) {
  test(`${protocol}: accepted visible conversation binds a canonical reference returned by thread-info`, async () => {
    const root = await mkdtemp(join(tmpdir(), "oco-references-"));
    const stateDir = join(root, "state");
    await mkdir(stateDir);
    const transport = referenceTransport(protocol);
    transport.setThreads("group-tools", [{ shortId: "references", groupId: "group-tools", channelId: target.channelId, name: "Service change", creatorUid: "owner-1", status: 1, memberCount: 1, messageCount: 0, createdAt: "2026-11-04", updatedAt: "2026-11-04" }]);
    const received = Promise.withResolvers<void>();
    const requests: Array<{ path: string; authorization: string | null; body: Record<string, unknown> }> = [];
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push({ path: new URL(request.url).pathname, authorization: request.headers.get("authorization"), body });
      const questions = body.questions;
      let answers: unknown;
      if (Array.isArray(questions)) {
        answers = questions.map((question: { name: string }) => ({ name: question.name, type: "choice", choice: "work_object", confidence: 0.99, probabilities: [{ value: "work_object", probability: 0.99 }, { value: "reference_or_unrelated", probability: 0.005 }, { value: "insufficient_evidence", probability: 0.005 }] }));
      } else {
        answers = Object.fromEntries(Object.keys(questions as object).map(name => [name, { type: "choice", choice: "work_object", confidence: 0.99, probabilities: { work_object: 0.99, reference_or_unrelated: 0.005, insufficient_evidence: 0.005 } }]));
      }
      received.resolve();
      return Response.json({ model: "fixture-model", answers, usage: { input_tokens: 1, output_tokens: 0 } });
    } });
    let info: unknown;
    const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
      if (text === "show references") {
        const tool = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
        info = JSON.parse(await tool.execute({ action: "thread-info" }, new AbortController().signal));
      } else {
        emit(assistantText("result", "Review accepted.", true));
      }
    });
    const config = {
      ownerId: "owner-1", allowedGroupIds: ["group-tools"], stateDir, homeDir: root,
      decisions: { enabled: true as const, protocol, baseUrl: "https://decision.example", model: "fixture-model", apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY", apiKey: "fixture-decision-key" },
    };
    const options = { config, transport, workerFactory: workers,
      decisionsFetch: (input: string | URL | Request, init?: RequestInit) => fetch(`${server.url.origin}${new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url).pathname}`, init),
    };
    const service = await createChannelService(options);
    try {
      await service.start();
      await submitAndWaitForNew(service, target, inbound({
        ...target,
        messageId: "bind",
        text: `/new ${root}`,
        mentionedUserIds: [transport.bot.uid],
      }), root);
      await submitAndWaitForTask(service, workers, target, inbound({
        ...target,
        messageId: "review",
        text: `Please review this PR for the current service change: ${canonical}/files?token=must-not-leave#diff-1`,
        mentionedUserIds: [transport.bot.uid],
      }));
      await waitForSignal(received.promise, "decision request");
      await waitForThreadInfo(
        service,
        workers,
        target,
        transport.bot.uid,
        () => info,
        value => hasThreadReference(value, "pr", canonical),
      );
      expect(info).toMatchObject({ references: { mr: [], pr: [canonical] } });
      expect(requests[0]?.path).toBe(protocol === "openai-decisions" ? "/v1/decisions" : "/v1/systemone");
      expect(requests[0]?.authorization).toBe("Bearer fixture-decision-key");
      const external = JSON.stringify(requests.map(request => request.body));
      expect(external).not.toContain("must-not-leave");
      expect(external).not.toContain(root);
      expect(external).not.toContain(target.channelId);
      expect(external).not.toContain("fixture-bot-token");
    } finally {
      await service.stop();
      server.stop(true);
      await rm(root, { recursive: true, force: true });
    }
  });
}
test("only a durable assistant final enters reference judging, and its full URL does not wait for provider I/O", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-final-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("assistant-final");
  transport.setThreads("group-tools", [{
    shortId: "references",
    groupId: "group-tools",
    channelId: target.channelId,
    name: "Service change",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-04",
    updatedAt: "2026-11-04",
  }]);
  const received = Promise.withResolvers<void>();
  let providerResponseSent = false;
  const releaseResponse = Promise.withResolvers<void>();
  const progressUrl = "https://github.com/example/service/pull/900";
  const finalText = `${"x".repeat(2998)}@[assistant-private-uid:Alice] ${canonical}`;
  const requests: Array<{ body: Record<string, unknown> }> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push({ body });
      received.resolve();
      await releaseResponse.promise;
      providerResponseSent = true;
      const questions = body.questions as Array<{ name: string }>;
      return Response.json({
        model: "fixture-model",
        answers: questions.map(question => ({
          name: question.name,
          type: "choice",
          choice: "work_object",
          confidence: 0.99,
          probabilities: [
            { value: "work_object", probability: 0.99 },
            { value: "reference_or_unrelated", probability: 0.005 },
            { value: "insufficient_evidence", probability: 0.005 },
          ],
        })),
      });
    },
  });
  let info: unknown;
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const tool = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      info = JSON.parse(await tool.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    emit(assistantText("reply", `progress only: ${progressUrl}`, false));
    emit(assistantText("reply", finalText, true));
  });
  const service = await createChannelService({
    config: {
      ownerId: "owner-1",
      allowedGroupIds: ["group-tools"],
      stateDir,
      homeDir: root,
      decisions: {
        enabled: true,
        protocol: "openai-decisions",
        baseUrl: "https://decision.example",
        model: "fixture-model",
        apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
        apiKey: "fixture-decision-key",
      },
    },
    transport,
    workerFactory: workers,
    decisionsFetch: (input, init) => fetch(
      `${server.url.origin}${new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url).pathname}`,
      init,
    ),
  });
  try {
    await service.start();
    await submitAndWaitForNew(service, target, inbound({
      ...target,
      messageId: "bind-final",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);
    const finalPromptCount = workers.prompts.length + 1;
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "final-source",
      text: "Please implement the requested service change.",
      mentionedUserIds: [transport.bot.uid],
    })), "final-source inbound receipt");
    await waitForPromptCount(workers, finalPromptCount, "final-source worker prompt");
    await waitForSignal(received.promise, "decision request");
    await waitForStatusCondition(
      service,
      target,
      status => status.state === "completed" && status.queued === 0,
      "task completion while decision response is held",
    );
    expect(providerResponseSent).toBe(false);
    const external = JSON.stringify(requests.map(request => request.body));
    expect(external).toContain(canonical);
    expect(external).not.toContain(progressUrl);
    expect(external).toContain("@Alice");
    expect(external).not.toContain("assistant-private-uid");
    releaseResponse.resolve();
    await waitForCondition(
      () => transport.sent.some(
        message => message.target.channelId === target.channelId && message.text.includes(canonical),
      ),
      "final reference answer delivery",
    );
    expect(transport.visibleText(target)).toContain("@Alice");
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => info,
      value => hasThreadReference(value, "pr", canonical),
    );
    expect(info).toMatchObject({ references: { mr: [], pr: [canonical] } });
  } finally {
    releaseResponse.resolve();
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("a plain pending-ask answer is judged once and only after its answer is durable", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-ask-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("ask");
  transport.setThreads("group-tools", [{
    shortId: "references",
    groupId: "group-tools",
    channelId: target.channelId,
    name: "Service change",
    creatorUid: "owner-1",
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-04",
    updatedAt: "2026-11-04",
  }]);
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  let pendingAtRequest: number | undefined;
  let service: ChannelService | undefined;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      pendingAtRequest = (await service!.status(target)).pendingQuestionIds.length;
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const tool = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      info = JSON.parse(await tool.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    const ask = promptContext!.tools!.find(tool => tool.name === "ask")!;
    await ask.execute(
      { questions: [{ id: "reference-answer", question: "Which PR is the direct work object?" }] },
      new AbortController().signal,
    );
    emit(assistantText("ask-result", "Answer recorded.", true));
  });
  let info: unknown;
  service = await createChannelService({
    config: {
      ownerId: "owner-1",
      allowedGroupIds: ["group-tools"],
      stateDir,
      homeDir: root,
      decisions: {
        enabled: true,
        protocol: "openai-decisions",
        baseUrl: "https://decision.example",
        model: "fixture-model",
        apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
        apiKey: "fixture-decision-key",
      },
    },
    transport,
    workerFactory: workers,
    decisionsFetch: (input, init) => fetch(
      `${server.url.origin}${new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url).pathname}`,
      init,
    ),
  });
  try {
    await service.start();
    const firstPromptCount = workers.prompts.length + 1;
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "ask-start",
      text: "Start the clarification.",
      mentionedUserIds: [transport.bot.uid],
    })), "ask-start inbound receipt");
    await waitForPromptCount(workers, firstPromptCount, "ask-start worker prompt");
    await waitForStatusCondition(
      service,
      target,
      status => status.pendingQuestionIds.length === 1,
      "pending clarification question",
    );
    const answerText = "Please review the linked item for this question.";
    const answerQuote = `Direct work item: ${canonical}`;
    const answer = inbound({
      ...target,
      messageId: "ask-answer",
      text: answerText,
      quoted: { senderName: "answer-quote-private-sender", text: answerQuote },
      mentionedUserIds: [transport.bot.uid],
    });
    await waitForSignal(service.submitInbound(answer), "ask answer inbound receipt");
    await waitForIngressJob({ stateDir, bot: transport.bot }, answer);
    await waitForSignal(requested.promise, "ask answer reference judgement");
    await waitForStatusCondition(
      service,
      target,
      status => status.state === "completed" && status.pendingQuestionIds.length === 0,
      "completed answered turn",
    );
    expect(pendingAtRequest).toBe(0);
    await waitForSignal(service.submitInbound(answer), "duplicate ask answer receipt");
    expect(requests).toHaveLength(1);
    const answerEvidence = evidenceFrom(requests[0]!);
    expect(answerEvidence.task).toBe(
      `Initial accepted requirement:\nStart the clarification.`
      + `\nAdditional accepted requirement:\n[引用]\n${answerQuote}\n\n${answerText}`,
    );
    expect(JSON.stringify(answerEvidence)).not.toContain("answer-quote-private-sender");
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => info,
      value => hasThreadReference(value, "pr", canonical),
    );
    expect(info).toMatchObject({ references: { mr: [], pr: [canonical] } });
    expect(requests).toHaveLength(1);
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("commands, unauthorized input, and link-free turns are excluded; accepted references use raw message text", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-admission-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("admission");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const attachmentUrl = "https://files.example/reference-notes.txt";
  transport.attachmentBodies.set(attachmentUrl, new TextEncoder().encode("materialized attachment data"));
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  let info: unknown;
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const tool = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      info = JSON.parse(await tool.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    emit(assistantText("admission-result", "Turn completed.", true));
  });
  const service = await createChannelService({
    config: { ...decisionsConfig(root), attachments: { enabled: true, maxBytes: 4096 } },
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  const unauthorizedText = `Review this untrusted link: https://github.com/example/service/pull/901`;
  const commandText = `/status https://github.com/example/service/pull/902`;
  const candidateText = `Review only the active change: ${canonical}`;
  try {
    await service.start();
    await submitAndWaitForNew(service, target, inbound({
      ...target,
      messageId: "admission-new",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "unauthorized-reference",
      senderId: "ordinary-member",
      text: unauthorizedText,
      mentionedUserIds: [],
    })), "unauthorized reference inbound receipt");
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "command-reference",
      text: commandText,
      mentionedUserIds: [transport.bot.uid],
    })), "status command inbound receipt");
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "link-free-turn",
      text: "Explain the normal service flow.",
      mentionedUserIds: [transport.bot.uid],
      attachments: [{
        url: attachmentUrl,
        kind: "file",
        name: "reference-notes.txt",
        size: 29,
        durationSeconds: 0,
      }],
    }));
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "raw-reference",
      text: candidateText,
      senderId: "ordinary-member",
      mentionedUserIds: [transport.bot.uid],
      attachments: [{
        url: attachmentUrl,
        kind: "file",
        name: "reference-notes.txt",
        size: 29,
        durationSeconds: 0,
      }],
    }));
    await waitForSignal(requested.promise, "raw reference decision request");
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => info,
      value => hasThreadReference(value, "pr", canonical),
    );
    expect(requests).toHaveLength(1);
    const external = JSON.stringify(requests);
    expect(external).toContain(candidateText);
    expect(evidenceFrom(requests[0]!).task).toBe(`Initial accepted requirement:\n${candidateText}`);
    expect(external).not.toContain(unauthorizedText);
    expect(external).not.toContain(commandText);
    expect(external).not.toContain("materialized attachment data");
    expect(external).not.toContain(root);
    expect(transport.downloadRequests).toHaveLength(2);
    expect(info).toMatchObject({ references: { mr: [], pr: [canonical] } });
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("a refused steer is judged once as a newly queued user handover", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-steer-refusal-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("steer-refused");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const firstPrompt = Promise.withResolvers<void>();
  const finishFirst = Promise.withResolvers<void>();
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  const initialText = "hold the current task without a link";
  const handoffText = `Please implement this direct work item: ${canonical}`;
  const workers = new FakeWorkerFactory(async ({ text, emit }) => {
    if (text === initialText) {
      firstPrompt.resolve();
      await finishFirst.promise;
      emit(assistantText("initial-result", "Existing task complete.", true));
      return;
    }
    emit(assistantText("queued-result", "Queued task complete.", true));
  });
  workers.failSteerWith(new WorkerSteerRejectedError("fixture refused the steer"));
  const service = await createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "steer-active",
      text: initialText,
      mentionedUserIds: [transport.bot.uid],
    })), "active task inbound receipt");
    await waitForSignal(firstPrompt.promise, "active task worker prompt");
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "steer-refused-queued",
      text: handoffText,
      mentionedUserIds: [transport.bot.uid],
    })), "refused steer inbound receipt");
    const taskStore = await openChannelStore(stateDir);
    try {
      await waitForCondition(
        () => taskStore.getTaskByMessageId(
          transport.bot.uid,
          target,
          "steer-refused-queued",
        )?.state === "queued",
        "refused steer task to be durably queued",
      );
      finishFirst.resolve();
      await waitForPromptCount(workers, 2, "queued handover worker prompt");
      await waitForSignal(requested.promise, "queued handover reference judgement");
      await waitForCondition(
        () => taskStore.getTaskByMessageId(
          transport.bot.uid,
          target,
          "steer-refused-queued",
        )?.state === "completed",
        "queued handover task completion",
      );
    } finally {
      taskStore.close();
    }
    expect(requests).toHaveLength(1);
    const evidence = evidenceFrom(requests[0]!);
    expect(evidence.task).toBe(`Initial accepted requirement:\n${handoffText}`);
  } finally {
    finishFirst.resolve();
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("a successfully steered visible user message joins the running task evidence", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-steer-accepted-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("steer-accepted");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const firstPrompt = Promise.withResolvers<void>();
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  const initialText = "work on the current service change";
  const steeredText = `Also review this direct work item: ${canonical}`;
  const workers = new FakeWorkerFactory(async ({ text, emit, waitForAppend, waitForAbort }) => {
    if (text === initialText) {
      firstPrompt.resolve();
      const appended = await Promise.race([
        waitForAppend().then(() => true),
        waitForAbort().then(() => false),
      ]);
      if (!appended) return;
      emit(assistantText("steered-result", "Steered task complete.", true));
      return;
    }
    emit(assistantText("unexpected-separate-result", "Unexpected separate task.", true));
  });
  const service = await createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await waitForSignal(service.submitInbound(inbound({
      ...target,
      messageId: "steer-start",
      text: initialText,
      mentionedUserIds: [transport.bot.uid],
    })), "steer-start inbound receipt");
    await waitForSignal(firstPrompt.promise, "steer-start worker prompt");
    const handover = inbound({
      ...target,
      messageId: "steer-visible-handover",
      text: steeredText,
      mentionedUserIds: [transport.bot.uid],
    });
    await waitForSignal(service.submitInbound(handover), "visible handover inbound receipt");
    await waitForIngressJob({ stateDir, bot: transport.bot }, handover);
    await waitForSignal(requested.promise, "steered reference judgement");
    await waitForStatusCondition(
      service,
      target,
      status => status.state === "completed" && status.queued === 0,
      "completion of the steered turn",
    );
    expect(workers.prompts).toHaveLength(1);
    expect(requests).toHaveLength(1);
    const evidence = evidenceFrom(requests[0]!);
    expect(evidence.task).toBe(
      `Initial accepted requirement:\n${initialText}\nAdditional accepted requirement:\n${steeredText}`,
    );
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("thread dispatch judges only the visible handover in the destination thread", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-dispatch-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const parent = { channelType: 2 as const, channelId: "group-tools" };
  const child = { channelType: 5 as const, channelId: "group-tools____references" };
  const transport = referenceTransport("dispatch");
  transport.setThreads("group-tools", [threadRecord("references", "Dispatch destination")]);
  const requested = Promise.withResolvers<void>();
  const dispatched = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  const parentText = "Dispatch the scoped change, not this parent reference: https://github.com/example/service/pull/919";
  const handoffText = `Review this work object in the destination thread: ${canonical}`;
  let childInfo: unknown;
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === parentText) {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      await management.execute(
        { action: "thread-dispatch", short_id: "references", text: handoffText },
        new AbortController().signal,
      );
      dispatched.resolve();
      emit(assistantText("parent-result", "Task handed over.", true));
      return;
    }
    if (text === "show references") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      childInfo = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    emit(assistantText("child-result", "Destination task complete.", true));
  });
  const service = await createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await waitForSignal(service.submitInbound(inbound({
      ...parent,
      messageId: "dispatch-parent-source",
      text: parentText,
      mentionedUserIds: [transport.bot.uid],
    })), "dispatch source inbound receipt");
    await waitForSignal(dispatched.promise, "thread dispatch host-tool effect");
    await waitForPromptCount(workers, 2, "destination thread worker prompt");
    await waitForSignal(requested.promise, "destination thread reference judgement");
    await waitForStatusCondition(
      service,
      parent,
      status => status.state === "completed" && status.queued === 0,
      "dispatch source task completion",
    );
    await waitForStatusCondition(
      service,
      child,
      status => status.state === "completed" && status.queued === 0,
      "destination thread task completion",
    );
    const evidence = evidenceFrom(requests[0]!);
    expect(requests).toHaveLength(1);
    expect(evidence.recent).toMatchObject([{ role: "user", text: handoffText }]);
    expect(evidence.task).toBe(`Initial accepted requirement:\n${handoffText}`);
    expect(JSON.stringify(evidence)).not.toContain("pull/919");
    await waitForThreadInfo(
      service,
      workers,
      child,
      transport.bot.uid,
      () => childInfo,
      value => hasThreadReference(value, "pr", canonical),
    );
    expect(childInfo).toMatchObject({ references: { mr: [], pr: [canonical] } });
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("fresh sessions retain bound references, clear prior context, and keep sibling threads isolated", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-lifecycle-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const sibling = { channelType: 5 as const, channelId: "group-tools____sibling" };
  const transport = referenceTransport("lifecycle");
  transport.setThreads("group-tools", [
    threadRecord("references", "Primary thread"),
    threadRecord("sibling", "Sibling thread"),
  ]);
  const originMarker = "primary-only context from the old session";
  const sharedThreadBrief = "durable shared thread brief";
  const siblingMarker = "sibling-only context";
  transport.setGroupContext(target, { content: sharedThreadBrief, version: 1, updatedAt: null, updatedBy: "owner-1" });
  transport.setGroupContext(sibling, { content: siblingMarker, version: 1, updatedAt: null, updatedBy: "owner-1" });
  const secondCanonical = "https://github.com/example/service/pull/18";
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      return workObjectResponse(body);
    },
  });
  const infoByTarget = new Map<string, unknown>();
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      const info: unknown = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
      const channelId = threadInfoChannelId(info);
      if (channelId === undefined) throw new Error("thread-info omitted its channel id");
      infoByTarget.set(channelId, info);
      return;
    }
    emit(assistantText("lifecycle-result", "Turn completed.", true));
  });
  const service = await createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "context-seed",
      text: `Seed the thread context before evaluating a link. ${originMarker}`,
      mentionedUserIds: [transport.bot.uid],
    }));
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "old-session-reference",
      text: `Continue the existing task: ${canonical}`,
      mentionedUserIds: [transport.bot.uid],
    }));
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => infoByTarget.get(target.channelId),
      value => hasThreadReference(value, "pr", canonical),
    );
    const firstEvidence = evidenceFrom(requests[0]!);
    expect(JSON.stringify(firstEvidence.recent)).toContain(originMarker);

    await submitAndWaitForNew(service, target, inbound({
      ...target,
      messageId: "fresh-session",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);
    await waitForThreadInfo(
      service,
      workers,
      sibling,
      transport.bot.uid,
      () => infoByTarget.get(sibling.channelId),
      value => {
        const references = threadReferencesFrom(value);
        return references !== undefined && references.mr.length === 0 && references.pr.length === 0;
      },
    );
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "new-session-reference",
      text: `Start the fresh-session work with this item: ${secondCanonical}`,
      mentionedUserIds: [transport.bot.uid],
    }));
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => infoByTarget.get(target.channelId),
      value => {
        const references = threadReferencesFrom(value)?.pr;
        return references?.includes(canonical) === true && references.includes(secondCanonical);
      },
    );
    expect(requests).toHaveLength(2);
    const newSessionEvidence = evidenceFrom(requests[1]!);
    expect(newSessionEvidence.thread).toContain(sharedThreadBrief);
    expect(JSON.stringify(newSessionEvidence)).not.toContain(originMarker);
    expect(JSON.stringify(newSessionEvidence)).not.toContain(siblingMarker);
    expect(infoByTarget.get(target.channelId)).toMatchObject({
      references: { mr: [], pr: [canonical, secondCanonical] },
    });
    expect(infoByTarget.get(sibling.channelId)).toMatchObject({ references: { mr: [], pr: [] } });
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("a committed assistant final is not observed again when the same frame is replayed after reopen", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-outbox-reopen-"));
  const bot = { uid: "reference-outbox-bot", name: "Reference Bot" };
  const transport = new FakeTransport({ bot });
  let store = await openChannelStore(root);
  store.setBotUid(bot.uid);
  const firstObservations: Array<{ taskId: string; messageId: string; text: string }> = [];
  let replays = 0;
  let outbox = createOutboxDelivery({
    store,
    transport,
    botUid: () => bot.uid,
    started: () => false,
    onAssistantFinal(_target, taskId, messageId, text) {
      firstObservations.push({ taskId, messageId, text });
    },
  });
  try {
    outbox.enqueueStreamText(target, "durable-task", {
      type: "text",
      messageId: "durable-assistant-final",
      text: `Final work object: ${canonical}`,
      final: true,
    }, { observeReferences: true });
    expect(firstObservations).toEqual([{
      taskId: "durable-task",
      messageId: "durable-assistant-final",
      text: `Final work object: ${canonical}`,
    }]);
    outbox.stopTimers();
    await outbox.settle();
    store.close();

    store = await openChannelStore(root);
    store.setBotUid(bot.uid);
    outbox = createOutboxDelivery({
      store,
      transport,
      botUid: () => bot.uid,
      started: () => false,
      onAssistantFinal() {
        replays += 1;
      },
    });
    outbox.enqueueStreamText(target, "durable-task", {
      type: "text",
      messageId: "durable-assistant-final",
      text: `Final work object: ${canonical}`,
      final: true,
    }, { observeReferences: true });
    outbox.stopTimers();
    await outbox.settle();
    expect(replays).toBe(0);
  } finally {
    outbox.stopTimers();
    await outbox.settle();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("successful thread deletion tombstones references before failing workspace cleanup and blocks later finals", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "oco-reference-deleted-")));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const workspace = join(root, "owned-workspace");
  await mkdir(workspace);
  const transport = referenceTransport("deleted");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const setupStore = await openChannelStore(stateDir);
  setupStore.setBotUid(transport.bot.uid);
  setupStore.saveThreadWorkspace({
    botUid: transport.bot.uid,
    channelType: 5,
    channelId: target.channelId,
    layout: "single",
    status: "ready",
    entryPath: null,
    projects: [{
      name: "service",
      repo: root,
      path: workspace,
      branch: "reference-cleanup",
      created: true,
    }],
    error: null,
  });
  setupStore.close();
  const worktrees = new FakeWorktrees(root);
  worktrees.removeFailure = new Error("fixture cleanup deliberately fails");
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      return workObjectResponse(body);
    },
  });
  const secondCanonical = "https://github.com/example/service/pull/20";
  let info: unknown;
  let deletionResult = "";
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      info = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    if (text === "delete the thread now") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      deletionResult = await management.execute(
        { action: "thread-delete" },
        new AbortController().signal,
      );
      emit(assistantText("deleted-final", `Completed after deletion: ${secondCanonical}`, true));
      return;
    }
    emit(assistantText("pre-delete-result", "Initial task completed.", true));
  });
  const service = await createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory: workers,
    worktrees,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "before-delete-reference",
      text: `Implement this direct service item: ${canonical}`,
      mentionedUserIds: [transport.bot.uid],
    }));
    await waitForSignal(requested.promise, "pre-delete reference judgement");
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => info,
      value => hasThreadReference(value, "pr", canonical),
    );
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "delete-thread-message",
      text: "delete the thread now",
      mentionedUserIds: [transport.bot.uid],
    }));
    expect(deletionResult).toContain("已删除线程");
    expect(deletionResult).toContain("清理失败");
    expect(worktrees.removals).toHaveLength(1);
    expect(requests).toHaveLength(1);
    await service.stop();
    const persistedStore = await openChannelStore(stateDir);
    try {
      expect(persistedStore.isThreadReferencesDeleted(transport.bot.uid, target)).toBe(true);
      expect(persistedStore.getThreadReferences(transport.bot.uid, target)).toEqual({ mr: [], pr: [] });
    } finally {
      persistedStore.close();
    }
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("thread-info returns stored references even when decisions are disabled", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-disabled-read-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("disabled");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const setupStore = await openChannelStore(stateDir);
  setupStore.setBotUid(transport.bot.uid);
  setupStore.addThreadReferences(transport.bot.uid, target, [{ kind: "pr", url: canonical }]);
  setupStore.close();
  let info: unknown;
  const workers = new FakeWorkerFactory(async ({ text, promptContext }) => {
    if (text !== "show references") return;
    const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
    info = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
  });
  const service = await createChannelService({
    config: {
      ownerId: "owner-1",
      allowedGroupIds: ["group-tools"],
      stateDir,
      homeDir: root,
    },
    transport,
    workerFactory: workers,
  });
  try {
    await service.start();
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "disabled-thread-info",
      text: "show references",
      mentionedUserIds: [transport.bot.uid],
    }));
    expect(info).toMatchObject({ references: { mr: [], pr: [canonical] } });
  } finally {
    await service.stop();
    await rm(root, { recursive: true, force: true });
  }
});

test("quoted text retains neutral provenance without sender or quoted attachment metadata", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-quoted-text-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const transport = referenceTransport("quoted-text");
  transport.setThreads("group-tools", [threadRecord("references", "Service change")]);
  const quotedCanonical = "https://github.com/example/service/pull/23";
  const attachmentUrl = "https://files.example/quoted-private-notes.txt";
  const attachmentContent = "quoted attachment must not enter reference evidence";
  transport.attachmentBodies.set(attachmentUrl, new TextEncoder().encode(attachmentContent));
  const quoteText = `This is a related example, not the active task: ${quotedCanonical}`;
  const ownText = "Please consider the quoted message as context.";
  const requested = Promise.withResolvers<void>();
  const requests: Array<Record<string, unknown>> = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const body = await request.json() as Record<string, unknown>;
      requests.push(body);
      requested.resolve();
      const questions = body.questions as Array<{ name: string }>;
      return Response.json({
        model: "fixture-model",
        answers: questions.map(question => ({
          name: question.name,
          type: "choice",
          choice: "reference_or_unrelated",
          confidence: 0.99,
          probabilities: [
            { value: "work_object", probability: 0.005 },
            { value: "reference_or_unrelated", probability: 0.99 },
            { value: "insufficient_evidence", probability: 0.005 },
          ],
        })),
      });
    },
  });
  let info: unknown;
  const workers = new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      info = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
      return;
    }
    emit(assistantText("quoted-result", "I will consider the supplied context.", true));
  });
  const service = await createChannelService({
    config: { ...decisionsConfig(root), attachments: { enabled: true, maxBytes: 4096 } },
    transport,
    workerFactory: workers,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  try {
    await service.start();
    await submitAndWaitForTask(service, workers, target, inbound({
      ...target,
      messageId: "quoted-only-candidate",
      text: ownText,
      quoted: { senderName: "quoted-private-sender-uid", text: quoteText },
      mentionedUserIds: [transport.bot.uid],
      attachments: [{
        url: attachmentUrl,
        kind: "file",
        name: "quoted-private-notes.txt",
        size: new TextEncoder().encode(attachmentContent).byteLength,
        durationSeconds: 0,
      }],
    }));
    await waitForSignal(requested.promise, "quoted-text reference decision request");
    expect(requests).toHaveLength(1);
    const evidence = evidenceFrom(requests[0]!);
    expect(evidence.recent).toHaveLength(1);
    expect(evidence.recent).toMatchObject([{
      role: "user",
      text: `[引用]\n${quoteText}\n\n${ownText}`,
    }]);
    const external = JSON.stringify(evidence);
    expect(external).toContain("[引用]");
    expect(external).toContain(quotedCanonical);
    expect(external).toContain(ownText);
    expect(external).not.toContain("quoted-private-sender-uid");
    expect(external).not.toContain(attachmentContent);
    expect(external).not.toContain(root);
    expect(transport.downloadRequests).toHaveLength(1);
    await waitForThreadInfo(
      service,
      workers,
      target,
      transport.bot.uid,
      () => info,
      value => {
        const references = threadReferencesFrom(value);
        return references !== undefined && references.mr.length === 0 && references.pr.length === 0;
      },
    );
    expect(info).toMatchObject({ references: { mr: [], pr: [] } });
  } finally {
    await service.stop();
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
});

test("reopened reference judging restores exact-MR requirements and complete reports without sibling or near-match history", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-history-reopen-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir);
  const sibling = { channelType: 5 as const, channelId: "group-tools____reference-sibling" };
  const urlA = "https://gitlab.example/group/service/-/merge_requests/41";
  const urlB = "https://gitlab.example/group/service/-/merge_requests/42";
  const urlC = "https://gitlab.example/group/service/-/merge_requests/43";
  const urlD = "https://gitlab.example/group/service/-/merge_requests/44";
  const similarUrl = "https://gitlab.example/group/service/-/merge_requests/410";
  const failedDecisionUrl = "https://gitlab.example/group/service/-/merge_requests/99";
  const noRestartMarker = "NO_RESTART_CANDIDATE_LIST";
  const postRestartMarker = "POST_RESTART_CANDIDATE_LIST";
  const initialBackground = "INITIAL_THREAD_BACKGROUND: keep the parser migration compatible.";
  const postRestartBackground = "POST_RESTART_THREAD_BACKGROUND: the compatibility report is authoritative.";
  const requirementA = "PRIMARY_REQUIREMENT_41_BEGIN implement nested YAML validation without accepting aliases PRIMARY_REQUIREMENT_41_END";
  const reportA = `PRIMARY_FULL_REPORT_41_BEGIN checked nested aliases and added regression coverage PRIMARY_FULL_REPORT_41_END ${urlA}/diffs?token=fixture-private-token`;
  const requirementB = "PRIMARY_REQUIREMENT_42_BEGIN migrate the parser while preserving the public validation contract PRIMARY_REQUIREMENT_42_END";
  const reportB = `PRIMARY_FULL_REPORT_42_BEGIN verified parser compatibility and recorded the changed contract PRIMARY_FULL_REPORT_42_END ${urlB}/diffs`;
  const requirementC = "PRIMARY_REQUIREMENT_43_BEGIN add a focused regression around malformed project metadata PRIMARY_REQUIREMENT_43_END";
  const reportC = `PRIMARY_FULL_REPORT_43_BEGIN exercised malformed metadata and documented the expected behavior PRIMARY_FULL_REPORT_43_END ${urlC}/diffs`;
  const requirementD = "PRIMARY_REQUIREMENT_44_BEGIN add a focused regression for unsupported project metadata PRIMARY_REQUIREMENT_44_END";
  const reportD = `PRIMARY_FULL_REPORT_44_BEGIN checked unsupported metadata and preserved the parser contract PRIMARY_FULL_REPORT_44_END ${urlD}/diffs`;
  const requiredHistoryByUrl: Record<string, {
    taskRequirement: string;
    reportStart: string;
    reportEnd: string;
    background: string;
    requestMarker: string;
  }> = {
    [urlA]: {
      taskRequirement: requirementA,
      reportStart: "PRIMARY_FULL_REPORT_41_BEGIN",
      reportEnd: "PRIMARY_FULL_REPORT_41_END",
      background: postRestartBackground,
      requestMarker: postRestartMarker,
    },
    [urlB]: {
      taskRequirement: requirementB,
      reportStart: "PRIMARY_FULL_REPORT_42_BEGIN",
      reportEnd: "PRIMARY_FULL_REPORT_42_END",
      background: postRestartBackground,
      requestMarker: postRestartMarker,
    },
    [urlC]: {
      taskRequirement: requirementC,
      reportStart: "PRIMARY_FULL_REPORT_43_BEGIN",
      reportEnd: "PRIMARY_FULL_REPORT_43_END",
      background: postRestartBackground,
      requestMarker: postRestartMarker,
    },
    [urlD]: {
      taskRequirement: requirementD,
      reportStart: "PRIMARY_FULL_REPORT_44_BEGIN",
      reportEnd: "PRIMARY_FULL_REPORT_44_END",
      background: initialBackground,
      requestMarker: noRestartMarker,
    },
  };
  const similarRequirement = "SIMILAR_URL_REQUIREMENT_410 must never be attributed to merge request 41";
  const similarReport = `SIMILAR_URL_REPORT_410 belongs only to the distinct numbered merge request ${similarUrl}`;
  const siblingRequirement = "SIBLING_ONLY_REQUIREMENT_41 belongs to the neighboring discussion";
  const siblingReport = `SIBLING_ONLY_REPORT_41 is from another subthread and must stay there ${urlA}`;
  const unrelatedRequirement = "UNRELATED_TASK_REQUIREMENT must not become evidence for these merge requests";
  const unrelatedReport = "UNRELATED_TASK_REPORT contains no merge request candidate";
  const failureStarted = Promise.withResolvers<void>();
  const releaseFailure = Promise.withResolvers<void>();
  const failureFinished = Promise.withResolvers<void>();
  const contextReadStarted = Promise.withResolvers<void>();
  const releaseContextRead = Promise.withResolvers<void>();
  const timeline: string[] = [];
  const requests: Array<{ evidence: HistoricalDecisionEvidence }> = [];
  const infoByTarget = new Map<string, unknown>();
  let holdNextPrimaryContextRead = false;
  let failureResponseSent = false;
  let firstService: ChannelService | undefined;
  let restartedService: ChannelService | undefined;

  const transport = referenceTransport("history-reopen");
  transport.setThreads("group-tools", [
    threadRecord("references", "Primary reference thread"),
    threadRecord("reference-sibling", "Sibling reference thread"),
  ]);
  transport.setGroupContext(target, { content: initialBackground, version: 1, updatedAt: null, updatedBy: "owner-1" });
  transport.setGroupContext(sibling, { content: "SIBLING_ONLY_BACKGROUND", version: 1, updatedAt: null, updatedBy: "owner-1" });
  const realGetGroupContext = transport.getGroupContext.bind(transport);
  transport.getGroupContext = async readTarget => {
    const delayed = holdNextPrimaryContextRead
      && readTarget.channelType === target.channelType
      && readTarget.channelId === target.channelId;
    if (delayed) {
      holdNextPrimaryContextRead = false;
      timeline.push("context-read-started");
      contextReadStarted.resolve();
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
      const evidence = evidenceFrom(body) as unknown as HistoricalDecisionEvidence;
      const serialized = JSON.stringify(evidence);
      const currentUserText = evidence.recent
        .filter(message => message.role === "user")
        .map(message => message.text)
        .join("\n");
      requests.push({ evidence });
      if (serialized.includes(postRestartMarker)) timeline.push("post-restart-decision-requested");

      if (evidence.candidates.some(candidate => candidate.url === failedDecisionUrl)) {
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
          const expected = candidate === undefined ? undefined : requiredHistoryByUrl[candidate.url];
          const relatedHistory = evidence.matchedHistory.find(history =>
            history.candidateId === question.name && history.url === candidate?.url,
          );
          const positive = candidate !== undefined
            && expected !== undefined
            && relatedHistory !== undefined
            && relatedHistory.taskRequirement.includes(expected.taskRequirement)
            && relatedHistory.reportExcerpt.includes(expected.reportStart)
            && relatedHistory.reportExcerpt.includes(expected.reportEnd)
            && relatedHistory.reportExcerpt.includes("[matched candidate URL omitted]")
            && currentUserText.includes(expected.requestMarker)
            && serialized.includes(expected.background);
          const choice = positive ? "work_object" : "insufficient_evidence";
          return {
            name: question.name,
            type: "choice",
            choice,
            confidence: positive ? 0.76 : 0.99,
            probabilities: [
              { value: "work_object", probability: positive ? 0.76 : 0.005 },
              { value: "reference_or_unrelated", probability: positive ? 0.12 : 0.005 },
              { value: "insufficient_evidence", probability: positive ? 0.12 : 0.99 },
            ],
          };
        }),
      });
    },
  });

  const reportByPrompt: Record<string, string> = {
    [requirementA]: reportA,
    [requirementB]: reportB,
    [requirementC]: reportC,
    [similarRequirement]: similarReport,
    [unrelatedRequirement]: unrelatedReport,
    [siblingRequirement]: siblingReport,
    [requirementD]: reportD,
  };
  const createWorkers = (): FakeWorkerFactory => new FakeWorkerFactory(async ({ text, emit, promptContext }) => {
    if (text === "show references") {
      const management = promptContext!.tools!.find(tool => tool.name === "octo_management")!;
      const info: unknown = JSON.parse(await management.execute({ action: "thread-info" }, new AbortController().signal));
      const channelId = threadInfoChannelId(info);
      if (channelId === undefined) throw new Error("thread-info omitted its channel id");
      infoByTarget.set(channelId, info);
      return;
    }
    emit(assistantText(`history-${text}`, reportByPrompt[text] ?? "Fixture task completed.", true));
  });
  const createService = (workerFactory: FakeWorkerFactory) => createChannelService({
    config: decisionsConfig(root),
    transport,
    workerFactory,
    decisionsFetch: decisionFetchFor(server.url.origin),
  });
  const submitTask = (
    service: ChannelService,
    workers: FakeWorkerFactory,
    chatTarget: ChatTarget,
    messageId: string,
    text: string,
  ): Promise<void> => submitAndWaitForTask(service, workers, chatTarget, inbound({
    ...chatTarget,
    messageId,
    text,
    mentionedUserIds: [transport.bot.uid],
  }));
  const candidateRequestCount = (url: string): number => requests.filter(({ evidence }) =>
    evidence.candidates.some(candidate => candidate.url === url),
  ).length;
  const waitForCandidateRequest = async (url: string, count: number): Promise<void> => {
    await waitForCondition(
      () => candidateRequestCount(url) >= count,
      `Decision request for ${url} (${count})`,
    );
  };

  const firstWorkers = createWorkers();
  try {
    const initialService = await createService(firstWorkers);
    firstService = initialService;
    await initialService.start();
    await submitAndWaitForNew(initialService, target, inbound({
      ...target,
      messageId: "history-primary-bind",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);
    await submitAndWaitForNew(initialService, sibling, inbound({
      ...sibling,
      messageId: "history-sibling-bind",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);

    await submitTask(initialService, firstWorkers, target, "history-source-a", requirementA);
    await waitForCandidateRequest(urlA, 1);
    await submitTask(initialService, firstWorkers, target, "history-source-b", requirementB);
    await waitForCandidateRequest(urlB, 1);
    await submitTask(initialService, firstWorkers, target, "history-source-c", requirementC);
    await waitForCandidateRequest(urlC, 1);
    await submitTask(initialService, firstWorkers, target, "history-source-d", requirementD);
    await waitForCandidateRequest(urlD, 1);
    await submitTask(initialService, firstWorkers, target, "history-similar-source", similarRequirement);
    await waitForCandidateRequest(similarUrl, 1);
    await submitTask(initialService, firstWorkers, target, "history-unrelated-source", unrelatedRequirement);
    await submitTask(initialService, firstWorkers, sibling, "history-sibling-source", siblingRequirement);
    await waitForCandidateRequest(urlA, 2);

    const failedMessage = inbound({
      ...target,
      messageId: "history-failing-decision",
      text: `This failure must not block the chat: ${failedDecisionUrl}`,
      mentionedUserIds: [transport.bot.uid],
    });
    await submitAndWaitForTask(initialService, firstWorkers, target, failedMessage);
    await waitForSignal(failureStarted.promise, "held failing Decision request");
    expect(failureResponseSent).toBe(false);
    releaseFailure.resolve();
    await waitForSignal(failureFinished.promise, "failing Decision response");

    for (let index = 1; index <= 4; index += 1) {
      await submitTask(initialService, firstWorkers, target, `history-age-before-${index}`, `age-before-${index}`);
    }

    const noRestartText = `${noRestartMarker}: continue the current thread work using ${urlD}`;
    await submitTask(initialService, firstWorkers, target, "history-no-restart-candidate", noRestartText);
    await waitForCandidateRequest(urlD, 2);
    await waitForThreadInfo(
      initialService,
      firstWorkers,
      target,
      transport.bot.uid,
      () => infoByTarget.get(target.channelId),
      value => threadReferencesFrom(value)?.mr.includes(urlD) === true,
    );
    for (let index = 1; index <= 4; index += 1) {
      await submitTask(initialService, firstWorkers, target, `history-age-after-${index}`, `age-after-${index}`);
    }
    await initialService.stop();
    firstService = undefined;

    const reopenedStore = await openChannelStore(stateDir);
    try {
      expect(reopenedStore.getThreadReferences(transport.bot.uid, target)).toEqual({ mr: [urlD], pr: [] });
    } finally {
      reopenedStore.close();
    }

    const restartedWorkers = firstWorkers;
    const restarted = await createService(restartedWorkers);
    restartedService = restarted;
    await restarted.start();
    await submitAndWaitForNew(restarted, target, inbound({
      ...target,
      messageId: "history-primary-rebind",
      text: `/new ${root}`,
      mentionedUserIds: [transport.bot.uid],
    }), root);

    transport.setGroupContext(target, {
      content: postRestartBackground,
      version: 2,
      updatedAt: "2026-11-05T00:00:00.000Z",
      updatedBy: "owner-1",
    });
    holdNextPrimaryContextRead = true;
    const restartedText = `${postRestartMarker}: please inspect these three merge requests:\n${urlA}\n${urlB}\n${urlC}`;
    const promptCount = restartedWorkers.prompts.length + 1;
    timeline.push("post-restart-inbound-submitted");
    await waitForSignal(restarted.submitInbound(inbound({
      ...target,
      messageId: "history-post-restart-list",
      text: restartedText,
      mentionedUserIds: [transport.bot.uid],
    })), "post-restart multi-MR inbound receipt");
    await waitForSignal(contextReadStarted.promise, "post-restart background read after observation");
    releaseContextRead.resolve();
    await waitForPromptCount(restartedWorkers, promptCount, "post-restart multi-MR worker prompt");
    await waitForStatusCondition(
      restarted,
      target,
      status => status.state === "completed" && status.queued === 0,
      "post-restart multi-MR task completion",
    );
    await waitForCandidateRequest(urlA, 3);
    await waitForCandidateRequest(urlB, 2);
    await waitForCandidateRequest(urlC, 2);
    const postRestartRequest = requests.find(({ evidence }) => JSON.stringify(evidence).includes(postRestartMarker));
    if (postRestartRequest === undefined) throw new Error("post-restart Decision request was not captured");
    expect(postRestartRequest.evidence.candidates.map(candidate => candidate.url)).toEqual([urlA, urlB, urlC]);
    const matchedHistory = postRestartRequest.evidence.matchedHistory;
    expect(matchedHistory.map(history => history.url).sort()).toEqual([urlA, urlB, urlC].sort());
    const historyByUrl = new Map(matchedHistory.map(history => [history.url, history]));
    for (const [url, requirement, reportStart, reportEnd] of [
      [urlA, requirementA, "PRIMARY_FULL_REPORT_41_BEGIN", "PRIMARY_FULL_REPORT_41_END"],
      [urlB, requirementB, "PRIMARY_FULL_REPORT_42_BEGIN", "PRIMARY_FULL_REPORT_42_END"],
      [urlC, requirementC, "PRIMARY_FULL_REPORT_43_BEGIN", "PRIMARY_FULL_REPORT_43_END"],
    ] as const) {
      const history = historyByUrl.get(url);
      const candidate = postRestartRequest.evidence.candidates.find(item => item.url === url);
      expect(history).toBeDefined();
      expect(history?.candidateId).toBe(candidate?.id);
      expect(history?.taskRequirement).toContain(requirement);
      expect(history?.reportExcerpt).toContain(reportStart);
      expect(history?.reportExcerpt).toContain(reportEnd);
      expect(history?.reportExcerpt).toContain("[matched candidate URL omitted]");
    }
    const serializedEvidence = JSON.stringify(postRestartRequest.evidence);
    expect(serializedEvidence).toContain(postRestartBackground);
    expect(postRestartRequest.evidence.task).not.toContain(similarRequirement);
    expect(postRestartRequest.evidence.task).not.toContain(similarReport);
    expect(postRestartRequest.evidence.task).not.toContain(unrelatedRequirement);
    expect(postRestartRequest.evidence.task).not.toContain(unrelatedReport);
    expect(postRestartRequest.evidence.task).not.toContain(failedDecisionUrl);
    for (const [marker, role] of [
      [similarRequirement, "user"],
      [similarReport, "assistant"],
      [unrelatedRequirement, "user"],
      [unrelatedReport, "assistant"],
      [failedDecisionUrl, "user"],
    ] as const) {
      if (!serializedEvidence.includes(marker)) continue;
      expect(postRestartRequest.evidence.recent.some(
        message => message.role === role && message.text.includes(marker),
      )).toBe(true);
    }
    expect(serializedEvidence).not.toContain(siblingRequirement);
    expect(serializedEvidence).not.toContain(siblingReport);
    expect(postRestartRequest.evidence.recent.some(
      message => message.role === "user" && message.text.includes(postRestartMarker),
    )).toBe(true);
    const submittedIndex = timeline.indexOf("post-restart-inbound-submitted");
    const contextStartedIndex = timeline.indexOf("context-read-started");
    const contextCompletedIndex = timeline.indexOf("context-read-completed");
    const decisionRequestedIndex = timeline.indexOf("post-restart-decision-requested");
    expect(submittedIndex).toBeGreaterThanOrEqual(0);
    expect(contextStartedIndex).toBeGreaterThan(submittedIndex);
    expect(contextCompletedIndex).toBeGreaterThan(contextStartedIndex);
    expect(decisionRequestedIndex).toBeGreaterThan(contextCompletedIndex);

    await waitForThreadInfo(
      restarted,
      restartedWorkers,
      target,
      transport.bot.uid,
      () => infoByTarget.get(target.channelId),
      value => {
        const references = threadReferencesFrom(value)?.mr;
        return references?.includes(urlA) === true
          && references.includes(urlB)
          && references.includes(urlC)
          && references.includes(urlD);
      },
    );
    await restarted.stop();
    restartedService = undefined;

    const persistedStore = await openChannelStore(stateDir);
    try {
      const references = persistedStore.getThreadReferences(transport.bot.uid, target);
      expect(references.mr).toHaveLength(4);
      expect(references.mr).toEqual(expect.arrayContaining([urlA, urlB, urlC, urlD]));
      expect(references.mr).not.toContain(similarUrl);
      expect(references.mr).not.toContain(failedDecisionUrl);
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
