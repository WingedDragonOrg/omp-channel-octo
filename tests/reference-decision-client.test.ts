import { expect, test, vi } from "bun:test";
import { createReferenceJudge } from "../src/references/decision-client.ts";
import type {
  EnabledDecisionsConfig,
  ReferenceCandidate,
  ReferenceEvidence,
  ReferenceKind,
} from "../src/references/types.ts";

interface CapturedRequest {
  path: string;
  method: string;
  authorization: string | null;
  contentEncoding: string | null;
  body: unknown;
}

function bodyObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("fixture request body was not a JSON object");
  }
  return value as Record<string, unknown>;
}

function questionNames(request: CapturedRequest, native: boolean): string[] {
  const questions = bodyObject(request.body).questions;
  if (native) {
    if (!Array.isArray(questions)) throw new Error("native questions were not an array");
    return questions.map((question) => {
      const answer = bodyObject(question);
      if (typeof answer.name !== "string") throw new Error("native question had no name");
      return answer.name;
    });
  }
  return Object.keys(bodyObject(questions));
}

function startFixture(
  respond: (request: CapturedRequest) => Response | Promise<Response>,
) {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(rawRequest) {
      const text = await rawRequest.text();
      let body: unknown;
      try {
        body = JSON.parse(text) as unknown;
      } catch {
        body = text;
      }
      const captured = {
        path: new URL(rawRequest.url).pathname,
        method: rawRequest.method,
        authorization: rawRequest.headers.get("authorization"),
        contentEncoding: rawRequest.headers.get("content-encoding"),
        body,
      };
      requests.push(captured);
      return await respond(captured);
    },
  });
  return { server, requests };
}

function config(protocol: EnabledDecisionsConfig["protocol"], baseUrl: string): EnabledDecisionsConfig {
  return {
    enabled: true,
    protocol,
    baseUrl,
    model: "fixture-model",
    apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
    apiKey: "fixture-decision-key",
  };
}

function candidate(id: string, kind: ReferenceKind = "pr"): ReferenceCandidate {
  return {
    id,
    kind,
    url: `https://github.com/example/service/pull/${id.replace(/[^0-9]/gu, "") || "17"}`,
    excerpt: `The current task explicitly asks to review candidate ${id}.`,
  };
}

function evidence(candidates: readonly ReferenceCandidate[]): ReferenceEvidence {
  return {
    thread: "Service change",
    task: "Review the service change and fix any regression.",
    recent: [
      { role: "user", text: "This pull request is the object of the current review." },
      { role: "assistant", text: "I will review that pull request." },
    ],
    candidates,
  };
}

function typeSafeAnswers(ids: readonly string[]): Record<string, unknown> {
  return Object.fromEntries(ids.map((id) => [id, {
    type: "choice",
    choice: "work_object",
    confidence: 0.01,
    probabilities: {
      work_object: 0.97,
      reference_or_unrelated: 0.02,
      insufficient_evidence: 0.01,
    },
  }]));
}

function openAiAnswers(ids: readonly string[]) {
  return ids.map((name) => ({
    name,
    type: "choice",
    choice: "work_object",
    confidence: 0.01,
    probabilities: ["work_object", "reference_or_unrelated", "insufficient_evidence"].map((value) => ({
      value,
      probability: value === "work_object" ? 0.97 : 0.015,
    })),
  }));
}

for (const protocol of ["typesafe-systemone", "openai-decisions"] as const) {
  test(`${protocol} sends its actual protocol over localhost and returns the selected-label probability`, async () => {
    const fixture = startFixture((request) => {
      const ids = questionNames(request, protocol === "openai-decisions");
      const answers = protocol === "openai-decisions" ? openAiAnswers(ids) : typeSafeAnswers(ids);
      return Response.json({ model: "fixture-model", answers, usage: { input_tokens: 1, output_tokens: 1 } });
    });

    // The request is over the SDK's compression threshold, so an absent encoding is observable.
    const source = evidence([candidate("candidate-17")]);
    source.task = "x".repeat(1_300);
    const judge = createReferenceJudge(config(protocol, fixture.server.url.origin));

    try {
      const judgments = await judge(source, new AbortController().signal);
      const request = fixture.requests[0];
      if (request === undefined) throw new Error("decision request did not reach localhost fixture");

      expect(judgments).toEqual([{ id: "candidate-17", choice: "work_object", probability: 0.97 }]);
      expect(fixture.requests).toHaveLength(1);
      expect(request.method).toBe("POST");
      expect(request.path).toBe(protocol === "openai-decisions" ? "/v1/decisions" : "/v1/systemone");
      expect(request.authorization).toBe("Bearer fixture-decision-key");
      expect(request.contentEncoding).toBeNull();

      const body = bodyObject(request.body);
      expect(body.model).toBe("fixture-model");
      if (protocol === "openai-decisions") {
        if (typeof body.input !== "string") throw new Error("native request input was not text");
        const input = bodyObject(JSON.parse(body.input) as unknown);
        expect(bodyObject(input.evidence).thread).toBe("Service change");
        expect(body.questions).toEqual([{
          name: "candidate-17",
          type: "choice",
          instructions: expect.any(String),
          choices: [
            { value: "work_object", description: expect.any(String) },
            { value: "reference_or_unrelated", description: expect.any(String) },
            { value: "insufficient_evidence", description: expect.any(String) },
          ],
        }]);
      } else {
        const state = bodyObject(body.state);
        const wireEvidence = bodyObject(state.evidence);
        expect(wireEvidence.thread).toBe("Service change");
        expect(Array.isArray(wireEvidence.candidates)).toBe(true);
        expect(bodyObject(body.questions)["candidate-17"]).toMatchObject({
          type: "choice",
          instructions: expect.any(String),
          criteria: {
            work_object: expect.any(String),
            reference_or_unrelated: expect.any(String),
            insufficient_evidence: expect.any(String),
          },
        });
      }
    } finally {
      fixture.server.stop(true);
    }
  });
}

test("instructions allow a current follow-up to match a same-thread delivery report without promoting history alone", async () => {
  const fixture = startFixture(request => Response.json({
    answers: openAiAnswers(questionNames(request, true)),
  }));
  const source = evidence([candidate("candidate-17")]);
  source.task = "Current status update: the earlier MR is merged; verify its follow-up.";
  source.matchedHistory = [{
    candidateId: "candidate-17",
    url: source.candidates[0]!.url,
    taskRequirement: "Prior accepted task: implement the service validation change.",
    reportExcerpt: "Prior final report: implementation delivered in this same thread.",
  }];
  const judge = createReferenceJudge(config("openai-decisions", fixture.server.url.origin));

  try {
    await judge(source, new AbortController().signal);
    const request = fixture.requests[0];
    if (request === undefined) throw new Error("decision request did not reach localhost fixture");
    const questions = bodyObject(request.body).questions;
    if (!Array.isArray(questions)) throw new Error("native questions were not an array");
    const instructions = bodyObject(questions[0]).instructions;
    if (typeof instructions !== "string") throw new Error("candidate question omitted instructions");

    expect(instructions).toContain("current status update");
    expect(instructions).toContain("same-thread delivery report");
    expect(instructions).toContain("follow-up");
    expect(instructions).toContain("historical report or another task's result alone is not current work-object evidence");
  } finally {
    fixture.server.stop(true);
  }
});

test("malformed, duplicate, refused, unknown and out-of-range answers omit only affected candidates", async () => {
  const ids = ["valid", "duplicate", "refused", "unknown-choice", "invalid-probability", "missing"];
  const fixture = startFixture((request) => {
    const answers = request.path === "/v1/decisions"
      ? [
        { name: "valid", type: "choice", choice: "work_object", confidence: 0.01, probabilities: [{ value: "work_object", probability: 0.96 }] },
        { name: "duplicate", type: "choice", choice: "work_object", probabilities: [{ value: "work_object", probability: 0.99 }] },
        { name: "duplicate", type: "refusal" },
        { name: "refused", type: "refusal" },
        { name: "unknown-choice", type: "choice", choice: "generated_url", probabilities: [{ value: "generated_url", probability: 1 }] },
        { name: "invalid-probability", type: "choice", choice: "work_object", probabilities: [{ value: "work_object", probability: 1.01 }] },
        { name: "server-added-id", type: "choice", choice: "work_object", probabilities: [{ value: "work_object", probability: 1 }] },
      ]
      : {
        valid: { type: "choice", choice: "work_object", confidence: 0.01, probabilities: { work_object: 0.96 } },
        refused: { type: "refusal" },
        "unknown-choice": { type: "choice", choice: "generated_url", confidence: 0.99, probabilities: { generated_url: 1 } },
        "invalid-probability": { type: "choice", choice: "work_object", confidence: 0.99, probabilities: { work_object: 1.01 } },
      };
    return Response.json({ answers });
  });

  try {
    const openAiJudge = createReferenceJudge(config("openai-decisions", fixture.server.url.origin));
    const openAiJudgments = await openAiJudge(evidence(ids.map((id) => candidate(id))), new AbortController().signal);
    expect(openAiJudgments).toEqual([{ id: "valid", choice: "work_object", probability: 0.96 }]);

    const typeSafeJudge = createReferenceJudge(config("typesafe-systemone", fixture.server.url.origin));
    const typeSafeJudgments = await typeSafeJudge(evidence(ids.map((id) => candidate(id))), new AbortController().signal);
    expect(typeSafeJudgments).toEqual([{ id: "valid", choice: "work_object", probability: 0.96 }]);
    expect(fixture.requests).toHaveLength(2);
  } finally {
    fixture.server.stop(true);
  }
});

test("an aborted judgement cancels a response body that never finishes", async () => {
  const bodyStarted = Promise.withResolvers<void>();
  const fixture = startFixture(() => new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('{"answers":'));
      bodyStarted.resolve();
    },
  }), { headers: { "content-type": "application/json" } }));
  const controller = new AbortController();

  try {
    const judge = createReferenceJudge(config("openai-decisions", fixture.server.url.origin));
    const pending = judge(evidence([candidate("candidate-17")]), controller.signal);
    await bodyStarted.promise;
    controller.abort(new DOMException("cancel fixture", "AbortError"));
    const result = await pending;
    expect(result).toEqual([]);
    expect(fixture.requests).toHaveLength(1);
  } finally {
    fixture.server.stop(true);
  }
});

test("the total deadline aborts a response body that stays open", async () => {
  const bodyStarted = Promise.withResolvers<void>();
  const fixture = startFixture(() => new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode("{"));
      bodyStarted.resolve();
    },
  }), { headers: { "content-type": "application/json" } }));
  vi.useFakeTimers();
  try {
    const judge = createReferenceJudge(config("openai-decisions", fixture.server.url.origin));
    const pending = judge(evidence([candidate("candidate-17")]), new AbortController().signal);
    await bodyStarted.promise;
    vi.advanceTimersByTime(3_000);
    const result = await pending;
    expect(result).toEqual([]);
    expect(fixture.requests).toHaveLength(1);
  } finally {
    vi.useRealTimers();
    fixture.server.stop(true);
  }
});

test("oversized response bodies are rejected before decoding", async () => {
  const fixture = startFixture(() => Response.json({ answers: [], unused: "x".repeat(70 * 1024) }));
  try {
    const judge = createReferenceJudge(config("openai-decisions", fixture.server.url.origin));
    const result = await judge(evidence([candidate("candidate-17")]), new AbortController().signal);
    expect(result).toEqual([]);
    expect(fixture.requests).toHaveLength(1);
  } finally {
    fixture.server.stop(true);
  }
});

test("redirects are rejected without following their location or issuing a fallback request", async () => {
  let fallbackRequests = 0;
  const fixture = startFixture((request) => {
    if (request.path === "/fallback") fallbackRequests++;
    return request.path === "/v1/systemone"
      ? new Response(null, { status: 302, headers: { location: "/fallback" } })
      : Response.json({ answers: [] });
  });

  try {
    const judge = createReferenceJudge(config("typesafe-systemone", fixture.server.url.origin));
    const result = await judge(evidence([candidate("candidate-17")]), new AbortController().signal);
    expect(result).toEqual([]);
    expect(fixture.requests.map((request) => request.path)).toEqual(["/v1/systemone"]);
    expect(fallbackRequests).toBe(0);
  } finally {
    fixture.server.stop(true);
  }
});

test("Typesafe failures emit no SDK diagnostics or response content", async () => {
  const fixture = startFixture(() => Response.json({ error: "private-response-marker" }, { status: 401 }));
  const captured: unknown[][] = [];
  const recordLog = (...args: unknown[]): void => {
    captured.push(args);
  };
  const original = {
    debug: console.debug,
    info: console.info,
    log: console.log,
    warn: console.warn,
    error: console.error,
  };
  console.debug = recordLog;
  console.info = recordLog;
  console.log = recordLog;
  console.warn = recordLog;
  console.error = recordLog;

  try {
    const judge = createReferenceJudge(config("typesafe-systemone", fixture.server.url.origin));
    const result = await judge(evidence([candidate("candidate-17")]), new AbortController().signal);
    expect(result).toEqual([]);
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]?.path).toBe("/v1/systemone");
    expect(JSON.stringify(captured)).not.toContain("private-response-marker");
    expect(JSON.stringify(captured)).not.toContain("fixture-decision-key");
    expect(captured).toEqual([]);
  } finally {
    console.debug = original.debug;
    console.info = original.info;
    console.log = original.log;
    console.warn = original.warn;
    console.error = original.error;
    fixture.server.stop(true);
  }
});
