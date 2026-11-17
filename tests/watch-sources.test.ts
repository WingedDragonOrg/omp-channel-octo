import { expect, test, vi } from "bun:test";
import { createWatchSource, parseWatchResource } from "../src/watch/sources.ts";
import type { WatchSourceObservation } from "../src/watch/sources.ts";

const GITHUB_API_HOST = "api.github.com";
const GITHUB_PR_PATH = "/repos/can1357/oh-my-pi/pulls/19544";
const GITHUB_COMMENTS_PATH = "/repos/can1357/oh-my-pi/issues/19544/comments";
const GITHUB_REVIEWS_PATH = "/repos/can1357/oh-my-pi/pulls/19544/reviews";
const GITLAB_HOST = "codex.mlamp.cn";
const GITLAB_PROJECT_PATH = "dmwork%2Focto-marketplace";
const GITLAB_MR_PATH = `/api/v4/projects/${GITLAB_PROJECT_PATH}/merge_requests/70`;
const GITLAB_NOTES_PATH = `${GITLAB_MR_PATH}/notes`;
const TEST_SOURCE_REQUEST_DEADLINE_ADVANCE_MS = 60_000;

interface CapturedRequest {
  url: URL;
  method: string;
  redirect: RequestRedirect;
}

interface SourceFixture {
  requests: CapturedRequest[];
  fetch: typeof globalThis.fetch;
  stop(): void;
}

function startFixture(
  apiHost: string,
  respond: (request: Request, url: URL) => Response | Promise<Response>,
): SourceFixture {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (request) => respond(request, new URL(request.url)),
  });

  // The adapter still constructs its real HTTPS API URL. This boundary rejects any
  // other host, then maps the request to an actual local HTTP fixture.
  const fetch: typeof globalThis.fetch = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const url = new URL(request.url);
      if (url.protocol !== "https:" || url.hostname !== apiHost) {
        throw new Error(`unexpected source host: ${url.origin}`);
      }
      requests.push({ url, method: request.method, redirect: request.redirect });

      const localUrl = new URL(`${url.pathname}${url.search}`, server.url);
      return await globalThis.fetch(new Request(localUrl, {
        method: request.method,
        headers: request.headers,
        signal: request.signal,
        redirect: request.redirect,
      }));
    },
    { preconnect: globalThis.fetch.preconnect.bind(globalThis.fetch) },
  );

  return { requests, fetch, stop: () => server.stop(true) };
}

function json(data: unknown, headers?: HeadersInit, status = 200): Response {
  const init: ResponseInit = {
    ...(headers === undefined ? {} : { headers }),
    status,
  };
  return Response.json(data, init);
}

function githubPullRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    state: "open",
    merged: false,
    title: "Initial title",
    head: { sha: "head-a" },
    updated_at: "2026-11-17T10:00:00Z",
    ...overrides,
  };
}

function githubComment(id: number, body: string, updatedAt: string): Record<string, unknown> {
  return {
    id,
    body,
    created_at: updatedAt,
    updated_at: updatedAt,
    user: { login: "reviewer" },
  };
}

function githubReview(id: number, body: string, submittedAt: string): Record<string, unknown> {
  return {
    id,
    body,
    state: "commented",
    submitted_at: submittedAt,
    user: { login: "reviewer" },
  };
}

function gitlabMergeRequest(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    state: "opened",
    title: "Initial title",
    merged_at: null,
    sha: "head-a",
    diff_refs: { head_sha: "head-a" },
    updated_at: "2026-11-17T10:00:00Z",
    ...overrides,
  };
}

function gitlabNote(id: number, body: string, updatedAt: string): Record<string, unknown> {
  return {
    id,
    body,
    created_at: updatedAt,
    updated_at: updatedAt,
    system: false,
    author: { username: "reviewer" },
  };
}

function githubNextPage(url: URL): HeadersInit {
  const next = new URL(`https://${GITHUB_API_HOST}${url.pathname}${url.search}`);
  next.searchParams.set("page", "2");
  return { link: `<${next.href}>; rel="next"` };
}

function eventIdentities(events: readonly { identity: string }[]): string[] {
  return events.map((event) => event.identity);
}

test("parses GitHub PR identifiers and HTTPS URLs, and preserves an explicit GitLab host", () => {
  const githubScheme = parseWatchResource("github://can1357/oh-my-pi/pull/19544");
  const githubHttps = parseWatchResource("https://github.com/can1357/oh-my-pi/pull/19544");
  expect(githubScheme).toMatchObject({
    provider: "github",
    host: "github.com",
    owner: "can1357",
    repo: "oh-my-pi",
    number: 19544,
  });
  expect(githubHttps).toEqual(githubScheme);

  const gitlabScheme = parseWatchResource(
    "gitlab://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
  );
  const gitlabHttps = parseWatchResource(
    "https://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
  );
  expect(gitlabScheme).toMatchObject({
    provider: "gitlab",
    host: "codex.mlamp.cn",
    projectPath: "dmwork/octo-marketplace",
    iid: 70,
  });
  expect(gitlabHttps).toEqual(gitlabScheme);
  expect(() => parseWatchResource("gitlab:///dmwork/octo-marketplace/-/merge_requests/70")).toThrow();
  expect(() => parseWatchResource("http://github.com/can1357/oh-my-pi/pull/19544")).toThrow();
});

test("GitHub establishes a single-page baseline, emits only new comments and reviews once, and replays stable identities", async () => {
  let phase: "baseline" | "changed" = "baseline";
  const oldComment = githubComment(101, "already present", "2026-11-16T10:00:00Z");
  const secondBaselineComment = githubComment(102, "also already present", "2026-11-16T11:00:00Z");
  const newCommentAt = new Date(Date.now() + 3_600_000).toISOString();
  const newComment = githubComment(103, "new comment", newCommentAt);
  const oldReview = githubReview(201, "already reviewed", "2026-11-16T12:00:00Z");
  const secondBaselineReview = githubReview(202, "also already reviewed", "2026-11-16T13:00:00Z");
  const newReviewAt = new Date(Date.now() + 3_600_000).toISOString();
  const newReview = githubReview(203, "new review", newReviewAt);

  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      if (phase === "changed") return json([oldComment, secondBaselineComment, newComment, newComment]);
      return json([oldComment, secondBaselineComment]);
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) {
      if (phase === "changed") return json([oldReview, secondBaselineReview, newReview]);
      return json([oldReview, secondBaselineReview]);
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const resource = parseWatchResource("github://can1357/oh-my-pi/pull/19544");
    const source = createWatchSource({ resource, fetch: fixture.fetch });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete single-page GitHub baseline");
    expect(baseline.events).toEqual([]);
    expect(JSON.parse(JSON.stringify(baseline.cursor))).toEqual(baseline.cursor);
    expect(fixture.requests.every(({ url }) =>
      (url.pathname !== GITHUB_COMMENTS_PATH && url.pathname !== GITHUB_REVIEWS_PATH) ||
      url.searchParams.get("page") === "1",
    )).toBe(true);
    const initialListRequests = fixture.requests.filter(({ url }) =>
      url.pathname === GITHUB_COMMENTS_PATH || url.pathname === GITHUB_REVIEWS_PATH,
    );
    expect(initialListRequests).toHaveLength(2);
    expect(initialListRequests.every(({ url }) => url.searchParams.get("per_page") === "100")).toBe(true);

    phase = "changed";
    const changed = await source.poll(baseline.cursor, signal);
    expect(changed.kind).toBe("success");
    if (changed.kind !== "success") throw new Error("expected GitHub events");
    expect(fixture.requests.filter(({ url }) =>
      url.pathname === GITHUB_COMMENTS_PATH || url.pathname === GITHUB_REVIEWS_PATH,
    )).toHaveLength(4);
    const comments = changed.events.filter((event) => event.type === "comment");
    const reviews = changed.events.filter((event) => event.type === "review");
    expect(comments).toHaveLength(1);
    expect(reviews).toHaveLength(1);
    expect(comments[0]).toMatchObject({ version: newCommentAt });
    expect(reviews[0]!.version.startsWith(`${newReviewAt}#sha256:`)).toBe(true);
    expect(new Set(eventIdentities(changed.events)).size).toBe(changed.events.length);

    const replay = await source.poll(JSON.parse(JSON.stringify(baseline.cursor)), signal);
    expect(replay.kind).toBe("success");
    if (replay.kind !== "success") throw new Error("expected replayable GitHub events");
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(changed.events));

    const settled = await source.poll(changed.cursor, signal);
    expect(settled.kind).toBe("success");
    if (settled.kind !== "success") throw new Error("expected settled GitHub cursor");
    expect(settled.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});

test("GitHub snapshot transitions preserve A-to-B-to-A and replay event identities from a serialized cursor", async () => {
  let current = githubPullRequest({ title: "A", head: { sha: "head-a" } });
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(current);
    if (url.pathname === GITHUB_COMMENTS_PATH || url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("https://github.com/can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const initial = await source.initialize(signal);
    expect(initial.kind).toBe("success");
    if (initial.kind !== "success") throw new Error("expected initial snapshot");
    expect(initial.snapshot).toMatchObject({ title: "A", headSha: "head-a" });

    current = githubPullRequest({ title: "B", head: { sha: "head-b" } });
    const toB = await source.poll(initial.cursor, signal);
    expect(toB.kind).toBe("success");
    if (toB.kind !== "success") throw new Error("expected A-to-B transition");
    expect(toB.snapshot).toMatchObject({ title: "B", headSha: "head-b" });
    expect(toB.events).toHaveLength(1);

    const replay = await source.poll(JSON.parse(JSON.stringify(initial.cursor)), signal);
    expect(replay.kind).toBe("success");
    if (replay.kind !== "success") throw new Error("expected replay from serialized cursor");
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(toB.events));

    current = githubPullRequest({ title: "A", head: { sha: "head-a" } });
    const toA = await source.poll(toB.cursor, signal);
    expect(toA.kind).toBe("success");
    if (toA.kind !== "success") throw new Error("expected B-to-A transition");
    expect(toA.snapshot).toMatchObject({ title: "A", headSha: "head-a" });
    expect(toA.events).toHaveLength(1);
    expect(toA.events[0]!.identity).not.toBe(toB.events[0]!.identity);
  } finally {
    fixture.stop();
  }
});

test("GitLab uses the resource's explicit host, baselines a single-page notes response, and deduplicates repeated note versions", async () => {
  let phase: "baseline" | "changed" = "baseline";
  const oldNote = gitlabNote(401, "historical note", "2026-11-16T10:00:00Z");
  const secondBaselineNote = gitlabNote(402, "another historical note", "2026-11-16T11:00:00Z");
  const newNoteAt = new Date(Date.now() + 3_600_000).toISOString();
  const newNote = gitlabNote(403, "new note", newNoteAt);

  const fixture = startFixture(GITLAB_HOST, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) {
      if (phase === "changed") return json([oldNote, secondBaselineNote, newNote, newNote]);
      return json([oldNote, secondBaselineNote], { "x-next-page": "" });
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const resource = parseWatchResource(
      "gitlab://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
    );
    const source = createWatchSource({ resource, fetch: fixture.fetch });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete single-page GitLab baseline");
    expect(baseline.events).toEqual([]);
    expect(fixture.requests.every(({ url }) => url.hostname === GITLAB_HOST)).toBe(true);
    expect(fixture.requests.every(({ url }) =>
      url.pathname !== GITLAB_NOTES_PATH || url.searchParams.get("page") === "1",
    )).toBe(true);
    const initialListRequests = fixture.requests.filter(({ url }) => url.pathname === GITLAB_NOTES_PATH);
    expect(initialListRequests).toHaveLength(1);
    expect(initialListRequests[0]?.url.searchParams.get("per_page")).toBe("100");

    phase = "changed";
    const changed = await source.poll(baseline.cursor, signal);
    expect(changed.kind).toBe("success");
    expect(fixture.requests.filter(({ url }) => url.pathname === GITLAB_NOTES_PATH)).toHaveLength(2);
    if (changed.kind !== "success") throw new Error("expected GitLab note event");
    const notes = changed.events.filter((event) => event.type === "note");
    expect(notes).toHaveLength(1);
    expect(notes[0]).toMatchObject({ version: newNoteAt });
    const settled = await source.poll(changed.cursor, signal);
    expect(settled.kind).toBe("success");
    if (settled.kind !== "success") throw new Error("expected settled GitLab cursor");
    expect(settled.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});

test("GitHub completes two bounded scans and deduplicates stable comments", async () => {
  const comments = Array.from({ length: 101 }, (_, index) => {
    const createdAt = new Date(Date.UTC(2026, 10, 16, 0, 0, index)).toISOString();
    return githubComment(800 + index, `baseline item ${index}`, createdAt);
  });
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const page = Number(url.searchParams.get("page"));
      const perPage = Number(url.searchParams.get("per_page"));
      const start = (page - 1) * perPage;
      return json(
        comments.slice(start, start + perPage),
        start + perPage < comments.length ? githubNextPage(url) : undefined,
      );
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("expected a stable baseline across both full scans");
    expect(result.events).toEqual([]);
    expect(Object.keys(result.cursor.versions.comments)).toHaveLength(101);
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2", "1", "2"]);
  } finally {
    fixture.stop();
  }
});

test("GitHub follows a reviews next-page link when establishing the baseline", async () => {
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) return json([]);
    if (url.pathname === GITHUB_REVIEWS_PATH) {
      if (url.searchParams.get("page") === "2") return json([]);
      return json([githubReview(551, "baseline review", "2026-11-16T10:00:00Z")], githubNextPage(url));
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("expected a baseline across both review pages");
    expect(result.events).toEqual([]);
    expect(Object.keys(result.cursor.versions.reviews)).toEqual(["551"]);
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_REVIEWS_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2", "1", "2"]);
  } finally {
    fixture.stop();
  }
});

test("GitLab completes two bounded scans and deduplicates stable notes", async () => {
  const notes = Array.from({ length: 101 }, (_, index) => {
    const createdAt = new Date(Date.UTC(2026, 10, 16, 0, 0, index)).toISOString();
    return gitlabNote(900 + index, `baseline note ${index}`, createdAt);
  });
  const fixture = startFixture(GITLAB_HOST, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) {
      const page = Number(url.searchParams.get("page"));
      const perPage = Number(url.searchParams.get("per_page"));
      const start = (page - 1) * perPage;
      return json(
        notes.slice(start, start + perPage),
        { "x-next-page": start + perPage < notes.length ? String(page + 1) : "" },
      );
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(
        "https://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
      ),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("expected a stable baseline across both full scans");
    expect(result.events).toEqual([]);
    expect(Object.keys(result.cursor.versions.notes)).toHaveLength(101);
    expect(fixture.requests.filter(({ url }) => url.pathname === GITLAB_NOTES_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2", "1", "2"]);
    expect(fixture.requests.every(({ url }) => url.hostname === GITLAB_HOST)).toBe(true);
  } finally {
    fixture.stop();
  }
});


test("a page shift during polling is detected by the second full scan", async () => {
  const records = Array.from({ length: 200 }, (_, index) => {
    const createdAt = new Date(Date.UTC(2026, 0, 1) + index * 1_000)
      .toISOString()
      .replace(".000Z", "Z");
    return githubComment(2_000 + index, `comment ${index}`, createdAt);
  });
  let shiftDuringPoll = false;
  let shifted = false;
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const page = Number(url.searchParams.get("page") ?? "1");
      const perPage = Number(url.searchParams.get("per_page"));
      const start = (page - 1) * perPage;
      const response = json(
        records.slice(start, start + perPage),
        start + perPage < records.length ? githubNextPage(url) : undefined,
      );
      if (shiftDuringPoll && !shifted && page === 1) {
        shifted = true;
        records.splice(49, 1);
      }
      return response;
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected a stable multi-page baseline");
    expect(Object.keys(baseline.cursor.versions.comments)).toHaveLength(200);

    shiftDuringPoll = true;
    const result = await source.poll(baseline.cursor, signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "inconsistent-scan" } });
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page")))
      .toEqual(["1", "2", "1", "2", "1", "2", "1", "2"]);
  } finally {
    fixture.stop();
  }
});


test("overlapping GitHub pages with same-timestamp objects reordered report a gap", async () => {
  const createdAt = "2026-11-16T10:00:00Z";
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      if (url.searchParams.get("page") === "2") {
        return json([
          githubComment(702, "second", createdAt),
          githubComment(701, "first", createdAt),
        ]);
      }
      return json([
        githubComment(701, "first", createdAt),
        githubComment(702, "second", createdAt),
      ], githubNextPage(url));
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "unordered-page" } });
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2"]);
  } finally {
    fixture.stop();
  }
});

test("overlapping GitHub pages with conflicting versions for one object report a gap", async () => {
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      return url.searchParams.get("page") === "2"
        ? json([githubComment(703, "edited between pages", "2026-11-16T11:00:00Z")])
        : json([githubComment(703, "baseline copy", "2026-11-16T10:00:00Z")], githubNextPage(url));
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "inconsistent-page" } });
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2"]);
  } finally {
    fixture.stop();
  }
});

test("a note permission rejection is explicit and never exposes the API response body", async () => {
  const fixture = startFixture(GITLAB_HOST, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    return json({ message: "private authorization details" }, undefined, 403);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(
        "gitlab://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
      ),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result).toMatchObject({ kind: "failure", failure: { code: "authorization-denied" } });
    expect(JSON.stringify(result)).not.toContain("private authorization details");
    expect(fixture.requests.every(({ method }) => method === "GET")).toBe(true);
    expect(fixture.requests.every(({ url }) => url.hostname === GITLAB_HOST)).toBe(true);
  } finally {
    fixture.stop();
  }
});

test("events created during the initial scan are candidates and are not re-emitted after cursor persistence", async () => {
  const createdAt = new Date(Date.now() + 3_600_000).toISOString();
  const duringBaseline = githubComment(701, "created during scan", createdAt);
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) return json([duringBaseline]);
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete GitHub baseline");
    expect(baseline.events).toHaveLength(1);
    expect(baseline.events[0]).toMatchObject({ type: "comment", version: createdAt });

    const replay = await source.poll(JSON.parse(JSON.stringify(baseline.cursor)), signal);
    expect(replay.kind).toBe("success");
    if (replay.kind !== "success") throw new Error("expected a replayable GitHub cursor");
    expect(replay.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});

test("disappearance of a baselined GitHub comment reports a coverage gap", async () => {
  let visible = true;
  const comment = githubComment(801, "will disappear", "2026-11-16T10:00:00Z");
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) return json(visible ? [comment] : []);
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete GitHub baseline");

    visible = false;
    const result = await source.poll(baseline.cursor, signal);
    expect(result.kind).toBe("gap");
    if (result.kind !== "gap") throw new Error("a deleted source object must report a coverage gap");
    expect(result.gap.code).toBe("source-object-missing");
  } finally {
    fixture.stop();
  }
});

test("source request hooks surround every paginated API request", async () => {
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      return url.searchParams.get("page") === "1" ? json([], githubNextPage(url)) : json([]);
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });
  const statuses: number[] = [];
  let beforeRequests = 0;

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
      beforeRequest: () => { beforeRequests += 1; },
      afterResponse: (response) => { statuses.push(response.status); },
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result.kind).toBe("success");
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page"))).toEqual(["1", "2", "1", "2"]);
    expect(beforeRequests).toBe(fixture.requests.length);
    expect(statuses).toEqual(fixture.requests.map(() => 200));
    expect(fixture.requests.every(({ method }) => method === "GET")).toBe(true);
  } finally {
    fixture.stop();
  }
});

test("a source pagination sequence stops at the bounded page limit and reports a gap", async () => {
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const next = new URL(`https://${GITHUB_API_HOST}${url.pathname}${url.search}`);
      next.searchParams.set("page", String(Number(url.searchParams.get("page")) + 1));
      return json([], { link: `<${next.href}>; rel="next"` });
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "pagination-limit" } });
    const commentPages = fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)
      .map(({ url }) => url.searchParams.get("page"));
    expect(commentPages).toEqual(Array.from({ length: 100 }, (_, index) => String(index + 1)));
    expect(JSON.stringify(result)).not.toMatch(/no change|unchanged/i);
  } finally {
    fixture.stop();
  }
});

test("a source HTTP response that never arrives fails with an explicit timeout", async () => {
  const requestArrived = Promise.withResolvers<void>();
  const fixture = startFixture(GITHUB_API_HOST, () => {
    requestArrived.resolve();
    return new Promise<Response>(() => {});
  });

  vi.useFakeTimers();
  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const pending = source.initialize(new AbortController().signal);
    await requestArrived.promise;
    vi.advanceTimersByTime(TEST_SOURCE_REQUEST_DEADLINE_ADVANCE_MS);

    const result = await pending;
    expect(result.kind).toBe("failure");
    if (result.kind !== "failure") throw new Error("a stalled request must not look like an unchanged source");
    expect(result.failure.code).toMatch(/timeout/i);
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]).toMatchObject({ method: "GET", url: { pathname: GITHUB_PR_PATH } });
  } finally {
    vi.useRealTimers();
    fixture.stop();
  }
});

test("a source body that never ends times out during polling without exposing its contents as no change", async () => {
  const tokenInBody = "ghp_source_fixture_secret_not_for_output";
  let stallBody = false;
  let waitingForStalledResponse = false;
  const stalledResponseReceived = Promise.withResolvers<void>();
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) {
      if (stallBody) {
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(`{"token":"${tokenInBody}",`));
            },
          }),
          { headers: { "content-type": "application/json" } },
        );
      }
      return json(githubPullRequest());
    }
    if (url.pathname === GITHUB_COMMENTS_PATH || url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
      afterResponse: () => {
        if (waitingForStalledResponse) stalledResponseReceived.resolve();
      },
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected a complete source baseline");

    vi.useFakeTimers();
    try {
      stallBody = true;
      waitingForStalledResponse = true;
      const pending = source.poll(baseline.cursor, signal);
      await stalledResponseReceived.promise;
      await Promise.resolve();
      vi.advanceTimersByTime(TEST_SOURCE_REQUEST_DEADLINE_ADVANCE_MS);

      const result = await pending;
      expect(result.kind).toBe("failure");
      if (result.kind !== "failure") throw new Error("a timed-out poll must not report no changes");
      expect(result.failure.code).toMatch(/timeout/i);
      expect(JSON.stringify(result)).not.toContain(tokenInBody);
      expect(fixture.requests).toHaveLength(5);
    } finally {
      vi.useRealTimers();
    }
  } finally {
    fixture.stop();
  }
});

test("an explicit caller abort during a stalled source request remains cancelled", async () => {
  const requestArrived = Promise.withResolvers<void>();
  const fixture = startFixture(GITHUB_API_HOST, () => {
    requestArrived.resolve();
    return new Promise<Response>(() => {});
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const controller = new AbortController();
    const pending = source.initialize(controller.signal);
    await requestArrived.promise;
    controller.abort();

    const result = await pending;
    expect(result).toEqual({ kind: "failure", failure: { code: "cancelled" } });
    expect(fixture.requests).toHaveLength(1);
  } finally {
    fixture.stop();
  }
});

test("GitLab note edits emit the new observable version and replay with the same identity", async () => {
  const existing = gitlabNote(902, "first note version", "2026-11-16T10:00:00Z");
  let current = existing;
  const fixture = startFixture(GITLAB_HOST, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) return json([current]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(
        "gitlab://codex.mlamp.cn/dmwork/octo-marketplace/-/merge_requests/70",
      ),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete GitLab baseline");
    expect(baseline.events).toEqual([]);

    current = { ...existing, body: "edited note version", updated_at: "2026-11-17T11:00:00Z" };
    const edited = await source.poll(baseline.cursor, signal);
    expect(edited.kind).toBe("success");
    if (edited.kind !== "success") throw new Error("expected an edited GitLab note");
    expect(edited.events).toHaveLength(1);
    expect(edited.events[0]).toMatchObject({ type: "note", version: "2026-11-17T11:00:00Z" });

    const replay = await source.poll(JSON.parse(JSON.stringify(baseline.cursor)), signal);
    expect(replay.kind).toBe("success");
    if (replay.kind !== "success") throw new Error("expected a replayable GitLab cursor");
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(edited.events));

    const settled = await source.poll(edited.cursor, signal);
    expect(settled.kind).toBe("success");
    if (settled.kind !== "success") throw new Error("expected a settled GitLab cursor");
    expect(settled.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});
test("GitHub rejects a cross-origin pagination link before requesting it", async () => {
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      return json([], {
        link: `<https://attacker.example${GITHUB_COMMENTS_PATH}?per_page=100&page=2&sort=created&direction=asc>; rel="next"`,
      });
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "invalid-pagination" } });
    expect(fixture.requests.every(({ url }) => url.hostname === GITHUB_API_HOST)).toBe(true);
    expect(fixture.requests.filter(({ url }) => url.pathname === GITHUB_COMMENTS_PATH)).toHaveLength(1);
  } finally {
    fixture.stop();
  }
});

test("source requests reject redirects rather than following them to another host", async () => {
  const fixture = startFixture(GITHUB_API_HOST, () =>
    new Response(null, {
      status: 302,
      headers: { location: "https://attacker.example/collect" },
    }),
  );

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const result = await source.initialize(new AbortController().signal);
    expect(result.kind).toBe("failure");
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]).toMatchObject({ method: "GET", redirect: "error" });
  } finally {
    fixture.stop();
  }
});
test("GitHub pending reviews are not events until submitted", async () => {
  const pendingReview = {
    ...githubReview(1101, "draft review", ""),
    state: "PENDING",
    submitted_at: null,
  };
  const submittedAt = new Date(Date.now() + 3_600_000).toISOString();
  let submitted = false;
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) return json([]);
    if (url.pathname === GITHUB_REVIEWS_PATH) {
      return json(submitted
        ? [githubReview(1101, "submitted review", submittedAt)]
        : [pendingReview]);
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("pending reviews must not block the baseline");
    expect(baseline.events).toEqual([]);

    submitted = true;
    const result = await source.poll(baseline.cursor, signal);
    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("a submitted review must become an event");
    expect(result.events).toHaveLength(1);
    expect(result.events[0]!.version.startsWith(`${submittedAt}#sha256:`)).toBe(true);
  } finally {
    fixture.stop();
  }
});

test("GitHub baseline gaps when a second-precision timestamp overlaps its millisecond boundary", async () => {
  const baselineStartedAt = Date.UTC(2026, 10, 17, 10, 0, 0, 500);
  const sourceTimestamp = new Date(Math.floor(baselineStartedAt / 1_000) * 1_000)
    .toISOString()
    .replace(".000Z", "Z");
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      return json([githubComment(2_301, "boundary comment", sourceTimestamp)]);
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const originalNow = Date.now;
    let initialization: Promise<WatchSourceObservation>;
    try {
      Date.now = () => baselineStartedAt;
      initialization = source.initialize(new AbortController().signal);
    } finally {
      Date.now = originalNow;
    }
    const result = await initialization;
    expect(result).toMatchObject({ kind: "gap", gap: { code: "baseline-timestamp-precision" } });
  } finally {
    fixture.stop();
  }
});

test("GitHub review body and state changes create distinct replay-stable versions", async () => {
  const submittedAt = "2026-11-16T10:00:00Z";
  let review = githubReview(2_401, "original review", submittedAt);
  const fixture = startFixture(GITHUB_API_HOST, (_request, url) => {
    if (url.pathname === GITHUB_PR_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) return json([]);
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([review]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://can1357/oh-my-pi/pull/19544"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = await source.initialize(signal);
    expect(baseline.kind).toBe("success");
    if (baseline.kind !== "success") throw new Error("expected complete review baseline");
    const legacyCursor = {
      ...baseline.cursor,
      versions: { ...baseline.cursor.versions, reviews: { "2401": submittedAt } },
    };
    expect(await source.poll(legacyCursor, signal)).toMatchObject({
      kind: "gap",
      gap: { code: "review-version-upgrade-required" },
    });

    review = { ...review, body: "edited review" };
    const edited = await source.poll(baseline.cursor, signal);
    expect(edited.kind).toBe("success");
    if (edited.kind !== "success") throw new Error("expected edited review event");
    expect(edited.events).toHaveLength(1);
    expect(edited.events[0]).toMatchObject({ type: "review", body: { text: "edited review" } });
    expect(edited.events[0]!.version).toMatch(/#sha256:[a-f0-9]{64}$/);
    const editedIdentity = edited.events[0]!.identity;

    review = { ...review, state: "DISMISSED" };
    const dismissed = await source.poll(edited.cursor, signal);
    expect(dismissed.kind).toBe("success");
    if (dismissed.kind !== "success") throw new Error("expected dismissed review event");
    expect(dismissed.events).toHaveLength(1);
    expect(dismissed.events[0]).toMatchObject({ type: "review", body: { state: "DISMISSED" } });
    expect(dismissed.events[0]!.identity).not.toBe(editedIdentity);

    const replay = await source.poll(edited.cursor, signal);
    expect(replay.kind).toBe("success");
    if (replay.kind !== "success") throw new Error("expected replayable review version");
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(dismissed.events));
  } finally {
    fixture.stop();
  }
});
