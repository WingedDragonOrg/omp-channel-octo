import { expect, test } from "bun:test";
import { createWatchSource, parseWatchResource } from "../src/watch/sources.ts";
import type { WatchSourceObservation } from "../src/watch/sources.ts";

const GITHUB_ORIGIN = "https://api.github.com";
const GITHUB_PULL_PATH = "/repos/acme/widget/pulls/7";
const GITHUB_COMMENTS_PATH = "/repos/acme/widget/issues/7/comments";
const GITHUB_REVIEWS_PATH = "/repos/acme/widget/pulls/7/reviews";
const GITLAB_HOST = "pagination.gitlab.test";
const GITLAB_ORIGIN = `https://${GITLAB_HOST}`;
const GITLAB_MR_PATH = "/api/v4/projects/team%2Fproject/merge_requests/3";
const GITLAB_NOTES_PATH = `${GITLAB_MR_PATH}/notes`;
const PAGE_SIZE = 100;

type Provider = "github" | "gitlab";

interface CapturedRequest {
  url: URL;
  method: string;
}

interface SourceFixture {
  requests: CapturedRequest[];
  fetch: typeof globalThis.fetch;
  stop(): void;
}

interface ProviderCase {
  name: string;
  provider: Provider;
  origin: string;
  resourceUrl: string;
  snapshotPath: string;
  listPath: string;
}

const PROVIDERS: readonly ProviderCase[] = [
  {
    name: "GitHub",
    provider: "github",
    origin: GITHUB_ORIGIN,
    resourceUrl: "github://acme/widget/pull/7",
    snapshotPath: GITHUB_PULL_PATH,
    listPath: GITHUB_COMMENTS_PATH,
  },
  {
    name: "GitLab",
    provider: "gitlab",
    origin: GITLAB_ORIGIN,
    resourceUrl: `gitlab://${GITLAB_HOST}/team/project/-/merge_requests/3`,
    snapshotPath: GITLAB_MR_PATH,
    listPath: GITLAB_NOTES_PATH,
  },
];

function startFixture(
  origin: string,
  respond: (request: Request, url: URL) => Response | Promise<Response>,
): SourceFixture {
  const requests: CapturedRequest[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (request) => respond(request, new URL(request.url)),
  });
  const fetch: typeof globalThis.fetch = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      const url = new URL(request.url);
      if (url.origin !== origin) throw new Error(`unexpected source origin: ${url.origin}`);
      requests.push({ url, method: request.method });
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

function json(value: unknown, headers?: HeadersInit, status = 200): Response {
  return Response.json(value, { ...(headers === undefined ? {} : { headers }), status });
}

function githubPullRequest(): Record<string, unknown> {
  return {
    state: "open",
    merged: false,
    title: "Initial title",
    head: { sha: "head-a" },
    updated_at: "2025-01-01T00:00:00Z",
  };
}

function gitlabMergeRequest(): Record<string, unknown> {
  return {
    state: "opened",
    title: "Initial title",
    merged_at: null,
    sha: "head-a",
    diff_refs: { head_sha: "head-a" },
    updated_at: "2025-01-01T00:00:00Z",
  };
}

function historicalTimestamp(index: number): string {
  return new Date(Date.UTC(2020, 0, 1) + index * 1_000).toISOString();
}

function githubComment(id: number, createdAt: string, body = `comment ${id}`): Record<string, unknown> {
  return {
    id,
    body,
    created_at: createdAt,
    updated_at: createdAt,
    user: { login: "reviewer" },
  };
}

function githubReview(id: number, submittedAt: string): Record<string, unknown> {
  return {
    id,
    body: `review ${id}`,
    state: "commented",
    submitted_at: submittedAt,
    user: { login: "reviewer" },
  };
}

function gitlabNote(id: number, createdAt: string, body = `note ${id}`): Record<string, unknown> {
  return {
    id,
    body,
    created_at: createdAt,
    updated_at: createdAt,
    system: false,
    author: { username: "reviewer" },
  };
}

function githubComments(count: number, firstId: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, index) =>
    githubComment(firstId + index, historicalTimestamp(index)));
}

function githubReviews(count: number, firstId: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, index) =>
    githubReview(firstId + index, historicalTimestamp(index)));
}

function gitlabNotes(count: number, firstId: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, index) =>
    gitlabNote(firstId + index, historicalTimestamp(index)));
}

function paginationHeaders(url: URL, provider: Provider, nextPage: number | null): HeadersInit {
  if (nextPage === null) return provider === "gitlab" ? { "x-next-page": "" } : {};
  const origin = provider === "github" ? GITHUB_ORIGIN : GITLAB_ORIGIN;
  const next = new URL(`${url.pathname}${url.search}`, origin);
  next.searchParams.set("page", String(nextPage));
  const link = `<${next.href}>; rel="next"`;
  return provider === "github"
    ? { link }
    : { link, "x-next-page": String(nextPage) };
}

function pageResponse(
  url: URL,
  provider: Provider,
  pageItems: readonly Record<string, unknown>[],
  nextPage: number | null,
): Response {
  return json(pageItems, paginationHeaders(url, provider, nextPage));
}

function regularPageResponse(
  url: URL,
  records: readonly Record<string, unknown>[],
  provider: Provider,
): Response {
  const page = Number(url.searchParams.get("page"));
  const pageSize = Number(url.searchParams.get("per_page"));
  if (!Number.isSafeInteger(page) || page < 1 || !Number.isSafeInteger(pageSize) || pageSize < 1) {
    throw new Error("source list request must include positive page and per_page parameters");
  }
  const start = (page - 1) * pageSize;
  const nextPage = start + pageSize < records.length ? page + 1 : null;
  return pageResponse(url, provider, records.slice(start, start + pageSize), nextPage);
}

function successful(
  result: WatchSourceObservation,
): Extract<WatchSourceObservation, { kind: "success" }> {
  if (result.kind !== "success") throw new Error(`expected source success, got ${JSON.stringify(result)}`);
  return result;
}

function eventIdentities(events: readonly { identity: string }[]): string[] {
  return events.map((event) => event.identity);
}

function pageNumbers(requests: readonly CapturedRequest[], path: string): string[] {
  return requests
    .filter(({ url }) => url.pathname === path)
    .map(({ url }) => url.searchParams.get("page") ?? "<missing>");
}


test("GitHub baselines multi-page history, captures later-page events, and replays serialized cursors once advanced", async () => {
  let phase: "baseline" | "poll" = "baseline";
  const comments = githubComments(215, 1_000);
  const reviews = githubReviews(115, 2_000);
  const newAt = new Date(Date.now() + 60_000).toISOString();
  const newComment = githubComment(1_999, newAt, "new comment on a later page");
  const newReview = githubReview(2_999, newAt);
  const fixture = startFixture(GITHUB_ORIGIN, (_request, url) => {
    if (url.pathname === GITHUB_PULL_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const records = phase === "baseline" ? comments : [...comments, newComment];
      return regularPageResponse(url, records, "github");
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) {
      const records = phase === "baseline" ? reviews : [...reviews, newReview];
      return regularPageResponse(url, records, "github");
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://acme/widget/pull/7"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = successful(await source.initialize(signal));

    expect(baseline.events).toEqual([]);
    expect(Object.keys(baseline.cursor.versions.comments)).toHaveLength(215);
    expect(Object.keys(baseline.cursor.versions.reviews)).toHaveLength(115);
    expect(pageNumbers(fixture.requests, GITHUB_COMMENTS_PATH)).toEqual(["1", "2", "3", "1", "2", "3"]);
    expect(pageNumbers(fixture.requests, GITHUB_REVIEWS_PATH)).toEqual(["1", "2", "1", "2"]);
    expect(fixture.requests.filter(({ url }) =>
      url.pathname === GITHUB_COMMENTS_PATH || url.pathname === GITHUB_REVIEWS_PATH,
    ).every(({ url }) => url.searchParams.get("per_page") === String(PAGE_SIZE))).toBe(true);

    const restoredCursor = JSON.parse(JSON.stringify(baseline.cursor));
    expect(restoredCursor).toEqual(baseline.cursor);
    phase = "poll";
    const firstPoll = successful(await source.poll(restoredCursor, signal));
    expect(firstPoll.events).toHaveLength(2);
    expect(firstPoll.events.map((event) => event.type).sort()).toEqual(["comment", "review"]);
    expect(firstPoll.events.find((event) => event.type === "comment")?.body.objectId).toBe("1999");
    expect(firstPoll.events.find((event) => event.type === "review")?.body.objectId).toBe("2999");
    expect(new Set(eventIdentities(firstPoll.events)).size).toBe(firstPoll.events.length);
    expect(JSON.parse(JSON.stringify(firstPoll.cursor))).toEqual(firstPoll.cursor);

    const replay = successful(await source.poll(restoredCursor, signal));
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(firstPoll.events));
    const advanced = successful(await source.poll(JSON.parse(JSON.stringify(firstPoll.cursor)), signal));
    expect(advanced.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});

test("GitLab baselines multi-page notes, captures a later-page event, and replays a serialized cursor idempotently", async () => {
  let phase: "baseline" | "poll" = "baseline";
  const notes = gitlabNotes(215, 3_000);
  const newAt = new Date(Date.now() + 60_000).toISOString();
  const newNote = gitlabNote(3_999, newAt, "new note on a later page");
  const fixture = startFixture(GITLAB_ORIGIN, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) {
      const records = phase === "baseline" ? notes : [...notes, newNote];
      return regularPageResponse(url, records, "gitlab");
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(`gitlab://${GITLAB_HOST}/team/project/-/merge_requests/3`),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = successful(await source.initialize(signal));

    expect(baseline.events).toEqual([]);
    expect(Object.keys(baseline.cursor.versions.notes)).toHaveLength(215);
    expect(pageNumbers(fixture.requests, GITLAB_NOTES_PATH)).toEqual(["1", "2", "3", "1", "2", "3"]);
    expect(fixture.requests.filter(({ url }) => url.pathname === GITLAB_NOTES_PATH)
      .every(({ url }) => url.searchParams.get("per_page") === String(PAGE_SIZE))).toBe(true);

    phase = "poll";
    const restoredCursor = JSON.parse(JSON.stringify(baseline.cursor));
    const firstPoll = successful(await source.poll(restoredCursor, signal));
    expect(firstPoll.events).toHaveLength(1);
    expect(firstPoll.events[0]).toMatchObject({ type: "note", body: { objectId: "3999" } });
    expect(JSON.parse(JSON.stringify(firstPoll.cursor))).toEqual(firstPoll.cursor);

    const replay = successful(await source.poll(restoredCursor, signal));
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(firstPoll.events));
    const advanced = successful(await source.poll(JSON.parse(JSON.stringify(firstPoll.cursor)), signal));
    expect(advanced.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});

test("a deletion between GitHub pages is surfaced as a coverage gap rather than an empty complete observation", async () => {
  let phase: "baseline" | "poll" = "baseline";
  let deletedDuringScan = false;
  const baselineRecords = githubComments(205, 4_000);
  const currentRecords = [...baselineRecords];
  const fixture = startFixture(GITHUB_ORIGIN, (_request, url) => {
    if (url.pathname === GITHUB_PULL_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const page = Number(url.searchParams.get("page") ?? "1");
      if (phase === "poll" && page === 1 && !deletedDuringScan) {
        const firstPage = currentRecords.slice(0, PAGE_SIZE);
        currentRecords.splice(PAGE_SIZE, 1);
        deletedDuringScan = true;
        return pageResponse(url, "github", firstPage, 2);
      }
      return regularPageResponse(url, currentRecords, "github");
    }
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://acme/widget/pull/7"),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = successful(await source.initialize(signal));
    expect(baseline.events).toEqual([]);

    phase = "poll";
    const changed = await source.poll(baseline.cursor, signal);
    expect(deletedDuringScan).toBe(true);
    expect(changed.kind).toBe("gap");
    if (changed.kind !== "gap") throw new Error("a scan known to have lost a tracked item must not report complete success");
    expect(changed.gap.code).toBeTruthy();
    expect("cursor" in changed).toBe(false);
  } finally {
    fixture.stop();
  }
});

test("overlapping GitLab pages deduplicate a note observed twice without duplicating its consumer identity", async () => {
  let phase: "baseline" | "poll" = "baseline";
  const notes = gitlabNotes(205, 5_000);
  const newAt = new Date(Date.now() + 60_000).toISOString();
  const newNote = gitlabNote(5_999, newAt, "created while pages were read");
  const fixture = startFixture(GITLAB_ORIGIN, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) {
      if (phase === "baseline") return regularPageResponse(url, notes, "gitlab");
      const page = Number(url.searchParams.get("page") ?? "1");
      if (page === 1) return pageResponse(url, "gitlab", notes.slice(0, 100), 2);
      if (page === 2) return pageResponse(url, "gitlab", notes.slice(95, 195), 3);
      if (page === 3) {
        return pageResponse(url, "gitlab", [...notes.slice(190), newNote], 4);
      }
      if (page === 4) return pageResponse(url, "gitlab", [newNote], null);
      return json([], { "x-next-page": "" });
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(`gitlab://${GITLAB_HOST}/team/project/-/merge_requests/3`),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = successful(await source.initialize(signal));
    expect(baseline.events).toEqual([]);
    phase = "poll";

    const polled = successful(await source.poll(JSON.parse(JSON.stringify(baseline.cursor)), signal));
    expect(polled.events).toHaveLength(1);
    expect(polled.events[0]).toMatchObject({ type: "note", body: { objectId: "5999" } });
    expect(new Set(eventIdentities(polled.events)).size).toBe(1);

    const replay = successful(await source.poll(baseline.cursor, signal));
    expect(eventIdentities(replay.events)).toEqual(eventIdentities(polled.events));
    const advanced = successful(await source.poll(polled.cursor, signal));
    expect(advanced.events).toEqual([]);
  } finally {
    fixture.stop();
  }
});
test("a new later-timestamp note before older unseen pages reports an ordering gap", async () => {
  let phase: "baseline" | "poll" = "baseline";
  const notes = gitlabNotes(205, 6_000);
  const newAt = new Date(Date.now() + 60_000).toISOString();
  const newNote = gitlabNote(6_999, newAt, "inserted ahead of older unseen notes");
  const fixture = startFixture(GITLAB_ORIGIN, (_request, url) => {
    if (url.pathname === GITLAB_MR_PATH) return json(gitlabMergeRequest());
    if (url.pathname === GITLAB_NOTES_PATH) {
      if (phase === "baseline") return regularPageResponse(url, notes, "gitlab");
      const page = Number(url.searchParams.get("page") ?? "1");
      if (page === 1) return pageResponse(url, "gitlab", notes.slice(0, 100), 2);
      if (page === 2) {
        return pageResponse(url, "gitlab", [...notes.slice(95, 194), newNote], 3);
      }
      if (page === 3) return pageResponse(url, "gitlab", [...notes.slice(190), newNote], null);
      return json([], { "x-next-page": "" });
    }
    return json({ message: "not found" }, undefined, 404);
  });

  try {
    const source = createWatchSource({
      resource: parseWatchResource(`gitlab://${GITLAB_HOST}/team/project/-/merge_requests/3`),
      fetch: fixture.fetch,
    });
    const signal = new AbortController().signal;
    const baseline = successful(await source.initialize(signal));
    phase = "poll";

    const result = await source.poll(baseline.cursor, signal);
    expect(result).toMatchObject({ kind: "gap", gap: { code: "unordered-page" } });
    expect("cursor" in result).toBe(false);
  } finally {
    fixture.stop();
  }
});

for (const provider of PROVIDERS) {
  test(`${provider.name} rejects a pagination link on a non-source origin`, async () => {
    const fixture = startFixture(provider.origin, (_request, url) => {
      if (url.pathname === provider.snapshotPath) {
        return json(provider.provider === "github" ? githubPullRequest() : gitlabMergeRequest());
      }
      if (url.pathname === provider.listPath) {
        const hostile = new URL(`${url.pathname}${url.search}`, provider.origin);
        hostile.hostname = "attacker.example";
        hostile.searchParams.set("page", "2");
        const headers = provider.provider === "github"
          ? { link: `<${hostile.href}>; rel="next"` }
          : { link: `<${hostile.href}>; rel="next"`, "x-next-page": "2" };
        return json([], headers);
      }
      return json({ message: "not found" }, undefined, 404);
    });

    try {
      const source = createWatchSource({
        resource: parseWatchResource(provider.resourceUrl),
        fetch: fixture.fetch,
      });
      const result = await source.initialize(new AbortController().signal);
      expect(result.kind).toBe("gap");
      if (result.kind !== "gap") throw new Error("cross-origin pagination must be rejected before issuing another request");
      expect(result.gap.code).toBeTruthy();
      expect(fixture.requests.map(({ url }) => url.origin)).toEqual([provider.origin, provider.origin]);
      expect(fixture.requests.filter(({ url }) => url.pathname === provider.listPath)).toHaveLength(1);
    } finally {
      fixture.stop();
    }
  });

  test(`${provider.name} exposes a paginated HTTP failure instead of reporting a successful baseline`, async () => {
    const records = provider.provider === "github"
      ? githubComments(101, 7_000)
      : gitlabNotes(101, 8_000);
    const fixture = startFixture(provider.origin, (_request, url) => {
      if (url.pathname === provider.snapshotPath) {
        return json(provider.provider === "github" ? githubPullRequest() : gitlabMergeRequest());
      }
      if (url.pathname === provider.listPath) {
        const page = Number(url.searchParams.get("page") ?? "1");
        if (page === 2) return json({ message: "private upstream diagnostic" }, undefined, 503);
        return regularPageResponse(url, records, provider.provider);
      }
      if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
      return json({ message: "not found" }, undefined, 404);
    });

    try {
      const source = createWatchSource({
        resource: parseWatchResource(provider.resourceUrl),
        fetch: fixture.fetch,
      });
      const result = await source.initialize(new AbortController().signal);
      expect(result.kind).toBe("failure");
      if (result.kind !== "failure") throw new Error("a page failure must remain visible to source consumers");
      expect(result.failure.code).toBe("source-server-error");
      expect(result.failure.code).not.toContain("private upstream diagnostic");
      expect(pageNumbers(fixture.requests, provider.listPath)).toEqual(["1", "2"]);
    } finally {
      fixture.stop();
    }
  });
}

test("GitHub reads a next link with additional link parameters", async () => {
  const comments = githubComments(101, 10_000);
  const fixture = startFixture(GITHUB_ORIGIN, (_request, url) => {
    if (url.pathname === GITHUB_PULL_PATH) return json(githubPullRequest());
    if (url.pathname === GITHUB_REVIEWS_PATH) return json([]);
    if (url.pathname === GITHUB_COMMENTS_PATH) {
      const page = Number(url.searchParams.get("page"));
      if (page === 1) {
        const next = new URL(`${url.pathname}${url.search}`, GITHUB_ORIGIN);
        next.searchParams.set("page", "2");
        return json(comments.slice(0, 100), { link: `<${next.href}>; rel="next"; type="application/json"` });
      }
      return json(comments.slice(100));
    }
    return json({ message: "not found" }, undefined, 404);
  });
  try {
    const source = createWatchSource({
      resource: parseWatchResource("github://acme/widget/pull/7"),
      fetch: fixture.fetch,
    });
    const baseline = successful(await source.initialize(new AbortController().signal));
    expect(Object.keys(baseline.cursor.versions.comments)).toHaveLength(101);
  } finally {
    fixture.stop();
  }
});
