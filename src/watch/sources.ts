import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { delimiter, join } from "node:path";
import { runCommand } from "../workspace/worktrunk.ts";
import { homedir } from "node:os";

export type WatchResource = GitHubPullResource | GitLabMergeRequestResource;
export type WatchSourceResource = WatchResource;

export interface GitHubPullResource {
  provider: "github";
  host: "github.com";
  owner: string;
  repo: string;
  number: number;
}

export interface GitLabMergeRequestResource {
  provider: "gitlab";
  host: string;
  projectPath: string;
  iid: number;
}

export interface WatchSourceSnapshot {
  state: string;
  merged: boolean;
  title: string;
  headSha: string;
}

export interface WatchSourceEvent {
  identity: string;
  type: "snapshot" | "comment" | "review" | "note";
  version: string;
  body: Record<string, unknown>;
}

export interface WatchSourceHooks {
  beforeRequest?: () => void;
  afterResponse?: (response: Response) => void;
}

export interface WatchSourceCursor {
  schema: 1;
  resource: string;
  baselineStartedAt: number;
  transitionSequence: number;
  snapshot: WatchSourceSnapshot;
  versions: {
    comments: Record<string, string>;
    reviews: Record<string, string>;
    notes: Record<string, string>;
  };
}

export type WatchSourceObservation =
  | {
      kind: "success";
      cursor: WatchSourceCursor;
      snapshot: WatchSourceSnapshot;
      events: WatchSourceEvent[];
    }
  | { kind: "gap"; gap: { code: string } }
  | { kind: "failure"; failure: { code: string } };
export type WatchSourceResult = WatchSourceObservation;

export interface WatchSource {
  initialize(signal: AbortSignal): Promise<WatchSourceObservation>;
  poll(cursor: WatchSourceCursor, signal: AbortSignal): Promise<WatchSourceObservation>;
}

export type WatchSourceEventType = WatchSourceEvent["type"];

const MAX_PAGE_SIZE = 100;
const MAX_TRACKED_OBJECTS = 10_000;
const MAX_SOURCE_PAGES = MAX_TRACKED_OBJECTS / MAX_PAGE_SIZE;
const MAX_API_RESPONSE_BYTES = 4 * 1024 * 1024;
const MAX_EVENT_TEXT_BYTES = 8 * 1024;
const MAX_AUTHOR_BYTES = 256;
const API_HEADERS = { accept: "application/vnd.github+json" };
const SOURCE_REQUEST_TIMEOUT_MS = 15_000;

class SourceCredentialError extends Error {
  constructor() {
    super("source credentials are unavailable");
  }
}

class SourceOriginError extends Error {
  constructor() {
    super("source request origin or method is not permitted");
  }
}

function executableSearchPaths(): string[] {
  const paths = (process.env.PATH ?? "").split(delimiter).filter((path) => path.startsWith("/"));
  return [...new Set([...paths, "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"])];
}

async function resolveExecutable(name: "gh" | "glab"): Promise<string | null> {
  for (const directory of executableSearchPaths()) {
    const candidate = join(directory, name);
    try {
      await access(candidate, constants.X_OK);
      return candidate;
    } catch {
      // Continue through the controlled absolute search path.
    }
  }
  return null;
}

function credentialCommandEnvironment(): Record<string, string> {
  const env: Record<string, string> = {
    HOME: process.env.HOME || process.env.USERPROFILE || homedir(),
    PATH: executableSearchPaths().join(delimiter),
  };
  for (const name of [
    "XDG_CONFIG_HOME",
    "GH_CONFIG_DIR",
    "GLAB_CONFIG_DIR",
    "GH_TOKEN",
    "GITHUB_TOKEN",
    "GITLAB_TOKEN",
    "GITLAB_ACCESS_TOKEN",
  ]) {
    const value = process.env[name];
    if (value !== undefined) env[name] = value;
  }
  return env;
}

async function loadSourceToken(resource: WatchSourceResource, signal: AbortSignal): Promise<string> {
  const github = resource.provider === "github";
  const executable = await resolveExecutable(github ? "gh" : "glab");
  if (!executable) throw new SourceCredentialError();
  const args = github
    ? ["auth", "token", "--hostname", "github.com"]
    : ["config", "get", "token", "--host", resource.host];
  const result = await runCommand([executable, ...args], {
    timeoutMs: 5_000,
    signal,
    env: credentialCommandEnvironment(),
    operation: "watch source credential lookup",
    maxOutputBytes: { stdout: 4_096, stderr: 1_024 },
  });
  if (!result || result.exitCode !== 0 || result.timedOut || result.aborted || result.outputExceeded) {
    throw new SourceCredentialError();
  }
  const token = result.stdout.trim();
  if (!token || /\s/.test(token)) throw new SourceCredentialError();
  return token;
}

class SourceHookError extends Error {
  constructor(readonly original: unknown) {
    super("source request hook failed");
  }
}

type SourceRequestInterruption = "cancelled" | "timeout";

interface SourceRequestDeadline {
  signal: AbortSignal;
  interruption: Promise<SourceRequestInterruption>;
  reason: SourceRequestInterruption | null;
  release(): void;
}

function sourceRequestDeadline(caller: AbortSignal): SourceRequestDeadline {
  const controller = new AbortController();
  const { promise: interruption, resolve } = Promise.withResolvers<SourceRequestInterruption>();
  let reason: SourceRequestInterruption | null = null;
  const interrupt = (next: SourceRequestInterruption): void => {
    if (reason !== null) return;
    reason = next;
    controller.abort(next === "cancelled" ? caller.reason : undefined);
    resolve(next);
  };
  const abortFromCaller = (): void => interrupt("cancelled");
  caller.addEventListener("abort", abortFromCaller, { once: true });
  if (caller.aborted) abortFromCaller();
  const timer = setTimeout(() => interrupt("timeout"), SOURCE_REQUEST_TIMEOUT_MS);

  return {
    signal: controller.signal,
    interruption,
    get reason() {
      return reason;
    },
    release() {
      clearTimeout(timer);
      caller.removeEventListener("abort", abortFromCaller);
    },
  };
}

function sourceRequestFailure(reason: SourceRequestInterruption) {
  return {
    kind: "failure" as const,
    code: reason === "cancelled" ? "cancelled" : "source-request-timeout",
  };
}

function sourceFetch(
  resource: WatchSourceResource,
  injected: typeof globalThis.fetch | undefined,
  hooks: WatchSourceHooks,
): typeof globalThis.fetch {
  const baseFetch = injected ?? globalThis.fetch;
  const expectedOrigin = resource.provider === "github" ? "https://api.github.com" : `https://${resource.host}`;
  let token: string | null = null;
  let pending: Promise<string> | null = null;
  const fetch: typeof globalThis.fetch = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = input instanceof Request ? input : null;
      let url: URL;
      try {
        if (input instanceof Request) {
          url = new URL(input.url);
        } else {
          url = new URL(input);
        }
      } catch {
        throw new SourceOriginError();
      }
      const method = (init?.method ?? request?.method ?? "GET").toUpperCase();
      if (url.protocol !== "https:" || url.origin !== expectedOrigin ||
          url.username || url.password || method !== "GET") {
        throw new SourceOriginError();
      }

      const headers = new Headers(init?.headers);
      if (!injected) {
        const signal = init?.signal ?? undefined;
        if (!token) {
          if (!pending) {
            if (!signal) throw new SourceCredentialError();
            pending = loadSourceToken(resource, signal);
          }
          try {
            token = await pending;
          } catch {
            pending = null;
            throw new SourceCredentialError();
          }
        }
        if (resource.provider === "gitlab") {
          headers.set("PRIVATE-TOKEN", token);
        } else {
          headers.set("authorization", `Bearer ${token}`);
          headers.set("X-GitHub-Api-Version", "2022-11-28");
        }
      }
      try {
        hooks.beforeRequest?.();
      } catch (error) {
        throw new SourceHookError(error);
      }
      const response = await baseFetch(input, { ...init, method: "GET", headers, redirect: "error" });
      try {
        hooks.afterResponse?.(response);
      } catch (error) {
        throw new SourceHookError(error);
      }
      return response;
    },
    { preconnect: globalThis.fetch.preconnect.bind(globalThis.fetch) },
  );
  return fetch;
}

interface SourceApi {
  resource: WatchSourceResource;
  fetch: typeof globalThis.fetch;
  pullRequestUrl: URL;
  commentsUrl: URL;
  reviewsOrNotesUrl: URL;
  headers: HeadersInit;
}

interface ParsedObject {
  id: string;
  version: string;
  createdAt: number;
  versionAt: number;
  event: WatchSourceEvent;
}

type SourceError =
  | { kind: "gap"; code: string }
  | { kind: "failure"; code: string };

type ScanResult = { kind: "success"; records: ParsedObject[] } | SourceError;
type RequestResult = { kind: "success"; response: Response; value: unknown } | SourceError;
type PageResult = { kind: "success"; records: ParsedObject[]; pages: number } | SourceError;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function positiveInteger(value: string | undefined): number | null {
  if (!value || !/^[1-9]\d*$/.test(value)) return null;
  const number = Number(value);
  return Number.isSafeInteger(number) ? number : null;
}

function validPathSegment(segment: string): boolean {
  return segment.length > 0 && segment !== "." && segment !== ".." &&
    !/[\\/\0]/.test(segment) && !segment.includes("%") && /^[A-Za-z0-9_.-]+$/.test(segment);
}

function decodePath(path: string): string[] | null {
  const parts = path.split("/").filter(Boolean);
  if (parts.some((part) => !validPathSegment(part))) return null;
  return parts;
}

function hasNoUrlExtras(url: URL): boolean {
  return !url.username && !url.password && !url.search && !url.hash;
}

function parseGitHubPath(path: string): GitHubPullResource | null {
  const match = /^\/([^/]+)\/([^/]+)\/pull\/(\d+)\/?$/.exec(path);
  if (!match) return null;
  const [owner, repo] = match.slice(1, 3);
  const number = positiveInteger(match[3]);
  if (!owner || !repo || !validPathSegment(owner) || !validPathSegment(repo) || number === null) return null;
  return { provider: "github", host: "github.com", owner, repo, number };
}

function parseGitLabPath(host: string, path: string): GitLabMergeRequestResource | null {
  const match = /^\/(.+)\/\-\/merge_requests\/(\d+)\/?$/.exec(path);
  if (!match) return null;
  const projectSegments = decodePath(match[1]!);
  const iid = positiveInteger(match[2]);
  if (!projectSegments || projectSegments.length < 2 || iid === null) return null;
  return { provider: "gitlab", host, projectPath: projectSegments.join("/"), iid };
}

/** Parse only the two explicitly supported public resource URL shapes. */
export function parseWatchResource(input: string): WatchSourceResource {
  if (typeof input !== "string" || !input || input !== input.trim()) {
    throw new Error("watch resource is invalid");
  }

  if (input.startsWith("github://")) {
    const url = new URL(input);
    if (url.protocol !== "github:" || !hasNoUrlExtras(url) || url.port) {
      throw new Error("watch resource is invalid");
    }
    const parsed = parseGitHubPath(`/${url.hostname}${url.pathname}`);
    if (!parsed) throw new Error("watch resource is invalid");
    return parsed;
  }

  if (input.startsWith("gitlab://")) {
    const url = new URL(input);
    if (url.protocol !== "gitlab:" || !hasNoUrlExtras(url) || !url.host) {
      throw new Error("watch resource is invalid");
    }
    const host = url.host.toLowerCase();
    const parsed = parseGitLabPath(host, url.pathname);
    if (!parsed) throw new Error("watch resource is invalid");
    return parsed;
  }

  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new Error("watch resource is invalid");
  }
  if (url.protocol !== "https:" || !hasNoUrlExtras(url)) {
    throw new Error("watch resource is invalid");
  }

  if (url.hostname.toLowerCase() === "github.com" && !url.port) {
    const parsed = parseGitHubPath(url.pathname);
    if (parsed) return parsed;
  }

  if (url.host) {
    const parsed = parseGitLabPath(url.host.toLowerCase(), url.pathname);
    if (parsed) return parsed;
  }
  throw new Error("watch resource is invalid");
}

function resourceKey(resource: WatchSourceResource): string {
  if (resource.provider === "github") {
    return JSON.stringify(["github", resource.host, resource.owner, resource.repo, resource.number]);
  }
  return JSON.stringify(["gitlab", resource.host, resource.projectPath, resource.iid]);
}

function apiFor(resource: WatchSourceResource, fetch: typeof globalThis.fetch): SourceApi {
  if (resource.provider === "github") {
    const base = `https://api.github.com/repos/${resource.owner}/${resource.repo}`;
    return {
      resource,
      fetch,
      pullRequestUrl: new URL(`${base}/pulls/${resource.number}`),
      commentsUrl: new URL(`${base}/issues/${resource.number}/comments`),
      reviewsOrNotesUrl: new URL(`${base}/pulls/${resource.number}/reviews`),
      headers: API_HEADERS,
    };
  }

  const origin = `https://${resource.host}`;
  const project = encodeURIComponent(resource.projectPath);
  const requestBase = `${origin}/api/v4/projects/${project}/merge_requests/${resource.iid}`;
  return {
    resource,
    fetch,
    pullRequestUrl: new URL(requestBase),
    commentsUrl: new URL(`${requestBase}/notes`),
    reviewsOrNotesUrl: new URL(`${requestBase}/notes`),
    headers: { accept: "application/json" },
  };
}

function codeForStatus(status: number): string {
  if (status === 401 || status === 403) return "authorization-denied";
  if (status === 404) return "source-unavailable";
  if (status === 429) return "rate-limited";
  if (status >= 500) return "source-server-error";
  return "source-request-failed";
}

async function readBoundedJson(
  response: Response,
  signal: AbortSignal,
): Promise<{ ok: true; value: unknown } | { ok: false }> {
  if (!response.body) return { ok: false };
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const cancelOnAbort = (): void => {
    void reader.cancel().catch(() => undefined);
  };
  signal.addEventListener("abort", cancelOnAbort, { once: true });
  if (signal.aborted) cancelOnAbort();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_API_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        return { ok: false };
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    try {
      return { ok: true, value: JSON.parse(new TextDecoder().decode(bytes)) as unknown };
    } catch {
      return { ok: false };
    }
  } catch {
    return { ok: false };
  } finally {
    signal.removeEventListener("abort", cancelOnAbort);
    reader.releaseLock();
  }
}

async function requestJson(api: SourceApi, url: URL, signal: AbortSignal): Promise<RequestResult> {
  if (signal.aborted) return { kind: "failure", code: "cancelled" };
  const deadline = sourceRequestDeadline(signal);
  try {
    const fetched = await Promise.race([
      api.fetch(url, {
        method: "GET",
        headers: api.headers,
        signal: deadline.signal,
        redirect: "error",
      }).then(
        (response) => ({ kind: "response" as const, response }),
        (error: unknown) => ({ kind: "error" as const, error }),
      ),
      deadline.interruption.then((reason) => ({ kind: "interrupted" as const, reason })),
    ]);
    if (fetched.kind === "interrupted") return sourceRequestFailure(fetched.reason);
    const interruption = deadline.reason;
    if (interruption !== null) {
      if (fetched.kind === "response") {
        void fetched.response.body?.cancel().catch(() => undefined);
      }
      return sourceRequestFailure(interruption);
    }
    if (fetched.kind === "error") {
      const { error } = fetched;
      if (error instanceof SourceHookError) throw error.original;
      if (error instanceof SourceOriginError) return { kind: "failure", code: "source-origin-rejected" };
      if (error instanceof SourceCredentialError) return { kind: "failure", code: "credentials-unavailable" };
      return { kind: "failure", code: "source-request-failed" };
    }

    const { response } = fetched;
    if (!response.ok) {
      void response.body?.cancel().catch(() => undefined);
      return { kind: "failure", code: codeForStatus(response.status) };
    }
    const bodyResult = await Promise.race([
      readBoundedJson(response, deadline.signal).then((body) => ({ kind: "body" as const, body })),
      deadline.interruption.then((reason) => ({ kind: "interrupted" as const, reason })),
    ]);
    if (bodyResult.kind === "interrupted") return sourceRequestFailure(bodyResult.reason);
    const bodyInterruption = deadline.reason;
    if (bodyInterruption !== null) return sourceRequestFailure(bodyInterruption);
    if (!bodyResult.body.ok) return { kind: "gap", code: "invalid-source-response" };
    return { kind: "success", response, value: bodyResult.body.value };
  } finally {
    deadline.release();
  }
}

function timestamp(value: unknown): { text: string; time: number } | null {
  if (typeof value !== "string" || !value) return null;
  const time = Date.parse(value);
  return Number.isFinite(time) ? { text: value, time } : null;
}

function bounded(value: string, maximum: number): string {
  const bytes = new TextEncoder().encode(value);
  if (bytes.byteLength <= maximum) return value;
  let end = maximum;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end -= 1;
  return new TextDecoder().decode(bytes.subarray(0, end));
}

function positiveId(value: unknown): string | null {
  if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) return String(value);
  if (typeof value === "string" && /^[1-9]\d*$/.test(value) && Number.isSafeInteger(Number(value))) return value;
  return null;
}

function eventIdentity(resource: WatchSourceResource, type: WatchSourceEvent["type"], objectId: string, version: string): string {
  return JSON.stringify([resourceKey(resource), type, objectId, version]);
}

function parseDiscussionEvent(
  resource: WatchSourceResource,
  kind: "comment" | "review" | "note",
  value: unknown,
): ParsedObject | null {
  if (!record(value)) return null;
  const id = positiveId(value.id);
  const versionValue = timestamp(kind === "review" ? value.submitted_at : value.updated_at);
  const created = timestamp(kind === "review" ? value.submitted_at : value.created_at);
  if (!id || !versionValue || !created || typeof value.body !== "string") return null;

  const reviewState = kind === "review" && typeof value.state === "string"
    ? bounded(value.state, 64)
    : null;
  if (kind === "review" && reviewState === null) return null;

  const authorValue = kind === "note" ? value.author : value.user;
  const author = record(authorValue)
    ? (typeof authorValue.username === "string" ? authorValue.username :
      typeof authorValue.login === "string" ? authorValue.login : "")
    : "";
  const boundedAuthor = bounded(author, MAX_AUTHOR_BYTES);
  const bodyText = bounded(value.body, MAX_EVENT_TEXT_BYTES);
  const version = reviewState === null
    ? versionValue.text
    : `${versionValue.text}#sha256:${createHash("sha256")
        .update(JSON.stringify([reviewState, bodyText, boundedAuthor]))
        .digest("hex")}`;
  const event: WatchSourceEvent = {
    identity: eventIdentity(resource, kind, id, version),
    type: kind,
    version,
    body: {
      objectId: id,
      text: bodyText,
      author: boundedAuthor,
      createdAt: created.text,
      updatedAt: versionValue.text,
      ...(reviewState === null ? {} : { state: reviewState }),
      ...(kind === "note" && typeof value.system === "boolean" ? { system: value.system } : {}),
    },
  };
  return { id, version, createdAt: created.time, versionAt: versionValue.time, event };
}

function parseSnapshot(resource: WatchSourceResource, value: unknown): WatchSourceSnapshot | null {
  if (!record(value) || typeof value.state !== "string" || !value.state || typeof value.title !== "string") return null;
  const state = bounded(value.state, 64);
  if (resource.provider === "github") {
    const head = record(value.head) ? value.head : null;
    if (typeof value.merged !== "boolean" || !head || typeof head.sha !== "string" || !head.sha) return null;
    return {
      state,
      merged: value.merged,
      title: bounded(value.title, MAX_EVENT_TEXT_BYTES),
      headSha: bounded(head.sha, 256),
    };
  }

  const diffRefs = record(value.diff_refs) ? value.diff_refs : null;
  const sha = diffRefs && typeof diffRefs.head_sha === "string"
    ? diffRefs.head_sha
    : typeof value.sha === "string" ? value.sha : null;
  if (!sha || !(value.merged_at === null || typeof value.merged_at === "string")) return null;
  return {
    state,
    merged: state === "merged" || value.merged_at !== null,
    title: bounded(value.title, MAX_EVENT_TEXT_BYTES),
    headSha: bounded(sha, 256),
  };
}

function sameSnapshot(left: WatchSourceSnapshot, right: WatchSourceSnapshot): boolean {
  return left.state === right.state && left.merged === right.merged &&
    left.title === right.title && left.headSha === right.headSha;
}

function parseNextLink(header: string | null): string | null {
  if (!header) return null;
  for (const part of header.split(/,\s*(?=<)/)) {
    const match = /^\s*<([^>]+)>(.*)$/.exec(part);
    const rel = match?.[2]?.match(/(?:^|;)\s*rel\s*=\s*(?:"([^"]*)"|([^;\s]+))/i);
    if (match?.[1] && (rel?.[1] ?? rel?.[2])?.split(/\s+/).includes("next")) return match[1];
  }
  return null;
}

function nextPageFromLink(
  header: string | null,
  currentUrl: URL,
  expectedPath: string,
  currentPage: number,
): number | null | "invalid" {
  const link = parseNextLink(header);
  if (link === null) return null;
  let next: URL;
  try {
    next = new URL(link, currentUrl);
  } catch {
    return "invalid";
  }
  const page = positiveInteger(next.searchParams.get("page") ?? undefined);
  const currentParameters = [...currentUrl.searchParams.entries()]
    .filter(([name]) => name !== "page")
    .sort(([leftName, leftValue], [rightName, rightValue]) =>
      leftName.localeCompare(rightName) || leftValue.localeCompare(rightValue));
  const nextParameters = [...next.searchParams.entries()]
    .filter(([name]) => name !== "page")
    .sort(([leftName, leftValue], [rightName, rightValue]) =>
      leftName.localeCompare(rightName) || leftValue.localeCompare(rightValue));
  if (next.origin !== currentUrl.origin || next.username || next.password || next.hash ||
      next.pathname !== expectedPath || next.searchParams.getAll("page").length !== 1 ||
      page !== currentPage + 1 || JSON.stringify(currentParameters) !== JSON.stringify(nextParameters)) {
    return "invalid";
  }
  return page;
}

function makeListUrl(api: SourceApi, url: URL, page: number): URL {
  const next = new URL(url.href);
  next.searchParams.set("per_page", "100");
  next.searchParams.set("page", String(page));
  if (api.resource.provider === "github" && url.pathname === api.commentsUrl.pathname) {
    next.searchParams.set("sort", "created");
    next.searchParams.set("direction", "asc");
  } else if (api.resource.provider === "gitlab") {
    next.searchParams.set("order_by", "created_at");
    next.searchParams.set("sort", "asc");
  }
  return next;
}

async function fetchPagesOnce(
  api: SourceApi,
  endpoint: URL,
  kind: "comment" | "review" | "note",
  signal: AbortSignal,
  maxRecords = MAX_TRACKED_OBJECTS,
): Promise<PageResult> {
  const records: ParsedObject[] = [];
  const seen = new Map<string, { parsed: ParsedObject; page: number; position: number }>();
  let highestCreatedAt = -Infinity;
  let page = 1;

  for (;;) {
    const currentUrl = makeListUrl(api, endpoint, page);
    const fetched = await requestJson(api, currentUrl, signal);
    if (fetched.kind !== "success") return fetched;
    if (!Array.isArray(fetched.value)) return { kind: "gap", code: "invalid-page" };
    if (fetched.value.length > MAX_PAGE_SIZE) return { kind: "gap", code: "pagination-limit" };

    const inPage = new Map<string, ParsedObject>();
    let previousCreatedAt = -Infinity;
    let previousOverlapPosition = -1;
    for (let position = 0; position < fetched.value.length; position += 1) {
      const value = fetched.value[position];
      if (kind === "review" && record(value) && value.state === "PENDING" &&
          (value.submitted_at === null || value.submitted_at === undefined)) {
        continue;
      }
      const parsed = parseDiscussionEvent(api.resource, kind, value);
      if (!parsed) return { kind: "gap", code: "invalid-event-record" };
      if (parsed.createdAt < previousCreatedAt) return { kind: "gap", code: "unordered-page" };
      previousCreatedAt = parsed.createdAt;

      const samePageRecord = inPage.get(parsed.id);
      if (samePageRecord) {
        if (samePageRecord.version !== parsed.version ||
            JSON.stringify(samePageRecord.event) !== JSON.stringify(parsed.event)) {
          return { kind: "gap", code: "inconsistent-page" };
        }
        continue;
      }
      inPage.set(parsed.id, parsed);

      const previous = seen.get(parsed.id);
      if (previous) {
        if (previous.parsed.version !== parsed.version ||
            JSON.stringify(previous.parsed.event) !== JSON.stringify(parsed.event)) {
          return { kind: "gap", code: "inconsistent-page" };
        }
        if (previous.page !== page - 1 || previous.position <= previousOverlapPosition) {
          return { kind: "gap", code: "unordered-page" };
        }
        previousOverlapPosition = previous.position;
        seen.set(parsed.id, { parsed: previous.parsed, page, position });
        continue;
      }
      if (parsed.createdAt < highestCreatedAt) return { kind: "gap", code: "unordered-page" };
      if (records.length >= maxRecords) return { kind: "gap", code: "pagination-limit" };
      highestCreatedAt = parsed.createdAt;
      records.push(parsed);
      seen.set(parsed.id, { parsed, page, position });
    }

    const linkNext = nextPageFromLink(
      fetched.response.headers.get("link"),
      currentUrl,
      endpoint.pathname,
      page,
    );
    if (linkNext === "invalid") return { kind: "gap", code: "invalid-pagination" };
    let nextPage: number | null = linkNext;
    if (api.resource.provider === "gitlab") {
      const rawNext = fetched.response.headers.get("x-next-page");
      if (rawNext !== null) {
        if (rawNext === "") {
          if (linkNext !== null) return { kind: "gap", code: "inconsistent-pagination" };
          nextPage = null;
        } else {
          const headerPage = positiveInteger(rawNext);
          if (headerPage === null || headerPage !== page + 1 ||
              (linkNext !== null && linkNext !== headerPage)) {
            return { kind: "gap", code: "invalid-pagination" };
          }
          nextPage = headerPage;
        }
      }
    }
    if (nextPage === null) return { kind: "success", records, pages: page };
    if (page >= MAX_SOURCE_PAGES) return { kind: "gap", code: "pagination-limit" };
    page = nextPage;
  }
}

function samePageScan(
  left: ParsedObject[],
  right: ParsedObject[],
): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const first = left[index]!;
    const second = right[index]!;
    const firstBody = first.event.body;
    const secondBody = second.event.body;
    if (first.id !== second.id || first.version !== second.version ||
        first.createdAt !== second.createdAt || first.versionAt !== second.versionAt ||
        first.event.identity !== second.event.identity || first.event.type !== second.event.type ||
        first.event.version !== second.event.version ||
        firstBody.objectId !== secondBody.objectId || firstBody.text !== secondBody.text ||
        firstBody.author !== secondBody.author || firstBody.createdAt !== secondBody.createdAt ||
        firstBody.updatedAt !== secondBody.updatedAt || firstBody.state !== secondBody.state ||
        firstBody.system !== secondBody.system) {
      return false;
    }
  }
  return true;
}

async function fetchPages(
  api: SourceApi,
  endpoint: URL,
  kind: "comment" | "review" | "note",
  signal: AbortSignal,
  maxRecords = MAX_TRACKED_OBJECTS,
): Promise<PageResult> {
  // Providers do not offer a snapshot token. Re-read multi-page lists within the
  // same bounded request budget and reject observable drift between full scans.
  const first = await fetchPagesOnce(api, endpoint, kind, signal, maxRecords);
  if (first.kind !== "success" || first.pages === 1) return first;
  const second = await fetchPagesOnce(api, endpoint, kind, signal, maxRecords);
  if (second.kind !== "success") return second;
  if (first.pages !== second.pages || !samePageScan(first.records, second.records)) {
    return { kind: "gap", code: "inconsistent-scan" };
  }
  return first;
}

interface ParsedCursor {
  cursor: WatchSourceCursor;
  hasLegacyReviewVersions: boolean;
}

function reviewVersionTimestamp(value: string): number | null {
  const match = /^(.+)#sha256:([a-f0-9]{64})$/.exec(value);
  if (!match?.[1]) return null;
  return timestamp(match[1])?.time ?? null;
}

function parseCursor(value: unknown, expectedResource: string): ParsedCursor | null {
  if (!record(value) || value.schema !== 1 || value.resource !== expectedResource ||
      !Number.isSafeInteger(value.baselineStartedAt) || !Number.isSafeInteger(value.transitionSequence) ||
      (value.transitionSequence as number) < 0 || !record(value.snapshot) || !record(value.versions)) return null;
  const snapshot = value.snapshot;
  if (typeof snapshot.state !== "string" || typeof snapshot.merged !== "boolean" ||
      typeof snapshot.title !== "string" || typeof snapshot.headSha !== "string") return null;
  const versions: WatchSourceCursor["versions"] = { comments: {}, reviews: {}, notes: {} };
  let hasLegacyReviewVersions = false;
  for (const kind of ["comments", "reviews", "notes"] as const) {
    const source = value.versions[kind];
    if (!record(source) || Object.keys(source).length > MAX_TRACKED_OBJECTS) return null;
    for (const [id, version] of Object.entries(source)) {
      if (!positiveId(id) || typeof version !== "string") return null;
      let validVersion = timestamp(version) !== null;
      if (kind === "reviews") {
        if (reviewVersionTimestamp(version) !== null) {
          validVersion = true;
        } else if (validVersion) {
          hasLegacyReviewVersions = true;
        }
      }
      if (!validVersion) return null;
      versions[kind][id] = version;
    }
  }
  return {
    cursor: {
      schema: 1,
      resource: expectedResource,
      baselineStartedAt: value.baselineStartedAt as number,
      transitionSequence: value.transitionSequence as number,
      snapshot: {
        state: snapshot.state,
        merged: snapshot.merged,
        title: snapshot.title,
        headSha: snapshot.headSha,
      },
      versions,
    },
    hasLegacyReviewVersions,
  };
}

function versionsOf(records: ParsedObject[]): Record<string, string> {
  const versions: Record<string, string> = {};
  for (const item of records) versions[item.id] = item.version;
  return versions;
}

function pageRecords(api: SourceApi, signal: AbortSignal): Promise<ScanResult> {
  return (async () => {
    const commentsOrNotes = await fetchPages(
      api,
      api.commentsUrl,
      api.resource.provider === "github" ? "comment" : "note",
      signal,
    );
    if (commentsOrNotes.kind !== "success") return commentsOrNotes;
    if (api.resource.provider === "gitlab") {
      return { kind: "success", records: commentsOrNotes.records };
    }
    const reviews = await fetchPages(
      api,
      api.reviewsOrNotesUrl,
      "review",
      signal,
      MAX_TRACKED_OBJECTS - commentsOrNotes.records.length,
    );
    if (reviews.kind !== "success") return reviews;
    return { kind: "success", records: [...commentsOrNotes.records, ...reviews.records] };
  })();
}

async function fetchSnapshot(api: SourceApi, signal: AbortSignal): Promise<
  { kind: "success"; snapshot: WatchSourceSnapshot } | { kind: "gap"; code: string } | { kind: "failure"; code: string }
> {
  const result = await requestJson(api, api.pullRequestUrl, signal);
  if (result.kind !== "success") return result;
  const snapshot = parseSnapshot(api.resource, result.value);
  return snapshot ? { kind: "success", snapshot } : { kind: "gap", code: "invalid-resource-snapshot" };
}

function snapshotEvent(
  resource: WatchSourceResource,
  sequence: number,
  previous: WatchSourceSnapshot,
  current: WatchSourceSnapshot,
): WatchSourceEvent {
  const version = String(sequence);
  return {
    identity: eventIdentity(resource, "snapshot", String(sequence), JSON.stringify([previous, current])),
    type: "snapshot",
    version,
    body: { previous, current },
  };
}


function sharesGitHubBaselineSecond(item: ParsedObject, baselineStartedAt: number): boolean {
  const baselineSecond = Math.floor(baselineStartedAt / 1_000);
  return Math.floor(item.createdAt / 1_000) === baselineSecond ||
    Math.floor(item.versionAt / 1_000) === baselineSecond;
}

function publicRecords(
  records: ParsedObject[],
  previous: Record<string, string>,
  baselineStartedAt: number,
): { events: WatchSourceEvent[]; error: string | null } {
  const events: WatchSourceEvent[] = [];
  const previousIds = new Set(Object.keys(previous));
  for (const item of records) {
    previousIds.delete(item.id);
    const lastVersion = previous[item.id];
    if (lastVersion === item.version) continue;
    if (lastVersion === undefined && Math.max(item.createdAt, item.versionAt) < baselineStartedAt) {
      return { events: [], error: "late-historical-object" };
    }
    events.push(item.event);
  }
  if (previousIds.size > 0) return { events: [], error: "source-object-missing" };
  return { events, error: null };
}

function mapScanError(result: SourceError): WatchSourceObservation {
  return result.kind === "gap"
    ? { kind: "gap", gap: { code: result.code } }
    : { kind: "failure", failure: { code: result.code } };
}

/** Build the first-party, read-only GitHub PR and GitLab MR REST adapters. */
export function createWatchSource(options: {
  resource: WatchSourceResource;
  fetch?: typeof globalThis.fetch;
} & WatchSourceHooks): WatchSource {
  const fetch = sourceFetch(options.resource, options.fetch, options);
  const api = apiFor(options.resource, fetch);
  const key = resourceKey(options.resource);

  return {
    async initialize(signal) {
      const baselineStartedAt = Date.now();
      const initialSnapshot = await fetchSnapshot(api, signal);
      if (initialSnapshot.kind !== "success") return mapScanError(initialSnapshot);
      const scan = await pageRecords(api, signal);
      if (scan.kind !== "success") return mapScanError(scan);
      if (options.resource.provider === "github" &&
          scan.records.some((item) => sharesGitHubBaselineSecond(item, baselineStartedAt))) {
        return { kind: "gap", gap: { code: "baseline-timestamp-precision" } };
      }
      const finalSnapshot = await fetchSnapshot(api, signal);
      if (finalSnapshot.kind !== "success") return mapScanError(finalSnapshot);
      if (!sameSnapshot(initialSnapshot.snapshot, finalSnapshot.snapshot)) {
        return { kind: "gap", gap: { code: "resource-changed-during-baseline" } };
      }
      if (scan.records.length > MAX_TRACKED_OBJECTS) {
        return { kind: "gap", gap: { code: "pagination-limit" } };
      }
      const candidates = scan.records
        .filter((item) => Math.max(item.createdAt, item.versionAt) >= baselineStartedAt)
        .map((item) => item.event);
      const cursor: WatchSourceCursor = {
        schema: 1,
        resource: key,
        baselineStartedAt,
        transitionSequence: 0,
        snapshot: initialSnapshot.snapshot,
        versions: {
          comments: options.resource.provider === "github" ? versionsOf(scan.records.filter((item) => item.event.type === "comment")) : {},
          reviews: options.resource.provider === "github" ? versionsOf(scan.records.filter((item) => item.event.type === "review")) : {},
          notes: options.resource.provider === "gitlab" ? versionsOf(scan.records) : {},
        },
      };
      return { kind: "success", cursor, snapshot: initialSnapshot.snapshot, events: candidates };
    },

    async poll(rawCursor, signal) {
      const parsedCursor = parseCursor(rawCursor, key);
      if (!parsedCursor) return { kind: "failure", failure: { code: "invalid-cursor" } };
      if (parsedCursor.hasLegacyReviewVersions) {
        return { kind: "gap", gap: { code: "review-version-upgrade-required" } };
      }
      const cursor = parsedCursor.cursor;
      const currentSnapshot = await fetchSnapshot(api, signal);
      if (currentSnapshot.kind !== "success") return mapScanError(currentSnapshot);
      const scan = await pageRecords(api, signal);
      if (scan.kind !== "success") return mapScanError(scan);
      const finalSnapshot = await fetchSnapshot(api, signal);
      if (finalSnapshot.kind !== "success") return mapScanError(finalSnapshot);
      if (!sameSnapshot(currentSnapshot.snapshot, finalSnapshot.snapshot)) {
        return { kind: "gap", gap: { code: "resource-changed-during-poll" } };
      }

      const recordEvents: WatchSourceEvent[] = [];
      const recordGroups: Array<{ previous: Record<string, string>; records: ParsedObject[] }> =
        options.resource.provider === "github"
          ? [
              { previous: cursor.versions.comments, records: scan.records.filter((item) => item.event.type === "comment") },
              { previous: cursor.versions.reviews, records: scan.records.filter((item) => item.event.type === "review") },
            ]
          : [{ previous: cursor.versions.notes, records: scan.records }];
      for (const group of recordGroups) {
        const result = publicRecords(group.records, group.previous, cursor.baselineStartedAt);
        if (result.error) return { kind: "gap", gap: { code: result.error } };
        recordEvents.push(...result.events);
      }
      const snapshotChanged = !sameSnapshot(cursor.snapshot, currentSnapshot.snapshot);
      const transitionSequence = cursor.transitionSequence + (snapshotChanged ? 1 : 0);
      const events = [...recordEvents];
      if (snapshotChanged) {
        events.unshift(snapshotEvent(options.resource, transitionSequence, cursor.snapshot, currentSnapshot.snapshot));
      }
      const nextCursor: WatchSourceCursor = {
        ...cursor,
        transitionSequence,
        snapshot: currentSnapshot.snapshot,
        versions: {
          comments: options.resource.provider === "github"
            ? versionsOf(scan.records.filter((item) => item.event.type === "comment"))
            : {},
          reviews: options.resource.provider === "github"
            ? versionsOf(scan.records.filter((item) => item.event.type === "review"))
            : {},
          notes: options.resource.provider === "gitlab" ? versionsOf(scan.records) : {},
        },
      };
      return { kind: "success", cursor: nextCursor, snapshot: currentSnapshot.snapshot, events };
    },
  };
}
