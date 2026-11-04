import { redactSensitiveText } from "../cards/execution-text.ts";
import type { ReferenceCandidate, ReferenceEvidence } from "./types.ts";

const MAX_CANDIDATES = 8;
const MAX_URL_CODE_POINTS = 2048;
const MAX_EXCERPT_CODE_POINTS = 1000;
const HTTP_URL_TOKEN = /https?:\/\/[^\s<>"'`]+/giu;
const EVIDENCE_URL_TOKEN = /\b[a-z][a-z0-9+.-]*:\/\/[^\s<>"'`]+/giu;
const TRAILING_PUNCTUATION = /[.,!;:，。！？；：、…）］｝〉》」』】]+$/u;
const EXCERPT_URL_MARKER = "[candidate URL omitted; nearby excerpt]";

export function extractReferenceCandidates(text: string, excludedUrls?: ReadonlySet<string>): ReferenceCandidate[] {
  const candidates: ReferenceCandidate[] = [];
  const seen = new Set<string>();
  const safeText = sanitizeReferenceText(text);

  for (const match of safeText.matchAll(HTTP_URL_TOKEN)) {
    const raw = trimUrlToken(match[0]);
    if (raw.length === 0 || codePointLength(raw) > MAX_URL_CODE_POINTS) continue;
    const parsed = parseReferenceUrl(raw);
    if (!parsed || seen.has(parsed.url)) continue;
    seen.add(parsed.url);
    if (codePointLength(parsed.url) > MAX_URL_CODE_POINTS) continue;
    if (excludedUrls?.has(parsed.url)) continue;
    if (candidates.length >= MAX_CANDIDATES) {
      const last = candidates[MAX_CANDIDATES - 1]!;
      last.excerpt = appendMarker(last.excerpt, "[additional candidate links omitted]", MAX_EXCERPT_CODE_POINTS);
      break;
    }

    const start = match.index ?? 0;
    const sourceUrlLength = codePointLength(raw);
    const startCodePoint = codePointLength(safeText.slice(0, start));
    candidates.push({
      id: `candidate-${candidates.length + 1}`,
      kind: parsed.kind,
      url: parsed.url,
      excerpt: excerptAround(safeText, startCodePoint, startCodePoint + sourceUrlLength),
    });
  }

  return candidates;
}

export function sanitizeReferenceEvidence(evidence: ReferenceEvidence): ReferenceEvidence {
  const candidates = evidence.candidates.flatMap((candidate) => {
    const url = sanitizeEvidenceUrl(candidate.url);
    if (!url) return [];
    return [{ ...candidate, url, excerpt: sanitizeReferenceText(candidate.excerpt) }];
  });
  return {
    thread: sanitizeReferenceText(evidence.thread),
    task: sanitizeReferenceText(evidence.task),
    recent: evidence.recent.map(({ role, text }) => ({ role, text: sanitizeReferenceText(text) })),
    candidates,
  };
}

function parseReferenceUrl(raw: string): { kind: "mr" | "pr"; url: string } | null {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return null;
  }
  if (
    (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
    parsed.username.length > 0 ||
    parsed.password.length > 0 ||
    /^https?:\/\/[^/?#]*@/iu.test(raw) ||
    parsed.hostname.length === 0
  ) {
    return null;
  }

  const github = /^\/([^/]+)\/([^/]+)\/pull\/(\d+)(?:\/(?:files|commits|checks))?\/?$/u.exec(parsed.pathname);
  if (github) {
    const id = positiveDecimal(github[3]!);
    if (id === null) return null;
    const owner = parsed.hostname === "github.com" ? github[1]!.toLowerCase() : github[1]!;
    const repository = parsed.hostname === "github.com" ? github[2]!.toLowerCase() : github[2]!;
    return {
      kind: "pr",
      url: `${parsed.protocol}//${parsed.host}/${owner}/${repository}/pull/${id}`,
    };
  }

  const gitlab = /^\/(.+)\/-\/merge_requests\/(\d+)(?:\/(?:diffs|commits|pipelines))?\/?$/u.exec(parsed.pathname);
  if (gitlab) {
    const projectPath = gitlab[1]!;
    const projectSegments = projectPath.split("/");
    if (projectSegments.length < 2 || projectSegments.some((segment) => segment.length === 0)) return null;
    const id = positiveDecimal(gitlab[2]!);
    if (id === null) return null;
    return {
      kind: "mr",
      url: `${parsed.protocol}//${parsed.host}/${gitlab[1]}/-/merge_requests/${id}`,
    };
  }

  return null;
}

function positiveDecimal(value: string): string | null {
  const normalized = BigInt(value);
  return normalized > 0n ? normalized.toString(10) : null;
}

function sanitizeEvidenceUrl(value: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return null;
  }
  if (
    (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
    parsed.username.length > 0 ||
    parsed.password.length > 0 ||
    /^[a-z][a-z0-9+.-]*:\/\/[^/?#]*@/iu.test(value)
  ) {
    return null;
  }
  return `${parsed.protocol}//${parsed.host}${parsed.pathname}`;
}

export function sanitizeReferenceText(value: string): string {
  return redactSensitiveText(value).replace(EVIDENCE_URL_TOKEN, (token) => {
    const raw = trimUrlToken(token);
    const suffix = token.slice(raw.length);
    return `${sanitizeEvidenceUrl(raw) ?? "[URL omitted]"}${suffix}`;
  });
}

function trimUrlToken(value: string): string {
  let result = value;
  while (result.length > 0) {
    const oldLength = result.length;
    result = result.replace(TRAILING_PUNCTUATION, "");
    if (result.endsWith(")") && countCharacter(result, ")") > countCharacter(result, "(")) result = result.slice(0, -1);
    if (result.endsWith("]") || result.endsWith("}") || result.endsWith(">")) result = result.slice(0, -1);
    if (result.length === oldLength) return result;
  }
  return result;
}

function excerptAround(text: string, start: number, end: number): string {
  const halfWindow = Math.floor((MAX_EXCERPT_CODE_POINTS - EXCERPT_URL_MARKER.length) / 2);
  const before = sliceCodePoints(text, Math.max(0, start - halfWindow), start);
  const after = sliceCodePoints(text, end, end + halfWindow);
  return `${before}${EXCERPT_URL_MARKER}${after}`;
}

function codePointLength(value: string): number {
  let length = 0;
  for (const _character of value) length += 1;
  return length;
}

function sliceCodePoints(value: string, start: number, end: number): string {
  let offset = 0;
  let index = 0;
  let startOffset = value.length;
  let endOffset = value.length;
  for (const character of value) {
    if (index === start) startOffset = offset;
    if (index === end) {
      endOffset = offset;
      break;
    }
    offset += character.length;
    index += 1;
  }
  if (start >= index && start > 0 && startOffset === value.length) return "";
  return value.slice(startOffset, endOffset);
}

function countCharacter(value: string, character: string): number {
  let count = 0;
  for (const part of value) if (part === character) count += 1;
  return count;
}

function appendMarker(value: string, marker: string, maximum: number): string {
  const remaining = maximum - codePointLength(marker);
  if (remaining <= 0) return sliceCodePoints(marker, 0, maximum);
  return `${sliceCodePoints(value, 0, remaining)}${marker}`;
}
