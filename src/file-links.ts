import { resolve as resolvePath } from "node:path";
import type { FileLinkResolverConfig } from "./contracts.ts";

const MAX_TEXT_BYTES = 3_000;
const URI_SCHEME = /^[a-z][a-z\d+.-]*:/iu;

export interface FileLinkResolver {
  isMatch(path: string): boolean;
  resolve(path: string, cwd: string): string | Promise<string>;
}

export interface FileLinkResolution {
  text: string;
  /** Destinations of rewritten links; only these complete Markdown units are kept atomic on split. */
  protectedHrefs: string[];
}

interface MarkdownLink {
  start: number;
  end: number;
  destinationStart: number;
  destinationEnd: number;
  destination: string;
}

interface MarkdownScan {
  links: MarkdownLink[];
  incompleteStart: number | null;
}

interface Replacement {
  start: number;
  end: number;
  href: string;
}

export class ZedLocalFileLinkResolver implements FileLinkResolver {
  isMatch(path: string): boolean {
    return isLocalFilePath(path);
  }

  resolve(path: string, cwd: string): string {
    const { filePath, suffix } = splitFilePathSuffix(path);
    const absolutePath = resolvePath(cwd, decodeFilePath(filePath));
    return `zed://file${encodeAbsolutePath(absolutePath)}${suffix}`;
  }
}

export class ZedSshFileLinkResolver implements FileLinkResolver {
  constructor(private readonly config: Extract<FileLinkResolverConfig, { type: "zed"; mode: "ssh" }>) {}

  isMatch(path: string): boolean {
    return isLocalFilePath(path);
  }

  resolve(path: string, cwd: string): string {
    const { filePath, suffix } = splitFilePathSuffix(path);
    const absolutePath = resolvePath(cwd, decodeFilePath(filePath));
    const hostname = encodeURIComponent(this.config.hostname);
    const username = this.config.username === undefined
      ? ""
      : `${encodeURIComponent(this.config.username)}@`;
    const port = this.config.port === undefined ? "" : `:${this.config.port}`;
    return `zed://ssh/${username}${hostname}${port}${encodeAbsolutePath(absolutePath)}${suffix}`;
  }
}

export function createZedFileLinkResolvers(
  configs: readonly FileLinkResolverConfig[],
): FileLinkResolver[] {
  const resolvers: FileLinkResolver[] = [];
  for (const config of configs) {
    if (config.type !== "zed") continue;
    resolvers.push(config.mode === "ssh"
      ? new ZedSshFileLinkResolver(config)
      : new ZedLocalFileLinkResolver());
  }
  return resolvers;
}

/**
 * Rewrites complete local destinations in ordinary Markdown. Code spans and fenced code blocks are
 * skipped. An unfinished link at the snapshot tail is withheld until completion or a final frame.
 * Resolvers are checked in order and the first match owns both success and failure for that link.
 */
export function resolveMarkdownFileLinks(
  text: string,
  cwd: string,
  resolvers: readonly FileLinkResolver[],
  final: boolean,
): FileLinkResolution | Promise<FileLinkResolution> {
  const scan = scanMarkdownLinks(text);
  const sourceEnd = !final && scan.incompleteStart !== null ? scan.incompleteStart : text.length;
  const sourceText = text.slice(0, sourceEnd);
  const links = scan.links.filter((link) => link.end <= sourceEnd);
  const replacements: Replacement[] = [];
  const protectedHrefs: string[] = [];

  const complete = (): FileLinkResolution => applyReplacements(sourceText, replacements, protectedHrefs);
  const resolveLink = (link: MarkdownLink): string | Promise<string> | null => {
    for (const resolver of resolvers) {
      let matches: boolean;
      try {
        matches = resolver.isMatch(splitFilePathSuffix(link.destination).filePath);
      } catch {
        // A broken matcher does not claim this link; later resolvers may still match it.
        continue;
      }
      if (!matches) continue;
      try {
        return resolver.resolve(link.destination, cwd);
      } catch (error) {
        // The first matching resolver owns this link; a failure preserves its original destination.
        return Promise.reject(error);
      }
    }
    return null;
  };
  const accept = (link: MarkdownLink, href: string): void => {
    if (typeof href !== "string" || href.length === 0) return;
    const convertedLink = sourceText.slice(link.start, link.destinationStart)
      + href
      + sourceText.slice(link.destinationEnd, link.end);
    if (Buffer.byteLength(convertedLink, "utf8") > MAX_TEXT_BYTES) return;
    replacements.push({
      start: link.destinationStart,
      end: link.destinationEnd,
      href,
    });
    protectedHrefs.push(href);
  };
  const resolveRemainder = async (
    index: number,
    current: MarkdownLink,
    pending: Promise<string>,
  ): Promise<FileLinkResolution> => {
    try {
      accept(current, await pending);
    } catch {
      // A resolver failure leaves the source destination in place; it never suppresses the reply.
    }
    for (let cursor = index + 1; cursor < links.length; cursor += 1) {
      const link = links[cursor] as MarkdownLink;
      const resolved = resolveLink(link);
      if (resolved === null) continue;
      try {
        accept(link, await resolved);
      } catch {
        // A resolver failure leaves the source destination in place; it never suppresses the reply.
      }
    }
    return complete();
  };

  for (let index = 0; index < links.length; index += 1) {
    const link = links[index] as MarkdownLink;
    const resolved = resolveLink(link);
    if (resolved === null) continue;
    if (isPromiseLike(resolved)) return resolveRemainder(index, link, Promise.resolve(resolved));
    accept(link, resolved);
  }
  return complete();
}

function applyReplacements(
  sourceText: string,
  replacements: readonly Replacement[],
  protectedHrefs: readonly string[],
): FileLinkResolution {
  if (replacements.length === 0) return { text: sourceText, protectedHrefs: [] };
  let text = "";
  let cursor = 0;
  for (const replacement of replacements) {
    text += sourceText.slice(cursor, replacement.start);
    text += replacement.href;
    cursor = replacement.end;
  }
  text += sourceText.slice(cursor);
  return { text, protectedHrefs: [...protectedHrefs] };
}

function isLocalFilePath(path: string): boolean {
  return path.length > 0
    && !path.startsWith("#")
    && !path.startsWith("?")
    && !path.startsWith("//")
    && !URI_SCHEME.test(path);
}

function splitFilePathSuffix(path: string): { filePath: string; suffix: string } {
  const fragmentIndex = path.indexOf("#");
  if (fragmentIndex < 0) return { filePath: path, suffix: "" };
  return { filePath: path.slice(0, fragmentIndex), suffix: path.slice(fragmentIndex) };
}

function decodeFilePath(path: string): string {
  try {
    return decodeURIComponent(path.replace(/\\([\\()])/gu, "$1"));
  } catch {
    return path;
  }
}

function encodeAbsolutePath(path: string): string {
  return path.split("/").map((segment) => encodeURIComponent(segment)).join("/");
}

function isPromiseLike<T>(value: T | Promise<T>): value is Promise<T> {
  return typeof value === "object" && value !== null && "then" in value
    && typeof (value as { then?: unknown }).then === "function";
}

function isEscaped(text: string, index: number): boolean {
  let backslashes = 0;
  for (let cursor = index - 1; cursor >= 0 && text[cursor] === "\\"; cursor -= 1) backslashes += 1;
  return backslashes % 2 === 1;
}

function runLength(text: string, start: number, token: string): number {
  let end = start;
  while (text[end] === token) end += 1;
  return end - start;
}

function lineEnd(text: string, start: number): number {
  const index = text.indexOf("\n", start);
  return index < 0 ? text.length : index;
}

function indentedCodeBlockEnd(text: string, start: number): number | null {
  let lineStart = start;
  let found = false;
  while (lineStart < text.length) {
    const end = lineEnd(text, lineStart);
    let cursor = lineStart;
    let spaces = 0;
    while (text[cursor] === " " && spaces < 4) {
      cursor += 1;
      spaces += 1;
    }
    const indented = spaces === 4 || text[cursor] === "\t";
    let blankCursor = lineStart;
    while (
      blankCursor < end
      && (text[blankCursor] === " " || text[blankCursor] === "\t" || text[blankCursor] === "\r")
    ) {
      blankCursor += 1;
    }
    const blank = blankCursor === end;
    if (indented) found = true;
    else if (blank && found) {
      // Blank lines remain part of an indented block until ordinary text resumes.
    } else {
      return found ? lineStart : null;
    }
    if (end === text.length) return text.length;
    lineStart = end + 1;
  }
  return found ? text.length : null;
}

function fenceAtLineStart(
  text: string,
  start: number,
): { character: "`" | "~"; length: number; afterLine: number } | null {
  let cursor = start;
  let indent = 0;
  while (text[cursor] === " " && indent < 4) {
    cursor += 1;
    indent += 1;
  }
  if (indent > 3 || (text[cursor] !== "`" && text[cursor] !== "~")) return null;
  const character = text[cursor] as "`" | "~";
  const length = runLength(text, cursor, character);
  if (length < 3) return null;
  const end = lineEnd(text, cursor + length);
  if (character === "`") {
    for (let index = cursor + length; index < end; index += 1) {
      if (text[index] === "`") return null;
    }
  }
  return { character, length, afterLine: end < text.length ? end + 1 : end };
}

function findClosingFence(
  text: string,
  start: number,
  character: "`" | "~",
  minimumLength: number,
): number {
  let lineStart = start;
  while (lineStart <= text.length) {
    let cursor = lineStart;
    let indent = 0;
    while (text[cursor] === " " && indent < 4) {
      cursor += 1;
      indent += 1;
    }
    if (indent <= 3 && text[cursor] === character) {
      const length = runLength(text, cursor, character);
      if (length >= minimumLength) {
        const end = lineEnd(text, cursor + length);
        let trailing = cursor + length;
        while (trailing < end && (text[trailing] === " " || text[trailing] === "\t" || text[trailing] === "\r")) {
          trailing += 1;
        }
        if (trailing === end) return end < text.length ? end + 1 : end;
      }
    }
    const end = lineEnd(text, lineStart);
    if (end === text.length) return -1;
    lineStart = end + 1;
  }
  return -1;
}

function matchingCodeSpan(text: string, start: number, length: number): number {
  let cursor = start;
  while (cursor < text.length) {
    const next = text.indexOf("`", cursor);
    if (next < 0) return -1;
    const nextLength = runLength(text, next, "`");
    if (nextLength === length) return next + nextLength;
    cursor = next + nextLength;
  }
  return -1;
}

function findClosingBracket(text: string, start: number): number {
  let depth = 1;
  for (let cursor = start + 1; cursor < text.length; cursor += 1) {
    if (isEscaped(text, cursor)) continue;
    if (text[cursor] === "[") depth += 1;
    else if (text[cursor] === "]" && --depth === 0) return cursor;
  }
  return -1;
}

function parseDestination(
  text: string,
  openParen: number,
): { destinationStart: number; destinationEnd: number; end: number } | "incomplete" | null {
  let cursor = openParen + 1;
  while (cursor < text.length && /[ \t\r\n]/u.test(text[cursor] as string)) cursor += 1;
  if (cursor === text.length) return "incomplete";

  let destinationStart: number;
  let destinationEnd: number;
  if (text[cursor] === "<") {
    destinationStart = cursor + 1;
    cursor = destinationStart;
    while (cursor < text.length && (text[cursor] !== ">" || isEscaped(text, cursor))) cursor += 1;
    if (cursor === text.length) return "incomplete";
    destinationEnd = cursor;
    cursor += 1;
  } else {
    destinationStart = cursor;
    let nestedParens = 0;
    while (cursor < text.length) {
      const character = text[cursor] as string;
      if (isEscaped(text, cursor)) {
        cursor += 1;
        continue;
      }
      if (character === "(") nestedParens += 1;
      else if (character === ")") {
        if (nestedParens === 0) break;
        nestedParens -= 1;
      } else if (/[ \t\r\n]/u.test(character)) {
        break;
      }
      cursor += 1;
    }
    destinationEnd = cursor;
  }

  while (cursor < text.length && /[ \t\r\n]/u.test(text[cursor] as string)) cursor += 1;
  if (cursor < text.length && text[cursor] === ")") return { destinationStart, destinationEnd, end: cursor + 1 };
  if (cursor === text.length) return "incomplete";

  const titleStart = text[cursor];
  if (titleStart === "\"" || titleStart === "'") {
    const titleEnd = findUnescaped(text, titleStart, cursor + 1);
    if (titleEnd < 0) return "incomplete";
    cursor = titleEnd + 1;
  } else if (titleStart === "(") {
    const titleEnd = findBalancedParens(text, cursor);
    if (titleEnd < 0) return "incomplete";
    cursor = titleEnd + 1;
  } else {
    return null;
  }
  while (cursor < text.length && /[ \t\r\n]/u.test(text[cursor] as string)) cursor += 1;
  if (cursor === text.length) return "incomplete";
  return text[cursor] === ")" ? { destinationStart, destinationEnd, end: cursor + 1 } : null;
}

function findUnescaped(text: string, character: string, start: number): number {
  for (let cursor = start; cursor < text.length; cursor += 1) {
    if (text[cursor] === character && !isEscaped(text, cursor)) return cursor;
  }
  return -1;
}

function findBalancedParens(text: string, start: number): number {
  let depth = 1;
  for (let cursor = start + 1; cursor < text.length; cursor += 1) {
    if (isEscaped(text, cursor)) continue;
    if (text[cursor] === "(") depth += 1;
    else if (text[cursor] === ")" && --depth === 0) return cursor;
  }
  return -1;
}

function scanMarkdownLinks(text: string): MarkdownScan {
  const links: MarkdownLink[] = [];
  let position = 0;
  while (position < text.length) {
    if (position === 0 || text[position - 1] === "\n") {
      const indentedCodeEnd = indentedCodeBlockEnd(text, position);
      if (indentedCodeEnd !== null) {
        position = indentedCodeEnd;
        continue;
      }
      const fence = fenceAtLineStart(text, position);
      if (fence) {
        const close = findClosingFence(text, fence.afterLine, fence.character, fence.length);
        if (close < 0) return { links, incompleteStart: null };
        position = close;
        continue;
      }
    }
    if (text[position] === "`" && !isEscaped(text, position)) {
      const length = runLength(text, position, "`");
      const close = matchingCodeSpan(text, position + length, length);
      if (close < 0) break;
      position = close;
      continue;
    }
    if (text[position] !== "[" || isEscaped(text, position)) {
      position += 1;
      continue;
    }

    const bracketEnd = findClosingBracket(text, position);
    if (bracketEnd < 0) return { links, incompleteStart: position };
    if (text[bracketEnd + 1] !== "(") {
      if (bracketEnd + 1 === text.length) return { links, incompleteStart: position };
      position = bracketEnd + 1;
      continue;
    }
    const parsed = parseDestination(text, bracketEnd + 1);
    if (parsed === "incomplete") return { links, incompleteStart: position };
    if (parsed === null) {
      position = bracketEnd + 2;
      continue;
    }
    const linkStart = position > 0 && text[position - 1] === "!" && !isEscaped(text, position - 1)
      ? position - 1
      : position;
    links.push({
      start: linkStart,
      end: parsed.end,
      destinationStart: parsed.destinationStart,
      destinationEnd: parsed.destinationEnd,
      destination: text.slice(parsed.destinationStart, parsed.destinationEnd),
    });
    position = parsed.end;
  }
  return { links, incompleteStart: null };
}

/** Splits on UTF-8 boundaries without cutting any listed converted Markdown link unit. */
export function splitUtf8WithProtectedMarkdownLinks(
  text: string,
  protectedHrefs: readonly string[],
  maxBytes = MAX_TEXT_BYTES,
): string[] {
  if (text.length === 0) return [""];
  const protectedUnits = protectedMarkdownLinkRanges(text, protectedHrefs)
    .filter(({ start, end }) => Buffer.byteLength(text.slice(start, end), "utf8") <= maxBytes);
  if (protectedUnits.length === 0) return splitUtf8(text, maxBytes);

  const characters = Array.from(text);
  const codeUnitOffsets = new Array<number>(characters.length + 1);
  codeUnitOffsets[0] = 0;
  for (let index = 0; index < characters.length; index += 1) {
    codeUnitOffsets[index + 1] = (codeUnitOffsets[index] ?? 0) + (characters[index]?.length ?? 0);
  }
  const protectedPoints = protectedUnits.map(({ start, end }) => ({
    start: codePointOffset(codeUnitOffsets, start),
    end: codePointOffset(codeUnitOffsets, end),
  }));

  const chunks: string[] = [];
  let start = 0;
  while (start < characters.length) {
    let bytes = 0;
    let end = start;
    while (end < characters.length) {
      const size = utf8ByteLength(characters[end]?.codePointAt(0) ?? 0);
      if (bytes + size > maxBytes) break;
      bytes += size;
      end += 1;
    }
    if (end === characters.length) {
      chunks.push(characters.slice(start).join(""));
      break;
    }
    const crossing = protectedPoints.find((range) => range.start >= start && range.start < end && range.end > end);
    const safeEnd = crossing?.start ?? end;
    const cut = findProtectedCut(characters, start, safeEnd, protectedPoints);
    chunks.push(characters.slice(start, cut).join(""));
    start = cut;
  }
  return chunks;
}

export interface MarkdownTextRange {
  start: number;
  end: number;
}

/** Finds complete converted Markdown links without changing their source text. */
export function protectedMarkdownLinkRanges(
  text: string,
  protectedHrefs: readonly string[],
): MarkdownTextRange[] {
  const hrefs = new Set(protectedHrefs);
  if (hrefs.size === 0) return [];
  const ranges: MarkdownTextRange[] = [];
  for (const link of scanMarkdownLinks(text).links) {
    if (hrefs.has(text.slice(link.destinationStart, link.destinationEnd))) {
      ranges.push({ start: link.start, end: link.end });
    }
  }
  return ranges;
}

function codePointOffset(offsets: readonly number[], codeUnitOffset: number): number {
  let low = 0;
  let high = offsets.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if ((offsets[middle] ?? 0) < codeUnitOffset) low = middle + 1;
    else high = middle;
  }
  return low;
}

function findProtectedCut(
  characters: readonly string[],
  start: number,
  end: number,
  protectedRanges: readonly { start: number; end: number }[],
): number {
  let sentence = -1;
  let clause = -1;
  let word = -1;
  for (let cut = end; cut > start; cut -= 1) {
    if (protectedRanges.some((range) => cut > range.start && cut < range.end)) continue;
    const character = characters[cut - 1] as string;
    const following = characters[cut];
    if (character === "\n") return cut;
    if (
      (character === "." || character === "!" || character === "?" || character === "," || character === ";")
      && following !== undefined
      && following !== " " && following !== "\t" && following !== "\n" && following !== "\r"
    ) continue;
    if (character === "。" || character === "！" || character === "？" || character === "；" || character === "…") {
      if (sentence < 0) sentence = cut;
    } else if (character === "，" || character === "、" || character === "," || character === ";") {
      if (clause < 0) clause = cut;
    } else if (character === " " || character === "\t") {
      if (word < 0) word = cut;
    }
  }
  if (sentence > 0) return sentence;
  if (clause > 0) return clause;
  if (word > 0) return word;
  return end;
}

function splitUtf8(text: string, maxBytes: number): string[] {
  const characters = Array.from(text);
  const chunks: string[] = [];
  let start = 0;
  while (start < characters.length) {
    let bytes = 0;
    let end = start;
    while (end < characters.length) {
      const size = utf8ByteLength(characters[end]?.codePointAt(0) ?? 0);
      if (bytes + size > maxBytes) break;
      bytes += size;
      end += 1;
    }
    if (end === characters.length) {
      chunks.push(characters.slice(start).join(""));
      break;
    }
    const cut = findProtectedCut(characters, start, end, []);
    chunks.push(characters.slice(start, cut).join(""));
    start = cut;
  }
  return chunks;
}

function utf8ByteLength(codePoint: number): number {
  if (codePoint < 0x80) return 1;
  if (codePoint < 0x800) return 2;
  if (codePoint < 0x10000) return 3;
  return 4;
}

export interface FileLinkStream {
  enqueue(text: string, final: boolean): Promise<void>;
  settle(): Promise<void>;
}

/** Resolves snapshots independently, but only the newest revision may commit. */
export function createFileLinkStream(
  resolve: (text: string, final: boolean) => FileLinkResolution | Promise<FileLinkResolution>,
  commit: (resolution: FileLinkResolution, final: boolean, sourceText: string) => void,
): FileLinkStream {
  let newestRevision = 0;
  let finalSeen = false;
  let latestOperation: Promise<void> = Promise.resolve();
  return {
    enqueue(text, final) {
      if (finalSeen && !final) return latestOperation;
      if (final) finalSeen = true;
      const revision = ++newestRevision;
      const operation = Promise.resolve().then(async () => {
        let resolution: FileLinkResolution;
        try {
          resolution = await resolve(text, final);
        } catch {
          const fallback = resolveMarkdownFileLinks(text, "", [], final);
          resolution = isPromiseLike(fallback) ? await fallback : fallback;
        }
        if (revision === newestRevision) commit(resolution, final, text);
      });
      latestOperation = operation;
      return operation;
    },
    async settle() {
      while (true) {
        const observed = latestOperation;
        try {
          await observed;
        } catch (error) {
          if (observed === latestOperation) throw error;
        }
        if (observed === latestOperation) return;
      }
    },
  };
}
