/** Parsed ordinary-reply text, keeping structured member markers distinct from literal text. */
export type ReplyTextPart =
  | { type: "text"; text: string }
  | { type: "mention"; uid: string; text: string };

type MemberMarker =
  | { kind: "valid"; end: number; uid: string; visibleText: string }
  | { kind: "oversized"; end: number }
  | { kind: "partial" }
  | { kind: "invalid" };

function isLineBreak(character: string | undefined): boolean {
  return character === "\n" || character === "\r";
}

function isEscaped(text: string, index: number): boolean {
  let slashes = 0;
  for (let position = index - 1; position >= 0 && text[position] === "\\"; position -= 1) {
    slashes += 1;
  }
  return slashes % 2 === 1;
}

function readRun(text: string, start: number, character: string): number {
  let end = start;
  while (text[end] === character) end += 1;
  return end - start;
}

function lineEnd(text: string, start: number): number {
  const newline = text.indexOf("\n", start);
  return newline < 0 ? text.length : newline;
}

function fenceAtLineStart(
  text: string,
  start: number,
): { character: "`" | "~"; length: number; afterLine: number } | null {
  let position = start;
  let indentation = 0;
  while (text[position] === " " && indentation < 4) {
    position += 1;
    indentation += 1;
  }
  if (indentation > 3) return null;
  const character = text[position];
  if (character !== "`" && character !== "~") return null;
  const length = readRun(text, position, character);
  if (length < 3) return null;
  const end = lineEnd(text, position + length);
  if (character === "`") {
    for (let index = position + length; index < end; index += 1) {
      if (text[index] === "`") return null;
    }
  }
  return {
    character,
    length,
    afterLine: end < text.length ? end + 1 : end,
  };
}

function closingFence(
  text: string,
  start: number,
  character: "`" | "~",
  minimumLength: number,
): number {
  let lineStart = start;
  while (lineStart <= text.length) {
    let position = lineStart;
    let indentation = 0;
    while (text[position] === " " && indentation < 4) {
      position += 1;
      indentation += 1;
    }
    if (indentation <= 3 && text[position] === character) {
      const length = readRun(text, position, character);
      if (length >= minimumLength) {
        const end = lineEnd(text, position + length);
        let trailing = position + length;
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

function matchingCodeSpan(text: string, start: number, runLength: number): number {
  let position = start;
  while (position < text.length) {
    const next = text.indexOf("`", position);
    if (next < 0) return -1;
    const length = readRun(text, next, "`");
    if (length === runLength) return next + length;
    position = next + length;
  }
  return -1;
}

function readMemberMarker(text: string, start: number, allowPartial: boolean, maxBytes: number): MemberMarker {
  if (text[start + 1] !== "[") {
    return allowPartial && start + 1 === text.length ? { kind: "partial" } : { kind: "invalid" };
  }

  const uidStart = start + 2;
  let colon = -1;
  for (let position = uidStart; position < text.length; position += 1) {
    const character = text[position] as string;
    if (isLineBreak(character)) return { kind: "invalid" };
    if (character === "]" && !isEscaped(text, position)) return { kind: "invalid" };
    if (character === ":" && !isEscaped(text, position)) {
      colon = position;
      break;
    }
  }
  if (colon < 0) return allowPartial ? { kind: "partial" } : { kind: "invalid" };
  if (colon === uidStart) return { kind: "invalid" };

  const uid = text.slice(uidStart, colon);
  const displayParts: string[] = [];
  let position = colon + 1;
  while (position < text.length) {
    const character = text[position] as string;
    if (isLineBreak(character)) return { kind: "invalid" };
    if (character === "\\") {
      const escaped = text[position + 1];
      if (escaped === undefined) return allowPartial ? { kind: "partial" } : { kind: "invalid" };
      if (escaped !== "\\" && escaped !== "]") return { kind: "invalid" };
      displayParts.push(escaped);
      position += 2;
      continue;
    }
    if (character === "]") {
      if (displayParts.length === 0) return { kind: "invalid" };
      const visibleText = `@${displayParts.join("")}`;
      if (Buffer.byteLength(visibleText, "utf8") > maxBytes) return { kind: "oversized", end: position + 1 };
      return { kind: "valid", end: position + 1, uid, visibleText };
    }
    displayParts.push(character);
    position += 1;
  }
  return allowPartial ? { kind: "partial" } : { kind: "invalid" };
}

function appendText(parts: ReplyTextPart[], text: string): void {
  if (text.length === 0) return;
  const previous = parts.at(-1);
  if (previous?.type === "text") {
    previous.text += text;
  } else {
    parts.push({ type: "text", text });
  }
}

/** Parses complete member markers outside Markdown code while retaining every literal byte. */
export function parseReplyMemberMarkers(
  text: string,
  allowPartial: boolean,
  maxMentionBytes: number,
): ReplyTextPart[] {
  const parts: ReplyTextPart[] = [];
  let literalStart = 0;
  let position = 0;

  while (position < text.length) {
    if (position === 0 || text[position - 1] === "\n") {
      const fence = fenceAtLineStart(text, position);
      if (fence) {
        const close = closingFence(text, fence.afterLine, fence.character, fence.length);
        if (close < 0) break;
        position = close;
        continue;
      }
    }

    if (text[position] === "`" && !isEscaped(text, position)) {
      const length = readRun(text, position, "`");
      const close = matchingCodeSpan(text, position + length, length);
      if (close >= 0) {
        position = close;
        continue;
      }
      // Until a later snapshot proves this code span closed, markers in the suffix are literal.
      position = text.length;
      continue;
    }

    if (text[position] !== "@" || isEscaped(text, position)) {
      position += 1;
      continue;
    }

    const marker = readMemberMarker(text, position, allowPartial, maxMentionBytes);
    if (marker.kind === "partial") {
      appendText(parts, text.slice(literalStart, position));
      return parts;
    }
    if (marker.kind === "oversized") {
      position = marker.end;
      continue;
    }
    if (marker.kind === "valid") {
      appendText(parts, text.slice(literalStart, position));
      parts.push({ type: "mention", uid: marker.uid, text: marker.visibleText });
      position = marker.end;
      literalStart = position;
      continue;
    }
    position += 1;
  }

  appendText(parts, text.slice(literalStart));
  return parts;
}
