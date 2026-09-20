/**
 * 展示文本净化:敏感凭据脱敏、控制字符与空白归一、按 Unicode code point 截断。
 * 由执行 tracker 迁入,供 tracker、研究快照净化与任何需要安全展示文本的边界复用。
 */

const MAX_LOCAL_VISIBLE_TEXT_CHARS = 240;

function truncateDisplayText(value: string, maximum = MAX_LOCAL_VISIBLE_TEXT_CHARS): string {
  let codePointCount = 0;
  let codeUnitOffset = 0;
  for (const character of value) {
    if (codePointCount >= maximum) return `${value.slice(0, codeUnitOffset)}…`;
    codePointCount += 1;
    codeUnitOffset += character.length;
  }
  return value;
}

function redactSensitiveText(value: string): string {
  let redacted = value;
  redacted = redacted.replace(
    /((?:authorization|api[-_.]?key|client[-_.]?secret|cookie|credentials?|pass(?:word|wd)|secret|session[-_.]?token|sig(?:nature)?|token)\s*[:=]\s*)(?:bearer\s+)?[^\s,;&]+/giu,
    "$1[redacted]",
  );
  redacted = redacted.replace(/(\b(?:bearer|basic|digest|token)\s+)[^\s,;&]+/giu, "$1[redacted]");
  redacted = redacted.replace(
    /([?&](?:access[-_.]?(?:key|token)|api[-_.]?key|authorization|password|passwd|secret|sig(?:nature)?|token|x[-_.]?api[-_.]?key)\s*=)[^&#\s]*/giu,
    "$1[redacted]",
  );
  redacted = redacted.replace(/\b(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{6,}\b/gu, "[redacted]");
  redacted = redacted.replace(/\bAKIA[0-9A-Z]{16}\b/gu, "[redacted]");
  return redacted;
}

export function safeDisplayText(value: unknown, maximum = MAX_LOCAL_VISIBLE_TEXT_CHARS): string | undefined {
  if (typeof value !== "string" || value.length === 0) return undefined;
  const normalized = redactSensitiveText(value)
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  if (normalized.length === 0) return undefined;
  return truncateDisplayText(normalized, maximum);
}
