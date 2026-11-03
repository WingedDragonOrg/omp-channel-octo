import { redactSensitiveText } from "./execution-text.ts";

/** Safe, bounded text retained from a native Git commit for progress cards. */
export interface CommitContent {
  readonly message?: string;
  readonly changelog?: string;
}

const TRUNCATION_MARKER = "…（已截断）";
const TRUNCATION_MARKER_CODE_POINTS = 6;
const INTERNAL_POSIX_PATH = /(^|[\s(\[{"'`=:])\/(?:Users|home|private|tmp|var|workspace|workspaces|Volumes|mnt|root|opt|Applications|System|Library|usr|etc|proc|dev)(?=\/|\s|["'`()\[\],;]|$)(?:\/[^\s"'`()\[\],;]*)*/gmu;
const INTERNAL_WINDOWS_PATH = /(^|[\s(\[{"'`=:])[A-Za-z]:\\(?:[^\s"'`()\[\],;]+\\)*[^\s"'`()\[\],;]*/gmu;

function truncateCodePoints(value: string, maximum: number): string {
  let count = 0;
  let codeUnitOffset = 0;
  let prefixOffset = 0;
  const prefixLength = maximum - TRUNCATION_MARKER_CODE_POINTS;
  for (const character of value) {
    if (count === prefixLength) prefixOffset = codeUnitOffset;
    if (count >= maximum) return `${value.slice(0, prefixOffset)}${TRUNCATION_MARKER}`;
    count += 1;
    codeUnitOffset += character.length;
  }
  return value;
}

function sanitizeField(value: unknown, maximum: number): string | undefined {
  if (typeof value !== "string" || value.length === 0) return undefined;
  const normalized = value
    .replace(/\r\n?/gu, "\n")
    .replace(/[\x00-\x09\x0b-\x1f\x7f]/gu, " ");
  const safeText = redactSensitiveText(normalized)
    .replace(INTERNAL_POSIX_PATH, "$1[本地路径]")
    .replace(INTERNAL_WINDOWS_PATH, "$1[本地路径]")
    .trim();
  if (safeText.length === 0) return undefined;
  return truncateCodePoints(safeText, maximum);
}

/**
 * Sanitizes untrusted native commit metadata before it enters a durable snapshot or card. Newline
 * structure is retained; only message/changelog are projected, so CLI output and diffs cannot leak.
 */
export function sanitizeCommitContent(value: CommitContent | undefined): CommitContent | undefined {
  const candidate: unknown = value;
  if (candidate === undefined || candidate === null || typeof candidate !== "object" || Array.isArray(candidate)) {
    return undefined;
  }
  const record = candidate as Record<string, unknown>;
  const message = sanitizeField(record.message, 800);
  const changelog = sanitizeField(record.changelog, 1_200);
  if (message === undefined && changelog === undefined) return undefined;
  return {
    ...(message === undefined ? {} : { message }),
    ...(changelog === undefined ? {} : { changelog }),
  };
}
