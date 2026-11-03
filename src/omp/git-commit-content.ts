import { sanitizeCommitContent, type CommitContent } from "../cards/commit-content.ts";
import { workerEnv } from "./profile.ts";

const CAPTURE_LIMIT_BYTES = 64 * 1024;
const TRUNCATION_MARKER = "…（已截断）";
const CHANGELOG_PATHSPEC = ":(top,glob)**/CHANGELOG.md";

type CapturedOutput = {
  text: string;
  truncated: boolean;
};

type GitCommandResult = {
  exitCode: number;
  stdout: CapturedOutput;
  stderr: CapturedOutput;
};

async function captureBounded(stream: ReadableStream<Uint8Array>): Promise<CapturedOutput> {
  const bytes = new Uint8Array(CAPTURE_LIMIT_BYTES);
  let length = 0;
  let truncated = false;
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      const available = CAPTURE_LIMIT_BYTES - length;
      const copied = Math.min(available, value.byteLength);
      if (copied > 0) {
        bytes.set(value.subarray(0, copied), length);
        length += copied;
      }
      if (copied < value.byteLength) truncated = true;
    }
  } finally {
    reader.releaseLock();
  }

  const decoder = new TextDecoder();
  const capturedBytes = bytes.subarray(0, length);
  const text = truncated
    ? decoder.decode(capturedBytes, { stream: true })
    : decoder.decode(capturedBytes);
  return { text, truncated };
}

async function gitCommand(cwd: string, args: readonly string[]): Promise<GitCommandResult> {
  const child = Bun.spawn(["git", "--no-pager", ...args], {
    cwd,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: workerEnv(),
  });
  const stdoutCapture = captureBounded(child.stdout).then(
    output => ({ output }),
    error => ({ error }),
  );
  const stderrCapture = captureBounded(child.stderr).then(
    output => ({ output }),
    error => ({ error }),
  );
  const [stdoutResult, stderrResult, exitCode] = await Promise.all([
    stdoutCapture,
    stderrCapture,
    child.exited,
  ]);
  if ("error" in stdoutResult) throw stdoutResult.error;
  if ("error" in stderrResult) throw stderrResult.error;
  return { exitCode, stdout: stdoutResult.output, stderr: stderrResult.output };
}

function withCaptureMarker(value: string, truncated: boolean): string {
  if (!truncated) return value;
  const separator = value.length === 0 || value.endsWith("\n") ? "" : "\n";
  return `${value}${separator}${TRUNCATION_MARKER}`;
}

function commitMessagesFrom(output: CapturedOutput): string {
  const messages = output.text
    .split("\0")
    .map(message => message.replace(/^\n+/u, "").replace(/\n+$/u, ""))
    .filter(message => message.length > 0);
  return withCaptureMarker(messages.join("\n\n"), output.truncated);
}

function addedChangelogLines(output: CapturedOutput): string | undefined {
  const lines = output.text.split("\n");
  if (output.truncated && !output.text.endsWith("\n")) lines.pop();
  const added: string[] = [];
  for (const line of lines) {
    if (line.startsWith("+++") || !line.startsWith("+")) continue;
    added.push(line.slice(1));
  }
  if (added.length === 0 && !output.truncated) return undefined;
  return withCaptureMarker(added.join("\n"), output.truncated);
}

async function emptyTreeId(cwd: string): Promise<string> {
  const result = await gitCommand(cwd, ["hash-object", "-t", "tree", "--stdin"]);
  if (result.exitCode !== 0) {
    throw new Error(`git hash-object -t tree --stdin failed (${result.exitCode}): ${result.stderr.text.trim()}`);
  }
  const oid = result.stdout.text.trim();
  if (oid.length === 0) throw new Error("git hash-object -t tree --stdin returned no object ID");
  return oid;
}

export async function readGitHead(cwd: string): Promise<string | null> {
  const result = await gitCommand(cwd, ["rev-parse", "--verify", "--quiet", "HEAD"]);
  if (result.exitCode === 1) return null;
  if (result.exitCode !== 0) {
    throw new Error(`git rev-parse --verify --quiet HEAD failed (${result.exitCode}): ${result.stderr.text.trim()}`);
  }
  const head = result.stdout.text.trim();
  if (head.length === 0) throw new Error("git rev-parse --verify --quiet HEAD returned no HEAD");
  return head;
}

export async function readGitCommitContent(
  cwd: string,
  headBefore: string | null,
  headAfter: string | null,
): Promise<CommitContent | undefined> {
  if (headAfter === null || headAfter === headBefore) return undefined;
  try {
    const changelogBase = headBefore ?? await emptyTreeId(cwd);
    const messageRange = headBefore === null ? headAfter : `${headBefore}..${headAfter}`;
    const [messageResult, changelogResult] = await Promise.all([
      gitCommand(cwd, [
        "log",
        "--no-patch",
        "--no-show-signature",
        "--no-notes",
        "--format=%B%x00",
        "-z",
        "--reverse",
        messageRange,
      ]),
      gitCommand(cwd, [
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--no-renames",
        "--no-color",
        "--unified=0",
        changelogBase,
        headAfter,
        "--",
        CHANGELOG_PATHSPEC,
      ]),
    ]);
    if (messageResult.exitCode !== 0 || changelogResult.exitCode !== 0) return undefined;
    const message = commitMessagesFrom(messageResult.stdout);
    const changelog = addedChangelogLines(changelogResult.stdout);
    return sanitizeCommitContent({
      ...(message.length === 0 ? {} : { message }),
      ...(changelog === undefined ? {} : { changelog }),
    });
  } catch {
    return undefined;
  }
}
