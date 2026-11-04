import { constants, type Stats } from "node:fs";
import { lstat, open, readlink, realpath } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { redactSensitiveText } from "../cards/execution-text.ts";

export interface ReviewProjectInput {
  readonly name: string;
  readonly path: string;
}

export type ReviewSnapshotScope = "project" | "head" | "staged" | "unstaged" | "untracked";

export interface ReviewUncovered {
  readonly scope: ReviewSnapshotScope;
  readonly detail: string;
}

export interface ReviewChangeScope {
  readonly files: readonly string[];
  /** Untruncated Git patch evidence; any credential redaction is recorded in `uncovered`. */
  readonly evidence: string;
}

export interface ReviewProjectSnapshot {
  readonly name: string;
  readonly requestedPath: string;
  readonly path?: string;
  readonly repositoryPath?: string;
  readonly headBefore?: string;
  readonly headAfter?: string;
  /** True only when HEAD and raw staged, worktree, and untracked evidence match a second full observation. */
  readonly baselineStable: boolean;
  /** Git cannot atomically snapshot the index, worktree, and untracked file contents together. */
  readonly captureAtomic: false;
  readonly staged: ReviewChangeScope;
  readonly unstaged: ReviewChangeScope;
  readonly untracked: ReviewChangeScope;
  readonly uncovered: readonly ReviewUncovered[];
}

export interface ReviewSnapshot {
  readonly capturedAt: string;
  readonly projects: readonly ReviewProjectSnapshot[];
}

type GitResult = {
  readonly exitCode: number;
  readonly stdout: Buffer;
  readonly stderrNonempty: boolean;
  readonly timedOut: boolean;
  readonly outputLimitExceeded: boolean;
};

type CapturedScope = {
  readonly snapshot: ReviewChangeScope;
  readonly digest: string;
  readonly complete: boolean;
};

const EMPTY_SCOPE: ReviewChangeScope = Object.freeze({ files: Object.freeze([]), evidence: "" });
const MAX_GIT_OUTPUT_BYTES = 4 * 1024 * 1024;
const MAX_UNTRACKED_EVIDENCE_BYTES = 4 * 1024 * 1024;
const MAX_UNTRACKED_FILE_BYTES = 2 * 1024 * 1024;
const GIT_TIMEOUT_MS = 30_000;
function gitEnvironment(): Readonly<Record<string, string>> {
  return {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    ...(process.env.HOME === undefined ? {} : { HOME: process.env.HOME }),
    ...(process.env.USERPROFILE === undefined ? {} : { USERPROFILE: process.env.USERPROFILE }),
    ...(process.env.XDG_CONFIG_HOME === undefined ? {} : { XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME }),
    ...(process.env.TMPDIR === undefined ? {} : { TMPDIR: process.env.TMPDIR }),
    ...(process.env.GIT_CONFIG_GLOBAL === undefined ? {} : { GIT_CONFIG_GLOBAL: process.env.GIT_CONFIG_GLOBAL }),
    ...(process.env.GIT_CONFIG_SYSTEM === undefined ? {} : { GIT_CONFIG_SYSTEM: process.env.GIT_CONFIG_SYSTEM }),
    ...(process.env.GIT_CONFIG_NOSYSTEM === undefined ? {} : { GIT_CONFIG_NOSYSTEM: process.env.GIT_CONFIG_NOSYSTEM }),
    GIT_EXTERNAL_DIFF: "",
    GIT_OPTIONAL_LOCKS: "0",
    GIT_PAGER: "cat",
    LC_ALL: "C",
  };
}

type ReadResult = { readonly output: Buffer; readonly exceeded: boolean };

async function readCapped(
  stream: ReadableStream<Uint8Array>,
  maximum: number,
  onLimit: () => void,
): Promise<ReadResult> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  let exceeded = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (exceeded) continue;
    if (bytes + value.byteLength > maximum) {
      exceeded = true;
      chunks.length = 0;
      onLimit();
      continue;
    }
    chunks.push(value);
    bytes += value.byteLength;
  }
  return { output: exceeded ? Buffer.alloc(0) : Buffer.concat(chunks, bytes), exceeded };
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<boolean> {
  const reader = stream.getReader();
  let nonempty = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) return nonempty;
    if (value.byteLength > 0) nonempty = true;
  }
}

function signalProcessGroup(pid: number, signal: NodeJS.Signals): void {
  if (process.platform === "win32") {
    try {
      process.kill(pid, signal);
    } catch {}
    return;
  }
  try {
    process.kill(-pid, signal);
  } catch {
    try {
      process.kill(pid, signal);
    } catch {}
  }
}

async function waitForProcessGroupExit(pid: number): Promise<void> {
  if (process.platform === "win32") return;
  while (true) {
    try {
      process.kill(-pid, 0);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
    }
    const delay = Promise.withResolvers<void>();
    setTimeout(delay.resolve, 10);
    await delay.promise;
  }
}

async function settleAll<T extends readonly unknown[]>(
  operations: { readonly [K in keyof T]: Promise<T[K]> },
): Promise<T> {
  const results = await Promise.allSettled(operations);
  const values: unknown[] = [];
  for (const result of results) {
    if (result.status === "rejected") throw result.reason;
    values.push(result.value);
  }
  return values as unknown as T;
}

async function git(cwd: string, args: readonly string[], signal?: AbortSignal): Promise<GitResult> {
  if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  const child = Bun.spawn(["git", "-c", "core.fsmonitor=false", "--no-pager", ...args], {
    cwd,
    env: gitEnvironment(),
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    detached: true,
  });
  let timedOut = false;
  let outputLimitExceeded = false;
  let terminationRequested = false;
  let killTimer: ReturnType<typeof setTimeout> | undefined;
  const terminate = () => {
    if (terminationRequested) return;
    terminationRequested = true;
    signalProcessGroup(child.pid, "SIGTERM");
    killTimer = setTimeout(() => signalProcessGroup(child.pid, "SIGKILL"), 250);
  };
  const abort = () => terminate();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) terminate();
  const timeout = setTimeout(() => {
    timedOut = true;
    terminate();
  }, GIT_TIMEOUT_MS);
  try {
    const [exitCode, stdout, stderrNonempty] = await settleAll([
      child.exited,
      readCapped(child.stdout, MAX_GIT_OUTPUT_BYTES, () => {
        outputLimitExceeded = true;
        terminate();
      }),
      drain(child.stderr),
    ]);
    await waitForProcessGroupExit(child.pid);
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    return {
      exitCode,
      stdout: stdout.output,
      stderrNonempty,
      timedOut,
      outputLimitExceeded: outputLimitExceeded || stdout.exceeded,
    };
  } catch (error) {
    terminate();
    await child.exited.catch(() => {});
    await waitForProcessGroupExit(child.pid);
    throw error;
  } finally {
    clearTimeout(timeout);
    clearTimeout(killTimer);
    signal?.removeEventListener("abort", abort);
  }
}

function text(output: Buffer): string {
  return output.toString("utf8");
}

function oneLine(output: Buffer): string | undefined {
  const value = text(output).trim();
  return value.length > 0 && !value.includes("\n") && !value.includes("\0") ? value : undefined;
}

function nulFields(output: Buffer): string[] {
  const fields = text(output).split("\0");
  if (fields.at(-1) === "") fields.pop();
  return fields;
}

export function redactReviewSensitiveText(value: string): { readonly text: string; readonly redacted: boolean } {
  const sensitiveKey = String.raw`(?:access[-_.]?token|api[-_.]?key|api[-_.]?secret|authorization|aws[-_.]?secret[-_.]?access[-_.]?key|client[-_.]?secret|cookie|credentials?|pass(?:word|wd)|private[-_.]?key|refresh[-_.]?token|secret|session[-_.]?token|sig(?:nature)?|signing[-_.]?key|token|x[-_.]?api[-_.]?key)`;
  const quotedKey = new RegExp(
    String.raw`(["'])(${sensitiveKey})\1(\s*:\s*)(["'])((?:\\.|(?!\4)[^\\])*)\4`,
    "giu",
  );
  let text = redactSensitiveText(value).replace(quotedKey, "$1$2$1$3$4[redacted]$4");
  text = text.replace(
    /-----BEGIN ([A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?)-----[\s\S]*?(?:-----END \1-----|$)/giu,
    "[redacted private key]",
  );
  return { text, redacted: text !== value };
}

function parseBinaryNumstat(output: Buffer): string[] {
  const fields = nulFields(output);
  const paths: string[] = [];
  for (let index = 0; index < fields.length;) {
    const record = fields[index++] ?? "";
    const firstTab = record.indexOf("\t");
    const secondTab = firstTab < 0 ? -1 : record.indexOf("\t", firstTab + 1);
    if (firstTab < 0 || secondTab < 0) continue;
    const added = record.slice(0, firstTab);
    const deleted = record.slice(firstTab + 1, secondTab);
    let path = record.slice(secondTab + 1);
    if (path.length === 0) {
      const source = fields[index++];
      const destination = fields[index++];
      path = destination ?? source ?? "";
    }
    if (added === "-" && deleted === "-" && path.length > 0) paths.push(path);
  }
  return [...new Set(paths)].sort((left, right) => left.localeCompare(right, "en"));
}
function parseDirtySubmodules(output: Buffer): string[] {
  return nulFields(output).flatMap(record => {
    if (!(record.startsWith("1 ") || record.startsWith("2 "))) return [];
    const pathFieldIndex = record[0] === "1" ? 8 : 9;
    const fields: string[] = [];
    let cursor = 0;
    for (let index = 0; index < pathFieldIndex; index += 1) {
      const separator = record.indexOf(" ", cursor);
      if (separator < 0) return [];
      fields.push(record.slice(cursor, separator));
      cursor = separator + 1;
    }
    const submoduleState = fields[2] ?? "";
    const path = record.slice(cursor);
    return submoduleState.startsWith("S") && /[MU]/u.test(submoduleState) && path
      ? [path]
      : [];
  });
}

function parseSubmodulePaths(output: Buffer): string[] {
  const fields = nulFields(output);
  const paths: string[] = [];
  for (let index = 0; index < fields.length;) {
    const metadata = fields[index++] ?? "";
    const path = fields[index++];
    if (path === undefined) break;
    const modes = metadata.slice(1).split(" ");
    if (modes[0] === "160000" || modes[1] === "160000") {
      const status = modes[4] ?? "";
      const destination = status.startsWith("R") || status.startsWith("C") ? fields[index++] : undefined;
      paths.push(destination ?? path);
    }
  }
  return paths;
}


function omitBinaryPatchBlocks(
  patch: string,
  scope: "staged" | "unstaged",
  binaryPaths: readonly string[],
  uncovered: ReviewUncovered[],
): string {
  const blocks = patch.split(/(?=^diff --git )/mu);
  const retained: string[] = [];
  let omittedBlocks = 0;
  for (const block of blocks) {
    if (/^(?:GIT binary patch|Binary files .* differ)\r?$/mu.test(block)) {
      omittedBlocks += 1;
    } else {
      retained.push(block);
    }
  }
  for (const path of binaryPaths) {
    uncovered.push({
      scope,
      detail: `binary content omitted from review evidence: ${
        JSON.stringify(safeEvidence(path, scope, uncovered))
      }`,
    });
  }
  if (omittedBlocks > binaryPaths.length) {
    uncovered.push({
      scope,
      detail: "binary content omitted from review evidence; its file path could not be verified",
    });
  }
  return retained.join("");
}

async function digestFileContents(path: string, expected: Stats, signal?: AbortSignal): Promise<string> {
  const handle = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
  try {
    const initial = await handle.stat();
    if (!initial.isFile()
      || initial.dev !== expected.dev
      || initial.ino !== expected.ino
      || initial.size !== expected.size) {
      throw new Error("untracked file changed before binary content verification");
    }
    const hash = createHash("sha256");
    const buffer = Buffer.allocUnsafe(64 * 1024);
    let bytes = 0;
    while (true) {
      if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, null);
      if (bytesRead === 0) break;
      bytes += bytesRead;
      if (bytes > MAX_UNTRACKED_FILE_BYTES) {
        throw new Error("untracked file grew past the content verification limit");
      }
      hash.update(buffer.subarray(0, bytesRead));
    }
    const final = await handle.stat();
    if (final.size !== initial.size || final.mtimeMs !== initial.mtimeMs || final.ctimeMs !== initial.ctimeMs) {
      throw new Error("untracked file changed during binary content verification");
    }
    return hash.digest("hex");
  } finally {
    await handle.close();
  }
}

function parseNameStatus(output: Buffer): string[] {
  const fields = nulFields(output);
  const files: string[] = [];
  for (let index = 0; index < fields.length;) {
    const status = fields[index++] ?? "";
    const path = fields[index++];
    if (path === undefined) break;
    if (status.startsWith("R") || status.startsWith("C")) {
      const destination = fields[index++];
      files.push(destination === undefined ? path : `${path} -> ${destination}`);
    } else {
      files.push(path);
    }
  }
  return files.sort((left, right) => left.localeCompare(right, "en"));
}

function safeEvidence(
  value: string,
  scope: "staged" | "unstaged" | "untracked",
  uncovered: ReviewUncovered[],
): string {
  const result = redactReviewSensitiveText(value);
  if (result.redacted && !uncovered.some(item =>
    item.scope === scope && item.detail === "sensitive-looking values were redacted from review evidence")) {
    uncovered.push({ scope, detail: "sensitive-looking values were redacted from review evidence" });
  }
  return result.text;
}


async function captureDiffScope(
  repositoryPath: string,
  scope: "staged" | "unstaged",
  uncovered: ReviewUncovered[],
  signal?: AbortSignal,
): Promise<CapturedScope> {
  const selector = scope === "staged" ? ["--cached", "HEAD"] : [];
  const common = ["diff", "--no-ext-diff", "--no-textconv"];
  const [names, numstat, patch, raw] = await settleAll([
    git(repositoryPath, [...common, "--name-status", "-z", "--find-renames", ...selector, "--"], signal),
    git(repositoryPath, [...common, "--numstat", "-z", "--find-renames", ...selector, "--"], signal),
    git(repositoryPath, [
      ...common,
      "--binary",
      "--full-index",
      "--find-renames",
      ...selector,
      "--",
    ], signal),
    git(repositoryPath, [...common, "--raw", "-z", "--find-renames", ...selector, "--"], signal),
  ]);
  const results = [names, numstat, patch, raw];
  if (results.some(result => result.outputLimitExceeded)) {
    uncovered.push({
      scope,
      detail: `${scope} evidence exceeded the ${MAX_GIT_OUTPUT_BYTES} byte Git output limit and was omitted`,
    });
  } else if (results.some(result => result.timedOut)) {
    uncovered.push({
      scope,
      detail: `${scope} Git evidence exceeded the ${GIT_TIMEOUT_MS} ms process timeout`,
    });
  } else if (results.some(result => result.exitCode !== 0)) {
    uncovered.push({ scope, detail: `${scope} Git evidence could not be captured completely` });
  }
  const rawFiles = names.exitCode === 0 ? parseNameStatus(names.stdout) : [];
  const binaryPaths = numstat.exitCode === 0 ? parseBinaryNumstat(numstat.stdout) : [];
  const rawPatch = patch.exitCode === 0 && !patch.outputLimitExceeded ? text(patch.stdout) : "";
  const evidence = patch.exitCode === 0 && !patch.outputLimitExceeded
    ? omitBinaryPatchBlocks(rawPatch, scope, binaryPaths, uncovered)
    : "";
  const digest = createHash("sha256")
    .update(String(names.exitCode)).update("\0").update(names.stdout)
    .update("\0").update(String(numstat.exitCode)).update("\0").update(numstat.stdout)
    .update("\0").update(String(patch.exitCode)).update("\0").update(patch.stdout)
    .update("\0").update(String(raw.exitCode)).update("\0").update(raw.stdout)
    .digest("hex");
  const submodulePaths = raw.exitCode === 0 ? parseSubmodulePaths(raw.stdout) : [];
  if (/^[+-]Subproject commit .*?-dirty$/mu.test(rawPatch)) {
    for (const path of submodulePaths) {
      uncovered.push({
        scope,
        detail: `dirty submodule content is not included in superproject evidence: ${JSON.stringify(safeEvidence(path, scope, uncovered))}`,
      });
    }
    if (submodulePaths.length === 0) {
      uncovered.push({
        scope,
        detail: "dirty submodule content is not included in superproject evidence; its path could not be verified",
      });
    }
  }
  return {
    snapshot: {
      files: rawFiles.map(path => safeEvidence(path, scope, uncovered)),
      evidence: patch.exitCode === 0 && !patch.outputLimitExceeded
        ? safeEvidence(evidence, scope, uncovered)
        : "",
    },
    digest,
    complete: results.every(result =>
      result.exitCode === 0 && !result.timedOut && !result.outputLimitExceeded),
  };
}

async function captureUntracked(
  repositoryPath: string,
  uncovered: ReviewUncovered[],
  signal?: AbortSignal,
): Promise<CapturedScope> {
  const listing = await git(repositoryPath, ["ls-files", "--others", "--exclude-standard", "-z", "--"], signal);
  const hasher = createHash("sha256").update(String(listing.exitCode)).update("\0").update(listing.stdout);
  if (listing.exitCode !== 0 || listing.timedOut || listing.outputLimitExceeded) {
    uncovered.push({
      scope: "untracked",
      detail: listing.outputLimitExceeded
        ? `untracked file list exceeded the ${MAX_GIT_OUTPUT_BYTES} byte Git output limit`
        : listing.timedOut
          ? `untracked file list exceeded the ${GIT_TIMEOUT_MS} ms Git timeout`
          : "untracked file list could not be captured",
    });
    return { snapshot: EMPTY_SCOPE, digest: hasher.digest("hex"), complete: false };
  }

  const listedPaths = nulFields(listing.stdout).sort((left, right) => left.localeCompare(right, "en"));
  const files = listedPaths.map(path => safeEvidence(path, "untracked", uncovered));
  const patches: string[] = [];
  let complete = true;
  let evidenceBytes = 0;
  let evidenceLimitExceeded = false;
  const appendEvidence = (evidence: string) => {
    const safe = safeEvidence(evidence, "untracked", uncovered);
    const bytes = Buffer.byteLength(safe, "utf8");
    if (evidenceLimitExceeded || evidenceBytes + bytes > MAX_UNTRACKED_EVIDENCE_BYTES) {
      complete = false;
      if (!evidenceLimitExceeded) {
        uncovered.push({
          scope: "untracked",
          detail: `untracked scope evidence exceeded the ${MAX_UNTRACKED_EVIDENCE_BYTES} byte capture limit; additional file contents were omitted`,
        });
      }
      evidenceLimitExceeded = true;
      return;
    }
    patches.push(safe);
    evidenceBytes += bytes;
  };
  for (const path of listedPaths) {
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    const fullPath = join(repositoryPath, path);
    let info: Stats;
    try {
      info = await lstat(fullPath);
    } catch {
      complete = false;
      uncovered.push({
        scope: "untracked",
        detail: `untracked path evidence is unavailable: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      continue;
    }
    const kind = info.isFile() ? "file" : info.isSymbolicLink() ? "link" : "unsupported";
    hasher.update("\0").update(path).update("\0").update(kind).update("\0").update(String(info.size));
    if (kind === "unsupported") {
      complete = false;
      uncovered.push({
        scope: "untracked",
        detail: `untracked path type is unsupported: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      continue;
    }
    if (kind === "link") {
      try {
        const target = await readlink(fullPath);
        hasher.update(target);
        appendEvidence(`[untracked symlink] ${safeEvidence(path, "untracked", uncovered)} -> ${
          safeEvidence(target, "untracked", uncovered)
        }\n`);
      } catch {
        complete = false;
        uncovered.push({
          scope: "untracked",
          detail: `untracked symlink target could not be captured: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
        });
      }
      continue;
    }
    if (info.size > MAX_UNTRACKED_FILE_BYTES) {
      complete = false;
      uncovered.push({
        scope: "untracked",
        detail: `untracked file exceeds the ${MAX_UNTRACKED_FILE_BYTES} byte capture limit: ${
          JSON.stringify(safeEvidence(path, "untracked", uncovered))
        }`,
      });
      continue;
    }

    const pathArgument = path === "-" ? "./-" : path;
    const numstat = await git(repositoryPath, [
      "diff", "--no-index", "--numstat", "-z", "--no-ext-diff", "--no-textconv", "--", "/dev/null", pathArgument,
    ], signal);
    hasher.update("\0numstat\0").update(String(numstat.exitCode)).update("\0").update(numstat.stdout);
    if ((numstat.exitCode !== 0 && numstat.exitCode !== 1)
      || (numstat.exitCode === 1 && numstat.stderrNonempty)
      || numstat.timedOut
      || numstat.outputLimitExceeded) {
      complete = false;
      uncovered.push({
        scope: "untracked",
        detail: numstat.outputLimitExceeded
          ? `untracked file statistics exceeded the ${MAX_GIT_OUTPUT_BYTES} byte Git output limit: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`
          : numstat.timedOut
            ? `untracked file statistics exceeded the ${GIT_TIMEOUT_MS} ms Git timeout: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`
            : `untracked file could not be classified safely: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      continue;
    }

    if (parseBinaryNumstat(numstat.stdout).length > 0) {
      uncovered.push({
        scope: "untracked",
        detail: `binary content omitted from review evidence: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      try {
        hasher.update("\0binary-sha256\0").update(await digestFileContents(fullPath, info, signal));
      } catch (error) {
        if (signal?.aborted) throw error;
        complete = false;
        uncovered.push({
          scope: "untracked",
          detail: `binary content could not be verified: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
        });
      }
      continue;
    }

    const patch = await git(repositoryPath, [
      "diff", "--no-index", "--no-ext-diff", "--no-textconv", "--", "/dev/null", pathArgument,
    ], signal);
    hasher.update("\0patch\0").update(String(patch.exitCode)).update("\0").update(patch.stdout);
    if ((patch.exitCode === 1 && patch.stderrNonempty)
      || (patch.exitCode !== 0 && patch.exitCode !== 1)
      || patch.timedOut
      || patch.outputLimitExceeded) {
      complete = false;
      uncovered.push({
        scope: "untracked",
        detail: patch.outputLimitExceeded
          ? `untracked file diff exceeded the ${MAX_GIT_OUTPUT_BYTES} byte Git output limit: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`
          : patch.timedOut
            ? `untracked file diff exceeded the ${GIT_TIMEOUT_MS} ms Git timeout: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`
            : `untracked file evidence could not be captured: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      continue;
    }
    const evidence = text(patch.stdout);
    if (/^Binary files .* differ$/mu.test(evidence)) {
      uncovered.push({
        scope: "untracked",
        detail: `binary content omitted from review evidence: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
      });
      try {
        hasher.update("\0binary-sha256\0").update(await digestFileContents(fullPath, info, signal));
      } catch (error) {
        if (signal?.aborted) throw error;
        complete = false;
        uncovered.push({
          scope: "untracked",
          detail: `binary content could not be verified: ${JSON.stringify(safeEvidence(path, "untracked", uncovered))}`,
        });
      }
    } else {
      const displayPath = safeEvidence(path, "untracked", uncovered);
      appendEvidence(safeEvidence(evidence, "untracked", uncovered)
        || `[untracked empty file] ${JSON.stringify(displayPath)}\n`);
    }
  }
  return {
    snapshot: { files, evidence: patches.join("") },
    digest: hasher.digest("hex"),
    complete,
  };
}

function unavailableProject(
  project: ReviewProjectInput,
  detail = "project path is unavailable",
): ReviewProjectSnapshot {
  return {
    name: project.name,
    requestedPath: project.path,
    baselineStable: false,
    captureAtomic: false,
    staged: EMPTY_SCOPE,
    unstaged: EMPTY_SCOPE,
    untracked: EMPTY_SCOPE,
    uncovered: [{ scope: "project", detail }],
  };
}

async function captureProject(
  project: ReviewProjectInput,
  signal?: AbortSignal,
): Promise<ReviewProjectSnapshot> {
  let projectPath: string;
  try {
    projectPath = await realpath(project.path);
  } catch {
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    return unavailableProject(project);
  }

  const rootResult = await git(projectPath, ["rev-parse", "--show-toplevel"], signal);
  const root = rootResult.exitCode === 0 && !rootResult.timedOut && !rootResult.outputLimitExceeded
    ? oneLine(rootResult.stdout)
    : undefined;
  if (!root) {
    return {
      ...unavailableProject(
        project,
        rootResult.outputLimitExceeded
          ? `Git root output exceeded the ${MAX_GIT_OUTPUT_BYTES} byte limit`
          : rootResult.timedOut
            ? `Git root lookup exceeded the ${GIT_TIMEOUT_MS} ms timeout`
            : "project path is not a readable Git worktree",
      ),
      path: projectPath,
    };
  }

  let repositoryPath: string;
  try {
    repositoryPath = await realpath(root);
  } catch {
    return {
      ...unavailableProject(project, "Git worktree root is unavailable"),
      path: projectPath,
    };
  }

  const uncovered: ReviewUncovered[] = [];
  const [headBeforeResult, statusBefore] = await settleAll([
    git(repositoryPath, ["rev-parse", "--verify", "HEAD"], signal),
    git(repositoryPath, [
      "status", "--porcelain=v2", "-z", "--untracked-files=all", "--ignore-submodules=none",
    ], signal),
  ]);
  const headBefore = headBeforeResult.exitCode === 0 && !headBeforeResult.timedOut
    ? oneLine(headBeforeResult.stdout)
    : undefined;
  if (!headBefore) {
    uncovered.push({
      scope: "head",
      detail: headBeforeResult.outputLimitExceeded
        ? `HEAD evidence exceeded the ${MAX_GIT_OUTPUT_BYTES} byte Git output limit`
        : headBeforeResult.timedOut
          ? `HEAD lookup exceeded the ${GIT_TIMEOUT_MS} ms Git timeout`
          : "HEAD could not be resolved; changes relative to HEAD were not captured",
    });
    return {
      name: project.name,
      requestedPath: project.path,
      path: projectPath,
      repositoryPath,
      baselineStable: false,
      captureAtomic: false,
      staged: EMPTY_SCOPE,
      unstaged: EMPTY_SCOPE,
      untracked: EMPTY_SCOPE,
      uncovered,
    };
  }
  if (statusBefore.exitCode !== 0 || statusBefore.timedOut || statusBefore.outputLimitExceeded) {
    uncovered.push({
      scope: "project",
      detail: statusBefore.outputLimitExceeded
        ? `Git worktree status exceeded the ${MAX_GIT_OUTPUT_BYTES} byte output limit`
        : statusBefore.timedOut
          ? `Git worktree status exceeded the ${GIT_TIMEOUT_MS} ms timeout`
          : "Git worktree status could not be verified",
    });
  }
  const dirtySubmodules = new Set(parseDirtySubmodules(statusBefore.stdout));
  const addDirtySubmodules = (paths: readonly string[]) => {
    for (const path of paths) dirtySubmodules.add(path);
  };
  const reportDirtySubmodules = () => {
    for (const path of dirtySubmodules) {
      const safePath = safeEvidence(path, "unstaged", uncovered);
      const detail = `dirty submodule contents are not included in review evidence: ${JSON.stringify(safePath)}`;
      if (!uncovered.some(item => item.scope === "project" && item.detail === detail)) {
        uncovered.push({ scope: "project", detail });
      }
    }
  };
  reportDirtySubmodules();

  const [stagedCapture, unstagedCapture, untrackedCapture] = await settleAll([
    captureDiffScope(repositoryPath, "staged", uncovered, signal),
    captureDiffScope(repositoryPath, "unstaged", uncovered, signal),
    captureUntracked(repositoryPath, uncovered, signal),
  ]);
  const verificationUncovered: ReviewUncovered[] = [];
  const [stagedVerification, unstagedVerification, untrackedVerification] = await settleAll([
    captureDiffScope(repositoryPath, "staged", verificationUncovered, signal),
    captureDiffScope(repositoryPath, "unstaged", verificationUncovered, signal),
    captureUntracked(repositoryPath, verificationUncovered, signal),
  ]);
  const statusAfter = await git(repositoryPath, [
    "status", "--porcelain=v2", "-z", "--untracked-files=all", "--ignore-submodules=none",
  ], signal);
  addDirtySubmodules(parseDirtySubmodules(statusAfter.stdout));
  reportDirtySubmodules();
  const headAfterResult = await git(repositoryPath, ["rev-parse", "--verify", "HEAD"], signal);
  const headAfter = headAfterResult.exitCode === 0 && !headAfterResult.timedOut
    ? oneLine(headAfterResult.stdout)
    : undefined;
  const statusStable = statusBefore.exitCode === 0 && statusAfter.exitCode === 0
    && !statusBefore.timedOut && !statusAfter.timedOut
    && !statusBefore.outputLimitExceeded && !statusAfter.outputLimitExceeded
    && statusBefore.stdout.equals(statusAfter.stdout);
  const evidenceStable = stagedCapture.digest === stagedVerification.digest
    && unstagedCapture.digest === unstagedVerification.digest
    && untrackedCapture.digest === untrackedVerification.digest;
  const captureComplete = stagedCapture.complete && unstagedCapture.complete && untrackedCapture.complete
    && stagedVerification.complete && unstagedVerification.complete && untrackedVerification.complete
    && statusBefore.exitCode === 0 && statusAfter.exitCode === 0
    && !statusBefore.timedOut && !statusAfter.timedOut
    && !statusBefore.outputLimitExceeded && !statusAfter.outputLimitExceeded;
  const baselineStable = Boolean(headAfter && headBefore === headAfter && statusStable
    && evidenceStable && captureComplete);
  if (!headAfter) {
    uncovered.push({
      scope: "head",
      detail: headAfterResult.timedOut
        ? `HEAD recheck exceeded the ${GIT_TIMEOUT_MS} ms Git timeout`
        : "HEAD could not be rechecked after capture",
    });
  } else if (headBefore !== headAfter) {
    uncovered.push({ scope: "head", detail: "HEAD changed during capture; scope evidence has a mixed baseline" });
  }
  if (!statusStable) {
    uncovered.push({ scope: "project", detail: "Git worktree status changed or could not be verified during capture" });
  }
  if (!captureComplete) {
    uncovered.push({ scope: "project", detail: "full content evidence could not be verified before and after capture" });
  } else if (!evidenceStable) {
    uncovered.push({ scope: "project", detail: "index, worktree, or untracked content changed during capture" });
  }
  return {
    name: project.name,
    requestedPath: project.path,
    path: projectPath,
    repositoryPath,
    headBefore,
    ...(headAfter === undefined ? {} : { headAfter }),
    baselineStable,
    captureAtomic: false,
    staged: stagedCapture.snapshot,
    unstaged: unstagedCapture.snapshot,
    untracked: untrackedCapture.snapshot,
    uncovered,
  };
}

export async function captureReviewSnapshot(
  projects: readonly ReviewProjectInput[],
  signal?: AbortSignal,
): Promise<ReviewSnapshot> {
  if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  const capturedAt = new Date().toISOString();
  const settled = await Promise.allSettled(projects.map(async project => {
    try {
      return await captureProject(project, signal);
    } catch (error) {
      if (signal?.aborted) throw error;
      return unavailableProject(project, "Git snapshot capture failed");
    }
  }));
  if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  const snapshots: ReviewProjectSnapshot[] = [];
  for (const result of settled) {
    if (result.status === "rejected") throw result.reason;
    snapshots.push(result.value);
  }
  return { capturedAt, projects: snapshots };
}
