import { realpath } from "node:fs/promises";
import { basename, isAbsolute } from "node:path";
import type { ThreadWorkspaceRecord } from "../store/store-records.ts";
import { runCommand, type CommandResult } from "./worktrunk.ts";

export type RepositoryChangeState = "git" | "non-git" | "unavailable";
export type RepositoryChangeBaseline = "HEAD" | "empty-tree" | null;

export interface RepositoryChange {
  name: string;
  path: string | null;
  state: RepositoryChangeState;
  baseline: RepositoryChangeBaseline;
  added: number;
  deleted: number;
  binaryFiles: number;
  untrackedFiles: number;
}

const GIT_STATUS_TOTAL_BUDGET_MS = 8_000;
const MAX_GIT_OUTPUT_CHARS = 256 * 1024;

interface GitTarget {
  name: string;
  path: string | null;
  requireExactRoot: boolean;
}

interface ParsedNumstat {
  added: number;
  deleted: number;
  binaryFiles: number;
}

function unavailable(target: GitTarget): RepositoryChange {
  return {
    name: target.name,
    path: target.path,
    state: "unavailable",
    baseline: null,
    added: 0,
    deleted: 0,
    binaryFiles: 0,
    untrackedFiles: 0,
  };
}

function nonGit(target: GitTarget): RepositoryChange {
  return {
    name: target.name,
    path: target.path,
    state: "non-git",
    baseline: null,
    added: 0,
    deleted: 0,
    binaryFiles: 0,
    untrackedFiles: 0,
  };
}

function outputWasCapped(result: CommandResult, maxOutputChars: number): boolean {
  return result.stdout.length >= maxOutputChars || result.stderr.length >= maxOutputChars;
}

function completedSuccessfully(
  result: CommandResult | null,
  deadline: number,
  maxOutputChars = MAX_GIT_OUTPUT_CHARS,
): result is CommandResult {
  return (
    result !== null &&
    !result.timedOut &&
    !result.aborted &&
    result.exitCode === 0 &&
    Date.now() < deadline &&
    !outputWasCapped(result, maxOutputChars)
  );
}

async function runGit(
  path: string,
  args: string[],
  signal: AbortSignal,
  deadline: number,
  maxOutputChars = MAX_GIT_OUTPUT_CHARS,
): Promise<CommandResult | null> {
  const timeoutMs = Math.floor(deadline - Date.now());
  if (timeoutMs <= 0 || signal.aborted) return null;
  try {
    return await runCommand(
      ["git", "-c", "core.fsmonitor=false", "-c", "diff.relative=false", "--no-optional-locks", "-C", path, ...args],
      { timeoutMs, signal, maxOutputChars },
    );
  } catch {
    return null;
  }
}

function parseNumstat(stdout: string): ParsedNumstat | null {
  if (stdout.length === 0) return { added: 0, deleted: 0, binaryFiles: 0 };
  const fields = stdout.split("\0");
  if (fields.at(-1) !== "") return null;

  let added = 0;
  let deleted = 0;
  let binaryFiles = 0;
  for (let index = 0; index < fields.length - 1;) {
    const record = fields[index++]!;
    const firstTab = record.indexOf("\t");
    const secondTab = firstTab < 0 ? -1 : record.indexOf("\t", firstTab + 1);
    if (firstTab < 0 || secondTab < 0) return null;

    const addedText = record.slice(0, firstTab);
    const deletedText = record.slice(firstTab + 1, secondTab);
    const path = record.slice(secondTab + 1);
    if (path.length === 0) {
      if (index + 1 >= fields.length - 1 || fields[index]!.length === 0 || fields[index + 1]!.length === 0) {
        return null;
      }
      index += 2;
    }

    if (addedText === "-" || deletedText === "-") {
      if (addedText !== "-" || deletedText !== "-") return null;
      binaryFiles += 1;
      continue;
    }
    if (!/^\d+$/u.test(addedText) || !/^\d+$/u.test(deletedText)) return null;
    const fileAdded = Number(addedText);
    const fileDeleted = Number(deletedText);
    if (!Number.isSafeInteger(fileAdded) || !Number.isSafeInteger(fileDeleted)) return null;
    added += fileAdded;
    deleted += fileDeleted;
    if (!Number.isSafeInteger(added) || !Number.isSafeInteger(deleted)) return null;
  }
  return { added, deleted, binaryFiles };
}

function countNulDelimitedPaths(stdout: string): number | null {
  if (stdout.length === 0) return 0;
  if (!stdout.endsWith("\0")) return null;
  return stdout.split("\0").length - 1;
}
async function sameCanonicalPath(left: string, right: string, deadline: number): Promise<boolean> {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) return false;
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<boolean>((resolve) => {
    timer = setTimeout(() => resolve(false), remainingMs);
  });
  try {
    return await Promise.race([
      Promise.all([realpath(left), realpath(right)]).then(
        ([canonicalLeft, canonicalRight]) => canonicalLeft === canonicalRight,
        () => false,
      ),
      timeout,
    ]);
  } finally {
    clearTimeout(timer!);
  }
}

async function inspectRepository(
  target: GitTarget,
  signal: AbortSignal,
  deadline: number,
): Promise<RepositoryChange> {
  const { path } = target;
  if (path === null || !isAbsolute(path) || signal.aborted) return unavailable(target);

  const root = await runGit(path, ["rev-parse", "--show-toplevel"], signal, deadline);
  if (root === null || root.timedOut || root.aborted || Date.now() >= deadline || outputWasCapped(root, MAX_GIT_OUTPUT_CHARS)) {
    return unavailable(target);
  }
  if (root.exitCode !== 0) {
    return root.stderr.toLowerCase().includes("not a git repository") ? nonGit(target) : unavailable(target);
  }
  const rootPath = root.stdout.endsWith("\r\n")
    ? root.stdout.slice(0, -2)
    : root.stdout.endsWith("\n")
      ? root.stdout.slice(0, -1)
      : "";
  if (!isAbsolute(rootPath)) return unavailable(target);
  if (target.requireExactRoot && !(await sameCanonicalPath(path, rootPath, deadline))) {
    return unavailable(target);
  }

  const head = await runGit(rootPath, ["rev-parse", "--verify", "--quiet", "HEAD^{commit}"], signal, deadline);
  if (head === null || head.timedOut || head.aborted || Date.now() >= deadline || outputWasCapped(head, MAX_GIT_OUTPUT_CHARS)) {
    return unavailable(target);
  }
  let baseline: Exclude<RepositoryChangeBaseline, null>;
  if (head.exitCode === 0 && /^[0-9a-f]{40,64}$/iu.test(head.stdout.trim())) {
    baseline = "HEAD";
  } else if (head.exitCode === 1 && head.stdout.trim().length === 0) {
    baseline = "empty-tree";
  } else {
    return unavailable(target);
  }

  let baseRef = "HEAD";
  if (baseline === "empty-tree") {
    const emptyTree = await runGit(rootPath, ["hash-object", "-t", "tree", "--stdin"], signal, deadline);
    if (!completedSuccessfully(emptyTree, deadline) || !/^[0-9a-f]{40,64}$/iu.test(emptyTree.stdout.trim())) {
      return unavailable(target);
    }
    baseRef = emptyTree.stdout.trim();
  }

  const [unmerged, numstat, untracked] = await Promise.all([
    runGit(rootPath, ["ls-files", "-u", "-z"], signal, deadline, 1),
    runGit(
      rootPath,
      ["diff", "--numstat", "-z", "--no-ext-diff", "--no-textconv", "--find-renames", baseRef, "--"],
      signal,
      deadline,
    ),
    runGit(rootPath, ["ls-files", "--others", "--exclude-standard", "-z"], signal, deadline),
  ]);

  if (
    !completedSuccessfully(unmerged, deadline, 1) ||
    !completedSuccessfully(numstat, deadline) ||
    !completedSuccessfully(untracked, deadline)
  ) {
    return unavailable(target);
  }
  if (unmerged.stdout.length > 0) return unavailable(target);

  const parsed = parseNumstat(numstat.stdout);
  const untrackedFiles = countNulDelimitedPaths(untracked.stdout);
  if (parsed === null || untrackedFiles === null) return unavailable(target);
  return {
    name: target.name,
    path: target.path,
    state: "git",
    baseline,
    ...parsed,
    untrackedFiles,
  };
}

export async function readWorkspaceChanges(
  cwd: string | null,
  workspace: ThreadWorkspaceRecord | null,
  signal: AbortSignal,
): Promise<RepositoryChange[]> {
  const deadline = Date.now() + GIT_STATUS_TOTAL_BUDGET_MS;
  const targets: GitTarget[] = workspace === null
    ? [{ name: cwd === null ? "当前工作区" : basename(cwd), path: cwd, requireExactRoot: false }]
    : workspace.projects.map((project) => ({ name: project.name, path: project.path, requireExactRoot: true }));
  return Promise.all(targets.map((target) => inspectRepository(target, signal, deadline)));
}
