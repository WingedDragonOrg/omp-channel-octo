import { constants, type Dirent } from "node:fs";
import { access, open, readdir, realpath, stat, type FileHandle } from "node:fs/promises";
import { basename, isAbsolute, resolve } from "node:path";

export interface ProjectSessionState {
  sessionId: string;
  sessionFile: string;
  cwd: string;
}

export interface ProjectSessionBinding {
  sessionId: string;
  cwd: string;
  sessionFile: string | null;
}

interface SessionHeader {
  id: string;
  cwd: string;
  previousSessionFiles: string[];
}

export interface LoadedSessionState {
  state: ProjectSessionState;
  previousSessionFiles: readonly string[];
}

export interface SessionSearchOptions {
  roots: readonly string[];
  maxDirectories?: number;
  maxFiles?: number;
}

const SESSION_HEADER_READ_BYTES = 64 * 1024;
const DEFAULT_MAX_DIRECTORIES = 4096;
const DEFAULT_MAX_FILES = 4096;

function sessionPathMatches(left: string, right: string): boolean {
  return resolve(left) === resolve(right);
}

async function readSessionHeader(sessionFile: string): Promise<SessionHeader> {
  let handle: FileHandle;
  try {
    handle = await open(sessionFile, "r");
  } catch {
    throw new Error(`session file is not readable: ${sessionFile}`);
  }
  try {
    const buffer = Buffer.allocUnsafe(SESSION_HEADER_READ_BYTES);
    const { bytesRead } = await handle.read(buffer, 0, SESSION_HEADER_READ_BYTES, 0);
    let offset = 0;
    while (offset < bytesRead) {
      const newline = buffer.indexOf(0x0a, offset);
      const hasNewline = newline >= 0 && newline < bytesRead;
      const end = hasNewline ? newline : bytesRead;
      const trimmed = buffer.subarray(offset, end).toString("utf8").trim();
      offset = hasNewline ? newline + 1 : bytesRead;
      if (trimmed.length === 0) continue;
      let value: unknown;
      try {
        value = JSON.parse(trimmed);
      } catch {
        throw new Error(`session header is not valid JSON: ${sessionFile}`);
      }
      if (value === null || typeof value !== "object" || Array.isArray(value) || !("type" in value)) {
        throw new Error(`session header is not an object: ${sessionFile}`);
      }
      if (value.type === "title") continue;
      if (
        value.type !== "session" ||
        !("id" in value) ||
        typeof value.id !== "string" ||
        value.id.length === 0
      ) {
        throw new Error(`session header has no valid identity: ${sessionFile}`);
      }
      if (!("cwd" in value) || typeof value.cwd !== "string" || value.cwd.length === 0) {
        throw new Error(`session header has no valid cwd: ${sessionFile}`);
      }
      const previousSessionFiles =
        "previousSessionFiles" in value && Array.isArray(value.previousSessionFiles)
          ? value.previousSessionFiles.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
          : [];
      return { id: value.id, cwd: value.cwd, previousSessionFiles };
    }
    throw new Error(`session file has no session header: ${sessionFile}`);
  } finally {
    await handle.close();
  }
}

async function canonicalExistingDirectory(cwd: string): Promise<string> {
  if (!isAbsolute(cwd)) throw new Error("session cwd must be absolute");
  try {
    const canonical = await realpath(cwd);
    const details = await stat(canonical);
    if (!details.isDirectory()) throw new Error("session cwd is not a directory");
    await access(canonical, constants.R_OK | constants.X_OK);
    return canonical;
  } catch {
    throw new Error(`session cwd is invalid: ${cwd}`);
  }
}

async function canonicalSessionFile(sessionFile: string): Promise<string> {
  if (!isAbsolute(sessionFile)) throw new Error("session file must be absolute");
  try {
    const canonical = await realpath(sessionFile);
    const details = await stat(canonical);
    if (!details.isFile()) throw new Error("session file is not a regular file");
    await access(canonical, constants.R_OK);
    return canonical;
  } catch {
    throw new Error(`session file is invalid: ${sessionFile}`);
  }
}

export async function loadValidatedSessionState(
  sessionFile: string,
  expectedId: string,
): Promise<LoadedSessionState> {
  const canonicalFile = await canonicalSessionFile(resolve(sessionFile));
  const header = await readSessionHeader(canonicalFile);
  if (expectedId.length === 0 || header.id !== expectedId) {
    throw new Error("OMP session state does not match its session header identity");
  }
  const cwd = await canonicalExistingDirectory(header.cwd);
  return {
    state: { sessionId: header.id, sessionFile: canonicalFile, cwd },
    previousSessionFiles: header.previousSessionFiles,
  };
}

async function verifiedState(
  state: ProjectSessionState,
  expectedCwd: string,
  expectedSessionId: string | undefined,
): Promise<ProjectSessionBinding> {
  if (!state || typeof state.sessionId !== "string" || state.sessionId.length === 0) {
    throw new Error("OMP state has no valid session ID");
  }
  if (expectedSessionId !== undefined && state.sessionId !== expectedSessionId) {
    throw new Error("OMP session identity changed unexpectedly");
  }
  const loaded = await loadValidatedSessionState(state.sessionFile, state.sessionId);
  const stateCwd = await canonicalExistingDirectory(state.cwd);
  const targetCwd = await canonicalExistingDirectory(expectedCwd);
  if (loaded.state.cwd !== stateCwd || stateCwd !== targetCwd) {
    throw new Error("OMP session cwd does not match the requested directory");
  }
  return {
    sessionId: loaded.state.sessionId,
    cwd: loaded.state.cwd,
    sessionFile: loaded.state.sessionFile,
  };
}
export async function verifyMovedSession(
  previous: ProjectSessionBinding,
  state: ProjectSessionState,
  targetCwd: string,
): Promise<ProjectSessionBinding> {
  return verifiedState(state, targetCwd, previous.sessionId);
}

export async function verifyFreshSession(
  previous: ProjectSessionBinding,
  state: ProjectSessionState,
  targetCwd: string,
): Promise<ProjectSessionBinding> {
  if (state.sessionId === previous.sessionId) {
    throw new Error("OMP fresh-session command did not create a new session");
  }
  return verifiedState(state, targetCwd, undefined);
}

async function collectOneLevelSessionFiles(
  root: string,
  targetBasename: string,
  state: {
    paths: string[];
    seenFiles: Set<string>;
    seenDirectories: Set<string>;
    maxDirectories: number;
    maxFiles: number;
    directoryCount: number;
    exceeded: boolean;
    failed: boolean;
  },
): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    state.failed = true;
    return;
  }
  const directTargetPresent = entries.some(
    entry => !entry.isSymbolicLink() && entry.isFile() && entry.name === targetBasename,
  );
  const artifactBasename = targetBasename.endsWith(".jsonl") ? targetBasename.slice(0, -".jsonl".length) : "";
  for (const entry of entries) {
    if (state.exceeded || state.failed) return;
    if (entry.isSymbolicLink()) continue;
    const path = resolve(root, entry.name);
    if (entry.isFile()) {
      if (entry.name !== targetBasename) continue;
      let canonical: string;
      try {
        canonical = await realpath(path);
      } catch {
        state.failed = true;
        return;
      }
      if (state.seenFiles.has(canonical)) continue;
      if (state.seenFiles.size >= state.maxFiles) {
        state.exceeded = true;
        return;
      }
      state.seenFiles.add(canonical);
      state.paths.push(canonical);
      continue;
    }
    if (
      !entry.isDirectory() ||
      entry.name.endsWith(".artifacts") ||
      (directTargetPresent && entry.name === artifactBasename)
    )
      continue;
    let canonicalDirectory: string;
    try {
      canonicalDirectory = await realpath(path);
    } catch {
      state.failed = true;
      return;
    }
    if (state.seenDirectories.has(canonicalDirectory)) continue;
    state.seenDirectories.add(canonicalDirectory);
    state.directoryCount++;
    if (state.directoryCount > state.maxDirectories) {
      state.exceeded = true;
      return;
    }
    let nestedEntries: Dirent[];
    try {
      nestedEntries = await readdir(canonicalDirectory, { withFileTypes: true });
    } catch {
      state.failed = true;
      return;
    }
    for (const nestedEntry of nestedEntries) {
      if (state.exceeded || state.failed) return;
      if (nestedEntry.isSymbolicLink() || !nestedEntry.isFile() || nestedEntry.name !== targetBasename) continue;
      const nestedPath = resolve(canonicalDirectory, nestedEntry.name);
      let canonicalFile: string;
      try {
        canonicalFile = await realpath(nestedPath);
      } catch {
        state.failed = true;
        return;
      }
      if (state.seenFiles.has(canonicalFile)) continue;
      if (state.seenFiles.size >= state.maxFiles) {
        state.exceeded = true;
        return;
      }
      state.seenFiles.add(canonicalFile);
      state.paths.push(canonicalFile);
    }
  }
}

function candidateMayMatch(
  candidatePath: string,
  oldSessionFile: string,
  loaded: LoadedSessionState,
): boolean {
  if (basename(candidatePath) !== basename(oldSessionFile)) return false;
  return loaded.previousSessionFiles.some(previous => sessionPathMatches(previous, oldSessionFile));
}

export async function findMovedSession(
  binding: ProjectSessionBinding,
  options: SessionSearchOptions,
): Promise<ProjectSessionBinding | undefined> {
  if (!binding.sessionFile || binding.sessionId.length === 0 || options.roots.length === 0) return undefined;
  const oldSessionFile = resolve(binding.sessionFile);
  const maxDirectories =
    Number.isSafeInteger(options.maxDirectories) && options.maxDirectories! >= 0
      ? options.maxDirectories!
      : DEFAULT_MAX_DIRECTORIES;
  const maxFiles =
    Number.isSafeInteger(options.maxFiles) && options.maxFiles! > 0 ? options.maxFiles! : DEFAULT_MAX_FILES;
  const scan = {
    paths: [] as string[],
    seenFiles: new Set<string>(),
    seenDirectories: new Set<string>(),
    maxDirectories,
    maxFiles,
    directoryCount: 0,
    exceeded: false,
    failed: false,
  };
  let scannedRoot = false;
  const visitedRoots = new Set<string>();
  for (const root of options.roots) {
    if (typeof root !== "string" || root.length === 0) continue;
    let canonicalRoot: string;
    try {
      canonicalRoot = await realpath(root);
      const details = await stat(canonicalRoot);
      if (!details.isDirectory()) continue;
    } catch {
      continue;
    }
    if (visitedRoots.has(canonicalRoot)) continue;
    visitedRoots.add(canonicalRoot);
    scan.seenDirectories.add(canonicalRoot);
    scannedRoot = true;
    await collectOneLevelSessionFiles(canonicalRoot, basename(oldSessionFile), scan);
    if (scan.exceeded || scan.failed) return undefined;
  }
  if (!scannedRoot) return undefined;
  const matches: ProjectSessionBinding[] = [];
  for (const candidatePath of scan.paths) {
    let loaded: LoadedSessionState;
    try {
      loaded = await loadValidatedSessionState(candidatePath, binding.sessionId);
    } catch {
      continue;
    }
    if (!candidateMayMatch(candidatePath, oldSessionFile, loaded)) continue;
    matches.push({
      sessionId: loaded.state.sessionId,
      cwd: loaded.state.cwd,
      sessionFile: loaded.state.sessionFile,
    });
    if (matches.length > 1) return undefined;
  }
  return matches[0];
}
