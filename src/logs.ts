/**
 * Service-owned log files under `~/.oco/logs`.
 *
 * The supervisor's own capture is a by-product: it lives in the supervisor's directory, has no
 * rotation, and disappears if the supervisor changes. The channel therefore writes its own log,
 * one file per local day, and `oco logs` reads exactly that.
 */
import { createWriteStream, type WriteStream } from "node:fs";
import { mkdir, open, readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { format } from "node:util";
import { ocoLogsDir } from "./config.ts";

export const LOG_RETENTION_MS = 14 * 24 * 60 * 60 * 1000;
export const DEFAULT_LOG_TAIL_LINES = 200;
export const DEFAULT_LOG_POLL_MS = 500;
const LOG_FILE_PREFIX = "channel-";
const LOG_FILE_SUFFIX = ".log";
const TAIL_READ_BYTES = 1024 * 1024;
const TEE_METHODS = ["log", "info", "warn", "error"] as const;

function pad(value: number): string {
  return value < 10 ? `0${value}` : String(value);
}

/** Local calendar date of `date`, as used in log file names. */
export function logDateStamp(date: Date): string {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

export function logFileName(date: Date): string {
  return `${LOG_FILE_PREFIX}${logDateStamp(date)}${LOG_FILE_SUFFIX}`;
}

export function logFilePath(date: Date, directory = ocoLogsDir()): string {
  return join(directory, logFileName(date));
}

export async function ensureLogsDirectory(directory = ocoLogsDir()): Promise<string> {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  return directory;
}

/** Removes log files whose last write is older than the retention window. Returns what it removed. */
export async function pruneLogFiles(
  options: { directory?: string; now?: Date; retentionMs?: number } = {},
): Promise<string[]> {
  const directory = options.directory ?? ocoLogsDir();
  const now = options.now ?? new Date();
  const retentionMs = options.retentionMs ?? LOG_RETENTION_MS;
  let entries: string[];
  try {
    entries = await readdir(directory);
  } catch {
    return [];
  }
  const removed: string[] = [];
  for (const entry of entries) {
    if (!entry.startsWith(LOG_FILE_PREFIX) || !entry.endsWith(LOG_FILE_SUFFIX)) continue;
    const path = join(directory, entry);
    try {
      const info = await stat(path);
      if (!info.isFile()) continue;
      if (now.getTime() - info.mtimeMs <= retentionMs) continue;
      await rm(path, { force: true });
      removed.push(path);
    } catch {
      // A file that vanished or resists removal must not keep the sweep from the rest.
    }
  }
  return removed;
}

/** Newest log file by name (the file name is the local date), or undefined when none exists. */
export async function newestLogFile(directory = ocoLogsDir()): Promise<string | undefined> {
  let entries: string[];
  try {
    entries = await readdir(directory);
  } catch {
    return undefined;
  }
  const names = entries
    .filter((entry) => entry.startsWith(LOG_FILE_PREFIX) && entry.endsWith(LOG_FILE_SUFFIX))
    .sort();
  const last = names.at(-1);
  return last === undefined ? undefined : join(directory, last);
}

export interface LogTee {
  /** Writes the trailing partial line and stops writing to the file. */
  close(): Promise<void>;
}

/**
 * Mirrors `console` output into the day's log file, one prefixed line per line, while the
 * terminal keeps the raw text. Only `console` is wrapped: `process.stdout` also carries the OMP
 * worker's protocol traffic, which must never be rewritten.
 */
export function installLogTee(options: { directory?: string; now?: () => Date } = {}): LogTee {
  const directory = options.directory ?? ocoLogsDir();
  const now = options.now ?? (() => new Date());
  const originals = TEE_METHODS.map((method) => [method, console[method]] as const);
  const reportTo = console.error.bind(console);
  let dateStamp: string | undefined;
  let stream: WriteStream | undefined;
  let failureReported = false;

  const reportFailure = (error: unknown): void => {
    if (failureReported) return;
    failureReported = true;
    reportTo(`[logs] 无法写入日志文件 ${directory}：${error instanceof Error ? error.message : String(error)}`);
  };

  const writeLine = (timestamp: Date, line: string): void => {
    const stamp = logDateStamp(timestamp);
    try {
      if (stamp !== dateStamp || !stream) {
        stream?.end();
        stream = createWriteStream(logFilePath(timestamp, directory), { flags: "a", mode: 0o600 });
        stream.on("error", reportFailure);
        dateStamp = stamp;
      }
      stream.write(`[${stamp} ${pad(timestamp.getHours())}:${pad(timestamp.getMinutes())}:${pad(timestamp.getSeconds())}] ${line}\n`);
    } catch (error) {
      reportFailure(error);
    }
  };

  for (const [method, original] of originals) {
    const bound = original.bind(console);
    console[method] = (...args: unknown[]): void => {
      bound(...args);
      // One console call is one message: every line it prints carries that call's timestamp, so a
      // later call can never re-date it and a date rollover cannot move finished lines.
      const timestamp = now();
      const lines = format(...args).split("\n");
      if (lines.at(-1) === "") lines.pop();
      for (const line of lines) writeLine(timestamp, line);
    };
  }

  return {
    async close(): Promise<void> {
      for (const [method, original] of originals) console[method] = original;
      const active = stream;
      stream = undefined;
      if (active) {
        const { promise, resolve } = Promise.withResolvers<void>();
        active.end(() => resolve());
        await promise;
      }
    },
  };
}

/** Last `lines` lines of a log file; reads a bounded tail instead of the whole file. */
export async function readLogTail(file: string, lines = DEFAULT_LOG_TAIL_LINES): Promise<string> {
  const wanted = Math.max(1, Math.trunc(lines));
  const handle = await open(file, "r");
  try {
    const size = (await handle.stat()).size;
    const length = Math.min(size, TAIL_READ_BYTES);
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, size - length);
    const text = buffer.toString("utf8");
    const all = text.split("\n");
    // A bounded read starts mid-line; that first fragment is not a whole line.
    if (size > length) all.shift();
    if (all.at(-1) === "") all.pop();
    return all.slice(-wanted).join("\n");
  } finally {
    await handle.close();
  }
}

export interface LogFollowOptions {
  readonly write: (text: string) => void;
  readonly signal: AbortSignal;
  readonly directory?: string;
  readonly pollMs?: number;
  /** Called once the baseline offset is established, so a writer can start appending. */
  readonly onReady?: () => void;
}

/**
 * Follows the newest log file: prints what is appended, and switches to the new file when the
 * local date rolls over. Returns when the caller aborts (Ctrl-C).
 */
export async function followLog(options: LogFollowOptions): Promise<void> {
  const directory = options.directory ?? ocoLogsDir();
  const pollMs = options.pollMs ?? DEFAULT_LOG_POLL_MS;
  let current = await newestLogFile(directory);
  let offset = 0;
  if (current) {
    try {
      offset = (await stat(current)).size;
    } catch {
      offset = 0;
    }
  }
  options.onReady?.();
  while (!options.signal.aborted) {
    await Bun.sleep(pollMs);
    if (options.signal.aborted) return;
    const newest = await newestLogFile(directory);
    if (newest !== current && newest !== undefined) {
      current = newest;
      offset = 0;
    }
    if (!current) continue;
    let size: number;
    try {
      size = (await stat(current)).size;
    } catch {
      current = undefined;
      offset = 0;
      continue;
    }
    if (size < offset) offset = 0;
    if (size === offset) continue;
    const length = size - offset;
    const buffer = Buffer.alloc(length);
    const handle = await open(current, "r");
    try {
      await handle.read(buffer, 0, length, offset);
    } finally {
      await handle.close();
    }
    offset = size;
    options.write(buffer.toString("utf8"));
  }
}
