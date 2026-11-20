import { Database } from "bun:sqlite";
import { chmod, lstat, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, resolve } from "node:path";
import { isRecord } from "./type-guards.ts";

export interface InputHistoryEntry {
  text: string;
  mentions?: unknown[];
}

export interface InputHistorySnapshot {
  limit: number;
  entries: InputHistoryEntry[];
}

const defaultDatabasePath = resolve(homedir(), ".config", "octobuddy-enhancer", "input-history.sqlite");
const defaultLimit = 100;
const schema = `
  CREATE TABLE IF NOT EXISTS input_history_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    history_limit INTEGER NOT NULL CHECK (history_limit > 0)
  );
  CREATE TABLE IF NOT EXISTS input_history_chats (
    chat_key TEXT PRIMARY KEY NOT NULL,
    entries_json TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS input_history_imports (
    chat_key TEXT PRIMARY KEY NOT NULL
  );
`;

function isErrno(error: unknown, code: string): boolean {
  return error !== null && typeof error === "object" && "code" in error && error.code === code;
}

function validateChatKey(chatKey: unknown): string {
  if (typeof chatKey !== "string") throw new Error("invalid input history chat key");
  try {
    const parsed: unknown = JSON.parse(chatKey);
    if (!Array.isArray(parsed) || parsed.length !== 2
      || (parsed[0] !== 1 && parsed[0] !== 2 && parsed[0] !== 5)
      || typeof parsed[1] !== "string" || parsed[1].length === 0
      || JSON.stringify(parsed) !== chatKey) {
      throw new Error("invalid input history chat key");
    }
  } catch {
    throw new Error("invalid input history chat key");
  }
  return chatKey;
}


function validateLimit(limit: unknown): number {
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1) {
    throw new Error("invalid input history limit");
  }
  return limit;
}

function validateEntry(value: unknown): InputHistoryEntry {
  if (!isRecord(value) || typeof value.text !== "string") {
    throw new Error("invalid input history entry");
  }
  if (value.mentions !== undefined && !Array.isArray(value.mentions)) {
    throw new Error("invalid input history mentions");
  }
  return {
    text: value.text,
    ...(value.mentions === undefined ? {} : { mentions: value.mentions }),
  };
}

function validateEntries(entries: unknown): InputHistoryEntry[] {
  if (!Array.isArray(entries)) throw new Error("invalid input history entries");
  return entries.map(validateEntry);
}

function decodeEntries(encoded: string): InputHistoryEntry[] {
  const parsed: unknown = JSON.parse(encoded);
  return validateEntries(parsed);
}

function readLimit(database: Database): number | undefined {
  const value: unknown = database.query("SELECT history_limit FROM input_history_config WHERE id = 1").get();
  if (value === null) return undefined;
  if (!isRecord(value) || typeof value.history_limit !== "number") throw new Error("invalid input history database");
  return validateLimit(value.history_limit);
}

function readEntries(database: Database, chatKey: string): InputHistoryEntry[] {
  const value: unknown = database.query(
    "SELECT entries_json FROM input_history_chats WHERE chat_key = ?",
  ).get(chatKey);
  if (value === null) return [];
  if (!isRecord(value) || typeof value.entries_json !== "string") throw new Error("invalid input history database");
  return decodeEntries(value.entries_json);
}

function writeEntries(database: Database, chatKey: string, entries: InputHistoryEntry[]): void {
  let encoded: string;
  try {
    encoded = JSON.stringify(entries);
  } catch {
    throw new Error("invalid input history entries");
  }
  database.query(`
    INSERT INTO input_history_chats (chat_key, entries_json) VALUES (?, ?)
    ON CONFLICT(chat_key) DO UPDATE SET entries_json = excluded.entries_json
  `).run(chatKey, encoded);
}

function transaction<T>(database: Database, operation: () => T): T {
  database.exec("BEGIN IMMEDIATE");
  try {
    const result = operation();
    database.exec("COMMIT");
    return result;
  } catch (error) {
    try { database.exec("ROLLBACK"); }
    catch { /* Keep the operation failure; database errors are exposed only as a fixed message. */ }
    throw error;
  }
}

async function openPrivateDatabase(path: string): Promise<Database> {
  const directory = dirname(path);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const directoryInfo = await lstat(directory);
  if (!directoryInfo.isDirectory() || directoryInfo.isSymbolicLink()) throw new Error("input history storage is unavailable");
  await chmod(directory, 0o700);

  try {
    const fileInfo = await lstat(path);
    if (!fileInfo.isFile() || fileInfo.isSymbolicLink()) throw new Error("input history storage is unavailable");
    await chmod(path, 0o600);
  } catch (error) {
    if (!isErrno(error, "ENOENT")) throw error;
  }

  const database = new Database(path);
  try {
    await chmod(path, 0o600);
    database.exec(schema);
    database.exec("PRAGMA synchronous = FULL");
    return database;
  } catch (error) {
    database.close();
    throw error;
  }
}

export class InputHistoryStore {
  readonly #databasePath: string;
  #databasePromise: Promise<Database> | undefined;
  #closed = false;

  constructor(databasePath = defaultDatabasePath) {
    this.#databasePath = resolve(databasePath);
  }

  async loadInputHistory(chatKey: string): Promise<InputHistorySnapshot> {
    const key = validateChatKey(chatKey);
    return this.withDatabase((database) => ({
      limit: readLimit(database) ?? defaultLimit,
      entries: readEntries(database, key),
    }));
  }

  async appendInputHistory(chatKey: string, entry: unknown): Promise<InputHistorySnapshot> {
    const key = validateChatKey(chatKey);
    const storedEntry = validateEntry(entry);
    return this.withDatabase((database) => transaction(database, () => {
      const limit = readLimit(database) ?? defaultLimit;
      const entries = [storedEntry, ...readEntries(database, key)].slice(0, limit);
      writeEntries(database, key, entries);
      return { limit, entries };
    }));
  }

  async setInputHistoryLimit(limit: number): Promise<number> {
    const nextLimit = validateLimit(limit);
    return this.withDatabase((database) => transaction(database, () => {
      database.query(`
        INSERT INTO input_history_config (id, history_limit) VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET history_limit = excluded.history_limit
      `).run(nextLimit);
      const rows: unknown = database.query(
        "SELECT chat_key, entries_json FROM input_history_chats",
      ).all();
      if (!Array.isArray(rows)) throw new Error("invalid input history database");
      for (const row of rows) {
        if (!isRecord(row) || typeof row.chat_key !== "string" || typeof row.entries_json !== "string") {
          throw new Error("invalid input history database");
        }
        writeEntries(database, row.chat_key, decodeEntries(row.entries_json).slice(0, nextLimit));
      }
      return nextLimit;
    }));
  }

  async importInputHistory(chatKey: string, entries: unknown, limit: number): Promise<InputHistorySnapshot> {
    const key = validateChatKey(chatKey);
    const importedEntries = validateEntries(entries);
    const importedLimit = validateLimit(limit);
    return this.withDatabase((database) => transaction(database, () => {
      const imported: unknown = database.query(
        "SELECT chat_key FROM input_history_imports WHERE chat_key = ?",
      ).get(key);
      if (imported !== null) {
        return { limit: readLimit(database) ?? defaultLimit, entries: readEntries(database, key) };
      }

      const storedLimit = readLimit(database);
      const limit = storedLimit ?? importedLimit;
      if (storedLimit === undefined) {
        database.query("INSERT INTO input_history_config (id, history_limit) VALUES (1, ?)").run(limit);
      }
      const merged = [...readEntries(database, key), ...importedEntries].slice(0, limit);
      writeEntries(database, key, merged);
      database.query("INSERT INTO input_history_imports (chat_key) VALUES (?)").run(key);
      return { limit, entries: merged };
    }));
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const opening = this.#databasePromise;
    if (!opening) return;
    const database = await opening.catch(() => undefined);
    database?.close();
  }

  private async withDatabase<T>(operation: (database: Database) => T): Promise<T> {
    try {
      if (this.#closed) throw new Error("input history storage is closed");
      const database = await this.database();
      if (this.#closed) throw new Error("input history storage is closed");
      return operation(database);
    } catch {
      throw new Error("input history storage operation failed");
    }
  }

  private database(): Promise<Database> {
    if (this.#closed) return Promise.reject(new Error("input history storage is closed"));
    if (!this.#databasePromise) {
      this.#databasePromise = openPrivateDatabase(this.#databasePath).catch(() => {
        throw new Error("input history storage is unavailable");
      });
    }
    return this.#databasePromise;
  }
}

