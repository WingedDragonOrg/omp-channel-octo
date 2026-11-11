import { Database } from "bun:sqlite";
import type { FileLinkResolverConfig } from "../contracts.ts";
import type { TaskFileLinkContext } from "./store-records.ts";
import { asString, query, run, transaction } from "./store-schema.ts";

export function getTaskFileLinkContext(db: Database, taskId: string): TaskFileLinkContext | null {
  const row = query(db,
    "SELECT task_id, cwd, resolvers_json FROM task_file_link_context WHERE task_id = ?",
    [taskId],
  )[0];
  if (!row) return null;
  return {
    taskId: asString(row.task_id),
    cwd: asString(row.cwd),
    resolvers: decodeResolvers(asString(row.resolvers_json)),
  };
}

/** Inserts the first task-start snapshot and returns that immutable value on every later call. */
export function ensureTaskFileLinkContext(
  db: Database,
  context: TaskFileLinkContext,
): TaskFileLinkContext {
  transaction(db, () => {
    run(db,
      `INSERT OR IGNORE INTO task_file_link_context(task_id, cwd, resolvers_json)
       VALUES(?, ?, ?)`,
      [context.taskId, context.cwd, JSON.stringify(context.resolvers)],
    );
  });
  const persisted = getTaskFileLinkContext(db, context.taskId);
  if (persisted === null) throw new Error(`task file-link context missing after insert: ${context.taskId}`);
  return persisted;
}

function decodeResolvers(serialized: string): FileLinkResolverConfig[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(serialized);
  } catch (error) {
    throw new Error("Invalid persisted task file-link resolver configuration JSON", { cause: error });
  }
  if (!Array.isArray(parsed) || !parsed.every(isResolverConfig)) {
    throw new Error("Invalid persisted task file-link resolver configuration shape");
  }
  return parsed;
}

function isResolverConfig(value: unknown): value is FileLinkResolverConfig {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (!("type" in value) || value.type !== "zed" || !("mode" in value)) return false;
  if (value.mode !== "local" && value.mode !== "ssh") return false;
  const fields = value as Record<string, unknown>;
  if (Object.keys(fields).some((key) => !["type", "mode", "hostname", "username", "port"].includes(key))) {
    return false;
  }
  if ("hostname" in fields && typeof fields.hostname !== "string") return false;
  if ("username" in fields && typeof fields.username !== "string") return false;
  if (
    "port" in fields
    && (typeof fields.port !== "number" || !Number.isInteger(fields.port) || fields.port < 1 || fields.port > 65_535)
  ) {
    return false;
  }
  if (fields.mode === "ssh") {
    return typeof fields.hostname === "string" && fields.hostname.length > 0;
  }
  return true;
}
