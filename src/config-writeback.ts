import { randomUUID } from "node:crypto";
import { chmod, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { AllowlistWriteback } from "./contracts.ts";

/**
 * Keeps the config file's `allowedGroupIds` in step with a chat the owner enabled at runtime.
 *
 * The file stays the operator's document: it is re-read at write time (never rewritten from the
 * startup snapshot), only that one array is appended to, and the write is an atomic
 * same-directory rename that keeps the original file mode. A failure is the caller's to report -
 * the durable grant in the state store is what actually admits the chat.
 */
export function createAllowlistWriteback(path: string): AllowlistWriteback {
  // Grants arrive from one process but not from one turn: serialize read-modify-write, so two
  // passphrases answered at once cannot overwrite each other's entry.
  let tail: Promise<void> = Promise.resolve();
  return {
    append(groupId: string): Promise<void> {
      const next = tail.then(() => appendAllowedGroupId(path, groupId));
      tail = next.catch(() => {});
      return next;
    },
  };
}

export async function appendAllowedGroupId(path: string, groupId: string): Promise<void> {
  const info = await stat(path);
  if (!info.isFile()) throw new Error("配置文件不是普通文件");
  const document: unknown = JSON.parse(await readFile(path, "utf8"));
  if (document === null || typeof document !== "object" || Array.isArray(document)) {
    throw new Error("配置文件根值不是 JSON 对象");
  }
  const record = document as Record<string, unknown>;
  const current = record.allowedGroupIds;
  if (current !== undefined && !Array.isArray(current)) {
    throw new Error("配置文件的 allowedGroupIds 不是数组");
  }
  const ids: unknown[] = current ?? [];
  if (ids.includes(groupId)) return;
  record.allowedGroupIds = [...ids, groupId];

  const mode = info.mode & 0o777;
  const temporary = join(dirname(path), `.${basename(path)}.tmp-${process.pid}-${randomUUID().slice(0, 8)}`);
  try {
    // The creation mode is the operator's mode with umask applied (never wider than the original);
    // the chmod then restores it exactly, so a private token-bearing file stays private.
    await writeFile(temporary, `${JSON.stringify(record, null, 2)}\n`, {
      encoding: "utf8",
      flag: "wx",
      mode,
    });
    await chmod(temporary, mode);
    await rename(temporary, path);
  } catch (error) {
    await unlink(temporary).catch(() => {});
    throw error;
  }
}
