import type { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { ThreadReference, ThreadReferences } from "../references/types.ts";
import { query, run, transaction } from "./store-schema.ts";

function validThreadIdentity(botUid: string, target: ChatTarget): boolean {
  return botUid.length > 0 && target.channelType === 5 && target.channelId.length > 0;
}

export function getThreadReferences(db: Database, botUid: string, target: ChatTarget): ThreadReferences {
  const references: ThreadReferences = { mr: [], pr: [] };
  if (!validThreadIdentity(botUid, target)) return references;

  const rows = query(
    db,
    `SELECT kind, canonical_url FROM thread_references
     WHERE bot_uid = ? AND channel_id = ? ORDER BY canonical_url`,
    [botUid, target.channelId],
  );
  for (const row of rows) {
    if (row.kind === "mr") references.mr.push(String(row.canonical_url));
    if (row.kind === "pr") references.pr.push(String(row.canonical_url));
  }
  return references;
}

export function addThreadReferences(
  db: Database,
  botUid: string,
  target: ChatTarget,
  references: readonly ThreadReference[],
): void {
  if (!validThreadIdentity(botUid, target) || references.length === 0) return;

  transaction(db, () => {
    const deleted = query(
      db,
      "SELECT 1 AS deleted FROM thread_reference_tombstones WHERE bot_uid = ? AND channel_id = ?",
      [botUid, target.channelId],
    );
    if (deleted.length > 0) return;

    for (const reference of references) {
      if (reference.url.length === 0) continue;
      run(
        db,
        `INSERT OR IGNORE INTO thread_references(bot_uid, channel_id, kind, canonical_url)
         VALUES(?, ?, ?, ?)`,
        [botUid, target.channelId, reference.kind, reference.url],
      );
    }
  });
}

export function isThreadReferencesDeleted(db: Database, botUid: string, target: ChatTarget): boolean {
  if (!validThreadIdentity(botUid, target)) return false;
  return query(
    db,
    "SELECT 1 AS deleted FROM thread_reference_tombstones WHERE bot_uid = ? AND channel_id = ?",
    [botUid, target.channelId],
  ).length > 0;
}

export function deleteThreadReferences(db: Database, botUid: string, target: ChatTarget): void {
  if (!validThreadIdentity(botUid, target)) return;

  transaction(db, () => {
    run(
      db,
      "INSERT OR IGNORE INTO thread_reference_tombstones(bot_uid, channel_id) VALUES(?, ?)",
      [botUid, target.channelId],
    );
    run(
      db,
      "DELETE FROM thread_references WHERE bot_uid = ? AND channel_id = ?",
      [botUid, target.channelId],
    );
  });
}
