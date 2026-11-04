import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { SessionContextSnapshotRecord } from "./store-records.ts";
import { asNumber, asString, query, run, targetValues, targetWhere } from "./store-schema.ts";

export function saveSessionContextSnapshot(db: Database, snapshot: SessionContextSnapshotRecord): void {
  if (
    snapshot.botUid.length === 0
    || snapshot.channelId.length === 0
    || (snapshot.channelType !== 1 && snapshot.channelType !== 2 && snapshot.channelType !== 5)
    || snapshot.sessionId.length === 0
    || snapshot.sessionFile.length === 0
    || snapshot.cwd.length === 0
    || (snapshot.usedTokens !== null && (!Number.isFinite(snapshot.usedTokens) || snapshot.usedTokens < 0))
    || (snapshot.contextWindow !== null && (!Number.isFinite(snapshot.contextWindow) || snapshot.contextWindow <= 0))
    || (snapshot.percent !== null && (!Number.isFinite(snapshot.percent) || snapshot.percent < 0))
    || (snapshot.usedTokens === null && snapshot.contextWindow === null && snapshot.percent === null && snapshot.model === null)
    || !Number.isFinite(Date.parse(snapshot.observedAt))
    || !Number.isSafeInteger(snapshot.fileSize)
    || snapshot.fileSize < 0
    || !Number.isFinite(snapshot.fileMtimeMs)
  ) {
    throw new Error("session context snapshot is invalid");
  }

  run(db,
    `INSERT INTO session_context_snapshots(
       bot_uid, channel_type, channel_id, session_id, session_file, cwd,
       used_tokens, context_window, percent, model, observed_at, file_size, file_mtime_ms
     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(bot_uid, channel_type, channel_id) DO UPDATE SET
       session_id = excluded.session_id,
       session_file = excluded.session_file,
       cwd = excluded.cwd,
       used_tokens = excluded.used_tokens,
       context_window = excluded.context_window,
       percent = excluded.percent,
       model = excluded.model,
       observed_at = excluded.observed_at,
       file_size = excluded.file_size,
       file_mtime_ms = excluded.file_mtime_ms
     WHERE excluded.observed_at >= session_context_snapshots.observed_at`,
    [
      snapshot.botUid,
      snapshot.channelType,
      snapshot.channelId,
      snapshot.sessionId,
      snapshot.sessionFile,
      snapshot.cwd,
      snapshot.usedTokens,
      snapshot.contextWindow,
      snapshot.percent,
      snapshot.model,
      Date.parse(snapshot.observedAt),
      snapshot.fileSize,
      snapshot.fileMtimeMs,
    ],
  );
}

export function getSessionContextSnapshot(
  db: Database,
  botUid: string,
  target: ChatTarget,
): SessionContextSnapshotRecord | null {
  if (
    botUid.length === 0
    || target.channelId.length === 0
    || (target.channelType !== 1 && target.channelType !== 2 && target.channelType !== 5)
  ) {
    return null;
  }
  const row = query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, session_file, cwd,
       used_tokens, context_window, percent, model, observed_at, file_size, file_mtime_ms
     FROM session_context_snapshots
     WHERE bot_uid = ? AND ${targetWhere(target)}`,
    [botUid, ...targetValues(target)],
  )[0];
  if (!row) return null;
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as ChatTarget["channelType"],
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    sessionFile: asString(row.session_file),
    cwd: asString(row.cwd),
    usedTokens: typeof row.used_tokens === "number" ? row.used_tokens : null,
    contextWindow: typeof row.context_window === "number" ? row.context_window : null,
    percent: typeof row.percent === "number" ? row.percent : null,
    model: typeof row.model === "string" ? row.model : null,
    observedAt: new Date(asNumber(row.observed_at)).toISOString(),
    fileSize: asNumber(row.file_size),
    fileMtimeMs: asNumber(row.file_mtime_ms),
  };
}
