import { Database } from "bun:sqlite";
import type { ChatTarget, ThreadSummary } from "../contracts.ts";
import type { BindingRecord, ThreadForkInput, ThreadForkPhase, ThreadForkRecord } from "./store-records.ts";
import { getBinding } from "./store-chats.ts";
import { asNullableString, asNumber, asString, query, run, transaction } from "./store-schema.ts";
import { recordCreatedThreadListen } from "./store-thread-listen.ts";
import { completeThreadWorkspaceIntentInTransaction, getThreadWorkspaceIntent } from "./store-workspaces.ts";

export function saveThreadFork(db: Database, input: ThreadForkInput): void {
  transaction(db, () => {
    const existingRow = query(db, "SELECT * FROM thread_forks WHERE id = ?", [input.id])[0];
    if (!existingRow) {
      const record = threadForkFromInput(input);
      const now = Date.now();
      run(
        db,
        `INSERT INTO thread_forks(
           id, bot_uid, source_channel_type, source_channel_id, source_session_id, source_session_file,
           cwd, group_id, name, preparation_dir, phase, preparation_started, session_id, session_file,
           thread_channel_id, thread_json, error, created_at, updated_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [...threadForkValues(record), now, now],
      );
      return;
    }

    const existing = threadForkFromRow(existingRow);
    assertImmutableIdentity(existing, input);
    const updated = threadForkFromInput(input, existing);
    if (
      existing.sessionId !== null
      && (updated.sessionId !== existing.sessionId || updated.sessionFile !== existing.sessionFile)
    ) {
      throw new Error("thread fork session identity cannot be changed");
    }
    if (existing.thread !== null && updated.thread?.channelId !== existing.thread.channelId) {
      throw new Error("thread fork target cannot be changed");
    }
    run(
      db,
      `UPDATE thread_forks SET
         phase = ?, preparation_started = ?, session_id = ?, session_file = ?, thread_channel_id = ?, thread_json = ?, error = ?, updated_at = ?
       WHERE id = ? AND bot_uid = ?`,
      [
        updated.phase,
        updated.preparationStarted ? 1 : 0,
        updated.sessionId,
        updated.sessionFile,
        updated.thread?.channelId ?? null,
        updated.thread === null ? null : JSON.stringify(updated.thread),
        updated.error,
        Date.now(),
        updated.id,
        updated.botUid,
      ],
    );
  });
}

export function getThreadFork(db: Database, botUid: string, id: string): ThreadForkRecord | null {
  const row = query(db, "SELECT * FROM thread_forks WHERE bot_uid = ? AND id = ?", [botUid, id])[0];
  return row ? threadForkFromRow(row) : null;
}

export function getThreadForkByTarget(
  db: Database,
  botUid: string,
  target: ChatTarget,
): ThreadForkRecord | null {
  if (target.channelType !== 5) return null;
  const row = query(
    db,
    "SELECT * FROM thread_forks WHERE bot_uid = ? AND thread_channel_id = ?",
    [botUid, target.channelId],
  )[0];
  return row ? threadForkFromRow(row) : null;
}

export function listThreadForks(db: Database, botUid: string): ThreadForkRecord[] {
  return query(
    db,
    "SELECT * FROM thread_forks WHERE bot_uid = ? ORDER BY created_at ASC, id ASC",
    [botUid],
  ).map(threadForkFromRow);
}

export function completeThreadFork(db: Database, botUid: string, id: string): ThreadForkRecord {
  return transaction(db, () => {
    const fork = getThreadFork(db, botUid, id);
    if (!fork) throw new Error("thread fork does not exist");
    if (fork.phase !== "remote-created" && fork.phase !== "ready") {
      throw new Error("thread fork is not ready for local completion");
    }
    if (!fork.sessionId || !fork.sessionFile || !fork.cwd || !fork.thread) {
      throw new Error("thread fork is missing its verified session or remote thread");
    }

    const target: ChatTarget = { channelType: 5, channelId: fork.thread.channelId };
    let intent = getThreadWorkspaceIntent(db, botUid, id);
    if (
      intent?.kind === "workspace"
      && intent.creatorChannelType === null
      && intent.creatorChannelId === null
    ) {
      run(
        db,
        `UPDATE thread_workspace_intents
         SET creator_channel_type = ?, creator_channel_id = ?, updated_at = ?
         WHERE id = ? AND bot_uid = ? AND creator_channel_type IS NULL AND creator_channel_id IS NULL`,
        [fork.sourceChannelType, fork.sourceChannelId, Date.now(), id, botUid],
      );
      intent = getThreadWorkspaceIntent(db, botUid, id);
    }
    if (
      !intent
      || intent.groupId !== fork.groupId
      || intent.creatorChannelType !== fork.sourceChannelType
      || intent.creatorChannelId !== fork.sourceChannelId
      || intent.channelIds.length !== 1
      || intent.channelIds[0] !== target.channelId
      || (intent.status !== "preparing" && intent.status !== "complete")
      || (intent.status === "complete" && fork.phase !== "ready")
    ) {
      throw new Error("thread fork requires its matching preparing workspace intent");
    }

    if (fork.thread.groupId !== fork.groupId) {
      throw new Error("thread fork target belongs to a different group");
    }
    ensureForkBinding(db, {
      botUid,
      ...target,
      sessionId: fork.sessionId,
      sessionFile: fork.sessionFile,
      cwd: fork.cwd,
    });
    const listen = recordCreatedThreadListen(db, {
      botUid,
      channelId: fork.thread.channelId,
      creatorChannelType: fork.sourceChannelType,
      creatorChannelId: fork.sourceChannelId,
      threadName: fork.thread.name,
      listen: "merge",
    });
    if (
      listen.creatorChannelType !== fork.sourceChannelType
      || listen.creatorChannelId !== fork.sourceChannelId
    ) {
      throw new Error("thread fork listen policy belongs to a different creator");
    }

    if (fork.phase !== "ready") {
      run(
        db,
        `UPDATE thread_forks SET phase = 'ready', error = NULL, updated_at = ?
         WHERE bot_uid = ? AND id = ? AND phase = 'remote-created'`,
        [Date.now(), botUid, id],
      );
    }
    completeThreadWorkspaceIntentInTransaction(db, botUid, id);
    const completed = getThreadFork(db, botUid, id);
    if (!completed || completed.phase !== "ready") throw new Error("thread fork completion was not persisted");
    return completed;
  });
}

function ensureForkBinding(db: Database, binding: BindingRecord): void {
  const existing = getBinding(db, binding.botUid, binding);
  if (existing) {
    if (
      existing.sessionId !== binding.sessionId
      || existing.sessionFile !== binding.sessionFile
      || existing.cwd !== binding.cwd
    ) {
      throw new Error("thread fork target already has a conflicting binding");
    }
    return;
  }

  const now = Date.now();
  run(
    db,
    `INSERT INTO chats(
       bot_uid, channel_type, channel_id, session_id, cwd, session_file, cwd_pinned,
       session_switched_at, created_at, updated_at
     ) VALUES(?, ?, ?, ?, ?, ?, 0, NULL, ?, ?)`,
    [binding.botUid, binding.channelType, binding.channelId, binding.sessionId, binding.cwd, binding.sessionFile, now, now],
  );
}

function assertImmutableIdentity(existing: ThreadForkRecord, input: ThreadForkInput): void {
  if (
    existing.botUid !== input.botUid
    || existing.sourceChannelType !== input.sourceChannelType
    || existing.sourceChannelId !== input.sourceChannelId
    || existing.sourceSessionId !== input.sourceSessionId
    || existing.sourceSessionFile !== input.sourceSessionFile
    || existing.cwd !== input.cwd
    || existing.groupId !== input.groupId
    || existing.name !== input.name
    || existing.preparationDir !== input.preparationDir
  ) {
    throw new Error("thread fork operation identity and source are immutable");
  }
}

function threadForkFromInput(input: ThreadForkInput, existing?: ThreadForkRecord): ThreadForkRecord {
  return {
    id: input.id,
    botUid: input.botUid,
    sourceChannelType: input.sourceChannelType,
    sourceChannelId: input.sourceChannelId,
    sourceSessionId: input.sourceSessionId,
    sourceSessionFile: input.sourceSessionFile,
    cwd: input.cwd,
    groupId: input.groupId,
    name: input.name,
    preparationDir: input.preparationDir,
    phase: input.phase ?? existing?.phase ?? "preparing",
    preparationStarted: existing?.preparationStarted === true || input.preparationStarted === true,
    sessionId: input.sessionId === undefined ? existing?.sessionId ?? null : input.sessionId,
    sessionFile: input.sessionFile === undefined ? existing?.sessionFile ?? null : input.sessionFile,
    thread: input.thread === undefined ? existing?.thread ?? null : input.thread,
    error: input.error === undefined ? existing?.error ?? null : input.error,
  };
}

function threadForkValues(record: ThreadForkRecord): (string | number | null)[] {
  return [
    record.id,
    record.botUid,
    record.sourceChannelType,
    record.sourceChannelId,
    record.sourceSessionId,
    record.sourceSessionFile,
    record.cwd,
    record.groupId,
    record.name,
    record.preparationDir,
    record.phase,
    record.preparationStarted ? 1 : 0,
    record.sessionId,
    record.sessionFile,
    record.thread?.channelId ?? null,
    record.thread === null ? null : JSON.stringify(record.thread),
    record.error,
  ];
}

function threadForkFromRow(row: Record<string, unknown>): ThreadForkRecord {
  const sourceChannelType = asNumber(row.source_channel_type);
  if (sourceChannelType !== 2 && sourceChannelType !== 5) {
    throw new Error("invalid thread fork source channel type in store");
  }
  const phase = row.phase;
  if (
    phase !== "preparing"
    && phase !== "prepared"
    && phase !== "creating"
    && phase !== "remote-created"
    && phase !== "ready"
    && phase !== "failed"
  ) {
    throw new Error("invalid thread fork phase in store");
  }
  const threadJson = asNullableString(row.thread_json);
  const threadChannelId = asNullableString(row.thread_channel_id);
  const thread = threadJson === null ? null : threadSummaryFromJson(threadJson);
  if ((thread === null) !== (threadChannelId === null) || (thread !== null && thread.channelId !== threadChannelId)) {
    throw new Error("invalid thread fork target in store");
  }
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    sourceChannelType,
    sourceChannelId: asString(row.source_channel_id),
    sourceSessionId: asString(row.source_session_id),
    sourceSessionFile: asString(row.source_session_file),
    cwd: asString(row.cwd),
    groupId: asString(row.group_id),
    name: asString(row.name),
    preparationDir: asString(row.preparation_dir),
    phase: phase as ThreadForkPhase,
    preparationStarted: asNumber(row.preparation_started) !== 0,
    sessionId: asNullableString(row.session_id),
    sessionFile: asNullableString(row.session_file),
    thread,
    error: asNullableString(row.error),
  };
}

function threadSummaryFromJson(json: string): ThreadSummary {
  let value: unknown;
  try {
    value = JSON.parse(json) as unknown;
  } catch {
    throw new Error("invalid thread summary in fork store");
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid thread summary in fork store");
  }
  const thread = value as Record<string, unknown>;
  if (
    typeof thread.shortId !== "string"
    || typeof thread.groupId !== "string"
    || typeof thread.channelId !== "string"
    || typeof thread.name !== "string"
    || typeof thread.creatorUid !== "string"
    || typeof thread.status !== "number"
    || typeof thread.memberCount !== "number"
    || typeof thread.messageCount !== "number"
    || typeof thread.createdAt !== "string"
    || typeof thread.updatedAt !== "string"
  ) {
    throw new Error("invalid thread summary in fork store");
  }
  return {
    shortId: thread.shortId,
    groupId: thread.groupId,
    channelId: thread.channelId,
    name: thread.name,
    creatorUid: thread.creatorUid,
    status: thread.status,
    memberCount: thread.memberCount,
    messageCount: thread.messageCount,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
  };
}
