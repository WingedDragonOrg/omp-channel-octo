/**
 * SQLite 的物理形状：建表 DDL、列迁移、行解码、带 db 句柄的查询辅助，以及 `openChannelStore`。
 * 领域读写分布在 `store-chats/grants/tasks/tan/cards/workers/delivery/ledgers/outbox/questions/references`，门面在 `index.ts`。
 */

import { Database } from "bun:sqlite";
import type { ChatExecutionState, ChatTarget, TextMention } from "../contracts.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";
import type { ContextLedgerRecord, HistoryLedgerRecord, OutboxRecord, OutboxTextPayload, QuestionRecord, TaskRecord, TaskStopIntent } from "./store-records.ts";

export type SqlValue = string | number | null | Uint8Array;
export type SqlRow = Record<string, unknown>;

export function targetWhere(target: ChatTarget): string {
  return "channel_type = ? AND channel_id = ?";
}

export function targetValues(target: ChatTarget): SqlValue[] {
  return [target.channelType, target.channelId];
}

/**
 * One row per progress card. A run opens a new card for every user round it serves, so the row is
 * keyed by the card's server message id: each card is then closed, retried, and dropped on its own.
 */
export const PROGRESS_CARD_COLUMNS = `
      bot_uid TEXT NOT NULL,
      task_id TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      last_seq INTEGER NOT NULL CHECK(last_seq >= 1),
      snapshot TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, task_id, message_id)`;

const THREAD_WORKSPACE_OPERATION_PHASES_SQL = [
  "accepted",
  "entry-creating",
  "entry-root-created",
  "entry-create-intent",
  "entry-created",
  "alias-creating",
  "alias-created",
  "project-not-started",
  "project-creating",
  "project-process-exited",
  "project-created",
  "move-dispatch-intent",
  "move-confirmed",
  "committed",
].map(phase => `'${phase}'`).join(", ");

const THREAD_WORKSPACE_OPERATION_COLUMNS = `
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      group_id TEXT NOT NULL,
      tool_call_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      schedule_order INTEGER NOT NULL CHECK(schedule_order >= 1),
      worktree_name_seed TEXT NOT NULL,
      projects_json TEXT NOT NULL,
      expected_workspace_json TEXT NOT NULL,
      expected_binding_json TEXT NOT NULL,
      candidate_workspace_json TEXT NOT NULL,
      candidate_binding_json TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN (${THREAD_WORKSPACE_OPERATION_PHASES_SQL})),
      status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'complete', 'failed', 'cancelled', 'unknown')),
      worker_ledger_id TEXT,
      process_identity_json TEXT,
      error TEXT,
      cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0, 1)),
      version INTEGER NOT NULL DEFAULT 1 CHECK(version >= 1),
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE(bot_uid, channel_id, tool_call_id)`;

/**
 * Group history accounting is keyed on full group (2) / CommunityTopic thread (5)
 * identities; anything else can never own a ledger row.
 */
export function validHistoryIdentity(botUid: string, target: ChatTarget, sessionId: string): boolean {
  return (
  botUid.length > 0 &&
  sessionId.length > 0 &&
  target.channelId.length > 0 &&
  (target.channelType === 2 || target.channelType === 5)
  );
}

export function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

export function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

export function asNumber(value: unknown): number {
  return typeof value === "number" ? value : Number(value ?? 0);
}

export function asState(value: unknown): ChatExecutionState {
  if (
  value === "none" ||
  value === "idle" ||
  value === "queued" ||
  value === "started" ||
  value === "running" ||
  value === "completed" ||
  value === "failed" ||
  value === "cancelled" ||
  value === "interrupted"
  ) {
    return value === "started" ? "running" : value;
  }
  return "failed";
}

/** 持久停止意图的读值；未知/空取值为 `null`（调用方按「没有叫停」处理）。 */
export function asStopIntent(value: unknown): TaskStopIntent | null {
  return value === "cancel" || value === "interrupt" || value === "question_timeout" ? value : null;
}


export function applySchema(db: Database): void {
  db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
  db.exec(`
    CREATE TABLE IF NOT EXISTS scheduler_order_state (
      singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton = 1),
      next_order INTEGER NOT NULL CHECK(next_order >= 1)
    );
    INSERT OR IGNORE INTO scheduler_order_state(singleton, next_order) VALUES(1, 1);
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY NOT NULL,
      value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS owners (
      bot_uid TEXT PRIMARY KEY NOT NULL,
      owner_id TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS owner_enrollments (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      accepted_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, message_id)
    );
    CREATE TABLE IF NOT EXISTS allowed_chats (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      granted_by TEXT NOT NULL,
      granted_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS chats (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      cwd TEXT NOT NULL,
      session_file TEXT,
      cwd_pinned INTEGER NOT NULL DEFAULT 0,
      session_switched_at INTEGER,
      session_initialization_json TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS session_context_snapshots (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      session_file TEXT NOT NULL,
      cwd TEXT NOT NULL,
      used_tokens REAL CHECK(used_tokens >= 0),
      context_window REAL CHECK(context_window > 0),
      percent REAL CHECK(percent >= 0),
      model TEXT,
      observed_at INTEGER NOT NULL,
      file_size INTEGER NOT NULL CHECK(file_size >= 0),
      file_mtime_ms REAL NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_workspaces (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      layout TEXT NOT NULL CHECK(layout IN ('single', 'shared', 'separate')),
      status TEXT NOT NULL CHECK(status IN ('preparing', 'ready', 'failed')),
      entry_path TEXT,
      workspace_root TEXT,
      projects TEXT NOT NULL,
      legacy_worktree_name TEXT,
      error TEXT,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_workspace_operations (
      ${THREAD_WORKSPACE_OPERATION_COLUMNS}
    );
    CREATE INDEX IF NOT EXISTS thread_workspace_operations_target
      ON thread_workspace_operations(bot_uid, channel_id, status, schedule_order);
    CREATE TABLE IF NOT EXISTS thread_workspace_cleanups (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('reserved', 'cleaning', 'unknown', 'settled')),
      ownership_json TEXT,
      binding_json TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS thread_workspace_cleanups_active_target
      ON thread_workspace_cleanups(bot_uid, channel_id)
      WHERE phase IN ('reserved', 'cleaning', 'unknown');
    CREATE INDEX IF NOT EXISTS thread_workspace_cleanups_phase
      ON thread_workspace_cleanups(bot_uid, phase, channel_id);
    CREATE TABLE IF NOT EXISTS thread_workspace_intents (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      group_id TEXT NOT NULL,
      intent_kind TEXT NOT NULL DEFAULT 'workspace' CHECK(intent_kind IN ('workspace', 'thread-create')),
      creator_channel_type INTEGER CHECK(creator_channel_type IS NULL OR creator_channel_type IN (2, 5)),
      creator_channel_id TEXT,
      status TEXT NOT NULL CHECK(status IN ('preparing', 'failed', 'complete')),
      channel_ids TEXT NOT NULL,
      error TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      CHECK((creator_channel_type IS NULL) = (creator_channel_id IS NULL))
    );
    CREATE TABLE IF NOT EXISTS thread_forks (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      source_channel_type INTEGER NOT NULL CHECK(source_channel_type IN (2, 5)),
      source_channel_id TEXT NOT NULL,
      source_session_id TEXT NOT NULL,
      source_session_file TEXT NOT NULL,
      cwd TEXT NOT NULL,
      group_id TEXT NOT NULL,
      name TEXT NOT NULL,
      preparation_dir TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('preparing', 'prepared', 'creating', 'remote-created', 'ready', 'failed')),
      preparation_started INTEGER NOT NULL DEFAULT 0 CHECK(preparation_started IN (0, 1)),
      session_id TEXT,
      session_file TEXT,
      thread_channel_id TEXT,
      thread_json TEXT,
      error TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      CHECK((thread_channel_id IS NULL) = (thread_json IS NULL))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS thread_forks_target
      ON thread_forks(bot_uid, thread_channel_id) WHERE thread_channel_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS thread_forks_bot
      ON thread_forks(bot_uid, created_at, id);
    CREATE TABLE IF NOT EXISTS tan_operations (
      bot_uid TEXT NOT NULL,
      id TEXT NOT NULL,
      source_channel_type INTEGER NOT NULL CHECK(source_channel_type IN (2, 5)),
      source_channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      source_session_id TEXT NOT NULL,
      source_session_file TEXT NOT NULL,
      cwd TEXT NOT NULL,
      phase TEXT NOT NULL CHECK(phase IN ('pending', 'done', 'failed', 'cancelled')),
      error TEXT,
      accepted_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, id),
      UNIQUE(bot_uid, source_channel_type, source_channel_id, message_id)
    );
    CREATE INDEX IF NOT EXISTS tan_operations_pending
      ON tan_operations(bot_uid, phase, accepted_at, id);
    CREATE INDEX IF NOT EXISTS thread_workspace_intent_group
      ON thread_workspace_intents(bot_uid, group_id, status);
    CREATE TABLE IF NOT EXISTS thread_listens (
      bot_uid TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      creator_channel_type INTEGER NOT NULL CHECK(creator_channel_type IN (2, 5)),
      creator_channel_id TEXT NOT NULL,
      thread_name TEXT NOT NULL,
      listen TEXT NOT NULL CHECK(listen IN ('off', 'merge', 'task')),
      merge_pending INTEGER NOT NULL DEFAULT 0 CHECK(merge_pending IN (0, 1)),
      policy_confirmed INTEGER NOT NULL DEFAULT 1 CHECK(policy_confirmed IN (0, 1)),
      PRIMARY KEY(bot_uid, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_archive_generations (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK(generation >= 0),
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_merge_events (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      target_branch TEXT NOT NULL,
      commit_hash TEXT,
      removed INTEGER NOT NULL CHECK(removed IN (0, 1)),
      archive TEXT NOT NULL CHECK(archive IN ('scheduled', 'not-scheduled')),
      delivery_state TEXT NOT NULL DEFAULT 'pending' CHECK(delivery_state IN ('pending', 'intent', 'delivered'))
    );
    CREATE TABLE IF NOT EXISTS thread_task_events (
      task_id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      delivery_state TEXT NOT NULL DEFAULT 'pending' CHECK(delivery_state IN ('pending', 'intent', 'delivered')),
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS thread_references (
      bot_uid TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('mr', 'pr')),
      canonical_url TEXT NOT NULL,
      PRIMARY KEY(bot_uid, channel_id, kind, canonical_url)
    );
    CREATE TABLE IF NOT EXISTS thread_reference_tombstones (
      bot_uid TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      PRIMARY KEY(bot_uid, channel_id)
    );
    CREATE INDEX IF NOT EXISTS thread_merge_events_bot ON thread_merge_events(bot_uid);
    CREATE INDEX IF NOT EXISTS thread_task_events_pending
      ON thread_task_events(bot_uid, delivery_state, created_at, task_id);
    CREATE TABLE IF NOT EXISTS inbound (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      supported INTEGER NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('task', 'command')),
      command_state TEXT NOT NULL DEFAULT 'pending' CHECK(command_state IN ('pending', 'completed', 'failed')),
      task_id TEXT,
      accepted_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ingress_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      schedule_order INTEGER NOT NULL DEFAULT 0,
      bot_uid TEXT NOT NULL,
      source_kind TEXT NOT NULL CHECK(source_kind IN ('recv', 'bot-event', 'synthetic')),
      source_id TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT,
      payload_json TEXT,
      route_snapshot_json TEXT,
      prepared_text TEXT,
      spool_paths_json TEXT NOT NULL DEFAULT '[]',
      ack_ready INTEGER NOT NULL CHECK(ack_ready IN (0, 1)),
      lane TEXT NOT NULL CHECK(lane IN ('ordered', 'control')),
      state TEXT NOT NULL CHECK(state IN ('accepted', 'preparing', 'dispatch-intent', 'handed-off', 'unknown', 'cancelled')),
      operation_checkpoint_json TEXT,
      consumer_kind TEXT CHECK(consumer_kind IS NULL OR consumer_kind IN ('task', 'steering', 'ask', 'operation', 'outbox')),
      consumer_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE(bot_uid, source_kind, source_id),
      CHECK((consumer_kind IS NULL) = (consumer_id IS NULL)),
      CHECK(state != 'cancelled' OR (payload_json IS NULL AND route_snapshot_json IS NULL AND ack_ready = 1))
    );
    CREATE INDEX IF NOT EXISTS ingress_jobs_recovery ON ingress_jobs(bot_uid, state, id);
    CREATE INDEX IF NOT EXISTS ingress_jobs_chat_order ON ingress_jobs(bot_uid, channel_type, channel_id, lane, id);
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      text TEXT NOT NULL,
      session_id TEXT NOT NULL,
      cwd TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('queued', 'started', 'completed', 'failed', 'cancelled', 'interrupted')),
      accepted_at INTEGER NOT NULL,
      schedule_order INTEGER NOT NULL DEFAULT 0,
      started_at INTEGER,
      finished_at INTEGER,
      result_text TEXT,
      error_text TEXT,
      message_seq INTEGER NOT NULL DEFAULT 0,
      history_eligible INTEGER NOT NULL DEFAULT 1,
      prompt_started INTEGER NOT NULL DEFAULT 0,
      prompt_accepted INTEGER NOT NULL DEFAULT 0,
      stop_intent TEXT,
      relay_suppressed INTEGER NOT NULL DEFAULT 0 CHECK(relay_suppressed IN (0, 1)),
      final_result_text TEXT
    );
    CREATE TABLE IF NOT EXISTS task_file_link_context (
      task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
      cwd TEXT NOT NULL,
      resolvers_json TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS review_jobs (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      sender_id TEXT NOT NULL,
      focus TEXT NOT NULL,
      projects_json TEXT NOT NULL,
      cwd TEXT NOT NULL,
      session_id TEXT NOT NULL,
      session_file TEXT,
      state TEXT NOT NULL CHECK(state IN ('accepted', 'running', 'completed', 'failed', 'stopped')),
      dispatch_started INTEGER NOT NULL DEFAULT 0 CHECK(dispatch_started IN (0, 1)),
      prompt_accepted INTEGER NOT NULL DEFAULT 0 CHECK(prompt_accepted IN (0, 1)),
      stop_requested INTEGER NOT NULL DEFAULT 0 CHECK(stop_requested IN (0, 1)),
      recovery_reported INTEGER NOT NULL DEFAULT 0 CHECK(recovery_reported IN (0, 1)),
      accepted_at INTEGER NOT NULL,
      started_at INTEGER,
      finished_at INTEGER,
      result_text TEXT,
      error_text TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS review_jobs_message
      ON review_jobs(bot_uid, channel_type, channel_id, message_id);
    CREATE UNIQUE INDEX IF NOT EXISTS review_jobs_one_active_chat
      ON review_jobs(bot_uid, channel_type, channel_id)
      WHERE state IN ('accepted', 'running');
    CREATE TABLE IF NOT EXISTS goal_sessions (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      goal_id TEXT,
      objective TEXT NOT NULL,
      status TEXT CHECK(status IS NULL OR status IN ('active', 'paused', 'budget-limited', 'complete', 'dropped')),
      tokens_used INTEGER NOT NULL DEFAULT 0 CHECK(tokens_used >= 0),
      time_used_seconds INTEGER NOT NULL DEFAULT 0 CHECK(time_used_seconds >= 0),
      token_budget INTEGER CHECK(token_budget IS NULL OR token_budget > 0),
      continuation_authorized INTEGER NOT NULL DEFAULT 0 CHECK(continuation_authorized IN (0, 1)),
      stop_intent TEXT CHECK(stop_intent IS NULL OR stop_intent IN ('pause', 'drop', 'interrupt', 'question_timeout', 'cancel')),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
    );
    CREATE TABLE IF NOT EXISTS goal_tasks (
      task_id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      request_op TEXT NOT NULL CHECK(request_op IN ('create', 'resume')),
      dispatch_op TEXT CHECK(dispatch_op IS NULL OR dispatch_op IN ('create', 'resume', 'recover')),
      objective TEXT NOT NULL DEFAULT '',
      token_budget INTEGER CHECK(token_budget IS NULL OR token_budget > 0),
      dispatched INTEGER NOT NULL DEFAULT 0 CHECK(dispatched IN (0, 1)),
      accepted INTEGER NOT NULL DEFAULT 0 CHECK(accepted IN (0, 1)),
      agent_accepted INTEGER NOT NULL DEFAULT 0 CHECK(agent_accepted IN (0, 1)),
      accepted_goal_id TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS goal_tasks_target
      ON goal_tasks(bot_uid, channel_type, channel_id, session_id, created_at, task_id);
    CREATE INDEX IF NOT EXISTS tasks_chat_order
      ON tasks(bot_uid, channel_type, channel_id, accepted_at, id);
    CREATE INDEX IF NOT EXISTS tasks_active
      ON tasks(bot_uid, channel_type, channel_id, state);
    CREATE TABLE IF NOT EXISTS outbox (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      task_id TEXT,
      kind TEXT NOT NULL CHECK(kind IN ('text', 'file')),
      text TEXT,
      path TEXT,
      file_kind TEXT CHECK(file_kind IN ('image', 'file')),
      client_msg_no TEXT NOT NULL UNIQUE,
      state TEXT NOT NULL CHECK(state IN ('pending', 'unknown', 'sent', 'dead')),
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      remote_message_id TEXT,
      stream_message_id TEXT,
      segment_index INTEGER NOT NULL DEFAULT 0,
      desired_version INTEGER NOT NULL DEFAULT 0,
      acked_version INTEGER NOT NULL DEFAULT 0,
      mention_json TEXT,
      stream_final INTEGER NOT NULL DEFAULT 0,
      create_payload_json TEXT,
      created_at INTEGER NOT NULL,
      sent_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS outbox_order
      ON outbox(bot_uid, channel_type, channel_id, created_at, id);
    CREATE TABLE IF NOT EXISTS questions (
      id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      payload TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('pending', 'answered', 'cancelled', 'expired')),
      answer TEXT,
      answered_by TEXT,
      closed_reason TEXT,
      created_at INTEGER NOT NULL,
      answered_at INTEGER,
      card_message_id TEXT,
      card_seq INTEGER,
      settled_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS questions_chat_pending
      ON questions(bot_uid, channel_type, channel_id, state);
    CREATE TABLE IF NOT EXISTS worker_ledger (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      session_id TEXT NOT NULL,
      session_file TEXT,
      pid INTEGER NOT NULL,
      pgid INTEGER NOT NULL,
      identity TEXT NOT NULL,
      cwd TEXT NOT NULL,
      move_target_cwd TEXT,
      state TEXT NOT NULL CHECK(state IN ('running', 'stopping', 'stopped', 'unknown')),
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS host_merge_leases (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      operation_kind TEXT NOT NULL DEFAULT 'merge' CHECK(operation_kind IN ('merge', 'thread-remove')),
      cwd TEXT NOT NULL,
      resource_cwds TEXT,
      phase TEXT NOT NULL CHECK(phase IN ('starting', 'running', 'exited', 'unknown')),
      pid INTEGER,
      pgid INTEGER,
      process_identity TEXT,
      source_text TEXT,
      operator_note TEXT,
      sender_id TEXT,
      message_seq INTEGER,
      receipt_outbox_id TEXT,
      archive_generation INTEGER,
      not_started_text TEXT,
      outcome_kind TEXT CHECK(outcome_kind IS NULL OR outcome_kind IN ('succeeded', 'execution-failed')),
      outcome_json TEXT,
      auto_commit_task_id TEXT,
      auto_commit_state TEXT CHECK(auto_commit_state IS NULL OR auto_commit_state IN ('committing', 'result-known', 'merge-ready', 'merging', 'unknown', 'complete')),
      auto_commit_dispatched INTEGER NOT NULL DEFAULT 0 CHECK(auto_commit_dispatched IN (0, 1)),
      auto_commit_result_json TEXT,
      stop_requested INTEGER NOT NULL DEFAULT 0 CHECK(stop_requested IN (0, 1)),
      removal_resources_json TEXT,
      removal_outcome_json TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS host_merge_leases_bot_cwd
      ON host_merge_leases(bot_uid, cwd, phase);
    CREATE INDEX IF NOT EXISTS host_merge_leases_source
      ON host_merge_leases(bot_uid, channel_type, channel_id, source_id, created_at);
    CREATE TABLE IF NOT EXISTS host_merge_progress_cards (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type = 5),
      channel_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      client_msg_no TEXT NOT NULL,
      message_id TEXT,
      delivery_state TEXT NOT NULL DEFAULT 'ready' CHECK(delivery_state IN ('ready', 'attempted', 'unknown', 'unavailable', 'bound')),
      last_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_seq >= 0),
      phase TEXT NOT NULL CHECK(phase IN (
        'preparing', 'process-started', 'process-exited', 'succeeded',
        'execution-failed', 'not-started', 'cancelled', 'unknown'
      )),
      started_at INTEGER NOT NULL,
      ended_at INTEGER,
      result_text TEXT,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, source_id)
    );
    CREATE INDEX IF NOT EXISTS host_merge_progress_cards_recovery
      ON host_merge_progress_cards(bot_uid, updated_at);
    CREATE TABLE IF NOT EXISTS session_context_ledger (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      version INTEGER NOT NULL CHECK(version >= 0),
      phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
      attempt_id TEXT NOT NULL,
      previous_version INTEGER CHECK(previous_version IS NULL OR previous_version >= 0),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
    );
    CREATE INDEX IF NOT EXISTS session_context_target
      ON session_context_ledger(bot_uid, channel_type, channel_id);
    CREATE TABLE IF NOT EXISTS group_history_ledger (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      last_seq INTEGER NOT NULL CHECK(last_seq >= 0),
      phase TEXT NOT NULL CHECK(phase IN ('pending', 'committed')),
      attempt_id TEXT NOT NULL,
      previous_seq INTEGER CHECK(previous_seq IS NULL OR previous_seq >= 0),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id, session_id)
    );
    CREATE INDEX IF NOT EXISTS group_history_target
      ON group_history_ledger(bot_uid, channel_type, channel_id);
    CREATE TABLE IF NOT EXISTS restart_notices (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      requested_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS chat_modes (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      mode TEXT NOT NULL CHECK(mode IN ('steer', 'queue')),
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS chat_models (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      model TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS thread_models (
      bot_uid TEXT NOT NULL,
      group_id TEXT NOT NULL,
      model TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, group_id)
    );
    CREATE TABLE IF NOT EXISTS progress_cards (${PROGRESS_CARD_COLUMNS}
    );
    CREATE TABLE IF NOT EXISTS bot_event_cursor (
      bot_uid TEXT PRIMARY KEY NOT NULL,
      event_id INTEGER NOT NULL CHECK(event_id >= 0),
      updated_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS chat_grant_operations (
      bot_uid TEXT NOT NULL,
      action_id TEXT NOT NULL,
      event_id INTEGER NOT NULL CHECK(event_id >= 0),
      owner_id TEXT NOT NULL,
      actor_id TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (2, 5)),
      channel_id TEXT NOT NULL,
      card_message_id TEXT NOT NULL,
      result_message_id TEXT NOT NULL,
      accepted_at INTEGER NOT NULL,
      card_edit_seq INTEGER NOT NULL DEFAULT 1 CHECK(card_edit_seq >= 1),
      config_mirror_state TEXT NOT NULL CHECK(config_mirror_state IN ('pending', 'succeeded', 'failed')),
      config_mirror_error TEXT,
      result_state TEXT NOT NULL CHECK(result_state IN ('pending', 'delivered')),
      PRIMARY KEY(bot_uid, action_id),
      UNIQUE(bot_uid, event_id)
    );
    CREATE TABLE IF NOT EXISTS watches (
      id TEXT PRIMARY KEY NOT NULL,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      source_task_id TEXT NOT NULL,
      creation_key TEXT NOT NULL,
      spec_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      deadline_at INTEGER NOT NULL,
      next_at INTEGER NOT NULL,
      checks INTEGER NOT NULL CHECK(checks >= 0),
      max_checks INTEGER NOT NULL CHECK(max_checks > 0),
      generation INTEGER NOT NULL CHECK(generation >= 0),
      failures INTEGER NOT NULL CHECK(failures >= 0),
      state TEXT NOT NULL CHECK(state IN ('waiting', 'checking', 'matched', 'failed', 'expired', 'stopped', 'deleted', 'unknown')),
      latest_json TEXT,
      matched_evidence TEXT,
      process_json TEXT,
      process_unknown INTEGER NOT NULL DEFAULT 0 CHECK(process_unknown IN (0, 1)),
      wake_task_id TEXT,
      wake_status TEXT NOT NULL CHECK(wake_status IN ('none', 'queued', 'accepted', 'dispatched', 'cancelled', 'failed')),
      stop_reason TEXT,
      matched_at INTEGER,
      UNIQUE(bot_uid, source_task_id, creation_key)
    );
    CREATE TABLE IF NOT EXISTS watch_chat_tombstones (
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      deleted_at INTEGER NOT NULL,
      PRIMARY KEY(bot_uid, channel_type, channel_id)
    );
    CREATE TABLE IF NOT EXISTS watch_match_events (
      message_id TEXT PRIMARY KEY NOT NULL,
      watch_id TEXT NOT NULL UNIQUE,
      bot_uid TEXT NOT NULL,
      channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
      channel_id TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS watches_due ON watches(state, next_at, deadline_at);
    CREATE INDEX IF NOT EXISTS watches_bot_chat ON watches(bot_uid, channel_type, channel_id, created_at, id);
    CREATE INDEX IF NOT EXISTS chat_grant_operations_recovery
      ON chat_grant_operations(bot_uid, config_mirror_state, result_state, accepted_at, event_id);
  `);
  // 两个 prompt 标记列同一次引入。补列与旧行收尾必须在同一事务里：中途崩溃要么都回滚（下次
  // 打开重来），要么都生效——否则会留下「列已补上、旧 started 行没被标记」的中间态，把旧行的
  // prompt 歧义退化成自动重放。
  const taskColumns = query(db, "PRAGMA table_info(tasks)");
  const promptStartedWasMissing = !taskColumns.some((row) => asString(row.name) === "prompt_started");
  const promptAcceptedWasMissing = !taskColumns.some((row) => asString(row.name) === "prompt_accepted");
  if (promptStartedWasMissing || promptAcceptedWasMissing) {
    transaction(db, () => {
      if (promptStartedWasMissing) {
        db.exec("ALTER TABLE tasks ADD COLUMN prompt_started INTEGER NOT NULL DEFAULT 0");
      }
      if (promptAcceptedWasMissing) {
        db.exec("ALTER TABLE tasks ADD COLUMN prompt_accepted INTEGER NOT NULL DEFAULT 0");
      }
      if (promptStartedWasMissing) markLegacyStartedUnconfirmed(db);
    });
  }
  for (const statement of [
    "ALTER TABLE outbox ADD COLUMN remote_message_id TEXT",
    "ALTER TABLE outbox ADD COLUMN stream_message_id TEXT",
    "ALTER TABLE outbox ADD COLUMN segment_index INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN desired_version INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN acked_version INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN mention_json TEXT",
    "ALTER TABLE outbox ADD COLUMN stream_final INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE outbox ADD COLUMN create_payload_json TEXT",
    "ALTER TABLE worker_ledger ADD COLUMN move_target_cwd TEXT",
    "ALTER TABLE ingress_jobs ADD COLUMN schedule_order INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN schedule_order INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN message_seq INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE tasks ADD COLUMN history_eligible INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE tasks ADD COLUMN stop_intent TEXT",
    "ALTER TABLE tasks ADD COLUMN final_result_text TEXT",
    "ALTER TABLE chats ADD COLUMN cwd_pinned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE chats ADD COLUMN session_switched_at INTEGER",
    "ALTER TABLE chats ADD COLUMN session_initialization_json TEXT",
    "ALTER TABLE questions ADD COLUMN card_message_id TEXT",
    "ALTER TABLE questions ADD COLUMN answered_by TEXT",
    "ALTER TABLE questions ADD COLUMN closed_reason TEXT",
    "ALTER TABLE thread_workspace_operations ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0, 1))",
    "ALTER TABLE thread_workspaces ADD COLUMN legacy_worktree_name TEXT",
    "ALTER TABLE thread_listens ADD COLUMN policy_confirmed INTEGER NOT NULL DEFAULT 1 CHECK(policy_confirmed IN (0, 1))",
    "ALTER TABLE thread_workspace_intents ADD COLUMN intent_kind TEXT NOT NULL DEFAULT 'workspace' CHECK(intent_kind IN ('workspace', 'thread-create'))",
    "ALTER TABLE thread_workspace_intents ADD COLUMN creator_channel_type INTEGER CHECK(creator_channel_type IS NULL OR creator_channel_type IN (2, 5))",
    "ALTER TABLE thread_workspace_intents ADD COLUMN creator_channel_id TEXT",
    "ALTER TABLE thread_workspaces ADD COLUMN workspace_root TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN source_text TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN resource_cwds TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN operator_note TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN sender_id TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN message_seq INTEGER",
    "ALTER TABLE host_merge_leases ADD COLUMN receipt_outbox_id TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN archive_generation INTEGER",
    "ALTER TABLE host_merge_leases ADD COLUMN not_started_text TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN outcome_kind TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN outcome_json TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN operation_kind TEXT NOT NULL DEFAULT 'merge' CHECK(operation_kind IN ('merge', 'thread-remove'))",
    "ALTER TABLE host_merge_leases ADD COLUMN removal_step TEXT CHECK(removal_step IS NULL OR removal_step IN ('worktree', 'branch'))",
    "ALTER TABLE host_merge_leases ADD COLUMN removal_resources_json TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN removal_outcome_json TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN auto_commit_task_id TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN auto_commit_state TEXT CHECK(auto_commit_state IS NULL OR auto_commit_state IN ('committing', 'result-known', 'merge-ready', 'merging', 'unknown', 'complete'))",
    "ALTER TABLE host_merge_leases ADD COLUMN auto_commit_dispatched INTEGER NOT NULL DEFAULT 1 CHECK(auto_commit_dispatched IN (0, 1))",
    "ALTER TABLE host_merge_leases ADD COLUMN auto_commit_result_json TEXT",
    "ALTER TABLE host_merge_leases ADD COLUMN stop_requested INTEGER NOT NULL DEFAULT 0 CHECK(stop_requested IN (0, 1))",
    "ALTER TABLE host_merge_progress_cards ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'unknown' CHECK(delivery_state IN ('ready', 'attempted', 'unknown', 'unavailable', 'bound'))",
    "ALTER TABLE watches ADD COLUMN process_unknown INTEGER NOT NULL DEFAULT 0 CHECK(process_unknown IN (0, 1))",
  ]) {
    try {
      db.exec(statement);
    } catch (error) {
      if (!String(error).includes("duplicate column")) throw error;
    }
  }
  migrateThreadWorkspaceOperationPhases(db);
  migrateSchedulerOrder(db);
  db.exec("UPDATE host_merge_progress_cards SET delivery_state = 'bound' WHERE delivery_state = 'unknown' AND message_id IS NOT NULL");
  migrateWorkerLedgerNullableSessionFile(db);
  migrateThreadForkPreparationStarted(db);
  db.exec(
    "CREATE INDEX IF NOT EXISTS host_merge_leases_operation_source ON host_merge_leases(bot_uid, operation_kind, channel_type, channel_id, source_id, created_at)",
  );
  migrateThreadListen(db);
  const questionColumns = query(db, "PRAGMA table_info(questions)");
  const cardSeqWasMissing = !questionColumns.some((row) => asString(row.name) === "card_seq");
  const settledAtWasMissing = !questionColumns.some((row) => asString(row.name) === "settled_at");
  if (cardSeqWasMissing || settledAtWasMissing) {
    transaction(db, () => {
      // `card_seq` 保持可空：NULL = 这一行没有持久序号（补列之前就落盘的旧行），未知与「已知的某个值」
      // 必须分得开。旧线上版本用的是内存序号加时间戳起点，序号可能已经到过 1.7e12 量级，这里绝不能
      // 预设一个小数字；旧行的高水位由首次预留时一次性 bootstrap（见 reserveQuestionCardSeq）。
      if (cardSeqWasMissing) {
        db.exec("ALTER TABLE questions ADD COLUMN card_seq INTEGER");
      }
      if (settledAtWasMissing) {
        db.exec("ALTER TABLE questions ADD COLUMN settled_at INTEGER");
      }
    });
  }
  // The former task-level relay table has no readers after per-thread listen policies replaced it.
  db.exec("DROP TABLE IF EXISTS thread_notify");
  db.exec(
    "CREATE INDEX IF NOT EXISTS outbox_stream ON outbox(bot_uid, task_id, stream_message_id, segment_index)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS questions_card ON questions(bot_uid, channel_type, channel_id, card_message_id)",
  );
  migrateProgressCards(db);
  migrateLoopStorage(db);
}

function migrateSchedulerOrder(db: Database): void {
  transaction(db, () => {
    const ingressRows = query(db, "SELECT id, created_at, schedule_order FROM ingress_jobs")
      .map(row => ({
        id: asNumber(row.id),
        at: asNumber(row.created_at),
        order: asNumber(row.schedule_order),
      }));
    const taskRows = query(db, "SELECT id, accepted_at, schedule_order FROM tasks")
      .map(row => ({
        id: asString(row.id),
        at: asNumber(row.accepted_at),
        order: asNumber(row.schedule_order),
      }));
    const linkedTasks = new Map<string, number>();
    for (const row of query(
      db,
      `SELECT id, consumer_id FROM ingress_jobs
       WHERE state = 'handed-off' AND consumer_kind = 'task' AND consumer_id IS NOT NULL
       ORDER BY id ASC`,
    )) {
      const taskId = asString(row.consumer_id);
      if (!linkedTasks.has(taskId)) linkedTasks.set(taskId, asNumber(row.id));
    }

    let maximum = 0;
    for (const row of ingressRows) maximum = Math.max(maximum, row.order);
    for (const row of taskRows) maximum = Math.max(maximum, row.order);
    const missing = ingressRows.some(row => row.order === 0)
      || taskRows.some(row => row.order === 0);
    if (missing) {
      const ingressById = new Map(ingressRows.map(row => [row.id, row]));
      const tasksById = new Map(taskRows.map(row => [row.id, row]));
      const events = [
        ...ingressRows
          .filter(row => row.order === 0)
          .map(row => ({ kind: "ingress" as const, id: row.id, at: row.at })),
        ...taskRows
          .filter(row => row.order === 0 && !linkedTasks.has(row.id))
          .map(row => ({ kind: "task" as const, id: row.id, at: row.at })),
      ];
      events.sort((left, right) =>
        left.at - right.at
        || (left.kind === right.kind ? 0 : left.kind === "ingress" ? -1 : 1)
        || String(left.id).localeCompare(String(right.id)),
      );
      let next = maximum === 0 ? 1 : maximum + 1;
      for (const event of events) {
        if (event.kind === "ingress") {
          const row = ingressById.get(event.id);
          if (row === undefined) continue;
          row.order = next;
          run(db, "UPDATE ingress_jobs SET schedule_order = ? WHERE id = ?", [next, event.id]);
        } else {
          const row = tasksById.get(event.id);
          if (row === undefined) continue;
          row.order = next;
          run(db, "UPDATE tasks SET schedule_order = ? WHERE id = ?", [next, event.id]);
        }
        next += 1;
      }
      for (const task of taskRows) {
        const ingressId = linkedTasks.get(task.id);
        const ingressOrder = ingressId === undefined ? undefined : ingressById.get(ingressId)?.order;
        if (ingressOrder !== undefined && task.order !== ingressOrder) {
          task.order = ingressOrder;
          run(db, "UPDATE tasks SET schedule_order = ? WHERE id = ?", [ingressOrder, task.id]);
        }
      }
    }

    const current = asNumber(query(db, "SELECT next_order FROM scheduler_order_state WHERE singleton = 1")[0]?.next_order);
    let maxOrder = 0;
    for (const row of ingressRows) maxOrder = Math.max(maxOrder, row.order);
    for (const row of taskRows) maxOrder = Math.max(maxOrder, row.order);
    const nextOrder = Math.max(current, maxOrder + 1, 1);
    run(db, "UPDATE scheduler_order_state SET next_order = ? WHERE singleton = 1", [nextOrder]);
  });
}

function migrateLoopStorage(db: Database): void {
  transaction(db, () => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS loop_bindings (
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        workspace_name TEXT NOT NULL,
        workspace_slug TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version >= 1),
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, group_id)
      );
      CREATE TABLE IF NOT EXISTS loop_task_snapshots (
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        task_json TEXT NOT NULL,
        business_json TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, group_id, workspace_id, task_id)
      );
      CREATE TABLE IF NOT EXISTS loop_inputs (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        binding_version INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('baseline', 'created', 'updated')),
        task_id TEXT,
        tasks_json TEXT NOT NULL,
        previous_json TEXT,
        state TEXT NOT NULL CHECK(state IN ('pending', 'intent', 'delivered', 'unknown', 'superseded')),
        target_channel_type INTEGER CHECK(target_channel_type IN (1, 2, 5)),
        target_channel_id TEXT,
        fault_notified INTEGER NOT NULL DEFAULT 0 CHECK(fault_notified IN (0, 1)),
        created_at INTEGER NOT NULL,
        CHECK((target_channel_type IS NULL) = (target_channel_id IS NULL))
      );
      CREATE INDEX IF NOT EXISTS loop_inputs_bot_sequence
        ON loop_inputs(bot_uid, sequence);
      CREATE INDEX IF NOT EXISTS loop_inputs_group_state
        ON loop_inputs(bot_uid, group_id, workspace_id, state, sequence);
      CREATE TABLE IF NOT EXISTS loop_task_routes (
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        thread_channel_id TEXT NOT NULL,
        version INTEGER NOT NULL CHECK(version >= 1),
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, group_id, workspace_id, task_id)
      );
      CREATE INDEX IF NOT EXISTS loop_task_routes_thread
        ON loop_task_routes(bot_uid, group_id, thread_channel_id, task_id);
      CREATE TABLE IF NOT EXISTS loop_thread_creations (
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        binding_version INTEGER NOT NULL CHECK(binding_version >= 1),
        route_version INTEGER NOT NULL CHECK(route_version >= 0),
        state TEXT NOT NULL CHECK(state IN ('reserved', 'allocated', 'ready', 'unknown', 'superseded')),
        thread_channel_id TEXT,
        error TEXT,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(bot_uid, group_id, workspace_id, task_id),
        UNIQUE(bot_uid, operation_id)
      );
      CREATE TABLE IF NOT EXISTS loop_thread_creation_attempts (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_uid TEXT NOT NULL,
        group_id TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        binding_version INTEGER NOT NULL CHECK(binding_version >= 1),
        route_version INTEGER NOT NULL CHECK(route_version >= 0),
        state TEXT NOT NULL CHECK(state IN ('reserved', 'allocated', 'ready', 'unknown', 'superseded')),
        thread_channel_id TEXT,
        error TEXT,
        updated_at INTEGER NOT NULL,
        UNIQUE(bot_uid, operation_id)
      );
      CREATE INDEX IF NOT EXISTS loop_thread_creation_attempts_key
        ON loop_thread_creation_attempts(bot_uid, group_id, workspace_id, task_id, sequence);
    `);
  });
}

function migrateWorkerLedgerNullableSessionFile(db: Database): void {
  const sessionFileColumn = query(db, "PRAGMA table_info(worker_ledger)")
    .find((row) => asString(row.name) === "session_file");
  if (!sessionFileColumn) throw new Error("worker ledger session_file column does not exist");
  if (asNumber(sessionFileColumn.notnull) === 0) return;

  transaction(db, () => {
    db.exec(`
      CREATE TABLE worker_ledger_nullable (
        id TEXT PRIMARY KEY NOT NULL,
        bot_uid TEXT NOT NULL,
        session_id TEXT NOT NULL,
        session_file TEXT,
        pid INTEGER NOT NULL,
        pgid INTEGER NOT NULL,
        identity TEXT NOT NULL,
        cwd TEXT NOT NULL,
        move_target_cwd TEXT,
        state TEXT NOT NULL CHECK(state IN ('running', 'stopping', 'stopped', 'unknown')),
        updated_at INTEGER NOT NULL
      );
      INSERT INTO worker_ledger_nullable(
        id, bot_uid, session_id, session_file, pid, pgid, identity, cwd,
        move_target_cwd, state, updated_at
      )
      SELECT
        id, bot_uid, session_id, session_file, pid, pgid, identity, cwd,
        move_target_cwd, state, updated_at
      FROM worker_ledger;
      DROP TABLE worker_ledger;
      ALTER TABLE worker_ledger_nullable RENAME TO worker_ledger;
    `);
  });
}

function migrateThreadForkPreparationStarted(db: Database): void {
  const columns = query(db, "PRAGMA table_info(thread_forks)");
  if (columns.some((row) => asString(row.name) === "preparation_started")) return;
  transaction(db, () => {
    db.exec(
      "ALTER TABLE thread_forks ADD COLUMN preparation_started INTEGER NOT NULL DEFAULT 0 CHECK(preparation_started IN (0, 1))",
    );
  });
}
// SQLite cannot widen CHECK constraints in place; preserve operation rows while rebuilding it.

function migrateThreadWorkspaceOperationPhases(db: Database): void {
  const sql = asString(
    query(
      db,
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'thread_workspace_operations'",
    )[0]?.sql,
  );
  if (sql.includes("'entry-create-intent'")) return;
  transaction(db, () => {
    db.exec(`
      CREATE TABLE thread_workspace_operations_phase_new (
        ${THREAD_WORKSPACE_OPERATION_COLUMNS}
      );
      INSERT INTO thread_workspace_operations_phase_new(
        id, bot_uid, channel_type, channel_id, group_id, tool_call_id, task_id,
        schedule_order, worktree_name_seed, projects_json, expected_workspace_json,
        expected_binding_json, candidate_workspace_json, candidate_binding_json,
        phase, status, worker_ledger_id, process_identity_json, error,
        cancel_requested, version, created_at, updated_at
      )
      SELECT
        id, bot_uid, channel_type, channel_id, group_id, tool_call_id, task_id,
        schedule_order, worktree_name_seed, projects_json, expected_workspace_json,
        expected_binding_json, candidate_workspace_json, candidate_binding_json,
        phase, status, worker_ledger_id, process_identity_json, error,
        cancel_requested, version, created_at, updated_at
      FROM thread_workspace_operations;
      DROP TABLE thread_workspace_operations;
      ALTER TABLE thread_workspace_operations_phase_new RENAME TO thread_workspace_operations;
      CREATE INDEX IF NOT EXISTS thread_workspace_operations_target
        ON thread_workspace_operations(bot_uid, channel_id, status, schedule_order);
    `);
  });
}

function migrateThreadListen(db: Database): void {
  const taskColumns = new Set(
    query(db, "PRAGMA table_info(tasks)").map((row) => asString(row.name)),
  );
  const legacyRelayColumns = [
    "relay_channel_type",
    "relay_channel_id",
    "relay_thread_name",
  ];
  const hasLegacyRelayColumns = legacyRelayColumns.some((column) => taskColumns.has(column));
  const suppressionMissing = !taskColumns.has("relay_suppressed");
  const eventColumns = new Set(
    query(db, "PRAGMA table_info(thread_merge_events)").map((row) => asString(row.name)),
  );
  const deliveryStateMissing = !eventColumns.has("delivery_state");
  const migrationDone = query(db, "SELECT 1 FROM meta WHERE key = ?", ["thread_listen_migration"])[0];
  if (!hasLegacyRelayColumns && !suppressionMissing && !deliveryStateMissing && migrationDone) return;

  transaction(db, () => {
    if (deliveryStateMissing) {
      db.exec("ALTER TABLE thread_merge_events ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'pending' CHECK(delivery_state IN ('pending', 'intent', 'delivered'))");
    }
    if (suppressionMissing) {
      db.exec("ALTER TABLE tasks ADD COLUMN relay_suppressed INTEGER NOT NULL DEFAULT 0 CHECK(relay_suppressed IN (0, 1))");
    }
    if (legacyRelayColumns.every((column) => taskColumns.has(column))) {
      run(db, `
        INSERT OR IGNORE INTO thread_listens(
          bot_uid, channel_id, creator_channel_type, creator_channel_id, thread_name, listen, merge_pending
        )
        SELECT bot_uid, channel_id, relay_channel_type, relay_channel_id,
               COALESCE(relay_thread_name, ''), 'merge', 0
        FROM tasks
        WHERE channel_type = 5
          AND relay_channel_type IN (2, 5)
          AND relay_channel_id IS NOT NULL
        ORDER BY accepted_at, id
      `);
    }
    for (const table of ["chats", "thread_workspaces"] as const) {
      for (const row of query(db,
        `SELECT bot_uid, channel_id FROM ${table} WHERE channel_type = 5 ORDER BY bot_uid, channel_id`,
      )) {
        const botUid = asString(row.bot_uid);
        const channelId = asString(row.channel_id);
        const creatorChannelId = threadParentGroupId(channelId);
        if (!creatorChannelId) continue;
        run(db,
          `INSERT OR IGNORE INTO thread_listens(
             bot_uid, channel_id, creator_channel_type, creator_channel_id, thread_name, listen, merge_pending
           ) VALUES(?, ?, 2, ?, '', 'merge', 0)`,
          [botUid, channelId, creatorChannelId],
        );
      }
    }
    for (const column of legacyRelayColumns) {
      if (taskColumns.has(column)) db.exec(`ALTER TABLE tasks DROP COLUMN ${column}`);
    }
    run(db,
      `INSERT INTO meta(key, value) VALUES('thread_listen_migration', '1')
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    );
  });
}

/**
 * 补列那一次的收尾：`prompt_started` 之前落盘的 `started` 行无法区分「prompt 已发出、会话里还在跑」
 * 与「刚 markStarted 还没发 prompt」，因此统一按「已派发但接受状态未知」记账。这里只写标记、绝不动
 * 任务状态与终态字段：开库可能由 CLI/status 触发，旧进程也许还在跑这些任务，开库本身不得打断它们。
 * 真正的结算发生在启动恢复（prepareTasksForRestart，service 拿到 Bot 锁并回收旧 worker 之后）：
 * 这类行会 fail-closed 成 interrupted 并作为 uncertain 交回服务说明原因。queued 行语义无歧义，
 * 仍按启动恢复流程重排执行。
 */
function markLegacyStartedUnconfirmed(db: Database): void {
  run(db, "UPDATE tasks SET prompt_started = 1, prompt_accepted = 0 WHERE state = 'started'");
}

export function migrateProgressCards(db: Database): void {
  const sql = asString(
    query(db, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'progress_cards'")[0]?.sql,
  );
  if (sql.includes("PRIMARY KEY(bot_uid, task_id, message_id)")) return;
  transaction(db, () => {
    db.exec(`
      ALTER TABLE progress_cards RENAME TO progress_cards_legacy;
      CREATE TABLE progress_cards (${PROGRESS_CARD_COLUMNS}
      );
      INSERT INTO progress_cards(
        bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
      )
      SELECT bot_uid, task_id, channel_type, channel_id, message_id, last_seq, snapshot, attempts, updated_at
      FROM progress_cards_legacy;
      DROP TABLE progress_cards_legacy;
    `);
  });
}

export function query(db: Database, sql: string, values: readonly SqlValue[] = []): SqlRow[] {
  return db.query(sql).all(...values) as SqlRow[];
}

export function run(db: Database, sql: string, values: readonly SqlValue[] = []): { changes: number } {
  const result = db.query(sql).run(...values) as { changes?: number };
  return { changes: result.changes ?? 0 };
}

const transactionDepth = new WeakMap<Database, number>();
let transactionSavepointId = 0;
export function transaction<T>(db: Database, work: () => T): T {
  const depth = transactionDepth.get(db) ?? 0;
  const savepoint = `store_tx_${++transactionSavepointId}`;
  db.exec(depth === 0 ? "BEGIN IMMEDIATE" : `SAVEPOINT ${savepoint}`);
  transactionDepth.set(db, depth + 1);
  try {
    const result = work();
    db.exec(depth === 0 ? "COMMIT" : `RELEASE SAVEPOINT ${savepoint}`);
    return result;
  } catch (error) {
    try {
      db.exec(depth === 0 ? "ROLLBACK" : `ROLLBACK TO SAVEPOINT ${savepoint}; RELEASE SAVEPOINT ${savepoint}`);
    } catch {
      // Preserve the original database failure.
    }
    throw error;
  } finally {
    if (depth === 0) transactionDepth.delete(db);
    else transactionDepth.set(db, depth);
  }
}
/** Allocates the next token in the durable order domain shared by ingress jobs and tasks. */
export function allocateScheduleOrder(db: Database): number {
  return transaction(db, () => {
    const row = query(db, "SELECT next_order FROM scheduler_order_state WHERE singleton = 1")[0];
    const order = asNumber(row?.next_order);
    if (!Number.isSafeInteger(order) || order < 1) {
      throw new Error("durable scheduler order is unavailable");
    }
    if (run(
      db,
      "UPDATE scheduler_order_state SET next_order = ? WHERE singleton = 1 AND next_order = ?",
      [order + 1, order],
    ).changes !== 1) {
      throw new Error("durable scheduler order changed unexpectedly");
    }
    return order;
  });
}


export function historyLedgerRow(db: Database, botUid: string, target: ChatTarget, sessionId: string): SqlRow | undefined {
  return query(db,
    `SELECT bot_uid, channel_type, channel_id, session_id, last_seq, phase, attempt_id, previous_seq
     FROM group_history_ledger
     WHERE bot_uid = ? AND ${targetWhere(target)} AND session_id = ?`,
    [botUid, ...targetValues(target), sessionId],
  )[0];
}

export function historyLedgerFromRow(row: SqlRow): HistoryLedgerRecord {
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    lastSeq: asNumber(row.last_seq),
    phase: row.phase === "committed" ? "committed" : "pending",
    attemptId: asString(row.attempt_id),
    previousSeq: row.previous_seq === null || row.previous_seq === undefined
      ? null
      : asNumber(row.previous_seq),
  };
}

export function contextLedgerFromRow(row: SqlRow): ContextLedgerRecord {
  return {
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 2 | 5,
    channelId: asString(row.channel_id),
    sessionId: asString(row.session_id),
    version: asNumber(row.version),
    phase: row.phase === "committed" ? "committed" : "pending",
    attemptId: asString(row.attempt_id),
    previousVersion: row.previous_version === null || row.previous_version === undefined
      ? null
      : asNumber(row.previous_version),
  };
}

export function taskFromRow(row: SqlRow): TaskRecord {
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    messageId: asString(row.message_id),
    senderId: asString(row.sender_id),
    text: asString(row.text),
    sessionId: asString(row.session_id),
    cwd: asString(row.cwd),
    state: asState(row.state),
    resultText: asNullableString(row.result_text),
    errorText: asNullableString(row.error_text),
    messageSeq: asNumber(row.message_seq),
    historyEligible: asNumber(row.history_eligible) !== 0,
    relaySuppressed: asNumber(row.relay_suppressed) !== 0,
  };
}

function mentionFromValue(value: unknown): TextMention | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  if (!Array.isArray(row.uids) || !Array.isArray(row.entities)) return null;
  if (!row.uids.every((uid) => typeof uid === "string")) return null;
  const entities: TextMention["entities"] = [];
  for (const value of row.entities) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return null;
    const entity = value as Record<string, unknown>;
    if (
      typeof entity.uid !== "string" ||
      typeof entity.offset !== "number" ||
      !Number.isSafeInteger(entity.offset) ||
      entity.offset < 0 ||
      typeof entity.length !== "number" ||
      !Number.isSafeInteger(entity.length) ||
      entity.length < 1
    ) {
      return null;
    }
    entities.push({ uid: entity.uid, offset: entity.offset, length: entity.length });
  }
  return { uids: [...row.uids] as string[], entities };
}

function mentionFromJson(value: unknown): TextMention | null {
  if (typeof value !== "string") return null;
  try {
    return mentionFromValue(JSON.parse(value) as unknown);
  } catch {
    return null;
  }
}

function createPayloadFromJson(value: unknown): OutboxTextPayload | null {
  if (typeof value !== "string") return null;
  try {
    const parsed: unknown = JSON.parse(value);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    const row = parsed as Record<string, unknown>;
    if (typeof row.text !== "string") return null;
    return { text: row.text, mention: mentionFromValue(row.mention) };
  } catch {
    return null;
  }
}

export function outboxFromRow(row: SqlRow): OutboxRecord {
  const kind = row.kind === "file" ? "file" : "text";
  const fileKind = row.file_kind === "image" || row.file_kind === "file" ? row.file_kind : null;
  const state = row.state === "sent" || row.state === "unknown" || row.state === "dead" ? row.state : "pending";
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    taskId: asNullableString(row.task_id),
    kind,
    text: asNullableString(row.text),
    path: asNullableString(row.path),
    fileKind,
    clientMsgNo: asString(row.client_msg_no),
    state,
    attempts: asNumber(row.attempts),
    lastError: asNullableString(row.last_error),
    remoteMessageId: asNullableString(row.remote_message_id),
    streamMessageId: asNullableString(row.stream_message_id),
    segmentIndex: asNumber(row.segment_index),
    desiredVersion: asNumber(row.desired_version),
    ackedVersion: asNumber(row.acked_version),
    mention: mentionFromJson(row.mention_json),
    streamFinal: asNumber(row.stream_final) !== 0,
    createPayload: createPayloadFromJson(row.create_payload_json),
  };
}

export function questionFromRow(row: SqlRow): QuestionRecord {
  const state = row.state === "answered" || row.state === "cancelled" || row.state === "expired" ? row.state : "pending";
  return {
    id: asString(row.id),
    taskId: asString(row.task_id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    payload: asString(row.payload),
    state,
    answer: asNullableString(row.answer),
    answeredBy: asNullableString(row.answered_by),
    cardMessageId: asNullableString(row.card_message_id),
    createdAt: asNumber(row.created_at),
    // 关闭原因按白名单读取；NULL、未知取值与旧库里的其它文本一律按 null（调用方按 ended 处理）。
    closedReason: row.closed_reason === "cancelled" || row.closed_reason === "interrupted" || row.closed_reason === "ended"
      ? row.closed_reason
      : null,
    settledAt: row.settled_at === null || row.settled_at === undefined ? null : asNumber(row.settled_at),
  };
}
