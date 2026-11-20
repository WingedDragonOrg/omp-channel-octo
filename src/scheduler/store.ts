import { Database } from "bun:sqlite";
import type { ChatTarget, WorkerProcess } from "../contracts.ts";
import {
  asNumber,
  asNullableString,
  asString,
  query,
  run,
  targetValues,
  transaction,
  type SqlRow,
} from "../store/store-schema.ts";
import type {
  ScheduleArtifact,
  ScheduleCreateResult,
  ScheduleOccurrenceResult,
  ScheduleRecord,
  ScheduleRunRecord,
  ScheduleRunSettlement,
  ScheduleStatus,
} from "./types.ts";

const ACTIVE_RUNS_SQL = "status IN ('claimed', 'dispatching', 'running') OR (status = 'unknown' AND process_exited = 0)";
const SCHEDULE_BUSY_SQL = "status IN ('claimed', 'dispatching', 'running', 'unknown')";
const MAX_ARTIFACTS_PER_RUN = 32;
const MAX_ARTIFACTS_JSON_BYTES = 32 * 1024;
const MAX_RUN_TEXT_CHARS = 8_192;

type StoredArtifact = ScheduleArtifact & { toolCallId: string };

function parseJson(value: unknown): unknown {
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return null;
  }
}

function chatTarget(channelType: unknown, channelId: unknown): ChatTarget {
  const type = asNumber(channelType);
  if (type !== 1 && type !== 2 && type !== 5) throw new Error("schedule database contains an invalid target type");
  const id = asString(channelId);
  if (!id) throw new Error("schedule database contains an empty target id");
  return { channelType: type, channelId: id };
}

function scheduleSpecFromJson(value: unknown): ScheduleRecord["spec"] {
  const parsed = parseJson(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("schedule specification is corrupt");
  const spec = parsed as Record<string, unknown>;
  if (spec.kind === "agent" && typeof spec.prompt === "string") return { kind: "agent", prompt: spec.prompt };
  if (spec.kind === "script" && spec.runner && typeof spec.runner === "object" && !Array.isArray(spec.runner)) {
    const runner = spec.runner as Record<string, unknown>;
    if (runner.kind === "shell" && typeof runner.script === "string") {
      return { kind: "script", runner: { kind: "shell", script: runner.script } };
    }
    if (
      runner.kind === "exec"
      && typeof runner.command === "string"
      && Array.isArray(runner.args)
      && runner.args.every((arg) => typeof arg === "string")
    ) {
      return { kind: "script", runner: { kind: "exec", command: runner.command, args: runner.args as string[] } };
    }
  }
  throw new Error("schedule specification is corrupt");
}

function processFromJson(value: unknown): WorkerProcess | null {
  const parsed = parseJson(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const row = parsed as Record<string, unknown>;
  if (
    !Number.isSafeInteger(row.pid)
    || !Number.isSafeInteger(row.pgid)
    || typeof row.identity !== "string"
    || typeof row.cwd !== "string"
  ) return null;
  return { pid: row.pid as number, pgid: row.pgid as number, identity: row.identity, cwd: row.cwd };
}

function artifactFromValue(value: unknown): StoredArtifact | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  if (
    typeof row.toolCallId !== "string"
    || typeof row.name !== "string"
    || typeof row.type !== "string"
  ) return null;
  let target: ChatTarget;
  try {
    target = chatTarget(row.target && (row.target as Record<string, unknown>).channelType, row.target && (row.target as Record<string, unknown>).channelId);
  } catch {
    return null;
  }
  const path = typeof row.path === "string" ? row.path : undefined;
  const remoteRef = typeof row.remoteRef === "string" ? row.remoteRef : undefined;
  if ((path === undefined) === (remoteRef === undefined)) return null;
  return {
    name: row.name,
    type: row.type,
    target,
    ...(path === undefined ? {} : { path }),
    ...(remoteRef === undefined ? {} : { remoteRef }),
    ...(typeof row.receipt === "string" ? { receipt: row.receipt } : {}),
    toolCallId: row.toolCallId,
  };
}

function artifactsFromJson(value: unknown): StoredArtifact[] {
  const parsed = parseJson(value);
  if (!Array.isArray(parsed)) return [];
  return parsed.map(artifactFromValue).filter((artifact): artifact is StoredArtifact => artifact !== null);
}

function scheduleFromRow(row: SqlRow): ScheduleRecord {
  const sourceKind = row.source_kind === "direct" ? "direct" : "synthetic";
  const status = row.status === "paused" || row.status === "stopped" ? row.status : "active";
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    name: asString(row.name),
    cron: asString(row.cron),
    timezone: asString(row.timezone),
    cwd: asString(row.cwd),
    spec: scheduleSpecFromJson(row.spec_json),
    source: chatTarget(row.source_channel_type, row.source_channel_id),
    sourceMessageId: asString(row.source_message_id),
    sourceRequesterUid: asNullableString(row.source_requester_uid),
    sourceKind,
    sourceTaskId: asString(row.source_task_id),
    toolCallId: asString(row.tool_call_id),
    target: chatTarget(row.target_channel_type, row.target_channel_id),
    timeoutMs: asNumber(row.timeout_ms),
    createdAt: asNumber(row.created_at),
    status,
  };
}

function runFromRow(row: SqlRow): ScheduleRunRecord {
  const status = row.status === "claimed"
    || row.status === "dispatching"
    || row.status === "running"
    || row.status === "skipped"
    || row.status === "cancelled"
    || row.status === "success"
    || row.status === "unknown"
    ? row.status
    : "failed";
  const stage = row.stage === "dispatch-intent"
    || row.stage === "accepted"
    || row.stage === "process-exited"
    || row.stage === "settled"
    ? row.stage
    : "claimed";
  return {
    id: asString(row.id),
    scheduleId: asString(row.schedule_id),
    botUid: asString(row.bot_uid),
    scheduledAt: asNumber(row.scheduled_at),
    target: chatTarget(row.target_channel_type, row.target_channel_id),
    createdAt: asNumber(row.created_at),
    startedAt: row.started_at === null || row.started_at === undefined ? null : asNumber(row.started_at),
    endedAt: row.ended_at === null || row.ended_at === undefined ? null : asNumber(row.ended_at),
    status,
    stage,
    stopRequested: asNumber(row.stop_requested) !== 0,
    promptAccepted: asNumber(row.prompt_accepted) !== 0,
    process: processFromJson(row.process_json),
    processExited: asNumber(row.process_exited) !== 0,
    sessionId: asNullableString(row.session_id),
    sessionFile: asNullableString(row.session_file),
    resultText: asNullableString(row.result_text),
    errorText: asNullableString(row.error_text),
    stdout: asNullableString(row.stdout),
    stderr: asNullableString(row.stderr),
    skippedReason: asNullableString(row.skipped_reason),
    artifacts: artifactsFromJson(row.artifacts_json).map(({ toolCallId: _toolCallId, ...artifact }) => artifact),
  };
}

function getScheduleById(db: Database, id: string): ScheduleRecord | null {
  const row = query(db, "SELECT * FROM schedules WHERE id = ?", [id])[0];
  return row ? scheduleFromRow(row) : null;
}

function getRunById(db: Database, id: string): ScheduleRunRecord | null {
  const row = query(db, "SELECT * FROM schedule_runs WHERE id = ?", [id])[0];
  return row ? runFromRow(row) : null;
}

function sameTarget(left: ChatTarget, right: ChatTarget): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}

function boundedText(value: string | null | undefined): string | null {
  if (value === undefined || value === null) return null;
  return value.slice(0, MAX_RUN_TEXT_CHARS);
}

export function createSchedule(db: Database, record: ScheduleRecord): ScheduleCreateResult {
  return transaction(db, () => {
    const sourceTask = query(
      db,
      `SELECT bot_uid, channel_type, channel_id, message_id, sender_id, schedule_create_identity_mixed
       FROM tasks WHERE id = ?`,
      [record.sourceTaskId],
    )[0];
    if (
      sourceTask === undefined
      || asString(sourceTask.bot_uid) !== record.botUid
      || asNumber(sourceTask.channel_type) !== record.source.channelType
      || asString(sourceTask.channel_id) !== record.source.channelId
      || asString(sourceTask.message_id) !== record.sourceMessageId
      || (record.sourceKind === "direct"
        && (record.sourceRequesterUid === null || asString(sourceTask.sender_id) !== record.sourceRequesterUid))
      || (record.sourceKind === "synthetic" && record.sourceRequesterUid !== null)
    ) {
      throw new Error("permission denied: schedule source task no longer matches its authenticated provenance");
    }
    if (asNumber(sourceTask.schedule_create_identity_mixed) !== 0) {
      throw new Error("permission denied: schedule creation is disabled after steering from another or unverifiable identity; retry with an independent message");
    }
    const existing = query(db,
      "SELECT * FROM schedules WHERE bot_uid = ? AND source_task_id = ? AND tool_call_id = ?",
      [record.botUid, record.sourceTaskId, record.toolCallId],
    )[0];
    if (existing) return { kind: "duplicate", record: scheduleFromRow(existing) };
    run(db,
      `INSERT INTO schedules(
        id, bot_uid, name, cron, timezone, cwd, spec_json,
        source_channel_type, source_channel_id, source_message_id, source_requester_uid, source_kind,
        source_task_id, tool_call_id, target_channel_type, target_channel_id, timeout_ms, created_at, status
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.id,
        record.botUid,
        record.name,
        record.cron,
        record.timezone,
        record.cwd,
        JSON.stringify(record.spec),
        record.source.channelType,
        record.source.channelId,
        record.sourceMessageId,
        record.sourceRequesterUid,
        record.sourceKind,
        record.sourceTaskId,
        record.toolCallId,
        record.target.channelType,
        record.target.channelId,
        record.timeoutMs,
        record.createdAt,
        record.status,
      ],
    );
    const inserted = getScheduleById(db, record.id);
    if (!inserted) throw new Error("schedule insert did not persist");
    return { kind: "created", record: inserted };
  });
}

export function getSchedule(db: Database, botUid: string, id: string): ScheduleRecord | null {
  const row = query(db, "SELECT * FROM schedules WHERE bot_uid = ? AND id = ?", [botUid, id])[0];
  return row ? scheduleFromRow(row) : null;
}

export function listSchedules(db: Database, botUid: string, source: ChatTarget, limit: number, offset: number): ScheduleRecord[] {
  return query(db,
    `SELECT * FROM schedules WHERE bot_uid = ? AND source_channel_type = ? AND source_channel_id = ?
     ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`,
    [botUid, ...targetValues(source), Math.max(1, Math.min(100, Math.trunc(limit))), Math.max(0, Math.trunc(offset))],
  ).map(scheduleFromRow);
}

export function listActiveSchedules(db: Database, botUid: string): ScheduleRecord[] {
  return query(db,
    "SELECT * FROM schedules WHERE bot_uid = ? AND status = 'active' ORDER BY created_at ASC, id ASC",
    [botUid],
  ).map(scheduleFromRow);
}

export function setScheduleStatus(
  db: Database,
  botUid: string,
  id: string,
  source: ChatTarget,
  status: ScheduleStatus,
  now: number,
): ScheduleRecord | null {
  return transaction(db, () => {
    const row = query(db,
      `SELECT * FROM schedules WHERE bot_uid = ? AND id = ?
       AND source_channel_type = ? AND source_channel_id = ?`,
      [botUid, id, ...targetValues(source)],
    )[0];
    if (!row) return null;
    const current = scheduleFromRow(row);
    const next = current.status === "stopped"
      ? "stopped"
      : status;
    run(db,
      "UPDATE schedules SET status = ?, stopped_at = CASE WHEN ? = 'stopped' THEN COALESCE(stopped_at, ?) ELSE stopped_at END WHERE bot_uid = ? AND id = ?",
      [next, next, now, botUid, id],
    );
    if (next === "stopped") {
      run(db,
        `UPDATE schedule_runs SET stop_requested = 1
         WHERE schedule_id = ? AND (${ACTIVE_RUNS_SQL})`,
        [id],
      );
    }
    return getScheduleById(db, id);
  });
}

export function claimScheduleOccurrence(
  db: Database,
  scheduleId: string,
  scheduledAt: number,
  now: number,
  maxConcurrent: number,
): ScheduleOccurrenceResult {
  return transaction(db, () => {
    const schedule = getScheduleById(db, scheduleId);
    if (!schedule || schedule.status !== "active") return { kind: "inactive", run: null };
    const existing = query(db,
      "SELECT * FROM schedule_runs WHERE schedule_id = ? AND scheduled_at = ?",
      [scheduleId, scheduledAt],
    )[0];
    if (existing) return { kind: "duplicate", run: runFromRow(existing) };
    const scheduleBusy = asNumber(query(db,
      `SELECT COUNT(*) AS count FROM schedule_runs WHERE schedule_id = ? AND (${SCHEDULE_BUSY_SQL})`,
      [scheduleId],
    )[0]?.count) > 0;
    const globalBusy = asNumber(query(db,
      `SELECT COUNT(*) AS count FROM schedule_runs WHERE bot_uid = ? AND (${ACTIVE_RUNS_SQL})`,
      [schedule.botUid],
    )[0]?.count);
    const max = Math.max(0, Math.trunc(maxConcurrent));
    const skippedReason = scheduleBusy
      ? "previous occurrence has not settled"
      : globalBusy >= max
        ? "global schedule concurrency limit reached"
        : null;
    const id = crypto.randomUUID();
    run(db,
      `INSERT INTO schedule_runs(
        id, schedule_id, bot_uid, scheduled_at, target_channel_type, target_channel_id,
        created_at, status, stage, ended_at, skipped_reason
      ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id,
        schedule.id,
        schedule.botUid,
        scheduledAt,
        schedule.target.channelType,
        schedule.target.channelId,
        now,
        skippedReason === null ? "claimed" : "skipped",
        skippedReason === null ? "claimed" : "settled",
        skippedReason === null ? null : now,
        skippedReason,
      ],
    );
    const created = getRunById(db, id);
    if (!created) throw new Error("schedule occurrence insert did not persist");
    return skippedReason === null
      ? { kind: "claimed", run: created }
      : { kind: "skipped", run: created };
  });
}

export function beginScheduleDispatch(db: Database, runId: string, now: number): boolean {
  return transaction(db, () => {
    const record = getRunById(db, runId);
    if (!record || record.status !== "claimed" || record.stopRequested) return false;
    const schedule = getScheduleById(db, record.scheduleId);
    if (!schedule || schedule.status !== "active") return false;
    return run(db,
      `UPDATE schedule_runs SET status = 'dispatching', stage = 'dispatch-intent', started_at = ?
       WHERE id = ? AND status = 'claimed' AND stop_requested = 0`,
      [now, runId],
    ).changes === 1;
  });
}

export function setScheduleProcess(db: Database, runId: string, process: WorkerProcess): boolean {
  return transaction(db, () => {
    const record = getRunById(db, runId);
    if (!record || (record.status !== "dispatching" && record.status !== "running")) return false;
    if (record.process && !record.processExited && record.process.identity !== process.identity) return false;
    return run(db,
      `UPDATE schedule_runs SET process_json = ?, process_exited = 0, status = 'running'
       WHERE id = ? AND status IN ('dispatching', 'running')`,
      [JSON.stringify(process), runId],
    ).changes === 1;
  });
}

export function setScheduleAgentSession(db: Database, runId: string, sessionId: string, sessionFile: string): boolean {
  if (!sessionId || !sessionFile) return false;
  return transaction(db, () => {
    const record = getRunById(db, runId);
    if (!record || (record.status !== "dispatching" && record.status !== "running")) return false;
    if ((record.sessionId !== null && record.sessionId !== sessionId)
      || (record.sessionFile !== null && record.sessionFile !== sessionFile)) return false;
    return run(db,
      `UPDATE schedule_runs SET session_id = ?, session_file = ?
       WHERE id = ? AND status IN ('dispatching', 'running')`,
      [sessionId, sessionFile, runId],
    ).changes === 1;
  });
}

export function markSchedulePromptAccepted(db: Database, runId: string, _now: number): boolean {
  return run(db,
    `UPDATE schedule_runs SET prompt_accepted = 1, stage = 'accepted'
     WHERE id = ? AND status IN ('dispatching', 'running')`,
    [runId],
  ).changes === 1;
}

export function markScheduleProcessExited(db: Database, runId: string, _now: number): boolean {
  return run(db,
    `UPDATE schedule_runs SET process_exited = 1, stage = 'process-exited'
     WHERE id = ? AND process_json IS NOT NULL AND process_exited = 0`,
    [runId],
  ).changes === 1;
}

export function addScheduleArtifact(
  db: Database,
  runId: string,
  artifact: ScheduleArtifact,
  toolCallId: string,
  _now: number,
): boolean {
  if (!toolCallId.trim() || !artifact.name.trim() || !artifact.type.trim()) return false;
  if ((typeof artifact.path === "string") === (typeof artifact.remoteRef === "string")) return false;
  return transaction(db, () => {
    const runRecord = query(db,
      "SELECT * FROM schedule_runs WHERE id = ? AND status IN ('dispatching', 'running') AND stop_requested = 0",
      [runId],
    )[0];
    if (!runRecord) return false;
    const schedule = getScheduleById(db, asString(runRecord.schedule_id));
    if (!schedule || !sameTarget(schedule.target, artifact.target)) return false;
    const rows = artifactsFromJson(runRecord.artifacts_json);
    const existing = rows.find((row) => row.toolCallId === toolCallId);
    const candidate: StoredArtifact = { ...artifact, toolCallId };
    if (existing) return JSON.stringify(existing) === JSON.stringify(candidate);
    if (rows.length >= MAX_ARTIFACTS_PER_RUN) return false;
    const next = [...rows, candidate];
    const json = JSON.stringify(next);
    if (new TextEncoder().encode(json).byteLength > MAX_ARTIFACTS_JSON_BYTES) return false;
    return run(db, "UPDATE schedule_runs SET artifacts_json = ? WHERE id = ? AND stop_requested = 0", [json, runId]).changes === 1;
  });
}

export function settleScheduleRun(
  db: Database,
  runId: string,
  settlement: ScheduleRunSettlement,
  now: number,
): ScheduleRunRecord | null {
  return transaction(db, () => {
    const record = getRunById(db, runId);
    if (!record) return null;
    if (record.stage === "settled") return record;
    const schedule = getScheduleById(db, record.scheduleId);
    const stopped = record.stopRequested || schedule?.status === "stopped";
    const processUnconfirmed = record.process !== null && !record.processExited;
    const status = settlement.status === "unknown" || processUnconfirmed
      ? "unknown"
      : stopped
        ? "cancelled"
        : settlement.status;
    run(db,
      `UPDATE schedule_runs SET status = ?, stage = 'settled', ended_at = ?,
       result_text = ?, error_text = ?, stdout = ?, stderr = ? WHERE id = ? AND stage != 'settled'`,
      [
        status,
        now,
        boundedText(settlement.resultText),
        boundedText(settlement.errorText),
        boundedText(settlement.stdout),
        boundedText(settlement.stderr),
        runId,
      ],
    );
    return getRunById(db, runId);
  });
}

export function getScheduleRun(db: Database, botUid: string, runId: string): ScheduleRunRecord | null {
  const row = query(db, "SELECT * FROM schedule_runs WHERE bot_uid = ? AND id = ?", [botUid, runId])[0];
  return row ? runFromRow(row) : null;
}

export function listScheduleRuns(db: Database, botUid: string, scheduleId: string, limit: number, offset: number): ScheduleRunRecord[] {
  return query(db,
    `SELECT * FROM schedule_runs WHERE bot_uid = ? AND schedule_id = ?
     ORDER BY scheduled_at DESC, id DESC LIMIT ? OFFSET ?`,
    [botUid, scheduleId, Math.max(1, Math.min(100, Math.trunc(limit))), Math.max(0, Math.trunc(offset))],
  ).map(runFromRow);
}

export function listRecoverableScheduleRuns(db: Database, botUid: string): ScheduleRunRecord[] {
  return query(db,
    `SELECT * FROM schedule_runs WHERE bot_uid = ?
     AND (status IN ('claimed', 'dispatching', 'running') OR (status = 'unknown' AND process_exited = 0))
     ORDER BY created_at ASC, id ASC`,
    [botUid],
  ).map(runFromRow);
}
export function listScheduleRunsNeedingStop(db: Database, botUid: string, scheduleId: string): ScheduleRunRecord[] {
  return query(db,
    `SELECT * FROM schedule_runs WHERE bot_uid = ? AND schedule_id = ?
     AND (
       status IN ('claimed', 'dispatching', 'running')
       OR (status = 'unknown' AND (process_exited = 0 OR process_json IS NULL))
     )
     ORDER BY created_at ASC, id ASC`,
    [botUid, scheduleId],
  ).map(runFromRow);
}
