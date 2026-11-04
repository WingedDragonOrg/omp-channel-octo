import { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import type { InboundClaim, ReviewJobRecord } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, targetValues, targetWhere, transaction } from "./store-schema.ts";

function fromRow(row: Record<string, unknown>): ReviewJobRecord {
  const state = asString(row.state);
  return {
    id: asString(row.id),
    botUid: asString(row.bot_uid),
    channelType: asNumber(row.channel_type) as 1 | 2 | 5,
    channelId: asString(row.channel_id),
    messageId: asString(row.message_id),
    senderId: asString(row.sender_id),
    focus: asString(row.focus),
    projects: (() => {
      try {
        const parsed = JSON.parse(asString(row.projects_json)) as unknown;
        if (!Array.isArray(parsed)) return [];
        return parsed.flatMap((item) => item && typeof item === "object"
          && "name" in item && typeof item.name === "string"
          && "path" in item && typeof item.path === "string"
          ? [{ name: item.name, path: item.path }]
          : []);
      } catch {
        return [];
      }
    })(),
    cwd: asString(row.cwd),
    sessionId: asString(row.session_id),
    sessionFile: asNullableString(row.session_file),
    state: state === "accepted" || state === "running" || state === "completed" || state === "failed" || state === "stopped" ? state : "failed",
    dispatchStarted: asNumber(row.dispatch_started) !== 0,
    promptAccepted: asNumber(row.prompt_accepted) !== 0,
    stopRequested: asNumber(row.stop_requested) !== 0,
    resultText: asNullableString(row.result_text),
    errorText: asNullableString(row.error_text),
  };
}

export function listTerminalReviewJobs(db: Database, botUid: string): ReviewJobRecord[] {
  return query(db, "SELECT * FROM review_jobs WHERE bot_uid=? AND state IN ('completed','failed','stopped')", [botUid]).map(fromRow);
}

/** Atomically converts a claimed `/review` command into an independently recoverable review job. */
export function acceptReviewJob(db: Database, claim: InboundClaim, job: ReviewJobRecord): boolean {
  return transaction(db, () => {
    if (query(db, `SELECT 1 FROM review_jobs WHERE bot_uid = ? AND ${targetWhere(job)} AND state IN ('accepted','running')`, [job.botUid, ...targetValues(job)])[0]) return false;
    const converted = run(db, `UPDATE inbound SET kind='command', command_state='completed', accepted_at=?
      WHERE bot_uid=? AND channel_type=? AND channel_id=? AND message_id=? AND kind='command' AND command_state='pending'`,
    [Date.now(), claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId]).changes === 1;
    if (!converted) return false;
    run(db, `INSERT INTO review_jobs(id,bot_uid,channel_type,channel_id,message_id,sender_id,focus,projects_json,cwd,session_id,session_file,state,dispatch_started,prompt_accepted,stop_requested,accepted_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,NULL,'accepted',0,0,0,?)`, [job.id, job.botUid, job.channelType, job.channelId, job.messageId, job.senderId, job.focus, JSON.stringify(job.projects), job.cwd, job.sessionId, Date.now()]);
    return true;
  });
}

export function getActiveReviewJob(db: Database, botUid: string, target: ChatTarget): ReviewJobRecord | null {
  const row = query(db, `SELECT * FROM review_jobs WHERE bot_uid=? AND ${targetWhere(target)} AND state IN ('accepted','running') ORDER BY accepted_at LIMIT 1`, [botUid, ...targetValues(target)])[0];
  return row ? fromRow(row) : null;
}
export function getReviewJob(db: Database, id: string): ReviewJobRecord | null {
  const row = query(db, "SELECT * FROM review_jobs WHERE id=?", [id])[0];
  return row ? fromRow(row) : null;
}
export function listRecoverableReviewJobs(db: Database, botUid: string): ReviewJobRecord[] {
  return query(db, "SELECT * FROM review_jobs WHERE bot_uid=? AND state IN ('accepted','running') ORDER BY accepted_at", [botUid]).map(fromRow);
}

/** Releases only orphaned pending `/review` claims so transport redelivery can be accepted after a crash. */
export function releaseOrphanedReviewClaims(db: Database, botUid: string): void {
  transaction(db, () => {
    const rows = query(db, `SELECT channel_type,channel_id,message_id,text FROM inbound
      WHERE bot_uid=? AND kind='command' AND command_state='pending'`, [botUid]);
    for (const row of rows) {
      if (!/^\/review(?:\s|$)/iu.test(asString(row.text).trim())) continue;
      const linked = query(db, `SELECT 1 FROM review_jobs
        WHERE bot_uid=? AND channel_type=? AND channel_id=? AND message_id=?`,
      [botUid, asNumber(row.channel_type), asString(row.channel_id), asString(row.message_id)])[0];
      if (linked !== undefined) continue;
      run(db, `DELETE FROM inbound WHERE bot_uid=? AND channel_type=? AND channel_id=?
        AND message_id=? AND kind='command' AND command_state='pending'`,
      [botUid, asNumber(row.channel_type), asString(row.channel_id), asString(row.message_id)]);
    }
  });
}
export function prepareReviewJobsForRestart(db: Database, botUid: string): { runnable: ReviewJobRecord[]; uncertain: ReviewJobRecord[]; stopped: ReviewJobRecord[] } {
  return transaction(db, () => {
    run(db, "UPDATE review_jobs SET state='stopped', finished_at=?, error_text='review stop requested before restart' WHERE bot_uid=? AND state IN ('accepted','running') AND stop_requested=1 AND id NOT IN (SELECT id FROM worker_ledger WHERE state != 'stopped')", [Date.now(), botUid]);
    run(db, "UPDATE review_jobs SET state='failed', finished_at=?, error_text='review dispatch acceptance was not confirmed' WHERE bot_uid=? AND state='running' AND dispatch_started=1 AND prompt_accepted=0 AND stop_requested=0 AND id NOT IN (SELECT id FROM worker_ledger WHERE state != 'stopped')", [Date.now(), botUid]);
    const stopped = query(db, "SELECT * FROM review_jobs WHERE bot_uid=? AND state='stopped' AND recovery_reported=0", [botUid]).map(fromRow);
    const uncertain = query(db, "SELECT * FROM review_jobs WHERE bot_uid=? AND state='failed' AND error_text='review dispatch acceptance was not confirmed' AND recovery_reported=0", [botUid]).map(fromRow);
    // Recovery notices become reported only in the same transaction that persists their outbox rows.
    run(db, "UPDATE review_jobs SET state='accepted' WHERE bot_uid=? AND state='running' AND dispatch_started=0 AND id NOT IN (SELECT id FROM worker_ledger WHERE state != 'stopped')", [botUid]);
    return {
      runnable: listRecoverableReviewJobs(db, botUid).filter(job =>
        !job.stopRequested
        && query(db, "SELECT 1 FROM worker_ledger WHERE id=? AND state != 'stopped'", [job.id])[0] === undefined),
      uncertain,
      stopped,
    };
  });
}

export function persistReviewRecoveryNotices(
  db: Database,
  botUid: string,
  entries: readonly { job: ReviewJobRecord; text: string; kind: "uncertain" | "stopped" }[],
): void {
  transaction(db, () => {
    for (const entry of entries) {
      const id = `review-recovery:${entry.kind}:${entry.job.id}`;
      const payload = JSON.stringify({ text: entry.text, mention: null });
      run(db, `INSERT OR IGNORE INTO outbox(
        id,bot_uid,channel_type,channel_id,task_id,kind,text,path,file_kind,client_msg_no,
        state,create_payload_json,created_at
      ) VALUES(?,?,?,?,?,'text',?,NULL,NULL,?,'pending',?,?)`, [
        id, botUid, entry.job.channelType, entry.job.channelId, entry.job.id,
        entry.text, id, payload, Date.now(),
      ]);
      run(db, "UPDATE review_jobs SET recovery_reported=1 WHERE id=? AND bot_uid=?", [entry.job.id, botUid]);
    }
  });
}
export function markReviewStarted(db: Database, id: string): boolean { return run(db, "UPDATE review_jobs SET state='running', started_at=? WHERE id=? AND state='accepted'", [Date.now(), id]).changes === 1; }
export function bindReviewSession(db: Database, id: string, sessionId: string, sessionFile: string): void { run(db, "UPDATE review_jobs SET session_id=?,session_file=? WHERE id=? AND state='running'", [sessionId, sessionFile, id]); }
export function markReviewDispatchStarted(db: Database, id: string): void { if (run(db, "UPDATE review_jobs SET dispatch_started=1,prompt_accepted=0 WHERE id=? AND state='running' AND dispatch_started=0", [id]).changes !== 1) throw new Error("review prompt was already dispatched"); }
export function markReviewPromptAccepted(db: Database, id: string): void { if (run(db, "UPDATE review_jobs SET prompt_accepted=1 WHERE id=? AND dispatch_started=1", [id]).changes !== 1) throw new Error("review prompt has no dispatch intent"); }
export function requestReviewStop(db: Database, id: string): boolean { return run(db, "UPDATE review_jobs SET stop_requested=1 WHERE id=? AND state IN ('accepted','running')", [id]).changes === 1; }
export function requestReviewStopForClaim(db: Database, claim: InboundClaim, id: string): boolean {
  return transaction(db, () => {
    const stopped = run(db, "UPDATE review_jobs SET stop_requested=1 WHERE id=? AND state IN ('accepted','running')", [id]).changes === 1;
    if (!stopped) return false;
    const completed = run(db, `UPDATE inbound SET command_state='completed'
      WHERE bot_uid=? AND channel_type=? AND channel_id=? AND message_id=?
        AND kind='command' AND command_state='pending'`,
    [claim.botUid, claim.target.channelType, claim.target.channelId, claim.messageId]).changes === 1;
    if (!completed) throw new Error("review stop command claim is not pending");
    return true;
  });
}
export function markReviewCompleted(db: Database, id: string, text: string | null): void { run(db, "UPDATE review_jobs SET state='completed',result_text=?,error_text=NULL,finished_at=? WHERE id=? AND state IN ('accepted','running')", [text,Date.now(),id]); }
export function markReviewFailed(db: Database, id: string, error: string): void { run(db, "UPDATE review_jobs SET state='failed',error_text=?,result_text=NULL,finished_at=? WHERE id=? AND state IN ('accepted','running')", [error,Date.now(),id]); }
export function markReviewStopped(db: Database, id: string): void { run(db, "UPDATE review_jobs SET state='stopped',error_text='review stopped',recovery_reported=0,finished_at=? WHERE id=? AND state IN ('accepted','running')", [Date.now(),id]); }
