import type { Database } from "bun:sqlite";
import type { ChatTarget } from "../contracts.ts";
import { recvIngressSourceId } from "../inbound/ingress-types.ts";
import { canonicalReferenceUrl, matchingReferenceCandidateUrls } from "../references/links.ts";
import type { ThreadReference, ThreadReferences } from "../references/types.ts";
import type { ReferenceHistoryEntry, ReferenceHistorySnapshot } from "./store-records.ts";
import { asNullableString, asNumber, asString, query, run, transaction } from "./store-schema.ts";

function validThreadIdentity(botUid: string, target: ChatTarget): boolean {
  return botUid.length > 0 && target.channelType === 5 && target.channelId.length > 0;
}

export function captureReferenceHistorySnapshot(
  db: Database,
  botUid: string,
  target: ChatTarget,
): ReferenceHistorySnapshot {
  const valid = validThreadIdentity(botUid, target);
  const inboundRowId = valid
    ? asNumber(query(db, "SELECT COALESCE(MAX(rowid), 0) AS row_id FROM inbound")[0]?.row_id)
    : 0;
  const outboxRowId = valid
    ? asNumber(query(db, "SELECT COALESCE(MAX(rowid), 0) AS row_id FROM outbox")[0]?.row_id)
    : 0;
  const ingressJobId = valid
    ? asNumber(query(db, "SELECT COALESCE(MAX(id), 0) AS row_id FROM ingress_jobs")[0]?.row_id)
    : 0;
  const unfinishedTaskRows = valid
    ? query(
      db,
      `SELECT id FROM tasks INDEXED BY tasks_active
       WHERE bot_uid = ? AND channel_type = ? AND channel_id = ?
         AND state IN ('queued', 'started')
       LIMIT ?`,
      [botUid, target.channelType, target.channelId, MAX_SNAPSHOT_ACTIVE_TASKS + 1],
    )
    : [];
  const assistantHistoryOverflow = unfinishedTaskRows.length > MAX_SNAPSHOT_ACTIVE_TASKS;
  return {
    botUid,
    channelType: target.channelType,
    channelId: target.channelId,
    inboundRowId,
    ingressJobId,
    outboxRowId,
    unfinishedTaskIds: assistantHistoryOverflow
      ? []
      : unfinishedTaskRows.map((row) => asString(row.id)),
    assistantHistoryOverflow,
    capturedAt: Date.now(),
  };
}

const MAX_REFERENCE_CANDIDATES = 8;
const MAX_PHYSICAL_INBOUND_ROWS = 4096;
const MAX_PHYSICAL_INGRESS_ROWS = 4096;
const MAX_PHYSICAL_OUTBOX_ROWS = 8192;
const MAX_SNAPSHOT_ACTIVE_TASKS = 256;
const MAX_REFERENCE_HISTORY_SCAN_INBOUND = 256;
const MAX_REFERENCE_HISTORY_SCAN_STREAMS = 128;
const MAX_REFERENCE_INBOUND_CODE_POINTS = 65_536;
const MAX_REFERENCE_STREAM_SEGMENTS = 128;
const MAX_REFERENCE_STREAM_SEGMENT_CODE_POINTS = 8_192;
const MAX_REFERENCE_STREAM_CODE_POINTS = 65_536;
const MAX_RECENT_REFERENCE_MESSAGES = 4;
const MAX_MATCHED_TASKS_PER_CANDIDATE = 2;
const MAX_MATCHED_STREAMS_PER_TASK = 2;
const MAX_MATCHED_STEERING_PER_CANDIDATE = 2;

interface ReferenceInboundRow {
  /** Steering claims have no persisted task association; their ingress consumer stores the message ID. */
  taskId: string | null;
  messageId: string;
  text: string;
  at: number;
  rowId: number;
}

interface ReferenceAssistantStream {
  taskId: string;
  streamMessageId: string;
  text: string;
  at: number;
  rowId: number;
}

interface ReferenceHistoryRow {
  entry: ReferenceHistoryEntry;
  at: number;
  rowId: number;
}

interface CandidateTaskStreams {
  at: number;
  streams: Map<string, ReferenceAssistantStream>;
}

interface CandidateHistoryMatches {
  inboundTasks: Map<string, ReferenceInboundRow>;
  reportTasks: Map<string, CandidateTaskStreams>;
  steering: Map<string, ReferenceInboundRow>;
}

function readReferenceInboundRows(
  db: Database,
  botUid: string,
  target: ChatTarget,
  snapshot: ReferenceHistorySnapshot,
  limit: number,
  taskIds?: readonly string[],
): ReferenceInboundRow[] {
  if (taskIds?.length === 0) return [];
  const taskFilter = taskIds
    ? `AND i.task_id IN (${taskIds.map(() => "?").join(", ")})`
    : "";
  const rows = query(
    db,
    `WITH recent_ingress AS MATERIALIZED (
       SELECT j.id, j.bot_uid, j.source_kind, j.source_id, j.channel_type, j.channel_id,
              j.message_id, j.state, j.ack_ready, j.lane, j.consumer_kind, j.consumer_id
       FROM ingress_jobs AS j INDEXED BY ingress_jobs_chat_order
       WHERE j.bot_uid = ? AND j.channel_type = ? AND j.channel_id = ?
         AND j.lane = 'ordered' AND j.id <= ?
       ORDER BY j.id DESC
       LIMIT ?
     ),
     recent_rowids AS MATERIALIZED (
       SELECT rowid AS row_id
       FROM inbound
       WHERE rowid <= ?
       ORDER BY rowid DESC
       LIMIT ?
     )
     SELECT i.task_id AS task_id, i.message_id AS message_id, i.text AS text,
            i.accepted_at AS event_at, i.rowid AS row_id,
            j.source_kind AS source_kind, j.source_id AS source_id
     FROM recent_rowids AS recent
     JOIN inbound AS i ON i.rowid = recent.row_id
     JOIN recent_ingress AS j
       ON j.bot_uid = i.bot_uid AND j.channel_type = i.channel_type AND j.channel_id = i.channel_id
       AND j.message_id = i.message_id AND j.state = 'handed-off' AND j.ack_ready = 1
       AND j.lane = 'ordered'
     LEFT JOIN tasks AS t
       ON t.id = i.task_id AND t.bot_uid = i.bot_uid
       AND t.channel_type = i.channel_type AND t.channel_id = i.channel_id
     WHERE i.bot_uid = ? AND i.channel_type = ? AND i.channel_id = ?
       AND i.accepted_at <= ? AND i.supported = 1 AND length(i.text) <= ?
       AND (
         (i.kind = 'task' AND i.task_id IS NOT NULL AND t.message_id = i.message_id
           AND t.history_eligible = 1 AND j.consumer_kind = 'task' AND j.consumer_id = i.task_id
           AND (j.source_kind = 'recv' OR
             (j.source_kind = 'synthetic' AND j.source_id = i.message_id
               AND substr(i.message_id, 1, 9) = 'dispatch:')))
         OR
         (i.kind = 'command' AND i.task_id IS NULL AND i.command_state = 'completed'
           AND j.consumer_kind = 'steering' AND j.consumer_id = i.message_id
           AND j.source_kind = 'recv')
       )
       ${taskFilter}
     ORDER BY i.accepted_at DESC, i.rowid DESC
     LIMIT ?`,
    [
      botUid,
      target.channelType,
      target.channelId,
      snapshot.ingressJobId,
      MAX_PHYSICAL_INGRESS_ROWS,
      snapshot.inboundRowId,
      MAX_PHYSICAL_INBOUND_ROWS,
      botUid,
      target.channelType,
      target.channelId,
      snapshot.capturedAt,
      MAX_REFERENCE_INBOUND_CODE_POINTS,
      ...(taskIds ?? []),
      limit,
    ],
  );
  return rows.flatMap((row) => {
    const messageId = asString(row.message_id);
    const sourceKind = asString(row.source_kind);
    const sourceId = asString(row.source_id);
    if (
      sourceKind === "recv" &&
      sourceId !== recvIngressSourceId(target.channelType, target.channelId, messageId)
    ) {
      return [];
    }
    if (
      sourceKind === "synthetic" &&
      (!messageId.startsWith("dispatch:") || sourceId !== messageId)
    ) {
      return [];
    }
    return [{
      taskId: asNullableString(row.task_id),
      messageId,
      text: asString(row.text),
      at: asNumber(row.event_at),
      rowId: asNumber(row.row_id),
    }];
  });
}

function readReferenceAssistantStreams(
  db: Database,
  botUid: string,
  target: ChatTarget,
  snapshot: ReferenceHistorySnapshot,
  limit: number,
): ReferenceAssistantStream[] {
  if (snapshot.assistantHistoryOverflow) return [];
  const activeTaskFilter = snapshot.unfinishedTaskIds.length > 0
    ? `AND o.task_id NOT IN (${snapshot.unfinishedTaskIds.map(() => "?").join(", ")})`
    : "";
  const streams = query(
    db,
    `WITH recent_rowids AS MATERIALIZED (
       SELECT rowid AS row_id
       FROM outbox
       WHERE rowid <= ?
       ORDER BY rowid DESC
       LIMIT ?
     )
     SELECT o.task_id AS task_id, o.stream_message_id AS stream_message_id,
            MAX(o.created_at) AS stream_created_at, MAX(o.rowid) AS stream_row_id
     FROM recent_rowids AS recent
     JOIN outbox AS o ON o.rowid = recent.row_id
     JOIN tasks AS t
       ON t.id = o.task_id AND t.bot_uid = o.bot_uid
       AND t.channel_type = o.channel_type AND t.channel_id = o.channel_id
     WHERE o.bot_uid = ? AND o.channel_type = ? AND o.channel_id = ?
       AND o.kind = 'text' AND o.task_id IS NOT NULL
       AND o.stream_message_id IS NOT NULL AND o.stream_final = 1
       ${activeTaskFilter}
       AND o.created_at <= ? AND t.finished_at IS NOT NULL AND t.finished_at <= ?
       AND substr(o.stream_message_id, length(o.task_id) + 1, 8) <> ':notice:'
     GROUP BY o.task_id, o.stream_message_id
     ORDER BY MAX(o.created_at) DESC, MAX(o.rowid) DESC
     LIMIT ?`,
    [
      snapshot.outboxRowId,
      MAX_PHYSICAL_OUTBOX_ROWS,
      botUid,
      target.channelType,
      target.channelId,
      ...snapshot.unfinishedTaskIds,
      snapshot.capturedAt,
      snapshot.capturedAt,
      limit,
    ],
  );

  const result: ReferenceAssistantStream[] = [];
  for (const stream of streams) {
    const taskId = asString(stream.task_id);
    const streamMessageId = asString(stream.stream_message_id);
    if (taskId.length === 0 || streamMessageId.length === 0) continue;

    const segments = query(
      db,
      `SELECT o.segment_index AS segment_index, length(o.text) AS text_length,
              substr(o.text, 1, ?) AS text
       FROM outbox AS o INDEXED BY outbox_stream
       WHERE o.bot_uid = ? AND o.task_id = ? AND o.stream_message_id = ?
         AND o.channel_type = ? AND o.channel_id = ?
         AND o.rowid <= ? AND o.created_at <= ?
         AND o.kind = 'text' AND o.stream_final = 1 AND o.text IS NOT NULL
       ORDER BY o.segment_index ASC, o.desired_version DESC,
                o.created_at DESC, o.rowid DESC
       LIMIT ?`,
      [
        MAX_REFERENCE_STREAM_SEGMENT_CODE_POINTS + 1,
        botUid,
        taskId,
        streamMessageId,
        target.channelType,
        target.channelId,
        snapshot.outboxRowId,
        snapshot.capturedAt,
        MAX_REFERENCE_STREAM_SEGMENTS + 1,
      ],
    );
    if (segments.length === 0 || segments.length > MAX_REFERENCE_STREAM_SEGMENTS) continue;

    const selectedSegments = new Map<number, string>();
    let codePoints = 0;
    let tooLarge = false;
    for (const row of segments) {
      const segmentIndex = asNumber(row.segment_index);
      if (selectedSegments.has(segmentIndex)) continue;
      const segmentCodePoints = asNumber(row.text_length);
      if (segmentCodePoints > MAX_REFERENCE_STREAM_SEGMENT_CODE_POINTS) {
        tooLarge = true;
        break;
      }
      codePoints += segmentCodePoints;
      if (codePoints > MAX_REFERENCE_STREAM_CODE_POINTS) {
        tooLarge = true;
        break;
      }
      selectedSegments.set(segmentIndex, asString(row.text));
    }
    if (tooLarge) continue;

    result.push({
      taskId,
      streamMessageId,
      text: [...selectedSegments.entries()]
        .sort(([left], [right]) => left - right)
        .map(([, segment]) => segment)
        .join(""),
      at: asNumber(stream.stream_created_at),
      rowId: asNumber(stream.stream_row_id),
    });
  }
  return result.sort((left, right) => right.at - left.at || right.rowId - left.rowId);
}


function addCandidateInbound(
  matches: CandidateHistoryMatches,
  row: ReferenceInboundRow,
): void {
  if (row.taskId === null) {
    const previous = matches.steering.get(row.messageId);
    if (!previous || row.at > previous.at) matches.steering.set(row.messageId, row);
    return;
  }
  const previous = matches.inboundTasks.get(row.taskId);
  if (!previous || row.at > previous.at) matches.inboundTasks.set(row.taskId, row);
}

function addCandidateStream(
  matches: CandidateHistoryMatches,
  stream: ReferenceAssistantStream,
): void {
  let task = matches.reportTasks.get(stream.taskId);
  if (!task) {
    task = { at: stream.at, streams: new Map() };
    matches.reportTasks.set(stream.taskId, task);
  }
  task.at = Math.max(task.at, stream.at);
  task.streams.set(stream.streamMessageId, stream);
}

export function listReferenceHistory(
  db: Database,
  botUid: string,
  target: ChatTarget,
  candidateUrls: readonly string[],
  snapshot?: ReferenceHistorySnapshot,
): ReferenceHistoryEntry[] {
  if (!validThreadIdentity(botUid, target)) return [];

  const historySnapshot = snapshot ?? captureReferenceHistorySnapshot(db, botUid, target);
  if (
    historySnapshot.botUid !== botUid ||
    historySnapshot.channelType !== target.channelType ||
    historySnapshot.channelId !== target.channelId ||
    !Number.isSafeInteger(historySnapshot.inboundRowId) ||
    historySnapshot.inboundRowId < 0 ||
    !Number.isSafeInteger(historySnapshot.outboxRowId) ||
    historySnapshot.outboxRowId < 0 ||
    !Number.isSafeInteger(historySnapshot.ingressJobId) ||
    historySnapshot.ingressJobId < 0 ||
    !Array.isArray(historySnapshot.unfinishedTaskIds) ||
    historySnapshot.unfinishedTaskIds.length > MAX_SNAPSHOT_ACTIVE_TASKS ||
    historySnapshot.unfinishedTaskIds.some((taskId) => typeof taskId !== "string" || taskId.length === 0) ||
    typeof historySnapshot.assistantHistoryOverflow !== "boolean" ||
    !Number.isFinite(historySnapshot.capturedAt)
  ) {
    return [];
  }


  const candidateSet = new Set(
    candidateUrls.slice(0, MAX_REFERENCE_CANDIDATES)
      .map((url) => canonicalReferenceUrl(url))
      .filter((url): url is string => url !== null),
  );
  const inboundRows = readReferenceInboundRows(
    db,
    botUid,
    target,
    historySnapshot,
    candidateSet.size > 0 ? MAX_REFERENCE_HISTORY_SCAN_INBOUND : MAX_RECENT_REFERENCE_MESSAGES,
  );
  const assistantStreams = readReferenceAssistantStreams(
    db,
    botUid,
    target,
    historySnapshot,
    candidateSet.size > 0 ? MAX_REFERENCE_HISTORY_SCAN_STREAMS : MAX_RECENT_REFERENCE_MESSAGES,
  );


  const candidateMatches = new Map<string, CandidateHistoryMatches>(
    [...candidateSet].map((url): [string, CandidateHistoryMatches] => [url, {
      inboundTasks: new Map<string, ReferenceInboundRow>(),
      reportTasks: new Map<string, CandidateTaskStreams>(),
      steering: new Map<string, ReferenceInboundRow>(),
    }]),
  );
  for (const row of inboundRows) {
    for (const url of matchingReferenceCandidateUrls(row.text, candidateSet)) {
      addCandidateInbound(candidateMatches.get(url)!, row);
    }
  }
  for (const stream of assistantStreams) {
    for (const url of matchingReferenceCandidateUrls(stream.text, candidateSet)) {
      addCandidateStream(candidateMatches.get(url)!, stream);
    }
  }

  const candidateTaskIds = new Set<string>();
  const exactCandidateStreams = new Map<string, ReferenceAssistantStream>();
  const candidateSteering = new Map<string, ReferenceInboundRow>();
  for (const matches of candidateMatches.values()) {
    const reportTasks = [...matches.reportTasks.entries()]
      .sort(([leftId, left], [rightId, right]) => right.at - left.at || leftId.localeCompare(rightId))
      .slice(0, MAX_MATCHED_TASKS_PER_CANDIDATE);
    for (const [taskId, task] of reportTasks) {
      candidateTaskIds.add(taskId);
      const exactStreams = [...task.streams.values()]
        .sort((left, right) => right.at - left.at || right.rowId - left.rowId)
        .slice(0, MAX_MATCHED_STREAMS_PER_TASK);
      for (const stream of exactStreams) {
        exactCandidateStreams.set(JSON.stringify([stream.taskId, stream.streamMessageId]), stream);
      }
    }

    const inputTasks = [...matches.inboundTasks.entries()]
      .sort(([leftId, left], [rightId, right]) => right.at - left.at || leftId.localeCompare(rightId))
      .slice(0, MAX_MATCHED_TASKS_PER_CANDIDATE);
    for (const [taskId] of inputTasks) candidateTaskIds.add(taskId);
    for (const [messageId, row] of [...matches.steering.entries()]
      .sort(([, left], [, right]) => right.at - left.at || right.rowId - left.rowId)
      .slice(0, MAX_MATCHED_STEERING_PER_CANDIDATE)) {
      candidateSteering.set(messageId, row);
    }
  }

  const history = new Map<string, ReferenceHistoryRow>();
  const addInbound = (row: ReferenceInboundRow): void => {
    const key = JSON.stringify(["user", row.taskId, row.messageId]);
    if (!history.has(key)) {
      history.set(key, {
        entry: { role: "user", text: row.text, taskId: row.taskId },
        at: row.at,
        rowId: row.rowId,
      });
    }
  };
  const addAssistant = (stream: ReferenceAssistantStream): void => {
    const key = JSON.stringify(["assistant", stream.taskId, stream.streamMessageId]);
    if (!history.has(key)) {
      history.set(key, {
        entry: { role: "assistant", text: stream.text, taskId: stream.taskId },
        at: stream.at,
        rowId: stream.rowId,
      });
    }
  };

  for (const row of inboundRows.slice(0, MAX_RECENT_REFERENCE_MESSAGES)) addInbound(row);
  for (const row of candidateSteering.values()) addInbound(row);
  for (const row of inboundRows) {
    if (row.taskId !== null && candidateTaskIds.has(row.taskId)) addInbound(row);
  }
  const reportTaskIds = [...candidateTaskIds].filter((taskId) =>
    [...exactCandidateStreams.values()].some((stream) => stream.taskId === taskId)
  );
  for (const row of readReferenceInboundRows(
    db,
    botUid,
    target,
    historySnapshot,
    reportTaskIds.length,
    reportTaskIds,
  )) {
    addInbound(row);
  }

  for (const stream of assistantStreams.slice(0, MAX_RECENT_REFERENCE_MESSAGES)) addAssistant(stream);
  for (const stream of exactCandidateStreams.values()) addAssistant(stream);

  return [...history.values()]
    .sort((left, right) =>
      left.at - right.at ||
      (left.entry.role === right.entry.role ? 0 : left.entry.role === "user" ? -1 : 1) ||
      left.rowId - right.rowId)
    .map(({ entry }) => entry);
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
