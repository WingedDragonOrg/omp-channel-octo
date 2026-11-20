import { constants as fsConstants } from "node:fs";
import { chmod, lstat, mkdir, open, readdir, realpath } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import type {
  ChatTarget,
  HostTool,
  OmpWorker,
  PromptContext,
  WorkerEvent,
  WorkerProcess,
} from "../contracts.ts";
import { redactSensitiveText } from "../cards/execution-text.ts";
import { parseCron, matchesCron, type ParsedCron } from "./cron.ts";
import { runScheduleScript } from "./script.ts";
import {
  SYSTEM_SCHEDULE_CLOCK,
  type ScheduleAccessRole,
  type ScheduleArtifact,
  type ScheduleCoordinatorOptions,
  type ScheduleCreateRequest,
  type ScheduleRecord,
  type ScheduleRunRecord,
  type ScheduleRunSettlement,
  type ScheduleScriptResult,
  type ScheduleSpec,
  type ScheduleStopResult,
} from "./types.ts";
import { taskTarget } from "../target.ts";
import type { TaskRecord } from "../store/store-records.ts";
import { loadValidatedSessionState, verifyFreshSession, verifyMovedSession } from "../session/project-session.ts";
import { projectSessionDir } from "../session/session-dir.ts";

const MINUTE_MS = 60_000;
const MAX_CONCURRENT_RUNS = 2;
const DEFAULT_TIMEOUT_MS = 30 * 60_000;
const MAX_TIMEOUT_MS = DEFAULT_TIMEOUT_MS;
const MAX_NAME_CHARS = 120;
const MAX_PROMPT_CHARS = 20_000;
const MAX_SCRIPT_CHARS = 32_000;
const MAX_ARGUMENTS = 128;
const MAX_ARGUMENT_CHARS = 4_096;
const MAX_RESULT_CHARS = 8_192;
const MAX_MANIFEST_BYTES = 32 * 1024;
const MAX_MANIFEST_ARTIFACTS = 32;
const MAX_ARTIFACT_FIELD_CHARS = 2_048;
const SCHEDULE_TOOL_NAME = "schedule";

interface ActiveRun {
  scheduleId: string;
  runId: string;
  controller: AbortController;
  promise: Promise<void>;
  ready: Promise<void>;
  resolveReady(): void;
  worker?: OmpWorker;
  workerStop?: Promise<boolean>;
  timedOut: boolean;
}


function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function boundedString(value: unknown, name: string, maximum: number, allowEmpty = false): string {
  if (typeof value !== "string") throw new Error(`${name} must be a string`);
  const result = value.trim();
  if ((!allowEmpty && result.length === 0) || result.length > maximum || /[\x00-\x1f\x7f]/u.test(result)) {
    throw new Error(`${name} is invalid`);
  }
  return result;
}

function executionText(value: unknown, name: string, maximum: number, allowEmpty = false): string {
  if (typeof value !== "string" || (!allowEmpty && !value.trim()) || value.length > maximum || value.includes("\0")) {
    throw new Error(`${name} is invalid`);
  }
  return value;
}

function parseTarget(value: unknown): ChatTarget {
  if (!isObject(value)) throw new Error("target must include channelType and channelId");
  const channelId = boundedString(value.channelId, "target.channelId", 512);
  if (value.channelType !== 1 && value.channelType !== 2 && value.channelType !== 5) {
    throw new Error("target.channelType must be 1, 2, or 5");
  }
  return { channelType: value.channelType, channelId };
}

function parseSpec(args: Record<string, unknown>): ScheduleSpec {
  const hasPrompt = typeof args.prompt === "string";
  const hasScript = typeof args.script === "string";
  const hasCommand = typeof args.command === "string" || args.args !== undefined;
  if (Number(hasPrompt) + Number(hasScript) + Number(hasCommand) !== 1) {
    throw new Error("provide exactly one of prompt, script, or command with args");
  }
  if (hasPrompt) {
    return { kind: "agent", prompt: executionText(args.prompt, "prompt", MAX_PROMPT_CHARS) };
  }
  if (hasScript) {
    return { kind: "script", runner: { kind: "shell", script: executionText(args.script, "script", MAX_SCRIPT_CHARS) } };
  }
  const command = boundedString(args.command, "command", MAX_ARGUMENT_CHARS);
  const rawArgs = args.args ?? [];
  if (!Array.isArray(rawArgs) || rawArgs.length > MAX_ARGUMENTS) throw new Error("args must be an array of at most 128 strings");
  const parsedArgs = rawArgs.map((value, index) => executionText(value, `args[${index}]`, MAX_ARGUMENT_CHARS, true));
  return { kind: "script", runner: { kind: "exec", command, args: parsedArgs } };
}

function parseCreateRequest(args: Record<string, unknown>): ScheduleCreateRequest {
  const name = boundedString(args.name, "name", MAX_NAME_CHARS);
  const cron = boundedString(args.cron, "cron", 256);
  const timezone = boundedString(args.timezone, "timezone", 128);
  // Parse at creation so invalid calendars never become durable runnable plans.
  parseCron(cron, timezone);
  const cwd = boundedString(args.cwd, "cwd", 4_096);
  if (!cwd.startsWith("/")) throw new Error("cwd must be an absolute path");
  const target = parseTarget(args.target);
  const timeoutSeconds = args.timeoutSeconds === undefined ? DEFAULT_TIMEOUT_MS / 1_000 : args.timeoutSeconds;
  if (typeof timeoutSeconds !== "number" || !Number.isSafeInteger(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > MAX_TIMEOUT_MS / 1_000) {
    throw new Error("timeoutSeconds must be from 1 to 1800");
  }
  return { name, cron, timezone, cwd, target, spec: parseSpec(args), timeoutMs: timeoutSeconds * 1_000 };
}

function safeResultText(value: string | null | undefined): string | null {
  if (typeof value !== "string" || value.length === 0) return null;
  const normalized = redactSensitiveText(value).replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/gu, " ");
  return normalized.length > MAX_RESULT_CHARS ? `${normalized.slice(0, MAX_RESULT_CHARS)}…` : normalized;
}

function safeArtifactText(value: unknown, name: string, maximum: number, allowEmpty = false): string {
  const text = boundedString(value, name, maximum, allowEmpty);
  const normalized = redactSensitiveText(text).replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/gu, " ");
  if ((!allowEmpty && normalized.length === 0) || normalized.length > maximum) {
    throw new Error(`${name} is invalid after redaction`);
  }
  return normalized;
}

function sameTarget(left: ChatTarget, right: ChatTarget): boolean {
  return left.channelType === right.channelType && left.channelId === right.channelId;
}

function toolEnvelope(properties: Record<string, unknown>, actions: readonly string[]) {
  return {
    type: "object",
    properties: {
      action: { type: "string", enum: [...actions] },
      ...properties,
    },
    required: ["action"],
    additionalProperties: false,
  };
}

function recordSummary(record: ScheduleRecord, full: boolean): Record<string, unknown> {
  const summary: Record<string, unknown> = {
    id: record.id,
    name: record.name,
    cron: record.cron,
    timezone: record.timezone,
    cwd: record.cwd,
    target: record.target,
    timeoutSeconds: record.timeoutMs / 1_000,
    createdAt: new Date(record.createdAt).toISOString(),
    status: record.status,
    source: record.source,
  };
  if (full) {
    summary.spec = record.spec;
    summary.sourceRequesterUid = record.sourceRequesterUid;
    summary.sourceKind = record.sourceKind;
    summary.sourceMessageId = record.sourceMessageId;
    summary.sourceTaskId = record.sourceTaskId;
  }
  return summary;
}

function runSummary(run: ScheduleRunRecord, full: boolean): Record<string, unknown> {
  const summary: Record<string, unknown> = {
    id: run.id,
    scheduleId: run.scheduleId,
    scheduledAt: new Date(run.scheduledAt).toISOString(),
    target: run.target,
    createdAt: new Date(run.createdAt).toISOString(),
    startedAt: run.startedAt === null ? null : new Date(run.startedAt).toISOString(),
    endedAt: run.endedAt === null ? null : new Date(run.endedAt).toISOString(),
    status: run.status,
    stage: run.stage,
    stopRequested: run.stopRequested,
    skippedReason: run.skippedReason,
  };
  if (full) {
    summary.promptAccepted = run.promptAccepted;
    summary.sessionId = run.sessionId;
    summary.result = run.resultText;
    summary.error = run.errorText;
    summary.stdout = run.stdout;
    summary.stderr = run.stderr;
    summary.artifacts = run.artifacts;
  }
  return summary;
}

function artifactFromManifest(value: unknown, planTarget: ChatTarget): ScheduleArtifact | null {
  if (!isObject(value)) return null;
  let name: string;
  let type: string;
  let target: ChatTarget;
  try {
    name = safeArtifactText(value.name, "artifact.name", MAX_ARTIFACT_FIELD_CHARS);
    type = safeArtifactText(value.type, "artifact.type", 128);
    target = parseTarget(value.target);
  } catch {
    return null;
  }
  if (!sameTarget(target, planTarget)) return null;
  const hasPath = typeof value.path === "string";
  const hasRemoteRef = typeof value.remoteRef === "string";
  if (hasPath === hasRemoteRef) return null;
  let artifactLocation: { path: string } | { remoteRef: string };
  try {
    artifactLocation = hasPath
      ? { path: safeArtifactText(value.path, "artifact.path", MAX_ARTIFACT_FIELD_CHARS) }
      : { remoteRef: safeArtifactText(value.remoteRef, "artifact.remoteRef", MAX_ARTIFACT_FIELD_CHARS) };
  } catch {
    return null;
  }
  const receipt = value.receipt === undefined
    ? undefined
    : safeArtifactText(value.receipt, "artifact.receipt", 1_024, true);
  return { name, type, target, ...artifactLocation, ...(receipt === undefined ? {} : { receipt }) };
}

function agentPrompt(record: ScheduleRecord, run: ScheduleRunRecord): string {
  return [
    "Execute this scheduled task now in this independent OMP session.",
    `schedule_id=${record.id}`,
    `run_id=${run.id}`,
    `scheduled_at_utc=${new Date(run.scheduledAt).toISOString()}`,
    `authorized_target=${JSON.stringify(record.target)}`,
    "This worker is not the source chat session. Do not disclose the source or this result to another chat.",
    "You may send only through the current Bot's available Octo CLI identity and the explicitly authorized target. Do not claim a send succeeded without a receipt.",
    "Execute the task as requested, including any requested Octo CLI delivery, only to the authorized target. Register each declared artifact with the schedule tool action=artifact using that exact target; registration is an audit record, not delivery.",
    "Task:",
    record.spec.kind === "agent" ? record.spec.prompt : "",
  ].join("\n");
}

function safeSessionPathSegment(value: string, name: string): string {
  if (value.length === 0 || value === "." || value === ".." || /[/\\\x00-\x1f\x7f]/u.test(value)) {
    throw new Error(`${name} is invalid`);
  }
  return value;
}

async function assertScheduleSessionDirectory(directory: string): Promise<void> {
  const details = await lstat(directory);
  if (!details.isDirectory() || details.isSymbolicLink() || await realpath(directory) !== resolve(directory)) {
    throw new Error("schedule agent session directory is not a private real directory");
  }
}

async function ensureScheduleSessionDirectory(directory: string): Promise<void> {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await assertScheduleSessionDirectory(directory);
  await chmod(directory, 0o700);
}

export class ScheduleCoordinator {
  readonly #options: ScheduleCoordinatorOptions;
  readonly #clock: NonNullable<ScheduleCoordinatorOptions["clock"]>;
  readonly #scriptRunner: NonNullable<ScheduleCoordinatorOptions["runScript"]>;
  readonly #activeRuns = new Map<string, ActiveRun>();
  readonly #parsedCron = new Map<string, ParsedCron>();
  #timer: NodeJS.Timeout | undefined;
  #started = false;
  #sealed = false;
  #shutdownPromise: Promise<void> | undefined;

  constructor(options: ScheduleCoordinatorOptions) {
    this.#options = options;
    this.#clock = options.clock ?? SYSTEM_SCHEDULE_CLOCK;
    this.#scriptRunner = options.runScript ?? runScheduleScript;
  }
  get hasInFlight(): boolean {
    return this.#activeRuns.size > 0;
  }


  createTool(task: TaskRecord): HostTool {
    return {
      name: SCHEDULE_TOOL_NAME,
      description: "分钟级 cron 计划管理。schedule.create 仅限原始请求者身份可验证且当前回合未接受其他或无法验证身份的 steering；若资格失效，请以独立消息重试。script 和 agent 以 Bot 当前 octo-cli 身份运行，并非沙箱；必须指定目标聊天和时区。script 环境提供 OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_TYPE、OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_ID 与 OMP_CHANNEL_SCHEDULE_ARTIFACTS_FILE。跨目标计划的来源聊天只可查看状态，不显示结果或产物。",
      parameters: toolEnvelope({
        name: { type: "string" },
        cron: { type: "string", description: "标准五字段 cron；最小粒度为一分钟。" },
        timezone: { type: "string", description: "IANA timezone。" },
        cwd: { type: "string", description: "执行时固定的绝对工作目录。" },
        prompt: { type: "string", description: "agent 任务，与 script/command 二选一。" },
        script: { type: "string", description: "固定 shell 文本，与 prompt/command 二选一。" },
        command: { type: "string", description: "直接 executable，与 prompt/script 二选一。" },
        args: { type: "array", items: { type: "string" }, maxItems: MAX_ARGUMENTS },
        target: {
          type: "object",
          properties: { channelType: { type: "integer", enum: [1, 2, 5] }, channelId: { type: "string" } },
          required: ["channelType", "channelId"],
          additionalProperties: false,
        },
        timeoutSeconds: { type: "integer", minimum: 1, maximum: MAX_TIMEOUT_MS / 1_000 },
        schedule_id: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 100 },
        offset: { type: "integer", minimum: 0 },
      }, ["create", "list", "get", "runs", "pause", "resume", "stop"]),
      execute: (args, signal, toolCallId) => this.#executeTool(task, args, signal, toolCallId),
    };
  }

  createRunTool(record: ScheduleRecord, run: ScheduleRunRecord): HostTool {
    return {
      name: SCHEDULE_TOOL_NAME,
      description: "登记当前 scheduled run 的一个产物；仅作审计，不会发送。target 必须等于计划授权目标。",
      parameters: toolEnvelope({
        name: { type: "string" },
        type: { type: "string" },
        path: { type: "string" },
        remoteRef: { type: "string" },
        target: {
          type: "object",
          properties: { channelType: { type: "integer", enum: [1, 2, 5] }, channelId: { type: "string" } },
          required: ["channelType", "channelId"],
          additionalProperties: false,
        },
        receipt: { type: "string" },
      }, ["artifact"]),
      execute: async (args, signal, toolCallId) => {
        if (signal.aborted) return "schedule artifact: cancelled";
        if (args.action !== "artifact") return "schedule artifact: unsupported action";
        const callId = toolCallId?.trim();
        if (!callId) return "schedule artifact: native tool call identity is required";
        let artifact: ScheduleArtifact;
        try {
          artifact = this.#parseArtifact(args, record.target);
        } catch (error) {
          return `schedule artifact: ${safeError(error)}`;
        }
        const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
        if (!latest || latest.scheduleId !== record.id) return "schedule artifact: run not found";
        const accepted = this.#options.store.addScheduleArtifact(run.id, artifact, callId, this.#clock.now());
        return accepted ? JSON.stringify({ registered: true, name: artifact.name }) : "schedule artifact: run is no longer accepting artifacts or target is not authorized";
      },
    };
  }

  async start(): Promise<void> {
    if (this.#started) return;
    if (this.#sealed) throw new Error("schedule coordinator is sealed");
    const botUid = this.#options.botUid();
    if (!botUid) throw new Error("schedule coordinator requires a registered Bot identity");
    await this.#recover(botUid);
    if (this.#sealed) throw new Error("schedule coordinator was sealed during startup");
    this.#started = true;
    this.#armNextMinute();
  }

  /** Synchronously closes all creation/dispatch admission before shutdown begins. */
  seal(): void {
    if (this.#sealed) return;
    this.#sealed = true;
    if (this.#timer !== undefined) this.#clock.clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  async stop(): Promise<void> {
    if (this.#shutdownPromise) return this.#shutdownPromise;
    this.seal();
    this.#shutdownPromise = this.#stopRunsForShutdown();
    return this.#shutdownPromise;
  }

  async #executeTool(
    task: TaskRecord,
    args: Record<string, unknown>,
    signal: AbortSignal,
    toolCallId: string | undefined,
  ): Promise<string> {
    if (signal.aborted) return "schedule: cancelled";
    if (this.#sealed) return "schedule: service is stopping";
    const action = args.action;
    try {
      if (action === "create") return await this.#create(task, args, signal, toolCallId);
      if (action === "list") return await this.#list(task, args, signal);
      if (action === "get") return await this.#get(task, args, signal);
      if (action === "runs") return await this.#runs(task, args, signal);
      if (action === "pause" || action === "resume" || action === "stop") {
        return await this.#changeStatus(task, args, signal, action);
      }
      return "schedule: unsupported action";
    } catch (error) {
      return `schedule: ${safeError(error)}`;
    }
  }

  async #create(task: TaskRecord, args: Record<string, unknown>, signal: AbortSignal, toolCallId?: string): Promise<string> {
    const nativeCallId = toolCallId?.trim();
    if (!nativeCallId) return "schedule: native tool call identity is required";
    const request = parseCreateRequest(args);
    const record = await this.#options.createRecord(task, request, nativeCallId, signal);
    if (signal.aborted || this.#sealed) return "schedule: creation cancelled before persistence";
    const result = this.#options.store.createSchedule(record);
    this.#parsedCron.set(result.record.id, parseCron(result.record.cron, result.record.timezone));
    return JSON.stringify({ kind: result.kind, schedule: recordSummary(result.record, false) });
  }

  async #list(task: TaskRecord, args: Record<string, unknown>, signal: AbortSignal): Promise<string> {
    const botUid = this.#requireBotUid();
    const target = taskTarget(task);
    const limit = paginationValue(args.limit, 20, 100);
    const offset = paginationValue(args.offset, 0, Number.MAX_SAFE_INTEGER);
    const records = this.#options.store.listSchedules(botUid, target, limit, offset);
    const visible: Record<string, unknown>[] = [];
    for (const record of records) {
      if (signal.aborted) return "schedule: cancelled";
      const role = await this.#classify(task, record, signal);
      if ((role === "source" || role === "owner") && sameTarget(target, record.source)) {
        visible.push(recordSummary(record, sameTarget(record.source, record.target)));
      }
    }
    return JSON.stringify({ schedules: visible });
  }

  async #get(task: TaskRecord, args: Record<string, unknown>, signal: AbortSignal): Promise<string> {
    const record = this.#findSchedule(args.schedule_id);
    if (!record) return "schedule: not found or not authorized";
    const role = await this.#classify(task, record, signal);
    if (role === "denied") return "schedule: not found or not authorized";
    const full = role === "owner" || sameTarget(record.source, record.target);
    return JSON.stringify({ schedule: recordSummary(record, full) });
  }

  async #runs(task: TaskRecord, args: Record<string, unknown>, signal: AbortSignal): Promise<string> {
    const record = this.#findSchedule(args.schedule_id);
    if (!record) return "schedule: not found or not authorized";
    const role = await this.#classify(task, record, signal);
    if (role === "denied") return "schedule: not found or not authorized";
    const full = role === "owner" || sameTarget(record.source, record.target);
    const limit = paginationValue(args.limit, 20, 100);
    const offset = paginationValue(args.offset, 0, Number.MAX_SAFE_INTEGER);
    const runs = this.#options.store.listScheduleRuns(record.botUid, record.id, limit, offset);
    return JSON.stringify({ runs: runs.map(run => runSummary(run, full)) });
  }

  async #changeStatus(
    task: TaskRecord,
    args: Record<string, unknown>,
    signal: AbortSignal,
    action: "pause" | "resume" | "stop",
  ): Promise<string> {
    const record = this.#findSchedule(args.schedule_id);
    if (!record) return "schedule: not found or not authorized";
    const role = await this.#classify(task, record, signal);
    if (role !== "source" && !(role === "owner" && sameTarget(taskTarget(task), record.source))) {
      return "schedule: not found or not authorized";
    }
    const nextStatus = action === "pause" ? "paused" : action === "resume" ? "active" : "stopped";
    const updated = this.#options.store.setScheduleStatus(record.botUid, record.id, record.source, nextStatus, this.#clock.now());
    if (!updated) return "schedule: not found or not authorized";
    if (action !== "stop") return JSON.stringify({ schedule: recordSummary(updated, sameTarget(updated.source, updated.target)) });
    const result = await this.#stopSchedule(updated);
    if (result.kind === "not-found") return "schedule: not found or not authorized";
    return JSON.stringify({ kind: result.kind, schedule: recordSummary(result.record, false), runs: result.runs.map(run => runSummary(run, false)) });
  }

  async #stopSchedule(record: ScheduleRecord): Promise<ScheduleStopResult> {
    const owned = [...this.#activeRuns.values()].filter(run => run.scheduleId === record.id);
    for (const active of owned) {
      active.controller.abort(new Error("schedule stopped"));
      if (active.worker) this.#stopWorker(active);
    }
    await Promise.allSettled(owned.map(active => active.promise));
    let runs = this.#options.store.listScheduleRuns(record.botUid, record.id, 100, 0);
    const unsettled = this.#options.store.listScheduleRunsNeedingStop(record.botUid, record.id);
    for (const run of unsettled) {
      if (run.status === "claimed") {
        this.#options.store.settleScheduleRun(run.id, { status: "cancelled" }, this.#clock.now());
        continue;
      }
      if (this.#activeRuns.has(run.id)) continue;
      if (run.process && !run.processExited) {
        const stopped = await this.#recoverProcess(run.process);
        if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        else this.#block(run.process.cwd || record.cwd);
      } else if (run.stage === "dispatch-intent" && run.process === null) {
        // A crash/stop in the dispatch gap is not proof that no side effect started.
        this.#options.store.settleScheduleRun(run.id, { status: "unknown", errorText: "dispatch outcome could not be confirmed" }, this.#clock.now());
        continue;
      }
      if (run.stage === "settled") continue;
      const current = this.#options.store.getScheduleRun(record.botUid, run.id);
      if (current?.stage !== "settled") {
        const uncertain = current !== null
          && (current.status === "unknown" || (current.process !== null && !current.processExited));
        this.#options.store.settleScheduleRun(run.id, {
          status: uncertain ? "unknown" : "cancelled",
          errorText: uncertain ? "process exit or run outcome could not be confirmed" : null,
        }, this.#clock.now());
      }
    }
    runs = this.#options.store.listScheduleRuns(record.botUid, record.id, 100, 0);
    const pending = this.#options.store.listScheduleRunsNeedingStop(record.botUid, record.id).length > 0;
    return { kind: pending ? "unknown" : "stopped", record: updatedRecord(this.#options.store, record), runs };
  }

  async #recover(botUid: string): Promise<void> {
    const recoverable = this.#options.store.listRecoverableScheduleRuns(botUid);
    const resumptions: Array<{ run: ScheduleRunRecord; schedule: ScheduleRecord }> = [];
    for (const run of recoverable) {
      const schedule = this.#options.store.getSchedule(botUid, run.scheduleId);
      if (!schedule) {
        this.#options.store.settleScheduleRun(run.id, { status: "unknown", errorText: "schedule record unavailable during recovery" }, this.#clock.now());
        continue;
      }
      if (run.stage === "claimed" && run.process === null) {
        this.#options.store.settleScheduleRun(run.id, { status: "skipped", errorText: "service restarted before dispatch" }, this.#clock.now());
        continue;
      }
      let processExited = run.processExited;
      if (run.process !== null && !processExited) {
        processExited = await this.#recoverProcess(run.process);
        if (processExited) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        else this.#block(run.process.cwd || schedule.cwd);
      }
      const current = this.#options.store.getScheduleRun(botUid, run.id);
      if (!current) continue;
      if (schedule.status === "stopped" || current.stopRequested) {
        const uncertain = current.process !== null && !processExited;
        this.#options.store.settleScheduleRun(run.id, {
          status: uncertain ? "unknown" : "cancelled",
          errorText: uncertain ? "stopped process identity could not be confirmed" : null,
        }, this.#clock.now());
        continue;
      }
      if (current.status === "unknown") continue;
      if (schedule.spec.kind !== "agent") {
        this.#options.store.settleScheduleRun(run.id, {
          status: "unknown",
          errorText: "script outcome is unknown after restart; automatic replay is disabled",
        }, this.#clock.now());
        continue;
      }
      if (!current.promptAccepted || current.sessionFile === null || current.sessionId === null || current.process === null) {
        this.#options.store.settleScheduleRun(run.id, {
          status: "unknown",
          errorText: "agent dispatch acceptance or session identity is unknown; prompt replay is disabled",
        }, this.#clock.now());
        continue;
      }
      if (!processExited) {
        this.#options.store.settleScheduleRun(run.id, { status: "unknown", errorText: "old agent process is still uncertain" }, this.#clock.now());
        continue;
      }
      const permitted = await this.#options.validateExecution(schedule, AbortSignal.timeout(10_000)).catch(() => false);
      if (!permitted) {
        this.#options.store.settleScheduleRun(run.id, { status: "failed", errorText: "source or target authorization is no longer valid" }, this.#clock.now());
        continue;
      }
      resumptions.push({ run: current, schedule });
    }

    // Start each accepted session through OMP's native resume path. Await only process/session
    // startup so no fresh cron dispatch can race recovery; the resumed turn remains in its run row.
    for (const entry of resumptions) {
      if (this.#sealed) return;
      const active = this.#newActive(entry.schedule.id, entry.run.id);
      const operation = this.#runAgent(entry.schedule, entry.run, active, true);
      active.promise = operation;
      this.#activeRuns.set(entry.run.id, active);
      await active.ready;
      if (this.#sealed) {
        active.controller.abort(new Error("service stopping"));
        if (active.worker) this.#stopWorker(active);
      }
    }
  }

  #armNextMinute(): void {
    if (!this.#started || this.#sealed || this.#timer !== undefined) return;
    const now = this.#clock.now();
    const nextMinute = (Math.floor(now / MINUTE_MS) + 1) * MINUTE_MS;
    this.#timer = this.#clock.setTimeout(() => {
      this.#timer = undefined;
      void this.#onMinute(nextMinute).finally(() => this.#armNextMinute());
    }, Math.max(0, nextMinute - now));
  }

  async #onMinute(scheduledAt: number): Promise<void> {
    if (this.#sealed) return;
    const botUid = this.#options.botUid();
    if (!botUid) return;
    const now = this.#clock.now();
    // A delayed timer skips elapsed UTC minutes instead of catching them up.
    if (now >= scheduledAt + MINUTE_MS) return;
    for (const schedule of this.#options.store.listActiveSchedules(botUid)) {
      if (this.#sealed) return;
      let parsed = this.#parsedCron.get(schedule.id);
      try {
        parsed ??= parseCron(schedule.cron, schedule.timezone);
        this.#parsedCron.set(schedule.id, parsed);
      } catch {
        continue;
      }
      if (!matchesCron(parsed, scheduledAt)) continue;
      const occurrence = this.#options.store.claimScheduleOccurrence(
        schedule.id,
        scheduledAt,
        this.#clock.now(),
        MAX_CONCURRENT_RUNS,
      );
      if (occurrence.kind !== "claimed") continue;
      const active = this.#newActive(schedule.id, occurrence.run.id);
      const operation = this.#dispatch(schedule, occurrence.run, active);
      active.promise = operation;
      this.#activeRuns.set(occurrence.run.id, active);
      void operation.finally(() => this.#activeRuns.delete(occurrence.run.id));
    }
  }

  async #dispatch(schedule: ScheduleRecord, run: ScheduleRunRecord, active: ActiveRun): Promise<void> {
    try {
      if (this.#sealed || active.controller.signal.aborted) {
        this.#options.store.settleScheduleRun(run.id, { status: "cancelled" }, this.#clock.now());
        return;
      }
      const valid = await this.#options.validateExecution(schedule, active.controller.signal).catch(() => false);
      if (!valid) {
        this.#options.store.settleScheduleRun(run.id, { status: "failed", errorText: "source or target authorization is no longer valid" }, this.#clock.now());
        return;
      }
      if (this.#sealed || active.controller.signal.aborted || !this.#options.store.beginScheduleDispatch(run.id, this.#clock.now())) {
        this.#options.store.settleScheduleRun(run.id, { status: "cancelled" }, this.#clock.now());
        return;
      }
      if (schedule.spec.kind === "script") {
        await this.#executeScript(schedule, run, active);
      } else {
        await this.#runAgent(schedule, run, active, false);
      }
    } catch {
      const current = this.#options.store.getScheduleRun(run.botUid, run.id);
      if (current?.stage !== "settled") {
        this.#options.store.settleScheduleRun(run.id, {
          status: current?.stage === "dispatch-intent"
            || current?.stage === "accepted"
            || current?.promptAccepted
            || (current?.process !== null && current?.process !== undefined && !current.processExited)
            ? "unknown"
            : "failed",
          errorText: "scheduled dispatch failed",
        }, this.#clock.now());
      }
      active.resolveReady();
    } finally {
      active.resolveReady();
      this.#activeRuns.delete(run.id);
    }
  }

  async #executeScript(schedule: ScheduleRecord, run: ScheduleRunRecord, active: ActiveRun): Promise<void> {
    const paths = await this.#prepareManifest(schedule, run);
    const authorized = await this.#options.validateExecution(schedule, active.controller.signal).catch(() => false);
    const latestRun = this.#options.store.getScheduleRun(run.botUid, run.id);
    const latestSchedule = this.#options.store.getSchedule(schedule.botUid, schedule.id);
    if (
      this.#sealed
      || active.controller.signal.aborted
      || latestRun?.stopRequested
      || latestSchedule?.status !== "active"
    ) {
      this.#options.store.settleScheduleRun(run.id, { status: "cancelled" }, this.#clock.now());
      return;
    }
    if (!authorized) {
      this.#options.store.settleScheduleRun(run.id, {
        status: "failed",
        errorText: "source or target authorization is no longer valid",
      }, this.#clock.now());
      return;
    }
    const timeout = AbortSignal.timeout(schedule.timeoutMs);
    const signal = AbortSignal.any([active.controller.signal, timeout]);
    let result: ScheduleScriptResult;
    try {
      result = await this.#scriptRunner({
        schedule,
        run,
        signal,
        manifestPath: paths.manifestPath,
        beforeSpawn: () => {
          const currentRun = this.#options.store.getScheduleRun(run.botUid, run.id);
          const currentSchedule = this.#options.store.getSchedule(schedule.botUid, schedule.id);
          if (
            this.#sealed
            || signal.aborted
            || !currentRun
            || currentRun.stopRequested
            || currentSchedule?.status !== "active"
          ) throw new Error("schedule was stopped before script spawn");
        },
        cliEnv: this.#options.cliEnv?.(),
        onSpawn: process => {
          if (!this.#options.store.setScheduleProcess(run.id, process)) throw new Error("schedule process identity was not persisted");
        },
        onExit: () => { this.#options.store.markScheduleProcessExited(run.id, this.#clock.now()); },
      });
    } catch {
      const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
      if (latest?.process !== null && latest?.processExited === false) this.#block(latest.process.cwd || schedule.cwd);
      this.#options.store.settleScheduleRun(run.id, {
        status: "unknown",
        errorText: "script execution outcome could not be confirmed",
      }, this.#clock.now());
      return;
    }
    if (result.kind === "not-started") {
      this.#options.store.settleScheduleRun(run.id, {
        status: result.reason === "cancelled" ? "cancelled" : "failed",
        errorText: result.reason === "spawn-failed" ? "script process could not be started" : null,
      }, this.#clock.now());
      return;
    }
    const output = "stdout" in result ? result.stdout : "";
    const errorOutput = "stderr" in result ? result.stderr : "";
    if (
      result.kind === "unknown"
      || ("processExited" in result && !result.processExited)
      || result.kind === "cancelled"
      || result.kind === "timed-out"
      || result.kind === "output-exceeded"
    ) {
      const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
      if (result.kind === "unknown" && result.reason === "process-identity") this.#block(schedule.cwd);
      else if (latest?.process && !latest.processExited) this.#block(latest.process.cwd || schedule.cwd);
      const errorText = result.kind === "timed-out"
        ? "script timed out with an uncertain external outcome"
        : result.kind === "output-exceeded"
          ? "script exceeded its output limit with an uncertain external outcome"
          : result.kind === "cancelled"
            ? "script was stopped after spawn; external outcome is uncertain"
            : "script process exit could not be confirmed";
      this.#options.store.settleScheduleRun(run.id, {
        status: "unknown",
        errorText,
        stdout: safeResultText(output),
        stderr: safeResultText(errorOutput),
      }, this.#clock.now());
      return;
    }
    let manifestError = false;
    try {
      await this.#persistManifest(schedule, run, paths.manifestPath);
    } catch {
      manifestError = true;
    }
    const status: ScheduleRunSettlement["status"] =
      result.exitCode === 0 && !manifestError ? "success" : "failed";
    this.#options.store.settleScheduleRun(run.id, {
      status,
      errorText: manifestError
        ? "artifact manifest was invalid or exceeded its limit"
        : result.exitCode !== 0 ? `script exited with code ${result.exitCode}` : null,
      stdout: safeResultText(output),
      stderr: safeResultText(errorOutput),
    }, this.#clock.now());
  }

  async #runAgent(schedule: ScheduleRecord, run: ScheduleRunRecord, active: ActiveRun, recovery: boolean): Promise<void> {
    if (schedule.spec.kind !== "agent") {
      this.#options.store.settleScheduleRun(run.id, { status: "failed", errorText: "agent schedule specification is missing" }, this.#clock.now());
      active.resolveReady();
      return;
    }
    let sessionDir = "";
    let sessionAccepted = run.promptAccepted;
    let promptCallStarted = recovery;
    let text = "";
    let timeoutTimer: ReturnType<typeof setTimeout> | undefined;
    const onEvent = (event: WorkerEvent): void => {
      if (event.type === "prompt_accepted") {
        sessionAccepted = true;
        this.#options.store.markSchedulePromptAccepted(run.id, this.#clock.now());
      } else if (event.type === "text" && event.final) {
        text += event.text;
        if (text.length > MAX_RESULT_CHARS) text = text.slice(-MAX_RESULT_CHARS);
      }
    };
    let spawnedProcess: WorkerProcess | null = null;
    try {
      const configuredRoot = this.#options.sessionRoot ?? join(this.#options.stateDir, "sessions");
      await mkdir(configuredRoot, { recursive: true, mode: 0o700 });
      const canonicalRoot = await realpath(configuredRoot);
      await assertScheduleSessionDirectory(canonicalRoot);
      const projectDir = projectSessionDir(canonicalRoot, schedule.cwd);
      await ensureScheduleSessionDirectory(projectDir);
      const schedulerDir = join(projectDir, "scheduler");
      await ensureScheduleSessionDirectory(schedulerDir);
      const scheduleDir = join(schedulerDir, safeSessionPathSegment(schedule.id, "schedule id"));
      await ensureScheduleSessionDirectory(scheduleDir);
      sessionDir = join(scheduleDir, safeSessionPathSegment(run.id, "run id"));
      if (recovery) {
        if (
          run.sessionFile === null
          || run.sessionId === null
          || resolve(dirname(run.sessionFile)) !== resolve(sessionDir)
        ) throw new Error("recovery session is outside its schedule run directory");
        await assertScheduleSessionDirectory(sessionDir);
        const sessionFileDetails = await lstat(run.sessionFile);
        if (!sessionFileDetails.isFile() || sessionFileDetails.isSymbolicLink()) {
          throw new Error("recovery session file is not a regular file");
        }
        const canonicalSessionFile = await realpath(run.sessionFile);
        if (canonicalSessionFile !== resolve(run.sessionFile) || dirname(canonicalSessionFile) !== resolve(sessionDir)) {
          throw new Error("recovery session file is not canonical within its run directory");
        }
        const loaded = await loadValidatedSessionState(canonicalSessionFile, run.sessionId);
        if (loaded.state.cwd !== schedule.cwd || dirname(loaded.state.sessionFile) !== resolve(sessionDir)) {
          throw new Error("recovery session state does not match the schedule run");
        }
      } else {
        if (run.sessionFile !== null || run.sessionId !== null) {
          throw new Error("new schedule occurrence already has an agent session");
        }
        await mkdir(sessionDir, { mode: 0o700 });
        await assertScheduleSessionDirectory(sessionDir);
        await chmod(sessionDir, 0o700);
        if ((await readdir(sessionDir)).length !== 0) throw new Error("new schedule run session directory is not empty");
      }
      const beforeSpawn = this.#options.store.getScheduleRun(run.botUid, run.id);
      const beforePlan = this.#options.store.getSchedule(schedule.botUid, schedule.id);
      if (
        this.#sealed
        || active.controller.signal.aborted
        || !beforeSpawn
        || beforeSpawn.stopRequested
        || beforePlan?.status === "stopped"
        || (!recovery && beforePlan?.status !== "active")
      ) {
        const preserveAcceptedRunForRestart = recovery
          && sessionAccepted
          && !beforeSpawn?.stopRequested
          && beforePlan?.status !== "stopped";
        if (this.#sealed && preserveAcceptedRunForRestart) return;
        this.#options.store.settleScheduleRun(run.id, { status: "cancelled" }, this.#clock.now());
        return;
      }
      const worker = await this.#options.workerFactory.create({
        cwd: schedule.cwd,
        sessionDir,
        ...(run.sessionFile === null ? {} : { sessionFile: run.sessionFile }),
        signal: active.controller.signal,
        onSpawn: process => {
          spawnedProcess = process;
          if (!this.#options.store.setScheduleProcess(run.id, process)) {
            throw new Error("schedule agent process identity was not persisted");
          }
        },
      });
      active.worker = worker;
      const workerState = await worker.refreshState();
      const previous = recovery
        ? { sessionId: run.sessionId!, cwd: schedule.cwd, sessionFile: run.sessionFile! }
        : { sessionId: "", cwd: schedule.cwd, sessionFile: null };
      const verifiedSession = recovery
        ? await verifyMovedSession(previous, workerState, schedule.cwd)
        : await verifyFreshSession(previous, workerState, schedule.cwd);
      if (
        verifiedSession.sessionFile === null
        || dirname(verifiedSession.sessionFile) !== resolve(sessionDir)
        || resolve(worker.sessionFile) !== verifiedSession.sessionFile
        || worker.sessionId !== verifiedSession.sessionId
        || (recovery && verifiedSession.sessionId !== run.sessionId)
      ) throw new Error("worker session identity is outside this schedule run");
      if (!this.#options.store.setScheduleAgentSession(run.id, verifiedSession.sessionId, verifiedSession.sessionFile)) {
        throw new Error("schedule agent session identity was not persisted");
      }
      active.resolveReady();
      const current = this.#options.store.getScheduleRun(run.botUid, run.id);
      const currentPlan = this.#options.store.getSchedule(schedule.botUid, schedule.id);
      if (
        this.#sealed
        || active.controller.signal.aborted
        || !current
        || current.stopRequested
        || currentPlan?.status === "stopped"
        || (!recovery && currentPlan?.status !== "active")
      ) {
        const stopped = await this.#stopWorkerAndWait(active);
        if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
        if (this.#sealed && sessionAccepted && stopped && latest?.stopRequested !== true) return;
        this.#options.store.settleScheduleRun(run.id, {
          status: stopped ? "cancelled" : "unknown",
          errorText: stopped ? null : "agent process exit could not be confirmed",
        }, this.#clock.now());
        if (!stopped) this.#block(schedule.cwd);
        return;
      }
      const stillAuthorized = await this.#options.validateExecution(schedule, active.controller.signal).catch(() => false);
      if (!stillAuthorized || active.controller.signal.aborted) {
        const stopped = await this.#stopWorkerAndWait(active);
        if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        this.#options.store.settleScheduleRun(run.id, {
          status: stopped ? "failed" : "unknown",
          errorText: stopped ? "source or target authorization changed before agent dispatch" : "agent process exit could not be confirmed",
        }, this.#clock.now());
        if (!stopped) this.#block(schedule.cwd);
        return;
      }
      const beforePrompt = this.#options.store.getScheduleRun(run.botUid, run.id);
      const planBeforePrompt = this.#options.store.getSchedule(schedule.botUid, schedule.id);
      if (
        !beforePrompt
        || beforePrompt.stopRequested
        || planBeforePrompt?.status === "stopped"
        || (!recovery && planBeforePrompt?.status !== "active")
        || active.controller.signal.aborted
      ) {
        const stopped = await this.#stopWorkerAndWait(active);
        if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
        if (this.#sealed && sessionAccepted && stopped && latest?.stopRequested !== true) return;
        this.#options.store.settleScheduleRun(run.id, {
          status: stopped ? "cancelled" : "unknown",
          errorText: stopped ? null : "agent process exit could not be confirmed",
        }, this.#clock.now());
        if (!stopped) this.#block(schedule.cwd);
        return;
      }
      const context: PromptContext = { tools: [this.createRunTool(schedule, run)] };
      timeoutTimer = setTimeout(() => {
        active.timedOut = true;
        active.controller.abort(new Error("schedule timeout"));
        this.#stopWorker(active);
      }, schedule.timeoutMs);
      promptCallStarted = true;
      if (recovery) {
        // Resume the exact accepted session through OMP's native retry/continuation path.
        await worker.resume(onEvent, context);
      } else {
        await worker.prompt(agentPrompt(schedule, run), onEvent, context);
      }
      if (!sessionAccepted) {
        const stopped = await this.#stopWorkerAndWait(active);
        if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
        this.#options.store.settleScheduleRun(run.id, {
          status: stopped ? "failed" : "unknown",
          errorText: "agent did not confirm prompt acceptance",
        }, this.#clock.now());
        if (!stopped) this.#block(schedule.cwd);
        return;
      }
      const stopped = await this.#stopWorkerAndWait(active);
      if (stopped) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
      const latest = this.#options.store.getScheduleRun(run.botUid, run.id);
      if (this.#sealed && latest?.stopRequested !== true && stopped) {
        // Service shutdown suspends accepted agent work; the next start resumes this exact session.
        return;
      }
      const settlement: ScheduleRunSettlement = !stopped
        ? { status: "unknown", errorText: "agent process exit could not be confirmed" }
        : active.timedOut
          ? { status: "unknown", errorText: "agent timed out with an uncertain external outcome", resultText: safeResultText(text) }
          : active.controller.signal.aborted && latest?.stopRequested === true && sessionAccepted
            ? { status: "unknown", errorText: "agent was stopped after prompt acceptance; external outcome is uncertain", resultText: safeResultText(text) }
            : active.controller.signal.aborted
              ? { status: "cancelled", resultText: safeResultText(text) }
              : { status: "success", resultText: safeResultText(text) };
      if (!stopped) this.#block(schedule.cwd);
      this.#options.store.settleScheduleRun(run.id, settlement, this.#clock.now());
    } catch {
      let stopped = false;
      let latest = this.#options.store.getScheduleRun(run.botUid, run.id);
      const process = spawnedProcess ?? latest?.process ?? null;
      if (active.worker) {
        stopped = await this.#stopWorkerAndWait(active);
      } else if (process === null || latest?.processExited === true) {
        stopped = true;
      } else {
        stopped = await this.#recoverProcess(process);
      }
      if (stopped && process !== null) this.#options.store.markScheduleProcessExited(run.id, this.#clock.now());
      latest = this.#options.store.getScheduleRun(run.botUid, run.id);
      const preserveForResume = this.#sealed && sessionAccepted && stopped && latest?.stopRequested !== true;
      if (preserveForResume) return;
      if (!stopped && process !== null) this.#block(process.cwd || schedule.cwd);
      const status: ScheduleRunSettlement["status"] = !stopped
        ? "unknown"
        : sessionAccepted || promptCallStarted
          ? "unknown"
          : active.controller.signal.aborted
            ? "cancelled"
            : "failed";
      this.#options.store.settleScheduleRun(run.id, {
        status,
        errorText: active.timedOut ? "agent timed out with an uncertain external outcome" : "agent execution outcome is unknown",
        resultText: safeResultText(text),
      }, this.#clock.now());
    } finally {
      if (timeoutTimer !== undefined) clearTimeout(timeoutTimer);
      active.resolveReady();
      if (this.#activeRuns.get(run.id) === active) this.#activeRuns.delete(run.id);
    }
  }

  async #prepareManifest(schedule: ScheduleRecord, run: ScheduleRunRecord): Promise<{ manifestPath: string }> {
    const stateRoot = await realpath(this.#options.stateDir);
    const schedulerDir = join(stateRoot, "scheduler");
    await ensureScheduleSessionDirectory(schedulerDir);
    const runsDir = join(schedulerDir, "runs");
    await ensureScheduleSessionDirectory(runsDir);
    const scheduleDir = join(runsDir, safeSessionPathSegment(schedule.id, "schedule id"));
    await ensureScheduleSessionDirectory(scheduleDir);
    const directory = join(scheduleDir, safeSessionPathSegment(run.id, "run id"));
    await mkdir(directory, { mode: 0o700 });
    await assertScheduleSessionDirectory(directory);
    await chmod(directory, 0o700);
    const manifestPath = join(directory, "artifacts.json");
    const handle = await open(manifestPath, "wx", 0o600);
    try {
      await handle.writeFile(JSON.stringify({ artifacts: [] }), "utf8");
      await handle.sync();
    } finally {
      await handle.close();
    }
    return { manifestPath };
  }

  async #persistManifest(schedule: ScheduleRecord, run: ScheduleRunRecord, manifestPath: string): Promise<void> {
    const file = await open(manifestPath, fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0));
    let content: string;
    try {
      const details = await file.stat();
      if (!details.isFile() || details.size > MAX_MANIFEST_BYTES) throw new Error("manifest size invalid");
      content = await file.readFile("utf8");
    } finally {
      await file.close();
    }
    if (Buffer.byteLength(content, "utf8") > MAX_MANIFEST_BYTES) throw new Error("manifest size invalid");
    const parsed: unknown = JSON.parse(content);
    if (!isObject(parsed) || !Array.isArray(parsed.artifacts) || parsed.artifacts.length > MAX_MANIFEST_ARTIFACTS) {
      throw new Error("manifest schema invalid");
    }
    for (let index = 0; index < parsed.artifacts.length; index += 1) {
      const artifact = artifactFromManifest(parsed.artifacts[index], schedule.target);
      if (!artifact) throw new Error("artifact invalid");
      const stored = this.#options.store.addScheduleArtifact(run.id, artifact, `script-manifest:${index}`, this.#clock.now());
      if (!stored) throw new Error("artifact rejected");
    }
  }

  #parseArtifact(args: Record<string, unknown>, authorizedTarget: ChatTarget): ScheduleArtifact {
    const name = safeArtifactText(args.name, "name", MAX_ARTIFACT_FIELD_CHARS);
    const type = safeArtifactText(args.type, "type", 128);
    const target = parseTarget(args.target);
    if (!sameTarget(target, authorizedTarget)) throw new Error("target must equal the plan's authorized target");
    const hasPath = typeof args.path === "string";
    const hasRemoteRef = typeof args.remoteRef === "string";
    if (hasPath === hasRemoteRef) throw new Error("provide exactly one of path or remoteRef");
    const location = hasPath
      ? { path: safeArtifactText(args.path, "path", MAX_ARTIFACT_FIELD_CHARS) }
      : { remoteRef: safeArtifactText(args.remoteRef, "remoteRef", MAX_ARTIFACT_FIELD_CHARS) };
    const receipt = args.receipt === undefined ? undefined : safeArtifactText(args.receipt, "receipt", 1_024, true);
    return { name, type, target, ...location, ...(receipt === undefined ? {} : { receipt }) };
  }

  #newActive(scheduleId: string, runId: string): ActiveRun {
    const { promise: ready, resolve } = Promise.withResolvers<void>();
    return {
      scheduleId,
      runId,
      controller: new AbortController(),
      promise: Promise.resolve(),
      ready,
      resolveReady: resolve,
      timedOut: false,
    };
  }

  #stopWorker(active: ActiveRun): void {
    if (!active.worker || active.workerStop) return;
    active.workerStop = active.worker.stop().catch(() => false);
  }
  async #stopWorkerAndWait(active: ActiveRun): Promise<boolean> {
    if (!active.worker) return false;
    this.#stopWorker(active);
    return active.workerStop ?? false;
  }

  async #stopRunsForShutdown(): Promise<void> {
    const activeRuns = [...this.#activeRuns.values()];
    for (const active of activeRuns) {
      active.controller.abort(new Error("channel stopping"));
      if (active.worker) this.#stopWorker(active);
    }
    await Promise.allSettled(activeRuns.map(active => active.promise));
  }

  async #recoverProcess(process: WorkerProcess): Promise<boolean> {
    try {
      return await this.#options.workerFactory.recover(process);
    } catch {
      return false;
    }
  }

  #classify(task: TaskRecord, record: ScheduleRecord, signal: AbortSignal): Promise<ScheduleAccessRole> {
    return this.#options.classifyAccess(task, record, signal).catch(() => "denied" as const);
  }

  #findSchedule(id: unknown): ScheduleRecord | null {
    if (typeof id !== "string" || id.length === 0 || id.length > 128) return null;
    const botUid = this.#options.botUid();
    return botUid ? this.#options.store.getSchedule(botUid, id) : null;
  }

  #requireBotUid(): string {
    const botUid = this.#options.botUid();
    if (!botUid) throw new Error("Bot identity is unavailable");
    return botUid;
  }

  #block(cwd: string): void {
    this.#options.blockDirectory?.(cwd);
  }
}

function paginationValue(value: unknown, fallback: number, maximum: number): number {
  if (value === undefined) return fallback;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new Error("pagination value is invalid");
  }
  return value;
}

function safeError(error: unknown): string {
  if (error instanceof Error) {
    const clean = redactSensitiveText(error.message).replace(/[\x00-\x1f\x7f]/gu, " ").trim();
    return clean.slice(0, 240) || "operation failed";
  }
  return "operation failed";
}

function updatedRecord(store: ScheduleCoordinatorOptions["store"], previous: ScheduleRecord): ScheduleRecord {
  return store.getSchedule(previous.botUid, previous.id) ?? previous;
}
