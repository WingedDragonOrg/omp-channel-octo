import { randomUUID } from "node:crypto";
import { lstat, mkdir, realpath, stat } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import type {
  ChannelTransport,
  ThreadListenLevel,
  ThreadSummary,
  ThreadWorkspaceRequest,
  WorktreePort,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type {
  ThreadWorkspaceProjectRecord,
  ThreadWorkspaceRecord,
} from "../store/store-records.ts";
import { runCommand } from "./worktrunk.ts";
import { worktreeSlug } from "./worktree.ts";

const MAX_THREAD_NAME_CHARS = 200;
const MAX_PROJECT_NAME_CHARS = 200;
const MAX_NORMALIZED_SLUG_CHARS = 100;
const MAX_BACKGROUND_CHARS = 500;
const MAX_ERROR_CHARS = 200;
const INPUT_CHECK_TIMEOUT_MS = 10_000;
const MAX_SHARED_SCOPE_OUTPUT_BYTES = 256 * 1024;
const DEFAULT_BACKGROUND = "在所列项目工作区中处理相关工作。";
const sharedEntryAllocationTails = new Map<string, Promise<void>>();
type WorkspaceLayout = ThreadWorkspaceRecord["layout"];

export interface ThreadWorkspaceHost {
  readonly stateDir: string;
  readonly transport: Pick<ChannelTransport, "createThread" | "updateGroupContext">;
  readonly worktrees: WorktreePort | undefined;
  readonly store: Pick<
    ChannelStore,
    | "getBinding"
    | "ensureBinding"
    | "saveThreadWorkspace"
    | "saveThreadWorkspaces"
    | "saveThreadWorkspaceIntent"
    | "addThreadWorkspaceIntentChannel"
    | "completeThreadWorkspaceIntent"
    | "failThreadWorkspaceIntent"
    | "listThreadWorkspaces"
  >;
  readonly botUid: string;
}

export interface ThreadWorkspaceScope {
  bindingCwd: string;
  name: string;
}

export interface ThreadWorkspaceCreatePlan {
  layout: WorkspaceLayout;
  projects: Array<Pick<ThreadWorkspaceProjectRecord, "name" | "repo"> & Partial<Pick<ThreadWorkspaceProjectRecord, "base">>>;
  background: string;
  /** Legacy worktree-only requests retain the `worktree` response field. */
  legacyWorktreeName?: string;
  /** Full normalized task slug shared by every project in this batch. */
  worktreeNameSeed: string;
  /** Host-derived main-group project scope used only for shared workspace placement. */
  sharedScope?: ThreadWorkspaceScope;
}

export interface ValidatedThreadCreation {
  workspace: ThreadWorkspaceCreatePlan | null;
  listen: ThreadListenLevel;
  background?: string;
}

export interface ThreadWorkspaceOutcome {
  thread: ThreadSummary;
  record: ThreadWorkspaceRecord;
  projectErrors: Array<string | undefined>;
  error?: string;
}

export interface ThreadWorkspaceBatchResult {
  layout: WorkspaceLayout;
  ready: boolean;
  threads: ThreadWorkspaceOutcome[];
  remoteFailures: Array<{ name: string; repo: string; error: string }>;
  error?: string;
  legacyWorktreeName?: string;
}

function boundedError(error: unknown, fallback: string): string {
  const message = error instanceof Error ? error.message : String(error);
  const detail = message.replace(/\s+/gu, " ").trim();
  return (detail.length > 0 ? detail : fallback).slice(0, MAX_ERROR_CHARS);
}

function optionalString(args: Record<string, unknown>, field: string): string | undefined {
  const value = args[field];
  if (value === undefined) return undefined;
  if (typeof value !== "string") throw new Error(`${field} must be a string`);
  return value;
}

function normalizedTaskSlug(name: string): string {
  const slug = worktreeSlug(name);
  if (slug.length === 0) {
    throw new Error("worktree_name must contain ASCII letters or digits");
  }
  if (slug.length > MAX_NORMALIZED_SLUG_CHARS) {
    throw new Error(
      `worktree_name normalized slug must be at most ${MAX_NORMALIZED_SLUG_CHARS} characters; provide a shorter complete semantic name`,
    );
  }
  return slug;
}

async function canonicalRepository(path: string, signal?: AbortSignal): Promise<string> {
  if (!isAbsolute(path)) throw new Error("projects[].repo must be an absolute path");
  const canonicalInput = await realpath(path);
  if (!(await stat(canonicalInput)).isDirectory()) {
    throw new Error("projects[].repo must name a Git repository directory");
  }
  const result = await runCommand(
    ["git", "-C", canonicalInput, "rev-parse", "--show-toplevel"],
    { timeoutMs: INPUT_CHECK_TIMEOUT_MS, ...(signal === undefined ? {} : { signal }) },
  );
  if (!result) throw new Error("无法执行 git 验证 projects[].repo");
  if (result.timedOut) throw new Error("projects[].repo Git 验证超时");
  if (result.aborted) throw new Error("projects[].repo Git 验证已取消");
  if (result.exitCode !== 0 || result.stdout.trim().length === 0) {
    throw new Error("projects[].repo 必须属于 Git 仓库");
  }
  return realpath(result.stdout.trim());
}

const NOT_GIT_WORKTREE = /not a git repository|must be run in a work tree/iu;

async function resolveSharedScopeName(
  host: ThreadWorkspaceHost,
  groupId: string,
  signal: AbortSignal,
  expectedScope?: ThreadWorkspaceScope,
): Promise<ThreadWorkspaceScope> {
  const binding = host.store.getBinding(host.botUid, { channelId: groupId, channelType: 2 });
  if (!binding) {
    throw new Error("主群尚无持久工作目录绑定，拒绝创建共享工作区");
  }
  if (!isAbsolute(binding.cwd)) {
    throw new Error("主群持久工作目录不是绝对路径，拒绝创建共享工作区");
  }

  let bindingCwd: string;
  try {
    bindingCwd = await realpath(binding.cwd);
    if (!(await stat(bindingCwd)).isDirectory()) throw new Error("not a directory");
  } catch {
    throw new Error("主群持久工作目录不可用，拒绝创建共享工作区");
  }
  if (expectedScope !== undefined) {
    if (bindingCwd !== expectedScope.bindingCwd) {
      throw new Error("主群工作目录绑定在准备期间已变化，拒绝创建共享工作区");
    }
    return expectedScope;
  }

  const result = await runCommand(
    ["git", "-C", bindingCwd, "worktree", "list", "--porcelain", "-z"],
    {
      timeoutMs: INPUT_CHECK_TIMEOUT_MS,
      signal,
      maxOutputBytes: { stdout: MAX_SHARED_SCOPE_OUTPUT_BYTES, stderr: 4_096 },
    },
  );
  if (!result) throw new Error("无法确认主群 Git 作用域，拒绝创建共享工作区");
  if (result.timedOut) throw new Error("主群 Git 作用域解析超时");
  if (result.aborted) throw new Error("主群 Git 作用域解析已取消");
  if (result.outputExceeded) throw new Error("主群 Git 作用域信息过大，拒绝创建共享工作区");

  let scopeRoot = bindingCwd;
  if (result.exitCode === 0) {
    const fields = result.stdout.split("\0");
    const firstWorktreeField = fields.findIndex((field) => field.startsWith("worktree "));
    if (firstWorktreeField < 0) {
      throw new Error("无法确认主群 Git 主检出根目录，拒绝创建共享工作区");
    }
    const primaryPath = fields[firstWorktreeField]!.slice("worktree ".length);
    if (!isAbsolute(primaryPath)) {
      throw new Error("无法确认主群 Git 主检出根目录，拒绝创建共享工作区");
    }
    try {
      scopeRoot = await realpath(primaryPath);
      if (!(await stat(scopeRoot)).isDirectory()) throw new Error("not a directory");
    } catch {
      throw new Error("主群 Git 主检出根目录不可用，拒绝创建共享工作区");
    }
  } else if (!NOT_GIT_WORKTREE.test(result.stderr)) {
    throw new Error("无法确认主群 Git 作用域，拒绝创建共享工作区");
  }

  const scopeName = basename(scopeRoot).toLowerCase() || "unknown";
  return { bindingCwd, name: scopeName };
}

export async function prepareThreadWorkspaceScope(
  host: ThreadWorkspaceHost,
  groupId: string,
  plan: ThreadWorkspaceCreatePlan,
  signal: AbortSignal,
): Promise<void> {
  if (plan.layout !== "shared") return;
  plan.sharedScope = await resolveSharedScopeName(host, groupId, signal);
}


async function validateSharedWorkspaceParent(
  stateDir: string,
  signal: AbortSignal,
): Promise<void> {
  let candidate = stateDir;
  const parent = join(stateDir, "thread-workspaces");
  let parentExists = true;
  try {
    await lstat(parent);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      parentExists = false;
    } else {
      throw new Error("无法验证共享工作区入口路径，拒绝创建");
    }
  }
  if (parentExists) {
    try {
      candidate = await realpath(parent);
      if (candidate !== parent) {
        throw new Error("共享工作区入口父目录不是规范路径，拒绝创建");
      }
      if (!(await stat(candidate)).isDirectory()) {
        throw new Error("共享工作区入口父目录无效，拒绝创建");
      }
    } catch {
      throw new Error("无法验证共享工作区入口路径，拒绝创建");
    }
  }

  const result = await runCommand(
    ["git", "-C", candidate, "rev-parse", "--show-toplevel"],
    { timeoutMs: INPUT_CHECK_TIMEOUT_MS, signal },
  );
  if (!result) throw new Error("无法验证共享工作区入口是否位于 Git worktree 内");
  if (result.timedOut) throw new Error("共享工作区入口 Git 验证超时");
  if (result.aborted) throw new Error("共享工作区入口 Git 验证已取消");
  if (result.exitCode === 0) {
    if (result.stdout.trim().length === 0) {
      throw new Error("无法验证共享工作区入口是否位于 Git worktree 内");
    }
    throw new Error("共享工作区入口位于 Git worktree 内，拒绝创建");
  }
}

async function validateBase(
  repo: string,
  base: string,
  signal?: AbortSignal,
): Promise<void> {
  if (base.length === 0 || base.length > 255 || /\s/u.test(base)) {
    throw new Error("projects[].base must be a non-empty Git ref without whitespace");
  }
  const result = await runCommand(
    ["git", "-C", repo, "rev-parse", "--verify", "--end-of-options", `${base}^{commit}`],
    { timeoutMs: INPUT_CHECK_TIMEOUT_MS, ...(signal === undefined ? {} : { signal }) },
  );
  if (!result) throw new Error("无法执行 git 验证 projects[].base");
  if (result.timedOut) throw new Error("projects[].base Git 验证超时");
  if (result.aborted) throw new Error("projects[].base Git 验证已取消");
  if (result.exitCode !== 0 || result.stdout.trim().length === 0) {
    throw new Error("projects[].base 必须解析为仓库中的 commit");
  }
}

/** Validates every workspace argument before create-thread can allocate a remote thread. */
export async function validateThreadCreation(
  args: Record<string, unknown>,
  callerCwd: string,
  legacyWorktreeName: string | undefined,
  signal?: AbortSignal,
): Promise<ValidatedThreadCreation> {
  for (const key of Object.keys(args)) {
    if (!["action", "name", "worktree_name", "projects", "layout", "background", "listen"].includes(key)) {
      throw new Error(`create-thread contains unsupported field: ${key}`);
    }
  }
  const listenValue = optionalString(args, "listen");
  if (
    listenValue !== undefined
    && listenValue !== "off"
    && listenValue !== "merge"
    && listenValue !== "task"
  ) {
    throw new Error("listen must be off, merge or task");
  }
  const listen = listenValue ?? "merge";
  const layout = optionalString(args, "layout");
  if (layout !== undefined && layout !== "shared" && layout !== "separate") {
    throw new Error("layout must be shared or separate");
  }
  const backgroundValue = optionalString(args, "background");
  const background = backgroundValue?.trim();
  if (
    backgroundValue !== undefined &&
    (
      background === undefined ||
      background.length === 0 ||
      background.length > MAX_BACKGROUND_CHARS ||
      /[\u0000-\u001f\u007f]/u.test(background)
    )
  ) {
    throw new Error(`background must contain 1-${MAX_BACKGROUND_CHARS} printable characters`);
  }

  const rawProjects = args.projects;
  if (rawProjects === undefined) {
    if (layout !== undefined) throw new Error("layout requires projects");
    if (legacyWorktreeName === undefined) {
      return {
        workspace: null,
        listen,
        ...(background === undefined ? {} : { background }),
      };
    }
    const repo = await realpath(callerCwd);
    if (!(await stat(repo)).isDirectory()) throw new Error("调用方工作目录不是目录");
    return {
      workspace: {
        layout: "single",
        projects: [{ name: legacyWorktreeName, repo }],
        background: background ?? DEFAULT_BACKGROUND,
        legacyWorktreeName,
        worktreeNameSeed: normalizedTaskSlug(legacyWorktreeName),
      },
      listen,
      ...(background === undefined ? {} : { background }),
    };
  }
  if (!Array.isArray(rawProjects) || rawProjects.length === 0) {
    throw new Error("projects must be a non-empty array");
  }
  if (rawProjects.length > 1 && layout === undefined) {
    throw new Error("layout is required when projects contains more than one project");
  }
  const taskName = optionalString(args, "worktree_name")?.trim();
  if (taskName === undefined || taskName.length === 0) {
    throw new Error("projects requires a full worktree_name task slug");
  }
  if (taskName.length > MAX_NORMALIZED_SLUG_CHARS) {
    throw new Error(
      `worktree_name must be at most ${MAX_NORMALIZED_SLUG_CHARS} characters; provide a shorter complete semantic name`,
    );
  }
  const taskSlug = normalizedTaskSlug(taskName);

  const rawNames = new Set<string>();
  const projectSlugs = new Set<string>();
  const parsedProjects: Array<{ name: string; repo: string; base?: string }> = [];
  for (const [index, value] of rawProjects.entries()) {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new Error(`projects[${index}] must be an object`);
    }
    const project = value as Record<string, unknown>;
    if (Object.keys(project).some((key) => key !== "name" && key !== "repo" && key !== "base")) {
      throw new Error(`projects[${index}] contains an unsupported field`);
    }
    if (typeof project.name !== "string") throw new Error(`projects[${index}].name must be a string`);
    const name = project.name.trim();
    if (name.length === 0 || name.length > MAX_PROJECT_NAME_CHARS) {
      throw new Error(`projects[${index}].name must contain 1-${MAX_PROJECT_NAME_CHARS} characters`);
    }
    const slug = worktreeSlug(name);
    if (slug.length === 0) {
      throw new Error(`projects[${index}].name must contain ASCII letters or digits`);
    }
    if (slug.length > MAX_NORMALIZED_SLUG_CHARS) {
      throw new Error(
        `projects[${index}].name normalized slug must be at most ${MAX_NORMALIZED_SLUG_CHARS} characters`,
      );
    }
    if (rawNames.has(name) || projectSlugs.has(slug)) {
      throw new Error("project names must have unique normalized ASCII slugs");
    }
    rawNames.add(name);
    projectSlugs.add(slug);
    if (typeof project.repo !== "string" || project.repo.trim().length === 0) {
      throw new Error(`projects[${index}].repo must be a non-empty absolute path`);
    }
    let base: string | undefined;
    if (project.base !== undefined) {
      if (typeof project.base !== "string") throw new Error(`projects[${index}].base must be a string`);
      base = project.base.trim();
    }
    parsedProjects.push({ name, repo: project.repo.trim(), ...(base === undefined ? {} : { base }) });
  }

  const projects: ThreadWorkspaceCreatePlan["projects"] = [];
  for (const project of parsedProjects) {
    const repo = await canonicalRepository(project.repo, signal);
    if (project.base !== undefined) await validateBase(repo, project.base, signal);
    projects.push({
      name: project.name,
      repo,
      ...(project.base === undefined ? {} : { base: project.base }),
    });
  }

  return {
    workspace: {
      layout: layout ?? "single",
      projects,
      background: background ?? DEFAULT_BACKGROUND,
      worktreeNameSeed: taskSlug,
    },
    listen,
    ...(background === undefined ? {} : { background }),
  };
}

function projectDirectoryName(project: Pick<ThreadWorkspaceProjectRecord, "name">): string {
  return worktreeSlug(project.name);
}


function renderThreadBackground(
  background: string,
  projects: readonly ThreadWorkspaceProjectRecord[],
  relatedShortIds: readonly string[],
): string {
  const lines = [`用途：${background}`];
  for (const project of projects) {
    if (!project.created || project.path === null) continue;
    lines.push(`- ${project.name}: ${project.path}`);
  }
  if (relatedShortIds.length > 0) lines.push(`相关线程：${relatedShortIds.join("、")}`);
  return lines.join("\n");
}

function workspaceRecord(
  botUid: string,
  thread: ThreadSummary,
  layout: WorkspaceLayout,
  entryPath: string | null,
  projects: readonly ThreadWorkspaceCreatePlan["projects"][number][],
  legacyWorktreeName?: string,
): ThreadWorkspaceRecord {
  return {
    botUid,
    channelId: thread.channelId,
    channelType: 5,
    layout,
    status: "preparing",
    workspaceRoot: null,
    entryPath,
    projects: projects.map((project) => ({ ...project, path: null, branch: null, created: false })),
    ...(legacyWorktreeName === undefined ? {} : { legacyWorktreeName }),
    error: null,
  };
}

function withSharedEntryPlan(
  record: ThreadWorkspaceRecord,
  workspaceRoot: string | null,
  entryPath: string | null,
  plannedEntryPath: string | null = entryPath,
): ThreadWorkspaceRecord {
  return {
    ...record,
    workspaceRoot,
    entryPath,
    projects: record.projects.map((project) => ({
      ...project,
      path: plannedEntryPath === null ? null : join(plannedEntryPath, projectDirectoryName(project)),
      branch: null,
      created: false,
    })),
  };
}

function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) return undefined;
  return typeof error.code === "string" ? error.code : undefined;
}

async function canonicalReservedPath(path: string): Promise<string> {
  if (!isAbsolute(path)) throw new Error("持久化的共享工作区路径不是绝对路径");
  let current = resolve(path);
  const missingSegments: string[] = [];
  for (;;) {
    try {
      let canonical = await realpath(current);
      for (let index = missingSegments.length - 1; index >= 0; index -= 1) {
        canonical = join(canonical, missingSegments[index]!);
      }
      return canonical;
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
      const parent = dirname(current);
      if (parent === current) throw error;
      missingSegments.push(basename(current));
      current = parent;
    }
  }
}

async function allocateSharedEntry(
  host: ThreadWorkspaceHost,
  record: ThreadWorkspaceRecord,
  taskSlug: string,
  scopeName: string,
  signal: AbortSignal,
): Promise<string> {
  const stateDir = await realpath(host.stateDir);
  return withSharedEntryAllocationLock(stateDir, async () => {
    const parent = join(stateDir, "thread-workspaces");
    await validateSharedWorkspaceParent(stateDir, signal);
    await mkdir(parent, { recursive: true });
    if (await realpath(parent) !== parent || !(await stat(parent)).isDirectory()) {
      throw new Error("共享工作区入口父目录不是规范目录，拒绝创建");
    }

    const reservedPaths = new Set<string>();
    for (const workspace of host.store.listThreadWorkspaces(host.botUid)) {
      if (workspace.channelId === record.channelId) continue;
      if (workspace.workspaceRoot !== null && workspace.workspaceRoot !== undefined) {
        reservedPaths.add(await canonicalReservedPath(workspace.workspaceRoot));
      }
      if (workspace.entryPath !== null) {
        reservedPaths.add(await canonicalReservedPath(workspace.entryPath));
      }
    }
    for (let suffix = 1; ; suffix += 1) {
      const name = suffix === 1 ? taskSlug : `${taskSlug}-${suffix}`;
      const candidate = join(parent, name);
      if (reservedPaths.has(candidate)) continue;
      try {
        await lstat(candidate);
        continue;
      } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
      }

      const entryPath = join(candidate, scopeName);
      const planned = withSharedEntryPlan(record, candidate, entryPath);
      host.store.saveThreadWorkspace(planned);
      Object.assign(record, planned);
      try {
        await mkdir(candidate);
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
        const cleared = withSharedEntryPlan(record, null, null, null);
        host.store.saveThreadWorkspace(cleared);
        Object.assign(record, cleared);
        continue;
      }

      const canonicalRoot = await realpath(candidate);
      if (canonicalRoot !== candidate || !(await stat(canonicalRoot)).isDirectory()) {
        throw new Error("共享工作区任务根路径被替换，拒绝使用");
      }
      try {
        await mkdir(entryPath);
      } catch (error) {
        if (errorCode(error) === "EEXIST") {
          const rootOnly = withSharedEntryPlan(record, candidate, null, entryPath);
          host.store.saveThreadWorkspace(rootOnly);
          Object.assign(record, rootOnly);
        }
        throw error;
      }
      const canonicalEntry = await realpath(entryPath);
      if (canonicalEntry !== entryPath || !(await stat(canonicalEntry)).isDirectory()) {
        throw new Error("共享工作区作用域入口路径被替换，拒绝使用");
      }
      return entryPath;
    }
  });
}

function withSharedEntryAllocationLock<T>(
  stateDir: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = sharedEntryAllocationTails.get(stateDir) ?? Promise.resolve();
  const current = previous.then(operation);
  const tail: Promise<void> = current.then(
    () => {},
    () => {},
  );
  sharedEntryAllocationTails.set(stateDir, tail);
  return current.finally(() => {
    if (sharedEntryAllocationTails.get(stateDir) === tail) {
      sharedEntryAllocationTails.delete(stateDir);
    }
  });
}

function isRequestedPathCandidate(candidate: string, requestedPath: string): boolean {
  if (candidate === requestedPath) return true;
  if (!candidate.startsWith(`${requestedPath}-`) || dirname(candidate) !== dirname(requestedPath)) {
    return false;
  }
  return /^-(?:[2-9]|[1-9]\d+)$/u.test(candidate.slice(requestedPath.length));
}

function ensureThreadBinding(
  host: ThreadWorkspaceHost,
  thread: ThreadSummary,
  cwd: string,
): void {
  host.store.ensureBinding({
    botUid: host.botUid,
    channelId: thread.channelId,
    channelType: 5,
    sessionId: randomUUID(),
    cwd,
    sessionFile: null,
    cwdPinned: true,
  });
}

function normalizedFailure(errors: readonly string[]): string | null {
  if (errors.length === 0) return null;
  return errors.join("; ").replace(/\s+/gu, " ").slice(0, MAX_ERROR_CHARS);
}

function addFailure(errors: string[], error: unknown, fallback: string): string {
  const detail = boundedError(error, fallback);
  errors.push(detail);
  return detail;
}

export async function createThreadWorkspaceBatch(
  host: ThreadWorkspaceHost,
  groupId: string,
  threadName: string,
  plan: ThreadWorkspaceCreatePlan,
  signal: AbortSignal,
  onThreadCreated?: (thread: ThreadSummary) => void,
): Promise<ThreadWorkspaceBatchResult> {
  if (plan.layout === "shared" && plan.projects.length === 0) {
    throw new Error("shared workspace requires at least one project");
  }
  if (plan.layout === "separate") {
    for (const project of plan.projects) {
      if (`${threadName} - ${project.name}`.length > MAX_THREAD_NAME_CHARS) {
        throw new Error(`线程名与项目名组合后必须不超过 ${MAX_THREAD_NAME_CHARS} 个字符`);
      }
    }
  }

  const stateDir = await realpath(host.stateDir);
  if (plan.layout === "shared") await validateSharedWorkspaceParent(stateDir, signal);
  const sharedScopeName = plan.layout === "shared"
    ? (await resolveSharedScopeName(host, groupId, signal, plan.sharedScope)).name
    : null;
  let entryPath: string | null = null;
  const outcomes: ThreadWorkspaceOutcome[] = [];
  const remoteFailures: ThreadWorkspaceBatchResult["remoteFailures"] = [];
  const errors: string[] = [];
  const records: ThreadWorkspaceRecord[] = [];
  let entryAvailable = plan.layout !== "shared";
  const cannotCreateResources = new Set<ThreadWorkspaceRecord>();
  const intentIds: string[] = [];
  const mappedIntents = new Map<string, string>();

  const remoteProjects = plan.layout === "separate"
    ? plan.projects.map((project) => [project])
    : [plan.projects];
  for (const [remoteIndex, projects] of remoteProjects.entries()) {
    const intentId = randomUUID();
    try {
      host.store.saveThreadWorkspaceIntent({
        id: intentId,
        botUid: host.botUid,
        groupId,
        status: "preparing",
        channelIds: [],
        error: null,
      });
      intentIds.push(intentId);
    } catch (error) {
      const detail = addFailure(errors, error, "无法持久化线程工作区创建意图");
      for (const remaining of remoteProjects.slice(remoteIndex).flat()) {
        remoteFailures.push({ name: remaining.name, repo: remaining.repo, error: detail });
      }
      break;
    }

    const threadLabel = plan.layout === "separate"
      ? `${threadName} - ${projects[0]!.name}`
      : threadName;
    let thread: ThreadSummary;
    try {
      thread = await host.transport.createThread(groupId, threadLabel, signal);
    } catch (error) {
      const detail = addFailure(errors, error, "线程创建失败");
      for (const failed of projects) {
        remoteFailures.push({ name: failed.name, repo: failed.repo, error: detail });
      }
      for (const skipped of remoteProjects.slice(remoteIndex + 1).flat()) {
        remoteFailures.push({
          name: skipped.name,
          repo: skipped.repo,
          error: "因前序线程创建结果未知，停止批次后续创建",
        });
      }
      break;
    }
    let listenError: string | undefined;
    try {
      onThreadCreated?.(thread);
    } catch (error) {
      listenError = addFailure(errors, error, "无法持久化线程监听策略");
    }

    const record = workspaceRecord(
      host.botUid,
      thread,
      plan.layout,
      entryPath,
      projects,
      plan.legacyWorktreeName,
    );
    const outcome: ThreadWorkspaceOutcome = {
      thread,
      record,
      projectErrors: projects.map(() => undefined),
    };
    outcomes.push(outcome);
    records.push(record);
    if (listenError !== undefined) {
      outcome.error = listenError;
      outcome.projectErrors = projects.map(() => listenError);
      record.status = "failed";
      record.error = listenError;
      cannotCreateResources.add(record);
    }
    let mayCreateResources = listenError === undefined;
    try {
      host.store.addThreadWorkspaceIntentChannel(host.botUid, intentId, thread.channelId);
      mappedIntents.set(thread.channelId, intentId);
    } catch (error) {
      const detail = addFailure(errors, error, "无法持久化远端线程身份");
      outcome.error = detail;
      outcome.projectErrors = projects.map(() => detail);
      cannotCreateResources.add(record);
      mayCreateResources = false;
    }
    try {
      host.store.saveThreadWorkspace(record);
    } catch (error) {
      const detail = addFailure(errors, error, "无法持久化线程工作区准备状态");
      record.status = "failed";
      record.error = detail;
      cannotCreateResources.add(record);
      outcome.error = detail;
      outcome.projectErrors = projects.map(() => detail);
      mayCreateResources = false;
    }
    if (!mayCreateResources) {
      for (const skipped of remoteProjects.slice(remoteIndex + 1).flat()) {
        remoteFailures.push({
          name: skipped.name,
          repo: skipped.repo,
          error: "因前序线程工作区所有权无法持久化，停止批次后续创建",
        });
      }
      break;
    }
  }

  if (plan.layout === "shared" && outcomes.length === 1 && host.worktrees !== undefined) {
    const outcome = outcomes[0]!;
    const record = outcome.record;
    if (!cannotCreateResources.has(record)) {
      try {
        const sharedEntryPath = await allocateSharedEntry(
          host,
          record,
          plan.worktreeNameSeed,
          sharedScopeName!,
          signal,
        );
        entryPath = sharedEntryPath;
        entryAvailable = true;
        ensureThreadBinding(host, outcome.thread, sharedEntryPath);
      } catch (error) {
        const detail = addFailure(errors, error, "共享工作区入口创建或持久化失败");
        outcome.error = detail;
      }
    }
  }

  if (host.worktrees !== undefined) {
    for (const outcome of outcomes) {
      const { thread, record } = outcome;
      if (cannotCreateResources.has(record)) {
        outcome.projectErrors = record.projects.map(() => outcome.error ?? "工作区准备状态未持久化");
        continue;
      }
      for (const [index, project] of record.projects.entries()) {
        if (plan.layout === "shared" && !entryAvailable) {
          outcome.projectErrors[index] = outcome.error ?? "共享工作区入口不可用";
          continue;
        }
        const requestedPath = plan.layout === "shared"
          ? join(entryPath!, projectDirectoryName(project))
          : undefined;
        const label = plan.worktreeNameSeed;
        const request: ThreadWorkspaceRequest = {
          baseDir: project.repo,
          label,
          threadId: thread.shortId,
          ...(requestedPath === undefined ? {} : { path: requestedPath }),
          ...(project.base === undefined ? {} : { base: project.base }),
          onPlanned: (planned) => {
            if (!isAbsolute(planned.cwd) || !isAbsolute(planned.repo) || planned.branch.length === 0) {
              throw new Error("WorktreePort 返回了无效的计划工作区");
            }
            if (!plan.legacyWorktreeName && planned.repo !== project.repo) {
              throw new Error("WorktreePort 选择的来源仓库与项目不一致");
            }
            if (requestedPath !== undefined && !isRequestedPathCandidate(planned.cwd, requestedPath)) {
              throw new Error("WorktreePort 选择的路径与共享项目目录不一致");
            }
            project.path = planned.cwd;
            project.branch = planned.branch;
            if (plan.legacyWorktreeName !== undefined) project.repo = planned.repo;
            host.store.saveThreadWorkspace(record);
          },
        };
        try {
          const workspace = await host.worktrees.create(request, signal);
          const canonicalPath = await realpath(workspace.cwd);
          const details = await stat(canonicalPath);
          if (
            canonicalPath !== workspace.cwd ||
            !details.isDirectory() ||
            workspace.branch.length === 0 ||
            (requestedPath !== undefined && !isRequestedPathCandidate(canonicalPath, requestedPath)) ||
            (project.path !== null && project.path !== canonicalPath)
          ) {
            throw new Error("WorktreePort 创建的目录与计划所有权不一致");
          }
          project.path = canonicalPath;
          project.branch = workspace.branch;
          project.created = true;
          host.store.saveThreadWorkspace(record);
          if (plan.layout !== "shared") ensureThreadBinding(host, thread, canonicalPath);
        } catch (error) {
          const detail = addFailure(errors, error, "隔离工作区创建失败");
          outcome.projectErrors[index] = detail;
          outcome.error = detail;
        }
      }
    }
  } else {
    for (const outcome of outcomes) {
      const detail = "本部署未配置隔离工作区能力";
      errors.push(detail);
      outcome.error = detail;
      outcome.projectErrors = outcome.record.projects.map(() => detail);
    }
  }

  for (const outcome of outcomes) {
    if (cannotCreateResources.has(outcome.record)) continue;
    const successfulProjects = outcome.record.projects.filter(
      (project) => project.created && project.path !== null,
    );
    try {
      await host.transport.updateGroupContext(
        { channelId: outcome.thread.channelId, channelType: 5 },
        renderThreadBackground(
          plan.background,
          successfulProjects,
          plan.layout === "separate" ? outcomes.map(({ thread }) => thread.shortId) : [],
        ),
        signal,
      );
    } catch (error) {
      const detail = addFailure(errors, error, "线程说明写入失败");
      outcome.error = detail;
    }
  }

  const firstFailure = normalizedFailure(errors);
  for (const outcome of outcomes) {
    outcome.record.status = firstFailure === null ? "ready" : "failed";
    outcome.record.error = firstFailure;
    if (firstFailure !== null) outcome.error ??= firstFailure;
  }
  let recordsPersisted = true;
  if (records.length > 0) {
    try {
      host.store.saveThreadWorkspaces(records);
    } catch (error) {
      const detail = addFailure(errors, error, "无法原子提交线程工作区状态");
      recordsPersisted = false;
      const failed = normalizedFailure(errors) ?? detail;
      for (const outcome of outcomes) {
        outcome.record.status = "failed";
        outcome.record.error = failed;
        outcome.error = failed;
      }
    }
  }

  let finalizedIntentCount = 0;
  if (recordsPersisted) {
    for (const outcome of outcomes) {
      const intentId = mappedIntents.get(outcome.thread.channelId);
      if (intentId === undefined) continue;
      try {
        if (outcome.record.status === "ready") {
          host.store.completeThreadWorkspaceIntent(host.botUid, intentId);
        } else {
          host.store.failThreadWorkspaceIntent(
            host.botUid,
            intentId,
            outcome.record.error ?? normalizedFailure(errors),
          );
        }
        finalizedIntentCount += 1;
      } catch (error) {
        const detail = addFailure(errors, error, "无法持久化线程工作区创建结果");
        outcome.record.status = "failed";
        outcome.record.error = detail;
        outcome.error = detail;
      }
    }
  }
  const intentsFinalized = finalizedIntentCount === intentIds.length;

  const finalError = normalizedFailure(errors);
  return {
    layout: plan.layout,
    ready: finalError === null && intentsFinalized && outcomes.length === remoteProjects.length,
    threads: outcomes,
    remoteFailures,
    ...(finalError === null ? {} : { error: finalError }),
    ...(plan.legacyWorktreeName === undefined ? {} : { legacyWorktreeName: plan.legacyWorktreeName }),
  };
}
