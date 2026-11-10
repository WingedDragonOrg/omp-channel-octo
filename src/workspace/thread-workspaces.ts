import { createHash, randomUUID } from "node:crypto";
import { lstat, mkdir, realpath, stat, symlink } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import type {
  ChannelTransport,
  ChatTarget,
  ThreadListenLevel,
  ThreadSummary,
  ThreadWorkspaceRequest,
  WorktreeCreateLifecycle,
  WorktreePort,
} from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type {
  BindingRecord,
  TaskRecord,
  ThreadWorkspaceOperationRecord,
  ThreadWorkspaceProjectRecord,
  ThreadWorkspaceRecord,
} from "../store/store-records.ts";
import { threadParentGroupId, threadShortId } from "../threads/thread-target.ts";
import { isTransientServerFailure } from "../errors.ts";
import { OctoError } from "../octo/error.ts";
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
function definitiveThreadCreateRejection(error: unknown): boolean {
  return error instanceof OctoError
    && error.status !== undefined
    && error.permanent
    && error.code !== undefined
    && !isTransientServerFailure(error);
}
type WorkspaceLayout = ThreadWorkspaceRecord["layout"];

export interface ThreadWorkspaceHost {
  readonly stateDir: string;
  readonly transport: Pick<ChannelTransport, "createThread" | "updateGroupContext">;
  readonly worktrees: WorktreePort | undefined;
  readonly store: Pick<
    ChannelStore,
    | "getBinding"
    | "getThreadWorkspace"
    | "ensureBindingIfMatches"
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

/** Additional durable-session capabilities used only by accepted workspace operations. */
export interface ThreadWorkspaceOperationHost extends ThreadWorkspaceHost {
  readonly store: ThreadWorkspaceHost["store"] & Pick<
    ChannelStore,
    | "getTask"
    | "getWorkerLedger"
    | "acceptThreadWorkspaceOperation"
    | "getThreadWorkspaceOperation"
    | "listThreadWorkspaceOperations"
    | "updateThreadWorkspaceOperation"
    | "finishThreadWorkspaceOperation"
  >;
  readonly moveThreadWorkspaceBinding: (
    target: ChatTarget,
    expected: BindingRecord,
    destinationCwd: string,
    workerLedgerId: string,
    signal: AbortSignal,
  ) => Promise<BindingRecord>;
  readonly scheduleThreadWorkspaceOperation: (operation: ThreadWorkspaceOperationRecord) => void;
  readonly blockOperationResources: (operation: ThreadWorkspaceOperationRecord) => void;
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
export interface ThreadWorkspaceAddPlan {
  worktreeNameSeed: string;
  projects: ThreadWorkspaceCreatePlan["projects"];
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

/** Accepts a durable append request and schedules it behind the source turn without waiting for it. */
export async function expandThreadWorkspace(
  host: ThreadWorkspaceOperationHost,
  task: TaskRecord,
  target: ChatTarget,
  args: Record<string, unknown>,
  signal: AbortSignal,
  toolCallId?: string,
): Promise<string> {
  if (
    args.action !== "thread-workspace-add"
    || Object.keys(args).some((key) =>
      key !== "action" && key !== "worktree_name" && key !== "projects" && key !== "short_id"
    )
  ) throw new Error("thread-workspace-add 参数无效");
  const groupId = threadParentGroupId(target.channelId);
  const shortId = threadShortId(target.channelId);
  if (target.channelType !== 5 || groupId === null || shortId === null) {
    throw new Error("thread-workspace-add 目标不是有效子区");
  }
  if (args.short_id !== undefined && args.short_id !== shortId) {
    throw new Error("short_id 与目标子区不匹配");
  }
  const callerGroupId = task.channelType === 2
    ? task.channelId
    : task.channelType === 5
      ? threadParentGroupId(task.channelId)
      : null;

  const persistedTask = host.store.getTask(task.id);
  if (
    task.botUid !== host.botUid
    || callerGroupId !== groupId
    || persistedTask === null
    || persistedTask.botUid !== task.botUid
    || persistedTask.channelType !== task.channelType
    || persistedTask.channelId !== task.channelId
    || persistedTask.messageId !== task.messageId
    || persistedTask.senderId !== task.senderId
    || (persistedTask.state !== "running" && (persistedTask.state as string) !== "started")
  ) throw new Error("thread-workspace-add 只能由当前运行的群或同群子区任务发起");
  const rawName = optionalString(args, "worktree_name")?.trim();
  if (rawName === undefined || rawName.length === 0) {
    throw new Error("thread-workspace-add requires worktree_name");
  }
  const validated = await validateThreadCreation(
    { action: "thread-workspace-add", worktree_name: rawName, projects: args.projects, layout: "shared" },
    "",
    undefined,
    signal,
  );
  const plan = validated.workspace;
  if (plan === null) throw new Error("thread-workspace-add requires projects");
  plan.layout = "shared";

  const nativeToolCallId = toolCallId?.trim();
  const toolId = createHash("sha256")
    .update(JSON.stringify(nativeToolCallId === undefined || nativeToolCallId.length === 0
      ? [task.id, target.channelId, rawName, plan.projects]
      : [task.id, target.channelId, nativeToolCallId]))
    .digest("hex");
  const operationId = `workspace-add-${createHash("sha256")
    .update(`${host.botUid}\0${target.channelId}\0${toolId}`)
    .digest("hex")}`;
  const prior = host.store.getThreadWorkspaceOperation(host.botUid, operationId);
  if (prior !== null) {
    if (
      prior.taskId !== task.id
      || prior.channelId !== target.channelId
      || prior.worktreeNameSeed !== plan.worktreeNameSeed
      || JSON.stringify(prior.projects) !== JSON.stringify(plan.projects)
    ) throw new Error("thread-workspace-add toolCallId was replayed with different arguments");
    if (prior.status === "queued" || prior.status === "running") {
      host.scheduleThreadWorkspaceOperation(prior);
    }
    return JSON.stringify({
      accepted: true,
      status: prior.status,
      queued: prior.status === "queued" || prior.status === "running",
      operation_id: prior.id,
    });
  }

  const workspace = host.store.getThreadWorkspace(host.botUid, target);
  const binding = host.store.getBinding(host.botUid, target);
  if (workspace === null || workspace.status !== "ready" || binding === null) {
    throw new Error("子区工作区或绑定尚未就绪，无法追加项目");
  }
  if (!host.worktrees) throw new Error("本部署未配置隔离工作区能力");
  if (workspace.projects.length === 0 || workspace.projects.some(project =>
    !project.created || project.path === null || project.branch === null
  )) throw new Error("现有子区项目未全部就绪，拒绝追加");

  const existingSlugs = new Set(workspace.projects.map(projectDirectoryName));
  for (const project of plan.projects) {
    if (existingSlugs.has(worktreeSlug(project.name))) {
      throw new Error(`project name already exists in this thread workspace: ${project.name}`);
    }
  }
  const bindingCwd = await realpath(binding.cwd);
  if (bindingCwd !== binding.cwd || !(await stat(bindingCwd)).isDirectory()) {
    throw new Error("子区绑定目录已被替换或不存在，拒绝迁移");
  }

  const converting = workspace.layout !== "shared";
  let entryPath: string | null = null;
  let workspaceRoot: string | null = null;
  let stateDir: string | null = null;
  let scopeName: string | null = null;
  if (!converting) {
    if (workspace.entryPath === null) throw new Error("shared 工作区缺少受管入口");
    entryPath = await realpath(workspace.entryPath);
    if (entryPath !== workspace.entryPath || bindingCwd !== entryPath) {
      throw new Error("子区绑定已移出受管 shared 入口，拒绝覆盖外部变更");
    }
    workspaceRoot = workspace.workspaceRoot ?? null;
  } else {
    if (!workspace.projects.some(project => pathContains(project.path!, bindingCwd))) {
      throw new Error("子区绑定已移出原项目工作区，拒绝覆盖外部变更");
    }
    for (const project of workspace.projects) await verifyOwnedProject(project, signal);
    const scopePlan: ThreadWorkspaceCreatePlan = {
      layout: "shared",
      projects: plan.projects,
      background: DEFAULT_BACKGROUND,
      worktreeNameSeed: plan.worktreeNameSeed,
    };
    await prepareThreadWorkspaceScope(host, groupId, scopePlan, signal);
    if (scopePlan.sharedScope === undefined) throw new Error("无法解析 shared 工作区作用域");
    stateDir = await realpath(host.stateDir);
    scopeName = scopePlan.sharedScope.name;
  }

  const operationInput = {
    id: operationId,
    botUid: host.botUid,
    channelType: 5 as const,
    channelId: target.channelId,
    groupId,
    toolCallId: toolId,
    taskId: task.id,
    worktreeNameSeed: plan.worktreeNameSeed,
    projects: plan.projects,
    expectedWorkspace: workspace,
    expectedBinding: binding,
    candidateBinding: { ...binding },
  };
  let accepted: { operation: ThreadWorkspaceOperationRecord; created: boolean };
  if (converting) {
    const allocationStateDir = stateDir!;
    const sharedScopeName = scopeName!;
    accepted = await withSharedEntryAllocationLock(allocationStateDir, async () => {
      const reservedPaths = new Set<string>();
      for (const owned of host.store.listThreadWorkspaces(host.botUid)) {
        if (owned.workspaceRoot !== null && owned.workspaceRoot !== undefined) {
          reservedPaths.add(owned.workspaceRoot);
        }
        if (owned.entryPath !== null) reservedPaths.add(owned.entryPath);
      }
      for (const pending of host.store.listThreadWorkspaceOperations(host.botUid)) {
        if (pending.status !== "queued" && pending.status !== "running" && pending.status !== "unknown") continue;
        if (pending.candidateWorkspace.workspaceRoot != null) {
          reservedPaths.add(pending.candidateWorkspace.workspaceRoot);
        }
        if (pending.candidateWorkspace.entryPath !== null) {
          reservedPaths.add(pending.candidateWorkspace.entryPath);
        }
      }

      const parent = join(allocationStateDir, "thread-workspaces");
      for (let suffix = 1; ; suffix += 1) {
        const name = suffix === 1 ? plan.worktreeNameSeed : `${plan.worktreeNameSeed}-${suffix}`;
        const candidateRoot = join(parent, name);
        if (reservedPaths.has(candidateRoot)) continue;
        try {
          await lstat(candidateRoot);
          continue;
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
        }
        const candidateEntry = join(candidateRoot, sharedScopeName);
        try {
          return host.store.acceptThreadWorkspaceOperation({
            ...operationInput,
            candidateWorkspace: expansionCandidateWorkspace(
              workspace,
              plan.projects,
              candidateEntry,
              candidateRoot,
              true,
            ),
          });
        } catch (error) {
          if (error instanceof Error && error.message === "shared workspace root is already reserved") {
            reservedPaths.add(candidateRoot);
            continue;
          }
          throw error;
        }
      }
    });
  } else {
    accepted = host.store.acceptThreadWorkspaceOperation({
      ...operationInput,
      candidateWorkspace: expansionCandidateWorkspace(
        workspace,
        plan.projects,
        entryPath!,
        workspaceRoot,
        false,
      ),
    });
  }
  host.scheduleThreadWorkspaceOperation(accepted.operation);
  return JSON.stringify({
    accepted: true,
    status: accepted.operation.status,
    queued: accepted.operation.status === "queued" || accepted.operation.status === "running",
    operation_id: accepted.operation.id,
  });
}

function expansionCandidateWorkspace(
  existing: ThreadWorkspaceRecord,
  requested: ThreadWorkspaceCreatePlan["projects"],
  entryPath: string,
  workspaceRoot: string | null,
  converting: boolean,
): ThreadWorkspaceRecord {
  const additions: ThreadWorkspaceProjectRecord[] = requested.map(project => ({
    ...project,
    path: join(entryPath, projectDirectoryName(project)),
    branch: null,
    created: false,
  }));
  const existingProjects = existing.projects.map(project => converting
    ? {
      ...project,
      accessPath: join(entryPath, projectDirectoryName(project)),
      accessTarget: project.path!,
    }
    : { ...project });
  return {
    ...existing,
    layout: "shared",
    status: "preparing",
    workspaceRoot,
    entryPath,
    error: null,
    projects: [...existingProjects, ...additions],
  };
}

/** Runs one already-accepted operation; all uncertain side effects remain durable and blocked. */
export async function runThreadWorkspaceOperation(
  host: ThreadWorkspaceOperationHost,
  operationId: string,
  signal: AbortSignal,
  isUserCancellationRequested: () => boolean = () => false,
): Promise<void> {
  const initialOperation = host.store.getThreadWorkspaceOperation(host.botUid, operationId);
  if (
    initialOperation === null
    || (
      initialOperation.status !== "queued"
      && !(initialOperation.status === "running" && initialOperation.cancelRequested)
    )
  ) return;
  let operation: ThreadWorkspaceOperationRecord = initialOperation;
  if (operation.status === "queued") {
    operation = updateWorkspaceOperation(host, operation, { status: "running" });
    operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
  }
  let confirmedEffects = operation.phase === "entry-root-created"
    || operation.phase === "entry-create-intent"
    || operation.phase === "entry-created"
    || operation.phase === "alias-created"
    || operation.phase === "project-created"
    || operation.phase === "move-confirmed"
    || operation.candidateWorkspace.projects.some(project =>
      project.created && operation.projects.some(requested => requested.name === project.name)
    );
  const cancelWithoutFurtherEffects = async (detail: string): Promise<void> => {
    operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
    if (operation.status === "unknown") {
      host.blockOperationResources(operation);
      return;
    }
    if (operation.status !== "running") return;
    if (
      operation.processIdentity !== null
      || operation.phase === "entry-creating"
      || operation.phase === "alias-creating"
      || operation.phase === "entry-create-intent"
      || operation.phase === "project-creating"
    ) {
      markWorkspaceOperationUnknown(host, operation, "取消时资源创建结果未知");
      return;
    }
    if (operation.phase === "move-dispatch-intent" && operation.workerLedgerId !== null) {
      const ledger = host.store.getWorkerLedger(host.botUid, operation.workerLedgerId);
      if (ledger !== null) {
        markWorkspaceOperationUnknown(host, operation, "取消时原生 /move 结果未知");
        return;
      }
    }
    if (operation.phase === "project-process-exited") {
      const reconciled = await reconcileExitedProject(host, operation);
      if (reconciled === "unknown") {
        markWorkspaceOperationUnknown(host, operation, "worktree 进程已退出但创建结果无法确认");
        return;
      }
      operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
      if (reconciled === "created") confirmedEffects = true;
      else operation = updateWorkspaceOperation(host, operation, { phase: "project-not-started" });
    }
    if (operation.phase === "move-confirmed") {
      const completed = host.store.finishThreadWorkspaceOperation(
        {
          ...operation,
          candidateWorkspace: { ...operation.candidateWorkspace, status: "ready", error: null },
        },
        "complete",
        true,
        true,
      );
      if (completed === null) markWorkspaceOperationUnknown(host, operation, "已确认的 /move 无法原子提交");
      return;
    }
    finishWorkspaceOperation(host, operation, "cancelled", confirmedEffects, false, detail);
  };
  try {
    const task = host.store.getTask(operation.taskId);
    if (
      task?.state === "cancelled"
      || operation.cancelRequested
      || isUserCancellationRequested()
    ) {
      await cancelWithoutFurtherEffects(
        task?.state === "cancelled" ? "来源任务已取消" : "目标聊天已取消子区工作区追加操作",
      );
      return;
    }
    assertWorkspaceOperationSnapshot(host, operation);
    if (!host.worktrees) throw new Error("本部署未配置隔离工作区能力");
    if (operation.phase === "project-process-exited") {
      const reconciled = await reconcileExitedProject(host, operation);
      if (reconciled === "unknown") {
        markWorkspaceOperationUnknown(host, operation, "已退出的 worktree 结果无法确认");
        return;
      }
      operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
      if (reconciled === "created") confirmedEffects = true;
      else operation = updateWorkspaceOperation(host, operation, { phase: "project-not-started" });
    }
    if (
      host.store.getTask(operation.taskId)?.state === "cancelled"
      || host.store.getThreadWorkspaceOperation(host.botUid, operation.id)?.cancelRequested
      || isUserCancellationRequested()
    ) {
      await cancelWithoutFurtherEffects(
        host.store.getTask(operation.taskId)?.state === "cancelled"
          ? "来源任务已取消"
          : "目标聊天已取消子区工作区追加操作",
      );
      return;
    }

    const converting = operation.expectedWorkspace.layout !== "shared";
    if (converting) {
      await ensureExpansionEntry(host, operation, signal, next => {
        operation = next;
        if (next.phase === "entry-root-created") confirmedEffects = true;
      });
      await ensureExpansionAliases(host, operation, signal, next => { operation = next; });
      confirmedEffects = true;
    }
    await createExpansionProjects(host, operation, signal, next => {
      operation = next;
      if (next.phase === "project-created") confirmedEffects = true;
    });
    if (operation.status === "unknown") {
      host.blockOperationResources(operation);
      return;
    }
    operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
    if (
      host.store.getTask(operation.taskId)?.state === "cancelled"
      || operation.cancelRequested
      || isUserCancellationRequested()
    ) {
      await cancelWithoutFurtherEffects(
        host.store.getTask(operation.taskId)?.state === "cancelled"
          ? "来源任务已取消"
          : "目标聊天已取消子区工作区追加操作",
      );
      return;
    }

    if (converting && operation.phase !== "move-confirmed") {
      if (signal.aborted) throw signal.reason ?? new Error("workspace expansion aborted");
      const workerLedgerId = randomUUID();
      operation = updateWorkspaceOperation(host, operation, {
        phase: "move-dispatch-intent",
        workerLedgerId,
      });
      const moved = await host.moveThreadWorkspaceBinding(
        { channelType: 5, channelId: operation.channelId },
        operation.expectedBinding,
        operation.candidateWorkspace.entryPath!,
        workerLedgerId,
        signal,
      );
      operation = updateWorkspaceOperation(host, operation, {
        candidateBinding: moved,
        phase: "move-confirmed",
        workerLedgerId: null,
      });
      confirmedEffects = true;
      operation = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
      if (operation.cancelRequested || isUserCancellationRequested()) {
        await cancelWithoutFurtherEffects("目标聊天已取消子区工作区追加操作");
        return;
      }
    }

    operation = {
      ...operation,
      candidateWorkspace: {
        ...operation.candidateWorkspace,
        status: "ready",
        error: null,
        projects: operation.candidateWorkspace.projects.map(project => ({ ...project })),
      },
    };
    const completed = host.store.finishThreadWorkspaceOperation(
      operation,
      "complete",
      true,
      converting,
    );
    if (completed === null) {
      markWorkspaceOperationUnknown(host, operation, "绑定或工作区所有权发生变化，无法原子发布");
    }
  } catch (error) {
    const current = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
    if (current.status === "unknown") return;
    operation = current;
    const aborted = signal.aborted;
    const userCancelled = current.cancelRequested
      || isUserCancellationRequested()
      || host.store.getTask(current.taskId)?.state === "cancelled";
    if (
      current.phase === "entry-creating"
      || current.phase === "entry-create-intent"
      || current.phase === "alias-creating"
      || current.phase === "project-creating"
    ) {
      markWorkspaceOperationUnknown(host, current, "资源创建阶段中断，副作用结果未知");
      return;
    }
    if ((aborted || userCancelled) && current.processIdentity !== null) {
      markWorkspaceOperationUnknown(host, current, "操作取消时 worktree 子进程结果未知");
      return;
    }
    if (current.phase === "move-dispatch-intent" && current.workerLedgerId !== null) {
      const ledger = host.store.getWorkerLedger(host.botUid, current.workerLedgerId);
      if (ledger !== null) {
        markWorkspaceOperationUnknown(host, current, "原生 /move 已派发，结果未知");
        return;
      }
    }
    if (userCancelled) {
      await cancelWithoutFurtherEffects(
        host.store.getTask(current.taskId)?.state === "cancelled"
          ? "来源任务已取消"
          : "目标聊天已取消子区工作区追加操作",
      );
      return;
    }
    if (aborted && current.phase === "project-process-exited") {
      const reconciled = await reconcileExitedProject(host, current).catch(() => "unknown");
      if (reconciled === "unknown") {
        markWorkspaceOperationUnknown(host, current, "worktree 进程已退出但创建结果无法确认");
        return;
      }
      if (reconciled === "created") {
        confirmedEffects = true;
        operation = host.store.getThreadWorkspaceOperation(host.botUid, current.id) ?? current;
      } else {
        operation = updateWorkspaceOperation(host, current, { phase: "project-not-started" });
      }
    }
    if (aborted) {
      updateWorkspaceOperation(host, operation, { status: "queued", error: null });
      return;
    }
    const detail = boundedError(error, "thread workspace expansion failed");
    if (confirmedEffects) {
      operation = {
        ...operation,
        candidateWorkspace: {
          ...operation.candidateWorkspace,
          status: "failed",
          error: detail,
        },
        error: detail,
      };
      const finalized = host.store.finishThreadWorkspaceOperation(
        operation,
        "failed",
        true,
        false,
      );
      if (finalized === null) markWorkspaceOperationUnknown(host, operation, detail);
      return;
    }
    finishWorkspaceOperation(host, operation, "failed", false, false, detail);
  }
}

async function ensureExpansionEntry(
  host: ThreadWorkspaceOperationHost,
  initial: ThreadWorkspaceOperationRecord,
  signal: AbortSignal,
  update: (operation: ThreadWorkspaceOperationRecord) => void,
): Promise<void> {
  let operation = initial;
  const root = operation.candidateWorkspace.workspaceRoot;
  const entry = operation.candidateWorkspace.entryPath;
  if (root == null || entry == null || !pathContains(root, entry)) {
    throw new Error("候选 shared 入口路径无效");
  }
  if (operation.phase === "accepted") {
    operation = updateWorkspaceOperation(host, operation, { phase: "entry-creating" });
    update(operation);
    await validateSharedWorkspaceParent(await realpath(host.stateDir), signal);
    await mkdir(dirname(root), { recursive: true });
    const parent = await realpath(dirname(root));
    if (parent !== dirname(root)) throw new Error("共享工作区任务根父目录被替换");
    await mkdir(root);
    if (await realpath(root) !== root || !(await stat(root)).isDirectory()) {
      throw new Error("共享工作区任务根路径被替换");
    }
    operation = updateWorkspaceOperation(host, operation, { phase: "entry-root-created" });
    update(operation);
  } else if (operation.phase === "entry-root-created") {
    if (await realpath(root) !== root || !(await stat(root)).isDirectory()) {
      throw new Error("已登记的共享工作区任务根丢失或被替换");
    }
  } else if (operation.phase === "entry-create-intent") {
    throw new Error("共享工作区作用域入口创建结果未知");
  } else if (operation.phase === "entry-created" || operation.phase === "alias-created"
    || operation.phase === "project-not-started" || operation.phase === "project-created"
    || operation.phase === "move-confirmed") {
    return;
  } else if (operation.phase !== "entry-creating") {
    throw new Error("共享工作区入口处于未知阶段");
  } else {
    throw new Error("共享工作区根目录创建结果未知");
  }
  if (signal.aborted) throw signal.reason ?? new Error("workspace expansion aborted");
  const rootReal = await realpath(root);
  if (rootReal !== root || !(await stat(rootReal)).isDirectory()) {
    throw new Error("共享工作区任务根路径无效");
  }
  operation = updateWorkspaceOperation(host, operation, { phase: "entry-create-intent" });
  update(operation);
  try {
    await mkdir(entry);
  } catch (error) {
    operation = updateWorkspaceOperation(host, operation, { phase: "entry-root-created" });
    update(operation);
    throw error;
  }
  if (await realpath(entry) !== entry || !(await stat(entry)).isDirectory()) {
    throw new Error("共享工作区作用域入口被替换");
  }
  operation = updateWorkspaceOperation(host, operation, { phase: "entry-created" });
  update(operation);
}

async function ensureExpansionAliases(
  host: ThreadWorkspaceOperationHost,
  initial: ThreadWorkspaceOperationRecord,
  signal: AbortSignal,
  update: (operation: ThreadWorkspaceOperationRecord) => void,
): Promise<void> {
  let operation = initial;
  for (const project of operation.candidateWorkspace.projects) {
    if (project.accessPath === undefined || project.accessTarget === undefined) continue;
    if (signal.aborted) throw signal.reason ?? new Error("workspace expansion aborted");
    const target = await realpath(project.accessTarget);
    if (target !== project.accessTarget) throw new Error("原项目工作区路径已变化，拒绝创建别名");
    try {
      const details = await lstat(project.accessPath);
      if (!details.isSymbolicLink() || await realpath(project.accessPath) !== target) {
        throw new Error("受管项目别名已存在且目标不匹配，拒绝覆盖");
      }
      continue;
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    operation = updateWorkspaceOperation(host, operation, { phase: "alias-creating" });
    update(operation);
    await symlink(target, project.accessPath, "dir");
    const link = await lstat(project.accessPath);
    if (!link.isSymbolicLink() || await realpath(project.accessPath) !== target) {
      throw new Error("受管项目别名创建后验证失败");
    }
    operation = updateWorkspaceOperation(host, operation, { phase: "alias-created" });
    update(operation);
  }
}

async function createExpansionProjects(
  host: ThreadWorkspaceOperationHost,
  initial: ThreadWorkspaceOperationRecord,
  signal: AbortSignal,
  update: (operation: ThreadWorkspaceOperationRecord) => void,
): Promise<void> {
  let operation = initial;
  const shortId = threadShortId(operation.channelId);
  if (shortId === null) throw new Error("workspace operation has invalid thread identity");
  for (const requested of operation.projects) {
    let project = operation.candidateWorkspace.projects.find(item => item.name === requested.name);
    if (project === undefined) throw new Error("候选项目所有权记录丢失");
    if (project.created) continue;
    if (signal.aborted) throw signal.reason ?? new Error("workspace expansion aborted");
    if (project.path === null) throw new Error("候选 worktree 路径未持久化");
    const requestedPath = project.path;
    operation = updateWorkspaceOperation(host, operation, { phase: "project-not-started" });
    update(operation);
    const lifecycle: WorktreeCreateLifecycle = {
      beforeSpawn: cwd => {
        if (cwd !== requested.repo) throw new Error("worktree create cwd differs from the validated repository");
        operation = updateWorkspaceOperation(host, operation, {
          phase: "project-creating",
          processIdentity: null,
        });
        update(operation);
      },
      onSpawn: process => {
        if (process.cwd !== requested.repo) throw new Error("worktree process cwd differs from its repository");
        operation = updateWorkspaceOperation(host, operation, {
          phase: "project-creating",
          processIdentity: process,
        });
        update(operation);
      },
      onConfirmedExit: () => {
        operation = updateWorkspaceOperation(host, operation, {
          phase: "project-process-exited",
          processIdentity: null,
        });
        update(operation);
      },
      onNotSpawned: () => {
        operation = updateWorkspaceOperation(host, operation, {
          phase: "project-not-started",
          processIdentity: null,
        });
        update(operation);
      },
      onUnknown: () => {
        operation = updateWorkspaceOperation(host, operation, {
          status: "unknown",
          error: "worktree create child process outcome is unknown",
        });
        update(operation);
      },
    };
    const request: ThreadWorkspaceRequest = {
      baseDir: requested.repo,
      label: operation.worktreeNameSeed,
      threadId: shortId,
      path: requestedPath,
      ...(requested.base === undefined ? {} : { base: requested.base }),
      onPlanned: planned => {
        if (
          !isAbsolute(planned.cwd)
          || planned.cwd !== requestedPath
          || planned.repo !== requested.repo
          || planned.branch.length === 0
        ) throw new Error("WorktreePort planned a path/repository outside the durable candidate");
        const projects = operation.candidateWorkspace.projects.map(item => item.name === requested.name
          ? { ...item, path: planned.cwd, branch: planned.branch, created: false }
          : item);
        operation = updateWorkspaceOperation(host, operation, {
          candidateWorkspace: { ...operation.candidateWorkspace, projects },
        });
        update(operation);
      },
      lifecycle,
    };
    try {
      const created = await host.worktrees!.create(request, signal);
      const canonical = await realpath(created.cwd);
      const plannedProject = operation.candidateWorkspace.projects.find(item => item.name === requested.name);
      if (
        plannedProject === undefined
        || canonical !== requestedPath
        || !(await stat(canonical)).isDirectory()
        || created.branch.length === 0
        || (plannedProject.branch !== null && plannedProject.branch !== created.branch)
      ) throw new Error("WorktreePort 创建路径与已登记候选不一致");
      await verifyOwnedProject({
        ...plannedProject,
        path: canonical,
        branch: created.branch,
        created: true,
      }, signal);
      const projects = operation.candidateWorkspace.projects.map(item => item.name === requested.name
        ? { ...item, path: canonical, branch: created.branch, created: true }
        : item);
      operation = updateWorkspaceOperation(host, operation, {
        candidateWorkspace: { ...operation.candidateWorkspace, projects },
        phase: "project-created",
        processIdentity: null,
      });
      update(operation);
    } catch (error) {
      const current = host.store.getThreadWorkspaceOperation(host.botUid, operation.id) ?? operation;
      if (current.status === "unknown" || current.processIdentity !== null) {
        if (current.status !== "unknown") markWorkspaceOperationUnknown(host, current, "worktree create result is unknown");
        update(host.store.getThreadWorkspaceOperation(host.botUid, current.id) ?? {
          ...current,
          status: "unknown",
        });
        return;
      }
      if (current.phase === "project-process-exited") {
        const reconciled = await reconcileExitedProject(host, current).catch(() => "unknown");
        if (reconciled === "created") {
          update(host.store.getThreadWorkspaceOperation(host.botUid, current.id) ?? current);
          continue;
        }
        if (reconciled === "unknown") {
          markWorkspaceOperationUnknown(host, current, "worktree process exited but result cannot be verified");
          update(host.store.getThreadWorkspaceOperation(host.botUid, current.id) ?? current);
          return;
        }
        operation = updateWorkspaceOperation(host, current, { phase: "project-not-started" });
        update(operation);
      }
      throw error;
    }
  }
}

async function reconcileExitedProject(
  host: ThreadWorkspaceOperationHost,
  operation: ThreadWorkspaceOperationRecord,
): Promise<"created" | "absent" | "unknown"> {
  const project = operation.candidateWorkspace.projects.find(item =>
    operation.projects.some(requested => requested.name === item.name)
  && !item.created
  );
  if (project === undefined || project.path === null || project.branch === null) return "unknown";
  try {
    const path = await realpath(project.path);
    if (path !== project.path || !(await stat(path)).isDirectory()) return "unknown";
  } catch (error) {
    return errorCode(error) === "ENOENT" ? "absent" : "unknown";
  }
  try {
    await verifyOwnedProject({ ...project, created: true }, new AbortController().signal);
  } catch {
    return "unknown";
  }
  const projects = operation.candidateWorkspace.projects.map(item => item.name === project.name
    ? { ...item, created: true }
    : item);
  const next = host.store.updateThreadWorkspaceOperation({
    ...operation,
    candidateWorkspace: { ...operation.candidateWorkspace, projects },
    phase: "project-created",
    status: "running",
    processIdentity: null,
  });
  if (next === null) return "unknown";
  return "created";
}

async function verifyOwnedProject(
  project: ThreadWorkspaceProjectRecord,
  signal: AbortSignal,
): Promise<void> {
  if (project.path === null || project.branch === null || !isAbsolute(project.path)) {
    throw new Error("project lacks a durable worktree path or branch");
  }
  const path = await realpath(project.path);
  if (path !== project.path || !(await stat(path)).isDirectory()) {
    throw new Error("project worktree path was replaced or removed");
  }
  const top = await runCommand(
    ["git", "-C", path, "rev-parse", "--show-toplevel"],
    { timeoutMs: INPUT_CHECK_TIMEOUT_MS, signal },
  );
  if (!top || top.exitCode !== 0 || top.timedOut || top.aborted || top.stdout.trim() !== path) {
    throw new Error("project path is not the recorded Git worktree");
  }
  const branch = await runCommand(
    ["git", "-C", path, "branch", "--show-current"],
    { timeoutMs: INPUT_CHECK_TIMEOUT_MS, signal },
  );
  if (!branch || branch.exitCode !== 0 || branch.timedOut || branch.aborted
    || branch.stdout.trim() !== project.branch) {
    throw new Error("project branch differs from its recorded ownership");
  }
  const commonDir = async (cwd: string): Promise<string> => {
    const result = await runCommand(
      ["git", "-C", cwd, "rev-parse", "--git-common-dir"],
      { timeoutMs: INPUT_CHECK_TIMEOUT_MS, signal },
    );
    if (!result || result.exitCode !== 0 || result.timedOut || result.aborted) {
      throw new Error("unable to verify project Git common directory");
    }
    return realpath(resolve(cwd, result.stdout.trim()));
  };
  if (await commonDir(path) !== await commonDir(project.repo)) {
    throw new Error("project worktree belongs to a different repository");
  }
}

function assertWorkspaceOperationSnapshot(
  host: ThreadWorkspaceOperationHost,
  operation: ThreadWorkspaceOperationRecord,
): void {
  const workspace = host.store.getThreadWorkspace(host.botUid, {
    channelType: 5,
    channelId: operation.channelId,
  });
  const binding = host.store.getBinding(host.botUid, {
    channelType: 5,
    channelId: operation.channelId,
  });
  if (
    JSON.stringify(workspace) !== JSON.stringify(operation.expectedWorkspace)
    || !sameWorkspaceBinding(binding, operation.expectedBinding)
  ) throw new Error("绑定或工作区所有权已变化，拒绝追加");
}

function sameWorkspaceBinding(left: BindingRecord | null, right: BindingRecord): boolean {
  return left !== null
    && left.botUid === right.botUid
    && left.channelType === right.channelType
    && left.channelId === right.channelId
    && left.sessionId === right.sessionId
    && left.cwd === right.cwd
    && left.sessionFile === right.sessionFile
    && (left.cwdPinned === true) === (right.cwdPinned === true)
    && (left.sessionSwitchedAt ?? null) === (right.sessionSwitchedAt ?? null)
    && JSON.stringify(left.sessionInitialization ?? null) === JSON.stringify(right.sessionInitialization ?? null);
}

function updateWorkspaceOperation(
  host: ThreadWorkspaceOperationHost,
  operation: ThreadWorkspaceOperationRecord,
  changes: Partial<Pick<
    ThreadWorkspaceOperationRecord,
    "candidateWorkspace" | "candidateBinding" | "phase" | "status" | "workerLedgerId" | "processIdentity" | "error"
  >>,
): ThreadWorkspaceOperationRecord {
  const updated = host.store.updateThreadWorkspaceOperation({ ...operation, ...changes });
  if (updated === null) throw new Error("thread workspace operation CAS failed");
  return updated;
}

function markWorkspaceOperationUnknown(
  host: ThreadWorkspaceOperationHost,
  operation: ThreadWorkspaceOperationRecord,
  error: string,
): void {
  if (operation.status === "unknown") {
    host.blockOperationResources(operation);
    return;
  }
  let durable = operation;
  try {
    durable = updateWorkspaceOperation(host, operation, {
      status: "unknown",
      error: boundedError(error, "thread workspace operation outcome is unknown"),
    });
  } catch {
    // The prior durable running record still blocks the thread; never release an unconfirmed effect.
  }
  host.blockOperationResources(durable);
}

function finishWorkspaceOperation(
  host: ThreadWorkspaceOperationHost,
  operation: ThreadWorkspaceOperationRecord,
  status: "failed" | "cancelled",
  publishCandidate: boolean,
  publishBinding: boolean,
  error: string,
): void {
  const candidateWorkspace = publishCandidate
    ? { ...operation.candidateWorkspace, status: "failed" as const, error }
    : operation.candidateWorkspace;
  const finished = host.store.finishThreadWorkspaceOperation(
    { ...operation, candidateWorkspace, error },
    status,
    publishCandidate,
    publishBinding,
  );
  if (finished === null) markWorkspaceOperationUnknown(host, operation, "operation finalization CAS failed");
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
function pathContains(parent: string, child: string): boolean {
  const resolved = relative(parent, child);
  return resolved === ""
    || (!isAbsolute(resolved) && resolved !== ".." && !resolved.startsWith(`..${sep}`));
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
  if (!host.store.ensureBindingIfMatches({
    botUid: host.botUid,
    channelId: thread.channelId,
    channelType: 5,
    sessionId: randomUUID(),
    cwd,
    sessionFile: null,
    cwdPinned: true,
  }, null)) {
    throw new Error(`thread ${thread.channelId} acquired a binding while its workspace was being created`);
  }
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
  creator: ChatTarget = { channelType: 2, channelId: groupId },
  beforeRemoteCreate?: () => Promise<void>,
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
        creatorChannelType: onThreadCreated === undefined ? null : creator.channelType as 2 | 5,
        creatorChannelId: onThreadCreated === undefined ? null : creator.channelId,
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
    if (signal.aborted) {
      const detail = "线程创建调用前已取消；未创建远端子区";
      host.store.failThreadWorkspaceIntent(host.botUid, intentId, detail);
      for (const failed of projects) {
        remoteFailures.push({ name: failed.name, repo: failed.repo, error: detail });
      }
      break;
    }
    if (beforeRemoteCreate !== undefined) {
      try {
        await beforeRemoteCreate();
      } catch (error) {
        host.store.failThreadWorkspaceIntent(
          host.botUid,
          intentId,
          boundedError(error, "线程创建前校验失败"),
        );
        throw error;
      }
    }
    try {
      thread = await host.transport.createThread(groupId, threadLabel, signal);
    } catch (error) {
      const detail = addFailure(errors, error, "线程创建失败");
      const definitive = definitiveThreadCreateRejection(error);
      if (definitive) host.store.failThreadWorkspaceIntent(host.botUid, intentId, detail);
      for (const failed of projects) {
        remoteFailures.push({ name: failed.name, repo: failed.repo, error: detail });
      }
      for (const skipped of remoteProjects.slice(remoteIndex + 1).flat()) {
        remoteFailures.push({
          name: skipped.name,
          repo: skipped.repo,
          error: definitive
            ? "因前序线程创建被明确拒绝，停止批次后续创建"
            : "因前序线程创建结果未知，停止批次后续创建",
        });
      }
      break;
    }
    let listenError: string | undefined;

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
    let mayCreateResources = true;
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
    if (mayCreateResources) {
      try {
        onThreadCreated?.(thread);
      } catch (error) {
        listenError = addFailure(errors, error, "无法持久化线程监听策略");
        outcome.error = listenError;
        outcome.projectErrors = projects.map(() => listenError);
        record.status = "failed";
        record.error = listenError;
        cannotCreateResources.add(record);
        mayCreateResources = false;
      }
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
