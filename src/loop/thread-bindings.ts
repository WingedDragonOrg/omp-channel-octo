import { randomUUID } from "node:crypto";
import type { ChatTarget } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import type {
  LoopBinding,
  LoopReadPort,
  LoopTask,
  LoopTaskKey,
  LoopThreadCreation,
} from "./types.ts";

export const LOOP_INPUT_MESSAGE_PREFIX = "loop:";

const CLOSED_LOOP_TASK_STATUSES: Readonly<Record<string, true>> = {
  backlog: true,
  done: true,
  cancelled: true,
};

export interface ResolvedLoopTask {
  binding: LoopBinding;
  key: LoopTaskKey;
  task: LoopTask;
}

/** Validates tasks in the current workspace and fences durable, single-flight thread creation. */
export class LoopThreadBindings {
  private readonly activeCreations = new Map<string, Promise<unknown>>();

  constructor(
    private readonly store: ChannelStore,
    private readonly read: LoopReadPort,
    readonly botUid: string,
  ) {}

  /** A reservation without a confirmed remote identity is ambiguous after restart. */
  async recover(): Promise<void> {
    for (const creation of this.store.listLoopThreadCreations(this.botUid)) {
      if (creation.state !== "reserved") continue;
      this.store.markLoopThreadCreationUnknown(
        this.keyOf(creation),
        creation.operationId,
        "创建预留在远端身份确认前中断；远端可能已创建子区。",
      );
    }
  }

  async resolveTask(groupId: string, taskId: string, signal?: AbortSignal): Promise<ResolvedLoopTask> {
    const binding = this.store.getLoopBinding(this.botUid, groupId);
    if (binding === null) throw new Error(`群 ${groupId} 尚未绑定 Loop workspace`);
    const task = await this.read.getTask(binding.workspaceId, taskId, signal);
    if (task.taskId !== taskId) throw new Error("Loop 任务读取返回了不同的 task ID");
    return {
      binding,
      key: { botUid: this.botUid, groupId, workspaceId: binding.workspaceId, taskId },
      task,
    };
  }

  /** Resolves durable Loop provenance and current status before automatic thread work. */
  async resolveAutomaticTask(
    messageId: string,
    target: ChatTarget,
    groupId: string,
    taskId: string,
    signal?: AbortSignal,
  ): Promise<ResolvedLoopTask> {
    if (!messageId.startsWith(LOOP_INPUT_MESSAGE_PREFIX)) {
      throw new Error("automatic Loop input provenance could not be verified");
    }
    const inputId = messageId.slice(LOOP_INPUT_MESSAGE_PREFIX.length);
    const input = inputId.length === 0 ? null : this.store.getLoopInput(inputId);
    const binding = this.store.getLoopBinding(this.botUid, groupId);
    if (
      input === null
      || input.botUid !== this.botUid
      || input.groupId !== groupId
      || input.target === null
      || input.target.channelType !== target.channelType
      || input.target.channelId !== target.channelId
      || (input.state !== "intent" && input.state !== "delivered")
      || binding === null
      || input.workspaceId !== binding.workspaceId
      || input.bindingVersion !== binding.version
      || !input.tasks.some((candidate) => candidate.taskId === taskId)
    ) {
      throw new Error("automatic Loop input does not identify this current workspace task");
    }
    const resolved = await this.resolveTask(groupId, taskId, signal);
    if (resolved.binding.workspaceId !== input.workspaceId) {
      throw new Error("automatic Loop task workspace changed before thread work");
    }
    const normalizedStatus = resolved.task.status.trim().toLowerCase();
    if (CLOSED_LOOP_TASK_STATUSES[normalizedStatus] === true) {
      throw new Error(`closed Loop task status ${normalizedStatus} cannot start automatic thread work`);
    }
    return resolved;
  }

  activeCreation(key: LoopTaskKey): Promise<unknown> | null {
    return this.activeCreations.get(this.keyIdentity(key)) ?? null;
  }

  /** One process may have concurrent native retries; durable rows fence subsequent processes. */
  async runCreation<T>(
    key: LoopTaskKey,
    operationId: string,
    create: (reservation: LoopThreadCreation) => Promise<T>,
  ): Promise<T> {
    const identity = this.keyIdentity(key);
    const active = this.activeCreations.get(identity);
    if (active !== undefined) return active as Promise<T>;

    const existing = this.store.getLoopThreadCreation(key);
    if (existing !== null && existing.state !== "unknown") {
      throw new Error("该 Loop task 已有创建记录；必须核对已有资源，禁止再次创建。");
    }
    const attempts = this.store.listLoopThreadCreationAttempts(key);
    const attemptOperationId = existing !== null
      || attempts.some((attempt) => attempt.operationId === operationId)
      ? `${operationId}:${randomUUID()}`
      : operationId;
    const reservation = this.store.reserveLoopThreadCreation(key, attemptOperationId);
    if (reservation.state !== "reserved" || reservation.operationId !== attemptOperationId) {
      throw new Error("该 Loop task 已有创建预留；必须核对已有资源，禁止再次创建。");
    }

    const running = Promise.resolve().then(() => create(reservation));
    this.activeCreations.set(identity, running);
    try {
      return await running;
    } finally {
      if (this.activeCreations.get(identity) === running) this.activeCreations.delete(identity);
    }
  }

  private keyIdentity(key: LoopTaskKey): string {
    return JSON.stringify([key.botUid, key.groupId, key.workspaceId, key.taskId]);
  }

  private keyOf(creation: LoopThreadCreation): LoopTaskKey {
    return {
      botUid: creation.botUid,
      groupId: creation.groupId,
      workspaceId: creation.workspaceId,
      taskId: creation.taskId,
    };
  }
}
