import type { ChatTarget } from "../contracts.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";
import type { ChannelStore } from "../store/index.ts";
import type {
  LoopBinding,
  LoopReadPort,
  LoopTask,
  LoopTaskKey,
  LoopThreadCreation,
} from "./types.ts";

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
        "创建预留在远端身份确认前中断；需要人工核对，禁止自动重建。",
      );
    }
  }

  isTargetBlocked(target: ChatTarget): boolean {
    if (target.channelType !== 5) return false;
    const groupId = threadParentGroupId(target.channelId);
    if (groupId === null) return false;

    const hasLocalOwnership =
      this.store.getBinding(this.botUid, target) !== null ||
      this.store.getThreadWorkspace(this.botUid, target) !== null;
    const currentWorkspaceId = this.store.getLoopBinding(this.botUid, groupId)?.workspaceId;
    const explicitlyRouted = currentWorkspaceId !== undefined &&
      this.store.listLoopThreadTasks(this.botUid, groupId, target.channelId)
        .some((route) => route.workspaceId === currentWorkspaceId);
    return this.store.listLoopThreadCreations(this.botUid).some((creation) => {
      if (creation.groupId !== groupId || creation.state === "ready") return false;
      if (creation.threadChannelId === target.channelId) return !explicitlyRouted;
      return creation.threadChannelId === null
        && (creation.state === "reserved" || creation.state === "unknown")
        && !hasLocalOwnership
        && !explicitlyRouted;
    });
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
    if (this.store.getLoopThreadCreation(key) !== null) {
      throw new Error("该 Loop task 已有创建记录；必须核对已有资源，禁止再次创建。");
    }
    const reservation = this.store.reserveLoopThreadCreation(key, operationId);
    if (reservation.state !== "reserved" || reservation.operationId !== operationId) {
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
