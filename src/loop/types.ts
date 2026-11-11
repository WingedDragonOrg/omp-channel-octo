import type { ChatTarget } from "../contracts.ts";

export interface LoopWorkspace {
  workspaceId: string;
  name: string;
  slug: string;
}

/** Normalized public task fields; native Loop IDs never identify channel execution tasks. */
export interface LoopTask {
  taskId: string;
  title: string;
  description: string | null;
  status: string;
  priority: string;
  assigneeType: string | null;
  assigneeId: string | null;
  assigneeOctoUid: string | null;
  projectId: string | null;
  parentTaskId: string | null;
  startDate: string | null;
  dueDate: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface LoopReadPort {
  resolveWorkspace(selector: string, signal?: AbortSignal): Promise<LoopWorkspace>;
  listTasks(workspaceId: string, signal?: AbortSignal): Promise<LoopTask[]>;
  getTask(workspaceId: string, taskId: string, signal?: AbortSignal): Promise<LoopTask>;
}

export interface LoopBinding extends LoopWorkspace {
  botUid: string;
  groupId: string;
  version: number;
}

export interface LoopTaskKey {
  botUid: string;
  groupId: string;
  workspaceId: string;
  taskId: string;
}

export interface LoopTaskRoute extends LoopTaskKey {
  threadChannelId: string;
  version: number;
}

export type LoopInputState = "pending" | "intent" | "delivered" | "unknown" | "superseded";
export interface LoopInput {
  id: string;
  botUid: string;
  groupId: string;
  workspaceId: string;
  bindingVersion: number;
  kind: "baseline" | "created" | "updated";
  taskId: string | null;
  tasks: LoopTask[];
  previous: LoopTask | null;
  state: LoopInputState;
  target: ChatTarget | null;
  faultNotified: boolean;
}

/** One durable remote creation attempt; operationId is unique per attempt, not per native call. */
export interface LoopThreadCreation extends LoopTaskKey {
  operationId: string;
  bindingVersion: number;
  routeVersion: number;
  state: "reserved" | "allocated" | "ready" | "unknown" | "superseded";
  threadChannelId: string | null;
  error: string | null;
}

/** Ordered creation history plus the current attempt, when a reservation exists. */
export type LoopThreadCreationAttempt = LoopThreadCreation;
