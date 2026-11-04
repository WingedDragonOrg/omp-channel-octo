import type { ChatTarget } from "../contracts.ts";
import type { ChannelStore } from "../store/index.ts";
import { LoopThreadBindings } from "./thread-bindings.ts";
import { renderLoopInput } from "./render.ts";
import type { LoopBinding, LoopInput, LoopReadPort, LoopTaskKey, LoopTaskRoute } from "./types.ts";

const SCAN_INTERVAL_MS = 60_000;

export type LoopDeliveryOutcome = "accepted" | "not-accepted" | "unknown";

export interface LoopControllerOptions {
  readonly store: ChannelStore;
  readonly read: LoopReadPort;
  readonly botUid: string;
  readonly validateTarget: (binding: LoopBinding, target: ChatTarget, signal: AbortSignal) => Promise<void>;
  readonly deliver: (
    input: LoopInput,
    binding: LoopBinding,
    target: ChatTarget,
    text: string,
    guard: () => boolean,
    beforeWorkerSteer: () => boolean,
    beforeTaskAccept: () => boolean,
    onWorkerSteerAccepted: () => void,
  ) => Promise<LoopDeliveryOutcome>;
  readonly isStopping: () => boolean;
  readonly notifyFault: (groupId: string) => Promise<void>;
}

interface LoopDestination {
  target: ChatTarget;
  route: LoopTaskRoute | null;
  key: LoopTaskKey | null;
}

function sameBinding(left: LoopBinding | null, right: LoopBinding | null): boolean {
  return left === null
    ? right === null
    : right !== null
      && left.workspaceId === right.workspaceId
      && left.version === right.version;
}

function bindingKey(binding: LoopBinding): string {
  return `${binding.botUid}:${binding.groupId}:${binding.workspaceId}`;
}

export class LoopController {
  readonly threads: LoopThreadBindings;

  private readonly abortController = new AbortController();
  private readonly bindingOperations = new Set<Promise<unknown>>();
  private started = false;
  private stopping = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private cycle: Promise<void> | undefined;
  private delivery: Promise<void> | undefined;
  private stopPromise: Promise<void> | undefined;
  private scanTail: Promise<void> = Promise.resolve();
  private readonly bindingGenerations = new Map<string, number>();
  private readonly workspaceReadTails = new Map<string, Promise<void>>();
  private readonly scanFailures = new Set<string>();
  private readonly deliveryFailures = new Set<string>();

  constructor(private readonly options: LoopControllerOptions) {
    this.threads = new LoopThreadBindings(options.store, options.read, options.botUid);
  }

  /** Called after old worker process groups have been recovered, before normal delivery starts. */
  async recover(): Promise<void> {
    await this.threads.recover();
    for (const input of this.options.store.listLoopInputs(this.options.botUid)) {
      if (input.state !== "intent") continue;
      if (input.target !== null && this.hasConfirmedAcceptance(input, input.target)) {
        this.options.store.markLoopInputDelivered(input.id);
      } else {
        this.options.store.markLoopInputUnknown(input.id);
      }
    }
  }

  start(): void {
    if (this.started || this.stopping || this.options.isStopping()) return;
    this.started = true;
    void this.runCycle().catch(() => {});
  }

  async bind(
    groupId: string,
    selector: string,
    signal?: AbortSignal,
  ): Promise<{ binding: LoopBinding; changed: boolean }> {
    const generation = (this.bindingGenerations.get(groupId) ?? 0) + 1;
    this.bindingGenerations.set(groupId, generation);
    const operation = this.bindAndDeliver(groupId, selector, generation, signal);
    this.bindingOperations.add(operation);
    try {
      return await operation;
    } finally {
      this.bindingOperations.delete(operation);
    }
  }

  async stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopPromise = this.stopInternal();
    return this.stopPromise;
  }

  private async stopInternal(): Promise<void> {
    this.stopping = true;
    this.started = false;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.abortController.abort(new Error("Loop controller stopped"));
    await Promise.allSettled([
      ...(this.cycle === undefined ? [] : [this.cycle]),
      ...(this.delivery === undefined ? [] : [this.delivery]),
      ...this.bindingOperations,
    ]);
  }

  private signal(external?: AbortSignal): AbortSignal {
    return external === undefined
      ? this.abortController.signal
      : AbortSignal.any([this.abortController.signal, external]);
  }
  private serializeScans<T>(operation: () => Promise<T>): Promise<T> {
    const running = this.scanTail.then(operation, operation);
    this.scanTail = running.then(() => {}, () => {});
    return running;
  }
  private serializeWorkspaceRead<T>(
    groupId: string,
    workspaceId: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const key = JSON.stringify([this.options.botUid, groupId, workspaceId]);
    const previous = this.workspaceReadTails.get(key) ?? Promise.resolve();
    const running = previous.then(operation, operation);
    const tail = running.then(() => {}, () => {});
    this.workspaceReadTails.set(key, tail);
    void tail.then(() => {
      if (this.workspaceReadTails.get(key) === tail) this.workspaceReadTails.delete(key);
    });
    return running;
  }

  private async bindAndDeliver(
    groupId: string,
    selector: string,
    generation: number,
    externalSignal?: AbortSignal,
  ): Promise<{ binding: LoopBinding; changed: boolean }> {
    const signal = this.signal(externalSignal);
    if (signal.aborted || this.stopping || this.options.isStopping()) {
      throw signal.reason ?? new Error("Loop binding cancelled");
    }
    const observed = this.options.store.getLoopBinding(this.options.botUid, groupId);
    const workspace = await this.options.read.resolveWorkspace(selector, signal);
    if (this.bindingGenerations.get(groupId) !== generation) {
      throw new Error("更新的绑定请求已开始；本次结果未应用。");
    }
    if (signal.aborted || this.stopping || this.options.isStopping()) {
      throw signal.reason ?? new Error("Loop binding cancelled");
    }
    const result = await this.serializeWorkspaceRead(groupId, workspace.workspaceId, async () => {
      if (this.bindingGenerations.get(groupId) !== generation) {
        throw new Error("更新的绑定请求已开始；本次结果未应用。");
      }
      if (signal.aborted || this.stopping || this.options.isStopping()) {
        throw signal.reason ?? new Error("Loop binding cancelled");
      }
      const tasks = await this.options.read.listTasks(workspace.workspaceId, signal);
      if (this.bindingGenerations.get(groupId) !== generation) {
        throw new Error("更新的绑定请求已开始；本次结果未应用。");
      }
      if (signal.aborted || this.stopping || this.options.isStopping()) {
        throw signal.reason ?? new Error("Loop binding cancelled");
      }
      const current = this.options.store.getLoopBinding(this.options.botUid, groupId);
      if (!sameBinding(observed, current)) {
        throw new Error("绑定读取期间当前 workspace 已变化；本次结果未应用，请重试。");
      }
      const bound = this.options.store.bindLoopWorkspace(
        this.options.botUid,
        groupId,
        workspace,
        tasks,
      );
      if (!bound.changed) this.options.store.commitLoopScan(bound.binding, tasks);
      return bound;
    });
    void this.drainPendingInputs().catch(() => this.logDeliveryFailure(result.binding));
    return result;
  }

  private runCycle(): Promise<void> {
    if (this.cycle !== undefined) return this.cycle;
    if (this.stopping || !this.started || this.options.isStopping()) return Promise.resolve();
    const operation = this.serializeScans(() => this.scanAndDeliver());
    this.cycle = operation;
    void operation.then(
      () => {
        if (this.cycle !== operation) return;
        this.cycle = undefined;
        this.scheduleNextScan();
      },
      () => {
        if (this.cycle !== operation) return;
        this.cycle = undefined;
        this.scheduleNextScan();
      },
    );
    return operation;
  }

  private scheduleNextScan(): void {
    if (this.stopping || !this.started || this.options.isStopping() || this.timer !== undefined) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.runCycle().catch(() => {});
    }, SCAN_INTERVAL_MS);
    this.timer.unref();
  }

  private async scanAndDeliver(): Promise<void> {
    const signal = this.abortController.signal;
    for (const binding of this.options.store.listLoopBindings(this.options.botUid)) {
      if (signal.aborted || this.stopping || this.options.isStopping()) return;
      const key = bindingKey(binding);
      try {
        await this.serializeWorkspaceRead(binding.groupId, binding.workspaceId, async () => {
          if (signal.aborted || this.stopping || this.options.isStopping()) return;
          const tasks = await this.options.read.listTasks(binding.workspaceId, signal);
          if (signal.aborted || this.stopping || this.options.isStopping()) return;
          if (this.options.store.commitLoopScan(binding, tasks)) this.scanFailures.delete(key);
        });
      } catch {
        if (signal.aborted || this.stopping || this.options.isStopping()) return;
        if (!this.scanFailures.has(key)) {
          this.scanFailures.add(key);
          console.error(`[loop] task scan failed group=${binding.groupId} workspace=${binding.workspaceId}`);
        }
      }
    }
    if (!signal.aborted && !this.stopping && !this.options.isStopping()) {
      await this.drainPendingInputs();
    }
  }

  private drainPendingInputs(): Promise<void> {
    if (this.delivery !== undefined) return this.delivery;
    if (this.stopping || this.options.isStopping()) return Promise.resolve();
    const operation = this.deliverPendingInputs();
    this.delivery = operation;
    void operation.then(
      () => {
        if (this.delivery === operation) this.delivery = undefined;
      },
      () => {
        if (this.delivery === operation) this.delivery = undefined;
      },
    );
    return operation;
  }

  private async deliverPendingInputs(): Promise<void> {
    for (const pending of this.options.store.listLoopInputs(this.options.botUid)) {
      if (this.stopping || this.options.isStopping() || this.abortController.signal.aborted) return;
      const input = this.options.store.prepareLoopInput(pending.id);
      if (input === null) continue;
      const binding = this.options.store.getLoopBinding(input.botUid, input.groupId);
      if (
        binding === null
        || binding.workspaceId !== input.workspaceId
        || binding.version !== input.bindingVersion
      ) continue;
      let destination = this.destination(input);
      const id = this.messageId(input);
      const acceptedTask = this.options.store.getTaskByMessageId(
        input.botUid,
        destination.target,
        id,
      );
      const commandState = this.options.store.getCommandClaimState(
        input.botUid,
        destination.target,
        id,
      );
      if (acceptedTask !== null || commandState === "completed") {
        if (this.options.store.claimLoopInputIntent(input.id, destination.target)) {
          this.options.store.markLoopInputDelivered(input.id);
        }
        continue;
      }
      if (commandState === "pending") {
        if (this.options.store.claimLoopInputIntent(input.id, destination.target)) {
          this.options.store.markLoopInputUnknown(input.id);
        }
        continue;
      }
      const signal = this.abortController.signal;
      try {
        await this.options.validateTarget(binding, destination.target, signal);
      } catch {
        if (this.stopping || this.options.isStopping() || signal.aborted) return;
        if (this.guard(input, binding, destination)) {
          await this.notifyInputFault(input);
          continue;
        }
      }
      if (!this.guard(input, binding, destination)) {
        const currentBinding = this.options.store.getLoopBinding(input.botUid, input.groupId);
        if (!sameBinding(binding, currentBinding)) continue;
        const updated = this.destination(input);
        if (
          updated.target.channelType === destination.target.channelType
          && updated.target.channelId === destination.target.channelId
          && updated.route?.version === destination.route?.version
        ) continue;
        destination = updated;
        try {
          await this.options.validateTarget(binding, destination.target, signal);
        } catch {
          if (this.stopping || this.options.isStopping() || signal.aborted) return;
          if (this.guard(input, binding, destination)) await this.notifyInputFault(input);
          continue;
        }
      }
      if (!this.guard(input, binding, destination)) continue;
      const routes = new Map<string, string>();
      for (const task of input.tasks) {
        const route = this.options.store.getLoopTaskRoute({
          botUid: input.botUid,
          groupId: input.groupId,
          workspaceId: input.workspaceId,
          taskId: task.taskId,
        });
        if (route !== null) routes.set(task.taskId, route.threadChannelId);
      }
      const text = renderLoopInput(input, destination.target, routes);
      const beforeWorkerSteer = (): boolean => this.claimIntent(input, destination.target, binding, destination);
      const beforeTaskAccept = (): boolean => this.claimIntent(input, destination.target, binding, destination);
      let steerAccepted = false;
      let outcome: LoopDeliveryOutcome;
      try {
        outcome = await this.options.deliver(
          input,
          binding,
          destination.target,
          text,
          () => this.guard(input, binding, destination),
          beforeWorkerSteer,
          beforeTaskAccept,
          () => { steerAccepted = true; },
        );
      } catch {
        const current = this.options.store.getLoopInput(input.id);
        if (
          current?.state === "pending"
          && (steerAccepted || this.hasConfirmedAcceptance(current, destination.target))
          && this.options.store.claimLoopInputIntent(input.id, destination.target)
        ) {
          this.options.store.markLoopInputDelivered(input.id);
          this.deliveryFailures.delete(bindingKey(binding));
          continue;
        }
        if (current?.state === "intent") {
          if (steerAccepted || (
            current.target !== null
            && this.hasConfirmedAcceptance(current, current.target)
          )) {
            this.options.store.markLoopInputDelivered(input.id);
          } else {
            this.options.store.markLoopInputUnknown(input.id);
            this.logDeliveryFailure(binding);
          }
        } else {
          this.logDeliveryFailure(binding);
        }
        continue;
      }
      const current = this.options.store.getLoopInput(input.id);
      if (current?.state === "pending" && (steerAccepted || outcome === "accepted")) {
        if (this.options.store.claimLoopInputIntent(input.id, destination.target)) {
          this.options.store.markLoopInputDelivered(input.id);
          this.deliveryFailures.delete(bindingKey(binding));
        }
        continue;
      }
      if (current?.state === "pending" && outcome === "unknown") {
        if (this.options.store.claimLoopInputIntent(input.id, destination.target)) {
          this.options.store.markLoopInputUnknown(input.id);
          this.logDeliveryFailure(binding);
        }
        continue;
      }
      if (current?.state !== "intent") continue;
      if (steerAccepted || outcome === "accepted") {
        this.deliveryFailures.delete(bindingKey(binding));
        this.options.store.markLoopInputDelivered(input.id);
      } else if (outcome === "not-accepted") {
        this.options.store.markLoopInputPending(input.id);
      } else {
        this.logDeliveryFailure(binding);
        this.options.store.markLoopInputUnknown(input.id);
      }
    }
  }

  private destination(input: LoopInput): LoopDestination {
    if (input.taskId === null) {
      return { target: { channelType: 2, channelId: input.groupId }, route: null, key: null };
    }
    const key: LoopTaskKey = {
      botUid: input.botUid,
      groupId: input.groupId,
      workspaceId: input.workspaceId,
      taskId: input.taskId,
    };
    const route = this.options.store.getLoopTaskRoute(key);
    return {
      target: route === null
        ? { channelType: 2, channelId: input.groupId }
        : { channelType: 5, channelId: route.threadChannelId },
      route,
      key,
    };
  }

  private guard(input: LoopInput, binding: LoopBinding, destination: LoopDestination): boolean {
    if (this.stopping || this.options.isStopping() || this.abortController.signal.aborted) return false;
    const currentBinding = this.options.store.getLoopBinding(input.botUid, input.groupId);
    if (!sameBinding(binding, currentBinding)) return false;
    if (!this.options.store.isLoopInputObserved(input)) return false;
    if (destination.key === null) return true;
    const currentRoute = this.options.store.getLoopTaskRoute(destination.key);
    return currentRoute?.version === destination.route?.version
      && currentRoute?.threadChannelId === destination.route?.threadChannelId;
  }

  private claimIntent(
    input: LoopInput,
    target: ChatTarget,
    binding: LoopBinding,
    destination: LoopDestination,
  ): boolean {
    if (!this.guard(input, binding, destination)) return false;
    return this.options.store.claimLoopInputIntent(input.id, target);
  }

  private async notifyInputFault(input: LoopInput): Promise<void> {
    const relatedFault = this.options.store.listLoopInputs(this.options.botUid).some((candidate) =>
      candidate.id !== input.id
      && candidate.faultNotified
      && candidate.botUid === input.botUid
      && candidate.groupId === input.groupId
      && candidate.workspaceId === input.workspaceId
      && candidate.taskId === input.taskId);
    const changed = this.options.store.markLoopInputFault(input.id, true);
    if (!changed || relatedFault) return;
    try {
      await this.options.notifyFault(input.groupId);
    } catch {
      // The durable input remains pending; another input in the same fault episode will not spam.
    }
  }

  private logDeliveryFailure(binding: LoopBinding): void {
    const key = bindingKey(binding);
    if (this.deliveryFailures.has(key)) return;
    this.deliveryFailures.add(key);
    console.error(`[loop] input delivery failed group=${binding.groupId} workspace=${binding.workspaceId}`);
  }

  private messageId(input: LoopInput): string {
    return `loop:${input.id}`;
  }

  private hasConfirmedAcceptance(input: LoopInput, target: ChatTarget): boolean {
    const messageId = this.messageId(input);
    return this.options.store.getTaskByMessageId(input.botUid, target, messageId) !== null
      || this.options.store.getCommandClaimState(input.botUid, target, messageId) === "completed";
  }
}
