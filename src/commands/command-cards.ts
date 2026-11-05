/**
 * 配置卡片菜单：`/switch` 与 `/mode` 的交互卡片会话。卡片本身由 `command-card.ts` 渲染，这里
 * 独占它的生命周期——发布、绑定远端 messageId、按 owner 与精确绑定校验点击、应用选择、失效与
 * 有界回收。
 *
 * 未点击的菜单会话仍是进程内临时状态：跨重启旧卡只得到重开指引。一次点击通过完整校验后，本模块
 * 产出可恢复快照并要求入口先持久交接；模型/worker/卡片 HTTP 由 `resumeAcceptedAction` 在事件
 * ACK 之外执行。本模块不复制命令语义，应用仍经 `applyChoice` 走文本命令的同一路径。
 *
 * 边界：会话总数上限 200、存活 15 分钟；卡片投递与改写各有 10 秒传输预算并与停机信号绑定；
 * 只有持久化 owner 本人的点击有效，非 owner 静默 ack 且不消耗 owner 的卡片；只有本卡发放过的
 * 取值会被应用；同聊天的同一类配置命令按到达顺序串行落地，最后一次选择一定最后写库。
 */

import { randomUUID } from "node:crypto";
import type { BotEventOutcome } from "../bot-events.ts";
import {
  buildCommandCard,
  buildCommandCardResult,
  COMMAND_INPUT_ID,
  type CommandCardKind,
  type CommandCardView,
} from "../cards/command-card.ts";
import type { BotCardAction, ChannelTransport, ChatTarget, SendReceipt } from "../contracts.ts";
import { safeErrorMessage } from "../errors.ts";
import type { ChannelStore } from "../store/index.ts";
import type { IngressValidatedConfigAction } from "../inbound/ingress-types.ts";
import { targetKey } from "../target.ts";

/** 配置卡片 action id 的前缀：Bot 事件消费者按它把配置点击交给本模块，其余交给问答卡。 */
export const CONFIG_CARD_ACTION_PREFIX = "cfg:";
/** 进程内保留的菜单会话上限，超出后最旧的一张先失效。 */
export const CONFIG_MENU_MAX_SESSIONS = 200;
/** 会话存活时间：超过即失效，点击只得到重开指引。 */
export const CONFIG_MENU_TTL_MS = 15 * 60 * 1_000;
/** 单次卡片投递/改写的传输预算，与问答卡同一量级。 */
const CARD_OP_TIMEOUT_MS = 10_000;
/** 排队期间聊天变得不可配置（停机、换 owner、移出允许清单）时的一行有界回执。 */
const UNAVAILABLE_APPLY_TEXT = "本次配置未应用：该聊天当前不可用（服务正在停止或已不在允许范围）。";

/** 本模块发放的 action id：`cfg:<类型>:<会话 uuid>`。 */
const CONFIG_ACTION_ID =
  /^cfg:(switch|mode):([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/u;

/** 每个配置类型对应的命令名：只用于失效与非法输入的有界指引。 */
const KIND_COMMAND: Record<CommandCardKind, string> = { switch: "/switch", mode: "/mode" };

/** 一张已发布（或正在发布）的菜单卡片。 */
interface ConfigMenuSession {
  readonly kind: CommandCardKind;
  readonly actionId: string;
  readonly target: ChatTarget;
  /** 发布时的持久化 owner；点击时以当次读取的 owner 为准。 */
  readonly ownerUid: string;
  /** 本卡发放过的取值；不在其中的输入一律不应用。 */
  readonly values: ReadonlySet<string>;
  readonly expiresAt: number;
  /** 远端 messageId；发送在途时为 `null`，此时的点击保留重试。 */
  messageId: string | null;
  /** 本卡已用的编辑序号；首次改写从 2 起。 */
  cardSeq: number;
}

/**
 * 已校验配置卡动作的可恢复快照。它只含本卡已经发放并核验过的值；eventId 是来源去重键，
 * messageId/actionId 只用于归因与动作真实性，不能替代 eventId。
 */
export type ConfigCardActionSnapshot = IngressValidatedConfigAction & {
  readonly eventId: number;
};

export type ConfigCardActionAcceptance = "accepted" | "duplicate" | "retry";

/** One FIFO slot created at durable ingress acceptance and released after application or suspension. */
export interface ConfigApplyReservation {
  run(work: () => string | null | Promise<string | null>): Promise<string | null>;
  release(): void;
}

/** Per-ingress durable checkpoint operations supplied by the coordinator. */
export interface ConfigActionCheckpoint {
  readonly dispatchIntent: boolean;
  readonly appliedResult: string | null;
  markDispatchIntent(): boolean;
  markAppliedResult(result: string): boolean;
  markUnknown(reason: string): void;
}

/** Signals that an accepted action was suspended before durable dispatch intent. */
export class ConfigActionSuspendedError extends Error {
  constructor() {
    super("configuration action suspended before dispatch");
    this.name = "ConfigActionSuspendedError";
  }
}

export interface ConfigMenuDeps {
  readonly store: ChannelStore;
  readonly transport: ChannelTransport;
  /** Bot 身份：owner 按它读取；身份未学习到时不发卡，按原文本回执。 */
  botUid(): string | undefined;
  /** 停机中不再开新卡、不再应用点击。 */
  stopping(): boolean;
  /** 点击时刻重新校验该聊天是否仍在允许清单内（与消息门共用同一授权）。 */
  chatAllowed(target: ChatTarget): boolean;
  /** Durable source dedupe: checked before looking up ephemeral menu state or emitting reopen text. */
  getAcceptedEvent(eventId: number): boolean;
  /**
   * 应用一次卡片取值：与文本命令同一条路径，返回逐字回执文本。抛错由本模块收敛为一行有界提示。
   */
  applyChoice(kind: CommandCardKind, target: ChatTarget, value: string): Promise<string>;
  /**
   * 在任何长动作前持久交接已校验快照；duplicate 表示该 event 已有消费者。accepted 的持久记录
   * 必须保留快照与恢复责任，直到 `resumeAcceptedAction` 返回的 Promise 真正完成。
   */
  acceptAction(
    snapshot: ConfigCardActionSnapshot,
    action: BotCardAction,
    reservation: ConfigApplyReservation,
  ): ConfigCardActionAcceptance;
  /** 结果与指引的文本出口，与既有命令共用同一个 outbox。 */
  enqueueText(target: ChatTarget, text: string, taskId: string | null): void;
  /** 停机信号：卡片投递与改写与它绑定，进程退出不会挂在网络上。 */
  shutdownSignal?: AbortSignal | undefined;
  /** 时钟；TTL 判定只用它。默认 `Date.now`。 */
  now?(): number;
}

/**
 * 一次配置请求的代次票据：`begin` 产出，查询与发布前用 `current()` 校验，请求结束 `release()`。
 * 票据把「这次请求开始时就是最新」这句话带过整个探测/发送过程，旧的探测结果因此无法在更新的
 * 命令之后发布。
 */
export interface ConfigMenuTicket {
  /** 这次请求是否仍未被更新的同类命令取代。 */
  current(): boolean;
  /** 请求结束（发布完成、放弃或失败）：归还票据；同类请求都结束后代次状态即回收。 */
  release(): void;
}

/** 命令侧需要的窄面：开始一次请求、开卡、让旧卡失效、按到达顺序串行应用。 */
export interface ConfigMenuPort {
  /**
   * 开始一次该聊天的该类配置请求：立刻让旧菜单失效并拿到代次票据。探测、投影或查询必须在它
   * 之后进行，`open` 用它判断自己是否已被取代。
   */
  begin(target: ChatTarget, kind: CommandCardKind): ConfigMenuTicket;
  /**
   * 打开一张菜单卡；票据已被取代时什么都不做（不发送、不回退文本），卡片不可用时按 `view.plain`
   * 原文回退，文本语义不变。调用方负责 `release()`。
   */
  open(target: ChatTarget, view: CommandCardView, ticket: ConfigMenuTicket): Promise<void>;
  /** 让该聊天该类型的当前菜单立即失效（非空文本命令与卡片点击都走到这里）。 */
  invalidate(target: ChatTarget, kind: CommandCardKind): void;
  /** Reserve the target/kind FIFO slot at ingress acceptance; repeated source IDs return the same slot. */
  reserveApply(kind: CommandCardKind, target: ChatTarget, sourceId: string): ConfigApplyReservation;
  /**
   * 同一 (聊天, 类型) 的配置应用串行：后到的命令排队在前一个落地之后。轮到本项时重新核对停机、
   * owner 与聊天授权，任一不成立就不再执行 `work`，只回一行有界回执；前一个失败不阻断后一个。
   */
  serialize(kind: CommandCardKind, target: ChatTarget, work: () => string | Promise<string>): Promise<string>;
}

export interface ConfigMenuDesk extends ConfigMenuPort {
  /** Bot 事件队列的一次 `cfg:` 点击；只等待持久短交接，不等待应用或卡片 HTTP。 */
  handleAction(action: BotCardAction, eventId: number): Promise<BotEventOutcome>;
  /** Replays an already-applied result for display, or executes only after durable dispatch intent. */
  resumeAcceptedAction(
    snapshot: ConfigCardActionSnapshot,
    reservation: ConfigApplyReservation,
    checkpoint: ConfigActionCheckpoint,
  ): Promise<void>;
}

/** 本模块发放的 action id 的类型；其他形状（含伪造成 `cfg:` 前缀的）返回 `null`。 */
export function configActionKind(actionId: string): CommandCardKind | null {
  const match = CONFIG_ACTION_ID.exec(actionId);
  return match === null ? null : (match[1] as CommandCardKind);
}

/** 一个 (聊天, 类型) 的代次状态：`held` 是在途票据数，归零即回收。 */
interface MenuGeneration {
  current: number;
  held: number;
}

export function createConfigMenuDesk(deps: ConfigMenuDeps): ConfigMenuDesk {
  const now = deps.now ?? Date.now;
  /** actionId → 会话；插入顺序即发布顺序，回收时最旧先出。 */
  const sessions = new Map<string, ConfigMenuSession>();
  /** (聊天, 类型) → 当前会话：新菜单取代旧菜单，文本命令让旧卡立即失效。 */
  const current = new Map<string, string>();
  /** (聊天, 类型) → 代次状态：只活在仍有在途请求的时候，票据归还后即回收。 */
  const generations = new Map<string, MenuGeneration>();
  /** (聊天, 类型) → 应用串行链：后到的配置命令在前一个落地之后执行。 */
  /** (聊天, 类型) → reserved application barrier, registered at source acceptance. */
  const applies = new Map<string, Promise<void>>();
  const reservations = new Map<string, ConfigApplyReservation>();
  const pendingTextReservations = new Map<string, ConfigApplyReservation[]>();
  /** Validated clicks whose durable ingress insert must be retried, even after the menu expires. */
  const pendingValidatedActions = new Map<number, ConfigCardActionSnapshot>();

  function menuKey(target: ChatTarget, kind: CommandCardKind): string {
    return `${targetKey(target)}:${kind}`;
  }

  function reserveApply(
    kind: CommandCardKind,
    target: ChatTarget,
    sourceId: string,
  ): ConfigApplyReservation {
    const key = menuKey(target, kind);
    const sourceKey = `${key}:${sourceId}`;
    const existing = reservations.get(sourceKey);
    if (existing !== undefined) return existing;
    const calledBot = deps.botUid();
    const ownerAtAcceptance = calledBot === undefined
      ? ""
      : (deps.store.getOwnerId(calledBot) ?? "").trim();
    const previous = applies.get(key) ?? Promise.resolve();
    const { promise: slot, resolve: releaseSlot } = Promise.withResolvers<void>();
    const tail = previous.then(() => slot);
    applies.set(key, tail);
    let released = false;
    let runPromise: Promise<string | null> | null = null;
    const removePending = (): void => {
      const queue = pendingTextReservations.get(key);
      if (queue === undefined) return;
      const index = queue.indexOf(reservation);
      if (index >= 0) queue.splice(index, 1);
      if (queue.length === 0) pendingTextReservations.delete(key);
    };
    const release = (): void => {
      if (released) return;
      released = true;
      removePending();
      releaseSlot();
      if (reservations.get(sourceKey) === reservation) reservations.delete(sourceKey);
      if (applies.get(key) === tail) {
        void tail.then(() => {
          if (applies.get(key) === tail) applies.delete(key);
        });
      }
    };
    const reservation: ConfigApplyReservation = {
      run(work): Promise<string | null> {
        if (runPromise !== null) return runPromise;
        if (released) return Promise.resolve(null);
        removePending();
        runPromise = previous.then(async () => {
          if (released) return null;
          const botUid = deps.botUid();
          const liveOwner = botUid === undefined ? "" : (deps.store.getOwnerId(botUid) ?? "").trim();
          if (
            botUid === undefined ||
            ownerAtAcceptance.length === 0 ||
            liveOwner !== ownerAtAcceptance ||
            !deps.chatAllowed(target)
          ) return UNAVAILABLE_APPLY_TEXT;
          return await work();
        }).finally(release);
        return runPromise;
      },
      release,
    };
    reservations.set(sourceKey, reservation);
    if (!sourceId.startsWith("bot-event:")) {
      const queue = pendingTextReservations.get(key) ?? [];
      queue.push(reservation);
      pendingTextReservations.set(key, queue);
    }
    return reservation;
  }

  function drop(actionId: string): void {
    const session = sessions.get(actionId);
    if (session === undefined) return;
    sessions.delete(actionId);
    const key = menuKey(session.target, session.kind);
    if (current.get(key) === actionId) current.delete(key);
  }

  /** 开始一次该聊天该类的请求：旧菜单立刻失效、代次前进，返回可校验可归还的票据。 */
  function begin(target: ChatTarget, kind: CommandCardKind): ConfigMenuTicket {
    const key = menuKey(target, kind);
    const live = current.get(key);
    if (live !== undefined) drop(live);
    const state = generations.get(key) ?? { current: 0, held: 0 };
    generations.set(key, state);
    state.current += 1;
    state.held += 1;
    const generation = state.current;
    let released = false;
    return {
      current: () => generations.get(key) === state && state.current === generation,
      release: () => {
        if (released) return;
        released = true;
        state.held -= 1;
        // 票据归还且没人再持有：这个聊天的代次状态随之回收，表不会随访问过的聊天无限增长。
        if (state.held === 0 && generations.get(key) === state) generations.delete(key);
      },
    };
  }

  function register(session: ConfigMenuSession): void {
    while (sessions.size >= CONFIG_MENU_MAX_SESSIONS) {
      const oldest = sessions.keys().next().value;
      if (oldest === undefined) break;
      drop(oldest);
    }
    sessions.set(session.actionId, session);
    current.set(menuKey(session.target, session.kind), session.actionId);
  }

  /** 按 actionId 取会话；过期的当场回收，与未知会话同路。 */
  function lookup(actionId: string): ConfigMenuSession | undefined {
    const session = sessions.get(actionId);
    if (session === undefined) return undefined;
    if (session.expiresAt <= now()) {
      drop(actionId);
      return undefined;
    }
    return session;
  }

  /** 卡片投递/改写的传输预算：停机信号 + 固定上限，二者取先到者。 */
  function budget(): AbortSignal {
    const timeout = AbortSignal.timeout(CARD_OP_TIMEOUT_MS);
    return deps.shutdownSignal === undefined
      ? timeout
      : AbortSignal.any([deps.shutdownSignal, timeout]);
  }

  /** 失效卡片的有界重开指引：还在同一聊天的是更新过的卡片，否则让用户重发命令。 */
  function reopenText(kind: CommandCardKind, target: ChatTarget): string {
    const command = KIND_COMMAND[kind];
    return current.get(menuKey(target, kind)) === undefined
      ? `这张配置卡片已失效；重新发送 ${command} 可打开新的配置卡片。`
      : `这张配置卡片已失效；请使用最近的 ${command} 卡片。`;
  }

  function invalidValueText(kind: CommandCardKind): string {
    return `未收到本卡片提供的选项，本次没有修改；重新发送 ${KIND_COMMAND[kind]} 可再次打开配置卡片。`;
  }


  /** 终态改写：卡片只承载既有 handler 的逐字回执，没有输入也没有动作。 */
  async function finishCard(
    session: ConfigMenuSession | ConfigCardActionSnapshot,
    receipt: string,
  ): Promise<void> {
    const messageId = session.messageId;
    if (messageId === null) {
      deps.enqueueText(session.target, receipt, null);
      return;
    }
    try {
      await deps.transport.editCard(
        session.target,
        messageId,
        buildCommandCardResult(session.kind, receipt),
        session.cardSeq + 1,
        budget(),
      );
    } catch (error) {
      // 改写失败不影响结果本身：同一段回执走文本出口，卡片保持原样。
      console.error(
        `[config-card] edit failed target=${targetKey(session.target)}：${safeErrorMessage(error, "edit failed")}`,
      );
      deps.enqueueText(session.target, receipt, null);
    }
  }

  async function open(target: ChatTarget, view: CommandCardView, ticket: ConfigMenuTicket): Promise<void> {
    // 查询期间已被更新的同类命令取代：不发送、不回退文本（那个命令有自己的回执）。
    if (!ticket.current()) return;
    const botUid = deps.stopping() ? undefined : deps.botUid();
    const ownerUid = botUid === undefined ? "" : (deps.store.getOwnerId(botUid) ?? "").trim();
    if (botUid === undefined || ownerUid.length === 0 || view.choices.length === 0) {
      // 身份未就绪、没有 owner、没有可选值或正在停机时，卡片没有意义：按原文本回执。
      deps.enqueueText(target, view.plain, null);
      return;
    }
    const actionId = `${CONFIG_CARD_ACTION_PREFIX}${view.kind}:${randomUUID()}`;
    const session: ConfigMenuSession = {
      kind: view.kind,
      actionId,
      target: { channelType: target.channelType, channelId: target.channelId },
      ownerUid,
      values: new Set(view.choices.map(({ value }) => value)),
      expiresAt: now() + CONFIG_MENU_TTL_MS,
      messageId: null,
      cardSeq: 1,
    };
    register(session);
    let receipt: SendReceipt | null = null;
    try {
      receipt = await deps.transport.sendCard(
        target,
        buildCommandCard(view, actionId),
        randomUUID(),
        budget(),
      );
    } catch (error) {
      console.error(
        `[config-card] send failed target=${targetKey(target)}：${safeErrorMessage(error, "send failed")}`,
      );
    }
    const messageId = receipt?.messageId;
    if (!ticket.current() || deps.stopping()) {
      // 发送期间被更新的命令取代，或进程正在停止：已发出的卡片不绑定、不复活，也不重复投递文本。
      drop(actionId);
      return;
    }
    if (messageId === undefined) {
      // 该部署不能承载卡片（拒绝、失败或没有回执）：原文本回退，命令语义不变。
      drop(actionId);
      deps.enqueueText(target, view.plain, null);
      return;
    }
    session.messageId = messageId;
  }

  function invalidate(target: ChatTarget, kind: CommandCardKind): void {
    // 代次前进即让在途的同类请求作废；票据当场归还，代次状态不留在表里。
    begin(target, kind).release();
  }

  function serialize(kind: CommandCardKind, target: ChatTarget, work: () => string | Promise<string>): Promise<string> {
    const key = menuKey(target, kind);
    const reservation = pendingTextReservations.get(key)?.[0]
      ?? reserveApply(kind, target, `local:${randomUUID()}`);
    return reservation.run(() => deps.stopping() ? UNAVAILABLE_APPLY_TEXT : work())
      .then((receipt) => receipt ?? UNAVAILABLE_APPLY_TEXT);
  }

  async function resumeAcceptedAction(
    snapshot: ConfigCardActionSnapshot,
    reservation: ConfigApplyReservation,
    checkpoint: ConfigActionCheckpoint,
  ): Promise<void> {
    if (checkpoint.appliedResult !== null) {
      reservation.release();
      await finishCard(snapshot, checkpoint.appliedResult);
      return;
    }
    if (checkpoint.dispatchIntent) {
      reservation.release();
      checkpoint.markUnknown("配置操作已有 dispatch intent 但没有持久结果；为避免重复应用，不再执行。");
      return;
    }
    let receipt: string | null;
    try {
      receipt = await reservation.run(async () => {
        if (deps.stopping()) throw new ConfigActionSuspendedError();
        if (!checkpoint.markDispatchIntent()) {
          checkpoint.markUnknown("配置操作无法持久化 dispatch intent；未执行应用。");
          return null;
        }
        const botUid = deps.stopping() ? undefined : deps.botUid();
        const liveOwner = botUid === undefined ? "" : (deps.store.getOwnerId(botUid) ?? "").trim();
        if (liveOwner !== snapshot.ownerUid || !deps.chatAllowed(snapshot.target)) {
          const unavailable = UNAVAILABLE_APPLY_TEXT;
          if (!checkpoint.markAppliedResult(unavailable)) {
            checkpoint.markUnknown("配置拒绝结果未能持久化；不重试操作。");
            return null;
          }
          return unavailable;
        }
        let applied: string;
        try {
          applied = await deps.applyChoice(snapshot.kind, snapshot.target, snapshot.value);
        } catch (error) {
          console.error(
            `[config-card] apply failed target=${targetKey(snapshot.target)}：${safeErrorMessage(error, "apply failed")}`,
          );
          checkpoint.markUnknown("配置应用结果不确定；为避免重复副作用，不再执行。");
          return null;
        }
        if (!checkpoint.markAppliedResult(applied)) {
          checkpoint.markUnknown("配置已执行但结果未能持久化；为避免重复应用，标记为未知。");
          return null;
        }
        return applied;
      });
    } catch (error) {
      if (error instanceof ConfigActionSuspendedError) throw error;
      checkpoint.markUnknown(`配置执行阶段异常：${safeErrorMessage(error, "unknown failure")}`);
      return;
    }
    if (receipt !== null) await finishCard(snapshot, receipt);
  }


  async function handleAction(action: BotCardAction, eventId: number): Promise<BotEventOutcome> {
    // Durable source dedupe precedes every ephemeral menu lookup or retry snapshot.
    try {
      if (deps.getAcceptedEvent(eventId)) {
        pendingValidatedActions.delete(eventId);
        return "handled";
      }
    } catch {
      return "retry";
    }
    const botUid = deps.botUid();
    if (botUid === undefined || deps.stopping()) return "retry";
    const kind = configActionKind(action.actionId);
    const retained = pendingValidatedActions.get(eventId);
    if (kind === null) return retained === undefined ? "ignored" : "retry";
    const target: ChatTarget = { channelType: action.channelType, channelId: action.channelId };
    const ownerUid = (deps.store.getOwnerId(botUid) ?? "").trim();
    let snapshot: ConfigCardActionSnapshot;
    if (retained !== undefined) {
      if (ownerUid.length === 0 || ownerUid !== retained.ownerUid || !deps.chatAllowed(target)) {
        pendingValidatedActions.delete(eventId);
        return "ignored";
      }
      if (action.operatorUid.trim() !== ownerUid) return "retry";
      if (
        retained.kind !== kind
        || retained.actionId !== action.actionId
        || retained.messageId !== action.messageId
        || retained.target.channelType !== action.channelType
        || retained.target.channelId !== action.channelId
        || retained.value !== (action.inputs[COMMAND_INPUT_ID] ?? "")
      ) return "retry";
      snapshot = retained;
    } else {
      // 配置面只归 owner：其他人的点击静默 ack 且不消耗 owner 的卡片。
      if (ownerUid.length === 0 || action.operatorUid.trim() !== ownerUid) return "ignored";
      if (!deps.chatAllowed(target)) return "ignored";
      const session = lookup(action.actionId);
      if (session === undefined) {
        // 未知（重启、跨进程残留、已消费）或过期：只给 owner 一段有界的重开指引。
        deps.enqueueText(target, reopenText(kind, target), null);
        return "handled";
      }
      // 先核对持久绑定，再谈「发送在途」：串聊天或换 owner 的事件不能借在途状态占住事件队列。
      if (
        session.ownerUid !== ownerUid ||
        session.target.channelType !== action.channelType ||
        session.target.channelId !== action.channelId
      ) {
        // 绑定不符（换 owner、串聊天或伪造）：静默丢弃，不消耗 owner 的卡片。
        return "ignored";
      }
      // 远端回执尚未绑定：保留事件，绑定后再按真实绑定判定（服务端已发布但回执在途的点击）。
      if (session.messageId === null) return "retry";
      if (session.messageId !== action.messageId) return "ignored";
      const value = action.inputs[COMMAND_INPUT_ID] ?? "";
      if (!session.values.has(value)) {
        drop(session.actionId);
        invalidate(target, kind);
        deps.enqueueText(target, invalidValueText(kind), null);
        return "handled";
      }
      snapshot = {
        eventId,
        kind,
        actionId: session.actionId,
        messageId: session.messageId,
        target: { ...session.target },
        ownerUid,
        value,
        cardSeq: session.cardSeq,
      };
      pendingValidatedActions.set(eventId, snapshot);
    }
    let reservation: ConfigApplyReservation;
    try {
      reservation = reserveApply(snapshot.kind, snapshot.target, `bot-event:${eventId}`);
    } catch {
      return "retry";
    }
    let acceptance: ConfigCardActionAcceptance;
    try {
      acceptance = deps.acceptAction(snapshot, action, reservation);
    } catch {
      reservation.release();
      return "retry";
    }
    if (acceptance === "retry") {
      reservation.release();
      return "retry";
    }
    pendingValidatedActions.delete(eventId);
    // 持久交接成功后才消费内存菜单；重投命中 duplicate 不会再执行动作。
    drop(snapshot.actionId);
    invalidate(snapshot.target, snapshot.kind);
    // accepted 快照由 ingress 持有并在回调返回后调度；本事件调用栈不启动或等待长动作。
    return "handled";
  }

  return { begin, open, invalidate, reserveApply, serialize, handleAction, resumeAcceptedAction };
}
