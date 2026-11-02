/**
 * 入站权限门的所有者：允许清单（配置基线 + owner 在状态盘里运行时开通的行）、群级免@的
 * 判定与两条缓存（结果与在途去重），以及两条「拒绝也能说话」的通知（未开通群的开通提示、
 * 成员命令提示）。`isAllowed` 与 `isPermittedChat` 分工明确：前者是入站唯一权限门，
 * 后者只回答「这个聊天在允许清单里吗」，供问答卡的成员作答判定复用。
 *
 * 模块不依赖 `service.ts`：bot 身份（getter，`start` 之后才学习到）、允许清单基线、写回端口、
 * 状态盘窄面、群级免@查询、停机信号、命令 claim 与出站排在构造时由 `CoreService` 注入。
 * 缓存与通知窗口 map 属本对象独占，外部拿不到可变集合。群对子区的允许清单继承规则是同一个：
 * 子区由自身 id 或父群 id 放行（见 `isAllowlistedChat`）。
 */

import type {
  AllowlistWriteback,
  BotIdentity,
  ChannelTransport,
  CoreServiceConfig,
  InboundMessage,
} from "../contracts.ts";
import { InboundShutdownError } from "../errors.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { InboundClaim } from "../store/store-records.ts";
import { targetKey } from "../target.ts";
import { threadParentGroupId } from "../threads/thread-target.ts";

/** How long one 免@ decision is reused before the group switch is consulted again. */
const MENTION_PREF_TTL_MS = 60_000;
/** A failed lookup keeps the stricter "must be @mentioned" answer only briefly. */
const MENTION_PREF_FAILURE_TTL_MS = 10_000;
/** Bounds the 免@ lookup on the inbound path: the handler, and RECVACK, wait for it. */
const MENTION_PREF_TIMEOUT_MS = 5_000;
/**
 * The one exception that lets an allowlist rejection speak: an owner's @Bot mention in an unlisted
 * group or thread gets a short notice carrying this exact reply text (compared after Bot-mention
 * stripping and outer whitespace trimming), and that reply enables the chat durably.
 */
const UNLISTED_CHAT_PASSPHRASE = "开通本群";
/** One notice per unlisted chat per window, so a burst of messages cannot become a burst of replies. */
const UNLISTED_CHAT_NOTICE_TTL_MS = 10 * 60 * 1_000;
/** Bound the process-local map while preserving one notice per chat during its TTL. */
const MAX_UNLISTED_CHAT_NOTICES = 500;
/** Same window for the members-cannot-command notice: the first command of a chat is explained once. */
const MEMBER_COMMAND_NOTICE_TTL_MS = 10 * 60 * 1_000;

/**
 * A thread (channelType=5) inherits its parent group's allowlist entry, so a freshly
 * created thread needs no extra configuration; an explicit full channel ID still wins.
 */
function isAllowlistedChat(
  allowedGroupIds: readonly string[],
  message: Pick<InboundMessage, "channelId" | "channelType">,
): boolean {
  if (allowedGroupIds.includes(message.channelId)) return true;
  if (message.channelType !== 5) return false;
  const parentGroupId = threadParentGroupId(message.channelId);
  return parentGroupId !== null && allowedGroupIds.includes(parentGroupId);
}

/**
 * What the gate may touch outside its own caches: the bot identity (a getter because it is learned
 * after `start`), the config allowlist baseline, the config-file mirror of a runtime grant, the
 * store rows behind both, the group-level 免@ lookup, the shutdown signal that cancels it, and the
 * claim/outbox ports the two notices deliver through.
 */
export interface ChatAuthorizationDeps {
  readonly bot: () => BotIdentity | undefined;
  readonly allowedGroupIds: CoreServiceConfig["allowedGroupIds"];
  readonly allowlistWriteback: AllowlistWriteback | undefined;
  readonly store: Pick<
    ChannelStore,
    "listAllowedChats" | "grantAllowedChat" | "claimCommand" | "completeCommand" | "failCommand"
  >;
  readonly transport: Pick<ChannelTransport, "getGroupMentionPref">;
  readonly shutdownSignal: AbortSignal;
  readonly commandClaim: (message: InboundMessage, name: string) => InboundClaim;
  readonly offerUnlistedChat: (message: InboundMessage, bot: BotIdentity) => Promise<void>;
  readonly enqueueText: Outbox["enqueueText"];
}

export class ChatAuthorization {
  private readonly mentionPrefs = new Map<string, { effective: boolean; expiresAt: number }>();
  private readonly mentionPrefRequests = new Map<string, Promise<boolean>>();
  /** Last unlisted-chat notice per chat; process-local, so a restart may re-notify once. */
  private readonly unlistedChatNotices = new Map<string, number>();
  /** Last members-cannot-command notice per chat; same process-local window pattern. */
  private readonly memberCommandNotices = new Map<string, number>();

  constructor(private readonly deps: ChatAuthorizationDeps) {}

  /**
   * The allowlist is the config's `allowedGroupIds` plus the chats the owner enabled at runtime
   * (durable in the state store). Threads keep the same inheritance rule over both sets: a thread
   * is permitted by its own id or by its parent group's.
   */
  isPermittedChat(message: Pick<InboundMessage, "channelId" | "channelType">): boolean {
    if (isAllowlistedChat(this.deps.allowedGroupIds, message)) return true;
    const bot = this.deps.bot();
    if (!bot) return false;
    const granted = this.deps.store.listAllowedChats(bot.uid);
    return granted.length > 0 && isAllowlistedChat(granted.map((chat) => chat.channelId), message);
  }

  async isAllowed(message: InboundMessage, ownerId: string | null): Promise<boolean> {
    const bot = this.deps.bot();
    if (!bot || !ownerId || message.messageId.trim().length === 0 || message.senderId.trim().length === 0) {
      return false;
    }
    const senderId = message.senderId.trim();
    if (senderId === bot.uid) return false;
    if (message.channelType === 1) {
      return senderId === ownerId;
    }
    if (!this.isPermittedChat(message)) return false;
    if (message.mentionedUserIds.includes(bot.uid)) return true;
    // The 免@ exemption belongs to the owner: a member's unmentioned message never runs — and
    // never triggers the upstream lookup either, so a rejected member costs no request.
    if (senderId !== ownerId) return false;
    return await this.groupAllowsNoMention(message);
  }

  /**
   * The single exception to "an allowlist rejection is silent": an owner's @Bot mention in an
   * unlisted group or thread gets one short, throttled card/text onboarding offer. The exact
   * passphrase is handled before this rejection path so a redelivered grant never becomes work.
   * Every other rejection remains silent.
   */
  isOwnerMentionedGrantPassphrase(message: InboundMessage, ownerId: string | null): boolean {
    const bot = this.deps.bot();
    if (!bot || !ownerId || !message.supported) return false;
    const senderId = message.senderId.trim();
    return senderId === ownerId.trim()
      && senderId !== bot.uid
      && (message.channelType === 2 || message.channelType === 5)
      && message.mentionedUserIds.includes(bot.uid)
      && message.text.trim() === UNLISTED_CHAT_PASSPHRASE;
  }

  async handleUnlistedOwnerContact(message: InboundMessage, ownerId: string | null): Promise<void> {
    const bot = this.deps.bot();
    if (!bot || !ownerId) return;
    const senderId = message.senderId.trim();
    if (senderId !== ownerId.trim() || senderId === bot.uid) return;
    if (message.channelType !== 2 && message.channelType !== 5) return;
    if (!message.mentionedUserIds.includes(bot.uid)) return;
    if (this.isPermittedChat(message)) return;
    if (this.isOwnerMentionedGrantPassphrase(message, ownerId)) {
      await this.grantUnlistedChat(message);
      return;
    }
    await this.noticeUnlistedChat(message, bot);
  }

  /** Reclaims a previously failed text grant before an already-open chat can route it as work. */
  async retryClaimedOwnerGrant(message: InboundMessage, ownerId: string | null): Promise<void> {
    if (!this.isOwnerMentionedGrantPassphrase(message, ownerId)) return;
    await this.grantUnlistedChat(message);
  }

  /** 本群 vs 本子区: throttling is per real chat id, and the card builder owns the copy. */
  private async noticeUnlistedChat(message: InboundMessage, bot: BotIdentity): Promise<void> {
    const key = targetKey(message);
    const now = Date.now();
    for (const [noticeKey, sentAt] of this.unlistedChatNotices) {
      if (now - sentAt >= UNLISTED_CHAT_NOTICE_TTL_MS) this.unlistedChatNotices.delete(noticeKey);
    }
    const lastNoticedAt = this.unlistedChatNotices.get(key);
    if (lastNoticedAt !== undefined && now - lastNoticedAt < UNLISTED_CHAT_NOTICE_TTL_MS) return;
    if (this.unlistedChatNotices.size >= MAX_UNLISTED_CHAT_NOTICES) {
      const oldest = this.unlistedChatNotices.keys().next().value;
      if (oldest !== undefined) this.unlistedChatNotices.delete(oldest);
    }
    // Reserve before awaiting the transport so simultaneous owner mentions cannot fan out cards.
    this.unlistedChatNotices.set(key, now);
    try {
      await this.deps.offerUnlistedChat(message, bot);
    } catch (error) {
      if (this.unlistedChatNotices.get(key) === now) this.unlistedChatNotices.delete(key);
      throw error;
    }
  }

  /**
   * The only visible effect of a member's `/` message: the command is not executed and never
   * reaches the agent. One durable claim per message id keeps a redelivery silent, and one
   * notice per chat per window keeps a stream of attempts from becoming a stream of replies.
   */
  async noticeMemberCommand(message: InboundMessage): Promise<void> {
    const bot = this.deps.bot();
    if (!bot) return;
    const claim = this.deps.commandClaim(message, "member-command");
    if (!this.deps.store.claimCommand(claim)) return;
    const key = targetKey(message);
    const lastNoticedAt = this.memberCommandNotices.get(key);
    if (lastNoticedAt !== undefined && Date.now() - lastNoticedAt < MEMBER_COMMAND_NOTICE_TTL_MS) {
      this.deps.store.completeCommand(claim);
      return;
    }
    const botName = bot.name.trim().length > 0 ? bot.name.trim() : bot.uid;
    try {
      await this.deps.enqueueText(message, `命令仅 owner 可用；需要助手做事请直接 @${botName} 说明。`, null);
      this.memberCommandNotices.set(key, Date.now());
      this.deps.store.completeCommand(claim);
    } catch {
      this.deps.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未能记录成员命令提示。");
    }
  }

  /**
   * Enables the chat the owner answered the passphrase in. The grant row is the durable permission
   * (the next message needs no restart and no config reload); the config file is then updated as
   * the operator-visible mirror, and a mirror failure is reported without taking the grant back.
   */
  private async grantUnlistedChat(message: InboundMessage): Promise<void> {
    const botUid = this.deps.bot()!.uid;
    // The passphrase is a control action, not a task: one durable claim per message id, so a
    // redelivery after the grant cannot fall through to a task that prompts the model with it.
    const claim = this.deps.commandClaim(message, "allow-chat");
    if (!this.deps.store.claimCommand(claim)) return;
    const scope = message.channelType === 5 ? "本子区" : "本群";
    let mirrorFailed = false;
    try {
      this.deps.store.grantAllowedChat({
        botUid,
        channelType: message.channelType === 5 ? 5 : 2,
        channelId: message.channelId,
        grantedBy: message.senderId.trim(),
      });
      if (this.deps.allowlistWriteback) {
        try {
          await this.deps.allowlistWriteback.append(message.channelId);
        } catch {
          mirrorFailed = true;
        }
      }
      console.error(
        `[allow] 已开通${scope} ${targetKey(message)}（owner ${message.senderId.trim()}）${
          mirrorFailed ? "；配置文件写回失败，仅状态盘生效" : ""
        }`,
      );
      await this.deps.enqueueText(
        message,
        mirrorFailed
          ? `已开通${scope}；配置文件写回失败，本次开通仅在状态盘生效。`
          : `已开通${scope}，后续 @${this.deps.bot()!.name} 即可正常使用。`,
        null,
      );
      this.deps.store.completeCommand(claim);
    } catch {
      this.deps.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未记录开通结果。");
    }
  }

  /**
   * 群级免@：服务器把 bot owner 的意图与群级开关 AND 后给出 `effective`，为真时群里不必 @Bot。
   * 结果按群缓存，查询失败一律保持「需要 @」，不因错误内容放宽权限。
   */
  private async groupAllowsNoMention(message: InboundMessage): Promise<boolean> {
    // 线程的免@开关属于父群；线程自身没有独立设置。
    const groupId =
      message.channelType === 5 ? threadParentGroupId(message.channelId) : message.channelId.trim();
    if (groupId === null || groupId.length === 0) return false;
    const cached = this.mentionPrefs.get(groupId);
    if (cached !== undefined && cached.expiresAt > Date.now()) return cached.effective;
    const inflight = this.mentionPrefRequests.get(groupId);
    if (inflight !== undefined) return await inflight;
    // 并发入站只发一次查询；同一个 promise 同时充当在途去重与结果缓存。
    const request = (async (): Promise<boolean> => {
      let effective = false;
      let ttl = MENTION_PREF_FAILURE_TTL_MS;
      try {
        const pref = await this.deps.transport.getGroupMentionPref(
          groupId,
          AbortSignal.any([
            AbortSignal.timeout(MENTION_PREF_TIMEOUT_MS),
            this.deps.shutdownSignal,
          ]),
        );
        effective = pref.effective;
        ttl = MENTION_PREF_TTL_MS;
      } catch {
        // 停机中止查询不是「需要 @」的结论：未接受的入站以有界取消拒绝，重投后重新判定。
        if (this.deps.shutdownSignal.aborted) throw new InboundShutdownError();
        // 保留「需要 @」的既有行为，只做短暂负缓存，避免查询失败时长挂入站处理。
      }
      this.mentionPrefs.set(groupId, { effective, expiresAt: Date.now() + ttl });
      return effective;
    })();
    this.mentionPrefRequests.set(groupId, request);
    try {
      return await request;
    } finally {
      this.mentionPrefRequests.delete(groupId);
    }
  }
}
