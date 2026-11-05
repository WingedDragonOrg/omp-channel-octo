/**
 * 入站消息的唯一组装点：在权限门、命令解析与待回答问题判定之后，把消息组装成任务正文
 * （用户文本 + 语音转写 + 附件清单）。附件在这里下载到本机、语音在这里转写，下载目录与
 * 命令 claim 的生命周期也由这里独占——失败时经命令 claim + outbox 只回复一次并返回 `null`，
 * 不建任务、不进入 steering；`null` 也表示该消息已被更早的处理路径（含重投去重）消费。
 *
 * `text` 默认是整条消息正文。命令路径传入自己的正文：`/thread <任务>` 与 `/queue <文本>` 传
 * 命令参数，注册 handler 传整条消息——凡是把正文交给 Agent 的命令路径都经这里组装，附件
 * （图片/文件/语音/视频）与引用块因此不会被命令吃掉。不消费正文的控制命令（`/status`、
 * `/cancel`、`/mode` 等）与待回答问题的答案不经过这里。
 *
 * 模块不依赖 `service.ts`：配置、入站记录的所有权（store）、传输层的下载与转写、群历史关联
 * 候选、停机取消信号、命令 claim 的构造与聊天出站队列都由 `CoreService` 以端口注入，由它在
 * 构造函数里创建一次本组装器。停机信号按对象注入（`abort()` 原地生效），因此每次组装读到的
 * 都是当前状态。
 */

import { chmod, mkdir, readdir, rm, stat, rename } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { join } from "node:path";
import {
  ATTACHMENTS_DISABLED_TEXT,
  ATTACHMENTS_OVER_BUDGET_TEXT,
  ATTACHMENT_DOWNLOAD_BUDGET_MS,
  ATTACHMENT_DOWNLOAD_TIMEOUT_MS,
  AttachmentError,
  MAX_MESSAGE_ATTACHMENTS,
  attachmentDirectory,
  attachmentRefusalText,
  composePromptText,
  existingFileSize,
  formatBytes,
  safeAttachmentName,
  type AttachmentFailureReason,
  type SavedAttachment,
} from "./attachments.ts";
import { recvIngressSourceId, type IngressJobRecord } from "./ingress-types.ts";
import type {
  ChannelTransport,
  CoreServiceConfig,
  HistoryMessageAttachment,
  InboundAttachment,
  InboundMessage,
} from "../contracts.ts";
import { InboundShutdownError } from "../errors.ts";
import { ATTACHMENT_KIND_LABELS } from "./group-history.ts";
import type { GroupHistoryCoordinator } from "./history-coordinator.ts";
import type { Outbox } from "../outbox.ts";
import type { ChannelStore } from "../store/index.ts";
import type { InboundClaim } from "../store/store-records.ts";

/** 系统说明行与正文之间保持空行分隔；正文为空时说明行本身就是正文。 */
function withSystemNote(text: string, note: string): string {
  const body = text.trim();
  return body.length === 0 ? note : `${body}\n\n${note}`;
}

/** 「《name》(416.7 KB)」/「《name》(416.7 KB) 等 2 项」；无名时退化为类型词，超长名截断。 */
function describeHistoryAttachments(attachments: readonly HistoryMessageAttachment[]): string {
  const first = attachments[0];
  if (first === undefined) return "附件消息";
  const label = ATTACHMENT_KIND_LABELS[first.kind] ?? "附件";
  const name = first.name.trim();
  const short = Array.from(name).length > 60 ? `${Array.from(name).slice(0, 60).join("")}…` : name;
  const size = first.size > 0 ? `（${formatBytes(first.size)}）` : "";
  const head = short.length > 0 ? `《${short}》${size}` : `${label}${size}`;
  return attachments.length > 1 ? `${head} 等 ${attachments.length} 项` : head;
}

/**
 * 关联取回失败时的诚实降级说明：绝不虚构「已收到」，而是让 Agent 据此请发送者引用或重传。
 * 与拒收文案不同，这里任务本身继续——关联是尽力协助，不是发送者的显式动作。
 */
function associationFailureNote(
  reason: AttachmentFailureReason,
  maxBytes: number,
  summary: string,
): string {
  let cause = "下载失败";
  if (reason === "oversize") {
    const megabytes = maxBytes / (1024 * 1024);
    cause = `超过 ${Number.isInteger(megabytes) ? megabytes : megabytes.toFixed(1)} MB 上限`;
  } else if (reason === "budget" || reason === "timeout") {
    cause = "下载超时";
  }
  return `[系统提示] 你刚才发送的${summary}未能取回（${cause}）；本条按纯文本处理，未读取任何文件。如需我查看该文件，请引用那条消息或重新发送。`;
}


/**
 * 组装器可触达的边界，全部由 `CoreService` 注入：它自己的配置切片、入站记录的所有权（命令
 * claim 的抢占与回滚）、传输层的下载与转写、群历史的关联候选，以及停机取消信号与聊天出站
 * 队列。停机判定同时给出布尔视图（清理分支）与上抛视图（有界取消），两者读同一个 `stopping`。
 */
export interface InboundMaterializerDeps {
  readonly config: Pick<CoreServiceConfig, "stateDir" | "attachments" | "groupHistory">;
  readonly store: Pick<ChannelStore, "claimCommand" | "releaseClaim" | "completeCommand" | "failCommand">;
  readonly transport: Pick<ChannelTransport, "downloadAttachment" | "transcribeAudio">;
  /** 「这个」的关联候选：只经 `attachmentCandidates` 观察既有历史 buffer，拿不到可变状态。 */
  readonly history: Pick<GroupHistoryCoordinator, "attachmentCandidates">;
  /** 停机取消源；`abort()` 原地生效，因此每次组装都读到当前状态。 */
  readonly shutdownSignal: AbortSignal;
  readonly stopping: () => boolean;
  /** 停机门：未持久接受的消息在这里以 `InboundShutdownError` 上抛，传输层因此不会 ACK。 */
  readonly throwIfStopping: () => void;
  /** 本次入站的命令 claim：与消息自身的去重键相同，只是 `kind` 为 `command`。 */
  readonly commandClaim: (message: InboundMessage, name: string) => InboundClaim;
  /** 附件被拒时的一次性回复通道；调用方照旧 await 该微任务边界。 */
  readonly enqueueText: Outbox["enqueueText"];
  /** Validates that this exact preparing ingress row owns the materialization. */
  readonly getIngressJob?: (id: number) => IngressJobRecord | null;
}

/** 附件事务的唯一所有者：下载、关联、拒收与失败清理都在这里，`CoreService` 只调用 `materialize`。 */
export class InboundMaterializer {
  constructor(private readonly deps: InboundMaterializerDeps) {}
  /** Prepares a durable ingress envelope without claiming a second, message-only command identity. */
  async materializeForIngress(
    jobId: number,
    message: InboundMessage,
    text = message.text,
    operationSignal?: AbortSignal,
  ): Promise<
    | { readonly kind: "prepared"; readonly text: string; readonly spoolPaths: readonly string[] }
    | { readonly kind: "refused"; readonly claim: InboundClaim; readonly text: string }
    | { readonly kind: "consumed" }
  > {
    const job = this.deps.getIngressJob?.(jobId);
    if (
      job === null
      || job === undefined
      || job.state !== "preparing"
      || job.ackReady
      || job.sourceKind !== "recv"
      || job.sourceId !== recvIngressSourceId(message.channelType, message.channelId, message.messageId)
      || job.messageId !== message.messageId
      || job.payload?.kind !== "recv"
      || job.payload.message.channelType !== message.channelType
      || job.payload.message.channelId !== message.channelId
      || job.payload.message.messageId !== message.messageId
    ) {
      throw new Error("ingress job does not own this attachment preparation");
    }
    let refusal: { claim: InboundClaim; text: string } | undefined;

    const createdSpoolPaths: string[] = [];
    const prepared = await this.materializeInternal(message, text, operationSignal, {
      ingressOwned: true,
      ingressJobId: jobId,
      createdSpoolPaths,
      deferRefusal: (claim, refusalText) => {
        refusal = { claim, text: refusalText };
      },
    });
    if (refusal !== undefined) return { kind: "refused", ...refusal };
    if (prepared === null) return { kind: "consumed" };
    let spoolPaths: string[];
    try {
      spoolPaths = await this.spoolPaths(message);
    } catch (error) {
      if (this.deps.getIngressJob?.(jobId)?.state === "cancelled") {
        await this.cleanupCreatedSpool(message, createdSpoolPaths);
      }
      throw error;
    }
    if (this.deps.getIngressJob?.(jobId)?.state === "cancelled") {
      await this.cleanupCreatedSpool(message, createdSpoolPaths);
    }
    return { kind: "prepared", text: prepared, spoolPaths };
  }
  async materialize(
    message: InboundMessage,
    text = message.text,
    operationSignal?: AbortSignal,
  ): Promise<string | null> {
    return this.materializeInternal(message, text, operationSignal);
  }

  private async materializeInternal(
    message: InboundMessage,
    text: string,
    operationSignal: AbortSignal | undefined,
    options: {
      ingressOwned?: boolean;
      ingressJobId?: number;
      createdSpoolPaths?: string[];
      deferRefusal?: (claim: InboundClaim, text: string) => void;
    } = {},
  ): Promise<string | null> {
    this.deps.throwIfStopping();
    const quotedText = message.quoted?.text.trim() ?? "";
    const association = this.associatedAttachments(message);
    // 歧义时不取回任何文件：说明写进正文，任务照常接受；引用块（若有）照常组装。
    if (association?.kind === "ambiguous") {
      return composePromptText(withSystemNote(text, association.note), [], message.quoted);
    }
    // 只带引用、不带附件时也要把引用块交给 Agent；普通文本任务的正文保持逐字不变。
    if (message.attachments.length === 0 && association === null) {
      return quotedText.length === 0 ? text : composePromptText(text, [], message.quoted);
    }
    const associated = association !== null;
    const requested: InboundAttachment[] = associated ? association.attachments : message.attachments;
    const attachmentConfig = this.deps.config.attachments;
    if (attachmentConfig?.enabled !== true) {
      // 关联要求附件接收开启，走到这里的一定是消息自带的附件。
      if (text.trim().length > 0 || quotedText.length > 0) {
        return composePromptText(text, [], message.quoted);
      }
      const claim = this.deps.commandClaim(message, "attachment");
      if (!options.ingressOwned && !this.deps.store.claimCommand(claim)) return null;
      await this.refuseAttachments(message, claim, ATTACHMENTS_DISABLED_TEXT, options.deferRefusal);
      return null;
    }
    const claim = this.deps.commandClaim(message, "attachment");
    if (!options.ingressOwned && !this.deps.store.claimCommand(claim)) return null;
    if (requested.length > MAX_MESSAGE_ATTACHMENTS) {
      if (associated) {
        // 尽力协助失败一律诚实降级，不整条拒收。
        if (!options.ingressOwned) this.deps.store.releaseClaim(claim);
        return composePromptText(
          withSystemNote(text, associationFailureNote("budget", attachmentConfig.maxBytes, association.summary)),
          [],
          message.quoted,
        );
      }
      await this.refuseAttachments(message, claim, ATTACHMENTS_OVER_BUDGET_TEXT, options.deferRefusal);
      return null;
    }
    const directory = attachmentDirectory(this.deps.config.stateDir, message, message.messageId);
    const created: string[] = [];
    const temporary: string[] = [];
    const budget = new AbortController();
    // 整条消息的下载预算（含转写）：超时就中止在途下载，按超限失败处理。
    const budgetTimer = setTimeout(() => budget.abort(), ATTACHMENT_DOWNLOAD_BUDGET_MS);
    try {
      await mkdir(directory, { recursive: true, mode: 0o700 });
      await chmod(directory, 0o700);
      this.deps.throwIfStopping();
      const files: SavedAttachment[] = [];
      for (const [index, attachment] of requested.entries()) {
        // 客户端声明的大小先做超限预检，不发起无谓的请求。
        if (attachment.size > attachmentConfig.maxBytes) {
          throw new AttachmentError("oversize", `attachment ${index} exceeds ${attachmentConfig.maxBytes} bytes`);
        }
        const signal = AbortSignal.any([
          AbortSignal.timeout(ATTACHMENT_DOWNLOAD_TIMEOUT_MS),
          budget.signal,
          this.deps.shutdownSignal,
          ...(operationSignal === undefined ? [] : [operationSignal]),
        ]);
        const safeName = safeAttachmentName(attachment.name, index, attachment.kind, attachment.url);
        const canonicalPath = join(directory, safeName);
        const displayName = safeName.slice(String(index).length + 1);
        const key = createHash("sha256")
          .update(JSON.stringify([
            options.ingressJobId ?? null,
            message.channelType,
            message.channelId,
            message.messageId,
            index,
            attachment.url,
          ]))
          .digest("hex")
          .slice(0, 32);
        const namePrefix = `.omp-ingress-${key}-`;
        const canonicalSize = await existingFileSize(canonicalPath);
        let path: string | undefined = canonicalSize === null ? undefined : canonicalPath;
        let bytes: number | null = canonicalSize;
        if (operationSignal?.aborted) {
          throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
        }
        this.deps.throwIfStopping();
        // A crash can leave either our transfer target or the transport's own nested .part.
        // Remove only files under this job+attachment key; never touch existing final assets.
        for (const name of await readdir(directory)) {
          if (name.startsWith(namePrefix)) await rm(join(directory, name), { force: true });
        }
        if (bytes === null) {
          const temporaryPath = join(directory, `${namePrefix}${randomUUID()}.part`);
          temporary.push(temporaryPath);
          let reportedBytes: number;
          try {
            reportedBytes = await this.deps.transport.downloadAttachment({
              url: attachment.url,
              path: temporaryPath,
              maxBytes: attachmentConfig.maxBytes,
              signal,
            });
            if (operationSignal?.aborted) {
              throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
            }
          } catch (error) {
            if (error instanceof AttachmentError && error.reason === "oversize") throw error;
            if (this.deps.shutdownSignal.aborted) throw new InboundShutdownError();
            // 单条 60 秒超时与整条 120 秒预算都会中止同一条 signal；只有预算耗尽按「超限」回复。
            if (operationSignal?.aborted) {
              throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
            }
            if (budget.signal.aborted) throw new AttachmentError("budget", "attachment budget exhausted");
            if (signal.aborted) throw new AttachmentError("timeout", "attachment download timed out");
            if (error instanceof AttachmentError) throw error;
            throw new AttachmentError("failed", "attachment download failed");
          }
          const transfer = await stat(temporaryPath);
          if (
            !transfer.isFile()
            || transfer.size <= 0
            || transfer.size !== reportedBytes
            || transfer.size > attachmentConfig.maxBytes
          ) {
            throw new AttachmentError("failed", "attachment download was incomplete");
          }
          await chmod(temporaryPath, 0o600);
          this.deps.throwIfStopping();
          // Rename only a closed, size-checked transfer to the stable user-visible filename.
          await rename(temporaryPath, canonicalPath);
          created.push(canonicalPath);
          options.createdSpoolPaths?.push(canonicalPath);
          path = canonicalPath;
          bytes = transfer.size;
        }
        if (path === undefined || bytes === null) {
          throw new AttachmentError("failed", "attachment spool was not durably published");
        }
        await chmod(path, 0o600);
        files.push({
          path,
          kind: attachment.kind,
          name: displayName,
          bytes,
          durationSeconds: attachment.durationSeconds,
          transcription:
            attachment.kind === "audio"
              ? await this.transcribeInboundAudio(path, budget.signal, operationSignal)
              : null,
        });
      }
      this.deps.throwIfStopping();
      if (operationSignal?.aborted) {
        throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
      }
      const promptText = composePromptText(
        associated ? withSystemNote(text, association.note) : text,
        files,
        message.quoted,
      );
      if (!options.ingressOwned) this.deps.store.releaseClaim(claim);
      return promptText;
    } catch (error) {
      // Shutdown leaves a preparing job recoverable, so keep complete published assets for its
      // next attempt. Explicit cancellation has removed durable ownership and must clean only the
      // files this attempt created, never files it reused.
      const currentIngress = options.ingressOwned && options.ingressJobId !== undefined
        ? this.deps.getIngressJob?.(options.ingressJobId)
        : null;
      const retainCompletedSpool = options.ingressOwned
        && currentIngress?.state !== "cancelled"
        && (operationSignal?.aborted || this.deps.shutdownSignal.aborted || this.deps.stopping());
      const cleanupPaths = retainCompletedSpool ? temporary : [...created, ...temporary];
      for (const path of cleanupPaths) await rm(path, { force: true }).catch(() => {});
      try {
        if ((await readdir(directory)).length === 0) {
          await rm(directory, { recursive: true, force: true });
        }
      } catch {
        // 目录未落盘或不可读时无需清理。
      }
      if (operationSignal?.aborted) {
        if (!options.ingressOwned) this.deps.store.releaseClaim(claim);
        throw operationSignal.reason instanceof Error
          ? operationSignal.reason
          : new Error("inbound preparation cancelled");
      }
      if (this.deps.stopping()) {
        // 停机中断时任务未接受：不发「附件拒绝」回复、不建任务；命令 claim 随中断回滚，
        // 同一消息重启后可重新接收；取消错误上抛，传输层不会 ACK。
        if (!options.ingressOwned) this.deps.store.releaseClaim(claim);
        throw error instanceof InboundShutdownError ? error : new InboundShutdownError();
      }
      if (operationSignal?.aborted) {
        this.deps.store.releaseClaim(claim);
        throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
      }
      const reason: AttachmentFailureReason = error instanceof AttachmentError ? error.reason : "failed";
      if (associated) {
        // 关联尽力协助失败：诚实降级为纯文本说明，而不是整条拒收——用户并未显式提交附件。
        if (!options.ingressOwned) this.deps.store.releaseClaim(claim);
        return composePromptText(
          withSystemNote(text, associationFailureNote(reason, attachmentConfig.maxBytes, association.summary)),
          [],
          message.quoted,
        );
      }
      await this.refuseAttachments(
        message,
        claim,
        attachmentRefusalText(reason, attachmentConfig.maxBytes),
        options.deferRefusal,
      );
      return null;
    } finally {
      clearTimeout(budgetTimer);
    }
  }

  private async cleanupCreatedSpool(message: InboundMessage, paths: readonly string[]): Promise<void> {
    const directory = attachmentDirectory(this.deps.config.stateDir, message, message.messageId);
    for (const path of paths) await rm(path, { force: true }).catch(() => {});
    try {
      if ((await readdir(directory)).length === 0) {
        await rm(directory, { recursive: true, force: true });
      }
    } catch {
      // The directory may already be absent or temporarily unreadable.
    }
  }

  /** Exact complete local files owned by this message; ingress jobs protect these from sweeping. */
  async spoolPaths(message: InboundMessage): Promise<string[]> {
    const directory = attachmentDirectory(this.deps.config.stateDir, message, message.messageId);
    try {
      const names = await readdir(directory);
      return names
        .filter(name => !name.startsWith(".omp-ingress-"))
        .map(name => join(directory, name));
    } catch (error) {
      if (
        error !== null
        && typeof error === "object"
        && "code" in error
        && error.code === "ENOENT"
      ) return [];
      throw error;
    }
  }

  /**
   * 「这个」场景的附件关联：受权消息本身无附件时，把同一发送者在最近
   * `GROUP_ATTACHMENT_ASSOCIATION_WINDOW_MS` 内、本进程实际观察到的带附件消息作为候选——
   * 恰好一条才视同所指并走与自带附件完全相同的下载/限额流程；多条只让 Agent 请其引用，
   * 零条则完全不改变行为（返回 null）。候选全部来自群历史 buffer（live 观察；服务重启
   * 期间的消息只经历史回填渲染、不参与取回），因此依赖群历史与附件接收同时开启；
   * 私聊没有历史 buffer，天然不参与。权限未扩大：候选消息本就在 bot 可读的群里，
   * 且请求者与发件人必须是同一人——绝不把别的成员的文件猜成「这个」。
   */
  private associatedAttachments(
    message: InboundMessage,
  ): { kind: "matched"; attachments: InboundAttachment[]; summary: string; note: string }
    | { kind: "ambiguous"; note: string }
    | null {
    if (message.attachments.length > 0) return null;
    if (this.deps.config.groupHistory?.enabled !== true) return null;
    if (this.deps.config.attachments?.enabled !== true) return null;
    if (message.channelType !== 2 && message.channelType !== 5) return null;
    if (!Number.isInteger(message.messageSeq) || message.messageSeq <= 0) return null;
    if (message.messageTimestamp <= 0) return null;
    const senderId = message.senderId.trim();
    if (senderId.length === 0) return null;
    const candidates = this.deps.history.attachmentCandidates(message);
    if (candidates.length === 0) return null;
    if (candidates.length === 1) {
      const candidate = candidates[0]!;
      const attachments = (candidate.attachments ?? [])
        .map((attachment) => ({ ...attachment, durationSeconds: 0 }));
      const summary = describeHistoryAttachments(candidate.attachments ?? []);
      return {
        kind: "matched",
        attachments,
        summary,
        note: `[系统提示] 本条消息未携带附件或显式引用；已按「同一发送者、最近 10 分钟内唯一一条附件消息」的规则取回你发送的${summary}，下方附件块即该文件。若它不是你所指的，请说明并引用对应消息。`,
      };
    }
    const listed = candidates.slice(-3)
      .map((candidate) => describeHistoryAttachments(candidate.attachments ?? []))
      .join("，");
    const more = candidates.length > 3 ? ` 等 ${candidates.length} 条` : "";
    return {
      kind: "ambiguous",
      note: `[系统提示] 本条消息未携带附件或显式引用；你最近 10 分钟内发送过 ${candidates.length} 条附件消息（${listed}${more}），无法确定「这个」指哪一份。请引用对应文件消息或重新上传；在那之前不要读取其中任何一份。`,
    };
  }

  /**
   * 语音转写是 best-effort：普通失败、空文本甚至单条转写超时都降级为无转写，任务照常接受；
   * 只有整条消息的下载预算被耗尽才按超限失败处理。
   */
  private async transcribeInboundAudio(
    path: string,
    budgetSignal: AbortSignal,
    operationSignal?: AbortSignal,
  ): Promise<string | null> {
    const signal = AbortSignal.any([
      AbortSignal.timeout(ATTACHMENT_DOWNLOAD_TIMEOUT_MS),
      budgetSignal,
      this.deps.shutdownSignal,
      ...(operationSignal === undefined ? [] : [operationSignal]),
    ]);
    try {
      const transcription = await this.deps.transport.transcribeAudio(path, signal);
      return transcription !== null && transcription.trim().length > 0 ? transcription : null;
    } catch {
      if (operationSignal?.aborted) {
        throw operationSignal.reason instanceof Error
          ? operationSignal.reason
          : new Error("inbound preparation cancelled");
      }
      // 停机中止不能降级成「无转写」后继续组装：被中断的入站以有界取消上抛。
      if (this.deps.shutdownSignal.aborted) throw new InboundShutdownError();
      if (operationSignal?.aborted) {
        throw operationSignal.reason instanceof Error ? operationSignal.reason : new Error("inbound operation aborted");
      }
      if (budgetSignal.aborted) {
        throw new AttachmentError("budget", "attachment budget exhausted during transcription");
      }
      return null;
    }
  }

  /** Attachment refusal: defer persistence to the ingress handoff when owned by a durable job. */
  private async refuseAttachments(
    message: InboundMessage,
    claim: InboundClaim,
    text: string,
    deferRefusal?: (claim: InboundClaim, text: string) => void,
  ): Promise<void> {
    if (deferRefusal !== undefined) {
      deferRefusal(claim, text);
      return;
    }
    try {
      await this.deps.enqueueText(message, text, null);
      this.deps.store.completeCommand(claim);
    } catch {
      this.deps.store.failCommand(claim);
      throw new Error("本地状态盘写入失败，未能记录附件消息。");
    }
  }
}
