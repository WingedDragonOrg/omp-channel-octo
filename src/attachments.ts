import { readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget, InboundAttachmentKind, InboundQuote } from "./contracts.ts";

/** Attachment directories untouched for this long are removed at service start. */
export const ATTACHMENT_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
/** Hard ceiling on attachments accepted from one message. */
export const MAX_MESSAGE_ATTACHMENTS = 8;
/** Wall-clock budget for downloading and transcribing one message's attachments. */
export const ATTACHMENT_DOWNLOAD_BUDGET_MS = 120_000;
/** Per-attachment download and transcription timeout. */
export const ATTACHMENT_DOWNLOAD_TIMEOUT_MS = 60_000;
const MAX_ATTACHMENT_NAME_LENGTH = 80;

/** One attachment already stored on this machine, as referenced by the task prompt. */
export interface SavedAttachment {
  path: string;
  kind: InboundAttachmentKind;
  name: string;
  bytes: number;
  durationSeconds: number;
  /** Server-side transcription, only ever set for audio attachments. */
  transcription: string | null;
}

export type AttachmentFailureReason = "oversize" | "timeout" | "failed";

/**
 * Thrown by `ChannelTransport.downloadAttachment` so the service can pick the matching
 * user-visible refusal without parsing messages.
 */
export class AttachmentError extends Error {
  readonly reason: AttachmentFailureReason;

  constructor(reason: AttachmentFailureReason, message: string) {
    super(message);
    this.name = "AttachmentError";
    this.reason = reason;
  }
}

export function attachmentRoot(stateDir: string): string {
  return join(stateDir, "attachments");
}

/** `<stateDir>/attachments/<channelType>-<channelId>/<messageId>/`, sanitized for the filesystem. */
export function attachmentDirectory(stateDir: string, target: ChatTarget, messageId: string): string {
  const segment = (value: string): string => {
    const cleaned = value.replace(/[^A-Za-z0-9._-]/gu, "_").replace(/^\.+/u, "");
    return cleaned.length === 0 ? "unknown" : cleaned.slice(0, 120);
  };
  return join(attachmentRoot(stateDir), `${target.channelType}-${segment(target.channelId)}`, segment(messageId));
}

/**
 * Builds the stored file name for one attachment: the client-declared name when it survives
 * sanitizing, otherwise a kind-based fallback carrying the extension inferred from the name
 * or the URL. The `index` prefix keeps same-named attachments of one message apart.
 */
export function safeAttachmentName(
  name: string,
  index: number,
  kind: InboundAttachmentKind,
  url: string,
  extensionHint = "",
): string {
  const extensionOf = (value: string): string => {
    const base = (value.split(/[?#]/u)[0] ?? "").split(/[\\/]/u).pop() ?? "";
    return /(\.[A-Za-z0-9]{1,12})$/u.exec(base)?.[1] ?? "";
  };
  const hinted = extensionHint.trim().length > 0
    ? (extensionHint.trim().startsWith(".") ? extensionHint.trim() : `.${extensionHint.trim()}`)
    : "";
  const extension = /^\.[A-Za-z0-9]{1,12}$/u.test(hinted)
    ? hinted
    : extensionOf(name) || extensionOf(url);
  const base = (name.split(/[\\/]/u).pop() ?? "")
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]/gu, "")
    .replace(/[:*?"<>|]/gu, "_")
    .trim()
    .replace(/^\.+/u, "")
    .slice(0, MAX_ATTACHMENT_NAME_LENGTH);
  if (base.length === 0) return `${index}-${kind}${extension}`;
  return `${index}-${base.toLowerCase().endsWith(extension.toLowerCase()) || extension.length === 0 ? base : `${base}${extension}`}`;
}

/** Renders the transcription, quoted-message, and attachment blocks appended to the user text. */
export function composePromptText(
  text: string,
  files: readonly SavedAttachment[],
  quote: InboundQuote | null = null,
): string {
  const quoteText = quote?.text.trim() ?? "";
  // 没有附件也没有引用时保持逐字返回：普通文本任务的正文与今天完全一致。
  if (files.length === 0 && quoteText.length === 0) return text;
  const sections = [text.trim()];
  if (quoteText.length > 0) {
    const senderName = quote?.senderName.trim() ?? "";
    const heading = senderName.length > 0 ? `[引用 ${senderName}]` : "[引用]";
    sections.push(`${heading}\n${quoteText}`);
  }
  for (const file of files) {
    const transcription = file.transcription?.trim() ?? "";
    if (transcription.length > 0) sections.push(`[语音转写] ${transcription}`);
  }
  if (files.length > 0) {
    const lines = files.map((file, index) => {
      const duration = file.durationSeconds > 0 ? ` (${file.durationSeconds}s)` : "";
      return `${index + 1}. ${file.kind} ${file.name} ${formatBytes(file.bytes)}${duration} → ${file.path}`;
    });
    sections.push(["附件（已保存到本机；需要时用 read 打开）：", ...lines].join("\n"));
  }
  return sections.filter(section => section.length > 0).join("\n\n");
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  const kib = bytes / 1024;
  if (kib < 1024) return `${kib.toFixed(1)} KB`;
  return `${(kib / 1024).toFixed(1)} MB`;
}

/**
 * Removes attachment directories whose last modification is older than `maxAgeMs`.
 * Best-effort: individual failures are ignored so a stale entry cannot block startup.
 */
export async function sweepAttachments(stateDir: string, maxAgeMs: number, now = Date.now()): Promise<number> {
  const root = attachmentRoot(stateDir);
  let channels: string[];
  try {
    channels = await readdir(root);
  } catch {
    return 0;
  }
  let removed = 0;
  for (const channel of channels) {
    const channelPath = join(root, channel);
    let messages: string[];
    try {
      messages = await readdir(channelPath);
    } catch {
      continue;
    }
    for (const message of messages) {
      const messagePath = join(channelPath, message);
      try {
        const info = await stat(messagePath);
        if (!info.isDirectory() || now - info.mtimeMs <= maxAgeMs) continue;
        await rm(messagePath, { recursive: true, force: true });
        removed += 1;
      } catch {
        continue;
      }
    }
    try {
      if ((await readdir(channelPath)).length === 0) await rm(channelPath, { recursive: true, force: true });
    } catch {
      continue;
    }
  }
  return removed;
}
