import { expect, test, vi } from "bun:test";
import { mkdir, mkdtemp, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import {
  ATTACHMENT_MAX_AGE_MS,
  AttachmentError,
  attachmentDirectory,
  attachmentRoot,
  composePromptText,
  formatBytes,
  safeAttachmentName,
  sweepAttachments,
  type AttachmentFailureReason,
  type SavedAttachment,
} from "../src/attachments.ts";
import type { ChatTarget } from "../src/contracts.ts";

const ATTACHMENT_HEADER = "附件（已保存到本机；需要时用 read 打开）：";

interface SavedOptions {
  path: string;
  name: string;
  kind?: SavedAttachment["kind"];
  bytes?: number;
  durationSeconds?: number;
  transcription?: string | null;
}

function saved(options: SavedOptions): SavedAttachment {
  return {
    path: options.path,
    name: options.name,
    kind: options.kind ?? "image",
    bytes: options.bytes ?? 0,
    durationSeconds: options.durationSeconds ?? 0,
    transcription: options.transcription ?? null,
  };
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

test("keeps a client-declared name that survives sanitizing", () => {
  const name = safeAttachmentName("quarterly-report.pdf", 1, "file", "https://files.example.com/a/quarterly-report.pdf");
  expect(name).toBe("1-quarterly-report.pdf");
});

test("keeps only the last path segment of a name carrying separators", () => {
  expect(safeAttachmentName("a/b/c.png", 1, "image", "https://host/x.png")).toBe("1-c.png");
  expect(safeAttachmentName("a\\b\\c.png", 1, "image", "https://host/x.png")).toBe("1-c.png");
});

test("strips control characters and replaces reserved name characters", () => {
  expect(safeAttachmentName("bad\u0001name\u007f.png", 1, "image", "https://host/x.png")).toBe("1-badname.png");
  expect(safeAttachmentName(`:*?"<>|x.png`, 1, "image", "https://host/x.png")).toBe(`1-${"_".repeat(7)}x.png`);
  expect(safeAttachmentName("a:b*c?.png", 1, "image", "https://host/x.png")).toBe("1-a_b_c_.png");
});

test("falls back to the index and kind when nothing usable is left of the name", () => {
  expect(safeAttachmentName("", 3, "image", "https://host/media/photo.jpg")).toBe("3-image.jpg");
  expect(safeAttachmentName("\u0000\u0001", 2, "file", "https://host/download")).toBe("2-file");
  expect(safeAttachmentName("..", 4, "video", "https://host/v.mp4")).toBe("4-video.mp4");
});

test("truncates an overlong name to 80 characters before appending the extension", () => {
  const name = safeAttachmentName(`${"x".repeat(100)}.png`, 5, "image", "https://host/y.png");
  expect(name).toBe(`5-${"x".repeat(80)}.png`);
});

test("counts the 80-character limit in code points, so a surrogate pair is never cut in half", () => {
  const emoji = "😀".repeat(41); // 41 code points, 82 UTF-16 units: nothing to cut
  expect(safeAttachmentName(emoji, 5, "image", "https://host/y.png")).toBe(`5-${emoji}.png`);

  const boundary = `${"x".repeat(79)}😀`; // 80 code points, 81 units: the cut must not land inside the pair
  const kept = safeAttachmentName(boundary, 6, "image", "https://host/y.png");
  expect(kept).toBe(`6-${boundary}.png`);
  // 落盘名按 UTF-8 编码：孤立代理位会被写成 U+FFFD，往返编码一次就能看出来。
  expect(Buffer.from(kept, "utf8").toString("utf8")).toBe(kept);

  expect(safeAttachmentName(`${"y".repeat(85)}😀`, 7, "image", "https://host/y.png")).toBe(`7-${"y".repeat(80)}.png`);
});

test("keeps same-named attachments of one message apart by index", () => {
  const first = safeAttachmentName("photo.jpg", 6, "image", "https://host/photo.jpg");
  const second = safeAttachmentName("photo.jpg", 7, "image", "https://host/photo.jpg");
  expect(first).toBe("6-photo.jpg");
  expect(second).toBe("7-photo.jpg");
});

test("prefers the extension hint over the name and the URL", () => {
  expect(safeAttachmentName("photo.jpg", 1, "image", "https://host/photo.jpg", "png")).toBe("1-photo.jpg.png");
  expect(safeAttachmentName("photo.jpg", 1, "image", "https://host/photo.jpg", ".webp")).toBe("1-photo.jpg.webp");
  expect(safeAttachmentName("photo.jpg", 1, "image", "https://host/photo.jpg", "toolongextension")).toBe("1-photo.jpg");
});

test("never repeats an extension the name already carries", () => {
  expect(safeAttachmentName("report.PDF", 1, "file", "https://host/report.pdf")).toBe("1-report.PDF");
});

test("infers the extension from the URL when the name has none", () => {
  expect(safeAttachmentName("noext", 1, "file", "https://host/path/archive.tar.gz?token=1")).toBe("1-noext.gz");
  expect(safeAttachmentName("readme", 1, "file", "https://host/download")).toBe("1-readme");
});

test("lays attachment directories out under the state directory", () => {
  const stateDir = "/state";
  const group: ChatTarget = { channelId: "group-1", channelType: 2 };
  expect(attachmentRoot(stateDir)).toBe(join(stateDir, "attachments"));
  expect(attachmentDirectory(stateDir, group, "msg-1")).toBe(join(stateDir, "attachments", "2-group-1", "msg-1"));
});

test("replaces characters that cannot appear in a directory name", () => {
  const stateDir = "/state";
  const thread: ChatTarget = { channelId: "gr@p/../x", channelType: 5 };
  expect(attachmentDirectory(stateDir, thread, "msg-1")).toBe(join(stateDir, "attachments", "5-gr_p_.._x", "msg-1"));
  expect(attachmentDirectory(stateDir, thread, "a:b?c")).toBe(join(stateDir, "attachments", "5-gr_p_.._x", "a_b_c"));
});

test("keeps every sanitized segment inside the attachment root", () => {
  const stateDir = "/state";
  const root = attachmentRoot(stateDir);
  const group: ChatTarget = { channelId: "group-1", channelType: 2 };
  const escaped = attachmentDirectory(stateDir, group, "../../etc/passwd");
  expect(escaped).toBe(join(root, "2-group-1", "_.._etc_passwd"));
  const escapedSegments = relative(root, escaped).split(/[\\/]/u);
  expect(escapedSegments).toHaveLength(2);
  expect(escapedSegments).not.toContain("..");
  expect(attachmentDirectory(stateDir, group, "..")).toBe(join(root, "2-group-1", "unknown"));
  expect(attachmentDirectory(stateDir, { channelId: "", channelType: 1 }, "msg-1")).toBe(join(root, "1-unknown", "msg-1"));
});

test("returns the text verbatim when a message carries no attachments", () => {
  expect(composePromptText("  keep  the \n spacing ", [])).toBe("  keep  the \n spacing ");
});

test("renders the user text, the transcription and the numbered attachment lines", () => {
  const files = [
    saved({
      path: "/state/attachments/2-group-1/msg-1/1-chart.png",
      name: "1-chart.png",
      kind: "image",
      bytes: 1572864,
    }),
    saved({
      path: "/state/attachments/2-group-1/msg-1/2-voice.amr",
      name: "2-voice.amr",
      kind: "audio",
      bytes: 2048,
      durationSeconds: 3,
      transcription: "  明天下午三点开会  ",
    }),
  ];
  expect(composePromptText("看下这两个文件", files)).toBe([
    "看下这两个文件",
    "[语音转写] 明天下午三点开会",
    [
      ATTACHMENT_HEADER,
      "1. image 1-chart.png 1.5 MB → /state/attachments/2-group-1/msg-1/1-chart.png",
      "2. audio 2-voice.amr 2.0 KB (3s) → /state/attachments/2-group-1/msg-1/2-voice.amr",
    ].join("\n"),
  ].join("\n\n"));
});

test("starts at the attachment block when the message carried no text", () => {
  const result = composePromptText("   ", [
    saved({ path: "/state/attachments/2-group-1/msg-1/1-chart.png", name: "1-chart.png" }),
  ]);
  expect(result.startsWith(`${ATTACHMENT_HEADER}\n1. image 1-chart.png 0 B → /state/attachments/2-group-1/msg-1/1-chart.png`)).toBe(true);
  expect(result.startsWith("\n")).toBe(false);
  expect(result).not.toContain("\n\n\n");
});

test("omits the transcription block when there is no usable transcription", () => {
  const files = [
    saved({ path: "/p/1-voice.amr", name: "1-voice.amr", kind: "audio", bytes: 1024, transcription: "   " }),
    saved({ path: "/p/2-voice.amr", name: "2-voice.amr", kind: "audio", bytes: 1024, transcription: null }),
  ];
  expect(composePromptText("听下", files)).toBe([
    "听下",
    [
      ATTACHMENT_HEADER,
      "1. audio 1-voice.amr 1.0 KB → /p/1-voice.amr",
      "2. audio 2-voice.amr 1.0 KB → /p/2-voice.amr",
    ].join("\n"),
  ].join("\n\n"));
});

test("renders the quoted message ahead of the attachment block", () => {
  const files = [
    saved({ path: "/state/attachments/2-group-1/msg-2/0-quoted.png", name: "0-quoted.png", kind: "image", bytes: 2048 }),
  ];
  expect(composePromptText("看下这个", files, { senderName: " 董振兴 ", text: " 原消息 " })).toBe([
    "看下这个",
    "[引用 董振兴]\n原消息",
    [
      ATTACHMENT_HEADER,
      "1. image 0-quoted.png 2.0 KB → /state/attachments/2-group-1/msg-2/0-quoted.png",
    ].join("\n"),
  ].join("\n\n"));
});

test("keeps an attachment-only reply's quote without an empty text section", () => {
  expect(composePromptText("   ", [], { senderName: "", text: "被引用的正文" }))
    .toBe("[引用]\n被引用的正文");
});

test("renders the quote heading alone when the quoted message is a file without text", () => {
  const files = [
    saved({
      path: "/state/attachments/2-group-1/m-363/0-report.xlsx",
      name: "report.xlsx",
      kind: "file",
    }),
  ];
  expect(composePromptText("有这个附件你看一眼呗？", files, { senderName: "赵奕", text: "  " })).toBe([
    "有这个附件你看一眼呗？",
    "[引用 赵奕]",
    [ATTACHMENT_HEADER, "1. file report.xlsx 0 B → /state/attachments/2-group-1/m-363/0-report.xlsx"].join("\n"),
  ].join("\n\n"));
  // 引用与正文都为空、只带附件时不渲染引用标题。
  expect(composePromptText("  ", files, { senderName: "赵奕", text: "" })).toBe([
    "[引用 赵奕]",
    [ATTACHMENT_HEADER, "1. file report.xlsx 0 B → /state/attachments/2-group-1/m-363/0-report.xlsx"].join("\n"),
  ].join("\n\n"));
});

test("renders a video attachment line with its declared duration", () => {
  const files = [
    saved({
      path: "/state/attachments/2-group-1/msg-1/1-clip.mp4",
      name: "1-clip.mp4",
      kind: "video",
      bytes: 2048,
      durationSeconds: 12,
    }),
  ];
  expect(composePromptText("看视频", files)).toBe([
    "看视频",
    [
      ATTACHMENT_HEADER,
      "1. video 1-clip.mp4 2.0 KB (12s) → /state/attachments/2-group-1/msg-1/1-clip.mp4",
    ].join("\n"),
  ].join("\n\n"));
});

test("renders byte counts across the B, KB and MB boundaries", () => {
  expect(formatBytes(0)).toBe("0 B");
  expect(formatBytes(1023)).toBe("1023 B");
  expect(formatBytes(1024)).toBe("1.0 KB");
  expect(formatBytes(1536)).toBe("1.5 KB");
  expect(formatBytes(1048576)).toBe("1.0 MB");
});

test("renders unusable byte counts as zero", () => {
  expect(formatBytes(Number.NaN)).toBe("0 B");
  expect(formatBytes(Number.POSITIVE_INFINITY)).toBe("0 B");
  expect(formatBytes(-1)).toBe("0 B");
});

test("sweeps stale message directories and removes emptied channel directories", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-attachments-"));
  try {
    const stateDir = join(root, "state");
    const staleChannel = join(attachmentRoot(stateDir), "2-stale-group");
    const staleMessage = join(staleChannel, "msg-old");
    const freshMessage = join(staleChannel, "msg-new");
    const emptyChannel = join(attachmentRoot(stateDir), "5-empty-thread");
    await mkdir(staleMessage, { recursive: true });
    await mkdir(freshMessage, { recursive: true });
    await mkdir(emptyChannel, { recursive: true });
    await writeFile(join(staleMessage, "1-chart.png"), "stale");
    await writeFile(join(freshMessage, "1-chart.png"), "fresh");
    const now = Date.now();
    const stale = new Date(now - ATTACHMENT_MAX_AGE_MS - 24 * 60 * 60 * 1000);
    const fresh = new Date(now - 60 * 60 * 1000);
    await utimes(staleMessage, stale, stale);
    await utimes(freshMessage, fresh, fresh);

    expect(await sweepAttachments(stateDir, ATTACHMENT_MAX_AGE_MS, now)).toBe(1);
    expect(await exists(staleMessage)).toBe(false);
    expect(await exists(freshMessage)).toBe(true);
    expect(await exists(staleChannel)).toBe(true);
    expect(await exists(emptyChannel)).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("returns zero without logging when the attachment root does not exist", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-attachments-"));
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    expect(await sweepAttachments(join(root, "state"), ATTACHMENT_MAX_AGE_MS, Date.now())).toBe(0);
    expect(logged).not.toHaveBeenCalled();
  } finally {
    logged.mockRestore();
    await rm(root, { recursive: true, force: true });
  }
});

test("logs a directory it cannot read and still sweeps the remaining ones", async () => {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-octo-attachments-"));
  const logged = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    const stateDir = join(root, "state");
    const staleMessage = join(attachmentRoot(stateDir), "1-dm", "msg-old");
    await mkdir(staleMessage, { recursive: true });
    await writeFile(join(staleMessage, "1-chart.png"), "stale");
    const now = Date.now();
    const stale = new Date(now - ATTACHMENT_MAX_AGE_MS - 24 * 60 * 60 * 1000);
    await utimes(staleMessage, stale, stale);
    // 通道位被一个普通文件占住：readdir 以 ENOTDIR 失败，清理要留痕并且继续处理后续通道。
    const blocked = join(attachmentRoot(stateDir), "not-a-directory");
    await writeFile(blocked, "blocker");

    expect(await sweepAttachments(stateDir, ATTACHMENT_MAX_AGE_MS, now)).toBe(1);
    expect(await exists(staleMessage)).toBe(false);
    expect(await exists(blocked)).toBe(true);
    expect(logged).toHaveBeenCalledWith(expect.stringContaining(blocked), expect.anything());
  } finally {
    logged.mockRestore();
    await rm(root, { recursive: true, force: true });
  }
});

test("exposes the failure reason and a stable name on AttachmentError", () => {
  const error = new AttachmentError("oversize", "attachment exceeds the 25 MB limit");
  expect(error).toBeInstanceOf(Error);
  expect(error).toBeInstanceOf(AttachmentError);
  expect(error.name).toBe("AttachmentError");
  expect(error.reason).toBe("oversize");
  expect(error.message).toBe("attachment exceeds the 25 MB limit");

  const reasons: AttachmentFailureReason[] = ["oversize", "budget", "timeout", "failed"];
  expect(reasons.map(reason => new AttachmentError(reason, reason).reason)).toEqual(reasons);
});
