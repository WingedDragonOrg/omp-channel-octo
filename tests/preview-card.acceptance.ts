import { expect, test } from "bun:test";
import { DEFAULT_API_URL } from "../src/config.ts";
import type { ChatTarget } from "../src/contracts.ts";
import { createExecutionTracker, type ExecutionTracker } from "../src/execution-card.ts";
import { createOctoTransport } from "../src/octo/index.ts";

function firstNonEmpty(...values: readonly (string | undefined)[]): string | undefined {
  for (const value of values) {
    const trimmed = value?.trim();
    if (trimmed) return trimmed;
  }
  return undefined;
}

function previewChannelType(): ChatTarget["channelType"] {
  switch (firstNonEmpty(process.env.OCTO_PREVIEW_CHANNEL_TYPE) ?? "1") {
    case "1":
      return 1;
    case "2":
      return 2;
    case "5":
      return 5;
    default:
      throw new Error("OCTO_PREVIEW_CHANNEL_TYPE 必须是 1、2 或 5");
  }
}

type PreviewToolTerminalState = "succeeded" | "failed" | "cancelled";
type PreviewErrorCategory = "worker" | "cancelled";

function addPreviewTool(
  tracker: ExecutionTracker,
  options: {
    readonly toolCallId: string;
    readonly toolName: string;
    readonly atMs: number;
    readonly summary: string;
    readonly status?: PreviewToolTerminalState;
    readonly endAtMs?: number;
    readonly errorCategory?: PreviewErrorCategory;
  },
): void {
  tracker.apply({
    type: "tool_start",
    toolCallId: options.toolCallId,
    toolName: options.toolName,
    atMs: options.atMs,
    summary: options.summary,
  });
  if (options.status === undefined || options.endAtMs === undefined) return;
  tracker.apply({
    type: "tool_end",
    toolCallId: options.toolCallId,
    atMs: options.endAtMs,
    status: options.status,
    summary: options.summary,
    ...(options.errorCategory === undefined ? {} : { errorCategory: options.errorCategory }),
  });
}

function previewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-acceptance", initialAtMs: 0 });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: 0 });
  addPreviewTool(tracker, {
    toolCallId: "preview-read",
    toolName: "read",
    atMs: 100,
    endAtMs: 220,
    status: "succeeded",
    summary: "读取项目配置",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-write",
    toolName: "write",
    atMs: 260,
    endAtMs: 380,
    status: "succeeded",
    summary: "写入预览数据",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-edit",
    toolName: "edit",
    atMs: 420,
    endAtMs: 540,
    status: "failed",
    errorCategory: "worker",
    summary: "修改卡片布局",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-bash",
    toolName: "bash",
    atMs: 580,
    endAtMs: 700,
    status: "cancelled",
    errorCategory: "cancelled",
    summary: "运行回归检查",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-old-task",
    toolName: "task",
    atMs: 740,
    endAtMs: 860,
    status: "failed",
    errorCategory: "worker",
    summary: "子代理 · reviewer",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-await",
    toolName: "subagent_await",
    atMs: 900,
    summary: "等待子代理 · verifier",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-task",
    toolName: "task",
    atMs: 980,
    endAtMs: 1_120,
    status: "succeeded",
    summary: "子代理 · scout",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-hub-send",
    toolName: "hub",
    atMs: 1_160,
    endAtMs: 1_300,
    status: "failed",
    errorCategory: "worker",
    summary: "IRC → preview-agent · 未收到回复",
  });
  addPreviewTool(tracker, {
    toolCallId: "preview-ask",
    toolName: "ask",
    atMs: 1_340,
    summary: "等待用户回答 · 2 个问题",
  });
  tracker.apply({
    type: "todo_update",
    atMs: 1_360,
    op: "start",
    phases: [
      {
        name: "准备阶段",
        tasks: [
          { content: "读取项目配置", status: "completed" },
          { content: "确认卡片能力", status: "completed" },
          { content: "清理旧预览数据", status: "abandoned" },
        ],
      },
      {
        name: "执行阶段",
        tasks: [
          { content: "启动子代理检查", status: "completed" },
          { content: "汇总混合工具样式", status: "in_progress" },
          { content: "等待授权后继续", status: "blocked", blocker: "等待用户确认" },
        ],
      },
      {
        name: "验收阶段",
        tasks: [
          { content: "验证成功状态", status: "completed" },
          { content: "验证等待状态", status: "pending" },
          { content: "验证失败状态", status: "in_progress" },
        ],
      },
      {
        name: "交付阶段",
        tasks: [
          { content: "发送真实预览", status: "in_progress" },
          { content: "确认移动端布局", status: "cancelled" },
          { content: "记录验收结果", status: "pending" },
        ],
      },
    ],
  });
  return tracker.snapshot(1_800);
}

const botToken = firstNonEmpty(process.env.BOT_TOKEN, process.env.OCTO_BOT_TOKEN);
const runPreviewTest = botToken === undefined ? test.skip : test;

/**
 * Opt-in live acceptance test. The filename intentionally stays outside the
 * default *.test.ts discovery set because this test sends a real Octo message.
 */
runPreviewTest(
  "sends a comprehensive live preview card with tool, subagent, and todo states",
  async () => {
    const configuredTargetId = firstNonEmpty(process.env.OCTO_PREVIEW_TARGET_UID, process.env.OCTO_OWNER_ID);
    const transport = createOctoTransport({
      apiUrl: firstNonEmpty(process.env.OCTO_API_URL) ?? DEFAULT_API_URL,
      botToken: botToken!,
    });
    try {
      const identity = await transport.initialize();
      const targetId = configuredTargetId ?? identity.ownerUid;
      if (!targetId) {
        throw new Error("注册响应未提供 owner UID；请设置 OCTO_PREVIEW_TARGET_UID（或 OCTO_OWNER_ID）");
      }
      const preview = previewSnapshot();
      expect(preview.tools.map(tool => tool.toolName)).toEqual([
        "read",
        "write",
        "edit",
        "bash",
        "task",
        "subagent_await",
        "task",
        "hub",
        "ask",
      ]);
      expect(preview.tools.map(tool => tool.state)).toEqual([
        "succeeded",
        "succeeded",
        "failed",
        "cancelled",
        "failed",
        "running",
        "succeeded",
        "failed",
        "running",
      ]);
      expect(preview.todos).toHaveLength(4);
      expect(preview.todos.reduce((count, phase) => count + phase.tasks.length, 0)).toBe(12);
      expect(new Set(preview.todos.flatMap(phase => phase.tasks.map(task => task.status)))).toEqual(new Set([
        "completed",
        "in_progress",
        "pending",
        "blocked",
        "abandoned",
        "cancelled",
      ]));
      const receipt = await transport.sendProgressCard(
        { channelId: targetId, channelType: previewChannelType() },
        preview,
        `preview-card-${Date.now()}-${process.pid}`,
      );
      expect(receipt?.messageId).toEqual(expect.any(String));
      console.log(`综合预览卡片已发送到 ${targetId}，包含 9 次 tool 调用、8 类工具、4 个 Todo 阶段、12 个任务及成功/等待/失败/取消/进行中状态，请在 Octo 中打开最新卡片查看。`);
    } finally {
      await transport.stop();
    }
  },
  { timeout: 30_000 },
);
