import { expect, test } from "bun:test";
import { DEFAULT_API_URL } from "../src/config.ts";
import type { ChatTarget } from "../src/contracts.ts";
import { createExecutionTracker } from "../src/execution-card.ts";
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

function previewSnapshot() {
  const tracker = createExecutionTracker({ runId: "preview-card-acceptance", initialAtMs: 0 });
  tracker.apply({ type: "turn_start", turnId: "preview-turn", atMs: 0 });
  tracker.apply({
    type: "tool_start",
    toolCallId: "preview-task",
    toolName: "task",
    atMs: 100,
    summary: "子代理 · scout",
  });
  tracker.apply({
    type: "tool_end",
    toolCallId: "preview-task",
    atMs: 240,
    status: "succeeded",
    summary: "子代理 · scout",
  });
  tracker.apply({
    type: "tool_start",
    toolCallId: "preview-hub-send",
    toolName: "hub",
    atMs: 280,
    summary: "IRC → preview-agent · 等待回复",
  });
  tracker.apply({
    type: "tool_end",
    toolCallId: "preview-hub-send",
    atMs: 420,
    status: "succeeded",
    summary: "IRC → preview-agent · 等待回复",
  });
  tracker.apply({
    type: "tool_start",
    toolCallId: "preview-ask",
    toolName: "ask",
    atMs: 460,
    summary: "等待用户回答 · 2 个问题",
  });
  tracker.apply({
    type: "todo_update",
    atMs: 480,
    op: "done",
    phases: [{
      name: "预览任务",
      tasks: [
        { content: "查看执行轨迹", status: "completed" },
        { content: "确认混合工具样式", status: "in_progress" },
      ],
    }],
  });
  return tracker.snapshot(900);
}

const botToken = firstNonEmpty(process.env.BOT_TOKEN, process.env.OCTO_BOT_TOKEN);
const runPreviewTest = botToken === undefined ? test.skip : test;

/**
 * Opt-in live acceptance test. The filename intentionally stays outside the
 * default *.test.ts discovery set because this test sends a real Octo message.
 */
runPreviewTest(
  "sends an immediate live mixed preview card for execution-trace acceptance",
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
      expect(preview.tools.map(tool => tool.toolName)).toEqual(["task", "hub", "ask"]);
      const receipt = await transport.sendProgressCard(
        { channelId: targetId, channelType: previewChannelType() },
        preview,
        `preview-card-${Date.now()}-${process.pid}`,
      );
      expect(receipt?.messageId).toEqual(expect.any(String));
      console.log(`混合预览卡片已发送到 ${targetId}，包含 task、hub、ask 执行轨迹及树形 Todo，请在 Octo 中打开最新卡片查看。`);
    } finally {
      await transport.stop();
    }
  },
  { timeout: 30_000 },
);
