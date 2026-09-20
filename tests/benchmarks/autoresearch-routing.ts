// /autoresearch 适配 benchmark —— channel 路由维度。
//
// 只测 omp-channel-octo 的路由语义，不实现 fake autoresearch 引擎：断言对象是
// FakeWorkerFactory 记录的 prompt，即真实 createChannelService（含真实 SQLite 状态盘）把
// 消息交付 OMP 这一刻的最终正文。bot identity、消息 id、sequence 与时间戳全部固定字面量，
// 无网络、日期或随机输入；每个 case 独立 harness，finally 里 stop + 删除状态目录。
// 功能缺口（首行被改写、权限门或去重失效）以 AssertionError 形式由公共 check 记为 GAP；
// 基础设施错误照常抛出，使整轮测量失败。
//
// 上游核对依据（仅作预期来源，不构成运行依赖）：OMP 的 /autoresearch 扩展中 goal 子命令经
// api.sendUserMessage 下发，无参数 / off / clear 走 ctx.ui.notify，agent_end 续跑带
// willContinue:true —— 这些消费方式都要求 prompt 首行原样保留命令文本。

import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { join } from "node:path";
import type { ChatTarget } from "../../src/contracts.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "../fixtures/core-fakes.ts";
import { check, type CheckResult } from "./autoresearch-types.ts";

const BOT = { uid: "bot-autoresearch-bench", name: "Autoresearch Bench Bot" } as const;
const OWNER = "owner-autoresearch-bench";
const MEMBER = "member-autoresearch-bench";
const DM: ChatTarget = { channelId: "dm-autoresearch-bench", channelType: 1 };
const ENABLED_GROUP: ChatTarget = { channelId: "group-autoresearch-enabled", channelType: 2 };
const UNLISTED_GROUP: ChatTarget = { channelId: "group-autoresearch-unlisted", channelType: 2 };
/** 群历史 fixture 的固定起点（2026-09-16T05:00Z），与 core-group-history 测试同一模式。 */
const T0 = Date.UTC(2026, 8, 16, 5, 0);

function rowCount(stateDir: string, table: "tasks" | "inbound" | "allowed_chats"): number {
  const database = new Database(join(stateDir, "channel.sqlite"), { readonly: true });
  try {
    const row = database.query(`SELECT COUNT(*) AS count FROM ${table}`).get() as { count: number };
    return row.count;
  } finally {
    database.close();
  }
}

/** (1) owner DM 的 /autoresearch goal：首行与整条正文都必须原样抵达 OMP。 */
async function caseOwnerDmGoalVerbatim(): Promise<void> {
  const harness = await createCoreHarness({ bot: BOT, ownerId: OWNER });
  try {
    await harness.service.handleInbound(inbound({
      ...DM,
      messageId: "ar-bench-dm-goal-0001",
      senderId: OWNER,
      text: "/autoresearch goal 以固定预算迭代 microGPT：验证指标必须逐轮提升\n补充：每轮固定同一数据切片与同一随机种子",
      messageTimestamp: T0,
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");

    assert.equal(harness.workers.prompts.length, 1);
    const forwarded = harness.workers.prompts[0]!.text;
    assert.equal(
      forwarded.split("\n", 1)[0],
      "/autoresearch goal 以固定预算迭代 microGPT：验证指标必须逐轮提升",
    );
    assert.equal(
      forwarded,
      "/autoresearch goal 以固定预算迭代 microGPT：验证指标必须逐轮提升\n补充：每轮固定同一数据切片与同一随机种子",
    );
  } finally {
    await closeCoreHarness(harness);
  }
}

/**
 * (2) 已开通群 + bot mention 的 /autoresearch goal：群历史已存在时命令首行不得被历史块
 * 顶掉（OMP 从 prompt 首行分派插件命令，前缀历史会让命令永远失效——属可记录的功能缺口）。
 * 复用 core-group-history 的确定性 fixture：先按固定 seq/时间戳投递三条成员消息，再触发命令，
 * 最后用一条普通任务证明历史确实存在、且命令轮既没有注入历史也没有推进水位线。
 */
async function caseGroupHistoryCommandFirstLine(): Promise<void> {
  const harness = await createCoreHarness({
    bot: BOT,
    ownerId: OWNER,
    allowedGroupIds: [ENABLED_GROUP.channelId],
    groupHistory: { enabled: true, maxMessages: 10 },
  });
  try {
    const members = [
      "历史消息一：上轮验证指标 0.412",
      "历史消息二：数据集切片固定为 s3",
      "历史消息三：每轮预算 20 分钟",
    ];
    for (const [index, text] of members.entries()) {
      const seq = index + 1;
      await harness.service.handleInbound(inbound({
        ...ENABLED_GROUP,
        messageId: `ar-bench-history-member-${seq}`,
        senderId: MEMBER,
        text,
        messageSeq: seq,
        messageTimestamp: T0 + seq * 60_000,
      }));
    }
    await harness.service.handleInbound(inbound({
      ...ENABLED_GROUP,
      messageId: "ar-bench-history-command",
      senderId: OWNER,
      text: "/autoresearch goal 守住首行：群历史不得改写命令",
      mentionedUserIds: [BOT.uid],
      messageSeq: 4,
      messageTimestamp: T0 + 4 * 60_000,
    }));
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, ENABLED_GROUP, (status) => status.state === "completed");

    assert.equal(harness.workers.prompts.length, 1);
    assert.equal(harness.workers.prompts[0]!.text, "/autoresearch goal 守住首行：群历史不得改写命令");
    // 命令轮不得发起历史补拉。
    assert.equal(harness.transport.historyCalls.length, 0);

    // 前提校验：历史确实存在，且 slash 轮没有消费它——下一条普通任务应注入这三条成员消息。
    await harness.service.handleInbound(inbound({
      ...ENABLED_GROUP,
      messageId: "ar-bench-history-followup",
      senderId: OWNER,
      text: "普通任务：汇总上面三条历史",
      mentionedUserIds: [BOT.uid],
      messageSeq: 5,
      messageTimestamp: T0 + 5 * 60_000,
    }));
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, ENABLED_GROUP, (status) => status.state === "completed");

    assert.equal(harness.workers.prompts.length, 2);
    const followUp = harness.workers.prompts[1]!.text;
    for (const content of [
      "历史消息一：上轮验证指标 0.412",
      "历史消息二：数据集切片固定为 s3",
      "历史消息三：每轮预算 20 分钟",
    ]) {
      assert.equal(followUp.split(content).length - 1, 1);
    }
  } finally {
    await closeCoreHarness(harness);
  }
}

/** (3) 成员在已开通群里发 /autoresearch clear --reset-tree：权限门必须拒绝执行——零 prompt、零 task 行、聊天无会话状态。 */
async function caseMemberClearRejected(): Promise<void> {
  const harness = await createCoreHarness({
    bot: BOT,
    ownerId: OWNER,
    allowedGroupIds: [ENABLED_GROUP.channelId],
  });
  try {
    await harness.service.handleInbound(inbound({
      ...ENABLED_GROUP,
      messageId: "ar-bench-member-clear-0001",
      senderId: MEMBER,
      text: "/autoresearch clear --reset-tree",
      mentionedUserIds: [BOT.uid],
      messageSeq: 1,
      messageTimestamp: T0 + 60_000,
    }));

    assert.equal(harness.workers.prompts.length, 0);
    assert.equal(rowCount(harness.stateDir, "tasks"), 0);
    assert.equal((await harness.service.status(ENABLED_GROUP)).state, "none");
  } finally {
    await closeCoreHarness(harness);
  }
}

/** (4) owner 命令重投：同一条 messageId 投递两次，只能有一个 prompt、一条 task 行。 */
async function caseOwnerRedeliverySingleTask(): Promise<void> {
  const harness = await createCoreHarness({ bot: BOT, ownerId: OWNER });
  try {
    const message = inbound({
      ...DM,
      messageId: "ar-bench-redelivery-0001",
      senderId: OWNER,
      text: "/autoresearch",
      messageTimestamp: T0,
    });
    await harness.service.handleInbound(message);
    await harness.service.handleInbound({ ...message });
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, DM, (status) => status.state === "completed");

    assert.equal(harness.workers.prompts.length, 1);
    assert.equal(harness.workers.prompts[0]!.text, "/autoresearch");
    assert.equal(rowCount(harness.stateDir, "tasks"), 1);
  } finally {
    await closeCoreHarness(harness);
  }
}

/** (5) owner 在未授权群发 /autoresearch：权限门必须拒绝执行——零 prompt、零 task、不写 inbound 去重行、不暗写 allowed_chats。 */
async function caseUnlistedGroupNotExecuted(): Promise<void> {
  const harness = await createCoreHarness({
    bot: BOT,
    ownerId: OWNER,
    allowedGroupIds: [ENABLED_GROUP.channelId],
  });
  try {
    await harness.service.handleInbound(inbound({
      ...UNLISTED_GROUP,
      messageId: "ar-bench-unlisted-0001",
      senderId: OWNER,
      text: "/autoresearch goal 未开通群不得执行",
      mentionedUserIds: [BOT.uid],
      messageSeq: 1,
      messageTimestamp: T0 + 60_000,
    }));

    assert.equal(harness.workers.prompts.length, 0);
    assert.equal(rowCount(harness.stateDir, "tasks"), 0);
    assert.equal(rowCount(harness.stateDir, "inbound"), 0);
    assert.equal(rowCount(harness.stateDir, "allowed_chats"), 0);
    assert.equal((await harness.service.status(UNLISTED_GROUP)).state, "none");
  } finally {
    await closeCoreHarness(harness);
  }
}

export async function measureRouting(): Promise<CheckResult[]> {
  return [
    await check("routing/owner-dm-goal-first-line-verbatim", caseOwnerDmGoalVerbatim),
    await check("routing/group-history-keeps-command-first-line", caseGroupHistoryCommandFirstLine),
    await check("routing/member-clear-reset-tree-rejected", caseMemberClearRejected),
    await check("routing/owner-command-redelivery-single-task", caseOwnerRedeliverySingleTask),
    await check("routing/unlisted-group-command-not-executed", caseUnlistedGroupNotExecuted),
  ];
}
