import { expect, test } from "bun:test";
import type { ChatTarget, ThreadSummary } from "../src/contracts.ts";
import {
  closeCoreHarness,
  createCoreHarness,
  inbound,
  submitAndWaitForIngressHandoff,
  waitForStatus,
  type CoreHarness,
  type FakePromptContext,
} from "./fixtures/core-fakes.ts";

const GROUP_A: ChatTarget = { channelType: 2, channelId: "schedule-group-a" };
const GROUP_B: ChatTarget = { channelType: 2, channelId: "schedule-group-b" };
const THREAD: ChatTarget = { channelType: 5, channelId: "schedule-group-a____topic-7" };

const OWNER_UID = "owner-1";
const MEMBER_UID = "schedule-member";
const OWNER_DM: ChatTarget = { channelType: 1, channelId: OWNER_UID };

interface ScheduleRecord {
  id: string;
  name: string;
  source: ChatTarget;
  target: ChatTarget;
  status: "active" | "paused" | "stopped";
}

interface ScheduleCreateResult {
  kind: "created" | "duplicate";
  schedule: ScheduleRecord;
}

interface ScheduleStopResult {
  kind: "stopped" | "unknown";
  schedule: ScheduleRecord;
  runs: unknown[];
}

interface ScheduleList {
  schedules: ScheduleRecord[];
}

function scheduleTool(context: FakePromptContext) {
  const tool = context.promptContext?.tools?.find(({ name }) => name === "schedule");
  if (!tool) throw new Error("schedule host tool missing");
  return tool;
}

function createArgs(name: string, target: ChatTarget, cwd: string) {
  return {
    action: "create",
    name,
    cron: "0 9 * * 1-5",
    timezone: "UTC",
    cwd,
    prompt: `Run ${name}`,
    target: { channelType: target.channelType, channelId: target.channelId },
  };
}

async function executeCreateSchedule(
  context: FakePromptContext,
  name: string,
  target: ChatTarget,
  cwd: string,
  toolCallId: string,
): Promise<string> {
  return scheduleTool(context).execute(
    createArgs(name, target, cwd),
    new AbortController().signal,
    toolCallId,
  );
}

async function createSchedule(
  context: FakePromptContext,
  name: string,
  target: ChatTarget,
  cwd: string,
  toolCallId: string,
): Promise<ScheduleCreateResult> {
  return JSON.parse(await executeCreateSchedule(context, name, target, cwd, toolCallId)) as ScheduleCreateResult;
}

async function listSchedules(context: FakePromptContext): Promise<ScheduleList> {
  const result = await scheduleTool(context).execute(
    { action: "list" },
    new AbortController().signal,
  );
  return JSON.parse(result) as ScheduleList;
}

function fixtureScheduleMembers(harness: CoreHarness) {
  const joinedAt = "2026-11-20T00:00:00.000Z";
  return [
    { uid: harness.bot.uid, name: harness.bot.name, role: 0, robot: true, joinedAt },
    { uid: OWNER_UID, name: "Schedule Owner", role: 1, robot: false, joinedAt },
    { uid: MEMBER_UID, name: "Schedule Member", role: 0, robot: false, joinedAt },
  ];
}

function setGroupAvailable(harness: CoreHarness, target: ChatTarget): void {
  harness.transport.setGroupInfo(target.channelId, {
    groupId: target.channelId,
    name: target.channelId,
    notice: "",
    creator: OWNER_UID,
    status: 1,
    createdAt: "2026-11-20T00:00:00.000Z",
  });
  harness.transport.setUserName(OWNER_UID, "Schedule Owner");
  harness.transport.setUserName(MEMBER_UID, "Schedule Member");
  harness.transport.setGroupMembers(target.channelId, fixtureScheduleMembers(harness));
}

function threadRecord(): ThreadSummary {
  return {
    shortId: "topic-7",
    groupId: GROUP_A.channelId,
    channelId: THREAD.channelId,
    name: "Schedule topic",
    creatorUid: OWNER_UID,
    status: 1,
    memberCount: 1,
    messageCount: 0,
    createdAt: "2026-11-20T00:00:00.000Z",
    updatedAt: "2026-11-20T00:00:00.000Z",
  };
}

function setThreadMembersAvailable(harness: CoreHarness): void {
  harness.transport.setThreadMembers(
    GROUP_A.channelId,
    "topic-7",
    fixtureScheduleMembers(harness),
  );
}

test("a directly authorized group task can create and list a schedule through the host tool", async () => {
  let created: ScheduleCreateResult | undefined;
  let listed: ScheduleList | undefined;
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId],
    prompt: async (context) => {
      created = await createSchedule(
        context,
        "direct-group-plan",
        GROUP_A,
        context.cwd,
        "direct-group-create",
      );
      listed = await listSchedules(context);
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-direct-member",
      senderId: MEMBER_UID,
      text: "Create and list a schedule in this group",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(created).toMatchObject({
      kind: "created",
      schedule: {
        name: "direct-group-plan",
        source: GROUP_A,
        target: GROUP_A,
      },
    });
    expect(listed?.schedules).toContainEqual(expect.objectContaining({
      id: created?.schedule.id,
      source: GROUP_A,
      target: GROUP_A,
    }));
  } finally {
    await closeCoreHarness(harness);
  }
});

test("only a directly authenticated owner can create a cross-group schedule", async () => {
  let ownerCreated: ScheduleCreateResult | undefined;
  let memberCreateResponse: string | undefined;
  let afterMemberAttempt: ScheduleList | undefined;
  const memberDone = Promise.withResolvers<void>();
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId, GROUP_B.channelId],
    prompt: async (context) => {
      if (context.text.includes("owner cross-group request")) {
        ownerCreated = await createSchedule(
          context,
          "owner-cross-group-plan",
          GROUP_B,
          context.cwd,
          "owner-cross-group-create",
        );
        return;
      }
      memberCreateResponse = await executeCreateSchedule(
        context,
        "member-cross-group-plan",
        GROUP_B,
        context.cwd,
        "member-cross-group-create",
      );
      afterMemberAttempt = await listSchedules(context);
      memberDone.resolve();
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    setGroupAvailable(harness, GROUP_B);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-owner-cross-group",
      senderId: OWNER_UID,
      text: "owner cross-group request",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-member-cross-group",
      senderId: MEMBER_UID,
      text: "member cross-group request",
      mentionedUserIds: [harness.bot.uid],
    }));
    await memberDone.promise;
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(ownerCreated).toMatchObject({
      kind: "created",
      schedule: { source: GROUP_A, target: GROUP_B },
    });
    expect(memberCreateResponse).toMatch(/^schedule: permission denied:/u);
    expect(afterMemberAttempt?.schedules).toHaveLength(1);
    expect(afterMemberAttempt?.schedules[0]).toMatchObject({
      id: ownerCreated?.schedule.id,
      target: GROUP_B,
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a different member steering an owner turn cannot create schedules for either group", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const steeringText = "member steering during owner schedule turn";
  let appendedText: string | undefined;
  let sameSourceResponse: string | undefined;
  let crossGroupResponse: string | undefined;
  let listed: ScheduleList | undefined;
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId, GROUP_B.channelId],
    prompt: async (context) => {
      if (!context.text.includes("owner source schedule turn")) return;
      promptStarted.resolve();
      const appended = await Promise.race([
        context.waitForAppend(),
        releasePrompt.promise.then(() => undefined),
      ]);
      if (appended === undefined) return;
      appendedText = appended.text;
      sameSourceResponse = await executeCreateSchedule(
        context,
        "different-steerer-same-source",
        GROUP_A,
        context.cwd,
        "different-steerer-same-source-create",
      );
      crossGroupResponse = await executeCreateSchedule(
        context,
        "different-steerer-cross-group",
        GROUP_B,
        context.cwd,
        "different-steerer-cross-group-create",
      );
      listed = await listSchedules(context);
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    setGroupAvailable(harness, GROUP_B);
    await submitAndWaitForIngressHandoff(harness, inbound({
      ...GROUP_A,
      messageId: "schedule-different-steerer-owner-source",
      senderId: OWNER_UID,
      text: "owner source schedule turn",
      mentionedUserIds: [harness.bot.uid],
    }));
    await promptStarted.promise;

    await submitAndWaitForIngressHandoff(harness, inbound({
      ...GROUP_A,
      messageId: "schedule-different-steerer-member",
      senderId: MEMBER_UID,
      text: steeringText,
      mentionedUserIds: [harness.bot.uid],
    }));
    releasePrompt.resolve();
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(harness.workers.workers[0]?.steerCalls).toEqual([steeringText]);
    expect(appendedText).toBe(steeringText);
    expect(sameSourceResponse).toMatch(/^schedule: permission denied:/u);
    expect(crossGroupResponse).toMatch(/^schedule: permission denied:/u);
    expect(listed?.schedules).toHaveLength(0);
  } finally {
    releasePrompt.resolve();
    await closeCoreHarness(harness);
  }
});

test("the owner can create a cross-group schedule after steering their own turn", async () => {
  const promptStarted = Promise.withResolvers<void>();
  const releasePrompt = Promise.withResolvers<void>();
  const steeringText = "owner steering during own schedule turn";
  let appendedText: string | undefined;
  let created: ScheduleCreateResult | undefined;
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId, GROUP_B.channelId],
    prompt: async (context) => {
      if (!context.text.includes("owner source schedule turn for owner steering")) return;
      promptStarted.resolve();
      const appended = await Promise.race([
        context.waitForAppend(),
        releasePrompt.promise.then(() => undefined),
      ]);
      if (appended === undefined) return;
      appendedText = appended.text;
      created = await createSchedule(
        context,
        "same-owner-steered-cross-group",
        GROUP_B,
        context.cwd,
        "same-owner-steered-cross-group-create",
      );
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    setGroupAvailable(harness, GROUP_B);
    await submitAndWaitForIngressHandoff(harness, inbound({
      ...GROUP_A,
      messageId: "schedule-same-owner-steerer-source",
      senderId: OWNER_UID,
      text: "owner source schedule turn for owner steering",
      mentionedUserIds: [harness.bot.uid],
    }));
    await promptStarted.promise;

    await submitAndWaitForIngressHandoff(harness, inbound({
      ...GROUP_A,
      messageId: "schedule-same-owner-steerer",
      senderId: OWNER_UID,
      text: steeringText,
      mentionedUserIds: [harness.bot.uid],
    }));
    releasePrompt.resolve();
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(harness.workers.workers[0]?.steerCalls).toEqual([steeringText]);
    expect(appendedText).toBe(steeringText);
    expect(created).toMatchObject({
      kind: "created",
      schedule: { source: GROUP_A, target: GROUP_B },
    });
  } finally {
    releasePrompt.resolve();
    await closeCoreHarness(harness);
  }
});

test("same-group main and thread requests retain their independently selected targets", async () => {
  let mainToThread: ScheduleCreateResult | undefined;
  let threadToMain: ScheduleCreateResult | undefined;
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId],
    prompt: async (context) => {
      if (context.text.includes("main-to-thread schedule")) {
        mainToThread = await createSchedule(
          context,
          "main-to-thread-plan",
          THREAD,
          context.cwd,
          "main-to-thread-create",
        );
      } else {
        threadToMain = await createSchedule(
          context,
          "thread-to-main-plan",
          GROUP_A,
          context.cwd,
          "thread-to-main-create",
        );
      }
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    harness.transport.setThreads(GROUP_A.channelId, [threadRecord()]);
    setThreadMembersAvailable(harness);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-main-to-thread",
      senderId: OWNER_UID,
      text: "main-to-thread schedule",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    await harness.service.submitInbound(inbound({
      ...THREAD,
      messageId: "schedule-thread-to-main",
      senderId: OWNER_UID,
      text: "thread-to-main schedule",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, THREAD, (status) => status.state === "completed");

    expect(mainToThread).toMatchObject({
      kind: "created",
      schedule: { source: GROUP_A, target: THREAD },
    });
    expect(threadToMain).toMatchObject({
      kind: "created",
      schedule: { source: THREAD, target: GROUP_A },
    });
  } finally {
    await closeCoreHarness(harness);
  }
});

test("source membership revoked after a valid schedule blocks another plan", async () => {
  let baseline: ScheduleCreateResult | undefined;
  let createResponse: string | undefined;
  let listed: ScheduleList | undefined;
  let harness!: CoreHarness;
  harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId],
    prompt: async (context) => {
      baseline = await createSchedule(
        context,
        "source-membership-baseline",
        GROUP_A,
        context.cwd,
        "source-membership-baseline-create",
      );
      harness.transport.setGroupMembers(
        GROUP_A.channelId,
        fixtureScheduleMembers(harness).filter(member => member.uid !== MEMBER_UID),
      );
      createResponse = await executeCreateSchedule(
        context,
        "revoked-source-member-plan",
        GROUP_A,
        context.cwd,
        "revoked-source-member-create",
      );
      harness.transport.setGroupMembers(GROUP_A.channelId, fixtureScheduleMembers(harness));
      listed = await listSchedules(context);
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-source-member-revoked",
      senderId: MEMBER_UID,
      text: "create after source membership revocation",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(baseline).toMatchObject({
      kind: "created",
      schedule: { source: GROUP_A, target: GROUP_A },
    });
    expect(createResponse).toMatch(/^schedule: permission denied:/u);
    expect(listed?.schedules).toHaveLength(1);
    expect(listed?.schedules[0]?.id).toBe(baseline?.schedule.id);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("target Bot membership revoked after a valid schedule blocks another plan", async () => {
  let baseline: ScheduleCreateResult | undefined;
  let createResponse: string | undefined;
  let listed: ScheduleList | undefined;
  let harness!: CoreHarness;
  harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId, GROUP_B.channelId],
    prompt: async (context) => {
      baseline = await createSchedule(
        context,
        "target-membership-baseline",
        GROUP_B,
        context.cwd,
        "target-membership-baseline-create",
      );
      harness.transport.setGroupMembers(
        GROUP_B.channelId,
        fixtureScheduleMembers(harness).filter(member => member.uid !== harness.bot.uid),
      );
      createResponse = await executeCreateSchedule(
        context,
        "revoked-target-bot-plan",
        GROUP_B,
        context.cwd,
        "revoked-target-bot-create",
      );
      listed = await listSchedules(context);
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    setGroupAvailable(harness, GROUP_B);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-target-bot-revoked",
      senderId: OWNER_UID,
      text: "create after target Bot membership revocation",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForStatus(harness.service, GROUP_A, (status) => status.state === "completed");

    expect(baseline).toMatchObject({
      kind: "created",
      schedule: { source: GROUP_A, target: GROUP_B },
    });
    expect(createResponse).toMatch(/^schedule: permission denied:/u);
    expect(listed?.schedules).toHaveLength(1);
    expect(listed?.schedules[0]?.id).toBe(baseline?.schedule.id);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a dispatched synthetic task with owner sender identity cannot create across groups", async () => {
  let syntheticCreateResponse: string | undefined;
  let schedulesAfterAttempt: ScheduleList | undefined;
  const harness = await createCoreHarness({
    allowedGroupIds: [GROUP_A.channelId, GROUP_B.channelId],
    prompt: async (context) => {
      if (context.text.includes("dispatch into the schedule thread")) {
        const management = context.promptContext?.tools?.find(({ name }) => name === "octo_management");
        if (!management) throw new Error("octo_management host tool missing");
        await management.execute(
          { action: "thread-dispatch", short_id: "topic-7", text: "synthetic schedule attempt" },
          new AbortController().signal,
          "dispatch-to-schedule-thread",
        );
        return;
      }

      syntheticCreateResponse = await executeCreateSchedule(
        context,
        "synthetic-cross-group-plan",
        GROUP_B,
        context.cwd,
        "synthetic-cross-group-create",
      );
      schedulesAfterAttempt = await listSchedules(context);
    },
  });
  try {
    setGroupAvailable(harness, GROUP_A);
    setGroupAvailable(harness, GROUP_B);
    harness.transport.setThreads(GROUP_A.channelId, [threadRecord()]);
    setThreadMembersAvailable(harness);
    await harness.service.submitInbound(inbound({
      ...GROUP_A,
      messageId: "schedule-member-dispatch",
      senderId: MEMBER_UID,
      text: "dispatch into the schedule thread",
      mentionedUserIds: [harness.bot.uid],
    }));
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, THREAD, (status) => status.state === "completed");
    expect(syntheticCreateResponse).toMatch(
      /^schedule: .*cross-chat schedules require a directly authenticated current owner/u,
    );
    expect(schedulesAfterAttempt?.schedules).toHaveLength(0);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("owner DM can list and stop a schedule whose source is that DM", async () => {
  let created: ScheduleCreateResult | undefined;
  let listed: ScheduleList | undefined;
  let stopped: ScheduleStopResult | undefined;
  const harness = await createCoreHarness({
    prompt: async (context) => {
      const createdRecord = await createSchedule(
        context,
        "owner-dm-plan",
        OWNER_DM,
        context.cwd,
        "owner-dm-create",
      );
      created = createdRecord;
      listed = await listSchedules(context);
      const stopResponse = await scheduleTool(context).execute(
        { action: "stop", schedule_id: createdRecord.schedule.id },
        new AbortController().signal,
        "owner-dm-stop",
      );
      stopped = JSON.parse(stopResponse) as ScheduleStopResult;
    },
  });
  try {
    harness.transport.setUserName(OWNER_UID, "Schedule Owner");
    await harness.service.submitInbound(inbound({
      ...OWNER_DM,
      messageId: "schedule-owner-dm",
      senderId: OWNER_UID,
      text: "create, list, and stop my DM schedule",
    }));
    await waitForStatus(harness.service, OWNER_DM, (status) => status.state === "completed");

    expect(created).toMatchObject({
      kind: "created",
      schedule: { source: OWNER_DM, target: OWNER_DM },
    });
    expect(listed?.schedules).toContainEqual(expect.objectContaining({
      id: created?.schedule.id,
      source: OWNER_DM,
      target: OWNER_DM,
    }));
    expect(stopped).toMatchObject({
      kind: "stopped",
      schedule: { id: created?.schedule.id, status: "stopped" },
      runs: [],
    });
  } finally {
    await closeCoreHarness(harness);
  }
});
