import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { recvIngressSourceId } from "../src/inbound/ingress-types.ts";
import { openChannelStore } from "../src/store/index.ts";
import {
  FakeWorkerFactory,
  closeCoreHarness,
  createCoreHarness,
  inbound,
  waitForStatus,
} from "./fixtures/core-fakes.ts";


async function waitUntil(
  check: () => boolean | Promise<boolean>,
  label: string,
  describeFailure?: () => string,
): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  const details = describeFailure?.();
  throw new Error(`did not observe ${label}${details ? ` (${details})` : ""}`);
}

async function observedState(promise: Promise<unknown>, turns = 4): Promise<"pending" | "fulfilled" | "rejected"> {
  let state: "pending" | "fulfilled" | "rejected" = "pending";
  void promise.then(
    () => { state = "fulfilled"; },
    () => { state = "rejected"; },
  );
  for (let turn = 0; turn < turns; turn += 1) {
    const next = Promise.withResolvers<void>();
    setImmediate(next.resolve);
    await next.promise;
  }
  return state;
}

function sentText(
  messages: readonly { target: { channelId: string; channelType: number }; text: string }[],
  target: { channelId: string; channelType: number },
): string {
  return messages
    .filter(({ target: actual }) =>
      actual.channelId === target.channelId && actual.channelType === target.channelType)
    .map(({ text }) => text)
    .join("\n");
}

test("receive returns before even the permission lookup starts", async () => {
  const harness = await createCoreHarness();
  const lookupStarted = Promise.withResolvers<void>();
  const releaseLookup = Promise.withResolvers<void>();
  let inReceiveCall = false;
  let lookupStartedInline: boolean | undefined;
  harness.transport.getGroupMentionPref = async () => {
    lookupStartedInline = inReceiveCall;
    lookupStarted.resolve();
    await releaseLookup.promise;
    return { effective: true };
  };

  try {
    inReceiveCall = true;
    const receipt = harness.service.receive(inbound({
      channelType: 2,
      channelId: "group-allowed",
      messageId: "ingress-sync-receive",
      text: "permission runs in the owned background operation",
    }));
    inReceiveCall = false;

    expect(receipt.ackReady).toBeInstanceOf(Promise);
    expect(lookupStartedInline).toBeUndefined();
    await lookupStarted.promise;
    expect(lookupStartedInline).toBe(false);

    releaseLookup.resolve();
    await receipt.ackReady;
    await waitForStatus(harness.service, { channelType: 2, channelId: "group-allowed" },
      status => status.state === "completed");
  } finally {
    inReceiveCall = false;
    releaseLookup.resolve();
    await closeCoreHarness(harness);
  }
});

test("a held no-mention lookup does not hold same-chat control or another chat's work", async () => {
  const harness = await createCoreHarness();
  const lookupStarted = Promise.withResolvers<void>();
  const releaseLookup = Promise.withResolvers<void>();
  harness.transport.getGroupMentionPref = async () => {
    lookupStarted.resolve();
    await releaseLookup.promise;
    return { effective: true };
  };
  const group = { channelType: 2 as const, channelId: "group-allowed" };
  const other = { channelType: 1 as const, channelId: "dm-ingress-independent" };

  try {
    const held = harness.service.receive(inbound({
      ...group,
      messageId: "ingress-held-mention-pref",
      text: "held authorization",
    }));
    await lookupStarted.promise;

    const status = harness.service.receive(inbound({
      ...group,
      messageId: "ingress-mentioned-status",
      text: "/status",
      mentionedUserIds: [harness.bot.uid],
    }));
    const ordinary = harness.service.receive(inbound({
      ...other,
      messageId: "ingress-other-chat-task",
      text: "independent chat work",
    }));

    await Promise.all([status.ackReady, ordinary.ackReady]);
    await waitUntil(
      () => sentText(harness.transport.sent, group).includes("state="),
      "same-chat status receipt",
    );
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.prompts[0]?.text).toBe("independent chat work");
    expect(await observedState(held.ackReady)).toBe("pending");

    releaseLookup.resolve();
    await held.ackReady;
    await waitForStatus(harness.service, other, statusValue => statusValue.state === "completed");
  } finally {
    releaseLookup.resolve();
    await closeCoreHarness(harness);
  }
});

test("cancel durably tombstones a pre-authorization candidate before its delayed permission can revive it", async () => {
  const harness = await createCoreHarness();
  const lookupStarted = Promise.withResolvers<void>();
  const releaseLookup = Promise.withResolvers<void>();
  let mentionLookups = 0;
  harness.transport.getGroupMentionPref = async (_groupId, signal) => {
    mentionLookups += 1;
    lookupStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(signal?.reason ?? new Error("candidate authorization cancelled"));
    if (signal?.aborted) rejectAbort();
    else signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseLookup.promise, aborted.promise]);
    return { effective: true };
  };
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  const candidate = inbound({
    ...target,
    messageId: "ingress-preauth-candidate",
    text: "must not revive after cancel",
  });

  try {
    const first = harness.service.receive(candidate);
    await lookupStarted.promise;
    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-preauth-cancel",
      text: "/cancel",
      mentionedUserIds: [harness.bot.uid],
    }));
    await cancel.ackReady;
    await first.ackReady;

    const redelivery = harness.service.receive(candidate);
    await redelivery.ackReady;
    expect(mentionLookups).toBe(1);
    expect(harness.workers.prompts).toHaveLength(0);

    const database = new Database(join(harness.stateDir, "channel.sqlite"), { readonly: true });
    try {
      const row = database.query(`
        SELECT state, payload_json, ack_ready
        FROM ingress_jobs
        WHERE bot_uid = ?1
          AND channel_type = ?2
          AND channel_id = ?3
          AND message_id = ?4
      `).get(harness.bot.uid, candidate.channelType, candidate.channelId, candidate.messageId);
      expect(row).toEqual({ state: "cancelled", payload_json: null, ack_ready: 1 });
    } finally {
      database.close();
    }
  } finally {
    releaseLookup.resolve();
    await closeCoreHarness(harness);
  }
});

test("a held attachment preparation is cancellable without blocking another chat", async () => {
  const harness = await createCoreHarness({
    attachments: { enabled: true, maxBytes: 1024 * 1024 },
  });
  const downloadStarted = Promise.withResolvers<void>();
  const releaseDownload = Promise.withResolvers<void>();
  const originalDownload = harness.transport.downloadAttachment.bind(harness.transport);
  let downloadCount = 0;
  harness.transport.downloadAttachment = async (request) => {
    downloadCount += 1;
    downloadStarted.resolve();
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(new Error("fixture attachment preparation cancelled"));
    if (request.signal?.aborted) rejectAbort();
    else request.signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseDownload.promise, aborted.promise]);
    return originalDownload(request);
  };
  const target = { channelType: 1 as const, channelId: "dm-ingress-attachment" };
  const other = { channelType: 1 as const, channelId: "dm-ingress-attachment-other" };
  const attachmentMessage = inbound({
    ...target,
    messageId: "ingress-held-attachment",
    text: "inspect the attachment",
    attachments: [{
      kind: "file",
      name: "held.txt",
      size: 4,
      durationSeconds: 0,
      url: "https://cdn.example.test/held.txt",
    }],
  });
  harness.transport.attachmentBodies.set("https://cdn.example.test/held.txt", new TextEncoder().encode("held"));

  try {
    const held = harness.service.receive(attachmentMessage);
    await downloadStarted.promise;
    expect(await observedState(held.ackReady)).toBe("pending");

    const cancel = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-cancel-preparing-attachment",
      text: "/cancel",
    }));
    const ordinary = harness.service.receive(inbound({
      ...other,
      messageId: "ingress-attachment-other-task",
      text: "unrelated work while media is held",
    }));

    await Promise.all([cancel.ackReady, ordinary.ackReady]);
    await held.ackReady;
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.prompts.map(({ text }) => text)).toEqual([
      "unrelated work while media is held",
    ]);
    await waitUntil(() => sentText(harness.transport.sent, target).includes("取消"), "cancel result");

    const redelivery = harness.service.receive(attachmentMessage);
    await redelivery.ackReady;
    expect(downloadCount).toBe(1);
    expect(harness.workers.prompts.some(({ text }) => text.includes("inspect the attachment"))).toBe(false);
  } finally {
    releaseDownload.resolve();
    await closeCoreHarness(harness);
  }
});

test("a held new installs a session barrier but not a control or cross-chat barrier", async () => {
  const workers = new FakeWorkerFactory();
  const newCreateStarted = Promise.withResolvers<void>();
  const releaseNewCreate = Promise.withResolvers<void>();
  let createCalls = 0;
  workers.beforeCreate = async () => {
    createCalls += 1;
    if (createCalls !== 1) return;
    newCreateStarted.resolve();
    await releaseNewCreate.promise;
  };
  const harness = await createCoreHarness({ workers });
  const target = { channelType: 1 as const, channelId: "dm-ingress-new" };
  const other = { channelType: 1 as const, channelId: "dm-ingress-new-other" };
  const nextCwd = join(harness.root, "new-session-cwd");
  await mkdir(nextCwd, { recursive: true });

  try {
    const switching = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-held-new",
      text: `/new ${nextCwd}`,
    }));
    await newCreateStarted.promise;
    await switching.ackReady;

    const status = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-status-during-new",
      text: "/status",
    }));
    const crossChat = harness.service.receive(inbound({
      ...other,
      messageId: "ingress-cross-chat-during-new",
      text: "cross-chat work during new",
    }));
    const afterSwitch = harness.service.receive(inbound({
      ...target,
      messageId: "ingress-after-new",
      text: "must use the replacement session",
    }));

    await Promise.all([status.ackReady, crossChat.ackReady, afterSwitch.ackReady]);
    await harness.workers.waitForPromptCount(1);
    expect(harness.workers.prompts[0]?.text).toBe("cross-chat work during new");
    expect(harness.workers.prompts.some(({ text }) => text === "must use the replacement session")).toBe(false);
    await waitUntil(() => sentText(harness.transport.sent, target).includes("state="), "status during new");

    releaseNewCreate.resolve();
    await harness.workers.waitForPromptCount(2);
    const completed = await waitForStatus(harness.service, target,
      statusValue => statusValue.state === "completed" && statusValue.cwd === nextCwd);
    const postSwitchPrompt = harness.workers.prompts.find(({ text }) => text === "must use the replacement session");
    expect(postSwitchPrompt).toMatchObject({ cwd: nextCwd, sessionId: completed.sessionId });
  } finally {
    releaseNewCreate.resolve();
    await closeCoreHarness(harness);
  }
});

test("merge admission blocks later same-chat text before a held worktrunk probe but not control", async () => {
  const harness = await createCoreHarness();
  const mergeSourceId = "ingress-merge-probe-command";
  const thread = { channelType: 5 as const, channelId: "group-allowed____ingress-merge-probe" };
  const workspace = join(harness.root, "merge-probe-workspace");
  await mkdir(workspace, { recursive: true });
  const store = await openChannelStore(harness.stateDir);
  store.ensureBinding({
    botUid: harness.bot.uid,
    ...thread,
    cwd: workspace,
    sessionId: "ingress-merge-probe-session",
    sessionFile: null,
    cwdPinned: true,
  });
  let probeStarted = false;
  const releaseProbe = Promise.withResolvers<void>();
  const port = harness.worktreePort;
  if (!port) throw new Error("expected worktree port");
  port.worktrunk = async (signal) => {
    probeStarted = true;
    const aborted = Promise.withResolvers<never>();
    const rejectAbort = () => aborted.reject(signal?.reason ?? new Error("worktrunk probe aborted"));
    if (signal?.aborted) rejectAbort();
    else signal?.addEventListener("abort", rejectAbort, { once: true });
    await Promise.race([releaseProbe.promise, aborted.promise]);
    return { available: true, command: "wt", version: "fixture" };
  };

  try {
    const merge = harness.service.receive(inbound({
      ...thread,
      messageId: mergeSourceId,
      text: "/thread merge, regression note",
      mentionedUserIds: [harness.bot.uid],
    }));
    await merge.ackReady;
    await waitUntil(() => probeStarted, "held worktrunk probe");

    const later = harness.service.receive(inbound({
      ...thread,
      messageId: "ingress-merge-probe-later",
      text: "must stay behind admitted merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    const status = harness.service.receive(inbound({
      ...thread,
      messageId: "ingress-merge-probe-status",
      text: "/status",
      mentionedUserIds: [harness.bot.uid],
    }));
    await Promise.all([later.ackReady, status.ackReady]);
    const mergeJob = store.getIngressJobBySource({
      botUid: harness.bot.uid,
      sourceKind: "recv",
      sourceId: recvIngressSourceId(thread.channelType, thread.channelId, mergeSourceId),
    });
    expect(mergeJob?.operationCheckpoint).toMatchObject({
      kind: "command",
      phase: "planning",
    });
    await waitUntil(
      () => sentText(harness.transport.sent, thread).includes("state="),
      "control result while worktrunk probe is held",
    );
    for (let turn = 0; turn < 4; turn += 1) {
      const next = Promise.withResolvers<void>();
      setImmediate(next.resolve);
      await next.promise;
    }
    expect(harness.workers.prompts.some(({ text }) => text === "must stay behind admitted merge")).toBe(false);

    releaseProbe.resolve();
    const mergeOutcome = () => store.getHostMergeLeaseBySourceId(
      harness.bot.uid,
      thread,
      mergeSourceId,
    );
    const laterTask = () => store.getTaskByMessageId(harness.bot.uid, thread, "ingress-merge-probe-later");
    // The admitted command owns the session and workspace until the host merge has a durable result.
    // Do not start the queued prompt while the test is still observing that merge transition.
    await waitUntil(
      () => {
        const lease = mergeOutcome();
        return lease?.phase === "exited" && lease.outcome?.kind === "succeeded";
      },
      "merge outcome after the held worktrunk probe",
      () => {
        const lease = mergeOutcome();
        const task = laterTask();
        const job = store.getIngressJobBySource({
          botUid: harness.bot.uid,
          sourceKind: "recv",
          sourceId: recvIngressSourceId(thread.channelType, thread.channelId, mergeSourceId),
        });
        return [
          `worktreeMerges=${harness.worktrees?.merges.length ?? "missing"}`,
          `lease=${lease ? `${lease.phase}/${lease.outcome?.kind ?? "no-outcome"}` : "missing"}`,
          `laterTask=${task?.state ?? "missing"}`,
          `command=${job?.state ?? "missing"}/${job?.operationCheckpoint?.kind === "command" ? job.operationCheckpoint.phase : "no-checkpoint"}`,
          `responses=${JSON.stringify(sentText(harness.transport.sent, thread))}`,
        ].join(", ");
      },
    );
    expect(harness.workers.prompts.some(({ text }) => text === "must stay behind admitted merge")).toBe(false);
    await waitUntil(
      () => harness.workers.prompts.some(({ text }) => text === "must stay behind admitted merge"),
      "later text after merge admission completes",
    );
    // Both same-thread inputs cancel the pending archive generation.
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
  } finally {
    releaseProbe.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});

test("a running host merge is acknowledged without blocking same-thread status or another chat", async () => {
  const harness = await createCoreHarness();
  const thread = { channelType: 5 as const, channelId: "group-allowed____ingress-merge" };
  const other = { channelType: 1 as const, channelId: "dm-ingress-merge-other" };
  const workspace = join(harness.root, "merge-workspace");
  await mkdir(workspace, { recursive: true });
  const store = await openChannelStore(harness.stateDir);
  store.ensureBinding({
    botUid: harness.bot.uid,
    ...thread,
    cwd: workspace,
    sessionId: "ingress-merge-session",
    sessionFile: null,
    cwdPinned: true,
  });
  let mergeStarted = false;
  const releaseMerge = Promise.withResolvers<void>();
  const port = harness.worktreePort;
  if (!port) throw new Error("expected worktree port");
  port.merge = async (request, signal) => {
    request.lifecycle?.beforeSpawn(request.cwd);
    request.lifecycle?.onSpawn({
      pid: 0x7fff_fffe,
      pgid: 0x7fff_fffe,
      identity: "pid=2147483646;start=ingress-fixture",
      cwd: request.cwd,
    });
    mergeStarted = true;
    await releaseMerge.promise;
    request.lifecycle?.onConfirmedExit();
    if (signal?.aborted) throw signal.reason ?? new Error("merge aborted");
    return {
      branch: "ingress-branch",
      target: request.target ?? "main",
      squashed: false,
      rebased: false,
      committed: true,
      removed: request.remove !== false,
    };
  };

  try {
    const merge = harness.service.receive(inbound({
      ...thread,
      messageId: "ingress-held-merge",
      text: "/thread merge",
      mentionedUserIds: [harness.bot.uid],
    }));
    await merge.ackReady;
    await waitUntil(() => mergeStarted, "held host merge process");

    const status = harness.service.receive(inbound({
      ...thread,
      messageId: "ingress-status-during-merge",
      text: "/status",
      mentionedUserIds: [harness.bot.uid],
    }));
    const ordinary = harness.service.receive(inbound({
      ...other,
      messageId: "ingress-other-during-merge",
      text: "ordinary work during merge",
    }));

    await Promise.all([status.ackReady, ordinary.ackReady]);
    await waitUntil(
      () => harness.workers.prompts.length >= 1,
      "other-chat worker during held merge",
    );
    expect(harness.workers.prompts[0]?.text).toBe("ordinary work during merge");
    await waitUntil(() => sentText(harness.transport.sent, thread).includes("state="), "status during merge");

    releaseMerge.resolve();
    await waitUntil(() => {
      const lease = store.getHostMergeLeaseBySourceId(
        harness.bot.uid,
        thread,
        "ingress-held-merge",
      );
      return lease?.phase === "exited" && lease.outcome?.kind === "succeeded";
    }, "host merge outcome after release");
    // The same-thread status input cancels archival even though the merge succeeds.
    expect(harness.transport.managementCalls.filter(({ action }) => action === "thread-archive")).toEqual([]);
  } finally {
    releaseMerge.resolve();
    store.close();
    await closeCoreHarness(harness);
  }
});
