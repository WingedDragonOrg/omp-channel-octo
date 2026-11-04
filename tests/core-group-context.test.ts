import { expect, test } from "bun:test";
import type { LocalCommandEvent } from "../src/contracts";
import {
  contextSidecarPath,
  readContextSidecar,
  writeContextSidecarFile,
} from "../src/session/context-sidecar";
import {
  assistantText,
  closeCoreHarness,
  createCoreHarness,
  FakeWorkerFactory,
  inbound,
  waitForStatus,
  type CoreHarness,
} from "./fixtures/core-fakes";


/**
 * One no-agent local command completion as `LocalCommandEvent` carries it: the command
 * (e.g. `/autoresearch` enable/off/clear) ran inside OMP without an agent turn, so this
 * prompt produces no group-context prepared receipt.
 */
const localCommandEvent = (): LocalCommandEvent => ({ type: "local_command" });

/**
 * Simulates a worker whose prompted turn never honored the shared-context injection: the
 * fixture worker writes the prepared receipt for every injected prompt, so the test removes
 * exactly the receipt from the sidecar through the same sidecar API the service uses — no
 * production toggle.
 */
async function scrubPreparedReceipt(sidecarRoot: string, sessionId: string): Promise<void> {
  const path = contextSidecarPath(sidecarRoot, sessionId);
  const sidecar = await readContextSidecar(path);
  const { prepared: _prepared, ...stripped } = sidecar;
  await writeContextSidecarFile(path, stripped);
}

test("keeps a session's loaded shared context until a new session is created", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-a", channelType: 2 as const };
    harness.transport.setGroupContext(group, {
      content: "Group A instructions",
      version: 3,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin-a",
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "group-context-first",
        ...group,
        text: "first group turn",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    const firstPrompt = harness.workers.prompts[0];
    expect(firstPrompt?.text).toBe("first group turn");

    // The scope publishes the session it drives before prompting and keeps it
    // as the current session; OMP opens sub-sessions in the same scope.
    const scopeRoot = firstPrompt?.sidecarRoot;
    expect(scopeRoot).toEqual(expect.any(String));
    expect(firstPrompt?.boundSessionId).toBe(firstPrompt?.sessionId);

    await harness.service.handleInbound(
      inbound({
        messageId: "group-context-unchanged",
        ...group,
        text: "second group turn",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    const unchangedPrompt = harness.workers.prompts[1];
    expect(unchangedPrompt?.sessionId).toBe(firstPrompt?.sessionId);
    expect(unchangedPrompt?.text).toBe("second group turn");
    expect(unchangedPrompt?.text).not.toContain("Group A instructions");
    expect(unchangedPrompt?.text).not.toContain('"groupContext"');

    harness.transport.setGroupContext(group, {
      content: "Group A updated instructions",
      version: 4,
      updatedAt: "2026-09-12T00:02:00.000Z",
      updatedBy: "admin-a",
    });
    await harness.service.handleInbound(
      inbound({
        messageId: "group-context-changed",
        ...group,
        text: "third group turn",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    const changedPrompt = harness.workers.prompts[2];
    expect(changedPrompt?.sessionId).toBe(firstPrompt?.sessionId);
    expect(changedPrompt?.text).toBe("third group turn");
    expect(harness.transport.groupContextCalls).toEqual([group]);

    await harness.service.handleInbound(
      inbound({
        messageId: "group-context-new-session",
        ...group,
        text: "/new",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const newSessionId = (await harness.service.status(group)).sessionId;
    if (!newSessionId) throw new Error("new group session was not created");
    expect(newSessionId).not.toBe(firstPrompt?.sessionId);

    harness.transport.setGroupContext(group, {
      content: "Group A latest instructions",
      version: 5,
      updatedAt: "2026-09-12T00:03:00.000Z",
      updatedBy: "admin-a",
    });
    await harness.service.handleInbound(
      inbound({
        messageId: "group-context-fresh-session",
        ...group,
        text: "first turn in new session",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(4);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    const freshSessionPrompt = harness.workers.prompts[3];
    expect(freshSessionPrompt?.sessionId).toBe(newSessionId);
    expect(freshSessionPrompt?.text).toBe("first turn in new session");
    expect(harness.transport.groupContextCalls).toEqual([group, group]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("loads group context once per session and keeps private chats isolated", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-fresh", channelType: 2 as const };
    const privateChat = { channelId: "dm-private", channelType: 1 as const };
    harness.transport.setGroupContext(group, {
      content: "shared group note",
      version: 1,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin",
    });
    harness.transport.setGroupContext(privateChat, {
      content: "must never be injected",
      version: 1,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin",
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "group-transport-first",
        ...group,
        text: "group transport first",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(1);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({
        messageId: "group-transport-second",
        ...group,
        text: "group transport second",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await harness.workers.waitForPromptCount(2);
    await waitForStatus(harness.service, group, (status) => status.state === "completed");

    await harness.service.handleInbound(
      inbound({ messageId: "private-isolated", ...privateChat, text: "private task" }),
    );
    await harness.workers.waitForPromptCount(3);
    await waitForStatus(
      harness.service,
      privateChat,
      (status) => status.state === "completed",
    );

    const privatePrompt = harness.workers.prompts.find(({ text }) => text === "private task");
    expect(privatePrompt?.text).toBe("private task");
    expect(privatePrompt?.text).not.toContain("must never be injected");
    expect(privatePrompt?.text).not.toContain("群说明是群共享上下文");
    expect(harness.transport.groupContextCalls).toEqual([group]);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a recovered group session does not repeat a committed unchanged snapshot", async () => {
  const first = await createCoreHarness();
  const group = { channelId: "group-b", channelType: 2 as const };
  let recovered: CoreHarness | undefined;
  let sessionId: string | undefined;
  try {
    first.transport.setGroupContext(group, {
      content: "persistent group instructions",
      version: 8,
      updatedAt: "2026-09-12T00:00:00.000Z",
      updatedBy: "admin-b",
    });

    await first.service.handleInbound(
      inbound({
        messageId: "group-recovery-first",
        ...group,
        text: "before service recovery",
        mentionedUserIds: [first.bot.uid],
      }),
    );
    await first.workers.waitForPromptCount(1);
    await waitForStatus(first.service, group, (status) => status.state === "completed");

    const firstPrompt = first.workers.prompts[0];
    if (!firstPrompt) throw new Error("first recovery prompt was not recorded");
    expect(firstPrompt.text).toBe("before service recovery");
    sessionId = firstPrompt.sessionId;
    expect(sessionId).toBeDefined();

    await closeCoreHarness(first, { remove: false });
    recovered = await createCoreHarness({
      root: first.root,
      bot: first.bot,
      transport: first.transport,
    });
    const restoredBinding = await recovered.service.status(group);
    expect(restoredBinding.sessionId).toBe(sessionId);

    await recovered.service.handleInbound(
      inbound({
        messageId: "group-recovery-second",
        ...group,
        text: "after service recovery",
        mentionedUserIds: [recovered.bot.uid],
      }),
    );
    await recovered.workers.waitForPromptCount(1);
    await waitForStatus(recovered.service, group, (status) => status.state === "completed");

    const recoveredPrompt = recovered.workers.prompts[0];
    expect(recoveredPrompt?.sessionId).toBe(sessionId);
    expect(recoveredPrompt?.text).toBe("after service recovery");
    expect(recoveredPrompt?.text).not.toContain("persistent group instructions");
    expect(first.transport.groupContextCalls).toEqual([group]);
  } finally {
    if (recovered) {
      await closeCoreHarness(recovered);
    } else {
      await closeCoreHarness(first);
    }
  }
});

test("a group-context read failure fails the task before any worker prompt", async () => {
  const harness = await createCoreHarness();
  try {
    const group = { channelId: "group-unavailable", channelType: 2 as const };
    harness.transport.failGroupContextWith(new Error("fixture context unavailable"));
    await harness.service.handleInbound(
      inbound({
        messageId: "group-read-failed",
        ...group,
        text: "must not execute",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    const status = await waitForStatus(
      harness.service,
      group,
      (next) => next.state === "failed",
    );
    expect(status.state).toBe("failed");
    expect(harness.workers.prompts).toHaveLength(0);
    expect(harness.transport.sent.some(({ text }) => /失败|说明|读取/.test(text))).toBe(true);
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a local-only command completes without a prepared receipt and leaves the context ledger for the next model turn", async () => {
  const workers = new FakeWorkerFactory(async ({ emit, sessionId, text }) => {
    const scopeRoot = workers.prompts.at(-1)?.sidecarRoot;
    if (text === "/autoresearch") {
      // Upstream no-agent completion (`prompt_result agentInvoked:false`): the local command
      // never drove an agent turn, so no prepared receipt exists for this prompt.
      emit(localCommandEvent());
      if (scopeRoot) await scrubPreparedReceipt(scopeRoot, sessionId);
      return;
    }
    emit({ type: "turn_start", turnId: "model-turn", atMs: 1 });
    emit(assistantText("assistant-model-turn", "model body", true));
    emit({ type: "turn_end", turnId: "model-turn", atMs: 2 });
  });
  const harness = await createCoreHarness({ workers });
  try {
    const group = { channelId: "group-a", channelType: 2 as const };
    harness.transport.setGroupContext(group, {
      content: "shared group instructions",
      version: 2,
      updatedAt: "2026-09-20T00:00:00.000Z",
      updatedBy: "admin",
    });

    await harness.service.handleInbound(
      inbound({
        messageId: "local-only-autoresearch",
        ...group,
        text: "/autoresearch",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await workers.waitForPromptCount(1);
    const localStatus = await waitForStatus(
      harness.service,
      group,
      (status) => status.state === "completed" || status.state === "failed",
    );
    expect(localStatus.state).toBe("completed");
    expect(workers.prompts[0]?.text).toBe("/autoresearch");

    await harness.service.handleInbound(
      inbound({
        messageId: "after-local-only",
        ...group,
        text: "model turn after local command",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await workers.waitForPromptCount(2);
    const modelStatus = await waitForStatus(
      harness.service,
      group,
      (status) => status.state === "completed" || status.state === "failed",
    );
    expect(modelStatus.state).toBe("completed");

    // The local-only prompt must not commit the ledger: the next generation still sees the
    // first injection of this version and commits it with its own real receipt.
    expect(workers.prompts[1]?.text).toBe("model turn after local command");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("a model turn that produced real events still requires its own prepared receipt", async () => {
  const workers = new FakeWorkerFactory(async ({ emit, sessionId }) => {
    const scopeRoot = workers.prompts.at(-1)?.sidecarRoot;
    // A genuine agent turn must own the receipt; scrubbing means this worker never honored it.
    if (scopeRoot) await scrubPreparedReceipt(scopeRoot, sessionId);
    emit({ type: "turn_start", turnId: "receipt-required-turn", atMs: 1 });
    emit(assistantText("assistant-receipt-required", "model body", true));
    emit({ type: "turn_end", turnId: "receipt-required-turn", atMs: 2 });
  });
  const harness = await createCoreHarness({ workers });
  try {
    const group = { channelId: "group-b", channelType: 2 as const };
    harness.transport.setGroupContext(group, {
      content: "shared instructions",
      version: 4,
      updatedAt: "2026-09-20T00:00:00.000Z",
      updatedBy: "admin",
    });
    await harness.service.handleInbound(
      inbound({
        messageId: "receipt-required-model-turn",
        ...group,
        text: "model turn with ignored receipt",
        mentionedUserIds: [harness.bot.uid],
      }),
    );
    await workers.waitForPromptCount(1);
    const status = await waitForStatus(
      harness.service,
      group,
      (next) => next.state === "completed" || next.state === "failed",
    );
    expect(status.state).toBe("failed");
    // The failure is the receipt guard, not an injection-less prompt.
    expect(workers.prompts[0]?.text).toBe("model turn with ignored receipt");
  } finally {
    await closeCoreHarness(harness);
  }
});
