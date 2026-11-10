import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openChannelStore } from "../src/store/index.ts";
import type { IngressJobInput } from "../src/inbound/ingress-types.ts";
import type { ThreadWorkspaceOperationInput } from "../src/store/store-records.ts";

function workspaceOperation(
  root: string,
  channelId: string,
  id: string,
): ThreadWorkspaceOperationInput {
  const botUid = "cancel-workspace-bot";
  const workspaceRoot = join(root, `workspace-${id}`);
  const entryPath = join(workspaceRoot, "scope");
  const expectedWorkspace = {
    botUid,
    channelType: 5 as const,
    channelId,
    layout: "single" as const,
    status: "ready" as const,
    entryPath: null,
    workspaceRoot: null,
    projects: [{ name: "existing", repo: join(root, "existing-repo"), path: join(root, "existing-worktree"), branch: "existing", created: true }],
    error: null,
  };
  const expectedBinding = {
    botUid,
    channelType: 5 as const,
    channelId,
    sessionId: "session-1",
    cwd: join(root, "existing-worktree"),
    sessionFile: join(root, "session.json"),
    cwdPinned: true,
    sessionSwitchedAt: null,
  };
  return {
    id,
    botUid,
    channelType: 5,
    channelId,
    groupId: "cancel-workspace-group",
    toolCallId: id,
    taskId: `task-${id}`,
    worktreeNameSeed: id,
    projects: [{ name: "added", repo: join(root, "added-repo") }],
    expectedWorkspace,
    expectedBinding,
    candidateWorkspace: {
      ...expectedWorkspace,
      layout: "shared",
      status: "preparing",
      entryPath,
      workspaceRoot,
      projects: [
        ...expectedWorkspace.projects,
        { name: "added", repo: join(root, "added-repo"), path: join(entryPath, "added"), branch: null, created: false },
      ],
    },
    candidateBinding: expectedBinding,
  };
}

function cancelInput(channelId: string, messageId: string): IngressJobInput {
  const message = {
    channelType: 5 as const,
    channelId,
    messageId,
    senderId: "owner-1",
    text: "/cancel",
    mentionedUserIds: ["cancel-workspace-bot"],
    attachments: [],
    quoted: null,
    supported: true,
    messageSeq: 1,
    messageTimestamp: 1_800_000_000_000,
  };
  return {
    botUid: "cancel-workspace-bot",
    sourceKind: "recv",
    sourceId: JSON.stringify([5, channelId, messageId]),
    channelType: 5,
    channelId,
    messageId,
    payload: { kind: "recv", message },
    routeSnapshot: {
      purpose: { kind: "command", action: "cancel" },
      sessionGeneration: 0,
      binding: null,
      barrierJobId: null,
    },
    lane: "control",
    state: "accepted",
    ackReady: true,
    operationCheckpoint: {
      kind: "command",
      action: "cancel",
      argument: "",
      phase: "admitted",
      expectedBinding: null,
    },
  };
}

test("accepting /cancel durably requests cancellation of queued and running workspace additions for its exact target", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-ingress-workspace-cancel-"));
  try {
    const store = await openChannelStore(root);
    try {
      const target = "cancel-workspace-group____thread-1";
      const queued = store.acceptThreadWorkspaceOperation(workspaceOperation(root, target, "queued-add")).operation;
      const failed = store.updateThreadWorkspaceOperation({ ...queued, status: "failed" });
      expect(failed).not.toBeNull();
      const running = store.acceptThreadWorkspaceOperation(workspaceOperation(root, target, "running-add")).operation;
      const started = store.updateThreadWorkspaceOperation({ ...running, status: "running" });
      expect(started).not.toBeNull();
      const queuedAgain = store.updateThreadWorkspaceOperation({ ...(failed ?? queued), status: "queued" });
      expect(queuedAgain).not.toBeNull();

      const otherTarget = "cancel-workspace-group____thread-2";
      const unrelated = store.acceptThreadWorkspaceOperation(
        workspaceOperation(root, otherTarget, "unrelated-add"),
      ).operation;
      const accepted = store.acceptIngressJob(cancelInput(target, "cancel-additions"));

      expect(accepted.job.operationCheckpoint).toMatchObject({
        kind: "command",
        action: "cancel",
        phase: "cancel-intent",
        cancelRequestedWorkspaceOperationIds: [queued.id, running.id],
      });
      expect(store.getThreadWorkspaceOperation("cancel-workspace-bot", queued.id)?.cancelRequested).toBe(true);
      expect(store.getThreadWorkspaceOperation("cancel-workspace-bot", running.id)?.cancelRequested).toBe(true);
      expect(store.getThreadWorkspaceOperation("cancel-workspace-bot", unrelated.id)?.cancelRequested).toBe(false);
    } finally {
      store.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
