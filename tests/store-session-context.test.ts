import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { SessionContextSnapshotRecord } from "../src/store/store-records.ts";

function sampleSnapshot(
  botUid: string,
  target: ChatTarget,
  overrides: Partial<SessionContextSnapshotRecord> = {},
): SessionContextSnapshotRecord {
  return {
    botUid,
    ...target,
    sessionId: "session-alpha",
    sessionFile: "/sessions/session-alpha.jsonl",
    cwd: "/workspaces/alpha",
    usedTokens: 4_096,
    contextWindow: 8_192,
    percent: 50,
    model: "fixture/context",
    observedAt: "2026-11-04T09:00:00.000Z",
    fileSize: 128,
    fileMtimeMs: 1_794_092_400_000,
    ...overrides,
  };
}

test("context snapshots persist the latest live sample per bot and chat across reopen", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-session-context-store-"));
  const target: ChatTarget = { channelType: 5, channelId: "group-context____topic-1" };
  const otherTarget: ChatTarget = { channelType: 5, channelId: "group-context____topic-2" };
  const first = sampleSnapshot("bot-context-a", target);
  const latest = sampleSnapshot("bot-context-a", target, {
    usedTokens: 5_120,
    percent: 62.5,
    observedAt: "2026-11-04T09:05:00.000Z",
    fileSize: 192,
    fileMtimeMs: 1_794_092_700_000,
  });
  const delayedOlder = sampleSnapshot("bot-context-a", target, {
    usedTokens: 3_072,
    percent: 37.5,
    observedAt: "2026-11-04T09:04:59.000Z",
    fileSize: 160,
    fileMtimeMs: 1_794_092_699_000,
  });
  let store = await openChannelStore(root);
  try {
    store.saveSessionContextSnapshot(first);
    store.saveSessionContextSnapshot(latest);
    store.saveSessionContextSnapshot(delayedOlder);
    store.saveSessionContextSnapshot(sampleSnapshot("bot-context-b", target, { usedTokens: 2_048 }));
    store.saveSessionContextSnapshot(sampleSnapshot("bot-context-a", otherTarget, {
      usedTokens: 1_024,
      contextWindow: null,
      percent: null,
    }));
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getSessionContextSnapshot("bot-context-a", target)).toEqual(latest);

    expect(store.getSessionContextSnapshot("bot-context-b", target)).toEqual(
      sampleSnapshot("bot-context-b", target, { usedTokens: 2_048 }),
    );
    expect(store.getSessionContextSnapshot("bot-context-a", otherTarget)).toEqual(
      sampleSnapshot("bot-context-a", otherTarget, {
        usedTokens: 1_024,
        contextWindow: null,
        percent: null,
      }),
    );
    expect(store.getSessionContextSnapshot("bot-context-b", otherTarget)).toBeNull();
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("a chat's latest snapshot retains exact session file and cwd identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-session-context-identity-"));
  const store = await openChannelStore(root);
  const target: ChatTarget = { channelType: 1, channelId: "dm-context-identity" };
  try {
    const prior = sampleSnapshot("bot-context-identity", target);
    store.saveSessionContextSnapshot(prior);
    const next = sampleSnapshot("bot-context-identity", target, {
      sessionId: "session-beta",
      sessionFile: "/sessions/session-beta.jsonl",
      cwd: "/workspaces/beta",
      observedAt: "2026-11-04T10:00:00.000Z",
    });
    store.saveSessionContextSnapshot(next);

    expect(store.getSessionContextSnapshot("bot-context-identity", target)).toEqual(next);
    expect(store.getSessionContextSnapshot("bot-context-identity", target)).not.toEqual(prior);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
