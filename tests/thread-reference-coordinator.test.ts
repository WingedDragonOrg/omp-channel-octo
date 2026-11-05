import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { ThreadReferenceCoordinator } from "../src/references/thread-references.ts";
import type { ReferenceEvidence, ReferenceJudgment, ThreadReference } from "../src/references/types.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";

const botUid = "reference-coordinator-bot";
const url = "https://github.com/acme/service/pull/17";

function target(shortId: string): ChatTarget {
  return { channelType: 5, channelId: `parent____${shortId}` };
}

function work(evidence: ReferenceEvidence, probability = 0.99): ReferenceJudgment[] {
  return evidence.candidates.map(({ id }) => ({ id, choice: "work_object" as const, probability }));
}

function holdJudge() {
  const calls: Array<{ evidence: ReferenceEvidence; signal: AbortSignal }> = [];
  const pending: Array<{ promise: Promise<readonly ReferenceJudgment[]>; resolve(value: readonly ReferenceJudgment[] | PromiseLike<readonly ReferenceJudgment[]>): void; reject(reason?: unknown): void }> = [];
  const judge = (evidence: ReferenceEvidence, signal: AbortSignal) => {
    calls.push({ evidence, signal });
    const result = Promise.withResolvers<readonly ReferenceJudgment[]>();
    pending.push(result);
    return result.promise;
  };
  return { calls, pending, judge };
}

function trackedStore(store: ChannelStore) {
  const writes: Array<{ botUid: string; target: ChatTarget; references: readonly ThreadReference[] }> = [];
  const signals: Array<{ promise: Promise<void>; resolve(value: void | PromiseLike<void>): void; reject(reason?: unknown): void }> = [];
  const api = {
    getThreadReferences: (uid: string, chat: ChatTarget) => store.getThreadReferences(uid, chat),
    addThreadReferences: (uid: string, chat: ChatTarget, references: readonly ThreadReference[]) => {
      store.addThreadReferences(uid, chat, references);
      writes.push({ botUid: uid, target: chat, references });
      signals.shift()?.resolve(undefined);
    },
    isThreadReferencesDeleted: (uid: string, chat: ChatTarget) => store.isThreadReferencesDeleted(uid, chat),
    deleteThreadReferences: (uid: string, chat: ChatTarget) => store.deleteThreadReferences(uid, chat),
  };
  return {
    api,
    writes,
    waitForWrite: () => {
      if (writes.length > 0) return Promise.resolve();
      const signal = Promise.withResolvers<void>();
      signals.push(signal);
      return signal.promise;
    },
  };
}

async function flushBackgroundWork(): Promise<void> {
  const completion = Promise.withResolvers<void>();
  setImmediate(() => completion.resolve(undefined));
  await completion.promise;
}

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "oco-reference-coordinator-"));
  const store = await openChannelStore(root);
  return { root, store };
}

test("captures role-preserving sanitized snapshots and does not deduplicate separate messages by URL", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store: tracked.api, botUid: () => botUid, judge: held.judge });
  try {
    coordinator.setThreadContext(target("context"), "Please review the Service component", "Service review");
    coordinator.observeUser(target("context"), "user-1", `> Please review ${url}?token=never-send#diff`, "task-1");
    coordinator.observeUser(target("context"), "user-2", "Also add regression coverage for the same service change.", "task-1");
    coordinator.observeAssistant(target("context"), "task-1", "assistant-1", `The same PR remains the direct work object: ${url}/files`);
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(2);
    expect(held.calls[0]?.evidence.recent.map(({ role }) => role)).toEqual(["user"]);
    expect(held.calls[0]?.evidence.thread).toContain("Service review");
    expect(JSON.stringify(held.calls[0]?.evidence)).not.toContain("never-send");
    expect(held.calls[0]?.evidence.recent[0]?.text).toContain("> Please review");
    expect(held.calls[1]?.evidence.recent.map(({ role }) => role)).toEqual(["user", "user", "assistant"]);
    expect(held.calls[0]?.evidence.task).not.toContain("regression coverage");
    expect(held.calls[1]?.evidence.task).toContain("regression coverage");

    held.pending[0]?.resolve([{ id: held.calls[0]!.evidence.candidates[0]!.id, choice: "insufficient_evidence", probability: 0.99 }]);
    held.pending[1]?.resolve(work(held.calls[1]!.evidence));
    await tracked.waitForWrite();
    expect(store.getThreadReferences(botUid, target("context"))).toEqual({ mr: [], pr: [url] });
    expect(tracked.writes).toHaveLength(1);
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("changed assistant final invalidates the older in-flight result for that message version", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store: tracked.api, botUid: () => botUid, judge: held.judge });
  try {
    coordinator.observeAssistant(target("versions"), "task-1", "assistant-final", `This PR is a reference example: ${url}`);
    coordinator.observeAssistant(target("versions"), "task-1", "assistant-final", `This PR is a reference example: ${url}`);
    await flushBackgroundWork();
    coordinator.observeAssistant(target("versions"), "task-1", "assistant-final", `Please review and fix the current service using ${url}`);
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(2);
    expect(held.calls[0]?.signal.aborted).toBe(true);
    held.pending[0]?.resolve(work(held.calls[0]!.evidence));
    held.pending[1]?.resolve([{ id: held.calls[1]!.evidence.candidates[0]!.id, choice: "insufficient_evidence", probability: 1 }]);
    await flushBackgroundWork();

    expect(store.getThreadReferences(botUid, target("versions"))).toEqual({ mr: [], pr: [] });
    expect(tracked.writes).toEqual([]);
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("keeps same local assistant message ids separate across distinct tasks", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const chat = target("assistant-task-identity");
  const firstUrl = "https://github.com/acme/service/pull/31";
  const secondUrl = "https://github.com/acme/service/pull/32";
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  try {
    coordinator.observeAssistant(chat, "task-one", "assistant-1", `Task one used ${firstUrl}`);
    coordinator.observeAssistant(chat, "task-two", "assistant-1", `Task two used ${secondUrl}`);
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(2);
    expect(held.calls[0]?.signal.aborted).toBe(false);
    expect(held.calls[1]?.signal.aborted).toBe(false);
    expect(held.calls[1]?.evidence.recent.map(({ text }) => text)).toEqual([
      `Task one used ${firstUrl}`,
      `Task two used ${secondUrl}`,
    ]);

    held.pending[0]?.resolve(work(held.calls[0]!.evidence));
    held.pending[1]?.resolve(work(held.calls[1]!.evidence));
    await flushBackgroundWork();
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [firstUrl, secondUrl] });
  } finally {
    await coordinator.stop();
    for (const pending of held.pending) pending.resolve([]);
    await flushBackgroundWork();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("sanitizes full URLs before context and excerpt limits", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  const chat = target("url-boundary-sanitization");
  const priorUrl = `https://example.test/${"x".repeat(600)}?tenant=private-account-42`;
  const ftpUrl = "ftp://example.test/report?tenant=314159#private-section";
  try {
    coordinator.observeUser(
      chat,
      "user-message",
      `${priorUrl} ${ftpUrl} Review candidate ${url}`,
      "task-1",
    );
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(1);
    const evidence = held.calls[0]!.evidence;
    const wire = JSON.stringify(evidence);
    expect(wire).not.toContain("private-account-42");
    expect(wire).not.toContain("314159");
    expect(wire).not.toContain("private-section");
    expect(evidence.candidates[0]?.excerpt).toContain("[URL omitted]");
    expect(evidence.task).toContain("[URL omitted]");
    expect(evidence.recent[0]?.text).toContain("[URL omitted]");
  } finally {
    await coordinator.stop();
    for (const pending of held.pending) pending.resolve([]);
    await flushBackgroundWork();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("does not classify slash-prefixed assistant finals as user commands", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  const chat = target("slash-prefixed-assistant");
  try {
    coordinator.observeAssistant(chat, "task-1", "assistant-final", `/workspace 已完成，PR: ${url}`);
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(1);
    held.pending[0]?.resolve(work(held.calls[0]!.evidence));
    await flushBackgroundWork();
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [url] });
  } finally {
    await coordinator.stop();
    for (const pending of held.pending) pending.resolve([]);
    await flushBackgroundWork();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("binds direct work above 0.75 but excludes the boundary and unrelated links", async () => {
  const { root, store } = await fixture();
  const chat = target("threshold");
  const candidateUrls = [
    "https://github.com/acme/service/pull/10",
    "https://github.com/acme/service/pull/11",
    "https://gitlab.example/acme/drive/-/merge_requests/75",
    "https://gitlab.example/acme/docs/-/merge_requests/187",
    "https://gitlab.example/acme/web/drive/-/merge_requests/62",
    "https://github.com/acme/service/pull/12",
  ];
  const probabilities = [0.75, 0.7501, 0.81, 0.84, 0.89, 0.99];
  const coordinator = new ThreadReferenceCoordinator({
    store,
    botUid: () => botUid,
    judge: async evidence => evidence.candidates.map((candidate, index) => ({
      id: candidate.id,
      choice: index === 5 ? "reference_or_unrelated" : "work_object",
      confidence: 0.99,
      probability: probabilities[index]!,
      url: "https://attacker.example/other/pull/999",
    } as ReferenceJudgment)),
  });
  try {
    coordinator.observeUser(chat, "candidates", `Review ${candidateUrls.join("\n")}`, "task-1");
    await flushBackgroundWork();
    expect(store.getThreadReferences(botUid, chat)).toEqual({
      mr: [
        "https://gitlab.example/acme/docs/-/merge_requests/187",
        "https://gitlab.example/acme/drive/-/merge_requests/75",
        "https://gitlab.example/acme/web/drive/-/merge_requests/62",
      ],
      pr: ["https://github.com/acme/service/pull/11"],
    });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("bounds global workload to two active and one hundred waiting batches", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store: tracked.api, botUid: () => botUid, judge: held.judge });
  try {
    for (let index = 0; index < 103; index += 1) {
      const chat = target(`bounded-${index}`);
      coordinator.observeUser(chat, `message-${index}`, `Please review https://github.com/acme/service/pull/${index + 1}`, `task-${index}`);
    }
    await flushBackgroundWork();
    expect(held.calls).toHaveLength(2);
    expect(held.calls.map(({ evidence }) => evidence.candidates[0]?.url)).toEqual([
      "https://github.com/acme/service/pull/1",
      "https://github.com/acme/service/pull/2",
    ]);

    held.pending[0]?.resolve([]);
    await flushBackgroundWork();
    expect(held.calls).toHaveLength(3);
    expect(held.calls[2]?.evidence.candidates[0]?.url).toBe("https://github.com/acme/service/pull/3");
    let resolved = 1;
    while (resolved < held.pending.length) {
      const currentLimit = held.pending.length;
      while (resolved < currentLimit) held.pending[resolved++]!.resolve([]);
      await flushBackgroundWork();
    }
    expect(held.calls).toHaveLength(102);
  } finally {
    await coordinator.stop();
    for (const pending of held.pending) pending.resolve([]);
    await flushBackgroundWork();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("deleting a thread tombstones it before late results and prevents observations after reopen", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const held = holdJudge();
  const chat = target("deleted");
  const coordinator = new ThreadReferenceCoordinator({ store: tracked.api, botUid: () => botUid, judge: held.judge });
  let storeClosed = false;
  try {
    coordinator.observeAssistant(chat, "task-1", "final-before-delete", `Please review ${url}`);
    await flushBackgroundWork();
    coordinator.deleteThread(chat);
    expect(store.isThreadReferencesDeleted(botUid, chat)).toBe(true);
    held.pending[0]?.resolve(work(held.calls[0]!.evidence));
    await flushBackgroundWork();
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [] });
    expect(held.calls[0]?.signal.aborted).toBe(true);
    store.close();
    storeClosed = true;

    const reopened = await openChannelStore(root);
    const afterRestart = new ThreadReferenceCoordinator({
      store: reopened,
      botUid: () => botUid,
      judge: async () => { throw new Error("deleted threads must not dispatch"); },
    });
    try {
      afterRestart.observeAssistant(chat, "old-task", "late-after-restart", `Please review ${url}`);
      expect(reopened.isThreadReferencesDeleted(botUid, chat)).toBe(true);
      expect(reopened.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [] });
    } finally {
      await afterRestart.stop();
      reopened.close();
    }
  } finally {
    await coordinator.stop();
    if (!storeClosed) store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("stopping prevents an uncooperative late judge from touching a closed store", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store: tracked.api, botUid: () => botUid, judge: held.judge });
  let storeClosed = false;
  try {
    coordinator.observeAssistant(target("stopped"), "task-1", "final", `Please review ${url}`);
    await flushBackgroundWork();
    await coordinator.stop();
    expect(held.calls[0]?.signal.aborted).toBe(true);
    store.close();
    storeClosed = true;
    held.pending[0]?.resolve(work(held.calls[0]!.evidence));
    await flushBackgroundWork();
    expect(tracked.writes).toEqual([]);

    const reopened = await openChannelStore(root);
    try {
      expect(reopened.getThreadReferences(botUid, target("stopped"))).toEqual({ mr: [], pr: [] });
    } finally {
      reopened.close();
    }
  } finally {
    await coordinator.stop();
    if (!storeClosed) store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("disabled capture and context reset never clear persisted references or recover old context", async () => {
  const { root, store } = await fixture();
  const chat = target("reset");
  store.addThreadReferences(botUid, chat, [{ kind: "pr", url }]);
  const disabled = new ThreadReferenceCoordinator({ store, botUid: () => botUid });
  disabled.setThreadContext(chat, "must not be captured", "disabled name");
  disabled.observeUser(chat, "disabled-message", `Review ${url}`, "disabled-task");
  await disabled.stop();

  const captured: ReferenceEvidence[] = [];
  const enabled = new ThreadReferenceCoordinator({
    store,
    botUid: () => botUid,
    judge: async (evidence) => { captured.push(evidence); return []; },
  });
  try {
    enabled.observeUser(chat, "new-context", "Review https://github.com/acme/service/pull/18", "new-task");
    await flushBackgroundWork();
    expect(captured[0]?.thread).not.toContain("disabled name");
    expect(captured[0]?.task).not.toContain("disabled-task");

    enabled.setThreadContext(chat, "old thread description", "Old name");
    enabled.observeUser(chat, "old", "The initial accepted requirement", "old-task");
    enabled.resetContext(chat);
    enabled.observeUser(chat, "after-reset", "Please review https://github.com/acme/service/pull/19", "newer-task");
    await flushBackgroundWork();
    expect(captured).toHaveLength(2);
    expect(captured[1]?.task).not.toContain("initial accepted requirement");
    expect(captured[1]?.recent.map(({ text }) => text)).toEqual([
      "Please review https://github.com/acme/service/pull/19",
    ]);
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [url] });
  } finally {
    await enabled.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});


test("limits task, thread, recent and candidate evidence in Unicode code points", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  const chat = target("limits");
  try {
    coordinator.setThreadContext(chat, "界".repeat(700), "界".repeat(700));
    coordinator.observeUser(chat, "large-input", `${"界".repeat(5000)} Please review ${url}`, "task-large");
    await flushBackgroundWork();

    const evidence = held.calls[0]!.evidence;
    expect([...evidence.thread].length).toBeLessThanOrEqual(1000);
    expect(evidence.thread).toContain("[truncated]");
    expect([...evidence.task].length).toBeLessThanOrEqual(3000);
    expect(evidence.task).toContain("[truncated]");
    expect(evidence.recent.reduce((total, message) => total + [...message.text].length, 0)).toBeLessThanOrEqual(4000);
    expect(evidence.recent[0]?.text).toContain("[truncated]");
    expect([...evidence.candidates[0]!.url].length).toBeLessThanOrEqual(2048);
    expect([...evidence.candidates[0]!.excerpt].length).toBeLessThanOrEqual(1000);
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("omits non-finite and out-of-range probabilities without discarding valid candidates", async () => {
  const { root, store } = await fixture();
  const tracked = trackedStore(store);
  const coordinator = new ThreadReferenceCoordinator({
    store: tracked.api,
    botUid: () => botUid,
    judge: async (evidence) => evidence.candidates.map((candidate, index) => ({
      id: candidate.id,
      choice: "work_object" as const,
      probability: [Number.NaN, 1.01, -0.01, Number.POSITIVE_INFINITY, 0.95][index]!,
    })),
  });
  const candidateUrls = Array.from({ length: 5 }, (_, index) => `https://github.com/acme/service/pull/${index + 30}`);
  try {
    coordinator.observeUser(target("probability"), "multi-candidate", `Review ${candidateUrls.join(" and ")}`, "task-1");
    await tracked.waitForWrite();
    expect(store.getThreadReferences(botUid, target("probability"))).toEqual({
      mr: [],
      pr: [candidateUrls[4]!],
    });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("filters saved references before applying the per-message candidate limit", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const chat = target("candidate-filter");
  const boundUrl = "https://github.com/acme/service/pull/1";
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  try {
    store.addThreadReferences(botUid, chat, [{ kind: "pr", url: boundUrl }]);
    const links = Array.from({ length: 10 }, (_, index) => `https://github.com/acme/service/pull/${index + 1}`);
    coordinator.observeUser(chat, "links", `Review ${links.join(" and ")}`, "task-1");
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(1);
    const candidates = held.calls[0]!.evidence.candidates;
    expect(candidates.map(({ url }) => url)).toEqual(links.slice(1, 9));
    expect(candidates[7]?.excerpt).toContain("[additional candidate links omitted]");
    held.pending[0]?.resolve([]);
    await flushBackgroundWork();
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [boundUrl] });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("keeps accepted requirements isolated by task when an older assistant final arrives later", async () => {
  const { root, store } = await fixture();
  const held = holdJudge();
  const chat = target("task-isolation");
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid, judge: held.judge });
  try {
    coordinator.observeUser(chat, "task-one", `Original requirements for task one: ${url}`, "task-one");
    coordinator.observeUser(chat, "task-two", "Separate requirements for task two: https://github.com/acme/service/pull/18", "task-two");
    coordinator.observeAssistant(chat, "task-one", "task-one-result", "Task one completed using https://github.com/acme/service/pull/19");
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(2);
    held.pending[0]?.resolve([]);
    held.pending[1]?.resolve([]);
    await flushBackgroundWork();

    expect(held.calls).toHaveLength(3);
    expect(held.calls[2]?.evidence.task).toContain("Original requirements for task one");
    expect(held.calls[2]?.evidence.task).not.toContain("Separate requirements for task two");
    expect(held.calls[2]?.evidence.task).not.toContain("task-one");
  } finally {
    await coordinator.stop();
    for (const pending of held.pending) pending.resolve([]);
    await flushBackgroundWork();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("tombstones a deleted thread even when external decisions are disabled", async () => {
  const { root, store } = await fixture();
  const chat = target("deleted-disabled");
  const coordinator = new ThreadReferenceCoordinator({ store, botUid: () => botUid });
  try {
    store.addThreadReferences(botUid, chat, [{ kind: "pr", url }]);
    coordinator.deleteThread(chat);
    expect(store.isThreadReferencesDeleted(botUid, chat)).toBe(true);
    expect(store.getThreadReferences(botUid, chat)).toEqual({ mr: [], pr: [] });
  } finally {
    await coordinator.stop();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("isolates passive store failures but surfaces thread deletion failures", async () => {
  const failure = new Error("store unavailable");
  const brokenStore = {
    getThreadReferences: () => { throw failure; },
    addThreadReferences: () => { throw failure; },
    isThreadReferencesDeleted: () => { throw failure; },
    deleteThreadReferences: () => { throw failure; },
  };
  const coordinator = new ThreadReferenceCoordinator({
    store: brokenStore,
    botUid: () => botUid,
    judge: async () => [],
  });
  const chat = target("store-failure");

  expect(() => coordinator.observeUser(chat, "user-message", `Review ${url}`, "task")).not.toThrow();
  expect(() => coordinator.observeAssistant(chat, "task", "assistant-final", `Review ${url}`)).not.toThrow();
  expect(() => coordinator.setThreadContext(chat, "Thread description")).not.toThrow();
  expect(() => coordinator.deleteThread(chat)).toThrow(failure);
  await coordinator.stop();
});
