import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatTarget } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import type { ThreadReference } from "../src/references/types.ts";

const botUid = "reference-store-bot";
const target: ChatTarget = { channelType: 5, channelId: "references-parent____review" };
const github: ThreadReference = { kind: "pr", url: "https://github.com/example/service/pull/17" };
const gitlab: ThreadReference = { kind: "mr", url: "https://gitlab.example/team/service/-/merge_requests/42" };

function tempRoot(): Promise<string> {
  return mkdtemp(join(tmpdir(), "oco-thread-references-"));
}

test("persists sorted references per bot and real thread and merges repeated additions", async () => {
  const root = await tempRoot();
  const otherThread: ChatTarget = { channelType: 5, channelId: "references-parent____other" };
  let store = await openChannelStore(root);
  try {
    store.addThreadReferences(botUid, target, [github, gitlab, github]);
    store.addThreadReferences(botUid, target, [{ kind: "pr", url: "https://github.com/example/api/pull/2" }]);
    store.addThreadReferences("another-bot", target, [gitlab]);
    store.addThreadReferences(botUid, otherThread, [github]);

    expect(store.getThreadReferences(botUid, target)).toEqual({
      mr: [gitlab.url],
      pr: ["https://github.com/example/api/pull/2", github.url],
    });
    expect(store.getThreadReferences("another-bot", target)).toEqual({ mr: [gitlab.url], pr: [] });
    expect(store.getThreadReferences(botUid, otherThread)).toEqual({ mr: [], pr: [github.url] });
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.getThreadReferences(botUid, target)).toEqual({
      mr: [gitlab.url],
      pr: ["https://github.com/example/api/pull/2", github.url],
    });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("deletion tombstone atomically clears references and permanently closes additions across reopen", async () => {
  const root = await tempRoot();
  let store = await openChannelStore(root);
  try {
    store.addThreadReferences(botUid, target, [github, gitlab]);
    expect(store.isThreadReferencesDeleted(botUid, target)).toBe(false);
    store.deleteThreadReferences(botUid, target);
    expect(store.getThreadReferences(botUid, target)).toEqual({ mr: [], pr: [] });
    expect(store.isThreadReferencesDeleted(botUid, target)).toBe(true);
    store.addThreadReferences(botUid, target, [github]);
    store.deleteThreadReferences(botUid, target);
    expect(store.getThreadReferences(botUid, target)).toEqual({ mr: [], pr: [] });
  } finally {
    store.close();
  }

  try {
    store = await openChannelStore(root);
    expect(store.isThreadReferencesDeleted(botUid, target)).toBe(true);
    store.addThreadReferences(botUid, target, [gitlab]);
    expect(store.getThreadReferences(botUid, target)).toEqual({ mr: [], pr: [] });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("opening a pre-reference database creates empty reference tables without changing old rows", async () => {
  const root = await tempRoot();
  const path = join(root, "channel.sqlite");
  let store = await openChannelStore(root);
  store.setBotUid("legacy-bot");
  store.setOwnerId("legacy-bot", "owner-kept");
  store.close();

  const legacyDb = new Database(path);
  legacyDb.exec("DROP TABLE thread_references; DROP TABLE thread_reference_tombstones;");
  legacyDb.close();

  store = await openChannelStore(root);
  try {
    expect(store.getBotUid()).toBe("legacy-bot");
    expect(store.getOwnerId("legacy-bot")).toBe("owner-kept");
    expect(store.getThreadReferences("legacy-bot", target)).toEqual({ mr: [], pr: [] });
    expect(store.isThreadReferencesDeleted("legacy-bot", target)).toBe(false);
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
});
