import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, watch } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, basename } from "node:path";
import { OctoError } from "../src/octo/error.ts";
import { TurnContext } from "../src/session/turn-context.ts";
import { contextSidecarPath, freshContextReadPaths, readContextSidecar, writeFreshContextReadFile } from "../src/session/context-sidecar.ts";
import { readGroupContext } from "../src/session/read-group-context.ts";
import { openChannelStore } from "../src/store/index.ts";

test.each(["recover", "exhaust"] as const)("compaction bridge %s keeps its exact-session snapshot", async mode => {
  const root = await mkdtemp(join(tmpdir(), "context-retry-"));
  const store = await openChannelStore(root);
  const target = { channelType: 5 as const, channelId: "parent____child" };
  const old = { present: true, content: "old", version: 1, updatedAt: null, updatedBy: "owner" };
  const fresh = { ...old, content: "fresh", version: 2 };
  let calls = 0;
  const context = new TurnContext({
    stateDir: root, store, botUid: "bot", target, cliReady: false,
    transport: { getGroupContext: async chat => {
      expect(chat).toEqual(target);
      calls++;
      if (calls === 2 || (mode === "exhaust" && calls > 2)) throw new OctoError("temporary", { status: 503 });
      return calls === 1 ? old : fresh;
    } },
  });
  const controller = new AbortController();
  try {
    await context.load("session-1", controller.signal);
    await context.prepare("session-1", { cliReady: false });
    const sidecar = await readContextSidecar(contextSidecarPath(context.workerScope.sidecarRoot, "session-1"));
    const requestId = randomUUID();
    const identity = { schemaVersion: 1, requestId, botUid: "bot", target, sessionId: "session-1", turnId: sidecar.turnId! };
    const paths = freshContextReadPaths(context.workerScope.sidecarRoot, identity.turnId, requestId);
    await mkdir(paths.directory, { recursive: true });
    const bridge = context.serveFreshContextReads(controller.signal);
    const events = watch(paths.directory, { signal: controller.signal });
    const response = (async () => {
      for await (const event of events) {
        if (event.filename?.toString() === basename(paths.response)) return JSON.parse(await readFile(paths.response, "utf8"));
      }
    })();
    await writeFreshContextReadFile(paths.request, identity);
    if (mode === "recover") {
      expect(await response).toEqual({ ...identity, snapshot: fresh });
      expect(calls).toBe(3);
    } else {
      expect(await response).toEqual({ ...identity, error: "fresh group context read failed" });
      expect(calls).toBe(4);
    }
    expect((await readContextSidecar(contextSidecarPath(context.workerScope.sidecarRoot, "session-1"))).snapshot).toEqual(old);
    controller.abort();
    await bridge;
  } finally {
    controller.abort();
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);

test("cancelling an in-flight context read rejects even if the transport ignores cancellation", async () => {
  const target = { channelType: 2 as const, channelId: "cancel-group" };
  const started = Promise.withResolvers<void>();
  const pending = Promise.withResolvers<null>();
  const controller = new AbortController();
  let calls = 0;
  const result = readGroupContext({
    getGroupContext: async () => {
      calls++;
      started.resolve();
      return pending.promise;
    },
  }, target, controller.signal);
  await started.promise;
  controller.abort(new Error("cancelled"));
  await expect(result).rejects.toThrow("cancelled");
  pending.resolve(null);
  expect(calls).toBe(1);
});
