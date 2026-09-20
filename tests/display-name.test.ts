import { expect, test } from "bun:test";
import { createDisplayNameCache } from "../src/display-name.ts";

/** A lookup double: answers from `names`, fails unregistered uids, records every call. */
function lookupDouble(names: Record<string, string> = {}) {
  const calls: string[] = [];
  const lookup = async (uid: string, signal?: AbortSignal): Promise<string> => {
    calls.push(uid);
    if (signal?.aborted) throw new Error("aborted");
    return names[uid] ?? "";
  };
  return { calls, lookup };
}

test("resolves each uid once and reuses the cached name", async () => {
  const { calls, lookup } = lookupDouble({ "owner-1": "董振兴", "member-9": "小李" });
  const cache = createDisplayNameCache(lookup);

  await expect(cache.names(["owner-1", "member-9", "owner-1", " ", ""])).resolves.toEqual(
    new Map([["owner-1", "董振兴"], ["member-9", "小李"]]),
  );
  await expect(cache.names(["owner-1", "member-9"])).resolves.toEqual(
    new Map([["owner-1", "董振兴"], ["member-9", "小李"]]),
  );
  expect(calls).toEqual(["owner-1", "member-9"]);
});

test("honours the TTL and forgets names past it", async () => {
  const { calls, lookup } = lookupDouble({ "owner-1": "董振兴" });
  const cache = createDisplayNameCache(lookup, { ttlMs: 0 });

  await cache.names(["owner-1"]);
  await cache.names(["owner-1"]);
  expect(calls).toEqual(["owner-1", "owner-1"]);
});

test("a failed or empty lookup yields no entry and is retried", async () => {
  let failing = true;
  const calls: string[] = [];
  const cache = createDisplayNameCache(async (uid) => {
    calls.push(uid);
    if (failing) throw new Error("server down");
    return "董振兴";
  });

  await expect(cache.names(["owner-1"])).resolves.toEqual(new Map());
  failing = false;
  await expect(cache.names(["owner-1"])).resolves.toEqual(new Map([["owner-1", "董振兴"]]));
  expect(calls).toEqual(["owner-1", "owner-1"]);

  const blank = createDisplayNameCache(async () => "  ");
  await expect(blank.names(["owner-1"])).resolves.toEqual(new Map());
});

test("shares one in-flight lookup per uid and never rejects", async () => {
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const calls: string[] = [];
  const cache = createDisplayNameCache(async (uid) => {
    calls.push(uid);
    await gate;
    return "董振兴";
  });

  const first = cache.names(["owner-1", "owner-1"]);
  const second = cache.names(["owner-1"]);
  release?.();
  await expect(first).resolves.toEqual(new Map([["owner-1", "董振兴"]]));
  await expect(second).resolves.toEqual(new Map([["owner-1", "董振兴"]]));
  expect(calls).toEqual(["owner-1"]);

  let available = false;
  const hostile = createDisplayNameCache(() => {
    if (!available) throw new Error("sync failure");
    return Promise.resolve("Recovered");
  });
  await expect(hostile.names(["owner-1"])).resolves.toEqual(new Map());
  available = true;
  await expect(hostile.names(["owner-1"])).resolves.toEqual(new Map([["owner-1", "Recovered"]]));
});

test("settles within the lookup budget even when the lookup ignores cancellation", async () => {
  const cache = createDisplayNameCache(
    () => new Promise<string>(() => {}),
    { lookupTimeoutMs: 20 },
  );
  const started = Date.now();
  await expect(cache.names(["owner-1"])).resolves.toEqual(new Map());
  expect(Date.now() - started).toBeLessThan(1_000);
});

test("bounds the cache by evicting the oldest entry", async () => {
  const { calls, lookup } = lookupDouble({ a: "A", b: "B" });
  const cache = createDisplayNameCache(lookup, { maxEntries: 1 });

  await cache.names(["a"]);
  await cache.names(["b"]);
  await cache.names(["a"]);
  expect(calls).toEqual(["a", "b", "a"]);
});
