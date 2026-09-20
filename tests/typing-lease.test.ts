import { expect, test, vi } from "bun:test";
import { createTypingLeaseManager } from "../src/typing-lease.ts";

test("shutdown stops renewals before consulting task state again", async () => {
  vi.useFakeTimers();
  let stopping = false;
  let requests = 0;
  let readsAfterStop = 0;
  const operations: Promise<void>[] = [];
  const leases = createTypingLeaseManager({
    sendTyping: async () => { requests++; },
    trackOperation: (operation) => { operations.push(operation); },
    taskState: () => {
      if (stopping) readsAfterStop++;
      return "active";
    },
    stopping: () => stopping,
  });
  try {
    leases.acquire({ channelType: 1, channelId: "shutdown-chat" }, "task:active");
    await Promise.all(operations);
    stopping = true;
    vi.advanceTimersByTime(9_000);
    await Promise.all(operations);
    expect(requests).toBe(1);
    expect(readsAfterStop).toBe(0);
  } finally {
    leases.releaseAll();
    vi.useRealTimers();
  }
});
