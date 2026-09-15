import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import contextExtension, { contextSidecarFileName } from "../src/omp/context-extension.ts";

type Target = {
  channelType: number;
  channelId: string;
};

type Snapshot = {
  content: string;
  present: boolean;
  version: number;
};

type InjectionDecision = "initial" | "version-change" | "unchanged";

type Marker = {
  phase: "pending" | "committed";
  botUid: string;
  target: Target;
  sessionId: string;
  version: number;
  attemptId: string;
};

type Sidecar = {
  schemaVersion: number;
  botUid: string;
  target: Target;
  sessionId: string;
  snapshot: Snapshot;
  injectionDecision: InjectionDecision;
  attemptId: string;
  marker: Marker;
  previousVersion?: number;
};

type Handler = (event: unknown, context: unknown) => unknown | Promise<unknown>;
type Invocation = { value: unknown; error: unknown | undefined };

type ContextResult = {
  systemPrompt?: unknown;
};

type CompactingResult = {
  context?: unknown;
  preserveData?: unknown;
};

const BOT_UID = "bot-context-test";
const TARGET: Target = { channelType: 2, channelId: "group-context-test" };
const SESSION_ID = "omp-session-real-context-test";
const ENVIRONMENT_KEYS = [
  "OMP_CHANNEL_OCTO_CONTEXT_ROOT",
  "OMP_CHANNEL_OCTO_CONTEXT_BOT_UID",
  "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE",
  "OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID",
] as const;

type EnvironmentKey = (typeof ENVIRONMENT_KEYS)[number];

function saveEnvironment(): Map<EnvironmentKey, string | undefined> {
  return new Map(ENVIRONMENT_KEYS.map((key) => [key, process.env[key]]));
}

function restoreEnvironment(previous: Map<EnvironmentKey, string | undefined>): void {
  for (const key of ENVIRONMENT_KEYS) {
    const value = previous.get(key);
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
}

function sidecarFor(
  injectionDecision: InjectionDecision,
  snapshot: Snapshot,
  markerPhase: Marker["phase"],
  attemptId: string,
  previousVersion?: number,
): Sidecar {
  const marker: Marker = {
    phase: markerPhase,
    botUid: BOT_UID,
    target: TARGET,
    sessionId: SESSION_ID,
    version: snapshot.version,
    attemptId,
  };
  return {
    schemaVersion: 1,
    botUid: BOT_UID,
    target: TARGET,
    sessionId: SESSION_ID,
    snapshot,
    injectionDecision,
    attemptId,
    marker,
    ...(previousVersion === undefined ? {} : { previousVersion }),
  };
}

async function withSidecar<T>(
  sidecar: Sidecar,
  run: (root: string, sidecarPath: string) => Promise<T>,
): Promise<T> {
  const previous = saveEnvironment();
  const root = await mkdtemp(join(tmpdir(), "omp-context-extension-"));
  const sidecarPath = join(root, contextSidecarFileName(sidecar.sessionId));
  await writeFile(sidecarPath, JSON.stringify(sidecar), "utf8");

  try {
    process.env.OMP_CHANNEL_OCTO_CONTEXT_ROOT = root;
    process.env.OMP_CHANNEL_OCTO_CONTEXT_BOT_UID = sidecar.botUid;
    process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE = String(sidecar.target.channelType);
    process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID = sidecar.target.channelId;
    return await run(root, sidecarPath);
  } finally {
    restoreEnvironment(previous);
    await rm(root, { recursive: true, force: true });
  }
}

async function readSidecar(sidecarPath: string): Promise<Sidecar & Record<string, unknown>> {
  return JSON.parse(await readFile(sidecarPath, "utf8")) as Sidecar & Record<string, unknown>;
}

function captureHandlers(): { beforeAgentStart: Handler; sessionCompacting: Handler } {
  const handlers = new Map<string, Handler>();
  const api = {
    on(event: string, handler: Handler): void {
      handlers.set(event, handler);
    },
  };
  contextExtension(api as unknown as Parameters<typeof contextExtension>[0]);

  const beforeAgentStart = handlers.get("before_agent_start");
  const sessionCompacting = handlers.get("session.compacting");
  expect(beforeAgentStart).toEqual(expect.any(Function));
  expect(sessionCompacting).toEqual(expect.any(Function));
  if (beforeAgentStart === undefined || sessionCompacting === undefined) {
    throw new Error("context extension did not register both lifecycle handlers");
  }
  return { beforeAgentStart, sessionCompacting };
}

function extensionContext(sessionId: string, abort: () => void): Record<string, unknown> {
  return {
    sessionManager: { getSessionId: () => sessionId },
    abort,
  };
}

async function invoke(handler: Handler, event: unknown, context: unknown): Promise<Invocation> {
  try {
    return { value: await handler(event, context), error: undefined };
  } catch (error) {
    return { value: undefined, error };
  }
}

function returnedSystemPrompt(value: unknown): string[] {
  const result = value as ContextResult | undefined;
  return Array.isArray(result?.systemPrompt) && result.systemPrompt.every((item) => typeof item === "string")
    ? (result.systemPrompt as string[])
    : [];
}

function returnedCompactionContext(value: unknown): string[] {
  const result = value as CompactingResult | undefined;
  return Array.isArray(result?.context) && result.context.every((item) => typeof item === "string")
    ? (result.context as string[])
    : [];
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

test("injects initial and version-change snapshots at system level and records matching prepared receipts", async () => {
  const initialSnapshot: Snapshot = {
    content: "Initial group instructions must be complete.",
    present: true,
    version: 4,
  };
  await withSidecar(
    sidecarFor("initial", initialSnapshot, "pending", "attempt-initial"),
    async (_root, sidecarPath) => {
      const { beforeAgentStart } = captureHandlers();
      const originalSystemPrompt = ["base system rule", "keep this system instruction"];
      const event = {
        type: "before_agent_start",
        prompt: "first group turn",
        systemPrompt: [...originalSystemPrompt],
      };
      const abort = () => {
        throw new Error("initial context unexpectedly aborted");
      };

      const initialResult = await beforeAgentStart(event, extensionContext(SESSION_ID, abort));
      const initialPrompt = returnedSystemPrompt(initialResult);
      expect(initialPrompt.slice(0, originalSystemPrompt.length)).toEqual(originalSystemPrompt);
      expect(initialPrompt.at(-1)).toContain(initialSnapshot.content);
      expect(initialPrompt.at(-1)).toContain(String(initialSnapshot.version));
      expect(event.systemPrompt).toEqual(originalSystemPrompt);

      const initialWritten = await readSidecar(sidecarPath);
      expect(initialWritten.marker).toMatchObject({ phase: "pending", attemptId: "attempt-initial" });
      const initialPrepared = recordValue(initialWritten.prepared);
      expect(initialPrepared).toEqual(
        expect.objectContaining({
          attemptId: "attempt-initial",
          botUid: BOT_UID,
          sessionId: SESSION_ID,
          version: initialSnapshot.version,
          target: TARGET,
        }),
      );
    },
  );

  const changedSnapshot: Snapshot = {
    content: "Latest group instructions replace the previous snapshot.",
    present: true,
    version: 9,
  };
  await withSidecar(
    sidecarFor("version-change", changedSnapshot, "pending", "attempt-version-change", 4),
    async (_root, sidecarPath) => {
      const { beforeAgentStart } = captureHandlers();
      const originalSystemPrompt = ["base system rule"];
      const event = {
        type: "before_agent_start",
        prompt: "group turn after update",
        systemPrompt: [...originalSystemPrompt],
      };
      const result = await beforeAgentStart(
        event,
        extensionContext(SESSION_ID, () => {
          throw new Error("version-change context unexpectedly aborted");
        }),
      );
      const systemPrompt = returnedSystemPrompt(result);
      expect(systemPrompt.slice(0, originalSystemPrompt.length)).toEqual(originalSystemPrompt);
      expect(systemPrompt.at(-1)).toContain(changedSnapshot.content);
      expect(systemPrompt.at(-1)).toContain(String(changedSnapshot.version));
      expect(systemPrompt.at(-1)).toMatch(/version|版本/i);

      const written = await readSidecar(sidecarPath);
      const prepared = recordValue(written.prepared);
      expect(prepared).toEqual(
        expect.objectContaining({
          attemptId: "attempt-version-change",
          botUid: BOT_UID,
          sessionId: SESSION_ID,
          version: changedSnapshot.version,
          target: TARGET,
        }),
      );
      expect(written.marker).toMatchObject({ phase: "pending", version: changedSnapshot.version });
    },
  );
});

test("does not return shared context for an unchanged ordinary turn", async () => {
  const snapshot: Snapshot = {
    content: "This unchanged snapshot must stay out of the ordinary prompt.",
    present: true,
    version: 12,
  };
  await withSidecar(
    sidecarFor("unchanged", snapshot, "committed", "attempt-committed"),
    async (_root, sidecarPath) => {
      const { beforeAgentStart } = captureHandlers();
      const originalSystemPrompt = ["base system rule"];
      const event = {
        type: "before_agent_start",
        prompt: "ordinary unchanged turn",
        systemPrompt: [...originalSystemPrompt],
      };
      const result = await beforeAgentStart(
        event,
        extensionContext(SESSION_ID, () => {
          throw new Error("unchanged context unexpectedly aborted");
        }),
      );

      expect(returnedSystemPrompt(result)).not.toContain(snapshot.content);
      expect(returnedSystemPrompt(result)).toEqual(originalSystemPrompt);
      expect(event.systemPrompt).toEqual(originalSystemPrompt);
      const written = await readSidecar(sidecarPath);
      expect(written.marker).toMatchObject({ phase: "committed", version: snapshot.version });
      expect(written.prepared).toBeUndefined();
    },
  );
});

test("restores the immutable current snapshot during compaction without changing the committed marker", async () => {
  const snapshot: Snapshot = {
    content: "The immutable group snapshot must survive compaction.",
    present: true,
    version: 12,
  };
  await withSidecar(
    sidecarFor("unchanged", snapshot, "committed", "attempt-committed"),
    async (_root, sidecarPath) => {
      const { sessionCompacting } = captureHandlers();
      const before = await readSidecar(sidecarPath);
      const result = await sessionCompacting(
        { type: "session.compacting", sessionId: SESSION_ID, messages: [] },
        extensionContext(SESSION_ID, () => {
          throw new Error("compaction context unexpectedly aborted");
        }),
      );
      const context = returnedCompactionContext(result);
      expect(context).toHaveLength(1);
      expect(context[0]).toContain(snapshot.content);
      expect(context[0]).toContain(String(snapshot.version));
      const preserveData = recordValue((result as CompactingResult | undefined)?.preserveData);
      expect(preserveData).toEqual(expect.objectContaining({ version: snapshot.version, target: TARGET }));

      const after = await readSidecar(sidecarPath);
      expect(after.snapshot).toEqual(before.snapshot);
      expect(after.marker).toEqual(before.marker);
      expect(after.prepared).toBeUndefined();
    },
  );
});

test("fails closed on session, bot, or target mismatches without returning shared context", async () => {
  const snapshot: Snapshot = {
    content: "Secret shared context must never cross its identity boundary.",
    present: true,
    version: 21,
  };
  const mismatches: Array<{
    name: string;
    sessionId: string;
    botUid: string;
    target: Target;
  }> = [
    { name: "session", sessionId: "wrong-session", botUid: BOT_UID, target: TARGET },
    { name: "bot", sessionId: SESSION_ID, botUid: "wrong-bot", target: TARGET },
    { name: "target", sessionId: SESSION_ID, botUid: BOT_UID, target: { channelType: 5, channelId: "wrong-target" } },
  ];

  for (const mismatch of mismatches) {
    await withSidecar(
      sidecarFor("initial", snapshot, "pending", `attempt-${mismatch.name}`),
      async (_root, sidecarPath) => {
        process.env.OMP_CHANNEL_OCTO_CONTEXT_BOT_UID = mismatch.botUid;
        process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE = String(mismatch.target.channelType);
        process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID = mismatch.target.channelId;
        const { beforeAgentStart, sessionCompacting } = captureHandlers();
        const originalSystemPrompt = ["base system rule"];
        let abortCalls = 0;
        const context = extensionContext(mismatch.sessionId, () => {
          abortCalls += 1;
        });

        const beforeAbortCalls = abortCalls;
        const beforeOutcome = await invoke(
          beforeAgentStart,
          {
            type: "before_agent_start",
            prompt: `mismatched ${mismatch.name} ordinary turn`,
            systemPrompt: [...originalSystemPrompt],
          },
          context,
        );
        expect(beforeOutcome.error).toBeDefined();
        expect(abortCalls).toBe(beforeAbortCalls + 1);

        const compactAbortCalls = abortCalls;
        const compactOutcome = await invoke(
          sessionCompacting,
          { type: "session.compacting", sessionId: mismatch.sessionId, messages: [] },
          context,
        );
        expect(compactOutcome.error).toBeDefined();
        expect(abortCalls).toBe(compactAbortCalls + 1);

        const written = await readSidecar(sidecarPath);
        expect(recordValue(written).prepared).toBeUndefined();
      },
    );
  }
});
