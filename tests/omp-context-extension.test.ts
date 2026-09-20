import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import contextExtension, { contextSidecarFileName } from "../src/omp/context-extension.ts";
import { boundSessionPath } from "../src/context-sidecar.ts";

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
/** A subagent session OMP opened inside the same worker process. */
const SUBAGENT_SESSION_ID = "01a0aa43-a96d-704e-be11-ba2e50ab559b";
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

type ScopeOptions = {
  /** Bot identity the worker environment carries; defaults to the sidecar's. */
  botUid?: string;
  /** Target the worker environment carries; defaults to the sidecar's. */
  target?: Target;
  /** Session the host publishes as bound; `null` publishes none. */
  boundSessionId?: string | null;
};

async function withSidecar<T>(
  sidecar: Sidecar,
  run: (root: string, sidecarPath: string) => Promise<T>,
  scope: ScopeOptions = {},
): Promise<T> {
  const previous = saveEnvironment();
  const botUid = scope.botUid ?? sidecar.botUid;
  const target = scope.target ?? sidecar.target;
  const boundSessionId = scope.boundSessionId === undefined ? sidecar.sessionId : scope.boundSessionId;
  const root = await mkdtemp(join(tmpdir(), "omp-context-extension-"));
  const sidecarPath = join(root, contextSidecarFileName(boundSessionId ?? sidecar.sessionId));
  if (boundSessionId !== null) {
    await writeFile(
      boundSessionPath(root),
      JSON.stringify({ botUid, target, sessionId: boundSessionId }),
      "utf8",
    );
  }
  await writeFile(sidecarPath, JSON.stringify(sidecar), "utf8");

  try {
    process.env.OMP_CHANNEL_OCTO_CONTEXT_ROOT = root;
    process.env.OMP_CHANNEL_OCTO_CONTEXT_BOT_UID = botUid;
    process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE = String(target.channelType);
    process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID = target.channelId;
    return await run(root, sidecarPath);
  } finally {
    restoreEnvironment(previous);
    await rm(root, { recursive: true, force: true });
  }
}

async function readSidecar(sidecarPath: string): Promise<Sidecar & Record<string, unknown>> {
  return JSON.parse(await readFile(sidecarPath, "utf8")) as Sidecar & Record<string, unknown>;
}

function captureHandlers(): { beforeAgentStart: Handler; sessionCompacting: Handler; modelContext?: Handler } {
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
  const modelContext = handlers.get("context");
  return { beforeAgentStart, sessionCompacting, ...(modelContext ? { modelContext } : {}) };
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

test("a native continuation injects pending shared context on every model call and prepares its receipt", async () => {
  const sidecar = sidecarFor("version-change", {content: "Current retry instructions", present: true, version: 9}, "pending", "retry-attempt", 8);
  await withSidecar(sidecar, async (_root, path) => {
    const { modelContext } = captureHandlers();
    const ctx = extensionContext(SESSION_ID, () => { throw new Error("unexpected abort"); });
    const messages = [{role: "user", content: "original failed request", timestamp: 1}];
    for (let call = 0; call < 2; call++) {
      const result = recordValue(await modelContext?.({type: "context", messages}, ctx));
      const received = result.messages as Array<Record<string, unknown>> | undefined;
      expect(received?.slice(0, -1)).toEqual(messages);
      expect(received?.at(-1)).toMatchObject({role: "developer", content: expect.stringContaining("Current retry instructions")});
      expect(recordValue((await readSidecar(path)).prepared)).toMatchObject({attemptId: "retry-attempt", sessionId: SESSION_ID, version: 9});
    }
    expect(messages).toEqual([{role: "user", content: "original failed request", timestamp: 1}]);
    expect(await modelContext?.({type: "context", messages}, extensionContext(SUBAGENT_SESSION_ID, () => {}))).toBeUndefined();
  });
});

test("the model context hook does not duplicate a snapshot already injected by before_agent_start", async () => {
  await withSidecar(sidecarFor("initial", {content: "Shared rules", present: true, version: 1}, "pending", "normal-attempt"), async () => {
    const { beforeAgentStart, modelContext } = captureHandlers();
    const ctx = extensionContext(SESSION_ID, () => {});
    await beforeAgentStart({systemPrompt: ["base"]}, ctx);
    expect(await modelContext?.({type: "context", messages: []}, ctx)).toBeUndefined();
  });
});

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

test("fails closed on a defective sidecar for the bound session without returning shared context", async () => {
  const snapshot: Snapshot = {
    content: "Secret shared context must never cross its identity boundary.",
    present: true,
    version: 21,
  };
  const defects: Array<{
    name: string;
    /** Sidecar identity that contradicts the scope the host published. */
    stored?: Partial<Sidecar>;
    /** Drop the sidecar file after the host published the turn. */
    drop?: boolean;
    /** Bot identity the worker environment carries. */
    scopeBotUid?: string;
    scopeTarget?: Target;
  }> = [
    { name: "missing-sidecar", drop: true },
    { name: "bot", stored: { botUid: "wrong-bot" } },
    { name: "target", stored: { target: { channelType: 5, channelId: "wrong-target" } } },
    { name: "session", stored: { sessionId: "wrong-session" } },
    { name: "scope", scopeBotUid: "wrong-scope-bot", scopeTarget: { channelType: 2, channelId: "wrong-scope-target" } },
  ];

  for (const defect of defects) {
    await withSidecar(
      { ...sidecarFor("initial", snapshot, "pending", `attempt-${defect.name}`), ...defect.stored },
      async (_root, sidecarPath) => {
        if (defect.drop) await rm(sidecarPath, { force: true });
        if (defect.scopeBotUid !== undefined) process.env.OMP_CHANNEL_OCTO_CONTEXT_BOT_UID = defect.scopeBotUid;
        if (defect.scopeTarget !== undefined) {
          process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_TYPE = String(defect.scopeTarget.channelType);
          process.env.OMP_CHANNEL_OCTO_CONTEXT_CHANNEL_ID = defect.scopeTarget.channelId;
        }
        const { beforeAgentStart, sessionCompacting } = captureHandlers();
        const originalSystemPrompt = ["base system rule"];
        let abortCalls = 0;
        const context = extensionContext(SESSION_ID, () => {
          abortCalls += 1;
        });

        const beforeAbortCalls = abortCalls;
        const beforeOutcome = await invoke(
          beforeAgentStart,
          {
            type: "before_agent_start",
            prompt: `defective ${defect.name} ordinary turn`,
            systemPrompt: [...originalSystemPrompt],
          },
          context,
        );
        expect(beforeOutcome.error).toBeDefined();
        expect(returnedSystemPrompt(beforeOutcome.value)).toEqual([]);
        expect(abortCalls).toBe(beforeAbortCalls + 1);

        const compactAbortCalls = abortCalls;
        const compactOutcome = await invoke(
          sessionCompacting,
          { type: "session.compacting", sessionId: SESSION_ID, messages: [] },
          context,
        );
        expect(compactOutcome.error).toBeDefined();
        expect(abortCalls).toBe(compactAbortCalls + 1);

        if (defect.drop) {
          expect(await readFile(sidecarPath, "utf8").catch(() => undefined)).toBeUndefined();
        } else {
          expect(recordValue(await readSidecar(sidecarPath)).prepared).toBeUndefined();
        }
      },
      { botUid: BOT_UID, target: TARGET, boundSessionId: SESSION_ID },
    );
  }
});

test("leaves a session the scope does not govern untouched instead of failing closed", async () => {
  // OMP runs subagents in their own session inside the worker process, so their
  // turns reach the extension while the host is driving the parent session.
  const snapshot: Snapshot = {
    content: "Group instructions belong to the channel turn only.",
    present: true,
    version: 33,
  };
  await withSidecar(
    sidecarFor("initial", snapshot, "pending", "attempt-subagent"),
    async (root, sidecarPath) => {
      const bound = JSON.parse(await readFile(boundSessionPath(root), "utf8")) as Record<string, unknown>;
      expect(bound).toEqual({ botUid: BOT_UID, target: TARGET, sessionId: SESSION_ID });

      const { beforeAgentStart, sessionCompacting } = captureHandlers();
      let abortCalls = 0;
      const context = extensionContext(SUBAGENT_SESSION_ID, () => {
        abortCalls += 1;
      });
      const subagentSystemPrompt = ["subagent system rule"];
      const event = {
        type: "before_agent_start",
        prompt: "subagent probe",
        systemPrompt: [...subagentSystemPrompt],
      };

      const outcome = await invoke(beforeAgentStart, event, context);
      expect(outcome.error).toBeUndefined();
      expect(outcome.value).toBeUndefined();
      expect(event.systemPrompt).toEqual(subagentSystemPrompt);

      const compactOutcome = await invoke(
        sessionCompacting,
        { type: "session.compacting", sessionId: SUBAGENT_SESSION_ID, messages: [] },
        context,
      );
      expect(compactOutcome.error).toBeUndefined();
      expect(compactOutcome.value).toBeUndefined();

      // The sub-session neither aborts nor touches the bound turn's receipt.
      expect(abortCalls).toBe(0);
      expect(recordValue(await readSidecar(sidecarPath)).prepared).toBeUndefined();
    },
  );
});

test("keeps the strict path in a scope that published no bound session", async () => {
  const snapshot: Snapshot = {
    content: "A scope without a published session keeps its original guarantee.",
    present: true,
    version: 34,
  };
  await withSidecar(
    sidecarFor("initial", snapshot, "pending", "attempt-unpublished"),
    async (root, sidecarPath) => {
      expect(await readFile(boundSessionPath(root), "utf8").catch(() => undefined)).toBeUndefined();
      const { beforeAgentStart } = captureHandlers();

      // The prepared turn still injects, exactly as before the pointer existed.
      const prepared = await invoke(
        beforeAgentStart,
        { type: "before_agent_start", prompt: "turn without a published scope", systemPrompt: ["base rule"] },
        extensionContext(SESSION_ID, () => {
          throw new Error("the prepared turn unexpectedly aborted");
        }),
      );
      expect(prepared.error).toBeUndefined();
      const prompt = returnedSystemPrompt(prepared.value);
      expect(prompt.at(-1)).toContain(snapshot.content);
      expect(recordValue(await readSidecar(sidecarPath)).prepared).toBeDefined();

      // A session the host never prepared still fails closed instead of
      // running contextless; the channel publishes a pointer before prompting,
      // so this branch only covers scopes that never went through a turn.
      let abortCalls = 0;
      const unprepared = await invoke(
        beforeAgentStart,
        { type: "before_agent_start", prompt: "unprepared session", systemPrompt: [] },
        extensionContext(SUBAGENT_SESSION_ID, () => {
          abortCalls += 1;
        }),
      );
      expect(unprepared.error).toBeDefined();
      expect(abortCalls).toBe(1);
    },
    { boundSessionId: null },
  );
});
