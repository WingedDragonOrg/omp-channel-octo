import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import contextExtension, { contextSidecarFileName } from "../src/omp/context-extension.ts";
import {
  boundSessionPath,
  readContextSidecar,
  writeAgentContextOverlay,
} from "../src/session/context-sidecar.ts";
import { formatMemberContext, memberSnapshotFor } from "../src/session/agent-context.ts";

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
  turnId: string;
  sessionId: string;
  snapshot: Snapshot;
  injectionDecision: InjectionDecision;
  attemptId: string;
  marker: Marker;
  previousVersion?: number;
  compactedContext?: {
    schemaVersion: number;
    requestId: string;
    botUid: string;
    target: Target;
    sessionId: string;
    turnId: string;
    attemptId: string;
    snapshot: Snapshot;
  };
};

type Handler = (event: unknown, context: unknown) => unknown | Promise<unknown>;
type Invocation = { value: unknown; error: unknown | undefined };

type PersistentMessage = {
  customType?: unknown;
  content?: unknown;
};

type ContextResult = {
  message?: unknown;
};

type PreCompactResult = {
  cancel?: unknown;
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
    turnId: "00000000-0000-4000-8000-000000000001",
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

function captureHandlers(sendMessage?: (
  message: { customType: string; content: string; display: false; details: Record<string, unknown> },
  options?: { triggerTurn?: boolean; deliverAs?: "steer" },
) => void): {
  beforeAgentStart: Handler;
  input?: Handler;
  sessionBeforeCompact?: Handler;
  modelContext?: Handler;
} {
  const handlers = new Map<string, Handler>();
  const api = {
    on(event: string, handler: Handler): void {
      handlers.set(event, handler);
    },
    ...(sendMessage === undefined ? {} : { sendMessage }),
  };
  contextExtension(api as unknown as Parameters<typeof contextExtension>[0]);

  const beforeAgentStart = handlers.get("before_agent_start");
  expect(beforeAgentStart).toEqual(expect.any(Function));
  if (beforeAgentStart === undefined) {
    throw new Error("context extension did not register the agent-start handler");
  }
  const input = handlers.get("input");
  const sessionBeforeCompact = handlers.get("session_before_compact");
  const modelContext = handlers.get("context");
  return {
    beforeAgentStart,
    ...(input ? { input } : {}),
    ...(sessionBeforeCompact ? { sessionBeforeCompact } : {}),
    ...(modelContext ? { modelContext } : {}),
  };
}

function requireHandler(handler: Handler | undefined, event: string): Handler {
  expect(handler).toEqual(expect.any(Function));
  if (handler === undefined) throw new Error(`context extension did not register ${event}`);
  return handler;
}

function extensionContext(
  sessionId: string,
  abort: () => void,
  branch: readonly unknown[] = [],
): Record<string, unknown> {
  return {
    sessionManager: { getSessionId: () => sessionId, getBranch: () => branch },
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


function returnedMessage(value: unknown): PersistentMessage {
  return recordValue((value as ContextResult | undefined)?.message);
}

function returnedMessageContent(value: unknown): string | undefined {
  const content = returnedMessage(value).content;
  return typeof content === "string" ? content : undefined;
}

function returnedCancel(value: unknown): boolean {
  return (value as PreCompactResult | undefined)?.cancel === true;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

test("leaves native slash-command input byte-for-byte unchanged", async () => {
  const sidecar = {
    ...sidecarFor("unchanged", { content: "", present: false, version: 1 }, "committed", "slash-input"),
    agentContext: {
      cliReady: true,
      memberSnapshot: {
        count: 1,
        members: [{ uid: "member-a", name: "Alice" }],
      },
    },
  };
  await withSidecar(sidecar, async () => {
    const { input } = captureHandlers();
    const transformInput = requireHandler(input, "input");
    expect(await transformInput(
      { type: "input", source: "rpc", text: "/autoresearch" },
      extensionContext(SESSION_ID, () => {}),
    )).toBeUndefined();
  });
});

test("large-roster identity changes alter the persisted fingerprint without inlining members", () => {
  const first = Array.from({ length: 11 }, (_, index) => ({
    uid: `member-${index}`,
    name: `Member ${index}`,
  }));
  const changed = first.map(member => ({ ...member }));
  changed[10] = { uid: "member-replaced", name: "Replacement" };
  const firstSnapshot = memberSnapshotFor(TARGET as Parameters<typeof memberSnapshotFor>[0], first);
  const changedSnapshot = memberSnapshotFor(TARGET as Parameters<typeof memberSnapshotFor>[0], changed);
  expect(firstSnapshot).not.toEqual(changedSnapshot);
  const rendered = formatMemberContext(
    TARGET as Parameters<typeof formatMemberContext>[0],
    changedSnapshot,
  );
  expect(rendered).not.toContain("member-replaced");
  expect(rendered).not.toContain("Replacement");
});

test("persists the canonical member snapshot in the user input through tools and retry", async () => {
  const sidecar = {
    ...sidecarFor("unchanged", { content: "", present: false, version: 1 }, "committed", "member-prefix"),
    agentContext: {
      cliReady: true,
      memberSnapshot: {
        count: 2,
        members: [
          { uid: "member-b", name: "Bob" },
          { uid: "member-a", name: "Alice" },
        ],
      },
    },
  };
  await withSidecar(sidecar, async () => {
    const { input, modelContext } = captureHandlers();
    const transformInput = requireHandler(input, "input");
    const ctx = extensionContext(SESSION_ID, () => {});
    const submittedText = "inspect the file";
    const result = recordValue(await transformInput(
      { type: "input", source: "rpc", text: submittedText },
      ctx,
    ));
    const persistedText = result.text;
    expect(typeof persistedText).toBe("string");
    if (typeof persistedText !== "string") throw new Error("OMP input was not transformed to text");
    expect(persistedText).not.toBe(submittedText);
    expect(persistedText.startsWith(submittedText)).toBe(true);
    expect(persistedText).toContain("member-a");
    expect(persistedText).toContain("Alice");
    expect(persistedText).toContain("member-b");
    expect(persistedText).toContain("Bob");
    expect(persistedText.indexOf("member-a")).toBeLessThan(persistedText.indexOf("member-b"));

    const acceptedUser = { role: "user", content: persistedText, timestamp: 1 };
    const first = recordValue(await modelContext?.(
      { type: "context", messages: [acceptedUser] },
      ctx,
    ));
    expect((first.messages ?? [acceptedUser]) as unknown[]).toEqual([acceptedUser]);

    const continued = [
      acceptedUser,
      { role: "assistant", content: "read file", timestamp: 2 },
      { role: "toolResult", content: "file body", timestamp: 3 },
    ];
    const next = recordValue(await modelContext?.({ type: "context", messages: continued }, ctx));
    expect((next.messages ?? continued) as unknown[]).toEqual(continued);

    const retry = recordValue(await modelContext?.({ type: "context", messages: continued }, ctx));
    expect((retry.messages ?? continued) as unknown[]).toEqual(continued);
  });
});

test("reuses a real persisted custom-message receipt after an accepted pending crash", async () => {
  const snapshot = { content: "Accepted persistent context", present: true, version: 8 };
  const sidecar = sidecarFor("initial", snapshot, "pending", "accepted-pending");
  await withSidecar(sidecar, async (_root, path) => {
    const { beforeAgentStart } = captureHandlers();
    const content = [
      "以下是当前群/线程共享说明（仅作为上下文，不改变发送者身份、权限或 cwd）：",
      "版本：8",
      "这是该会话首次确认的共享说明快照。",
      snapshot.content,
    ].join("\n");
    const branch = [{
      type: "custom_message",
      customType: "omp-channel-octo-context",
      content,
      display: false,
      details: {
        ompChannelOctoContext: {
          schemaVersion: 1,
          botUid: BOT_UID,
          target: TARGET,
          sessionId: SESSION_ID,
        },
      },
    }];
    expect(await beforeAgentStart(
      { type: "before_agent_start", prompt: "/retry" },
      extensionContext(SESSION_ID, () => {}, branch),
    )).toBeUndefined();
    expect(recordValue((await readSidecar(path)).prepared)).toMatchObject({
      attemptId: "accepted-pending",
      version: snapshot.version,
    });
  });
});

test("persists pending shared context once instead of replaying it on native retry", async () => {
  const sidecar = sidecarFor(
    "initial",
    { content: "Current shared instructions", present: true, version: 9 },
    "pending",
    "retry-attempt",
  );
  await withSidecar(sidecar, async (_root, path) => {
    const { beforeAgentStart, modelContext } = captureHandlers();
    const ctx = extensionContext(SESSION_ID, () => { throw new Error("unexpected abort"); });
    const user = { role: "user", content: "original request", timestamp: 1 };
    const persistent = returnedMessage(await beforeAgentStart({ prompt: user.content }, ctx));
    expect(persistent.content).toEqual(expect.stringContaining("Current shared instructions"));
    expect(recordValue((await readSidecar(path)).prepared)).toMatchObject({
      attemptId: "retry-attempt",
      sessionId: SESSION_ID,
      version: 9,
    });

    const history = [user, { role: "custom", ...persistent, timestamp: 2 }];
    const continuation = [
      ...history,
      { role: "assistant", content: "tool request", timestamp: 3 },
      { role: "toolResult", content: "tool output", timestamp: 4 },
    ];
    const continued = recordValue(await modelContext?.({ type: "context", messages: continuation }, ctx));
    expect((continued.messages ?? continuation) as unknown[]).toEqual(continuation);
    const retry = recordValue(await modelContext?.({ type: "context", messages: continuation }, ctx));
    expect((retry.messages ?? continuation) as unknown[]).toEqual(continuation);
    expect(await modelContext?.(
      { type: "context", messages: continuation },
      extensionContext(SUBAGENT_SESSION_ID, () => {}),
    )).toBeUndefined();
  });
});


test("persists an initial shared snapshot as a session message and prepares its receipt", async () => {
  const snapshot: Snapshot = {
    content: "Initial group instructions must be complete.",
    present: true,
    version: 4,
  };
  await withSidecar(
    sidecarFor("initial", snapshot, "pending", "attempt-initial"),
    async (_root, sidecarPath) => {
      const { beforeAgentStart } = captureHandlers();
      const result = await beforeAgentStart(
        { type: "before_agent_start", prompt: "first group turn" },
        extensionContext(SESSION_ID, () => {
          throw new Error("initial context unexpectedly aborted");
        }),
      );
      const content = returnedMessageContent(result);
      expect(content).toEqual(expect.stringContaining(snapshot.content));
      expect(content).toEqual(expect.stringContaining(String(snapshot.version)));

      const written = await readSidecar(sidecarPath);
      expect(written.marker).toMatchObject({ phase: "pending", attemptId: "attempt-initial" });
      expect(recordValue(written.prepared)).toEqual(expect.objectContaining({
        attemptId: "attempt-initial",
        botUid: BOT_UID,
        sessionId: SESSION_ID,
        version: snapshot.version,
        target: TARGET,
      }));
    },
  );
});

test("persists initial context when native goal bypasses the prompt-start hook", async () => {
  const snapshot: Snapshot = {
    content: "Native goal context must enter persistent session history.",
    present: true,
    version: 5,
  };
  await withSidecar(
    sidecarFor("initial", snapshot, "pending", "attempt-native-goal"),
    async (_root, sidecarPath) => {
      const sent: Array<{
        message: { customType: string; content: string; display: false; details: Record<string, unknown> };
        options?: { triggerTurn?: boolean; deliverAs?: "steer" };
      }> = [];
      const { modelContext } = captureHandlers((message, options) => {
        sent.push({ message, ...(options === undefined ? {} : { options }) });
      });
      const context = extensionContext(SESSION_ID, () => {
        throw new Error("native goal context unexpectedly aborted");
      });
      const first = recordValue(await modelContext?.(
        { type: "context", messages: [] },
        context,
      ));
      const firstMessages = first.messages as Array<Record<string, unknown>>;
      expect(firstMessages).toHaveLength(1);
      expect(firstMessages[0]).toMatchObject({
        role: "custom",
        customType: "omp-channel-octo-context",
        content: expect.stringContaining(snapshot.content),
        display: false,
      });
      expect(sent).toHaveLength(1);
      expect(sent[0]?.options).toEqual({ triggerTurn: false, deliverAs: "steer" });

      const persistent = {
        type: "message",
        message: { role: "custom", ...sent[0]!.message },
      };
      const retry = await modelContext?.(
        { type: "context", messages: firstMessages },
        extensionContext(SESSION_ID, () => {}, [persistent]),
      );
      expect(retry).toBeUndefined();
      expect(sent).toHaveLength(1);
      expect(recordValue((await readSidecar(sidecarPath)).prepared)).toMatchObject({
        attemptId: "attempt-native-goal",
        sessionId: SESSION_ID,
        version: snapshot.version,
      });
    },
  );
});

test("restores the committed compaction snapshot immediately after its summary", async () => {
  const previous: Snapshot = {
    content: "Previously shared context.",
    present: true,
    version: 12,
  };
  const fresh: Snapshot = {
    content: "Fresh instructions preserved by the successful compaction.",
    present: true,
    version: 13,
  };
  const sidecar = sidecarFor("unchanged", previous, "committed", "attempt-compacted");
  const receipt = {
    schemaVersion: 1,
    requestId: "00000000-0000-4000-8000-000000000002",
    botUid: BOT_UID,
    target: TARGET,
    sessionId: SESSION_ID,
    turnId: sidecar.turnId,
    attemptId: sidecar.attemptId,
    snapshot: fresh,
  };
  await withSidecar(sidecar, async (_root, sidecarPath) => {
    const { beforeAgentStart, modelContext } = captureHandlers();
    const branch = [{
      type: "compaction",
      preserveData: { ompChannelOctoContext: receipt },
    }];
    const context = extensionContext(SESSION_ID, () => {
      throw new Error("committed compaction context unexpectedly aborted");
    }, branch);
    expect(await beforeAgentStart(
      { type: "before_agent_start", prompt: "continue after compaction" },
      context,
    )).toBeUndefined();

    const original = [
      { role: "compactionSummary", content: "Summary retained from compaction." },
      { role: "assistant", content: "Continue the user task." },
    ];
    const restored = recordValue(await modelContext?.(
      { type: "context", messages: original },
      context,
    ));
    const messages = restored.messages as Array<Record<string, unknown>>;
    expect(messages).toHaveLength(3);
    expect(messages[0]).toEqual(original[0]);
    const contextMessage = messages[1]!;
    expect(contextMessage.role).toBe("custom");
    expect(contextMessage.customType).toBe("omp-channel-octo-context");
    expect(typeof contextMessage.content).toBe("string");
    if (typeof contextMessage.content !== "string") throw new Error("compacted context is not text");
    expect(contextMessage.content).toContain(fresh.content);
    expect(contextMessage.display).toBe(false);
    expect(messages[2]).toEqual(original[1]);
    expect((await readSidecar(sidecarPath)).compactedContext).toMatchObject({
      snapshot: { content: fresh.content, version: fresh.version },
      requestId: receipt.requestId,
      turnId: receipt.turnId,
    });

    expect(await modelContext?.(
      { type: "context", messages },
      context,
    )).toBeUndefined();
  });
});

test("fork projection removes only managed context from another chat scope", async () => {
  const snapshot: Snapshot = {
    content: "Fresh target context.",
    present: true,
    version: 2,
  };
  await withSidecar(
    sidecarFor("unchanged", snapshot, "committed", "attempt-fork-target"),
    async () => {
      const { modelContext } = captureHandlers();
      const sourceTarget = { channelType: 2 as const, channelId: "source-group" };
      const sourceBusinessText = "Explicit source user business history.";
      const foreignMemberSuffix = formatMemberContext(sourceTarget, {
        count: 1,
        members: [{ uid: "source-member", name: "Source member" }],
      });
      const foreignManaged = {
        role: "custom",
        customType: "omp-channel-octo-context",
        content: "SOURCE_MEMORY_MUST_NOT_PROJECT",
        details: {
          ompChannelOctoContext: {
            schemaVersion: 1,
            botUid: BOT_UID,
            target: sourceTarget,
            sessionId: "source-session",
          },
        },
      };
      const currentManaged = {
        role: "custom",
        customType: "omp-channel-octo-context",
        content: "Current target managed context.",
        details: {
          ompChannelOctoContext: {
            schemaVersion: 1,
            botUid: BOT_UID,
            target: TARGET,
            sessionId: SESSION_ID,
          },
        },
      };
      const reminderBlock = {
        type: "text",
        text: "<system-reminder>fork target reminder</system-reminder>",
        metadata: { source: "runtime" },
      };
      const original = [
        foreignManaged,
        {
          role: "user",
          content: [
            reminderBlock,
            { type: "text", text: `${sourceBusinessText}\n\n${foreignMemberSuffix}` },
          ],
        },
        currentManaged,
        { role: "assistant", content: "Existing assistant history." },
      ];
      const projected = recordValue(await modelContext?.(
        { type: "context", messages: original },
        extensionContext(SESSION_ID, () => {}),
      ));
      expect(projected.messages).toEqual([
        {
          role: "user",
          content: [
            reminderBlock,
            { type: "text", text: sourceBusinessText },
          ],
        },
        currentManaged,
        original[3],
      ]);
    },
  );
});

test("does not add a shared message on an unchanged ordinary turn", async () => {
  const snapshot: Snapshot = {
    content: "This unchanged snapshot must stay out of the ordinary prompt.",
    present: true,
    version: 12,
  };
  await withSidecar(
    sidecarFor("unchanged", snapshot, "committed", "attempt-committed"),
    async (_root, sidecarPath) => {
      const { beforeAgentStart } = captureHandlers();
      const result = await beforeAgentStart(
        { type: "before_agent_start", prompt: "ordinary unchanged turn" },
        extensionContext(SESSION_ID, () => {
          throw new Error("unchanged context unexpectedly aborted");
        }),
      );

      expect(returnedMessageContent(result)).toBeUndefined();
      const written = await readSidecar(sidecarPath);
      expect(written.marker).toMatchObject({ phase: "committed", version: snapshot.version });
      expect(written.prepared).toBeUndefined();
    },
  );
});

test("cancels compaction when a fresh host read is unavailable and preserves prior context", async () => {
  const snapshot: Snapshot = {
    content: "The last confirmed snapshot must remain available after a read failure.",
    present: true,
    version: 12,
  };
  await withSidecar(
    sidecarFor("unchanged", snapshot, "committed", "attempt-committed"),
    async (_root, sidecarPath) => {
      const { sessionBeforeCompact } = captureHandlers();
      const before = await readSidecar(sidecarPath);
      const result = await sessionBeforeCompact?.(
        { type: "session_before_compact", signal: new AbortController().signal },
        extensionContext(SESSION_ID, () => {}),
      );

      expect(returnedCancel(result)).toBe(true);
      expect(JSON.stringify(result)).not.toContain(snapshot.content);
      const after = await readSidecar(sidecarPath);
      expect(after.snapshot).toEqual(before.snapshot);
      expect(after.marker).toEqual(before.marker);
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
        const { beforeAgentStart, sessionBeforeCompact: compactHandler } = captureHandlers();
        const sessionBeforeCompact = requireHandler(compactHandler, "session_before_compact");
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
          },
          context,
        );
        expect(beforeOutcome.error).toBeDefined();
        expect(returnedMessageContent(beforeOutcome.value)).toBeUndefined();
        expect(abortCalls).toBe(beforeAbortCalls + 1);

        const compactOutcome = await invoke(
          sessionBeforeCompact,
          { type: "session_before_compact", signal: new AbortController().signal },
          context,
        );
        expect(compactOutcome.error).toBeUndefined();
        expect(returnedCancel(compactOutcome.value)).toBe(true);

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

test("leaves a subagent input and compaction outside the bound chat session", async () => {
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

      const { beforeAgentStart, input, sessionBeforeCompact: compactHandler } = captureHandlers();
      const sessionBeforeCompact = requireHandler(compactHandler, "session_before_compact");
      let abortCalls = 0;
      const context = extensionContext(SUBAGENT_SESSION_ID, () => {
        abortCalls += 1;
      });
      const inputHandler = requireHandler(input, "input");
      expect(await inputHandler(
        { type: "input", source: "rpc", text: "subagent request" },
        context,
      )).toBeUndefined();

      const outcome = await invoke(
        beforeAgentStart,
        { type: "before_agent_start", prompt: "subagent probe" },
        context,
      );
      expect(outcome.error).toBeUndefined();
      expect(outcome.value).toBeUndefined();

      const compactOutcome = await sessionBeforeCompact(
        { type: "session_before_compact", signal: new AbortController().signal },
        context,
      );
      expect(compactOutcome).toBeUndefined();
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

      const prepared = await invoke(
        beforeAgentStart,
        { type: "before_agent_start", prompt: "turn without a published scope" },
        extensionContext(SESSION_ID, () => {
          throw new Error("the prepared turn unexpectedly aborted");
        }),
      );
      expect(prepared.error).toBeUndefined();
      expect(returnedMessageContent(prepared.value)).toEqual(
        expect.stringContaining(snapshot.content),
      );
      expect(recordValue(await readSidecar(sidecarPath)).prepared).toBeDefined();

      let abortCalls = 0;
      const unprepared = await invoke(
        beforeAgentStart,
        { type: "before_agent_start", prompt: "unprepared session" },
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
