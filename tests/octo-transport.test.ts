import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createOctoTransport } from "../src/octo/index";
import type { ChannelTransport, InboundMessage } from "../src/contracts";
import type { ExecutionSnapshot } from "../src/execution-card";
import {
  createOctoProtocolFixture,
  waitForFixture,
  type FixtureConnection,
  type OctoProtocolFixture,
  type OctoProtocolFixtureOptions,
} from "./fixtures/octo-protocol-server";

let fixture: OctoProtocolFixture | undefined;
let transport: ChannelTransport | undefined;
let temporaryDirectory: string | undefined;

afterEach(async () => {
  await transport?.stop().catch(() => {});
  await fixture?.close().catch(() => {});
  if (temporaryDirectory) await rm(temporaryDirectory, { recursive: true, force: true });
  transport = undefined;
  fixture = undefined;
  temporaryDirectory = undefined;
});

// These integration tests deliberately use the platform clock: the value under test is
// real WebSocket handshake/reconnect timing, which fake timers cannot drive.
function delayed(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

async function readyConnection(connection: FixtureConnection): Promise<FixtureConnection> {
  return waitForFixture(
    () => connection.ready ? connection : undefined,
    2_000,
    "Octo fixture connection did not reach CONNACK ready",
  );
}

async function startFixtureTransport(
  options: OctoProtocolFixtureOptions = {},
  onMessage: (message: InboundMessage) => Promise<void> = async () => {},
): Promise<{ connection: FixtureConnection; online: { count: number } }> {
  const currentFixture = await createOctoProtocolFixture(options);
  fixture = currentFixture;
  const created = createOctoTransport({
    apiUrl: currentFixture.apiUrl,
    botToken: currentFixture.botToken,
  });
  transport = created;
  await created.initialize();
  const online = { count: 0 };
  const startPromise = created.start(onMessage, () => {
    online.count += 1;
  });
  const connection = await currentFixture.waitForConnection(1);
  await readyConnection(connection);
  await startPromise;
  return { connection, online };
}

const SUPPORTED_CARD_CONFIG: Record<string, unknown> = {
  card_enabled: true,
  display_enabled: true,
  interaction_enabled: false,
  reasoning_enabled: true,
  reasoning_template_ref: { id: "progress", version: "1" },
};

function cardProfile(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    enabled: true,
    profiles: ["octo/v1"],
    card_version: "1.5",
    elements: SUPPORTED_CARD_ELEMENTS,
    actions: SUPPORTED_CARD_ACTIONS,
    config: { ...SUPPORTED_CARD_CONFIG },
    ...overrides,
  };
}

const SUPPORTED_CARD_ELEMENTS = ["TextBlock", "Container", "ColumnSet", "ActionSet"];
const SUPPORTED_CARD_ACTIONS = ["Action.OpenUrl", "Action.ToggleVisibility", "Action.CopyToClipboard"];

function executionSnapshot(
  state: ExecutionSnapshot["state"] = "running",
  overrides: Partial<ExecutionSnapshot> = {},
): ExecutionSnapshot {
  const terminal = state !== "running";
  const toolState: ExecutionSnapshot["tools"][number]["state"] = state === "running"
    ? "running"
    : state === "completed"
      ? "succeeded"
      : state;
  const todoStatus: ExecutionSnapshot["todos"][number]["tasks"][number]["status"] = state === "running"
    ? "in_progress"
    : state === "completed"
      ? "completed"
      : state === "cancelled"
        ? "abandoned"
        : "blocked";
  return {
    runId: "run-1",
    state,
    startedAtMs: 0,
    endedAtMs: terminal ? 1_000 : null,
    elapsedMs: 1_000,
    phaseCount: 1,
    toolCount: 1,
    tools: [{
      toolCallId: "tool-1",
      toolName: "shell",
      state: toolState,
      startedAtMs: 0,
      endedAtMs: terminal ? 1_000 : null,
      durationMs: terminal ? 1_000 : null,
      summary: terminal ? "Completed shell" : "Running shell",
    }],
    todos: [{
      name: "Execution",
      tasks: [{ content: "Run command", status: todoStatus }],
    }],
    ...overrides,
  };
}

async function expectNoProgressCard(profile: Record<string, unknown>): Promise<void> {
  fixture = await createOctoProtocolFixture({ cardProfileBody: profile });
  transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
  await transport.initialize();
  const currentFixture = fixture;
  const result = await transport.sendProgressCard(
    { channelId: currentFixture.ownerUid, channelType: 1 },
    executionSnapshot(),
    "gated-progress",
  );
  expect(result).toBeNull();
  expect(currentFixture.cardProfileRequests).toHaveLength(1);
  expect(currentFixture.sentMessages).toHaveLength(0);
}

async function expectProgressCard(profile: Record<string, unknown>): Promise<void> {
  fixture = await createOctoProtocolFixture({ cardProfileBody: profile });
  transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
  await transport.initialize();
  const currentFixture = fixture;
  const result = await transport.sendProgressCard(
    { channelId: currentFixture.ownerUid, channelType: 1 },
    executionSnapshot(),
    "supported-progress",
  );
  expect(result).toEqual({ messageId: "9007199254740993" });
  expect(currentFixture.cardProfileRequests).toHaveLength(1);
  expect(currentFixture.sentMessages).toHaveLength(1);
}

describe("Octo registration and WuKongIM transport", () => {
  test("uses the registered ws_url, performs v4 crypto, and ACKs only after the handler resolves", async () => {
    fixture = await createOctoProtocolFixture({ advertisedWsQuery: "?advertised=registration" });
    transport = createOctoTransport({
      apiUrl: fixture.apiUrl,
      botToken: fixture.botToken,
    });

    const identity = await transport.initialize();
    expect(identity.uid).toBe(fixture.botUid);
    expect(identity.name).toBe(fixture.botName);
    expect(fixture.registerRequests).toHaveLength(1);
    expect(fixture.registerRequests[0]).toMatchObject({
      path: "/v1/bot/register",
      authorization: `Bearer ${fixture.botToken}`,
    });

    const received: Array<Record<string, unknown>> = [];
    const handlerStartedState = Promise.withResolvers<void>();
    const handlerGate = Promise.withResolvers<void>();
    const online = { count: 0 };
    const startPromise = transport.start(async (message) => {
      received.push(message as unknown as Record<string, unknown>);
      handlerStartedState.resolve();
      await handlerGate.promise;
    }, () => {
      online.count += 1;
    });

    const connection = await fixture.waitForConnection(1);
    await readyConnection(connection);
    expect(connection.wsPath).toBe("/?advertised=registration");
    await startPromise;
    expect(online.count).toBe(1);

    fixture.sendInbound(connection, {
      fromUid: fixture.ownerUid,
      channelType: 1,
      text: "真实 AES-CBC 文本 ✅",
      messageId: "9007199254740995",
      messageSeq: 41,
      timestamp: 1_757_656_001,
    }, { fragmentSizes: [1, 2, 3, 5, 8] });

    await handlerStartedState.promise;
    expect(connection.recvAcks).toHaveLength(0);
    handlerGate.resolve();
    await waitForFixture(
      () => connection.recvAcks.length === 1 ? connection.recvAcks : undefined,
      2_000,
      "inbound RECVACK was not sent after handler completion",
    );

    expect(received[0]).toMatchObject({
      channelId: fixture.ownerUid,
      channelType: 1,
      messageId: "9007199254740995",
      senderId: fixture.ownerUid,
      text: "真实 AES-CBC 文本 ✅",
      mentionedUserIds: [],
      supported: true,
    });
    expect(connection.recvAcks[0]).toEqual({
      messageId: "9007199254740995",
      messageSeq: 41,
    });
  });
  test("normalizes every DM wire channel to the sender and ignores synthetic peers", async () => {
    const received: InboundMessage[] = [];
    const started = await startFixtureTransport({}, async (message) => {
      received.push(message);
    });
    const currentFixture = fixture!;
    currentFixture.sendInboundBatch(started.connection, [
      {
        fromUid: currentFixture.ownerUid,
        channelId: `${currentFixture.ownerUid}@${currentFixture.botUid}`,
        channelType: 1,
        text: "owner at bot",
        messageId: "9007199254741011",
        messageSeq: 61,
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: currentFixture.botUid,
        channelType: 1,
        text: "plain bot channel",
        messageId: "9007199254741012",
        messageSeq: 62,
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: `${currentFixture.ownerUid}@synthetic-peer`,
        channelType: 1,
        text: "ignore synthetic peer",
        messageId: "9007199254741013",
        messageSeq: 63,
      },
    ]);

    await waitForFixture(
      () => received.length === 2 ? received : undefined,
      2_000,
      "DM wire channel variants were not routed",
    );
    expect(received.map((message) => message.channelId)).toEqual([
      currentFixture.ownerUid,
      currentFixture.ownerUid,
    ]);
    await transport!.sendText(received[0]!, "reply through normalized DM", "normalized-dm-reply");
    expect(currentFixture.sentMessages[0]?.body.channel_id).toBe(currentFixture.ownerUid);
  });

  test("does not report start ready before authenticated CONNACK", async () => {
    fixture = await createOctoProtocolFixture({ connackDelayMs: 80 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    let settled = false;
    const startPromise = transport.start(async () => {}, () => {});
    const observed = startPromise.then(
      () => { settled = true; },
      () => { settled = true; },
    );
    await delayed(20);
    expect(settled).toBe(false);
    await observed;
    expect(settled).toBe(true);
  });

  test("passes structured mention identities through and strips only a verified bot mention", async () => {
    const received: Array<Record<string, unknown>> = [];
    const started = await startFixtureTransport({}, async (message) => {
      received.push(message as unknown as Record<string, unknown>);
    });
    const { connection } = started;
    const botPrefix = `@${fixture!.botName}`;
    const mentionLength = botPrefix.length;

    fixture!.sendInboundBatch(connection, [
      {
        fromUid: fixture!.ownerUid,
        channelId: "group-real-mention",
        channelType: 2,
        text: `${botPrefix} /status`,
        mention: {
          entities: [{ uid: fixture!.botUid, offset: 0, length: mentionLength }],
        },
        messageId: "9007199254741001",
        messageSeq: 52,
      },
      {
        fromUid: "other-member",
        channelId: "group-fake-mention",
        channelType: 2,
        text: `${botPrefix} /status`,
        mention: {
          entities: [{ uid: "other-member", offset: 0, length: mentionLength }],
        },
        messageId: "9007199254741002",
        messageSeq: 53,
      },
    ], { fragmentSizes: [1, 4, 9] });

    await waitForFixture(
      () => received.length === 2 ? received : undefined,
      2_000,
      "fixture mentions were not delivered",
    );
    await waitForFixture(
      () => connection.recvAcks.length === 2 ? connection.recvAcks : undefined,
      2_000,
      "fixture mention frames were not acknowledged",
    );

    expect(received[0]).toMatchObject({
      channelId: "group-real-mention",
      channelType: 2,
      text: "/status",
      mentionedUserIds: [fixture!.botUid],
    });
    expect(received[1]).toMatchObject({
      channelId: "group-fake-mention",
      channelType: 2,
      text: `${botPrefix} /status`,
      mentionedUserIds: ["other-member"],
    });
  });

  test("re-registers and authenticates with fresh credentials after reconnect", async () => {
    fixture = await createOctoProtocolFixture({
      registrationTokens: ["im-token-first", "im-token-second"],
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const online = { count: 0 };
    const startPromise = transport.start(async () => {}, () => {
      online.count += 1;
    });
    const first = await fixture.waitForConnection(1);
    await readyConnection(first);
    await startPromise;
    expect(first.connect?.token).toBe("im-token-first");

    fixture.closeConnection(0);
    const second = await fixture.waitForConnection(2, 4_500);
    await readyConnection(second);
    await waitForFixture(
      () => online.count >= 2 ? online.count : undefined,
      4_500,
      "transport did not report the reconnected ready state",
    );
    expect(fixture.registerRequests.length).toBeGreaterThanOrEqual(2);
    expect(second.connect?.token).toBe("im-token-second");
  });

  test("stops on permanent REST registration authentication failure", async () => {
    fixture = await createOctoProtocolFixture({ registerStatus: 401 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });

    await expect(transport.initialize()).rejects.toThrow(/401|register|auth/i);
    expect(fixture.registerRequests).toHaveLength(1);
    expect(fixture.registerRequests[0]!.path).toBe("/v1/bot/register");
    expect(fixture.connections).toHaveLength(0);
  });

  test("rejects a permanent WebSocket CONNACK authentication failure without reconnecting", async () => {
    fixture = await createOctoProtocolFixture({ connackReasonCodes: [2] });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    const startPromise = transport.start(async () => {}, () => {});
    const outcome = await Promise.race([
      startPromise.then(() => "resolved", () => "rejected"),
      delayed(1_000).then(() => "timeout"),
    ]);
    expect(outcome).toBe("rejected");
    const connectionCount = fixture.connections.length;
    await delayed(100);
    expect(fixture.connections.length).toBe(connectionCount);
  });
});

describe("Octo text send and target context", () => {
  test("sends the exact stable client_msg_no and preserves an int64 response id", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    const target = { channelId: fixture.ownerUid, channelType: 1 as const };
    const first = await transport.sendText(target, "第一次回复", "stable-client-msg-1");
    const second = await transport.sendText(target, "第一次回复", "stable-client-msg-1");

    expect(first).toEqual({ messageId: "9007199254740993" });
    expect(second).toEqual({ messageId: "9007199254740993" });
    expect(fixture.sentMessages).toHaveLength(2);
    expect(fixture.sentMessages.map(({ body }) => body.client_msg_no)).toEqual([
      "stable-client-msg-1",
      "stable-client-msg-1",
    ]);
    expect(fixture.sentMessages[0]).toMatchObject({
      authorization: `Bearer ${fixture.botToken}`,
      body: {
        channel_id: fixture.ownerUid,
        channel_type: 1,
        payload: { type: 1, content: "第一次回复" },
      },
    });
  });

  test("rejects a single text send above the 3000 UTF-8-byte transport limit", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await expect(
      transport.sendText(
        { channelId: fixture.ownerUid, channelType: 1 },
        "🙂".repeat(751),
        "too-large",
      ),
    ).rejects.toThrow(/3000|byte/i);
    expect(fixture.sentMessages).toHaveLength(0);
  });

  test("edits a type-1 message in place with the upstream content_edit envelope", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const firstText = "seed";
    const finalText = "  **answer**  \n\n- keep the paragraph\n\n[docs](https://example.com)\n";

    const receipt = await transport.sendText(target, firstText, "stream-text-1");
    await transport.editText(target, receipt.messageId!, finalText);
    await transport.editText(target, receipt.messageId!, "");

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    expect(currentFixture.sentMessages).toHaveLength(1);
    expect(currentFixture.messageEdits).toHaveLength(2);
    expect(currentFixture.messageEdits.map((request) => request.body.message_id)).toEqual([
      "9007199254740993",
      "9007199254740993",
    ]);
    expect(currentFixture.messageEdits.map((request) => request.body.channel_id)).toEqual([
      currentFixture.ownerUid,
      currentFixture.ownerUid,
    ]);
    expect(currentFixture.messageEdits.map((request) => request.body.channel_type)).toEqual([1, 1]);

    const firstEdit = JSON.parse(
      currentFixture.messageEdits[0]!.body.content_edit as string,
    ) as Record<string, unknown>;
    expect(firstEdit).toEqual({ type: 1, content: finalText });
    const finalEdit = JSON.parse(
      currentFixture.messageEdits[1]!.body.content_edit as string,
    ) as Record<string, unknown>;
    expect(finalEdit).toEqual({ type: 1, content: "" });
    expect(currentFixture.getLatestEditedText(receipt.messageId!)).toBe("");
  });

  test("enforces the 3000-byte UTF-8 limit for type-1 edits", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const receipt = await transport.sendText(target, "seed", "edit-size-limit");
    const boundary = "🙂".repeat(750);

    await transport.editText(target, receipt.messageId!, boundary);
    await expect(
      transport.editText(target, receipt.messageId!, `${boundary}🙂`),
    ).rejects.toThrow(/3000|byte/i);

    expect(currentFixture.messageEdits).toHaveLength(1);
    expect(currentFixture.getLatestEditedText(receipt.messageId!)).toBe(boundary);
  });

  test("propagates ordinary text edit HTTP failures without sending a fallback message", async () => {
    fixture = await createOctoProtocolFixture({ messageEditStatus: 503 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const receipt = await transport.sendText(target, "seed", "edit-http-failure");

    await expect(
      transport.editText(target, receipt.messageId!, "failed edit"),
    ).rejects.toThrow(/503|edit/i);

    expect(currentFixture.sentMessages).toHaveLength(1);
    expect(currentFixture.messageEdits).toHaveLength(1);
    expect(currentFixture.getLatestEditedText(receipt.messageId!)).toBeUndefined();
  });

  test("times out an ordinary text edit when the protocol fixture does not respond", async () => {
    fixture = await createOctoProtocolFixture({ messageEditDelayMs: 15_100 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const receipt = await transport.sendText(target, "seed", "edit-timeout");

    await expect(
      transport.editText(target, receipt.messageId!, "timed out edit"),
    ).rejects.toThrow(/timed out/i);

    expect(currentFixture.messageEdits).toHaveLength(1);
    expect(currentFixture.getLatestEditedText(receipt.messageId!)).toBeUndefined();
  }, 20_000);

  test("aborts an in-flight sendText request through its AbortSignal", async () => {
    fixture = await createOctoProtocolFixture({ sendMessageDelayMs: 100 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const abort = new AbortController();
    const pending = transport.sendText(target, "cancelled send", "send-abort", abort.signal);

    await waitForFixture(
      () => currentFixture.sentMessages.length === 1 ? true : undefined,
      1_000,
      "sendText request did not reach the protocol fixture",
    );
    abort.abort();
    await expect(pending).rejects.toThrow(/abort/i);
    expect(currentFixture.sentMessages).toHaveLength(1);
  });

  test("aborts an in-flight editText request through its AbortSignal", async () => {
    fixture = await createOctoProtocolFixture({ messageEditDelayMs: 100 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };
    const receipt = await transport.sendText(target, "seed", "edit-abort-seed");
    const abort = new AbortController();
    const pending = transport.editText(target, receipt.messageId!, "cancelled edit", abort.signal);

    await waitForFixture(
      () => currentFixture.messageEdits.length === 1 ? true : undefined,
      1_000,
      "editText request did not reach the protocol fixture",
    );
    abort.abort();
    await expect(pending).rejects.toThrow(/abort/i);
    expect(currentFixture.getLatestEditedText(receipt.messageId!)).toBeUndefined();
  });

  test("resolves DM, group, and thread context through their actual REST paths", async () => {
    const groupId = "group/中文 & name";
    const threadId = `${groupId}____thread/42`;
    fixture = await createOctoProtocolFixture({
      groupContexts: {
        [groupId]: {
          content: "# Group rules",
          version: 3,
          updatedAt: "2026-09-12T10:00:00Z",
          updatedBy: "owner-user-uid",
        },
        [threadId]: {
          content: "# Thread rules",
          version: 7,
          updatedAt: "2026-09-12T10:01:00Z",
          updatedBy: "thread-owner",
        },
        "empty-group": {
          content: "",
          version: 8,
          updatedAt: null,
          updatedBy: "",
        },
      },
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    const dm = await transport.getGroupContext({ channelId: fixture.ownerUid, channelType: 1 });
    expect(dm).toBeNull();
    expect(fixture.groupContextRequests).toHaveLength(0);

    const group = await transport.getGroupContext({ channelId: groupId, channelType: 2 });
    expect(group).toEqual({
      content: "# Group rules",
      version: 3,
      updatedAt: "2026-09-12T10:00:00Z",
      updatedBy: "owner-user-uid",
    });
    expect(fixture.groupContextRequests[0]).toMatchObject({
      path: `/v1/bot/groups/${encodeURIComponent(groupId)}/md`,
      target: groupId,
      authorization: `Bearer ${fixture.botToken}`,
    });

    const thread = await transport.getGroupContext({ channelId: threadId, channelType: 5 });
    expect(thread).toEqual({
      content: "# Thread rules",
      version: 7,
      updatedAt: "2026-09-12T10:01:00Z",
      updatedBy: "thread-owner",
    });
    expect(fixture.groupContextRequests[1]).toMatchObject({
      path: `/v1/bot/groups/${encodeURIComponent(groupId)}/threads/${encodeURIComponent("thread/42")}/md`,
      target: threadId,
    });

    await expect(
      transport.getGroupContext({ channelId: "empty-group", channelType: 2 }),
    ).resolves.toBeNull();
    await expect(
      transport.getGroupContext({ channelId: "missing-group", channelType: 2 }),
    ).resolves.toBeNull();
  });

  test("fails closed on context HTTP errors, invalid schema, and a response over 64 KiB", async () => {
    const invalidId = "invalid-schema";
    const forbiddenId = "forbidden-group";
    const unauthorizedId = "unauthorized-group";
    const serverErrorId = "server-error-group";
    const hugeId = "huge-group";
    fixture = await createOctoProtocolFixture({
      groupContextBodies: {
        [invalidId]: { content: 42, version: "not-a-number", updated_at: null, updated_by: "" },
      },
      groupContextStatuses: {
        [forbiddenId]: 403,
        [unauthorizedId]: 401,
        [serverErrorId]: 500,
      },
      groupContexts: {
        [hugeId]: {
          content: "x".repeat(65 * 1024),
          version: 1,
          updatedAt: null,
          updatedBy: "",
        },
      },
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await expect(
      transport.getGroupContext({ channelId: unauthorizedId, channelType: 2 }),
    ).rejects.toThrow(/401|context|auth/i);
    await expect(
      transport.getGroupContext({ channelId: forbiddenId, channelType: 2 }),
    ).rejects.toThrow(/403|context|forbidden/i);
    await expect(
      transport.getGroupContext({ channelId: serverErrorId, channelType: 2 }),
    ).rejects.toThrow(/500|context|server/i);
    await expect(
      transport.getGroupContext({ channelId: invalidId, channelType: 2 }),
    ).rejects.toThrow(/schema|context|invalid/i);
    await expect(
      transport.getGroupContext({ channelId: hugeId, channelType: 2 }),
    ).rejects.toThrow(/64|byte|large|limit/i);
  });

  test("updates group and thread context through PUT and refuses DM updates", async () => {
    const groupId = "group with spaces";
    const threadId = `${groupId}____thread/with slash`;
    const forbiddenId = "forbidden-update";
    fixture = await createOctoProtocolFixture({
      groupContextPutVersions: { [groupId]: 12, [threadId]: 13 },
      groupContextPutStatuses: { [forbiddenId]: 403 },
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await expect(
      transport.updateGroupContext({ channelId: groupId, channelType: 2 }, "# New group rules"),
    ).resolves.toEqual({ version: 12 });
    await expect(
      transport.updateGroupContext({ channelId: threadId, channelType: 5 }, "# New thread rules"),
    ).resolves.toEqual({ version: 13 });
    expect(fixture.groupContextUpdates[0]).toMatchObject({
      path: `/v1/bot/groups/${encodeURIComponent(groupId)}/md`,
      target: groupId,
      body: { content: "# New group rules" },
    });
    expect(fixture.groupContextUpdates[1]).toMatchObject({
      path: `/v1/bot/groups/${encodeURIComponent(groupId)}/threads/${encodeURIComponent("thread/with slash")}/md`,
      target: threadId,
      body: { content: "# New thread rules" },
    });
    await expect(
      transport.updateGroupContext({ channelId: fixture.ownerUid, channelType: 1 }, "ignored"),
    ).rejects.toThrow(/DM|group|private|unsupported/i);
    expect(fixture.groupContextUpdates).toHaveLength(2);
    await expect(
      transport.updateGroupContext({ channelId: forbiddenId, channelType: 2 }, "denied"),
    ).rejects.toThrow(/403|permission|context|update/i);
  });
});

describe("Octo typing and progress cards", () => {
  test("posts typing to the target with the authenticated bot", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const target = { channelId: fixture.ownerUid, channelType: 1 as const };

    await transport.sendTyping(target);

    expect(fixture.typingRequests).toEqual([{
      path: "/v1/bot/typing",
      authorization: `Bearer ${fixture.botToken}`,
      body: { channel_id: fixture.ownerUid, channel_type: 1 },
    }]);
  });

  test("sends one type-17 Adaptive Card and edits that message in place", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };

    const receipt = await transport.sendProgressCard(target, executionSnapshot(), "stable-progress-1");

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    expect(currentFixture.cardProfileRequests).toEqual([{
      path: "/v1/bot/card/profile",
      authorization: `Bearer ${currentFixture.botToken}`,
    }]);
    expect(currentFixture.sentMessages).toHaveLength(1);
    expect(currentFixture.sentMessages[0]).toMatchObject({
      authorization: `Bearer ${currentFixture.botToken}`,
      body: {
        channel_id: currentFixture.ownerUid,
        channel_type: 1,
        client_msg_no: "stable-progress-1",
        payload: {
          type: 17,
          profile: "octo/v1",
          card_version: "1.5",
        },
      },
    });

    const sentPayload = currentFixture.sentMessages[0]!.body.payload as Record<string, unknown>;
    const sentCard = sentPayload.card as Record<string, unknown>;
    expect(sentCard).toMatchObject({
      type: "AdaptiveCard",
      version: "1.5",
      metadata: { octo_layout: "agent_progress_v1" },
    });

    expect(typeof sentPayload.plain).toBe("string");
    expect(sentPayload.plain).toContain("处理进度");
    expect(sentPayload.plain).toContain("shell");

    const messageId = receipt!.messageId!;
    await transport.editProgressCard(target, messageId, executionSnapshot("running"), 1);
    await transport.editProgressCard(target, messageId, executionSnapshot("completed"), 2);

    expect(currentFixture.sentMessages).toHaveLength(1);
    expect(currentFixture.messageEdits).toHaveLength(2);
    expect(currentFixture.messageEdits.map((request) => request.path)).toEqual([
      "/v1/bot/message/edit",
      "/v1/bot/message/edit",
    ]);
    expect(currentFixture.messageEdits.map((request) => request.body.message_id)).toEqual([
      "9007199254740993",
      "9007199254740993",
    ]);
    expect(currentFixture.messageEdits.map((request) => request.body.channel_id)).toEqual([
      currentFixture.ownerUid,
      currentFixture.ownerUid,
    ]);
    expect(currentFixture.messageEdits.map((request) => request.body.channel_type)).toEqual([1, 1]);

    const runningEdit = JSON.parse(
      currentFixture.messageEdits[0]!.body.content_edit as string,
    ) as Record<string, unknown>;
    expect(runningEdit).toMatchObject({
      type: 17,
      profile: "octo/v1",
      card_version: "1.5",
      card_seq: 1,
      transient: true,
    });
    expect(runningEdit.card).toMatchObject({
      type: "AdaptiveCard",
      version: "1.5",
      metadata: { octo_layout: "agent_progress_v1" },
    });
    expect(typeof runningEdit.plain).toBe("string");
    expect(runningEdit.plain).toContain("shell");

    const completedEdit = JSON.parse(
      currentFixture.messageEdits[1]!.body.content_edit as string,
    ) as Record<string, unknown>;
    expect(completedEdit).toMatchObject({
      type: 17,
      profile: "octo/v1",
      card_version: "1.5",
      card_seq: 2,
    });
    expect(completedEdit).not.toHaveProperty("transient");
    expect(completedEdit.card).toMatchObject({
      type: "AdaptiveCard",
      version: "1.5",
      metadata: { octo_layout: "agent_progress_v1" },
    });
    expect(typeof completedEdit.plain).toBe("string");
    expect(completedEdit.plain).toContain("shell");
  });

  test("keeps complete consumer-visible fallback information in a flat card", async () => {
    fixture = await createOctoProtocolFixture({
      cardProfileBody: cardProfile({ elements: ["TextBlock"], actions: [] }),
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const result = await transport.sendProgressCard(
      { channelId: currentFixture.ownerUid, channelType: 1 },
      executionSnapshot(),
      "flat-progress",
    );

    expect(result).toEqual({ messageId: "9007199254740993" });
    const payload = currentFixture.sentMessages[0]!.body.payload as Record<string, unknown>;
    expect(typeof payload.plain).toBe("string");
    expect(payload.plain).toContain("处理进度");
    expect(payload.plain).toContain("进行中");
    expect(payload.plain).toContain("1 个阶段");
    expect(payload.plain).toContain("1 次工具调用");
    expect(payload.plain).toContain("shell");
    expect(payload.plain).toContain("Run command");
  });

  test("does not send a card when the advertised profile is disabled", async () => {
    await expectNoProgressCard(cardProfile({ enabled: false }));
  });

  test("does not send a card when Adaptive Card 1.5 is not advertised", async () => {
    await expectNoProgressCard(cardProfile({ card_version: "1.4" }));
  });

  test("does not send a card when octo/v1 is not advertised", async () => {
    await expectNoProgressCard(cardProfile({ profiles: ["octo/v2"] }));
  });

  test("does not send a card when the server disables card output", async () => {
    await expectNoProgressCard(cardProfile({
      config: {
        ...SUPPORTED_CARD_CONFIG,
        card_enabled: false,
        display_enabled: false,
      },
    }));
  });

  test("does not send a card when the server disables display output", async () => {
    await expectNoProgressCard(cardProfile({
      config: {
        ...SUPPORTED_CARD_CONFIG,
        display_enabled: false,
      },
    }));
  });

  test("does not send a card when reasoning output is disabled", async () => {
    await expectNoProgressCard(cardProfile({
      config: {
        ...SUPPORTED_CARD_CONFIG,
        reasoning_enabled: false,
        reasoning_template_ref: null,
      },
    }));
  });

  test("does not send a card when TextBlock is not advertised", async () => {
    await expectNoProgressCard(cardProfile({ elements: ["Container", "ColumnSet", "ActionSet"] }));
  });

  test("falls back when Container is not advertised", async () => {
    await expectProgressCard(cardProfile({ elements: ["TextBlock", "ColumnSet", "ActionSet"] }));
  });

  test("falls back when ColumnSet is not advertised", async () => {
    await expectProgressCard(cardProfile({ elements: ["TextBlock", "Container", "ActionSet"] }));
  });

  test("keeps the card when the optional toggle action is not advertised", async () => {
    await expectProgressCard(cardProfile({ actions: ["Action.OpenUrl"] }));
  });

  test("accepts the numeric enabled profile flag", async () => {
    await expectProgressCard(cardProfile({ enabled: 1 }));
  });

  test("does not send a card above the advertised payload byte limit", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_payload_bytes: 32 } }));
  });

  test("does not send a card above the advertised node limit", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_nodes: 3 } }));
  });

  test("does not send a card above the advertised depth limit", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_depth: 2 } }));
  });

  test("does not send a card when max_nodes is explicitly zero", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_nodes: 0 } }));
  });

  test("does not send a card when max_depth is explicitly negative", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_depth: -1 } }));
  });

  test("does not send a card when max_payload_bytes has an invalid type", async () => {
    await expectNoProgressCard(cardProfile({ limits: { max_payload_bytes: "524288" } }));
  });

  test("propagates typing and card-edit HTTP failures without chat fallback", async () => {
    fixture = await createOctoProtocolFixture({ typingStatus: 503, messageEditStatus: 500 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };

    await expect(transport.sendTyping(target)).rejects.toThrow(/503|typing/i);
    expect(currentFixture.sentMessages).toHaveLength(0);

    const receipt = await transport.sendProgressCard(
      target,
      executionSnapshot(),
      "edit-failure",
    );
    await expect(
      transport.editProgressCard(target, receipt!.messageId!, executionSnapshot("failed"), 1),
    ).rejects.toThrow(/500|edit/i);
    expect(currentFixture.sentMessages).toHaveLength(1);
    expect(currentFixture.messageEdits).toHaveLength(1);
  });

  test("propagates card-profile HTTP failures without trying a text send", async () => {
    fixture = await createOctoProtocolFixture({ cardProfileStatus: 503 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await expect(
      transport.sendProgressCard(
        { channelId: fixture.ownerUid, channelType: 1 },
        executionSnapshot(),
        "profile-failure",
      ),
    ).rejects.toThrow(/503|profile/i);
    expect(fixture.sentMessages).toHaveLength(0);
  });

  test("propagates cancellation to typing, card lookup, and card edit requests", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };

    const typingAbort = new AbortController();
    typingAbort.abort();
    await expect(transport.sendTyping(target, typingAbort.signal)).rejects.toThrow(/abort/i);
    expect(currentFixture.typingRequests).toHaveLength(0);

    const cardAbort = new AbortController();
    cardAbort.abort();
    await expect(
      transport.sendProgressCard(
        target,
        executionSnapshot("cancelled"),
        "cancelled-progress",
        cardAbort.signal,
      ),
    ).rejects.toThrow(/abort/i);
    expect(currentFixture.cardProfileRequests).toHaveLength(0);
    expect(currentFixture.sentMessages).toHaveLength(0);

    const receipt = await transport.sendProgressCard(
      target,
      executionSnapshot(),
      "edit-cancel",
    );
    const editAbort = new AbortController();
    editAbort.abort();
    await expect(
      transport.editProgressCard(
        target,
        receipt!.messageId!,
        executionSnapshot("cancelled"),
        1,
        editAbort.signal,
      ),
    ).rejects.toThrow(/abort/i);
    expect(currentFixture.messageEdits).toHaveLength(0);
  });
});

describe("Octo presigned file sending", () => {
  test("uploads a real image stream without bot auth and sends type 2 with dimensions", async () => {
    fixture = await createOctoProtocolFixture({
      presignedContentType: "image/png",
      presignedContentDisposition: 'inline; filename="photo.bin"',
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-file-test-"));
    const imagePath = join(temporaryDirectory, "photo.bin");
    const imageBytes = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=",
      "base64",
    );
    await writeFile(imagePath, imageBytes);

    const receipt = await transport.sendFile(
      { channelId: "group-images", channelType: 2 },
      imagePath,
      "image",
      "stable-image-1",
    );

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    expect(fixture.presignRequests).toHaveLength(1);
    expect(fixture.presignRequests[0]).toMatchObject({
      filename: "photo.bin",
      fileSize: imageBytes.length,
      contentType: "image/png",
      authorization: `Bearer ${fixture.botToken}`,
    });
    expect(fixture.uploadRequests).toHaveLength(1);
    expect(fixture.uploadRequests[0]).toMatchObject({
      authorization: undefined,
      contentType: "image/png",
      contentLength: String(imageBytes.length),
      contentDisposition: 'inline; filename="photo.bin"',
      accepted: true,
    });
    expect(fixture.uploadRequests[0]!.body.equals(imageBytes)).toBe(true);
    expect(fixture.sentMessages[0]).toMatchObject({
      body: {
        channel_id: "group-images",
        channel_type: 2,
        client_msg_no: "stable-image-1",
        payload: {
          type: 2,
          url: `${fixture.apiUrl}/fixture-download/1`,
          name: "photo.bin",
          size: imageBytes.length,
          width: 1,
          height: 1,
        },
      },
    });
  });

  test("uploads a regular file and sends it as type 8 with the stable id", async () => {
    fixture = await createOctoProtocolFixture({
      presignedContentType: "application/octet-stream",
      presignedContentDisposition: 'attachment; filename="notes.data"',
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-file-test-"));
    const filePath = join(temporaryDirectory, "notes.data");
    const body = Buffer.from("ordinary file body", "utf8");
    await writeFile(filePath, body);

    const receipt = await transport.sendFile(
      { channelId: "group-files", channelType: 2 },
      filePath,
      "file",
      "stable-file-1",
    );

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    expect(fixture.presignRequests[0]).toMatchObject({
      filename: "notes.data",
      fileSize: body.length,
      contentType: "application/octet-stream",
    });
    expect(fixture.uploadRequests[0]).toMatchObject({
      authorization: undefined,
      contentType: "application/octet-stream",
      contentLength: String(body.length),
      contentDisposition: 'attachment; filename="notes.data"',
      accepted: true,
    });
    expect(fixture.uploadRequests[0]!.body.equals(body)).toBe(true);
    expect(fixture.sentMessages[0]).toMatchObject({
      body: {
        channel_id: "group-files",
        channel_type: 2,
        client_msg_no: "stable-file-1",
        payload: {
          type: 8,
          url: `${fixture.apiUrl}/fixture-download/1`,
          name: "notes.data",
          size: body.length,
        },
      },
    });
  });

  test("fails safely on a presigned upload size mismatch before sending the message", async () => {
    fixture = await createOctoProtocolFixture({ uploadExpectedSizeDelta: 1 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-file-test-"));
    const filePath = join(temporaryDirectory, "notes.data");
    const body = Buffer.from("ordinary file body", "utf8");
    await writeFile(filePath, body);

    await expect(
      transport.sendFile(
        { channelId: "group-files", channelType: 2 },
        filePath,
        "file",
        "stable-file-1",
      ),
    ).rejects.toThrow(/size|mismatch|upload|403/i);
    expect(fixture.uploadRequests).toHaveLength(1);
    expect(fixture.uploadRequests[0]!.accepted).toBe(false);
    expect(fixture.sentMessages).toHaveLength(0);
  });

  test("rejects an image whose bytes are not an image MIME before presign", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    temporaryDirectory = await mkdtemp(join(tmpdir(), "octo-file-test-"));
    const invalidImagePath = join(temporaryDirectory, "not-an-image.png");
    await writeFile(invalidImagePath, Buffer.from("plain text, not PNG", "utf8"));

    await expect(
      transport.sendFile(
        { channelId: "group-images", channelType: 2 },
        invalidImagePath,
        "image",
        "invalid-image",
      ),
    ).rejects.toThrow(/image|MIME|unsupported|format/i);
    expect(fixture.presignRequests).toHaveLength(0);
    expect(fixture.sentMessages).toHaveLength(0);
  });
});
