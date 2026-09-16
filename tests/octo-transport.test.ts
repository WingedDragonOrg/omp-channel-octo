import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createOctoTransport } from "../src/octo/index";
import { buildAskCard, buildAskCardSet } from "../src/ask-card";
import type { ChannelTransport, InboundMessage } from "../src/contracts";
import type { ExecutionSnapshot } from "../src/execution-tracker";
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
    subagentCount: 0,
    subagents: [],
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
    const ownerUid = "ownerUid" in identity && typeof identity.ownerUid === "string" ? identity.ownerUid : undefined;
    expect(ownerUid).toBe(fixture.ownerUid);
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

  test("strips a verified bot mention wherever the client placed it", async () => {
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
        channelId: "group-trailing-entity",
        channelType: 2,
        text: `/status ${botPrefix}`,
        mention: {
          entities: [{ uid: fixture!.botUid, offset: "/status ".length, length: mentionLength }],
        },
        messageId: "9007199254741021",
        messageSeq: 71,
      },
      {
        fromUid: fixture!.ownerUid,
        channelId: "group-trailing-uid",
        channelType: 2,
        text: `/new /tmp ${botPrefix}`,
        mention: { uids: [fixture!.botUid] },
        messageId: "9007199254741022",
        messageSeq: 72,
      },
      {
        fromUid: fixture!.ownerUid,
        channelId: "group-inline-uid",
        channelType: 2,
        text: `清理 ${botPrefix} 这个目录`,
        mention: { uids: [fixture!.botUid] },
        messageId: "9007199254741023",
        messageSeq: 73,
      },
      {
        fromUid: fixture!.ownerUid,
        channelId: "group-trailing-uid-token",
        channelType: 2,
        text: `/status @${fixture!.botUid}`,
        mention: { uids: [fixture!.botUid] },
        messageId: "9007199254741025",
        messageSeq: 74,
      },
      {
        fromUid: fixture!.ownerUid,
        channelId: "group-unverified-uid",
        channelType: 2,
        text: `${botPrefix} /status`,
        mention: { uids: ["other-member"] },
        messageId: "9007199254741024",
        messageSeq: 75,
      },
    ]);

    await waitForFixture(
      () => received.length === 5 ? received : undefined,
      2_000,
      "fixture mention placements were not delivered",
    );

    expect(received[0]).toMatchObject({ channelId: "group-trailing-entity", text: "/status" });
    expect(received[1]).toMatchObject({ channelId: "group-trailing-uid", text: "/new /tmp" });
    expect(received[2]).toMatchObject({ channelId: "group-inline-uid", text: "清理  这个目录" });
    expect(received[3]).toMatchObject({ channelId: "group-trailing-uid-token", text: "/status" });
    expect(received[4]).toMatchObject({ channelId: "group-unverified-uid", text: `${botPrefix} /status` });
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

  test("recovers from a server kick by reconnecting with backoff", async () => {
    fixture = await createOctoProtocolFixture({
      registrationTokens: ["im-token-first", "im-token-second"],
    });
    const logs: string[] = [];
    transport = createOctoTransport({
      apiUrl: fixture.apiUrl,
      botToken: fixture.botToken,
      logger: (message) => logs.push(message),
    });
    await transport.initialize();
    const online = { count: 0 };
    const startPromise = transport.start(async () => {}, () => {
      online.count += 1;
    });
    const first = await fixture.waitForConnection(1);
    await readyConnection(first);
    await startPromise;

    fixture.kickConnection(0, 12, "connect kicked");
    const second = await fixture.waitForConnection(2, 6_000);
    await readyConnection(second);
    await waitForFixture(
      () => online.count >= 2 ? online.count : undefined,
      6_000,
      "transport did not recover from the server kick",
    );
    expect(second.connect?.token).toBe("im-token-second");
    expect(fixture.registerRequests.length).toBeGreaterThanOrEqual(2);
    expect(logs.some((line) => line.includes("reason 12") && line.includes("reconnecting"))).toBe(true);
  }, 15_000);

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
    const emptyThreadId = `${groupId}____empty-thread`;
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
        [emptyThreadId]: {
          content: "",
          version: 9,
          updatedAt: null,
          updatedBy: "",
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
      present: true,
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
      present: true,
    });
    expect(fixture.groupContextRequests[1]).toMatchObject({
      path: `/v1/bot/groups/${encodeURIComponent(groupId)}/threads/${encodeURIComponent("thread/42")}/md`,
      target: threadId,
    });

    await expect(
      transport.getGroupContext({ channelId: "empty-group", channelType: 2 }),
    ).resolves.toEqual({
      content: "",
      version: 8,
      updatedAt: null,
      updatedBy: "",
      present: false,
    });
    await expect(
      transport.getGroupContext({ channelId: emptyThreadId, channelType: 5 }),
    ).resolves.toEqual({
      content: "",
      version: 9,
      updatedAt: null,
      updatedBy: "",
      present: false,
    });
    await expect(
      transport.getGroupContext({ channelId: "missing-group", channelType: 2 }),
    ).resolves.toEqual({
      content: "",
      version: 0,
      updatedAt: null,
      updatedBy: "",
      present: false,
    });
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

describe("Octo group and thread management", () => {
  test("drives group info, threads, and thread membership through their REST paths", async () => {
    const groupId = "e3bc5af0d4b84b18bb3770a74bd222c6";
    const encodedGroup = encodeURIComponent(groupId);
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    transport = createOctoTransport({
      apiUrl: currentFixture.apiUrl,
      botToken: currentFixture.botToken,
    });
    await transport.initialize();

    await expect(transport.getGroupInfo(groupId)).resolves.toMatchObject({
      groupId,
      name: "Fixture Group",
      creator: currentFixture.ownerUid,
      status: 1,
    });
    await expect(transport.getGroupMembers(groupId)).resolves.toEqual([
      {
        uid: currentFixture.ownerUid,
        name: "Fixture Owner",
        role: 1,
        robot: false,
        joinedAt: "2026-09-12T00:00:00.000Z",
      },
      {
        uid: currentFixture.botUid,
        name: currentFixture.botName,
        role: 0,
        robot: true,
        joinedAt: "2026-09-12T00:00:00.000Z",
      },
    ]);

    await transport.updateGroupInfo(groupId, { name: "Renamed", notice: "New notice" });
    await expect(transport.getGroupInfo(groupId)).resolves.toMatchObject({
      name: "Renamed",
      notice: "New notice",
    });

    await expect(transport.createThread(groupId, "Thread one")).resolves.toEqual({
      shortId: "thread-0",
      groupId,
      channelId: `${groupId}____thread-0`,
      name: "Thread one",
      creatorUid: currentFixture.botUid,
      status: 1,
      memberCount: 1,
      messageCount: 0,
      createdAt: "2026-09-12T00:00:00.000Z",
      updatedAt: "2026-09-12T00:00:00.000Z",
    });
    await expect(transport.listThreads(groupId)).resolves.toMatchObject([
      { shortId: "thread-0", name: "Thread one" },
    ]);
    await expect(transport.getThread(groupId, "thread-0")).resolves.toMatchObject({
      shortId: "thread-0",
      name: "Thread one",
    });
    await expect(transport.getThreadMembers(groupId, "thread-0")).resolves.toEqual([
      {
        uid: currentFixture.botUid,
        name: currentFixture.botName,
        role: 1,
        robot: false,
        joinedAt: "2026-09-12T00:00:00.000Z",
      },
    ]);
    await transport.joinThread(groupId, "thread-0");
    await transport.leaveThread(groupId, "thread-0");
    await transport.deleteThread(groupId, "thread-0");
    await expect(transport.listThreads(groupId)).resolves.toEqual([]);

    expect(
      currentFixture.managementRequests.map(({ method, path }) => `${method} ${path}`),
    ).toEqual([
      `GET /v1/bot/groups/${encodedGroup}`,
      `GET /v1/bot/groups/${encodedGroup}/members`,
      `PUT /v1/bot/groups/${encodedGroup}/info`,
      `GET /v1/bot/groups/${encodedGroup}`,
      `POST /v1/bot/groups/${encodedGroup}/threads`,
      `GET /v1/bot/groups/${encodedGroup}/threads`,
      `GET /v1/bot/groups/${encodedGroup}/threads/thread-0`,
      `GET /v1/bot/groups/${encodedGroup}/threads/thread-0/members`,
      `POST /v1/bot/groups/${encodedGroup}/threads/thread-0/join`,
      `POST /v1/bot/groups/${encodedGroup}/threads/thread-0/leave`,
      `DELETE /v1/bot/groups/${encodedGroup}/threads/thread-0`,
      `GET /v1/bot/groups/${encodedGroup}/threads`,
    ]);
    expect(currentFixture.managementRequests[2]?.body).toEqual({
      name: "Renamed",
      notice: "New notice",
    });
    expect(currentFixture.managementRequests[4]?.body).toEqual({ name: "Thread one" });
    for (const request of currentFixture.managementRequests) {
      expect(request.authorization).toBe(`Bearer ${currentFixture.botToken}`);
    }
  });

  test("surfaces the server message, HTTP status, and invalid schemas", async () => {
    const forbiddenGroup = "forbidden-group";
    const brokenGroup = "broken-group";
    const currentFixture = await createOctoProtocolFixture({
      managementStatuses: { [`GET /v1/bot/groups/${forbiddenGroup}`]: 403 },
      managementBodies: {
        [`GET /v1/bot/groups/${brokenGroup}/members`]: [{ uid: 42 }],
        [`GET /v1/bot/groups/${brokenGroup}/threads`]: { threads: [] },
      },
    });
    fixture = currentFixture;
    transport = createOctoTransport({
      apiUrl: currentFixture.apiUrl,
      botToken: currentFixture.botToken,
    });
    await transport.initialize();

    await expect(transport.getGroupInfo(forbiddenGroup)).rejects.toThrow(/403/);
    await expect(transport.getGroupMembers(brokenGroup)).rejects.toThrow(/schema|invalid/i);
    await expect(transport.listThreads(brokenGroup)).rejects.toThrow(/schema|invalid/i);
    await expect(transport.getThread("plain-group", "missing")).rejects.toThrow(/400.*请求参数有误/);
    await expect(transport.createThread("bad/group", "x")).rejects.toThrow(/group id is invalid/);
    await expect(transport.getThread("plain-group", "")).rejects.toThrow(/thread id is invalid/);
  });

  test("keeps the server's machine error code for an internal store failure", async () => {
    const groupId = "store-failed-group";
    const routeKey = `DELETE /v1/bot/groups/${groupId}/threads/thread-0`;
    const currentFixture = await createOctoProtocolFixture({
      managementStatuses: { [routeKey]: 400 },
      managementBodies: {
        [routeKey]: {
          error: {
            code: "err.server.bot_api.store_failed",
            details: {},
            http_status: 500,
            message: "服务器内部错误。",
          },
          msg: "服务器内部错误。",
          status: 400,
        },
      },
    });
    fixture = currentFixture;
    transport = createOctoTransport({
      apiUrl: currentFixture.apiUrl,
      botToken: currentFixture.botToken,
    });
    await transport.initialize();

    // The wire status is the bot API's legacy compatibility 400; the body's code carries the
    // real category, and it must survive into the error the retry policy classifies.
    await expect(transport.deleteThread(groupId, "thread-0")).rejects.toThrow(
      /HTTP 400 \(err\.server\.bot_api\.store_failed\): 服务器内部错误。/u,
    );
  });

  test("reads the group 免@ decision, including the legacy no_mention shape", async () => {
    const openGroup = "no-mention-group";
    const legacyGroup = "legacy-pref-group";
    const brokenGroup = "broken-pref-group";
    const currentFixture = await createOctoProtocolFixture({
      managementBodies: {
        [`GET /v1/bot/groups/${openGroup}/mention_pref`]: {
          no_mention: 1,
          group_allow_no_mention: 1,
          effective: true,
        },
        [`GET /v1/bot/groups/${legacyGroup}/mention_pref`]: { no_mention: 1 },
        [`GET /v1/bot/groups/${brokenGroup}/mention_pref`]: { effective: "yes" },
      },
    });
    fixture = currentFixture;
    transport = createOctoTransport({
      apiUrl: currentFixture.apiUrl,
      botToken: currentFixture.botToken,
    });
    await transport.initialize();

    await expect(transport.getGroupMentionPref(openGroup)).resolves.toEqual({ effective: true });
    await expect(transport.getGroupMentionPref(legacyGroup)).resolves.toEqual({ effective: true });
    await expect(transport.getGroupMentionPref(brokenGroup)).rejects.toThrow(/schema|invalid/i);

    expect(currentFixture.managementRequests.map(({ method, path }) => `${method} ${path}`)).toEqual([
      `GET /v1/bot/groups/${openGroup}/mention_pref`,
      `GET /v1/bot/groups/${legacyGroup}/mention_pref`,
      `GET /v1/bot/groups/${brokenGroup}/mention_pref`,
    ]);
    for (const request of currentFixture.managementRequests) {
      expect(request.authorization).toBe(`Bearer ${currentFixture.botToken}`);
    }
  });

  test("spends a connection of its own on a delete that asks for one", async () => {
    const groupId = "fresh-connection-group";
    const currentFixture = await createOctoProtocolFixture();
    fixture = currentFixture;
    transport = createOctoTransport({
      apiUrl: currentFixture.apiUrl,
      botToken: currentFixture.botToken,
    });
    await transport.initialize();

    await transport.createThread(groupId, "First");
    await transport.createThread(groupId, "Second");
    await transport.deleteThread(groupId, "thread-1", undefined, { freshConnection: true });

    const [first, second, deleted] = currentFixture.managementRequests;
    expect(deleted?.path).toBe(`/v1/bot/groups/${groupId}/threads/thread-1`);
    expect(deleted?.authorization).toBe(`Bearer ${currentFixture.botToken}`);
    // The retry connection bypasses the pool, so this delete arrives on a TCP connection that
    // no earlier request used; without it the request would share the pooled connection.
    expect(deleted?.remotePort).toBeDefined();
    expect([first?.remotePort, second?.remotePort]).not.toContain(deleted?.remotePort);
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

describe("Octo interactive cards and Bot events", () => {
  const interactiveProfile = (): Record<string, unknown> => cardProfile({
    profiles: ["octo/v1", "octo/v2"],
    config: { ...SUPPORTED_CARD_CONFIG, interaction_enabled: true },
  });
  const askQuestion = {
    question: "Which deployment target?",
    options: [{ label: "staging" }, { label: "production" }],
    multi: false,
  };

  test("sends an octo/v2 ask card and rewrites it into the answered frame", async () => {
    fixture = await createOctoProtocolFixture({ cardProfileBody: interactiveProfile() });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: currentFixture.ownerUid, channelType: 1 as const };

    const receipt = await transport.sendCard(
      target,
      buildAskCard(askQuestion, { kind: "pending", generation: 1 }, "问题：Which deployment target?"),
      "ask-card-1",
    );

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    const payload = currentFixture.sentMessages[0]!.body.payload as Record<string, unknown>;
    expect(payload).toMatchObject({ type: 17, profile: "octo/v2", card_version: "1.5" });
    expect(payload.plain).toContain("Which deployment target?");
    expect((payload.card as Record<string, unknown>).actions).toMatchObject([
      { type: "Action.Submit", id: "ask_opt_1", title: "1. staging" },
      { type: "Action.Submit", id: "ask_opt_2", title: "2. production" },
      { type: "Action.Submit", id: "ask_submit", title: "提交其他答案" },
    ]);
    expect((payload.card as Record<string, unknown>).body).toContainEqual(
      expect.objectContaining({ type: "Input.Text", id: "ask_text" }),
    );

    await transport.editCard(
      target,
      receipt!.messageId!,
      buildAskCard(askQuestion, { kind: "answered", answer: "2" }, "已回复：2. production"),
      2,
    );

    expect(currentFixture.messageEdits).toHaveLength(1);
    const envelope = JSON.parse(String(currentFixture.messageEdits[0]!.body.content_edit)) as Record<string, unknown>;
    expect(envelope).toMatchObject({ type: 17, profile: "octo/v2", card_seq: 2 });
    expect((envelope.card as Record<string, unknown>).actions).toBeUndefined();
  });

  test("sends a multi-question set frame with one submit and per-question inputs", async () => {
    fixture = await createOctoProtocolFixture({ cardProfileBody: interactiveProfile() });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;

    const receipt = await transport.sendCard(
      { channelId: currentFixture.ownerUid, channelType: 1 },
      buildAskCardSet(
        [
          { question: askQuestion, answer: null },
          { question: { question: "When do we ship?", options: [], multi: false }, answer: null },
        ],
        { kind: "open", generation: 1 },
        "问题 1/2：Which deployment target?\n问题 2/2：When do we ship?",
      ),
      "ask-set-1",
    );

    expect(receipt).toEqual({ messageId: "9007199254740993" });
    const payload = currentFixture.sentMessages[0]!.body.payload as Record<string, unknown>;
    expect(payload).toMatchObject({ type: 17, profile: "octo/v2", card_version: "1.5" });
    expect(payload.plain).toContain("问题 2/2");
    const card = payload.card as Record<string, unknown>;
    expect(card.actions).toMatchObject([
      { type: "Action.Submit", id: "ask_submit", title: "提交全部答案" },
    ]);
    const inputIds = (card.body as Record<string, unknown>[])
      .map((element) => element.id)
      .filter((id) => typeof id === "string");
    expect(inputIds).toEqual(["ask_choices_1", "ask_text_1", "ask_text_2"]);
  });

  test("declines a set frame the advertised node limit cannot carry", async () => {
    fixture = await createOctoProtocolFixture({
      cardProfileBody: cardProfile({
        profiles: ["octo/v1", "octo/v2"],
        config: { ...SUPPORTED_CARD_CONFIG, interaction_enabled: true },
        limits: { max_nodes: 10 },
      }),
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    const receipt = await transport.sendCard(
      { channelId: fixture.ownerUid, channelType: 1 },
      buildAskCardSet(
        [
          { question: askQuestion, answer: null },
          { question: { question: "When do we ship?", options: [], multi: false }, answer: null },
        ],
        { kind: "open", generation: 1 },
        "问题 1/2：Which deployment target?\n问题 2/2：When do we ship?",
      ),
      "ask-set-too-big",
    );

    expect(receipt).toBeNull();
    expect(fixture.sentMessages).toHaveLength(0);
  });

  test("declines an interactive card while the deployment keeps interaction off", async () => {
    fixture = await createOctoProtocolFixture({ cardProfileBody: cardProfile() });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    const receipt = await transport.sendCard(
      { channelId: fixture.ownerUid, channelType: 1 },
      buildAskCard(askQuestion, { kind: "pending", generation: 1 }, "问题：Which deployment target?"),
      "ask-card-off",
    );

    expect(receipt).toBeNull();
    expect(fixture.sentMessages).toHaveLength(0);
  });

  test("reads card actions from the Bot event queue and acknowledges the processed id", async () => {
    fixture = await createOctoProtocolFixture({
      botEventBatches: [[
        {
          event_id: 41,
          event_type: "card_action",
          event_data: {
            message_id: "card-message",
            channel_id: "group-1",
            channel_type: 2,
            action_id: "ask_opt_2",
            operator_uid: "owner-1",
            inputs: { ask_text: "ship it" },
          },
        },
        { event_id: 42, event_type: "bot_task", event_data: {} },
        {
          event_id: 43,
          event_type: "card_action",
          event_data: {
            message_id: "card-other",
            channel_id: "group-1",
            channel_type: 9,
            action_id: "ask_opt_1",
            operator_uid: "owner-1",
          },
        },
      ]],
    });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;

    const events = await transport.fetchBotEvents({ afterEventId: 40, limit: 20, waitSeconds: 0 });

    expect(currentFixture.botEventRequests[0]).toMatchObject({
      path: "/v1/bot/events",
      authorization: `Bearer ${currentFixture.botToken}`,
      body: { event_id: 40, limit: 20, wait: 0 },
    });
    expect(events).toEqual([
      {
        eventId: 41,
        kind: "card_action",
        action: {
          messageId: "card-message",
          channelType: 2,
          channelId: "group-1",
          actionId: "ask_opt_2",
          operatorUid: "owner-1",
          inputs: { ask_text: "ship it" },
        },
      },
      { eventId: 42, kind: "other" },
      // An unknown channel type is not a click this channel could have sent.
      { eventId: 43, kind: "other" },
    ]);

    await transport.ackBotEvent(41);
    expect(currentFixture.botEventAcks).toEqual([
      { path: "/v1/bot/events/41/ack", authorization: `Bearer ${currentFixture.botToken}` },
    ]);
  });

  test("clamps the event cursor hold to what the client is willing to wait for", async () => {
    fixture = await createOctoProtocolFixture({});
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await transport.fetchBotEvents({ afterEventId: 0, limit: 500, waitSeconds: 60 });

    expect(fixture.botEventRequests[0]!.body).toMatchObject({ limit: 100, wait: 25 });
  });

  test("treats a missing Bot event endpoint as no event support", async () => {
    fixture = await createOctoProtocolFixture({ botEventsStatus: 404 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    expect(await transport.fetchBotEvents({ afterEventId: 0, limit: 20, waitSeconds: 0 })).toBeNull();
  });

  test("propagates Bot event queue and acknowledgement failures", async () => {
    fixture = await createOctoProtocolFixture({ botEventsStatus: 500, botEventAckStatus: 503 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();

    await expect(
      transport.fetchBotEvents({ afterEventId: 0, limit: 20, waitSeconds: 0 }),
    ).rejects.toThrow(/500/);
    await expect(transport.ackBotEvent(41)).rejects.toThrow(/503/);
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

/** WuKongIM carries the payload as base64-encoded JSON with the same shape as WS frames. */
function encodedHistoryPayload(payload: Record<string, unknown>): string {
  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64");
}

describe("Octo group history and inbound sequence metadata", () => {
  test("carries the decoded RECV sequence and server timestamp into InboundMessage", async () => {
    const received: InboundMessage[] = [];
    const started = await startFixtureTransport({}, async (message) => {
      received.push(message);
    });
    const currentFixture = fixture!;
    currentFixture.sendInboundBatch(started.connection, [
      {
        fromUid: currentFixture.ownerUid,
        channelId: "group-seq-carry",
        channelType: 2,
        text: "group with seq",
        messageId: "9007199254741051",
        messageSeq: 4_102,
        timestamp: 1_757_656_000,
      },
      {
        fromUid: currentFixture.ownerUid,
        channelType: 1,
        text: "dm with seq",
        messageId: "9007199254741052",
        messageSeq: 77,
        timestamp: 1_757_656_001,
      },
      {
        fromUid: currentFixture.ownerUid,
        channelId: "group-seq-unknown",
        channelType: 2,
        text: "group without seq",
        messageId: "9007199254741053",
        messageSeq: 0,
        timestamp: 1_757_656_002,
      },
    ]);

    await waitForFixture(
      () => received.length === 3 ? received : undefined,
      2_000,
      "fixture inbound messages were not delivered",
    );

    expect(received.map((message) => [
      message.channelId,
      message.messageSeq,
      message.messageTimestamp,
    ])).toEqual([
      ["group-seq-carry", 4_102, 1_757_656_000_000],
      [currentFixture.ownerUid, 77, 1_757_656_001_000],
      ["group-seq-unknown", 0, 1_757_656_002_000],
    ]);
  });

  test("posts the exact history window and decodes base64 text payloads in seq order", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    currentFixture.setMessagesSync({
      messages: [
        {
          message_seq: 14,
          from_uid: "member-b",
          timestamp: 1_757_656_005,
          payload: encodedHistoryPayload({ type: 1, content: "newer line ✅" }),
        },
        {
          message_seq: 12,
          from_uid: "member-a",
          timestamp: 1_757_656_000,
          payload: encodedHistoryPayload({ type: 1, content: "older line" }),
        },
      ],
    });

    const history = await transport.fetchGroupMessages(
      { channelId: "group-history-1", channelType: 2 },
      { sinceSeq: 11, untilSeq: 15, limit: 50 },
    );

    expect(currentFixture.messagesSyncRequests).toEqual([
      {
        path: "/v1/bot/messages/sync",
        authorization: `Bearer ${currentFixture.botToken}`,
        body: {
          channel_id: "group-history-1",
          channel_type: 2,
          start_message_seq: 11,
          end_message_seq: 15,
          limit: 50,
          pull_mode: 1,
        },
      },
    ]);
    expect(history).toEqual([
      { seq: 12, senderId: "member-a", text: "older line", timestamp: 1_757_656_000_000 },
      { seq: 14, senderId: "member-b", text: "newer line ✅", timestamp: 1_757_656_005_000 },
    ]);

    // A thread keeps its full `group____topic` id, and (0, 0) asks for the newest window.
    await transport.fetchGroupMessages(
      { channelId: "group-history-1____topic-9", channelType: 5 },
      { sinceSeq: 0, untilSeq: 0, limit: 20 },
    );

    expect(currentFixture.messagesSyncRequests[1]).toMatchObject({
      path: "/v1/bot/messages/sync",
      body: {
        channel_id: "group-history-1____topic-9",
        channel_type: 5,
        start_message_seq: 0,
        end_message_seq: 0,
        limit: 20,
        pull_mode: 1,
      },
    });
  });

  test("keeps only text from other senders and skips malformed rows individually", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const text = (content: string): string => encodedHistoryPayload({ type: 1, content });
    currentFixture.setMessagesSync({
      messages: [
        { message_seq: 28, from_uid: "member-h", timestamp: 1_757_656_008, payload: text("kept two") },
        { message_seq: 22, from_uid: currentFixture.botUid, timestamp: 1_757_656_002, payload: text("bot echo") },
        { message_seq: 20, from_uid: "member-a", timestamp: 1_757_656_000, payload: text("kept one") },
        { message_seq: 21, from_uid: "member-b", timestamp: 1_757_656_001, payload: encodedHistoryPayload({ type: 2, content: "image caption" }) },
        { message_seq: 23, from_uid: "member-c", timestamp: 1_757_656_003, payload: "%%% not base64 json %%%" },
        { message_seq: 24, from_uid: "member-d", timestamp: 1_757_656_004 },
        { message_seq: 0, from_uid: "member-e", timestamp: 1_757_656_005, payload: text("no seq") },
        { message_seq: 12.5, from_uid: "member-f", timestamp: 1_757_656_006, payload: text("fractional seq") },
        { message_seq: 25, from_uid: "member-g", timestamp: 1_757_656_007, payload: text("") },
        { message_seq: 26, from_uid: "", timestamp: 1_757_656_009, payload: text("no sender") },
        { message_seq: 27, from_uid: "member-i", timestamp: "2026-09-16T00:00:00Z", payload: text("bad timestamp") },
      ],
    });

    const history = await transport.fetchGroupMessages(
      { channelId: "group-history-2", channelType: 2 },
      { sinceSeq: 19, untilSeq: 29, limit: 50 },
    );

    expect(history).toEqual([
      { seq: 20, senderId: "member-a", text: "kept one", timestamp: 1_757_656_000_000 },
      // 图片类型没有 caption 时退回 content：历史回填复用同一张类型表取文本，但不产生附件。
      { seq: 21, senderId: "member-b", text: "image caption", timestamp: 1_757_656_001_000 },
      { seq: 28, senderId: "member-h", text: "kept two", timestamp: 1_757_656_008_000 },
    ]);
  });

  test("rejects history sync failures instead of returning an empty window", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const target = { channelId: "group-history-3", channelType: 2 as const };
    const page = { sinceSeq: 0, untilSeq: 0, limit: 20 };

    currentFixture.setMessagesSync({ messages: [] }, 500);
    await expect(transport.fetchGroupMessages(target, page)).rejects.toThrow(/500|sync/i);

    currentFixture.setMessagesSync("{not json");
    await expect(transport.fetchGroupMessages(target, page)).rejects.toThrow(/JSON|sync/i);

    currentFixture.setMessagesSync({ messages: "not-an-array" });
    await expect(transport.fetchGroupMessages(target, page)).rejects.toThrow(/schema|sync/i);

    expect(currentFixture.messagesSyncRequests).toHaveLength(3);
  });

  test("rejects history sync for DM targets without sending a request", async () => {
    fixture = await createOctoProtocolFixture();
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;

    await expect(
      transport.fetchGroupMessages(
        { channelId: currentFixture.ownerUid, channelType: 1 },
        { sinceSeq: 0, untilSeq: 0, limit: 20 },
      ),
    ).rejects.toThrow(/DM|group|history|thread/i);
    expect(currentFixture.messagesSyncRequests).toHaveLength(0);
  });

  test("aborts an in-flight history sync through its AbortSignal", async () => {
    fixture = await createOctoProtocolFixture({ messageSyncDelayMs: 100 });
    transport = createOctoTransport({ apiUrl: fixture.apiUrl, botToken: fixture.botToken });
    await transport.initialize();
    const currentFixture = fixture;
    const abort = new AbortController();
    const pending = transport.fetchGroupMessages(
      { channelId: "group-history-4", channelType: 2 },
      { sinceSeq: 0, untilSeq: 0, limit: 20, signal: abort.signal },
    );

    await waitForFixture(
      () => currentFixture.messagesSyncRequests.length === 1 ? true : undefined,
      1_000,
      "history sync request did not reach the protocol fixture",
    );
    abort.abort();
    await expect(pending).rejects.toThrow(/abort/i);
    expect(currentFixture.messagesSyncRequests).toHaveLength(1);
  });
});
