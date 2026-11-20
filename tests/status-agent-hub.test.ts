import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import type { ChannelService, ChatTarget } from "../src/contracts.ts";
import { createWorkerFactory } from "../src/omp/index.ts";
import { startStatusServer } from "../src/status/server.ts";
import { createCommandCatalogHarness, type CommandCatalogHarness } from "./command-catalog-test-seam";
import { inbound } from "./fixtures/core-fakes";

interface AgentHubAgent {
  id: string;
  parentId: string | null;
  agent: string;
  status: string;
  description: string | null;
  activity: unknown[];
  messages: unknown[];
}

interface AgentHubSnapshot extends ChatTarget {
  version: 1;
  agents: AgentHubAgent[];
}

type AgentHubService = Pick<
  ChannelService,
  "submitInbound" | "statusSnapshot" | "chatStatusSnapshot" | "chatDetailsSnapshot"
> & {
  chatAgentsSnapshot(target: ChatTarget, signal: AbortSignal): Promise<AgentHubSnapshot>;
};

interface AgentHubServerOptions {
  listenAddress: string;
  port: number;
  token: string;
  statusSnapshot: AgentHubService["statusSnapshot"];
  chatStatusSnapshot: AgentHubService["chatStatusSnapshot"];
  chatDetailsSnapshot: AgentHubService["chatDetailsSnapshot"];
  chatAgentsSnapshot: AgentHubService["chatAgentsSnapshot"];
}

interface AgentHubStatusServer {
  port: number;
  stop(): Promise<void>;
}

const startAgentHubStatusServer = startStatusServer as unknown as (
  options: AgentHubServerOptions,
) => Promise<AgentHubStatusServer>;
const TOKEN = "agent-hub-test-token-0123456789abcdef";
const rpcFixturePath = resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts");

type TranscriptMessage = { id: string; role: "user" | "assistant"; text: string };

async function writeOmpTranscript(
  sessionFile: string,
  options: {
    sessionId: string;
    cwd: string;
    agent?: string;
    task?: string;
    messages?: TranscriptMessage[];
  },
): Promise<void> {
  const timestamp = "2026-11-19T10:00:00.000Z";
  const entries: Array<Record<string, unknown>> = [
    { type: "session", version: 3, id: options.sessionId, timestamp, cwd: options.cwd },
  ];
  let parentId: string | null = null;
  if (options.agent !== undefined) {
    const initId = `${options.sessionId}-init`;
    entries.push({
      type: "session_init",
      id: initId,
      parentId: null,
      timestamp,
      systemPrompt: [],
      task: options.task ?? `Run ${options.agent}`,
      tools: [],
      agent: options.agent,
    });
    parentId = initId;
  }
  for (const message of options.messages ?? []) {
    entries.push({
      type: "message",
      id: message.id,
      parentId,
      timestamp,
      message: { id: message.id, role: message.role, content: [{ type: "text", text: message.text }] },
    });
    parentId = message.id;
  }
  await mkdir(dirname(sessionFile), { recursive: true });
  await writeFile(sessionFile, `${entries.map(entry => JSON.stringify(entry)).join("\n")}\n`, "utf8");
}

async function readFixtureLog(path: string): Promise<Array<Record<string, unknown>>> {
  try {
    return (await readFile(path, "utf8"))
      .split("\n")
      .filter(line => line.length > 0)
      .map(line => JSON.parse(line) as Record<string, unknown>);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}

async function waitForFixtureLog(
  path: string,
  predicate: (entries: Array<Record<string, unknown>>) => boolean,
  timeoutMs = 10_000,
): Promise<Array<Record<string, unknown>>> {
  // The RPC fixture is an external child process; observe its durable log rather than advancing fake timers.
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const entries = await readFixtureLog(path);
    if (predicate(entries)) return entries;
    await Bun.sleep(10);
  }
  throw new Error(`fixture log condition was not met within ${timeoutMs}ms`);
}

async function statusServerFor(
  harness: CommandCatalogHarness,
  token = TOKEN,
): Promise<AgentHubStatusServer> {
  const service = harness.service as unknown as AgentHubService;
  return startAgentHubStatusServer({
    listenAddress: "127.0.0.1",
    port: 0,
    token,
    statusSnapshot: () => service.statusSnapshot(),
    chatStatusSnapshot: target => service.chatStatusSnapshot(target),
    chatDetailsSnapshot: (target, signal) => service.chatDetailsSnapshot(target, signal),
    chatAgentsSnapshot: (target, signal) => service.chatAgentsSnapshot(target, signal),
  });
}

function agentsUrl(baseUrl: string, target: ChatTarget): string {
  const query = new URLSearchParams({ channelType: String(target.channelType), channelId: target.channelId });
  return `${baseUrl}/v1/chats/agents?${query}`;
}

function findAgent(snapshot: AgentHubSnapshot, id: string): AgentHubAgent | undefined {
  return snapshot.agents.find(agent => agent.id === id);
}

function messagesContain(agent: AgentHubAgent | undefined, text: string): boolean {
  return JSON.stringify(agent?.messages ?? []).includes(text);
}

test("HTTP returns the live RPC roster and complete OMP artifacts for the bound chat without changing it", async () => {
  const fixtureRoot = await mkdtemp(join(tmpdir(), "octobuddy-agent-hub-rpc-"));
  const fixtureLog = join(fixtureRoot, "worker.log");
  const gatePath = `${fixtureLog}.agent-hub`;
  const workerFactory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [
      rpcFixturePath,
      "--mode",
      "interactive",
      "--approval-mode",
      "always-ask",
      "--fixture-scenario",
      "agent-hub",
      "--fixture-log",
      fixtureLog,
    ],
    startupTimeoutMs: 5_000,
    cancelTimeoutMs: 1_500,
  });
  const target: ChatTarget = { channelType: 2, channelId: "agent-hub-live" };
  let harness: CommandCatalogHarness | undefined;
  let server: AgentHubStatusServer | undefined;
  let fixtureTurnReachedGate = false;
  try {
    harness = await createCommandCatalogHarness({ allowedGroupIds: [target.channelId], workerFactory });
    const sessionFile = join(harness.stateDir, "agent-hub-root.jsonl");
    await writeOmpTranscript(sessionFile, {
      sessionId: "fixture-session-agent-hub",
      cwd: harness.root,
    });
    harness.store.ensureBinding({
      botUid: harness.bot.uid,
      ...target,
      sessionId: "fixture-session-agent-hub",
      sessionFile,
      cwd: harness.root,
      cwdPinned: true,
    });
    const statusServer = await statusServerFor(harness);
    server = statusServer;
    const service = harness.service as unknown as AgentHubService;
    await service.submitInbound(inbound({
      ...target,
      messageId: "agent-hub-live-task",
      senderId: "owner-1",
      text: "run the fixture's live agent",
      mentionedUserIds: [harness.bot.uid],
    }));
    await waitForFixtureLog(fixtureLog, entries => entries.some(entry => entry.kind === "agent_hub_ready"));
    fixtureTurnReachedGate = true;
    expect(existsSync(gatePath)).toBe(true);

    const baseUrl = `http://127.0.0.1:${statusServer.port}`;
    const url = agentsUrl(baseUrl, target);
    const beforeRead = {
      binding: harness.store.getBinding(harness.bot.uid, target),
      latestTask: harness.store.getLatestTask(harness.bot.uid, target),
      ledger: harness.store.listWorkerLedger(harness.bot.uid),
    };

    const unauthorized = await fetch(url);
    expect(unauthorized.status).toBe(401);
    expect((await readFixtureLog(fixtureLog)).filter(entry => {
      const command = entry.command as Record<string, unknown> | undefined;
      return entry.kind === "command" && command?.type === "get_subagents";
    })).toHaveLength(0);

    const response = await fetch(url, { headers: { authorization: `Bearer ${TOKEN}` } });
    expect(response.status).toBe(200);
    const snapshot = await response.json() as AgentHubSnapshot;
    expect(snapshot).toMatchObject({ version: 1, ...target });
    const root = findAgent(snapshot, "main");
    const live = findAgent(snapshot, "live-agent");
    const nested = findAgent(snapshot, "nested-helper");
    const rootAdvisor = findAgent(snapshot, "Main/advisor");
    const nestedAdvisor = findAgent(snapshot, "live-agent/advisor:audit");
    expect(messagesContain(root, "root conversation reply")).toBe(true);
    expect(JSON.stringify(root?.activity)).toContain("root conversation reply");
    expect(live).toMatchObject({ parentId: null, agent: "scout", status: "running", description: "Live scout" });
    expect(messagesContain(live, "live child reply")).toBe(true);
    expect(JSON.stringify(live?.activity)).toContain("live child reply");
    expect(JSON.stringify(live?.activity)).toContain("waiting for the read-only hub");
    expect(nested).toMatchObject({ parentId: "live-agent", agent: "reviewer", status: "parked" });
    expect(messagesContain(nested, "nested child reply")).toBe(true);
    expect(rootAdvisor).toMatchObject({ parentId: null, status: "parked" });
    expect(messagesContain(rootAdvisor, "root advisor reply")).toBe(true);
    expect(nestedAdvisor).toMatchObject({ parentId: "live-agent", status: "parked" });
    expect(messagesContain(nestedAdvisor, "nested advisor reply")).toBe(true);
    expect(snapshot.agents.every(agent => Array.isArray(agent.activity) && Array.isArray(agent.messages))).toBe(true);

    const afterRead = {
      binding: harness.store.getBinding(harness.bot.uid, target),
      latestTask: harness.store.getLatestTask(harness.bot.uid, target),
      ledger: harness.store.listWorkerLedger(harness.bot.uid),
    };
    expect(afterRead).toEqual(beforeRead);
    const logAfterGet = await readFixtureLog(fixtureLog);
    const getSubagentsCount = logAfterGet.filter(entry => {
      const command = entry.command as Record<string, unknown> | undefined;
      return entry.kind === "command" && command?.type === "get_subagents";
    }).length;
    const getMessagesCount = logAfterGet.filter(entry => {
      const command = entry.command as Record<string, unknown> | undefined;
      return entry.kind === "command" && command?.type === "get_subagent_messages";
    }).length;
    expect(getSubagentsCount).toBe(1);
    expect(getMessagesCount).toBe(1);
    expect(logAfterGet.filter(entry => entry.kind === "prompt")).toHaveLength(1);
    expect(logAfterGet.filter(entry => entry.kind === "startup")).toHaveLength(1);

    const post = await fetch(url, { method: "POST", headers: { authorization: `Bearer ${TOKEN}` } });
    expect(post.status).toBe(405);
    const logAfterPost = await readFixtureLog(fixtureLog);
    expect(logAfterPost.filter(entry => {
      const command = entry.command as Record<string, unknown> | undefined;
      return entry.kind === "command" && command?.type === "get_subagents";
    })).toHaveLength(getSubagentsCount);
    expect(logAfterPost.filter(entry => {
      const command = entry.command as Record<string, unknown> | undefined;
      return entry.kind === "command" && command?.type === "get_subagent_messages";
    })).toHaveLength(getMessagesCount);
    expect(logAfterPost.filter(entry => entry.kind === "prompt")).toHaveLength(1);
  } finally {
    await rm(gatePath, { force: true });
    if (fixtureTurnReachedGate) {
      await waitForFixtureLog(fixtureLog, entries => entries.some(entry => entry.kind === "turn_complete"), 5_000).catch(() => {});
    }
    await server?.stop();
    await harness?.close();
    await rm(fixtureRoot, { recursive: true, force: true });
  }
});

test("HTTP keeps persisted agent, nested-agent, and advisor histories isolated by bound root session without starting workers", async () => {
  const harness = await createCommandCatalogHarness({ allowedGroupIds: ["agent-hub-history-a", "agent-hub-history-b"] });
  let server: AgentHubStatusServer | undefined;
  const targets: Array<{ target: ChatTarget; sessionFile: string; sessionId: string; label: "A" | "B" }> = [
    {
      target: { channelType: 2, channelId: "agent-hub-history-a" },
      sessionFile: join(harness.stateDir, "history-a.jsonl"),
      sessionId: "history-session-a",
      label: "A",
    },
    {
      target: { channelType: 2, channelId: "agent-hub-history-b" },
      sessionFile: join(harness.stateDir, "history-b.jsonl"),
      sessionId: "history-session-b",
      label: "B",
    },
  ];
  try {
    for (const item of targets) {
      await writeOmpTranscript(item.sessionFile, {
        sessionId: item.sessionId,
        cwd: harness.root,
        messages: [
          { id: `${item.label}-root-user`, role: "user", text: `${item.label} root input` },
          { id: `${item.label}-root-assistant`, role: "assistant", text: `${item.label} root reply` },
        ],
      });
      // OMP persists children under the root .jsonl basename, nesting descendants in a
      // directory named for their parent agent ID; advisor transcripts use reserved names.
      const artifactRoot = item.sessionFile.slice(0, -".jsonl".length);
      await writeOmpTranscript(join(artifactRoot, "shared-agent.jsonl"), {
        sessionId: `${item.sessionId}-child`,
        cwd: harness.root,
        agent: "scout",
        task: `Inspect history ${item.label}`,
        messages: [
          { id: `${item.label}-child-user`, role: "user", text: `${item.label} child input` },
          { id: `${item.label}-child-assistant`, role: "assistant", text: `${item.label} child reply` },
        ],
      });
      await writeOmpTranscript(join(artifactRoot, "shared-agent", "nested-helper.jsonl"), {
        sessionId: `${item.sessionId}-nested`,
        cwd: harness.root,
        agent: "reviewer",
        task: `Review history ${item.label}`,
        messages: [{ id: `${item.label}-nested-assistant`, role: "assistant", text: `${item.label} nested reply` }],
      });
      await writeOmpTranscript(join(artifactRoot, "__advisor.audit.jsonl"), {
        sessionId: `${item.sessionId}-root-advisor`,
        cwd: harness.root,
        agent: "advisor",
        task: `Observe root ${item.label}`,
        messages: [{ id: `${item.label}-advisor-assistant`, role: "assistant", text: `${item.label} advisor reply` }],
      });
      await writeOmpTranscript(join(artifactRoot, "shared-agent", "__advisor.audit.jsonl"), {
        sessionId: `${item.sessionId}-nested-advisor`,
        cwd: harness.root,
        agent: "advisor",
        task: `Observe child ${item.label}`,
        messages: [{ id: `${item.label}-nested-advisor-assistant`, role: "assistant", text: `${item.label} nested advisor reply` }],
      });
      harness.store.ensureBinding({
        botUid: harness.bot.uid,
        ...item.target,
        sessionId: item.sessionId,
        sessionFile: item.sessionFile,
        cwd: harness.root,
        cwdPinned: true,
      });
    }
    const statusServer = await statusServerFor(harness);
    server = statusServer;
    const baseUrl = `http://127.0.0.1:${statusServer.port}`;
    const snapshots: AgentHubSnapshot[] = [];
    for (const item of [targets[0]!, targets[1]!, targets[0]!]) {
      const response = await fetch(agentsUrl(baseUrl, item.target), {
        headers: { authorization: `Bearer ${TOKEN}` },
      });
      expect(response.status).toBe(200);
      snapshots.push(await response.json() as AgentHubSnapshot);
    }
    const [snapshotA, snapshotB, snapshotAAgain] = snapshots as [AgentHubSnapshot, AgentHubSnapshot, AgentHubSnapshot];
    expect(snapshotA?.channelId).toBe(targets[0]!.target.channelId);
    expect(snapshotB?.channelId).toBe(targets[1]!.target.channelId);
    expect(snapshotAAgain).toEqual(snapshotA);

    for (const [snapshot, label] of [[snapshotA, "A"], [snapshotB, "B"]] as const) {
      expect(messagesContain(findAgent(snapshot, "main"), `${label} root reply`)).toBe(true);
      const child = findAgent(snapshot, "shared-agent");
      expect(child).toMatchObject({ parentId: null, agent: "scout", status: "parked" });
      expect(JSON.stringify(child?.activity)).toContain(`${label} child reply`);
      expect(messagesContain(child, `${label} child reply`)).toBe(true);
      expect(messagesContain(child, label === "A" ? "B child reply" : "A child reply")).toBe(false);
      const nested = findAgent(snapshot, "nested-helper");
      expect(nested).toMatchObject({ parentId: "shared-agent", agent: "reviewer", status: "parked" });
      expect(messagesContain(nested, `${label} nested reply`)).toBe(true);
      const advisor = findAgent(snapshot, "Main/advisor:audit");
      expect(advisor).toMatchObject({ parentId: null, status: "parked" });
      expect(messagesContain(advisor, `${label} advisor reply`)).toBe(true);
      const nestedAdvisor = findAgent(snapshot, "shared-agent/advisor:audit");
      expect(nestedAdvisor).toMatchObject({ parentId: "shared-agent", status: "parked" });
      expect(messagesContain(nestedAdvisor, `${label} nested advisor reply`)).toBe(true);
    }

    expect(harness.workers.creations).toEqual([]);
    for (const item of targets) {
      expect(harness.store.getLatestTask(harness.bot.uid, item.target)).toBeNull();
      expect(harness.store.getBinding(harness.bot.uid, item.target)?.sessionFile).toBe(item.sessionFile);
    }
  } finally {
    await server?.stop();
    await harness.close();
  }
});
