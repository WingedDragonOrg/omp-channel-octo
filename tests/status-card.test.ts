import { expect, test } from "bun:test";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { buildStatusCard } from "../src/cards/status-card.ts";
import type { ChannelStatus } from "../src/contracts.ts";
import { openChannelStore } from "../src/store/index.ts";
import { closeCoreHarness, createCoreHarness, FakeTransport, inbound } from "./fixtures/core-fakes.ts";
import type { SentCard } from "./fixtures/core-fakes.ts";

function bodyElements(card: Record<string, unknown>): Record<string, unknown>[] {
  const walk = (value: unknown): Record<string, unknown>[] => {
    if (Array.isArray(value)) return value.flatMap(walk);
    if (!value || typeof value !== "object") return [];
    const node = value as Record<string, unknown>;
    return [node, ...Object.values(node).flatMap(walk)];
  };
  return walk(card.body);
}

function bodyTexts(card: SentCard): string[] {
  return bodyElements(card.frame.card)
    .flatMap(node => typeof node.text === "string" ? [node.text] : []);
}

function statusCard(transport: FakeTransport): SentCard {
  expect(transport.sentCards).toHaveLength(1);
  return transport.sentCards[0]!;
}

async function runGit(cwd: string, ...args: string[]): Promise<string> {
  const child = Bun.spawn(["git", "-C", cwd, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: {
      ...process.env,
      GIT_CONFIG_GLOBAL: "/dev/null",
      GIT_CONFIG_NOSYSTEM: "1",
    },
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  if (exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${stderr}`);
  return stdout.trim();
}

async function createRepository(path: string, binary = false): Promise<void> {
  await mkdir(path, { recursive: true });
  await runGit(path, "init", "-q");
  await runGit(path, "config", "user.name", "Status card test");
  await runGit(path, "config", "user.email", "status-card@example.invalid");
  await writeFile(join(path, "tracked.txt"), "before\n");
  if (binary) await writeFile(join(path, "binary.dat"), Buffer.from([0, 1, 2]));
  await runGit(path, "add", "tracked.txt", ...(binary ? ["binary.dat"] : []));
  await runGit(path, "commit", "-q", "-m", "initial status snapshot");
}

test("/status uses an octo/v1 TextBlock-only card and keeps the status receipt fields", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  try {
    await harness.service.handleInbound(inbound({ messageId: "status-card-shape", text: "/status" }));

    const card = statusCard(transport);
    expect(card.frame.profile).toBe("octo/v1");
    expect(card.frame.card.type).toBe("AdaptiveCard");
    const elements = bodyElements(card.frame.card);
    expect(elements.every(({ type }) => type === "TextBlock")).toBe(true);
    for (const field of [
      "session=",
      "cwd=",
      "operation=",
      "state=",
      "goal=",
      "mode=",
      "model=",
      "liveModel=",
      "queued=",
      "outbox=",
      "unknown=",
      "pendingQuestionIds=",
    ]) {
      expect(card.frame.plain).toContain(field);
    }
    expect(bodyTexts(card).join("\n")).toContain("会话");
  } finally {
    await closeCoreHarness(harness);
  }
});

test("/status puts context first, formats freshness readably, and preserves partial native estimates", () => {
  const status: ChannelStatus = {
    sessionId: "session-context",
    cwd: "/tmp/status-context",
    state: "running",
    mode: "steer",
    model: null,
    queued: 0,
    outbox: 0,
    unknownOutbox: 0,
    pendingQuestionIds: [],
  };
  const input = {
    status,
    operation: null,
    goal: "none",
    modelPolicy: "不指定",
    liveModel: "provider/model:think",
    repositories: [],
    references: { mr: [], pr: [] },
  };
  const card = buildStatusCard({
    ...input,
    context: {
      source: "live",
      stale: false,
      usedTokens: 121_000,
      contextWindow: null,
      percent: null,
      model: "provider/model:think",
      observedAt: "2026-09-12T10:11:12.000Z",
      note: "OMP 原生上下文估算。",
    },
  });
  const visible = bodyElements(card.card)
    .flatMap(node => typeof node.text === "string" ? [node.text] : [])
    .join("\n");
  expect(visible.indexOf("会话上下文")).toBeLessThan(visible.indexOf("会话与任务"));
  expect(visible).toContain("121k / 上限未知 · 比例未知");
  expect(visible).toContain("OMP 原生估算 · 采样于 2026-09-12 10:11:12 +00:00");
  expect(visible).not.toContain("2026-09-12T10:11:12");
  expect(visible.match(/provider\/model:think/gu)).toHaveLength(1);

  const missingTokens = buildStatusCard({
    ...input,
    context: {
      source: "live",
      stale: false,
      usedTokens: null,
      contextWindow: 200_000,
      percent: null,
      model: null,
      observedAt: null,
      note: "OMP 未提供 token 使用量。",
    },
  });
  expect(bodyElements(missingTokens.card).flatMap(node => typeof node.text === "string" ? [node.text] : []).join("\n"))
    .toContain("已用未知 / 200k");
});

test("/status reports Git changes separately for every persisted workspace project", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = false;
  const harness = await createCoreHarness({ transport });
  const target = { channelType: 5 as const, channelId: "group-allowed____status-projects" };
  const entryPath = join(harness.root, "thread-workspaces", "status-projects");
  const alphaPath = join(entryPath, "projects", "alpha");
  const betaPath = join(entryPath, "projects", "beta");
  const store = await openChannelStore(harness.stateDir);
  try {
    await createRepository(alphaPath, true);
    await writeFile(join(alphaPath, "tracked.txt"), "after\n");
    await writeFile(join(alphaPath, "binary.dat"), Buffer.from([0, 1, 3]));
    await writeFile(join(alphaPath, "untracked.txt"), `${"extra line\n".repeat(40)}`);
    await createRepository(betaPath);
    store.saveThreadWorkspace({
      botUid: harness.bot.uid,
      ...target,
      layout: "shared",
      status: "ready",
      entryPath,
      projects: [
        { name: "alpha", repo: alphaPath, base: "main", path: alphaPath, branch: "status-alpha", created: true },
        { name: "beta", repo: betaPath, base: "main", path: betaPath, branch: "status-beta", created: true },
      ],
      error: null,
    });

    await harness.service.handleInbound(inbound({
      ...target,
      messageId: "status-projects",
      text: "/status",
      mentionedUserIds: [harness.bot.uid],
    }));

    const reply = await transport.waitForRemoteText(({ target: sentTarget, text }) =>
      sentTarget.channelType === target.channelType
      && sentTarget.channelId === target.channelId
      && text.includes("session="),
    );
    const visible = reply.text;
    expect(visible).toContain("alpha");
    expect(visible).toContain(alphaPath);
    expect(visible).toContain("beta");
    expect(visible).toContain(betaPath);
    expect(visible).toContain("+1");
    expect(visible).toContain("-1");
    expect(/(?:二进制|binary)[^\n]*1|1[^\n]*(?:二进制|binary)/iu.test(visible)).toBe(true);
    expect(/(?:未跟踪|untracked)[^\n]*1|1[^\n]*(?:未跟踪|untracked)/iu.test(visible)).toBe(true);
    expect(visible).not.toContain("+41");
    expect(transport.sentCards).toHaveLength(0);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("/status shows persisted PR/MR URLs only when references exist", async () => {
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ transport });
  const linked = { channelType: 5 as const, channelId: "group-allowed____status-references" };
  const empty = { channelType: 5 as const, channelId: "group-allowed____status-no-references" };
  const store = await openChannelStore(harness.stateDir);
  try {
    store.addThreadReferences(harness.bot.uid, linked, [
      { kind: "pr", url: "https://github.com/acme/service/pull/42" },
      { kind: "mr", url: "https://gitlab.example/acme/service/-/merge_requests/7" },
    ]);
    await harness.service.handleInbound(inbound({
      ...linked,
      messageId: "status-with-references",
      mentionedUserIds: [harness.bot.uid],
      text: "/status",
    }));
    await harness.service.handleInbound(inbound({
      ...empty,
      messageId: "status-without-references",
      mentionedUserIds: [harness.bot.uid],
      text: "/status",
    }));

    const linkedCard = transport.sentCards.find(card => card.target.channelId === linked.channelId);
    const emptyCard = transport.sentCards.find(card => card.target.channelId === empty.channelId);
    expect(linkedCard).toBeDefined();
    expect(emptyCard).toBeDefined();
    const linkedText = bodyTexts(linkedCard!).join("\n");
    const emptyText = bodyTexts(emptyCard!).join("\n");
    expect(linkedText).toContain(
      "[PR · https://github.com/acme/service/pull/42](https://github.com/acme/service/pull/42)",
    );
    expect(linkedText).toContain(
      "[MR · https://gitlab.example/acme/service/-/merge_requests/7](https://gitlab.example/acme/service/-/merge_requests/7)",
    );
    expect(linkedCard!.frame.plain).toContain("reference=PR · https://github.com/acme/service/pull/42");
    expect(linkedCard!.frame.plain).toContain("reference=MR · https://gitlab.example/acme/service/-/merge_requests/7");
    expect(linkedText).toContain("PR");
    expect(linkedText).toContain("https://github.com/acme/service/pull/42");

    expect(linkedText).toContain("MR");
    expect(linkedText).toContain("https://gitlab.example/acme/service/-/merge_requests/7");
    expect(emptyText).not.toMatch(/references?|\\bPR\\b|\\bMR\\b/iu);
  } finally {
    store.close();
    await closeCoreHarness(harness);
  }
});
test("status reference links keep complete hrefs and redact sensitive URL text", () => {
  const secretUrl = "https://github.com/acme/service/pull/42?token=ghp_123456abcdef";
  const longUrl = `https://gitlab.example/group/project/-/merge_requests/7?label=${"a".repeat(320)}`;
  const card = buildStatusCard({
    status: {
      sessionId: null,
      cwd: null,
      state: "none",
      mode: "steer",
      model: null,
      queued: 0,
      outbox: 0,
      unknownOutbox: 0,
      pendingQuestionIds: [],
    },
    operation: null,
    goal: "none",
    modelPolicy: "不指定",
    liveModel: "无运行中会话",
    context: {
      source: "unavailable",
      stale: false,
      usedTokens: null,
      contextWindow: null,
      percent: null,
      model: null,
      observedAt: null,
      note: "当前无绑定会话。",
    },
    repositories: [],
    references: { pr: [secretUrl], mr: [longUrl] },
  });
  const visible = bodyElements(card.card)
    .flatMap(node => typeof node.text === "string" ? [node.text] : [])
    .join("\n");
  expect(visible).toContain("[redacted]");
  expect(visible).not.toContain("ghp_123456abcdef");
  expect(visible).toContain(`](${longUrl})`);
  expect(card.plain).toContain(`reference=MR · ${longUrl}`);
});
