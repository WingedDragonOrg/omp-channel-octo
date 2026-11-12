import { createServer, type ServerResponse } from "node:http";
import { chmod, mkdtemp, mkdir, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "bun:test";
import { createOwnedCdpFixture, writePlugin, type OwnedCdpFixture } from "./fixtures/host.ts";

type HostStartOptions = {
  appPath: string;
  port: number;
  pid: number;
  startedAt: string;
  pluginDirectories: string[];
  statusConfigPath?: string;
};

type HostSession = {
  stop(): Promise<void>;
  setPluginEnabled(pluginId: string, enabled: boolean): Promise<void>;
};

type HostEntry = { startHost(options: HostStartOptions): Promise<HostSession> };
type StatusSnapshot = {
  version: 1;
  pid: number;
  botUid: string | null;
  ownerUid: string | null;
  runningSessions: number;
  queuedTasks: number;
  sampledAt: string;
};
type ChatStatusTarget = { channelType: 1 | 2 | 5; channelId: string };
type ChatStatusSnapshot = {
  version: 1;
  sampledAt: string;
  channelType: 1 | 2 | 5;
  channelId: string;
  state: "none" | "idle" | "queued" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
  model: string | null;
  cwd: string | null;
  queuedTasks: number;
  pendingQuestions: number;
};

type ChatDetailsSnapshot = {
  version: 1;
  sampledAt: string;
  channelType: 1 | 2 | 5;
  channelId: string;
  context: {
    source: "live" | "persisted" | "unavailable";
    stale: boolean;
    usedTokens: number | null;
    contextWindow: number | null;
    percent: number | null;
    model: string | null;
    observedAt: string | null;
    note: string;
  };
  references: { pr: string[]; mr: string[] };
  tasks: { workspaceId: string; taskId: string; title: string | null; status: string | null; url: string | null }[];
};


type StatusRequest = { method: string; path: string; authorization: string | undefined };

type LocalStatusServer = {
  baseUrl: string;
  requests(): StatusRequest[];
  chatRequests(): StatusRequest[];
  detailsRequests(): StatusRequest[];
  releaseChatResponse(target: ChatStatusTarget): void;
  pendingChatResponse(target: ChatStatusTarget): boolean;
  abortedResponse(): boolean;
  abortedChatResponse(): boolean;
  abortedChatTarget(target: ChatStatusTarget): boolean;
  releaseDetailsResponse(target: ChatStatusTarget): void;
  pendingDetailsResponse(target: ChatStatusTarget): boolean;
  abortedDetailsResponse(): boolean;
  abortedDetailsTarget(target: ChatStatusTarget): boolean;
  close(): Promise<void>;
};

type StatusHost = {
  appPath: string;
  fixture: OwnedCdpFixture;
  host: HostSession;
  server: LocalStatusServer;
};

const bearerToken = "status-bridge-acceptance-token-0123456789abcdef";
const snapshot: StatusSnapshot = {
  version: 1,
  pid: 47291,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 6,
  queuedTasks: 3,
  sampledAt: "2026-11-11T10:14:30.000Z",
};

async function loadHostEntry(): Promise<HostEntry> {
  // Dynamic loading keeps missing host capabilities observable as runtime test failures, not import/type errors.
  const entry: unknown = await import("../src/host/index.ts");
  if (!entry || typeof entry !== "object" || !("startHost" in entry) || typeof entry.startHost !== "function") {
    throw new Error("Host behavior entry must export startHost(options)");
  }
  return { startHost: entry.startHost as HostEntry["startHost"] };
}

async function createFakeAppBundle(parent: string): Promise<string> {
  const appPath = join(parent, "OctoBuddy.app");
  const pages = [
    "Contents/Resources/communication-renderer/index.html",
    "Contents/Resources/app.asar/out/renderer/index.html",
    "Contents/Resources/settings/index.html",
  ];
  for (const page of pages) {
    const path = join(appPath, page);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, "<!doctype html><html></html>");
  }
  return await realpath(appPath);
}

const communicationUrl = (appPath: string) =>
  pathToFileURL(join(appPath, "Contents/Resources/communication-renderer/index.html")).href;
const conversationUrl = (appPath: string) =>
  `${pathToFileURL(join(appPath, "Contents/Resources/app.asar/out/renderer/index.html")).href}#/conversation`;
const settingsUrl = (appPath: string) =>
  pathToFileURL(join(appPath, "Contents/Resources/settings/index.html")).href;

function chatStatusKey(target: ChatStatusTarget): string {
  return `${target.channelType}:${target.channelId}`;
}

function chatSnapshotFor(target: ChatStatusTarget): ChatStatusSnapshot {
  const thread = target.channelType === 5;
  return {
    version: 1,
    sampledAt: snapshot.sampledAt,
    channelType: target.channelType,
    channelId: target.channelId,
    state: thread ? "queued" : "running",
    model: `model-${target.channelId}`,
    cwd: `/workspace/${target.channelId}`,
    queuedTasks: thread ? 2 : 0,
    pendingQuestions: thread ? 1 : 0,
  };
}

function chatDetailsSnapshotFor(target: ChatStatusTarget): ChatDetailsSnapshot {
  const thread = target.channelType === 5;
  return {
    version: 1,
    sampledAt: snapshot.sampledAt,
    channelType: target.channelType,
    channelId: target.channelId,
    context: {
      source: "live",
      stale: false,
      usedTokens: 12_345,
      contextWindow: 200_000,
      percent: 6.17,
      model: `model-${target.channelId}`,
      observedAt: snapshot.sampledAt,
      note: "Current live worker",
    },
    references: {
      pr: thread ? ["https://github.com/example/project/pull/42"] : [],
      mr: thread ? ["https://gitlab.com/example/project/-/merge_requests/7"] : [],
    },
    tasks: thread ? [{
      workspaceId: "workspace-17",
      taskId: "TASK-42",
      title: "Persist callback",
      status: "in_progress",
      url: "https://loop.example/tasks/TASK-42",
    }] : [],
  };
}


function createLocalStatusServer(options: {
  holdResponse?: boolean;
  holdChatTargets?: ChatStatusTarget[];
  holdDetailsTargets?: ChatStatusTarget[];
} = {}): Promise<LocalStatusServer> {
  const requests: StatusRequest[] = [];
  const chatRequests: StatusRequest[] = [];
  const detailsRequests: StatusRequest[] = [];
  const heldChatTargets = new Set((options.holdChatTargets ?? []).map(chatStatusKey));
  const heldDetailsTargets = new Set((options.holdDetailsTargets ?? []).map(chatStatusKey));
  const pendingChatResponses = new Map<string, ServerResponse[]>();
  const pendingDetailsResponses = new Map<string, ServerResponse[]>();
  let didAbortResponse = false;
  let didAbortChatResponse = false;
  let didAbortDetailsResponse = false;
  const abortedChatTargets = new Set<string>();
  const abortedDetailsTargets = new Set<string>();
  const server = createServer((request, response) => {
    const path = request.url ?? "/";
    requests.push({
      method: request.method ?? "",
      path,
      authorization: request.headers.authorization,
    });
    const url = new URL(path, "http://127.0.0.1");
    if (request.method !== "GET" || (url.pathname !== "/v1/status" && url.pathname !== "/v1/chats/status"
      && url.pathname !== "/v1/chats/details")) {
      response.writeHead(404).end();
      return;
    }
    if (request.headers.authorization !== `Bearer ${bearerToken}`) {
      response.writeHead(401, { "content-type": "application/json" }).end(JSON.stringify({ error: { code: "unauthorized" } }));
      return;
    }
    if (url.pathname === "/v1/status") {
      if (options.holdResponse) {
        response.on("close", () => {
          if (!response.writableEnded) didAbortResponse = true;
        });
        return;
      }
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify(snapshot));
      return;
    }
    const channelType = Number(url.searchParams.get("channelType"));
    const channelId = url.searchParams.get("channelId");
    if (![1, 2, 5].includes(channelType) || channelId === null) {
      response.writeHead(400, { "content-type": "application/json" }).end(JSON.stringify({ error: { code: "invalid_request" } }));
      return;
    }
    const target = { channelType: channelType as ChatStatusTarget["channelType"], channelId };
    if (url.pathname === "/v1/chats/status") {
      chatRequests.push({ method: request.method ?? "", path, authorization: request.headers.authorization });
    } else {
      detailsRequests.push({ method: request.method ?? "", path, authorization: request.headers.authorization });
    }
    const key = chatStatusKey(target);
    if (url.pathname === "/v1/chats/status" && heldChatTargets.has(key)) {
      const responses = pendingChatResponses.get(key) ?? [];
      responses.push(response);
      pendingChatResponses.set(key, responses);
      response.on("close", () => {
        if (response.writableEnded) return;
        didAbortChatResponse = true;
        abortedChatTargets.add(key);
        const remaining = pendingChatResponses.get(key)?.filter((pending) => pending !== response) ?? [];
        if (remaining.length === 0) pendingChatResponses.delete(key);
        else pendingChatResponses.set(key, remaining);
      });
      return;
    }
    if (url.pathname === "/v1/chats/details" && heldDetailsTargets.has(key)) {
      const responses = pendingDetailsResponses.get(key) ?? [];
      responses.push(response);
      pendingDetailsResponses.set(key, responses);
      response.on("close", () => {
        if (response.writableEnded) return;
        didAbortDetailsResponse = true;
        abortedDetailsTargets.add(key);
        const remaining = pendingDetailsResponses.get(key)?.filter((pending) => pending !== response) ?? [];
        if (remaining.length === 0) pendingDetailsResponses.delete(key);
        else pendingDetailsResponses.set(key, remaining);
      });
      return;
    }
    response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(url.pathname === "/v1/chats/details"
      ? chatDetailsSnapshotFor(target)
      : chatSnapshotFor(target)));
  });
  const { promise, resolve, reject } = Promise.withResolvers<LocalStatusServer>();
  server.once("error", reject);
  server.listen(0, "127.0.0.1", () => {
    server.off("error", reject);
    const address = server.address();
    if (!address || typeof address === "string") {
      reject(new Error("Local status server did not bind a TCP port"));
      return;
    }
    resolve({
      baseUrl: `http://127.0.0.1:${address.port}`,
      requests: () => [...requests],
      chatRequests: () => [...chatRequests],
      detailsRequests: () => [...detailsRequests],
      releaseChatResponse: (target) => {
        const key = chatStatusKey(target);
        const response = pendingChatResponses.get(key)?.shift();
        if (!response) throw new Error(`No held chat status response for ${key}`);
        if (pendingChatResponses.get(key)?.length === 0) pendingChatResponses.delete(key);
        response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
        response.end(JSON.stringify(chatSnapshotFor(target)));
      },
      pendingChatResponse: (target) => (pendingChatResponses.get(chatStatusKey(target)) ?? [])
        .some((response) => !response.writableEnded),
      abortedChatTarget: (target) => abortedChatTargets.has(chatStatusKey(target)),
      abortedResponse: () => didAbortResponse,
      abortedChatResponse: () => didAbortChatResponse,
      releaseDetailsResponse: (target) => {
        const key = chatStatusKey(target);
        const response = pendingDetailsResponses.get(key)?.shift();
        if (!response) throw new Error(`No held chat details response for ${key}`);
        if (pendingDetailsResponses.get(key)?.length === 0) pendingDetailsResponses.delete(key);
        response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
        response.end(JSON.stringify(chatDetailsSnapshotFor(target)));
      },
      pendingDetailsResponse: (target) => (pendingDetailsResponses.get(chatStatusKey(target)) ?? [])
        .some((response) => !response.writableEnded),
      abortedDetailsResponse: () => didAbortDetailsResponse,
      abortedDetailsTarget: (target) => abortedDetailsTargets.has(chatStatusKey(target)),
      close: async () => {
        server.closeAllConnections();
        const closed = Promise.withResolvers<void>();
        server.close((error) => error ? closed.reject(error) : closed.resolve());
        await closed.promise;
      },
    });
  });
  return promise;
}

async function withStatusHost(
  options: {
    holdResponse?: boolean;
    holdChatTargets?: ChatStatusTarget[];
    holdDetailsTargets?: ChatStatusTarget[];
    preparePlugins?: (pluginRoot: string) => Promise<void>;
    prepareTargets?: (fixture: OwnedCdpFixture, root: string, appPath: string) => Promise<void>;
  },
  exercise: (setup: StatusHost) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-status-bridge-"));
  let fixture: OwnedCdpFixture | undefined;
  let host: HostSession | undefined;
  let server: LocalStatusServer | undefined;
  try {
    const appPath = await createFakeAppBundle(root);
    fixture = await createOwnedCdpFixture({ runMainWorldScripts: false });
    server = await createLocalStatusServer({
      holdResponse: options.holdResponse ?? false,
      ...(options.holdChatTargets !== undefined ? { holdChatTargets: options.holdChatTargets } : {}),
      ...(options.holdDetailsTargets !== undefined ? { holdDetailsTargets: options.holdDetailsTargets } : {}),
    });
    const pluginRoot = join(root, "extra-plugins");
    await mkdir(pluginRoot, { recursive: true });
    await options.preparePlugins?.(pluginRoot);
    await options.prepareTargets?.(fixture, root, appPath);

    const statusConfigPath = join(root, "status.json");
    await writeFile(statusConfigPath, JSON.stringify({ baseUrl: server.baseUrl, token: bearerToken }), { mode: 0o600 });
    await chmod(statusConfigPath, 0o600);
    const configMode = (await stat(statusConfigPath)).mode & 0o777;
    expect(configMode).toBe(0o600);

    const { startHost } = await loadHostEntry();
    host = await startHost({
      appPath,
      port: fixture.port,
      pid: fixture.pid,
      startedAt: fixture.startedAt,
      pluginDirectories: [join(import.meta.dir, "..", "plugins"), pluginRoot],
      statusConfigPath,
    });
    await exercise({ appPath, fixture, host, server });
  } finally {
    try { await host?.stop(); }
    finally {
      try { await fixture?.close(); }
      finally {
        try { await server?.close(); }
        finally { await rm(root, { recursive: true, force: true }); }
      }
    }
  }
}


async function waitUntil(predicate: () => boolean | Promise<boolean>, description: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  if (await predicate()) return;
  throw new Error(`Timed out waiting for ${description}`);
}

async function statusContext(fixture: OwnedCdpFixture, targetId: string) {
  return (await fixture.contexts(targetId)).find((context) =>
    !context.isDefault && context.worldName.includes("channel-status"));
}

// The owned CDP fixture's synthetic location omits protocol; patch the isolated context before replaying the packaged source.
async function activatePackagedStatusPlugin(fixture: OwnedCdpFixture, targetId: string): Promise<void> {
  const context = await statusContext(fixture, targetId);
  if (!context) throw new Error("The packaged status plugin has no authorized isolated-world context");
  const capability = await fixture.evaluate(targetId, context.id, "typeof window.octoEnhancer?.getChannelStatus");
  expect(capability).toBe("function");
  const source = await readFile(new URL("../plugins/channel-status/content.js", import.meta.url), "utf8");
  await fixture.evaluate(targetId, context.id, `window.location.protocol = "file:";\n${source}`);
}
async function readPanel(fixture: OwnedCdpFixture, targetId: string, contextId: number): Promise<{
  state: string | undefined;
  expanded: string | null;
  hidden: boolean | undefined;
  text: string;
}> {
  return await fixture.evaluate(targetId, contextId, `(() => {
    const root = document.querySelector("[data-octobuddy-channel-status-root]");
    const button = root?.querySelector("[data-octobuddy-channel-status-button]");
    const panel = document.getElementById("octobuddy-channel-status-panel");
    const text = (node) => [node.textContent || "", ...node.children.map(text)].join(" ");
    return {
      state: root?.dataset.state,
      expanded: button?.getAttribute("aria-expanded") ?? null,
      hidden: panel?.hidden,
      text: panel ? text(panel) : "",
    };
  })()`) as {
    state: string | undefined;
    expanded: string | null;
    hidden: boolean | undefined;
    text: string;
  };
}

test("the packaged status plugin displays the real local snapshot only on its communication target", async () => {
  await withStatusHost({
    prepareTargets: async (fixture, root, appPath) => {
      const forgedAppPath = await createFakeAppBundle(join(root, "forged"));
      await fixture.addTarget({ id: "status-communication-view", url: communicationUrl(appPath), title: "Octo Communication" });
      await fixture.addTarget({ id: "status-conversation-view", url: conversationUrl(appPath), title: "OctoBuddy conversation" });
      await fixture.addTarget({ id: "status-settings-view", url: settingsUrl(appPath), title: "OctoBuddy settings" });
      await fixture.addTarget({ id: "status-forged-view", url: communicationUrl(forgedAppPath), title: "Forged Octo Communication" });
    },
  }, async ({ fixture, server }) => {
    const targetId = "status-communication-view";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the channel-status isolated world");
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    await activatePackagedStatusPlugin(fixture, targetId);
    await waitUntil(() => server.requests().length >= 1, "the real localhost status request");
    await waitUntil(async () => (await readPanel(fixture, targetId, context!.id)).state === "online", "the live status button");

    const initialPanel = await readPanel(fixture, targetId, context!.id);
    expect(initialPanel.state).toBe("online");
    expect(initialPanel.expanded).toBe("false");
    expect(await fixture.click(targetId, context!.id, "[data-octobuddy-channel-status-button]", true)).toBe(true);
    await waitUntil(async () => {
      const panel = await readPanel(fixture, targetId, context!.id);
      return panel.hidden === false && panel.text.includes("47291");
    }, "the expanded panel to display the local service snapshot");
    const panel = await readPanel(fixture, targetId, context!.id);
    expect(panel.text).toContain("47291");
    expect(panel.text).toContain("6");
    expect(panel.text).toContain("3");
    expect(server.requests().every((request) => request.authorization === `Bearer ${bearerToken}`)).toBe(true);
    expect(server.requests().every((request) => request.path === "/v1/status" && request.method === "GET")).toBe(true);

    expect((await fixture.effects("status-conversation-view", "status-conversation-view-main")).elementIds).not.toContain("octobuddy-channel-status-panel");
    expect((await fixture.effects("status-settings-view", "status-settings-view-main")).elementIds).not.toContain("octobuddy-channel-status-panel");
    expect(await fixture.contexts("status-forged-view")).toEqual([]);
  });
}, 15_000);

test("the status navigation script is registered in the page world and disposed during disable and host stop", async () => {
  await withStatusHost({
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-navigation-lifecycle",
      url: communicationUrl(appPath),
    }),
  }, async ({ appPath, fixture, host }) => {
    const targetId = "status-navigation-lifecycle";
    const frameId = `${targetId}-main`;
    const mainWorld = () => fixture.context(targetId, frameId);
    const navigationScripts = async () => (await fixture.calls(targetId)).filter((call) =>
      call.method === "Page.addScriptToEvaluateOnNewDocument" && !("worldName" in call.params));
    const seedNavigationDisposer = (contextId: number) => fixture.evaluate(targetId, contextId, `(() => {
      window.__octobuddyStatusNavigation = {
        dispose() {
          const count = Number(document.documentElement.dataset.statusNavigationDisposeCount || "0") + 1;
          document.documentElement.dataset.statusNavigationDisposeCount = String(count);
          delete window.__octobuddyStatusNavigation;
        },
      };
      return true;
    })()`);

    await waitUntil(async () => (await mainWorld()) !== undefined, "the communication main world");
    const initialContext = await mainWorld();
    expect(initialContext?.isDefault).toBe(true);
    expect(await navigationScripts()).toHaveLength(1);

    await seedNavigationDisposer(initialContext!.id);
    await host.setPluginEnabled("channel-status", false);
    expect((await fixture.effects(targetId, frameId)).dataset.statusNavigationDisposeCount).toBe("1");

    await fixture.navigate(targetId, frameId, communicationUrl(appPath));
    await fixture.drain();
    await waitUntil(async () => (await mainWorld()) !== undefined, "the disabled communication document");
    const disabledContext = await mainWorld();
    expect(await fixture.evaluate(targetId, disabledContext!.id, "typeof window.__octobuddyStatusNavigation")).toBe("undefined");
    expect(await navigationScripts()).toHaveLength(1);

    await host.setPluginEnabled("channel-status", true);
    expect(await navigationScripts()).toHaveLength(2);
    await fixture.navigate(targetId, frameId, communicationUrl(appPath));
    await fixture.drain();
    await waitUntil(async () => (await mainWorld()) !== undefined, "the re-enabled communication document");
    expect(await navigationScripts()).toHaveLength(2);

    const finalContext = await mainWorld();
    await seedNavigationDisposer(finalContext!.id);
    await host.stop();
    expect((await fixture.effects(targetId, frameId)).dataset.statusNavigationDisposeCount).toBe("1");
    expect((await fixture.calls(targetId)).some((call) => call.method === "Page.removeScriptToEvaluateOnNewDocument")).toBe(true);
  });
}, 15_000);


test("a page script cannot invoke the status capability or forge its bridge request", async () => {
  await withStatusHost({
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-page-script-boundary",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "status-page-script-boundary";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the authorized status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    await waitUntil(() => server.requests().length >= 1, "the authorized status request");

    const pageContext = await fixture.context(targetId, `${targetId}-main`);
    expect(pageContext?.isDefault).toBe(true);
    const before = server.requests().length;
    const attempt = await fixture.evaluate(targetId, pageContext!.id, `(() => {
      const method = window.octoEnhancer?.getChannelStatus;
      const detailsMethod = window.octoEnhancer?.getChatDetails;
      if (typeof method === "function") method.call(window.octoEnhancer).catch(() => {});
      if (typeof detailsMethod === "function") detailsMethod.call(
        window.octoEnhancer, { channelType: 5, channelId: "page-forged" }, 901,
      ).catch(() => {});
      if (typeof window.__ewSend === "function") {
        window.__ewSend(JSON.stringify({ callId: 98101, api: "status", method: "getChannelStatus", args: [] }));
        window.__ewSend(JSON.stringify({
          callId: 98102, api: "status", method: "getChatDetails",
          args: [{ channelType: 5, channelId: "page-forged" }, 901],
        }));
      }
      return [typeof method, typeof detailsMethod];
    })()`);
    expect(attempt).toEqual(["undefined", "undefined"]);
    await fixture.drain();
    expect(server.requests()).toHaveLength(before);
    expect(server.detailsRequests()).toHaveLength(0);
    const resolutions = await fixture.resolutions(targetId, pageContext!.id);
    expect(resolutions.some((resolution) => resolution.callId === 98101 || resolution.callId === 98102)).toBe(false);
  });
}, 15_000);

test("a different plugin cannot impersonate the status plugin", async () => {
  await withStatusHost({
    preparePlugins: async (pluginRoot) => {
      await writePlugin(pluginRoot, {
        id: "status-spoof",
        contentScripts: [{ matches: ["file:///*"], js: ["spoof.js"] }],
        files: {
          "spoof.js": `window.__attemptStatusSpoof = () => {
            const method = window.octoEnhancer?.getChannelStatus;
            if (typeof method !== "function") {
              document.documentElement.dataset.statusSpoof = "unavailable";
              return;
            }
            void method.call(window.octoEnhancer, { pluginId: "channel-status" }).then(
              (value) => { document.documentElement.dataset.statusSpoof = value?.state === "online" ? "online" : "resolved"; },
              (error) => { document.documentElement.dataset.statusSpoof = "rejected:" + String(error?.message ?? error); },
            );
          };
          window.__attemptDetailsSpoof = () => {
            const method = window.octoEnhancer?.getChatDetails;
            if (typeof method !== "function") {
              document.documentElement.dataset.statusDetailsSpoof = "unavailable";
              return;
            }
            void method.call(window.octoEnhancer, { channelType: 5, channelId: "spoof-thread" }, 903).then(
              (value) => { document.documentElement.dataset.statusDetailsSpoof = value?.state === "online" ? "online" : "resolved"; },
              (error) => { document.documentElement.dataset.statusDetailsSpoof = "rejected:" + String(error?.message ?? error); },
            );
          };`,
        },
      });
    },
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-spoof-attempt",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "status-spoof-attempt";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the authorized status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    await waitUntil(() => server.requests().length >= 1, "the real status request");
    const spoofContext = (await fixture.contexts(targetId)).find((context) =>
      !context.isDefault && context.worldName.includes("status-spoof"));
    expect(spoofContext).toBeDefined();

    const before = server.requests().length;
    expect(await fixture.evaluate(targetId, spoofContext!.id, "window.__attemptStatusSpoof(); window.__attemptDetailsSpoof(); true")).toBe(true);
    await waitUntil(async () => {
      const effects = await fixture.effects(targetId, `${targetId}-main`);
      return typeof effects.dataset.statusSpoof === "string" && typeof effects.dataset.statusDetailsSpoof === "string";
    }, "the status and details spoof attempts");
    const result = (await fixture.effects(targetId, `${targetId}-main`)).dataset.statusSpoof;
    const detailsResult = (await fixture.effects(targetId, `${targetId}-main`)).dataset.statusDetailsSpoof;
    expect(result).not.toBe("online");
    expect(detailsResult).not.toBe("online");
    expect(server.requests()).toHaveLength(before);
    expect(server.detailsRequests()).toHaveLength(0);
  });
}, 15_000);

test("disabling the status plugin aborts its pending HTTP request and rejects later bridge calls", async () => {
  await withStatusHost({
    holdResponse: true,
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-disable-boundary",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, host, server }) => {
    const targetId = "status-disable-boundary";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    await waitUntil(() => server.requests().length === 1, "the held localhost status request");
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();

    await host.setPluginEnabled("channel-status", false);
    await waitUntil(() => server.abortedResponse(), "the in-flight HTTP response to be aborted");
    await waitUntil(async () => !(await fixture.effects(targetId, `${targetId}-main`)).elementIds.includes("octobuddy-channel-status-panel"), "the status UI to be disposed");

    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      const method = window.octoEnhancer?.getChannelStatus;
      if (typeof method !== "function") {
        document.documentElement.dataset.disabledStatusBridge = "missing";
        return true;
      }
      void method.call(window.octoEnhancer).then(
        () => { document.documentElement.dataset.disabledStatusBridge = "fulfilled"; },
        (error) => { document.documentElement.dataset.disabledStatusBridge = String(error?.message ?? error); },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(async () => typeof (await fixture.effects(targetId, `${targetId}-main`)).dataset.disabledStatusBridge === "string", "the disabled bridge call to settle");
    expect((await fixture.effects(targetId, `${targetId}-main`)).dataset.disabledStatusBridge).toBe("plugin is disabled");
    expect(server.requests()).toHaveLength(1);
  });
}, 15_000);


test("the chat status bridge preserves each requested target across out-of-order localhost responses", async () => {
  const previousTarget = { channelType: 2, channelId: "group-previous" } as const;
  const currentTarget = { channelType: 5, channelId: "group____thread-7" } as const;
  const directTarget = { channelType: 1, channelId: "bot-uid-bridge-test" } as const;
  await withStatusHost({
    holdChatTargets: [previousTarget],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-chat-bridge",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "status-chat-bridge";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    expect(await fixture.evaluate(targetId, context!.id, "typeof window.octoEnhancer?.getChatStatus")).toBe("function");

    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__chatBridgeResults = {};
      window.octoEnhancer.getChatStatus(${JSON.stringify(previousTarget)}, 101).then(
        (result) => { window.__chatBridgeResults.previous = result; },
        (error) => { window.__chatBridgeResults.previous = { error: String(error?.message ?? error) }; },
      );
      window.octoEnhancer.getChatStatus(${JSON.stringify(currentTarget)}, 102).then(
        (result) => { window.__chatBridgeResults.current = result; },
        (error) => { window.__chatBridgeResults.current = { error: String(error?.message ?? error) }; },
      );
      window.octoEnhancer.getChatStatus(${JSON.stringify(directTarget)}, 103).then(
        (result) => { window.__chatBridgeResults.direct = result; },
        (error) => { window.__chatBridgeResults.direct = { error: String(error?.message ?? error) }; },
      );
      return true;
    })()`)).toBe(true);

    await waitUntil(() => server.chatRequests().length === 3, "all localhost chat status requests");
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__chatBridgeResults.current !== undefined && window.__chatBridgeResults.direct !== undefined",
    ) === true, "the newer chat responses");
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatBridgeResults.previous === undefined")).toBe(true);
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatBridgeResults.current")).toEqual({
      state: "online",
      snapshot: chatSnapshotFor(currentTarget),
    });
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatBridgeResults.direct")).toEqual({
      state: "online",
      snapshot: chatSnapshotFor(directTarget),
    });

    server.releaseChatResponse(previousTarget);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__chatBridgeResults.previous !== undefined",
    ) === true, "the previous chat response");
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatBridgeResults.previous")).toEqual({
      state: "online",
      snapshot: chatSnapshotFor(previousTarget),
    });

    const requestedTargets = server.chatRequests().map(({ path }) => {
      const url = new URL(path, server.baseUrl);
      expect(url.pathname).toBe("/v1/chats/status");
      return {
        channelType: Number(url.searchParams.get("channelType")),
        channelId: url.searchParams.get("channelId"),
      };
    }).sort((left, right) => left.channelType - right.channelType);
    expect(requestedTargets).toEqual([directTarget, previousTarget, currentTarget]);
    expect(server.chatRequests().every((request) =>
      request.method === "GET" && request.authorization === `Bearer ${bearerToken}`)).toBe(true);
  });
}, 15_000);

test("disabling the status plugin cancels a pending chat status bridge request", async () => {
  const target = { channelType: 5, channelId: "group____thread-cancel" } as const;
  await withStatusHost({
    holdChatTargets: [target],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-chat-cancel",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, host, server }) => {
    const targetId = "status-chat-cancel";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__chatBridgeCancelResult = undefined;
      window.octoEnhancer.getChatStatus(${JSON.stringify(target)}, 201).then(
        (result) => { window.__chatBridgeCancelResult = { result }; },
        (error) => { window.__chatBridgeCancelResult = { error: String(error?.message ?? error) }; },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(() => server.chatRequests().length === 1, "the held localhost chat status request");

    await host.setPluginEnabled("channel-status", false);
    await waitUntil(() => server.abortedChatResponse(), "the pending chat response to be aborted");
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__chatBridgeCancelResult?.error === "plugin is disabled"',
    ) === true, "the in-flight bridge call to reject on disable");

    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__chatBridgeDisabledResult = undefined;
      window.octoEnhancer.getChatStatus(${JSON.stringify(target)}, 202).then(
        () => { window.__chatBridgeDisabledResult = "fulfilled"; },
        (error) => { window.__chatBridgeDisabledResult = String(error?.message ?? error); },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__chatBridgeDisabledResult === "plugin is disabled"',
    ) === true, "later chat status bridge calls to reject while disabled");
    expect(server.chatRequests()).toHaveLength(1);
  });
}, 15_000);

test("cancelChatStatus aborts only the request with the matching panel generation", async () => {
  const previousTarget = { channelType: 2, channelId: "group-before-switch" } as const;
  const currentTarget = { channelType: 5, channelId: "group____thread-after-switch" } as const;
  await withStatusHost({
    holdChatTargets: [previousTarget, currentTarget],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "status-chat-request-cancel",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "status-chat-request-cancel";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the authorized status context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();

    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__chatRequestResults = {};
      window.octoEnhancer.getChatStatus(${JSON.stringify(previousTarget)}, 301).then(
        (result) => { window.__chatRequestResults.previous = { result }; },
        (error) => { window.__chatRequestResults.previous = { error: String(error?.message ?? error) }; },
      );
      window.octoEnhancer.getChatStatus(${JSON.stringify(currentTarget)}, 302).then(
        (result) => { window.__chatRequestResults.current = { result }; },
        (error) => { window.__chatRequestResults.current = { error: String(error?.message ?? error) }; },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(() => server.chatRequests().length === 2, "both held localhost chat requests");

    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__chatRequestCancel = undefined;
      window.octoEnhancer.cancelChatStatus(301).then(
        () => { window.__chatRequestCancel = "done"; },
        (error) => { window.__chatRequestCancel = String(error?.message ?? error); },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(() => server.abortedChatTarget(previousTarget), "the old chat request to be aborted");
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__chatRequestResults.previous?.error !== undefined && window.__chatRequestCancel !== undefined',
    ) === true, "the canceled request and cancellation call to settle");

    expect(server.abortedChatTarget(currentTarget)).toBe(false);
    expect(server.pendingChatResponse(currentTarget)).toBe(true);
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatRequestResults.current === undefined")).toBe(true);
    server.releaseChatResponse(currentTarget);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__chatRequestResults.current !== undefined",
    ) === true, "the current chat request to complete");
    expect(await fixture.evaluate(targetId, context!.id, "window.__chatRequestResults.current")).toEqual({
      result: { state: "online", snapshot: chatSnapshotFor(currentTarget) },
    });
    expect(typeof (await fixture.evaluate(targetId, context!.id, "window.__chatRequestResults.previous.error"))).toBe("string");
    expect(server.chatRequests()).toHaveLength(2);
  });
}, 15_000);

test("chat details are available only to the authorized status plugin on the communication page", async () => {
  const target = { channelType: 5, channelId: "group + 7____thread-details" } as const;
  await withStatusHost({
    prepareTargets: async (fixture, _root, appPath) => {
      await fixture.addTarget({ id: "details-authorized-communication", url: communicationUrl(appPath) });
      await fixture.addTarget({ id: "details-denied-conversation", url: conversationUrl(appPath) });
    },
  }, async ({ fixture, server }) => {
    const authorizedTargetId = "details-authorized-communication";
    const deniedTargetId = "details-denied-conversation";
    await waitUntil(async () =>
      (await statusContext(fixture, authorizedTargetId)) !== undefined &&
      (await statusContext(fixture, deniedTargetId)) !== undefined,
    "both status plugin contexts");
    await activatePackagedStatusPlugin(fixture, authorizedTargetId);
    await activatePackagedStatusPlugin(fixture, deniedTargetId);
    const authorizedContext = await statusContext(fixture, authorizedTargetId);
    const deniedContext = await statusContext(fixture, deniedTargetId);
    expect(authorizedContext).toBeDefined();
    expect(deniedContext).toBeDefined();
    expect(await fixture.evaluate(authorizedTargetId, authorizedContext!.id, "typeof window.octoEnhancer?.getChatDetails")).toBe("function");
    expect(await fixture.evaluate(deniedTargetId, deniedContext!.id, "typeof window.octoEnhancer?.getChatDetails")).toBe("function");

    await fixture.evaluate(authorizedTargetId, authorizedContext!.id, `window.__detailsAuthorized = undefined;
      window.octoEnhancer.getChatDetails(${JSON.stringify(target)}, 401).then(
        (result) => { window.__detailsAuthorized = { result }; },
        (error) => { window.__detailsAuthorized = { error: String(error?.message ?? error) }; },
      );`);
    await fixture.evaluate(deniedTargetId, deniedContext!.id, `window.__detailsDenied = undefined;
      window.octoEnhancer.getChatDetails(${JSON.stringify(target)}, 402).then(
        (result) => { window.__detailsDenied = { result }; },
        (error) => { window.__detailsDenied = { error: String(error?.message ?? error) }; },
      );`);

    await waitUntil(async () => await fixture.evaluate(
      authorizedTargetId,
      authorizedContext!.id,
      "window.__detailsAuthorized !== undefined",
    ) === true, "the authorized chat details result");
    await waitUntil(async () => await fixture.evaluate(
      deniedTargetId,
      deniedContext!.id,
      "window.__detailsDenied !== undefined",
    ) === true, "the non-communication details denial");
    expect(await fixture.evaluate(authorizedTargetId, authorizedContext!.id, "window.__detailsAuthorized")).toEqual({
      result: { state: "online", snapshot: chatDetailsSnapshotFor(target) },
    });
    expect(await fixture.evaluate(deniedTargetId, deniedContext!.id, "window.__detailsDenied.error")).toContain("not authorized");
    expect(server.detailsRequests()).toHaveLength(1);
    const detailsUrl = new URL(server.detailsRequests()[0]!.path, server.baseUrl);
    expect(detailsUrl.pathname).toBe("/v1/chats/details");
    expect(detailsUrl.searchParams.get("channelType")).toBe("5");
    expect(detailsUrl.searchParams.get("channelId")).toBe(target.channelId);
    expect(server.detailsRequests()[0]).toMatchObject({
      method: "GET",
      authorization: `Bearer ${bearerToken}`,
    });

    const pageContext = await fixture.context(authorizedTargetId, `${authorizedTargetId}-main`);
    expect(pageContext?.isDefault).toBe(true);
    expect(await fixture.evaluate(authorizedTargetId, pageContext!.id, "typeof window.octoEnhancer?.getChatDetails")).toBe("undefined");
  });
}, 15_000);

test("disabling the status plugin aborts pending chat details and revokes later bridge calls", async () => {
  const target = { channelType: 5, channelId: "group____details-disable" } as const;
  await withStatusHost({
    holdDetailsTargets: [target],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "details-disable-boundary",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, host, server }) => {
    const targetId = "details-disable-boundary";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    await fixture.evaluate(targetId, context!.id, `window.__detailsDisableResult = undefined;
      window.octoEnhancer.getChatDetails(${JSON.stringify(target)}, 501).then(
        (result) => { window.__detailsDisableResult = { result }; },
        (error) => { window.__detailsDisableResult = { error: String(error?.message ?? error) }; },
      );`);
    await waitUntil(() => server.pendingDetailsResponse(target), "the held chat details request");

    await host.setPluginEnabled("channel-status", false);
    await waitUntil(() => server.abortedDetailsTarget(target), "the details request to abort on disable");
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__detailsDisableResult?.error === 'plugin is disabled'",
    ) === true, "the pending bridge call to reject on disable");
    await fixture.evaluate(targetId, context!.id, `window.__detailsDisabledCall = undefined;
      window.octoEnhancer.getChatDetails(${JSON.stringify(target)}, 502).then(
        () => { window.__detailsDisabledCall = "fulfilled"; },
        (error) => { window.__detailsDisabledCall = String(error?.message ?? error); },
      );`);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__detailsDisabledCall !== undefined",
    ) === true, "subsequent disabled details calls to settle");
    expect(await fixture.evaluate(targetId, context!.id, "window.__detailsDisabledCall")).toBe("plugin is disabled");
    expect(server.detailsRequests()).toHaveLength(1);
  });
}, 15_000);

test("destroying a status plugin execution context aborts its pending chat details request", async () => {
  const target = { channelType: 5, channelId: "group____details-destroy" } as const;
  await withStatusHost({
    holdDetailsTargets: [target],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "details-destroy-boundary",
      url: communicationUrl(appPath),
    }),
  }, async ({ appPath, fixture, server }) => {
    const targetId = "details-destroy-boundary";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the status plugin context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    await fixture.evaluate(targetId, context!.id, `window.octoEnhancer.getChatDetails(${JSON.stringify(target)}, 601).catch(() => {});`);
    await waitUntil(() => server.pendingDetailsResponse(target), "the held details request before navigation");

    await fixture.navigate(targetId, `${targetId}-main`, settingsUrl(appPath));
    await waitUntil(() => server.abortedDetailsTarget(target), "the pending request to abort when its execution context is destroyed");
    expect(server.pendingDetailsResponse(target)).toBe(false);
  });
}, 15_000);

test("chat details cancellation is isolated from chat status requests with the same request ID", async () => {
  const detailsCancelTarget = { channelType: 2, channelId: "details-cancel-id-isolation" } as const;
  const statusCancelTarget = { channelType: 5, channelId: "group____status-cancel-id-isolation" } as const;
  await withStatusHost({
    holdChatTargets: [detailsCancelTarget, statusCancelTarget],
    holdDetailsTargets: [detailsCancelTarget, statusCancelTarget],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "details-request-id-isolation",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "details-request-id-isolation";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the authorized status context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    expect(await fixture.evaluate(targetId, context!.id, "typeof window.octoEnhancer?.getChatDetails")).toBe("function");
    expect(await fixture.evaluate(targetId, context!.id, "typeof window.octoEnhancer?.cancelChatDetails")).toBe("function");

    await fixture.evaluate(targetId, context!.id, `window.__requestKindResults = {};
      window.octoEnhancer.getChatStatus(${JSON.stringify(detailsCancelTarget)}, 701).then(
        (result) => { window.__requestKindResults.statusDetailsCancel = { result }; },
        (error) => { window.__requestKindResults.statusDetailsCancel = { error: String(error?.message ?? error) }; },
      );
      window.octoEnhancer.getChatDetails(${JSON.stringify(detailsCancelTarget)}, 701).then(
        (result) => { window.__requestKindResults.detailsCancel = { result }; },
        (error) => { window.__requestKindResults.detailsCancel = { error: String(error?.message ?? error) }; },
      );`);
    await waitUntil(() => server.pendingChatResponse(detailsCancelTarget) && server.pendingDetailsResponse(detailsCancelTarget),
      "same-ID status and details requests");
    await fixture.evaluate(targetId, context!.id, `window.__detailsCancelResult = undefined;
      window.octoEnhancer.cancelChatDetails(701).then(
        () => { window.__detailsCancelResult = "done"; },
        (error) => { window.__detailsCancelResult = String(error?.message ?? error); },
      );
      true;`);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__detailsCancelResult === "done"',
    ) === true, "details cancellation bridge call to resolve");
    await waitUntil(() => server.abortedDetailsTarget(detailsCancelTarget), "details-only cancellation");
    expect(server.pendingChatResponse(detailsCancelTarget)).toBe(true);
    expect(server.abortedChatTarget(detailsCancelTarget)).toBe(false);
    server.releaseChatResponse(detailsCancelTarget);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__requestKindResults.statusDetailsCancel?.result !== undefined",
    ) === true, "the status request with the matching numeric ID to remain active");
    expect(await fixture.evaluate(targetId, context!.id, "window.__requestKindResults.statusDetailsCancel")).toEqual({
      result: { state: "online", snapshot: chatSnapshotFor(detailsCancelTarget) },
    });

    await fixture.evaluate(targetId, context!.id, `window.__requestKindResults.statusCancel = undefined;
      window.octoEnhancer.getChatStatus(${JSON.stringify(statusCancelTarget)}, 702).then(
        (result) => { window.__requestKindResults.statusCancel = { result }; },
        (error) => { window.__requestKindResults.statusCancel = { error: String(error?.message ?? error) }; },
      );
      window.__requestKindResults.detailsStatusCancel = undefined;
      window.octoEnhancer.getChatDetails(${JSON.stringify(statusCancelTarget)}, 702).then(
        (result) => { window.__requestKindResults.detailsStatusCancel = { result }; },
        (error) => { window.__requestKindResults.detailsStatusCancel = { error: String(error?.message ?? error) }; },
      );`);
    await waitUntil(() => server.pendingChatResponse(statusCancelTarget) && server.pendingDetailsResponse(statusCancelTarget),
      "second same-ID status and details requests");
    await fixture.evaluate(targetId, context!.id, `window.__statusCancelResult = undefined;
      window.octoEnhancer.cancelChatStatus(702).then(
        () => { window.__statusCancelResult = "done"; },
        (error) => { window.__statusCancelResult = String(error?.message ?? error); },
      );
      true;`);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__statusCancelResult === "done"',
    ) === true, "status cancellation bridge call to resolve");
    await waitUntil(() => server.abortedChatTarget(statusCancelTarget), "status-only cancellation");
    expect(server.pendingDetailsResponse(statusCancelTarget)).toBe(true);
    expect(server.abortedDetailsTarget(statusCancelTarget)).toBe(false);
    server.releaseDetailsResponse(statusCancelTarget);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__requestKindResults.detailsStatusCancel?.result !== undefined",
    ) === true, "the details request with the matching numeric ID to remain active");
    expect(await fixture.evaluate(targetId, context!.id, "window.__requestKindResults.detailsStatusCancel")).toEqual({
      result: { state: "online", snapshot: chatDetailsSnapshotFor(statusCancelTarget) },
    });
    const expectedTargets = [
      `${detailsCancelTarget.channelType}:${detailsCancelTarget.channelId}`,
      `${statusCancelTarget.channelType}:${statusCancelTarget.channelId}`,
    ].sort();
    const requestTargets = (requests: StatusRequest[], pathname: string) => requests.map(({ path }) => {
      const url = new URL(path, server.baseUrl);
      expect(url.pathname).toBe(pathname);
      return `${url.searchParams.get("channelType")}:${url.searchParams.get("channelId")}`;
    }).sort();
    expect(server.chatRequests()).toHaveLength(2);
    expect(requestTargets(server.chatRequests(), "/v1/chats/status")).toEqual(expectedTargets);
    expect(server.detailsRequests()).toHaveLength(2);
    expect(requestTargets(server.detailsRequests(), "/v1/chats/details")).toEqual(expectedTargets);
  });
}, 15_000);

test("switching detail targets cancels the old request without canceling the current target", async () => {
  const previousTarget = { channelType: 2, channelId: "group-before-details-switch" } as const;
  const currentTarget = { channelType: 5, channelId: "group____thread-after-details-switch" } as const;
  await withStatusHost({
    holdDetailsTargets: [previousTarget, currentTarget],
    prepareTargets: (fixture, _root, appPath) => fixture.addTarget({
      id: "details-target-switch",
      url: communicationUrl(appPath),
    }),
  }, async ({ fixture, server }) => {
    const targetId = "details-target-switch";
    await waitUntil(async () => (await statusContext(fixture, targetId)) !== undefined, "the authorized status context");
    await activatePackagedStatusPlugin(fixture, targetId);
    const context = await statusContext(fixture, targetId);
    expect(context).toBeDefined();
    expect(await fixture.evaluate(targetId, context!.id, `(() => {
      window.__detailSwitchResults = {};
      window.octoEnhancer.getChatDetails(${JSON.stringify(previousTarget)}, 801).then(
        (result) => { window.__detailSwitchResults.previous = { result }; },
        (error) => { window.__detailSwitchResults.previous = { error: String(error?.message ?? error) }; },
      );
      window.octoEnhancer.getChatDetails(${JSON.stringify(currentTarget)}, 802).then(
        (result) => { window.__detailSwitchResults.current = { result }; },
        (error) => { window.__detailSwitchResults.current = { error: String(error?.message ?? error) }; },
      );
      return true;
    })()`)).toBe(true);
    await waitUntil(() => server.pendingDetailsResponse(previousTarget) && server.pendingDetailsResponse(currentTarget),
      "both previous and current chat details requests");

    await fixture.evaluate(targetId, context!.id, `window.__detailSwitchCancel = undefined;
      window.octoEnhancer.cancelChatDetails(801).then(
        () => { window.__detailSwitchCancel = "done"; },
        (error) => { window.__detailSwitchCancel = String(error?.message ?? error); },
      );
      true;`);
    await waitUntil(() => server.abortedDetailsTarget(previousTarget), "the previous target request to be canceled");
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      'window.__detailSwitchCancel === "done" && window.__detailSwitchResults.previous?.error !== undefined',
    ) === true, "the previous request and cancellation bridge call to settle");
    expect(server.pendingDetailsResponse(currentTarget)).toBe(true);
    expect(server.abortedDetailsTarget(currentTarget)).toBe(false);
    expect(await fixture.evaluate(targetId, context!.id, "window.__detailSwitchResults.current === undefined")).toBe(true);

    server.releaseDetailsResponse(currentTarget);
    await waitUntil(async () => await fixture.evaluate(
      targetId,
      context!.id,
      "window.__detailSwitchResults.current !== undefined",
    ) === true, "the current target details response");
    expect(await fixture.evaluate(targetId, context!.id, "window.__detailSwitchResults.current")).toEqual({
      result: { state: "online", snapshot: chatDetailsSnapshotFor(currentTarget) },
    });
    const requestedTargets = server.detailsRequests().map(({ path }) => {
      const url = new URL(path, server.baseUrl);
      expect(url.pathname).toBe("/v1/chats/details");
      return `${url.searchParams.get("channelType")}:${url.searchParams.get("channelId")}`;
    }).sort();
    const expectedTargets = [
      `${previousTarget.channelType}:${previousTarget.channelId}`,
      `${currentTarget.channelType}:${currentTarget.channelId}`,
    ].sort();
    expect(server.detailsRequests()).toHaveLength(2);
    expect(requestedTargets).toEqual(expectedTargets);
    expect(server.detailsRequests().every((request) =>
      request.method === "GET" && request.authorization === `Bearer ${bearerToken}`)).toBe(true);
  });
}, 15_000);
