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

type StatusRequest = { method: string; path: string; authorization: string | undefined };

type LocalStatusServer = {
  baseUrl: string;
  requests(): StatusRequest[];
  chatRequests(): StatusRequest[];
  releaseChatResponse(target: ChatStatusTarget): void;
  pendingChatResponse(target: ChatStatusTarget): boolean;
  abortedResponse(): boolean;
  abortedChatResponse(): boolean;
  abortedChatTarget(target: ChatStatusTarget): boolean;
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

function createLocalStatusServer(options: {
  holdResponse?: boolean;
  holdChatTargets?: ChatStatusTarget[];
} = {}): Promise<LocalStatusServer> {
  const requests: StatusRequest[] = [];
  const chatRequests: StatusRequest[] = [];
  const heldChatTargets = new Set((options.holdChatTargets ?? []).map(chatStatusKey));
  const pendingChatResponses = new Map<string, ServerResponse[]>();
  let didAbortResponse = false;
  let didAbortChatResponse = false;
  const abortedChatTargets = new Set<string>();
  const server = createServer((request, response) => {
    const path = request.url ?? "/";
    requests.push({
      method: request.method ?? "",
      path,
      authorization: request.headers.authorization,
    });
    const url = new URL(path, "http://127.0.0.1");
    if (request.method !== "GET" || (url.pathname !== "/v1/status" && url.pathname !== "/v1/chats/status")) {
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
    chatRequests.push({ method: request.method ?? "", path, authorization: request.headers.authorization });
    const key = chatStatusKey(target);
    if (heldChatTargets.has(key)) {
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
    response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(chatSnapshotFor(target)));
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
      if (typeof method === "function") {
        const pending = method.call(window.octoEnhancer);
        pending?.catch?.(() => {});
      }
      const forged = JSON.stringify({ callId: 98101, api: "status", method: "getChannelStatus", args: [] });
      if (typeof window.__ewSend === "function") window.__ewSend(forged);
      return typeof method;
    })()`);
    expect(attempt).toBe("undefined");
    await fixture.drain();
    expect(server.requests()).toHaveLength(before);
    expect((await fixture.resolutions(targetId, pageContext!.id)).some((resolution) => resolution.callId === 98101)).toBe(false);
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
    expect(await fixture.evaluate(targetId, spoofContext!.id, "window.__attemptStatusSpoof(); true")).toBe(true);
    await waitUntil(async () => {
      const effects = await fixture.effects(targetId, `${targetId}-main`);
      return typeof effects.dataset.statusSpoof === "string";
    }, "the spoof attempt result");
    const result = (await fixture.effects(targetId, `${targetId}-main`)).dataset.statusSpoof;
    expect(result).not.toBe("online");
    expect(server.requests()).toHaveLength(before);
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
