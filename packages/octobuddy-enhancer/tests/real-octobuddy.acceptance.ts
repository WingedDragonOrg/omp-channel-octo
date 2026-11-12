import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:http";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { assertOwnedCdp, processStartIdentity } from "../src/launcher/identity.ts";
import { resolveOctoBuddyApp } from "../src/launcher/paths.ts";
import { createFakeZed } from "./fixtures/host.ts";

/**
 * Prerequisites: Bun, the package's `chrome-remote-interface` dependency, Node on PATH for
 * the fake-Zed fixture, `/bin/ps` and `lsof`, and an installed OctoBuddy.app whose child
 * prints its loopback ephemeral CDP URL and opens `Contents/Resources/app.asar/out/renderer/index.html`.
 * The status acceptance also observes the installed communication renderer and uses only a disposable loopback HTTP fixture.
 * The child uses no login and does not click the ordinary HTTP link. HOME, OCTOBUDDY_HOME,
 * workspace, fake Zed, and Electron's `--user-data-dir` are created under one temporary root;
 * the app child receives E2E_TEST=1 and does not reuse the user's instance.
 */


type CdpTarget = {
  id: string;
  type: string;
  url: string;
  title?: string;
};

type CdpClient = {
  send(method: string, params?: Record<string, unknown>, sessionId?: string): Promise<unknown>;
  once(eventName: string, listener: (...args: unknown[]) => void): unknown;
  close(): Promise<void>;
};

type CdpFactory = {
  (options: { target: string }): Promise<CdpClient>;
  List(options: { host: string; port: number }): Promise<CdpTarget[]>;
  Version(options: { host: string; port: number }): Promise<{ webSocketDebuggerUrl: string }>;
};

type HostOptions = {
  port: number;
  pid: number;
  startedAt: string;
  appPath: string;
  pluginDirectories: string[];
  zedExecutable: string;
  statusConfigPath?: string;
};

type HostSession = { stop(): Promise<void>; setPluginEnabled(pluginId: string, enabled: boolean): Promise<void> };
type StatusRequest = { method: string; path: string; authorization: string | undefined };
type RendererChannel = { channelID: string; channelType: 1 | 2 | 5 };
type RendererChatIdentity = { channelType: 1 | 2 | 5; channelId: string };
type RendererChatEvent = {
  type: "octobuddy-enhancer:chat";
  channelType: 1 | 2 | 5 | null;
  channelId: string | null;
};
type LocalStatusServer = {
  baseUrl: string;
  requests(): StatusRequest[];
  close(): Promise<void>;
};

type AccessibilityNode = {
  role?: { value?: string };
  name?: { value?: string };
  value?: { value?: string };
  ignored?: boolean;
};
type HostEntry = { startHost(options: HostOptions): Promise<HostSession> };

type IsolatedPaths = {
  root: string;
  home: string;
  octobuddyHome: string;
  workspace: string;
  userDataDir: string;
  tempDir: string;
};

type ActionBox = {
  accessibleName: string;
  x: number;
  y: number;
  width: number;
  height: number;
  inViewport: boolean;
  disabled: boolean;
};

const cdp = createRequire(import.meta.url)("chrome-remote-interface") as CdpFactory;
const cdpHost = "127.0.0.1";
const testTimeoutMs = 240_000;
const statusToken = "isolated-status-acceptance-token-2026-11-11-0123456789";
const rendererChatProbeKey = "__octobuddyRealRendererChatProbe";
const rendererChatRequestType = "octobuddy-enhancer:chat-request";
const localStatusSnapshot = {
  version: 1,
  pid: 68427,
  botUid: "bot-17",
  ownerUid: "owner-17",
  runningSessions: 4,
  queuedTasks: 2,
  sampledAt: "2026-11-11T10:14:30.000Z",
};

async function createLocalStatusServer(): Promise<LocalStatusServer> {
  const received: StatusRequest[] = [];
  const server = createServer((request, response) => {
    received.push({
      method: request.method ?? "",
      path: request.url ?? "",
      authorization: request.headers.authorization,
    });
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method !== "GET" || !["/v1/status", "/v1/chats/status", "/v1/chats/details"].includes(url.pathname)) {
      response.writeHead(404).end();
      return;
    }
    if (request.headers.authorization !== `Bearer ${statusToken}`) {
      response.writeHead(401, { "content-type": "application/json" })
        .end(JSON.stringify({ error: { code: "unauthorized" } }));
      return;
    }
    if (url.pathname === "/v1/status") {
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify(localStatusSnapshot));
      return;
    }
    const channelType = Number(url.searchParams.get("channelType"));
    const channelId = url.searchParams.get("channelId");
    if (![1, 2, 5].includes(channelType) || !channelId) {
      response.writeHead(400, { "content-type": "application/json", "cache-control": "no-store" })
        .end(JSON.stringify({ error: { code: "invalid_request" } }));
      return;
    }
    if (url.pathname === "/v1/chats/details") {
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify({
        version: 1, sampledAt: localStatusSnapshot.sampledAt, channelType, channelId,
        context: {
          source: "live", stale: false, usedTokens: 4200, contextWindow: 10000,
          percent: 42, model: "test/model", observedAt: localStatusSnapshot.sampledAt, note: "实时估算",
        },
        references: { pr: ["https://github.com/example/project/pull/42"], mr: [] },
        tasks: [{ workspaceId: "workspace-1", taskId: "task-1", title: "面板验收任务", status: "doing", url: null }],
      }));
      return;
    }
    response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify({
      version: 1,
      sampledAt: localStatusSnapshot.sampledAt,
      channelType,
      channelId,
      state: "idle",
      model: null,
      cwd: null,
      queuedTasks: 0,
      pendingQuestions: 0,
    }));
  });
  const { promise, resolve, reject } = Promise.withResolvers<LocalStatusServer>();
  server.once("error", reject);
  server.listen(0, "127.0.0.1", () => {
    server.off("error", reject);
    const address = server.address();
    if (!address || typeof address === "string") {
      reject(new Error("Local status fixture did not bind a TCP port"));
      return;
    }
    resolve({
      baseUrl: `http://127.0.0.1:${address.port}`,
      requests: () => [...received],
      close: async () => {
        server.closeAllConnections();
        const closed = Promise.withResolvers<void>();
        server.close((error) => error ? closed.reject(error) : closed.resolve());
        await closed.promise;
      },
    });
  });
  return await promise;
}

async function loadHostEntry(): Promise<HostEntry> {
  // Keep a missing production entry an acceptance failure rather than a test-discovery failure.
  const entry: unknown = await import("../src/host/index.ts");
  if (!entry || typeof entry !== "object" || !("startHost" in entry) || typeof entry.startHost !== "function") {
    throw new Error("Host behavior entry must export startHost(options)");
  }
  return { startHost: entry.startHost as HostEntry["startHost"] };
}

function isolatedEnvironment(paths: IsolatedPaths): NodeJS.ProcessEnv {
  return {
    PATH: "/usr/bin:/bin:/usr/sbin:/sbin",
    HOME: paths.home,
    TMPDIR: paths.tempDir,
    OCTOBUDDY_HOME: paths.octobuddyHome,
    OCTOBUDDY_DEFAULT_WORKSPACE: paths.workspace,
    E2E_TEST: "1",
    LC_ALL: "C",
    TZ: "UTC",
  };
}

function setProcessEnvironment(values: Record<string, string>): () => void {
  const originals = new Map<string, string | undefined>();
  for (const [key, value] of Object.entries(values)) {
    originals.set(key, process.env[key]);
    process.env[key] = value;
  }
  return () => {
    for (const [key, value] of originals) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  };
}

function errorCode(error: unknown): string | undefined {
  return error && typeof error === "object" && "code" in error
    ? String((error as NodeJS.ErrnoException).code)
    : undefined;
}

function processGroupExists(processGroupId: number): boolean {
  try {
    process.kill(-processGroupId, 0);
    return true;
  } catch (error) {
    if (errorCode(error) === "ESRCH") return false;
    if (errorCode(error) === "EPERM") return true;
    throw error;
  }
}

function signalProcessGroup(processGroupId: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-processGroupId, signal);
  } catch (error) {
    if (errorCode(error) !== "ESRCH") throw error;
  }
}

async function waitUntil(predicate: () => Promise<boolean>, description: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(50);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function terminateProcessGroup(child: ChildProcess | undefined): Promise<void> {
  const processGroupId = child?.pid;
  if (!processGroupId) return;

  signalProcessGroup(processGroupId, "SIGTERM");
  try {
    await waitUntil(async () => !processGroupExists(processGroupId), "OctoBuddy process group to exit", 8_000);
  } catch {
    signalProcessGroup(processGroupId, "SIGKILL");
    await waitUntil(async () => !processGroupExists(processGroupId), "OctoBuddy process group to be killed", 8_000);
  }
}

async function waitForCdpPort(child: ChildProcess, stderr: { text: string }): Promise<number> {
  if (!child.stderr) throw new Error("OctoBuddy stderr was not captured");
  const { promise, resolve: fulfill, reject } = Promise.withResolvers<number>();
  let settled = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const finish = (error?: Error, port?: number) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    if (error) reject(error);
    else if (port !== undefined) fulfill(port);
  };
  const onData = (chunk: Buffer | string) => {
    stderr.text = `${stderr.text}${String(chunk)}`.slice(-64 * 1024);
    const match = stderr.text.match(/DevTools listening on ws:\/\/127\.0\.0\.1:(\d+)\//);
    if (match) finish(undefined, Number(match[1]));
  };
  const onError = (error: Error) => finish(new Error(`Could not launch installed OctoBuddy: ${error.message}`));
  const onExit = (code: number | null, signal: NodeJS.Signals | null) => finish(new Error(
    `OctoBuddy exited before exposing CDP (code=${String(code)}, signal=${String(signal)}): ${stderr.text.slice(-4_000)}`,
  ));

  timer = setTimeout(() => finish(new Error(`OctoBuddy did not expose a local CDP port: ${stderr.text.slice(-4_000)}`)), 45_000);
  child.stderr.on("data", onData);
  child.once("error", onError);
  child.once("exit", onExit);
  try {
    return await promise;
  } finally {
    clearTimeout(timer);
    child.off("error", onError);
    child.off("exit", onExit);
  }
}

async function send<T>(client: CdpClient, method: string, params: Record<string, unknown> = {}, sessionId?: string): Promise<T> {
  return await client.send(method, params, sessionId) as T;
}

async function evaluate<T>(client: CdpClient, sessionId: string, expression: string): Promise<T> {
  const response = await send<{
    result?: { value?: T };
    exceptionDetails?: { text?: string };
  }>(client, "Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId);
  if (response.exceptionDetails) throw new Error(`Page evaluation failed: ${response.exceptionDetails.text ?? "unknown exception"}`);
  return response.result?.value as T;
}

function pagePath(url: string): string | undefined {
  try {
    const parsed = new URL(url);
    return parsed.protocol === "file:" ? parsed.pathname : undefined;
  } catch {
    return undefined;
  }
}

async function waitForMainTarget(port: number, expectedPath: string): Promise<CdpTarget> {
  let matches: CdpTarget[] = [];
  await waitUntil(async () => {
    try {
      matches = (await cdp.List({ host: cdpHost, port })).filter((target) =>
        target.type === "page" && pagePath(target.url) === expectedPath,
      );
      return matches.length > 0;
    } catch {
      return false;
    }
  }, "the installed OctoBuddy main renderer target", 45_000);
  if (matches.length !== 1) {
    throw new Error(`Expected one installed OctoBuddy main renderer target, found ${matches.length}`);
  }
  return matches[0]!;
}

async function readAccessibilityTree(client: CdpClient, sessionId: string): Promise<{ nodes: AccessibilityNode[] }> {
  return await send(client, "Accessibility.getFullAXTree", {}, sessionId) as { nodes: AccessibilityNode[] };
}

function visibleAccessibilityNodes(tree: { nodes: AccessibilityNode[] }): AccessibilityNode[] {
  return tree.nodes.filter((node) => node.ignored !== true);
}

async function waitForStatusButton(client: CdpClient, sessionId: string): Promise<ActionBox> {
  let box: ActionBox | null = null;
  const expression = `(() => {
    const button = document.querySelector("[data-octobuddy-channel-status-button]");
    if (!button) return null;
    const rect = button.getBoundingClientRect();
    const style = getComputedStyle(button);
    return {
      accessibleName: button.getAttribute("aria-label") || button.textContent.trim(),
      x: rect.x,
      y: rect.y,
      width: rect.width,
      height: rect.height,
      inViewport: rect.width > 0 && rect.height > 0 && rect.left >= 0 && rect.top >= 0
        && rect.right <= window.innerWidth && rect.bottom <= window.innerHeight
        && style.display !== "none" && style.visibility !== "hidden",
      disabled: button.disabled,
    };
  })()`;
  await waitUntil(async () => {
    try {
      box = await evaluate<ActionBox | null>(client, sessionId, expression);
      return Boolean(box?.inViewport && !box.disabled);
    } catch {
      return false;
    }
  }, "the visible installed channel-status control", 15_000).catch(error => {
    throw new Error(`${error instanceof Error ? error.message : String(error)}; geometry=${JSON.stringify(box)}`);
  });
  if (!box) throw new Error("The installed channel-status control is missing");
  return box;
}

function probeRowsExpression(prefix: string, fileUrl: string, rawAbsolutePath: string): string {
  const rows = [
    { id: `${prefix}-file-url`, href: fileUrl, label: "Open file URL", top: 96 },
    { id: `${prefix}-absolute-path`, href: rawAbsolutePath, label: "Open absolute path", top: 138 },
    { id: `${prefix}-ordinary-http`, href: "https://example.invalid/unchanged", label: "Ordinary web link", top: 180 },
  ];
  return `(() => {
    if (!document.body) return false;
    for (const id of ${JSON.stringify(rows.map((row) => `${row.id}-row`))}) document.getElementById(id)?.remove();
    for (const item of ${JSON.stringify(rows)}) {
      const row = document.createElement("div");
      row.id = item.id + "-row";
      row.style.position = "fixed";
      row.style.left = "24px";
      row.style.top = item.top + "px";
      row.style.zIndex = "2147483647";
      row.style.display = "flex";
      row.style.alignItems = "center";
      row.style.gap = "8px";
      const anchor = document.createElement("a");
      anchor.id = item.id;
      anchor.setAttribute("href", item.href);
      anchor.textContent = item.label;
      row.append(anchor);
      document.body.append(row);
    }
    return true;
  })()`;
}

async function waitForAction(client: CdpClient, sessionId: string, rowId: string): Promise<ActionBox> {
  let box: ActionBox | null = null;
  const expression = `(() => {
    const row = document.getElementById(${JSON.stringify(rowId)});
    const button = row?.querySelector('[data-octobuddy-zed-action]');
    if (!button) return null;
    const rect = button.getBoundingClientRect();
    const style = getComputedStyle(button);
    return {
      accessibleName: button.getAttribute("aria-label") || button.textContent.trim(),
      x: rect.x,
      y: rect.y,
      width: rect.width,
      height: rect.height,
      inViewport: rect.width > 0 && rect.height > 0 && rect.left >= 0 && rect.top >= 0
        && rect.right <= window.innerWidth && rect.bottom <= window.innerHeight
        && style.display !== "none" && style.visibility !== "hidden",
      disabled: button.disabled,
    };
  })()`;
  await waitUntil(async () => {
    try {
      box = await evaluate<ActionBox | null>(client, sessionId, expression);
      return Boolean(box?.inViewport && !box.disabled);
    } catch {
      return false;
    }
  }, `a visible Zed action in ${rowId}`, 25_000);
  if (!box) throw new Error(`Missing Zed action in ${rowId}`);
  return box;
}

async function dispatchMouseClick(client: CdpClient, sessionId: string, box: ActionBox): Promise<void> {
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await send(client, "Page.bringToFront", {}, sessionId);
  await send(client, "Input.dispatchMouseEvent", { type: "mouseMoved", x, y, button: "none", buttons: 0 }, sessionId);
  await send(client, "Input.dispatchMouseEvent", {
    type: "mousePressed", x, y, button: "left", buttons: 1, clickCount: 1, pointerType: "mouse",
  }, sessionId);
  await send(client, "Input.dispatchMouseEvent", {
    type: "mouseReleased", x, y, button: "left", buttons: 0, clickCount: 1, pointerType: "mouse",
  }, sessionId);
}

function bridgeAttackExpression(path: string): string {
  const forgedRequest = JSON.stringify(JSON.stringify({
    callId: 19001,
    extId: "zed-open",
    api: "zed",
    method: "open",
    args: [{ path, line: 37 }],
  }));
  return `(() => {
    const frame = document.createElement("iframe");
    frame.id = "octobuddy-e2e-untrusted-frame";
    frame.src = "about:blank";
    document.body.append(frame);
    const invoke = (scope) => {
      let sent = false;
      try {
        const api = scope.octoEnhancer;
        if (api && typeof api.openZed === "function") {
          const result = api.openZed({ path: ${JSON.stringify(path)}, line: 37 });
          if (result && typeof result.catch === "function") result.catch(() => {});
          sent = true;
        }
      } catch {}
      try {
        if (typeof scope.__ewSend === "function") {
          const result = scope.__ewSend(${forgedRequest});
          if (result && typeof result.catch === "function") result.catch(() => {});
          sent = true;
        }
      } catch {}
      return sent;
    };
    const child = frame.contentWindow;
    return {
      pageApiVisible: typeof window.octoEnhancer?.openZed === "function",
      frameApiVisible: Boolean(child && typeof child.octoEnhancer?.openZed === "function"),
      pageAttempted: invoke(window),
      frameAttempted: child ? invoke(child) : false,
    };
  })()`;
}

async function readZedArguments(path: string): Promise<string[][]> {
  try {
    return (await readFile(path, "utf8"))
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as string[]);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return [];
    throw error;
  }
}

async function waitForZedCalls(argvFile: string, count: number): Promise<void> {
  await waitUntil(async () => {
    try {
      return (await readZedArguments(argvFile)).length >= count;
    } catch {
      return false;
    }
  }, `${count} fake Zed invocation(s)`, 15_000);
}

async function linkState(client: CdpClient, sessionId: string, prefix: string): Promise<{
  href: string | null;
  hasAction: boolean;
}> {
  return evaluate(client, sessionId, `(() => {
    const row = document.getElementById(${JSON.stringify(`${prefix}-ordinary-http-row`)});
    const link = document.getElementById(${JSON.stringify(`${prefix}-ordinary-http`)});
    return {
      href: link?.getAttribute("href") ?? null,
      hasAction: Boolean(row?.querySelector("[data-octobuddy-zed-action]")),
    };
  })()`);
}

async function probeActionCount(client: CdpClient, sessionId: string, prefix: string): Promise<number> {
  return evaluate(client, sessionId, `(() => {
    let count = 0;
    for (const id of ${JSON.stringify([`${prefix}-file-url-row`, `${prefix}-absolute-path-row`])}) {
      count += document.getElementById(id)?.querySelectorAll("[data-octobuddy-zed-action]").length ?? 0;
    }
    return count;
  })()`);
}

async function readRendererChatEvents(client: CdpClient, sessionId: string): Promise<RendererChatEvent[]> {
  return evaluate(client, sessionId, `(() => {
    const probe = window[${JSON.stringify(rendererChatProbeKey)}];
    return Array.isArray(probe?.events) ? probe.events.map((event) => ({
      type: event.type,
      channelType: event.channelType ?? null,
      channelId: event.channelId ?? null,
    })) : [];
  })()`);
}


test("real installed OctoBuddy loads and disposes the Zed and channel status plugins in isolation", async () => {
  const app = await resolveOctoBuddyApp();

  const root = await mkdtemp(join(tmpdir(), "octobuddy-real-zed-"));
  const paths: IsolatedPaths = {
    root,
    home: join(root, "home"),
    octobuddyHome: join(root, "octobuddy-home"),
    workspace: join(root, "workspace with spaces"),
    userDataDir: join(root, "electron-user-data"),
    tempDir: join(root, "tmp"),
  };
  let child: ChildProcess | undefined;
  let host: HostSession | undefined;
  let browser: CdpClient | undefined;
  let pageSessionId: string | undefined;
  let restoreHostEnvironment: (() => void) | undefined;
  let statusServer: LocalStatusServer | undefined;

  try {
    await Promise.all([
      mkdir(paths.home, { recursive: true }),
      mkdir(paths.octobuddyHome, { recursive: true }),
      mkdir(paths.workspace, { recursive: true }),
      mkdir(paths.tempDir, { recursive: true }),
    ]);
    const fakeZed = await createFakeZed(join(root, "fake-zed"));
    const statusFixture = await createLocalStatusServer();
    statusServer = statusFixture;
    const statusConfigPath = join(root, "channel-status.json");
    await writeFile(statusConfigPath, JSON.stringify({
      baseUrl: statusFixture.baseUrl,
      token: statusToken,
    }), { mode: 0o600 });
    await chmod(statusConfigPath, 0o600);
    expect((await stat(statusConfigPath)).mode & 0o777).toBe(0o600);
    const fileUrlPath = join(paths.workspace, "file URL with spaces.md");
    const literalPercentPath = join(paths.workspace, "literal%20 path with spaces.md");
    await Promise.all([
      writeFile(fileUrlPath, "line one\n".repeat(40)),
      writeFile(literalPercentPath, "line one\n".repeat(20)),
    ]);
    const fileUrl = `${pathToFileURL(fileUrlPath).href}#L37`;
    const rawAbsolutePath = `${literalPercentPath}:9`;
    const childEnvironment = isolatedEnvironment(paths);

    child = spawn(app.executablePath, [
      "--remote-debugging-address=127.0.0.1",
      "--remote-debugging-port=0",
      `--user-data-dir=${paths.userDataDir}`,
    ], {
      cwd: paths.workspace,
      detached: true,
      env: childEnvironment,
      stdio: ["ignore", "ignore", "pipe"],
    });
    if (!child.pid) throw new Error("Installed OctoBuddy did not create a child process");
    const childPid = child.pid;
    const stderr = { text: "" };
    const port = await waitForCdpPort(child, stderr);
    expect(port).toBeGreaterThan(0);
    const startedAt = await processStartIdentity(childPid);
    await assertOwnedCdp({ port, pid: childPid, startedAt });

    restoreHostEnvironment = setProcessEnvironment({
      HOME: paths.home,
      TMPDIR: paths.tempDir,
      OCTOBUDDY_HOME: paths.octobuddyHome,
      E2E_TEST: "1",
    });
    const { startHost } = await loadHostEntry();
    host = await startHost({
      port,
      pid: childPid,
      startedAt,
      appPath: app.bundlePath,
      pluginDirectories: [join(resolve(import.meta.dir, ".."), "plugins")],
      zedExecutable: fakeZed.executable,
      statusConfigPath,
    });

    const version = await cdp.Version({ host: cdpHost, port });
    const browserWebSocket = new URL(version.webSocketDebuggerUrl);
    expect(browserWebSocket.protocol).toBe("ws:");
    expect(browserWebSocket.hostname).toBe(cdpHost);
    expect(Number(browserWebSocket.port)).toBe(port);
    browser = await cdp({ target: version.webSocketDebuggerUrl });

    const rendererPath = join(app.bundlePath, "Contents", "Resources", "app.asar", "out", "renderer", "index.html");
    const expectedRendererPath = new URL(pathToFileURL(rendererPath).href).pathname;
    const target = await waitForMainTarget(port, expectedRendererPath);
    const attached = await send<{ sessionId: string }>(browser, "Target.attachToTarget", {
      targetId: target.id,
      flatten: true,
    });
    pageSessionId = attached.sessionId;
    await send(browser, "Page.enable", {}, pageSessionId);
    await send(browser, "Runtime.enable", {}, pageSessionId);

    const frameTree = await send<{ frameTree: { frame: { url: string; parentId?: string } } }>(
      browser,
      "Page.getFrameTree",
      {},
      pageSessionId,
    );
    expect(pagePath(frameTree.frameTree.frame.url)).toBe(expectedRendererPath);
    expect(frameTree.frameTree.frame.parentId).toBeUndefined();

    expect(await evaluate<boolean>(browser, pageSessionId, 'document.querySelector("#octo-enhancer-toolbar") === null')).toBe(true);
    await send(browser, "Input.dispatchKeyEvent", {
      type: "keyDown", key: "P", code: "KeyP", modifiers: 13, windowsVirtualKeyCode: 80,
    }, pageSessionId);
    await send(browser, "Input.dispatchKeyEvent", {
      type: "keyUp", key: "P", code: "KeyP", modifiers: 13, windowsVirtualKeyCode: 80,
    }, pageSessionId);
    await waitUntil(async () => {
      const tree = await send<{ nodes: Array<{ name?: { value?: string } }> }>(browser!, "Accessibility.getFullAXTree", {}, pageSessionId);
      return tree.nodes.some((node) => node.name?.value === "OctoBuddy plugin management");
    }, "plugin management panel after keyboard shortcut", 10_000);
    await send(browser, "Input.dispatchKeyEvent", {
      type: "keyDown", key: "P", code: "KeyP", modifiers: 13, windowsVirtualKeyCode: 80,
    }, pageSessionId);
    await send(browser, "Input.dispatchKeyEvent", {
      type: "keyUp", key: "P", code: "KeyP", modifiers: 13, windowsVirtualKeyCode: 80,
    }, pageSessionId);

    const initialRows = await evaluate<boolean>(browser, pageSessionId, probeRowsExpression("zed-e2e-initial", fileUrl, rawAbsolutePath));
    expect(initialRows).toBe(true);
    const fileUrlAction = await waitForAction(browser, pageSessionId, "zed-e2e-initial-file-url-row");
    const rawPathAction = await waitForAction(browser, pageSessionId, "zed-e2e-initial-absolute-path-row");
    expect(fileUrlAction.accessibleName).toMatch(/zed/i);
    expect(rawPathAction.accessibleName).toMatch(/zed/i);

    const ordinaryLink = await linkState(browser, pageSessionId, "zed-e2e-initial");
    expect(ordinaryLink).toEqual({ href: "https://example.invalid/unchanged", hasAction: false });

    const syntheticResult = await evaluate<boolean>(browser, pageSessionId, `(() => {
      const button = document.querySelector("#zed-e2e-initial-file-url-row [data-octobuddy-zed-action]");
      if (!button) return false;
      button.click();
      return true;
    })()`);
    expect(syntheticResult).toBe(true);
    const bridgeAttack = await evaluate<{
      pageApiVisible: boolean;
      frameApiVisible: boolean;
      pageAttempted: boolean;
      frameAttempted: boolean;
    }>(browser, pageSessionId, bridgeAttackExpression(literalPercentPath));
    expect(bridgeAttack.pageApiVisible).toBe(false);
    expect(bridgeAttack.frameApiVisible).toBe(false);
    await Bun.sleep(350);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([]);

    await dispatchMouseClick(browser, pageSessionId, fileUrlAction);
    await waitForZedCalls(fakeZed.argvFile, 1);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([[`${await realpath(fileUrlPath)}:37:1`]]);

    await dispatchMouseClick(browser, pageSessionId, rawPathAction);
    await waitForZedCalls(fakeZed.argvFile, 2);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([
      [`${await realpath(fileUrlPath)}:37:1`],
      [`${await realpath(literalPercentPath)}:9:1`],
    ]);

    const zedUrl = "zed://ssh/my-mini/Users/dong/workspace/omp-channel-octo.local-file-link-resolver-plugins/docs/specs/2026-09-12-octo-channel-design.md";
    const remoteLink = await evaluate<ActionBox>(browser, pageSessionId, `(() => {
      const link = document.createElement("a");
      link.href = ${JSON.stringify(zedUrl)};
      link.textContent = "Remote Zed file";
      Object.assign(link.style, { position: "fixed", left: "24px", top: "230px", zIndex: "2147483647" });
      document.body.append(link);
      const rect = link.getBoundingClientRect();
      return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
    })()`);
    await dispatchMouseClick(browser, pageSessionId, remoteLink);
    await waitForZedCalls(fakeZed.argvFile, 3);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([
      [`${await realpath(fileUrlPath)}:37:1`],
      [`${await realpath(literalPercentPath)}:9:1`],
      [zedUrl],
    ]);

    const sanitizedLink = await evaluate<ActionBox>(browser, pageSessionId, `(() => {
      const link = document.createElement("a");
      link.href = "";
      link.setAttribute("node", "[object Object]");
      link.textContent = "Sanitized remote link";
      const source = "[Sanitized remote link](" + ${JSON.stringify(zedUrl)} + ")";
      link.__reactProps$fixture = { node: { position: { start: { offset: 0 }, end: { offset: source.length } } } };
      link.__reactFiber$fixture = { return: { memoizedProps: { children: source }, return: null } };
      Object.assign(link.style, { position: "fixed", left: "24px", top: "270px", zIndex: "2147483647" });
      document.body.append(link);
      const rect = link.getBoundingClientRect();
      return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
    })()`);
    await dispatchMouseClick(browser, pageSessionId, sanitizedLink);
    await waitForZedCalls(fakeZed.argvFile, 4);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([
      [`${await realpath(fileUrlPath)}:37:1`],
      [`${await realpath(literalPercentPath)}:9:1`],
      [zedUrl],
      [zedUrl],
    ]);

    const loadEvent = Promise.withResolvers<void>();
    const reloadTimer = setTimeout(() => loadEvent.reject(new Error("Timed out reloading OctoBuddy main renderer")), 30_000);
    browser.once(`Page.loadEventFired.${pageSessionId}`, () => {
      clearTimeout(reloadTimer);
      loadEvent.resolve();
    });
    try {
      await send(browser, "Page.reload", { ignoreCache: true }, pageSessionId);
      await loadEvent.promise;
    } finally {
      clearTimeout(reloadTimer);
    }
    const reloadedRows = await evaluate<boolean>(browser, pageSessionId, probeRowsExpression("zed-e2e-reloaded", fileUrl, rawAbsolutePath));
    expect(reloadedRows).toBe(true);
    const reloadedUrlAction = await waitForAction(browser, pageSessionId, "zed-e2e-reloaded-file-url-row");
    const reloadedPathAction = await waitForAction(browser, pageSessionId, "zed-e2e-reloaded-absolute-path-row");
    expect(reloadedUrlAction.accessibleName).toMatch(/zed/i);
    expect(reloadedPathAction.accessibleName).toMatch(/zed/i);
    expect(await linkState(browser, pageSessionId, "zed-e2e-reloaded")).toEqual({
      href: "https://example.invalid/unchanged",
      hasAction: false,
    });

    // The isolated, unauthenticated renderer starts without a selected chat. Later,
    // controlled channel values exercise its actual shared instance and observer.
    const communicationPagePath = join(
      app.bundlePath,
      "Contents",
      "Resources",
      "communication-renderer",
      "index.html",
    );
    expect((await stat(communicationPagePath)).isFile()).toBe(true);
    const communicationPageUrl = pathToFileURL(communicationPagePath).href;
    const communicationPath = new URL(communicationPageUrl).pathname;
    const communicationTarget = await waitForMainTarget(port, communicationPath);
    expect(pagePath(communicationTarget.url)).toBe(communicationPath);
    const communicationAttachment = await send<{ sessionId: string }>(browser, "Target.attachToTarget", {
      targetId: communicationTarget.id,
      flatten: true,
    });
    const communicationSessionId = communicationAttachment.sessionId;
    await send(browser, "Page.enable", {}, communicationSessionId);
    await send(browser, "Runtime.enable", {}, communicationSessionId);
    await waitUntil(async () => {
      try {
        return await evaluate<boolean>(browser!, communicationSessionId, "typeof window.__octobuddyStatusNavigation?.dispose === 'function'");
      } catch {
        return false;
      }
    }, "the installed main-world channel navigation observer", 10_000);
    const rendererProbe = await evaluate<{
      appModuleUrl: string;
      initialIdentity: RendererChatIdentity | null;
      observerInstalled: boolean;
    }>(browser, communicationSessionId, `(async () => {
      const appLinks = [...document.querySelectorAll('link[rel="modulepreload"]')]
        .filter((element) => /\\/App-[^/]+\\.js$/.test(element.href));
      if (appLinks.length !== 1) throw new Error("Expected one installed OctoBuddy App module preload");
      const appModule = await import(appLinks[0].href);
      const shared = appModule.i?.shared;
      if (!shared) throw new Error("Installed App module has no shared instance");
      const initialChannel = shared.openChannel;
      const identityOf = (channel) => channel
        && [1, 2, 5].includes(channel.channelType)
        && typeof channel.channelID === "string"
        ? { channelType: channel.channelType, channelId: channel.channelID }
        : null;
      const probe = { shared, originalOpenChannel: initialChannel, identityOf, events: [], listener: undefined };
      probe.listener = (event) => {
        const message = event.data;
        if (event.source === window && message?.type === "octobuddy-enhancer:chat") {
          probe.events.push({
            type: message.type,
            channelType: message.channelType ?? null,
            channelId: message.channelId ?? null,
          });
        }
      };
      Object.defineProperty(window, ${JSON.stringify(rendererChatProbeKey)}, { configurable: true, value: probe });
      window.addEventListener("message", probe.listener);
      return {
        appModuleUrl: appLinks[0].href,
        initialIdentity: identityOf(initialChannel),
        observerInstalled: typeof window.__octobuddyStatusNavigation?.dispose === "function",
      };
    })()`);
    expect(rendererProbe.appModuleUrl).toMatch(/\/App-[^/]+\.js$/);
    expect(rendererProbe.observerInstalled).toBe(true);
    expect(rendererProbe.initialIdentity).toBeNull();
    await evaluate(browser, communicationSessionId, `window.postMessage({ type: ${JSON.stringify(rendererChatRequestType)} }, "*")`);
    await waitUntil(async () => (await readRendererChatEvents(browser!, communicationSessionId)).length > 0,
      "the observer's initial selection from the installed App module", 10_000);
    expect(await readRendererChatEvents(browser, communicationSessionId)).toContainEqual({
      type: "octobuddy-enhancer:chat",
      channelType: null,
      channelId: null,
    });
    // The unauthenticated fixture keeps the communication view at zero size; emulate an open view.
    await send(browser, "Emulation.setDeviceMetricsOverride", {
      width: 1024, height: 768, deviceScaleFactor: 1, mobile: false,
    }, communicationSessionId);
    let communicationFrameTree: { frameTree: { frame: { url: string; parentId?: string } } } | undefined;
    await waitUntil(async () => {
      try {
        const frameTree = await send<{ frameTree: { frame: { url: string; parentId?: string } } }>(
          browser!,
          "Page.getFrameTree",
          {},
          communicationSessionId,
        );
        communicationFrameTree = frameTree;
        return pagePath(frameTree.frameTree.frame.url) === communicationPath;
      } catch {
        return false;
      }
    }, "the installed communication renderer main frame", 15_000);
    expect(communicationFrameTree?.frameTree.frame.parentId).toBeUndefined();

    await waitUntil(async () => {
      try {
        const tree = await readAccessibilityTree(browser!, communicationSessionId);
        return visibleAccessibilityNodes(tree).some((node) =>
          node.role?.value === "button" && node.name?.value === "频道状态：在线",
        );
      } catch {
        return false;
      }
    }, "the online channel-status control in the real page accessibility tree", 20_000);
    const initialStatusTree = await readAccessibilityTree(browser, communicationSessionId);
    const initialStatusNodes = visibleAccessibilityNodes(initialStatusTree);
    expect(initialStatusNodes.some((node) =>
      node.role?.value === "button" && node.name?.value === "频道状态：在线",
    )).toBe(true);
    expect(initialStatusNodes.some((node) =>
      node.role?.value === "region" && node.name?.value === "频道状态详情",
    )).toBe(false);
    const initialButton = await waitForStatusButton(browser, communicationSessionId);
    expect(initialButton.x + initialButton.width).toBeCloseTo(1024, 0);
    expect(initialButton.y + initialButton.height / 2).toBeCloseTo(384, 0);
    const initialOpacity = await evaluate<number>(browser, communicationSessionId,
      `Number(getComputedStyle(document.querySelector("[data-octobuddy-channel-status-button]")).opacity)`);
    expect(initialOpacity).toBeLessThan(1);
    const dragX = initialButton.x + initialButton.width / 2;
    const dragY = initialButton.y + initialButton.height / 2;
    await send(browser, "Input.dispatchMouseEvent", { type: "mouseMoved", x: dragX, y: dragY, button: "none", buttons: 0 }, communicationSessionId);
    expect(await evaluate<number>(browser, communicationSessionId,
      `Number(getComputedStyle(document.querySelector("[data-octobuddy-channel-status-button]")).opacity)`)).toBe(1);
    await send(browser, "Input.dispatchMouseEvent", { type: "mousePressed", x: dragX, y: dragY, button: "left", buttons: 1, clickCount: 1 }, communicationSessionId);
    await send(browser, "Input.dispatchMouseEvent", { type: "mouseMoved", x: dragX - 180, y: dragY + 100, button: "left", buttons: 1 }, communicationSessionId);
    await send(browser, "Input.dispatchMouseEvent", { type: "mouseReleased", x: dragX - 180, y: dragY + 100, button: "left", buttons: 0, clickCount: 1 }, communicationSessionId);
    const draggedButton = await waitForStatusButton(browser, communicationSessionId);
    expect(draggedButton.x).toBeLessThan(initialButton.x - 150);
    expect(draggedButton.y).toBeGreaterThan(initialButton.y + 70);
    expect(await evaluate<string>(browser, communicationSessionId,
      `document.querySelector("[data-octobuddy-channel-status-button]").getAttribute("aria-expanded")`)).toBe("false");


    await dispatchMouseClick(browser, communicationSessionId, await waitForStatusButton(browser, communicationSessionId));
    await waitUntil(async () => {
      try {
        const nodes = visibleAccessibilityNodes(await readAccessibilityTree(browser!, communicationSessionId));
        const text = nodes.map((node) => `${node.name?.value ?? ""} ${node.value?.value ?? ""}`).join(" ");
        return nodes.some((node) => node.role?.value === "region" && node.name?.value === "频道状态详情")
          && text.includes("服务状态：在线")
          && text.includes(String(localStatusSnapshot.pid))
          && text.includes("无法识别当前聊天");
      } catch {
        return false;
      }
    }, "the expanded channel-status details and local HTTP snapshot in the accessibility tree", 15_000);
    const expandedStatusNodes = visibleAccessibilityNodes(await readAccessibilityTree(browser, communicationSessionId));
    const expandedStatusText = expandedStatusNodes
      .map((node) => `${node.name?.value ?? ""} ${node.value?.value ?? ""}`)
      .join(" ");
    expect(expandedStatusNodes.some((node) =>
      node.role?.value === "region" && node.name?.value === "频道状态详情",
    )).toBe(true);
    expect(expandedStatusText).toContain("服务状态：在线");
    expect(expandedStatusText).toContain(String(localStatusSnapshot.pid));
    expect(expandedStatusText).toContain(String(localStatusSnapshot.runningSessions));
    expect(expandedStatusText).toContain(String(localStatusSnapshot.queuedTasks));
    expect(expandedStatusText).toContain("无法识别当前聊天");
    for (const name of ["状态", "上下文", "关联"]) {
      expect(expandedStatusNodes.some((node) => node.role?.value === "tab" && node.name?.value === name)).toBe(true);
    }
    const panelWidth = await evaluate<number>(browser, communicationSessionId,
      `document.querySelector("[data-octobuddy-channel-status-panel]").getBoundingClientRect().width`);
    expect(panelWidth).toBeGreaterThanOrEqual(400);
    const controlledGroup: RendererChannel = { channelID: "status-observer-test-group", channelType: 2 };
    const controlledThread: RendererChannel = { channelID: "status-observer-test-thread", channelType: 5 };
    const assignRendererChannel = async (channel: RendererChannel | null) =>
      evaluate<{
        before: number;
        after: number;
        identity: RendererChatIdentity | null;
      }>(browser!, communicationSessionId, `(() => {
        const probe = window[${JSON.stringify(rendererChatProbeKey)}];
        const before = probe.events.length;
        probe.shared.openChannel = ${JSON.stringify(channel)};
        return {
          before,
          after: probe.events.length,
          identity: probe.identityOf(probe.shared.openChannel),
        };
      })()`);
    const assertRendererEvent = async (identity: RendererChatIdentity | null, description: string, after: number) => {
      const expected: RendererChatEvent = {
        type: "octobuddy-enhancer:chat",
        channelType: identity?.channelType ?? null,
        channelId: identity?.channelId ?? null,
      };
      await waitUntil(async () => (await readRendererChatEvents(browser!, communicationSessionId)).slice(after).some((event) =>
        event.type === expected.type
        && event.channelType === expected.channelType
        && event.channelId === expected.channelId,
      ), description, 10_000);
      expect((await readRendererChatEvents(browser!, communicationSessionId)).slice(after)).toContainEqual(expected);
    };
    const rendererStatusText = async () => visibleAccessibilityNodes(
      await readAccessibilityTree(browser!, communicationSessionId),
    ).map((node) => `${node.name?.value ?? ""} ${node.value?.value ?? ""}`).join(" ");

    const groupAssignment = await assignRendererChannel(controlledGroup);
    expect(groupAssignment.identity).toEqual({ channelType: 2, channelId: controlledGroup.channelID });
    expect(groupAssignment.after).toBe(groupAssignment.before);
    await assertRendererEvent({ channelType: 2, channelId: controlledGroup.channelID },
      "silent shared.openChannel assignment to publish through the main-world observer", groupAssignment.after);
    await waitUntil(async () => {
      const text = await rendererStatusText();
      return text.includes(`群聊 ${controlledGroup.channelID}`) && text.includes("空闲");
    }, "the real panel to show the selected test group status", 10_000);

    const threadAssignment = await assignRendererChannel(controlledThread);
    expect(threadAssignment.identity).toEqual({ channelType: 5, channelId: controlledThread.channelID });
    expect(threadAssignment.after).toBe(threadAssignment.before);
    await assertRendererEvent({ channelType: 5, channelId: controlledThread.channelID },
      "silent shared.openChannel reassignment to publish a chat change", threadAssignment.after);
    await waitUntil(async () => {
      const text = await rendererStatusText();
      return text.includes(`话题 ${controlledThread.channelID}`) && text.includes("空闲");
    }, "the real panel to replace the old group with the selected thread status", 10_000);
    await evaluate(browser, communicationSessionId, `Array.from(document.querySelectorAll('[data-octobuddy-channel-status-panel] [role="tab"]'))
      .find((element) => element.textContent.trim() === "上下文").click()`);
    await waitUntil(async () => {
      const text = await rendererStatusText();
      return text.includes("test/model") && /4,?200/.test(text);
    }, "the real panel to render the selected thread's live context", 10_000);
    await evaluate(browser, communicationSessionId, `Array.from(document.querySelectorAll('[data-octobuddy-channel-status-panel] [role="tab"]'))
      .find((element) => element.textContent.trim() === "关联").click()`);
    await waitUntil(async () => {
      const text = await rendererStatusText();
      const hasLink = await evaluate<boolean>(browser!, communicationSessionId,
        `!!document.querySelector('a[href="https://github.com/example/project/pull/42"]')`);
      return hasLink && text.includes("面板验收任务");
    }, "the real panel to render the selected thread's associations", 10_000);
    await evaluate(browser, communicationSessionId, `Array.from(document.querySelectorAll('[data-octobuddy-channel-status-panel] [role="tab"]'))
      .find((element) => element.textContent.trim() === "状态").click()`);

    const clearedAssignment = await assignRendererChannel(null);
    expect(clearedAssignment.identity).toBeNull();
    expect(clearedAssignment.after).toBe(clearedAssignment.before);
    await assertRendererEvent(null, "silent shared.openChannel clearing to publish an unknown-chat update", clearedAssignment.after);
    await waitUntil(async () => (await rendererStatusText()).includes("无法识别当前聊天"),
      "the real panel to clear the previous chat after selection is removed", 10_000);
    await evaluate(browser, communicationSessionId, `(() => {
      const probe = window[${JSON.stringify(rendererChatProbeKey)}];
      probe.shared.openChannel = probe.originalOpenChannel;
    })()`);


    await dispatchMouseClick(browser, communicationSessionId, await waitForStatusButton(browser, communicationSessionId));
    await waitUntil(async () => {
      try {
        const nodes = visibleAccessibilityNodes(await readAccessibilityTree(browser!, communicationSessionId));
        return nodes.some((node) => node.role?.value === "button" && node.name?.value === "频道状态：在线")
          && !nodes.some((node) => node.role?.value === "region" && node.name?.value === "频道状态详情");
      } catch {
        return false;
      }
    }, "the collapsed channel-status control in the real page accessibility tree", 10_000);
    const statusRequests = statusFixture.requests();
    expect(statusRequests.length).toBeGreaterThan(0);
    expect(statusRequests.every((request) => {
      const url = new URL(request.path, statusFixture.baseUrl);
      return request.method === "GET"
        && request.authorization === `Bearer ${statusToken}`
        && ["/v1/status", "/v1/chats/status", "/v1/chats/details"].includes(url.pathname);
    })).toBe(true);
    for (const target of [controlledGroup, controlledThread]) {
      expect(statusRequests.some((request) => {
        const url = new URL(request.path, statusFixture.baseUrl);
        return url.pathname === "/v1/chats/status"
          && url.searchParams.get("channelType") === String(target.channelType)
          && url.searchParams.get("channelId") === target.channelID;
      })).toBe(true);
    }
    expect(statusRequests.some((request) => {
      const url = new URL(request.path, statusFixture.baseUrl);
      return url.pathname === "/v1/chats/details"
        && url.searchParams.get("channelType") === String(controlledThread.channelType)
        && url.searchParams.get("channelId") === controlledThread.channelID;
    })).toBe(true);

    await host.setPluginEnabled("channel-status", false);
    await waitUntil(async () => await evaluate<boolean>(browser!, communicationSessionId, `window.__octobuddyStatusNavigation === undefined
      && !document.querySelector("[data-octobuddy-channel-status-root]")`),
    "the real renderer to remove the observer and panel when disabled", 10_000);
    await host.setPluginEnabled("channel-status", true);
    await waitUntil(async () => await evaluate<boolean>(browser!, communicationSessionId, `typeof window.__octobuddyStatusNavigation?.dispose === "function"
      && document.querySelectorAll("[data-octobuddy-channel-status-root]").length === 1`),
    "the real renderer to restore exactly one observer and panel when enabled", 10_000);

    const disposedPrefix = "zed-e2e-reloaded";
    await host.stop();
    host = undefined;
    restoreHostEnvironment();
    restoreHostEnvironment = undefined;
    await waitUntil(async () => (await probeActionCount(browser!, pageSessionId!, disposedPrefix)) === 0,
      "Zed actions to disappear after host disposal", 10_000);
    await waitUntil(async () => {
      const ui = await evaluate<{ root: boolean; panel: boolean }>(browser!, communicationSessionId, `({
        root: document.querySelector("[data-octobuddy-channel-status-root]") !== null,
        panel: document.getElementById("octobuddy-channel-status-panel") !== null,
      })`);
      const nodes = visibleAccessibilityNodes(await readAccessibilityTree(browser!, communicationSessionId));
      return !ui.root && !ui.panel && !nodes.some((node) =>
        node.role?.value === "button" && node.name?.value?.startsWith("频道状态："),
      );
    }, "channel-status control to disappear after host disposal", 10_000);
    expect(await evaluate<boolean>(browser, communicationSessionId, "window.__octobuddyStatusNavigation === undefined")).toBe(true);


    const afterDisposeInserted = await evaluate<boolean>(
      browser,
      pageSessionId,
      probeRowsExpression("zed-e2e-after-dispose", fileUrl, rawAbsolutePath),
    );
    expect(afterDisposeInserted).toBe(true);
    await Bun.sleep(500);
    expect(await probeActionCount(browser, pageSessionId, "zed-e2e-after-dispose")).toBe(0);
    expect(await readZedArguments(fakeZed.argvFile)).toEqual([
      [`${await realpath(fileUrlPath)}:37:1`],
      [`${await realpath(literalPercentPath)}:9:1`],
      [zedUrl],
      [zedUrl],
    ]);
    expect(child.exitCode).toBeNull();
    expect(await processStartIdentity(childPid)).toBe(startedAt);
  } finally {
    try {
      await host?.stop();
    } finally {
      restoreHostEnvironment?.();
      try {
        await browser?.close();
      } finally {
        try {
          await statusServer?.close();
        } finally {
          try {
            await terminateProcessGroup(child);
          } finally {
            await rm(root, { recursive: true, force: true });
          }
        }
      }
    }
  }
}, testTimeoutMs);
