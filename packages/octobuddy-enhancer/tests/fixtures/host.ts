import { execFile, spawn } from "node:child_process";
import { createServer, type IncomingMessage } from "node:http";
import { createInterface } from "node:readline";
import { chmod, mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createContext as createVmContext, runInContext, type Context } from "node:vm";
import { promisify } from "node:util";
import { WebSocket, WebSocketServer } from "ws";

const execFileAsync = promisify(execFile);

type Frame = { id: string; url: string; parentId?: string };
type Target = { id: string; url: string; title?: string; frames: Frame[] };
type ProtocolCall = { targetId: string; method: string; params: Record<string, unknown> };
type ProtocolContext = { targetId: string; id: number; frameId: string; worldName: string; isDefault: boolean };
type Resolution = { callId: number; result: unknown; isError: boolean };
type Timer = { id: number; due: number; callback: (...args: unknown[]) => unknown; args: unknown[]; interval?: number };
type TestElement = {
  id: string;
  tagName: string;
  dataset: Record<string, string>;
  style: Record<string, unknown>;
  children: TestElement[];
  textContent: string;
  parentNode?: TestElement;
  appendChild(child: TestElement): TestElement;
  remove(): void;
  setAttribute(name: string, value: string): void;
  addEventListener(type: string, listener: (event: Record<string, unknown>) => void): void;
  removeEventListener(type: string, listener: (event: Record<string, unknown>) => void): void;
  dispatchEvent(event: Record<string, unknown> & { type: string }): boolean;
  querySelector(selector: string): TestElement | null;
};
type DocumentReadyState = "loading" | "interactive" | "complete";
type TestDocument = {
  readyState: DocumentReadyState;
  documentElement: TestElement;
  head: TestElement;
  body: TestElement;
  createElement(tagName: string): TestElement;
  getElementById(id: string): TestElement | null;
  querySelector(selector: string): TestElement | null;
  querySelectorAll(selector: string): TestElement[];
  addEventListener(type: string, listener: (event: Record<string, unknown>) => void): void;
  removeEventListener(type: string, listener: (event: Record<string, unknown>) => void): void;
  dispatchEvent(event: Record<string, unknown> & { type: string }): void;
};
type RuntimeContext = ProtocolContext & {
  sandbox: Record<string, unknown>;
  vm: Context;
  document: TestDocument;
  timers: Map<number, Timer>;
  now: number;
  resolutions: Resolution[];
};
type CdpReady = { type: "ready"; port: number; pid: number; startedAt: string };
type CdpSession = {
  targetId: string;
  socket: WebSocket;
  runtimeEnabled: boolean;
  pageEnabled: boolean;
  contexts: Map<string, ProtocolContext>;
  runtimes: Map<string, RuntimeContext>;
  bindings: Set<string>;
  scripts: Map<string, { source: string; worldName: string }>;
};

export interface FakeZedFixture {
  executable: string;
  argvFile: string;
  markerFile: string;
}
export interface GatedHostProcessOptions {
  appPath: string;
  port: number;
  pid: number;
  startedAt: string;
  pluginDirectories: string[];
  zedExecutable: string;
  gatePath: string;
}

export interface GatedHostProcessFixture {
  waitForRealpathGate(): Promise<void>;
  releaseRealpathGate(): void;
  stop(): Promise<void>;
}

export interface OwnedCdpFixture {
  port: number;
  pid: number;
  startedAt: string;
  addTarget(target: { id: string; url: string; title?: string; frames?: Frame[] }): Promise<void>;
  navigate(targetId: string, frameId: string, url: string): Promise<void>;
  navigateWithinDocument(targetId: string, frameId: string, url: string): Promise<void>;
  setReadyState(targetId: string, frameId: string, readyState: DocumentReadyState): Promise<void>;
  contexts(targetId: string): Promise<ProtocolContext[]>;
  context(targetId: string, frameId: string, worldName?: string): Promise<ProtocolContext | undefined>;
  callBinding(targetId: string, contextId: number, name: string, payload: string): Promise<void>;
  calls(targetId?: string): Promise<ProtocolCall[]>;
  evaluate(targetId: string, contextId: number, expression: string): Promise<unknown>;
  effects(targetId: string, frameId: string): Promise<{ dataset: Record<string, string>; elementIds: string[] }>;
  click(targetId: string, contextId: number, selector: string, isTrusted: boolean): Promise<boolean>;
  clickAndNavigate(targetId: string, contextId: number, selector: string, frameId: string, url: string): Promise<boolean>;
  advanceTimers(targetId: string, contextId: number, milliseconds: number): Promise<void>;
  resolutions(targetId: string, contextId: number): Promise<Resolution[]>;
  drain(): Promise<void>;
  close(): Promise<void>;
}
export async function createOwnedCdpFixture(options: { runMainWorldScripts?: boolean } = {}): Promise<OwnedCdpFixture> {
  const child = spawn(process.execPath, [
    fileURLToPath(import.meta.url),
    "--cdp-fixture-child",
    options.runMainWorldScripts === false ? "false" : "true",
  ], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stdout = createInterface({ input: child.stdout! });
  child.stderr?.resume();
  const ready = Promise.withResolvers<CdpReady>();
  stdout.on("line", (line) => {
    try {
      const value = JSON.parse(line) as Partial<CdpReady>;
      if (value.type === "ready" && typeof value.port === "number" && typeof value.pid === "number" && typeof value.startedAt === "string") {
        ready.resolve(value as CdpReady);
      }
    } catch { /* startup errors are reported by the child exit */ }
  });
  child.once("error", ready.reject);
  child.once("exit", (code, signal) => ready.reject(new Error(`CDP fixture exited before ready (code=${code}, signal=${signal})`)));

  let info: CdpReady;
  try { info = await ready.promise; }
  catch (error) { child.kill(); throw error; }

  const request = async <T>(path: string, body?: unknown): Promise<T> => {
    const options: RequestInit = { method: body === undefined ? "GET" : "POST" };
    if (body !== undefined) {
      options.headers = { "content-type": "application/json" };
      options.body = JSON.stringify(body);
    }
    const response = await fetch(`http://127.0.0.1:${info.port}${path}`, options);
    if (!response.ok) throw new Error(`CDP fixture ${path} returned HTTP ${response.status}: ${await response.text()}`);
    return await response.json() as T;
  };
  const readContexts = async (targetId: string) =>
    (await request<{ contexts: ProtocolContext[] }>(`/fixture/state?targetId=${encodeURIComponent(targetId)}`)).contexts;

  return {
    port: info.port,
    pid: info.pid,
    startedAt: info.startedAt,
    async addTarget(target) { await request("/fixture/targets", target); },
    async navigate(targetId, frameId, url) { await request("/fixture/navigate", { targetId, frameId, url }); },
    async navigateWithinDocument(targetId, frameId, url) {
      await request("/fixture/navigate-within-document", { targetId, frameId, url });
    },
    async setReadyState(targetId, frameId, readyState) {
      await request("/fixture/ready-state", { targetId, frameId, readyState });
    },
    async contexts(targetId) { return readContexts(targetId); },
    async context(targetId, frameId, worldName = "") {
      return (await readContexts(targetId)).find((context) => context.frameId === frameId && context.worldName === worldName);
    },
    async callBinding(targetId, contextId, name, payload) { await request("/fixture/binding", { targetId, contextId, name, payload }); },
    async calls(targetId) {
      const query = targetId === undefined ? "" : `?targetId=${encodeURIComponent(targetId)}`;
      return (await request<{ calls: ProtocolCall[] }>(`/fixture/calls${query}`)).calls;
    },
    async evaluate(targetId, contextId, expression) {
      return (await request<{ result: unknown }>("/fixture/evaluate", { targetId, contextId, expression })).result;
    },
    async effects(targetId, frameId) {
      return await request<{ dataset: Record<string, string>; elementIds: string[] }>(
        `/fixture/effects?targetId=${encodeURIComponent(targetId)}&frameId=${encodeURIComponent(frameId)}`,
      );
    },
    async click(targetId, contextId, selector, isTrusted) {
      return (await request<{ clicked: boolean }>("/fixture/click", { targetId, contextId, selector, isTrusted })).clicked;
    },
    async clickAndNavigate(targetId, contextId, selector, frameId, url) {
      return (await request<{ clicked: boolean }>("/fixture/click-and-navigate", {
        targetId, contextId, selector, frameId, url,
      })).clicked;
    },
    async advanceTimers(targetId, contextId, milliseconds) {
      await request("/fixture/advance-timers", { targetId, contextId, milliseconds });
    },
    async resolutions(targetId, contextId) {
      return (await request<{ resolutions: Resolution[] }>(
        `/fixture/resolutions?targetId=${encodeURIComponent(targetId)}&contextId=${contextId}`,
      )).resolutions;
    },
    async drain() { await request("/fixture/drain", {}); },
    async close() {
      const exited = Promise.withResolvers<void>();
      child.once("exit", () => exited.resolve());
      try { await request("/fixture/close", {}); }
      finally {
        const timeout = setTimeout(() => child.kill("SIGTERM"), 2_000);
        await exited.promise;
        clearTimeout(timeout);
        stdout.close();
      }
    },
  };
}

export async function writePlugin(
  root: string,
  options: {
    id: string;
    contentScripts: Array<{ matches: string[]; js: string[]; all_frames?: boolean; include_globs?: string[]; run_at?: "document_start" | "document_end" | "document_idle" }>;
    files?: Record<string, string>;
  },
): Promise<string> {
  const directory = join(root, options.id);
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, "manifest.json"), JSON.stringify({
    manifest_version: 3,
    id: options.id,
    name: options.id,
    version: "1.0.0",
    content_scripts: options.contentScripts,
  }, null, 2));
  for (const [relativePath, source] of Object.entries(options.files ?? {})) {
    const filePath = join(directory, relativePath);
    await mkdir(dirname(filePath), { recursive: true });
    await writeFile(filePath, source);
  }
  return directory;
}

export async function createFakeZed(directory: string): Promise<FakeZedFixture> {
  const executable = join(directory, "fake zed");
  const argvFile = join(directory, "zed argv.jsonl");
  const markerFile = join(directory, "shell-side-effect");
  await mkdir(directory, { recursive: true });
  await writeFile(executable, `#!/usr/bin/env node\nconst fs = require("node:fs");\nfs.appendFileSync(${JSON.stringify(argvFile)}, JSON.stringify(process.argv.slice(2)) + "\\n");\nfs.appendFileSync(${JSON.stringify(markerFile)}, "started\\n");\n`);
  await chmod(executable, 0o755);
  return { executable, argvFile, markerFile };
}
export async function createGatedHostProcessFixture(options: GatedHostProcessOptions): Promise<GatedHostProcessFixture> {
  const fixturePath = join(dirname(fileURLToPath(import.meta.url)), "host-gated-child.fixture.ts");
  const child = spawn(process.execPath, ["test", "--timeout", "30000", fixturePath], {
    cwd: join(dirname(fileURLToPath(import.meta.url)), "../.."),
    env: { ...process.env, OCTO_HOST_GATE_OPTIONS: JSON.stringify(options) },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const stdout = createInterface({ input: child.stdout! });
  let stderr = "";
  child.stderr?.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
  const exited = Promise.withResolvers<{ code: number | null; signal: NodeJS.Signals | null }>();
  let exitStatus: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  const queued = new Map<string, Record<string, unknown>>();
  type Waiter = {
    resolve(message: Record<string, unknown>): void;
    reject(error: Error): void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pending = new Map<string, Waiter[]>();
  child.once("exit", (code, signal) => {
    exitStatus = { code, signal };
    exited.resolve(exitStatus);
    for (const values of pending.values()) for (const waiter of values.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error(`gated host child exited (${code ?? signal}): ${stderr}`));
    }
  });
  stdout.on("line", (line) => {
    let message: Record<string, unknown>;
    try { message = JSON.parse(line) as Record<string, unknown>; }
    catch { return; }
    if (typeof message.type !== "string") return;
    const waiting = pending.get(message.type)?.shift();
    if (waiting) {
      clearTimeout(waiting.timer);
      waiting.resolve(message);
    } else queued.set(message.type, message);
  });
  child.once("error", (error) => {
    for (const values of pending.values()) for (const waiter of values.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
  });
  const waitFor = (type: string, timeoutMs = 10_000): Promise<Record<string, unknown>> => {
    const existing = queued.get(type);
    if (existing) {
      queued.delete(type);
      return Promise.resolve(existing);
    }
    if (exitStatus) return Promise.reject(new Error(`gated host child exited (${exitStatus.code ?? exitStatus.signal}): ${stderr}`));
    return new Promise((resolve, reject) => {
      const waiter: Waiter = {
        resolve,
        reject,
        timer: setTimeout(() => {
          const values = pending.get(type) ?? [];
          pending.set(type, values.filter((candidate) => candidate !== waiter));
          reject(new Error(`timed out waiting for gated host child ${type}: ${stderr}`));
        }, timeoutMs),
      };
      const values = pending.get(type) ?? [];
      values.push(waiter);
      pending.set(type, values);
    });
  };
  try {
    await waitFor("ready");
  } catch (error) {
    child.kill();
    throw error;
  }
  let stopPromise: Promise<void> | undefined;
  return {
    async waitForRealpathGate() {
      await waitFor("realpath-gate-entered");
    },
    releaseRealpathGate() {
      child.stdin?.write("release\n");
    },
    stop() {
      if (stopPromise) return stopPromise;
      stopPromise = (async () => {
        if (exitStatus) {
          const status = await exited.promise;
          if (status.code !== 0) throw new Error(`gated host child exited (${status.code ?? status.signal}): ${stderr}`);
          return;
        }
        child.stdin?.write("release\nstop\n");
        child.stdin?.end();
        const message = await waitFor("stopped");
        if (message.type !== "stopped") throw new Error(`gated host child failed to stop: ${stderr}`);
        const status = await exited.promise;
        if (status.code !== 0) throw new Error(`gated host child exited (${status.code ?? status.signal}): ${stderr}`);
      })();
      return stopPromise;
    },
  };
}

async function readJsonBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(Buffer.from(chunk));
  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
}

function createTestDocument(): TestDocument {
  const documentListeners = new Map<string, Array<(event: Record<string, unknown>) => void>>();
  const makeElement = (tagName: string): TestElement => {
    const listeners = new Map<string, Array<(event: Record<string, unknown>) => void>>();
    const attributes = new Map<string, string>();
    const element: TestElement = {
      id: "",
      tagName: tagName.toUpperCase(),
      dataset: {},
      style: {},
      children: [],
      textContent: "",
      appendChild(child) {
        child.parentNode?.children.splice(child.parentNode.children.indexOf(child), 1);
        child.parentNode = element;
        element.children.push(child);
        return child;
      },
      remove() {
        if (!element.parentNode) return;
        const siblings = element.parentNode.children;
        const index = siblings.indexOf(element);
        if (index >= 0) siblings.splice(index, 1);
        delete element.parentNode;
      },
      setAttribute(name, value) {
        attributes.set(name, value);
        if (name === "id") element.id = value;
        if (name.startsWith("data-")) {
          const key = name.slice(5).replace(/-([a-z])/g, (_match, letter: string) => letter.toUpperCase());
          element.dataset[key] = value;
        }
      },
      addEventListener(type, listener) {
        const values = listeners.get(type) ?? [];
        values.push(listener);
        listeners.set(type, values);
      },
      removeEventListener(type, listener) {
        listeners.set(type, (listeners.get(type) ?? []).filter((candidate) => candidate !== listener));
      },
      dispatchEvent(event) {
        const dispatched = { ...event, target: event.target ?? element, currentTarget: element, defaultPrevented: false };
        for (const listener of [...(listeners.get(event.type) ?? [])]) {
          try { listener(dispatched); } catch (error) { console.error("[fixture element event]", error); }
        }
        return dispatched.defaultPrevented !== true;
      },
      querySelector(selector) { return query(element, selector, false)[0] ?? null; },
    };
    Object.assign(element, {
      classList: {
        add: (...names: string[]) => { element.setAttribute("class", [...names].join(" ")); },
        remove: (...names: string[]) => {
          const old = attributes.get("class")?.split(/\s+/).filter(Boolean) ?? [];
          element.setAttribute("class", old.filter((name) => !names.includes(name)).join(" "));
        },
        toggle: (name: string, force?: boolean) => {
          const names = new Set(attributes.get("class")?.split(/\s+/).filter(Boolean) ?? []);
          const enabled = force ?? !names.has(name);
          if (enabled) names.add(name);
          else names.delete(name);
          element.setAttribute("class", [...names].join(" "));
          return enabled;
        },
        contains: (name: string) => attributes.get("class")?.split(/\s+/).includes(name) ?? false,
      },
      append: (...children: TestElement[]) => { for (const child of children) element.appendChild(child); },
      replaceChildren: (...children: TestElement[]) => {
        element.children.splice(0);
        for (const child of children) element.appendChild(child);
      },
      attachShadow: () => {
        const shadow = makeElement("shadow");
        Object.defineProperty(shadow, "innerHTML", {
          set(value: string) {
            shadow.children.splice(0);
            for (const match of value.matchAll(/<([a-zA-Z0-9]+)([^>]*)>/g)) {
              const child = makeElement(match[1] ?? "div");
              const id = /\bid="([^"]+)"/.exec(match[2] ?? "");
              if (id?.[1]) child.setAttribute("id", id[1]);
              shadow.appendChild(child);
            }
          },
        });
        return shadow;
      },
      getAttribute: (name: string) => attributes.get(name) ?? null,
      removeAttribute: (name: string) => attributes.delete(name),
    });
    return element;
  };
  const matches = (element: TestElement, selector: string): boolean => {
    if (selector.startsWith("#")) return element.id === selector.slice(1);
    const dataMatch = /^\[data-([a-z-]+)(?:=["']?([^"'\\]]+)["']?)?\]$/.exec(selector);
    if (dataMatch) {
      const key = dataMatch[1]!.replace(/-([a-z])/g, (_match, letter: string) => letter.toUpperCase());
      return dataMatch[2] === undefined ? key in element.dataset : element.dataset[key] === dataMatch[2];
    }
    const idMatch = /^\[id=["']?([^"'\\]]+)["']?\]$/.exec(selector);
    if (idMatch) return element.id === idMatch[1];
    return element.tagName.toLowerCase() === selector.toLowerCase();
  };
  const query = (root: TestElement, selector: string, includeRoot: boolean): TestElement[] => {
    const found: TestElement[] = [];
    const visit = (element: TestElement) => {
      if ((includeRoot || element !== root) && matches(element, selector)) found.push(element);
      for (const child of element.children) visit(child);
    };
    visit(root);
    return found;
  };
  const documentElement = makeElement("html");
  const head = makeElement("head");
  const body = makeElement("body");
  documentElement.appendChild(head);
  documentElement.appendChild(body);
  return {
    readyState: "complete",
    documentElement,
    head,
    body,
    createElement: makeElement,
    getElementById(id) { return query(documentElement, `#${id}`, true)[0] ?? null; },
    querySelector(selector) { return query(documentElement, selector, true)[0] ?? null; },
    querySelectorAll(selector) { return query(documentElement, selector, true); },
    addEventListener(type, listener) {
      const values = documentListeners.get(type) ?? [];
      values.push(listener);
      documentListeners.set(type, values);
    },
    removeEventListener(type, listener) {
      const values = documentListeners.get(type) ?? [];
      documentListeners.set(type, values.filter((candidate) => candidate !== listener));
    },
    dispatchEvent(event) {
      for (const listener of [...(documentListeners.get(event.type) ?? [])]) {
        try { listener(event); } catch (error) { console.error("[fixture document event]", error); }
      }
    },
  };
}

async function runCdpFixtureChild(): Promise<void> {
  const runMainWorldScripts = process.argv[3] !== "false";
  const targets = new Map<string, Target>();
  const calls: ProtocolCall[] = [];
  const contexts = new Map<string, ProtocolContext>();
  const documents = new Map<string, TestDocument>();
  const sessions = new Set<CdpSession>();
  let nextContextId = 1;
  let nextScriptId = 1;
  let port = 0;

  const send = (session: CdpSession, value: Record<string, unknown>) => {
    if (session.socket.readyState === WebSocket.OPEN) session.socket.send(JSON.stringify(value));
  };
  const emit = (session: CdpSession, method: string, params: Record<string, unknown>) => send(session, { method, params });
  const runtimeForId = (session: CdpSession, contextId: number) =>
    [...session.runtimes.values()].find((runtime) => runtime.id === contextId);
  // Capture the host's actual CDP resolution evaluation; the shim's resolver is intentionally immutable.
  const captureResolution = (runtime: RuntimeContext, expression: string, result: unknown) => {
    const match = /^window\.__ewResolve\?\.\((\d+),[\s\S]*,\s*(true|false)\);$/.exec(expression);
    if (!match) return;
    runtime.resolutions.push({ callId: Number(match[1]), result, isError: match[2] === "true" });
  };
  const runScript = (runtime: RuntimeContext, source: string) => {
    try { runInContext(source, runtime.vm, { timeout: 1_000 }); }
    catch { /* A bad injected script must not prevent other scripts from executing. */ }
  };
  const createContext = (session: CdpSession, frame: Frame, worldName = "") => {
    const key = `${frame.id}\u0000${worldName}`;
    const existing = session.contexts.get(key);
    if (existing) return existing;
    const context: ProtocolContext = {
      targetId: session.targetId,
      id: nextContextId++,
      frameId: frame.id,
      worldName,
      isDefault: worldName === "",
    };
    const documentKey = `${session.targetId}\u0000${frame.id}`;
    const document = documents.get(documentKey) ?? createTestDocument();
    documents.set(documentKey, document);
    let runtime!: RuntimeContext;
    let nextTimerId = 1;
    const schedule = (callback: unknown, delay: unknown, args: unknown[], interval?: number) => {
      const id = nextTimerId++;
      runtime.timers.set(id, {
        id,
        due: runtime.now + Math.max(0, Number(delay) || 0),
        callback: callback as Timer["callback"],
        args,
        ...(interval === undefined ? {} : { interval }),
      });
      return id;
    };
    const location = new URL(frame.url);
    const documentFacade = {
      get readyState() { return document.readyState; },
      set readyState(value: DocumentReadyState) { document.readyState = value; },
      documentElement: document.documentElement,
      head: document.head,
      body: document.body,
      createElement: (tagName: string) => document.createElement(tagName),
      getElementById: (id: string) => document.getElementById(id),
      querySelector: (selector: string) => document.querySelector(selector),
      querySelectorAll: (selector: string) => document.querySelectorAll(selector),
      addEventListener: document.addEventListener.bind(document),
      removeEventListener: document.removeEventListener.bind(document),
      dispatchEvent: document.dispatchEvent.bind(document),
    };
    const sandbox: Record<string, unknown> = {
      document: documentFacade,
      location: { href: location.href, origin: location.origin, pathname: location.pathname, hash: location.hash },
      navigator: { userAgent: "OctoBuddy host fixture" },
      console,
      URL,
      setTimeout: (callback: unknown, delay?: unknown, ...args: unknown[]) => schedule(callback, delay, args),
      clearTimeout: (id: number) => runtime.timers.delete(Number(id)),
      setInterval: (callback: unknown, delay?: unknown, ...args: unknown[]) =>
        schedule(callback, delay, args, Math.max(1, Number(delay) || 1)),
      clearInterval: (id: number) => runtime.timers.delete(Number(id)),
      requestAnimationFrame: (callback: (time: number) => unknown) => schedule(callback, 16, [runtime.now + 16]),
      cancelAnimationFrame: (id: number) => runtime.timers.delete(Number(id)),
      MutationObserver: class { observe() {} disconnect() {} takeRecords() { return []; } },
    };
    sandbox.window = sandbox;
    sandbox.self = sandbox;
    const target = targets.get(session.targetId);
    const mainFrame = target?.frames.find((candidate) => !candidate.parentId);
    const top = mainFrame ? session.runtimes.get(`${mainFrame.id}\u0000${worldName}`)?.sandbox : undefined;
    const parent = frame.parentId ? session.runtimes.get(`${frame.parentId}\u0000${worldName}`)?.sandbox : undefined;
    sandbox.top = top ?? sandbox;
    sandbox.parent = parent ?? sandbox;
    sandbox.frameElement = frame.parentId ? {} : null;
    const listeners = new Map<string, Array<(event: Record<string, unknown>) => void>>();
    sandbox.addEventListener = (type: string, listener: (event: Record<string, unknown>) => void) => {
      const values = listeners.get(type) ?? [];
      values.push(listener);
      listeners.set(type, values);
    };
    sandbox.removeEventListener = (type: string, listener: (event: Record<string, unknown>) => void) => {
      listeners.set(type, (listeners.get(type) ?? []).filter((candidate) => candidate !== listener));
    };
    sandbox.dispatchEvent = (event: Record<string, unknown> & { type: string }) => {
      for (const listener of [...(listeners.get(event.type) ?? [])]) {
        try { listener(event); } catch (error) { console.error("[fixture window event]", error); }
      }
      return true;
    };
    sandbox.postMessage = (data: unknown) => {
      for (const listener of listeners.get("message") ?? []) listener({ data, source: sandbox, origin: location.origin });
    };
    runtime = {
      ...context,
      sandbox,
      vm: createVmContext(sandbox),
      document,
      timers: new Map(),
      now: 0,
      resolutions: [],
    };
    sandbox.__ewResolve = () => undefined;
    session.contexts.set(key, context);
    session.runtimes.set(key, runtime);
    contexts.set(`${session.targetId}\u0000${context.id}`, context);
    for (const bindingName of session.bindings) {
      sandbox[bindingName] = (payload: string) => emit(session, "Runtime.bindingCalled", {
        name: bindingName,
        payload: String(payload),
        executionContextId: context.id,
      });
    }
    if (session.runtimeEnabled) {
      emit(session, "Runtime.executionContextCreated", {
        context: { id: context.id, origin: location.origin, name: worldName, auxData: { frameId: frame.id, isDefault: context.isDefault } },
      });
    }
    return context;
  };
  const runtimeFor = (session: CdpSession, frame: Frame, worldName = "") => {
    createContext(session, frame, worldName);
    return session.runtimes.get(`${frame.id}\u0000${worldName}`)!;
  };
  const runRegisteredScripts = (session: CdpSession, runtime: RuntimeContext) => {
    for (const script of session.scripts.values()) {
      if (script.worldName === runtime.worldName && (runtime.worldName !== "" || runMainWorldScripts)) {
        runScript(runtime, script.source);
      }
    }
  };
  const frameContexts = (session: CdpSession, frame: Frame, runScripts: boolean) => {
    const worlds = new Set(["", ...[...session.scripts.values()].map((script) => script.worldName)]);
    for (const worldName of worlds) {
      const key = `${frame.id}\u0000${worldName}`;
      const existed = session.runtimes.has(key);
      const runtime = runtimeFor(session, frame, worldName);
      if (runScripts && !existed) runRegisteredScripts(session, runtime);
    }
  };
  const elementIds = (document: TestDocument) => {
    const ids: string[] = [];
    const visit = (element: TestElement) => {
      if (element.id) ids.push(element.id);
      for (const child of element.children) visit(child);
    };
    visit(document.documentElement);
    return ids;
  };
  const timerAdvance = async (runtime: RuntimeContext, milliseconds: number) => {
    const end = runtime.now + Math.max(0, milliseconds);
    for (;;) {
      const next = [...runtime.timers.values()].filter((timer) => timer.due <= end).sort((a, b) => a.due - b.due)[0];
      if (!next) break;
      runtime.now = next.due;
      if (next.interval === undefined) runtime.timers.delete(next.id);
      try { await next.callback(...next.args); } catch { /* Timers are isolated from the fixture driver. */ }
      if (next.interval !== undefined && runtime.timers.has(next.id)) {
        runtime.timers.set(next.id, { ...next, due: next.due + next.interval });
      }
    }
    runtime.now = end;
  };
  const navigateFrame = (targetId: string, frameId: string, url: string): boolean => {
    const target = targets.get(targetId);
    const frame = target?.frames.find((candidate) => candidate.id === frameId);
    if (!target || !frame) return false;
    frame.url = url;
    if (!frame.parentId) target.url = frame.url;
    documents.set(`${targetId}\u0000${frameId}`, createTestDocument());
    for (const session of sessions) if (session.targetId === targetId) {
      for (const [key, context] of session.contexts) {
        if (context.frameId !== frameId) continue;
        emit(session, "Runtime.executionContextDestroyed", { executionContextId: context.id });
        session.contexts.delete(key);
        session.runtimes.delete(key);
        contexts.delete(`${targetId}\u0000${context.id}`);
      }
      emit(session, "Page.frameNavigated", { frame: { id: frame.id, url: frame.url, parentId: frame.parentId } });
      if (session.runtimeEnabled) frameContexts(session, frame, true);
    }
    return true;
  };
  const frameTree = (frame: Frame, target: Target): Record<string, unknown> => ({
    frame: { id: frame.id, url: frame.url, loaderId: `loader-${frame.id}` },
    childFrames: target.frames.filter((candidate) => candidate.parentId === frame.id).map((child) => frameTree(child, target)),
  });
  const respond = (session: CdpSession, id: number, result: Record<string, unknown> = {}) => send(session, { id, result });

  const webSockets = new WebSocketServer({ noServer: true });
  const serverClosed = Promise.withResolvers<void>();
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    const json = (value: unknown) => {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(value));
    };
    if (request.method === "GET" && url.pathname === "/json/list") {
      json([...targets.values()].map((target) => ({
        id: target.id,
        type: "page",
        title: target.title ?? "OctoBuddy test page",
        url: target.url,
        webSocketDebuggerUrl: `ws://127.0.0.1:${port}/devtools/page/${encodeURIComponent(target.id)}`,
      })));
      return;
    }
    if (request.method === "GET" && url.pathname === "/json/version") {
      json({ Browser: "Chrome/fixture", webSocketDebuggerUrl: `ws://127.0.0.1:${port}/devtools/browser/fixture` });
      return;
    }
    void (async () => {
      if (request.method === "GET" && url.pathname === "/fixture/state") {
        const targetId = url.searchParams.get("targetId");
        json({ contexts: [...contexts.values()].filter((context) => targetId === null || context.targetId === targetId) });
        return;
      }
      if (request.method === "GET" && url.pathname === "/fixture/calls") {
        const targetId = url.searchParams.get("targetId");
        json({ calls: targetId === null ? calls : calls.filter((call) => call.targetId === targetId) });
        return;
      }
      if (request.method === "GET" && url.pathname === "/fixture/effects") {
        const targetId = url.searchParams.get("targetId") ?? "";
        const frameId = url.searchParams.get("frameId") ?? "";
        const document = documents.get(`${targetId}\u0000${frameId}`);
        json({ dataset: { ...(document?.documentElement.dataset ?? {}) }, elementIds: document ? elementIds(document) : [] });
        return;
      }
      if (request.method === "GET" && url.pathname === "/fixture/resolutions") {
        const targetId = url.searchParams.get("targetId") ?? "";
        const contextId = Number(url.searchParams.get("contextId"));
        const session = [...sessions].find((candidate) => candidate.targetId === targetId);
        json({ resolutions: session ? runtimeForId(session, contextId)?.resolutions ?? [] : [] });
        return;
      }
      if (request.method !== "POST") { response.writeHead(404).end(); return; }
      const input = await readJsonBody(request);
      if (url.pathname === "/fixture/targets") {
        const id = String(input.id);
        const targetUrl = String(input.url);
        const frames = Array.isArray(input.frames) ? input.frames as Frame[] : [{ id: `${id}-main`, url: targetUrl }];
        const title = typeof input.title === "string" ? input.title : undefined;
        targets.set(id, { id, url: targetUrl, ...(title === undefined ? {} : { title }), frames });
        for (const frame of frames) documents.set(`${id}\u0000${frame.id}`, createTestDocument());
        json({ ok: true });
        return;
      }
      if (url.pathname === "/fixture/navigate") {
        const targetId = String(input.targetId);
        const frameId = String(input.frameId);
        if (!navigateFrame(targetId, frameId, String(input.url))) { response.writeHead(404).end(); return; }
        json({ ok: true });
        return;
      }
      if (url.pathname === "/fixture/navigate-within-document") {
        const targetId = String(input.targetId);
        const frameId = String(input.frameId);
        const target = targets.get(targetId);
        const frame = target?.frames.find((candidate) => candidate.id === frameId);
        if (!target || !frame) { response.writeHead(404).end(); return; }
        frame.url = String(input.url);
        if (!frame.parentId) target.url = frame.url;
        const location = new URL(frame.url);
        for (const session of sessions) if (session.targetId === targetId) {
          for (const runtime of session.runtimes.values()) {
            if (runtime.frameId !== frameId) continue;
            Object.assign(runtime.sandbox.location as Record<string, unknown>, {
              href: location.href,
              origin: location.origin,
              pathname: location.pathname,
              hash: location.hash,
            });
          }
          emit(session, "Page.navigatedWithinDocument", { frameId, url: frame.url, navigationType: "fragment" });
        }
        json({ ok: true });
        return;
      }
      if (url.pathname === "/fixture/ready-state") {
        const targetId = String(input.targetId);
        const frameId = String(input.frameId);
        const state = input.readyState;
        const target = targets.get(targetId);
        const frame = target?.frames.find((candidate) => candidate.id === frameId);
        const document = documents.get(`${targetId}\u0000${frameId}`);
        if (!target || !frame || !document || !["loading", "interactive", "complete"].includes(String(state))) {
          response.writeHead(404).end();
          return;
        }
        document.readyState = state as DocumentReadyState;
        const eventNames = state === "loading" ? [] : state === "interactive" ? ["readystatechange", "DOMContentLoaded"] : [
          "readystatechange", "DOMContentLoaded", "readystatechange", "load",
        ];
        for (const eventName of eventNames) {
          const event = { type: eventName, target: document, isTrusted: true };
          document.dispatchEvent(event);
          for (const session of sessions) if (session.targetId === targetId) {
            for (const runtime of session.runtimes.values()) {
              if (runtime.frameId !== frameId) continue;
              (runtime.sandbox.dispatchEvent as ((event: Record<string, unknown> & { type: string }) => unknown) | undefined)?.call(runtime.sandbox, event);
            }
            if (eventName === "DOMContentLoaded" || eventName === "load") {
              emit(session, "Page.lifecycleEvent", {
                frameId,
                loaderId: `loader-${frameId}`,
                name: eventName,
                timestamp: Date.now() / 1_000,
              });
            }
          }
        }
        if (state === "complete") for (const session of sessions) if (session.targetId === targetId) {
          emit(session, "Page.lifecycleEvent", {
            frameId,
            loaderId: `loader-${frameId}`,
            name: "networkIdle",
            timestamp: Date.now() / 1_000,
          });
        }
        json({ ok: true });
        return;
      }
      if (url.pathname === "/fixture/evaluate") {
        const session = [...sessions].find((candidate) => candidate.targetId === String(input.targetId));
        const runtime = session ? runtimeForId(session, Number(input.contextId)) : undefined;
        if (!runtime) { json({ result: undefined }); return; }
        const result = runInContext(String(input.expression ?? ""), runtime.vm, { timeout: 1_000 });
        json({ result: result === undefined ? null : result });
        return;
      }
      if (url.pathname === "/fixture/click-and-navigate") {
        const session = [...sessions].find((candidate) => candidate.targetId === String(input.targetId));
        const runtime = session ? runtimeForId(session, Number(input.contextId)) : undefined;
        const element = runtime?.document.querySelector(String(input.selector));
        const clicked = element?.dispatchEvent({ type: "click", isTrusted: true }) ?? false;
        const navigated = navigateFrame(String(input.targetId), String(input.frameId), String(input.url));
        if (!navigated) { response.writeHead(404).end(); return; }
        json({ clicked });
        return;
      }
      if (url.pathname === "/fixture/click") {
        const session = [...sessions].find((candidate) => candidate.targetId === String(input.targetId));
        const runtime = session ? runtimeForId(session, Number(input.contextId)) : undefined;
        const element = runtime?.document.querySelector(String(input.selector));
        const clicked = element?.dispatchEvent({ type: "click", isTrusted: input.isTrusted === true }) ?? false;
        json({ clicked });
        return;
      }
      if (url.pathname === "/fixture/advance-timers") {
        const session = [...sessions].find((candidate) => candidate.targetId === String(input.targetId));
        const runtime = session ? runtimeForId(session, Number(input.contextId)) : undefined;
        if (runtime) await timerAdvance(runtime, Number(input.milliseconds));
        json({ ok: Boolean(runtime) });
        return;
      }
      if (url.pathname === "/fixture/drain") {
        await new Promise((resolve) => setTimeout(resolve, 100));
        json({ ok: true });
        return;
      }
      if (url.pathname === "/fixture/binding") {
        const session = [...sessions].find((candidate) => candidate.targetId === String(input.targetId));
        if (session) emit(session, "Runtime.bindingCalled", {
          name: String(input.name),
          payload: String(input.payload),
          executionContextId: Number(input.contextId),
        });
        json({ ok: Boolean(session) });
        return;
      }
      if (url.pathname === "/fixture/close") {
        json({ ok: true });
        queueMicrotask(() => {
          for (const session of sessions) session.socket.close();
          webSockets.close();
          server.close();
        });
        return;
      }
      response.writeHead(404).end();
    })().catch((error: unknown) => {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
    });
  });

  server.on("close", () => serverClosed.resolve());
  server.on("upgrade", (request, socket, head) => {
    if (!/^\/devtools\/page\/[^/]+$/.test(request.url ?? "")) { socket.destroy(); return; }
    webSockets.handleUpgrade(request, socket, head, (webSocket) => webSockets.emit("connection", webSocket, request));
  });
  webSockets.on("connection", (socket, request) => {
    const targetId = decodeURIComponent(new URL(request.url ?? "/", "http://127.0.0.1").pathname.split("/").pop() ?? "");
    const session: CdpSession = {
      targetId,
      socket,
      runtimeEnabled: false,
      pageEnabled: false,
      contexts: new Map(),
      runtimes: new Map(),
      bindings: new Set(),
      scripts: new Map(),
    };
    sessions.add(session);
    socket.on("message", (raw) => {
      let message: { id: number; method: string; params?: Record<string, unknown> };
      try { message = JSON.parse(raw.toString()) as typeof message; } catch { return; }
      const target = targets.get(session.targetId);
      if (!target) return;
      const params = message.params ?? {};
      calls.push({ targetId: session.targetId, method: message.method, params });
      switch (message.method) {
        case "Runtime.enable":
          session.runtimeEnabled = true;
          respond(session, message.id);
          if (session.pageEnabled) for (const frame of target.frames) frameContexts(session, frame, true);
          break;
        case "Page.enable":
          session.pageEnabled = true;
          respond(session, message.id);
          for (const frame of target.frames) emit(session, "Page.frameNavigated", { frame: { id: frame.id, url: frame.url, parentId: frame.parentId } });
          if (session.runtimeEnabled) for (const frame of target.frames) frameContexts(session, frame, true);
          break;
        case "Runtime.addBinding": {
          const name = String(params.name ?? "");
          session.bindings.add(name);
          for (const runtime of session.runtimes.values()) {
            runtime.sandbox[name] = (payload: string) => emit(session, "Runtime.bindingCalled", {
              name,
              payload: String(payload),
              executionContextId: runtime.id,
            });
          }
          respond(session, message.id);
          break;
        }
        case "Page.createIsolatedWorld": {
          const frame = target.frames.find((candidate) => candidate.id === String(params.frameId));
          if (!frame) { respond(session, message.id, {}); break; }
          const worldName = String(params.worldName ?? "");
          const runtime = runtimeFor(session, frame, worldName);
          runRegisteredScripts(session, runtime);
          respond(session, message.id, { executionContextId: runtime.id });
          break;
        }
        case "Page.addScriptToEvaluateOnNewDocument": {
          const identifier = `fixture-script-${nextScriptId++}`;
          const script = { source: String(params.source ?? ""), worldName: String(params.worldName ?? "") };
          session.scripts.set(identifier, script);
          if (params.runImmediately === true && session.runtimeEnabled) {
            for (const frame of target.frames) {
              if (script.worldName === "" && !runMainWorldScripts) continue;
              const runtime = runtimeFor(session, frame, script.worldName);
              runScript(runtime, script.source);
            }
          }
          respond(session, message.id, { identifier });
          break;
        }
        case "Page.removeScriptToEvaluateOnNewDocument":
          session.scripts.delete(String(params.identifier ?? ""));
          respond(session, message.id);
          break;
        case "Page.getFrameTree": {
          const main = target.frames.find((frame) => !frame.parentId);
          respond(session, message.id, main ? { frameTree: frameTree(main, target) } : {});
          break;
        }
        case "Page.reload":
        case "Runtime.runIfWaitingForDebugger":
          respond(session, message.id);
          break;
        case "Runtime.evaluate": {
          const runtime = runtimeForId(session, Number(params.contextId));
          if (!runtime) { respond(session, message.id, { result: { type: "undefined" } }); break; }
          let result: unknown;
          const expression = String(params.expression ?? "");
          try {
            result = runInContext(expression, runtime.vm, { timeout: 1_000 });
            captureResolution(runtime, expression, result);
          } catch (error) {
            respond(session, message.id, {
              result: { type: "undefined" },
              exceptionDetails: { text: error instanceof Error ? error.message : String(error) },
            });
            break;
          }
          const sendResult = (value: unknown) => {
            const type = value === null ? "object" : typeof value;
            let remote: Record<string, unknown> = { type };
            if (value === null) remote.subtype = "null";
            if (value === null || type === "string" || type === "number" || type === "boolean") remote.value = value;
            else if (value !== undefined) {
              try { remote.value = JSON.parse(JSON.stringify(value)); } catch { /* Remote object is not by-value serializable. */ }
            }
            respond(session, message.id, { result: remote });
          };
          if (params.awaitPromise === true && result && typeof (result as PromiseLike<unknown>).then === "function") {
            void Promise.resolve(result).then(sendResult, () => respond(session, message.id, { result: { type: "undefined" } }));
          } else sendResult(result);
          break;
        }
        default:
          respond(session, message.id);
      }
    });
    socket.on("close", () => {
      sessions.delete(session);
      for (const context of session.contexts.values()) contexts.delete(`${session.targetId}\u0000${context.id}`);
      session.contexts.clear();
      session.runtimes.clear();
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => { server.off("error", reject); resolve(); });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("CDP fixture failed to bind a TCP port");
  port = address.port;
  const { stdout } = await execFileAsync("ps", ["-p", String(process.pid), "-o", "lstart="], {
    encoding: "utf8",
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
  });
  process.stdout.write(`${JSON.stringify({ type: "ready", port, pid: process.pid, startedAt: stdout.trim() })}\n`);
  await serverClosed.promise;
}

if (process.argv[2] === "--cdp-fixture-child") await runCdpFixtureChild();
