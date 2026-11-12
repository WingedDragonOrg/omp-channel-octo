import { createRequire } from "node:module";
import { readFile, realpath } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { isRecord } from "./type-guards.ts";
import { assertOwnedCdp } from "../launcher/identity.ts";
import {
  allowedOctoBuddyPageUrls,
  buildContentScriptSource,
  isAllowedOctoBuddyUrl,
  loadPlugins,
  managementPluginId,
  type HostPlugin,
} from "./plugins.ts";
import { openInZed } from "./zed.ts";
import { createStatusClient, type ChatTarget, type StatusClient } from "./status-client.ts";

// Restricted electron-webext host fork: retain its CDP attachment, named-world injection, and Runtime.addBinding model while exposing only OctoBuddy capabilities.

const require = createRequire(import.meta.url);
const connect = require("chrome-remote-interface") as CdpConnector;
const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const vendorDirectory = join(packageDirectory, "vendor", "electron-webext", "framework");
const bindingName = "__ewSend";
const managementWorld = `octo-${managementPluginId}`;
const startupTimeoutMs = 10_000;

interface CdpConnector {
  (options: { target: string; local: true }): Promise<CdpClient>;
}

interface CdpClient {
  Page: {
    enable(): Promise<unknown>;
    getFrameTree(): Promise<{ frameTree?: FrameTree }>
    addScriptToEvaluateOnNewDocument(params: { source: string; worldName?: string; runImmediately: true }): Promise<{ identifier: string }>;
    removeScriptToEvaluateOnNewDocument(params: { identifier: string }): Promise<unknown>;
    frameNavigated(handler: (event: { frame: FrameInfo }) => void): unknown;
  };
  Runtime: {
    enable(): Promise<unknown>;
    addBinding(params: { name: string }): Promise<unknown>;
    evaluate(params: { expression: string; contextId?: number; returnByValue?: boolean; awaitPromise?: boolean }): Promise<EvaluationResult>;
    executionContextCreated(handler: (event: { context: ExecutionContextInfo }) => void): unknown;
    executionContextDestroyed(handler: (event: { executionContextId: number }) => void): unknown;
    executionContextsCleared(handler: () => void): unknown;
    bindingCalled(handler: (event: BindingCall) => void): unknown;
    exceptionThrown(handler: (event: { exceptionDetails?: { text?: string; exception?: { description?: string }; executionContextId?: number } }) => void): unknown;
  };
  on(event: string, listener: (...args: unknown[]) => void): unknown;
  close(): Promise<unknown> | unknown;
}

interface FrameInfo {
  id: string;
  url: string;
  parentId?: string;
}

interface FrameTree {
  frame: FrameInfo;
}

interface ExecutionContextInfo {
  id: number;
  name?: string;
  auxData?: { frameId?: string; isDefault?: boolean };
}

interface BindingCall {
  name: string;
  payload: string;
  executionContextId: number;
}

interface EvaluationResult {
  result?: { value?: unknown };
  exceptionDetails?: { text?: string; exception?: { description?: string } };
}

interface TargetInfo {
  id: string;
  type: string;
  url: string;
  webSocketDebuggerUrl: string;
}

interface HostIdentity {
  pid: number;
  startedAt: string;
  port: number;
  appPath: string;
}

export interface HostStartOptions extends HostIdentity {
  pluginDirectories: string[];
  zedExecutable?: string;
  statusConfigPath?: string;
}

export interface HostSession {
  stop(): Promise<void>;
  setPluginEnabled(pluginId: string, enabled: boolean): Promise<void>;
}

interface ContextOwner {
  pluginId: string;
  frameId: string;
}

type StatusRequestKind = "service" | "chat-status" | "chat-details";

interface StatusRequest {
  pluginId: string;
  targetId: string;
  contextId: number;
  requestKind: StatusRequestKind;
  requestId?: number;
  controller: AbortController;
}

interface TargetAttachment {
  target: TargetInfo;
  client: CdpClient;
  rootFrameId: string;
  rootUrl: string;
  allowed: boolean;
  closed: boolean;
  contexts: Map<number, ContextOwner>;
  scriptIds: Map<string, string[]>;
  mainWorldContextId: number | undefined;
}

interface BridgeRequest {
  callId: number;
  api: string;
  method: string;
  args: unknown[];
  trustedClick: boolean;
}

interface ManagementAsset {
  fileName: string;
  source: string;
}

function notice(message: string): void {
  process.stderr.write(`[octobuddy-enhancer] ${message}\n`);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
const cdpOperationTimeoutMs = 3_000;

async function withCdpTimeout<T>(
  operation: Promise<T>,
  label: string,
  timeoutMs = cdpOperationTimeoutMs,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`CDP ${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function connectTarget(webSocketDebuggerUrl: string): Promise<CdpClient> {
  const pending = connect({ target: webSocketDebuggerUrl, local: true });
  try {
    return await withCdpTimeout(pending, "target websocket connection");
  } catch (error) {
    void pending.then((client) => Promise.resolve(client.close()).catch(() => undefined), () => undefined);
    throw error;
  }
}

function worldName(pluginId: string): string {
  return `octo-${pluginId}`;
}

function extensionInfo(plugin: HostPlugin): Record<string, unknown> {
  return {
    id: plugin.id,
    name: plugin.name,
    version: plugin.version,
    description: plugin.description,
    enabled: plugin.enabled,
    type: "extension",
    installType: plugin.builtin ? "admin" : "development",
    mayDisable: !plugin.builtin,
    errors: [...plugin.errors],
  };
}

function makeManagementPlugin(): HostPlugin {
  return {
    id: managementPluginId,
    name: "OctoBuddy Enhancer",
    version: "0.1.0",
    description: "Manage trusted local plugins.",
    builtin: true,
    enabled: true,
    directory: vendorDirectory,
    units: [],
    errors: [],
    generation: 0,
    cleanupBlockedContexts: new Set(),
  };
}

function trustedTargetSocket(value: string, port: number): boolean {
  try {
    const url = new URL(value);
    const loopback = url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]";
    return url.protocol === "ws:" && loopback && Number(url.port) === port
      && url.username === "" && url.password === "" && url.pathname.startsWith("/devtools/page/");
  } catch {
    return false;
  }
}

function targetFrom(value: unknown, port: number, appPath: string): TargetInfo | undefined {
  if (!isRecord(value)) return undefined;
  const target = value;
  if (target.type !== "page" || typeof target.id !== "string" || target.id.length === 0) return undefined;
  if (typeof target.url !== "string" || !isAllowedOctoBuddyUrl(target.url, appPath)) return undefined;
  if (typeof target.webSocketDebuggerUrl !== "string" || !trustedTargetSocket(target.webSocketDebuggerUrl, port)) return undefined;
  return {
    id: target.id,
    type: "page",
    url: target.url,
    webSocketDebuggerUrl: target.webSocketDebuggerUrl,
  };
}

function isCommunicationRendererUrl(value: string, appPath: string): boolean {
  try {
    const url = new URL(value);
    if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return false;
    url.hash = "";
    return url.href === allowedOctoBuddyPageUrls(appPath)[0];
  } catch {
    return false;
  }
}

async function loadManagementAssets(): Promise<{ shim: string; scripts: ManagementAsset[] }> {
  const shim = await readFile(join(vendorDirectory, "host", "shim.js"), "utf8");
  const scripts = await Promise.all(["toolbar.js", "panel.js"].map(async (fileName) => ({
    fileName,
    source: await readFile(join(vendorDirectory, "extensions", "_management", fileName), "utf8"),
  })));
  return { shim, scripts };
}

function identitySource(pluginId: string, isManagement: boolean): string {
  const value = JSON.stringify({ id: pluginId, management: isManagement });
  return `if (!Object.prototype.hasOwnProperty.call(window, "__ewSelfInfo")) Object.defineProperty(window, "__ewSelfInfo", { configurable: false, enumerable: false, writable: false, value: Object.freeze(${value}) });`;
}

function worldBootstrapSource(pluginId: string, isManagement: boolean, shim: string, appPath: string): string {
  const allowedUrls = JSON.stringify(allowedOctoBuddyPageUrls(appPath));
  return `(() => {
    if (window !== window.top) return;
    let url;
    try { url = new URL(window.location.href); } catch { return; }
    if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return;
    url.hash = "";
    if (!${allowedUrls}.includes(url.href)) return;
    ${identitySource(pluginId, isManagement)}
    ${shim}
  })();`;
}

function managementScriptSource(source: string, appPath: string): string {
  const allowedUrls = JSON.stringify(allowedOctoBuddyPageUrls(appPath));
  return `(() => {
    if (window !== window.top) return;
    let url;
    try { url = new URL(window.location.href); } catch { return; }
    if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return;
    url.hash = "";
    if (!${allowedUrls}.includes(url.href)) return;
    const run = () => { if (window === window.top) {\n${source}\n} };
    if (document.readyState === "complete" || document.readyState === "interactive") run();
    else { const ready = () => run(); document.addEventListener("DOMContentLoaded", ready, { once: true }); window.__ewHostRegisterDispose(() => document.removeEventListener("DOMContentLoaded", ready)); }
  })();`;
}
function statusNavigationSource(source: string, appPath: string): string {
  const communicationUrl = JSON.stringify(allowedOctoBuddyPageUrls(appPath)[0] ?? "");
  return `(() => {
    if (window !== window.top) return;
    let url;
    try { url = new URL(window.location.href); } catch { return; }
    if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return;
    url.hash = "";
    if (url.href !== ${communicationUrl}) return;
    ${source}
  })();`;
}

function parseBridgeRequest(payload: string): BridgeRequest | undefined {
  if (payload.length > 64 * 1024) return undefined;
  let value: unknown;
  try { value = JSON.parse(payload); }
  catch { return undefined; }
  if (!isRecord(value)) return undefined;
  const request = value;
  if (typeof request.callId !== "number" || !Number.isSafeInteger(request.callId) || request.callId < 1) return undefined;
  if (typeof request.api !== "string" || typeof request.method !== "string") return undefined;
  if (!Array.isArray(request.args)) return undefined;
  return {
    callId: request.callId as number,
    api: request.api,
    method: request.method,
    args: request.args,
    trustedClick: request.trustedClick === true,
  };
}
function isStatusRequestId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function contextKey(targetId: string, contextId: number): string {
  return `${targetId}:${contextId}`;
}

function sourceExpression(value: unknown): string {
  return value === undefined ? "undefined" : JSON.stringify(value) ?? "null";
}

function disposalProblems(value: unknown): string[] {
  if (!isRecord(value)) return ["the isolated-world cleanup report is unavailable"];
  const problems: string[] = [];
  for (const name of ["count", "ran", "residualNodes", "residualListeners", "residualTimers"] as const) {
    const count = value[name];
    if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) {
      problems.push(`cleanup report contains an invalid ${name}`);
    }
  }
  if (typeof value.residualNodes === "number" && value.residualNodes > 0) {
    problems.push(`${value.residualNodes} DOM node(s) remain connected`);
  }
  if (typeof value.residualListeners === "number" && value.residualListeners > 0) {
    problems.push(`${value.residualListeners} event listener(s) remain active`);
  }
  if (typeof value.residualTimers === "number" && value.residualTimers > 0) {
    problems.push(`${value.residualTimers} timer(s) remain active`);
  }
  if (typeof value.uncertain !== "boolean") problems.push("cleanup report omits tracking certainty");
  else if (value.uncertain) problems.push("cleanup tracking was incomplete");
  if (!Array.isArray(value.errors)) problems.push("cleanup report omits its error list");
  else if (value.errors.length > 0) problems.push(...value.errors.map(String));
  return problems;
}


class OctoBuddyHost {
  readonly #identity: HostIdentity;
  readonly #plugins: Map<string, HostPlugin>;
  readonly #shim: string;
  readonly #managementScripts: ManagementAsset[];
  readonly #zedExecutable: string | undefined;
  readonly #builtinZedDirectory: string | undefined;
  readonly #builtinStatusDirectory: string | undefined;
  readonly #statusNavigationScript: string | undefined;
  readonly #statusClient: StatusClient;
  readonly #statusRequests = new Set<StatusRequest>();
  readonly #worldOwners = new Map<string, string>();
  readonly #attachments = new Map<string, TargetAttachment>();
  readonly #pluginTransitions = new Map<string, Promise<void>>();
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #discovering: Promise<void> | undefined;
  #stopPromise: Promise<void> | undefined;
  #stopped = false;

  constructor(
    identity: HostIdentity,
    plugins: Map<string, HostPlugin>,
    assets: { shim: string; scripts: ManagementAsset[] },
    zedExecutable: string | undefined,
    builtinZedDirectory: string | undefined,
    builtinStatusDirectory: string | undefined,
    statusNavigationScript: string | undefined,
    statusClient: StatusClient,
  ) {
    this.#identity = identity;
    this.#plugins = plugins;
    this.#plugins.set(managementPluginId, makeManagementPlugin());
    this.#shim = assets.shim;
    this.#managementScripts = assets.scripts;
    this.#zedExecutable = zedExecutable;
    this.#builtinZedDirectory = builtinZedDirectory;
    this.#builtinStatusDirectory = builtinStatusDirectory;
    this.#statusNavigationScript = statusNavigationScript;
    this.#statusClient = statusClient;
    for (const plugin of this.#plugins.values()) this.#worldOwners.set(worldName(plugin.id), plugin.id);
  }

  async start(): Promise<void> {
    const deadline = Date.now() + startupTimeoutMs;
    while (!this.#stopped && Date.now() < deadline) {
      await withCdpTimeout(this.discover(), "initial target discovery and attachment", Math.max(1, deadline - Date.now()));
      if (this.#attachments.size > 0) break;
      const { promise, resolve } = Promise.withResolvers<void>();
      setTimeout(resolve, 200);
      await promise;
    }
    if (this.#attachments.size === 0) throw new Error("no eligible OctoBuddy page was attached before the host startup deadline");
    this.#pollTimer = setInterval(() => {
      void this.discover().catch((error: unknown) => {
        notice(`target discovery failed: ${errorMessage(error)}`);
      });
    }, 750);
  }

  setPluginEnabled(pluginId: string, enabled: boolean): Promise<void> {
    return this.queuePluginTask(pluginId, () => this.applyPluginEnabled(pluginId, enabled));
  }

  private queuePluginTask(pluginId: string, task: () => Promise<void>): Promise<void> {
    const previous = this.#pluginTransitions.get(pluginId) ?? Promise.resolve();
    const operation = previous.catch(() => undefined).then(task);
    this.#pluginTransitions.set(pluginId, operation);
    return operation.finally(() => {
      if (this.#pluginTransitions.get(pluginId) === operation) this.#pluginTransitions.delete(pluginId);
    });
  }

  private cancelStatusRequests(
    reason: "disabled" | "expired" | "stopped",
    pluginId?: string,
    targetId?: string,
    contextId?: number,
  ): void {
    for (const request of this.#statusRequests) {
      if (pluginId !== undefined && request.pluginId !== pluginId) continue;
      if (targetId !== undefined && request.targetId !== targetId) continue;
      if (contextId !== undefined && request.contextId !== contextId) continue;
      if (request.controller.signal.aborted) continue;
      request.controller.abort(reason === "disabled" ? new Error("plugin is disabled") : undefined);
    }
  }

  private async applyPluginEnabled(pluginId: string, enabled: boolean): Promise<void> {
    if (this.#stopped) throw new Error("host is stopped");
    await assertOwnedCdp(this.#identity);
    if (this.#stopped) throw new Error("host is stopped");
    const plugin = this.#plugins.get(pluginId);
    if (!plugin || plugin.builtin) throw new Error(`plugin cannot be changed: ${pluginId}`);
    if (plugin.enabled === enabled) return;
    if (enabled && plugin.cleanupBlockedContexts.size > 0) {
      throw new Error("plugin effects could not be cleaned; navigate the affected OctoBuddy page before enabling it again");
    }
    if (enabled && [...this.#attachments.values()].some((attachment) => attachment.scriptIds.has(pluginId))) {
      throw new Error("plugin scripts could not be removed; close and reopen the affected OctoBuddy page before enabling it again");
    }

    plugin.enabled = enabled;
    if (!enabled) this.cancelStatusRequests("disabled", pluginId);
    plugin.generation += 1;
    const failures: Error[] = [];
    if (!enabled) {
      for (const attachment of this.#attachments.values()) {
        try { await this.removePluginScripts(attachment, plugin.id); }
        catch (error) {
          failures.push(error instanceof Error ? error : new Error(String(error)));
        }
        if (this.isBuiltinStatusPlugin(plugin)) {
          try { await this.disposeStatusNavigation(attachment, "plugin disable"); }
          catch (error) {
            if (attachment.mainWorldContextId !== undefined) {
              plugin.cleanupBlockedContexts.add(contextKey(attachment.target.id, attachment.mainWorldContextId));
            }
            failures.push(error instanceof Error ? error : new Error(String(error)));
          }
        }
        for (const [contextId, owner] of attachment.contexts) {
          if (owner.pluginId !== plugin.id) continue;
          try {
            const result = await withCdpTimeout(attachment.client.Runtime.evaluate({
              expression: `(() => { if (window.__ewSelfInfo?.id !== ${JSON.stringify(plugin.id)}) return null; window.__ewRevoke?.(); return window.__ewHostDispose?.(); })()`,
              contextId,
              returnByValue: true,
            }), `Runtime.evaluate during disable (${attachment.target.id})`);
            if (result.exceptionDetails) throw new Error(result.exceptionDetails.text || result.exceptionDetails.exception?.description || "plugin disposer failed");
            const disposal = result.result?.value;
            if (disposal === null) continue;
            const problems = disposalProblems(disposal);
            if (problems.length > 0) {
              plugin.cleanupBlockedContexts.add(contextKey(attachment.target.id, contextId));
              failures.push(new Error(`plugin ${plugin.id} cleanup incomplete: ${problems.join("; ")}`));
            }
          } catch (error) {
            plugin.cleanupBlockedContexts.add(contextKey(attachment.target.id, contextId));
            failures.push(error instanceof Error ? error : new Error(String(error)));
          }
        }
      }
      await this.emitManagement("management.onDisabled", extensionInfo(plugin));
    } else {
      for (const attachment of this.#attachments.values()) {
        if (!attachment.allowed || !isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)) continue;
        try { await this.installPlugin(attachment, plugin); }
        catch (error) {
          const message = this.recordPluginError(plugin, error);
          failures.push(new Error(message));
        }
      }
      await this.emitManagement("management.onEnabled", extensionInfo(plugin));
    }

    if (failures.length > 0) throw new AggregateError(failures, `plugin ${plugin.id} ${enabled ? "enable" : "disable"} did not complete cleanly`);
  }

  stop(): Promise<void> {
    if (!this.#stopPromise) this.#stopPromise = this.stopInternal();
    return this.#stopPromise;
  }

  private async discover(): Promise<void> {
    if (this.#stopped) return;
    if (this.#discovering) {
      await this.#discovering;
      return;
    }
    const current = this.discoverInternal();
    this.#discovering = current;
    try { await current; }
    finally { if (this.#discovering === current) this.#discovering = undefined; }
  }

  private async discoverInternal(): Promise<void> {
    await assertOwnedCdp(this.#identity);
    const response = await fetch(`http://127.0.0.1:${this.#identity.port}/json/list`, { signal: AbortSignal.timeout(3_000) });
    if (!response.ok) throw new Error(`CDP target discovery returned HTTP ${response.status}`);
    const rawTargets: unknown = await response.json();
    if (!Array.isArray(rawTargets)) throw new Error("CDP target discovery returned an invalid list");
    await assertOwnedCdp(this.#identity);

    const eligible = new Map<string, TargetInfo>();
    for (const value of rawTargets) {
      const target = targetFrom(value, this.#identity.port, this.#identity.appPath);
      if (target) eligible.set(target.id, target);
    }
    for (const target of eligible.values()) {
      if (this.#stopped) return;
      const existing = this.#attachments.get(target.id);
      if (existing) {
        if (existing.target.webSocketDebuggerUrl !== target.webSocketDebuggerUrl) {
          await this.detachTarget(target.id);
          await this.attachTarget(target);
        }
        continue;
      }
      try { await this.attachTarget(target); }
      catch (error) { notice(`could not attach target ${target.id}: ${errorMessage(error)}`); }
    }
    for (const targetId of [...this.#attachments.keys()]) {
      if (!eligible.has(targetId)) await this.detachTarget(targetId);
    }
  }

  private async attachTarget(target: TargetInfo): Promise<void> {
    if (this.#stopped || this.#attachments.has(target.id)) return;
    await assertOwnedCdp(this.#identity);
    if (this.#stopped) return;
    const client = await connectTarget(target.webSocketDebuggerUrl);
    if (this.#stopped) {
      await withCdpTimeout(Promise.resolve(client.close()), `closing target ${target.id}`);
      return;
    }
    const attachment: TargetAttachment = {
      target,
      client,
      rootFrameId: "",
      rootUrl: target.url,
      allowed: false,
      closed: false,
      contexts: new Map(),
      scriptIds: new Map(),
      mainWorldContextId: undefined,
    };
    this.#attachments.set(target.id, attachment);

    client.Page.frameNavigated(({ frame }) => this.onFrameNavigated(attachment, frame));
    client.Runtime.executionContextCreated(({ context }) => this.onExecutionContextCreated(attachment, context));
    client.Runtime.executionContextDestroyed(({ executionContextId }) => this.onExecutionContextDestroyed(attachment, executionContextId));
    client.Runtime.executionContextsCleared(() => this.clearContexts(attachment));
    client.Runtime.bindingCalled((event) => {
      if (event.name !== bindingName) return;
      void this.handleBinding(attachment, event).catch((error: unknown) => notice(`bridge request failed: ${errorMessage(error)}`));
    });
    client.Runtime.exceptionThrown(({ exceptionDetails }) => this.onException(attachment, exceptionDetails));
    client.on("error", (error) => notice(`CDP session ${target.id}: ${errorMessage(error)}`));
    client.on("disconnect", () => {
      if (attachment.closed || this.#stopped) return;
      void this.detachTarget(target.id, false).then(() => this.discover()).catch((error: unknown) => {
        notice(`CDP reconnect failed: ${errorMessage(error)}`);
      });
    });

    try {
      await assertOwnedCdp(this.#identity);
      await withCdpTimeout(client.Runtime.addBinding({ name: bindingName }), `Runtime.addBinding (${target.id})`);
      await withCdpTimeout(client.Page.enable(), `Page.enable (${target.id})`);
      const { frameTree } = await withCdpTimeout(client.Page.getFrameTree(), `Page.getFrameTree (${target.id})`);
      if (!frameTree?.frame?.id || !isAllowedOctoBuddyUrl(frameTree.frame.url, this.#identity.appPath)) {
        throw new Error("target's main frame is not an eligible OctoBuddy page");
      }
      attachment.rootFrameId = frameTree.frame.id;
      attachment.rootUrl = frameTree.frame.url;
      attachment.allowed = true;
      await withCdpTimeout(client.Runtime.enable(), `Runtime.enable (${target.id})`);
      await assertOwnedCdp(this.#identity);
      await this.installManagement(attachment);
      for (const plugin of this.#plugins.values()) {
        if (plugin.builtin || !plugin.enabled) continue;
        try {
          await this.queuePluginTask(plugin.id, () => this.installPlugin(attachment, plugin));
        } catch (error) { this.recordPluginError(plugin, error); }
      }
    } catch (error) {
      await this.detachTarget(target.id).catch((closeError: unknown) => {
        notice(`failed to close partial target ${target.id}: ${errorMessage(closeError)}`);
      });
      throw error;
    }
  }


  private onFrameNavigated(attachment: TargetAttachment, frame: FrameInfo): void {
    if (attachment.closed) return;
    if (frame.parentId) {
      return;
    }
    this.clearContexts(attachment);
    attachment.rootFrameId = frame.id;
    attachment.rootUrl = frame.url;
    attachment.allowed = isAllowedOctoBuddyUrl(frame.url, this.#identity.appPath);
  }

  private clearContexts(attachment: TargetAttachment): void {
    if (attachment.mainWorldContextId !== undefined) {
      this.#plugins.get("channel-status")?.cleanupBlockedContexts.delete(
        contextKey(attachment.target.id, attachment.mainWorldContextId),
      );
      attachment.mainWorldContextId = undefined;
    }
    for (const [contextId, owner] of attachment.contexts) {
      this.cancelStatusRequests("expired", undefined, attachment.target.id, contextId);
      if (owner.pluginId === managementPluginId) continue;
      this.#plugins.get(owner.pluginId)?.cleanupBlockedContexts.delete(contextKey(attachment.target.id, contextId));
    }
    attachment.contexts.clear();
  }

  private onExecutionContextCreated(attachment: TargetAttachment, context: ExecutionContextInfo): void {
    const frameId = context.auxData?.frameId;
    if (!frameId) return;
    if (context.auxData?.isDefault === true) {
      if (frameId === attachment.rootFrameId) attachment.mainWorldContextId = context.id;
      return;
    }
    const pluginId = context.name ? this.#worldOwners.get(context.name) : undefined;
    if (context.auxData?.isDefault !== false || !pluginId) return;
    if (!attachment.allowed || frameId !== attachment.rootFrameId || !isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)) return;
    const plugin = this.#plugins.get(pluginId);
    if (!plugin || (!plugin.builtin && !plugin.enabled)) return;
    attachment.contexts.set(context.id, { pluginId, frameId });
  }

  private onExecutionContextDestroyed(attachment: TargetAttachment, contextId: number): void {
    const owner = attachment.contexts.get(contextId);
    this.cancelStatusRequests("expired", undefined, attachment.target.id, contextId);
    attachment.contexts.delete(contextId);
    if (attachment.mainWorldContextId === contextId) {
      this.#plugins.get("channel-status")?.cleanupBlockedContexts.delete(contextKey(attachment.target.id, contextId));
      attachment.mainWorldContextId = undefined;
    }
    if (!owner || owner.pluginId === managementPluginId) return;
    this.#plugins.get(owner.pluginId)?.cleanupBlockedContexts.delete(contextKey(attachment.target.id, contextId));
  }


  private onException(
    attachment: TargetAttachment,
    exceptionDetails: { text?: string; exception?: { description?: string }; executionContextId?: number } | undefined,
  ): void {
    const contextId = exceptionDetails?.executionContextId;
    if (contextId === undefined) return;
    const owner = attachment.contexts.get(contextId);
    if (!owner) return;
    const plugin = this.#plugins.get(owner.pluginId);
    if (!plugin) return;
    this.recordPluginError(plugin, new Error(exceptionDetails?.exception?.description || exceptionDetails?.text || "plugin script failed"));
  }

  private async installManagement(attachment: TargetAttachment): Promise<void> {
    const plugin = this.#plugins.get(managementPluginId)!;
    const source = worldBootstrapSource(plugin.id, true, this.#shim, this.#identity.appPath);
    await this.addScript(attachment, plugin.id, source, managementWorld);
    for (const asset of this.#managementScripts) {
      await this.addScript(attachment, plugin.id, managementScriptSource(asset.source, this.#identity.appPath), managementWorld);
    }
  }

  private async installPlugin(attachment: TargetAttachment, plugin: HostPlugin): Promise<void> {
    if (!plugin.enabled || !attachment.allowed || !isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)
      || attachment.scriptIds.has(plugin.id)) return;
    const world = worldName(plugin.id);
    const shimSource = worldBootstrapSource(plugin.id, false, this.#shim, this.#identity.appPath);
    await this.addScript(attachment, plugin.id, shimSource, world);
    for (const unit of plugin.units) {
      await this.addScript(attachment, plugin.id, buildContentScriptSource(plugin, unit, this.#identity.appPath), world);
    }
    if (this.isBuiltinStatusPlugin(plugin) && this.#statusNavigationScript !== undefined) {
      await this.addScript(
        attachment,
        plugin.id,
        statusNavigationSource(this.#statusNavigationScript, this.#identity.appPath),
      );
    }
  }

  private async addScript(attachment: TargetAttachment, pluginId: string, source: string, world?: string): Promise<void> {
    if (this.#stopped || attachment.closed || !attachment.allowed || !isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)) return;
    await assertOwnedCdp(this.#identity);
    const { identifier } = await withCdpTimeout(attachment.client.Page.addScriptToEvaluateOnNewDocument({
      source,
      ...(world === undefined ? {} : { worldName: world }),
      runImmediately: true,
    }), `Page.addScriptToEvaluateOnNewDocument (${attachment.target.id})`);
    const identifiers = attachment.scriptIds.get(pluginId) ?? [];
    identifiers.push(identifier);
    attachment.scriptIds.set(pluginId, identifiers);
  }
  private async disposeStatusNavigation(
    attachment: TargetAttachment,
    phase: "plugin disable" | "host stop",
  ): Promise<void> {
    const plugin = this.#plugins.get("channel-status");
    const contextId = attachment.mainWorldContextId;
    if (!plugin || !this.isBuiltinStatusPlugin(plugin) || contextId === undefined || attachment.closed
      || !isCommunicationRendererUrl(attachment.rootUrl, this.#identity.appPath)) return;
    const response = await withCdpTimeout(attachment.client.Runtime.evaluate({
      expression: `(() => {
        const observer = window.__octobuddyStatusNavigation;
        if (!observer) return false;
        if (typeof observer.dispose !== "function") throw new Error("status navigation disposer is unavailable");
        observer.dispose();
        return true;
      })()`,
      contextId,
      returnByValue: true,
    }), `Runtime.evaluate during ${phase} (${attachment.target.id})`);
    if (response.exceptionDetails) {
      throw new Error(response.exceptionDetails.text || response.exceptionDetails.exception?.description || "status navigation disposal failed");
    }
  }

  private async removePluginScripts(attachment: TargetAttachment, pluginId: string): Promise<void> {
    const identifiers = attachment.scriptIds.get(pluginId) ?? [];
    const errors: Error[] = [];
    const remaining: string[] = [];
    for (const identifier of identifiers) {
      try { await withCdpTimeout(attachment.client.Page.removeScriptToEvaluateOnNewDocument({ identifier }), `Page.removeScriptToEvaluateOnNewDocument (${attachment.target.id})`); }
      catch (error) {
        errors.push(error instanceof Error ? error : new Error(String(error)));
        remaining.push(identifier);
      }
    }
    if (remaining.length > 0) attachment.scriptIds.set(pluginId, remaining);
    else attachment.scriptIds.delete(pluginId);
    if (errors.length > 0) throw new AggregateError(errors, `could not remove scripts for ${pluginId}`);
  }

  private async detachTarget(targetId: string, removeScripts = true): Promise<void> {
    const attachment = this.#attachments.get(targetId);
    if (!attachment) return;
    attachment.closed = true;
    this.#attachments.delete(targetId);
    this.clearContexts(attachment);
    const errors: Error[] = [];
    if (removeScripts) {
      for (const pluginId of attachment.scriptIds.keys()) {
        try { await this.removePluginScripts(attachment, pluginId); }
        catch (error) { errors.push(error instanceof Error ? error : new Error(String(error))); }
      }
    }
    try { await withCdpTimeout(Promise.resolve(attachment.client.close()), `closing target ${targetId}`); }
    catch (error) { errors.push(error instanceof Error ? error : new Error(String(error))); }
    if (errors.length > 0) throw new AggregateError(errors, `could not fully detach target ${targetId}`);
  }

  private async handleBinding(attachment: TargetAttachment, event: BindingCall): Promise<void> {
    if (this.#stopped || attachment.closed || event.name !== bindingName) return;
    const context = attachment.contexts.get(event.executionContextId);
    if (!context || context.frameId !== attachment.rootFrameId || !attachment.allowed || !isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)) return;
    const request = parseBridgeRequest(event.payload);
    if (!request) return;
    const plugin = this.#plugins.get(context.pluginId);
    if (!plugin || (!plugin.builtin && !plugin.enabled)) return;

    try {
      await assertOwnedCdp(this.#identity);
      const result = context.pluginId === managementPluginId
        ? await this.handleManagement(plugin, request)
        : await this.handlePlugin(plugin, attachment, event.executionContextId, request);
      if (request.api === "status" && !this.isCurrentPluginContext(plugin, attachment, event.executionContextId)) return;
      await this.resolveBridge(
        attachment,
        event.executionContextId,
        request.callId,
        result,
        false,
        request.api === "status" ? plugin : undefined,
      );
    } catch (error) {
      const message = errorMessage(error);
      const canceled = request.api === "status" && error instanceof Error && error.name === "AbortError";
      if (request.api === "status" && !this.isCurrentPluginContext(plugin, attachment, event.executionContextId)) return;
      if (context.pluginId !== managementPluginId && message !== "plugin is disabled" && !canceled) {
        this.recordPluginError(plugin, new Error(message));
      }
      await this.resolveBridge(
        attachment,
        event.executionContextId,
        request.callId,
        message,
        true,
        request.api === "status" ? plugin : undefined,
      ).catch((resolveError: unknown) => {
        notice(`could not return bridge error to ${plugin.id}: ${errorMessage(resolveError)}`);
      });
    }
  }

  private async handleManagement(plugin: HostPlugin, request: BridgeRequest): Promise<unknown> {
    if (request.api !== "management") throw new Error(`unsupported management API: ${request.api}.${request.method}`);
    if (request.method === "getAll" && request.args.length === 0) return [...this.#plugins.values()].map(extensionInfo);
    if (request.method === "get" && request.args.length === 1 && typeof request.args[0] === "string") {
      const entry = this.#plugins.get(request.args[0]);
      if (!entry) throw new Error(`plugin not found: ${request.args[0]}`);
      return extensionInfo(entry);
    }
    if (request.method === "getSelf" && request.args.length === 0) return extensionInfo(plugin);
    if (request.method === "setEnabled" && request.args.length === 2) {
      if (!request.trustedClick) throw new Error("management changes require a trusted user click");
      const [id, enabled] = request.args;
      if (typeof id !== "string" || typeof enabled !== "boolean") throw new Error("invalid plugin state change");
      await this.setPluginEnabled(id, enabled);
      return undefined;
    }
    throw new Error(`unsupported management API: ${request.method}`);
  }

  private async handlePlugin(
    plugin: HostPlugin,
    attachment: TargetAttachment,
    contextId: number,
    request: BridgeRequest,
  ): Promise<unknown> {
    if (request.api === "status" && request.method === "openChatWorkspace" && request.args.length === 1) {
      if (!request.trustedClick || !this.isAuthorizedStatusContext(plugin, attachment, contextId)) {
        throw new Error("opening the workspace requires a trusted status-panel click");
      }
      const target = request.args[0];
      if (!isRecord(target) || (target.channelType !== 1 && target.channelType !== 2 && target.channelType !== 5)
        || typeof target.channelId !== "string" || !target.channelId) throw new Error("invalid chat target");
      const generation = plugin.generation;
      const currentChat = async () => {
        const result = await withCdpTimeout(attachment.client.Runtime.evaluate({
          expression: `(async () => {
            const link = [...document.querySelectorAll('link[rel="modulepreload"]')]
              .find(element => /\\/App-[^/]+\\.js$/.test(element.href));
            const module = await import(link.href);
            const chat = module.i.shared.openChannel;
            return { channelType: chat?.channelType, channelId: chat?.channelID };
          })()`,
          awaitPromise: true,
          returnByValue: true,
        }), "reading current chat");
        return result.result?.value;
      };
      const matches = async () => {
        const chat = await currentChat();
        if (!isRecord(chat) || chat.channelType !== target.channelType) return false;
        if (target.channelType !== 1) return chat.channelId === target.channelId;
        const service = await this.#statusClient.get();
        return service.state === "online" && chat.channelId === service.snapshot.botUid
          && target.channelId === service.snapshot.ownerUid;
      };
      if (!await matches()) throw new Error("chat changed before Zed could be opened");
      const status = await this.#statusClient.getChat(target as unknown as ChatTarget);
      if (status.state !== "online" || status.snapshot.cwd === null || !status.snapshot.zedWorkspaceUrl) throw new Error("chat workspace is unavailable");
      await openInZed({ path: status.snapshot.zedWorkspaceUrl }, {
        ...(this.#zedExecutable !== undefined ? { zedExecutable: this.#zedExecutable } : {}),
        workspace: true,
      }, async () => {
        if (!this.isAuthorizedStatusContext(plugin, attachment, contextId) || plugin.generation !== generation || !await matches()) return false;
        const latest = await this.#statusClient.getChat(target as unknown as ChatTarget);
        return latest.state === "online" && latest.snapshot.cwd === status.snapshot.cwd
          && latest.snapshot.zedWorkspaceUrl === status.snapshot.zedWorkspaceUrl
          && this.isAuthorizedStatusContext(plugin, attachment, contextId) && plugin.generation === generation;
      });
      return undefined;
    }
    if (request.api === "status" && request.method === "getChannelStatus" && request.args.length === 0) {
      return this.withStatusRequest(
        plugin,
        attachment,
        contextId,
        (signal) => this.#statusClient.get(signal),
        "service",
      );
    }
    if (request.api === "status" && request.method === "getChatStatus") {
      if (request.args.length !== 2 || !isStatusRequestId(request.args[1])) {
        throw new Error("invalid chat status request");
      }
      return this.withStatusRequest(
        plugin,
        attachment,
        contextId,
        (signal) => this.#statusClient.getChat(request.args[0] as ChatTarget, signal),
        "chat-status",
        request.args[1],
      );
    }
    if (request.api === "status" && request.method === "getChatDetails") {
      if (request.args.length !== 2 || !isStatusRequestId(request.args[1])) {
        throw new Error("invalid chat details request");
      }
      return this.withStatusRequest(
        plugin,
        attachment,
        contextId,
        (signal) => this.#statusClient.getChatDetails(request.args[0] as ChatTarget, signal),
        "chat-details",
        request.args[1],
      );
    }
    if (request.api === "status" && request.method === "cancelChatStatus") {
      if (request.args.length !== 1 || !isStatusRequestId(request.args[0])) {
        throw new Error("invalid chat status cancellation request");
      }
      this.cancelChatStatus(plugin, attachment, contextId, request.args[0]);
      return undefined;
    }
    if (request.api === "status" && request.method === "cancelChatDetails") {
      if (request.args.length !== 1 || !isStatusRequestId(request.args[0])) {
        throw new Error("invalid chat details cancellation request");
      }
      this.cancelChatDetails(plugin, attachment, contextId, request.args[0]);
      return undefined;
    }
    if (request.api !== "zed" || request.method !== "openZed" || request.args.length !== 1) {
      throw new Error(`unsupported plugin capability: ${request.api}.${request.method}`);
    }
    if (plugin.id !== "zed-open" || (plugin.directory !== this.#builtinZedDirectory && this.#zedExecutable === undefined)) {
      throw new Error("plugin is not authorized to open files in Zed");
    }
    if (!request.trustedClick) throw new Error("opening Zed requires a trusted plugin click");
    const generation = plugin.generation;
    const isCurrent = () => {
      const owner = attachment.contexts.get(contextId);
      return !this.#stopped && !attachment.closed && attachment.allowed
        && isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath) && owner?.pluginId === plugin.id
        && owner.frameId === attachment.rootFrameId && plugin.enabled && plugin.generation === generation;
    };
    let argument = request.args[0];
    if (isRecord(argument) && typeof argument.linkId === "string") {
      const expression = `(() => {
        const anchor = [...document.querySelectorAll("a[data-octobuddy-zed-click]")].find(a => a.getAttribute("data-octobuddy-zed-click") === ${JSON.stringify(argument.linkId)});
        if (!anchor || anchor.getAttribute("href") !== "") return null;
        const propsKey = Object.keys(anchor).find(key => key.startsWith("__reactProps$"));
        const fiberKey = Object.keys(anchor).find(key => key.startsWith("__reactFiber$"));
        const position = anchor[propsKey]?.node?.position;
        if (!Number.isSafeInteger(position?.start?.offset) || !Number.isSafeInteger(position?.end?.offset)) return null;
        for (let fiber = anchor[fiberKey]; fiber; fiber = fiber.return) {
          const text = fiber.memoizedProps?.children;
          if (typeof text !== "string") continue;
          const link = text.slice(position.start.offset, position.end.offset);
          const separator = link.lastIndexOf("](");
          if (!link.startsWith("[") || separator < 0 || !link.endsWith(")")) continue;
          const url = link.slice(separator + 2, -1);
          if (/^zed:\\/\\/[^/\\s]+\\S*$/i.test(url)) return url;
        }
        return null;
      })()`;
      const result = await withCdpTimeout(attachment.client.Runtime.evaluate({ expression, returnByValue: true }), "resolving Zed message link");
      if (result.exceptionDetails || typeof result.result?.value !== "string") throw new Error("Zed message link could not be resolved");
      argument = { path: result.result.value };
    }
    let cwd: string | undefined;
    let chatStillCurrent: (() => Promise<boolean>) | undefined;
    if (isRecord(argument) && typeof argument.path === "string" && /^zed:\/\/ssh\//i.test(argument.path)) {
      if (!isCommunicationRendererUrl(attachment.rootUrl, this.#identity.appPath)) {
        throw new Error("chat workspace is unavailable");
      }
      const currentChat = async (): Promise<ChatTarget> => {
        // The page's hashed module is chosen by the installed OctoBuddy build.
        const result = await withCdpTimeout(attachment.client.Runtime.evaluate({
          expression: `(async () => {
            const link = [...document.querySelectorAll('link[rel="modulepreload"]')]
              .find(element => /\\/App-[^/]+\\.js$/.test(element.href));
            const module = await import(link.href);
            const chat = module.i.shared.openChannel;
            return { channelType: chat?.channelType, channelId: chat?.channelID };
          })()`,
          awaitPromise: true,
          returnByValue: true,
        }), "reading current chat");
        const target = result.result?.value;
        if (!isRecord(target) || (target.channelType !== 2 && target.channelType !== 5)
          || typeof target.channelId !== "string" || !target.channelId) {
          throw new Error("current group or thread is unavailable");
        }
        return { channelType: target.channelType, channelId: target.channelId };
      };
      const target = await currentChat();
      const status = await this.#statusClient.getChat(target);
      if (status.state !== "online" || status.snapshot.cwd === null) {
        throw new Error("chat workspace is unavailable");
      }
      chatStillCurrent = async () => {
        const chat = await currentChat();
        return chat.channelType === target.channelType && chat.channelId === target.channelId;
      };
      if (!isCurrent() || !await chatStillCurrent()) throw new Error("chat changed before Zed could be opened");
      cwd = status.snapshot.cwd;
    }
    await openInZed(argument, {
      ...(this.#zedExecutable !== undefined ? { zedExecutable: this.#zedExecutable } : {}),
      ...(cwd !== undefined ? { cwd } : {}),
    }, async () => isCurrent() && (chatStillCurrent === undefined || await chatStillCurrent()) && isCurrent());
    return undefined;
  }

  private isCurrentPluginContext(plugin: HostPlugin, attachment: TargetAttachment, contextId: number): boolean {
    const owner = attachment.contexts.get(contextId);
    return this.#plugins.get(plugin.id) === plugin
      && owner?.pluginId === plugin.id
      && owner.frameId === attachment.rootFrameId
      && !this.#stopped
      && !attachment.closed
      && attachment.allowed
      && isAllowedOctoBuddyUrl(attachment.rootUrl, this.#identity.appPath)
      && plugin.enabled;
  }

  private isBuiltinStatusPlugin(plugin: HostPlugin): boolean {
    return this.#builtinStatusDirectory !== undefined
      && plugin.id === "channel-status"
      && plugin.directory === this.#builtinStatusDirectory
      && this.#plugins.get("channel-status") === plugin;
  }
  private isAuthorizedStatusContext(plugin: HostPlugin, attachment: TargetAttachment, contextId: number): boolean {
    return this.isCurrentPluginContext(plugin, attachment, contextId)
      && this.isBuiltinStatusPlugin(plugin)
      && isCommunicationRendererUrl(attachment.rootUrl, this.#identity.appPath);
  }


  private async withStatusRequest<T>(
    plugin: HostPlugin,
    attachment: TargetAttachment,
    contextId: number,
    get: (signal: AbortSignal) => Promise<T>,
    requestKind: StatusRequestKind,
    requestId?: number,
  ): Promise<T> {
    if (!this.isAuthorizedStatusContext(plugin, attachment, contextId)) {
      throw new Error("plugin is not authorized to read channel status");
    }
    const generation = plugin.generation;
    const request: StatusRequest = {
      pluginId: plugin.id,
      targetId: attachment.target.id,
      contextId,
      requestKind,
      ...(requestId === undefined ? {} : { requestId }),
      controller: new AbortController(),
    };
    this.#statusRequests.add(request);
    try {
      const result = await get(request.controller.signal);
      if (!this.isAuthorizedStatusContext(plugin, attachment, contextId) || plugin.generation !== generation) {
        throw request.controller.signal.reason ?? new DOMException("plugin context is no longer active", "AbortError");
      }
      return result;
    } finally {
      this.#statusRequests.delete(request);
    }
  }
  private cancelChatStatus(
    plugin: HostPlugin,
    attachment: TargetAttachment,
    contextId: number,
    requestId: number,
  ): void {
    if (!this.isAuthorizedStatusContext(plugin, attachment, contextId)) {
      throw new Error("plugin is not authorized to cancel chat status");
    }
    for (const request of this.#statusRequests) {
      if (request.pluginId !== plugin.id || request.targetId !== attachment.target.id
        || request.contextId !== contextId || request.requestKind !== "chat-status" || request.requestId !== requestId
        || request.controller.signal.aborted) continue;
      request.controller.abort();
    }
  }

  private cancelChatDetails(
    plugin: HostPlugin,
    attachment: TargetAttachment,
    contextId: number,
    requestId: number,
  ): void {
    if (!this.isAuthorizedStatusContext(plugin, attachment, contextId)) {
      throw new Error("plugin is not authorized to cancel chat details");
    }
    for (const request of this.#statusRequests) {
      if (request.pluginId !== plugin.id || request.targetId !== attachment.target.id
        || request.contextId !== contextId || request.requestKind !== "chat-details" || request.requestId !== requestId
        || request.controller.signal.aborted) continue;
      request.controller.abort();
    }
  }

  private async resolveBridge(
    attachment: TargetAttachment,
    contextId: number,
    callId: number,
    result: unknown,
    isError: boolean,
    statusPlugin?: HostPlugin,
  ): Promise<void> {
    if (statusPlugin && !this.isCurrentPluginContext(statusPlugin, attachment, contextId)) return;
    if (attachment.closed || !attachment.contexts.has(contextId)) return;
    const expression = `window.__ewResolve?.(${callId}, ${sourceExpression(result)}, ${isError});`;
    const response = await withCdpTimeout(attachment.client.Runtime.evaluate({ expression, contextId, returnByValue: true }), `Runtime.evaluate (${attachment.target.id})`);
    if (response.exceptionDetails) {
      throw new Error(response.exceptionDetails.text || response.exceptionDetails.exception?.description || "isolated bridge resolution failed");
    }
  }

  private async emitManagement(name: string, payload: Record<string, unknown>): Promise<void> {
    const expression = `window.__ewEmit?.(${JSON.stringify(name)}, ${JSON.stringify(payload)});`;
    for (const attachment of this.#attachments.values()) {
      if (attachment.closed || !attachment.allowed) continue;
      for (const [contextId, owner] of attachment.contexts) {
        if (owner.pluginId !== managementPluginId) continue;
        try {
          const response = await withCdpTimeout(attachment.client.Runtime.evaluate({ expression, contextId, returnByValue: true }), `Runtime.evaluate management event (${attachment.target.id})`);
          if (response.exceptionDetails) throw new Error(response.exceptionDetails.text || "management event failed");
        } catch (error) {
          notice(`management event ${name} failed: ${errorMessage(error)}`);
        }
      }
    }
  }

  private recordPluginError(plugin: HostPlugin, error: unknown): string {
    const message = errorMessage(error).slice(0, 500);
    if (!plugin.errors.includes(message)) plugin.errors.push(message);
    notice(`plugin ${plugin.id} failed: ${message}`);
    void this.emitManagement("management.onErrors", { id: plugin.id, messages: [...plugin.errors] });
    return message;
  }

  private async stopInternal(): Promise<void> {
    this.#stopped = true;
    this.cancelStatusRequests("stopped");
    if (this.#pollTimer) clearInterval(this.#pollTimer);
    this.#pollTimer = undefined;
    const errors: Error[] = [];
    if (this.#discovering) {
      try { await this.#discovering; }
      catch (error) { errors.push(error instanceof Error ? error : new Error(String(error))); }
    }
    await Promise.all([...this.#pluginTransitions.values()].map((operation) => operation.catch(() => undefined)));
    for (const attachment of [...this.#attachments.values()]) {
      try { await this.disposeStatusNavigation(attachment, "host stop"); }
      catch (error) {
        const failure = error instanceof Error ? error : new Error(String(error));
        notice(`status navigation cleanup failed during host stop: ${failure.message}`);
        errors.push(failure);
      }
      for (const [contextId, owner] of attachment.contexts) {
        try {
          const identity = await withCdpTimeout(attachment.client.Runtime.evaluate({
            expression: "({ pluginId: window.__ewSelfInfo?.id, hasDispose: typeof window.__ewHostDispose === \"function\" })",
            contextId,
            returnByValue: true,
          }), `Runtime.evaluate during host stop (${attachment.target.id})`);
          const identityValue = identity.result?.value;
          if (identity.exceptionDetails || !isRecord(identityValue) || identityValue.pluginId !== owner.pluginId) continue;
          if (identityValue.hasDispose !== true) throw new Error("isolated-world cleanup function is unavailable");
          const response = await withCdpTimeout(attachment.client.Runtime.evaluate({
            expression: "(() => { window.__ewRevoke?.(); return window.__ewHostDispose?.(); })()",
            contextId,
            returnByValue: true,
          }), `Runtime.evaluate during host stop (${attachment.target.id})`);
          if (response.exceptionDetails) {
            const error = new Error(response.exceptionDetails.text || "unknown error");
            notice(`plugin ${owner.pluginId} cleanup failed during host stop: ${error.message}`);
            errors.push(error);
          } else {
            const problems = disposalProblems(response.result?.value);
            if (problems.length > 0) {
              const error = new Error(`plugin ${owner.pluginId} cleanup incomplete: ${problems.join("; ")}`);
              notice(error.message);
              errors.push(error);
            }
          }
        } catch (error) {
          const failure = error instanceof Error ? error : new Error(String(error));
          notice(`plugin ${owner.pluginId} cleanup could not run during host stop: ${failure.message}`);
          errors.push(failure);
        }
      }
      try { await this.detachTarget(attachment.target.id); }
      catch (error) { errors.push(error instanceof Error ? error : new Error(String(error))); }
    }
    if (errors.length > 0) throw new AggregateError(errors, "host stopped with CDP cleanup errors");
  }
}

export async function startHost(options: HostStartOptions): Promise<HostSession> {
  if (!Number.isSafeInteger(options.port) || options.port < 1 || options.port > 65_535) throw new Error("invalid CDP port");
  if (!Number.isSafeInteger(options.pid) || options.pid < 1) throw new Error("invalid OctoBuddy PID");
  if (!options.startedAt || options.startedAt.trim() !== options.startedAt) throw new Error("invalid OctoBuddy process identity");
  if (!isAbsolute(options.appPath)) throw new Error("appPath must be an absolute OctoBuddy bundle path");
  if (!Array.isArray(options.pluginDirectories) || options.pluginDirectories.some((directory) => typeof directory !== "string")) {
    throw new Error("pluginDirectories must be an array of trusted directory paths");
  }

  const identity = { ...options, appPath: await realpath(options.appPath) };
  await assertOwnedCdp(identity);
  const [loadedPlugins, assets, builtinZedDirectory, builtinStatusDirectory] = await Promise.all([
    loadPlugins(options.pluginDirectories),
    loadManagementAssets(),
    realpath(join(packageDirectory, "plugins", "zed")).catch(() => undefined),
    realpath(join(packageDirectory, "plugins", "channel-status")).catch(() => undefined),
  ]);
  const statusNavigationScript = builtinStatusDirectory === undefined
    ? undefined
    : await readFile(join(builtinStatusDirectory, "navigation.js"), "utf8");
  const statusClient = createStatusClient({
    configPath: options.statusConfigPath ?? join(homedir(), ".config", "octobuddy-enhancer", "status.json"),
  });
  const host = new OctoBuddyHost(
    identity,
    loadedPlugins,
    assets,
    options.zedExecutable,
    builtinZedDirectory,
    builtinStatusDirectory,
    statusNavigationScript,
    statusClient,
  );
  try {
    await host.start();
  } catch (error) {
    try { await host.stop(); }
    catch (stopError) {
      throw new AggregateError([error, stopError], "host startup failed and cleanup was incomplete");
    }
    throw error;
  }
  return {
    stop: () => host.stop(),
    setPluginEnabled: (pluginId, enabled) => host.setPluginEnabled(pluginId, enabled),
  };
}
