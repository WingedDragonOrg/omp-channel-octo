import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { FakeZedFixture, OwnedCdpFixture } from "./fixtures/host.ts";
import { createFakeZed, createGatedHostProcessFixture, createOwnedCdpFixture, writePlugin } from "./fixtures/host.ts";
import { openInZed } from "../src/host/zed.ts";
type HostTestSetup = Pick<HostStartOptions, "pluginDirectories" | "zedExecutable"> & {
  zed?: FakeZedFixture;
  filePath?: string;
};
type HostStartOptions = {
  appPath: string;
  port: number;
  pid: number;
  startedAt: string;
  pluginDirectories: string[];
  zedExecutable?: string;
};
type HostSession = {
  stop(): Promise<void>;
  setPluginEnabled(pluginId: string, enabled: boolean): Promise<void>;
};
type HostEntry = { startHost(options: HostStartOptions): Promise<HostSession> };
async function createFakeAppBundle(parent: string): Promise<string> {
  const appPath = join(parent, "OctoBuddy.app");
  const pages = [
    "Contents/Resources/communication-renderer/index.html",
    "Contents/Resources/app.asar/out/renderer/index.html",
    "Contents/Resources/settings/index.html",
  ];
  for (const page of pages) {
    const path = join(appPath, page);
    await mkdir(join(path, ".."), { recursive: true });
    await writeFile(path, "<!doctype html><html></html>");
  }
  return await realpath(appPath);
}

const communicationUrl = (appPath: string) =>
  pathToFileURL(join(appPath, "Contents/Resources/communication-renderer/index.html")).href;
const mainConversationUrl = (appPath: string) =>
  `${pathToFileURL(join(appPath, "Contents/Resources/app.asar/out/renderer/index.html")).href}#/conversation`;
const unrelatedFileUrl = (appPath: string) =>
  pathToFileURL(join(appPath, "Contents/Resources/settings/index.html")).href;

async function loadHostEntry(): Promise<HostEntry> {
  // Keep the missing production entry a test failure, not a test-file load failure.
  const entry: unknown = await import("../src/host/index.ts");
  if (!entry || typeof entry !== "object" || !("startHost" in entry) || typeof entry.startHost !== "function") {
    throw new Error("Host behavior entry must export startHost(options)");
  }
  return { startHost: entry.startHost as HostEntry["startHost"] };
}

async function withHost(
  setup: (root: string, fixture: OwnedCdpFixture, appPath: string) => Promise<HostTestSetup>,
  exercise: (host: HostSession, fixture: OwnedCdpFixture, root: string, setup: HostTestSetup, appPath: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-host-"));
  const appPath = await createFakeAppBundle(root);
  const fixture = await createOwnedCdpFixture();
  let host: HostSession | undefined;
  try {
    const options = await setup(root, fixture, appPath);
    const { startHost } = await loadHostEntry();
    host = await startHost({
      appPath,
      port: fixture.port,
      pid: fixture.pid,
      startedAt: fixture.startedAt,
      pluginDirectories: options.pluginDirectories,
      ...(options.zedExecutable ? { zedExecutable: options.zedExecutable } : {}),
    });
    await exercise(host, fixture, root, options, appPath);
  } finally {
    try { await host?.stop(); }
    finally {
      try { await fixture.close(); }
      finally { await rm(root, { recursive: true, force: true }); }
    }
  }
}

// The host and fake app are separate processes; poll their observable CDP boundary.
async function waitUntil(predicate: () => boolean | Promise<boolean>, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    const { promise, resolve } = Promise.withResolvers<void>();
    setTimeout(resolve, 10);
    await promise;
  }
  if (await predicate()) return;
  throw new Error(`Timed out waiting for host behavior after ${timeoutMs}ms`);
}


async function zedArguments(argvFile: string): Promise<string[][]> {
  if (!existsSync(argvFile)) return [];
  const text = await readFile(argvFile, "utf8");
  return text.split("\n").filter(Boolean).map((line) => JSON.parse(line) as string[]);
}

test("passes a Zed SSH URL as one argument without local file resolution", async () => {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-zed-url-"));
  try {
    const zed = await createFakeZed(root);
    const url = "zed://ssh/my-mini/Users/dong/workspace/project/docs/design.md";
    await openInZed({ path: url }, { zedExecutable: zed.executable }, () => true);
    await waitUntil(async () => (await zedArguments(zed.argvFile)).length === 1);
    expect(await zedArguments(zed.argvFile)).toEqual([[url]]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 10_000);

test("matches every content_scripts entry and limits file pages to the two OctoBuddy entry points", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "match-probe",
      contentScripts: [
        { matches: ["file:///*"], js: ["file-page.js"] },
        { matches: ["https://must-not-match.invalid/*"], js: ["https-page.js"] },
      ],
      files: {
        "file-page.js": 'document.documentElement.dataset.filePage = "ran";',
        "https-page.js": 'document.documentElement.dataset.httpsPage = "ran";',
      },
    });
    const forgedAppPath = await createFakeAppBundle(join(root, "untrusted"));
    await fixture.addTarget({ id: "communication-view", url: communicationUrl(appPath), title: "Octo Communication" });
    await fixture.addTarget({ id: "main-conversation", url: mainConversationUrl(appPath), title: "OctoBuddy" });
    await fixture.addTarget({ id: "unrelated-file-view", url: unrelatedFileUrl(appPath), title: "OctoBuddy settings" });
    await fixture.addTarget({
      id: "forged-communication-view",
      url: communicationUrl(forgedAppPath),
      title: "Forged Octo Communication",
    });
    return { pluginDirectories: [pluginRoot] };
  }, async (_host, fixture) => {
    await waitUntil(async () => (await fixture.effects("communication-view", "communication-view-main")).dataset.filePage === "ran");
    await waitUntil(async () => (await fixture.effects("main-conversation", "main-conversation-main")).dataset.filePage === "ran");
    const communication = await fixture.effects("communication-view", "communication-view-main");
    const main = await fixture.effects("main-conversation", "main-conversation-main");
    const unrelated = await fixture.effects("unrelated-file-view", "unrelated-file-view-main");
    const forged = await fixture.effects("forged-communication-view", "forged-communication-view-main");

    expect(communication.dataset.filePage).toBe("ran");
    expect(main.dataset.filePage).toBe("ran");
    expect(main.elementIds).not.toContain("octo-enhancer-toolbar");
    expect(communication.elementIds).not.toContain("octo-enhancer-toolbar");
    expect(communication.dataset.httpsPage).toBeUndefined();
    expect(main.dataset.httpsPage).toBeUndefined();
    expect(unrelated.dataset.filePage).toBeUndefined();
    expect(forged.dataset.filePage).toBeUndefined();
  });
}, 10_000);

test("rechecks the current hash route before a delayed document_idle script executes", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "idle-route-probe",
      contentScripts: [{ matches: ["file:///*"], include_globs: ["*#/conversation"], js: ["idle.js"], run_at: "document_idle" }],
      files: { "idle.js": 'document.documentElement.dataset.idleRouteProbe = "ran";' },
    });
    const targetId = "conversation-idle";
    await fixture.addTarget({ id: targetId, url: mainConversationUrl(appPath), title: "OctoBuddy" });
    await fixture.setReadyState(targetId, `${targetId}-main`, "loading");
    return { pluginDirectories: [pluginRoot] };
  }, async (_host, fixture, _root, _setup, appPath) => {
    const targetId = "conversation-idle";
    const frameId = `${targetId}-main`;
    await waitUntil(async () => (await fixture.context(targetId, frameId)) !== undefined);
    await fixture.drain();
    expect((await fixture.effects(targetId, frameId)).dataset.idleRouteProbe).toBeUndefined();

    await fixture.navigateWithinDocument(targetId, frameId, mainConversationUrl(appPath).replace("#/conversation", "#/settings"));
    await fixture.drain();
    await fixture.setReadyState(targetId, frameId, "complete");
    await fixture.drain();
    expect((await fixture.effects(targetId, frameId)).dataset.idleRouteProbe).toBeUndefined();
  });
}, 10_000);

test("rechecks frame policy after navigation and never injects into child frames", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "frame-probe",
      contentScripts: [{ matches: ["file:///*"], js: ["frame.js"], all_frames: true }],
      files: { "frame.js": 'document.documentElement.dataset.frameProbe = "ran";' },
    });
    await fixture.addTarget({
      id: "communication-with-frames",
      url: communicationUrl(appPath),
      title: "Octo Communication",
      frames: [
        { id: "communication-main", url: communicationUrl(appPath) },
        { id: "same-url-child", url: communicationUrl(appPath), parentId: "communication-main" },
        { id: "other-child", url: unrelatedFileUrl(appPath), parentId: "communication-main" },
      ],
    });
    return { pluginDirectories: [pluginRoot] };
  }, async (_host, fixture, _root, _setup, appPath) => {
    const targetId = "communication-with-frames";
    await waitUntil(async () => (await fixture.effects(targetId, "communication-main")).dataset.frameProbe === "ran");
    await waitUntil(async () => (await fixture.contexts(targetId)).some((context) => context.frameId === "same-url-child"));
    expect((await fixture.effects(targetId, "same-url-child")).dataset.frameProbe).toBeUndefined();
    expect((await fixture.effects(targetId, "other-child")).dataset.frameProbe).toBeUndefined();

    const initialMain = await fixture.context(targetId, "communication-main");
    expect(initialMain).toBeDefined();
    await fixture.navigate(targetId, "communication-main", unrelatedFileUrl(appPath));
    await waitUntil(async () => {
      const navigated = await fixture.context(targetId, "communication-main");
      return Boolean(navigated && navigated.id !== initialMain!.id);
    });
    await fixture.drain();
    expect((await fixture.effects(targetId, "communication-main")).dataset.frameProbe).toBeUndefined();
    await fixture.navigate(targetId, "communication-main", communicationUrl(appPath));
    await waitUntil(async () => (await fixture.effects(targetId, "communication-main")).dataset.frameProbe === "ran");
  });
}, 10_000);

test("isolates management scripts and does not expose getAll to a page that forges _management", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "unprivileged-plugin",
      contentScripts: [{ matches: ["file:///*"], js: ["content.js"] }],
      files: { "content.js": 'document.documentElement.dataset.unprivileged = "ran";' },
    });
    await fixture.addTarget({ id: "communication-management", url: communicationUrl(appPath), title: "Octo Communication" });
    return { pluginDirectories: [pluginRoot] };
  }, async (_host, fixture) => {
    const targetId = "communication-management";
    await waitUntil(async () => {
      const contexts = await fixture.contexts(targetId);
      return contexts.some((context) => !context.isDefault && context.worldName.includes("_management"))
        && contexts.some((context) => !context.isDefault && context.worldName.includes("unprivileged-plugin"));
    });
    const contexts = await fixture.contexts(targetId);
    const managementContext = contexts.find((context) => !context.isDefault && context.worldName.includes("_management"));
    const pluginContext = contexts.find((context) => !context.isDefault && context.worldName.includes("unprivileged-plugin"));
    const pageContext = await fixture.context(targetId, "communication-management-main", "");
    expect(managementContext).toBeDefined();
    expect(managementContext!.isDefault).toBe(false);
    expect(pluginContext).toBeDefined();
    expect(pageContext?.isDefault).toBe(true);
    expect(await fixture.evaluate(targetId, pageContext!.id, "typeof window.octoEnhancer?.openZed")).toBe("undefined");

    const forgedPageCall = JSON.stringify({
      callId: 701,
      extId: "_management",
      api: "management",
      method: "getAll",
      args: [],
    });
    const forgedPluginCall = JSON.stringify({
      callId: 702,
      extId: "_management",
      api: "management",
      method: "getAll",
      args: [],
    });
    if (await fixture.evaluate(targetId, pageContext!.id, "typeof window.__ewSend") === "function") {
      await fixture.evaluate(targetId, pageContext!.id, `window.__ewSend(${JSON.stringify(forgedPageCall)})`);
    } else {
      await fixture.callBinding(targetId, pageContext!.id, "__ewSend", forgedPageCall);
    }
    if (await fixture.evaluate(targetId, pluginContext!.id, "typeof window.__ewSend") === "function") {
      await fixture.evaluate(targetId, pluginContext!.id, `window.__ewSend(${JSON.stringify(forgedPluginCall)})`);
    } else {
      await fixture.callBinding(targetId, pluginContext!.id, "__ewSend", forgedPluginCall);
    }
    await fixture.drain();
    const pageResolutions = await fixture.resolutions(targetId, pageContext!.id);
    const pluginResolutions = await fixture.resolutions(targetId, pluginContext!.id);
    expect(pageResolutions.some((resolution) => resolution.callId === 701 && !resolution.isError)).toBe(false);
    expect(pluginResolutions.some((resolution) => resolution.callId === 702 && !resolution.isError)).toBe(false);
  });
}, 10_000);

test("a failing plugin does not prevent a separate valid plugin from injecting", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "broken-plugin",
      contentScripts: [{ matches: ["file:///*"], js: ["broken.js"] }],
      files: { "broken.js": "if (" },
    });
    await writePlugin(pluginRoot, {
      id: "working-plugin",
      contentScripts: [{ matches: ["file:///*"], js: ["working.js"] }],
      files: { "working.js": 'document.documentElement.dataset.survivingPlugin = "ran";' },
    });
    await fixture.addTarget({ id: "communication-plugin-isolation", url: communicationUrl(appPath), title: "Octo Communication" });
    return { pluginDirectories: [pluginRoot] };
  }, async (_host, fixture) => {
    const targetId = "communication-plugin-isolation";
    await waitUntil(async () => (await fixture.effects(targetId, "communication-plugin-isolation-main")).dataset.survivingPlugin === "ran");
    expect((await fixture.effects(targetId, "communication-plugin-isolation-main")).dataset.survivingPlugin).toBe("ran");
  });
}, 10_000);

test("Zed requires a trusted plugin gesture, preserves path:line:column, and revokes a delayed call on disable", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    const zed = await createFakeZed(join(root, "bin"));
    const filePath = join(root, "notes with spaces $(printf altered).md");
    await writeFile(filePath, "line 1\n".repeat(40));
    const pluginSource = `
      const button = document.createElement("button");
      button.id = "open-with-zed";
      button.addEventListener("click", (event) => {
        if (!event.isTrusted) return;
        window.octoEnhancer.openZed({ path: ${JSON.stringify(filePath)}, line: 37 }).catch(() => {});
        setTimeout(() => window.octoEnhancer.openZed({ path: ${JSON.stringify(filePath)}, line: 38 }).catch(() => {}), 10);
      });
      document.body.appendChild(button);
      window.octoEnhancer.onDispose(() => button.remove());
    `;
    await writePlugin(pluginRoot, {
      id: "zed-open",
      contentScripts: [{ matches: ["file:///*"], js: ["zed.js"] }],
      files: { "zed.js": pluginSource },
    });
    await fixture.addTarget({ id: "communication-zed", url: communicationUrl(appPath), title: "Octo Communication" });
    return { pluginDirectories: [pluginRoot], zedExecutable: zed.executable, zed, filePath };
  }, async (host, fixture, _root, setup) => {
    const targetId = "communication-zed";
    const zed = setup.zed;
    const filePath = setup.filePath;
    if (!zed || !filePath) throw new Error("Zed test fixture was not initialized");
    await waitUntil(async () => (await fixture.contexts(targetId)).some((context) =>
      !context.isDefault && context.worldName.includes("zed-open")));
    const pluginContext = (await fixture.contexts(targetId)).find((context) =>
      !context.isDefault && context.worldName.includes("zed-open"));
    expect(pluginContext).toBeDefined();
    await waitUntil(async () => (await fixture.effects(targetId, "communication-zed-main")).elementIds.includes("open-with-zed"));

    expect(await fixture.click(targetId, pluginContext!.id, "#open-with-zed", false)).toBe(true);
    await fixture.drain();
    expect(await zedArguments(zed.argvFile)).toEqual([]);
    expect(existsSync(zed.markerFile)).toBe(false);

    expect(await fixture.click(targetId, pluginContext!.id, "#open-with-zed", true)).toBe(true);
    await waitUntil(async () => (await zedArguments(zed.argvFile)).length === 1);
    expect(await zedArguments(zed.argvFile)).toEqual([[`${await realpath(filePath)}:37:1`]]);
    expect(existsSync(zed.markerFile)).toBe(true);

    await host.setPluginEnabled("zed-open", false);
    await fixture.advanceTimers(targetId, pluginContext!.id, 1_000);
    await fixture.drain();
    expect(await zedArguments(zed.argvFile)).toEqual([[`${await realpath(filePath)}:37:1`]]);
    expect((await fixture.effects(targetId, "communication-zed-main")).elementIds).not.toContain("open-with-zed");
  });
}, 10_000);

test("reports incomplete disable when a plugin leaves DOM UI without a disposer", async () => {
  await withHost(async (root, fixture, appPath) => {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "residual-ui",
      contentScripts: [{ matches: ["file:///*"], js: ["ui.js"] }],
      files: {
        "ui.js": `
          const panel = document.createElement("aside");
          panel.id = "residual-plugin-ui";
          panel.addEventListener("click", () => {
            const count = Number(document.documentElement.dataset.residualClickCount ?? "0") + 1;
            document.documentElement.dataset.residualClickCount = String(count);
          });
          document.body.appendChild(panel);
        `,
      },
    });
    await fixture.addTarget({ id: "residual-ui-target", url: communicationUrl(appPath) });
    return { pluginDirectories: [pluginRoot] };
  }, async (host, fixture) => {
    const targetId = "residual-ui-target";
    const frameId = `${targetId}-main`;
    await waitUntil(async () => (await fixture.effects(targetId, frameId)).elementIds.includes("residual-plugin-ui"));
    const pluginContext = (await fixture.contexts(targetId)).find((context) =>
      !context.isDefault && context.worldName.includes("residual-ui"));
    expect(pluginContext).toBeDefined();

    expect(await fixture.click(targetId, pluginContext!.id, "#residual-plugin-ui", true)).toBe(true);
    expect((await fixture.effects(targetId, frameId)).dataset.residualClickCount).toBe("1");
    await expect(host.setPluginEnabled("residual-ui", false)).rejects.toThrow();
    expect((await fixture.effects(targetId, frameId)).elementIds).toContain("residual-plugin-ui");

    expect(await fixture.click(targetId, pluginContext!.id, "#residual-plugin-ui", true)).toBe(true);
    expect((await fixture.effects(targetId, frameId)).dataset.residualClickCount).toBe("1");
    await fixture.navigate(targetId, frameId, "about:blank");
    await fixture.drain();
  });
}, 10_000);

test("revokes Zed after navigation invalidates a context captured before realpath validation", async () => {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-host-race-"));
  const appPath = await createFakeAppBundle(root);
  const fixture = await createOwnedCdpFixture();
  let host: Awaited<ReturnType<typeof createGatedHostProcessFixture>> | undefined;
  try {
    const pluginRoot = join(root, "plugins");
    const zed = await createFakeZed(join(root, "bin"));
    const filePath = join(root, "race note.md");
    await writeFile(filePath, "valid file\n");
    await writePlugin(pluginRoot, {
      id: "zed-open",
      contentScripts: [{ matches: ["file:///*"], js: ["zed.js"] }],
      files: {
        "zed.js": `
          const button = document.createElement("button");
          button.id = "open-during-validation";
          button.addEventListener("click", (event) => {
            if (event.isTrusted) window.octoEnhancer.openZed({ path: ${JSON.stringify(filePath)} }).catch(() => {});
          });
          const barrier = document.createElement("button");
          barrier.id = "context-barrier";
          barrier.addEventListener("click", (event) => {
            if (event.isTrusted) window.octoEnhancer.openZed({ path: "relative-path" }).catch(() => {});
          });
          document.body.appendChild(button);
          document.body.appendChild(barrier);
          window.octoEnhancer.onDispose(() => { button.remove(); barrier.remove(); });
        `,
      },
    });
    const targetId = "zed-navigation-race";
    const frameId = `${targetId}-main`;
    await fixture.addTarget({ id: targetId, url: communicationUrl(appPath) });
    host = await createGatedHostProcessFixture({
      appPath,
      port: fixture.port,
      pid: fixture.pid,
      startedAt: fixture.startedAt,
      pluginDirectories: [pluginRoot],
      zedExecutable: zed.executable,
      gatePath: filePath,
    });

    await waitUntil(async () => (await fixture.contexts(targetId)).some((context) =>
      !context.isDefault && context.worldName.includes("zed-open")));
    const initialContext = (await fixture.contexts(targetId)).find((context) =>
      context.frameId === frameId && !context.isDefault && context.worldName.includes("zed-open"));
    expect(initialContext).toBeDefined();
    await waitUntil(async () => (await fixture.effects(targetId, frameId)).elementIds.includes("open-during-validation"));
    const invalidPathEventCount = async () => (await fixture.calls(targetId)).filter((call) =>
      call.method === "Runtime.evaluate"
      && String(call.params.expression).includes("management.onErrors")
      && String(call.params.expression).includes("absolute local file path")).length;
    const initialBarrierCount = await invalidPathEventCount();
    expect(await fixture.click(targetId, initialContext!.id, "#context-barrier", true)).toBe(true);
    await waitUntil(async () => (await invalidPathEventCount()) > initialBarrierCount);
    expect(await fixture.click(targetId, initialContext!.id, "#open-during-validation", true)).toBe(true);

    // The child emits this only from the realpath wrapper called by openInZed,
    // after handleBinding has captured the requesting context owner.
    await host.waitForRealpathGate();
    await fixture.navigate(targetId, frameId, communicationUrl(appPath));
    await waitUntil(async () => !(await fixture.contexts(targetId)).some((context) => context.id === initialContext!.id));
    await waitUntil(async () => {
      const contexts = await fixture.contexts(targetId);
      const current = contexts.find((context) =>
        context.frameId === frameId && !context.isDefault && context.worldName.includes("zed-open"));
      const effects = await fixture.effects(targetId, frameId);
      return Boolean(current && current.id !== initialContext!.id
        && effects.elementIds.includes("context-barrier"));
    });
    const currentContext = (await fixture.contexts(targetId)).find((context) =>
      context.frameId === frameId && !context.isDefault && context.worldName.includes("zed-open"));
    expect(currentContext).toBeDefined();
    const reattachedBarrierCount = await invalidPathEventCount();
    expect(await fixture.click(targetId, currentContext!.id, "#context-barrier", true)).toBe(true);
    await waitUntil(async () => (await invalidPathEventCount()) > reattachedBarrierCount);

    host.releaseRealpathGate();
    await waitUntil(async () => {
      const calls = await fixture.calls(targetId);
      const revoked = calls.some((call) => call.method === "Runtime.evaluate"
        && String(call.params.expression).includes("management.onErrors")
        && String(call.params.expression).includes("plugin was disabled before Zed could be opened"));
      return revoked || (await zedArguments(zed.argvFile)).length > 0;
    });
    expect(await zedArguments(zed.argvFile)).toEqual([]);
    expect(existsSync(zed.markerFile)).toBe(false);
  } finally {
    try { await host?.stop(); } catch { /* Cleanup errors must not hide the race assertion. */ }
    try { await fixture.close(); }
    finally { await rm(root, { recursive: true, force: true }); }
  }
}, 20_000);

test("stopping reports an unremoved plugin element", async () => {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-host-residual-stop-"));
  const fixture = await createOwnedCdpFixture();
  try {
    const appPath = await createFakeAppBundle(root);
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "residual-stop",
      contentScripts: [{ matches: ["file:///*"], js: ["ui.js"] }],
      files: { "ui.js": 'const node = document.createElement("aside"); node.id = "residual-stop-ui"; document.body.appendChild(node);' },
    });
    await fixture.addTarget({ id: "residual-stop-target", url: communicationUrl(appPath) });
    const { startHost } = await loadHostEntry();
    const host = await startHost({ appPath, port: fixture.port, pid: fixture.pid, startedAt: fixture.startedAt, pluginDirectories: [pluginRoot] });
    await waitUntil(async () => (await fixture.effects("residual-stop-target", "residual-stop-target-main")).elementIds.includes("residual-stop-ui"));
    await expect(host.stop()).rejects.toThrow("host stopped with CDP cleanup errors");
  } finally {
    try { await fixture.close(); }
    finally { await rm(root, { recursive: true, force: true }); }
  }
}, 10_000);

test("stopping the host detaches its CDP session and prevents later page injection", async () => {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-host-stop-"));
  const appPath = await createFakeAppBundle(root);
  const fixture = await createOwnedCdpFixture();
  let host: HostSession | undefined;
  try {
    const pluginRoot = join(root, "plugins");
    await writePlugin(pluginRoot, {
      id: "stop-probe",
      contentScripts: [{ matches: ["file:///*"], js: ["probe.js"] }],
      files: { "probe.js": 'document.documentElement.dataset.stopProbe = "ran";' },
    });
    await fixture.addTarget({ id: "before-stop", url: communicationUrl(appPath) });
    const { startHost } = await loadHostEntry();
    host = await startHost({
      appPath,
      port: fixture.port,
      pid: fixture.pid,
      startedAt: fixture.startedAt,
      pluginDirectories: [pluginRoot],
    });
    await waitUntil(async () => (await fixture.effects("before-stop", "before-stop-main")).dataset.stopProbe === "ran");

    await host.stop();
    host = undefined;
    await waitUntil(async () => (await fixture.contexts("before-stop")).length === 0);
    await fixture.addTarget({ id: "after-stop", url: communicationUrl(appPath) });
    await fixture.drain();
    expect(await fixture.contexts("after-stop")).toEqual([]);
    expect((await fixture.effects("after-stop", "after-stop-main")).dataset.stopProbe).toBeUndefined();
  } finally {
    try { await host?.stop(); }
    finally {
      try { await fixture.close(); }
      finally { await rm(root, { recursive: true, force: true }); }
    }
  }
}, 10_000);

test("refuses a CDP listener whose process start identity does not match", async () => {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-enhancer-host-identity-"));
  const appPath = await createFakeAppBundle(root);
  const fixture = await createOwnedCdpFixture();
  let unexpectedlyStarted: HostSession | undefined;
  try {
    const { startHost } = await loadHostEntry();
    const start = startHost({
      appPath,
      port: fixture.port,
      pid: fixture.pid,
      startedAt: `${fixture.startedAt} mismatched`,
      pluginDirectories: [],
    }).then((host) => {
      unexpectedlyStarted = host;
      return host;
    });
    try { await expect(start).rejects.toThrow(); }
    finally { await unexpectedlyStarted?.stop(); }
  } finally {
    try { await fixture.close(); }
    finally { await rm(root, { recursive: true, force: true }); }
  }
}, 10_000);
