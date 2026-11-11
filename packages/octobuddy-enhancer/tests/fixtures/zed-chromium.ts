import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { chromium, type BrowserContext, type CDPSession, type Locator, type Page, type Route } from "playwright";

export type ZedFixturePage = "communication" | "renderer";
export type ZedFixtureLinkKind = "file-url" | "absolute-path" | "http" | "relative";

export interface ZedFixtureLink {
  kind: ZedFixtureLinkKind;
  label: string;
  line?: number;
}

export interface ZedChromiumFixture {
  page: Page;
  filePath: string;
  link(index: number): Locator;
  bridgeCalls(): Promise<Array<{ path: string; line?: number }>>;
  disposalRegistrations(): Promise<number>;
  blockedRequests(): string[];
  waitForBlockedRequest(prefix: string): Promise<string>;
  dispose(): Promise<number>;
}

interface FixtureOptions {
  target: ZedFixturePage;
  links: ZedFixtureLink[];
}

const contentScriptPath = new URL("../../plugins/zed/content.js", import.meta.url);
const isolatedWorldName = "octobuddy-zed-test-world";

/**
 * Requires the repository's Playwright 1.55.0 dependency and its Chromium
 * binary, plus packages/octobuddy-enhancer/plugins/zed/content.js for green
 * behavior. Uses only a temporary profile and temporary OctoBuddy-shaped
 * file:// pages; HTTP(S) requests are aborted. The only fake is the native
 * host capability recorder.
 */
export async function withZedChromiumPage(
  options: FixtureOptions,
  exercise: (fixture: ZedChromiumFixture) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "octobuddy-zed-e2e-"));
  let context: BrowserContext | undefined;

  try {
    const profileDir = join(root, "profile");
    const appRoot = join(root, "OctoBuddy.app");
    const sourceDir = join(root, "workspace with spaces", "src");
    const filePath = join(sourceDir, "Widget with spaces.ts");
    const targetPath = options.target === "communication"
      ? join(appRoot, "Contents", "Resources", "communication-renderer", "index.html")
      : join(appRoot, "Contents", "Resources", "app.asar", "out", "renderer", "index.html");

    await Promise.all([
      mkdir(profileDir, { recursive: true }),
      mkdir(sourceDir, { recursive: true }),
      mkdir(dirname(targetPath), { recursive: true }),
    ]);
    await writeFile(filePath, "export const widget = true;\n", "utf8");
    await writeFile(join(dirname(targetPath), "relative.ts"), "export const relative = true;\n", "utf8");

    const anchorMarkup = options.links.map((link, index) => {
      const line = link.line ?? 1;
      const suffix = link.kind === "file-url" || link.kind === "http"
        ? `#L${line}`
        : `:${line}`;
      const href = link.kind === "file-url"
        ? `${pathToFileURL(filePath).href}#L${line}`
        : link.kind === "absolute-path"
          ? `${filePath}:${line}`
          : link.kind === "relative"
            ? `./relative.ts#L${line}`
            : `https://example.invalid/source/Widget.ts${suffix}`;
      return `<section><a data-fixture-link="${index}" data-fixture-kind="${link.kind}" href="${escapeAttribute(href)}"${link.kind === "http" ? ' target="_blank"' : ""}>${escapeHtml(link.label)}</a></section>`;
    }).join("\n");

    const html = `<!doctype html>
<html><head><meta charset="utf-8"><title>OctoBuddy isolated fixture</title>
<style>
  body { margin: 40px; font: 16px sans-serif; }
  section { min-height: 48px; margin: 0 0 260px; }
  a { color: #0645ad; }
</style></head>
<body><main>${anchorMarkup}</main>
<script>
  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    const link = event.target.closest("[data-fixture-link]");
    if (link && link.dataset.fixtureKind !== "http") event.preventDefault();
  }, true);
</script></body></html>`;
    await writeFile(targetPath, html, "utf8");

    context = await chromium.launchPersistentContext(profileDir, {
      headless: true,
      args: [
        "--disable-background-networking",
        "--disable-component-update",
        "--disable-default-apps",
        "--disable-sync",
        "--no-first-run",
        "--no-default-browser-check",
      ],
    });
    const blockedRequests: string[] = [];
    const blockedRequestWaiters: Array<{ prefix: string; resolve(url: string): void }> = [];
    const waitForBlockedRequest = (prefix: string): Promise<string> => {
      const completed = blockedRequests.find((url) => url.startsWith(prefix));
      if (completed) return Promise.resolve(completed);
      const { promise, resolve } = Promise.withResolvers<string>();
      blockedRequestWaiters.push({ prefix, resolve });
      return promise;
    };
    await context.route("**/*", async (route: Route) => {
      const protocol = new URL(route.request().url()).protocol;
      if (protocol === "http:" || protocol === "https:") {
        const url = route.request().url();
        await route.abort();
        blockedRequests.push(url);
        for (let index = blockedRequestWaiters.length - 1; index >= 0; index -= 1) {
          const waiter = blockedRequestWaiters[index]!;
          if (url.startsWith(waiter.prefix)) {
            blockedRequestWaiters.splice(index, 1);
            waiter.resolve(url);
          }
        }
        return;
      }
      await route.continue();
    });

    const page = context.pages()[0] ?? await context.newPage();
    page.setDefaultTimeout(3_000);
    const cdp = await context.newCDPSession(page);
    await cdp.send("Page.enable");
    await cdp.send("Runtime.enable");
    await page.goto(pathToFileURL(targetPath).href, { waitUntil: "load" });

    const frameTree = await cdp.send("Page.getFrameTree") as { frameTree: { frame: { id: string } } };
    const isolated = await cdp.send("Page.createIsolatedWorld", {
      frameId: frameTree.frameTree.frame.id,
      worldName: isolatedWorldName,
      grantUniveralAccess: false,
    }) as { executionContextId: number };
    const contextId = isolated.executionContextId;
    const pluginSource = await readPluginSource();
    await evaluateInWorld(cdp, contextId, bridgeBootstrap);
    if (pluginSource !== "") await evaluateInWorld(cdp, contextId, pluginSource);

    const readBridge = async <T>(expression: string): Promise<T> => {
      const response = await cdp.send("Runtime.evaluate", {
        expression,
        contextId,
        awaitPromise: true,
        returnByValue: true,
      }) as { result?: { value?: T }; exceptionDetails?: { text?: string } };
      if (response.exceptionDetails) throw new Error(response.exceptionDetails.text ?? "isolated-world fixture evaluation failed");
      return response.result?.value as T;
    };

    await exercise({
      page,
      filePath,
      link: (index) => page.locator(`[data-fixture-link="${index}"]`),
      bridgeCalls: () => readBridge("window.__zedTestBridge.calls"),
      disposalRegistrations: () => readBridge("window.__zedTestBridge.disposeCallbacks.length"),
      blockedRequests: () => blockedRequests,
      waitForBlockedRequest,
      dispose: () => readBridge(`(async () => {
        const callbacks = window.__zedTestBridge.disposeCallbacks.splice(0);
        for (const callback of callbacks) await callback();
        return callbacks.length;
      })()`),
    });
  } finally {
    try {
      await context?.close();
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
}

async function readPluginSource(): Promise<string> {
  try {
    return await readFile(contentScriptPath, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "";
    throw error;
  }
}

async function evaluateInWorld(cdp: CDPSession, contextId: number, expression: string): Promise<void> {
  const result = await cdp.send("Runtime.evaluate", {
    expression,
    contextId,
    awaitPromise: true,
    returnByValue: true,
  }) as { exceptionDetails?: { text?: string } };
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.text ?? "isolated-world plugin evaluation failed");
}

const bridgeBootstrap = `(() => {
  const calls = [];
  const disposeCallbacks = [];
  Object.defineProperty(window, "__zedTestBridge", {
    configurable: false,
    value: { calls, disposeCallbacks },
  });
  Object.defineProperty(window, "octoEnhancer", {
    configurable: false,
    value: Object.freeze({
      openZed: async (request) => {
        calls.push(structuredClone(request));
      },
      onDispose: (callback) => {
        if (typeof callback !== "function") throw new TypeError("onDispose requires a callback");
        disposeCallbacks.push(callback);
      },
    }),
  });
})()`;

function escapeAttribute(value: string): string {
  return value.replaceAll("&", "&amp;").replaceAll('"', "&quot;").replaceAll("<", "&lt;");
}

function escapeHtml(value: string): string {
  return escapeAttribute(value).replaceAll(">", "&gt;");
}
