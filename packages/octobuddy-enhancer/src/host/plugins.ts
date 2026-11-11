import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { isAbsolute, join, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isRecord } from "./type-guards.ts";

export const managementPluginId = "_management";

export interface ContentScriptUnit {
  kind: "js" | "css";
  path: string;
  source: string;
  matches: string[];
  excludeMatches: string[];
  includeGlobs: string[];
  excludeGlobs: string[];
  runAt: "document_start" | "document_end" | "document_idle";
}

export interface HostPlugin {
  id: string;
  name: string;
  version: string;
  description: string;
  builtin: boolean;
  enabled: boolean;
  directory: string;
  units: ContentScriptUnit[];
  errors: string[];
  generation: number;
  cleanupBlockedContexts: Set<string>;
}


function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function bounded(value: string, limit = 500): string {
  return value.length <= limit ? value : `${value.slice(0, limit)}…`;
}

function describeError(error: unknown): string {
  return bounded(error instanceof Error ? error.message : String(error));
}

function reportPluginError(id: string, error: unknown): string {
  const message = describeError(error);
  process.stderr.write(`[octobuddy-enhancer] plugin ${id}: ${message}\n`);
  return message;
}

function isSafePluginId(id: string): boolean {
  return /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/.test(id) && id !== managementPluginId;
}

function isInside(directory: string, file: string): boolean {
  return file.startsWith(`${directory}${sep}`);
}

async function readPluginSource(pluginDirectory: string, relativePath: string): Promise<string> {
  if (!relativePath || isAbsolute(relativePath) || relativePath.includes("\0")) {
    throw new Error(`invalid content script path: ${relativePath}`);
  }
  const base = await realpath(pluginDirectory);
  const candidate = resolve(base, relativePath);
  const resolved = await realpath(candidate);
  if (!isInside(base, resolved)) throw new Error(`content script escapes its plugin directory: ${relativePath}`);
  const info = await lstat(candidate);
  if (!info.isFile() || info.isSymbolicLink()) throw new Error(`content script is not a regular file: ${relativePath}`);
  return await readFile(resolved, "utf8");
}

async function assertPrivatePlugin(pluginDirectory: string, manifestPath: string): Promise<void> {
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("private plugin ownership checks are unavailable on this platform");
  const directoryInfo = await lstat(pluginDirectory);
  const manifestInfo = await lstat(manifestPath);
  if (!directoryInfo.isDirectory() || directoryInfo.isSymbolicLink() || directoryInfo.uid !== uid || (directoryInfo.mode & 0o077) !== 0) {
    throw new Error("user plugin directory must be owned by the current user and private");
  }
  if (!manifestInfo.isFile() || manifestInfo.isSymbolicLink() || manifestInfo.uid !== uid || (manifestInfo.mode & 0o077) !== 0) {
    throw new Error("user plugin manifest must be owned by the current user and private");
  }
}

function normalizeRunAt(value: unknown): ContentScriptUnit["runAt"] | undefined {
  if (value === undefined) return "document_idle";
  if (value === "document_start" || value === "document_end" || value === "document_idle") return value;
  return undefined;
}

async function loadPluginDirectory(
  pluginDirectory: string,
  privateRoot: boolean,
): Promise<HostPlugin | undefined> {
  const pluginPath = await realpath(pluginDirectory);
  const manifestPath = resolve(pluginPath, "manifest.json");
  const idHint = pluginDirectory.split(sep).at(-1) || "unknown";
  try {
    const info = await lstat(manifestPath);
    if (!info.isFile() || info.isSymbolicLink()) throw new Error("manifest.json is not a regular file");
    if (privateRoot) await assertPrivatePlugin(pluginPath, manifestPath);
    const manifestText = await readFile(manifestPath, "utf8");
    const parsed: unknown = JSON.parse(manifestText);
    if (!isRecord(parsed) || parsed.manifest_version !== 3) throw new Error("manifest must be a Manifest V3 object");

    const idValue = parsed.id;
    const id = typeof idValue === "string"
      ? idValue
      : `local-${createHash("sha256").update(pluginPath).digest("hex").slice(0, 16)}`;
    if (!isSafePluginId(id)) throw new Error(`invalid plugin id: ${id}`);

    const name = typeof parsed.name === "string" && parsed.name.length > 0 ? bounded(parsed.name, 160) : id;
    const version = typeof parsed.version === "string" ? bounded(parsed.version, 80) : "";
    const description = typeof parsed.description === "string" ? bounded(parsed.description, 500) : "";
    const errors: string[] = [];
    const units: ContentScriptUnit[] = [];
    const entries = Array.isArray(parsed.content_scripts) ? parsed.content_scripts : [];

    for (let entryIndex = 0; entryIndex < entries.length; entryIndex += 1) {
      const entry = entries[entryIndex];
      if (!isRecord(entry)) {
        errors.push(reportPluginError(id, new Error(`content_scripts[${entryIndex}] is invalid`)));
        continue;
      }
      if (typeof entry.world === "string" && entry.world.toLowerCase() !== "isolated") {
        errors.push(reportPluginError(id, new Error(`content_scripts[${entryIndex}] cannot run in the page world`)));
        continue;
      }
      const matches = asStringArray(entry.matches);
      if (matches.length === 0) {
        errors.push(reportPluginError(id, new Error(`content_scripts[${entryIndex}] has no match patterns`)));
        continue;
      }
      const runAt = normalizeRunAt(entry.run_at);
      if (!runAt) {
        errors.push(reportPluginError(id, new Error(`content_scripts[${entryIndex}] has an unsupported run_at value`)));
        continue;
      }
      const common = {
        matches,
        excludeMatches: asStringArray(entry.exclude_matches),
        includeGlobs: asStringArray(entry.include_globs),
        excludeGlobs: asStringArray(entry.exclude_globs),
        runAt,
      };
      for (const kind of ["css", "js"] as const) {
        for (const relativePath of asStringArray(entry[kind])) {
          try {
            const source = await readPluginSource(pluginPath, relativePath);
            if (kind === "js") {
              try { new Function(source); } // Parse only; execution stays in the plugin's isolated renderer world.
              catch (error) {
                errors.push(reportPluginError(id, new Error(`${relativePath}: invalid JavaScript: ${describeError(error)}`)));
                continue;
              }
            }
            units.push({ ...common, kind, path: relativePath, source });
          } catch (error) {
            errors.push(reportPluginError(id, error));
          }
        }
      }
    }

    return {
      id,
      name,
      version,
      description,
      builtin: false,
      enabled: true,
      directory: pluginPath,
      units,
      errors,
      generation: 0,
      cleanupBlockedContexts: new Set(),
    };
  } catch (error) {
    reportPluginError(idHint, error);
    return undefined;
  }
}

export async function loadPlugins(pluginDirectories: string[]): Promise<Map<string, HostPlugin>> {
  const result = new Map<string, HostPlugin>();
  const userRoot = resolve(homedir(), ".config", "octobuddy-enhancer", "plugins");
  for (const root of pluginDirectories) {
    let rootPath: string;
    try {
      const rootInfo = await lstat(root);
      if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) {
        reportPluginError(root, new Error("plugin root is not a directory"));
        continue;
      }
      rootPath = await realpath(root);
    } catch (error) {
      if (error !== null && typeof error === "object" && "code" in error && error.code === "ENOENT") continue;
      reportPluginError(root, error);
      continue;
    }

    let children: string[];
    try {
      children = (await readdir(rootPath)).sort();
    } catch (error) {
      reportPluginError(rootPath, error);
      continue;
    }
    for (const name of children) {
      const candidate = resolve(rootPath, name);
      try {
        const childInfo = await lstat(candidate);
        if (!childInfo.isDirectory() || childInfo.isSymbolicLink()) continue;
        const plugin = await loadPluginDirectory(candidate, rootPath === userRoot);
        if (!plugin) continue;
        if (result.has(plugin.id)) {
          reportPluginError(plugin.id, new Error("duplicate plugin id; the earlier trusted plugin remains active"));
          continue;
        }
        result.set(plugin.id, plugin);
      } catch (error) {
        reportPluginError(name, error);
      }
    }
  }
  return result;
}

export function allowedOctoBuddyPageUrls(appPath: string): string[] {
  const resources = join(appPath, "Contents", "Resources");
  return [
    pathToFileURL(join(resources, "communication-renderer", "index.html")).href,
    pathToFileURL(join(resources, "app.asar", "out", "renderer", "index.html")).href,
  ];
}

export function isAllowedOctoBuddyUrl(value: string, appPath: string): boolean {
  try {
    const url = new URL(value);
    if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return false;
    url.hash = "";
    return allowedOctoBuddyPageUrls(appPath).includes(pathToFileURL(fileURLToPath(url)).href);
  } catch {
    return false;
  }
}

// Match and per-entry injection semantics follow electron-webext, restricted to the two OctoBuddy file pages.
function matchPattern(value: string, pattern: string): boolean {
  if (pattern === "<all_urls>") return true;
  const parsedUrl = new URL(value);
  const match = /^([a-zA-Z*]+):\/\/([^/]*)(\/.*)$/.exec(pattern);
  if (!match) return false;
  const scheme = match[1]!;
  const host = match[2]!;
  const path = match[3]!;
  if (scheme !== "*" && scheme !== parsedUrl.protocol.slice(0, -1)) return false;
  if (scheme === "*" && parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") return false;
  if (parsedUrl.protocol === "file:") {
    if (host !== "" && host !== "*") return false;
  } else if (host !== "*" && host !== parsedUrl.hostname && !(host.startsWith("*.") && parsedUrl.hostname.endsWith(host.slice(1)))) {
    return false;
  }
  const pathRegex = new RegExp(`^${path.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*")}$`);
  return pathRegex.test(parsedUrl.pathname);
}

function matchesGlob(value: string, glob: string): boolean {
  const regex = new RegExp(`^${glob.replace(/[.+^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*").replaceAll("?", ".")}$`);
  return regex.test(value);
}

export function matchesContentScriptUrl(value: string, unit: ContentScriptUnit, appPath: string): boolean {
  if (!isAllowedOctoBuddyUrl(value, appPath)) return false;
  let url: URL;
  try { url = new URL(value); }
  catch { return false; }
  if (!unit.matches.some((pattern) => {
    try { return matchPattern(url.href, pattern); }
    catch { return false; }
  })) return false;
  if (unit.excludeMatches.some((pattern) => {
    try { return matchPattern(url.href, pattern); }
    catch { return false; }
  })) return false;
  if (unit.includeGlobs.length > 0 && !unit.includeGlobs.some((glob) => matchesGlob(url.href, glob))) return false;
  return !unit.excludeGlobs.some((glob) => matchesGlob(url.href, glob));
}

export function buildContentScriptSource(plugin: HostPlugin, unit: ContentScriptUnit, appPath: string): string {
  const allowedUrls = JSON.stringify(allowedOctoBuddyPageUrls(appPath));
  const matchPatterns = JSON.stringify(unit.matches);
  const excludeMatches = JSON.stringify(unit.excludeMatches);
  const includeGlobs = JSON.stringify(unit.includeGlobs);
  const excludeGlobs = JSON.stringify(unit.excludeGlobs);
  const runBody = unit.kind === "css"
    ? `const style = document.createElement("style"); style.dataset.octoEnhancerPlugin = ${JSON.stringify(plugin.id)}; style.textContent = ${JSON.stringify(unit.source)}; (document.head || document.documentElement).appendChild(style); window.__ewHostRegisterDispose(() => style.remove());`
    : unit.source;
  const schedule = unit.kind === "css" || unit.runAt === "document_start"
    ? "run();"
    : unit.runAt === "document_end"
      ? `if (document.readyState === "loading") { const onReady = () => run(); document.addEventListener("DOMContentLoaded", onReady, { once: true }); window.__ewHostRegisterDispose(() => document.removeEventListener("DOMContentLoaded", onReady)); } else run();`
      : `if (document.readyState === "complete") run(); else { const onReady = () => run(); window.addEventListener("load", onReady, { once: true }); window.__ewHostRegisterDispose(() => window.removeEventListener("load", onReady)); }`;

  return `(() => {
    const matchPattern = ${matchPattern.toString()};
    const matchesGlob = ${matchesGlob.toString()};
    const matchesCurrentUrl = (value) => {
      let url;
      try { url = new URL(value); } catch { return false; }
      if (url.protocol !== "file:" || url.hostname !== "" || url.search !== "") return false;
      const pathUrl = new URL(url.href);
      pathUrl.hash = "";
      if (!${allowedUrls}.includes(pathUrl.href)) return false;
      const matches = ${matchPatterns};
      const excludes = ${excludeMatches};
      const includes = ${includeGlobs};
      const excludedGlobs = ${excludeGlobs};
      if (!matches.some((pattern) => { try { return matchPattern(value, pattern); } catch { return false; } })) return false;
      if (excludes.some((pattern) => { try { return matchPattern(value, pattern); } catch { return false; } })) return false;
      if (includes.length && !includes.some((glob) => matchesGlob(value, glob))) return false;
      return !excludedGlobs.some((glob) => matchesGlob(value, glob));
    };
    const run = () => {
      if (window !== window.top || !matchesCurrentUrl(window.location.href)) return;
      window.__ewHostActivate?.();
      window.__ewHostMarkScript?.();
      ${runBody}
    };
    if (!matchesCurrentUrl(window.location.href)) return;
    ${schedule}
  })();`;
}
