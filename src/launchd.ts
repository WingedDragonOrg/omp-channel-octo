import { chmod, mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, isAbsolute } from "node:path";

export const DEFAULT_LAUNCHD_LABEL = "com.omp.channel-octo";
export const DEFAULT_LAUNCHD_PLIST_PATH = "~/Library/LaunchAgents/com.omp.channel-octo.plist";

export interface LaunchdOptions {
  label: string;
  programArguments: string[];
  workingDirectory?: string;
  stdoutPath?: string;
  stderrPath?: string;
  environmentVariables?: Record<string, string>;
  runAtLoad?: boolean;
  keepAlive?: boolean;
}

function xml(value: string): string {
  if (/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value)) {
    throw new Error("launchd plist 包含非法 XML 字符");
  }
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function stringArray(values: readonly string[]): string {
  return `<array>\n${values.map((value) => `    <string>${xml(value)}</string>`).join("\n")}\n  </array>`;
}

function bool(value: boolean): string {
  return value ? "<true/>" : "<false/>";
}

function validatePath(value: string, field: string): void {
  if (!isAbsolute(value)) throw new Error(`launchd ${field} 必须是绝对路径`);
}

export function generateLaunchdPlist(options: LaunchdOptions): string {
  if (typeof options.label !== "string" || options.label.trim().length === 0) {
    throw new Error("launchd label 不能为空");
  }
  if (
    !Array.isArray(options.programArguments)
    || options.programArguments.length === 0
    || options.programArguments.some((value) => typeof value !== "string" || value.length === 0)
  ) {
    throw new Error("launchd ProgramArguments 不能为空");
  }
  validatePath(options.programArguments[0]!, "第一个 ProgramArguments");
  if (options.workingDirectory !== undefined) validatePath(options.workingDirectory, "WorkingDirectory");
  if (options.stdoutPath !== undefined) validatePath(options.stdoutPath, "StandardOutPath");
  if (options.stderrPath !== undefined) validatePath(options.stderrPath, "StandardErrorPath");

  const environment = options.environmentVariables;
  if (environment !== undefined) {
    for (const [key, value] of Object.entries(environment)) {
      if (key.length === 0 || key.includes("=") || key.includes("\u0000")) {
        throw new Error("launchd EnvironmentVariables 名称无效");
      }
      if (typeof value !== "string") throw new Error("launchd EnvironmentVariables 值无效");
    }
  }

  const lines = [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
    "<plist version=\"1.0\">",
    "<dict>",
    `  <key>Label</key>\n  <string>${xml(options.label)}</string>`,
    `  <key>ProgramArguments</key>\n  ${stringArray(options.programArguments)}`,
  ];
  if (options.workingDirectory !== undefined) {
    lines.push(`  <key>WorkingDirectory</key>\n  <string>${xml(options.workingDirectory)}</string>`);
  }
  if (options.stdoutPath !== undefined) {
    lines.push(`  <key>StandardOutPath</key>\n  <string>${xml(options.stdoutPath)}</string>`);
  }
  if (options.stderrPath !== undefined) {
    lines.push(`  <key>StandardErrorPath</key>\n  <string>${xml(options.stderrPath)}</string>`);
  }
  if (environment !== undefined && Object.keys(environment).length > 0) {
    lines.push("  <key>EnvironmentVariables</key>", "  <dict>");
    for (const [key, value] of Object.entries(environment).sort(([left], [right]) => left.localeCompare(right))) {
      lines.push(`    <key>${xml(key)}</key>\n    <string>${xml(value)}</string>`);
    }
    lines.push("  </dict>");
  }
  lines.push(
    `  <key>RunAtLoad</key>\n  ${bool(options.runAtLoad ?? true)}`,
    `  <key>KeepAlive</key>\n  ${bool(options.keepAlive ?? true)}`,
    "</dict>",
    "</plist>",
    "",
  );
  return lines.join("\n");
}

export async function installLaunchdPlist(path: string, options: LaunchdOptions): Promise<void> {
  validatePath(path, "plist 路径");
  const parent = dirname(path);
  const temporaryPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  const contents = generateLaunchdPlist(options);
  try {
    await mkdir(parent, { recursive: true, mode: 0o700 });
    await chmod(parent, 0o700);
    await writeFile(temporaryPath, contents, { encoding: "utf8", flag: "wx", mode: 0o600 });
    await chmod(temporaryPath, 0o600);
    await rename(temporaryPath, path);
    await chmod(path, 0o600);
  } catch {
    await unlink(temporaryPath).catch(() => {});
    throw new Error("无法写入 launchd plist");
  }
}

export async function uninstallLaunchdPlist(path: string): Promise<void> {
  validatePath(path, "plist 路径");
  try {
    await unlink(path);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return;
    throw new Error("无法删除 launchd plist");
  }
}
