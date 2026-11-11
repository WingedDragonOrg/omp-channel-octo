import { chmod, mkdir, realpath, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const systemApplications = ["/Applications", "/System/Applications"];

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function parseOutput(args: string[]): string {
  if (args.length === 0) return join(packageDir, "dist", "OctoBuddy Enhancer.app");
  if (args.length !== 2 || args[0] !== "--output" || !args[1]) {
    throw new Error("usage: bun scripts/build-app.ts [--output <path>]");
  }
  return args[1];
}

function ensureNotSystemApplication(outputPath: string): void {
  const resolvedOutput = resolve(outputPath);
  for (const root of systemApplications) {
    if (resolvedOutput === root || resolvedOutput.startsWith(`${root}/`)) {
      throw new Error(`refusing to write app bundle into system applications directory: ${resolvedOutput}`);
    }
  }
}

async function buildApp(outputPath: string): Promise<void> {
  const bundlePath = resolve(outputPath);
  ensureNotSystemApplication(bundlePath);
  const contentsPath = join(bundlePath, "Contents");
  const executablePath = join(contentsPath, "MacOS", "OctoBuddyEnhancer");
  const bunPath = process.execPath;
  const cliPath = join(packageDir, "src", "cli.ts");
  await mkdir(dirname(bundlePath), { recursive: true });
  ensureNotSystemApplication(join(await realpath(dirname(bundlePath)), basename(bundlePath)));
  await mkdir(bundlePath, { mode: 0o755 });
  await mkdir(join(contentsPath, "MacOS"), { recursive: true, mode: 0o755 });

  const infoPlist = [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
    "<plist version=\"1.0\"><dict>",
    "<key>CFBundleExecutable</key><string>OctoBuddyEnhancer</string>",
    "<key>CFBundleIdentifier</key><string>local.octobuddy.enhancer</string>",
    "<key>CFBundleName</key><string>OctoBuddy Enhancer</string>",
    "<key>CFBundlePackageType</key><string>APPL</string>",
    "<key>CFBundleShortVersionString</key><string>1.0</string>",
    "<key>CFBundleVersion</key><string>1</string>",
    "</dict></plist>",
    "",
  ].join("\n");
  const launcher = [
    "#!/bin/sh",
    `exec ${shellQuote(bunPath)} ${shellQuote(cliPath)} --dock`,
    "",
  ].join("\n");

  await writeFile(join(contentsPath, "Info.plist"), infoPlist, { encoding: "utf8", flag: "wx", mode: 0o644 });
  await writeFile(executablePath, launcher, { encoding: "utf8", flag: "wx", mode: 0o755 });
  await chmod(executablePath, 0o755);
  process.stdout.write(`${bundlePath}\n`);
}

if (import.meta.main) {
  try {
    await buildApp(parseOutput(process.argv.slice(2)));
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
