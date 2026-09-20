import { chmod, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "bun:test";
import { ensureStateDirectory, loadConfig, parseConfig } from "../src/runtime/config.ts";
import { generateLaunchdPlist, installLaunchdPlist } from "../src/runtime/launchd.ts";

function configDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    apiUrl: "https://octo.example.test/api",
    ownerId: "owner-uid",
    allowedGroupIds: ["group-a"],
    ompCommand: "/usr/bin/omp",
    stateDir: "/tmp/omp-channel-octo-test",
    botToken: "private-token",
    ...overrides,
  };
}

test("config rejects token environment names that are not valid OCTO-prefixed variables", () => {
  expect(() => parseConfig(configDocument({ botToken: undefined, botTokenEnv: "OCTO_BAD-NAME" }), {
    env: { "OCTO_BAD-NAME": "token" },
  })).toThrow(/botTokenEnv/);
});

test("config loader refuses a private-token file with group or other permissions", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-channel-config-"));
  const path = join(directory, "config.json");
  try {
    await writeFile(path, JSON.stringify(configDocument()), { mode: 0o644 });
    await expect(loadConfig(path)).rejects.toThrow(/权限|私有|private/i);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("state directory must be a writable directory, not an existing file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-channel-state-"));
  const path = join(directory, "not-a-directory");
  try {
    await writeFile(path, "file");
    const config = parseConfig(configDocument({ stateDir: path }));
    await expect(ensureStateDirectory(config)).rejects.toThrow(/目录|directory/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("launchd generation requires absolute executable arguments and escapes XML", () => {
  expect(() => generateLaunchdPlist({
    label: "com.example.octo",
    programArguments: ["bun", "run", "/tmp/cli.ts"],
  })).toThrow(/绝对|absolute/);

  const plist = generateLaunchdPlist({
    label: "com.example.octo<&",
    programArguments: ["/usr/bin/bun", "run", "/tmp/cli<&.ts"],
  });
  expect(plist).toContain("com.example.octo&lt;&amp;");
  expect(plist).toContain("/tmp/cli&lt;&amp;.ts");
  expect(plist).toContain("<key>KeepAlive</key>");
  expect(plist).toContain("<key>RunAtLoad</key>");
});

test("installLaunchdPlist repairs permissions on an existing plist", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omp-channel-plist-"));
  const path = join(directory, "agent.plist");
  try {
    await writeFile(path, "old", { mode: 0o644 });
    await installLaunchdPlist(path, {
      label: "com.example.octo",
      programArguments: ["/usr/bin/bun", "run", "/tmp/cli.ts"],
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect(await readFile(path, "utf8")).toContain("<plist version=\"1.0\">");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
