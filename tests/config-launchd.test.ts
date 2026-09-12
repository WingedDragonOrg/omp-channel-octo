import { expect, test } from "bun:test";
import { generateLaunchdPlist } from "../src/launchd.ts";
import { parseConfig } from "../src/config.ts";

function configDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    apiUrl: "https://octo.example.test/api",
    botToken: "secret-value",
    allowedGroupIds: [],
    ompCommand: "omp",
    stateDir: "/tmp/octo",
    ...overrides,
  };
}

test("config parser resolves paths and accepts only filtered token env names", () => {
  const config = parseConfig(
    {
      apiUrl: "https://octo.example.test/api",
      botTokenEnv: "OCTO_BOT_TOKEN",
      ownerId: "owner",
      allowedGroupIds: ["group-a", "thread-a"],
      ompCommand: "omp",
      stateDir: "~/state/octo",
    },
    { homeDir: "/home/test", env: { OCTO_BOT_TOKEN: "secret-value" } },
  );
  expect(config.stateDir).toBe("/home/test/state/octo");
  expect(config.botToken).toBe("secret-value");
  expect(config.wsUrl).toBeUndefined();
  expect(() => parseConfig(
    {
      apiUrl: "https://octo.example.test",
      botTokenEnv: "AWS_SECRET_ACCESS_KEY",
      ownerId: "owner",
      allowedGroupIds: [],
      ompCommand: "omp",
      stateDir: "/tmp/octo",
    },
    { env: { AWS_SECRET_ACCESS_KEY: "secret" } },
  )).toThrow(/前缀/);
});

test("config parser uses the default API and omits owner when both fields are absent", () => {
  const document = configDocument();
  delete document.apiUrl;
  const config = parseConfig(document);
  expect(config.apiUrl).toBe("https://im.deepminer.com.cn");
  expect(config).not.toHaveProperty("ownerId");
});

test("blank owner is treated as unconfigured", () => {
  const config = parseConfig(configDocument({ ownerId: " \t " }));
  expect(config).not.toHaveProperty("ownerId");
});

test("non-string owner values remain invalid", () => {
  for (const ownerId of [null, 123, false, [], {}]) {
    expect(() => parseConfig(configDocument({ ownerId }))).toThrow(/ownerId/);
  }
});

test("explicit API URL and owner remain configured", () => {
  const config = parseConfig(configDocument({
    apiUrl: "https://octo.example.test/api",
    ownerId: "explicit-owner",
  }));
  expect(config.apiUrl).toBe("https://octo.example.test/api");
  expect(config.ownerId).toBe("explicit-owner");
});

test("config still requires credentials and all other required fields", () => {
  for (const field of ["allowedGroupIds", "ompCommand", "stateDir"] as const) {
    const document = configDocument({ ownerId: "owner" });
    delete document[field];
    expect(() => parseConfig(document)).toThrow(new RegExp(field));
  }

  const withoutToken = configDocument({ ownerId: "owner" });
  delete withoutToken.botToken;
  expect(() => parseConfig(withoutToken, { env: {} })).toThrow(/OCTO_BOT_TOKEN/);
});

test("launchd plist escapes XML and keeps credentials out of program arguments", () => {
  const plist = generateLaunchdPlist({
    label: "com.example.octo<&",
    programArguments: ["/usr/bin/bun", "run", "src/cli.ts", "serve", "--config", "/tmp/config<&.json"],
    stdoutPath: "/tmp/stdout.log",
    stderrPath: "/tmp/stderr.log",
  });
  expect(plist).toContain("com.example.octo&lt;&amp;");
  expect(plist).toContain("/tmp/config&lt;&amp;.json");
  expect(plist).not.toContain("bot-token");
  expect(plist).toContain("<key>RunAtLoad</key>");
});
