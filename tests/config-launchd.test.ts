import { readFileSync } from "node:fs";
import { expect, test } from "bun:test";
import { generateLaunchdPlist } from "../src/launchd.ts";
import {
  DEFAULT_CONFIG_PATH,
  ocoConfigDir,
  ocoLogsDir,
  parseConfig,
  toCoreConfig,
} from "../src/config.ts";

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

test("the retired restart command is refused instead of silently ignored", () => {
  expect(() => parseConfig(configDocument({ restart: { command: ["super", "-y", "restart", "omp-channel-octo"] } })))
    .toThrow(/restart/);
  expect(() => parseConfig(configDocument({ restart: {} }))).toThrow(/restart/);
});

test("the default config path lives under the oco home", () => {
  expect(DEFAULT_CONFIG_PATH).toBe("~/.oco/config/config.json");
  expect(ocoConfigDir("/home/test")).toBe("/home/test/.oco/config");
  expect(ocoLogsDir("/home/test")).toBe("/home/test/.oco/logs");
});

test("group history defaults to enabled with 50 messages", () => {
  expect(toCoreConfig(parseConfig(configDocument())).groupHistory).toEqual({ enabled: true, maxMessages: 50 });
  expect(toCoreConfig(parseConfig(configDocument({ groupHistory: {} }))).groupHistory)
    .toEqual({ enabled: true, maxMessages: 50 });
  expect(toCoreConfig(parseConfig(configDocument({ groupHistory: { maxMessages: 12 } }))).groupHistory)
    .toEqual({ enabled: true, maxMessages: 12 });
  expect(toCoreConfig(parseConfig(configDocument({ groupHistory: { enabled: false } }))).groupHistory)
    .toEqual({ enabled: false, maxMessages: 50 });
});

test("group history passes an explicit window through to the core config", () => {
  expect(toCoreConfig(parseConfig(configDocument({
    groupHistory: { enabled: true, maxMessages: 200 },
  }))).groupHistory).toEqual({ enabled: true, maxMessages: 200 });
  expect(toCoreConfig(parseConfig(configDocument({
    groupHistory: { enabled: true, maxMessages: 1 },
  }))).groupHistory).toEqual({ enabled: true, maxMessages: 1 });
});

test("group history rejects an out-of-range or non-integer message window", () => {
  for (const maxMessages of [0, 201, 1.5, "20", null]) {
    expect(() => parseConfig(configDocument({ groupHistory: { enabled: true, maxMessages } })))
      .toThrow(/maxMessages/);
  }
});

test("group history rejects a non-boolean enabled flag", () => {
  for (const enabled of ["true", 1, null]) {
    expect(() => parseConfig(configDocument({ groupHistory: { enabled } }))).toThrow(/enabled/);
  }
});

test("group history rejects unknown fields and non-object values", () => {
  expect(() => parseConfig(configDocument({ groupHistory: { enabled: true, limit: 5 } })))
    .toThrow(/groupHistory\.limit/);
  for (const groupHistory of [[], "on", null, 3]) {
    expect(() => parseConfig(configDocument({ groupHistory }))).toThrow(/groupHistory/);
  }
});

test("attachments default to enabled with a 25 MiB ceiling", () => {
  expect(toCoreConfig(parseConfig(configDocument())).attachments)
    .toEqual({ enabled: true, maxBytes: 26_214_400 });
  expect(toCoreConfig(parseConfig(configDocument({ attachments: {} }))).attachments)
    .toEqual({ enabled: true, maxBytes: 26_214_400 });
  // 只配上限不等于关掉：enabled 仍取默认值 true
  expect(toCoreConfig(parseConfig(configDocument({ attachments: { maxBytes: 1024 } }))).attachments)
    .toEqual({ enabled: true, maxBytes: 1024 });
});

test("attachments pass an explicit enabled flag and ceiling through", () => {
  expect(toCoreConfig(parseConfig(configDocument({
    attachments: { enabled: false, maxBytes: 1024 },
  }))).attachments).toEqual({ enabled: false, maxBytes: 1024 });
  expect(toCoreConfig(parseConfig(configDocument({
    attachments: { maxBytes: 1_073_741_824 },
  }))).attachments).toEqual({ enabled: true, maxBytes: 1_073_741_824 });
});

test("attachments reject an out-of-range or non-integer byte ceiling", () => {
  for (const maxBytes of [0, 512, 1_073_741_825, 1024.5, "1024", null]) {
    expect(() => parseConfig(configDocument({ attachments: { maxBytes } })))
      .toThrow(/maxBytes/);
  }
});

test("attachments reject a non-boolean enabled flag", () => {
  for (const enabled of ["true", 1, null]) {
    expect(() => parseConfig(configDocument({ attachments: { enabled } }))).toThrow(/enabled/);
  }
});

test("attachments reject unknown fields and non-object values", () => {
  expect(() => parseConfig(configDocument({ attachments: { enabled: true, foo: 1 } })))
    .toThrow(/attachments\.foo/);
  for (const attachments of [[], "on", null, 3]) {
    expect(() => parseConfig(configDocument({ attachments }))).toThrow(/attachments/);
  }
});

test("config.example.json stays parseable and documents the group history and attachment defaults", () => {
  const example = JSON.parse(
    readFileSync(new URL("../config.example.json", import.meta.url), "utf8"),
  ) as Record<string, unknown>;
  expect(example.groupHistory).toEqual({ enabled: true, maxMessages: 50 });
  expect(example.attachments).toEqual({ enabled: true, maxBytes: 26_214_400 });
  const parsed = toCoreConfig(parseConfig(example, { env: { OCTO_BOT_TOKEN: "example-token" } }));
  expect(parsed.groupHistory).toEqual({ enabled: true, maxMessages: 50 });
  expect(parsed.attachments).toEqual({ enabled: true, maxBytes: 26_214_400 });
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
