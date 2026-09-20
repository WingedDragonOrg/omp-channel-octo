import { chmod, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "bun:test";
import { OCTO_CLI_ENV_ALLOWLIST, createOctoCliCredentialProvisioner } from "../src/runtime/octo-cli.ts";
import { octoCliConfigDir } from "../src/runtime/config.ts";

/**
 * The provisioner exists so OMP tasks can run `octo-cli` as this channel's bot without the raw
 * token entering the worker environment. These tests drive it against a stub CLI: they assert the
 * exact login invocation, where the credential lands, and that every failure leaves the agent
 * without a CLI identity instead of breaking startup.
 */

type StubMode = "ok" | "fail" | "bad-json" | "wrong-bot";

type StubCall = {
  argv: string[];
  stdin: string;
  env: Record<string, string>;
};

const roots = new Set<string>();

afterEach(async () => {
  for (const root of [...roots]) {
    await rm(root, { recursive: true, force: true });
    roots.delete(root);
  }
});

function stubSource(mode: StubMode, robotId: string | undefined): string {
  return `#!/usr/bin/env bun
import { readFileSync, writeFileSync } from "node:fs";

const stdin = readFileSync(0, "utf8");
const argv = process.argv.slice(2);
writeFileSync(process.env.STUB_CALL_PATH, JSON.stringify({
  argv,
  stdin,
  env: Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("OCTO"))),
}));
const mode = ${JSON.stringify(mode)};
if (mode === "fail") {
  console.error("登录失败：token 无效");
  process.exit(3);
}
if (mode === "bad-json") {
  console.log("not json at all");
  process.exit(0);
}
// Unless a test forces another identity, the stub answers as the bot it was asked to store.
const robotId = ${JSON.stringify(robotId ?? null)} ?? argv[argv.indexOf("--bot-id") + 1] ?? "";
console.log(JSON.stringify({
  ok: true,
  data: { profile: "oco", robot_id: robotId, bot_kind: "user_bot" },
}));
`;
}

async function createStub(options: { mode?: StubMode; robotId?: string } = {}) {
  const root = await mkdtemp(join(tmpdir(), "octo-cli-stub-"));
  roots.add(root);
  const home = join(root, "home");
  const binDir = join(root, "bin");
  await mkdir(binDir, { recursive: true });
  await mkdir(home, { recursive: true });
  const command = join(binDir, "octo-cli");
  await writeFile(command, stubSource(options.mode ?? "ok", options.robotId));
  await chmod(command, 0o755);
  const callPath = join(root, "call.json");
  return { root, home, command, callPath };
}

async function readCall(path: string): Promise<StubCall> {
  return JSON.parse(await readFile(path, "utf8")) as StubCall;
}

const BOT_UID = "probe_bot_0123456789abcdef";
const TOKEN = "bf_test_token_value";
const API_URL = "https://im.deepminer.com.cn";

test("a stored profile lands in the bot's own store directory and receives the token on stdin", async () => {
  const stub = await createStub();
  const lines: string[] = [];
  // The host environment carries the raw token; the login child must not inherit it.
  const previous = Bun.env["OCTO_BOT_TOKEN"];
  Bun.env["OCTO_BOT_TOKEN"] = TOKEN;
  Bun.env["STUB_CALL_PATH"] = stub.callPath;
  try {
    const provisioner = createOctoCliCredentialProvisioner({
      botToken: TOKEN,
      apiUrl: API_URL,
      homeDir: stub.home,
      command: stub.command,
      logger: (message) => lines.push(message),
    });
    await provisioner.provision(BOT_UID);

    const storeDir = octoCliConfigDir(BOT_UID, stub.home);
    expect(provisioner.env()).toEqual({
      OCTO_CONFIG_DIR: storeDir,
      OCTO_API_BASE_URL: API_URL,
      OCTO_BOT_ID: BOT_UID,
    });
    for (const name of Object.keys(provisioner.env() ?? {})) {
      expect(OCTO_CLI_ENV_ALLOWLIST[name]).toBe(true);
    }
    expect((await stat(storeDir)).mode & 0o777).toBe(0o700);

    const call = await readCall(stub.callPath);
    expect(call.argv).toEqual([
      "auth",
      "login",
      "--profile",
      "oco",
      "--bot-id",
      BOT_UID,
      "--with-token",
    ]);
    expect(call.argv.join(" ")).not.toContain(TOKEN);
    expect(call.stdin).toBe(TOKEN);
    expect(call.env["OCTO_CONFIG_DIR"]).toBe(storeDir);
    expect(call.env["OCTO_API_BASE_URL"]).toBe(API_URL);
    expect(call.env["OCTO_BOT_TOKEN"]).toBeUndefined();
    expect(lines.join("\n")).toContain("凭据已就绪");
    expect(lines.join("\n")).not.toContain(TOKEN);
  } finally {
    if (previous === undefined) delete Bun.env["OCTO_BOT_TOKEN"];
    else Bun.env["OCTO_BOT_TOKEN"] = previous;
    delete Bun.env["STUB_CALL_PATH"];
  }
});

test("an unusable CLI leaves the agent without a CLI identity instead of failing startup", async () => {
  for (const mode of ["fail", "bad-json"] as const) {
    const stub = await createStub({ mode, robotId: BOT_UID });
    const lines: string[] = [];
    Bun.env["STUB_CALL_PATH"] = stub.callPath;
    try {
      const provisioner = createOctoCliCredentialProvisioner({
        botToken: TOKEN,
        apiUrl: API_URL,
        homeDir: stub.home,
        command: stub.command,
        logger: (message) => lines.push(message),
      });
      await provisioner.provision(BOT_UID);
      expect(provisioner.env()).toBeUndefined();
      expect(lines.length).toBe(1);
      expect(lines[0]).toContain("不可用");
      expect(lines[0]).not.toContain(TOKEN);
    } finally {
      delete Bun.env["STUB_CALL_PATH"];
    }
  }
});

test("a credential belonging to another bot is rejected", async () => {
  const stub = await createStub({ robotId: "someone-else" });
  const lines: string[] = [];
  Bun.env["STUB_CALL_PATH"] = stub.callPath;
  try {
    const provisioner = createOctoCliCredentialProvisioner({
      botToken: TOKEN,
      apiUrl: API_URL,
      homeDir: stub.home,
      command: stub.command,
      logger: (message) => lines.push(message),
    });
    await provisioner.provision(BOT_UID);
    expect(provisioner.env()).toBeUndefined();
    expect(lines[0]).toContain("归属");
  } finally {
    delete Bun.env["STUB_CALL_PATH"];
  }
});

test("a missing octo-cli executable is reported once and never throws", async () => {
  const stub = await createStub();
  const lines: string[] = [];
  const provisioner = createOctoCliCredentialProvisioner({
    botToken: TOKEN,
    apiUrl: API_URL,
    homeDir: stub.home,
    command: join(stub.root, "bin", "octo-cli-absent"),
    logger: (message) => lines.push(message),
  });
  await provisioner.provision(BOT_UID);
  expect(provisioner.env()).toBeUndefined();
  expect(lines.length).toBe(1);
  expect(lines[0]).toContain("无法执行");
});

test("provisioning a second bot switches the worker to that bot's store", async () => {
  const stub = await createStub();
  Bun.env["STUB_CALL_PATH"] = stub.callPath;
  try {
    const provisioner = createOctoCliCredentialProvisioner({
      botToken: TOKEN,
      apiUrl: API_URL,
      homeDir: stub.home,
      command: stub.command,
      logger: () => {},
    });
    await provisioner.provision(BOT_UID);
    expect(provisioner.env()?.["OCTO_BOT_ID"]).toBe(BOT_UID);
    await provisioner.provision("second-bot");
    expect(provisioner.env()?.["OCTO_BOT_ID"]).toBe("second-bot");
    expect(provisioner.env()?.["OCTO_CONFIG_DIR"]).toBe(octoCliConfigDir("second-bot", stub.home));
  } finally {
    delete Bun.env["STUB_CALL_PATH"];
  }
});
