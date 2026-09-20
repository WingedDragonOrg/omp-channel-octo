import { expect, test } from "bun:test";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  createSupervisorClient,
  parseSuperExport,
  parseSuperInfo,
  type SupervisorClient,
  type SupervisorRun,
  type SupervisorRunResult,
} from "../src/runtime/supervisor.ts";

const CHECKOUT = "/repo/omp-channel-octo";

function exportDocument(services: readonly unknown[]): string {
  return JSON.stringify({ prune: false, services });
}

const channelProgram = {
  name: "omp-channel-octo",
  command: "/bin/sh",
  args: ["-c", ". ./.envrc; exec bun run src/cli.ts start --config /tmp/config.json"],
  cwd: CHECKOUT,
};

const unrelatedProgram = { name: "web", command: "/usr/bin/node", args: ["server.js"], cwd: "/srv/web" };

function supervisorRun(
  handler: (argv: readonly string[]) => SupervisorRunResult,
): { run: SupervisorRun; calls: string[][] } {
  const calls: string[][] = [];
  return {
    calls,
    run: async (argv) => {
      calls.push([...argv]);
      return handler(argv);
    },
  };
}

function clientWith(handler: (argv: readonly string[]) => SupervisorRunResult): SupervisorClient {
  return createSupervisorClient({ command: "super", run: supervisorRun(handler).run });
}

test("super export parsing keeps named programs and tolerates unrelated entries", () => {
  const programs = parseSuperExport(JSON.parse(exportDocument([
    channelProgram,
    unrelatedProgram,
    { name: "  " },
    { name: "sparse" },
    "not-an-object",
  ])));

  expect(programs).toEqual([
    { name: "omp-channel-octo", command: "/bin/sh", args: channelProgram.args, cwd: CHECKOUT },
    { name: "web", command: "/usr/bin/node", args: ["server.js"], cwd: "/srv/web" },
    { name: "sparse", command: "", args: [], cwd: undefined },
  ]);
  expect(() => parseSuperExport({ services: "nope" })).toThrow(/services/);
  expect(() => parseSuperExport([])).toThrow(/JSON 对象/);
});

test("super info parsing reads the live state and refuses unparsable output", () => {
  expect(parseSuperInfo("State:     Healthy\nPID:       72433\n")).toEqual({ state: "Healthy", pid: 72433 });
  expect(parseSuperInfo("State: Stopped\n")).toEqual({ state: "Stopped", pid: undefined });
  expect(parseSuperInfo("nothing useful")).toBeUndefined();
});

test("checkout detection matches the program that runs this CLI from this directory", async () => {
  const client = clientWith(() => ({ code: 0, stdout: exportDocument([channelProgram, unrelatedProgram]), stderr: "" }));
  expect(await client.detectCheckout(CHECKOUT)).toEqual([{ kind: "super", program: "omp-channel-octo" }]);
  expect(await client.detectCheckout("/repo/other-checkout")).toEqual([]);
});

test("checkout detection ignores a program that shares the directory but is not this channel", async () => {
  const client = clientWith(() => ({
    code: 0,
    stdout: exportDocument([
      { name: "unrelated", command: "/usr/bin/tail", args: ["-f", "/tmp/log"], cwd: CHECKOUT },
      { name: "linked", command: "/Users/test/.bun/bin/oco", args: ["start"], cwd: CHECKOUT },
    ]),
    stderr: "",
  }));
  expect(await client.detectCheckout(CHECKOUT)).toEqual([{ kind: "super", program: "linked" }]);
});

test("checkout detection reports every candidate instead of picking one", async () => {
  const client = clientWith(() => ({
    code: 0,
    stdout: exportDocument([
      channelProgram,
      { ...channelProgram, name: "omp-channel-octo-staging" },
    ]),
    stderr: "",
  }));
  const matches = await client.detectCheckout(CHECKOUT);
  expect(matches.map((entry) => entry.program)).toEqual(["omp-channel-octo", "omp-channel-octo-staging"]);
});

test("self detection trusts the live PID, not the shape of the program", async () => {
  const client = clientWith((argv) => {
    if (argv[0] === "export") return { code: 0, stdout: exportDocument([channelProgram, unrelatedProgram]), stderr: "" };
    return { code: 0, stdout: `State: Healthy\nPID: ${argv[1] === "omp-channel-octo" ? 4242 : 7}\n`, stderr: "" };
  });
  expect(await client.detectSelf(4242)).toEqual({ kind: "super", program: "omp-channel-octo" });
  expect(await client.detectSelf(9999)).toBeUndefined();
});

test("restart argv waits for health for the CLI and stays detached for the channel", () => {
  const client = clientWith(() => ({ code: 0, stdout: "{}", stderr: "" }));
  const target = { kind: "super" as const, program: "omp-channel-octo" };
  expect(client.restartArgv(target, 45)).toEqual([
    "super",
    "-y",
    "restart",
    "--wait-healthy",
    "--timeout",
    "45",
    "omp-channel-octo",
  ]);
  expect(client.detachedRestartArgv(target)).toEqual(["super", "-y", "restart", "omp-channel-octo"]);
});

test("super failures surface the client's own message instead of an empty guess", async () => {
  const failing = clientWith(() => ({ code: 1, stdout: "", stderr: "error: no such program\n" }));
  await expect(failing.listPrograms()).rejects.toThrow(/no such program/);

  const garbage = clientWith(() => ({ code: 0, stdout: "not json", stderr: "" }));
  await expect(garbage.listPrograms()).rejects.toThrow(/合法 JSON/);

  const missingInfo = clientWith(() => ({ code: 0, stdout: "unexpected", stderr: "" }));
  await expect(missingInfo.info("omp-channel-octo")).rejects.toThrow(/无法解析/);
});

test("a hung super client fails on the deadline instead of blocking the CLI", async () => {
  const root = await mkdtemp(join(tmpdir(), "oco-super-timeout-"));
  try {
    const script = join(root, "super");
    await writeFile(script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n", "utf8");
    await chmod(script, 0o755);
    const client = createSupervisorClient({ command: script, timeoutMs: 150 });
    await expect(client.listPrograms()).rejects.toThrow(/超时/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
