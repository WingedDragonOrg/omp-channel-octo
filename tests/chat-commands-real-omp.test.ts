import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { createWorkerFactory } from "../src/omp/index.ts";
import type { ChatTarget } from "../src/contracts.ts";
import { createCommandCatalogHarness } from "./command-catalog-test-seam";
import { startStatusServer } from "../src/status/server.ts";

const ompCommand = process.env.OMP_REAL_COMMAND ?? Bun.which("omp");
const realOmpSuite = ompCommand === null ? describe.skip : describe;
const TOKEN = "real-omp-command-catalog-test-token-0123456789";


realOmpSuite("installed OMP command catalog", () => {
  test("HTTP discovers each authorized chat's project commands without changing its session", async () => {
    const projectRoot = await mkdtemp(join(tmpdir(), "octobuddy-command-projects-"));
    const projectA = join(projectRoot, "project-a");
    const projectB = join(projectRoot, "project-b");
    await mkdir(join(projectA, ".omp", "commands"), { recursive: true });
    await mkdir(join(projectB, ".omp", "commands"), { recursive: true });
    await writeFile(join(projectA, ".omp", "commands", "project-a-suggestion.md"), "# Project A suggestion\n\nUse this command in project A.\n");
    await writeFile(join(projectB, ".omp", "commands", "project-b-suggestion.md"), "# Project B suggestion\n\nUse this command in project B.\n");

    const workerFactory = createWorkerFactory({
      ompCommand: ompCommand!,
      startupTimeoutMs: 30_000,
      cancelTimeoutMs: 3_000,
    });
    const harness = await createCommandCatalogHarness({
      allowedGroupIds: ["project-chat-a", "project-chat-b"],
      workerFactory,
    });
    const targets: Array<{ target: ChatTarget; cwd: string; sessionId: string }> = [
      { target: { channelType: 2, channelId: "project-chat-a" }, cwd: projectA, sessionId: "bound-session-a" },
      { target: { channelType: 2, channelId: "project-chat-b" }, cwd: projectB, sessionId: "bound-session-b" },
    ];
    for (const item of targets) {
      harness.store.ensureBinding({
        botUid: harness.bot.uid,
        ...item.target,
        sessionId: item.sessionId,
        sessionFile: null,
        cwd: item.cwd,
        cwdPinned: true,
      });
    }

    const server = await startStatusServer({
      listenAddress: "127.0.0.1",
      port: 0,
      token: TOKEN,
      statusSnapshot: () => harness.service.statusSnapshot(),
      chatStatusSnapshot: target => harness.service.chatStatusSnapshot(target),
      chatDetailsSnapshot: (target, signal) => harness.service.chatDetailsSnapshot(target, signal),
      chatCommandsSnapshot: (target, signal) => harness.service.chatCommandsSnapshot(target, signal),
    });
    try {
      const before = targets.map(({ target }) => harness.store.getBinding(harness.bot.uid, target));
      const baseUrl = `http://127.0.0.1:${server.port}`;
      expect((await fetch(`${baseUrl}/v1/chats/commands?channelType=2&channelId=project-chat-a`)).status).toBe(401);

      const responses: unknown[] = [];
      for (const { target } of targets) {
        const query = new URLSearchParams({
          channelType: String(target.channelType),
          channelId: target.channelId,
        });
        const response = await fetch(`${baseUrl}/v1/chats/commands?${query}`, {
          headers: { authorization: `Bearer ${TOKEN}` },
        });
        expect(response.status).toBe(200);
        responses.push(await response.json());
      }

      const [commandsA, commandsB] = responses as Array<{
        channelId: string;
        commands: Array<{ name: string; source: "channel" | "omp" }>;
        omp: "available" | "unavailable";
      }>;
      expect(commandsA?.channelId).toBe("project-chat-a");
      expect(commandsA?.omp).toBe("available");
      expect(commandsA?.commands.some(command => command.name === "project-a-suggestion" && command.source === "omp")).toBe(true);
      expect(commandsA?.commands.some(command => command.name === "project-b-suggestion")).toBe(false);
      expect(commandsB?.channelId).toBe("project-chat-b");
      expect(commandsB?.omp).toBe("available");
      expect(commandsB?.commands.some(command => command.name === "project-b-suggestion" && command.source === "omp")).toBe(true);
      expect(commandsB?.commands.some(command => command.name === "project-a-suggestion")).toBe(false);
      expect(targets.map(({ target }) => harness.store.getBinding(harness.bot.uid, target))).toEqual(before);
      expect(targets.map(({ target }) => harness.store.getLatestTask(harness.bot.uid, target))).toEqual([null, null]);

      expect(harness.store.listWorkerLedger(harness.bot.uid)).toEqual([]);
    } finally {
      await server.stop();
      await harness.close();
      await rm(projectRoot, { recursive: true, force: true });
    }
  }, 90_000);
});
