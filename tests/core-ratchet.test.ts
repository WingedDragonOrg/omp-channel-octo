import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "bun:test";
import type { OmpWorker, WorkerOptions } from "../src/contracts.ts";
import { contextSidecarPath, writePreparedReceipt } from "../src/session/context-sidecar.ts";
import { createWorkerFactory } from "../src/omp/index.ts";
import { FakeTransport, FakeWorkerFactory, cardActionEvent, closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes.ts";

test("ratchet native approval reaches members and their selection returns to OMP", async () => {
  const root = await mkdtemp(join(tmpdir(), "ratchet-core-"));
  const logPath = join(root, "rpc.log");
  const factory = createWorkerFactory({
    ompCommand: process.execPath,
    extraArgs: [resolve(import.meta.dir, "fixtures/omp-rpc-worker.ts"), "--fixture-scenario", "ratchet-ask", "--fixture-log", logPath],
    startupTimeoutMs: 5_000,
  });
  class RpcWorkers extends FakeWorkerFactory {
    override async create(options: WorkerOptions): Promise<OmpWorker> {
      const worker = await factory.create(options);
      const prompt = worker.prompt.bind(worker);
      worker.prompt = async (text, onEvent, context) => {
        const injection = context?.contextInjection;
        if (injection && injection.injectionDecision !== "unchanged" && options.contextScope) {
          const session = await worker.refreshState();
          await writePreparedReceipt(contextSidecarPath(options.contextScope.sidecarRoot, session.sessionId), {
            botUid: options.contextScope.botUid,
            target: options.contextScope.target,
            sessionId: session.sessionId,
            version: injection.snapshot.version,
            attemptId: injection.attemptId,
          });
        }
        return prompt(text, onEvent, context);
      };
      return worker;
    }
  }
  const transport = new FakeTransport();
  transport.cardSupported = true;
  const harness = await createCoreHarness({ root, workers: new RpcWorkers(), transport });
  const target = { channelType: 2 as const, channelId: "group-allowed" };
  try {
    await harness.service.submitInbound(inbound({ ...target, messageId: "ratchet-start", text: "/ratchet flow", mentionedUserIds: [harness.bot.uid] }));
    const first = await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 1);
    const card = transport.sentCards[0]!;
    expect(JSON.stringify(card.frame.card)).toContain("Current input preview");
    transport.pushBotEvent(cardActionEvent(1, { messageId: card.messageId, ...target, actionId: "ask_opt_1", operatorUid: "member-9", inputs: {} }));
    await transport.waitForAck(1);
    await waitForStatus(harness.service, target, status => status.pendingQuestionIds.length === 1 && status.pendingQuestionIds[0] !== first.pendingQuestionIds[0]);
    await harness.service.submitInbound(inbound({ ...target, messageId: "ratchet-revision", senderId: "member-9", text: "change the inputs", mentionedUserIds: [harness.bot.uid] }));
    await waitForStatus(harness.service, target, status => status.state === "completed" && status.pendingQuestionIds.length === 0);
    const log = (await readFile(logPath, "utf8")).trim().split("\n").map(line => JSON.parse(line));
    const replies = log.filter(entry => entry.kind === "command" && entry.command?.type === "extension_ui_response").map(entry => entry.command);
    expect(replies[0]).toEqual({ type: "extension_ui_response", id: "fixture-ratchet-ui-1", answers: [{ id: "ratchet-input", selectedOptions: ["Approve"] }] });
    expect(replies[1]).toEqual({ type: "extension_ui_response", id: "fixture-ratchet-ui-2", value: "change the inputs" });
  } finally {
    await closeCoreHarness(harness);
  }
});
