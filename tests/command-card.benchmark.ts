import assert from "node:assert/strict";
import { closeCoreHarness, createCoreHarness, FakeTransport, inbound } from "./fixtures/core-fakes.ts";

// Fixed UI-affordance workload, not a latency benchmark. Click correctness remains a
// separate regression gate; this score does not claim that configuration was saved.
const commands = ["/switch", "/mode"] as const;
const surfaces = [
  { channelType: 1 as const, channelId: "dm-owner" },
  { channelType: 2 as const, channelId: "group-allowed" },
  { channelType: 5 as const, channelId: "group-allowed____benchmark" },
];

function nodes(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) return value.flatMap(nodes);
  if (value === null || typeof value !== "object") return [];
  const node = value as Record<string, unknown>;
  return [node, ...Object.values(node).flatMap(nodes)];
}

function configurable(card: unknown): boolean {
  const all = nodes(card);
  const submits = all.filter(node => node.type === "Action.Submit");
  return submits.length >= 2 || (submits.length >= 1 && all.some(node =>
    node.type === "Input.ChoiceSet" && Array.isArray(node.choices) && node.choices.length >= 2));
}

let covered = 0;
let scenarios = 0;
let textFallbacks = 0;
for (const cardSupported of [true, false]) {
  for (const target of surfaces) {
    for (const command of commands) {
      const transport = new FakeTransport();
      transport.cardSupported = cardSupported;
      const harness = await createCoreHarness({ transport });
      try {
        await harness.service.handleInbound(inbound({
          ...target,
          messageId: `benchmark-${target.channelType}-${command.slice(1)}-${cardSupported}`,
          text: command,
          mentionedUserIds: [harness.bot.uid],
        }));
        // Graceful stop drains durable output before inspecting the recorded wire calls.
        await harness.service.stop();
        assert.equal(harness.workers.prompts.length, 0, "configuration must not invoke the model");
        const cards = transport.sentCards.filter(card =>
          card.target.channelType === target.channelType && card.target.channelId === target.channelId);
        const texts = transport.sent.filter(message =>
          message.target.channelType === target.channelType && message.target.channelId === target.channelId);
        assert.ok(cards.length > 0 || texts.length > 0, `${command} must produce a response`);
        if (cardSupported) {
          scenarios++;
          if (cards.some(card => configurable(card.frame.card))) covered++;
        } else {
          assert.equal(cards.length, 0, "unsupported deployments must use text");
          assert.ok(texts.length > 0, "text fallback is required");
          textFallbacks++;
        }
      } finally {
        await closeCoreHarness(harness);
      }
    }
  }
}
console.log(`METRIC command_card_coverage=${100 * covered / scenarios}`);
console.log(`METRIC configurable_surfaces=${covered}`);
console.log(`METRIC card_scenarios=${scenarios}`);
console.log(`METRIC text_fallbacks=${textFallbacks}`);
