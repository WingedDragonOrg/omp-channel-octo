import { expect, test } from "bun:test";
import { buildContextCard } from "../src/cards/context-card.ts";

test("context card emphasizes native usage while preserving every breakdown and unknown line", () => {
  const frame = buildContextCard([
    "Context window: 500000 tokens (2% used)",
    "System prompt [░░] 1% 2861 tokens",
    "Free [█░] 83% 414228 tokens",
    "Additional context details: unavailable",
  ].join("\n"));
  const rendered = JSON.stringify(frame.card);
  expect(rendered).toContain("2% 已用");
  expect(rendered).toContain("500000");
  expect(rendered).toContain("System prompt");
  expect(rendered).toContain("2861");
  expect(rendered).toContain("83%");
  expect(rendered).toContain("414228");
  expect(rendered).toContain("Additional context details: unavailable");
  expect(rendered).not.toMatch(/[░█\x1b]/u);
});
