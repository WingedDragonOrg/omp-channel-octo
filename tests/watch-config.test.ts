import { expect, test } from "bun:test";
import { parseConfig, toCoreConfig } from "../src/runtime/config.ts";

function configDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    botToken: "fixture-bot-token",
    allowedGroupIds: [],
    ompCommand: "omp",
    stateDir: "/tmp/watch-config",
    ...overrides,
  };
}

const decisions = {
  enabled: true,
  protocol: "openai-decisions",
  baseUrl: "https://decision.example/proxy",
  model: "shared-fixture",
  apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
};
const environment = { OMP_CHANNEL_OCTO_DECISION_KEY: "shared-fixture-key" };

// A consumer enabling Decisions must be able to create a watch with the same resolved provider.
test("enabled Decisions enables watch and resolves one shared provider configuration", () => {
  for (const protocol of ["openai-decisions", "typesafe-systemone"] as const) {
    const parsed = toCoreConfig(parseConfig(configDocument({ decisions: { ...decisions, protocol } }), { env: environment }));
    expect(parsed.watch).toEqual({ enabled: true });
    expect(parsed.decisions).toEqual({
      enabled: true,
      protocol,
      baseUrl: "https://decision.example/proxy",
      model: "shared-fixture",
      apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
      apiKey: "shared-fixture-key",
    });
  }
});

test("watch can be disabled while reference Decisions stays enabled", () => {
  const parsed = toCoreConfig(parseConfig(configDocument({ decisions, watch: { enabled: false } }), { env: environment }));
  expect(parsed.decisions?.enabled).toBe(true);
  expect(parsed.watch).toEqual({ enabled: false });
});

test("an empty watch block follows enabled Decisions", () => {
  const parsed = parseConfig(configDocument({ decisions, watch: {} }), { env: environment });
  expect(parsed.watch).toEqual({ enabled: true });
});

test("watch performs no provider setup without enabled Decisions", () => {
  for (const configured of [undefined, { enabled: false }, {}]) {
    const parsed = toCoreConfig(parseConfig(configDocument({ decisions: configured }), { env: {} }));
    expect(parsed.watch).toEqual({ enabled: false });
  }
});

test("explicitly enabling watch requires enabled Decisions", () => {
  for (const configured of [undefined, { enabled: false }]) {
    expect(() => parseConfig(configDocument({ decisions: configured, watch: { enabled: true } }), { env: {} })).toThrow(/decisions/);
  }
});

test("watch configuration accepts only a boolean enabled switch", () => {
  for (const watch of [null, [], true, { enabled: "true" }, { enabled: true, baseUrl: "https://other.example" }, { enabled: true, model: "other" }, { enabled: true, apiKeyEnv: "OMP_CHANNEL_OCTO_OTHER_KEY" }, { enabled: true, protocol: "typesafe-systemone" }]) {
    expect(() => parseConfig(configDocument({ decisions, watch }), { env: environment })).toThrow(/watch/);
  }
});
