import { expect, test } from "bun:test";
import { parseConfig, toCoreConfig } from "../src/runtime/config.ts";

function configDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    botToken: "fixture-bot-token",
    allowedGroupIds: [],
    ompCommand: "omp",
    stateDir: "/tmp/reference-decisions-config",
    ...overrides,
  };
}


test("enabled decisions resolve only the explicitly named OMP_CHANNEL_OCTO_ key", () => {
  const config = parseConfig(configDocument({
    decisions: {
      enabled: true,
      protocol: "openai-decisions",
      baseUrl: "https://decision.example/proxy",
      model: "fixture-model",
      apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
    },
  }), {
    env: {
      OMP_CHANNEL_OCTO_DECISION_KEY: "explicit-fixture-key",
      OPENAI_API_KEY: "must-not-be-used",
      TYPESAFE_API_KEY: "must-not-be-used-either",
    },
  });

  expect(toCoreConfig(config).decisions).toEqual({
    enabled: true,
    protocol: "openai-decisions",
    baseUrl: "https://decision.example/proxy",
    model: "fixture-model",
    apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
    apiKey: "explicit-fixture-key",
  });
});

test("enabled decisions reject missing explicit keys and malformed operator settings", () => {
  const enabled = {
    enabled: true,
    protocol: "openai-decisions",
    baseUrl: "https://decision.example/proxy",
    model: "fixture-model",
    apiKeyEnv: "OMP_CHANNEL_OCTO_DECISION_KEY",
  };

  expect(() => parseConfig(configDocument({ decisions: enabled }), {
    env: { OPENAI_API_KEY: "must-not-be-used" },
  })).toThrow(/OMP_CHANNEL_OCTO_DECISION_KEY/);

  const invalid = [
    { ...enabled, enabled: "true" },
    { ...enabled, protocol: "openrouter-decisions" },
    { ...enabled, protocol: undefined },
    { ...enabled, baseUrl: "http://decision.example" },
    { ...enabled, baseUrl: "https://user:secret@decision.example" },
    { ...enabled, baseUrl: "https://@decision.example" },
    { ...enabled, baseUrl: "https://decision.example/proxy?tenant=private" },
    { ...enabled, baseUrl: "https://decision.example/proxy#fragment" },
    { ...enabled, baseUrl: "https://decision.example/v1/decisions" },
    { ...enabled, baseUrl: "https://decision.example/v1/systemone/" },
    { ...enabled, model: "  " },
    { ...enabled, apiKeyEnv: "OPENAI_API_KEY" },
    { ...enabled, apiKeyEnv: "OMP_CHANNEL_OCTO_" },
    { ...enabled, unsupported: true },
    { ...enabled, apiKey: "raw-config-key-is-forbidden" },
    { ...enabled, baseUrl: undefined },
    { ...enabled, model: undefined },
    { ...enabled, apiKeyEnv: undefined },
  ];

  for (const decisions of invalid) {
    expect(() => parseConfig(configDocument({ decisions }), {
      env: { OMP_CHANNEL_OCTO_DECISION_KEY: "explicit-fixture-key" },
    })).toThrow(/decisions/);
  }
});
