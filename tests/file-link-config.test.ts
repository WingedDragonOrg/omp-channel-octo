import { expect, test } from "bun:test";
import { parseConfig, toCoreConfig } from "../src/runtime/config.ts";

const document = {
  botToken: "fixture-bot-token",
  allowedGroupIds: [],
  ompCommand: "omp",
  stateDir: "/tmp/file-link-config",
};

test("Zed SSH environment config applies to the channel", () => {
  const core = toCoreConfig(parseConfig(document, { env: {
    OMP_CHANNEL_OCTO_ZED_MODE: "ssh",
    OMP_CHANNEL_OCTO_ZED_HOSTNAME: "my-mini",
    OMP_CHANNEL_OCTO_ZED_USERNAME: "dev",
    OMP_CHANNEL_OCTO_ZED_PORT: "2222",
  } }));
  expect(core.fileLinks).toEqual({ resolvers: [
    { type: "zed", mode: "ssh", hostname: "my-mini", username: "dev", port: 2222 },
  ] });
});

test("Zed local mode and disabled default are selected through the environment", () => {
  expect(parseConfig(document, { env: {} }).fileLinks).toBeUndefined();
  expect(parseConfig(document, { env: { OMP_CHANNEL_OCTO_ZED_MODE: "local" } }).fileLinks)
    .toEqual({ resolvers: [{ type: "zed", mode: "local" }] });
});

test("invalid Zed environment configuration is rejected", () => {
  expect(() => parseConfig(document, { env: { OMP_CHANNEL_OCTO_ZED_MODE: "ssh" } })).toThrow(/HOSTNAME/);
  expect(() => parseConfig(document, { env: { OMP_CHANNEL_OCTO_ZED_MODE: "remote" } })).toThrow(/MODE/);
  expect(() => parseConfig(document, { env: {
    OMP_CHANNEL_OCTO_ZED_MODE: "ssh", OMP_CHANNEL_OCTO_ZED_HOSTNAME: "my-mini",
    OMP_CHANNEL_OCTO_ZED_PORT: "bad",
  } })).toThrow(/PORT/);
});
