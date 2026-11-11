import { mock, test } from "bun:test";
import { createInterface } from "node:readline";

type HostOptions = {
  appPath: string;
  port: number;
  pid: number;
  startedAt: string;
  pluginDirectories: string[];
  zedExecutable: string;
  gatePath: string;
};

const config = JSON.parse(process.env.OCTO_HOST_GATE_OPTIONS ?? "null") as HostOptions | null;
if (!config) throw new Error("missing gated host fixture options");

const emit = (message: Record<string, unknown>) => process.stdout.write(`${JSON.stringify(message)}\n`);

test("runs a host process with a gated realpath", async () => {
  const originalFs = await import("node:fs/promises");
  const originalRealpath = originalFs.realpath as (...args: unknown[]) => Promise<unknown>;
  const realpathGate = Promise.withResolvers<void>();
  let gateEntered = false;
  mock.module("node:fs/promises", () => ({
    ...originalFs,
    realpath: async (...args: unknown[]) => {
      if (!gateEntered && String(args[0]) === config.gatePath) {
        gateEntered = true;
        emit({ type: "realpath-gate-entered", path: config.gatePath });
        await realpathGate.promise;
      }
      return await Reflect.apply(originalRealpath, undefined, args);
    },
  }));

  const { startHost } = await import("../../src/host/index.ts");
  const host = await startHost({
    appPath: config.appPath,
    port: config.port,
    pid: config.pid,
    startedAt: config.startedAt,
    pluginDirectories: config.pluginDirectories,
    zedExecutable: config.zedExecutable,
  });
  emit({ type: "ready" });

  const commands = createInterface({ input: process.stdin });
  const stopped = Promise.withResolvers<void>();
  commands.on("line", (line) => {
    if (line === "release") realpathGate.resolve();
    if (line === "stop") {
      realpathGate.resolve();
      void host.stop().then(() => {
        emit({ type: "stopped" });
        stopped.resolve();
      }, (error: unknown) => {
        emit({ type: "stop-error", message: error instanceof Error ? error.message : String(error) });
        stopped.reject(error);
      });
    }
  });
  await stopped.promise;
}, 30_000);
