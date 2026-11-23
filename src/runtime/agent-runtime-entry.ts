import { readFile } from "node:fs/promises";
import { startAgentRuntimeServer, type AgentRuntimeServerOptions } from "./agent-runtime.ts";

const configPath = process.argv[2];
if (!configPath) throw new Error("Agent runtime daemon config path is required");

const config = JSON.parse(await readFile(configPath, "utf8")) as AgentRuntimeServerOptions;
const server = await startAgentRuntimeServer(config);
let stopping = false;

const stop = (): void => {
  if (stopping) return;
  stopping = true;
  void server.shutdown().then(
    () => process.exit(0),
    error => {
      console.error("Agent runtime shutdown failed", error);
      process.exit(1);
    },
  );
};

process.once("SIGTERM", stop);
process.once("SIGINT", stop);
