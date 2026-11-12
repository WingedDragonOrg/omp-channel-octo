import { spawn, type ChildProcess } from "node:child_process";
import type { OctoBuddyApp } from "./paths.ts";
import { withoutStatusEnvironment } from "./status-environment.ts";

export interface StartedOctoBuddy {
  child: ChildProcess;
  pid: number;
  port: Promise<number>;
}

export function startOctoBuddy(app: OctoBuddyApp, timeoutMs = 25_000): StartedOctoBuddy {
  const env = withoutStatusEnvironment(process.env);
  const child = spawn(app.executablePath, [
    "--remote-debugging-address=127.0.0.1",
    "--remote-debugging-port=0",
  ], {
    stdio: ["ignore", "ignore", "pipe"],
    env,
  });

  if (!child.pid || !child.stderr) {
    child.on("error", () => {});
    throw new Error("OctoBuddy could not be started with a monitorable process");
  }

  let stderr = "";
  let settled = false;
  let timeout: ReturnType<typeof setTimeout>;
  const port = new Promise<number>((resolve, reject) => {
    const finish = (error?: Error, value?: number) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (error) reject(error);
      else resolve(value!);
    };
    const onData = (chunk: Buffer | string) => {
      if (settled) return;
      stderr = `${stderr}${chunk.toString()}`.slice(-32_768);
      const match = stderr.match(/DevTools listening on ws:\/\/127\.0\.0\.1:(\d+)\//);
      if (!match) return;
      const value = Number(match[1]);
      if (!Number.isInteger(value) || value < 1 || value > 65_535) {
        finish(new Error(`OctoBuddy reported an invalid CDP port: ${match[1]}`));
        return;
      }
      finish(undefined, value);
    };
    const onError = (error: Error) => finish(new Error(`OctoBuddy could not start: ${error.message}`, { cause: error }));
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      finish(new Error(`OctoBuddy exited before its CDP endpoint became ready (code=${code}, signal=${signal})`));
    };
    timeout = setTimeout(() => {
      finish(new Error(`Timed out waiting for OctoBuddy to report its CDP port${stderr ? `: ${stderr.trim()}` : ""}`));
    }, timeoutMs);
    child.stderr!.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });

  return { child, pid: child.pid, port };
}
