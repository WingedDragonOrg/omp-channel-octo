import { spawn, type ChildProcess } from "node:child_process";
import { processStartIdentity } from "./identity.ts";

export interface ManagedService {
  child: ChildProcess;
  startedAt: Promise<string>;
  ready: Promise<{ ok: true } | { ok: false; error: Error }>;
}

export interface ServiceExit {
  code: number | null;
  signal: NodeJS.Signals | null;
}

function waitForReady(child: ChildProcess, name: string, timeoutMs: number): Promise<void> {
  const output = child.stdout;
  if (!output) return Promise.reject(new Error(`${name} has no protocol output stream`));
  return new Promise<void>((resolve, reject) => {
    let buffer = "";
    let settled = false;
    let timeout: ReturnType<typeof setTimeout>;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (error) reject(error);
      else resolve();
    };
    const onData = (chunk: Buffer | string) => {
      if (settled) return;
      buffer += chunk.toString();
      if (buffer.length > 64 * 1024) {
        finish(new Error(`${name} protocol line exceeded 64 KiB`));
        return;
      }
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) return;
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (!line) continue;
        let message: unknown;
        try {
          message = JSON.parse(line);
        } catch (error) {
          finish(new Error(`${name} emitted invalid JSON before readiness`, { cause: error }));
          return;
        }
        if (!message || typeof message !== "object") {
          finish(new Error(`${name} emitted an invalid readiness message`));
          return;
        }
        const response = message as { type?: unknown; message?: unknown };
        if (response.type === "ready") {
          finish();
          return;
        }
        if (response.type === "error") {
          finish(new Error(typeof response.message === "string" && response.message ? response.message : `${name} failed`));
          return;
        }
        finish(new Error(`${name} emitted an unsupported readiness message`));
        return;
      }
    };
    const onError = (error: Error) => finish(new Error(`${name} could not start: ${error.message}`, { cause: error }));
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      finish(new Error(`${name} exited before readiness (code=${code}, signal=${signal})`));
    };
    timeout = setTimeout(() => finish(new Error(`Timed out waiting for ${name} readiness`)), timeoutMs);
    output.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

export function startManagedService(
  scriptPath: string,
  args: string[],
  name: string,
  options: { detached: boolean; timeoutMs: number },
): ManagedService {
  const child = spawn(process.execPath, [scriptPath, ...args], {
    detached: options.detached,
    env: process.env,
    stdio: ["ignore", "pipe", "ignore"],
  });
  const ready = waitForReady(child, name, options.timeoutMs).then(
    () => ({ ok: true as const }),
    (error: unknown) => ({ ok: false as const, error: error instanceof Error ? error : new Error(String(error)) }),
  );
  const startedAt = child.pid
    ? processStartIdentity(child.pid)
    : Promise.reject(new Error(`${name} process did not receive a PID`));
  return { child, startedAt, ready };
}

export function waitForProcessExit(child: ChildProcess): Promise<ServiceExit> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode });
  }
  return new Promise<ServiceExit>((resolve) => {
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
}
