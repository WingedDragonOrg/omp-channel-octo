import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

type Frame = Record<string, unknown>;

function argument(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index < 0 ? undefined : process.argv[index + 1];
}

const sessionDir = resolve(argument("--session-dir") ?? process.cwd());
const sessionId = "command-catalog-fixture-session";
const sessionFile = join(sessionDir, `${sessionId}.jsonl`);

function send(frame: Frame): void {
  process.stdout.write(`${JSON.stringify(frame)}\n`);
}

function persistSession(): void {
  mkdirSync(sessionDir, { recursive: true });
  writeFileSync(sessionFile, `${JSON.stringify({
    type: "session",
    version: 3,
    id: sessionId,
    timestamp: new Date().toISOString(),
    cwd: process.cwd(),
  })}\n`, "utf8");
}

function response(id: string | undefined, command: string, data: Frame = {}): void {
  send({ type: "response", ...(id === undefined ? {} : { id }), command, success: true, data });
}

function handle(command: Frame): void {
  const id = typeof command.id === "string" ? command.id : undefined;
  const type = typeof command.type === "string" ? command.type : "";
  if (type === "negotiate_protocol") {
    response(id, type, { protocolVersion: 2 });
    return;
  }
  if (type === "set_subagent_subscription") {
    response(id, type, { level: "progress" });
    return;
  }
  if (type === "new_session") {
    persistSession();
    response(id, type, { cancelled: false });
    return;
  }
  if (type === "get_state") {
    persistSession();
    response(id, type, { sessionFile, sessionId, contextUsage: { tokens: 0, contextWindow: 8_192, percent: 0 } });
    return;
  }
  if (type === "get_available_commands") {
    response(id, type, {
      commands: [{ name: "project-file-command", source: "file", description: "Project command", input: { hint: "<topic>" } }],
    });
    setTimeout(() => {
      send({
        type: "available_commands_update",
        commands: [{ name: "updated-skill-command", source: "skill", description: "Updated command", input: { hint: "<query>" } }],
      });
    }, 20);
    return;
  }
  send({ type: "response", ...(id === undefined ? {} : { id }), command: type, success: false, error: { message: `Unexpected command ${type}` } });
}

send({ type: "ready", supportedProtocolVersions: [2] });
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string | Buffer) => {
  input += String(chunk);
  for (;;) {
    const newline = input.indexOf("\n");
    if (newline < 0) break;
    const line = input.slice(0, newline);
    input = input.slice(newline + 1);
    if (!line.trim()) continue;
    try {
      const value: unknown = JSON.parse(line);
      if (value && typeof value === "object" && !Array.isArray(value)) handle(value as Frame);
    } catch (error) {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    }
  }
});
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
