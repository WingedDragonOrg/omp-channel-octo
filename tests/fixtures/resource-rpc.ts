import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// Local v1 RPC peer. Both a live host-tool return and a resumed answer produce
// observable output; no model, network, user profile, or background work.
const args = process.argv.slice(2);
const flag = (key: string) => args[args.indexOf(key) + 1];
const sessionFile = args.includes("--resume") ? flag("--resume")! : join(flag("--session-dir")!, "resource.jsonl");
const sessionId = "resource-session";
const emit = (frame: object) => process.stdout.write(JSON.stringify(frame) + "\n");
let asking = false;
function finish(text: string) {
  const message = { id: "resource-result", role: "assistant", content: [{ type: "text", text }] };
  emit({ type: "message_start", message: { ...message, content: [] } });
  emit({ type: "message_end", message });
  emit({ type: "agent_end", messages: [message], willContinue: false });
}
function handle(c: Record<string, any>) {
  const response = (data: unknown = {}) => emit({ type: "response", command: c.type, id: c.id, success: true, data });
  switch (c.type) {
    case "new_session":
      writeFileSync(sessionFile, JSON.stringify({ type: "session", version: 3, id: sessionId, cwd: process.cwd(), timestamp: "2026-01-01T00:00:00.000Z" }) + "\n");
      response({ cancelled: false }); break;
    case "get_state":
      if (!existsSync(sessionFile)) throw new Error("missing durable session");
      if (!readFileSync(sessionFile, "utf8").includes(sessionId)) throw new Error("wrong session");
      response({ sessionId, sessionFile }); break;
    case "set_subagent_subscription": response({ level: "progress" }); break;
    case "set_host_tools":
      if (!c.tools.some((t: { name: string }) => t.name === "ask")) throw new Error("ask not registered");
      response({ toolNames: c.tools.map((t: { name: string }) => t.name) }); break;
    case "prompt":
      response({ agentInvoked: true });
      emit({ type: "agent_start" });
      if (c.message.includes("benchmark-answer")) { finish(c.message); break; }
      if (existsSync(sessionFile + ".asked")) throw new Error("initial task replayed");
      writeFileSync(sessionFile + ".asked", "asked");
      asking = true;
      emit({ type: "host_tool_call", id: "resource-ask", toolCallId: "resource-tool", toolName: "ask", arguments: { questions: [{ id: "resource", question: "Provide benchmark answer" }] } });
      break;
    case "host_tool_result":
      if (asking) { asking = false; if (c.isError !== true) finish(JSON.stringify(c.result)); }
      break;
    case "abort":
      asking = false; response(); emit({ type: "agent_end", messages: [], willContinue: false }); break;
    default: throw new Error(`Unexpected RPC command: ${c.type}`);
  }
}
emit({ type: "ready", protocolVersion: 1 });
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
  input += chunk;
  let end: number;
  while ((end = input.indexOf("\n")) >= 0) {
    const line = input.slice(0, end); input = input.slice(end + 1);
    if (line.trim()) handle(JSON.parse(line));
  }
});
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
