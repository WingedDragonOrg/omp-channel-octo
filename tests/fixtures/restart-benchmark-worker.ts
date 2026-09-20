import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

// Offline OMP boundary: a durable checkpoint separates a committed effect from
// unfinished work. Replaying the original prompt repeats the effect; /retry
// continues the checkpoint. Session ownership is checked before any RPC frame.
type Frame = Record<string, unknown>;
const argv = process.argv.slice(2);
function flag(name: string): string | undefined {
  const index = argv.lastIndexOf(name);
  return index < 0 ? undefined : argv[index + 1];
}
const auditPath = flag("--audit");
if (!auditPath) throw new Error("--audit is required");
const sessionDir = flag("--session-dir");
if (!sessionDir) throw new Error("--session-dir is required");
const resumed = flag("--resume");
const sessionFile = resolve(resumed ?? join(sessionDir, "restart-benchmark.jsonl"));
const sessionId = "restart-benchmark-session";
const checkpoint = `${sessionFile}.checkpoint`;
const ownerFile = `${sessionFile}.owner`;
function audit(kind: string, details: Frame = {}): void {
  appendFileSync(auditPath!, `${JSON.stringify({ kind, pid: process.pid, sessionFile, ...details })}\n`);
}
function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}
if (existsSync(ownerFile)) {
  const previous = Number(readFileSync(ownerFile, "utf8"));
  if (!Number.isSafeInteger(previous) || previous <= 1) throw new Error("Invalid owner PID");
  if (alive(previous)) {
    audit("overlap", { previous });
    process.exit(73);
  }
}
writeFileSync(ownerFile, String(process.pid));
audit("startup", { resumed: resumed !== undefined });
if (resumed && !existsSync(sessionFile)) throw new Error("Missing resumed session");
function emit(frame: Frame): void { process.stdout.write(`${JSON.stringify(frame)}\n`); }
function reply(command: Frame, data?: unknown): void {
  emit({ type: "response", id: command.id, command: command.type, success: true, data });
}
function assistant(text: string): void {
  const message = { id: `benchmark-${text}`, role: "assistant", content: [{ type: "text", text }] };
  emit({ type: "message_start", message: { ...message, content: [] } });
  emit({ type: "message_update", message, assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text, partial: message } });
  emit({ type: "message_end", message });
}
function finish(text: string): void {
  assistant(text);
  emit({ type: "agent_end", messages: [], willContinue: false });
}
function handle(command: Frame): void {
  switch (command.type) {
    case "new_session":
      if (existsSync(sessionFile)) audit("session_reset");
      writeFileSync(sessionFile, `${JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: "2026-01-01T00:00:00.000Z", cwd: process.cwd() })}\n`);
      reply(command, { cancelled: false });
      return;
    case "get_state":
      reply(command, { sessionId, sessionFile, model: { provider: "fixture", id: "offline" }, isStreaming: false });
      return;
    case "set_subagent_subscription":
      reply(command, { level: command.level });
      return;
    case "set_host_tools":
    case "abort":
      reply(command);
      return;
    case "prompt": {
      const text = String(command.message ?? "");
      audit("prompt", { text });
      if (text === "/retry") {
        const pending = existsSync(checkpoint) && readFileSync(checkpoint, "utf8") === "pending";
        emit({ type: "command_output", text: pending ? "Retrying the last failed turn." : "Nothing to retry." });
        reply(command, { agentInvoked: pending });
        if (pending) {
          emit({ type: "agent_start" });
          writeFileSync(checkpoint, "completed");
          audit("effect", { name: "finish" });
          finish("BENCHMARK_RUNNING_DONE");
        }
        return;
      }
      if (!["benchmark:completed", "benchmark:running", "benchmark:queued"].includes(text)) {
        emit({ type: "response", id: command.id, command: "prompt", success: false, error: "Unsupported benchmark input" });
        return;
      }
      reply(command, { agentInvoked: true });
      emit({ type: "agent_start" });
      appendFileSync(sessionFile, `${JSON.stringify({ type: "message", message: { role: "user", content: text } })}\n`);
      if (text === "benchmark:running") {
        audit("effect", { name: "begin" });
        writeFileSync(checkpoint, "pending");
        assistant("BENCHMARK_CHECKPOINT");
        audit("checkpoint");
        // Remain in flight until the host stops us. A fresh process must resume.
      } else {
        const name = text === "benchmark:queued" ? "queued" : "completed";
        audit("effect", { name });
        finish(name === "queued" ? "BENCHMARK_QUEUED_DONE" : "BENCHMARK_COMPLETED_DONE");
      }
      return;
    }
    default:
      emit({ type: "response", id: command.id, command: command.type, success: false, error: "Unsupported benchmark RPC command" });
  }
}
let exiting = false;
function exitSoon(): void {
  if (exiting) return;
  exiting = true;
  audit("exit_requested");
  // Acknowledge abort immediately, but keep the process alive: an ACK is not exit.
  setTimeout(() => { audit("exit"); process.exit(0); }, 400);
}
process.on("SIGTERM", exitSoon);
process.stdin.on("end", exitSoon);
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  input += chunk;
  for (;;) {
    const end = input.indexOf("\n");
    if (end < 0) break;
    const line = input.slice(0, end);
    input = input.slice(end + 1);
    if (line.trim()) handle(JSON.parse(line) as Frame);
  }
});
process.stdin.resume();
emit({ type: "ready", protocolVersion: 1, supportedProtocolVersions: [1], maxFrameBytes: 1048576 });
