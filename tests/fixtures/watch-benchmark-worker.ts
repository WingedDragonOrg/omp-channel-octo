import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

// Real stdio child for the offline watch benchmark. No model or external platform is contacted.
type Frame = Record<string, unknown>;
const argv = process.argv.slice(2);
function flag(name: string): string { return argv[argv.indexOf(name) + 1]!; }
const auditPath = flag("--audit");
const evidenceRoot = flag("--evidence-root");
const sessionDir = flag("--session-dir");
const resumeIndex = argv.lastIndexOf("--resume");
const sessionFile = resumeIndex < 0 ? join(sessionDir, `watch-${process.pid}.jsonl`) : resolve(argv[resumeIndex + 1]!);
let sessionId = `watch-session-${process.pid}`;
if (existsSync(sessionFile)) {
  const header: unknown = JSON.parse(readFileSync(sessionFile, "utf8").split("\n")[0]!);
  if (header === null || typeof header !== "object" || !("id" in header) || typeof header.id !== "string") {
    throw new Error("Invalid benchmark session header");
  }
  sessionId = header.id;
}
function audit(kind: string, fields: Frame = {}): void {
  appendFileSync(auditPath, `${JSON.stringify({kind,pid:process.pid,sessionFile,...fields})}\n`);
}
function emit(frame: Frame): void { process.stdout.write(`${JSON.stringify(frame)}\n`); }
function reply(command: Frame, data?: unknown): void {
  emit({type:"response",id:command.id,command:command.type,success:true,data});
}
function finish(text: string): void {
  const message = {id:`message-${process.pid}-${Date.now()}`,role:"assistant",content:[{type:"text",text}]};
  emit({type:"message_start",message:{...message,content:[]}});
  emit({type:"message_update",message,assistantMessageEvent:{type:"text_delta",contentIndex:0,delta:text,partial:message}});
  emit({type:"message_end",message});
  emit({type:"agent_end",messages:[],willContinue:false});
}
let pendingTool = false;
function handle(command: Frame): void {
  switch (command.type) {
    case "new_session":
      writeFileSync(sessionFile, `${JSON.stringify({type:"session",version:3,id:sessionId,timestamp:"2026-01-01T00:00:00.000Z",cwd:process.cwd()})}\n`);
      reply(command,{cancelled:false}); return;
    case "get_state":
      reply(command,{sessionId,sessionFile,model:{provider:"fixture",id:"offline"},isStreaming:false}); return;
    case "get_available_models":
      reply(command,{models:[{provider:"fixture",id:"offline",name:"Offline"}]}); return;
    case "set_subagent_subscription":
    case "set_host_tools":
    case "abort": reply(command); return;
    case "prompt": {
      const text = String(command.message ?? "");
      audit("prompt",{text});
      if (text === "/retry") {
        emit({type:"command_output",text:"Nothing to retry."});
        reply(command,{agentInvoked:false});
        emit({type:"prompt_result",id:command.id,agentInvoked:false}); return;
      }
      reply(command,{agentInvoked:true});
      emit({type:"agent_start"});
      appendFileSync(sessionFile,`${JSON.stringify({type:"message",message:{role:"user",content:text}})}\n`);
      const match = /^watch:register:(\w+)$/u.exec(text);
      if (match) {
        pendingTool = true;
        const key = match[1]!;
        const args = key === "crashed"
          ? ["/bin/sh", "-c", `echo $$ > '${join(evidenceRoot, "collector.pid")}'; sleep 20; /bin/cat '${join(evidenceRoot, `${key}.txt`)}'`]
          : ["/bin/cat", join(evidenceRoot, `${key}.txt`)];
        emit({type:"host_tool_call",id:"watch-host-request",toolCallId:`watch-native-${key}`,toolName:"watch",arguments:{
          action:"create",command:args[0],args:args.slice(1),cwd:evidenceRoot,
          condition:"The communication politely addresses the customer's concerns and gives a clear next step.",interval:60,timeout:key === "expired" ? 1 : 180,
        }});
      } else finish(text === "foreground" ? "FOREGROUND_DONE" : "WATCH_WAKE_DONE");
      return;
    }
    case "host_tool_result":
      if (!pendingTool) throw new Error("unexpected host tool result");
      pendingTool = false;
      audit("registered",{isError:command.isError === true,result:command.result});
      finish(command.isError === true ? "WATCH_REGISTER_FAILED" : "WATCH_REGISTERED"); return;
    default:
      emit({type:"response",id:command.id,command:command.type,success:false,error:"Unsupported benchmark command"});
  }
}
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data",(chunk:string)=>{
  input += chunk;
  for (;;) {
    const end = input.indexOf("\n");
    if (end < 0) return;
    const line = input.slice(0,end); input = input.slice(end+1);
    if (line.trim()) handle(JSON.parse(line) as Frame);
  }
});
function exit(): void { audit("exit"); process.exit(0); }
process.on("SIGTERM",exit);
process.stdin.on("end",exit);
process.stdin.resume();
audit("startup");
emit({type:"ready",protocolVersion:1,supportedProtocolVersions:[1],maxFrameBytes:1048576});
