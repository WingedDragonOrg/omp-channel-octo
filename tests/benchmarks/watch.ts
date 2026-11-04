import assert from "node:assert/strict";
import { Database } from "bun:sqlite";
import { existsSync, readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelStore } from "../../src/store/index.ts";
import { createOctoProtocolFixture, waitForFixture, type FixtureConnection } from "../fixtures/octo-protocol-server.ts";

// Full offline path: encrypted Octo -> CLI -> real RPC child -> native collector -> HTTPS Decisions -> SQLite -> queued RPC task.
// Every account, key, message, file and process belongs to this disposable fixture.
const checkout = resolve(import.meta.dir, "../..");
const root = await realpath(await mkdtemp(join(tmpdir(), "oco-watch-benchmark-")));
const fixture = await createOctoProtocolFixture({ cardProfileBody: { enabled: false }, botEventsStatus: 404 });
const children: Bun.Subprocess[] = [];
const outputs: Promise<string[]>[] = [];
const auditPath = join(root, "audit.jsonl");
const configPath = join(root, "config.json");
const stateDir = join(root, "state");
const key = "synthetic-watch-key";
const target = { channelType: 1 as const, channelId: fixture.ownerUid };
const gates = new Map<string, { promise: Promise<void>; resolve: () => void }>();
const requests: string[] = [];
let store: ChannelStore | undefined;
let db: Database | undefined;
let provider: Bun.Server<undefined> | undefined;
let success = false;

function auditRows(): Array<{kind:string;pid:number;isError?:boolean;text?:string}> {
  return existsSync(auditPath) ? readFileSync(auditPath,"utf8").trim().split("\n").filter(Boolean).map(line=>JSON.parse(line)) : [];
}
function alive(pid: number): boolean {
  try { process.kill(pid,0); return true; }
  catch (error) { if (typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH") return false; throw error; }
}
async function exit(child: Bun.Subprocess): Promise<number> {
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error("watch CLI exit deadline exceeded")), 10_000);
  try {
    return await Promise.race([child.exited, timeout.promise]);
  } finally { clearTimeout(timer); }
}
function send(connection: FixtureConnection, text: string, messageId: string): void {
  fixture.sendInbound(connection,{fromUid:fixture.ownerUid,channelId:`${fixture.ownerUid}@${fixture.botUid}`,channelType:1,text,messageId});
}
function task(messageId: string): {id:string;state:string;session_id:string|null} | null {
  return db!.query("SELECT id,state,session_id FROM tasks WHERE message_id = ?").get(messageId) as {id:string;state:string;session_id:string|null} | null;
}
async function launch(): Promise<{child:Bun.Subprocess;connection:FixtureConnection}> {
  const child = Bun.spawn([process.execPath,join(checkout,"src/cli.ts"),"run","--config",configPath],{
    cwd:root,
    env:{HOME:root,PATH:`${join(root,"bin")}:/usr/bin:/bin`,TMPDIR:root,LANG:"C",TZ:"UTC",NODE_EXTRA_CA_CERTS:join(root,"certificate.pem"),OMP_CHANNEL_OCTO_WATCH_KEY:key},
    stdin:"ignore",stdout:"pipe",stderr:"pipe",
  });
  children.push(child);
  outputs.push(Promise.all([new Response(child.stdout).text(),new Response(child.stderr).text()]));
  const connection = await fixture.waitForConnection(children.length,15_000);
  await waitForFixture(()=>connection.ready ? true : undefined,10_000,"watch CLI failed to authenticate");
  return {child,connection};
}
async function registered(connection: FixtureConnection, name: string, messageId: string) {
  const before = store!.listWatches(fixture.botUid,target,50,0).map(row=>row.id);
  send(connection,`watch:register:${name}`,messageId);
  await waitForFixture(()=>task(messageId)?.state === "completed" ? true : undefined,10_000,`${name} source task did not finish`);
  const summaries = store!.listWatches(fixture.botUid,target,50,0);
  const summary = summaries.find(row=>!before.includes(row.id));
  assert.ok(summary,`${name} did not create a watch`);
  const record = store!.getWatch(fixture.botUid,summary.id);
  assert.ok(record);
  await waitForFixture(()=>auditRows().filter(row=>row.kind === "startup").every(row=>!alive(row.pid)) ? true : undefined,5_000,"source RPC worker stayed resident");
  return record;
}
function hold(name: string): void {
  const gate = Promise.withResolvers<void>(); gates.set(name,{promise:gate.promise,resolve:()=>gate.resolve()});
}

try {
  await mkdir(join(root,"tmp"));
  await mkdir(join(root,"bin"));
  await writeFile(join(root,"bin","octo-cli"),"#!/bin/sh\nexit 127\n",{mode:0o755});
  await writeFile(join(root,"tls.conf"),"[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=v3\n[dn]\nCN=localhost\n[v3]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:TRUE\nkeyUsage=keyCertSign,digitalSignature,keyEncipherment\n");
  const certificate = Bun.spawn(["openssl","req","-new","-newkey","rsa:2048","-nodes","-x509","-days","1","-keyout",join(root,"key.pem"),"-out",join(root,"certificate.pem"),"-config",join(root,"tls.conf")],{stdout:"ignore",stderr:"pipe"});
  const certificateError = await new Response(certificate.stderr).text();
  assert.equal(await certificate.exited,0,certificateError);
  provider = Bun.serve({
    hostname:"127.0.0.1",port:0,tls:{key:Bun.file(join(root,"key.pem")),cert:Bun.file(join(root,"certificate.pem"))},
    async fetch(request) {
      assert.equal(new URL(request.url).pathname,"/v1/decisions");
      assert.equal(request.headers.get("authorization"),`Bearer ${key}`);
      const body: unknown = await request.json();
      assert.ok(body !== null && typeof body === "object" && "input" in body && typeof body.input === "string");
      assert.ok("model" in body);
      assert.equal(body.model,"fixture-only");
      const input: unknown = JSON.parse(body.input);
      assert.ok(input !== null && typeof input === "object" && "evidence" in input);
      const evidence = input.evidence;
      assert.ok(evidence !== null && typeof evidence === "object" && "stdout" in evidence && typeof evidence.stdout === "string");
      assert.deepEqual(Object.keys(evidence).sort(),["collectedAt","condition","stdout"]);
      const name = evidence.stdout.trim();
      requests.push(name);
      const gate = gates.get(name);
      if (gate) await gate.promise;
      return Response.json({answers:[{name:"watch_condition",type:"choice",choice:name === "waiting" ? "not_satisfied" : "satisfied",probabilities:[{value:name === "waiting" ? "not_satisfied" : "satisfied",probability:0.99}]}]});
    },
  });
  await writeFile(configPath,JSON.stringify({
    apiUrl:fixture.apiUrl,botToken:fixture.botToken,ownerId:fixture.ownerUid,allowedGroupIds:[],stateDir,sessionsDir:join(root,"sessions"),
    ompCommand:process.execPath,ompExtraArgs:[join(checkout,"tests/fixtures/watch-benchmark-worker.ts"),"--audit",auditPath,"--evidence-root",root],
    startupTimeoutMs:5_000,cancelTimeoutMs:2_000,groupHistory:{enabled:false,maxMessages:50},
    decisions:{enabled:true,protocol:"openai-decisions",baseUrl:`https://127.0.0.1:${provider.port}`,model:"fixture-only",apiKeyEnv:"OMP_CHANNEL_OCTO_WATCH_KEY"},
  }),{mode:0o600});
  for (const name of ["positive","stopped","expired","waiting","crashed"]) await writeFile(join(root,`${name}.txt`),name);
  const first = await launch();
  store = await ChannelStore.open(join(stateDir,"channel.sqlite"));
  db = new Database(join(stateDir,"channel.sqlite"),{readonly:true});

  hold("positive");
  const positive = await registered(first.connection,"positive","101");
  await waitForFixture(()=>requests.includes("positive") ? true : undefined,5_000,"collector did not reach HTTPS provider");
  assert.equal(task(`watch:${positive.id}`),null,"normal check started an agent");
  send(first.connection,"foreground","102");
  await waitForFixture(()=>task("102")?.state === "completed" ? true : undefined,5_000,"watch held the source chat execution lease");
  gates.get("positive")!.resolve();
  await waitForFixture(()=>task(`watch:${positive.id}`)?.state === "completed" ? true : undefined,10_000,"semantic match did not complete a queued wake");
  assert.equal(requests.filter(name=>name === "positive").length,1);
  console.log("CASE match after source exit + concurrent foreground: passed");

  hold("stopped");
  const stopped = await registered(first.connection,"stopped","103");
  await waitForFixture(()=>requests.includes("stopped") ? true : undefined,5_000,"stop case provider was not in flight");
  send(first.connection,`/watch stop ${stopped.id}`,"104");
  await waitForFixture(()=>store!.getWatch(fixture.botUid,stopped.id)?.state === "stopped" ? true : undefined,5_000,"stop did not persist");
  gates.get("stopped")!.resolve();
  assert.equal(task(`watch:${stopped.id}`),null);

  hold("expired");
  const expired = await registered(first.connection,"expired","105");
  await waitForFixture(()=>store!.getWatch(fixture.botUid,expired.id)?.state === "expired" ? true : undefined,5_000,"timeout did not expire watch");
  gates.get("expired")!.resolve();
  assert.equal(task(`watch:${expired.id}`),null);
  console.log("CASE stop/expiry with late provider response: passed");

  const waiting = await registered(first.connection,"waiting","106");
  await waitForFixture(()=>store!.getWatch(fixture.botUid,waiting.id)?.latest !== null && store!.getWatch(fixture.botUid,waiting.id)?.state === "waiting" ? true : undefined,5_000,"valid not-satisfied result was not retained");
  const waitingBefore = store.getWatch(fixture.botUid,waiting.id)!;
  first.child.kill("SIGTERM"); assert.equal(await exit(first.child),0);
  const second = await launch();
  const waitingAfter = store.getWatch(fixture.botUid,waiting.id)!;
  assert.equal(waitingAfter.deadlineAt,waitingBefore.deadlineAt);
  assert.equal(waitingAfter.checks,waitingBefore.checks);
  assert.equal(store.getWatch(fixture.botUid,stopped.id)?.state,"stopped");
  assert.equal(store.getWatch(fixture.botUid,expired.id)?.state,"expired");
  send(second.connection,`/watch stop ${waiting.id}`,"107");
  await waitForFixture(()=>store!.getWatch(fixture.botUid,waiting.id)?.state === "stopped" ? true : undefined,5_000,"restored watch could not be stopped");
  console.log("CASE graceful restart preserves deadline/budget/terminal states: passed");

  const crashed = await registered(second.connection,"crashed","108");
  await waitForFixture(()=>existsSync(join(root,"collector.pid")) ? true : undefined,5_000,"crash collector never began");
  const oldCollector = Number((await readFile(join(root,"collector.pid"),"utf8")).trim());
  assert.ok(alive(oldCollector));
  second.child.kill("SIGKILL"); await exit(second.child);
  const third = await launch();
  await waitForFixture(()=>!alive(oldCollector) ? true : undefined,10_000,"old collector survived recovery");
  send(third.connection,`/watch stop ${crashed.id}`,"109");
  await waitForFixture(()=>["stopped","unknown"].includes(store!.getWatch(fixture.botUid,crashed.id)!.state) ? true : undefined,5_000,"crashed watch lost its stop/unknown outcome");
  assert.equal(task(`watch:${crashed.id}`),null);
  assert.equal(task(`watch:${stopped.id}`),null);
  assert.equal(task(`watch:${expired.id}`),null);
  assert.equal(requests.filter(name=>name === "crashed").length,0);
  const wakeCount = db.query<{n:number}, []>("SELECT count(*) AS n FROM tasks WHERE message_id LIKE 'watch:%'").get();
  assert.equal(wakeCount?.n,1);
  third.child.kill("SIGTERM"); assert.equal(await exit(third.child),0);
  console.log("CASE SIGKILL collector ownership recovery + zero stopped/expired wakes: passed");
  for (const output of await Promise.all(outputs)) {
    assert.ok(!output.join("\n").includes(key),"provider secret reached CLI output");
    assert.ok(!output.join("\n").includes(fixture.botToken),"bot secret reached CLI output");
  }
  assert.ok(auditRows().filter(row=>row.kind === "registered").every(row=>row.isError === false));
  success = true;
  console.log(JSON.stringify({result:"passed",providerRequests:requests.length,wakeTasks:1,stoppedOrExpiredWakes:0,realRpcProcesses:auditRows().filter(row=>row.kind === "startup").length}));
} finally {
  for (const gate of gates.values()) gate.resolve();
  for (const child of children) {
    if (child.exitCode === null) { child.kill("SIGTERM"); try { await exit(child); } catch { child.kill("SIGKILL"); await child.exited; } }
  }
  store?.close(); db?.close(); provider?.stop(true); await fixture.close();
  if (success) await rm(root,{recursive:true,force:true});
  else console.error(`Watch benchmark diagnostic directory: ${root}`);
}
