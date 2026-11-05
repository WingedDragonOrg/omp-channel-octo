import { expect, test } from "bun:test";
import type { ChatTarget, PromptContext } from "../src/contracts.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import { closeCoreHarness, createCoreHarness, inbound, waitForStatus } from "./fixtures/core-fakes.ts";
import type { CoreHarness } from "./fixtures/core-fakes.ts";

const target = { channelType: 1 as const, channelId: "owner-1" };
const watchConfig = { enabled: true as const, protocol: "openai-decisions" as const, baseUrl: "https://watch.example", model: "fixture", apiKeyEnv: "OMP_CHANNEL_OCTO_WATCH_KEY", apiKey: "synthetic-watch-key" };

async function eventually<T>(read: () => T | null | undefined | false): Promise<T> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    const value = read();
    if (value) return value;
    const turn = Promise.withResolvers<void>();
    setImmediate(turn.resolve);
    await turn.promise;
  }
  throw new Error("watch boundary did not settle");
}
async function waitForTaskState(
  harness: CoreHarness,
  target: ChatTarget,
  messageId: string,
  state: "queued" | "running" | "completed" | "cancelled" | "interrupted" | "failed",
): Promise<void> {
  const store = await openChannelStore(harness.stateDir);
  try {
    await eventually(() =>
      store.getTaskByMessageId(harness.bot.uid, target, messageId)?.state === state,
    );
  } finally {
    store.close();
  }
}

async function register(context: PromptContext | undefined, timeout = 60): Promise<string> {
  const tool = context?.tools?.find(entry => entry.name === "watch");
  if (!tool) throw new Error("watch tool unavailable");
  const result: {id:string} = JSON.parse(await tool.execute({action:"create",command:"fixture-collector",condition:"The response thoughtfully resolves the customer's concerns.",interval:60,timeout},new AbortController().signal,"source-native-watch-call"));
  return result.id;
}

// Subscription identity is the chat; only the newly accepted wake becomes exact-session work.
test("watch survives /new and its wake uses the new session without replaying source input", async () => {
  const collected = Promise.withResolvers<{kind:"success";stdout:string}>();
  let watchId = "";
  let sourceSession = "";
  let wakeSession = "";
  const harness = await createCoreHarness({
    decisions:watchConfig,watchCollector:async()=>collected.promise,
    watchJudge:async()=>({kind:"judgment",choice:"satisfied",probability:0.99}),
    prompt:async context=>{
      if (context.text === "explicit source request") {
        sourceSession = context.sessionId;
        watchId = await register(context.promptContext);
      } else wakeSession = context.sessionId;
    },
  });
  let store: ChannelStore | undefined;
  try {
    store = await openChannelStore(harness.stateDir);
    await harness.service.submitInbound(inbound({...target,messageId:"session-source",text:"explicit source request"}));
    await waitForTaskState(harness, target, "session-source", "completed");
    expect(watchId).not.toBe("");
    expect(sourceSession).not.toBe("");
    // A completed task row precedes runtime lease release; /new must wait until the source worker is closed.
    await eventually(() =>
      harness.workers.workers.find(worker => worker.sessionId === sourceSession)?.stopped,
    );

    const previousReplyIds = new Set(
      harness.transport.listRemoteText(target).map(({ messageId }) => messageId),
    );
    await harness.service.submitInbound(inbound({...target,messageId:"session-new",text:"/new"}));
    const newReceipt = await eventually(() =>
      harness.transport.listRemoteText(target).find(({ messageId, text }) =>
        !previousReplyIds.has(messageId) && text.startsWith("已创建会话 "),
      ),
    );
    const current = store.getBinding(harness.bot.uid, target);
    if (!current) throw new Error("/new did not persist its new binding");
    expect(current.sessionId).not.toBe(sourceSession);
    expect(newReceipt.text).toContain(current.sessionId);
    collected.resolve({kind:"success",stdout:"A thoughtful resolution and clear next step."});
    await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "completed");
    expect(current.sessionId).toBe(wakeSession);
    expect(harness.workers.prompts.filter(prompt=>prompt.text === "explicit source request")).toHaveLength(1);
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    collected.resolve({kind:"success",stdout:"late"});
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("cancelling the source task preserves its terminal state while the independent watch can wake", async () => {
  const collected = Promise.withResolvers<{kind:"success";stdout:string}>();
  const sourceReady = Promise.withResolvers<void>();
  let watchId = "";
  const harness = await createCoreHarness({
    decisions:watchConfig,watchCollector:async()=>collected.promise,
    watchJudge:async()=>({kind:"judgment",choice:"satisfied",probability:0.99}),
    prompt:async context=>{
      if (context.text !== "source to cancel") return;
      watchId = await register(context.promptContext);
      sourceReady.resolve();
      await context.waitForAbort();
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(inbound({...target,messageId:"cancel-source",text:"source to cancel"}));
    await sourceReady.promise;
    await harness.service.submitInbound(inbound({...target,messageId:"cancel-source-control",text:"/cancel"}));
    await waitForTaskState(harness, target, "cancel-source", "cancelled");
    store = await openChannelStore(harness.stateDir);
    const source = store.getTaskByMessageId(harness.bot.uid,target,"cancel-source");
    expect(source?.state).toBe("cancelled");
    collected.resolve({kind:"success",stdout:"A thoughtful resolution and clear next step."});
    await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "completed");
    expect(store.getTask(source!.id)?.state).toBe("cancelled");
    expect(store.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.id).not.toBe(source!.id);
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    collected.resolve({kind:"success",stdout:"late"});
    store?.close();
    await closeCoreHarness(harness);
  }
});

test("a match queues behind a pending ask without answering it or steering the waiting task", async () => {
  const collected = Promise.withResolvers<{kind:"success";stdout:string}>();
  let watchId = "";
  let answered: string | undefined;
  let appended: () => readonly unknown[] = () => [];
  const harness = await createCoreHarness({
    decisions:watchConfig,watchCollector:async()=>collected.promise,
    watchJudge:async()=>({kind:"judgment",choice:"satisfied",probability:0.99}),
    prompt:async context=>{
      if (context.text === "source before ask") watchId = await register(context.promptContext);
      if (context.text === "needs owner answer") {
        appended = context.appended;
        const ask = context.promptContext?.tools?.find(tool=>tool.name === "ask");
        if (!ask) throw new Error("ask unavailable");
        answered = await ask.execute({questions:[{question:"Which plan should proceed?"}]},new AbortController().signal,"ask-native-id");
      }
    },
  });
  let store: ChannelStore | undefined;
  try {
    await harness.service.submitInbound(inbound({...target,messageId:"ask-source",text:"source before ask"}));
    await waitForTaskState(harness, target, "ask-source", "completed");
    await harness.service.submitInbound(inbound({...target,messageId:"ask-busy",text:"needs owner answer"}));
    const waiting = await waitForStatus(harness.service,target,status=>status.pendingQuestionIds.length === 1);
    store = await openChannelStore(harness.stateDir);
    collected.resolve({kind:"success",stdout:"A thoughtful resolution and clear next step."});
    await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "queued");
    expect((await harness.service.status(target)).pendingQuestionIds).toEqual(waiting.pendingQuestionIds);
    expect(answered).toBeUndefined();
    expect(appended()).toEqual([]);
    expect(harness.workers.prompts).toHaveLength(2);
    await harness.service.submitInbound(inbound({...target,messageId:"ask-owner-answer",text:"Use the approved plan."}));
    await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "completed");
    expect(answered).toContain("Use the approved plan.");
    expect(harness.workers.prompts).toHaveLength(3);
  } finally {
    collected.resolve({kind:"success",stdout:"late"});
    if ((await harness.service.status(target)).pendingQuestionIds.length > 0) {
      await harness.service.submitInbound(inbound({...target,messageId:"ask-cleanup-answer",text:"Finish fixture."}));
    }
    store?.close();
    await closeCoreHarness(harness);
  }
});

for (const boundary of ["stop","deadline"] as const) {
  test(`a ${boundary} before first prompt prevents an already queued watch wake`, async () => {
    const collected = Promise.withResolvers<{kind:"success";stdout:string}>();
    const busy = Promise.withResolvers<void>();
    const busyStarted = Promise.withResolvers<void>();
    let watchId = "";
    const harness = await createCoreHarness({
      decisions:watchConfig,watchCollector:async()=>collected.promise,
      watchJudge:async()=>({kind:"judgment",choice:"satisfied",probability:0.99}),
      prompt:async context=>{
        if (context.text === "source before queue") watchId = await register(context.promptContext,boundary === "deadline" ? 1 : 60);
        if (context.text === "busy queue owner") { busyStarted.resolve(); await busy.promise; }
      },
    });
    let store: ChannelStore | undefined;
    try {
      await harness.service.submitInbound(inbound({...target,messageId:`${boundary}-source`,text:"source before queue"}));
      await waitForTaskState(harness, target, `${boundary}-source`, "completed");
      await harness.service.submitInbound(inbound({...target,messageId:`${boundary}-busy`,text:"busy queue owner"}));
      await busyStarted.promise;
      store = await openChannelStore(harness.stateDir);
      collected.resolve({kind:"success",stdout:"A thoughtful resolution and clear next step."});
      await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "queued");
      if (boundary === "stop") {
        const stopMessage = inbound({ ...target, messageId: "watch-stop-queued", text: `/watch stop ${watchId}` });
        await harness.service.submitInbound(stopMessage);
        await eventually(() => store!.getWatch(harness.bot.uid, watchId)?.state === "stopped");
      } else {
        await eventually(()=>store!.getWatch(harness.bot.uid,watchId)?.state === "expired");
      }
      busy.resolve();
      await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`watch:${watchId}`)?.state === "cancelled");
      await eventually(()=>store!.getTaskByMessageId(harness.bot.uid,target,`${boundary}-busy`)?.state === "completed");
      expect(harness.workers.prompts.map(prompt=>prompt.text)).toEqual(["source before queue","busy queue owner"]);
      expect(store.getWatch(harness.bot.uid,watchId)?.wakeStatus).toBe("cancelled");
    } finally {
      collected.resolve({kind:"success",stdout:"late"}); busy.resolve();
      store?.close(); await closeCoreHarness(harness);
    }
  });
}

test("confirmed remote thread deletion seals an in-flight watch before its late result", async () => {
  const thread = {channelType:5 as const,channelId:"group-allowed____watch-delete"};
  const collected = Promise.withResolvers<{kind:"success";stdout:string}>();
  let watchId = "";
  let collectorSignal: AbortSignal | undefined;
  const harness = await createCoreHarness({
    decisions:watchConfig,
    watchCollector:async(_spec,signal)=>{collectorSignal=signal;return collected.promise;},
    watchJudge:async()=>({kind:"judgment",choice:"satisfied",probability:0.99}),
    prompt:async context=>{
      if (context.text === "source before delete") watchId = await register(context.promptContext);
      if (context.text === "delete watched thread") {
        const management = context.promptContext?.tools?.find(tool=>tool.name === "octo_management");
        if (!management) throw new Error("management unavailable");
        await management.execute({action:"thread-delete",short_id:"watch-delete"},new AbortController().signal,"delete-watch-thread");
      }
    },
  });
  let store: ChannelStore | undefined;
  try {
    harness.transport.setThreads("group-allowed",[{shortId:"watch-delete",groupId:"group-allowed",channelId:thread.channelId,name:"Watched thread",creatorUid:"owner-1",status:1,memberCount:1,messageCount:0,createdAt:"2026-11-04T00:00:00Z",updatedAt:"2026-11-04T00:00:00Z"}]);
    await harness.service.submitInbound(inbound({...thread,messageId:"delete-source",text:"source before delete",mentionedUserIds:[harness.bot.uid]}));
    await waitForTaskState(harness, thread, "delete-source", "completed");
    await eventually(()=>collectorSignal);
    await harness.service.submitInbound(inbound({channelType:2,channelId:"group-allowed",messageId:"delete-control",text:"delete watched thread",mentionedUserIds:[harness.bot.uid]}));
    await waitForTaskState(harness, {channelType:2,channelId:"group-allowed"}, "delete-control", "completed");
    store = await openChannelStore(harness.stateDir);
    expect(store.getWatch(harness.bot.uid,watchId)?.state).toBe("deleted");
    expect(collectorSignal!.aborted).toBe(true);
    collected.resolve({kind:"success",stdout:"late successful result"});
    await harness.service.stop();
    expect(store.getTaskByMessageId(harness.bot.uid,thread,`watch:${watchId}`)).toBeNull();
    expect(store.isWatchChatDeleted(harness.bot.uid,thread)).toBe(true);
    expect(harness.workers.prompts).toHaveLength(2);
  } finally {
    collected.resolve({kind:"success",stdout:"late"});
    store?.close(); await closeCoreHarness(harness);
  }
});

test("explicit watch cwd follows existing home and quoting rules and rejects relative paths", async () => {
  let relativeRejected = false;
  const registeredCwds: string[] = [];
  let taskCwd = "";
  const harness = await createCoreHarness({
    decisions: watchConfig,
    watchCollector: async () => ({ kind: "success", stdout: "not ready" }),
    watchJudge: async () => ({ kind: "judgment", choice: "not_satisfied", probability: 0.99 }),
    prompt: async context => {
      taskCwd = context.cwd;
      const tool = context.promptContext?.tools?.find(entry => entry.name === "watch");
      if (!tool) throw new Error("watch unavailable");
      const args = { action: "create", command: "fixture-collector", condition: "The report thoughtfully addresses the concerns.", interval: 60, timeout: 60 };
      relativeRejected = await tool.execute({ ...args, cwd: "." }, new AbortController().signal, "relative-cwd").then(() => false, () => true);
      for (const [cwd, callId] of [["~/tmp", "home-cwd"], [`'${context.cwd}'`, "quoted-cwd"]]) {
        const result: { id: string } = JSON.parse(await tool.execute({ ...args, cwd }, new AbortController().signal, callId));
        const detail: { spec: { cwd: string } } = JSON.parse(await tool.execute({ action: "get", id: result.id }, new AbortController().signal));
        registeredCwds.push(detail.spec.cwd);
      }
    },
  });
  try {
    await harness.service.submitInbound(inbound({ ...target, messageId: "watch-cwd-policy", text: "Explicitly watch these reports from the selected directories." }));
    await waitForTaskState(harness, target, "watch-cwd-policy", "completed");
    expect(relativeRejected).toBe(true);
    expect(registeredCwds).toEqual([taskCwd, taskCwd]);
  } finally {
    await closeCoreHarness(harness);
  }
});
