# 派发回传：thread-dispatch 的 notify 开关

日期：2026-09-17

## 背景与目标

`thread-dispatch` 把一条任务交到子区后，任务在子区自己的聊天里结束，派发方不会知道结果。本次给派发加一个可选开关 `notify`：派发时由派发方自行判断这条手尾是否需要回报；开启后，该任务运行中到达终态（完成/失败/取消/中断）时，`CoreService` 向发起派发的聊天投递一条有界回传通知（合成入站消息 `notify:<子区任务 id>`），由该聊天的助手作为新输入处理，可以据此向本群反馈或继续处理。

回传是逐条派发的能力：登记随任务行持久化，重启后仍然有效；默认关闭。

## 范围

### 包含

- `thread-dispatch` 新增可选布尔参数 `notify`（默认 `false`）；派发成为独立任务时，回传目标（派发方聊天）与派发时捕获的子区名随任务行写入。
- 派发任务运行中终态（`completed`/`failed`/`cancelled`/`interrupted`）的检测点，以及向派发方聊天的通知投递（并入运行中回合或成为其最新任务）。
- 回传通知的有界文本模板与去重语义（每任务至多一条）。
- 工具描述中的使用建议（何时开启），以及 `/thread` 交接指令里的同一建议。
- 主规格、根 `AGENTS.md` 与测试同步。

### 不包含

- 子区级 / 聊天级开关或默认值：没有「这个子区一律回传」的登记；由派发方逐条决定，默认关闭。
- 频率上限与聚合：不设节流窗口、批量摘要或定时器；条数由派发方的判断控制。
- 服务停止/重启导致的终态不做回传（见下）。
- 已被并入运行中回合的派发（没有独立任务行，回传不生效；工具结果与收据说明）。
- 回传的发送回执与失败重试：投递失败只记一行日志。
- 对既有任务补开或关闭回传的动作：需要回执的活重新派发时开启。

## 已知约束

- Bot 自己发送的消息永不启动任务：回传必须有意旁路该规则。通知以合成 `InboundMessage` 经 `appendToRunningTask`/`acceptTask` 走与派发相同的 durable 入站路径，不经过 `handleInbound`，因此不触发权限门禁、不会成为待答问题的答案；`messageSeq = 0` 使其不参与群历史水位。
- 任务终态由任务行落盘（`markCompleted`/`markFailed`/`markCancelled`/`markInterrupted`）。派发是显式交接：派发任务的入站消息 ID 带 `dispatch:` 前缀，派发深度固定为 1；被并入运行中回合的派发不产生任务行。
- 只有 owner 能驱动 `octo_management`；回传内容单条有界，不含 token、原始工具参数/结果或协议载荷。
- 同一任务至多通知一次：通知消息 ID 固定为 `notify:<子区任务 id>`，复用 `inbound` 的 durable claim 去重。
- 旧版本的子区级登记表 `thread_notify` 在 schema 初始化时无条件删除（`DROP TABLE IF EXISTS`），没有读者。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 开关形态 | `thread-dispatch` 的可选布尔 `notify`（默认 `false`） | 回传是派发方对自己手尾的要求；逐条决定贴合场景，默认关闭不会复现「几乎都开」 |
| 通知目标 | 发起派发的聊天本身（群聊派发指向该群；子区派发指向该子区） | 回执落在做决定的地方，请求链可追溯 |
| 消费语义 | 通知作为一条合成入站消息：派发方有可接收 steering 的运行中回合时并入该回合，否则成为其 durable 任务（恰好一次唤醒） | 与派发同构；「派发方助手收到」有明确机制，且一次通知只唤醒一次 |
| 触发终态 | 运行中产生的 `completed`/`failed`/`cancelled`/`interrupted` | 终态才是有意义的结束点；失败、取消、中断各带独立语义行 |
| 停止/重启 | 不回传：`stopping` 期间的 settle 一律跳过，`markInterruptedOnStart` 不接入 | 停止是基础设施事件而不是任务结果；崩溃循环不得放大成派发方的成批模型回合 |
| 并入回合的派发 | 不生效：没有任务行就没有登记；工具结果 `notify=skipped` 且收据说明 | 并入的内容随该回合结束，没有独立结束点可回传 |
| 通知内容 | 有界结构化模板：子区身份、任务 id、状态、任务摘要（≤200 字符）、结果/失败原因/取消或中断说明（≤1500 字符）、一行回传提示；子区名在派发时捕获 | 单条有界、结果主体保留、可被派发方助手直接消费；子区后续改名不影响既有回传 |
| 去重 | 通知消息 ID `notify:<子区任务 id>` + `inbound` claim | 同一任务至多一条通知；重启、重放都不重复 |
| 持久化 | 任务行新增 `relay_channel_type`/`relay_channel_id`/`relay_thread_name`（null = 不登记） | 登记与任务同生命周期；重启后排队任务照常回传，无需恢复过程 |
| 来源 | 只回传派发任务；子区内直接发出的消息、`/queue`、问答唤醒永不回传 | 人在子区里发消息说明人在场；回传只报告派出去的手尾 |
| 频率 | 不设硬性上限；工具描述与 `/thread` 指令给出「派发后不再盯、又需要知道结果（含失败）时才开启」的建议 | 条数由派发方的判断控制，机制保持简单 |
| 投递失败 | 只记 `[thread-notify] 回传投递失败 task=…` 一行日志 | 任务结果本身已在子区聊天；回传是尽力而为的增量 |

## 设计

### 工具与登记

- `thread-dispatch` 读取可选布尔 `notify`（非布尔值报 `notify must be a boolean`，在解析线程之前拒绝），默认 `false`。
- 派发成为独立任务时：工具把 `{channelType, channelId, threadName}`（调用方聊天 + `getThread` 取到的线程名）交给投递路径，随任务行写入 `relay_*` 三列。工具结果 `notify: "registered"`，收据写「已在子区 … 作为独立任务执行；结束时将把结果回传到这里。」。
- 派发被并入运行中回合时：没有任务行，回传不登记；工具结果 `notify: "skipped"`（附 `notify_note: 内容并入运行中回合，不会单独回传结果`），收据写「并入的这部分不会单独回传结果。」。
- `create-thread` 不再有 `notify` 参数，`thread-delete` 不再清理任何登记（登记随任务行自然存在）。

### 检测点

运行中终态统一从任务运行流程收集：任务收尾（`finally`）中读一次任务终态，属于四个终态即调用 `notifyThreadOutcome`；该路径覆盖运行期间的全部 settle（含 `/cancel` 与 `/interrupt`）。另有两处显式调用：

- `/cancel` 的排队任务在逐个 settle 之后回传（`cancelQueued` 中 `for (const taskId of queued) await this.notifyThreadOutcome(taskId)`）；
- `runTask`/`runScheduledTask` 的启动前失败（绑定目录变更、目录仍被阻塞）在任务落成 `failed` 并回复失败话术之后调用。

`start()` 的 `markInterruptedOnStart` 不接入；`stopping` 期间的 settle 由 `notifyThreadOutcome` 自身的守卫跳过。

### 投递

`CoreService.notifyThreadOutcome(taskId)`：

1. `stopping` 为真、bot 未就绪、任务不存在、`channelType !== 5`、`relay_channel_id`/`relay_channel_type` 为空、状态非四个终态之一 → 直接返回。
2. 用任务行渲染通知文本，组装合成 `InboundMessage`：目标为任务行上的回传目标；`messageId = notify:<taskId>`；`senderId` 为持久 owner（缺失时回退 bot uid）；`messageSeq = 0`；`supported = true`；无附件与引用。
3. `appendToRunningTask`（派发方有可接收 steering 的运行中回合时并入）→ 否则 `acceptTask`（成为派发方的最新任务）。
4. 整体 try/catch：任何失败只记一行 `[thread-notify]` 日志，不重试。

回传在任务收尾（`finally`）中于该任务的 runtime 注销之后、`maybeRunDeferredRestart` 之前发出：派发方与子区共享 cwd 时，任务不再被视作「排在这个运行中的回合之后」，因此不会多收一行「开始执行排队消息」回执；同时回传落成的排队任务仍会推迟已排定的重启。

### 通知文本（逐字模板）

```
[子区任务回传] 「<派发时捕获的线程名>」的一条任务已结束。      ← 名字缺失时为「[子区任务回传] 一条子区任务已结束。」
子区：short_id <short_id>；channel_id <channel_id>
任务 ID：<任务 id>
状态：已完成 | 失败 | 已取消 | 已中断
任务：<任务正文 trim 后截 200 字符；空则省略本行>
结果：<resultText trim 后截 1500 字符；空为「（无正文）」>            ← completed
失败原因：<errorText trim 后截 1500 字符；空为「（未记录原因）」>      ← failed
说明：任务已取消（不回滚已发生的副作用）。                           ← cancelled
说明：<errorText trim 后截 1500 字符；空为「任务已中断。」>           ← interrupted
这是派发这条任务时要求的回传通知；如需向本群反馈或继续处理，直接回复即可。
```

截断均以 `…` 结尾（`bounded`）。

### 模块与接口

- `src/contracts.ts`：`TaskRelayTarget`（`channelType: 2 | 5`、`channelId`、`threadName`）。
- `src/store-records.ts`：`TaskRecord` 新增 `relayChannelType`/`relayChannelId`/`relayThreadName`；`ThreadNotifyRecord` 删除。
- `src/store-schema.ts`：`tasks` 三个新列、`thread_notify` 建表移除并在初始化时 `DROP TABLE IF EXISTS`；`taskFromRow` 解码新列。
- `src/store-tasks.ts`：`acceptTask` 写入 relay 列。
- `src/store-chats.ts` / `src/store.ts`：删除 `registerThreadNotify`/`getThreadNotify`/`clearThreadNotify` 与门面转发。
- `src/thread-notify.ts`：`threadOutcomeNotice(task)` 从任务行渲染（模板、`bounded`、`stateLabel`、`outcomeLine`）。
- `src/host-tools.ts`：`thread-dispatch` 的 `notify` 参数与登记、收据、工具描述；`create-thread` 去掉 `notify`；`thread-delete` 去掉清理；投递接口接收 `TaskRelayTarget | null`。
- `src/service.ts`：`acceptTask` 接收 relay 目标；`notifyThreadOutcome` 读任务行。
- `src/thread-commands.ts`：`threadIntentPrompt` 包含是否请求完成通知的判断建议。

## 错误与边界情况

- 通知投递失败（状态盘写入失败等）：只记日志，不重试。
- 优雅停止/重启期间的 settle 不回传；`/cancel`、`/interrupt` 与 `ask` 等待超时照常回传。
- 派发被并入运行中回合：不生成任务行与回传，工具结果与收据说明。
- 子区被删除：已存在的任务行不受影响（迟到的结束仍如实报告）；期间不能产生新的登记。
- 派发方聊天已被删除或不可达：通知仍按 durable 路径接受，后续投递由既有 outbox 语义处理。
- 通知进入派发方时该聊天有待回答问题：不在答案路径上，问题保持待答。
- 通知以 steering 并入运行中回合时，派发方照常得到一行「已并入当前回合」回执。
- 任务正文或结果超长：分别截到 200/1500 字符并带 `…`。
- 同一任务至多一条通知；重启、重放后不重复。
- 旧版本子区级登记（`thread_notify` 表）随升级删除，不再回传；需要回执的活在新版本里重新派发时开启。

## 验收标准

1. `thread-dispatch notify=true` 且派发成为独立任务：任务行带上回传目标与子区名，工具结果 `notify=registered`、收据注明结束时回传。
2. 派发任务 `completed`：派发方聊天恰好收到一条合成消息（消息 ID `notify:<任务 id>`），含状态与结果摘要；派发方空闲时成为其一个任务，运行中且可接收 steering 时并入该回合、不成为第二个 prompt。
3. 派发任务运行中产生的 `failed`/`cancelled`/`interrupted`：各有一条对应语义的通知（`/cancel` 连排队的派发任务一并回传）。
4. 服务停止/重启导致的终态（`stopping` 期间的 `cancelled`、`markInterruptedOnStart` 的 `interrupted`）不产生任何回传。
5. 未带 `notify` 的派发与一切非派发来源的子区任务（直接发的消息、`/queue`）：不产生任何通知、额外回合或登记。
6. 派发被并入运行中回合：不产生登记与回传，工具结果 `notify=skipped` 且收据说明。
7. 同一任务至多一条通知；重启（同一状态目录的新进程）后任务行登记保留、新派发照常回传、旧任务不重复。
8. `bun run typecheck` 与 `bun test` 全绿；既有命令、派发与任务流程不回归。

## 未决事项

无。
