# 子区任务完成回传（create-thread 回传开关）

日期：2026-09-17

## 背景与目标

`octo_management` 的 `create-thread` 目前只有创建与工作区能力：子区里的任务结束后，只有子区自己的聊天看到结果，创建它的群聊会话不会知道。本次为 `create-thread` 增加一个可选布尔参数 `notify`：开启后，该子区每条任务结束（完成/失败/取消/中断）时，创建它的聊天助手会话收到一条有界回传通知，并据此可以自行决定是否向本群反馈或继续处理。

## 范围

### 包含

- `octo_management` 的 `create-thread` 新增可选布尔参数 `notify`（默认 `false`），按子区持久化登记，服务重启后仍生效。
- 子区任务运行中终态（`completed`/`failed`/`cancelled`/`interrupted`）的检测点与向创建方聊天的通知投递。
- 回传通知的文本模板、边界与去重语义。
- `thread-delete` 联动清理该子区的回传登记。
- 主规格、根 `AGENTS.md` 与测试同步。

### 不包含

- 服务停止/重启导致的终态不回传：优雅停止（SIGTERM、进程内 `stop()`）会把在途任务落成 `cancelled`（与用户 `/cancel` 共用取消分支，用 `stopping` 标志区分），`markInterruptedOnStart` 把遗留任务落成 `interrupted`——两条路径都不回传。重启通告仍只发给任务所在聊天；重启恢复不得放大成创建方成批回合（崩溃循环尤其如此），也不产生「回传任务又被重启中断」的伪通告。
- 开关的读取/修改动作：登记只在 `create-thread` 时写入，`thread-info` 等动作不暴露该状态；不对既有子区补登记。
- 私聊建区：`octo_management` 只在群/线程可用，通知目标也因此只可能是群（channelType=2）或子区（channelType=5）。
- 通知的发送回执与失败重试：投递失败只记日志（与 `wakeTurn` 的先例一致），不打扰子区、不落第二张表。
- 对通知回合的特殊能力限制：通知唤醒的回合与普通任务一样拥有全部 host tools（见「错误与边界情况」）。

## 已知约束

- Bot 自己发的消息永不启动任务：回传必须有意旁路该规则。通知以合成 `InboundMessage` 经 `appendToRunningTask`/`acceptTask` 走与 `wakeTurn`、`thread-dispatch` 相同的 durable 入站路径，不经过 `handleInbound`，因此既不触发权限门，也不会被当成待回答问题的答案，`messageSeq = 0` 使其不参与群历史水位。
- 任务终态由 `ChannelStore` 落盘（`markCompleted`/`markFailed`/`markCancelled`/`markInterrupted`）。运行中终态、优雅停止落成的 `cancelled`、重启落成的 `interrupted` 是三条不同来源。
- 每个聊天独立绑定与会话；通知投递到创建方聊天，不影响子区内部的正文与进度卡投递。
- 只有 owner 能驱动 `octo_management`（权限门禁与 allowlist 不变）；通知内容单条有界，不含 token、原始工具参数/结果或协议载荷。
- 同一子区任务至多通知一次：通知消息 ID 固定为 `notify:<子区任务 id>`，复用 `inbound` 去重键。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 参数命名 | `notify`（布尔，默认 `false`） | 语义是「任务结束时通知创建方」；`listen` 的方向相反（回传发生在创建方侧） |
| 通知目标 | 调用 `create-thread` 的聊天本身（群聊建区即该群；子区建子区即该子区） | 保持请求链：子区建子区时通知回到请求方而不是跳过它；常见场景就是群聊 |
| 消费语义 | 通知作为一条合成入站消息交付：创建方有可接收 steering 的运行中回合时并入该回合，否则成为创建方自己的 durable 任务（恰好一次新回合） | 与 `wakeTurn`/`thread-dispatch` 同构；「主 agent 收到」有明确机制，且一次通知只唤醒一次 |
| 触发终态 | 运行中产生的 `completed`/`failed`/`cancelled`/`interrupted` | 覆盖验收要求；失败、取消、中断各带独立的语义行 |
| 停止/重启 | 不回传：`stopping` 期间的 settle 一律跳过，`markInterruptedOnStart` 不接入 | 停止是基础设施事件而不是任务结果；回传若在停机前落盘，下一次启动只会把它中断成创建方的伪通告；崩溃循环不得放大成成批模型回合 |
| 通知内容 | 有界结构化模板：子区身份、任务 id、状态、任务摘要（≤200 字符）、结果/失败原因/取消或中断说明（≤1500 字符）、一行自动回传提示 | 单条有界、结果主体保留、可被创建方助手直接消费；模板逐字见下 |
| 去重 | 通知消息 ID `notify:<子区任务 id>` + `inbound` claim | 同一任务至多一条通知；重启、重放都不会重复 |
| 持久化 | 新表 `thread_notify`：子区目标 → 创建方目标 + 名称 + 时间 | 开关按子区保存、跨重启生效；不依赖子区绑定是否已创建（未建工作区/未建会话的子区同样生效） |
| 登记权威 | `create-thread` 就是该新子区的权威：`notify=true` 写入，未开启时清除可能残留的同 id 登记 | 服务端 short_id 可能复用；新建子区永远不继承旧创建方 |
| 登记失败 | 建区不回滚，结果带 `notify_error`（≤200 字符） | 线程已在服务端创建，与 `worktree_error` 同一先例 |
| 投递失败 | 只记 `[thread-notify]` 日志 | 任务结果本身已在子区聊天；回传是尽力而为的增量 |

## 设计

### 工具与登记

- `create-thread` 读取 `notify`（`optionalBoolean`，非布尔值报 `notify must be a boolean`）。线程创建成功后、工作区处理之前同步回传登记：
  - `notify === true`：写入 `thread_notify` 行（子区目标 `{channelType: 5, channelId: thread.channelId}`，创建方为调用方聊天目标，名称为服务端返回的 `thread.name`），结果追加 `notify: true`。
  - 未开启：清除该 `channel_id` 上可能残留的登记（服务端 short_id 复用场景），结果不追加任何字段。
  - 状态盘写入失败：返回 `notify_error: <单行 ≤200 字符>`，线程与工作区路径不受影响。
- `thread-delete` 在服务端删除成功后清理该子区登记（`threadChannelId(groupId, shortId)`）；清理失败并入回执说明（best-effort，删除已生效）。
- 工具描述与参数描述同步说明默认值与语义。

### 检测点

运行中终态统一从 `runTask` 收集，避免遗漏：

- `runTask` 的 `finally`：读一次任务终态，属于四个终态即调用回传（覆盖 try 内全部 settle 路径，含 `/cancel` 与 `/interrupt`）。
- 停止期间（`this.stopping === true`）一律跳过：优雅停止把在途任务落成 `cancelled`，它与用户 `/cancel` 共用取消分支，只能靠该标志区分。
- `runTask` 两个启动前失败（绑定目录变更、目录仍被阻塞）与 `runScheduledTask` 的「绑定目录在启动前持续变更」失败：settle 之后显式调用回传（`failed` 语义）。
- `/cancel`：`cancelQueued` 返回的每个排队任务逐个回传（运行中任务由它自己的 `finally` 覆盖）。
- `start()` 的 `markInterruptedOnStart`：不接入。

### 投递

`CoreService.notifyThreadOutcome(taskId)`（私有）：

1. `stopping` 为真时直接返回；读任务；非 `channelType=5`、非四个终态、无 `thread_notify` 登记时直接返回。
2. 组装 `InboundMessage`：目标为登记中的创建方目标；`messageId = notify:<taskId>`；`senderId` 为持久 owner（缺失时回退 bot uid）；`messageSeq = 0`；`supported = true`；无附件与引用。
3. `appendToRunningTask` → `acceptTask`（与 `wakeTurn` 相同两步）；函数整体 try/catch，任何失败只记一行 `[thread-notify]` 日志。

在 `runTask` 的 `finally` 中，回传在 `runtimes.delete(taskId)` 之后、`maybeRunDeferredRestart` 之前发出：创建方与子区共享 cwd 时，任务不再被视作「排在这个运行中的回合之后」，因此创建方不会多收一行「开始执行排队消息」回执；同时回传落成的排队任务仍会推迟已排定的重启。

### 通知文本（逐字模板）

```
[子区任务回传] 「<名称>」的一条任务已结束。
子区：short_id <short_id>；channel_id <channel_id>
任务 ID：<任务 id>
状态：已完成 | 失败 | 已取消 | 已中断
任务：<任务正文摘要，首 200 字符，空则省略本行>
结果：<result_text 首 1500 字符；无正文为「（无正文）」>        ← completed
失败原因：<error_text 首 1500 字符>                              ← failed
说明：任务已取消（不回滚已发生的副作用）。                        ← cancelled
说明：<error_text 首 1500 字符>（如「用户中断当前回合」）          ← interrupted
这是系统自动回传的子区任务通知；如需向本群反馈或继续处理，直接回复即可。
```

截断带 `…` 标记。上限是字符口径（整条约 2.5K 字符，中文 UTF-8 约数 KB），远低于 RPC 帧与任务正文的任何既有约束；单条有界。

### 模块与接口

- `src/store-records.ts`：`ThreadNotifyRecord`（bot、子区 `ChatTarget`、`creatorChannelType`/`creatorChannelId`、`threadName`、`createdAt`）。
- `src/store-schema.ts`：`thread_notify` 建表，键为 `(bot_uid, channel_type, channel_id)`，其中 `channel_type` 恒为 5（子区目标）；创建方类型在独立的 `creator_channel_type` 列上，限 2/5。
- `src/store-chats.ts`：`registerThreadNotify`、`getThreadNotify`、`clearThreadNotify`；`src/store.ts` 门面转发。
- `src/thread-notify.ts`（新）：通知前缀/消息 ID 组装、文本模板与截断常量（纯函数）。
- `src/host-tools.ts`：`notify` 参数、登记同步与 `notify_error`、`thread-delete` 清理、描述更新。
- `src/service.ts`：`notifyThreadOutcome` 与上列检测点接线。

## 错误与边界情况

- 通知投递失败（状态盘写入失败等）：只记日志，不重试；同一任务的再次触发点不存在（终态任务不会被再次 settle），不会重放。
- 优雅停止/重启期间的 settle 不回传（见「不包含」）；`/cancel`、`/interrupt` 与 `ask` 等待超时照常回传。
- 子区被删除且登记已清理：不再回传；若删除发生时有在途任务先完成，登记尚在，则按实回传（迟到但真实）。
- 创建方聊天已被删除或不可达：通知仍按 durable 路径接受，后续投递由既有 outbox 语义处理，不新增失败面。
- 通知进入创建方时该聊天有待回答问题：不在答案路径上，问题保持待答（与 `thread-dispatch` 的显式交接一致）。
- 通知以 steering 并入运行中回合时，创建方照常得到一行「已并入当前回合」回执（既有机制，正文前 20 字有界）。
- 回传回合保留全部 host tools：创建方助手可据此继续处理（包括再次 `thread-dispatch`），这是它对一条新输入的可见自主决定，不是机制递归——机制只保证同一任务至多一次通知、一次唤醒；对话式循环与任何普通任务一样由 owner 用 `/cancel` 或删除子区终止。
- 子区任务正文或结果超长：分别截到 200/1500 字符并带 `…`；`error_text` 缺失时状态行之后用固定的「（未记录原因）/任务已中断。」兜底。
- 多任务连续结束：每任务一条通知，条数有界于任务数，不做合批。
- `thread_notify` 中的 `threadName` 是创建时的服务端名称，后续改名不追溯。

## 验收标准

1. `create-thread notify=true` 创建后登记落盘（结果带 `notify: true`）；服务重启后登记仍生效：新任务结束仍会回传。
2. 子区任务 `completed`：创建方聊天恰好收到一条合成消息（消息 ID `notify:<任务 id>`），内容包含状态与任务结果摘要；创建方空闲时它成为该聊天的一个任务（一次唤醒），运行中且可接收 steering 时并入该回合。
3. 子区任务在运行中产生的 `failed`/`cancelled`/`interrupted`：各有一条对应语义的通知（失败原因 / 取消说明 / 中断说明）。
4. 服务停止/重启导致的终态（`stopping` 期间落成的 `cancelled`、`markInterruptedOnStart` 落成的 `interrupted`）不产生任何回传，创建方不新增回合或消息。
5. `notify` 省略或 `false`：不产生任何通知、不产生任何额外回合或消息；且清除该 `channel_id` 上残留的登记。
6. 同一任务至多一条通知；重启、重放后不重复。
7. `thread-delete` 删除子区后清理回传登记；清理失败不影响删除回执的成立。
8. 既有命令、派发与任务流程不受影响；`bun run typecheck` 与 `bun test` 全绿。

## 未决事项

无。
