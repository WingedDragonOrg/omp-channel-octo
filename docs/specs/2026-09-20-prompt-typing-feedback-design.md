# typing 即时反馈（agent-attached 消息提前亮起）

日期：2026-09-20

## 背景与目标

用户反馈：发消息给 Bot 后 typing 没有立刻显示，而是等待一会才出现；用户提出「是否应该接收到之后就开始显示 typing？」。本子区派发任务：定位当前延迟所在阶段，评估并实现让用户及时看到输入状态的方案。

现状：第一个 typing POST 在任务实际开始时才发出（`src/service.ts` `runTask` 入口的 `startTyping()`，位于 worker 创建之前），此前的附件下载/转写阶段（整消息预算 120s）与接受/调度阶段完全没有 typing；密集多轮会话还会撞上服务端 90s/180s 节流死区，typing 最长约 90s 不可见，且 bot 侧无法观测（服务端一律回 200）。

目标：对确认会执行的消息，在权限门通过后尽早发出 typing，并在组装/排队/执行期间连续保持，直到任务终态熄灭。

## 当前时序与延迟定位（调查证据）

从 WS 帧到达到客户端显示 typing 的完整链路，逐阶段给出代码证据：

| 阶段 | 位置 | 成本/行为 |
| --- | --- | --- |
| WS 帧解密与归一化 | `src/octo/index.ts` `handleRecv` | 快；随后**所有入站消息经 `inboundChain` 串行处理**，且 RECV ACK 在 handler 完成后才发。一条含附件消息的 handler 慢（见下），同连接后续所有消息的门与 typing 都被整体阻塞 |
| 权限门 | `handleInbound` → `isAllowed` | SQLite 本地判定；群 owner 免@ 冷缓存时多一次 `GET /v1/bot/groups/{no}/mention_pref`（60s 缓存），通常 <0.3s |
| 待回答问题判定 | `answerPendingQuestion` | SQLite，快 |
| 消息组装（唯一组装点） | `materializeInbound` | 纯文本零成本；**含附件时逐条下载+语音转写：单条超时 60s、整消息预算 120s（`ATTACHMENT_DOWNLOAD_BUDGET_MS`），期间没有任何 typing** |
| 接受与调度 | `acceptTask` → `scheduleTask` | 绑定重解析（本地）、SQLite 事务、目录租约与聊天串行链；空闲时 <0.1s，忙时排队 |
| 任务开始 | `runTask` 入口 | `markStarted` 后 `startTyping()` 发出**首个** typing POST；随后才是群说明读取、session 目录、**worker 创建**、history sync、prompt。**worker 启动不延后 typing（startTyping 已先于 worker 创建）** |
| 服务端节流 | 上游 `Mininglamp-OSS/octo-server` `modules/bot_api/typing.go` | 按 (robot, channel_id, channel_type) 记 `typing_start`/`typing_count`（Redis，各 TTL 180s）：窗口开启后 ≤90s 内每个 POST 都派发 CMDTyping；90s 后静默丢弃（HTTP 仍 200）；开键过期（距开窗 180s）才允许新窗口，每计数周期至多 3 个窗口。**密集多轮会话（回合间隔约 90–180s）时后一回合的 typing 最长约 90s 不显示，这是「等待一会才出现」的最常见来源；bot 侧不可绕行、不可观测** |
| 客户端显示 | 上游 `Mininglamp-OSS/octo-web` `TypingManager.tsx` / `module.tsx` | CMD 监听器 `cmd=="typing"` → `addTyping` 即时显示；每条 typing 状态 8s 自灭（`Typing.start` `setTimeout 8s`），重复 CMD `restart` 续期；**同会话任意新消息到达即清除 typing**；回前台/重连 `resetAll` 全清 |

结论：worker 启动不是延迟源；可感延迟来自 (a) 附件组装期间无 typing（自身消息最长 120s，并串行阻塞同连接后续消息）、(b) 服务端 90s/180s 死区（外部约束，本仓库规格第 25 行已记录）、(c) 客户端「任意消息清除 + 3s 续期」造成的秒级空窗。本设计修 (a) 并把首个 typing 提前到「门后判定 agent-attached」；(b)(c) 属外部契约，按要求在验收汇报中与「已验证的发送时机」分开陈述。

## 范围

### 包含

- 按 (bot, chat) 的 typing 租约：门后判定 agent-attached 、进入组装点之前亮起，每 3s 续期；同一聊天任一时刻至多一条续期环。
- 租约接续：任务被持久接受时在同一条续期环上并入 `task:<id>` owner（先 acquire task 租约、调用方随后 release 消息租约，环不重启）；**任务排队等待（同聊天排队与跨聊天同目录等待）期间保持亮起**；任务终态（完成/失败/取消/启动失败/排队中取消）熄灭。
- agent-attached 命令路径（`/queue <文本>`、`/thread <任务>`、`/thread merge`、observing 注册命令）在各自交给 agent 前的同一点位亮起。
- `runTask` 入口的防御性幂等 acquire：任何未来把任务带进 `runTask` 的路径自动点亮（当前生产触达路径只有 `acceptTask` 的调度链；进程重启会把 queued 任务一并在 `markInterruptedOnStart` 转终态中断，不存在重启后重排 queued 任务的路径）。
- 行为测试（相对时序与路径开/灭）、`bun run typecheck`、`bun test`，主规格、根 `AGENTS.md` 同步。

### 不包含

- 绕行或改变服务端 90s/180s 节流（服务端无相关 API；客户端无「取消 typing」端点，熄灭语义见设计）；不承诺 typing 跨死区连续可见。
- 改变客户端 8s 自灭、任意消息清除、回前台全清的行为。
- 重构入站串行链（慢附件消息阻塞同连接后续消息：作为残留记录，不在本次范围）。
- 「排队中」与「执行中」的差异化提示（typing 不区分二者）。
- 为调查启动第二个抢占 Bot 连接的服务或事件消费者；合并/重启常驻服务。

## 已知约束

- typing 必须在权限校验后产生：未授权、被拒绝、Bot 自身消息、owner 认领、成员命令提示、不支持类型等路径一律不发。
- 完成、失败、取消、并发任务与保活/停止语义不变；「提前亮起后无法清除」由两兜底避免：客户端 8s 自灭/任意消息清除（服务端契约），租约 owner 集合与终态释放（本设计）。
- 续期间隔维持 3s（`TYPING_RENEW_MS`），每个 typing 请求维持独立 2s deadline；单次 POST 失败仅丢弃该次（与现状一致），不重连、不升级。
- 服务端 200 不区分「已派发」与「已丢弃」：任何测试只能断言本端发送时机，不能断言客户端显示效果。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 触发时机 | 权限门通过、判定 agent-attached 后**立即**发，进入附件组装/转写之前 | owner 拍板；准备与排队阶段不再被无关慢工作延后 |
| 排队等待期间 | 亮起（同聊天排队与跨聊天同目录等待一致） | owner 拍板「反馈最早」；typing 语义为「已受理在处理」，不承诺「正在执行」 |
| 追加（steer） | 不变：运行任务的续期环持续；命中 steer 的消息租约立即释放 | 该聊天已亮；用户消息到达清一次，≤3s+RTT 复亮 |
| 非 agent 命令（`/status`、`/new`、`/cancel` 等） | 不亮 | 命令秒内应答，typing 是噪音 |
| 待回答问题答复 | 答复消息本身不点亮；被唤醒的任务在接受时亮起 | 答复即输入的既有语义不变 |
| intercepting 注册命令 | 组装期间亮起，handler 完成后熄灭 | 生产环境注册表为空（仅契约 seam）；handler 自流，不接管任务生命周期 |
| 熄灭 | 任务终态、消息不落地、服务停止三处熄；另有每 tick 任务行状态校验兜底 | 防止「提前亮起后无法清除」 |
| 续期间隔 | 3s 不变 | 窗口内足以续住客户端 8s 气泡；更密只会加重服务端计数 |

## 设计

**TypingLease**（服务内，按 `targetKey(bot, target)` 单例）：

- 状态：owner 集合、renew timer、在途请求 abort。
- `acquire(owner)`：首个 owner 开环——立即 `POST /v1/bot/typing`（2s deadline，与现有 trackActivity 同款收集进 `activityOperations`），随后每 3s 续期；后续 owner 只加入集合，不开第二条环。
- `release(owner)`：最后一个 owner 离开时清 timer、abort 在途请求。
- 防御兜底：每次 tick 对 `task:` 前缀 owner 校验任务行状态（`store.getTask`），行缺失或终态自动释放（覆盖排队中 `/cancel` 等一切漏 release 路径）。`msg:` 租约不依赖兜底回收——它的释放由调用方 try/finally 硬保证（见「归宿」）。
- 服务停止：`stopInternal` 置 `stopping` 后 `acquire` 立即失效（no-op，切断新入口，含 host tool `deliverToChat` 等并发调用方）；全部租约的 `releaseAll` 固定发生在 `activityOperations` settle 之后、`store.close()` 之前（清 timer + abort 在途），tick 兜底不会打到已关闭的 store。

**点亮时机**（各路径在「组装点之前」一处）：

- `handleInbound` 未注册 slash：注册 handler 未命中后、该分支自身 `materializeInbound` 之前 `acquire("msg:"+messageId)`（该分支在 `answerPendingQuestion` 判定之前，是独立点位）。
- `handleInbound` 普通消息：`answerPendingQuestion` 之后、`materializeInbound` 之前 `acquire("msg:"+messageId)`（覆盖组装点期间最长 120s 的下载/转写；纯文本在同事件循环内完成，租约开销可忽略）。
- `/queue`、`/thread <任务>`、observing 命令：`releaseClaim` 后、各自 `materializeInbound` 前 acquire。
- `/thread merge`：`releaseClaim` 后、构造 merge 指令正文前 acquire（该路径没有组装点，不下载附件）。
- intercepting 命令：`materializeInbound` 前 acquire；命令完成（含 claim 去重早退、materialize 返回 null 或抛错、`handler.handle` 结束/抛错）后 release。

**归宿**（硬要求：每个 acquire 点位之后到归宿点的整段代码用 try/finally 包裹，finally 中「若本消息租约仍持有则 release」——规格不依赖出口枚举的完备性，WS 重投、拒绝抛错等一切出口都被 finally 覆盖；下列出口仍逐条列出以便测试对照）：

- `materializeInbound` 返回 `null`（附件拒收/超预算/组装失败/claim 重复消费）→ release。
- `materializeInbound` 或其拒绝回复的持久化抛错（如 `refuseAttachments` 写盘失败）→ release（finally 保证）。
- intercepting 命令的 `claimCommand` 去重早退（重投）→ release（finally 保证）。
- `acceptTask` 返回 false（重投去重）或抛错 → release。
- `appendToRunningTask` 命中 steer → release（运行任务的租约继续持有，环不断）。
- `acceptTask` **成功时在自身内部** `acquire("task:"+taskId)`（先 acquire 后 schedule），调用方随后 release 消息租约；owner 集合从 `{msg}` 经 `{msg,task}` 到 `{task}`，环不重启。`acceptTask` 是**唯一**的 task 租约入口，因此不经 handleInbound 的任务（`deliverToChat`：host tool / `notify:` 合成消息 / 超时问题的答复唤醒）排队期间同样亮起。
- intercepting 命令无任务归宿，`handler.handle` 结束后 release。

**执行阶段**：`runTask` 不再自建 typing 环——在 `markStarted` 成功后幂等 `acquire("task:"+taskId)`（正常路径 owner 已在集合，no-op；这是防御点：当前生产触达 `runTask` 的路径只有 `acceptTask` 的调度链，重启后遗留 queued 任务被 `markInterruptedOnStart` 转终态、不会被重排，但任何未来新触达路径在此自动点亮，无需再改本设计）；任务走到任一终态（完成/失败/取消/启动失败）时 `release("task:"+taskId)`。原有每个请求 2s deadline、失败吞掉的语义保留到租约实现中。

## 错误与边界情况

- 服务端死区：密集多轮下 typing 可能最长约 90s 不显示；通道不可观测，验收只承诺发送时机。
- 入站串行链：慢附件消息阻塞同连接后续消息；后续消息的 typing 在其 handler 开始前无法亮起（残留，见范围）。
- 排队误导：typing 亮 ≠ 正在执行（owner 已确认接受该语义）。
- 排队中 `/cancel`：任务行转 cancelled，下一 tick（≤3s）兜底释放；客户端另有 8s 自灭。
- 附件被拒（未启用/数量超预算/下载超时）：消息租约释放，不残留。`/queue` 空参数、`/thread` 空任务等用法拒绝发生在 acquire 之前，本就不产生租约（测试断言「零 typing」，而不是断言一次不存在的释放）。
- 首次任务 claim 去重失败（同消息重投）：`acceptTask` 返回 false，释放，无副作用。
- 同聊天同目标至多一条续期环：manager 按 `targetKey` 单例，owner 集合去重。

## 验收标准

1. 时序证据（本文件「当前时序与延迟定位」）：最早安全触发点 = 权限门通过且判定 agent-attached；可测边界 = typing 调用先于附件下载开始、先于 worker 创建。
2. 行为测试断言相对时序与路径开/灭（需要夹具扩展：共享 `FakeTransport` 记录 typing 调用序列）：typing 先于附件下载与 worker 创建发出；附件下载期间续期继续；权限拒绝/不支持/成员命令/非 agent 命令路径零 typing；附件拒收、intercepting claim 去重早退等一切出口无泄漏（归宿 try/finally 覆盖全矩阵）；`/queue` 空参数零 typing；steer 命中后消息租约释放且环不断；排队等待期间保持；任务终态熄灭；排队中 `/cancel` 由 tick 兜底释放。
3. `bun run typecheck` 与 `bun test` 通过。
4. 主规格（typing 契约条目）与根 `AGENTS.md` 同步；汇报区分「已验证的发送时机」与「未验证的客户端显示效果」。

## 未决事项

无。两条残留风险（服务端 90s/180s 死区；入站串行链下慢附件消息阻塞后续消息）已在「范围/错误与边界情况」记录为不解决项。
