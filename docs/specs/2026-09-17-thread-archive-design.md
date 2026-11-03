# 子区归档与 merge 后自动归档

日期：2026-09-17（2026-11-03 更新：宿主合并回执 barrier 与严格出站排空）

## 背景与目标

`octo_management` 增加 `thread-archive` 动作：归档当前群/子区的子区（与其他 `thread-*` 动作同口径，子区里省略 `short_id` 即当前线程），幂等。子区 `/thread merge` 合并成功后自动归档该子区（默认 pipeline）：合并完成即归档、无需手动归档；归档失败不改变 merge 结果本身。

## 已知事实

- 服务端 bot 归档端点已在上游 octo-server `main` 合并：路由 `POST /v1/bot/groups/:group_no/threads/:short_id/archive`（`modules/bot_api/bot_api.go` 线程路由组），处理器 `botArchiveThread`（`modules/bot_api/threads.go`，守卫 `protectAIContainerMutation`，与 bot 线程删除同口径），复用 `modules/thread/service.go` 的 `ArchiveThread(groupNo, shortID, operatorUID)`：幂等（已归档直接返回 nil）、`UpdateStatusFrom(active→archived)` CAS、权限 = 子区创建者或群管理员且父群活跃成员；`bot_api/thread_archive_test.go` 覆盖幂等、权限与路由。
- 归档子区收到任何消息都会自动解档：`modules/thread/api.go` 的 `onMessages` → `RecordMessageAndReactivate` 对子区频道里任何正文消息（无发送者过滤；bot 的 `sendMessage` 未设 `NoPersist`，webhook 只丢弃 `NoPersist`/`SyncOnce`）会把 status 改回 active。因此归档必须安排在该聊天回合最后一条回复送达之后。
- 部署状态（2026-09-20 实测）：本 bot 身份经 octo-cli REST 对部署服务器 `im.deepminer.com.cn` 调 `POST /v1/bot/groups/{g}/threads/{s}/archive` 仍返回纯 gin 404（`404 page not found`，非 bot API 错误信封），即**该实例尚未部署含该端点的服务端版本**；探针用的是一次性子区（创建 → 归档 404 → 删除），已清理干净。channel 本分支按上述契约实现与测试，真机端到端验收待服务端部署后补做。
- 服务端 4xx 分类：部署未升级时归档得到的是裸 404（无 bot 错误码），本 channel 按永久失败处理——不重试、立即给一条有界说明。

## 范围

### 包含

- 传输层 `archiveThread`（`src/contracts.ts`、`src/octo/index.ts`）。
- `octo_management` 新增 `thread-archive` 动作（含工具描述与参数说明）。
- `thread-merge` 成功后自动归档（默认 pipeline、工作区已删除的路径）。
- 回合末执行器：排定 → 该聊天回合输出（含进度卡终态编辑）投递排空 → 归档；有界重试与失败说明。
- 主规格、AGENTS.md 与测试同步。

### 不包含

- 服务端端点实现（已由上游合并，不在本分支）。
- 取消归档（unarchive）、批量归档、归档列表入口。
- 通用归档计划的跨重启持久化。宿主合并已有未消费的持久成功 outcome 时，沿既有恢复链重建回执和归档，不执行 Git；已消费 outcome 的进程内归档计划仍在停止时放弃。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 归档时机 | 工具内不即时归档，只「排定」；在该聊天任务回合末、回合输出与进度卡终态编辑排空后执行 | 任何消息都会把归档子区解档；即时归档会被紧随其后的回复解档 |
| 自动归档条件 | `thread-merge` 成功且默认 pipeline（工作区已删除，`summary.removed === true`）时排定 | 「合并完成即归档」对应默认路径；显式保留工作区通常意味着还要继续用这个子区 |
| 排定载体 | `ThreadArchiveScheduler`（`src/threads/thread-archive.ts`）进程内 `Map<string, PendingThreadArchive>`（键为 `targetKey`），一次性（执行或放弃后清除）；宿主命令先持久化稳定成功回执，再发布携带该 outbox ID 的归档计划 | 单一归档所有者；成功事实不依赖已删除的 cwd |
| 新消息 | 该聊天的新入站（过权限门禁后）清除该聊天的待归档 | 用户回来了就不归档；避免归档↔解档抖动 |
| 重试 | 仅瞬时失败（HTTP 5xx，或 bot API 把内部 500 包裹成 400 的 `err.server.bot_api.store_failed`）重试：最多 5 次尝试、尝试间隔 5s；永久 4xx 立即失败；停止时放弃 | 与既有 `deleteThread` 的瞬发失败认知一致（`src/errors.ts` 的 `isTransientServerFailure` 是两处共用的判定）；计时走 `setTimeout`，测试用假定时器推进，不真等 5s 壁钟 |
| 结果可见性 | 合并结果进工具结果与回复；归档成功只留服务日志（子区状态即证据）；归档失败向发起聊天回一条 ≤200 字符单行说明 | 归档成功后再向子区发消息会把它解档；失败说明只影响收尾，不改变 merge 结果 |
| 手动 `thread-archive` | 与自动路径共用同一排定/执行器 | 时机语义一致：即时归档同样会被本回合回复解档 |
| 幂等 | 重复排定覆盖同一键（一次执行）；服务端对已归档子区返回成功 | 两端幂等，重复调用无害 |

## 设计

### 传输（`src/contracts.ts`、`src/octo/index.ts`）

- `ChannelTransport` 新增 `archiveThread(groupId, shortId, signal?): Promise<void>`：`POST /v1/bot/groups/{g}/threads/{s}/archive`（无 body），路径由 `threadPath(groupId, shortId) + "/archive"` 组装，错误沿用 `OctoError`（`status`/`code`）。
- 不做 `freshConnection` 之类的额外旋钮：重试策略在服务侧，按固定间隔重试。

### 工具（`src/host-tools.ts`）

- `MANAGEMENT_BASE_ACTIONS` 增加 `thread-archive`；目标解析沿用 `threadId()`（显式 `short_id`，否则当前子区，群会话缺 `short_id` 报错）。
- 执行：`host.planThreadArchive(target, groupId, shortId)` 排定后返回有界回执 `已排定归档子区 <shortId>：本回合最后一条回复送达后执行`。
- `thread-merge` 成功且 `summary.removed === true` 时同样排定；工具结果 JSON 在合并结果旁给出 `archive: "scheduled"`（与合并结果分别可见）。
- `octo_management` 的工具描述与 `action` 枚举同步（含「归档排定到回合末执行」的说明）。

### 归档调度（`src/threads/thread-archive.ts`）与服务接线

- `ThreadArchiveScheduler` 独占 `pendingThreadArchives: Map<string, PendingThreadArchive>`（键为 `targetKey`，值为聊天目标 + `groupId`/`shortId`）及停止信号。
- `HostToolHost.planThreadArchive(target, groupId, shortId)` 接线到 `schedule`（写入或覆盖）。
- `handleInbound`：过权限门禁后调用 `cancel` 清除该聊天的待归档（新消息意味着会话仍在继续）。
- `runTask` 的 `finally`：在 `notifyThreadOutcome` 之后、`maybeRunDeferredRestart` 之前执行 `threadArchives.run(target, progress.tail)`：
  1. 取该聊天的待归档；没有、或服务正在停止则返回。
  2. `await progressTail`（该回合已排队的进度卡操作），再按 100ms 轮询出站队列（`outbox.flush` + `store.countPendingOutbox`），直到没有 `pending`/`unknown` 行；不能用一次 flush 返回或等待超时替代排空。投递退避和有界失败预算仍由 outbox 独占，归档等待不绕过它们。
  3. 每次尝试前确认排定仍在（排空期间的新消息可能已取消它）且服务未在停止；在则按重试策略调用 `transport.archiveThread`。
  4. 结束即清除排定；成功留一条 `[thread-archive] 已归档子区 <shortId> target=<key>` 服务日志；失败留一条 `[thread-archive] 归档失败 …` 服务日志并向发起聊天投一条 `归档失败：<单行 ≤200 字符>`。
- 服务停止时调用 `stop()` 唤醒重试等待；调度器在 `stopping` 时放弃待归档：不执行、不通知。
- 宿主 `/thread merge` 成功无需 agent 任务：命令持久化稳定回执后发布归档计划，服务收集该聊天残留的进度卡 tail 并启动同一执行器；指定回执必须为 `sent`，`dead`、缺行或未知状态都不能作为成功交付证据。
- 已有未消费宿主成功 outcome 的启动恢复在进度卡恢复、transport 启动完成后释放归档计划；回执使用原稳定 ID，工作树缺失不阻止结果交付，也不重跑 Git。普通停机仍取消进程内等待，不把归档失败回传父群。

### 重试语义

- 第 1 次尝试立即执行；失败且为瞬时错误时等待 `THREAD_ARCHIVE_RETRY_MS`（5s）再试，最多 `THREAD_ARCHIVE_ATTEMPTS`（5）次尝试；永久错误立即结束。
- 等待期间收到停止信号（`stopped.promise`）即刻结束等待并放弃，不为一次重试等满间隔；等待用 `setTimeout`（`vi.useFakeTimers` 可推进）。

## 错误与边界

- 端点未就绪（裸 404 / 永久 4xx）：立即失败，一条有界说明（不重试、不影响合并结果）。
- 权限不足（服务端按 bot 线程删除同口径映射的错误）：若为 `store_failed` 类 400 按瞬时错误重试至多 5 次，最终失败仍只是有界说明。
- 出站处于重试退避：持续等待队列排空；即使指定成功回执已经 `sent`，其他正文未排空也不归档。永久失败/预算耗尽由 outbox 标记 `dead` 后让出队首；成功回执本身为 `dead` 则不归档。
- 进程在普通计划排定后、执行前重启：进程内计划放弃；只有仍有未消费持久宿主 outcome 时按上述恢复链补做。合并成功事实与归档计划分离，补归档不能再次执行 Git。
- 任务失败/取消/中断：排定照常执行（合并已经发生）。
- 排空期间或等待重试期间该聊天来新消息/新任务：清除排定，归档不再执行，也不发失败说明。
- 服务停止：等待立即结束、归档放弃（不发说明）。

## 验收标准

1. `thread-archive` 在群/子区会话按既有门禁可用，子区里省略 `short_id` 即当前线程；输出有界；重复调用只产生一次归档。
2. `thread-merge` 成功（`removed=true`）后自动排定归档：工具结果分别给出合并结果与归档排定；归档失败仅是发起聊天里一条 ≤200 字符说明，不改变 merge 结果。
3. 时序：本回合回复（与进度卡终态编辑）先送达、归档后执行（测试直接用夹具断言调用顺序）；回合末窗口内新入站取消排定。
4. 重试：瞬时失败总计 5 次尝试、间隔 5s（假定时器）；永久失败即刻；停止时放弃。
5. 已送达成功回执、其后仍有待投递正文的场景：推进超过旧 10 秒窗口仍不归档；正文确认后归档一次，新授权入站或停止则放弃。
6. `bun run typecheck` 与 `bun test` 通过；主规格与实现一致；真实 Git/worktree + 本地 transport/worker fixture 验证命令、清理、回执、卡片与归档顺序；线上服务端异步消息监听及真实客户端状态另行验证。

## 验收证据

`tests/core-host-tools.test.ts` 6 例（真实 CoreService、真实 SQLite、夹具 transport/worker）：

- 「thread-archive schedules one archive that runs after the turn's reply is delivered」：子区里两次调用只排定一次，归档恰好发生一次且夹具记录到「回复已先送出」（`archiveOrder[0].sentBefore` 含本回合回复文本），群会话显式 `short_id` 同样可用。
- 「a default-pipeline merge schedules the thread's archive beside the merge result」：默认 pipeline 合并后工具结果含 `"archive":"scheduled"`，归档目标为该子区且在回复送达后执行。
- 「a transient archive failure retries five times five seconds apart and ends as one bounded notice」：假定时器下 5 秒未到不重试、过 5 秒重试一次，总计 5 次尝试后停止；聊天收到唯一一条 `归档失败：` 单行说明。
- 「thread-archive does not retry a permanent failure and reports one bounded line」：403 只尝试一次，推进一分钟也不重试。
- 「a new message in the same chat cancels the scheduled archive」：回合仍在运行时的新入站清除排定，后继任务开始即证明收尾已结束且零归档调用。
- 「a pending archive is dropped when the channel stops before it runs」：停在重试等待中时关停立即结束等待、放弃归档且不发失败说明。

真机探针（2026-09-20，bot 身份 octo-cli，REST）：在 `Omp-Channel-Octo开发` 群建一次性子区 `2101625888524210176` → `POST .../archive` 返回 gin 404（服务端未部署）→ `DELETE` 清理，群内无残留。

### 2026-11-03 调查与修复 SDD

- **目标与边界**：默认宿主合并清理工作区后，稳定成功回执、正文和终态卡片先于归档；`remove:false`、失败和资格拒绝不排定。保持新授权入站取消、停止放弃、瞬时错误最多五次尝试及永久错误有界通知；归档仍只由 `ThreadArchiveScheduler` 执行。
- **现场事实**：来源子区 `2117451560106594304` 的服务日志在 13:37:39 记录归档调用成功；SQLite 中成功回执在 13:37:39.129 标记 `sent`，前一助手正文最后确认时间为 13:35:45.046，下一条本地 outbox 投递在 14:14:23.030。持久成功事件目标提交为 `551ec0ffec04da25e9e0fd05d7ceee515f6b456a`，归档字段为 `scheduled`，不是最终归档状态。以上能排除“未调用归档”，不能证明客户端持续处于归档状态，也不能把 14:14 的新命令当作此前用户观察的原因。
- **本地可复现缺口**：旧 `drainChatOutbox` 等待十秒后继续归档，即使仍有 `pending`/`unknown` 正文。回归以真实 SQLite 保存已送达回执和未送达正文，推进十二秒后观察到归档调用，证明指定回执 barrier 不能替代整个聊天排空。此缺口不等于已经证明了现场因果。
- **实施**：移除排空的超时放行，保留 100ms 轮询、outbox 自身退避/失败预算和调度器取消信号；不新增归档执行器、Git 重试、工作树重建或成功事件。
- **消息身份**：同一消息的 durable claim 与稳定 operation ID 防止重跑；14:14 的不同消息是新命令，不映射为旧 merge 的重放。旧成功事件保持不变，工作树已清理时新操作仍按既有资格检查拒绝。
- **线上限制**：HTTP 发送确认与本地队列排空不证明服务端消息监听器已完成解档/统计，也不证明真实客户端已经刷新。需服务端处理时序或状态证据才能归因；本地 fixture 不替代该证据。不在问题子区补发消息、手工归档或再次合并。
- **服务端边界证据**：[`handleMessageNotify`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/webhook/api.go#L268-L331) 在消息事务提交后通知监听器；[`Thread.onMessages`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/thread/api.go#L47-L108) 对普通子区正文执行解档/统计。该 webhook 入口与发送 ACK 分离，存在 ACK 后才处理消息的时序可能；缺现场两入口轨迹，不能将此可能性写成已证实根因。
- **回归证据**：新增出站等待的送达、取消、停止三个边界；原实现推进十二秒时错误调用归档，修复后等待送达才调用，取消/停止零调用。既有宿主恢复用例改用已不存在的 cwd，确认卡片恢复先于归档且合并端口零调用。专项六文件 102 项及真实 Git fixture/重启/lease 迁移四文件 61 项通过。
- **离线生命周期证据**：`bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两场景均通过；恢复执行与排队任务各两项，session overlaps、duplicate effects 和 disruption notices 均为零。
- **真实 Worktrunk smoke**：抛弃式仓库使用实际 `wt v0.80.0`，隔离 HOME/XDG/Git 配置，无真实模型或 Octo 请求。实际顺序为「进度卡创建 → 终态编辑开始 → 终态编辑送达 → wt merge 开始/结束 → 成功回执送达 → 归档完成」。验证 main 内容与 HEAD 更新、worktree/分支清理、同消息重投仅一次 merge/回执/归档；宿主 merge 未增加 child worker 或任务。既有 Git integration fixture 另覆盖冲突及 hook 失败后的修复与重试。
- **完整检查**：`bun run typecheck` 通过；`bun test` 共 1167 项、85 文件全部通过。


## 未决事项

- 现场 2026-11-03 已有归档调用成功日志；仍缺服务端解档/统计处理时序及客户端状态刷新证据，不能仅凭本地成功返回解释用户所见。验证这些状态需要独立线上只读证据；本任务不改变真实问题子区。
