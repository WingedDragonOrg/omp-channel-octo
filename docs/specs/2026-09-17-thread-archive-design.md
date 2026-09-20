# 子区归档与 merge 后自动归档

日期：2026-09-17（2026-09-20 更新：服务端端点已合并、部署状态与验收证据）

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
- 跨重启的持久化补做：排定是进程内一次性状态，进程重启即放弃（见「错误与边界」）。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 归档时机 | 工具内不即时归档，只「排定」；在该聊天任务回合末、回合输出与进度卡终态编辑排空后执行 | 任何消息都会把归档子区解档；即时归档会被紧随其后的回复解档 |
| 自动归档条件 | `thread-merge` 成功且默认 pipeline（工作区已删除，`summary.removed === true`）时排定 | 「合并完成即归档」对应默认路径；显式保留工作区通常意味着还要继续用这个子区 |
| 排定载体 | `ThreadArchiveScheduler`（`src/thread-archive.ts`）进程内 `Map<string, PendingThreadArchive>`（键为 `targetKey`），一次性（执行或放弃后清除） | 与 `pendingRestart` 同风格；不为一次性收尾新增持久化 |
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

### 归档调度（`src/thread-archive.ts`）与服务接线

- `ThreadArchiveScheduler` 独占 `pendingThreadArchives: Map<string, PendingThreadArchive>`（键为 `targetKey`，值为聊天目标 + `groupId`/`shortId`）及停止信号。
- `HostToolHost.planThreadArchive(target, groupId, shortId)` 接线到 `schedule`（写入或覆盖）。
- `handleInbound`：过权限门禁后调用 `cancel` 清除该聊天的待归档（新消息意味着会话仍在继续）。
- `runTask` 的 `finally`：在 `notifyThreadOutcome` 之后、`maybeRunDeferredRestart` 之前执行 `threadArchives.run(target, progress.tail)`：
  1. 取该聊天的待归档；没有、或服务正在停止则返回。
  2. `await progressTail`（该回合已排队的进度卡操作），再按 100ms 轮询、最长 10s（`THREAD_ARCHIVE_DRAIN_MS`/`THREAD_ARCHIVE_DRAIN_POLL_MS`）排空该聊天的出站队列（`outbox.flush` + `store.countPendingOutbox`），确保回复与卡片先送达。
  3. 每次尝试前确认排定仍在（排空期间的新消息可能已取消它）且服务未在停止；在则按重试策略调用 `transport.archiveThread`。
  4. 结束即清除排定；成功留一条 `[thread-archive] 已归档子区 <shortId> target=<key>` 服务日志；失败留一条 `[thread-archive] 归档失败 …` 服务日志并向发起聊天投一条 `归档失败：<单行 ≤200 字符>`。
- 服务停止时调用 `stop()` 唤醒重试等待；调度器在 `stopping` 时放弃待归档：不执行、不通知。

### 重试语义

- 第 1 次尝试立即执行；失败且为瞬时错误时等待 `THREAD_ARCHIVE_RETRY_MS`（5s）再试，最多 `THREAD_ARCHIVE_ATTEMPTS`（5）次尝试；永久错误立即结束。
- 等待期间收到停止信号（`stopped.promise`）即刻结束等待并放弃，不为一次重试等满间隔；等待用 `setTimeout`（`vi.useFakeTimers` 可推进）。

## 错误与边界

- 端点未就绪（裸 404 / 永久 4xx）：立即失败，一条有界说明（不重试、不影响合并结果）。
- 权限不足（服务端按 bot 线程删除同口径映射的错误）：若为 `store_failed` 类 400 按瞬时错误重试至多 5 次，最终失败仍只是有界说明。
- 投递排空超时（10s）：照常执行归档；若仍有残余消息稍后送达，可能把子区解档为 active（可接受，归档可重复调用补做）。
- 进程在排定后、执行前重启：排定丢失，子区保持 active；owner 可以再次触发归档（两端幂等）。
- 任务失败/取消/中断：排定照常执行（合并已经发生）。
- 排空期间或等待重试期间该聊天来新消息/新任务：清除排定，归档不再执行，也不发失败说明。
- 服务停止：等待立即结束、归档放弃（不发说明）。

## 验收标准

1. `thread-archive` 在群/子区会话按既有门禁可用，子区里省略 `short_id` 即当前线程；输出有界；重复调用只产生一次归档。
2. `thread-merge` 成功（`removed=true`）后自动排定归档：工具结果分别给出合并结果与归档排定；归档失败仅是发起聊天里一条 ≤200 字符说明，不改变 merge 结果。
3. 时序：本回合回复（与进度卡终态编辑）先送达、归档后执行（测试直接用夹具断言调用顺序）；回合末窗口内新入站取消排定。
4. 重试：瞬时失败总计 5 次尝试、间隔 5s（假定时器）；永久失败即刻；停止时放弃。
5. `bun run typecheck` 与 `bun test` 通过；主规格与 AGENTS.md 与实现一致；服务端端点部署后完成真机验收：建一次性子区 → `/thread merge` → 自动归档 → 清理。

## 验收证据

`tests/core-host-tools.test.ts` 6 例（真实 CoreService、真实 SQLite、夹具 transport/worker）：

- 「thread-archive schedules one archive that runs after the turn's reply is delivered」：子区里两次调用只排定一次，归档恰好发生一次且夹具记录到「回复已先送出」（`archiveOrder[0].sentBefore` 含本回合回复文本），群会话显式 `short_id` 同样可用。
- 「a default-pipeline merge schedules the thread's archive beside the merge result」：默认 pipeline 合并后工具结果含 `"archive":"scheduled"`，归档目标为该子区且在回复送达后执行。
- 「a transient archive failure retries five times five seconds apart and ends as one bounded notice」：假定时器下 5 秒未到不重试、过 5 秒重试一次，总计 5 次尝试后停止；聊天收到唯一一条 `归档失败：` 单行说明。
- 「thread-archive does not retry a permanent failure and reports one bounded line」：403 只尝试一次，推进一分钟也不重试。
- 「a new message in the same chat cancels the scheduled archive」：回合仍在运行时的新入站清除排定，后继任务开始即证明收尾已结束且零归档调用。
- 「a pending archive is dropped when the channel stops before it runs」：停在重试等待中时关停立即结束等待、放弃归档且不发失败说明。

真机探针（2026-09-20，bot 身份 octo-cli，REST）：在 `Omp-Channel-Octo开发` 群建一次性子区 `2101625888524210176` → `POST .../archive` 返回 gin 404（服务端未部署）→ `DELETE` 清理，群内无残留。

## 未决事项

- 部署：`im.deepminer.com.cn` 尚未升级到含 bot 归档端点的 octo-server 版本；部署完成后由本子区补做真机端到端验收（建一次性子区 → `/thread merge` → 自动归档 → 清理）。
