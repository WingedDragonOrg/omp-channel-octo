# 非阻塞入站与受管后台执行设计

日期：2026-11-04
状态：实施中；不部署、不重启常驻 Bot。整体验收尚未完成，仅记录实际观察到的分项结果。

## 用户原始需求

以下为 2026-11-09 会话恢复需求，按对话顺序记录；其中第 4 条细化了待处理消息的处理方式。

1. 「为什么现在无法对话？使用 /new 也无法正常运行？」
2. 「为什么会有这个问题？不应该出现这个情况才对，重启竟然无法解决吗？」
3. 「进程或绑定无法确认 那我新的/new 应该能正常执行才对，执行完就正确了，现在为啥不可以？」
4. 「类似情况都看下」；「卡主的仅提示，用用户自己觉得他在发生一次还是忽略」
5. 用户对本节及 §12 对应的恢复设计回复「ok」予以确认。
6. 对旧进程仍运行或退出无法确认的处理补充：「有旧 worker 应该明确提醒等待完成，或/stop /cancel 取消后再进行」；本条细化第 3 条的恢复条件。

原有入站接收与排序设计保留；本次补充未知会话切换后的显式恢复合同，§12 对应本节需求。

## 1. 目标与已确认边界

**铁律：入站 handler 不等待任何业务动作完成。** 它只做有界的协议内存操作，把消息交给服务拥有的入口，然后立即返回。资源竞争、文件系统探测、网络、子进程、模型、回执发送与归档都不在 handler 调用栈中启动或等待。

owner 已选择的接收语义：**完整权限门通过前不持久化消息正文；后台完成授权和持久受理后，每条消息独立 ACK。** handler 返回不等于 ACK，不等于任务执行完成。尚未 ACK 且尚未持久受理的消息依赖现有服务端重投路径。

保持现有 owner、允许列表、mention、免@、成员命令拒绝、首次认领、ask、线程派发与 exact-session 合同。普通任务仍只按聊天和精确 session 调度，不增加 cwd、父子目录、父子聊天或共同仓库串行锁。commit/merge/remove 继续保护实际副作用路径及 canonical worktree roots。

## 2. 故障事实与证据限度

### 2.1 已确定的传播机制

当前源码及事故前后对应 Git 版本中的 `src/octo/index.ts` 相同：`OctoTransport.handleRecv` 的 `inboundChain` 为整个 Bot 共用，不按聊天区分。

```text
RECV
  → 全局 inboundChain
  → await CoreService.handleInbound
  → await processInbound
  → await handleCommand
  → await builtin handler
  → await executeHostThreadMerge
  → 等资源 / wt 与 hooks / 结果持久化 / 命令回执 flush
  → 返回后才 RECVACK
```

关键位置：

- `src/octo/index.ts:1235-1244`：全局串行与 handler 返回后 ACK。
- `src/service.ts:1109-1117,1205,2091-2141`：从 transport 一直等待 builtin 完成。
- `src/commands/thread-commands.ts:508-543`：命令等待完整 host merge。
- `src/service.ts:4020-4067`：`acquireThreadMergeLease` 无限、可取消地轮询资源。
- `src/service.ts:2119`：合并等特定命令还等待本聊天 outbox flush。

因此一个 merge 等待就足以阻止其后所有聊天的普通文字、查询与控制命令进入业务路由；不要求两个 Git merge 已同时运行。`/cancel` 虽然有取消 controller，也到不了调用它的 handler。原有控制命令不进入成员/steering 等业务串行链的约定，被更外层 transport 全局链抵消。

### 2.2 只读现场时间线

来源为本机 channel 日志、真实状态库的只读一致性副本、Super/进程信息、`main` 分支 reflog 和 Worktrunk 命令日志。正文、凭据与完整用户查询不作为证据输出。

| 时间（本机 UTC+08:00） | 观察 |
| --- | --- |
| 15:49:20—23 | 日志记录服务重新启动。 |
| 16:16:25 | `main` 被 wt 更新到 `222041b`，但没有相应的服务重启记录；磁盘更新不等于旧进程加载更新。 |
| 16:32:55 | 子区 `…5552384` 接受一次 `/thread merge`。 |
| 16:33:36 | 该输入产生稳定 ID 的 merge repair task，证明这一次走到了实际执行失败的修复分支。 |
| 16:37:30 | 主群接受任务 `5b30a1d5…`；停机日志表明它在 16:46:38 仍需挂起。 |
| 16:37:52 | 子区 `…8980224` 接受精确 `/thread merge`，消息 ID `2117899114107539456`。此后到关停没有新的入站 claim。 |
| 16:37:52—16:46:38 | 同一状态副本中有 31 条 outbox 记录成功发送，最大创建至发送延迟 5.074 秒；不能把整个区间解释为事件循环或 SQLite 完全停住。 |
| 16:46:38 | 同秒出现 969 次 `Octo inbound handler failed`，其中 933 个不同消息 ID；包含重投和非业务消息，不等于 969 条用户命令。 |
| 16:46:39 | 日志记录重新启动。 |
| 16:48:00 | 另一条 merge 被接收；后续部分消息直到 16:52:17 才形成入站记录。 |
| 16:52:15—17 | wt 日志记录一组 pre-merge 检查成功；main reflog 在 16:52:16 更新至 `885c7f0`；随后有归档成功记录。 |
| 16:52:18—19 | 再次关停并启动；现场已经变化。 |
| 16:52:40 | `/status` 的入站 claim 完成，对应回执当秒为 `sent`。 |
| 16:53:25 起 | 本调查子区普通输入已被接收并执行；只读 host status 返回 PID 58757、queued_tasks=0、blocked_cwds=0。 |

事故前已运行的进程没有不可变的构建指纹，不能把当前 HEAD 当成事故进程的精确全量版本。[INFERENCE] 结合启动时间与 main reflog，15:49 进程很可能加载当时的 `7ce1147`。已逐一核对 `7ce1147`、`222041b`、`885c7f0`：transport、thread command 和 worktree 关键文件哈希相同。另直接提取并读取 `7ce1147` 历史 blob：`service.ts:3732-3779` 同样无限、可取消地轮询 merge acquisition；`task-scheduler.ts:1109-1183` 同样在实际 source/target 资源、活动使用者与较早队列冲突时返回未获取。因此全局入站与资源等待的连接并非只存在于当前版本；当时具体是哪项占用仍未知。

### 2.3 尚不能归因的具体阶段

16:37:52 那次命令没有保留下来的 host merge lease 或成功事件；事故进程及当时的 wt/Git 子进程已不在现场。当前 lease 表为空，不能倒推出当时一定没有启动进程。

项目确实配置并发 `bun run typecheck` 与 `bun test` 的 pre-merge hooks；wt 日志确实存在分钟级检查记录。但日志按命令结束记账，缺少某次记录不能证明 hook 从未开始。没有证据证明此次事故是 Git index/rebase 锁死、SQLite 长事务、某个 hook 卡死或出站全局堵塞。

**确定的架构根因是把长业务等待接到了全局接收完成条件上。** [INFERENCE] `acquireThreadMergeLease` 等实际目标资源占用，是现场的合理候选等待点；具体等待阶段不能仅靠现有历史记录定案。归档本身由服务追踪的独立 operation 执行；其 receipt/card/outbox barrier 必须保留，但不得成为入站完成条件。

### 2.4 本次隔离运行证据

`bun /tmp/omp-merge-inbound-chain-smoke.mjs` 已实际执行：真实加密 Octo fixture、真实 transport/CoreService/WorktreePort、两个真实 linked worktree、同一 main 目标和实际运行的可控 pre-merge hook。wt 命令为明确标识的本地 wt-compatible Git pipeline，不是已安装的 Worktrunk CLI；没有调用真实模型或线上 Bot。

- 第一合并 hook 被持有 500ms 时，第二合并、第三聊天 `/status` 和普通文字均已送入加密连接，但只有第一合并进入 handler；ACK=0，查询回复=0。
- 释放后，两次合并依序完成，main 有基线加两次合并共 3 个提交，两个临时 worktree 被移除，ACK=4/4；status 与普通查询回复都在释放后出现。
- 这证明真实 Git/hook 等待能经全局入口传播；不代表两个 merge 进程同时执行，也不宣称重现了事故中那次 merge 的具体阻塞阶段。

第二场实际执行 `bun /tmp/omp-merge-direct-contention-smoke.mjs`，直接并发调用 CoreService，绕开 transport 链作对照。第一合并 hook held 时，两条 merge 都进入 CoreService，但只有第一条到达 WorktreePort merge；独立 query repo 的 `/status` 与普通查询在释放前分别收到回复。释放后第二 merge 才进入 port，两次合并成功。此场确认同目标资源互斥与查询可运行可以同时成立；ACK 不适用，也不代表 transport 已修复。两场均在约 2.5 秒内结束并清理临时资源。


## 3. 方案选择

采用三层结构，复用现有业务调度器、去重、worker/process ledger 和 outbox；新增的只有入口交接与已授权消息的短期持久交接记录。

```text
WebSocket / 本地合成输入
  → 同步 receive：登记内存投递、返回 ackReady receipt
  → 后台权限与短受理
      ├─ 确定忽略/拒绝：按现有规则消费，ackReady 完成
      └─ 已授权：写 ingress_jobs 并冻结用途/屏障
          → 后台完成所需正文准备，持久化可恢复输入后 ackReady 完成
          → 后台业务调度交接
              ├─ 控制/查询与 ask 答案
              ├─ 普通文本/附件/steering/排队任务
              └─ 原生命令、会话操作、merge/remove/fork/commit
                  → 各领域现有受管 operation / scheduler / ledger
                  → 持久 outcome / outbox
                  → 按聊天独立发送，归档另等真实排空
```

`ackReady` 是“可以确认这条输入已被可靠接管或确定消费”的信号，不是业务结果。业务完成信号不能再通过这个接口返回给 transport。

## 4. 明确的模块与接口边界

### 4.1 Transport

`ChannelTransport.start` 的消息回调改为同步返回接收票据：

```ts
interface InboundReceipt {
  ackReady: Promise<void>;
}

(message: InboundMessage) => InboundReceipt
```

`OctoTransport.handleRecv` 完成既有有界解码与归一化后，调用回调、登记本条票据并立即结束。删除全局 `inboundChain`。不以按聊天的 transport chain 替代它。

每条票据独立监听 `ackReady`：成功后仅在原连接仍为当前有效连接时发送该消息的 RECVACK；失败不 ACK。按消息身份逐条确认，不得用后一个消息的完成代替前一个消息的确认。断线重投依靠持久键去重，不把旧连接票据迁移成新连接 ACK。上游 [WuKongIM `399e269` 的 `Runtime.Recvack`](https://github.com/WuKongIM/WuKongIM/blob/399e269b181374f6308758653896ccc2c8cb6ca0/internal/runtime/delivery/runtime.go) 明确清除一个 exact pending identity；这是协议实现佐证，不是线上所用 IM 构建的证明，实施验收仍须核对目标版本。

票据集合归 transport 生命周期所有，所有 rejection 都有消费者。`stop` 封闭入口后等待已中止的短交接清理；不等待模型、Git、outbox 或归档结束。真实业务 operation 的停机责任在 CoreService。

### 4.2 IngressCoordinator

新增 `src/inbound/ingress-coordinator.ts`，唯一负责：

- 同步登记 envelope、投递顺序及每条 `ackReady`；实际后台工作在回调返回后调度。
- 对同一可信来源投递键共享在途受理结果；WebSocket 使用 `(bot, target, messageId)`，Bot 事件使用 `(bot, eventId)`。
- 按现有逻辑后台完成权限判定、首次认领、开通口令及静默拒绝；权限未通过不写正文交接记录。
- 已授权消息写入交接记录；只有正文已可恢复、或确定没有正文准备需求时才完成 `ackReady`。附件、转写和关联媒体的准备仍全部在后台，不返回 handler 调用栈。
- 未开通群/子区 owner @ 消息仍是确定消费且不建立 ingress job；节流开通卡交给 CoreService 跟踪的异步 operation，卡片 HTTP 不得阻塞同聊天 admission 或 `ackReady`。
- 入口封闭、未受理投递的取消、已授权待处理记录的恢复与服务活动计数。

它不拥有 Git 锁、worker、模型策略、ask 状态、任务执行或卡片内容。`CoreService` 继续拥有业务编排与领域 operation；不把原 `processInbound` 换个名字直接从同步接收回调启动。

后台执行仍须使用异步网络、文件与受管子进程 API；把长 `spawnSync`、同步文件遍历或 CPU 密集工作包进 Promise 不算非阻塞。SQLite 仅承担短本地事务，事务内不得进行网络或等待子进程。

### 4.3 业务处理

将现有 `processInbound` 拆成后台授权/受理和已授权业务处理。生产入口为同步 `receive(message): InboundReceipt`；显式 `submitInbound` 只等待持久受理，不等待长业务完成。旧 `handleInbound` 不保留兼容路径，所有原直接调用者按所需语义迁移到这两个入口。

已授权 envelope 不意味着已创建模型任务。只有现有 `acceptTask` 或 steering/ask/命令的最终消费事务，才能决定消息用途。

- 附件下载、转写、成员与说明读取、history backfill：业务准备阶段。
- `/new`、`/reset`、`/move`、`/switch`、`/btw`、`/fork`、`/thread merge/remove`、native commit、goal RPC：各自受管后台 operation。
- 自定义 command handler：后台执行，保持 intercept/observing 既有语义。
- 控制回执：写入 outbox 后可完成本地处理，不等 HTTP 发送成功。
- merge 成功：先持久 outcome 和稳定回执，再结束 merge 业务；归档继续等 receipt `sent`、正文和终态卡排空。

不保留旧的“await handler 完整业务结束才完成接收”兼容入口。

## 5. 持久交接与 ACK

### 5.1 `ingress_jobs`

在现有 SQLite 中增量创建 `ingress_jobs`。正常行只保存通过完整权限门、尚未完成领域交接的输入；取消尚未授权候选时，允许写没有正文的身份 tombstone，见 §6.2。

| 字段 | 合同 |
| --- | --- |
| `id` | SQLite 自增受理顺序；同聊天 ordered 消息在授权受理时保持原到达次序。 |
| `bot_uid, source_kind, source_id` | 唯一投递键。`recv` 使用真实聊天与服务器 message ID；`bot-event` 使用 event ID；合成输入使用调用方既有稳定身份。来源类型由可信入口赋予，正文不能伪造。 |
| `channel_type, channel_id, message_id` | 原聊天及消息归因；卡片点击的 message ID 不是事件去重键，同卡的不同事件不能互相覆盖。 |
| `payload_json` | 已授权的来源判别联合：归一化 InboundMessage，或已校验的 card action/event 快照。准备前可含附件 URL/元数据，不存 Bot 凭据、协议密文或环境变量；身份 tombstone 此列为 NULL。 |
| `route_snapshot_json` | 受理时判定的消息用途：命令 action、普通输入或精确 question ID 集合；明确保存“当时没有待答问题”。同时记录 session 代次及较早会话切换 job 的屏障身份。 |
| `prepared_text, spool_paths_json` | 原有 materializer 产生的完整正文和已持久本地媒体路径；恢复不得重新下载已确认保存的媒体或换取新的正文。 |
| `ack_ready` | 持久的布尔状态；仅有原始 URL 的 preparing 行为 false，不能因“已有 job”就提前 ACK 重投。 |
| `lane` | `ordered` 或 `control`。 |
| `state` | `accepted`、`preparing`、`dispatch-intent`、`handed-off`、`unknown`、`cancelled`。 |
| `operation_checkpoint_json` | waiting 命令的阶段、expected binding/session 和已安装屏障所需的原始参数；只记录当前命令已确定的信息，不推测目标路径。 |
| `consumer_kind, consumer_id` | task、steering、ask、可恢复领域 operation 或稳定 outbox 的精确身份；pending command claim 本身不算消费者。 |
| `created_at, updated_at` | 受理与状态更新时间。 |

`schedule_order` 由 `scheduler_order_state` 持久分配，贯穿 ingress job 与直接创建的任务（包括 watch 输入）；重启按同一顺序域恢复，不能只比较各表自增 ID。媒体准备失败等待重投时，保留同聊天顺序屏障，防止较晚正文越过。

`accepted/preparing` 尚不允许非幂等原生动作。进入 native RPC、远端创建、Git 写进程等既有副作用边界前，先持久 `dispatch-intent`，并按具体领域协议建立 ledger/消费身份。状态不明时不重跑原始命令。

### 5.2 媒体与 ACK 的可恢复条件

不假设附件 URL 的有效期，也不增加刷新 URL 的推测路径。凡既有物化流程需要下载、转写或历史媒体关联，均在后台完成正文准备；本地文件确认落盘、`prepared_text/spool_paths_json` 与 `ack_ready=true` 提交后才允许 ACK。等待下载不会挡住其他接收或控制输入。

`ack_ready=false` 时中断：仍不 ACK，保留重投及未派发准备的恢复资格；在途下载按原清理规则收尾。准备失败且按原合同拒绝输入时，先把对应失败回执持久化并确定消费，再 ACK，不能静默丢掉媒体。已 ACK 但尚未交接的 job 所引用 spool 不得被清理器删除；交给 task 后由既有任务/附件归属继续管理。

枚举 spool 的目录时只有 `ENOENT` 可视为空；其它文件系统错误必须使准备失败并保持不可 ACK。若 job 已持久进入 `cancelled`，只清理本次准备新建的文件；停机中断而仍可恢复的 spool 继续保留。

### 5.3 原子交接与 waiting 命令

任务执行仍在 tasks，进程在 worker/host merge ledger，问题在 questions，投递在 outbox。交接事务同时完成既有业务去重、精确消费者发布和 ingress 状态转换。

**`handed-off` 的前提是消费者已有独立恢复责任。** 以下情况不能标记 handed-off 或清掉 ingress 正文/checkpoint：

- 只有 `command_state=pending`，后台 merge 尚在规划或等待会话、host ledger 尚未建立。
- `/new`、`/reset`、`/move` 仍在等 session 或候选 worker，领域记录尚不足以恢复切换顺序。
- 只创建了内存 Promise、AbortController 或 scheduler reservation。

这些命令保留 `accepted/preparing` 与 operation checkpoint。恢复先按受理顺序重建其 session/资源意图、会话切换屏障，再放行较晚普通输入；确认没有 dispatch-intent 才继续等待/准备。由精确 ingress job 身份恢复已归属的 pending claim，不能再调用公共 `claimCommand` 把它当作重投拒绝，也不能让任意重复消息取得恢复权。

实际副作用已经交给可恢复领域 ledger 时可以交接；没有此消费者的命令必须由 ingress 继续持有，直到 outcome/稳定回执已持久化。完成交接后移除重复 envelope 正文；交给 task 的记录保留不含正文的受理路由快照，作为初始化会话和恢复顺序的精确来源证明。身份去重沿用既有 inbound/领域记录。

### 5.4 崩溃边界

| 边界 | 恢复行为 |
| --- | --- |
| 只在内存、未过权限门 | 不 ACK；不留正文；等待服务端重投。 |
| 授权完成、交接事务失败 | `ackReady` 拒绝；不启动副作用；等待重投。 |
| job 存在，但 `ack_ready=false` | 不因重投命中就 ACK；继续同一准备或持久完成失败消费。 |
| `ack_ready=true`、ACK 丢失 | 重投关联同一 job 或最终消费记录；不下载第二份媒体、不重复执行。 |
| waiting 命令已 ACK、没有 dispatch-intent | 从 ingress checkpoint 重建意图/屏障并继续等待；不依赖 pending claim 自行恢复。 |
| 已完成 task/ask/steering 消费事务 | 依据精确消费者接续，不重新判定消息用途或再创建任务。 |
| 有 `dispatch-intent`，副作用是否开始不确定 | 交具体领域 ledger 恢复；无可靠结果则 `unknown`，不自动再执行。 |
| 已知业务 outcome，回执未发出 | 只恢复稳定 outbox/归档，不重复业务。 |
| 已持久取消身份、原正文尚未授权或重投 | 只确定消费，不再准备或执行，不把旧输入恢复成新工作。 |
| 旧库已有 pending command、没有新交接记录 | 不扫描正文自动重排；只按既有精确领域 ledger 恢复，不能证明未执行则保持未知。 |

## 6. 顺序与控制旁路

### 6.1 两种顺序，不设全局业务链

同聊天 `ordered` 消息的权限判定与受理保持到达顺序，互不拖住其他聊天。控制消息独立调度自己的权限门，不排在较早 ordered 消息的免@查询后面；其自身没有 mention 时仍须等待自己的免@结论，不跳过授权。持久取消对较早尚未授权候选的覆盖见 §6.2。

后台 `ordered` 队列只保证本聊天消息的业务受理顺序，不把完整长命令 lifetime 串成下一条消息的受理前提。长命令同步安装其已有调度意图/顺序屏障后，交给 service-owned operation，入口继续处理。

- 普通文本、附件、`/queue <正文>`、`/thread <任务>`、未知 slash：保留本聊天任务/steering 输入顺序。
- Worker 明确拒绝 steering（本地未运行/未流式等前置拒绝或 RPC `success: false`）时按原任务路径持久交接；dispatch-intent 后的 transport、timeout 或畸形响应等不确定结果保持 `unknown`，不得再创建任务。
- `/new`、`/reset`、`/move`：保持会话切换屏障；较晚的普通输入不能越过未完成的切换而误用旧绑定。屏障不挡查询和取消。
- 独立 `/commit` 与 `thread-delete` 保留原有资源保护语义。`/thread remove`、`/thread merge`（包括自动提交）只保留当前聊天与精确 session 顺序，不因其他聊天任务、相关目录/仓库活动或未确认进程账本等待/拒绝，也不以等效路径锁替代；保留取消，并验证持久工作区归属/实际路径。无关聊天和 worktree 工作可并发，Git 冲突与部分效果仍可能发生。
- fork：源绑定快照与创建意图按顺序取得，后续准备在后台继续；不以完整 fork 生命周期阻塞源聊天普通执行。

### 6.2 控制入口

`/cancel`、`/stop`、`/interrupt`、`/commit stop`、goal pause/drop、watch stop，以及 `/status`、`/help`、只读队列/策略查询走独立控制受理。`/restart` 只登记延期意图，空闲判定必须计入未交接 ingress job 和受管 command operation。

控制旁路只跳过不相关的业务等待，不跳过权限、精确目标、持久取消意图或已有命令自己的模型选择次序。需要 RPC/探测/Git 读取的查询也在后台运行；该查询自己的等待不能阻止另一条控制输入被受理。

`/cancel` 先持久写停止意图，再取消目标 job/operation。取消未派发 merge 可以停止等待；已启动进程必须等待整个受管进程组退出确认，不能把“已接收停止请求”报成“已停止成功”。停止结果未知时保留 watchdog 和 ledger，禁止自动重放。

取消范围沿用各命令既有语义：例如 `/interrupt` 不清掉其本应保留的普通排队文本。对范围内先到但尚未完成授权的内存候选，control 受理事务按稳定投递键写 `state=cancelled`、`payload_json=NULL` 的身份 tombstone，并中止对应内存准备。旧消息重投仍命中此身份，不会在取消后复活。查询消息及既有取消范围之外的输入不受影响；没有写入此身份之前不得宣称其已取消。

`/cancel` 与 `/commit stop` 的持久受理事务同时提交候选身份 tombstone；取消问答时快照精确的待答 question ID，不能在恢复或 handler 中重新匹配后来出现的问题。已进入 `dispatch-intent` 的动作不被盲目取消：只给该 job 写取消请求；steering 明确拒绝且 task 尚未原子交接时可取消其回退，若交接先提交则由取消事务按既有 task 身份停止；结果不确定时仍保持 `unknown`，绝不重放。配置卡片 action 不属于普通任务取消范围。

`/interrupt` 在 `dispatch-intent` 前写 task stop intent；goal pause/drop 在此前撤销对应续跑授权并取消待运行 continuation；watch stop 在此前持久停止所选 watch。新输入取消 thread archive 的时点是可靠受理，而非仅建 preparing 行或尚未可恢复的媒体下载。

文字 ask 答案的用途在授权受理事务中冻结到 `route_snapshot_json`：精确 question ID 集合或明确的“无待答问题”、当时 session 代次与较早切换屏障。恢复只消费这个集合，绝不重新查询“当前问题”作匹配。Q1 已答、失效或被 `/new`/`/reset` 作废时，按 Q1 既有 CAS/关闭原因与失效通知规则确定消费，不能改答 Q2 或变成普通任务；生命周期 ended 的晚答仍沿用原合同。快照为无问题的普通输入也不能去回答后来才打开的问题。答案不排在 merge 执行后，但仍遵守较早 deliberate session switch 屏障；thread-dispatch 和系统输入始终不走答案路径。

### 6.3 卡片与 Bot 事件

WebSocket 的 RECVACK 与 Bot event queue 的游标 ACK 是不同协议。Bot 事件仍只提交连续已消费的游标，不借本设计越过未可靠受理的事件。

命令卡片动作在校验 owner、聊天、代次和 action ID 后，以 `(bot_uid, bot-event, event_id)` 持久交给同一后台命令路径；轮询 handler 不等待模型探测、worker 或卡片 HTTP 发送。卡片 message ID 仅用于归因与原 action 的 CAS，不替代 event ID 去重。ask 点击先按现有 CAS 持久记录答案，再后台重绘；姓名查询、card edit 和最终答案展示不成为事件队列继续接收的条件。未知/未授权动作保持既有忽略语义，action ID 不授予权限。

## 7. 合并资源与输出生命周期

`WorktreePort.planMerge` 规划 source 和已检出的 target；执行前复核 plan 与绑定。merge 不申请工作区资源并发锁，同目标并发冲突由 Git/Worktrunk 报告，可能留下部分副作用。

子区首次任务可能在 `/thread merge` 已冻结其未初始化绑定后才验证并写入精确 session file。merge 只可接受由同一 chat、较早 `ordered` ingress 明确 handed-off 到同一 task 的初次 session 初始化：绑定更新与 task ID、完整旧绑定快照（session ID、cwd、空 session file、pinned 标记及 `session_switched_at`）必须在同一 SQLite 事务中提交；task/ingress source、冻结 snapshot 与 merge 的 session-generation/barrier 必须精确匹配，当前 cwd、pinned 标记和 `session_switched_at` 不变。`/new`、`/reset`、`/move` 或其它绑定身份变化清除此 provenance；旧库/旧 snapshot 无 provenance 时保守拒绝。此例外不是 cwd-only 放行，也不增加普通任务目录锁。

后台 command operation 在等待聊天或精确 session 时尚无 `wt merge` 进程；操作状态应区分 waiting 与 running。受管进程继续使用现有 PID/PGID/identity、watchdog、600 秒 merge 执行上限及整组退出确认，不能用超时包装 Promise 后遗弃原进程。

outbox 继续按真实聊天、稳定 client ID 与版本发送。merge/command job 的业务完成不等 outbox 网络排空；归档 operation 单独等待完整成功回执 `sent`、正文及卡片 tail。新授权输入取消归档的时点移到可靠受理时，不能等正文下载或 merge 后台结束才取消。

## 8. 停机与恢复所有权

1. 封闭同步接收入口；新票据拒绝，不 ACK。
2. 中止尚未持久受理的权限查询，完成这些票据的清理；不写正文、不制造成功接收。
3. 已落盘的 ingress job 保持可恢复；停止后台 admission 与准备流程，防止停机后继续 native dispatch。
4. 按既有领域顺序停止 task、goal、fork、commit、merge/remove 的受管进程，等待整组退出；未知身份/退出不确定继续 fail closed。
5. 完成所有后台 operation 和短交接事务的收尾后，停止 outbox/transport，最后关闭 store。

恢复先处理旧进程身份与资源封锁，再恢复已知领域 outcome，最后恢复确认未派发的 ingress job。已接受任务沿精确 session 续接，不能重新从 envelope 生成原始 prompt。显式取消记录优先于所有恢复调度。

## 9. 修改范围

| 模块 | 具体改动 |
| --- | --- |
| `src/contracts.ts`、`src/octo/index.ts` | 同步 receipt 接口；逐条 ACK 跟踪；移除全局 handler 完成链。 |
| `src/inbound/ingress-coordinator.ts` | 内存投递、后台授权、短持久受理、ordered/control 入口、停机与恢复。 |
| `src/store/store-schema.ts`、store facade/domain 文件 | 增量 ingress_jobs 表；受理/领域消费的原子交接；旧 pending claim 不自动重排。 |
| `src/service.ts` | 拆开接收、授权、业务准备与 command operation；注册后台所有权、停止/恢复与 idle 条件。 |
| `src/commands/*`、`src/session/session-lifecycle.ts` | 长命令后台执行，保持原有 session/模型次序与命令权限。 |
| `src/tasks/task-scheduler.ts` | 接受短期顺序意图；维持现有 exact-session 和破坏操作资源边界，不恢复普通任务目录锁。 |
| `src/questions.ts`、`src/bot-events.ts`、命令卡片处理 | 持久答案/动作受理与网络展示分离；保持事件游标与 action generation 合同。 |
| 主设计及关联权限、附件、线程、恢复规格 | 实施时统一更新到本接收/ACK 边界，不能同时保留 handler 等待长业务的旧合同。 |

不添加外部队列、服务、依赖、用户配置、自动 Git 重试或新模型调用。表迁移在一次性旧库副本上演练，不能改真实业务状态来验收。

## 10. 实施状态与验收

入站持久交接和后台调度在隔离工作区实现；验证范围包括真实 CLI、加密协议夹具、临时 Git worktree 与本地有限模型。Git 验收使用 wt 兼容测试脚本，不调用常驻 Bot。

### 10.1 入口与接收

1. 真实 encrypted Octo fixture 连续发送同区 merge、同区 cancel、其他群 help/status 和普通文字；第一条后台权限或进程阶段 held 时，接收 callback 均已返回，其余投递各自有票据。测试用可控 gate 证明，不靠源码文本断言。
2. 同一消息重复 RECV、断线重投、ACK 丢失：一个授权交接、一个最终消费者，无重复任务/命令/答案/回执。
3. 未授权及 Bot 回声没有正文交接行、任务或下载；权限查询挂起不阻塞其他聊天控制。状态盘提交失败不 ACK、不执行副作用。
4. receive callback 返回但尚未授权的消息随后遭遇 SIGKILL：重投后受理一次。授权交接提交后、ACK 前 SIGKILL：从既有 job 接续一次。
   补充故障注入：waiting merge 已 ACK、尚无 host merge ledger 时 SIGKILL，恢复同一 waiting operation 一次；切换命令已 ACK、尚未创建候选 worker 时 SIGKILL，先恢复屏障，再处理切换后的普通消息。


### 10.2 真 Git 与资源竞争

5. 在临时真实 repo 的 linked worktree 执行 merge，用可控长时 hook 持有进程；其他聊天普通查询和同区控制在 hook 释放前完成受理并能给出对应结果。
6. 其他聊天持有工作区时 merge 不因资源使用等待；同区 cancel 能中止等待或执行的 merge，其他聊天与普通查询不被牵连。
7. 取消已经启动的 merge：真实后代 hook/process group 必须全部退出或保留未知状态。退出未确认与无业务 outcome 的 exited 记录重启后不自动重试 Git。
8. 并发 merge、plan 变化、独立 worktree、shared 拒绝与 exact-session/FIFO 原有用例继续通过。

### 10.3 其他长路径与用户可见输出

9. 分别 held 附件下载、免@查询、成员获取、`/new` candidate 启动/停止、`/move`、`/switch` probe、`/btw`、fork、native commit、自定义 handler；均不能阻塞接收 callback，也不能挡住无关聊天或同聊天取消/查询的受理。
10. `/new`/`/move` 之前和之后的普通消息使用正确绑定；ask 文本/点击只回答原 question；thread-dispatch 从不误答 ask；成员不能因旁路进入命令面。
    补充：Q1 对应文字已 ACK，随后卡片答完 Q1、打开 Q2，再重启；旧文字只能按 Q1 身份结算。免@查询 held 时同聊天明确 @ 的控制命令照常受理；取消落盘后才完成授权或重投的旧候选不能执行。
    媒体补充：下载完成前拒绝 ACK；原 URL 失效前已保存媒体、ACK 后重启，恢复只使用本地 spool；准备拒绝必须先有持久失败结果，不能只 ACK。
11. 卡片查询/重绘发送 held 时，其他消息和事件继续被可靠受理；真实卡片/文字结果由原 outbox/卡片生命周期发送，不靠 ACK 冒充结果已展示。
12. merge outcome 已知但 receipt HTTP held：控制与普通输入继续受理；归档必须等真实 `sent` 和 card/body tail；新授权输入仍能取消归档。

### 10.4 实施验收门

实施时运行 `bun run typecheck`、完整 `bun test` 和隔离的真实 CLI、协议、Git 与生命周期验收；旧状态库在只读副本上重复迁移，检查已有表旧列内容保持一致。

## 11. 风险与交付边界

- 服务端重投是未 ACK 输入的恢复前提；独立 ACK 和窗口行为须用协议夹具与可用服务端实现核对，不能把处理过的后序消息当作前序消息 ACK。
- 已授权后的持久交接必须原子接入既有 task/steering/ask/command 消费；媒体以本地可恢复 spool 为 ACK 条件，不能只凭临时 URL。两次独立状态写入会产生丢失或重复窗口。
- 业务顺序必须由明确的 admission/barrier 保留；取消/查询旁路不能让正文穿过会话切换或破坏操作预留。
- 后台化必须有持久状态、AbortController 和停机 owner；没有所有权的 detached Promise 不满足设计。
- 本工作区不部署、不重启常驻 Bot；上线须另行完成集成与部署。

## 12. 未知会话切换的显式恢复

### 12.1 承诺与范围

owner 可在当前聊天通过新的 `/new` 或 `/new <目录>`，从结果未知的较早 `/new`、`/reset` 或 `/move` 恢复对话。新的命令是有独立身份的新操作，不重放原命令。旧结果继续如实记录为未知；新会话成功不证明旧操作的结果或旧进程已退出。本节细化 §5.4、§6.1 和 §8 的未知结果与会话屏障合同。

只有已形成未知会话切换屏障时，此恢复入口才能超越该屏障及其后被阻塞的有序输入；正常尚在执行的切换继续遵守现有次序及取消合同。权限、目标聊天、路径规则和新会话候选的验证不变。

### 12.2 独立会话、绑定与并发

- 恢复命令创建新 session，不打开或复用旧切换涉及的精确 session；候选 worker 的 session 身份、文件和停止确认按现有新建会话合同验证。旧切换结果未知但其 worker 已确认停止时，释放仅由该次切换遗留的目录阻塞，允许同一 cwd 建立独立新 session；保留同一 session 的独占及 `/commit`、`thread-delete` 等操作专属保护。
- 成功发布必须以持久会话代次或等效条件阻止旧操作及其恢复路径迟到提交旧绑定；并发恢复只能有一个有效切换顺序。新绑定、旧屏障解除、被覆盖输入的最终处置及其稳定通知必须原子提交，或使用可恢复的持久中间状态保证崩溃后具有同等效果。不得先对外报告恢复成功，再留下可能由旧操作覆盖的绑定。
- 所有可能写入聊天绑定的并发路径，包括查询触发的会话核对、任务启动及旧操作恢复，都必须以其读取时的持久代次或精确旧绑定作条件提交；条件失效时不得写回覆盖新绑定。
- 新候选失败、取消或结果不确定时，不能报告恢复成功，旧屏障及相关输入保持可恢复状态；新操作自己的未知结果同样保留，不能自动重跑。旧 worker 仍在运行或其退出无法确认时，不解除其精确 session 和目录保护，不宣称对话已恢复；明确提示等待其完成，或使用 `/stop`、`/cancel` 按现有权限和目标范围请求停止后重试。取消请求本身不是退出确认：停止未确认时继续保护，保留账本和 watchdog，并明确报告仍无法确认。
- `/move` 可能已迁移原会话：新 `/new` 只确立自己的新绑定，不推断原迁移结果、不删除旧 session 文件、不重放迁移。旧操作身份与任何已知路径继续保留供核对。

### 12.3 被屏障挡住的输入

以受理顺序和精确聊天为界，较早未知切换之后、成功恢复 `/new` 之前已可靠受理、尚未派发的普通消息、附件及命令，不自动执行：持久标记未执行并发送可识别原消息的通知，说明用户可自行决定重新发送或忽略。提示使用消息身份及必要的安全摘要，不泄露附件 URL、凭据或原始协议内容；多个消息可汇总但每条可识别。消息和附件的持久记录按现有保留规则保存，不因恢复静默删除。重复投递、服务重启和通知重试不能把这些输入重新执行或重复生成处置。

对恢复命令之前通过完整权限门、已建立顺序身份、但仍在准备或未 ACK 的输入，也持久记录同一身份的防执行处置；准备完成或服务端重投不能使它越过恢复屏障执行。权限门前不持久化消息正文，取消或停止身份 tombstone 沿用 §6.2。附件准备和 ACK 仍遵守 §5.2 的可恢复条件；不得为了生成提示而提前 ACK 未可靠保存的媒体。

已经派发且结果未知的输入继续记录为未知，提示不得称其肯定未执行，不自动重试。原用途已冻结为 ask 答案的输入按原 question 身份、关闭原因和会话切换失效规则处理，不重新匹配其他问题，也不转为普通任务。恢复 `/new` 之后的新输入使用新绑定，按既有聊天 FIFO 接续。

### 12.4 其他未知结果

fork、线程和 Loop 远端创建的未知分配保留其独立 ledger、已知身份及去重门禁，源聊天恢复不证明远端未分配，也不自动再次创建。steering、commit、merge/remove 等已派发副作用的未知结果不自动重放，不报告虚假成功；其进程和操作专属保护继续适用，但不得成为无关聊天或独立新会话的永久屏障。归档仍受已知业务结果及真实输出排空合同约束。

### 12.5 验收

1. 对 `/new`、`/reset`、`/move` 在派发前、派发后、候选停止和绑定提交边界注入中断、取消、重启，验证新 `/new` 可按本节恢复；只在未知屏障下旁路，旧操作不自动重放，也不能迟到覆盖新绑定。（需求 1–3）
2. 分别覆盖旧 worker 已确认停止、仍运行及退出状态未知：已停止时允许在原 cwd 创建独立 session；未确认时提示等待或通过 `/stop`、`/cancel` 请求停止，只有确认退出才释放该 worker 的保护并允许重试；停止失败或身份未知不能报告已恢复。精确 session 与操作专属资源保护不被绕过。（需求 3–4、6）
3. 已授权、尚在准备或已可靠接收但未派发的普通文字、附件、命令按身份处置并通知未执行；未 ACK 重投不能执行，媒体 ACK 不提前。不自动执行，用户可重新发送或忽略。ask 保持原身份及失效规则，已派发未知输入不被当作确定未执行或重复执行。（需求 4）
4. 对恢复绑定、屏障处置和通知持久化各边界执行重复投递、并发 `/new`、重启及 SIGKILL：不丢输入、不重复执行、不重复生成通知，恢复后绑定一致；候选失败不错误释放屏障。（需求 2–4）
5. 检查 fork、远端创建、steering、commit、merge/remove 的未知结果：保留各自领域账本和副作用门禁，不重放旧操作，也不将无关独立对话永久阻塞。（需求 4）
6. 通过隔离的真实 CLI、有限本地模型及加密 Octo fixture 验证会话恢复、消息提示和重启；数据库迁移使用一次性副本。现场业务状态只读，不注册或重启常驻 Bot，也不修改真实状态作为验收。（需求 1–4）
