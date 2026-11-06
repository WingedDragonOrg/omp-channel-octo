# OMP Octo 个人远程助手设计

日期：2026-09-12
修订日期：2026-11-06
状态：产品设计已确认

## 背景与目标

通过 Octo 私聊和指定群聊操作本机 OMP。使用独立于终端的持久会话，保持用户当前全自动执行习惯。channel 是常驻消息服务，OMP 是通过 stdio RPC 驱动的子进程；不依赖模型 API auth-gateway。

## 范围

包含个人单 Bot、单服务实例、文本指令与回复、执行状态、新建会话、指定目录、取消、会话恢复、消息去重和连接重建。支持私聊、指定群及 CommunityTopic 线程（channelType=5）内明确 @Bot 的指令；支持群/线程共享说明、线程默认继承父群工作目录、群/线程对话历史的增量上下文注入（详细设计见 `docs/specs/2026-09-16-group-history-context-design.md`）、线程隔离工作区的 Worktrunk 生命周期（详细设计见 `docs/specs/2026-09-17-thread-worktrunk-design.md`）、任务启动失败的分类报错与可观测性（详细设计见 `docs/specs/2026-09-17-task-startup-failure-design.md`）、常见入站消息类型的内容提取与附件接收（详细设计见 `docs/specs/2026-09-16-inbound-message-content-design.md`）、子区级回传监听（`listen=off|merge|task`，详细设计见 `docs/specs/2026-09-17-thread-completion-notify-design.md`）、出站文件和持久化问答、已开通群/子区内成员的消息驱动与 ask 卡片作答（命令仍仅 owner；详细设计见 `docs/specs/2026-09-17-group-member-participation-design.md`）、子区归档与合并后自动归档（详细设计见 `docs/specs/2026-09-17-thread-archive-design.md`）。

包含发给当前 owner 私聊的服务启动就绪与正常停机通知：覆盖 Super、`oco` 和聊天命令最终触发的同一服务生命周期，按事件做有界发送，不承担异常离线监控。

包含服务侧获取的每轮群/线程成员上下文、system/developer 中的 mention 使用规则与 Octo CLI 能力入口，以及普通助手回复中的结构化 mention。正文和完整 mention 随流式输出持续更新同一消息；具体边界见「每轮成员与操作上下文」及「普通回复中的 mention」。

包含 owner-only 原生持续目标管理 `/goal`：创建时 token 预算、状态查询、暂停、恢复、删除、跨回合执行与在途目标跨服务重启续接，详细契约见 `docs/specs/2026-11-02-goal-command-design.md`。

包含主群/子区已落盘上下文即时分叉：owner-only `/fork [子区名称]` 和独立 `fork` 宿主工具共用原生 CLI `--fork`，沿用 cwd、建立独立 session 并绑定同父群新子区。详细契约见 `docs/specs/2026-11-03-thread-fork-design.md`。

包含 owner-only `/tan <用户请求>`：在同父群创建继承当前已落盘会话的独立 fork 子区，并将物化请求作为目标子区的任务执行；源任务继续，文件目录共享。持久接受、取消、恢复及子区结果契约见 `docs/specs/2026-11-04-tan-fork-task-design.md`。

包含可选外部决策服务驱动的子区 PR/MR 引用绑定：operator 显式配置共用 Decisions 服务的协议、模型和密钥后，仅从已受理的当前子区可见正文识别候选，以必要有界上下文判断是否为直接工作对象；高概率结果去重持久化到本地 `thread-info.references`，不改 Octo/Fleet。判断异步尽力而为，已绑定引用跨重启保留，未完成判断不恢复或补扫。详细契约见 `docs/specs/2026-11-04-thread-pr-mr-reference-binding-design.md`。

包含 owner-only `/bind loop <url|workspace_id|workspace_name|workspace_slug>` 的 workspace 感知：仅扫描显式绑定 ID，以完整分页快照比较业务变化；只为当前负责人 Octo UID 精确命中 channel owner/Bot，或已有明确 task—子区关联的任务产生 agent 输入。首次整理及未关联的关注任务交主群，明确关联的任务变化直达子区；其余任务仅更新快照。thread 工具通过 `task_id` 创建或维护来源关联，执行结果按既有 listen 策略回传。权限、持久去重、未知接受及创建保护、隔离验收见 `docs/specs/2026-11-04-loop-workspace-task-awareness-design.md`。

群与子区当前事实清单使用当前 GROUP.md / THREAD.md 中的固定 `### Memory Start ###` / `### Memory End ###` 区段。agent 先通过已有 `octo_management.description-read` 读取完整说明，只修改该区段并保留区段外原文，再经 `description-write` 写回；宿主要求本任务先显式读取，并在写前重新核对正文与版本，说明变化时拒绝并要求重读。群与子区分别使用自己的说明，沿用现有版本快照与 compaction 注入。完整合同与验收见 `docs/specs/2026-11-04-group-thread-current-facts-design.md`。

包含显式请求驱动的有界语义 `watch`：宿主持久保存采集器、完成条件、间隔和截止时间，独立于原 OMP worker 周期检查；单次命中生成归属聊天的独立排队输入。启用 `decisions` 时 watch 默认启用，并与引用识别共用 protocol、endpoint、model 和 key；可用 `watch.enabled=false` 单独关闭。客观状态、退出或事件条件使用程序机制。完整授权、预算、停止与恢复契约见 `docs/specs/2026-11-04-bounded-stoppable-watch-design.md`。

不包含终端会话接管、多用户服务、通用网络 Agent gateway、文档评论任务、服务端通用 bot_task、群历史归档与检索、出站附件之外的通用文件托管和交互 callback 系统。channel 仅提供由 OMP host tool 明确发起的出站图片/文件发送，以及由能力 profile 明确启用的 Octo raw type17 `octo/v1` AdaptiveCard 1.5 进度展示和 `octo/v2` 交互问答卡片；交互卡片的上行只使用该 Bot 自己的事件队列（`/v1/bot/events` 轮询与 ack），不注册回调地址、不实现通用 callback 或审批流。收到的入站消息按已确认的类型表提取内容，贴纸、系统与 CMD 等没有内容的类型明确告知，不作为成功执行。范围以个人 IM 交互为准，不继承参考适配器的其他任务入口。

## 已知约束与资料

- 本机当前 OMP v18.1.21 提供 `--mode rpc`、`--cwd`、`--resume`、`--approval-mode yolo`、`--extension` 及 `session.compacting` 生命周期能力；本地兼容解码器按 v18.1.18 保持 RPC 兼容。RPC 提供 prompt、steer、abort、状态和会话事件；启动 ready 不代表模型调用可用。模型选择走启动参数、内建命令与 RPC 只读目录（v18.2.3 实测）：OMP 自带 `/switch` 内建命令（ACP 可见），选择器接受 `provider/id`、模型 id/唯一子串、`@角色`（`settings.modelRoles`）与 `:level` 后缀，命中后只改本会话（`setModelTemporary`），分发发生在 RPC 的 prompt 路径、agent 回合之前（响应 `agentInvoked:false`），因此回合运行中同样可用；成功时先发 `command_output` 与带解析后模型与级别的 `config_update`，再回响应，无法解析时只发 `command_output`（`Unknown model: …`）。`--model <provider>/<id>[:level]` 用同一解析器（`@smol` 这类角色别名同样接受），启动时生效并覆盖 `--resume` 会话里记录的模型；传无法解析的值使 worker 启动失败（`Model "x/y" not found`），所以落盘前必须由 OMP 自己校验过。`get_state` 返回当前模型与思考级别，`get_available_models` 返回当前已配置凭据的可用模型。线程隔离工作区在本机存在可执行 Worktrunk（命令兼容性由 wt 自身判定）时由宿主调用 wt 完成创建、合并与清理（v0.76.0 实测；合并没有回退），否则创建回退 `omp worktree add`（v18.2.1 实测，clone-first 由 `worktree.clone` 设置决定）、清理回退 `git worktree remove`。子代理可观测性走 OMP 自身的 subagent 通道（v18.2.1 实测）：`set_subagent_subscription` 选择级别（`off`/`progress`/`events`，默认 `off`），订阅后 RPC 侧推送 `subagent_lifecycle`（`id`、`agent`、`agentSource`、`status: started|completed|failed|aborted`、可选 `description`、`index`、`parentToolCallId`、`detached`）、`subagent_progress`（`progress.status`、`lastIntent`、`currentTool`、`toolCount`、`recentTools` 等）与 `subagent_event`（原始子事件），`get_subagents` 另有存活子代理快照。channel 只订阅 `progress` 级别。
- OMP SDK 可嵌入，但本设计选择 RPC，保持进程边界。
- 持续目标按 OMP 18.15.0 的结构化 `{type:"goal", op:"get|create|pause|resume|drop"}` 接入，不依赖 TUI slash 注册。目标执行 worker 只覆盖 `goal.continuationModes: [rpc]`，不覆盖 `goal.enabled`。create/resume 自发启动的回合没有 prompt ticket；单轮 `agent_end` 即使 `isTerminal:true` 也不代表目标执行已静止，应等待 `session_settled`。已有目标替换及创建后预算变更当前未由原生 RPC 开放，明确反馈。
- Octo 运行参考：[openclaw-channel-octo](https://github.com/Mininglamp-OSS/openclaw-channel-octo)。其 README 确认 REST 注册、WebSocket 实时收发和自动重连。
- 账户参考：[create-openclaw-octo](https://github.com/Mininglamp-OSS/octo-adapters/tree/main/create-openclaw-octo)。使用 BotFather 创建的 User Bot；`app_` App Bot 仅支持私聊，不能满足群聊验收。
- octo-adapters 当前源码树主要提供安装 CLI；协议运行时代码在独立 channel 仓库。实施时以运行时代码核实报文、消息 ID、mention 元数据和发送接口，不凭顶层 README 猜测端点。
- RPC、SDK 和审批机制参考 OMP 自带 docs/rpc.md、docs/sdk.md、docs/approval-mode.md。
- 上游 `Mininglamp-OSS/octo-server` 的 `modules/bot_api/typing.go` 表明 90 秒后至 180 秒 TTL 窗口可能抑制 CMD typing 投递，即使 HTTP 返回 200；channel 对 agent-attached 消息在权限门通过后立即发出 typing，并在附件组装、排队与执行期间每 3 秒续期、任务终态熄灭（契约详见验收 39 与 `docs/specs/2026-09-20-prompt-typing-feedback-design.md`），不通过客户端绕行，也不承诺绕过服务端节流或离线时连续可见。真实部署尚未完成超过 90 秒的视觉验证；密集多轮会话中服务端死区可能使 typing 最长约 90 秒不显示，属上游既定契约。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 身份 | owner 用户 ID、允许群 ID 列表；已开通群/子区内的成员可驱动助手 | 私聊、命令与配置面只归 owner；成员在允许列表命中的聊天内以结构化 @Bot 发起消息、回答 ask 卡片（详细设计见 `docs/specs/2026-09-17-group-member-participation-design.md`） |
| 执行 | yolo；channel 不添加逐次审批 | 保持全自动习惯 |
| 会话 | 每个聊天一个当前会话，历史保留 | 隔离聊天上下文 |
| 目录 | `/new [目录]`、`/reset [目录]`；省略目录按目标默认目录解析（DM/群沿用当前绑定，首次 `~/tmp`） | 每次 fresh 会话都有确定工作目标 |
| 线程目录 | CommunityTopic 线程尚无已验证会话时，默认目录取父群当前绑定 cwd；线程首次成功建立会话即固定，与父群解耦 | 线程默认落在父群项目上；cwd 可共享，session 与共享说明仍按聊天目标隔离 |
| 线程隔离 | `create-thread` 支持 `projects:[{name,repo,base?}]`、`layout` 与简短 `background`；多项目由 agent 显式选择 shared（一个非仓库入口内多个隔离 worktree）或 separate（每项目一个 thread/worktree），单项目保留 `worktree_name`。宿主准备实际工作区、持久化绑定并写入 THREAD.md 后才报告 ready、允许派发 | 工作区与项目来源明确，任务只来自后续派发正文；详细 SDD 见 `2026-09-17-thread-worktrunk-design.md` |
| 工作区合并 | 单项目/separate 子区的 `/thread merge` 由宿主按聊天和 session 顺序执行：dirty 时复用 OMP 原生 commit、进度、受管进程与停止处理，但不取得 `/commit` 的工作区资源租约，也不因其他聊天任务、相关目录/仓库活动或未确认 worker 账本等待或拒绝；clean 路径同样不取得资源锁。仍按持久归属和实际路径、来源/分支验证工作区，提交后的身份、HEAD、计划及干净状态校验失败即不启动 wt。提交或合并的未知结果不自动重放或称成功；当前操作的取消与 session 顺序保持。风险是自动提交可能与并发写入、Git 操作或 hooks 交错，产生冲突或部分副作用，不承诺整体回滚。成功仍由原有结果投递、清理与归档路径完成；仅已确认的 wt 合并执行失败交给本聊天 agent 修复。shared 固定拒绝且不创建任务，ready fork 仍只归档，直接 host `thread-merge` 接口与语义不变 | 保留现有 OMP 提交策略与合并契约；不以 wt 默认 `--stage all` 接收提交后残留改动 |
| 工作区清理 | `thread-delete` 在服务端删除成功后按持久资源归属清理，不依赖活动 cwd；有可用 wt 时 `wt remove`，否则 `git worktree remove`。仅此 host-tool 删除流程保持既有占用检查；未确认创建、身份不符或脏 worktree 按原策略保留，显式 force 才丢弃脏改动，入口只删除空目录 | 显式换目录不遗失原资源归属，也不误删用户目录或未知文件；此规则不扩展到 `/thread remove` |
| 清理并归档 | owner 在目标子区执行 `/thread remove [--force]`；按持久归属清理 single/separate/shared worktree 和分支，全部结果确认且回执/正文/终态卡排空后归档；正常路径零 agent 任务。默认保护 dirty 和未合并提交，force 明确授权丢弃两者。命令不因其他聊天任务、相关目录/仓库活动或未确认进程账本等待或拒绝，也不以等效路径锁替代；仍验证归属、来源与实际路径。部分或未知结果保留残留/未知状态、不归档、不自动重放或称成功 | 不合并、不删除聊天记录；并发 Git 副作用可冲突或部分完成；直接 `/commit` 与 `thread-delete` 原保护不变 |
| 子区回传 | 子区持久策略 `listen=off|merge|task`，默认 merge，仅实际合并成功回传创建方聊天；task 额外回传派发任务四种终态；off 静默。合并失败及修复失败始终在子区处理。通知按接收聊天追加模式交付且不回答 pending ask | 单一子区策略、稳定事件去重；普通任务不打扰父会话 |
| 迁移 | `/move <目录>` 使用 OMP 原生 `/move`，沿用当前 session ID、历史与附件 | 保持会话连续性，不伪造新会话 |
| 调度 | 同聊天 `ordered` 输入按受理次序安装任务、会话切换与操作屏障；长命令交给服务拥有的后台 operation 后不再阻塞后续受理。取消、停止与只读查询走独立 control 入口，保留当前聊天与精确 session 顺序及各自取消语义；`/thread remove`、`/thread merge` 不建立跨聊天路径/仓库资源屏障，独立会话不因 cwd 相同、祖先/后代、父群/子区或 shared 资源归属而互斥。独立 `/commit` 与 `thread-delete` 保持既有资源保护 | 保留同聊天业务顺序及各命令原有保护边界；避免完整长命令 lifetime 成为同聊天或全局入站完成条件 |
| 故障 | 确认旧进程退出后，已进入生成的任务沿原 session 续接，未派发任务按序执行；接受状态不明则停止自动恢复 | 续接不重放原始 prompt，不把未知工具副作用当作尚未发生 |
| 共享说明 | 新 session 首次和 compact 边界加载当前聊天最新说明；普通回合沿用已加载快照 | 持久写入与会话刷新分离，说明进入原生会话消息，不反复改写 system |
| 成员上下文 | 新任务与 steering 查询当前聊天成员；首次及语义变化时随对应用户输入持久保存 | 相同成员不重复，历史不移动，API 顺序不构成变化 |
| Mention 投递 | 助手以 `@[uid:显示名]` 表达点名，宿主增量转换并在同一消息流式发送结构化 mention | 保持回复连贯、流式可见及真实 @提醒 |
| CLI 能力入口 | 固定 system 提供业务能力摘要、当前 bot/聊天身份与 skills/schema 发现路径 | 普通回合、工具续调、原生 retry/恢复位置一致；CLI 真实就绪状态可变化 |
| 入站内容 | 按已确认类型表提取文本；媒体附件下载到状态目录后以本机绝对路径交给 Agent；回复消息按同一张表读取客户端内联的被引用 payload（文本进引用块、媒体并入本条附件——被引用的是无正文媒体时以 `[引用 <发送者>]` 标题注明来源）；群/线程中受权消息本身无附件、无可用引用时，按「同一发送者、10 分钟窗口、唯一一条带附件消息」做「这个」关联取回（歧义/跨发送者/超窗一律不关联，详见 `2026-09-16-group-history-context-design.md`）；贴纸、系统与 CMD 等仍明确告知不支持 | 让常见消息真正可驱动 Agent，同时保持权限门、单一体积上限和可解释的拒绝行为 |
| 群自助开通 | 允许列表 = 配置 `allowedGroupIds` ∪ 状态盘 `allowed_chats`；owner 在未开通群/子区 @Bot 时收到节流的开通确认卡，交互卡不可用时使用固定口令 `开通本群`（状态盘持久，配置文件同步追加） | owner 现场把 bot 拉进新群后无需改配置或重启；非 owner 与非 @Bot 的入站消息保持静默拒绝，开通动作只归 owner |

## 设计

### 服务编排边界

`CoreService` 负责入站、任务与服务生命周期的编排，通过明确的类型化端口调用职责模块。`inbound/history-coordinator.ts` 独占每个聊天的历史覆盖 buffer，负责有界回填及 ledger prepare/commit；权限门之前的记录和 prompt 结束后的提交时点由服务保持。`commands/model-controller.ts` 负责模型命令、聊天模型持久化及临时探测 worker 的完整生命周期，服务只提供活 worker、目录解析、出站投递和目录阻塞端口。模块不依赖 `CoreService` 实例，也不共享其可变状态集合。

`inbound/ingress-coordinator.ts` 独占同步 receive 之后的投递登记、后台授权、短持久受理、`ordered`/`control` 入口及恢复。receive callback 只做有界内存登记并同步返回 `InboundReceipt { ackReady }`；`submitInbound` 只等待持久受理，不等待任务、模型、Git、回执发送或归档完成。`inbound/chat-authorization.ts` 独占权限判定、免@查询缓存、运行时开通及节流通知；`inbound/inbound-materializer.ts` 负责唯一的正文资源组装入口，持有每次下载预算和本次新建文件清单，统一处理失败回滚。完整权限门通过前不持久正文；已授权且需要媒体的输入在正文与 spool 可恢复落盘后才完成逐消息 `ackReady`。三者通过同一个服务 shutdown signal 取消在途 I/O，共用 `errors.ts` 的 `InboundShutdownError`；服务继续拥有领域业务编排与受管后台 operation。

`cards/progress-cards.ts` 统一拥有进度卡的持久恢复与每任务轮次状态。服务将已归一化的 `ExecutionEvent` 交给每任务的 `TaskProgress`；研究帧自身可以开卡，其最近快照在同一任务滚卡时结转，新任务独立开始。队列 tail 仍登记到服务关停排空集合，子区归档前读取并等待当前 tail。`session/session-lifecycle.ts` 负责会话身份/cwd 校验、目录回退、`/new` 和原生 `/move`；它通过 `TaskScheduler` 取得聊天/会话租约，blocked 集合继续由服务单点拥有，源、目标目录仅检查未确认退出的 worker 阻塞。

`runtime/worker-lease.ts` 独占运行期 worker 的终止、恢复回收和台账收尾；业务侧只取得不含 `stop` 的 `WorkerSession`，服务的停机、取消、问答超时及任务收尾共用同一 lease。首次 `close` 的策略和完整 Promise（包含台账写入失败）由 lease 保存，并发与后续关闭复用同一结果。停止或恢复未确认时，按 spawn 的真实 cwd 与预先声明的迁移目标阻塞目录；迁移目标随 spawn 台账一次写入，初始化失败和重启恢复也使用这份记录。

`/new`、`/move` 的候选 worker 通过 `withStoppedWorker` 运行：scope 验证会话、确认进程停止并完成台账更新后才交付结果，调用方随后持久化绑定并发送回执。停止未确认或台账更新失败不会交付成功结果；操作失败仍执行关闭，关闭中的台账错误优先返回，否则保留原操作错误。聊天/会话租约覆盖整个 scope 和绑定提交。

`commands/thread-commands.ts` 负责 `/thread` 的任务交接及 `/thread merge` 的宿主优先编排，命令去重仍在服务入口完成。命令与工具复用 `threads/thread-merge.ts` 的合并核心；只有真实 merge 执行失败生成稳定身份的修复输入。merge 由服务拥有的后台 operation 执行，不延长 receive callback 或 `ackReady`。`threads/thread-archive.ts` 独占待归档集合和停止唤醒信号；新授权入站可靠受理时持久递增线程归档代次并取消排定，重启恢复只恢复 lease 捕获代次仍有效的计划；归档等待指定成功回执 `sent`、进度卡 tail 与聊天 outbox 排空。卡片发送结果未知时禁止重发，且不把本地超时视为远端已排空。出站退避期间持续等待，不能以等待超时替代排空；保持瞬时归档失败重试与停止放弃契约。

`/thread remove` 与 `thread-delete` 的持久资源清理由 `threads/thread-workspace-cleanup.ts` 独占，调用方显式选择分支策略。remove 的每个 worktree/branch 步骤复用 `workspace/worktrunk.ts` 的受管进程与 watchdog，并在 `host_merge_leases` 以 `operation_kind=thread-remove` 记录原完整资源组、步骤、进程身份及结果；旧行默认为 merge，两个结果命名空间独立。只有已知业务 outcome 可在启动时补发回执并排定归档；`exited` 但无 outcome 仍为 unknown，不重做 Git、不称成功，也不作为独立新命令的资源占用门禁。缺失目录仍保留调度资源键，只对现存 cwd 探测 Git root；持久归属和实际路径验证仍是清理前提。

`tasks/task-scheduler.ts` 独占聊天串行链、排定任务、聊天/会话租约、等待回执标记及关停任务快照；不同会话不因 cwd 相同而互相排队。运行中 worker 和 blocked 目录仍由服务管理，调度仅经明确端口查询。`commands/command-routing.ts` 统一内建命令别名及注册校验，从别名表推导的 action 类型约束完整分派表；durable claim 保留用户输入的命令名，重启仍在 claim 完成后请求，未知命令照常走既有 handler 或 OMP 任务路径。

#### 独立聊天并行调度 SDD（2026-11-03）

**目标与范围。** 普通任务、恢复任务、持续目标和会话控制统一按聊天及精确 OMP session 调度。父群和子区、其他独立聊天，即使绑定同一 cwd、祖先/后代目录、同一 shared 入口或不同项目，也可同时启动 worker 并完成回合。文件修改由任务自行协调，不承诺普通 Agent 写操作互斥。

**职责。** `TaskScheduler` 保留聊天 FIFO、session 独占、命令顺序及关停排空。普通执行不取得工作区资源组排他租约，也不通过较早任务或普通会话命令的 repository 预留重新建立跨聊天目录串行。持久 `thread_workspaces` 继续记录创建状态、入口、项目、来源及分支，供就绪校验和清理使用，不作为普通任务执行锁。

**操作安全。** 独立 native `/commit` 与 host-tool `thread-delete` 保持现有资源排他及工作使用检查。`/thread remove` 与 `/thread merge`（包括其自动提交阶段）不检查或等待其他聊天的任务、目标、提交、目录/仓库活动或未确认 worker ledger，不取得等效路径锁；仍按持久归属验证资源路径、仓库、分支和当前操作计划，且保留同聊天/精确 session 顺序、取消与命令去重。shared 清理按持久记录逐项处理项目及入口，不删除未知资源或递归删除未知文件。force 只授权既定脏改动丢弃，不绕过身份、来源、路径替换或其他 checkout 验证。并发 Git/Worktrunk 写入可产生冲突与部分副作用，不承诺事务回滚。

**操作接口与集成。** `tryAcquireCommandSession` 负责会话控制，`reserveDestructiveCommandSession` 表示同聊天的 FIFO 意图，`tryAcquireDestructiveResources(cwds, task)` 仅供仍要求资源保护的操作期升级。`/thread remove` 按当前聊天和精确 session 顺序受理，但不因其他聊天、实际路径占用或未确认进程 ledger 而等待/拒绝，不以等效路径锁替代；它按持久记录逐项核验路径身份、Git 来源、分支及 other checkout。`/thread merge` 仅在需要自动原生提交时执行该提交，不取得独立 `/commit` 的仓库资源排他；merge plan 仍核验 source、真实 target checkout 和绑定身份。直接 host-tool `thread-merge` 保持不申请资源租约的原契约。`thread-delete` 与独立 `/commit` 继续各用其现有资源保护；权限、清理归属及远端删除策略仍归各自模块。

merge 路径由 `WorktreePort.planMerge` 提供 source＋真实 target checkout；默认目标使用 wt 自身的分支解析。执行前复核计划并固定显式 target；目标没有 checkout 时只记录 source，不伪造目录。Git ref CAS 与 index 锁各自保护单次操作，并不使 staging、rebase、hook、checkout 更新及清理成为整体事务。

**生命周期与恢复。** 普通会话命令持有聊天/session 至候选 worker 停止和绑定提交完成；goal 持有至 `session_settled`。未确认退出的 worker 仍不能复用其 session，但旧 worker/host-operation ledger 中的未知进程或结果不阻塞新的 `/thread remove` 或 `/thread merge`，亦不证明进程仍存活。merge/remove 为每个操作持久保留身份、步骤与结果；同一稳定操作的未知结果不得自动重放、续跑后续破坏性阶段或宣称成功，取消和停止必须记录实际操作结果；remove 任一步未知均不排定归档。独立 `/commit` 继续使用原资源租约，退出未确认仍封锁对应工作树；`thread-delete` 恢复和占用语义不变。重启按原身份交付已知结果，不重放原 prompt、不把 queued 批量失败、不改写线上任务。

**验收。** 在隔离夹具验证父→子、子→父、shared 不同项目、入口→项目、相同 cwd、祖先→后代的普通任务并发及聊天 FIFO、同 session 独占、取消、new/move、goal 与恢复。验证 `/thread remove` 和 `/thread merge` 在另一个聊天任务、相关目录/Git 活动及旧未确认 ledger 存在时仍按原命令顺序启动，不等待/拒绝且无替代路径锁；同时验证当前聊天/session 顺序、取消、持久归属/路径/branch 校验、部分副作用和未知历史结果不重放/不称成功。direct host merge、shared/fork 行为不变；独立 `/commit` 与 `thread-delete` 原资源冲突语义不变。真实 RPC 子进程、临时 Git 工作区 smoke 与既有离线 restart benchmark 仅使用隔离资源，不操作常驻服务。

**隔离验收证据。**

- 最终 `bun run typecheck` 通过；隔离运行全量 `bun test` 为 1231 pass、0 fail（86 个文件，7527 条断言）。定向只读安全复核未发现新的阻塞问题。

- 实施前，六个目录关系并发回归均因独立 worker 无法在 held worker 退出前产生副作用而失败；merge 目标安全的三条行为回归也确认目标占用及计划变更时仍会执行合并。回归分别保留在 `tests/core-concurrency.test.ts` 和 `tests/core-merge-target-safety.test.ts`。
- `bun tests/benchmarks/concurrency.ts` 使用安装的 OMP 与有限 localhost 模型完成 14 个任务、16 次模型请求；23 个 OMP 进程均确认退出，session overlap 与残留受管子进程均为 0。占用时真实 remove 调用为 0，释放后实际删除两个临时 Git worktree 及空入口。
- `bun tests/benchmarks/restart.ts` 的 graceful/SIGKILL 两场景均成功续接并恢复 queued 工作，session overlap、重复副作用及中断通知均为 0。`bun tests/benchmarks/goal.ts` 完成真实 OMP 的两轮目标、pause/resume/drop、graceful/crash 恢复与禁用目标拒绝，23 次本地模型请求、重复 create 为 0。
- 对真实数据库的只读一致性快照执行两次应用打开：25 张表、9424 行的所有原列逐行摘要保持一致，新增 `resource_cwds`，`integrity_check=ok`。生产数据库未迁移；快照及演练脚本已删除。
- 验收未注册真实 bot、未向真实群发消息、未重启常驻服务、未操作线上任务。

#### OMP 启动自主回合与任务输入 SDD（2026-11-05）

OMP 扩展可在 `session_start` 恢复观察并发起自主回合；RPC `ready` 只证明通信就绪，不证明 session 空闲。受理任务时，同一 session 的自主回合与任务输入必须保持顺序，独立 session 仍可并发。任务 prompt 的接受与完成只能归因于该输入自己的原生事件，不能把自主回合的输出、错误或终态记作任务结果。

任务等待期间取消或关闭不得派发未消费的输入；重启沿用已持久化的派发及接受状态，接受状态不明时保持既有 fail-closed 契约，不能重播已接受的 prompt。普通任务、恢复和原生目标继续持有各自的聊天/session 租约直到真实结算，保留 steer、queue 和取消语义。

当前 worker 在派发前未等待 session 已有的自主回合。即使先查询 `get_state.isSettled`，查询与原生 admission 也不是同一原子操作。OMP 18.16 的生命周期帧没有请求或回合身份，无法证明快照之后新启动的自主回合与任务事件的归属。完整契约仍依赖 OMP 提供可关联的原子 admission 和生命周期身份；失败后不得重放可能已接受的 prompt。

验收使用可门控的 RPC 子进程和隔离真实 OMP：启动自主回合保持运行时提交用户任务，验证输入只执行一次、输出归属正确、其他 session 可并发，以及失败、取消和重启边界。真实验收只使用临时 profile/session、localhost 模型和本地扩展，不访问真实业务。

`session/turn-context.ts` 负责精确 session 的说明快照、ledger/sidecar 事务、成员输入准备及宿主 compact 刷新。新 session 读取说明，普通回合复用；真实会话确认后发布 scope，成功时校验 prepared receipt 并 CAS 结算，取消、中断或失败保留已确认上下文与未结算状态。DM 只发布身份作用域，不读取共享说明。`omp/events.ts` 统一折叠 RPC 消息、回合、工具及子代理帧；worker 保留消费方回调异常边界，只拒绝当前 prompt。

`tasks/task-events.ts` 同步消费每任务的投影事件，维护按 messageId 聚合的输出及本地命令标记；prompt 接受回执先由服务入口持久化，不进入投影。通知、本地命令、ask 超时过滤、停止后工作、正文、steer 消费和进度事件的顺序固定。研究报告通过同步端口落盘，执行事件交给 `TaskProgress`。`runtime/service-restart.ts` 负责命令资格、待执行的空闲重启及旧版本通报行的静默清理；命令 claim 完成后才请求宿主重启，任务恢复由服务与状态盘协调。

### 运行和配置

使用 TypeScript/Bun 常驻服务，主动连接 Octo，启动并管理 OMP RPC worker。无需为 RPC 开放网络监听端口。macOS 使用 launchd 托管；机器休眠或离线期间不承诺可用。

本机入口为 `oco`（`omp-channel-octo` 的别名，`bun link` 安装）：`start` 是守护进程托管的服务进程入口，`run` 是不探测守护进程的调试入口，`restart`/`status`/`logs` 通过自动识别出的守护进程程序操作常驻实例，`test --card` 发送真实预览卡。配置在 `~/.oco/config/config.json`，服务日志由服务自持在 `~/.oco/logs/`，状态目录仍在 `~/.local/state/omp-channel-octo`（数据库、附件、sidecar、spool），OMP 会话默认落在 `~/.oco/sessions/`（每个项目一个子目录，见下）；守护进程与程序名不写进配置。详见 `2026-09-16-oco-cli-entry-design.md`。

每次 Super CLI 查询有独立期限（默认 10 秒），覆盖进程退出及 stdout/stderr 读取。在 POSIX 上该调用使用独立进程组，期限到达时终止该组及直接子进程，即使子进程忽略 SIGTERM 或后代仍持有管道，也以超时错误结束调用。

配置包含 Octo API 地址（缺省为 `https://im.deepminer.com.cn/`）、Bot token、可选 owner 用户 ID、允许群 ID、OMP 可执行文件、本地状态目录与会话目录（`sessionsDir`，缺省 `~/.oco/sessions`，每个项目一个子目录）；WebSocket 地址可按现有发现机制取得或显式配置；`ask` 块可配置问答等待超时 `timeoutMs`（默认 6 小时，允许 30 分钟到 7 天的整数毫秒，越界或非法值启动即拒绝）。未配置 owner 时，服务仅在该 Bot 尚无持久 owner 的条件下接受首次有效私聊绑定。凭证只来自本地配置或环境，不通过聊天设置，不写入日志。使用环境凭证时环境变量名默认 `OCTO_BOT_TOKEN`，显式名称仅允许 `OCTO_` 或 `OMP_CHANNEL_OCTO_` 前缀；所有这两类前缀的变量都会从 worker/watchdog 环境中剔除，以免子进程继承凭证。启动时校验配置、账户能力和状态目录可写性。同一 Bot 只允许一个 channel 实例，使用本机独占锁；不支持跨机器复制运行同一账户。
发卡前必须通过同一能力门禁：profile `enabled=true`（兼容服务端返回 `1`），`profiles` 包含 `octo/v1`，`card_version=1.5`，且 `config.card_enabled=true`、`config.display_enabled=true`、`config.reasoning_enabled=true`；`TextBlock` 是唯一硬依赖，`Container` 等可选元素缺失时使用 fallback；若响应提供 `elements` 或 `limits`，则遵守所需元素和各项限额（包括 payload、node、depth 限额）。能力响应明确不满足必需条件时按未启用处理；HTTP 失败属于请求失败，不等同未启用，不通过发送试探能力。
`launchd install` 只负责生成并写入权限为 `0600` 的 plist，不调用 `launchctl`；需要实际加载服务时由运维显式执行：

```sh
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"
launchctl bootout "gui/$(id -u)/com.omp.channel-octo"
```

`launchd uninstall` 只删除 plist，不代替 `bootout`。当凭证来自配置环境变量时，安装生成的私有 plist 会包含该变量及 token；该文件必须继续保持 `0600`。plist 固定记录安装时解析出的 OMP 可执行文件，运行时复用该路径，不自动切换本机其他 OMP。

复用本机 OMP 模型、Skills、扩展和项目环境。worker 明确设置 yolo，避免项目默认模式改变约定。OMP 显式工具 deny 或必须使用 UI 的扩展不由 channel 绕过；无法无界面完成时明确失败，不静默等待。执行权限不是沙箱，也不承诺限制 Agent 对目录外文件或网络的访问。

### 入站权限与路由

路由键为 Bot 身份、聊天类型和真实聊天 ID 的组合，不使用昵称。私聊（channelType=1）在已有持久 owner 时必须来自 owner；未配置且尚无持久 owner 时，仅支持的非空文本、`senderId` 与 `messageId` 非空且发送者非 Bot 的首条有效私聊可原子绑定 owner，并只确认不执行任务。传输层可能携带 `owner@bot` 形式的 raw channel ID，但归一后的 `channelId` 与 `senderId` 始终取真实发送者 UID，`owner@bot` 只属于传输层 ID。群（channelType=2）和 CommunityTopic 线程（channelType=5）要求允许列表命中的真实 channel ID 与结构化 mention 指向当前 Bot，且不会触发首次认领：owner 的消息按此放行；已开通聊天内的其他成员同样以「结构化 @Bot」为条件放行（成员不享有免@豁免），其余成员消息静默拒绝。群级免@只对 owner 生效：`GET /v1/bot/groups/{group_no}/mention_pref` 的 `effective` 由服务端把 bot owner 的免@意图与群级 `allow_no_mention` 开关 AND 后给出，为真时 owner 在该群及其线程的未 @Bot 消息照常放行——这是群设置本身授予的豁免，不是 channel 放宽权限。判定按群缓存 60 秒（线程用父群 ID），且只在 owner 消息没有 mention 时才发起查询；查询失败或响应不符合契约时保持「必须 @Bot」，并只做短暂负缓存，避免上游故障拖住入站处理。免@只解除 owner 消息的 mention 条件，允许列表、Bot 回声过滤与首次认领规则都不变。线程的允许关系继承父群：允许列表里有该线程的完整真实 channel ID，或父群 ID（`____` 之前的段）在允许列表里，都算命中，因此白名单群下新建的线程无需额外配置即可对话；不在允许列表的群及其线程仍一律拒绝。文本中的名字或 @ 字符串不能替代 mention 元数据。目录继承复用父群绑定的 cwd，允许关系继承只决定该线程能否发起任务，两者都不改变 owner 判定、共享说明隔离或线程自身的绑定。先验证，再处理所有命令和普通消息。Bot 自发消息、成员未 @Bot 的消息、未开通聊天里的消息与其他任务入口均不触发 Agent。

允许列表由两部分取并集：配置的 `allowedGroupIds`（操作者基线）与 owner 在运行中开通的聊天（状态盘 `allowed_chats` 表，跨重启保留）；两者的命中与线程继承规则完全一致。未开通群/子区默认静默拒绝（不落 `inbound`、不打日志、不回复）；唯一例外是 owner 在该聊天内 @Bot：发送一张开通确认卡，同一聊天 10 分钟窗口内只提示一次（节流在进程内，重启后可能再提示一次）。卡片说明开通后群成员可 @Bot 发起任务、命令仍仅限 owner，并指出只有 owner 可以确认；群显示「开通本群」，子区显示「开通本子区」。触发提示的原消息不执行；成功后提示「已开通，请重新 @Bot 发送任务」。

确认卡使用 `octo/v2` 交互卡片和 Bot 事件队列，不创建 `ask` 问题或任务。每张卡的不可猜测 action ID 绑定当前 Bot、发送时的 owner、真实聊天类型和 ID、发送回执的 message ID；处理点击时核对这些绑定及当前持久 owner，只有当前 owner 在该卡所属聊天的点击能开通。其他成员的点击不生效、不消耗卡片，以有界提示说明「仅 Bot owner 可以开通」；无效、串聊天或不匹配的事件只确认并丢弃。授权先幂等写入 `allowed_chats`（`bot_uid`、`channel_type`、`channel_id`、`granted_by`、`granted_at`），再将 ID 追加进配置文件 `allowedGroupIds`（原子重写、保留原文件权限与其余字段）；写回失败只记日志并在结果中说明，不回滚授权。持久开通后才确认事件；重复投递和重复点击不能重复开通或创建任务。成功后将卡片改为已开通状态；改写失败时发送同样的文本结果，授权仍有效。

开通点击的持久操作记录绑定事件身份、卡片与授权目标，并记录配置镜像及结果投递进度；允许列表写入与操作记录在同一事务提交。事件重投或重启后按此记录继续完成配置写回和结果投递，再确认事件；已确认的操作不重复授权或回执。恢复不依赖进程内卡片绑定，但只能恢复已经持久接受的操作，不能把一张重启后的旧卡作为新的授权。授权尚未落盘时的失败沿用事件队列有限重试；重试耗尽则按队列规则确认并丢弃，同时向聊天发送开通失败、重新 @Bot 的提示，不声称开通成功。配置写回明确失败即记录失败并在结果中提示，不撤销授权。

卡片发送在途时先核对已知的 Bot、owner 与聊天，再等待消息回执；发送最终失败或等待超过发送预算时，该点击不授权，发送文本口令提示并终结事件，不让它无限阻塞队列。

卡片绑定为进程内状态，重启后旧卡不能用于授权；owner 点击旧卡时，若该聊天已经开通则提示已开通，否则提示重新 @Bot 以获取开通方式。当前 owner 改变后旧卡不能授权。卡片发送期间到达的点击须等发送回执绑定后核对，不得仅凭 action ID 授权。部署不支持交互卡片、卡片超出能力限制或发送失败时，回退文字提示：owner 回复 `@Bot 开通本群`，mention 剥离并去掉首尾空白后须恰好等于该口令；口令消息按既有命令 claim 去重，重投不会重复开通、也不会落成任务。无论提示采用卡片还是文字，未开通聊天的该口令都能使用，保证卡片失效或重启时仍可开通。非 owner、未 @Bot、近似文本、Bot 回声与私聊的入站拒绝路径保持原样。子区开通只覆盖该子区；父群开通后其子区按继承规则随之可用。口令不是注册命令：已开通聊天收到同一文本时按普通消息处理。

口令消息的已有命令 claim 必须在已开通聊天的普通消息路由前按原消息身份检查：原开通消息重投直接去重；开通后另一条新消息即使文本相同，仍按普通消息处理。

入站记录绑定原始聊天、发送者、消息 ID、会话 ID 和目录；之后回复只使用该记录，不读取可变的“当前聊天”。群和线程输出对成员可见，不发送完整工具原始结果、模型思考或系统上下文；发送助手可见正文与真实工具执行的简短进度。进度不作为普通聊天文字，而是在首次真实工具进度时懒建同一张可编辑卡，后续按最新状态编辑；卡片不可用或状态接口故障不得阻断正文。助手正文仍可能包含用户明确要求的文件内容，不承诺自动识别所有敏感信息。

### 原生 BTW 旁路问答 SDD（2026-11-04）

**目标与入口。** owner-only `/btw <问题>` 使用 OMP 18.16.0 的原生 `{type:"btw",question}`，读取当前聊天会话上下文。权限沿用 owner、允许列表和 mention 门禁，命令沿用 durable claim 去重；不创建普通 task、不 steering、不消费 pending ask。空参数返回用法，没有已绑定原会话明确拒绝。

**协议边界。** `OmpWorker.btw(question, signal?): Promise<string>` 独占本 worker 的一个旁路请求。`btw_record` / `btw_delta` 不进入主 WorkerEvent；忽略 delta，只在同一 record 的 `complete` 终态返回权威 `answer`。启动响应只用于关联 record ID，不能当作答案。处理 running 早于 response、快速终态、其他 record、重复终态、原生拒绝、进程退出及取消竞态；error/cancelled/interrupted 不展示部分答案为成功。AbortSignal 通过原生 `btw_cancel` 撤销已发起的旁路，关闭/崩溃须结算 waiter。

**会话与生命周期。** 主任务运行时直接使用精确绑定的活 worker，不取得另一个 session lease、不排在主 prompt 后。主 prompt 先结束时，worker、chat/session lease 和资源使用保护保持至已启动 BTW 结算；旁路失败不能改变主任务结果。空闲时取得现有 chat/session lease，打开绑定的原 sessionFile，核验 session ID/file/cwd，沿用受管进程、watchdog 和 ledger，结束后确认停止再释放 lease。没有原文件不新建会话；会话切换及 commit/remove 的既有占用保护保持生效。停止/取消须中断旁路并释放 waiter、worker 和 lease，不自动重放在途 BTW。

主任务尚在准备 worker、未进入原生 prompt 时返回忙碌提示，不把 BTW 排在整条任务之后。已完成 prompt 的任务仍在释放 worker/session lease 时，只等待该 taskId 的调度收尾；随后仍须核对请求时的原绑定，绑定已切换则拒绝，不读取替代会话。

**回答投递。** 每次 BTW 以独立文本消息展示问题与完整权威答案，使用现有 durable outbox、UTF-8 安全分段及有限错误展示；与主任务消息 ID 和 task 输出完全分离，不写主任务 result、不作为 ask 回答。原生 BTW 可保存自己的 sidecar，但答案不进入主 session 消息历史。不增加 follow-up、历史列表、话题或 branch 操作。

**验收。** 先观察 core/RPC 失败回归，再实现：held 主任务期间独立答案先到；主 prompt 先结束不杀 BTW；失败不伤主任务；pending ask 不变；空闲精确会话、空参数、缺失绑定、重复命令、并发 BTW、原生失败、取消/停止/崩溃均有明确结果且无资源泄漏。运行定向 core/RPC、typecheck、全量测试、既有 restart benchmark。隔离 profile/session＋有限 localhost 模型＋本地加密 Octo 夹具执行真实 OMP 烟测，核对 BTW 不写主消息历史；不注册/重启常驻 bot、不调用付费模型，模型质量和真实客户端视觉不由本地夹具证明。

**隔离验收证据。** `bun run typecheck` 通过；独立 TMPDIR 下的全量 `bun test` 为 1491 pass、0 fail（111 文件、9020 条断言）。core/RPC 回归先观察缺失行为失败，另两条启动阶段和会话替换竞态也先复现再修复。`bun tests/benchmarks/btw.ts` 使用安装的真实 OMP、有限 localhost 模型及本地加密 Octo 完成 8 次模型请求：held 主任务旁路先答、主任务先结束仍保活、空闲精确原会话、14,419 字节答案完整分段、重复命令去重和停机原生取消均通过；主 session 历史内旁路答案为 0，剩余受管 worker 为 0。既有 restart benchmark 的 graceful/SIGKILL 两场景通过，恢复任务 2、恢复排队任务 2，会话重叠、重复副作用和中断通知均为 0。展示采用文本，真实 Octo 客户端视觉未验证；本地模型替身只证明协议、投递与生命周期，不证明模型质量。验收未注册或重启常驻 bot，未调用付费模型，未提交、推送、合并或部署。

### 原生代码提交命令 SDD

**目标与范围**：owner 通过 `/commit` 提交当前聊天绑定的单仓库、单 worktree 工作区，通过 `/commit stop` 停止该聊天当前的提交操作。只提供这两种写法；其他参数返回用法。提交由已安装 OMP 的原生 commit 路径完成，channel 不用普通模型请求替代提交，也不自行生成 Git commit。命令不推送。

**原生能力核实**：本机 OMP 18.15.0 提供 CLI 子命令 `omp commit`，没有 RPC commit 或内置 `/commit`；把 `/commit` 当 RPC prompt 会进入普通模型回合，不能作为本实现。channel 经已配置的 OMP executable 调用 `commit`，不导入 OMP runtime。保留原生默认提交策略：已有 staged changes 时使用它们，没有时自动 stage 当前仓库的改动，可能更新 changelog，并可能把同一仓库的改动拆为多个 commit。channel 不公开 `--push`、`--dry-run`、`--legacy`、`--context` 等参数。

CLI 不提供结构化提交结果，退出码 0 也可能表示没有改动，退出码 1 的降级路径也可能已经产生提交。因此运行前后读取真实 Git HEAD，结果回执区分无新提交、成功创建提交、非零退出但已产生提交，以及停止时已经落下的提交。停止通过既有受管进程组终止机制执行，不发送 RPC `abort`；中断可能保留 staged/changelog 改动或 Git lock，channel 不自动撤销或删除。

**授权与工作区**：沿用 owner、允许列表和 mention 门禁以及持久消息去重。必须存在当前聊天绑定；根据持久 workspace 的 layout、项目路径与归属判断支持范围，不解析 GROUP.md/THREAD.md。shared、多仓库或多 worktree 工作区在启动提交前拒绝，包括把绑定改到 shared 的某个项目子目录的情况。separate 布局按每个子区自己的单项目绑定处理；源仓库有其他正常 linked worktree 不构成拒绝条件。

**调度与生命周期**：提交持有聊天、会话及 canonical Git worktree 的操作期写租约，直至受管进程确认退出；同一实际工作树已有写操作时明确报告忙碌。不同聊天的普通任务彼此仍可并行，提交不能与其操作工作树内其他聊天的写任务无保护并发。提交使用现有 worker 工厂、进程组、parent-death watchdog 与持久 worker ledger，不另建无人负责的后台进程。接纳与副作用开始意图先落盘；重启先确认旧进程退出，已发出的提交不重放、不通过普通会话续接再执行；结果不确定时明确提示核对 Git 状态。

接纳将已认领的命令行在同一事务内转换为 `text="/commit"` 的 durable task，不释放并重新认领入站消息。原生提交身份由该事务保留的 `inbound.kind=task`、`command_state=completed` 及任务关联确定；`completed` 在此表示命令接纳已转移，不表示 Git 已完成。普通队列或派发正文即使恰为 `/commit`，仍按普通 OMP 任务处理。提交不创建或 prompt 聊天的 RPC 会话；ledger 的会话字段只归因于该任务的原绑定。原生进程先停在 gate，身份、watchdog 与 ledger 登记完成，且任务 `prompt_started=1` 落盘后，才允许 `run()` 释放 gate。该路径永不记录 `prompt_accepted`，因此中途重启按已有 `1/0` 不确定接受路径终结，不进入普通 `/retry` 或同会话 continuation。

独立原生 `/commit` 继续以规范化 worktree top-level 为边界持有 Git 写租约；绑定在仓库子目录也占用同一租约，不同 linked worktree 仍各自独立。普通任务彼此保留既有同 cwd 多会话并行。`/thread merge` 的自动提交复用 OMP 原生 commit 执行与反馈，但不取得该写租约，也不因并发任务、仓库/目录活动或未确认进程账本等待或拒绝；其 clean 路径、wt merge 阶段与直接 host tool 同样没有跨聊天资源排他。当前聊天/exact-session 顺序、提交后的身份与工作树状态核验、取消及未知结果语义仍适用。风险是 OMP 原生提交与并发写入交错，可能将其他写入纳入提交或导致 Git/Hook 冲突、部分副作用；不构成文件写入隔离。

**停止语义**：`/commit stop` 是即时控制路径，不取得待停止操作持有的执行队列位置。只停止同一聊天对应的独立原生 `/commit` 或 `/thread merge` 自动提交及其后续合并，不清除无关排队任务，不停止其他聊天或普通任务。停止回执以进程实际退出和提交结果为准；已完成的提交保留，不执行回滚；无活动、重复 stop、完成与停止竞争均不得误报已停止。独立 `/commit` 无法确认退出时仍按原契约保留资源阻塞；自动提交的未知结果停止本次 merge、不启动后续 wt、不自动重放/称成功，但不对其他聊天安装资源阻塞。

既有 `/cancel`（`/stop`）也会取消本聊天的提交/自动合并，并按原语义清除本聊天的排队任务；`/commit stop` 保留无关队列。`/interrupt` 对原生 `/commit` 保持既有提示“使用 `/commit stop`”的语义；对正在运行的 `/thread merge` 则按既有 merge 中断路径停止该命令及其自动提交，不对原生会话回合发送 RPC abort。

**反馈与验收**：向原聊天反馈启动、提交完成、提交失败或停止结果，错误有界且不转发原始模型/工具日志。行为回归覆盖权限、持久工作区拒绝、正常 linked worktree、资源竞争、持续目标及宿主合并之间的租约、停止边界、创建失败与关闭竞态，以及恢复。延迟重启将原生提交与等待/执行中的宿主合并均计入活动操作，合并回执处理完成后才重新评估空闲状态。真实 smoke 必须通过临时单项目 Git 仓库执行原生提交，核对 HEAD、文件内容及 channel 回执，并用受控运行场景验证 stop 和进程清理；不提交实现仓库、不推送、不部署、不重启常驻服务。

#### 原生提交进度卡实施 SDD（2026-11-03）

**输出依据**：已安装 OMP 18.16.0 的 `commit --help`、内嵌 `commit/agentic/index.ts` / `agent.ts` 和抛弃式 Git 仓库的非 TTY 管道实测。CLI 直接写出 `● Resolving model...`、自动 stage、changelog/context 探测、`● Starting commit agent...`、工具结束行、`● agent finished (...)`、fallback 与 `● Creating commit...`。非 TTY 不输出临时 assistant 刷新，但仍输出完整 assistant 正文、工具参数及内部路径，不能直接转发。实测 fallback 创建真实提交后退出码为 1，故输出中的 `Commit created.` 不构成成功判据。

**边界与事件**：`omp/commit.ts` 继续独占 gate、管道读取、进程组、watchdog 与真实 HEAD 结果；释放执行 gate 时报告开始，stdout/stderr 的各自增量解码器只把已核实的固定整行阶段映射为固定中文摘要，工具仅提取已核实白名单名称与结束状态。动态正文、diff、错误详情、模型输出和参数一律丢弃。每条输入行有固定上限，超长行整体丢弃至下一分隔符；UTF-8、ANSI 控制序列及 CR/LF 可跨 chunk，重复刷新不重复记录。

**单一展示链**：`CommitWorkerOptions.onProgress` 同步报告安全的 `commit_progress` 执行事件；回调异常与卡片 I/O 不影响管道排空或 CLI 结果。`CoreService.runCommitTask` 使用现有 `ProgressCards.startTask` / `TaskProgress.apply` / `finish` / `stop` / `tail`，不创建 RPC worker 或 agent 回合。`ExecutionTracker` 的提交快照保留当前摘要、最近 6 条去重步骤、服务依据真实结果产生的终态摘要及经净化限长的提交内容；既有 durable snapshot 编解码与恢复继续负责收尾。卡片标题明确正在提交，采用现有 builder、能力门禁和预算搜索；不以工具调用或 todo 假装原生阶段完成。

**刷新与终态**：实际开始事件开一张卡，所有后续事件更新同一卡，运行中编辑沿用 1 秒合并限频。无新阶段的长命令每 10 秒更新实际耗时，不推断进度或百分比。只有原生进程退出且既有受管进程组退出确认后，才依据 `CommitResult.exitCode/headBefore/headAfter` 呈现完成、无新提交、失败、停止及已产生提交的结果。停止未确认的卡显示结果未知，不写成已停止；保留 watchdog、ledger、目录阻塞与 shutdown 契约。最终文本回执保持独立，卡片不可用、创建/编辑失败不阻断执行、停止或回执。

**提交内容**：终态结果之下展示实际 Git commit message（标题及正文）和本次提交范围内根目录/嵌套 `CHANGELOG.md` 的新增正文，随后展示最近步骤。来源只限已确认变化的 `headBefore..headAfter`；首次提交涵盖初始树，不读取工作树未提交内容，不从 stdout 拼接内容，不展示代码 diff、diff 元数据或内部绝对路径。commit message 合计上限 800 个 Unicode code point，changelog 合计上限 1,200 个；保留换行，截断时附“已截断”标记且计入该上限。凭据沿用展示净化边界；内容读取和脱敏先于持久化，恢复与降级仍执行相同上限。HEAD 未变时不增加内容区；非零退出或停止后已产生的提交同样可展示其真实内容。内容读取失败不改变已经确认的退出码/HEAD 结果，不影响最终文本回执。

**验收**：先增加并观察失败的 parser/worker/core/card 行为测试，再实现。覆盖跨 chunk UTF-8/ANSI/CR、重复与大量输出、敏感内容不投影、单卡持续更新与静默耗时、真实结果各分支、停止失败隔离、卡片故障下仍排空和交付。运行 native commit/core/card 回归、typecheck、全量测试与离线 restart benchmark；抛弃式真实 Git+受管子进程验证提交和停止，真实 CLI 使用隔离 profile 与 localhost 模型替身。必要预览只投当前子区，不注册真实 Bot 连接；本任务不提交、推送、合并或重启常驻服务。

**进度卡与内容验收记录**：`bun run typecheck` 通过；native commit/core/card 专项 170 pass、0 fail，完整 `bun test` 为 1,201 pass、0 fail（85 个文件）。离线 restart benchmark 的 graceful/SIGKILL 两种场景通过，session overlap 和 duplicate effect 均为 0。抛弃式真实 Git、安装版 OMP、localhost 模型替身与 `CoreService` 完成成功、无新提交、退出码 1 但已生成提交、pre-commit hook 阻塞时停止四条路径；4 次操作各创建 1 张卡，共编辑 11 次，无 RPC prompt，受管进程组均退出。实际生成的 `fix: updated tracked value` 标题/正文及新增 changelog 条目进入同一张终态卡，旧历史和未提交内容未进入投影。当前子区只发送 1 张 Octo 预览卡并在原卡上更新内容，未注册真实 Bot 连接；最终 wire envelope 为 28/200 节点。实际 builder 的 340px/440px 本地渲染已检查，Octo 发送与编辑回执已确认；本地 renderer 不等于真实客户端像素验证。既有预览结构测试 7 pass，4 个需 token 的 live case 跳过；上述单卡实发使用当前 Bot 的已验证 `octo-cli` 身份完成。


隔离验收使用临时 channel 实例、`FakeTransport`、真实 OMP CLI 和临时 Git 仓库：普通 `/commit` 创建 `d986aec1b887cef130272b7bffe8e26c7dddcd76`，文件内容与 clean 状态核对通过，channel 收到启动和 `HEAD=5e388075→d986aec1` 完成回执。真实 pre-commit hook 阻塞期间发送 `/commit stop`，受管进程组确认消失、HEAD 保持 `d986aec1…`。separate 的单项目 linked worktree 创建 `8908099f91f694da18132de8fd006aad5ba748fe`，源工作树 HEAD 保持 `46588bc3…`，linked 工作树 clean，channel 收到对应提交结果。该验收没有连接真实 Octo transport，不代表常驻服务已加载此实现。

最终检查：`bun run typecheck` 通过；专项命令/goal/宿主合并/worker 生命周期测试为 117 pass、0 fail；`bun test` 为 1149 pass、0 fail（83 个文件）。`bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两个真实 CLI 恢复场景均通过，恢复任务与队列各 2 次，session overlap 和 duplicate effect 均为 0。`bun tests/benchmarks/goal.ts` 通过真实 OMP、隔离 profile/session 和 localhost 模型验证有限目标完成、pause/resume、drop、graceful/SIGKILL 恢复及 disabled 门禁，23 次本地模型请求，无重复目标创建。回归同时覆盖已派发提交不重放，以及未派发的 `0/0` 提交在未确认 worker 阻塞期间继续保持 queued。

### 命令

channel 支持下列命令，`/help` 展示用法；`/retry` 经 OMP 原生命令透传，其余由 channel 自身命令注册表处理。这里不依赖 Octo 客户端提供命令菜单 API。

| 命令 | 行为 |
| --- | --- |
| `/new [目录]` | 创建并绑定 fresh OMP 会话；省略目录时按目标默认目录解析，无可用绑定时使用 `~/tmp`；成功回执含 session id、cwd 与该会话实际生效的模型（`已创建会话 <id>，cwd=<目录>，模型=<provider/id[:level]>`，未指定聊天模型时取 OMP 实际默认；模型查询失败或未报告时以不含模型的回执送达）；切换成功时把本次切换时间写进该聊天的绑定（`chats.session_switched_at`），问答卡据此判定切换前提出的旧问题不再接受迟到提交；切换失败不改动绑定，也不记切换 |
| `/reset [目录]` | 与 `/new` 使用相同的目录解析，但明确重置为 fresh 会话；原会话历史文件保留；与 `/new` 共用创建路径，成功回执同式（含实际模型），会话切换时间同样记录 |
| `/move <目录>` | 调用当前会话的 OMP 原生 `/move`，校验刷新后的 session state，沿用同一 session ID、历史和附件 |
| `/fork [子区名称]` | 从当前主群/子区精确持久绑定即时 native fork，创建同父群新子区并绑定独立 session，共用 cwd；源回合继续，新子区等待实际新消息再 resume |
| `/tan <用户请求>` | 即时分叉当前主群/子区已落盘会话，在同父群独立子区执行请求；结果与进度在子区，目录共享 |
| `/watch [list\|<id>\|stop <id\|all>]` | owner-only 宿主控制当前聊天的持久 watch；查看、停止不调用模型，不回答 pending ask。订阅默认 5 分钟检查、6 小时到期，显式停止独立于源任务取消或会话切换 |
| `/thread <任务>` | 交给助手处理：助手自行确定子区名称与 `worktree_name`，新建带独立 git worktree 的 CommunityTopic 子区并把任务派发进去（派发正文按 SDD 格式写；清晰到可直接生成 SDD 的需求直接开工，否则发起会话只做够派发的最少澄清、剩余澄清由子区按 SDD 格式进行）；只在群聊中可用 |
| `/thread model [选择器\|clear]` | 仅 owner 在主群查询/设置/清除所属子区默认模型；原生 fresh probe 解析成功后持久化，主群自身模型不变；下次子区 worker 启动生效，不切换运行中会话 |
| `/thread merge` | 沿用目录的 ready fork 子区仅归档 Octo。普通隔离 worktree 子区由宿主执行 wt 合并/清理，不按资源占用限制并发；成功零 agent 任务、回执送达后归档；仅实际执行失败交给本聊天 agent 修复。尾随文字作为 owner 上下文，不拼入 shell |
| `/thread remove [--force]` | 沿用目录的 ready fork 子区仅归档 Octo，保留本地资源。普通 single/separate/shared 子区清理持久拥有的 worktree 及分支后归档；默认保护 dirty 和未合并提交，force 授权丢弃。其他聊天活动、仓库/目录活动和旧未确认 process ledger 不构成等待/拒绝门禁或等效路径锁；仍验证持久归属与实际路径/来源/分支，遵守当前 chat/session 顺序与取消。部分或未知结果不归档、不重放、不称成功 |
| `/queue <文本>` | 把该文本作为排队任务追加在当前任务之后，不受当前追加模式影响；正文经与普通任务相同的组装点，随消息发来的附件以本机绝对路径进入排队任务的正文 |
| `/mode [steer\|queue]` | 设置/报告该聊天的追加模式：`steer`（默认，`default` 同义）把运行中的新消息注入当前回合，`queue` 把它变成排队任务；无参数时报告当前设置，部署声明 `octo/v2` 时改发 owner-only 配置卡片（一行选择 + 一次提交；卡片不可用时回退为同一段文本） |
| `/switch [provider\|模型]` | 查询实际模型、下一次启动策略与来源；无参数查询全部 provider，参数精确匹配 OMP 当前目录的 provider 时仅查询该 provider。两种查询都只读、最多展示 100 个模型（default 控制项另计），卡片与文本标明范围及展示数/总数，超过时明确「仅展示前 100 个，共 N 个」并引导筛选或直接 `/switch provider/model`。`default` 优先清除显式覆盖，子区重新继承主群子区默认，否则不指定参数、由 OMP 自行决定。其余参数由 OMP 原生解析并切换（`provider/id`、模型 id/子串、`@角色`、`:level`），成功后持久化。运行中使用活会话，空闲用 fresh probe；菜单不可用时同源文本回退。 |
| `/goal`、`/goal show` | 查询本聊天绑定的原生目标，显示 goal ID、目标、状态、用量和预算 |
| `/goal set [--budget N] <目标>` | durable 接纳后结构化创建并持续执行；预算为正整数 token；已有目标时拒绝替换 |
| `/goal pause`、`/goal resume`、`/goal drop` | 暂停后续续跑、恢复或删除目标；控制请求不排在持续目标后面，启动/恢复遵循任务和目录串行 |
| `/goal budget [N\|off]` | 无参数查看预算；创建后修改/清除明确反馈原生 RPC 不支持 |
| `/retry` | 在当前绑定的 OMP 会话中重试上次失败或中止的回合，由 OMP 原生实现判定与恢复；与其他原生 slash 输入相同，忙时按独立任务排队，执行时以会话当时的状态为准 |
| `/commit` | 用 OMP 原生 commit 提交当前聊天绑定的单仓库、单 worktree 工作区；持久 shared/多项目归属或资源忙碌时拒绝，不推送 |
| `/commit stop` | 即时停止本聊天当前提交，确认进程清理后报告结果；不停止无关任务、不回滚已完成提交 |
| `/interrupt` | 中止当前聊天的运行中回合，保留已排队消息与它后面的排队任务；不终止 worker |
| `/cancel`（`/stop` 同义） | 取消当前聊天正在执行的任务，并清除该聊天待执行消息 |
| `/status` | 只读状态卡：当前会话、cwd、任务/队列/待答问题、实际模型与启动策略、当前 context 占用、逐项目 Git 净变更及已有 PR/MR 引用；卡片不可用时同源文本回退 |
| `/restart [now]` | 通过检测出的守护进程重启 channel；允许在途及排队任务存在，旧 worker 确认退出后沿原会话续接任务，再执行队列；两种写法行为一致 |
| `/autoresearch-history` | 只读导出：把本聊天最近一次研究运行的完整记录作为文件经既有 outbox 投递；随发随执行，不打断、不并入也不排队在运行中的任务之后；没有研究记录时只回一句短提示 |
| `/help` | 按会话、子区、任务、模型和其他分类展示命令用法 |

当前聊天有待回答问题时，owner 在该聊天的非命令回复就是答案本身：合法选项编号按选项校验，其他文本原样作为自填答案；没有待回答问题时同一文本按普通任务输入处理。待回答问题的答案优先于追加模式，不能被 steering 或排队吞掉。`thread-dispatch` 交付到子区的内容是显式交接而不是 owner 的回复，不作为该子区待回答问题的答案。问题以交互卡片投递时，owner 对该卡片的一次按钮点击同样是这个问题的答案：点击与文本回复共用同一个答案 compare-and-set，只有一方生效，其他人的点击只 ack 不生效，卡片只是多一条作答路径而非唯一路径。

追加模式决定该聊天在已有运行中任务时如何处置新消息，按 `botUid` 与真实聊天目标隔离并持久化，跨重启保留，未设置时默认 `steer`。`steer` 模式下的普通文本经持久 claim 后作为 OMP `steer` 帧注入当前运行中的回合，与当前回合共用同一条任务和正文流；每条被注入的消息都是新的一轮，进度随之在新卡片上继续（见进度卡章节）。`queue` 模式下的普通文本走既有 durable acceptance，成为在该任务之后执行的排队任务，保留独立的正文消息与进度卡。OMP worker 只在其回合仍在流式输出时接受 steering：没有运行中的回合、回合已经结束或 OMP 拒绝该帧时，该文本回退为普通排队任务，不静默丢弃。`/queue <文本>` 始终走排队任务路径。把正文交给助手的命令与普通文本走同一条追加路径：`/thread <任务>` 的交付说明同样按该聊天的追加模式处置（运行中的回合能接收 steering 时并入该回合，否则回退为 durable 任务），`/queue` 则是任何模式下都排队的显式入口。`thread-dispatch` 交付到子区的内容按目标子区自己的追加模式处置：子区运行中的回合能接收 steering 时并入该回合，否则成为该子区的 durable 任务，工具结果与发起群回执都如实报告这次交付。追加模式因而是每个聊天普通正文唯一的处置开关，到达该聊天的普通正文都由它决定并入回合还是成为任务；未注册的 slash 原生命令（如 `/autoresearch`）是显式例外——它们始终按下文所述的独立任务路径接受（`acceptTask`），不经 steering 并入运行中的回合，因此任务运行中发送 `/autoresearch off` 这类开关命令是排队执行、而非即时生效，需要立刻停止当前工作时先 `/cancel`（该命令同时清除排队消息）再发送。steering 不产生新的任务记录或正文消息，也不改写当前回合已建立的群说明快照。

追加消息的可见性以「被消费」为准，而不是「投递成功」：`steer` 帧返回成功只表示消息进入 OMP 的内存待注入队列；OMP 在注入边界把它并入回合上下文（表现为一条 `role: user` 且 `steering: true` 的用户消息进入事件流）时，channel 才在该聊天补一行「已并入当前回合 ▸ <正文前 20 字>」回执。`queue` 模式与 `/queue` 产生的排队任务，只有接受时该聊天或目录已在忙、因而确实等待过的，才在真正从 `queued` 变为 `started` 时补一行「开始执行排队消息 ▸ <正文前 20 字>」；尚未被消费或被 `/cancel` 清除的消息不产生回执；重启后持久队列按序继续，OMP 尚未消费的内存 steering 不承诺恢复。回执正文取该消息实际注入回合的正文（含引用/附件块）的单行前 20 字，超长以省略号截断，空文本只发前缀。

`/interrupt` 先持久化该任务的停止意图，再向运行中的回合发送 OMP `abort` 并保留同一 worker；确认成功才回复中断确认，明确拒绝时撤回意图并报告失败。不取消排队任务、不清空已追加的消息。回合因中止结束且其后没有真实工作时落 interrupted、进度卡落 cancelled；其后确实产生正文或工具工作则清除停止意图并按正常完成结算。进程在确认中断后崩溃时，恢复按已落盘的意图结算，不能将该任务自动续跑。

`/cancel` 与 `/stop` 是控制路径：发送 abort、等待 worker 停止，并清除该聊天的待执行消息与排队任务，因此同时也丢弃尚未消费的 steering 与排队消息；不回滚已经发生的副作用。

`/restart` 由 channel 按自身 PID 检测出的守护进程执行（见 `2026-09-16-oco-cli-entry-design.md`）；未检测到时明确拒绝，不猜测重启机制。命令以独立会话派生，守护进程负责停止 channel；命令没有停止进程时 channel 继续服务并记录告警。`/restart` 与 `/restart now` 都允许忙时重启，发起聊天收到一次请求回执。停机保留在途任务与队列，新进程先核验并停止遗留 worker，再沿任务原 session 续接，随后执行队列；任务恢复成功不另发中断、取消或恢复完成通报，服务启停本身按生命周期通知规则向 owner 私聊发送时间与 PID。无法确认旧进程停止的目录保持阻塞；session 身份或 prompt 接受状态不明时明确报告恢复失败。可续接任务继续编辑原进度卡，并保留待答问题。

`/switch [provider|模型]` 按聊天目标查看与切换模型。无参数查询全部 provider，精确 provider 参数只读筛选目录；其余选择器由 OMP 原生 `/switch` 解析，成功后按 bot 和聊天目标将规范 `provider/id[:level]` 写入 `chat_models`，失败不改配置。显式选择在运行中直接切换本聊天 live 会话，空闲时复用 fresh 一次性探测 worker。查询从 live 会话读取实际模型，否则探测目录，并单独显示下一次启动策略与来源。探测使用临时会话目录、既有 worker ledger 与停止回收流程，不读写聊天会话。

主群 `/thread model [选择器|clear]` 统一管理所属子区默认。空参数查询；未设置明确「不指定，由 OMP 自行决定」。设置始终经 fresh probe 原生解析，不使用父群 live worker、不改变父群 `/switch` 模型；`clear` 仅删除默认，不解析为模型名，不删除子区显式覆盖。数据保存在增量创建的 `thread_models`，主键 `(bot_uid, group_id)`，旧聊天配置不复制。仅 owner 主群可用；DM/子区明确拒绝。精确 `model` 首词加空白/结束才路由控制命令，`model-fix` 等保留任务意图，`merge` 行为不变。沿用 durable claim，pending ask 不被回答、附件不下载，修改默认不污染 `/switch` 菜单。

有效启动策略由状态层统一解析：本聊天显式 `/switch` > 所属主群专用子区默认 > 不传 `model`。主群自己的模型从不成为继承来源。普通任务及恢复、goal 执行/控制、commit、`/new`、`/reset`、`/move` 均在 worker 启动时读取该策略；无值时完全省略参数，resume 会话模型处理交给 OMP。默认变更动态作用于已有和新子区的下一次启动，不中断/批量切换 live 会话。子区 `/switch default` 只删自身覆盖、重新继承；无父群默认时由 OMP 自行决定。回执与 `/switch`、`/status` 区分运行中的实际模型与下一次启动策略。配置按 bot/主群隔离，跨重启保留。详细 SDD 与验收见 `2026-09-17-switch-model-command-design.md`。

#### `/status` 上下文与仓库状态卡 SDD（2026-11-04）

- **目标与范围**：owner 在当前聊天查询既有会话、cwd、任务状态、操作、目标、追加模式、实际模型与下次启动策略、队列、待投递/未知投递和待答问题，并查看当前绑定 session 的 context 占用和工作区 Git 净变更。仅增加只读状态卡，不增加交互动作，不改变进度卡。
- **数据所有权**：OMP worker 层负责原生只读 RPC 的解码与当前 session 身份；`session/status-context.ts` 负责选择活会话与绑定 session 的持久数据、区分来源和时效。`workspace/git-status.ts` 负责只读 Git 查询与持久 workspace/project 到实际 worktree 的映射。`cards/status-card.ts` 纯渲染卡片及同源文本。服务只编排已有状态与这两类查询、投递和停机取消。
- **上下文不变量**：OMP 18.16.0 的 `get_state.data.contextUsage` 原生返回 `tokens/contextWindow/percent`，同一响应的 `model` 提供实际模型；不转发响应中的 system prompt。运行中只查询当前精确绑定的活 worker，不等待主任务结束、不发 prompt/steer、不创建第二个 worker。活查询及正常任务结束、worker 停止前将最后观测保存到 channel SQLite 的精确 bot/聊天/session/file 快照；空闲核验真实绑定文件后读取这份快照，标注记录时间和「非实时」。旧会话没有快照时明确未知，不启动 fresh probe。已用上下文、模型上下文上限和有依据的比例独立于整段会话累计 input/output/cache token 消耗；外部文件变化及压缩后的历史不得声称当前占用。模型启动策略不充作该历史模型的真实上限。
- **主指标**：卡片优先显示当前 context 的「已用量 / 模型上限 · 占用比例」，如 `121k / 200k · 60%`；数值取原生 `contextUsage` 的占用估算，不展示会话累计用量作为此指标。来源、记录时间和历史/未知含义紧贴指标，历史不能以实时主指标的样式冒充当前值。
- **Git 口径**：以当前 worktree 的 `HEAD` 到工作树的 tracked 净变化计算 `+N/-N`，不相加 staged 与 unstaged，因此同一行只计最终净变化。发现 Git root 后统一查询整棵工作树，绑定 cwd 位于子目录时也不漏掉根目录或其他项目路径；命令级固定 `diff.relative=false`。无提交仓库使用空树基线并标注。未跟踪文件只列文件数并明确未计入行数，二进制只列文件数而不伪造行数。non-Git 正常显示无仓库统计；查询失败或受限显示不可用而非零。查询禁用 external diff/textconv 和仓库 fsmonitor hook，不暂存、提交或改配置。
- **项目身份**：从当前聊天持久 workspace 记录识别 single/separate/shared；逐项目使用记录中的真实 worktree 路径和项目名，shared 入口不作仓库、不查询父群来源 repo。路径未就绪或已丢失按该项目报告不可用。没有 workspace 记录时仅查询当前绑定 cwd；显式切换 cwd 不改变持久资源归属。
- **关联引用**：读取现有 `getThreadReferences(botUid, target)` 的本子区持久 PR/MR canonical URL，以类型和链接显示；不为状态查询识别新引用、扫描远端或读取其他子区。引用均为空时，卡片与文本完全省略 reference 区域，不显示空标题或占位。
- **并发、授权与投递**：沿用 owner、allowlist、mention 和命令持久去重，在 pending ask 和任务 steering 之前处理；不消费回答、不取得普通目录排他锁。OMP/Git 查询有界并响应停机，Git 不获得写权限。使用既有 `sendCard` 的 `octo/v1` 能力/信封限制；卡片只依赖 TextBlock。发送不支持、超限或失败时，同源状态文字进入既有 outbox。安全显示沿用凭据脱敏与有界字段，不输出原始协议或 Git 错误正文。
- **验收计划**：实施前观察缺失行为回归失败；覆盖运行中 held task/ask、空闲原 session、缺失/陈旧/压缩边界、卡片回退、owner/去重、多项目身份和真实临时 Git 的 staged/unstaged 净差。实施后执行专项 RPC/命令/卡片回归、`bun run typecheck`、`bun test`；真实安装 OMP 配合有限 localhost 模型及本地 Octo 协议夹具，验证及时只读查询、任务继续和原 session 不被替换。真实 builder 输出以浏览器渲染并记录桌面/窄屏证据。验收不注册或重启常驻 Bot，不调用真实模型，不修改 Loop/Fleet 业务数据；真实客户端渲染检查仅向本子区投递标明固定示例数据的状态卡。

**已观测验收证据**

- 真实 builder 的 live、persisted、stale、未知 context、未知上限、shared＋引用六种状态，分别以 440px/340px 渲染，共 12 次；渲染错误与横向溢出均为 0。最大样本为 23 个节点、3 层深度、完整信封 3480 字节。
- 当前 Bot 使用既有凭据投递一张固定示例卡（消息 `2117881590603223040`），未重新注册。真实 Octo Web 客户端显示 `121k / 200k · 60.5%`、两项目差异和两个正确 PR/MR 链接；实测卡片宽 442px、高约 514px、子元素横向溢出 0。截图只保留卡片区域，示例不代表当前 session 数值。
- 对状态库的只读一致性副本运行应用迁移两次，原有 27 张表、13310 行的字段和内容摘要全部保留，新增 context 快照表，`integrity_check=ok`；私有副本已删除。
- 隔离 restart benchmark 的 graceful/SIGKILL 均恢复原任务与队列，重复副作用、会话重叠和中断通知均为 0。
- 实施前已观察状态卡、逐项目 Git、PR/MR 引用及模型上限缺失的失败回归；最终使用独立临时目录串行执行：专项 227 项通过，`bun run typecheck` 通过，全量 1511 项通过（116 个文件、9096 条断言）。
- `bun tests/benchmarks/status.ts` 使用已安装 OMP、有限 localhost 模型及本地加密 Octo 夹具：held 任务期间状态读取不创建或停止 worker、不改变 session，随后任务继续完成；持久快照逐字段匹配最后一条原生 `get_state`，空闲状态标为 persisted，模型请求总数为 1。
- 实际客户端像素验收覆盖 Octo Web；340px 窄屏验收使用本地 renderer。原生移动客户端未做像素验收。context 数值是 OMP 的当前占用估算，空闲快照始终标明最后记录与非实时，不承诺 provider 精确计量。



#### `/switch` provider 目录与 100 模型展示 SDD（2026-11-03）

- **目标与范围**：无参数查询全部 provider；参数精确等于当前 OMP 目录中的 provider 标识时只查询该 provider。`default` 清除语义优先，其余非匹配参数继续交给 OMP 原生选择器；不实现分页、自动补全或新的模糊解析。
- **所有权**：`ModelSwitchController` 从运行中 worker 或聊天 cwd 下的 fresh probe 获取实时目录、判定精确 provider、筛选并生成菜单视图。`model-switch.ts` 独占模型选项整理、100 模型上限及范围/计数文案；同一组有效、去重后的模型同时生成卡片和文本，default 控制项另计。`command-card.ts` 渲染实际模型、下一次启动策略及目录说明；`command-cards.ts` 继续独占 owner、代次、TTL、重复点击与串行应用。
- **流程与不变量**：每次命令先取代次票据、失效旧菜单，再读取目录。非空请求保留到达顺序串行；一次 probe 完成目录判定及必要的原生解析，停止并清理后再发布或持久化结果。查询只读，不调用 `switchModel`、不写 `chat_models`、不改变运行中模型、不起 agent 回合、不回答 pending ask。点击仍调用既有 `applySwitch`，原生解析成功才写模型；模型来源和继承策略不变。
- **展示**：卡片和文本共享目录说明，标明「全部 provider」或具体 provider、真实总数和展示数，同时区分查询到的实际模型与下一次启动策略。100 个完整展示；超过时明确「仅展示前 100 个，共 N 个」，说明其余未列出，引导 `/switch <provider>` 精确筛选或直接 `/switch provider/model`。保留目录顺序，default 不占 100 个模型名额。空目录也显示范围和 0/0。
- **验收**：多 provider、全局/provider 各 100/101 边界、default 与 provider 同名优先级、原生选择器/角色/级别/拒绝、空闲 probe 停止清理、运行中只读及点击应用、owner/重复/过期/旧代次、慢查询被新查询取代、pending ask 和文本回退。先运行缺失行为的失败测试，再实施；专项回归、typecheck、全量测试及真实 builder 的 100 模型卡渲染/提交 smoke。检查含 `plain` 的完整 wire 字节预算及上游 ChoiceSet 节点规则；本地渲染不等于真实 Octo 客户端验收，不建立竞争 bot 连接。

一次性模型探测抛错且 worker 停止未获确认时，channel 使用已记录的进程身份执行恢复回收；确认停止后结束 worker 台账，否则保留台账并阻塞工作目录。

`/switch` 查询（无参数或精确 provider）与 `/mode` 无参数回执在 `octo/v2` 部署以 owner-only 配置卡片投递。`src/cards/command-card.ts` 是纯渲染；`src/commands/command-cards.ts` 独占菜单生命周期。卡片为一行 `Input.ChoiceSet`（id `config_value`）和唯一 `Action.Submit`（id `cfg:<类型>:<会话 id>`）；`/mode` 取值为 `steer`/`queue`，`/switch` 为 default 加最多 100 个有效、唯一的 `provider/id`。卡片可见目录说明与文本同源；帧的 plain 在不支持、拒绝、发送失败或无回执时原文回退。菜单不持久化，上限 200 张、TTL 15 分钟、跨进程失效；owner 从状态盘当次读取，并验证发布 owner、messageId、真实聊天及 actionId，非 owner 或错绑点击静默 ack、不消耗卡片，发送回执在途时保留重试。只接受本卡发放过的取值，应用仍走既有 handler。文本及点击同聊天同类命令串行落地；请求先持有代次票据、使旧菜单失效，查询或发送期间被更新命令（含点击）取代的结果不得再发布，票据归还即回收状态。串行执行前复核停机、owner、聊天允许范围，失败只回有界提示。旧卡、过期和跨进程残留只给重开指引；点击先失效再应用，重投不重复落库。终态卡无输入及动作，展示真实应用回执，编辑失败按同文文本投递。

群中使用 `@Bot /new <目录>`、`@Bot /reset <目录>` 或 `@Bot /move <目录>` 等相同命令；剥离已验证的 Bot mention 后解析。命令与 mention 的先后顺序不固定：客户端可能发送 `@Bot /command <参数>` 或 `/command <参数> @Bot`，解析前剥离全部已验证的 Bot mention，无论它位于命令前、命令后还是参数中间，并清理剥离处残留的空白。结构化 span 与文本对不上或缺失时，退回按 mention uid 列表匹配独立出现的 `@BotName` 或 `@BotUid` 词元（两侧为空白或文本边界，允许尾随冒号）；未指向当前 Bot 的文本型 `@` 不剥离，用户正文中的其他 `@` 保持原样。命令只允许 owner 发起：成员发来的 `/` 开头消息一律不执行、也不交给 Agent（OMP 会在 RPC prompt 路径对内建命令做分发，透传等于把命令面暴露给成员），同一聊天 10 分钟窗口内至多回一条提示「命令仅 owner 可用；需要助手做事请直接 @<BotName> 说明。」，并按既有命令 claim 去重、重投不重复提示。channel 内置 `/new`、`/reset`、`/move`、`/thread`、`/queue`、`/mode`、`/switch`、`/commit`、`/interrupt`、`/cancel`、`/stop`、`/status`、`/help`、`/restart`、`/autoresearch-history` 始终硬拦截，不发送给模型；若存在同名观察器也不能改变这一安全语义。其他 slash 输入按原始 `message.text` 作为普通 OMP task 透传，未知命令不再返回“不支持的命令”。

`/fork` 同属 owner-only channel 控制命令：在 pending ask、steering 和 OMP slash prompt 路径之前硬拦截，仅在已授权主群/子区执行；不生成任务或回答源问题。

`/goal` 同属 owner-only 内置硬拦截命令；命令参数转为持久结构化请求，不进入普通模型 prompt。控制接受（原生响应及 exact goal ID）与实际 `agent_start` 生成接受分别记账；RPC 前先持久化派发意图，缺少控制接受的已派发请求不重放。暂停、删除、中断、取消及 ask 超时先撤销持续授权，取消该聊天尚未执行的目标启动/恢复请求；普通排队消息沿用各停止命令既有规则。目标恢复按原会话及 exact goal ID，停止意图优先，不通过普通续接正文替代原生目标。

`/retry` 保持 OMP 原生语义（v18.2.6）：会话末尾可重试的失败或中止回合由 OMP 恢复，成功时回执为 `Retrying the last failed turn.`，没有可重试回合时为 `Nothing to retry.`。channel 沿用绑定会话与既有 slash 任务路径，不从任务表复制上一条 prompt；owner/允许列表/mention、持久去重、会话串行与群历史首行保护保持不变。运行中的消息不会把 `/retry` 当成 steering；需要先中止当前回合时可使用 `/interrupt`。worker 把当前 prompt 分派期间、响应确认之前的 `command_output.text` 转为 info 通知，通过既有有界通知 outbox 单独投递；帧无上游 id，使用请求 id 与输出序号生成唯一消息 id。确认后的旁路命令输出（如回合运行中的 `/switch`）不归入该回合。无生成的 `agentInvoked:false` 正常本地完成；成功重试的 `agentInvoked:true` 等待真实 `agent_end`，助手正文与进度照常交付。

OMP 原生 `/autoresearch` 经上述 slash 任务路径执行：无参数开启会话内研究模式，已开启时再次发送则关闭；`off` 关闭；`<goal>` 经扩展的 `api.sendUserMessage` 启动或恢复研究。`clear` 关闭并清空研究状态，在 `autoresearch/*` 分支默认硬重置回基线并清理未跟踪文件（没有基线则通知跳过 Git 重置）；`--reset-tree` 在其他分支强制执行同一重置，`--keep-tree` 跳过 Git 重置。OMP v18.2.6 的所有 `clear` 变体仍会删除 `autoresearch.sh` 等遗留研究文件与 `.autoresearch` 目录，channel 不增加确认或撤销。`agent_end.willContinue:true` 的后续回合继续在同一任务中流式投递。`extension_ui_request.notify` 转为带级别的独立通知，使用持久 outbox，重复帧复用稳定消息 ID，不混入助手正文、任务结果或中断归因；活动回合外的通知忽略。无模型生成的本地命令由明确的 `local_command` 事件识别，完成时保留待提交群说明，直到下一次真实生成获得 prepared receipt 后提交（验收 40/41）。研究状态、实验工具和 Git 操作由 OMP 管理；Octo 展示通知、助手正文与工具进度卡，OMP TUI 自定义仪表盘不通过 RPC 提供。

对提供现代结算字段的 OMP，所有生成任务在 `session_settled` 或对应 `prompt_result.sessionSettled:true` 后才清 host tools、结算任务并释放目录；不能仅凭 `isTerminal` 或 `yielded` 提前收尾。原生目标即使静止仍可能未达成：保留预算受限状态，将仍 active 的未完成目标原生暂停并如实反馈。原生确认完成/删除后的当前状态投影清除，历史任务审计保留，同一会话可继续创建新目标。重启恢复在途目标先原生 `/retry`，仍有授权的原目标才 resume；不存在可重试回合时不发送通用 continuation prompt。

channel 在工具进度之上补两块由权威实验状态驱动的只读呈现，不从工具参数或文本猜测来推测实验结果：其一，权威 `research_update` 事件参与该轮进度卡——研究帧自身即可开卡，轮次更新沿同一张卡推进，卡面研究带常显摘要（目标/指标/基线/最佳/改善/轮次上限与研究 phase + 任务终态），最近 6 轮记录可折叠展开（与工具轨迹、终态折叠相互独立），卡片节点预算不足时展开行按 6→3→1→0 回收并以省略数明示，研究摘要永不牺牲；研究快照的 recent 窗口最多 32 条当前段记录，旧记录 runNumber 为 null 时保留为未编号行、既不丢弃也不伪造轮次号。任务终态摘要以执行状态覆盖可能过期的研究 phase，不展示与当前状态矛盾的阶段文案；纯文本（无卡片能力的部署）回退同样携带研究摘要与最近行。steer 滚出的新卡与 todo 结转同理，从该任务最近一次研究快照在滚卡时刻起步，而新普通任务的卡不带旧研究。其二，权威 init/log 帧上的脱敏完整记录（跨全部段，不含 ASI、notes、命令、原始输出与本机路径）按 bot+真实聊天私有落盘（仅状态盘 `research-reports/`，原子替换 latest，同聊天写入严格有序，stop 等待已排队写入，写失败只记日志、不拖垮产生它的任务，绝不读 OMP 原始数据库）；报告不超 1 MiB，超限时按整行保留最新记录、标注省略条数（不切半行半字符、不保留最旧段），跨段记录的数值列用中性「指标值」而非当前段指标名，上游 results 超过 10000 行直接拒绝，不伪造聚合。`/autoresearch-history` 是 owner-only 内置只读命令，经既有命令门与 messageId 级持久 claim 去重，运行中也立即执行，不启动模型、不回答待答问题，把该聊天最近一份完整记录以不可变 spool 附件放入既有文件 outbox——入队字节固定，之后的报告重写碰不到待发附件；入队失败只清理自己这一个 spool 目录后走既有命令失败路径；重启后仍可导出，其他聊天与其他成员的请求只得到无泄漏的拒绝或提示。

可通过服务选项注册自定义 `ChannelCommandHandler`：`name` 去除首尾空白并按小写规范化，`intercept` 明确决定是否消费，`handle(context)` 接收原始 `InboundMessage`、规范化命令名和解析后的 argument。`intercept=true` 在持久命令去重后执行 handler，成功后消费且不启动 OMP；失败保持可重试的命令状态且不启动 OMP。`intercept=false` 先完成现有任务 durable acceptance，再调用 handler 并继续透传原始文本；handler 异常不得阻断任务或 prompt，重复消息不得重复 handler 或 task。handler 必须经过既有 owner、群/线程 mention 和消息 ID 校验；空名、非法名或规范化后的重复注册在创建服务时拒绝，不能静默选择。

`/new`、`/reset` 的完整剩余文本是一个路径；可移除一对匹配的外围单引号或双引号。支持绝对路径、当前运行账户的 `~/`；拒绝相对路径、其他用户的 `~name` 和空引号。路径不经 shell 执行，不展开环境变量、通配符或命令替换；特殊字符按文件名处理。

省略目录时的默认目录按目标解析：① 已有已验证会话或显式隔离工作区绑定（`cwd_pinned`）沿用规范化 cwd；② 未建立会话且没有隔离绑定的 CommunityTopic 线程取父群当前绑定 cwd；③ 首次无可用绑定时取 `~/tmp`。父群由真实 channel ID 的 `____` 前段解析。shared 入口或任一项目路径缺失时拒绝执行；preparing/failed 的工作区也不可按继承顺序回落。历史单仓库绑定的既有失效回退契约保持。

`/thread <任务>` 把原始意图与交付说明交给助手，不由命令同步建区。正文经过同一附件/引用组装点，沿聊天追加模式 steering 或成为 durable 任务。助手确定名称、项目列表和布局，多项目自行按耦合程度选择 shared/separate，用 `create-thread` 完成工作区、绑定与简短背景；只有 ready 才能 `thread-dispatch`。派发正文按 SDD 的目标、范围、约束与验收写：明确需求直接让子区生成可实施 SDD 并开工，否则发起聊天只做够交接的最少澄清，余下澄清留在子区，需要 owner 决策时用 ask，环境有 spec-design 时使用。详细澄清与实施不在父会话开展。部分创建或说明失败只报告资源状态，不派发、不回落父目录执行。命令仅在群聊中创建，DM/子区拒绝创建，空正文回复用法；派发显示与发起群回执沿用既有路径。

#### 会话分叉与共用目录子区

`/fork [子区名称]` 与 `fork({name?})` 只取当前调用聊天的精确持久绑定；工具不接受源聊天、文件或目录参数，须有原生 `toolCallId` 以持久去重。主群创建所属子区，子区创建同父群同级子区；沿用现有 owner、allowlist 与工具授权，不复制权限。

源回合可继续运行：独立受管理进程通过 `--fork <精确 sessionFile>` 读取已落盘历史，不占源 session 执行租约，不发送用户 prompt 或摘要请求。新 session 的不同 ID、有效文件、相同 canonical cwd 经核验，准备进程退出确认并收尾 ledger 后才可绑定；目标首次实际新消息用该文件 `--resume`。尚无落盘源文件或失效绑定立即失败。

`thread_forks` 持久记录来源、准备目录、新 session、远端子区、阶段与错误；复用线程创建意图保护未知远端结果。目标说明成功后，在同一事务发布 binding、默认 merge listen、ready 与意图完成。已知远端结果仅恢复同一子区的说明/绑定，不重新 fork 或创建；未知结果不猜测、不重试创建。未 ready 记录即使有 binding 也不可自动执行或回落空会话。准备过程可登记共享路径/root 使用以保护 standalone `/commit` 与 `thread-delete` 的既有保护边界；这些记录不阻挡 `/thread remove` 或 `/thread merge`，也不串行化普通同 cwd 任务。

目标不复制源 workspace 归属、显式模型、pending ask、宿主队列、卡片动作、goal 续跑授权或上下文回执；后续 worker 走目标自身的模型策略与群/线程上下文。目录共用、session 历史独立；原生读取之后的源输出不承诺进入目标。

持久标记为沿用目录的 ready fork 子区，`/thread merge`、`/thread remove [--force]` 和当前子区的 host `thread-merge` 均只排定 Octo 归档。merge 等待本聊天较早运行/排队工作，remove 忙时拒绝；不因源聊天同 cwd 而等待，不执行 Git 或清理目录/分支/session，不产生合并事件或修复任务。归档等待真实回执、正文和终态卡排空，沿用新入站取消、瞬时失败重试和停机放弃。普通 worktree/shared 的下述语义保持。

#### 隔离工作区收尾


`/thread merge` 是子区中的宿主合并入口：`/thread` 参数去首尾空白后首词为 `merge`（后为行尾或非字母数字、下划线、连字符分隔符，`merge-fix` 不算）即进入该路径。宿主先验证子区、隔离绑定、持久资源身份与 wt 能力，并按当前聊天和精确 session 顺序受理/执行；这些命令级顺序不创建跨聊天路径或仓库资源锁。单项目/separate 工作树存在 staged、unstaged 或未忽略 untracked 改动时，运行 OMP 原生 commit，复用原生进度卡、worker ledger、受管进程与 `/commit stop`/`/cancel`，但不取得独立 `/commit` 的 canonical Git root 资源租约，也不因其他聊天任务、目录/Git 活动或未确认 worker ledger 等待或拒绝；clean 路径同样不申请资源锁。仍依据持久归属核验实际路径、来源仓库、分支/checkout、绑定与合并计划。仅在本次自动提交进程组确认退出、零退出码、HEAD 改变且复核一致、工作树干净、计划未变且 stop/cancel 未先受理时，才进入既有 wt 默认暂存/提交/squash/rebase/fast-forward/删除 pipeline。失败、取消、无新 HEAD、结果/退出未知、校验失败及重启后不重放或继续合并；本次自动提交未知时不启动 wt、不称成功，但其 ledger 不封锁其他聊天的独立工作。已有未知 merge/commit/remove ledger 同样不证明进程仍存活或副作用成功，也不作为新操作的忙碌门禁。提交阶段与 merge 阶段的外部 Git 副作用都可能与普通任务、其他提交/清理或 hooks 交错，可能包含并发文件、发生冲突或留下部分效果；不承诺流水线原子回滚。直接 host `thread-merge` 不自动提交且仍沿用原接口语义；shared 固定拒绝且不创建任务，ready fork 仍只归档。持久 merge lease 记录 operation 身份/恢复事实并可 ACK，不是路径锁；同聊天的查询/控制入站可在 commit 或 merge 运行期间处理，不创建额外 RPC/agent 任务。每个命令身份使用独立进度卡记录可证实阶段、耗时与终态，不重发创建结果未知的卡片。结果经持久 outbox 送达，卡片 I/O 不阻塞 merge 结果回执；只有回执送达、相关进度卡终态确认排空和聊天 outbox 排空后才归档子区，cwd 被删除不影响这一路径。只有已实际执行且结果已知的 wt merge 非零退出才把脱敏有界诊断、工作区、原命令与 owner 尾随补充说明交给本聊天 agent 修复，再经同一 host `thread-merge` 核心重试；资格拒绝、自动提交失败/取消或结果未知不派修复。命令 claim 保留，接管输入身份稳定，重复投递与重启不重跑已知成功提交/合并或重复创建修复任务；Git 与 SQLite 间结果不明时停止自动重放。等待和执行响应取消、中断及停机。详细 SDD 和交付验收见 `2026-09-17-thread-worktrunk-design.md`。

shared 工作区先按持久资源记录识别，并直接回复「当前为多项目工作区，不支持 /thread merge。」；上述合并资格、宿主执行与 wt 探测仅适用于单项目/separate。

线程的目录继承只在它还没有已验证会话且没有显式隔离绑定时生效。线程第一次成功建立会话（`refreshState` 确认并持久化 session 文件）后，其 cwd 固定为绑定 cwd；父群随后 `/new`、`/reset`、`/move` 不改变任何既有线程绑定。已建绑定但从未成功建立会话的线程在下一个任务重新按上述顺序解析，并把解析结果写回绑定，显式 `/new <目录>` 会解除隔离绑定。服务启动时不为存量绑定做一次性回填，也不新增用户可见提示；`/status` 按绑定 cwd 照常报告。

`/move` 必须提供目标路径，按同样的安全规则解析；它只在当前绑定存在精确可恢复 session 文件时执行。channel 先取得并持有当前聊天与绑定会话的 lease，调用 OMP 原生 `/move <目录>`，再通过 `refreshState` 校验 session ID、session 文件和规范化目标 cwd，成功持久化后才更新绑定。其他聊天的独立会话可同时使用源目录或目标目录；当前聊天有运行/排队任务、同一会话 lease 冲突，或源/目标目录存在未确认停止的 worker 时拒绝，不隐式迁移或丢弃任务。

显式目录必须已经存在、为目录且可访问。使用 realpath 规范化并记录；多个聊天可绑定同一真实目录，使用独立 session 并行执行，文件修改的协调由任务自行负责。创建 fresh worker 时传入 `--cwd`，不靠会话内 cd 模拟目录切换；创建或持久化失败保留原会话绑定。`/new`、`/reset`、`/move` 成功后才确认成功。

当前聊天有运行中或排队任务时 `/new`、`/reset`、`/move` 明确拒绝，并提示先 `/cancel`；没有会话时第一条合法普通文本按同一默认目录解析顺序创建会话，再处理消息。历史 OMP 会话文件保留，本次不提供 IM 历史列表或切回命令。

原生迁移的 worker 若无法确认停止，源目录和目标目录都持久标记为 blocked；服务重启恢复 worker ledger 时仍同时阻塞两目录，直到停止状态可确认。外部移动不通过最近文件猜测，而在后文定义的精确身份规则下自动重绑。

### 群说明、Host tools 与问答

新 session 的首次模型输入在完成权限、去重和 durable acceptance 后读取当前 Octo 共享说明；channelType=2 从 `groups/{id}/md` 读取 `GROUP.md`，channelType=5 从 `groups/{parent}/threads/{short}/md` 读取独立 `THREAD.md`。读取失败不启动模型请求；空说明是有效快照，不冒充读取失败或清空记忆。快照保留服务端 `present`、完整 `content` 和 version，用于持久状态和写前校验；这些动态元数据不进入固定 system 指引。

共享说明按 `(botUid, channelType, channelId, exact OMP sessionId)` 隔离。`refreshState` 确认真实 session 后，宿主发布受控 sidecar 与 bound-session 指针；占位 session ID、cwd、父群或兄弟区不能作为说明来源。worker 内非绑定的 task/eval session 不自动得到聊天身份、成员或说明。sidecar 与刷新请求均不包含 Octo token。

普通首次说明通过 OMP `before_agent_start` 返回的持久 `CustomMessagePayload` 进入原生会话历史；无 prompt ticket 的原生 goal 或跳过该 hook 的续接，通过首次 `context` 边界单飞确认持久消息与 prepared receipt，固定恢复其位置，不重放原始输入。普通回合保留已加载说明，不因远端 version 变化重新读取或改写 system。`/move`、原生 `/retry` 和服务重启续接仍是同一 session；`/new` 和 fork 新 target 按各自精确目标加载说明。自动说明携带结构化 bot/target/session 身份；fork 的请求投影保留显式业务历史，排除来源聊天的自动说明和成员后缀，原生历史文件保持不变。

继续复用说明 ledger、sidecar 和 prepared receipt。prepared 证明精确 scope/session 的指定内容已准备，不代表模型已接受；宿主按既有 acceptance 与 CAS 条件结算。已确认的原生会话消息负责恢复历史，不通过每次模型请求尾部临时 developer 补灌说明。失败、中断和取消保留已确认上下文及未结算状态，不把丢失 receipt 或读取失败伪装为成功。

compact 是明确刷新边界：extension 在 `session_before_compact` 使用该事件的 AbortSignal 发起宿主 fresh read，成功后 `session.compacting` 把最新完整说明交给压缩输入及原生 compaction `preserveData`。只有已提交的 compaction entry 才能发布新快照，说明在摘要后固定位置重放；自定义 hook 跳过 `session.compacting` 时，sidecar 暂存带 branch leaf 的 fresh 候选，确认该 leaf 之后有已提交 compaction entry 才提升为恢复 receipt。文件桥接校验 bot、聊天、exact session、turn 和 request 身份，有界且可取消，由宿主持有 transport 和停机生命周期；不用 headless UI，不开网络 listener，不暴露 token。读取失败显式取消压缩，不以旧 sidecar 冒充 fresh read；失败或未提交均保留已确认记忆。DM 无需启动共享说明刷新 watcher。

`description-read` 每次实时读取，结果自然追加到当前对话；`description-write` 只负责持久写入，不主动改变会话说明。写前读取凭据、版本与正文核对、单次消耗和本聊天隔离保持不变。详细 SDD 与验收见 `2026-11-04-group-thread-current-facts-design.md`。群历史仍为用户消息的独立增量上下文，slash 首行、水位与授权规则不变。

#### 每轮成员与操作上下文

成员快照的获取由 channel 服务负责，使用既有 transport API，不通过 agent 工具或 CLI 获取：群聊调用 `GET /v1/bot/groups/{group}/members`，CommunityTopic 调用 `GET /v1/bot/groups/{parent}/threads/{short}/members`。线程快照只描述当前线程，并标明父群 ID；私聊不读取群或线程成员。快照包含明确的聊天目标、API 返回的总人数，以及小名单的名称和 UID：人数不超过 10 时列出全部 `{name, uid}`，超过 10 时只给人数和当前 `octo_management` 成员查询动作，需点名时由助手查询具体成员。人数为 API 返回列表的数量，不能把工具展示截断后的条数当作总人数；空列表是有效的 0 人结果。名称作为 JSON 字符串数据编码，UID 按 API 原值使用，不从名称推导，也不套用某一种 UID 长度或字符集。

快照跟随 channel 的新输入刷新：已授权、去重后的新任务在开始执行时请求一次，排队期间不预取；steering 输入在并入前请求一次，在下一次模型调用前发布新快照。工具调用和同一回合的后续模型请求复用该快照，不发起周期刷新或跨轮缓存。重启后尚未执行的排队任务仍按新任务获取；OMP 原生 `/retry` 及通过原生重试续跑的在途任务沿用 OMP 会话上下文，不请求成员 API，也不新增成员块。服务的控制命令、单纯记录 ask 答案的路径及被拒绝、重复的输入不触发成员获取；ask 答案若另行唤醒新任务或成为 steering，则在该新输入的交付边界获取。

同一聊天的成员获取、当前 session 复核、快照准备与 steering 交付按已受理输入顺序串行，控制命令不进入等待链。快照只有在对应新输入被 OMP 消费时才进入历史；未消费、拒绝或失败的 steering 不能提前改变运行中模型上下文。同文本的多条输入仍按输入顺序区分。原回合已退出时交回普通任务路径，在真正执行时获取自身快照。

成员获取是本次新输入的前置条件：失败、超时或响应格式不符时不使用旧名单、不编造人数，新任务按启动失败结算，发送「成员信息读取失败，未启动任务。」；steering 刷新失败则不把该输入并入或悄悄变成排队任务，发送「成员信息读取失败，本条消息未并入当前回合。」且已有任务继续。错误细节沿既有有界日志/任务失败通道记录，不向聊天泄漏原始响应。steering 期间原回合已结束或不再接收输入时，沿既有普通任务路径交付，在真正执行时重新获取。成员请求遵守既有网络截止和停机取消；不阻塞控制命令，不新增自动重试或配置项。

worker 已创建后的任务执行异常，向任务所在聊天发送失败提示及捕获的原始异常消息（`Error.message`，非 Error 值使用字符串形式），保留换行和完整内容；长消息按既有 outbox 分段投递。群聊和线程中的成员可见其中可能包含的路径、请求内容或凭据。启动阶段的分类提示与成员 API 错误处理保持各自的合同。

成员快照是对应普通用户输入的一部分，在原生会话中持久保存；slash 命令完整原文不变。首次新输入附加一次，后续只在语义变化时追加。比较按 UID 和名称规范排序，API 顺序变化不算变动；小名单保留名称和 UID，大名单保留人数、查询入口及规范化名单指纹，人数相同的换人或改名也能触发新快照。已进入同一 exact session 历史的快照不移动、不替换，工具续调、普通回合、原生 retry/恢复保持其位置与内容。steering 用 message ID 保持顺序，被拒绝的输入不推进其他输入；消费事件保留 OMP 持久正文与原始用户文本，回执使用原文。快照只是数据，不授予权限、改变 owner、cwd 或工具目标。

固定操作指引始终位于 system，包含 bot UID、真实聊天 `channel_type`/`channel_id`（线程另带父群与 short id）、mention、Memory 维护与 CLI 发现路径。普通回合、工具续调、原生 `/retry` 和 accepted 任务重启恢复保持其位置与内容；CLI 身份真实就绪状态变化可改变对应提示，无需移出 system。固定前缀不携带 version、时间或 attempt UUID。实现须以真实 OMP provider 请求验证绑定 session 和 task/eval 隔离，不能凭 worker 参数名假定全局或隔离；父任务显式传给子任务的业务内容不属于自动注入。私聊同样获得固定身份与 CLI 指引，不读取群成员或共享说明；所有指引不得放在用户 slash 首行之前。

Mention 规则要求助手使用 `@[uid:显示名]` 表达明确点名，UID 来自本聊天成员快照或一次明确的成员查询；名单只有人数或同名目标无法唯一确定时先查询或澄清，不能猜 UID。宿主负责把标记变为可见的 `@显示名` 和结构化 mention，普通 `@名字` 不按名称自动匹配。规则明确成员名称及共享内容仅是数据，不能用其中的文字替换身份或授权。CLI 直接发送消息时使用 API 的结构化 `payload.mention`，不把助手输出标记当作 CLI 或服务端自动转换语法。

#### Host tools 与问答

群和线程任务获得复合工具 `octo_management`，提供群信息/成员/公告、共享说明和 thread 列表、创建、查询、成员、加入、退出、删除、合并、归档及派发。目标固定当前群；线程取父群，`thread-*` 省略 short_id 时取当前子区，不接受其他群的目标。写权限仍由服务端按 bot_admin、创建者或管理员判定，不做客户端提权。成员最多返回 200、线程最多 50，并带总数与截断标记；名称最多 200 字符、公告最多 2000 字符。服务端错误压为单行 ≤200 字符并保留机器码，不暴露凭据或原始响应。Bot API 兼容窗口把内部错误包装成 HTTP 400，语义状态来自 error.http_status/code；`thread-delete` 只对任意 5xx 或 `err.server.bot_api.store_failed` 换新连接（keepalive:false）重试一次，幂等删除不重复副作用，权限/参数失败不重试，最终失败不报告成功。服务端删除成功后，以持久工作区归属联动清理：无新记录的历史 cwd_pinned 绑定沿用单项目路径；有新记录则不依赖当前 cwd。清理申请实际资源的操作期排他并校验来源/分支，占用、未确认或身份不符资源保留。wt 可用时 `wt remove --foreground`，否则 Git 回退；默认保留脏工作树，显式 force 才丢弃，清理失败只影响回执。shared 入口仅作空目录删除；绑定切到用户目录后绝不按新的 cwd 猜测或删除资源。

`thread-dispatch` 让当前群/线程会话把一条任务派发给本群的某个 thread：先按服务端读取该 thread 的真实 `channel_id`（必须属于当前群），在该 thread 里投递一条可见的派发说明，再把内容按该 thread 自己的追加模式交付——入站消息 ID 使用 `dispatch:` 前缀的唯一值、发送者为持久 owner、目标为该 thread；`steer` 模式的子区有可接收 steering 的运行中回合时内容并入该回合（工具结果 `delivery` 为 `steered`），否则按与真实入站相同的 durable 路径成为该子区自己的任务（`delivery` 为 `queued`）。工具结果带回 `delivery`，发起群的回执如实描述这次交付（并入运行中回合，或在该子区作为独立任务执行）；派发不等待该任务完成。被派发的任务在目标 thread 的绑定与 session 中执行，cwd 按线程默认目录规则解析（未建会话时继承父群绑定 cwd，各自独立会话可并行执行），结果投递到该 thread 而不是发起聊天。派发是显式交接而不是 owner 的回复，因此从不作为该 thread 待回答问题的答案；被派发任务自身仍照常排队、在自己回合里接收 steering 与问答。派发的深度固定为 1：任务记录的消息 ID 带 `dispatch:` 前缀时，该任务的 `octo_management` 不提供 `thread-dispatch` 动作，客户端也拒绝该动作。

`thread-send-parent` 是子区显式向创建方提交业务输入的动作：只接受当前子区和有界正文，不允许指定目标；目标取持久创建方关系，遵守当前授权，与自动 `listen` 档位独立。宿主标注真实来源及非用户消息、非 ask 答案、非新增授权的语义，以稳定工具调用身份持久接收为创建方独立排队任务；即使创建方处于 steer 模式也不并入当前回合。回执中的 queued 只表示接收，不表示已处理；重复调用不重复执行，结果不明不盲目重投。详细边界见 `2026-11-05-thread-creator-input-design.md`。

`create-thread` 支持 `projects:[{name,repo,base?}]`、`layout`、`background` 与既有 `worktree_name`。单项目可省略 layout；多项目必须显式选择 `shared` 或 `separate`。shared 创建一个 thread，以状态目录下非仓库入口为 cwd，入口内每个项目各有其 repo 派生的隔离 worktree；separate 按每个项目的 repo 分别创建 thread/worktree，返回各自 short id、实际路径、分支与状态。`background` 仅说明创建目的与涉及项目，宿主写入实际成功路径和必要关联子区，不承载任务、SDD 或验收；具体工作严格来自后续派发。既有 `worktree_name` 单仓库路径与命名能力保留，无显式项目时从调用方 cwd 解析源仓库；名称必须含 ASCII 字母或数字。wt 优先、不可用时回退真实 `omp worktree add`，wt 执行失败不静默切换。SQLite 独立保存资源归属和 preparing/ready/failed 状态，每步记录计划与确认结果；整次调用全部工作区、绑定和 THREAD.md 成功才 ready。部分失败返回已完成/失败/未确认资源，保留现场，不自动派发；重启后 preparing/failed 仍不可执行，也不能回落到父目录。普通入站与 thread-dispatch 共用门禁。显式 `/new`、`/move` 只改变当前绑定，不遗失旧资源；shared 缺失项目或入口时停止执行。详细接口、资源租约、失败与验收 SDD 统一见 `2026-09-17-thread-worktrunk-design.md`。

新建隔离资源的任务名由 agent 通过 `worktree_name` 提供，显式 projects 时必填。ASCII 小写、单连字符规范化后任务与项目 slug 均限 1–100 字符，不截断单词；超长要求 agent 缩短完整语义。shared 使用 `<stateDir>/thread-workspaces/<唯一任务目录>/<主群作用域名称>` 作为入口，入口内项目目录独立使用项目 slug，各不同仓库分支使用同一完整任务 slug；single/separate 保持 `<repo>.<任务 slug>` 布局。主群作用域取持久绑定 cwd 对应的 Git 主检出根目录名或非 Git cwd 目录名，按 OMP 规则转小写；从子区发起仍读取主群绑定。在有效 Hindsight 配置一致时，相同作用域复用 bank 及项目标签；配置由用户管理。实际任务路径或分支重名才追加 `-2`、`-3`，项目显示名或规范化冲突则创建前要求明确限定名。任务目录与入口均持久记录归属，清理只按记录非递归移除空目录；已有资源和归属不改名。详细边界及验收见既有 Worktrunk SDD。


子区通过 `create-thread listen=off|merge|task` 设置回传策略，省略为 `merge`；`thread-listen` 查询或修改，`thread-info` 展示当前档位。目标持久化为创建方聊天，已有子区升级为 merge，旧在途派发遵循当前策略。merge 仅回传宿主实际合并成功；task 额外回传独立派发任务的 completed/failed/cancelled/interrupted；off 不自动回传。普通停机挂起不回传，恢复后的真实终态按当前策略决定。steering 派发没有独立终态。合并意图、尝试及后续修复持久抑制任务通知，任意档位都不得通过任务失败旁路回传合并失败。shared 不支持合并，默认静默，需要派发结果时显式 task。合成通知沿既有权限、去重和追加路径交付，messageSeq=0，不成为待答问题答案。任务通知仅带来源、任务 ID、状态与必要固定说明；合并通知仅带来源、目标分支、实际提交、工作区清理和归档排定状态，不附原始任务或 agent 输出。详见 `2026-09-17-thread-completion-notify-design.md`。

回传输入明确标注其来自子区，是系统结果通知而非用户新消息、待答问题答案或新增授权。宿主提供可追溯来源与必要终态摘要，并引导助手结合当前任务和待确认事项自行判断相关性：相关时说明结果补充或改变了什么，不把子区结论视为用户确认；不相关时明确是另一项任务，不混入当前任务结论。是否回复、整合结果或继续处理由助手决定，不因收到回传自动切换工作；既有权限、追加模式和去重边界保持不变。

`thread-merge` 是 `octo_management` 中执行工作区合并的动作：只接受当前子区，且要求该子区绑定隔离工作区、部署提供隔离能力、本机存在可用 wt；宿主以工作区为 cwd 运行 `wt merge --format json`（600 秒上限、随工具取消信号中止、不传 `-y`），执行前校验工作区目录存在且是 linked worktree（`rev-parse --git-dir` 与 `--git-common-dir` 不同，主检出因此不可合并）。可选参数逐项映射 wt：`target`（默认仓库默认分支）、`squash`/`commit`/`rebase`/`remove`/`ff`（默认 true，false 映射 `--no-*`）、`stage`（`all`/`tracked`/`none`，默认 `all`），全部省略即 wt 默认 pipeline（squash、rebase、fast-forward、合并后删除工作区与已合并分支）。成功返回 wt 的结果对象（`branch`/`target`/`squashed`/`rebased`/`committed`/`removed`）；失败返回单行、≤200 字符原因——rebase 冲突时工作区留在 rebase 现场、目标分支未改动，助手处置后可重试。

shared 多项目工作区的 `/thread merge` 固定回复「当前为多项目工作区，不支持 /thread merge。」并终止命令处理；不创建合并任务、不调用 wt。直接 `thread-merge` 也按持久归属拒绝，不能绕过此边界。separate 与单项目保留上述单仓库流程。

`octo_management` 的 `description-read` 每次通过实时 `getGroupContext` 读取服务端当前说明，不使用会话或 compaction 快照；工具结果自然追加至对话。`description-write` 只更新当前群/线程目标且需要服务端 `bot_admin` 权限，保留本任务先读、写前重读核对、读取凭据单次消耗等合同；写入不刷新普通会话，下次 compact 或新 session 才自动加载。私聊不读取或注入群说明，也不获得群说明动作。

群和线程还提供独立 `fork` 宿主工具，参数只有可选 `name`，与 `/fork` 使用同一持久流程；不通过 `octo_management create-thread` 创建空会话来替代分叉。

所有任务都获得固定目标与 cwd 的 `ask`、`octo_send_file` 工具，以及控制 channel 自身的 `channel_control`；群和线程额外获得复合工具 `octo_management`。`channel_control` 的 `restart` 排定一次 channel 重启：它在本回合结束后、且 channel 没有运行中或排队任务时才让检测出的守护进程重启自己，重启后按持久任务状态恢复；因此在自己的回合里改完代码即可让新实现生效（自迭代），既不打断正在投递的回复，也不需要外部脚本或延迟作业。未检测到守护进程时明确回复未检测到且不猜测重启机制。`status` 返回进程 PID、运行中的会话目标、排队任务数、被阻塞目录数与是否已排定重启，供重启前后核对。文件工具的相对路径按任务 cwd 解析，绝对路径可指向本地其他文件，系统不提供目录沙箱保证；发送前复制到 stateDir spool；outbox 保存稳定 client message ID，断线重试不重跑任务。文件永久失败标记 dead 并报告，但不得阻塞后续文字 outbox。

所有已授权任务还获得当前聊天限定的 `watch` 宿主工具，提供 create/list/get/stop。create 仅用于用户显式要求的语义条件观察；源 wake 不能自动创建后继订阅。采集脚本沿用可信本机执行边界，不构成只读沙箱；宿主不保持 OMP worker 等待，命中不作为问题答案或当前回合 steering。启用 `decisions` 默认同时启用 watch，并复用当前连接、模型和密钥；`watch` 配置只保留可选的 enabled 开关。实现与验收按 watch SDD 维护。

`ask` 创建带持久化 ID 的问题后暂停当前任务，并给出一段包含问题正文、编号选项（如有）和回复规则的文本。只要该聊天存在待回答的问题，owner 或已开通群/子区内的成员在该聊天的下一条非命令文本即被当作答案：只有一个待答问题时它就是该问题的答案——合法选项编号按该问题单选/多选的数量和范围校验，其他文本原样作为自填答案返回给模型；一次 ask 的多个问题同时待答时，这条文本作为整组答案，同一文本写入该组每个仍未作答的问题（逐题按各自规则解释：编号在范围内取编号、否则原样），该 ask 立即在所有问题上返回而不逐题等待，已作答的问题不重复写入。命令仍然优先。不提供默认答案；无待回答问题、不同聊天与重复投递的回复都不作为答案，其中无待回答问题时的文本按普通任务输入处理。等待超时不再是过期：等待超过配置的 `ask.timeoutMs`（默认 6 小时，允许 30 分钟到 7 天）时，问题行保持 pending、卡片保持可提交，等待中的回合像被取消的 tool call 一样停住（任务记为 interrupted，不向聊天发送任何提示）；owner 或成员之后的答复照常记录为答案，并作为该会话的新回合输入投递（正文写明所答问题与还原后的答案）——运行中的回合按该聊天的追加模式并入，否则成为 durable 任务——agent 在同一 session 继续。同一次 ask 的整组问题共用创建时开始的计时，整组一起停住。显式取消封闭该聊天的问题；停机保留问题行与卡片。问题关闭后收到的卡片提交按关闭原因分流：只有「不是用户明确停止、也不属于 `/new`/`/reset` 切换前的旧会话」的关闭才按迟到答复接受——先按行 compare-and-set 记为 answered（含提交人，同一提交只落一次，事件重投与重启重放因此静默），再把「原问题＋还原后的答案＋提交人」作为该聊天的新输入投递（运行中的回合按追加模式并入，否则成为 durable 任务，恰好一次唤醒）；它不恢复原任务、不重放原 prompt，也不作为该聊天另一张待答问题的答案。关闭原因是用户明确停止（`/cancel`、`/interrupt`，含重启恢复里结算这两种意图的行）或该问题提出于最后一次 `/new`/`/reset` 之前时，提交照旧只回一次「该问题已失效，本次点击未作为答案。」提示、不落答案；已记录答案的问题再收到的提交保持静默，既不覆盖答案也不投递。关闭原因随行持久化（`questions.closed_reason`：`cancelled`/`interrupted`/`ended`；迁移前的旧行与 `expired` 旧值按 `ended` 处理）。每轮 ask 持久化原生 `toolCallId`、调用组身份与 `settled_at`：恢复后同一调用重放返回原答案及 `answeredBy`；未交付的一组可由续跑中对应 ask 整组认领，包括全部已答复但结果尚未交付的情形；新调用不复用已交付历史。恢复窗口内的答复由认领方消费，不另建唤醒任务；任务收束时关闭未认领的待答行，已答复但未认领的内容补投一次。问答卡每次编辑前在 SQLite 原子预留 `card_seq`，跨重启递增；旧行未知序号仅首次使用保守高水位初始化。

恢复期的问题快照在开放入站与 Bot 事件消费之前建立，因此 worker 尚未创建时到达的答案也留给原调用消费，不另起任务。认领先匹配原生 toolCallId，只有没有精确身份时才尝试未落定组的整组语义匹配；同身份参数不符明确失败。语义接管把新调用身份原子写回每行后才认领，正常答案交付前落定；持久化失败不冒充成功交付。同一调用可以重复取回其结果，但这些答案不会再唤醒第二个回合，也不会被新调用复用。取消清除该聊天的全部恢复快照，包括已答复但尚未重接的组。

达到 `ask.timeoutMs` 后优先回收 worker：先发有界 `abort`，无论确认成功或失败，都进入既有有界进程组停止流程，不以收到 `agent_end` 为释放前提。停止期间到达的答案持久排队，不再 steering 到正在退出的 worker；确认退出后才释放聊天/session 租约并在原 session 继续。停止导致的 prompt 拒绝记为 interrupted，不发送执行失败通知；未确认进程退出仍阻塞目录，不能并发恢复同一会话。最大等待配置与问题的 pending 状态不因此缩短或失效。

整组问题初始化中途失败时，已建立的问题取消、等待者与超时计时器释放，原始落库错误继续返回调用方；后续普通消息仍走正常输入路径，修复存储故障后的下一次提问可正常完成。

部署宣布交互档时（`GET /v1/bot/card/profile` 的 `profiles` 含 `octo/v2`、`config.card_enabled`/`display_enabled`/`interaction_enabled` 为真），问题改以 type-17 交互卡片投递，并把卡片的远端 message ID 记在承载该问题的行上；信封的 `plain` 与同一问题的纯文本渲染逐字一致，所以不认该 profile 的客户端照常显示问题文本。

问答卡以问题、选项和作答控件的阅读顺序组织。问题标题使用 Medium 字号与 Bolder 字重，正文保持默认字号。单题与多题的选项统一使用展开、可换行的 `Input.ChoiceSet`，每个可点击选项标签包含完整编号、加粗标题及换行说明，由客户端呈现左侧选择控件与独立选项块。单选仅允许选择一项，多选允许选择多项；选择只改变输入状态，点击底部提交后才记录答案。单题卡保留一个「提交」动作，同时处理选择与「其他」输入。无选项题使用多行 `Input.Text`，保留粘贴的换行。选项题的「其他」输入保持单行，填写后以它为准；空提交或单选提交多个编号均不成答。已发出的旧单选按钮卡仍按其原 action id 解析，避免让待回答问题失效。

一次 ask 的多个问题共用一张卡：顶部显示待回答数与总数；每题标题携带题序，题目之间以细分隔线区分；每题有独立输入 id（`ask_choices_<n>`/`ask_text_<n>`，n 为题序）。选项题使用展开的单选或多选列表，完整标题与说明同处选择行，并附「其他」自由输入；无选项题使用多行输入。整卡只有一个「提交已填答案」。一次提交把填写了的问题各做一次答案 compare-and-set（同一存储事务），未填的问题保持待答、卡片换新的 action id 生成重绘后继续可答；全部作答后卡片改写成终态，每题显示已回复与所选或自填内容。已回复行紧随题目，以 Good 语义色显示答案和署名；完整选项与说明仍保留在其后，作为较弱层级的回看内容。整卡发不出时——未宣布交互档、信封超出服务端 limits（node/depth/payload）、或发送失败——回退为逐题投递：每题按上面的单题卡或纯文本单独提问并逐个等待，问题语义不变。

点击与文本回复共用同一个答案 compare-and-set：只有一方生效；owner 的点击在任何聊天有效，成员的点击仅在问题所在聊天是已开通群/子区（允许列表命中，线程继承父群）时有效，其余点击不生效（ack 但不落答案、不重绘、不唤醒）；问题关闭后的点击不覆盖已记录的答案，按关闭原因分流的规则见上（生命周期关闭按迟到答复投递，用户明确停止与切换前的旧问题只回一次失效提示，已答问题的新提交静默），而非本卡发放形状的 action id（含伪造的 `cfg:` 前缀）一律静默、连提示都不回；服务端对同一 `(message_id, action_id, operator_uid)` 只受理一次点击，因此未成答的点击（例如空内容提交）用新的 action id 重绘卡片并回复一次提示，用户仍可继续作答。

每次答案落盘同时记录提交人 uid（问题行 `answered_by`）：`ask` 工具返回值的每条答案带 `answeredBy`（身份始终是 uid），而用户看到的署名把该 uid 解析成显示名——`GET /v1/bot/user/info` 按 uid 取名，结果进 30 分钟 TTL、最多 200 条、单次查询 3 秒预算的有界缓存（同一 uid 的在途查询合并，失败不缓存），查不到、超时或失败一律回退成 uid，且从不影响答案落盘与卡片改写；卡片已回复行把名字渲染成自带 `weight: Bolder` 的 `TextRun`（真机实测 2026-09-19：加粗成立，Markdown 文本在卡片内不承担加粗），同一句的 `plain` 文本回退走消息正文，那里没有样式可用，名字用 Markdown 加粗（`已由 **名字** 回复：…`，真机实测正文把两个星号吃掉并显示为加粗）；两处句子同源，只有强调手段不同；等待超时后的唤醒文本仍按 uid 标注提交人；需要名字时 agent 也可用 `octo_management` 的 `group-members` 查询。答案落盘后卡片被 best-effort 改写成终态（已回复并带上所选项、已取消），改写失败只记日志，不影响答案。

交互卡片的上行由 channel 主动消费 Bot 事件队列：`POST /v1/bot/events` 长轮询读取（`wait` 上限低于客户端请求超时并留余量），游标按 `botUid` 持久化且单调递增，事件处理完成后才 `POST /v1/bot/events/{event_id}/ack`，ack 失败或处理可重试时保留游标让同一事件下轮重读。投递是至少一次：重复投递的点击按已记录答案处理，不再作答也不重复改写。该队列只有本 channel 一个消费者；非 `card_action` 的事件（含无 `event_type` 的旧式消息事件）ack 后略过，不参与聊天路由。部署没有该端点时事件轮询自行停止，问答退回纯文本路径。

事件 ACK 暂时失败时保留当前游标，按轮询失败相同的 5 秒退避继续读取和确认；不终止消费者，也不越过尚未确认的事件处理同批后续事件。事件处理返回可重试结果时同样间隔 5 秒再尝试，连续 5 次仍无法处理才确认并丢弃该事件，避免即时重投在故障恢复前耗尽预算。
卡片已发布但发送回执尚在途时，同聊天的有效 ask 点击保留为可重试事件，待远端 message ID 绑定到问题后再作答与 ACK；单题与整组卡都遵守该顺序。已结束发送且没有对应绑定的未知卡片仍按既有规则忽略，不能猜测绑定到另一道题。
回执到达并绑定后重新读取问题的持久状态：在发送途中已经通过文字作答或关闭的问题，单题卡与整组卡都补写当前状态；卡片展示与已记录答案一致，发送完成不会重新打开已结束的问题。
整组卡的成员由远端 message ID 绑定的全部问题决定，而非当前待答问题数量。部分作答后，剩余问题被文字回答或取消时仍重绘整张卡，保留先前答案与署名，并关闭剩余输入。
单题卡发送失败或被部署拒绝时，纯文本回退只投递仍待答的问题；发送途中已记录答案或关闭的问题直接沿用持久状态。

除 host tool 外，任务里的 Agent 还可以用本机 `octo-cli` 以本 bot 身份直接访问 Octo Bot API：宿主注册 Bot 后把该 bot 的 token 经 stdin 写入自己的 octo-cli 凭据目录（`~/.oco/octo-cli/<botUid>`，目录权限 0700，命令行与日志中不出现 token），并把 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 注入 OMP worker 环境。一个 bot 一份目录，宿主服务多个 bot 时凭据互不共享。原始 token 不进入 worker 环境：OMP worker 继续按前缀剥离宿主继承的 `OCTO_*` 与 `OMP_CHANNEL_OCTO_*`，只有这三个名字允许进入子进程。凭据准备是 best-effort，octo-cli 缺失、登录失败或返回的 robot_id 与当前 bot 不一致时只记录一条有界日志并保持不注入，不影响启动与任务。因此 host tool "目标固定为当前聊天" 的承诺只约束 host tool 自身；经 octo-cli 的调用不受该绑定限制，其授权完全由服务端按本 bot 身份判定。Agent 的能力发现及身份核对要求由下文固定操作指引统一提供。

#### Octo CLI 能力入口

每轮固定操作指引把 `octo-cli` 描述为当前 bot 访问 Octo 业务的入口，而不只描述为 host tool 的 API 补充。摘要覆盖 Loop（任务、派发、专家/专家团、工作区与执行进度）、Docs（文档、表格、白板、PPT、评论与版本）、Drive（空间、目录、上传下载、分享与挂载）、HTML（交互 HTML 文档、发布与版本）、Messaging/Files（消息、群、线程、历史搜索与附件）、Mail（邮件）及 Marketplace（插件、技能与专家资源）。摘要不承诺当前 bot 具有所有资源的访问或写权限，也不把安装 CLI 等同于凭据配置成功。

指引提供固定的发现顺序：先以 `octo-cli auth status` 确认当前身份与宿主配置的 bot 一致；用 `octo-cli skills` 发现本机指南，`octo-cli skills octo-shared` 和 `octo-cli skills <相关技能名>` 按需读取，参数不明确时查 `octo-cli schema <操作>` 或 `<命令> --help`。不在每轮内联各域完整手册，不要求每轮执行能力探测命令；业务摘要与实际受支持命令保持同步，具体操作以当前安装版本的指南和 schema 为准。

宿主提供的 CLI 身份就绪状态必须真实：凭据配置成功时沿用上述三项环境变量；CLI 缺失或凭据准备失败时说明本轮没有已就绪的 CLI 身份，继续使用可用 host tools，不能宣称已经配置成功。Agent 不切换其他 profile、凭据或 API 路由绕过权限失败；CLI 自身的授权及操作限制由相应指南与服务端执行。`channel_control` 保持自身 restart/status 职责，CLI 入口说明由固定操作指引统一拥有，避免两处独立维护同一份业务提示。此项不新增 host tools、凭据类型或业务权限。

### RPC 与输出

worker ready 后协商协议版本；本地实现保持 OMP v18.1.18 兼容的 RPC 解码，按请求 ID 匹配响应并正确处理 v2 分块；宿主独立管理 worker 生命周期。`agent_end.willContinue=true` 表示仍有后续生命周期或消息，必须继续等待，不提前收尾正文或 typing；prompt 接受不代表任务完成，以生命周期事件和异步错误判断结果；本地完成但未调用模型的 prompt 也必须结束状态。

按需启动 worker；同一持久会话只能被一个 worker 写入。新会话只通过 fresh RPC `new_session` 创建并立即落盘；对已有绑定，恢复前先按 session ID 与 `previousSessionFiles` breadcrumb 在 native roots 中寻找并验证唯一外移候选，找到后持久化重绑；扫描或验证失败则保留原绑定并报告失败，不能用“最近会话”替代精确身份。恢复 worker 时仅传入已经确认存在且准确的 session 文件和保存的目录。正常退出释放 worker。
OMP extension 在 worker 启动时由固定路径自动加载；sidecar 根路径通过宿主显式 allowlist 的非秘密 worker 输入传给 extension，sidecar 在真实 session ID 确认后、第一次 `prompt` 前原子发布，extension 在生命周期回调中按该 ID 精确读取并写入 `prepared` receipt。绑定 session 指针在该 turn 的第一次 `prompt` 前发布，并作为该作用域的当前 session 保留；worker 进程内由 OMP 打开的 subagent 或 eval 子会话因此不会被当作 channel turn，extension 不按 cwd、全局路径或模糊 session 猜测身份。宿主必须能观察 matching receipt 和 prompt acceptance，并以 attempt ID 做单调 CAS；worker 停止、重启、恢复和 compaction 不得绕过 `pending`/`committed` 判定。宿主不向 extension 暴露 Octo 认证凭证，extension 不得按 cwd、全局路径或模糊 session 查找 sidecar。

#### 会话压缩

手动和自动压缩的生命周期不产生聊天通知；任务的普通输出、错误及取消处理保持原有行为。压缩算法、模型选择、`/compact` 首行分派以及共享说明合同不变。

OMP 的 `/compact` 本地命令可能先返回 ACK，再在后台完成压缩。worker 持有当前任务直至匹配的原生手动压缩结束，或已核实的无 start 终态命令输出、取消或停止；不透传可能包含 provider 内容的命令输出。自动压缩生命周期不改变任务的收尾方式。session 上下文刷新仍须以已提交的原生 compaction entry 为依据。

验收使用隔离 RPC 子进程覆盖自动方法切换和独立轮次、手动多种模式、无 start、取消及停止，并验证均无压缩通知；离线真实 OMP 配合有限 localhost 模型和本地 Octo 夹具验证手动和自动压缩提交、worker 生命周期及正常输出。

`WorkerEvent` 的正文事件为稳定的本地 `messageId`、累计正文快照和 `final` 标记；上游没有原生 ID 时由 adapter 生成。每个 assistant message 独立维护 stream，可流式投递的正文首次非空时立即创建正文消息，后续累计快照只编辑同一个远端 message ID；结构化 mention 也在同一流中增量更新，不同 assistant message 不合并。`final=true` 是该消息的权威结束快照，允许比此前更短或为空；已有远端消息必须编辑到最终空文本，尚未创建且从未有可投递正文的空最终事件不发送空白新消息。

正文增量可先于 `message_start` 到达；迟到或重复的开始事件复用当前尚未结束、上游身份兼容的 stream，保留累计正文和本地 message ID。消息结束后或出现明确不同的上游身份时才分配下一条消息 ID。续接任务为新 worker 的正文与通知使用独立输出段；从头计数的 `assistant-1` 不覆盖前一进程已经发出的消息，旧 outbox 仍沿持久 ID 排空。

正文编辑以最新累计快照为准，首次创建不等待整轮结束；编辑最多每 1000ms 一次并按目标串行，最终快照优先完成，不得由旧快照覆盖新快照。正文按 3000 UTF-8 bytes 上限分段，每段在自己的窗口（该段起点起 3000 bytes）内取层级最高、同层最靠后的自然断点：段落/换行 > 句末标点（。！？；…! ? .）> 逗号/顿号（，、,;）> 空白；窗口内没有任何断点（如无分隔的长串、URL、代码）才在该窗口末尾硬切，ASCII 标点只在后面是空白或文本结尾时才算断点，避免小数、缩写与 URL 里的点被误判。断点字符归上一段，其后的空白留在下一段开头，不修剪也不吞并。每段的边界只由该段自己的窗口决定，因此已由满窗口定界的段在流式增量下逐字稳定、不重排，规模变化只发生在仍在累计的最后一段及其新增段；保留原始换行、空格和 inline 标记；分段 stream 使用稳定的 client message ID，分段数量变化时清理 stale tail。每次网络操作前持久化该分段的目标快照及 `desiredVersion`/`ackedVersion`；首次 create 在网络请求前 durable 标为 `unknown`，送达未知时不重跑 Agent，重连/重启沿用稳定 ID 绑定已创建消息并将最新 desired 快照补做 edit。

#### 普通回复中的 mention

本节转换仅针对助手的普通正文流；命令回执、入站引用、工具结果、进度卡及其他宿主生成文本不作为 mention 指令解析。群与线程中的 `@[uid:显示名]` 生成一个明确点名，显示名非空、单行，UID 使用成员 API 提供的真实值。以第一个未转义冒号分隔 UID 与显示名，显示名中的 `\`、`]` 分别以 `\\`、`\]` 表达；支持同一消息中的多个人和重复出现的同一人。Markdown 围栏代码块、行内代码及转义标记保持字面文本，普通 `@名字` 与其他无效标记不产生结构化 mention。私聊中的标记保持字面文本；本项只提供明确 UID 点名，不新增广播语法或名称兜底匹配。

宿主把完整标记转换为 `@显示名`，为每个位置生成 `{uid, offset, length}`，offset/length 相对于最终发送段正文，以 UTF-16 code unit 计数；`mention.uids` 按该段出现顺序去重，`mention.entities` 保留每次出现并按 offset 排序。发送和编辑的 payload 使用同一确定性转换，不能在编辑中遗漏或保留过期 mention。UID 或 mention 元数据发生变化也算目标快照变化，不能因可见文本相同而跳过投递。转换不重新查询成员或用户资料；名称与 UID 来自已确认的标记，不在出站阶段另行猜测身份。

流式状态按单条 assistant message 隔离。增量尾部尚不能判定是正文还是 mention 的标记前缀先暂存，已确定的普通正文照常发送，不把半个标记泄漏到已发送正文；确认不是标记后恢复为普通文本。未闭合的行内代码或围栏上下文中出现候选标记时，先等待上下文确定或 final，再决定是否解析，不能提前发出日后被确认为代码的点名。完整有效且上下文已确定的 mention 立即转换并与其后的正文一起流式投递，已创建消息通过编辑更新正文及 mention；正文以 mention 开始时，标记完整即可发送，无须等待 final。其他 assistant message 的独立投递不受影响。

final 是权威快照：删除、移动或修正 mention、缩短正文、最终空正文均按最终结果处理，清理过期分段；最终无有效 mention 时按普通正文完成。最终拼接的可见正文等于 final 的完整转换结果，不能丢字或重复。

转换后的正文沿用 3000 UTF-8 bytes 上限及既有自然断点优先级分段，单个 mention 的可见文本作为原子片段，不在其内部切开，每段独立重算 entities 偏移。单个标记的可见文本本身超出一段上限时保持字面文本、按普通正文分段，不发送该标记的 mention。其他原始换行、空格及 inline 标记保持不变。

每次累计快照持久化足以确定性重建各段的正文与 mention，不依赖进程内成员缓存；final 的目标快照更新、过期段清理与幂等状态原子提交，事务提交前不开始由该次快照新触发的发送或编辑。沿用既有 desired/acked 版本、发送前 unknown 状态、稳定 client message ID、失败预算与停机取消；成功回执丢失后的重试、重复 final、重连和重启不得重复创建同一分段。尚未完成的标记前缀不投递；取消、失败或停机不自动补发该前缀，已投递的完整内容保持原有恢复语义。

API 创建或编辑消息均携带对应正文的完整 `mention.uids`/`mention.entities`，不额外发送点名通知；后续重试/对账修正只更新原消息，不通过重新创建消息补提醒。真实 Octo 群与子区实测在已有消息的编辑中加入 mention，可显示目标成员并产生 @提醒；不承诺操作系统推送送达，也不撤回已经发出的提醒。端到端验收须在真实 Octo 群/线程确认显示与提醒，不能仅以编辑接口返回成功判为通过。

#### 进度展示

真实 `tool_start`/`tool_end`、`turn_start`/`turn_end`、`todo_update` 和 `run_end` 事件由单一 `ExecutionTracker` 累计阶段、工具、todo、终态和 elapsed；每一轮——任务开始的一轮，或追加消息开启的新一轮——各自在其第一份真实工具或 todo 进度出现时懒建一张可编辑卡，之后只编辑该轮的卡。追加消息被接受时，上一轮的卡片立即以一次终态编辑收尾（`completed`），新的一轮从自己的第一份真实进度起在会话底部另开一张卡，因此正在看的进度始终在最新消息下方，而不是停在任务最初那次工具调用处；该轮的卡沿用任务当前的 todo 清单，阶段、工具与耗时只统计本轮。运行中 elapsed 只在真实事件到达时随卡片刷新，事件编辑遵守 1000ms 合并窗口，无事件时不做周期重绘；卡片 edit 使用严格递增的单卡 sequence，终态为 `completed`、`cancelled` 或 `failed`。卡片采用“摘要头部—当前状态—任务清单—子代理—执行轨迹”的进度通知布局；任务清单按阶段→任务的树形层级渲染，使用分支连接线和嵌套区块表达父子关系；任务节点只显示状态图标、任务内容和 blocker，不重复渲染状态文字；富布局下阶段标题与任务分支使用结构化 ColumnSet 分列，避免依赖比例字体空格对齐；富布局能力下使用强调色 Container、状态/耗时摘要和带状态元数据的步骤面板，轨迹步骤使用 Web 端兼容的 ColumnSet，轨迹行使用固定 24px 等宽标记列和 stretch 正文列，状态摘要置于正文列并允许自然换行，避免窄屏下正文被状态列挤压；状态图标与步骤内容、状态摘要同处彩色背景 Container 内；子代理列表位于执行轨迹之前；当对应区块超过三个步骤且能力支持 `Action.ToggleVisibility` 时，在标题行右侧提供“展开”/“收起”控件，默认展示最近三个步骤，其余步骤通过“展开”查看；能力支持 `Action.ToggleVisibility` 时，卡片头部始终提供“收起”/“展开”整卡详情控件：运行中默认展开任务、子代理和执行轨迹详情，收起后只保留摘要头部、当前状态与告警；终态卡片默认收起这三类详情，保留状态摘要，并可通过头部的“展开”恢复；状态标记按语义统一：成功/已完成使用绿色 `✓`，等待、取消和阻塞使用中性灰色 `-`，失败和放弃使用红色 `×`，进行中使用蓝色/Accent `•`；取消任务内容在支持 `RichTextBlock` 时使用删除线；能力不足时保持信息完整的降级文本。内置 `hub`/IRC、`task`、`todo` 和 `ask` 工具使用操作级安全摘要，IRC 轨迹显示方向、目标或协作状态，不显示消息正文；命令类工具显示压成单行的有界命令文本摘要，便于在轨迹里直接看到所运行的命令。卡片只展示安全摘要，不转发工具参数/结果、原始 payload、模型思考或秘密。超出渲染预算且仍可展开时，轨迹与子代理都使用同一组紧凑行，并放进各自区块的灰色 emphasis 历史带：每个步骤一行文本，状态字形后用不换行空格连接 `·` 与摘要，仍按状态着色，不用 ColumnSet 分列，以便同样的节点预算容纳更多隐藏行；能力不支持展开时没有可隐藏的状态，这些紧凑行照常显示（历史带保持可见），隐藏计数同样照常给出。
富布局下子代理区块由 OMP 自身的 subagent 事件驱动，而不是从工具调用名推断：worker 订阅 `progress` 级别后，`subagent_lifecycle` 的 `started` 记下一个真实的子代理（取 payload 的 `id`、`agent` 与可选 `description`），`completed`/`failed`/`aborted` 才是它的终态，`subagent_progress` 提供进行中的 `status` 与可选 `lastIntent`；同一 id 的终态之后不再被进度帧复活，`aborted` 归一为“已取消”。卡片只关心子代理的整体状态：行内容是 agent 名称、用途摘要、生命周期状态，以及当前正在运行的工具与已用工具次数（`<n> 次调用`），不渲染每个子代理自己的 `HH:mm:ss` 或耗时，也不展示 `subagent_event` 之类的子事件；子代理自己的会话记录保留全部细节。仍在运行或等待的子代理作为默认丰富行；已完成、失败、取消或停止的子代理默认隐藏，能力支持 `Action.ToggleVisibility` 时随“展开”在子代理区块的灰色 emphasis 历史带（`subagent_omitted_steps`）里以紧凑行查看，每个子代理只渲染一次，能力不支持该动作时该带保持可见而不是被折叠藏起；卡片只渲染事件报告过的子代理，OMP 不提供该订阅时该区块整体不出现（worker 记一条日志并照常执行）。执行轨迹默认展示最近三个工具步骤的丰富行，其余步骤（丰富窗口裁掉的较旧行与被预算剪掉的步骤）统一渲染在轨迹区块的灰色 emphasis 历史带（`timeline_omitted_steps`）里，一条单行紧凑文本一个步骤；该带是步骤列表 `timeline_detail` 的兄弟节点而不是子节点，因为 Web 客户端的 agent-progress 层会把 `#timeline_detail` 直接子级里有填充的 Container 重绘成失败步骤红块，放在外面才保住灰色填充。丰富行在状态摘要行首显示该步骤的 `HH:mm:ss` 本地墙钟时间（取快照里持久化的 `startedAtMs`，重放补发的终态卡同样显示），紧凑行是单行文本、不带时间。当前 OMP worker 以通用 RPC `tool_execution_start`/`tool_execution_end` 形成安全工具事件：`bash` 的摘要是压成单行、截断到 72 字符的命令文本，路径类工具只取安全文件名组件，其余工具按操作语义派生；命令文本中的凭据形态字符串由卡片层统一脱敏，宿主始终不转发原始 args/result。OMP 另有可选的 `set_subagent_subscription` 生命周期/进度事件桥，但它不属于 `pi.on` 专用 hook，且尚未纳入本服务的 `WorkerEvent` 卡片契约。
任务清单是本次运行对用户的承诺，执行轨迹是可展开回看的过程记录，因此节点预算不足时先压执行轨迹、任务清单最后才挤：适配循环对“完整可见任务清单”从最多 32 个可见工具步骤起逐级下调，每一级内部先按 8/4/2 收缩可展开隐藏行的渲染行数，全部级别都放不下时才按 `32/24/16/12/8/6/4/2` 的梯级缩小可见任务带，最后一级只保留隐藏计数。被裁剪时保留隐藏任务摘要；支持 `Action.ToggleVisibility` 时，标题行显示“展开”/“收起”按钮，展开后按原阶段和原任务顺序把可展开的隐藏任务插回树形位置，并复用富布局任务行的状态标记、颜色、blocker 与层级连接。可见任务围绕交接带选取：先钉住所有 `in_progress` 与 `blocked` 任务，再从交接锚点（第一个进行中任务，其次第一个未完成任务，全部完成时取末尾）向两侧扩展、已完成侧优先，使“已完成 ↔ 未完成”的分界及其前后若干条始终连成一段；可展开的隐藏任务取距离交接带最近的那些（上限 `min(hiddenRowLimit, 6)`），展开即向外延伸该段，而不是跳到清单两端。只有既没有可见任务、也没有可展开隐藏任务的阶段不渲染，避免出现没有子节点的裸阶段标题。能力不支持该动作时只保留安全的隐藏计数。

状态标记列以字面符号显示等待与取消状态：进入 TextBlock 的独立 `-` 必须转义为 Markdown 字面字符，避免被解释成空列表而在固定宽度的标记列内裁切；工具、子代理与运行状态使用同一规则。
富布局任务树的分支与状态标记共用固定 64px 列，正文使用 stretch 列；标记对齐任务首行，窄屏保留完整树形符号，并优先给任务内容与 blocker 留出换行宽度。
富布局头部将 Medium 字号的加粗标题与小号执行来源放在左侧，展开/收起控件在右侧垂直居中；不支持折叠时，标题区域使用完整可用宽度。
运行状态摘要将阶段数、工具调用数与耗时合并为一行可换行文本；同一摘要中的耗时只出现一次，状态标题保持独立层级。
工具与子代理的状态、时间或调用计数使用状态语义色的常规强度，字号仍为辅助层级；工具名称与摘要保持分行，便于窄屏扫读。

发卡前必须通过能力 profile 门禁：profile 启用，包含 `octo/v1` 与 card version 1.5，且 card/display/reasoning 开关均启用；`TextBlock` 是唯一硬依赖，`Container` 等布局元素缺失时使用扁平 fallback；`Action.ToggleVisibility` 仅在能力声明支持时渲染。遵守服务端声明的 payload、node、depth 等 limits：适配搜索按实际 wire envelope（`card` 加外层帧字段）统计节点数、深度与字节数，与服务端口径一致，避免选中一个服务端必然拒收的候选；无法满足时不发卡且不阻断正文；同一轮最多一张卡。

每张卡按 `(botUid, taskId, messageId)` 独立持久化：建卡后保存远端 message ID、sequence 1 与快照，成功 edit 更新 sequence 与快照，终态 edit 成功后清行。重启时，可续接任务接管按首次创建顺序（rowid）排列的最新一张卡，保留 message ID、序号、todo、工具历史与研究状态；下一次编辑从持久序号 +1 开始，不另建卡。旧进程仍运行的步骤在最后观测时刻收束，续接任务保持 running。之前已滚掉的轮以 completed 收尾；任务已有终态或无法续接的卡按 completed/failed/cancelled 补发终态。恢复后新轮使用独立幂等键段。终态补发使用 2 秒 deadline；失败保留行并递增 attempts 与 sequence，达到上限则丢弃并记录，坏载荷不阻断启动。

恢复时推断结束的工具行持久化内部 `recoverySettled` 标记；原生 retry 重用同一 toolCallId 时，真实 start 可重新打开该行，真实 end 清除标记。标记不参与卡片渲染，跨多次重启保留；真正已结束的步骤不能被重复 start 复活。即使续接任务已没有卡片行，新轮仍使用恢复幂等键段。研究快照属于整个任务，接管后立刻收到追加消息滚卡也继续保留。

typing 由按聊天单例的租约环驱动：agent-attached 消息在权限门通过、判定交给 agent 后立即发出（普通消息在待回答问题判定之后、附件组装之前；未注册 `/xxx`、`/queue`、`/thread`、`/thread merge` 的失败修复输入与 observing/intercepting 命令在各自交给 agent 前的同一点位），其后每 3 秒续期；同一聊天任一时刻至多一条续期环，owner 为 `msg:<messageId>` 与 `task:<id>` 租约——任务被持久接受时并入 task 租约、环不重启，同聊天排队与跨聊天同目录等待期间保持亮起，任务终态（完成/失败/取消/启动失败）释放，排队中 `/cancel` 等一切漏释放路径由每 tick 的任务行状态兜底回收，消息租约的释放由调用方 try/finally 硬保证（覆盖附件拒收、拒绝抛错、claim 去重早退等一切出口）；服务停止时切断新 acquire 并在 activity settle 后、store 关闭前释放全部租约；非 agent 命令、待回答问题答复、成员命令提示、不支持类型、权限拒绝与 Bot 回声一律不发送 typing。每个 typing/card 活动请求使用独立 2 秒 deadline，终态卡片使用独立短 signal。运行中卡片失败可按有界节流重试；终态 edit 失败只做一次 best-effort，随后 dispose 卡片活动，不无限重试，其遗留的持久记录由下一次启动补发。卡片正文被 Bot API 之前的部署边缘拒绝时——响应的状态码来自边缘本身、正文不是 Bot API 的错误信封，因而没有机器错误码——该轮改用不带工具与子代理步骤摘要的形态重试，并沿用至该轮的终态编辑：切换一次即成，状态、计数、任务清单与子代理行照旧，被拒事件与切换各记一条有界日志。卡片队列 promise 纳入服务生命周期并在停止时于任务结束后等待其有界收敛，不能拖延正文投递、取消 kill、目录解锁或正常业务。typing/card 失败不阻断正文或业务。

已有 typing 租约时，每条新到的 agent-attached 消息仍立即重发 typing，覆盖追加与排队消息，并从这次发送起重排唯一的 3 秒续期 timer；同一消息租约的重复 acquire 与 task 租约接续不重发。这样客户端因新消息清除 typing 后，本端无需等待下一次续期才补发。

typing 的已排定 tick 在服务进入 stopping 后直接回收该租约环，不再读取任务状态或发起续期请求，避免停机等待期间继续增加后台活动。

### 持久化、调度与恢复

本地 SQLite 保存聊天绑定、动态允许集合（`allowed_chats`：运行中开通的聊天及其 `granted_by`/`granted_at`）、任务、入站交接/去重键、问题、出站记录、进度卡记录和子区监听策略、创建方目标及持久合并成功事件，OMP 自身保存对话。receive callback 同步返回逐消息 `ackReady` 票据；完整权限门通过前不持久正文。授权后的短交接以 Bot 身份、可信来源类型与稳定来源 ID 唯一标识；媒体输入只有在本地 spool 与组装正文可恢复落盘后才可 ACK。状态盘写入失败时不发送成功 ACK、不启动副作用；ACK 只表示输入已被可靠接管或确定消费，不表示任务、命令、回执发送或归档完成。领域消费与 ingress 状态在同一事务交接给 task、steering、ask、受管 operation 或稳定 outbox；只有 pending command claim 时仍由 ingress 保留恢复责任。每个聊天的追加模式（`steer`/`queue`）持久化在与绑定、任务同一套状态盘中，未写入行时按 `steer` 处理。作为 steering 消费的入站消息同样先 durable claim 再发送 RPC 帧：claim 成功而 OMP 拒绝该帧时释放 claim 并回退为普通排队任务，已 claim 未确认的消息在服务器重投时不会重复注入。

附件下载对 HTTP 408、429、5xx 和连接或响应流中断最多尝试三次，等待 200 ms、400 ms 均计入既有单附件 60 秒及整消息 120 秒预算；永久错误、超限、取消和停机不重试。每次失败清理临时文件，只有完整文件才可发布并交接；显式附件耗尽后拒收，隐式关联耗尽后保留纯文本降级。下载错误不暴露附件 URL 或签名。详细合同见 `docs/specs/2026-09-16-inbound-message-content-design.md`。

共享说明的持久 marker 与 sidecar 状态属于 SQLite 中的 session context ledger，至少记录 `botUid`、真实聊天目标、真实 OMP session ID、当前 server version、`pending`/`committed` 阶段和 injection attempt ID。marker 的提交必须是同 session/目标/version 的 CAS；同一 pending 在重放前沿用其 attempt ID，提交后不得再次接受旧 attempt。新 session 不能继承旧 session marker，外部移动若保持同一真实 session ID 则保留 marker，`/new`/`/reset` 产生新 session 时清空其 marker。sidecar 与 marker 发布、extension 注入确认和 `description-write` 的后续 version 发布均不得以占位 session ID 完成。

任务状态为 queued、started、completed、failed、cancelled、interrupted。开始执行时写 started，真正派发 prompt 前写 `prompt_started=1` 与已验证的 session ID，收到该 prompt 的 `agent_start` 后同步写 `prompt_accepted=1`；本地 slash ACK 不算进入生成。每条 assistant stream 的分段独立持久化 outbox，任务终态保存本次真实 assistant 正文。正常执行及重启恢复按接受顺序、聊天与会话串行；不同会话可以共享目录并行执行，不提供文件写入隔离保证。

恢复在 Bot 独占锁及旧 worker 回收之后执行：queued 保持排队，started 且尚未派发的 0/0 行重新排队；1/1 行沿原任务和原 session 续接，不重新发送原始 prompt；1/0 行因接受状态未知而落 interrupted、封闭其待答问题并明确通知。续接调用 OMP 原生 `/retry`，仅当其明确本地完成（无可重试回合）才发送同会话继续指令，要求先核验不确定工具结果；真实错误直接失败，内部 retry 回执不投递。无法确认旧进程退出的目录保留队列并阻塞新 worker；恢复所需 session/cwd 缺失时失败，不回退 fresh 绑定。恢复队列先占调度顺序，待启动完成后放行，启动失败保持 queued。显式取消、中断、已完成任务均不自动恢复；去重记录跨重启保留。升级旧库添加恢复字段，并把旧 started 行标为接受状态未知，开库本身不改业务状态。外部副作用已发生而工具结果尚未落盘时，系统不能承诺通用 exactly-once。

`tasks.stop_intent` 持久化 `interrupt` 或 `question_timeout`，优先于 prompt 恢复标记处理；终态写入清除此列。任务进入 completed、failed、cancelled、接受状态未知或显式 interrupted 时，同事务取消该任务的 pending 问题，已答复行不动；排队取消遵守同一不变量。取消时记录关闭原因（`questions.closed_reason`：`cancelled`＝用户 `/cancel`，`interrupted`＝用户 `/interrupt`，`ended`＝任务自行结束、恢复收尾与重启相关收尾），显式 `/interrupt` 的停止意图与这批取消同事务落盘——它对迟到提交不可接受，恢复过程再被 SIGKILL 也不会留下可被当成长答复的旧题。超时意图在回收 worker 前写入，并在同一事务中取消恢复快照里未认领的待答旧题（记 `ended`），只保留本轮正在等待的题供迟到答复；问题 id 必须属于该任务。卡片关闭是随后可重复执行的投影。显式中断还清除本任务的内存恢复快照，已答复但未认领的内容不会因此另起回合。停止意图结算与接受状态未知的结算都产生真实 interrupted 终态，并按子区当前 listen 策略和合并修复抑制标记决定回传；普通停机挂起不产生结果回传。

取消是控制路径，不在普通任务队列后等待：发送 abort，等待 worker 停止；超过 10 秒关闭该 worker 进程树。只有确认进程退出且 channel 管理的执行已停止后才释放聊天/session 租约及共享使用登记；无法确认则保持该目录阻塞并报告未知状态。取消不回滚已发生的副作用，不保证已派生到外部系统的工作停止。服务启动恢复前核查自身管理的遗留 worker；不能确认停止时不运行冲突目录的新任务。迁移操作无法确认停止时同时保留 source/target 两目录 blocked，跨重启继续生效。

服务停止时先封闭同步 receive 入口，新投递不 ACK；中止尚未持久受理的附件下载、语音转写和权限查询，等待这些短交接清理后才关闭状态盘。已落盘的 ingress job 保持恢复资格；未确认接收的输入依赖服务端重投。任务、会话操作、merge/remove 等已交接业务按各自 ledger、受管进程组和停止顺序收尾，transport 的票据清理不等待模型、Git、outbox 或归档完成。附件准备中止时只删除本次新建的临时文件，不删除已被可恢复 job 或领域消费者引用的 spool。

**生命周期私聊通知（2026-11-02 确认）**：生命周期通知由服务本身统一发出，不在 Super hooks、CLI 或聊天命令各自建立发送逻辑。只发给当前 Bot 已确认的 owner 的私聊（channelType=1），配置中的显式 owner 优先，并按既有规则写入该 Bot 的状态盘；未显式配置时使用状态盘持久 owner。不使用 Bot 注册响应中的 `ownerUid` 替代 channel 的控制权绑定，不向群、线程、最近发言人或其他 Bot 的 owner 发送。尚未认领时跳过，不在后来首次认领时补发；首次认领回执保持现状。

启动通知正文为 `omp-channel-octo 已启动`，另附事件发生时的本地时间（含时区偏移）和当前服务 PID。发送时必须已完成 Bot 身份与独占锁确认、遗留 worker 回收及恢复任务调度准备，并已建立 Octo 连接、开放服务处理；它表示 channel 就绪，不表示所有遗留任务已执行完成。启动失败或重复实例被独占锁拒绝时不发送；每次成功启动最多尝试一次，连接重连、重复调用 `start()` 不重复发送。发送前若已进入 stopping，跳过启动通知。

正常停机通知正文为 `omp-channel-octo 正在停止；在途任务将挂起，启动后继续。`，另附本次停机事件的本地时间（含时区偏移）和当前服务 PID。在封闭新入站并中止入站 I/O 后、开始等待 worker 等停机资源收尾前尝试发送，此时 Octo transport 与状态盘尚未关闭。仅对已成功就绪的服务发送，初始化失败清理不发；重复 `stop()`、重复信号共用同一次停机，不重复发送。不宣称进程已经退出，不改变任务挂起、会话独占和重启续接语义。

Super 的启动、停止、重启，`oco` 触发的服务重启，SIGINT/SIGTERM，以及聊天 `/restart` 或 `channel_control.restart` 最终触发的正常生命周期，遵守同一通知规则。正常重启时，owner 私聊先收到旧进程正在停止、再收到新进程已启动（两次发送都成功时）；聊天 `/restart` 原有的发起聊天回执和 `channel_control.restart` 的延期说明保留，它们不是生命周期通知。服务恢复不增加其他 PID 完成通报或任务打断通报。

生命周期通知使用现有 transport 文本发送能力，每个事件一次发送尝试，独立的 2 秒总等待预算，不复用已被停机中止的入站 signal。失败或超时只记录一条有界、无凭据的服务日志，继续启动或停机，不改变它们原有的成功与失败判定；网络故障仍遵守服务自身连接与恢复语义。通知不进入持久 outbox，不登记重启通报行，不跨进程补发、不重试，不等待已有任务输出队列，也不启动 OMP、建立聊天会话或创建任务、进度卡。通知发出后进程仍可能继续运行一段收尾时间；无回执或超时不承诺远端绝对未收到，也不以补发实现恰好一次。

SIGKILL、进程崩溃、断电及启动前认证失败等无法执行正常通知路径的情形不承诺停机或失败通知；不添加外部监控、异常离线告警、心跳、重启风暴聚合或新的配置开关。异常后成功重新启动仍按成功启动规则发送通知。

Octo 断线不取消已经开始的 Agent；正文 outbox 与任务状态持久化，重连后按聊天顺序继续投递，不重跑任务。重连采用有上限退避（1s 起、翻倍、上限 30s，向下抖动）并重新认证；服务端主动 DISCONNECT（被踢，如同一 Bot 的另一次登录接管会话）与底层断线走同一条退避重连路径；只有注册或 CONNACK 认证失败才停止重连并给出本地明确错误。重复消息经持久去重。文字和文件 outbox 按聊天顺序发送，送达未知时沿用稳定 client message ID；投递失败按有上限退避重试（1s 起、翻倍、上限 30s、向下抖动），连续失败到 6 次或遇到永久失败（4xx，`err.server.bot_api.store_failed` 除外）就把该行标记 `dead`、在 `last_error` 与日志里保留真实失败原因，随即继续投递队尾——一条投不出去的消息不会把整个聊天永久静音；成功投递或内容更新会清零尝试计数。文件永久失败同样标记 dead 并报告，不阻塞后续文字回复。

会话文件按项目隔离存放在 `sessionsDir`（缺省 `~/.oco/sessions`，可配置）下：每个项目（任务绑定的 canonical cwd）一个 `<basename>-<路径摘要>` 子目录，worker 以该项目目录作为 `--session-dir` 启动。同 cwd 的聊天共用项目会话目录，但各自持有独立 session ID 与精确 session 文件；不同 cwd 使用不同项目会话目录。改动前的 `<stateDir>/sessions` 扁平根保留在恢复候选里，已有绑定继续按记录的绝对 `session_file` 恢复。

外部移动恢复只接受精确 session ID 且通过 session 文件的 `previousSessionFiles` breadcrumb 证明身份；不按最近文件、不按时间、不按模糊 basename 选择。候选 roots 按 OMP native profile 与 `.env` 对当前有效 cwd 的解析结果取得，再加 channel 的会话根（该项目的 `sessionsDir` 子目录）与改动前的状态目录扁平 sessions 根。候选不唯一、无法读取或无法验证时保持原绑定，不自动猜测；唯一候选通过 session header 的身份与 cwd 校验后立即持久化重绑，不启动 worker。`refreshState` 仅用于原生 `/move` 与 fresh 命令的 worker state 校验。

## 验收标准

1. 实际 Octo 私聊发起文件操作，在选定目录观察结果并收到完成回复；全程没有工具审批。
2. owner 在允许群 @Bot 和允许的 CommunityTopic 线程（channelType=5）可执行：线程的允许关系继承父群，父群在允许列表里时该群下新建的线程无需额外配置即可对话，父群不在允许列表的线程一律拒绝；其他成员、未 @、伪造文本 @、Bot 回声均不启动 worker 或命令。
3. `/new`、`/reset` 省略目录时按目标默认目录解析的固定顺序取目录：已有已验证会话沿用绑定 cwd，线程尚无会话时取父群绑定 cwd，其余使用 `~/tmp`；显式目录（含空格）规范化为真实目录；`/move <目录>` 使用原生 OMP `/move`，同一 session ID 保留历史与附件，非法目录或校验失败不改变绑定。
4. 首次普通消息创建默认目录会话（DM/群为 `~/tmp`）；从其他目录再次 `/new` 或 `/reset` 仍沿用当前绑定目录并创建 fresh session。
5. 私聊与两个群/线程的上下文不串线；交错完成的结果各自返回原聊天；同一持久 session 在共享说明 version 不变时连续任务不重复注入，version 变化时只注入一次新的完整快照并给出变化说明；共享说明读取失败不启动 worker。
6. 私聊获得 `ask`、`octo_send_file` 与 `channel_control`；群/线程额外获得 `octo_management`，其 `description-read` 实时读取对应服务端 `GROUP.md` 或独立 `THREAD.md`（分别对应 `groups/{id}/md` 与 `groups/{parent}/threads/{short}/md`），`group-info`/`group-members`/`group-update`/`list-threads`/`create-thread`/`thread-info`/`thread-members`/`thread-join`/`thread-leave`/`thread-delete` 作用于当前群或当前线程，写操作依赖服务端授权；`thread-dispatch` 把一条任务派发给本群 thread，交付按该 thread 的追加模式处置（并入可接收 steering 的运行中回合，否则成为该 thread 的任务）、结果如实回到工具结果与发起群回执、向两个聊天各投递一条可见说明，派发深度为 1；新建和无可靠 marker 的恢复 session 首次注入共享说明，普通未变化 turn 不注入；工具不能改写目标聊天或 cwd。
7. 多个聊天可绑定同一规范化目录，各自独立会话的任务可重叠执行；同聊天、同会话仍串行。其他聊天在源/目标目录运行或排队时 `/new`、`/reset`、`/move` 可成功，迁移保持自身会话身份；这些命令在当前聊天运行或排队、同会话 lease 冲突时拒绝。worker 未确认停止时，相关目录跨重启保持 blocked。
8. 普通正文保留原始换行、空格和 inline 标记；每个 assistant message 独立 stream，可投递前文首次非空时立即 create，后续以同一 message ID 每 1000ms 最多一次串行 edit，最终更短或空快照权威生效；正文按 3000 UTF-8 bytes 上限和既定自然断点优先级分段，已由满窗口定界的普通段不随流式增量重排，并清理 stale tail。结构化 mention 的增量转换和原子分段按验收 57–59 的专门契约执行。异步 prompt 失败、本地完成和 worker 异常退出均正确结束任务状态，不生成通用完成文字。
9. 正文 outbox 在 create 前 durable 记录 `unknown`，保存 desired/acked 版本；断线、重启或送达未知恢复时沿用稳定 client ID、编辑最新快照且不重跑 Agent，不因失败重复创建；单条投递的失败有界收敛——退避重试到 6 次或遇到永久失败即标记 `dead` 并把队尾让给后续消息，重启恢复仍从队首排空 pending。
   失败归属实际发出的快照版本：在途旧版本失败时若已有新 desired 版本，不消耗新版本的重试预算，也不据旧错误将新版本标记 dead；未知送达后的修正请求按修正时实际读取的版本计数。服务停止导致的在途请求中止不消耗投递预算，保留持久行供下一进程恢复，不因停机耗尽最后一次重试而丢弃正文。
   普通文字、文件和送达未知提示也纳入同一停机取消与排空流程；中止后的持久状态和重试次数保持不变。文件检查或未知提示结束后若服务已停止，不再开始下一次发送，恢复后沿用原 client ID 投递。
10. 执行过程中取消能越过待执行队列；后续队列被清除，取消不被宣称为回滚。未确认停止时不启动同目录新任务。
11. `ask` 问题可跨消息持久等待；owner 或已开通群/子区内成员在同一聊天的下一条非命令文本被当作答案：单个待答问题时按该问题校验（编号选项按单选/多选校验，其他文本原样返回），一次 ask 的多个问题同时待答时同一条文本作为整组答案写入每个仍未作答的问题（逐题按各自规则解释）并使该 ask 立即返回；不同聊天、无待回答问题与重复投递的文本都不作为答案，其中无待回答问题时的文本按普通任务处理；取消不使用默认答案。等待超时可配置：`ask.timeoutMs` 默认 6 小时、允许 30 分钟到 7 天，越界或非整数在下一次启动即被配置校验拒绝（`config.example.json` 给出 21600000 的示例）。等待超过该超时不作为过期：问题行保持 pending、卡片保持可提交、不发任何提示，当前回合作像被取消的 tool call 一样停住（任务记为 interrupted，agent 不自行继续）；owner 或成员之后的文本回复或卡片提交照常记录答案（并记录提交人 uid：`answered_by`，随 `answeredBy` 进入 ask 工具返回、随「由 <uid> 回复」进入卡片改写与唤醒文本）并作为同一 session 的新回合输入唤醒 agent（运行中的回合按该聊天追加模式并入，否则成为 durable 任务），同一答案不重复作答，超时后的部分提交与普通提交规则一致（已答不重复、未答保持待答）。宣布交互档时问题以 `octo/v2` 卡片投递：单题卡为选项按钮、选择列表或自由输入，选项题另带「其他」自由输入（填写后以它为准、覆盖选择）；一次 ask 的多个问题共用一张卡、每题独立输入 id、整卡一个提交，一次提交按题各做一次 CAS（同一事务），未填的保持待答并以新 generation 重绘，全部作答后卡片改写为终态（每题显示已回复内容）；owner 的点击在任何聊天生效、成员的点击在已开通群/子区生效，并与文本回复争用同一个答案判定，重复投递不重复作答；整卡超出 limits 或发送失败时回退逐题投递（单题卡或纯文本），卡片不可用时退回纯文本；事件队列按持久游标消费、处理后才 ack，非 `card_action` 事件与无端点部署都不影响问答。
12. 在接收、started 落盘、执行、回复阶段分别终止服务再启动：绑定与历史保持，0/0 任务重排、1/1 任务沿原 session 续接、1/0 任务明确拒绝自动恢复；旧 worker 未确认退出时不得启动同目录写入者，同消息重投不再执行已记录的副作用；外部移动只按精确 session ID 与 breadcrumb 唯一重绑，roots 依 native profile/`.env` 有效 cwd 解析，不选最近文件。
13. Octo 断开再连接不重跑任务；已持久化正文从 outbox 继续投递，送达未知明确呈现；真实 `ExecutionEvent` tracker 每轮用一张卡累计工具/turn/todo/run 终态与 elapsed，只在真实事件到达时随卡片刷新（编辑按 1000ms 合并窗口推进，无周期重绘），遵守 TextBlock/Container fallback、ToggleVisibility 能力和 payload/node/depth limits（按 wire envelope 计数），卡片 edit sequence 单调递增，预算不足时先压执行轨迹、任务清单最后压，裁剪后仍保留交接带（进行中任务、其相邻任务与已完成/未完成分界）、隐藏任务摘要与可展开的邻近隐藏行，不渲染没有子节点的裸阶段标题，丰富轨迹行在状态行首显示该步骤的 `HH:mm:ss` 本地时间，安全摘要不含原始 payload 与参数对象（命令类工具显示单行、截断到 72 字符并经凭据脱敏的命令文本）；typing 每 3 秒续期、活动请求 2 秒有界，终态卡片失败只做一次 best-effort，均不阻断正文；正文被 Bot API 之前的部署边缘拒绝的卡片（4xx 且响应不是 Bot API 的错误信封）改用不带工具与子代理步骤摘要的形态重试，该轮此后的运行中编辑与终态编辑沿用该形态，切换一次即成、其余字段照旧；每张卡的身份、sequence 与最近快照各占一行持久化，终态编辑成功后删除该行，进程重启后可续接任务接管最新卡片继续编辑，旧轮及终态任务的遗留卡补发终态；序号递增、失败有界，旧轮迟到更新不改变接管顺序。
14. 状态盘写入失败不启动新任务；同 Bot 第二实例不能启动（在重新注册 Bot 之前即报错退出）；App Bot 配置不能被验收为支持群聊。
15. `launchd install` 仅写入权限为 `0600` 的 plist，不调用 `launchctl`；实际启动、停止分别由 `launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"` 与 `launchctl bootout "gui/$(id -u)/com.omp.channel-octo"` 完成，`launchd uninstall` 只删除 plist。环境凭证会进入该私有 plist，runtime 复用 plist 中安装时解析出的 OMP 路径；退出交互终端后仍能从实际 Octo 收发，日志不包含 token。
16. 新建 session 的第一次 agent start 会注入目标的完整共享说明；恢复 session 只在没有可靠的同 session/目标/version committed marker，或 marker 仍处于 pending 时补注入；已 committed 的恢复不会因重新启动 worker 自动重复注入。
17. 同一 session 的共享说明 version 变化时，OMP 收到一次完整最新快照和明确的版本变化说明；删除也按带 server version 的变化处理并明确当前无说明；version 不变的普通 turn 的 OMP prompt 不包含共享说明正文，也不把共享说明并入群历史块。
18. OMP compaction 后，压缩上下文保留当前 turn immutable snapshot 的完整共享说明和 version 边界；即使 version 不变也只在该 compaction 生命周期中恢复性注入，下一次普通 turn 不重复注入；当前 turn 的 `octo_management` `description-write` 不改写这份 snapshot。
19. 共享说明 sidecar、持久 marker 和 extension 输入按 Bot、真实聊天目标与真实 OMP session 隔离，不包含 Bot token；extension 或 sidecar 目标不匹配、输入不完整或 session ID 未确认时在进入模型前拒绝共享上下文；同一 worker 进程内非绑定的 OMP 子会话（subagent、eval）既不被注入也不被拒绝，只有绑定 session 在 sidecar 缺失或校验失败时进入该失败路径，从未发布绑定 session 的作用域沿用原有严格行为；marker CAS 失败时保留 pending、不得标为 committed，并在下一任务重放。
20. OMP prompt acceptance 与 context marker 提交之间发生崩溃时，pending 状态可恢复并在下一任务重放，不能静默跳过；正常成功路径不重复注入同一 version，重放使用明确的 injection attempt ID。
21. `/restart` 只使用检测出的守护进程，忙时允许重启；未检测到时明确拒绝。真实 CLI 基准同时覆盖优雅重启与 SIGKILL：旧 worker 已退出才打开原 session，续接任务先完成、队列随后完成，绑定不变、已记录副作用不重复、没有恢复打断通知。启动后 `/status` 可用，进度卡沿原消息继续；守护进程未停止 channel 时保持运行并记录告警。
22. CommunityTopic 线程在尚无已验证会话时，其默认目录取父群当前绑定 cwd；父群无绑定、真实 channel ID 不含合法父群段或父群目录不可用时回退 `~/tmp`，不拒绝任务。线程首次成功建立会话后 cwd 固定，父群随后 `/new`、`/reset`、`/move` 不改写该线程；已建绑定但从未成功建立会话的线程在下一个任务重新按父群解析并写回绑定；启动时不对存量绑定做一次性回填。线程继承期间仍使用自己的 session、自己的 `THREAD.md` 与独立 marker，各自会话与父群可在共享目录中并行执行，不新增用户可见提示。

23. 追加模式按聊天目标持久化并在重启后保留，默认 `steer`：`steer` 模式下运行中收到的普通文本经持久 claim 后作为 OMP `steer` 注入当前回合，不新增任务或正文消息，并为该轮新开一张进度卡（上一轮的卡以 `completed` 收尾）；`/mode queue` 后同一文本走 durable acceptance 成为排队任务，独立正文与进度卡；`/queue <文本>` 在任何模式下都走排队任务（正文经同一附件组装点：随消息发来的附件落盘后以绝对路径进入排队任务正文）；没有运行中的回合、回合已结束或 OMP 拒绝该帧时该文本回退为排队任务而不丢失；同一 messageId 重投只注入一次。`/interrupt` 中止运行中回合并保留该回合已追加的消息及其后的排队任务，worker 进程不被终止；回合因中止结束且无后续真实事件时任务记为 interrupted、进度卡记为 cancelled，回合继续产出时正常结算。`/stop` 与 `/cancel` 等价，清除排队任务与未消费的追加消息。追加消息的消费可见：被消费的 steering 在并入回合上下文时、确实等待过的排队任务在开始执行时，各在该聊天补一行有界回执；仅投递成功而尚未消费、或被取消清除的追加消息不发回执；重启恢复持久队列，尚未被 OMP 消费的内存 steering 不承诺恢复。

24. 群级免@：服务端 `mention_pref.effective` 为真时，owner 在该群及其线程发送的未 @Bot 文本正常启动任务；`effective` 为假、查询失败或响应不可解析时不启动任务、不回复也不落任何入站副作用；同一个群在 60 秒缓存窗口内只查询一次，带 mention 的消息不触发查询；非 owner、不在允许列表或 Bot 回声在任何情况下都不产生该查询。

25. 线程隔离：`create-thread` 保留 `worktree_name` 单仓库路径及 ASCII 命名、占用退让；显式 `projects` 各用其 repo 与可选 base。shared 一个非仓库入口加多个项目 worktree；separate 每项目独立 thread/worktree。全部工作区、绑定与 THREAD.md 成功才 ready；部分失败及重启 preparing/failed 不执行。归属独立持久化，shared 缺失路径 fail closed，显式 `/new`、`/move` 换绑不丢失资源；验收细则见 Worktrunk SDD。

26. `/thread`：群聊中 owner 的 `/thread <任务>` 只把意图转成当前聊天的一个任务（正文含原始意图与交付说明：由助手自行确定子区名称与 `worktree_name`、`create-thread` 建区、`thread-dispatch` 派发；派发正文按 SDD 格式写——清晰到可直接生成 SDD 的需求直接写清、由子区立即生成 SDD 并开工，否则发起会话只做够派发的最少澄清、由子区把剩余需求按 SDD 格式澄清成 SDD 后再实现），命令本身不建子区、不建工作区、不发回执；随后助手按该说明建出子区与独立工作区（子区名与工作区名由助手决定，不再由任务文本机械派生）、子区绑定固定到工作区、任务以该目录启动自己的 session 并在子区显示派发说明、发起群收到一行如实描述这次交付（并入运行中回合或作为独立任务执行）的回执；任务文本为空回复用法；私聊与子区中的 `/thread <任务>` 调用明确拒绝且不产生任务（子区中的 `/thread merge` 是独立的合并入口，见 31）；正文按该聊天的追加模式处置：运行中的回合能接收 steering 时交付说明并入该回合（不新开任务），否则成为 durable 任务并在正忙时排队等待；随消息发来的附件（例如富文本里的截图）与普通任务走同一条组装点，落盘后的绝对路径写进原始意图，不再只剩 `[图片]` 占位。

27. Agent 的 Octo 身份：注册 Bot 后宿主把该 bot 的 token 经 stdin 写入 `~/.oco/octo-cli/<botUid>` 的 octo-cli profile（目录 0700，token 不出现在命令行、日志与工具结果中），并只把 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 注入 OMP worker；宿主继承的 `OCTO_BOT_TOKEN` 与其他 `OCTO_*` 仍被剥离，不进入子进程；每个 bot 一份独立凭据目录，后注册的 bot 不会复用前一个的凭据；octo-cli 缺失、登录失败、响应非 JSON 或返回的 robot_id 与当前 bot 不一致时只记录一条 ≤200 字符且不含 token 的日志、保持不注入，channel 照常启动并继续处理任务；任务里的 Agent 可以 `octo-cli` 以本 bot 身份访问未被 host tool 包装的 Octo Bot API，该路径不受 host tool 的当前聊天绑定约束，授权由服务端判定。

28. `thread-delete` 的瞬时失败重试：首次响应为 `err.server.bot_api.store_failed`（线缆 HTTP 400、body 声明 `http_status: 500`）或任意 5xx 时，用同一 `short_id` 换新连接重试一次（第二次调用携带新连接标记，不复用连接池连接）；重试成功时工具返回「已删除线程 `<short_id>`」且线程从列表消失；重试仍失败时工具错误被压成单行、≤200 字符并保留机器码（`err.server.bot_api.store_failed`；多行与超长服务端正文都在压缩截断范围内），线程保持存在、不报告假成功；4xx 参数与权限拒绝只调用一次、不重试。删除成功后按 `force` 口径联动清理 `cwd_pinned` 工作区（见 31）：干净工作区被删除（经 wt 或 git 回退）并在回执写明，有未提交改动且未传 `force` 时保留并提示 `force=true`，`force=true` 时丢弃未提交改动；工作区已不存在时回执说明；清理失败只进入回执、不影响已生效的删除。

29. 未开通群/子区的 owner 自助开通：允许列表为配置 `allowedGroupIds` 与状态盘 `allowed_chats` 的并集，线程继承规则对两者一致。owner @Bot 收到确认卡，同一聊天 10 分钟内不重复提示，原消息不启动任务、不落 `inbound`、不写 `tasks`。当前 owner 点击对应聊天和消息的卡片后，授权持久生效，配置文件同步追加，卡片显示成功及重新发送任务的提示；非 owner 点击不授权、不消耗卡片。伪造或错绑聊天、消息、owner 的事件不授权；重复点击、事件重投及重启不产生重复授权或任务。重启后旧卡不能授权，owner 点击旧卡得到已开通或重新获取开通方式的提示；在发送回执绑定前到达的点击须等待核验。卡片不可用、超出能力限制或发送失败时提示文本口令，owner 以 @Bot 加精确口令仍可开通。配置写回失败时授权仍有效并明确告知；卡片改写失败时文本回执说明结果。子区开通只覆盖该子区，父群开通后其子区按继承生效；未开通群内非 owner、未 @Bot、近似文本与 Bot 回声的入站消息保持静默，已开通聊天照常运行。
30. 进度卡的子代理区块只来自 OMP 的 subagent 事件：worker 启动时以 `set_subagent_subscription` 订阅 `progress` 级别（失败时只记一条日志，worker 照常可用），`subagent_lifecycle` 的 `started`/终态与 `subagent_progress` 的进行中状态被翻译为 `subagent_start`/`subagent_update`/`subagent_end`；卡片按事件的真实 `id`、`agent`、`status`、`lastIntent`、`currentTool` 与 `toolCount` 渲染运行中/已完成的子代理并计数，行内只有整体状态与工具活动（不渲染子事件、每个子代理的时钟或耗时），同一个 id 的终态不被后续进度复活，run 结束仍无终态的子代理按该回合终态收尾；子代理行不进入执行轨迹、不使用工具名推断；卡片持久载荷包含子代理行，重启补发时按同样规则重放；工具名（如 `task`）在轨迹里只作为普通工具行出现。

31. 子区工作区的 Worktrunk 生命周期：创建经本机可执行的 `wt switch --create`，使用项目实际来源、指定路径和可选 base，hooks/120 秒上限/取消沿用现有契约；wt 不可用时才回退 OMP。创建失败保留资源并返回未就绪。单项目/separate 的 `/thread merge` 在持久受理后由宿主后台 operation 执行 wt 默认 pipeline，不按资源占用限制并发；完整合并、结果回执与归档均不延长 receive callback 或输入 ACK。成功零 agent 任务、清理工作区与分支、回执送达及进度卡排空后归档；实际执行失败才交给 agent 修复并通过 host tool 重试。保持 600 秒上限、linked worktree 与持久身份校验、现场保留、去重及取消边界；同区取消/查询与无关聊天不等待 merge 完整结束。shared 命令与 host 均明确拒绝且不产生合并任务或副作用。thread-delete 在服务端成功后按持久归属、实际资源操作期排他和来源/分支校验清理；脏改动默认保留，force 才丢弃，占用/身份不符/未确认资源保留，入口只删除空目录。
32. `/switch` 模型选择：空参数查询全部 provider；精确 provider 参数只显示该 provider，查询不改聊天模型或活会话、不成为 agent 任务或 pending ask 答案。全局/provider 各验证 100 模型完整展示、101 仅展示前 100 并明确总数和缺项；default 控制项另计，卡片与文本范围和计数一致。运行中查活 worker 当前目录，空闲 fresh probe 在聊天 cwd 读取并停止、清理 ledger/临时目录。未精确匹配 provider 的参数继续原生解析模型 id/子串、`provider/id`、`@角色`、`:level`，拒绝及空白不改模型；default 清除优先于同名 provider。原生切换成功持久化并传给之后启动的 worker（含 resume、`/new`、`/reset`、`/move`），运行中原地切换且原回合继续；`/status`、`/help` 保持可用。

33. 子区回传：默认创建及迁移为 merge，普通派发各终态不新增父聊天消息、任务或 steering；off 静默；task 对独立派发四种终态及合并成功按当前策略回传。所有档位的合并失败及修复失败留在子区。创建与修改持久生效、跨重启保留；旧任务级通知登记不参与策略。通知有界、不附原始任务或整段输出，保留来源和非授权语义；稳定事件 ID 与 inbound claim 防止重复，不回答 pending ask；shared 默认静默、显式 task 报告派发，合并拒绝不变。
34. 会话目录布局：OMP 会话文件按项目隔离存放在 `sessionsDir`（缺省 `~/.oco/sessions`，可配置、按既有 `~` 规则解析）下——每个项目（任务绑定的 canonical cwd）一个 `<basename>-<路径摘要>` 子目录（0700）；`/new`、`/move` 与该聊天下一个任务都以该项目目录作为 worker 的 `--session-dir`，同一个 cwd 稳定映射同一目录，同名但路径不同的项目互不混淆；外部移动恢复的候选 roots 为该项目的会话目录、改动前的 `<stateDir>/sessions` 扁平根与 OMP native roots；未配置 `sessionsDir` 的配置仍按 `<stateDir>/sessions` 工作，已有绑定记录的绝对 `session_file` 不受影响。
35. 成员参与：已开通群/子区内，成员（非 owner）以结构化 @Bot 发出的消息走与 owner 相同的入站路径（待答问题作答、附件组装、steering/排队、任务接受与失败分类），全部当前支持的内容类型都可用；成员未 @Bot 的消息、私聊非 owner、未开通群/子区的成员消息一律静默拒绝且零副作用（不落 `inbound`、不建任务、零 mention_pref 查询）；成员的 `/` 开头消息不执行、不交给 Agent，同一聊天 10 分钟内至多一条提示，owner 的命令行为不回归；ask 卡片在已开通群/子区内任何成员可点击/提交，答案记录提交人（`answered_by`）并进入 `ask` 工具返回值（`answeredBy`）与卡片改写（「已由 <名字> 回复」，查不到名字回退 uid），私聊与未开通聊天的点击仍被忽略且无副作用；文本与点击争用同一 compare-and-set；放开的只是「成员可驱动助手」，命令、配置与私聊面不放开。
36. 回答署名显示名：`ask` 已回复行的署名把 `answered_by` 的 uid 解析成显示名（`GET /v1/bot/user/info`；30 分钟 TTL、最多 200 条、单次查询 3 秒预算的有界缓存，同一 uid 的在途查询合并，失败不缓存）；查不到、超时或失败一律回退成 uid，解析失败绝不影响答案落盘、卡片改写与投递，`ask` 工具返回值里的 `answeredBy` 始终是 uid；卡片里署名以自带 `weight: "Bolder"` 的 `TextRun` 呈现，同帧 `plain` 文本回退（消息正文那一路）用 Markdown 加粗，两句同源；等待超时后的唤醒文本仍按 uid 标注提交人。
   显示名查询同步抛错与异步拒绝都只影响本次解析；清理在途记录后，下次查询同一 uid 必须能重新请求并取得恢复后的显示名。
37. 会话创建回执的模型提示：`/new` 与 `/reset` 共用同一会话创建路径，其成功回执在 session id 与 cwd 之后附该新会话实际生效的模型（`已创建会话 <id>，cwd=<目录>，模型=<provider/id[:level]>`）。模型在停止候选 worker 前取自创建该会话的候选 worker 自身——与 `/switch`、`/status` 相同的 `worker.models()` 只读状态查询，不发起生成回合：设定了聊天模型时，候选 worker 以 `--model` 启动，回执即该经 OMP 生效的存值（含 thinking level）；未设定时回执展示 OMP 实际默认模型，绝不把字面 `default` 当作模型名。模型查询失败或 OMP 未报告当前模型时，以不含模型的回执（`已创建会话 <id>，cwd=<目录>`）送达且不编造模型值，`/new`、`/reset` 的其余失败路径（目录无效、租约占用、会话校验或持久化失败等）的回执与行为不变；`/move` 迁移既有会话，不产生创建回执。
38. 群历史与「这个」关联、引用媒体的入站合同：带附件消息（含无 caption 媒体）在群/线程历史中存活为完整条目并渲染 `[文件 名称 (大小)]` 式标记（历史自身零下载，相对 preview 等不可用 URL 不保留）；受权消息本身无附件、无可用引用时按「同一发送者、10 分钟窗口、唯一候选」视同引用，经同一附件组装点下载（`maxBytes` 预检、8 条上限、单条 60 秒与整消息 120 秒预算、语音转写）并以绝对路径进入 prompt，`[系统提示]` 注明关联来源；歧义、跨发送者、超窗、未知 seq/时间、`groupHistory` 或 `attachments` 关闭时一律不关联，且歧义时明确请发送者引用具体消息、绝不虚构已收到或已读取；关联取回失败降级为纯文本 `[系统提示]` 而非拒收任务。显式引用文件（`reply.payload` 内联 type 8/9、绝对 http(s) URL）经同一张表成为可下载附件，被引用媒体无正文时以 `[引用 <发送者>]` 标题注明来源，相对 preview 路径整个引用不成立。
39. typing 即时反馈：agent-attached 消息（普通任务消息、未注册 `/xxx`、`/queue <文本>`、`/thread <任务>`、`/thread merge` 的失败修复输入、observing/intercepting 注册命令）在权限门通过、判定交给 agent 后立即发出 typing——普通消息在待回答问题判定之后、附件组装之前，各命令路径在各自交给 agent 前的同一点位；同一聊天任一时刻至多一条 3 秒续期环，owner 为 `msg:<messageId>`/`task:<id>` 租约：任务被持久接受时同聊天排队与跨聊天同目录等待期间保持亮起，任务终态（完成/失败/取消/启动失败）释放，排队中 `/cancel` 等漏释放路径由每 tick 的任务行状态兜底回收；消息租约的释放由调用方 try/finally 硬保证，steer 命中的消息租约立即释放且续期不中断；非 agent 命令、待回答问题答复、成员命令提示、不支持类型、权限拒绝与 Bot 回声一律不发送 typing；服务停止时切断新 acquire 并在 activity settle 后、store 关闭前释放全部租约。
40. 通知可见性：`WorkerNoticeEvent`（`{type:"notice", messageId, text, level:"info"|"warning"|"error"}`）来自活动且未结算 prompt 上的 `extension_ui_request.notify`，以及当前 prompt 分派确认之前的 `command_output`。扩展通知的 `messageId` 取上游帧 id，非空、无 C0 控制字符或 DEL、不超过 200 个 UTF-16 code units；命令输出按请求 id 与输出序号生成 id，级别为 `info`。正文剥除除 `\n\r\t` 外的 C0 控制字符与 DEL，trim 后按 Unicode 字符边界截断至最多 4000 个 UTF-16 code units（含省略号）。无效或空文本丢弃，未知或缺失级别按 `info` 处理；启动期、结算后通知与其他被动 UI 帧不投递。CoreService 在中断归因和进度追踪之前，把通知作为 `notice:<messageId>` 的独立终态文本流交给 outbox，并添加级别标签；重复 id 复用同一条消息，不混入助手正文或任务结果，不创建进度卡。离线通知跨重启恢复投递，不重跑任务。未注册 slash 输入按既有独立任务路径接受，权限、去重和排队语义保持不变。
41. 本地命令的群说明提交：worker 对当前 prompt 的成功响应 `data.agentInvoked:false` 或匹配的 `prompt_result.agentInvoked:false`，在结算前恰好发出一次 `LocalCommandEvent`（`{type:"local_command"}`）。带 id 的完成帧必须匹配当前请求；旧协议省略 id 时仍接受，错误类型的 id、旧请求 id 和结算后的重放均忽略。CoreService 不把该事件计为中断后的工作或工具进度，本地命令完成时清理 sidecar，但不伪造 prepared receipt、不提交群说明 ledger。待提交的快照留给下一次真实生成回合，沿用既有 `initial`/`version-change` 与 pending attempt 规则；真实生成缺回执仍失败。研究模式保存在 OMP 原生会话中，恢复同一会话可继续使用该模式；服务重启不自动重跑已开始的任务。

42. 子区归档：`octo_management` 的 `thread-archive` 在群/子区会话可用（与其他 `thread-*` 同口径：子区里省略 `short_id` 即当前线程，群会话必须显式给出），输出有界；归档不在工具内即时执行，而是排定到该聊天本回合输出（助手正文与进度卡终态编辑）排空之后——任何发进子区的消息都会把归档子区解档，所以归档必须晚于本回合投递；仅瞬时失败（HTTP 5xx，或 bot API 把内部 500 包在线缆 400 上的 `err.server.bot_api.store_failed`）重试，最多 5 次尝试、每次间隔 5 秒，永久失败（如 403，或端点未部署时的纯 404）即刻结束，服务停止时立即结束等待并放弃；最终失败只向发起聊天投一条 `归档失败：` 加单行 ≤200 字符原因的说明，成功留一条服务日志，两者都不改变已发生的合并；重复调用与重复排定只产生一次归档（服务端对已归档子区返回成功，两端幂等）；同一聊天排定后出现新的已授权且可靠受理的输入即取消该排定。归档等待属于独立后台 operation，不影响该输入的 receive callback 或 ACK。
43. 合并后自动归档：`thread-merge` 成功且默认 pipeline 删除了工作区（`summary.removed === true`）时自动排定该子区的归档，工具结果在合并结果旁给出 `archive: "scheduled"`；显式保留工作区（`remove:false`）不排定；合并结果与归档结果分别可见，归档失败只是一条有界说明。

44. 研究带进度卡：权威 `research_update` 事件自身即可为当前轮开卡，后续研究帧沿同一张卡推进轮次，任务终态后研究带仍保留在卡上；steer 滚出的新卡沿用该任务最近一次研究快照，而新普通任务的进度卡无任何研究带。
45. `/autoresearch-history` 导出：owner-only；运行中任务不打断、不转向、不排队，不启动模型也不回答待答问题；同一 messageId 重投只发一次；导出文件内容与本聊天最近一份权威完整记录逐字节一致，入队附件为不可变 spool，后续报告重写不改变待发字节；没有研究记录的聊天只回短提示且不含任何其他聊天内容；报告按 bot+真实聊天隔离私有落盘，重启后可导出；报告落盘失败时产生它的任务仍按正常完成结算、产出照常投递。
46. `/retry`：使用当前绑定的 OMP 会话和原生重试语义；成功恢复时回执与重新生成的正文分别投递，等待真实回合结束；没有可重试回合时投递 OMP 提示并正常本地完成，群说明不伪造提交。重复入站不重复执行；运行中回合的旁路 `/switch` 输出不混入其通知。跳过 `before_agent_start` 的原生续回合仍通过 `context` 注入 pending 的共享说明并写入 matching receipt，后续模型调用保留快照，普通回合不重复注入、子会话不串入父群说明。

47. `/switch` 查询和 `/mode` 的配置卡片：octo/v2 可用时投递 owner-only 选择卡，否则同源文本回退；模型菜单含 default 加最多 100 个模型，并可见范围/计数/超限指引。点击验证当次 owner、发布 owner、messageId、真实聊天、actionId 及本卡选项；错绑和非 owner 不消耗卡片，在途点击保留重试。应用走既有解析和持久化路径；同聊天同类命令按到达顺序串行，代次贯穿查询、probe 清理与发送，慢查询被新查询/选择取代后不得复活旧菜单。排队执行前复核停机/owner/授权。过期、旧代次、跨进程卡片只给有界重开提示；重复点击不覆盖后续选择。终态卡展示真实结果且无控件，编辑失败回退文本。生命周期不持久化（上限 200 张、TTL 15 分钟），命令及配置点击不建 ask 行、不起生成回合、不回答 pending ask。

48. 迟到提交：问题关闭后的卡片提交按关闭原因分流——关闭原因是用户明确停止（`/cancel`、`/interrupt`，含重启恢复里结算这两种意图的行）或该问题提出于最后一次 `/new`/`/reset` 之前时，只回一次「该问题已失效，本次点击未作为答案。」且不落答案；其余关闭（任务 completed/failed、恢复收尾、重启接受状态未知、续接失败、超时清扫）按行 compare-and-set 记为 answered 并把「原问题＋还原后的答案＋提交人」作为该聊天的新输入投递（空闲成为 durable 任务、忙碌按该聊天追加模式并入，恰好一次唤醒），既不恢复原任务、也不重放原 prompt、也不作为同聊天另一张待答问题的答案；已记录答案的问题再收到的提交静默（不覆盖、不投递）；同一提交的事件重投、重复投递与服务重启都不产生第二次记录或第二次投递；无权限点击、无绑定卡片与未知 action 一律不生效；落答的卡片随即 best-effort 改写成终态（已回复并带署名，或按关闭原因关闭）。

49. 生命周期启动通知：配置 owner 或已有持久 owner 时，真实 CLI 启动并建立 Octo 连接、完成恢复调度准备后，owner 私聊收到一条 `omp-channel-octo 已启动`，含本次事件时间、时区偏移及实际服务 PID；其他私聊、群与线程收不到该通知。通知不表示恢复任务已完成。重复 `start()`、Octo 重连、启动失败及独占锁拒绝均不重复或误发；未认领 owner 时跳过，首次认领不补发。
50. 生命周期停机通知：对已就绪的真实 CLI 发送 SIGTERM 或 SIGINT，封闭新入站后、关闭 Octo 连接前向 owner 私聊发送一条 `omp-channel-octo 正在停止；在途任务将挂起，启动后继续。`，含本次事件时间、时区偏移及旧服务 PID；重复 stop 或信号仅尝试一次，初始化失败清理不发。进程仍完成既有 worker、会话租约、操作安全与状态盘收尾，不把停机改为取消任务。
51. 正常重启通知：真实 CLI 经 Super 兼容的测试守护端重启，两次发送成功时 owner 私聊按顺序收到旧进程停机、新进程启动通知，PID 与各自实际进程一致；聊天 `/restart` 的原发起聊天回执保留，群与线程只接收其原有命令回执，不接收生命周期通知。任务仍按原 session 续接，不重放已接受 prompt。
52. 生命周期通知失败：分别模拟启动和停机通知被拒、网络失败及发送不响应，每次通知在独立 2 秒预算内结束等待并留下有界日志，生命周期继续正常完成；不重试、不阻塞 worker 收尾、不被普通 outbox 队头阻塞，不在后续重启补发过时通知，不创建任务、会话或进度卡。
53. 异常退出边界：真实 CLI 被 SIGKILL 时不要求停机通知，后续成功启动仍发送启动通知；没有外部监控依赖。部署验收通过当前 Super 服务做一次正常重启，并在 owner 的实际 Octo 私聊确认停机与启动两条通知及 PID；本地夹具通过不替代这一真实投递验收。
54. 每轮成员快照：连续两个群任务和两个线程任务分别通过对应成员 API 获取本轮数据，第二轮能看到成员变化；排队任务在执行时获取而非入队时获取。0、10、11 人分别得到有效空名单、完整名称/UID 名单、准确人数与查询入口；线程只含本线程数据并标明父群，两个聊天的名单不混用，私聊不请求成员 API。
55. 新输入边界与失败：steering 下一次模型调用使用更新后的成员块，同轮工具调用和后续模型请求不额外查询；原生 `/retry` 及在途任务重启续跑关闭成员注入资格，磁盘有旧快照时也不请求成员或新增成员块，重启后未运行的队列任务正常获取，续跑中的新 steering 按新输入刷新。同一聊天两条 steering 按受理顺序发布；受控延迟和 session 切换不能使旧输入覆盖较新快照，多条输入在模型调用前已交付时只见最新快照。拒绝、重复输入、控制命令和没有唤醒新输入的 ask 作答不获取成员。成员 API 拒绝、超时和格式错误使新任务不进入模型并收到准确失败提示；steering 不并入、不自动排队、已有任务继续，且不泄漏旧名单或原始错误。
56. 注入层级与 CLI：实际模型请求中成员为临时 developer 数据，固定 system/developer 指引包含真实 bot/聊天身份、mention 规则、Loop/Docs/Drive/HTML/Messaging/Files/Mail/Marketplace 摘要及 skills/schema/help 入口；同一模型请求只含当前成员块，名字中的换行、引号和指令样文本按数据编码，授权仍由宿主判定。DM 有操作指引而无成员块；群/线程与 DM 的非绑定 subagent/eval 均不收到成员块、bot UID、聊天 ID、父群或 short id 等聊天特定字段；OMP 原生命令首行保持不变，GROUP.md/THREAD.md 的快照、去重及恢复不变。CLI 凭据成功与失败两条路径的指引均与实际环境一致。
57. Mention 内容：含中文、emoji、多个成员、重复成员及需转义显示名的正文转换后具有精确的可见文本、UID 去重及 UTF-16 entities；代码块、行内代码、转义示例、普通 `@名字`、格式无效标记和 DM 不产生 mention。只修改 UID 而显示名不变时仍更新 payload；final 删除或修改 mention 时不沿用旧 entities。
58. Mention 流式：逐 chunk 送入拆开的标记，已确定前文在 final 前可见；标记完整且上下文确定后与后续正文在 final 前流式更新同一消息，不能泄漏半个标记，也不能为 mention 强制创建第二条消息。未闭合行内代码先出现完整标记、后出现闭合反引号时，整个发送历史均不能含该标记的 UID 或 entities；围栏代码同理。正文从 mention 开始、final 移动/删除 mention、最终缩短或空正文、没有 final 的取消/失败均符合设计；其他独立 assistant message 不受阻塞。
59. 分段与恢复：多字节正文跨 3000-byte 边界时 mention 原子不被拆开，各段发送正文均在限额内且 entities 正确；超限单个标记保持字面文本而不点名。真实 SQLite/传输夹具覆盖最终快照、过期段清理和 final 幂等状态的同事务提交，写入失败或提交前崩溃不产生半提交或 final 的网络副作用。重复 final、创建成功回执丢失、重连与重启沿用稳定 client ID、正文、UID 和 entities，不重跑模型或重复创建分段；停机不发送尚不完整的标记。
60. 真实 Octo 验收：在允许群和 CommunityTopic 各发送一条流式回复，在同一条消息编辑中加入真实成员 mention，确认正文连续显示、目标成员正确且接收方收到 @提醒；无额外点名通知。另确认普通无 mention 回复保持既有流式体验。单元测试、HTTP 成功或只有文字 `@名字` 均不能替代真实客户端验收。

## 实施前提与未决事项
每轮成员上下文、固定操作指引、CLI 能力入口及普通回复 mention 已实现并通过验收 54–60。真实 OMP 验证了成员注入与 steering 刷新；真实 Octo 群和子区通过出站队列以同一消息 ID 完成前文、mention 和后续正文的流式更新，接收方确认显示及 @提醒正确、无重复提醒。类型检查、全量测试和正常重启/SIGKILL 恢复基准通过；本次实现未重启常驻服务。

既有实现已完成 `/new`/`/reset` 目录绑定、原生 `/move`、精确外部 session 恢复、正文 outbox、执行 tracker、卡片和生命周期边界；共享说明采用 session/version/OMP lifecycle context 设计；线程默认继承父群工作目录已实现：`src/threads/thread-target.ts` 是线程目标父群解析的单一来源，传输层 `contextPath` 与服务的默认目录解析都经它取父群 ID。线程隔离工作区已按验收 25 落地（`src/workspace/worktree.ts` 的 worktree port + 绑定的 `cwd_pinned`），依赖部署解析出的 OMP 可执行文件提供 `worktree add`；OMP 未启用 `worktree.clone` 时新工作区不含被忽略文件。Worktrunk 生命周期已按验收 31 落地：`src/workspace/worktrunk.ts` 负责可执行探测、wt 命令参数与结果解析、单行错误边界，`src/workspace/worktree.ts` 在本机有可用 wt 时用它创建、合并与清理，否则创建与清理回退现状；宿主调用不传 `-y`，命令审批按本机 Worktrunk 的项目配置进行。Agent 的 Octo 身份已按验收 27 落地：`src/runtime/octo-cli.ts` 维护每个 bot 一份 octo-cli 凭据目录，`workerEnv` 以 allowlist 把三个变量注入 OMP worker，其余 `OCTO_*` 继续被剥离；该路径要求部署环境存在 `octo-cli`，缺失时只降级为无 CLI 身份。模型切换已按验收 32 落地：`src/commands/model-switch.ts` 负责选择器解析与有界列表渲染，`chat_models` 表按聊天目标持久化模型，任务路径与 `/new`、`/reset`、`/move` 组装 worker 时读取该值并传 `--model`，`/switch` 的探测复用既有 `WorkerFactory` 起一个 fresh、无 prompt 的一次性 worker 并清理其临时会话目录。原生扩展通知的聊天可见性已按验收 40 落地：`WorkerNoticeEvent` 进入 `WorkerEvent` 联合类型，worker 事件翻译层把上游 `notify` 帧净化转换为该事件，`src/service.ts` 在 `workAfterStop` 判定与进度追踪之前按级别标签投递，复用出站队列的持久化、分块与有界重试，同一上游帧 id 经既有 stream 去重不重复投递。本地命令的群说明提交豁免已按验收 41 落地：`LocalCommandEvent` 进入 `WorkerEvent` 联合类型（worker 仅在匹配的 no-agent 完成恰好发出一次），`src/service.ts` 在 `workAfterStop` 判定前置位 `localOnly`，该回合跳过 context commit、sidecar 照常移除、ledger 保持 pending 至下一真实生成回合。子区归档与合并后自动归档已按验收 42/43 落地：`ChannelTransport.archiveThread` 调 `POST /v1/bot/groups/{g}/threads/{s}/archive`，`octo_management` 的 `thread-archive` 与默认 pipeline 的 `thread-merge` 收尾都只经 `planThreadArchive` 排定，`CoreService` 在该聊天回合输出排空后执行一次归档，仅瞬时失败（5xx 或 `err.server.bot_api.store_failed`）按 5 秒间隔最多 5 次尝试、停止时放弃、最终失败只向发起聊天补一条有界说明。

配置卡片已按验收 47 落地：`src/cards/command-card.ts` 是纯渲染（一行 `Input.ChoiceSet` + 一次 `Action.Submit`，终态帧无输入无动作），`src/commands/command-cards.ts` 持有进程内菜单会话（上限 200、TTL 15 分钟、按 owner 与 messageId/聊天/actionId 精确绑定、同聊天同类命令串行应用、发送在途点击保留重试、失效与失败都回落到文本），`src/service.ts` 只把 Bot 事件的 `cfg:` 前缀 action 交给它、其余仍交给问答卡。

迟到提交的三个边界取舍——换过会话（`/new`/`/reset`）之后提出的旧卡、用户明确 `/cancel`／`/interrupt` 之后的旧卡、以及已答复的卡片上再来一次不同的选择——已由 owner 于 2026-09-21 在本子区确认，分别维持「只回一次失效说明」「只回一次失效说明」「静默且不覆盖已落定答案」的处置。

迟到提交已按验收 48 落地：`questions.closed_reason` 随取消落在同一事务（`/cancel` 与 `/interrupt` 记用户停止，任务自行结束、恢复收尾与重启相关收尾记 `ended`，迁移前旧行与 `expired` 按 `ended` 读取），`chats.session_switched_at` 由 `/new`/`/reset` 成功切换时写入（`/move` 保持会话身份、不记切换），`src/questions.ts` 按这两个事实把关闭行的点击分流为「迟到答复」「静默」与「失效提示」三种处置，迟到答复复用既有等待超时唤醒路径（同一 compare-and-set、同一追加模式投递、同一份问题与答案渲染）。

生命周期私聊通知已按验收 49–53 接入 `CoreService`：启动操作在连接和恢复准备完成后发送启动通知，正常停机先封闭入站，再通过独立文本请求发送停机通知，最后收尾 worker、transport 与状态盘。并发启动共用启动 Promise；已就绪但启动通知尚在途时，停机先等待该次有界发送结束，保持启停通知和连接关闭顺序。当前 owner 从状态盘按 Bot 身份读取，通知使用独立的 2 秒 signal 与 deadline，不创建持久 outbox 或重启通报行。

研究只读呈现与完整记录导出切片按验收 44/45 落线：`src/omp/events.ts` 通过每个 prompt 独立的 `ResearchTracker` 折叠权威工具帧，`src/tasks/task-events.ts` 将 `research_update` 的完整报告交回服务落盘，`src/cards/progress-cards.ts` 拥有该任务最近快照、研究帧开卡与滚卡结转。`src/research/research-report.ts` 负责按 bot+真实聊天摘要键控的原子 latest 与导出不可变 spool（同聊天链式有序写入、stop 排水）；`src/service.ts` 接线报告存储并实现 owner-only 硬拦截 `/autoresearch-history`（入队 file outbox、messageId 级持久去重、入队失败自清 spool）。

未决事项：无（子区归档的真机端到端验收待部署服务器升级到含 bot 归档端点的 octo-server 版本后补做，见验收 42/43 证据）。线程默认目录继承已按验收 3/4/22 落地并验证（见下）；真实 OMP compaction、删除 version、崩溃重放和凭证隔离验证仍按共享说明章节的验收补做。

```sh
bun install
bun link   # 安装短入口 oco（~/.bun/bin/oco → 本 checkout）
mkdir -p ~/.oco/config
cp config.example.json ~/.oco/config/config.json
chmod 600 ~/.oco/config/config.json
# 可按需编辑 apiUrl（缺省为 https://im.deepminer.com.cn/）、ownerId（可省略）、allowedGroupIds，并在 botToken 与 botTokenEnv 中二选一
oco check --config ~/.oco/config/config.json
# 前台运行（守护进程托管的入口）：
oco start --config ~/.oco/config/config.json
# 调试运行（不参与守护进程）：
oco run --config ~/.oco/config/config.json
# 交由 launchd 管理时：
oco launchd install --config ~/.oco/config/config.json
# 重启 / 看状态 / 看日志 / 发预览卡：
oco restart
oco status
oco logs --tail 200
oco test --card
```

真实卡片视觉验收使用 `oco test --card`（等价于 `bun run test:preview-card`），仅在显式指定时向真实 Octo 会话发送一张综合预览卡片，包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）与 5 个子代理生命周期（2 个进行中、1 个失败、1 个已完成、1 个已取消）、4 个阶段和 12 个 Todo 任务，覆盖成功、等待、失败、取消和进行中状态；它读取 `BOT_TOKEN` 或 `OCTO_BOT_TOKEN`，目标优先读取 `OCTO_PREVIEW_TARGET_UID` 或 `OCTO_OWNER_ID`，未指定时使用 Bot 注册响应中的 owner UID，并可选读取 `OCTO_API_URL` 与 `OCTO_PREVIEW_CHANNEL_TYPE`（`1`、`2` 或 `5`），并会先应用 checkout 的 `.envrc`。普通 `bun test` 不执行该真实发送测试。
`botToken` 与 `botTokenEnv` 互斥；使用环境凭证时先设置 `OCTO_BOT_TOKEN`（或配置中允许前缀的变量名）。安装 launchd 后按上文命令显式执行 `bootstrap`/`bootout`；实际项目级 OMP/Octo 协议、会话迁移、正文与进度卡行为已完成实现。launchd 尚未加载；脱离终端运行和真人登录态完整自动触发回合尚未验证，仍以用户本机 Bot 配置和部署可达为前提。
首条有效私聊必须是支持的非空文本，且 `senderId`、`messageId` 非空并且发送者不是 Bot；它会原子持久化 owner 绑定和稳定去重确认，只发送确认，不执行任务。群聊和线程永远不会触发首次认领。首次认领存在风险：未配置 owner 时，第一个满足这些条件的私聊用户会成为 owner，请先确认 Bot 私聊入口的可信范围。

显式配置的 owner 是权威来源，服务启动时会持久化它；即使之后删除配置中的 owner，也不会重新开放首次认领。绑定跨重启保留，重复的首条消息不会执行任务；`check`/`serve` 在配置未提供 owner 时会提示“若状态中尚无持久 owner，将等待首次有效私聊绑定”。

本地实现还保留各阶段崩溃恢复、SQLite 故障注入、文件 outbox、`ask` 和 owner/群权限边界；接受状态明确的在途任务由原 session 自动续接，旧进程退出与接受状态门禁共同约束恢复。AdaptiveCard 视觉边界仅记录真实 Octo AdaptiveCard SDK DOM：390px 宽度无横向溢出、折叠/展开可用、todo 可见、`completed`/`cancelled`/`failed` 三种终态 SDK 校验无错误；实时预览命令覆盖带登录态的真实卡片发送，截图像素与完整真人交互仍需单独观察，不能混同。
验收证据（仅记录已完成的最终状态）：已在真实 `https://im.deepminer.com.cn/v1/bot/register` 注册成功并验证加密 WebSocket 认证成功；真实 CLI `check`/`serve`/`SIGTERM` 生命周期与 `plutil` 校验保持有效，真实 OMP fresh worker 可成功启停。真实 OMP 经加密 Octo wire 的正文为 2 次 create、19 次 edit，最终文本逐字一致；进度为 1 张卡、18 次 edit、6 个 phase、5 个 tool、2 个 todo，typing 18 次，stop 后无新增。真实 native project 20.66s 覆盖私聊与两个群隔离、含空格目录、native `/move` 同 ID 保留历史/附件、`/new`/`/reset` 复用绑定 cwd、外部 move 自动重绑后在新项目真实写入文件以及重启保持绑定。真实 Bot HTTP 已验证正文 create 加 2 次同 ID edit、卡片 create 加 2 次同 ID edit；实时预览验收命令已成功发送一张包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）、4 个阶段、11 个 Todo 任务及全部状态的综合预览卡片。launchd 尚未加载，脱离终端和完整真人登录态仍未验证；上文记录的 AdaptiveCard SDK DOM 证据不等同截图像素或完整 Octo 登录态视觉验收。

线程默认继承父群工作目录的验收证据：核心矩阵 `tests/thread-cwd-inheritance.test.ts` 覆盖父群继承、父群无绑定 / channel ID 无合法父群段 / 父群目录已删除三类回退、`/new` 无参继承与后续沿用、线程会话建立后固定、未验证会话重解析并写回、显式目录覆盖、以及 DM/群既有语义保持，共 10 例；把 `src/service.ts` 回退到修改前版本时其中 5 例失败，单独移除父群分支或移除父群 cwd 不可用时的静默回退分别使 6 例、1 例失败，确认用例不是空转。真实进程验收在 `tests/cli-lifecycle.test.ts`：真实 CLI + 加密协议夹具 + 真实 SQLite 下，父群 `@Bot /new` 建立目录后，线程任务以该目录启动真实 worker 子进程（夹具日志的 startup cwd 恰为该继承目录）、线程按 `/v1/bot/groups/<group>/threads/<thread>/md` 读取自己的 `THREAD.md`、`/status` 报告继承 cwd，并在 SIGTERM 重启后保持。
线程允许关系继承与派发的验收证据：`tests/core-host-tools.test.ts` 的 "threads inherit the parent group allowlist without their own entry" 覆盖父群在允许列表时其线程无需额外条目即可执行、父群不在允许列表时线程仍被拒绝并保持 `none`；移除线程的父群段允许命中后该用例失败。"thread dispatch queues the task in the target thread and reports it in both chats" 覆盖 `create-thread` + `thread-dispatch` 把任务交付给空闲子区时该子区接受真实任务（继承父群 cwd、使用自己的会话）、工具结果 `delivery=queued`、线程与发起群各收到一条可见说明、以及被派发的任务其 `octo_management` 动作集不再包含 `thread-dispatch`；移除派发深度守卫后该用例失败。"a dispatch into a running thread joins that turn in steer mode" 覆盖 `steer` 模式子区运行中的派发：内容并入该回合（断言该子区 `queued=0`、运行中的回合逐字收到派发正文、没有第二条任务），发起群回执写明并入运行中回合。"a dispatch into a queue-mode thread lands as its own task, never an answer" 覆盖 `queue` 模式子区在运行中且有待回答问题时的派发：问题保持待答（派发不被当作答案）、派发任务排队等待、回执写明在该子区作为独立任务执行。让派发不再跟随子区模式（始终直接接受任务）后第三个用例失败，把派发交给待回答问题后第四个用例失败，让回执只写一种结果后第三个用例失败，确认用例不是空转。

线程隔离工作区的验收证据：`tests/worktree.test.ts` 用真实 git 夹具 + 等价 argv 的可执行 stub 覆盖 slug 与候选命名、成功创建同名工作区、分支或目录被占用时退到 `<slug>-<short_id>`、不含 ASCII 的名称被拒、非 git 目录与命令失败的 ≤200 字符单行错误、以及超时与已取消请求；`tests/core-host-tools.test.ts` 的 "create-thread worktree_name binds the new thread to a workspace of its own" 断言工作区请求携带调用方绑定 cwd 与给定名称、新线程被绑定到工作区（其第一个任务以该目录启动）、成功结果含 `worktree.path/branch`，"create-thread worktree_name reports a workspace failure while keeping the thread usable" 断言工作区失败时线程仍按父群目录执行且结果含 `worktree_error`，同一文件的参数校验用例覆盖 `worktree_name` 非字符串与纯中文名称在创建线程前被拒；`tests/thread-cwd-inheritance.test.ts` 的 C11 覆盖父群 `/new` 换目录后隔离线程仍留在工作区，C12 覆盖工作区被删除时任务按父群目录完成而不是被拒绝。把 `defaultWorkDirectory` 的 `cwdPinned` 分支、或隔离线程的绑定写入分别移除后，C11 与 "create-thread worktree_name binds…" 用例各自失败，确认用例不是空转。真机冒烟（本机真实 `omp`，经 OMP 回退路径）：`createWorktreePort` 在临时仓库创建 `smoke-project.smoke-isolate`（359 ms，clone-first 把被忽略的 `node_modules` 一并带进新工作区），同名第二次调用按 `smoke-isolate-123456789012345` 退让，验证后 `git worktree remove` 清理。真实入口验收在 `tests/cli-lifecycle.test.ts`：真实 CLI + 加密协议夹具 + RPC worker 夹具下，群任务经 `octo_management` `create-thread` `worktree_name` 走部署同款入口（解析出的 OMP 可执行文件）创建真实 git worktree，断言返回路径是磁盘上带该分支的工作区；把 `runService` 的 `worktrees` 接线移除后该用例以线上同款 `worktree_error` 失败，确认用例不是空转。

子区工作区 Worktrunk 生命周期的验收证据（验收 31）：`tests/worktree.test.ts` 用真实 git 夹具与可执行 stub 覆盖——探测选择可执行候选并回报命令与版本，实际命令结果决定操作成败，缺失候选保持回退（候选按 PATH 名优先、其后是常见安装位置，实测见下）；创建经 `wt switch --create <branch> --no-cd --format json --config-set worktree-path=<候选路径>` 落到给定路径（同一次调用断言 OMP 回退可执行文件一次都没被调用），wt 不可用时回退 `omp worktree add`，失败命令与超时则压成单行 ≤200 字符错误、按取消中止（不回退）；`merge` 把默认 pipeline、`target`、`--no-*`、`--stage` 逐项映射成 argv 并解析 wt 的 JSON 结果、无 wt 与在非 linked worktree 中调用分别拒绝；`remove` 经 `wt remove --foreground` 回报分支处置、wt 缺失时回退 `git worktree remove`（有未提交改动拒绝、`force` 丢弃、主检出拒绝）。`tests/core-thread-command.test.ts` 覆盖创建交接与资格拒绝，`tests/core-thread-merge-host-first.test.ts` 覆盖宿主先执行、成功零任务、失败一次接管、去重与资源独占；真实 Git 命令串联 smoke 另验证目标内容、清理与回执先于归档。`tests/core-host-tools.test.ts` 覆盖 `thread-merge` 把工作区目录与映射后的参数交给端口并把 wt 结果对象回给模型、非法 `stage` 与非子区调用被拒、端口失败压成单行 ≤200 字符、未绑定工作区的子区拒绝，以及 `thread-delete` 的联动清理：干净工作区经端口删除且回执写明、工作区已不存在时回执说明、dirty 未传 `force` 时保留并提示 `force=true`、`force` 时以 `force: true` 删除。真机实测（本仓库 + 本机真实 Worktrunk v0.76.0，直接驱动 `createWorktreePort`）：探测返回 `{"available":true,"command":"/opt/homebrew/bin/wt","version":"v0.76.0"}`（本机 PATH 不含 Homebrew 目录，探测按预设位置命中）；创建经 `wt switch --create wt-smoke-20260917 --no-cd --format json --config-set worktree-path="…/omp-channel-octo.thread-worktree-wt.wt-smoke-20260917"` 在 0.42 秒内建成同名目录与分支，pre-start 的 `bun install` 由 wt 以 exit 0 执行（`.git/wt/logs/commands.jsonl` 记录 `dur_ms:31`），工作区带 `node_modules`；合并以 `wt merge --format json wt-smoke-target` 执行 31.7 秒，pre-merge 的 `bun run typecheck` 与 `bun test` 各以 exit 0 运行（同日志记录，31.2 秒），返回 `{"branch":"wt-smoke-20260917","target":"wt-smoke-target","removed":true}`，目标分支收到该提交、工作区与已合并分支被删除；回退实测：候选不可用时探测报 `missing`，创建经 `omp worktree add` 成功、删除经 `git worktree remove` 报 `{"kind":"removed","via":"git"}`；删除另经 wt 实测：新建的 `wt-remove-smoke` 工作区经 `wt remove --foreground` 删除并回报 `{"kind":"removed","via":"wt","branchOutcome":"deleted"}`（目录与分支均消失），对已删除路径再次调用回报 `{"kind":"missing"}`。宿主调用从不传 `-y`/`--no-hooks`，审批按项目非交互执行。
`/thread` 的验收证据：`tests/core-steering.test.ts` 的 "/thread joins the running turn in steer mode instead of waiting behind it" 覆盖群内已有运行中回合时的 `/thread <任务>`：交付说明并入该回合（断言正文含「原始意图」与 `thread-dispatch`、没有第二条任务、`queued=0`），把交付说明改回直接接受任务后该用例失败（`queued=1`）。`tests/core-thread-command.test.ts` 覆盖 4 例——群内 `/thread <意图>` 只产生一条以该意图结尾、含 `create-thread`/`worktree_name`/`thread-dispatch`/SDD 正文格式与澄清要求的任务 prompt（cwd 取该群绑定目录），此刻不建子区、不请求工作区、不发回执；夹具在 prompt 回调里按交付说明调用 `octo_management`（`create-thread` 带助手自定的 `name`/`worktree_name`，再 `thread-dispatch`），断言工作区请求携带该名称与调用方绑定 cwd、子区绑定到工作区并让派发任务以工作区为 cwd 启动、子区收到派发说明、群收到派发回执；空任务、私聊与子区中的调用分别回复用法或明确拒绝且不产生任务；`queue` 模式下群内已有运行中任务时该任务排队、在前一回合结束后才以指令 prompt 启动（`steer` 模式下的并入见 `tests/core-steering.test.ts`）。把交付说明换成裸意图后前 2 例失败、单独移除子区守卫后拒绝用例失败，确认用例不是空转。`/thread` 带附件的用例在 `tests/core-attachments.test.ts`（"thread 带附件：意图走同一条组装点，附件随交付说明一起交给助手"）：群内 `/thread <任务>` 带图片时恰好下载一次、图片按其绝对路径进「原始意图」块且落盘字节与远端一致；把 `handleThreadCommand` 里的组装点调用换成直接接受任务后该用例零下载失败，确认用例不是空转。

群级免@的验收证据：`tests/core-mention-pref.test.ts` 覆盖 `effective` 为真时未 @Bot 的群消息与线程消息各启动一个任务、为假或查询失败时保持不启动且不回复、同一个群两次未 @Bot 只查询一次而带 mention 的消息不查询、被 owner/允许列表/Bot 回声规则先拒的消息零查询；`tests/octo-transport.test.ts` 的 "reads the group 免@ decision, including the legacy no_mention shape" 用真实 HTTP 夹具断言端点路径与 Bot 鉴权头，覆盖 `effective` 布尔与旧版 `no_mention` 0/1 两种形状，以及不可解析响应被拒。把 `isAllowed` 的免@分支摘掉后 `tests/core-mention-pref.test.ts` 有 4 例失败，确认用例不是空转。线上只读实测（本群）：`GET /v1/bot/groups/e3bc5af0d4b84b18bb3770a74bd222c6/mention_pref` 返回 `{"no_mention":0,"group_allow_no_mention":1,"effective":false}`，即群开关已允许、bot owner 尚未开启，因此本群当前仍需 @Bot。
进度卡恢复的验收证据：`tests/core-progress-card-recovery.test.ts` 覆盖在线落盘与清行、终态补发及失败上限、多轮与坏载荷、续接接管同一 message ID 与递增序号、保留 todo/工具/研究/阶段计数、旧轮迟到重写仍按创建顺序接管、恢复后新轮幂等键隔离，以及 CoreService 停机后沿原卡完成。`tests/core-presentation.test.ts` 继续覆盖 steering 滚卡与在线渲染；布局未改变。

无感重启的离线实进程验收由 `bun tests/benchmarks/restart.ts` 执行：固定正常重启与 SIGKILL 两种场景，原任务沿原 session 完成、排队任务继续，且旧 worker 退出后才出现新写入者；基准同时检查已提交副作用、重复投递与再次重启。已测结果为 2/2，session overlap、duplicate effect 与 disruption notice 均为 0。隔离环境中的真实 OMP 18.2.6 探针验证本地 `/retry` 无可重试回合时同会话继续，原用户输入与已完成历史各保留一份。真实状态盘的只读备份迁移两次，17 张表、4234 行原字段保持不变；旧 started 行在显式恢复时因接受状态未知安全收束，终态行不变且再次恢复幂等。
进度卡正文被部署边缘拒绝时的降级验收证据（验收 13）：线上 403 不是 Bot API 的判定——服务端编辑端点对非本 bot 消息与不存在的消息分别回 `err.server.bot_api.message_edit_forbidden` / `err.server.bot_api.message_not_found`（`pkg/errcode/bot_api.go`，守卫在 `modules/bot_api/send.go`），两者都是 JSON 信封；而日志里 18:23:25 与 18:27:39 的编辑失败（`target=5:e3bc5af0d4b84b18bb3770a74bd222c6____2100480855066546176 seq=30`）响应为 HTTP 403 且没有信封携带的机器码。只读探针（本仓库真实 transport 与真实 `stripCardSummaries`，token 取自主检出的 `.envrc`，目标 message_id=1 因而不会写任何消息）在同一次运行中给出成对结果：带 `cat > /tmp/probe.ts <<'EOF'` 摘要的卡片帧被边缘拒绝——`status=403`、`code=null`、错误文本以 `(non-bot-API response body)` 结尾；同一快照经 `stripCardSummaries` 后落到 Bot API 并被正常判定——`status=400`、`code=err.server.bot_api.message_not_found`（「消息不存在。」），证明降级形态确实穿过边缘，服务端像对待普通调用一样应答。`tests/octo-transport.test.ts` 用真实 HTTP 夹具覆盖两种应答：JSON 信封的 400 在错误文本中保留机器码与详情（`err.server.bot_api.request_invalid`），非 Bot API 响应体的 403 标为 `non-bot-API response body` 且 `code` 保持 undefined（`status` 仍为 403）。`tests/core-presentation.test.ts` 的 "an edge-refused card body is retried without step summaries and still closes" 用真实 CoreService、真实 SQLite 与夹具 worker 断言：带步骤摘要的运行中编辑被边缘拒绝后，重试落在同一张卡（message ID 不变、未重新建卡）且帧内工具行不再带 `summary` 键而 `toolName`/`state`/任务清单照旧，随后的终态编辑沿用同一形态并成功收尾，日志恰好给出「运行中编辑失败」与「卡片正文被网关拒绝，改用不带步骤摘要的形态重试」各一条；把降级切换去掉后该用例失败（重试帧仍带摘要），确认用例不是空转。夹具新增 `messageEditErrorBody`/`messageEditErrorContentType` 用于回放边缘的网页应答；建卡路径未观测到同类拒绝（两日日志 0 条创建失败），其失败恢复保持原样。

追加消息消费可见性的验收证据：`tests/omp-worker.test.ts` 的 "steer injects a message into the streaming turn until that run ends" 断言真实子进程夹具发出的 `role: user` 且 `steering: true` 消息帧被翻译为恰好一次 `steer_consumed` 且文本逐字一致，同文件的 message-end 用例断言未标记 `steering` 的用户消息不产生该事件；`tests/core-steering.test.ts` 的 "a steered message reports the moment it joins the running turn" 断言消费后在聊天投递唯一一行「已并入当前回合 ▸ …」（含 20 字截断），"queue mode turns a message during a run into a task that follows it" 断言只有确实等待过的消息在开始执行时收到一行「开始执行排队消息 ▸ second task」。把 `#emitWorkerEvent` 的 steering 分支、`onEvent` 的消费分支、`runTask` 的开始回执分别移除后，对应的三个用例各自失败，确认用例不是空转。真机实测（不经过常驻服务，用真实 OMP 18.2.1 可执行文件与本仓库 worker 工厂直接驱动）：prompt 请求执行 `sleep 8`，4 秒后投递 steering 文本，事件流为 `turn_end`(0) → `turn_start`(1) → `steer_consumed`（文本逐字一致）→ 后续工具事件，全程恰好一次 `steer_consumed`，首轮 prompt 自身不产生该事件。
`channel_control` 的验收证据：`tests/core-host-tools.test.ts` 覆盖私聊与群/线程的工具集合及 `restart`/`status` 行为。`status` 报告本进程 PID 与 restart_pending；`restart` 在回合内只排定重启，本回合及队列尚未结束时不调用重启端口，全部空闲后恰好调用一次。未检测到守护进程时明确拒绝。真实 CLI 重启后的可用性由 `tests/cli-lifecycle.test.ts` 查询新进程 `/status` 验证。
交互问答卡片与 Bot 事件队列的验收证据：`tests/core-ask-card.test.ts` 覆盖单选提交与文本回复、自由输入、多选、空提交后 action id 更新、权限边界、点击重放、发送失败回退、多题整组与部分作答、显示名归因，以及等待超时后的继续作答。单选长标签用例检查完整内容、选择输入、唯一提交动作与信封节点/深度预算，并通过实际 CoreService 提交第二项后记录答案 `2` 与卡片改写验证答案契约；多题降级为单题时使用相同的选择后提交路径。旧单选按钮的持久卡片继续由原 action 解析路径覆盖。`tests/octo-transport.test.ts` 的 "Octo interactive cards and Bot events" 用真实 HTTP/WS 夹具覆盖 `octo/v2` 卡片发送与已答复改写、交互未开启时拒绝出卡、`POST /v1/bot/events` 的游标与 ack、`wait` 上限被夹到客户端请求超时以内、端点缺失视为无事件支持、队列与 ack 失败传播，保留独立输入 id 与提交 action id 的 wire 契约。

单题选择统一验收（2026-09-21）：单题单选与多题共用展开选择控件，浏览器实际勾选不会提交，点击提交后经生产解析器得到所选答案；多选与自由输入覆盖同一提交链路。固定视觉基准完成 42 次渲染、12 次提交与 60 次展开/收起，横向溢出为 0；类型检查及全量 830 项测试通过。真实 builder 的单题单选预览经当前 bot 身份发送至 owner 私聊，服务端接收成功；真实客户端像素表现仍需用户查看预览确认，常驻服务未重启。

问答卡视觉评测（2026-09-20）：`bash autoresearch.sh` 使用真实 `buildAskCard` / `buildAskCardSet` 输出、固定的 AdaptiveCards 3.0.4 与 Chromium，在离线环境渲染单选、长选项、多选、自由输入、已回答、关闭、多题、部分回答与全部回答 9 个场景，分别检查 340px / 440px 卡片宽度；每次执行 8 次真实渲染器提交并交给生产答案解析器。主 agent 已查看两种宽度的截图，核对完整标签、选项与动作邻近性、题目层级、已回答署名与多行输入。最终几何代理指标 `layout_cost=395.44`（基线 405.66），18 次渲染无横向溢出，最大 29 节点、深度 6、载荷 3008 字节。分数不是审美评分，保留方案同时以阅读与操作效果判断。另已通过真实 Bot API 向 owner 私聊发送 3 张带「样式预览」标签的单选、多题与部分回答卡片，服务端均返回 message ID；没有关联待答任务、重新注册 Bot 或重启常驻服务。真实 Octo Web 在本次浏览器环境中要求登录，因此这些回执只证明服务端接收，真实客户端的视觉结果尚待验收，本地 SDK 截图不能替代该项。

迟到提交按关闭原因分流的验收证据（2026-09-21）：本次截图（子区 `5d6764b12e6d4a64842290c2f5437dbd____2101576845110546432` 的「归因已清…下一步？」卡片）经只读取证还原为「恢复收尾」关闭——问题行 `49c95989-240f-4a64-a2b7-6f967a94ffef:b00db434-f90c-4804-a752-3bd519ebaa47` 15:50:03 提问、23:14:27 关闭；其任务 15:39:05 接受、21:50:03 结束（= 提问后恰好 6 小时，等待超时停住的回合）；日志 `[2026-09-20 23:14:26] [restart] 已执行 … super -y restart omp-channel-octo` 说明该行是随重启的启动清扫被取消的（当班版本未在终态写入里连带取消它），旧行没有 `closed_reason`，按新规则读作 `ended`，属于接受的迟到提交——owner 次日 10:07 的点击因此只拿到「该问题已失效」回执，在新实现里会按该聊天的新输入投递。测试覆盖：`tests/core-ask-restart.test.ts`（32 例）在真 store 上断言 `ended` 关闭行的单选/整组提交各自 compare-and-set 落答案（含迁移前 `closed_reason` 为 NULL 的旧行）、一次性唤醒（正文含原问题、渲染后的答案与提交人）、重复提交与同一事件重投静默、空提交与越界编号不补答案、非本卡 action id 不回应，`cancelled`/`interrupted` 关闭行与早于 `chats.session_switched_at` 的旧行只回失效说明且不落答案；`tests/core-ask-card.test.ts`（34 例）的 "a submission on a card the task failure closed becomes the chat's new input" 用真实 CoreService 走通「任务失败关闭 → 点击 → 新任务（正文含原问题、答案、提交人）→ 重投不重复执行」，"a late submission joins the running turn in steer mode" 断言忙碌聊天里同一次提交并入正在跑的回合而不新起任务（append mode 不变）；`tests/core-new.test.ts` 与 `tests/project-bindings.test.ts` 断言 `/new` 写入切换时间、`/move` 与随后同会话绑定写入不覆盖它。合并 `09e87c5` 后 `bun run typecheck`、全量 840 项测试、固定卡片基准（`layout_cost=442.92`、42 次渲染、横向溢出 0、小按钮 0、最大 200 节点／深度 10／17109 字节）与重启基准 `bun tests/benchmarks/restart.ts`（graceful/crash 两例，无会话重叠、无重复副作用）通过。关闭行说明与迟到答复行在真实客户端上的像素表现未单独验收：两者复用既有已验证的关闭/已回复卡形状，仅文案不同（真机结论见 2026-09-19 条目）。

综合卡片视觉评测（2026-09-21）：固定离线基准覆盖进度、终态、长任务、研究进度、能力降级、问答与配置卡，共 21 个场景 × 340px / 440px；每轮执行 12 次提交和 60 次展开/收起。最终 `layout_cost=442.79`（基线 2456.73），其中裁切计数由 2 降至 0，初始卡片平均高度由 456.73px 降至 442.79px（3.05%），小尺寸按钮计数为 0；该指标是几何缺陷代理，不是审美或用户满意度评分。最大信封为 200 节点、深度 10、17109 字节，最终复测结果一致。主 agent 已逐项查看保留方案的双宽截图并操作折叠控件，确认标记可见、任务首行对齐、长任务交接与状态详情可读。类型检查、隔离 TMPDIR 的全量 746 项测试及真实 `oco test --card` 通过；最终综合、长任务与研究三张预览已获发送回执。浏览器中的真实 Octo Web 仍停在登录页，因此上述截图属于本地 SDK 验收，真实客户端像素结果尚未确认；常驻服务未重启。


回答署名显示名的验收证据（2026-09-20）：`tests/display-name.test.ts` 覆盖缓存 6 例——同一 uid 只解析一次并复用、TTL 到期后重新解析、查询失败或返回空名时不落条目且下次重试、同一 uid 的并发查询合并成一次请求、查询实现无视取消信号时解析仍在预算内收尾（20 ms 预算的用例）、条目超过上限时按插入顺序淘汰最旧的。`tests/core-ask-card.test.ts` 新增 3 例（真实 CoreService、真实 SQLite 与夹具 transport）——单题卡改写后署名 run 逐字为 `{ type: "TextRun", text: "董振兴", weight: "Bolder" }` 且同帧 `plain` 为 `已由 **董振兴** 回复：2. production`，而 `ask` 工具返回值仍是 `answeredBy: "owner-1"`（身份不变）；名字查询失败（夹具 `userInfoFailure`）时卡片照常改写成 `已由 **owner-1** 回复：2. production` 且答案照常落盘；整组卡上两个提交人各自解析（有名用名、无名回退 uid）且同一 uid 只查询一次。`tests/octo-transport.test.ts` 新增 1 例用真实 HTTP 夹具断言 `GET /v1/bot/user/info` 的 Bot 鉴权、响应解析（含 `uid` 之外的 `name`/`avatar`）与查无此人时的 HTTP 失败。真机验收（2026-09-19/20，真实 transport + 真实缓存 + 真实 builder 直发 owner 私聊）：线上 `bot user-info` 解析出 `6e91530e… → 董振兴`（缓存据此产出名字）；四行对照探针卡（A Markdown、B `TextRun` weight、C 加粗+底色、D 无样式）由 owner 截图确认 A/B 加粗、D 不加粗，正文文本探针的两个星号被吃掉并显示为加粗；按 owner 选择去掉底色后，成品单题卡（message id 2101496571743866880）与两题组卡（2101496571903250432）以 `weight: Bolder` 的 `TextRun` 署名发出，未知 uid 的行按 uid 回退。截图见子区消息 2101495549050916864。

问答等待超时停回合与唤醒的验收证据（验收 11）：`tests/core-ask-card.test.ts` 新增 3 例，用 40 ms 的 `ask.timeoutMs` 驱动真实 CoreService、真实 SQLite 与夹具 worker——（1）单题超时后回合停住：该 worker 恰好收到一次 abort、任务终态为 interrupted、聊天零提示消息、卡片零改写（原 action id 保持可提交）；owner 的文本回复记录答案并在同一 session 唤醒一个正文含题面、`id=` 与还原选项标签（`答案：1. staging`）的新回合，卡片随后改写成「已回复」终态。（2）超时后的卡片提交同样作答并唤醒；同一 `(message_id, action_id, operator_uid)` 的重复投递不再作答、不再改写、不再唤醒。（3）整组超时只停一次回合、两行都保持待答且卡片零改写；随后一次部分提交只作答已填题并唤醒只含该题的回合、卡片以新 generation 重绘，剩余题再用文本答复照样唤醒，两题都不重复作答。把超时分支改回「过期 + 拒绝」（不调用 `stopTurnForQuestion`）后这 3 例全部失败（任务状态停在 running）；去掉无等待者时的 `wakeForAnswers` 后，第一例停在等待第二个 prompt、第二例在等待唤醒回合时超时，确认用例不是空转。`tests/config-launchd.test.ts` 覆盖配置面：缺省与 `ask: {}` 都得到 21 600 000（6 小时），显式值原样透传；1 799 999、0、负数、1.5、604 800 001、字符串与 null 都被拒（错误信息含 `ask.timeoutMs`），未知字段 `ask.waitMs` 与非对象值被拒；`config.example.json` 断言含 `{ "timeoutMs": 21600000 }` 且整体仍可解析。本轮验证走夹具路径（未做真机会话），另以本机私有配置 `~/.oco/config/config.json` 只读解析确认兼容：文件仍可解析且缺省得到 `ask.timeoutMs=21600000`；常驻服务尚未重启加载本实现。

Agent Octo 身份的验收证据（验收 27）：`tests/octo-cli.test.ts` 用 stub CLI 覆盖登录调用本身（`auth login --profile oco --bot-id <uid> --with-token`，token 走 stdin 且不出现在 argv）、凭据目录为 `<home>/.oco/octo-cli/<botUid>` 且权限 0700、登录子进程不再继承宿主的 `OCTO_BOT_TOKEN`、`env()` 只返回 allowlist 中的三个名字，以及退出码非零、响应非 JSON、robot_id 不匹配、可执行文件缺失四种失败下 `env()` 保持 undefined 且只记一条不含 token 的日志；`tests/omp-worker.test.ts` 用真实子进程断言 OMP worker 收到 `OCTO_CONFIG_DIR`/`OCTO_API_BASE_URL`/`OCTO_BOT_ID`，而宿主继承的与调用方误传的 `OCTO_BOT_TOKEN` 都不进入子进程；`tests/core-boundaries.test.ts` 断言 `start()` 以注册到的 bot uid 调用 `provision`，并在 `provision` 抛错时 channel 仍能完成一次任务。线上实测（合并到主检出并经守护进程重启加载实现后）：服务日志出现 `[octo-cli] bot … 的 CLI 凭据已就绪：/Users/dong/.oco/octo-cli/<botUid>`；任务 shell 里 `printenv` 显示 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 且 `OCTO_BOT_TOKEN` 不存在；`octo-cli auth status` 返回 `source=profile:oco` 与 `robot_id=28azkybli6q43216d14_bot`；经该身份调用未被 host tool 包装的端点成功：`octo-cli bot space-members --limit 2`（2 条）、`octo-cli api GET /v1/bot/groups/<本群>/incoming-webhooks`（`list: []`）、`octo-cli bot user-info --uid <owner uid>`（「董振兴」）。

`thread-delete` 瞬时失败重试的验收证据（验收 28）：线上诊断显示 `store_failed` 失败在时间上成簇出现且与服务进程无关——同一服务进程在失败 16 秒后以完全相同的 `short_id` 重试成功（14:32:19 → 14:32:35），独立 curl 进程也在同一窗口内对同一个存活子区命中过同一种 400（16:50:46，响应体为 `{"error":{"code":"err.server.bot_api.store_failed","details":{},"http_status":500,"message":"服务器内部错误。"},"msg":"服务器内部错误。","status":400}`），随后 octo-cli 对该子区删除成功（16:50:55）；对服务 DELETE 请求的字节级抓取显示除 `user-agent`/`accept-encoding` 等传输层默认头外不存在任何参数差异（无 body、无 query）。服务端源码 `pkg/httperr/respond.go` 的 `ResponseErrorL` 在兼容窗口内把传输状态固定为 400、真实状态放在正文 `error.http_status`，`pkg/errcode/bot_api.go` 中 `err.server.bot_api.store_failed` 声明为 `Internal: true` 的 500 错误，参数错误则是另一个 code（`err.server.bot_api.request_invalid`）。`tests/core-host-tools.test.ts` 覆盖三条路径："thread-delete retries a transient store failure on a fresh connection"（先注入 `store_failed`(400)+`code` 失败，断言两次 `thread-delete` 调用、第二次带 `freshConnection: true`、结果「已删除线程 topic-9」、线程从列表消失）、"thread-delete reports one bounded line when the retry fails too"（503 触发重试，重试再以 5xx+多行超长正文失败时工具错误被压成恰好 1 行 200 字符、线程保持存在）、"thread-delete does not retry a permanent server rejection"（403 只发生一次调用、错误原样返回）；`tests/octo-transport.test.ts` 的 "spends a connection of its own on a delete that asks for one" 用真实 HTTP 夹具记录每个管理请求的客户端端口，断言带 `freshConnection` 的 DELETE 落在一条此前未使用的 TCP 连接上——临时去掉该选项后同池连接被复用、该断言失败，证明它不是空断言；"keeps the server's machine error code for an internal store failure" 用夹具回放线上信封（线缆 400 + `err.server.bot_api.store_failed` + `http_status: 500`），断言错误文本保留该机器码——重试分类读取的正是这个字段。

未开通群自助开通的验收证据（验收 29）：`tests/core-unlisted-chat.test.ts` 覆盖 6 例——owner 在未开通群 @Bot 恰好收到一条通知（同一窗口内重复发送不再提示），且该消息不产生任务、`inbound`/`tasks`/待发 outbox 全部为空（同一组断言同时覆盖非 owner、未 @Bot 与 Bot 回声的静默）；owner 以「@Bot + 开通本群」回复后写入 `allowed_chats`（含 `granted_by`）并把 ID 交给写回端口，下一条消息立即作为任务执行、不再有通知，重启（同一状态目录的新进程）后仍然有效；非 owner、未 @Bot 与近似文本都不开通（近似文本只拿到通知）；子区按「本子区」提示并只开通自身（父群保持未开通），随后开通父群又让另一个从未单独开通的子区按继承规则可用；已开通群的任务照常执行且从不出现通知；写回失败时开通保留、确认消息注明仅状态盘生效。`tests/config-writeback.test.ts` 覆盖写回模块 3 例——追加保留其余字段、目录不残留临时文件且文件权限保持 0600/0640；缺失的 `allowedGroupIds` 被创建、并发两次追加都落地；不可解析的文件、非数组的 `allowedGroupIds` 与缺失文件都被拒绝且原文件不变。历史测试变异把当时命名的 `handleInbound` 新路径换回原来的静默 `return` 后，该文件 6 例中 5 例失败；把 `isAllowed` 的允许集合还原为只比较配置后 3 例开通用例失败，把写回端口的串行化去掉后并发用例失败，确认用例不是空转。该历史符号不构成现行入口兼容合同。

开通确认卡验收（验收 29）：`tests/core-unlisted-chat.test.ts`、`tests/store-chat-grants.test.ts` 与 `tests/bot-events.test.ts` 共 33 例通过，覆盖卡片授权、绑定与 owner 校验、重启恢复、事件确认、文本降级和配置写回失败。以 SQLite backup 创建现有状态盘的临时快照，使用应用迁移打开两次，原有表行数保持一致并建立授权操作表。服务路径冒烟中，owner 点击后写入目标群与授权人、完成一次卡片改写并确认事件，原消息未启动任务；实际卡片构建器的群、子区及结果帧在本地 Adaptive Cards 渲染器检查了桌面和窄屏布局。Octo 客户端实际渲染未在此次验收中观察。

真机实测（验收 29，合并到主检出并重启加载实现后，服务 PID 61734 → 17698）：owner 在未开通群 PaperLens Issue 群（`2c21fe9151f14d0cb6a456241c6388ec`）先后两次 @O-小爱 只收到一条「本群未开通。回复「@O-小爱 开通本群」即可开通本群。」（节流生效），随后回复「@O-小爱 开通本群」收到「已开通本群，后续 @O-小爱 即可正常使用。」；服务日志出现 `[allow] 已开通本群 2:2c21fe9151f14d0cb6a456241c6388ec`，状态盘 `allowed_chats` 出现对应行（`granted_by` 为该群发言的 owner，`granted_at` 与日志同秒），配置文件 `allowedGroupIds` 由一项变为两项（原允许群 + 该群）。

进度卡子代理事件的验收证据（验收 30）：`tests/omp-worker.test.ts` 的两例用真实子进程夹具断言翻译层——`set_subagent_subscription` 恰好发送一次且级别为 `progress`，`subagent_lifecycle`/`subagent_progress` 帧被翻译为 `subagent_start`/`subagent_update`（含 `lastIntent`、`currentTool`、`toolCount`）/`subagent_end`（`aborted` 归一为 `cancelled`），子代理自己的 prompt（`SECRET_CHILD_ASSIGNMENT`）与工具参数（`SECRET_CHILD_ARGS`）不出现在事件里；夹具拒绝订阅时 worker 仍完成该回合（正文与父级 tool 事件照常），只是不产生子代理事件。`tests/execution-card.test.ts` 覆盖卡片侧：运行中的子代理作为独立区块的丰富行且不进入执行轨迹、终态子代理落入可展开的历史带并给出隐藏计数、同一 id 的终态不被后续进度或重复 `started` 复活、run 结束时仍无终态的子代理按回合终态收尾、子代理行只保留整体状态与工具活动（断言卡片与纯文本都不含该子代理的时钟与耗时，`glob`、`2 次调用` 正常显示）、持久载荷重放后子代理保留 id/agent/状态并参与补发渲染、没有 ToggleVisibility 时终态行以可见紧凑行呈现而不被折叠隐藏。`tests/preview-card.acceptance.ts` 的预览夹具改用真实生命周期事件（断言 `reviewer/failed`、`verifier/running`、`scout/completed`、`cleanup/cancelled`、`analyst/running`），并向真实 Octo 会话发送综合与长任务两张卡片，两者均返回 message ID（本 worktree 通过 checkout 等价命令 `set -a && . ./.envrc && set +a && bun run test:preview-card` 发送）。真机实测：本仓库 worker 工厂以安装的 OMP 18.2.1 直接驱动（不经过常驻服务），prompt 要求启动 scout 子代理统计 `src` 下 `.ts` 文件数，事件流出现 `tool_start(task)` → `subagent_start(scout, running)` → 多条 `subagent_update`（`lastIntent` 如 `Globbing for .d.ts declaration files`、`toolCount` 递增）→ `subagent_end(completed)`，快照得到 1 个子代理（`scout`、13.0s、`toolCount=5`），卡片渲染出 `子代理 · 1` 区块与已完成的行；同一次运行里 `task` 工具行只作为普通轨迹行出现，子代理区块的状态与计数全部来自上述事件。

`/switch` 模型切换的验收证据（验收 32）：`tests/model-switch.test.ts` 覆盖纯逻辑 3 例——存储值带级别的拼装（`provider/id:level`、无级别时不带后缀）、错误行里选择器的有界化（空白折叠、控制字符去掉、超过 60 字符截断）、列表渲染（按 provider 分组、未知当前模型、空目录、超过 80 个模型时截断并附总数）。`tests/core-switch-command.test.ts` 用真实 CoreService、真实 SQLite 与夹具 worker 覆盖 10 例——空 `/switch` 的回复逐字等于期望的当前模型与分组列表、探测 worker 恰好创建一次并被停止、worker ledger 无遗留行、临时会话目录无残留；`/switch fixture/fast` 的回执、`chat_models` 落盘、下一回合的 `WorkerOptions.model` 为该值、`/status` 的 `model=` 行；`/switch @fast` 存角色解析出的 `other/fast:max`、`:high` 后缀存进 `fixture/fast:high` 并传给下一回合；同一状态目录重启后新进程仍以该模型启动；OMP 拒绝的选择器回单行错误且不落盘、后续 worker 不带 `--model`；含空白的选择器直接回未找到且一个 worker 都不创建；`/switch default` 清除后恢复无 `--model`；运行中的回合收到 `/switch` 时由该回合的活会话执行（断言恰好一次创建、该 worker 收到分发）、回执标注「运行中会话已同步切换」、下一回合用新模型；运行中回合里的空 `/switch` 由活会话作答（断言没有探测 worker）；探测失败回 `无法从 OMP 读取模型信息，请稍后重试。`且不改变状态；空目录的列表回答为 `OMP 当前没有可用模型。`、切换回答为未找到。`tests/omp-worker.test.ts` 用真实子进程夹具覆盖 worker 侧 5 例——`--model`（含无级别与带级别写法）出现在子进程 argv 且 `models()` 报告对应级别、`--model` 与 `--resume` 并存、`models()` 解析 `get_state` 的模型与级别及 `get_available_models` 列表（跳过缺 id 的畸形条目、空 name 不报告）、空列表原样返回；`switchModel` 把 `/switch <选择器>` 作为 prompt 帧发出并按 `config_update` 判定成功（角色别名 `@fast` 解析为 `other/fast:max`、模型选择器保持会话级别、未知选择器回 `ok:false` 且会话不变、三次分发的文本逐字记录在夹具日志里）；`switchModel` 在流式回合中同样可用且该回合继续跑完并照常接收 steering。把 `config_update` 的截获去掉后 worker 侧 2 例失败，把 `/switch` 的活会话分支去掉（一律走探测）后服务侧 2 例失败，把 `--model` 的 argv 追加去掉后 worker 侧 2 例失败，确认用例不是空转。真机实测（本仓库 worker 工厂直接驱动安装的 OMP 18.2.3，不经过常驻服务）：fresh worker 的 `models()` 报告当前 `octo-openai/deepseek-v4.1-flash`（级别 `max`）与 49 个可用模型；`switchModel("@tiny")` 返回 `{ok:true, model:"openai-codex/gpt-5.6-luna", level:"max"}`（与该角色的配置值一致）且随后 `models()` 报告同一模型；`switchModel("definitely-not-a-model")` 返回 `{ok:false}` 且会话不变；在一个真实 `sleep 20` 回合并发流式期间调用 `switchModel("@smol")` 返回 `{ok:true, model:"octo-openai/deepseek-v4.1-flash", level:"max"}`，会话立即切换而该回合照常跑完；用存储形态的 `--model openai-codex/gpt-5.6-luna:max` 启动 worker，`models()` 报告该模型与级别 `max`，证明落盘值在下次启动时原样生效。

子区回传的验收证据（验收 33）：CoreService 与真实 SQLite 测试覆盖默认/off 静默、task 派发四终态、真实宿主合并成功事件、失败修复隔离、策略创建/修改/重启、shared、pending ask、追加模式与异步交付门控。稳定操作重放不再次执行合并；事件 pending / intent / delivered 区分未派发与未知接受。相关 6 文件 87 项通过；完整类型检查与 1050 项测试通过。501 条任务的一致性状态库副本迁移及二次打开保持旧数据，79 个子区默认 merge；真实 Git/Worktrunk smoke 的默认成功回传一次，off、真实冲突及修复失败零回传。详情与 host-first 集成接口见 `2026-09-17-thread-completion-notify-design.md` 的验收记录。


会话目录布局的验收证据（验收 34）：`tests/session-dir.test.ts` 覆盖 4 例——同一个 cwd 的项目名稳定、同名不同路径的项目名不同（`app-<8 位十六进制>`，非 ASCII basename 回退 `project-<摘要>`）、`ensureProjectSessionDir` 以 0700 创建并返回该项目目录、`sessionsRoot` 缺省回退状态目录而显式配置优先，以及用真实 CoreService 与夹具 worker 断言 `/new` 与随后的普通任务两次创建的会话文件都落在 `<sessionsDir>/<项目名>/` 下。`tests/config-launchd.test.ts` 断言 `parseConfig` 的缺省 `sessionsDir` 为 `<home>/.oco/sessions`、显式值按 `~` 解析后进入 `toCoreConfig`；`tests/project-bindings.test.ts` 的既有移动/租约用例改为从状态盘读取绑定的 `session_file`，不再假设扁平布局。
会话创建回执模型提示的验收证据（验收 37）：`tests/core-new.test.ts` 新增 5 例（真实 CoreService、真实 SQLite 与夹具 worker）——（1）无聊天模型时 `/new` 的回执逐字为 `已创建会话 <session id>，cwd=<目录>，模型=fixture/default`，即 OMP 实际默认而非字面 `default`（断言与 `status()` 读出的 session id、cwd 一致）；（2）先 `/switch fixture/fast:high` 再 `/new`，回执为 `…，模型=fixture/fast:high`，并断言该候选 worker 的 `WorkerOptions.model` 同为 `fixture/fast:high`（回执就是会话实际生效配置）；（3）`/reset` 与 `/new` 共用创建路径，回执同式含模型；（4）模型查询失败（夹具 `failModelsWith`）时以不含模型的 `已创建会话 <id>，cwd=<目录>` 送达、会话照常建成（不使 `/new` 失败）；（5）OMP 未报告当前模型时同样以不含模型的回执送达——两例共同证明不编造模型值。把 `createdSessionModel` 的取值从回执中去掉后，（1）（2）（3）例全部失败；把查询失败时的降级去掉（让异常传播）后，（4）例失败，确认用例不是空转。
typing 即时反馈的验收证据（验收 39）：`tests/core-typing-lease.test.ts` 用真实 CoreService、真实 SQLite 与夹具 transport/worker 覆盖 8 例，断言以夹具记录的 typing 调用跨组件相对顺序与精确计数为准——（1）typing 在 worker 创建之前发出（接受即亮，不等 worker 启动）；（2）附件下载在途期间 typing 先于下载调用，且按 3 秒间隔恰好续期一次（覆盖此前最长 120 秒无反馈的窗口）；（3）附件数量超预算被拒后停止续期、无租约泄漏；（4）`/status`、不支持的消息类型、成员命令提示与 `/queue` 空参数全程零 typing、不建任务；（5）消息并入运行中回合（steer）后消息租约释放、环不重启，仍只有一条环按 3 秒恰好续期一次，任务完成后停止；（6）queue 模式下运行任务与排队任务同属一条环、排队等待期间保持亮起、两个任务终态后停止；（7）排队中 `/cancel` 后运行任务租约在 finally 释放、排队任务由 tick 任务行状态兜底回收，续期在两个间隔内停止；（8）intercepting 命令同消息重投（claim 去重早退）后无租约残留。夹具扩展：`tests/fixtures/core-fakes.ts` 的 `FakeTransport.sendTyping` 记录 `typingCalls`，`createCoreHarness` 支持 `attachments` 配置。
outbox 有界失败与队列放行的验收证据（验收 9）：`tests/core-outbox.test.ts` 用真实 CoreService、真实 SQLite 与夹具 transport 覆盖 3 例——（1）一条正文只失败一次时按稳定 client ID 自行重试成功，同聊天后续回复按序投递，全程不调用重连；（2）transport 对某条正文持续回 500 时，该行在有上限退避（1s 起、翻倍、上限 30s、向下抖动）重试 6 次后标记 `dead`，`last_error` 保留真实的 `HTTP 500` 文本、`outbox`/`unknown` 计数归零，且其后投递的第二条消息在被放弃后立即送达——把预算判定去掉后同一用例在 5 分钟窗口内记录到 303 次尝试、第二条消息始终不投递（原始问题的复现）；（3）第一个进程在 transport 离线时留下的 pending 行由重启后的新进程从队首排空（零重跑、正文恰好投递一次）。`tests/core-thread-command.test.ts` 的 "a sub-thread's /thread merge is recognised by its first word and carries the trailing note" 覆盖首词识别：`/thread merge, 已经完成的话，merge 一下` 进入合并路径、补充说明以「owner 补充说明：」进入指令正文，而 `/thread merge-fix` 仍按建区意图处理（子区内只回既有提示、不产生任务）。
outbox 有界失败与队列放行的验收证据（验收 9）：`tests/core-outbox.test.ts` 用真实 CoreService、真实 SQLite 与夹具 transport 覆盖 3 例——（1）一条正文只失败一次时按稳定 client ID 自行重试成功，同聊天后续回复按序投递，全程不调用重连；（2）transport 对某条正文持续回 500 时，该行在有上限退避（1s 起、翻倍、上限 30s、向下抖动）重试 6 次后标记 `dead`，`last_error` 保留真实的 `HTTP 500` 文本、`outbox`/`unknown` 计数归零，且其后投递的第二条消息在被放弃后立即送达——把预算判定去掉后同一用例在 5 分钟窗口内记录到 303 次尝试、第二条消息始终不投递（原始问题的复现）；（3）第一个进程在 transport 离线时留下的 pending 行由重启后的新进程从队首排空（零重跑、正文恰好投递一次）。`tests/core-thread-command.test.ts` 的 "a sub-thread's /thread merge is recognised by its first word and carries the trailing note" 覆盖首词识别：`/thread merge, 已经完成的话，merge 一下` 进入合并路径、补充说明以「owner 补充说明：」进入指令正文，而 `/thread merge-fix` 仍按建区意图处理（子区内只回既有提示、不产生任务）。
群历史附件元数据、「这个」关联与引用媒体的验收证据（验收 38 及历史规格验收 17/18/19）：取证基于线上事故（2026-09-20，成员发 .xlsx 后 @Bot 说「这个」，bot 回答未收到附件）：经 bot 身份 `message sync` 回读真实 payload，文件消息（type 8，name/extension/size/绝对 CDN 直链齐全）与显式引用消息（`reply.payload` 内联同一份文件描述）均确认服务端数据完整，丢失发生在本 channel 的归一化与历史翻译层；对该 URL 的 Range 探针返回 206、`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` 与 `PK\x03\x04` 魔数，确认其为真实二进制文件而非预览/特殊引用形态。`tests/octo-content.test.ts` 的 "reads a quoted file message through the real client payload shape" 按取证形状（type 1 正文 + `reply.payload` 内联完整 type 8）断言引用文件经真实加密 wire 归一化后成为可下载附件、type 9 别名同表、相对 preview 路径引用整条不成立；"keeps text plus absolute attachment references and never downloads" 断言 sync 回填保留附件元数据、preview 文件不进历史且零下载。`tests/group-history.test.ts` 覆盖媒体消息在缓冲中的存活与深拷贝、`[文件 名称 (大小)]` 标记与 caption 追加渲染、`attachmentCandidates` 的发送者/seq/10 分钟窗过滤。`tests/core-attachments.test.ts` 覆盖——引用 .xlsx 恰好一次下载、`[引用 <发送者>]` 标题与绝对路径进正文、落盘字节可开卷（合法 zip 容器含 `xl/workbook.xml`）；群成员 @Bot 直接发文件过权限门恰好一次下载；「这个」五例：唯一候选取回（prompt 含来源 `[系统提示]` 与附件绝对路径、字节一致）、歧义列名不取件、他人文件不关联、11 分钟超窗不关联、`groupHistory` 缺失不关联。`tests/core-group-history.test.ts` 的 live 图片用例更新为「caption + 附件标记 + 覆盖」，断言传者不同不触发下载。把 `src/` 回退到改动前，以上用例除引用文件归一化 pin（该传输行为取证后确认此前已正确）外全部失败。真实客户端的在线验证（线上 bot 实际读到那条「这个」所指的 xlsx）依赖常驻服务的部署重启，本次未执行；本地真实链路另行以隔离 smoke 验证（真实归一化 → 真实 HTTP 下载 → 组装 → 用 zip 读取器开卷 .xlsx）。

原生 `/autoresearch` 通知可见性与路由的验收证据（验收 40）：`tests/fixtures/autoresearch-rpc-worker.ts` 是上游 autoresearch 扩展在 RPC 边界的确定性替身（notify 帧、脏工作区拒绝、goal 的四个实验工具与 `willContinue` 续跑全部字面固定，离线无模型、无网络）。`tests/core-presentation.test.ts` 新增 4 例，用真实 CoreService、真实 SQLite 与夹具 transport/worker 覆盖服务侧渲染——（1）notice-only 命令结局（脏工作区 error 拒绝）作为该聊天唯一一条消息投递，正文含通知全文且带级别内容（非裸文本），任务行 `completed` 且 `resultText` 为空、零进度卡；（2）两段助手正文与同一上游 id 重复到达的 info notice 交错时消息彼此独立、notice 正文只出现一次、任务行 `resultText` 恰好是两段助手正文；（3）离线时 notice 留在持久 outbox，重启后新进程排空恰好投递一次，且新进程的 `prompts` 计数为零（不重跑已结算任务）；（4）`/interrupt` 之后到达的 notice 仍单独投递，任务终态保持 `interrupted`。该分支未实现时 4 例全部按预期失败：1/2/3 例通知零投递且无持久出站行，第 4 例被信息性通知改写为 `completed`。worker 翻译层（上游 `extension_ui_request` notify 帧到 `WorkerNoticeEvent` 的字段净化、级别归一与活动 prompt 过滤）由 `tests/omp-worker.test.ts` 用真实子进程夹具覆盖。能力复测：`tests/benchmarks/autoresearch-routing.ts` 的固定路由 5 例（owner DM goal 首行与整条原样抵达、已开通群 goal 首行不被群历史块改写、成员 `clear --reset-tree` 被权限门拒绝且零 prompt 零 task 行、owner 命令重投只执行一次、未开通群零副作用拒绝）与 `tests/benchmarks/autoresearch-worker.ts` 的固定 worker 5 例（enable/off/脏工作区三类 notify 在 worker 事件中可见、goal 正文与 `willContinue` 续跑正文均可见、四个实验工具的 start/end 帧按序可见且终态成功）。

本地命令群说明提交的验收证据（验收 41）：`tests/core-group-context.test.ts` 覆盖带群说明的本地命令在没有 prepared receipt 时正常完成、下一个模型回合仍按 `initial` 注入并提交，以及真实生成回合缺回执时仍失败。该问题先由隔离临时仓库中的原生 OMP 复现，再由回归测试复现；修复不伪造回执，也不放宽模型回合的校验。`tests/omp-worker.test.ts` 覆盖两条 `agentInvoked:false` 路径恰好发出一次 `local_command`、结算后的重放无效、旧请求 ID 或非字符串 ID 不能结束新回合，以及缺省 ID 的旧协议兼容。原生 OMP 隔离验证使用临时 HOME、会话目录和 Git 仓库：无参数开启、停止 worker 后恢复同一会话并切换关闭、`clear --keep-tree`、群内本地命令正常完成均通过；目标回合使用本地 Responses API 替身，验证真实模型 HTTP 路径携带待注入群说明与四个实验工具，并将返回正文投递到夹具 transport。此验证不连接线上 Bot，也不评价真实模型的优化质量。

原生实验工具的隔离验收：本地 Responses API 替身依次请求实际 OMP 的 `init_experiment`、`run_experiment`、`log_experiment` 和 `update_notes`，在临时仓库执行固定输出 `METRIC score=5` 的 shell 基准。后续真实 HTTP 请求携带实际工具结果，确认基线日志 `Logged run #1: keep`、shell 指标输出与 notes 更新回执；最终正文和 OMP 自动安排的下一回合正常结束。另核验 `clear --keep-tree` 保留普通项目文件，但仍删除 `autoresearch.sh`，随后恢复临时基准再进行工具链验收。

子区归档与合并后自动归档的验收证据（验收 42/43）：`tests/core-host-tools.test.ts` 用真实 CoreService、真实 SQLite 与夹具 transport/worker 覆盖 6 例——「thread-archive schedules one archive that runs after the turn's reply is delivered」断言子区里两次调用只排定一次、归档恰好发生一次且夹具记录的调用顺序里本回合回复已经送出（`archiveOrder[0].sentBefore` 含回复文本）；「a default-pipeline merge schedules the thread's archive beside the merge result」断言默认 pipeline 合并后工具结果含 `"archive":"scheduled"`、归档目标为该子区且在回复送达后执行；「a transient archive failure retries five times five seconds apart and ends as one bounded notice」在假定时器下断言 5 秒未到不重试、过 5 秒重试一次、总计 5 次尝试后停止，聊天收到唯一一条 `归档失败：` 单行说明；「thread-archive does not retry a permanent failure and reports one bounded line」断言 403 只尝试一次且推进一分钟不重试、群会话显式 `short_id` 同样可用；「a new message in the same chat cancels the scheduled archive」断言回合末窗口内的新入站清除排定（后继任务开始即证明收尾已结束、零归档调用）；「a pending archive is dropped when the channel stops before it runs」断言停在重试等待中时关停立即结束等待、放弃归档且不发失败说明。服务端契约与部署状态：归档端点已在上游 octo-server `main` 合并（`POST /v1/bot/groups/:group_no/threads/:short_id/archive`，守卫 `protectAIContainerMutation`，服务端 `ArchiveThread` 幂等且任何消息都会重新激活归档子区）；2026-09-20 用本 bot 身份（octo-cli，REST 探针）对部署服务器 `im.deepminer.com.cn` 实测该路径仍返回纯 gin 404（`404 page not found`，非 bot API 错误信封），即该实例尚未部署该端点，真机端到端验收（建一次性子区 → `/thread merge` → 自动归档 → 清理）待部署后补做；同次探针按「建一次性子区 → 归档（404）→ 删除」完成并清理（群内无残留）。

研究只读呈现与完整记录导出的验收证据（验收 44/45）：

- **服务回归**：`tests/core-research.test.ts` 用真实 CoreService、真实 SQLite 与夹具 transport/worker 覆盖 9 例：研究事件开卡并原地更新、终态保留及未编号行；steer 滚卡继承与新任务隔离；运行中立即导出且不新建 prompt；messageId 重投去重；成员和其它聊天不能取得报告；无记录时不启动模型；重启后导出；spool 字节不可变；报告目录被同名普通文件占用时，任务仍完成、正文照常投递。
- **数据与展示**：`tests/omp-worker.test.ts`、`tests/execution-card.test.ts` 覆盖原生工具调用配对、权威 init/log 报告、非法或陈旧结果拒绝、报告不进入执行快照、研究投影整体替换及恢复、终态显示、独立折叠和节点预算。全套 `bun test` 为 693 pass / 0 fail（54 个文件），`bun run typecheck` 通过。`tests/preview-card.acceptance.ts` 在无 token 环境下为 7 pass / 3 skip。
- **原生 OMP**：本地 Responses 替身驱动实际 OMP 执行 init → 3 轮 run/log → final，结果为 10 keep / 6 discard / 8 keep；baseline=10、best=8，同一进度卡 3 次投递/更新，`/autoresearch-history` 文件导出成功。
- **浏览器渲染**：真实 builder 输出在本地 AdaptiveCards renderer 中以 340/440 像素宽度验证独立展开/收起、主卡折叠保留研究摘要、终态、无 Toggle、TextBlock-only 和零基线。原生三轮输出卡为 104 节点；18 节点的 TextBlock-only 卡仍保留摘要、最近一条记录与省略数。
- **报告边界**：10000 行输入生成 1,047,987 字节报告，保留最新完整 UTF-8 行；超过 10000 行整拒，快照净化只访问末 32 条；6 种 bot/聊天键互相隔离，目录 0700、文件 0600；并发写有序、不可变 spool、重启读回和超大报告拒绝均通过运行验证。真实 Markdown 解析器确认导出自由文本不会生成 HTML 标签。
- **线上预览**：真实 CLI `bun run src/cli.ts test --card` 加载主检出的 `.envrc` 后为 10 pass / 0 fail，综合、长任务、研究三类预览均成功投递到已核对的持久 owner 私聊；研究卡单项发卡验收也通过。配置目标直发路径不注册 WebSocket，常驻服务保持运行。

验证范围：线上发卡成功依据 API 回执；本机 Octo 浏览器未登录，未进行真实客户端像素验收。本地 renderer 和 API 回执不能代替 Octo 各客户端的实际呈现检查。

`/retry` 的验收证据（验收 46）：`tests/omp-worker.test.ts` 通过真实 RPC 子进程覆盖原生命令回执、等待重试生成结束、无可重试回合的本地完成以及运行中 `/switch` 输出隔离；`tests/omp-context-extension.test.ts` 覆盖跳过 agent-start 的续回合在每次模型请求中携带 pending 共享说明并写入匹配 receipt、普通回合不重复注入、子会话隔离。隔离 smoke 使用实际 OMP 18.2.6、CoreService、SQLite 与夹具 Octo transport：群聊空 `/retry` 返回 `OMP：Nothing to retry.`，任务 completed，重复消息零重复回执；本地 Responses API 第一次返回确定性 400，第二次返回 SSE 正文，原任务 failed 后 `/retry` 在同一 session 恢复为 completed，恰好两次模型 HTTP 请求都携带群说明，回执与正文分别投递。本地模型替身只验证协议、恢复和上下文链路，不调用真实模型或线上 Bot。

配置卡片的验收证据（验收 47）：`tests/core-command-card.test.ts` 用真实 CoreService、真实 SQLite、夹具 worker 与可点击的假 transport 覆盖 14 例——`/switch` 卡片的模型选择落 `chat_models`、下一个 worker 带 `--model`，且不产生生成回合；`/mode` 卡片只认 owner（成员点击静默且不消耗卡片），随后的文本命令与重复投递都不会被旧卡覆盖；`default` 选择清空聊天模型，终态卡承载该命令的逐字回执且帧内无输入、无动作；回合运行中的点击走该回合的活会话（全程只有一个 worker，`switchModel` 分发与 store 同步）；同聊天的待答问题与任务计数不受配置点击影响，问答路径随后照常工作；非 owner、串消息、串聊天、未发放 action id 与未发放取值的点击一律不改变状态，其中只有 owner 点击未知卡片时得到一段有界重开指引；部署不支持卡片、帧被拒、发送抛错三条路径都逐字给出原文本回执；旧菜单被新命令取代后，晚到的旧卡不再可点击；跨进程重启后旧卡作废而新进程的新卡照常；新菜单取代同聊天的同类旧菜单；在途的目录查询被更新的文本选择取代后不发卡（0 张）且该选择保持生效。另有二个单元级用例直接驱动菜单会话（应用写成真实的 `chat_modes` 落库）：发送回执未绑定前的点击返回重试、绑定后才应用并改写为无控件终态；TTL 过期、伪造 action id、非 owner 点击与 200 张上限（最旧先失效、最新仍可用）分别断言。`tests/model-switch.test.ts` 增加 2 例覆盖卡片的取值构造：只发放能作选择器的 `provider/id`（空 provider、含空白与重复条目被丢弃）且上限与文本目录一致（`default` + 80 条），卡片的当前值行与文本回退首行同源。

配置卡片独立验收（2026-09-21）：本地 AdaptiveCards 3.0.4 浏览器渲染实际 builder 输出，340px 与 440px 宽度均无 SDK 校验错误；包含 80 个模型加默认选项的帧为 89 节点。另将实际 CoreService 生成的卡片接入本地浏览器，选择 `fixture/fast`、`queue` 并分别点击「应用」，提交经 Bot 事件消费路径后从 SQLite 回读得到 `model=fixture/fast`、`mode=queue`，两次配置均未产生生成回合。最终 `bun run typecheck` 与 `bun test` 通过（746 项），固定配置入口基准为 6/6，文本回退为 6/6。该验收使用本地 transport/worker 夹具；真实 Octo 客户端呈现与线上点击尚未验收，未部署或重启常驻服务。

`/switch` provider 与 100 模型验收（2026-11-03，验收 32/47）：

- TDD 首次新增 7 项按预期失败：精确 provider 被解释为切换选择器，全局列表仅展示 80 个。实现后专项模型/配置卡/RPC 回归为 124 pass、0 fail；`bun run typecheck` 通过；全量 `bun test` 为 1158 pass、0 fail（83 文件）。
- 全局及 provider 各 100/101 模型场景通过真实 CoreService 构造帧；scope、总数、展示数、超限指引和第 100 项 selector 均正确。owner/重复点击、旧代次、慢查询取代、运行中只读、pending ask、文本回退及 default 同名 provider 优先级均有行为回归。
- 本地 ego-browser 使用真实 builder 输出和 AdaptiveCards SDK 渲染四个场景，340px/440px 共八次呈现，SDK 校验错误及横向溢出均为 0。四次选择 `fixture/model-099` 后点击「应用」，提交经实际 Bot 事件消费路径落 SQLite，模型均为该值、终态编辑各一次、生成回合均为 0。
- 固定上游 octo-web `a9f5923d89fcd7a6ebbdab56fb755a1c4d9f0ce7` 的原始 `validateCardForOcto.ts` 及依赖对四帧均返回 `ok:true`，包含 default 的 101 个选项通过 ChoiceSet 节点门禁。实际 `cardFrameEnvelope`（含 plain、profile、card_version、card_seq）为 8,372–8,604 UTF-8 字节，host 统计 110 节点、深度 4，通过 200/16/28,672 的保守预算，也低于服务端 512 KiB 限制。
- 真实安装 OMP 的 controller/RPC 烟测完成全局及 `anthropic` provider 两次只读查询，筛选为 19 个模型，聊天模型保持 null；两个原生 worker 均确认退出、ledger 为空、两个临时 session 目录均已删除，不调用模型生成。
- 验证表面为本地 SDK 浏览器、固定上游校验器与真实 OMP；真实 Octo 客户端呈现及线上点击未验收。本次未向真实群发送验收卡、未注册竞争 bot 连接、未部署或重启常驻服务。
- `/switch` 与 `/thread model` 共用 `chatModel` 的实际状态/下一次启动策略展示，目录筛选不参与模型继承决策。`handleSwitch(message, argument)`、`applySwitch(target, requested)` 公共接口不变，原生成功后的写入/回执由私有 `switchReceipt` 供文本选择和卡片点击复用；`resetChatModel` 只清聊天覆盖并重新解析继承策略。`CommandCardView` 同时保留 `nextModel` 与 `catalogSummary`；`ConfigMenuPort`、store、WorkerOptions 及调度接口沿用各自既有契约。
- 与主群子区默认的联合验收：模型/菜单/存储专项 55 项通过，typecheck 通过，全量 1173 项通过（85 文件）。真实 CoreService 与 SQLite 烟测中，provider 筛选同时展示继承策略与 100/101 模型计数，第 100 个选项 `fixture/m98` 正确持久化；default 清除显式覆盖后，父群策略保持 `fixture/fast:high`，下一 worker 按此模型启动。合并后的实际帧为 6,964 UTF-8 字节、110 节点、深度 4；本地 SDK 在 340/440px 均无校验错误或横向溢出，提交值与选项一致。该联合验收仍为本地表面，不代表真实 Octo 客户端验收。


生命周期通知验收（2026-11-02，验收 49–53）：`tests/core-lifecycle-notices.test.ts` 与 `tests/cli-lifecycle.test.ts` 共 19 项通过，覆盖配置 owner 与持久 owner、未认领跳过、启动准备失败、重复启停及重连、在途启动通知与停机竞态、停机前封闭入站且早于 worker/transport 收尾、拒绝及忽略取消的发送各自受 2 秒预算约束，以及 outbox 队头阻塞不影响通知。真实 CLI 的本地时间使用独立 `Asia/Kathmandu` 环境验收 `+05:45` 偏移，SIGTERM/SIGINT 时通知发送期间 WebSocket 保持开放，重复实例与 SIGKILL 后重启各按既定规则运行。受影响的 10 个测试文件共 128 项通过；`bun run typecheck` 与完整 `bun test` 通过（66 个文件、867 项、4826 个断言），普通业务回复的测试按目标聊天隔离，生命周期消息在 transport 夹具中保留实际记录。

同次离线真实进程烟测 `bun tests/benchmarks/restart.ts` 的优雅重启、SIGKILL 两种场景均通过：2 个原任务续接、2 个排队任务继续，会话重叠、重复副作用及任务打断通报均为 0。此次通知实现不改变任务恢复和持久 outbox 语义。

当前 Super 部署已经通过 `oco restart` 加载实现，随后完成一次新版本到新版本的正常重启验收（PID 30637 → 30740）。在已登录的 Octo Web「O-小爱」owner 私聊中实际观察到：`2026-11-02T15:43:24+08:00` 的旧进程正在停止通知，随后是同秒的新进程已启动通知；两条正文的 PID 与 Super 实际进程一致。新进程持续 Healthy，并保持已建立的 HTTPS 连接。真实私聊 `/status` 返回原会话、`queued=0`、`outbox=0`；随后同一聊天的文件操作经实际 OMP 完成，31 字节内容及末尾换行精确核对，助手完成回复在客户端可见，任务行记为 completed，验收临时文件已删除。

## 原生持续目标验收（2026-11-02）

`/goal` 的结构化请求、精确会话/目标归属、控制接受和生成接受、持续授权及停止意图已接通。目标运行保持 worker、host tools、ask、输出与目录 lease，直到原生 `session_settled`；完成或删除后清除当前投影，保留任务审计。显式恢复核对原生状态，不把过期缓存当作目标事实；控制 worker 与执行 worker 都应用本聊天的模型设置。完整契约及实现细节见 `docs/specs/2026-11-02-goal-command-design.md`。

最终 `bun run typecheck` 与 `bun test` 通过：909 项、68 个文件、5,169 个断言。`bun tests/benchmarks/goal.ts` 经真实 OMP 18.15.0、真实 CLI、隔离 profile/session、有限 localhost 模型及本地 Octo fixture 完成 23 次模型请求，覆盖两轮续跑、状态、暂停恢复、删除、无进展、预算限制、禁用配置、切换模型后原 provider 不可用，以及 graceful/SIGKILL 后同会话同目标恢复，重复创建为 0。既有真实进程重启基准两种场景也通过，原任务和队列均恢复，会话重叠与重复副作用为 0。

真实状态库只读一致性副本连续迁移两次，原有 17 张表、5,282 行逐表旧列哈希一致，新增两张目标表；私有副本与探针脚本已移除。本次目标实现仅在隔离工作区验收，未部署、未重启常驻服务、未合并；本地模型替身验证协议与执行路径，不证明真实模型质量。
