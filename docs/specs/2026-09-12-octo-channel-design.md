# OMP Octo 个人远程助手设计

日期：2026-09-12
修订日期：2026-09-16
状态：产品设计已确认

## 背景与目标

通过 Octo 私聊和指定群聊操作本机 OMP。使用独立于终端的持久会话，保持用户当前全自动执行习惯。channel 是常驻消息服务，OMP 是通过 stdio RPC 驱动的子进程；不依赖模型 API auth-gateway。

## 范围

包含个人单 Bot、单服务实例、文本指令与回复、执行状态、新建会话、指定目录、取消、会话恢复、消息去重和连接重建。支持私聊、指定群及 CommunityTopic 线程（channelType=5）内明确 @Bot 的指令；支持群/线程共享说明、线程默认继承父群工作目录、群/线程对话历史的增量上下文注入（详细设计见 `docs/specs/2026-09-16-group-history-context-design.md`）、常见入站消息类型的内容提取与附件接收（详细设计见 `docs/specs/2026-09-16-inbound-message-content-design.md`）、出站文件和持久化问答。

不包含终端会话接管、多用户服务、通用网络 Agent gateway、文档评论任务、服务端通用 bot_task、群历史归档与检索、出站附件之外的通用文件托管和交互 callback 系统。channel 仅提供由 OMP host tool 明确发起的出站图片/文件发送，以及由能力 profile 明确启用的 Octo raw type17 `octo/v1` AdaptiveCard 1.5 进度展示和 `octo/v2` 交互问答卡片；交互卡片的上行只使用该 Bot 自己的事件队列（`/v1/bot/events` 轮询与 ack），不注册回调地址、不实现通用 callback 或审批流。收到的入站消息按已确认的类型表提取内容，贴纸、系统与 CMD 等没有内容的类型明确告知，不作为成功执行。范围以个人 IM 交互为准，不继承参考适配器的其他任务入口。

## 已知约束与资料

- 本机当前 OMP v18.1.21 提供 `--mode rpc`、`--cwd`、`--resume`、`--approval-mode yolo`、`--extension` 及 `session.compacting` 生命周期能力；本地兼容解码器按 v18.1.18 保持 RPC 兼容。RPC 提供 prompt、steer、abort、状态和会话事件；启动 ready 不代表模型调用可用。线程隔离工作区另外使用 `omp worktree add`（v18.2.1 实测，clone-first 由 `worktree.clone` 设置决定）。
- OMP SDK 可嵌入，但本设计选择 RPC，保持进程边界。
- Octo 运行参考：[openclaw-channel-octo](https://github.com/Mininglamp-OSS/openclaw-channel-octo)。其 README 确认 REST 注册、WebSocket 实时收发和自动重连。
- 账户参考：[create-openclaw-octo](https://github.com/Mininglamp-OSS/octo-adapters/tree/main/create-openclaw-octo)。使用 BotFather 创建的 User Bot；`app_` App Bot 仅支持私聊，不能满足群聊验收。
- octo-adapters 当前源码树主要提供安装 CLI；协议运行时代码在独立 channel 仓库。实施时以运行时代码核实报文、消息 ID、mention 元数据和发送接口，不凭顶层 README 猜测端点。
- RPC、SDK 和审批机制参考 OMP 自带 docs/rpc.md、docs/sdk.md、docs/approval-mode.md。
- 上游 `Mininglamp-OSS/octo-server` 的 `modules/bot_api/typing.go` 表明 90 秒后至 180 秒 TTL 窗口可能抑制 CMD typing 投递，即使 HTTP 返回 200；channel 只保证执行期间每 3 秒续期，不通过客户端绕行，也不承诺绕过服务端节流或离线时连续可见。真实部署尚未完成超过 90 秒的视觉验证。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 身份 | 唯一 owner 用户 ID、允许群 ID 列表 | 只有用户本人可以驱动本机 |
| 执行 | yolo；channel 不添加逐次审批 | 保持全自动习惯 |
| 会话 | 每个聊天一个当前会话，历史保留 | 隔离聊天上下文 |
| 目录 | `/new [目录]`、`/reset [目录]`；省略目录按目标默认目录解析（DM/群沿用当前绑定，首次 `~/tmp`） | 每次 fresh 会话都有确定工作目标 |
| 线程目录 | CommunityTopic 线程尚无已验证会话时，默认目录取父群当前绑定 cwd；线程首次成功建立会话即固定，与父群解耦 | 线程默认落在父群项目上；cwd 可共享，session 与共享说明仍按聊天目标隔离 |
| 线程隔离 | `create-thread` 带 `worktree_name` 时在调用方目录所属仓库旁建立 git worktree（目录与分支名即该名称），并把新线程绑定到该目录（`cwd_pinned`） | 需要独立工作区的线程不必共享父群目录；工作区名字由调用方显式给出，绑定与目录解析顺序一致，`/move` 可显式退出 |
| 迁移 | `/move <目录>` 使用 OMP 原生 `/move`，沿用当前 session ID、历史与附件 | 保持会话连续性，不伪造新会话 |
| 调度 | 同会话串行，同规范化目录串行；迁移同时持有真实源、目标目录 lease | 避免 channel 内任务同时修改同一目录 |
| 故障 | 保存历史，不自动重跑已交付 Agent 的任务 | 工具副作用无法安全推断或回滚 |
| 共享说明 | 按持久 OMP session 与聊天目标绑定 version；首次或 version 变化时注入，普通未变化 turn 不注入 | 降低重复输入并保持群/线程隔离；压缩生命周期单独负责恢复 |
| 入站内容 | 按已确认类型表提取文本；媒体附件下载到状态目录后以本机绝对路径交给 Agent；回复消息按同一张表读取客户端内联的被引用 payload（文本进引用块、媒体并入本条附件）；贴纸、系统与 CMD 等仍明确告知不支持 | 让常见消息真正可驱动 Agent，同时保持权限门、单一体积上限和可解释的拒绝行为 |

## 设计

### 运行和配置

使用 TypeScript/Bun 常驻服务，主动连接 Octo，启动并管理 OMP RPC worker。无需为 RPC 开放网络监听端口。macOS 使用 launchd 托管；机器休眠或离线期间不承诺可用。

本机入口为 `oco`（`omp-channel-octo` 的别名，`bun link` 安装）：`start` 是守护进程托管的服务进程入口，`run` 是不探测守护进程的调试入口，`restart`/`status`/`logs` 通过自动识别出的守护进程程序操作常驻实例，`test --card` 发送真实预览卡。配置在 `~/.oco/config/config.json`，服务日志由服务自持在 `~/.oco/logs/`，状态目录仍在 `~/.local/state/omp-channel-octo`；守护进程与程序名不写进配置。详见 `2026-09-16-oco-cli-entry-design.md`。

配置包含 Octo API 地址（缺省为 `https://im.deepminer.com.cn/`）、Bot token、可选 owner 用户 ID、允许群 ID、OMP 可执行文件及本地状态目录；WebSocket 地址可按现有发现机制取得或显式配置。未配置 owner 时，服务仅在该 Bot 尚无持久 owner 的条件下接受首次有效私聊绑定。凭证只来自本地配置或环境，不通过聊天设置，不写入日志。使用环境凭证时环境变量名默认 `OCTO_BOT_TOKEN`，显式名称仅允许 `OCTO_` 或 `OMP_CHANNEL_OCTO_` 前缀；所有这两类前缀的变量都会从 worker/watchdog 环境中剔除，以免子进程继承凭证。启动时校验配置、账户能力和状态目录可写性。同一 Bot 只允许一个 channel 实例，使用本机独占锁；不支持跨机器复制运行同一账户。
发卡前必须通过同一能力门禁：profile `enabled=true`（兼容服务端返回 `1`），`profiles` 包含 `octo/v1`，`card_version=1.5`，且 `config.card_enabled=true`、`config.display_enabled=true`、`config.reasoning_enabled=true`；`TextBlock` 是唯一硬依赖，`Container` 等可选元素缺失时使用 fallback；若响应提供 `elements` 或 `limits`，则遵守所需元素和各项限额（包括 payload、node、depth 限额）。能力响应明确不满足必需条件时按未启用处理；HTTP 失败属于请求失败，不等同未启用，不通过发送试探能力。
`launchd install` 只负责生成并写入权限为 `0600` 的 plist，不调用 `launchctl`；需要实际加载服务时由运维显式执行：

```sh
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"
launchctl bootout "gui/$(id -u)/com.omp.channel-octo"
```

`launchd uninstall` 只删除 plist，不代替 `bootout`。当凭证来自配置环境变量时，安装生成的私有 plist 会包含该变量及 token；该文件必须继续保持 `0600`。plist 固定记录安装时解析出的 OMP 可执行文件，运行时复用该路径，不自动切换本机其他 OMP。

复用本机 OMP 模型、Skills、扩展和项目环境。worker 明确设置 yolo，避免项目默认模式改变约定。OMP 显式工具 deny 或必须使用 UI 的扩展不由 channel 绕过；无法无界面完成时明确失败，不静默等待。执行权限不是沙箱，也不承诺限制 Agent 对目录外文件或网络的访问。

### 入站权限与路由

路由键为 Bot 身份、聊天类型和真实聊天 ID 的组合，不使用昵称。私聊（channelType=1）在已有持久 owner 时必须来自 owner；未配置且尚无持久 owner 时，仅支持的非空文本、`senderId` 与 `messageId` 非空且发送者非 Bot 的首条有效私聊可原子绑定 owner，并只确认不执行任务。传输层可能携带 `owner@bot` 形式的 raw channel ID，但归一后的 `channelId` 与 `senderId` 始终取真实发送者 UID，`owner@bot` 只属于传输层 ID。群（channelType=2）和 CommunityTopic 线程（channelType=5）同时要求 owner、允许列表命中的真实 channel ID、结构化 mention 指向当前 Bot，且不会触发首次认领。群级免@开启时不再要求 mention：`GET /v1/bot/groups/{group_no}/mention_pref` 的 `effective` 由服务端把 bot owner 的免@意图与群级 `allow_no_mention` 开关 AND 后给出，为真时该群及其线程接受未 @Bot 的消息——这是群设置本身授予的豁免，不是 channel 放宽权限。判定按群缓存 60 秒（线程用父群 ID）；查询失败或响应不符合契约时保持「必须 @Bot」，并只做短暂负缓存，避免上游故障拖住入站处理。免@只解除 mention 条件，owner、允许列表、Bot 回声过滤与首次认领规则都不变。线程的允许关系继承父群：允许列表里有该线程的完整真实 channel ID，或父群 ID（`____` 之前的段）在允许列表里，都算命中，因此白名单群下新建的线程无需额外配置即可对话；不在允许列表的群及其线程仍一律拒绝。文本中的名字或 @ 字符串不能替代 mention 元数据。目录继承复用父群绑定的 cwd，允许关系继承只决定该线程能否发起任务，两者都不改变 owner 判定、共享说明隔离或线程自身的绑定。先验证，再处理所有命令和普通消息。Bot 自发消息、其他成员消息、其他任务入口均不触发 Agent。

入站记录绑定原始聊天、发送者、消息 ID、会话 ID 和目录；之后回复只使用该记录，不读取可变的“当前聊天”。群和线程输出对成员可见，不发送完整工具原始结果、模型思考或系统上下文；发送助手可见正文与真实工具执行的简短进度。进度不作为普通聊天文字，而是在首次真实工具进度时懒建同一张可编辑卡，后续按最新状态编辑；卡片不可用或状态接口故障不得阻断正文。助手正文仍可能包含用户明确要求的文件内容，不承诺自动识别所有敏感信息。

### 命令

channel 在自身命令注册表中注册下列命令，`/help` 展示用法。这里不依赖 Octo 客户端提供命令菜单 API。

| 命令 | 行为 |
| --- | --- |
| `/new [目录]` | 创建并绑定 fresh OMP 会话；省略目录时按目标默认目录解析，无可用绑定时使用 `~/tmp` |
| `/reset [目录]` | 与 `/new` 使用相同的目录解析，但明确重置为 fresh 会话；原会话历史文件保留 |
| `/move <目录>` | 调用当前会话的 OMP 原生 `/move`，校验刷新后的 session state，沿用同一 session ID、历史和附件 |
| `/thread <任务>` | 交给助手处理：助手自行确定子区名称与 `worktree_name`，新建带独立 git worktree 的 CommunityTopic 子区并把任务派发进去（需求明确直接开工，不明确则由子区先澄清）；只在群聊中可用 |
| `/queue <文本>` | 把该文本作为排队任务追加在当前任务之后，不受当前追加模式影响；正文经与普通任务相同的组装点，随消息发来的附件以本机绝对路径进入排队任务的正文 |
| `/mode [steer\|queue]` | 设置/报告该聊天的追加模式：`steer`（默认，`default` 同义）把运行中的新消息注入当前回合，`queue` 把它变成排队任务；无参数时报告当前设置 |
| `/interrupt` | 中止当前聊天的运行中回合，保留已排队消息与它后面的排队任务；不终止 worker |
| `/cancel`（`/stop` 同义） | 取消当前聊天正在执行的任务，并清除该聊天待执行消息 |
| `/status` | 返回当前会话、目录、追加模式和运行/排队/中断状态 |
| `/restart [now]` | 通过部署方配置的重启命令安全重启 channel；省略参数时在有任何运行/排队任务时拒绝；`now` 强制重启并中断在途任务 |
| `/help` | 展示 channel 命令及目录用法 |

当前聊天有待回答问题时，owner 在该聊天的非命令回复就是答案本身：合法选项编号按选项校验，其他文本原样作为自填答案；没有待回答问题时同一文本按普通任务输入处理。待回答问题的答案优先于追加模式，不能被 steering 或排队吞掉。问题以交互卡片投递时，owner 对该卡片的一次按钮点击同样是这个问题的答案：点击与文本回复共用同一个答案 compare-and-set，只有一方生效，其他人的点击只 ack 不生效，卡片只是多一条作答路径而非唯一路径。

追加模式决定该聊天在已有运行中任务时如何处置新消息，按 `botUid` 与真实聊天目标隔离并持久化，跨重启保留，未设置时默认 `steer`。`steer` 模式下的普通文本经持久 claim 后作为 OMP `steer` 帧注入当前运行中的回合，与当前回合共用同一条任务和正文流；每条被注入的消息都是新的一轮，进度随之在新卡片上继续（见进度卡章节）。`queue` 模式下的普通文本走既有 durable acceptance，成为在该任务之后执行的排队任务，保留独立的正文消息与进度卡。OMP worker 只在其回合仍在流式输出时接受 steering：没有运行中的回合、回合已经结束或 OMP 拒绝该帧时，该文本回退为普通排队任务，不静默丢弃。`/queue <文本>` 始终走排队任务路径。steering 不产生新的任务记录或正文消息，也不改写当前回合已建立的群说明快照。

追加消息的可见性以「被消费」为准，而不是「投递成功」：`steer` 帧返回成功只表示消息进入 OMP 的内存待注入队列；OMP 在注入边界把它并入回合上下文（表现为一条 `role: user` 且 `steering: true` 的用户消息进入事件流）时，channel 才在该聊天补一行「已并入当前回合 ▸ <正文前 20 字>」回执。`queue` 模式与 `/queue` 产生的排队任务，只有接受时该聊天或目录已在忙、因而确实等待过的，才在真正从 `queued` 变为 `started` 时补一行「开始执行排队消息 ▸ <正文前 20 字>」；未被消费、被 `/cancel` 或进程重启丢弃的追加消息不产生回执。回执正文取该消息实际注入回合的正文（含引用/附件块）的单行前 20 字，超长以省略号截断，空文本只发前缀。

`/interrupt` 向该聊天运行中的回合发送 OMP `abort` 并保留同一 worker：不进入排队任务的 durable 取消路径，不清空已通过 steering 或 `/queue` 追加的消息，也不停止 worker 进程；该回合之后的排队任务按原顺序继续执行。回合因该中止结束且其后没有任何真实事件时，任务终态记为 interrupted，进度卡记为 cancelled；若回合继续产出正文或工具事件（例如处理已追加的消息），任务按正常完成结算。`/interrupt` 在运行时即时回复一条中断确认，失败时明确回复中断未被接受。

`/cancel` 与 `/stop` 是控制路径：发送 abort、等待 worker 停止，并清除该聊天的待执行消息与排队任务，因此同时也丢弃尚未消费的 steering 与排队消息；不回滚已经发生的副作用。

`/restart` 由 channel 检测出的守护进程执行：启动时按自身 PID 在 Super 的程序列表里认出托管自己的程序（见 `2026-09-16-oco-cli-entry-design.md`），命中才提供重启端口；未检测到守护进程时明确回复“未检测到守护进程，无法重启 channel。”且不猜测重启机制。命令由 channel 以独立会话派生，进程停止由该守护进程负责（Super 的 `restart` 会先给 channel 发 SIGTERM），channel 自己不会在命令未停止它时擅自退出，只记录一条告警。发起的聊天先收到“正在重启 channel”，请求被持久化；进程恢复后在原聊天投递“channel 已重启完成：PID …，启动于 …”。重启时被中断的在途任务发送“channel 正在重启，本会话任务已中断；重启完成后会在本会话确认。”，未开始的排队任务在下次启动按 interrupted 报告。重启完成通报超过 10 分钟未投递则丢弃，不对未发生的重启做确认。被中断回合的进度卡不再停留在「进行中」：卡片身份与快照已按任务持久化，新进程在启动时补发一次终态编辑（见卡片持久化），并照常投递重启中断的正文通知。

群中使用 `@Bot /new <目录>`、`@Bot /reset <目录>` 或 `@Bot /move <目录>` 等相同命令；剥离已验证的 Bot mention 后解析。命令与 mention 的先后顺序不固定：客户端可能发送 `@Bot /command <参数>` 或 `/command <参数> @Bot`，解析前剥离全部已验证的 Bot mention，无论它位于命令前、命令后还是参数中间，并清理剥离处残留的空白。结构化 span 与文本对不上或缺失时，退回按 mention uid 列表匹配独立出现的 `@BotName` 或 `@BotUid` 词元（两侧为空白或文本边界，允许尾随冒号）；未指向当前 Bot 的文本型 `@` 不剥离，用户正文中的其他 `@` 保持原样。命令只允许 owner 发起：群、线程和私聊中其他成员的命令一律拒绝，既不执行也不回复。channel 内置 `/new`、`/reset`、`/move`、`/thread`、`/queue`、`/mode`、`/interrupt`、`/cancel`、`/stop`、`/status`、`/help`、`/restart` 始终硬拦截，不发送给模型；若存在同名观察器也不能改变这一安全语义。其他 slash 输入按原始 `message.text` 作为普通 OMP task 透传，未知命令不再返回“不支持的命令”。
可通过服务选项注册自定义 `ChannelCommandHandler`：`name` 去除首尾空白并按小写规范化，`intercept` 明确决定是否消费，`handle(context)` 接收原始 `InboundMessage`、规范化命令名和解析后的 argument。`intercept=true` 在持久命令去重后执行 handler，成功后消费且不启动 OMP；失败保持可重试的命令状态且不启动 OMP。`intercept=false` 先完成现有任务 durable acceptance，再调用 handler 并继续透传原始文本；handler 异常不得阻断任务或 prompt，重复消息不得重复 handler 或 task。handler 必须经过既有 owner、群/线程 mention 和消息 ID 校验；空名、非法名或规范化后的重复注册在创建服务时拒绝，不能静默选择。

`/new`、`/reset` 的完整剩余文本是一个路径；可移除一对匹配的外围单引号或双引号。支持绝对路径、当前运行账户的 `~/`；拒绝相对路径、其他用户的 `~name` 和空引号。路径不经 shell 执行，不展开环境变量、通配符或命令替换；特殊字符按文件名处理。

省略目录时的默认目录按目标解析，顺序固定为：① 该目标已有绑定且绑定已有已验证会话文件，或该绑定目录是显式指定的隔离工作区（`cwd_pinned`，尚未建立会话时同样生效），沿用其规范化 cwd；② 目标是 CommunityTopic 线程（channelType=5）且其父群存在绑定，取父群绑定 cwd；③ 其余情况首次解析为 `~/tmp`。父群指线程真实 channel ID 中 `____` 之前的父群 ID，按 channelType=2 目标查询；channelId 不含合法父群段、父群无绑定或父群 cwd 已不可用时按 ③ 处理，不因此拒绝任务。显式绑定的隔离目录不可用时按后续顺序继续解析（线程回退父群或 `~/tmp`），不拒绝任务。

`/thread <任务>` 把意图交给助手处理，而不是由宿主同步建区：命令只把消息转成当前聊天的一个普通任务（正忙时在该聊天已有回合之后排队），任务正文是「原始意图 + 交付说明」——正文先经普通任务同一条组装点组装（随消息发来的附件下载到本机、绝对路径写进意图，引用块同理），再交给助手：先理解意图并自行确定简洁的子区名称与 ASCII 的 `worktree_name`；用 `octo_management` 的 `create-thread`（带 `worktree_name`）建隔离子区，再用 `thread-dispatch` 把任务派发进去；需求明确时直接写明要做什么、让子区立刻开工，需求不明确时要求子区先用 `ask` 向 owner 澄清、明确后再执行；澄清发生在子区里，命令发起的聊天不发起澄清提问。子区据此按 `create-thread` 相同的路径获得独立 git worktree 并把绑定固定到该目录（`cwd_pinned`），派发经与 `thread-dispatch` 相同的 durable 入站路径（发送者为持久 owner，子区内显示派发说明，命令发起的群收到一行回执）。工作区创建失败不回滚已创建的子区与已派发的任务：工具结果报告 `worktree_error`，子区按普通目录顺序执行任务。`/thread` 只在群聊（channelType=2）中可用，私聊与子区中的调用分别明确拒绝且不创建子区；任务文本为空时回复用法。

线程的目录继承只在它还没有已验证会话且没有显式隔离绑定时生效。线程第一次成功建立会话（`refreshState` 确认并持久化 session 文件）后，其 cwd 固定为绑定 cwd；父群随后 `/new`、`/reset`、`/move` 不改变任何既有线程绑定。已建绑定但从未成功建立会话的线程在下一个任务重新按上述顺序解析，并把解析结果写回绑定，显式 `/new <目录>` 会解除隔离绑定。服务启动时不为存量绑定做一次性回填，也不新增用户可见提示；`/status` 按绑定 cwd 照常报告。

`/move` 必须提供目标路径，按同样的安全规则解析；它只在当前绑定存在精确可恢复 session 文件时执行。channel 先取得并持有真实源目录与目标目录 lease，调用 OMP 原生 `/move <目录>`，再通过 `refreshState` 校验 session ID、session 文件和规范化目标 cwd，成功持久化后才更新绑定。源目录或目标目录有运行/排队任务、未确认停止的 worker 或 lease 冲突时拒绝，不隐式迁移或丢弃任务。

显式目录必须已经存在、为目录且可访问。使用 realpath 规范化并记录，符号链接别名共享同一个调度锁。创建 fresh worker 时传入 `--cwd`，不靠会话内 cd 模拟目录切换；创建或持久化失败保留原会话绑定。`/new`、`/reset`、`/move` 成功后才确认成功。

当前聊天有运行中或排队任务时 `/new`、`/reset`、`/move` 明确拒绝，并提示先 `/cancel`；没有会话时第一条合法普通文本按同一默认目录解析顺序创建会话，再处理消息。历史 OMP 会话文件保留，本次不提供 IM 历史列表或切回命令。

原生迁移的 worker 若无法确认停止，源目录和目标目录都持久标记为 blocked；服务重启恢复 worker ledger 时仍同时阻塞两目录，直到停止状态可确认。外部移动不通过最近文件猜测，而在后文定义的精确身份规则下自动重绑。

### 群说明、Host tools 与问答

每个群或线程任务在完成权限、去重和 durable acceptance 后，检查一次当前 Octo 共享说明的 version；channelType=2 从服务端 `groups/{id}/md` 读取 `GROUP.md`，channelType=5 从 `groups/{parent}/threads/{short}/md` 读取独立 `THREAD.md`。读取共享说明失败则该任务失败且不得启动 worker。读取结果规范化为带 `present`、完整 `content` 和按目标稳定且不可复用的 `version` 的快照；空内容是 `present=false` 的有效快照，必须保留其 server version。初次从未有说明的 404 可规范化为只用于初始无说明状态的 version 0 哨兵；已有 server version 的目标删除时必须由服务端返回新的删除 version，不能用无 revision 的 404 假装 version 未变化。

共享说明状态按 `botUid`、真实聊天目标和持久 OMP session 隔离。worker `create` 完成并由 `refreshState` 确认真实 OMP session ID 后，宿主才发布该 session 的受控 context sidecar；任务或绑定阶段产生的占位 session ID 不能作为 sidecar 或 marker 的身份。sidecar 只包含目标、真实 session ID、当前 turn 的 immutable snapshot、注入决策和不含秘密的 injection attempt ID，不包含 Bot token，extension 不得按 cwd、全局文件或模糊 session 回退。

新建 session 在第一次 agent start 注入完整的最新共享说明。恢复已有 session 时，若持久 marker 已证明同一 session、目标和 version 已成功提交，则不重复注入；没有 marker 或 marker 处于 pending 时按首次注入处理。普通 turn 只有在当前 version 与已提交 marker 不同时才注入，注入内容为完整最新快照，并附带版本变化说明；不生成 old/new diff。普通 `worker.prompt` 的 system-level 共享说明只由固定 OMP extension 的 `before_agent_start` 生命周期从 sidecar 注入，prompt 文本本身不携带共享说明正文；extension 的输入必须校验 Bot、目标和真实 session ID，缺失或不匹配时 fail closed。启用群历史后，prompt 文本由宿主拼接"群对话历史块 + 原始用户文本"，历史块属于用户消息而不是 system-level 上下文，与共享说明互不合并，规则见 `docs/specs/2026-09-16-group-history-context-design.md`。

注入采用 `pending`/`committed` 两阶段。宿主先原子发布 sidecar 和 pending 状态；extension 在返回 system-level context 前，以 injection attempt ID 原子写入同 sidecar 作用域的 `prepared` receipt。receipt 只证明 extension 已校验目标并生成了指定上下文，不代表模型已经接受；receipt 缺失、过期或绑定不匹配时，`before_agent_start` 必须失败，OMP 不得进入模型调用。宿主观察到 matching receipt 和 OMP prompt acceptance 后，才以同一 attempt ID 做 CAS，把该 version 标为 committed；未确认 acceptance 的 pending 在下一次任务中重放，不能静默跳过。正常成功路径同一 version 只注入一次；进程在 acceptance 与 marker 提交之间崩溃时按保守的 at-least-once 重放，以优先保证不丢共享说明。

OMP `session.compacting` 生命周期在压缩时把当前 turn sidecar 中的完整共享说明和 version 边界加入压缩上下文，即使 version 未变化；这是为保证压缩后仍保留 group/thread 语义的恢复性注入例外，不推进普通 turn 的 committed marker。当前 turn 的 sidecar 在 `octo_management` 的 `description-write` 后保持 immutable，compaction 使用本 turn 已建立的快照；写入成功的新 version 只在下一 turn 重新读取并发布。恢复后的下一次普通 turn继续依据 committed marker 判断，不因 compaction 再次重复注入。GROUP.md/THREAD.md 中的内容是共享上下文，不改变发送者身份、权限或 cwd。

群和线程任务获得一个复合工具 `octo_management`，动作固定为 `group-info`、`group-members`、`group-update`、`description-read`、`description-write`、`list-threads`、`create-thread`、`thread-info`、`thread-members`、`thread-join`、`thread-leave`、`thread-delete`、`thread-dispatch`，对应服务端 `groups/{id}`、`groups/{id}/members`、`groups/{id}/info`、`groups/{id}/md`、`groups/{id}/threads`、`groups/{id}/threads/{short}` 及其 `members`/`join`/`leave` 路径。目标固定为当前聊天：群任务取当前 `group_no`，线程任务取父群 `group_no`，`thread-*` 动作省略 `short_id` 时取当前线程，模型不能传入其他群或线程 ID。`group-update`、`thread-delete`、`thread-join`、`thread-leave` 等写操作的授权由服务端判定（`bot_admin`、线程创建者或群管理员），客户端不放行服务端拒绝的动作，也不做额外提权。失败时把服务端错误正文压成单行、截断到 200 字符后作为工具错误返回，便于模型纠正参数；错误文本在 HTTP 状态后带上服务端机器码（如 `err.server.bot_api.store_failed`）——bot API 在兼容窗口内把所有错误都以线缆 400 返回（服务端 `pkg/httperr/respond.go` 的 `ResponseErrorL` 固定传输状态 400，正文 `error.http_status` 才是语义状态），因此参数错误（`err.server.bot_api.request_invalid`）与内部失败只能靠正文的 code 区分；返回值按固定上限裁剪（成员 200、线程 50）并显式给出总数与截断标记，名称截断到 200 字符、公告截断到 2000 字符，不返回原始 HTTP 响应或凭据。`thread-delete` 对服务端瞬时存储失败做有界重试：响应为任意 5xx，或响应体 `error.code` 为 `err.server.bot_api.store_failed`（该内部失败在兼容窗口内同样以线缆 400 到达）时，换一条新连接（Bun fetch 显式 `keepalive: false`，不复用连接池的 keep-alive 连接）重试一次；服务端删除本身幂等，重试不会重复删除。重试成功后同样返回「已删除线程 `<short_id>`」；两次都失败时仍返回单行、≤200 字符的服务端错误，不把失败折叠成成功；4xx 参数与权限拒绝不重试。

`thread-dispatch` 让当前群/线程会话把一条任务派发给本群的某个 thread：先按服务端读取该 thread 的真实 `channel_id`（必须属于当前群），在该 thread 里投递一条可见的派发说明，再用与真实入站相同的 durable 路径为该 thread 接受一个任务（入站消息 ID 使用 `dispatch:` 前缀的唯一值、发送者为持久 owner、目标为该 thread），最后在当前聊天回执一行派发结果。派发不等待该任务完成，回执只表示已排队；被派发的任务在目标 thread 的绑定与 session 中执行，cwd 按线程默认目录规则解析（未建会话时继承父群绑定 cwd，因此与父群任务按目录串行），结果投递到该 thread 而不是发起聊天。由于派发复用真实入站路径，目标 thread 的问答、追加模式与排队语义照常生效。派发的深度固定为 1：任务记录的消息 ID 带 `dispatch:` 前缀时，该任务的 `octo_management` 不提供 `thread-dispatch` 动作，客户端也拒绝该动作。

`create-thread` 另有可选字符串参数 `worktree_name`：给出名称时，宿主在调用方当前目录所属的仓库（`git rev-parse --show-toplevel`）旁建立独立 git worktree，并把新线程绑定固定到该目录。名称同时是工作区目录与分支名（`<repo>.<worktree_name>` 的 ASCII slug，超过 32 字符截断），因此必须含 ASCII 字母或数字——纯中文等无法 slug 的名称在创建线程前被拒绝（`worktree_name must contain ASCII letters or digits (e.g. fix-login)`），不生成无意义的数字目录；名称被占用时退到 `<slug>-<short_id>`。创建调用部署解析出的 OMP 可执行文件的 `omp worktree add`，因此继承 OMP 自身的 clone-first 行为（未跟踪与被忽略的文件如 `node_modules` 一并克隆，无需重新安装依赖），有界超时 120 秒并随工具取消信号中止。绑定写入 `chats.cwd_pinned`，线程随后按该目录建立自己的 session；worktree 创建失败不改写已经创建的线程，工具结果返回单行、截断 200 字符的 `worktree_error`，线程仍按普通目录顺序使用。成功时结果在 `worktree` 字段返回路径与分支。省略该参数时不建立工作区，线程按默认目录规则继承；部署未提供隔离能力时同样以 `worktree_error` 说明，不伪装成功。

`octo_management` 的 `description-read` 每次通过实时 `getGroupContext` 读取服务端当前说明，不使用 prompt 或 compaction 快照；`description-write` 只更新当前群/线程目标且需要服务端 `bot_admin` 权限。写入成功后，后续任务和 extension sidecar 使用新的 version；当前 turn 已经建立的 context 不回写。私聊不读取或注入群说明，也不获得群说明动作。

所有任务都获得固定目标与 cwd 的 `ask`、`octo_send_file` 工具，以及控制 channel 自身的 `channel_control`；群和线程额外获得复合工具 `octo_management`。`channel_control` 的 `restart` 排定一次 channel 重启：它在本回合结束后、且 channel 没有运行中或排队任务时才让检测出的守护进程重启自己，完成后在发起会话通报新 PID；因此在自己的回合里改完代码即可让新实现生效（自迭代），既不打断正在投递的回复，也不需要外部脚本或延迟作业。未检测到守护进程时明确回复未检测到且不猜测重启机制。`status` 返回进程 PID、运行中的会话目标、排队任务数、被阻塞目录数与是否已排定重启，供重启前后核对。文件工具的相对路径按任务 cwd 解析，绝对路径可指向本地其他文件，系统不提供目录沙箱保证；发送前复制到 stateDir spool；outbox 保存稳定 client message ID，断线重试不重跑任务。文件永久失败标记 dead 并报告，但不得阻塞后续文字 outbox。

`ask` 创建带持久化 ID 的问题后暂停当前任务，并给出一段包含问题正文、编号选项（如有）和回复规则的文本。只要该聊天存在待回答的问题，owner 在该聊天的下一条非命令文本即为该聊天最早待回答问题的答案：合法选项编号按单选/多选的数量和范围校验，其他文本原样作为自填答案返回给模型；命令仍然优先。不提供默认答案，无待回答问题、不同聊天、重复或过期后的回复都不作为答案，其中无待回答问题和过期后的文本按普通任务输入处理。问题十分钟无答案即过期；取消、服务重启会使等待失效。

部署宣布交互档时（`GET /v1/bot/card/profile` 的 `profiles` 含 `octo/v2`、`config.card_enabled`/`display_enabled`/`interaction_enabled` 为真），问题改以一张 type-17 交互卡片投递，并把卡片的远端 message ID 记在问题行上：卡体是问题正文与编号选项，单选问题为每个选项一个 `Action.Submit` 按钮，多选问题为 `Input.ChoiceSet` 加提交按钮，无选项问题为 `Input.Text` 加提交按钮；信封的 `plain` 与纯文本路径逐字一致，所以不认该 profile 的客户端照常显示问题文本。卡片发送失败（含未宣布交互档）时退回原有的纯文本投递，问题语义不变。点击与文本回复共用同一个答案 compare-and-set：只有一方生效，非 owner 的点击不生效，问题关闭后的点击不覆盖已记录的答案；服务端对同一 `(message_id, action_id, operator_uid)` 只受理一次点击，因此未成答的点击（例如空内容提交）用新的 action id 重绘卡片并回复一次提示，用户仍可继续作答。答案落盘后卡片被 best-effort 改写成终态（已回复并带上所选项、已过期、已取消），改写失败只记日志，不影响答案。

交互卡片的上行由 channel 主动消费 Bot 事件队列：`POST /v1/bot/events` 长轮询读取（`wait` 上限低于客户端请求超时并留余量），游标按 `botUid` 持久化且单调递增，事件处理完成后才 `POST /v1/bot/events/{event_id}/ack`，ack 失败或处理可重试时保留游标让同一事件下轮重读。投递是至少一次：重复投递的点击按已记录答案处理，不再作答也不重复改写。该队列只有本 channel 一个消费者；非 `card_action` 的事件（含无 `event_type` 的旧式消息事件）ack 后略过，不参与聊天路由。部署没有该端点时事件轮询自行停止，问答退回纯文本路径。

除 host tool 外，任务里的 Agent 还可以用本机 `octo-cli` 以本 bot 身份直接访问 Octo Bot API：宿主注册 Bot 后把该 bot 的 token 经 stdin 写入自己的 octo-cli 凭据目录（`~/.oco/octo-cli/<botUid>`，目录权限 0700，命令行与日志中不出现 token），并把 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 注入 OMP worker 环境。一个 bot 一份目录，宿主服务多个 bot 时凭据互不共享。原始 token 不进入 worker 环境：OMP worker 继续按前缀剥离宿主继承的 `OCTO_*` 与 `OMP_CHANNEL_OCTO_*`，只有这三个名字允许进入子进程。凭据准备是 best-effort，octo-cli 缺失、登录失败或返回的 robot_id 与当前 bot 不一致时只记录一条有界日志并保持不注入，不影响启动与任务。因此 host tool "目标固定为当前聊天" 的承诺只约束 host tool 自身；经 octo-cli 的调用不受该绑定限制，其授权完全由服务端按本 bot 身份判定。`channel_control` 的说明同时告知 Agent 这条路径，并要求它先用 `octo-cli auth status` 自证身份，身份不是本 bot 时不使用。


### RPC 与输出

worker ready 后协商协议版本；本地实现保持 OMP v18.1.18 兼容的 RPC 解码，按请求 ID 匹配响应并正确处理 v2 分块；宿主独立管理 worker 生命周期。`agent_end.willContinue=true` 表示仍有后续生命周期或消息，必须继续等待，不提前收尾正文或 typing；prompt 接受不代表任务完成，以生命周期事件和异步错误判断结果；本地完成但未调用模型的 prompt 也必须结束状态。

按需启动 worker；同一持久会话只能被一个 worker 写入。新会话只通过 fresh RPC `new_session` 创建并立即落盘；对已有绑定，恢复前先按 session ID 与 `previousSessionFiles` breadcrumb 在 native roots 中寻找并验证唯一外移候选，找到后持久化重绑；扫描或验证失败则保留原绑定并报告失败，不能用“最近会话”替代精确身份。恢复 worker 时仅传入已经确认存在且准确的 session 文件和保存的目录。正常退出释放 worker。
OMP extension 在 worker 启动时由固定路径自动加载；sidecar 根路径通过宿主显式 allowlist 的非秘密 worker 输入传给 extension，sidecar 在真实 session ID 确认后、第一次 `prompt` 前原子发布，extension 在生命周期回调中按该 ID 精确读取并写入 `prepared` receipt。宿主必须能观察 matching receipt 和 prompt acceptance，并以 attempt ID 做单调 CAS；worker 停止、重启、恢复和 compaction 不得绕过 `pending`/`committed` 判定。宿主不向 extension 暴露 Octo 认证凭证，extension 不得按 cwd、全局路径或模糊 session 查找 sidecar。

`WorkerEvent` 的正文事件为稳定的本地 `messageId`、累计正文快照和 `final` 标记；上游没有原生 ID 时由 adapter 生成。每个 assistant message 独立维护 stream，首次非空快照立即创建一条正文消息，后续累计快照只编辑同一个远端 message ID；不同 assistant message 不合并。`final=true` 是该消息的权威结束快照，允许比此前更短或为空；已有远端消息必须编辑到最终空文本，尚未创建且从未有非空正文的空最终事件不发送空白新消息。

正文增量可先于 `message_start` 到达；迟到或重复的开始事件复用当前尚未结束、上游身份兼容的 stream，保留累计正文和本地 message ID。消息结束后或出现明确不同的上游身份时才分配下一条消息 ID。

正文编辑以最新累计快照为准，首次创建不等待整轮结束；编辑最多每 1000ms 一次并按目标串行，最终快照优先完成，不得由旧快照覆盖新快照。正文按 3000 UTF-8 bytes 稳定分段，保留原始换行、空格和 inline 标记；分段 stream 使用稳定的 client message ID，分段数量变化时清理 stale tail。每次网络操作前持久化该分段的目标快照及 `desiredVersion`/`ackedVersion`；首次 create 在网络请求前 durable 标为 `unknown`，送达未知时不重跑 Agent，重连/重启沿用稳定 ID 绑定已创建消息并将最新 desired 快照补做 edit。

真实 `tool_start`/`tool_end`、`turn_start`/`turn_end`、`todo_update` 和 `run_end` 事件由单一 `ExecutionTracker` 累计阶段、工具、todo、终态和 elapsed；每一轮——任务开始的一轮，或追加消息开启的新一轮——各自在其第一份真实工具或 todo 进度出现时懒建一张可编辑卡，之后只编辑该轮的卡。追加消息被接受时，上一轮的卡片立即以一次终态编辑收尾（`completed`），新的一轮从自己的第一份真实进度起在会话底部另开一张卡，因此正在看的进度始终在最新消息下方，而不是停在任务最初那次工具调用处；该轮的卡沿用任务当前的 todo 清单，阶段、工具与耗时只统计本轮。运行中 elapsed 只在真实事件到达时随卡片刷新，事件编辑遵守 1000ms 合并窗口，无事件时不做周期重绘；卡片 edit 使用严格递增的单卡 sequence，终态为 `completed`、`cancelled` 或 `failed`。卡片采用“摘要头部—当前状态—任务清单—子代理—执行轨迹”的进度通知布局；任务清单按阶段→任务的树形层级渲染，使用分支连接线和嵌套区块表达父子关系；任务节点只显示状态图标、任务内容和 blocker，不重复渲染状态文字；富布局下阶段标题与任务分支使用结构化 ColumnSet 分列，避免依赖比例字体空格对齐；富布局能力下使用强调色 Container、状态/耗时摘要和带状态元数据的步骤面板，轨迹步骤使用 Web 端兼容的 ColumnSet，轨迹行使用固定 24px 等宽标记列和 stretch 正文列，状态摘要置于正文列并允许自然换行，避免窄屏下正文被状态列挤压；状态图标与步骤内容、状态摘要同处彩色背景 Container 内；子代理列表位于执行轨迹之前；当对应区块超过三个步骤且能力支持 `Action.ToggleVisibility` 时，在标题行右侧提供“展开”/“收起”控件，默认展示最近三个步骤，其余步骤通过“展开”查看；能力支持 `Action.ToggleVisibility` 时，卡片头部始终提供“收起”/“展开”整卡详情控件：运行中默认展开任务、子代理和执行轨迹详情，收起后只保留摘要头部、当前状态与告警；终态卡片默认收起这三类详情，保留状态摘要，并可通过头部的“展开”恢复；状态标记按语义统一：成功/已完成使用绿色 `✓`，等待、取消和阻塞使用中性灰色 `-`，失败和放弃使用红色 `×`，进行中使用蓝色/Accent `•`；取消任务内容在支持 `RichTextBlock` 时使用删除线；能力不足时保持信息完整的降级文本。内置 `hub`/IRC、`task`、`todo` 和 `ask` 工具使用操作级安全摘要，IRC 轨迹显示方向、目标或协作状态，不显示消息正文；命令类工具显示压成单行的有界命令文本摘要，便于在轨迹里直接看到所运行的命令。卡片只展示安全摘要，不转发工具参数/结果、原始 payload、模型思考或秘密。超出渲染预算且仍可展开时，轨迹与子代理都使用同一组紧凑 ColumnSet 行，状态标记列与 stretch 摘要列保持一致；不支持展开时只保留安全的隐藏计数。紧凑行的等待字形后使用不换行空格，避免客户端将 `- ` 解析成 Markdown 列表。
富布局下子代理区块只将仍在运行或等待的子代理作为默认丰富行；已完成、失败、取消或停止的子代理默认隐藏，能力支持 `Action.ToggleVisibility` 时随“展开”以紧凑行查看。执行轨迹默认展示最近三个工具步骤的丰富行，其余步骤即使展开也只使用紧凑文字行。当前 OMP worker 以通用 RPC `tool_execution_start`/`tool_execution_end` 形成安全工具事件：`bash` 的摘要是压成单行、截断到 72 字符的命令文本，路径类工具只取安全文件名组件，其余工具按操作语义派生；命令文本中的凭据形态字符串由卡片层统一脱敏，宿主始终不转发原始 args/result。OMP 另有可选的 `set_subagent_subscription` 生命周期/进度事件桥，但它不属于 `pi.on` 专用 hook，且尚未纳入本服务的 `WorkerEvent` 卡片契约。
任务清单发生预算裁剪时保留隐藏任务摘要；支持 `Action.ToggleVisibility` 时，标题行显示“展开”/“收起”按钮，展开后按原阶段和原任务顺序将被裁剪任务插回树形位置，并复用富布局任务行的状态标记、颜色、blocker 与层级连接；若卡片节点预算不足，先按 8/4/2 收缩可展开隐藏行的渲染行数（任务隐藏行上限不超过 4），保留隐藏数量提示，仍不足时再按既有顺序裁剪可见工具行与任务行；能力不支持该动作时只保留安全的隐藏计数。

发卡前必须通过能力 profile 门禁：profile 启用，包含 `octo/v1` 与 card version 1.5，且 card/display/reasoning 开关均启用；`TextBlock` 是唯一硬依赖，`Container` 等布局元素缺失时使用扁平 fallback；`Action.ToggleVisibility` 仅在能力声明支持时渲染。遵守服务端声明的 payload、node、depth 等 limits，无法满足时不发卡且不阻断正文；同一轮最多一张卡。

每张卡各自一行，按 `(botUid, taskId, messageId)` 持久化：建卡成功即写入远端 message ID、sequence 1 与当时的快照，之后每次被服务端接受的 edit 覆盖为新的 sequence 与快照，终态 edit 成功后删除该行。同一任务的多张卡互不覆盖，追加消息收尾上一轮时它仍是自己那一行，因此每一张遗留的卡都能被独立补发。进程重启后仍遗留的行由新进程补发一次终态编辑：sequence 取持久值 +1，状态按任务的终态取 `completed`/`failed`，其余（含重启中断）取 `cancelled`，快照由持久载荷重放同一个 tracker 得到，因此轨迹、脱敏与终态规则与在线回合一致。补发失败时保留该行并记录有界原因，且把持久 sequence 一并加一，避免与可能已落地但响应丢失的上一次尝试撞车；同一行累计尝试达到上限后丢弃并记录，不无限重试。补发遵循与在线 edit 相同的 2 秒有界 deadline，失败或载荷不可解析都不阻断启动。

执行开始立即发送 typing 并每 3 秒续期，成功、异常、取消和停止均清理 timer 并 abort 在途运行中请求；每个 typing/card 活动请求使用独立 2 秒 deadline，终态卡片使用独立短 signal。运行中卡片失败可按有界节流重试；终态 edit 失败只做一次 best-effort，随后 dispose 卡片活动，不无限重试，其遗留的持久记录由下一次启动补发。卡片队列 promise 纳入服务生命周期并在停止时于任务结束后等待其有界收敛，不能拖延正文投递、取消 kill、目录解锁或正常业务。typing/card 失败不阻断正文或业务。

### 持久化、调度与恢复

本地 SQLite 保存聊天绑定、任务、入站去重键、问题、出站记录和进度卡记录，OMP 自身保存对话。入站唯一键使用 Bot 身份、聊天类型、真实聊天 ID 和服务器消息 ID。没有可靠 ID 的任务拒绝执行。去重记录与接收状态事务写入成功后才确认接收；状态盘写入失败时不发送成功 ACK、不启动 Agent，并保留可重试的命令状态。每个聊天的追加模式（`steer`/`queue`）持久化在与绑定、任务同一套状态盘中，未写入行时按 `steer` 处理。作为 steering 消费的入站消息同样先 durable claim 再发送 RPC 帧：claim 成功而 OMP 拒绝该帧时释放 claim 并回退为普通排队任务，已 claim 未确认的消息在服务器重投时不会重复注入。
共享说明的持久 marker 与 sidecar 状态属于 SQLite 中的 session context ledger，至少记录 `botUid`、真实聊天目标、真实 OMP session ID、当前 server version、`pending`/`committed` 阶段和 injection attempt ID。marker 的提交必须是同 session/目标/version 的 CAS；同一 pending 在重放前沿用其 attempt ID，提交后不得再次接受旧 attempt。新 session 不能继承旧 session marker，外部移动若保持同一真实 session ID 则保留 marker，`/new`/`/reset` 产生新 session 时清空其 marker。sidecar 与 marker 发布、extension 注入确认和 `description-write` 的后续 version 发布均不得以占位 session ID 完成。

状态为 queued、started、completed、failed、cancelled、interrupted。启动 Agent 前持久化 started；跨此边界后永不自动重跑，即使崩溃发生在写入与实际 prompt 之间。每条 assistant 正文 stream 的每个分段独立持久化 outbox，任务终态只保存真实 assistant 正文（无正文则为空），并与终态状态写入 durable store。保存的不只是内存队列。正常运行按接受顺序调度，同规范化目录串行；不同目录不提供重叠树、硬链接或外部终端写入隔离保证。

服务重启将遗留 queued 和 started 标记 interrupted，清除自动执行资格，在重新连接后向原聊天报告需重新发送。历史和当前绑定可恢复不意味着任务继续执行。重启会使未完成问题等待失效；去重记录仍保留，服务器重投同一消息不能重新启动副作用；用户主动发送的新消息产生新的任务。

取消是控制路径，不在普通任务队列后等待：发送 abort，等待 worker 停止；超过 10 秒关闭该 worker 进程树。只有确认进程退出且 channel 管理的执行已停止后才释放目录锁；无法确认则保持该目录阻塞并报告未知状态。取消不回滚已发生的副作用，不保证已派生到外部系统的工作停止。服务启动恢复前核查自身管理的遗留 worker；不能确认停止时不运行冲突目录的新任务。迁移操作无法确认停止时同时保留 source/target 两目录 blocked，跨重启继续生效。

Octo 断线不取消已经开始的 Agent；正文 outbox 与任务状态持久化，重连后按聊天顺序继续投递，不重跑任务。重连采用有上限退避并重新认证，重复消息经持久去重；永久认证失败停止重连并给出本地明确错误。文字和文件 outbox 按聊天顺序发送，送达未知时沿用稳定 client message ID；文件永久失败标记 dead 并报告，不阻塞后续文字回复。

外部移动恢复只接受精确 session ID 且通过 session 文件的 `previousSessionFiles` breadcrumb 证明身份；不按最近文件、不按时间、不按模糊 basename 选择。候选 roots 按 OMP native profile 与 `.env` 对当前有效 cwd 的解析结果取得，再加 channel state sessions root。候选不唯一、无法读取或无法验证时保持原绑定，不自动猜测；唯一候选通过 session header 的身份与 cwd 校验后立即持久化重绑，不启动 worker。`refreshState` 仅用于原生 `/move` 与 fresh 命令的 worker state 校验。

## 验收标准

1. 实际 Octo 私聊发起文件操作，在选定目录观察结果并收到完成回复；全程没有工具审批。
2. owner 在允许群 @Bot 和允许的 CommunityTopic 线程（channelType=5）可执行：线程的允许关系继承父群，父群在允许列表里时该群下新建的线程无需额外配置即可对话，父群不在允许列表的线程一律拒绝；其他成员、未 @、伪造文本 @、Bot 回声均不启动 worker 或命令。
3. `/new`、`/reset` 省略目录时按目标默认目录解析的固定顺序取目录：已有已验证会话沿用绑定 cwd，线程尚无会话时取父群绑定 cwd，其余使用 `~/tmp`；显式目录（含空格）规范化为真实目录；`/move <目录>` 使用原生 OMP `/move`，同一 session ID 保留历史与附件，非法目录或校验失败不改变绑定。
4. 首次普通消息创建默认目录会话（DM/群为 `~/tmp`）；从其他目录再次 `/new` 或 `/reset` 仍沿用当前绑定目录并创建 fresh session。
5. 私聊与两个群/线程的上下文不串线；交错完成的结果各自返回原聊天；同一持久 session 在共享说明 version 不变时连续任务不重复注入，version 变化时只注入一次新的完整快照并给出变化说明；共享说明读取失败不启动 worker。
6. 私聊获得 `ask`、`octo_send_file` 与 `channel_control`；群/线程额外获得 `octo_management`，其 `description-read` 实时读取对应服务端 `GROUP.md` 或独立 `THREAD.md`（分别对应 `groups/{id}/md` 与 `groups/{parent}/threads/{short}/md`），`group-info`/`group-members`/`group-update`/`list-threads`/`create-thread`/`thread-info`/`thread-members`/`thread-join`/`thread-leave`/`thread-delete` 作用于当前群或当前线程，写操作依赖服务端授权；`thread-dispatch` 把一条任务派发给本群 thread 并在该 thread 排队执行、向两个聊天各投递一条可见说明，派发深度为 1；新建和无可靠 marker 的恢复 session 首次注入共享说明，普通未变化 turn 不注入；工具不能改写目标聊天或 cwd。
7. 同规范化目录的两个聊天写入任务不重叠；`/new`、`/reset`、`/move` 在运行或排队时拒绝；`/move` 同时取得真实 source/target lease，未确认停止时两目录跨重启保持 blocked。
8. 长正文保留原始换行、空格和 inline 标记；每个 assistant message 独立 stream，首次非空累计快照立即 create，后续累计快照以同一 message ID 每 1000ms 最多一次串行 edit，最终更短或空快照权威生效；正文按 3000 UTF-8 bytes 稳定分段并清理 stale tail，异步 prompt 失败、本地完成和 worker 异常退出均正确结束任务状态，不生成通用完成文字。
9. 正文 outbox 在 create 前 durable 记录 `unknown`，保存 desired/acked 版本；断线、重启或送达未知恢复时沿用稳定 client ID、编辑最新快照且不重跑 Agent，不因失败重复创建。
10. 执行过程中取消能越过待执行队列；后续队列被清除，取消不被宣称为回滚。未确认停止时不启动同目录新任务。
11. `ask` 问题可跨消息持久等待；owner 在同一聊天的下一条非命令文本被当作答案（编号选项按单选/多选校验，其他文本原样返回），不同聊天、无待回答问题、重复或过期后的文本都不作为答案并按普通任务处理，取消不使用默认答案。宣布交互档时问题以 `octo/v2` 卡片投递，单选/多选/无选项三种卡形各自带选项按钮、选择列表或自由输入，点击只由 owner 生效并与文本回复争用同一个答案判定；未成答的点击用新 action id 重绘后仍可作答，答案落盘后卡片改写为终态，卡片不可用时退回纯文本；事件队列按持久游标消费、处理后才 ack，重复投递不重复作答，非 `card_action` 事件与无端点部署都不影响问答。
12. 在接收、started 落盘、执行、回复阶段分别终止服务再启动：绑定可恢复，历史可读，遗留任务报告中断，同消息重投不再执行副作用；外部移动只按精确 session ID 与 breadcrumb 唯一重绑，roots 依 native profile/`.env` 有效 cwd 解析，不选最近文件。
13. Octo 断开再连接不重跑任务；已持久化正文从 outbox 继续投递，送达未知明确呈现；真实 `ExecutionEvent` tracker 每轮用一张卡累计工具/turn/todo/run 终态与 elapsed，只在真实事件到达时随卡片刷新（编辑按 1000ms 合并窗口推进，无周期重绘），遵守 TextBlock/Container fallback、ToggleVisibility 能力和 payload/node/depth limits，卡片 edit sequence 单调递增，安全摘要不含原始 payload 与参数对象（命令类工具显示单行、截断到 72 字符并经凭据脱敏的命令文本）；typing 每 3 秒续期、活动请求 2 秒有界，终态卡片失败只做一次 best-effort，均不阻断正文；每张卡的身份、sequence 与最近快照各占一行持久化，终态编辑成功后删除该行，进程重启后仍遗留的卡片（同一任务可能多张）由下一次启动逐张补发一次终态编辑（序号严格递增、状态取任务终态、失败按有界次数重试后丢弃），使客户端无需重新执行任务即可离开「进行中」。
14. 状态盘写入失败不启动新任务；同 Bot 第二实例不能启动（在重新注册 Bot 之前即报错退出）；App Bot 配置不能被验收为支持群聊。
15. `launchd install` 仅写入权限为 `0600` 的 plist，不调用 `launchctl`；实际启动、停止分别由 `launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"` 与 `launchctl bootout "gui/$(id -u)/com.omp.channel-octo"` 完成，`launchd uninstall` 只删除 plist。环境凭证会进入该私有 plist，runtime 复用 plist 中安装时解析出的 OMP 路径；退出交互终端后仍能从实际 Octo 收发，日志不包含 token。
16. 新建 session 的第一次 agent start 会注入目标的完整共享说明；恢复 session 只在没有可靠的同 session/目标/version committed marker，或 marker 仍处于 pending 时补注入；已 committed 的恢复不会因重新启动 worker 自动重复注入。
17. 同一 session 的共享说明 version 变化时，OMP 收到一次完整最新快照和明确的版本变化说明；删除也按带 server version 的变化处理并明确当前无说明；version 不变的普通 turn 的 OMP prompt 不包含共享说明正文，也不把共享说明并入群历史块。
18. OMP compaction 后，压缩上下文保留当前 turn immutable snapshot 的完整共享说明和 version 边界；即使 version 不变也只在该 compaction 生命周期中恢复性注入，下一次普通 turn 不重复注入；当前 turn 的 `octo_management` `description-write` 不改写这份 snapshot。
19. 共享说明 sidecar、持久 marker 和 extension 输入按 Bot、真实聊天目标与真实 OMP session 隔离，不包含 Bot token；extension 或 sidecar 目标不匹配、输入不完整或 session ID 未确认时在进入模型前拒绝共享上下文；marker CAS 失败时保留 pending、不得标为 committed，并在下一任务重放。
20. OMP prompt acceptance 与 context marker 提交之间发生崩溃时，pending 状态可恢复并在下一任务重放，不能静默跳过；正常成功路径不重复注入同一 version，重放使用明确的 injection attempt ID。
21. `/restart` 只使用检测出的守护进程，未检测到守护进程时明确拒绝且不猜测重启机制；存在运行或排队任务时默认拒绝并提示 `/cancel` 或 `/restart now`；`now` 先持久化重启请求再执行重启，发起聊天收到“正在重启 channel”，进程恢复后在发起聊天与被打断会话投递重启完成通报，超过 10 分钟的旧通报不投递；守护进程未停止进程时 channel 继续运行并记录告警。真实 CLI 端到端覆盖检测、进程自行以 0 退出以及重启后的确认投递。重启中断在途任务时，该任务已发出的进度卡由重启后的进程补发一次终态编辑（sequence 取持久值 +1、状态 cancelled），不再停留在「进行中」。
22. CommunityTopic 线程在尚无已验证会话时，其默认目录取父群当前绑定 cwd；父群无绑定、真实 channel ID 不含合法父群段或父群目录不可用时回退 `~/tmp`，不拒绝任务。线程首次成功建立会话后 cwd 固定，父群随后 `/new`、`/reset`、`/move` 不改写该线程；已建绑定但从未成功建立会话的线程在下一个任务重新按父群解析并写回绑定；启动时不对存量绑定做一次性回填。线程继承期间仍使用自己的 session、自己的 `THREAD.md` 与独立 marker，任务按规范化目录与父群串行，不新增用户可见提示。

23. 追加模式按聊天目标持久化并在重启后保留，默认 `steer`：`steer` 模式下运行中收到的普通文本经持久 claim 后作为 OMP `steer` 注入当前回合，不新增任务或正文消息，并为该轮新开一张进度卡（上一轮的卡以 `completed` 收尾）；`/mode queue` 后同一文本走 durable acceptance 成为排队任务，独立正文与进度卡；`/queue <文本>` 在任何模式下都走排队任务（正文经同一附件组装点：随消息发来的附件落盘后以绝对路径进入排队任务正文）；没有运行中的回合、回合已结束或 OMP 拒绝该帧时该文本回退为排队任务而不丢失；同一 messageId 重投只注入一次。`/interrupt` 中止运行中回合并保留该回合已追加的消息及其后的排队任务，worker 进程不被终止；回合因中止结束且无后续真实事件时任务记为 interrupted、进度卡记为 cancelled，回合继续产出时正常结算。`/stop` 与 `/cancel` 等价，清除排队任务与未消费的追加消息。追加消息的消费可见：被消费的 steering 在并入回合上下文时、确实等待过的排队任务在开始执行时，各在该聊天补一行有界回执；仅投递成功而尚未消费、或被取消/重启丢弃的追加消息不发回执。

24. 群级免@：服务端 `mention_pref.effective` 为真时，owner 在该群及其线程发送的未 @Bot 文本正常启动任务；`effective` 为假、查询失败或响应不可解析时不启动任务、不回复也不落任何入站副作用；同一个群在 60 秒缓存窗口内只查询一次，带 mention 的消息不触发查询；非 owner、不在允许列表或 Bot 回声在任何情况下都不产生该查询。

25. 线程隔离：`octo_management` 的 `create-thread` 带 `worktree_name` 时，在调用方当前目录所属仓库旁建立独立 git worktree（经部署的 OMP 可执行文件的 `omp worktree add`，clone-first 复制被忽略文件），工作区目录与分支即该名称（slug 截断 32 字符，被占用时退到 `<slug>-<short_id>`），并把新线程绑定固定到该目录（`chats.cwd_pinned`）；该线程下一个任务以工作区目录启动自己的 session，父群随后 `/new` 改目录不影响它；工作区目录被删除时线程按普通目录顺序回退父群目录而不是拒绝任务；显式 `/new <目录>`、`/move <目录>` 解除固定；名称不含 ASCII 字母或数字时在创建线程前以参数错误拒绝；worktree 创建失败时不改写已创建的线程，工具结果返回单行、≤200 字符的 `worktree_error`；省略 `worktree_name` 时线程照旧继承父群目录。

26. `/thread`：群聊中 owner 的 `/thread <任务>` 只把意图转成当前聊天的一个任务（正文含原始意图与交付说明：由助手自行确定子区名称与 `worktree_name`、`create-thread` 建区、`thread-dispatch` 派发；需求明确直接开工，不明确则要求子区先用 `ask` 向 owner 澄清），命令本身不建子区、不建工作区、不发回执；随后助手按该说明建出子区与独立工作区（子区名与工作区名由助手决定，不再由任务文本机械派生）、子区绑定固定到工作区、任务以该目录启动自己的 session 并在子区显示派发说明、发起群收到一行回执；任务文本为空回复用法；私聊与子区中的调用明确拒绝且不产生任务；当前聊天有运行中任务时该任务排队等待而不是 steer；随消息发来的附件（例如富文本里的截图）与普通任务走同一条组装点，落盘后的绝对路径写进原始意图，不再只剩 `[图片]` 占位。

27. Agent 的 Octo 身份：注册 Bot 后宿主把该 bot 的 token 经 stdin 写入 `~/.oco/octo-cli/<botUid>` 的 octo-cli profile（目录 0700，token 不出现在命令行、日志与工具结果中），并只把 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 注入 OMP worker；宿主继承的 `OCTO_BOT_TOKEN` 与其他 `OCTO_*` 仍被剥离，不进入子进程；每个 bot 一份独立凭据目录，后注册的 bot 不会复用前一个的凭据；octo-cli 缺失、登录失败、响应非 JSON 或返回的 robot_id 与当前 bot 不一致时只记录一条 ≤200 字符且不含 token 的日志、保持不注入，channel 照常启动并继续处理任务；任务里的 Agent 可以 `octo-cli` 以本 bot 身份访问未被 host tool 包装的 Octo Bot API，该路径不受 host tool 的当前聊天绑定约束，授权由服务端判定。

28. `thread-delete` 的瞬时失败重试：首次响应为 `err.server.bot_api.store_failed`（线缆 HTTP 400、body 声明 `http_status: 500`）或任意 5xx 时，用同一 `short_id` 换新连接重试一次（第二次调用携带新连接标记，不复用连接池连接）；重试成功时工具返回「已删除线程 `<short_id>`」且线程从列表消失；重试仍失败时工具错误被压成单行、≤200 字符并保留机器码（`err.server.bot_api.store_failed`；多行与超长服务端正文都在压缩截断范围内），线程保持存在、不报告假成功；4xx 参数与权限拒绝只调用一次、不重试。

## 实施前提与未决事项
既有实现已完成 `/new`/`/reset` 目录绑定、原生 `/move`、精确外部 session 恢复、正文 outbox、执行 tracker、卡片和生命周期边界；共享说明采用 session/version/OMP lifecycle context 设计；线程默认继承父群工作目录已实现：`src/thread-target.ts` 是线程目标父群解析的单一来源，传输层 `contextPath` 与服务的默认目录解析都经它取父群 ID。线程隔离工作区已按验收 25 落地（`src/worktree.ts` 的 worktree port + 绑定的 `cwd_pinned`），依赖部署解析出的 OMP 可执行文件提供 `worktree add`；OMP 未启用 `worktree.clone` 时新工作区不含被忽略文件。Agent 的 Octo 身份已按验收 27 落地：`src/octo-cli.ts` 维护每个 bot 一份 octo-cli 凭据目录，`workerEnv` 以 allowlist 把三个变量注入 OMP worker，其余 `OCTO_*` 继续被剥离；该路径要求部署环境存在 `octo-cli`，缺失时只降级为无 CLI 身份。
未决事项：无。线程默认目录继承已按验收 3/4/22 落地并验证（见下）；真实 OMP compaction、删除 version、崩溃重放和凭证隔离验证仍按共享说明章节的验收补做。

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

真实卡片视觉验收使用 `oco test --card`（等价于 `bun run test:preview-card`），仅在显式指定时向真实 Octo 会话发送一张综合预览卡片，包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）、4 个阶段和 12 个 Todo 任务，覆盖成功、等待、失败、取消和进行中状态；它读取 `BOT_TOKEN` 或 `OCTO_BOT_TOKEN`，目标优先读取 `OCTO_PREVIEW_TARGET_UID` 或 `OCTO_OWNER_ID`，未指定时使用 Bot 注册响应中的 owner UID，并可选读取 `OCTO_API_URL` 与 `OCTO_PREVIEW_CHANNEL_TYPE`（`1`、`2` 或 `5`），并会先应用 checkout 的 `.envrc`。普通 `bun test` 不执行该真实发送测试。
`botToken` 与 `botTokenEnv` 互斥；使用环境凭证时先设置 `OCTO_BOT_TOKEN`（或配置中允许前缀的变量名）。安装 launchd 后按上文命令显式执行 `bootstrap`/`bootout`；实际项目级 OMP/Octo 协议、会话迁移、正文与进度卡行为已完成实现。launchd 尚未加载；脱离终端运行和真人登录态完整自动触发回合尚未验证，仍以用户本机 Bot 配置和部署可达为前提。
首条有效私聊必须是支持的非空文本，且 `senderId`、`messageId` 非空并且发送者不是 Bot；它会原子持久化 owner 绑定和稳定去重确认，只发送确认，不执行任务。群聊和线程永远不会触发首次认领。首次认领存在风险：未配置 owner 时，第一个满足这些条件的私聊用户会成为 owner，请先确认 Bot 私聊入口的可信范围。

显式配置的 owner 是权威来源，服务启动时会持久化它；即使之后删除配置中的 owner，也不会重新开放首次认领。绑定跨重启保留，重复的首条消息不会执行任务；`check`/`serve` 在配置未提供 owner 时会提示“若状态中尚无持久 owner，将等待首次有效私聊绑定”。

本地实现还保留各阶段崩溃恢复、SQLite 故障注入、文件 outbox、`ask` 和 owner/群权限边界；状态与历史可恢复不代表已开始 Agent 自动续跑。AdaptiveCard 视觉边界仅记录真实 Octo AdaptiveCard SDK DOM：390px 宽度无横向溢出、折叠/展开可用、todo 可见、`completed`/`cancelled`/`failed` 三种终态 SDK 校验无错误；实时预览命令覆盖带登录态的真实卡片发送，截图像素与完整真人交互仍需单独观察，不能混同。
验收证据（仅记录已完成的最终状态）：已在真实 `https://im.deepminer.com.cn/v1/bot/register` 注册成功并验证加密 WebSocket 认证成功；真实 CLI `check`/`serve`/`SIGTERM` 生命周期与 `plutil` 校验保持有效，真实 OMP fresh worker 可成功启停。真实 OMP 经加密 Octo wire 的正文为 2 次 create、19 次 edit，最终文本逐字一致；进度为 1 张卡、18 次 edit、6 个 phase、5 个 tool、2 个 todo，typing 18 次，stop 后无新增。真实 native project 20.66s 覆盖私聊与两个群隔离、含空格目录、native `/move` 同 ID 保留历史/附件、`/new`/`/reset` 复用绑定 cwd、外部 move 自动重绑后在新项目真实写入文件以及重启保持绑定。真实 Bot HTTP 已验证正文 create 加 2 次同 ID edit、卡片 create 加 2 次同 ID edit；实时预览验收命令已成功发送一张包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）、4 个阶段、11 个 Todo 任务及全部状态的综合预览卡片。launchd 尚未加载，脱离终端和完整真人登录态仍未验证；上文记录的 AdaptiveCard SDK DOM 证据不等同截图像素或完整 Octo 登录态视觉验收。

线程默认继承父群工作目录的验收证据：核心矩阵 `tests/thread-cwd-inheritance.test.ts` 覆盖父群继承、父群无绑定 / channel ID 无合法父群段 / 父群目录已删除三类回退、`/new` 无参继承与后续沿用、线程会话建立后固定、未验证会话重解析并写回、显式目录覆盖、以及 DM/群既有语义保持，共 10 例；把 `src/service.ts` 回退到修改前版本时其中 5 例失败，单独移除父群分支或移除父群 cwd 不可用时的静默回退分别使 6 例、1 例失败，确认用例不是空转。真实进程验收在 `tests/cli-lifecycle.test.ts`：真实 CLI + 加密协议夹具 + 真实 SQLite 下，父群 `@Bot /new` 建立目录后，线程任务以该目录启动真实 worker 子进程（夹具日志的 startup cwd 恰为该继承目录）、线程按 `/v1/bot/groups/<group>/threads/<thread>/md` 读取自己的 `THREAD.md`、`/status` 报告继承 cwd，并在 SIGTERM 重启后保持。
线程允许关系继承与派发的验收证据：`tests/core-host-tools.test.ts` 的 "threads inherit the parent group allowlist without their own entry" 覆盖父群在允许列表时其线程无需额外条目即可执行、父群不在允许列表时线程仍被拒绝并保持 `none`；"thread dispatch queues the task in the target thread and reports it in both chats" 覆盖 `create-thread` + `thread-dispatch` 在目标线程接受真实任务（继承父群 cwd、与父群任务同目录串行）、线程与发起群各收到一条可见说明、以及被派发的任务其 `octo_management` 动作集不再包含 `thread-dispatch`。把允许命中回退为只比较完整 channel ID 并移除派发深度守卫后，这两个用例分别失败，确认用例不是空转。

线程隔离工作区的验收证据：`tests/worktree.test.ts` 用真实 git 夹具 + 等价 argv 的可执行 stub 覆盖 slug 与候选命名、成功创建同名工作区、分支或目录被占用时退到 `<slug>-<short_id>`、不含 ASCII 的名称被拒、非 git 目录与命令失败的 ≤200 字符单行错误、以及超时与已取消请求；`tests/core-host-tools.test.ts` 的 "create-thread worktree_name binds the new thread to a workspace of its own" 断言工作区请求携带调用方绑定 cwd 与给定名称、新线程被绑定到工作区（其第一个任务以该目录启动）、成功结果含 `worktree.path/branch`，"create-thread worktree_name reports a workspace failure while keeping the thread usable" 断言工作区失败时线程仍按父群目录执行且结果含 `worktree_error`，同一文件的参数校验用例覆盖 `worktree_name` 非字符串与纯中文名称在创建线程前被拒；`tests/thread-cwd-inheritance.test.ts` 的 C11 覆盖父群 `/new` 换目录后隔离线程仍留在工作区，C12 覆盖工作区被删除时任务按父群目录完成而不是被拒绝。把 `defaultWorkDirectory` 的 `cwdPinned` 分支、或隔离线程的绑定写入分别移除后，C11 与 "create-thread worktree_name binds…" 用例各自失败，确认用例不是空转。真机冒烟（本机真实 `omp`）：`createOmpWorktreePort` 在临时仓库创建 `smoke-project.smoke-isolate`（359 ms，clone-first 把被忽略的 `node_modules` 一并带进新工作区），同名第二次调用按 `smoke-isolate-123456789012345` 退让，验证后 `git worktree remove` 清理。真实入口验收在 `tests/cli-lifecycle.test.ts`：真实 CLI + 加密协议夹具 + RPC worker 夹具下，群任务经 `octo_management` `create-thread` `worktree_name` 走部署同款入口（解析出的 OMP 可执行文件）创建真实 git worktree，断言返回路径是磁盘上带该分支的工作区；把 `runService` 的 `worktrees` 接线移除后该用例以线上同款 `worktree_error` 失败，确认用例不是空转。
`/thread` 的验收证据：`tests/core-thread-command.test.ts` 覆盖 4 例——群内 `/thread <意图>` 只产生一条以该意图结尾、含 `create-thread`/`worktree_name`/`thread-dispatch`/澄清分支说明的任务 prompt（cwd 取该群绑定目录），此刻不建子区、不请求工作区、不发回执；夹具在 prompt 回调里按交付说明调用 `octo_management`（`create-thread` 带助手自定的 `name`/`worktree_name`，再 `thread-dispatch`），断言工作区请求携带该名称与调用方绑定 cwd、子区绑定到工作区并让派发任务以工作区为 cwd 启动、子区收到派发说明、群收到派发回执；空任务、私聊与子区中的调用分别回复用法或明确拒绝且不产生任务；群内已有运行中任务时该任务排队、在前一回合结束后才以指令 prompt 启动。把交付说明换成裸意图后前 2 例失败、单独移除子区守卫后拒绝用例失败，确认用例不是空转。`/thread` 带附件的用例在 `tests/core-attachments.test.ts`（"thread 带附件：意图走同一条组装点，附件随交付说明一起交给助手"）：群内 `/thread <任务>` 带图片时恰好下载一次、图片按其绝对路径进「原始意图」块且落盘字节与远端一致；把 `handleThreadCommand` 里的组装点调用换成直接接受任务后该用例零下载失败，确认用例不是空转。

群级免@的验收证据：`tests/core-mention-pref.test.ts` 覆盖 `effective` 为真时未 @Bot 的群消息与线程消息各启动一个任务、为假或查询失败时保持不启动且不回复、同一个群两次未 @Bot 只查询一次而带 mention 的消息不查询、被 owner/允许列表/Bot 回声规则先拒的消息零查询；`tests/octo-transport.test.ts` 的 "reads the group 免@ decision, including the legacy no_mention shape" 用真实 HTTP 夹具断言端点路径与 Bot 鉴权头，覆盖 `effective` 布尔与旧版 `no_mention` 0/1 两种形状，以及不可解析响应被拒。把 `isAllowed` 的免@分支摘掉后 `tests/core-mention-pref.test.ts` 有 4 例失败，确认用例不是空转。线上只读实测（本群）：`GET /v1/bot/groups/e3bc5af0d4b84b18bb3770a74bd222c6/mention_pref` 返回 `{"no_mention":0,"group_allow_no_mention":1,"effective":false}`，即群开关已允许、bot owner 尚未开启，因此本群当前仍需 @Bot。
进度卡持久化与重启补发的验收证据：`tests/core-progress-card-recovery.test.ts` 覆盖 7 例——在线回合把服务端已接受的卡片帧（message ID、sequence、快照）写入状态盘并在终态编辑成功后清除该行；重启后遗留的 started 任务以 `cancelled` 补发一次终态编辑，已 completed 的任务以 `completed` 补发，序号均为持久值 +1，重放出的快照保留原有工具轨迹与耗时；同一任务遗留的多张卡各占一行、各自补发一次终态编辑（每张卡的序号各取自己的持久值 +1）；补发失败保留该行（attempts 与序号同时 +1，避免与可能已落地的尝试撞车）并在下一次启动成功收尾；连续失败达到上限后该行被丢弃；不可解析的载荷不产生任何编辑且不阻断启动。把 `start()` 中的补发调用移除后这 6 例补发/丢弃用例全部失败，把建卡成功后的持久化调用移除后在线回合用例失败，确认用例不是空转。追加消息换卡由 `tests/core-presentation.test.ts` 的 "a steered message closes the current card and the next round opens a new one" 端到端断言：被追加的消息让上一轮的卡收到唯一一次 `completed` 终态编辑（其轨迹保留该轮两个工具），新的一轮在下一份真实工具进度时以不同的 client message ID 另建一张卡，且该卡的阶段与工具只统计本轮、todo 清单沿用上一轮；把 `deliverSteer` 里的 `rollProgressCard` 调用去掉后该用例失败，确认用例不是空转。旧库升级由同一文件的 "a database written before per-round cards keeps its rows and gains card identity" 覆盖：按旧结构 `PRIMARY KEY(bot_uid, task_id)` 建立的数据库在打开后保留遗留行（含 attempts 与 sequence）、表结构变为 `PRIMARY KEY(bot_uid, task_id, message_id)`、同一任务的第二张卡可与第一张并存、删除其中一张只影响它自己，且重复打开不再次重建；把 `migrateProgressCards` 调用去掉后该用例失败，确认用例不是空转。
追加消息消费可见性的验收证据：`tests/omp-worker.test.ts` 的 "steer injects a message into the streaming turn until that run ends" 断言真实子进程夹具发出的 `role: user` 且 `steering: true` 消息帧被翻译为恰好一次 `steer_consumed` 且文本逐字一致，同文件的 message-end 用例断言未标记 `steering` 的用户消息不产生该事件；`tests/core-steering.test.ts` 的 "a steered message reports the moment it joins the running turn" 断言消费后在聊天投递唯一一行「已并入当前回合 ▸ …」（含 20 字截断），"queue mode turns a message during a run into a task that follows it" 断言只有确实等待过的消息在开始执行时收到一行「开始执行排队消息 ▸ second task」。把 `#emitWorkerEvent` 的 steering 分支、`onEvent` 的消费分支、`runTask` 的开始回执分别移除后，对应的三个用例各自失败，确认用例不是空转。真机实测（不经过常驻服务，用真实 OMP 18.2.1 可执行文件与本仓库 worker 工厂直接驱动）：prompt 请求执行 `sleep 8`，4 秒后投递 steering 文本，事件流为 `turn_end`(0) → `turn_start`(1) → `steer_consumed`（文本逐字一致）→ 后续工具事件，全程恰好一次 `steer_consumed`，首轮 prompt 自身不产生该事件。
`channel_control` 的验收证据：`tests/core-host-tools.test.ts` 覆盖私聊与群/线程的工具集合断言（`ask,channel_control,octo_send_file`，群内追加 `octo_management`），以及 `channel_control` 的 `restart`/`status` 行为——`status` 返回本进程 PID 且初始 `restart_pending=false`；`restart` 在回合内只返回「已排定重启」，此刻重启命令一次都没有执行过；同一聊天排队任务运行期间重启保持等待；队列清空后重启命令恰好执行一次，并为发起会话写入重启通报（下次启动据此投递新 PID）；未注入重启端口（即未检测到守护进程）时返回未检测到守护进程的文本且不写通报。把 `createHostTools` 中的 `channel_control` 摘掉后，上述工具集合断言与两例 `channel_control` 行为用例全部失败，确认用例不是空转。
交互问答卡片与 Bot 事件队列的验收证据：`tests/core-ask-card.test.ts` 覆盖 8 例——单选问题的卡片点击被记录为答案并把同一张卡改写成已回复状态（含所选项、`card_seq` 递增、按钮消失）、同一问题的纯文本回复同样作答并改写同一张卡、无选项问题由自由输入提交作答、多选按所选索引作答、空提交用新 action id 重绘卡片且问题保持可答、非 owner 的点击保持问题待答且不产生答案、答案记录后的重复投递点击静默且不更新卡片、卡片发送失败退回纯文本问题（问题仍可被文本作答、事件仍被 ack）；把 `deliverQuestion` 的卡片分支关掉后 7 例失败，单独去掉点击的 owner 校验后非 owner 用例失败，确认用例不是空转。`tests/octo-transport.test.ts` 的 "Octo interactive cards and Bot events" 用真实 HTTP/WS 夹具覆盖 `octo/v2` 卡片发送与已答复改写、交互未开启时拒绝出卡、`POST /v1/bot/events` 的游标与 ack、`wait` 上限被夹到客户端请求超时以内、端点缺失视为无事件支持、队列与 ack 失败传播。线上实测：本 Bot 的 `GET /v1/bot/card/profile` 宣布 `octo/v2` 与交互开关；探针向真实私聊发送交互卡片被服务端接受并返回 message id；服务实现启动后经 `POST /v1/bot/events` 与逐条 ack 清空既有 54 条非交互事件积压（含无 `event_type` 的旧式消息事件与 `bot_joined_group`），无重试与错误日志，游标持久化。真人点击一轮（在服务发出的 `ask` 卡片上点击并观察答案入账与卡片改写）仍需在真实客户端单独完成，未计入本节证据。


Agent Octo 身份的验收证据（验收 27）：`tests/octo-cli.test.ts` 用 stub CLI 覆盖登录调用本身（`auth login --profile oco --bot-id <uid> --with-token`，token 走 stdin 且不出现在 argv）、凭据目录为 `<home>/.oco/octo-cli/<botUid>` 且权限 0700、登录子进程不再继承宿主的 `OCTO_BOT_TOKEN`、`env()` 只返回 allowlist 中的三个名字，以及退出码非零、响应非 JSON、robot_id 不匹配、可执行文件缺失四种失败下 `env()` 保持 undefined 且只记一条不含 token 的日志；`tests/omp-worker.test.ts` 用真实子进程断言 OMP worker 收到 `OCTO_CONFIG_DIR`/`OCTO_API_BASE_URL`/`OCTO_BOT_ID`，而宿主继承的与调用方误传的 `OCTO_BOT_TOKEN` 都不进入子进程；`tests/core-boundaries.test.ts` 断言 `start()` 以注册到的 bot uid 调用 `provision`，并在 `provision` 抛错时 channel 仍能完成一次任务。线上实测（合并到主检出并经守护进程重启加载实现后）：服务日志出现 `[octo-cli] bot … 的 CLI 凭据已就绪：/Users/dong/.oco/octo-cli/<botUid>`；任务 shell 里 `printenv` 显示 `OCTO_CONFIG_DIR`、`OCTO_API_BASE_URL`、`OCTO_BOT_ID` 且 `OCTO_BOT_TOKEN` 不存在；`octo-cli auth status` 返回 `source=profile:oco` 与 `robot_id=28azkybli6q43216d14_bot`；经该身份调用未被 host tool 包装的端点成功：`octo-cli bot space-members --limit 2`（2 条）、`octo-cli api GET /v1/bot/groups/<本群>/incoming-webhooks`（`list: []`）、`octo-cli bot user-info --uid <owner uid>`（「董振兴」）。

`thread-delete` 瞬时失败重试的验收证据（验收 28）：线上诊断显示 `store_failed` 失败在时间上成簇出现且与服务进程无关——同一服务进程在失败 16 秒后以完全相同的 `short_id` 重试成功（14:32:19 → 14:32:35），独立 curl 进程也在同一窗口内对同一个存活子区命中过同一种 400（16:50:46，响应体为 `{"error":{"code":"err.server.bot_api.store_failed","details":{},"http_status":500,"message":"服务器内部错误。"},"msg":"服务器内部错误。","status":400}`），随后 octo-cli 对该子区删除成功（16:50:55）；对服务 DELETE 请求的字节级抓取显示除 `user-agent`/`accept-encoding` 等传输层默认头外不存在任何参数差异（无 body、无 query）。服务端源码 `pkg/httperr/respond.go` 的 `ResponseErrorL` 在兼容窗口内把传输状态固定为 400、真实状态放在正文 `error.http_status`，`pkg/errcode/bot_api.go` 中 `err.server.bot_api.store_failed` 声明为 `Internal: true` 的 500 错误，参数错误则是另一个 code（`err.server.bot_api.request_invalid`）。`tests/core-host-tools.test.ts` 覆盖三条路径："thread-delete retries a transient store failure on a fresh connection"（先注入 `store_failed`(400)+`code` 失败，断言两次 `thread-delete` 调用、第二次带 `freshConnection: true`、结果「已删除线程 topic-9」、线程从列表消失）、"thread-delete reports one bounded line when the retry fails too"（503 触发重试，重试再以 5xx+多行超长正文失败时工具错误被压成恰好 1 行 200 字符、线程保持存在）、"thread-delete does not retry a permanent server rejection"（403 只发生一次调用、错误原样返回）；`tests/octo-transport.test.ts` 的 "spends a connection of its own on a delete that asks for one" 用真实 HTTP 夹具记录每个管理请求的客户端端口，断言带 `freshConnection` 的 DELETE 落在一条此前未使用的 TCP 连接上——临时去掉该选项后同池连接被复用、该断言失败，证明它不是空断言；"keeps the server's machine error code for an internal store failure" 用夹具回放线上信封（线缆 400 + `err.server.bot_api.store_failed` + `http_status: 500`），断言错误文本保留该机器码——重试分类读取的正是这个字段。
