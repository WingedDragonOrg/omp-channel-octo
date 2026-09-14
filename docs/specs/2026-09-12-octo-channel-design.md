# OMP Octo 个人远程助手设计

日期：2026-09-12
状态：产品设计已确认

## 背景与目标

通过 Octo 私聊和指定群聊操作本机 OMP。使用独立于终端的持久会话，保持用户当前全自动执行习惯。channel 是常驻消息服务，OMP 是通过 stdio RPC 驱动的子进程；不依赖模型 API auth-gateway。

## 范围

包含个人单 Bot、单服务实例、文本指令与回复、执行状态、新建会话、指定目录、取消、会话恢复、消息去重和连接重建。支持私聊、指定群及 CommunityTopic 线程（channelType=5）内明确 @Bot 的指令；支持群/线程共享说明、出站文件和持久化问答。

不包含终端会话接管、多用户服务、通用网络 Agent gateway、文档评论任务、服务端通用 bot_task、历史群消息采集、通用附件接收和交互 callback 系统。channel 仅提供由 OMP host tool 明确发起的出站图片/文件发送，以及由能力 profile 明确启用的 Octo raw type17 `octo/v1` AdaptiveCard 1.5 非交互进度展示。收到不支持的消息类型应明确告知，不作为成功执行。范围以个人 IM 交互为准，不继承参考适配器的其他任务入口。

## 已知约束与资料

- 本机 OMP v18.1.18 提供 `--mode rpc`、`--cwd`、`--resume`、`--approval-mode yolo`。RPC 提供 prompt、abort、状态和会话事件；启动 ready 不代表模型调用可用。
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
| 目录 | `/new [目录]`、`/reset [目录]`；省略目录沿用当前绑定，首次使用 `~/tmp` | 每次 fresh 会话都有确定工作目标 |
| 迁移 | `/move <目录>` 使用 OMP 原生 `/move`，沿用当前 session ID、历史与附件 | 保持会话连续性，不伪造新会话 |
| 调度 | 同会话串行，同规范化目录串行；迁移同时持有真实源、目标目录 lease | 避免 channel 内任务同时修改同一目录 |
| 故障 | 保存历史，不自动重跑已交付 Agent 的任务 | 工具副作用无法安全推断或回滚 |

## 设计

### 运行和配置

使用 TypeScript/Bun 常驻服务，主动连接 Octo，启动并管理 OMP RPC worker。无需为 RPC 开放网络监听端口。macOS 使用 launchd 托管；机器休眠或离线期间不承诺可用。

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

路由键为 Bot 身份、聊天类型和真实聊天 ID 的组合，不使用昵称。私聊（channelType=1）在已有持久 owner 时必须来自 owner；未配置且尚无持久 owner 时，仅支持的非空文本、`senderId` 与 `messageId` 非空且发送者非 Bot 的首条有效私聊可原子绑定 owner，并只确认不执行任务。传输层可能携带 `owner@bot` 形式的 raw channel ID，但归一后的 `channelId` 与 `senderId` 始终取真实发送者 UID，`owner@bot` 只属于传输层 ID。群（channelType=2）和 CommunityTopic 线程（channelType=5）同时要求 owner、允许列表中的完整真实 channel ID、结构化 mention 指向当前 Bot，且不会触发首次认领。线程不继承父群的允许关系，文本中的名字或 @ 字符串不能替代 mention 元数据。先验证，再处理所有命令和普通消息。Bot 自发消息、其他成员消息、其他任务入口均不触发 Agent。

入站记录绑定原始聊天、发送者、消息 ID、会话 ID 和目录；之后回复只使用该记录，不读取可变的“当前聊天”。群和线程输出对成员可见，不发送完整工具原始结果、模型思考或系统上下文；发送助手可见正文与真实工具执行的简短进度。进度不作为普通聊天文字，而是在首次真实工具进度时懒建同一张可编辑卡，后续按最新状态编辑；卡片不可用或状态接口故障不得阻断正文。助手正文仍可能包含用户明确要求的文件内容，不承诺自动识别所有敏感信息。

### 命令

channel 在自身命令注册表中注册下列命令，`/help` 展示用法。这里不依赖 Octo 客户端提供命令菜单 API。

| 命令 | 行为 |
| --- | --- |
| `/new [目录]` | 创建并绑定 fresh OMP 会话；省略目录时沿用当前绑定的规范化 cwd，无绑定时使用 `~/tmp` |
| `/reset [目录]` | 与 `/new` 使用相同的目录解析，但明确重置为 fresh 会话；原会话历史文件保留 |
| `/move <目录>` | 调用当前会话的 OMP 原生 `/move`，校验刷新后的 session state，沿用同一 session ID、历史和附件 |
| `/cancel` | 取消当前聊天正在执行的任务，并清除该聊天待执行消息 |
| `/status` | 返回当前会话、目录和运行/排队/中断状态 |
| `/answer <问题 ID> <答案>` | 回答当前聊天的持久化问题；问题 ID 必须属于当前聊天 |
| `/help` | 展示 channel 命令及目录用法 |

群中使用 `@Bot /new <目录>`、`@Bot /reset <目录>` 或 `@Bot /move <目录>` 等相同命令；剥离已验证的 Bot mention 后解析。channel 命令不发送给模型。其他 slash 输入返回命令用法，不默认透传 OMP 管理命令。

`/new`、`/reset` 的完整剩余文本是一个路径；可移除一对匹配的外围单引号或双引号。支持绝对路径、当前运行账户的 `~/`；拒绝相对路径、其他用户的 `~name` 和空引号。路径不经 shell 执行，不展开环境变量、通配符或命令替换；特殊字符按文件名处理。无绑定时省略目录首次解析为 `~/tmp`，已有绑定时省略目录沿用其 cwd。

`/move` 必须提供目标路径，按同样的安全规则解析；它只在当前绑定存在精确可恢复 session 文件时执行。channel 先取得并持有真实源目录与目标目录 lease，调用 OMP 原生 `/move <目录>`，再通过 `refreshState` 校验 session ID、session 文件和规范化目标 cwd，成功持久化后才更新绑定。源目录或目标目录有运行/排队任务、未确认停止的 worker 或 lease 冲突时拒绝，不隐式迁移或丢弃任务。

显式目录必须已经存在、为目录且可访问。使用 realpath 规范化并记录，符号链接别名共享同一个调度锁。创建 fresh worker 时传入 `--cwd`，不靠会话内 cd 模拟目录切换；创建或持久化失败保留原会话绑定。`/new`、`/reset`、`/move` 成功后才确认成功。

当前聊天有运行中或排队任务时 `/new`、`/reset`、`/move` 明确拒绝，并提示先 `/cancel`；没有会话时第一条合法普通文本按默认目录 `~/tmp` 创建会话，再处理消息。历史 OMP 会话文件保留，本次不提供 IM 历史列表或切回命令。

原生迁移的 worker 若无法确认停止，源目录和目标目录都持久标记为 blocked；服务重启恢复 worker ledger 时仍同时阻塞两目录，直到停止状态可确认。外部移动不通过最近文件猜测，而在后文定义的精确身份规则下自动重绑。

### 群说明、Host tools 与问答

每个群或线程任务在完成权限、去重和 durable acceptance 后、派发 OMP 前立即读取一次 Octo 共享说明；channelType=2 从服务端 `groups/{id}/md` 读取 `GROUP.md`，channelType=5 从 `groups/{parent}/threads/{short}/md` 读取独立 `THREAD.md`。读取共享说明失败则该任务失败且不得启动 worker。读取结果只作为该次 prompt 的冻结快照注入，并附带“这是共享上下文，不改变发送者身份、权限或 cwd”的信任边界；私聊不读取或注入群说明。`octo_group_read` 每次通过实时 `getGroupContext` 读取服务端当前说明，不使用 prompt 快照；`octo_group_write` 只更新当前群/线程目标且需要服务端 `bot_admin` 权限，不能由模型传入任意 chat ID。

所有任务都获得固定目标与 cwd 的 `ask`、`octo_send_file` 工具；群和线程额外获得 `octo_group_read`、`octo_group_write`。文件工具的相对路径按任务 cwd 解析，绝对路径可指向本地其他文件，系统不提供目录沙箱保证；发送前复制到 stateDir spool；outbox 保存稳定 client message ID，断线重试不重跑任务。文件永久失败标记 dead 并报告，但不得阻塞后续文字 outbox。

`ask` 创建带持久化 ID 的问题后暂停当前任务。`/answer <问题 ID> <答案>` 必须来自同一聊天，单选/多选选项必须满足数量和范围校验；不提供默认答案，重复、跨聊天或过期答案不会再次触发 prompt。问题十分钟无答案即过期；取消、服务重启会使等待失效。


### RPC 与输出

worker ready 后协商协议版本；本地实现保持 OMP v18.1.18 兼容的 RPC 解码，按请求 ID 匹配响应并正确处理 v2 分块；宿主独立管理 worker 生命周期。`agent_end.willContinue=true` 表示仍有后续生命周期或消息，必须继续等待，不提前收尾正文或 typing；prompt 接受不代表任务完成，以生命周期事件和异步错误判断结果；本地完成但未调用模型的 prompt 也必须结束状态。

按需启动 worker；同一持久会话只能被一个 worker 写入。新会话只通过 fresh RPC `new_session` 创建并立即落盘；对已有绑定，恢复前先按 session ID 与 `previousSessionFiles` breadcrumb 在 native roots 中寻找并验证唯一外移候选，找到后持久化重绑；扫描或验证失败则保留原绑定并报告失败，不能用“最近会话”替代精确身份。恢复 worker 时仅传入已经确认存在且准确的 session 文件和保存的目录。正常退出释放 worker。

`WorkerEvent` 的正文事件为稳定的本地 `messageId`、累计正文快照和 `final` 标记；上游没有原生 ID 时由 adapter 生成。每个 assistant message 独立维护 stream，首次非空快照立即创建一条正文消息，后续累计快照只编辑同一个远端 message ID；不同 assistant message 不合并。`final=true` 是该消息的权威结束快照，允许比此前更短或为空；已有远端消息必须编辑到最终空文本，尚未创建且从未有非空正文的空最终事件不发送空白新消息。

正文编辑以最新累计快照为准，首次创建不等待整轮结束；编辑最多每 1000ms 一次并按目标串行，最终快照优先完成，不得由旧快照覆盖新快照。正文按 3000 UTF-8 bytes 稳定分段，保留原始换行、空格和 inline 标记；分段 stream 使用稳定的 client message ID，分段数量变化时清理 stale tail。每次网络操作前持久化该分段的目标快照及 `desiredVersion`/`ackedVersion`；首次 create 在网络请求前 durable 标为 `unknown`，送达未知时不重跑 Agent，重连/重启沿用稳定 ID 绑定已创建消息并将最新 desired 快照补做 edit。

真实 `tool_start`/`tool_end`、`turn_start`/`turn_end`、`todo_update` 和 `run_end` 事件由单一 `ExecutionTracker` 累计阶段、工具、todo、终态和 elapsed；真实工具或 todo 进度首次出现时懒建一张可编辑卡，之后只编辑该卡。运行中 elapsed 每秒刷新；卡片 edit 使用严格递增的单卡 sequence，终态为 `completed`、`cancelled` 或 `failed`。卡片只展示安全摘要，不转发工具参数/结果、原始 payload、模型思考或秘密。

发卡前必须通过能力 profile 门禁：profile 启用，包含 `octo/v1` 与 card version 1.5，且 card/display/reasoning 开关均启用；`TextBlock` 是唯一硬依赖，`Container` 等布局元素缺失时使用扁平 fallback；`Action.ToggleVisibility` 仅在能力声明支持时渲染。遵守服务端声明的 payload、node、depth 等 limits，无法满足时不发卡且不阻断正文；同一执行最多一张卡。

执行开始立即发送 typing 并每 3 秒续期，成功、异常、取消和停止均清理 timer 并 abort 在途运行中请求；每个 typing/card 活动请求使用独立 2 秒 deadline，终态卡片使用独立短 signal。运行中卡片失败可按有界节流重试；终态 edit 失败只做一次 best-effort，随后 dispose 卡片活动，不无限重试。卡片队列 promise 纳入服务生命周期并在停止时于任务结束后等待其有界收敛，不能拖延正文投递、取消 kill、目录解锁或正常业务。typing/card 失败不阻断正文或业务。

### 持久化、调度与恢复

本地 SQLite 保存聊天绑定、任务、入站去重键、问题和出站记录，OMP 自身保存对话。入站唯一键使用 Bot 身份、聊天类型、真实聊天 ID 和服务器消息 ID。没有可靠 ID 的任务拒绝执行。去重记录与接收状态事务写入成功后才确认接收；状态盘写入失败时不发送成功 ACK、不启动 Agent，并保留可重试的命令状态。

状态为 queued、started、completed、failed、cancelled、interrupted。启动 Agent 前持久化 started；跨此边界后永不自动重跑，即使崩溃发生在写入与实际 prompt 之间。每条 assistant 正文 stream 的每个分段独立持久化 outbox，任务终态只保存真实 assistant 正文（无正文则为空），并与终态状态写入 durable store。保存的不只是内存队列。正常运行按接受顺序调度，同规范化目录串行；不同目录不提供重叠树、硬链接或外部终端写入隔离保证。

服务重启将遗留 queued 和 started 标记 interrupted，清除自动执行资格，在重新连接后向原聊天报告需重新发送。历史和当前绑定可恢复不意味着任务继续执行。重启会使未完成问题等待失效；去重记录仍保留，服务器重投同一消息不能重新启动副作用；用户主动发送的新消息产生新的任务。

取消是控制路径，不在普通任务队列后等待：发送 abort，等待 worker 停止；超过 10 秒关闭该 worker 进程树。只有确认进程退出且 channel 管理的执行已停止后才释放目录锁；无法确认则保持该目录阻塞并报告未知状态。取消不回滚已发生的副作用，不保证已派生到外部系统的工作停止。服务启动恢复前核查自身管理的遗留 worker；不能确认停止时不运行冲突目录的新任务。迁移操作无法确认停止时同时保留 source/target 两目录 blocked，跨重启继续生效。

Octo 断线不取消已经开始的 Agent；正文 outbox 与任务状态持久化，重连后按聊天顺序继续投递，不重跑任务。重连采用有上限退避并重新认证，重复消息经持久去重；永久认证失败停止重连并给出本地明确错误。文字和文件 outbox 按聊天顺序发送，送达未知时沿用稳定 client message ID；文件永久失败标记 dead 并报告，不阻塞后续文字回复。

外部移动恢复只接受精确 session ID 且通过 session 文件的 `previousSessionFiles` breadcrumb 证明身份；不按最近文件、不按时间、不按模糊 basename 选择。候选 roots 按 OMP native profile 与 `.env` 对当前有效 cwd 的解析结果取得，再加 channel state sessions root。候选不唯一、无法读取或无法验证时保持原绑定，不自动猜测；唯一候选通过 session header 的身份与 cwd 校验后立即持久化重绑，不启动 worker。`refreshState` 仅用于原生 `/move` 与 fresh 命令的 worker state 校验。

## 验收标准

1. 实际 Octo 私聊发起文件操作，在选定目录观察结果并收到完成回复；全程没有工具审批。
2. owner 在允许群 @Bot 和允许的 CommunityTopic 线程（channelType=5）可执行；其他成员、未允许完整 channel ID、未 @、伪造文本 @、Bot 回声均不启动 worker 或命令。
3. `/new`、`/reset` 省略目录时沿用当前绑定 cwd，首次无绑定使用 `~/tmp`；显式目录（含空格）规范化为真实目录；`/move <目录>` 使用原生 OMP `/move`，同一 session ID 保留历史与附件，非法目录或校验失败不改变绑定。
4. 首次普通消息创建 `~/tmp` 会话；从其他目录再次 `/new` 或 `/reset` 仍沿用当前绑定目录并创建 fresh session。
5. 私聊与两个群/线程的上下文不串线；交错完成的结果各自返回原聊天；群说明读取失败不启动 worker。
6. 私聊获得 `ask` 与 `octo_send_file`，群/线程额外获得 `octo_group_read`，实时读取对应服务端 `GROUP.md` 或独立 `THREAD.md`（分别对应 `groups/{id}/md` 与 `groups/{parent}/threads/{short}/md`，prompt 仍使用任务开始时冻结的快照），以及需要服务端 `bot_admin` 权限的 `octo_group_write`；工具不能改写目标聊天或 cwd。
7. 同规范化目录的两个聊天写入任务不重叠；`/new`、`/reset`、`/move` 在运行或排队时拒绝；`/move` 同时取得真实 source/target lease，未确认停止时两目录跨重启保持 blocked。
8. 长正文保留原始换行、空格和 inline 标记；每个 assistant message 独立 stream，首次非空累计快照立即 create，后续累计快照以同一 message ID 每 1000ms 最多一次串行 edit，最终更短或空快照权威生效；正文按 3000 UTF-8 bytes 稳定分段并清理 stale tail，异步 prompt 失败、本地完成和 worker 异常退出均正确结束任务状态，不生成通用完成文字。
9. 正文 outbox 在 create 前 durable 记录 `unknown`，保存 desired/acked 版本；断线、重启或送达未知恢复时沿用稳定 client ID、编辑最新快照且不重跑 Agent，不因失败重复创建。
10. 执行过程中取消能越过待执行队列；后续队列被清除，取消不被宣称为回滚。未确认停止时不启动同目录新任务。
11. `ask` 问题可跨消息持久等待，单选/多选校验、十分钟过期、跨聊天或重复回答均安全拒绝，取消不使用默认答案。
12. 在接收、started 落盘、执行、回复阶段分别终止服务再启动：绑定可恢复，历史可读，遗留任务报告中断，同消息重投不再执行副作用；外部移动只按精确 session ID 与 breadcrumb 唯一重绑，roots 依 native profile/`.env` 有效 cwd 解析，不选最近文件。
13. Octo 断开再连接不重跑任务；已持久化正文从 outbox 继续投递，送达未知明确呈现；真实 `ExecutionEvent` tracker 用一张卡累计工具/turn/todo/run 终态与每秒 elapsed，遵守 TextBlock/Container fallback、ToggleVisibility 能力和 payload/node/depth limits，卡片 edit sequence 单调递增，安全摘要不含原始 payload；typing 每 3 秒续期、活动请求 2 秒有界，终态卡片失败只做一次 best-effort，均不阻断正文。
14. 状态盘写入失败不启动新任务；同 Bot 第二实例不能启动；App Bot 配置不能被验收为支持群聊。
15. `launchd install` 仅写入权限为 `0600` 的 plist，不调用 `launchctl`；实际启动、停止分别由 `launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"` 与 `launchctl bootout "gui/$(id -u)/com.omp.channel-octo"` 完成，`launchd uninstall` 只删除 plist。环境凭证会进入该私有 plist，runtime 复用 plist 中安装时解析出的 OMP 路径；退出交互终端后仍能从实际 Octo 收发，日志不包含 token。

## 实施前提与未决事项

实现已完成。最终行为包括：`/new`/`/reset` 沿当前绑定解析目录（首次为 `~/tmp`），原生 `/move` 保持 session ID、历史和附件并通过 source/target lease 与刷新状态校验；外部移动按精确 session ID 和 `previousSessionFiles` breadcrumb 唯一发现，roots 依 native profile 与 `.env` 对有效 cwd 的解析，不按最近文件猜测。正文采用累计 snapshot、稳定 assistant message ID、3000 UTF-8 分段、1 秒串行编辑节流、最终短/空快照权威更新和 durable `unknown`/`desiredVersion`/`ackedVersion` 恢复，不重跑 Agent。`ExecutionEvent` tracker 覆盖工具、turn、todo、run 终态与每秒 elapsed，单卡 sequence、TextBlock 硬依赖及 Container fallback、ToggleVisibility 与 limits 门禁、安全摘要和有界 typing/card 活动均已纳入。

```sh
bun install
mkdir -p ~/.config/omp-channel-octo
cp config.example.json ~/.config/omp-channel-octo/config.json
chmod 600 ~/.config/omp-channel-octo/config.json
# 可按需编辑 apiUrl（缺省为 https://im.deepminer.com.cn/）、ownerId（可省略）、allowedGroupIds，并在 botToken 与 botTokenEnv 中二选一
bun run check -- --config ~/.config/omp-channel-octo/config.json
# 前台运行：
bun run serve -- --config ~/.config/omp-channel-octo/config.json
# 交由 launchd 管理时：
bun run launchd -- install --config ~/.config/omp-channel-octo/config.json
```

`botToken` 与 `botTokenEnv` 互斥；使用环境凭证时先设置 `OCTO_BOT_TOKEN`（或配置中允许前缀的变量名）。安装 launchd 后按上文命令显式执行 `bootstrap`/`bootout`；实际项目级 OMP/Octo 协议、会话迁移、正文与进度卡行为已完成实现。launchd 尚未加载；脱离终端运行和真人登录态完整自动触发回合尚未验证，仍以用户本机 Bot 配置和部署可达为前提。
首条有效私聊必须是支持的非空文本，且 `senderId`、`messageId` 非空并且发送者不是 Bot；它会原子持久化 owner 绑定和稳定去重确认，只发送确认，不执行任务。群聊和线程永远不会触发首次认领。首次认领存在风险：未配置 owner 时，第一个满足这些条件的私聊用户会成为 owner，请先确认 Bot 私聊入口的可信范围。

显式配置的 owner 是权威来源，服务启动时会持久化它；即使之后删除配置中的 owner，也不会重新开放首次认领。绑定跨重启保留，重复的首条消息不会执行任务；`check`/`serve` 在配置未提供 owner 时会提示“若状态中尚无持久 owner，将等待首次有效私聊绑定”。

本地实现还保留各阶段崩溃恢复、SQLite 故障注入、文件 outbox、`ask` 和 owner/群权限边界；状态与历史可恢复不代表已开始 Agent 自动续跑。AdaptiveCard 视觉边界仅记录真实 Octo AdaptiveCard SDK DOM：390px 宽度无横向溢出、折叠/展开可用、todo 可见、`completed`/`cancelled`/`failed` 三种终态 SDK 校验无错误；没有截图像素证据，也没有带登录态的完整 Octo 端视觉验证，不能据此宣称完整真人 UI 验收。
验收证据（仅记录已完成的最终状态）：已在真实 `https://im.deepminer.com.cn/v1/bot/register` 注册成功并验证加密 WebSocket 认证成功；真实 CLI `check`/`serve`/`SIGTERM` 生命周期与 `plutil` 校验保持有效，真实 OMP fresh worker 可成功启停。真实 OMP 经加密 Octo wire 的正文为 2 次 create、19 次 edit，最终文本逐字一致；进度为 1 张卡、18 次 edit、6 个 phase、5 个 tool、2 个 todo，typing 18 次，stop 后无新增。真实 native project 20.66s 覆盖私聊与两个群隔离、含空格目录、native `/move` 同 ID 保留历史/附件、`/new`/`/reset` 复用绑定 cwd、外部 move 自动重绑后在新项目真实写入文件以及重启保持绑定。真实 Bot HTTP 已验证正文 create 加 2 次同 ID edit、卡片 create 加 2 次同 ID edit。launchd 尚未加载，脱离终端和完整真人登录态端到端仍未验证；第 168 行所述 SDK DOM 证据不等同截图像素或完整 Octo 登录态视觉验收。
