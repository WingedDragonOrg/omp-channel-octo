# OMP Octo 个人远程助手设计

日期：2026-09-12
修订日期：2026-09-15
状态：产品设计已确认

## 背景与目标

通过 Octo 私聊和指定群聊操作本机 OMP。使用独立于终端的持久会话，保持用户当前全自动执行习惯。channel 是常驻消息服务，OMP 是通过 stdio RPC 驱动的子进程；不依赖模型 API auth-gateway。

## 范围

包含个人单 Bot、单服务实例、文本指令与回复、执行状态、新建会话、指定目录、取消、会话恢复、消息去重和连接重建。支持私聊、指定群及 CommunityTopic 线程（channelType=5）内明确 @Bot 的指令；支持群/线程共享说明、出站文件和持久化问答。

不包含终端会话接管、多用户服务、通用网络 Agent gateway、文档评论任务、服务端通用 bot_task、历史群消息采集、通用附件接收和交互 callback 系统。channel 仅提供由 OMP host tool 明确发起的出站图片/文件发送，以及由能力 profile 明确启用的 Octo raw type17 `octo/v1` AdaptiveCard 1.5 非交互进度展示。收到不支持的消息类型应明确告知，不作为成功执行。范围以个人 IM 交互为准，不继承参考适配器的其他任务入口。

## 已知约束与资料

- 本机当前 OMP v18.1.21 提供 `--mode rpc`、`--cwd`、`--resume`、`--approval-mode yolo`、`--extension` 及 `session.compacting` 生命周期能力；本地兼容解码器按 v18.1.18 保持 RPC 兼容。RPC 提供 prompt、abort、状态和会话事件；启动 ready 不代表模型调用可用。
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
| 共享说明 | 按持久 OMP session 与聊天目标绑定 version；首次或 version 变化时注入，普通未变化 turn 不注入 | 降低重复输入并保持群/线程隔离；压缩生命周期单独负责恢复 |

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
| `/restart [now]` | 通过部署方配置的重启命令安全重启 channel；省略参数时在有任何运行/排队任务时拒绝；`now` 强制重启并中断在途任务 |
| `/help` | 展示 channel 命令及目录用法 |

当前聊天有待回答问题时，owner 在该聊天的非命令回复就是答案本身：合法选项编号按选项校验，其他文本原样作为自填答案；没有待回答问题时同一文本按普通任务输入处理。

`/restart` 只在配置了 `restart.command`（非空字符串数组，例如 `["super","-y","restart","omp-channel-octo"]`）时可用；未配置时明确回复未配置且不猜测重启机制。命令由 channel 以独立会话派生，进程停止由该命令对应的进程管理器负责（Super 的 `restart` 会先给 channel 发 SIGTERM），channel 自己不会在命令未停止它时擅自退出，只记录一条告警。发起的聊天先收到“正在重启 channel”，请求被持久化；进程恢复后在原聊天投递“channel 已重启完成：PID …，启动于 …”。重启时被中断的在途任务发送“channel 正在重启，本会话任务已中断；重启完成后会在本会话确认。”，未开始的排队任务在下次启动按 interrupted 报告。重启完成通报超过 10 分钟未投递则丢弃，不对未发生的重启做确认。

群中使用 `@Bot /new <目录>`、`@Bot /reset <目录>` 或 `@Bot /move <目录>` 等相同命令；剥离已验证的 Bot mention 后解析。命令与 mention 的先后顺序不固定：客户端可能发送 `@Bot /command <参数>` 或 `/command <参数> @Bot`，解析前剥离全部已验证的 Bot mention，无论它位于命令前、命令后还是参数中间，并清理剥离处残留的空白。结构化 span 与文本对不上或缺失时，退回按 mention uid 列表匹配独立出现的 `@BotName` 或 `@BotUid` 词元（两侧为空白或文本边界，允许尾随冒号）；未指向当前 Bot 的文本型 `@` 不剥离，用户正文中的其他 `@` 保持原样。命令只允许 owner 发起：群、线程和私聊中其他成员的命令一律拒绝，既不执行也不回复。channel 内置 `/new`、`/reset`、`/move`、`/cancel`、`/status`、`/help`、`/restart` 始终硬拦截，不发送给模型；若存在同名观察器也不能改变这一安全语义。其他 slash 输入按原始 `message.text` 作为普通 OMP task 透传，未知命令不再返回“不支持的命令”。
可通过服务选项注册自定义 `ChannelCommandHandler`：`name` 去除首尾空白并按小写规范化，`intercept` 明确决定是否消费，`handle(context)` 接收原始 `InboundMessage`、规范化命令名和解析后的 argument。`intercept=true` 在持久命令去重后执行 handler，成功后消费且不启动 OMP；失败保持可重试的命令状态且不启动 OMP。`intercept=false` 先完成现有任务 durable acceptance，再调用 handler 并继续透传原始文本；handler 异常不得阻断任务或 prompt，重复消息不得重复 handler 或 task。handler 必须经过既有 owner、群/线程 mention 和消息 ID 校验；空名、非法名或规范化后的重复注册在创建服务时拒绝，不能静默选择。

`/new`、`/reset` 的完整剩余文本是一个路径；可移除一对匹配的外围单引号或双引号。支持绝对路径、当前运行账户的 `~/`；拒绝相对路径、其他用户的 `~name` 和空引号。路径不经 shell 执行，不展开环境变量、通配符或命令替换；特殊字符按文件名处理。无绑定时省略目录首次解析为 `~/tmp`，已有绑定时省略目录沿用其 cwd。

`/move` 必须提供目标路径，按同样的安全规则解析；它只在当前绑定存在精确可恢复 session 文件时执行。channel 先取得并持有真实源目录与目标目录 lease，调用 OMP 原生 `/move <目录>`，再通过 `refreshState` 校验 session ID、session 文件和规范化目标 cwd，成功持久化后才更新绑定。源目录或目标目录有运行/排队任务、未确认停止的 worker 或 lease 冲突时拒绝，不隐式迁移或丢弃任务。

显式目录必须已经存在、为目录且可访问。使用 realpath 规范化并记录，符号链接别名共享同一个调度锁。创建 fresh worker 时传入 `--cwd`，不靠会话内 cd 模拟目录切换；创建或持久化失败保留原会话绑定。`/new`、`/reset`、`/move` 成功后才确认成功。

当前聊天有运行中或排队任务时 `/new`、`/reset`、`/move` 明确拒绝，并提示先 `/cancel`；没有会话时第一条合法普通文本按默认目录 `~/tmp` 创建会话，再处理消息。历史 OMP 会话文件保留，本次不提供 IM 历史列表或切回命令。

原生迁移的 worker 若无法确认停止，源目录和目标目录都持久标记为 blocked；服务重启恢复 worker ledger 时仍同时阻塞两目录，直到停止状态可确认。外部移动不通过最近文件猜测，而在后文定义的精确身份规则下自动重绑。

### 群说明、Host tools 与问答

每个群或线程任务在完成权限、去重和 durable acceptance 后，检查一次当前 Octo 共享说明的 version；channelType=2 从服务端 `groups/{id}/md` 读取 `GROUP.md`，channelType=5 从 `groups/{parent}/threads/{short}/md` 读取独立 `THREAD.md`。读取共享说明失败则该任务失败且不得启动 worker。读取结果规范化为带 `present`、完整 `content` 和按目标稳定且不可复用的 `version` 的快照；空内容是 `present=false` 的有效快照，必须保留其 server version。初次从未有说明的 404 可规范化为只用于初始无说明状态的 version 0 哨兵；已有 server version 的目标删除时必须由服务端返回新的删除 version，不能用无 revision 的 404 假装 version 未变化。

共享说明状态按 `botUid`、真实聊天目标和持久 OMP session 隔离。worker `create` 完成并由 `refreshState` 确认真实 OMP session ID 后，宿主才发布该 session 的受控 context sidecar；任务或绑定阶段产生的占位 session ID 不能作为 sidecar 或 marker 的身份。sidecar 只包含目标、真实 session ID、当前 turn 的 immutable snapshot、注入决策和不含秘密的 injection attempt ID，不包含 Bot token，extension 不得按 cwd、全局文件或模糊 session 回退。

新建 session 在第一次 agent start 注入完整的最新共享说明。恢复已有 session 时，若持久 marker 已证明同一 session、目标和 version 已成功提交，则不重复注入；没有 marker 或 marker 处于 pending 时按首次注入处理。普通 turn 只有在当前 version 与已提交 marker 不同时才注入，注入内容为完整最新快照，并附带版本变化说明；不生成 old/new diff。普通 `worker.prompt` 只发送用户消息，群说明由固定 OMP extension 的 `before_agent_start` 生命周期从 sidecar 以 system-level context 注入；extension 的输入必须校验 Bot、目标和真实 session ID，缺失或不匹配时 fail closed。

注入采用 `pending`/`committed` 两阶段。宿主先原子发布 sidecar 和 pending 状态；extension 在返回 system-level context 前，以 injection attempt ID 原子写入同 sidecar 作用域的 `prepared` receipt。receipt 只证明 extension 已校验目标并生成了指定上下文，不代表模型已经接受；receipt 缺失、过期或绑定不匹配时，`before_agent_start` 必须失败，OMP 不得进入模型调用。宿主观察到 matching receipt 和 OMP prompt acceptance 后，才以同一 attempt ID 做 CAS，把该 version 标为 committed；未确认 acceptance 的 pending 在下一次任务中重放，不能静默跳过。正常成功路径同一 version 只注入一次；进程在 acceptance 与 marker 提交之间崩溃时按保守的 at-least-once 重放，以优先保证不丢共享说明。

OMP `session.compacting` 生命周期在压缩时把当前 turn sidecar 中的完整共享说明和 version 边界加入压缩上下文，即使 version 未变化；这是为保证压缩后仍保留 group/thread 语义的恢复性注入例外，不推进普通 turn 的 committed marker。当前 turn 的 sidecar 在 `octo_group_write` 后保持 immutable，compaction 使用本 turn 已建立的快照；写入成功的新 version 只在下一 turn 重新读取并发布。恢复后的下一次普通 turn继续依据 committed marker 判断，不因 compaction 再次重复注入。GROUP.md/THREAD.md 中的内容是共享上下文，不改变发送者身份、权限或 cwd。

`octo_group_read` 每次通过实时 `getGroupContext` 读取服务端当前说明，不使用 prompt 或 compaction 快照；`octo_group_write` 只更新当前群/线程目标且需要服务端 `bot_admin` 权限，不能由模型传入任意 chat ID。写入成功后，后续任务和 extension sidecar 使用新的 version；当前 turn 已经建立的 context 不回写。私聊不读取或注入群说明，也不获得群说明工具。

所有任务都获得固定目标与 cwd 的 `ask`、`octo_send_file` 工具；群和线程额外获得 `octo_group_read`、`octo_group_write`。文件工具的相对路径按任务 cwd 解析，绝对路径可指向本地其他文件，系统不提供目录沙箱保证；发送前复制到 stateDir spool；outbox 保存稳定 client message ID，断线重试不重跑任务。文件永久失败标记 dead 并报告，但不得阻塞后续文字 outbox。

`ask` 创建带持久化 ID 的问题后暂停当前任务，并给出一段包含问题正文、编号选项（如有）和回复规则的文本。只要该聊天存在待回答的问题，owner 在该聊天的下一条非命令文本即为该聊天最早待回答问题的答案：合法选项编号按单选/多选的数量和范围校验，其他文本原样作为自填答案返回给模型；命令仍然优先。不提供默认答案，无待回答问题、不同聊天、重复或过期后的回复都不作为答案，其中无待回答问题和过期后的文本按普通任务输入处理。问题十分钟无答案即过期；取消、服务重启会使等待失效。


### RPC 与输出

worker ready 后协商协议版本；本地实现保持 OMP v18.1.18 兼容的 RPC 解码，按请求 ID 匹配响应并正确处理 v2 分块；宿主独立管理 worker 生命周期。`agent_end.willContinue=true` 表示仍有后续生命周期或消息，必须继续等待，不提前收尾正文或 typing；prompt 接受不代表任务完成，以生命周期事件和异步错误判断结果；本地完成但未调用模型的 prompt 也必须结束状态。

按需启动 worker；同一持久会话只能被一个 worker 写入。新会话只通过 fresh RPC `new_session` 创建并立即落盘；对已有绑定，恢复前先按 session ID 与 `previousSessionFiles` breadcrumb 在 native roots 中寻找并验证唯一外移候选，找到后持久化重绑；扫描或验证失败则保留原绑定并报告失败，不能用“最近会话”替代精确身份。恢复 worker 时仅传入已经确认存在且准确的 session 文件和保存的目录。正常退出释放 worker。
OMP extension 在 worker 启动时由固定路径自动加载；sidecar 根路径通过宿主显式 allowlist 的非秘密 worker 输入传给 extension，sidecar 在真实 session ID 确认后、第一次 `prompt` 前原子发布，extension 在生命周期回调中按该 ID 精确读取并写入 `prepared` receipt。宿主必须能观察 matching receipt 和 prompt acceptance，并以 attempt ID 做单调 CAS；worker 停止、重启、恢复和 compaction 不得绕过 `pending`/`committed` 判定。宿主不向 extension 暴露 Octo 认证凭证，extension 不得按 cwd、全局路径或模糊 session 查找 sidecar。

`WorkerEvent` 的正文事件为稳定的本地 `messageId`、累计正文快照和 `final` 标记；上游没有原生 ID 时由 adapter 生成。每个 assistant message 独立维护 stream，首次非空快照立即创建一条正文消息，后续累计快照只编辑同一个远端 message ID；不同 assistant message 不合并。`final=true` 是该消息的权威结束快照，允许比此前更短或为空；已有远端消息必须编辑到最终空文本，尚未创建且从未有非空正文的空最终事件不发送空白新消息。

正文增量可先于 `message_start` 到达；迟到或重复的开始事件复用当前尚未结束、上游身份兼容的 stream，保留累计正文和本地 message ID。消息结束后或出现明确不同的上游身份时才分配下一条消息 ID。

正文编辑以最新累计快照为准，首次创建不等待整轮结束；编辑最多每 1000ms 一次并按目标串行，最终快照优先完成，不得由旧快照覆盖新快照。正文按 3000 UTF-8 bytes 稳定分段，保留原始换行、空格和 inline 标记；分段 stream 使用稳定的 client message ID，分段数量变化时清理 stale tail。每次网络操作前持久化该分段的目标快照及 `desiredVersion`/`ackedVersion`；首次 create 在网络请求前 durable 标为 `unknown`，送达未知时不重跑 Agent，重连/重启沿用稳定 ID 绑定已创建消息并将最新 desired 快照补做 edit。

真实 `tool_start`/`tool_end`、`turn_start`/`turn_end`、`todo_update` 和 `run_end` 事件由单一 `ExecutionTracker` 累计阶段、工具、todo、终态和 elapsed；真实工具或 todo 进度首次出现时懒建一张可编辑卡，之后只编辑该卡。运行中 elapsed 只在真实事件到达时随卡片刷新，事件编辑遵守 1000ms 合并窗口，无事件时不做周期重绘；卡片 edit 使用严格递增的单卡 sequence，终态为 `completed`、`cancelled` 或 `failed`。卡片采用“摘要头部—当前状态—任务清单—子代理—执行轨迹”的进度通知布局；任务清单按阶段→任务的树形层级渲染，使用分支连接线和嵌套区块表达父子关系；任务节点只显示状态图标、任务内容和 blocker，不重复渲染状态文字；富布局下阶段标题与任务分支使用结构化 ColumnSet 分列，避免依赖比例字体空格对齐；富布局能力下使用强调色 Container、状态/耗时摘要和带状态元数据的步骤面板，轨迹步骤使用 Web 端兼容的 ColumnSet，轨迹行使用固定 24px 等宽标记列和 stretch 正文列，状态摘要置于正文列并允许自然换行，避免窄屏下正文被状态列挤压；状态图标与步骤内容、状态摘要同处彩色背景 Container 内；子代理列表位于执行轨迹之前；当对应区块超过三个步骤且能力支持 `Action.ToggleVisibility` 时，在标题行右侧提供“展开”/“收起”控件，默认展示最近三个步骤，其余步骤通过“展开”查看；能力支持 `Action.ToggleVisibility` 时，卡片头部始终提供“收起”/“展开”整卡详情控件：运行中默认展开任务、子代理和执行轨迹详情，收起后只保留摘要头部、当前状态与告警；终态卡片默认收起这三类详情，保留状态摘要，并可通过头部的“展开”恢复；状态标记按语义统一：成功/已完成使用绿色 `✓`，等待、取消和阻塞使用中性灰色 `-`，失败和放弃使用红色 `×`，进行中使用蓝色/Accent `•`；取消任务内容在支持 `RichTextBlock` 时使用删除线；能力不足时保持信息完整的降级文本。内置 `hub`/IRC、`task`、`todo` 和 `ask` 工具使用操作级安全摘要，IRC 轨迹显示方向、目标或协作状态，不显示消息正文；命令类工具显示压成单行的有界命令文本摘要，便于在轨迹里直接看到所运行的命令。卡片只展示安全摘要，不转发工具参数/结果、原始 payload、模型思考或秘密。超出渲染预算且仍可展开时，轨迹与子代理都使用同一组紧凑 ColumnSet 行，状态标记列与 stretch 摘要列保持一致；不支持展开时只保留安全的隐藏计数。紧凑行的等待字形后使用不换行空格，避免客户端将 `- ` 解析成 Markdown 列表。
富布局下子代理区块只将仍在运行或等待的子代理作为默认丰富行；已完成、失败、取消或停止的子代理默认隐藏，能力支持 `Action.ToggleVisibility` 时随“展开”以紧凑行查看。执行轨迹默认展示最近三个工具步骤的丰富行，其余步骤即使展开也只使用紧凑文字行。当前 OMP worker 以通用 RPC `tool_execution_start`/`tool_execution_end` 形成安全工具事件：`bash` 的摘要是压成单行、截断到 72 字符的命令文本，路径类工具只取安全文件名组件，其余工具按操作语义派生；命令文本中的凭据形态字符串由卡片层统一脱敏，宿主始终不转发原始 args/result。OMP 另有可选的 `set_subagent_subscription` 生命周期/进度事件桥，但它不属于 `pi.on` 专用 hook，且尚未纳入本服务的 `WorkerEvent` 卡片契约。
任务清单发生预算裁剪时保留隐藏任务摘要；支持 `Action.ToggleVisibility` 时，标题行显示“展开”/“收起”按钮，展开后按原阶段和原任务顺序将被裁剪任务插回树形位置，并复用富布局任务行的状态标记、颜色、blocker 与层级连接；若卡片节点预算不足，先按 8/4/2 收缩可展开隐藏行的渲染行数（任务隐藏行上限不超过 4），保留隐藏数量提示，仍不足时再按既有顺序裁剪可见工具行与任务行；能力不支持该动作时只保留安全的隐藏计数。

发卡前必须通过能力 profile 门禁：profile 启用，包含 `octo/v1` 与 card version 1.5，且 card/display/reasoning 开关均启用；`TextBlock` 是唯一硬依赖，`Container` 等布局元素缺失时使用扁平 fallback；`Action.ToggleVisibility` 仅在能力声明支持时渲染。遵守服务端声明的 payload、node、depth 等 limits，无法满足时不发卡且不阻断正文；同一执行最多一张卡。

执行开始立即发送 typing 并每 3 秒续期，成功、异常、取消和停止均清理 timer 并 abort 在途运行中请求；每个 typing/card 活动请求使用独立 2 秒 deadline，终态卡片使用独立短 signal。运行中卡片失败可按有界节流重试；终态 edit 失败只做一次 best-effort，随后 dispose 卡片活动，不无限重试。卡片队列 promise 纳入服务生命周期并在停止时于任务结束后等待其有界收敛，不能拖延正文投递、取消 kill、目录解锁或正常业务。typing/card 失败不阻断正文或业务。

### 持久化、调度与恢复

本地 SQLite 保存聊天绑定、任务、入站去重键、问题和出站记录，OMP 自身保存对话。入站唯一键使用 Bot 身份、聊天类型、真实聊天 ID 和服务器消息 ID。没有可靠 ID 的任务拒绝执行。去重记录与接收状态事务写入成功后才确认接收；状态盘写入失败时不发送成功 ACK、不启动 Agent，并保留可重试的命令状态。
共享说明的持久 marker 与 sidecar 状态属于 SQLite 中的 session context ledger，至少记录 `botUid`、真实聊天目标、真实 OMP session ID、当前 server version、`pending`/`committed` 阶段和 injection attempt ID。marker 的提交必须是同 session/目标/version 的 CAS；同一 pending 在重放前沿用其 attempt ID，提交后不得再次接受旧 attempt。新 session 不能继承旧 session marker，外部移动若保持同一真实 session ID 则保留 marker，`/new`/`/reset` 产生新 session 时清空其 marker。sidecar 与 marker 发布、extension 注入确认和 `octo_group_write` 的后续 version 发布均不得以占位 session ID 完成。

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
5. 私聊与两个群/线程的上下文不串线；交错完成的结果各自返回原聊天；同一持久 session 在共享说明 version 不变时连续任务不重复注入，version 变化时只注入一次新的完整快照并给出变化说明；共享说明读取失败不启动 worker。
6. 私聊获得 `ask` 与 `octo_send_file`，群/线程额外获得 `octo_group_read`，实时读取对应服务端 `GROUP.md` 或独立 `THREAD.md`（分别对应 `groups/{id}/md` 与 `groups/{parent}/threads/{short}/md`），以及需要服务端 `bot_admin` 权限的 `octo_group_write`；新建和无可靠 marker 的恢复 session 首次注入共享说明，普通未变化 turn 不注入；工具不能改写目标聊天或 cwd。
7. 同规范化目录的两个聊天写入任务不重叠；`/new`、`/reset`、`/move` 在运行或排队时拒绝；`/move` 同时取得真实 source/target lease，未确认停止时两目录跨重启保持 blocked。
8. 长正文保留原始换行、空格和 inline 标记；每个 assistant message 独立 stream，首次非空累计快照立即 create，后续累计快照以同一 message ID 每 1000ms 最多一次串行 edit，最终更短或空快照权威生效；正文按 3000 UTF-8 bytes 稳定分段并清理 stale tail，异步 prompt 失败、本地完成和 worker 异常退出均正确结束任务状态，不生成通用完成文字。
9. 正文 outbox 在 create 前 durable 记录 `unknown`，保存 desired/acked 版本；断线、重启或送达未知恢复时沿用稳定 client ID、编辑最新快照且不重跑 Agent，不因失败重复创建。
10. 执行过程中取消能越过待执行队列；后续队列被清除，取消不被宣称为回滚。未确认停止时不启动同目录新任务。
11. `ask` 问题可跨消息持久等待；owner 在同一聊天的下一条非命令文本被当作答案（编号选项按单选/多选校验，其他文本原样返回），不同聊天、无待回答问题、重复或过期后的文本都不作为答案并按普通任务处理，取消不使用默认答案。
12. 在接收、started 落盘、执行、回复阶段分别终止服务再启动：绑定可恢复，历史可读，遗留任务报告中断，同消息重投不再执行副作用；外部移动只按精确 session ID 与 breadcrumb 唯一重绑，roots 依 native profile/`.env` 有效 cwd 解析，不选最近文件。
13. Octo 断开再连接不重跑任务；已持久化正文从 outbox 继续投递，送达未知明确呈现；真实 `ExecutionEvent` tracker 用一张卡累计工具/turn/todo/run 终态与 elapsed，只在真实事件到达时随卡片刷新（编辑按 1000ms 合并窗口推进，无周期重绘），遵守 TextBlock/Container fallback、ToggleVisibility 能力和 payload/node/depth limits，卡片 edit sequence 单调递增，安全摘要不含原始 payload 与参数对象（命令类工具显示单行、截断到 72 字符并经凭据脱敏的命令文本）；typing 每 3 秒续期、活动请求 2 秒有界，终态卡片失败只做一次 best-effort，均不阻断正文。
14. 状态盘写入失败不启动新任务；同 Bot 第二实例不能启动；App Bot 配置不能被验收为支持群聊。
15. `launchd install` 仅写入权限为 `0600` 的 plist，不调用 `launchctl`；实际启动、停止分别由 `launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"` 与 `launchctl bootout "gui/$(id -u)/com.omp.channel-octo"` 完成，`launchd uninstall` 只删除 plist。环境凭证会进入该私有 plist，runtime 复用 plist 中安装时解析出的 OMP 路径；退出交互终端后仍能从实际 Octo 收发，日志不包含 token。
16. 新建 session 的第一次 agent start 会注入目标的完整共享说明；恢复 session 只在没有可靠的同 session/目标/version committed marker，或 marker 仍处于 pending 时补注入；已 committed 的恢复不会因重新启动 worker 自动重复注入。
17. 同一 session 的共享说明 version 变化时，OMP 收到一次完整最新快照和明确的版本变化说明；删除也按带 server version 的变化处理并明确当前无说明；version 不变的普通 turn 的 OMP prompt 不包含共享说明正文。
18. OMP compaction 后，压缩上下文保留当前 turn immutable snapshot 的完整共享说明和 version 边界；即使 version 不变也只在该 compaction 生命周期中恢复性注入，下一次普通 turn 不重复注入；当前 turn 的 `octo_group_write` 不改写这份 snapshot。
19. 共享说明 sidecar、持久 marker 和 extension 输入按 Bot、真实聊天目标与真实 OMP session 隔离，不包含 Bot token；extension 或 sidecar 目标不匹配、输入不完整或 session ID 未确认时在进入模型前拒绝共享上下文；marker CAS 失败时保留 pending、不得标为 committed，并在下一任务重放。
20. OMP prompt acceptance 与 context marker 提交之间发生崩溃时，pending 状态可恢复并在下一任务重放，不能静默跳过；正常成功路径不重复注入同一 version，重放使用明确的 injection attempt ID。
21. `/restart` 只使用部署配置的 `restart.command`，未配置时明确拒绝且不猜测重启机制；存在运行或排队任务时默认拒绝并提示 `/cancel` 或 `/restart now`；`now` 先持久化重启请求再执行命令，发起聊天收到“正在重启 channel”，进程恢复后在发起聊天与被打断会话投递重启完成通报，超过 10 分钟的旧通报不投递；重启命令未停止进程时 channel 继续运行并记录告警。真实 CLI 端到端覆盖命令执行、进程自行以 0 退出以及重启后的确认投递。

## 实施前提与未决事项

既有实现已完成 `/new`/`/reset` 目录绑定、原生 `/move`、精确外部 session 恢复、正文 outbox、执行 tracker、卡片和生命周期边界；共享说明本次采用 session/version/OMP lifecycle context 设计，相关实现状态以本规格新增验收和代码为准。
未决事项：无。本次规格只确认共享说明的 session/version/lifecycle 语义；实现阶段须按新增验收落地并补充真实 OMP compaction、删除 version、崩溃重放和凭证隔离验证。

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

真实卡片视觉验收使用独立命令 `bun run test:preview-card`，仅在显式指定该命令时向真实 Octo 会话发送一张综合预览卡片，包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）、4 个阶段和 12 个 Todo 任务，覆盖成功、等待、失败、取消和进行中状态；它读取 `BOT_TOKEN` 或 `OCTO_BOT_TOKEN`，目标优先读取 `OCTO_PREVIEW_TARGET_UID` 或 `OCTO_OWNER_ID`，未指定时使用 Bot 注册响应中的 owner UID，并可选读取 `OCTO_API_URL` 与 `OCTO_PREVIEW_CHANNEL_TYPE`（`1`、`2` 或 `5`）。普通 `bun test` 不执行该真实发送测试。
`botToken` 与 `botTokenEnv` 互斥；使用环境凭证时先设置 `OCTO_BOT_TOKEN`（或配置中允许前缀的变量名）。安装 launchd 后按上文命令显式执行 `bootstrap`/`bootout`；实际项目级 OMP/Octo 协议、会话迁移、正文与进度卡行为已完成实现。launchd 尚未加载；脱离终端运行和真人登录态完整自动触发回合尚未验证，仍以用户本机 Bot 配置和部署可达为前提。
首条有效私聊必须是支持的非空文本，且 `senderId`、`messageId` 非空并且发送者不是 Bot；它会原子持久化 owner 绑定和稳定去重确认，只发送确认，不执行任务。群聊和线程永远不会触发首次认领。首次认领存在风险：未配置 owner 时，第一个满足这些条件的私聊用户会成为 owner，请先确认 Bot 私聊入口的可信范围。

显式配置的 owner 是权威来源，服务启动时会持久化它；即使之后删除配置中的 owner，也不会重新开放首次认领。绑定跨重启保留，重复的首条消息不会执行任务；`check`/`serve` 在配置未提供 owner 时会提示“若状态中尚无持久 owner，将等待首次有效私聊绑定”。

本地实现还保留各阶段崩溃恢复、SQLite 故障注入、文件 outbox、`ask` 和 owner/群权限边界；状态与历史可恢复不代表已开始 Agent 自动续跑。AdaptiveCard 视觉边界仅记录真实 Octo AdaptiveCard SDK DOM：390px 宽度无横向溢出、折叠/展开可用、todo 可见、`completed`/`cancelled`/`failed` 三种终态 SDK 校验无错误；实时预览命令覆盖带登录态的真实卡片发送，截图像素与完整真人交互仍需单独观察，不能混同。
验收证据（仅记录已完成的最终状态）：已在真实 `https://im.deepminer.com.cn/v1/bot/register` 注册成功并验证加密 WebSocket 认证成功；真实 CLI `check`/`serve`/`SIGTERM` 生命周期与 `plutil` 校验保持有效，真实 OMP fresh worker 可成功启停。真实 OMP 经加密 Octo wire 的正文为 2 次 create、19 次 edit，最终文本逐字一致；进度为 1 张卡、18 次 edit、6 个 phase、5 个 tool、2 个 todo，typing 18 次，stop 后无新增。真实 native project 20.66s 覆盖私聊与两个群隔离、含空格目录、native `/move` 同 ID 保留历史/附件、`/new`/`/reset` 复用绑定 cwd、外部 move 自动重绑后在新项目真实写入文件以及重启保持绑定。真实 Bot HTTP 已验证正文 create 加 2 次同 ID edit、卡片 create 加 2 次同 ID edit；实时预览验收命令已成功发送一张包含 11 次 tool 调用（`read`、`write`、`edit`、`bash`、`task`、`subagent_await`、`subagent_stop`、`hub`、`ask`）、4 个阶段、11 个 Todo 任务及全部状态的综合预览卡片。launchd 尚未加载，脱离终端和完整真人登录态仍未验证；第 168 行所述 SDK DOM 证据不等同截图像素或完整 Octo 登录态视觉验收。
