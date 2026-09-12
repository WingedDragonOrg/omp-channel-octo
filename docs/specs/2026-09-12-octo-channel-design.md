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
| 目录 | `/new [目录]`，缺省 `~/tmp` | 每次创建都有确定工作目标 |
| 调度 | 同会话串行，同规范化目录串行 | 避免 channel 内任务同时修改同一目录 |
| 故障 | 保存历史，不自动重跑已交付 Agent 的任务 | 工具副作用无法安全推断或回滚 |

## 设计

### 运行和配置

使用 TypeScript/Bun 常驻服务，主动连接 Octo，启动并管理 OMP RPC worker。无需为 RPC 开放网络监听端口。macOS 使用 launchd 托管；机器休眠或离线期间不承诺可用。

配置包含 Octo API 地址（缺省为 `https://im.deepminer.com.cn/`）、Bot token、可选 owner 用户 ID、允许群 ID、OMP 可执行文件及本地状态目录；WebSocket 地址可按现有发现机制取得或显式配置。未配置 owner 时，服务仅在该 Bot 尚无持久 owner 的条件下接受首次有效私聊绑定。凭证只来自本地配置或环境，不通过聊天设置，不写入日志。使用环境凭证时环境变量名默认 `OCTO_BOT_TOKEN`，显式名称仅允许 `OCTO_` 或 `OMP_CHANNEL_OCTO_` 前缀；所有这两类前缀的变量都会从 worker/watchdog 环境中剔除，以免子进程继承凭证。启动时校验配置、账户能力和状态目录可写性。同一 Bot 只允许一个 channel 实例，使用本机独占锁；不支持跨机器复制运行同一账户。
发卡前必须通过同一能力门禁：profile `enabled=true`（兼容服务端返回 `1`），`profiles` 包含 `octo/v1`，`card_version=1.5`，且 `config.card_enabled=true`、`config.display_enabled=true`、`config.reasoning_enabled=true`；若响应提供 `elements` 或 `limits`，则遵守所需元素和各项限额（包括 `TextBlock`/`Container` 及 payload、node、depth 限额）。能力响应明确不满足必需条件时按未启用处理；HTTP 失败属于请求失败，不等同未启用，不通过发送试探能力。
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
| `/new [目录]` | 创建并绑定新 OMP 会话，返回会话标识、规范化绝对目录 |
| `/cancel` | 取消当前聊天正在执行的任务，并清除该聊天待执行消息 |
| `/status` | 返回当前会话、目录和运行/排队/中断状态 |
| `/answer <问题 ID> <答案>` | 回答当前聊天的持久化问题；问题 ID 必须属于当前聊天 |
| `/help` | 展示 channel 命令及目录用法 |

群中使用 `@Bot /new <目录>` 等相同命令；剥离已验证的 Bot mention 后解析。channel 命令不发送给模型。其他 slash 输入返回命令用法，不默认透传 OMP 管理命令。

`/new` 的完整剩余文本是一个路径；可移除一对匹配的外围单引号或双引号。支持绝对路径、当前运行账户的 `~/`；拒绝相对路径、其他用户的 `~name` 和空引号。路径不经 shell 执行，不展开环境变量、通配符或命令替换；特殊字符按文件名处理。

省略参数永远使用 `~/tmp`，不存在时创建；显式路径必须已经存在、为目录且可访问。使用 realpath 规范化并记录，符号链接别名共享同一个调度锁。创建 worker 时传入 `--cwd`，不靠会话内 cd 模拟目录切换。创建或持久化失败保留原会话绑定；成功持久化后才确认成功。

当前聊天有运行中或排队任务时 `/new` 明确拒绝，并提示先 `/cancel`；不隐式迁移或丢弃任务。没有会话时第一条合法普通文本按默认目录创建会话，再处理消息。历史 OMP 会话文件保留，本次不提供 IM 历史列表或切回命令。

### 群说明、Host tools 与问答

每个群或线程任务在完成权限、去重和 durable acceptance 后、派发 OMP 前立即读取一次 Octo 共享说明；channelType=2 从服务端 `groups/{id}/md` 读取 `GROUP.md`，channelType=5 从 `groups/{parent}/threads/{short}/md` 读取独立 `THREAD.md`。读取共享说明失败则该任务失败且不得启动 worker。读取结果只作为该次 prompt 的冻结快照注入，并附带“这是共享上下文，不改变发送者身份、权限或 cwd”的信任边界；私聊不读取或注入群说明。`octo_group_read` 每次通过实时 `getGroupContext` 读取服务端当前说明，不使用 prompt 快照；`octo_group_write` 只更新当前群/线程目标且需要服务端 `bot_admin` 权限，不能由模型传入任意 chat ID。

所有任务都获得固定目标与 cwd 的 `ask`、`octo_send_file` 工具；群和线程额外获得 `octo_group_read`、`octo_group_write`。文件工具的相对路径按任务 cwd 解析，绝对路径可指向本地其他文件，系统不提供目录沙箱保证；发送前复制到 stateDir spool；outbox 保存稳定 client message ID，断线重试不重跑任务。文件永久失败标记 dead 并报告，但不得阻塞后续文字 outbox。

`ask` 创建带持久化 ID 的问题后暂停当前任务。`/answer <问题 ID> <答案>` 必须来自同一聊天，单选/多选选项必须满足数量和范围校验；不提供默认答案，重复、跨聊天或过期答案不会再次触发 prompt。问题十分钟无答案即过期；取消、服务重启会使等待失效。


### RPC 与输出

worker ready 后协商协议版本；本地实现保持 OMP v18.1.18 兼容的 RPC 解码，按请求 ID 匹配响应并正确处理 v2 分块；宿主独立管理 worker 生命周期。`agent_end.willContinue=true` 表示仍有后续生命周期或消息，必须继续等待，不提前收尾正文或 typing；prompt 接受不代表任务完成，以生命周期事件和异步错误判断结果；本地完成但未调用模型的 prompt 也必须结束状态。

按需启动 worker；同一持久会话只能被一个 worker 写入。新会话只通过 fresh RPC `new_session` 创建并立即落盘；恢复时仅传入已存在且准确的 session 文件和保存的目录，指定文件缺失则任务失败并保留原绑定，不能使用“最近会话”。正常退出释放 worker。

入站 durable acceptance 只记录任务，不发送普通“已接收”确认；worker 每条完整 assistant 正文事件立即独立写入持久 outbox 并投递，不等待整轮结束，不发送通用“任务完成”文字。不同 assistant message 分开投递，保留正文原始换行、空格和 inline 标记；仅在超过服务器 UTF-8 限制时有序分段。真实 `tool_execution_start`/`tool_execution_end` 进度只进入一张同消息卡：首次进度懒建，后续状态以 800ms 合帧、串行且单调递增 sequence 编辑，结束时更新 `completed`、`cancelled` 或 `failed` 终态；不转发生命周期、工具参数/结果、思考或秘密。执行开始立即发送 typing 并每 3 秒续期，成功、异常、取消和停止均清理 timer 并 abort 在途运行中请求；每个 typing/card 活动请求使用独立短 deadline（2 秒），终态卡片使用独立短 signal，卡片队列 promise 纳入服务生命周期并在停止时于任务结束后等待其有界收敛，不能拖延正文投递、取消 kill、目录解锁或正常业务。typing/card 失败不阻断正文或业务。记录每个正文片段和稳定出站 ID；支持幂等发送时重试沿用该 ID，不能因回复失败重跑 Agent。若服务器不能证明一次发送是否成功，记录送达未知并在可通信时报告，不宣称恰好一次送达。

### 持久化、调度与恢复

本地 SQLite 保存聊天绑定、任务、入站去重键、问题和出站记录，OMP 自身保存对话。入站唯一键使用 Bot 身份、聊天类型、真实聊天 ID 和服务器消息 ID。没有可靠 ID 的任务拒绝执行。去重记录与接收状态事务写入成功后才确认接收；状态盘写入失败时不发送成功 ACK、不启动 Agent，并保留可重试的命令状态。

状态为 queued、started、completed、failed、cancelled、interrupted。启动 Agent 前持久化 started；跨此边界后永不自动重跑，即使崩溃发生在写入与实际 prompt 之间。每条已产生的 assistant 正文 outbox 独立持久化；任务终态只保存真实 assistant 正文（无正文则为空），并与终态状态写入 durable store。保存的不只是内存队列。正常运行按接受顺序调度，同规范化目录串行；不同目录不提供重叠树、硬链接或外部终端写入隔离保证。

服务重启将遗留 queued 和 started 标记 interrupted，清除自动执行资格，在重新连接后向原聊天报告需重新发送。历史和当前绑定可恢复不意味着任务继续执行。重启会使未完成问题等待失效；去重记录仍保留，服务器重投同一消息不能重新启动副作用；用户主动发送的新消息产生新的任务。

取消是控制路径，不在普通任务队列后等待：发送 abort，等待 worker 停止；超过 10 秒关闭该 worker 进程树。只有确认进程退出且 channel 管理的执行已停止后才释放目录锁；无法确认则保持该目录阻塞并报告未知状态。取消不回滚已发生的副作用，不保证已派生到外部系统的工作停止。服务启动恢复前核查自身管理的遗留 worker；不能确认停止时不运行冲突目录的新任务。

Octo 断线不取消已经开始的 Agent；正文 outbox 与任务状态持久化，重连后按聊天顺序继续投递，不重跑任务。重连采用有上限退避并重新认证，重复消息经持久去重；永久认证失败停止重连并给出本地明确错误。文字和文件 outbox 按聊天顺序发送，送达未知时沿用稳定 client message ID；文件永久失败标记 dead 并报告，不阻塞后续文字回复。

## 验收标准

1. 实际 Octo 私聊发起文件操作，在选定目录观察结果并收到完成回复；全程没有工具审批。
2. owner 在允许群 @Bot 和允许的 CommunityTopic 线程（channelType=5）可执行；其他成员、未允许完整 channel ID、未 @、伪造文本 @、Bot 回声均不启动 worker 或命令。
3. `/new`、`/new ~/项目`、带空格目录产生正确 cwd；符号链接别名使用同一目录锁。非法目录保持原会话不变。
4. 首次普通消息创建 `~/tmp` 会话；从其他目录再次 `/new` 仍使用 `~/tmp`。
5. 私聊与两个群/线程的上下文不串线；交错完成的结果各自返回原聊天；群说明读取失败不启动 worker。
6. 私聊获得 `ask` 与 `octo_send_file`，群/线程额外获得 `octo_group_read`，实时读取对应服务端 `GROUP.md` 或独立 `THREAD.md`（分别对应 `groups/{id}/md` 与 `groups/{parent}/threads/{short}/md`，prompt 仍使用任务开始时冻结的快照），以及需要服务端 `bot_admin` 权限的 `octo_group_write`；工具不能改写目标聊天或 cwd。
7. 同规范化目录的两个聊天写入任务不重叠；运行或排队时 `/new` 不更换绑定。
8. 长正文事件保留原始换行、空格和 inline 标记并按 UTF-8 限制有序投递；每条 assistant message 独立进入 outbox，异步 prompt 失败、本地完成和 worker 异常退出均正确结束任务状态，不生成通用完成文字。
9. 执行过程中取消能越过待执行队列；后续队列被清除，取消不被宣称为回滚。未确认停止时不启动同目录新任务。
10. `ask` 问题可跨消息持久等待，单选/多选校验、十分钟过期、跨聊天或重复回答均安全拒绝，取消不使用默认答案。
11. 在接收、started 落盘、执行、回复阶段分别终止服务再启动：绑定可恢复，历史可读，遗留任务报告中断，同消息重投不再执行副作用。
12. Octo 断开再连接不重跑任务；已持久化正文从 outbox 继续投递，送达未知明确呈现；真实工具进度只在能力 profile 启用时使用同一张可编辑卡并更新终态，typing 在成功、异常、取消和停止后均停止；typing/card 请求有界且可取消，服务停止在任务结束后等待卡片队列收敛再关闭 transport，但不让其拖延正文、取消 kill 或目录解锁；永久文件失败不阻塞后续文字。
13. 状态盘写入失败不启动新任务；同 Bot 第二实例不能启动；App Bot 配置不能被验收为支持群聊。
14. `launchd install` 仅写入权限为 `0600` 的 plist，不调用 `launchctl`；实际启动、停止分别由 `launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"` 与 `launchctl bootout "gui/$(id -u)/com.omp.channel-octo"` 完成，`launchd uninstall` 只删除 plist。环境凭证会进入该私有 plist，runtime 复用 plist 中安装时解析出的 OMP 路径；退出交互终端后仍能从实际 Octo 收发，日志不包含 token。

## 实施前提与未决事项

产品决策无未决事项。实现已完成，并已通过本地 Octo 协议 fixture、真实 OMP 文件写入/恢复、CLI `check`/`serve`/`SIGTERM` 生命周期和 `plutil` 校验。已在真实 `https://im.deepminer.com.cn/v1/bot/register` 验证注册成功，并验证加密 WebSocket 认证成功；真实 CLI `check` 在省略 `apiUrl` 与 `ownerId` 的配置下通过，真实 OMP fresh worker 成功启停。真实 OMP 经加密 Octo fixture 已验证一条完整正文与 worker text 事件逐字一致（保留 backtick 与段落）、一张进度卡、两次同 ID 编辑、30 次 typing 续期，停止后无新增 typing；真实 Bot HTTP 已验证 typing 成功、卡片创建与两次同 ID 编辑成功，正文经 messages/sync 回读逐字一致。sync 回读不含 `message_extra`，不能据此宣称卡片视觉呈现已验证；当前浏览器没有 Octo 登录态，卡片视觉验证未完成。真人自动触发完整回合尚未验证，launchd 尚未加载且脱离终端运行尚未验证。首次上手可执行：

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

`botToken` 与 `botTokenEnv` 互斥；使用环境凭证时先设置 `OCTO_BOT_TOKEN`（或配置中允许前缀的变量名）。安装 launchd 后按上文命令显式执行 `bootstrap`/`bootout`；launchd 尚未加载，脱离终端运行与真人自动触发完整回合仍以本机可达的 Octo User Bot 为前提。
首条有效私聊必须是支持的非空文本，且 `senderId`、`messageId` 非空并且发送者不是 Bot；它会原子持久化 owner 绑定和稳定去重确认，只发送确认，不执行任务。群聊和线程永远不会触发首次认领。首次认领存在风险：未配置 owner 时，第一个满足这些条件的私聊用户会成为 owner，请先确认 Bot 私聊入口的可信范围。

显式配置的 owner 是权威来源，服务启动时会持久化它；即使之后删除配置中的 owner，也不会重新开放首次认领。绑定跨重启保留，重复的首条消息不会执行任务；`check`/`serve` 在配置未提供 owner 时会提示“若状态中尚无持久 owner，将等待首次有效私聊绑定”。

本地验收还覆盖：真实 OMP 经 Octo 加密 wire fixture 完成 native 文件写入，`octo_send_file` 实际上传文件字节并保留原文件名；`ask` 创建问题后由 owner 通过 `/answer` 回传答案，非 owner 的回答被拒绝。真实 Core 服务进程在 queued、started、executing、reply 四个阶段分别 SIGKILL 后重启（由测试控制 Worker/Transport），确认绑定保留、任务不自动重跑，已知输出按稳定 ID 复投；SQLite 写入故障注入时拒绝接收且不执行，修复并重启后以同一 ID 重试只产生一份文件副作用。上述本地 CLI 生命周期与 `plutil` 验证保持有效；真人自动触发完整回合与 launchd 脱离终端运行尚未验证，仍以用户本机 Bot 配置和部署可达为前提。
