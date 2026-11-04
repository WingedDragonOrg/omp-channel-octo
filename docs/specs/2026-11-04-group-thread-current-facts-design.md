# 群与子区当前事实清单

日期：2026-11-04
状态：已实现并通过隔离验收。

## 目标与存放位置

当前任务需要持续保留的约束、决策与状态，直接维护在当前聊天已有说明中：群使用 GROUP.md，子区使用 THREAD.md。

```markdown
### Memory Start ###

{内容}

### Memory End ###
```

## 操作方式

复用现有 `octo_management`：

1. 用 `description-read` 读取当前完整说明。
2. 只在两个标记之间更新或追加当前事实；没有区段时，在文末建立该区段。
3. 保留区段外的全部原文，再用 `description-write` 写回完整说明。

固定区段由 agent 按约定维护，内容直接使用 Markdown；owner 可要求修改、删除某段或清空。宿主在现有说明写入工具上执行以下前置校验。

## 写入前置条件

- 当前任务必须先成功调用 `description-read`；自动注入的说明、其他聊天或上一任务的读取不能代替这次工具读取。
- 宿主保存读到的正文和版本。每次 `description-write` 前重新读取真实说明，两者有任何变化就拒绝，提示重新 `description-read`，不提交旧正文。
- 一次写入尝试消耗这次读取凭据；成功写入后或写入失败后，再次写入都必须重新读取。
- 同一工具实例内的说明读写依次执行，避免两个并行写调用复用同一次读取。其他管理动作不进入这条队列。
- 这是宿主写前校验，不是服务端原子 CAS。已核对的群与子区说明写接口只接收 `content`，没有期望版本条件；其他客户端仍可能在最后一次 GET 与 PUT 之间写入。若要消除该窗口，需要服务端支持条件更新，本仓库不宣称已经实现。

接口依据：[群说明 `updateGroupMd`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/bot_api/groups.go)、[子区说明 `botUpdateThreadMd`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/bot_api/threads.go)。

## 作用域与上下文

- owner 已选择当前聊天独立：群只使用本群 GROUP.md，子区只使用本区 THREAD.md；不自动读取父群、上浮内容或同步到兄弟区。
- 固定操作指引提供上述维护方式，现有聊天授权和服务端实际权限保持不变。
- Memory 内容随当前聊天完整说明加载；持久写入和会话刷新是两个独立操作。
- 新 session 的首次模型输入读取最新说明，作为原生持久会话消息注入。普通回合沿用已加载内容，不查询远端版本，也不添加或移除 system 中的动态说明。
- compact 边界通过宿主读取当前聊天最新说明；成功读取后加入压缩输入与恢复数据，失败不能以旧 sidecar 冒充最新读取，也不能清空已保留说明。
- `description-read` 始终实时读取，工具结果自然追加到当前对话，保留上述写前凭据规则。`description-write` 不主动刷新会话。
- `/move` 和服务重启恢复保留同一 session 的说明状态；`/new`、fork 新 target 使用各自精确目标和 session，不自动继承父群或兄弟区说明。
 
## 会话上下文稳定性 SDD

### 所有权与不变量

- 宿主负责成员 API、说明 API、权限、网络预算与取消；OMP extension 不持有 Octo token，不启用网络 listener。
- 身份与固定操作指引属于固定 system；普通输入、工具续调、原生 `/retry` 和 accepted 任务恢复使用相同位置及内容。CLI 身份真实就绪状态可改变对应提示；版本、时间和 attempt ID 不进入固定指引。
- 聊天上下文按 `(botUid, channelType, channelId, exact sessionId)` 隔离；worker 内非绑定的 task/eval session 不自动取得 bot/chat 指引、成员或说明。父任务主动传递的业务内容属于显式输入。
- 成员快照属于消费它的用户输入，写入原生会话历史；首次输入附加一次，后续仅在语义变化时附加。成员按 UID 与名称规范排序比较，API 顺序不形成变化。
- 历史成员块不可移动或替换；工具续调、重试和恢复不在请求末尾另拼成员块。steering 未消费前不提交其快照，消费后与对应输入一起保留；相同文本的多个输入仍按输入顺序区分。
- 沿用原生 slash 首行分派、增量群历史水位和原生 retry，不为续接添加文本前缀，也不重放已接受的原始输入。

### 实施接缝

- `WorkerContextScope.cliReady` 与 bot/target 一起传给当前 worker，宿主逐 worker 生成 CLI `--append-system-prompt`；不把聊天 guide 放入 factory 的全局 extra args。真实请求验收必须分别触发 task 与 eval child。
- RPC `input` hook 仅变换当前绑定 session 即将写入历史的普通用户正文，slash 命令完整原文保持不变。宿主先准备成员，extension 按已消费历史与待交付输入的 message ID 去重；拒绝某条 steering 不回退其他输入的消费位置。大群仍只显示人数、查询入口和规范化名单指纹，以检测人数不变的换人或改名。
- 普通首次说明使用 `before_agent_start` 的持久 CustomMessage。无 prompt ticket 的原生 goal 或跳过该 hook 的续接，在首次 `context` 边界按 matching 原生历史/receipt 单飞确认持久消息，固定恢复其位置，不重放用户输入。compact fresh read 使用 `session_before_compact` 的取消信号；读取成功后 `session.compacting` 将完整快照放入原生 compaction entry 的 `preserveData`，仅压缩成功提交后才成为新会话说明。
- OMP 当前不向 extension 提供手动压缩的 post-success 回调。恢复以已提交的原生 compaction entry 为真相，在 compaction summary 后固定位置呈现该快照；直到下一次 compact，正文和位置保持不变。未提交或失败的压缩不发布新快照，不在每次请求尾部动态补灌。
- 自动说明带结构化 bot/target/session 身份。fork 保留原生历史文件及显式用户业务正文；目标会话的请求投影排除来源聊天自动说明与成员后缀，使用目标自己的新快照。
- 自定义 compaction hook 跳过 `session.compacting` 时，fresh 候选暂存在 sidecar，并记录压缩前 branch leaf。仅在原生 branch 确认该 leaf 之后出现已提交 compaction entry 后，候选才转为绑定该 entry 的恢复快照；失败或未提交不会发布。

### 持久化与失败边界

- 复用现有 sidecar、prepared receipt、说明 ledger 与原生会话消息；不引入通用记忆系统。receipt 证明精确 scope/session 的上下文已准备，不把写入成功视为模型已接受。
- 宿主刷新请求必须携带精确 scope/session 与本次请求身份，有限截止且可取消；已过期或其他 session 的响应不能提交。
- 读取失败、压缩失败和停机保留已确认的历史与说明，不伪装成成功刷新。恢复保留 exact-session 排他与受管 worker 退出确认，不增加普通目录排他。
- 本次不新增 SQLite schema；若实施发现必须变更，须先说明必要性并在 throwaway 数据库演练。

### 验证

- 实施前实际观察 consumer-visible 失败回归，再做最小实现；删除已过时的临时上下文形状测试。
- 使用真实已安装 OMP、隔离 profile/session、有限 localhost 模型与本地 Octo fixture 捕获实际 provider 请求。验证 system 和已有消息前缀、成员输入归属、retry/恢复、实际 compact fresh read、主动 reread、task/eval 隔离及取消。
- 执行定向 context/member/RPC/core 回归、typecheck、全量测试与离线 restart benchmark；生命周期变化补真实 CLI smoke。请求前缀比较不代表真实 provider cache 命中率。

## 验收

- 检查真实模型请求能获得维护指引，工具仍使用已有的说明读写操作。
- 未读取就写、读取后正文或版本已变化、重复使用读取凭据、并行写调用必须拒绝不合格写入；重新读取后可正常写入。
- 用隔离 localhost 模型、真实 OMP 和本地 Octo fixture 驱动完整说明读取与写回；观察固定区段内变更、区段外原文保留、群/子区范围，以及新 session / compact 刷新和普通回合延迟可见。
- 运行现有群上下文、compaction、会话和权限回归，以及 typecheck、全量测试和离线重启验收。合成模型 smoke 证明接线与注入，不替代真实模型遵循操作约定的质量评估。

仅修改 omp-channel-octo。对象监控独立交付；本任务不自动提交、推送、合并或重启常驻服务。

## 先前说明写入验收记录

- `bun run typecheck` 通过；定向说明写入、host tools、群上下文与 extension 回归共 47 项通过。
- `bun test --timeout 30000`：1393 pass、0 fail，104 个测试文件，8578 条断言。全量运行使用 30 秒单测预算。
- 真实已安装 OMP 18.16.0、有限 localhost 模型和本地 Octo fixture 完成 15 次实际模型请求：验证未读写入被拒绝、原说明区段外原文保留、群/子区/兄弟区隔离、当前回合快照固定与下一回合版本注入。
- 通过原生 `compact` RPC 完成实际压缩：2 次摘要模型请求、1 条持久 compaction 记录；压缩后和空闲 SIGKILL 重启后的实际模型输入仍取得本区 Memory 内容。
- 离线 `tests/benchmarks/restart.ts` 的 graceful/SIGKILL 两场景均续接 accepted 工作并恢复排队任务；会话重叠、重复副作用与中断通知均为 0。
- 验收使用临时配置、会话与合成内容，未调用真实 provider 或注册常驻 Bot；一次性 smoke 脚本、请求捕获与临时数据已移除。

## 会话上下文稳定性验收记录

- 实施前已实际观察普通回合说明刷新、成员块位置变化和固定 guide 缺失的失败回归；补充 slash 原文、大群身份变化及 fork 精确作用域回归。
- `bun run typecheck` 通过。最终全量使用独立临时目录执行 `bun test --timeout 30000`：**1503 pass、0 fail**，112 个测试文件，8920 条断言。独立 presentation / 真实 CLI lifecycle 检查 27 项通过。
- `bun tests/benchmarks/context.ts` 使用真实已安装 OMP 18.16.0、隔离 profile/session、有限 localhost 模型和本地加密 Octo fixture，完整通过 **29 次实际 provider 请求**。
- 普通工具续调、成员 API 重排与 SIGKILL 后原生 retry 的 system 消息 SHA-256 均为 `9eb43f71bd1a0c9f27eb43de0d86617824b8f217de17e2a8a8e7a5ea6f89837c`，guide 位于唯一 system 消息的索引 0；对应既有消息前缀分别保留 13、15、25 条。原生 goal 自身使用独立的 OMP goal system，固定 channel guide 保持一致且说明取得 committed receipt。
- 实际 native compact 通过隔离验收扩展调用 `ctx.compact()`，产生 conversation、split-turn、short-summary 共 3 次有限摘要请求及 1 条已提交 compaction 记录。完整最新 Memory/说明进入原生 `preserveData`；后续普通请求与工具续调中的说明固定在消息索引 2，UTF-8 正文 295 字节，不自动重读远端说明。
- compact 后外部说明再次更新，普通请求仍保留压缩时快照；`description-read` 的真实工具结果读取到新内容。真实 task 与 eval child 请求均未自动继承父聊天 guide、成员或说明；fork 目标保留显式业务历史，使用自己的新说明和成员，不使用来源自动快照。
- 最终 `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两场景均通过：accepted 任务续接 2、排队任务恢复 2，会话重叠、重复副作用和中断通知均为 0。
- 未新增 SQLite schema；说明写前读取凭据与权限合同保持不变。验收只操作一次性本地资源；未调用真实付费 provider、未注册或重启常驻 Bot，未提交、推送、合并或部署。
- 这些请求证据证明拼装、位置和恢复行为，不代表真实 provider 的 cache 命中率或模型遵循 Memory 约定的质量。
