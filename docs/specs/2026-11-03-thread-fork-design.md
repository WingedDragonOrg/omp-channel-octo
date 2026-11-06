# Channel 会话分叉与子区绑定

日期：2026-11-03
修订日期：2026-11-06

## 目标

提供 channel 内置 `/fork` 命令和 `fork` 宿主工具：通过 OMP 原生分叉能力，将当前主群或子区会话的已持久化上下文分叉为独立 session，创建新的 Octo 子区并绑定它。新子区后续对话 resume 分叉 session。

用户已确认：沿用源会话工作目录；源 agent 运行中也立即分叉，不排队、不打断源回合。

既有线程创建、权限、持久接受和会话所有权契约继续适用；本文件同时维护实现边界及隔离验收记录。

## 已核实的原生能力

本机 OMP v18.16.0：

- RPC `fork` 在源会话有用户工作或维护操作时返回 `session_busy`。空闲成功后会切换调用 worker 的当前 session，因此不适合直接用于保留源聊天身份的分叉。
- CLI 支持 `--fork`，参数为 session 文件路径或 ID；当前 `--help` 未展示此参数，但解析器和终端 `/fork pane` 均使用该入口。
- `--fork` 走 OMP 原生 `SessionManager.forkFrom`，创建新 session，不是恢复源 session。channel 不加载 OMP runtime，也不自行复制或重写 JSONL。
- 原生启动分叉使用 `repairInterruptedTail`；中间回合如何形成可继续的记录由 OMP 处理，channel 不伪造 tool result 或补写 assistant 消息。

已执行隔离 smoke：真实 OMP 连接 localhost 有限模型替身；先完成一轮使源 session 落盘，再保持第二轮模型响应未返回。期间 RPC fork 被拒绝，外部 `--fork` 成功，新 session ID 独立、cwd 相同、已落盘用户上下文存在。停止准备进程后，新进程 `--resume` 保留分叉身份和上下文；原 session 文件未被这两个外部操作改变，源回合解除等待后正常完成。未连接真实模型或注册 Octo bot。

另已观察到：全新会话首轮响应前，OMP 报告的 session 路径可能尚无文件。该情形不能声明分叉成功。

## 命令和工具

- `/fork [子区名称]`：channel 内置控制命令，整体参数作为可选子区名称，不转发为 OMP slash prompt。
- `fork` 宿主工具：以当前调用聊天为源，接受可选 `name`；与命令共用分叉和创建流程，不允许任意指定其他聊天或本机 session 文件作为源。
- 名称省略时从当前聊天名称生成简短分叉名称，沿现有线程名称规则处理。
- 入口仅在已授权主群和子区提供，命令遵循 owner 门禁；工具沿用当前任务的宿主授权，不因历史文本获得权限。
- 主群发起时，新子区属于该群；子区发起时，新子区属于同一父群，是同级子区，不创建嵌套子区。
- 不附带实现任务，不自动发送源用户 prompt，不生成摘要替代历史；创建成功后等待新子区实际收到用户消息。
- 不增加 cwd、projects、layout、worktree_name 参数。本能力只分叉上下文，工作区隔离继续使用现有 `/thread` 创建能力。

## 上下文和目录语义

1. 源以当前聊天持久绑定中的精确 session 身份为准，不选择最近会话或模糊匹配其他文件。
2. 分叉读取原生操作执行时已落盘的上下文；不承诺与用户按下发送键同一时刻的内存快照。尚未落盘内容及之后生成的消息不进入分叉。
3. 使用源 session 的真实 canonical cwd；新 session header、worker state 和目标聊天 binding 必须一致。源聊天始终保留原 session 身份与绑定。
4. 新旧会话历史在分叉后独立，文件仍是同一份；分叉不是 Git 分支、worktree 或文件副本。
5. 工作区资源归属不复制给新子区。新子区仅使用该目录，不能据此获得删除源子区资源的所有权；既有占用检查仍需识别新会话的实际使用。
6. 新子区没有显式聊天模型覆盖，沿用现有“子区显式设置 > 父群子区默认 > 不传 model”规则。不传 model 时由 OMP 决定如何采用分叉历史记录中的模型。
7. 复制历史不复制宿主权限或执行状态：源任务、待答问题、卡片动作、队列、goal 续跑授权和工作区所有权仍属于源聊天。新子区群说明/成员上下文按新身份准备，不能复用源聊天的注入回执。

## 执行流程

1. 验证源聊天权限、绑定、精确 session 文件及可用 cwd。源文件尚不存在、绑定失效或来源不明时立即给有界失败，不排队等待源回合。
2. 持久记录本次操作身份与源身份，复用命令 claim 或 native toolCallId 去重；源忙不申请其 session 执行 lease，不停止源 worker。
3. 独立受管理的 OMP 准备进程使用 `--fork` 和精确源文件启动，保持源 cwd，使用专用会话输出目录。该进程不带用户 prompt，不打开源 session 执行。
4. 读取准备进程原生状态，验证新 ID 不同、文件有效且 cwd 一致。准备进程确认退出并完成 ledger 收尾后，才允许目标聊天 resume 此 session。
5. 复用线程创建的持久意图及归属流程创建远端子区，再提交目标绑定；绑定包含分叉后的精确 session ID、文件与 cwd，不经过创建全新空会话的路径。
6. 新子区 ready 仅在原生分叉、准备进程退出、远端创建、目标绑定及必要说明全部成功后成立。返回真实子区名称、short_id、session ID 和 cwd，并提示目录共享。
7. 新子区收到消息时以分叉文件 `--resume`，按独立聊天正常调度。监听沿用默认 merge，创建方是实际发起 fork 的聊天。

创建时持久记录该子区为“沿用目录的 fork 子区”及源会话身份，以区分主动共享目录与原有 worktree 丢失的情况；不能仅凭目录当前不存在或不是 linked worktree 识别此类型。

“不排队”指不等待源 agent 回合结束；原生分叉、线程创建和持久化本身仍需实际完成后才能报告成功。

## 沿用目录的 fork 子区收尾

用户明确要求：此类子区中的 `/thread merge`、`/thread remove` 都只清理 Octo 子区。

- 两个命令均复用现有归档流程，在本子区回执及卡片输出排空后归档 Octo 子区，保留其聊天历史。
- 不执行 wt/Git 合并，不删除分支、worktree 或共用目录，不删除源 session 或 forked session 文件，不清理本地资源。
- 不触发 agent 修复任务，不发布 Git 合并成功事件；默认 merge 监听不会因为本次仅归档而向创建方发送伪合并结果。
- 回执明确说明本次仅归档子区，归档排定和实际完成按真实状态表达，不能显示“代码已合并”或“工作区已删除”。
- 沿用已有 owner 权限、当前子区运行/排队工作处置、归档取消及失败处理契约；不因目录与源聊天相同而等待源聊天任务结束。
- 此分支仅适用于持久记录明确标记的沿用目录 fork 子区。普通 worktree 子区继续遵循现有 merge/remove 资源处理契约，不能把工作树丢失作为跳过资源清理的依据。

## 失败、恢复与安全边界

- 源操作不受分叉成败影响，不切换其 session，不重放、取消或追加原 prompt。
- 重复命令或工具调用复用同一操作记录及实际结果，不产生多份远端子区。
- 准备进程退出未确认时，新 session 不对执行开放；沿现有 watchdog、ledger 和生命周期安全处理。
- 分叉文件生成但远端创建失败，或远端已创建但绑定未完成时，保留已知资源与准备状态，回执报告实际成功部分，不回退为普通空会话。
- 重启不猜测远端创建是否成功。复用现有创建意图的失败关闭原则；结果不明时停止自动分配，不重新创建另一子区。
- 只清理本次操作明确拥有、未被使用的准备资源；不清理源文件、源工作目录或其他操作的资源。
- 历史可能包含来源群的信息。该能力保持现有 owner 控制与目标子区访问规则，不主动扩展成员、跨群传播或向其他群分叉。

具体操作记录、阶段字段和清理实现应在实施时复用现有生命周期所有者；不能依赖进程内 map 作为重启后的唯一依据。

## 验收标准

1. 主群和子区都能通过命令及工具创建同父群的新子区；源与目标 session ID 不同、cwd 相同，没有新建 worktree。
2. 源真实模型请求保持进行时，fork 无需等源回合结束；源身份不变、不中断，之后仍正常完成。
3. 新子区能 resume 分叉 session 并读取已落盘历史，源 fork 后的新增回复不自动进入目标历史；不得仅证明文件存在。
4. 准备进程退出确认先于新子区 worker 启动，不发生同一分叉 session 双进程占用。
5. 首轮未落盘、缺失绑定、错误 cwd、原生分叉失败、远端创建失败和部分完成均有真实有界结果，不创建伪 ready 或空会话 fallback。
6. 重复投递与重启不重复创建；接受状态不明保持失败关闭，源任务不被重放。
7. 源待答卡片不能回答新子区任务；上下文注入、模型策略及共享目录清理占用边界符合现有契约。
8. 使用临时 profile、真实 OMP、localhost 有限模型及本地 Octo fixture 验证完整 fork → 创建 → resume；覆盖命令和回合内宿主工具两条入口。schema 或生命周期变化须按项目要求演练迁移并执行离线重启基准。
9. 沿用目录 fork 子区的 `/thread merge` 和 `/thread remove` 都仅归档 Octo 子区；源及目标 session 文件、共用目录、Git 分支保持不变，源运行中任务不受影响。普通 worktree 子区的命令行为不变，不产生伪 Git 合并通知。

## 实施设计与风险边界

### 单一职责

- worker 工厂通过 `WorkerOptions.forkFrom` 启动 `--fork <精确文件>`，沿用现有 RPC 状态读取、进程组、watchdog 和 worker ledger。分叉启动不发送 `new_session`、`--resume` 或用户 prompt。
- fork 编排负责精确来源验证、操作去重、原生准备、远端创建和结果恢复；命令与工具仅提供当前聊天及操作身份。
- store 持久保存来源、准备目录、新 session、远端 thread、阶段和有界错误；ready 发布与目标 binding、默认 listen、创建意图完成在同一事务提交。
- 归档沿用现有 receipt barrier、正文和卡片排空、新入站取消及失败处理；fork 类型来自持久记录，不来自 Git/cwd 探测。

### 持久阶段

`preparing → prepared → creating → remote-created → ready` 表示已确认推进的结果；失败保留已知 session/thread 及失败原因，不删除这些实际结果。

1. `preparing` 在启动原生进程前写入，`preparation_started` 在调用准备工厂前持久标记派发意图。候选 ID/文件在停止前保存，但只有身份核验、进程退出确认和 ledger 收尾后才成为 `prepared`；进程退出不明时不发布目标绑定。
2. `creating` 与既有线程创建意图在远端请求前持久化。请求结果不明时不再次分配远端资源；无已知绑定的同群子区继续受到创建意图门禁保护。
3. 成功收到远端身份后立即记录 `remote-created`。之后只恢复该 thread 的说明及本地绑定，不再次 fork 或 create。
4. 目标说明成功且候选 session 仍可验证后，事务发布 `ready`。即使某次异常留下 binding，未 ready 的 fork 记录仍阻止普通任务或会话修复将其当作可执行会话。

目标发布使用能够插入完整聊天绑定的事务路径，不能对尚不存在的目标只调用 session 字段更新。若目标已有不同 binding，拒绝覆盖。既有创建意图的终结证据须显式支持同一操作的 ready fork 记录，不能要求 fork 伪造 workspace 归属。

重启首先执行既有 worker ledger 恢复，再处理 fork 记录，最后开放任务。没有已确认新 session 的准备操作不重新猜选会话文件；没有已确认远端身份的创建操作不猜选子区。已知部分结果在回执中列明，不能以空 session 替代。

准备恢复按 bot 和操作 ID 精确读取包含 stopped 行的 worker ledger。未派发且无 ledger 可终结失败；已派发但 ledger 缺失则仍属未知，保持阻塞。ledger 使用本次操作的独立身份，准备文件尚未知时 `session_file=NULL`，不冒用源 session。已确认准备结束、远端尚未派发且没有后续恢复入口的操作保留候选结果并终结意图，不永久占用共用目录。`remote-created` 继续只恢复同一已知子区。

正式 Bot API 的明确永久拒绝可终结未分配意图；HTTP 5xx、`store_failed`、传输中断或来源不明的响应不证明远端未分配，继续保持未知结果门禁。失败回执携带已知候选 session、真实子区 short_id 和 cwd。

`worker_ledger.session_file` 的 nullable 迁移在单一事务重建表，保留原进程身份、状态、迁移目标和时间戳。新 `thread_forks` 及派发标记遵循应用实际打开路径的幂等迁移。

### 授权、调度与关闭

命令在既有 owner/allowlist 门禁后直接执行，不加入源任务 FIFO；工具的来源固定为调用任务所属聊天的当前精确 binding。子区来源只取持久 channel id 中的父群，目标始终同群。名称只按既有 printable 名称边界验证。

准备进程不取得源 session 的执行租约；其 ledger 和独立输出目录只属于本次操作。准备期间登记共用路径与 canonical worktree root 的共享使用，可继续服务于 standalone `/commit` 与 host-tool `thread-delete` 的既有资源保护，但不因在途 fork 准备阻挡 `/thread remove` 或 `/thread merge`，也不形成普通任务间目录排他。merge 不按工作区资源限制并发。取消信号覆盖 worker 创建和握手期间，生命周期所有者确认本次 fork 准备进程停止并收尾自己的 ledger 后才释放其资源使用记录，不能只让等待者提前返回。取消/停机终止本次准备与远端 I/O，不因分叉去中断源回合；未知的旧准备状态只影响该 fork 自身恢复与分配，不成为 remove/merge 的资源门禁。

仅归档命令检查本子区的运行/排队工作，不取得共用 cwd 的 Git 排他，也不等待其他聊天的独立源 session。源模型设置、goal 授权、ask/card、注入账本和 workspace 归属不复制；目标后续 worker 沿现有模型解析及自身上下文路径启动。

## 隔离验收

- native fork、store、命令/工具、仅归档及重启边界均有 TDD 回归，覆盖退出确认、派发意图、已派发但无 ledger、HTTP 503 分配不明、部分结果回执与回执送达前的延期重启保护。fork 专项为 57 pass、0 fail；`bun run typecheck` 通过。
- 全量 `bun test --timeout 30000` 为 1,340 pass、0 fail，覆盖 97 个文件、8,318 条断言；真实 Git/进程集成测试使用 30 秒单测预算，未跳过用例。
- native fork 的真实子进程夹具使用 5 秒启动预算，完整源身份与 tracked close/ledger 用例使用 10 秒总预算；保留源文件内容、会话身份、取消时限、进程退出及台账收尾断言。
- `bun tests/benchmarks/fork.ts` 使用安装版真实 OMP、完整 channel CLI、临时 profile、loopback 有限模型和本地加密 Octo fixture，四条主群/子区 × 命令/工具路径均通过。4 次目标真实 resume 读取继承历史，4 个准备进程均先退出确认，准备 prompt 和自动派发均为 0；源身份及 held 期间文件字节变动为 0。2 次原生 fork 工具调用，共 14 个有限模型请求。
- fork 子区 `/thread merge`、`/thread remove` 各执行一次真实 channel→Octo fixture 归档路径；子区记录与历史保留，无 DELETE，source/target session 文件、共用目录和 Git 分支不变，无伪合并事件。
- `bun tests/benchmarks/restart.ts` 的 graceful/SIGKILL 场景均通过：恢复 accepted 与 queued 任务各 2 个，session overlap、重复副作用和中断通知均为 0。
- 对真实状态库只读一致性快照执行应用迁移并连续打开两次：原有 25 张表、11,073 行的每个原列逐行摘要保持一致，`integrity_check=ok`；确认 worker ledger 文件列可空、fork 派发标记存在，未遗留迁移临时表。快照和演练脚本已移除。
- 验收只使用一次性目录、Git 仓库、profile 和本地服务；未注册真实 Bot，未操作真实源 session 或常驻服务。本工作区未提交、推送或合并。
