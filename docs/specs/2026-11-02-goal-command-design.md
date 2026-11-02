# Octo 持续目标命令设计

日期：2026-11-02
状态：已实现并通过隔离验收

## 目标与范围

在 Octo 当前聊天的持久 OMP 会话中，通过 owner-only `/goal` 管理和执行原生持续目标。使用 OMP 18.15.0 的结构化 goal RPC；channel 拥有执行归属和持久接纳，目标内容、状态、预算核算和自动续跑归 OMP。不得将命令作为普通模型 prompt。

## 命令契约

| 命令 | 行为 |
| --- | --- |
| `/goal`、`/goal show` | 查询绑定原生会话，显示 objective、goal ID、状态、tokensUsed、tokenBudget、timeUsedSeconds；没有目标时明确反馈 |
| `/goal set [--budget N] <目标>` | 创建并启动；预算为正整数 token，创建后按原生规则累计。已有目标时拒绝，提示先 drop |
| `/goal resume` | 恢复原生 paused 目标并持续执行；已由本聊天执行时仅报告当前状态 |
| `/goal pause` | 撤销续跑授权、取消尚未执行的目标启动/恢复请求，原生 pause 停止后续续跑；当前回合可完成 |
| `/goal drop` | 撤销续跑授权、取消尚未执行的目标启动/恢复请求，并原生 drop；命令自身就是删除确认，当前回合可完成 |
| `/goal budget [N\|off]` | 查看预算；修改/清除明确反馈当前原生 RPC 不支持，不删除重建来替代预算调整 |

预算不是模型单次请求的硬上限。用量采用 OMP 的 input + cacheWrite + output，不包含 cacheRead。达到预算后的 budget-limited 状态如实展示；不能通过普通消息或无效 resume 绕过。

## 启用与调度

owner 的 set/resume 显式授权当前会话自动执行。仅目标执行 worker 使用进程级配置 overlay 将 goal.continuationModes 设置为包含 rpc；不写全局设置，不覆盖 goal.enabled:false。拒绝或缺少原生接口时给出明确错误。

启动/恢复作为 durable task，继承 owner、允许列表、mention、message ID 去重、聊天串行、聊天/会话与工作区资源租约和 worker 台账。普通任务正忙时启动请求排队；重复 resume 不制造第二个运行。show/pause/drop 是控制操作，直接使用当前活 worker；空闲则持同一聊天/会话及所属工作区资源租约打开绑定会话，不使用 fresh 临时会话代替。普通目录的不同会话不因 cwd 相同互相排队，已归属工作区仍按资源串行；目录不可用、会话或工作区占用、旧 worker 退出未确认时明确反馈。

显式 resume 在没有活跃目标任务时核对精确原生会话状态，不以缓存 active/paused 决定实际目标是否存在；查询期间的新停止意图使本次核实失效。目录忙且已知目标为 paused 时可保留安全的持久排队路径，派发仍须核对 exact goal ID。目标控制与执行 worker 均继承本聊天已指定的模型，覆盖会话记录中的模型。

普通消息仍遵循 steer/queue，待答 ask 的文字回复优先作为答案。show/pause/drop 不回答 ask。pause/drop 不强行中止当前模型或工具；立即中断使用 interrupt，取消并清队列使用 cancel。所有显式停止先写持久意图、撤销目标续跑授权；pause/drop/interrupt 不清除普通排队任务，cancel 按既有规则清除全部排队任务。停止后普通消息、迟到答案、重启均不恢复目标，需显式 resume。

## 原生执行生命周期

结构化 create/resume 可以自行发起 agent_start，没有 prompt ticket 或 prompt_result。worker 必须在派发前安装 host tools、上下文和事件消费者，捕获目标接受回执和全部后续消息。goal_updated 及 RPC 查询是状态来源；channel 不从助手正文推测目标完成。

一次目标执行覆盖多次 agent_start/agent_end；整段执行等待 session_settled，不能以 willContinue 或 isTerminal 判断整段结束。单轮完成不清 host tools、不关闭 ask、不结束任务卡、不释放会话/工作区租约或 worker 所有权。原生静止后查询最终目标状态：目标完成/删除与目标仍存在必须明确区分，未达成不能显示“目标已完成”。原生因无进展、blocked 或预算限制不继续时，结束当前执行并保留目标、如实反馈；收拢 worker 前将仍 active 的未完成目标暂停，避免后续普通消息隐式恢复。

目标状态、持续执行授权和 task 终态分开建模。原生目标 ID、会话 ID 和本聊天 task 构成归属；channel 只持久化控制请求、原生接受事实、目标身份和恢复授权，不复制原生 goal journal 或预算引擎。command acceptance 与 agent_start 分开；缺少接受事实的已派发操作 fail closed，不凭 objective 匹配/重新 create。

每个结构化启动/恢复请求在 RPC 写入之前，同步持久化操作类型、原会话身份及 dispatched 标记。goal_accepted 回调同步提交 accepted 与 exact goal ID；agent_start 另记实际生成接受，不替代控制接受。恢复优先处理停止意图，dispatched 但无 accepted 的请求 fail closed；只有 dispatched=false 的请求才能正常从队列执行。不得先发 RPC 再补派发标记。

fresh 会话在接收时可有 channel 占位 session ID；只允许尚未派发的 create 在一个事务内把 binding、task、目标投影和请求提交为 OMP 实际 session ID。派发后及恢复时禁止换绑。原生进度可能先于控制响应到达：接受前缓冲，goal_accepted 同步落盘后才按 exact goal ID 提交进度，进度本身不充当接受回执。

## 重启与停止

在途目标跨 channel 重启自动续跑。先确认旧 worker/process group 退出，再打开 exact 原会话，核对 exact goal ID。已接受的中断回合优先交给原生 retry；若没有可重试回合，不发通用 continuation prompt，而是恢复原生目标。重试后目标仍未完成且授权有效时调用原生 goal resume。已完成/删除则结算，不创建替代目标；目标身份不符或接受状态不明则 fail closed。恢复流使用新的正文/通知 namespace。

recover 保留原先已证实的控制接受及 goal ID，另记恢复派发与新进程的生成接受。原生 retry 可以直接完成目标，或者恢复时查询已确认原目标不存在；这些路径不制造新的 goal_accepted 回执，也不要求再次 create/resume。原生恢复为 paused 的快照不等同于 owner 撤销授权；只有控制意图和执行最终结算改变恢复授权。

retry 继承 OMP 对失败工具批次的原生重试语义；不能对任意非幂等外部副作用承诺通用 exactly-once。channel 不重发原始目标、不额外重试工具，并保留精确任务/会话/目标身份及原生工具回执。重启基准中的“无重复副作用”是该固定可审计工作负载的实测结果。

显式 pause/drop/interrupt/cancel 与 ask 超时先持久撤销目标恢复授权，优先于重启恢复。暂停后的当前回合如果尚未结束就进程退出，不自动恢复该目标任务。ask 的题目、迟到回答和现有卡片行为保持原契约，但迟到回答不重新授权目标续跑。new/reset 不迁移目标到 fresh 会话；move 仍保留原会话身份并遵守既有忙碌拒绝、聊天/会话及工作区资源租约和停止确认。普通任务的恢复规则不变。

## 边界与实现归属

- commands 负责参数、状态反馈及即时控制；service 负责 durable task 接纳、恢复授权与 native run 的调度。
- worker 负责结构化协议校验、goal 操作、真实静止边界及 retry/resume 次序；进程终止仍由 worker lease 独占。
- store 持久化目标控制操作和接受/停止事实，迁移对旧任务无语义改变。已排队目标启动/恢复请求必须能按聊天单独取消，不影响普通任务。
- `goal_tasks` 记录结构化请求、精确会话/目标身份、派发与两类接受事实；`goal_sessions` 记录原生当前状态投影及独立的持续授权/停止意图。原生确认完成、删除或不存在后移除当前投影，任务审计保留，同一会话可以继续新建目标。
- 原任务已终态但 worker lease 尚未完成停止确认时，不把该 worker 当作活控制会话；控制操作明确反馈目录忙。新目标可以 durable 排队，但必须等前一个 worker 完整收拢才执行。
- owner-only 路由在资源组装和任务接纳前生效；控制命令不下载附件。状态文字使用既有安全、单行和长度界限。

## 验收

1. 新建（含预算）、查看、暂停、恢复、删除在真实绑定会话中生效；不支持的预算修改/替换明确拒绝且不改目标。
2. 两个及以上原生回合、完成、无进展、预算耗尽及失败正确收尾；第一轮结束后 worker、host tools、会话/工作区租约仍归同一任务。
3. busy/queue/steer、重复消息/重复 resume、待答 ask、显式停止、会话切换、会话/工作区占用及目录阻塞、worker 退出与服务重启覆盖；接受不明不重复创建，停止意图不恢复。
4. 真实 OMP RPC 子进程 + 临时 profile/session + 有限本地模型替身证明创建、跨轮续跑、状态、停止和恢复；不连接常驻 bot。执行既有真实 CLI/process 重启 benchmark、相关回归、typecheck、全量 bun test。
5. 在真实状态库的一致性只读备份上运行应用自身 schema migration 两次，核对原数据及旧任务恢复语义；保留旧 schema 回归。
6. 更新主规格、用户帮助、README 和维护指南；记录实际验证结果，不将协议替身测试称为真实模型质量验证。

## 实施与验证记录

2026-11-02，在当前隔离工作区完成：

- `bun run typecheck` 通过；`bun test` 为 **909 pass / 0 fail**，68 个文件、5,169 个断言。覆盖原生接受前进度、控制接受与生成接受分离、停止后迟到接受、重复恢复、忙目录、清理窗口、问答及超时迟答、状态缓存过期、精确身份恢复与旧库迁移。
- `bun tests/benchmarks/goal.ts` 使用真实 OMP 18.15.0、真实 channel CLI、本地 Octo HTTP/WebSocket fixture 和有限 localhost 模型替身。整段工作负载 23 次模型请求，验证两轮目标完成、live show/pause、resume、drop、无进展暂停、预算达到 `budget-limited`、禁止越过预算恢复，以及 `goal.enabled:false` 拒绝且没有模型调用。
- 同一真实工作负载在暂停后切换聊天模型，并移除原 provider 配置，验证状态控制与后续目标执行均使用已指定的替代模型；正常停机和 SIGKILL 后恢复仍沿原会话及原 goal ID，没有重复 create。
- `bun tests/benchmarks/restart.ts` 的 graceful/crash 两种场景均为 seamless=1、resumed=1、queued=1、notices=0、overlaps=0、duplicates=0。
- 用只读连接取得真实状态库的一致性副本，运行应用自身 `openChannelStore` 迁移两次。原有 17 张表、5,282 行按全部旧列计算的逐表哈希一致；新增 `goal_tasks`、`goal_sessions`。私有副本和临时探针脚本在验证后移除。
- 规格经过一次独立只读审查；恢复契约明确要求 RPC 前持久化派发、接受回调同步提交 exact goal ID，缺少接受事实时停止恢复。
- 与 `main` 的 `4e5a2ad` 集成后，`bun run typecheck` 通过；全量测试为 **1,031 pass / 0 fail**，76 个文件、6,154 个断言。目标控制回归验证普通目录中的不同会话可并行：show/pause/drop 操作原目标时，另一会话的任务保持运行。
- 集成版本重新通过真实 OMP goal 与 restart 基准：有限目标工作负载仍为 23 次 localhost 模型请求、重复 create 为 0；graceful/crash 两种重启场景的 session overlap、duplicate effect、disruption notice 均为 0。

上述验证不调用真实模型 provider，不接管常驻 Bot，不代表真实模型质量或已登录 Octo 客户端视觉验收。本次实现未部署、未重启常驻服务。
