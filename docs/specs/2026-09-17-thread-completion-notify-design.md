# 子区回传监听级别

日期：2026-11-02

## 用户原始需求

2026-11-11：

> 目标：子区执行失败时，向创建方的任务终态回传包含实际失败原因，而非仅显示「派发任务失败」。
> 范围：调查失败原因从任务启动/执行、持久化到 listen=task 父区回传的完整链路；生成 SDD 并直接实施。
> 约束：准确关联任务，遵守现有 listen 策略和可见性；原因使用安全、受限且有意义的内容，不泄露凭据、模型思维或原始敏感数据；未知原因如实说明，不编造。
> 验收：例如群说明读取失败导致任务未启动时，创建方收到能辨别原因的失败回传；覆盖执行失败、启动失败及原因不可用的回归测试，更新相关文档。

同日补充：

> 无法识别的原因明确显示为“原因不可用”。原始错误载荷不进入回传。
> 为什么不可以？都要返回，否则主群 agent 根本无法分辨到底什么原因，怎么处理！！

2026-11-11，结果回传补充：

> 目标：任务级子区回传应携带有用的实际结果，而非仅报告「已完成」。
> 范围：调查子区派发任务和 Loop 任务的最终产出、持久化、重启恢复与父区回传路径，设计并实现可靠的结果回传。当前 threadOutcomeNotice 对成功任务只有固定状态文案；失败原因已有单独实现，应保留。生成 SDD 并直接实施。
> 约束：保持 listen=task 才回传任务终态，merge/off 的现有策略和权限边界不变；结果来自实际任务产出，不编造；避免泄露凭据、内部思维或无关内容，明确处理结果缺失与长度限制；不得把回传当作用户确认或待答问题的答案。
> 验收：任务完成时父区收到可理解的实际结果，失败原因仍可见；结果跨重启与消息重试正确关联，不重复或错发；覆盖不同监听级别、结果缺失及安全边界的行为测试，更新相关文档。

## 目标与范围

每个子区持久保存 `listen: off | merge | task`，默认 `merge`，回传目标为创建方聊天。策略在终态交付时读取，修改立即影响尚未结束的任务，跨重启保留。shared 工作区保持合并拒绝，因此默认没有自动回传；显式 task 可接收派发结果。

- off：不自动回传。
- merge：仅宿主实际完成合并后回传一次。
- task：派发或 Loop 变化在子区产生独立任务时，完成、失败、取消、中断可回传；合并成功也回传。
- 任意档位均不回传合并失败及其修复失败。停机挂起不是任务终态。

## 工具契约

`create-thread` 增加 `listen`（默认 merge），包括 single/shared/separate 与普通子区；separate 每个创建结果各自保存策略及同一创建方目标。`thread-listen` 使用既有 thread-* 定位方式，带 listen 修改，不带时查询。`thread-info` 同时报告策略。修改策略不修改创建方目标。

`thread-dispatch` 不再接受 notify。派发仍复用权限、深度限制、durable 接受、追加模式和收据；独立派发任务及直接送达子区的 Loop 独立任务有自己的终态通知，steering 部分没有独立终态。Loop 变化本身只送关联子区；此处订阅的是执行结果，不是变更镜像。已有任务级登记不参与决策。

## 持久化及迁移

新增子区策略表：bot + 子区 channel ID 唯一，保存创建方聊天、名称、listen、mergePending。新增合并成功事件表：稳定操作事件 ID、来源、目标分支、提交、宿主清理处置、归档排定结果，以及 pending / intent / delivered 的交付回执。事件交付复用稳定 synthetic inbound ID 去重；事件 ID 来自稳定操作身份，而不只按目标提交去重，以区分保留工作区后的不同合并尝试。

移除 tasks 的 relay_channel_type / relay_channel_id / relay_thread_name 与 TaskRelayTarget。迁移事务先为已有子区建立 merge 策略，再删除旧列。已知旧回传目标仅作为历史来源信息迁移，不保留旧开关；缺少创建方记录的旧子区使用其真实父群。普通已有子区可在首次访问时按真实父群建立默认策略。旧 notify=true 在途任务统一遵循 merge。

任务新增 relaySuppressed 标记；不再携带通知开关或目标。派发身份由既有 dispatch: 消息 ID 判断，Loop 独立输入由 loop: 身份识别；两者在子区终态时共用策略及合并抑制。

入站先于创建流程登记时，同一创建方的默认回退记录保持未确认；创建结果写入请求的名称和监听策略并确认。显式 `thread-listen` 修改及已确认记录不得被后续创建重放覆盖；创建方不一致时拒绝覆盖。既有数据库的策略视为已确认。

## 合并事件单一归属与失败隔离

合并成功事件的唯一发布者是宿主合并成功路径，不是 agent 的自然语言、任务 completed，也不是归档回调。`/thread merge` 命令、`thread-merge` 工具与持久成功结果的恢复共用 `publishThreadMergeSuccess(target, summary, archive, operationId)`，由 CoreService 持久化并交付。真实 WorktreePort 从已经验证的 Git common directory 读取合并后目标 ref 的提交；事件包含 summary.target、commit、summary.removed 与 archive=scheduled/not-scheduled。宿主会额外等待异步清理的实际路径消失及完整进程组退出，不只依据 Worktrunk 的 removed=true；父聊天通知保留「工作区清理：已排定」的保守表述，removed=false 表述保留。归档同样只表述已排定或未排定。

资格通过的 `/thread merge` 命令与 `thread-merge` 工具在宿主实际执行前持久设置 mergePending，并将该子区在途任务标为 relaySuppressed，后续接受的修复任务继承此标记。mergePending 期间所有派发终态回传被抑制；成功后解除子区 pending，但这些任务自身标记不解除，防止合并任务结束再发普通结果。失败保持 pending，重启和后续修复失败均不唤醒父会话。资格拒绝不置 pending；shared 命令仍只有固定拒绝文案、零任务，直接 host 合并拒绝只抑制当前任务，不影响后续正常 task 档派发。

宿主优先命令使用原消息 ID 作为稳定 operationId，通过 `replayThreadMergeSuccess(target, operationId)` 读取已有成功收据；未命中时在资格通过、实际执行前复用 `ensureThreadListen` / `beginThreadMerge`，合并成功及归档排定后仅调用同一成功事件发布者，不能从命令回执另发通知。host tool 的 operationId 为 task.id 与 native toolCallId 的组合，同一调用重试保持身份。持久 host merge outcome 的恢复只补交付同一事件，不重新执行 wt；已成功操作的重放不改变新操作的 pending。

## 交付与内容

CoreService 持有实际交付：通知使用持久 owner 身份、messageSeq=0，经 appendToRunningTask / acceptTask 交付，不经过 pending ask 答案路径；在真实 worker.steer 与 durable task 接受之前重新检查当前策略、合并抑制及权限。停止期间不交付。合并事件 pending 表示尚未派发，可在恢复时继续；紧贴真实 RPC 之前持久记录 intent，接受状态不明的 intent 不自动重放；durable 接受、成功交付及 off 消费记 delivered。off 消费后的事件改档或重启不重放；稳定 inbound claim 防止重复。

任务通知带来源子区、任务 ID、终态及必要说明，区分派发任务与 Loop 任务。失败时回传该任务持久化的 `error_text`，不以固定分类替代具体原因；空值才说明原因不可用。回传使用既有展示文本净化，对可识别凭据脱敏、压平空白并限制长度。任务错误在写入时已受长度限制，成员 API 的外部详情仍按其既有安全策略处理，未持久化或已截断的详情无法恢复。脱敏依赖已知模式，不能保证识别所有敏感内容；启用 task 回传意味着创建方聊天可见这些诊断。合并通知仅带来源、事件身份、目标分支、提交、工作区清理状态及归档排定状态。所有可变字段有界。

完成通知使用该任务最后一条实际 assistant 正文，不使用工具载荷、模型思维、任务输入或其他任务的输出；未取得正文时明确说明结果不可用。正文在回传边界复用展示脱敏规则，归一空白并限制长度，截断时标明；可识别凭据会被遮蔽，模式匹配无法保证识别所有秘密，因此 task 档使结果对创建方可见。失败仍回传原有安全处理后的持久错误，不以结果替代原因。派发和 Loop 独立任务适用同一规则；steering 没有独立任务结果。

任务终态及其结果必须先持久化，父区输入使用该任务的稳定身份。终态落库与通知交付之间发生重启时，恢复尚未交付的通知，不重新执行子区任务；持久接受后的消息重试不能产生重复父区输入。steer 已派发但接受状态不明时不得再次派发或回退成新任务。恢复和重试必须重新执行授权、监听策略和合并抑制门禁，且不能进入待答问题的答案路径。

实现契约：任务终态在同一事务内登记 `thread_task_events`（`task_id` 唯一，回执 `pending` / `intent` / `delivered`），只登记独立派发与 Loop 任务；merge intent 期间、`relaySuppressed` 任务与非子区任务不登记，批量取消与重启结算同样在各自事务里登记。旧库不回填，升级前的终态行没有事件，永不补发。成功正文取该任务自身在最终 assistant 帧到达时持久化的 `final_result_text`（仅回传候选且仍在途时写入），不取聚合旁白文本；缺失时先尝试该任务受管 outbox 中最新一条 final assistant 流（排除 `notice:` 与 resume 命名空间下的通知帧），该流为空或不存在则明确「结果不可用」，不回退更早输出。写入失败使该回合失败，绝不以陈旧正文成功回传。交付时重检策略、合并抑制、owner 与授权；策略不符、合并抑制或授权拒绝一律持久消费，之后改档、重启或补授权都不再泄露旧结果。交付前按稳定 `notify:<taskId>` 检查已接受的父区输入；steer 仅在真实 RPC 前把 `pending` 改为 `intent`，`intent` 不自动重放也不回退为新任务。重启只重投 `pending` 事件。

保留来源与处理指引：系统回传不是用户新消息、问题答案或新增授权；接收 agent 自行判断与当前任务及待确认事项的关联，相关时说明补充或改变了什么，不相关时不混入当前结论，不自动切换工作。

## 验收

1. 默认创建和旧数据库迁移后普通派发的所有终态无父聊天消息、任务或 steering，含旧 notify=true 在途任务。
2. merge 仅真实合并成功回传一次；失败及后续修复失败均留在子区。
3. off 不回传；task 覆盖派发或 Loop 直达子区独立任务的四种终态与合并成功，合并任务及修复任务无旁路。
4. create-thread/listen、thread-listen 查询修改和 thread-info 持久有效，跨重启保持，旧 notify 路径全部迁出。
5. shared 默认静默，task 派发有效，合并仍拒绝。
6. 内容有界且保留来源语义，ask、权限、去重、追加模式和归档顺序不变。
7. 先用行为回归测试证明差异；迁移在抛弃式副本以真实 ChannelStore.open 演练并二次打开；隔离 Git/进程 smoke 验证实际合并及通知。运行相关测试、typecheck 与全量测试，不推送、合并或重启常驻服务，不向真实父群发送验收通知。
8. task 档失败通知关联实际失败任务，并回传持久化的具体启动或执行错误；缺失原因明确不可用，可识别凭据脱敏。其他策略与合并抑制不变。
9. task 档成功通知携带对应任务最后一条实际 assistant 正文的脱敏有界摘录；无正文明确不可用。终态后、交付前重启恢复同一事件；已接受回执不重复，未知 steer intent 不自动重投。旧终态不补发，策略与授权拒绝后的通知不在后续补发。

## 验收记录

2026-11-11 结果回传验收：`tests/thread-notify.test.ts`、`tests/core-thread-notify.test.ts`、`tests/store-thread-listen.test.ts` 及 Loop API、存储、路由和派发回归通过；包含终态前续跑、终态后重启补发、未派发重投与未知 steer intent 不重投。`bun run typecheck` 通过；离线 restart benchmark 的 graceful 和 crash 场景均为零会话重叠、零重复副作用。测试使用隔离夹具，不发送真实父区消息。

2026-11-11 本次验收：`tests/thread-notify.test.ts`、`tests/core-thread-notify.test.ts`、`tests/store-thread-listen.test.ts`、`tests/thread-merge-host-first.integration.test.ts` 和 `tests/core-startup-failure.test.ts` 共 41 项通过；旧库迁移在临时数据库上打开两次，真实 Git 集成覆盖合并与通知。`bun run typecheck` 通过。离线 `thread-merge-auto-commit.ts` 使用真实 OMP、Worktrunk 和临时 Git 仓库，成功合并、提交失败及取消三项通过；`restart.ts` 的 graceful 和 crash 场景均通过，无会话重叠或重复副作用。上述验收不连接真实 Octo。

创建策略竞态修复后，监听存储、回传及 fork 相关测试共 42 项通过；迁移和隔离真实 Git 合并相关测试共 12 项通过；类型检查通过。

全量 `bun test`：1933 通过、5 失败。`core-workspace-ownership.test.ts` 的 unmapped intent 用例超时，随后 3 例因 Bot lock 失败；`core-multi-project-workspaces.test.ts` 的 shared dispatch 用例报告接受了变更的 worktree branch。两个文件分别独立运行时仍有相同失败；全量门禁未通过。

工作区测试现按未映射创建意图不阻塞独立子区的合同验证；`core-workspace-ownership.test.ts` 与 `core-bot-lock.test.ts` 共 16 项通过。本次未重跑全量测试。

- `bun run typecheck` 通过；完整 `bun test` 为 1050 pass / 0 fail，77 个文件、6284 个断言。相关 listen、host tools、群历史、多项目、store、通知测试的独立运行是 87 pass / 0 fail。
- 状态库使用 SQLite readonly backup 的一致性副本，经真实 ChannelStore.open 迁移并再次打开：501 条任务与既有任务列内容保持一致，26 条旧回传登记迁为来源信息，79 个子区全部默认 merge；19 张既有业务表行数和原 meta 内容保持，旧任务 relay 列已删除，integrity_check=ok。迁移回归在临时跳过迁移调用时按预期失败，恢复调用后通过。
- 隔离真实 Git/Worktrunk 经 CoreService 和 host 工具完成两次成功合并：默认 merge 回传 1 次，off 回传 0 次，提交与真实目标 ref 一致；真实 rebase 冲突以及后续派发修复失败均 0 次父聊天输入，冲突时目标提交未变。最终通知为 712 UTF-8 bytes，清理和归档按真实排定状态表述。
- `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两种离线真实进程场景通过：原任务续接 2、恢复排队任务 2、打断通报 0、会话重叠 0、重复副作用 0。
- 本次只在隔离工作区实施，验收使用本地 transport/worker fixture；未向真实父群发送通知，未推送、合并或重启常驻服务。host-first 分支的后续集成遵循上文单一发布接口，本次未改其工作区。
