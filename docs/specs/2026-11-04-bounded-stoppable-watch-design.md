# 有界、可停止的语义 watch

日期：2026-11-04
状态：已实现并通过离线验收；watch 默认随 Decisions 启用，并共用其连接、模型和密钥配置。

## 目标与确认决定

由 channel 宿主持久管理 `采集器 + 语义完成条件 + interval + timeout`。用户显式要求持续关注后，宿主周期执行采集，把有界证据交给 Decisions 判定；首次满足时在归属聊天受理一条独立续办任务。等待不保留 OMP worker，不占聊天/session 执行 lease。

确认内容：

1. 通用可信本机采集脚本或 executable＋argv，信任边界与已授权 agent 执行 bash 相同，不声称只读沙箱。
2. 一次命中即结束；立即首次检查，当前已经满足也命中。
3. 默认间隔 5 分钟、期限 6 小时；最短间隔 1 分钟、最长期限 24 小时、最多 300 次检查。
4. watch 是聊天级订阅，独立于源任务和源 session。源任务取消/完成、`/new`、`/reset` 不停止订阅。创建任务身份只用于来源、重复调用识别和审计。
5. 唤醒只入归属聊天的独立任务队列，不 steering 当前回合，不回答 pending ask，不赋予新的业务操作权限。
6. 引用识别与 watch 共用 `decisions` 的 protocol、endpoint、model 和 key；启用 Decisions 默认启用 watch 及约定的条件/stdout 外传，可用 `watch.enabled=false` 单独关闭。
7. 本区已获实施授权；不自动提交、推送、合并、部署、重启常驻服务或调用真实 provider。

## 范围与适用性

完成条件能够由退出码、明确字段或已有事件直接确定时，使用相应程序/事件机制，不使用语义 watch。采集器非零退出只表示采集失败，不是满足条件。工具使用说明须要求 agent 先选确定性机制；不声称程序能够证明任意自然语言条件没有客观判法。

watch 不以对象平台种类限制采集器；宿主不内置任意 URL crawler，不根据正文链接发现账号、凭据或权限。采集脚本的访问路径及凭据使用由用户已有任务授权约束。Decisions 只输出判定，不执行或改写命令。

不修改 Fleet、Octo 或 OMP；不依赖群级/子区级事实功能的新表或提示注入，不自动将采集结果写作群事实。Loop workspace 扫描与路由保持其独立设计。

## 接口

### 宿主工具 `watch`

`action=create|list|get|stop`，目标固定为调用任务所在 bot＋真实聊天，不接受传入其他聊天或身份。

create 参数：

- `script`：固定 shell 脚本文本，与 `command`/`args` 二选一；执行为 `/bin/bash -c <script>`，不隐式使用 login shell。
- `command` 与可选 `args: string[]`：程序及字面 argv；不拼成 shell 文本。
- `cwd` 可选，缺省为调用任务绑定目录；显式目录沿用既有绝对路径、`~/` 和一层引号规则，拒绝相对路径，登记为固定 canonical 目录。换聊天 session 或 move 不改写采集规格。
- `condition`：用户给出的语义完成条件，最多 4096 UTF-8 字节。
- `interval` 与 `timeout`：秒；默认 300、21600。最小 interval=60、最大 timeout=86400；timeout 必须大于零。计划检查数为 `ceil(timeout/interval)`（立即首次，截止时刻不再开始），不得超过 300。

返回 watch ID、归属聊天、创建时间、绝对截止时间、间隔、最大检查次数和当前状态。脚本最多 16384 UTF-8 字节，argv 序列化最多 16384 字节。不持久保存调用方传入的环境变量或秘密；子进程使用既有受控环境边界，并可使用该 bot 已配置的 CLI 身份。受控环境不是权限沙箱。

create 由当前已授权任务执行，仅在用户显式提出持续关注时使用。agent 不得因链接出现、工作未结束或 watch 唤醒而自行创建后继 watch。以 bot＋源任务＋native toolCallId 保证恢复同一调用不重复创建；缺少 native 身份时拒绝 create，不以 RPC request ID 代替。

list/get/stop 只读取或修改当前聊天；get 包含登记规格、已用预算、最近一次采集/判定、命中证据、停止原因和唤醒处理结果。列表分页有固定上限，不将全部脚本/采集结果塞进列表。

### owner 控制命令

`/watch list`、`/watch <id>`、`/watch stop <id|all>`；无参数等同 list。沿既有 owner-only、命令 claim 和有界回执，不启动模型。停止不存在或其他聊天的 ID 不泄漏其信息。功能关闭时仍可查询、停止已存 watch。

## 预算与调度

每聊天最多 5 个有效 watch、每 bot 最多 20 个。有效包括等待、检查及命中后尚未完成唤醒处理的记录；配额满时拒绝新建，不排一个无界创建队列。全服务最多并行两轮检查，同 watch 最多一轮。

采集总预算 30 秒（包括进程和 stdout/stderr 排空），stdout 最多 16 KiB，stderr 最多 8 KiB。超限中止进程组并记录失败，不把截断证据交给判定模型。命令、cwd 不可执行、非零退出、超时、输出超限均为明确失败。脚本可能产生副作用，宿主不声称能回滚；恶意代码主动逃离 process group 不属于沙箱保证。

检查前事务领取代次并扣预算，记录实际派发意图；进程组身份持久后才允许采集器执行。使用 parent-death watchdog，保存 PID/PGID/启动身份。确认整个进程组退出后才结算进程记录。无法确认退出时该 watch 进入 unknown，保留进程及资源阻塞信息，不执行下一轮或自动唤醒。

采集不持有聊天/session lease。对实际 cwd 登记短期共享资源使用，以保护显式 merge/remove；普通任务不因相同 cwd 被串行。已删除或不可用的 cwd 如实失败，不回退聊天新目录。

间隔按上一次检查开始时间计算；超过计划时不追赶，每次完成后最多安排一个下一次检查。截止时间不随错误、停机或重启延长。到期前开始、到期后返回的结果不能命中。下次检查时刻超出截止时间则等待到期，不再请求。

## 判定与外传

operator 在 `decisions` 中统一配置 `typesafe-systemone|openai-decisions`、HTTPS endpoint、model 和 `OMP_CHANNEL_OCTO_` key 环境名；引用识别与 watch 直接复用该配置。`decisions.enabled=true` 时 watch 默认启用；缺少或关闭 Decisions 时 watch 也关闭。`watch` 仅接受可选 enabled 开关，`watch.enabled=false` 可单独关闭观察；显式启用 watch 要求 Decisions 已启用。密钥不加入 worker/采集/watchdog 环境 allowlist。使用现有已安装 Typesafe SDK 与 OpenAI 原生协议，不切换 OMP 模型。

只外传 condition、成功采集的完整有界 stdout 和采集时间/当前判定时间。不得自动加入群历史、任务正文、session、成员、系统提示、工具原始历史或 stderr。stdout 是用户授权采集结果，可能包含业务敏感信息；既有秘密模式净化只能降低明显凭据外传风险，不是完整 DLP。operator 启用须接受该范围及 provider 数据政策。

条件与采集输出分开放入固定规则的数据区，忽略采集输出内修改规则、执行动作、选定答案的指令。固定 choice 为 satisfied、not_satisfied、insufficient_evidence；satisfied 概率有限且在 [0,1]、不低于 0.95 才命中。概率不是校准准确率承诺。有效但门槛以下的 satisfied 记 evidence-insufficient，不冒充明确未满足。

每轮最多一次 provider 请求，总预算 3 秒，响应体最多 64 KiB，禁止 HTTP redirect，SDK retry=0、日志关闭、压缩关闭。失败、超时、拒绝、畸形/缺答/重复答案与证据不足有不同结果。失败只在下一个正常周期再检查，不立即重发；每次尝试都消耗预算，无法确认上游接受也不退还。连续三轮采集/判定失败终止 failed；正常有效判定（含证据不足）重置失败计数。

### Operator 启用

启用 `decisions` 后即可使用 watch，省略 `watch` 块时跟随 Decisions。例如：

```json
{
  "decisions": {
    "enabled": true,
    "protocol": "openai-decisions",
    "baseUrl": "https://decisions.example",
    "model": "operator-selected-model",
    "apiKeyEnv": "OMP_CHANNEL_OCTO_DECISION_KEY"
  }
}
```

密钥只通过共用 Decisions 配置指定的宿主环境变量读取；示例不含真实凭据。若只需引用识别，可加 `"watch": {"enabled": false}`。启用能力不会自动创建订阅；仍需用户显式提出语义观察要求。启用 Decisions 意味着授权将该 watch 的条件和有界 stdout 发往选定 provider，各用途继续遵守各自证据范围，不自动加入聊天全文或其他用途的数据。

## 持久化与生命周期

store 独占状态转换：waiting/checking → waiting、matched、failed、expired、stopped、deleted、unknown。记录包含固定 bot/聊天、源任务和创建调用、规格、创建/截止/下次时刻、检查数、代次、最近检查结果及有界证据、命中时间、停止原因、进程身份和唯一唤醒关联。每个 watch 最多保留最近一次检查和命中证据，不复制全部采集历史；轮次计数与异常/未确认结果保留可追溯字段。

命中与唯一事件身份 `watch:<id>` 同事务发布。匹配代次、仍有效状态、未截止和未删除必须在写入时复核。重复响应或过时代次不能覆盖终态。宿主已确认子区删除使用持久删除标记，先于可能失败的工作区 cleanup 停止该聊天全部 watch，并阻止旧任务新建。

正常停机先封闭检查/创建/派发并 abort 在途请求，确认并回收采集进程，等待检查尾部在 store 关闭前退出；有效 waiting 保留。SIGKILL 后先核查旧进程身份、停止精确旧进程组，再恢复 watch；不确定时 unknown。中断的检查已扣预算，不重放该轮，将其记录为中断后在下个合法周期重新采集。所有终态不因开库/重启复活。重启期间错过周期不补跑。

物理表为 `watches`、`watch_chat_tombstones`、`watch_match_events`。`watches.process_unknown` 独立于业务终态：即使尚未获得可验证的 PID，未知进程结果也保留 cwd 资源保护、配额和并发占用；stop、到期或删除不能抹去该安全状态。重启不能通过猜测 PID 或重新执行采集来消除未知结果。已核验的进程身份在 parked-shell 放行、watchdog 启动之前持久化；watchdog 失败时也有可恢复所有权，确认进程组退出后再清除登记。

通过其他客户端删除、撤权不保证即时可知；读取/受理时发现不可用，记录明确失败或权限撤销，不伪造无变化，不改送父群、不新建子区。归档不是删除，命中独立任务可能按现有输入行为重新激活归属子区。

## 唤醒与停止边界

只在归属聊天权限和 bot 身份仍有效时受理独立排队任务，不走普通入站 pending-answer 路径，不 steering 当前任务。正文包含系统来源声明、watch ID、语义条件、采集来源及时间、有界命中证据、任务来源信息，并声明不属于 owner 新授权。外部输出从非命令前缀开始，不能被当作 slash command。

源任务取消和换 session 不撤销聊天订阅。唤醒受理时使用当前聊天有效绑定；受理之后按既有 exact-session 调度和恢复规则，不改写或复活源任务。watch 订阅独立并不改变已接受任务的取消/恢复合同。子区终态回传沿用 listen 与修复抑制规则，监控不向父群发送镜像，不绕过 off。

stop 事务撤销检查/未派发唤醒资格，并标记尚未开始生成的关联续办任务不可执行；随后 abort 采集/判定。迟到结果在提交、受理以及首次 prompt 派发三个边界均检查状态和截止时间。命中后排队直到过期同样不得开始生成。stop 成功表示没有后续新检查/首次 prompt 派发，不表示已派发任务的副作用回滚。已实际派发的续办任务在 stop 回执展示 task ID，使用已有 `/cancel` 停止其运行；停止失败不谎称进程已退出。

本地唯一身份、入站 claim、任务关联与状态事务保证可重放的本地受理不重复。任务 prompt 实际派发意图、agent_start 接受与完成分别记录；接受不明时沿既有 fail-closed 行为，不新造一个任务重试。外部模型请求、脚本副作用和未知接受不承诺 exactly-once。

## 模块归属与并行集成

- `src/watch/`：类型/校验、定时协调、采集进程及 Decisions 判定；对 service 暴露窄 create/list/get/stop、恢复/停止、派发资格接口。
- `src/store/`：增量表、查询与事务/CAS，复用既有 worker/process 和任务受理持久边界。
- `src/service.ts`：授权、host tool/命令接线、共享资源登记、任务受理及首次 prompt gate、启动/停机/远端删除。
- `src/runtime/config.ts`、`src/contracts.ts`：共用 Decisions 连接与 watch enabled 开关；watch 默认跟随 Decisions。
- `src/host-tools.ts`、`src/commands/command-routing.ts`：当前聊天工具与 owner 入口。

不编辑其他并行工作区。集成时与 facts、Loop 变更分别合并 service/host-tools/store/config 的独立接线，不复制其队列或改动 listen 语义。

## 验收

1. 真实 CoreService 路径创建后原 worker 完成并停止，宿主仍能采集/判断；正常轮次不发聊天消息或调用 agent，且新消息可运行。
2. 脚本和 argv 均按固定 cwd 执行；当前满足立即单次命中；未满足/证据不足继续，明确失败与权限/目录错误可查。确定性场景的工具约定明确。
3. stop/到期/宿主删除在采集、Decisions、命中未受理和已排队各阶段阻止新增首次 prompt；迟到响应、重复响应、重启均不复活。已派发状态诚实显示。
4. source cancel、换 session、同 cwd 多聊天独立；wake 在当前绑定中接受，之后保持 exact-session。pending ask 不被回答、当前任务不被 steering、listen=off 不回传。
5. interval、timeout、检查预算、配额、并发、stdout/stderr/响应上限均有可观察边界；三次连续失败终止，无无限重试/补跑/模型自续期。
6. localhost 两种 Decisions wire fixture 捕获真实 SDK/HTTP 请求，证明只有授权证据、固定 choice、无 redirect/retry、密钥不进入采集和 worker；无收费 provider 调用。
7. 抛弃式 SQLite 旧库副本通过真实应用迁移/重开，旧数据保留，stop/删除与未确认进程持久。
8. 真实本地采集进程、watchdog、SIGTERM/SIGKILL 重启 smoke，无重复唤醒或旧组重叠；运行既有 restart benchmark、typecheck、全量测试。
9. 不注册真实 Bot、不启动真实监控、不改变常驻配置或服务、不提交/推送/合并。

## 实施前核查依据

已只读核查引用设计、`src/references/decision-client.ts` 两种协议及失败吞并边界、`src/omp/process.ts` process group/watchdog、`src/workspace/worktrunk.ts` 有界命令入口，以及 service/任务存储的接受与恢复边界。引用业务的失败空集合不能直接充当 watch 判定结果；协议层可复用但 watch 必须保留明确失败类别。本机 `gh pr view --help` 只作本地命令契约核查，没有访问真实仓库；watch 不依赖平台 CLI 固定字段。

## 已观察的离线验收

生命周期与恢复边界已通过独立只读审查。service 为 watch wake 单独复核已受理会话，变化时终止该次待派发输入，不转投其他会话。

| 检查 | 已观察结果 |
| --- | --- |
| `bun run typecheck` | 通过 |
| 共用 Decisions 配置、watch 集成及双协议专项 | 5 个文件、34 项通过 |
| 隔离 TMPDIR 执行 `bun test` | 112 个文件、1,449 项通过，0 失败 |

CoreService 集成回归同时覆盖：原任务取消后独立唤醒、`/new` 后使用新 session、pending ask 保持待答且不 steering、已排队 wake 被 stop/到期撤销、远端子区确认删除后迟到结果无效、owner 私聊归属变化后的撤权及 cwd 解析规则。

停机顺序保证创建封口及 fork/merge/remove/入站 I/O 撤销在首次异步等待前完成；watch 检查先 abort，再于 worker/入站收尾后、store 关闭前等待 drain。该顺序保留原有 fork 远端创建的取消边界。

- `bun tests/benchmarks/watch.ts`：真实加密 Octo fixture、CLI、RPC 子进程、采集进程/watchdog、HTTPS localhost Decisions 与 SQLite 全路径通过。原 worker 退出后继续检查，期间前台任务正常完成；单次命中生成一条续办任务。stop/到期后迟到响应，以及 graceful restart、SIGKILL 旧采集进程回收均通过。实际发起 4 次本地判定请求，7 个真实 RPC 子进程，停止或到期后的唤醒为 0。
- `bun tests/benchmarks/restart.ts`：graceful/SIGKILL 两个场景通过，恢复 2 个在途任务和 2 个排队任务；打断通知、会话重叠和重复副作用均为 0。
- 真实状态库通过只读 SQLite backup 复制到私有抛弃式目录，再运行实际 `ChannelStore.open` 迁移及重复开库。原有 27 张表、13,013 行的逐表内容哈希保持一致，`quick_check=ok`；验证 native 调用去重、stop、删除标记及重开后的迟到结果不能复活。真实状态库未运行迁移。

这些证据验证宿主协议和状态机制，不验证真实 Decisions 模型的语义质量、概率校准、费用或线上平台权限。生产使用沿用当前 Decisions endpoint/model/key；operator 启用时接受对应 provider 的数据政策和各用途的有界外传范围。
