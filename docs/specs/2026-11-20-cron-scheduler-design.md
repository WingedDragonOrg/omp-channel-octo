# 分钟级定时任务

日期：2026-11-20
状态：已实现，隔离验收见文末。

## 用户原始需求

1. 「为 channel 增加 scheduler 与 `schedule` 工具，支持分钟级 cron 定时执行 script 或 agent 任务。」
2. 「调度任务记录创建来源，精确区分私聊、群聊、子区及具体身份；记录每次运行结束的结果和可能的产物，并明确结果/产物的目标私聊、群聊或子区。agent/script 自行通过 `octo-cli` 发送产物。设计任务创建、查询和管理、运行状态及目标表达，使用户和 agent 能实际使用。」
3. 「先使用 spec-design 调查既有 watch、任务调度、持久化、权限及恢复机制，形成完整 SDD；对不能从现有约定确定的关键产品决策在本子区通过最少问答与 owner 确认，方案确认后直接实现。cron 最小粒度为分钟，拒绝秒级持续触发。明确时区、重启或错过触发、并发/重复执行、停止、来源与目标权限边界、script 与 agent 执行身份及生命周期、结果和产物记录方式，避免误投递、越权或重放未知副作用。不得假设创建来源自动授予其他目标的发送权限；不要注册或重启常驻 bot，也不要操作真实业务数据。使用隔离环境和本地 fixture 验证。」
4. 「确认后的 SDD 覆盖以上行为及边界；schedule 工具和 scheduler 端到端可用，分钟级 cron 正确触发 script 与 agent，来源、运行结果、产物及指定目标可追溯；覆盖权限、并发、取消、重启与重复触发等关键边界，执行针对性自动测试及隔离实际运行验证，同步相关规格并报告结果。」
5. 补充：「跨聊天仅owner 可创建，但注意群聊和对应群聊的子区不属于跨聊天」。随后确认：同一群的主群及子区、子区之间视为同一聊天范围；其他设计按本规格执行。

## 调查依据

现有 watch 在 `src/watch/index.ts`、`src/watch/collector.ts` 和 `src/store/store-watch.ts` 中实现 source task/native tool call 去重、进程组身份、watchdog、状态代次及恢复；它是有期限的语义检查，不支持日历计划和每次执行账本。`src/tasks/task-scheduler.ts` 管理聊天和精确 session 的任务队列，不是 cron 调度器。`src/inbound/chat-authorization.ts` 的 allowlist 是入站聊天许可，不是任意用户向任意目标发送的授权。`TaskRecord.senderId` 对合成任务可能是 owner，不能证明原始请求者。`src/runtime/octo-cli.ts` 的凭据具有 Bot 的服务端权限，不能作为目标沙箱。Outbox 不替调用方检查跨目标授权。

## 对外契约

`schedule` 为任务绑定的宿主工具，支持 `create`, `list`, `get`, `runs`, `pause`, `resume`, `stop` 和运行中的 `artifact` 登记。计划管理限定来源聊天；详情读取另遵循下述跨范围保护。执行中的 agent 只能登记所属运行的产物。创建仅在用户明确提出定时执行时使用；重复的同一来源 task/native toolCallId 返回同一记录。宿主提供工具说明，明确身份、能力和风险。计划包含名称、五字段 cron、IANA 时区、固定 canonical cwd、script（固定 shell 文本或 executable 与字面 argv）或 agent prompt、执行时限、明确的目标 `channelType` 与 `channelId`。时间和列表均返回时区及 UTC 时间。支持普通五字段的数值、列表、范围和步长，星期日 0 或 7；当日与星期同时受限时按标准 cron 的 OR 语义。不得接受秒字段或小于一分钟的触发。

示例：每天上海时间 09:00 执行脚本，目标为指定子区：

```json
{"action":"create","name":"日报","cron":"0 9 * * *","timezone":"Asia/Shanghai","cwd":"/workspace/project","script":"./daily-report.sh","target":{"channelType":5,"channelId":"group-id____thread-id"}}
```

改为 agent 执行时，用 `prompt` 替换 `script`；直接执行程序时使用 `command` 和字面 `args`。`create` 返回 `{kind, schedule}`，其中 `schedule.id` 用于 `{"action":"get","schedule_id":"…"}`、`runs`、`pause`、`resume` 和 `stop`；`list`、`runs` 支持 `limit`/`offset`。计划中的 agent 用 `{"action":"artifact","name":"report","type":"text/plain","path":"/workspace/project/report.txt","target":{"channelType":5,"channelId":"group-id____thread-id"}}` 登记产物，登记不等于发送。错误返回以 `schedule:` 开头的文本。

当目标不是来源聊天本身时，即使同属一个群，来源聊天只可查看不含结果正文或产物信息的状态元数据；即使请求者是 owner，群内回复仍对群成员可见。详细结果和产物可由 owner 在其已认证私聊中通过计划 ID 查询，或由执行者按目标实际权限自行发送；不得仅凭来源或计划 ID 向群公开目标内容。

默认运行时限 30 分钟，创建者可以指定；按固定上限约束并在详情返回实际值。暂停和恢复保留计划及历史，不补执行暂停期间时刻。停止永久撤销计划并取消本计划已领取/在途运行，不影响其他计划或聊天任务；返回确认退出或未知，不能声称已回滚。管理接口不泄露其他来源聊天的记录。

## 身份、目标与授权

持久化 Bot UID、精确来源类型/ID、来源消息 ID、原始请求者 UID、来源任务 ID、native tool call ID、创建时间，以及独立目标类型/ID；区分直接用户输入和合成来源。使用可信的持久入站来源判定人类身份，不得将合成任务的 `senderId` 当作真实 owner。来源是私聊时只允许当前 owner；群或子区沿用已启用聊天的成员授权。群及其子区构成同一范围，包括同群子区之间；其他群或私聊为跨范围，仅真实 owner 的明确目标请求可以创建。创建及每次执行前分别核验来源授权、目标身份与可用性和当前 Bot 身份；对应群子区关系须用真实 ID 和已知关系验证。目标无效、不可核验或权限撤销时记失败，不回退父群或其他聊天。允许清单不是发送权证明；实际发送由 Octo 服务端裁决。

活跃回合可接受多个成员的 steering；宿主工具闭包绑定原任务，并不能证明后续调用由哪一条 steering 触发。因此任何可能已经进入该回合的不同身份输入都使该回合的计划创建资格失效，拒绝创建并要求以独立消息重试；同一身份输入不受此限制。资格在 steering 受理及工具创建之间保持持久、竞态安全，重启恢复也不得把未知输入当作同一身份。查询和管理不依赖该创建资格。

执行环境使用现有 Bot 专属 octo-cli 凭据，与普通 agent/bash 同一信任边界，不是沙箱。登记目标是授权和审计要求，不能技术上限制任意脚本使用 Bot 的其他实际权限。不得声称实现了该限制。外部服务可随时撤权，预检查不保证发送成功。未经确认不发送或泄露其他目标的结果。

## 日历、领取与执行

以 UTC 分钟时刻为 occurrence 唯一身份，在指定 IANA 时区解释字段。DST 不存在的本地时间跳过，重复的本地时间按两个不同的 UTC 时刻各执行。新计划从创建后的下一个整分钟开始；恢复后只计算未来时刻，停机期间不补跑。每个 `(schedule_id, scheduled_at_utc)` 唯一，事务先生成运行记录并领取，再启动。若该计划上一轮仍未终结或进程结果未知，本轮标记 skipped，不积压、不重叠；不同计划可并行，设定有限全局并发并在超出时记录 skipped。停止、权限撤销与首次派发之间以持久状态复核，迟到完成不得复活计划。

每轮在副作用前进入持久派发阶段并受本机派发栅栏保护。停止先持久撤销，再等待所有已经进入派发阶段的操作完成握手：确认没有启动，或确认已启动的进程/worker 退出后才报告已停止；无法确认则报告 unknown。启动与停止竞争时不能在停止确认后放行新的副作用；重启时未结算的派发阶段按可能已启动处理，不盲目重试。

script 以固定 cwd、受控 worker 环境和明确的 argv 启动，固定 shell 文本使用 `/bin/bash -c`，不加载 login shell。`OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_TYPE` 与 `OMP_CHANNEL_SCHEDULE_TARGET_CHANNEL_ID` 环境变量提供精确目标，供脚本自行投递。使用现有受管进程组、启动身份与 parent-death watchdog；stdout/stderr 有界保存，退出码、开始结束时刻、失败/超时/取消/未知均可查。进程身份须在副作用前持久化，停止先撤销再终止并等待进程组。无法确认退出即 unknown，保留身份和资源保护，不运行下一轮。重启先恢复旧进程身份并确认退出；未能确认则不触碰不匹配的进程。

agent 每次以独立 session 和固定 cwd 执行，不使用或修改来源/目标聊天 session，不 steering 或回答 pending ask。获得当前 Bot CLI 身份和登记目标，但不替 agent 自动发送产物；执行自身负责调用 octo-cli。记录其独立 task/session、结果及完成状态。确保已确认接受的 agent 仅按原 session 原生续接，未确认接受不重新发送 prompt；无法证明安全续接时标记 unknown。保留已执行外部副作用可能发生的事实，任何不确定发送不重试。

## 运行结果与产物

每个运行持久保存 planned/start/end 时刻、状态、目标、有限结果或错误、进程/agent 关联及声明产物。script 通过 `OMP_CHANNEL_SCHEDULE_ARTIFACTS_FILE` 获得本次运行专属清单文件路径，写入 `{"artifacts":[{"name":"…","type":"…","path":"…","target":{"channelType":1,"channelId":"…"}}]}`；也可用 `remoteRef` 代替 `path`。agent 通过 `schedule artifact` 登记。结果和产物目标均须等于计划中已授权目标，记录仅表示执行者声明，不表示完成投递；如有发送者提供的可核对回执可一并保存。清单必须有界，不能读取任意其他任务的清单。输出和错误有界保存并按既有秘密净化规则处理，净化不保证识别所有敏感内容。

结果领取、进程派发、进程退出和业务结算分别持久化；崩溃在任意缝隙都不得重放潜在已发生的脚本或 agent 副作用。未知既不冒充成功，也不被本地超时视为远端投递失败。运行历史可按计划分页查询，清晰区分 skipped、cancelled、failed、success 和 unknown。

## 生命周期与迁移

新增 SQLite 表和增量迁移，旧任务和状态不变；每项创建和 occurrence 均有唯一约束和事务状态转换。服务启动先恢复旧进程和受理记录，再允许调度；正常停机先封口创建和派发，再中止并确认在途运行，数据库关闭前完成收尾。计划独立于创建任务、其取消和 `/new`、`/reset`；来源子区删除或权限撤销应撤销未来执行。对无法验证身份或进程退出的情况 fail closed。不可因设备时钟跳变重复执行同一 UTC occurrence。

## 验收

- 按原需求 1、2：通过真实宿主工具创建和管理 script 与 agent 计划，指定时区、来源、目标，按分钟触发且每次结果和声明产物可追溯；发送路径由任务自行调用本地 Octo fixture。
- 在派发许可复核后、实际启动前并发 stop，证明 stop 不提前确认且其后不出现新副作用；跨范围计划在群中不显示私聊结果或产物，owner 私聊查询受独立身份校验。
- 按原需求 3 和补充 5：私聊、主群、子区和不同群的身份/目标分别覆盖；非 owner 跨群拒绝，同群子区允许但目标仍须验证；不使用真实 Bot、真实 provider 或业务数据。
- 验证 cron 字段、DST、重启和停机错过、不重叠、并发饱和、唯一 occurrence、暂停/停止与迟到结果、来源及目标撤权、进程未知和 prompt 接受未知均符合上述合同。
- 用隔离 SQLite、真实本地脚本进程/watchdog、真实安装 OMP 和有限 localhost 模型及本地 Octo fixture 验证完整路径；执行针对性自动测试和既有生命周期回归。不可宣称本地 fixture 证明真实服务权限或模型质量。

## 隔离验收记录

- `bun test tests/core-schedule.test.ts tests/schedule-cron.test.ts tests/store-schedule.test.ts tests/schedule-recovery.test.ts`：29 项通过；包含混合身份 steering、持久来源、重复触发、停止及恢复。
- `bun test tests/core-steering.test.ts tests/store-ingress.test.ts tests/core-ingress-routing.test.ts tests/core-ingress-recovery.test.ts`：33 项通过。`bun test tests/cli-lifecycle.test.ts`：8 项通过；`bun run typecheck` 通过。
- `bun tests/benchmarks/schedule.ts`：使用已安装 OMP、有限本地模型和隔离 Octo fixture，script 与 agent 分别在两个真实分钟边界执行，记录来源、精确目标、运行结果及声明产物，并由任务调用隔离 octo-cli 发送；共 11 次本地模型请求。
- `bun tests/benchmarks/restart.ts`：正常重启及崩溃重启各通过；两个运行中任务续接、两个排队任务恢复，会话重叠和重复副作用均为 0。

验证未注册或重启常驻 Bot，也未访问真实业务数据或真实模型 provider；fixture 不能证明线上目标权限或真实模型质量。
