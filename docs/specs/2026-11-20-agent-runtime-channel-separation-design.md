# Agent 运行时与 Channel 分离设计

日期：2026-11-20  
状态：已实施；隔离验收见文末

## 用户原始需求

1. 实现 agent 运行时与 channel 服务分离：channel 重启期间正在运行的 OMP 进程继续执行；重启后接管同一进程和任务。借鉴 Orca 后台 daemon 持有进程、客户端重新连接的思路。
2. 先调研 worker、stdio RPC、host tools、进程看门狗、任务与输出恢复，形成 SDD 并实施。运行时独立持有 OMP 进程和通信，重连后恢复任务、事件和交互；保留机器重启或运行时退出后的持久会话恢复能力。
3. channel 重启不得终止仍受运行时管理的 OMP；不得重复执行 prompt 或并发写同一会话。处理重启期间输出、任务完成、host tool 调用及 ask，消息和状态不丢失、不重复。验证进程身份和归属；未知时不能误判退出并启动第二个 worker。保持权限、取消、队列和投递语义，不操作生产 resident bot。
4. 使用隔离环境真实运行 OMP，在执行中重启 channel，证明同一 OMP PID 持续运行、任务完成且结果正确投递；覆盖重启期间输出、host tool/ask、重复重连、取消、worker 或运行时退出、未知归属和 session 独占，以及冷恢复和生命周期回归。
5. 补充并替代第 3、4 项中有关远端消息补送与不重复的要求：“不要视图改任何服务端的东西，我们保证自己就行，我们没有任何补送和不重复的需求”。prompt 不重复执行和 session 独占要求保持不变。

## 已核实的基线

- `src/omp/index.ts` 在 channel 进程内持有 OMP 的 stdio、RPC 请求表、活动 prompt、事件回调及 host-tool 闭包；匿名 pipe 不能由新 channel 根据 PID 重新打开。
- `src/omp/process.ts` 的 watchdog 监视 channel 身份；channel 消失后终止 OMP 进程组。`src/runtime/worker-lease.ts` 和 `src/service.ts` 的正常停机也关闭 worker；启动恢复先回收旧 worker，再将已接受任务通过新 worker 和原 session 续接。
- `src/outbox.ts` 和 store 持久化已进入 channel 的文本流与投递状态，不保存 channel 未读取的 OMP 事件。`src/questions.ts` 用 toolCallId 认领持久问题，但一般 host tool 的远端副作用没有统一的幂等记录。`octo_send_file` 当前生成随机投递身份；题卡发送和本地回执之间也存在未知结果窗口。
- 现有 `tests/benchmarks/restart.ts` 是冷恢复测试，明确要求旧 worker 退出；它不能证明原 PID 跨 channel 重启存活。现有冷恢复的 `/retry` 也不承诺工具副作用恰好一次。

## 目标边界

运行时进程独立拥有活动 OMP 进程、stdio、RPC 状态和 watchdog。channel 只拥有受权消息、业务编排及 Octo 投递；正常关闭或崩溃不关闭活动运行时任务。正常取消仍指向原 worker。机器或运行时退出后，确认旧进程组退出才进入现有同 session 冷恢复；身份或退出状态不明时保持阻塞。

IPC 仅限本机当前用户，受私有状态目录权限及每次运行身份约束；不新增网络监听或开放通用 agent 服务。运行时生命周期与 channel 独立，同一状态目录只允许一个确认身份的运行时。启动、重连、停止和运行时死亡之间必须有持久身份记录与明确的互斥仲裁；不能凭 PID 存在性、过期 socket 或连接失败认定旧进程已退出。watchdog 改为监视实际运行时而不是 channel；运行时自身异常退出时沿用可确认的进程组回收。

## 协议与所有权

每个活动运行以稳定的 bot、task、session、worker ledger ID 和运行时代次标识。创建意图必须在实际 spawn 前持久化；spawn、PID/PGID/进程身份、watchdog 就绪和 session 绑定在可恢复记录中有明确阶段。未知 spawn 结果不能重试创建。运行时持有唯一 session 执行权；channel 重启接管相同 task 和运行而非重新派发 prompt。单一 channel 连接通过代次栅栏取得控制权；旧连接失效后不得继续提交结果或控制操作。重复重连只重建代理，不创建新的 OMP 或新的任务。

运行时单实例用独立 SQLite 锁库的 `BEGIN EXCLUSIVE` 长事务仲裁；锁文件存在与否不代表锁归属。运行时在私有目录保存进程身份、随机握手凭据与 worker 台账，通过 Unix socket 和版本化协议接收 channel 连接。新连接接管时递增运行时内的连接代次并关闭旧连接，旧连接不得继续发控制请求。channel 的 worker ledger 保存 runtime ID，旧行保留 legacy 归属，避免把未知 runtime worker 当作旧式本地进程回收。

runtime worker 台账记录创建意图、进程身份、会话与运行阶段：`spawn-intent → identity-verified → omp-ready → active → stopping → exited-confirmed`；身份不明时为 `unknown` 并保持会话占用。运行时死亡后 stdio 不能按 PID 恢复，须确认旧进程组退出，再使用原有冷恢复。

prompt 使用稳定 dispatch ID 和参数摘要；相同 dispatch ID 返回原状态，不重复向 stdin 写入。运行时内的事件序列供重连观察；channel 将 prompt 接受事件写入任务台账。运行时自身退出后，按原有接受状态和进程身份规则进行冷恢复。

运行时在 channel 断开时保留 worker 事件和终结状态，重连后交给原任务；channel 仍使用现有 outbox 和投递语义。事件、任务身份和活动状态由运行时管理；持久存储和重连方式须保证运行中的 prompt 不因重连重发。远端消息送达与重复沿用现有服务端及 outbox 行为。

RPC 请求、回复、active prompt、分块解码和 OMP 收发由运行时保留。持久化 prompt dispatch intent 与 OMP 接受事件仍是独立边界；断连不重发 prompt。scheduler 在 channel 恢复时先恢复活动任务和 exact-session 排他占用，再释放新入站和队列。控制操作有稳定身份；取消意图先落盘，再对原运行执行 abort，重复操作不得建立第二个 worker。

## Host tool 与 ask

运行时保留 host 调用及其原生 toolCallId；channel 断开不取消运行时等待。重连后重建对应 host tool 和 ask 等待，并把结果返回同一 OMP 调用。取消传递给原 worker，保留既有任务及 ask 超时语义。

跨进程崩溃时，外部服务调用与本地提交不是一个事务；执行结果未知时沿用各工具已有的恢复与未知结果语义。答案沿用 question CAS 和原 toolCallId，恢复等待中的交互。审批、权限和 bot 身份仍由现有 channel 规则决定。

运行时保存每次 OMP host 调用的原生身份、请求和等待状态。channel 恢复后重建对应 host tool 和 ask 等待，并将结果交还同一 OMP 调用；调用与答案的持久化沿用各工具现有语义。


## 故障与恢复

- channel 正常退出或 SIGKILL：运行时和 OMP 不因 channel 退出被杀，输出和 host 请求继续保留；新 channel 恢复事件、交互和队列。
- worker 已确认退出：先消费已有事件和结果，再按真实终态结算；未确认业务结果不推断成功。
- 运行时已确认退出：确认 watchdog 及旧 worker 进程组退出，再执行既有 prompt 接受判定和同 session 冷恢复；机器重启同理。旧身份仍可能存在或不可验证时阻塞，不启动新写者。
- 网络发送结果未知：沿用现有 outbox 及工具的投递和恢复语义。
- 同时存在多个 channel 或运行时启动者：仅身份与持久互斥均确认的实例拥有写入权；失败方不终止或接管未知进程。

## 验收

在临时 HOME、state、session、凭据和 Git 目录中，使用真实安装 OMP、有限 localhost 模型及本地 Octo fixture；不得注册或重启生产 bot。

1. 任务发出第一段输出后暂停，记录真实 OMP PID、身份、session 与 task；分别以正常关闭和 SIGKILL 替换 channel。重启期间释放模型输出并完成任务，断言同一 PID 持续存活、同一 session 无并发写者，任务完成且结果正确投递。
2. 在 channel 离线时分别触发 host tool 与 ask，重连后完成原调用；检查 toolCallId、答案和返回值。覆盖重复重连、部分输出和任务在离线期间终结。
3. 重连前后取消正在运行的任务和等待的 ask，验证原 worker 接收取消，队列顺序与既有取消/超时语义保持；重复取消不重复副作用。
4. worker 退出、运行时退出、身份不明、同一 session 并发请求和重复启动均通过真实进程与故障注入验证；身份不明不产生第二个 worker，确认死亡后的冷恢复不重放原 prompt。
5. 运行相关 worker、ask、输出、恢复、生命周期和隔离的现有 restart 基准；该冷恢复基准按新旧运行时实际故障条件更新，不以重写断言代替同 PID 热重启证明。

## 验证记录

- `bun tests/benchmarks/agent-runtime-restart.ts`：真实 OMP、隔离 channel 和本地 fixture；验证同一 PID 跨重启、离线输出与离线完成、host tool、ask 重连及取消。
- `bun tests/benchmarks/restart.ts`：隔离运行时退出后的 graceful/crash 冷恢复、队列顺序、session 排他与副作用。
- `bun test tests/agent-runtime.test.ts`：身份未知、session 独占、worker 退出与重复 attach。
- `bun test tests/store-restart.test.ts tests/store-goal.test.ts tests/core-restart.test.ts tests/core-ask-restart.test.ts tests/omp-worker.test.ts`：215 项通过。
- `bun test tests/cli-lifecycle.test.ts tests/core-lifecycle-notices.test.ts tests/core-progress-card-recovery.test.ts`：40 项通过。
- `bun run typecheck` 通过。

运行时进程存活期间事件保存在运行时内存；运行时自身退出后通过已确认进程退出与持久 session 走冷恢复，不承诺原进程内事件跨运行时退出重放。

