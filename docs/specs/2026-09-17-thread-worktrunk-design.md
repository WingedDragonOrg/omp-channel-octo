# 子区 worktree 的 Worktrunk 生命周期

日期：2026-09-17
修订日期：2026-11-02

## 背景与目标

本机存在可执行 Worktrunk（`wt`）时，`/thread` 的隔离工作区生命周期改由宿主（channel 服务）经 `wt` 执行：创建、合并与 `thread-delete` 联动清理。`wt` 不可用时创建保持现状（`omp worktree add`）；`/thread merge` 没有回退，只做明确提示。所有 `wt` 调用都在宿主进程内执行，子区 agent 不直接运行 `wt`；参数兼容性由 wt 的实际执行结果判定。

## 范围

### 包含

- `create-thread --worktree_name` 的 wt 创建路径（`wt switch --create`，保留现有目录/分支命名与占用退让）与 `omp worktree add` 回退。
- `thread-delete` 的隔离工作区清理：新增可选布尔参数 `force`；默认只清理干净工作区，`wt` 不可用时回退 `git worktree remove`；清理结果进入同一条工具回执。
- 子区命令 `/thread merge`：只做检查并注入「合并指令」prompt（与建区 prompt 同构、内容不同），命令即 owner 对合并 + 清理的确认——正文要求除非有明确必要理由（并向 owner 说明）直接按默认参数执行，要保留工作区须说明理由并显式传 `remove:false`；经 `octo_management` 新增的 `thread-merge` 动作由宿主执行 `wt merge`。
- 主规格、AGENTS.md 与测试同步。

### 不包含

- worktree 列表、复用与切换（`wt list`、`wt switch` 复用已存在分支）。
- 主群与私聊中的 merge；`wt` 不可用时的 merge 回退。
- worktree 路径模板的用户配置（wt 默认路径模板与现有命名一致；宿主用 `--config-set worktree-path` 固定到计算出的候选路径）。

## 已知约束

- 候选 wt 能启动即视为可执行，`--version` 的输出与退出码仅供探测展示；创建、合并、删除按实际命令结果判定兼容性。wt 执行失败直接报错，创建不静默切换到 OMP。
- 本项目 hooks 审批已完成（`.config/wt.toml`：pre-start `bun install`，pre-merge `bun run typecheck` + `bun test`）。宿主调用不传 `-y`/`--no-hooks`：已审批命令照常执行，未审批命令在非交互环境中由 wt 自己报错退出（单行有界回报），不依赖交互式审批。
- 常驻服务 PATH 由部署给定，Homebrew 目录通常不在其中；可用性判定必须同时覆盖 PATH 与常见安装位置。
- 命令都有超时与进程终止：创建 120 秒、合并 600 秒（pre-merge 会跑 typecheck 与全量测试）、删除 120 秒；错误单行、≤200 字符；不发散敏感信息。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 接入层 | 宿主侧执行 wt（`src/workspace/worktrunk.ts` + worktree 端口），agent 只经 `octo_management` 触发 | 与现有 create-thread 的 worktree 路径同构；审批、超时与错误边界集中在宿主 |
| 创建回退 | `wt` 不可用回退 `omp worktree add` | 不回归现状 |
| merge 形态 | `/thread merge` 只注入合并指令 prompt；命令即 owner 的合并 + 清理确认——除非有明确必要理由（并说明）按默认参数执行，保留工作区须说明理由并显式 `remove:false` | owner 定稿：命令发出即默认收尾，执行与参数仍由 agent 决定 |
| merge 默认行为 | 沿用 `wt merge` 默认 pipeline（squash、rebase、fast-forward、合并后删除工作区与分支） | owner 定稿：wt 默认删除 |
| merge 无回退 | `wt` 不可用时命令只提示、不注入任务 | 合并必须有可执行的 wt |
| 删除联动 | 默认只清理干净工作区；`force=true` 用 `-f`/`--force`；wt 不可用回退 git | owner 定稿：默认干净清理，force 由 agent 自行传参 |
| 目录命名 | 保留 `<repo>.<slug>` 与 `<slug>-<threadId>` 退让；wt 侧用 `--config-set worktree-path` 固定到候选路径 | 验收要求目录/分支符合既有约定 |

## 设计

### 多项目 thread 创建与工作区 SDD

#### 目标与对外契约

统一由 `octo_management create-thread` 创建子区、准备隔离工作区、持久化绑定并写入 Octo `THREAD.md`。具体工作只来自后续 `thread-dispatch` 的正文；创建本身不启动实施。保留当前群作用域、owner 权限、去重和单项目行为。

创建参数：

- `name`：子区名称；`worktree_name`：可选 ASCII 工作区/分支命名种子，沿用现有命名能力。
- `projects`：显式项目列表，每项 `{name, repo, base?}`；`repo` 是项目仓库的绝对路径，`base` 是可选起始 Git ref。未提供项目时，原单仓库工作区仍取调用方 cwd 所属仓库。
- `layout`：多项目必须由 agent 根据任务耦合选择 `shared` 或 `separate`，不要求 owner 每次决定；单项目可省略。
- `background`：为什么创建、涉及哪些项目的简短背景。宿主补充实际成功的 worktree 路径；不是任务、SDD、实施要求或验收要求。

`shared` 一次创建一个 thread，cwd 是宿主状态目录下独立的入口目录。入口本身不是 Git 仓库；每个项目在入口内拥有来自其 `repo` 的独立 Git worktree，不共享项目主检出，不把多个项目变成一个仓库。

`separate` 一次按项目分别创建多个 thread，每个 thread 绑定各自 `repo` 派生的 worktree。返回每个项目的 short id、实际路径、分支和创建结果。背景说明可列出相关子区，便于定位；后续派发不重写背景。

#### 状态、归属与就绪

工作区资源归属独立于当前会话绑定，SQLite 记录 thread、layout、入口、每个项目来源仓库/起始 ref/计划与实际路径/分支/创建确认状态，以及 `preparing`、`ready`、`failed` 状态。仅原 `worktree_name` 单仓库调用带可选 `legacy_worktree_name` 持久标记，用于保持其未建立会话时工作区缺失后回退父群的既有行为；显式 projects 的 single/shared/separate 不继承该回退。运行时不解析 `THREAD.md` 决定路径、资源归属或执行资格。

每次远端 thread 创建之前，先在 `thread_workspace_intents` 保存独立创建意图；separate 的每个远端创建各有自己的 intent，不能用已完成项目推断未映射远端资源不存在。创建返回后把真实 channel ID 映射到该 intent，再保存 preparing 归属记录。Git 副作用之前记录已分配候选路径，只有 create 返回成功才标记实际资源已创建。每个项目成功后保存结果与 cwd 绑定；整次调用所有工作区、绑定和 `THREAD.md` 均成功后，才在一个 SQLite transaction 内将整批工作区标记 ready，随后分别完成对应 intent。背景只写确认成功的实际资源，不把计划值当成成功。

跨 Octo API 与 Git 不伪装成事务。任一步失败，报告已创建的 thread、已完成与未确认/失败的本地资源；整批保持不可派发，不自动开始实施。保留这些资源供明确处置，不自动回滚用户可能已经修改的 worktree。进程重启不会把 preparing/failed 自动提升为 ready，普通入站和工具派发都不能绕过就绪门禁、回落父群目录执行。

远端结果未知或 channel 映射/归属写入失败时，持久 intent 保持未完成，不把没有 workspace 行误判为普通可执行 thread。未映射 intent 保守阻止同组没有持久绑定的新 thread 自动执行，不阻断已有绑定的历史 thread；owner 可用显式 `/new <目录>` 建立确定绑定。已有 terminal workspace 行足以确认提交已完成时，恢复只补收其 intent，不重跑创建。intent 和归属都来自 SQLite，不另建文件状态源。

#### 调度、绑定与恢复

调度器把 shared 入口、项目 worktree 及其子目录解析为同一组资源键；任务、命令和删除共用目录租约。通过 `/new`、`/move` 或另一个聊天绑定到项目子目录，也不能与 shared 回合并发写同一 worktree。worker 停止未确认时，由持久资源映射把阻塞传播到同组目录；恢复仍先确认旧进程退出。

`/new` 无目录只换会话，保留当前工作区；显式 `/new <目录>`、`/move <目录>` 改变活动绑定，但不丢弃原资源归属。shared 入口或项目缺失时停止执行，不套用单仓库失效后的父目录回退。删除以持久归属为准，不以当前 cwd 猜测资源。

#### 合并与删除

shared 的 `/thread merge` 只回复「当前为多项目工作区，不支持 /thread merge。」；不创建任务、不探测或执行任何仓库合并。直接调用 `thread-merge` 同样拒绝。separate 与单项目继续使用既有单仓库合并流程；不提供跨仓库自动合并。

`thread-delete` 保持服务端删除与有界重试契约，之后仅处理本 thread 记录的资源。占用中的资源保留；未确认创建成功的资源保留并明确报告；删除前校验实际 worktree 的来源与分支，拒绝主检出或被替换的路径。默认保留脏 worktree，只有显式 `force=true` 才丢弃改动。入口只在其已知 worktree 被安全清理后以空目录删除，不递归删除未知文件。显式换目录后也不能误删新的用户目录。

#### 验收

1. 两个临时真实 Git 仓库经 host 创建 shared：一个 thread、一个非仓库入口、两个来源正确的独立 worktree；agent cwd 和背景实际路径一致，任务能访问两个项目，主检出未变。
2. 同样两仓库一次创建 separate：两个 thread 分别绑定正确仓库，返回可分别派发的 short id；背景只含目的/项目/路径。
3. shared 命令和 host 合并均拒绝，零合并任务与副作用；原单项目和 separate 合并保持正常。
4. 回归覆盖部分创建失败、说明写入失败、重启归属、目录并发、脏工作树保护、删除及显式绑定变化；新 SQLite schema 在旧数据库临时副本通过实际应用迁移与重复打开演练。
5. `bun run typecheck`、`bun test`、真实 Git/host 创建绑定 smoke 和进程恢复工作负载通过；线上 Octo API 未验收时明确标注，不把本地协议夹具当作线上验收。


### 可用性判定

`WorktreePort.worktrunk()` 返回可用（命令与版本展示）或缺失两类结果。候选命令按序探测：`wt`（PATH）→ `/opt/homebrew/bin/wt` → `/usr/local/bin/wt` → `~/.cargo/bin/wt` → `~/.local/bin/wt` → `~/.bun/bin/wt`；每个候选以 5 秒上限运行一次 `--version`，能够启动即选用该候选；版本文本、退出码及是否符合 semver 均不影响可用性判定。每次操作现探测，不缓存：安装或升级后无需重启即可生效。

### 创建（create-thread --worktree_name）

无显式项目的单仓库路径由调用方 cwd 解析仓库根；显式 projects 则各用传入 repo。按 canonical Git common-dir 串行分配目录/分支并执行创建，排除已占用分支和目录，候选为 `<repo>.<slug>` 与 `<slug>-<threadId>`；shared 使用入口内指定项目路径。随后：

- wt 可用：`wt switch --create <branch> --no-cd --format json --config-set worktree-path="<候选路径>"`（在仓库根运行，120 秒上限）。hooks 按项目配置执行（pre-start 阻塞）。返回 JSON 的 `path` 经 realpath 作为绑定 cwd。
- wt 不可用：现状 `omp worktree add <path> -b <branch> -C <baseDir> -q`。

目录与分支命名、占用退让继续沿用现有约定；错误单行 ≤200 字符，不重试、不自动清理。绑定继续写入 `chats.cwd_pinned`，资源状态和派发门禁按上述多项目 SDD 管理。

### 合并（/thread merge → thread-merge → wt merge）

命令路径（service）：`/thread merge` 按首词识别 merge，后接分隔符时保留 owner 补充说明，`merge-fix` 仍是创建意图。shared 先按上述固定提示拒绝，不产生任务。单项目/separate 在 channelType=5、该子区绑定隔离工作区（`cwd_pinned`）且 wt 可用时，释放命令 claim 后把合并指令按该聊天追加模式交付：可 steering 则并入，否则 durable 任务。非子区、未绑定工作区、部署无能力、wt 不可用时均只回复提示。

合并指令 prompt 与建区 prompt 同构（意图 → 确认收尾 → 调用 `octo_management` → 汇报并处置失败），内容为：owner 要求把本子区的隔离工作区工作合并回目标分支；合并由宿主执行，调用 `octo_management` 的 `thread-merge`，不要自己运行 wt；默认走 wt 默认 pipeline（暂存工作区改动并提交、squash、rebase 到目标分支、fast-forward，合并后删除工作区与分支）；这条命令即代表 owner 确认合并并清理——除非有明确必要理由（并说明）直接按默认参数执行，要保留工作区须说明理由并显式传 `remove:false`，其他参数确有需要时显式传入并说明理由；工作区删除后本子区 cwd 随之失效是该命令的既定结果、owner 已知，正文要求不反复说明、也不作为保留理由；成功汇报目标分支与结果细节；失败（rebase 冲突、pre-merge 检查失败）在本子区处置后重试并最终汇报；附当前隔离工作区路径。

工具路径（host-tools）：`octo_management` 新增动作 `thread-merge`，仅在当前聊天是子区时可执行（否则单行错误）。可选参数一一对应 wt 语义，全部省略即 wt 默认 pipeline：

- `target`：目标分支；省略用仓库默认分支（wt 默认）。
- `squash`、`commit`、`rebase`、`remove`、`ff`：布尔，默认 true；false 映射 `--no-squash`/`--no-commit`/`--no-rebase`/`--no-remove`/`--no-ff`。
- `stage`：`all`/`tracked`/`none`，默认 `all`。

执行前校验：该子区绑定存在且 `cwd_pinned`、部署提供隔离能力、wt 可用、工作区目录存在且是 linked worktree（`rev-parse --git-dir` ≠ `--git-common-dir`，防止在主检出上误合并）；随后在宿主内以工作区为 cwd 运行 `wt merge --format json`（600 秒上限、随工具取消信号中止、不传 `-y`）。成功返回 wt 的结果对象（`branch`/`target`/`squashed`/`rebased`/`committed`/`removed`）；失败返回单行 ≤200 字符原因——rebase 冲突时 wt 已把工作区留在 rebase 现场、目标分支未改动，agent 解决或按提示 abort 后可重试。

### thread-delete 联动清理

服务端删除成功后（既有瞬时失败重试语义不变），按持久资源记录清理本子区拥有的工作区；没有新资源记录的历史 `cwd_pinned` 绑定保留既有兼容路径。清理前取得相同调度资源租约，校验 worktree 来源与分支：

- 工作区目录不存在 → 说明「隔离工作区已不存在」。
- wt 可用 → `wt remove --foreground --format json <path>`（`force=true` 加 `-f`）；wt 在分支已合并时删除分支、未合并时保留。
- wt 不可用 → `git -C <path> worktree remove [--force] <path>`（git 自身拒绝主检出与非 worktree 路径；回退不删分支）。

`force` 默认 false；脏工作区保留并提示可传 `force=true`，显式 force 才丢弃未提交改动。清理失败只影响回执，保留未清理资源的归属记录；无隔离能力或无资源时不清理。已显式换到其他目录的 thread 仍清理其原来拥有的资源，不清理新的活动目录。shared 入口仅用空目录删除，未知文件保留。

### 模块与接口

- `src/contracts.ts`：`WorktreePort` 增加 `worktrunk()`/`merge()`/`remove()`；新增 `WorktrunkProbe`、`WorktreeMergeRequest`、`WorktreeMergeSummary`、`WorktreeRemovalRequest`、`WorktreeRemovalOutcome`。
- `src/workspace/worktrunk.ts`（新）：候选探测、wt switch/merge/remove 的 argv 与 JSON 解析、git worktree 删除回退、linked worktree 校验、共享的受限命令执行与单行错误文本。
- `src/workspace/worktree.ts`：保留 slug/候选命名与端口组装；工厂更名 `createWorktreePort`（创建优先 wt，不可用回退现状）。
- `src/threads/thread-target.ts`：新增 `threadChannelId(groupId, shortId)`——线程目标 ID 组装与解析同源。
- `src/service.ts`：`/thread merge` 分支、合并指令 prompt、提示文案、`/help` 与 `/thread` 用法更新。
- `src/host-tools.ts`：`thread-merge` 动作、`thread-delete` 的 `force` 与清理回执、工具描述更新。

## 错误与边界情况

- 未审批的 hook：非交互环境下 wt 以非零退出并给出说明，宿主压成单行 ≤200 字符回报；创建成为 `worktree_error`，合并成为工具错误，删除成为回执里的清理失败说明；宿主永不传 `-y` 或 `--no-hooks`。
- linked worktree 校验在本地判定，不依赖 wt 行为；工作区目录不存在时合并明确报「已不存在」。
- 合并冲突或 pre-merge 失败：工作区不删除、目标分支未改动；工具返回单行原因，agent 处置后重试。
- 合并成功且按默认删除工作区后：该子区绑定仍指向已删除目录，子区继续工作需显式 `/new <目录>`、`/move <目录>`，或删除该子区（此时清理回执说明工作区已不存在）。
- `thread-delete` 的服务端删除保持既有语义；本地清理遇到运行/排队/未确认停止的任务占用时保留资源并报告，不与任务并发删除。
- shared 创建前检查状态目录与入口祖先不位于 Git worktree 内；否则入口会向上发现用户仓库，故在创建 intent 或远端资源前明确拒绝，不自动选择其他目录。
- 探测逐候选串行；某个候选不可执行按缺失继续下一个；`--version` 输出无法解析视为缺失。

## 验收标准

1. wt 可用时 `create-thread --worktree_name` 经 `wt switch --create` 创建：目录 `<repo>.<slug>`（被占用退让 `<slug>-<thread_id>`）、分支同名、hooks 按 `.config/wt.toml` 执行，绑定 cwd 为该目录；wt 不可用时行为与现状一致（`omp worktree add`）。
2. 子区中 `/thread merge`：绑定隔离工作区且 wt 可用时注入合并指令 prompt（含 `thread-merge` 与汇报/处置要求），命令本身不执行 wt；主群、私聊、未绑定工作区的子区、部署无隔离能力、wt 不可用时只回一条提示、不注入任务。
3. `octo_management` 的 `thread-merge` 在合法子区上执行 `wt merge`（默认 pipeline，或逐参数映射的 `--no-*`/`--stage`/`target`），成功返回结果对象、失败返回单行 ≤200 字符原因；非子区、未绑定工作区、目录不存在或非 linked worktree、wt 不可用时明确拒绝且不执行。
4. `thread-delete`：按持久归属安全清理；验证占用、脏改动、force、路径替换、显式换绑和未知入口文件。历史单项目绑定保持兼容，服务端删除与重试语义不变。
5. `bun run typecheck` 与 `bun test` 通过；主规格与 AGENTS.md 同步；用临时真实仓库验证创建、单项目合并与删除，不在用户项目主检出执行验收副作用。

## 2026-11-02 隔离验收证据

- 两个临时真实 Git 仓库，经 `CoreService` → host tool → 真实 wt v0.76.0 创建 shared 与 separate；共享入口不是仓库，任务以持久绑定 cwd 读写正确来源的项目，背景包含实际路径。创建与隔离任务写入后主检出 HEAD 与 `git status --porcelain` 均保持不变。shared 命令与 host 合并均拒绝；两个 separate 工作区随后显式经真实 wt 合并到各自 main、清理并排定归档。
- 本机 wt v0.80.0 经真实 CoreService/host 工具完成 shared 与 separate 创建、子区任务访问、两个 separate 分支合并和 shared 资源删除；最终两个仓库的 git worktree list 均只剩主检出，删除回执含两个项目与空入口已清理。创建及隔离任务阶段主检出保持不变。版本探测只报告元数据，实际命令决定操作结果。真实 OMP 也完成了精确项目路径、release 起点、创建前计划回调及 Git 删除验收。
- 对运行中数据库做只读一致性备份，在私有临时副本经真实 `ChannelStore.open` 迁移并重复打开：原 17 张表逐行摘要及行数完全一致，新增 `thread_workspaces` 与 `thread_workspace_intents`；新归属、intent 和目录别名可写、可读、跨重开保留。未对生产数据库执行应用迁移。
- 在已创建 workspace 表但尚无兼容标记列的同一临时副本，再由应用迁移加入 `legacy_worktree_name`；原 17 张表摘要仍完全一致，原归属可读，带标记的记录写入、关闭、重开后保持。
- `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两种真实 CLI/worker/SQLite 工作负载均完成续接及排队恢复：`session_overlaps=0`、`duplicate_effects=0`、`disruption_notices=0`。
- Rebase 到包含群开通、成员上下文及 mention 流式输出的主分支后，保留普通共享目录的跨会话并行，并为持久工作区叠加资源组独占。`bun run typecheck` 通过；全量 `bun test` 为 989 pass、0 fail（74 文件、989 例）。同一集成版本再次通过本机 wt v0.80.0 的真实 host 创建、任务访问、separate 合并及 shared 删除 smoke，以及 graceful/SIGKILL 恢复基准。
- Octo API 与消息表面在本次 smoke 中使用隔离夹具，未完成线上 thread 创建、THREAD.md 写入权限或真实客户端验收；未重启常驻服务。

## 未决事项

无。
