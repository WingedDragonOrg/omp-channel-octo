# 子区 worktree 的 Worktrunk 生命周期

日期：2026-09-17
修订日期：2026-11-03

## 背景与目标

本机存在可执行 Worktrunk（`wt`）时，`/thread` 的隔离工作区生命周期改由宿主（channel 服务）经 `wt` 执行：创建、合并与 `thread-delete` 联动清理。`wt` 不可用时创建保持现状（`omp worktree add`）；`/thread merge` 没有回退，只做明确提示。所有 `wt` 调用都在宿主进程内执行，子区 agent 不直接运行 `wt`；参数兼容性由 wt 的实际执行结果判定。

## 范围

### 包含

- `create-thread --worktree_name` 的 wt 创建路径（`wt switch --create`，完整语义 slug 与递增占用退让）与 `omp worktree add` 回退。
- `thread-delete` 的隔离工作区清理：新增可选布尔参数 `force`；默认只清理干净工作区，`wt` 不可用时回退 `git worktree remove`；清理结果进入同一条工具回执。
- 子区命令 `/thread merge`：宿主直接执行默认合并与清理，不按工作区资源限制并发；成功不创建 agent 任务，回执送达后归档子区。仅实际合并执行失败时交给本聊天 agent 修复，修复后通过同一宿主 `thread-merge` 核心重试。
- 子区命令 `/thread remove [--force]`：宿主清理持久拥有的单项目或 shared 多项目 worktree 与分支，全部确认完成后归档 Octo 子区、保留聊天记录。
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
| 接入层 | 命令与 `octo_management thread-merge` 共用宿主合并核心，底层经 worktree 端口执行 wt | 资格、资源租约、成功状态与归档只有一套实现 |
| 创建回退 | `wt` 不可用回退 `omp worktree add` | 不回归现状 |
| merge 形态 | `/thread merge` 先由宿主直接执行默认 pipeline；实际执行失败才交给 agent | 命令即 owner 对默认合并和清理的确认，正常路径不启动模型 |
| merge 默认行为 | 沿用 `wt merge` 默认 pipeline（squash、rebase、fast-forward、合并后删除工作区与分支） | owner 定稿：wt 默认删除 |
| merge 无回退 | `wt` 不可用时命令只提示、不注入任务 | 合并必须有可执行的 wt |
| 删除联动 | 默认只清理干净工作区；`force=true` 用 `-f`/`--force`；wt 不可用回退 git | owner 定稿：默认干净清理，force 由 agent 自行传参 |
| 目录命名 | shared 入口使用任务 slug、项目目录使用独立项目 slug；single/separate 保持 `<repo>.<任务 slug>`；实际重名才追加 `-2`、`-3` | 名称完整可读、项目顺序无关，wt 侧固定实际候选路径 |

## 设计

### 宿主合并进度卡 SDD

- **目标**：`/thread merge` 的宿主合并使用独立、可编辑的进度卡，显示可证实的阶段、运行时长和最终结果；不创建模型任务获取进度。
- **事件边界**：准备与校验、受管进程组身份确认及完整退出以宿主实际生命周期为准；进程组身份确认不表示 Worktrunk 已开始执行。`wt merge --format json` 只在结束时提供结构化结果，当前封装不流式呈现中间阶段。仅在 wt 明确提供可归因的事件时展示更细阶段或等待对象；静默期间刷新耗时，不推断 hook、Git 子步骤或百分比。
- **身份与隔离**：进度归属于命令的稳定入站身份，与普通任务、`/commit` 和失败修复任务的卡片分离。持久 lease 在 merge 实际运行期间允许命令 ACK；同聊天的查询/控制入站不得等待 merge 完成。重复入站与恢复沿用既有持久合并结果和进程保护，不重复执行合并。
- **投递与收尾**：卡片投递、编辑和计时刷新不得阻塞受管进程输出排空、合并结果持久化或结果回执。明确不支持可跳过卡片；请求失败或超时不得重发结果未知的创建请求，也不得把本地超时当作远端已排空，未知投递保持归档屏障。终态只陈述已确认的成功、失败、取消或未知结果及实际清理状态，归档排定不等于归档成功。成功回执与终态卡确认排空后才可归档；新入站持久递增归档代次，恢复仅在 lease 捕获代次仍与当前相同的情况下恢复归档。
- **边界**：资格拒绝和 fork 的纯归档路径保留原语义；只有实际执行失败进入既有修复任务。取消与停机仍等待受管进程组的真实退出，未知结果保持现有保护；不改变合并选项、资源并发或 listen 策略。
- **验收**：隔离真实 Git/worktree 与长时 hook 验证运行中建卡、同卡耗时刷新、非阻塞、结果和归档顺序；覆盖失败、取消、未知结果、能力缺失、重复入站及重启，并运行仓库回归和离线 restart benchmark。

### 多项目 thread 创建与工作区 SDD

#### 目标与对外契约

统一由 `octo_management create-thread` 创建子区、准备隔离工作区、持久化绑定并写入 Octo `THREAD.md`。具体工作只来自后续 `thread-dispatch` 的正文；创建本身不启动实施。保留当前群作用域、owner 权限、去重和单项目行为。

创建参数：

- `name`：子区名称；`worktree_name`：agent 提供的简短、完整语义任务名。显式 `projects` 创建必须提供；未提供 projects 时仍可省略以创建普通子区。
- `projects`：显式项目列表，每项 `{name, repo, base?}`；`repo` 是项目仓库的绝对路径，`base` 是可选起始 Git ref。未提供项目时，原单仓库工作区仍取调用方 cwd 所属仓库。
- `layout`：多项目必须由 agent 根据任务耦合选择 `shared` 或 `separate`，不要求 owner 每次决定；单项目可省略。
- `background`：为什么创建、涉及哪些项目的简短背景。宿主补充实际成功的 worktree 路径；不是任务、SDD、实施要求或验收要求。

`shared` 一次创建一个 thread，cwd 是宿主状态目录下独立的入口目录。入口本身不是 Git 仓库；每个项目在入口内拥有来自其 `repo` 的独立 Git worktree，不共享项目主检出，不把多个项目变成一个仓库。

`separate` 一次按项目分别创建多个 thread，每个 thread 绑定各自 `repo` 派生的 worktree。返回每个项目的 short id、实际路径、分支和创建结果。背景说明可列出相关子区，便于定位；后续派发不重写背景。

#### 可读命名与分配 SDD

**目标。** 新建资源使用完整可读的任务名和独立项目名；项目输入顺序不参与目录或分支命名。只改变新建分配，不迁移、重命名或重新推导已有入口、worktree、分支及持久归属。

**输入与长度。** agent 为 `worktree_name` 选择简短完整语义名，建议直接提供 ASCII 小写、单连字符分词的 slug。宿主沿用 ASCII 规范化（小写、连续非字母数字转为单连字符、去掉首尾连字符），不翻译中文，不调用模型、不按字符截断。任务 slug 与项目 slug 均须为 1–100 字符；工具输入 `worktree_name` 同样最多 100 字符。超长或无可用 ASCII 的输入在远端创建前拒绝，提示提供更短但完整的语义名称。冲突后缀不占基础 slug 的 100 字符额度，完整保留基础名称。

显式 `projects` 必须提供 `worktree_name`，包括 single；缺少时提示 agent 补齐任务 slug，不从线程显示名或项目名称猜任务。每个 `projects[].name` 既是显示名，也是独立项目 slug 的来源，通常直接使用项目/仓库名。规范化后无 ASCII、超长、重复显示名或重复 slug 均在任何远端创建前拒绝；要求使用 `admin-web`、`customer-web` 这类明确限定名，不自动添加项目序号。同一来源仓库沿用现有多 worktree 能力；使用 `backend-baseline`、`backend-feature` 等用途名称区分目录，不增加创建模式。

**布局与分支。**

| 资源 | 正常名称 | 真实冲突时 |
| --- | --- | --- |
| shared 任务目录与入口 | `<stateDir>/thread-workspaces/<任务 slug>/<主群作用域名称>` | 任务目录以 `<任务 slug>-2`、`-3` 退让；入口名称保持作用域名称 |
| shared 项目目录 | `<入口>/<项目 slug>` | 同父目录 `<项目 slug>-2`、`-3`，只在实际路径占用时退让 |
| single/separate worktree | `<repo>.<任务 slug>` | 同父目录 `<repo>.<任务 slug>-2`、`-3` |
| 各项目分支 | `<任务 slug>` | 仅在对应 Git 仓库重名时 `<任务 slug>-2`、`-3` |

不同 Git 仓库可以使用同一个完整任务分支名；分支重名不改变 shared 内正常项目目录名。项目排列序号不进入正常资源名。示例任务 `fix-login-flow` 的 shared 入口下为 `backend/`、`frontend/`，两个仓库分支均为 `fix-login-flow`；这些是规则示意，不是固定资源路径。

**所有权与竞态。** 保留创建前持久 intent、每个实际候选在副作用前持久化、Git common-dir 串行分配、整批 ready 和资源组租约。shared 入口独占创建，不复用已有目录、文件或符号链接；分配须同时排除持久归属预留和文件系统占用。冲突分配只选新候选，不删除或覆盖已有资源。路径候选改变时先更新持久计划，成功回执、绑定与资源记录使用实际确认路径/分支；不能将计划值当作成功，不能从 THREAD.md 推断归属。取消或异常恢复继续 fail closed，不重新生成旧名称或重放创建。

任务目录和入口的预留不等于创建确认：独占 mkdir 前同时保存入口下各项目的计划路径，`created=false`，实际 Git 候选确定后再由 `onPlanned` 更新。若任务目录被外部抢占且后续计划撤销或保存失败，这些未确认计划仍阻止删除目录；清理只在确认创建的项目均安全移除、且不存在未确认计划时尝试空目录删除。旧记录不重新推导资源名称。

**交付与说明。** 创建结果和 SQLite 保存实际路径及分支；Octo THREAD.md 只含背景、项目、实际成功路径及 separate 关联线程，不加入任务正文或验收。host-tool schema、`/thread` handoff 与操作说明共同要求 agent 提供任务 slug 和明确项目名。

**验收。** TDD 先观察完整任务名被截断、项目序号污染路径/分支的失败。真实 Git 回归覆盖 shared/single/separate、交换项目顺序、任务与项目长度边界、显示名/规范化冲突、入口/路径/分支占用、批内与并发重名；验证回执、SQLite、真实 Git 和 THREAD.md 一致。复用部分失败、取消、恢复、租约、dirty 保护及按归属删除测试，旧记录重开不改名。另以抛弃式真实仓库和本地 transport fixture smoke shared/separate，运行 typecheck、全量测试和离线 restart 工作负载；不操作常驻服务或用户已有资源。

#### shared 记忆作用域复用（2026-11-05）

- 新建 shared 使用目标主群当前持久 session 绑定的 cwd 推导 OMP 项目作用域；从子区发起时仍以主群绑定为准，不使用调用任务的偶然 cwd。缺少可确认的主群绑定时，在远端资源分配前拒绝。
- OMP 的项目标识按 Git 主检出根目录名（bare 仓库的 worktree 使用 bare common 目录名）、非 Git cwd 目录名的小写值计算，文件系统根目录的空名称使用 `unknown`；同名即同一业务作用域。在主群与子区的有效 Hindsight 配置一致时，`per-project` 复用 bank，`per-project-tagged` 复用 bank 和项目标签，`global` 复用全局 bank。项目级模式下不同名称的作用域不得被合并。配置由用户管理，宿主不复制、修改或校验 OMP 配置。
- 新建入口位于 `<stateDir>/thread-workspaces/<唯一任务目录>/<主群作用域名称>`，项目 worktree 位于入口下。任务目录独占分配、持久预留；入口与任务目录均记录所有权，绑定指向入口。已有工作区、session 与记忆不迁移。
- 创建、取消和重启恢复保留已知及未确认资源身份；清理按持久所有权先处理 worktree，再非递归删除已知入口及已知任务目录。未知文件、替换路径和未确认的创建均保留；旧记录继续按原入口清理，不推导其父目录所有权。
- 隔离验收覆盖 Git 主检出和 linked worktree、非 Git 目录、同名和不同名作用域、来源子区、任务目录冲突、持久化与重启、旧记录及安全清理；以真实安装的 OMP 和隔离 Hindsight 夹具验证 bank 和标签，不读写真实记忆。


#### 状态、归属与就绪

工作区资源归属独立于当前会话绑定，SQLite 记录 thread、layout、入口、每个项目来源仓库/起始 ref/计划与实际路径/分支/创建确认状态，以及 `preparing`、`ready`、`failed` 状态。仅原 `worktree_name` 单仓库调用带可选 `legacy_worktree_name` 持久标记，用于保持其未建立会话时工作区缺失后回退父群的既有行为；显式 projects 的 single/shared/separate 不继承该回退。运行时不解析 `THREAD.md` 决定路径、资源归属或执行资格。

每次远端 thread 创建之前，先在 `thread_workspace_intents` 保存独立创建意图；separate 的每个远端创建各有自己的 intent，不能用已完成项目推断未映射远端资源不存在。创建返回后把真实 channel ID 映射到该 intent，再保存 preparing 归属记录。Git 副作用之前记录已分配候选路径，只有 create 返回成功才标记实际资源已创建。每个项目成功后保存结果与 cwd 绑定；整次调用所有工作区、绑定和 `THREAD.md` 均成功后，才在一个 SQLite transaction 内将整批工作区标记 ready，随后分别完成对应 intent。背景只写确认成功的实际资源，不把计划值当成成功。

跨 Octo API 与 Git 不伪装成事务。任一步失败，报告已创建的 thread、已完成与未确认/失败的本地资源；整批保持不可派发，不自动开始实施。保留这些资源供明确处置，不自动回滚用户可能已经修改的 worktree。进程重启不会把 preparing/failed 自动提升为 ready，普通入站和工具派发都不能绕过就绪门禁、回落父群目录执行。

远端结果未知或 channel 映射/归属写入失败时，持久 intent 保持未完成，不把没有 workspace 行误判为普通可执行 thread。未映射 intent 保守阻止同组没有持久绑定的新 thread 自动执行，不阻断已有绑定的历史 thread；owner 可用显式 `/new <目录>` 建立确定绑定。已有 terminal workspace 行足以确认提交已完成时，恢复只补收其 intent，不重跑创建。intent 和归属都来自 SQLite，不另建文件状态源。

#### 调度、绑定与恢复

普通任务、恢复、goal 与会话控制按聊天和精确 session 调度。shared 入口、不同项目、项目子目录、相同 cwd 或父子聊天均不建立普通执行目录互斥；独立会话可以并行修改文件，由任务协调改动。持久资源归属仅服务于创建状态、绑定校验及清理。显式 commit/remove 按实际操作路径和 canonical Git worktree root 取得操作期排他；merge 不按资源限制并发。普通 worker 只登记共享使用关系，不彼此串行。worker 停止未确认时，受保护的非 merge 操作保持生命周期封锁，恢复先确认旧进程退出。统一并发契约见主规格「独立聊天并行调度 SDD（2026-11-03）」。

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

无显式项目的单仓库路径由调用方 cwd 解析仓库根；显式 projects 则各用传入 repo。按上述可读命名 SDD 分配：canonical Git common-dir 下串行排除已占用分支和目录，实际冲突以递增后缀退让；shared 使用独立项目路径，分支冲突不改变正常项目目录。随后：

- wt 可用：`wt switch --create <branch> --no-cd --format json --config-set worktree-path="<候选路径>"`（在仓库根运行，120 秒上限）。hooks 按项目配置执行（pre-start 阻塞）。返回 JSON 的 `path` 经 realpath 作为绑定 cwd。
- wt 不可用：现状 `omp worktree add <path> -b <branch> -C <baseDir> -q`。

目录与分支使用完整 slug，超长明确拒绝；错误单行 ≤200 字符，命令失败不重试、不自动清理。绑定继续写入 `chats.cwd_pinned`，资源状态和派发门禁按上述多项目 SDD 管理。

### 合并（/thread merge → thread-merge → wt merge）

#### 宿主优先合并 SDD

**目标与范围。** `/thread merge` 按首词识别 merge，后接分隔符时保留 owner 补充说明，`merge-fix` 仍是创建意图。命令即 owner 对默认暂存、提交、squash、rebase、fast-forward、删除 worktree 与已合并分支的确认。正常路径由宿主直接完成，不启动 worker、模型或 agent 任务。补充说明是失败接管时的上下文，不作为 shell 文本或 wt 参数执行。

**资格边界。** 沿用 owner、允许列表、mention 与消息身份门禁。shared 在探测 wt 前固定回复「当前为多项目工作区，不支持 /thread merge。」；非子区、未绑定隔离工作区、工作区未就绪、无隔离能力、无 wt、目录缺失、非 linked worktree 或持久来源/分支不符均明确拒绝，零合并副作用、零修复任务。不得操作主检出或被替换的路径。

**调度与取消。** 命令在授权后先持久受理并安装等待 checkpoint/会话意图，再交给服务拥有的后台 merge operation；同步 receive callback 与逐消息 `ackReady` 不等待规划、wt/hook、回执发送或归档完成。`WorktreePort.planMerge` 在副作用前确认 source canonical path、目标分支及目标当前检出的 worktree；默认分支通过 wt 自身的 `config state default-branch` 解析，显式 target 沿既有参数。合并不因 source、target 或其他工作使用资源而等待或拒绝，也不以 merge 阻挡其他独立工作；同一聊天及精确 session 顺序仍适用。执行前重新核对绑定、计划路径与资源身份，把已解析目标显式交给 wt，计划变化则拒绝执行。同聊天 `/cancel`、`/interrupt` 和只读查询走独立 control 受理，可在 merge 等待/执行期间到达；它们不绕过权限或进程退出确认。等待与执行响应取消和服务停止，取消不是可修复的 merge 失败。并发访问可能使 Git、Worktrunk 或 hook 产生冲突及部分副作用，ref CAS 和 index 锁不构成流水线整体事务。

**共享核心。** 命令与 `octo_management thread-merge` 共用 `threads/thread-merge.ts` 的资格校验、worktree 调用、成功归属更新与归档排定。工具支持下述可选参数；两个入口均不申请工作区资源并发租约，命令调用传默认选项。底层只把已实际运行的 wt merge 非零退出归类为可修复执行失败；启动前拒绝、取消和成功结果无法确认均不冒充冲突或检查失败，不自动重跑副作用。

**成功交付。** 成功须同时确认原 worktree 路径消失和整个 wt/Git/hook 进程组退出，不只依赖 wt JSON 的 `removed:true`。Worktrunk 可在主命令退出后留下空占位目录并异步清理；宿主在原合并的超时和取消预算内等待自然收尾，不把合法清理子进程当作遗留进程提前杀掉。已知结果先写入 `host_merge_leases`，再将实际来源分支、目标分支与清理结果送入稳定身份的持久 outbox；失败修复同样使用稳定任务身份，交付持久确认之后才 settle 结果记录。启动只补交付、不重跑 merge；恢复归档还须等待遗留进度卡恢复结束及 transport 就绪。工作树删除造成 cwd 消失不阻止回执或归档。业务完成、回执网络送达与归档是三个边界：merge outcome 持久后后台 operation 可结束，归档仍须等待成功回执确实 `sent` 且既有正文/进度卡输出排空。新授权且可靠受理的输入取消排定，原命令重投去重不取消；服务停止放弃排定，归档沿用既有瞬时失败重试及有界失败通知。回执尚未送达或已永久失败时不能先归档。

**失败接管与去重。** 实际 rebase 冲突、pre-merge hook 非零退出保留现场，将真实、脱敏、单行有界诊断、工作区路径、原命令与 owner 补充说明交给该聊天 agent；修复后调用宿主 `thread-merge` 重试，不由 agent 直接运行 wt。原命令持久 claim 始终保留，失败接管使用稳定消息身份，重投与重启不能再执行一次成功合并或创建第二个修复任务。外部 Git 副作用与 SQLite 不是事务；结果不明时保持已受理状态、停止自动重放，不能猜测成功或重新合并。

**进程所有权与恢复。** 在 wt spawn 前将 source＋真实 target checkout 计划路径作为 `host_merge_leases.resource_cwds` 持久化，再记录可验证的 PID、PGID 与进程身份；旧行无该列值时仅保留已知 source cwd，不猜测目标。watchdog 覆盖宿主意外退出，正常取消确认整个 Git/hook 进程组退出。进程退出与业务结果确认分开：成功结果或明确执行失败尚未持久确认的记录仍是未终结操作。启动时在恢复任务之前核验遗留进程并尝试停止；未确认终结的原合并绝不自动重放，结果未知不能猜测成功。merge 路径记录不限制其他独立工作的并发。schema 变更须在真实数据库的只读一致性备份上以应用迁移演练并重复打开，验证旧表逐行摘要不变。

**验收。** 先运行零 agent 成功路径的失败回归；实现后验证宿主真实合并、目标内容、worktree/分支删除、回执先于唯一归档。用临时真实 Git 仓库分别制造 rebase 冲突与 pre-merge 失败，验证实际执行在接管之前、现场保留且接管一次、修复后工具重试成功。覆盖资格拒绝、重复投递/重启、运行与排队占用、跨聊天资源别名、取消/停止、cwd 删除及归档失败。只在临时仓库执行合并/删除验收，不触碰常驻服务。

工具路径（host-tools）：`octo_management` 新增动作 `thread-merge`，仅在当前聊天是子区时可执行（否则单行错误）。可选参数一一对应 wt 语义，全部省略即 wt 默认 pipeline：

- `target`：目标分支；省略用仓库默认分支（wt 默认）。
- `squash`、`commit`、`rebase`、`remove`、`ff`：布尔，默认 true；false 映射 `--no-squash`/`--no-commit`/`--no-rebase`/`--no-remove`/`--no-ff`。
- `stage`：`all`/`tracked`/`none`，默认 `all`。

执行前校验：该子区绑定存在且 `cwd_pinned`、部署提供隔离能力、wt 可用、目录存在且是 linked worktree（`rev-parse --git-dir` ≠ `--git-common-dir`），并与持久归属中的来源仓库和分支相符；随后宿主以工作区为 cwd 运行 `wt merge --format json`（600 秒上限、随取消信号中止、不传 `-y`）。成功返回 wt 的结果对象（`branch`/`target`/`squashed`/`rebased`/`committed`/`removed`）；失败返回脱敏单行 ≤200 字符诊断。rebase 冲突保留现场，agent 解决或按诊断 abort 后通过工具重试；较晚阶段失败不猜测目标分支或清理是否已发生。

### thread-delete 联动清理

服务端删除成功后（既有瞬时失败重试语义不变），按持久资源记录清理本子区拥有的工作区；没有新资源记录的历史 `cwd_pinned` 绑定保留既有兼容路径。清理前针对实际项目路径和入口取得操作期排他，检查其他活跃及已接受的使用者，校验 worktree 来源与分支；只豁免调用任务本身。普通任务之间不持有这些排他资源：

- 工作区目录不存在 → 说明「隔离工作区已不存在」。
- wt 可用 → `wt remove --foreground --format json <path>`（`force=true` 加 `-f`）；wt 在分支已合并时删除分支、未合并时保留。
- wt 不可用 → `git -C <path> worktree remove [--force] <path>`（git 自身拒绝主检出与非 worktree 路径；回退不删分支）。

`force` 默认 false；脏工作区保留并提示可传 `force=true`，显式 force 才丢弃未提交改动。清理失败只影响回执，保留未清理资源的归属记录；无隔离能力或无资源时不清理。已显式换到其他目录的 thread 仍清理其原来拥有的资源，不清理新的活动目录。shared 入口仅用空目录删除，未知文件保留。

### 宿主清理并归档命令 SDD（2026-11-03）

owner 已确认 `/thread remove [--force]` 的以下契约，并授权按 TDD 实施：

- **入口与范围**：仅在当前子区执行，沿用 owner、allowlist、mention 和 durable command claim；只把完整首词 `remove` 识别为控制命令，`remove-fix` 等仍走既有任务语义。只接受可选 `--force`，其余参数返回用法。控制路径不下载附件、不回答 pending ask、不创建模型或 agent 任务，不执行 Git 合并，不删除远端 Octo 子区，不产生合并成功事件或创建方通知。
- **默认保护**：按持久所有权清理 single、separate 和 shared 的资源，不从当前 cwd、路径命名或 THREAD.md 推断。默认要求 worktree 干净，且 Git 确认关联分支已并入来源仓库默认分支；dirty、未合并或无法确认时保留该项目资源并报错，提示显式 `--force`。成功清理必须删除 worktree 及其关联分支，不默默保留分支。
- **默认分支判定**：使用本地 `worktrunk.default-branch`、已缓存 remote HEAD 或可唯一确定的本地默认分支；不为清理发起网络查询。默认安全判定采用 Git 提交祖先关系，squash/rebase 后无法按该关系确认时仍要求显式 force。
- **共享清理策略**：从现有 `thread-delete` 提取唯一资源清理流程，调用方显式选择是否要求删除分支；只有新命令要求 `deleteBranch: true`，既有 `thread-delete` 的分支保留/删除策略不变。工作树移除与分支删除分别确认，不能因为一次 remove 调用返回成功就清空所有归属；worktree 已删但分支失败时保留可追溯的 branch-only 残留和原资源路径，供新命令核对。
- **force 授权**：`--force` 明确授权丢弃已记录 worktree 内的未提交改动（含未跟踪文件），并删除未合并分支；唯一提交可能失去引用。force 不绕过 linked worktree、来源仓库、分支、路径身份、资源占用或进程退出确认，也不删除源仓库、主 checkout、未知路径或 shared 入口内的未知文件。Worktrunk 的 `-f` 与 `-D` 授权不同，不得仅以 `-f` 代替本命令的完整 force 契约。
- **缺失与换绑**：显式 `/new <dir>` 或 `/move <dir>` 不改变原资源归属，不清理新的用户目录。worktree 已缺失时仍核对记录中的分支；只有关联分支也已删除，才确认该项目完整清理。分支仍在其他 worktree 检出时拒绝删除。资源已全部不存在可完成归档，不要求先恢复 cwd。未确认创建或归属不完整的资源保留并说明。
- **调度与进程**：复用工作区清理所有者、完整资源组租约与现有进程/watchdog 机制；子目录绑定、task、goal、commit 和未确认退出的非 merge 进程参与占用判断。merge 不参与资源并发限制。占用时拒绝而不强杀未知进程。租约覆盖身份核验、破坏性操作、整个受管进程组确认退出及结果持久化。不能因删除成功后资源映射缩小而提前释放原资源组。
- **去重与恢复**：破坏性副作用前记录命令及逐项执行意图、进程所有权；逐项持久化实际 worktree/分支结果和残留。重复消息不再次执行。取消或停机停止后续项目，确认在途进程组退出后才释放资源；未确认退出或结果未知时保留资源阻塞，不自动重放破坏性操作。确认结果的恢复只交付稳定回执，不重做 Git。
- **异常分流**：普通、已确认退出的项目清理失败可以继续其他项目；进程退出未知或取消/停机立即停止后续项目。受管步骤在副作用前保存 intent、spawn 身份并在整组退出后确认；未知步骤与原完整资源组的阻塞一起持久保留，不能沿用现有文本错误处理后无条件放行的语义。
- **部分失败**：安全可清理的项目可独立完成，已成功项不回滚。任何残留、未知结果、分支未删或 shared 入口非空均不归档；回执逐项说明已清理结果与残留路径/原因，残留所有权持久保留，新消息发起的命令仅处理剩余资源。shared 入口仅在项目清理确认后用空目录删除。
- **归档与反馈**：全部资源清理确认后，持久稳定回执分别说明项目、分支实际状态和“归档已排定”。复用 `ThreadArchiveScheduler`，指定回执 `sent`、该聊天正文及进度卡终态输出排空后才执行归档。成功后不再往子区发消息，失败沿现有瞬时错误最多五次、间隔五秒及永久错误有界通知契约；新授权输入在可靠受理时取消，停机放弃仍有效。归档等待由后台 operation 独立拥有，不延长输入 ACK；归档状态不冒充清理结果。
- **执行与回执**：有 wt 时受管执行 `wt remove --foreground --no-delete-branch`，否则受管执行 `git worktree remove`；其后以预读 OID 条件删除记录分支引用。每个步骤独立记录进程身份，分支移动或锁失败保留分支并报告真实有界诊断。清理回执逐项携带项目、实际路径、分支及清理/缺失/残留状态；超过 3000 UTF-8 bytes 时按既有分段规则保存稳定身份的多个 outbox 行，全部回执分段 `sent` 才允许归档。
- **验收**：先证明宿主命令缺失行为，再验证单项目/shared、子目录和换绑、缺失 worktree/分支、dirty/未合并/default/force、分支另处检出、未知路径、部分失败、重复消息、取消/停止/恢复及归档顺序。真实 Git/process smoke 只用抛弃式仓库和本地 transport fixture，目标分支 HEAD 不因 remove 被改写，零额外 agent 任务。执行相关 core/workspace/archive/host-tools 回归、typecheck、全量测试；涉及生命周期或持久化时执行 restart benchmark 和抛弃式迁移演练。不自动提交、推送、合并或重启常驻服务；线上边界单独报告。

### 模块与接口

- `src/contracts.ts`：`WorktreePort` 增加 `worktrunk()`/`merge()`/`remove()`；新增 `WorktrunkProbe`、`WorktreeMergeRequest`、`WorktreeMergeSummary`、`WorktreeRemovalRequest`、`WorktreeRemovalOutcome`。
- `src/workspace/worktrunk.ts`（新）：候选探测、wt switch/merge/remove 的 argv 与 JSON 解析、git worktree 删除回退、linked worktree 校验、共享的受限命令执行与单行错误文本。
- `src/workspace/worktree.ts`：完整 ASCII slug、common-dir 内独立分支/路径递增分配和 `createWorktreePort` 端口组装（创建优先 wt，不可用回退 OMP）。
- `src/workspace/thread-workspaces.ts`：创建前任务与项目命名校验、shared 入口持久预留与独占创建、项目路径组合、整批资源就绪及实际路径背景发布。
- `src/threads/thread-target.ts`：新增 `threadChannelId(groupId, shortId)`——线程目标 ID 组装与解析同源。
- `src/commands/thread-commands.ts`、`src/service.ts`：宿主优先命令编排、失败接管、回执与归档接线；`TaskScheduler` 提供工作区独占。
- `src/threads/thread-merge.ts`：命令与 host tool 共用的资格、合并与成功收尾核心；`src/host-tools.ts` 保留工具参数映射与 `thread-delete` 清理回执。
- `src/threads/thread-workspace-cleanup.ts`：`thread-delete` 与 `/thread remove` 共用的持久资源清理、逐项归属更新及空入口处置；旧删除工具保留原分支策略。
- `src/store/store-host-merges.ts`：同一受管宿主进程台账按 `operation_kind` 区分 merge/remove；remove 保存步骤、原完整资源组和待交付结果，不产生合并成功事件。

## 错误与边界情况

- 未审批的 hook：非交互环境下 wt 以非零退出并给出说明，宿主压成单行 ≤200 字符回报；创建成为 `worktree_error`，合并成为工具错误，删除成为回执里的清理失败说明；宿主永不传 `-y` 或 `--no-hooks`。
- linked worktree 校验在本地判定，不依赖 wt 行为；工作区目录不存在时合并明确报「已不存在」。
- 合并冲突或 pre-merge 失败：保留工作现场，交付真实有界诊断供 agent 修复并通过 host tool 重试；不得声称所有非零退出都保证目标分支未变，失败可能发生在 pipeline 的较晚阶段。
- 合并成功且按默认删除工作区后：该子区绑定仍指向已删除目录，子区继续工作需显式 `/new <目录>`、`/move <目录>`，或删除该子区（此时清理回执说明工作区已不存在）。
- `thread-delete` 的服务端删除保持既有语义；本地清理遇到运行/排队/未确认停止的任务占用时保留资源并报告，不与任务并发删除。
- shared 创建前检查状态目录与入口祖先不位于 Git worktree 内；否则入口会向上发现用户仓库，故在创建 intent 或远端资源前明确拒绝，不自动选择其他目录。
- 探测逐候选串行；某个候选不可执行按缺失继续下一个；`--version` 输出无法解析视为缺失。

## 验收标准

1. wt 可用时 `create-thread --worktree_name` 经 `wt switch --create` 创建：single/separate 目录 `<repo>.<完整任务 slug>`、分支为任务 slug，shared 入口为任务 slug、项目目录独立命名；实际占用才递增后缀。hooks 按 `.config/wt.toml` 执行，绑定 cwd 为实际路径；wt 不可用时通过 `omp worktree add` 使用相同分配结果。
2. 子区中 `/thread merge`：绑定隔离工作区且 wt 可用时宿主先执行默认合并，成功零 agent 任务、清理工作树和分支，回执送达及进度卡排空后归档一次；实际执行失败才接管修复。资格拒绝只回提示、不执行、不创建任务；重投、重启、忙碌、取消和停止保持独占及去重边界。
3. `octo_management` 的 `thread-merge` 在合法子区上执行 `wt merge`（默认 pipeline，或逐参数映射的 `--no-*`/`--stage`/`target`），成功返回结果对象、失败返回单行 ≤200 字符原因；非子区、未绑定工作区、目录不存在或非 linked worktree、wt 不可用时明确拒绝且不执行。
4. `thread-delete`：按持久归属安全清理；验证占用、脏改动、force、路径替换、显式换绑和未知入口文件。历史单项目绑定保持兼容，服务端删除与重试语义不变。
5. `bun run typecheck` 与 `bun test` 通过；主规格与 AGENTS.md 同步；用临时真实仓库验证创建、单项目合并与删除，不在用户项目主检出执行验收副作用。

## 2026-11-02 隔离验收证据

- 两个临时真实 Git 仓库，经 `CoreService` → host tool → 真实 wt v0.76.0 创建 shared 与 separate；共享入口不是仓库，任务以持久绑定 cwd 读写正确来源的项目，背景包含实际路径。创建与隔离任务写入后主检出 HEAD 与 `git status --porcelain` 均保持不变。shared 命令与 host 合并均拒绝；两个 separate 工作区随后显式经真实 wt 合并到各自 main、清理并排定归档。
- 本机 wt v0.80.0 经真实 CoreService/host 工具完成 shared 与 separate 创建、子区任务访问、两个 separate 分支合并和 shared 资源删除；最终两个仓库的 git worktree list 均只剩主检出，删除回执含两个项目与空入口已清理。创建及隔离任务阶段主检出保持不变。版本探测只报告元数据，实际命令决定操作结果。真实 OMP 也完成了精确项目路径、release 起点、创建前计划回调及 Git 删除验收。
- 对运行中数据库做只读一致性备份，在私有临时副本经真实 `ChannelStore.open` 迁移并重复打开：原 17 张表逐行摘要及行数完全一致，新增 `thread_workspaces` 与 `thread_workspace_intents`；新归属、intent 和目录别名可写、可读、跨重开保留。未对生产数据库执行应用迁移。
- 在已创建 workspace 表但尚无兼容标记列的同一临时副本，再由应用迁移加入 `legacy_worktree_name`；原 17 张表摘要仍完全一致，原归属可读，带标记的记录写入、关闭、重开后保持。
- `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 两种真实 CLI/worker/SQLite 工作负载均完成续接及排队恢复：`session_overlaps=0`、`duplicate_effects=0`、`disruption_notices=0`。
- 历史集成验收：当时版本通过 `bun run typecheck` 及全量 `bun test`（989 pass、0 fail，74 文件），并通过本机 wt v0.80.0 的 host 创建、任务访问、separate 合并、shared 删除 smoke 及 graceful/SIGKILL 恢复基准。当前普通执行并发约定以主规格「独立聊天并行调度 SDD（2026-11-03）」为准。
- Octo API 与消息表面在本次 smoke 中使用隔离夹具，未完成线上 thread 创建、THREAD.md 写入权限或真实客户端验收；未重启常驻服务。

## 2026-11-03 宿主优先合并验收

- 零 agent 成功回归先在原实现失败：预期宿主 merge 调用一次，实际为空。实现后的命令、调度与持久恢复子集为 40 pass、0 fail。
- 临时真实仓库经当时命名的 `CoreService.handleInbound("/thread merge")`（现行入口合同为同步 receive / `submitInbound` 等待持久受理）→ 真实 Worktrunk v0.80.0 → Git 默认 pipeline 完成一次合并：已提交内容与未提交文件均进入目标分支，worktree 路径和已合并分支均消失；worker 与 agent task 数均为零，回执先于唯一归档。同一命令重投没有第二次合并。该历史观察不表示现行 `submitInbound` 等待完整合并。
- 真实 wt 的异步清理已单独取证：主命令退出后保留空占位目录，原进程组中的后台清理约一秒后移除路径及 trash。宿主成功边界等待这一步结束；取消和超时仍受原合并预算约束。
- SQLite 以运行中数据库的只读一致性备份演练应用迁移并重复打开：原 20 张表的行数和逐行摘要完全一致，新增 `host_merge_leases`。临时源码副本移除该表的创建迁移后，相关行为测试按预期因表缺失失败；仓库源码未参与该变异。
- 真实 CLI/worker/SQLite 的 graceful 和 SIGKILL 基准均恢复在途及排队任务：`session_overlaps=0`、`duplicate_effects=0`、`disruption_notices=0`。
- 最终 `bun run typecheck` 通过；完整 `bun test` 为 1023 pass、0 fail，覆盖 77 个文件。临时真实 Git 的 rebase 冲突与 pre-merge hook 失败均先由宿主执行，再由唯一修复任务保留现场、处置并通过 host tool 重试收尾。
- Octo 消息与归档使用隔离 transport 夹具；本次未操作线上子区或重启常驻服务。

## 2026-11-03 可读工作区命名验收

- TDD 先观察到底层 5 例、多项目层 13 例可读命名失败：完整任务名被截断、入口不含任务名、项目序号进入路径与分支、缺少任务 slug 仍创建，以及占用退让不符合约定。另以持久候选写入后注入外部空目录、拒绝计划撤销保存的确定性用例，先确认 thread-delete 会删掉外部目录，再验证其被保留。
- 工作区、归属、host tools 与 thread 命令相关 6 文件回归为 112 pass、0 fail；覆盖 100 字符边界、分支/路径独立递增、项目顺序、同仓库用途目录、持久入口预留、并发、失败批次和旧记录。`bun run typecheck` 通过。
- 全量 `bun test` 在独立临时 `TMPDIR` 中执行：1229 pass、0 fail，覆盖 85 文件、7623 条断言。初次与相关回归并发时，固定夹具身份 `bot-fixture` 的跨进程独占锁冲突导致 4 例启动失败；隔离测试锁命名空间后完整通过，未为此修改产品锁逻辑。
- 两个抛弃式真实 Git 仓库经 `CoreService` → host tool → 本机真实 wt v0.80.0 创建两批 shared（第二批交换项目顺序）和一批 separate，共 6 个项目 worktree。观察到完整任务入口、稳定 `backend/` 与 `frontend/` 目录、不同仓库同任务分支，以及第二批真实冲突时的 `-2` 后缀。SQLite、工具回执、实际 Git 路径/分支和本地 THREAD.md 说明一致。
- 同一 smoke 加入历史入口及带序号的项目路径，停止并重建服务后逐值比较全部归属不变；随后通过 thread-delete 清理新旧测试资源。来源主检出的 HEAD 与干净状态不变，最终每仓库仅保留主 worktree。
- 真实 wt 在第一项目创建成功后取消批次：结果不 ready、派发拒绝；重启保留已确认和未确认计划。thread-delete 只清理确认成功的 worktree，保留未确认计划与入口；来源仓库保持干净且仅有主 worktree。
- 离线真实 CLI/worker/SQLite restart 基准的 graceful 与 SIGKILL 两场均续接在途任务并恢复队列：`seamless_restart_cases=2`、`session_overlaps=0`、`duplicate_effects=0`、`disruption_notices=0`。
- Octo 表面使用本地 transport 夹具，未连接线上 Bot；测试仓库、状态目录和一次性脚本均仅用于隔离验收。没有提交、推送、合并或重启常驻服务。

## 2026-11-03 清理并归档验收

- TDD 先观察到宿主命令未清理、默认误移除未合并 worktree、分支遗留、缺失路径和分支锁诊断缺失的失败回归；实现后覆盖真实 Git 的默认保护、force、分支 CAS、其他 worktree 检出、路径替换、shared 部分结果和取消。
- 实际 Worktrunk v0.80.0 在抛弃式仓库经 CoreService 完成普通与 force 两种清理：两个 worktree 及关联分支均已删除，来源目标分支 HEAD 不变，回执先于唯一归档，重复命令不重复归档，额外 worker 和合并事件均为零。Octo transport 使用本地夹具。
- 当前源码复核另以实际 wt v0.80.0 经宿主批次创建 shared（一个子区、两个项目）和 separate（两个子区、每区一个项目），确认持久状态 ready、项目分支及来源 common-dir 正确；随后由 `/thread remove` 清理四个真实 worktree 与分支、归档三个夹具子区。dirty/未合并项目先被默认命令保留，再由显式 force 清理；两个来源仓库 HEAD 均不变，无额外 worker、合并事件或线上 Octo 请求。
- 真实阻塞删除进程组在 `/cancel` 后确认退出，原 worktree、分支及目标 HEAD 保留；重启回归验证已退出但业务结果未知时保留原完整资源组阻塞，已知结果只恢复回执与归档、不重做删除。长回执逐项覆盖全部项目，任一回执分段未送达均不归档。
- 运行中状态库经只读一致性备份，在私有临时副本调用应用迁移并连续打开三次；原 25 张表、8,802 行的所有旧列逐行摘要与行数一致，新增四列 `operation_kind`、`removal_step`、`removal_resources_json`、`removal_outcome_json`，`integrity_check=ok`。remove 步骤、资源组与结果跨重开保留，merge/remove 命名空间独立；原数据库未执行迁移。
- `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 均通过：恢复任务和排队任务各 2 项，`session_overlaps=0`、`duplicate_effects=0`、`disruption_notices=0`。
- `bun run typecheck` 通过；独立 TMPDIR 下完整 `bun test` 为 1249 pass、0 fail，90 个文件。测试未绕过生产 bot 独占锁。
- 未提交、推送、合并或重启常驻服务；未删除真实用户子区/工作树。线上归档权限、服务端解档处理时序及真实客户端归档状态未在本次验收。

## 未决事项

无。
