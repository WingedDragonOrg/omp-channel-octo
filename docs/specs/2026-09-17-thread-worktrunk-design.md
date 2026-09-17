# 子区 worktree 的 Worktrunk 生命周期

日期：2026-09-17

## 背景与目标

本机存在兼容 Worktrunk（`wt`，稳定 v0.76.x）时，`/thread` 的隔离工作区生命周期改由宿主（channel 服务）经 `wt` 执行：创建、合并与 `thread-delete` 联动清理。`wt` 不可用时创建保持现状（`omp worktree add`）；`/thread merge` 没有回退，只做明确提示。所有 `wt` 调用都在宿主进程内执行，子区 agent 不直接运行 `wt`。

## 范围

### 包含

- `create-thread --worktree_name` 的 wt 创建路径（`wt switch --create`，保留现有目录/分支命名与占用退让）与 `omp worktree add` 回退。
- `thread-delete` 的隔离工作区清理：新增可选布尔参数 `force`；默认只清理干净工作区，`wt` 不可用时回退 `git worktree remove`；清理结果进入同一条工具回执。
- 子区命令 `/thread merge`：只做检查并注入「合并指令」prompt（与建区 prompt 同构、内容不同），命令即 owner 对合并 + 清理的确认——正文要求除非有明确必要理由（并向 owner 说明）直接按默认参数执行，要保留工作区须说明理由并显式传 `remove:false`；经 `octo_management` 新增的 `thread-merge` 动作由宿主执行 `wt merge`。
- 主规格、AGENTS.md 与测试同步。

### 不包含

- worktree 列表、复用与切换（`wt list`、`wt switch` 复用已存在分支）。
- 主群与私聊中的 merge；`wt` 缺失或版本不符时的 merge 回退。
- worktree 路径模板的用户配置（wt 默认路径模板与现有命名一致；宿主用 `--config-set worktree-path` 固定到计算出的候选路径）。

## 已知约束

- 只接受稳定 v0.76.x：`wt --version` 输出解析出 `0.76.` 版本号才视为可用；其他版本视为不可用（合并提示版本不受支持）。
- 本项目 hooks 审批已完成（`.config/wt.toml`：pre-start `bun install`，pre-merge `bun run typecheck` + `bun test`）。宿主调用不传 `-y`/`--no-hooks`：已审批命令照常执行，未审批命令在非交互环境中由 wt 自己报错退出（单行有界回报），不依赖交互式审批。
- 常驻服务 PATH 由部署给定，Homebrew 目录通常不在其中；可用性判定必须同时覆盖 PATH 与常见安装位置。
- 命令都有超时与进程终止：创建 120 秒、合并 600 秒（pre-merge 会跑 typecheck 与全量测试）、删除 120 秒；错误单行、≤200 字符；不发散敏感信息。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 接入层 | 宿主侧执行 wt（`src/worktrunk.ts` + worktree 端口），agent 只经 `octo_management` 触发 | 与现有 create-thread 的 worktree 路径同构；审批、超时与错误边界集中在宿主 |
| 创建回退 | `wt` 不可用回退 `omp worktree add` | 不回归现状 |
| merge 形态 | `/thread merge` 只注入合并指令 prompt；命令即 owner 的合并 + 清理确认——除非有明确必要理由（并说明）按默认参数执行，保留工作区须说明理由并显式 `remove:false` | owner 定稿：命令发出即默认收尾，执行与参数仍由 agent 决定 |
| merge 默认行为 | 沿用 `wt merge` 默认 pipeline（squash、rebase、fast-forward、合并后删除工作区与分支） | owner 定稿：wt 默认删除 |
| merge 无回退 | `wt` 缺失或版本不符时命令只提示、不注入任务 | 没有兼容 wt 就没有可执行路径 |
| 删除联动 | 默认只清理干净工作区；`force=true` 用 `-f`/`--force`；wt 不可用回退 git | owner 定稿：默认干净清理，force 由 agent 自行传参 |
| 目录命名 | 保留 `<repo>.<slug>` 与 `<slug>-<threadId>` 退让；wt 侧用 `--config-set worktree-path` 固定到候选路径 | 验收要求目录/分支符合既有约定 |

## 设计

### 可用性判定

`WorktreePort.worktrunk()` 返回三类结果：可用（命令与版本）、缺失、版本不受支持。候选命令按序探测：`wt`（PATH）→ `/opt/homebrew/bin/wt` → `/usr/local/bin/wt` → `~/.cargo/bin/wt` → `~/.local/bin/wt` → `~/.bun/bin/wt`；每个候选以 10 秒上限运行一次 `--version`，退出码 0 且版本为 v0.76.x 才算可用。每次操作现探测，不缓存：安装或升级后无需重启即可生效。

### 创建（create-thread --worktree_name）

现有流程不变：由调用方 cwd 解析仓库根、确认名称可 slug、按 `git for-each-ref` 排除已占用分支、按 `stat` 排除已占用目录，选中候选（`<repo>.<slug>`，被占用退让 `<slug>-<threadId>`）。随后：

- wt 可用：`wt switch --create <branch> --no-cd --format json --config-set worktree-path="<候选路径>"`（在仓库根运行，120 秒上限）。hooks 按项目配置执行（pre-start 阻塞）。返回 JSON 的 `path` 经 realpath 作为绑定 cwd。
- wt 不可用：现状 `omp worktree add <path> -b <branch> -C <baseDir> -q`。

目录与分支命名、占用退让与失败语义保持现状（失败/超时/取消返回单行 ≤200 字符 `worktree_error`，不重试、不自动清理）；绑定继续写入 `chats.cwd_pinned`。

### 合并（/thread merge → thread-merge → wt merge）

命令路径（service）：`/thread merge`（`/thread` 参数去首尾空白后大小写不敏感地等于 `merge`）在 channelType=5、该子区绑定隔离工作区（`cwd_pinned`）且 wt 可用时，释放命令 claim 后把「合并指令」prompt 按该聊天的追加模式交付（与 `/thread <任务>` 相同：可接收 steering 的运行中回合并入，否则 durable 任务）。其余场合只回复一条提示、不注入任务：非子区（主群、私聊）、子区未绑定隔离工作区、部署无隔离能力、wt 缺失或版本不受支持（提示带上发现的版本）。

合并指令 prompt 与建区 prompt 同构（意图 → 确认收尾 → 调用 `octo_management` → 汇报并处置失败），内容为：owner 要求把本子区的隔离工作区工作合并回目标分支；合并由宿主执行，调用 `octo_management` 的 `thread-merge`，不要自己运行 wt；默认走 wt 默认 pipeline（暂存工作区改动并提交、squash、rebase 到目标分支、fast-forward，合并后删除工作区与分支）；这条命令即代表 owner 确认合并并清理——除非有明确必要理由（并说明）直接按默认参数执行，要保留工作区须说明理由并显式传 `remove:false`，其他参数确有需要时显式传入并说明理由；工作区删除后本子区 cwd 随之失效是该命令的既定结果、owner 已知，正文要求不反复说明、也不作为保留理由；成功汇报目标分支与结果细节；失败（rebase 冲突、pre-merge 检查失败）在本子区处置后重试并最终汇报；附当前隔离工作区路径。

工具路径（host-tools）：`octo_management` 新增动作 `thread-merge`，仅在当前聊天是子区时可执行（否则单行错误）。可选参数一一对应 wt 语义，全部省略即 wt 默认 pipeline：

- `target`：目标分支；省略用仓库默认分支（wt 默认）。
- `squash`、`commit`、`rebase`、`remove`、`ff`：布尔，默认 true；false 映射 `--no-squash`/`--no-commit`/`--no-rebase`/`--no-remove`/`--no-ff`。
- `stage`：`all`/`tracked`/`none`，默认 `all`。

执行前校验：该子区绑定存在且 `cwd_pinned`、部署提供隔离能力、wt 可用、工作区目录存在且是 linked worktree（`rev-parse --git-dir` ≠ `--git-common-dir`，防止在主检出上误合并）；随后在宿主内以工作区为 cwd 运行 `wt merge --format json`（600 秒上限、随工具取消信号中止、不传 `-y`）。成功返回 wt 的结果对象（`branch`/`target`/`squashed`/`rebased`/`committed`/`removed`）；失败返回单行 ≤200 字符原因——rebase 冲突时 wt 已把工作区留在 rebase 现场、目标分支未改动，agent 解决或按提示 abort 后可重试。

### thread-delete 联动清理

服务端删除成功后（既有瞬时失败重试语义不变），当被删子区存在 `cwd_pinned` 绑定时按序清理：

- 工作区目录不存在 → 说明「隔离工作区已不存在」。
- wt 可用 → `wt remove --foreground --format json <path>`（`force=true` 加 `-f`）；wt 在分支已合并时删除分支、未合并时保留。
- wt 不可用 → `git -C <path> worktree remove [--force] <path>`（git 自身拒绝主检出与非 worktree 路径；回退不删分支）。

`force` 是 `thread-delete` 的新可选布尔参数，默认 false：只清理干净工作区，有未提交改动时保留工作区，回执说明「可传 force=true 强制删除」；`force=true` 时丢弃未提交改动。清理失败只影响回执（删除已经生效）；回执形如「已删除线程 `<short_id>`；隔离工作区已清理（分支已删除）」或「…；隔离工作区未清理：<单行 ≤200 字符>」。部署无隔离能力或绑定非 `cwd_pinned` 时不清理、回执维持原文。

### 模块与接口

- `src/contracts.ts`：`WorktreePort` 增加 `worktrunk()`/`merge()`/`remove()`；新增 `WorktrunkProbe`、`WorktreeMergeRequest`、`WorktreeMergeSummary`、`WorktreeRemovalRequest`、`WorktreeRemovalOutcome`。
- `src/worktrunk.ts`（新）：候选探测、wt switch/merge/remove 的 argv 与 JSON 解析、git worktree 删除回退、linked worktree 校验、共享的受限命令执行与单行错误文本。
- `src/worktree.ts`：保留 slug/候选命名与端口组装；工厂更名 `createWorktreePort`（创建优先 wt，不可用回退现状）。
- `src/thread-target.ts`：新增 `threadChannelId(groupId, shortId)`——线程目标 ID 组装与解析同源。
- `src/service.ts`：`/thread merge` 分支、合并指令 prompt、提示文案、`/help` 与 `/thread` 用法更新。
- `src/host-tools.ts`：`thread-merge` 动作、`thread-delete` 的 `force` 与清理回执、工具描述更新。

## 错误与边界情况

- 未审批的 hook：非交互环境下 wt 以非零退出并给出说明，宿主压成单行 ≤200 字符回报；创建成为 `worktree_error`，合并成为工具错误，删除成为回执里的清理失败说明；宿主永不传 `-y` 或 `--no-hooks`。
- linked worktree 校验在本地判定，不依赖 wt 行为；工作区目录不存在时合并明确报「已不存在」。
- 合并冲突或 pre-merge 失败：工作区不删除、目标分支未改动；工具返回单行原因，agent 处置后重试。
- 合并成功且按默认删除工作区后：该子区绑定仍指向已删除目录，子区继续工作需显式 `/new <目录>`、`/move <目录>`，或删除该子区（此时清理回执说明工作区已不存在）。
- `thread-delete` 不为运行中任务做额外守卫（删除本身已是破坏性操作）；清理失败如实回报。
- 探测逐候选串行；某个候选不可执行按缺失继续下一个；`--version` 输出无法解析视为缺失。

## 验收标准

1. wt 可用时 `create-thread --worktree_name` 经 `wt switch --create` 创建：目录 `<repo>.<slug>`（被占用退让 `<slug>-<thread_id>`）、分支同名、hooks 按 `.config/wt.toml` 执行，绑定 cwd 为该目录；wt 不可用时行为与现状一致（`omp worktree add`）。
2. 子区中 `/thread merge`：绑定隔离工作区且 wt 可用时注入合并指令 prompt（含 `thread-merge` 与汇报/处置要求），命令本身不执行 wt；主群、私聊、未绑定工作区的子区、部署无隔离能力、wt 缺失或版本不受支持时只回一条提示、不注入任务。
3. `octo_management` 的 `thread-merge` 在合法子区上执行 `wt merge`（默认 pipeline，或逐参数映射的 `--no-*`/`--stage`/`target`），成功返回结果对象、失败返回单行 ≤200 字符原因；非子区、未绑定工作区、目录不存在或非 linked worktree、wt 不可用时明确拒绝且不执行。
4. `thread-delete`：干净工作区按口径清理（wt 缺失时 git 回退）；有未提交改动且未传 `force` 时工作区保留且回执说明可 `force=true`；`force=true` 时强制删除；工作区已不存在、部署无隔离能力、绑定非 `cwd_pinned` 时不做多余动作；服务端删除与重试语义不变。
5. `bun run typecheck` 与 `bun test` 通过；主规格与 AGENTS.md 与实现一致；真机验证：在本仓库用本机真实 wt 建一个子区 worktree 并跑一次创建 + merge（pre-merge 实际执行 typecheck/test）流程，记录证据。

## 未决事项

无。
