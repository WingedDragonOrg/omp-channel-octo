# oco 入口与本机运维设计

日期：2026-09-16

## 背景与目标

channel 的日常操作目前散落在四处：`bun run src/cli.ts serve --config …` 启动、个人脚本 `restart.sh` 重启、`super` 客户端看状态与日志、`bun run test:preview-card` 发真实预览卡。其中重启脚本依赖部署方在 channel 配置里写死守护进程命令（`restart.command`），日志只有守护进程自己那份捕获。

目标：提供一个短入口 `oco`（`omp-channel-octo` 的别名，同一程序、同一 help），把跑服务、重启、看状态、看日志、发验收卡收进同一 CLI；守护进程与程序名由 oco 自己识别，不写进配置、不需要在命令行指定；服务运行日志由服务自持并落在 `~/.oco/logs/`。

## 范围

### 包含

- `package.json` 增加 `"oco": "src/cli.ts"` 别名与 `bun link` 安装步骤。
- 命令面：`start`、`run`、`check`、`restart`、`status`、`logs`、`test --card`、`launchd install|uninstall`、`help`。
- 守护进程探测抽象：本期实现 `super` 类型，按 PID 或 checkout 上下文自动识别程序名。
- 服务自持日志：`start`/`run` 把自身控制台输出追加到 `~/.oco/logs/channel-YYYY-MM-DD.log`，启动时清理 14 天前的文件；`oco logs` 读同一目录。
- 默认配置路径迁移到 `~/.oco/config/config.json`；`~/.oco/{config,logs}` 布局。
- 移除 `restart.command` 配置项，聊天内 `/restart`、`channel_control.restart` 与 `oco restart` 共用探测。
- launchd plist 的 stdout/stderr 路径改到 `~/.oco/logs/`。
- 删除 `restart.sh`；同步主规格、`AGENTS.md` 与运维 skill。

### 不包含

- 一次调用执行多条子命令（批量）。
- launchd 守护类型的实现（接口预留，本期探测不到即明确报错）。
- 状态目录迁移：SQLite 与附件仍在 `~/.local/state/omp-channel-octo`。
- channel 内既有命令（`/new`、`/reset`、`/move`、`/cancel`、`/status`、`/help`）语义变更。
- 日志远程上报、结构化解析、按大小切分。

## 已知约束

- `super add`/`super update` 不暴露按服务指定日志路径的参数（stack 里存在 `stdout_logfile`/`stderr_logfile` 字段，但默认 null），服务日志实际写在 `~/.super/logs/<id>.out|.err` 且不轮转；因此 oco 的日志读取只依赖自持文件，Super 那份捕获只是副产品。
- 探测可用的 Super 数据面：`super export --format json` 给出全部程序的 `name`/`command`/`args`/`cwd`；`super info <program>` 给出 `State:`/`PID:`/`Full Cmd:`/`CWD:`；`super list` 给出程序名与 PID。三者都是本机 CLI 调用，没有稳定的 JSON 状态接口。
- 同一 Bot 只允许一个实例（`CoreService` 抢 Bot 独占锁）；前台 `start`/`run` 与常驻实例并存时，第二个实例若先重新注册 Bot 就会中断在线会话，因此启动前先查该锁：锁由存活进程持有时直接报错退出，不注册 Bot。
- 服务运行期输出全部经 `console.log`/`console.error`；`src/omp/index.ts` 的 watchdog 直接写 `process.stdout`（协议标记），不得被日志层包装。
- 配置含 `botToken` 时文件权限必须为 `0600`（现有 `loadConfig` 校验），迁移必须保持该权限。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| `oco` 归属 | `package.json` 的 `bin` 增加 `"oco": "src/cli.ts"`，安装用 `bun link` | 一个程序两个名字，零分叉；`~/.bun/bin` 已在 PATH；软链跟随工作树，与 Super 启动的是同一份代码 |
| 守护进程识别 | 新增 `src/supervisor.ts`，`SupervisorTarget` 抽象 + 本期 `kind: "super"`；自动探测，不接受服务名参数、不读 channel 配置 | 部署姿势属于机器环境，不是 channel 配置；写进配置会让同一份代码换机器就要改配置 |
| 启动动词 | `start` 是唯一的服务进程入口（原 `serve`），`run` 是不参与守护进程的调试入口 | 两个前台动词语义靠"是否参与守护进程"区分；守护进程 Stack 与 launchd 运行 `start`，调试用 `run` |
| 重复启动 | 注册 Bot 之前检查 Bot 独占锁，被存活进程持有时报错退出 | 第二个实例的重新注册会切断在线实例的 WebSocket；用一个既有判据（锁）在动手之前拦住，而不是打完广告再失败 |
| 日志归属 | 服务自持：`start`/`run` 把 `console` 输出同时写终端与 `~/.oco/logs/channel-YYYY-MM-DD.log` | 不依赖守护进程的日志实现；前台运行也有文件；14 天窗口与附件清理一致 |
| `restart.command` | 删除 | 与自动探测重复；"未配置"分支不再有意义 |
| 目录 | `~/.oco/{config,logs}` | 与 `~/.super`、`~/.omp` 同风格；token 不进仓库；跨 worktree 与 checkout 移动稳定 |
| `--program` | 仅作为多候选时的逃生口，默认永远自动 | 常态不需要写程序名，歧义时不静默猜 |

## 设计

### 入口与安装

同一可执行文件两个名字：`omp-channel-octo`（既有）与 `oco`（新）。在 checkout 执行一次 `bun link`，Bun 生成 `~/.bun/bin/oco` 与 `~/.bun/bin/omp-channel-octo` 软链指向工作树；此后 `oco` 在任意 cwd 可用，且始终执行当前 checkout 的代码。Bun 会在脚本看到 `argv` 前解析软链，因此调用名只能从 shell 的 `_` 取；`_` 是这两个名字之一时 help 用它渲染，其他入口（`bun run src/cli.ts`、守护进程 Stack）回落到 `oco`，两个名字行为一致。

`package.json` 现有脚本收敛为 `check`、`start`、`launchd`（去掉 `serve`，不再有重复动词），保留为免安装路径；`oco` 是推荐入口。

### 目录布局

| 路径 | 用途 | 权限 |
| --- | --- | --- |
| `~/.oco/config/config.json` | channel 配置（原 `~/.config/omp-channel-octo/config.json`） | `0600` |
| `~/.oco/logs/channel-YYYY-MM-DD.log` | 服务自持日志 | `0600`（目录 `0700`） |
| `~/.oco/logs/launchd.out.log`、`launchd.err.log` | launchd plist 的 stdout/stderr 捕获 | 由 launchd 创建 |
| `~/.local/state/omp-channel-octo/` | SQLite、出站、附件（不变） | `0700` |

`DEFAULT_CONFIG_PATH` 改为 `~/.oco/config/config.json`。`~/.oco/config` 与 `~/.oco/logs` 由 oco 在写入前按需创建（`0700`）。

### 命令面

| 命令 | 行为 |
| --- | --- |
| `oco` / `oco help` | 打印帮助（按调用名渲染），列出全部命令 |
| `oco start [--config PATH] [--omp-command PATH]` | 前台运行 channel 进程（原 `serve` 语义），守护进程 Stack 与 launchd 运行的就是它；启动文件日志；按自身 PID 探测守护进程，探测到即提供重启端口；Bot 独占锁被存活进程持有时报错退出（退出码 1） |
| `oco run [--config PATH] [--omp-command PATH]` | 调试入口：同样前台运行 channel 进程，但不探测守护进程、不提供重启端口；其余行为（含重复启动拒绝、文件日志、steer/queue 等）与 `start` 相同 |
| `oco check [--config PATH]` | 既有语义（注册 Bot 并起停 fresh worker，不调用模型） |
| `oco restart [--program NAME] [--timeout SECONDS]` | 探测守护程序 → `super -y restart --wait-healthy --timeout <N> <program>` → `super info <program>`，打印重启前后 PID 与状态；PID 未变化时追加一行告警。`N` 默认 60 |
| `oco status` | 打印 oco 自己的摘要：守护类型、程序名、状态、PID、运行时长、配置路径、当前日志文件；无守护进程时说明未检测到并仍打印配置与日志路径 |
| `oco logs [--tail N] [--follow\|-f]` | 读 `~/.oco/logs/` 最新日志文件末 N 行（默认 200）；`-f` 先输出尾部再跟随，跨天自动切到新文件 |
| `oco test --card` | 应用 checkout `.envrc`（存在时）后运行 `bun test <checkout>/tests/preview-card.acceptance.ts`，退出码透传；不带 `--card` 时报用法 |
| `oco launchd install\|uninstall` | 既有语义；install 生成的 plist 把 stdout/stderr 指到 `~/.oco/logs/` |

### 服务进程：`start` 与 `run`

两者执行同一段服务启动逻辑（构建 transport、worker factory、`CoreService`，装日志 tee，等待 SIGINT/SIGTERM），差别只有守护进程参与度。相同的启动顺序是：先按状态盘中持久化的 Bot uid 检查 Bot 独占锁——锁由存活进程持有时打印 `同一 Bot 已有 channel 实例运行（PID n）` 并以退出码 1 结束，不注册 Bot；锁不存在、或持有者进程已不存在（崩溃残留）时继续注册并写入自己的锁。

- `start`：守护进程 Stack 与 launchd 运行的就是它。启动时以自身 PID 走一次守护进程探测；命中即注入重启端口，聊天内 `/restart` 与 `channel_control.restart` 因此可用（自迭代路径）；未命中则不注入端口，服务对这些请求回复"未检测到守护进程，无法重启 channel"。
- `run`：不探测守护进程、不注入重启端口；重复启动拒绝、日志与锁行为与 `start` 相同。调试时不会因为一次 `/restart` 而重启生产实例。

### 守护进程探测

`src/supervisor.ts` 提供探测与执行两层，纯逻辑与进程调用分离，便于测试注入：

- 候选来源：`super export --format json` 的程序列表（`name`、`command`、`args`、`cwd`）。
- 自进程模式（服务内触发重启）：调用方传入自己的 PID，对命令行走本 checkout CLI 的候选逐个读 `super info <program>`，PID 完全相等者即目标；无匹配即"未检测到"。
- 上下文模式（`oco restart`/`oco status`）：匹配 `cwd`（经 realpath 归一）等于本 checkout 根目录、且命令行指向本 CLI 的程序（含 checkout 路径、`cli.ts` 或 `oco` 入口）；恰好一个即目标，多个则报错并列出候选名（提示用 `--program`），零个即"未检测到"。
- 探测不到、`super` 不可执行或输出无法解析，都明确报错，不猜测、不回落。
- `restart` 的进程调用：`super -y restart --wait-healthy --timeout <N> <program>`；程序内触发的重启沿用既有方式——分离会话派生 `super -y restart <program>` 后本进程等待守护进程发 SIGTERM，既有 watchdog 告警逻辑不变。

`SupervisorTarget` 保留 `kind` 字段与"探测 → 重启 → 读状态"三个动作，launchd 等类型后续按同一形状补实现。

### 日志落盘与 `oco logs`

- `start` 与 `run` 启动时创建当天的 `channel-YYYY-MM-DD.log`，包装 `console.log`/`console.error`/`console.warn`/`console.info`：原始输出照常写终端，同时按行加 `[YYYY-MM-DD HH:MM:SS] ` 前缀写入文件；不包装 `process.stdout`/`process.stderr`，也不改动 OMP worker 的协议流。
- 跨天时后续行写入新日期的文件；启动时删除 `~/.oco/logs/` 中 14 天前的 `channel-*.log`。
- 日志目录不可创建或文件不可写时，打印一行错误并继续运行——日志失败不得阻断服务。
- `oco logs` 读取最新日志文件；`-f` 用有界轮询跟随文件增长并在日期切换后改读新文件；目录中没有任何日志文件时明确提示。

### 配置与迁移

- `config.example.json` 去掉 `restart` 块；`src/config.ts` 删除 `restartCommand`、`parseRestartCommand` 与 `restart` 键白名单项。
- `CoreService` 的 restart 端口由 cli 层注入：探测成功时提供"派生守护进程重启"的 request，探测失败时不提供端口，服务回复"未检测到守护进程，无法重启 channel"；`channel_control.restart` 与聊天内 `/restart` 共用该端口，既有排定、持久化重启请求与重启后通报逻辑不变。
- 迁移步骤：创建 `~/.oco/{config,logs}`（`0700`）→ 复制现有配置到 `~/.oco/config/config.json` 并去掉 `restart` 块（`0600`）→ 更新 Super stack 的 `--config` 并 `super apply -f` → 用 `oco restart` 与 `oco logs` 验证 → 确认无引用后删除旧 `~/.config/omp-channel-octo/`。

## 错误与边界情况

| 情形 | 行为 |
| --- | --- |
| 未知命令或参数 | 打印错误与帮助，退出码 1（既有行为） |
| `super` 不在 PATH | 明确报"未找到 super 可执行文件"，退出码 1 |
| 上下文模式命中多个程序 | 列出候选名并提示 `--program`，退出码 1 |
| 重启超时或 super 报错 | 透传 super 的输出与失败，退出码非零；channel 进程仍由守护进程管理 |
| 重启后 PID 未变化 | 打印告警行，命令仍以成功退出（super 已接受重启请求） |
| `oco logs` 无日志文件 | 提示日志目录与"尚未产生日志"，退出码 1 |
| `.envrc` 缺失（`test --card`） | 按既有 acceptance 测试的环境校验报错，退出码透传 |
| 日志目录不可写 | `start`/`run` 打印一行错误后继续运行 |
| 前台 `start`/`run` 与常驻实例并存 | 注册 Bot 之前报错退出（退出码 1），在线实例不受影响；锁的持有者进程已不存在时按残留处理，正常启动 |

## 验收标准

1. `bun link` 后 `~/.bun/bin/oco` 存在且可执行；在任意 cwd 运行 `oco help` 列出全部命令；以 `omp-channel-octo` 调用时帮助显示该名。
2. `oco status` 打印守护类型、程序名、状态、PID、运行时长、配置路径与当前日志文件；无守护进程时明确说明未检测到，并仍打印配置与日志路径。
3. `oco restart` 在终端自动识别程序并重启：服务 PID 变化、状态 Healthy、输出重启前后 PID；多个候选时报错并列出候选；无守护进程时退出码非零。
4. 未配置 `restart.command` 时，聊天内 `/restart` 与 `channel_control.restart` 仍能重启 channel 并通报新 PID；探测不到守护进程时明确拒绝且不猜测。
5. `oco start`/`oco run` 运行期间 `~/.oco/logs/channel-YYYY-MM-DD.log` 出现带 `[YYYY-MM-DD HH:MM:SS]` 前缀的行，终端输出保持原样；14 天前的 `channel-*.log` 在启动时被删除。
6. `oco logs --tail N` 打印最新日志文件末 N 行；`-f` 跟随新增行并在跨天后接上新文件。
7. `oco test --card` 在未手动加载 `.envrc` 的 shell 中也能发出真实预览卡，退出码透传。
8. 默认配置路径为 `~/.oco/config/config.json`；旧路径不再被读取；`config.example.json` 不含 `restart` 块；含 `botToken` 的配置文件仍要求 `0600`。
9. `restart.sh` 已删除；`AGENTS.md`、主规格与运维 skill 不再引用它，且 `oco restart`/`oco status`/`oco logs` 为其记载的操作路径。
10. `oco run` 启动的进程不提供重启端口：该实例里聊天内 `/restart` 与 `channel_control.restart` 回复"未检测到守护进程，无法重启 channel"；`oco start` 由守护进程拉起时同一请求可用。
11. 常驻实例在跑时再执行 `oco start`：命令以退出码 1 结束并打印 `同一 Bot 已有 channel 实例运行（PID n）`，全过程不发生 Bot 注册，在线实例继续收发；常驻实例退出后（锁文件残留、进程已不存在）`oco start` 正常启动。

## 未决事项

无。
