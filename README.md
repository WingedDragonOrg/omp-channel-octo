# omp-channel-octo

通过 Octo 私聊、群聊和 CommunityTopic 子区操作本机 [Oh My Pi（OMP）](https://github.com/can1357/oh-my-pi) 的常驻服务，使用 TypeScript 和 Bun 实现。

服务主动连接 Octo，通过 stdio RPC 启动独立的 OMP 子进程。每个聊天保有自己的持久会话、工作目录和模型设置，可在聊天中执行任务、追加指令、回答问题、收发文件，以及派发隔离工作区任务。

## 主要能力

- **持久会话**：会话与终端分离；支持新建、重置和保留历史的目录迁移。
- **任务调度**：按聊天和会话串行执行，多个聊天可绑定同一工作目录并行工作；新消息可并入运行中的回合，也可排队。
- **消息与文件**：流式编辑助手回复，接收入站附件和引用内容，支持语音转写及出站文件。
- **执行反馈**：展示工具、待办和子代理进度；支持交互问答卡片，也可直接用文字作答。卡片能力取决于 Octo 部署，卡片失败不阻断正文。
- **原生命令与通知**：未注册的 OMP slash 命令（如 `/autoresearch`）按独立任务原样转交 OMP 会话；扩展通知按级别标签独立投递，离线排队且跨服务重启可达。
- **持续目标**：`/goal` 通过原生结构化 RPC 管理目标；支持创建时 token 预算、跨回合续跑、暂停恢复及在途目标的重启续接。
- **群组上下文**：注入群与子区说明、增量聊天历史，按会话记录已读取的位置。
- **成员与点名**：每条新任务和 steering 获取当前群或子区成员信息；普通回复可生成结构化 @成员提醒。
- **隔离子区**：为子区创建 git worktree，派发任务，并通过 Worktrunk 合并和清理工作区。
- **恢复与投递**：SQLite 持久化任务、问题及出站队列；重连后恢复投递，已开始的任务不会因重启自动重跑。

## 安全边界

OMP 以 **yolo 模式**运行，channel 不增加逐次操作审批，也不是沙箱。助手具备运行账户的本机权限；请只在可信机器上使用，并谨慎开通群聊。

- 私聊与聊天命令仅供 owner 使用。
- 群聊和子区必须在允许列表内。已开通聊天中的其他成员可通过结构化 `@Bot` 发起任务、回答待处理问题，但不能执行聊天命令。
- owner 默认也需要 `@Bot`；只有服务端的群级免 @ 设置生效时才豁免。成员始终需要 @ 才能发起普通任务。
- 群内助手回复对成员可见，包括任务要求助手读取并回复的文件内容。
- 同一个 Bot 只运行一个服务实例，避免连接和事件消费互相争抢。

建议在首次启动前配置 `ownerId`。未配置且没有已保存的 owner 时，**首个有效私聊发送者会认领 Bot**；该条消息只完成认领，不执行任务。

## 环境准备

必需：

- [Bun](https://bun.sh/)，用于安装依赖和运行服务。
- 已安装、配置好模型凭据且可在本机正常使用的 OMP；默认从 `PATH` 查找 `omp`。
- 由 Octo BotFather 创建的 **User Bot** 及其 token。需要群聊能力时不能使用仅支持私聊的 `app_` App Bot。

可选：

- **Worktrunk**：提供隔离工作区创建、合并及清理。宿主使用本机可执行的 wt，命令兼容性由 wt 自身判定；缺少 wt 时创建回退到 `omp worktree add`，清理回退到 git，合并需先安装 wt。
- **octo-cli**：服务可为每个 Bot 配置独立 CLI 身份，使助手能以该 Bot 调用更多 Octo 接口。
- **Super**：托管常驻进程，并提供 `oco status`、`oco restart` 所需的守护进程识别与操作能力。

## 快速开始

在仓库目录安装依赖并注册 CLI：

```sh
bun install
bun link
```

`oco` 与 `omp-channel-octo` 是同一程序的两个入口。确保 Bun 的 bin 目录在 `PATH` 中。

创建私有配置文件；已有配置时直接编辑，不要覆盖：

```sh
mkdir -p ~/.oco/config
cp config.example.json ~/.oco/config/config.json
chmod 600 ~/.oco/config/config.json
```

按 [配置模板](config.example.json) 设置 API 地址、凭据来源和允许群列表，并建议添加 `ownerId`：

```json
{
  "apiUrl": "https://im.deepminer.com.cn/",
  "botTokenEnv": "OCTO_BOT_TOKEN",
  "ownerId": "你的 Octo 用户 ID",
  "allowedGroupIds": [],
  "ompCommand": "omp",
  "stateDir": "~/.local/state/omp-channel-octo",
  "sessionsDir": "~/.oco/sessions",
  "groupHistory": { "enabled": true, "maxMessages": 50 },
  "attachments": { "enabled": true, "maxBytes": 26214400 },
  "ask": { "timeoutMs": 21600000 }
}
```

在启动环境中提供 `OCTO_BOT_TOKEN`，例如通过本机私有环境文件或守护进程的环境配置注入。也可在私有 JSON 配置中使用 `botToken`，此时必须删除 `botTokenEnv`；两者不能同时配置。不要把 token 写入仓库或聊天。

在**尚未运行同一 Bot 实例**时检查配置并前台启动：

```sh
oco check --config ~/.oco/config/config.json
oco run --config ~/.oco/config/config.json
```

`check` 会真实注册 Bot 并验证 OMP fresh worker 的启动和停止，不调用模型。**不要把它当作在线服务的存活探针**：重新注册可能影响正在运行的 Bot 连接。

`run` 用于前台调试，不参与守护进程探测。确认可用后，用守护进程托管 `oco start --config ~/.oco/config/config.json`，并在启动常驻实例前停止前台实例。守护进程必须有自己的凭据环境，不能依赖交互终端里的变量。

### 首次聊天

1. 私聊 Bot；若未预设 owner，先完成认领，再发送任务。
2. 用 `/new ~/workspace/your-project` 绑定已有项目目录。
3. 发送任务描述；任务运行时继续发消息，默认会并入当前回合。
4. 群聊中使用客户端真正的 @ 提及，例如 `@Bot /status`。未开通的群里，owner 可按提示发送 `@Bot 开通本群`，授权立即生效并持久保存。开通父群也覆盖其子区；单独开通子区仅覆盖该子区。

## 聊天命令

以下命令仅 owner 可用。群聊和子区还需满足允许列表与 @ 规则。

| 命令 | 用途 |
| --- | --- |
| `/new [目录]` | 创建新会话；回执显示会话、目录及可查询到的实际模型。 |
| `/reset [目录]` | 重置为新会话，保留旧历史文件。 |
| `/move <目录>` | 通过 OMP 原生迁移保留当前会话 ID、历史与附件。 |
| `/thread <任务>` | 在群内交由助手按项目与耦合程度创建隔离子区，工作区和背景就绪后派发任务。 |
| `/thread model [选择器\|clear]` | 主群查询、设置或清除所属子区默认模型；只影响子区下一次 worker 启动，主群自身模型不变。 |
| `/thread merge [补充说明]` | 单项目及 separate 子区请求合并；默认合并成功后删除工作区和已合并分支。shared 多项目工作区只回复不支持提示。 |
| `/queue <文本>` | 显式追加一个排队任务。 |
| `/mode [steer\|queue]` | 查看或设置新消息的追加方式，默认 `steer`。 |
| `/switch [模型]` | 查看当前模型与可用列表，或切换并保存本聊天模型。支持 `provider/id`、模型 ID/唯一子串、`@角色` 和 `:level` 后缀。 |
| `/switch default` | 清除本聊天显式设置；子区重新继承主群子区默认，否则不指定模型、由 OMP 自行决定；运行中的会话不变。 |
| `/goal`、`/goal show` | 查看当前会话的原生目标、状态、用量及预算。 |
| `/goal set [--budget N] <目标>` | 创建目标并持续执行；`N` 为正整数 token 预算。 |
| `/goal pause`、`/goal resume`、`/goal drop` | 暂停后续续跑、恢复或删除目标。 |
| `/goal budget` | 查看预算；当前原生 RPC 仅支持创建时设置预算。 |
| `/retry` | 由 OMP 重试当前会话上次失败或中止的回合；忙时作为独立任务排队。没有可重试回合时返回 OMP 提示。 |
| `/commit` | 用 OMP 原生 commit 提交当前绑定的单仓库、单 worktree 工作区；资源忙碌时拒绝。 |
| `/commit stop` | 停止本聊天当前提交；已完成的提交保留，不影响其他聊天或无关任务。 |
| `/interrupt` | 中止当前回合，保留 worker 与后续排队消息。 |
| `/cancel`、`/stop` | 取消当前任务并清除该聊天待执行消息；不会回滚已发生的副作用。 |
| `/status` | 查看会话、目录、模型、追加模式及任务状态。 |
| `/restart [now]` | 经已识别的守护进程重启服务；两种写法等价，在途任务挂起，旧 worker 确认退出后沿原会话续接。 |
| `/autoresearch-history` | 导出本聊天最近一次研究运行的完整记录文件，随发随得，不占用运行中的任务。 |
| `/help` | 查看命令帮助。 |

目录参数接受绝对路径或当前账户的 `~/` 路径，可加一层引号；不展开环境变量、通配符或 shell 命令。省略目录通常沿用当前绑定，首次无绑定时使用 `~/tmp`。子区建立会话前可继承父群目录，建立后固定为自己的绑定；隔离工作区绑定优先。

模型启动策略：**本聊天显式 `/switch` > 所属主群 `/thread model` > 不传模型参数**。主群自己的 `/switch` 不会成为子区继承来源。`/thread model clear` 只删除主群子区默认，保留各子区显式设置。配置按 bot 与主群隔离、跨重启保留；变更适用于已有和新子区的下一次 worker 启动，包括普通任务及恢复、goal、commit、会话创建/重置/移动。`/switch` 与 `/status` 区分运行模型和下一次启动策略。

未注册的 `/xxx` 输入连同参数原样转交该聊天的 OMP 会话，由 OMP 分派命令或处理正文。权限与上表一致：私聊限 owner，群内成员不能执行 slash 命令。这类输入作为独立任务排队，不并入正在运行的回合。需要立刻停止当前工作时，先 `/cancel`（同时清除该聊天的排队消息），再发送目标命令。

`/retry` 沿用当前绑定的 OMP 会话，由 OMP 决定可重试的回合与恢复方式。命令回执（如 `Retrying the last failed turn.`、`Nothing to retry.`）以 `OMP：` 通知独立投递，生成内容照常流式返回。若当前回合仍在运行，可先 `/interrupt`，再发送 `/retry`；执行时以 OMP 会话当时的状态为准。

`/commit` 必须已有明确的聊天目录绑定。shared、多仓库或多 worktree 工作区不支持；separate 的每个单项目子区以及正常单项目 linked worktree 可以使用。判断依据是持久工作区归属，而非背景说明或源仓库的 linked worktree 总数。命令不接受额外提交参数，不推送；停止或服务中断不回滚已发生的 Git 修改。已发出的提交在重启后不自动再次执行，结果不确定时先核对 Git 状态再决定是否发起新提交。

提交策略使用安装版 OMP 的默认行为：有 staged changes 时提交暂存内容，否则自动 stage 当前仓库的改动；可能更新 changelog 或拆成多个 commit。channel 根据原生 CLI 退出码及运行前后的 Git HEAD 反馈结果；非零退出或停止时若已经产生提交，会保留并报告该提交。停止可能留下暂存改动或 Git lock，需按 Git 的实际状态处理。

群或子区的新任务执行时，服务直接读取当前聊天的成员信息；运行中追加 steering 时重新读取。10 人及以下向助手提供名称和 UID，更多成员时提供人数和查询入口。成员信息读取失败时，任务不会启动；steering 不会并入，已运行的任务继续。`/retry` 保持 OMP 原生重试语义，不重新读取成员。

助手可以在普通回复中使用结构化 mention 点名成员，完整的 mention 和后续正文在同一条消息中流式更新；代码示例和普通 `@名字` 文本不会自动点名。正文超过单条消息长度限制时按既有规则分段。

安装并配置 `octo-cli` 后，助手可按当前 Bot 的权限使用 Loop、Docs、Drive、HTML、消息、文件、邮件和 Marketplace 等能力；它会通过 CLI 的 `skills`、`schema` 和帮助按需发现具体操作。

### 多项目子区

助手通过 `octo_management` 的 `create-thread` 统一创建子区、准备 worktree、绑定 cwd，并写入 Octo `THREAD.md` 背景说明。多项目由助手显式选择 layout：

- `shared`：一个子区、一个非仓库入口目录，入口下每个项目各有独立 Git worktree。适合需要同一回合联动修改的项目。
- `separate`：一次调用按项目创建多个子区，各自绑定来源仓库的隔离 worktree。适合可以独立派发的工作。

参数示例（路径替换为本机真实仓库）：

```json
{
  "action": "create-thread",
  "name": "登录链路联调",
  "worktree_name": "login-flow",
  "layout": "shared",
  "projects": [
    {"name": "web", "repo": "/workspace/web", "base": "main"},
    {"name": "api", "repo": "/workspace/api", "base": "main"}
  ],
  "background": "联调登录链路，涉及前端与 API。"
}
```

同一参数改为 `"layout": "separate"`，返回 `threads` 列表，每项携带 `short_id`、`cwd`、项目路径与分支。单项目可省略 layout；既有 `name` + `worktree_name` 单仓库调用继续可用。

只有结果 `ready: true` 才能继续 `thread-dispatch`；全部 worktree、持久绑定与共享说明均须成功。部分失败会返回已完成/未确认资源与错误，保留现场，不自动开始实施。THREAD.md 仅含创建目的、项目及实际路径；具体目标、实施要求与验收写在派发正文中。

远端创建结果未知时，持久创建意图会阻止同组尚无绑定的新子区自动执行，已有绑定不受影响；owner 可显式 `/new <目录>` 建立确定绑定。shared 要求状态目录和入口祖先位于 Git worktree 之外，否则创建前即拒绝，避免把用户仓库误当入口。

shared 不支持 `/thread merge` 或工具 `thread-merge`。`thread-delete` 按持久资源归属清理，默认保留脏工作树和占用中的资源；`force: true` 才丢弃未提交改动。显式 `/new <目录>` 或 `/move <目录>` 改变当前绑定，但保留原工作区归属以便删除时清理；`/new` 无目录沿用工作区。

### 持续目标 `/goal`

需要支持 goal RPC 和 `session_settled` 的 OMP，已按 18.15.0 验证。owner 的 `set` / `resume` 授权当前会话持续执行；channel 只对目标执行 worker 加载 `src/omp/goal-rpc.yml`，开启 RPC 续跑，不修改全局设置。OMP 的 `goal.enabled: false` 仍会拒绝启动。

```text
/goal set --budget 120000 完成导入器实现并运行验收
/goal show
/goal pause
/goal resume
/goal drop
```

目标状态与单次任务结束分开：多轮续跑共用原任务，原生静止后才收尾。无进展时暂停未完成目标；达到预算时显示 `budget-limited`，不是宣布目标达成。预算按原生累计 input、cacheWrite、output，排除 cacheRead，不是硬 token 上限。已有目标时 `set` 明确拒绝；预算修改/清除需等待原生 RPC 提供相应能力，当前命令不会删除重建目标。

普通任务正忙时，目标启动/恢复请求排队；`show` / `pause` / `drop` 直接控制本聊天的活会话。暂停或删除不强行中止当前回合，需要立即中断用 `/interrupt`，取消并清队列用 `/cancel`。显式停止与 ask 超时撤销目标续跑授权，普通消息和迟到答案不会恢复目标。

服务重启会自动续接仍有授权的在途目标：先确认旧进程退出，再核对原会话和原生 goal ID，使用原生 `/retry` / goal resume；接受状态不明时停止恢复，不重新发送目标正文。详细边界见[持续目标设计](docs/specs/2026-11-02-goal-command-design.md)。

### 原生 `/autoresearch`

在已绑定的项目目录中使用：

| 输入 | OMP 行为 |
| --- | --- |
| `/autoresearch` | 开启研究模式；已开启时再次发送则关闭。 |
| `/autoresearch <目标>` | 开启或恢复研究，并把目标交给助手执行。 |
| `/autoresearch off` | 关闭研究模式；运行中的任务会先结束，再执行这条排队命令。 |
| `/autoresearch clear --keep-tree` | 关闭并清空研究状态，跳过 Git 重置；仍删除 `autoresearch.sh` 等遗留研究产物。 |
| `/autoresearch clear` | 关闭并清空研究状态；在 `autoresearch/*` 分支上默认硬重置到基线并清理未跟踪文件。没有基线时通知跳过重置。 |

**`clear` 会删除研究产物，并可能丢弃工作树改动。** OMP v18.2.6 的 `clear`（包括 `--keep-tree`）会删除 `autoresearch.sh` 及旧版研究文件；`clear --reset-tree` 还会在其他分支强制执行 Git 重置。channel 不增加确认或撤销机制。

开启、关闭和错误通知通过持久出站队列发到原聊天，带 `OMP：`、`OMP 警告：` 或 `OMP 错误：` 标签；它们与助手正文分开，不记入任务结果。研究续回合的正文和工具进度沿用现有流式消息与进度卡。

本地命令不调用模型，群说明保持待注入，直到下一次真实模型回合取得注入回执。OMP 会话中的研究模式随同一会话恢复；已用原生 OMP 验证开启后停止 worker、恢复会话再切换关闭。服务重启仍不会自动重跑已开始的任务。

研究状态、实验工具和 Git 操作由 OMP 管理。OMP 的 TUI 仪表盘不通过 RPC 提供，Octo 中查看通知、助手正文和工具进度。

channel 另补两块只读呈现，均由权威实验状态驱动，不从工具参数或文本猜测实验结果：

- **研究附卡**：进度卡常显目标、指标、基线、最佳、改善幅度和轮次进度；最近最多 6 条记录可独立展开。空间不足时减少近期记录条数并注明省略数，保留摘要；任务结束后显示终态，纯文本回退同样携带研究摘要与最近行。steer 滚出的新卡沿用同一最新快照，新的普通任务不受影响。
- **完整记录**：跨全部段的脱敏 Markdown（上限 1 MiB，超限时保留最新整行并标注省略条数；旧格式缺少轮次号的记录显示为未编号）按 bot+聊天私有落盘并原子替换。owner 随时可用 [`/autoresearch-history`](#聊天命令) 把它作为文件收回本聊天——运行中的任务不打断、不排队，重复投递只发一次，没有研究记录的聊天只回一句提示，成员与其它聊天都取不到它。
- **投递韧性**：导出的附件在入队时即固定字节，之后的报告重写不改变已待发内容；报告落盘失败只记日志，不影响产生它的任务；服务重启后仍可导出上一份记录。

有待回答问题时，聊天中的普通回复优先作为答案，而不是新任务。可点击问答卡片，也可直接回复文本；同一轮多个问题可用一条文本一起回答。默认等待 6 小时后停止等待中的回合，问题仍可回答，之后的答案会唤醒原会话继续处理。

## 配置与数据

| 项目 | 默认值或说明 |
| --- | --- |
| 配置文件 | `~/.oco/config/config.json`；使用 `--config` 指定其他文件。 |
| `botTokenEnv` | 默认 `OCTO_BOT_TOKEN`；自定义名称须以 `OCTO_` 或 `OMP_CHANNEL_OCTO_` 开头。 |
| `allowedGroupIds` | 默认空数组；与运行中开通并持久保存的聊天取并集。 |
| `ompCommand` | 默认 `omp`；也可用启动参数 `--omp-command` 指定可执行文件。 |
| `stateDir` | `~/.local/state/omp-channel-octo`；保存 `channel.sqlite`、附件、研究完整记录报告及运行状态。 |
| `sessionsDir` | `~/.oco/sessions`；按项目目录隔离保存 OMP 会话。 |
| 服务日志 | `~/.oco/logs/channel-YYYY-MM-DD.log`，保留 14 天。 |
| `groupHistory` | 默认启用，每轮最多注入 50 条群/子区历史。 |
| `attachments` | 默认启用，附件大小上限 25 MiB；启动时清理超过 14 天的附件。 |
| `ask.timeoutMs` | 默认 6 小时，可配置为 30 分钟到 7 天的整数毫秒。 |

状态、会话、附件和日志可能包含私有内容，应按敏感数据保存和备份。配置字段及校验以 [src/runtime/config.ts](src/runtime/config.ts) 为准。

## 日常运维

```sh
oco status
oco logs --tail 200
oco logs -f
oco restart
```

当前守护进程自动识别实现支持 Super；无需在 channel 配置中填写程序名。未识别到守护进程或匹配到多个候选时，CLI 会拒绝猜测。日志读取使用服务自己的日志目录。

服务就绪后会向已绑定 owner 的私聊发送「omp-channel-octo 已启动」，正常停机前发送「omp-channel-octo 正在停止；在途任务将挂起，启动后继续。」，两条通知均附本地时间、时区偏移和服务 PID。Super、`oco` 与聊天命令触发的正常启停使用同一路径；正常重启依次收到停机和启动通知，聊天命令原有回执保留。

每条生命周期通知只尝试一次，最多等待 2 秒；失败记服务日志，不阻止启停，不在下次启动补发。通知直接发送，不受任务出站队列阻塞；尚未绑定 owner 时跳过，首次认领不补发启动通知。SIGKILL、崩溃、断电和启动失败不保证停机或失败通知；断线重连不重复发送启动通知。

### macOS launchd

也可生成 launchd 配置并显式加载：

```sh
oco launchd install --config ~/.oco/config/config.json
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.omp.channel-octo.plist"
```

安装命令只写 plist，不启动服务。使用环境凭据时，生成的 plist 会包含 token，必须保持私有权限；安装前先确保当前环境已提供凭据。launchd 托管实例的启停使用 `launchctl`，当前 `oco restart` 的自动识别路径面向 Super。

卸载时先停止，再删除 plist：

```sh
launchctl bootout "gui/$(id -u)/com.omp.channel-octo"
oco launchd uninstall
```

## 开发与验证

```sh
bun run typecheck
bun test
```

测试覆盖权限、持久化、流式投递、重启恢复、工作区和 RPC 生命周期；HTTP/WebSocket 与真实子进程夹具不调用模型。

目标生命周期另有显式真实 OMP 验收，使用隔离的配置/会话、localhost 模型替身和本地 Octo fixture，不需要模型凭据，也不连接常驻 Bot：

```sh
bun tests/benchmarks/goal.ts
# 或 OMP_REAL_COMMAND=/绝对路径/omp bun tests/benchmarks/goal.ts
bun tests/benchmarks/restart.ts
```

涉及执行卡片布局的改动还需运行真实预览，并在 Octo 客户端检查：

```sh
oco test --card
```

该命令会读取仓库的 `.envrc`，使用其中配置的 Bot 凭据、API 地址和预览目标发送真实消息。它是线上验收操作，不是离线单元测试。

### 代码结构

仓库是单一 private Bun package（`package.json` 无 `workspaces`，无拆包），运行时代码全在 `src/` 下，按领域目录组织：

```text
src/cli.ts                  CLI 与服务生命周期
src/service.ts              权限、命令、调度和任务协调
src/contracts.ts            传输、入站、worker、服务与命令契约
src/host-tools.ts           worker 暴露的宿主工具及其绑定目标
src/questions.ts            持久化问答与答案交付
src/outbox.ts               正文分块、串行编辑与有界重试投递
src/display-name.ts         uid → 显示名解析（问答归属）
src/typing-lease.ts         每聊天 typing 租约与续期
src/bot-events.ts           Bot 事件队列长轮询（卡片点击）
src/target.ts               聊天与流目标键
src/errors.ts               共享错误面与瞬时失败判定
src/abort.ts                取消信号转失败路径
src/octo/                   Octo HTTP、WebSocket、消息及卡片传输
src/omp/                    OMP 子进程、RPC、事件与进程清理
src/store/                  SQLite 持久化与迁移（index.ts 门面 + store-*.ts 分域读写）
src/cards/                  执行进度跟踪、卡片渲染与交互问答卡片
src/session/                会话身份、目录布局与回合上下文
src/inbound/                入站权限、正文组装、附件与群历史
src/commands/               命令解析、模型切换、持续目标与 /thread 指令
src/tasks/                  任务串行链、会话租约与失败分类
src/threads/                子区目标解析、归档与完成通知
src/workspace/              工作目录解析、隔离工作区与 Worktrunk 适配
src/runtime/                配置、守护进程识别、日志、launchd 与重启
src/research/               研究呈现与报告导出
tests/                      行为测试、协议和子进程夹具
```

详细契约见 [总体设计](docs/specs/2026-09-12-octo-channel-design.md)，专题设计见 [docs/specs/](docs/specs/)。
