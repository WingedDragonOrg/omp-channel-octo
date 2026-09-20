# omp-channel-octo

通过 Octo 私聊、群聊和 CommunityTopic 子区操作本机 [Oh My Pi（OMP）](https://github.com/can1357/oh-my-pi) 的常驻服务，使用 TypeScript 和 Bun 实现。

服务主动连接 Octo，通过 stdio RPC 启动独立的 OMP 子进程。每个聊天保有自己的持久会话、工作目录和模型设置，可在聊天中执行任务、追加指令、回答问题、收发文件，以及派发隔离工作区任务。

## 主要能力

- **持久会话**：会话与终端分离；支持新建、重置和保留历史的目录迁移。
- **任务调度**：按聊天和真实工作目录串行执行；新消息可并入运行中的回合，也可排队。
- **消息与文件**：流式编辑助手回复，接收入站附件和引用内容，支持语音转写及出站文件。
- **执行反馈**：展示工具、待办和子代理进度；支持交互问答卡片，也可直接用文字作答。卡片能力取决于 Octo 部署，卡片失败不阻断正文。
- **群组上下文**：注入群与子区说明、增量聊天历史，按会话记录已读取的位置。
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

- **Worktrunk 稳定版 v0.76.x**：提供隔离工作区创建、合并及清理。缺少兼容版本时，创建回退到 `omp worktree add`，清理回退到 git；工作区合并需要兼容 Worktrunk。
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
| `/thread <任务>` | 在群内交由助手创建隔离子区并派发任务。 |
| `/thread merge [补充说明]` | 在符合条件的隔离子区内请求合并；默认合并成功后删除工作区和已合并分支。命令本身即合并与清理的确认。 |
| `/queue <文本>` | 显式追加一个排队任务。 |
| `/mode [steer\|queue]` | 查看或设置新消息的追加方式，默认 `steer`。 |
| `/switch [模型]` | 查看当前模型与可用列表，或切换并保存本聊天模型。支持 `provider/id`、模型 ID/唯一子串、`@角色` 和 `:level` 后缀。 |
| `/switch default` | 清除聊天模型设置，后续 worker 跟随 OMP 默认；不改变正在运行的会话模型。 |
| `/interrupt` | 中止当前回合，保留 worker 与后续排队消息。 |
| `/cancel`、`/stop` | 取消当前任务并清除该聊天待执行消息；不会回滚已发生的副作用。 |
| `/status` | 查看会话、目录、模型、追加模式及任务状态。 |
| `/restart [now]` | 经已识别的守护进程重启服务；有在途任务时普通重启拒绝，`now` 强制中断并重启。 |
| `/help` | 查看命令帮助。 |

目录参数接受绝对路径或当前账户的 `~/` 路径，可加一层引号；不展开环境变量、通配符或 shell 命令。省略目录通常沿用当前绑定，首次无绑定时使用 `~/tmp`。子区建立会话前可继承父群目录，建立后固定为自己的绑定；隔离工作区绑定优先。

有待回答问题时，聊天中的普通回复优先作为答案，而不是新任务。可点击问答卡片，也可直接回复文本；同一轮多个问题可用一条文本一起回答。默认等待 6 小时后停止等待中的回合，问题仍可回答，之后的答案会唤醒原会话继续处理。

## 配置与数据

| 项目 | 默认值或说明 |
| --- | --- |
| 配置文件 | `~/.oco/config/config.json`；使用 `--config` 指定其他文件。 |
| `botTokenEnv` | 默认 `OCTO_BOT_TOKEN`；自定义名称须以 `OCTO_` 或 `OMP_CHANNEL_OCTO_` 开头。 |
| `allowedGroupIds` | 默认空数组；与运行中开通并持久保存的聊天取并集。 |
| `ompCommand` | 默认 `omp`；也可用启动参数 `--omp-command` 指定可执行文件。 |
| `stateDir` | `~/.local/state/omp-channel-octo`；保存 `channel.sqlite`、附件及运行状态。 |
| `sessionsDir` | `~/.oco/sessions`；按项目目录隔离保存 OMP 会话。 |
| 服务日志 | `~/.oco/logs/channel-YYYY-MM-DD.log`，保留 14 天。 |
| `groupHistory` | 默认启用，每轮最多注入 50 条群/子区历史。 |
| `attachments` | 默认启用，附件大小上限 25 MiB；启动时清理超过 14 天的附件。 |
| `ask.timeoutMs` | 默认 6 小时，可配置为 30 分钟到 7 天的整数毫秒。 |

状态、会话、附件和日志可能包含私有内容，应按敏感数据保存和备份。配置字段及校验以 [src/config.ts](src/config.ts) 为准。

## 日常运维

```sh
oco status
oco logs --tail 200
oco logs -f
oco restart
```

当前守护进程自动识别实现支持 Super；无需在 channel 配置中填写程序名。未识别到守护进程或匹配到多个候选时，CLI 会拒绝猜测。日志读取使用服务自己的日志目录。

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

涉及执行卡片布局的改动还需运行真实预览，并在 Octo 客户端检查：

```sh
oco test --card
```

该命令会读取仓库的 `.envrc`，使用其中配置的 Bot 凭据、API 地址和预览目标发送真实消息。它是线上验收操作，不是离线单元测试。

### 代码结构

```text
src/cli.ts                  CLI 与服务生命周期
src/service.ts              权限、命令、调度和任务协调
src/octo/                   Octo HTTP、WebSocket、消息及卡片传输
src/omp/                    OMP 子进程、RPC、事件与进程清理
src/store*.ts               SQLite 持久化与迁移
src/execution-*.ts          执行进度跟踪与卡片渲染
src/questions.ts            持久化问答与答案交付
src/ask-card.ts             交互问答卡片
src/worktree.ts             隔离工作区生命周期
src/worktrunk.ts            Worktrunk 命令适配
tests/                      行为测试、协议和子进程夹具
```

详细契约见 [总体设计](docs/specs/2026-09-12-octo-channel-design.md)，专题设计见 [docs/specs/](docs/specs/)。
