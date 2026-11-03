# 聊天模型与子区默认模型

日期：2026-09-17
修订日期：2026-11-03

## 背景与目标

channel 需要一个 owner 可见的模型切换入口：在聊天里查看当前模型与可用模型列表，并把本聊天的模型切到指定模型，切换按聊天持久化、服务重启后仍生效；已经启动的回合也要能直接切换。

已核实的现状与 OMP 能力（OMP 18.2.3，`--mode rpc`）：

- channel 的 OMP worker 每回合创建一个、回合结束即停止，运行中的回合有一个活着的 OMP 会话（`runtime.worker`）。
- OMP 自带 `/switch` 内建命令（`slash-commands/builtin-modes.ts`，ACP 可见）：选择器支持 `provider/id`、模型 id、唯一子串、`@角色`（`settings.modelRoles`）与 `:level` 后缀；命中后调用 `session.setModelTemporary`（只改本会话，不写全局默认）。分发发生在 RPC 的 prompt 路径、`session.prompt()` 之前，因此它不是 agent 回合：响应带 `agentInvoked:false`，**回合运行中同样可用**（实测：流式回合里 `/switch @tiny` 立即生效，回合继续跑完）。
- 成功时 OMP 先发 `command_output`（`Session-only model: <provider>/<id>.`）与 `config_update`（`model` 为完整模型对象、`thinkingLevel` 为切换后的级别），再回响应；选择器无法解析时只发 `command_output`（`Unknown model: <选择器>`），不改会话。
- `--model` 用同一个选择器解析器（`@smol`、`provider/id`、`:level` 都接受，实测 `--model @smol` 解析为角色的模型与级别），启动时生效并覆盖 `--resume` 会话里记录的模型；传无法解析的值会让 worker 启动失败，因此落盘前必须由 OMP 自己校验过。
- RPC 另有 `set_model`/`set_thinking_level`，但它们只接受具体 `provider/modelId`，角色与级别的解析仍要自己做，本设计不使用。

## 范围

### 包含

- 新内置命令 `/switch`：
  - 无参数：报告本聊天当前模型与可用模型列表（含角色可用性说明由 OMP 解析，见下）。
  - 带参数：把选择器交给 OMP 自己的 `/switch` 解析并应用（`provider/id`、模型 id/子串、`@角色`、`:level`）。
  - `default`：清除本聊天显式模型；子区重新继承所属主群的子区默认，其他聊天不指定模型（`@default` 是角色写法，走原生解析）。
- 主群 `/thread model [选择器|clear]`：查询、设置或清除本群所属子区的默认模型；只允许 owner 在主群执行。
- 生效路径分两部分：
  - **已有运行中的回合**：选择器直接分发给该回合的活会话，会话就地切换（OMP 只改本会话，不产生 agent 回合）。
  - **之后启动的 worker**：统一读取有效策略，按「本聊天显式指定 > 所属主群子区默认 > 不传 model」决定参数；覆盖普通任务及恢复、goal 执行/控制、commit、`/new`、`/reset`、`/move`。主群自己的 `/switch` 不是子区继承来源。
- `/status` 增加 `model=` 行；`/help`、主规格、根 `AGENTS.md` 的命令清单同步。
- 测试：纯函数（渲染与边界）、服务集成（持久化/生效/重启保持/运行中切换/错误路径）、真实 RPC fixture（`--model`、`get_state`/`get_available_models`、`/switch` 分发与 `config_update`）。

### 不包含

- 选择器语法本身的实现（`@角色` 与别名的映射、模糊匹配偏好、`:level` 语义）——一律由 OMP 解析，channel 不复制。
- OMP 全局模型配置写入；channel 只管理自己的聊天显式值与主群子区默认。
- 模型列表缓存或后台刷新（每次命令现探测）。
- 角色清单的枚举渲染（列表仍按 provider 分组列出模型；`@角色` 按 OMP 的映射解析）。

## 已知约束

- 命令沿用既有 owner 门禁：只有 owner 能执行；群/线程仍需 @ 与 allowlist。
- 用户可见输出中文、有界；错误单行；不回显 token、协议帧或内部错误对象。
- 不改 OMP 本体，只使用既有 RPC 命令与 `--model` 启动参数。
- 探测 worker 不得触碰目标聊天的会话文件：fresh（无 `--resume`）、不跑 agent 回合、独立临时会话目录。
- 落盘的模型值只来自当次由 OMP 解析成功的切换，避免启动期解析失败使后续回合无法启动。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 选择器解析 | 交给 OMP 的 `/switch` | 角色映射、模糊匹配偏好、`:level` 语义都是 OMP 的实现细节；复制一份必然漂移 |
| 运行中切换 | 把 `/switch` 分发给活会话 | 原生路径，实测回合运行中可用且不影响该回合；不引入 `set_model` 的自解析 |
| 未启动路径 | 持久化 + worker 启动时传 `--model` | worker 每回合创建，启动参数是唯一稳定落点；`--model` 与 `/switch` 同一解析器，且覆盖 resume 记录 |
| 落盘值 | 解析后的 `provider/id:level` | 规范、稳定；模糊选择器不会在下次启动时解析到别的模型；`:level` 保证角色级别一并复现 |
| 成功判定 | OMP 的 `config_update` 帧 | 结构化信号，带解析后的模型与级别；`command_output` 只是人类可读文本，不解析它 |
| 模型列表来源 | `get_state` + `get_available_models` | 与真实回合同一 OMP 配置（cwd/env/profile）；列表即「已配置凭据的可用模型」 |
| 报告模型 | live 查询报告运行模型；有效策略报告下一次启动的模型及来源 | 区分运行状态与配置；不把 fresh 探测结果当作 resume 保证 |
| 持久化粒度 | 显式值按聊天目标隔离；子区默认按 bot 与主群 ID 隔离 | 父群自身模型与子区启动策略分别存储，绑定重建不改变配置 |
| 探测 worker | 复用 `WorkerFactory`，记录 worker ledger，不取目录 lease | 与 `/new` 候选 worker 同构；fresh、无 agent 回合，不与运行中的回合争用 |
| 探测会话目录 | 每次 `mkdtemp` 临时目录，停止后删除 | 探测会话文件无长期价值；不污染 `sessionsDir` 下的项目会话目录 |

## 设计
### 子区默认模型 SDD

**目标**：owner 在主群统一设置子区下一次启动策略，子区可独立覆盖。未配置任何有效值时完全不传 `model`，包括 resume 会话，由 OMP 自行处理模型选择。

**命令与权限**：

- `/thread model` 查询该主群的子区默认；未配置明确显示「不指定，由 OMP 自行决定」。
- `/thread model <选择器>` 使用现有 fresh probe 与 OMP 原生 `/switch` 解析；支持规范 ID、唯一子串、角色和级别。只在成功解析且 probe 已完成既有释放流程后写入规范值；失败保留旧值。不得调用主群 live worker 的 `switchModel`。
- `/thread model clear` 直接删除该主群默认行；不解析 `clear`，不写任何替代模型，不删子区显式值。
- 仅精确首词 `model`（后接空白或结束）进入此控制路径；`model-fix` 等仍是任务意图，`merge` 保留原路由。
- DM、子区明确拒绝，不创建/派发任务；沿用 owner、allowlist、mention 与 durable command claim。pending ask 不被回答，附件不下载。
- 主群默认查询/修改按现有配置序列化机制排序，不使父群 `/switch` 菜单失效、不发布模型选择菜单。

**状态与数据所有权**：

- `chat_models` 与 `getChatModel` 保持 explicit-only；新增 `thread_models(bot_uid, group_id, model, updated_at)`，主键 `(bot_uid, group_id)`，通过现有 schema 初始化增量创建，不迁移/复制旧模型值。
- `ChannelStore.resolveChatModel(botUid, target)` 是有效优先级唯一所有者，返回模型及 `chat|thread|omp` 来源；子区父群 ID 复用 `threadParentGroupId`，不依赖当前绑定、创建者或 THREAD.md。
- 所有实际 worker 消费点在启动时读取该解析结果；没有值时 options 不含 `model`。新子区无需复制配置，已有子区也立即适用下一次启动策略。
- 默认变更不遍历、不打断、不切换已有运行会话。子区显式 `/switch` 仍立即切换本聊天的 live worker；`/switch default` 仅清覆盖，不切 live。

**回执与验收**：设置/清除回执注明下一次 worker 启动生效、运行中会话保持原模型。`/switch` 与 `/status` 显示有效策略及来源；运行中查询同时区分 live 模型与下次启动策略。验证设置→继承→覆盖→清覆盖继承→clear 无参数路径，跨 bot/群与重启隔离、所有 worker 入口、控制边界与命令去重。先观察缺失行为红灯，再实现；抛弃式真实子进程 smoke 检查 argv，旧 SQLite 一致性副本用实际迁移路径连续打开验证数据保留与幂等。验收不连接真实 bot、不向真实群发消息、不部署。


### 命令接入

`BUILTIN_COMMANDS` 与既有命令分派硬拦截 `/switch`，`/thread` 路由精确 `model` 子命令；`/help` 说明查询、选择器、子区默认与清覆盖语义。模型控制命令不消费正文附件，聊天有待回答问题时仍正常执行。

参数去首尾空白后：

- 空：报告当前模型 + 可用模型列表。
- `default`（大小写不敏感，且不含 `/`）：清除本聊天模型。
- 其他：交给 OMP 解析。

### 探测 worker

`withProbeWorker(target, run)` 是 `/switch` 三种路径共用的探针：按 `defaultWorkDirectory(target)` 解析 cwd（失败回退配置的默认目录，全部不可用回 `无法解析该聊天的工作目录，/switch 暂不可用。`），`mkdtemp(join(tmpdir(), "omp-channel-octo-switch-"))` 作为会话目录，`workerFactory.create({cwd, sessionDir, onSpawn})` 并按既有惯例写入 worker ledger（前缀 `switch-`），`run` 完成后停止 worker（失败走既有恢复路径并计入 blocked 目录），随即删除临时目录。

### 三条路径

1. **无参数**：取 `catalog = live.models()`（有运行中回合）或 `withProbeWorker(worker => worker.models())`。`models()` 读 `get_state` 的模型与级别、`get_available_models` 的列表。回执：

   ```
   当前模型：<provider/id[:level]>（<来源>）
   可用模型（来自 OMP 已配置 provider，共 N 个）：
   <provider>：<id1>、<id2>…
   ```

   查询同时报告当前 live 模型（或 fresh 探测模型）与下一次启动策略，来源为本聊天指定、主群子区默认或不指定由 OMP 自行决定。无模型报告时明确未知，不拿配置伪造运行状态；列表为空时报告 OMP 没有可用模型；总模型数超过 80 时按 provider 顺序截断并说明总数。

2. **`/switch <选择器>`**：`liveWorker(target)` 命中时直接 `live.switchModel(选择器)`，否则 `withProbeWorker(worker => worker.switchModel(选择器))`。`switchModel` 发出 `prompt` 帧 `/switch <选择器>`，以响应前到达的 `config_update` 为成功信号（带 `model.provider`/`model.id` 与 `thinkingLevel`）；没有它就视为选择器被 OMP 拒绝。成功时把 `provider/id:level` 写入 `chat_models`，回执：

   - `已切换本聊天模型为 <值>，从下一回合开始生效。`（空闲，探测 worker 解析）
   - `已切换本聊天模型为 <值>（运行中会话已同步切换）。`（运行中回合）

   被拒绝时回 `未找到模型 "<选择器>"；发送 /switch 查看可用模型列表。`（选择器经有界化处理：控制字符去掉、空白折叠、截断到 60 字符），不改任何状态。选择器含空白时不发探测，直接按未找到回复（选择器是单个词元，白名单般省掉一次进程启动）。

3. **`/switch default`**：删除本聊天显式配置，重新解析有效策略。子区有父群子区默认时回执说明继承的规范值；否则明确不指定模型、由 OMP 自行决定。运行中会话不被改动，回执说明下一次 worker 启动生效；该路径不创建探测 worker。

WebSocket/网络失败一律收敛为 `无法从 OMP 读取模型信息，请稍后重试。`，且不改变持久化状态。命令执行的存储写入先于回执：写库失败走既有「命令未能持久完成，请重试。」路径。

### 生效路径

`chat_models` 表（DDL 同 `chat_modes` 的形状）：

```sql
CREATE TABLE IF NOT EXISTS chat_models (
  bot_uid TEXT NOT NULL,
  channel_type INTEGER NOT NULL CHECK(channel_type IN (1, 2, 5)),
  channel_id TEXT NOT NULL,
  model TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(bot_uid, channel_type, channel_id)
);
```

```sql
CREATE TABLE IF NOT EXISTS thread_models (
  bot_uid TEXT NOT NULL,
  group_id TEXT NOT NULL,
  model TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(bot_uid, group_id)
);
```

store 侧保留 `getChatModel`、`setChatModel`、`clearChatModel`；增加主群子区默认读写与集中有效策略解析，经 `ChannelStore` 门面暴露。

所有实际 worker 创建点读取一次有效策略；有值才设 `model`，RPC 与原生 commit 沿用已有 `--model` 参数构建。没有模型时不传参数，也不强制覆盖 resume 会话记录。

`/status` 的 `model=` 为有效启动策略，带本聊天指定、主群子区默认或 OMP 自行决定的来源；运行中模型与下一次启动策略分别标明。

### 帧与边界

- `config_update` 在 `RpcWorker` 的帧分发里单独截获（`#handleConfigUpdate`），只接受带字符串 `provider`/`id` 的 `model`，记录 `provider/id`、`thinkingLevel` 与递增序号；`switchModel` 在发送前取序号、响应到达后比较，序号未前进即失败。依赖 OMP「同一 handler 内先发 `config_update` 再回响应」的既有顺序（实测与源码一致）；顺序若变化，失败模式是有效选择器被判为未找到，不会误改状态。
- 该帧也天然中断 `#activePrompt` 之外的分发路径：探测 worker 没有活动回合也能收到它。

## 错误与边界情况

- 实际回合启动时已存模型无法解析（角色被删、模型下线）：该回合 worker 启动失败，按既有 worker 启动失败路径回报；`/switch` 仍可用（探测不依赖已存值）改回可用模型或 `default`。
- 探测 worker 未取得目录 lease：与运行中的回合并发是允许的；探测只读配置与目录，探测本身不产生排队。
- 探测 cwd 不存在（工作区被删）：按默认目录顺序回退；全部不可用才拒绝命令。
- `/switch` 输入含引号或其他字符：按字面作为选择器交给 OMP，不做 shell 展开；含空白时按未找到处理。
- 同一聊天连续多次 `/switch`：每次独立解析，最后一次写入生效；消息 id 去重沿用既有命令 claim。
- 显式值按私聊/群/子区独立；只有子区会读取所属主群专用默认，主群自身 `/switch` 从不成为继承来源。
- 运行中回合内 owner 发 `/switch`：活会话立即切换，该回合继续跑完（后续请求用新模型）；未启动的排队任务按启动时的绑定生效。

## 验收标准

1. 空 `/switch`：报告查询到的模型与按 provider 分组的可用列表，并显示下一次启动策略和来源。运行中模型与配置不同也必须分别展示，不把「不指定」当作一个具体模型；fresh 探测不代表 resume 模型。
2. `/switch <选择器>`（`provider/id`、模型 id/子串、`@角色`、`:level`）：回执明确；此后该聊天新建的 worker 带 `--model <解析后的 provider/id:level>`（测试断言 `WorkerOptions.model` 与真实子进程 argv；真机脚本验证 `switchModel` 的返回值与 `get_state` 一致），`/status` 的 `model=` 显示新值。
3. 运行中切换：回合运行中发 `/switch` 时由该回合的活会话执行（测试断言探测 worker 没有被创建），真机验证会话模型立即变化且该回合正常跑完。
4. 持久化：写库后服务重启（重建服务实例、同一 stateDir），该聊天新回合的 worker 仍带同一 `--model`。
5. 无效选择器：回复单行有界中文错误，`chat_models` 不变，后续回合 worker 的 `--model` 维持原值（无模型时完全不变）。
6. 主群设置/clear 不切换运行中的父群/子区；下一次普通任务/恢复、goal、commit、新建/重置/移动会话一致遵循有效策略。子区显式值优先，清覆盖重新继承，clear 后无覆盖的 options 不含 `model`。
7. 主群命令空参数、解析拒绝、owner/group 门禁、去重、pending ask、附件控制边界与关键词边界全部覆盖；SQLite 旧库升级保留旧配置且重复打开幂等。
8. `bun run typecheck` 与 `bun test` 全绿；主规格、命令说明与实现一致。

### 本次验证

- TDD 已观察缺失接口、子区任务/恢复/commit 未携带继承模型及设置命令未启动 fresh probe 的红灯；实现后 `bun run typecheck` 通过，`bun test` 为 1164 pass、0 fail（85 个文件）。
- 抛弃式服务配合真实 RPC 子进程夹具验证 14 次启动：角色解析后继承、普通任务继承、显式覆盖、父群变更后覆盖保留、清覆盖重新继承、clear 后新会话和 resume argv 均无 `--model`、clear 保留其他子区显式值、父群自身模型不变。使用本地 transport 与协议/上下文夹具，不调用模型、不连接真实 Bot。
- 真实状态库的只读一致性副本经实际 `openChannelStore` 连续打开两次，配置读写/清除和跨 bot 隔离通过；原有 25 张表、7174 行旧列哈希一致，`PRAGMA integrity_check` 为 `ok`，新增表主键为 `(bot_uid, group_id)`。抛弃式源码副本删除该建表 DDL 后，永久迁移回归按预期因缺表失败。
- 配置卡采用真实 builder 输出，在本地 AdaptiveCards 渲染器验证继承、显式覆盖、不指定三种状态；340/440 px 截图已检查，340 px 无水平溢出、无 schema 验证错误；三张样例各 11 个节点、深度 4，选择 default 后提交值正确。本地渲染不代表真实 Octo 客户端像素验收。


## 未决事项

无。
