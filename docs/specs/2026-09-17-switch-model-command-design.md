# /switch 模型切换命令

日期：2026-09-17

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
  - `default`：清除本聊天模型，之后启动的 worker 跟随 OMP 默认（`@default` 是角色写法，走原生解析）。
- 生效路径分两部分：
  - **已有运行中的回合**：选择器直接分发给该回合的活会话，会话就地切换（OMP 只改本会话，不产生 agent 回合）。
  - **未启动 / 之后新建的会话**：把解析后的 `provider/id:level` 存进 `chat_models`；任务、`/new`、`/reset`、`/move` 创建 worker 时传 `--model <值>`，resume 的会话同样被覆盖。
- `/status` 增加 `model=` 行；`/help`、主规格、根 `AGENTS.md` 的命令清单同步。
- 测试：纯函数（渲染与边界）、服务集成（持久化/生效/重启保持/运行中切换/错误路径）、真实 RPC fixture（`--model`、`get_state`/`get_available_models`、`/switch` 分发与 `config_update`）。

### 不包含

- 选择器语法本身的实现（`@角色` 与别名的映射、模糊匹配偏好、`:level` 语义）——一律由 OMP 解析，channel 不复制。
- 会话的全局默认模型写入（`/model` 的 `set_model` 语义）；channel 只存自己的 `chat_models`。
- 模型列表缓存或后台刷新（每次命令现探测）。
- 子区与父群之间的模型继承（模型按聊天目标隔离，与 `/mode` 同粒度）。
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
| 报告当前模型 | 有运行中回合时用该会话的 `get_state`；否则用探测 worker；再否则用本聊天指定值 | 「当前正在使用」以活会话为准；空闲时探测反映新会话会用到的默认 |
| 持久化粒度 | 每聊天目标（DM/群/子区各自独立，不继承），新表 `chat_models` | 与 `chat_modes` 的既有惯例一致；不耦合 `chats` 绑定的重建/外部重绑 |
| 探测 worker | 复用 `WorkerFactory`，记录 worker ledger，不取目录 lease | 与 `/new` 候选 worker 同构；fresh、无 agent 回合，不与运行中的回合争用 |
| 探测会话目录 | 每次 `mkdtemp` 临时目录，停止后删除 | 探测会话文件无长期价值；不污染 `sessionsDir` 下的项目会话目录 |

## 设计

### 命令接入

`BUILTIN_COMMANDS` 增加 `switch`，`handleCommand` 增加分支调用 `handleSwitch(message, argument)`；命令硬拦截（不发给模型），`/help` 增加一行「`/switch [模型]` 查看/切换本聊天模型（`provider/id`、模型 id/子串、`@角色` 如 `@default`；`default` 恢复跟随 OMP 默认）」。`/switch` 是控制命令：不消费正文附件。聊天有待回答问题时命令仍正常执行。

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

   来源按优先级：运行中会话（`运行中会话`，与本聊天指定值不同时追加 `；本聊天模型 <值>`；相同时为 `本聊天指定`）→ 本聊天指定（`本聊天指定`）→ OMP 默认（`OMP 默认`）。`get_state` 没给模型时为 `当前模型：未知（OMP 未报告）`；列表为空时第二行为 `OMP 当前没有可用模型。`；总模型数超过 80 时按 provider 顺序截断并追加「…（共 N 个）」。

2. **`/switch <选择器>`**：`liveWorker(target)` 命中时直接 `live.switchModel(选择器)`，否则 `withProbeWorker(worker => worker.switchModel(选择器))`。`switchModel` 发出 `prompt` 帧 `/switch <选择器>`，以响应前到达的 `config_update` 为成功信号（带 `model.provider`/`model.id` 与 `thinkingLevel`）；没有它就视为选择器被 OMP 拒绝。成功时把 `provider/id:level` 写入 `chat_models`，回执：

   - `已切换本聊天模型为 <值>，从下一回合开始生效。`（空闲，探测 worker 解析）
   - `已切换本聊天模型为 <值>（运行中会话已同步切换）。`（运行中回合）

   被拒绝时回 `未找到模型 "<选择器>"；发送 /switch 查看可用模型列表。`（选择器经有界化处理：控制字符去掉、空白折叠、截断到 60 字符），不改任何状态。选择器含空白时不发探测，直接按未找到回复（选择器是单个词元，白名单般省掉一次进程启动）。

3. **`/switch default`**：本聊天没有指定值时回 `本聊天未指定模型，继续跟随 OMP 默认。`；否则删除该行，并在空闲（无运行中回合）时用探测 worker 取当前默认，回 `已恢复跟随 OMP 默认模型（当前为 <值>）。`；探测失败或存在运行中会话时省略括号部分。运行中的会话不被改动——要移动它用 `/switch @default`。

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

store 侧：`getChatModel`、`setChatModel`、`clearChatModel`，经 `ChannelStore` 门面暴露。

worker 创建点（任务路径、`/new`、`/reset`、`/move`）在组装 `WorkerOptions` 时读取一次 `getChatModel`，有值则设 `model` 传入；`RpcWorkerFactory` 在 `--session-dir` 之后、`--resume` 之前追加 `--model <值>`。没有模型时 argv 与现状完全一致。

`/status` 在既有行后增加 `model=`：有值为存储的 `provider/id[:level]`，无值为 `未指定（OMP 默认）`。

### 帧与边界

- `config_update` 在 `RpcWorker` 的帧分发里单独截获（`#handleConfigUpdate`），只接受带字符串 `provider`/`id` 的 `model`，记录 `provider/id`、`thinkingLevel` 与递增序号；`switchModel` 在发送前取序号、响应到达后比较，序号未前进即失败。依赖 OMP「同一 handler 内先发 `config_update` 再回响应」的既有顺序（实测与源码一致）；顺序若变化，失败模式是有效选择器被判为未找到，不会误改状态。
- 该帧也天然中断 `#activePrompt` 之外的分发路径：探测 worker 没有活动回合也能收到它。

## 错误与边界情况

- 实际回合启动时已存模型无法解析（角色被删、模型下线）：该回合 worker 启动失败，按既有 worker 启动失败路径回报；`/switch` 仍可用（探测不依赖已存值）改回可用模型或 `default`。
- 探测 worker 未取得目录 lease：与运行中的回合并发是允许的；探测只读配置与目录，探测本身不产生排队。
- 探测 cwd 不存在（工作区被删）：按默认目录顺序回退；全部不可用才拒绝命令。
- `/switch` 输入含引号或其他字符：按字面作为选择器交给 OMP，不做 shell 展开；含空白时按未找到处理。
- 同一聊天连续多次 `/switch`：每次独立解析，最后一次写入生效；消息 id 去重沿用既有命令 claim。
- 群/线程/私聊一致：命令在三种目标都可用，模型互不影响。
- 运行中回合内 owner 发 `/switch`：活会话立即切换，该回合继续跑完（后续请求用新模型）；未启动的排队任务按启动时的绑定生效。

## 验收标准

1. 空 `/switch`：回复包含当前模型名（运行中回合为活会话的模型并标注来源；空闲时有聊天模型为该值并标注「本聊天指定」，否则为 OMP 默认并标注「OMP 默认」）与按 provider 分组的可用模型列表，列表条数与 OMP `get_available_models` 一致（可解释为「已配置 provider 的模型」）。
2. `/switch <选择器>`（`provider/id`、模型 id/子串、`@角色`、`:level`）：回执明确；此后该聊天新建的 worker 带 `--model <解析后的 provider/id:level>`（测试断言 `WorkerOptions.model` 与真实子进程 argv；真机脚本验证 `switchModel` 的返回值与 `get_state` 一致），`/status` 的 `model=` 显示新值。
3. 运行中切换：回合运行中发 `/switch` 时由该回合的活会话执行（测试断言探测 worker 没有被创建），真机验证会话模型立即变化且该回合正常跑完。
4. 持久化：写库后服务重启（重建服务实例、同一 stateDir），该聊天新回合的 worker 仍带同一 `--model`。
5. 无效选择器：回复单行有界中文错误，`chat_models` 不变，后续回合 worker 的 `--model` 维持原值（无模型时完全不变）。
6. `bun run typecheck` 与 `bun test` 全绿；主规格、根 `AGENTS.md` 的命令清单与实现一致。

## 未决事项

无。
