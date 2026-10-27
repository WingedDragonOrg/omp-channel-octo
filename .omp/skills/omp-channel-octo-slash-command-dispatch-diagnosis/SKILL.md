---
name: omp-channel-octo-slash-command-dispatch-diagnosis
description: 诊断 omp-channel-octo/Octo 里发出去的自定义 slash 命令（/wtm、插件命令）为什么没执行、变成普通对话、或任务直接失败（unsupported interactive UI），以及 OMP 扩展输出为什么看不到
---

# 诊断 Octo 渠道里的 slash 命令为什么没按预期执行

适用：用户说「我发的 /xxx 没执行 / 变成直接对话 / 任务失败但看不到原因 / 插件命令没有任何输出」，且目标系统是 omp-channel-octo + OMP（含 `~/.omp/plugins` 里的 marketplace 插件，如 wtm）。

## 0. 先分清三层

```
Octo 消息 → CoreService 路由 → worker.prompt（OMP RPC）→ OMP 命令分派 / 模型回合
```

任何一层都能吞掉命令，按下面顺序取证，不要凭截图猜。

## 1. channel 层：命令是被本地处理、转交 OMP，还是被前缀破坏

- `src/service.ts` 的 `handleInbound`：只有 `BUILTIN_COMMANDS`（`/new /reset /move /thread /queue /mode /interrupt /cancel /stop /status /help /restart`）和注册的 `ChannelCommandHandler` 在本地处理；**未注册的 `/xxx` 按普通任务转交 OMP**（这是设计，不是 bug）。
- 关键陷阱：群/线程会走 `historyTurnFor`，把 `[群聊历史 …]\n…\n---\n<用户文本>` 拼在任务正文前。OMP 只从 **prompt 首行**分派命令，前缀会让整行退化成普通用户消息。
  - 2026-09-16 起 `acceptTask` 用 `historyEligible: historyEligible && !isCommand(text)` 修掉了这条路径；回归测试在 `tests/core-group-history.test.ts`（"an unregistered slash task reaches OMP verbatim…"）。
  - 判断当前部署是否含该修复：`oco restart` 后重试，或看 prompt 是否仍以块开头。

## 2. 会话记录层：看 OMP 实际收到了什么（最强证据）

stateDir 默认 `~/.local/state/omp-channel-octo`，每轮任务一个 OMP session 文件：

```sh
ls -lt ~/.local/state/omp-channel-octo/sessions/*.jsonl | head
# 找 role:user 的那条，正文即 worker.prompt 的逐字内容
```

- 正文以 `[群聊历史` 开头 → 是前缀问题（第 1 层）。
- 紧跟的 `custom` / `tool_execution_start` 是 bash `which wtm`、`read skill://wtm` 之类，说明模型把它当聊天处理。
- 另可查 SQLite：`sqlite3 ~/.local/state/omp-channel-octo/channel.sqlite "select channel_type, channel_id, state, substr(text,1,80), datetime(accepted_at/1000,'unixepoch','localtime') from tasks order by accepted_at desc limit 5;"`（`channel_type=5` 即子区，会走历史注入）。

## 3. 真实 OMP 对照实验（区分 channel 问题与 OMP 问题）

用真 `omp` 起一个 `--mode rpc` 进程，只发一帧 prompt，观察帧类型；不带扩展也能复现命令分派：

```ts
// probe.ts：bun probe.ts '<prompt 文本>' <git cwd> <session dir>
const child = Bun.spawn([`${process.env.HOME}/.bun/bin/omp`, "--mode", "rpc", "--approval-mode", "yolo",
  "--cwd", cwd, "--session-dir", sessionDir], { cwd, stdin: "pipe", stdout: "pipe", stderr: "ignore" });
// 逐行读 stdout；收到 {type:"ready"} 后写：JSON.stringify({type:"prompt", message}) + "\n"
```

判读：

- `prompt_result agentInvoked=false` → 命令在进程内执行，**模型没被调用**（期望行为）。
- 出现 `extension_ui_request method=notify` → 扩展输出走 notify（`/wtm list` 的 worktree 列表就是这么出来的）。
- 出现 `extension_ui_request method=confirm` → 见第 4 层。
- 什么都没有、最终出现 assistant message → 当普通文本进了模型。

对照组：裸 `/wtm list` 与「历史块 + `/wtm rm self`」各跑一次，即可证明前缀是分水岭。

## 4. 交互确认层：RPC 宿主也算「有 UI」

实测（wtm 1.4.0）：`omp --mode rpc` 下 `ctx.hasUI` 为 **true**，`/wtm rm <name>`（不带 `-y`）会发 `confirm`。

- omp-channel-octo 的处理（`src/omp/index.ts` `#handleUiRequest`）：非 `PASSIVE_UI_METHODS` 的 UI 请求先回 `{cancelled:true}`，然后 `#rejectPrompt` + 停 worker，理由 `OMP requested unsupported interactive UI (<method>); worker stopped safely`。
- 用户侧只会看到「任务执行失败；未能可靠完成。」（`genericTaskFailure`），DB `tasks.error_text` 仅 `worker failed` —— 原因既不进聊天也不进日志，需要读代码或加日志才能定位。
- 因此：**改插件时不要用 `hasUI` 判断是否跳过确认**（这里恒为 true），要用显式开关或 `-y`。
- 需要交互确认之外的其它 UI 方法（select/input/editor）同样会杀掉本轮。

## 5. 输出可见性层：notify 被丢弃

- `PASSIVE_UI_METHODS`（notify/setStatus/setWidget/setTitle/set_editor_text/open_url）在 channel 里直接 return，**不进聊天、不落 outbox**。
- 后果：`/wtm list`、`/wtm rm`、`/wtm rm self` 的结果与 `/move <primary>` 交接全部不可见 —— 命令"执行成功但用户什么也没看到"。
- 若要投递，注意每个任务都会新起一个 worker，启动期还会有 `skill-gate: …` 这类 notify，必须按"仅 prompt 进行中"过滤，否则每轮刷噪音。

## 6. 收尾路径：`rm self` 之后的 `/move`

wtm 的 `rm self` 让你接着发 `/move <primary>`；本 channel 的 `/move` 会用**旧 cwd** 起候选 worker（`src/service.ts` `handleMove` → `workerFactory.create({cwd: previous.cwd})`），旧目录已被删时直接抛 `OMP worker cwd does not exist`，回复「无法创建可恢复的 OMP 会话，当前绑定未改变。」。

- 回到主树用显式 `/new <主工作区目录>`：它只按新目录起会话并覆盖绑定（顺带清掉 `cwd_pinned`）。
- 子区 worktree 已被删但绑定仍 `cwd_pinned=1` 且有已验证 session 时，后续任务都会失败；显式 `/new <dir>` 是恢复路径（无 session 的子区则会自动回落到父群 cwd）。

## 检查清单

```text
1 service.ts：该 /xxx 是 builtin / 注册 handler / 还是转交 OMP？
2 sessions/*.jsonl：prompt 是否被 [群聊历史 …] 前缀？
3 rpc probe：prompt_result agentInvoked 是 false 还是无？
4 有 confirm 帧 → 非交互渠道必失败 → 让用户带 -y 或改插件显式开关
5 只有 notify 帧 → 命令成功但输出被丢弃
6 rm self 之后 → 用 /new <主工作区>，不要 /move
```

## 已验证环境

2026-09-16，macOS，omp homebrew/bun 版（`~/.bun/bin/omp`），wtm 插件 1.4.0（`~/.omp/plugins/cache/plugins/winged-dragon-org___wtm___1.4.0`），omp-channel-octo 工作副本 `~/workspace/omp-channel-octo`。
