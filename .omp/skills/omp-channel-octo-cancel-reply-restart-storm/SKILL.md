---
name: omp-channel-octo-cancel-reply-restart-storm
description: "Diagnose omp-channel-octo/Octo bot that stays \"online\" but answers every message with 当前任务已取消 (or 因重启中断) — prove a restart storm from Super/launchd before touching routing or the worker"
---

# 症状

Octo 私聊/群里 channel「在线且已启动」，但每次发消息都收到：

```
当前任务已取消；不回滚已发生的副作用。
```
（或 `channel 正在重启，本会话任务已中断…` / `任务 X 因重启中断，请重新发送。`，进度卡终态显示「已取消」）

不要先怀疑权限、mention、steering 或 OMP worker —— 这句话只有一条来源。

# 唯一来源

`src/service.ts`:
- `DEFAULT_CANCELLED_TEXT` / `DEFAULT_RESTARTING_TEXT`
- `sendCancelledIfNeeded()` 在 `runtime.cancelRequested` 时入 durable outbox
- `cancelRequested = true` 只由两处设置：`stopInternal()`（进程正在停止/重启）与 `handleCancel()`（`/cancel`、`/stop`）

进程停止时 `stopInternal()` 给每个在途 runtime 打 `cancelRequested`，该任务 unwind 时 `sendCancelledIfNeeded()` 把取消文案写进 outbox（任务状态 = `cancelled`、`error_text='cancelled'`）；**下一个进程启动后 flush outbox 才把它发出去**，所以在用户看来是「发消息→立刻被取消」。`tasks.finished_at` 与 `outbox.created_at` 落在同一秒就是这个特征。

# 先分流：风暴还是单次自杀式重启

```sh
grep "Restart requested" ~/.super/logs/launchd.out.log | awk -F'T' '{split($2,a,":"); print $1" "a[1]":00"}' | uniq -c
```

- **172+ 次/小时（≈每 20.7s 一次）** → 重启风暴，走下面「风暴」一节。
- **个位数/小时的孤立一次** → 单次重启；先看它是不是**正在跑的那个回合自己重启了 channel**，走「自杀式重启」一节。

## 风暴：launchd/外部 job 在循环重启

```sh
ps -eo pid,ppid,lstart,command | grep -Ei "super |restart"
launchctl list | grep -iE "omp|octo|restart|card"     # 注意无 plist 的 submit job
launchctl list <label>                                # 读 Program
```

`launchctl submit -l <label> -- /tmp/xxx.sh` 注册的 job **没有 plist**，`OnDemand = false` 会让 launchd 在脚本退出后不断重新拉起：一次性脚本 `sleep 20; ./restart.sh` 就变成每 20.7s 重启一次（实测连续 7 小时、1090+ 次）。

```sh
launchctl remove <label>            # 提示 "Could not find service ... in domain for port" 也可能是成功的
launchctl list | grep <label>; launchctl print gui/$(id -u)/<label>   # 复核
rm -f /tmp/<script>.sh              # 断掉 job 的 Program
```

## 自杀式重启：回合自己 `./restart.sh`

症状：某轮任务跑了几分钟（进度卡显示「已取消 · N 个阶段 · Xm Ys」），`finished_at` 精确等于 super 日志里那次 restart 的时刻；期间该聊天**没有** `channel 已重启完成` 的 outbox 行、`restart_notices` 表为空。

判据（区分「内部请求的重启」与「外部命令的重启」）：任何走 channel 自身路径的重启（`/restart`、`channel_control restart`）都会先写 `restart_notices` 行，新进程启动后投递 `channel 已重启完成：PID …`。没有这条 outbox ⇒ 重启是外部命令（bash 里 `./restart.sh` / `super -y restart`）发起的，正在跑的回合被自己砍断。

定位证据（agent 自己的会话记录）：

```sh
cd ~/.omp/agent/sessions/<session-dir>   # 群会话文件路径见 channel.sqlite chats.session_file
grep -o "restart\.sh[^\"]\{0,80\}" <session>.jsonl | tail
```

`./restart.sh 2>&1 | tail -8` 作为最后一个工具调用出现就是它：agent 改完代码后自杀式重启以加载新实现（先有鸡还是先有蛋——`channel_control` 这类自迭代工具在重启后才存在）。正确做法是下一个回合起改用 `channel_control restart`（或 `/restart`）：重启被排到本回合结束且无运行/排队任务之后，回复不会被砍，通知是「channel 正在重启…」+「已重启完成：PID …」。

# 验收（两类都要做）

```sh
grep -c "Restart requested" ~/.super/logs/launchd.out.log   # 风暴后计数必须冻结；单次重启后应保持不变
tail -2 ~/.super/logs/launchd.out.log
pgrep -fl "<script>.sh"                                    # 无输出
source ~/.super/env.sh; super info omp-channel-octo         # State: Healthy
ps -o pid,lstart,etime -p $(pgrep -f "cli.ts serve" | head -1)   # 存活时间 > 2 个原周期
lsof -p <pid> -nP | grep ESTABLISHED                       # 到 Octo 的 TLS 连接
sqlite3 ~/.local/state/omp-channel-octo/channel.sqlite "select state,count(*) from worker_ledger group by state;"  # 全为 stopped
```

被重启打断的任务已 `cancelled`、不会重跑；steer 模式下用户后续消息被注入同一回合时也会一并被砍（没有独立 task 行），需要用户重发。
收尾提醒：不要在 agent turn 里用 `launchctl submit` 或后台 `./restart.sh` 去重启常驻服务——这是本类事故的成因。
