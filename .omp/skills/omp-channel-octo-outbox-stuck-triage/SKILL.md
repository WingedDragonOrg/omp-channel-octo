---
name: omp-channel-octo-outbox-stuck-triage
description: 诊断 omp-channel-octo/Octo「助手文本不投递但卡片正常」的 outbox 卡死（头阻塞 / 重试风暴），只读取证、安全作废过期行并验证队列排空
---

# 适用

聊天里助手文本毫无反应、而 ask 卡片与卡片编辑可见（卡片直发，不经 outbox）时。先区分：

- 入站没到 → `omp-channel-octo-inbound-delivery-triage`
- 上游生成中断 vs Octo 流投递 → `omp-half-reply-root-cause-diagnosis`
- 本 skill：**outbox 头阻塞 / 重试风暴**（出站文本卡住、卡片却正常）

代码：`src/outbox.ts`（flush/retry/markOutboxDead）、`src/store-outbox.ts`（listPendingOutbox 只取每目标队首、state IN ('pending','unknown')）、`src/octo/api.ts`+`src/octo/index.ts`（sendText/message_edit 的真实错误）。

# 1. 只读取证（不改服务、不改配置）

```python
import sqlite3, os
DB = os.path.expanduser("~/.local/state/omp-channel-octo/channel.sqlite")
conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
# 每个目标的状态分布与队首
conn.execute("""SELECT channel_id, state, count(*) n FROM outbox GROUP BY 1,2 ORDER BY 1,2""")
# 卡死形态：队首 attempts 随时间每秒 +1、last_error 恒为同一句、队尾 attempts=0
conn.execute("""SELECT id, state, attempts, created_at, last_error FROM outbox
                WHERE channel_id = ? AND state IN ('pending','unknown')
                ORDER BY created_at LIMIT 5""", (channel_id,))
```

也可以查 `inbound`/`tasks` 证明消息确实被处理过（服务侧收到并生成过回复），把问题锁在出站投递层。

判读：

- 队首 `unknown`（发送结果未知、无 remote id 的 create）永远重试 = 头阻塞；后面所有行 `attempts=0`。
- 旧实现每个失败行固定 1s 重试，**重启也不排空**（恢复仍从队首开始）。
- `last_error` 只有 `transport unavailable; delivery is unknown` 这类固定文案时，原始 HTTP 状态已被吞掉——只能从同期日志/其他端点（如 message/edit 403）交叉推断，事后无法追回。

# 2. 判定处置：作废而不是补发

小时级过期、语义已被后续消息取代（命令回复、完成汇报）的 pending 行应当作废：补发会把一段过期对话一次性倒进聊天，且可能自相矛盾。

# 3. 安全作废（唯一一处对 state 的写）

单条事务、只改状态不重发；先把原因写进 `last_error` 作为线索，再置 `state='dead'`：

```python
death_note = "voided <date> by outbox repair: stale blocked row, not resent; prior: " + (last_error or "(none)")
conn.execute("UPDATE outbox SET state='dead', last_error=? WHERE id=? AND state IN ('pending','unknown')", (death_note, row_id))
conn.commit()
```

`dead` 不在 `listPendingOutbox` 的 `state IN ('pending','unknown')` 里，所以旧代码也会跳过它；正在运行的服务哪怕刚刚选中该行，随后的 UPDATE 也只会命中 0 行。

# 4. 验证（服务不停机也能验）

- 每个目标 `pending`/`unknown` 计数归零：`SELECT state, count(*) FROM outbox GROUP BY 1`。
- 队首 `attempts` 在两个相隔数秒的窗口内不再增长 = 重试循环已停（比单次快照可靠）。
- 可选：在目标聊天发一条新消息，观察是否落库为 `sent`。
- 修复代码合入并重启后，服务端失败也会自行收敛（1s→30s 退避、6 次或永久失败即 dead），并保留真实原因。
