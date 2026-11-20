# OctoBuddy Agent Hub

日期：2026-11-19

## 用户原始需求

1. 「将 agent hub 视图本身集成到 @packages/octobuddy-enhancer/」；确认目标是 OMP 的 Agent Hub，而不是 `/agents` 配置界面。
2. 选择只读 Agent Hub：查看 agent 树、活动和对话，不提供控制。
3. 确认设计，并补充：「ok，但是不要防御式编程，这只有我自己使用，不应该限制，都 open」。

## 设计

在 OctoBuddy 增强面板中提供 Agent Hub，显示当前聊天绑定会话的 agent 列表、父子关系、状态、活动和对话。宽窗口并排显示列表及详情，窄窗口可切换。切换聊天时同步切换数据，不展示旧聊天内容。

Channel 提供只读 HTTP 接口，增强器通过既有宿主桥接和 HTTP 配置访问；沿用现有 Bearer token，不增加独立凭据、内容筛选或额外权限分级。该 token 因此可以读取 Agent Hub 的任务、活动与对话内容，使用明文 HTTP 时内容和 token 均可能被网络观察。Agent Hub 不提供发消息、停止或恢复操作。

使用运行中 worker 的 OMP 数据和当前会话的持久 agent 记录；查询不启动 worker、不改变会话。可读取的原始对话和活动直接展示，不人为裁剪内容。数据不可用时如实显示；区分无 agent 和数据读取失败。通过隔离 fixture 验证运行中和持久记录、聊天切换及界面展示；不重启常驻服务或使用真实登录资料。

OMP RPC 的 `get_subagents` 返回当前 worker 的 roster 和最新 progress，不提供完整历史 progress 事件流或父子关系；父子关系从当前会话的 artifacts 目录还原。已停止 worker 的记录展示为持久状态，不能据此推断原生 Hub 的实时内存状态。历史会话与实时会话读取均不启动额外 worker。
