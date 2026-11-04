# Loop workspace 任务感知与子区路由

日期：2026-11-04
状态：已实现并通过客观预过滤及隔离验收；未部署

## 背景与目标

由 owner 将 Loop workspace 显式绑定到 Octo 主群。channel 使用当前 Bot 合法获准的 REST API 对绑定 workspace 做全量分页快照，但 agent 输入仅包括已有当前 task—子区路由的任务，或 `assignee_type=member` 且非空 `assignee_octo_uid` 匹配当前持久化 channel owner UID / Bot UID 的任务。owner 从持久 channel store 读取；比较 `assignee_octo_uid`，不把 Fleet UUID 型 `assignee_id` 当作 Octo UID。首次 baseline 与增量使用相同门槛；其他人指派或未分配、且无路由的任务只推进快照，不启动或 steering agent。已有路由的任务即使改派仍直达原子区。owner 人工交办和显式 `thread-bind` 保持原有路径；输入资格由宿主的身份与路由字段匹配确定。

本规格只定义 channel 自身能力。Fleet 源码及服务不作修改；不注册 daemon/runtime、不 claim Loop execution、不实现其执行协议。Loop 评论、状态、分配及其他业务写回不因绑定或收到任务数据而获得授权，仍需单独授权。

## 关键决策

| 项目 | 决策 |
| --- | --- |
| 入口 | owner-only 内建命令 `/bind loop <url\|workspace_id\|workspace_name\|workspace_slug>` |
| 扫描范围 | 仅扫描持久绑定的 workspace ID；名称和 slug 的发现仅发生在绑定解析时 |
| 初次绑定 | 提交覆盖 workspace 全部任务的首次快照；仅对符合路由/成员指派门槛的任务交付 baseline，零符合任务不启动空 baseline agent |
| 自动动作 | 符合交付门槛的任务由对应 agent 处理；主群 agent 可沿已有流程复用子区、维护关联、新建子区并派发 |
| 澄清 | agent 自行决定在主群或对应子区与人澄清 |
| 路由 | 当前 task 路由直达对应子区；无路由但由 owner/Bot 成员指派的 task 交给主群；其他无路由 task 只更新快照 |
| 主群获知进展 | 沿用子区创建时的 `listen` 策略，或子区主动通知；不逐条转发已关联任务的 Loop 变化 |
| 感知方式 | 默认每 60 秒分页扫描并比较任务快照；一次扫描不与自身重叠 |
| 可靠性 | 持久保存基线、变化、关联及创建意图；状态轮询不是完整事件日志 |

60 秒是工程默认值，不增加本次范围之外的轮询配置面。正常感知延迟约为一个间隔加扫描、排队耗时；故障和服务停机期间没有固定时限保证。

## 已验证的接入事实

以下区分现有事实与待实现设计，不能将本节只读验证当作 channel 集成验收。

- 当前安装 `octo-cli schema task.list`：`GET /fleet/api/v1/tasks`，必须提供 `X-Workspace-ID`；使用一基 `page`、`page_size`，返回 `total/page/page_size`，响应契约页大小最大 100。
- 列表支持 `date_field=created_at|updated_at` 与 `date_start/date_end` 同时使用，起点包含、终点不包含；没有已确认的 `updated_since` 或事件回放 cursor 契约。本设计先采用完整分页快照，不把时间筛选等同于可靠增量事件流。
- `task.get` 为 `GET /fleet/api/v1/tasks/{task_id}`；`workspace.get` 为 `GET /fleet/api/v1/workspaces/{workspace_id}`，均使用显式 workspace 范围。workspace 名称和 slug 可通过有权限的分页 `workspace.list` 解析。
- 已以当前 Bot `28azkybli6q43216d14_bot` 完成只读实测：workspace 列表成功；一个授权 workspace 的任务第 1、2 页返回不同任务，总数为 11；任务详情和 `updated_at` 时间范围查询成功。该次样本记录了 `task_id`、`assignee_type/id`、`created_at/updated_at`，未验证新增字段的线上取值。Fleet `internal/handler/issue.go:61` 将可空 `AssigneeOctoUID` 输出为非省略的 `assignee_octo_uid`；`internal/publicapi/normalize.go:55–69` 保留该字段，`:234–247` 仅转换 agent/squad 类型、保留 member。Fleet `docs/member-assignee-octo-uid.md:5–16,38` 明确 member 的 `assignee_id` 是 Fleet user UUID、`assignee_octo_uid` 是 Octo UID；channel 只用后者匹配 owner/Bot。
- Fleet `internal/middleware/workspace.go:758–790` 校验 Bot、Space/workspace 范围并进行 Project 授权；`owner_fallback.go:12–35` 仅接受服务端验证的 owner 授权来源。channel 不选择他人身份或绕过权限。
- Loop 客户端 `packages/dmloop/src/issueDeepLink.ts:26–72` 定义 `/fleet/<workspace-slug>/issues/<identifier>` 与 `/fleet/<workspace-slug>/projects/<project-id>` 两种含 workspace 的链接；普通 `/loop` 不携带 workspace 身份。
- channel `src/service.ts` 的 `hostToolHost().deliverToChat` 直接走追加或独立任务接受；`src/threads/thread-notify.ts` 已定义系统通知不是问题答案或新增授权。新输入复用这些边界，不走普通文本回答 ask 的入口。

既有验证覆盖 Bot 的只读字段样本、本地 HTTP 分页与错误边界，以及旧版隔离的 Loop→主群/子区、迁移和重启路径；本次新增公开字段由 Fleet API 源码与契约文档证明，未声称有新的线上值实测。实际服务器的跨页一致性与大 workspace 持续轮询负载仍需按部署规模观察；本规格不把局部读权限或本地验收等同于这些线上保证。现有用户 WebSocket 广播及 status webhook 不作为本设计依赖，也不要求 Fleet 增补能力。

## 命令与绑定

### `/bind loop <selector>`

命令只允许 owner 在已开通的主群执行；私聊及子区返回适用范围说明，不创建绑定或 agent 任务。绑定归属为当前 Bot 与主群，群聊绑定不是 Loop 服务端授权。

解析剩余全文作为一个 selector，支持名称中的空格：

1. UUID：直接读取同一 ID 的 workspace，校验实际访问权限。
2. URL：只解析与当前 Octo API 部署同源的已知 Loop 深链接路径，从上述任务/项目链接提取 workspace slug，经完整授权列表唯一解析到 ID，再读取 workspace 验证。链接用于提取 workspace，不绑定链接中的单个任务或 Loop project；不请求任意 URL，不从无 workspace 身份的 `/loop` 推测当前选择。
3. 名称或裸 slug：分页读取当前 Bot 可访问的全部 workspace，同时按完整名称及 slug 精确匹配，以不同 workspace ID 去重；零匹配报未找到，多个 ID（包括名称与另一 workspace 的 slug 冲突）报歧义并列出候选 ID。列表未完整读取不得宣布唯一或提交绑定。

列表记录以 `workspace_id` 为身份，详情以 `id` 为身份；兼容既有详情 `workspace_id` 形状。若记录同时包含两个身份字段，均须为有效 UUID 且一致；详情身份还须与请求 ID 一致。只接受服务端返回的 workspace ID，不从 slug 推导 UUID。当前 Octo Server Project 派生 workspace 使用该 Project UUID；Loop 业务项目的 `project_id` 是独立身份。

确认 ID 指解析、验证并在成功回执展示最终 ID，不增加第二次交互确认。成功回执包含主群、workspace 名称和 ID；持久化以 ID 为准，名称变更不改变目标。

每个主群有一个当前 workspace 绑定。重复绑定同一 ID 保留原关联并执行一次状态扫描，不重复进行初次整理；显式绑定不同 ID 替换该群当前绑定，递增绑定版本。旧 workspace 的在途扫描结果不能提交到新版本，旧待交付输入不再新增派发；已接受的 agent 工作不自动取消。旧任务关联按原 workspace 保存，不串到新 workspace。重新绑定已有历史的 ID 时使用其持久关联进行整理。

绑定解析或首次完整任务读取失败时，不发布成功绑定、不启动初次整理，已有有效绑定保持原状。名称列表只在上述解析需要时使用；日常扫描、启动恢复和 ID 绑定不要求枚举 workspace。

不同主群的显式绑定独立。相同 workspace 被绑定到不同群时，快照读取可共享，自动处理和任务关联仍以各自群的授权范围为准；不跨群推断或复用子区。

### 初次整理输入

首次完整扫描将绑定版本及覆盖 workspace 全部 task 的快照在本地事务中发布。按本规格的同一任务交付门槛生成 baseline：有当前路由的 task 直达对应子区；没有路由、但满足 owner/Bot 成员指派条件的 task 才进入主群 baseline。无符合条件的 task 时不创建或交付空 baseline agent 输入。owner 仍通过原命令路径收到绑定回执；不能将原 `/bind` 文本当作模型 slash 命令再次执行。

Baseline 摘要携带 workspace 身份、task ID、符合门槛的任务摘要、已有 task—子区关联、允许的自动动作及来源说明。每批最多 20 个摘要，标明本批稳定身份、数量和 task IDs；标题/描述分别预览最多前 160/600 个 UTF-16 代码单元，超限附省略号与截断标记，并提示按 workspace/task ID 读取完整详情。存储为全部 workspace task 保留完整快照；不符合 agent 交付门槛的任务不进入 baseline 或 agent prompt。基线提交后观察到的变化另行持久化并按完全相同的路由/指派门槛处理。

Agent 处理收到的 task 输入，可沿现有流程保持/调整关联、复用或创建子区并派发。任务交付资格由宿主按负责人身份和明确路由确定。

## Loop task 与子区关联

### 身份与工具

“Loop task ID”与 channel 自己的执行 task ID 分开命名。完整路由键为 `(bot_uid, parent_group_id, workspace_id, loop_task_id)`，一个路由键只有一个当前子区目标。一个子区可以承载多个显式关联的 Loop task，改绑某个 task 不影响其他关联。

`octo_management` 扩展如下：

- `create-thread`：可选 `task_id`。从调用所在主群或子区的真实父群解析当前 workspace；验证 task 属于该 workspace 后，为该 task 预留创建意图，准备子区，ready 时发布关联。创建与绑定完成后，agent 再用 `thread-dispatch` 派发正文。
- 新增 `thread-bind`：必填 `task_id`，按既有 `short_id` 规则定位本群子区；设置或显式替换该 task 的当前关联。重复绑定到同一目标幂等。回执包含 workspace ID、task ID、原目标及新目标；替换不删除原子区或取消其已接受工作。
- `thread-dispatch`：可选 `task_id`，用于标注任务来源。携带时必须与该目标的当前关联一致；派发本身不隐式改绑。workspace/task 来源随实际子区输入持久保存，当前路由在实际接受前复核。同一 channel 任务的同一 native `toolCallId` 使用稳定输入身份，已确认接受不重复执行，steer 结果不明时保留 pending claim 并拒绝降级为第二个任务。普通派发省略时维持原行为。
- `thread-info`、`list-threads`：返回本地已知的关联 workspace/task IDs；保留既有分页/截断说明，不通过名称或 THREAD.md 猜关联。

带 `task_id` 的工具只能操作本群当前绑定 workspace 及本群子区，不接受模型提供其他 workspace 或 owner 身份来扩大范围。已关联 task 再次请求 `create-thread` 时返回既有目标；需要换目标时使用显式改绑，不能自动再建一个。

`task_id` 单值对应一个当前路由目标；`create-thread` 携带它时不得一次产生多个 separate 子区，相关参数在外部副作用前拒绝。多项目可创建一个 shared 子区；需要 separate 子区时先按既有方式创建，再由 agent 显式选择其中一个作该 task 的路由目标。

### 创建与改绑顺序

创建预留以路由键唯一持久化，关联 native `toolCallId` 和 channel 任务身份，在远端创建前写入；普通子区和有 worktree 子区都覆盖。已有 ready 目标、已知部分结果或未确认远端分配必须被后续调用识别：恢复并核对已有结果，不重复创建。未知远端分配不自动重试创建。

创建预留记录其依据的 workspace 绑定版本和 task 关联版本。`thread-bind` 显式修改关联会递增版本；创建 ready 发布及重启恢复必须以原版本条件提交。期间已发生改绑或 workspace 替换时，原创建不得覆盖当前关联：保留实际分配结果及资源归属供核对，回执分别报告已创建资源和当前有效路由目标，不报告原目标绑定成功，也不自动派发或删除已创建资源。

只有远端子区、所需工作区、绑定及说明准备成功才发布可路由关联；ready 与关联发布保持原子可见。THREAD.md 继续只记录简短背景和实际路径，任务正文及验收放入派发内容。

改绑与实际输入接受之间复核关联版本：尚未接受的变化送当前有效路由；旧目标已确认接受的输入不再复制给新目标，也不自动取消。没有当前路由的变化只有在 `assignee_type=member` 且非空 `assignee_octo_uid` 匹配持久 channel owner UID 或 Bot UID 时才进入主群，主群按本聊天顺序处理；其他变化只推进快照。创建完成前的待交付变化在接受前重新选择目标。

## 扫描、变化与交付

### 快照比较

- 只读已绑定 ID，默认每 60 秒完整分页扫描，页大小 100，使用服务端已有的 `created_at asc` 排序。一次扫描内按 task ID 去重；分页结束后才可提交。
- 快照比较涵盖任务创建与列表/详情可读的业务变化：标题、描述、状态、优先级、`assignee_type`、Fleet UUID 型 `assignee_id`、`assignee_octo_uid`、Loop project、父任务及计划日期。比较规范化业务字段；`updated_at` 是来源版本信息，不因它单独改变就重复唤醒 agent。涉及详情的字段由实际读契约补读，不能凭空假设列表完整。
- 评论、执行日志、运行时心跳及每次中间修改不属于本次感知范围。assignment 变化（包括取消分配）仍推进 workspace 全量快照。实际交付时先检查当前 task 路由：有路由即送原子区，不受新 assignee 影响；无路由时仅当 `assignee_type=member` 且非空 `assignee_octo_uid` 匹配当前持久 channel owner UID 或 Bot UID 才送主群，其他情况仅记录快照。
- 初次 baseline 与后续扫描都覆盖全 workspace 并应用完全相同的门槛。首次或新出现的 task 有当前路由时直达子区；没有路由但符合 owner/Bot 成员指派条件时送主群；其余 task 不产生 agent input、任务、steering 或通知。符合门槛的主群输入可用现有工具创建子区、绑定和派发。
- 扫描中任何一页或必要详情读取失败，丢弃本轮候选，不改变成功基线，不生成本轮部分变化。下一轮仍比较最后成功快照。
- 任务从列表消失不能单凭缺失认定已删除或解除关联，不自动建区、清理资源或开工；删除事件的完整跟踪不在本次范围。

成功快照及其派生变化在同一事务保存。变化记录包含路由键、递增本地版本、前后业务值、观测时间、来源版本和交付状态；稳定输入 ID 来自这条变化记录，不以 `updated_at` 或内容 hash 单独作全局唯一键，以保留跨扫描观察到的 A→B→A。

定时扫描和同 workspace 的显式绑定扫描共享当前群/workspace 的读取—提交串行段，避免较早读取的旧快照在新结果后回写。绑定 selector 解析独立进行，不同 workspace 的显式替换不等待旧 workspace 读取，旧结果仍由绑定版本及请求代次拒绝。

### 路由与恢复

实际交付前重新读取当前绑定、关联、最新成功快照中的负责人及聊天权限；实际接受独立任务或发起 steering RPC 前再次校验：

```text
Baseline 或 Loop task 变化
  ├─ 有当前路由 → 对应子区 agent（不看当前 assignee）
  ├─ 无路由，member 指派给 channel owner/Bot → 主群 agent
  └─ 无路由，未分配/指派给其他人/UID 不匹配 → 仅快照，不启动 agent
```

既有子区删除、工作区未就绪或访问拒绝时，保留变化和关联，不静默改送主群当作未关联任务，也不自动新建。需要人或主群修正关联时，每个未解决故障产生一次有界处置提示；它不是逐条任务变化通知。归档子区保留关联，沿用既有消息到达后的重新激活行为。

输入携带明确的系统来源、workspace/task 身份、变化摘要和当前关联，不冒充用户命令，不回答 pending ask，不授予新增权限。遵守目标聊天的追加模式和既有深度限制；遇到正在等待 ask 的运行任务时保留独立待处理输入，不把 Loop 数据作为答案或强制打断等待。

channel 在真实 steering RPC 前落盘派发意图，独立任务使用稳定输入 ID durable 接受。未派发输入可在恢复后交付；已确认接受的工作按现有原 session 恢复；steering 接受状态不明时不自动重放，记录为待核对，防止重复副作用。重启先恢复旧进程所有权，再处理这些输入。稳定 ID 和持久回执保证重复扫描、重复命令及重复交付不重复接受同一变化；不承诺底层接受状态不明时自动 exactly-once。

取消清理只将实际接受的系统输入记为已交付；尚未发起 RPC 的等待输入释放其 steer claim，保持待交付状态。停机先封闭并中止 Loop 的新读取/交付，接着关闭 worker 以释放 RPC 和 steerSerial 等待，最后在关闭 store 前等待 Loop drain；不能先等依赖 worker 退出的派发完成再关闭 worker。

尚未接受的 pending 输入在事务中重新准备：按最新快照裁剪失去关注资格的任务；主群 baseline 内已建立子区关联的任务持久拆为逐 task 子区输入，余下的 owner/Bot 任务继续以有界主群批次交付。全无关注任务时将该输入置为 `superseded`。拆分后的输入身份持久保存，重复准备不重复生成；`intent`、`unknown` 和已接受输入保持原接受语义。

### 主群与子区的信息边界

已关联 task 的正常变化只发子区，主群没有对应镜像消息、agent 任务或 steering。主群获知工作结果沿用 `docs/specs/2026-09-17-thread-completion-notify-design.md`：`listen=off|merge|task`，默认 merge；listen 不是 Loop 变更订阅。

Loop 变化直接在子区接受为独立 channel 任务时，保留系统来源，并使其执行终态按该子区现有 task 监听策略判定；steering 部分仍没有独立终态。合并失败及修复失败的抑制保持不变。子区主动通知主群走当前已授权消息能力，不新增越权身份或自动向 Loop 写回。

## 职责与持久化边界

- Loop 读端口独占 CLI/REST 请求、分页与响应归一化，沿用当前 Bot 的凭据和 API 路由，不注册或切换 Bot。部署未具备有效读权限时明确报错，不伪造空列表。
- workspace 感知协调器保留绑定 workspace 的全量分页快照并比较业务变化；只按当前 task 路由优先、否则按 member `assignee_octo_uid` 与持久 owner/Bot UID 精确匹配决定 agent 目标。不得按 assignee UUID、project 名单或主观任务相关性筛选；不创建子区、不自行操作 Git。
- ChannelStore 拥有群—workspace 绑定版本、快照、变化/初次整理交付台账、task—子区关联及创建预留，原子约束放在存储层。
- CoreService 负责输入路由、权限复核、聊天/session FIFO 和生命周期；agent 通过宿主 thread 工具维护关联并执行工作，不让扫描器直接调用模型或操作 Git。
- 旧数据库迁移为上述能力增加存储；没有 workspace 绑定的既有聊天行为保持不变，不能从上下文文字推导绑定。跨进程恢复沿用现有 worker、创建意图和 workspace 所有权规则。

## 真实保证与成本

状态查询不提供完整事件日志，分页也没有已验证的跨页一致快照保证。查询间发生后又撤回的创建/分配或业务变化可能不可见；页间并发变动可能造成当轮跳项，后续扫描用于再次核对持续存在的状态，不承诺每次中间变化不丢。要逐事件保证需要不同的上游读取契约，当前设计不作此承诺。

每个独立 workspace 全量扫描约需 `max(1, ceil(N/100))` 次列表读取，必要详情另外计费；1,000 个任务按分钟扫描约 600 次列表请求/小时。不同群绑定同一 workspace 时可共享读取，但保留独立授权与路由。扫描本身不调用模型；仅对首次 baseline 或后续变更中符合“已有 task 路由，或 owner/Bot 当前 member 指派”条件的输入启动 agent。实际限流、列表字段覆盖和大任务量耗时需在实施验收中测量，不以样本权限推定所有 workspace 可读。

## 验收标准

1. owner 可用 UUID、已支持的 Loop 深链接或完整名称绑定主群；名称唯一解析覆盖全部分页，重名/无匹配/中途失败有明确结果且不改已有绑定。无 workspace 身份的 URL 不猜测目标，外部 URL 不触发任意地址请求。
2. 成功回执显示实际 workspace ID 和目标群；首次快照覆盖 workspace 全部任务，但 baseline 仅交付符合“当前路由或 owner/Bot 成员指派”门槛的 task；零符合任务不启动空 baseline agent。重复命令、重启及同 ID 重绑不重复整理；替换绑定时旧扫描不能提交，新旧 workspace 关联不串用。
3. 日常扫描、启动恢复及 UUID 绑定不调用 workspace list；未绑定 workspace 无任务扫描。第一页成功而后续页失败时，成功基线和派生变化均保持不变。
4. 首次 baseline 和增量严格按相同规则筛选 agent 输入：已有当前 task 路由，或无路由且 `assignee_type=member`、非空 `assignee_octo_uid` 匹配持久 channel owner UID / Bot UID。符合条件的 owner/Bot task 可沿原工具路径更新关联、创建子区并派发；其他无路由任务不自动交给 agent。Fleet UUID 型 `assignee_id` 不与 Octo UID 比较；人工交办和显式 `thread-bind` 不受自动筛选限制。
5. task 创建、assignment 变化/取消分配及所列业务字段（包括 `assignee_octo_uid`）可被全 workspace 快照比较识别；纯更新时间变化不触发。跨扫描 A→B→A 是两个变化；未观测中间态不伪造事件。非合格任务变化推进快照但不产生 agent 副作用。
6. 有当前路由的 baseline/增量变化仅到对应子区，且不因 assignee 改派中断；无路由时仅当前 owner/Bot member assignment 的 task 到主群，其他人指派和未分配 task 只推进快照。改绑后未接受变化仅到新目标，已接受输入不复制；不同群、不同 workspace 的同名任务不串线。owner 人工交办和显式 `thread-bind` 沿原路径。
7. 同一 task 的并发/重试创建只产生一个当前目标；远端创建前后、ready 前后和绑定发布前后的崩溃覆盖普通及 worktree 子区。创建期间显式改绑或替换 workspace 时，旧创建及其恢复不能覆盖新关联，回执区分已分配资源与当前目标。未知远端分配保持待核对，不重复分配；多目标 separate 参数在副作用前拒绝。
8. pending ask 保持原答案所有权，Loop 输入不作为答案、不当作 slash 命令、不扩大权限。符合门槛的 agent input 可按原语义澄清；只更新快照的 task 不产生模型请求、channel 执行任务、子区创建、steering 或通知。
9. 子区缺失/未就绪/无权限不隐式解除关联或重复建区，未解决故障只产生一次处置提示。归档可按现有消息语义重新激活；不因列表缺项自动删除资源。
10. 既有 listen 默认与合并失败抑制保持；task 档覆盖直接接受的 Loop 独立任务终态，steering 无独立终态。每次 Loop 变化不会因订阅而自动镜像到主群。
11. 数据库迁移在抛弃式只读一致性副本上用真实应用迁移演练并二次打开，既有数据保持；隔离进程/协议夹具验证扫描→路由门槛→主群/子区交付、故障恢复和接受状态不明路径。Loop CLI benchmark 保留真实 60 秒 timer、原生单次建区和两次 restart；证明其他成员/未分配 task 的初次与增量输入被抑制且快照推进、prompt 不含这些 task IDs、对应无 agent 副作用，已关联 task 改派后仍直达 child。运行定向回归、typecheck、全量测试及既有 restart benchmark。
12. 验收不修改 Fleet、不注册常驻 Bot、不调用真实写接口、不创建真实 Loop 任务或子区、不重启常驻服务。真实 Bot 读权限验证仅作明确授权范围内的只读检查；模型及 Octo 写入行为使用隔离本地夹具。

## 交付边界

实现与文档保存在当前任务工作区，未提交、推送、合并或部署；常驻服务、生产数据库结构、Loop 业务状态及本子区 THREAD.md 保持原运行状态。实际环境只做当前 Bot 身份和授权任务字段的只读检查，所有 Bot 注册、线程创建、模型请求及重启验收均发生在独立本地夹具。

## 实施与验收记录

2026-11-04，Bun 1.4.4；安装版 OMP、真实 channel CLI/RPC、有限 localhost 模型及本地协议夹具：

- `bun run typecheck` 通过；最终 `bun test` 为 **1418 pass / 0 fail**，104 个文件、8678 条断言。
- 定向 Loop API/store/core 回归 **78 pass / 0 fail**。覆盖 owner/Bot 与其他成员、未分配、Fleet UUID/Octo UID 区分、类型和 UID 碰撞、转入/转出、明确路由、空 baseline、全量快照、pending 裁剪及事务拆分、接受状态保护、交付前改派/改绑和 ask 隔离。
- `bun tests/benchmarks/loop.ts` 通过：真实定时扫描先将两项不关注任务的新业务值写入快照，模型请求、执行任务、建区和消息副作用均未增加；随后第二次定时扫描将已关联任务改派给其他成员的变化直达原子区。两次重启后仍无重复输入或主群镜像。最终为 **6 次模型请求、10 次 Loop 读取、1 次建区、1 个主群任务、3 个子区任务**，不关注任务 2 项，其 agent 副作用为 **0**；重复输入、主群变更镜像及 workspace list 请求为 **0**。有限模型检查无关 task ID 及独有标题/描述不会进入 agent prompt。
- `bun tests/benchmarks/restart.ts` 的 graceful/SIGKILL 两场景均通过；恢复在途任务 2 个、恢复排队任务 2 个，中断通知、会话重叠与重复副作用均为 0。
- 同一组 SQLite DDL 已通过真实状态库的只读一致性副本演练：`ChannelStore.open` 应用迁移并二次打开，原 **27 张表、12,223 行**的全部原列逐表摘要保持一致，新增 5 张 Loop 表，`integrity_check=ok`。私有副本已删除，生产数据库未经应用迁移。负责人 UID 存于任务 JSON，本次过滤修正未增加 DDL；回归同时验证旧 JSON 缺该字段与 `null` 等价。
- 当前 Bot 的授权只读任务样本已验证此前的 14 个公开字段；新增 `assignee_octo_uid` 已核对现有 Fleet 响应与 public API 保留契约，并通过本地真实 HTTP list/get、详情补读及非法类型回归，尚未实测其线上取值。线上跨页一致性、大 workspace 负载仍属于运行观察边界。

| 验收项 | 当前证据 |
| --- | --- |
| 1 | `tests/loop-api.test.ts` 的分页名称/URL/UUID、重名、错误响应；`tests/core-loop-command.test.ts` 的命令作用域与无模型误启动。 |
| 2 | `tests/store-loop.test.ts` 与 `tests/core-loop-prefilter.test.ts` 的有界 baseline、零关注任务、子区分路、同 ID 幂等和版本替换；真实 CLI baseline 建区派发。 |
| 3 | API 分页失败与不完整响应回归；routing 的读取失败/替换竞态；真实 CLI 读取审计无 workspace list。 |
| 4 | API 的 canonical Octo UID；store/core-prefilter 的严格身份及路由谓词；`tests/core-loop-tools.test.ts` 的显式绑定、改绑、人工操作与来源校验。 |
| 5 | store 的时间戳不触发、业务差异、身份解析变化、转入/转出、缺失保留及已观察 A→B→A；真实 CLI 的忽略任务新业务值快照。 |
| 6 | core-prefilter 的既有路由 baseline、改派后子区直达、接受前重检与跨范围隔离；真实 CLI 的原子区回执及主群任务计数。 |
| 7 | store/tools 的普通与 worktree 创建、唯一预留、并发/重放、未知及部分远端结果、ready CAS 和多 separate 拒绝。 |
| 8 | core-prefilter 的不关注事件零输入与 pending ask 保持；routing 的系统来源、队列和完整数据；真实模型请求边界的任务泄漏检查。 |
| 9 | routing 的关联子区不可用、故障提示去重与恢复；tools 的未知分配门禁及原有归档行为回归。 |
| 10 | routing 的直接 Loop 独立任务 listen 回传；既有 thread-notify 与合并抑制测试；真实 CLI 的主群镜像为 0。 |
| 11 | 最终定向/全量/类型检查、两次实际周期扫描的 CLI benchmark、restart benchmark、真实数据库副本迁移及旧 pending/JSON 回归。 |
| 12 | 写路径与 Bot/模型协议验收全部使用 localhost 和抛弃式资源；真实环境仅作授权身份/字段只读检查及数据库 backup。 |
