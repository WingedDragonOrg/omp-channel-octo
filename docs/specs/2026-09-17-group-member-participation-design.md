# 群成员参与（成员消息驱动与 ask 卡片作答）

日期：2026-09-17

## 背景与目标

现状：已开通群/子区里只有 owner 的消息能驱动助手；ask 问答卡片也只有 owner 能作答——`src/questions.ts` 的 `answerFromCard` 校验 `operatorUid === owner`，其他成员点击后卡片短暂置灰又恢复，问题保持挂起（现场证据：同事在群里提交后无任何效果）。

目标：让已开通群/子区内的成员真正参与协作——

1. 成员的普通消息（当前全部支持的内容类型：文本、引用、附件、语音转写与富文本）与 owner 一样驱动助手；
2. 成员可以点击/提交 ask 卡片答案，也可以直接回复文本作答；
3. 每条答案记录提交人并交给 agent（agent 必须知道是谁答的）；
4. 命令保持仅 owner；本设计不新增配置项，不引入第二套权限模型。

这是对主规格「只有用户本人可以驱动本机」的一次有意放开：成员任务在群会话里以既有 yolo 模式全自动执行，并使用该聊天的全部 host tools（含 `octo_management`、`channel_control`）。发布方应只把 Bot 留在可信群。

## 范围

### 包含

- 权限门对成员消息的放行规则（已开通聊天 + 结构化 @Bot）与其拒绝路径的无副作用约束。
- 成员消息与 owner 消息共用同一条后续路径（待回答问题作答、附件组装、steering、排队与任务接受）。
- 成员 `/` 开头消息的节流提示（不执行、不交给 agent）。
- ask 卡片点击的成员授权（群/子区）。
- 答案提交人记录（问题表 `answered_by`）、ask 工具返回、卡片改写与超时唤醒文本中的提交人标注。
- 主规格、根 `AGENTS.md` 与测试同步。

### 不包含

- 成员使用任何命令（含 `/status`、`/help`）：一律不执行、不交给 agent。
- 私聊：仍只有 owner；首次认领、去重与 `owner@bot` 归一规则不变。
- 未开通群/子区的成员消息：保持静默拒绝；owner 的未开通提示与 `开通本群` 口令路径不变。
- 群级免@（`mention_pref.effective`）：只对 owner 生效，成员永远需要结构化 @Bot。
- 按发送者区分工具集或能力：host tools 仍按聊天与任务组装，不按发送者裁剪。
- 答案的优先级语义：不设 owner 优先或覆盖，沿用既有 compare-and-set（先落答案者胜）。
- 新增配置项、人员名单或第二套权限存储。

## 已知约束

- 成员消息必须通过既有前置条件：Bot 已注册、持久 owner 存在、消息 ID 与发送者非空、发送者不是 Bot。owner 缺失时任何消息都不进入处理（首次认领不变）。
- mention 判定用归一化后的结构化 `mentionedUserIds`；文本型 `@BotName` 的兜底匹配只用于命令解析前的 mention 剥离。
- 卡片点击经 Bot 事件队列送达（`/v1/bot/events` 长轮询与 ack）；服务端已按有效卡片帧校验可见性，channel 侧仍按问题行所在聊天再校验一次授权。
- 文本作答与点击作答的唯一仲裁点是问题行的 compare-and-set（`answerQuestion` 的 `state = 'pending'` 条件更新）。
- 群历史记录、消息去重、追加模式（steer/queue）、附件下载与语音转写、会话与目录隔离都不按发送者区分。
- OMP 会在 RPC prompt 路径上对内建命令（如 `/switch`、`/new`）做命令分发（响应 `agentInvoked:false`），因此成员以 `/` 开头的文本绝不能作为任务正文交给 OMP。
- 问题行已持久化卡片 message ID（`card_message_id`），点击按 `(botUid, target, messageId)` 定位问题行。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 成员范围 | 已开通群/子区内的全部成员，不新增名单 | owner 拍板；复用既有群级门禁（`allowedGroupIds` ∪ `allowed_chats`，线程继承父群），不引入第二套权限模型 |
| 成员放行条件 | 已开通聊天 + 结构化 @Bot | 明确指向 Bot 的意图；群里每条消息都启动任务会淹没会话 |
| 群级免@ | 仍只对 owner 生效 | 免@是 owner 的会话便利，不是全群广播权限；成员不 @ 的消息零副作用 |
| 成员命令 | 不执行、不交给 agent，回一条节流提示 | owner 拍板；交给 agent 等于把 OMP 内建命令面暴露给成员 |
| 提示节流 | 同一聊天 10 分钟至多一条（进程内，重启可再来一条） | 与本群未开通提示同一先例；重复命令不刷屏 |
| 作答授权 | 问题所在聊天是已开通群/子区时任何成员可点击/提交；文本作答走同一门禁 | 与「成员可聊天」同一信任边界；私聊保持 owner |
| 提交人记录 | 问题行新增 `answered_by`（uid）；随答案进入 ask 工具返回值、唤醒文本与卡片改写 | agent 必须知道谁答的；uid 是事件里唯一到达的身份，需要名字时 agent 可用 `octo_management` 的 `group-members` 查 |
| 卡片文案 | 不提示可作答范围；答案改写显示「已由 <uid> 回复」 | 避免把权限细节发给全群；作答结果对群透明 |
| 答案仲裁 | 沿用 compare-and-set：先落答案者胜，落败点击静默 ack | 不引入优先级；owner 与成员同一条路径 |

## 设计

### 入站权限门（`src/service.ts` 的 `isAllowed`）

前置检查不变（bot、owner、非空 ID、Bot 回声）。之后：

- 私聊（channelType=1）：`senderId === ownerId`，不变。
- 群/线程（channelType=2/5）：
  - sender 是 owner：`isPermittedChat` → 有结构化 mention → `groupAllowsNoMention`（免@，60 秒缓存）——逐步不变；
  - sender 是成员：`isPermittedChat(message)` 为真且 `mentionedUserIds` 含 Bot uid 时才放行；否则拒绝，且**不查询** mention_pref（不为被拒消息打上游请求）。

被拒的成员消息与既有拒绝路径一致：不落 `inbound`、不写任务、不发提示、不打日志（`handleUnlistedOwnerContact` 仍是 owner-only）。

放行后成员消息走与 owner 完全相同的后续路径：`!supported` 拒绝 → 命令分流（见下）→ 待回答问题作答 → 附件组装点 → steering / 排队 / 接受任务。任务行、回执、失败分类、卡片与 typing 都不区分发送者。

### 成员命令提示

在 `!message.supported` 拒绝之后、命令分流之前插入：

- 条件：`senderId !== ownerId` 且 `isCommand(text)`（text 为已剥离 Bot mention 并 trim 的正文）。
- 动作：以命令 claim（`commandClaim(message, "member-command")`）做消息级去重——重投不重复处理；同一聊天 10 分钟窗口内至多投递一条提示（进程内节流，与未开通提示同构），窗口内后续命令消息只消费 claim、不提示。
- 提示文本：`命令仅 owner 可用；需要助手做事请直接 @<botName> 说明。`（名称为空回退 uid）。
- 不执行任何命令、不组装附件、不进入作答路径、不建任务。

owner 的命令路径（内建命令、注册 handler、未知 `/xxx` 透传任务）完全不变。

### ask 卡片作答授权（`src/questions.ts` 的 `answerFromCard`）

- `ownerId` 非空且 `action.operatorUid.trim() === ownerId`：owner 路径不变（任何聊天）。
- 否则视为成员：`operatorUid` 非空，且 `memberAnswerAllowed({ channelType, channelId })` 为真才继续；为假返回 `ignored`（ack 但不落答案、不重绘、不唤醒）。
- `memberAnswerAllowed` 由服务注入（`QuestionDeskDeps`）：`target.channelType !== 1 && isPermittedChat(target)`——私聊只有 owner；群/子区按既有允许列表（含线程继承）判定，与消息门禁同一函数、同一数据源。

作答后续流程与 owner 完全相同：`listQuestionsByCard` 定位问题行、`parseAskActionId` 解码、`resolveAskAnswer(s)`/`normalizeAnswer` 校验、compare-and-set 落答案、卡片改写、等待者释放或超时唤醒。

### 答案提交人

- 存储：`questions` 表新增列 `answered_by TEXT`（迁移加入既有 ALTER 列表；旧库已作答行为 NULL）。`QuestionRecord` 增加 `answeredBy: string | null`。
- 写入：`answerQuestion`/`answerQuestions` 接收 `answeredBy`；文本路径取 `message.senderId.trim()`（owner 与成员同构），卡片路径取 `action.operatorUid.trim()`。
- 等待者：`ask` 的等待以 `{ answer, answeredBy }` 结算；工具返回 `{"answers":[{"id","questionId","answer","answeredBy"}]}`。
- 超时唤醒（`wakeForAnswers`）：`答案（由 <uid> 回复）：…`；旧行为无可读提交人时保持 `答案：…`。
- 卡片改写：单题卡与整组卡的已回复行在 `answeredBy` 存在时渲染 `已由 <uid> 回复：<答案文本>`，否则保持 `已回复：…`（含 owner 作答——提交人对全群可见）。

### 模块与接口

- `src/service.ts`：`isAllowed` 成员分支、成员命令提示（常量、节流表、提示函数）、`memberAnswerAllowed` 注入。
- `src/questions.ts`：`answerFromCard` 授权、答案携带 `answeredBy` 的整条链路、`wakeForAnswers` 与卡片文案。
- `src/ask-card.ts`：`AskCardState.answered` 与 `AskCardSetQuestion` 增加 `answeredBy`，已回复行渲染提交人。
- `src/store-schema.ts` / `src/store-records.ts` / `src/store-questions.ts` / `src/store.ts`：`answered_by` 列、记录字段与两个作答 API 的签名。

## 错误与边界情况

- 成员在未开通群/子区 @Bot：静默拒绝（不触发 owner 通知、不触发口令开通、零 mention_pref 查询）。
- 成员未 @Bot 的群消息：静默拒绝，零副作用。
- 成员命令在节流窗口内重复：只消费 claim；同一消息 ID 重投同样不重复提示。
- 成员点击时聊天已不是已开通（问题行仍在）：`ignored`，无副作用。
- 成员点击已被作答的问题（重放/竞态落败）：静默 ack，不重绘（沿用 owner 规则）。
- 同一问题的文本与点击并发：compare-and-set 只落一份；落败方按既有语义（静默/无效提示）处理。
- 旧库升级后已作答的行 `answered_by` 为 NULL：卡片与唤醒文本回退为不含提交人的旧文案；新作答一律带提交人。
- 成员消息的附件超限、语音失败、unsupported 类型、任务启动失败：与 owner 相同的有界错误路径，不新增分支。
- 成员点击的卡片问题行与聊天不匹配（伪造/跨聊天）：`listQuestionsByCard` 按目标过滤，返回空即 `ignored`。
- 提交人 uid 超长或被伪造：uid 来自事件帧与服务端校验，channel 侧按有界文本渲染（卡片文案随既有卡片构造有界）。

## 验收标准

1. 已开通群内成员（带结构化 @Bot）的文本消息启动任务：任务正文、回执、失败分类、steering/排队、历史注入与 owner 同路径；成员可发送全部当前支持的内容类型（含附件与语音转写）。
2. 成员不带 @Bot 的群消息、私聊非 owner、未开通群/子区的成员消息：全部无副作用（无任务、无 `inbound` 行、无待发消息、零 mention_pref 查询）。
3. 成员发 `/` 开头消息（已注册与未知命令）：不执行、不建任务、不交给 agent；同一聊天 10 分钟内至多一条提示；owner 命令行为不回归（含未知 `/xxx` 照旧按任务透传）。
4. 已开通群/子区中，非 owner 成员点击 ask 卡片按钮或提交自由输入：答案按既有校验记录、卡片改写为已回复并显示提交人、等待中的 ask 立即返回；ask 工具返回值中该答案带 `answeredBy=<成员 uid>`。
5. 私聊非 owner 点击与未开通聊天的点击：仍被忽略且无副作用（不落答案、不重绘、不唤醒）。
6. 文本作答与点击并发只落一份答案；重复点击与重投静默；有 pending 问题时成员或 owner 的普通文本同样作答，提交人记录为发送者；等待超时后的答复唤醒文本包含「由 <uid> 回复」。
7. owner 行为不回归：私聊/群消息、全部命令、免@、卡片作答与卡片改写（现在带提交人）全部正常。
8. `bun run typecheck` 与 `bun test` 全绿；真机验收：被放行的非 owner 成员在真实群中点击卡片提交成功（答案落盘带其 uid、卡片改写、任务继续），证据随完成回报给出。

## 未决事项

无。
