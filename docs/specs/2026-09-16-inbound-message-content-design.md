# 入站消息类型与附件

日期：2026-09-16

## 背景与目标

现在传输层只把 `payload.type === 1` 当作「支持」，其余类型一律回复「不支持的消息类型，不会启动任务。」结果是在群里 @Bot 发一张截图、一份 PDF、一条语音、一次富文本或一次聊天记录转发，都无法驱动 Agent：图片被当成不支持，语音和视频甚至没有落点。

目标：把**内容可用**的常见消息变成可执行的任务输入——文本类摊平成提示词文本，媒体类下载到本机并在提示词里给出绝对路径，语音额外通过服务端 ASR 得到文字。真正没有内容的噪音类型（贴纸、系统/CMD）保持现状：明确告知、不作为成功执行。

## 范围

### 包含

- 入站类型表（1 文本、2 图片、3 GIF、4 语音、5 视频、6 位置、7 名片、8 文件、9 文件别名、11 合并转发、14 富文本、17 交互卡片）的文本与附件提取。
- 回复（引用）消息：客户端内联在 `payload.reply.payload` 里的被引用 payload 同样过一遍类型表——文本成为引用块，媒体成为本条消息的附件。
- `InboundMessage` 携带 `attachments`；`supported` 语义从「是文本消息」扩展为「能形成可用输入」。
- 媒体附件（图片/GIF/语音/视频/文件/富文本内嵌图片）下载到状态目录，绝对路径写进任务正文。
- 语音消息的服务端转写（best-effort）。
- 新配置项 `attachments`：`enabled`（默认 `true`）与 `maxBytes`（默认 `26214400`，即 25 MiB）。
- 附件目录的保留策略：服务启动时清理 14 天前的目录。
- 群/线程历史：附件消息的**可提取文本**进入历史正文。

### 不包含

- 贴纸（12/13）、加入组织（16）、截屏（20）、错误/CMD（97/98/99）、系统类（1000+/2000+）、RTC（99xx）的记录：仍回复「不支持的消息类型，不会启动任务。」
- 解析 type-17 卡片树：只取服务端权威 `plain`。
- 引用链：只读一层，被引用消息自身再引用别的消息时不再展开。
- 按 ID 回查被引用消息：客户端已经把整条 payload 内联在 `reply` 里，不额外请求服务端。
- 下载合并转发内嵌消息的附件：只摊平文本，内层媒体渲染为占位符。
- 图片缩放/转码、OCR、音视频理解；转写只调用部署已有的 ASR 服务，不在本机跑模型。
- 出站能力变化：host tool 的图片/文件发送保持现状。
- 附件进入 OMP session 的本地副本、artifacts 归档或历史附件回填。

## 已知约束

- 回复消息的真实形状（线上实测：`现在能看到这个图片了吗` 回复 seq 12）：`payload.reply = { root_mid, message_id, message_seq, from_uid, from_name, payload }`，其中 `reply.payload` 是被引用消息的**完整** payload（同一套 `type` / `content` / `plain` / `mention` 字段，实测为 `type 14` 富文本），因此类型表可直接复用，不需要第二个解析器。回复方的 `type` 与引用无关（实测为 `type 1`），`reply` 由服务端在 WS 与 `messages/sync` 两条来源上一并投递。
- Octo 类型枚举见 octo-lib `common/msg.go`：1 Text、2 Image、3 GIF、4 Voice、5 Video、6 Location、7 Card、8 File、11 MultipleForward、12 VectorSticker、13 EmojiSticker、14 RichText、16、17 InteractiveCard、97/98/99、1000+、2000、9989+。
- 各类型字段（跨仓取证：octo-web `packages/dmworkbase/src/Messages/*` 的 `decodeJSON`、octo-server `modules/bot_api/obo_fanout_content_type_test.go` 的样本 payload、octo-lib `common/richtext.go`、参考适配器 `openclaw-channel-octo/src/inbound.ts` 的 `resolveContent`）：
  - 2 图片：`url`、`caption`、`name`、`size`、`width`、`height`（`size` 由线上真实图片消息的 payload 实测确认，用于下载前的超限预检）。
  - 3 GIF：`url`、`width`、`height`。
  - 4 语音：`url`；时长在不同端为 `second` / `timeTrad` / `duration`。
  - 5 视频：`url`、`cover`、`size`、`width`、`height`、`second`。
  - 8 文件：`url`、`name`、`extension`、`size`、`caption`。
  - 11 合并转发：`users[{uid,name}]` 与 `msgs[{from_uid,timestamp,payload}]`；服务端不对深度、条数或大小设闸门，内层 payload 可递归。
  - 14 富文本：`content` 为 block 数组（**写入端 write-strict，合法块只有 `text` 与 `image`**：text 用 `text`，image 用 `url`+`width`+`height`，`size`/`name` 可选；未知块类型被写入端拒绝，解析端按宽容原则「有 `text` 取 `text`、否则忽略」，与 octo-lib `BuildRichTextPlain` 的 default 分支一致），顶层 `plain` 为服务端生成的权威纯文本。
  - 17 交互卡片：`plain`（服务端保证非空，空 body 时由服务端回退 `[卡片]`）；实现同样在 `plain` 缺失/为空时回退 `[卡片]`（与参考适配器的 `CARD_PLACEHOLDER`、octo-server Decision 8 一致），避免把空正文交给 Agent。
  - 6 位置：`latitude|lat`、`longitude|lng|lon`、`address`、`title`；7 名片：`name`、`uid`。
  - 客户端解码器对这些类型各有一份字段表，逐条比对后与本设计一致：3 GIF 只有 `url`(+宽高)；5 视频 `url`/`cover`/`size`/`width`/`height`/`second`；6 位置用 `lat`/`lng`(+`title`/`address`)；7 名片 `name`/`uid`；8 文件除 `url` 外还有 `extension`，客户端明确「后缀优先取文件名，服务端 `extension` 不可靠」。客户端对图片/文件额外容忍 `url || remoteUrl`，但 `remoteUrl` 是它自己的本地序列化字段（落库前会被写回 `url`），线上 payload 实测只有 `url`，因此这里只读 `url`。
- 9 不是 octo-lib 的枚举值，但 octo-server 用例明确记录部分客户端把文件发成 9（`modules/bot_api/obo_fanout_content_type_test.go` 的 `file_type_9_e2e_report_value` 子用例注释），因此按文件类型处理。
- 附件 URL 是对象存储的公开直链：octo-server `modules/file/api.go` 的 `Service.DownloadURL` 是纯字符串拼接（MinIO 后端注释明确 bucket 默认公共读），下载不需要鉴权头、不带时效；需要登录的预签名 URL 只出现在另外两个端点且 30 分钟过期。`DownloadURL` 失败时服务端会回退为相对路径 `file/preview/<...>`，而该形状对应的是**需要登录态**的 `/v1/file/preview/*path`（`modules/file/api.go` 的路由注册），Bot 没有该登录态，因此相对 URL 一律视为不可用，不猜路由、不剥离段、不做相对解析。
- Bot 侧接收不按内容类型过滤：octo-server `modules/bot_api/obo_fanout.go` 的 CONTENT-TYPE CONTRACT 注释与 `TestFanout_ContentTypeAgnostic` 锁定了该契约；WS 载荷端到端加密，服务端也无法按类型筛掉。
- 语音转写由独立 ASR 微服务提供，octo-server 的 `POST /v1/bot/voice/transcribe` 只做 multipart 转发，不落库、不写回消息 payload。线上实测该端点返回 200（`engine=qw`）；文件字段名固定为 `audio`，可选 `mode`（`edit` / `append`）会被转发。
- OMP RPC 单帧物理上限为 1 MiB，prompt 的原生图片附件走 base64 inline，因此内联只适合约 700 KB 以下的原图；`read` 工具对本地图片在 vision 模型下直接内联解码，绝对路径不受 cwd / session root 限制，且属于自动批准的 read tier。
- `CoreService.isAllowed`（owner + 允许群/线程 + 结构化 mention）在任务接受前执行，是附件下载的天然前置门禁。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 判定口径 | 用类型表与可提取内容判定「支持」，`supported` 语义扩展为「能形成可用输入」 | 「不支持」只留给确实没有内容的类型；加类型不等于放宽权限 |
| 空文本的文本类消息 | 文本类（1/14/17）恒支持：`@Bot` 单独出现时正文为空，回合照常运行 | 只 @ 一下相当于「叫一声」，让 Agent 自己回应要做什么，比回一句「不支持的消息类型」合理；类型 1 的既有行为因此保持不变 |
| 文本来源 | 各类型的原生文本字段：`content`、`caption`、`plain`、blocks 摊平、转发展开、位置/名片一行 | 与参考适配器和服务端 `plain` 生成口径一致，不发明第二套语义 |
| 附件交付 | 下载到状态目录，提示词给出绝对路径 | 统一覆盖所有文件类型；绕开 OMP RPC 1 MiB 帧上限造成的图片大小悬崖；路径在整个 session 内稳定 |
| 下载时机 | 唯一组装点：权限门与待回答问题判定之后、steer/任务派发之前；凡是把正文交给 Agent 的命令路径（`/thread <任务>`、`/queue <文本>`、注册 handler）都经这里组装，不消费正文的控制命令与答案路径不取资源 | 未授权消息不触发外网请求；只有一个组装点才能保证组装正文同时进入 steering 与任务，且不会出现"下载了却没人读" |
| 附件 URL | 只接受绝对 http/https；相对 URL 视为不可用 | 服务端的相对回退指向需登录态的路由，Bot 取不到；不猜路由、不做相对解析，避免把错误地址当成功 |
| 附件拒绝通知 | 复用既有命令 claim + outbox 通道，只回复一次，且不写 task 行 | 与「不支持」路径同一套去重：重连重投不会重复下载、重复回复 |
| 失败清理 | 只删除本次尝试新建的文件与临时文件，目录空了才移除 | 同一 messageId 重投时不得删掉已被接受任务引用的附件 |
| 语音 | 下载后调用 `POST /v1/bot/voice/transcribe`，失败则保留音频路径 | 没有转写文本的语音对 Agent 等于不可用；转写是 best-effort，不阻断任务 |
| 幂等 | 目标文件已存在且非空即复用；写入走临时文件 + rename | 重投、重放、steer 回退不会重复下载，也不会留下半截文件 |
| 提示词形态 | 唯一组装点产出「用户文本 + 转写块 + 附件块」，同一份文本传给 steering 与任务接受 | steering 不建任务、不经过 `tasks.text`，必须显式携带同一份组装文本；接受的正文仍落在既有 `tasks.text`，排队与群历史自动一致 |
| 大小上限 | 单一配置 `attachments.maxBytes`，默认 25 MiB，超限即拒绝任务 | 一次大文件转发不应打满磁盘或长时间占用连接 |
| 保留 | 启动时清理 14 天前的附件目录，best-effort | 无上限增长不可接受；清理失败不影响启动 |
| 群历史 | 只把可提取文本写进历史正文，附件本身不注入 | 历史只承载文本，避免历史注入触发成批下载 |
| 引用消息 | 用同一张类型表读 `reply.payload`，媒体并入本条消息的附件（按绝对 URL 去重），文本渲染成 `[引用 <发送者>]` 块 | 客户端已内联整条 payload，回查服务端既慢又要多一条权限面；一条回复仍是一次任务，引用只是上下文 |

## 设计

### 数据契约

- `InboundMessage` 增加 `attachments: InboundAttachment[]`；`InboundAttachment = { url, kind: "image" | "file" | "audio" | "video", name, size, durationSeconds }`。`size` / `durationSeconds` 未知时为 `0`；`url` 一律是绝对 http/https，相对路径与其他不可用 URL 不产生附件项。`size` 是客户端声明值，只用于下载前的超限判定；提示词里展示的大小取实际落盘字节数。
- `InboundMessage` 增加 `quoted: InboundQuote | null`；`InboundQuote = { senderName, text }`，`text` 是被引用消息的可提取文本，按 code point 截断到 2000。被引用媒体不放进 `quoted`，而是并入 `attachments`（本条消息自己的在前、按绝对 URL 去重），因此下载、大小上限、失败清理、重投复用与提示词附件块都只有一套逻辑。
- `supported`：文本类（1 文本、14 富文本、17 卡片）恒为 `true`——它们的内容就是文本，空文本（例如只 @Bot 没有别的话）仍是一次合法的普通回合，交给 Agent 自然回应；其余类型为 `text.trim().length > 0 || attachments.length > 0`，即必须是媒体真的可用（提取到文本或附件）才算支持；类型表之外的类型恒为 `false`。`text` 是该类型的可提取文本（mention 仍在传输层按既有规则剥离）。
- `ChannelTransport` 增加：
  - `downloadAttachment({ url, path, maxBytes, signal }): Promise<number>`：落盘并返回字节数。
  - `transcribeAudio(path, signal): Promise<string | null>`：`null` 表示无转写可用。
- `CoreServiceConfig` 增加 `attachments: { enabled: boolean; maxBytes: number }`；`src/runtime/config.ts` 严格校验（内部未知字段拒绝；`maxBytes` 为 1 KiB..1 GiB 的整数；缺省 `{ enabled: true, maxBytes: 26214400 }`；`enabled: false` 时 `maxBytes` 仍按默认解析）。
- 群/线程历史的实时缓冲与 sync 补洞必须使用**同一个**类型提取函数，只取其中的文本部分；否则同一张图在两条来源里的口径会不一致。回填路径（`POST /v1/bot/messages/sync`）不产出附件、不做 URL 判定，只按该函数取文本；mention 处理沿用各路径现状，本设计不改。
- **唯一组装点**：`handleInbound` 在权限门、命令解析、待回答问题判定之后，在 steering 与任务派发之前执行一次「下载 + 转写 + 组装」，产出组装正文。该正文显式传给两个出口：`appendToRunningTask`（成功则为 steering，不建任务，但注入的就是这份正文）与 `acceptTask`（成为 `tasks.text`）。除这两个出口外没有其它消费方：
  - 命令是否取资源取决于它是否消费正文：`/thread <任务>` 与 `/queue <文本>` 的正文是命令参数，注册 handler 的正文是整条消息——这些路径先把命令 claim 释放（或本来就还没有 claim），再把各自的正文交给同一条组装点，附件（图片/文件/语音/视频）与引用块照常进入组装正文；组装失败（附件被拒）时不接受任务、不调用 handler。拦截型 handler 拿到的是组装后的正文（`message.text` 即组装文本），观察型 handler 接受的任务正文与它看到的正文是同一份。
  - 不消费正文的控制命令（`/status`、`/help`、`/new`、`/move`、`/mode`、`/cancel`、`/stop`、`/interrupt`、`/restart`）**不处理附件**：不下载、不落盘。它们没有把资源交给 Agent 的出口，取回来只会是无人阅读的落盘文件，且会让 `/cancel`、`/stop` 这类命令被下载预算拖住。
  - 有待回答问题且该文本构成答案时，消息在答案路径被消费，附件不下载、不落盘。
  - 因此不存在"下载了但没有任何消费者"的分支。
- 不新增 SQLite 列：附件清单随任务正文一起持久化在既有 `tasks.text`。

### 类型表

| type | 文本 | 附件 |
| --- | --- | --- |
| 1 文本 | `content` | — |
| 2 图片 | `caption`（缺失时回退 `content`） | `url` → `image`，带 `name` / `size` |
| 3 GIF | — | `url` → `image` |
| 4 语音 | 服务端转写结果（见「附件管道」） | `url` → `audio`（时长取 `second` / `timeTrad` / `duration` 中第一个有效值） |
| 5 视频 | — | `url` → `video`（时长同上） |
| 6 位置 | `[位置信息: <lat>,<lng>]`（有 `address` / `title` 时附加） | — |
| 7 名片 | `[名片: <name> (<uid>)]` | — |
| 8 文件 | `caption`（缺失时回退 `content`） | `url` → `file`，带 `name` / `size` / `extension`（`name` 缺后缀时按 `extension` 补回，存盘名与提示词都用补全后的名字） |
| 9 文件别名 | 同 8 | 同 8 |
| 11 合并转发 | `[合并转发]` + `users`/`msgs` 展开（深度 1） | — |
| 14 富文本 | 顶层 `plain` 优先，否则 blocks 摊平（image 块注入 `[图片]`，未知块只取 `text`） | 仅 image block 的 `url` → `image` |
| 17 交互卡片 | `plain`（缺失时回退 `[卡片]`） | — |
| 其他 | — | — |

「其他」保持既有行为：回复「不支持的消息类型，不会启动任务。」，不写 inbound / task 记录，不产生任何外网请求。

回复（引用）：`payload.reply.payload` 走同一张表，取到的文本成为引用块的正文、取到的媒体并入本条消息的附件。`supported` 仍只由本条消息自己的 payload 决定——引用内容随之一起进入任务，但不会把贴纸之类的本条类型变成支持。被引用消息自身还有 `reply` 时不再展开。

### 附件管道

- 目录：`<stateDir>/attachments/<channelType>-<channelId>/<messageId>/`，目录权限 `0700`，文件名 `<index>-<safeName>`，文件权限 `0600`。
- `safeName` 取 payload 的 `name`，去掉路径分隔符与控制字符、按 code point 截断到 80 字符；缺失或清洗后为空时回退为 `<kind>-<index>` 加由扩展名或 URL 推断的后缀；同一消息内以 `index` 前缀保证唯一。
- 顺序：权限门 → 命令与待回答问题判定 → 逐条下载 → 语音转写 → 组装正文 → steering 或任务接受。绑定目录在 `acceptTask` 内解析与快照，所以下载发生在绑定解析之前：组装点必须早于 steering（steering 不解析绑定，且必须拿到同一份组装正文）。取舍是——「绑定目录解析失败」这一类极少数情况会留下本次已下载的文件；它们位于该消息自己的目录内，重投直接复用，最迟由 14 天清理回收，不会成为无主副本。
- 失败按「失败模式」表回复后返回，不进入 steering 与任务接受。
- 单条消息的附件数量上限为 8，整条消息的下载总预算为 120 秒（含转写）：超过任一限制即按超限失败处理。这条限制的存在意义是入站 handler 串行执行且 RECVACK 在 handler 返回后才发送，不能让它无限期挂住这条 WebSocket。
- 下载：只接受绝对 http/https；明文 http 允许（URL 由 Octo 客户端产生，部分部署的存储地址没有 TLS）；跟随服务端重定向（重定向后仍然强制大小上限与超时）；60 秒超时；已知 `size > maxBytes` 时直接判超限、不发起请求；否则先做 `Content-Length` 预检，再流式累计，累计超过 `maxBytes` 立即中止；先写 `.part-<random>` 临时文件，成功后 rename 到最终路径；目标已存在且字节数大于 0 时直接复用（跳过下载）。
- 失败清理：只删除本次尝试新建的文件与本次的临时文件；目录为空时才移除目录。已经被接受任务引用的文件永不因后续失败被删除。
- 转写：对 `kind === "audio"` 的附件调用 `POST /v1/bot/voice/transcribe`，multipart 字段 `mode=edit` 与 `audio`（文件名保留原后缀，Content-Type 按后缀映射，未知用 `application/octet-stream`），60 秒超时；非 2xx、响应不可解析或文本为空白一律视为「无转写」。超过 20 MiB 的音频不送转写（真实语音远小于此，避免把大文件读进内存）。

### 提示词形态

任务正文 = 用户文本 +（可选）引用块 +（可选）转写块 +（可选）附件块，块之间空行分隔：

```
<用户文本>

[引用 董振兴]
@O-小爱 [图片]这个开关要打开

[语音转写] 帮我看下这个页面的报错

附件（已保存到本机；需要时用 read 打开）：
1. image image.png 1.2 MB → /Users/you/.local/state/omp-channel-octo/attachments/2-<channelId>/<messageId>/0-image.png
2. audio 语音.amr 8.0 KB (3s) → /Users/you/.local/state/omp-channel-octo/attachments/2-<channelId>/<messageId>/1-语音.amr
```

- 只有用户文本、且本条消息不是回复时，提示词与今天完全一致（逐字返回，不 trim）。
- 引用块只在被引用消息有可提取文本时出现，标题为 `[引用 <发送者>]`，发送者名缺失时退回 `[引用]`；被引用文本原样保留（其中的历史 mention 不剥离，那是被引用者说过的话）。被引用媒体以普通附件行出现，路径落在**回复消息**自己的附件目录里。
- 附件块只在有附件时出现；转写块只在拿到非空转写时出现。
- 附件块里的大小取实际落盘字节数，时长取 payload 声明的秒数（未知则不打印时长）。
- 被接受的正文落在既有 `tasks.text`，因此排队任务与群历史拼接天然一致；steering 不建任务，直接注入同一份组装正文（含附件块），因此运行中的回合同样拿得到附件。
- 附件路径属于提示词内容，不进入群聊正文、进度卡或任何日志。

### 失败模式

| 情况 | 行为 |
| --- | --- |
| 附件下载失败（网络错误、非 2xx、超时） | 走既有命令 claim + outbox 回复「附件下载失败，未接收任务。」（只回复一次，重投不重复回复），不写 task 行；只清理本次新建的文件 |
| 任一附件超过 `maxBytes` | 同样通道回复「附件超过 <N> MB 上限，未接收任务。」；不写 task 行 |
| 附件数量超过 8 或总预算超过 120 秒 | 同样通道回复「附件过多或下载超时，未接收任务。」；不写 task 行 |
| URL 不可用（相对路径、非 http/https、为空） | 该附件不计入；若因此没有文本也没有其他附件，按「其他类型」处理（回复不支持） |
| 被引用消息的文本与媒体都不可用（相对 URL、类型不支持、`reply` 缺 payload） | 该引用不成立：不加引用块、不产生附件，本条消息按自身内容照常处理 |
| 被引用媒体超过 `maxBytes` 或下载失败 | 与自身附件同一条路径：按对应文案拒绝整条任务，引用块也随之不投递 |
| 只有 mention、没有内容（`@Bot` 单独出现） | 文本类消息照常是一次普通回合：正文为空，Agent 正常回应（例如询问要做什么）；不回复「不支持的消息类型」 |
| 语音转写失败或返回空文本 | 任务照常接受，提示词只含音频路径 |
| `attachments.enabled=false` | 附件一律不计入：有文本则按纯文本任务；没有文本时回复「附件接收已关闭，未接收任务。」 |
| 权限门未通过（非 owner、未 @、非允许群/线程） | 不下载、不落盘、不产生任何外网请求 |
| 未认领状态下收到 attachment-only 私聊 | 保持静默（与今天空文本私聊一致）：不下载、不回复、不认领 owner |
| 同一消息重复投递 | 走既有 inbound claim 去重；若该消息此前已被接受，组装命中同一目录与文件名，直接复用已落盘文件，不重新下载、不重复回复 |
| 命令消息（`/status` 等控制命令、`/new`、注册 handler） | 控制命令不消费正文：不下载、不落盘，命令语义与今天完全一致；`/thread <任务>`、`/queue <文本>` 与注册 handler 的正文交给组装点，附件照常落盘并进入组装正文 |

### 保留与清理

- 服务启动时扫描 `<stateDir>/attachments/*/*`，删除最后修改时间早于 14 天的消息目录；单个目录失败只记日志，不影响启动与其他目录。该规则同时覆盖崩溃遗留的 `.part-<random>` 临时文件（它们只在消息目录内）。
- 附件不参与 outbox、恢复或重放语义：它们只是提示词里引用的普通文件。
- 保留期是「session 可能重新读取同一路径」的隐含假设：超过 14 天的会话若再要求 Agent 打开旧附件，会得到不存在的路径；这是可接受的代价，换来磁盘不无上限增长。

## 错误与边界情况

- `caption` 与 `content` 同时存在时优先 `caption`。
- 合并转发展开限制为深度 1、最多 30 条内层消息、正文合计最多 4000 个 code point，超出部分以「…（其余 N 条省略）」标注；内层再次出现合并转发时只渲染 `[合并转发]` 占位，不递归。
- 富文本摊平后的文本按 code point 截断到 2000。
- 消息同时有文本与附件时仍是**一条**任务，文本在前、附件块在后；只有附件时组装正文以附件块开头，因此不会向 worker 送空正文。
- 群/线程历史里出现的附件消息只贡献可提取文本（例如图片 caption、富文本摊平文本、合并转发展开、卡片 `plain`），不触发下载、不产生附件块。历史因此比今天多出卡片与转发文本这类第三方文本，仍受既有的条目截断与块大小上限约束。
- 首次 owner 认领条件**不变**：仍是「非空文本 + 未认领 + 私聊 + 发送者非 Bot」。图片/语音私聊不参与认领，也不推动认领；这与「认领只确认、不执行」的既有约定一致。
- 有待回答问题时的带附件消息：文本仍按既有规则作为答案处理，附件不参与回答、不落盘；只有不构成答案的消息才走任务路径。
- 附件路径可能包含非 ASCII 文件名；写入与提示词使用同一字符串，不做额外转码。
- 图片 `url` 指向 404 或非图片内容时不额外校验内容类型：下载成功即交付路径，由 Agent 自行判断内容。
- 引用只读一层：`reply.payload.reply` 不展开，避免自引用或深链放大；引用文本按 code point 截断到 2000。
- 引用媒体与本条消息自己的附件 URL 相同时只保留一条，落盘一次；被引用媒体落在回复消息的目录里，与原始消息的副本互不影响（同 URL 再被回复会被复用规则挡住重复下载）。

## 验收标准

1. 群里 @Bot 发一张没有文字的截图：产生**一条**任务，提示词含该图片的绝对路径，落盘字节数与远端一致，Agent 能描述图中内容。
2. 发文字 + 图片：仍是一条任务，提示词同时含用户文本与附件块。
3. 发 PDF / zip / 视频 / GIF：分别以 `file` / `video` / `image` 落盘，提示词中的 kind、文件名与大小正确。
4. 发一条语音：提示词含 `[语音转写]` 与转写文本，且音频文件同时落盘。
5. 语音转写失败或返回空文本时：任务照常启动，提示词保留音频路径且不含空的转写块。
6. 发一条富文本（图文混排）：提示词含摊平文本（image 块位置为 `[图片]`），内嵌图片作为附件落盘。
7. 发一次聊天记录合并转发：提示词含 `[合并转发]` 与按发送者展开的文本，内层媒体只出现占位符，不产生任何下载。
8. 发位置或名片：提示词含一行结构化文本，不产生附件。
9. 发一张交互卡片：提示词为服务端 `plain`。
10. 发贴纸：仍回复「不支持的消息类型，不会启动任务。」，不建任务（不写 `task` 行）；只为去重落一条 `command` claim，重投不重复回复。
11. 发一个超过 `maxBytes` 的文件：回复超限文案，不建任务，附件目录被清理。
12. 附件 URL 不可达时：回复「附件下载失败，未接收任务。」，不建任务。
13. 非 owner、未 @Bot 或非允许群的成员发图片：不产生任何下载请求（用 fixture 断言零下载调用）。
14. 相对路径 URL 的图片消息不产生下载请求：有文本时按纯文本任务，无文本时回复「不支持的消息类型，不会启动任务。」
15. `attachments.enabled=false` 时：附件消息行为与今天一致（有文本按纯文本任务，没有文本时回复「附件接收已关闭，未接收任务。」），不产生落盘。
16. 启动清理只删除最后修改时间早于 14 天的附件目录，保留期内的目录不受影响。
17. 群历史注入中，带 caption 的图片消息以 caption 文本出现，且不触发附件下载；实时缓冲与 sync 回填两条来源对同一条消息给出相同文本。
18. 运行中的回合收到「caption + 图片」时走 steering：不建任务，注入的正文含 caption 与附件块，图片已落盘。
19. 命令消息带附件时按是否消费正文分流：`/status` 等控制命令零下载、命令语义与今天一致；`/queue <文本>` 与 `/thread <任务>` 各自恰好下载一次，排队任务与交给助手的意图都带附件块与绝对路径；注册 handler（拦截型与观察型）同样拿到组装后的正文。非 owner 的命令消息先被权限门拒绝，零外网请求、不建任务。
20. 同一条已被接受的任务消息被重投：不重新下载、不重复回复、不删除已落盘文件。
21. 无文字的图片私聊在未认领状态下保持静默：不下载、不回复、不认领 owner；认领后同样的图片会正常成为任务。
22. 单条消息附件数量超过 8 或下载总预算超过 120 秒：回复「附件过多或下载超时，未接收任务。」，不建任务。
23. 半失败清理：一条有两个附件的消息在第二个附件失败时，只删除本次新建的文件；此前已被接受的同 messageId 任务引用到的文件仍然存在。
24. 只 @Bot、不带任何内容：不回复「不支持的消息类型，不会启动任务。」，而是照常启动一个正文为空的回合，Agent 正常回应（例如询问要做什么）；贴纸等其他类型仍回复「不支持」。
25. 回复一条带图的消息、自己只写字：产生**一条**任务，提示词含用户文本、`[引用 <发送者>]` 块与被引用图片的附件行；图片落盘在本条消息的目录下，字节数与远端一致。
26. 直接回复一条带图的消息、自己不写字：同样产生一条任务，正文以引用块与附件块开头。
27. 被引用消息只有相对 URL、或类型不在类型表内时：不产生附件、不加引用块，本条消息按自身内容处理；`supported` 仍只由本条 payload 决定。
28. 被引用媒体与自己的附件是同一个 URL 时只落盘一次、提示词里只出现一行；被引用媒体下载失败或超限时按既定文案拒绝整条任务。

## 验收证据

自动化证据按层分布（`bun test` 与 `bun run typecheck` 全绿）：

- 传输层（`src/octo/index.ts` 与其 `src/octo/content.ts` 归一化层，夹具为真实 HTTP/WebSocket 与加密协议的 `tests/octo-protocol-server.ts`）：`tests/octo-content.test.ts` 覆盖全类型抽取（1/2/3/4/5/6/7/8/9/11/14/17）与贴纸仍不支持、相对 URL 被丢弃、合并转发的条数/码点上限与内层嵌套占位、富文本 image 块摊平、引用消息四例（有文字 / 无文字 / 被引用不可下载 / 与本条同 URL 去重）、下载落盘与复用、跟随重定向且重定向目标超限仍拒绝、落盘文件 `0600`、语音转写（成功 / 503 / 空白 / 不可解析 / 超 20 MiB / 本地文件缺失）、群历史 sync 回填。
- 服务层（`tests/core-attachments.test.ts`，确定性假传输与假 worker）：图片与「文字 + 图片」各产生一条任务且提示词含绝对路径与真实字节、`file` 附件行、语音转写块、声明超限与真实字节超限、单条超时 →「附件下载失败」与总预算耗尽 →「附件过多或下载超时」、数量上限 8、`enabled=false`、不支持类型回复固定文案且重投不重复、拒绝路径（非 owner / 未 @ / 非允许群 / 未认领私聊 / 待回答问题）零下载、steering 复用同一份组装正文、组装正文按命令是否消费正文分流（`/status` 与答案路径零下载；`/queue <文本>`、`/thread <任务>` 与注册 handler 的正文都取附件；被权限门拒绝的命令消息零下载）、重投不重复下载、半失败清理、14 天清理、附件目录 `0700`。
- 组装层（`tests/attachments.test.ts`）：提示词版式（用户文本 / `[引用 <发送者>]` / `[语音转写]` / 附件行）、按 code point 截断的落盘名、`formatBytes`、`AttachmentError` 原因、清理失败记日志而目录本就不存在时保持静默。
- 群历史与配置：`tests/core-group-history.test.ts` 断言带 caption 的图片在实时缓冲里只贡献文本且零下载；`tests/octo-transport.test.ts` 断言 sync 回填对 `type: 2` 取 caption、对畸形行逐条跳过；`tests/config-launchd.test.ts` 断言 `attachments` 的缺省、未知字段拒绝、范围校验与 `enabled: false`。

覆盖层次：富文本、合并转发、位置/名片、卡片目前只有传输层证据——它们在服务层只是「文本 + 附件」，服务层端到端由图片、语音、文件三类代表覆盖。引用相关：第 25 条有服务层用例（引用正文与引用附件进入同一条任务），第 26 条有组装层用例（空文本时正文以引用块开头），第 27、28 条目前只有传输层用例。

不在自动化覆盖内的是第 1 条本身：它要求真实 IM 客户端发一次图片（客户端 → 服务端链路），以及末句「Agent 能描述图中内容」这一模型行为。真实对象存储直链下载与真实 `POST /v1/bot/voice/transcribe` 已分别用生产 transport 单独验证过。

## 未决事项

无。`POST /v1/bot/voice/transcribe` 的可用性依赖部署侧 ASR 服务；未配置时该端点报错，按「无转写」降级，不影响其他类型。
