---
name: omp-channel-octo-add-bot-api-capability
description: 在 omp-channel-octo 里接入新的 Octo 能力时使用：先判断该能力是否已可经注入的 octo-cli 身份访问（2026-09-16 起每个 bot 有独立 CLI 凭据），再决定是否做成 Host tool；覆盖 contracts→transport→service 工具→测试 fixture→spec 的固定顺序、目标绑定与授权边界、有界输出规则、以及不打断常驻服务的真机验证流程
---

用户说「接入这些 tool」「把更多 Octo 能力接进来」时的固定流程。先读 `docs/specs/2026-09-12-octo-channel-design.md`（唯一规格，编号会随实现增长）：host tools 在「群说明、Host tools 与问答」，octo-cli 身份在验收 26/27 附近，改完必须同步它。

## 0. 先判断：要不要做成 host tool

2026-09-16 起，channel 在注册 Bot 后会给每个 bot 准备一份 octo-cli 凭据，并把 `OCTO_CONFIG_DIR`／`OCTO_API_BASE_URL`／`OCTO_BOT_ID` 注入 OMP worker（`src/octo-cli.ts`）。因此任务里的 agent 本来就能用 `octo-cli` 以本 bot 身份打任意 Octo Bot API 端点——包括没有 host tool 包装的那些。

先把候选按下面两条分流：

- **不需要 host tool**：纯读取便利、通用 API 调用、上游 CLI 已覆盖的按需操作。直接告诉用户在任务里用 `octo-cli`（`octo-cli schema --list` / `octo-cli <domain> --help` 自发现；`octo-cli skills` 有内嵌文档）。加一个薄包装只会重复上游、又多一份要维护的 parser。
- **需要 host tool**：必须锁定「当前聊天」而不能让模型指别的目标的写操作；需要 channel 自身状态的动作（worktree/派发深度/session、共享说明的 version 语义）；必须固定裁剪与错误压缩的输出；必须在群/线程 chat 里按 `allowedGroupIds` 门禁后才出现的能力。这些用 host tool 表达，`octo-cli` 给不了。

判断依据是「谁能约束目标与输出」，不是「哪个更好写」。目标锁定只对 host tool 成立：经 octo-cli 的调用由服务端按 bot 身份授权，模型可以指向该 bot 所在的任何群/线程。

### octo-cli 身份的关键事实（实测）

- `OCTO_CONFIG_DIR` 才是隔离开关：指向一个只有本 bot profile 的目录时，`OCTO_BOT_TOKEN` 才会生效（`bot_token_source=env:OCTO_BOT_TOKEN`）。本机默认 `~/.octo-cli` 里可能已有一份别的 bot profile，**有 profile 时 env token 被忽略**，直接注入 `OCTO_BOT_TOKEN` 是无效的。
- 生产口径：`echo "$TOKEN" | octo-cli auth login --profile oco --bot-id <robot_id> --with-token`，目录 `~/.oco/octo-cli/<botUid>`（0700），token 只走 stdin；worker 只收 `OCTO_CONFIG_DIR`/`OCTO_API_BASE_URL`/`OCTO_BOT_ID`，`workerEnv` 里的 allowlist 会丢掉其他 `OCTO_*`（含 token）。
- `--verbose` 只打 method+URL+status，不打 Authorization；凭据库密钥是机器绑定（IOPlatformUUID/hostname + salt 经 scrypt，需 `ioreg`，所以 PATH 必须含 `/usr/sbin`，否则报 `profile: read machine id: ioreg: exec ...`）。同用户进程可拷目录解密，所以它不是防同用户进程的保险箱。
- 选错身份的危险是静默的：没被隔离时 octo-cli 会以默认 profile 的 bot 行事。工具/文档里都要让 agent 先 `octo-cli auth status` 自证身份再用。

## 1. 先定契约（不要先写代码）

- **一个复合 tool 优于 N 个单动作 tool**：`octo_management` 用 `action` 枚举分派，schema 里只放 `action`/`short_id`/`name`/`notice`/`content` 这类通用字段。
- **目标固定为当前聊天**：群任务取当前 `group_no`，线程（channelType=5）取父群 `group_no`（`threadParentGroupId(target.channelId)`），`thread-*` 省略 `short_id` 时取 `target.channelId.slice(parent.length + 4)`。绝不接受模型传入的 chat/target 参数——仓库已有「工具不能改写目标聊天或 cwd」的承诺。
- **授权交服务端**：`group-update` 需 `bot_admin`、`thread-delete` 需线程创建者/群管理员；服务端拒绝就报错，不在客户端放行，也不做额外提权。跨目标动作（createGroup、add/remove-members、space/members、send-message）默认不接。
- **凭据类能力单独决策**：入站 webhook 的 create/regenerate 返回可往群里投递的推送 URL+token。要么不做，要么把凭据写到 stateDir 私有文件、工具只回路径；不要默认把它塞进群消息或模型上下文。

## 2. 改动顺序（缺一个就编译/测试不过）

1. `src/contracts.ts`：加结果类型（`GroupInfo`/`GroupInfoPatch`/`GroupMember`/`ThreadSummary` 风格）与 `ChannelTransport` 方法。**加接口方法会让每个实现类都报错**，所以紧接着第 2、3 步，否则 `bun run typecheck` 会列出所有用 FakeTransport 的测试文件。
2. `src/octo/index.ts`：一个 HTTP op 一个方法，走 `botApiRequest(...)`/`requestWithTimeout(...)` + `apiUrl`/`authHeaders`；响应必须逐字段校验（`isJsonRecord`/`requiredString`），新增 parser 放在 `parseVersion` 附近。资源 id 用 `managementId()` 挡空/超长/含 `/`。错误映射把服务端 `{error:{message},msg}` 压成单行截断 200 字符（`Octo <label> failed with HTTP <status>: <message>`），`400/401/403/404` 记 `permanent`。
3. `src/service.ts`：`createHostTools` 里按 `isGroupTarget`/`parentGroupId !== null` 决定是否挂载；动作分派写成一个 `runManagementTool(...)` 私有方法，参数校验在分派前统一做（`action` 白名单、`optionalText`/`namedText`/`threadId` 闭包）。输出按上限裁剪：成员 200、线程 50，附 `count`/`truncated`，名称 200、公告 2000 字符；不返回原始 HTTP 响应、凭据或工具原始 args。常量集中放在 `DEFAULT_*` 常量区。
4. `tests/fixtures/core-fakes.ts`：`FakeTransport` 必须实现新方法，并给测试用的 setter/失败钩子（`setGroupInfo`/`setGroupMembers`/`setThreads`/`failManagementWith`）与调用记录（`managementCalls`）。用私有 `recordManagement(action, ...)` 统一记录+abort+failure 检查。
5. `tests/fixtures/octo-protocol-server.ts`：在 `handleHttpRequest` 里加路由解析（`managementRouteFor(method, pathname)`）+ `respondManagement(route, body, response)`，并用 `managementStatuses`/`managementBodies`（键为 `${METHOD} ${pathname}`，pathname 是**已编码**的 URL path）供测试注入错误/畸形响应。**新的路由正则不要和既有 `/md` 路由撞车**。
6. `tests/octo-transport.test.ts`：真 HTTP 集成测试，断言完整 `method + path` 序列、请求体、`Authorization`，以及错误/畸形响应路径。
7. `tests/core-host-tools.test.ts`：工具名清单（群 `ask,octo_management,octo_send_file,channel_control`；私聊 `ask,octo_send_file,channel_control`）、线程 chat 里的父群/当前线程解析、参数校验与截断、传输失败透出。
8. spec：新增工具契约段（动作集、目标绑定、服务端授权、返回边界）+ 同步验收条目；全仓库 `grep -rn 'octo_group_read\|octo_group_write'` 必须为空（重命名要走干净切换，不留别名）。

## 3. 真机接口先摸形状（再写 parser）

```sh
T=$OCTO_BOT_TOKEN; B=$OCTO_API_URL; G=<group_no>
curl -s "$B/v1/bot/groups/$G" -H "Authorization: Bearer $T"          # name/notice/creator/status/created_at
curl -s "$B/v1/bot/groups/$G/members" -H "Authorization: Bearer $T"  # uid/name/role/robot
curl -s "$B/v1/bot/groups/$G/threads" -H "Authorization: Bearer $T"
```

已知实况（2026-09-16）：失败统一返回 HTTP 400 + `{error:{code,message},msg,status}`；`leave-thread` 对创建者自己的线程可能返回 500 `服务器内部错误。`；`create-thread` 返回的 `created_at/updated_at` 是 `0001-01-01 00:00:00`（首条消息后才有值）；线程 channel id = `{group_no}____{short_id}`（4 个下划线）；非群成员访问群信息 → `机器人不是该群成员。`。
只读探测是安全的；**不要**为摸形状去 `register`（会开第二设备会话）。

## 4. 验证顺序

```sh
bun run typecheck && bun test          # 460+ pass；fixture 未实现新方法时先在这里爆
```

真机验证：直接构造 transport 调新方法，**不要 initialize()**（会 register → 开第二设备会话 → 可能踢掉常驻进程 WS，reason != 0 时 transport 永久停摆而进程仍 Healthy）：

```ts
const t = createOctoTransport({ apiUrl, botToken });   // 不 await t.initialize()
await t.getGroupInfo(GROUP); await t.createThread(GROUP, "probe");
// …探测完 deleteThread 清理，群里不留残留
```

涉及 octo-cli 身份时，用干净环境自证绑定（把 token 从 env 里去掉）：

```sh
env -i PATH="$HOME/.bun/bin:$HOME/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin" HOME=$HOME \
  OCTO_CONFIG_DIR=$HOME/.oco/octo-cli/<botUid> OCTO_API_BASE_URL=https://im.deepminer.com.cn OCTO_BOT_ID=<botUid> \
  octo-cli group get <group_no>        # identity.source 必须是 profile:<本 bot> 的 profile
```

## 5. 部署与端到端触发

常驻服务的运行目录是**主检出**（`~/.super/conf/conf.d/omp-channel-octo.toml` 的 cwd），不是线程的 worktree：在 worktree 里改完必须让改动进入主检出才能上线。主检出可能被别的线程同时编辑，先 `git -C <主检出> status --short` 确认干净、再合并（不干净时 `merge --ff-only` 会因重叠文件被拒；不要替别人提交）。随后用 `channel_control restart`（channel 自己检测到的守护进程）重启，确认 Healthy。

群/线程里 owner 的 @mention 才会触发任务：`allowedGroupIds` 必须含该 `group_no`（线程则含完整 `{group_no}____{short}`，因为门禁比较的是 `message.channelId`），否则消息在任何持久化之前被**静默丢弃**、`inbound` 表无行、日志无痕。worker 环境是否真的拿到注入变量，只能在新一轮任务里验：让 agent 跑 `octo-cli auth status` 并回报 `identity.robot_id`。本地用 `tests/octo-cli.test.ts`、`tests/omp-worker.test.ts`（真实子进程读 env）和 `tests/core-boundaries.test.ts` 覆盖同等语义。
