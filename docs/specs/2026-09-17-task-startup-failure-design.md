# 任务启动失败的分类报错与可观测性

日期：2026-09-17

## 背景与目标

群/子区任务在 worker 创建成功之前失败时，用户收到的都是同一句「群说明读取失败，未启动任务。」，任务行也只写「群说明读取失败」。这句文案与实际失败点无关，真实异常在该分支被丢弃（不进日志、不进任务行），因此一次「任务没跑起来」无法从任何持久面定位。

2026-09-17 的真实现场给出两类互不相同的失败：

| 现场 | 时间（本地） | 任务行 | 耗时 | 真实失败点（证据） |
| --- | --- | --- | --- | --- |
| 绑定工作区被删除 | 2026-09-16 22:00:04 / 22:01:13 | `群说明读取失败` | 0.11 / 0.05 秒 | 绑定 cwd 已不存在（该目录现仍缺失），在 `OMP worker cwd does not exist` 处快速失败，没有 worker 进程 |
| OMP 子进程未就绪 | 2026-09-17 11:52:56 | `群说明读取失败` | 12 秒 | 子进程已 spawn（`worker_ledger.identity` = `pid=63279;start=Thu Sep 17 11:52:56 2026`）但在 10 秒 ready 预算内没就绪：其 `~/.omp/logs` 只有第一行启动日志，系统日志显示同一批启动步骤比基线慢 20–35 倍；同一 cwd + 同一 session 文件 + 同一作用域环境在事后复现为 0.56 秒就绪。同期该主机每约 10 秒被 launchd 重新拉起一个必然崩溃的 Bun 进程（`com.agent-team.daemon`，缺 `bullmq`，`runs` 累计 73177），且有一次 spawn 与本 worker 落在同一瞬间（11:52:56.248）；因果关系未能证明，但它是这台机器上持续的瞬时负载来源 |

目标：

1. 启动前失败按类别报错：群说明读取失败 / 绑定不可用 / worker 启动失败永远不再互相冒充。
2. 真实原因以**有界、单行、无秘密**的形式进入服务日志与任务行，使一次失败可从日志和 SQLite 直接读出。
3. 失败的分类与原因码来自类型化错误，而不是对错误文本做字符串匹配。

## 范围

### 包含

- 启动前失败（群说明读取、绑定会话文件、worker 进程创建与就绪）的分类、日志、任务行与聊天文案。
- worker 工厂（`src/omp/index.ts` 的 `create`）与宿主绑定检查抛出的类型化原因码（`src/contracts.ts` 的 `TaskStartupError`）。
- 已存在 worker 之后失败的任务行与日志同样带上真实原因（聊天文案保持现状）。
- 主规格中「共享说明读取失败则该任务失败且不得启动 worker」的承诺与验收保持逐字不变。

### 不包含

- 群说明读取失败时的降级执行（owner 拍板保持既有承诺：失败且不启动 worker）。
- 启动失败的重试（owner 拍板不加，改为提高默认启动预算）。
- 工作区被移除后绑定目录的自动重绑（owner 拍板保持现状：明确报错、绑定不变）。
- 文案的多语言与用户可配置；`/status` 输出字段的任何变化（`/status` 不展示任务行，任务行仍只面向日志与运维查询）。

## 已知约束

- `src/errors.ts` 的既有约定：错误对象本身不进日志与卡片。本设计只放行**有界（≤200 字符）、单行、控制字符清除**的文本，以及固定词表里的原因码；原始错误对象、协议载荷与凭据不出现在任何面。
- 群/子区的对话文案对群成员可见，聊天文案因此不含路径；路径只出现在服务日志与任务行（`tasks.cwd` 本来就存目录，任务行是 owner 侧 SQLite）。
- 聊天文案与任务行都不能因这次改动变得不可 grep：`群说明读取失败` 仍是该类的行内前缀。
- 失败分类必须与「worker 是否已存在」解耦：`workerFactory.create` 可能已经 spawn 子进程后才失败（2026-09-17 现场），此时 `worker` 变量仍未赋值，但失败点已经属于 worker 启动而不是群说明读取。

## 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 原因来源 | 类型化错误 `TaskStartupError`（`code` 取有界词表） | 字符串匹配会被消息改写悄悄打断；原因码可直接进日志、任务行与文案，且无泄密面 |
| 类别划分 | 4 类：`context`（群说明读取）/ `binding`（绑定不可用）/ `worker`（worker 启动失败）/ `generic`（已启动后失败） | 与 owner 提出的验收一致（群说明读取失败 vs worker 启动失败/目录不可用）；每类对应一个可执行动作 |
| 类别判定 | 有原因码时按码归类（`cwd-missing`/`session-invalid` → `binding`，其余 worker 码 → `worker`）；无原因码时按阶段（群说明读取阶段 → `context`，其余启动阶段 → `worker`） | 阶段只用于兜底，主判据是码；`create` 已 spawn 后失败仍归 `worker` |
| 聊天文案 | 固定句式 + 具体原因码，不含路径、不含原始错误；`context` 与 `generic` 沿用逐字不变的两句 | 群可见面的信息量固定且安全；owner 用于定位的两句保持原样 |
| 真实原因去向 | 服务日志一行（`[task] 启动失败 … kind=… code=… phase=… detail=…`，worker 已存在时为 `执行失败`）与任务行 `error_text`（`<类前缀>（<原因码>）：<单行文本>`） | 现场缺的正是这两处：日志空白、任务行只有固定文案；原因码进任务行便于按码检索 |
| 文本有界 | `failureText()` 单行化 + 控制字符清除 + ≤200 字符 | 与既有「卡片/群消息只放有界文案」一致；日志与 SQLite 行不会被无界 dump 污染 |
| 类前缀 | 任务行前缀沿用既有词汇（`群说明读取失败`、`任务执行失败`）并新增 `绑定不可用`、`worker 启动失败` | 历史行与 grep 习惯不被打断 |
| 群说明读取失败 | 保持现状：任务失败且不启动 worker | owner 拍板 2026-09-17；主规格第 129 行与验收第 204 条已确认该承诺，群说明承载群内规则，静默降级会让回复脱离群约定 |
| 启动失败重试 | 不加：把默认启动预算从 10 秒提高到 30 秒（`startupTimeoutMs` 仍可覆盖） | owner 拍板 2026-09-17；实测失败来自环境瞬时变慢，给慢启动更多时间比多跑一次握手更省 |
| 看门狗就绪窗口 | 与 worker 启动预算共用同一 `startupTimeoutMs`（默认 30 秒），不设独立配置；超时仍判 `watchdog-unavailable` 并在有界时间内回收 | 2026-09-20 现场：typing 子区任务 `cda0b274` 在主机负载 31+ 时以 `watchdog-unavailable` 失败，固定 1 秒就绪窗口把慢启动误判为坏启动（就绪耗时、stderr、退出码现场均无留存，未能确证）；预算与 worker 对齐避免第二套阈值 |
| 工作区被移除 | 保持绑定不变，只报 `绑定不可用（cwd-missing）` | owner 拍板 2026-09-17；绑定由 owner 显式控制，回落父群目录会静默改变隔离语义 |

## 设计

### 原因码（`src/contracts.ts`）

```ts
export type TaskStartupCode =
  | "cwd-missing"           // 绑定 cwd（含隔离工作区）不存在
  | "session-dir-missing"   // 会话目录（sessionsDir 下的项目目录）不存在/不可用
  | "session-invalid"       // 绑定会话文件缺失，或恢复会话状态校验失败
  | "spawn-failed"          // 子进程无法 spawn
  | "identity-unverified"   // 子进程身份探测失败
  | "watchdog-unavailable"  // 父死看门狗无法就绪
  | "not-ready"             // 启动预算内未收到 ready
  | "exited"                // 子进程提前退出或输出中断
  | "handshake-failed";     // 协议协商、new_session、get_state 等启动握手失败

export class TaskStartupError extends Error {
  readonly code: TaskStartupCode;
}
```

`src/omp/index.ts` 的 `create()` 与 `initialize()` 在各失败点抛出带码的错误（消息文本保持现有措辞，便于既有断言与运维检索）：`cwd-missing`、`session-dir-missing`、`spawn-failed`、`identity-unverified`、`watchdog-unavailable`、`not-ready`、`exited`、`handshake-failed`、`session-invalid`（`refreshState` 的会话状态校验）。宿主在 `existingSessionFile(binding.sessionFile)` 为空时抛 `session-invalid`。

### 分类与呈现（`src/task-failure.ts`）

```ts
export type TaskStartupPhase = "context" | "startup";
export type TaskFailureKind = "context" | "binding" | "worker" | "generic";

export type StartupFailure = {
  readonly kind: TaskFailureKind;
  readonly code: string;   // 具体原因码，无码时取该类的类码
  readonly detail: string; // failureText() 的结果，写日志与任务行
  readonly notice: string; // 聊天里那一句
  readonly row: string;    // tasks.error_text
};

export function classifyStartupFailure(error: unknown, phase: TaskStartupPhase): StartupFailure;
```

`runTask` 只在读取群说明成功后把阶段推进到 `startup`；`catch` 分支的 `!worker` 路径调用该函数。

| kind | 触发 | 聊天文案 | 任务行前缀 |
| --- | --- | --- | --- |
| `context` | 群说明读取/规范化失败（群/子区任务） | `群说明读取失败，未启动任务。` | `群说明读取失败` |
| `binding` | `cwd-missing`、`session-invalid` | `任务未启动：绑定的工作目录或会话不可用（<code>）。` | `绑定不可用` |
| `worker` | 其余 worker 启动码与启动阶段的无码失败 | `任务未启动：OMP 运行进程启动失败（<code>）。` | `worker 启动失败` |
| `generic` | worker 已创建后的失败 | `任务执行失败；未能可靠完成。` | `任务执行失败` |

任务行格式为 `<类前缀>（<原因码>）：<单行文本>`（如 `绑定不可用（cwd-missing）：OMP worker cwd does not exist: …`），聊天文案只带原因码、不带路径。

类码（无具体原因码时使用）：`context-read`、`binding-unavailable`、`worker-start`、`task-failed`。

### 服务接线（`src/service.ts`）

- `runTask` 的启动阶段跟踪：`let phase: TaskStartupPhase = "context"`，群说明读取（仅群/子区）完成后置为 `"startup"`。
- 失败分支（`!worker`）：`console.error` 一行（`[task] 未启动 task=<id> target=<key> phase=<phase> kind=<kind> code=<code>：<detail>`）、`markFailed(taskId, row)`、`finishProgress("failed")`、`enqueueText(notice)`。
- 已存在 worker 的失败分支：聊天文案保持 `任务执行失败；未能可靠完成。`，任务行改为 `任务执行失败：<detail>`，并同样记录一行日志（现状是行内只有常量 `worker failed`、日志空白）。
- 删除 `genericTaskFailure` 与 `contextFailure`（被分类结果取代）。

## 失败模式

| 情况 | 行为 |
| --- | --- |
| 群说明读取失败（HTTP/超时/形状非法） | `context` 类：文案与主规格承诺不变；行与日志带真实原因 |
| 绑定 cwd 不存在（工作区被移除） | `binding` 类：快速失败、无 spawn；行与日志给出 `cwd-missing` |
| 绑定会话文件缺失 | `binding` 类：`session-invalid` |
| worker ready 超时 | `worker` 类：`not-ready`；子进程已被 `create` 的失败清理路径停止 |
| 子进程提前退出/输出中断 | `worker` 类：`exited` |
| spawn、身份失败 | `worker` 类：对应码 |
| 看门狗启动失败 | `worker` 类：`watchdog-unavailable`；有界 detail 区分 spawn 异常、提前退出（退出码 + ≤1024 字符 stderr 头部摘要、单行化）、stdout 读取失败、就绪超时（含预算毫秒数）；任一失败都 SIGKILL 回收该看门狗子进程，随后由工厂停止已启动的 OMP 进程组，安全门禁不变（watchdog 未就绪不创建 worker） |
| 私聊任务（无群说明读取） | 不产生 `context` 类：读说明阶段被跳过 |
| worker 已创建后的失败 | `generic` 类：文案不变，行与日志带真实原因 |
| 任务被取消 | 不变：仍走 `markCancelled` 与取消回执 |
| 无码错误进入启动阶段 | `worker` 类，类码 `worker-start` |

## 错误与边界情况

- `failureText` 的输入是 `Error.message`（或字符串），空结果回落到固定兜底文案；跨行与超长文本被压平、截断到 200 字符，避免日志注入与无界行。
- 文案里的原因码来自固定词表，不含路径、昵称、token 或载荷片段。
- 事件顺序不变：仍是先 `markFailed` 再投递文案（`enqueueText` 绑定 taskId，重启后按 outbox 恢复投递）。
- 任务行只追加前缀与真实原因，`/status` 不读取它，群成员看不到。
- 取消竞态：`runtime.cancelRequested` 优先，分类逻辑不介入。
- 失败任务不推进群说明 ledger 与群历史水位线（现状）；下一轮任务按同一规则重放。

## 验收标准

1. 群说明读取失败时，聊天收到逐字不变的 `群说明读取失败，未启动任务。`；任务行以 `群说明读取失败` 开头且含真实原因；日志有且仅有一行 `[task] 未启动 …`。
2. 绑定工作区目录不存在时：真实 worker 工厂在 spawn 之前以 `cwd-missing` 失败；任务行以 `绑定不可用（cwd-missing）：` 开头；聊天文案为 `任务未启动：绑定的工作目录或会话不可用（cwd-missing）。`；没有 worker 进程。
3. worker 未就绪（缩短启动预算的真实工厂）以 `not-ready` 失败：任务行以 `worker 启动失败` 开头且含 `not-ready`；聊天文案含 `not-ready`。
4. 子进程提前退出归 `exited`；spawn/身份/看门狗失败各有码且都归 `worker` 类。
5. worker 已创建后失败：聊天文案保持 `任务执行失败；未能可靠完成。`，任务行与日志含真实原因（不再是常量 `worker failed`）。
6. 私聊任务失败不会出现 `群说明读取失败`：无群说明读取阶段时按码归 `worker`/`binding` 类。
7. 既有断言不变：`tests/core-group-context.test.ts` 的群说明失败用例、取消/中断路径与 outbox 恢复行为全部保持通过。
8. worker 启动预算默认 30 秒：冷启动慢到十几秒、但确有进展的子进程不再被判失败；`startupTimeoutMs` 仍逐项覆盖该默认值（`not-ready` 的失败用例用缩短的预算驱动）。
9. `bun run typecheck` 与 `bun test` 全绿。

## 未决事项

无。2026-09-17 owner 对三项设计决策拍板，已写入「关键决策」：群说明读取失败保持现状；不加重试、默认启动预算改为 30 秒；工作区被移除后保持绑定不变。
