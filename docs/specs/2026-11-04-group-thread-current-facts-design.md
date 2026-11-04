# 群与子区当前事实清单

日期：2026-11-04
状态：已实现并通过隔离验收。

## 目标与存放位置

当前任务需要持续保留的约束、决策与状态，直接维护在当前聊天已有说明中：群使用 GROUP.md，子区使用 THREAD.md。

```markdown
### Memory Start ###

{内容}

### Memory End ###
```

## 操作方式

复用现有 `octo_management`：

1. 用 `description-read` 读取当前完整说明。
2. 只在两个标记之间更新或追加当前事实；没有区段时，在文末建立该区段。
3. 保留区段外的全部原文，再用 `description-write` 写回完整说明。

固定区段由 agent 按约定维护，内容直接使用 Markdown；owner 可要求修改、删除某段或清空。宿主在现有说明写入工具上执行以下前置校验。

## 写入前置条件

- 当前任务必须先成功调用 `description-read`；自动注入的说明、其他聊天或上一任务的读取不能代替这次工具读取。
- 宿主保存读到的正文和版本。每次 `description-write` 前重新读取真实说明，两者有任何变化就拒绝，提示重新 `description-read`，不提交旧正文。
- 一次写入尝试消耗这次读取凭据；成功写入后或写入失败后，再次写入都必须重新读取。
- 同一工具实例内的说明读写依次执行，避免两个并行写调用复用同一次读取。其他管理动作不进入这条队列。
- 这是宿主写前校验，不是服务端原子 CAS。已核对的群与子区说明写接口只接收 `content`，没有期望版本条件；其他客户端仍可能在最后一次 GET 与 PUT 之间写入。若要消除该窗口，需要服务端支持条件更新，本仓库不宣称已经实现。

接口依据：[群说明 `updateGroupMd`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/bot_api/groups.go)、[子区说明 `botUpdateThreadMd`](https://github.com/Mininglamp-OSS/octo-server/blob/9a06a89d5e3dd5d668ef69388f672c5cf65026ec/modules/bot_api/threads.go)。

## 作用域与上下文

- owner 已选择当前聊天独立：群只使用本群 GROUP.md，子区只使用本区 THREAD.md；不自动读取父群、上浮内容或同步到兄弟区。
- 固定操作指引提供上述维护方式，现有聊天授权和服务端实际权限保持不变。
- Memory 内容包含在当前说明快照中，复用现有版本读取、ledger/sidecar 注入与 compaction 恢复机制。
- 当前回合快照固定，说明更新由下一任务读取；未变化的普通回合不重复注入完整说明。
- 会话新建、迁移、恢复与 fork 沿用既有聊天绑定和说明行为，不按 cwd 推导其他聊天内容。

## 验收

- 检查真实模型请求能获得维护指引，工具仍使用已有的说明读写操作。
- 未读取就写、读取后正文或版本已变化、重复使用读取凭据、并行写调用必须拒绝不合格写入；重新读取后可正常写入。
- 用隔离 localhost 模型、真实 OMP 和本地 Octo fixture 驱动完整说明读取与写回；观察固定区段内变更、区段外原文保留、群/子区范围及后续任务的版本注入。
- 运行现有群上下文、compaction、会话和权限回归，以及 typecheck、全量测试和离线重启验收。合成模型 smoke 证明接线与注入，不替代真实模型遵循操作约定的质量评估。

仅修改 omp-channel-octo。对象监控独立交付；本任务不自动提交、推送、合并或重启常驻服务。

## 验收记录

- `bun run typecheck` 通过；定向说明写入、host tools、群上下文与 extension 回归共 47 项通过。
- `bun test --timeout 30000`：1393 pass、0 fail，104 个测试文件，8578 条断言。全量运行使用 30 秒单测预算。
- 真实已安装 OMP 18.16.0、有限 localhost 模型和本地 Octo fixture 完成 15 次实际模型请求：验证未读写入被拒绝、原说明区段外原文保留、群/子区/兄弟区隔离、当前回合快照固定与下一回合版本注入。
- 通过原生 `compact` RPC 完成实际压缩：2 次摘要模型请求、1 条持久 compaction 记录；压缩后和空闲 SIGKILL 重启后的实际模型输入仍取得本区 Memory 内容。
- 离线 `tests/benchmarks/restart.ts` 的 graceful/SIGKILL 两场景均续接 accepted 工作并恢复排队任务；会话重叠、重复副作用与中断通知均为 0。
- 验收使用临时配置、会话与合成内容，未调用真实 provider 或注册常驻 Bot；一次性 smoke 脚本、请求捕获与临时数据已移除。
