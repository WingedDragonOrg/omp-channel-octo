# 合并资源占用与并发边界调查 SDD

日期：2026-11-04  
状态：owner 已确认两个合并入口不实施资源并发限制；本工作区已实现并完成隔离验收。

## 目标和边界

定位 17:45 截图中的拒绝，区分可见竞争、按任务工作目录推断和未知进程风险；通过真实 Git/worktree/Worktrunk 实验界定原子性，提出可实施的最小资源覆盖。普通独立聊天及 session 继续互相并发；本调查不处理 transport inboundChain 队头阻塞、卡片展示或线上资源清理。真实现场只读，不重试线上合并，不重启常驻服务。

## 现场证据

- 截图子区为 `e3bc5af0d4b84b18bb3770a74bd222c6____2117869479948980224`。17:45:20 的输入是普通任务，不是 `/thread merge` 命令；会话记录于 17:45:25.862 调用 `octo_management`，action `thread-merge`、target `main`、`remove=false`，17:45:25.968 返回原文「隔离工作区正被其他任务使用，无法合并」。会话文件为 `~/.oco/sessions/omp-channel-octo.status-context-and-repository-changes-4a1f62c8/2026-11-04T06-40-38-292Z_01a1a549-c794-75de-8b51-b9c7db0cd4e2.jsonl`，行 1102–1106；时间为本机 UTC+8。
- source 是 `/Users/dong/workspace/omp-channel-octo.status-context-and-repository-changes`，任务 `aa1ee35a-6b40-4ba6-8397-84fb95bbdfd1`，session `01a1a549-c794-75de-8b51-b9c7db0cd4e2`，worker PID/PGID 9283；17:45:20.065 启动，17:45:35.329 结束。
- target `main` checkout 是 `/Users/dong/workspace/omp-channel-octo`。同一时段主群任务 `ad359e30-4820-4384-8e27-49d727d2a594` 绑定该 cwd，session `01a0bcf1-3cba-77ce-b968-6e73c783968c`，worker PID/PGID 7346；17:44:46.568 启动，17:46:29.963 结束。只读 SQLite `tasks` 与 `worker_ledger` 可核对 ID、路径和时间；两个 ledger 此后均为 `stopped`。
- **归因：[推断，高置信]** 当时 target checkout 的主群任务符合活动资源冲突条件，因此足以导致拒绝。内存 `activeResourceUsers` 和每次拒绝的命中项没有持久快照，无法事后证明它是唯一命中项，也不能证明主群任务实际修改了目标文件、index 或 ref。它正在运行是事实；实际写竞争未被证明。该任务属于父群并不是拒绝理由，真正关系是其 cwd 与目标 checkout 相同。
- 调用者的 task ID 被资源检查明确排除；该机制表明调用者自身不应产生这次冲突。没有证据表明不相关 worktree、已结束任务或旧 merge lease 是本次拒绝原因。只读快照时 `host_merge_leases` 为空；此次拒绝发生在 `beforeSpawn` 之前，因此没有该次 merge 进程或新 lease。截图时 status 记录还显示主群及源子区均为 running；这只能佐证运行，不单独证明锁 owner。历史快照无法还原当时所有内存 reservation 或短暂异常。
- 17:51 左右的只读快照显示两个当事任务均已结束，不代表未来重试一定成功。本次未对真实工作区执行合并。

## 截图时的判定

`src/host-tools.ts:1381-1442` 把模型任务传给 `mergeThreadWorkspace`；`src/threads/thread-merge.ts:143-168` 用 `planMerge` 规划并申请资源，申请失败即抛截图原文。`src/workspace/worktree.ts:789-852` 的计划包括 source canonical worktree，和目标分支已 checkout 时经过验证的 target worktree；未 checkout 目标没有捏造路径。持有租约后重新规划，计划改变则拒绝。host tool 即时拒绝；`src/service.ts:4179-4296` 的 `/thread merge` 是无模型任务的另一入口，先预留并等待较早冲突工作。

`src/tasks/task-scheduler.ts:696-765` 检查：停止中、blocked directory、活动资源用户、排他租约、同 chat/session 排队、更早冲突 reservation、较早排队任务和未知 repository block；caller task 被排除。资源关系是相等或祖先/后代路径，或者相同 canonical **worktree root**，不是单纯相同 Git common-dir。普通任务只发布共享使用，不彼此上锁。已终态的任务在 worker 确认退出和资源清理之前仍可能保持使用；确认停止后正常释放。

该报错不是“其他任务在写”的证明：普通任务仅因 cwd/root 关联而被视作潜在使用者；较早排队任务、进程退出未知、reservation 及停止中也可触发。检查过程中的异常被 `tryAcquireDestructiveResources` 捕获并返回空，较早 queued task 的 binding/root 探测异常也直接判冲突（`task-scheduler.ts:1026-1052`），故与资源无关的探测异常也可能呈现相同提示。此现象是代码可达性，不是本次截图的已证实原因。

### 三类事实

1. **真实副作用竞争**：同一 checkout 同时 stage、rebase、更新工作树或删除目录，或两条 pipeline 同时更新同一 ref；应以可观察进程和动作证明，不能由任务状态单独断定。
2. **保守推断**：运行/排队任务的 cwd 或 checkout root 与 source/target 相关，仅说明它可能使用或写该资源；模型生成阶段也可能被计入，不能据此声称实际发生了写竞争。
3. **未知结果/退出**：旧受管 worker 或 merge 进程组无法确认退出，或 Git 副作用已开始但业务结果未持久化。此时阻塞对应已知资源是为防止与仍运行的进程竞争或重复不可逆副作用，不等同于证明进程仍存活。

## 隔离实证

本机 Git 与 Worktrunk v0.80.0；每场景为新建一次性仓库和至少两个真实 worktree。真实 `wt merge` 使用临时配置的 `pre-merge`/`pre-remove` hook；barrier hook 在 Git common-dir 写各自 ready 文件后等待 release，确保并发到达指定阶段。实验使用 `-y` 批准一次性仓库的已知 hook；宿主正式调用仍不传 `-y`。实验后资源已删除。

| 场景 | 命令与观察 | 能证明的边界 |
| --- | --- | --- |
| 同目标 | 两个 source 同时运行 `wt merge main --no-squash --no-remove -y --format json`，两个 hook 均到达并看到相同旧 main；释放后一次 exit 0，一次 exit 1，失败为 `cannot lock ref ... is at ... but expected ...`。目标只包含胜方 tip，失败 source 分支及 worktree 保留，目标 index/checkout 干净。 | 此次 target ref 的 expected-old-value CAS 防止丢失另一更新；两条 pipeline 并未在 hook 前整体串行。不能推广为所有交错下必有且仅有一个成功。 |
| 不同目标 | 两个 source 分别合并 `target1`/`target2`，两个 hook 同时到达，均 exit 0，两个 target refs 各自前进。目标仅为 refs，没有分别 checkout。 | 独立目标 ref 允许并行，不需要 repository-wide 互斥；不证明共享 checkout 或共享 hook 副作用独立。 |
| 普通任务与目标 checkout | source hook 暂停时，另一进程在 main checkout 写新文件并 `git add`；随后 merge exit 0，main 前进，普通文件仍 staged、内容保留且不在 merge commit 中。 | wt 不会替宿主阻止普通进程修改同一目标 index/checkout。本次未发生覆盖或吞改；不证明所有并发写都安全。 |
| 默认 pipeline 后续 pre-merge 失败 | source 含已修改和未跟踪文件，`wt merge main -y --format json` 在 staging、squash、rebase 后 hook exit 42。main 不变，source branch tip 和 index 已改变、工作树变干净，存在 wt backup ref。 | pre-merge 失败不回滚之前的 source 副作用，备份不是整体事务回滚。 |
| rebase 冲突 | `wt merge main --no-squash --no-remove` exit 1；main ref 不变，source detached、index 有 `UU`、`rebase-merge` 存在；实验后 `git rebase --abort`。 | checkout/index 和 rebase 元数据可保留中间态。 |
| pre-remove 失败 | `wt merge main --no-squash` 在更新 target 后 pre-remove hook exit 43；target 已前进，source worktree 和 branch 保留。 | 非零退出不意味着目标未更新；merge 和删除不组成原子事务。 |

Git ref CAS 和 index lock 分别保护对应单次操作，不能把源工作树自动 stage/commit、squash/rebase、任意 hooks、target checkout/index 更新、目标 ref 推进、worktree/branch 清理与宿主数据库结果视为同一事务。hooks 可以写外部状态，其作用范围取决于具体 hook；本实验未证明任意 hook 外部副作用可回滚。锁只协调受宿主管理的参与者，外部 shell、编辑器和其他进程不受宿主锁约束。

## 已确认方案

`octo_management thread-merge` 和 `/thread merge` 不对 source、target 或其他任务和操作实施资源并发限制，也不以合并占用阻挡其他独立工作。合并可以与其他合并、普通任务、commit 或 workspace 清理交错执行；同一聊天和精确 session 的既有顺序与任务身份语义不变。Git 和 Worktrunk 的实际成功、冲突或失败结果照实返回，不承诺流水线原子回滚或自动重试。

此选择允许同一 source 上的暂存、squash、rebase 和删除与其他文件操作交错，也允许 target checkout/index 更新与其他写入交错；Git ref CAS 和 index 锁不能覆盖任意普通文件写或 hook 外部副作用。不能依据当前是否仅在模型生成判断任务未来不会写。不同目标也可能通过 source 或 hook 共享状态。

继续持久化进程身份与业务结果；取消等待实际进程组终结，结果未知时不把该次操作当成成功，也不自动重放该次操作。已知路径记录用于诊断和恢复，但不作为独立合并或其他独立工作的资源并发限制。身份、归属和目标计划验证仍用于确保操作指向指定工作区；不建立全局仓库锁或通用锁框架。

## 验收与实施边界

1. 两个入口均验证 source、target 上普通任务和其他合并或破坏操作在先启动与后启动两种顺序下均不因资源占用等待或拒绝；独立 worktree 和同目录均可并发。验证实际副作用或完成，不只看 started 状态。
2. 在可控 barrier 的真实 Git/wt 场景覆盖同目标有定义的成功、CAS/冲突或失败结果，不声称必成功；不同目标可并发；checkout/index 与 hook、晚期失败的实际结果可核对。
3. 覆盖结束、取消、重启与未知结果：保留进程身份和真实结果，未知时不猜测成功或自动重放原操作；不把历史 merge 资源记录变成新操作的并发限制。
4. 先加失败回归，再做最小实现，同步 `2026-09-12-octo-channel-design.md` 与 `2026-09-17-thread-worktrunk-design.md` 的实际契约及必要的仓库指引；运行相关资源/merge/core 测试、`bun run typecheck`、全量 `bun test`、临时真实 Git smoke 和 `bun tests/benchmarks/restart.ts`。不动其他子区的 inbound 队列改动，不提交、推送、合并或部署。

## 隔离验收记录

- 实施前的 host-tool 回归在 target checkout 普通任务运行时返回占用错误；实现后，两个入口的 source/target 并发及 commit/remove 交错行为纳入定向集，7 个文件共 131 项通过。
- `bun run typecheck` 通过；全量 `bun test` 为 1558 pass、0 fail。一次较早的全量运行有 1 个进度卡终态时序断言失败；该测试随后在定向集及全量重跑通过。
- `bun tests/benchmarks/restart.ts` 的 graceful 与 SIGKILL 均恢复在途及 queued 任务；session overlap、重复副作用和中断通知均为 0。`bun tests/benchmarks/concurrency.ts` 的 23 个真实 OMP 进程均确认退出，session overlap 为 0，未遗留受管子进程。
- `tests/thread-merge-host-first.integration.test.ts` 通过临时真实 Git 仓库验证宿主合并、冲突及 hook 失败修复；其中 wt 命令是 fixture。上文并发及流水线边界实验另使用真实 Worktrunk v0.80.0。
- 所有 Git 修改均在一次性资源；现场数据库只读，未重试线上合并、未重启或注册常驻 bot，未提交、推送、合并或部署。
