---
name: octo-adaptivecard-node-budget
description: 设计或修复 Octo（im.deepminer.com.cn）执行进度 AdaptiveCard 时使用：真实服务端 max_nodes=200，需按元素预算选渲染方案，并保证所有行样式统一、超预算的整行不渲染只报计数。
---

# Octo 进度卡节点预算与统一样式

## 何时使用

改 Octo/AdaptiveCard 进度卡布局、折叠展开、行样式，或卡片出现「某些行样式不同」「展开后样式变了」「内容莫名消失」时。

## 硬事实

真实服务端能力接口（无需登录，仅需 bot token）：

```sh
source .envrc >/dev/null 2>&1
curl -sS -H "Authorization: Bearer $OCTO_BOT_TOKEN" \
  "${OCTO_API_URL:-https://im.deepminer.com.cn}/v1/bot/card/profile"
```

返回 `limits.max_nodes = 200`、`max_depth = 16`、`max_payload_bytes = 524288`，profile `octo/v1,octo/v2`，card_version `1.5`。**节点上限是 200，不是「很大」**：9 次工具调用 + 12 个 Todo 任务刚好贴着上限。

## 步骤

1. **先量预算，别先改样式。** 用与 `src/octo/index.ts` 同口径的遍历统计节点数（Array 不计，每个对象计 1，含 `elementId/isVisible` 这种 toggle target 对象）。

2. **实测每类元素成本**（在本仓库实测口径）：

   | 元素 | 节点 |
   |---|---|
   | 面板行（ColumnSet→Column→Container→3×TextBlock） | 6 |
   | 面板行 + 内层 ColumnSet（右侧状态列） | 10 |
   | Todo 任务行（ColumnSet + 2 Column + 2 TextBlock） | 5 |
   | Todo 阶段行 | 5 |
   | 阶段包裹 Container | 1 |
   | ActionSet + action + 每目标 | 2 + 目标数 |

3. **用脚本复现真实场景再决策**：在 `eval` 里用模块的 `createExecutionTracker` 重放一份与线上同规模的事件（工具数、Todo 数、折叠窗口 `MAX_VISIBLE_TOOLS`），再按真实 caps 调 `buildExecutionCard`，打印节点数与 `plain`。改前后各跑一次，用数字说话。

4. **要腾预算时，按这个优先级砍**：
   - 删重复信息（如状态区右侧耗时列与 metrics 行重复 → 省 2）
   - 合并字形到文案（`${glyph} ${label}` 一个 TextBlock，省 2/行）
   - 去掉纯布局用的包裹 Container（阶段容器 → 同容器直排，省 1/阶段）
   - 绝不用「给一部分行换更简陋的样式」来省预算

5. **超预算的行为**：整行不渲染，只输出计数（`更早的步骤未展示：N 个`）。不要为了显示一行而降低该行样式。

## 不变量

- 默认可见行、折叠隐藏行、展开后的历史行使用**同一个渲染函数**；折叠只改 `isVisible`，不改结构。
- 行序为时间顺序：折叠的历史行在前，最新 N 步在后，与「已隐藏前 N 个步骤」文案一致。
- 能力不支持 `Action.ToggleVisibility` 时不存在收起状态：所有行默认展开，不渲染收起控件，也不要有 `isVisible:false` 却无入口恢复的行。
- `ColumnSet` 必须是轨迹容器（`timeline_detail`）的直接子节点，状态色 `Container` 放在其 stretch 列内（Web 端渲染要求）。

## 验证

- `bun run typecheck && bun test`（`tests/execution-card.test.ts` 覆盖结构、折叠目标、预算、状态色）。
- 真实卡片：`source .envrc && bun run test:preview-card`，再在手机端确认展开前后行样式一致。
- 回归测试要显式断言「所有行面板样式相同」与「无扁平行残留」，否则样式回归不会被抓到。
