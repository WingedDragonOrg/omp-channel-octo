---
name: octo-card-render-diagnosis
description: "诊断 Octo 进度卡（omp-channel-octo 的 execution-card）在客户端渲染与预期不符时使用：对齐错位、背景色不对、某块没底色/变成粉色、节点超限卡片退化成 plain；含\"先证卡下发了什么、读 octo-web 客户端源码定位 DOM 后处理、再本地渲染量几何\"的取证链路"
---

# Octo 进度卡渲染诊断

用于「卡片在 Octo 客户端看起来不对」这类问题：错位、背景色不对、某块没底色/变粉、退化 plain。
核心原则：**先证服务器实际下发了什么，再谈渲染；客户端不是纯 SDK 渲染——它还会按元素 id 改写 DOM。**

## 1. 先取"实际下发的卡片原文"（最高价值证据）

Bot token 在仓库 `.envrc`（gitignore）。Bot API 能读回历史消息 payload（base64）：

```ts
const envrc = await Bun.file("<repo>/.envrc").text();
const token = envrc.match(/^export OCTO_BOT_TOKEN='([^']+)'$/m)?.[1];
const api = envrc.match(/^export OCTO_API_URL='([^']+)'$/m)?.[1];        // https://im.deepminer.com.cn
const target = envrc.match(/^export OCTO_PREVIEW_TARGET_UID='([^']+)'$/m)?.[1];
const json = await fetch(`${api}/v1/bot/messages/sync`, {
  method: "POST",
  headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  body: JSON.stringify({ channel_id: target, channel_type: 1, start_message_seq: 0,
    end_message_seq: 4294967295, limit: 200, pull_mode: 1 }),
}).then(r => r.json());
// 每条 message.payload 是 base64；解码后 type:17 的是卡片：
// { card, card_version, plain, profile, render_profile, space_id, type }
```

- `start/end/limit/pull_mode` 必填（缺 seq 会返回 0 条）；`limit` 上限 200。
- 用 `card` 里 `id`/`style` 与本地 `buildExecutionCard` 输出逐项比对 → 判定是**服务端**改写还是**客户端**渲染差异。历史上多次证明服务端原样存储，问题都在客户端渲染。
- 服务器/客户端把容器样式按名映射，不认识的属性会被忽略：**加非标准字段（如 Container 的 `padding`）在本地 JS SDK 里被无视**，不要指望它生效。

## 2. 客户端源码就在手边：Mininglamp-OSS/octo-web（别对着 minified bundle 猜）

`octo-web` 是 Web/桌面（Electron）客户端源码仓库，未随 `~/.wtc/projects/octo/` 检出，但可直接浅克隆（秒级）：

```bash
cd /tmp && git clone --depth 1 --filter=blob:none --no-checkout https://github.com/Mininglamp-OSS/octo-web.git
cd octo-web && git sparse-checkout init --cone && git sparse-checkout set src packages && git checkout
# 卡片渲染全部在：
#   packages/dmworkbase/src/Messages/InteractiveCard/
#     sdk/agentProgressLayout.ts   ← DOM 后处理（本 skill 最大的坑，见 §3）
#     sdk/octoHostConfig.ts        ← style → --wk-* token 的调色板（attention = --wk-color-danger-bg）
#     index.css                    ← .wk-interactive-card-progress-step--status 等真实视觉
#     renderDecision.ts / renderProfile.ts / validateCardForOcto.ts / cardLayout.ts
```

`grep -rn "progress-step\|#timeline_detail\|octo_layout" packages/dmworkbase/src/Messages/InteractiveCard` 一般 30 秒内定位规则。

## 3. 本地真实渲染量几何（可复现、无需打扰用户）

Octo 客户端（Android/iOS/web）都是 Microsoft AdaptiveCards SDK，`style` 背景 / `width` 像素列 / `horizontalAlignment` / `style` 带来的 **container padding** 都按 SDK 规则走。用 Chromium + `adaptivecards` 复现几何：

```bash
mkdir -p /tmp/ac-render && cd /tmp/ac-render && bun add adaptivecards playwright-core
# chromium 用 playwright 缓存：~/Library/Caches/ms-playwright/chromium_headless_shell-*/chrome-headless-shell-mac-arm64/chrome-headless-shell
```

渲染 + 量测要点：
- `page.setContent` 内联 `dist/adaptivecards.css`，容器背景 `#fff; padding:16px`，viewport 设 505 CSS px、`deviceScaleFactor: 2`（贴合真实屏幕）。
- `card.parse(json); host.appendChild(card.render())`，然后读 `.ac-textBlock` 的 `getBoundingClientRect()` 与 `getComputedStyle(el).backgroundColor`。
- `isVisible: false` 的元素 rect 是 0 → 量之前递归删掉所有 `isVisible` 键（模拟展开态）。
- 对比"改前/改后"：把改前状态用 JSON 变异造出来（例如把某个 group 的 `style` 改回去），一次渲染里对比。

## 4. 已知的客户端渲染事实

| 元素 | 常量/事实 |
|---|---|
| `Container.style` 背景 | emphasis → 浅灰；accent → **白（无底色）**；attention → 粉；default/无 style → 透明 |
| container padding | 只有**带背景 style** 的 Container 才有，客户端为 16 CSS px（=32 设备像素 @2x）。`padding` 属性不生效 |
| 面板行网格 | Container(padding 16) → ColumnSet → Column(24px 标记) → stretch 正文 ⇒ 标记/正文列 = 内容列 +16 / +40 |
| Todo 行 | 固定 80px 标记列（树前缀 + `[✓]` 括起字形，Monospace，`wrap:false`） |
| 平铺历史行 | 组容器本身带 style 时内部自然获得 16px 内缩；不带 style 的组容器 = 无内缩，行会左移 16px |
| 节点预算 | 服务端 `max_nodes: 200`（profile API `/v1/bot/card/profile`），客户端也二次防御 `MAX_NODES=200 / MAX_DEPTH=16`（octo-web `InteractiveCard/types.ts`）。`buildExecutionCard` 超限返回 **null**（卡直接不发）。每行 +2 节点就可能撑破 200 |

### 4.1 坑：octo-web 会按元素 id 改 DOM（`#timeline_detail` + 有底色 Container = 粉块）

`metadata.octo_layout === "agent_progress_v1"` 的卡（我们的进度卡就是）会走 octo-web 的 agent-progress 层：

- `sdk/agentProgressLayout.ts` 遍历 **`#timeline_detail` 的直接子元素**，凡是 `.ac-container` 且**带 inline 底色**的，加类 `wk-interactive-card-progress-step--status` 并**清空自己的 backgroundColor / padding**；
- `index.css` 里该类的规则是 `background: var(--wk-color-danger-bg)` + `border: 1px solid var(--wk-color-danger-border)` ⇒ 视觉上是**粉底 + 描边的"失败步骤状态块"**；
- 这是客户端的既有契约（`__tests__/agentProgressLayout.test.ts` 用 `❌ 读取文件` + 粉色 `rgb(254,242,242)` 固化了它），不是 bug。

因此**任何带 `style` 的 Container 都不能直接挂在 `#timeline_detail` 下**，否则整块被客户端刷成粉红（历史上"执行轨迹历史带变红、padding 被清掉导致行左移、和子代理灰带不对齐"就是这条）。

正确形态：

```
timeline_section
├─ SectionHeader（执行轨迹 + 展开/收起 ActionSet）
├─ timeline_detail            ← 只放步骤：hidden summary TextBlock + ColumnSet 行（行内 Container 是嵌套的，安全）
└─ timeline_omitted_steps     ← 灰色历史带（style: emphasis），放在 detail 旁边；折叠仍由 ActionSet 按 id 控制
```

配套不变量（值得写成回归断言）：**`#timeline_detail` 的直接子元素里不允许出现带 `style` 的 Container**（`item.type === "Container" && item.style !== undefined` 必须为空数组）——这正是客户端那条规则的判定条件，结构上守住它就不会被刷粉。

对照：`#subagent_detail`、`#todo_detail` 不在客户端那条规则的作用域内，带底色的组容器在那里照常按 `style` 渲染（灰就是灰）。

## 5. 从截图量真实像素（用户发图时）

图片会被存成 blob：`~/.omp/agent/blobs/*.png`（有时不可得，可用 `read attachment://N?q=...` 让视觉模型读）。
解码用 ffmpeg→raw+numpy，用"每行墨迹列簇"求标记/正文 x：

```bash
ffmpeg -y -i shot.png -f rawvideo -pix_fmt rgb24 out.raw
```
```python
a = np.fromfile("out.raw", np.uint8).reshape(h, w, 3).astype(int)
lum = a.mean(axis=2); ink = lum < 200          # 依阈值挑文字
cols = np.where(ink[y0:y1].any(axis=0))[0]      # 按 y 带切行，按 ≥5px 间隔聚簇
```
用"卡片左边缘 / 页面灰底"做共同参照系，跨截图对齐坐标。

## 6. 交付纪律

- 改完 `src/execution-card.ts` **必须**发真实卡：`set -a && . ./.envrc && set +a && bun run test:preview-card`（live 用例带 token 时发送到预览会话）。
- 发完用 §1 的 sync 读回**已下发**的卡，验证结构（例如"`timeline_detail` 下没有带底色的 Container"），这是端到端证据。
- 几何改动跑 `bun run typecheck` + `bun test`；`tests/preview-card.acceptance.ts` 里有非 live 的结构断言。
- 修正几何/样式时同步改 `tests/execution-card.test.ts` 的断言（它按列宽/字形文本精确断言）。
