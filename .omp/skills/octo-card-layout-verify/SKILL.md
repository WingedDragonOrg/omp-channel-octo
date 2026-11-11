---
name: octo-card-layout-verify
description: 当要验证/调整 Octo 执行进度卡（omp-channel-octo 的 src/execution-card.ts）或其它 AdaptiveCard 的列对齐、背景色、节点预算时使用：本地渲染量几何、从 bot API 取回已下发卡原文、发 live 预览卡。
---

# Octo 卡片布局验证

三条互补的证据通道。改 `src/execution-card.ts` 后按顺序用：**本地渲染量几何** → **取回已下发原文** → **发 live 预览**。

## 1. 本地渲染量几何（不需要用户、不需要登录）

一次性装依赖（网络可达）：

```bash
mkdir -p /tmp/ac-render && cd /tmp/ac-render && bun add adaptivecards playwright-core
```

Chromium 用 playwright 缓存里的 headless shell，别下载：

```bash
ls ~/Library/Caches/ms-playwright/chromium_headless_shell-*/chrome-headless-shell-mac-arm64/chrome-headless-shell
```

生成卡 JSON：写一个 `/tmp/geom/gen.ts`，`import { createExecutionTracker, buildExecutionCard } from "<repo>/src/execution-card.ts"`，按 `tests/preview-card.acceptance.ts` 的 `previewSnapshot()` 重放同样的事件（tool_start/tool_end/todo_update），capabilities 取 `previewCapabilities`（maxNodes 200）。`buildExecutionCard` 返回 null 就说明**超了服务端 200 节点上限**，不是 bug。

渲染 + 量：`playwright-core` 启动上面的可执行文件，`page.setContent` 内联 `node_modules/adaptivecards/dist/adaptivecards.css`，`page.addScriptTag({path: .../adaptivecards.js})`，页内 `new AdaptiveCards.AdaptiveCard().parse(json)` + `render()`，然后对 `.ac-textBlock` 取 `getBoundingClientRect()`，对容器取 `getComputedStyle(el).backgroundColor`。

要点：
- 折叠的容器（`isVisible:false`）在渲染前先递归删掉 `isVisible`，否则 `rect` 全 0。
- 视口用 505 CSS px（≈ 客户端 1010 设备像素 @2x）。
- 比对"同一列在不同区块是否重合"看**正文列 x**；`TextBlock` 的 `rect.right` 量不到字形墨迹（块会撑满列宽）。
- 像素级对照客户端截图：blob 在 `~/.omp/agent/blobs/*.png`（按 IHDR 找尺寸），`ffmpeg -f rawvideo -pix_fmt rgb24` 解成 raw 后用 numpy 按行求 ink 列簇；截图是 2x，卡左缘、面板左缘、字形 x、正文 x 相减即得各段 inset。
- 官方 JS SDK 与 Octo 客户端不完全同参：SDK 里样式容器 padding 是 20px，客户端实测 16px。用 SDK 验证"两区块是否互相一致"，用截图验证"与客户端绝对像素是否一致"，别混用。

## 2. 取回已下发的卡原文（证明服务端没改写）

```ts
POST {OCTO_API_URL}/v1/bot/messages/sync
Authorization: Bearer $OCTO_BOT_TOKEN
{ channel_id: <owner uid>, channel_type: 1, start_message_seq: 0, end_message_seq: 4294967295, limit: 200, pull_mode: 1 }
```

返回 `messages[].payload` 是 **base64**；解码得 `{type:17, profile, card_version, card, plain, render_profile, space_id}`。用 `card` 逐字段和自己本地构建的 JSON 比对，能直接判定"客户端看到的样式"与"我们发出的样式"是否一致——比猜客户端行为快得多。`message_seq` 递增，最后一条即最新卡。

## 3. 发 live 预览卡（改完必发）

```bash
cd <repo> && set -a && . ./.envrc && set +a && bun run test:preview-card
```

`.envrc`（gitignored）提供 `OCTO_BOT_TOKEN / OCTO_PREVIEW_TARGET_UID / OCTO_PREVIEW_CHANNEL_TYPE / OCTO_API_URL`；live 用例只在存在 token 时执行，见 `tests/preview-card.acceptance.ts` 末尾。

## 已验证的客户端渲染事实（别再重复试错）

- 客户端 hostConfig：`emphasis` → 灰（#F7F8F9，无边框，含 16px padding）；`attention` → 粉底 + 红边；`good`/`accent` 同族语义色。
- 只有**带背景样式**的容器才有 container padding：`style:"default"` + `padding:"Default"`（或 `true`）在 JS SDK 里实测 **paddingLeft=0**，Column 的 `padding` 同样不生效。想要无背景又要有 inset，只能把 inset 写进列宽。
- 带背景的组容器颜色**会被客户端按内容带偏**：同卡内两个 `style:"emphasis"` 组，以失败行结尾的那个渲染成 attention 粉。所以"两组历史行颜色一致"只能靠**两边都无背景**。
- 行网格：面板行 = 容器 padding 16 + 标记列 24px；历史行 = 标记列 40px（16+24）且字形 `horizontalAlignment:"Right"` + 尾随 `\u00a0`，两者字形列与正文列重合。
- 节点预算是硬约束（服务端 `max_nodes:200`）。给每行加列会直接撑爆并让卡片整体不生成；改列结构时先跑上面的 gen.ts 确认非 null。
