# OctoBuddy Enhancer

macOS 上的 OctoBuddy 本地插件宿主。增强器启动已安装的 OctoBuddy，不修改其应用包；插件可以在允许的页面添加界面和交互。

## 安装与启动

在本目录安装依赖并构建启动入口：

```sh
bun install
bun run scripts/build-app.ts
```

生成的 `dist/OctoBuddy Enhancer.app` 可从 Finder 打开或加入 Dock。构建出的入口引用本目录和构建时的 Bun 路径；移动或删除工作目录后需要重新构建。普通 OctoBuddy 已运行时，先退出它，再打开增强入口。

首次启用时会提示本地调试风险。增强器使用仅绑定本机的 CDP 随机端口；随机端口不是认证，本机其他进程仍可能读取或控制应用页面。只安装信任的本地插件：插件可访问其允许页面的内容，isolated world 不是安全沙箱。如果增强宿主停止而 OctoBuddy 仍运行，CDP 端口可能继续开放；退出该 OctoBuddy 实例可关闭端口。

## 本地插件

将可信插件放在 `~/.config/octobuddy-enhancer/plugins/<插件目录>/`。插件根目录、各插件目录和 `manifest.json` 需要由当前用户拥有且不允许其他用户访问；目录权限可设为 `0700`，manifest 可设为 `0600`。目录不存在时只加载内置插件。

插件使用 Manifest V3 的 `content_scripts` 声明本地 JS/CSS 和匹配规则。脚本在允许的 OctoBuddy 页面隔离环境中运行，可通过 `window.octoEnhancer.onDispose(callback)` 注册清理函数。在 OctoBuddy 内按 `⌘⌥⇧P` 打开或关闭插件管理界面；清理无法确认完成时会报告错误。`openZed` 是内置 Zed 插件的受限能力，不是任意本地命令接口。

## Channel 状态面板

内置 `channel-status` 插件在 OctoBuddy 通信页面右边缘中部显示状态入口，默认半透明，鼠标悬停时不透明；可拖拽位置，点击展开或收起面板。在增强器目录的私有 `.envrc` 配置服务地址和独立 Bearer token：

```sh
export OCTOBUDDY_HTTP_BASE_URL='http://my-mini:8765'
export OCTOBUDDY_HTTP_TOKEN='<与服务端相同的 token>'
```

进程环境变量优先于 `.envrc`；状态变量只传递给增强器宿主。已有私有配置 `~/.config/octobuddy-enhancer/status.json` 可作为未设置环境变量字段的回退，文件需由当前用户拥有、权限为 `0600` 且无额外 ACL。base URL 不要附加 `/v1/status`；公网应使用 HTTPS，明文 HTTP 会使 Bearer token 有被窃听的风险。该 token 允许读取整个 channel 的只读状态，应妥善保管。

面板显示服务整体状态及最近成功采样时间，并读取 OctoBuddy 页面提供的当前目标，随聊天切换自动查询群、子区或 Bot 私聊状态。群与子区使用页面提供的聊天 ID；Bot 私聊的页面 ID 是 Bot UID，而服务侧私聊目标是 owner UID，面板依据服务快照将其映射到该目标。聊天状态包括执行状态、模型策略、工作目录、排队任务数和待回答问题数。服务端启用、停用及 token 文件要求见主仓库 [状态接口说明](../../README.md#只读-http-状态接口)。

## Zed

内置插件为明确的本地文件链接提供 Zed 操作，支持行号；点击 `zed://` 链接会将完整链接交给 Zed，例如 `zed://ssh/my-mini/Users/dong/project/file.ts`。SSH 连接由本机 Zed 和 SSH 配置处理。其他链接保留原来的打开方式。Zed 需要安装在本机；路径和链接作为独立参数传递，不作为 shell 命令运行。

## 验证

```sh
PLAYWRIGHT_SKIP_BROWSER_GC=1 bunx playwright install chromium
bun run test
bun run typecheck
bun run test:integration
```

集成验收启动已安装的 OctoBuddy，使用临时用户目录和临时 Zed 记录器，不使用真实登录资料或真实 Zed。插件宿主基于 electron-webext 的固定版本维护；支持的扩展 API 以实际实现和验收为准，不保证兼容任意 Chrome 扩展。
