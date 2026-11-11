import { spawnSync } from "node:child_process";

const osascript = "/usr/bin/osascript";
const systemPath = "/usr/bin:/bin:/usr/sbin:/sbin";

const consentScript = [
  "on run argv",
  "set promptText to item 1 of argv",
  "set answer to display dialog promptText with title \"OctoBuddy Enhancer\" buttons {\"Cancel\", \"Enable CDP\"} default button \"Cancel\" cancel button \"Cancel\" with icon caution",
  "return button returned of answer",
  "end run",
].join("\n");

const notificationScript = [
  "on run argv",
  "display notification (item 1 of argv) with title \"OctoBuddy Enhancer\"",
  "end run",
].join("\n");

export function requestNativeCdpConsent(): boolean {
  const prompt = "本机其他程序可能通过 CDP 读取或控制 OctoBuddy 页面。CDP 仅绑定 127.0.0.1，随机端口不是认证。是否继续启用？";
  const result = spawnSync(osascript, ["-e", consentScript, prompt], {
    encoding: "utf8",
    env: { ...process.env, PATH: systemPath, LC_ALL: "C", TZ: "UTC" },
    timeout: 120_000,
  });
  return result.status === 0 && result.stdout.trim() === "Enable CDP";
}

export function showLauncherNotification(message: string): void {
  spawnSync(osascript, ["-e", notificationScript, message], {
    encoding: "utf8",
    env: { ...process.env, PATH: systemPath, LC_ALL: "C", TZ: "UTC" },
    stdio: "ignore",
    timeout: 10_000,
  });
}
