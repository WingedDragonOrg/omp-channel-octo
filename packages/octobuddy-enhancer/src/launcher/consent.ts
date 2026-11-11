import { readFile } from "node:fs/promises";
import { createInterface } from "node:readline/promises";
import { stdin, stdout } from "node:process";
import { requestNativeCdpConsent } from "./notifications.ts";
import { launcherPaths, writeJsonAtomically } from "./status.ts";

const riskPrompt = "本机 CDP 远程调试端口允许其他本地进程连接、读取或控制 OctoBuddy。仅继续表示你接受此风险；拒绝将不会启动 OctoBuddy。";

async function hasAcceptedConsent(path: string): Promise<boolean> {
  let contents: string;
  try {
    contents = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }

  try {
    const value: unknown = JSON.parse(contents);
    return Boolean(value && typeof value === "object" && "version" in value && "accepted" in value
      && value.version === 1 && value.accepted === true);
  } catch {
    return false;
  }
}

async function askForConsent(useNativeDialog: boolean): Promise<boolean> {
  if (useNativeDialog) return requestNativeCdpConsent();
  stdout.write(`${riskPrompt}\n继续启用 CDP？[y/N] `);
  const interface_ = createInterface({ input: stdin, output: stdout, terminal: Boolean(stdin.isTTY) });
  try {
    const answer = (await interface_.question("")).trim().toLowerCase();
    return answer === "y" || answer === "yes";
  } catch {
    return false;
  } finally {
    interface_.close();
  }
}

export async function requestCdpConsent(useNativeDialog: boolean, stateDir: string): Promise<boolean> {
  const paths = launcherPaths(stateDir);
  if (await hasAcceptedConsent(paths.consentFile)) return true;
  if (!(await askForConsent(useNativeDialog))) return false;
  await writeJsonAtomically(paths.consentFile, { version: 1, accepted: true, acceptedAt: new Date().toISOString() });
  return true;
}
