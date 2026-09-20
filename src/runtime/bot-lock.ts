import { createHash } from "node:crypto";
import { mkdir, open, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { asAppError } from "../errors.ts";

/**
 * 一个 Bot 身份同时只允许一个服务实例：锁文件写在临时目录，内容是本进程的 PID 与 Bot uid，
 * 进程已消失的锁视为空闲，因此崩溃过的实例不会挡住下一次启动。
 */
export type LockHandle = {
  path: string;
  release(): Promise<void>;
};

function lockFilePath(botUid: string): string {
  const digest = createHash("sha256").update(botUid, "utf8").digest("hex");
  return join(tmpdir(), "omp-channel-octo-locks", `${digest}.lock`);
}

async function processAlive(pid: number): Promise<boolean> {
  if (!Number.isSafeInteger(pid) || pid < 2) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const appError = asAppError(error);
    return appError.code === "EPERM";
  }
}

/**
 * Live holder of this Bot's lock, or `undefined` when the lock is free. A lock file whose process
 * is gone counts as free, so a crashed instance never blocks the next one.
 */
async function botLockHolder(botUid: string): Promise<{ path: string; pid?: number } | undefined> {
  const path = lockFilePath(botUid);
  let text: string;
  try {
    text = await Bun.file(path).text();
  } catch {
    return undefined;
  }
  let parsed: { pid?: unknown; botUid?: unknown } = {};
  try {
    parsed = JSON.parse(text) as { pid?: unknown; botUid?: unknown };
  } catch {
    parsed = {};
  }
  const pid = typeof parsed.pid === "number" ? parsed.pid : undefined;
  if (parsed.botUid === botUid && pid !== undefined && !(await processAlive(pid))) return undefined;
  return { path, ...(pid === undefined ? {} : { pid }) };
}

/**
 * Refuses to continue while another live process serves this Bot, and does so before the Bot is
 * registered: a second instance would otherwise re-register the Bot and cut off the WebSocket of
 * the instance that is already serving messages.
 */
export async function assertBotLockFree(botUid: string): Promise<void> {
  const holder = await botLockHolder(botUid);
  if (holder === undefined) return;
  const owner = holder.pid === undefined ? "" : `（PID ${holder.pid}）`;
  throw new Error(`同一 Bot 已有 channel 实例运行${owner}：一个 Bot 同时只服务一个实例，请先停止该实例再启动。`);
}

export async function acquireBotLock(botUid: string): Promise<LockHandle> {
  const path = lockFilePath(botUid);
  await mkdir(dirname(path), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const file = await open(path, "wx", 0o600);
      await file.writeFile(JSON.stringify({ pid: process.pid, botUid }), "utf8");
      await file.close();
      return {
        path,
        release: async () => {
          try {
            await unlink(path);
          } catch (error) {
            const appError = asAppError(error);
            if (appError.code !== "ENOENT") throw error;
          }
        },
      };
    } catch (error) {
      const appError = asAppError(error);
      if (appError.code !== "EEXIST" || attempt !== 0) {
        throw new Error("同一 Bot 已有 channel 实例运行");
      }
      if (await botLockHolder(botUid)) throw new Error("同一 Bot 已有 channel 实例运行");
      await unlink(path).catch(() => {});
    }
  }
  throw new Error("无法取得 Bot 独占锁");
}
