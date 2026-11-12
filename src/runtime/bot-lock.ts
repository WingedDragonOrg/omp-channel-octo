import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, readFile, stat, unlink } from "node:fs/promises";
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

type LockRecord = { pid?: unknown; botUid?: unknown; token?: unknown };

function lockFilePath(botUid: string): string {
  const digest = createHash("sha256").update(botUid, "utf8").digest("hex");
  return join(tmpdir(), "omp-channel-octo-locks", `${digest}.lock`);
}

async function processAlive(pid: number): Promise<boolean> {
  if (!Number.isSafeInteger(pid) || pid < 2) throw new Error("Bot 锁 PID 无效，无法确认持有者状态");
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = asAppError(error).code;
    if (code === "EPERM") return true;
    if (code === "ESRCH") return false;
    throw new Error("无法确认 Bot 锁持有者状态", { cause: error });
  }
}

/**
 * Live or unverifiable holder of this Bot's lock, or `undefined` when absent or verifiably stale.
 * Read, parse, and liveness uncertainty is never treated as an empty lock.
 */
async function botLockHolder(botUid: string): Promise<{ path: string; pid?: number } | undefined> {
  const path = lockFilePath(botUid);
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    if (asAppError(error).code === "ENOENT") return undefined;
    throw new Error("无法读取 Bot 独占锁，拒绝启动", { cause: error });
  }
  let parsed: LockRecord;
  try {
    parsed = JSON.parse(text) as LockRecord;
  } catch {
    return { path };
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

async function createBotLock(botUid: string, path: string): Promise<LockHandle> {
  const token = randomUUID();
  const file = await open(path, "wx", 0o600);
  try {
    await file.writeFile(JSON.stringify({ pid: process.pid, botUid, token }), "utf8");
    await file.sync();
  } catch (error) {
    await file.close().catch(() => {});
    await unlink(path).catch(() => {});
    throw error;
  }
  await file.close();
  return {
    path,
    release: async () => {
      try {
        const text = await readFile(path, "utf8");
        const current = JSON.parse(text) as LockRecord;
        if (current.pid !== process.pid || current.botUid !== botUid || current.token !== token) return;
        const info = await stat(path);
        if (!info.isFile()) return;
        await unlink(path);
      } catch (error) {
        const code = asAppError(error).code;
        if (code !== "ENOENT") throw error;
      }
    },
  };
}

/**
 * Migration cannot reclaim an existing file: legacy binaries may already have observed a stale
 * PID and be about to unlink that path. Exclusive creation is therefore the only accepted proof
 * that the migration owns the Bot lock; even stale or unreadable files fail closed.
 */
export async function acquireBotLockForMigration(botUid: string): Promise<LockHandle> {
  const path = lockFilePath(botUid);
  await mkdir(dirname(path), { recursive: true });
  try {
    return await createBotLock(botUid, path);
  } catch (error) {
    const code = asAppError(error).code;
    if (code === "EEXIST") {
      throw new Error("Bot 独占锁已存在；迁移拒绝回收或绕过该锁");
    }
    throw new Error("无法取得 Bot 迁移独占锁", { cause: error });
  }
}

export async function acquireBotLock(botUid: string): Promise<LockHandle> {
  const path = lockFilePath(botUid);
  await mkdir(dirname(path), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      return await createBotLock(botUid, path);
    } catch (error) {
      const appError = asAppError(error);
      if (appError.code !== "EEXIST" || attempt !== 0) {
        throw new Error("同一 Bot 已有 channel 实例运行");
      }
      if (await botLockHolder(botUid)) throw new Error("同一 Bot 已有 channel 实例运行");
      // Regular startup keeps its established dead-PID recovery behavior. Migration uses the
      // strict acquisition above because this legacy reclaim path cannot coordinate with older
      // binaries that already observed the stale file.
      try {
        await unlink(path);
      } catch (unlinkError) {
        if (asAppError(unlinkError).code !== "ENOENT") throw unlinkError;
      }
    }
  }
  throw new Error("无法取得 Bot 独占锁");
}
