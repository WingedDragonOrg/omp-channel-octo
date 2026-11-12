import { createHash, randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, link, mkdir, open, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, normalize } from "node:path";
import { Database } from "bun:sqlite";
import { acquireBotLockForMigration, type LockHandle } from "./bot-lock.ts";

const LEGACY_STATE_RELATIVE_PATH = join(".local", "state", "omp-channel-octo");
const TARGET_DATABASE_NAME = "channel.sqlite";
const JOURNAL_NAME = ".state-migration.json";
const TEMP_DATABASE_PREFIX = ".channel.sqlite-migration-";
const SHA256_PATTERN = /^[a-f0-9]{64}$/u;

export interface StateMigrationConfig {
  stateDir: string;
  stateDirIsDefault: boolean;
  homeDir?: string;
}

export interface StateMigrationLock {
  botUid: string;
  lock: LockHandle;
}

type MigrationJournal = {
  version: 1;
  phase: "pending" | "complete";
  botUid: string;
  sourceFingerprint: string;
  tempDatabase: string;
  snapshotSha256: string | null;
};

function migrationError(message: string, cause?: unknown): Error {
  return cause === undefined ? new Error(`状态迁移拒绝继续：${message}`) : new Error(`状态迁移拒绝继续：${message}`, { cause });
}

function sqlString(value: string): string {
  if (value.includes("\u0000")) throw migrationError("数据库路径包含 NUL 字符");
  return `'${value.replaceAll("'", "''")}'`;
}

function isMissing(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

async function regularFileExists(path: string): Promise<boolean> {
  try {
    const info = await lstat(path);
    if (!info.isFile() || info.isSymbolicLink()) throw migrationError(`预期普通文件：${path}`);
    return true;
  } catch (error) {
    if (isMissing(error)) return false;
    throw error;
  }
}

async function readBotUid(databasePath: string): Promise<string> {
  let database: Database;
  try {
    database = new Database(databasePath, { readonly: true });
  } catch (error) {
    throw migrationError("无法以只读方式打开旧 SQLite 数据库", error);
  }
  try {
    const row = database.query("SELECT value FROM meta WHERE key = 'bot_uid'").get() as { value?: unknown } | null;
    if (typeof row?.value !== "string" || row.value.trim().length === 0) {
      throw migrationError("旧 SQLite 数据库没有可验证的 meta.bot_uid");
    }
    return row.value;
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("状态迁移拒绝继续：")) throw error;
    throw migrationError("旧 SQLite 数据库缺少有效的 Bot 身份元数据", error);
  } finally {
    database.close();
  }
}

async function hashRegularFile(path: string): Promise<string> {
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink()) throw migrationError(`拒绝读取非普通数据库文件：${path}`);
  const hash = createHash("sha256");
  try {
    for await (const chunk of createReadStream(path)) hash.update(chunk as Uint8Array);
  } catch (error) {
    throw migrationError(`无法读取数据库文件：${path}`, error);
  }
  return hash.digest("hex");
}

async function sourceFingerprint(databasePath: string): Promise<string> {
  let database: Database;
  try {
    database = new Database(databasePath, { readonly: true });
  } catch (error) {
    throw migrationError("无法读取旧 SQLite 数据库的逻辑快照", error);
  }
  try {
    return createHash("sha256").update(database.serialize()).digest("hex");
  } catch (error) {
    throw migrationError("无法计算旧 SQLite 数据库的逻辑快照", error);
  } finally {
    database.close();
  }
}

function parseJournal(text: string): MigrationJournal {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (error) {
    throw migrationError("迁移记录损坏，拒绝覆盖目标数据库", error);
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw migrationError("迁移记录格式无效");
  }
  const candidate = value as Partial<MigrationJournal>;
  if (
    candidate.version !== 1
    || (candidate.phase !== "pending" && candidate.phase !== "complete")
    || typeof candidate.botUid !== "string"
    || candidate.botUid.trim().length === 0
    || typeof candidate.sourceFingerprint !== "string"
    || !SHA256_PATTERN.test(candidate.sourceFingerprint)
    || typeof candidate.tempDatabase !== "string"
    || !/^\.channel\.sqlite-migration-[a-f0-9-]+\.tmp$/u.test(candidate.tempDatabase)
    || !(candidate.snapshotSha256 === null || (typeof candidate.snapshotSha256 === "string" && SHA256_PATTERN.test(candidate.snapshotSha256)))
    || (candidate.phase === "complete" && candidate.snapshotSha256 === null)
  ) {
    throw migrationError("迁移记录字段无效，拒绝覆盖目标数据库");
  }
  return candidate as MigrationJournal;
}

async function readJournal(path: string): Promise<MigrationJournal | undefined> {
  try {
    const info = await lstat(path);
    if (!info.isFile() || info.isSymbolicLink()) throw migrationError("迁移记录不是普通文件");
    return parseJournal(await readFile(path, "utf8"));
  } catch (error) {
    if (isMissing(error)) return undefined;
    throw error;
  }
}

async function syncFile(path: string): Promise<void> {
  const file = await open(path, "r+");
  try {
    await file.sync();
  } finally {
    await file.close();
  }
}

async function writeJournal(path: string, journal: MigrationJournal, create: boolean): Promise<void> {
  const temporaryPath = join(
    normalize(join(path, "..")),
    `.state-migration-record-${randomUUID()}.tmp`,
  );
  try {
    await writeFile(temporaryPath, `${JSON.stringify(journal)}\n`, { encoding: "utf8", flag: "wx", mode: 0o600 });
    await syncFile(temporaryPath);
    if (create) {
      await link(temporaryPath, path);
      await unlink(temporaryPath);
    } else {
      await rename(temporaryPath, path);
    }
  } finally {
    await unlink(temporaryPath).catch(error => {
      if (!isMissing(error)) throw error;
    });
  }
}

async function removeOwnedTemp(path: string): Promise<void> {
  try {
    const info = await lstat(path);
    if (!info.isFile() || info.isSymbolicLink()) throw migrationError("迁移临时数据库不是普通文件");
    await unlink(path);
  } catch (error) {
    if (!isMissing(error)) throw error;
  }
}

async function validateDatabase(path: string, botUid: string, integrityCheck: boolean): Promise<void> {
  let database: Database;
  try {
    database = new Database(path, { readonly: true });
  } catch (error) {
    throw migrationError("迁移后的 SQLite 数据库无法打开", error);
  }
  try {
    const row = database.query("SELECT value FROM meta WHERE key = 'bot_uid'").get() as { value?: unknown } | null;
    if (row?.value !== botUid) throw migrationError("迁移后的数据库 Bot 身份与源数据不一致");
    if (integrityCheck) {
      const result = database.query("PRAGMA integrity_check").get() as Record<string, unknown> | null;
      if (result?.integrity_check !== "ok") throw migrationError("迁移后的 SQLite 数据库完整性检查失败");
    }
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("状态迁移拒绝继续：")) throw error;
    throw migrationError("迁移后的 SQLite 数据库校验失败", error);
  } finally {
    database.close();
  }
}

async function createSnapshot(sourcePath: string, tempPath: string, botUid: string): Promise<string> {
  await removeOwnedTemp(tempPath);
  let source: Database;
  try {
    source = new Database(sourcePath, { readonly: true });
  } catch (error) {
    throw migrationError("无法打开旧 SQLite 数据库进行一致性快照", error);
  }
  try {
    source.exec(`VACUUM INTO ${sqlString(tempPath)}`);
  } catch (error) {
    throw migrationError("SQLite 一致性快照失败（已提交 WAL 数据未迁移）", error);
  } finally {
    source.close();
  }
  await syncFile(tempPath);
  await validateDatabase(tempPath, botUid, true);
  return hashRegularFile(tempPath);
}

async function publishSnapshot(
  journalPath: string,
  targetPath: string,
  tempPath: string,
  sourcePath: string,
  targetDir: string,
  journal: MigrationJournal,
): Promise<void> {
  let targetExists = await regularFileExists(targetPath);
  if (targetExists) {
    if (journal.snapshotSha256 === null || await hashRegularFile(targetPath) !== journal.snapshotSha256) {
      throw migrationError("目标状态数据库已存在且无法证明属于本次迁移，拒绝覆盖或合并");
    }
    await validateDatabase(targetPath, journal.botUid, false);
    if (journal.phase !== "complete") {
      await writeJournal(journalPath, { ...journal, phase: "complete" }, false);
    }
    await removeOwnedTemp(tempPath);
    return;
  }

  let tempMatches = false;
  if (await regularFileExists(tempPath)) {
    tempMatches = journal.snapshotSha256 !== null && await hashRegularFile(tempPath) === journal.snapshotSha256;
  }
  if (!tempMatches) {
    await removeOwnedTemp(tempPath);
    const before = await sourceFingerprint(sourcePath);
    if (before !== journal.sourceFingerprint) {
      throw migrationError("旧数据库在迁移中发生变化，拒绝发布不完整快照");
    }
    const snapshotHash = await createSnapshot(sourcePath, tempPath, journal.botUid);
    const after = await sourceFingerprint(sourcePath);
    if (after !== journal.sourceFingerprint) {
      await removeOwnedTemp(tempPath);
      throw migrationError("旧数据库在快照期间发生变化，拒绝发布不完整快照");
    }
    journal = { ...journal, snapshotSha256: snapshotHash };
    await writeJournal(journalPath, journal, false);
  }

  try {
    await link(tempPath, targetPath);
  } catch (error) {
    if (isMissing(error)) throw migrationError("目标状态目录在迁移期间消失", error);
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      targetExists = await regularFileExists(targetPath);
      if (!targetExists || journal.snapshotSha256 === null || await hashRegularFile(targetPath) !== journal.snapshotSha256) {
        throw migrationError("目标状态数据库在迁移期间被其他数据占用，拒绝覆盖");
      }
    } else {
      throw migrationError("无法以原子且不覆盖的方式发布迁移数据库", error);
    }
  }
  await validateDatabase(targetPath, journal.botUid, true);
  await writeJournal(journalPath, { ...journal, phase: "complete" }, false);
  await removeOwnedTemp(tempPath);
  await syncFile(join(targetDir, TARGET_DATABASE_NAME));
}

/**
 * Copy only the legacy SQLite database to the default state directory. The source remains in place;
 * VACUUM INTO takes a consistent SQLite snapshot including committed WAL frames. The returned Bot
 * lock remains held so the caller can transfer it directly to CoreService before registration.
 */
export async function prepareStateMigration(config: StateMigrationConfig): Promise<StateMigrationLock | undefined> {
  if (!config.stateDirIsDefault) return undefined;
  const home = normalize(config.homeDir ?? homedir());
  if (!isAbsolute(home) || !isAbsolute(config.stateDir)) throw migrationError("homeDir 与 stateDir 必须是绝对路径");
  const expectedTarget = normalize(join(home, ".oco", "state"));
  if (normalize(config.stateDir) !== expectedTarget) throw migrationError("默认 stateDir 与 homeDir 不一致，拒绝迁移到其他目录");

  const legacyDir = join(home, LEGACY_STATE_RELATIVE_PATH);
  const sourcePath = join(legacyDir, TARGET_DATABASE_NAME);
  const targetDir = expectedTarget;
  const targetPath = join(targetDir, TARGET_DATABASE_NAME);
  const journalPath = join(targetDir, JOURNAL_NAME);
  const journal = await readJournal(journalPath);
  const sourceExists = await regularFileExists(sourcePath);

  if (journal === undefined && !sourceExists) return undefined;
  if (journal === undefined && await regularFileExists(targetPath)) {
    throw migrationError("目标状态数据库已存在且没有迁移记录，拒绝覆盖或合并");
  }

  let botUid: string;
  if (journal !== undefined) {
    botUid = journal.botUid;
  } else {
    botUid = await readBotUid(sourcePath);
  }

  const lock = await acquireBotLockForMigration(botUid);
  try {
    const latestJournal = await readJournal(journalPath);
    if (journal === undefined && latestJournal !== undefined) {
      throw migrationError("迁移记录在获取锁期间出现，请重新启动以安全恢复");
    }
    if (journal !== undefined && latestJournal?.sourceFingerprint !== journal.sourceFingerprint) {
      throw migrationError("迁移记录在获取锁期间发生变化");
    }

    if (latestJournal?.phase === "complete") {
      if (!(await regularFileExists(targetPath))) throw migrationError("迁移记录存在但目标数据库缺失");
      await validateDatabase(targetPath, botUid, false);
      if (sourceExists) {
        if (await readBotUid(sourcePath) !== botUid) throw migrationError("源数据库 Bot 身份与迁移记录不一致");
        if (await sourceFingerprint(sourcePath) !== latestJournal.sourceFingerprint) {
          throw migrationError("旧数据库在迁移后发生变化；拒绝覆盖或合并");
        }
      }
      return { botUid, lock };
    }

    if (!(await regularFileExists(sourcePath))) throw migrationError("迁移尚未完成但旧数据库已不存在");
    if (await readBotUid(sourcePath) !== botUid) throw migrationError("旧数据库 Bot 身份与迁移锁不一致");
    const fingerprint = await sourceFingerprint(sourcePath);
    if (latestJournal && fingerprint !== latestJournal.sourceFingerprint) {
      throw migrationError("旧数据库在中断期间发生变化，拒绝从不确定快照继续");
    }

    await mkdir(targetDir, { recursive: true, mode: 0o700 });
    let pending = latestJournal;
    if (pending === undefined) {
      if (await regularFileExists(targetPath)) throw migrationError("目标状态数据库已存在且无法证明属于本次迁移");
      pending = {
        version: 1,
        phase: "pending",
        botUid,
        sourceFingerprint: fingerprint,
        tempDatabase: `${TEMP_DATABASE_PREFIX}${randomUUID()}.tmp`,
        snapshotSha256: null,
      };
      await writeJournal(journalPath, pending, true);
    }
    const tempPath = join(targetDir, pending.tempDatabase);
    await publishSnapshot(journalPath, targetPath, tempPath, sourcePath, targetDir, pending);
    return { botUid, lock };
  } catch (error) {
    await lock.release().catch(() => {});
    throw error;
  }
}
