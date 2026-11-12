import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, stat, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireBotLock, acquireBotLockForMigration } from "../src/runtime/bot-lock.ts";
import { prepareStateMigration, type StateMigrationLock } from "../src/runtime/state-migration.ts";

const BOT_UID = `state-migration-${process.pid}`;

async function withHome(run: (home: string) => Promise<void>): Promise<void> {
  const home = await mkdtemp(join(tmpdir(), "oco-state-migration-"));
  try {
    await run(home);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
}

function legacyPaths(home: string): { legacyDir: string; sourceDb: string; stateDir: string; targetDb: string } {
  const legacyDir = join(home, ".local", "state", "omp-channel-octo");
  const stateDir = join(home, ".oco", "state");
  return { legacyDir, sourceDb: join(legacyDir, "channel.sqlite"), stateDir, targetDb: join(stateDir, "channel.sqlite") };
}

async function createLegacyDatabase(path: string, botUid = BOT_UID): Promise<Database> {
  await mkdir(join(path, ".."), { recursive: true });
  const db = new Database(path);
  db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;");
  db.exec("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);");
  db.query("INSERT INTO meta(key, value) VALUES('bot_uid', ?)").run(botUid);
  db.exec("CREATE TABLE records(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);");
  db.query("INSERT INTO records(payload) VALUES(?)").run("committed in WAL");
  return db;
}

async function release(lock: { release(): Promise<void> } | undefined): Promise<void> {
  if (lock) await lock.release();
}

test("migrates committed WAL rows without changing stored paths, and repeats without overwriting new state", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    const source = await createLegacyDatabase(paths.sourceDb);
    const storedPath = join(home, "sessions", "session.jsonl");
    source.query("CREATE TABLE paths(value TEXT NOT NULL)").run();
    source.query("INSERT INTO paths(value) VALUES(?)").run(storedPath);
    await expect(stat(join(paths.legacyDir, "channel.sqlite-wal"))).resolves.toBeDefined();

    let first: StateMigrationLock | undefined;
    try {
      first = await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home });
      expect(first?.botUid).toBe(BOT_UID);
      const target = new Database(paths.targetDb, { readonly: true });
      try {
        expect(target.query("SELECT payload FROM records").get()).toEqual({ payload: "committed in WAL" });
        expect(target.query("SELECT value FROM paths").get()).toEqual({ value: storedPath });
      } finally {
        target.close();
      }
    } finally {
      await release(first?.lock);
    }
    // Closing a WAL connection may checkpoint/truncate the physical files without changing rows.
    source.close();

    const writer = new Database(paths.targetDb);
    writer.query("INSERT INTO records(payload) VALUES(?)").run("new target state");
    writer.close();

    let second: StateMigrationLock | undefined;
    try {
      second = await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home });
      expect(second?.botUid).toBe(BOT_UID);
      const target = new Database(paths.targetDb, { readonly: true });
      try {
        expect(target.query("SELECT payload FROM records ORDER BY id").all()).toEqual([
          { payload: "committed in WAL" },
          { payload: "new target state" },
        ]);
      } finally {
        target.close();
      }
    } finally {
      await release(second?.lock);
      
    }
  });
});

test("does not trigger migration for explicit state directories or a missing legacy database", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    expect(await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: false, homeDir: home })).toBeUndefined();
    expect(await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home })).toBeUndefined();
  });
});

test("refuses to overwrite an independent target database", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    const source = await createLegacyDatabase(paths.sourceDb);
    await mkdir(paths.stateDir, { recursive: true });
    const target = new Database(paths.targetDb);
    target.exec("CREATE TABLE independent(value TEXT NOT NULL);");
    target.query("INSERT INTO independent(value) VALUES('keep')").run();
    target.close();
    try {
      await expect(prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home }))
        .rejects.toThrow(/目标状态数据库|冲突/u);
      const existing = new Database(paths.targetDb, { readonly: true });
      try {
        expect(existing.query("SELECT value FROM independent").get()).toEqual({ value: "keep" });
      } finally {
        existing.close();
      }
    } finally {
      source.close();
    }
  });
});

test("refuses migration when the legacy database has no persisted bot uid", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    const source = new Database(paths.sourceDb);
    source.exec("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);");
    source.exec("CREATE TABLE records(value TEXT);");
    source.close();
    await expect(prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home }))
      .rejects.toThrow(/bot_uid|身份/u);
  });
});

test("refuses migration while the same bot lock is held", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    const source = await createLegacyDatabase(paths.sourceDb);
    const active = await acquireBotLock(BOT_UID);
    try {
      await expect(prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home }))
        .rejects.toThrow(/锁|实例/u);
    } finally {
      await active.release();
      source.close();
    }
  });
});

test("migration lock acquisition fails closed for a stale lock", async () => {
  const lock = await acquireBotLock(BOT_UID);
  const path = lock.path;
  await lock.release();
  await writeFile(path, JSON.stringify({ pid: Number.MAX_SAFE_INTEGER, botUid: BOT_UID }), { mode: 0o600 });
  try {
    await expect(acquireBotLockForMigration(BOT_UID)).rejects.toThrow(/锁|实例/u);
  } finally {
    await unlink(path);
  }
});

test("recovers an interrupted publish when the target matches the journaled snapshot", async () => {
  await withHome(async home => {
    const paths = legacyPaths(home);
    const source = await createLegacyDatabase(paths.sourceDb);
    await expect(stat(join(paths.legacyDir, "channel.sqlite-wal"))).resolves.toBeDefined();
    let original: StateMigrationLock | undefined;
    try {
      original = await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home });
    } finally {
      await release(original?.lock);
    }

    const markerPath = join(paths.stateDir, ".state-migration.json");
    const marker = JSON.parse(await readFile(markerPath, "utf8")) as { phase: string };
    marker.phase = "pending";
    await writeFile(markerPath, `${JSON.stringify(marker)}\n`, { mode: 0o600 });
    const snapshotHash = createHash("sha256").update(await readFile(paths.targetDb)).digest("hex");

    const recovered = await prepareStateMigration({ stateDir: paths.stateDir, stateDirIsDefault: true, homeDir: home });
    try {
      expect(recovered?.botUid).toBe(BOT_UID);
      const completed = JSON.parse(await readFile(markerPath, "utf8")) as { phase: string; snapshotSha256: string };
      expect(completed.phase).toBe("complete");
      expect(completed.snapshotSha256).toBe(snapshotHash);
    } finally {
      await release(recovered?.lock);
      source.close();
    }
  });
});
