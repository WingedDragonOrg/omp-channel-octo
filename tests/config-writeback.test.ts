import { expect, test } from "bun:test";
import { chmod, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAllowedGroupId, createAllowlistWriteback } from "../src/runtime/config-writeback";

async function writeConfig(directory: string, document: unknown, mode = 0o600): Promise<string> {
  const path = join(directory, "config.json");
  await writeFile(path, `${JSON.stringify(document, null, 2)}\n`, { encoding: "utf8", mode });
  await chmod(path, mode);
  return path;
}

async function readDocument(path: string): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
}

test("appends one id while preserving every other field, the mode, and the directory", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-writeback-"));
  try {
    const path = await writeConfig(directory, {
      apiUrl: "https://im.example.test",
      allowedGroupIds: ["group-a"],
      ompCommand: "omp",
      attachments: { enabled: true },
    });
    await appendAllowedGroupId(path, "group-b");

    expect(await readDocument(path)).toEqual({
      apiUrl: "https://im.example.test",
      allowedGroupIds: ["group-a", "group-b"],
      ompCommand: "omp",
      attachments: { enabled: true },
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    // The atomic write leaves nothing behind beside the config file itself.
    expect(await readdir(directory)).toEqual(["config.json"]);

    // Idempotent: the id is never appended twice.
    await appendAllowedGroupId(path, "group-b");
    expect((await readDocument(path)).allowedGroupIds).toEqual(["group-a", "group-b"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("creates the allowlist when absent and serializes concurrent appends", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-writeback-"));
  try {
    const path = await writeConfig(directory, { apiUrl: "https://im.example.test" }, 0o640);
    const writeback = createAllowlistWriteback(path);
    await Promise.all([writeback.append("group-1"), writeback.append("group-2")]);

    expect(await readDocument(path)).toEqual({
      apiUrl: "https://im.example.test",
      allowedGroupIds: ["group-1", "group-2"],
    });
    expect((await stat(path)).mode & 0o777).toBe(0o640);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("refuses to rewrite a file it cannot parse or whose allowlist is not an array", async () => {
  const directory = await mkdtemp(join(tmpdir(), "oco-writeback-"));
  try {
    const path = join(directory, "config.json");
    await writeFile(path, "{ not json", { encoding: "utf8", mode: 0o600 });
    await expect(appendAllowedGroupId(path, "group-x")).rejects.toThrow();
    expect(await readFile(path, "utf8")).toBe("{ not json");

    await writeConfig(directory, { allowedGroupIds: "group-a" });
    await expect(appendAllowedGroupId(path, "group-x")).rejects.toThrow();
    expect((await readDocument(path)).allowedGroupIds).toBe("group-a");

    await rm(path);
    await expect(appendAllowedGroupId(path, "group-x")).rejects.toThrow();
    expect(await readdir(directory)).toEqual([]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
