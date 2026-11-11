import { expect, test } from "bun:test";
import {
  createFileLinkStream,
  resolveMarkdownFileLinks,
  splitUtf8WithProtectedMarkdownLinks,
  ZedLocalFileLinkResolver,
  ZedSshFileLinkResolver,
  type FileLinkResolution,
  type FileLinkResolver,
} from "../src/file-links.ts";

const cwd = "/Users/dev/workspace";

function expectResolved(
  result: FileLinkResolution | Promise<FileLinkResolution>,
): asserts result is FileLinkResolution {
  if (result instanceof Promise) throw new Error("expected the Zed resolver to complete synchronously");
}


test("Zed resolvers rewrite Markdown file destinations but leave URLs and code untouched", () => {
  const text = [
    "Local: [guide](docs/guide%20one.md)",
    "Remote: [notes](/srv/shared/meeting.md)",
    "Web: [help](https://example.com/guide)",
    "Inline: `[snippet](docs/inline.md)`",
    "```md",
    "[example](docs/example.md)",
    "```",
    "    [indented](docs/indented.md)",
  ].join("\n");
  const result = resolveMarkdownFileLinks(text, cwd, [
    new ZedLocalFileLinkResolver(),
  ], true);
  expectResolved(result);
  expect(result.text).toBe([
    "Local: [guide](zed://file/Users/dev/workspace/docs/guide%20one.md)",
    "Remote: [notes](zed://file/srv/shared/meeting.md)",
    "Web: [help](https://example.com/guide)",
    "Inline: `[snippet](docs/inline.md)`",
    "```md",
    "[example](docs/example.md)",
    "```",
    "    [indented](docs/indented.md)",
  ].join("\n"));
});

test("SSH links encode the remote path and include the configured authority", () => {
  const result = resolveMarkdownFileLinks(
    "Open [notes](<reports/weekly notes.md>)",
    cwd,
    [new ZedSshFileLinkResolver({
      type: "zed",
      mode: "ssh",
      hostname: "my-mini",
      username: "dev",
      port: 2222,
    })],
    true,
  );
  expectResolved(result);
  expect(result.text).toBe(
    "Open [notes](<zed://ssh/dev@my-mini:2222/Users/dev/workspace/reports/weekly%20notes.md>)",
  );
});

test("a non-final incomplete link tail is withheld, then a final incomplete tail stays literal", () => {
  const resolver = new ZedLocalFileLinkResolver();
  const partial = resolveMarkdownFileLinks("Read: [guide](docs/gui", cwd, [resolver], false);
  expectResolved(partial);
  expect(partial.text).toBe("Read: ");

  const final = resolveMarkdownFileLinks("Read: [guide](docs/gui", cwd, [resolver], true);
  expectResolved(final);
  expect(final.text).toBe("Read: [guide](docs/gui");
});

test("a converted Markdown link that crosses a 3000-byte cut is moved whole to the next segment", () => {
  const result = resolveMarkdownFileLinks(
    `${"a".repeat(2_990)} [guide](docs/guide.md)`,
    cwd,
    [new ZedLocalFileLinkResolver()],
    true,
  );
  expectResolved(result);
  const chunks = splitUtf8WithProtectedMarkdownLinks(result.text, result.protectedHrefs);
  expect(chunks.map((chunk) => Buffer.byteLength(chunk, "utf8")).every((size) => size <= 3_000)).toBe(true);
  expect(chunks).toEqual([
    "a".repeat(2_990) + " ",
    "[guide](zed://file/Users/dev/workspace/docs/guide.md)",
  ]);
});

test("async final projection does not wait for a stale resolver and stale results cannot overwrite it", async () => {
  const oldResolution = Promise.withResolvers<{ text: string; protectedHrefs: string[] }>();
  const oldStarted = Promise.withResolvers<void>();
  const commits: string[] = [];
  const stream = createFileLinkStream(
    (text) => {
      if (text !== "old snapshot") return { text, protectedHrefs: [] };
      oldStarted.resolve();
      return oldResolution.promise;
    },
    (resolved) => commits.push(resolved.text),
  );

  const old = stream.enqueue("old snapshot", false);
  await oldStarted.promise;
  const final = stream.enqueue("authoritative final", true);
  await final;
  await stream.enqueue("late non-final snapshot", false);
  expect(commits).toEqual(["authoritative final"]);
  oldResolution.resolve({ text: "stale resolved text", protectedHrefs: [] });
  await old;

  expect(commits).toEqual(["authoritative final"]);
});

test("stream settlement propagates final projection persistence failures", async () => {
  const stream = createFileLinkStream(
    (text) => ({ text, protectedHrefs: [] }),
    () => {
      throw new Error("fixture projection persistence failure");
    },
  );
  await stream.enqueue("final body", true).catch(() => {});
  await expect(stream.settle()).rejects.toThrow("fixture projection persistence failure");
});

test("local and SSH Zed resolvers each reject ordinary web URLs", () => {
  const local = new ZedLocalFileLinkResolver();
  const ssh = new ZedSshFileLinkResolver({ type: "zed", mode: "ssh", hostname: "my-mini" });
  expect(local.isMatch("docs/guide.md")).toBe(true);
  expect(ssh.isMatch("docs/guide.md")).toBe(true);
  expect(local.isMatch("https://example.com/guide")).toBe(false);
  expect(ssh.isMatch("https://example.com/guide")).toBe(false);
});

test("a converted link too large for one message remains in its original form", () => {
  const original = `[guide](${"a".repeat(2_990)})`;
  const result = resolveMarkdownFileLinks(original, cwd, [new ZedLocalFileLinkResolver()], true);
  expectResolved(result);
  expect(result.text).toBe(original);
  const chunks = splitUtf8WithProtectedMarkdownLinks(result.text, result.protectedHrefs);
  expect(chunks.join("")).toBe(original);
  expect(chunks.every((chunk) => Buffer.byteLength(chunk, "utf8") <= 3_000)).toBe(true);
});

test("a failing first matching resolver keeps the source instead of falling through", async () => {
  let laterResolutions = 0;
  const result = await resolveMarkdownFileLinks(
    "[guide](docs/guide.md)",
    cwd,
    [
      { isMatch: () => true, resolve: () => { throw new Error("first resolver failed"); } },
      {
        isMatch: () => true,
        resolve: () => {
          laterResolutions += 1;
          return "zed://file/should-not-be-used.md";
        },
      },
    ],
    true,
  );
  expect(result.text).toBe("[guide](docs/guide.md)");
  expect(laterResolutions).toBe(0);
});

test("async resolver failure leaves the original link intact", async () => {
  const failingResolver: FileLinkResolver = {
    isMatch: () => true,
    resolve: async () => {
      throw new Error("fixture resolver failed");
    },
  };
  const result = await resolveMarkdownFileLinks("[guide](docs/guide.md)", cwd, [failingResolver], true);
  expect(result.text).toBe("[guide](docs/guide.md)");
  expect(result.protectedHrefs).toEqual([]);
});
