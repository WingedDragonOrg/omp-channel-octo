import { expect, test } from "bun:test";
import {
  extractReferenceCandidates,
  matchingReferenceCandidateUrls,
  sanitizeReferenceEvidence,
} from "../src/references/links.ts";
import type { ReferenceEvidence } from "../src/references/types.ts";

test("canonicalizes GitHub and nested GitLab references while stripping URL decoration", () => {
  const candidates = extractReferenceCandidates(
    "See [the PR](https://GitHub.com/Example/Service/pull/00017/files/?token=do-not-send#diff)； " +
      "and https://gitlab.example/Team/Platform/Service/-/merge_requests/00042/pipelines/?private_token=secret#jobs。",
  );

  expect(candidates.map(({ kind, url }) => ({ kind, url }))).toEqual([
    { kind: "pr", url: "https://github.com/example/service/pull/17" },
    { kind: "mr", url: "https://gitlab.example/Team/Platform/Service/-/merge_requests/42" },
  ]);
});

test("rejects credentials, unsupported paths, zero ids, and non-HTTP links", () => {
  const candidates = extractReferenceCandidates([
    "https://user:pass@github.com/example/service/pull/17",
    "https://@github.com/example/service/pull/17",
    "https://github.com/example/service/pull/0",
    "https://github.com/example/service/pull/17/discussions",
    "https://gitlab.example/team/service/-/merge_requests/9/notes",
    "ssh://github.com/example/service/pull/17",
    "#17",
  ].join(" "));

  expect(candidates).toEqual([]);
});

test("deduplicates canonical detail links, preserves distinct hosts and schemes, and caps candidates", () => {
  const links = [
    "https://github.com/Org/Repo/pull/17?view=1",
    "https://github.com/org/repo/pull/17/commits",
    "http://github.com/org/repo/pull/17",
    "https://github.com:8443/org/repo/pull/17",
    ...Array.from({ length: 8 }, (_, index) => `https://code-${index}.example/group/project/-/merge_requests/${index + 1}`),
  ];
  const candidates = extractReferenceCandidates(links.join("\n"));

  expect(candidates.map(({ url }) => url)).toEqual([
    "https://github.com/org/repo/pull/17",
    "http://github.com/org/repo/pull/17",
    "https://github.com:8443/org/repo/pull/17",
    "https://code-0.example/group/project/-/merge_requests/1",
    "https://code-1.example/group/project/-/merge_requests/2",
    "https://code-2.example/group/project/-/merge_requests/3",
    "https://code-3.example/group/project/-/merge_requests/4",
    "https://code-4.example/group/project/-/merge_requests/5",
  ]);
});

test("preserves owner and repository path case on self-hosted GitHub-compatible instances", () => {
  const candidates = extractReferenceCandidates(
    "https://git.example/Example/Service/pull/00017/files",
  );

  expect(candidates.map(({ url }) => url)).toEqual([
    "https://git.example/Example/Service/pull/17",
  ]);
});

test("candidate matching scans sanitized text and canonicalizes detail URLs", () => {
  const candidate = "https://git.example/acme/project/-/merge_requests/42";
  expect(matchingReferenceCandidateUrls(
    `https://git.example/acme/project/-/merge_requests/42/diffs?token=hidden`,
    new Set([candidate]),
  )).toEqual(new Set([candidate]));
  expect(matchingReferenceCandidateUrls(
    `ftp://files.example.invalid/${candidate}`,
    new Set([candidate]),
  )).toEqual(new Set());
});

test("trims long unmatched closing-parenthesis suffixes without losing the URL", () => {
  const candidate = "https://git.example/acme/project/-/merge_requests/42";
  const text = `${candidate}${")".repeat(10_000)}`;
  expect(extractReferenceCandidates(text).map(({ url }) => url)).toEqual([candidate]);
});

test("sanitizes every evidence field without flattening roles, quotes, or line breaks", () => {
  const evidence: ReferenceEvidence = {
    thread: "Review brief\nContext: authorization: Bearer fixture-token",
    task: "Please review https://github.com/acme/app/pull/17?token=private#discussion\napi_key=super-secret\nftp://example.test/report?tenant=314159#private-section",
    recent: [
      { role: "user", text: "> Quoted: https://gitlab.example/group/app/-/merge_requests/9?private_token=hidden#diff\nGH: ghp_abcdefghijklmnop\nftp://example.test/report?tenant=314159#private-section" },
      { role: "assistant", text: "https://person:password@github.com/acme/app/pull/18?token=secret\nftp://example.test/report?tenant=314159#private-section" },
    ],
    candidates: [{
      id: "candidate-1",
      kind: "pr",
      url: "https://github.com/acme/app/pull/17?token=private#discussion",
      excerpt: "Quoted PR: https://github.com/acme/app/pull/17?token=private#discussion\n> compare this one\nftp://example.test/report?tenant=314159#private-section",
    }],
  };

  const sanitized = sanitizeReferenceEvidence(evidence);
  const wireText = JSON.stringify(sanitized);

  expect(sanitized.recent.map(({ role }) => role)).toEqual(["user", "assistant"]);
  expect(sanitized.recent[0]?.text).toContain("> Quoted:");
  expect(sanitized.recent[0]?.text).toContain("\n");
  expect(sanitized.task).toContain("https://github.com/acme/app/pull/17");
  expect(sanitized.candidates[0]?.url).toBe("https://github.com/acme/app/pull/17");

  expect(wireText).not.toContain("314159");
  expect(wireText).not.toContain("private-section");
  expect(wireText).not.toContain("private");
  expect(wireText).not.toContain("hidden");
  expect(wireText).not.toContain("fixture-token");
  expect(wireText).not.toContain("super-secret");
  expect(wireText).not.toContain("abcdefghijklmnop");
  expect(wireText).not.toContain("person");
  expect(wireText).not.toContain("password");
  expect(wireText).not.toContain("discussion");
  expect(wireText).not.toContain("#diff");
});
