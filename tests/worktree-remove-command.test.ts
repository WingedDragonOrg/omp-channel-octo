import { expect, test } from "bun:test";
import { existsSync, lstatSync } from "node:fs";
import { chmod, mkdtemp, mkdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  WorktreeMergeProcessIdentity,
  WorktreePort,
  WorktreeRemovalLifecycle,
  WorktreeRemovalStep,
} from "../src/contracts.ts";
import { createWorktreePort } from "../src/workspace/worktree.ts";


interface GitResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

async function runGitResult(directory: string, args: string[]): Promise<GitResult> {
  const child = Bun.spawn(["git", "-C", directory, ...args], {
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { stdout: stdout.trim(), stderr: stderr.trim(), exitCode };
}

async function runGit(directory: string, args: string[]): Promise<string> {
  const result = await runGitResult(directory, args);
  if (result.exitCode !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
  }
  return result.stdout;
}

async function hasBranch(repo: string, branch: string): Promise<boolean> {
  const result = await runGitResult(repo, ["show-ref", "--verify", "--quiet", `refs/heads/${branch}`]);
  if (result.exitCode === 0) return true;
  if (result.exitCode === 1) return false;
  throw new Error(`git show-ref failed: ${result.stderr}`);
}

async function createRepo(root: string): Promise<string> {
  const repo = join(root, "project");
  await mkdir(repo);
  await writeFile(join(repo, "file.txt"), "initial\n", "utf8");
  await runGit(repo, ["init", "-q", "-b", "main"]);
  await runGit(repo, ["add", "."]);
  await runGit(repo, [
    "-c",
    "user.email=test@example.com",
    "-c",
    "user.name=Fixture",
    "commit",
    "-q",
    "-m",
    "initial",
  ]);
  return repo;
}

async function addWorktree(repo: string, root: string, name: string, branch: string): Promise<string> {
  const path = join(root, name);
  await runGit(repo, ["worktree", "add", "-q", "-b", branch, path]);
  return path;
}

async function commitFile(path: string, name: string, contents: string, message: string): Promise<void> {
  await writeFile(join(path, name), contents, "utf8");
  await runGit(path, ["add", name]);
  await runGit(path, [
    "-c",
    "user.email=test@example.com",
    "-c",
    "user.name=Fixture",
    "commit",
    "-q",
    "-m",
    message,
  ]);
}

async function withRepository(run: (root: string, repo: string) => Promise<void>): Promise<void> {
  const root = await realpath(await mkdtemp(join(tmpdir(), "omp-thread-remove-")));
  try {
    await run(root, await createRepo(root));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function gitFallbackPort(root: string): WorktreePort {
  return createWorktreePort({
    command: "/nonexistent/omp",
    worktrunkCandidates: [join(root, "no-worktrunk-installed")],
  });
}

async function writeStub(root: string, body: string): Promise<string> {
  const path = join(root, "wt-stub");
  await writeFile(path, `#!/bin/sh\n${body}\n`, "utf8");
  await chmod(path, 0o755);
  return path;
}

const WT_REMOVE_STUB = String.raw`if [ "$1" = "--version" ]; then
  printf 'wt v0.80.0\n'
  exit 0
fi
[ "$1" = "remove" ] || exit 2
shift
force=0
keep_branch=0
worktree=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -f) force=1 ;;
    --no-delete-branch) keep_branch=1 ;;
    -*) ;;
    *) worktree="$1" ;;
  esac
  shift
done
[ "$keep_branch" -eq 1 ] || exit 3
[ -n "$worktree" ] || exit 2
if [ "$force" -eq 1 ]; then
  git -C "$worktree" worktree remove --force "$worktree" || exit $?
else
  git -C "$worktree" worktree remove "$worktree" || exit $?
fi
printf '[{"branch_outcome":"kept"}]\n'`;

test("default branch deletion refuses an unmerged branch and preserves its worktree and ref", async () => {
  await withRepository(async (root, repo) => {
    const branch = "unmerged-feature";
    const worktree = await addWorktree(repo, root, "unmerged-worktree", branch);
    await commitFile(worktree, "feature.txt", "unmerged change\n", "unmerged change");
    const mainHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    const port = gitFallbackPort(root);

    const rejected = await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
    }).then(
      () => false,
      () => true,
    );
    const actual = {
      rejected,
      worktreeExists: existsSync(worktree),
      branchExists: await hasBranch(repo, branch),
      mainHead: await runGit(repo, ["rev-parse", "refs/heads/main"]),
    };

    expect(actual).toEqual({
      rejected: true,
      worktreeExists: true,
      branchExists: true,
      mainHead,
    });
  });
});

test("default branch deletion refuses a dirty worktree without dropping its untracked file or branch", async () => {
  await withRepository(async (root, repo) => {
    const branch = "dirty-feature";
    const worktree = await addWorktree(repo, root, "dirty-worktree", branch);
    const dirtyFile = join(worktree, "untracked.txt");
    await writeFile(dirtyFile, "keep this\n", "utf8");
    const port = gitFallbackPort(root);

    const rejected = await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
    }).then(
      () => false,
      () => true,
    );

    expect({
      rejected,
      worktreeExists: existsSync(worktree),
      dirtyFile: existsSync(dirtyFile) ? await readFile(dirtyFile, "utf8") : null,
      branchExists: await hasBranch(repo, branch),
    }).toEqual({
      rejected: true,
      worktreeExists: true,
      dirtyFile: "keep this\n",
      branchExists: true,
    });
  });
});

test("default removal deletes a merged branch and worktree while preserving main HEAD and tracking each process", async () => {
  await withRepository(async (root, repo) => {
    const branch = "merged-feature";
    const worktree = await addWorktree(repo, root, "merged-worktree", branch);
    await runGit(repo, ["branch", "development"]);
    await runGit(repo, ["config", "--local", "worktrunk.default-branch", "main"]);
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    await runGit(repo, ["switch", "-q", "development"]);
    const mainHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    const wt = await writeStub(root, WT_REMOVE_STUB);
    const port = createWorktreePort({
      command: "/nonexistent/omp",
      worktrunkCandidates: [wt],
    });
    const lifecycleEvents: string[] = [];
    const processIdentities: WorktreeMergeProcessIdentity[] = [];
    const lifecycle = (step: WorktreeRemovalStep): WorktreeRemovalLifecycle => {
      lifecycleEvents.push(`factory:${step}`);
      return {
        beforeSpawn: () => lifecycleEvents.push(`beforeSpawn:${step}`),
        onSpawn: (identity) => {
          processIdentities.push(identity);
          lifecycleEvents.push(`onSpawn:${step}`);
        },
        onConfirmedExit: () => lifecycleEvents.push(`onConfirmedExit:${step}`),
        onNotSpawned: () => lifecycleEvents.push(`onNotSpawned:${step}`),
        onUnknown: () => lifecycleEvents.push(`onUnknown:${step}`),
      };
    };

    await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
      lifecycle,
    });

    expect({
      worktreeExists: existsSync(worktree),
      branchExists: await hasBranch(repo, branch),
      mainHead: await runGit(repo, ["rev-parse", "refs/heads/main"]),
    }).toEqual({ worktreeExists: false, branchExists: false, mainHead });
    expect(lifecycleEvents).toEqual([
      "factory:worktree",
      "beforeSpawn:worktree",
      "onSpawn:worktree",
      "onConfirmedExit:worktree",
      "factory:branch",
      "beforeSpawn:branch",
      "onSpawn:branch",
      "onConfirmedExit:branch",
    ]);
    expect(
      processIdentities.length === 2 &&
        processIdentities.every(
          (identity) =>
            Number.isInteger(identity.pid) &&
            identity.pid > 0 &&
            Number.isInteger(identity.pgid) &&
            identity.pgid > 0 &&
            identity.identity.length > 0,
        ),
    ).toBe(true);
    expect(processIdentities.map((identity) => identity.cwd)).toEqual([worktree, repo]);
  });
});

test("cached remote HEAD identifies the default branch without consulting the current checkout", async () => {
  await withRepository(async (root, repo) => {
    const branch = "remote-default-feature";
    const worktree = await addWorktree(repo, root, "remote-default-worktree", branch);
    await commitFile(worktree, "feature.txt", "merged into stable\n", "merged into stable");
    await runGit(repo, ["switch", "-q", "-c", "stable"]);
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    await runGit(repo, ["switch", "-q", "main"]);
    const mainHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    const stableHead = await runGit(repo, ["rev-parse", "refs/heads/stable"]);
    await runGit(repo, ["config", "remote.origin.url", "https://example.invalid/project.git"]);
    await runGit(repo, ["update-ref", "refs/remotes/origin/stable", stableHead]);
    await runGit(repo, ["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/stable"]);
    const port = gitFallbackPort(root);

    await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
    });

    expect({
      worktreeExists: existsSync(worktree),
      branchExists: await hasBranch(repo, branch),
      mainHead: await runGit(repo, ["rev-parse", "refs/heads/main"]),
    }).toEqual({ worktreeExists: false, branchExists: false, mainHead });
  });
});

test("branch compare-and-delete reports a moved ref while preserving its new value", async () => {
  await withRepository(async (root, repo) => {
    const branch = "moving-feature";
    const worktree = await addWorktree(repo, root, "moving-worktree", branch);
    await commitFile(worktree, "feature.txt", "merged change\n", "merged change");
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    const originalBranchHead = await runGit(repo, ["rev-parse", `refs/heads/${branch}`]);
    await runGit(repo, [
      "-c",
      "user.email=test@example.com",
      "-c",
      "user.name=Fixture",
      "commit",
      "--allow-empty",
      "-q",
      "-m",
      "advance main after merge",
    ]);
    const replacementHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    let branchMoved = false;
    const port = gitFallbackPort(root);
    const lifecycle = (step: WorktreeRemovalStep): WorktreeRemovalLifecycle => ({
      beforeSpawn: () => {
        if (step !== "branch" || branchMoved) return;
        const result = Bun.spawnSync(
          ["git", "-C", repo, "branch", "--force", branch, replacementHead],
          { stdin: "ignore", stdout: "ignore", stderr: "ignore" },
        );
        if (result.exitCode !== 0) throw new Error("failed to move branch in the removal race fixture");
        branchMoved = true;
      },
      onSpawn: () => {},
      onConfirmedExit: () => {},
      onNotSpawned: () => {},
      onUnknown: () => {},
    });

    const result = await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
      lifecycle,
    }).then(
      () => ({ rejected: false, message: "" }),
      (error: unknown) => ({
        rejected: true,
        message: error instanceof Error ? error.message : String(error),
      }),
    );
    const branchHead = (await hasBranch(repo, branch))
      ? await runGit(repo, ["rev-parse", `refs/heads/${branch}`])
      : null;

    expect({
      rejected: result.rejected,
      diagnosticIncludesBranch: result.message.includes(branch),
      branchMoved,
      worktreeExists: existsSync(worktree),
      branchHead,
    }).toEqual({
      rejected: true,
      diagnosticIncludesBranch: true,
      branchMoved: true,
      worktreeExists: false,
      branchHead: replacementHead,
    });
    expect(replacementHead).not.toBe(originalBranchHead);
  });
});

test("branch ref lock failure surfaces Git diagnostics and keeps the residual branch", async () => {
  await withRepository(async (root, repo) => {
    const branch = "locked-feature";
    const worktree = await addWorktree(repo, root, "locked-worktree", branch);
    await commitFile(worktree, "feature.txt", "merged change\n", "merged change");
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    const lockPath = join(repo, ".git", "refs", "heads", `${branch}.lock`);
    const lockContents = "held by another process\n";
    await writeFile(lockPath, lockContents, "utf8");
    const port = gitFallbackPort(root);

    const result = await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
    }).then(
      () => ({ rejected: false, message: "" }),
      (error: unknown) => ({
        rejected: true,
        message: error instanceof Error ? error.message : String(error),
      }),
    );

    expect({
      rejected: result.rejected,
      diagnosticIncludesBranch: result.message.includes(branch),
      diagnosticIncludesLock: result.message.toLowerCase().includes("lock"),
      worktreeExists: existsSync(worktree),
      branchExists: await hasBranch(repo, branch),
      lockContents: await readFile(lockPath, "utf8"),
    }).toEqual({
      rejected: true,
      diagnosticIncludesBranch: true,
      diagnosticIncludesLock: true,
      worktreeExists: false,
      branchExists: true,
      lockContents,
    });
  });
});


test("force removal deletes both an unmerged branch and a dirty worktree", async () => {
  await withRepository(async (root, repo) => {
    const branch = "forced-feature";
    const worktree = await addWorktree(repo, root, "forced-worktree", branch);
    await commitFile(worktree, "feature.txt", "unmerged change\n", "unmerged change");
    await writeFile(join(worktree, "untracked.txt"), "discarded change\n", "utf8");
    const mainHead = await runGit(repo, ["rev-parse", "refs/heads/main"]);
    const port = gitFallbackPort(root);

    await port.remove({
      path: worktree,
      repo,
      branch,
      force: true,
      deleteBranch: true,
    });

    expect({
      worktreeExists: existsSync(worktree),
      branchExists: await hasBranch(repo, branch),
      mainHead: await runGit(repo, ["rev-parse", "refs/heads/main"]),
    }).toEqual({ worktreeExists: false, branchExists: false, mainHead });
  });
});

test("a missing worktree still allows deletion of its remaining merged branch", async () => {
  await withRepository(async (root, repo) => {
    const branch = "orphaned-merged-feature";
    const worktree = await addWorktree(repo, root, "removed-worktree", branch);
    await commitFile(worktree, "feature.txt", "merged change\n", "merged change");
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    await runGit(repo, ["worktree", "remove", worktree]);
    expect(await hasBranch(repo, branch)).toBe(true);
    const port = gitFallbackPort(root);

    await port.remove({
      path: worktree,
      repo,
      branch,
      deleteBranch: true,
    });

    expect({ worktreeExists: existsSync(worktree), branchExists: await hasBranch(repo, branch) }).toEqual({
      worktreeExists: false,
      branchExists: false,
    });
  });
});

test("force cannot delete a branch that is checked out in another worktree", async () => {
  await withRepository(async (root, repo) => {
    const branch = "checked-out-elsewhere";
    const otherWorktree = await addWorktree(repo, root, "other-worktree", branch);
    await commitFile(otherWorktree, "feature.txt", "merged change\n", "merged change");
    await runGit(repo, ["merge", "--quiet", "--ff-only", branch]);
    const recordedPath = join(root, "missing-recorded-path");
    const port = gitFallbackPort(root);

    const rejected = await port.remove({
      path: recordedPath,
      repo,
      branch,
      force: true,
      deleteBranch: true,
    }).then(
      () => false,
      () => true,
    );

    expect({
      rejected,
      recordedPathExists: existsSync(recordedPath),
      otherWorktreeExists: existsSync(otherWorktree),
      otherWorktreeBranch: await runGit(otherWorktree, ["branch", "--show-current"]),
      branchExists: await hasBranch(repo, branch),
    }).toEqual({
      rejected: true,
      recordedPathExists: false,
      otherWorktreeExists: true,
      otherWorktreeBranch: branch,
      branchExists: true,
    });
  });
});

test("a replaced worktree path is not removed and its branch stays attached to the relocated worktree", async () => {
  await withRepository(async (root, repo) => {
    const branch = "relocated-feature";
    const recordedPath = await addWorktree(repo, root, "recorded-worktree", branch);
    const relocatedPath = join(root, "relocated-worktree");
    await runGit(repo, ["worktree", "move", recordedPath, relocatedPath]);
    await symlink(relocatedPath, recordedPath);
    const port = gitFallbackPort(root);

    const rejected = await port.remove({
      path: recordedPath,
      repo,
      branch,
      force: true,
      deleteBranch: true,
    }).then(
      () => false,
      () => true,
    );

    expect({
      rejected,
      pathIsSymlink: lstatSync(recordedPath).isSymbolicLink(),
      resolvedPath: await realpath(recordedPath),
      relocatedWorktreeExists: existsSync(relocatedPath),
      branchExists: await hasBranch(repo, branch),
      relocatedBranch: await runGit(relocatedPath, ["branch", "--show-current"]),
    }).toEqual({
      rejected: true,
      pathIsSymlink: true,
      resolvedPath: relocatedPath,
      relocatedWorktreeExists: true,
      branchExists: true,
      relocatedBranch: branch,
    });
  });
});

test("force cannot delete a recorded branch through an unknown non-worktree path", async () => {
  await withRepository(async (root, repo) => {
    const branch = "unowned-existing-branch";
    await runGit(repo, ["branch", branch]);
    const unknownPath = join(root, "unknown-directory");
    await mkdir(unknownPath);
    const sentinel = join(unknownPath, "keep.txt");
    await writeFile(sentinel, "not an owned worktree\n", "utf8");
    const port = gitFallbackPort(root);

    const rejected = await port.remove({
      path: unknownPath,
      repo,
      branch,
      force: true,
      deleteBranch: true,
    }).then(
      () => false,
      () => true,
    );

    expect({
      rejected,
      sentinel: existsSync(sentinel) ? await readFile(sentinel, "utf8") : null,
      branchExists: await hasBranch(repo, branch),
    }).toEqual({
      rejected: true,
      sentinel: "not an owned worktree\n",
      branchExists: true,
    });
  });
});
