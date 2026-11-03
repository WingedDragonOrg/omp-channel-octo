/**
 * Incremental, strictly bounded decoder for the native `omp commit` output.
 *
 * The CLI writes complete assistant turns, tool arguments, diffs, and paths into the same pipes
 * as its fixed stage lines. Only the verified fixed lines below become summaries; every other
 * line is dropped. A line is decoded only after its terminator arrived, so a multi-byte UTF-8
 * character and an ANSI escape sequence may be split across arbitrary pipe chunks.
 */

export interface CommitOutputParser {
  write(bytes: Uint8Array): void;
  end(): void;
}

const MAX_LINE_BYTES = 8 * 1024;

const STAGE_SUMMARIES: Record<string, string> = {
  "● Resolving model...": "正在解析模型",
  "No staged changes detected, staging all changes...": "正在暂存全部改动",
  "● Detecting changelog targets...": "正在检测变更日志",
  "● Discovering context files...": "正在发现上下文文件",
  "● Starting commit agent...": "正在启动提交代理",
  "● Using fallback commit generation...": "正在使用回退提交生成",
  "● Agent did not provide proposal, using fallback...": "提交代理未提供方案，正在使用回退生成",
  "● Applying changelog entries...": "正在写入变更日志",
  "● Creating commit...": "正在创建提交",
  "● Creating split commits...": "正在创建拆分提交",
};

/** The only tools the native commit agent registers; anything else is not a commit stage. */
const COMMIT_TOOL_NAMES: Record<string, true> = {
  GitOverview: true,
  GitFileDiff: true,
  GitHunk: true,
  RecentCommits: true,
  AnalyzeFiles: true,
  ProposeChangelog: true,
  ProposeCommit: true,
  SplitCommit: true,
};

const AGENT_FINISHED = /^● agent finished \(\d+ messages?, \d+ tools?\)$/u;
const TOOL_END = /^([\uF00C\uF00D])\s+([A-Za-z][A-Za-z0-9]*)$/u;

const ANSI_ESCAPE =
  /\u001B\][^\u0007]*(?:\u0007|\u001B\\)|\u001B\[[0-?]*[ -/]*[@-~]|\u009B[0-?]*[ -/]*[@-~]|\u001B[@-_]/gu;
const CONTROL_CHARACTERS = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/gu;

function cleanLine(text: string): string {
  return text.replace(ANSI_ESCAPE, "").replace(CONTROL_CHARACTERS, "").trim();
}

function summaryForLine(text: string): string | undefined {
  const fixed = STAGE_SUMMARIES[text];
  if (fixed !== undefined && Object.hasOwn(STAGE_SUMMARIES, text)) return fixed;
  if (AGENT_FINISHED.test(text)) return "提交代理本轮已结束";
  const tool = TOOL_END.exec(text);
  if (tool === null) return undefined;
  const name = tool[2];
  if (name === undefined || !Object.hasOwn(COMMIT_TOOL_NAMES, name)) return undefined;
  return `${tool[1] === "\uF00C" ? "工具完成" : "工具失败"}：${name}`;
}

export function createCommitOutputParser(onSummary: (summary: string) => void): CommitOutputParser {
  const decoder = new TextDecoder();
  const line = new Uint8Array(MAX_LINE_BYTES);
  let length = 0;
  let discarding = false;

  function finishLine(): void {
    if (discarding) {
      discarding = false;
      length = 0;
      return;
    }
    if (length === 0) return;
    const text = cleanLine(decoder.decode(line.subarray(0, length)));
    length = 0;
    if (text.length === 0) return;
    const summary = summaryForLine(text);
    if (summary !== undefined) onSummary(summary);
  }

  return {
    write(bytes: Uint8Array): void {
      for (let index = 0; index < bytes.length; index += 1) {
        const byte = bytes[index]!;
        if (byte === 0x0a || byte === 0x0d) {
          finishLine();
          continue;
        }
        if (discarding) continue;
        if (length === MAX_LINE_BYTES) {
          discarding = true;
          length = 0;
          continue;
        }
        line[length] = byte;
        length += 1;
      }
    },
    end(): void {
      // A line without a terminator is not a complete fixed line; never project a partial one.
      length = 0;
      discarding = false;
    },
  };
}
