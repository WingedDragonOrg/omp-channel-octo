import type { CardFrame } from "../contracts.ts";

/** Render native context estimates without the terminal-only bar. */
export function buildContextCard(output: string): CardFrame {
  const lines = output.split("\n").map(line => line.replace(/\[[█▓▒░▏▎▍▌▋▊▉\s]+\]/gu, "").trim()).filter(Boolean);
  const header = lines[0]?.match(/^Context window:\s*(\d+)\s+tokens\s+\(([\d.]+)% used\)$/u);
  const body: Record<string, unknown>[] = [
    { type: "TextBlock", text: "上下文用量", weight: "Bolder", size: "Medium", wrap: true },
  ];
  if (header) {
    body.push(
      { type: "TextBlock", text: `${header[2]}% 已用`, size: "ExtraLarge", weight: "Bolder", color: "Accent", spacing: "Medium", wrap: true },
      { type: "TextBlock", text: `上下文窗口 ${header[1]} tokens`, isSubtle: true, spacing: "None", wrap: true },
    );
  }
  const details = header ? lines.slice(1) : lines;
  if (details.length > 0) {
    body.push({ type: "TextBlock", text: "占用明细", weight: "Bolder", spacing: "Medium", separator: true, wrap: true });
    for (const line of details) {
      const metric = line.match(/^(.+?)\s+([\d.]+%)\s+(\d+)\s+tokens$/u);
      body.push({
        type: "TextBlock",
        text: metric ? `${metric[1]}　 **${metric[2]}** · ${metric[3]} tokens` : line,
        spacing: "Small",
        wrap: true,
      });
    }
  }
  return {
    card: {
      type: "AdaptiveCard",
      version: "1.5",
      body,
    },
    plain: lines.join("\n"),
    profile: "octo/v1",
  };
}
