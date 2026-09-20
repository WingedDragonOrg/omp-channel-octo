import { buildAskCard, buildAskCardSet, type AskCardQuestion } from "../../src/ask-card.ts";

// Fixed synthetic content. Keep this workload unchanged across UI experiments.
const single: AskCardQuestion = {
  question: "这次发布采用哪种上线方式？",
  multi: false,
  options: [
    { label: "灰度发布（推荐）", description: "先向少量用户开放，观察错误率后逐步扩大范围。" },
    { label: "全量发布", description: "一次开放给所有用户，适合已经充分验证的变更。" },
    { label: "暂缓发布", description: "保留当前版本，等依赖服务验收完成再安排上线。" },
  ],
};
const long: AskCardQuestion = {
  question: "移动端需要同时展示比较长的中文选项与技术细节，你希望采用哪种呈现方式？",
  multi: false,
  options: [
    { label: "保留完整说明，并让每个选项独占一行方便阅读和点击", description: "使用清晰的标题和说明层级；窄屏也能看完关键差异。" },
    { label: "按任务阶段分组，优先展示当前需要决定的事项", description: "适合多步骤流程，已回答的内容仍然可以回看。" },
    { label: "Desktop / Mobile shared layout with accessible controls", description: "Keyboard navigation and long English labels must remain usable." },
  ],
};
const multi: AskCardQuestion = { ...single, question: "本次验收需要覆盖哪些环境？", multi: true };
const free: AskCardQuestion = { question: "还有哪些上线约束需要补充？", multi: false, options: [] };
const names = new Map([["reviewer", "小林"]]);
const entries = [single, multi, free].map(question => ({ question, answer: null, answeredBy: null }));
const partial = entries.map((entry, i) => i === 0 ? { ...entry, answer: "1", answeredBy: "reviewer" } : entry);
const completed = entries.map((entry, i) => ({ ...entry, answer: i === 2 ? "请保留回滚入口。" : "1", answeredBy: "reviewer" }));
export const askCardCases = [
  { id: "single", active: true, questions: [single], frame: buildAskCard(single, { kind: "pending", generation: 1 }, single.question) },
  { id: "long", active: true, questions: [long], frame: buildAskCard(long, { kind: "pending", generation: 1 }, long.question) },
  { id: "multi", active: true, questions: [multi], frame: buildAskCard(multi, { kind: "pending", generation: 1 }, multi.question) },
  { id: "free", active: true, questions: [free], frame: buildAskCard(free, { kind: "pending", generation: 1 }, free.question) },
  { id: "answered", active: false, questions: [single], frame: buildAskCard(single, { kind: "answered", answer: "1", answeredBy: "reviewer" }, "小林选择灰度发布", names) },
  { id: "inactive", active: false, questions: [single], frame: buildAskCard(single, { kind: "inactive", note: "本次问题已关闭。" }, "本次问题已关闭。") },
  { id: "set", active: true, questions: [single, multi, free], frame: buildAskCardSet(entries, { kind: "open", generation: 1 }, "发布前确认") },
  { id: "partial", active: true, questions: [single, multi, free], frame: buildAskCardSet(partial, { kind: "open", generation: 2 }, "还有两题待回答", names) },
  { id: "complete", active: false, questions: [single, multi, free], frame: buildAskCardSet(completed, { kind: "closed", note: null }, "全部问题已回答", names) },
];
