import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { ChatTarget } from "./contracts.ts";
import { MAX_RESEARCH_REPORT_BYTES } from "./research.ts";

/**
 * 研究完整记录的私有落盘：每次权威 state 生成的新报告原子替换该 bot+聊天的 latest，
 * 重启后仍可导出。导出时在 file outbox 入队前复制一份不可变 spool——队列里待发的附件
 * 永远保持入队瞬间的字节，之后的报告重写碰不到它。
 *
 * 上限与上游同源（`MAX_RESEARCH_REPORT_BYTES`，ResearchData 侧生成即截断）：写前按 UTF-8
 * 字节校验、读前 stat 校验，超限内容一律不读入内存，也绝不让报告 I/O 拖垮模型任务。
 */

/** 报告根目录（0700）。测试与安全审计从这里定位真实落盘面，不经过 OMP 的任何数据库。 */
export function researchReportsRoot(stateDir: string): string {
  return join(stateDir, "research-reports");
}

export interface ResearchReportKey {
  botUid: string;
  target: ChatTarget;
}

interface ReportChain {
  file: string;
  tail: Promise<void>;
}

export interface ResearchReportStore {
  /**
   * 原子替换该聊天的 latest 报告。同一聊天的写入严格有序（链式），不同聊天互不阻塞；
   * 一次写失败不堵住该聊天的后续写入，rejection 由调用方按「不拖垮模型任务」处理。
   * 超限报告只会拿到 rejected Promise，绝不在事件路径上同步抛错。
   */
  update(key: ResearchReportKey, report: string): Promise<void>;
  /**
   * 把该聊天 latest 报告复制成不可变 spool 文件并返回其路径；没有研究报告、或落盘内容
   * 不是合规的常规文件（含超限）时返回 `undefined`。会先等该聊天已排队的写入落盘，
   * 因此导出看到的总是最新的持久字节；读取先 stat 限界，绝不把任意大的文件读入内存。
   */
  spool(key: ResearchReportKey): Promise<string | undefined>;
  /** 等待所有聊天已排队的写入写到结果（落盘或失败）；单次失败不阻断停机。 */
  drain(): Promise<void>;
}

export function createResearchReportStore(options: {
  rootDir: string;
  spoolRootDir: string;
}): ResearchReportStore {
  const chains = new Map<string, ReportChain>();

  const chainFor = (key: ResearchReportKey): ReportChain => {
    // The bot and real chat identity share one latest report.
    const id = createHash("sha256")
      .update(`${key.botUid}\0${key.target.channelType}\0${key.target.channelId}`, "utf8")
      .digest("hex");
    let chain = chains.get(id);
    if (chain === undefined) {
      chain = { file: join(options.rootDir, `report-${id}.md`), tail: Promise.resolve() };
      chains.set(id, chain);
    }
    return chain;
  };

  /** 读取 latest 的合规内容；不存在或不合规（非常规文件、超限）都按「没有」处理。 */
  const readPersistedReport = async (file: string): Promise<string | undefined> => {
    let details;
    try {
      details = await stat(file);
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return undefined;
      throw error;
    }
    if (!details.isFile() || details.size > MAX_RESEARCH_REPORT_BYTES) return undefined;
    return await readFile(file, "utf8");
  };

  return {
    update(key, report) {
      const chain = chainFor(key);
      const next = chain.tail
        .catch(() => {})
        .then(async () => {
          if (Buffer.byteLength(report, "utf8") > MAX_RESEARCH_REPORT_BYTES) {
            throw new Error("research report exceeds the 1 MiB cap");
          }
          // 临时文件 + rename 的原子替换：读者永远看到完整旧字节或完整新字节。
          await mkdir(options.rootDir, { recursive: true, mode: 0o700 });
          const temporary = `${chain.file}.${randomUUID()}.tmp`;
          try {
            await writeFile(temporary, report, { mode: 0o600 });
            await rename(temporary, chain.file);
          } finally {
            await rm(temporary, { force: true }).catch(() => {});
          }
        });
      chain.tail = next;
      return next;
    },

    async spool(key) {
      const chain = chainFor(key);
      await chain.tail.catch(() => {});
      const report = await readPersistedReport(chain.file);
      if (report === undefined) return undefined;
      const spoolDirectory = join(options.spoolRootDir, randomUUID());
      const spoolPath = join(spoolDirectory, "autoresearch-history.md");
      try {
        await mkdir(spoolDirectory, { recursive: true, mode: 0o700 });
        await writeFile(spoolPath, report, { mode: 0o600 });
        return spoolPath;
      } catch (error) {
        await rm(spoolDirectory, { recursive: true, force: true }).catch(() => {});
        throw error;
      }
    },

    async drain() {
      await Promise.allSettled([...chains.values()].map((chain) => chain.tail));
    },
  };
}
