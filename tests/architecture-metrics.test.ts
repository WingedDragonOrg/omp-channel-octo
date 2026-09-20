import { expect, test } from "bun:test";
import { analyzeArchitecture } from "./benchmarks/architecture-metrics.ts";

// A nested callback must not inflate its enclosing function's complexity, and
// comments, whitespace, type declarations and strings must not change the score.
test("counts executable decisions once, independent of source formatting", () => {
  const source = `
    type Choice = "if" | "while";
    export function choose(value: number) {
      if (value > 0 && value < 10) return value;
      return [value].map(item => item ? item : 0);
    }
  `;
  const compact = `type Choice="if"|"while";export function choose(value:number){if(value>0&&value<10)return value;return [value].map(item=>item?item:0);}`;
  for (const text of [source, compact]) {
    const result = analyzeArchitecture(new Map([["src/choice.ts", text]]));
    expect(result.metrics.module_complexity_peak).toBe(5);
    expect(result.metrics.function_complexity_peak).toBe(3);
    expect(result.metrics.total_complexity).toBe(5);
  }
});

test("measures every module so moving a hotspot cannot hide it", () => {
  const result = analyzeArchitecture(new Map([
    ["src/service.ts", `export { choose } from "./domain/choice.ts";`],
    ["src/domain/choice.ts", `export function choose(x: number) { if (x) return 1; return 0; }`],
  ]));
  expect(result.metrics.module_complexity_peak).toBe(2);
  expect(result.hotspots[0]?.path).toBe("src/domain/choice.ts");
  expect(result.metrics.internal_dependency_edges).toBe(1);
});

test("includes top-level decisions and ignores erased type-only imports", () => {
  const result = analyzeArchitecture(new Map([
    ["src/a.ts", `import type { B } from "./b.ts"; export const a = true ? 1 : 2;`],
    ["src/b.ts", `import { a } from "./a.ts"; export type B = number; export const b = a;`],
  ]));
  expect(result.metrics.total_complexity).toBe(1);
  expect(result.metrics.internal_dependency_edges).toBe(2);
  expect(result.metrics.runtime_cycle_modules).toBe(0);
});

test("detects cycles through re-exports and literal dynamic imports", () => {
  const result = analyzeArchitecture(new Map([
    ["src/a.ts", `export { b } from "./b";`],
    ["src/b.ts", `export const b = () => import("./a.ts");`],
    ["src/c.ts", `import { b } from "./b.ts"; export { b };`],
  ]));
  expect(result.metrics.internal_dependency_edges).toBe(3);
  expect(result.metrics.runtime_cycle_modules).toBe(2);
});

test("refuses empty, unparsable and unresolved source graphs", () => {
  expect(() => analyzeArchitecture(new Map())).toThrow();
  expect(() => analyzeArchitecture(new Map([["src/a.ts", "export function broken( {"]]))).toThrow();
  expect(() => analyzeArchitecture(new Map([["src/a.ts", 'export { missing } from "./missing.ts";']]))).toThrow();
});
