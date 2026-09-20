import assert from "node:assert/strict";
import { posix } from "node:path";
import ts from "typescript";

export interface ArchitectureResult {
  metrics: {
    module_complexity_peak: number;
    function_complexity_peak: number;
    total_complexity: number;
    source_modules: number;
    internal_dependency_edges: number;
    runtime_cycle_modules: number;
  };
  hotspots: { path: string; complexity: number }[];
}

// Architecture proxy, not a claim to measure design quality. A module's weight is
// one per executable function plus each if/loop/catch/case/ternary/logical branch.
// Nested callbacks own their decisions; top-level decisions still belong to the
// module. Formatting, comments and erased types cannot improve this score.
export function analyzeArchitecture(sources: ReadonlyMap<string, string>): ArchitectureResult {
  assert.ok(sources.size > 0, "no production sources found");
  const runtimeEdges = new Map<string, Set<string>>();
  const hotspots: ArchitectureResult["hotspots"] = [];
  let functionPeak = 0;
  let edgeCount = 0;

  function dependencies(path: string, text: string): Set<string> {
    const result = new Set<string>();
    for (const reference of ts.preProcessFile(text, true, true).importedFiles) {
      if (!reference.fileName.startsWith(".")) continue;
      const base = posix.normalize(posix.join(posix.dirname(path), reference.fileName));
      const target = [base, `${base}.ts`, `${base}.tsx`, `${base}/index.ts`, `${base}/index.tsx`]
        .find(candidate => sources.has(candidate));
      assert.ok(target, `${path}: unresolved internal dependency ${reference.fileName}`);
      result.add(target);
    }
    return result;
  }

  for (const path of [...sources.keys()].sort()) {
    const text = sources.get(path)!;
    const emitted = ts.transpileModule(text, {
      fileName: path,
      reportDiagnostics: true,
      compilerOptions: { target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext },
    });
    const errors = emitted.diagnostics?.filter(item => item.category === ts.DiagnosticCategory.Error) ?? [];
    assert.equal(errors.length, 0, `${path}: ${errors.map(item => ts.flattenDiagnosticMessageText(item.messageText, " ")).join("; ")}`);
    edgeCount += dependencies(path, text).size;
    // Use emitted imports so both explicit and inferred type-only dependencies
    // are excluded from runtime cycles, while still contributing to coupling.
    runtimeEdges.set(path, dependencies(path, emitted.outputText));
    const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
    const functions: { complexity: number }[] = [];
    let complexity = 0;

    function visit(node: ts.Node, owner?: { complexity: number }): void {
      if (ts.isTypeNode(node) || ts.isInterfaceDeclaration(node) || ts.isTypeAliasDeclaration(node)) return;
      if (ts.isFunctionLike(node) && "body" in node && node.body) {
        owner = { complexity: 1 };
        functions.push(owner);
        complexity += 1;
      }
      let decision = false;
      switch (node.kind) {
        case ts.SyntaxKind.IfStatement:
        case ts.SyntaxKind.ForStatement:
        case ts.SyntaxKind.ForInStatement:
        case ts.SyntaxKind.ForOfStatement:
        case ts.SyntaxKind.WhileStatement:
        case ts.SyntaxKind.DoStatement:
        case ts.SyntaxKind.CatchClause:
        case ts.SyntaxKind.CaseClause:
        case ts.SyntaxKind.ConditionalExpression:
          decision = true;
          break;
        case ts.SyntaxKind.BinaryExpression: {
          const operator = (node as ts.BinaryExpression).operatorToken.kind;
          decision = operator === ts.SyntaxKind.AmpersandAmpersandToken
            || operator === ts.SyntaxKind.BarBarToken
            || operator === ts.SyntaxKind.QuestionQuestionToken
            || operator === ts.SyntaxKind.AmpersandAmpersandEqualsToken
            || operator === ts.SyntaxKind.BarBarEqualsToken
            || operator === ts.SyntaxKind.QuestionQuestionEqualsToken;
          break;
        }
      }
      if (decision) {
        complexity += 1;
        if (owner) owner.complexity += 1;
      }
      ts.forEachChild(node, child => visit(child, owner));
    }

    visit(source);
    for (const fn of functions) functionPeak = Math.max(functionPeak, fn.complexity);
    hotspots.push({ path, complexity });
  }

  // Count exactly the modules that can reach themselves. Dependants outside a
  // cycle do not count; a self-import does. The repository graph is small.
  let cycleModules = 0;
  for (const origin of runtimeEdges.keys()) {
    const visited = new Set<string>();
    const pending = [...runtimeEdges.get(origin)!];
    while (pending.length > 0) {
      const current = pending.pop()!;
      if (current === origin) {
        cycleModules += 1;
        break;
      }
      if (visited.has(current)) continue;
      visited.add(current);
      pending.push(...runtimeEdges.get(current)!);
    }
  }
  hotspots.sort((left, right) => right.complexity - left.complexity || (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
  return {
    metrics: {
      module_complexity_peak: hotspots[0]!.complexity,
      function_complexity_peak: functionPeak,
      total_complexity: hotspots.reduce((sum, item) => sum + item.complexity, 0),
      source_modules: sources.size,
      internal_dependency_edges: edgeCount,
      runtime_cycle_modules: cycleModules,
    },
    hotspots,
  };
}
