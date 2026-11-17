import { expect, test } from "bun:test";
import { assertAotSupported } from "../scripts/aot-support.ts";

test("AOT build rejects Bun help that does not advertise --aot", () => {
  expect(() => assertAotSupported("bun build options:\n  --compile\n  --minify")).toThrow(/--aot/);
});

test("AOT build accepts Bun help that advertises the exact --aot option", () => {
  expect(() => assertAotSupported("bun build options:\n  --compile\n  --aot  Enable ahead-of-time compilation")).not.toThrow();
});

test("a similarly named option does not count as AOT support", () => {
  expect(() => assertAotSupported("bun build options:\n  --aot-cache  Cache compiled output")).toThrow(/--aot/);
});
