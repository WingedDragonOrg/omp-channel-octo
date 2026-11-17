export function assertAotSupported(buildHelp: string): void {
  if (!/(?:^|\s)--aot(?=$|\s|[=,])/m.test(buildHelp)) {
    throw new Error("当前 Bun 的 bun build --help 未声明 --aot；请使用支持 AOT 编译的 Bun 版本。");
  }
}
