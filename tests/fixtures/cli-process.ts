import { waitForFixture } from "./octo-protocol-server.ts";

export interface ObservedCli {
  output: Promise<[string, string]>;
  waitFor<T>(read: () => T | undefined, timeoutMs: number, description: string): Promise<T>;
}

/** CLI protocol tests own their HOME and do not expose an HTTP status listener. */
export function cliEnvironment(home: string, overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  const env = Object.fromEntries(
    Object.entries(process.env).filter(([key]) => !key.startsWith("OMP_CHANNEL_OCTO_HTTP_")),
  );
  return { ...env, HOME: home, TZ: "UTC", OMP_CHANNEL_OCTO_HTTP_ENABLED: "false", ...overrides };
}

/** Drain both pipes immediately and report an exited CLI instead of waiting for a missing reply. */
export function observeCli(child: Bun.Subprocess<"ignore", "pipe", "pipe">): ObservedCli {
  const output = Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return {
    output,
    async waitFor<T>(read: () => T | undefined, timeoutMs: number, description: string): Promise<T> {
      try {
        return await waitForFixture(() => {
          const value = read();
          if (value !== undefined) return value;
          if (child.exitCode !== null) throw new Error("CLI exited before the fixture condition");
          return undefined;
        }, timeoutMs, description);
      } catch (error) {
        if (child.exitCode === null) throw error;
        const [stdout, stderr] = await output;
        throw new Error(`${description}: CLI exited with code ${child.exitCode}\n${stdout}\n${stderr}`);
      }
    },
  };
}
