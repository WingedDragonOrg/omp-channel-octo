import { AssertionError } from "node:assert";

export interface CheckResult {
  name: string;
  passed: boolean;
  detail?: string;
}

/** Capability gaps are measurements; setup, protocol and cleanup failures are fatal. */
export async function check(name: string, run: () => void | Promise<void>): Promise<CheckResult> {
  try {
    await run();
    return { name, passed: true };
  } catch (error) {
    if (!(error instanceof AssertionError)) throw error;
    return { name, passed: false, detail: error.message.replace(/\s+/g, " ").slice(0, 240) };
  }
}
