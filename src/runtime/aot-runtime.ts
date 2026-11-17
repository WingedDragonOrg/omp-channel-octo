import { realpathSync } from "node:fs";
import { dirname, resolve } from "node:path";

declare const OMP_CHANNEL_OCTO_AOT: boolean;

export const IS_AOT_BINARY = typeof OMP_CHANNEL_OCTO_AOT === "undefined" ? false : OMP_CHANNEL_OCTO_AOT;

export function runtimeAssetPath(relativePath: string): string {
  if (IS_AOT_BINARY) return resolve(dirname(realpathSync(process.execPath)), "runtime", relativePath);
  return resolve(import.meta.dir, "..", relativePath);
}

export const OMP_CONTEXT_EXTENSION_PATH = runtimeAssetPath("omp/context-extension.ts");
export const OMP_GOAL_RPC_CONFIG_PATH = runtimeAssetPath("omp/goal-rpc.yml");
