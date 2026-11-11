import { setTimeout as sleep } from "node:timers/promises";
import type { ChannelTransport, ChatTarget, GroupContext } from "../contracts.ts";
import { isTransientServerFailure } from "../errors.ts";
import { OctoError } from "../octo/error.ts";

export async function readGroupContext(
  transport: Pick<ChannelTransport, "getGroupContext">,
  target: ChatTarget,
  signal: AbortSignal,
): Promise<GroupContext | null> {
  for (let attempt = 0; ; attempt++) {
    signal.throwIfAborted();
    const timeout = AbortSignal.timeout(15_000);
    const requestSignal = AbortSignal.any([signal, timeout]);
    const aborted = Promise.withResolvers<never>();
    const onAbort = (): void => aborted.reject(requestSignal.reason);
    requestSignal.addEventListener("abort", onAbort, { once: true });
    try {
      const result = await Promise.race([transport.getGroupContext(target, requestSignal), aborted.promise]);
      signal.throwIfAborted();
      return result;
    } catch (error) {
      signal.throwIfAborted();
      const transient = timeout.aborted || isTransientServerFailure(error)
        || (error instanceof OctoError && error.status === undefined
          && /^Octo group context request (failed|timed out)$/.test(error.message));
      if (!transient || attempt === 2) throw error;
    } finally {
      requestSignal.removeEventListener("abort", onAbort);
    }
    await sleep(1_000 * 2 ** attempt, undefined, { signal });
  }
}
