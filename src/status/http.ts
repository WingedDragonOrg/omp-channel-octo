import { createHash, timingSafeEqual } from "node:crypto";
import { createZedFileLinkResolvers } from "../file-links.ts";

import type {
  ChatDetailsSnapshot,
  ChatStatusSnapshot,
  ChatTarget,
  FileLinkConfig,
  ServiceStatusSnapshot,
} from "../contracts.ts";

interface StatusHandlerOptions {
  token: string;
  statusSnapshot: () => ServiceStatusSnapshot;
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot;
  chatDetailsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<ChatDetailsSnapshot>;
  fileLinks?: FileLinkConfig;
}

const JSON_HEADERS = {
  "cache-control": "no-store",
  "content-type": "application/json; charset=utf-8",
};

function jsonResponse(body: unknown, status = 200): Response {
  const serialized = JSON.stringify(body);
  if (serialized === undefined) throw new TypeError("Response body is not serializable");
  return new Response(serialized, { status, headers: JSON_HEADERS });
}

function errorResponse(status: number, code: string): Response {
  return jsonResponse({ error: { code } }, status);
}

function isAuthorized(request: Request, token: string): boolean {
  const authorization = request.headers.get("authorization");
  const match = authorization === null ? null : /^Bearer ([\x21-\x7e]+)$/.exec(authorization);
  const candidate = match?.[1] ?? "";
  const candidateDigest = createHash("sha256").update(candidate, "utf8").digest();
  const configuredDigest = createHash("sha256").update(token, "utf8").digest();
  const equal = timingSafeEqual(candidateDigest, configuredDigest);
  return match !== null && equal;
}

function hasOnlyUniqueParameters(params: URLSearchParams, allowed: readonly string[]): boolean {
  const allowedSet = new Set(allowed);
  const seen = new Set<string>();
  for (const [name] of params) {
    if (!allowedSet.has(name) || seen.has(name)) return false;
    seen.add(name);
  }
  return seen.size === allowedSet.size;
}

function parseChatTarget(params: URLSearchParams): ChatTarget | null {
  if (!hasOnlyUniqueParameters(params, ["channelType", "channelId"])) return null;

  const channelTypeValue = params.get("channelType");
  const channelId = params.get("channelId");
  if (channelTypeValue === null || channelId === null || channelId.trim().length === 0) return null;
  if (!/^-?\d+$/.test(channelTypeValue)) return null;

  const channelType = Number(channelTypeValue);
  if (channelType !== 1 && channelType !== 2 && channelType !== 5) return null;
  return { channelType, channelId };
}

export function createStatusHandler({
  token,
  statusSnapshot,
  chatStatusSnapshot,
  chatDetailsSnapshot,
  fileLinks,
}: StatusHandlerOptions): (request: Request) => Promise<Response> {
  return async request => {
    try {
      if (!isAuthorized(request, token)) return errorResponse(401, "unauthorized");
      if (request.method !== "GET") return errorResponse(405, "method_not_allowed");

      const url = new URL(request.url);
      if (url.pathname === "/v1/status") {
        if (!hasOnlyUniqueParameters(url.searchParams, [])) {
          return errorResponse(400, "invalid_request");
        }
        return jsonResponse(statusSnapshot());
      }

      if (url.pathname === "/v1/chats/status") {
        const target = parseChatTarget(url.searchParams);
        if (target === null) return errorResponse(400, "invalid_request");

        const snapshot = chatStatusSnapshot(target);
        if (snapshot.channelType !== target.channelType || snapshot.channelId !== target.channelId) {
          return errorResponse(500, "internal_error");
        }
        const resolver = fileLinks && createZedFileLinkResolvers(fileLinks.resolvers)[0];
        const zedWorkspaceUrl = snapshot.cwd && resolver ? await resolver.resolve(".", snapshot.cwd) : null;
        return jsonResponse({ ...snapshot, zedWorkspaceUrl });
      }

      if (url.pathname === "/v1/chats/details") {
        const target = parseChatTarget(url.searchParams);
        if (target === null) return errorResponse(400, "invalid_request");

        const snapshot = await chatDetailsSnapshot(target, request.signal);
        if (snapshot.channelType !== target.channelType || snapshot.channelId !== target.channelId) {
          return errorResponse(500, "internal_error");
        }
        return jsonResponse(snapshot);
      }

      return errorResponse(404, "not_found");
    } catch {
      return errorResponse(500, "internal_error");
    }
  };
}
