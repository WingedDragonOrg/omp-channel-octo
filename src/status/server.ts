import { constants as fsConstants } from "node:fs";
import { open, type FileHandle } from "node:fs/promises";
import { createStatusHandler } from "./http.ts";
import { assertNoExtendedAcl } from "./private-file-acl.ts";

import type { ChatDetailsSnapshot, ChatStatusSnapshot, ChatTarget, FileLinkConfig, ServiceStatusSnapshot } from "../contracts.ts";

export interface StatusServerOptions {
  listenAddress: string;
  port: number;
  /** Direct bearer token; when set the token file is never read. */
  token?: string;
  /** Private token file, re-read for every request when no direct token is set. */
  tokenFile?: string;
  statusSnapshot: () => ServiceStatusSnapshot;
  chatStatusSnapshot: (target: ChatTarget) => ChatStatusSnapshot;
  chatDetailsSnapshot: (target: ChatTarget, signal: AbortSignal) => Promise<ChatDetailsSnapshot>;
  fileLinks?: FileLinkConfig;
}

export interface StatusServer {
  port: number;
  stop: () => Promise<void>;
}

const MIN_TOKEN_HEX_LENGTH = 64;
const MAX_TOKEN_HEX_LENGTH = 16_384;

async function readPrivateToken(tokenFile: string): Promise<string> {
  let file: FileHandle | undefined;
  try {
    const noFollow = fsConstants.O_NOFOLLOW;
    if (typeof noFollow !== "number" || typeof process.geteuid !== "function") {
      throw new Error("unsupported credential-file checks");
    }

    file = await open(tokenFile, fsConstants.O_RDONLY | fsConstants.O_NONBLOCK | noFollow);
    const details = await file.stat();
    const ownerUid = process.geteuid();
    if (
      !details.isFile()
      || details.uid !== ownerUid
      || (details.mode & 0o077) !== 0
      || (details.mode & 0o400) === 0
      || !Number.isSafeInteger(details.size)
      || details.size < MIN_TOKEN_HEX_LENGTH
      || details.size > MAX_TOKEN_HEX_LENGTH
      || details.size % 2 !== 0
    ) {
      throw new Error("unsafe status token file");
    }

    assertNoExtendedAcl(file.fd);

    const bytes = Buffer.alloc(details.size);
    let bytesRead = 0;
    while (bytesRead < bytes.length) {
      const result = await file.read(bytes, bytesRead, bytes.length - bytesRead, bytesRead);
      if (result.bytesRead === 0) break;
      bytesRead += result.bytesRead;
    }
    if (bytesRead !== details.size || (await file.stat()).size !== details.size) {
      throw new Error("incomplete status token file");
    }

    for (let index = 0; index < bytesRead; index += 1) {
      const byte = bytes.readUInt8(index);
      const isDigit = byte >= 0x30 && byte <= 0x39;
      const isLowerHex = byte >= 0x61 && byte <= 0x66;
      const isUpperHex = byte >= 0x41 && byte <= 0x46;
      if (!isDigit && !isLowerHex && !isUpperHex) throw new Error("invalid status token format");
    }

    return bytes.toString("ascii", 0, bytesRead);
  } catch {
    throw new Error("Status token file must be owned and private and contain at least 32 bytes of hexadecimal token data");
  } finally {
    await file?.close().catch(() => {});
  }
}


export async function startStatusServer(options: StatusServerOptions): Promise<StatusServer> {
  const directToken = options.token;
  if (directToken === undefined && options.tokenFile !== undefined) {
    await readPrivateToken(options.tokenFile);
  } else if (directToken === undefined) {
    throw new Error("Status server requires OMP_CHANNEL_OCTO_HTTP_TOKEN or OMP_CHANNEL_OCTO_HTTP_TOKEN_FILE");
  }

  const server = (() => {
    try {
      return Bun.serve({
        hostname: options.listenAddress,
        port: options.port,
        fetch: async request => {
          try {
            const token = directToken ?? await readPrivateToken(options.tokenFile as string);
            return createStatusHandler({
              token,
              statusSnapshot: options.statusSnapshot,
              chatStatusSnapshot: options.chatStatusSnapshot,
              chatDetailsSnapshot: options.chatDetailsSnapshot,
              ...(options.fileLinks === undefined ? {} : { fileLinks: options.fileLinks }),
            })(request);
          } catch {
            return Response.json({ error: { code: "internal_error" } }, {
              status: 500,
              headers: { "cache-control": "no-store" },
            });
          }
        },
      });
    } catch {
      throw new Error("Status server could not be started");
    }
  })();
  const port = server.port;
  if (port === undefined) {
    try {
      await server.stop(true);
    } catch {
      // Do not include runtime details in startup errors.
    }
    throw new Error("Status server could not be started");
  }

  let stopping: Promise<void> | undefined;
  return {
    port,
    stop: () => {
      if (!stopping) {
        stopping = (async () => {
          try {
            await server.stop(true);
          } catch {
            throw new Error("Status server could not be stopped");
          }
        })();
      }
      return stopping;
    },
  };
}
