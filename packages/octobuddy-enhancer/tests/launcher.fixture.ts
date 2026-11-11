import { appendFileSync, existsSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";


function option(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
}

function requireOption(args: string[], name: string): string {
  const value = option(args, name);
  if (value === undefined) throw new Error(`missing fixture argument ${name}`);
  return value;
}

function cdpPort(args: string[]): number {
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]!;
    if (argument.startsWith("--remote-debugging-port=")) {
      return Number(argument.slice("--remote-debugging-port=".length));
    }
    if (argument === "--remote-debugging-port") return Number(args[index + 1]);
  }
  return 0;
}

function processStartIdentity(pid: number): string {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "lstart="], {
    env: { ...process.env, LC_ALL: "C", TZ: "UTC" },
    encoding: "utf8",
  });
  if (result.status !== 0) throw new Error(`cannot read process start identity for PID ${pid}`);
  return result.stdout.trim();
}

function webSocketFrame(opcode: number, payload: Buffer): Buffer {
  if (payload.length < 126) {
    return Buffer.concat([Buffer.from([0x80 | opcode, payload.length]), payload]);
  }
  if (payload.length <= 0xffff) {
    const header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(payload.length, 2);
    return Buffer.concat([header, payload]);
  }
  const header = Buffer.alloc(10);
  header[0] = 0x80 | opcode;
  header[1] = 127;
  header.writeBigUInt64BE(BigInt(payload.length), 2);
  return Buffer.concat([header, payload]);
}

interface FixtureCdpState {
  pending: Buffer;
  rendererUrl?: string;
  frameId: string;
  nextScriptId: number;
  nextContextId: number;
  worldContexts: Map<string, number>;
}

interface FixtureCdpEvent {
  method: string;
  params: Record<string, unknown>;
}

function cdpResult(
  method: string | undefined,
  state: FixtureCdpState,
): Record<string, unknown> {
  switch (method) {
    case "Browser.getVersion":
      return { protocolVersion: "1.3", product: "OctoBuddy-fixture", revision: "fixture", userAgent: "OctoBuddy-fixture", jsVersion: "fixture" };
    case "Target.getTargets":
      return { targetInfos: [] };
    case "Target.getBrowserContexts":
      return { browserContextIds: [] };
    case "Target.attachToTarget":
      return { sessionId: "fixture-session" };
    case "Target.getTargetInfo":
      return { targetInfo: { targetId: "fixture-page", type: "page", title: "OctoBuddy fixture", url: state.rendererUrl ?? "about:blank", attached: true } };
    case "Page.getFrameTree":
      return { frameTree: { frame: { id: state.frameId, url: state.rendererUrl ?? "about:blank" }, childFrames: [] } };
    case "Page.addScriptToEvaluateOnNewDocument":
      state.nextScriptId += 1;
      return { identifier: `fixture-script-${state.nextScriptId}` };
    case "Runtime.evaluate":
      return { result: { type: "undefined" } };
    case "SystemInfo.getProcessInfo":
      return { processInfo: [] };
    case "Browser.getWindowForTarget":
      return { windowId: 1 };
    case "Browser.getWindowBounds":
      return { bounds: { left: 0, top: 0, width: 800, height: 600, windowState: "normal" } };
    case "Target.createBrowserContext":
      return { browserContextId: "fixture-context" };
    case "Target.createTarget":
      return { targetId: "fixture-target" };
    default:
      return {};
  }
}

function cdpEvents(
  method: string | undefined,
  params: Record<string, unknown> | undefined,
  state: FixtureCdpState,
): FixtureCdpEvent[] {
  if (!state.rendererUrl) return [];
  if (method === "Page.enable") {
    return [{
      method: "Page.frameNavigated",
      params: {
        frame: {
          id: state.frameId,
          url: state.rendererUrl,
          loaderId: "fixture-loader",
          securityOrigin: "file://",
          mimeType: "text/html",
        },
      },
    }];
  }
  if (method === "Runtime.enable") {
    return [{
      method: "Runtime.executionContextCreated",
      params: {
        context: {
          id: state.nextContextId,
          origin: "file://",
          name: "",
          auxData: { frameId: state.frameId, isDefault: true, type: "default" },
        },
      },
    }];
  }
  if (method === "Page.addScriptToEvaluateOnNewDocument" && typeof params?.worldName === "string") {
    if (state.worldContexts.has(params.worldName)) return [];
    state.nextContextId += 1;
    state.worldContexts.set(params.worldName, state.nextContextId);
    return [{
      method: "Runtime.executionContextCreated",
      params: {
        context: {
          id: state.nextContextId,
          origin: "file://",
          name: params.worldName,
          auxData: { frameId: state.frameId, isDefault: false, type: "isolated" },
        },
      },
    }];
  }
  return [];
}

function consumeWebSocketFrames(socket: Duplex, state: FixtureCdpState, incoming: Buffer): void {
  state.pending = Buffer.concat([state.pending, incoming]);
  while (state.pending.length >= 2) {
    const first = state.pending[0]!;
    const second = state.pending[1]!;
    const opcode = first & 0x0f;
    const masked = (second & 0x80) !== 0;
    let payloadLength = second & 0x7f;
    let offset = 2;
    if (payloadLength === 126) {
      if (state.pending.length < 4) return;
      payloadLength = state.pending.readUInt16BE(2);
      offset = 4;
    } else if (payloadLength === 127) {
      if (state.pending.length < 10) return;
      const largeLength = state.pending.readBigUInt64BE(2);
      if (largeLength > BigInt(Number.MAX_SAFE_INTEGER)) {
        socket.destroy();
        return;
      }
      payloadLength = Number(largeLength);
      offset = 10;
    }
    const maskOffset = offset;
    if (masked) offset += 4;
    const frameEnd = offset + payloadLength;
    if (state.pending.length < frameEnd) return;
    const payload = Buffer.from(state.pending.subarray(offset, frameEnd));
    if (masked) {
      const mask = state.pending.subarray(maskOffset, maskOffset + 4);
      for (let index = 0; index < payload.length; index += 1) payload[index] = payload[index]! ^ mask[index % 4]!;
    }
    state.pending = state.pending.subarray(frameEnd);
    if (opcode === 8) {
      socket.end();
      continue;
    }
    if (opcode === 9) {
      socket.write(webSocketFrame(10, payload));
      continue;
    }
    if (opcode !== 1) continue;
    try {
      const request = JSON.parse(payload.toString("utf8")) as {
        id?: number;
        method?: string;
        params?: Record<string, unknown>;
      };
      if (typeof request.id !== "number") continue;
      const response = Buffer.from(JSON.stringify({ id: request.id, result: cdpResult(request.method, state) }));
      socket.write(webSocketFrame(1, response));
      for (const event of cdpEvents(request.method, request.params, state)) {
        socket.write(webSocketFrame(1, Buffer.from(JSON.stringify(event))));
      }
    } catch {
      socket.destroy();
      return;
    }
  }
}

interface DebugEndpoint {
  server: Server;
  sockets: Set<Duplex>;
}

async function startDebugEndpoint(port: number, rendererUrl?: string): Promise<DebugEndpoint> {
  const sockets = new Set<Duplex>();
  const server = createServer((request, response) => {
    if (request.url === "/json/version") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        Browser: "OctoBuddy-launcher-fixture",
        "Protocol-Version": "1.3",
        webSocketDebuggerUrl: `ws://127.0.0.1:${request.socket.localPort}/devtools/browser/fixture`,
      }));
      return;
    }
    if (request.url === "/json/list") {
      const targets = rendererUrl ? [{
        id: "fixture-page",
        type: "page",
        title: "OctoBuddy fixture",
        url: rendererUrl,
        webSocketDebuggerUrl: `ws://127.0.0.1:${request.socket.localPort}/devtools/page/fixture-page`,
      }] : [];
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(targets));
      return;
    }
    response.writeHead(404);
    response.end("Not found");
  });
  server.on("upgrade", (request, socket, head) => {
    const key = request.headers["sec-websocket-key"];
    const pageSocket = rendererUrl !== undefined && request.url === "/devtools/page/fixture-page";
    if ((!pageSocket && request.url !== "/devtools/browser/fixture") || typeof key !== "string") {
      socket.destroy();
      return;
    }
    const accept = createHash("sha1")
      .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
      .digest("base64");
    socket.write([
      "HTTP/1.1 101 Switching Protocols",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Accept: ${accept}`,
      "",
      "",
    ].join("\r\n"));
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    const state: FixtureCdpState = {
      pending: Buffer.alloc(0),
      ...(pageSocket && rendererUrl ? { rendererUrl } : {}),
      frameId: "fixture-main-frame",
      nextScriptId: 0,
      nextContextId: 1,
      worldContexts: new Map(),
    };
    socket.on("data", (data) => consumeWebSocketFrames(socket, state, Buffer.from(data)));
    if (head.length > 0) consumeWebSocketFrames(socket, state, head);
  });
  const listening = once(server, "listening");
  server.listen(port, "127.0.0.1");
  await listening;
  return { server, sockets };
}

async function serveUntilStopped(
  port: number,
  marker: string,
  stopFile: string,
  launchLog?: string,
  rendererUrl?: string,
): Promise<void> {
  const { server, sockets } = await startDebugEndpoint(port, rendererUrl);
  const address = server.address() as AddressInfo;
  const identity = {
    pid: process.pid,
    port: address.port,
    startedAt: processStartIdentity(process.pid),
  };
  if (launchLog) appendFileSync(launchLog, `${JSON.stringify({ ...identity, argv: process.argv.slice(2) })}\n`);
  else writeFileSync(marker, JSON.stringify(identity), "utf8");
  process.stderr.write(`DevTools listening on ws://127.0.0.1:${address.port}/devtools/browser/fixture\n`);

  let stopping = false;
  let poll: NodeJS.Timeout;
  const stop = (): void => {
    if (stopping) return;
    stopping = true;
    clearInterval(poll);
    for (const socket of sockets) socket.destroy();
    server.close(() => process.exit(0));
  };
  poll = setInterval(() => {
    if (existsSync(stopFile)) stop();
  }, 15);
  process.once("SIGTERM", stop);
}


export async function runFixture(mode: string | undefined, args: string[]): Promise<void> {
  if (mode === "listener") {
    await serveUntilStopped(
      Number(requireOption(args, "--port")),
      requireOption(args, "--marker"),
      requireOption(args, "--stop-file"),
    );
    return;
  }

  if (mode === "app") {
    await serveUntilStopped(
      cdpPort(args),
      "",
      requireOption(args, "--stop-file"),
      requireOption(args, "--launch-log"),
      requireOption(args, "--renderer-url"),
    );
    return;
  }
  throw new Error(`unknown launcher fixture mode: ${String(mode)}`);
}

if (import.meta.main) {
  const [mode, ...args] = process.argv.slice(2);
  void runFixture(mode, args).catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
