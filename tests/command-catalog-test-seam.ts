import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
  BotIdentity,
  ChatTarget,
  ChannelService,
  ChannelCommandHandler,
  CoreServiceConfig,
  OmpWorker,
  WorkerFactory,
  WorkerOptions,
} from "../src/contracts.ts";
import { createChannelService } from "../src/service.ts";
import { openChannelStore, type ChannelStore } from "../src/store/index.ts";
import { FakeTransport, FakeWorkerFactory } from "./fixtures/core-fakes";

export interface TestOmpAvailableCommand {
  name: string;
  source: "builtin" | "file" | "skill" | "extension";
  description?: string;
  hint?: string;
  aliases?: string[];
  subcommands?: Array<{ name: string; description?: string; usage?: string }>;
}

export interface TestChatCommandSuggestion {
  name: string;
  source: "channel" | "omp";
  description?: string;
  hint?: string;
  aliases?: string[];
  subcommands?: Array<{ name: string; description?: string; hint?: string }>;
}

export interface TestChatCommandsSnapshot extends ChatTarget {
  version: 1;
  commands: TestChatCommandSuggestion[];
  omp: "available" | "unavailable";
}

export type AvailableCommandsHandler = (
  options: WorkerOptions,
  signal?: AbortSignal,
) => Promise<readonly TestOmpAvailableCommand[]>;

export type CommandCatalogService = Pick<ChannelService, "statusSnapshot" | "chatStatusSnapshot" | "chatDetailsSnapshot">
  & {
    chatCommandsSnapshot(target: ChatTarget, signal: AbortSignal): Promise<TestChatCommandsSnapshot | null>;
  };

export interface CommandCatalogHarness {
  root: string;
  stateDir: string;
  bot: BotIdentity;
  workers: FakeWorkerFactory;
  service: CommandCatalogService;
  store: ChannelStore;
  stop(): Promise<void>;
  close(): Promise<void>;
}

/** Decorates ordinary test workers with the RPC-only catalog seam. */
export function createCommandWorkerFactory(
  workers: FakeWorkerFactory,
  availableCommands: AvailableCommandsHandler,
  stopConfirmed: () => boolean = () => true,
): WorkerFactory {
  return {
    create: async (options) => {
      const worker = await workers.create(options);
      const commandWorker = worker as OmpWorker & {
        availableCommands?: (signal?: AbortSignal) => Promise<readonly TestOmpAvailableCommand[]>;
      };
      commandWorker.availableCommands = signal => availableCommands(options, signal);
      const stop = worker.stop.bind(worker);
      worker.stop = async () => stopConfirmed() ? stop() : false;
      return worker;
    },
    createCommit: options => workers.createCommit(options),
    recover: processInfo => workers.recover(processInfo),
  };
}

/** Builds a real CoreService with injected RPC workers while keeping all test state isolated. */
export async function createCommandCatalogHarness(options: {
  allowedGroupIds?: string[];
  workerFactory?: WorkerFactory;
  availableCommands?: AvailableCommandsHandler;
  commandHandlers?: readonly ChannelCommandHandler[];
  stopConfirmed?: () => boolean;
} = {}): Promise<CommandCatalogHarness> {
  const root = await mkdtemp(join(tmpdir(), "omp-channel-command-catalog-"));
  const stateDir = join(root, "state");
  await mkdir(stateDir, { recursive: true });
  const bot: BotIdentity = { uid: `command-catalog-bot-${process.pid}`, name: "Command Catalog Bot" };
  const transport = new FakeTransport({ bot });
  const workers = new FakeWorkerFactory();
  const workerFactory = options.workerFactory ?? createCommandWorkerFactory(
    workers,
    options.availableCommands ?? (async () => []),
    options.stopConfirmed,
  );
  const config = {
    ownerId: "owner-1",
    allowedGroupIds: options.allowedGroupIds ?? ["group-allowed"],
    stateDir,
    homeDir: root,
  } as CoreServiceConfig;
  const implementation = await createChannelService({
    config,
    transport,
    workerFactory,
    ...(options.commandHandlers === undefined ? {} : { commandHandlers: options.commandHandlers }),
  });

  try {
    await implementation.start();
    const store = await openChannelStore(stateDir);
    const service = implementation as unknown as CommandCatalogService;
    return {
      root,
      stateDir,
      bot,
      workers,
      service,
      store,
      stop: () => implementation.stop(),
      close: async () => {
        await implementation.stop();
        store.close();
        await rm(root, { recursive: true, force: true });
      },
    };
  } catch (error) {
    await implementation.stop().catch(() => {});
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}
