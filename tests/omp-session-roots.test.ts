import { expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join, relative } from "node:path";
import { createWorkerFactory } from "../src/omp/index.ts";

type SessionRootFactory = {
	readonly sessionRootsFor?: (cwd: string) => Promise<readonly string[]>;
};

const ENVIRONMENT_KEYS = [
	"HOME",
	"OMP_PROFILE",
	"PI_PROFILE",
	"PI_CONFIG_DIR",
	"PI_CODING_AGENT_DIR",
	"XDG_DATA_HOME",
	"XDG_STATE_HOME",
	"XDG_CACHE_HOME",
	"OMP_CODING_AGENT_DIR",
	"NODE_ENV",
] as const;

type EnvironmentKey = (typeof ENVIRONMENT_KEYS)[number];

function saveEnvironment(): Map<EnvironmentKey, string | undefined> {
	return new Map(ENVIRONMENT_KEYS.map(key => [key, process.env[key]]));
}

function restoreEnvironment(previous: Map<EnvironmentKey, string | undefined>): void {
	for (const key of ENVIRONMENT_KEYS) {
		const value = previous.get(key);
		if (value === undefined) delete process.env[key];
		else process.env[key] = value;
	}
}

function configDirFromHome(configRoot: string): string {
	const value = relative(homedir(), configRoot);
	return value.length > 0 ? value : ".";
}

async function rootsFor(factory: SessionRootFactory, cwd: string): Promise<readonly string[]> {
	const resolver = factory.sessionRootsFor;
	expect(resolver).toEqual(expect.any(Function));
	if (typeof resolver !== "function") throw new Error("sessionRootsFor is unavailable");
	return resolver.call(factory, cwd);
}

test("derives the default worker session root from the worker cwd project .env", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-project-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const projectData = join(root, "project-data");
	const previous = saveEnvironment();
	try {
		await mkdir(join(projectData, "omp"), { recursive: true });
		await mkdir(project, { recursive: true });
		await writeFile(join(project, ".env"), `XDG_DATA_HOME=${projectData}\n`);
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.OMP_PROFILE;
		delete process.env.PI_PROFILE;
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(projectData, "omp", "sessions")]);
		expect(process.env.XDG_DATA_HOME).toBeUndefined();
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("loads a named profile agent .env when deriving its XDG session root", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-profile-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const profileAgent = join(configRoot, "profiles", "work", "agent");
	const profileData = join(root, "profile-data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(profileAgent, { recursive: true });
		await mkdir(join(profileData, "omp", "profiles", "work"), { recursive: true });
		await writeFile(join(profileAgent, ".env"), `XDG_DATA_HOME=${profileData}\n`);
		process.env.OMP_PROFILE = "work";
		process.env.PI_PROFILE = "ignored";
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(profileData, "omp", "profiles", "work", "sessions")]);
		expect(process.env.OMP_PROFILE).toBe("work");
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("explicit default profile ignores a profile-derived agent override and loads default .env", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-default-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const defaultAgent = join(configRoot, "agent");
	const profileAgent = join(configRoot, "profiles", "work", "agent");
	const defaultData = join(root, "default-data");
	const profileData = join(root, "profile-data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(defaultAgent, { recursive: true });
		await mkdir(profileAgent, { recursive: true });
		await mkdir(join(defaultData, "omp"), { recursive: true });
		await mkdir(join(profileData, "omp", "profiles", "work"), { recursive: true });
		await writeFile(join(defaultAgent, ".env"), `XDG_DATA_HOME=${defaultData}\n`);
		await writeFile(join(profileAgent, ".env"), `XDG_DATA_HOME=${profileData}\n`);
		process.env.OMP_PROFILE = "work";
		process.env.PI_PROFILE = "work";
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		process.env.PI_CODING_AGENT_DIR = profileAgent;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath, extraArgs: ["--profile", "default"] });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(defaultData, "omp", "sessions")]);
		expect(process.env.PI_CODING_AGENT_DIR).toBe(profileAgent);
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("includes Bun-loaded project .env.local and expands HOME for the worker cwd", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-dotenv-local-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const envHome = join(root, "env-home");
	const localData = join(envHome, "local-data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(join(localData, "omp"), { recursive: true });
		await writeFile(join(project, ".env.local"), "XDG_DATA_HOME=$HOME/local-data\n");
		process.env.HOME = envHome;
		process.env.NODE_ENV = "test";
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.OMP_PROFILE;
		delete process.env.PI_PROFILE;
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(localData, "omp", "sessions")]);
		expect(process.env.XDG_DATA_HOME).toBeUndefined();
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("does not steal a profile-looking value consumed by --system-prompt", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-profile-argv-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const defaultAgent = join(configRoot, "agent");
	const profileAgent = join(configRoot, "profiles", "work", "agent");
	const defaultData = join(root, "default-data");
	const profileData = join(root, "profile-data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(defaultAgent, { recursive: true });
		await mkdir(profileAgent, { recursive: true });
		await mkdir(join(defaultData, "omp"), { recursive: true });
		await mkdir(join(profileData, "omp", "profiles", "work"), { recursive: true });
		await writeFile(join(defaultAgent, ".env"), `XDG_DATA_HOME=${defaultData}\n`);
		await writeFile(join(profileAgent, ".env"), `XDG_DATA_HOME=${profileData}\n`);
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.OMP_PROFILE;
		delete process.env.PI_PROFILE;
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({
			ompCommand: process.execPath,
			extraArgs: ["--system-prompt", "--profile=work"],
		});
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(defaultData, "omp", "sessions")]);
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("uses an explicit valid profile despite an invalid inherited profile", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-profile-invalid-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const profileAgent = join(configRoot, "profiles", "work", "agent");
	const profileData = join(root, "profile-data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(profileAgent, { recursive: true });
		await mkdir(join(profileData, "omp", "profiles", "work"), { recursive: true });
		await writeFile(join(profileAgent, ".env"), `XDG_DATA_HOME=${profileData}\n`);
		process.env.OMP_PROFILE = "BAD!";
		delete process.env.PI_PROFILE;
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath, extraArgs: ["--profile", "work"] });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(profileData, "omp", "profiles", "work", "sessions")]);
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("resolves a relative XDG data home against the worker cwd", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-relative-xdg-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const projectData = join(project, "data");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(join(projectData, "omp"), { recursive: true });
		await writeFile(join(project, ".env"), "XDG_DATA_HOME=./data\n");
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.OMP_PROFILE;
		delete process.env.PI_PROFILE;
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(projectData, "omp", "sessions")]);
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});

test("ignores an OMP agent-dir alias loaded only from project .env.local", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "omp-session-roots-local-alias-")));
	const configRoot = join(root, "config");
	const project = join(root, "project");
	const customAgent = join(root, "custom-agent");
	const previous = saveEnvironment();
	try {
		await mkdir(project, { recursive: true });
		await mkdir(customAgent, { recursive: true });
		await writeFile(join(project, ".env.local"), `OMP_CODING_AGENT_DIR=${customAgent}\n`);
		process.env.PI_CONFIG_DIR = configDirFromHome(configRoot);
		delete process.env.OMP_PROFILE;
		delete process.env.PI_PROFILE;
		delete process.env.OMP_CODING_AGENT_DIR;
		delete process.env.PI_CODING_AGENT_DIR;
		delete process.env.XDG_DATA_HOME;
		delete process.env.XDG_STATE_HOME;
		delete process.env.XDG_CACHE_HOME;

		const factory = createWorkerFactory({ ompCommand: process.execPath });
		const roots = await rootsFor(factory, project);

		expect(roots).toEqual([join(configRoot, "agent", "sessions")]);
	} finally {
		restoreEnvironment(previous);
		await rm(root, { recursive: true, force: true });
	}
});
