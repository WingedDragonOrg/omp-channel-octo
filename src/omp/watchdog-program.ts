
export function runWatchdog(watchdogArgs: readonly string[]): void {
  const parentPid = Number(watchdogArgs[0]);
  const workerPid = Number(watchdogArgs[1]);
  const workerPgid = Number(watchdogArgs[2]);
  const parentIdentity = watchdogArgs[3] ?? "";
  const configuredKillGraceMs = Number(watchdogArgs[4]);
  const terminateOnWorkerExit = watchdogArgs[5] !== "false";
  const killGraceMs = Number.isFinite(configuredKillGraceMs) ? Math.max(0, configuredKillGraceMs) : 10_000;
  const decoder = new TextDecoder();
  let terminating = false;
  let killSent = false;
  let killDeadline = 0;

  function processAlive(pid: number): boolean {
    if (!Number.isSafeInteger(pid) || pid < 2) return false;
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return error !== null && typeof error === "object" && "code" in error && error.code === "EPERM";
    }
  }

  function processStart(pid: number): string {
    try {
      const result = Bun.spawnSync(["/bin/ps", "-p", String(pid), "-o", "lstart="]);
      if (result.exitCode !== 0) return "";
      return decoder.decode(result.stdout).trim();
    } catch {
      return "";
    }
  }

  function parentAlive(): boolean {
    if (!processAlive(parentPid)) return false;
    if (!parentIdentity) return true;
    const current = processStart(parentPid);
    return current.length > 0 && `pid=${parentPid};start=${current}` === parentIdentity;
  }

  function groupAlive(): boolean {
    if (!Number.isSafeInteger(workerPgid) || workerPgid < 2) return false;
    try {
      process.kill(-workerPgid, 0);
      return true;
    } catch (error) {
      return error !== null && typeof error === "object" && "code" in error && error.code === "EPERM";
    }
  }

  function signalGroup(signal: NodeJS.Signals): boolean {
    if (!groupAlive()) return false;
    try {
      process.kill(-workerPgid, signal);
      return true;
    } catch {
      return false;
    }
  }

  function beginTermination(): void {
    if (!terminating) {
      terminating = true;
      killDeadline = Date.now() + killGraceMs;
      signalGroup("SIGTERM");
    }
    if (!groupAlive()) {
      process.exit(0);
    }
    if (Date.now() >= killDeadline) {
      if (killSent) {
        process.exit(1);
      }
      killSent = true;
      killDeadline = Date.now() + 1_000;
      signalGroup("SIGKILL");
    }
  }

  process.stdout.write("__omp_watchdog_ready__\n");
  setInterval(() => {
    if (terminating) {
      beginTermination();
      return;
    }
    if (!parentAlive() || (terminateOnWorkerExit && !processAlive(workerPid)) || !groupAlive()) beginTermination();
  }, 100);
}
