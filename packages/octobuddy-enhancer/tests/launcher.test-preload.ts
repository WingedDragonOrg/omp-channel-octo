import * as fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join, resolve } from "node:path";

const lockFile = process.env.OCTOBUDDY_TEST_LOCK_FILE;
const gateDirectory = process.env.OCTOBUDDY_TEST_LOCK_GATE;
const role = process.env.OCTOBUDDY_TEST_LOCK_ROLE;

if (lockFile && gateDirectory && role) {
  const unlink = fs.promises.unlink.bind(fs.promises);
  fs.promises.unlink = async (path) => {
    if (resolve(path.toString()) === resolve(lockFile)) {
      fs.writeFileSync(join(gateDirectory, `${role}.unlink-waiting`), "ready\n", "utf8");
      const release = join(gateDirectory, `${role}.unlink-release`);
      await new Promise<void>((resolveGate) => {
        const timer = setInterval(() => {
          if (!fs.existsSync(release)) return;
          clearInterval(timer);
          resolveGate();
        }, 5);
      });
    }
    await unlink(path);
  };
  syncBuiltinESMExports();
}
