import { writeFileSync } from "node:fs";

const markerPath = process.env.OMP_EXTENSION_MARKER;
if (!markerPath) throw new Error("OMP extension marker path is required");
writeFileSync(markerPath, "loaded\n", "utf8");

export default function markerExtension(): void {}
