// Benchmark-only random stream: fixes retry jitter and synthetic message IDs.
let state = 20260920;
Math.random = () => {
  state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
  return state / 0x1_0000_0000;
};

// These workloads use in-process transports and local RPC children only.
const forbidNetwork = () => {
  throw new Error("Network fetch is forbidden in the stability benchmark");
};
globalThis.fetch = Object.assign(forbidNetwork, { preconnect: forbidNetwork });
