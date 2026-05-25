export function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 2) {
    const key = argv[i].replace(/^--/, "");
    args[key] = argv[i + 1];
  }

  const peers = {};
  if (args.peers) {
    for (const entry of args.peers.split(",")) {
      const [id, address] = entry.split("=");
      peers[id] = address;
    }
  }

  return {
    id: args.id,
    port: parseInt(args.port, 10),
    peers,
    dataDir: args.dataDir || `./data/${args.id}`,
  };
}
