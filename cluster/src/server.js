import { fileURLToPath } from "node:url";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

import { parseArgs } from "./config.js";
import { Storage } from "./storage.js";
import { KvStateMachine } from "./state-machine.js";
import { RaftNode } from "./raft.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROTO_PATH = path.resolve(__dirname, "../../proto/raft.proto");

async function main() {
  const cfg = parseArgs(process.argv);
  if (!cfg.id || !cfg.port) {
    console.error(
      "uso: node server.js --id <id> --port <port> --peers <id=host:port,...>",
    );
    process.exit(1);
  }

  const storage = new Storage(cfg.dataDir);
  await storage.init();

  const sm = new KvStateMachine();
  const node = new RaftNode({
    id: cfg.id,
    peers: cfg.peers,
    storage,
    stateMachine: sm,
  });

  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDef).raft;

  const server = new grpc.Server();

  server.addService(proto.RaftInternal.service, {
    requestVote: async (call, callback) => {
      try {
        const reply = await node.handleRequestVote(call.request);
        callback(null, reply);
      } catch (err) {
        callback(err);
      }
    },

    appendEntries: async (call, callback) => {
      try {
        const reply = await node.handleAppendEntries(call.request);
        callback(null, reply);
      } catch (err) {
        callback(err);
      }
    },
  });

  server.addService(proto.RaftClient.service, {
    submit: async (call, callback) => {
      const { command } = call.request;
      if (!command || !command.op || !command.key) {
        return callback(null, { ok: false, error: "invalid_command" });
      }

      try {
        await node.submitCommand(command);
        callback(null, { ok: true });
      } catch (err) {
        if (err.message === "not_leader") {
          return callback(null, {
            ok: false,
            error: "not_leader",
            leaderId: err.leaderId || "",
            leaderAddress: err.leaderAddress || "",
          });
        }
        if (
          err.message === "not_leader_anymore" ||
          err.message === "entry_overwritten"
        ) {
          return callback(null, { ok: false, error: err.message });
        }
        callback(null, { ok: false, error: err.message });
      }
    },
  });

  const bindAddr = `0.0.0.0:${cfg.port}`;
  server.bindAsync(
    bindAddr,
    grpc.ServerCredentials.createInsecure(),
    (err, boundPort) => {
      if (err) {
        console.error("erro ao iniciar gRPC:", err);
        process.exit(1);
      }
      console.log(
        `[${cfg.id}] gRPC server on :${boundPort}, peers=${Object.keys(cfg.peers).join(",")}`,
      );
      node.start();
    },
  );
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
