import { fileURLToPath } from "node:url";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROTO_PATH = path.resolve(__dirname, "../../proto/raft.proto");

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});
const protoDescriptor = grpc.loadPackageDefinition(packageDef);
const RaftInternalService = protoDescriptor.raft.RaftInternal;

const stubCache = new Map();

function getStub(address) {
  if (!stubCache.has(address)) {
    const stub = new RaftInternalService(
      address,
      grpc.credentials.createInsecure(),
    );
    stubCache.set(address, stub);
  }
  return stubCache.get(address);
}

const RPC_MAP = {
  "request-vote": "requestVote",
  "append-entries": "appendEntries",
};

export function callRpc(peerAddress, rpcName, payload, timeoutMs = 200) {
  return new Promise((resolve) => {
    const stub = getStub(peerAddress);
    const methodName = RPC_MAP[rpcName];

    if (!methodName) {
      console.error(`RPC desconhecido: ${rpcName}`);
      return resolve(null);
    }

    const deadline = new Date(Date.now() + timeoutMs);

    stub[methodName](payload, { deadline }, (err, response) => {
      if (err) {
        return resolve(null);
      }
      resolve(response);
    });
  });
}
