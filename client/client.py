import sys
import time
import grpc

import raft_pb2
import raft_pb2_grpc

NODES = [
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
    "localhost:50054",
]

MAX_ATTEMPTS = 8


def parse_args(args):
    if len(args) < 2:
        return None
    op = args[0].upper()
    if op == "SET" and len(args) == 3:
        return raft_pb2.Command(op="set", key=args[1], value=args[2])
    if op == "DELETE" and len(args) == 2:
        return raft_pb2.Command(op="delete", key=args[1])
    return None


def try_submit(address, command):
    try:
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftClientStub(channel)
            reply = stub.Submit(
                raft_pb2.SubmitRequest(command=command),
                timeout=2.0,
            )
            return reply, None
    except grpc.RpcError as e:
        return None, e.code().name


def submit_with_retry(command):
    candidates = list(NODES)

    for attempt in range(MAX_ATTEMPTS):
        if not candidates:
            print("  nenhum nó respondeu, aguardando eleição...", file=sys.stderr)
            time.sleep(0.8)
            candidates = list(NODES)
            continue

        address = candidates.pop(0)
        print(f"  tentando {address}...", file=sys.stderr)

        reply, err = try_submit(address, command)

        if err is not None:
            print(f"  -> {address} não respondeu ({err})", file=sys.stderr)
            continue

        if reply.ok:
            return reply

        if reply.error == "not_leader" and reply.leader_address:
            print(f"  -> {address} é follower, líder é {reply.leader_address}",
                  file=sys.stderr)
            candidates.insert(0, reply.leader_address)
            continue

        if reply.error == "not_leader":
            print(f"  -> {address} sem líder conhecido, tentando próximo",
                  file=sys.stderr)
            continue

        print(f"  -> erro temporário: {reply.error}", file=sys.stderr)
        time.sleep(0.3)

    return None


def main():
    command = parse_args(sys.argv[1:])
    if command is None:
        print("uso:\n  python client.py SET <chave> <valor>\n"
              "  python client.py DELETE <chave>", file=sys.stderr)
        sys.exit(1)

    reply = submit_with_retry(command)
    if reply is None:
        print("FALHA: máximo de tentativas alcançado", file=sys.stderr)
        sys.exit(1)

    print("OK")
    print(f"resposta: ok={reply.ok}")


if __name__ == "__main__":
    main()
