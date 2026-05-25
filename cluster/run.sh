#!/bin/bash
# Sobe 4 nós Raft, todos no localhost.

set -e
cd "$(dirname "$0")"

rm -rf data/

PEERS_N1="n2=localhost:50052,n3=localhost:50053,n4=localhost:50054"
PEERS_N2="n1=localhost:50051,n3=localhost:50053,n4=localhost:50054"
PEERS_N3="n1=localhost:50051,n2=localhost:50052,n4=localhost:50054"
PEERS_N4="n1=localhost:50051,n2=localhost:50052,n3=localhost:50053"


node src/server.js --id n1 --port 50051 --peers $PEERS_N1 > /tmp/raft-n1.log 2>&1 &
echo "n1 pid: $!"
node src/server.js --id n2 --port 50052 --peers $PEERS_N2 > /tmp/raft-n2.log 2>&1 &
echo "n2 pid: $!"
node src/server.js --id n3 --port 50053 --peers $PEERS_N3 > /tmp/raft-n3.log 2>&1 &
echo "n3 pid: $!"
node src/server.js --id n4 --port 50054 --peers $PEERS_N4 > /tmp/raft-n4.log 2>&1 &
echo "n4 pid: $!"
echo "logs em /tmp/raft-n*.log"
wait
