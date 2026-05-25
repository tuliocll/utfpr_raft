# Trabalho - Algoritmo de Consenso Raft com gRPC

Disciplina de Sistemas Distribuídos - UTFPR.

Implementação do algoritmo Raft com 4 nós em Node.js, cliente em Python, comunicação via gRPC + Protocol Buffers.

## Como rodar

**1 Subir o cluster:**

```bash
cd cluster
npm install
./run.sh bg
```

**2 Rodar comandos:**

```bash
cd client
python -m venv .venv
source .venv/bin/activate.fish   # ou activate caso use bash/zsh
pip install grpcio grpcio-tools

python client.py SET x 10
python client.py DELETE x
```
