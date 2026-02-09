#!/bin/bash
set -e

# Cleanup
echo "Cleaning up..."
pkill -f "bin/storage" || true
pkill -f "bin/gateway" || true
lsof -ti:8081,8082,8083,8090 | xargs kill -9 2>/dev/null || true
rm -rf /tmp/metadata-test-*
sleep 1

# Build
echo "Building..."
go build -o bin/gateway ./cmd/gateway
go build -o bin/storage ./cmd/storage

# Generate Configs
mkdir -p configs
cat <<EOF > configs/storage-1.yaml
server:
  node_id: storage-1
  hostname: 127.0.0.1
  port: 8081
gossip:
  port: 9081
  seeds: []
lsm:
  data_dir: /tmp/metadata-test-1
EOF

cat <<EOF > configs/storage-2.yaml
server:
  node_id: storage-2
  hostname: 127.0.0.1
  port: 8082
gossip:
  port: 9082
  seeds: ["127.0.0.1:9081"]
lsm:
  data_dir: /tmp/metadata-test-2
EOF

cat <<EOF > configs/gateway.yaml
server:
  addr: ":8090"
app:
  chunk_size: 8388608
  max_file_size: 2147483648
  parallel_chunks: 4
  replicas: 2
storage:
  seeds: ["127.0.0.1:8081", "127.0.0.1:8082"]
EOF

# Start Nodes
echo "Starting Nodes..."
./bin/storage -configPath configs/storage-1.yaml > storage1.log 2>&1 &
PID1=$!
sleep 1
./bin/storage -configPath configs/storage-2.yaml > storage2.log 2>&1 &
PID2=$!
sleep 1
./bin/gateway -configPath configs/gateway.yaml > gateway.log 2>&1 &
PID_GW=$!

sleep 5

echo "Testing Upload with Metadata..."
echo "Metadata test content" > meta_test.txt
curl -v -F "file=@meta_test.txt" http://localhost:8090/files

echo -e "\nFetching Metadata..."
curl -v "http://localhost:8090/files/metadata?id=meta_test.txt"

echo -e "\nCleaning up..."
kill $PID1 $PID2 $PID_GW || true
rm meta_test.txt
rm -rf configs
