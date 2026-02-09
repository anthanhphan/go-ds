#!/bin/bash
set -e

# Cleanup
echo "Cleaning up..."
rm -rf /tmp/storage-1 /tmp/storage-2 /tmp/storage-3 /tmp/gateway-data
pkill -f "bin/storage" || true
pkill -f "bin/gateway" || true
# Ensure ports are free
lsof -ti:8081,8082,8083,9081,9082,9083,8090 | xargs kill -9 2>/dev/null || true
sleep 2

# Build
echo "Building..."
go build -o bin/gateway ./cmd/gateway
go build -o bin/storage ./cmd/storage

# Create temporary config files
echo "Generating configs..."
mkdir -p configs

cat <<EOF > configs/storage-1.yaml
server:
  node_id: storage-1
  hostname: 127.0.0.1
  port: 8081
  shard_id: shard-1
  replica_id: 1
gossip:
  port: 9081
  seeds: []
lsm:
  data_dir: /tmp/storage-1
EOF

cat <<EOF > configs/storage-2.yaml
server:
  node_id: storage-2
  hostname: 127.0.0.1
  port: 8082
  shard_id: shard-1
  replica_id: 2
gossip:
  port: 9082
  seeds: ["127.0.0.1:9081"]
lsm:
  data_dir: /tmp/storage-2
EOF

cat <<EOF > configs/storage-3.yaml
server:
  node_id: storage-3
  hostname: 127.0.0.1
  port: 8083
  shard_id: shard-2
  replica_id: 1
gossip:
  port: 9083
  seeds: ["127.0.0.1:9081"]
lsm:
  data_dir: /tmp/storage-3
EOF

cat <<EOF > configs/storage-4.yaml
server:
  node_id: storage-4
  hostname: 127.0.0.1
  port: 8084
  shard_id: shard-2
  replica_id: 2
gossip:
  port: 9084
  seeds: ["127.0.0.1:9081"]
lsm:
  data_dir: /tmp/storage-4
EOF

cat <<EOF > configs/gateway.yaml
server:
  addr: ":8090"
app:
  chunk_size: 8388608
  max_file_size: 2147483648
  consistency: quorum
  replicas: 2
storage:
  seeds: ["127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083", "127.0.0.1:8084"]
EOF

# Start Storage Nodes
echo "Starting Storage Nodes..."
./bin/storage -configPath configs/storage-1.yaml > storage1.log 2>&1 &
PID1=$!
sleep 1
./bin/storage -configPath configs/storage-2.yaml > storage2.log 2>&1 &
PID2=$!
sleep 1
./bin/storage -configPath configs/storage-3.yaml > storage3.log 2>&1 &
PID3=$!
sleep 1
./bin/storage -configPath configs/storage-4.yaml > storage4.log 2>&1 &
PID4=$!

# Start Gateway
echo "Starting Gateway..."
./bin/gateway -configPath configs/gateway.yaml > gateway.log 2>&1 &
PID_GW=$!

sleep 5

# Create a test file
echo "Creating test file (20MB)..."
dd if=/dev/urandom of=testfile.dat bs=1M count=20

# Upload
echo "Uploading file..."
curl -v -F "file=@testfile.dat" http://localhost:8090/files

echo "Upload complete. Waiting for replication..."
sleep 2

# Verify
echo "Verifying shards..."
SIZE1=$(du -s /tmp/storage-1/data.log 2>/dev/null | awk '{print $1}') || SIZE1=0
SIZE2=$(du -s /tmp/storage-2/data.log 2>/dev/null | awk '{print $1}') || SIZE2=0
SIZE3=$(du -s /tmp/storage-3/data.log 2>/dev/null | awk '{print $1}') || SIZE3=0
SIZE4=$(du -s /tmp/storage-4/data.log 2>/dev/null | awk '{print $1}') || SIZE4=0

echo "Shard 1 (Replica 1): $SIZE1"
echo "Shard 1 (Replica 2): $SIZE2"
echo "Shard 2 (Replica 1): $SIZE3"
echo "Shard 2 (Replica 2): $SIZE4"

if [ "$SIZE1" == "$SIZE2" ] && [ "$SIZE3" == "$SIZE4" ]; then
    echo "SUCCESS: Replicas in the same shard have identical sizes."
else
    echo "WARNING: Replica sizes differ (consistent hashing might distribute unevenly, but within shard should match if all uploaded)."
fi

# Download
echo "Downloading file..."
curl -v "http://localhost:8090/files?id=testfile.dat&consistency=quorum" -o downloaded.dat

echo "Verifying integrity..."
MD5_ORIG=$(md5sum testfile.dat | awk '{print $1}')
MD5_DOWN=$(md5sum downloaded.dat | awk '{print $1}')

if [ "$MD5_ORIG" == "$MD5_DOWN" ]; then
    echo "SUCCESS: Downloaded file matches original."
else
    echo "FAILURE: Downloaded file mismatch."
    echo "Original: $MD5_ORIG"
    echo "Downloaded: $MD5_DOWN"
    exit 1
fi

# Cleanup
echo "Cleaning up..."
kill $PID1 $PID2 $PID3 $PID4 $PID_GW || true
rm testfile.dat downloaded.dat
rm -rf configs
