#!/usr/bin/env bash
set -e

echo "Building Distributed ACID KV Store binaries..."
cargo build --release --workspace

echo "Starting Node 1..."
./target/release/kv-node --config config/node-1.toml > node-1.log 2>&1 &
PID1=$!

echo "Starting Node 2..."
./target/release/kv-node --config config/node-2.toml > node-2.log 2>&1 &
PID2=$!

echo "Starting Node 3..."
./target/release/kv-node --config config/node-3.toml > node-3.log 2>&1 &
PID3=$!

echo "Cluster started with PIDs: Node1=$PID1, Node2=$PID2, Node3=$PID3"
echo "Logs are being written to node-1.log, node-2.log, node-3.log"
wait $PID1 $PID2 $PID3
