#!/usr/bin/env bash
set -e

echo "=== Running Distributed ACID KV Store Benchmarks ==="

echo "1. Running Cargo benchmark suites..."
cargo bench --workspace

echo "2. Running latency and throughput simulation tests..."
cargo test -p simulation --release -- --nocapture

echo "=== Benchmarks Complete ==="