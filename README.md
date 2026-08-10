# Distributed ACID Key-Value Store

A high-performance, fault-tolerant, horizontally scalable distributed key-value store built in Rust with strict ACID transactional guarantees.

## Architecture Highlights

- **Multi-Raft Consensus (`raft-engine`)**: Key-range sharded Multi-Raft implementation supporting dynamic region splits (at 64 MB thresholds), automatic leader election, snapshot transfers, and active load distribution across nodes.
- **Percolator 2-Phase Commit (`txn-coordinator`)**: Decentralized 2PC implementation featuring primary & secondary lock resolution, Lock TTL Heartbeats, Conflict Detection via Hybrid Logical Clocks (HLC), write conflict rollbacks, permanent `Rollback` records, and automatic lock resolution for expired TTLs.
- **MVCC Storage Engine (`storage`)**: Partition-separated storage (`DEFAULT`, `LOCK`, `WRITE`) backed by Fjall engine with Multi-Version Concurrency Control timestamp encoding for lock-free point lookups, MVCC range scans, snapshot reads, and background MVCC Garbage Collection (`gc_keys_older_than`).
- **gRPC Node Server (`node`) & Client SDK (`client`)**: High-throughput Tokio + Tonic gRPC execution node alongside a CLI client tool (`kv-cli`) with `get`, `put`, and `scan` support.
- **Deterministic Fault Testing & Benchmarks (`simulation`)**: `madsim`-ready fault injection test suite verifying leader election, 2PC lock resolution, MVCC range scan correctness, and Criterion micro-benchmarks (`transaction_bench`).

## Workspace Crate Structure

```
crates/
├── proto/           # Protobuf compilation & gRPC modules
├── storage/         # Fjall storage engine, MVCC timestamp encoding & GC
├── raft-engine/     # Multi-Raft consensus engine & range router
├── txn-coordinator/ # Percolator 2PC engine, HLC & lock resolver
├── node/            # Server binary execution engine & Tokio gRPC server
├── client/          # Client SDK & kv-cli binary
└── simulation/      # Deterministic fault simulation & Criterion benchmarks
```

## Quick Start

### Building
```bash
cargo build --workspace
```

### Running Local 3-Node Cluster
```bash
./scripts/start-cluster.sh
```

Or using Docker Compose:
```bash
docker-compose up -d
```

### Client CLI Usage
```bash
# Write key-value pair via 2PC transaction
cargo run --bin kv-cli -- put mykey myvalue

# Read value at current HLC timestamp
cargo run --bin kv-cli -- get mykey

# Range scan keys in range [start_key, end_key)
cargo run --bin kv-cli -- scan mykey1 mykey9 100
```

### Running Tests & Simulation
```bash
cargo test --workspace
```

### Running Benchmarks
```bash
./scripts/run-benchmarks.sh
# Or directly run criterion benchmark:
cargo bench -p simulation
```
