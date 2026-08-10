
# Distributed ACID Key-Value Store

A high-performance, fault-tolerant, horizontally scalable distributed key-value store built in Rust with strict ACID transactional guarantees.

## Architecture Highlights

- **Multi-Raft Consensus (`raft-engine`)**: Key-range sharded Multi-Raft implementation supporting Dynamic Region Range Splitting (at configurable 10,000 keys or 64MB thresholds), `SplitCmd` Raft log consensus, `RegionEpoch` tracking, automatic leader election, snapshot transfers, and active load distribution across nodes.
- **Percolator 2-Phase Commit (`txn-coordinator`)**: Decentralized 2PC implementation featuring primary & secondary lock resolution, Lock TTL Heartbeats, Conflict Detection via Hybrid Logical Clocks (HLC), write conflict rollbacks, permanent `Rollback` records, and non-blocking Percolator lock resolution for expired TTLs.
- **Asynchronous gRPC Request Batching & Pipelining (`client` & `node`)**: Thread-safe `BatchCollector` in client SDK buffering prewrite and commit mutations into high-throughput micro-batches (flushed every 2ms or 128 keys) over `BatchPrewrite` and `BatchCommit` gRPC RPCs, executed atomically in single Fjall database transaction batches on storage engine nodes.
- **Full Multi-Raft gRPC Network Pipeline (`raft-engine` & `node`)**: Real streaming gRPC network transport over `RaftService::Step` and `SendMessage` with Fjall disk persistence for Raft `HardState`, `ConfState`, snapshots, and uncommitted log entries in background `RawNode::ready()` event processing loops.
- **MVCC Storage Engine (`storage`)**: Partition-separated storage (`DEFAULT`, `LOCK`, `WRITE`) backed by Fjall engine with Multi-Version Concurrency Control timestamp encoding for lock-free point lookups, MVCC range scans, snapshot reads, and background MVCC Garbage Collection (`gc_keys_older_than`).
- **Jepsen-Style Chaos Fault-Injection Harness (`simulation`)**: Async `FaultyTransport` proxy layer capable of injecting network partitions (isolated leaders), mid-2PC node crashes, and 30% asymmetric packet loss, validated by `test_chaos_linearizability_under_partition` and `test_chaos_orphan_lock_cleanup_on_node_crash`.

## Workspace Crate Structure

```
crates/
├── proto/           # Protobuf compilation, KvService, TxnService & RaftService gRPC definitions
├── storage/         # Fjall storage engine, MVCC timestamp encoding, GC & median split key finder
├── raft-engine/     # Multi-Raft consensus engine, RaftStorage persistence, RaftCmd & RegionRouter
├── txn-coordinator/ # Percolator 2PC engine, HLC, lock resolver & atomic Fjall batch execution
├── node/            # Node server engine, Tokio gRPC services & background Raft ready processing loops
├── client/          # Client SDK, BatchCollector & kv-cli binary
└── simulation/      # Jepsen-style chaos fault injection, unit/integration tests & Criterion micro-benchmarks
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
<img width="1920" height="1080" alt="Screenshot 2026-08-10 230142" src="https://github.com/user-attachments/assets/d7683bba-0b23-4885-892a-66d7283b6995" />
<img width="1920" height="1080" alt="Screenshot 2026-08-10 230130" src="https://github.com/user-attachments/assets/50bd86e1-e9ae-41b4-84cd-1d377eeeabaf" />

