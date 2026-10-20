# raftkv

A distributed, replicated key-value store built from scratch in Go, using a
self-implemented Raft consensus protocol. This is a learning project aimed at
building production-grade distributed-systems intuition: durability, consensus,
concurrency correctness, and observability — not just a CRUD API.

Inspired by the internals of systems like etcd, TiKV, and CockroachDB, scaled
down to a single-developer, learnable size.

## Status

🚧 Early development. See [TODO.md](./TODO.md) for the full build plan and
current progress.

Current phase: **Phase 2: Networking Layer**

## Goals

- Implement Raft consensus from scratch (leader election, log replication,
  snapshots, membership changes, linearizable reads) — not via a library,
  at least initially.
- Build a durable single-node storage engine (WAL + snapshotting) before
  adding replication.
- Prove correctness under real fault injection (network partitions, node
  crashes, retries) rather than just happy-path testing.
- Instrument the system properly: structured logs, Prometheus metrics,
  OpenTelemetry tracing.

## Non-goals (v1)

- No cross-shard transactions (sharding is a stretch goal; if implemented,
  cross-shard atomicity is explicitly out of scope).
- No SQL layer — this is a raw KV store.
- No client-side load balancing beyond basic leader discovery.

## Architecture (high level)

```
client ──gRPC──> [ Node A: API layer ]
                        │
                        ▼
                  [ Raft consensus ]──gRPC──> Node B, Node C, ...
                        │
                        ▼
                  [ Storage engine ]
                   (WAL + snapshot)
```

Each node runs the API layer, the Raft state machine, and the storage engine.
Writes go through the Raft leader and are only acknowledged once committed by
quorum. Reads use a lease or read-index protocol to guarantee linearizability.

## Consistency model

- **Writes:** linearizable, committed only after quorum acknowledgment.
- **Reads:** linearizable (via leader lease / read-index — see TODO).
- **Durability:** an acknowledged write survives a crash of any node,
  including the one that accepted it, as long as quorum survives.

## Running locally

Build the server and the CLI:

```bash
go build -o bin/kvserver ./cmd/kvserver
go build -o bin/kvcli ./cmd/kvcli
```

Start a server (data is kept in `./data/` and survives restarts; Ctrl+C stops it gracefully):

```bash
./bin/kvserver --addr :7000 --wal ./data/raftkv.wal
```

Use it from another terminal:

```bash
./bin/kvcli put name alice
./bin/kvcli get name          # alice
./bin/kvcli delete name
./bin/kvcli get name          # "not found", exit code 1
./bin/kvcli snapshot          # snapshot the state and truncate the WAL
```

`kvcli` exit codes: 0 ok, 1 not found, 2 any other failure. Use
`-addr host:port` before the command for a server that is not on `localhost:7000`.

Server flags: `--addr`, `--wal`, `--max-inflight`, `--max-wait`, `--drain-timeout`
(run `./bin/kvserver -h` for defaults).

Acceptance demo (kill -9 recovery, snapshot, shutdown under load):

```bash
./demo.sh
```

## Testing

This project treats fault-injection testing as a first-class citizen, not an
afterthought:

- **Kill-9 tests**: random-point process kills to verify durability guarantees.
- **Partition tests**: simulated network partitions to verify Raft safety.
- **Linearizability checks**: recorded operation histories verified against a
  linearizability checker (e.g., Porcupine) under concurrent load + faults.
- **Race/leak detection**: `go test -race` and goroutine-leak detection
  (`goleak`) run on every test.

Run the full suite:

```bash
go test -race ./...
```

## Observability

- **Metrics:** Prometheus (election rate, replication lag, commit latency,
  fsync latency)
- **Tracing:** OpenTelemetry, propagated through gRPC metadata
- **Dashboards:** Grafana (see `/deploy/grafana` once added)

## Roadmap

See [TODO.md](./TODO.md) for the full phase-by-phase breakdown:

1. Storage engine (WAL, snapshotting, crash recovery)
2. Networking layer (gRPC, backpressure, graceful shutdown)
3. Raft consensus (election, replication, snapshots, membership, linearizable reads)
4. Concurrency correctness (idempotency, CAS, race/leak-free)
5. Observability (metrics, tracing, dashboards)
6. Sharding / multi-Raft (stretch)

## Why this project exists

This is a deliberate learning project to build hands-on understanding of the
distributed-systems concepts that come up constantly in senior backend work:
consensus, replication, CAP-theorem tradeoffs in practice, idempotency,
concurrency correctness, and operability — by building them rather than just
reading about them.

## License

MIT