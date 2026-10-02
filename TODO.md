# TODO — Distributed KV Store (Raft, Go)

## Phase 0: Groundwork
- [x] Read Raft paper (full read-through #1)
- [~] Read Raft paper again, focus on Figure 2 (state + RPC rules)
- [x] Walk through Raft visualization (thesecretlivesofdata.com/raft)
- [x] Skim etcd's `raft` package layout for structural reference
- [x] Write 1-page design doc: supported ops, consistency model, non-goals
- [x] Decide: gRPC for all networking (confirm)
- [x] Set up repo, module structure, CI (lint + test + race detector on push)
- [ ] Set up `goleak` in test suite baseline

---

## Phase 1: Storage Engine (single-node, durable)
- [x] In-memory map + mutex baseline (Get/Put/Delete)
- [x] Design WAL record format (length-prefixed + CRC32 checksum)
- [x] Implement WAL append-on-write, `fsync` per write
- [ ] Implement WAL replay on startup (rebuild map from log)
- [ ] Handle torn/partial last record on recovery (detect + discard)
- [ ] Implement snapshotting (dump map to disk)
- [ ] Implement WAL truncation after snapshot
- [ ] Handle concurrent reads during snapshot (RWMutex or copy-on-write)
- [ ] Benchmark: fsync-per-write vs batched/group commit
- [ ] **Test:** kill -9 test harness (random-point process kill + restart + verify)
- [ ] **Acceptance:** no acknowledged write lost across N kill-9 runs
- [ ] **Acceptance:** recovery time scales with WAL-since-snapshot, not total data

---

## Phase 2: Networking Layer
- [ ] Define proto file: Get / Put / Delete RPCs
- [ ] Implement gRPC server wrapping Phase 1 storage
- [ ] Implement gRPC client library
- [ ] Add `context.WithTimeout` on every client call
- [ ] Add server-side concurrency limit / semaphore (backpressure)
- [ ] Implement graceful shutdown (drain in-flight, reject new, deadline)
- [ ] **Benchmark:** p99 latency at target load (pick a number, hit it)
- [ ] **Test:** shutdown mid-load, verify no dropped in-flight requests

---

## Phase 3: Raft Consensus — Leader Election
- [ ] Implement Raft state fields (currentTerm, votedFor, log[], etc. — Figure 2)
- [ ] Implement RequestVote RPC (candidate + receiver logic)
- [ ] Implement randomized election timeout (per-node, re-randomized each timeout)
- [ ] Implement heartbeat (empty AppendEntries) from leader
- [ ] Table-driven tests for every term-comparison rule in Figure 2
- [ ] **Test:** 3-node cluster elects a leader
- [ ] **Test:** kill leader, cluster re-elects within bounded time
- [ ] **Test:** verify no split-vote livelock under repeated contention (stress test)

## Phase 3: Raft Consensus — Log Replication
- [ ] Implement AppendEntries RPC (leader side: send, retry on failure)
- [ ] Implement AppendEntries RPC (follower side: consistency check via prevLogIndex/prevLogTerm)
- [ ] Implement commit index advancement (quorum ack)
- [ ] Implement apply loop (commit index → state machine)
- [ ] **Test:** write survives on majority, unreachable minority catches up later
- [ ] **Test:** log divergence resolved correctly (leader overwrites follower's conflicting entries)

## Phase 3: Raft Consensus — Snapshots / Compaction
- [ ] Implement local snapshot (state machine + last-included-index/term)
- [ ] Implement InstallSnapshot RPC (leader → lagging follower)
- [ ] Handle snapshot/log boundary correctly (off-by-one check)
- [ ] **Test:** follower way behind gets snapshot instead of full log replay

## Phase 3: Raft Consensus — Membership Changes
- [ ] Implement single-node-change membership protocol (add one node)
- [ ] Implement single-node-change removal
- [ ] **Test:** add node to live cluster, no downtime, it catches up and participates

## Phase 3: Raft Consensus — Linearizable Reads
- [ ] Implement leader lease OR read-index protocol (pick one)
- [ ] **Test:** partitioned old leader cannot serve stale reads after new leader elected

## Phase 3: Fault Injection & Correctness Proof
- [ ] Build network partition simulator (drop/delay between specific node pairs)
- [ ] Build node crash/restart injection harness
- [ ] Integrate Porcupine (or similar) linearizability checker
- [ ] **Acceptance:** 5-node cluster tolerates 2 simultaneous failures, no data loss, stays available
- [ ] **Acceptance:** cluster converges to single leader + consistent log after partition heals (measure time)
- [ ] **Acceptance:** Jepsen-style run (concurrent ops + random faults) passes linearizability check

---

## Phase 4: Concurrency Correctness & Client Semantics
- [ ] Add idempotency key to write requests
- [ ] Implement leader-side dedup cache for idempotency keys
- [ ] Add versioning per key (value + version)
- [ ] Implement compare-and-swap (CAS) operation
- [ ] Run `go test -race` across full suite, fix all findings
- [ ] Add `goleak` checks to every test that spawns goroutines
- [ ] **Test:** forced retry-storm test, verify zero duplicate applies
- [ ] **Acceptance:** race detector clean, goroutine-leak detector clean

---

## Phase 5: Observability
- [ ] Structured logging: leader changes, term changes, commit index advances
- [ ] Prometheus metric: elections per hour
- [ ] Prometheus metric: replication lag per follower
- [ ] Prometheus metric: commit latency histogram
- [ ] Prometheus metric: WAL fsync latency
- [ ] OpenTelemetry tracing: propagate trace context through gRPC metadata
- [ ] Trace a single client request end-to-end (leader → replicate → commit → respond)
- [ ] Build Grafana dashboard (import metrics above)
- [ ] Decide INFO vs DEBUG log levels (avoid heartbeat log spam)
- [ ] **Acceptance:** diagnose an injected slow-disk fault using only dashboards/traces
- [ ] **Acceptance:** diagnose an injected network-delay fault using only dashboards/traces

---

## Phase 6: Sharding / Multi-Raft (stretch)
- [ ] Design keyspace partitioning scheme (range-based)
- [ ] Run multiple independent Raft groups, one per shard
- [ ] Build routing layer (key → shard owner)
- [ ] Implement shard rebalancing (no downtime)
- [ ] Document cross-shard atomicity limitation (no 2PC in v1 — explicit non-goal)

---

## Ongoing / Always
- [ ] Keep design doc updated as decisions change
- [ ] Re-run kill-9 test after every storage-layer change
- [ ] Re-run fault-injection suite after every Raft change
- [ ] Keep README's "current status" section accurate