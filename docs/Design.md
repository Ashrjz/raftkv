# raftkv design

Status: draft v0. Deliberately basic; revise as decisions change.

## 1. Goal
A distributed, replicated key-value store in Go, built on Raft, as a learning project.

**Done:** single-node durable storage (WAL, snapshots, crash recovery) and the gRPC layer (Phases 1-2).

**Current scope:** everything through Phase 3, Log Replication: a 3-node cluster that elects a leader, replicates a log to a majority, commits entries, and applies them to a key-value state machine.

**Out of scope for now:** Raft snapshots/compaction, membership changes, linearizable reads, fault-injection harness, idempotency/CAS, observability, sharding.

## 2. Supported operations
- `Get(key)`, `Put(key, value)`, `Delete(key)`
- Keys are strings (over gRPC they must be valid UTF-8); values are `[]byte`
- Limits: key 1 byte to 1 KiB, value up to 16 MiB - 4 KiB. An empty value is legal and reads back as empty.
- Later (Phase 4): compare-and-swap with per-key versions

## 3. Consistency model
- Goal: linearizable operations, all going through the leader.
- Writes are acknowledged only after commit (replicated on a majority and applied).
- Reads: linearizable reads (ReadIndex vs leader lease) are deferred to Phase 3, Linearizable Reads. Until then, reads go through the leader with no stale-read guarantee.
- A client timeout means the outcome is unknown (the write may or may not have committed). Idempotency keys (Phase 4) address this.

## 4. Failure model (assumptions)
- Nodes may crash and restart; disks survive the crash.
- The network may drop, delay, duplicate, reorder, and partition.
- No malicious (Byzantine) nodes.
- `fsync` really persists data.
- The cluster is available only while a majority of nodes are up and connected (3 nodes tolerate 1 failure, 5 tolerate 2).

## 5. Non-goals
- Multi-key transactions and cross-shard atomicity
- Authentication and TLS
- Watches, TTLs, range scans
- Byzantine fault tolerance
- Multi-region deployment

## 6. Architecture
**Raft core style: direct.** The Raft struct holds its own state and calls its peers and storage itself, using real timers and goroutines. This keeps the code close to Figure 2 of the Raft paper.

Two small interfaces keep I/O swappable:
- `Transport`: sends `RequestVote` and `AppendEntries` to peers. The real implementation uses gRPC; tests can use a fake in-memory version that drops or delays messages.
- `Storage`: persists `currentTerm`, `votedFor`, and (from log replication on) the log. **Decided:** it is a separate small store, not the Phase 1 WAL. Leader election uses an atomically replaced `raft-state` file; the log gets its own file format with suffix truncation in the replication step. See `raft.md` sections 7 and 12.

The state machine (the key-value map) is separate from Raft. Raft hands it committed entries through an apply loop. **Planned:** the state machine becomes a plain `MemStore` and the Raft log is the durability layer, so commands are not logged twice. The Phase 1 `Engine` is not used by Raft. To be confirmed when log replication starts.

**Rules from day one (safety, not style):**
- Persist `currentTerm`, `votedFor`, and log entries **before** replying to any RPC.
- One mutex guards Raft state; never hold it during a network call.

## 7. Key decisions
| Decision           | Choice                                        | Alternatives considered                      | Why                                                                                                                                                                        |
| ------------------ | --------------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Networking         | gRPC                                          | `net/rpc`, raw TCP                           | Schema-first protos, deadlines via `context`, trace metadata for Phase 5                                                                                                   |
| Raft core style    | Direct, with `Transport`/`Storage` interfaces | I/O-free state machine (etcd's `Ready` loop) | Closest to Figure 2, fastest path to replication; interfaces keep testing and fault injection possible. Trade-off: timing-dependent tests are harder to make deterministic |
| Linearizable reads | Deferred                                      | ReadIndex, leader lease                      | Needs working replication first                                                                                                                                            |
| Durability         | WAL with fsync per write, under one engine mutex | Group commit                              | Simple and correct; caps writes at about 450/s. Group commit exists in the WAL but the engine doesn't use it                                                               |
| Snapshots          | Manual, via Admin RPC; WAL truncated after a durable snapshot | Automatic trigger          | Explicit and testable; the WAL grows until called                                                                                                                          |
| Backpressure       | Global in-flight limit; excess requests get `ResourceExhausted` | Per-connection stream limit | gRPC's per-connection limit is not global                                                                                                                                  |
| Message size       | `MaxPayload` + 64 KiB, server and client      | gRPC default (4 MiB)                         | The default is below the engine's value limit
| Raft storage       | Separate store: `raft-state` file (term, vote) now; own log file later | Extend the Phase 1 WAL with new record types | The WAL has only Put/Delete and prefix truncation; Raft needs suffix truncation, and reusing `Engine` would log each command twice. Cost: a second on-disk format |                                                                                                                              |