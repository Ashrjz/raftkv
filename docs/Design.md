# raftkv design (v0)

Status: draft v0. Deliberately basic; revise as decisions change.

## 1. Goal
A distributed, replicated key-value store in Go, built on Raft, as a learning project.

**Current scope:** everything through Phase 3, Log Replication. That means a 3-node cluster that elects a leader, replicates a log to a majority, commits entries, and applies them to a key-value state machine.

**Out of scope for now** (later phases): Raft snapshots/compaction, membership changes, linearizable reads, fault-injection harness, idempotency/CAS, observability, sharding.

## 2. Supported operations (v1)
- `Get(key)`, `Put(key, value)`, `Delete(key)`
- Keys and values are `[]byte`
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

## 5. Non-goals (v1)
- Multi-key transactions and cross-shard atomicity
- Authentication and TLS
- Watches, TTLs, range scans
- Byzantine fault tolerance
- Multi-region deployment

## 6. Architecture
**Raft core style: direct.** The Raft struct holds its own state and calls its peers and storage itself, using real timers and goroutines. This keeps the code close to Figure 2 of the Raft paper.

Two small interfaces keep I/O swappable:
- `Transport`: sends `RequestVote` and `AppendEntries` to peers. The real implementation uses gRPC; tests can use a fake in-memory version that drops or delays messages.
- `Storage`: persists `currentTerm`, `votedFor`, and the log. The real implementation uses the Phase 1 WAL; tests can use an in-memory version.

The state machine (the key-value map) is separate from Raft. Raft hands it committed entries through an apply loop.

**Rules from day one (safety, not style):**
- Persist `currentTerm`, `votedFor`, and log entries **before** replying to any RPC.
- One mutex guards Raft state; never hold it during a network call.

## 7. Key decisions
| Decision           | Choice                                        | Alternatives considered                      | Why                                                                                                                                                                        |
| ------------------ | --------------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Networking         | gRPC                                          | `net/rpc`, raw TCP                           | Schema-first protos, deadlines via `context`, trace metadata for Phase 5                                                                                                   |
| Raft core style    | Direct, with `Transport`/`Storage` interfaces | I/O-free state machine (etcd's `Ready` loop) | Closest to Figure 2, fastest path to replication; interfaces keep testing and fault injection possible. Trade-off: timing-dependent tests are harder to make deterministic |
| Linearizable reads | Deferred                                      | ReadIndex, leader lease                      | Needs working replication first                                                                                                                                            |

## 8. Change log
- v0: chose direct-style Raft core; scoped current work to Phase 3 Log Replication.