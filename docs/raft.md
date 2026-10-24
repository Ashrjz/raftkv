# Raft: Leader Election

Status: draft. Scope is the Phase 3 "Leader Election" items only. Log
replication, Raft snapshots, membership changes and linearizable reads extend
this document later.

Location: `internal/raft/`, `proto/raft.proto`

## 1. What is this?

A Raft node that, in a static cluster, elects one leader per term and keeps it
in power with heartbeats. It follows Figure 2 of the Raft paper. This step has
no client commands: the log is empty and `AppendEntries` is used only as a
heartbeat.

## 2. Where it fits

```
 client ──gRPC──> server ──> [ Raft ] ──> state machine (Phase 3, replication)
                               │
              Transport <──────┤──────> Storage (hard state on disk)
         (gRPC to peers; fake in tests)
```

The node does not depend on the Phase 1-2 code in this step. The client path
is wired in during log replication.

## 3. Files

| File                  | Purpose                                                                 |
| --------------------- | ----------------------------------------------------------------------- |
| `proto/raft.proto`    | `Raft` service: `RequestVote`, `AppendEntries` (all Figure 2 fields)    |
| `types.go`            | `NodeID`, role, `LogEntry`, RPC args/replies, `Config`                  |
| `storage.go`          | `Storage` interface and the file-backed hard-state store                |
| `transport.go`        | `Transport` interface                                                   |
| `raft.go`             | `Raft` struct, run loop, RPC handlers, election, heartbeat, `Stop`      |
| `grpctransport.go`    | Real `Transport` over gRPC (added last)                                 |
| `memtransport_test.go`| In-memory fake `Transport` that can drop, delay or disconnect nodes     |

## 4. State (Figure 2)

| Kind                | Field                       | This step                          |
| ------------------- | --------------------------- | ---------------------------------- |
| Persistent          | `currentTerm`, `votedFor`   | Persisted (section 7)              |
| Persistent          | `log[]`                     | In memory only, empty (sentinel at index 0) |
| Volatile            | `commitIndex`, `lastApplied`| Declared, unused                   |
| Volatile on leaders | `nextIndex[]`, `matchIndex[]`| Declared, unused                  |
| Not in Figure 2     | role, `leaderID`, election timer, quit channel | Volatile        |

`NodeID` is a non-zero `uint64`; `votedFor = 0` means "no vote this term".
The in-memory log is only there so the vote check has `lastLogIndex` and
`lastLogTerm` (both 0 for now).

## 5. RPCs

### RequestVote
Args: `term`, `candidateId`, `lastLogIndex`, `lastLogTerm`.
Reply: `term`, `voteGranted`.

Receiver, in order:
1. If `term > currentTerm`: adopt the term, become follower, clear `votedFor`.
2. If `term < currentTerm`: reply `false`.
3. Grant only if `votedFor` is `0` or `candidateId`, **and** the candidate's log
   is at least as up-to-date as ours (higher last term wins; equal last term:
   longer log wins).
4. On grant: persist `votedFor`, then reply, then reset the election timer.

### AppendEntries (heartbeat only in this step)
Args: `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]`,
`leaderCommit`. Reply: `term`, `success`.

Receiver, in order:
1. If `term > currentTerm`: adopt the term, become follower, clear `votedFor`.
2. If `term < currentTerm`: reply `success = false` with our term.
3. Otherwise this is the current leader: a candidate steps down to follower,
   `leaderID` is recorded and the election timer is reset.
4. The `prevLogIndex`/`prevLogTerm` check, appending, and `leaderCommit` are
   added with log replication. With an empty log they trivially pass.

The proto carries all fields now so the wire format doesn't change later.

## 6. Rules

**All roles**
- Any RPC request or reply with `term > currentTerm`: adopt it, become
  follower, clear `votedFor`, persist.
- `currentTerm` and `votedFor` are persisted **before** replying to a request
  or sending a `RequestVote`.

**Follower**
- If the election timer fires with no valid `AppendEntries` and no vote
  granted, become candidate.
- The timer is reset only by a valid `AppendEntries` from the current leader or
  by granting a vote. A rejected `RequestVote` does not reset it.

**Candidate (election)**
1. `currentTerm++`, `votedFor = self`, persist, reset the timer with a freshly
   randomized timeout.
2. Send `RequestVote` to every peer in parallel goroutines.
3. Majority (`n/2 + 1`, counting the self vote) means become leader.
4. A valid `AppendEntries` from a leader of the same or a higher term means
   become follower.
5. Timer fires again: start a new election (back to step 1).

**Leader**
- On election, send a heartbeat to every peer immediately, then every
  `HeartbeatInterval`.
- Step down on any reply with a higher term.
- No check-quorum: a partitioned leader stays leader until it sees a higher
  term. Acceptable here; revisit with linearizable reads.

## 7. Hard-state storage

`Storage` in this step covers `currentTerm` and `votedFor` only:

```go
type HardState struct { Term uint64; VotedFor NodeID }

type Storage interface {
    Load() (HardState, error)  // zero value if nothing was saved
    Save(HardState) error      // returns only after the data is durable
}
```

The Raft log gets its own file format in the replication step (it needs suffix
truncation, which the Phase 1 WAL does not have). The Phase 1 WAL is not reused.

File `raft-state` in the data directory, same conventions as the WAL and
snapshot (big-endian, CRC32C):

| Field    | Size | Notes                         |
| -------- | ---: | ----------------------------- |
| Magic    |    4 | `RKVH`                        |
| Version  |    2 | `1`                           |
| Reserved |    2 | Must be 0                     |
| Term     |    8 |                               |
| VotedFor |    8 | `0` = none                    |
| CRC      |    4 | CRC32C over the first 24 bytes |

- **Save:** write `raft-state.tmp`, fsync, rename over `raft-state`, fsync the
  directory.
- **Load:** a missing file is a fresh node (`0, 0`). A short file, bad magic,
  version or CRC fails startup, with no fallback to a fresh state (that would
  allow a node to vote twice in a term). A stale `raft-state.tmp` is deleted
  on startup.
- If `Save` fails the handler returns an error: no vote is granted and no
  reply claims a state that is not durable.

## 8. Timers and configuration

```go
type Config struct {
    ID                NodeID
    Peers             map[NodeID]string // static list, including self
    ElectionTimeoutMin, ElectionTimeoutMax time.Duration
    HeartbeatInterval time.Duration
    Rand              *rand.Rand        // seeded in tests
}
```

- Election timeout is uniform in `[Min, Max)` and **re-randomized every time**
  it is armed. Suggested defaults: 150-300 ms election, 50 ms heartbeat. The
  heartbeat must be well below the minimum election timeout.
- Real `time.Timer`; tests use short timeouts and a seeded `Rand`.
- Peers are a static list from flags (`--id`, `--peers 1=host:port,...`).
  Membership changes are out of scope.

## 9. Concurrency rules

- One mutex guards all Raft state. It is **never held during a network call**
  or a disk write that blocks other RPCs for long; copy what you need, unlock,
  call, then lock again.
- After every RPC reply, re-check that `currentTerm` and the role are the same
  as when the request was sent. If not, drop the reply.
- Each peer call runs in its own goroutine with a context timeout.
- `Stop()` closes a quit channel and waits on a `WaitGroup`, so every
  goroutine (timer loop, heartbeat loop, per-peer calls) is joined before it
  returns. Handlers called after `Stop` return an error.
- Role and term changes are logged with `log.Printf` for now; structured
  logging comes in Phase 5.

## 10. Transport

```go
type Transport interface {
    RequestVote(ctx context.Context, to NodeID, a RequestVoteArgs) (RequestVoteReply, error)
    AppendEntries(ctx context.Context, to NodeID, a AppendEntriesArgs) (AppendEntriesReply, error)
}
```

Tests use an in-memory implementation that can drop, delay or disconnect
specific nodes. The real one is a thin gRPC client over `proto/raft.proto`.
Raft messages go on a separate listener from the client API, bound to
localhost like everything else (no auth, a stated non-goal).

## 11. Test plan (maps to TODO)

- Table-driven tests for each term rule in sections 5 and 6 (vote granted or
  denied, up-to-date check, step-down, timer reset or not).
- Hard-state store: round trip, missing file, truncated or corrupt file, stale
  tmp file.
- 3-node in-memory cluster elects exactly one leader.
- Kill the leader: a new one is elected within a bounded time.
- Repeated contention (many short-timeout runs, seeded): no split-vote livelock.
- Same cluster tests over real gRPC at the end.

## 12. Known limitations and open questions

- No log, so `AppendEntries` carries no data and the up-to-date check always
  compares equal logs.
- No check-quorum or pre-vote: a node cut off from the cluster keeps raising
  its term and can disrupt a healthy leader when it reconnects.
- Open for replication: the log file format (suffix truncation), using a plain
  `MemStore` as the state machine so commands are not logged twice, and how
  the client API finds and redirects to the leader.