# Store Module Design

Location: `internal/store/`

## 1. What is this?

The store holds the data: a key-value map with three operations.

| Operation         | Meaning                                          |
| ----------------- | ------------------------------------------------ |
| `Get(key)`        | Return the value for a key, or `ErrNotFound`     |
| `Put(key, value)` | Create or overwrite a key                        |
| `Delete(key)`     | Remove a key (no error if it was already absent) |

Two implementations sit behind the `Store` interface:

- **`MemStore`**: the in-memory map. Safe for concurrent use, not durable.
- **`Engine`**: `MemStore` plus a WAL and snapshots. This is what the server
  runs. An acknowledged write survives a crash.

Byte formats are in `wal-format.md` and `snapshot-format.md`.

## 2. Where it fits

```
 client ──gRPC──> server ──> [ Raft ] ──> Engine ──> MemStore
 (Phase 2)                   (Phase 3)      └──> WAL + snapshot (disk)
```

Today the server calls `Engine` directly. The store knows nothing about
networking or consensus. In Raft terms it is the state machine, but the Raft
log will be stored separately (see `raft.md`).

## 3. Files

| File           | Purpose                                                         |
| -------------- | --------------------------------------------------------------- |
| `store.go`     | `Store` interface and `ErrNotFound`                             |
| `memstore.go`  | `MemStore`, plus `clone()` (snapshot copy)                      |
| `engine.go`    | `Engine`: WAL-before-apply, recovery, `Snapshot`, `Close`       |

WAL and snapshot code lives in `internal/wal/`.

## 4. Design decisions

- **Interface first.** The server, engine and future Raft apply loop depend on
  the `Store` shape, not on one implementation.
- **Errors.** `MemStore` never fails. `Engine` can return `wal.ErrEmptyKey`,
  `wal.ErrTooLarge`, `wal.ErrBroken` (see below) and `wal.ErrClosed`, all
  wrapped so `errors.Is` works. A missing key is `ErrNotFound`.
- **Keys are `string`, values are `[]byte`.** `[]byte` can't be a map key.
- **Locking.** `MemStore` uses a `sync.RWMutex`. Values are copied in and out
  so callers can never change stored data.
- **Delete is idempotent.** Deleting a missing key is not an error. The engine
  still logs the tombstone.
- **WAL first, then memory.** `Engine` appends and fsyncs, then updates the map.
  `Get` therefore never sees non-durable data, and a crash after the fsync is
  fixed by replay.
- **One mutex across "append + apply".** Writes are serialized, one fsync each.
  This also lets a snapshot capture the map and the WAL offset at the same
  instant. `Get` bypasses this mutex.
- **Recovery.** On startup: delete a stale `snapshot.tmp`, load the snapshot,
  replay the WAL after the snapshot's offset, and discard a torn tail. A bad
  snapshot fails startup; it never falls back to a full replay.
- **Broken engine.** After a failed WAL write or fsync, all later writes return
  `ErrBroken` until the process restarts. Reads keep working.
- **Snapshot.** Writers pause only while the map is copied. The file write,
  rename and WAL truncation happen outside the lock. Any snapshot error means no
  truncation. If only truncation fails, the next call retries it.
  Lock order: `snapMu`, then `mu`.
- **Close** waits for an in-flight snapshot before closing the WAL.
- **No context.** Engine calls ignore `context`, so a timed-out write may still
  commit and a running snapshot can't be cancelled.

## 5. Baseline numbers

WAL append with fsync (AMD Ryzen 7 6800HS, Linux, amd64):

```
sync  goroutines=1    ~2.15 ms/op   1.0 records/fsync
group goroutines=64   ~77 µs/op     32.7 records/fsync
```

`Engine` uses the synchronous path under a global mutex, so write throughput is
capped at about 450 puts/s regardless of client concurrency.

## 6. Known limitations and open questions

- **Writes are serialized**, one fsync each. Reads are not affected.
- **Empty value and `nil` are the same after recovery.** Use `len(v) == 0`, not
  `v == nil`. An empty value is legal and is not a Delete.
- **Snapshots are manual** (Admin RPC or `kvcli snapshot`). Without a call, the
  WAL and restart time keep growing.
- **No versioning or CAS yet** (Phase 4), and no range scan.
- **Raft storage (decided, see `raft.md`).** The Raft node uses its own store
  (`raft-state` file, later a log file) instead of this WAL, which has only
  Put/Delete records and prefix truncation. Planned: Raft will apply
  committed entries to a plain `MemStore`, so `Engine` is not on the Raft
  path and commands are not logged twice. Confirm when log replication
  starts.