# Store Module Design

Location: `internal/store/`

## 1. What is this?

The store is the part of raftkv that actually holds the data. It is a key-value
map with three operations:

| Operation         | Meaning                                          |
| ----------------- | ------------------------------------------------ |
| `Get(key)`        | Return the value for a key, or `ErrNotFound`     |
| `Put(key, value)` | Create or overwrite a key                        |
| `Delete(key)`     | Remove a key (no error if it was already absent) |

Right now it lives entirely in memory. Later phases will make it durable
(WAL + snapshots) and then put Raft in front of it. This document explains the
current in-memory version and why it looks the way it does.

## 2. Where it fits in the bigger picture

```
 client ──gRPC──> server ──> [ Raft ] ──> Store  ──> (WAL + snapshots on disk)
 (Phase 2)                   (Phase 3)    (Phase 1)
```

The store knows nothing about networking or consensus. It only answers:
"what is the value for this key, and how do I change it safely?" Keeping it
that ignorant is deliberate: each layer can be built and tested on its own.

Eventually, Raft will call `Put`/`Delete` on the store once a command has been
agreed on by a majority of nodes (the "apply loop"). The store is the
**state machine** in Raft terminology.

## 3. Files

| File               | Purpose                                           |
| ------------------ | ------------------------------------------------- |
| `store.go`         | The `Store` interface and the `ErrNotFound` error |
| `memstore.go`      | `MemStore`, the in-memory implementation          |
| `memstore_test.go` | Unit tests, a concurrency test, and a benchmark   |

## 4. Design decisions and why

### 4.1 A `Store` interface, not just a struct
Other layers (WAL-backed store, Raft apply loop, test fakes) depend on the
*shape* of the store, not on one implementation. Defining the interface first
lets us swap implementations without touching callers.

### 4.2 `Put` and `Delete` return `error`
An in-memory map cannot fail, so the error is always `nil` today. But once a
write goes to disk through a WAL, it can fail (disk full, fsync error).
Putting `error` in the signature now avoids changing every caller later.

### 4.3 `ErrNotFound` sentinel for missing keys
`Get` returns `ErrNotFound` for a missing key. Callers check it with
`errors.Is(err, store.ErrNotFound)`. In Phase 2 the gRPC layer will translate
it to the `NotFound` status code.

### 4.4 Keys are `string`, values are `[]byte`
Go map keys must be comparable, and `[]byte` is not, so keys are strings.
Values are raw bytes because the store should not care what they contain.

### 4.5 A mutex protects the map
Go maps are not safe for concurrent use. A simultaneous read and write is a
data race and can crash the program. Many goroutines will call the store
(gRPC handlers, the Raft apply loop), so every access goes through a lock.

We use `sync.RWMutex`:
- `Get` takes a **read lock** (many readers can hold it at once).
- `Put` and `Delete` take the **write lock** (exclusive).

`RWMutex` also prepares us for snapshots, where reads must keep working while
a snapshot is being taken.

### 4.6 Values are copied on the way in and on the way out
A `[]byte` is a pointer to shared memory. Without copying, a caller could
modify a slice after calling `Put`, or modify the slice returned by `Get`, and
silently change what is stored, bypassing the lock. Copying makes the store the
sole owner of its data. (`TestNoAliasing` checks this.)

The copy in `Put` is made before taking the lock, so the lock is held only for
the map assignment.

### 4.7 `Delete` of a missing key is not an error
Deleting is **idempotent**: doing it twice has the same result as doing it
once. This matters later, because client retries (Phase 4) and Raft log
replay can apply the same command more than once.

## 5. How it is tested

| Test               | What it proves                                                             |
| ------------------ | -------------------------------------------------------------------------- |
| `TestPutGetDelete` | Basic behaviour: missing key, create, overwrite, delete, idempotent delete |
| `TestNoAliasing`   | Mutating the caller's slice never changes stored data                      |
| `TestConcurrent`   | Many goroutines using the store at once; meaningful only with `-race`      |
| `BenchmarkPut`     | Baseline write throughput with no durability                               |

Run:

```
go test -race ./internal/store/
go test -bench . -benchmem ./internal/store/
```

Note for Windows/PowerShell: write `-bench .` (with a space) or
`-bench="."`. The unquoted form `-bench=.` is split by PowerShell.

## 6. Baseline numbers

Machine: AMD Ryzen 7 6800HS, Windows, amd64.

```
BenchmarkPut-16   5784598   208.8 ns/op   21 B/op   2 allocs/op
```

Caveats: the benchmark builds keys with `fmt.Sprintf` inside the loop, so part
of the 208 ns is key formatting, not the store. Treat this as a rough
"no-durability ceiling" to compare against when fsync is added in Phase 1.

## 7. Known limitations and open questions

- **Not durable.** Everything is lost on restart. This is what the WAL and
  snapshot items in Phase 1 fix.
- **`nil` vs empty value is undefined.** `Put(k, nil)` stores a nil value, and
  `Get` returns `nil, nil`, which is indistinguishable from an empty value.
  Decide this before designing the WAL record format, since the format must
  encode it.
- **No versioning or compare-and-swap yet.** Planned for Phase 4.
- **No iteration or range scan.** Not needed yet; snapshotting will need a way
  to read all entries consistently.

## 8. What changes next

1. A WAL record format and append-on-write, so writes survive a crash.
2. WAL replay on startup to rebuild the map.
3. Snapshots and WAL truncation.

The `Store` interface should stay the same through all of this. Only the
implementation behind it changes.