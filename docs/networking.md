# Networking Layer Design

Location: `proto/`, `internal/server/`, `client/`, `cmd/kvserver/`

## 1. What is this?

The networking layer exposes the storage engine over gRPC: `Get`, `Put` and
`Delete` for clients, and `Snapshot` for operators. It also holds the hardening
around that: client timeouts, backpressure, and graceful shutdown. It contains
no storage logic; durability lives in the engine (`store.md`, `wal-format.md`,
`snapshot-format.md`).

## 2. Where it fits in the bigger picture

```
 client ──gRPC──> server (limiter, validation, error mapping) ──> Engine ──> WAL + snapshot
```

The server depends on a small interface, not on the engine, so the thing behind
it can change (for example a replicated state machine) without touching the
handlers.

## 3. Files

| File                        | Purpose                                                    |
| --------------------------- | ---------------------------------------------------------- |
| `proto/kv.proto`            | Contract: `KV` (Get/Put/Delete) and `Admin` (Snapshot)     |
| `internal/server/server.go` | `Store` interface, KV handlers, request validation         |
| `internal/server/errors.go` | `toStatus`: engine error to gRPC status                    |
| `internal/server/limits.go` | `MaxMessageSize`, derived from the WAL's `MaxPayload`      |
| `internal/server/limit.go`  | Concurrency-limit interceptor                              |
| `internal/server/run.go`    | Serve until cancelled, then drain                          |
| `internal/server/admin.go`  | `Snapshotter` interface, Admin `Snapshot` handler          |
| `client/client.go`          | Client library: default timeouts, `ErrNotFound`, `Snapshot`|
| `cmd/kvserver/main.go`      | Wires engine, server and signals into a runnable program   |

## 4. API

| RPC              | Request / response | Notes                                        |
| ---------------- | ------------------ | -------------------------------------------- |
| `KV.Get`         | `key` / `value`    | A missing key is status `NotFound`           |
| `KV.Put`         | `key`, `value` / - | Create or overwrite                          |
| `KV.Delete`      | `key` / -          | Idempotent: a missing key is not an error    |
| `Admin.Snapshot` | - / -              | Snapshot the state and truncate the WAL      |

All RPCs are unary. Keys are `string` and values are `bytes`, as in `store.Store`.

## 5. Design decisions and why

### 5.1 The server depends on interfaces, not the engine
`server.Store` has the same shape as `store.Store`, so `*store.Engine` satisfies
it directly. `Snapshotter` is a separate one-method interface, so the data
handlers never see snapshot machinery.

### 5.2 A missing key is status `NotFound`
`store.ErrNotFound` maps to `codes.NotFound` (store doc 4.3), and the client
turns it back into `client.ErrNotFound`. "The key is absent" stays
distinguishable from real failures, with one mechanism instead of two.

### 5.3 Writes are validated before they reach the engine
`Put` rejects an empty key, a key over `wal.MaxKeySize` and a value over
`wal.MaxValueSize`; `Delete` rejects an empty or oversized key. All return
`InvalidArgument`.

The engine rejects these too, but only after taking its write mutex, which is
held across an fsync. Checking first keeps an invalid request from queueing
behind other writers just to be refused. The engine's own errors map to the
same code as a backstop. `Get` is not validated, matching the engine: a key
that cannot exist is simply `NotFound`.

### 5.4 One error mapping for every handler

| Condition                                       | gRPC code                       |
| ----------------------------------------------- | ------------------------------- |
| `wal.ErrEmptyKey`, `wal.ErrTooLarge`            | `InvalidArgument`               |
| `store.ErrNotFound`                             | `NotFound`                      |
| `wal.ErrClosed`                                 | `Unavailable`                   |
| `wal.ErrBroken` ("restart required")            | `Internal`                      |
| cancelled or expired context                    | `Canceled` / `DeadlineExceeded` |
| anything else, including snapshot errors        | `Internal`                      |

Unexpected errors are logged in full on the server; the client gets a generic
message so internal details such as file paths do not leak.

### 5.5 Message size limit
gRPC's default receive limit (4 MiB) is below the engine's value limit
(16 MiB - 4 KiB). `MaxMessageSize = wal.MaxPayload + 64 KiB` is applied on the
server and as a client default, so the two limits cannot drift apart. A value
over the engine limit but under the message limit gets `InvalidArgument`;
anything larger is rejected by gRPC with `ResourceExhausted`.

### 5.6 Backpressure: a semaphore interceptor
At most `--max-inflight` handlers run at once. A request waits for a slot for at
most `--max-wait` (or until its own deadline), then is shed with
`ResourceExhausted`. The limit is global, which gRPC's per-connection
`MaxConcurrentStreams` cannot provide, and the error code is ours to choose.

### 5.7 Graceful shutdown: drain, then close storage
On SIGINT/SIGTERM, `GracefulStop` rejects new RPCs (clients see `Unavailable`)
and waits for in-flight ones, including a running snapshot. If draining exceeds
`--drain-timeout`, a hard `Stop` follows. `main` closes the engine only after the
server has stopped, so no handler can touch a closed WAL. `Engine.Close` also
waits for an in-flight snapshot, so even a forced stop is safe.

### 5.8 Timeouts live in the client library
Every client call gets a default timeout (2 s for data, 60 s for snapshot)
unless the caller's context already carries a deadline, which then wins.

### 5.9 Snapshot is an explicit Admin RPC
This matches the snapshot spec: explicit calls only, no automatic trigger. It is
a separate service so admin calls stay apart from the data API. It runs
synchronously and is serialized by the engine's `snapMu`. If the snapshot is
written but WAL truncation fails, the call returns `Internal`; the snapshot is
durable and the next call retries the truncation.

## 6. Behavior inherited from the engine

- **Writes are serialized, one fsync each** (~2.1 ms, see the WAL benchmark), so
  write latency under load grows with the number of concurrent writers.
  `max-wait` bounds the wait for a slot, not the wait on the engine mutex.
- **Reads are not blocked by fsyncs.** `Get` takes only a memory read lock.
- **The engine ignores `context`.** A request that times out may still commit,
  and a running snapshot cannot be cancelled. A client timeout therefore means
  the outcome is unknown (`Design.md` section 3).
- **Empty value and `nil` are the same**, as in the engine and on the wire.

## 7. Known limitations

- No TLS or authentication (a non-goal). The Admin service shares the data port,
  so anyone who can reach the port can trigger a snapshot.
- Over gRPC, keys must be valid UTF-8 (a proto3 `string` rule). The engine itself
  accepts any bytes.
- gRPC buffers a whole message before the limiter runs, so the limiter does not
  bound memory (worst case roughly in-flight requests x 16 MiB).
- A caller that cancels while waiting for a slot sees `ResourceExhausted`, not
  `Canceled`.
- Snapshots are manual: without a call, the WAL is never compacted.