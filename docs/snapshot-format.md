# Snapshot Format (v1)

A snapshot is a point-in-time copy of the full key-value map, plus the WAL
position it covers. Recovery loads the snapshot, then replays only the WAL
records after that position.

## Conventions

- Big-endian, fixed-width integers (same as the WAL).
- Checksum: CRC32C (Castagnoli), same as the WAL.
- Key and value limits are the WAL's constants (`MaxKeySize`, `MaxValueSize`),
  so any pair valid in the WAL fits in a snapshot.
- No backward compatibility: any format change bumps the version.

## Files

All files live in the same directory as the WAL.

| Name           | Purpose                                              |
| -------------- | ---------------------------------------------------- |
| `snapshot`     | Current snapshot (at most one)                       |
| `snapshot.tmp` | In-progress write; never read, deleted on startup    |

## Layout

```
[ Header (12 B) ][ Metadata (16 B) ][ Entry 0 .. Entry N-1 ][ Footer (4 B) ]
```

### Header (12 bytes)

| Field    | Size | Rule                                      |
| -------- | ---: | ----------------------------------------- |
| Magic    |    8 | `RKVSNAP\0`                               |
| Version  |    2 | `1`; any other value is rejected          |
| Reserved |    2 | Must be 0; non-zero is rejected           |

### Metadata (16 bytes)

| Field      | Size | Notes                                            |
| ---------- | ---: | ------------------------------------------------ |
| WALOffset  |    8 | Logical WAL offset covered by this snapshot      |
| EntryCount |    8 | Number of entries that follow                    |

### Entry

| Field    | Size     | Notes                                      |
| -------- | -------: | ------------------------------------------ |
| KeyLen   |        4 | 1 to `MaxKeySize` (0 is rejected)          |
| ValueLen |        4 | 0 to `MaxValueSize`                        |
| Key      |   KeyLen |                                            |
| Value    | ValueLen | A zero-length value loads as `nil`         |

Entries are in no defined order (two snapshots of the same data may differ
byte for byte). Duplicate keys are corruption.

### Footer (4 bytes)

CRC32C over every preceding byte of the file.

## WAL position

`WALOffset` is a logical offset: the total record bytes ever appended to the
WAL, excluding headers. Truncation does not change it. The WAL header's
`BaseOffset` is the logical offset of its first record, so replay starts at:

```
physical position = 20 + (WALOffset - BaseOffset)
```

`WALOffset < BaseOffset` (WAL truncated past the snapshot) or `WALOffset`
beyond the end of the WAL is corruption (`ErrBadOffset`) and fails startup.

## Writing (atomic)

1. Under the engine lock, copy the map and read the WAL's logical end offset.
2. Write the full file to `snapshot.tmp`.
3. fsync `snapshot.tmp`.
4. Rename it over `snapshot`.
5. fsync the parent directory.

A crash before step 4 leaves the previous snapshot (or none) and the full WAL.
A crash after step 4 leaves the new snapshot and an untruncated WAL. Both
recover correctly.

Failure rules:
- Any error before the rename removes `snapshot.tmp` and leaves the old
  snapshot in place.
- If the directory fsync fails after the rename, the new snapshot may or may not
  be durable. The snapshot is treated as not taken, and the WAL is **not**
  truncated.

## Concurrency

Writers are blocked only while the map is copied (step 1). Steps 2-5 and the WAL
truncation run without the engine lock; writes made meanwhile land after the
captured offset and are kept as the new WAL tail. Snapshots are serialized, so
an older one can never replace a newer one.

## Loading and validation

Startup fails (no fallback to full WAL replay) if:

- magic, version or reserved field is invalid
- the file is shorter than header + metadata + footer
- `EntryCount` is impossible for the file size (checked before allocating)
- any `KeyLen` or `ValueLen` is out of range, or an entry runs past the end of
  the file (checked before allocating)
- the file ends early, or has bytes between the last entry and the footer
- a duplicate key is found
- the footer CRC does not match

Entries are decoded into a temporary map, and the engine's map is replaced
only after the CRC verifies.

## Recovery order

1. Delete `snapshot.tmp` if present.
2. If `snapshot` exists, load and validate it, then set the map.
3. Replay the WAL from the position derived above.
4. Torn-tail handling is the same as in `wal-format.md`.

## Non-goals (v1)

- Incremental or compressed snapshots.
- Multiple retained snapshots.
- Automatic triggers: snapshots run only when `Snapshot()` is called (Admin RPC
  or `kvcli snapshot`).
- Backward compatibility.